/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "share/stat/ob_dbms_stats_executor.h"
#include "share/stat/ob_basic_stats_estimator.h"
#include "share/stat/ob_hybrid_hist_estimator.h"
#include "share/stat/ob_opt_column_stat_cache.h"
#include "share/stat/ob_opt_stat_manager.h"
#include "share/stat/ob_dbms_stats_utils.h"
#include "share/stat/ob_incremental_stat_estimator.h"
#include "share/stat/ob_dbms_stats_history_manager.h"
#include "share/stat/ob_dbms_stats_lock_unlock.h"
#include "share/stat/ob_index_stats_estimator.h"
#include "pl/sys_package/ob_dbms_stats.h"
#include "share/stat/ob_opt_stat_gather_stat.h"
#include "share/stat/ob_dbms_stats_gather.h"
#include "observer/omt/ob_tenant.h"
#include "observer/ob_inner_sql_connection_pool.h"
#include "observer/ob_inner_sql_connection.h"
#include "src/observer/ob_server.h"

namespace oceanbase {
using namespace pl;
namespace common {

/**
 * @brief ObDbmsStatsUtils::gather_table_stats
 *  构造表级别统计信息的收集 SQL
 * @return
 */
int ObDbmsStatsExecutor::gather_table_stats(ObExecContext &ctx,
                                            const ObTableStatParam &param,
                                            ObOptStatRunningMonitor &running_monitor)
{
  int ret = OB_SUCCESS;
  LOG_TRACE("begin to gather table stats", K(param));
  PartitionIdBlockMap partition_id_block_map;
  GatherHelper gather_helper(running_monitor);
  ObMySQLTransaction gather_trans;
  ObMySQLTransaction backup_trans;
  if (OB_FAIL(gather_trans.start(ctx.get_sql_proxy(), param.tenant_id_)) ||
      OB_FAIL(backup_trans.start(ctx.get_sql_proxy(), param.tenant_id_))) {
    LOG_WARN("fail to start transaction", K(ret));
  } else if (OB_FAIL(prepare_gather_stats(ctx, gather_trans, param,
                                          partition_id_block_map,
                                          gather_helper))) {
    LOG_WARN("failed to prepare gather stats", K(ret));
  } else if (gather_helper.is_split_gather_ &&
             OB_FAIL(split_gather_stats(ctx, gather_trans, param, &partition_id_block_map, gather_helper))) {
    LOG_WARN("failed to split gather stats", K(ret));
  } else if (!gather_helper.is_split_gather_ &&
             OB_FAIL(no_split_gather_stats(ctx, gather_trans, param, &partition_id_block_map, gather_helper))) {
    LOG_WARN("failed to do gather stats", K(ret));
  } else if (OB_FAIL(running_monitor.add_monitor_info(ObOptStatRunningPhase::BACKUP_HISTORY_STATS))) {
    LOG_WARN("failed to add add monitor info", K(ret));
  } else if (!param.is_temp_table_ &&
             OB_FAIL(ObDbmsStatsHistoryManager::backup_opt_stats(ctx, backup_trans, param,
                                                                 ObTimeUtility::current_time(),
                                                                 true))) {
    LOG_WARN("failed to backup opt stats", K(ret));
  } else if (share::schema::ObTableType::EXTERNAL_TABLE != param.ref_table_type_ &&
             OB_FAIL(ObBasicStatsEstimator::update_last_modified_count(gather_trans.get_connection(), param))) {
    LOG_WARN("failed to update last modified count", K(ret));
  }
  //end gather trans
  if (OB_SUCC(ret)) {
    if (OB_FAIL(gather_trans.end(true))) {
      LOG_WARN("fail to commit transaction", K(ret));
    }
  } else {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = gather_trans.end(false))) {
      LOG_WARN("fail to roll back transaction", K(tmp_ret));
    }
  }
  //end backup trans
  if (OB_SUCC(ret)) {
    if (OB_FAIL(backup_trans.end(true))) {
      LOG_WARN("fail to commit transaction", K(ret));
    }
  } else {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = backup_trans.end(false))) {
      LOG_WARN("fail to roll back transaction", K(tmp_ret));
    }
  }
  return ret;
}

int ObDbmsStatsExecutor::split_gather_stats(ObExecContext &ctx,
                                            ObMySQLTransaction &trans,
                                            const ObTableStatParam &param,
                                            const PartitionIdBlockMap *partition_id_block_map,
                                            GatherHelper &gather_helper)

{
  int ret = OB_SUCCESS;
  if (param.subpart_stat_param_.need_modify_) {//process subpart stats
    if (param.part_level_ != share::schema::PARTITION_LEVEL_TWO) {
      /*do nothing*/
    } else if (OB_FAIL(split_gather_partition_stats(ctx, trans, param, SUBPARTITION_LEVEL,
                                                    partition_id_block_map, gather_helper))) {
      LOG_WARN("failed to split gather partition stats", K(ret));
    }
  }
  if (OB_SUCC(ret) && param.part_stat_param_.need_modify_) {//process part stats
    gather_helper.is_approx_gather_ = param.part_stat_param_.can_use_approx_;
    if (OB_FAIL(THIS_WORKER.check_status())) {
      LOG_WARN("check status failed", KR(ret));
    } else if (param.part_level_ == share::schema::PARTITION_LEVEL_ZERO) {
      /*do nothing*/
    } else if (OB_UNLIKELY(!param.part_stat_param_.can_use_approx_ && !param.approx_part_infos_.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(ret), K(param));
    } else if (!param.part_infos_.empty()) {//gather some part stats
      if (OB_FAIL(split_gather_partition_stats(ctx, trans, param, PARTITION_LEVEL,
                                               partition_id_block_map, gather_helper))) {
        LOG_WARN("failed to split gather partition stats", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (param.part_stat_param_.can_use_approx_ && !param.approx_part_infos_.empty()) {
        if (OB_FAIL(split_derive_part_stats_by_subpart_stats(ctx, trans, param,
                                                             partition_id_block_map,
                                                             gather_helper))) {
          LOG_WARN("failed to split derive part stats by subpart stats", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret) && param.global_stat_param_.need_modify_) {//process global stats
    gather_helper.is_approx_gather_ = param.global_stat_param_.gather_approx_;
    if (OB_FAIL(THIS_WORKER.check_status())) {
      LOG_WARN("check status failed", KR(ret));
    } else if (OB_FAIL(split_gather_global_stats(ctx, trans, param, partition_id_block_map, gather_helper))) {
      LOG_WARN("failed to split gather table stats", K(ret));
    }
  }
  return ret;
}

int ObDbmsStatsExecutor::no_split_gather_stats(ObExecContext &ctx,
                                               ObMySQLTransaction &trans,
                                               const ObTableStatParam &param,
                                               const PartitionIdBlockMap *partition_id_block_map,
                                               GatherHelper &gather_helper)

{
  int ret = OB_SUCCESS;
  ObArray<ObOptTableStat *> all_tstats;
  ObArray<ObOptColumnStat *> all_cstats;
  ObSEArray<ObOptStat, 4> subpart_opt_stats;
  ObSEArray<ObOptStat, 4> part_opt_stats;
  ObSEArray<ObOptStat, 1> global_opt_stats;
  if (param.subpart_stat_param_.need_modify_) {//process subpart stats
    ObOptStatGatherParam gather_param;
    if (param.part_level_ != share::schema::PARTITION_LEVEL_TWO ||
        param.subpart_infos_.empty()) {
      /*do nothing*/
    } else if (OB_FAIL(gather_helper.running_monitor_.add_monitor_info(ObOptStatRunningPhase::GATHER_SUBPART_STATS))) {
      LOG_WARN("failed to add add monitor info", K(ret));
    } else if (OB_FAIL(ObDbmsStatsUtils::prepare_gather_stat_param(param, SUBPARTITION_LEVEL, partition_id_block_map,
                                                                   false, gather_helper.gather_vectorize_,
                                                                   gather_helper.use_column_store_,
                                                                   gather_param))) {
      LOG_WARN("failed to prepare gather stat param", K(ret));
    } else if (OB_FAIL(do_gather_stats(ctx, trans, gather_param,
                                       param.subpart_infos_,
                                       param.column_params_,
                                       true,
                                       subpart_opt_stats,
                                       all_tstats,
                                       all_cstats))) {
      LOG_WARN("failed to do gather stats", K(ret));
    }
  }
  if (OB_SUCC(ret) && param.part_stat_param_.need_modify_) {//process part stats
    if (OB_FAIL(THIS_WORKER.check_status())) {
      LOG_WARN("check status failed", KR(ret));
    } else if (param.part_level_ == share::schema::PARTITION_LEVEL_ZERO) {
      /*do nothing*/
    } else if (OB_UNLIKELY(!param.part_stat_param_.can_use_approx_ && !param.approx_part_infos_.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(ret), K(param));
    } else if (!param.part_infos_.empty()) {//gather some part stats
      ObOptStatGatherParam gather_param;
      if (OB_FAIL(gather_helper.running_monitor_.add_monitor_info(ObOptStatRunningPhase::GATHER_PART_STATS))) {
        LOG_WARN("failed to add add monitor info", K(ret));
      } else if (OB_FAIL(ObDbmsStatsUtils::prepare_gather_stat_param(param, PARTITION_LEVEL, partition_id_block_map,
                                                                     false, gather_helper.gather_vectorize_,
                                                                     gather_helper.use_column_store_,
                                                                     gather_param))) {
        LOG_WARN("failed to prepare gather stat param", K(ret));
      } else if (OB_FAIL(do_gather_stats(ctx, trans, gather_param,
                                         param.part_infos_,
                                         param.column_params_,
                                         true,
                                         part_opt_stats,
                                         all_tstats,
                                         all_cstats))) {
        LOG_WARN("failed to do gather stats", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (param.part_stat_param_.can_use_approx_ && !param.approx_part_infos_.empty()) {//approx some part stats base on subpart stats
        if (OB_UNLIKELY(subpart_opt_stats.empty())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected error", K(ret), K(param.approx_part_infos_), K(subpart_opt_stats));
        } else if (OB_FAIL(gather_helper.running_monitor_.add_monitor_info(ObOptStatRunningPhase::APPROX_GATHER_PART_STATS))) {
          LOG_WARN("failed to add add monitor info", K(ret));
        } else if (OB_FAIL(ObIncrementalStatEstimator::derive_part_stats_from_subpart_stats(ctx, param, subpart_opt_stats, part_opt_stats))) {
          LOG_WARN("failed to try derive global stat", K(ret));
        } else if (OB_FAIL(ObDbmsStatsUtils::calssify_opt_stat(part_opt_stats,
                                                               all_tstats,
                                                               all_cstats))) {
          LOG_WARN("failed to calssify opt stat", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret) && param.global_stat_param_.need_modify_) {//process global stats
    ObOptStatGatherParam gather_param;
    ObOptStat global_opt_stat;
    ObArray<PartInfo> dummy_part_infos;
    if (OB_FAIL(THIS_WORKER.check_status())) {
      LOG_WARN("check status failed", KR(ret));
    } else if (param.global_stat_param_.gather_approx_ && !part_opt_stats.empty()) {//approx global stats base on part stats
      if (OB_FAIL(gather_helper.running_monitor_.add_monitor_info(ObOptStatRunningPhase::APPROX_GATHER_GLOBAL_STATS))) {
        LOG_WARN("failed to add add monitor info", K(ret));
      } else if (OB_FAIL(ObIncrementalStatEstimator::derive_global_stat_from_part_stats(ctx, param, part_opt_stats, global_opt_stat))) {
        LOG_WARN("failed to try derive global stat", K(ret));
      } else if (OB_FAIL(global_opt_stats.push_back(global_opt_stat))) {
        LOG_WARN("failed to push back", K(ret));
      } else if (OB_FAIL(ObDbmsStatsUtils::calssify_opt_stat(global_opt_stats,
                                                             all_tstats,
                                                             all_cstats))) {
        LOG_WARN("failed to calssify opt stat", K(ret));
      }
    } else if (OB_FAIL(gather_helper.running_monitor_.add_monitor_info(ObOptStatRunningPhase::GATHER_GLOBAL_STATS))) {
      LOG_WARN("failed to add add monitor info", K(ret));
    } else if (OB_FAIL(ObDbmsStatsUtils::prepare_gather_stat_param(param, TABLE_LEVEL, partition_id_block_map,
                                                                   false, gather_helper.gather_vectorize_,
                                                                   gather_helper.use_column_store_,
                                                                   gather_param))) {
      LOG_WARN("failed to prepare gather stat param", K(ret));
    } else if (OB_FAIL(do_gather_stats(ctx, trans, gather_param,
                                       dummy_part_infos,
                                       param.column_params_,
                                       true,
                                       global_opt_stats,
                                       all_tstats,
                                       all_cstats))) {
      LOG_WARN("failed to do gather stats", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObDbmsStatsUtils::split_batch_write(ctx, trans.get_connection(), all_tstats, all_cstats))) {
      LOG_WARN("failed to split batch write", K(ret));
    }
  }
  return ret;
}

/** @brief ObDbmsStatsExecutor::prepare_gather_stats used to prepare gather table stats, including:
 * 1.estimate block count;
 * 2.get the maximum num of partitions and columns for each stat gather.
*/
int ObDbmsStatsExecutor::prepare_gather_stats(ObExecContext &ctx,
                                              ObMySQLTransaction &trans,
                                              const ObTableStatParam &param,
                                              PartitionIdBlockMap &partition_id_block_map,
                                              GatherHelper &gather_helper)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(partition_id_block_map.create(10000,
                                            ObModIds::OB_HASH_BUCKET_TABLE_STATISTICS,
                                            ObModIds::OB_HASH_BUCKET_TABLE_STATISTICS,
                                            param.tenant_id_))) {
    LOG_WARN("failed to create hash map", K(ret));
  } else if (param.need_estimate_block_ &&
             share::schema::ObTableType::EXTERNAL_TABLE != param.ref_table_type_ &&
             OB_FAIL(ObBasicStatsEstimator::estimate_block_count(ctx, param,
                                                                 partition_id_block_map,
                                                                 gather_helper.use_column_store_,
                                                                 gather_helper.use_split_part_))) {
    LOG_WARN("failed to estimate block count", K(ret));
  } else if (OB_FAIL(check_need_split_gather(param, gather_helper))) {
    LOG_WARN("failed to check need split gather", K(ret));
  } else {
    LOG_TRACE("succeed to prepare gather stats", K(param), K(gather_helper));
  }
  return ret;
}

int ObDbmsStatsExecutor::split_gather_partition_stats(ObExecContext &ctx,
                                                      ObMySQLTransaction &trans,
                                                      const ObTableStatParam &param,
                                                      StatLevel stat_level,
                                                      const PartitionIdBlockMap *partition_id_block_map,
                                                      const GatherHelper &gather_helper)
{
  int ret = OB_SUCCESS;
  ObOptStatGatherParam gather_param;
  ObArenaAllocator allocator("SplitGatherStat", OB_MALLOC_NORMAL_BLOCK_SIZE, param.tenant_id_);
  const ObIArray<PartInfo> &partition_infos = stat_level == PARTITION_LEVEL ? param.part_infos_ : param.subpart_infos_;
  if (OB_UNLIKELY((stat_level != PARTITION_LEVEL && stat_level != SUBPARTITION_LEVEL) ||
                   !gather_helper.is_split_gather_ ||
                   gather_helper.maximum_gather_col_cnt_ < 1 ||
                   gather_helper.maximum_gather_part_cnt_ < 1 ||
                   partition_infos.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(param), K(stat_level), K(gather_helper));
  } else if (OB_FAIL(ObDbmsStatsUtils::prepare_gather_stat_param(param, stat_level, partition_id_block_map, true,
                                                                 gather_helper.gather_vectorize_,
                                                                 gather_helper.use_column_store_,
                                                                 gather_param))) {
    LOG_WARN("failed to prepare gather stat param", K(ret));
  } else {//need split gather
    int64_t idx_part = 0;
    int64_t total_split_cnt = std::ceil((1.0 * partition_infos.count() / gather_helper.maximum_gather_part_cnt_) *
                                         (1.0 * param.column_params_.count() / gather_helper.maximum_gather_col_cnt_));
    ObOptStatRunningPhase current_phase = stat_level == SUBPARTITION_LEVEL ? ObOptStatRunningPhase::GATHER_SUBPART_STATS : ObOptStatRunningPhase::GATHER_PART_STATS;
    ObOptStatRunningPhase next_phase = stat_level == SUBPARTITION_LEVEL ? ObOptStatRunningPhase::GATHER_PART_STATS : ObOptStatRunningPhase::GATHER_GLOBAL_STATS;
    double split_extra_ratio = gather_helper.running_monitor_.get_monitor_extra_progress_ratio(current_phase, next_phase, total_split_cnt);
    double extra_ratio = 0.0;
    do {
      ObSEArray<PartInfo, 4> gather_partition_infos;
      for (int64_t i = 0; OB_SUCC(ret) && i < gather_helper.maximum_gather_part_cnt_ && idx_part < partition_infos.count(); ++i) {
        if (OB_FAIL(gather_partition_infos.push_back(partition_infos.at(idx_part++)))) {
          LOG_WARN("failed to push back", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (gather_helper.maximum_gather_col_cnt_ >= param.get_need_gather_column()) {
          ObSEArray<ObOptTableStat *, 4> all_tstats;
          ObSEArray<ObOptColumnStat *, 4> all_cstats;
          ObSEArray<ObOptStat, 4> opt_stats;
          gather_param.allocator_ = &allocator;
          if (OB_FAIL(gather_helper.running_monitor_.add_monitor_info(current_phase, extra_ratio))) {
            LOG_WARN("failed to add add monitor info", K(ret));
          } else if (OB_FAIL(do_gather_stats(ctx, trans, gather_param,
                                             gather_partition_infos,
                                             param.column_params_,
                                             true,
                                             opt_stats,
                                             all_tstats,
                                             all_cstats))) {
            LOG_WARN("failed to do gather stats", K(ret));
          } else {
            allocator.reuse();//Phased memory release for split gather, in order to reuse memory
          }
        } else {
          int64_t idx_col = 0;
          ObOptTableStat part_Stat;
          ObSEArray<ObOptTableStat *, 1> all_tstats;
          if (OB_FAIL(all_tstats.push_back(&part_Stat))) {
            LOG_WARN("faile to push back", K(ret));
          } else if (OB_FAIL(fetch_gather_table_snapshot_read(trans.get_connection(), gather_param.tenant_id_, gather_param.sepcify_scn_))) {
            LOG_WARN("failed to fetch gather table snapshot read", K(ret));
            ret = OB_SUCCESS;//if we failed to get the read snapshot, just skip, not specify the snapshot
          }
          if (OB_SUCC(ret)) {
            do {
              ObSEArray<ObColumnStatParam, 4> gather_column_params;
              for (int64_t i = 0; OB_SUCC(ret) && i < gather_helper.maximum_gather_col_cnt_ && idx_col < param.column_params_.count(); ++i) {
                if (OB_FAIL(gather_column_params.push_back(param.column_params_.at(idx_col++)))) {
                  LOG_WARN("failed to push back", K(ret));
                }
              }
              if (OB_SUCC(ret)) {
                ObSEArray<ObOptColumnStat *, 4> all_cstats;
                ObSEArray<ObOptStat, 4> opt_stats;
                gather_param.allocator_ = &allocator;
                if (OB_FAIL(gather_helper.running_monitor_.add_monitor_info(current_phase, extra_ratio))) {
                  LOG_WARN("failed to add add monitor info", K(ret));
                } else if (OB_FAIL(do_gather_stats(ctx, trans, gather_param,
                                                   gather_partition_infos,
                                                   gather_column_params,
                                                   idx_col == param.column_params_.count(),
                                                   opt_stats,
                                                   all_tstats,
                                                   all_cstats))) {
                  if (gather_param.sepcify_scn_ > 0 &&
                      (ret == OB_TABLE_DEFINITION_CHANGED || ret == OB_SNAPSHOT_DISCARDED)) {
                    LOG_WARN("failed to specify snapshot to gather stats, try no specify snapshot to gather stats", K(ret));
                    gather_param.sepcify_scn_ = 0;
                    allocator.reuse();
                    opt_stats.reset();
                    all_cstats.reset();
                    if (OB_FAIL(do_gather_stats(ctx, trans, gather_param,
                                                gather_partition_infos,
                                                gather_column_params,
                                                idx_col == param.column_params_.count(),
                                                opt_stats,
                                                all_tstats,
                                                all_cstats))) {
                      LOG_WARN("failed to do gather stats", K(ret));
                    } else {
                      allocator.reuse();//Phased memory release for split gather, in order to reuse memory
                    }
                  } else {
                    LOG_WARN("failed to do gather stats", K(ret));
                  }
                } else {
                  allocator.reuse();//Phased memory release for split gather, in order to reuse memory
                }
              }
            } while(OB_SUCC(ret) && idx_col < param.column_params_.count());
          }
          gather_param.sepcify_scn_ = 0;//Try to ensure that the stat of a partition are collected in a snapshot
        }
      }
      extra_ratio += split_extra_ratio;
    } while (OB_SUCC(ret) && idx_part < partition_infos.count());
  }
  return ret;
}

int ObDbmsStatsExecutor::split_derive_part_stats_by_subpart_stats(ObExecContext &ctx,
                                                                  ObMySQLTransaction &trans,
                                                                  const ObTableStatParam &param,
                                                                  const PartitionIdBlockMap *partition_id_block_map,
                                                                  const GatherHelper &gather_helper)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("SplitDeriveStat", OB_MALLOC_NORMAL_BLOCK_SIZE, param.tenant_id_);
  if (OB_UNLIKELY(!param.part_stat_param_.can_use_approx_ ||
                  !param.subpart_stat_param_.need_modify_ ||
                   param.part_level_ != share::schema::PARTITION_LEVEL_TWO ||
                   param.approx_part_infos_.empty() ||
                   !gather_helper.is_split_gather_ ||
                   gather_helper.maximum_gather_col_cnt_ < 1 ||
                   gather_helper.maximum_gather_part_cnt_ < 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(param), K(gather_helper));
  } else {//need split gather
    int64_t idx_part = 0;
    int64_t total_split_cnt = std::ceil((1.0 * param.approx_part_infos_.count() / gather_helper.maximum_gather_part_cnt_) *
                                         (1.0 * param.column_params_.count() / gather_helper.maximum_gather_col_cnt_));
    ObOptStatRunningPhase current_phase = ObOptStatRunningPhase::APPROX_GATHER_PART_STATS;
    ObOptStatRunningPhase next_phase = ObOptStatRunningPhase::APPROX_GATHER_GLOBAL_STATS;
    double split_extra_ratio = gather_helper.running_monitor_.get_monitor_extra_progress_ratio(current_phase, next_phase, total_split_cnt);
    double extra_ratio = 0.0;
    do {
      ObSEArray<PartInfo, 4> approx_part_infos;
      for (int64_t i = 0; OB_SUCC(ret) && i < gather_helper.maximum_gather_part_cnt_ && idx_part < param.approx_part_infos_.count(); ++i) {
        if (OB_FAIL(approx_part_infos.push_back(param.approx_part_infos_.at(idx_part++)))) {
          LOG_WARN("failed to push back", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (gather_helper.maximum_gather_col_cnt_ >= param.column_params_.count()) {
          ObSEArray<ObOptTableStat *, 4> all_tstats;
          ObTableStatParam derive_param;
          if (OB_FAIL(derive_param.assign(param))) {
            LOG_WARN("failed to assign", K(ret));
          } else if (OB_FAIL(derive_param.approx_part_infos_.assign(approx_part_infos))) {
            LOG_WARN("failed to assign", K(ret));
          } else {
            derive_param.allocator_ = &allocator;
            if (OB_FAIL(gather_helper.running_monitor_.add_monitor_info(current_phase, extra_ratio))) {
              LOG_WARN("failed to add add monitor info", K(ret));
            } else if (OB_FAIL(ObIncrementalStatEstimator::derive_split_gather_stats(ctx, trans, derive_param, true, true, all_tstats))) {
              LOG_WARN("failed to derive split gather stats", K(ret));
            } else {
              allocator.reuse();//Phased memory release for split gather, in order to reuse memory
            }
          }
        } else {
          int64_t idx_col = 0;
          ObOptTableStat part_Stat;
          ObSEArray<ObOptTableStat *, 1> all_tstats;
          if (OB_FAIL(all_tstats.push_back(&part_Stat))) {
            LOG_WARN("faile to push back", K(ret));
          } else {
            do {
              ObSEArray<ObColumnStatParam, 4> gather_column_params;
              for (int64_t i = 0; OB_SUCC(ret) && i < gather_helper.maximum_gather_col_cnt_ && idx_col < param.column_params_.count(); ++i) {
                if (OB_FAIL(gather_column_params.push_back(param.column_params_.at(idx_col++)))) {
                  LOG_WARN("failed to push back", K(ret));
                }
              }
              if (OB_SUCC(ret)) {
                ObTableStatParam derive_param;
                if (OB_FAIL(derive_param.assign(param))) {
                  LOG_WARN("failed to assign", K(ret));
                } else if (OB_FAIL(derive_param.approx_part_infos_.assign(approx_part_infos))) {
                  LOG_WARN("failed to assign", K(ret));
                } else if (OB_FAIL(derive_param.column_params_.assign(gather_column_params))) {
                  LOG_WARN("failed to assign", K(ret));
                } else {
                  derive_param.allocator_ = &allocator;
                  if (OB_FAIL(gather_helper.running_monitor_.add_monitor_info(current_phase, extra_ratio))) {
                    LOG_WARN("failed to add add monitor info", K(ret));
                  } else if (OB_FAIL(ObIncrementalStatEstimator::derive_split_gather_stats(ctx, trans,
                                                                                           derive_param, true,
                                                                                           idx_col == param.column_params_.count(),
                                                                                           all_tstats))) {
                    LOG_WARN("failed to derive split gather stats", K(ret));
                  } else {
                    allocator.reuse();//Phased memory release for split gather, in order to reuse memory
                  }
                }
              }
            } while(OB_SUCC(ret) && idx_col < param.column_params_.count());
          }
        }
      }
      extra_ratio += split_extra_ratio;
    } while (OB_SUCC(ret) && idx_part < param.approx_part_infos_.count());
  }
  return ret;
}

int ObDbmsStatsExecutor::split_gather_global_stats(ObExecContext &ctx,
                                                   ObMySQLTransaction &trans,
                                                   const ObTableStatParam &param,
                                                   const PartitionIdBlockMap *partition_id_block_map,
                                                   GatherHelper &gather_helper)
{
  int ret = OB_SUCCESS;
  ObOptStatGatherParam gather_param;
  ObArenaAllocator allocator("SplitGatherStat", OB_MALLOC_NORMAL_BLOCK_SIZE, param.tenant_id_);
  if (OB_UNLIKELY(gather_helper.maximum_gather_col_cnt_ < 1 ||
                  !gather_helper.is_split_gather_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(param), K(gather_helper));
  } else if (OB_FAIL(ObDbmsStatsUtils::prepare_gather_stat_param(param, TABLE_LEVEL, partition_id_block_map, true,
                                                                 gather_helper.gather_vectorize_,
                                                                 gather_helper.use_column_store_,
                                                                 gather_param))) {
    LOG_WARN("failed to prepare gather stat param", K(ret));
  } else {//need split gather
    int64_t idx_col = 0;
    ObOptTableStat global_Stat;
    ObSEArray<ObOptTableStat *, 1> all_tstats;
    if (OB_FAIL(all_tstats.push_back(&global_Stat))) {
      LOG_WARN("faile to push back", K(ret));
    } else if (OB_FAIL(fetch_gather_table_snapshot_read(trans.get_connection(), gather_param.tenant_id_, gather_param.sepcify_scn_))) {
      LOG_WARN("failed to fetch gather table snapshot read", K(ret));
      ret = OB_SUCCESS;//if we failed to get the read snapshot, just skip, not specify the snapshot
    }
    if (OB_SUCC(ret)) {
      int64_t total_split_cnt = std::ceil(1.0 * param.column_params_.count() / gather_helper.maximum_gather_col_cnt_);
      ObOptStatRunningPhase current_phase = gather_helper.is_approx_gather_ && param.part_level_ != share::schema::PARTITION_LEVEL_ZERO ?
                                              ObOptStatRunningPhase::APPROX_GATHER_GLOBAL_STATS : ObOptStatRunningPhase::GATHER_GLOBAL_STATS;
      ObOptStatRunningPhase next_phase = ObOptStatRunningPhase::BACKUP_HISTORY_STATS;
      double split_extra_ratio = gather_helper.running_monitor_.get_monitor_extra_progress_ratio(current_phase, next_phase, total_split_cnt);
      double extra_ratio = 0.0;
      do {
        ObSEArray<ObColumnStatParam, 4> gather_column_params;
        for (int64_t i = 0; OB_SUCC(ret) && i < gather_helper.maximum_gather_col_cnt_ && idx_col < param.column_params_.count(); ++i) {
          if (OB_FAIL(gather_column_params.push_back(param.column_params_.at(idx_col++)))) {
            LOG_WARN("failed to push back", K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          if (gather_helper.is_approx_gather_ && param.part_level_ != share::schema::PARTITION_LEVEL_ZERO) {
            ObTableStatParam derive_param;
            if (OB_FAIL(derive_param.assign(param))) {
              LOG_WARN("failed to assign", K(ret));
            } else if (OB_FAIL(derive_param.column_params_.assign(gather_column_params))) {
              LOG_WARN("failed to assign", K(ret));
            } else {
              derive_param.allocator_ = &allocator;
              if (OB_FAIL(gather_helper.running_monitor_.add_monitor_info(current_phase, extra_ratio))) {
                LOG_WARN("failed to add add monitor info", K(ret));
              } else if (OB_FAIL(ObIncrementalStatEstimator::derive_split_gather_stats(ctx, trans,
                                                                                       derive_param,
                                                                                       false,
                                                                                       idx_col == param.column_params_.count(),
                                                                                       all_tstats))) {
                LOG_WARN("failed to derive split gather stats", K(ret));
              } else {
                allocator.reuse();//Phased memory release for split gather, in order to reuse memory
              }
            }
          } else {
            ObArray<PartInfo> dummy_part_infos;
            ObSEArray<ObOptColumnStat *, 4> all_cstats;
            ObSEArray<ObOptStat, 4> opt_stats;
            gather_param.allocator_ = &allocator;
            if (OB_FAIL(gather_helper.running_monitor_.add_monitor_info(current_phase, extra_ratio))) {
              LOG_WARN("failed to add add monitor info", K(ret));
            } else if (OB_FAIL(do_gather_stats(ctx, trans, gather_param,
                                               dummy_part_infos,
                                               gather_column_params,
                                               idx_col == param.column_params_.count(),
                                               opt_stats,
                                               all_tstats,
                                               all_cstats))) {
              if (gather_param.sepcify_scn_ > 0 &&
                  (ret == OB_TABLE_DEFINITION_CHANGED || OB_SNAPSHOT_DISCARDED == ret)) {
                LOG_WARN("failed to specify snapshot to gather stats, try no specify snapshot to gather stats", K(ret));
                gather_param.sepcify_scn_ = 0;
                allocator.reuse();
                opt_stats.reset();
                all_cstats.reset();
                if (OB_FAIL(do_gather_stats(ctx, trans, gather_param,
                                            dummy_part_infos,
                                            gather_column_params,
                                            idx_col == param.column_params_.count(),
                                            opt_stats,
                                            all_tstats,
                                            all_cstats))) {
                  LOG_WARN("failed to do gather stats", K(ret));
                } else {
                  allocator.reuse();//Phased memory release for split gather, in order to reuse memory
                }
              } else {
                LOG_WARN("failed to do gather stats", K(ret));
              }
            } else {
              allocator.reuse();//Phased memory release for split gather, in order to reuse memory
            }
          }
        }
        extra_ratio += split_extra_ratio;
      } while(OB_SUCC(ret) && idx_col < param.column_params_.count());
    }
  }
  return ret;
}

int ObDbmsStatsExecutor::do_gather_stats(ObExecContext &ctx,
                                         ObMySQLTransaction &trans,
                                         ObOptStatGatherParam &param,
                                         const ObIArray<PartInfo> &gather_partition_infos,
                                         const ObIArray<ObColumnStatParam> &gather_column_params,
                                         bool is_all_columns_gather,
                                         ObIArray<ObOptStat> &opt_stats,
                                         ObIArray<ObOptTableStat *> &all_tstats,
                                         ObIArray<ObOptColumnStat *> &all_cstats)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObOptTableStat *, 4> tmp_all_tstats;
  if (OB_FAIL(THIS_WORKER.check_status())) {
    LOG_WARN("check status failed", KR(ret));
  } else if (OB_FAIL(param.partition_infos_.assign(gather_partition_infos))) {
    LOG_WARN("failed to assign", K(ret));
  } else if (OB_FAIL(param.column_params_.assign(gather_column_params))) {
    LOG_WARN("failed to assign", K(ret));
  } else if (OB_FAIL(ObDbmsStatsGather::gather_stats(ctx, param, opt_stats))) {
    LOG_WARN("failed to gather stats", K(ret));
  } else if (OB_FAIL(ObDbmsStatsUtils::check_all_cols_range_skew(gather_column_params, opt_stats))) {
    LOG_WARN("failed to check all cols range skew", K(ret));
  } else if (OB_FAIL(ObDbmsStatsUtils::calssify_opt_stat(opt_stats,
                                                         tmp_all_tstats,
                                                         all_cstats))) {
    LOG_WARN("failed to calssify opt stat", K(ret));
  } else if (param.is_split_gather_) {//avoid memory use too much, write current gather stats.
    if (OB_FAIL(ObDbmsStatsUtils::merge_split_gather_tab_stats(all_tstats, tmp_all_tstats))) {
      LOG_WARN("failed to merge split gather tab stats", K(ret));
    } else if (OB_FAIL(ObDbmsStatsUtils::split_batch_write(ctx, trans.get_connection(),
                                                           is_all_columns_gather ? all_tstats : tmp_all_tstats,
                                                           all_cstats))) {
      LOG_WARN("failed to split batch write", K(ret));
    } else {/*do nothing*/}
  } else if (OB_FAIL(append(all_tstats,tmp_all_tstats))) {
    LOG_WARN("failed to append", K(ret));
  }
  return ret;
}

//Get the maximum number of partitions and columns for each stat gather and check need split gather
int ObDbmsStatsExecutor::check_need_split_gather(const ObTableStatParam &param,
                                                 GatherHelper &gather_helper)
{
  int ret = OB_SUCCESS;
  int64_t column_cnt = param.get_need_gather_column();
  int64_t partition_cnt = param.subpart_stat_param_.need_modify_ ? param.subpart_infos_.count() :
                            (param.part_stat_param_.need_modify_ ? param.part_infos_.count() + param.approx_part_infos_.count() : 1);
  bool need_histgoram = param.subpart_stat_param_.need_modify_ ? param.subpart_stat_param_.gather_histogram_ :
                            (param.part_stat_param_.need_modify_ ? param.part_stat_param_.gather_histogram_ : param.global_stat_param_.gather_histogram_);
  partition_cnt = partition_cnt == 0 ? 1 : partition_cnt;
  int64_t origin_partition_cnt = partition_cnt;
  int64_t gather_vectorize = DEFAULT_STAT_GATHER_VECTOR_BATCH_SIZE;
  //cache table stat size
  int64_t tab_stat_size = sizeof(ObOptTableStat) * partition_cnt;
  //cache histogram stat size
  int64_t col_histogram_size = need_histgoram ? get_column_histogram_size(param.column_params_) : 0;
  //cache column stat size
  int64_t col_stat_size = (sizeof(ObOptColumnStat) + ObOptColumnStat::NUM_LLC_BUCKET) * partition_cnt * column_cnt + col_histogram_size * partition_cnt;
  //calc stat size
  int64_t calc_stat_size = gather_vectorize * 1000 * column_cnt * param.degree_;
  //max memory used
  int64_t max_memory_used = tab_stat_size + col_stat_size + calc_stat_size;
  //get the max work arena size
  int64_t max_wa_memory_size = MIN_GATHER_WORK_ARANA_SIZE;
  if (OB_FAIL(get_max_work_area_size(param.tenant_id_, max_wa_memory_size))) {
    LOG_WARN("failed to get max work area size", K(ret));
  } else if (OB_UNLIKELY(max_wa_memory_size < MIN_GATHER_WORK_ARANA_SIZE)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(max_wa_memory_size), K(MIN_GATHER_WORK_ARANA_SIZE));
  } else if (max_memory_used <= max_wa_memory_size && (!gather_helper.use_split_part_ || partition_cnt<=1)) {
    gather_helper.maximum_gather_col_cnt_ = column_cnt;
    gather_helper.maximum_gather_part_cnt_ = partition_cnt;
    gather_helper.is_split_gather_ = false;
    gather_helper.gather_vectorize_ = gather_vectorize;
  } else {
    //firstly, split according the partition
    if (gather_helper.use_split_part_) {
      partition_cnt = 1;
      tab_stat_size = sizeof(ObOptTableStat) * partition_cnt;
      col_stat_size = (sizeof(ObOptColumnStat) + ObOptColumnStat::NUM_LLC_BUCKET) * partition_cnt * column_cnt +
                                                            col_histogram_size * partition_cnt;
      max_memory_used = tab_stat_size + col_stat_size + calc_stat_size;
    }
    while (partition_cnt > 1 && max_memory_used > max_wa_memory_size) {
      partition_cnt = partition_cnt / 2;
      tab_stat_size = sizeof(ObOptTableStat) * partition_cnt;
      col_stat_size = (sizeof(ObOptColumnStat) + ObOptColumnStat::NUM_LLC_BUCKET) * partition_cnt * column_cnt +
                                                            col_histogram_size * partition_cnt;
      max_memory_used = tab_stat_size + col_stat_size + calc_stat_size;
    }
    if (max_memory_used <= max_wa_memory_size) {
      gather_helper.maximum_gather_col_cnt_ = column_cnt;
      gather_helper.maximum_gather_part_cnt_ = partition_cnt;
      gather_helper.is_split_gather_ = origin_partition_cnt != partition_cnt;
      gather_helper.gather_vectorize_ = gather_vectorize;
    } else {
      const int64_t MINIMUM_OF_VECTOR_SIZE = 8;
      //secondly, split according the vector size
      while (gather_vectorize > MINIMUM_OF_VECTOR_SIZE && max_memory_used > max_wa_memory_size) {
        gather_vectorize = gather_vectorize / 2;
        calc_stat_size = gather_vectorize * 1000 * column_cnt * param.degree_;
        max_memory_used = tab_stat_size + col_stat_size + calc_stat_size;
      }
      if (max_memory_used <= max_wa_memory_size) {
        gather_helper.maximum_gather_col_cnt_ = column_cnt;
        gather_helper.maximum_gather_part_cnt_ = partition_cnt;
        gather_helper.is_split_gather_ = origin_partition_cnt != partition_cnt;
        gather_helper.gather_vectorize_ = gather_vectorize;
      } else {
        //lastly, split according the column
        while (column_cnt > 1 && max_memory_used > max_wa_memory_size) {
          column_cnt = column_cnt / 2;
          col_histogram_size = col_histogram_size / 2;
          col_stat_size = (sizeof(ObOptColumnStat) + ObOptColumnStat::NUM_LLC_BUCKET) * partition_cnt * column_cnt +
                                                                 col_histogram_size * partition_cnt;
          max_memory_used = tab_stat_size + col_stat_size + calc_stat_size;
        }
        gather_helper.maximum_gather_col_cnt_ = column_cnt;
        gather_helper.maximum_gather_part_cnt_ = partition_cnt;
        gather_helper.is_split_gather_ = true;
        gather_helper.gather_vectorize_ = gather_vectorize;
      }
    }
  }
  LOG_TRACE("succeed to get the maximum num of part and column for stat gather", K(param), K(max_memory_used),
                                         K(max_wa_memory_size), K(tab_stat_size), K(col_histogram_size),
                                         K(col_stat_size), K(calc_stat_size), K(gather_helper));
  if (gather_helper.is_split_gather_) {
    LOG_TRACE("stat gather will use split gather", K(param.degree_), K(max_memory_used),
                                                  K(max_wa_memory_size), K(tab_stat_size),
                                                  K(col_histogram_size), K(col_stat_size),
                                                  K(calc_stat_size), K(gather_helper));
  }
  return ret;
}

int ObDbmsStatsExecutor::get_max_work_area_size(uint64_t tenant_id, int64_t &max_wa_memory_size)
{
  int ret = OB_SUCCESS;
  max_wa_memory_size = 0;
  const ObTenantBase *tenant = NULL;
  if (OB_ISNULL(tenant = MTL_CTX())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else {
    int64_t worker_cnt = std::max(static_cast<const omt::ObTenant *>(tenant)->min_worker_cnt(), 4L);
    max_wa_memory_size = lib::get_tenant_memory_limit(tenant_id) / worker_cnt;
    if (lib::ObMallocAllocator::get_instance() != NULL) {
      ObTenantCtxAllocatorGuard ta = lib::ObMallocAllocator::get_instance()->get_tenant_ctx_allocator(tenant_id, common::ObCtxIds::WORK_AREA);
      max_wa_memory_size = ta->get_limit() / worker_cnt;
    }
    max_wa_memory_size = std::max(MIN_GATHER_WORK_ARANA_SIZE, max_wa_memory_size);
  }
  return ret;
}

int64_t ObDbmsStatsExecutor::get_column_histogram_size(const ObIArray<ObColumnStatParam> &column_params)
{
  int64_t histogram_size = 0;
  for (int64_t i = 0; i < column_params.count(); ++i) {
    if (column_params.at(i).need_basic_stat() && column_params.at(i).bucket_num_ > 1) {
      histogram_size += (sizeof(ObObj) + OPT_STATS_MAX_VALUE_CHAR_LEN) * column_params.at(i).bucket_num_;
    }
  }
  return histogram_size;
}

/**
 * @brief ObDbmsStatsUtils::set_table_stats
 *  set table stats
 * @return
 */
int ObDbmsStatsExecutor::set_table_stats(ObExecContext &ctx,
                                         const ObSetTableStatParam &param)
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  ObArenaAllocator alloc("ObSetTableStats", OB_MALLOC_NORMAL_BLOCK_SIZE, param.table_param_.tenant_id_);
  ObOptTableStat table_stat;
  ObOptStatManager &mgr = ObOptStatManager::get_instance();
  int64_t partition_id = param.table_param_.global_part_id_;
  ObOptTableStat::Key key(param.table_param_.tenant_id_, param.table_param_.table_id_, partition_id);
  StatLevel stat_level = TABLE_LEVEL;
  int64_t stattype = param.table_param_.stattype_;
  if (param.table_param_.subpart_infos_.count() == 1) {
    key.partition_id_ = param.table_param_.subpart_infos_.at(0).part_id_;
    stat_level = SUBPARTITION_LEVEL;
    stattype = param.table_param_.subpart_infos_.at(0).part_stattype_;
  } else if (param.table_param_.part_infos_.count() == 1) {
    key.partition_id_ = param.table_param_.part_infos_.at(0).part_id_;
    stat_level = PARTITION_LEVEL;
    stattype = param.table_param_.part_infos_.at(0).part_stattype_;
  }
  if (OB_FAIL(mgr.get_table_stat(param.table_param_.tenant_id_, key, table_stat))) {
    LOG_WARN("failed to get table stat", K(ret));
  } else {//reset infos
    table_stat.set_table_id(key.table_id_);
    table_stat.set_partition_id(key.partition_id_);
    table_stat.set_object_type(stat_level);
    table_stat.set_stattype_locked(stattype);
    table_stat.set_last_analyzed(0);
  }
  if (OB_SUCC(ret)) {
    ObMySQLTransaction trans;
    //begin trans
    if (OB_FAIL(trans.start(ctx.get_sql_proxy(), param.table_param_.tenant_id_))) {
      LOG_WARN("fail to start transaction", K(ret));
    } else if (OB_FAIL(do_set_table_stats(param, &table_stat))) {
      LOG_WARN("failed to do set table stats", K(ret));
    ////before update, we need record history stats.
    } else if (!param.table_param_.is_temp_table_ &&
               OB_FAIL(ObDbmsStatsHistoryManager::backup_opt_stats(ctx, trans, param.table_param_, ObTimeUtility::current_time()))) {
      LOG_WARN("failed to backup opt stats", K(ret));
    } else if (OB_FAIL(mgr.update_table_stat(param.table_param_.tenant_id_,
                                             trans.get_connection(),
                                             &table_stat,
                                             param.table_param_.is_index_stat_))) {
      LOG_WARN("failed to update table stats", K(ret));
    } else {
      LOG_TRACE("end set table stats", K(param), K(table_stat));
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(trans.end(true))) {
        LOG_WARN("fail to commit transaction", K(ret));
      }
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(false))) {
        LOG_WARN("fail to roll back transaction", K(tmp_ret));
      }
    }
  }
  return ret;
}

/**
 * @brief ObDbmsStatsUtils::set_column_stats
 *  set column stats
 * @return
 */

int ObDbmsStatsExecutor::set_column_stats(ObExecContext &ctx,
                                          const ObSetColumnStatParam &param)
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  ObOptColumnStatHandle col_stat_handle;
  ObOptStatManager &mgr = ObOptStatManager::get_instance();
  ObOptColumnStat::Key key;
  ObSEArray<ObOptColumnStat *, 4> column_stats;
  ObIAllocator *alloc = NULL;
  if (OB_UNLIKELY(param.table_param_.column_params_.count() != 1) ||
      OB_ISNULL(alloc = param.table_param_.allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(param.table_param_.column_params_.count()),
                                     K(param.table_param_.allocator_));
  } else {
    StatLevel stat_level = TABLE_LEVEL;
    key.tenant_id_ = param.table_param_.tenant_id_;
    key.table_id_ = param.table_param_.table_id_;
    key.column_id_ = param.table_param_.column_params_.at(0).column_id_;
    int64_t partition_id = param.table_param_.global_part_id_;
    key.partition_id_ = partition_id;
    if (param.table_param_.subpart_infos_.count() == 1) {
      key.partition_id_ = param.table_param_.subpart_infos_.at(0).part_id_;
      stat_level = SUBPARTITION_LEVEL;
    } else if (param.table_param_.part_infos_.count() == 1) {
      key.partition_id_ = param.table_param_.part_infos_.at(0).part_id_;
      stat_level = PARTITION_LEVEL;
    }
    ObOptColumnStat *col_stat = NULL;
    if (OB_FAIL(mgr.get_column_stat(param.table_param_.tenant_id_, key, col_stat_handle))) {
      LOG_WARN("failed to get column stat", K(ret), K(key));
    } else if (OB_ISNULL(col_stat_handle.stat_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(col_stat_handle.stat_), K(ret));
    } else if (OB_ISNULL(col_stat = ObOptColumnStat::malloc_new_column_stat(*alloc))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to create column stat", K(ret));
    } else if (OB_FAIL(col_stat->deep_copy(*col_stat_handle.stat_))) {
      LOG_WARN("failed to deep copy", K(ret));
    } else {//reset base infos
      col_stat->set_table_id(key.table_id_);
      col_stat->set_partition_id(key.partition_id_);
      col_stat->set_stat_level(stat_level);
      col_stat->set_column_id(key.column_id_);
      col_stat->set_collation_type(param.table_param_.column_params_.at(0).cs_type_);
      col_stat->set_last_analyzed(0);
      if (OB_FAIL(do_set_column_stats(*alloc, ctx.get_my_session()->get_dtc_params(), param, col_stat))) {
        LOG_WARN("failed to do set table stats", K(ret));
      } else if (OB_FAIL(column_stats.push_back(col_stat))) {
        LOG_WARN("failed to push back column stat", K(ret));
      } else {
        ObMySQLTransaction trans;
        if (OB_FAIL(trans.start(ctx.get_sql_proxy(), param.table_param_.tenant_id_))) {
          LOG_WARN("fail to start transaction", K(ret));
        } else if (OB_FAIL(mgr.update_column_stat(ctx.get_virtual_table_ctx().schema_guard_,
                                                  param.table_param_.tenant_id_,
                                                  trans.get_connection(),
                                                  column_stats,
                                                  true,
                                                  CREATE_OBJ_PRINT_PARAM(ctx.get_my_session())))) {
          LOG_WARN("failed to update column stats", K(ret));
        } else {
          LOG_TRACE("end set column stats", K(param), K(*col_stat));
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(trans.end(true))) {
            LOG_WARN("fail to commit transaction", K(ret));
          }
        } else {
          int tmp_ret = OB_SUCCESS;
          if (OB_SUCCESS != (tmp_ret = trans.end(false))) {
            LOG_WARN("fail to roll back transaction", K(tmp_ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObDbmsStatsExecutor::do_set_table_stats(const ObSetTableStatParam &param,
                                            ObOptTableStat *table_stat)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_stat)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(table_stat));
  } else if (param.numrows_ < 0 ||
             param.avgrlen_ < 0 ||
             param.nummacroblks_ < 0 ||
             param.nummicroblks_ < 0) {
    ret = OB_ERR_DBMS_STATS_PL;
    LOG_WARN("Invalid or inconsistent input values", K(ret), K(param));
    LOG_USER_ERROR(OB_ERR_DBMS_STATS_PL,"Invalid or inconsistent input values");
  } else {
    //1.set numrows_
    if (param.numrows_ > 0) {
      table_stat->set_row_count(param.numrows_);
    }
    //2.set numblks_
    // if (param.numblks_ > 0) {
    //   table_stat->set_macro_block_num(param.numrows_);
    // }
    //3.avgrlen_
    if (param.avgrlen_ > 0) {
      table_stat->set_avg_row_size(param.avgrlen_);
    }
    if (param.nummacroblks_ > 0) {
      table_stat->set_macro_block_num(param.nummacroblks_);
    }
    if (param.nummicroblks_ > 0) {
      table_stat->set_micro_block_num(param.nummicroblks_);
    }
    //other options support later.
    LOG_TRACE("succeed to do set table stats", K(*table_stat));
  }
  return ret;
}

int ObDbmsStatsExecutor::do_set_column_stats(ObIAllocator &allocator,
                                             const ObDataTypeCastParams &dtc_params,
                                             const ObSetColumnStatParam &param,
                                             ObOptColumnStat *&column_stat)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(column_stat)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(column_stat));
  } else if (param.distcnt_ < 0 ||
             param.density_ < 0 ||
             param.nullcnt_ < 0 ||
             param.avgclen_ < 0) {
    ret = OB_ERR_DBMS_STATS_PL;
    LOG_WARN("Invalid or inconsistent input values", K(ret), K(param));
    LOG_USER_ERROR(OB_ERR_DBMS_STATS_PL,"Invalid or inconsistent input values");
  } else {
    //1.set distcnt_
    if (param.distcnt_ > 0) {
      column_stat->set_num_distinct(param.distcnt_);
    }
    //2.set density_
    if (param.density_ > 0) {
      column_stat->get_histogram().set_density(param.density_);
    }
    //3.nullcnt_
    if (param.nullcnt_ > 0) {
      column_stat->set_num_null(param.nullcnt_);
    }
    //4.avgclen_
    if (param.avgclen_ > 0) {
      column_stat->set_avg_len(param.avgclen_);
    }
    //5.set max/val value
    if (param.hist_param_.minval_ != NULL || param.hist_param_.maxval_ != NULL) {
      ObCastCtx cast_ctx(&allocator, &dtc_params, CM_NONE, param.col_meta_.get_collation_type());
      if ((param.hist_param_.minval_ != NULL &&
           OB_FAIL(ObObjCaster::to_type(param.col_meta_.get_type(), cast_ctx, *param.hist_param_.minval_, column_stat->get_min_value()))) ||
          (param.hist_param_.maxval_ != NULL &&
           OB_FAIL(ObObjCaster::to_type(param.col_meta_.get_type(), cast_ctx, *param.hist_param_.maxval_, column_stat->get_max_value())))) {
        ret = OB_ERR_DBMS_STATS_PL;
        LOG_WARN("Invalid or inconsistent input values", K(ret), K(param));
        LOG_USER_ERROR(OB_ERR_DBMS_STATS_PL,"Invalid or inconsistent input values");
      }
    }
    //6.set hist_param TODO @jiangxiu.wt
    //other options support later.
    LOG_TRACE("succeed to do set column stats", K(param), K(*column_stat));
  }
  return ret;
}

int ObDbmsStatsExecutor::delete_table_stats(ObExecContext &ctx,
                                            const ObTableStatParam &param,
                                            const bool cascade_columns)
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  ObSEArray<int64_t, 4> part_ids;
  ObSEArray<int64_t, 4> no_stats_partition_ids;
  ObSEArray<uint64_t, 4> part_stattypes;
  uint64_t table_id = param.table_id_;
  if (param.global_stat_param_.need_modify_) {
    if (OB_FAIL(part_ids.push_back(param.global_part_id_))) {
      LOG_WARN("failed to push back partition id", K(ret));
    } else if (param.stattype_ == NULL_TYPE) {
      /*do nothing*/
    } else if (OB_FAIL(no_stats_partition_ids.push_back(param.global_part_id_))) {
      LOG_WARN("failed to push back", K(ret));
    } else if (OB_FAIL(part_stattypes.push_back(param.stattype_))) {
      LOG_WARN("failed to push back", K(ret));
    } else {/*do nothing*/}
  }
  if (OB_SUCC(ret) && param.part_stat_param_.need_modify_) {
    for (int64_t i = 0; OB_SUCC(ret) && i < param.part_infos_.count(); ++i) {
      if (OB_FAIL(part_ids.push_back(param.part_infos_.at(i).part_id_))) {
        LOG_WARN("failed to push back partition id", K(ret));
      } else if (param.part_infos_.at(i).part_stattype_ == NULL_TYPE) {
        /*do nothing*/
      } else if (OB_FAIL(no_stats_partition_ids.push_back(param.part_infos_.at(i).part_id_))) {
        LOG_WARN("failed to push back", K(ret));
      } else if (OB_FAIL(part_stattypes.push_back(param.part_infos_.at(i).part_stattype_))) {
        LOG_WARN("failed to push back", K(ret));
      } else {/*do nothing*/}
    }
  }
  if (OB_SUCC(ret) && param.subpart_stat_param_.need_modify_) {
    for (int64_t i = 0; OB_SUCC(ret) && i < param.subpart_infos_.count(); ++i) {
      if (OB_FAIL(part_ids.push_back(param.subpart_infos_.at(i).part_id_))) {
        LOG_WARN("failed to push back partition id", K(ret));
      } else if (param.subpart_infos_.at(i).part_stattype_ == NULL_TYPE) {
        /*do nothing*/
      } else if (OB_FAIL(no_stats_partition_ids.push_back(param.subpart_infos_.at(i).part_id_))) {
        LOG_WARN("failed to push back", K(ret));
      } else if (OB_FAIL(part_stattypes.push_back(param.subpart_infos_.at(i).part_stattype_))) {
        LOG_WARN("failed to push back", K(ret));
      } else {/*do nothing*/}
    }
  }
  if (OB_SUCC(ret)) {
    int64_t affected_rows = 0;
    //before delete, we need record history stats.
    ObMySQLTransaction trans;
    //begin trans
    if (OB_FAIL(trans.start(ctx.get_sql_proxy(), param.tenant_id_))) {
      LOG_WARN("fail to start transaction", K(ret));
    } else if (!param.is_temp_table_ &&
               OB_FAIL(ObDbmsStatsHistoryManager::backup_opt_stats(ctx, trans, param, ObTimeUtility::current_time()))) {
      LOG_WARN("failed to backup opt stats", K(ret));
    } else if (OB_FAIL(ObOptStatManager::get_instance().delete_table_stat(param.tenant_id_,
                                                                          table_id,
                                                                          part_ids,
                                                                          cascade_columns,
                                                                          param.degree_,
                                                                          affected_rows))) {
      LOG_WARN("failed to delete table stats", K(ret));
    } else if (OB_FAIL(reset_table_locked_state(ctx, param, no_stats_partition_ids, part_stattypes))) {
      LOG_WARN("failed to reset table locked state", K(ret));
    }
    if (OB_SUCC(ret) && affected_rows != 0) {
      if (OB_FAIL(trans.end(true))) {
        LOG_WARN("fail to commit transaction", K(ret));
      }
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(false))) {
        LOG_WARN("fail to roll back transaction", K(tmp_ret));
      }
    }
  }
  return ret;
}

int ObDbmsStatsExecutor::delete_column_stats(ObExecContext &ctx,
                                             const ObTableStatParam &param,
                                             const bool only_histogram)
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  uint64_t table_id = param.table_id_;
  ObSEArray<int64_t, 4> part_ids;
  ObSEArray<uint64_t, 4> column_ids;
  if (OB_UNLIKELY(param.column_params_.count() < 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("one column is expected", K(ret), K(param.column_params_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < param.column_params_.count(); ++i) {
      if (OB_FAIL(column_ids.push_back(param.column_params_.at(i).column_id_))) {
        LOG_WARN("failed to push back", K(ret));
      } else {/*do nothing*/}
    }
    if (OB_SUCC(ret) && param.global_stat_param_.need_modify_) {
      if (OB_FAIL(part_ids.push_back(param.global_part_id_))) {
        LOG_WARN("failed to push back partition id", K(ret));
      } else {/*do nothing*/}
    }
    if (OB_SUCC(ret) && param.part_stat_param_.need_modify_) {
      for (int64_t i = 0; OB_SUCC(ret) && i < param.part_infos_.count(); ++i) {
        if (OB_FAIL(part_ids.push_back(param.part_infos_.at(i).part_id_))) {
          LOG_WARN("failed to push back partition id", K(ret));
        } else {/*do nothing*/}
      }
    }
    if (OB_SUCC(ret) && param.subpart_stat_param_.need_modify_) {
      for (int64_t i = 0; OB_SUCC(ret) && i < param.subpart_infos_.count(); ++i) {
        if (OB_FAIL(part_ids.push_back(param.subpart_infos_.at(i).part_id_))) {
          LOG_WARN("failed to push back partition id", K(ret));
        } else {/*do nothing*/}
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObOptStatManager::get_instance().delete_column_stat(param.tenant_id_,
                                                                    table_id,
                                                                    column_ids,
                                                                    part_ids,
                                                                    only_histogram,
                                                                    param.degree_))) {
      LOG_WARN("failed to delete column stat", K(ret));
    }
  }
  return ret;
}

ObOptStat::~ObOptStat()
{
  if (NULL != table_stat_) {
    // table_stat_->~ObOptTableStat();
    table_stat_ = NULL;
  }
  for (int64_t i = 0; i < column_stats_.count(); ++i) {
    if (NULL != column_stats_.at(i)) {
      // column_stats_.at(i)->~ObOptColumnStat();
      column_stats_.at(i) = NULL;
    }
  }
}

int ObDbmsStatsExecutor::reset_table_locked_state(ObExecContext &ctx,
                                                  const ObTableStatParam &param,
                                                  const ObIArray<int64_t> &no_stats_partition_ids,
                                                  const ObIArray<uint64_t> &part_stattypes)
{
  int ret = OB_SUCCESS;
  ObSqlString insert_sql;
  int64_t affected_rows = 0;
  ObMySQLProxy *mysql_proxy = NULL;
  if (no_stats_partition_ids.empty()) {
    /*do nothing*/
  } else if (OB_ISNULL(mysql_proxy = ctx.get_sql_proxy())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(mysql_proxy));
  } else if (OB_FAIL(ObDbmsStatsLockUnlock::get_insert_locked_type_sql(param,
                                                                       no_stats_partition_ids,
                                                                       part_stattypes,
                                                                       insert_sql))) {
    LOG_WARN("failed to get insert locked type sql", K(ret));
  } else if (OB_FAIL(mysql_proxy->write(param.tenant_id_, insert_sql.ptr(), affected_rows))) {
    LOG_WARN("fail to exec sql", K(insert_sql), K(ret));
  } else {
    LOG_TRACE("Succeed to reset table locked state", K(insert_sql), K(param));
  }
  return ret;
}

int ObDbmsStatsExecutor::gather_index_stats(ObExecContext &ctx,
                                            const ObTableStatParam &param)
{
  int ret = OB_SUCCESS;
  ObArray<ObOptTableStat *> all_index_stats;
  ObArray<ObOptTableStat *> part_index_stats;
  ObArray<ObOptColumnStat *> empty_cstats;
  ObOptStatGatherParam gather_param;
  PartitionIdBlockMap partition_id_block_map;
  bool use_column_store = false;
  bool use_split_part = false;
  LOG_TRACE("begin gather index stats", K(param));
  if (OB_FAIL(partition_id_block_map.create(10000,
                                            ObModIds::OB_HASH_BUCKET_TABLE_STATISTICS,
                                            ObModIds::OB_HASH_BUCKET_TABLE_STATISTICS,
                                            param.tenant_id_))) {
    LOG_WARN("failed to create hash map", K(ret));
  } else if (param.need_estimate_block_ &&
             OB_FAIL(ObBasicStatsEstimator::estimate_block_count(ctx, param,
                                                                 partition_id_block_map,
                                                                 use_column_store,
                                                                 use_split_part))) {
    LOG_WARN("failed to estimate block count", K(ret));
  } else if (OB_FAIL(ObDbmsStatsUtils::prepare_gather_stat_param(param, INVALID_LEVEL, &partition_id_block_map,
                                                                 false, DEFAULT_STAT_GATHER_VECTOR_BATCH_SIZE,
                                                                 use_column_store, gather_param))) {
    LOG_WARN("failed to prepare gather stat param", K(ret));
  } else if (OB_FAIL(gather_param.column_params_.assign(param.column_params_))) {
    LOG_WARN("failed to assign", K(ret));
  }
  if (OB_SUCC(ret) && param.subpart_stat_param_.need_modify_) {
    gather_param.stat_level_ = SUBPARTITION_LEVEL;
    if (param.part_level_ != share::schema::PARTITION_LEVEL_TWO) {
      /*do nothing*/
    } else if (OB_FAIL(gather_param.partition_infos_.assign(param.subpart_infos_))) {
      LOG_WARN("failed to assign", K(ret));
    } else if (OB_FAIL(ObDbmsStatsGather::gather_index_stats(ctx, gather_param, all_index_stats))) {
      LOG_WARN("failed to gather subpart index stats", K(ret));
    } else {/*do nothing*/}
  }
  if (OB_SUCC(ret) && param.part_stat_param_.need_modify_) {
     gather_param.stat_level_ = PARTITION_LEVEL;
    if (param.part_level_ == share::schema::PARTITION_LEVEL_ZERO) {
      /*do nothing*/
    } else if (param.part_stat_param_.can_use_approx_ && !param.approx_part_infos_.empty()) {//approx some part stats base on subpart stats
      if (OB_UNLIKELY(all_index_stats.empty())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(ret), K(param.approx_part_infos_), K(all_index_stats));
      } else if (OB_FAIL(ObIncrementalStatEstimator::derive_part_index_stat_by_subpart_index_stats(param,
                                                                                                   all_index_stats,
                                                                                                   part_index_stats))) {
        LOG_WARN("failed to derive part index stat by subpart index stats", K(ret));
      } else if (OB_FAIL(append(all_index_stats, part_index_stats))) {
        LOG_WARN("failed to append", K(ret));
      }
    } else if (OB_UNLIKELY(!param.approx_part_infos_.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(ret), K(param));
    }
    if (OB_SUCC(ret) && !param.part_infos_.empty()) {
      if (OB_FAIL(gather_param.partition_infos_.assign(param.part_infos_))) {
        LOG_WARN("failed to assign", K(ret));
      } else if (OB_FAIL(ObDbmsStatsGather::gather_index_stats(ctx, gather_param, part_index_stats))) {
        LOG_WARN("failed to gather part index stats", K(ret));
      } else if (OB_FAIL(append(all_index_stats, part_index_stats))) {
        LOG_WARN("failed to append", K(ret));
      } else {/*do nothing*/}
    }
  }
  if (OB_SUCC(ret) && (param.global_stat_param_.need_modify_)) {
    gather_param.stat_level_ = TABLE_LEVEL;
    gather_param.partition_infos_.reset();
    if (param.global_stat_param_.gather_approx_ && !part_index_stats.empty()) {//approx global stats base on part stats
      if (OB_FAIL(ObIncrementalStatEstimator::derive_global_index_stat_by_part_index_stats(param,
                                                                                           part_index_stats,
                                                                                           all_index_stats))) {
        LOG_WARN("failed to derive global index stat by part index stats", K(ret));
      }
    } else if (OB_FAIL(ObDbmsStatsGather::gather_index_stats(ctx, gather_param, all_index_stats))) {
      LOG_WARN("failed to gather index stats", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObDbmsStatsUtils::split_batch_write(ctx, all_index_stats, empty_cstats, true))) {
      LOG_WARN("failed to split batch write", K(ret));
    } else {/*do nothing*/}
  }

  if (partition_id_block_map.created()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = partition_id_block_map.destroy())) {
      LOG_WARN("failed to destroy hash map", K(ret), K(tmp_ret));
      ret = COVER_SUCC(tmp_ret);
    }
  }
  return ret;
}

int ObDbmsStatsExecutor::update_online_stat(ObExecContext &ctx,
                                            ObTableStatParam &param,
                                            share::schema::ObSchemaGetterGuard *schema_guard,
                                            const TabStatIndMap &online_table_stats,
                                            const ColStatIndMap &online_column_stats,
                                            const ObIArray<ObOptDmlStat *> *dml_stats)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("ObOnlineStat", OB_MALLOC_NORMAL_BLOCK_SIZE, param.tenant_id_);
  ObSEArray<ObOptTableStat *, 4> cur_table_stats;
  ObSEArray<ObOptColumnStat *, 4> cur_column_stats;
  ObSEArray<ObOptTableStat *, 4>  table_stats;
  ObSEArray<ObOptColumnStat *, 4> column_stats;
  bool succ_to_write_stats = false;
  if (OB_FAIL(ObDbmsStatsLockUnlock::check_stat_locked(ctx, param))) {
    LOG_WARN("fail to check lock stat", K(ret));
    if (ret == OB_ERR_DBMS_STATS_PL) {
      param.global_stat_param_.reset_gather_stat();
      param.part_stat_param_.reset_gather_stat();
      param.subpart_stat_param_.reset_gather_stat();
      ret = OB_SUCCESS; // ignore lock check error
    }
  } else {
    SMART_VAR(sql::ObSQLSessionInfo::StmtSavedValue, saved_value) {
      int64_t nested_count = -1;
      ObSqlString old_db_name;
      int64_t old_db_id = -1;
      int64_t old_trx_lock_timeout = -1;
      bool need_restore_session = false;
      bool need_reset_default_database = false;
      bool need_reset_trx_lock_timeout = false;
      common::sqlclient::ObISQLConnection *conn = NULL;
      ctx.set_is_online_stats_gathering(true);
      //lib::CompatModeGuard guard(lib::Worker::CompatMode::MYSQL);
      if (OB_FAIL(prepare_conn_and_store_session_for_online_stats(ctx.get_my_session(),
                                                                  ctx.get_sql_proxy(),
                                                                  schema_guard,
                                                                  saved_value,
                                                                  nested_count,
                                                                  old_db_name,
                                                                  old_db_id,
                                                                  old_trx_lock_timeout,
                                                                  need_restore_session,
                                                                  need_reset_default_database,
                                                                  need_reset_trx_lock_timeout,
                                                                  conn))) {
        LOG_WARN("failed to prepare conn and store session for online stats", K(ret));
      } else if (nullptr != dml_stats && ObOptStatMonitorManager::update_dml_stat_info_from_direct_load(*dml_stats, conn)) {
        LOG_WARN("fail to update dml stat info", K(ret));
      } else if (OB_FAIL(ObDbmsStatsUtils::get_current_opt_stats(allocator,
                                                                 conn,
                                                                 param,
                                                                 cur_table_stats,
                                                                 cur_column_stats))) {
        LOG_WARN("failed to get current opt stats", K(ret));
      } else if (OB_FAIL(ObDbmsStatsUtils::merge_tab_stats(param,
                                                           online_table_stats,
                                                           cur_table_stats,
                                                           table_stats))) {
        LOG_WARN("fail to merge tab stats", K(ret), K(cur_table_stats));
      } else if (OB_FAIL(ObDbmsStatsUtils::merge_col_stats(param,
                                                           online_column_stats,
                                                           cur_column_stats,
                                                           column_stats))) {
        LOG_WARN("fail to merge col stats", K(ret), K(cur_column_stats));
      } else if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_3_2_0 &&
                 OB_FAIL(ObDbmsStatsUtils::scale_col_stats(param.tenant_id_,
                                                           table_stats,
                                                           column_stats))) {
        LOG_WARN("failed to scale col stats", K(ret));
      } else if (OB_FAIL(ObDbmsStatsUtils::split_batch_write(ctx, conn, table_stats, column_stats, false, true))) {
        LOG_WARN("fail to update stat", K(ret), K(table_stats), K(column_stats));
      } else if (OB_FAIL(ObBasicStatsEstimator::update_last_modified_count(conn, param))) {
        LOG_WARN("failed to update last modified count", K(ret));
      } else {
        succ_to_write_stats = true;
      }
      if (ret == OB_ERR_EXCLUSIVE_LOCK_CONFLICT || ret == OB_ERR_SHARED_LOCK_CONFLICT) {
        ret = OB_SUCCESS;
        LOG_INFO("update online stats occur lock conflict, just skip");
      }
      //release source
      //guard.~CompatModeGuard();
      ctx.set_is_online_stats_gathering(false);
      if (OB_NOT_NULL(conn)) {
        int tmp_ret = OB_SUCCESS;
        ctx.get_sql_proxy()->close(conn, tmp_ret);
        ret = COVER_SUCC(tmp_ret);
      }
      //restore session
      if (need_restore_session) {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = restore_session_for_online_stat(ctx.get_my_session(),
                                                                     saved_value,
                                                                     nested_count,
                                                                     old_db_name,
                                                                     old_db_id,
                                                                     old_trx_lock_timeout,
                                                                     need_reset_default_database,
                                                                     need_reset_trx_lock_timeout))) {
          ret = COVER_SUCC(tmp_ret);
          LOG_WARN("failed to restore session", K(tmp_ret));
        }
      }
      //update stat cache
      if (succ_to_write_stats) {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = pl::ObDbmsStats::update_stat_cache(ctx.get_my_session()->get_rpc_tenant_id(), param))) {
          ret = COVER_SUCC(tmp_ret);
          LOG_WARN("fail to update stat cache", K(tmp_ret));
        }
      }
    }
  }
  return ret;
}

int ObDbmsStatsExecutor::prepare_conn_and_store_session_for_online_stats(sql::ObSQLSessionInfo *session,
                                                                         common::ObMySQLProxy *sql_proxy,
                                                                         share::schema::ObSchemaGetterGuard *schema_guard,
                                                                         sql::ObSQLSessionInfo::StmtSavedValue &saved_value,
                                                                         int64_t &nested_count,
                                                                         ObSqlString &old_db_name,
                                                                         int64_t &old_db_id,
                                                                         int64_t &old_trx_lock_timeout,
                                                                         bool &need_restore_session,
                                                                         bool &need_reset_default_database,
                                                                         bool &need_reset_trx_lock_timeout,
                                                                         sqlclient::ObISQLConnection *&conn)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session) || OB_ISNULL(sql_proxy) || OB_ISNULL(schema_guard)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(session), K(sql_proxy));
  //1.save session info
  } else if (OB_FAIL(session->save_session(saved_value))) {
    LOG_WARN("failed to saved value", K(ret));
  } else {
    need_restore_session = true;
    nested_count = session->get_nested_count();
    //2.modify seesion info
    //2.1 modify query start time
    session->set_query_start_time(ObTimeUtility::current_time());
    session->set_inner_session();
    session->set_nested_count(-1);
    if (OB_FAIL(old_db_name.append(session->get_database_name()))) {
      LOG_WARN("failed to append", K(ret));
    } else {
      old_db_id = session->get_database_id();
      //2.2 modify seesion compatible mode
      ObObj mysql_mode;
      mysql_mode.set_int(0);
      if (OB_FAIL(session->update_sys_variable(share::SYS_VAR_OB_COMPATIBILITY_MODE, mysql_mode))) {
        LOG_WARN("failed to update sys variable for compatibility mode", K(ret));
      } else {
        //2.3.modify session database name and database id
        const share::schema::ObDatabaseSchema *db_schema = NULL;
        if (OB_FAIL(schema_guard->get_database_schema(MTL_ID(), OB_SYS_DATABASE_ID, db_schema))) {
          LOG_WARN("failed to get database schema", K(ret));
        } else if (OB_ISNULL(db_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected error", K(ret), K(db_schema));
        } else if (OB_FAIL(session->set_default_database(db_schema->get_database_name_str()))) {
          LOG_WARN("faile to set default database", K(ret));
        } else {
          session->set_database_id(db_schema->get_database_id());
          need_reset_default_database = true;
          //2.3 modify session trx lock timeout
          old_trx_lock_timeout = session->get_trx_lock_timeout();
          ObObj trx_lock_timeout;
          trx_lock_timeout.set_int(0);
          if (OB_FAIL(session->update_sys_variable(share::SYS_VAR_OB_TRX_LOCK_TIMEOUT, trx_lock_timeout))) {
            LOG_WARN("failed to update sys variable for trx lock timeout", K(ret));
          } else {
            need_reset_trx_lock_timeout = true;
            //3.get conn to update stats
            observer::ObInnerSQLConnectionPool *pool = NULL;
            if (OB_ISNULL(pool = static_cast<observer::ObInnerSQLConnectionPool *>(sql_proxy->get_pool()))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("get unexpected error", K(ret), K(pool));
            } else if (OB_FAIL(pool->acquire(session, conn))) {
              LOG_WARN("failed to acquire conn", K(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObDbmsStatsExecutor::restore_session_for_online_stat(sql::ObSQLSessionInfo *session,
                                                         sql::ObSQLSessionInfo::StmtSavedValue &saved_value,
                                                         int64_t nested_count,
                                                         ObSqlString &old_db_name,
                                                         int64_t old_db_id,
                                                         int64_t old_trx_lock_timeout,
                                                         bool need_reset_default_database,
                                                         bool need_reset_trx_lock_timeout)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(session));
  } else {
    //1.restore sesssion
    if (OB_FAIL(session->restore_session(saved_value))) {
      LOG_WARN("failed to restore session", K(ret), K(session));
    } else {
      session->set_nested_count(nested_count);
    }
    //2.restore seesion compatible oracle mode
    if (lib::is_oracle_mode()) {
      ObObj oracle_mode;
      oracle_mode.set_int(1);
      int tmp_ret = session->update_sys_variable(share::SYS_VAR_OB_COMPATIBILITY_MODE, oracle_mode);
      if (tmp_ret != OB_SUCCESS) {
        ret = COVER_SUCC(tmp_ret);
        LOG_WARN("failed to update sys variable for compatibility mode", K(tmp_ret));
      }
    }
    //3.restore session database name and database id
    if (need_reset_default_database) {
      int tmp_ret = session->set_default_database(old_db_name.string());
      if (tmp_ret != OB_SUCCESS) {
        ret = COVER_SUCC(tmp_ret);
        LOG_WARN("failed to reset default database", K(tmp_ret), K(old_db_name));
      } else {
        session->set_database_id(old_db_id);
      }
    }
    //4.restore trx lock timeout
    if (need_reset_trx_lock_timeout) {
      ObObj trx_lock_timeout;
      trx_lock_timeout.set_int(old_trx_lock_timeout);
      int tmp_ret = session->update_sys_variable(share::SYS_VAR_OB_TRX_LOCK_TIMEOUT, trx_lock_timeout);
      if (tmp_ret != OB_SUCCESS) {
        ret = COVER_SUCC(tmp_ret);
        LOG_WARN("failed to update sys variable for trx lock timeout", K(tmp_ret));
      }
    }
  }
  return ret;
}

int ObDbmsStatsExecutor::fetch_gather_table_snapshot_read(common::sqlclient::ObISQLConnection *conn,
                                                          uint64_t tenant_id,
                                                          uint64_t &current_scn)
{
  int ret = OB_SUCCESS;
  const char *sql_str_fmt = "SELECT current_scn() FROM dual";
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    sqlclient::ObMySQLResult *result = NULL;
    if (OB_ISNULL(conn)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(ret), K(conn));
    } else if (OB_FAIL(conn->execute_read(tenant_id, sql_str_fmt, res))) {
      LOG_WARN("faield to exec read", K(ret));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to execute sql", K(ret));
    } else {
      ObObj value;
      int64_t col_idx = 0;
      if (OB_FAIL(result->next())) {
        LOG_WARN("failed to get next row", K(ret));
      } else if (OB_FAIL(result->get_obj(col_idx, value))) {
        LOG_WARN("get obj failed", K(ret));
      } else if (value.is_number()) {
        number::ObNumber scn_num;
        if (OB_FAIL(value.get_number(scn_num))) {
          LOG_WARN("get number failed", K(ret));
        } else if (OB_UNLIKELY(!scn_num.is_valid_uint64(current_scn))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected value value", K(ret), K(value));
        }
      } else if (OB_LIKELY(value.is_uint64())) {
        current_scn = value.get_uint64();
      } else if (value.is_int()) {
        current_scn = value.get_int();
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected value type", K(ret), K(value));
      }
    }
  }
  LOG_TRACE("succeed to fetch gather table snapshot read", K(current_scn));
  return ret;
}

int ObDbmsStatsExecutor::cancel_gather_stats(ObExecContext &ctx, ObString &task_id)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = ctx.get_my_session();
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(session));
  } else {
    const common::ObAddr &local_addr = ctx.get_addr();
    uint64_t tenant_id = session->get_effective_tenant_id();
    char *task_ip = NULL;
    int32_t task_port = 0;
    ObArenaAllocator allocator("CancelGather", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id);
    if (OB_FAIL(fetch_gather_task_addr(ctx.get_sql_proxy(), allocator, tenant_id,
                                       task_id, task_ip, task_port))) {
      LOG_WARN("failed to fetch gather task addr", K(ret));
    } else {
      common::ObAddr rpc_addr(static_cast<common::ObAddr::VER>(local_addr.get_version()), task_ip, task_port);
      if (local_addr == rpc_addr) {//local
        if (OB_FAIL(ObOptStatGatherStatList::instance().cancel_gather_stats(tenant_id,
                                                                            task_id))) {
          LOG_WARN("failed to cancel gather stats", K(ret));
        } else {/*do nothing*/}
      } else {//remote
        int64_t timeout = std::min(10000000L, THIS_WORKER.get_timeout_remain());
        obrpc::ObCancelGatherStatsArg arg;
        arg.tenant_id_ = tenant_id;
        arg.task_id_ = task_id;
        if (OB_UNLIKELY(0 >= timeout)) {
          ret = OB_TIMEOUT;
          LOG_WARN("query timeout is reached", K(ret), K(timeout));
        } else if (OB_FAIL(GCTX.srv_rpc_proxy_->to(rpc_addr)
                                                  .timeout(timeout)
                                                  .by(tenant_id)
                                                  .cancel_gather_stats(arg))) {
          LOG_WARN("failed to cancel gather stats",  K(ret), K(rpc_addr), K(arg));
        } else {/*do nothing*/}
      }
    }
  }
  return ret;
}

int ObDbmsStatsExecutor::fetch_gather_task_addr(ObCommonSqlProxy *sql_proxy,
                                                ObIAllocator &allcoator,
                                                uint64_t tenant_id,
                                                const ObString &task_id,
                                                char *&svr_ip,
                                                int32_t &svr_port)
{
  int ret = OB_SUCCESS;
  ObSqlString raw_sql;
  if (OB_FAIL(raw_sql.append_fmt("SELECT svr_ip, svr_port FROM %s WHERE task_id = \'%.*s\'",
                                 share::OB_ALL_VIRTUAL_OPT_STAT_GATHER_MONITOR_TNAME,
                                 task_id.length(),
                                 task_id.ptr()))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, proxy_result) {
      sqlclient::ObMySQLResult *client_result = NULL;
      ObSQLClientRetryWeak sql_client_retry_weak(sql_proxy);
      if (OB_FAIL(sql_client_retry_weak.read(proxy_result, tenant_id, raw_sql.ptr()))) {
        LOG_WARN("failed to execute sql", K(ret), K(raw_sql));
      } else if (OB_ISNULL(client_result = proxy_result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to execute sql", K(ret));
      } else {
        bool got_result = false;
        while (OB_SUCC(ret) && OB_SUCC(client_result->next())) {
          if (OB_UNLIKELY(got_result)) {//only one rows
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected error", K(ret), K(task_id), K(task_id), K(svr_port));
          } else {
            got_result = true;
            int64_t idx1 = 0;
            int64_t idx2 = 1;
            ObObj obj;
            ObString str;
            int64_t tmp_val;
            if (OB_FAIL(client_result->get_obj(idx1, obj))) {
              LOG_WARN("failed to get object", K(ret));
            } else if (OB_FAIL(obj.get_string(str))) {
              LOG_WARN("failed to get int", K(ret), K(obj));
            } else if (OB_FAIL(client_result->get_obj(idx2, obj))) {
              LOG_WARN("failed to get object", K(ret));
            } else if (OB_FAIL(obj.get_int(tmp_val))) {
              LOG_WARN("failed to get int", K(ret), K(obj));
            } else {
              svr_port = static_cast<int32_t>(tmp_val);
            }
            if (OB_SUCC(ret) && !str.empty()) {
              if (OB_ISNULL(svr_ip = static_cast<char*>(allcoator.alloc(str.length() + 1)))) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                LOG_WARN("failed to alloc memory for saved session value", K(ret), K(svr_ip));
              } else {
                MEMSET(svr_ip, 0, str.length() + 1);
                MEMCPY(svr_ip, str.ptr(), str.length());
                LOG_TRACE("succeed to fetch gather task addr", K(str), K(svr_port));
              }
            }
          }
        }
        ret = OB_ITER_END == ret ? OB_SUCCESS : ret;
        if (OB_SUCC(ret) && !got_result) {//invalid task id
          ret = OB_ERR_DBMS_STATS_PL;
          LOG_WARN("The optimizer stats gather task has ended or the task doesn't exist", K(ret), K(task_id));
          LOG_USER_ERROR(OB_ERR_DBMS_STATS_PL, "The optimizer stats gather task has ended or the task doesn't exist");
        }
      }
      int tmp_ret = OB_SUCCESS;
      if (NULL != client_result) {
        if (OB_SUCCESS != (tmp_ret = client_result->close())) {
          LOG_WARN("close result set failed", K(ret), K(tmp_ret));
          ret = COVER_SUCC(tmp_ret);
        }
      }
    }
  }
  return ret;
}

int ObDbmsStatsExecutor::gather_system_stats(ObExecContext &ctx, int64_t tenant_id)
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  int64_t cpu_mhz = OBSERVER.get_cpu_frequency_khz()/1000;
  int64_t network_speed = OBSERVER.get_network_speed() / 1024.0 / 1024.0;
  int64_t disk_seq_read_speed = 0;
  int64_t disk_rnd_read_speed = 0;
  OptSystemIoBenchmark &io_benchmark = OptSystemIoBenchmark::get_instance();
  if (io_benchmark.is_init()) {
    disk_seq_read_speed = io_benchmark.get_disk_seq_read_speed();
    disk_rnd_read_speed = io_benchmark.get_disk_rnd_read_speed();
  } else if (OB_FAIL(io_benchmark.run_benchmark(ctx.get_allocator()))) {
    LOG_WARN("failed to run io benchmark", K(ret));
  } else {
    disk_seq_read_speed = io_benchmark.get_disk_seq_read_speed();
    disk_rnd_read_speed = io_benchmark.get_disk_rnd_read_speed();
  }
  if (OB_SUCC(ret)) {
    ObOptSystemStat system_stat;
    ObOptStatManager &mgr = ObOptStatManager::get_instance();
    int64_t current_time = ObTimeUtility::current_time();
    system_stat.set_last_analyzed(current_time);
    system_stat.set_cpu_speed(cpu_mhz);
    system_stat.set_disk_seq_read_speed(disk_seq_read_speed);
    system_stat.set_disk_rnd_read_speed(disk_rnd_read_speed);
    system_stat.set_network_speed(network_speed);
    if (OB_FAIL(mgr.update_system_stats(tenant_id,
                                       &system_stat))) {
      LOG_WARN("failed to update system stats", K(ret));
    }
  }
  return ret;
}

int ObDbmsStatsExecutor::delete_system_stats(ObExecContext &ctx, int64_t tenant_id)
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  ObOptStatManager &mgr = ObOptStatManager::get_instance();
  if (OB_FAIL(mgr.delete_system_stats(tenant_id))) {
    LOG_WARN("failed to delete system stats", K(ret));
  }
  return ret;
}

int ObDbmsStatsExecutor::set_system_stats(ObExecContext &ctx, const ObSetSystemStatParam &param)
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  ObOptSystemStat system_stat;
  ObOptStatManager &mgr = ObOptStatManager::get_instance();
  if (OB_FAIL(mgr.get_system_stat(param.tenant_id_, system_stat))) {
    LOG_WARN("failed to get table stat", K(ret));
  } else if (ObCharset::case_insensitive_equal(param.name_, "cpu_speed")) {
    system_stat.set_cpu_speed(param.value_);
  } else if (ObCharset::case_insensitive_equal(param.name_, "disk_seq_read_speed")) {
    system_stat.set_disk_seq_read_speed(param.value_);
  } else if (ObCharset::case_insensitive_equal(param.name_, "disk_rnd_read_speed")) {
    system_stat.set_disk_rnd_read_speed(param.value_);
  } else if (ObCharset::case_insensitive_equal(param.name_, "network_speed")) {
    system_stat.set_network_speed(param.value_);
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(mgr.update_system_stats(param.tenant_id_,
                                            &system_stat))) {
    LOG_WARN("failed to update system stats", K(ret));
  } else {
    LOG_TRACE("end set system stats", K(param), K(system_stat));
  }
  return ret;
}

} // namespace common
} // namespace oceanbase

