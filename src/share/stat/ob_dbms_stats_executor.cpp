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
#include "share/stat/ob_incremental_stat_estimator.h"
#include "share/stat/ob_dbms_stats_history_manager.h"
#include "share/stat/ob_dbms_stats_lock_unlock.h"
#include "share/stat/ob_index_stats_estimator.h"
#include "pl/sys_package/ob_dbms_stats.h"
#include "share/stat/ob_dbms_stats_gather.h"
#include "observer/omt/ob_tenant.h"
#include "src/observer/ob_server.h"
#include "src/share/stat/ob_opt_stat_monitor_manager.h"
#include "src/share/stat/ob_opt_stat_manager.h"
#include "lib/random/ob_random.h"
#include "share/stat/ob_opt_stat_manager.h"
#include "sql/optimizer/ob_optimizer_util.h"

namespace oceanbase {
using namespace pl;
namespace common {
ERRSIM_POINT_DEF(ERRSIM_RANDOM_GATHER_STATS_OPTION);
ERRSIM_POINT_DEF(ERRSIM_FAILED_ANALYZE_TABLE_STATS);
ERRSIM_POINT_DEF(ERRSIM_FAILED_ANALYZE_TIMEOUT);

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
  PartitionIdSkipRateMap partition_id_skip_rate_map;
  GatherHelper gather_helper(running_monitor);
  ObMySQLTransaction backup_trans;
  if (OB_FAIL(backup_trans.start(ctx.get_sql_proxy(), param.tenant_id_))) {
    LOG_WARN("fail to start transaction", K(ret));
  } else if (OB_FAIL(running_monitor.add_monitor_info(ObOptStatRunningPhase::BACKUP_HISTORY_STATS))) {
    LOG_WARN("failed to add add monitor info", K(ret));
  } else if (!param.is_temp_table_ && !param.is_async_gather_ &&
             OB_FAIL(ObDbmsStatsHistoryManager::backup_opt_stats(
                 ctx, backup_trans, param, ObTimeUtility::current_time(), true))) {
    LOG_WARN("failed to backup opt stats", K(ret));
  } else if (OB_FAIL(
                 prepare_gather_stats(ctx, param, partition_id_block_map, partition_id_skip_rate_map, gather_helper))) {
    LOG_WARN("failed to prepare gather stats", K(ret));
  } else if (OB_FAIL(gather_partition_stats(ctx,
                                            param,
                                            &partition_id_block_map,
                                            &partition_id_skip_rate_map,
                                            gather_helper,
                                            running_monitor.failed_part_ids_))) {
    LOG_WARN("failed to gather gather stats", K(ret));
    int tmp_ret = OB_SUCCESS;
     ObSEArray<int64_t, 8> temp_failed_part_ids;
    if (OB_SUCCESS != (tmp_ret = collect_last_part_and_global_if_timeout(ctx,
                                                                         param,
                                                                         &partition_id_block_map,
                                                                         &partition_id_skip_rate_map,
                                                                         gather_helper,
                                                                         temp_failed_part_ids))) {
      LOG_WARN("fail to collect last part stats", K(tmp_ret));
    }
  }

  // end backup trans
  if (OB_SUCC(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = OB_FAIL(backup_trans.end(true)))) {
      LOG_WARN("fail to commit transaction", K(tmp_ret));
    }
  } else {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = backup_trans.end(false))) {
      LOG_WARN("fail to roll back transaction", K(tmp_ret));
    }
  }
  return ret;
}

int ObDbmsStatsExecutor::update_dml_modified_info(sqlclient::ObISQLConnection *conn, const ObTableStatParam &param)
{
  int ret = OB_SUCCESS;
  sql::ObSQLSessionInfo *origin_session = THIS_WORKER.get_session();
  int64_t origin_timeout = THIS_WORKER.get_timeout_ts();
  THIS_WORKER.set_session(NULL);
  const int64_t MAX_UPDATE_OPT_GATHER_STAT_TIMEOUT = 10000000;  // default 10 seconds
  THIS_WORKER.set_timeout_ts(MAX_UPDATE_OPT_GATHER_STAT_TIMEOUT + ObTimeUtility::current_time());
  if (OB_FAIL(ObBasicStatsEstimator::update_last_modified_count(conn, param))) {
    LOG_WARN("failed to update last modified count", K(ret));
  }
  THIS_WORKER.set_session(origin_session);
  THIS_WORKER.set_timeout_ts(origin_timeout);
  return ret;
}

int ObDbmsStatsExecutor::gather_partition_stats(ObExecContext &ctx,
                                                const ObTableStatParam &param,
                                                const PartitionIdBlockMap *partition_id_block_map,
                                                const PartitionIdSkipRateMap *partition_id_skip_rate_map,
                                                GatherHelper &gather_helper,
                                                ObIArray<int64_t> &failed_part_ids)
{
  int ret = OB_SUCCESS;
  LOG_TRACE("begin to gather table stats", K(param));
  typedef ObSEArray<TaskColumnParam, 2> TaskColumnParamInfo;
  typedef ObSEArray<GatherPartInfos, 2> BatchTaskPartInfo;
  SMART_VARS_2((BatchTaskPartInfo, batch_part_infos),(TaskColumnParamInfo, batch_task_col_infos))
  {
    if (OB_FAIL(split_table_part_param(param, gather_helper, batch_part_infos))) {
      LOG_WARN("failed to prepare gather stat param", K(ret));
    } else if (OB_FAIL(split_column_param(param, gather_helper, batch_task_col_infos))) {
      LOG_WARN("failed to split column param", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < batch_part_infos.count(); i++) {
        SMART_VAR(ObTableStatParam, derive_param)
        {
          ObMySQLTransaction gather_trans;
          GatherPartInfos &taskInfo = batch_part_infos.at(i);
          // init partition stats_analyze_task param
          if (OB_FAIL(gather_trans.start(ctx.get_sql_proxy(), param.tenant_id_))) {
            LOG_WARN("fail to start transaction", K(ret));
          } else if ( i == 0 && param.use_part_derive_global_ && OB_FAIL(ObOptStatManager::get_instance().update_stats_internal_stat(param.tenant_id_, gather_trans.get_connection(), param.table_id_, param.global_part_id_))) {
            LOG_WARN("failed to update stats internal stat", K(ret));
          } else if (OB_FAIL(derive_param.assign(param))) {
            LOG_WARN("failed to assign", K(ret));
          } else if (OB_FAIL(derive_param.subpart_infos_.assign(taskInfo.sub_part_infos_))) {
            LOG_WARN("failed to assign", K(ret));
          } else if (!taskInfo.approx_gather_ && OB_FAIL(derive_param.part_infos_.assign(taskInfo.part_infos_))) {
            LOG_WARN("failed to assign", K(ret));
          } else if (taskInfo.approx_gather_ && OB_FAIL(derive_param.approx_part_infos_.assign(taskInfo.part_infos_))) {
            LOG_WARN("failed to assign", K(ret));
          } else {
            derive_param.global_stat_param_.need_modify_ = taskInfo.gather_global_;
            if (ERRSIM_FAILED_ANALYZE_TABLE_STATS) {
              ret = ERRSIM_FAILED_ANALYZE_TABLE_STATS;
              LOG_ERROR("ERRSIM: failed to ANALYZE table stats", K(ret));
            } else if (i > 0 && ERRSIM_FAILED_ANALYZE_TIMEOUT) {
              ret = OB_TIMEOUT;
              THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() + 10);
              LOG_ERROR("ERRSIM: failed to ANALYZE timeout", K(ret));
            }
          }
          if (!OB_SUCC(ret)) {
          } else if (OB_FAIL(do_split_gather_stats(ctx,
                                                   gather_trans,
                                                   taskInfo,
                                                   batch_task_col_infos,
                                                   derive_param,
                                                   partition_id_block_map,
                                                   partition_id_skip_rate_map,
                                                   gather_helper))) {
            LOG_WARN("failed to gather statts", K(ret));
          } else if (share::schema::ObTableType::EXTERNAL_TABLE != param.ref_table_type_ &&
                     OB_FAIL(update_dml_modified_info(gather_trans.get_connection(), derive_param))) {
            LOG_WARN("failed to update dml info count", K(ret));
          }

          // end gather trans
          if (OB_SUCC(ret)) {
            int tmp_ret = OB_SUCCESS;
            if (OB_SUCCESS != (tmp_ret = OB_FAIL(gather_trans.end(true)))) {
              LOG_WARN("fail to commit transaction", K(tmp_ret));
            }
          } else {
            int tmp_ret = OB_SUCCESS;
            if (OB_SUCCESS != (tmp_ret = gather_trans.end(false))) {
              LOG_WARN("fail to roll back transaction", K(tmp_ret));
            }
          }

          if (OB_FAIL(ret)) {
            int temp_ret = ret;
            if (OB_FAIL(collect_executed_part_ids(derive_param, failed_part_ids))) {
              LOG_WARN("collect executed part-ids failed", K(ret));
            }
            if (temp_ret == OB_TIMEOUT && i > 0) {
              if (OB_FAIL(collect_last_parts(derive_param, gather_helper))) {
                LOG_WARN("failed collect table stats", K(ret));
              }
            }
            ret = temp_ret;
          }
          // Try to ensure that the stat of a partition are collected in a snapshot
          gather_helper.sepcify_scn_ = 0;
          gather_helper.is_split_column_ = false;
          gather_helper.is_all_col_gathered_ = false;
        }
      }
    }
  }
  return ret;
}

int ObDbmsStatsExecutor::collect_last_parts(const ObTableStatParam &param, GatherHelper &gather_helper)
{
  int ret = OB_SUCCESS;
  if (param.subpart_stat_param_.need_modify_ || param.part_stat_param_.need_modify_) {
    if (!param.approx_part_infos_.empty()) {
      if (OB_FAIL(gather_helper.lasted_collect_parts_.push_back(param.approx_part_infos_.at(0)))) {
        LOG_WARN("failed to push back part_info", K(ret));
      }
    } else if (!param.part_infos_.empty()) {
      if (OB_FAIL(gather_helper.lasted_collect_parts_.push_back(param.part_infos_.at(0)))) {
        LOG_WARN("failed to push back part_info", K(ret));
      }
    }
  }

  return ret;
}

int ObDbmsStatsExecutor::collect_last_part_and_global_if_timeout(
    ObExecContext &ctx,
    const ObTableStatParam &origin_param,
    const PartitionIdBlockMap *partition_id_block_map,
    const PartitionIdSkipRateMap *partition_id_skip_rate_map,
    GatherHelper &gather_helper,
    ObIArray<int64_t> &failed_part_ids)
{
  int ret = OB_SUCCESS;
  if (!gather_helper.lasted_collect_parts_.empty()) {
    ObTableStatParam temp_stat_param;
    if (OB_FAIL(temp_stat_param.assign(origin_param))) {
      LOG_WARN("failed to assign", K(ret));
    } else {
      temp_stat_param.subpart_stat_param_.need_modify_ = false;
      temp_stat_param.subpart_infos_.reset();
      if (origin_param.subpart_stat_param_.need_modify_) {  // handle subpart table
        if (!origin_param.approx_part_infos_.empty()) {
          temp_stat_param.approx_part_infos_.reset();
          if (OB_FAIL(temp_stat_param.approx_part_infos_.assign(gather_helper.lasted_collect_parts_))) {
            LOG_WARN("failed to push back part_info", K(ret));
          } else {
            temp_stat_param.part_stat_param_.need_modify_ = true;
          }
        } else if (!origin_param.part_infos_.empty()) {
          temp_stat_param.part_infos_.reset();
          if (OB_FAIL(temp_stat_param.part_infos_.assign(gather_helper.lasted_collect_parts_))) {
            LOG_WARN("failed to push back part_info", K(ret));
          } else {
            temp_stat_param.part_stat_param_.need_modify_ = true;
          }
        } else {
          temp_stat_param.part_infos_.reset();
          temp_stat_param.approx_part_infos_.reset();
          temp_stat_param.part_stat_param_.need_modify_ = false;
        }
      }

      if (origin_param.part_stat_param_.need_modify_ || origin_param.global_stat_param_.need_modify_) {
        temp_stat_param.global_stat_param_ = origin_param.global_stat_param_;
      }
    }
    temp_stat_param.duration_time_ = temp_stat_param.duration_time_ > 0
                                         ? std::min(temp_stat_param.duration_time_ / 100, 10000000L)
                                         : 10000000L;  // extra 10s
    int64_t origin_duration_time = THIS_WORKER.get_timeout_ts();
    sql::ObSQLSessionInfo *origin_session = THIS_WORKER.get_session();
    THIS_WORKER.set_session(NULL);
    THIS_WORKER.set_timeout_ts(temp_stat_param.duration_time_ + ObTimeUtility::current_time());
    if (OB_SUCC(ret) && OB_FAIL(gather_partition_stats(ctx,
                                                       temp_stat_param,
                                                       partition_id_block_map,
                                                       partition_id_skip_rate_map,
                                                       gather_helper,
                                                       failed_part_ids))) {
      LOG_WARN("failed to collect global or part ", K(ret));
    }
    THIS_WORKER.set_session(origin_session);
    THIS_WORKER.set_timeout_ts(origin_duration_time);
  }

  return ret;
}

int ObDbmsStatsExecutor::do_split_gather_stats(ObExecContext &ctx,
                                               ObMySQLTransaction &trans,
                                               GatherPartInfos &taskInfo,
                                               ObIArray<TaskColumnParam> &batch_task_col_infos,
                                               ObTableStatParam &derive_param,
                                               const PartitionIdBlockMap *partition_id_block_map,
                                               const PartitionIdSkipRateMap *partition_id_skip_rate_map,
                                               GatherHelper &gather_helper)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_allocator("SplitTmpStat", OB_MALLOC_NORMAL_BLOCK_SIZE, derive_param.tenant_id_);
  ObArenaAllocator allocator("SplitGatherStat", OB_MALLOC_NORMAL_BLOCK_SIZE, derive_param.tenant_id_);
  // table level's avg_length depends on each column avg-length
  ObSEArray<ObOptTableStat *, 4> all_table_tats;
  derive_param.allocator_ = &allocator;

  if (batch_task_col_infos.count() > 1) {
    if (OB_FAIL(fetch_gather_table_snapshot_read(
            trans.get_connection(), derive_param.tenant_id_, gather_helper.sepcify_scn_))) {
      LOG_WARN("failed to fetch gather table snapshot read", K(ret));
      // if we failed to get the read snapshot, just skip, not specify the snapshot
      ret = OB_SUCCESS;
    }
    gather_helper.is_split_column_ = true;
  }

  if (OB_SUCC(ret) && !taskInfo.sub_part_infos_.empty()) {  // process subpart stats
    ObSEArray<ObOptColumnStat *, 4> all_cstats;
    if (OB_FAIL(ObDbmsStatsUtils::init_table_stats(tmp_allocator, taskInfo.sub_part_infos_.count(), all_table_tats))) {
      LOG_WARN("failed to init table stats", K(ret));
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < batch_task_col_infos.count(); j++) {
      TaskColumnParam &task_column_parm = batch_task_col_infos.at(j);
      gather_helper.is_all_col_gathered_ = (j == batch_task_col_infos.count() - 1);

      if (OB_FAIL(ObDbmsStatsUtils::assign_col_param(task_column_parm.column_params_,
                                                     task_column_parm.start,
                                                     task_column_parm.end,
                                                     derive_param.column_params_))) {
        LOG_WARN("failed to assign", K(ret));
      } else if (OB_FAIL(
                     gather_helper.running_monitor_.add_monitor_info(ObOptStatRunningPhase::GATHER_SUBPART_STATS))) {
        LOG_WARN("failed to add add monitor info", K(ret));
      } else if (OB_FAIL(do_gather_stats_with_retry(ctx,
                                                    trans,
                                                    StatLevel::SUBPARTITION_LEVEL,
                                                    taskInfo.sub_part_infos_,
                                                    partition_id_block_map,
                                                    partition_id_skip_rate_map,
                                                    gather_helper,
                                                    derive_param,
                                                    all_table_tats))) {
        LOG_WARN("failed to do gather stats", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < all_table_tats.count(); ++i) {
      if (NULL != all_table_tats.at(i)) {
        all_table_tats.at(i)->~ObOptTableStat();
      }
    }
    tmp_allocator.reuse();
  }

  if (OB_SUCC(ret) && !taskInfo.part_infos_.empty()) {  // process part stats
    all_table_tats.reset();
    if (OB_FAIL(ObDbmsStatsUtils::init_table_stats(tmp_allocator, taskInfo.part_infos_.count(), all_table_tats))) {
      LOG_WARN("failed to init table stats", K(ret));
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < batch_task_col_infos.count(); j++) {
      TaskColumnParam &task_column_parm = batch_task_col_infos.at(j);
      gather_helper.is_all_col_gathered_ = (j == batch_task_col_infos.count() - 1);
     if (OB_FAIL(ObDbmsStatsUtils::assign_col_param(task_column_parm.column_params_,
                                                     task_column_parm.start,
                                                     task_column_parm.end,
                                                     derive_param.column_params_))) {
        LOG_WARN("failed to assign", K(ret));
      } else if (taskInfo.approx_gather_) {
        if (OB_FAIL(gather_helper.running_monitor_.add_monitor_info(ObOptStatRunningPhase::APPROX_GATHER_PART_STATS))) {
          LOG_WARN("failed to add add monitor info", K(ret));
        } else if (OB_FAIL(ObIncrementalStatEstimator::derive_split_gather_stats(ctx,
                                                                                 trans,
                                                                                 derive_param,
                                                                                 partition_id_block_map,
                                                                                 &gather_helper.running_monitor_.audit_,
                                                                                 true /*derive_part_stat*/,
                                                                                 gather_helper.is_all_col_gathered_,
                                                                                 all_table_tats))) {
          LOG_WARN("failed to derive split gather stats", K(ret), K(derive_param));
        }
      } else {
        if (OB_FAIL(gather_helper.running_monitor_.add_monitor_info(ObOptStatRunningPhase::GATHER_PART_STATS))) {
          LOG_WARN("failed to add add monitor info", K(ret));
        } else if (OB_FAIL(do_gather_stats_with_retry(ctx,
                                                      trans,
                                                      StatLevel::PARTITION_LEVEL,
                                                      taskInfo.part_infos_,
                                                      partition_id_block_map,
                                                      partition_id_skip_rate_map,
                                                      gather_helper,
                                                      derive_param,
                                                      all_table_tats))) {
          LOG_WARN("failed to do gather stats", K(ret));
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < all_table_tats.count(); ++i) {
      if (NULL != all_table_tats.at(i)) {
        all_table_tats.at(i)->~ObOptTableStat();
      }
    }
    tmp_allocator.reuse();
  }

  if (OB_SUCC(ret) && taskInfo.gather_global_) {  // process global stats
    all_table_tats.reset();
    gather_helper.is_approx_gather_ = derive_param.global_stat_param_.gather_approx_;
    if (OB_FAIL(ObDbmsStatsUtils::init_table_stats(tmp_allocator, 1, all_table_tats))) {
      LOG_WARN("failed to init table stats", K(ret));
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < batch_task_col_infos.count(); j++) {
      TaskColumnParam &task_column_parm = batch_task_col_infos.at(j);
      gather_helper.is_all_col_gathered_ = (j == batch_task_col_infos.count() - 1);
      if (OB_FAIL(ObDbmsStatsUtils::assign_col_param(task_column_parm.column_params_,
                                                     task_column_parm.start,
                                                     task_column_parm.end,
                                                     derive_param.column_params_))) {
        LOG_WARN("failed to assign", K(ret));
      } else {
        if (gather_helper.is_approx_gather_ && derive_param.part_level_ != share::schema::PARTITION_LEVEL_ZERO) {
          if (OB_FAIL(ObIncrementalStatEstimator::derive_split_gather_stats(ctx,
                                                                            trans,
                                                                            derive_param,
                                                                            partition_id_block_map,
                                                                            &gather_helper.running_monitor_.audit_,
                                                                            false /*derive_part_stat*/,
                                                                            gather_helper.is_all_col_gathered_,
                                                                            all_table_tats))) {
            LOG_WARN("failed to derive split gather stats", K(ret));
          }
        } else {
          ObArray<PartInfo> dummy_part_infos;
          if (OB_FAIL(gather_helper.running_monitor_.add_monitor_info(ObOptStatRunningPhase::GATHER_GLOBAL_STATS))) {
            LOG_WARN("failed to add add monitor info", K(ret));
          } else if (OB_FAIL(do_gather_stats_with_retry(ctx,
                                                        trans,
                                                        StatLevel::TABLE_LEVEL,
                                                        dummy_part_infos,
                                                        partition_id_block_map,
                                                        partition_id_skip_rate_map,
                                                        gather_helper,
                                                        derive_param,
                                                        all_table_tats))) {
            LOG_WARN("failed to do gather stats", K(ret));
          }
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < all_table_tats.count(); ++i) {
      if (NULL != all_table_tats.at(i)) {
        all_table_tats.at(i)->~ObOptTableStat();
      }
    }
    tmp_allocator.reuse();
  }

  return ret;
}

int ObDbmsStatsExecutor::split_column_param(const ObTableStatParam &stat_param,
                                            GatherHelper &gather_helper,
                                            ObIArray<TaskColumnParam> &batch_task_col_infos)
{
  int ret = OB_SUCCESS;
  if (gather_helper.maximum_gather_col_cnt_ <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid maximum_gather_col_cnt_", K(gather_helper.maximum_gather_col_cnt_));
  } else {
    void *buf = NULL;
    TaskColumnParam *task_col_param = NULL;
    ObArenaAllocator temp_allocator("SplitGatherStat", OB_MALLOC_NORMAL_BLOCK_SIZE, stat_param.tenant_id_);
    int idx_col = 0;
    while (OB_SUCC(ret) && idx_col < stat_param.column_params_.count()) {
      if (OB_ISNULL(buf = temp_allocator.alloc(sizeof(TaskColumnParam)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc memory", K(ret), K(buf));
      } else {
        task_col_param = new (buf) TaskColumnParam();
        task_col_param->column_params_ = &stat_param.column_params_;
        task_col_param->start = idx_col;
        task_col_param->end = std::min(idx_col + gather_helper.maximum_gather_col_cnt_, stat_param.column_params_.count());
        idx_col = task_col_param->end;
        if (OB_FAIL(OB_FAIL(batch_task_col_infos.push_back(*task_col_param)))) {
          LOG_WARN("failed to push back", K(ret));
        }
      }
    }
    temp_allocator.reuse();
  }
  return ret;
}

int ObDbmsStatsExecutor::split_table_part_param(const ObTableStatParam &stat_param,
                                                const GatherHelper &gather_helper,
                                                ObIArray<GatherPartInfos> &batch_part_infos)
{
  int ret = OB_SUCCESS;
  hash::ObHashMap<int64_t, PartInfo*> part_id_to_approx_part_map;
  hash::ObHashMap<int64_t, ObArray<PartInfo*>> part_id_to_subpart_map;
  int64_t batch_cnt = 0;
  GatherPartInfos gather_info;
  if (OB_FAIL(construct_part_to_subpart_map(stat_param, part_id_to_approx_part_map, part_id_to_subpart_map))) {
    LOG_WARN("create part_to_subpart fail", K(ret));
  } else if (OB_FAIL(batch_part_infos.push_back(gather_info))) {
    LOG_WARN("fail to pusk back initial stats_collect_task_info ", K(ret));
  } else {
    if (OB_SUCC(ret) && stat_param.subpart_stat_param_.need_modify_) {  // process subpart stats
      hash::ObHashMap<int64_t, ObArray<PartInfo*>>::const_iterator iter = part_id_to_subpart_map.begin();
      int64_t k = 0;
      for (; OB_SUCC(ret) && iter != part_id_to_subpart_map.end(); iter++) {
        const ObArray<PartInfo*> &subparts = iter->second;
        int64_t part_id = iter->first;
        for (int64_t i = 0; OB_SUCC(ret) && i < subparts.count(); i++) {
          batch_cnt++;
          if (OB_ISNULL(subparts.at(i))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected error with NULL subpart", K(ret));
          } else if (OB_FAIL(batch_part_infos.at(batch_part_infos.count() - 1).sub_part_infos_.push_back(*subparts.at(i)))) {
            LOG_WARN("failed to push back sub_part_info", K(ret));
          } else if (i == subparts.count() - 1) {
            if (OB_FAIL(add_L0_L1_part_to_task(part_id,
                                               stat_param.global_stat_param_.need_modify_ &&
                                                   k == part_id_to_subpart_map.size() - 1,
                                               part_id_to_approx_part_map,
                                               batch_part_infos.at(batch_part_infos.count() - 1)))) {
              LOG_WARN("add part1 or part0 to task  failed", K(ret), K(part_id));
            } else {
              batch_cnt++;
              k++;
            }
          }

          if (OB_SUCC(ret) && batch_cnt >= gather_helper.maximum_gather_part_cnt_) {
            GatherPartInfos gather_info;
            if (OB_FAIL(batch_part_infos.push_back(gather_info))) {
              LOG_WARN("fail to pusk back initial stats_collect_task_info ", K(ret));
            } else {
              batch_cnt = 0;
            }
          }
        }
      }
    }

    if (OB_SUCC(ret) && stat_param.part_stat_param_.need_modify_) {  // process part stats
      // approx part should collect with sub-parts only handle partinfo
      for (int64_t i = 0; OB_SUCC(ret) && i < stat_param.part_infos_.count(); i++) {
        const PartInfo &temp_part = stat_param.part_infos_.at(i);
        batch_cnt++;
        if (OB_FAIL(batch_part_infos.at(batch_part_infos.count() - 1).part_infos_.push_back(temp_part))) {
          LOG_WARN("add L1 part to param fail", K(ret));
        } else if (i == stat_param.part_infos_.count() - 1) {
          batch_cnt++;
          batch_part_infos.at(batch_part_infos.count() - 1).gather_global_ = stat_param.global_stat_param_.need_modify_;
        }

        if (OB_SUCC(ret) && batch_cnt >= gather_helper.maximum_gather_part_cnt_) {
          GatherPartInfos gather_info;
          if (OB_FAIL(batch_part_infos.push_back(gather_info))) {
            LOG_WARN("fail to pusk back initial stats_collect_task_info ", K(ret));
          } else {
            batch_cnt = 0;
          }
        }
      }

      // check if contain apporx_part_and_global
      if (!stat_param.subpart_stat_param_.need_modify_) {
        for (int64_t i = 0; OB_SUCC(ret) && i < stat_param.approx_part_infos_.count(); i++) {
          const PartInfo &temp_part = stat_param.approx_part_infos_.at(i);
          batch_part_infos.at(batch_part_infos.count() - 1).approx_gather_ = true;
          batch_cnt++;
          if (OB_FAIL(batch_part_infos.at(batch_part_infos.count() - 1).part_infos_.push_back(temp_part))) {
            LOG_WARN("add L1 part to param fail", K(ret));
          } else if (i == stat_param.part_infos_.count() - 1) {
            batch_cnt++;
            batch_part_infos.at(batch_part_infos.count() - 1).gather_global_ =
                stat_param.global_stat_param_.need_modify_;
          }

          if (OB_SUCC(ret) && batch_cnt >= gather_helper.maximum_gather_part_cnt_) {
            GatherPartInfos gather_info;
            if (OB_FAIL(batch_part_infos.push_back(gather_info))) {
              LOG_WARN("fail to pusk back initial stats_collect_task_info ", K(ret));
            } else {
              batch_cnt = 0;
            }
          }
        }
      }
    }

    if (OB_SUCC(ret) && stat_param.global_stat_param_.need_modify_) {  // process global stats
      batch_part_infos.at(batch_part_infos.count() - 1).gather_global_ = stat_param.global_stat_param_.need_modify_;
    }

    if (OB_SUCC(ret) && batch_part_infos.at(batch_part_infos.count() - 1).is_invalid_task()) {
      batch_part_infos.pop_back();
    }
  }
  int tmp_ret = OB_SUCCESS;
  if (part_id_to_approx_part_map.created() && OB_SUCCESS != (tmp_ret = part_id_to_approx_part_map.destroy())) {
    LOG_WARN("failed to destroy part_id_to_approx_part_map hash map", K(tmp_ret));
  }
  if (part_id_to_subpart_map.created() && OB_SUCCESS != (tmp_ret = part_id_to_subpart_map.destroy())) {
    LOG_WARN("failed to destroy part_id_to_subpart_map hash map", K(tmp_ret));
  }
  return ret;
}

int ObDbmsStatsExecutor::add_L0_L1_part_to_task(uint64_t part_id,
                                                bool need_collect_global,
                                                const hash::ObHashMap<int64_t, PartInfo *> &part_id_to_approx_part_map,
                                                GatherPartInfos &gather_info)
{
  int ret = OB_SUCCESS;
  PartInfo *part = NULL;
  if (OB_FAIL(part_id_to_approx_part_map.get_refactored(part_id, part))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get part_id_to_approx_part_map", K(ret), K(part_id));
    }
  } else if (OB_ISNULL(part)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret));
  } else {
    gather_info.approx_gather_ = true;
    if (OB_FAIL(gather_info.part_infos_.push_back(*part))) {
      LOG_WARN("failed to push approx_part_infos_ failed", K(ret));
    }
  }

  gather_info.gather_global_ = need_collect_global;
  return ret;
}

int ObDbmsStatsExecutor::construct_part_to_subpart_map(const ObTableStatParam &stat_param,
                                                       hash::ObHashMap<int64_t, PartInfo*> &part_id_to_approx_part_map,
                                                       hash::ObHashMap<int64_t, ObArray<PartInfo*>> &part_id_to_subpart_map)
{
  int ret = OB_SUCCESS;
  // init
  if (!part_id_to_approx_part_map.created() &&
      OB_FAIL(part_id_to_approx_part_map.create(64, "aproxPartMap", "DBMS_STATS"))) {
    LOG_WARN("create part_id_to_approx_part_map fail", K(ret));
  } else if (!part_id_to_subpart_map.created() &&
             OB_FAIL(part_id_to_subpart_map.create(64, "idToSubMap", "DBMS_STATS"))) {
    LOG_WARN("create part_id_to_subpart_map fail", K(ret));
  }

  // set part_id_to_subpart_map
  for (int64_t i = 0; OB_SUCC(ret) && i < stat_param.subpart_infos_.count(); i++) {
    const PartInfo &subpart = stat_param.subpart_infos_.at(i);
    ObArray<PartInfo*> temp_subpart_infos;
    if (OB_FAIL(part_id_to_subpart_map.get_refactored(subpart.first_part_id_, temp_subpart_infos))) {
      if (OB_HASH_NOT_EXIST == ret) {  // first time
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("part_id_to_subpart_map get fail", K(ret), K(subpart));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(temp_subpart_infos.push_back(const_cast<PartInfo*>(&subpart)))) {
        LOG_WARN("fail to push back to temp_subpart_infos array", K(ret), K(subpart));
      } else if (OB_FAIL(part_id_to_subpart_map.set_refactored(
                     subpart.first_part_id_, temp_subpart_infos, true /* overwrite */))) {
        LOG_WARN("part_id_to_subpart_map set fail", K(ret), K(subpart));
      }
    }
  }

  // set part_id_to_approx_part_map
  for (int64_t i = 0; OB_SUCC(ret) && i < stat_param.approx_part_infos_.count(); i++) {
    const PartInfo &part = stat_param.approx_part_infos_.at(i);
    if (OB_FAIL(part_id_to_approx_part_map.set_refactored(part.part_id_, const_cast<PartInfo*>(&part)))) {
      LOG_WARN("failed to set part_id_to_approx_part_map", K(ret));
    }
  }

  return ret;
}

/** @brief ObDbmsStatsExecutor::prepare_gather_stats used to prepare gather table stats, including:
 * 1.estimate block count;
 * 2.adjust async gather param base on the estimate rowcnt info.
 * 3.get the maximum num of partitions and columns for each stat gather.
 */
int ObDbmsStatsExecutor::prepare_gather_stats(ObExecContext &ctx,
                                              const ObTableStatParam &param,
                                              PartitionIdBlockMap &partition_id_block_map,
                                              PartitionIdSkipRateMap &partition_id_skip_rate_map,
                                              GatherHelper &gather_helper)
{
  int ret = OB_SUCCESS;
    int64_t start_time = 0;
  if (OB_FAIL(partition_id_block_map.create(10000,
                                            ObModIds::OB_HASH_BUCKET_TABLE_STATISTICS,
                                            ObModIds::OB_HASH_BUCKET_TABLE_STATISTICS,
                                            param.tenant_id_))) {
    LOG_WARN("failed to create hash map", K(ret));
  } else if (OB_FAIL(partition_id_skip_rate_map.create(10000,
                                                       ObModIds::OB_HASH_BUCKET_TABLE_STATISTICS,
                                                       ObModIds::OB_HASH_BUCKET_TABLE_STATISTICS,
                                                       param.tenant_id_))) {
    LOG_WARN("failed to create hash map", K(ret));
  } else if (OB_FALSE_IT(start_time = ObTimeUtility::current_time())) {
    //do nothing
  } else if (param.need_estimate_block_ && share::schema::ObTableType::EXTERNAL_TABLE != param.ref_table_type_ &&
             OB_FAIL(ObBasicStatsEstimator::estimate_block_count(
                 ctx, param, partition_id_block_map, gather_helper.use_column_store_))) {
    LOG_WARN("failed to estimate block count", K(ret));
  } else if (OB_FAIL(gather_helper.running_monitor_.audit_.add_flush_block_count_audit(ObTimeUtility::current_time() -
                                                                                       start_time))) {
    LOG_WARN("failed to add flush stats audit", K(ret));
  } else if (OB_FALSE_IT(start_time = ObTimeUtility::current_time())) {
    //do nothing
  } else if (param.skip_rate_sample_cnt_ != 0 && OB_FAIL(ObBasicStatsEstimator::estimate_skip_rate(
                                                     ctx, param, partition_id_skip_rate_map, partition_id_block_map))) {
    LOG_WARN("failed to estimate_skip_rate", K(ret));
  } else if (OB_FAIL(gather_helper.running_monitor_.audit_.add_flush_skip_rate_audit(ObTimeUtility::current_time() -
                                                                                     start_time))) {
    LOG_WARN("failed to add flush stats audit", K(ret));
  } else if (OB_FAIL(determine_auto_sample_table(ctx, const_cast<ObTableStatParam&>(param)))) {
      LOG_WARN("failed to determine auto sample table", K(ret));
  } else if (param.is_auto_sample_size_ &&
             OB_FAIL(try_use_prefix_index_refine_min_max(ctx, const_cast<ObTableStatParam &>(param)))) {
    LOG_WARN("failed to parse refine min max options", K(ret));
  } else if (param.is_auto_sample_size_ && OB_FAIL(check_use_single_partition_gather(
                                               partition_id_block_map, param, gather_helper.use_single_part_))) {
    LOG_WARN("failed to adjust auto gather param", K(ret));
  } else if (OB_FAIL(get_stats_collect_batch_size(ctx.get_sql_proxy(),
                                                  ctx.get_my_session()->get_effective_tenant_id(),
                                                  param.table_id_,
                                                  gather_helper.batch_part_size_))) {
    LOG_WARN("failed to get auto stats collect batch size", K(ret));
  } else if (OB_FAIL(check_need_split_gather(param, gather_helper))) {
    LOG_WARN("failed to check need split gather", K(ret));
  } else {
    LOG_TRACE("succeed to prepare gather stats", K(param), K(gather_helper));
  }
  return ret;
}

/*
 *  todo 统一 ObOptStatGatherParam GatherHelper ObTableStatParam 的用途
 */
int ObDbmsStatsExecutor::do_gather_stats_with_retry(ObExecContext &ctx,
                                                    ObMySQLTransaction &trans,
                                                    StatLevel stat_level,
                                                    const ObIArray<PartInfo> &gather_partition_infos,
                                                    const PartitionIdBlockMap *partition_id_block_map,
                                                    const PartitionIdSkipRateMap *partition_id_skip_rate_map,
                                                    GatherHelper &gather_helper,
                                                    ObTableStatParam &derive_param,
                                                    ObIArray<ObOptTableStat *> &all_tstats)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObOptStat, 4> opt_stats;
  ObSEArray<ObOptColumnStat *, 4> all_cstats;
  ObArenaAllocator allocator("SplitGatherStat", OB_MALLOC_NORMAL_BLOCK_SIZE, derive_param.tenant_id_);
  SMART_VAR(ObOptStatGatherParam, gather_param)
  {
    gather_param.allocator_ = &allocator;
    gather_param.sepcify_scn_ = gather_helper.sepcify_scn_;
    if (OB_UNLIKELY(!is_valid_tenant_id(derive_param.tenant_id_))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("tenant id invalid", KR(ret), K(derive_param.tenant_id_));
    } else if (OB_ISNULL(gather_param.allocator_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid args with null allocator", KR(ret));
    } else if (OB_FAIL(ObDbmsStatsUtils::prepare_gather_stat_param(derive_param,
                                                                   stat_level,
                                                                   partition_id_block_map,
                                                                   partition_id_skip_rate_map,
                                                                   true,
                                                                   gather_helper.gather_vectorize_,
                                                                   gather_helper.use_column_store_,
                                                                   gather_param))) {
      LOG_WARN("failed to prepare gather stat param", K(ret));
    } else if (OB_FAIL(do_gather_stats(ctx,
                                       trans,
                                       gather_param,
                                       gather_partition_infos,
                                       derive_param.column_params_,
                                       gather_helper.is_all_col_gathered_,
                                       gather_helper.running_monitor_.audit_,
                                       opt_stats,
                                       all_tstats,
                                       all_cstats))) {
      if (gather_param.sepcify_scn_ > 0 && (ret == OB_TABLE_DEFINITION_CHANGED || ret == OB_SNAPSHOT_DISCARDED)) {
        LOG_WARN("failed to specify snapshot to gather stats, try no specify snapshot to gather stats", K(ret));
        gather_param.sepcify_scn_ = 0;
        gather_param.allocator_->reuse();
        opt_stats.reset();
        all_cstats.reset();
        if (OB_FAIL(do_gather_stats(ctx,
                                    trans,
                                    gather_param,
                                    gather_partition_infos,
                                    derive_param.column_params_,
                                    gather_helper.is_all_col_gathered_,
                                    gather_helper.running_monitor_.audit_,
                                    opt_stats,
                                    all_tstats,
                                    all_cstats))) {
          LOG_WARN("failed to do gather stats", K(ret));
        } else {
          gather_param.allocator_->reuse();  // Phased memory release for split gather, in order to reuse memory
        }
      } else {
        LOG_WARN("failed to do gather stats", K(ret));
      }
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
                                         ObOptStatGatherAudit &audit,
                                         ObIArray<ObOptStat> &opt_stats,
                                         ObIArray<ObOptTableStat *> &all_tstats,
                                         ObIArray<ObOptColumnStat *> &all_cstats)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObOptTableStat *, 4> tmp_all_tstats;
  int64_t start_time = 0;
  if (OB_FAIL(THIS_WORKER.check_status())) {
    LOG_WARN("check status failed", KR(ret));
  } else if (OB_FAIL(param.partition_infos_.assign(gather_partition_infos))) {
    LOG_WARN("failed to assign", K(ret));
  } else if (OB_FAIL(param.column_params_.assign(gather_column_params))) {
    LOG_WARN("failed to assign", K(ret));
  } else if (OB_FAIL(ObDbmsStatsGather::gather_stats(ctx, param, audit, opt_stats))) {
    LOG_WARN("failed to gather stats", K(ret));
  } else if (OB_FAIL(ObDbmsStatsUtils::check_all_cols_range_skew(gather_column_params, opt_stats))) {
    LOG_WARN("failed to check all cols range skew", K(ret));
  } else if (OB_FAIL(ObDbmsStatsUtils::calssify_opt_stat(opt_stats,
                                                         tmp_all_tstats,
                                                         all_cstats))) {
    LOG_WARN("failed to calssify opt stat", K(ret));
  } else if (OB_FAIL(ObDbmsStatsUtils::merge_split_gather_tab_stats(all_tstats, tmp_all_tstats))) {
    // avoid memory use too much, write current gather stats.
    LOG_WARN("failed to merge split gather tab stats", K(ret));
  } else if (OB_FALSE_IT(start_time = ObTimeUtility::current_time())) {
  } else if (param.use_part_derive_global_ &&
             OB_FAIL(mark_internal_stat(
                 param.global_part_id_, is_all_columns_gather ? all_tstats : tmp_all_tstats, all_cstats))) {
    LOG_WARN("failed to merge split gather tab stats", K(ret));
  } else if (OB_FAIL(ObDbmsStatsUtils::split_batch_write(
                 ctx, trans.get_connection(), is_all_columns_gather ? all_tstats : tmp_all_tstats, all_cstats))) {
    LOG_WARN("failed to split batch write", K(ret));
  } else if (OB_FAIL(audit.add_flush_stats_audit(ObTimeUtility::current_time() - start_time))) {
    LOG_WARN("failed to add flush stats audit", K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

// Get the maximum number of partitions and columns for each stat gather and check need split gather
int ObDbmsStatsExecutor::check_need_split_gather(const ObTableStatParam &param, GatherHelper &gather_helper)
{
  int ret = OB_SUCCESS;
  bool random_split_part = ERRSIM_RANDOM_GATHER_STATS_OPTION;
  int64_t origin_column_cnt = param.get_need_gather_column();
  int64_t partition_cnt =
      (param.subpart_stat_param_.need_modify_ ? param.subpart_infos_.count() : 0) +
      (param.part_stat_param_.need_modify_ ? param.part_infos_.count() + param.approx_part_infos_.count() : 0) + 1;
  int64_t column_cnt = origin_column_cnt == 0 ? 1 : origin_column_cnt;

  if (param.is_auto_gather_ || param.is_async_gather_) {
    int64_t max_wa_memory_size = MIN_GATHER_WORK_ARANA_SIZE;
    if (OB_FAIL(ObDbmsStatsUtils::get_max_work_area_size(param.tenant_id_, max_wa_memory_size))) {
      LOG_WARN("failed to get max work area size", K(ret));
    } else {
      int64_t max_gather_col_cnt = max_wa_memory_size >= 1 * 1024L * 1024L * 1024L /*1G*/
                                       ? MAX_GATHER_COLUMN_COUNT_PER_QUERY_FOR_LARGE_TENANT
                                       : MAX_GATHER_COLUMN_COUNT_PER_QUERY_FOR_SMALL_TENANT;
      column_cnt = column_cnt > max_gather_col_cnt ? max_gather_col_cnt : column_cnt;
    }

    if (OB_SUCC(ret) && gather_helper.use_single_part_) {
      gather_helper.maximum_gather_part_cnt_ = 1;
    } else if (gather_helper.batch_part_size_ == 0) {
      gather_helper.maximum_gather_part_cnt_ = partition_cnt;
    } else {
      gather_helper.maximum_gather_part_cnt_ = gather_helper.batch_part_size_;
    }
    gather_helper.gather_vectorize_ = DEFAULT_STAT_GATHER_VECTOR_BATCH_SIZE;
    gather_helper.maximum_gather_col_cnt_ = column_cnt;
    gather_helper.is_split_gather_ =
        gather_helper.maximum_gather_part_cnt_ < partition_cnt || column_cnt != origin_column_cnt;

    if (OB_SUCC(ret) && random_split_part) {
      gather_helper.maximum_gather_col_cnt_ = ObRandom::rand(1, column_cnt);
      gather_helper.maximum_gather_part_cnt_ = ObRandom::rand(1, partition_cnt);
      if (gather_helper.maximum_gather_col_cnt_ > column_cnt) {
        gather_helper.maximum_gather_col_cnt_ = column_cnt;
      }
      gather_helper.is_split_gather_ = gather_helper.maximum_gather_part_cnt_ != partition_cnt ||
                                       gather_helper.maximum_gather_col_cnt_ != origin_column_cnt;
    }
    if (gather_helper.is_split_gather_) {
      LOG_TRACE("stat gather will use split gather", K(param.degree_), K(gather_helper));
    }
    LOG_TRACE("succeed to get the maximum num of part and column for stat gather", K(param), K(gather_helper));
  }
  else {
    gather_helper.maximum_gather_part_cnt_ = partition_cnt;
    gather_helper.maximum_gather_col_cnt_ = std::min(column_cnt, MAX_GATHER_COLUMN_COUNT_PER_QUERY_FOR_LARGE_TENANT);
    gather_helper.is_split_gather_ = gather_helper.maximum_gather_part_cnt_ < partition_cnt ||
                                     gather_helper.maximum_gather_col_cnt_ < origin_column_cnt;
  }
  return ret;
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
  } else if (param.is_invalid()) {
    ret = OB_ERR_DBMS_STATS_PL;
    LOG_WARN("Invalid or inconsistent input values", K(ret), K(param));
    LOG_USER_ERROR(OB_ERR_DBMS_STATS_PL,"Invalid or inconsistent input values");
  } else {
    //1.set numrows_
    if (param.numrows_ != ObSetTableStatParam::NULLOPT) {
      table_stat->set_row_count(param.numrows_);
    }
    //2.set numblks_
    // if (param.numblks_ > 0) {
    //   table_stat->set_macro_block_num(param.numrows_);
    // }
    //3.avgrlen_
    if (param.avgrlen_ != ObSetTableStatParam::NULLOPT) {
      table_stat->set_avg_row_size(param.avgrlen_);
    }
    if (param.nummacroblks_ != ObSetTableStatParam::NULLOPT) {
      table_stat->set_macro_block_num(param.nummacroblks_);
    }
    if (param.nummicroblks_ != ObSetTableStatParam::NULLOPT) {
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
  } else if (param.is_invalid()) {
    ret = OB_ERR_DBMS_STATS_PL;
    LOG_WARN("Invalid or inconsistent input values", K(ret), K(param));
    LOG_USER_ERROR(OB_ERR_DBMS_STATS_PL,"Invalid or inconsistent input values");
  } else {
    //1.set distcnt_
    if (param.distcnt_ != ObSetColumnStatParam::NULLOPT) {
      column_stat->set_num_distinct(param.distcnt_);
    }
    //2.set density_
    if (param.density_ != ObSetColumnStatParam::NULLOPT) {
      column_stat->get_histogram().set_density(param.density_);
    }
    //3.nullcnt_
    if (param.nullcnt_ != ObSetColumnStatParam::NULLOPT) {
      column_stat->set_num_null(param.nullcnt_);
    }
    //4.avgclen_
    if (param.avgclen_ != ObSetColumnStatParam::NULLOPT) {
      column_stat->set_avg_len(param.avgclen_);
    }
    //5.set max/val value
    if (param.hist_param_.minval_ != NULL || param.hist_param_.maxval_ != NULL) {
      ObCastCtx cast_ctx(&allocator, &dtc_params, CM_NONE, param.col_meta_.get_collation_type());
      ObAccuracy res_acc;
      if (param.col_meta_.is_decimal_int()) {
        res_acc = param.col_accuracy_;
        cast_ctx.res_accuracy_ = &res_acc;
      }
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
  ObArray<ObOptColumnStat *> all_column_stats;
  ObSEArray<ObOptStat, 4> subpart_opt_stats;
  ObSEArray<ObOptStat, 4> part_opt_stats;
  ObSEArray<ObOptStat, 1> global_opt_stats;
  ObOptStat global_opt_stat;
  ObOptStatGatherParam gather_param;
  PartitionIdBlockMap partition_id_block_map;
  bool use_column_store = false;
  ObSEArray<ObOptColumnStat*, 4> copy_stats;
  LOG_TRACE("begin gather index stats", K(param));
  if (OB_FAIL(partition_id_block_map.create(10000,
                                            ObModIds::OB_HASH_BUCKET_TABLE_STATISTICS,
                                            ObModIds::OB_HASH_BUCKET_TABLE_STATISTICS,
                                            param.tenant_id_))) {
    LOG_WARN("failed to create hash map", K(ret));
  } else if (param.need_estimate_block_ && OB_FAIL(ObBasicStatsEstimator::estimate_block_count(
                                               ctx, param, partition_id_block_map, use_column_store))) {
    LOG_WARN("failed to estimate block count", K(ret));
  } else if (OB_FAIL(ObDbmsStatsUtils::prepare_gather_stat_param(param,
                                                                 INVALID_LEVEL,
                                                                 &partition_id_block_map,
                                                                 NULL,
                                                                 false,
                                                                 DEFAULT_STAT_GATHER_VECTOR_BATCH_SIZE,
                                                                 use_column_store,
                                                                 gather_param))) {
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
    } else if (OB_FAIL(ObDbmsStatsGather::gather_index_stats(ctx, gather_param, subpart_opt_stats, all_index_stats, all_column_stats))) {
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
      } else if (all_column_stats.empty()) {
        if (OB_FAIL(ObIncrementalStatEstimator::derive_part_index_stat_by_subpart_index_stats(param,
                                                                                              all_index_stats,
                                                                                              part_index_stats))) {
          LOG_WARN("failed to derive part index stat by subpart index stats", K(ret));
        } else if (OB_FAIL(append(all_index_stats, part_index_stats))) {
          LOG_WARN("failed to append", K(ret));
        }
      } else {
        if (OB_FAIL(ObIncrementalStatEstimator::derive_part_index_column_stat_by_subpart_index(ctx,
                                                                                               *param.allocator_,
                                                                                               param,
                                                                                               subpart_opt_stats,
                                                                                               part_opt_stats))) {
          LOG_WARN("failed to derived part");
        } else if (OB_FAIL(ObDbmsStatsUtils::calssify_opt_stat(part_opt_stats,
                                                               all_index_stats,
                                                               all_column_stats))) {
          LOG_WARN("failed to classify opt stat", K(ret));
        }
      }
    } else if (OB_UNLIKELY(!param.approx_part_infos_.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(ret), K(param));
    }
    if (OB_SUCC(ret) && !param.part_infos_.empty()) {
      if (OB_FAIL(gather_param.partition_infos_.assign(param.part_infos_))) {
        LOG_WARN("failed to assign", K(ret));
      } else if (OB_FAIL(ObDbmsStatsGather::gather_index_stats(ctx, gather_param, part_opt_stats, part_index_stats, all_column_stats))) {
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
      if (all_column_stats.empty()) {
        if (OB_FAIL(ObIncrementalStatEstimator::derive_global_index_stat_by_part_index_stats(param,
                                                                                             part_index_stats,
                                                                                             all_index_stats))) {
          LOG_WARN("failed to derive global index stat by part index stats", K(ret));
        }
      } else {
        if (OB_FAIL(ObIncrementalStatEstimator::derive_global_index_column_stat_by_part_index(ctx,
                                                                                                     *param.allocator_,
                                                                                                     param,
                                                                                                     part_opt_stats,
                                                                                                     global_opt_stat))) {
          LOG_WARN("failed to derive global index column stat by part index", K(ret));
        } else if (OB_FAIL(global_opt_stats.push_back(global_opt_stat))) {
          LOG_WARN("failed to push back opt stats", K(ret));
        }else if (OB_FAIL(ObDbmsStatsUtils::calssify_opt_stat(global_opt_stats,
                                                               all_index_stats,
                                                               all_column_stats))) {
          LOG_WARN("failed to classify opt stat", K(ret));
        }
      }
    } else if (OB_FAIL(ObDbmsStatsGather::gather_index_stats(ctx, gather_param, global_opt_stats, all_index_stats, all_column_stats))) {
      LOG_WARN("failed to gather index stats", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (all_column_stats.empty()) {
    // do nothing
  } else if (param.is_global_index_ ) {
    if (OB_FAIL(ObDbmsStatsUtils::copy_global_index_prefix_stats_to_text(
                                    ctx.get_virtual_table_ctx().schema_guard_,
                                                                         *param.allocator_,
                                                                         all_column_stats,
                                                                         param.prefix_column_pairs_,
                                                                         param.tenant_id_,
                                                                         param.data_table_id_,
                                                                         copy_stats))) {
      LOG_WARN("failed to copy global index prefix stats to text", K(ret));
    } else if (OB_FAIL(append(all_column_stats, copy_stats))) {
      LOG_WARN("failed to append copy stats", K(ret));
    }
  } else if (OB_FAIL(ObDbmsStatsUtils::deduce_index_column_stat_to_table(
                                        ctx.get_virtual_table_ctx().schema_guard_,
                                                                         param.tenant_id_,
                                                                         param.table_id_,
                                                                         param.data_table_id_,
                                                                         param.part_level_,
                                                                         all_column_stats))) {
    LOG_WARN("failed to trans index column stat to table", K(ret));
  } else if (param.prefix_column_pairs_.empty()) {
    // do nothing
  } else if (OB_FAIL(ObDbmsStatsUtils::copy_local_index_prefix_stats_to_text(
                                       *param.allocator_,
                                       all_column_stats,
                                       param.prefix_column_pairs_,
                                       copy_stats))) {
    LOG_WARN("failed to copy local index prefix stats to text", K(ret));
  } else if (OB_FAIL(append(all_column_stats, copy_stats))) {
    LOG_WARN("failed to append copy stats", K(ret));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObDbmsStatsUtils::split_batch_write(ctx, all_index_stats, all_column_stats, true))) {
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
      int64_t old_trx_lock_timeout = -1;
      bool need_restore_session = false;
      bool need_reset_trx_lock_timeout = false;
      common::sqlclient::ObISQLConnection *conn = NULL;
      ctx.set_is_online_stats_gathering(true);
      const ObString stash_savepoint_name("online stat stash savepoint");
      bool has_stash_savepoint = false;
      //lib::CompatModeGuard guard(lib::Worker::CompatMode::MYSQL);
      if (OB_FAIL(ObDbmsStatsUtils::cancel_async_gather_stats(ctx))) {
        LOG_WARN("failed to cancel async gather stats", K(ret));
      } else if (OB_FAIL(prepare_conn_and_store_session_for_online_stats(ctx.get_my_session(),
                                                                         ctx.get_sql_proxy(),
                                                                         schema_guard,
                                                                         saved_value,
                                                                         nested_count,
                                                                         old_trx_lock_timeout,
                                                                         need_restore_session,
                                                                         need_reset_trx_lock_timeout,
                                                                         conn))) {
        LOG_WARN("failed to prepare conn and store session for online stats", K(ret));
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
      } else if (OB_FAIL(ObSqlTransControl::create_stash_savepoint(ctx, stash_savepoint_name))) {
        LOG_WARN("failed to create stash savepoint", K(ret));
      } else if (OB_FALSE_IT(has_stash_savepoint = true)) {
      } else if (nullptr != dml_stats && ObOptStatMonitorManager::update_dml_stat_info_from_direct_load(*dml_stats, conn)) {
        LOG_WARN("fail to update dml stat info", K(ret));
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
      if (has_stash_savepoint) {
        int pop_ret = ObSqlTransControl::release_stash_savepoint(ctx, stash_savepoint_name);
        if (OB_SUCCESS != pop_ret) {
          LOG_WARN("fail to release stash savepoint", K(pop_ret));
          ret = OB_SUCCESS == ret ? pop_ret : ret;
        }
      }
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
                                                                     old_trx_lock_timeout,
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
                                                                         int64_t &old_trx_lock_timeout,
                                                                         bool &need_restore_session,
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
    //2.2 modify seesion compatible mode
    ObObj mysql_mode;
    mysql_mode.set_int(0);
    if (OB_FAIL(session->update_sys_variable(share::SYS_VAR_OB_COMPATIBILITY_MODE, mysql_mode))) {
      LOG_WARN("failed to update sys variable for compatibility mode", K(ret));
    } else {
      //2.3.modify session database name and database id and catalog id
      if (OB_FAIL(session->set_internal_catalog_db())) {
        LOG_WARN("failed to set session catalog and database", K(ret));
      } else {
        //2.4 modify session trx lock timeout
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
  return ret;
}

int ObDbmsStatsExecutor::restore_session_for_online_stat(sql::ObSQLSessionInfo *session,
                                                         sql::ObSQLSessionInfo::StmtSavedValue &saved_value,
                                                         int64_t nested_count,
                                                         int64_t old_trx_lock_timeout,
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
    //3.restore trx lock timeout
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
  int64_t cpu_mhz = OBSERVER_FREQUENCE.get_cpu_frequency_khz()/1000;
  int64_t network_speed = OBSERVER.get_network_speed() / 1024.0 / 1024.0;
  int64_t disk_seq_read_speed = 0;
  int64_t disk_rnd_read_speed = 0;
  OptSystemIoBenchmark &io_benchmark = OptSystemIoBenchmark::get_instance();
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || is_virtual_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant id invalid", KR(ret), K(tenant_id));
  } else if (io_benchmark.is_init()) {
    disk_seq_read_speed = io_benchmark.get_disk_seq_read_speed();
    disk_rnd_read_speed = io_benchmark.get_disk_rnd_read_speed();
  } else if (OB_FAIL(io_benchmark.run_benchmark(ctx.get_allocator(), tenant_id))) {
    LOG_WARN("failed to run io benchmark", KR(ret), K(tenant_id));
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


bool ObDbmsStatsExecutor::is_async_gather_partition_id(const int64_t partition_id,
                                                       const ObIArray<int64_t> *async_partition_ids)
{
  bool is_found = false;
  if (async_partition_ids != NULL) {
    for (int64_t i = 0; !is_found && i < async_partition_ids->count(); ++i) {
      is_found = partition_id == async_partition_ids->at(i);
    }
  }
  return is_found;
}

int ObDbmsStatsExecutor::determine_auto_sample_table(ObExecContext &ctx, ObTableStatParam &param)
{
  int ret = OB_SUCCESS;
  share::schema::ObSchemaGetterGuard *schema_guard = ctx.get_virtual_table_ctx().schema_guard_;
  const share::schema::ObTableSchema *table_schema = NULL;
  if (param.auto_sample_row_cnt_ == 0) {
    param.is_auto_sample_size_ = false;
  } else if (!param.is_auto_sample_size_) {
    // do nothing
  } else if (OB_ISNULL(schema_guard) ||
             OB_ISNULL(ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(ctx.get_my_session()->get_effective_tenant_id(),
                                                    param.table_id_,
                                                    table_schema))) {
    LOG_WARN("failed to get index schema", K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    param.is_auto_sample_size_ = !table_schema->is_vir_table() &&
                                 !table_schema->is_external_table();
  }
  return ret;
}

int ObDbmsStatsExecutor::try_use_prefix_index_refine_min_max(ObExecContext &ctx, ObTableStatParam &param)
{
  int ret = OB_SUCCESS;
  uint64_t index_tids[OB_MAX_INDEX_PER_TABLE + 2];
  int64_t index_count = OB_MAX_INDEX_PER_TABLE + 1;
  share::schema::ObSchemaGetterGuard *schema_guard = ctx.get_virtual_table_ctx().schema_guard_;
  const share::schema::ObTableSchema *table_schema = NULL;
  ObSEArray<uint64_t, 4> refine_columns;
  ObSEArray<ObString, 4> refine_index_names;
  uint64_t first_column_id = OB_INVALID_ID;
  ObSEArray<uint64_t, 4> rowkey_ids;
  if (OB_ISNULL(schema_guard) || OB_ISNULL(ctx.get_my_session()) || OB_ISNULL(param.allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(
                 ctx.get_my_session()->get_effective_tenant_id(), param.table_id_, table_schema))) {
    LOG_WARN("failed to get index schema", K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(ObDbmsStatsUtils::get_table_index_infos(schema_guard,
                                                             ctx.get_my_session()->get_effective_tenant_id(),
                                                             param.table_id_,
                                                             index_tids,
                                                             index_count))) {
    LOG_WARN("failed to get table index infos", K(ret));
  } else {
    index_tids[index_count++] = param.table_id_;
    for (int64_t i = 0; OB_SUCC(ret) && i < index_count; ++i) {
      const share::schema::ObTableSchema *index_schema = NULL;
      ObString index_name;
      rowkey_ids.reuse();
      if (OB_FAIL(schema_guard->get_table_schema(
              ctx.get_my_session()->get_effective_tenant_id(), index_tids[i], index_schema))) {
        LOG_WARN("failed to get index schema", K(ret));
      } else if (OB_ISNULL(index_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(index_schema->get_rowkey_column_ids(rowkey_ids))) {
        LOG_WARN("failed to get rowkey column ids", K(ret));
      } else if (rowkey_ids.empty()) {
        // do nothing
      } else if (OB_FALSE_IT(first_column_id = rowkey_ids.at(0))) {
      } else if (index_schema->is_global_index_table() &&
                 table_schema->get_part_level() != ObPartitionLevel::PARTITION_LEVEL_ONE) {
        // do nothing
      } else if (ObOptimizerUtil::find_item(refine_columns, first_column_id)) {
        // do nothing
      } else if (index_tids[i] != param.table_id_ && OB_FAIL(index_schema->get_index_name(index_name))) {
        LOG_WARN("failed to get index name", K(ret));
      } else if (OB_FAIL(refine_columns.push_back(first_column_id))) {
        LOG_WARN("failed to push back array", K(ret));
      } else if (OB_FAIL(refine_index_names.push_back(index_name))) {
        LOG_WARN("failed to push back array", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < param.column_params_.count(); ++i) {
      int64_t index = -1;
      if (!param.column_params_.at(i).need_basic_stat()) {
        // do nothing
      } else if (!ObOptimizerUtil::find_item(refine_columns, param.column_params_.at(i).column_id_, &index)) {
        // do nothing
      } else if (OB_FAIL(ob_write_string(
                     *param.allocator_, refine_index_names.at(index), param.column_params_.at(i).index_name_))) {
        LOG_WARN("failed to write string", K(ret));
      } else {
        param.column_params_.at(i).set_need_refine_min_max();
        param.need_refine_min_max_ = true;
      }
    }
  }
  return ret;
}
/**
 * 检查是否需要切换为单分区统计信息收集模式
 * @param partition_id_block_map 分区ID到块统计信息的映射表
 * @param param 表统计参数配置
 * @param need_single_part  输出标志位，true表示需要单分区模式
 * @return
 */
int ObDbmsStatsExecutor::check_use_single_partition_gather(const PartitionIdBlockMap &partition_id_block_map,
                                                           const ObTableStatParam &param,
                                                           bool &need_single_part)
{
  int ret = OB_SUCCESS;
  LOG_TRACE("begin to adjsut gather param", K(param));
  if (param.part_level_ == share::schema::ObPartitionLevel::PARTITION_LEVEL_ZERO) {
    // do nohting
  } else {
    BlockNumStat *block_num_stat = NULL;
    int64_t row_cnt = 0;
    if (OB_FAIL(partition_id_block_map.get_refactored(param.global_part_id_, block_num_stat))) {
      if (OB_LIKELY(OB_HASH_NOT_EXIST == ret)) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to get refactored", K(ret));
      }
    } else if (OB_ISNULL(block_num_stat)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(ret), K(block_num_stat));
    } else if (OB_FALSE_IT(row_cnt = block_num_stat->sstable_row_cnt_ + block_num_stat->memtable_row_cnt_)) {
    } else if (row_cnt < param.auto_sample_row_cnt_) {
      // do nothing
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && !need_single_part && i < param.subpart_infos_.count(); ++i) {
        if (OB_FAIL(partition_id_block_map.get_refactored(param.subpart_infos_.at(i).part_id_, block_num_stat))) {
          if (OB_LIKELY(OB_HASH_NOT_EXIST == ret)) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("failed to get refactored", K(ret));
          }
        } else if (OB_ISNULL(block_num_stat)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected error", K(ret), K(block_num_stat));
        } else if (OB_FALSE_IT(row_cnt = block_num_stat->sstable_row_cnt_ + block_num_stat->memtable_row_cnt_)) {
        } else if (row_cnt < param.auto_sample_row_cnt_) {
          // do nothing
        } else {
          need_single_part = true;
        }
      }
      if (OB_SUCC(ret) && !need_single_part && param.part_stat_param_.need_modify_) {
        for (int64_t i = 0; OB_SUCC(ret) && !need_single_part && i < param.part_infos_.count(); ++i) {
          if (OB_FAIL(partition_id_block_map.get_refactored(param.part_infos_.at(i).part_id_, block_num_stat))) {
            if (OB_LIKELY(OB_HASH_NOT_EXIST == ret)) {
              ret = OB_SUCCESS;
            } else {
              LOG_WARN("failed to get refactored", K(ret));
            }
          } else if (OB_ISNULL(block_num_stat)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected error", K(ret), K(block_num_stat));
          } else if (OB_FALSE_IT(row_cnt = block_num_stat->sstable_row_cnt_ + block_num_stat->memtable_row_cnt_)) {
          } else if (row_cnt < param.auto_sample_row_cnt_) {
            // do nothing
          } else {
            need_single_part = true;
          }
        }
      }
    }
  }
  LOG_TRACE("end to adjsut auto gather param", K(param), K(need_single_part));
  return ret;
}

int ObDbmsStatsExecutor::collect_executed_part_ids(const ObTableStatParam &stat_param, ObIArray<int64_t> &part_ids)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ret) && stat_param.subpart_stat_param_.need_modify_) {  // process subpart stats
    for (int64_t i = 0; OB_SUCC(ret) && i < stat_param.subpart_infos_.count(); i++) {
      PartInfo part = stat_param.subpart_infos_.at(i);
      if (OB_FAIL(part_ids.push_back(part.part_id_))) {
        LOG_WARN("failed to push back failed part_info_id", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && stat_param.part_stat_param_.need_modify_) {  // process part stats
    for (int64_t i = 0; OB_SUCC(ret) && i < stat_param.part_infos_.count(); i++) {
      PartInfo part = stat_param.part_infos_.at(i);
      if (OB_FAIL(part_ids.push_back(part.part_id_))) {
        LOG_WARN("failed to push back failed part_info_id", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < stat_param.approx_part_infos_.count(); i++) {
      PartInfo part = stat_param.approx_part_infos_.at(i);
      if (OB_FAIL(part_ids.push_back(part.part_id_))) {
        LOG_WARN("failed to push back failed part_info_id", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && stat_param.global_stat_param_.need_modify_) {  // process part stats
    if (OB_FAIL(part_ids.push_back(stat_param.global_part_id_))) {
      LOG_WARN("failed to push back failed part_info_id", K(ret));
    }
  }
  return ret;
}

int ObDbmsStatsExecutor::get_skip_rate_sample_count(ObMySQLProxy *mysql_proxy,
                                                    const uint64_t tenant_id,
                                                    const uint64_t table_id,
                                                    int64_t &sample_count)
{
  int ret = OB_SUCCESS;
  ObObj result;
  ObSkipRateSamplePrefs skip_rate_sample_pref;
  sample_count = DEFAULT_SKIP_RATE_SAMPLE_COUNT;
  ObArenaAllocator tmp_alloc("OptStatPrefs", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id);
  if (OB_FAIL(ObDbmsStatsPreferences::get_prefs(
          mysql_proxy, tmp_alloc, tenant_id, table_id, skip_rate_sample_pref.get_stat_pref_name(), result))) {
    LOG_WARN("failed to get prefs", K(ret));
  } else if (!result.is_null()) {
    ObCastCtx cast_ctx(&tmp_alloc, NULL, CM_NONE, ObCharset::get_system_collation());
    ObObj dest_obj;
    if (OB_FAIL(ObObjCaster::to_type(ObNumberType, cast_ctx, result, dest_obj))) {
      LOG_WARN("failed to cast number to double type", K(ret));
    } else if (OB_FAIL(dest_obj.get_number().extract_valid_int64_with_trunc(sample_count))) {
      LOG_WARN("failed to get extract valid int64 with trunc ", K(ret));
    } else if (sample_count < 0 || sample_count > MAX_SKIP_RATE_SAMPLE_COUNT) {
      ret = OB_ERR_DBMS_STATS_PL;
      LOG_WARN( "Illegal skip rate sample count, must in [0, MAX_SKIP_RATE_SAMPLE_COUNT]", K(ret), K(sample_count));
    } else {
      LOG_TRACE("Succeed to get skip rate sample count", K(sample_count));
    }
  }
  return ret;
}

int ObDbmsStatsExecutor::get_stats_collect_batch_size(ObMySQLProxy *mysql_proxy,
                                                      const uint64_t tenant_id,
                                                      const uint64_t table_id,
                                                      int64_t &batch_part_size)
{
  int ret = OB_SUCCESS;
  ObObj result;
  ObString opt_name("GATHER_STATS_BATCH_SIZE");
  ObArenaAllocator tmp_alloc("OptStatPrefs", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id);
  if (OB_FAIL(ObDbmsStatsPreferences::get_prefs(mysql_proxy, tmp_alloc, tenant_id, table_id, opt_name, result))) {
    LOG_WARN("failed to get prefs", K(ret));
  } else if (!result.is_null()) {
    ObCastCtx cast_ctx(&tmp_alloc, NULL, CM_NONE, ObCharset::get_system_collation());
    ObObj dest_obj;
    if (OB_FAIL(ObObjCaster::to_type(ObNumberType, cast_ctx, result, dest_obj))) {
      LOG_WARN("failed to cast number to double type", K(ret));
    } else if (OB_FAIL(dest_obj.get_number().extract_valid_int64_with_trunc(batch_part_size))) {
      LOG_WARN("failed to get extract valid int64 with trunc ", K(ret));
    } else if (batch_part_size < 0) {
      ret = OB_ERR_DBMS_STATS_PL;
      LOG_WARN("Illegal auto gather stats batch size must greater than 0", K(ret), K(batch_part_size));
    } else {
      LOG_TRACE("Succeed to get table gather stats batch size", K(batch_part_size));
    }
  }
  return ret;
}

int ObDbmsStatsExecutor::mark_internal_stat(const int64_t global_part_id,
                                            ObIArray<ObOptTableStat *> &all_tstats,
                                            ObIArray<ObOptColumnStat *> &all_cstats)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < all_tstats.count(); i++) {
    if (OB_ISNULL(all_tstats.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else if (all_tstats.at(i)->get_partition_id() != global_part_id) {
      all_tstats.at(i)->set_is_interal();
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < all_cstats.count(); i++) {
    if (OB_ISNULL(all_cstats.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else if (all_cstats.at(i)->get_partition_id() != global_part_id) {
      all_cstats.at(i)->set_is_interal();
    }
  }
  return ret;
}

}  // namespace common
}  // namespace oceanbase
