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
namespace oceanbase {
using namespace pl;
namespace common {

/**
 * @brief ObDbmsStatsUtils::gather_table_stats
 *  构造表级别统计信息的收集 SQL
 * @return
 */
int ObDbmsStatsExecutor::gather_table_stats(ObExecContext &ctx,
                                            const ObTableStatParam &param)
{
  int ret = OB_SUCCESS;
  LOG_TRACE("begin to gather table stats", K(param));
  ObArray<ObOptTableStat *> all_tstats;
  ObArray<ObOptColumnStat *> all_cstats;
  ObSEArray<ObOptStat, 4> approx_opt_part_stats;
  ObExtraParam extra;
  extra.start_time_ = ObTimeUtility::current_time();
  if (OB_ISNULL(param.allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(param.allocator_));
  } else if (OB_FAIL(extra.partition_id_block_map_.create(10000,
                                                          ObModIds::OB_HASH_BUCKET_TABLE_STATISTICS,
                                                          ObModIds::OB_HASH_BUCKET_TABLE_STATISTICS,
                                                          param.tenant_id_))) {
    LOG_WARN("failed to create hash map", K(ret));
  } else if (param.need_estimate_block_ &&
             share::schema::ObTableType::EXTERNAL_TABLE != param.ref_table_type_ &&
             OB_FAIL(ObBasicStatsEstimator::estimate_block_count(ctx, param,
                                                                 extra.partition_id_block_map_))) {
    LOG_WARN("failed to estimate block count", K(ret));
  }
  if (OB_SUCC(ret) && param.subpart_stat_param_.need_modify_) {
    extra.type_ = SUBPARTITION_LEVEL;
    extra.nth_part_ = 0;
    extra.need_histogram_ = param.subpart_stat_param_.gather_histogram_;
    ObSEArray<ObOptStat, 4> opt_stats;
    if (param.part_level_ != share::schema::PARTITION_LEVEL_TWO) {
      /*do nothing*/
    } else if (OB_FAIL(do_gather_stats(ctx, param, extra, approx_opt_part_stats, opt_stats))) {
      LOG_WARN("failed to gather subpartition stats", K(ret));
    } else if (OB_FAIL(ObDbmsStatsUtils::calssify_opt_stat(opt_stats,
                                                           all_tstats,
                                                           all_cstats))) {
      LOG_WARN("failed to calssify opt stat", K(ret));
    }
  }
  if (OB_SUCC(ret) && param.part_stat_param_.need_modify_) {
    extra.type_ = PARTITION_LEVEL;
    extra.nth_part_ = 0;
    extra.need_histogram_ = param.part_stat_param_.gather_histogram_;
    ObSEArray<ObOptStat, 4> opt_stats;
    if (param.part_level_ == share::schema::PARTITION_LEVEL_ZERO) {
      /*do nothing*/
    } else if (OB_FAIL(do_gather_stats(ctx, param, extra, approx_opt_part_stats, opt_stats))) {
      LOG_WARN("failed to gather partition stats", K(ret));
    } else if (OB_FAIL(ObDbmsStatsUtils::calssify_opt_stat(opt_stats,
                                                           all_tstats,
                                                           all_cstats))) {
      LOG_WARN("failed to calssify opt stat", K(ret));
    }
  }
  if (OB_SUCC(ret) &&
      param.global_stat_param_.need_modify_ &&
      !param.global_stat_param_.gather_approx_) {
    extra.type_ = TABLE_LEVEL;
    extra.nth_part_ = 0;
    extra.need_histogram_ = param.global_stat_param_.gather_histogram_;
    ObSEArray<ObOptStat, 4> opt_stats;
    if (OB_FAIL(do_gather_stats(ctx, param, extra, approx_opt_part_stats, opt_stats))) {
      LOG_WARN("failed to gather table stats", K(ret));
    } else if (OB_FAIL(ObDbmsStatsUtils::calssify_opt_stat(opt_stats,
                                                           all_tstats,
                                                           all_cstats))) {
      LOG_WARN("failed to calssify opt stat", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    ObSEArray<ObOptTableStatHandle, 4> history_tab_handles;
    ObSEArray<ObOptColumnStatHandle, 4> history_col_handles;
    //before write, we need record history stats.
    if (!param.is_temp_table_ &&
        OB_FAIL(ObDbmsStatsHistoryManager::get_history_stat_handles(ctx, param,
                                                                    history_tab_handles,
                                                                    history_col_handles))) {
      LOG_WARN("failed to get history stat handles", K(ret));
    } else if (OB_FAIL(ObDbmsStatsUtils::split_batch_write(ctx, all_tstats, all_cstats))) {
      LOG_WARN("failed to split batch write", K(ret));
    } else if (!param.is_temp_table_ &&
               OB_FAIL(ObDbmsStatsUtils::batch_write_history_stats(ctx,
                                                                   history_tab_handles,
                                                                   history_col_handles))) {
      LOG_WARN("failed to batch write history stats", K(ret));
    } else if (share::schema::ObTableType::EXTERNAL_TABLE != param.ref_table_type_ &&
               OB_FAIL(ObBasicStatsEstimator::update_last_modified_count(ctx, param))) {
      LOG_WARN("failed to update last modified count", K(ret));
    } else {/*do nothing*/}
  }

  if (extra.partition_id_block_map_.created()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = extra.partition_id_block_map_.destroy())) {
      LOG_WARN("failed to destroy hash map", K(ret), K(tmp_ret));
      ret = COVER_SUCC(tmp_ret);
    }
  }
  return ret;
}

int ObDbmsStatsExecutor::do_gather_stats(ObExecContext &ctx,
                                         const ObTableStatParam &param,
                                         ObExtraParam &extra,
                                         ObIArray<ObOptStat> &approx_part_opt_stats,
                                         ObIArray<ObOptStat> &opt_stats)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(param.allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(param.allocator_));
  } else if (OB_FAIL(init_opt_stats(*param.allocator_, param, extra, opt_stats))) {
    LOG_WARN("failed to init opt stats", K(ret));
  } else if (opt_stats.empty()) {
    /*do nothing*/
  } else {
    ObBasicStatsEstimator basic_est(ctx, *param.allocator_);
    ObHybridHistEstimator hybrid_est(ctx, *param.allocator_);
    if (OB_FAIL(basic_est.estimate(param, extra, opt_stats))) {
      LOG_WARN("failed to estimate basic statistics", K(ret));
    } else if (extra.need_histogram_ && OB_FAIL(hybrid_est.estimate(param, extra, opt_stats))) {
      LOG_WARN("failed to estimate hybrid histogram", K(ret));
    } else {/*do nothing*/}
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObIncrementalStatEstimator::try_derive_global_stat(ctx, param,
                                                                   extra, approx_part_opt_stats,
                                                                   opt_stats))) {
      LOG_WARN("failed to try derive global stat", K(ret));
    } else if (OB_FAIL(check_all_cols_range_skew(param, opt_stats))) {
      LOG_WARN("failed to check all cols range skew", K(ret));
    } else {/*do nothing*/}
  }
  return ret;
}

int ObDbmsStatsExecutor::check_all_cols_range_skew(const ObTableStatParam &param,
                                                   ObIArray<ObOptStat> &opt_stats)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < opt_stats.count(); ++i) {
    ObIArray<ObOptColumnStat *> &col_stats = opt_stats.at(i).column_stats_;
    if (OB_UNLIKELY(param.column_params_.count() != col_stats.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(param.column_params_.count()),
                                       K(col_stats.count()), K(ret));
    } else {
      for (int64_t j = 0; OB_SUCC(ret) && j < param.column_params_.count(); ++j) {
        const ObColumnStatParam &col_param = param.column_params_.at(j);
        if (col_param.is_size_skewonly() || col_param.is_size_auto()) {
          if (OB_ISNULL(col_stats.at(j))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null", K(ret), K(col_stats.at(j)));
          } else {
            ObHistogram &hist = col_stats.at(j)->get_histogram();
            if ((hist.get_type() == ObHistType::FREQUENCY && col_param.is_size_skewonly()) ||
                hist.get_type() == ObHistType::HYBIRD) {
              if (OB_UNLIKELY(hist.get_bucket_size() < 1 || col_param.bucket_num_ < 1)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("get unexpected error", K(ret), K(hist.get_bucket_size()),
                                                 K(col_param.bucket_num_), K(*col_stats.at(j)));
              } else {
                bool is_even_dist = false;
                int64_t standard_cnt = hist.get_type() == ObHistType::FREQUENCY ?
                                         hist.get_buckets().at(0).endpoint_num_ :
                                         hist.get_sample_size() / col_param.bucket_num_;
                if (OB_FAIL(ObDbmsStatsUtils::check_range_skew(hist.get_type(),
                                                               hist.get_buckets(),
                                                               standard_cnt,
                                                               is_even_dist))) {
                  LOG_WARN("failed to check range skew", K(ret));
                } else if (is_even_dist) {//Evenly distributed, no need to build a histogram.
                  LOG_TRACE("check hist range skew is evenly distributed", K(hist.get_type()));
                  hist.reset();
                }
              }
            } else {/*do nothing*/}
          }
        } else {/*do nothing*/}
      }
    }
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
  ObArenaAllocator alloc(ObModIds::OB_BUFFER);
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
    ObSEArray<ObOptTableStatHandle, 4> history_tab_handles;
    ObSEArray<ObOptColumnStatHandle, 4> history_col_handles;
    if (OB_FAIL(do_set_table_stats(param, &table_stat))) {
      LOG_WARN("failed to do set table stats", K(ret));
    ////before update, we need record history stats.
    } else if (!param.table_param_.is_temp_table_ &&
               OB_FAIL(ObDbmsStatsHistoryManager::get_history_stat_handles(ctx, param.table_param_,
                                                                           history_tab_handles,
                                                                           history_col_handles))) {
      LOG_WARN("failed to get history stat handles", K(ret));
    } else if (OB_FAIL(mgr.update_table_stat(param.table_param_.tenant_id_,
                                             &table_stat,
                                             param.table_param_.is_index_stat_))) {
      LOG_WARN("failed to update table stats", K(ret));
    } else if (OB_FAIL(ObDbmsStatsUtils::batch_write_history_stats(ctx,
                                                                   history_tab_handles,
                                                                   history_col_handles))) {
      LOG_WARN("failed to batch write history stats", K(ret));
    } else {
      LOG_TRACE("end set table stats", K(param), K(table_stat));
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
      if (OB_FAIL(do_set_column_stats(param, col_stat))) {
        LOG_WARN("failed to do set table stats", K(ret));
      } else if (OB_FAIL(column_stats.push_back(col_stat))) {
        LOG_WARN("failed to push back column stat", K(ret));
      } else {
        ObMySQLTransaction trans;
        if (OB_FAIL(trans.start(ctx.get_sql_proxy(), param.table_param_.tenant_id_))) {
          LOG_WARN("fail to start transaction", K(ret));
        } else if (OB_FAIL(mgr.update_column_stat(ctx.get_virtual_table_ctx().schema_guard_,
                                                  param.table_param_.tenant_id_,
                                                  trans,
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

int ObDbmsStatsExecutor::do_set_column_stats(const ObSetColumnStatParam &param,
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
    //5.set hist_param TODO @jiangxiu.wt
    //other options support later.
    LOG_TRACE("succeed to do set column stats", K(*column_stat));
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
    PartInfo part_info;
    ObExtraParam extra;
    extra.type_ = TABLE_LEVEL;
    extra.nth_part_ = 0;
    if (OB_FAIL(ObDbmsStatsUtils::get_part_info(param, extra, part_info))) {
      LOG_WARN("failed to get part info", K(ret));
    } else if (OB_FAIL(part_ids.push_back(part_info.part_id_))) {
      LOG_WARN("failed to push back partition id", K(ret));
    } else if (param.stattype_ == NULL_TYPE) {
      /*do nothing*/
    } else if (OB_FAIL(no_stats_partition_ids.push_back(part_info.part_id_))) {
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
    ObSEArray<ObOptTableStatHandle, 4> history_tab_handles;
    ObSEArray<ObOptColumnStatHandle, 4> history_col_handles;
    int64_t affected_rows = 0;
    //before delete, we need record history stats.
    if (!param.is_temp_table_ &&
        OB_FAIL(ObDbmsStatsHistoryManager::get_history_stat_handles(ctx, param,
                                                                    history_tab_handles,
                                                                    history_col_handles))) {
      LOG_WARN("failed to get history stat handles", K(ret));
    } else if (OB_FAIL(ObOptStatManager::get_instance().delete_table_stat(param.tenant_id_,
                                                                          table_id,
                                                                          part_ids,
                                                                          cascade_columns,
                                                                          affected_rows))) {
      LOG_WARN("failed to delete table stats", K(ret));
    } else if (affected_rows != 0 && !param.is_temp_table_ &&
               OB_FAIL(ObDbmsStatsUtils::batch_write_history_stats(ctx,
                                                                   history_tab_handles,
                                                                   history_col_handles))) {
      LOG_WARN("failed to batch write history stats", K(ret));
    } else if (OB_FAIL(reset_table_locked_state(ctx, param, no_stats_partition_ids, part_stattypes))) {
      LOG_WARN("failed to reset table locked state", K(ret));
    } else {/*do nothing*/}
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
      PartInfo part_info;
      ObExtraParam extra;
      extra.type_ = TABLE_LEVEL;
      extra.nth_part_ = 0;
      if (OB_FAIL(ObDbmsStatsUtils::get_part_info(param, extra, part_info))) {
        LOG_WARN("failed to get part info", K(ret));
      } else if (OB_FAIL(part_ids.push_back(part_info.part_id_))) {
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
                                                                    only_histogram))) {
      LOG_WARN("failed to delete column stat", K(ret));
    }
  }
  return ret;
}

int ObDbmsStatsExecutor::init_opt_stats(ObIAllocator &allocator,
                                        const ObTableStatParam &param,
                                        ObExtraParam &extra,
                                        ObIArray<ObOptStat> &opt_stats)
{
  int ret = OB_SUCCESS;
  if (TABLE_LEVEL == extra.type_) {
    ObOptStat stat;
    if (OB_FAIL(init_opt_stat(allocator, param, extra, stat))) {
      LOG_WARN("failed to init opt stat");
    } else if (OB_FAIL(opt_stats.push_back(stat))) {
      LOG_WARN("failed to push back stat");
    } else {/*do nothing*/}
  } else if (PARTITION_LEVEL == extra.type_) {
    for (int64_t i = 0; OB_SUCC(ret) && i < param.part_infos_.count(); ++i) {
      extra.nth_part_ = i;
      ObOptStat stat;
      if (OB_FAIL(init_opt_stat(allocator, param, extra, stat))) {
        LOG_WARN("failed to init opt stat");
      } else if (OB_FAIL(opt_stats.push_back(stat))) {
        LOG_WARN("failed to push back stat");
      } else {/*do nothing*/}
    }
  } else if (SUBPARTITION_LEVEL == extra.type_) {
    for (int64_t i = 0; OB_SUCC(ret) && i < param.subpart_infos_.count(); ++i) {
      extra.nth_part_ = i;
      ObOptStat stat;
      if (OB_FAIL(init_opt_stat(allocator, param, extra, stat))) {
        LOG_WARN("failed to init opt stat");
      } else if (OB_FAIL(opt_stats.push_back(stat))) {
        LOG_WARN("failed to push back stat");
      } else {/*do nothing*/}
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected type", K(ret), K(extra.type_));
  }
  return ret;
}

int ObDbmsStatsExecutor::init_opt_stat(ObIAllocator &allocator,
                                       const ObTableStatParam &param,
                                       const ObExtraParam &extra,
                                       ObOptStat &stat)
{
  int ret = OB_SUCCESS;
  void *ptr = NULL;
  PartInfo part_info;
  BolckNumPair block_num_pair;
  ObOptTableStat *&tab_stat = stat.table_stat_;
  if (OB_FAIL(stat.column_stats_.prepare_allocate(param.column_params_.count())))  {
    LOG_WARN("failed to prepare allocate column stat", K(ret));
  } else if (OB_ISNULL(ptr = allocator.alloc(sizeof(ObOptTableStat)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("memory is not enough", K(ret), K(ptr));
  } else if (OB_FAIL(ObDbmsStatsUtils::get_part_info(param, extra, part_info))) {
    LOG_WARN("failed to get partition info", K(ret));
  } else {
    LOG_TRACE("prepare opt stat", K(part_info), K(extra.type_), K(param.part_level_));
    tab_stat = new (ptr) ObOptTableStat();
    tab_stat->set_table_id(param.table_id_);
    tab_stat->set_partition_id(part_info.part_id_);
    tab_stat->set_object_type(extra.type_);
    tab_stat->set_stattype_locked(part_info.part_stattype_);
    if (OB_FAIL(extra.partition_id_block_map_.get_refactored(part_info.part_id_, block_num_pair))) {
      if (OB_LIKELY(OB_HASH_NOT_EXIST == ret)) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to get refactored", K(ret));
      }
    } else {
      tab_stat->set_macro_block_num(block_num_pair.first);
      tab_stat->set_micro_block_num(block_num_pair.second);
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < param.column_params_.count(); ++i) {
    ObOptColumnStat *&col_stat = stat.column_stats_.at(i);
    if (OB_ISNULL(col_stat = ObOptColumnStat::malloc_new_column_stat(allocator))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("memory is not enough", K(ret), K(ptr));
    } else {
      col_stat->set_table_id(param.column_params_.at(i).need_basic_stat() ? param.table_id_: -1);
      col_stat->set_partition_id(part_info.part_id_);
      col_stat->set_stat_level(extra.type_);
      col_stat->set_column_id(param.column_params_.at(i).column_id_);
      col_stat->set_collation_type(param.column_params_.at(i).cs_type_);
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
  ObArray<ObOptColumnStat *> empty_cstats;
  ObExtraParam extra;
  extra.start_time_ = ObTimeUtility::current_time();
  LOG_TRACE("begin gather index stats", K(param));
  if (OB_FAIL(extra.partition_id_block_map_.create(10000,
                                                   ObModIds::OB_HASH_BUCKET_TABLE_STATISTICS,
                                                   ObModIds::OB_HASH_BUCKET_TABLE_STATISTICS,
                                                   param.tenant_id_))) {
    LOG_WARN("failed to create hash map", K(ret));
  } else if (OB_FAIL(ObBasicStatsEstimator::estimate_block_count(ctx, param,
                                                                 extra.partition_id_block_map_))) {
    LOG_WARN("failed to estimate block count", K(ret));
  }
  if (OB_SUCC(ret) && param.subpart_stat_param_.need_modify_) {
    extra.type_ = SUBPARTITION_LEVEL;
    extra.nth_part_ = 0;
    if (param.part_level_ != share::schema::PARTITION_LEVEL_TWO) {
      /*do nothing*/
    } else if (OB_FAIL(do_gather_index_stats(ctx, param, extra, all_index_stats))) {
      LOG_WARN("failed to gather subpartition stats", K(ret));
    } else {/*do nothing*/}
  }
  if (OB_SUCC(ret) && param.part_stat_param_.need_modify_) {
    extra.type_ = PARTITION_LEVEL;
    extra.nth_part_ = 0;
    if (param.part_level_ == share::schema::PARTITION_LEVEL_ZERO) {
      /*do nothing*/
    } else if (OB_FAIL(do_gather_index_stats(ctx, param, extra, all_index_stats))) {
      LOG_WARN("failed to gather partition stats", K(ret));
    } else {/*do nothing*/}
  }
  if (OB_SUCC(ret) && (param.global_stat_param_.need_modify_)) {
    extra.type_ = TABLE_LEVEL;
    extra.nth_part_ = 0;
    if (OB_FAIL(do_gather_index_stats(ctx, param, extra, all_index_stats))) {
      LOG_WARN("failed to gather table stats", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObDbmsStatsUtils::split_batch_write(ctx, all_index_stats, empty_cstats, true))) {
      LOG_WARN("failed to split batch write", K(ret));
    } else {/*do nothing*/}
  }

  if (extra.partition_id_block_map_.created()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = extra.partition_id_block_map_.destroy())) {
      LOG_WARN("failed to destroy hash map", K(ret), K(tmp_ret));
      ret = COVER_SUCC(tmp_ret);
    }
  }
  return ret;
}

int ObDbmsStatsExecutor::do_gather_index_stats(ObExecContext &ctx,
                                               const ObTableStatParam &param,
                                               ObExtraParam &extra,
                                               ObIArray<ObOptTableStat *> &all_index_stats)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObOptStat, 4> opt_stats;
  if (OB_ISNULL(param.allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(param.allocator_));
  } else if (OB_FAIL(init_opt_stats(*param.allocator_, param, extra, opt_stats))) {
    LOG_WARN("failed to init opt stats", K(ret));
  } else if (opt_stats.empty()) {
    /*do nothing*/
  } else {
    ObIndexStatsEstimator index_est(ctx, *param.allocator_);
    if (OB_FAIL(index_est.estimate(param, extra, opt_stats))) {
      LOG_WARN("failed to estimate basic statistics", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < opt_stats.count(); ++i) {
        if (OB_ISNULL(opt_stats.at(i).table_stat_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret), K(opt_stats.at(i).table_stat_));
        } else if (OB_FAIL(all_index_stats.push_back(opt_stats.at(i).table_stat_))) {
          LOG_WARN("failed to append", K(ret));
        } else {/*do nothing*/}
      }
    }
  }
  return ret;
}

int ObDbmsStatsExecutor::update_online_stat(ObExecContext &ctx,
                                            ObTableStatParam &param,
                                            share::schema::ObSchemaGetterGuard *schema_guard,
                                            const TabStatIndMap &online_table_stats,
                                            const ColStatIndMap &online_column_stats)
{
  int ret = OB_SUCCESS;

  int64_t affected_rows = 0;
  ObSEArray<ObOptTableStatHandle, 4> cur_tab_handles;
  ObSEArray<ObOptColumnStatHandle, 4> cur_col_handles;
  ObSEArray<ObOptTableStat *, 4>  table_stats;
  ObSEArray<ObOptColumnStat *, 4> column_stats;
  if (OB_FAIL(ObDbmsStatsLockUnlock::check_stat_locked(ctx, param))) {
    if (ret == OB_ERR_DBMS_STATS_PL) {
      param.global_stat_param_.reset_gather_stat();
      param.part_stat_param_.reset_gather_stat();
      param.subpart_stat_param_.reset_gather_stat();
      ret = OB_SUCCESS; // ignore lock check error
    }
    LOG_WARN("fail to check lock stat", K(ret));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObDbmsStatsUtils::get_current_opt_stats(param,
                                                             cur_tab_handles,
                                                             cur_col_handles))) {
    LOG_WARN("failed to get current opt stats", K(ret));
  } else if (OB_FAIL(ObDbmsStatsUtils::merge_tab_stats(param,
                                                       online_table_stats,
                                                       cur_tab_handles,
                                                       table_stats))) {
    LOG_WARN("fail to merge tab stats", K(ret), K(cur_tab_handles));
  } else if (OB_FAIL(ObDbmsStatsUtils::merge_col_stats(param,
                                                       online_column_stats,
                                                       cur_col_handles,
                                                       column_stats))) {
    LOG_WARN("fail to merge col stats", K(ret), K(cur_col_handles));
  } else if (OB_FAIL(ObDbmsStatsUtils::split_batch_write(ctx, table_stats, column_stats,
                                                         false, false, true))) {
    LOG_WARN("fail to update stat", K(ret), K(table_stats), K(column_stats));
  } else if (OB_FAIL(ObBasicStatsEstimator::update_last_modified_count(ctx, param))) {
    LOG_WARN("failed to update last modified count", K(ret));
  } else if (OB_FAIL(pl::ObDbmsStats::update_stat_cache(ctx.get_my_session()->get_rpc_tenant_id(), param))) {
    LOG_WARN("fail to update stat cache", K(ret));
  } else {
    // should reuse stats out-side this function.
  }

  return ret;
}

} // namespace common
} // namespace oceanbase

