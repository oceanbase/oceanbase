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
#include "share/stat/ob_incremental_stat_estimator.h"
#include "share/stat/ob_dbms_stats_executor.h"
#include "share/stat/ob_basic_stats_estimator.h"
#include "share/stat/ob_hybrid_hist_estimator.h"
#include "share/stat/ob_opt_stat_manager.h"
#include "share/stat/ob_dbms_stats_utils.h"
#include "share/stat/ob_stat_item.h"
#include "share/stat/ob_topk_hist_estimator.h"
#include "pl/sys_package/ob_dbms_stats.h"
#include "share/stat/ob_dbms_stats_history_manager.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase {
using namespace pl;
namespace common {

int ObIncrementalStatEstimator::derive_global_stat_by_direct_load(ObExecContext &ctx,
                                                                  const uint64_t table_id)
{
  int ret = OB_SUCCESS;
  const share::schema::ObTableSchema *table_schema = NULL;
  share::schema::ObSchemaGetterGuard *schema_guard = ctx.get_virtual_table_ctx().schema_guard_;
  ObSEArray<ObOptStat, 4> part_opt_stats;
  ObSEArray<ObOptStat, 4> all_derive_opt_stats;
  ObSEArray<ObOptTableStat, 4> part_tab_stats;
  ObSEArray<ObOptColumnStatHandle, 4> part_col_handles;
  ObArenaAllocator alloc("ObIncrStats", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  SMART_VAR(ObTableStatParam, param) {
    param.allocator_ = &alloc;
    if (OB_FAIL(gen_opt_stat_param_by_direct_load(ctx, alloc, table_id, param))) {
      LOG_WARN("failed to gen opt stat param by direct load", K(ret));
    } else if (param.part_level_ != share::schema::PARTITION_LEVEL_ONE &&
              param.part_level_ != share::schema::PARTITION_LEVEL_TWO) {
      //if we don't derive, we need update some cache info.
      if (OB_FAIL(ObBasicStatsEstimator::update_last_modified_count(ctx, param))) {
        LOG_WARN("failed to update last modified count", K(ret));
      } else if (OB_FAIL(pl::ObDbmsStats::update_stat_cache(ctx.get_my_session()->get_rpc_tenant_id(), param))) {
        LOG_WARN("fail to update stat cache", K(ret));
      }
    } else if (OB_FAIL(get_all_part_opt_stats(param,
                                              param.part_level_ == share::schema::PARTITION_LEVEL_ONE ?
                                                param.all_part_infos_ : param.all_subpart_infos_,
                                              part_tab_stats,
                                              part_col_handles,
                                              part_opt_stats))) {
      LOG_WARN("failed to get all part opt stats", K(ret));
    } else {
      bool nee_derive_part = param.part_level_ == share::schema::PARTITION_LEVEL_TWO;
      //derive part stat first
      if (nee_derive_part) {
        ObSEArray<ObOptStat, 1> empty_opt_stats;
        if (OB_FAIL(do_derive_part_stats_from_subpart_stats(ctx, alloc, param, empty_opt_stats,
                                                            part_opt_stats, all_derive_opt_stats))) {
          LOG_WARN("failed to do derive part stats from subpart stats", K(ret));
        }
      }
      //derive global stat
      if (OB_SUCC(ret)) {
        ObOptStat global_opt_stat;
        bool need_derive_hist = false;
        if (OB_FAIL(do_derive_global_stat(ctx, alloc, param,
                                          nee_derive_part ? all_derive_opt_stats : part_opt_stats,
                                          need_derive_hist,
                                          TABLE_LEVEL, param.global_part_id_,
                                          global_opt_stat))) {
          LOG_WARN("Failed to derive global stat from part stat", K(ret));
        } else if (OB_FAIL(all_derive_opt_stats.push_back(global_opt_stat))) {
          LOG_WARN("faield to push back", K(ret));
        }
      }
      //write all stat
      if (OB_SUCC(ret)) {
        ObSEArray<ObOptTableStat *, 4> all_tstats;
        ObSEArray<ObOptColumnStat *, 4> all_cstats;
        //direct load does't process history stats.
        if (OB_FAIL(ObDbmsStatsUtils::calssify_opt_stat(all_derive_opt_stats,
                                                        all_tstats,
                                                        all_cstats))) {
          LOG_WARN("failed to calssify opt stat", K(ret));
        } else if (OB_FAIL(ObDbmsStatsUtils::split_batch_write(ctx, all_tstats, all_cstats))) {
          LOG_WARN("failed to split batch write", K(ret));
        } else if (OB_FAIL(ObBasicStatsEstimator::update_last_modified_count(ctx, param))) {
          LOG_WARN("failed to update last modified count", K(ret));
        } else if (OB_FAIL(pl::ObDbmsStats::update_stat_cache(ctx.get_my_session()->get_rpc_tenant_id(), param))) {
          LOG_WARN("fail to update stat cache", K(ret));
        }
      }
    }
    LOG_TRACE("succeed to derive global stat by direct load", K(param), K(part_tab_stats));
  }
  return ret;
}

int ObIncrementalStatEstimator::derive_global_stat_from_part_stats(
    ObExecContext &ctx,
    const ObTableStatParam &param,
    const ObIArray<ObOptStat> &part_opt_stats,
    ObOptStat &global_opt_stat)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObOptTableStat, 4> table_stats;
  ObSEArray<ObOptColumnStatHandle, 4> col_handles;
  ObSEArray<ObOptStat, 4> tmp_opt_stats;
  ObSEArray<int64_t, 4> no_regather_part_ids;
  bool need_derive_hist = true;
  if (OB_ISNULL(param.allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(param));
  } else if (part_opt_stats.empty()) {
    /*do nothing*/
  } else if (OB_FAIL(tmp_opt_stats.assign(part_opt_stats))) {
    LOG_WARN("failed to assign", K(ret));
  } else {
    //if specify part/subpart name, need get other already existed part stats, but only derive base
    //stat, not derive histogram stats.
    if (!param.part_name_.empty()) {
      if (OB_UNLIKELY(tmp_opt_stats.count() != 1)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error, expected array count is 1", K(ret),
                                                                    K(tmp_opt_stats.count()));
      } else if (OB_FAIL(get_table_and_column_stats(tmp_opt_stats.at(0),
                                                    param,
                                                    table_stats,
                                                    col_handles))) {
        LOG_WARN("failed to get table and column stats", K(ret));
      } else if (OB_FAIL(generate_all_opt_stat(table_stats,
                                               col_handles,
                                               param.column_params_.count(),
                                               tmp_opt_stats))) {
        LOG_WARN("failed to generate all opt stat", K(ret));
      } else {
        need_derive_hist = false;
      }
    //get regather partition stats(stats is locked or not stale).
    } else {
      int64_t cur_part_id = OB_INVALID_ID;
      for (int64_t i = 0; OB_SUCC(ret) && i < param.no_regather_partition_ids_.count(); ++i) {
        if (!ObDbmsStatsUtils::is_subpart_id(param.all_subpart_infos_,
                                             param.no_regather_partition_ids_.at(i),
                                             cur_part_id)) {
          if (OB_FAIL(no_regather_part_ids.push_back(param.no_regather_partition_ids_.at(i)))) {
            LOG_WARN("failed to push back", K(ret));
          }
        }
      }
      if (OB_SUCC(ret)) {
        ObSEArray<uint64_t, 4> column_ids;
        if (OB_FAIL(get_column_ids(param.column_params_, column_ids))) {
          LOG_WARN("failed to get column ids", K(ret));
        } else if (OB_FAIL(get_no_regather_partition_stats(param.tenant_id_, param.table_id_, column_ids,
                                                           no_regather_part_ids, table_stats,
                                                           col_handles, tmp_opt_stats))) {
          LOG_WARN("failed to get locked partition stats", K(ret));
        } else {/*do nothing*/}
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(do_derive_global_stat(ctx, *param.allocator_, param, tmp_opt_stats, need_derive_hist,
                                        TABLE_LEVEL,param.global_part_id_,
                                        global_opt_stat))) {
        LOG_WARN("Failed to derive global stat from part stat", K(ret));
      } else {
        LOG_TRACE("Succeed to derive global stat from part stats", K(global_opt_stat), K(param));
      }
    }
  }
  return ret;
}

int ObIncrementalStatEstimator::derive_part_stats_from_subpart_stats(
    ObExecContext &ctx,
    const ObTableStatParam &param,
    const ObIArray<ObOptStat> &gather_subpart_opt_stats,
    ObIArray<ObOptStat> &approx_part_opt_stats)
{
  int ret = OB_SUCCESS;
  ObOptStat global_opt_stat;
  ObSEArray<ObOptTableStat, 4> no_regather_table_stats;
  ObSEArray<ObOptColumnStatHandle, 4> no_regather_col_handles;
  ObSEArray<ObOptStat, 4> no_regather_subpart_opt_stats;
  if (OB_ISNULL(param.allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(param));
  } else if (param.approx_part_infos_.empty()) {
    /*do nothing*/
  } else if (OB_FAIL(get_no_regather_subpart_stats(param, no_regather_table_stats,
                                                   no_regather_col_handles,
                                                   no_regather_subpart_opt_stats))) {
    LOG_WARN("failed to get locked partition stats", K(ret));
  } else if (OB_FAIL(do_derive_part_stats_from_subpart_stats(ctx, *param.allocator_, param,
                                                             no_regather_subpart_opt_stats,
                                                             gather_subpart_opt_stats,
                                                             approx_part_opt_stats))) {
    LOG_WARN("failed to do derive part stats from subpart stats", K(ret));
  } else {
    LOG_TRACE("Succeed to derive part stats from subpart stats", K(approx_part_opt_stats.count()),
                                                                 K(gather_subpart_opt_stats.count()),
                                                                 K(param));
  }
  return ret;
}

int ObIncrementalStatEstimator::derive_split_gather_stats(ObExecContext &ctx,
                                                          ObMySQLTransaction &trans,
                                                          const ObTableStatParam &param,
                                                          bool derive_part_stat,
                                                          bool is_all_columns_gather,
                                                          ObIArray<ObOptTableStat *> &all_tstats)
{
  int ret = OB_SUCCESS;
  if (param.part_level_ != share::schema::PARTITION_LEVEL_ONE &&
      param.part_level_ != share::schema::PARTITION_LEVEL_TWO) {
    //do nothing
  } else if (OB_ISNULL(param.allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(param));
  } else if (OB_FAIL(THIS_WORKER.check_status())) {
    LOG_WARN("check status failed", KR(ret));
  } else {
    ObArenaAllocator allocator("IncrementStats", OB_MALLOC_NORMAL_BLOCK_SIZE, param.tenant_id_);
    ObSEArray<ObOptTableStat *, 4> cur_table_stats;
    ObSEArray<ObOptColumnStat *, 4> cur_column_stats;
    ObSEArray<ObOptStat, 4> part_opt_stats;
    ObSEArray<ObOptStat, 4> derive_opt_stats;
    ObTableStatParam new_param;
    if (OB_FAIL(prepare_get_opt_stats_param(param, derive_part_stat, new_param))) {
      LOG_WARN("failed to prepare get opt stats param", K(ret));
    } else if (OB_FAIL(ObDbmsStatsUtils::get_current_opt_stats(allocator,
                                                               trans.get_connection(),
                                                               new_param,
                                                               cur_table_stats,
                                                               cur_column_stats))) {
      LOG_WARN("failed to get current opt stats", K(ret));
    } else if (OB_FAIL(generate_all_opt_stat(cur_table_stats,
                                             cur_column_stats,
                                             param.column_params_.count(),
                                             part_opt_stats))) {
      LOG_WARN("failed to generate all opt stat", K(ret));
    } else if (derive_part_stat) {
      ObSEArray<ObOptStat, 1> empty_opt_stats;
      if (OB_FAIL(do_derive_part_stats_from_subpart_stats(ctx, *param.allocator_, param, empty_opt_stats,
                                                          part_opt_stats, derive_opt_stats))) {
        LOG_WARN("failed to do derive part stats from subpart stats", K(ret));
      }
    } else {
      ObOptStat global_opt_stat;
      bool need_derive_hist = true;
      if (OB_FAIL(do_derive_global_stat(ctx, *param.allocator_, param,
                                        part_opt_stats,
                                        need_derive_hist,
                                        TABLE_LEVEL, param.global_part_id_,
                                        global_opt_stat))) {
        LOG_WARN("Failed to derive global stat from part stat", K(ret));
      } else if (OB_FAIL(derive_opt_stats.push_back(global_opt_stat))) {
        LOG_WARN("faield to push back", K(ret));
      }
    }
    //write all stat
    if (OB_SUCC(ret)) {
      ObSEArray<ObOptTableStat *, 4> tmp_all_tstats;
      ObSEArray<ObOptColumnStat *, 4> all_cstats;
      if (OB_FAIL(ObDbmsStatsUtils::calssify_opt_stat(derive_opt_stats,
                                                      tmp_all_tstats,
                                                      all_cstats))) {
        LOG_WARN("failed to calssify opt stat", K(ret));
      } else if (OB_FAIL(ObDbmsStatsUtils::merge_split_gather_tab_stats(all_tstats, tmp_all_tstats))) {
        LOG_WARN("failed to merge split gather tab stats", K(ret));
      } else if (OB_FAIL(ObDbmsStatsUtils::split_batch_write(ctx, trans.get_connection(),
                                                              is_all_columns_gather ? all_tstats : tmp_all_tstats,
                                                              all_cstats))) {
        LOG_WARN("failed to split batch write", K(ret));
      }
    }
    LOG_TRACE("succeed to derive split gather stats", K(param), K(derive_part_stat), K(part_opt_stats), K(derive_opt_stats));
  }
  return ret;
}

int ObIncrementalStatEstimator::do_derive_part_stats_from_subpart_stats(
    ObExecContext &ctx,
    ObIAllocator &alloc,
    const ObTableStatParam &param,
    const ObIArray<ObOptStat> &no_regather_subpart_opt_stats,
    const ObIArray<ObOptStat> &gather_subpart_opt_stats,
    ObIArray<ObOptStat> &approx_part_opt_stats)
{
  int ret = OB_SUCCESS;
  int64_t cur_part_id = OB_INVALID_ID;
  for (int64_t i = 0; OB_SUCC(ret) && i < param.approx_part_infos_.count(); ++i) {
    ObOptStat opt_part_stat;
    ObSEArray<ObOptStat, 4> subpart_opt_stats;
    //get no regather subpart stats
    for (int64_t j = 0; OB_SUCC(ret) && j < no_regather_subpart_opt_stats.count(); ++j) {
      const ObOptStat &tmp_subpart_opt_stat = no_regather_subpart_opt_stats.at(j);
      if (OB_ISNULL(tmp_subpart_opt_stat.table_stat_) ||
          OB_UNLIKELY(!ObDbmsStatsUtils::is_subpart_id(param.all_subpart_infos_,
                                                       tmp_subpart_opt_stat.table_stat_->get_partition_id(),
                                                       cur_part_id))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(ret), K(tmp_subpart_opt_stat.table_stat_));
      } else if (param.approx_part_infos_.at(i).part_id_ == cur_part_id) {
        if (OB_FAIL(subpart_opt_stats.push_back(tmp_subpart_opt_stat))) {
          LOG_WARN("failed to push back", K(ret));
        } else {/*do nothing*/}
      } else {/*do nothing*/}
    }
    //get regather subpart stats
    for (int64_t j = 0; OB_SUCC(ret) && j < gather_subpart_opt_stats.count(); ++j) {
      if (OB_ISNULL(gather_subpart_opt_stats.at(j).table_stat_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(ret), K(gather_subpart_opt_stats.at(j).table_stat_));
      } else if (ObDbmsStatsUtils::is_subpart_id(param.all_subpart_infos_,
                                                 gather_subpart_opt_stats.at(j).table_stat_->get_partition_id(),
                                                 cur_part_id)) {
        if (param.approx_part_infos_.at(i).part_id_ == cur_part_id) {
          if (OB_FAIL(subpart_opt_stats.push_back(gather_subpart_opt_stats.at(j)))) {
            LOG_WARN("failed to push back", K(ret));
          } else {/*do nothing*/}
        } else {/*do nothing*/}
      } else {/*do nothing*/}
    }
    //derive part stat from subpart stats
    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(subpart_opt_stats.count() != param.approx_part_infos_.at(i).subpart_cnt_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(ret), K(subpart_opt_stats.count()),
                                         K(param.approx_part_infos_.at(i)));
      } else if (OB_FAIL(do_derive_global_stat(ctx, alloc, param, subpart_opt_stats, true,
                                               PARTITION_LEVEL, param.approx_part_infos_.at(i).part_id_,
                                               opt_part_stat))) {
        LOG_WARN("Failed to derive global stat from part stat", K(ret));
      } else if (OB_FAIL(approx_part_opt_stats.push_back(opt_part_stat))) {
        LOG_WARN("faield to push back", K(ret));
      } else {/*do nothing*/}
    }
  }
  return ret;
}

int ObIncrementalStatEstimator::generate_all_opt_stat(ObIArray<ObOptTableStat> &table_stats,
                                                      const ObIArray<ObOptColumnStatHandle> &col_handles,
                                                      int64_t col_cnt,
                                                      ObIArray<ObOptStat> &all_opt_stats)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < table_stats.count(); ++i) {
    ObOptStat opt_stat;
    opt_stat.table_stat_ = &table_stats.at(i);
    for (int64_t j = 0; OB_SUCC(ret) && opt_stat.column_stats_.count() < col_cnt && j < col_handles.count(); ++j) {
      if (OB_ISNULL(col_handles.at(j).stat_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(ret), K(col_handles.at(j).stat_));
      } else if (opt_stat.table_stat_->get_partition_id() == col_handles.at(j).stat_->get_partition_id()) {
        if (OB_FAIL(opt_stat.column_stats_.push_back(
                                    const_cast<ObOptColumnStat*>(col_handles.at(j).stat_)))) {
          LOG_WARN("failed to push back col stat", K(ret));
        } else {/*do nothing*/}
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(opt_stat.column_stats_.count() != col_cnt)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(ret), K(opt_stat.column_stats_), K(col_cnt));
      } else if (OB_FAIL(all_opt_stats.push_back(opt_stat))) {
        LOG_WARN("failed to push back opt stat", K(ret));
      } else {/*do nothing*/}
    }
  }
  return ret;
}

int ObIncrementalStatEstimator::generate_all_opt_stat(ObIArray<ObOptTableStat *> &table_stats,
                                                      ObIArray<ObOptColumnStat *> &col_stats,
                                                      int64_t col_cnt,
                                                      ObIArray<ObOptStat> &all_opt_stats)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < table_stats.count(); ++i) {
    ObOptStat opt_stat;
    opt_stat.table_stat_ = table_stats.at(i);
    for (int64_t j = 0; OB_SUCC(ret) && j < col_cnt; ++j) {
      int64_t idx = i * col_cnt + j;
      if (OB_UNLIKELY(idx >= col_stats.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(ret), K(idx), K(col_stats.count()));
      } else if (OB_ISNULL(col_stats.at(idx)) || OB_ISNULL(opt_stat.table_stat_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(ret), K(table_stats), K(col_stats), K(i), K(idx));
      } else if (opt_stat.table_stat_->get_partition_id() == col_stats.at(idx)->get_partition_id()) {
        if (OB_FAIL(opt_stat.column_stats_.push_back(col_stats.at(idx)))) {
          LOG_WARN("failed to push back col stat", K(ret));
        } else {/*do nothing*/}
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(opt_stat.column_stats_.count() != col_cnt)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(ret), K(table_stats), K(col_cnt), K(col_stats));
      } else if (OB_FAIL(all_opt_stats.push_back(opt_stat))) {
        LOG_WARN("failed to push back opt stat", K(ret));
      } else {/*do nothing*/}
    }
  }
  return ret;
}

int ObIncrementalStatEstimator::get_table_and_column_stats(
                                                       ObOptStat &src_opt_stat,
                                                       const ObTableStatParam &param,
                                                       ObIArray<ObOptTableStat> &table_stats,
                                                       ObIArray<ObOptColumnStatHandle> &col_handles)
{
  int ret = OB_SUCCESS;
  int64_t table_id = param.table_id_;
  ObSEArray<int64_t, 4> part_ids;
  ObSEArray<uint64_t, 4> column_ids;
  if (OB_FAIL(get_part_ids_and_column_ids_info(src_opt_stat, param, part_ids, column_ids))) {
    LOG_WARN("failed to get part ids and column ids info", K(ret));
  } else if (OB_FAIL(ObOptStatManager::get_instance().get_table_stat(param.tenant_id_,
                                                                     table_id,
                                                                     part_ids,
                                                                     table_stats))) {
    LOG_WARN("failed to get table stat", K(ret));
  } else if (OB_FAIL(ObOptStatManager::get_instance().get_column_stat(param.tenant_id_,
                                                                      table_id,
                                                                      part_ids,
                                                                      column_ids,
                                                                      col_handles))) {
    LOG_WARN("failed to get column stat", K(ret));
  } else {
    LOG_TRACE("Succeed to get table and column stats", K(table_stats.count()),
                                                       K(col_handles.count()));
  }
  return ret;
}

int ObIncrementalStatEstimator::get_part_ids_and_column_ids_info(ObOptStat &src_opt_stat,
                                                                 const ObTableStatParam &param,
                                                                 ObIArray<int64_t> &part_ids,
                                                                 ObIArray<uint64_t> &column_ids)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(src_opt_stat.column_stats_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error, expected isn't empty", K(ret), K(src_opt_stat.column_stats_));
  } else {
    int64_t no_gather_id = -1;
    for (int64_t i = 0; OB_SUCC(ret) && i < src_opt_stat.column_stats_.count(); ++i) {
      ObOptColumnStat *col_stat = NULL;
      if (OB_ISNULL(col_stat = src_opt_stat.column_stats_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(col_stat));
      } else if (OB_FAIL(column_ids.push_back(col_stat->get_column_id()))) {
        LOG_WARN("failed to push back", K(ret), K(col_stat));
      } else {
        no_gather_id = col_stat->get_partition_id();
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(no_gather_id == -1)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error, expected valid id", K(ret), K(no_gather_id));
      } else if (OB_UNLIKELY(param.all_part_infos_.empty())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error, expected isn't empty", K(ret), K(param.all_part_infos_));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < param.all_part_infos_.count(); ++i) {
          if (param.all_part_infos_.at(i).part_id_ == no_gather_id) {
            /*do nothing*/
          } else if (OB_FAIL(part_ids.push_back(param.all_part_infos_.at(i).part_id_))) {
            LOG_WARN("failed to push back id", K(ret));
          } else {/*do nothing*/}
        }
      }
    }
  }
  return ret;
}

int ObIncrementalStatEstimator::do_derive_global_stat(ObExecContext &ctx,
                                                      ObIAllocator &alloc,
                                                      const ObTableStatParam &param,
                                                      ObIArray<ObOptStat> &part_opt_stats,
                                                      bool need_derive_hist,
                                                      const StatLevel &approx_level,
                                                      const int64_t partition_id,
                                                      ObOptStat &global_opt_stat)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(derive_global_tbl_stat(alloc, param,  approx_level, partition_id,
                                     part_opt_stats, global_opt_stat))) {
    LOG_WARN("failed to derive global tbl stat from part tbl stat", K(ret));
  } else if (OB_FAIL(derive_global_col_stat(ctx, alloc, param, part_opt_stats, need_derive_hist,
                                            approx_level, partition_id, global_opt_stat))) {
    LOG_WARN("failed to derive global col stat from part col stat", K(ret));
  } else {
    LOG_TRACE("Succeed to derive global stat", K(part_opt_stats));
  }
  return ret;
}

int ObIncrementalStatEstimator::derive_global_tbl_stat(ObIAllocator &alloc,
                                                       const ObTableStatParam &param,
                                                       const StatLevel &approx_level,
                                                       const int64_t partition_id,
                                                       ObIArray<ObOptStat> &part_opt_stats,
                                                       ObOptStat &global_opt_stat)
{
  int ret = OB_SUCCESS;
  ObOptTableStat* tmp_tbl_stat = NULL;
  if (OB_UNLIKELY(part_opt_stats.count() <= 0 || approx_level == INVALID_LEVEL ||
                  approx_level == SUBPARTITION_LEVEL) ||
      OB_ISNULL(tmp_tbl_stat = part_opt_stats.at(0).table_stat_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(tmp_tbl_stat), K(part_opt_stats.count()),
                                     K(approx_level), K(ret));
  } else {
    ObOptTableStat *&table_stat = global_opt_stat.table_stat_;
    void *ptr = NULL;
    if (OB_ISNULL(ptr = alloc.alloc(sizeof(ObOptTableStat)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("memory is not enough", K(ret), K(ptr));
    } else {
      table_stat = new (ptr) ObOptTableStat();
      ObGlobalTableStat global_tstat;
      for (int64_t i = 0; OB_SUCC(ret) && i < part_opt_stats.count(); ++i) {
        ObOptTableStat *opt_tbl_stat = part_opt_stats.at(i).table_stat_;
        if (OB_ISNULL(opt_tbl_stat)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret), K(opt_tbl_stat));
        } else {
          global_tstat.add(opt_tbl_stat->get_row_count(),
                           opt_tbl_stat->get_avg_row_size(),
                           opt_tbl_stat->get_data_size(),
                           opt_tbl_stat->get_macro_block_num(),
                           opt_tbl_stat->get_micro_block_num());
        }
      }
      if (OB_SUCC(ret)) {
        //set global table stat
        table_stat->set_table_id(param.table_id_);
        table_stat->set_partition_id(partition_id);
        table_stat->set_object_type(approx_level);
        table_stat->set_row_count(global_tstat.get_row_count());
        table_stat->set_sample_size(global_tstat.get_row_count());
        table_stat->set_avg_row_size(global_tstat.get_avg_row_size());
        table_stat->set_data_size(global_tstat.get_avg_data_size());
        table_stat->set_macro_block_num(global_tstat.get_macro_block_count());
        table_stat->set_micro_block_num(global_tstat.get_micro_block_count());
        table_stat->set_stattype_locked(param.stattype_);
        LOG_TRACE("succeed to derive global tbl stat", K(*table_stat));
      }
    }
  }
  return ret;
}

int ObIncrementalStatEstimator::derive_global_col_stat(ObExecContext &ctx,
                                                       ObIAllocator &alloc,
                                                       const ObTableStatParam &param,
                                                       ObIArray<ObOptStat> &part_opt_stats,
                                                       bool need_derive_hist,
                                                       const StatLevel &approx_level,
                                                       const int64_t partition_id,
                                                       ObOptStat &global_opt_stat)
{
  int ret = OB_SUCCESS;
  int64_t part_cnt = part_opt_stats.count();
  int64_t column_cnt = param.column_params_.count();
  bool need_gather_hist = false;
  ObIArray<ObOptColumnStat *> &col_stats = global_opt_stat.column_stats_;
  if (OB_UNLIKELY(part_cnt <= 0 || column_cnt <= 0 ||
                  approx_level == INVALID_LEVEL || approx_level == SUBPARTITION_LEVEL) ||
      OB_ISNULL(global_opt_stat.table_stat_) ||
      OB_ISNULL(param.allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(part_cnt), K(column_cnt), K(approx_level),
                                     K(global_opt_stat.table_stat_), K(param.allocator_), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt; ++i) {
      ObOptColumnStat *col_stat = NULL;
      if (OB_ISNULL(col_stat = ObOptColumnStat::malloc_new_column_stat(alloc))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("memory is not enough", K(ret), K(col_stat));
      } else {
        ObGlobalMinEval min_eval;
        ObGlobalMaxEval max_eval;
        ObGlobalNullEval null_eval;
        ObGlobalNotNullEval not_null_eval;
        ObGlobalNdvEval ndv_eval;
        ObGlobalAvglenEval avglen_eval;
        ObSEArray<ObHistogram, 4> all_part_histograms;
        bool can_try_derive_hist = need_derive_hist;
        int64_t max_bucket_num = param.column_params_.at(i).bucket_num_;
        for (int64_t j = 0; OB_SUCC(ret) && j < part_cnt; ++j) {
          ObOptColumnStat *opt_col_stat = NULL;
          ObOptTableStat *opt_tab_stat = part_opt_stats.at(j).table_stat_;
          for (int64_t k = 0;
               OB_SUCC(ret) && opt_col_stat == NULL && k < part_opt_stats.at(j).column_stats_.count();
               ++k) {
            if (OB_ISNULL(part_opt_stats.at(j).column_stats_.at(k))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("get unexpected null", K(ret), K(part_opt_stats.at(j).column_stats_), K(k));
            } else if (param.column_params_.at(i).column_id_ ==
                       part_opt_stats.at(j).column_stats_.at(k)->get_column_id()) {
              opt_col_stat = part_opt_stats.at(j).column_stats_.at(k);
            }
          }
          if (OB_FAIL(ret)) {
          } else if (OB_ISNULL(opt_col_stat) || OB_ISNULL(opt_tab_stat)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null", K(ret), K(part_opt_stats.at(j).column_stats_),
                                            K(param.column_params_.at(i)));
          } else if (opt_col_stat->get_num_distinct() == 0 && opt_col_stat->get_num_null() == 0) {
            /*do nothing*/
          } else if (need_derive_hist && opt_col_stat->get_histogram().is_valid() &&
                     OB_FAIL(all_part_histograms.push_back(opt_col_stat->get_histogram()))) {
            LOG_WARN("failed to push back histogram", K(ret));
          } else {
            can_try_derive_hist &= (opt_col_stat->get_num_distinct() == 0 || opt_col_stat->get_histogram().is_valid());
            null_eval.add(opt_col_stat->get_num_null());
            if (opt_col_stat->get_num_distinct() != 0) {
              min_eval.add(opt_col_stat->get_min_value());
              max_eval.add(opt_col_stat->get_max_value());
              ndv_eval.add(opt_col_stat->get_num_distinct(), opt_col_stat->get_llc_bitmap());
              not_null_eval.add(opt_tab_stat->get_row_count() - opt_col_stat->get_num_null());
            }
            if (opt_col_stat->get_avg_len() != 0) {
              avglen_eval.add(opt_col_stat->get_avg_len());
            }
          }
        }
        if (OB_SUCC(ret)) {
          col_stat->set_table_id(param.column_params_.at(i).need_basic_stat() ? param.table_id_ : OB_INVALID_ID);
          col_stat->set_partition_id(partition_id);
          col_stat->set_column_id(param.column_params_.at(i).column_id_);
          col_stat->set_stat_level(approx_level);
          col_stat->set_num_null(null_eval.get());
          col_stat->set_num_not_null(not_null_eval.get());
          col_stat->get_histogram().set_sample_size(not_null_eval.get());
          col_stat->set_num_distinct(ndv_eval.get());
          col_stat->set_avg_len(avglen_eval.get());
          ndv_eval.get_llc_bitmap(col_stat->get_llc_bitmap(), col_stat->get_llc_bitmap_size());
          col_stat->set_llc_bitmap_size(ObOptColumnStat::NUM_LLC_BUCKET);
          col_stat->set_collation_type(param.column_params_.at(i).cs_type_);
          ObObj new_min_obj, new_max_obj;
          //maybe the stat is from KVCACHE, need deep copy min/max obj.
          if (OB_FAIL(ob_write_obj(alloc, min_eval.get(), new_min_obj)) ||
              FALSE_IT(col_stat->set_min_value(new_min_obj))) {
            LOG_WARN("failed to set min value", K(ret), K(min_eval.get()), K(new_min_obj));
          } else if (OB_FAIL(ob_write_obj(alloc, max_eval.get(), new_max_obj)) ||
                     FALSE_IT(col_stat->set_max_value(new_max_obj))) {
            LOG_WARN("failed to set max value", K(ret), K(max_eval.get()), K(new_max_obj));
          } else if (OB_FAIL(col_stats.push_back(col_stat))) {
            LOG_WARN("failed to push back", K(ret));
          } else if (need_derive_hist) {
            if (can_try_derive_hist && col_stat->get_num_distinct() > 0) {
              if (OB_FAIL(derive_global_histogram(all_part_histograms,
                                                  alloc,
                                                  max_bucket_num,
                                                  global_opt_stat.table_stat_->get_row_count(),
                                                  col_stat->get_num_not_null(),
                                                  col_stat->get_num_distinct(),
                                                  col_stat->get_histogram(),
                                                  need_gather_hist))) {
                LOG_WARN("failed to derive global histogram from part histogram", K(ret));
              }
            } else if (max_bucket_num > 1 &&
                       param.column_params_.at(i).need_basic_stat() &&
                       col_stat->get_num_distinct() > 0) {
              need_gather_hist = true;
              int64_t max_disuse_cnt = std::ceil(col_stat->get_num_not_null() * 1.0 / max_bucket_num);
              //After testing, the error of using hyperloglog to estimate ndv is within %5.
              const double MAX_LLC_NDV_ERR_RATE = !param.need_approx_ndv_ ? 0.0 : 0.05;
              const int64_t fault_tolerance_cnt = std::ceil(col_stat->get_num_distinct() * MAX_LLC_NDV_ERR_RATE);
              if (col_stat->get_num_distinct() >= max_bucket_num + max_disuse_cnt + fault_tolerance_cnt) {
                //directly gather hybrid histogram
                col_stat->get_histogram().set_type(ObHistType::HYBIRD);
              } else {
                //otherwise, try gather top frequery histogram
                col_stat->get_histogram().set_type(ObHistType::TOP_FREQUENCY);
              }
            }
            LOG_TRACE("succeed to derive global col stat", K(*col_stat));
          }
        }
      }
    }
    if (OB_SUCC(ret) && need_gather_hist) {
      ObOptStatGatherParam gather_param;
      ObTopkHistEstimator topk_est(ctx, *param.allocator_);
      ObHybridHistEstimator hybrid_est(ctx, *param.allocator_);
      ObSEArray<ObOptStat, 1> tmp_opt_stats;
      if (OB_FAIL(ObDbmsStatsUtils::prepare_gather_stat_param(param, approx_level, NULL, false,
                                                              DEFAULT_STAT_GATHER_VECTOR_BATCH_SIZE,
                                                              false, gather_param))) {
        LOG_WARN("failed to assign", K(ret));
      } else if (OB_FAIL(gather_param.column_params_.assign(param.column_params_))) {
        LOG_WARN("failed to assign", K(ret));
      } else if (gather_param.stat_level_ != TABLE_LEVEL &&
                 OB_FAIL(gather_param.partition_infos_.assign(param.approx_part_infos_))) {
        LOG_WARN("failed to assign", K(ret));
      } else if (gather_param.stat_level_ != TABLE_LEVEL &&
                 OB_FAIL(ObDbmsStatsUtils::remove_stat_gather_param_partition_info(global_opt_stat.table_stat_->get_partition_id(),
                                                                                   gather_param))) {
        LOG_WARN("failed to remove stat gather param partition info", K(ret));
      } else if (OB_FAIL(topk_est.estimate(gather_param, global_opt_stat))) {
        LOG_WARN("failed to estimate topk histogram", K(ret));
      } else if (OB_FAIL(hybrid_est.estimate(gather_param, global_opt_stat))) {
        LOG_WARN("failed to estimate hybrid histogram", K(ret));
      } else if (OB_FAIL(tmp_opt_stats.push_back(global_opt_stat))) {
        LOG_WARN("failed to push back", K(ret));
      } else if (OB_FAIL(ObDbmsStatsUtils::check_all_cols_range_skew(gather_param.column_params_,
                                                                     tmp_opt_stats))) {
        LOG_WARN("failed to check all cols range skew", K(ret));
      } else {
        LOG_TRACE("succeed to gather hist", K(gather_param), K(global_opt_stat));
      }
    }
  }
  return ret;
}

int ObIncrementalStatEstimator::derive_global_histogram(ObIArray<ObHistogram> &all_part_histograms,
                                                        common::ObIAllocator &allocator,
                                                        int64_t max_bucket_num,
                                                        int64_t total_row_count,
                                                        int64_t not_null_count,
                                                        int64_t num_distinct,
                                                        ObHistogram &histogram,
                                                        bool &need_gather_hist)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObHistBucket, 4> all_bkts;
  histogram.reset();
  void *ptr = NULL;
  if (max_bucket_num <= 1) {
    //do nothing
  } else if (OB_ISNULL(ptr = allocator.alloc(sizeof(ObTopKFrequencyHistograms)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret), K(ptr));
  } else {
    ObTopKFrequencyHistograms *top_k_fre_hist = new (ptr) ObTopKFrequencyHistograms();
    top_k_fre_hist->set_window_size(1000);
    top_k_fre_hist->set_item_size(256);
    top_k_fre_hist->set_is_topk_hist_need_des_row(true);
    top_k_fre_hist->set_max_disuse_cnt(std::ceil(not_null_count * 1.0 / max_bucket_num));
    for (int64_t i = 0; OB_SUCC(ret) && i < all_part_histograms.count(); ++i) {
      if (all_part_histograms.at(i).is_valid()) {
        if (all_part_histograms.at(i).get_type() == ObHistType::FREQUENCY ||
            all_part_histograms.at(i).get_type() == ObHistType::TOP_FREQUENCY ||
            all_part_histograms.at(i).get_type() == ObHistType::HYBIRD) {
          const ObHistogram::Buckets &part_bkts = all_part_histograms.at(i).get_buckets();
          for (int64_t j = 0; OB_SUCC(ret) && j < part_bkts.count(); ++j) {
            if (OB_FAIL(top_k_fre_hist->add_top_k_frequency_item(part_bkts.at(j).endpoint_value_,
                                                                 part_bkts.at(j).endpoint_repeat_count_))) {
              LOG_WARN("failed to add topk frequency item", K(ret));
            }
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected hist type", K(ret), K(all_part_histograms.at(i).get_type()));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(all_part_histograms.at(i)));
      }
    }
    if (OB_SUCC(ret)) {
      ObHistogram tmp_histogram;
      if (OB_FAIL(top_k_fre_hist->create_topk_fre_items())) {
        LOG_WARN("failed to adjust frequency sort", K(ret));
      } else if (top_k_fre_hist->get_buckets().count() == 0) {
        //directly to gather hybrid histogram
        histogram.set_type(ObHistType::HYBIRD);
        need_gather_hist = true;
      } else if (OB_FAIL(ObStatTopKHist::build_histogram_from_topk_items(
                                                                      allocator,
                                                                      top_k_fre_hist->get_buckets(),
                                                                      max_bucket_num,
                                                                      total_row_count,
                                                                      not_null_count,
                                                                      num_distinct,
                                                                      tmp_histogram))) {
        LOG_WARN("failed to try build topk histogram", K(ret));
      } else if (OB_FAIL(histogram.deep_copy(allocator, tmp_histogram))) {
        LOG_WARN("failed to deep copy", K(ret));
      } else {
        need_gather_hist |= histogram.is_hybrid();
      }
    }
    if (top_k_fre_hist != NULL) {
      top_k_fre_hist->~ObTopKFrequencyHistograms();
      top_k_fre_hist = NULL;
    }
  }
  return ret;
}

int ObIncrementalStatEstimator::get_no_regather_partition_stats(
    const uint64_t tenant_id,
    const uint64_t table_id,
    const ObIArray<uint64_t> &column_ids,
    const ObIArray<int64_t> &no_regather_partition_ids,
    ObIArray<ObOptTableStat> &no_regather_table_stats,
    ObIArray<ObOptColumnStatHandle> &no_regather_col_handles,
    ObIArray<ObOptStat> &part_opt_stats)
{
  int ret = OB_SUCCESS;
  if (no_regather_partition_ids.empty()) {
    /*do nothing*/
  } else if (OB_FAIL(ObOptStatManager::get_instance().get_table_stat(tenant_id,
                                                                     table_id,
                                                                     no_regather_partition_ids,
                                                                     no_regather_table_stats))) {
    LOG_WARN("failed to get table stat", K(ret));
  } else if (OB_FAIL(ObOptStatManager::get_instance().get_column_stat(tenant_id,
                                                                      table_id,
                                                                      no_regather_partition_ids,
                                                                      column_ids,
                                                                      no_regather_col_handles))) {
    LOG_WARN("failed to get column stat", K(ret));
  } else if (OB_FAIL(generate_all_opt_stat(no_regather_table_stats,
                                           no_regather_col_handles,
                                           column_ids.count(),
                                           part_opt_stats))) {
    LOG_WARN("failed to generate all opt stat", K(ret));
  } else {
    LOG_TRACE("Succeed to get locked partition stats", K(no_regather_partition_ids));
  }
  return ret;
}

int ObIncrementalStatEstimator::get_column_ids(const ObIArray<ObColumnStatParam> &column_params,
                                               ObIArray<uint64_t> &column_ids)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < column_params.count(); ++i) {
    if (OB_FAIL(column_ids.push_back(column_params.at(i).column_id_))) {
      LOG_WARN("failed to push back", K(ret));
    } else {/*do nothing*/}
  }
  return ret;
}

int ObIncrementalStatEstimator::get_need_hybrid_part_infos(const ObTableStatParam &param,
                                                           const ObIArray<ObOptStat> &need_hybrid_hist_opt_stats,
                                                           ObIArray<PartInfo> &hybrid_part_infos)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < need_hybrid_hist_opt_stats.count(); ++i) {
    if (OB_ISNULL(need_hybrid_hist_opt_stats.at(i).table_stat_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(need_hybrid_hist_opt_stats.at(i).table_stat_));
    } else {
      bool find_it = false;
      for (int64_t j = 0; OB_SUCC(ret) && !find_it && j < param.approx_part_infos_.count(); ++j) {
        if (need_hybrid_hist_opt_stats.at(i).table_stat_->get_partition_id() ==
                                                        param.approx_part_infos_.at(j).part_id_) {
          if (OB_FAIL(hybrid_part_infos.push_back(param.approx_part_infos_.at(j)))) {
            LOG_WARN("failed to push back", K(ret));
          } else {
            find_it = true;
          }
        } else {/*do noting*/}
      }
      if (OB_SUCC(ret) && !find_it) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(ret), K(param.approx_part_infos_),
                                          K(*need_hybrid_hist_opt_stats.at(i).table_stat_));
      }
    }
  }
  return ret;
}

int ObIncrementalStatEstimator::get_no_regather_subpart_stats(
    const ObTableStatParam &param,
    ObIArray<ObOptTableStat> &no_regather_table_stats,
    ObIArray<ObOptColumnStatHandle> &no_regather_col_handles,
    ObIArray<ObOptStat> &subpart_opt_stats)
{
  int ret = OB_SUCCESS;
  ObSEArray<int64_t, 4> derive_need_no_regather_subpart_ids;
  int64_t part_id = OB_INVALID_ID;
  for (int64_t i = 0; OB_SUCC(ret) && i < param.approx_part_infos_.count(); ++i) {
    for (int64_t j = 0; OB_SUCC(ret) && j < param.no_regather_partition_ids_.count(); ++j) {
      if (ObDbmsStatsUtils::is_subpart_id(param.all_subpart_infos_,
                                          param.no_regather_partition_ids_.at(j),
                                          part_id)) {
        if (part_id == param.approx_part_infos_.at(i).part_id_) {
          if (OB_FAIL(derive_need_no_regather_subpart_ids.push_back(
                                                         param.no_regather_partition_ids_.at(j)))) {
            LOG_WARN("failed to push back", K(ret));
          }
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    ObSEArray<uint64_t, 4> column_ids;
    if (OB_FAIL(get_column_ids(param.column_params_, column_ids))) {
      LOG_WARN("failed to get column ids", K(ret));
    } else if (OB_FAIL(get_no_regather_partition_stats(param.tenant_id_,
                                                       param.table_id_,
                                                       column_ids,
                                                       derive_need_no_regather_subpart_ids,
                                                       no_regather_table_stats,
                                                       no_regather_col_handles,
                                                       subpart_opt_stats))) {
      LOG_WARN("failed to get no regather partition stats", K(ret));
    } else {/*do nothing*/}
  }
  return ret;
}

int ObIncrementalStatEstimator::gen_opt_stat_param_by_direct_load(ObExecContext &ctx,
                                                                  ObIAllocator &alloc,
                                                                  const uint64_t table_id,
                                                                  ObTableStatParam &param)
{
  int ret = OB_SUCCESS;
  const share::schema::ObTableSchema *table_schema = NULL;
  share::schema::ObSchemaGetterGuard *schema_guard = ctx.get_virtual_table_ctx().schema_guard_;
  param.table_id_ = table_id;
  ObSEArray<int64_t, 4> part_ids;
  ObSEArray<int64_t, 4> subpart_ids;
  if (OB_ISNULL(schema_guard = ctx.get_virtual_table_ctx().schema_guard_) ||
       OB_ISNULL(ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(schema_guard), K(ctx.get_my_session()));
  } else if (OB_FAIL(schema_guard->get_table_schema(ctx.get_my_session()->get_effective_tenant_id(),
                                                    table_id,
                                                    table_schema))) {
    LOG_WARN("failed to get table schema", K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret));
  } else if (OB_FAIL(ObDbmsStatsUtils::get_part_infos(*table_schema,
                                                      alloc,
                                                      param.part_infos_,
                                                      param.subpart_infos_,
                                                      part_ids,
                                                      subpart_ids))) {
    LOG_WARN("failed to get partition infos", K(ret));
  } else if (OB_FAIL(param.all_part_infos_.assign(param.part_infos_)) ||
             OB_FAIL(param.all_subpart_infos_.assign(param.subpart_infos_))) {
    LOG_WARN("failed to assign", K(ret));
  } else if (OB_FAIL(pl::ObDbmsStats::init_column_stat_params(alloc,
                                                              *schema_guard,
                                                              *table_schema,
                                                              param.column_params_))) {
    LOG_WARN("failed to init column stat params", K(ret));
  } else {
    for (int64_t i = 0; i < param.column_params_.count(); ++i) {
      if (param.column_params_.at(i).is_valid_opt_col()) {
        param.column_params_.at(i).set_need_basic_stat();
      }
    }
    param.tenant_id_ = ctx.get_my_session()->get_effective_tenant_id();
    param.part_level_ = table_schema->get_part_level();
    param.global_stat_param_.set_gather_stat(true);
    param.part_stat_param_.set_gather_stat(table_schema->get_part_level() == share::schema::ObPartitionLevel::PARTITION_LEVEL_TWO);
    param.subpart_stat_param_.set_gather_stat();
    if (OB_FAIL(pl::ObDbmsStats::set_param_global_part_id(ctx, param))) {
      LOG_WARN("failed to set param globa part id", K(ret));
    } else if (table_schema->get_part_level() == share::schema::ObPartitionLevel::PARTITION_LEVEL_TWO) {
      if (OB_FAIL(param.approx_part_infos_.assign(param.part_infos_))) {
        LOG_WARN("failed to assign", K(ret));
      } else {
        param.part_infos_.reset();
      }
    }
    LOG_TRACE("succeed to gen opt stat param by direct load", K(param));
  }
  return ret;
}

int ObIncrementalStatEstimator::get_all_part_opt_stats(
    const ObTableStatParam param,
    const ObIArray<PartInfo> &partition_infos,
    ObIArray<ObOptTableStat> &part_tab_stats,
    ObIArray<ObOptColumnStatHandle> &part_col_handles,
    ObIArray<ObOptStat> &part_opt_stats)
{
  int ret = OB_SUCCESS;
  ObSEArray<int64_t ,4> partition_ids;
  ObSEArray<uint64_t, 4> column_ids;
  if (OB_UNLIKELY(param.part_level_ != share::schema::PARTITION_LEVEL_ONE &&
                  param.part_level_ != share::schema::PARTITION_LEVEL_TWO)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(param));
  } else if (OB_FAIL(get_partition_ids(partition_infos, partition_ids))) {
    LOG_WARN("failed to get column ids", K(ret));
  } else if (OB_FAIL(get_column_ids(param.column_params_, column_ids))) {
    LOG_WARN("failed to get column ids", K(ret));
  } else if (OB_FAIL(ObOptStatManager::get_instance().get_table_stat(param.tenant_id_,
                                                                     param.table_id_,
                                                                     partition_ids,
                                                                     part_tab_stats))) {
    LOG_WARN("failed to get table stat", K(ret));
  } else if (OB_FAIL(ObOptStatManager::get_instance().get_column_stat(param.tenant_id_,
                                                                      param.table_id_,
                                                                      partition_ids,
                                                                      column_ids,
                                                                      part_col_handles))) {
    LOG_WARN("failed to get column stat", K(ret));
  } else if (OB_FAIL(generate_all_opt_stat(part_tab_stats,
                                           part_col_handles,
                                           column_ids.count(),
                                           part_opt_stats))) {
    LOG_WARN("failed to generate all opt stat", K(ret));
  } else {
    LOG_TRACE("Succeed get all part opt stats", K(param), K(part_tab_stats));
  }
  return ret;
}

int ObIncrementalStatEstimator::get_partition_ids(const ObIArray<PartInfo> &partition_infos,
                                                  ObIArray<int64_t> &partition_ids)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < partition_infos.count(); ++i) {
    if (OB_FAIL(partition_ids.push_back(partition_infos.at(i).part_id_))) {
      LOG_WARN("failed to push back", K(ret));
    }
  }
  return ret;
}

int ObIncrementalStatEstimator::derive_part_index_stat_by_subpart_index_stats(
    const ObTableStatParam &param,
    const ObIArray<ObOptTableStat *> &subpart_index_stats,
    ObIArray<ObOptTableStat *> &part_index_stats)
{
  int ret = OB_SUCCESS;
  int64_t cur_part_id = OB_INVALID_ID;
  for (int64_t i = 0; OB_SUCC(ret) && i < param.approx_part_infos_.count(); ++i) {
    ObSEArray<ObOptStat, 4> subpart_opt_stats;
    for (int64_t j = 0; OB_SUCC(ret) && j < subpart_index_stats.count(); ++j) {
      if (OB_ISNULL(subpart_index_stats.at(j))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(ret), K(subpart_index_stats.at(j)));
      } else if (ObDbmsStatsUtils::is_subpart_id(param.all_subpart_infos_,
                                                 subpart_index_stats.at(j)->get_partition_id(),
                                                 cur_part_id)) {
        if (param.approx_part_infos_.at(i).part_id_ == cur_part_id) {
          ObOptStat tmp_opt_stat;
          tmp_opt_stat.table_stat_ = subpart_index_stats.at(j);
          if (OB_FAIL(subpart_opt_stats.push_back(tmp_opt_stat))) {
            LOG_WARN("failed to push back", K(ret));
          } else {/*do nothing*/}
        } else {/*do nothing*/}
      } else {/*do nothing*/}
    }
    //derive part stat from subpart stats
    if (OB_SUCC(ret)) {
      ObOptStat opt_part_stat;
      if (OB_ISNULL(param.allocator_) ||
          OB_UNLIKELY(subpart_opt_stats.count() != param.approx_part_infos_.at(i).subpart_cnt_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(ret), K(param), K(subpart_opt_stats.count()),
                                         K(param.approx_part_infos_.at(i)));
      } else if (OB_FAIL(derive_global_tbl_stat(*param.allocator_, param,
                                                PARTITION_LEVEL,
                                                param.approx_part_infos_.at(i).part_id_,
                                                subpart_opt_stats,
                                                opt_part_stat))) {
        LOG_WARN("failed to derive global tbl stat from part tbl stat", K(ret));
      } else if (OB_FAIL(part_index_stats.push_back(opt_part_stat.table_stat_))) {
        LOG_WARN("failed to push back", K(ret));
      } else {
        LOG_TRACE("succeed to derive part index stat by subpart index stats", K(param),
                                                       K(subpart_index_stats), K(part_index_stats));
      }
    }
  }
  return ret;
}

int ObIncrementalStatEstimator::derive_global_index_stat_by_part_index_stats(
    const ObTableStatParam &param,
    const ObIArray<ObOptTableStat *> &part_index_stats,
    ObIArray<ObOptTableStat *> &all_index_stats)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObOptStat, 4> part_opt_stats;
  for (int64_t i = 0; OB_SUCC(ret) && i < part_index_stats.count(); ++i) {
    ObOptStat tmp_opt_stat;
    tmp_opt_stat.table_stat_ = part_index_stats.at(i);
    ret = part_opt_stats.push_back(tmp_opt_stat);
  }
  if (OB_SUCC(ret)) {
    ObOptStat global_opt_stat;
    if (OB_ISNULL(param.allocator_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(ret), K(param.allocator_));
    } else if (OB_FAIL(derive_global_tbl_stat(*param.allocator_, param,
                                              TABLE_LEVEL,
                                              param.global_part_id_,
                                              part_opt_stats,
                                              global_opt_stat))) {
      LOG_WARN("failed to derive global tbl stat from part tbl stat", K(ret));
    } else if (OB_FAIL(all_index_stats.push_back(global_opt_stat.table_stat_))) {
      LOG_WARN("failed to push back", K(ret));
    } else {
      LOG_TRACE("succeed to derive global index stat by part index stats", K(param), K(all_index_stats));
    }
  }
  return ret;
}

int ObIncrementalStatEstimator::prepare_get_opt_stats_param(const ObTableStatParam &param,
                                                            bool derive_part_stat,
                                                            ObTableStatParam &new_param)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(new_param.assign(param))) {
    LOG_WARN("failed to assign", K(ret));
  } else {
    //reset relation param
    new_param.global_stat_param_.reset_gather_stat();
    new_param.part_stat_param_.reset_gather_stat();
    new_param.subpart_stat_param_.reset_gather_stat();
    new_param.part_infos_.reset();
    new_param.approx_part_infos_.reset();
    new_param.subpart_infos_.reset();
    new_param.all_part_infos_.reset();
    new_param.all_subpart_infos_.reset();
    if (!derive_part_stat) {
      new_param.part_stat_param_.set_gather_stat();
      if (OB_FAIL(new_param.part_infos_.assign(param.all_part_infos_))) {
        LOG_WARN("failed to assign", K(ret));
      }
    } else {
      new_param.subpart_stat_param_.set_gather_stat();
      for (int64_t i = 0; OB_SUCC(ret) && i < param.approx_part_infos_.count(); ++i) {
        int64_t subpart_cnt = 0;
        for (int64_t j = 0;
            OB_SUCC(ret) && subpart_cnt < param.approx_part_infos_.at(i).subpart_cnt_ && j < param.all_subpart_infos_.count();
            ++j) {
          if (param.approx_part_infos_.at(i).part_id_ == param.all_subpart_infos_.at(j).first_part_id_) {
            if (OB_FAIL(new_param.subpart_infos_.push_back(param.all_subpart_infos_.at(j)))) {
              LOG_WARN("failed to push back", K(ret));
            } else {
              ++ subpart_cnt;
            }
          }
        }
        if (OB_UNLIKELY(subpart_cnt != param.approx_part_infos_.at(i).subpart_cnt_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected error", K(ret), K(subpart_cnt), K(param));
        }
      }
    }
  }
  return ret;
}

} // namespace common
} // namespace oceanbase
