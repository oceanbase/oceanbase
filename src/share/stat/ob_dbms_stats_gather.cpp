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
#include "share/stat/ob_dbms_stats_gather.h"
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
 * @return
 */
int ObDbmsStatsGather::gather_stats(ObExecContext &ctx,
                                    const ObOptStatGatherParam &param,
                                    ObIArray<ObOptStat> &opt_stats)
{
  int ret = OB_SUCCESS;
  LOG_TRACE("begin to gather table stats", K(param));
  if (OB_ISNULL(param.allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(param.allocator_));
  } else if (OB_FAIL(init_opt_stats(*param.allocator_, param, opt_stats))) {
    LOG_WARN("failed to init opt stats", K(ret));
  } else if (!opt_stats.empty()) {
    //1.firstly esimate basic stat
    ObBasicStatsEstimator basic_est(ctx, *param.allocator_);
    if (OB_FAIL(basic_est.estimate(param, opt_stats))) {
      LOG_WARN("failed to estimate basic statistics", K(ret));
    } else if (param.need_histogram_) {
      for (int64_t i = 0; OB_SUCC(ret) && i < opt_stats.count(); ++i) {
        ObOptStatGatherParam new_param;
        ObTopkHistEstimator topk_est(ctx, *param.allocator_);
        ObHybridHistEstimator hybrid_est(ctx, *param.allocator_);
        if (OB_ISNULL(opt_stats.at(i).table_stat_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected error", K(ret), K(opt_stats.at(i).table_stat_));
        } else if (OB_FAIL(THIS_WORKER.check_status())) {
          LOG_WARN("check status failed", KR(ret));
        } else if (opt_stats.at(i).table_stat_->get_row_count() <= 0) {
          //empty table or empty partition, no need gather histogram, just skip.
        } else if (OB_FAIL(new_param.assign(param))) {
          LOG_WARN("failed to assign", K(ret));
        } else if (new_param.stat_level_ != TABLE_LEVEL &&
                   OB_FAIL(ObDbmsStatsUtils::remove_stat_gather_param_partition_info(opt_stats.at(i).table_stat_->get_partition_id(),
                                                                                     new_param))) {
          LOG_WARN("failed to remove stat gather param partition info", K(ret));
        } else if (OB_FAIL(classfy_column_histogram(new_param, opt_stats.at(i)))) {
          LOG_WARN("failed to classfy column histogram", K(ret));
        } else if (OB_FAIL(topk_est.estimate(new_param, opt_stats.at(i)))) {
          LOG_WARN("failed to estimate topk histogram", K(ret));
        } else if (OB_FAIL(hybrid_est.estimate(new_param, opt_stats.at(i)))) {
          LOG_WARN("failed to estimate hybrid histogram", K(ret));
        }
      }
    } else {/*do nothing*/}
  }
  return ret;
}

int ObDbmsStatsGather::classfy_column_histogram(const ObOptStatGatherParam &param,
                                                ObOptStat &opt_stat)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(opt_stat.table_stat_) ||
      OB_UNLIKELY(param.column_params_.count() != opt_stat.column_stats_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(opt_stat), K(param));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < param.column_params_.count(); ++i) {
      const ObColumnStatParam &col_param = param.column_params_.at(i);
      ObOptColumnStat *dst_col_stat = opt_stat.column_stats_.at(i);
      if (OB_UNLIKELY(dst_col_stat->get_column_id() != col_param.column_id_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(ret), KPC(dst_col_stat), K(col_param));
      } else if (col_param.need_basic_stat() &&
                 col_param.bucket_num_ > 1 &&
                 dst_col_stat->get_num_distinct() > 0 &&
                 dst_col_stat->get_num_not_null() > 0) {
        int64_t max_disuse_cnt = std::ceil(dst_col_stat->get_num_not_null() * 1.0 / col_param.bucket_num_);
        //After testing, the error of using hyperloglog to estimate ndv is within %5.
        const double MAX_LLC_NDV_ERR_RATE = !param.need_approx_ndv_ ? 0.0 : 0.05;
        const int64_t fault_tolerance_cnt = std::ceil(dst_col_stat->get_num_distinct() * MAX_LLC_NDV_ERR_RATE);
        double sample_val = dst_col_stat->get_histogram().get_sample_size() * 100.0 / dst_col_stat->get_num_not_null();
        if (dst_col_stat->get_num_distinct() >= col_param.bucket_num_ + max_disuse_cnt + fault_tolerance_cnt ||
            sample_val < 100.0 * (1.0 - 1.0 / col_param.bucket_num_)) {
          //directly gather hybrid histogram
          dst_col_stat->get_histogram().set_type(ObHistType::HYBIRD);
        } else {
          //otherwise, try gather top frequery histogram
          dst_col_stat->get_histogram().set_type(ObHistType::TOP_FREQUENCY);
        }
      }
    }
  }
  return ret;
}

int ObDbmsStatsGather::init_opt_stats(ObIAllocator &allocator,
                                      const ObOptStatGatherParam &param,
                                      ObIArray<ObOptStat> &opt_stats)
{
  int ret = OB_SUCCESS;
  if (TABLE_LEVEL == param.stat_level_) {
    ObOptStat stat;
    if (OB_FAIL(init_opt_stat(allocator, param, param.global_part_id_, param.stattype_, stat))) {
      LOG_WARN("failed to init opt stat", K(ret));
    } else if (OB_FAIL(opt_stats.push_back(stat))) {
      LOG_WARN("failed to push back stat", K(ret));
    } else {/*do nothing*/}
  } else if (PARTITION_LEVEL == param.stat_level_ ||
             SUBPARTITION_LEVEL == param.stat_level_) {
    for (int64_t i = 0; OB_SUCC(ret) && i < param.partition_infos_.count(); ++i) {
      ObOptStat stat;
      if (OB_FAIL(init_opt_stat(allocator, param, param.partition_infos_.at(i).part_id_,
                                param.partition_infos_.at(i).part_stattype_, stat))) {
        LOG_WARN("failed to init opt stat");
      } else if (OB_FAIL(opt_stats.push_back(stat))) {
        LOG_WARN("failed to push back stat");
      } else {/*do nothing*/}
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected stat level", K(ret), K(param.stat_level_));
  }
  return ret;
}

int ObDbmsStatsGather::init_opt_stat(ObIAllocator &allocator,
                                     const ObOptStatGatherParam &param,
                                     const int64_t part_id,
                                     const int64_t part_stattype,
                                     ObOptStat &stat)
{
  int ret = OB_SUCCESS;
  void *ptr = NULL;
  BlockNumStat *block_num_stat = NULL;
  ObOptTableStat *&tab_stat = stat.table_stat_;
  if (OB_FAIL(stat.column_stats_.prepare_allocate(param.column_params_.count())))  {
    LOG_WARN("failed to prepare allocate column stat", K(ret));
  } else if (OB_ISNULL(ptr = allocator.alloc(sizeof(ObOptTableStat)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("memory is not enough", K(ret), K(ptr));
  } else {
    tab_stat = new (ptr) ObOptTableStat();
    tab_stat->set_table_id(param.table_id_);
    tab_stat->set_partition_id(part_id);
    tab_stat->set_object_type(param.stat_level_);
    tab_stat->set_stattype_locked(part_stattype);
    if (OB_ISNULL(param.partition_id_block_map_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(ret), K(param.partition_id_block_map_));
    } else if (OB_FAIL(param.partition_id_block_map_->get_refactored(part_id, block_num_stat))) {
      if (OB_LIKELY(OB_HASH_NOT_EXIST == ret)) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to get refactored", K(ret));
      }
    } else if (OB_ISNULL(block_num_stat)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(ret), K(block_num_stat));
    } else {
      tab_stat->set_macro_block_num(block_num_stat->tab_macro_cnt_);
      tab_stat->set_micro_block_num(block_num_stat->tab_micro_cnt_);
      tab_stat->set_sstable_row_count(block_num_stat->sstable_row_cnt_);
      tab_stat->set_memtable_row_count(block_num_stat->memtable_row_cnt_);
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < param.column_params_.count(); ++i) {
    ObOptColumnStat *&col_stat = stat.column_stats_.at(i);
    if (OB_UNLIKELY(!param.column_params_.at(i).need_col_stat())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(ret), K(i), K(param));
    } else if (OB_ISNULL(col_stat = ObOptColumnStat::malloc_new_column_stat(allocator))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("memory is not enough", K(ret), K(col_stat));
    } else {
      col_stat->set_table_id(param.column_params_.at(i).need_basic_stat() ? param.table_id_: OB_INVALID_ID);
      col_stat->set_partition_id(part_id);
      col_stat->set_stat_level(param.stat_level_);
      col_stat->set_column_id(param.column_params_.at(i).column_id_);
      col_stat->set_collation_type(param.column_params_.at(i).cs_type_);
      if (block_num_stat != NULL) {
        if (OB_LIKELY(param.column_group_params_.count() == block_num_stat->cg_macro_cnt_arr_.count() &&
                      param.column_group_params_.count() == block_num_stat->cg_micro_cnt_arr_.count())) {
          bool found_it = false;
          for (int64_t j = 0; !found_it && j < param.column_group_params_.count(); ++j) {
            if (param.column_group_params_.at(j).column_id_arr_.count() == 1 &&
                param.column_group_params_.at(j).column_id_arr_.at(0) == param.column_params_.at(i).column_id_) {
              col_stat->set_cg_macro_blk_cnt(block_num_stat->cg_macro_cnt_arr_.at(j));
              col_stat->set_cg_micro_blk_cnt(block_num_stat->cg_micro_cnt_arr_.at(j));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObDbmsStatsGather::gather_index_stats(ObExecContext &ctx,
                                          const ObOptStatGatherParam &param,
                                          ObIArray<ObOptTableStat *> &all_index_stats)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObOptStat, 4> opt_stats;
  LOG_TRACE("begin to gather index stats", K(param));
  if (OB_ISNULL(param.allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(param.allocator_));
  } else if (OB_FAIL(init_opt_stats(*param.allocator_, param, opt_stats))) {
    LOG_WARN("failed to init opt stats", K(ret));
  } else if (opt_stats.empty()) {
    /*do nothing*/
  } else {
    ObIndexStatsEstimator index_est(ctx, *param.allocator_);
    if (OB_FAIL(index_est.estimate(param, opt_stats))) {
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

} // namespace common
} // namespace oceanbase
