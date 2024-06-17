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
#include "sql/engine/aggregate/ob_adaptive_bypass_ctrl.h"
#include "sql/optimizer/ob_opt_selectivity.h"


namespace oceanbase
{
namespace sql
{

void ObAdaptiveByPassCtrl::gby_process_state(int64_t probe_cnt,
                                             int64_t row_cnt,
                                             int64_t mem_size)
{
  int64_t min_period_cnt = MIN_PERIOD_CNT;
  processed_cnt_ += probe_cnt;
  if (!by_pass_ctrl_enabled_) {
    // do nothing
  } else if (STATE_PROCESS_HT == state_) {
    // got this state from dump, hold this state
  } else if (0 == probe_cnt) {
  } else if (STATE_L2_INSERT == state_) {
    // insert until exceed l2 cache
    if (!in_cache_mem_bound(row_cnt, mem_size, INIT_L2_CACHE_SIZE)) {
      state_ = STATE_ANALYZE;
    }
  } else if (STATE_L3_INSERT == state_) {
    // insert until exceed l3 cache
    if (!in_cache_mem_bound(row_cnt, mem_size, INIT_L3_CACHE_SIZE)) {
      state_ = STATE_ANALYZE;
    }
  } else if (STATE_MAX_MEM_INSERT == state_) {
    if (row_cnt > scaled_llc_est_ndv_ &&
        (static_cast<double>(row_cnt) / probe_cnt) > (1 / static_cast<double>(cut_ratio_))) {
      state_ = STATE_PROCESS_HT;
      set_max_rebuild_times();
      int ret = OB_SUCCESS;//no use, just for LOG_TRACE.
      LOG_TRACE("STATE_MAX_MEM_INSERT goto STATE_PROCESS_HT", K(ret), K(probe_cnt), K(row_cnt));
    }
  } else if (STATE_ANALYZE == state_) {
    double ratio = MIN_RATIO_FOR_L3;
    probe_cnt_for_period_[round_times_ % MAX_REBUILD_TIMES] = probe_cnt;
    ndv_cnt_for_period_[round_times_ % MAX_REBUILD_TIMES] = row_cnt;
    ++round_times_;
    int64_t exists_cnt = probe_cnt - row_cnt;
    if (static_cast<double> (exists_cnt) / probe_cnt >=
                      std::max(ratio, 1 - (1 / static_cast<double> (cut_ratio_)))) {
      // very good distinct rate, can expend hash map to l3 cache
      rebuild_times_ = 0;
      if (scaled_llc_est_ndv_) {
        state_ = STATE_PROCESS_HT;
      } else if (in_cache_mem_bound(row_cnt, mem_size, INIT_L3_CACHE_SIZE)) {
        state_ = STATE_L3_INSERT;
        need_resize_hash_table_ = true;
      } else {
        state_ = STATE_PROCESS_HT;
      }
    } else if (round_times_ >= MAX_REBUILD_TIMES) {
      double select_rows = 0.0;
      double ndv = 0.0;
      for (int64_t i = 0; i < MAX_REBUILD_TIMES; ++i) {
        select_rows += probe_cnt_for_period_[i];
        ndv += ndv_cnt_for_period_[i];
      }
      ndv /= MAX_REBUILD_TIMES;
      double rows = select_rows / MAX_REBUILD_TIMES;
      double new_ndv = ObOptSelectivity::scale_distinct(select_rows, rows, ndv);
      double new_ratio = 1 - new_ndv / select_rows;
      if (new_ratio >= std::max(ratio, 1 - (1 / static_cast<double> (cut_ratio_)))) {
        // very good distinct rate, can expend hash map to l3 cache
        rebuild_times_ = 0;
        if (scaled_llc_est_ndv_) {
          state_ = STATE_PROCESS_HT;
        } else if (in_cache_mem_bound(row_cnt, mem_size, INIT_L3_CACHE_SIZE)) {
          state_ = STATE_L3_INSERT;
          need_resize_hash_table_ = true;
        } else {
          state_ = STATE_PROCESS_HT;
        }
      } else if (new_ratio >= 1 - (1 / static_cast<double> (cut_ratio_))) {
        // good distinct rate, reset rebuild times
        state_ = STATE_PROCESS_HT;
        rebuild_times_ = 0;
      } else {
        // distinct rate is not good
        // prepare to release curr hash table
        state_ = STATE_PROCESS_HT;
        if (scaled_llc_est_ndv_) {
          set_max_rebuild_times();
        }
      }
      LOG_TRACE("adaptive groupby try redefine ratio", K(select_rows), K(rows), K(ndv),
                                                       K(new_ndv), K(new_ratio), K(state_), K(processed_cnt_));
    } else if (static_cast<double> (exists_cnt) / probe_cnt >=
                                          1 - (1 / static_cast<double> (cut_ratio_))) {
      // good distinct rate, reset rebuild times
      state_ = STATE_PROCESS_HT;
      rebuild_times_ = 0;
    } else {
      // distinct rate is not good
      // prepare to release curr hash table
      state_ = STATE_PROCESS_HT;
      if (scaled_llc_est_ndv_) {
        set_max_rebuild_times();
      }
    }
    LOG_TRACE("adaptive groupby generate new state", K(state_), K(rebuild_times_), K(cut_ratio_),
                                                     K(mem_size), K(op_id_), K(row_cnt),
                                                     K(probe_cnt), K(exists_cnt), K(processed_cnt_));
  }
}


} // end namespace sql
} // end namespace oceanbase