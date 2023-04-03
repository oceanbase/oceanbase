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


namespace oceanbase
{
namespace sql
{

void ObAdaptiveByPassCtrl::gby_process_state(int64_t probe_cnt, int64_t row_cnt, int64_t mem_size)
{
  int64_t min_period_cnt = MIN_PERIOD_CNT;
  if (!by_pass_ctrl_enabled_) {
    // do nothing
  } else if (STATE_PROCESS_HT == state_) {
    // got this state from dump, hold this state
  } else if (0 == probe_cnt) {
  } else if (STATE_L2_INSERT == state_) {
    // insert until exceed l2 cache
    probe_cnt_ += probe_cnt;
    if (!in_l2_cache(row_cnt, mem_size)) {
      state_ = STATE_ANALYZE;
    }
  } else if (STATE_L3_INSERT == state_) {
    // insert until exceed l3 cache
    probe_cnt_ += probe_cnt;
    if (!in_l3_cache(row_cnt, mem_size)) {
      state_ = STATE_ANALYZE;
    }
  } else if (STATE_ANALYZE == state_) {
    probe_cnt_ += probe_cnt;
    double ratio = MIN_RATIO_FOR_L3;
    if (static_cast<double> (exists_cnt_) / probe_cnt_ >=
                      std::max(ratio, 1 - (1 / static_cast<double> (cut_ratio_)))) {
      // very good distinct rate, can expend hash map to l3 cache
      rebuild_times_ = 0;
      if (in_l2_cache(row_cnt, mem_size)) {
        state_ = STATE_L3_INSERT;
        need_resize_hash_table_ = true;
      } else {
        state_ = STATE_PROCESS_HT;
      }
    } else if (static_cast<double> (exists_cnt_) / probe_cnt_ >=
                                          1 - (1 / static_cast<double> (cut_ratio_))) {
      // good distinct rate, reset rebuild times
      state_ = STATE_PROCESS_HT;
      rebuild_times_ = 0;
    } else {
      // distinct rate is not good
      // prepare to release curr hash table
      state_ = STATE_PROCESS_HT;
    }
    LOG_TRACE("get new state", K(state_), K(processed_cnt_), K(exists_cnt_),
                              K(probe_cnt_), K(rebuild_times_), K(cut_ratio_), K(mem_size), K(op_id_), K(row_cnt));
    probe_cnt_ = 0;
    exists_cnt_ = 0;
  }
}


} // end namespace sql
} // end namespace oceanbase