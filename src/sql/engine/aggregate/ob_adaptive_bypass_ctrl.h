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

#ifndef OCEANBASE_SRC_SQL_ENGINE_ADAPTIVE_BYPASS_CTRL_H_
#define OCEANBASE_SRC_SQL_ENGINE_ADAPTIVE_BYPASS_CTRL_H_

#include "sql/engine/ob_operator.h"
#include "lib/utility/ob_tracepoint.h"

namespace oceanbase
{
namespace sql
{

const int64_t INIT_L2_CACHE_SIZE = get_level2_cache_size();
const int64_t INIT_L3_CACHE_SIZE = get_level3_cache_size();
const int64_t MAX_L3_CACHE_SIZE = 50 *1024 *1024; //50M
const uint64_t FORCE_GPD = 0x100;
const int64_t MAX_REBUILD_TIMES = 5;
constexpr const double MIN_RATIO_FOR_L3 = 0.80;
class ObAdaptiveByPassCtrl {
public:
  typedef enum {
    STATE_L2_INSERT = 0,
    STATE_L3_INSERT,
    STATE_MAX_MEM_INSERT, // Use the maximum available memory for data deduplication
    STATE_PROBE,
    STATE_ANALYZE,
    STATE_PROCESS_HT,
  } ByPassState;
  static const int64_t MIN_PERIOD_CNT = 1000;
  static const uint64_t INIT_CUT_RATIO = 3;
  ObAdaptiveByPassCtrl () : by_pass_(false), processed_cnt_(0), state_(STATE_L2_INSERT),
                         period_cnt_(MIN_PERIOD_CNT), probe_cnt_(0), exists_cnt_(0),
                         rebuild_times_(0), cut_ratio_(INIT_CUT_RATIO), by_pass_ctrl_enabled_(false),
                         small_row_cnt_(0), op_id_(-1), need_resize_hash_table_(false),
                         round_times_(0), scaled_llc_est_ndv_(0) {}
  inline void reset() {
    by_pass_ = false;
    processed_cnt_ = 0;
    scaled_llc_est_ndv_ = 0;  // reset scaled_llc_est_ndv_ before reset_state()
    reset_state();
    period_cnt_ = MIN_PERIOD_CNT;
    probe_cnt_ = 0;
    exists_cnt_ = 0;
    rebuild_times_ = 0;
    need_resize_hash_table_ = false;
  }
  inline void reset_state() { state_ = (scaled_llc_est_ndv_ ? STATE_MAX_MEM_INSERT : STATE_L2_INSERT); }
  inline void set_max_mem_insert_state() { state_ = STATE_MAX_MEM_INSERT; }
  inline bool is_max_mem_insert_state() { return  STATE_MAX_MEM_INSERT == state_; }
  inline void set_analyze_state() { state_ = STATE_ANALYZE; }
  inline bool is_analyze_state() { return  STATE_ANALYZE == state_; }
  inline void start_process_ht() { state_ = STATE_PROCESS_HT; }
  inline bool processing_ht() { return STATE_PROCESS_HT == state_; }
  inline bool in_cache_mem_bound(int64_t row_cnt, int64_t mem_size, int64_t mem_bound)
  {
    return 0 != small_row_cnt_ ? (row_cnt < small_row_cnt_) : (mem_size < mem_bound);
  }
  void gby_process_state(int64_t probe_cnt, int64_t row_cnt, int64_t mem_size);
  inline void inc_processed_cnt(int64_t new_processed_cnt) { processed_cnt_ += new_processed_cnt; }
  inline void inc_probe_cnt_() { ++probe_cnt_; }
  inline void inc_rebuild_times() { ++rebuild_times_; }
  inline void inc_exists_cnt() { ++exists_cnt_; }
  inline void set_cut_ratio(uint64_t cut_ratio) { cut_ratio_ = cut_ratio; }
  inline bool by_passing() { return by_pass_; }
  inline void start_by_pass() { by_pass_ = true; }
  inline void stop_by_pass() { by_pass_ = false; }
  inline void reset_rebuild_times() { rebuild_times_ = 0; }
  inline void bypass_rebackto_insert(uint64_t llc_est_ndv) { scaled_llc_est_ndv_ = llc_est_ndv / 2 * 3; stop_by_pass(); start_process_ht(); reset_rebuild_times(); round_times_ = 0; need_resize_hash_table_ = false; }
  inline bool rebuild_times_exceeded() { return rebuild_times_ >= MAX_REBUILD_TIMES; }
  inline void set_max_rebuild_times() { rebuild_times_ = MAX_REBUILD_TIMES + 1; }
  inline void open_by_pass_ctrl() { by_pass_ctrl_enabled_ = true; }
  inline void set_op_id(int64_t op_id) { op_id_ = op_id; }
  inline void set_small_row_cnt(int64_t row_cnt) { small_row_cnt_ = row_cnt; }
  inline int64_t get_small_row_cnt() const { return small_row_cnt_; }
  bool by_pass_;
  int64_t processed_cnt_;
  ByPassState state_;
  int64_t period_cnt_;
  int64_t probe_cnt_;
  int64_t exists_cnt_;
  int64_t rebuild_times_;
  uint64_t cut_ratio_;
  bool by_pass_ctrl_enabled_;
  int64_t small_row_cnt_; // 0 will be omit
  int64_t op_id_;
  bool need_resize_hash_table_;
  int64_t probe_cnt_for_period_[MAX_REBUILD_TIMES];
  int64_t ndv_cnt_for_period_[MAX_REBUILD_TIMES];
  int64_t round_times_;
  uint64_t scaled_llc_est_ndv_;
};

} // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_SRC_SQL_ENGINE_ADAPTIVE_BYPASS_CTRL_H_ */