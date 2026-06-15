/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include "ob_adaptive_bypass_ctrl.h"
#include "sql/optimizer/ob_opt_selectivity.h"
#include "sql/engine/aggregate/ob_aggregate_processor.h"
#include "sql/engine/expr/ob_expr_estimate_ndv.h"
#include "sql/engine/ob_tenant_sql_memory_manager.h"


namespace oceanbase
{
namespace sql
{

void ObAdaptiveByPassCtrl::process_state(int64_t probe_cnt,
                                             int64_t row_cnt,
                                             int64_t mem_size)
{
  int64_t min_period_cnt = MIN_PERIOD_CNT;
  processed_cnt_ = probe_cnt + last_round_processed_cnt_;
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
  } else if (STATE_L2_INSERT_5X == state_) {
    if (!in_cache_mem_bound(row_cnt, mem_size, INIT_L2_CACHE_SIZE_5X)) {
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
      // 命中率>95%,升级到L3cache
      rebuild_times_ = 0;
      if (scaled_llc_est_ndv_) {
        state_ = STATE_PROCESS_HT;
      } else if (in_cache_mem_bound(row_cnt, mem_size, INIT_L3_CACHE_SIZE)) {
        // now is L2_INSERT or L2_INSERT_5, can update to L3
        state_ = STATE_L3_INSERT;
        need_resize_hash_table_ = true;
      } else {
        // now is L3_INSERT, restart round
        state_ = STATE_PROCESS_HT;
      }
    } else if (round_times_ >= MAX_REBUILD_TIMES && in_cache_mem_bound(row_cnt,
                mem_size, min(INIT_L2_CACHE_SIZE_5X, INIT_L3_CACHE_SIZE))) {
      // if in L3insert or L2insert_5, no need calc new_ratio
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
        need_become_l2_insert_5x_ = true;
        rebuild_times_ = 0;
      } else {
        // distinct rate is not good
        // prepare to release curr hash table
        state_ = STATE_PROCESS_HT;
        if (scaled_llc_est_ndv_) {
          set_max_rebuild_times();
        }
      }
      LOG_TRACE("adaptive groupby try redefine ratio", K(select_rows), K(rows), K(ndv), K(round_times_),
                                                       K(new_ndv), K(new_ratio), K(state_), K(processed_cnt_));
    } else if (static_cast<double> (exists_cnt) / probe_cnt >=
                                          1 - (1 / static_cast<double> (cut_ratio_))) {
      // good distinct rate, reset rebuild times
      state_ = STATE_PROCESS_HT;
      if (!in_cache_mem_bound(row_cnt, mem_size, INIT_L2_CACHE_SIZE_5X)) {
        // now is L2_INSERT_5 or L3_INSERT , resize ht is ok
        need_resize_hash_table_ = true;
      }
      rebuild_times_ = 0;
    } else {
      // distinct rate is not good
      // prepare to release curr hash table
      state_ = STATE_PROCESS_HT;
      if (scaled_llc_est_ndv_) {
        set_max_rebuild_times();
      }
      if (!in_cache_mem_bound(row_cnt, mem_size, INIT_L2_CACHE_SIZE_5X)) {
        // now is L2_INSERT_5 or L3_INSERT , resize ht is ok
        need_resize_hash_table_ = true;
        set_max_rebuild_times();
      }
    }
    LOG_TRACE("adaptive groupby generate new state", K(state_), K(rebuild_times_), K(cut_ratio_),
                                                     K(mem_size), K(op_id_), K(row_cnt),
                                                     K(probe_cnt), K(exists_cnt), K(processed_cnt_));
  }
}

int LlcEstimate::init_llc_map(common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  char *llc_map = nullptr;
  int64_t llc_map_size = 0;
  if (OB_FAIL(ObAggregateProcessor::llc_init_empty(llc_map, llc_map_size, allocator))) {
    LOG_WARN("failed to init llc map", K(ret));
  } else {
    llc_map_.assign_ptr(llc_map, llc_map_size);
  }
  return ret;
}

int LlcEstimate::reset()
{
  int ret = OB_SUCCESS;
  if (!llc_map_.empty()) {
    MEMSET(llc_map_.ptr(), 0, llc_map_.length());
  }
  avg_group_mem_ = 0;
  est_cnt_ = 0;
  last_est_cnt_ = 0;
  sample_batch_cnt_ = 0;
  sample_interval_ = 1;
  checks_since_last_reduce_ = 0;
  return ret;
}

int LlcEstimate::init(common::ObIAllocator &allocator, bool enabled)
{
  int ret = OB_SUCCESS;
  enabled_ = enabled;
  if (enabled) {
    if (OB_FAIL(init_llc_map(allocator))) {
      LOG_WARN("failed to init llc map", K(ret));
    }
  }
  return ret;
}

void LlcEstimate::add_value(uint64_t hash_value)
{
  ObAggregateProcessor::llc_add_value(hash_value, llc_map_);
  ++est_cnt_;
}

void LlcEstimate::set_avg_group_mem(int64_t row_cnt, int64_t mem_used)
{
  if (0 == avg_group_mem_ && enabled_) {
    avg_group_mem_ = (0 == row_cnt) ? 128
                              : (static_cast<double>(mem_used) / row_cnt);
  }
}

int ObAdaptiveByPassCtrl::check_llc_ndv()
{
  int ret = OB_SUCCESS;
  int64_t ndv = -1;
  int64_t global_bound_size = 0;
  bool ndv_ratio_is_small_enough = false;
  bool has_enough_mem_for_deduplication = false;
  ObTenantSqlMemoryManager *tenant_sql_mem_manager = NULL;
  tenant_sql_mem_manager = MTL(ObTenantSqlMemoryManager*);
  ObExprEstimateNdv::llc_estimate_ndv(ndv, llc_est_.llc_map_);
  if (0 == llc_est_.est_cnt_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect zero cnt", K(llc_est_.est_cnt_), K(ret));
  } else if (FALSE_IT(ndv_ratio_is_small_enough =
                 (ndv * 1.0 / llc_est_.est_cnt_) < LlcEstimate::LLC_NDV_RATIO)) {
  } else if (FALSE_IT(llc_est_.last_est_cnt_ = llc_est_.est_cnt_)) {
  } else if (OB_ISNULL(tenant_sql_mem_manager)) {
    uint64_t tenant_id = MTL_ID();
    if (OB_MAX_RESERVED_TENANT_ID < tenant_id) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null ptr", K(ret));
    } else if (ndv_ratio_is_small_enough) {
      bypass_rebackto_insert(ndv);
      llc_est_.enabled_ = false;
    } else {
      llc_try_increase_sample_interval();
    }
  } else {
    global_bound_size = tenant_sql_mem_manager->get_global_bound_size();
    has_enough_mem_for_deduplication =
        (global_bound_size * LlcEstimate::GLOBAL_BOUND_RATIO) >
        (llc_est_.avg_group_mem_ * ndv);
    LOG_TRACE("check llc ndv", K(ndv_ratio_is_small_enough), K(ndv),
              K(llc_est_.est_cnt_),
              K(has_enough_mem_for_deduplication),
              K(llc_est_.avg_group_mem_), K(global_bound_size));
    if (!has_enough_mem_for_deduplication) {
      llc_est_.enabled_ = false;
      LOG_TRACE("stop check llc ndv and continue bypass",
                K(ndv_ratio_is_small_enough), K(ndv), K(llc_est_.est_cnt_),
                K(has_enough_mem_for_deduplication), K(llc_est_.avg_group_mem_),
                K(global_bound_size));
    } else if (ndv_ratio_is_small_enough) {
      bypass_rebackto_insert(ndv);
      llc_est_.enabled_ = false;
      LOG_TRACE("reback into deduplication state",
                K(ndv_ratio_is_small_enough), K(ndv), K(llc_est_.est_cnt_),
                K(has_enough_mem_for_deduplication), K(llc_est_.avg_group_mem_),
                K(global_bound_size));
    } else {
      llc_try_increase_sample_interval();
    }
  }
  return ret;
}

void ObAdaptiveByPassCtrl::llc_try_increase_sample_interval()
{
  ++llc_est_.checks_since_last_reduce_;
  if (llc_est_.checks_since_last_reduce_ >= LlcEstimate::LLC_NUM_CHECKS_TO_REDUCE) {
    llc_est_.sample_interval_ = std::min(llc_est_.sample_interval_ * 2,
                                        LlcEstimate::LLC_SAMPLE_INTERVAL_CAP);
    llc_est_.checks_since_last_reduce_ = 0;
    LOG_TRACE("llc sample divisor increased",
              K(llc_est_.sample_interval_), K(llc_est_.est_cnt_));
  }
}

int ObAdaptiveByPassCtrl::llc_add_batch_and_check(const uint64_t *hash_vals,
                                                  const ObBitVector *skip,
                                                  int64_t size,
                                                  bool ready_to_check)
{
  int ret = OB_SUCCESS;
  ++llc_est_.sample_batch_cnt_;
  for (int64_t i = 0; OB_SUCC(ret) && llc_est_.enabled_ && i < size; ++i) {
    if (skip != nullptr && skip->exist(i)) { continue; }
    llc_est_.add_value(hash_vals[i]);
  }
  if (llc_est_.enabled_ &&
      (llc_est_.est_cnt_ - llc_est_.last_est_cnt_) > LlcEstimate::ESTIMATE_MOD_NUM &&
      ready_to_check && OB_FAIL(check_llc_ndv())) {
    LOG_WARN("failed to check llc ndv", K(ret));
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase