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
 *
 * statics participants br output and support suggest for redo_dispatcher
 * TODO: statics feature is in Sorter module, dispatcher invoke function to get a struct which
 * record trans_id/skew log_stream/redo count suggested to dispatch. Sorter will own a seperate
 * thread to handle the statics work.
 */

#ifndef OCEANBASE_LIBOBCDC_TRANS_DISPATCH_CTX_
#define OCEANBASE_LIBOBCDC_TRANS_DISPATCH_CTX_

#include "ob_log_trans_ctx.h"
#include "lib/container/ob_array.h"           // ObArray

namespace oceanbase
{
namespace libobcdc
{
class ObLogTransRedoDispatcher;

struct PartTransDispatchBudget
{
  PartTransDispatchBudget() { reset(); }

  PartTransTask   *part_trans_task_;
  int64_t         dispatch_budget_ CACHE_ALIGNED;
  int64_t         dispatched_size_ CACHE_ALIGNED;
  int64_t         skew_weight_;

  void reset()
  {
    part_trans_task_ = NULL;
    dispatch_budget_ = 0;
    dispatched_size_ = 0;
    skew_weight_ = 0;
  }

  void reset(PartTransTask *part_trans_task)
  {
    reset();
    part_trans_task_ = part_trans_task;
    skew_weight_ = 1;
  }

  void reset_budget(const int64_t dispatch_budget)
  {
    ATOMIC_SET(&dispatch_budget_, dispatch_budget);
    ATOMIC_SET(&dispatched_size_, 0);
  }

  void set_weight(const int64_t weight)
  {
    skew_weight_ = weight;
  }

  bool is_valid() const
  {
    return OB_NOT_NULL(part_trans_task_)
        && ATOMIC_LOAD(&dispatch_budget_) >= 0
        && ATOMIC_LOAD(&dispatched_size_) >= 0;
  }

  bool can_dispatch() const
  {
    return is_valid() && !is_budget_used_up();
  }

  void inc_dispatched_size(const int64_t redo_size)
  {
    ATOMIC_AAF(&dispatched_size_, redo_size);
  }

  bool is_budget_used_up() const
  {
    return ATOMIC_LOAD(&dispatch_budget_) <= ATOMIC_LOAD(&dispatched_size_);
  }

  TO_STRING_KV(KP_(part_trans_task), KPC_(part_trans_task), K_(dispatch_budget), K_(dispatched_size), K_(skew_weight));

};

typedef common::ObArray<PartTransDispatchBudget> PartBudgetArray;
// monitor of redo dispatcher when output br by seq_no
class TransDispatchCtx
{
public:
  TransDispatchCtx();
  ~TransDispatchCtx() { reset(); }
public:
  int init(TransCtx &trans);
  void reset();
public:
  int reblance_budget(
      const int64_t redo_memory_limit,
      const int64_t current_used_memory,
      const TransCtx &trans);
  PartBudgetArray& get_normal_priority_part_budgets() { return normal_priority_part_budget_arr_; }
  PartBudgetArray& get_high_priority_part_budgets() { return high_priority_part_budget_arr_; }
  void inc_dispatched_part() { ATOMIC_INC(&dispatched_part_count_); }
  bool is_trans_dispatched() const { return ATOMIC_LOAD(&dispatched_part_count_) == total_part_count_; }
public:
  TO_STRING_KV(K_(trans_id), K_(total_part_count), K_(dispatched_part_count),
    K_(normal_priority_part_budget_arr), K_(high_priority_part_budget_arr));

private:
  int64_t get_total_need_reblance_part_cnt_() const;
  void set_normal_priority_budget_(const int64_t &average_budget);
  int collect_sorter_feedback_();

private:
  transaction::ObTransID  trans_id_;
  int64_t                 total_part_count_;
  int64_t                 dispatched_part_count_;
  // is_dispatching_ = false means not dispatch any redo,
  // will only dispatch one redo for the first round
  bool                    is_dispatching_;
  // assume PartTransTask that all redo dispatched will be removed from arr while
  // RedoDispatcher::dispatch_part_redo_with_budget
  PartBudgetArray         normal_priority_part_budget_arr_;
  PartBudgetArray         high_priority_part_budget_arr_;
private:
  DISALLOW_COPY_AND_ASSIGN(TransDispatchCtx);
};

} // namespace libobcdc
} // namespace oceanbase
#endif // end OCEANBASE_LIBOBCDC_TRANS_DISPATCH_CTX_
