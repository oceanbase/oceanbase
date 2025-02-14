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

#include "src/sql/engine/ob_by_pass_operator.h"
#include "src/sql/engine/ob_exec_context.h"

namespace oceanbase
{
using namespace common;
namespace sql
{
int ObByPassOperator::get_next_row()
{
  int ret = common::OB_SUCCESS;
  if (by_pass_) {
    begin_cpu_time_counting();
    ASH_ITEM_ATTACH_GUARD(plan_line_id, spec_.id_);
    if (ctx_.get_my_session()->is_user_session() || spec_.plan_->get_phy_plan_hint().monitor_) {
      IGNORE_RETURN try_register_rt_monitor_node(1);
    }
    if (OB_FAIL(check_stack_once())) {
      LOG_WARN("too deep recursive", K(ret));
    } else if (OB_UNLIKELY(get_spec().is_vectorized())) {
      // Operator itself supports vectorization, while parent operator does NOT.
      // Use vectorize method to get next row.
      if (OB_FAIL(get_next_row_vectorizely())) {
        // do nothing
      }
    } else {
      ret = left_->get_next_row();
      if (OB_SUCC(ret) && OB_FAIL(after_by_pass_next_row())) {
        LOG_WARN("by pass next row failed", K(ret));
      }
      if (OB_SUCCESS == ret) {
        op_monitor_info_.output_row_count_++;
        if (!got_first_row_) {
          op_monitor_info_.first_row_time_ = oceanbase::common::ObClockGenerator::getClock();
          got_first_row_ = true;
        }
      } else if (OB_ITER_END == ret) {
        if (got_first_row_) {
          op_monitor_info_.last_row_time_ = oceanbase::common::ObClockGenerator::getClock();
        }
      }
    }
    end_ash_line_id_reg(ret);
    end_cpu_time_counting();
  } else {
    ret = ObOperator::get_next_row();
  }
  return ret;
}

int ObByPassOperator::get_next_batch(const int64_t max_row_cnt, const ObBatchRows *&batch_rows)
{
  int ret = common::OB_SUCCESS;
  if (by_pass_) {
    begin_cpu_time_counting();
    ASH_ITEM_ATTACH_GUARD(plan_line_id, spec_.id_);
    if (ctx_.get_my_session()->is_user_session() || spec_.plan_->get_phy_plan_hint().monitor_) {
      IGNORE_RETURN try_register_rt_monitor_node(brs_.size_);
    }
    if (OB_FAIL(check_stack_once())) {
      LOG_WARN("too deep recursive", K(ret));
    } else if (OB_UNLIKELY(spec_.need_check_output_datum_ && brs_checker_)) {
      if (OB_FAIL(brs_checker_->check_datum_modified())) {
        LOG_WARN("check output datum failed", K(ret), "id", spec_.get_id(), "op_name", op_name());
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_LIKELY(get_spec().is_vectorized())) {
      if (OB_FAIL(left_->get_next_batch(max_row_cnt, batch_rows))) {
        LOG_WARN("left_ fail to get_next_batch", K(ret));
      } else if (OB_FAIL(brs_.copy(batch_rows))) {
        LOG_WARN("copy batch_rows to brs_ failed", K(ret));
      } else if (OB_FAIL(after_by_pass_next_batch(batch_rows))) {
        LOG_WARN("by pass next row failed", K(ret));
      }
      if (OB_SUCC(ret)) {
        int64_t skipped_rows_count = brs_.skip_->accumulate_bit_cnt(brs_.size_);
        op_monitor_info_.output_row_count_ += brs_.size_ - skipped_rows_count;
        op_monitor_info_.skipped_rows_count_ += skipped_rows_count;
        ++op_monitor_info_.output_batches_;
        if (!got_first_row_ && !brs_.end_) {
          op_monitor_info_.first_row_time_ = ObClockGenerator::getClock();;
          got_first_row_ = true;
        }
        if (brs_.end_) {
          op_monitor_info_.last_row_time_ = ObClockGenerator::getClock();
        }
      }
    } else {
      // Operator does NOT support vectorization, while its parent does. Return
      // the batch with only 1 row
      if (OB_FAIL(get_next_batch_with_onlyone_row())) {
        // do nothing
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(spec_.need_check_output_datum_) && brs_checker_ && !brs_.end_ && brs_.size_ > 0) {
        OZ(brs_checker_->save(brs_.size_));
      }
      LOG_DEBUG("get next batch", "id", spec_.get_id(), "op_name", op_name(), K(brs_));
    }
    end_ash_line_id_reg(ret);
    end_cpu_time_counting();
  } else {
    ret = ObOperator::get_next_batch(max_row_cnt, batch_rows);
  }
  return ret;
}

int ObByPassOperator::inner_rescan()
{
  by_pass_ = false;
  return ObOperator::inner_rescan();
}

int ObByPassOperator::after_by_pass_next_row()
{
  int ret = common::OB_SUCCESS;
  clear_evaluated_flag();
  return ret;
}

int ObByPassOperator::after_by_pass_next_batch(const ObBatchRows *&batch_rows)
{
  int ret = common::OB_SUCCESS;
  clear_evaluated_flag();
  for (int i = 0; OB_SUCC(ret) && i < get_spec().output_.count(); ++i) {
    if (OB_FAIL(get_spec().output_.at(i)->eval_batch(eval_ctx_,
        *(batch_rows->skip_), batch_rows->size_))) {
      LOG_WARN("Fail to eval batch", K(ret), K(i));
    }
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase
