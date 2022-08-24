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

#include "sql/engine/join/ob_nested_loop_join_op.h"
#include "sql/engine/table/ob_table_scan_op.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase {
using namespace common;
namespace sql {

OB_SERIALIZE_MEMBER((ObNestedLoopJoinSpec, ObBasicNestedLoopJoinSpec), use_group_, batch_size_);

ObNestedLoopJoinOp::ObNestedLoopJoinOp(ObExecContext& exec_ctx, const ObOpSpec& spec, ObOpInput* input)
    : ObBasicNestedLoopJoinOp(exec_ctx, spec, input),
      state_(JS_READ_LEFT),
      mem_context_(nullptr),
      is_left_end_(false),
      last_store_row_(),
      save_last_row_(false)
{
  state_operation_func_[JS_JOIN_END] = &ObNestedLoopJoinOp::join_end_operate;
  state_function_func_[JS_JOIN_END][FT_ITER_GOING] = NULL;
  state_function_func_[JS_JOIN_END][FT_ITER_END] = &ObNestedLoopJoinOp::join_end_func_end;

  state_operation_func_[JS_READ_LEFT] = &ObNestedLoopJoinOp::read_left_operate;
  state_function_func_[JS_READ_LEFT][FT_ITER_GOING] = &ObNestedLoopJoinOp::read_left_func_going;
  state_function_func_[JS_READ_LEFT][FT_ITER_END] = &ObNestedLoopJoinOp::read_left_func_end;

  state_operation_func_[JS_READ_RIGHT] = &ObNestedLoopJoinOp::read_right_operate;
  state_function_func_[JS_READ_RIGHT][FT_ITER_GOING] = &ObNestedLoopJoinOp::read_right_func_going;
  state_function_func_[JS_READ_RIGHT][FT_ITER_END] = &ObNestedLoopJoinOp::read_right_func_end;
}

int ObNestedLoopJoinOp::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(left_) || OB_ISNULL(right_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("nlp_op child is null", KP(left_), KP(right_), K(ret));
  } else if (OB_FAIL(ObBasicNestedLoopJoinOp::inner_open())) {
    LOG_WARN("failed to open in base class", K(ret));
  }
  return ret;
}

int ObNestedLoopJoinOp::switch_iterator()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(left_->switch_iterator())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("switch left child iterator failed", K(ret));
    }
  } else {
    reset_buf_state();
  }

  return ret;
}

int ObNestedLoopJoinOp::set_param_null() {
  int ret = OB_SUCCESS;
  ObDatum null_datum;
  null_datum.set_null();
  for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.rescan_params_.count(); ++i) {
    OZ(MY_SPEC.rescan_params_.at(i).update_dynamic_param(eval_ctx_,
                                                         null_datum));
    LOG_DEBUG("prepare_rescan_params", K(ret), K(i));
  }
  return ret;
}

int ObNestedLoopJoinOp::rescan() {
  int ret = OB_SUCCESS;
  reset_buf_state();
  ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  if (OB_FAIL(set_param_null())) {
    LOG_WARN("failed to set param null", K(ret));
  } else if (OB_FAIL(ObBasicNestedLoopJoinOp::rescan())) {
    LOG_WARN("failed to rescan", K(ret));
  }

  return ret;
}

int ObNestedLoopJoinOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(LEFT_SEMI_JOIN == MY_SPEC.join_type_ || LEFT_ANTI_JOIN == MY_SPEC.join_type_)) {
    if (OB_FAIL(join_row_with_semi_join())) {
      if (ret != OB_ITER_END) {
        LOG_WARN("failed to join row with semi join", K(ret));
      }
    }
  } else {
    state_operation_func_type state_operation = NULL;
    state_function_func_type state_function = NULL;
    int func = -1;
    output_row_produced_ = false;
    while (OB_SUCC(ret) && !output_row_produced_) {
      state_operation = this->ObNestedLoopJoinOp::state_operation_func_[state_];
      if (OB_ITER_END == (ret = (this->*state_operation)())) {
        func = FT_ITER_END;
        ret = OB_SUCCESS;
      } else if (OB_FAIL(ret)) {
        LOG_WARN("failed state operation", K(ret), K(state_));
      } else {
        func = FT_ITER_GOING;
      }
      if (OB_SUCC(ret)) {
        state_function = this->ObNestedLoopJoinOp::state_function_func_[state_][func];
        if (OB_FAIL((this->*state_function)()) && OB_ITER_END != ret) {
          LOG_WARN("failed state function", K(ret), K(state_), K(func));
        }
      }
    }  // while end
  }
  if (OB_ITER_END == ret) {
    if (OB_FAIL(set_param_null())) {
      LOG_WARN("failed to set param null", K(ret));
    } else {
      ret = OB_ITER_END;
    }
  }

  return ret;
}

void ObNestedLoopJoinOp::reset_buf_state()
{
  state_ = JS_READ_LEFT;
  left_store_.reset();
  left_store_iter_.reset();
  is_left_end_ = false;
  last_store_row_.reset();
  save_last_row_ = false;
}

int ObNestedLoopJoinOp::join_row_with_semi_join()
{
  int ret = OB_SUCCESS;
  const bool is_anti = (LEFT_ANTI_JOIN == MY_SPEC.join_type_);
  while (OB_SUCC(ret) && OB_SUCC(left_->get_next_row())) {
    clear_evaluated_flag();
    if (OB_FAIL(try_check_status())) {
      LOG_WARN("check status failed", K(ret));
    } else if (!MY_SPEC.rescan_params_.empty()) {
      ObExecContext::ObPlanRestartGuard restart_plan(ctx_);
      if (OB_FAIL(prepare_rescan_params())) {
        LOG_WARN("prepare right child rescan param failed", K(ret));
      } else if (OB_FAIL(right_->rescan())) {
        LOG_WARN("rescan right child failed", K(ret));
      }
    } else {
      if (OB_FAIL(prepare_rescan_params())) {
        LOG_WARN("prepare right child rescan param failed", K(ret));
      } else if (OB_FAIL(right_->rescan())) {
        LOG_WARN("rescan right child failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      bool is_matched = false;
      while (OB_SUCC(ret) && !is_matched && OB_SUCC(right_->get_next_row())) {
        clear_evaluated_flag();
        if (OB_FAIL(try_check_status())) {
          LOG_WARN("check status failed", K(ret));
        } else if (OB_FAIL(calc_other_conds(is_matched))) {
          LOG_WARN("calc other conditions failed", K(ret));
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
      if (OB_SUCC(ret) && is_anti != is_matched) {
        // 1. the exit conditions of semi and anti semi are different,
        // 2. they share the same outer while loop,
        // 3. is_matched must init to false for inner while loop,
        // so we need explicitly break.
        break;
      }
    }
  }

  if (OB_FAIL(ret)) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get next row failed", K(ret));
    }
  }

  return ret;
}

int ObNestedLoopJoinOp::join_end_operate()
{
  return OB_ITER_END;
}

int ObNestedLoopJoinOp::join_end_func_end()
{
  return OB_ITER_END;
}

int ObNestedLoopJoinOp::read_left_operate()
{
  int ret = OB_SUCCESS;
  if (MY_SPEC.use_group_) {
    if (OB_FAIL(group_read_left_operate()) && OB_ITER_END != ret) {
      LOG_WARN("failed to read left group", K(ret));
    }
  } else if (OB_FAIL(get_next_left_row()) && OB_ITER_END != ret) {
    LOG_WARN("failed to get next left row", K(ret));
  }

  return ret;
}

int ObNestedLoopJoinOp::group_read_left_operate()
{
  int ret = OB_SUCCESS;
  ObTableScanOp* right_tsc = reinterpret_cast<ObTableScanOp*>(right_);
  if (left_store_iter_.is_valid() && left_store_iter_.has_next()) {
    // set right table scan result again, result is next cache.
    if (OB_FAIL(right_tsc->bnl_switch_iterator())) {
      if (OB_ITER_END == ret) {
        ret = OB_ERR_UNEXPECTED;
      }
      LOG_WARN("failed to get next right row from group", K(ret));
    }
  } else {
    if (OB_FAIL(right_tsc->group_rescan_init(MY_SPEC.batch_size_))) {
      LOG_WARN("Failed to init group rescan", K(ret));
    } else if (is_left_end_) {
      ret = OB_ITER_END;
    } else {
      if (OB_ISNULL(mem_context_)) {
        ObSQLSessionInfo* session = ctx_.get_my_session();
        uint64_t tenant_id = session->get_effective_tenant_id();
        lib::ContextParam param;
        param.set_mem_attr(tenant_id, ObModIds::OB_SQL_NLJ_CACHE, ObCtxIds::WORK_AREA)
            .set_properties(lib::USE_TL_PAGE_OPTIONAL);
        if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(mem_context_, param))) {
          LOG_WARN("create entity failed", K(ret));
        } else if (OB_ISNULL(mem_context_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("null memory entity returned", K(ret));
        } else if (OB_FAIL(left_store_.init(UINT64_MAX, tenant_id, ObCtxIds::WORK_AREA))) {
          LOG_WARN("init row store failed", K(ret));
        } else {
          left_store_.set_allocator(mem_context_->get_malloc_allocator());
        }
      }

      bool ignore_end = false;
      if (OB_SUCC(ret)) {
        left_store_.reset();
        left_store_iter_.reset();
        mem_context_->get_arena_allocator().reset();
        if (OB_ISNULL(last_store_row_.get_store_row())) {
          if (save_last_row_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected status: store row is null", K(ret));
          } else if (OB_FAIL(last_store_row_.init(
                         mem_context_->get_malloc_allocator(), left_->get_spec().output_.count()))) {
            LOG_WARN("failed to init right last row", K(ret));
          }
        } else if (save_last_row_) {
          if (OB_FAIL(last_store_row_.restore(left_->get_spec().output_, eval_ctx_))) {
            LOG_WARN("failed to restore left row", K(ret));
          }
        }
        save_last_row_ = false;
        while (OB_SUCC(ret) && !is_full()) {
          // need clear evaluated flag, since prepare_rescan_params() will evaluate expression.
          clear_evaluated_flag();
          if (OB_FAIL(get_next_left_row())) {
            if (OB_ITER_END != ret) {
              LOG_WARN("failed to get next left row", K(ret));
            } else {
              is_left_end_ = true;
            }
          } else if (OB_FAIL(left_store_.add_row(left_->get_spec().output_, &eval_ctx_))) {
            LOG_WARN("failed to store left row", K(ret));
            // do nothing
          } else if (OB_FAIL(prepare_rescan_params(true /*is_group*/))) {
            LOG_WARN("failed to prepare rescan params", K(ret));
          } else if (OB_FAIL(deep_copy_dynamic_obj())) {
            LOG_WARN("fail to deep copy dynamic obj", K(ret));
          } else if (OB_FAIL(right_tsc->group_add_query_range())) {
            LOG_WARN("failed to rescan right op", K(ret));
          } else {
            ignore_end = true;
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(last_store_row_.shadow_copy(left_->get_spec().output_, eval_ctx_))) {
            LOG_WARN("failed to shadow copy last left row", K(ret));
          } else {
            save_last_row_ = true;
          }
        }
      }

      if (OB_SUCC(ret) || (ignore_end && OB_ITER_END == ret)) {
        ret = OB_SUCCESS;
        if (OB_FAIL(left_store_.finish_add_row(false))) {
          LOG_WARN("failed to finish add row to row store", K(ret));
        } else if (OB_FAIL(left_store_.begin(left_store_iter_))) {
          LOG_WARN("failed to begin iterator for chunk row store", K(ret));
        } else if (OB_FAIL(right_tsc->group_rescan())) {
          LOG_WARN("failed to rescan right op", K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(left_store_iter_.get_next_row(left_->get_spec().output_, eval_ctx_))) {
      LOG_WARN("Failed to get next row", K(ret));
    } else {
      left_row_joined_ = false;
    }
  }
  return ret;
}

int ObNestedLoopJoinOp::deep_copy_dynamic_obj()
{
  int ret = OB_SUCCESS;
  int64_t param_cnt = get_spec().rescan_params_.count();
  ObPhysicalPlanCtx* plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  ParamStore& param_store = plan_ctx->get_param_store_for_update();
  if (OB_ISNULL(mem_context_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("mem entity not init", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < param_cnt; ++i) {
    const ObDynamicParamSetter& rescan_param = get_spec().rescan_params_.at(i);
    int64_t param_idx = rescan_param.param_idx_;
    if (OB_FAIL(
            ob_write_obj(mem_context_->get_arena_allocator(), param_store.at(param_idx), param_store.at(param_idx)))) {
      LOG_WARN("deep copy dynamic param", K(ret));
    }
  }

  return ret;
}

int ObNestedLoopJoinOp::read_left_func_going()
{
  int ret = OB_SUCCESS;
  if (MY_SPEC.use_group_) {
    // do nothing
  } else if (!MY_SPEC.rescan_params_.empty()) {
    ObExecContext::ObPlanRestartGuard restart_plan(ctx_);
    if (OB_FAIL(prepare_rescan_params())) {
      LOG_WARN("failed to prepare rescan params", K(ret));
    } else if (OB_FAIL(right_->rescan())) {
      LOG_WARN("failed to rescan right op", K(ret));
    } else { /*do nothing*/
    }
  } else {
    if (OB_FAIL(prepare_rescan_params())) {
      LOG_WARN("failed to prepare rescan params", K(ret));
    } else if (OB_FAIL(right_->rescan())) {
      LOG_WARN("failed to rescan right op", K(ret));
    } else { /*do nothing*/
    }
  }
  state_ = JS_READ_RIGHT;

  return ret;
}

int ObNestedLoopJoinOp::read_left_func_end()
{
  state_ = JS_JOIN_END;
  return OB_ITER_END;
}

int ObNestedLoopJoinOp::read_right_operate()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_next_right_row()) && OB_ITER_END != ret) {
    LOG_WARN("failed to get next right row", K(ret));
  } else {
    clear_evaluated_flag();
  }

  return ret;
}

int ObNestedLoopJoinOp::read_right_func_going()
{
  int ret = OB_SUCCESS;
  bool is_match = false;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(calc_other_conds(is_match))) {
      LOG_WARN("failed to compare left and right row on other join conds", K(ret));
    } else if (is_match) {
      output_row_produced_ = true;
      left_row_joined_ = true;  // left row joined sign.
    } else {
    }
  }

  return ret;
}

int ObNestedLoopJoinOp::read_right_func_end()
{
  int ret = OB_SUCCESS;
  if (need_left_join() && !left_row_joined_) {
    output_row_produced_ = true;
    if (OB_FAIL(blank_right_row())) {
      LOG_WARN("failed to blank right row", K(ret));
    }
  }
  state_ = JS_READ_LEFT;

  return ret;
}

bool ObNestedLoopJoinOp::is_full() const
{
  return left_store_.get_row_cnt() >= MY_SPEC.batch_size_;
}

}  // end namespace sql
}  // end namespace oceanbase
