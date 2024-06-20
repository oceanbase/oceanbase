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
#include "sql/engine/basic/ob_material_vec_op.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

OB_SERIALIZE_MEMBER((ObNestedLoopJoinSpec, ObBasicNestedLoopJoinSpec),
                    group_rescan_, group_size_,
                    left_expr_ids_in_other_cond_,
                    left_rescan_params_,
                    right_rescan_params_);

ObNestedLoopJoinOp::ObNestedLoopJoinOp(ObExecContext &exec_ctx,
                                       const ObOpSpec &spec,
                                       ObOpInput *input)
  : ObBasicNestedLoopJoinOp(exec_ctx, spec, input),
    state_(JS_READ_LEFT), mem_context_(nullptr), left_store_("NljLStore"), is_left_end_(false),
    last_store_row_(), save_last_row_(false), defered_right_rescan_(false),
    batch_rescan_ctl_(),
    batch_state_(JS_FILL_LEFT), save_last_batch_(false),
    batch_mem_ctx_(NULL), stored_rows_(NULL), right_store_("NljRStore"), left_brs_(NULL), left_matched_(NULL),
    need_switch_iter_(false), iter_end_(false), op_max_batch_size_(0),
    max_group_size_(OB_MAX_BULK_JOIN_ROWS),
    group_join_buffer_(),
    match_left_batch_end_(false), match_right_batch_end_(false), l_idx_(0),
    no_match_row_found_(true), need_output_row_(false), left_expr_extend_size_(0)
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
  int64_t simulate_group_size = - EVENT_CALL(EventTable::EN_DAS_SIMULATE_GROUP_SIZE);
  int64_t group_size = 0;
  if (simulate_group_size > 0) {
    max_group_size_ = simulate_group_size;
    group_size = simulate_group_size;
    LOG_TRACE("simulate group size is", K(simulate_group_size));
  } else {
    group_size = MY_SPEC.group_size_;
  }
  if (OB_SUCC(ret) && is_vectorized()) {
    if (MY_SPEC.group_rescan_) {
      if (simulate_group_size > 0) {
        max_group_size_ = simulate_group_size + MY_SPEC.plan_->get_batch_size();
      } else {
        max_group_size_ = OB_MAX_BULK_JOIN_ROWS + MY_SPEC.plan_->get_batch_size();
      }
      LOG_TRACE("max group size of NLJ is", K(max_group_size_), K(MY_SPEC.plan_->get_batch_size()));
    }
    if (OB_ISNULL(batch_mem_ctx_)) {
      ObSQLSessionInfo *session = ctx_.get_my_session();
      uint64_t tenant_id =session->get_effective_tenant_id();
      lib::ContextParam param;
      const int64_t mem_limit = 8 * 1024 * 1024; //8M;
      param.set_mem_attr(tenant_id,
                         ObModIds::OB_SQL_NLJ_CACHE,
                         ObCtxIds::WORK_AREA)
        .set_properties(lib::USE_TL_PAGE_OPTIONAL);
      if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(batch_mem_ctx_, param))) {
        LOG_WARN("create entity failed", K(ret));
      } else if (OB_ISNULL(batch_mem_ctx_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null memory entity returned", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      char *buf = (char *)batch_mem_ctx_->get_arena_allocator()
                          .alloc(ObBitVector::memory_size(MY_SPEC.max_batch_size_));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc", K(ret));
      } else {
        MEMSET(buf, 0, ObBitVector::memory_size(MY_SPEC.max_batch_size_));
        left_matched_ = to_bit_vector(buf);
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(left_batch_.init(&(left_->get_spec().output_),
                                   &(batch_mem_ctx_->get_arena_allocator()),
                                   MY_SPEC.max_batch_size_))) {
        LOG_WARN("fail to init batch", K(ret));
      } else if (MY_SPEC.enable_px_batch_rescan_) {
        if (OB_FAIL(last_save_batch_.init(&left_->get_spec().output_,
                                          &batch_mem_ctx_->get_arena_allocator(),
                                          MY_SPEC.max_batch_size_))) {
          LOG_WARN("fail to init batch", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret) && MY_SPEC.group_rescan_) {
    if (OB_FAIL(group_join_buffer_.init(this,
                                        max_group_size_,
                                        group_size,
                                        &MY_SPEC.rescan_params_,
                                        &MY_SPEC.left_rescan_params_,
                                        &MY_SPEC.right_rescan_params_))) {
      LOG_WARN("init batch info failed", KR(ret));
    }
  }
  return ret;
}
//NLJ has its own switch_iterator
int ObNestedLoopJoinOp::switch_iterator()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOperator::inner_switch_iterator())) {
    LOG_WARN("failed to inner switch iterator", K(ret));
  } else if (OB_FAIL(left_->switch_iterator())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("switch left child iterator failed", K(ret));
    }
  } else {
    reset_buf_state();
  }

#ifndef NDEBUG
  OX(OB_ASSERT(false == brs_.end_));
#endif

  return ret;
}

int ObNestedLoopJoinOp::rescan()
{
  int ret = OB_SUCCESS;
  //NLJ's rescan should only drive left child's rescan,
  //the right child's rescan is defer to rescan_right_operator() driven by get_next_row();
  defered_right_rescan_ = true;
  if (!MY_SPEC.group_rescan_) {
    if (OB_FAIL(left_->rescan())) {
      LOG_WARN("rescan left child operator failed", KR(ret), "child op_type", left_->op_name());
    } else if (OB_FAIL(inner_rescan())) {
      LOG_WARN("failed to inner rescan", KR(ret));
    }
  } else {
    if (OB_FAIL(group_join_buffer_.init_above_group_params())) {
      LOG_WARN("init above bnlj params failed", KR(ret));
    } else if (OB_FAIL(group_join_buffer_.rescan_left())) {
      LOG_WARN("rescan left failed", KR(ret));
    } else if (OB_FAIL(inner_rescan())) {
      LOG_WARN("inner rescan failed", KR(ret));
    }
  }

#ifndef NDEBUG
  OX(OB_ASSERT(false == brs_.end_));
#endif

  return ret;
}

int ObNestedLoopJoinOp::do_drain_exch_multi_lvel_bnlj()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(try_open())) {
    LOG_WARN("fail to open operator", K(ret));
  } else if (!exch_drained_) {
    // the drain request is triggered by current NLJ operator, and current NLJ is a multi level Batch NLJ
    // It will block rescan request for it's child operator, if the drain request is passed to it's child operator
    // The child operators will be marked as iter-end_, and will not get any row if rescan is blocked
    // So we block the drain request here; Only set current operator to end;
    int tmp_ret = inner_drain_exch();
    exch_drained_ = true;
    brs_.end_ = true;
    batch_reach_end_ = true;
    row_reach_end_ = true;
    if (OB_SUCC(ret)) {
      ret = tmp_ret;
    }
  }
  return ret;
}

int ObNestedLoopJoinOp::do_drain_exch()
{
  int ret = OB_SUCCESS;
  if (!MY_SPEC.group_rescan_) {
    if (OB_FAIL( ObOperator::do_drain_exch())) {
      LOG_WARN("failed to drain NLJ operator", K(ret));
    }
  } else if (!group_join_buffer_.is_multi_level()) {
    if (OB_FAIL( ObOperator::do_drain_exch())) {
      LOG_WARN("failed to drain NLJ operator", K(ret));
    }
  } else {
    if (!is_operator_end()) {
      // the drain request is triggered by parent operator
      // NLJ needs to pass the drain request to it's child operator
      LOG_TRACE("The drain request is passed by parent operator");
      if (OB_FAIL( ObOperator::do_drain_exch())) {
        LOG_WARN("failed to drain normal NLJ operator", K(ret));
      }
    } else if (OB_FAIL(do_drain_exch_multi_lvel_bnlj())) {
      LOG_WARN("failed to drain multi level NLJ operator", K(ret));
    }
  }
  return ret;
}

int ObNestedLoopJoinOp::inner_rescan()
{
  int ret = OB_SUCCESS;
  reset_buf_state();
  set_param_null();
  if (OB_FAIL(ObBasicNestedLoopJoinOp::inner_rescan())) {
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
    output_row_produced_  = false;
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
    } // while end
  }
  if (OB_ITER_END == ret) {
    set_param_null();
  }

  return ret;
}

void ObNestedLoopJoinOp::reset_buf_state()
{
  state_ = JS_READ_LEFT;
  left_store_iter_.reset();
  left_store_.reset();
  is_left_end_ = false;
  last_store_row_.reset();
  save_last_row_ = false;
  batch_rescan_ctl_.reset();
  batch_state_ = JS_FILL_LEFT;
  save_last_batch_ = false;
  need_switch_iter_ = false;
  iter_end_ = false;
  left_batch_.clear_saved_size();
  last_save_batch_.clear_saved_size();
  match_left_batch_end_ = false;
  match_right_batch_end_ = false;
  l_idx_ = 0;
  no_match_row_found_ = true;
  need_output_row_ = false;
  left_expr_extend_size_ = 0;
}

int ObNestedLoopJoinOp::fill_cur_row_rescan_param()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = ctx_.get_physical_plan_ctx();
  if (OB_ISNULL(plan_ctx)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("plan ctx or left row is null", K(ret));
  } else if (batch_rescan_ctl_.cur_idx_ >= batch_rescan_ctl_.params_.get_count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row idx is unexpected", K(ret),
             K(batch_rescan_ctl_.cur_idx_), K(batch_rescan_ctl_.params_.get_count()));
  } else {
    common::ObIArray<common::ObObjParam>& params =
        batch_rescan_ctl_.params_.get_one_batch_params(batch_rescan_ctl_.cur_idx_);
    int64_t param_cnt = params.count();
    int64_t idx = OB_INVALID_ID;
    for (int64_t i = 0; OB_SUCC(ret) && i < param_cnt; ++i) {
      idx = batch_rescan_ctl_.params_.get_param_idx(i);
      plan_ctx->get_param_store_for_update().at(idx) = params.at(i);
      const ObDynamicParamSetter &rescan_param = get_spec().rescan_params_.at(i);
      if (OB_FAIL(rescan_param.set_dynamic_param(eval_ctx_))) {
        LOG_WARN("fail to set dynamic param", K(ret));
      }
    }
  }
  return ret;
}

int ObNestedLoopJoinOp::join_row_with_semi_join()
{
  int ret = OB_SUCCESS;
  const bool is_anti = (LEFT_ANTI_JOIN == MY_SPEC.join_type_);
  while (OB_SUCC(ret) && OB_SUCC(get_next_left_row())) {
    clear_evaluated_flag();
    if (OB_FAIL(try_check_status())) {
      LOG_WARN("check status failed", K(ret));
    } else if (OB_FAIL(prepare_rescan_params())) {
      LOG_WARN("prepare right child rescan param failed", K(ret));
    } else if (OB_FAIL(rescan_right_operator())) {
      LOG_WARN("rescan right child failed", K(ret));
    }
    if (OB_SUCC(ret)) {
      // 迭代右表
      bool is_matched = false;
      while (OB_SUCC(ret)
             && !is_matched
             && OB_SUCC(right_->get_next_row())) {
        clear_evaluated_flag();
        if (OB_FAIL(try_check_status())) {
          LOG_WARN("check status failed", K(ret));
        } else if (OB_FAIL(calc_other_conds(is_matched))) {
          LOG_WARN("calc other conditions failed", K(ret));
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS; // 右表不存在和左表匹配的行，所以迭代左表下一行
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
  clear_evaluated_flag();
  if (MY_SPEC.group_rescan_ || MY_SPEC.enable_px_batch_rescan_) {
    if (OB_FAIL(group_read_left_operate()) && OB_ITER_END != ret) {
      LOG_WARN("failed to read left group", K(ret));
    }
  } else if (FALSE_IT(set_param_null())) {
  } else if (OB_FAIL(get_next_left_row()) && OB_ITER_END != ret) {
    LOG_WARN("failed to get next left row", K(ret));
  }

  return ret;
}


int ObNestedLoopJoinOp::rescan_params_batch_one(int64_t batch_idx)
{
  int ret = OB_SUCCESS;
  left_batch_.to_exprs(eval_ctx_, batch_idx, batch_idx);
  if (OB_FAIL(prepare_rescan_params())) {
    LOG_WARN("failed to prepare rescan params", K(ret));
  } else if (OB_FAIL(rescan_right_operator())) {
    LOG_WARN("failed to rescan right op", K(ret));
  }

  return ret;
}

int ObNestedLoopJoinOp::rescan_right_operator()
{
  int ret = OB_SUCCESS;
  bool do_rescan = false;
  if (defered_right_rescan_) {
    do_rescan = true;
    defered_right_rescan_ = false;
  } else {
    // FIXME bin.lb: handle monitor dump + material ?
    if (PHY_MATERIAL == right_->get_spec().type_) {
      if (OB_FAIL(static_cast<ObMaterialOp*>(right_)->rewind())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("rewind failed", K(ret));
        }
      }
    } else if (PHY_VEC_MATERIAL == right_->get_spec().type_) {
      if (OB_FAIL(static_cast<ObMaterialVecOp*>(right_)->rewind())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("rewind failed", K(ret));
        }
      }
    } else {
      do_rescan = true;
    }
  }
  if (OB_SUCC(ret) && do_rescan) {
    if (OB_FAIL(right_->rescan())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("rescan right failed", K(ret));
      }
    } else {
      /*do nothing*/
    }
  }
  return ret;
}

int ObNestedLoopJoinOp::group_read_left_operate()
{
  int ret = OB_SUCCESS;
  if (MY_SPEC.enable_px_batch_rescan_) {
    if (left_store_iter_.is_valid() && left_store_iter_.has_next()) {
      // 重新设置右表 table scan result, result 为下一个 cache
      if (MY_SPEC.enable_px_batch_rescan_) {
        batch_rescan_ctl_.cur_idx_++;
      }
    } else {
      if (is_left_end_) {
        ret = OB_ITER_END;
      } else {
        if (OB_ISNULL(mem_context_)) {
          ObSQLSessionInfo *session = ctx_.get_my_session();
          uint64_t tenant_id =session->get_effective_tenant_id();
          lib::ContextParam param;
          param.set_mem_attr(tenant_id,
                             ObModIds::OB_SQL_NLJ_CACHE,
                             ObCtxIds::WORK_AREA)
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
          // 没有下一个了, 尝试填充 cache.
          batch_rescan_ctl_.reuse();
          left_store_iter_.reset();
          left_store_.reset();
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
          set_param_null();
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
            } else if (OB_FAIL(prepare_rescan_params(true/*is_group*/))) {
              LOG_WARN("failed to prepare rescan params", K(ret));
              // 下压参数数据是由被换的原始表达式计算生成, 比如c1 = c2 + 1--> c1 = ?;
              // 下压参数?的值, 由c2+1计算而来, c2+1的内存是复用的, 如果此时不深拷贝
              // 计算query range的下压param, 则可能导致后面query range的结果和
              // 前面query range的obobj对应的ptr(string/number类型在obj中ptr)使用相同指针;
            } else {
              ignore_end = true;
            }
          }
          if (OB_SUCC(ret)) {
            // here need to set param null, because dynamic datum ptr
            // which from last batch row may invalid
            set_param_null();
            if (OB_FAIL(last_store_row_.shadow_copy(left_->get_spec().output_, eval_ctx_))) {
              LOG_WARN("failed to shadow copy last left row", K(ret));
            } else {
              save_last_row_ = true;
            }
          }
        }
        if (OB_SUCC(ret) || (ignore_end && OB_ITER_END == ret)) {
          ret = OB_SUCCESS;
          ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(ctx_);
          if (OB_FAIL(left_store_.finish_add_row(false))) {
            LOG_WARN("failed to finish add row to row store", K(ret));
          } else if (OB_FAIL(left_store_.begin(left_store_iter_))) {
            LOG_WARN("failed to begin iterator for chunk row store", K(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      // 拿到下一行 ret = OB_SUCCESS;
      clear_evaluated_flag();
      if (OB_FAIL(left_store_iter_.get_next_row(left_->get_spec().output_,
                                                eval_ctx_))) {
        LOG_WARN("Failed to get next row", K(ret));
      } else if (MY_SPEC.enable_px_batch_rescan_ && OB_FAIL(fill_cur_row_rescan_param())) {
        LOG_WARN("fail to fill cur row rescan param", K(ret));
      } else if (MY_SPEC.enable_px_batch_rescan_) {
        OZ(right_->rescan());
      }
      if (OB_SUCC(ret)) {
        left_row_joined_ = false;
      }
    }
  } else {
    // das group rescan
    bool has_next = false;
    if (OB_FAIL(group_join_buffer_.fill_group_buffer())) {
      LOG_WARN("fill group buffer failed", KR(ret));
    } else if (OB_FAIL(group_join_buffer_.has_next_left_row(has_next))) {
      LOG_WARN("check has next failed", KR(ret));
    } else if (has_next) {
      clear_evaluated_flag();
      if (OB_FAIL(group_join_buffer_.rescan_right())) {
        if (OB_ITER_END == ret) {
          ret = OB_ERR_UNEXPECTED;
        }
        LOG_WARN("rescan right failed", KR(ret));
      }
    } else {
      ret = OB_ITER_END;
    }
    if (OB_SUCC(ret)) {
      clear_evaluated_flag();
      if (OB_FAIL(group_join_buffer_.get_next_row_from_store())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get next row failed", KR(ret));
        }
      } else {
        left_row_joined_ = false;
      }
    }
  }
  return ret;
}

int ObNestedLoopJoinOp::read_left_func_going()
{
  int ret = OB_SUCCESS;
  if (MY_SPEC.group_rescan_ || MY_SPEC.enable_px_batch_rescan_) {
    // do nothing
    // group nested loop join 已经做过 rescan 了
  } else if (OB_FAIL(prepare_rescan_params())) {
    LOG_WARN("failed to prepare rescan params", K(ret));
  } else if (OB_FAIL(rescan_right_operator())) {
    LOG_WARN("rescan right operator failed", K(ret));
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
  clear_evaluated_flag();
  if (OB_FAIL(get_next_row_from_right()) && OB_ITER_END != ret) {
    LOG_WARN("failed to get next right row", K(ret));
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
      left_row_joined_ = true;     // left row joined sign.
    } else {}
  }

  return ret;
}

int ObNestedLoopJoinOp::read_right_func_end()
{
  int ret = OB_SUCCESS;
  if (need_left_join() && !left_row_joined_) {
    output_row_produced_ = true;
    if (OB_FAIL(blank_row(right_->get_spec().output_))) {
      LOG_WARN("failed to blank right row", K(ret));
    }
  }
  state_ = JS_READ_LEFT;

  return ret;
}

bool ObNestedLoopJoinOp::is_full() const
{
  return left_store_.get_row_cnt() >= MY_SPEC.group_size_;
}

int ObNestedLoopJoinOp::get_left_batch()
{
  int ret = OB_SUCCESS;
  if (MY_SPEC.group_rescan_ || MY_SPEC.enable_px_batch_rescan_) {
    if (OB_FAIL(group_get_left_batch(left_brs_)) && OB_ITER_END != ret) {
      LOG_WARN("fail to get left batch", K(ret));
    }
  } else {
    // Reset exec param before get left row, because the exec param still reference
    // to the previous row, when get next left row, it may become wild pointer.
    // The exec parameter may be accessed by the under PX execution by serialization, which
    // serialize whole parameters store.
    set_param_null();
    if (is_left_end_) {
      // do nothing
      ret = OB_ITER_END;
    } else if (!IS_LEFT_SEMI_ANTI_JOIN(MY_SPEC.join_type_) // semi/anti has to_exprs in out process
               && FALSE_IT(left_batch_.to_exprs(eval_ctx_))) {
    } else if (OB_FAIL(left_->get_next_batch(op_max_batch_size_, left_brs_))) {
      LOG_WARN("fail to get next batch", K(ret));
    } else if (left_brs_->end_) {
      is_left_end_ = true;
    }
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (left_brs_->end_ && left_brs_->size_ == 0) {
    ret = OB_ITER_END;
  } else {
    left_batch_.from_exprs(eval_ctx_, left_brs_->skip_, left_brs_->size_);
  }

  return ret;
}

int ObNestedLoopJoinOp::group_get_left_batch(const ObBatchRows *&left_brs)
{
  int ret = OB_SUCCESS;
  if (MY_SPEC.enable_px_batch_rescan_) {
    left_brs = &left_->get_brs();
    if (left_store_iter_.is_valid() && left_store_iter_.has_next()) {
      // do nothing
    } else {
      if (is_left_end_) {
        ret = OB_ITER_END;
        // do nothing
      } else {
        if (OB_ISNULL(mem_context_)) {
          ObSQLSessionInfo *session = ctx_.get_my_session();
          uint64_t tenant_id =session->get_effective_tenant_id();
          lib::ContextParam param;
          param.set_mem_attr(tenant_id,
                             ObModIds::OB_SQL_NLJ_CACHE,
                             ObCtxIds::WORK_AREA)
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
        if (OB_SUCC(ret)) {
          // 没有下一个了, 尝试填充 cache.
          batch_rescan_ctl_.reuse();
          left_store_iter_.reset();
          left_store_.reset();
          mem_context_->get_arena_allocator().reset();
          save_last_row_ = false;
          ObEvalCtx::BatchInfoScopeGuard batch_info_guard(eval_ctx_);
          while (OB_SUCC(ret) && continue_fetching()) {
            // need clear evaluated flag, since prepare_rescan_params() will evaluate expression.
            clear_evaluated_flag();
            if (save_last_batch_) {
              last_save_batch_.to_exprs(eval_ctx_);
              save_last_batch_ = false;
            }
            set_param_null();
            if (OB_FAIL(left_->get_next_batch(op_max_batch_size_, left_brs_))) {
              LOG_WARN("failed to get next left row", K(ret));
            } else if (left_brs_->end_) {
              is_left_end_ = true;
            }
            for (int64_t l_idx = 0;  OB_SUCC(ret) && l_idx < left_brs_->size_; l_idx++) {
              if (left_brs_->skip_->exist(l_idx)) { continue; }
              batch_info_guard.set_batch_idx(l_idx);
              batch_info_guard.set_batch_size(left_brs_->size_);
              if (OB_FAIL(left_store_.add_row(left_->get_spec().output_, &eval_ctx_))) {
                LOG_WARN("failed to store left row", K(ret));
                // do nothing
              } else if (OB_FAIL(prepare_rescan_params(true))) {
                LOG_WARN("failed to prepare rescan params", K(ret));
                // 下压参数数据是由被换的原始表达式计算生成, 比如c1 = c2 + 1--> c1 = ?;
                // 下压参数?的值, 由c2+1计算而来, c2+1的内存是复用的, 如果此时不深拷贝
                // 计算query range的下压param, 则可能导致后面query range的结果和
                // 前面query range的obobj对应的ptr(string/number类型在obj中ptr)使用相同指针;
              }
            } // for end
          }
          if (OB_SUCC(ret)) {
            set_param_null();
            if (left_brs_->size_ == 0 && left_brs_->end_) {
              // do nothing
            } else {
              last_save_batch_.from_exprs(eval_ctx_, left_brs_->skip_, left_brs_->size_);
              save_last_batch_ = true;
            }
          }
          clear_evaluated_flag();
        }
        if (OB_SUCC(ret) ) {
          if (left_store_.get_row_cnt() <= 0) {
            ret = OB_ITER_END;
          } else if (OB_FAIL(left_store_.finish_add_row(false))) {
            LOG_WARN("failed to finish add row to row store", K(ret));
          } else if (OB_FAIL(left_store_.begin(left_store_iter_))) {
            LOG_WARN("failed to begin iterator for chunk row store", K(ret));
          } else {
            need_switch_iter_ = false;
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      int64_t read_size = 0;
      int64_t max_size = MY_SPEC.max_batch_size_;
      last_save_batch_.extend_save(eval_ctx_, max_size);
      if (OB_FAIL(left_store_iter_.get_next_batch(left_->get_spec().output_,
                                                  eval_ctx_,
                                                  max_size,
                                                  read_size))) {
        if (OB_ITER_END == ret) {
          // do nothing
        } else {
          LOG_WARN("Failed to get next row", K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        // left_brs.size_ may be larger or smaller than read_size:
        //   left_brs.size_ > read_size: group size is small and lots of left rows were skipped;
        //   left_brs.size_ < read_size: left_brs reaches iter end with no enough rows;
        // Thus, we need to reset skip_ with max size of left_brs.size_ and read_size.
        const_cast<ObBatchRows *>(left_brs)->skip_->reset(std::max(left_brs->size_, read_size));
        const_cast<ObBatchRows *>(left_brs)->size_ = read_size;
        const_cast<ObBatchRows *>(left_brs)->end_ = false;
        left_row_joined_ = false;
      }
    }
  } else {
    // das group rescan
    bool has_next = false;
    if (OB_FAIL(group_join_buffer_.batch_fill_group_buffer(op_max_batch_size_, left_brs_))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("batch fill group buffer failed", KR(ret));
      }
    } else if (OB_FAIL(group_join_buffer_.has_next_left_row(has_next))) {
      LOG_WARN("check has next failed", KR(ret));
    } else if (!has_next) {
      ret = OB_ITER_END;
    }
    if (OB_SUCC(ret)) {
      int64_t read_size = 0;
      int64_t max_size = op_max_batch_size_;
      if (OB_FAIL(group_join_buffer_.get_next_batch_from_store(max_size, read_size))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get next batch from store failed", KR(ret));
        }
      } else {
        // left_brs.size_ may be larger or smaller than read_size:
        //   left_brs.size_ > read_size: group size is small and lots of left rows were skipped;
        //   left_brs.size_ < read_size: left_brs reaches iter end with no enough rows;
        // Thus, we need to reset skip_ with max size of left_brs.size_ and read_size.
        const_cast<ObBatchRows *>(left_brs)->skip_->reset(std::max(left_brs->size_, read_size));
        const_cast<ObBatchRows *>(left_brs)->size_ = read_size;
        const_cast<ObBatchRows *>(left_brs)->end_ = false;
        left_row_joined_ = false;
      }
    }
  }
  return ret;
}



int ObNestedLoopJoinOp::rescan_right_op()
{
  int ret = OB_SUCCESS;
  ObEvalCtx::BatchInfoScopeGuard batch_info_guard(eval_ctx_);
  // Note:
  // Overwrite batch_size in the beginning of the loop as eval_ctx_.batch_size
  // would be modified when processing right child.
  // Adding seperated guards for left/right children can also solve the problem,
  // we don't choose that way due to performance reason.
  batch_info_guard.set_batch_size(left_brs_->size_);
  if (!MY_SPEC.group_rescan_ && !MY_SPEC.enable_px_batch_rescan_) {
    batch_info_guard.set_batch_idx(l_idx_);
    if (OB_FAIL(rescan_params_batch_one(l_idx_))) {
      LOG_WARN("fail to rescan params", K(ret));
    }
  } else if (MY_SPEC.group_rescan_ && !MY_SPEC.enable_px_batch_rescan_) {
    if (OB_FAIL(group_join_buffer_.rescan_right())) {
      if (OB_ITER_END == ret) {
        ret = OB_ERR_UNEXPECTED;
      }
      LOG_WARN("rescan right failed", KR(ret));
    } else if (OB_FAIL(group_join_buffer_.fill_cur_row_group_param())) {
      LOG_WARN("fill group param failed", KR(ret));
    }
  } else if (MY_SPEC.enable_px_batch_rescan_) {
    // NOTE: left batch is ALWAYS continous, NO need to check skip for
    // left_brs under px batch rescan
    batch_info_guard.set_batch_idx(l_idx_);
    left_batch_.to_exprs(eval_ctx_, l_idx_, l_idx_);
    if (OB_FAIL(fill_cur_row_rescan_param())) {
      LOG_WARN("fail to fill cur row rescan param", K(ret));
    } else if (OB_FAIL(right_->rescan())) {
      LOG_WARN("failed to rescan right", K(ret));
    } else {
      // do nothing
    }
  }
  return ret;
}

int ObNestedLoopJoinOp::process_right_batch()
{
  int ret = OB_SUCCESS;
  ObEvalCtx::BatchInfoScopeGuard batch_info_guard(eval_ctx_);
  batch_info_guard.set_batch_size(left_brs_->size_);
  reset_batchrows();
  const ObBatchRows *right_brs = &right_->get_brs();
  const ObIArray<ObExpr *> &conds = get_spec().other_join_conds_;
  clear_evaluated_flag();
  DASGroupScanMarkGuard mark_guard(ctx_.get_das_ctx(), MY_SPEC.group_rescan_);
  if (OB_FAIL(get_next_batch_from_right(right_brs))) {
    LOG_WARN("fail to get next right batch", K(ret), K(MY_SPEC));
  } else if (0 == right_brs->size_ && right_brs->end_) {
    match_right_batch_end_ = true;
  } else {
    if (MY_SPEC.enable_px_batch_rescan_) {
      last_save_batch_.extend_save(eval_ctx_, right_brs->size_);
    } else if (MY_SPEC.group_rescan_) {
      group_join_buffer_.get_last_batch().extend_save(eval_ctx_, right_brs->size_);
    } else {
      left_batch_.extend_save(eval_ctx_, right_brs->size_);
    }
    left_expr_extend(right_brs->size_);
    if (0 == conds.count()) {
      brs_.skip_->deep_copy(*right_brs->skip_, right_brs->size_);
    } else {
      batch_info_guard.set_batch_size(right_brs->size_);
      bool is_match = false;
      for (int64_t r_idx = 0; OB_SUCC(ret) && r_idx < right_brs->size_; r_idx++) {
        batch_info_guard.set_batch_idx(r_idx);
        if (right_brs->skip_->exist(r_idx)) {
          brs_.skip_->set(r_idx);
        } else if (OB_FAIL(calc_other_conds(is_match))) {
          LOG_WARN("calc_other_conds failed", K(ret), K(r_idx),
                   K(right_brs->size_));
        } else if (!is_match) {
          brs_.skip_->set(r_idx);
        } else { /*do nothing*/
        }
        LOG_DEBUG("cal_other_conds finished ", K(is_match), K(l_idx_), K(r_idx));
      } // for conds end
    }

    if (OB_SUCC(ret)) {
      brs_.size_ = right_brs->size_;
      int64_t skip_cnt = brs_.skip_->accumulate_bit_cnt(right_brs->size_);
      if (IS_LEFT_SEMI_ANTI_JOIN(MY_SPEC.join_type_)) {
        if (right_brs->size_ - skip_cnt > 0) {
          left_matched_->set(l_idx_);
          match_right_batch_end_ = true;
        }
      } else {
        if (right_brs->size_ - skip_cnt > 0) {
          need_output_row_ = true;
          no_match_row_found_ = false;
        }
      }
      match_right_batch_end_ = match_right_batch_end_ || right_brs->end_;
    }
  }
  // outer join
  if (OB_SUCC(ret)) {
    if (match_right_batch_end_ && no_match_row_found_ && need_left_join()) {
      need_output_row_ = true;
    }
  }

  return ret;
}
// Expand left row full column
int ObNestedLoopJoinOp::left_expr_extend(int32_t size)
{
  int ret = OB_SUCCESS;
  for (int32_t r_idx = left_expr_extend_size_; OB_SUCC(ret) && r_idx < size; r_idx++) {
    left_batch_.to_exprs(eval_ctx_, l_idx_, r_idx);
  }
  if (left_expr_extend_size_ < size) {
    left_expr_extend_size_ = size;
  }
  return ret;
}

int ObNestedLoopJoinOp::output()
{
  int ret = OB_SUCCESS;
  if (IS_LEFT_SEMI_ANTI_JOIN(MY_SPEC.join_type_)) {
    reset_batchrows();
    if (LEFT_SEMI_JOIN == MY_SPEC.join_type_) {
      brs_.skip_->bit_calculate(*left_batch_.get_skip(), *left_matched_, left_batch_.get_size(),
                                [](const uint64_t l, const uint64_t r) { return (l | (~r)); });
    } else if (LEFT_ANTI_JOIN == MY_SPEC.join_type_) {
      brs_.skip_->bit_calculate(*left_batch_.get_skip(), *left_matched_, left_batch_.get_size(),
                                [](const uint64_t l, const uint64_t r) { return (l | r); });
    }
    if (MY_SPEC.enable_px_batch_rescan_) {
      last_save_batch_.extend_save(eval_ctx_, left_batch_.get_size());
    }
    left_batch_.to_exprs(eval_ctx_);
    brs_.size_ = left_batch_.get_size();
    left_matched_->reset(left_batch_.get_size());
  } else {
    // do nothing.
  }
  // outer join: generate a blank row for LEFT OUTER JOIN
  // Note: optimizer guarantee there is NO RIGHT/FULL OUTER JOIN for NLJ
  if (OB_SUCC(ret) && match_right_batch_end_ && no_match_row_found_ && need_left_join()) {
    reset_batchrows();
    brs_.size_ = 1;
    ObEvalCtx::BatchInfoScopeGuard guard(eval_ctx_);
    guard.set_batch_idx(0);
    blank_row_batch_one(right_->get_spec().output_);
    if (MY_SPEC.enable_px_batch_rescan_) {
      last_save_batch_.extend_save(eval_ctx_, 1);
    } else if (!MY_SPEC.group_rescan_) {
      left_batch_.extend_save(eval_ctx_, 1);
    }
    left_batch_.to_exprs(eval_ctx_, l_idx_, 0);
  }

  return ret;
}

void ObNestedLoopJoinOp::reset_left_batch_state()
{
  match_left_batch_end_ = false;
  l_idx_ = 0;
}

void ObNestedLoopJoinOp::reset_right_batch_state()
{
  match_right_batch_end_ = false;
  l_idx_++;
  no_match_row_found_ = true;
  left_expr_extend_size_ = 0;
  if (MY_SPEC.enable_px_batch_rescan_) {
    batch_rescan_ctl_.cur_idx_++;
  }
}

void ObNestedLoopJoinOp::skip_l_idx()
{
  if (!MY_SPEC.group_rescan_ && !MY_SPEC.enable_px_batch_rescan_) {
    while (l_idx_ >= 0 && l_idx_ < left_brs_->size_) {
      if (left_brs_->skip_->exist(l_idx_)) {
        l_idx_++;
      } else {
        break;
      }
    }
  }
}

int ObNestedLoopJoinOp::inner_get_next_batch(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  if (iter_end_) {
    brs_.size_ = 0;
    brs_.end_ = true;
  }
  op_max_batch_size_ = min(max_row_cnt, MY_SPEC.max_batch_size_);
  while (!iter_end_ && OB_SUCC(ret)) {
    clear_evaluated_flag();
    if (JS_FILL_LEFT == batch_state_) {
      if (OB_FAIL(get_left_batch())) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          brs_.size_ = 0;
          brs_.end_ = true;
          iter_end_ = true;
        } else {
          LOG_WARN("fail to get left batch", K(ret));
        }
      } else {
        batch_state_ = JS_RESCAN_RIGHT_OP;
      }
    }
    if (OB_SUCC(ret) && JS_RESCAN_RIGHT_OP == batch_state_) {
      skip_l_idx();
      if (l_idx_ >= left_brs_->size_) {
        match_left_batch_end_ = true;
      }
      if (!match_left_batch_end_ && OB_FAIL(rescan_right_op())) {
        LOG_WARN("fail to rescan right op", K(ret));
      } else {
        if (match_left_batch_end_ && IS_LEFT_SEMI_ANTI_JOIN(MY_SPEC.join_type_)) {
          batch_state_ = JS_OUTPUT;
          reset_left_batch_state();
        } else if (match_left_batch_end_) {
          batch_state_ = JS_FILL_LEFT;
          reset_left_batch_state();
        } else {
          batch_state_ = JS_PROCESS_RIGHT_BATCH;
        }
      }
    }
    if (OB_SUCC(ret) && JS_PROCESS_RIGHT_BATCH == batch_state_) {
      if (OB_FAIL(process_right_batch())) {
        LOG_WARN("fail to process right batch", K(ret));
      } else {
        if (IS_LEFT_SEMI_ANTI_JOIN(MY_SPEC.join_type_)) {
          if (match_right_batch_end_) {
            batch_state_ = JS_RESCAN_RIGHT_OP;
            reset_right_batch_state();
          } else  {
            batch_state_ = JS_PROCESS_RIGHT_BATCH;
          }
        } else {
          if (need_output_row_) {
            batch_state_ = JS_OUTPUT;
            need_output_row_ = false;
          } else {
            if (match_right_batch_end_) {
              batch_state_ = JS_RESCAN_RIGHT_OP;
              reset_right_batch_state();
            } else {
              batch_state_ = JS_PROCESS_RIGHT_BATCH;
            }
          }
        }
      }
    }
    if (OB_SUCC(ret) && JS_OUTPUT == batch_state_) {
      if (OB_FAIL(output())) {
        LOG_WARN("fail to output", K(ret));
      } else {
        if (IS_LEFT_SEMI_ANTI_JOIN(MY_SPEC.join_type_)) {
          batch_state_ = JS_FILL_LEFT;
        } else if (match_right_batch_end_) {
          batch_state_ = JS_RESCAN_RIGHT_OP;
          reset_right_batch_state();
        } else {
          batch_state_ = JS_PROCESS_RIGHT_BATCH;
        }
        break;
      }
    }
  }
  if (OB_SUCC(ret) && iter_end_) {
    set_param_null();
  }

  return ret;
}


//calc other conditions
int ObNestedLoopJoinOp::calc_other_conds(bool &is_match)
{
  int ret = OB_SUCCESS;
  is_match = true;
  const ObIArray<ObExpr *> &conds = MY_SPEC.other_join_conds_;
  ObDatum *cmp_res = NULL;
  ARRAY_FOREACH(conds, i) {
    auto cond = conds.at(i);
    if (OB_FAIL(conds.at(i)->eval(eval_ctx_, cmp_res))) {
      LOG_WARN("fail to calc other join condition", K(ret), K(*conds.at(i)));
    } else if (cmp_res->is_null() || 0 == cmp_res->get_int()) {
      is_match = false;
      break;
    }
  }

  return ret;
}

int ObNestedLoopJoinOp::get_next_batch_from_right(const ObBatchRows *right_brs)
{
  int ret = OB_SUCCESS;
  if (!MY_SPEC.group_rescan_) {
    ret = right_->get_next_batch(op_max_batch_size_, right_brs);
  } else {
    ret = group_join_buffer_.get_next_batch_from_right(op_max_batch_size_, right_brs);
  }
  return ret;
}

int ObNestedLoopJoinOp::get_next_row_from_right()
{
  int ret = OB_SUCCESS;
  if (!MY_SPEC.group_rescan_) {
    ret = right_->get_next_row();
  } else {
    ret = group_join_buffer_.get_next_row_from_right();
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase
