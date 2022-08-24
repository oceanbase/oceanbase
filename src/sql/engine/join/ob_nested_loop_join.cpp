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

#include "sql/engine/join/ob_nested_loop_join.h"

#include "lib/utility/ob_tracepoint.h"
#include "common/rowkey/ob_rowkey.h"
#include "sql/engine/table/ob_table_scan.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase {
using namespace common;
namespace sql {

void ObNestedLoopJoin::ObNestedLoopJoinCtx::reset()
{
  state_ = JS_READ_LEFT;
  batch_join_ctx_.left_rows_.reset();
  batch_join_ctx_.left_rows_iter_.set_invalid();
  is_left_end_ = false;
}

ObNestedLoopJoin::ObNestedLoopJoin(ObIAllocator& alloc) : ObBasicNestedLoopJoin(alloc)
{
  state_operation_func_[JS_JOIN_END] = &ObNestedLoopJoin::join_end_operate;
  state_function_func_[JS_JOIN_END][FT_ITER_GOING] = NULL;
  state_function_func_[JS_JOIN_END][FT_ITER_END] = &ObNestedLoopJoin::join_end_func_end;

  state_operation_func_[JS_READ_LEFT] = &ObNestedLoopJoin::read_left_operate;
  state_function_func_[JS_READ_LEFT][FT_ITER_GOING] = &ObNestedLoopJoin::read_left_func_going;
  state_function_func_[JS_READ_LEFT][FT_ITER_END] = &ObNestedLoopJoin::read_left_func_end;

  state_operation_func_[JS_READ_RIGHT] = &ObNestedLoopJoin::read_right_operate;
  state_function_func_[JS_READ_RIGHT][FT_ITER_GOING] = &ObNestedLoopJoin::read_right_func_going;
  state_function_func_[JS_READ_RIGHT][FT_ITER_END] = &ObNestedLoopJoin::read_right_func_end;

  set_mem_limit(DEFAULT_MEM_LIMIT);
  set_cache_limit(DEFAULT_CACHE_LIMIT);
  use_group_ = false;
}

ObNestedLoopJoin::~ObNestedLoopJoin()
{}

void ObNestedLoopJoin::reset()
{
  ObBasicNestedLoopJoin::reset();
}

void ObNestedLoopJoin::reuse()
{
  ObBasicNestedLoopJoin::reuse();
}

int ObNestedLoopJoin::inner_open(ObExecContext& exec_ctx) const
{
  int ret = OB_SUCCESS;

  ObNestedLoopJoinCtx* join_ctx = NULL;
  if (OB_ISNULL(my_phy_plan_)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("my phy plan is null", K(ret));
  } else if (OB_FAIL(OB_I(t1) ObBasicNestedLoopJoin::inner_open(exec_ctx))) {
    LOG_WARN("failed to open in base class", K(ret));
  } else if (OB_I(t5) OB_ISNULL(join_ctx = GET_PHY_OPERATOR_CTX(ObNestedLoopJoinCtx, exec_ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get nested loop join ctx", K(ret));
  } else if (OB_FAIL(wrap_expr_ctx(exec_ctx, join_ctx->expr_ctx_))) {
    LOG_WARN("fail to wrap expr ctx", K(exec_ctx), K(ret));
  }

  if (OB_SUCC(ret) && use_group()) {
    ObTableScan* right_table_scan = NULL;
    if (OB_ISNULL(right_op_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL right op", K(ret));
    } else if (PHY_TABLE_SCAN != right_op_->get_type()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Wrong right op", K(ret), "op_type", ob_phy_operator_type_str(right_op_->get_type()));
    } else if (OB_ISNULL(right_table_scan = static_cast<ObTableScan*>(right_op_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL right table scan", K(ret));
    } else if (FALSE_IT(right_table_scan->set_batch_scan_flag(true))) {
    }
  }
  return ret;
}

int ObNestedLoopJoin::switch_iterator(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPhyOperator* left_child = NULL;
  ObNestedLoopJoinCtx* join_ctx = GET_PHY_OPERATOR_CTX(ObNestedLoopJoinCtx, ctx, get_id());
  // switch bind array not support swtiching right table now
  if (OB_ISNULL(join_ctx)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("join ctx is null", K(ret));
  } else if (OB_ISNULL(left_child = get_child(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("left child is null", K(ret));
  } else if (OB_FAIL(left_child->switch_iterator(ctx))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("switch left child iterator failed", K(ret));
    }
  } else {
    join_ctx->reset();
  }
  return ret;
}

int ObNestedLoopJoin::reset_rescan_params(ObExecContext &ctx) const {
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = NULL;
  if (OB_ISNULL(plan_ctx = GET_PHY_PLAN_CTX(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("physical plan context is null", K(ret));
  } else {
    for (int64_t i = 0; i < rescan_params_.count(); ++i) {
      int64_t idx = rescan_params_.at(i).param_idx_;
      plan_ctx->get_param_store_for_update().at(idx).set_null();
      LOG_TRACE("prepare_rescan_params", K(ret), K(i), K(idx));
    }
  }

  return ret;
}

int ObNestedLoopJoin::rescan(ObExecContext &exec_ctx) const {
  int ret = OB_SUCCESS;
  ObNestedLoopJoinCtx *join_ctx =
      GET_PHY_OPERATOR_CTX(ObNestedLoopJoinCtx, exec_ctx, get_id());
  if (OB_ISNULL(join_ctx)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("join ctx is null", K(ret));
  } else {
    join_ctx->reset();
    if (OB_FAIL(reset_rescan_params(exec_ctx))) {
      LOG_WARN("fail to reset rescan params", K(ret));
    } else if (OB_FAIL(ObBasicNestedLoopJoin::rescan(exec_ctx))) {
      LOG_WARN("failed to rescan", K(ret));
    }
  }
  return ret;
}

int ObNestedLoopJoin::inner_get_next_row(ObExecContext& exec_ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ObNestedLoopJoinCtx* join_ctx = NULL;
  if (OB_I(t1) OB_ISNULL(join_ctx = GET_PHY_OPERATOR_CTX(ObNestedLoopJoinCtx, exec_ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get nested loop join ctx", K(ret));
  } else if (OB_ISNULL(right_op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("right_op is null", K(ret), K(right_op_));
  } else if (OB_UNLIKELY(!use_group() && use_batch_index_join(right_op_->get_type()))) {
    if (OB_FAIL(batch_index_join_get_next(exec_ctx, row))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("batch index join get next failed", K(ret));
      }
    }
  } else {
    if (OB_UNLIKELY(LEFT_SEMI_JOIN == join_type_ || LEFT_ANTI_JOIN == join_type_)) {
      if (OB_FAIL(join_row_with_semi_join(exec_ctx, row))) {
        if (ret != OB_ITER_END) {
          LOG_WARN("failed to join row with semi join", K(ret));
        }
      }
    } else {
      state_operation_func_type state_operation = NULL;
      state_function_func_type state_function = NULL;
      int func = -1;
      row = NULL;
      ObJoinState& state = join_ctx->state_;
      while (OB_SUCC(ret) && NULL == row) {
        state_operation = this->ObNestedLoopJoin::state_operation_func_[state];
        if (OB_ISNULL(state_operation)) {
          ret = OB_BAD_NULL_ERROR;
          LOG_WARN("state operation is null ", K(ret));
        } else if (OB_ITER_END == (ret = OB_I(t3)(this->*state_operation)(*join_ctx))) {
          func = FT_ITER_END;
          ret = OB_SUCCESS;
        } else if (OB_FAIL(ret)) {
          LOG_WARN("failed state operation", K(ret), K(state));
        } else {
          func = FT_ITER_GOING;
        }
        if (OB_SUCC(ret)) {
          state_function = this->ObNestedLoopJoin::state_function_func_[state][func];
          if (OB_ISNULL(state_function)) {
            ret = OB_BAD_NULL_ERROR;
            LOG_WARN("state operation is null ", K(ret));
          } else if (OB_FAIL(OB_I(t5)(this->*state_function)(*join_ctx, row)) && OB_ITER_END != ret) {
            LOG_WARN("failed state function", K(ret), K(state), K(func));
          }
        }
      }  // while
    }
  }
  if (OB_SUCC(ret) && (LEFT_SEMI_JOIN == join_type_ || LEFT_ANTI_JOIN == join_type_)) {
    if (OB_FAIL(copy_cur_row(*join_ctx, row))) {
      LOG_WARN("copy current row failed", K(ret));
    }
  }
  if (OB_ITER_END == ret) {
    if (OB_FAIL(reset_rescan_params(exec_ctx))) {
      LOG_WARN("fail to reset rescan params", K(ret));
    } else {
      ret = OB_ITER_END;
    }
  }

  if (OB_SUCC(ret)) {
    LOG_TRACE("get next row from nested loop join", K(*row));
  }
  return ret;
}

int ObNestedLoopJoin::inner_create_operator_ctx(ObExecContext& exec_ctx, ObPhyOperatorCtx*& op_ctx) const
{
  int ret = OB_SUCCESS;
  ObNestedLoopJoinCtx* join_ctx = NULL;
  if (OB_FAIL(OB_I(t1) CREATE_PHY_OPERATOR_CTX(ObNestedLoopJoinCtx, exec_ctx, get_id(), get_type(), join_ctx))) {
    LOG_WARN("failed to create nested loop join ctx", K(ret));
  } else {
    op_ctx = join_ctx;
  }
  return ret;
}

int ObNestedLoopJoin::join_end_operate(ObNestedLoopJoinCtx& join_ctx) const
{
  UNUSED(join_ctx);
  return OB_ITER_END;
}

int ObNestedLoopJoin::join_end_func_end(ObNestedLoopJoinCtx& join_ctx, const ObNewRow*& row) const
{
  UNUSED(join_ctx);
  UNUSED(row);
  return OB_ITER_END;
}

int ObNestedLoopJoin::read_left_operate(ObNestedLoopJoinCtx& join_ctx) const
{
  int ret = OB_SUCCESS;
  if (use_group()) {
    if (OB_FAIL(group_read_left_operate(join_ctx)) && OB_ITER_END != ret) {
      LOG_WARN("failed to read left group", K(ret));
    }
  } else if (OB_FAIL(OB_I(t1) get_next_left_row(join_ctx)) && OB_ITER_END != ret) {
    LOG_WARN("failed to get next left row", K(ret));
  }
  return ret;
}

int ObNestedLoopJoin::group_read_left_operate(ObNestedLoopJoinCtx& join_ctx) const
{
  int ret = OB_SUCCESS;
  if (join_ctx.batch_join_ctx_.left_rows_iter_.is_valid() && join_ctx.batch_join_ctx_.left_rows_iter_.has_next()) {
    if (OB_FAIL(OB_I(t1) group_rescan_right(join_ctx))) {
      if (OB_ITER_END == ret) {
        ret = OB_ERR_UNEXPECTED;
      }
      LOG_WARN("failed to get next right row from group", K(ret));
    }
  } else {
    ObTableScan* right_table_scan = NULL;
    if (OB_ISNULL(right_op_) || PHY_TABLE_SCAN != right_op_->get_type()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Wrong right op", K(ret), K(right_op_));
    } else if (OB_ISNULL(right_table_scan = static_cast<ObTableScan*>(right_op_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL right table scan", K(ret));
    } else if (OB_FAIL(right_table_scan->group_rescan_init(join_ctx.exec_ctx_, get_cache_limit()))) {
      LOG_WARN("Failed to init group rescan", K(ret));
    } else if (join_ctx.is_left_end_) {
      ret = OB_ITER_END;
    } else {
      join_ctx.batch_join_ctx_.left_rows_.reuse();
      join_ctx.batch_join_ctx_.left_rows_iter_.reset();
      const ObRowStore::StoredRow* stored_row = NULL;

      bool ignore_end = false;
      while (OB_SUCC(ret) && !is_full(join_ctx.batch_join_ctx_.left_rows_)) {
        if (OB_FAIL(OB_I(t1) get_next_left_row(join_ctx))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("failed to get next left row", K(ret));
          } else {
            join_ctx.is_left_end_ = true;
          }
        } else if (OB_ISNULL(join_ctx.left_row_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL left row", K(ret));
        } else if (OB_FAIL(join_ctx.batch_join_ctx_.left_rows_.add_row(*join_ctx.left_row_, stored_row, 0, false))) {
          LOG_WARN("failed to store left row", K(ret));
        } else if (OB_ISNULL(stored_row)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("stored row is null", K(ret));
        } else if (FALSE_IT(join_ctx.left_row_ = static_cast<const ObNewRow*>(stored_row->get_compact_row_ptr()))) {
          // do nothing
        } else if (OB_FAIL(OB_I(t1) prepare_rescan_params_for_group(join_ctx))) {
          LOG_WARN("failed to prepare rescan params", K(ret));
        } else if (OB_FAIL(right_table_scan->group_add_query_range(join_ctx.exec_ctx_))) {
          LOG_WARN("failed to rescan right op", K(ret));
        } else {
          ignore_end = true;
        }
      }

      if (OB_SUCC(ret) || (ignore_end && OB_ITER_END == ret)) {
        ret = OB_SUCCESS;
        join_ctx.batch_join_ctx_.left_rows_iter_ = join_ctx.batch_join_ctx_.left_rows_.begin();
        if (OB_FAIL(OB_I(t3) right_table_scan->group_rescan(join_ctx.exec_ctx_))) {
          LOG_WARN("failed to rescan right op", K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    ObNewRow* tmp_left_row = NULL;
    if (OB_FAIL(join_ctx.batch_join_ctx_.left_rows_iter_.get_next_row(tmp_left_row, NULL))) {
      LOG_WARN("Failed to get next row", K(ret));
    } else if (OB_ISNULL(tmp_left_row)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL next row", K(ret));
    } else {
      join_ctx.left_row_joined_ = false;
      tmp_left_row->projector_ = const_cast<int32_t*>(left_op_->get_projector());
      tmp_left_row->projector_size_ = left_op_->get_projector_size();
      join_ctx.left_row_ = tmp_left_row;
      LOG_DEBUG("Success to get cached next left row, ", K(*join_ctx.left_row_));
    }
  }
  return ret;
}

int ObNestedLoopJoin::group_rescan_right(ObNestedLoopJoinCtx& join_ctx) const
{
  int ret = OB_SUCCESS;
  ObTableScan* right_table_scan = NULL;
  ObTableScan::ObTableScanCtx* scan_ctx = NULL;
  if (OB_ISNULL(right_op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL right_op_", K(ret));
  } else if (OB_ISNULL(right_table_scan = static_cast<ObTableScan*>(right_op_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL right_scan op", K(ret));
  } else if (OB_ISNULL(scan_ctx = GET_PHY_OPERATOR_CTX(
                           ObTableScan::ObTableScanCtx, join_ctx.exec_ctx_, right_table_scan->get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get right op ctx", K(ret));
  } else if (OB_ISNULL(scan_ctx->result_iters_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("scan_ctx->result_iter_ is NULL", K(ret));
  } else if (OB_FAIL(scan_ctx->result_iters_->get_next_iter(scan_ctx->result_))) {
    LOG_WARN("failed to get next iter", K(ret));
  } else {
    scan_ctx->iter_end_ = false;
  }
  return ret;
}

int ObNestedLoopJoin::read_left_func_going(ObNestedLoopJoinCtx& join_ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ObExecContext& exec_ctx = join_ctx.exec_ctx_;
  if (OB_ISNULL(right_op_)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("right op is null ", K(ret));
  } else if (OB_FAIL(save_left_row(join_ctx))) {
    LOG_WARN("failed to save left row", K(ret));
  } else if (use_group()) {
    // do nothing
  } else if (!rescan_params_.empty()) {
    ObExecContext::ObPlanRestartGuard restart_plan(exec_ctx);
    if (OB_FAIL(OB_I(t1) prepare_rescan_params(join_ctx))) {
      LOG_WARN("failed to prepare rescan params", K(ret));
    } else if (OB_FAIL(OB_I(t3) right_op_->rescan(exec_ctx))) {
      LOG_WARN("failed to rescan right op", K(ret));
    } else { /*do nothing*/
    }
  } else {
    if (OB_FAIL(OB_I(t1) prepare_rescan_params(join_ctx))) {
      LOG_WARN("failed to prepare rescan params", K(ret));
    } else if (OB_FAIL(OB_I(t3) right_op_->rescan(exec_ctx))) {
      LOG_WARN("failed to rescan right op", K(ret));
    } else { /*do nothing*/
    }
  }
  join_ctx.state_ = JS_READ_RIGHT;
  UNUSED(row);
  return ret;
}

int ObNestedLoopJoin::read_left_func_end(ObNestedLoopJoinCtx& join_ctx, const ObNewRow*& row) const
{
  join_ctx.state_ = JS_JOIN_END;
  UNUSED(row);
  return OB_ITER_END;
}

int ObNestedLoopJoin::read_right_operate(ObNestedLoopJoinCtx& join_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_last_left_row(join_ctx))) {
    LOG_WARN("failed to get last left row", K(ret));
  } else if (OB_FAIL(OB_I(t1) get_next_right_row(join_ctx)) && OB_ITER_END != ret) {
    LOG_WARN("failed to get next right row", K(ret));
  } else {
  }
  if (NULL != join_ctx.right_row_) {
    LOG_DEBUG("get right row", K(ret), K(*join_ctx.right_row_));
  } else {
    LOG_DEBUG("null right row", K(ret), K(join_ctx.right_row_));
  }
  return ret;
}

int ObNestedLoopJoin::read_right_func_going(ObNestedLoopJoinCtx& join_ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  bool is_match = false;
  if (OB_FAIL(save_left_row(join_ctx))) {
    LOG_WARN("failed to save left row", K(ret));
  } else if (OB_ISNULL(join_ctx.left_row_) || OB_ISNULL(join_ctx.right_row_)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("left row or right row is null", K(ret));
  } else if (OB_FAIL(OB_I(t3) calc_other_conds(join_ctx, is_match))) {
    LOG_WARN("failed to compare left and right row on other join conds", K(ret));
  } else if (is_match) {
    if (OB_FAIL(OB_I(t5) join_rows(join_ctx, row))) {
      LOG_WARN("failed to join rows", K(ret));
    } else {
      join_ctx.left_row_joined_ = true;  // left row joined sign.
    }
    LOG_DEBUG("nested loop join match", K(is_match), K(*row));
  } else {
  }
  return ret;
}

int ObNestedLoopJoin::read_right_func_end(ObNestedLoopJoinCtx& join_ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  if (need_left_join() && !join_ctx.left_row_joined_) {
    if (OB_FAIL(OB_I(t1) left_join_rows(join_ctx, row))) {
      LOG_WARN("failed to left join rows", K(ret));
    }
  }
  join_ctx.state_ = JS_READ_LEFT;
  return ret;
}

int ObNestedLoopJoin::prepare_rescan_params_for_group(ObNestedLoopJoinCtx& join_ctx) const
{
  int ret = OB_SUCCESS;
  ObObjParam res_obj;
  ObPhysicalPlanCtx* plan_ctx = join_ctx.exec_ctx_.get_physical_plan_ctx();
  if (OB_ISNULL(plan_ctx) || OB_ISNULL(join_ctx.left_row_)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("plan ctx or left row is null", K(ret));
  } else {
    int64_t param_cnt = rescan_params_.count();
    const ObSqlExpression* expr = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < param_cnt; ++i) {
      if (OB_ISNULL(expr = rescan_params_.at(i).expr_)) {
        ret = OB_BAD_NULL_ERROR;
        LOG_WARN("rescan param expr is null", K(ret), K(i));
      } else if (OB_FAIL(expr->calc(join_ctx.expr_ctx_, *join_ctx.left_row_, res_obj))) {
        LOG_WARN("failed to calc expr for rescan param", K(ret), K(i));
      } else {
        int64_t idx = rescan_params_.at(i).param_idx_;
        plan_ctx->get_param_store_for_update().at(idx) = res_obj;
        LOG_DEBUG("prepare_rescan_params", K(ret), K(i), K(res_obj), K(idx), K(expr), K(*join_ctx.left_row_));
      }
    }
  }
  return ret;
}

int ObNestedLoopJoin::join_row_with_semi_join(ObExecContext& ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ObNestedLoopJoinCtx* join_ctx = GET_PHY_OPERATOR_CTX(ObNestedLoopJoinCtx, ctx, get_id());
  if (OB_ISNULL(join_ctx) || OB_ISNULL(left_op_) || OB_ISNULL(right_op_)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("join ctx or left / right op is null", K(ret));
  } else if (OB_UNLIKELY(LEFT_SEMI_JOIN != join_type_ && LEFT_ANTI_JOIN != join_type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("join type should be left semi join or left anti semi join", K(ret), K_(join_type));
  } else {
    const bool is_anti = (LEFT_ANTI_JOIN == join_type_);
    while (OB_SUCC(ret) && OB_SUCC(left_op_->get_next_row(ctx, join_ctx->left_row_))) {
      if (OB_FAIL(try_check_status(ctx))) {
        LOG_WARN("check status failed", K(ret));
      } else if (!rescan_params_.empty()) {
        ObExecContext::ObPlanRestartGuard restart_plan(ctx);
        if (OB_FAIL(prepare_rescan_params(*join_ctx))) {
          LOG_WARN("prepare right child rescan param failed", K(ret));
        } else if (OB_FAIL(right_op_->rescan(ctx))) {
          LOG_WARN("rescan right child failed", K(ret));
        }
      } else {
        if (OB_FAIL(prepare_rescan_params(*join_ctx))) {
          LOG_WARN("prepare right child rescan param failed", K(ret));
        } else if (OB_FAIL(right_op_->rescan(ctx))) {
          LOG_WARN("rescan right child failed", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        bool is_matched = false;
        while (OB_SUCC(ret) && !is_matched && OB_SUCC(right_op_->get_next_row(ctx, join_ctx->right_row_))) {
          if (OB_FAIL(try_check_status(ctx))) {
            LOG_WARN("check status failed", K(ret));
          } else if (OB_FAIL(calc_other_conds(*join_ctx, is_matched))) {
            LOG_WARN("calc other conditions failed", K(ret));
          }
        }
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;  // There is no row matching the left table in the right table, so iterate over the next row
                             // in the left table
        }
        if (OB_SUCC(ret) && is_anti != is_matched) {
          // 1. the exit conditions of semi and anti semi are different,
          // 2. they share the same outer while loop,
          // 3. is_matched must init to false for inner while loop,
          // so we need explicitly break.
          break;
        }
      }  // else
    }    // while
    if (OB_FAIL(ret)) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get next row failed", K(ret));
      }
    } else {
      row = join_ctx->left_row_;
    }
  }
  return ret;
}

int ObNestedLoopJoin::batch_index_join_get_next(ObExecContext& exec_ctx, const common::ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ObNestedLoopJoinCtx* join_ctx = GET_PHY_OPERATOR_CTX(ObNestedLoopJoinCtx, exec_ctx, get_id());
  if (OB_ISNULL(join_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get nested loop join ctx", K(ret));
  } else {
    // LOG_INFO("YZF debug, left rows cache", "valid", join_ctx->batch_join_ctx_.left_rows_iter_.is_valid(),
    //          "has_next", join_ctx->batch_join_ctx_.left_rows_iter_.has_next());
    if (!join_ctx->batch_join_ctx_.left_rows_iter_.is_valid() ||
        !join_ctx->batch_join_ctx_.left_rows_iter_.has_next()) {
      ret = bij_fill_left_rows(exec_ctx);
    }
    if (OB_SUCC(ret)) {
      ret = bij_join_rows(exec_ctx, row);
    }
  }
  return ret;
}

int ObNestedLoopJoin::bij_fill_left_rows(ObExecContext& exec_ctx) const
{
  int ret = OB_SUCCESS;
  ObNestedLoopJoinCtx* join_ctx = GET_PHY_OPERATOR_CTX(ObNestedLoopJoinCtx, exec_ctx, get_id());
  int64_t BATCH_SIZE = 1000;
  if (OB_ISNULL(join_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get nested loop join ctx", K(ret));
  } else if (OB_ISNULL(right_op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("right_op is null", K(ret), K(right_op_));
  } else if (PHY_TABLE_SCAN != right_op_->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("right op is not TABLE_SCAN type, plan is incorrect", K(ret), K(right_op_->get_type()));
  } else {
    ObTableScan* right_table_scan = static_cast<ObTableScan*>(right_op_);
    if (OB_FAIL(right_table_scan->batch_rescan_init(exec_ctx))) {
      LOG_WARN("failed to init batch_rescan", K(ret));
    } else {
      // clear rows
      join_ctx->batch_join_ctx_.left_rows_.reuse();
      join_ctx->batch_join_ctx_.left_rows_iter_.reset();
    }
    const ObNewRow* tmp_left_row = NULL;
    const ObRowStore::StoredRow* stored_row = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < BATCH_SIZE; ++i) {
      // We have to deep copy the left row before calling batch_rescan_add_key,
      // otherwise the batch_scan's key ranges will reference the freed memory
      if (OB_FAIL(left_op_->get_next_row(exec_ctx, tmp_left_row))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to get the left row", K(ret));
        }
      } else if (OB_FAIL(join_ctx->batch_join_ctx_.left_rows_.add_row(*tmp_left_row, stored_row, 0, false))) {
        LOG_WARN("failed to store left row", K(ret));
      } else {
        join_ctx->left_row_ = static_cast<const ObNewRow*>(stored_row->get_compact_row_ptr());
        if (OB_FAIL(OB_I(t1) prepare_rescan_params(*join_ctx))) {
          LOG_WARN("failed to prepare rescan params", K(ret));
        } else if (OB_FAIL(right_table_scan->batch_rescan_add_key(exec_ctx))) {
          LOG_WARN("failed to rescan right op", K(ret));
        }
      }
    }  // end for

    if (OB_ITER_END == ret && 0 < join_ctx->batch_join_ctx_.left_rows_.get_row_count()) {
      ret = OB_SUCCESS;
    }

    if (OB_SUCC(ret) && 0 < join_ctx->batch_join_ctx_.left_rows_.get_row_count()) {
      if (OB_FAIL(right_table_scan->batch_rescan(exec_ctx))) {
        LOG_WARN("failed to batch_rescan", K(ret));
      } else {
        // init iterator
        join_ctx->batch_join_ctx_.left_rows_iter_ = join_ctx->batch_join_ctx_.left_rows_.begin();
        LOG_DEBUG("YZF debug, prepared rows for batch scan the right table",
            "row_count",
            join_ctx->batch_join_ctx_.left_rows_.get_row_count());
      }
    }
  }
  return ret;
}

int ObNestedLoopJoin::bij_join_rows(ObExecContext& exec_ctx, const common::ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ObNestedLoopJoinCtx* join_ctx = GET_PHY_OPERATOR_CTX(ObNestedLoopJoinCtx, exec_ctx, get_id());
  ObNewRow* left_row = NULL;
  row = NULL;
  while (OB_SUCC(ret) && NULL == row) {
    if (OB_FAIL(try_check_status(exec_ctx))) {
      LOG_WARN("check status failed", K(ret));
    } else if (OB_FAIL(join_ctx->batch_join_ctx_.left_rows_iter_.get_next_row(left_row, NULL))) {
      if (OB_ITER_END == ret) {
        int tmp_ret = right_op_->get_next_row(exec_ctx, join_ctx->right_row_);
        if (OB_ITER_END != tmp_ret) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("left cache and the right op should have the same count of rows",
              K(ret),
              K(tmp_ret),
              "right_row",
              *join_ctx->right_row_);
        }
      }
      LOG_WARN("failed to get left row", K(ret));
    } else if (OB_FAIL(right_op_->get_next_row(exec_ctx, join_ctx->right_row_))) {
      LOG_WARN("failed to get right row", K(ret));
    } else if (OB_ISNULL(left_row)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("left_row is null");
    } else {
      left_row->projector_ = const_cast<int32_t*>(left_op_->get_projector());
      left_row->projector_size_ = left_op_->get_projector_size();
      join_ctx->left_row_ = left_row;
      join_ctx->last_left_row_ = NULL;
      bool is_matched = false;
      if (LEFT_SEMI_JOIN == join_type_) {
        if (OB_FAIL(calc_other_conds(*join_ctx, is_matched))) {
          LOG_WARN("calc other conditions failed", K(ret));
        } else if (is_matched) {
          row = join_ctx->left_row_;
        }
      } else if (LEFT_ANTI_JOIN == join_type_) {
        if (OB_FAIL(calc_other_conds(*join_ctx, is_matched))) {
          LOG_WARN("calc other conditions failed", K(ret));
        } else if (!is_matched) {
          row = join_ctx->left_row_;
        }
      } else if (INNER_JOIN == join_type_) {
        ret = read_right_func_going(*join_ctx, row);
        // LOG_INFO("YZF debug, join rows", K(ret), K(row), K(*join_ctx->left_row_), K(*join_ctx->right_row_));
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("nested-loop-join with batch index seek does not support this join type", K(ret), K_(join_type));
      }
    }
  }
  return ret;
}

bool ObNestedLoopJoin::is_full(ObRowStore& rs) const
{
  return rs.get_data_size() >= get_mem_limit() || rs.get_row_count() >= get_cache_limit();
}

OB_SERIALIZE_MEMBER((ObNestedLoopJoin, ObBasicNestedLoopJoin), use_group_, mem_limit_);
}  // namespace sql
}  // namespace oceanbase
