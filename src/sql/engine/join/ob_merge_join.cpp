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

#include "sql/engine/join/ob_merge_join.h"

#include "lib/utility/utility.h"
#include "lib/utility/ob_tracepoint.h"
#include "common/row/ob_row_util.h"
#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/session/ob_sql_session_info.h"
namespace oceanbase {
using namespace common;
namespace sql {

// REGISTER_PHY_OPERATOR(ObMergeJoin, PHY_MERGE_JOIN);

ObMergeJoin::ObMergeJoinCtx::ObMergeJoinCtx(ObExecContext& ctx)
    : ObJoinCtx(ctx),
      state_(JS_JOIN_BEGIN),
      right_cache_row_buf_(),
      stored_row_(NULL),
      right_cache_(),
      right_cache_iter_(),
      right_iter_end_(false),
      equal_cmp_(0),
      left_row_matched_(false)
{}

ObMergeJoin::ObMergeJoin(ObIAllocator& alloc) : ObJoin(alloc), merge_directions_(alloc), is_left_unique_(false)
{
  state_operation_func_[JS_JOIN_END] = &ObMergeJoin::join_end_operate;
  state_function_func_[JS_JOIN_END][FT_ROWS_EQUAL] = NULL;
  state_function_func_[JS_JOIN_END][FT_ROWS_DIFF] = NULL;
  state_function_func_[JS_JOIN_END][FT_ITER_GOING] = NULL;
  state_function_func_[JS_JOIN_END][FT_ITER_END] = &ObMergeJoin::join_end_func_end;

  state_operation_func_[JS_JOIN_BEGIN] = &ObMergeJoin::join_begin_operate;
  state_function_func_[JS_JOIN_BEGIN][FT_ROWS_EQUAL] = NULL;
  state_function_func_[JS_JOIN_BEGIN][FT_ROWS_DIFF] = NULL;
  state_function_func_[JS_JOIN_BEGIN][FT_ITER_GOING] = &ObMergeJoin::join_begin_func_going;
  state_function_func_[JS_JOIN_BEGIN][FT_ITER_END] = &ObMergeJoin::join_begin_func_end;

  state_operation_func_[JS_LEFT_JOIN] = &ObMergeJoin::left_join_operate;
  state_function_func_[JS_LEFT_JOIN][FT_ROWS_EQUAL] = NULL;
  state_function_func_[JS_LEFT_JOIN][FT_ROWS_DIFF] = NULL;
  state_function_func_[JS_LEFT_JOIN][FT_ITER_GOING] = &ObMergeJoin::left_join_func_going;
  state_function_func_[JS_LEFT_JOIN][FT_ITER_END] = &ObMergeJoin::left_join_func_end;

  state_operation_func_[JS_RIGHT_JOIN_CACHE] = &ObMergeJoin::right_join_cache_operate;
  state_function_func_[JS_RIGHT_JOIN_CACHE][FT_ROWS_EQUAL] = NULL;
  state_function_func_[JS_RIGHT_JOIN_CACHE][FT_ROWS_DIFF] = NULL;
  state_function_func_[JS_RIGHT_JOIN_CACHE][FT_ITER_GOING] = &ObMergeJoin::right_join_cache_func_going;
  state_function_func_[JS_RIGHT_JOIN_CACHE][FT_ITER_END] = &ObMergeJoin::right_join_cache_func_end;

  state_operation_func_[JS_RIGHT_JOIN] = &ObMergeJoin::right_join_operate;
  state_function_func_[JS_RIGHT_JOIN][FT_ROWS_EQUAL] = NULL;
  state_function_func_[JS_RIGHT_JOIN][FT_ROWS_DIFF] = NULL;
  state_function_func_[JS_RIGHT_JOIN][FT_ITER_GOING] = &ObMergeJoin::right_join_func_going;
  state_function_func_[JS_RIGHT_JOIN][FT_ITER_END] = &ObMergeJoin::right_join_func_end;

  state_operation_func_[JS_GOING_END_ONLY] = NULL;
  state_function_func_[JS_GOING_END_ONLY][FT_ROWS_EQUAL] = NULL;
  state_function_func_[JS_GOING_END_ONLY][FT_ROWS_DIFF] = NULL;
  state_function_func_[JS_GOING_END_ONLY][FT_ITER_GOING] = NULL;
  state_function_func_[JS_GOING_END_ONLY][FT_ITER_END] = NULL;

  state_operation_func_[JS_EMPTY_CACHE] = &ObMergeJoin::empty_cache_operate;
  state_function_func_[JS_EMPTY_CACHE][FT_ROWS_EQUAL] = &ObMergeJoin::empty_cache_func_equal;
  state_function_func_[JS_EMPTY_CACHE][FT_ROWS_DIFF] = &ObMergeJoin::empty_cache_func_diff;
  state_function_func_[JS_EMPTY_CACHE][FT_ITER_GOING] = NULL;
  state_function_func_[JS_EMPTY_CACHE][FT_ITER_END] = &ObMergeJoin::empty_cache_func_end;

  state_operation_func_[JS_FILL_CACHE] = &ObMergeJoin::fill_cache_operate;
  state_function_func_[JS_FILL_CACHE][FT_ROWS_EQUAL] = &ObMergeJoin::fill_cache_func_equal;
  state_function_func_[JS_FILL_CACHE][FT_ROWS_DIFF] = &ObMergeJoin::fill_cache_func_diff_end;
  state_function_func_[JS_FILL_CACHE][FT_ITER_GOING] = NULL;
  state_function_func_[JS_FILL_CACHE][FT_ITER_END] = &ObMergeJoin::fill_cache_func_diff_end;

  state_operation_func_[JS_FULL_CACHE] = &ObMergeJoin::full_cache_operate;
  state_function_func_[JS_FULL_CACHE][FT_ROWS_EQUAL] = &ObMergeJoin::full_cache_func_equal;
  state_function_func_[JS_FULL_CACHE][FT_ROWS_DIFF] = &ObMergeJoin::full_cache_func_diff;
  state_function_func_[JS_FULL_CACHE][FT_ITER_GOING] = NULL;
  state_function_func_[JS_FULL_CACHE][FT_ITER_END] = &ObMergeJoin::full_cache_func_end;

  state_operation_func_[JS_READ_CACHE] = &ObMergeJoin::read_cache_operate;
  state_function_func_[JS_READ_CACHE][FT_ROWS_EQUAL] = NULL;
  state_function_func_[JS_READ_CACHE][FT_ROWS_DIFF] = NULL;
  state_function_func_[JS_READ_CACHE][FT_ITER_GOING] = &ObMergeJoin::read_cache_func_going;
  state_function_func_[JS_READ_CACHE][FT_ITER_END] = &ObMergeJoin::read_cache_func_end;
}

const int64_t ObMergeJoin::MERGE_DIRECTION_ASC = 1;
const int64_t ObMergeJoin::MERGE_DIRECTION_DESC = -1;

ObMergeJoin::~ObMergeJoin()
{
  reset();
}

void ObMergeJoin::reset()
{
  is_left_unique_ = false;
  merge_directions_.reset();
  ObJoin::reset();
}

void ObMergeJoin::reuse()
{
  is_left_unique_ = false;
  merge_directions_.reuse();
  ObJoin::reuse();
}

int ObMergeJoin::inner_open(ObExecContext& exec_ctx) const
{
  int ret = OB_SUCCESS;
  ObMergeJoinCtx* join_ctx = NULL;
  ObSQLSessionInfo* session = NULL;
  if ((OB_I(t1) OB_UNLIKELY(equal_join_conds_.get_size()) <= 0) || OB_ISNULL(right_op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no equal join conds or right op is null", K(ret));
  } else if (OB_FAIL(OB_I(t3) ObJoin::inner_open(exec_ctx))) {
    LOG_WARN("failed to open in base class", K(ret));
  } else if (OB_I(t7) OB_ISNULL(join_ctx = GET_PHY_OPERATOR_CTX(ObMergeJoinCtx, exec_ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get merge join ctx", K(ret));
  } else if (OB_ISNULL(session = exec_ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get my session", K(ret));
  } else if (OB_FAIL(
                 OB_I(t9) join_ctx->alloc_row_cells(right_op_->get_column_count(), join_ctx->right_cache_row_buf_))) {
    LOG_WARN("failed to create right cache row buf", K(ret));
  } else {
    join_ctx->set_tenant_id(session->get_effective_tenant_id());
    join_ctx->right_cache_row_buf_.projector_ = const_cast<int32_t*>(right_op_->get_projector());
    join_ctx->right_cache_row_buf_.projector_size_ = right_op_->get_projector_size();
  }
  return ret;
}

int ObMergeJoin::rescan(ObExecContext& exec_ctx) const
{
  int ret = OB_SUCCESS;
  ObMergeJoinCtx* join_ctx = GET_PHY_OPERATOR_CTX(ObMergeJoinCtx, exec_ctx, get_id());
  if (OB_ISNULL(join_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get merge join ctx", K(ret));
  } else {
    join_ctx->reset();
    if (OB_FAIL(ObJoin::rescan(exec_ctx))) {
      LOG_WARN("failed to rescan ObJoin", K(ret));
    }
  }
  return ret;
}

int ObMergeJoin::switch_iterator(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObMergeJoinCtx* join_ctx = GET_PHY_OPERATOR_CTX(ObMergeJoinCtx, ctx, get_id());
  if (OB_ISNULL(join_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get merge join ctx", K(ret));
  } else {
    join_ctx->reset();
    if (OB_FAIL(ObJoin::switch_iterator(ctx))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to rescan ObJoin", K(ret));
      }
    }
  }
  return ret;
}

int ObMergeJoin::inner_close(ObExecContext& exec_ctx) const
{
  UNUSED(exec_ctx);
  int ret = OB_SUCCESS;
  return ret;
}

int ObMergeJoin::inner_get_next_row(ObExecContext& exec_ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ObMergeJoinCtx* join_ctx = NULL;
  state_operation_func_type state_operation = NULL;
  state_function_func_type state_function = NULL;
  int func = -1;
  row = NULL;
  if (OB_I(t1)(OB_ISNULL(join_ctx = GET_PHY_OPERATOR_CTX(ObMergeJoinCtx, exec_ctx, get_id())))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get merge join ctx", K(ret));
  } else {
    ObJoinState& state = join_ctx->state_;
    while (OB_SUCC(ret) && NULL == row) {
      state_operation = this->ObMergeJoin::state_operation_func_[state];
      if (OB_ISNULL(state_operation)) {
        ret = OB_BAD_NULL_ERROR;
        LOG_WARN("state operation is null ", K(ret));
      } else if (OB_ITER_END == (ret = OB_I(t3)(this->*state_operation)(*join_ctx))) {
        func = FT_ITER_END;
        ret = OB_SUCCESS;
      } else if (OB_FAIL(ret)) {
        LOG_WARN("failed to exec state operation", K(ret), K(state));
      } else if (state < JS_GOING_END_ONLY) {  // see comment for JS_GOING_END_ONLY.
        func = FT_ITER_GOING;
      } else if (OB_FAIL(OB_I(t5) calc_equal_conds(*join_ctx, join_ctx->equal_cmp_, &merge_directions_))) {
        LOG_WARN("failed to calc equal conds", K(ret), K(state));
      } else if (0 == join_ctx->equal_cmp_) {
        func = FT_ROWS_EQUAL;
      } else {
        func = FT_ROWS_DIFF;
      }
      if (OB_SUCC(ret)) {
        state_function = this->ObMergeJoin::state_function_func_[state][func];
        if (OB_ISNULL(state_function)) {
          ret = OB_BAD_NULL_ERROR;
          LOG_WARN("state operation is null ", K(ret));
        } else if (OB_FAIL(OB_I(t7)(this->*state_function)(*join_ctx, row)) && OB_ITER_END != ret) {
          LOG_WARN("failed state function", K(ret), K(state), K(func));
        }
      }
    }  // while
  }
  if (OB_SUCC(ret) && (LEFT_SEMI_JOIN == join_type_ || LEFT_ANTI_JOIN == join_type_)) {
    if (OB_FAIL(copy_cur_row(*join_ctx, row))) {
      LOG_WARN("copy current row failed", K(ret));
    }
  }

  return ret;
}

int ObMergeJoin::join_end_operate(ObMergeJoinCtx& join_ctx) const
{
  UNUSED(join_ctx);
  return OB_ITER_END;
}

int ObMergeJoin::join_end_func_end(ObMergeJoinCtx& join_ctx, const ObNewRow*& row) const
{
  UNUSED(join_ctx);
  UNUSED(row);
  return OB_ITER_END;
}

int ObMergeJoin::join_begin_operate(ObMergeJoinCtx& join_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(OB_I(t1) get_next_left_row(join_ctx)) && OB_ITER_END != ret) {
    LOG_WARN("failed to get next left row", K(ret));
  }
  return ret;
}

int ObMergeJoin::join_begin_func_going(ObMergeJoinCtx& join_ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(save_left_row(join_ctx))) {
    LOG_WARN("failed to save left row", K(ret));
  } else {
    join_ctx.state_ = JS_EMPTY_CACHE;
  }
  UNUSED(row);
  return ret;
}

int ObMergeJoin::join_begin_func_end(ObMergeJoinCtx& join_ctx, const ObNewRow*& row) const
{
  if (need_right_join()) {
    join_ctx.state_ = JS_RIGHT_JOIN;
  } else {
    join_ctx.state_ = JS_JOIN_END;
  }
  UNUSED(row);
  return OB_SUCCESS;
}

int ObMergeJoin::left_join_operate(ObMergeJoinCtx& join_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(OB_I(t1) get_left_row(join_ctx)) && OB_ITER_END != ret) {
    LOG_WARN("failed to get left row", K(ret));
  }
  return ret;
}

int ObMergeJoin::left_join_func_going(ObMergeJoinCtx& join_ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  if (LEFT_ANTI_JOIN == join_type_) {
    row = join_ctx.left_row_;
    join_ctx.left_row_ = NULL;
  } else if (OB_FAIL(OB_I(t1) left_join_rows(join_ctx, row))) {
    LOG_WARN("failed to left join rows", K(ret));
  }
  return ret;
}

int ObMergeJoin::left_join_func_end(ObMergeJoinCtx& join_ctx, const ObNewRow*& row) const
{
  join_ctx.state_ = JS_JOIN_END;
  UNUSED(row);
  return OB_SUCCESS;
}

int ObMergeJoin::right_join_cache_operate(ObMergeJoinCtx& join_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(OB_I(t1) get_next_right_cache_row(join_ctx)) && OB_ITER_END != ret) {
    LOG_WARN("failed to get next right cache row", K(ret));
  }
  return ret;
}

int ObMergeJoin::right_join_cache_func_going(ObMergeJoinCtx& join_ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(join_ctx.stored_row_)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("stored row is null", K(ret));
  } else if (0 == join_ctx.stored_row_->payload_) {
    if (OB_FAIL(OB_I(t1) right_join_rows(join_ctx, row))) {
      LOG_WARN("failed to right join rows", K(ret));
    }
  } else {
  }
  return ret;
}

int ObMergeJoin::right_join_cache_func_end(ObMergeJoinCtx& join_ctx, const ObNewRow*& row) const
{
  join_ctx.right_cache_.reuse_hold_one_block();
  join_ctx.right_cache_iter_.reset();
  // When full cache diff, and right_iter_end is true,
  // handle data in right cache first, then if state is FULL_OUTER_JOIN,
  // hanlde left data in the end.
  if (join_ctx.right_iter_end_ && FULL_OUTER_JOIN == join_type_) {
    join_ctx.state_ = JS_LEFT_JOIN;
  } else if (NULL == join_ctx.last_left_row_) {
    join_ctx.state_ = JS_RIGHT_JOIN;
  } else {
    join_ctx.state_ = JS_EMPTY_CACHE;
  }
  UNUSED(row);
  return OB_SUCCESS;
}

int ObMergeJoin::right_join_operate(ObMergeJoinCtx& join_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(OB_I(t1) get_right_row(join_ctx)) && OB_ITER_END != ret) {
    LOG_WARN("failed to get right row", K(ret));
  }
  return ret;
}

int ObMergeJoin::right_join_func_going(ObMergeJoinCtx& join_ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(OB_I(t1) right_join_rows(join_ctx, row))) {
    LOG_WARN("failed to right join rows", K(ret));
  }
  return ret;
}

int ObMergeJoin::right_join_func_end(ObMergeJoinCtx& join_ctx, const ObNewRow*& row) const
{
  join_ctx.state_ = JS_JOIN_END;
  UNUSED(row);
  return OB_SUCCESS;
}

int ObMergeJoin::read_cache_operate(ObMergeJoinCtx& join_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_last_left_row(join_ctx))) {
    LOG_WARN("failed to get last left row", K(ret));
  } else if (OB_FAIL(OB_I(t1) get_next_right_cache_row(join_ctx)) && OB_ITER_END != ret) {
    LOG_WARN("failed to get next right cache row", K(ret));
  }
  return ret;
}

int ObMergeJoin::read_cache_func_going(ObMergeJoinCtx& join_ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(OB_I(t1) trans_to_read_cache(join_ctx, row))) {
    LOG_WARN("failed to state read cache", K(ret));
  }
  return ret;
}

int ObMergeJoin::read_cache_func_end(ObMergeJoinCtx& join_ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  if (LEFT_ANTI_JOIN == join_type_ && JS_READ_CACHE == join_ctx.state_) {
    if (!join_ctx.left_row_matched_) {
      row = join_ctx.left_row_;
      join_ctx.left_row_ = NULL;
    }
    join_ctx.left_row_matched_ = false;
  } else if (LEFT_SEMI_JOIN == join_type_ && JS_READ_CACHE == join_ctx.state_) {
    if (join_ctx.left_row_matched_) {
      row = join_ctx.left_row_;
      join_ctx.left_row_ = NULL;
    }
    join_ctx.left_row_matched_ = false;
  } else if (need_left_join() && !join_ctx.left_row_joined_) {
    if (OB_FAIL(OB_I(t1) left_join_rows(join_ctx, row))) {
      LOG_WARN("failed to left join rows", K(ret));
    }
  } else {
  }
  if (OB_SUCC(ret)) {
    // can't save right row cause it may have saved some right row now.
    // save_right_row(join_ctx);
    join_ctx.right_cache_iter_ = join_ctx.right_cache_.begin();
    join_ctx.state_ = JS_FULL_CACHE;
  }
  return ret;
}

int ObMergeJoin::full_cache_operate(ObMergeJoinCtx& join_ctx) const
{
  int ret = OB_SUCCESS;
  if (!OB_ISNULL(join_ctx.last_left_row_)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("last left row is null", K(ret));
  } else if (OB_FAIL(OB_I(t1) get_next_left_row(join_ctx))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get next left row", K(ret));
    } else {
    }
  } else if (OB_FAIL(OB_I(t3) get_next_right_cache_row(join_ctx))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get next right cache row", K(ret));
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("right cache should not be empty", K(ret));
    }
  }
  return ret;
}

int ObMergeJoin::full_cache_func_equal(ObMergeJoinCtx& join_ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(OB_I(t1) trans_to_read_cache(join_ctx, row))) {
    LOG_WARN("failed to state read cache", K(ret));
  }
  return ret;
}

int ObMergeJoin::full_cache_func_diff(ObMergeJoinCtx& join_ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  // exit if right table is end and join type is semi/inner join.
  if (join_ctx.right_iter_end_) {
    if (OB_FAIL(save_left_row(join_ctx))) {
      LOG_WARN("failed to save left row", K(ret));
    } else if (LEFT_OUTER_JOIN == join_type_ || LEFT_ANTI_JOIN == join_type_) {
      join_ctx.state_ = JS_LEFT_JOIN;
    } else if (need_right_join()) {
      // When full cache diff, and right_iter_end is true,
      // handle data in right cache first, then if state is FULL_OUTER_JOIN,
      // hanlde left data in the end.
      join_ctx.right_cache_iter_ = join_ctx.right_cache_.begin();
      join_ctx.state_ = JS_RIGHT_JOIN_CACHE;
    } else {
      join_ctx.state_ = JS_JOIN_END;
    }
  } else {
    if (OB_FAIL(save_left_row(join_ctx))) {
      LOG_WARN("failed to save left row", K(ret));
    } else if (need_right_join()) {
      join_ctx.right_cache_iter_ = join_ctx.right_cache_.begin();
      join_ctx.state_ = JS_RIGHT_JOIN_CACHE;
    } else {
      join_ctx.right_cache_.reuse_hold_one_block();
      join_ctx.state_ = JS_EMPTY_CACHE;
    }
  }
  UNUSED(row);
  return ret;
}

int ObMergeJoin::full_cache_func_end(ObMergeJoinCtx& join_ctx, const ObNewRow*& row) const
{
  if (need_right_join()) {
    join_ctx.right_cache_iter_ = join_ctx.right_cache_.begin();
    join_ctx.state_ = JS_RIGHT_JOIN_CACHE;
  } else {
    join_ctx.state_ = JS_JOIN_END;
  }
  UNUSED(row);
  return OB_SUCCESS;
}

int ObMergeJoin::empty_cache_operate(ObMergeJoinCtx& join_ctx) const
{
  // why we don't use equal_cmp_ to decide which last_xxxx_row_ and which child op we should
  // read?
  // see this state transfer process:
  // FILL_CACHE: cur right row doesn't equal to cur left row, so save it to last right row,
  //             then transfer to FULL_CACHE.
  // FULL_CACHE: cur left row doesn't equal to right cache row, so save it to last left row,
  //             then transfer to EMPTY_CACHE.
  // EMPTY_CACHE: now we have last left row and last right row that need to compare, so we can't
  //              read from either child op.
  // 1. if we use equal_cmp_ here, how to indicate that we use last left row and last right row
  //    instead of reading from child op? if < 0 means reading from left op and > 0 means right,
  //    only == 0 can say reading neither child op, but we think it's a bad idea, cause == 0
  //    usually means cur left and right row is equal, which situation should not happens in
  //    this state operation.
  // 2. settig equal_cmp_ to 0 is a easy-to-forget operation used on for the state transfer
  //    process above.
  // 3. saving cur left or right row to last left or right row when probably used in future
  //    state is a general principle, not only for the state transfer process above.
  // so we use last left or right row here instead of equal_cmp_.
  int ret = OB_SUCCESS;
  if (OB_ISNULL(join_ctx.last_left_row_) && OB_ISNULL(join_ctx.last_right_row_)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("last left row and last right row is null", K(ret));
  } else if (NULL != join_ctx.last_left_row_) {
    if (OB_FAIL(get_last_left_row(join_ctx))) {
      LOG_WARN("failed to get last left row", K(ret));
    } else if (OB_FAIL(OB_I(t1) get_right_row(join_ctx)) && OB_ITER_END != ret) {
      LOG_WARN("failed to get right row", K(ret));
    }
  } else if (NULL != join_ctx.last_right_row_) {
    if (OB_FAIL(get_last_right_row(join_ctx))) {
      LOG_WARN("failed to get last right row", K(ret));
    } else if (OB_FAIL(OB_I(t3) get_left_row(join_ctx)) && OB_ITER_END != ret) {
      LOG_WARN("failed to get left row", K(ret));
    }
  } else {
  }
  return ret;
}

int ObMergeJoin::empty_cache_func_equal(ObMergeJoinCtx& join_ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(OB_I(t1) trans_to_fill_cache(join_ctx, row))) {
    LOG_WARN("failed to fill cache", K(ret));
  }
  return ret;
}

int ObMergeJoin::empty_cache_func_diff(ObMergeJoinCtx& join_ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  // can't use left_row_ or right_row_, cause both are not NULL -_-!.
  // actually this is the only function must use equal_cmp_, other functions use last_xxx_row.
  if (join_ctx.equal_cmp_ < 0) {
    if (OB_FAIL(save_right_row(join_ctx))) {
      LOG_WARN("failed to save right row", K(ret));
    } else if (LEFT_ANTI_JOIN == join_type_) {
      row = join_ctx.left_row_;
      join_ctx.left_row_ = NULL;
    } else if (need_left_join()) {
      if (OB_FAIL(OB_I(t1) left_join_rows(join_ctx, row))) {
        LOG_WARN("failed to left join rows", K(ret));
      }
    } else {
    }
  } else if (join_ctx.equal_cmp_ > 0) {
    if (OB_FAIL(save_left_row(join_ctx))) {
      LOG_WARN("failed to save left row", K(ret));
    } else if (need_right_join()) {
      if (OB_FAIL(OB_I(t3) right_join_rows(join_ctx, row))) {
        LOG_WARN("failed to right join rows", K(ret));
      }
    } else {
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("compare result should NOT be equal", K(ret));
  }
  return ret;
}

int ObMergeJoin::empty_cache_func_end(ObMergeJoinCtx& join_ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  // can use equal_cmp_ also, but I prefer left_row_ or right_row_.
  if (NULL != join_ctx.left_row_) {
    if (need_left_join() || LEFT_OUTER_JOIN == join_type_ || LEFT_ANTI_JOIN == join_type_) {
      if (OB_FAIL(save_left_row(join_ctx))) {
        LOG_WARN("failed to save left row", K(ret));
      } else {
        join_ctx.state_ = JS_LEFT_JOIN;
      }
    } else {
      join_ctx.state_ = JS_JOIN_END;
    }
  } else if (NULL != join_ctx.right_row_) {
    if (need_right_join()) {
      if (OB_FAIL(save_right_row(join_ctx))) {
        LOG_WARN("failed to save right row", K(ret));
      } else {
        join_ctx.right_cache_iter_ = join_ctx.right_cache_.begin();
        join_ctx.state_ = JS_RIGHT_JOIN_CACHE;
      }
    } else {
      join_ctx.state_ = JS_JOIN_END;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("compare result should NOT be equal", K(ret));
  }
  UNUSED(row);
  return ret;
}

int ObMergeJoin::fill_cache_operate(ObMergeJoinCtx& join_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_last_left_row(join_ctx))) {
    LOG_WARN("failed to get last left row", K(ret));
  } else if (OB_FAIL(OB_I(t1) get_next_right_row(join_ctx))) {
    if (OB_ITER_END == ret) {
      join_ctx.right_iter_end_ = true;
    } else {
      LOG_WARN("failed to get right row", K(ret));
    }
  }
  return ret;
}

int ObMergeJoin::fill_cache_func_equal(ObMergeJoinCtx& join_ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(OB_I(t1) trans_to_fill_cache(join_ctx, row))) {
    LOG_WARN("failed to state fill cache", K(ret));
  }
  return ret;
}

int ObMergeJoin::fill_cache_func_diff_end(ObMergeJoinCtx& join_ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  row = NULL;
  // After fill cache,  if last_left_row is expired_, don't need to output.
  // If last_left_row is not expired_(false),
  // it means there is no matching rows of this row and we should output this row.
  if (LEFT_ANTI_JOIN == join_type_) {
    if (!join_ctx.left_row_matched_) {
      row = join_ctx.left_row_;
    }
    join_ctx.left_row_ = NULL;
    join_ctx.left_row_matched_ = false;
  } else if (LEFT_SEMI_JOIN == join_type_) {
    if (join_ctx.left_row_matched_) {
      row = join_ctx.left_row_;
    }
    join_ctx.left_row_ = NULL;
    join_ctx.left_row_matched_ = false;
  } else if (need_left_join() && !join_ctx.left_row_joined_) {
    if (OB_FAIL(OB_I(t1) left_join_rows(join_ctx, row))) {
      LOG_WARN("failed to left join rows", K(ret));
    }
  } else {
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(save_right_row(join_ctx))) {
      LOG_WARN("failed to save right row", K(ret));
    } else {
      join_ctx.right_cache_iter_ = join_ctx.right_cache_.begin();
      join_ctx.state_ = JS_FULL_CACHE;
    }
  }
  return ret;
}

int ObMergeJoin::get_next_right_cache_row(ObMergeJoinCtx& join_ctx) const
{
  int ret = OB_SUCCESS;
  ObRowStore::Iterator& iter = join_ctx.right_cache_iter_;
  ObRowStore::StoredRow*& stored_row = join_ctx.stored_row_;
  ObNewRow& row_buf = join_ctx.right_cache_row_buf_;
  join_ctx.right_row_ = &join_ctx.right_cache_row_buf_;
  if (OB_FAIL(THIS_WORKER.check_status())) {
    LOG_WARN("check physical plan status failed", K(ret));
  } else if (OB_FAIL(OB_I(t1) iter.get_next_row(row_buf, NULL, &stored_row))) {
    join_ctx.right_row_ = NULL;
  }
  return ret;
}

int ObMergeJoin::trans_to_read_cache(ObMergeJoinCtx& join_ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  bool is_match = false;
  row = NULL;
  if (OB_ISNULL(join_ctx.left_row_) || OB_ISNULL(join_ctx.right_row_)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("left row or right row is null", K(ret));
  } else if (OB_FAIL(OB_I(t1) calc_other_conds(join_ctx, is_match))) {
    LOG_WARN("failed to compare left and right row on other join conds", K(ret));
  } else {
    if (is_match) {
      if (LEFT_ANTI_JOIN == join_type_ || LEFT_SEMI_JOIN == join_type_) {
        join_ctx.left_row_matched_ = true;
      } else if (OB_FAIL(OB_I(t3) join_rows(join_ctx, row))) {
        LOG_WARN("failed to join rows", K(ret));
      } else if (OB_ISNULL(join_ctx.stored_row_)) {
        ret = OB_BAD_NULL_ERROR;
        LOG_WARN("stored row is null", K(ret));
      } else {
        join_ctx.left_row_joined_ = true;    // left row joined sign.
        join_ctx.stored_row_->payload_ = 1;  // right row joined sign.
      }
    } else {
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(save_left_row(join_ctx))) {
        LOG_WARN("failed to save left row", K(ret));
      } else {
        join_ctx.state_ = JS_READ_CACHE;
      }
    }
  }
  return ret;
}

int ObMergeJoin::trans_to_fill_cache(ObMergeJoinCtx& join_ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  bool is_match = false;
  ObRowStore& right_cache = join_ctx.right_cache_;
  const ObRowStore::StoredRow* stored_row = NULL;
  row = NULL;
  if (OB_ISNULL(join_ctx.left_row_) || OB_ISNULL(join_ctx.right_row_)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("left row or right row is null", K(ret));
  } else if (OB_FAIL(OB_I(t1) calc_other_conds(join_ctx, is_match))) {
    LOG_WARN("failed to compare left and right row on other join conds", K(ret));
  } else if (!is_skip_cache() &&
             OB_FAIL(OB_I(t3) right_cache.add_row(
                 *join_ctx.right_row_, stored_row, is_match ? 1 : 0, false /* doesn't by projector */))) {
    LOG_WARN("failed to add right row to cache", K(ret));
  } else {
    if (is_match) {
      if (LEFT_ANTI_JOIN == join_type_ || LEFT_SEMI_JOIN == join_type_) {
        join_ctx.left_row_matched_ = true;
      } else if (OB_FAIL(OB_I(t5) join_rows(join_ctx, row))) {
        LOG_WARN("failed to join rows", K(ret), K(join_type_));
      } else {
        join_ctx.left_row_joined_ = true;
      }
    } else {
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(save_left_row(join_ctx))) {
        LOG_WARN("failed to save left row", K(ret));
      } else if (is_skip_cache()) {
        join_ctx.state_ = JS_EMPTY_CACHE;
      } else {
        join_ctx.state_ = JS_FILL_CACHE;
      }
    }
  }
  return ret;
}

int ObMergeJoin::inner_create_operator_ctx(ObExecContext& exec_ctx, ObPhyOperatorCtx*& op_ctx) const
{
  int ret = OB_SUCCESS;
  ObMergeJoinCtx* join_ctx = NULL;
  if (OB_FAIL(CREATE_PHY_OPERATOR_CTX(ObMergeJoinCtx, exec_ctx, get_id(), get_type(), join_ctx))) {
    LOG_WARN("failed to create merge join ctx", K(ret));
  } else {
    op_ctx = join_ctx;
  }
  return ret;
}

OB_SERIALIZE_MEMBER((ObMergeJoin, ObJoin), merge_directions_);

}  // namespace sql
}  // namespace oceanbase
