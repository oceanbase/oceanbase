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

#include "sql/engine/join/ob_merge_join_op.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase {
using namespace common;
namespace sql {
OB_SERIALIZE_MEMBER((ObMergeJoinSpec, ObJoinSpec), equal_cond_infos_, merge_directions_, is_left_unique_);

OB_SERIALIZE_MEMBER(ObMergeJoinSpec::EqualConditionInfo, expr_, ser_eval_func_, is_opposite_);

const int64_t ObMergeJoinSpec::MERGE_DIRECTION_ASC = 1;
const int64_t ObMergeJoinSpec::MERGE_DIRECTION_DESC = -1;

ObMergeJoinOp::ObMergeJoinOp(ObExecContext& exec_ctx, const ObOpSpec& spec, ObOpInput* input)
    : ObJoinOp(exec_ctx, spec, input),
      state_(JS_JOIN_BEGIN),
      mem_context_(NULL),
      stored_row_(NULL),
      empty_cache_iter_side_(ITER_BOTH),
      equal_cmp_(-1),
      left_row_matched_(false)
{
  state_operation_func_[JS_JOIN_END] = &ObMergeJoinOp::join_end_operate;
  state_function_func_[JS_JOIN_END][FT_ROWS_EQUAL] = NULL;
  state_function_func_[JS_JOIN_END][FT_ROWS_DIFF] = NULL;
  state_function_func_[JS_JOIN_END][FT_ITER_GOING] = NULL;
  state_function_func_[JS_JOIN_END][FT_ITER_END] = &ObMergeJoinOp::join_end_func_end;

  state_operation_func_[JS_JOIN_BEGIN] = &ObMergeJoinOp::join_begin_operate;
  state_function_func_[JS_JOIN_BEGIN][FT_ROWS_EQUAL] = NULL;
  state_function_func_[JS_JOIN_BEGIN][FT_ROWS_DIFF] = NULL;
  state_function_func_[JS_JOIN_BEGIN][FT_ITER_GOING] = &ObMergeJoinOp::join_begin_func_going;
  state_function_func_[JS_JOIN_BEGIN][FT_ITER_END] = &ObMergeJoinOp::join_begin_func_end;

  state_operation_func_[JS_LEFT_JOIN] = &ObMergeJoinOp::left_join_operate;
  state_function_func_[JS_LEFT_JOIN][FT_ROWS_EQUAL] = NULL;
  state_function_func_[JS_LEFT_JOIN][FT_ROWS_DIFF] = NULL;
  state_function_func_[JS_LEFT_JOIN][FT_ITER_GOING] = &ObMergeJoinOp::left_join_func_going;
  state_function_func_[JS_LEFT_JOIN][FT_ITER_END] = &ObMergeJoinOp::left_join_func_end;

  state_operation_func_[JS_RIGHT_JOIN_CACHE] = &ObMergeJoinOp::right_join_cache_operate;
  state_function_func_[JS_RIGHT_JOIN_CACHE][FT_ROWS_EQUAL] = NULL;
  state_function_func_[JS_RIGHT_JOIN_CACHE][FT_ROWS_DIFF] = NULL;
  state_function_func_[JS_RIGHT_JOIN_CACHE][FT_ITER_GOING] = &ObMergeJoinOp::right_join_cache_func_going;
  state_function_func_[JS_RIGHT_JOIN_CACHE][FT_ITER_END] = &ObMergeJoinOp::right_join_cache_func_end;

  state_operation_func_[JS_RIGHT_JOIN] = &ObMergeJoinOp::right_join_operate;
  state_function_func_[JS_RIGHT_JOIN][FT_ROWS_EQUAL] = NULL;
  state_function_func_[JS_RIGHT_JOIN][FT_ROWS_DIFF] = NULL;
  state_function_func_[JS_RIGHT_JOIN][FT_ITER_GOING] = &ObMergeJoinOp::right_join_func_going;
  state_function_func_[JS_RIGHT_JOIN][FT_ITER_END] = &ObMergeJoinOp::right_join_func_end;

  state_operation_func_[JS_GOING_END_ONLY] = NULL;
  state_function_func_[JS_GOING_END_ONLY][FT_ROWS_EQUAL] = NULL;
  state_function_func_[JS_GOING_END_ONLY][FT_ROWS_DIFF] = NULL;
  state_function_func_[JS_GOING_END_ONLY][FT_ITER_GOING] = NULL;
  state_function_func_[JS_GOING_END_ONLY][FT_ITER_END] = NULL;

  state_operation_func_[JS_EMPTY_CACHE] = &ObMergeJoinOp::empty_cache_operate;
  state_function_func_[JS_EMPTY_CACHE][FT_ROWS_EQUAL] = &ObMergeJoinOp::empty_cache_func_equal;
  state_function_func_[JS_EMPTY_CACHE][FT_ROWS_DIFF] = &ObMergeJoinOp::empty_cache_func_diff;
  state_function_func_[JS_EMPTY_CACHE][FT_ITER_GOING] = NULL;
  state_function_func_[JS_EMPTY_CACHE][FT_ITER_END] = &ObMergeJoinOp::empty_cache_func_end;

  state_operation_func_[JS_FILL_CACHE] = &ObMergeJoinOp::fill_cache_operate;
  state_function_func_[JS_FILL_CACHE][FT_ROWS_EQUAL] = &ObMergeJoinOp::fill_cache_func_equal;
  state_function_func_[JS_FILL_CACHE][FT_ROWS_DIFF] = &ObMergeJoinOp::fill_cache_func_diff_end;
  state_function_func_[JS_FILL_CACHE][FT_ITER_GOING] = NULL;
  state_function_func_[JS_FILL_CACHE][FT_ITER_END] = &ObMergeJoinOp::fill_cache_func_diff_end;

  state_operation_func_[JS_FULL_CACHE] = &ObMergeJoinOp::full_cache_operate;
  state_function_func_[JS_FULL_CACHE][FT_ROWS_EQUAL] = &ObMergeJoinOp::full_cache_func_equal;
  state_function_func_[JS_FULL_CACHE][FT_ROWS_DIFF] = &ObMergeJoinOp::full_cache_func_diff;
  state_function_func_[JS_FULL_CACHE][FT_ITER_GOING] = NULL;
  state_function_func_[JS_FULL_CACHE][FT_ITER_END] = &ObMergeJoinOp::full_cache_func_end;

  state_operation_func_[JS_READ_CACHE] = &ObMergeJoinOp::read_cache_operate;
  state_function_func_[JS_READ_CACHE][FT_ROWS_EQUAL] = NULL;
  state_function_func_[JS_READ_CACHE][FT_ROWS_DIFF] = NULL;
  state_function_func_[JS_READ_CACHE][FT_ITER_GOING] = &ObMergeJoinOp::read_cache_func_going;
  state_function_func_[JS_READ_CACHE][FT_ITER_END] = &ObMergeJoinOp::read_cache_func_end;
}

int ObMergeJoinOp::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObJoinOp::inner_open())) {
    LOG_WARN("failed to open in base class", K(ret));
  } else if (OB_FAIL(left_fetcher_.init(*left_, ctx_.get_allocator(), &left_row_joined_)) ||
             OB_FAIL(right_fetcher_.init(*right_, ctx_.get_allocator(), NULL))) {
    LOG_WARN("init row fetcher failed", K(ret));
  }
  return ret;
}

int ObMergeJoinOp::switch_iterator()
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_FAIL(ObJoinOp::switch_iterator())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to rescan ObJoin", K(ret));
    }
  }

  return ret;
}

int ObMergeJoinOp::rescan()
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_FAIL(ObJoinOp::rescan())) {
    LOG_WARN("failed to rescan ObJoin", K(ret));
  }

  return ret;
}

int ObMergeJoinOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  state_operation_func_type state_operation = NULL;
  state_function_func_type state_function = NULL;
  int func = -1;
  output_row_produced_ = false;
  while (OB_SUCC(ret) && !output_row_produced_) {
    state_operation = this->ObMergeJoinOp::state_operation_func_[state_];
    OB_ASSERT(NULL != state_operation);
    if (OB_ITER_END == (ret = (this->*state_operation)())) {
      func = FT_ITER_END;
      ret = OB_SUCCESS;
    } else if (OB_FAIL(ret)) {
      LOG_WARN("failed to exec state operation", K(ret), K(state_));
    } else if (FALSE_IT(clear_evaluated_flag())) {
      // do nothing
    } else if (state_ < JS_GOING_END_ONLY) {  // see comment for JS_GOING_END_ONLY.
      func = FT_ITER_GOING;
    } else if (OB_FAIL(calc_equal_conds(equal_cmp_))) {
      LOG_WARN("failed to calc equal conds", K(ret), K(state_));
    } else if (0 == equal_cmp_) {
      func = FT_ROWS_EQUAL;
    } else {
      func = FT_ROWS_DIFF;
    }

    if (OB_SUCC(ret)) {
      state_function = this->ObMergeJoinOp::state_function_func_[state_][func];
      if (OB_ISNULL(state_function)) {
        ret = OB_BAD_NULL_ERROR;
        LOG_WARN("state operation is null ", K(ret));
      } else if (OB_FAIL((this->*state_function)()) && OB_ITER_END != ret) {
        LOG_WARN("failed state function", K(ret), K(state_), K(func));
      }
    }
  }  // while end

  return ret;
}

int ObMergeJoinOp::join_end_operate()
{
  return OB_ITER_END;
}

int ObMergeJoinOp::join_end_func_end()
{
  return OB_ITER_END;
}

int ObMergeJoinOp::join_begin_operate()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(left_fetcher_.next()) && OB_ITER_END != ret) {
    LOG_WARN("failed to get next left row", K(ret));
  }

  return ret;
}

int ObMergeJoinOp::join_begin_func_going()
{
  int ret = OB_SUCCESS;
  state_ = JS_EMPTY_CACHE;
  empty_cache_iter_side_ = ITER_RIGHT;
  return ret;
}

int ObMergeJoinOp::join_begin_func_end()
{
  if (need_right_join()) {
    state_ = JS_RIGHT_JOIN;
  } else {
    state_ = JS_JOIN_END;
  }

  return OB_SUCCESS;
}

int ObMergeJoinOp::left_join_operate()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(left_fetcher_.next()) && OB_ITER_END != ret) {
    LOG_WARN("failed to get left row", K(ret));
  }

  return ret;
}

int ObMergeJoinOp::left_join_func_going()
{
  int ret = OB_SUCCESS;
  output_row_produced_ = true;
  if (LEFT_ANTI_JOIN != MY_SPEC.join_type_ && OB_FAIL(blank_right_row())) {
    LOG_WARN("fail to blank left row", K(ret));
  }

  return ret;
}

int ObMergeJoinOp::left_join_func_end()
{
  state_ = JS_JOIN_END;
  return OB_SUCCESS;
}

int ObMergeJoinOp::right_join_cache_operate()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_next_right_cache_row()) && OB_ITER_END != ret) {
    LOG_WARN("failed to get next right cache row", K(ret));
  }

  return ret;
}

int ObMergeJoinOp::right_join_cache_func_going()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stored_row_)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("stored row is null", K(ret));
  } else if (!stored_row_->is_match()) {
    output_row_produced_ = true;
    if (OB_FAIL(blank_left_row())) {
      LOG_WARN("fail to blank right row", K(ret));
    }
  } else {
  }

  return ret;
}

int ObMergeJoinOp::right_join_cache_func_end()
{
  right_cache_.reset();
  right_cache_iter_.reset();
  // when right reach end, process right cache,
  // if full outer join, then to process left.
  if (right_fetcher_.reach_end_) {
    if (FULL_OUTER_JOIN == MY_SPEC.join_type_) {
      state_ = JS_LEFT_JOIN;
    } else {
      state_ = JS_JOIN_END;
    }
  } else {
    if (left_fetcher_.reach_end_) {
      state_ = JS_RIGHT_JOIN;
    } else {
      state_ = JS_EMPTY_CACHE;
    }
  }

  return OB_SUCCESS;
}

int ObMergeJoinOp::right_join_operate()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(right_fetcher_.next()) && OB_ITER_END != ret) {
    LOG_WARN("failed to get right row", K(ret));
  }

  return ret;
}

int ObMergeJoinOp::right_join_func_going()
{
  int ret = OB_SUCCESS;
  output_row_produced_ = true;
  if (OB_FAIL(blank_left_row())) {
    LOG_WARN("fail to blank right row", K(ret));
  }

  return ret;
}

int ObMergeJoinOp::right_join_func_end()
{
  state_ = JS_JOIN_END;
  return OB_SUCCESS;
}

int ObMergeJoinOp::read_cache_operate()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_next_right_cache_row()) && OB_ITER_END != ret) {
    LOG_WARN("failed to get next right cache row", K(ret));
  }

  return ret;
}

int ObMergeJoinOp::read_cache_func_going()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(trans_to_read_cache())) {
    LOG_WARN("failed to state read cache", K(ret));
  }

  return ret;
}

int ObMergeJoinOp::read_cache_func_end()
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  if (LEFT_ANTI_JOIN == MY_SPEC.join_type_ && JS_READ_CACHE == state_) {
    if (!left_row_matched_) {
      output_row_produced_ = true;
    }
    left_row_matched_ = false;
  } else if (LEFT_SEMI_JOIN == MY_SPEC.join_type_ && JS_READ_CACHE == state_) {
    if (left_row_matched_) {
      output_row_produced_ = true;
    }
    left_row_matched_ = false;
  } else if (need_left_join() && !left_row_joined_) {
    output_row_produced_ = true;
    if (OB_FAIL(blank_right_row())) {
      LOG_WARN("fail to blank left row", K(ret));
    }
  } else {
  }
  if (OB_SUCC(ret)) {
    left_row_matched_ = false;
    if (OB_FAIL(right_cache_.begin(right_cache_iter_))) {
      LOG_WARN("failed to begin iterator for chunk row store", K(ret));
    } else {
      state_ = JS_FULL_CACHE;
    }
  }

  return ret;
}

int ObMergeJoinOp::full_cache_operate()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(left_fetcher_.next())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get left row failed", K(ret));
    }
  } else if (OB_FAIL(get_next_right_cache_row())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get next right cache row", K(ret));
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("right cache should not be empty", K(ret));
    }
  }

  return ret;
}

int ObMergeJoinOp::full_cache_func_equal()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(trans_to_read_cache())) {
    LOG_WARN("failed to state read cache", K(ret));
  }

  return ret;
}

int ObMergeJoinOp::full_cache_func_diff()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(left_fetcher_.save_last())) {
    LOG_WARN("save last left row failed", K(ret));
  } else {
    if (right_fetcher_.reach_end_) {
      if (LEFT_OUTER_JOIN == MY_SPEC.join_type_ || LEFT_ANTI_JOIN == MY_SPEC.join_type_) {
        state_ = JS_LEFT_JOIN;
      } else if (need_right_join()) {
        if (OB_FAIL(right_cache_.begin(right_cache_iter_))) {
          LOG_WARN("failed to begin iterator for chunk row store", K(ret));
        } else {
          state_ = JS_RIGHT_JOIN_CACHE;
        }
      } else {
        state_ = JS_JOIN_END;
      }
    } else {
      if (need_right_join()) {
        if (OB_FAIL(right_cache_.begin(right_cache_iter_))) {
          LOG_WARN("failed to begin iterator for chunk row store", K(ret));
        } else {
          state_ = JS_RIGHT_JOIN_CACHE;
        }
      } else {
        right_cache_.reset();
        right_cache_iter_.reset();
        state_ = JS_EMPTY_CACHE;
      }
    }
  }

  return ret;
}

// left iterate end
int ObMergeJoinOp::full_cache_func_end()
{
  int ret = OB_SUCCESS;
  if (need_right_join()) {
    if (OB_FAIL(right_cache_.begin(right_cache_iter_))) {
      LOG_WARN("failed to begin iterator for chunk row store", K(ret));
    } else {
      state_ = JS_RIGHT_JOIN_CACHE;
    }
  } else {
    state_ = JS_JOIN_END;
  }

  return ret;
}

int ObMergeJoinOp::empty_cache_operate()
{
  int ret = OB_SUCCESS;
  if (empty_cache_iter_side_ & ITER_LEFT) {
    if (OB_FAIL(left_fetcher_.next()) && OB_ITER_END != ret) {
      LOG_WARN("get row from left failed", K(ret));
    }

    // For left join, the right row may be overwrite by blank_right_row(), need to recover here
    // if right side has backup row and right will not by iterated.
    if (OB_SUCC(ret) && !(empty_cache_iter_side_ & ITER_RIGHT) && right_fetcher_.has_backup_row_ && need_left_join()) {
      if (OB_FAIL(right_fetcher_.restore())) {
        LOG_WARN("restore backup row failed", K(ret));
      }
    }
  }
  if (OB_SUCCESS == ret && (empty_cache_iter_side_ & ITER_RIGHT)) {
    if (OB_FAIL(right_fetcher_.next()) && OB_ITER_END != ret) {
      LOG_WARN("get row from right failed", K(ret));
    }
    // Same with the left join here, see comment above.
    if (OB_SUCC(ret) && !(empty_cache_iter_side_ & ITER_LEFT) && left_fetcher_.has_backup_row_ && need_right_join()) {
      if (OB_FAIL(left_fetcher_.restore())) {
        LOG_WARN("restore backup row failed", K(ret));
      }
    }
  }
  return ret;
}

int ObMergeJoinOp::empty_cache_func_equal()
{
  int ret = OB_SUCCESS;
  if (MY_SPEC.is_skip_cache()) {
    // Keep in JS_EMPTY_CACHE state, iterate right side.
    empty_cache_iter_side_ = ITER_RIGHT;
  } else {
    // When reenter JS_EMPTY_CACHE state after JS_FILL_CACHE and JS_FULL_CACHE
    // iterate both side.
    empty_cache_iter_side_ = ITER_BOTH;
  }
  if (OB_FAIL(trans_to_fill_cache())) {
    LOG_WARN("failed to fill cache", K(ret));
  }
  return ret;
}

int ObMergeJoinOp::empty_cache_func_diff()
{
  int ret = OB_SUCCESS;
  if (equal_cmp_ < 0) {
    empty_cache_iter_side_ = ITER_LEFT;
    if (LEFT_ANTI_JOIN == MY_SPEC.join_type_) {
      output_row_produced_ = true;
    } else if (need_left_join()) {
      output_row_produced_ = true;
      if (OB_FAIL(blank_right_row())) {
        LOG_WARN("fail to blank left row", K(ret));
      }
    } else {
    }
  } else if (equal_cmp_ > 0) {
    empty_cache_iter_side_ = ITER_RIGHT;
    ;
    if (need_right_join()) {
      output_row_produced_ = true;
      if (OB_FAIL(blank_left_row())) {
        LOG_WARN("fail to blank right row", K(ret));
      }
    } else {
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("compare result should NOT be equal", K(ret));
  }

  return ret;
}

int ObMergeJoinOp::empty_cache_func_end()
{
  int ret = OB_SUCCESS;
  if (right_fetcher_.reach_end_) {
    if (need_left_join() || LEFT_OUTER_JOIN == MY_SPEC.join_type_ || LEFT_ANTI_JOIN == MY_SPEC.join_type_) {
      state_ = JS_LEFT_JOIN;
      if (OB_FAIL(left_fetcher_.save_last())) {
        LOG_WARN("save last row failed", K(ret));
      }
    } else {
      state_ = JS_JOIN_END;
    }
  } else if (left_fetcher_.reach_end_) {
    if (need_right_join()) {
      state_ = JS_RIGHT_JOIN;
      if (OB_FAIL(right_fetcher_.save_last())) {
        LOG_WARN("save last row failed", K(ret));
      }
    } else {
      state_ = JS_JOIN_END;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("one side should reach end", K(ret));
  }

  return ret;
}

int ObMergeJoinOp::fill_cache_operate()
{
  int ret = OB_SUCCESS;
  // left row is already fetched.
  if (OB_FAIL(right_fetcher_.next()) && OB_ITER_END != ret) {
    LOG_WARN("get row from right failed", K(ret));
  }

  return ret;
}

int ObMergeJoinOp::fill_cache_func_equal()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(trans_to_fill_cache())) {
    LOG_WARN("failed to state fill cache", K(ret));
  }

  return ret;
}

int ObMergeJoinOp::fill_cache_func_diff_end()
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  if (!right_fetcher_.reach_end_ && OB_FAIL(right_fetcher_.save_last())) {
    LOG_WARN("save last row failed", K(ret));
  } else if (OB_FAIL(right_cache_.finish_add_row(false))) {
    LOG_WARN("failed to finish add row to row store", K(ret));
  } else if (OB_FAIL(right_cache_.begin(right_cache_iter_))) {
    LOG_WARN("failed to begin iterator for chunk row store", K(ret));
  } else {
    state_ = JS_FULL_CACHE;
    if (LEFT_ANTI_JOIN == MY_SPEC.join_type_) {
      if (!left_row_matched_) {
        output_row_produced_ = true;
      }
      left_row_matched_ = false;
    } else if (LEFT_SEMI_JOIN == MY_SPEC.join_type_) {
      if (left_row_matched_) {
        output_row_produced_ = true;
      }
      left_row_matched_ = false;
    } else if (need_left_join() && !left_row_joined_) {
      output_row_produced_ = true;
      if (OB_FAIL(blank_right_row())) {
        LOG_WARN("fail to blank left row", K(ret));
      }
    }
  }

  return ret;
}

int ObMergeJoinOp::get_next_right_cache_row()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(try_check_status())) {
    LOG_WARN("check physical plan status failed", K(ret));
  } else if (OB_FAIL(right_cache_iter_.get_next_row(right_->get_spec().output_,
                 eval_ctx_,
                 const_cast<const ObChunkDatumStore::StoredRow**>(
                     reinterpret_cast<ObChunkDatumStore::StoredRow**>(&stored_row_))))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get next row from right cache failed", K(ret));
    }
  }

  return ret;
}

int ObMergeJoinOp::trans_to_read_cache()
{
  int ret = OB_SUCCESS;
  bool is_match = false;
  if (OB_FAIL(calc_other_conds(is_match))) {
    LOG_WARN("failed to compare left and right row on other join conds", K(ret));
  } else {
    if (is_match) {
      if (LEFT_ANTI_JOIN == MY_SPEC.join_type_ || LEFT_SEMI_JOIN == MY_SPEC.join_type_) {
        left_row_matched_ = true;
      } else if (FALSE_IT(output_row_produced_ = true)) {
        // do nothing
      } else if (OB_ISNULL(stored_row_)) {
        ret = OB_BAD_NULL_ERROR;
        LOG_WARN("stored row is null", K(ret));
      } else {
        left_row_joined_ = true;          // left row joined sign.
        stored_row_->set_is_match(true);  // right row joined sign.
      }
    } else {
    }
    if (OB_SUCC(ret)) {
      state_ = JS_READ_CACHE;
    }
  }

  return ret;
}

int ObMergeJoinOp::trans_to_fill_cache()
{
  int ret = OB_SUCCESS;
  bool is_match = false;
  if (OB_FAIL(calc_other_conds(is_match))) {
    LOG_WARN("failed to compare left and right row on other join conds", K(ret));
  } else if (!MY_SPEC.is_skip_cache()) {
    if (OB_ISNULL(mem_context_)) {
      ObSQLSessionInfo* session = ctx_.get_my_session();
      uint64_t tenant_id = session->get_effective_tenant_id();
      lib::ContextParam param;
      param.set_mem_attr(tenant_id, ObModIds::OB_SQL_MERGE_JOIN, ObCtxIds::WORK_AREA)
          .set_properties(lib::USE_TL_PAGE_OPTIONAL);
      if (OB_FAIL(CURRENT_CONTEXT.CREATE_CONTEXT(mem_context_, param))) {
        LOG_WARN("create entity failed", K(ret));
      } else if (OB_ISNULL(mem_context_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null memory entity returned", K(ret));
      } else if (OB_FAIL(right_cache_.init(UINT64_MAX /*mem_limit*/,
                     tenant_id,
                     ObCtxIds::WORK_AREA,
                     ObModIds::OB_SQL_MERGE_JOIN,
                     false /*disable_dump*/,
                     sizeof(bool) /*row_extend_size, is_matched*/))) {
        LOG_WARN("init row store failed", K(ret));
      } else {
        right_cache_.set_allocator(mem_context_->get_malloc_allocator());
      }
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(right_cache_.add_row(right_->get_spec().output_,
                   &eval_ctx_,
                   reinterpret_cast<ObChunkDatumStore::StoredRow**>(&stored_row_)))) {
      LOG_WARN("failed to add right row to cache", K(ret));
    } else if (OB_ISNULL(stored_row_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to add stored row", K(stored_row_), K(ret));
    } else {
      stored_row_->set_is_match(is_match);
    }
  }
  if (OB_SUCC(ret)) {
    if (is_match) {
      if (LEFT_ANTI_JOIN == MY_SPEC.join_type_ || LEFT_SEMI_JOIN == MY_SPEC.join_type_) {
        left_row_matched_ = true;
      } else if (FALSE_IT(output_row_produced_ = true)) {
        // do nothing
      } else {
        left_row_joined_ = true;
      }
    } else {
    }
    if (OB_SUCC(ret)) {
      if (MY_SPEC.is_skip_cache()) {
        state_ = JS_EMPTY_CACHE;
      } else {
        state_ = JS_FILL_CACHE;
      }
    }
  }

  return ret;
}

int ObMergeJoinOp::calc_equal_conds(int64_t& cmp_res)
{
  int ret = OB_SUCCESS;
  cmp_res = 0;
  for (int64_t i = 0; OB_SUCC(ret) && 0 == cmp_res && i < MY_SPEC.equal_cond_infos_.count(); i++) {
    const ObMergeJoinSpec::EqualConditionInfo& equal_cond = MY_SPEC.equal_cond_infos_.at(i);
    ObExpr* l_expr = equal_cond.expr_->args_[0];
    ObExpr* r_expr = equal_cond.expr_->args_[1];
    ObDatum* l_datum = NULL;
    ObDatum* r_datum = NULL;
    if (OB_FAIL(l_expr->eval(eval_ctx_, l_datum)) || OB_FAIL(r_expr->eval(eval_ctx_, r_datum))) {
      LOG_WARN("expr evaluate failed", K(ret));
    } else {
      if (l_datum->is_null() && r_datum->is_null()) {
        cmp_res = (T_OP_NSEQ == equal_cond.expr_->type_) ? 0 : -1;
      } else if (0 != (cmp_res = equal_cond.ns_cmp_func_(*l_datum, *r_datum))) {
        cmp_res *= MY_SPEC.merge_directions_.at(i);
        if (equal_cond.is_opposite_) {
          cmp_res *= -1;
        }
      }
    }
  }  // for end

  return ret;
}

}  // end namespace sql
}  // end namespace oceanbase
