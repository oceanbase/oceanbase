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

namespace oceanbase
{
using namespace common;
namespace sql
{
static const int64_t BATCH_MULTIPLE_TIMES = 10;
OB_SERIALIZE_MEMBER((ObMergeJoinSpec, ObJoinSpec), equal_cond_infos_,
                    merge_directions_, is_left_unique_,
                    left_child_fetcher_all_exprs_,
                    right_child_fetcher_all_exprs_);

OB_SERIALIZE_MEMBER(ObMergeJoinSpec::EqualConditionInfo,
                    expr_, ser_eval_func_, is_opposite_);

const int64_t ObMergeJoinSpec::MERGE_DIRECTION_ASC = 1;
const int64_t ObMergeJoinSpec::MERGE_DIRECTION_DESC = -1;

ObMergeJoinOp::ObMergeJoinOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
  : ObJoinOp(exec_ctx, spec, input),
    state_(JS_JOIN_BEGIN),
    mem_context_(NULL),
    right_cache_(ObModIds::OB_SQL_MERGE_JOIN),
    stored_row_(NULL),
    stored_row_idx_(-1),
    empty_cache_iter_side_(ITER_BOTH),
    equal_cmp_(-1),
    left_row_matched_(false),
    cmp_res_(0),
    match_groups_(),
    is_last_right_join_output_(false),
    output_cache_(),
    rj_match_vec_(NULL),
    rj_match_vec_size_(0),
    group_idx_(0),
    left_group_(),
    right_group_(),
    batch_join_state_(BJS_JOIN_BEGIN),
    left_brs_fetcher_(match_groups_, *this, exec_ctx.get_allocator()),
    right_brs_fetcher_(match_groups_, *this, exec_ctx.get_allocator()),
    profile_(ObSqlWorkAreaType::HASH_WORK_AREA),
    sql_mem_processor_(profile_, op_monitor_info_),
    left_mem_bound_ratio_(0)
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

  state_operation_func_[JS_GOING_END_ONLY]   = NULL;
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
  ObIAllocator &allocator = ctx_.get_allocator();
  if (OB_FAIL(ObJoinOp::inner_open())) {
    LOG_WARN("failed to open in base class", K(ret));
  } else if (OB_FAIL(left_fetcher_.init(*left_, allocator, &left_row_joined_))
             || OB_FAIL(right_fetcher_.init(*right_, allocator, NULL))) {
    LOG_WARN("init row fetcher failed", K(ret));
  } else if (OB_FAIL(init_mem_context())) {
    LOG_WARN("fail to init memory context", K(ret));
  } else if (MY_SPEC.is_vectorized()) {
    const ExprFixedArray &left_outputs = left_->get_spec().output_;
    const ExprFixedArray &right_outputs = right_->get_spec().output_;
    const uint64_t tenant_id = ctx_.get_my_session()->get_effective_tenant_id();
    const ObIArray<ObMergeJoinSpec::EqualConditionInfo> &equal_cond_infos =
                                                          MY_SPEC.equal_cond_infos_;
    const int64_t left_width = left_->get_spec().width_;
    const int64_t right_width = right_->get_spec().width_;
    const double width_ratio = ((double)left_width) / ((double)(left_width + right_width));
    const double MIN_LEFT_MEM_BOUND_RATIO = 0.2;
    const double MAX_LEFT_MEM_BOUND_RATIO = 0.8;
    // We prefer more memory to the left, otherwise there may waste memory, so left_mem_bound_ratio_
    // is multiplied by a coefficient of 1.2.
    left_mem_bound_ratio_ = MAX(MIN(MAX_LEFT_MEM_BOUND_RATIO, 1.2 * width_ratio), MIN_LEFT_MEM_BOUND_RATIO);
    const int64_t cache_size = MY_SPEC.max_batch_size_ * BATCH_MULTIPLE_TIMES *
                                  (left_width + right_width);
    if (OB_FAIL(sql_mem_processor_.init(&mem_context_->get_malloc_allocator(),
                                        tenant_id,
                                        std::max(2L << 20, cache_size),
                                        MY_SPEC.type_,
                                        MY_SPEC.id_,
                                        &ctx_))) {
      LOG_WARN("failed to init sql memory manager processor", K(ret));
    } else if (OB_FAIL(left_brs_fetcher_.init(
                tenant_id, true, left_, equal_cond_infos,
                &(MY_SPEC.left_child_fetcher_all_exprs_)))) {
      LOG_WARN("init left batch fetcher failed", K(ret));
    } else if (OB_FAIL(right_brs_fetcher_.init(
                    tenant_id, false, right_, equal_cond_infos,
                    &(MY_SPEC.right_child_fetcher_all_exprs_)))) {
      LOG_WARN("init right batch fetcher failed", K(ret));
    }
    LOG_TRACE("trace init sql mem mgr for merge join", K(profile_.get_cache_size()),
                                                        K(profile_.get_expect_size()));
  } else {
    // non-vectorized
    const uint64_t tenant_id = ctx_.get_my_session()->get_effective_tenant_id();
    if (!MY_SPEC.is_skip_cache()) {
      if (OB_FAIL(sql_mem_processor_.init(&mem_context_->get_malloc_allocator(),
                                          tenant_id, 256 * right_->get_spec().width_,
                                          MY_SPEC.type_,
                                          MY_SPEC.id_,
                                          &ctx_))) {
        LOG_WARN("failed to init sql memory manager processor", K(ret));
      } else if (OB_FAIL(right_cache_.init(UINT64_MAX/*mem_limit*/,
                                    tenant_id,
                                    ObCtxIds::WORK_AREA,
                                    ObModIds::OB_SQL_MERGE_JOIN))) {
        LOG_WARN("init row store failed", K(ret));
      } else {
        right_cache_.set_allocator(mem_context_->get_malloc_allocator());
        right_cache_.set_callback(&sql_mem_processor_);
        right_cache_.set_io_event_observer(&io_event_observer_);
        right_cache_.set_dir_id(sql_mem_processor_.get_dir_id());
      }
    }
  }
  LOG_TRACE("merge join left unique", K(MY_SPEC.id_), K(MY_SPEC.is_left_unique_));
  return ret;
}

int ObMergeJoinOp::inner_switch_iterator()
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_FAIL(ObJoinOp::inner_switch_iterator())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to rescan ObJoin", K(ret));
    }
  }

  return ret;
}

int ObMergeJoinOp::inner_rescan()
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_FAIL(ObJoinOp::inner_rescan())) {
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
  } // while end

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
  if (LEFT_ANTI_JOIN != MY_SPEC.join_type_
      && OB_FAIL(blank_right_row())) {
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
  } else if (!is_match(stored_row_idx_)) {
    output_row_produced_ = true;
    if (OB_FAIL(blank_left_row())) {
      LOG_WARN("fail to blank right row", K(ret));
    }
  } else {}

  return ret;
}

int ObMergeJoinOp::right_join_cache_func_end()
{
  right_cache_.reset();
  right_cache_iter_.reset();
  stored_row_idx_ = -1;
  rj_match_vec_->reset(rj_match_vec_size_);
  // full cache diff, 且right reach end时,
  // 先将right cache中数据处理, 结束后如果是FULL_OUTER_JOIN,
  // 再处理LEFT 数据
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
  if ((LEFT_SEMI_JOIN == MY_SPEC.join_type_ || LEFT_ANTI_JOIN == MY_SPEC.join_type_)
       && left_row_matched_) {
    //terminate read right rows in advance
    ret = OB_ITER_END;
  } else if (OB_FAIL(get_next_right_cache_row()) && OB_ITER_END != ret) {
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
  } else {}
  if (OB_SUCC(ret)) {
    left_row_matched_ = false;
    if (OB_FAIL(right_cache_.begin(right_cache_iter_))) {
      LOG_WARN("failed to begin iterator for chunk row store", K(ret));
    } else {
      stored_row_idx_ = -1;
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
    //在右表已迭代结束时semi join和inner join可提前退出
    if (right_fetcher_.reach_end_) {
      if (LEFT_OUTER_JOIN == MY_SPEC.join_type_ || LEFT_ANTI_JOIN == MY_SPEC.join_type_) {
        state_ = JS_LEFT_JOIN;
      } else if (need_right_join()) {
        // full cache diff, 且right reach end时,
        // 先将right cache中数据处理, 结束后如果是FULL_OUTER_JOIN,
        // 再处理LEFT 数据
        if (OB_FAIL(right_cache_.begin(right_cache_iter_))) {
          LOG_WARN("failed to begin iterator for chunk row store", K(ret));
        } else {
          stored_row_idx_ = -1;
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
          stored_row_idx_ = -1;
          state_ = JS_RIGHT_JOIN_CACHE;
        }
      } else {
        right_cache_.reset();
        right_cache_iter_.reset();
        stored_row_idx_ = -1;
        state_ = JS_EMPTY_CACHE;
      }
    }
  }

  return ret;
}

// 当左表数据已迭代结束时
int ObMergeJoinOp::full_cache_func_end()
{
  int ret = OB_SUCCESS;
  if (need_right_join()) {
    if (OB_FAIL(right_cache_.begin(right_cache_iter_))) {
      LOG_WARN("failed to begin iterator for chunk row store", K(ret));
    } else {
      stored_row_idx_ = -1;
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
    if (OB_SUCC(ret)
        && !(empty_cache_iter_side_ & ITER_RIGHT)
        && right_fetcher_.has_backup_row_
        && need_left_join()) {
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
    if (OB_SUCC(ret)
        && !(empty_cache_iter_side_ & ITER_LEFT)
        && left_fetcher_.has_backup_row_
        && need_right_join()) {
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
    } else {}
  } else if (equal_cmp_ > 0) {
    empty_cache_iter_side_ = ITER_RIGHT;;
    if (need_right_join()) {
      output_row_produced_ = true;
      if (OB_FAIL(blank_left_row())) {
        LOG_WARN("fail to blank right row", K(ret));
      }
    } else {}
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
    if (need_left_join()
        || LEFT_OUTER_JOIN == MY_SPEC.join_type_
        || LEFT_ANTI_JOIN == MY_SPEC.join_type_) {
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
    stored_row_idx_ = -1;
    state_ = JS_FULL_CACHE;
    //fill cache结束,  如果last_left_row标记为expired_, 则不用输出,
    //如果last_left_row没有标记expired_(false), 则表示该行不存在符合条件的行，可输出
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
  } else if (OB_FAIL(right_cache_iter_.get_next_row(right_->get_spec().output_, eval_ctx_,
                     const_cast<const ObChunkDatumStore::StoredRow **>(&stored_row_)))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get next row from right cache failed", K(ret));
    }
  } else {
    ++stored_row_idx_;
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
      if (LEFT_ANTI_JOIN == MY_SPEC.join_type_
          || LEFT_SEMI_JOIN == MY_SPEC.join_type_) {
        left_row_matched_ = true;
      } else if (FALSE_IT(output_row_produced_ = true)) {
        // do nothing
      } else if (OB_ISNULL(stored_row_)) {
        ret = OB_BAD_NULL_ERROR;
        LOG_WARN("stored row is null", K(ret));
      } else {
        left_row_joined_ = true;     // left row joined sign.
        if (need_right_join() && OB_FAIL(set_is_match(stored_row_idx_, true))) {
          LOG_WARN("fail to set right row joined sign", K(ret), K(stored_row_idx_));
        }
      }
    } else {}
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
    if (OB_FAIL(process_dump())) {
      LOG_WARN("failed to process dump", K(ret));
    } else if (OB_FAIL(right_cache_.add_row(right_->get_spec().output_, &eval_ctx_, &stored_row_))) {
      LOG_WARN("failed to add right row to cache", K(ret));
    } else if (OB_ISNULL(stored_row_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to add stored row", K(stored_row_), K(ret));
    } else if (need_right_join() && OB_FAIL(set_is_match(++stored_row_idx_, is_match))) {
      LOG_WARN("failed to set right join flags", K(ret), K(stored_row_idx_));
    }
  }
  if (OB_SUCC(ret)) {
    if (is_match) {
      if (LEFT_ANTI_JOIN == MY_SPEC.join_type_
          || LEFT_SEMI_JOIN == MY_SPEC.join_type_) {
        left_row_matched_ = true;
      } else if (FALSE_IT(output_row_produced_ = true)) {
        // do nothing
      } else {
        left_row_joined_ = true;
      }
    } else {}
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

int ObMergeJoinOp::calc_equal_conds(int64_t &cmp_res)
{
  int ret = OB_SUCCESS;
  cmp_res = 0;
  for (int64_t i = 0;
       OB_SUCC(ret) && 0 == cmp_res && i < MY_SPEC.equal_cond_infos_.count();
       i++) {
    const ObMergeJoinSpec::EqualConditionInfo &equal_cond = MY_SPEC.equal_cond_infos_.at(i);
    ObExpr *l_expr = equal_cond.is_opposite_ ? equal_cond.expr_->args_[1]
                                             : equal_cond.expr_->args_[0];
    ObExpr *r_expr = equal_cond.is_opposite_ ? equal_cond.expr_->args_[0]
                                             : equal_cond.expr_->args_[1];
    ObDatum *l_datum = NULL;
    ObDatum *r_datum = NULL;
    if (OB_FAIL(l_expr->eval(eval_ctx_, l_datum))
        || OB_FAIL(r_expr->eval(eval_ctx_, r_datum))) {
      LOG_WARN("expr evaluate failed", K(ret));
    } else {
      if (l_datum->is_null() && r_datum->is_null()) {
        cmp_res = (T_OP_NSEQ == equal_cond.expr_->type_) ? 0 : -1;
      } else {
        int cmp_ret = 0;
        if (OB_FAIL(equal_cond.ns_cmp_func_(*l_datum, *r_datum, cmp_ret))) {
          LOG_WARN("failed to compare", K(ret));
        } else if (cmp_ret != 0) {
          cmp_res = cmp_ret;
          cmp_res *= MY_SPEC.merge_directions_.at(i);
        }
      }
    }
  } // for end

  return ret;
}

int ObMergeJoinOp::init_mem_context()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(mem_context_)) {
    ObSQLSessionInfo *session = ctx_.get_my_session();
    uint64_t tenant_id = session->get_effective_tenant_id();
    lib::ContextParam param;
    param.set_mem_attr(tenant_id,
                        ObModIds::OB_SQL_MERGE_JOIN,
                        ObCtxIds::WORK_AREA)
      .set_properties(lib::USE_TL_PAGE_OPTIONAL);
    if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(mem_context_, param))) {
      LOG_WARN("create entity failed", K(ret));
    } else if (OB_ISNULL(mem_context_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null memory entity returned", K(ret));
    }
  }
  return ret;
}

int ObMergeJoinOp::set_is_match(const int64_t idx, const bool is_match)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expand_match_flags_if_necessary(idx + 1, true))) {
    LOG_WARN("fail to expand match flags", K(ret));
  } else if (is_match) {
    rj_match_vec_->set(idx);
  }
  return ret;
}

int ObMergeJoinOp::process_dump()
{
  int ret = OB_SUCCESS;
  bool updated = false;
  bool dumped = false;
  UNUSED(updated);
  if (OB_FAIL(sql_mem_processor_.update_max_available_mem_size_periodically(
      &mem_context_->get_malloc_allocator(),
      [&](int64_t cur_cnt){ return right_cache_.get_row_cnt_in_memory() > cur_cnt; },
      updated))) {
    LOG_WARN("failed to update max available memory size periodically", K(ret));
  } else if (sql_mem_processor_.get_data_size() > sql_mem_processor_.get_mem_bound()
         && GCONF.is_sql_operator_dump_enabled()
          && OB_FAIL(sql_mem_processor_.extend_max_memory_size(
            &mem_context_->get_malloc_allocator(),
            [&](int64_t max_memory_size) {
              return sql_mem_processor_.get_data_size() > max_memory_size;
            },
            dumped, sql_mem_processor_.get_data_size()))) {
    LOG_WARN("failed to extend max memory size", K(ret));
  } else if (dumped && OB_FAIL(right_cache_.dump(false, true))) {
    LOG_WARN("failed to dump row store", K(ret));
  } else {
    LOG_TRACE("trace material dump", K(sql_mem_processor_.get_data_size()),
      K(right_cache_.get_row_cnt_in_memory()), K(sql_mem_processor_.get_mem_bound()));
  }
  return ret;
}

int ObMergeJoinOp::ChildBatchFetcher::init(
    const uint64_t tenant_id, bool is_left, ObOperator *child,
    const ObIArray<ObMergeJoinSpec::EqualConditionInfo> &equal_cond_infos,
    const ExprFixedArray *all_exprs)
{
  int ret = OB_SUCCESS;
  child_ = child;
  all_exprs_ = all_exprs;
  // child operator's spec batch_size might be zero(vectorization NOT enabled case)
  // In that case, child return 1 row at a time, set local batch default 1
  batch_size_ =
      child->get_spec().max_batch_size_ == 0 ? 1 : child->get_spec().max_batch_size_;
  if (OB_FAIL(datum_store_.init(0/*mem_limit*/,
                                tenant_id,
                                ObCtxIds::WORK_AREA,
                                ObModIds::OB_SQL_MERGE_JOIN))) {
    LOG_WARN("init datum store failed", K(ret));
  } else if (OB_FAIL(brs_holder_.init(*all_exprs_, merge_join_op_.eval_ctx_))) {
    LOG_WARN("init brs holder failed", K(ret));
  } else if (OB_FAIL(equal_param_idx_.init(equal_cond_infos.count()))) {
    LOG_WARN("init equal param idx failed", K(ret));
  } else if (OB_ISNULL(merge_join_op_.mem_context_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null memory context", K(ret));
  } else {
    datum_store_.set_allocator(merge_join_op_.mem_context_->get_malloc_allocator());
    datum_store_.set_mem_stat(&(merge_join_op_.sql_mem_processor_));
    datum_store_.set_io_observer(&(merge_join_op_.io_event_observer_));
    datum_store_.set_dir_id(merge_join_op_.sql_mem_processor_.get_dir_id());
    for (int64_t i = 0; i < equal_cond_infos.count() && OB_SUCC(ret); i++) {
      const ObMergeJoinSpec::EqualConditionInfo &equal_cond = equal_cond_infos.at(i);
      ObExpr *param_expr = NULL;
      if ((is_left && !equal_cond.is_opposite_) || (!is_left && equal_cond.is_opposite_)) {
        param_expr = equal_cond.expr_->args_[0];
      } else {
        param_expr = equal_cond.expr_->args_[1];
      }
      int64_t idx = -1;
      for (int64_t j = 0; j < all_exprs_->count() && idx < 0; j++) {
        if (all_exprs_->at(j) == param_expr) {
          idx = j;
        }
      }
      if (OB_UNLIKELY(idx < 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("equal cond param not found in child output", K(ret), K(i), K(equal_cond),
                  KPC(param_expr), K(is_left), K(merge_join_op_.left_->get_spec().output_),
                  K(merge_join_op_.right_->get_spec().output_));
      } else if (OB_FAIL(equal_param_idx_.push_back(idx))) {
        LOG_WARN("push back failed", K(ret));
      }
    }
    LOG_DEBUG("end init equal conds params idx", K(equal_param_idx_));
  }
  return ret;
}

int ObMergeJoinOp::ChildBatchFetcher::get_next_batch(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  const int64_t remain_backup_rows = backup_rows_cnt_ - backup_rows_used_;
  if (OB_UNLIKELY(0 != remain_backup_rows)) {
    if (OB_UNLIKELY(backup_datums_.count() != all_exprs_->count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("store datums cnt and child output cnt not equal", K(ret),
                K(backup_datums_.count()), K(all_exprs_->count()));
    } else {
      const int64_t restore_cnt = MIN(max_row_cnt, remain_backup_rows);
      for (int64_t i = 0; i < backup_datums_.count(); i++) {
        const ObExpr *expr = all_exprs_->at(i);
        if (expr->is_const_expr()) {
          continue;
        } else {
          ObDatum *datum = all_exprs_->at(i)->locate_batch_datums(merge_join_op_.eval_ctx_);
          MEMCPY(datum, backup_datums_.at(i) + backup_rows_used_, sizeof(ObDatum) * restore_cnt);
          all_exprs_->at(i)->set_evaluated_projected(merge_join_op_.eval_ctx_);
        }
      }
      brs_.size_ = restore_cnt;
      brs_.end_ = false;
      brs_.skip_ = NULL;
      backup_rows_used_ += restore_cnt;
    }
  } else {
    if (brs_.end_) {
      // not more fetch if reach end
      brs_.size_ = 0;
    } else {
      const ObBatchRows *child_brs = NULL;
      if (OB_FAIL(child_->get_next_batch(max_row_cnt, child_brs))) {
        LOG_WARN("get child next batch failed", K(ret));
      } else {
        brs_ = *child_brs;
        brs_holder_.reset();
      }
    }
    for (auto i = 0; OB_SUCC(ret) && i < all_exprs_->count(); i++) {
      auto *expr = all_exprs_->at(i);
      if (OB_FAIL(expr->eval_batch(merge_join_op_.eval_ctx_, *(brs_.skip_),
                                   brs_.size_))) {
        LOG_WARN("expr batch evaluation failed", K(ret), KPC(expr));
      }
    }
  }
  cur_idx_ = 0;
  LOG_DEBUG("end get next batch", K(this), K(brs_));
  return ret;
}

// The other fetcher is already end, backup remaining rows in this fetcher
// and output them after output all rows in output_cache_.
int ObMergeJoinOp::ChildBatchFetcher::backup_remain_rows()
{
  int ret = OB_SUCCESS;
  ObIAllocator &allocator = merge_join_op_.ctx_.get_allocator();
  if (OB_UNLIKELY(cur_idx_ >= brs_.size_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no remain rows", K(ret), K(cur_idx_), K(brs_.size_));
  } else if (backup_datums_.empty()) {
    int64_t alloc_size = sizeof(ObDatum) * merge_join_op_.spec_.max_batch_size_;
    for (int64_t i = 0; i < all_exprs_->count() && OB_SUCC(ret); i++) {
      const ObExpr *expr = all_exprs_->at(i);
      ObDatum *datum = NULL;
      // if expr is const, use NULL datum pointer as padding.
      if (!expr->is_const_expr() &&
            OB_ISNULL(datum = static_cast<ObDatum *>(allocator.alloc(alloc_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret));
      } else if (OB_FAIL(backup_datums_.push_back(datum))) {
        LOG_WARN("push back failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(all_exprs_->count() != backup_datums_.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("count mismatch", K(ret), K(all_exprs_->count()), K(backup_datums_.count()));
    } else {
      for (int64_t i = 0; i < all_exprs_->count() && OB_SUCC(ret); i++) {
        const ObExpr *expr = all_exprs_->at(i);
        if (expr->is_const_expr()) {
          continue;
        } else {
          backup_rows_cnt_ = 0;
          backup_rows_used_ = 0;
          ObDatumVector src_datum = expr->locate_expr_datumvector(merge_join_op_.eval_ctx_);
          ObDatum *datum = backup_datums_.at(i);
          if (OB_ISNULL(datum)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("backup datums memory is null", K(ret), K(i), K(all_exprs_->count()));
          } else {
            for (int64_t j = cur_idx_; j < brs_.size_ && OB_SUCC(ret); j++) {
              if (!brs_.skip_->contain(j)) {
                datum[backup_rows_cnt_++] = *src_datum.at(j);
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObMergeJoinOp::ChildBatchFetcher::get_next_nonskip_row(bool &got_next_batch)
{
  int ret = OB_SUCCESS;
  got_next_batch = false;
  if (OB_UNLIKELY(cur_idx_ < brs_.size_ && NULL == brs_.skip_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("brs skip is null", K(ret), K(brs_), K(cur_idx_));
  } else {
    bool next_row_found = false;
    while (OB_SUCC(ret) && !next_row_found) {
      cur_idx_++;
      if (cur_idx_ < brs_.size_) {
        next_row_found = !brs_.skip_->contain(cur_idx_);
      } else if (FALSE_IT(merge_join_op_.clear_evaluated_flag())) {
      } else if (OB_FAIL(get_next_batch(batch_size_))) {
        LOG_WARN("get next batch failed", K(ret));
      } else if (OB_ISNULL(brs_.skip_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("brs skip is null", K(ret));
      } else {
        got_next_batch = true;
        next_row_found = !brs_.skip_->contain(cur_idx_) || iter_end();
        LOG_DEBUG("get_next_nonskip_row", K(cur_idx_), K(brs_.size_),
                 K(got_next_batch), K(next_row_found));
      }
    }
  }
  return ret;
}

template<bool need_store_unmatch, bool is_left>
int ObMergeJoinOp::ChildBatchFetcher::get_next_small_group(int64_t &cmp_res)
{
  int ret = OB_SUCCESS;
  bool greater_found = false;
  bool all_batch_finished = false;
  bool enough_datums = merge_join_op_.has_enough_datums();
  JoinRowList row_list(datum_store_.get_row_cnt());
  ObEvalCtx::BatchInfoScopeGuard guard(merge_join_op_.eval_ctx_);
  while (OB_SUCC(ret) && !all_batch_finished && !greater_found && !enough_datums) {
    if (need_store_unmatch && OB_LIKELY(cur_idx_ < brs_.size_)) {
      ObRADatumStore::StoredRow *stored_row = NULL;
      guard.set_batch_idx(cur_idx_);
      guard.set_batch_size(brs_.size_);
      if (OB_FAIL(datum_store_.add_row(*all_exprs_, &(merge_join_op_.eval_ctx_), &stored_row))) {
        LOG_WARN("add row failed", K(ret));
      } else if (OB_ISNULL(stored_row)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("add stored row failed", K(ret));
      } else {
        row_list.end_ += 1;
      }
    }
    if (OB_SUCC(ret)) {
      bool got_next_batch = false;
      if (OB_FAIL(get_next_nonskip_row(got_next_batch))) {
        LOG_WARN("get next non-skip row failed", K(ret));
      } else if (got_next_batch) {
        enough_datums = merge_join_op_.has_enough_datums();
        all_batch_finished = iter_end();
      }
    }
    if (OB_SUCC(ret) && cur_idx_ < brs_.size_) {
      if (OB_FAIL(merge_join_op_.calc_equal_conds_with_batch_idx(cmp_res))) {
        LOG_WARN("calc equal conds with batch index failed", K(ret));
      } else {
        greater_found = is_left ? cmp_res >= 0 : cmp_res <= 0;
      }
    }
  }
  if (OB_SUCC(ret) && need_store_unmatch) {
    if (OB_FAIL(merge_join_op_.match_groups_.push_back(std::make_pair(
      is_left ? row_list : JoinRowList(),
      is_left ? JoinRowList() : row_list)))) {
      LOG_WARN("match group push back failed", K(ret));
    }
  }
  return ret;
}

template<bool is_left>
int ObMergeJoinOp::ChildBatchFetcher::get_next_equal_group(JoinRowList &row_list,
                                                    const ObRADatumStore::StoredRow *stored_row,
                                                    const bool is_unique,
                                                    ObRADatumStore::StoredRow *&new_stored_row)
{
  int ret = OB_SUCCESS;
  bool greater_found = false;
  bool all_batch_finished = false;
  int64_t cmp_res = 0;
  if (is_unique) {
    bool got_next_batch = false;
    if (OB_FAIL(get_next_nonskip_row(got_next_batch))) {
      LOG_WARN("get next non-skip row failed", K(ret));
    }
  } else {
    ObEvalCtx::BatchInfoScopeGuard guard(merge_join_op_.eval_ctx_);
    while (OB_SUCC(ret) && !all_batch_finished && !greater_found) {
      bool got_next_batch = false;
      if (OB_FAIL(get_next_nonskip_row(got_next_batch))) {
        LOG_WARN("get next non-skip row failed", K(ret));
      } else if (got_next_batch) {
        all_batch_finished = iter_end();
      }

      if (OB_SUCC(ret) && !all_batch_finished) {
        if (OB_FAIL(merge_join_op_.calc_equal_conds_with_stored_row<!is_left>(
                                        stored_row, cur_idx_, cmp_res))) {
          LOG_WARN("calc equal conds failed", K(ret));
        } else {
          greater_found = is_left ? cmp_res > 0 : cmp_res < 0;
          if (!greater_found) {
            guard.set_batch_idx(cur_idx_);
            guard.set_batch_size(batch_size_);
            if (OB_FAIL(datum_store_.add_row(*all_exprs_, &(merge_join_op_.eval_ctx_), &new_stored_row))) {
              LOG_WARN("add row failed", K(ret));
            } else if (OB_ISNULL(new_stored_row)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("add stored row failed", K(ret));
            } else {
              row_list.end_ += 1;
            }
          }
        }
      }
    }
  }
  LOG_DEBUG("end get next equal group, need store match", K(cur_idx_),
            K(row_list), K(datum_store_.get_row_cnt()), K(is_unique));
  return ret;
}

int ObMergeJoinOp::ChildBatchFetcher::get_list_row(int64_t idx, ObRADatumStore::StoredRow *&stored_row)
{
  int ret = OB_SUCCESS;
  const ObRADatumStore::StoredRow *row = NULL;
  if (OB_FAIL(datum_store_.get_row(idx, row))) {
    LOG_WARN("fail to get row from ra datum store", K(ret), K(idx));
  } else if (OB_ISNULL(row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null row from list", K(ret), K(idx), K(lbt()));
  } else {
    stored_row = const_cast<ObRADatumStore::StoredRow*>(row);
  }
  return ret;
}

// calc equal conds with specified left_fechter batch_idx and right_fechter batch_idx
int ObMergeJoinOp::calc_equal_conds_with_batch_idx(int64_t &cmp_res)
{
  int ret = OB_SUCCESS;
  cmp_res = 0;
  int64_t l_table_batch_idx = left_brs_fetcher_.cur_idx_;
  int64_t r_table_batch_idx = right_brs_fetcher_.cur_idx_;
  for (int64_t i = 0;
       OB_SUCC(ret) && 0 == cmp_res && i < MY_SPEC.equal_cond_infos_.count();
       i++) {
    const ObMergeJoinSpec::EqualConditionInfo &equal_cond = MY_SPEC.equal_cond_infos_.at(i);
    // Preformance critical: NO null ptr check for equal condition children
    ObExpr *l_child_expr = equal_cond.is_opposite_ ? equal_cond.expr_->args_[1]
                                             : equal_cond.expr_->args_[0];
    ObExpr *r_child_expr = equal_cond.is_opposite_ ? equal_cond.expr_->args_[0]
                                             : equal_cond.expr_->args_[1];
    ObDatum *l_datum = &l_child_expr->locate_expr_datum(eval_ctx_, l_table_batch_idx);
    ObDatum *r_datum = &r_child_expr->locate_expr_datum(eval_ctx_, r_table_batch_idx);
    if (l_datum->is_null() && r_datum->is_null()) {
      cmp_res = (T_OP_NSEQ == equal_cond.expr_->type_) ? 0 : -1;
    } else {
      int cmp_ret = 0;
      if (OB_FAIL(equal_cond.ns_cmp_func_(*l_datum, *r_datum, cmp_ret))) {
        LOG_WARN("failed to compare", K(ret));
      } else if (cmp_ret != 0) {
        cmp_res = cmp_ret;
        cmp_res *= MY_SPEC.merge_directions_.at(i);
      }
    }
    LOG_DEBUG("calc equal cond with batch idx", KPC(l_datum), KPC(r_datum));

  } // for end
  LOG_DEBUG("calc equal cond with batch idx", K(l_table_batch_idx), K(r_table_batch_idx),
            K(cmp_res), K(left_brs_fetcher_.brs_), K(right_brs_fetcher_.brs_));
  return ret;
}

// compare left/right row in batch with right/left stored row
template<bool is_left_table_stored_row>
int ObMergeJoinOp::calc_equal_conds_with_stored_row(const ObRADatumStore::StoredRow *stored_row,
                                                    int64_t batch_idx,
                                                    int64_t &cmp_res)
{
  int ret = OB_SUCCESS;
  common::ObFixedArray<int64_t, common::ObIAllocator> &equal_param_idx =
                                    is_left_table_stored_row ? left_brs_fetcher_.equal_param_idx_
                                      : right_brs_fetcher_.equal_param_idx_;
  for (int64_t i = 0;
       OB_SUCC(ret) && 0 == cmp_res && i < MY_SPEC.equal_cond_infos_.count();
       i++) {
    const ObMergeJoinSpec::EqualConditionInfo &equal_cond = MY_SPEC.equal_cond_infos_.at(i);
    // Preformance critical: NO null ptr check for equal condition children
    ObExpr *l_child_expr = equal_cond.is_opposite_ ? equal_cond.expr_->args_[1]
                                                  : equal_cond.expr_->args_[0];
    ObExpr *r_child_expr = equal_cond.is_opposite_ ? equal_cond.expr_->args_[0]
        : equal_cond.expr_->args_[1];
    ObDatum *l_table_datum = nullptr;
    ObDatum *r_table_datum = nullptr;

    if (is_left_table_stored_row) {
      l_table_datum = const_cast<ObDatum *>(&stored_row->cells()[equal_param_idx.at(i)]);
      r_table_datum = &r_child_expr->locate_expr_datum(eval_ctx_, batch_idx);
    } else {
      l_table_datum = &l_child_expr->locate_expr_datum(eval_ctx_, batch_idx);
      r_table_datum = const_cast<ObDatum *>(&stored_row->cells()[equal_param_idx.at(i)]);
    }
    if (OB_SUCC(ret)) {
    if (l_table_datum->is_null() && r_table_datum->is_null()) {
      cmp_res = (T_OP_NSEQ == equal_cond.expr_->type_) ? 0 : -1;
    } else {
      int cmp_ret = 0;
      if (OB_FAIL(equal_cond.ns_cmp_func_(*l_table_datum, *r_table_datum, cmp_ret))) {
        LOG_WARN("failed to compare", K(ret));
      } else if (cmp_ret != 0) {
        cmp_res = cmp_ret;
        cmp_res *= MY_SPEC.merge_directions_.at(i);
      }
    }
    }
  } // for end
  LOG_DEBUG("calc equal cond with stored row", K(is_left_table_stored_row), K(batch_idx),
            K(cmp_res), K(left_brs_fetcher_.brs_), K(right_brs_fetcher_.brs_));
  return ret;
}

int ObMergeJoinOp::batch_join_begin()
{
  int ret = OB_SUCCESS;
  bool got_next_batch = false;
  if (OB_FAIL(left_brs_fetcher_.get_next_nonskip_row(got_next_batch))) {
    LOG_WARN("get left batch failed", K(ret));
  } else if (OB_UNLIKELY(left_brs_fetcher_.iter_end())) {
    batch_join_state_ = need_right_join() ? BJS_OUTPUT_RIGHT : BJS_JOIN_END;
  } else if (OB_FAIL(right_brs_fetcher_.get_next_nonskip_row(got_next_batch))) {
    LOG_WARN("get right next batch failed", K(ret));
  } else if (right_brs_fetcher_.iter_end()) {
    if (need_store_left_unmatch_rows()) {
      if (OB_FAIL(left_brs_fetcher_.backup_remain_rows())) {
        LOG_WARN("backup remain rows failed", K(ret));
      } else if (OB_FAIL(left_brs_fetcher_.brs_holder_.save(MY_SPEC.max_batch_size_))) {
        LOG_WARN("failed to backup left expr datum", K(ret));
      } else {
        batch_join_state_ = BJS_OUTPUT_LEFT;
      }
    } else {
      batch_join_state_ = BJS_JOIN_END;
    }
  }
  if (OB_SUCC(ret) && OB_LIKELY(BJS_JOIN_BEGIN == batch_join_state_)) {
    if (OB_FAIL(calc_equal_conds_with_batch_idx(cmp_res_))) {
      LOG_WARN("calc equal cond with batch index failed", K(ret));
    } else {
      batch_join_state_ = BJS_JOIN_BOTH;
    }
  }
  return ret;
}

int ObMergeJoinOp::store_group_first_row(
    ChildBatchFetcher &child_fetcher, JoinRowList &row_list,
    ObRADatumStore::StoredRow *&stored_row,
    ObEvalCtx::BatchInfoScopeGuard &guard)
{
  int ret = OB_SUCCESS;
  ObRADatumStore::StoredRow *res_row = NULL;
  guard.set_batch_idx(child_fetcher.cur_idx_);
  if (OB_FAIL(child_fetcher.datum_store_.add_row(*child_fetcher.all_exprs_,
                                                    &eval_ctx_, &res_row))) {
    LOG_WARN("add row failed", K(ret));
  } else if (OB_ISNULL(res_row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("add stored row failed", K(ret));
  } else {
    row_list.end_ += 1;
    stored_row = res_row;
  }
  return ret;
}

// when left and right current row match, iterate both children and put <left_group, right_group> in match_groups_
int ObMergeJoinOp::iterate_both_chidren(ObEvalCtx::BatchInfoScopeGuard &guard)
{
  int ret = OB_SUCCESS;
  ObRADatumStore *l_store = &left_brs_fetcher_.datum_store_;
  ObRADatumStore *r_store = &right_brs_fetcher_.datum_store_;
  JoinRowList l_row_list(l_store->get_row_cnt());
  JoinRowList r_row_list(r_store->get_row_cnt());
  ObRADatumStore::StoredRow *l_row = NULL;
  ObRADatumStore::StoredRow *r_row = NULL;
  bool updated = false;
  if (OB_FAIL(sql_mem_processor_.update_max_available_mem_size_periodically(
              &mem_context_->get_malloc_allocator(),
              [&](int64_t cur_cnt) { return get_total_rows_in_datum_store() > cur_cnt; },
              updated))) {
    LOG_WARN("failed to update max available memory size periodically", K(ret));
  }
  int64_t t_mem_bound = sql_mem_processor_.get_mem_bound();
  int64_t l_mem_bound = static_cast<int64_t>(left_mem_bound_ratio_ * t_mem_bound);
  int64_t r_mem_bound = 0; // will be set to the remaining memory size later.
  l_store->set_mem_limit(l_mem_bound);
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(store_group_first_row(left_brs_fetcher_, l_row_list, l_row, guard))) {
    LOG_WARN("store left group first row failed", K(ret));
  } else if (OB_FAIL(store_group_first_row(right_brs_fetcher_, r_row_list, r_row, guard))) {
    LOG_WARN("store right group first row failed", K(ret));
  } else if (OB_FAIL((left_brs_fetcher_.get_next_equal_group<true>(l_row_list, r_row,
                                                                   MY_SPEC.is_left_unique_,
                                                                   l_row)))) {
    LOG_WARN("get left next group failed", K(ret));
  } else if (OB_UNLIKELY((r_mem_bound = t_mem_bound - l_store->get_mem_hold()) < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected negative mem bound", K(ret), K(t_mem_bound), K(l_mem_bound),
                                              K(r_mem_bound), K(l_store->get_mem_hold()));
  } else if (FALSE_IT(r_store->set_mem_limit(r_mem_bound))) {
  } else if (OB_FAIL((right_brs_fetcher_.get_next_equal_group<false>(r_row_list, l_row, false,
                                                                     r_row)))) {
    LOG_WARN("get right next group failed", K(ret));
  } else if (OB_FAIL(match_groups_.push_back(std::make_pair(l_row_list, r_row_list)))) {
    LOG_WARN("match group push back failed", K(ret));
  } else if (!left_brs_fetcher_.iter_end() && !right_brs_fetcher_.iter_end()) {
    if (OB_FAIL(calc_equal_conds_with_batch_idx(cmp_res_))) {
      LOG_WARN("calc equal cond with batch index failed", K(ret));
    }
  }
  return ret;
}

// both child not end
int ObMergeJoinOp::batch_join_both()
{
  int ret = OB_SUCCESS;
  left_brs_fetcher_.datum_store_.reuse();
  right_brs_fetcher_.datum_store_.reuse();
  LOG_DEBUG("start batch join both, restore both holders");
  if (OB_FAIL(left_brs_fetcher_.brs_holder_.restore())) {
    LOG_WARN("restore left holder failed", K(ret));
  } else if (OB_FAIL(right_brs_fetcher_.brs_holder_.restore())) {
    LOG_WARN("restore right holder failed", K(ret));
  } else {
    LOG_DEBUG("before iterate both sides", K(cmp_res_),
              K(left_brs_fetcher_.datum_store_.get_row_cnt()),
              K(right_brs_fetcher_.datum_store_.get_row_cnt()), K(match_groups_.count()));
    // Routine 'iterate_both_chidren()' would modify eval_ctx_.batch_idx_.
    // It is too heavy to add guard inside 'iterate_both_chidren()' which is row
    // iteration, therefore add the guard here.
    ObEvalCtx::BatchInfoScopeGuard guard(eval_ctx_);
    // iterator both children until one of them is end or we have got enough rows in datum store
    while (!left_brs_fetcher_.iter_end() && !right_brs_fetcher_.iter_end()
           && !has_enough_datums() && OB_SUCC(ret)) {
      if (cmp_res_ < 0) {
        if (need_store_left_unmatch_rows()) {
          if (OB_FAIL((left_brs_fetcher_.get_next_small_group<true, true>(cmp_res_)))) {
            LOG_WARN("get left next group failed", K(ret));
          }
        } else if (OB_FAIL((left_brs_fetcher_.get_next_small_group<false, true>(cmp_res_)))) {
          LOG_WARN("get left next group failed", K(ret));
        }
      } else if (cmp_res_ > 0) {
        if (need_right_join()) {
          if (OB_FAIL((right_brs_fetcher_.get_next_small_group<true, false>(cmp_res_)))) {
            LOG_WARN("get right next group failed", K(ret));
          }
        } else if (OB_FAIL((right_brs_fetcher_.get_next_small_group<false, false>(cmp_res_)))) {
          LOG_WARN("get right next group failed", K(ret));
        }
      } else if (OB_FAIL(iterate_both_chidren(guard))) {
        LOG_WARN("iterate both children failed", K(ret));
      }
      LOG_DEBUG("end iterate both side", K(cmp_res_),
              K(left_brs_fetcher_.datum_store_.get_row_cnt()),
              K(right_brs_fetcher_.datum_store_.get_row_cnt()), K(match_groups_.count()));
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(left_brs_fetcher_.datum_store_.finish_add_row())) {
        LOG_WARN("left datum store finish add row failed", K(ret));
      } else if (OB_FAIL(right_brs_fetcher_.datum_store_.finish_add_row())) {
        LOG_WARN("right datum store finish add row failed", K(ret));
      } else if (OB_UNLIKELY(left_brs_fetcher_.iter_end() && !right_brs_fetcher_.iter_end())) {
        if (OB_FAIL(right_brs_fetcher_.backup_remain_rows())) {
          LOG_WARN("backup right remain rows failed", K(ret));
        }
      } else if (OB_UNLIKELY(!left_brs_fetcher_.iter_end() && right_brs_fetcher_.iter_end())) {
        if (OB_FAIL(left_brs_fetcher_.backup_remain_rows())) {
          LOG_WARN("backup left remain rows failed", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      bool has_next = false;
      if (OB_FAIL(next_match_group(has_next))) {
        LOG_WARN("fail to get next match group", K(ret));
      } else if (has_next) {
        batch_join_state_ = BJS_MATCH_GROUP;
        left_brs_fetcher_.brs_holder_.save(MY_SPEC.max_batch_size_);
        right_brs_fetcher_.brs_holder_.save(MY_SPEC.max_batch_size_);
      } else {
        switch_state_if_reach_end(true);
      }
    }
  }
  return ret;
}

int ObMergeJoinOp::match_group_rows(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  ObJoinType join_type = MY_SPEC.join_type_;
  switch (join_type) {
    case INNER_JOIN :
      ret = match_group_rows<false, false, INNER_JOIN>(max_row_cnt);
      break;
    case LEFT_SEMI_JOIN :
      ret = match_group_rows<false, true, LEFT_SEMI_JOIN>(max_row_cnt);
      break;
    case LEFT_ANTI_JOIN :
      ret = match_group_rows<false, true, LEFT_ANTI_JOIN>(max_row_cnt);
      break;
    case LEFT_OUTER_JOIN :
      ret = match_group_rows<false, true, LEFT_OUTER_JOIN>(max_row_cnt);
      break;
    case RIGHT_OUTER_JOIN :
      ret = match_group_rows<true, false, RIGHT_OUTER_JOIN>(max_row_cnt);
      break;
    case FULL_OUTER_JOIN :
      ret = match_group_rows<true, true, FULL_OUTER_JOIN>(max_row_cnt);
      break;
    default : {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected join type", K(ret), K(join_type));
    }
  }
  if (OB_SUCC(ret) && output_cache_.count() > 0) {
    batch_join_state_ = BJS_OUTPUT_STORE;
  } else {
    switch_state_if_reach_end(false);
  }
  return ret;
}

// eval other conditions for rows in matched_groups, stored rows in output_cache_ if conds are true.
template<bool left_empty_allowed, bool right_empty_allowed, ObJoinType join_type>
int ObMergeJoinOp::match_group_rows(const int64_t max_row_cnt)
{
  LOG_DEBUG("start match group rows", K(left_empty_allowed), K(right_empty_allowed),
              K(match_groups_.count()), K(output_cache_.count()));
  int ret = OB_SUCCESS;
  // put params of other conds always in the first row of batch, so set batch_idx to 0
  // and batch_size to 1 here.
  ObEvalCtx::BatchInfoScopeGuard batch_info_guard(eval_ctx_);
  batch_info_guard.set_batch_idx(0);
  batch_info_guard.set_batch_size(1);
  const ExprFixedArray &left_output = left_->get_spec().output_;
  const ExprFixedArray &right_output = right_->get_spec().output_;
  const int64_t batch_size = MIN(max_row_cnt, MY_SPEC.max_batch_size_);
  bool has_next = true;
  // before matching group, re-project current left row to other conds, because the memory of the
  // last projection may have expired.
  if (OB_FAIL(left_row_to_other_conds())) {
    LOG_WARN("fail to project row to left other conds exprs", K(ret));
  }
  while (OB_SUCC(ret) && output_cache_.count() < batch_size && has_next) {
    /*
     * 1. left list is empty，only in full outer join，no matched right rows.
     * 2. right list is empty，in left outer/anti join or full outer join，no matched left rows.
     * 3，both lists are not empty，then calculate other conds and fill output_cache_。
     */
    bool got_row = false;
    int64_t left_output_idx = -1; // -1 means null if got row
    int64_t right_output_idx = -1;
    if (left_empty_allowed && left_group_.empty() && right_group_.has_next()) {
      right_output_idx = right_group_.cur_++;
      got_row = true;
    } else if (right_empty_allowed && right_group_.empty() && left_group_.has_next()) {
      left_output_idx = left_group_.cur_++;
      got_row = true;
    } else if (is_last_right_join_output_) {
      // for right/full join, it is a process of traversing the right branch data to check whether
      // it has been matched. it should output [-1, r_idx] when the right row has not been matched
      // by the row from left.
      while (OB_SUCC(ret) && right_group_.has_next() && !got_row) {
        const int64_t r_idx = right_group_.cur_++;
        if (!is_match(r_idx - right_group_.start_)) {
          right_output_idx = r_idx;
          got_row = true;
        }
      }
    } else {
      // calc other conds, then push back index pair to output cache according to is_match
      // and join type.
      bool is_match = false;
      ObRADatumStore::StoredRow *r_stored_row = NULL;
      while (OB_SUCC(ret) && right_group_.has_next() && !got_row) {
        const int64_t r_idx = right_group_.cur_++;
        if (MY_SPEC.other_join_conds_.count() > 0) {
          clear_evaluated_flag();
          if (OB_FAIL(right_brs_fetcher_.get_list_row(r_idx, r_stored_row))) {
            LOG_WARN("get row in list failed", K(ret));
          } else if (OB_FAIL(r_stored_row->to_expr(right_output, eval_ctx_))) {
            LOG_WARN("right datums to expr failed", K(ret));
          } else if (OB_FAIL(calc_other_conds(is_match))) {
            LOG_WARN("calc other conds failed", K(ret));
          }
        } else {
          is_match = true;
        }
        if (OB_SUCC(ret)) {
          if (INNER_JOIN != join_type && is_match) {
            left_row_matched_ = true;
            if (need_right_join()) {
              rj_match_vec_->set(r_idx - right_group_.start_);
            }
            if (LEFT_SEMI_JOIN == MY_SPEC.join_type_ || LEFT_ANTI_JOIN == MY_SPEC.join_type_) {
              right_group_.set_end();
            }
          }
          if (output_equal_rows_directly() && is_match) {
            left_output_idx = left_group_.cur_;
            right_output_idx = r_idx;
            got_row = true;
          }
        }
      }
      // right_group_ reach end
      if (OB_SUCC(ret) && !got_row) {
        if ((need_store_left_unmatch_rows() && !left_row_matched_) ||
               (LEFT_SEMI_JOIN == join_type && left_row_matched_)) {
          left_output_idx = left_group_.cur_;
          got_row = true;
        }
        ++left_group_.cur_;
        if (left_group_.has_next()) {
          left_row_matched_ = false;
          right_group_.rescan();
          if (OB_FAIL(left_row_to_other_conds())) {
            LOG_WARN("fail to project row to left other conds exprs", K(ret));
          }
        } else if (left_empty_allowed && MY_SPEC.other_join_conds_.count() > 0) {
          // do right join
          is_last_right_join_output_ = true;
          right_group_.rescan();
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (got_row && OB_FAIL(output_cache_.push_back(std::make_pair(left_output_idx, right_output_idx)))) {
        LOG_WARN("output cache push back failed", K(ret));
      } else if (!left_group_.has_next() && !right_group_.has_next() &&
                  OB_FAIL(next_match_group(has_next))) {
        LOG_WARN("fail to get next match group", K(ret));
      } else if (OB_FAIL(left_row_to_other_conds())) {
        LOG_WARN("fail to project row to left other conds exprs", K(ret));
      }
    }
  }
  clear_evaluated_flag();
  return ret;
}

int ObMergeJoinOp::output_cache_rows()
{
  int ret = OB_SUCCESS;
  ObJoinType join_type = MY_SPEC.join_type_;
  switch (join_type) {
    case INNER_JOIN :
    case LEFT_SEMI_JOIN :
    case LEFT_ANTI_JOIN :
      ret = output_cache_rows<false, false>();
      break;
    case LEFT_OUTER_JOIN :
      ret = output_cache_rows<false, true>();
      break;
    case RIGHT_OUTER_JOIN :
      ret = output_cache_rows<true, false>();
      break;
    case FULL_OUTER_JOIN :
      ret = output_cache_rows<true, true>();
      break;
    default : {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected join type", K(ret), K(join_type));
    }
  }
  return ret;
}

template <bool need_blank_left, bool need_blank_right>
int ObMergeJoinOp::output_cache_rows()
{
  int ret = OB_SUCCESS;
  ObEvalCtx::BatchInfoScopeGuard guard(eval_ctx_);
  ObRADatumStore::StoredRow *left_row = NULL;
  ObRADatumStore::StoredRow *right_row = NULL;
  LOG_DEBUG("start output rows in cache", K(output_cache_.count()));
  const ExprFixedArray &left_output = left_->get_spec().output_;
  const ExprFixedArray &right_output = right_->get_spec().output_;
  brs_.size_ = output_cache_.count();
  guard.set_batch_size(brs_.size_);
  set_output_it_age(&output_rows_it_age_);
  for (int64_t idx = 0; idx < output_cache_.count() && OB_SUCC(ret); ++idx) {
    guard.set_batch_idx(idx);
    RowsPair output = output_cache_.at(idx);
    int64_t l_idx = output.first;
    int64_t r_idx = output.second;
    if (-1 != l_idx) {
      if (OB_FAIL(left_brs_fetcher_.get_list_row(l_idx, left_row))) {
        LOG_WARN("fail to get left row from ra datum store", K(ret));
      } else if (OB_FAIL(left_row->to_expr(left_output, eval_ctx_))) {
        LOG_WARN("left row to expr failed", K(ret));
      }
    } else if (need_blank_left) {
      if (OB_FAIL(ObJoinOp::blank_row(left_output))) {
        LOG_WARN("blank left row failed", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (-1 != r_idx) {
      if (OB_FAIL(right_brs_fetcher_.get_list_row(r_idx, right_row))) {
        LOG_WARN("fail to get right row from ra datum store", K(ret));
      } else if (OB_FAIL(right_row->to_expr(right_output, eval_ctx_))) {
        LOG_WARN("right row to expr failed", K(ret));
      }
    } else if (need_blank_right) {
      if (OB_FAIL(ObJoinOp::blank_row(right_output))) {
        LOG_WARN("blank right row failed", K(ret));
      }
    }
  }
  set_output_it_age(NULL);
  output_cache_.reuse();
  if (group_idx_ >= match_groups_.count() && !left_group_.has_next() && !right_group_.has_next()) {
    match_groups_.reset();
    group_idx_ = 0;
    if (left_brs_fetcher_.iter_end()) {
      batch_join_state_ = (right_brs_fetcher_.iter_end() || !need_right_join())
                           ? BJS_JOIN_END : BJS_OUTPUT_RIGHT;
    } else if (right_brs_fetcher_.iter_end()) {
      batch_join_state_ = need_store_left_unmatch_rows() ? BJS_OUTPUT_LEFT : BJS_JOIN_END;
    } else {
      batch_join_state_ = BJS_JOIN_BOTH;
    }
  } else {
    batch_join_state_ = BJS_MATCH_GROUP;
  }

  LOG_DEBUG("output rows in cache", K(brs_.size_), K(output_cache_.count()), K(batch_join_state_),
              K(&left_brs_fetcher_), K(&right_brs_fetcher_));
  return ret;
}

bool ObMergeJoinOp::has_enough_datums()
{
  int64_t cur_datums_cnt = get_total_rows_in_datum_store();
  int64_t max_datums_cnt = MY_SPEC.max_batch_size_ * BATCH_MULTIPLE_TIMES;
  return cur_datums_cnt >= max_datums_cnt;
}

int ObMergeJoinOp::output_side_rows(ChildBatchFetcher &batch_fetcher,
                                    const ExprFixedArray *blank_exprs,
                                    const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(batch_fetcher.iter_end())) {
    batch_join_state_ = BJS_JOIN_END;
  } else if (OB_FAIL(batch_fetcher.brs_holder_.restore())) {
    LOG_WARN("fetcher restore failed", K(ret));
  } else if (FALSE_IT(clear_evaluated_flag())) {
  } else if (OB_FAIL(batch_fetcher.get_next_batch(max_row_cnt))) {
    LOG_WARN("get child next batch failed", K(ret));
  } else if (OB_UNLIKELY(batch_fetcher.iter_end())) {
    batch_join_state_ = BJS_JOIN_END;
    brs_.size_ = 0;
    brs_.end_ = true;
  } else {
    brs_.size_ = batch_fetcher.brs_.size_;
    brs_.end_ = batch_fetcher.brs_.end_;
    if (OB_ISNULL(batch_fetcher.brs_.skip_)) {
      brs_.skip_->reset(brs_.size_);
    } else {
      brs_.skip_->deep_copy(*batch_fetcher.brs_.skip_, brs_.size_);
    }
    if (NULL != blank_exprs) {
      ObEvalCtx::BatchInfoScopeGuard guard(eval_ctx_);
      for (int64_t i = 0; i < brs_.size_ && OB_SUCC(ret); i++) {
        if (!brs_.skip_->contain(i)) {
          guard.set_batch_idx(i);
          if (OB_FAIL(ObJoinOp::blank_row(*blank_exprs))) {
            LOG_WARN("blank row failed", K(ret), KPC(blank_exprs));
          }
        }
      }
    }
  }
  return ret;
}

// get next match group to join
int ObMergeJoinOp::next_match_group(bool &has_next)
{
  int ret = OB_SUCCESS;
  has_next = false;
  if (group_idx_ < match_groups_.count()) {
    RowsListPair rows_list_pair = match_groups_.at(group_idx_++);
    left_group_ = rows_list_pair.first;
    right_group_ = rows_list_pair.second;
    if (OB_UNLIKELY(left_group_.empty() && right_group_.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("group is empty", K(ret), K(group_idx_), K(match_groups_.count()),
                                 K(left_group_), K(right_group_));
    } else if (!left_group_.empty() && !right_group_.empty() && need_right_join()) {
      if (OB_FAIL(expand_match_flags_if_necessary(right_group_.count(), false))) {
        LOG_WARN("fail to expand right join match flags", K(ret), K(right_group_.count()));
      } else {
        // reset all flags to false
        rj_match_vec_->reset(right_group_.count());
      }
    }
    has_next = true;
  }
  left_row_matched_ = false;
  is_last_right_join_output_ = false;
  return ret;
}

int ObMergeJoinOp::expand_match_flags_if_necessary(const int64_t size, const bool copy_flags)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(size > rj_match_vec_size_)) {
    ObIAllocator &allocator = ctx_.get_allocator();
    if (OB_ISNULL(rj_match_vec_)) {
      rj_match_vec_size_ = std::max(static_cast<int64_t>(ObBitVector::WORD_BITS), next_pow2(size));
      const int64_t mem_size = ObBitVector::memory_size(rj_match_vec_size_);
      if (OB_ISNULL(rj_match_vec_ = to_bit_vector(allocator.alloc(mem_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate right join match flags failed", K(ret));
      } else {
        rj_match_vec_->reset(rj_match_vec_size_);
      }
    } else {
      ObBitVector *ori_vec = rj_match_vec_;
      const int64_t new_size = next_pow2(size);
      const int64_t mem_size = ObBitVector::memory_size(new_size);
      if (OB_ISNULL(rj_match_vec_ = to_bit_vector(allocator.alloc(mem_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate right join match flags failed", K(ret));
      } else {
        rj_match_vec_->reset(new_size);
        if (copy_flags) {
          rj_match_vec_->deep_copy(*ori_vec, rj_match_vec_size_);
        }
        rj_match_vec_size_ = new_size;
      }
      allocator.free(ori_vec);
    }
  }
  return ret;
}

void ObMergeJoinOp::switch_state_if_reach_end(const bool need_save_datum)
{
  if (OB_UNLIKELY(left_brs_fetcher_.iter_end())) {
    if (right_brs_fetcher_.iter_end() || !need_right_join()) {
      batch_join_state_ = BJS_JOIN_END;
    } else {
      batch_join_state_ = BJS_OUTPUT_RIGHT;
      if (need_save_datum) {
        right_brs_fetcher_.brs_holder_.save(MY_SPEC.max_batch_size_);
      }
    }
  } else if (OB_UNLIKELY(right_brs_fetcher_.iter_end())) {
    if (!need_store_left_unmatch_rows()) {
      batch_join_state_ = BJS_JOIN_END;
    } else {
      batch_join_state_ = BJS_OUTPUT_LEFT;
      if (need_save_datum) {
        left_brs_fetcher_.brs_holder_.save(MY_SPEC.max_batch_size_);
      }
    }
  }
}

int ObMergeJoinOp::left_row_to_other_conds()
{
  int ret = OB_SUCCESS;
  if (MY_SPEC.other_join_conds_.count() > 0 && left_group_.has_next()) {
    // make sure batch_size and batch_index here are 1 and 0 respectively.
    ObRADatumStore::StoredRow *l_stored_row = NULL;
    if (OB_FAIL(left_brs_fetcher_.get_list_row(left_group_.cur_, l_stored_row))) {
      LOG_WARN("fail to get row from list", K(ret));
    } else if (OB_FAIL(l_stored_row->to_expr(left_->get_spec().output_, eval_ctx_))) {
      LOG_WARN("left datums to expr failed", K(ret));
    }
  }
  return ret;
}

int ObMergeJoinOp::inner_get_next_batch(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  if (OB_UNLIKELY(BJS_JOIN_BEGIN == batch_join_state_) && OB_FAIL(batch_join_begin())) {
    LOG_WARN("batch join begin failed", K(ret));
  } else {
    while (OB_SUCC(ret) && (BJS_MATCH_GROUP == batch_join_state_ ||
                             BJS_JOIN_BOTH == batch_join_state_)) {
      while (OB_SUCC(ret) && BJS_JOIN_BOTH == batch_join_state_) {
        // get rows from left and right children. put them in output_cache if conds are satisfied.
        // if output_cache is empty, keep in this loop and call batch_join_both.
        if (OB_FAIL(batch_join_both())) {
          LOG_WARN("batch join both failed", K(ret));
        }
      }

      // get join result by comparing other conds and considering different join types.
      // `match_group_rows()` return at most max_batch_size results and put them in `output_cache`.
      if (OB_SUCC(ret) && BJS_MATCH_GROUP == batch_join_state_ &&
            OB_FAIL(match_group_rows(max_row_cnt))) {
        LOG_WARN("fail to match group rows", K(ret));
      } else if (OB_UNLIKELY(BJS_MATCH_GROUP == batch_join_state_)) {
        // join result for the match_groups is empty and both fetcher still have data to join,
        // should do join both again to produce new match groups.
        batch_join_state_ = BJS_JOIN_BOTH;
      }
    }
    // ready to output
    if (OB_SUCC(ret)) {
      switch (batch_join_state_) {
        case BJS_OUTPUT_STORE : {
          if (OB_FAIL(output_cache_rows())) {
            LOG_WARN("output cache rows failed", K(ret));
          }
          break;
        }
        case BJS_OUTPUT_LEFT : {
          bool need_blank_right = need_left_join();
          if (OB_FAIL(output_side_rows(left_brs_fetcher_,
                                       need_blank_right ? &right_->get_spec().output_ : NULL,
                                       max_row_cnt))) {
            LOG_WARN("output left side rows failed", K(ret));
          }
          break;
        }
        case BJS_OUTPUT_RIGHT : {
          bool need_blank_left = need_right_join();
          if (OB_FAIL(output_side_rows(right_brs_fetcher_,
                                       need_blank_left ? &left_->get_spec().output_ : NULL,
                                       max_row_cnt))) {
            LOG_WARN("output right side rows failed", K(ret));
          }
          break;
        }
        case BJS_JOIN_END : {
          brs_.size_ = 0;
          brs_.end_ = true;
          break;
        }
        default : {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid batch join state", K(batch_join_state_));
          break;
        }
      }
    }
  }
  // Expr when matching group rows use batch_size as 1 for evaluation. When it is shared expr and
  // eval again as output expr, it will cause core dump because batch_size in output expr is set to
  // a value greater than 1. So we clear evaluated flag here to make shared expr in output can
  // evaluated normally.
  clear_evaluated_flag();
  return ret;
}

} // end namespace sql
} // end namespace oceanbase
