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

#include "sql/engine/join/ob_block_based_nested_loop_join.h"
#include "lib/utility/ob_tracepoint.h"
#include "lib/hash/ob_hashset.h"
#include "common/rowkey/ob_rowkey.h"
#include "sql/engine/table/ob_table_scan.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase {
namespace sql {
using namespace common;

// REGISTER_PHY_OPERATOR(ObBLKNestedLoopJoin, PHY_BLOCK_BASED_NESTED_LOOP_JOIN);

ObBLKNestedLoopJoin::ObBLKNestedLoopJoin(common::ObIAllocator& alloc) : ObBasicNestedLoopJoin(alloc)
{
  // init left table cache ^ memory_limit. goto iter_end or iter_going
  state_operation_func_[JS_JOIN_BEGIN] = &ObBLKNestedLoopJoin::join_begin_operate;
  // switch state to left_join
  state_function_func_[JS_JOIN_BEGIN][FT_ITER_GOING] = &ObBLKNestedLoopJoin::join_begin_func_going;
  // switch state to end
  state_function_func_[JS_JOIN_BEGIN][FT_ITER_END] = &ObBLKNestedLoopJoin::join_begin_func_end;

  // read row from left table.
  // if ITER_END and no more data in cache, goto iter_end, otherwise goto iter_going
  state_operation_func_[JS_JOIN_LEFT_JOIN] = &ObBLKNestedLoopJoin::join_left_operate;
  // cache left table row to cache. if cache not full and left table still has data,
  // continue left_join, otherwise goto right_join
  state_function_func_[JS_JOIN_LEFT_JOIN][FT_ITER_GOING] = &ObBLKNestedLoopJoin::join_left_func_going;
  // clean left table cache, goto end
  state_function_func_[JS_JOIN_LEFT_JOIN][FT_ITER_END] = &ObBLKNestedLoopJoin::join_left_func_end;

  // read row from right table. goto iter_end if no more data, otherwise goto iter_going
  state_operation_func_[JS_JOIN_RIGHT_JOIN] = &ObBLKNestedLoopJoin::join_right_operate;
  // clear left table cache flag, goto read_cache
  state_function_func_[JS_JOIN_RIGHT_JOIN][FT_ITER_GOING] = &ObBLKNestedLoopJoin::join_right_func_going;
  // check if outter join. if not, clear left table cache, goto left_join, otherwise goto outer_join
  state_function_func_[JS_JOIN_RIGHT_JOIN][FT_ITER_END] = &ObBLKNestedLoopJoin::join_right_func_end;

  // read a row from cache. if no more data, goto iter_end, otherwise goto iter_going
  state_operation_func_[JS_JOIN_READ_CACHE] = &ObBLKNestedLoopJoin::join_read_cache_operate;
  // join a row, goto read_cache
  state_function_func_[JS_JOIN_READ_CACHE][FT_ITER_GOING] = &ObBLKNestedLoopJoin::join_read_cache_func_going;
  // clear cache, goto left_join
  state_function_func_[JS_JOIN_READ_CACHE][FT_ITER_END] = &ObBLKNestedLoopJoin::join_read_cache_func_end;

  // read a from from cache. if no more data, goto iter_end, otherwise goto iter_going
  state_operation_func_[JS_JOIN_OUTER_JOIN] = &ObBLKNestedLoopJoin::join_outer_join_operate;
  // if row not joined, continue outer_join
  state_function_func_[JS_JOIN_OUTER_JOIN][FT_ITER_GOING] = &ObBLKNestedLoopJoin::join_outer_join_func_going;
  // clear cache and flags, goto left_join
  state_function_func_[JS_JOIN_OUTER_JOIN][FT_ITER_END] = &ObBLKNestedLoopJoin::join_outer_join_func_end;

  // do nothing, goto iter_end
  state_operation_func_[JS_JOIN_END] = &ObBLKNestedLoopJoin::join_end_operate;
  state_function_func_[JS_JOIN_END][FT_ITER_GOING] = NULL;
  // return iter_end
  state_function_func_[JS_JOIN_END][FT_ITER_END] = &ObBLKNestedLoopJoin::join_end_func_end;
}

ObBLKNestedLoopJoin::~ObBLKNestedLoopJoin()
{}

void ObBLKNestedLoopJoin::reset()
{
  ObBasicNestedLoopJoin::reset();
}

void ObBLKNestedLoopJoin::resuse()
{
  ObBasicNestedLoopJoin::reuse();
}

int ObBLKNestedLoopJoin::inner_open(ObExecContext& exec_ctx) const
{
  int ret = OB_SUCCESS;
  ObBLKNestedLoopJoinCtx* join_ctx = NULL;
  if (NULL == my_phy_plan_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("physical plan should not be null", K(ret));
  } else if (OB_FAIL(E(EventTable::EN_1) ObBasicNestedLoopJoin::inner_open(exec_ctx))) {
    LOG_WARN("failed to open in base class", K(ret));
  } else if (OB_I(t5)(NULL == (join_ctx = GET_PHY_OPERATOR_CTX(ObBLKNestedLoopJoinCtx, exec_ctx, get_id())))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get nested loop join ctx", K(ret));
  } else if (NULL == left_op_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("left op is null", K(ret));
  } else {
    if (OB_FAIL(OB_I(t9) join_ctx->alloc_row_cells(left_op_->get_column_count(), join_ctx->left_cache_row_buf_))) {
      LOG_WARN("failed to create right cache row buf in nested loop join ctx", K(ret));
    } else {
      join_ctx->left_cache_row_buf_.projector_ = const_cast<int32_t*>(left_op_->get_projector());
      join_ctx->left_cache_row_buf_.projector_size_ = left_op_->get_projector_size();
      join_ctx->expr_ctx_.phy_plan_ctx_ = exec_ctx.get_physical_plan_ctx();
      join_ctx->expr_ctx_.calc_buf_ = &join_ctx->get_calc_buf();
      join_ctx->set_left_end_flag(false);
    }
  }
  return ret;
}

int ObBLKNestedLoopJoin::rescan(ObExecContext& exec_ctx) const
{
  int ret = OB_SUCCESS;
  ObBLKNestedLoopJoinCtx* join_ctx = NULL;
  if (OB_I(t2)(NULL == (join_ctx = GET_PHY_OPERATOR_CTX(ObBLKNestedLoopJoinCtx, exec_ctx, get_id())))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get nested loop join ctx", K(ret));
  } else {
    join_ctx->reset();
    ret = ObBasicNestedLoopJoin::rescan(exec_ctx);
  }
  return ret;
}

int ObBLKNestedLoopJoin::inner_get_next_row(ObExecContext& exec_ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ObBLKNestedLoopJoinCtx* join_ctx = NULL;
  if (OB_I(t1)(NULL == (join_ctx = GET_PHY_OPERATOR_CTX(ObBLKNestedLoopJoinCtx, exec_ctx, get_id())))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get nested loop join ctx", K(ret));
  } else {
    state_operation_func_type state_operation = NULL;
    state_function_func_type state_function = NULL;
    int func = -1;
    row = NULL;
    ObJoinState& state = join_ctx->state_;
    while (OB_SUCC(ret) && NULL == row) {
      state_operation = ObBLKNestedLoopJoin::state_operation_func_[state];
      if (NULL == state_operation) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get state operation", K(ret));
      } else {
        if (OB_ITER_END == (ret = OB_I(t3)(this->*state_operation)(*join_ctx))) {
          func = FT_ITER_END;
          ret = OB_SUCCESS;
        } else if (OB_FAIL(ret)) {
          LOG_WARN("failed state operation", K(state), K(ret));
        } else {
          func = FT_ITER_GOING;
        }
        if (OB_SUCC(ret)) {
          state_function = ObBLKNestedLoopJoin::state_function_func_[state][func];
          if (NULL == state_function) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to get state function", K(ret), K(state), K(func));
          } else {
            row = NULL;
            if (OB_FAIL(OB_I(t5)(this->*state_function)(*join_ctx, row)) && OB_ITER_END != ret) {
              LOG_WARN("failed state function", K(ret), K(state), K(func));
            }
          }
        }
      }
    }
  }

  return ret;
}

int ObBLKNestedLoopJoin::inner_create_operator_ctx(ObExecContext& exec_ctx, ObPhyOperatorCtx*& op_ctx) const
{
  int ret = OB_SUCCESS;
  ObBLKNestedLoopJoinCtx* join_ctx = NULL;
  if (OB_FAIL(OB_I(t1) CREATE_PHY_OPERATOR_CTX(ObBLKNestedLoopJoinCtx, exec_ctx, get_id(), get_type(), join_ctx))) {
    LOG_WARN("failed to create nested loop join ctx", K(ret));
  } else {
    op_ctx = join_ctx;
  }
  return ret;
}

int ObBLKNestedLoopJoin::join_end_operate(ObBLKNestedLoopJoinCtx& join_ctx) const
{
  UNUSED(join_ctx);
  return OB_ITER_END;
}

int ObBLKNestedLoopJoin::join_end_func_end(ObBLKNestedLoopJoinCtx& join_ctx, const ObNewRow*& row) const
{
  UNUSED(join_ctx);
  UNUSED(row);
  return OB_ITER_END;
}

int ObBLKNestedLoopJoin::join_begin_operate(ObBLKNestedLoopJoinCtx& join_ctx) const
{
  int ret = OB_SUCCESS;
  ObObj val;
  ObSQLSessionInfo* session_info = join_ctx.exec_ctx_.get_my_session();
  if (NULL == session_info) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("session is invalid", K(ret));
  } else if (OB_FAIL(session_info->get_sys_variable(share::SYS_VAR_OB_BNL_JOIN_CACHE_SIZE, val))) {
    LOG_WARN("Get sys variable error", K(ret));
  } else {
    if (0 != val.get_int()) {
      join_ctx.mem_limit_ = val.get_int();
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(LEFT_SEMI_JOIN == join_type_)) {
      join_ctx.bnl_jointype_ = BNL_SEMI_JOIN;
    } else if (OB_UNLIKELY(LEFT_ANTI_JOIN == join_type_)) {
      join_ctx.bnl_jointype_ = BNL_ANTI_JOIN;
    } else if (need_left_join()) {
      join_ctx.bnl_jointype_ = BNL_OUTER_JOIN;
    } else {
      join_ctx.bnl_jointype_ = BNL_INNER_JOIN;
    }
    if (join_ctx.is_left_end()) {
      ret = OB_ITER_END;
    }
  }
  return ret;
}

int ObBLKNestedLoopJoin::join_begin_func_going(ObBLKNestedLoopJoinCtx& join_ctx, const ObNewRow*& row) const
{
  UNUSED(row);
  int ret = OB_SUCCESS;
  join_ctx.state_ = JS_JOIN_LEFT_JOIN;
  return ret;
}

int ObBLKNestedLoopJoin::join_begin_func_end(ObBLKNestedLoopJoinCtx& join_ctx, const ObNewRow*& row) const
{
  UNUSED(row);
  int ret = OB_SUCCESS;
  join_ctx.state_ = JS_JOIN_END;
  return ret;
}

int ObBLKNestedLoopJoin::join_left_operate(ObBLKNestedLoopJoinCtx& join_ctx) const
{
  int ret = OB_SUCCESS;
  ObRowStore& left_cache = join_ctx.left_cache_;
  if (join_ctx.is_left_end()) {
    ret = OB_ITER_END;
    if (0 != left_cache.get_row_count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error", K(ret));
    }
  } else if (join_ctx.is_full()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", K(ret));
  } else {
    if (OB_FAIL(OB_I(t1) get_next_left_row(join_ctx)) && OB_ITER_END != ret) {
      LOG_WARN("failed to get next left row", K(ret));
    } else if (OB_ITER_END == ret) {
      join_ctx.set_left_end_flag(true);
      if (0 != left_cache.get_row_count()) {
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int ObBLKNestedLoopJoin::join_left_func_going(ObBLKNestedLoopJoinCtx& join_ctx, const ObNewRow*& row) const
{
  UNUSED(row);
  int ret = OB_SUCCESS;
  const ObNewRow* left_row = join_ctx.left_row_;
  ObRowStore& left_cache = join_ctx.left_cache_;
  const ObRowStore::StoredRow* stored_row = NULL;
  if (NULL == left_row) {
    if (!join_ctx.is_left_end()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error", K(ret));
    } else {
      if (0 == left_cache.get_row_count()) {
        join_ctx.state_ = JS_JOIN_LEFT_JOIN;
      } else {
        join_ctx.state_ = JS_JOIN_RIGHT_JOIN;
      }
    }
  } else {
    if (OB_FAIL(OB_I(t3) left_cache.add_row(*left_row, stored_row, 0, false))) {
      LOG_WARN("failed to add left row to cache", K(ret), K(left_row));
    } else {
      if (join_ctx.is_full()) {
        join_ctx.left_cache_index_ = 0;
        join_ctx.state_ = JS_JOIN_RIGHT_JOIN;
      } else {
        join_ctx.state_ = JS_JOIN_LEFT_JOIN;
      }
    }
  }

  if (OB_SUCCESS == ret && JS_JOIN_RIGHT_JOIN == join_ctx.state_) {
    if (OB_FAIL(save_left_row(join_ctx))) {
      LOG_WARN("failed to save left row", K(ret));
    } else if (OB_FAIL(join_ctx.init_left_has_joined(join_ctx.left_cache_.get_row_count()))) {
      LOG_WARN("init array failed", K(ret));
    } else if (OB_FAIL(init_right_child(join_ctx))) {
      LOG_WARN("init right child failed", K(ret));
    }
  }
  return ret;
}

int ObBLKNestedLoopJoin::join_left_func_end(ObBLKNestedLoopJoinCtx& join_ctx, const ObNewRow*& row) const
{
  UNUSED(row);
  int ret = OB_SUCCESS;
  join_ctx.left_cache_.clear_rows();
  join_ctx.state_ = JS_JOIN_END;
  return ret;
}

int ObBLKNestedLoopJoin::init_right_child(ObBLKNestedLoopJoinCtx& join_ctx) const
{
  int ret = OB_SUCCESS;
  ObExecContext& exec_ctx = join_ctx.exec_ctx_;
  if (OB_ISNULL(right_op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("right op is null", K(ret));
  } else if (PHY_TABLE_SCAN == right_op_->get_type() && rescan_params_.count() > 0) {
    ObTableScan* right_table_scan = static_cast<ObTableScan*>(right_op_);
    if (OB_FAIL(right_table_scan->reset_query_range(exec_ctx))) {
      LOG_WARN("failed to reset query range of right child", K(ret));
    } else {
      join_ctx.left_cache_iter_ = join_ctx.left_cache_.begin();
      hash::ObHashSet<ParamaterWrapper> distinct_param_set;
      const int64_t BUCKET_SIZE = 64;
      if (OB_FAIL(distinct_param_set.create(BUCKET_SIZE))) {
        LOG_WARN("fail to init hash set with bucket size", K(ret));
      } else {
        while (OB_SUCC(ret)) {
          ParamaterWrapper params;
          if (OB_FAIL(read_row_from_left_cache(join_ctx))) {
            if (OB_ITER_END != ret) {
              LOG_WARN("failed to read left row from cache", K(ret));
            }
          } else if (OB_FAIL(prepare_rescan_params(join_ctx))) {
            LOG_WARN("failed to prepare rescan params", K(ret));
          } else if (OB_FAIL(make_paramater_warpper(join_ctx, params))) {
            LOG_WARN("failed to compute hash value", K(ret));
          } else if (OB_HASH_NOT_EXIST == (ret = distinct_param_set.exist_refactored(params))) {
            if (OB_FAIL(right_table_scan->add_query_range(exec_ctx))) {
              LOG_WARN("failed to add query range", K(ret));
            } else if (OB_FAIL(distinct_param_set.set_refactored(params))) {
              LOG_WARN("failed to add value into hash set", K(ret));
            } else {
              ret = OB_SUCCESS;
            }
          } else if (OB_HASH_EXIST != ret) {
            LOG_WARN("failed to check hash set exist", K(ret));
          } else {
            ret = OB_SUCCESS;
          }
        }
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          if (OB_FAIL(right_table_scan->rescan_after_adding_query_range(exec_ctx))) {
            LOG_WARN("failed to rescan right child after extracting query range", K(ret));
          }
        } else {
          LOG_WARN("failed to extract query range for right child", K(ret));
        }
      }
    }
  } else {
    if (OB_FAIL(OB_I(t3) right_op_->rescan(exec_ctx))) {
      LOG_WARN("failed to rescan right op", K(ret));
    }
  }
  return ret;
}

int ObBLKNestedLoopJoin::make_paramater_warpper(ObBLKNestedLoopJoinCtx& join_ctx, ParamaterWrapper& params) const
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* plan_ctx = join_ctx.exec_ctx_.get_physical_plan_ctx();
  if (OB_ISNULL(plan_ctx)) {
    ret = OB_BAD_NULL_ERROR;
    LOG_WARN("plan ctx or left row is null", K(ret));
  } else {
    for (int64_t i = 0; i < rescan_params_.count() && OB_SUCC(ret); i++) {
      ObObjParam& param = plan_ctx->get_param_store_for_update().at(rescan_params_.at(i).param_idx_);
      if (OB_FAIL(params.paramater_list_.push_back(param))) {
        LOG_WARN("failed to push back param into list", K(ret));
      }
    }
  }
  return ret;
}

int ObBLKNestedLoopJoin::join_right_operate(ObBLKNestedLoopJoinCtx& join_ctx) const
{
  int ret = OB_SUCCESS;
  join_ctx.left_cache_iter_ = join_ctx.left_cache_.begin();
  if (OB_FAIL(OB_I(t1) get_next_right_row(join_ctx)) && OB_ITER_END != ret) {
    LOG_WARN("failed to get next right row", K(ret));
  }
  return ret;
}

int ObBLKNestedLoopJoin::join_right_func_going(ObBLKNestedLoopJoinCtx& join_ctx, const ObNewRow*& row) const
{
  UNUSED(row);
  int ret = OB_SUCCESS;
  if (OB_FAIL(save_right_row(join_ctx))) {
    LOG_WARN("failed to save right row", K(ret));
  } else {
    join_ctx.left_cache_index_ = 0;
    join_ctx.state_ = JS_JOIN_READ_CACHE;
  }
  return ret;
}

int ObBLKNestedLoopJoin::join_right_func_end(ObBLKNestedLoopJoinCtx& join_ctx, const ObNewRow*& row) const
{
  UNUSED(row);
  int ret = OB_SUCCESS;
  if (BNL_INNER_JOIN == join_ctx.bnl_jointype_) {
    if (OB_FAIL(join_ctx.clear_left_has_joined())) {
      LOG_WARN("clear array failed", K(ret));
    } else {
      join_ctx.left_cache_.clear_rows();
      join_ctx.state_ = JS_JOIN_LEFT_JOIN;
    }
  } else {
    join_ctx.left_cache_index_ = 0;
    join_ctx.left_cache_iter_ = join_ctx.left_cache_.begin();
    join_ctx.state_ = JS_JOIN_OUTER_JOIN;
  }
  return ret;
}

int ObBLKNestedLoopJoin::join_read_cache_operate(ObBLKNestedLoopJoinCtx& join_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_last_right_row(join_ctx))) {
    LOG_WARN("failed to get last right row", K(ret));
  } else if (OB_FAIL(read_row_from_left_cache(join_ctx)) && OB_ITER_END != ret) {
    LOG_WARN("failed to read row from left_cache", K(ret), K(join_ctx.left_cache_));
  }
  return ret;
}

int ObBLKNestedLoopJoin::join_read_cache_func_going(ObBLKNestedLoopJoinCtx& join_ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  bool is_match = false;
  row = NULL;
  if (OB_FAIL(save_right_row(join_ctx))) {
    LOG_WARN("failed to save right row", K(ret));
  } else if (NULL == join_ctx.left_row_ || NULL == join_ctx.right_row_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("left row and right row should not null", K(ret), K(join_ctx.left_row_), K(join_ctx.right_row_));
  } else if (OB_FAIL(OB_I(t3) calc_other_conds(join_ctx, is_match))) {
    LOG_WARN("failed to compare left and right row on other join conds", K(ret));
  } else if (is_match) {
    if (BNL_INNER_JOIN == join_ctx.bnl_jointype_ || BNL_OUTER_JOIN == join_ctx.bnl_jointype_) {
      if (OB_FAIL(OB_I(t5) join_rows(join_ctx, row))) {
        LOG_WARN("failed to join row", K(ret));
      }
    }
  } else {
  }  // do nothing
  if (OB_SUCCESS == ret && OB_FAIL(join_ctx.set_left_row_joined(join_ctx.left_cache_index_, is_match))) {
    LOG_WARN("failed to set left_join_rowed", K(ret));
  }
  ++join_ctx.left_cache_index_;
  join_ctx.left_row_ = NULL;
  join_ctx.state_ = JS_JOIN_READ_CACHE;
  return ret;
}

int ObBLKNestedLoopJoin::join_read_cache_func_end(ObBLKNestedLoopJoinCtx& join_ctx, const ObNewRow*& row) const
{
  UNUSED(row);
  int ret = OB_SUCCESS;
  join_ctx.left_cache_index_ = 0;
  join_ctx.state_ = JS_JOIN_RIGHT_JOIN;
  return ret;
}

int ObBLKNestedLoopJoin::join_outer_join_operate(ObBLKNestedLoopJoinCtx& join_ctx) const
{
  int ret = OB_SUCCESS;
  join_ctx.left_row_ = NULL;
  if (OB_FAIL(read_row_from_left_cache(join_ctx)) && OB_ITER_END != ret) {
    LOG_WARN("failed to read row from left_cache", K(ret), K(join_ctx.left_cache_));
  }
  return ret;
}

int ObBLKNestedLoopJoin::join_outer_join_func_going(ObBLKNestedLoopJoinCtx& join_ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(join_ctx.get_left_row_joined(join_ctx.left_cache_index_, join_ctx.left_row_joined_))) {
    LOG_WARN(
        "failed to get left_row_joined", K(ret), K(join_ctx.left_cache_index_), K(join_ctx.left_has_joined_.size()));
  } else {
    ++join_ctx.left_cache_index_;
    if (!join_ctx.left_row_joined_) {
      if (BNL_OUTER_JOIN == join_ctx.bnl_jointype_) {
        if (OB_FAIL(OB_I(t1) left_join_rows(join_ctx, row))) {
          LOG_WARN("failed to left join rows", K(ret));
        }
      } else if (BNL_ANTI_JOIN == join_ctx.bnl_jointype_) {
        row = join_ctx.left_row_;
        if (OB_FAIL(copy_cur_row(join_ctx, row))) {
          LOG_WARN("copy current row failed", K(ret));
        }
      }
    } else {
      if (BNL_SEMI_JOIN == join_ctx.bnl_jointype_) {
        row = join_ctx.left_row_;
        if (OB_FAIL(copy_cur_row(join_ctx, row))) {
          LOG_WARN("copy current row failed", K(ret));
        }
      }
    }
  }
  join_ctx.left_row_ = NULL;
  join_ctx.state_ = JS_JOIN_OUTER_JOIN;
  return ret;
}

int ObBLKNestedLoopJoin::join_outer_join_func_end(ObBLKNestedLoopJoinCtx& join_ctx, const ObNewRow*& row) const
{
  UNUSED(row);
  int ret = OB_SUCCESS;
  if (OB_FAIL(join_ctx.clear_left_has_joined())) {
    LOG_WARN("clear array failed", K(ret));
  } else {
    join_ctx.left_cache_.clear_rows();
    join_ctx.state_ = JS_JOIN_LEFT_JOIN;
  }
  return ret;
}

int ObBLKNestedLoopJoin::read_row_from_left_cache(ObBLKNestedLoopJoinCtx& join_ctx) const
{
  int ret = OB_SUCCESS;
  ObRowStore::Iterator& iter = join_ctx.left_cache_iter_;
  ObRowStore::StoredRow*& stored_row = join_ctx.stored_row_;
  ObNewRow& row_buf = join_ctx.left_cache_row_buf_;
  join_ctx.left_row_ = &join_ctx.left_cache_row_buf_;
  if (OB_FAIL(try_check_status(join_ctx.exec_ctx_))) {
    LOG_WARN("check physical plan status failed", K(ret));
  } else if (OB_FAIL(OB_I(t1) iter.get_next_row(row_buf, NULL, &stored_row)) && OB_ITER_END != ret) {
    LOG_WARN("failed to get next row from cache", K(ret), K(join_ctx.left_cache_.get_row_count()));
  }
  return ret;
}

int ObBLKNestedLoopJoin::ObBLKNestedLoopJoinCtx::init_left_has_joined(int64_t size)
{
  int ret = OB_SUCCESS;
  if (size < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("size shoud >0", K(ret));
  } else {
    ret = left_has_joined_.reserve(size);
    for (int64 i = 0; OB_SUCC(ret) && i < size; ++i) {
      ret = left_has_joined_.push_back(false);
    }
  }
  return ret;
}

int ObBLKNestedLoopJoin::ObBLKNestedLoopJoinCtx::clear_left_has_joined()
{
  left_cache_index_ = 0;
  while (!left_has_joined_.empty()) {
    left_has_joined_.pop_back();
  }
  return OB_SUCCESS;
}

int ObBLKNestedLoopJoin::ObBLKNestedLoopJoinCtx::get_left_row_joined(int64_t index, bool& has_joined)
{
  int ret = OB_SUCCESS;
  if (index >= left_has_joined_.size()) {
    ret = OB_INDEX_OUT_OF_RANGE;
  } else {
    has_joined = left_has_joined_.at(index);
  }
  return ret;
}

int ObBLKNestedLoopJoin::ObBLKNestedLoopJoinCtx::set_left_row_joined(int64_t index, bool& is_join)
{
  int ret = OB_SUCCESS;
  if (index >= left_has_joined_.size()) {
    ret = OB_INDEX_OUT_OF_RANGE;
  } else {
    left_has_joined_.at(index) = left_has_joined_.at(index) | is_join;
  }
  return ret;
}

OB_SERIALIZE_MEMBER((ObBLKNestedLoopJoin, ObBasicNestedLoopJoin));
}  // namespace sql
}  // namespace oceanbase
