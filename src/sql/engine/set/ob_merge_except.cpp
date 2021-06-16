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
#include "sql/engine/set/ob_merge_except.h"
#include "lib/utility/utility.h"
#include "common/object/ob_object.h"
#include "common/row/ob_row_util.h"
#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase {
using namespace common;

namespace sql {
class ObMergeExcept::ObMergeExceptCtx : public ObMergeSetOperatorCtx {
public:
  explicit ObMergeExceptCtx(ObExecContext& ctx) : ObMergeSetOperatorCtx(ctx), right_iter_end_(false), right_row_(NULL)
  {
    need_skip_init_row_ = true;
  }
  void reset()
  {
    right_iter_end_ = false;
    right_row_ = NULL;
    need_skip_init_row_ = true;
    for (int64_t i = 0; i < last_output_row_.count_; ++i) {
      last_output_row_.cells_[i].set_null();
    }
  }
  virtual void destroy()
  {
    ObMergeSetOperatorCtx::destroy();
  }

private:
  bool right_iter_end_;
  const ObNewRow* right_row_;
  friend class ObMergeExcept;
};

ObMergeExcept::ObMergeExcept(common::ObIAllocator& alloc) : ObMergeSetOperator(alloc), get_next_row_func_(NULL)
{}

ObMergeExcept::~ObMergeExcept()
{
  get_next_row_func_ = NULL;
}

void ObMergeExcept::reset()
{
  get_next_row_func_ = NULL;
  ObMergeSetOperator::reset();
}

void ObMergeExcept::reuse()
{
  get_next_row_func_ = NULL;
  ObMergeSetOperator::reuse();
}

void ObMergeExcept::set_distinct(bool is_distinct)
{
  ObMergeSetOperator::set_distinct(is_distinct);
  if (is_distinct) {
    get_next_row_func_ = &ObMergeExcept::distinct_get_next_row;
  } else {
    get_next_row_func_ = &ObMergeExcept::all_get_next_row;
  }
}

int ObMergeExcept::inner_create_operator_ctx(ObExecContext& ctx, ObPhyOperatorCtx*& op_ctx) const
{
  return CREATE_PHY_OPERATOR_CTX(ObMergeExceptCtx, ctx, get_id(), PHY_MERGE_EXCEPT, op_ctx);
}

int ObMergeExcept::inner_open(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObMergeExceptCtx* except_ctx = NULL;
  if (OB_FAIL(ObMergeSetOperator::inner_open(ctx))) {
    LOG_WARN("fail to open set operator", K(ret));
  } else if (OB_ISNULL(except_ctx = GET_PHY_OPERATOR_CTX(ObMergeExceptCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get except context", K(ctx), K_(id), K(ret));
  } else {
  }
  return ret;
}

int ObMergeExcept::rescan(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObMergeExceptCtx* except_ctx = NULL;
  except_ctx = GET_PHY_OPERATOR_CTX(ObMergeExceptCtx, ctx, get_id());
  if (OB_ISNULL(except_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("got except context is NULL", K(ret));
  } else {
    except_ctx->reset();
    if (OB_FAIL(ObMergeSetOperator::rescan(ctx))) {
      LOG_WARN("failed to rescan", K(ret));
    }
  }
  return ret;
}

int ObMergeExcept::inner_close(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ret = ObMergeSetOperator::inner_close(ctx);
  return ret;
}

int ObMergeExcept::inner_get_next_row(ObExecContext& ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ObMergeExceptCtx* intersect_ctx = NULL;
  if (OB_ISNULL(get_next_row_func_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get_next_row_func_ is NULL", K(ret));
  } else if (OB_ISNULL(intersect_ctx = GET_PHY_OPERATOR_CTX(ObMergeExceptCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get merge intersect context failed", K(ret));
  } else if (OB_FAIL((this->*get_next_row_func_)(ctx, row))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get next row failed", K(ret));
    }
  } else if (OB_FAIL(copy_cur_row_by_projector(*intersect_ctx, row))) {
    LOG_WARN("copy current row failed", K(ret));
  } else {
  }
  return ret;
}

int ObMergeExcept::distinct_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  int cmp = 0;
  bool break_outer_loop = false;
  const ObNewRow* left_row = NULL;
  ObMergeExceptCtx* merge_except_ctx = NULL;
  ObPhyOperator* left_op = NULL;
  ObPhyOperator* right_op = NULL;

  if (OB_ISNULL(merge_except_ctx = GET_PHY_OPERATOR_CTX(ObMergeExceptCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get physical operator context", K(ctx), K_(id), K(ret));
  } else if (OB_ISNULL(left_op = get_child(FIRST_CHILD)) || OB_ISNULL(right_op = get_child(SECOND_CHILD))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("left_op or right_op is NULL", K(left_op), K(right_op), K(ret));
  } else {
    bool is_get_first_row = true;
    while (OB_SUCC(ret) && OB_SUCC(do_strict_distinct(*left_op, ctx, merge_except_ctx->last_output_row_, left_row))) {
      if (is_get_first_row) {
        if (OB_FAIL(merge_except_ctx->store_last_row(*left_row))) {
          LOG_WARN("fail to store last row", K(ret), K(*left_row));
        } else {
          is_get_first_row = false;
        }
      }
      break_outer_loop = merge_except_ctx->right_iter_end_;
      while (OB_SUCCESS == ret && !merge_except_ctx->right_iter_end_) {
        if (OB_ISNULL(left_row)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("left_row is NULL", K_(id), K(ret));
        } else if (NULL != merge_except_ctx->right_row_) {
          if (OB_FAIL(strict_compare(*left_row, *merge_except_ctx->right_row_, cmp))) {
            LOG_WARN("compare input_row with right_row failed", K(*left_row), K_(*merge_except_ctx->right_row), K(ret));
          } else if (cmp < 0) {
            // input_row is not in the right set, output input_row
            break_outer_loop = true;
            break;
          } else if (0 == cmp) {
            // break to get another left row
            break;
          } else {
            if (OB_FAIL(right_op->get_next_row(ctx, merge_except_ctx->right_row_))) {
              if (OB_ITER_END == ret) {
                merge_except_ctx->right_iter_end_ = true;
                merge_except_ctx->right_row_ = NULL;
                break_outer_loop = true;
                ret = OB_SUCCESS;
              } else {
                LOG_WARN("fail to get right operator row", K(ctx), K(ret));
              }
            } else {
            }
          }
        } else {
          // get first row
          if (OB_FAIL(right_op->get_next_row(ctx, merge_except_ctx->right_row_))) {
            if (OB_ITER_END == ret) {
              merge_except_ctx->right_iter_end_ = true;
              merge_except_ctx->right_row_ = NULL;
              break_outer_loop = true;
              ret = OB_SUCCESS;
            } else {
              LOG_WARN("fail to get right operator row", K(ctx), K(ret));
            }
          }
        }
      }
      if (break_outer_loop) {
        break;  // cmp < 0 implement that the left_row is not in the right set, so output it
      }
    }
  }

  if (OB_UNLIKELY(OB_SUCCESS != ret && OB_ITER_END != ret)) {
    LOG_WARN("fail to get next row", K(ret));
  } else if (OB_SUCC(ret)) {
    row = left_row;
    if (OB_ISNULL(row)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("row is NULL", K(ret));
    } else if (OB_FAIL(merge_except_ctx->store_last_row(*row))) {
      LOG_WARN("fail to store last row", K(ret), K(*row));
    } else {
    }
  }
  return ret;
}
/**
 * the algorithm of get_next row:
 * state mechine, use two while loop to control the state transition
 * state 1: get next row from left operator, if in the end of iteration, exit,
 * otherwise, jump to state 2.
 * state 2: if the right_row doesn't exist, jump to state 3, otherwise, jump to state 4.
 * state 3: get next row from right operator, if in the end of iterator, output left_row,
 * otherwise, jump to state 4.
 * state 4: compare left_row with right_row, if left_row < right_row, jump to state 5,
 * if left_row = right_row, jump to state 6, otherwise, jump to state 7
 * state 5: output left row, exit
 * state 6: get next row from right operator, if in the end of iterator, right_iter_end_=true,
 * otherwise, jump to state 1.
 * state 7: get next row from right operator, if in the end of iterator, output left_row,
 * otherwise, jump to state 4.
 */
int ObMergeExcept::all_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  int cmp = 0;
  bool break_outer_loop = false;
  const ObNewRow* left_row = NULL;
  ObMergeExceptCtx* merge_except_ctx = NULL;
  ObPhyOperator* left_op = NULL;
  ObPhyOperator* right_op = NULL;

  if (OB_ISNULL(merge_except_ctx = GET_PHY_OPERATOR_CTX(ObMergeExceptCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get physical operator context", K(ctx), K_(id), K(ret));
  } else if (OB_ISNULL(left_op = get_child(FIRST_CHILD)) || OB_ISNULL(right_op = get_child(SECOND_CHILD))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("left_op or right_op is NULL", K(left_op), K(right_op), K(ret));
  } else {
    while (OB_SUCCESS == ret && OB_SUCC(left_op->get_next_row(ctx, left_row))) {
      break_outer_loop = merge_except_ctx->right_iter_end_;
      while (OB_SUCCESS == ret && !merge_except_ctx->right_iter_end_) {
        if (OB_ISNULL(left_row)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("left row is NULL", K(ret));
        } else if (NULL != merge_except_ctx->right_row_) {
          if (OB_FAIL(strict_compare(*left_row, *merge_except_ctx->right_row_, cmp))) {
            LOG_WARN("compare input_row with right_row failed", K(*left_row), K_(*merge_except_ctx->right_row), K(ret));
          } else if (cmp < 0) {
            // input_row is not in the right set, output input_row
            break_outer_loop = true;
            break;
          } else if (0 == cmp) {
            // break to get another left row
            break;
          } else {
            if (OB_FAIL(right_op->get_next_row(ctx, merge_except_ctx->right_row_))) {
              if (OB_ITER_END == ret) {
                merge_except_ctx->right_iter_end_ = true;
                merge_except_ctx->right_row_ = NULL;
                break_outer_loop = true;
                ret = OB_SUCCESS;
              } else {
                LOG_WARN("fail to get right operator row", K(ctx), K(ret));
              }
            }
          }
        } else {
          // get first row
          if (OB_FAIL(right_op->get_next_row(ctx, merge_except_ctx->right_row_))) {
            if (OB_ITER_END == ret) {
              merge_except_ctx->right_iter_end_ = true;
              merge_except_ctx->right_row_ = NULL;
              break_outer_loop = true;
              ret = OB_SUCCESS;
            } else {
              LOG_WARN("fail to get right operator row", K(ctx), K(ret));
            }
          }
        }
      }
      if (break_outer_loop) {
        break;  // cmp < 0 implement that the left_row is not in the right set, so output it
      }
    }
  }
  if (OB_SUCCESS != ret && OB_ITER_END != ret) {
    LOG_WARN("fail to get next row", K(ret));
  } else if (OB_SUCC(ret)) {
    row = left_row;
  } else {
  }
  return ret;
}
}  // namespace sql
}  // namespace oceanbase
