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
#include "sql/engine/set/ob_merge_intersect.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase {
using namespace common;

namespace sql {
class ObMergeIntersect::ObMergeIntersectCtx : public ObMergeSetOperatorCtx {
public:
  explicit ObMergeIntersectCtx(ObExecContext& ctx)
      : ObMergeSetOperatorCtx(ctx), right_iter_end_(false), right_row_(NULL)
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

  friend class ObMergeIntersect;
};

ObMergeIntersect::ObMergeIntersect(common::ObIAllocator& alloc) : ObMergeSetOperator(alloc), get_next_row_func_(NULL)
{}

ObMergeIntersect::~ObMergeIntersect()
{
  get_next_row_func_ = NULL;
}

void ObMergeIntersect::reset()
{
  get_next_row_func_ = NULL;
  ObMergeSetOperator::reset();
}

void ObMergeIntersect::reuse()
{
  get_next_row_func_ = NULL;
  ObMergeSetOperator::reuse();
}

void ObMergeIntersect::set_distinct(bool is_distinct)
{
  ObMergeSetOperator::set_distinct(is_distinct);
  if (is_distinct) {
    get_next_row_func_ = &ObMergeIntersect::distinct_get_next_row;
  } else {
    get_next_row_func_ = &ObMergeIntersect::all_get_next_row;
  }
}

int ObMergeIntersect::inner_open(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObMergeIntersectCtx* intersect_ctx = NULL;

  if (OB_ISNULL(get_next_row_func_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("merge_intersect not init", K(ret));
  } else if (OB_FAIL(ObMergeSetOperator::inner_open(ctx))) {
    LOG_WARN("failed to open double child operators", K(ret));
  } else if (OB_ISNULL(intersect_ctx = GET_PHY_OPERATOR_CTX(ObMergeIntersectCtx, ctx, get_id()))) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("fail to get merge intersect context", K(ctx), K_(id), K(ret));
  } else {
  }
  return ret;
}

int ObMergeIntersect::rescan(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObMergeIntersectCtx* set_ctx = NULL;
  if (OB_ISNULL(set_ctx = GET_PHY_OPERATOR_CTX(ObMergeIntersectCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get operator ctx", K(ret));
  } else {
    set_ctx->reset();
    ret = ObMergeSetOperator::rescan(ctx);
  }
  return ret;
}

int ObMergeIntersect::inner_close(ObExecContext& ctx) const
{
  return ObMergeSetOperator::inner_close(ctx);
}

/*
int ObMergeIntersect::close(ObExecContext &ctx) const
{
  return ObSetOperator::close(ctx);
}
*/
int ObMergeIntersect::inner_get_next_row(ObExecContext& ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ObMergeIntersectCtx* intersect_ctx = NULL;
  if (OB_ISNULL(get_next_row_func_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("merge_intersect not init", K(ret));
  } else if (OB_ISNULL(intersect_ctx = GET_PHY_OPERATOR_CTX(ObMergeIntersectCtx, ctx, get_id()))) {
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

int ObMergeIntersect::distinct_get_next_row(ObExecContext& ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  int cmp = 0;
  bool break_outer_loop = false;
  const ObNewRow* left_row = NULL;
  ObMergeIntersectCtx* intersect_ctx = NULL;
  ObPhyOperator* left_op = NULL;
  ObPhyOperator* right_op = NULL;

  if (OB_ISNULL(intersect_ctx = GET_PHY_OPERATOR_CTX(ObMergeIntersectCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get physical operator context", K(ctx), K_(id), K(ret));
  } else if (OB_ISNULL(left_op = get_child(FIRST_CHILD)) || OB_ISNULL(right_op = get_child(SECOND_CHILD))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("left_op or right_op is NULL", K(left_op), K(right_op), K(ret));
  } else {
    bool is_get_first_row = true;
    while (OB_SUCCESS == ret && OB_SUCC(do_strict_distinct(*left_op, ctx, intersect_ctx->last_output_row_, left_row))) {
      if (is_get_first_row) {
        if (OB_FAIL(intersect_ctx->store_last_row(*left_row))) {
          LOG_WARN("fail to store last row", K(ret), K(*left_row));
        } else {
          is_get_first_row = false;
        }
      }
      while (OB_SUCCESS == ret && !intersect_ctx->right_iter_end_) {
        if (NULL != intersect_ctx->right_row_) {
          if (OB_ISNULL(left_row)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("got left row is NULL", K(ret));
          } else if (OB_FAIL(strict_compare(*left_row, *intersect_ctx->right_row_, cmp))) {
            LOG_WARN("compare input_row with right_row failed", K(*left_row), K_(*intersect_ctx->right_row), K(ret));
          } else if (cmp < 0) {
            // input_row is not in the right set, break to get another left row
            break;
          } else if (0 == cmp) {
            break_outer_loop = true;
            break;
          } else {
            if (OB_FAIL(right_op->get_next_row(ctx, intersect_ctx->right_row_))) {
              if (OB_ITER_END == ret) {
                intersect_ctx->right_iter_end_ = true;
                intersect_ctx->right_row_ = NULL;
                ret = OB_SUCCESS;
              } else {
                LOG_WARN("fail to get right operator row", K(ctx), K(ret));
              }
            }
          }
        } else {
          // get first row
          if (OB_FAIL(right_op->get_next_row(ctx, intersect_ctx->right_row_))) {
            if (OB_ITER_END == ret) {
              intersect_ctx->right_iter_end_ = true;
              intersect_ctx->right_row_ = NULL;
              ret = OB_SUCCESS;
            } else {
              LOG_WARN("fail to get right operator row", K(ctx), K(ret));
            }
          }
        }
      }
      if (break_outer_loop) {
        break;
      }
    }
  }
  if (OB_SUCCESS != ret && OB_ITER_END != ret) {
    LOG_WARN("fail to get next row", K(ret));
  } else if (OB_SUCC(ret)) {
    row = left_row;
    if (OB_ISNULL(left_row)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("got left_row is NULL", K(ret));
    } else if (OB_FAIL(intersect_ctx->store_last_row(*row))) {
      LOG_WARN("fail to store last row", K(*row), K(ret));
    } else {
    }
  }
  return ret;
}

int ObMergeIntersect::all_get_next_row(ObExecContext& ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  int cmp = 0;
  bool break_outer_loop = false;
  const ObNewRow* left_row = NULL;
  ObMergeIntersectCtx* intersect_ctx = NULL;
  ObPhyOperator* left_op = NULL;
  ObPhyOperator* right_op = NULL;

  if (OB_ISNULL(intersect_ctx = GET_PHY_OPERATOR_CTX(ObMergeIntersectCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get physical operator context", K(ctx), K_(id), K(ret));
  } else if (OB_ISNULL(left_op = get_child(FIRST_CHILD)) || OB_ISNULL(right_op = get_child(SECOND_CHILD))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("left_op or right_op is NULL", K(left_op), K(right_op), K(ret));
  } else {
    while (OB_SUCCESS == ret && OB_SUCC(left_op->get_next_row(ctx, left_row))) {
      while (OB_SUCCESS == ret && !intersect_ctx->right_iter_end_) {
        if (NULL != intersect_ctx->right_row_) {
          if (OB_ISNULL(left_row)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("got left row is NULL", K(ret));
          } else if (OB_FAIL(strict_compare(*left_row, *intersect_ctx->right_row_, cmp))) {
            LOG_WARN("compare input_row with right_row failed", K(*left_row), K_(*intersect_ctx->right_row), K(ret));
          } else if (cmp < 0) {
            // input_row is not in the right set, break to get another left row
            break;
          } else if (0 == cmp) {
            break_outer_loop = true;
            break;
          } else {
            if (OB_FAIL(right_op->get_next_row(ctx, intersect_ctx->right_row_))) {
              if (OB_ITER_END == ret) {
                intersect_ctx->right_iter_end_ = true;
                intersect_ctx->right_row_ = NULL;
                ret = OB_SUCCESS;
              } else {
                LOG_WARN("fail to get right operator row", K(ctx), K(ret));
              }
            }
          }
        } else {
          // get first row
          if (OB_FAIL(right_op->get_next_row(ctx, intersect_ctx->right_row_))) {
            if (OB_ITER_END == ret) {
              intersect_ctx->right_iter_end_ = true;
              intersect_ctx->right_row_ = NULL;
              ret = OB_SUCCESS;
            } else {
              LOG_WARN("fail to get right operator row", K(ctx), K(ret));
            }
          }
        }
      }
      if (break_outer_loop) {
        break;
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

int ObMergeIntersect::inner_create_operator_ctx(ObExecContext& ctx, ObPhyOperatorCtx*& op_ctx) const
{
  return CREATE_PHY_OPERATOR_CTX(ObMergeIntersectCtx, ctx, get_id(), get_type(), op_ctx);
}
}  // namespace sql
}  // namespace oceanbase
