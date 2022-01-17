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
#include "sql/engine/set/ob_merge_union.h"
#include "lib/utility/utility.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/allocator/page_arena.h"
#include "common/object/ob_object.h"
#include "common/row/ob_row.h"
#include "share/object/ob_obj_cast.h"
//#include "sql/engine/expr/ob_expr_promotion_util.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase {
using namespace common;
namespace sql {
// REGISTER_PHY_OPERATOR(ObMergeUnion, PHY_MERGE_UNION);

class ObMergeUnion::ObMergeUnionCtx : public ObMergeSetOperatorCtx {
public:
  explicit ObMergeUnionCtx(ObExecContext& ctx)
      : ObMergeSetOperatorCtx(ctx),
        cur_child_op_(NULL),
        next_child_op_idx_(1),
        got_first_row_(false),
        candidate_output_row_(NULL),
        candidate_child_op_(NULL)
  {}
  virtual void destroy()
  {
    ObMergeSetOperatorCtx::destroy();
  }

private:
  ObPhyOperator* cur_child_op_;
  int64_t next_child_op_idx_;
  bool got_first_row_;
  const ObNewRow* candidate_output_row_;
  ObPhyOperator* candidate_child_op_;

  friend class ObMergeUnion;
};

ObMergeUnion::ObMergeUnion(common::ObIAllocator& alloc) : ObMergeSetOperator(alloc), get_next_row_func_(NULL)
{}

ObMergeUnion::~ObMergeUnion()
{}

void ObMergeUnion::reset()
{
  get_next_row_func_ = NULL;
  ObMergeSetOperator::reset();
}

void ObMergeUnion::reuse()
{
  get_next_row_func_ = NULL;
  ObMergeSetOperator::reuse();
}

int ObMergeUnion::inner_create_operator_ctx(ObExecContext& ctx, ObPhyOperatorCtx*& op_ctx) const
{
  return CREATE_PHY_OPERATOR_CTX(ObMergeUnionCtx, ctx, get_id(), get_type(), op_ctx);
}

int ObMergeUnion::init_op_ctx(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPhyOperatorCtx* op_ctx = NULL;
  if (OB_FAIL(inner_create_operator_ctx(ctx, op_ctx))) {
    LOG_WARN("create p == hysical operator context failed", K(ret));
  } else if (OB_ISNULL(op_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("got op_ctx is NULL", K(ret));
  } else if (OB_FAIL(init_cur_row(*op_ctx, true))) {
    LOG_WARN("init current row failed", K(ret));
  } else {
  }
  return ret;
}

/*
 * When UNION ALL, we get results from two query one by one
 */
int ObMergeUnion::all_get_next_row(ObExecContext& ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ObMergeUnionCtx* merge_union_ctx = NULL;

  if (OB_ISNULL(merge_union_ctx = GET_PHY_OPERATOR_CTX(ObMergeUnionCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get physical operator failed", K(ctx), K_(id));
  } else if (OB_ISNULL(merge_union_ctx->cur_child_op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get physical operator failed", K(ctx), K_(id));
  } else if (OB_FAIL(merge_union_ctx->cur_child_op_->get_next_row(ctx, row))) {
    // get next row with the next child operator
    while (OB_ITER_END == ret && merge_union_ctx->next_child_op_idx_ < get_child_num()) {
      merge_union_ctx->cur_child_op_ = get_child(merge_union_ctx->next_child_op_idx_);
      ++merge_union_ctx->next_child_op_idx_;
      if (OB_ISNULL(merge_union_ctx->cur_child_op_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get physical operator failed", K(ctx), K_(id));
      } else {
        ret = merge_union_ctx->cur_child_op_->get_next_row(ctx, row);
      }
    }
    if (OB_SUCCESS != ret && OB_ITER_END != ret) {
      LOG_WARN("get next row failed", K(ret));
    }
  } else {
  }
  return ret;
}

int ObMergeUnion::get_first_row(ObExecContext& ctx, ObMergeUnionCtx& union_ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  int cmp = 0;
  const ObNewRow* left_row = NULL;
  const ObNewRow* right_row = NULL;
  ObPhyOperator* left_op = NULL;
  ObPhyOperator* right_op = NULL;
  if (OB_ISNULL(left_op = get_child(FIRST_CHILD)) || OB_ISNULL(right_op = get_child(SECOND_CHILD))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("left_op or right_op is NULL", K(left_op), K(right_op), K(ret));
  } else if (OB_FAIL(left_op->get_next_row(ctx, left_row))) {
    if (OB_ITER_END == ret) {
      // switch to the right operator
      union_ctx.candidate_output_row_ = NULL;
      union_ctx.candidate_child_op_ = NULL;
      union_ctx.cur_child_op_ = right_op;
      ret = union_ctx.cur_child_op_->get_next_row(ctx, row);
      if (OB_SUCCESS != ret && OB_ITER_END != ret) {
        LOG_WARN("failed to get next row", K(ret));
      }
    }
  } else if (OB_ISNULL(left_row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("got left_row is NULL", K(ret));
  } else if (OB_FAIL(do_strict_distinct(*right_op, ctx, *left_row, right_row, cmp))) {
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      union_ctx.candidate_child_op_ = NULL;
      union_ctx.candidate_output_row_ = NULL;
      union_ctx.cur_child_op_ = left_op;
      row = left_row;
    } else {
      LOG_WARN("fail to get right operator's row", K(ret));
    }
  } else {
    union_ctx.candidate_child_op_ = cmp < 0 ? right_op : left_op;
    union_ctx.candidate_output_row_ = cmp < 0 ? right_row : left_row;
    union_ctx.cur_child_op_ = cmp < 0 ? left_op : right_op;
    row = cmp < 0 ? left_row : right_row;
  }
  return ret;
}
/**
 * When UNION DISTINCT, we consider that left and right query already in ordered.
 * cur_child_operator: get current row from this operator in the first place
 * candidate_output_row && candidate_child_operator: the candidate output row from the candidate
 * child operator
 *
 * in distinct_get_nexr_row, we get the next row from the cur_child_operator as input_row at first,
 * input_row is distinct with the last output row
 * if in the end of the cur_child_opertor iterator, we must output the candidate_output_row
 * as the current row and switch the cur_child_operator to candidate_child_operator to get next row,
 *
 * if in the end of the candidate_child_operator iterator, we only need to get next row from the
 * cur_child_operator, and don't need to compare with candidate_child_operator's row
 *
 * if cur_child_operator and candidate_child_operator are present, we need to compare input_row
 * with the  candidate_output_row, if input_row is less than candidate_output_row, return input_row
 * as the result, if input_row equal to candidate_output_row, return input_row as the result and
 * get the distinct candidate_output_row from the candidate_child_operator, otherwise, return
 * candidate_output_row as the result and switch candidate_child_operator with cur_child_operator
 * for the next iteration
 */
int ObMergeUnion::distinct_get_next_row(ObExecContext& ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  int cmp = 0;
  const ObNewRow* input_row = NULL;
  ObMergeUnionCtx* merge_union_ctx = NULL;

  if (OB_ISNULL(merge_union_ctx = GET_PHY_OPERATOR_CTX(ObMergeUnionCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get physical operator failed", K(ctx), K_(id));
  } else {
    if (merge_union_ctx->got_first_row_) {
      int cur_child_err = OB_SUCCESS;
      int candidate_child_err = OB_SUCCESS;
      if (OB_ISNULL(merge_union_ctx->cur_child_op_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("cur_child_op is NULL", K(ret));
      } else if (OB_UNLIKELY(
                     (OB_SUCCESS !=
                         (cur_child_err = do_strict_distinct(
                              *merge_union_ctx->cur_child_op_, ctx, merge_union_ctx->last_output_row_, input_row))))) {
        if (OB_ITER_END == cur_child_err) {
          if (OB_LIKELY(NULL != merge_union_ctx->candidate_child_op_)) {
            // current operator in the end of iterator, so switch to the candidate operator
            merge_union_ctx->cur_child_op_ = merge_union_ctx->candidate_child_op_;
            merge_union_ctx->candidate_child_op_ = NULL;
            row = merge_union_ctx->candidate_output_row_;
            merge_union_ctx->candidate_output_row_ = NULL;
          } else {
            ret = OB_ITER_END;
          }
        } else {
          ret = cur_child_err;
          LOG_WARN("failed to do_strict_distinct", K(ret));
        }
      } else if (NULL == merge_union_ctx->candidate_child_op_) {
        row = input_row;
      } else if (OB_UNLIKELY(NULL == input_row || NULL == merge_union_ctx->candidate_output_row_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("input row is NULL or candidate_output_row_ is NULL",
            K(input_row),
            K(merge_union_ctx->candidate_output_row_),
            K(ret));
      } else if (OB_FAIL(strict_compare(*input_row, *merge_union_ctx->candidate_output_row_, cmp))) {
        LOG_WARN("compatible compare failed", K(ret));
      } else if (0 == cmp) {
        // left row equal to right row
        row = input_row;

        candidate_child_err = do_strict_distinct(
            *merge_union_ctx->candidate_child_op_, ctx, *input_row, merge_union_ctx->candidate_output_row_);

        if (OB_SUCCESS != candidate_child_err) {
          if (OB_ITER_END == candidate_child_err) {
            // candidate operator in the end of row iteration, candidate operator not exist
            merge_union_ctx->candidate_child_op_ = NULL;
            merge_union_ctx->candidate_output_row_ = NULL;
          } else {
            ret = candidate_child_err;
            LOG_WARN("candidate child operator get next row failed", K(ret));
          }
        }
      } else if (cmp < 0) {
        // output current row
        row = input_row;
      } else if (cmp > 0) {
        // output candidate row and switch candidate operator to current operator for next iteration
        ObPhyOperator* tmp_op = NULL;
        row = merge_union_ctx->candidate_output_row_;
        merge_union_ctx->candidate_output_row_ = input_row;
        tmp_op = merge_union_ctx->candidate_child_op_;
        merge_union_ctx->candidate_child_op_ = merge_union_ctx->cur_child_op_;
        merge_union_ctx->cur_child_op_ = tmp_op;
      } else {
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(merge_union_ctx->store_last_row(*row))) {
          LOG_WARN("storage current row for next compare failed", K(ret));
        }
      }
    } else {
      // the first row, not need to compare the row
      // first, get next row
      if (OB_FAIL(get_first_row(ctx, *merge_union_ctx, row))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get first row failed", K(ret));
        }
      }
      // second, storage current row
      if (OB_SUCC(ret)) {
        merge_union_ctx->got_first_row_ = true;
        if (OB_FAIL(merge_union_ctx->store_last_row(*row))) {
          LOG_WARN("storage current row for next compare failed", K(ret));
        }
      }
    }
  }
  return ret;
}

void ObMergeUnion::set_distinct(bool is_distinct)
{
  ObMergeSetOperator::set_distinct(is_distinct);
  if (is_distinct) {
    get_next_row_func_ = &ObMergeUnion::distinct_get_next_row;
  } else {
    get_next_row_func_ = &ObMergeUnion::all_get_next_row;
  }
}

int ObMergeUnion::inner_open(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObMergeUnionCtx* merge_union_ctx = NULL;

  if (OB_FAIL(ObMergeSetOperator::inner_open(ctx))) {
    LOG_WARN("failed to open set operator", K(ret));
  } else if (OB_ISNULL(merge_union_ctx = GET_PHY_OPERATOR_CTX(ObMergeUnionCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get physical operator context failed", K(ctx), K_(id), K(ret));
  } else {
    merge_union_ctx->cur_child_op_ = get_child(FIRST_CHILD);
    merge_union_ctx->next_child_op_idx_ = 1;
  }
  return ret;
}

int ObMergeUnion::rescan(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObMergeUnionCtx* union_ctx = NULL;
  if (OB_ISNULL(union_ctx = GET_PHY_OPERATOR_CTX(ObMergeUnionCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN(" failed to get operator context ", K(ret));
  } else {
    union_ctx->cur_child_op_ = get_child(FIRST_CHILD);
    union_ctx->next_child_op_idx_ = 1;
    union_ctx->got_first_row_ = false;
    union_ctx->candidate_output_row_ = NULL;
    union_ctx->candidate_child_op_ = NULL;
    ret = ObMergeSetOperator::rescan(ctx);
  }
  return ret;
}

int ObMergeUnion::inner_close(ObExecContext& ctx) const
{
  return ObMergeSetOperator::inner_close(ctx);
}

int ObMergeUnion::inner_get_next_row(ObExecContext& ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ObMergeUnionCtx* union_ctx = NULL;
  if (OB_ISNULL(get_next_row_func_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("get_next_row_func is NULL", K(ret));
  } else if (OB_ISNULL(union_ctx = GET_PHY_OPERATOR_CTX(ObMergeUnionCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get merge intersect context failed");
  } else if (OB_FAIL((this->*get_next_row_func_)(ctx, row))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get next row failed", K(ret));
    }
  } else if (OB_FAIL(copy_cur_row_by_projector(*union_ctx, row))) {
    LOG_WARN("copy current row with projector failed", K(ret));
  } else {
  }
  return ret;
}
}  // namespace sql
}  // namespace oceanbase
