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
#include "sql/engine/expr/ob_iter_expr_set_operation.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase {
using namespace common;
namespace sql {
int ObSetOpIterExpr::get_left_row(ObIterExprCtx& expr_ctx, ObSetOpIterExprCtx& set_ctx) const
{
  int ret = OB_SUCCESS;
  if (set_ctx.left_row_ != NULL) {
    // do nothing
  } else if (!set_ctx.left_is_end_) {
    if (OB_ISNULL(left_iter_)) {
      ret = OB_NOT_INIT;
      LOG_WARN("left iterator expr isn't inited");
    } else if (OB_FAIL(left_iter_->get_next_row(expr_ctx, set_ctx.left_row_))) {
      if (ret != OB_ITER_END) {
        LOG_WARN("get left iterator row failed", K(ret));
      } else {
        ret = OB_SUCCESS;
        set_ctx.left_is_end_ = true;
        set_ctx.left_row_ = NULL;
      }
    }
  } else {
    set_ctx.left_row_ = NULL;
  }
  return ret;
}

int ObSetOpIterExpr::get_right_row(ObIterExprCtx& expr_ctx, ObSetOpIterExprCtx& set_ctx) const
{
  int ret = OB_SUCCESS;
  if (set_ctx.right_row_ != NULL) {
    // do nothing
  } else if (!set_ctx.right_is_end_) {
    if (OB_ISNULL(right_iter_)) {
      ret = OB_NOT_INIT;
      LOG_WARN("right iterator expr isn't inited");
    } else if (OB_FAIL(right_iter_->get_next_row(expr_ctx, set_ctx.right_row_))) {
      if (ret != OB_ITER_END) {
        LOG_WARN("get right iterator row failed", K(ret));
      } else {
        ret = OB_SUCCESS;
        set_ctx.right_is_end_ = true;
        set_ctx.right_row_ = NULL;
      }
    }
  } else {
    set_ctx.right_row_ = NULL;
  }
  return ret;
}

int ObSetOpIterExpr::compare_row(const ObNewRow& left_row, const ObNewRow& right_row, int& result) const
{
  result = 0;
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(left_row.is_invalid()) || OB_UNLIKELY(right_row.is_invalid()) ||
      OB_UNLIKELY(left_row.get_count() != right_row.get_count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("left row or right row is invalid", K(left_row), K(right_row));
  }
  for (int64_t i = 0; OB_SUCC(ret) && result == 0 && i < left_row.get_count(); ++i) {
    result = left_row.get_cell(i).compare(right_row.get_cell(i));
  }
  return ret;
}

int ObSetOpIterExpr::get_next_row(ObIterExprCtx& expr_ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ObSetOpIterExprCtx* set_ctx = NULL;
  if (OB_ISNULL(left_iter_) || OB_ISNULL(right_iter_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("exec ctx is null", K_(left_iter), K_(right_iter));
  } else if (OB_ISNULL(set_ctx = expr_ctx.get_expr_op_ctx<ObSetOpIterExprCtx>(get_expr_id()))) {
    // create set op ctx
    if (OB_FAIL(expr_ctx.create_expr_op_ctx(get_expr_id(), set_ctx))) {
      LOG_WARN("create expr operator ctx failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    switch (get_expr_type()) {
      case T_OP_UNION: {
        ret = get_union_next_row(expr_ctx, *set_ctx, row);
        break;
      }
      case T_OP_INTERSECT: {
        ret = get_intersect_next_row(expr_ctx, *set_ctx, row);
        break;
      }
      case T_OP_EXCEPT: {
        ret = get_except_next_row(expr_ctx, *set_ctx, row);
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected expr type", K(get_expr_type()));
        break;
      }
    }
  }
  return ret;
}

int ObSetOpIterExpr::get_union_next_row(
    ObIterExprCtx& expr_ctx, ObSetOpIterExprCtx& set_ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  int result = 0;
  if (OB_FAIL(get_left_row(expr_ctx, set_ctx))) {
    LOG_WARN("get left row failed", K(ret));
  } else if (OB_FAIL(get_right_row(expr_ctx, set_ctx))) {
    LOG_WARN("get right row failed", K(ret));
  } else if (OB_ISNULL(set_ctx.left_row_) && OB_ISNULL(set_ctx.right_row_)) {  // both null
    ret = OB_ITER_END;
  } else if (OB_ISNULL(set_ctx.left_row_) || OB_ISNULL(set_ctx.right_row_)) {  // only one is null
    row = (set_ctx.left_row_ != NULL ? set_ctx.left_row_ : set_ctx.right_row_);
    set_ctx.left_row_ = NULL;
    set_ctx.right_row_ = NULL;
  } else if (OB_FAIL(compare_row(*set_ctx.left_row_, *set_ctx.right_row_, result))) {
    LOG_WARN("compare row failed", K(ret));
  } else if (result < 0) {
    row = set_ctx.left_row_;
    set_ctx.left_row_ = NULL;
  } else if (result > 0) {
    row = set_ctx.right_row_;
    set_ctx.right_row_ = NULL;
  } else {  // left equal right
    row = set_ctx.left_row_;
    set_ctx.left_row_ = NULL;
    set_ctx.right_row_ = NULL;
  }
  return ret;
}

int ObSetOpIterExpr::get_intersect_next_row(
    ObIterExprCtx& expr_ctx, ObSetOpIterExprCtx& set_ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  int result = 0;
  bool is_break = false;
  while (OB_SUCC(ret) && !is_break) {
    if (OB_FAIL(get_left_row(expr_ctx, set_ctx))) {
      LOG_WARN("get left row failed", K(ret));
    } else if (OB_FAIL(get_right_row(expr_ctx, set_ctx))) {
      LOG_WARN("get right row failed", K(ret));
    } else if (OB_ISNULL(set_ctx.left_row_) || OB_ISNULL(set_ctx.right_row_)) {
      // we need to find the rows that exist in the right set
      // if right row is null, means no rows in the right set any more
      ret = OB_ITER_END;
    } else if (OB_FAIL(compare_row(*set_ctx.left_row_, *set_ctx.right_row_, result))) {
      LOG_WARN("compare row failed", K(ret));
    } else if (result < 0) {
      // left row not in the right set, so get another left row
      set_ctx.left_row_ = NULL;
    } else if (0 == result) {
      // find the intersect row, output this row and break
      row = set_ctx.left_row_;
      set_ctx.left_row_ = NULL;
      set_ctx.right_row_ = NULL;
      is_break = true;
    } else {  // result > 0
      // right row not in the left set, so get another right row
      set_ctx.right_row_ = NULL;
    }
  }
  return ret;
}

int ObSetOpIterExpr::get_except_next_row(
    ObIterExprCtx& expr_ctx, ObSetOpIterExprCtx& set_ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  int result = 0;
  bool is_break = false;
  while (OB_SUCC(ret) && !is_break) {
    if (OB_FAIL(get_left_row(expr_ctx, set_ctx))) {
      LOG_WARN("get left row failed", K(ret));
    } else if (OB_FAIL(get_right_row(expr_ctx, set_ctx))) {
      LOG_WARN("get right row failed", K(ret));
    } else if (OB_ISNULL(set_ctx.left_row_)) {
      ret = OB_ITER_END;
    } else if (OB_ISNULL(set_ctx.right_row_)) {
      // left row isn't null, right row is null
      row = set_ctx.left_row_;
      set_ctx.left_row_ = NULL;
      is_break = true;
    } else if (OB_FAIL(compare_row(*set_ctx.left_row_, *set_ctx.right_row_, result))) {
      LOG_WARN("compare row failed", K(ret));
    } else if (result < 0) {
      // left row not in the right set, so output the left row
      row = set_ctx.left_row_;
      set_ctx.left_row_ = NULL;
      is_break = true;
    } else if (0 == result) {  // left row equal to right row, ignore intersect row
      set_ctx.left_row_ = NULL;
      set_ctx.right_row_ = NULL;
    } else {  // result > 0, can't determine the left row must outside in the right set, so get another right row
      set_ctx.right_row_ = NULL;
    }
  }
  return ret;
}
}  // namespace sql
}  // namespace oceanbase
