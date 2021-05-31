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
#include "sql/engine/expr/ob_expr_not_exists.h"
#include "common/row/ob_row_iterator.h"
namespace oceanbase {
using namespace common;
namespace sql {

ObExprNotExists::ObExprNotExists(ObIAllocator& alloc)
    : ObSubQueryRelationalExpr(alloc, T_OP_NOT_EXISTS, N_NOT_EXISTS, 1, NOT_ROW_DIMENSION)
{}

ObExprNotExists::~ObExprNotExists()
{}

int ObExprNotExists::calc_result_type1(ObExprResType& type, ObExprResType& type1, ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!type1.is_int())) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
  } else {
    type.set_int32();
    type.set_result_flag(OB_MYSQL_NOT_NULL_FLAG);
    type.set_scale(DEFAULT_SCALE_FOR_INTEGER);
    type.set_precision(DEFAULT_PRECISION_FOR_BOOL);
  }
  return ret;
}

int ObExprNotExists::calc_result1(ObObj& result, const ObObj& obj1, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  int64_t query_idx = OB_INVALID_INDEX;
  ObNewRow* row = NULL;
  ObNewRowIterator* row_iter = NULL;

  if (OB_ISNULL(expr_ctx.subplan_iters_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("expr_ctx.subplan_iters_ is null");
  } else {
    ObIArray<ObNewRowIterator*>* row_iters = expr_ctx.subplan_iters_;
    if (OB_FAIL(obj1.get_int(query_idx))) {
      LOG_WARN("get int failed", K(obj1), K(ret));
    } else if (OB_UNLIKELY(query_idx < 0 || query_idx >= row_iters->count())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("query_idx is invalid", "row_iter_count", row_iters->count(), K(query_idx));
    } else if (OB_ISNULL(row_iter = row_iters->at(query_idx))) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("row iterator is null", K(query_idx));
    } else if (OB_FAIL(row_iter->get_next_row(row))) {
      if (OB_LIKELY(OB_ITER_END == ret)) {
        ret = OB_SUCCESS;
        result.set_int32(1);  // not exist
      } else {
        LOG_WARN("get next row failed", K(ret));
      }
    } else {
      result.set_int32(0);  // exist row,so return false
    }
  }
  return ret;
}

int ObExprNotExists::cg_expr(ObExprCGCtx&, const ObRawExpr&, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  CK(1 == rt_expr.arg_cnt_);
  rt_expr.eval_func_ = &not_exists_eval;
  return ret;
};

int ObExprNotExists::not_exists_eval(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  bool exists = false;
  if (OB_FAIL(check_exists(expr, ctx, exists))) {
    LOG_WARN("check exists failed", K(ret));
  } else {
    expr_datum.set_bool(!exists);
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
