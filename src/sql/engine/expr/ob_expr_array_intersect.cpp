/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_array_intersect.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::omt;

namespace oceanbase
{
namespace sql
{
ObExprArrayIntersect::ObExprArrayIntersect(ObIAllocator &alloc)
    : ObExprArraySetOperation(alloc, T_FUNC_SYS_ARRAY_INTERSECT, N_ARRAY_INTERSECT, MORE_THAN_ONE, NOT_ROW_DIMENSION)
{
}

ObExprArrayIntersect::~ObExprArrayIntersect()
{
}

int ObExprArrayIntersect::eval_array_intersect(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  return eval_array_set_operation(expr, ctx, res, INTERSECT);
}

int ObExprArrayIntersect::eval_array_intersect_batch(const ObExpr &expr,
                          ObEvalCtx &ctx,
                          const ObBitVector &skip,
                          const int64_t batch_size)
{
  return eval_array_set_operation_batch(expr, ctx, skip, batch_size, INTERSECT);
}

int ObExprArrayIntersect::eval_array_intersect_vector(const ObExpr &expr,
                          ObEvalCtx &ctx,
                          const ObBitVector &skip,
                          const EvalBound &bound)
{
  return eval_array_set_operation_vector(expr, ctx, skip, bound, INTERSECT);
}

int ObExprArrayIntersect::cg_expr(ObExprCGCtx &expr_cg_ctx,
                          const ObRawExpr &raw_expr,
                          ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_array_intersect;
  rt_expr.eval_batch_func_ = eval_array_intersect_batch;
  rt_expr.eval_vector_func_ = eval_array_intersect_vector;
  return OB_SUCCESS;
}

} // namespace sql
} // namespace oceanbase