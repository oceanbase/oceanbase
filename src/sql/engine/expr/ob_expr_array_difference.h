/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 * This file contains implementation for array_difference expression.
 */

#ifndef OCEANBASE_SQL_OB_EXPR_ARRAY_DIFFERENCE
#define OCEANBASE_SQL_OB_EXPR_ARRAY_DIFFERENCE

#include "sql/engine/expr/ob_expr_operator.h"
#include "lib/udt/ob_array_type.h"

namespace oceanbase
{
namespace sql
{
class ObExprArrayDifference : public ObFuncExprOperator
{
public:
  explicit ObExprArrayDifference(common::ObIAllocator &alloc);
  virtual ~ObExprArrayDifference();

  virtual int calc_result_type1(ObExprResType &type, ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx) const override;
  static int eval_array_difference(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  static int eval_array_difference_batch(const ObExpr &expr, ObEvalCtx &ctx,
                                         const ObBitVector &skip, const int64_t batch_size);
  static int eval_array_difference_vector(const ObExpr &expr, ObEvalCtx &ctx,
                                          const ObBitVector &skip, const EvalBound &bound);
  static int calc_difference(ObIArrayType *src_arr, ObIArrayType *res_arr);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprArrayDifference);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_ARRAY_DIFFERENCE