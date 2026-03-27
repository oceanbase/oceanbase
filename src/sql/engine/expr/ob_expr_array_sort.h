/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 * This file contains implementation for array_sort.
 */

#ifndef OCEANBASE_SQL_OB_EXPR_ARRAY_SORT
#define OCEANBASE_SQL_OB_EXPR_ARRAY_SORT

#include "sql/engine/expr/ob_expr_operator.h"
#include "lib/udt/ob_array_type.h"

namespace oceanbase
{
namespace sql
{
class ObExprArraySort : public ObFuncExprOperator
{
public:
  explicit ObExprArraySort(common::ObIAllocator &alloc);
  explicit ObExprArraySort(common::ObIAllocator &alloc, ObExprOperatorType type,
                              const char *name, int32_t param_num, int32_t dimension);
  virtual ~ObExprArraySort();
  virtual int calc_result_type1(ObExprResType &type, ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx) const override;
  static int eval_array_sort(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  static int eval_array_sort_batch(const ObExpr &expr, ObEvalCtx &ctx,
                                      const ObBitVector &skip, const int64_t batch_size);
  static int eval_array_sort_vector(const ObExpr &expr, ObEvalCtx &ctx,
                                       const ObBitVector &skip, const EvalBound &bound);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprArraySort);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_ARRAY_SORT