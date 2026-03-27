/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 * This file contains implementation for array_avg expression.
 */

#ifndef OCEANBASE_SQL_OB_EXPR_ARRAY_AVG
#define OCEANBASE_SQL_OB_EXPR_ARRAY_AVG

#include "sql/engine/expr/ob_expr_operator.h"
#include "lib/udt/ob_array_type.h"

namespace oceanbase
{
namespace sql
{
class ObExprArrayAvg : public ObFuncExprOperator
{
public:
  explicit ObExprArrayAvg(common::ObIAllocator &alloc);
  virtual ~ObExprArrayAvg();

  virtual int calc_result_type1(ObExprResType &type, ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx) const override;



  static int eval_array_avg(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  static int eval_array_avg_batch(const ObExpr &expr, ObEvalCtx &ctx,
                                  const ObBitVector &skip, const int64_t batch_size);
  static int eval_array_avg_vector(const ObExpr &expr, ObEvalCtx &ctx,
                                   const ObBitVector &skip, const EvalBound &bound);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprArrayAvg);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_ARRAY_AVG