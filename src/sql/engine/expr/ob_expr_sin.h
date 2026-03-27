/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_EXPR_SIN_
#define OCEANBASE_SQL_ENGINE_EXPR_SIN_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{

class ObExprSin : public ObFuncExprOperator
{
public:
  explicit  ObExprSin(common::ObIAllocator &alloc);
  virtual   ~ObExprSin();
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &radian,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const override;
  static int eval_number_sin_vector(const ObExpr &expr,
                                    ObEvalCtx &ctx,
                                    const ObBitVector &skip,
                                    const EvalBound &bound);
  static int eval_double_sin_vector(const ObExpr &expr,
                                    ObEvalCtx &ctx,
                                    const ObBitVector &skip,
                                    const EvalBound &bound);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprSin);
};

}
}
#endif /* OCEANBASE_SQL_ENGINE_EXPR_SIN_ */
