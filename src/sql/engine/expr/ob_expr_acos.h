/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_ACOS_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_ACOS_

#include "sql/engine/expr/ob_expr_operator.h"
namespace oceanbase
{
namespace sql
{
class ObExprAcos : public ObFuncExprOperator
{
public:
  explicit  ObExprAcos(common::ObIAllocator &alloc);
  virtual ~ObExprAcos();
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const override;
  static int eval_number_acos_vector(const ObExpr &expr,
                                     ObEvalCtx &ctx,
                                     const ObBitVector &skip,
                                     const EvalBound &bound);
  static int eval_double_acos_vector(const ObExpr &expr,
                                     ObEvalCtx &ctx,
                                     const ObBitVector &skip,
                                     const EvalBound &bound);

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprAcos);
};

}
}
#endif /* OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_HEX_ */
