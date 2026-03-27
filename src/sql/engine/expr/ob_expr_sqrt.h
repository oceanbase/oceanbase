/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_SQRT_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_SQRT_

#include "sql/engine/expr/ob_expr_operator.h"
namespace oceanbase
{
namespace sql
{
class ObExprSqrt : public ObFuncExprOperator
{
public:
  explicit  ObExprSqrt(common::ObIAllocator &alloc);
  virtual ~ObExprSqrt();
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprSqrt);
private:
};

}
}
#endif /* OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_HEX_ */
