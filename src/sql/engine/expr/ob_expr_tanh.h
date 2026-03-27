/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_TANH_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_TANH_

#include "sql/engine/expr/ob_expr_operator.h"
namespace oceanbase
{
namespace sql
{
class ObExprTanh : public ObFuncExprOperator
{
public:
  explicit  ObExprTanh(common::ObIAllocator &alloc);
  virtual ~ObExprTanh();
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprTanh);
};

}
}
#endif /* OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_TANH_ */
