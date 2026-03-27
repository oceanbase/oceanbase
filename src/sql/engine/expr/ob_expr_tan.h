/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_EXPR_TAN_
#define OCEANBASE_SQL_ENGINE_EXPR_TAN_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{

class ObExprTan : public ObFuncExprOperator
{
public:
  explicit  ObExprTan(common::ObIAllocator &alloc);
  virtual   ~ObExprTan();
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &radian,
                                common::ObExprTypeCtx &type_ctx) const;

  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprTan);
};

}
}
#endif /* OCEANBASE_SQL_ENGINE_EXPR_TAN_ */
