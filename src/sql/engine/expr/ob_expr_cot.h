/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_EXPR_COT_
#define OCEANBASE_SQL_ENGINE_EXPR_COT_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{

class ObExprCot : public ObFuncExprOperator
{
public:
  explicit  ObExprCot(common::ObIAllocator &alloc);
  virtual   ~ObExprCot();
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &radian,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const override;
  static int calc_cot_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprCot);
};

}
}
#endif /* OCEANBASE_SQL_ENGINE_EXPR_COT_ */
