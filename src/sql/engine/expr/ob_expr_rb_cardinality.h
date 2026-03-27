/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_EXPR_RB_CARDINALITY_
#define OCEANBASE_SQL_OB_EXPR_RB_CARDINALITY_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprRbCardinality : public ObFuncExprOperator
{
public:
  explicit ObExprRbCardinality(common::ObIAllocator &alloc);
  virtual ~ObExprRbCardinality();
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx)
                                const override;
  static int eval_rb_cardinality(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprRbCardinality);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_RB_CARDINALITY_