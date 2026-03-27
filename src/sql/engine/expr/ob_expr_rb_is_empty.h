/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_EXPR_RB_IS_EMPTY_
#define OCEANBASE_SQL_OB_EXPR_RB_IS_EMPTY_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprRbIsEmpty : public ObFuncExprOperator
{
public:
  explicit ObExprRbIsEmpty(common::ObIAllocator &alloc);
  virtual ~ObExprRbIsEmpty();
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx)
                                const override;
  static int eval_rb_is_empty(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprRbIsEmpty);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_RB_IS_EMPTY_