/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 * This file contains implementation for rb_from_string.
 */

#ifndef OCEANBASE_SQL_OB_EXPR_RB_FROM_STRING_
#define OCEANBASE_SQL_OB_EXPR_RB_FROM_STRING_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprRbFromString : public ObFuncExprOperator
{
public:
  explicit ObExprRbFromString(common::ObIAllocator &alloc);
  virtual ~ObExprRbFromString();
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx)
                                const override;
  static int eval_rb_from_string(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprRbFromString);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_RB_FROM_STRING_