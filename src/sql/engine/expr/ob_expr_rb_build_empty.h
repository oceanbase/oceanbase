/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 * This file contains implementation for rb_build_empty.
 */

#ifndef OCEANBASE_SQL_OB_EXPR_RB_BUILD_EMPTY_
#define OCEANBASE_SQL_OB_EXPR_RB_BUILD_EMPTY_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprRbBuildEmpty : public ObFuncExprOperator
{
public:
  explicit ObExprRbBuildEmpty(common::ObIAllocator &alloc);
  virtual ~ObExprRbBuildEmpty();
  virtual int calc_result_type0(ObExprResType &type, common::ObExprTypeCtx &type_ctx) const;

  static int eval_rb_build_empty(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprRbBuildEmpty);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_RB_BUILD_EMPTY_