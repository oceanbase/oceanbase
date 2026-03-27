/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_PASSWORD_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_PASSWORD_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprPassword : public ObStringExprOperator
{
public:
  explicit ObExprPassword(common::ObIAllocator &alloc);
  virtual ~ObExprPassword();
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &text,
                                common::ObExprTypeCtx &type_ctx) const;

  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;

  static int eval_password(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &datum);

  DECLARE_SET_LOCAL_SESSION_VARS;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprPassword);
};

}
}
#endif
