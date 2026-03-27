/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_EXPR_ORA_LOGIN_USER_H
#define _OB_EXPR_ORA_LOGIN_USER_H

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprOraLoginUser : public ObFuncExprOperator
{
public:
  explicit  ObExprOraLoginUser(common::ObIAllocator &alloc);
  virtual ~ObExprOraLoginUser();
  virtual int calc_result_type0(ObExprResType &type,
                                common::ObExprTypeCtx &type_ctx) const override;
  static int eval_ora_login_user(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const override;
private:
  static const int DEFAULT_LENGTH = 4000;
  DISALLOW_COPY_AND_ASSIGN(ObExprOraLoginUser);
};
}
}
#endif
