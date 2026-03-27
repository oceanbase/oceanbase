/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_EXPR_VOID_H_
#define _OB_EXPR_VOID_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprVoid : public ObFuncExprOperator
{
public:
  explicit ObExprVoid(common::ObIAllocator &alloc);
  virtual ~ObExprVoid() {};

  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx) const override;
  static int calc_void_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprVoid);
};
} // namespace sql
} // namespace oceanbase

#endif // _OB_EXPR_VOID_H_
