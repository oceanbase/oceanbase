/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_EXPR_CURRENT_SCN_H_
#define OCEANBASE_SQL_OB_EXPR_CURRENT_SCN_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprCurrentScn : public ObFuncExprOperator
{
public:
  explicit  ObExprCurrentScn(common::ObIAllocator &alloc);
  virtual ~ObExprCurrentScn();
  virtual int calc_result_type0(ObExprResType &type, common::ObExprTypeCtx &type_ctx) const;
  static int eval_current_scn(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprCurrentScn);
};

} //sql
} //oceanbase
#endif //OCEANBASE_SQL_OB_EXPR_CURRENT_SCN_H_
