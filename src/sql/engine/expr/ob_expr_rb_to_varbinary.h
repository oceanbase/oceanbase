/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_EXPR_RB_TO_VARBINARY_
#define OCEANBASE_SQL_OB_EXPR_RB_TO_VARBINARY_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprRbToVarbinary : public ObFuncExprOperator
{
public:
  explicit ObExprRbToVarbinary(common::ObIAllocator &alloc);
  virtual ~ObExprRbToVarbinary();
  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx)
                                const override;
  static int eval_rb_to_varbinary(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprRbToVarbinary);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_RB_TO_VARBINARY_