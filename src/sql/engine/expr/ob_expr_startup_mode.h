/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_STARTUP_MODE_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_STARTUP_MODE_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{

class ObExprStartUpMode : public ObStringExprOperator
{
public:
  explicit ObExprStartUpMode(common::ObIAllocator &alloc);
  virtual ~ObExprStartUpMode();
  virtual int calc_result_type0(ObExprResType &type, common::ObExprTypeCtx &type_ctx) const;
  static int eval_startup_mode(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprStartUpMode);
};

}
}
#endif // OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_STARTUP_MODE_
