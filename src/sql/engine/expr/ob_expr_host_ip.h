/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_HOST_IP_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_HOST_IP_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprHostIP : public ObFuncExprOperator
{
public:
  explicit  ObExprHostIP(common::ObIAllocator &alloc);
  virtual ~ObExprHostIP();
  virtual int calc_result_type0(ObExprResType &type, common::ObExprTypeCtx &type_ctx) const;
  static int eval_host_ip(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprHostIP);
};
}
}
#endif // OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_HOST_IP_

