/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_RPC_PORT_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_RPC_PORT_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprRpcPort : public ObFuncExprOperator
{
public:
  explicit  ObExprRpcPort(common::ObIAllocator &alloc);
  virtual ~ObExprRpcPort();
  virtual int calc_result_type0(ObExprResType &type, common::ObExprTypeCtx &type_ctx) const;
  static int eval_rpc_port(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprRpcPort);
};
}
}
#endif // OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_RPC_PORT_

