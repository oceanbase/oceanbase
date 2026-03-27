/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_CONNECTION_ID_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_CONNECTION_ID_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprConnectionId : public ObFuncExprOperator
{
public:
  explicit  ObExprConnectionId(common::ObIAllocator &alloc);
  virtual ~ObExprConnectionId();
  virtual int calc_result_type0(ObExprResType &type, common::ObExprTypeCtx &type_ctx) const;
  static int eval_connection_id(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprConnectionId);
};
}//sql
}//oceanbase
#endif /* OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_CONNECTION_ID_ */
