/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_EFFECTIVE_TENANT_ID_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_EFFECTIVE_TENANT_ID_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprEffectiveTenantId : public ObFuncExprOperator
{
public:
  explicit  ObExprEffectiveTenantId(common::ObIAllocator &alloc);
  virtual ~ObExprEffectiveTenantId();
  virtual int calc_result_type0(ObExprResType &type,
                                common::ObExprTypeCtx &type_ctx) const;
  static int eval_effective_tenant_id(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprEffectiveTenantId);
};
}
}
#endif /* OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_EFFECTIVE_TENANT_ID_ */

