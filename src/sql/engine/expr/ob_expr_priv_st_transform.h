/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_EXPR_PRIV_ST_TRANSFORM_
#define OCEANBASE_SQL_OB_EXPR_PRIV_ST_TRANSFORM_

#include "sql/engine/expr/ob_expr_operator.h"
#include "observer/omt/ob_tenant_srs.h"

namespace oceanbase
{
namespace sql
{
class ObExprPrivSTTransform : public ObFuncExprOperator
{
public:
  explicit ObExprPrivSTTransform(common::ObIAllocator &alloc);
  virtual ~ObExprPrivSTTransform() {}
  virtual int calc_result_typeN(ObExprResType& type,
                                ObExprResType* types,
                                int64_t param_num,
                                common::ObExprTypeCtx& type_ctx)
                                const override;
  static int eval_priv_st_transform(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
    DISALLOW_COPY_AND_ASSIGN(ObExprPrivSTTransform);
};
} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_PRIV_ST_TRANSFORM_