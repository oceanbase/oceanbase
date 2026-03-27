/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 * This file contains implementation for st_transform.
 */

#ifndef OCEANBASE_SQL_OB_EXPR_ST_TRANSFORM_
#define OCEANBASE_SQL_OB_EXPR_ST_TRANSFORM_

#include "sql/engine/expr/ob_expr_operator.h"
#include "observer/omt/ob_tenant_srs.h"

namespace oceanbase
{
namespace sql
{
class ObExprSTTransform : public ObFuncExprOperator
{
public:
  explicit ObExprSTTransform(common::ObIAllocator &alloc);
  virtual ~ObExprSTTransform() {}
  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                common::ObExprTypeCtx &type_ctx)
                                const override;
  static int eval_st_transform(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
    DISALLOW_COPY_AND_ASSIGN(ObExprSTTransform);
};
} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_ST_TRANSFORM_