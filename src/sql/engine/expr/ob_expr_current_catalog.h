/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_CURRENT_CATALOG_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_CURRENT_CATALOG_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprCurrentCatalog : public ObFuncExprOperator
{
public:
  explicit  ObExprCurrentCatalog(common::ObIAllocator &alloc);
  virtual ~ObExprCurrentCatalog();
  virtual int calc_result_type0(ObExprResType &type,
                                common::ObExprTypeCtx &type_ctx) const;
  static int eval_current_catalog(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprCurrentCatalog);
};
}
}
#endif /* OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_CURRENT_CATALOG_ */
