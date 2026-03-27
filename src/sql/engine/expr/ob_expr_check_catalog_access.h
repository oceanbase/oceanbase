/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_CHECK_CATALOG_ACCESS_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_CHECK_CATALOG_ACCESS_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprCheckCatalogAccess : public ObFuncExprOperator
{
public:
  explicit  ObExprCheckCatalogAccess(common::ObIAllocator &alloc);
  virtual ~ObExprCheckCatalogAccess();
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx) const;
  static int eval_check_catalog_access(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprCheckCatalogAccess);
};
}
}
#endif /* OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_CHECK_CATALOG_ACCESS_ */
