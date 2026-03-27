/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_EXPR_SUBQUERY_NS_EQUAL_H
#define OCEANBASE_SQL_ENGINE_EXPR_SUBQUERY_NS_EQUAL_H

#include "sql/engine/expr/ob_expr_operator.h"
namespace oceanbase
{
namespace sql
{
class ObExprSubQueryNSEqual : public ObSubQueryRelationalExpr
{
public:
  explicit  ObExprSubQueryNSEqual(common::ObIAllocator &alloc);
  virtual ~ObExprSubQueryNSEqual();

  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override
  {
    return ObSubQueryRelationalExpr::cg_expr(op_cg_ctx, raw_expr, rt_expr);
  }
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprSubQueryNSEqual);
};
}
}
#endif /* OCEANBASE_SQL_ENGINE_EXPR_SUBQUERY_NS_EQUAL_H */


