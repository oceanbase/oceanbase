/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_EXPR_SUBQUERY_NOT_EQUAL_H_
#define OCEANBASE_SQL_OB_EXPR_SUBQUERY_NOT_EQUAL_H_

#include "sql/engine/expr/ob_expr_operator.h"
namespace oceanbase
{
namespace sql
{
class ObExprSubQueryNotEqual : public ObSubQueryRelationalExpr
{
public:
  explicit  ObExprSubQueryNotEqual(common::ObIAllocator &alloc);
  virtual ~ObExprSubQueryNotEqual();


  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override
  {
    return ObSubQueryRelationalExpr::cg_expr(op_cg_ctx, raw_expr, rt_expr);
  }
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprSubQueryNotEqual);
};
}  // namespace sql
}  // namespace oceanbase
#endif //OCEANBASE_SQL_OB_EXPR_SUBQUERY_NOT_EQUAL_H_
