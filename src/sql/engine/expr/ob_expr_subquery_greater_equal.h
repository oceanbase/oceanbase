/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_EXPR_SUBQUERY_GREATER_EQUAL_H_
#define OCEANBASE_SQL_OB_EXPR_SUBQUERY_GREATER_EQUAL_H_

#include "lib/ob_name_def.h"
#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprSubQueryGreaterEqual : public ObSubQueryRelationalExpr
{
public:
  explicit  ObExprSubQueryGreaterEqual(common::ObIAllocator &alloc);
  virtual ~ObExprSubQueryGreaterEqual();

  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override
  {
    return ObSubQueryRelationalExpr::cg_expr(op_cg_ctx, raw_expr, rt_expr);
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprSubQueryGreaterEqual);
};
}  // namespace sql
}  // namespace oceanbase
#endif //OCEANBASE_SQL_OB_EXPR_SUBQUERY_GREATER_EQUAL_H_
