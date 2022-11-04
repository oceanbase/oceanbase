/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
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


