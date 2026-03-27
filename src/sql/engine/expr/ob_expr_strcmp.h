/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef SRC_SQL_ENGINE_EXPR_OB_EXPR_STRCMP_H_
#define SRC_SQL_ENGINE_EXPR_OB_EXPR_STRCMP_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprStrcmp: public ObRelationalExprOperator
{
public:
  ObExprStrcmp();
  explicit  ObExprStrcmp(common::ObIAllocator &alloc);
  virtual ~ObExprStrcmp() {};
  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
              ObExpr &rt_expr) const override
  {
    return ObRelationalExprOperator::cg_expr(expr_cg_ctx, raw_expr, rt_expr);
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprStrcmp);
};
}
}

#endif /* SRC_SQL_ENGINE_EXPR_OB_EXPR_STRCMP_H_ */
