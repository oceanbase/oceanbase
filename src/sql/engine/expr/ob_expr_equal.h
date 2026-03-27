/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_EXPR_EQUAL_H_
#define OCEANBASE_SQL_OB_EXPR_EQUAL_H_

#include "lib/ob_name_def.h"
#include "share/object/ob_obj_cast.h"
#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprEqual: public ObRelationalExprOperator
{
public:
  ObExprEqual();
  explicit  ObExprEqual(common::ObIAllocator &alloc);
  virtual ~ObExprEqual() {};
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
              ObExpr &rt_expr) const override
  {
    return ObRelationalExprOperator::cg_expr(expr_cg_ctx, raw_expr, rt_expr);
  }

  static int calc(common::ObObj &result,
                  const common::ObObj &obj1,
                  const common::ObObj &obj2,
                  const common::ObCompareCtx &cmp_ctx,
                  common::ObCastCtx &cast_ctx);
  static int calc_cast(common::ObObj &result,
                       const common::ObObj &obj1,
                       const common::ObObj &obj2,
                       const common::ObCompareCtx &cmp_ctx,
                       common::ObCastCtx &cast_ctx);
  static int calc_without_cast(common::ObObj &result,
                       const common::ObObj &obj1,
                       const common::ObObj &obj2,
                       const common::ObCompareCtx &cmp_ctx,
                       bool &need_cast);
  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                common::ObExprTypeCtx &type_ctx) const;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprEqual);
};

} // end namespace sql
} // end namespace oceanbase

#endif //OCEANBASE_SQL_OB_EXPR_EQUAL_H_
