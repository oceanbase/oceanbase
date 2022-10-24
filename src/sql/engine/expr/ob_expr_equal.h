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
