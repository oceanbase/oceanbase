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
 * This file contains implementation for point expr.
 */

#ifndef OCEANBASE_SQL_OB_EXPR_POINT_
#define OCEANBASE_SQL_OB_EXPR_POINT_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprPoint : public ObFuncExprOperator
{
public:
  explicit ObExprPoint(common::ObIAllocator &alloc);
  virtual ~ObExprPoint();
  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                common::ObExprTypeCtx &type_ctx) const override;
  virtual int calc_result2(common::ObObj &result,
                           const common::ObObj &obj1,
                           const common::ObObj &obj2,
                           common::ObExprCtx &expr_ctx) const override;
  static int eval_point(const ObExpr &expr,
                        ObEvalCtx &ctx,
                        ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprPoint);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_POINT_