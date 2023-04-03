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

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_RADIANS_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_RADIANS_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprRadians : public ObFuncExprOperator
{
public:
  explicit  ObExprRadians(common::ObIAllocator &alloc);
  virtual ~ObExprRadians();
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx) const;
  static int calc_radians_expr(const ObExpr &expr, ObEvalCtx &ctx,
                               ObDatum &res_datum);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const override;
private:
  const static double radians_ratio_;
  DISALLOW_COPY_AND_ASSIGN(ObExprRadians);
};

}
}
#endif /* OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_RADIANS_ */
