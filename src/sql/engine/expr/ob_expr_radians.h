/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
  template <typename ArgVec, typename ResVec>
  static int vector_radians(VECTOR_EVAL_FUNC_ARG_DECL);
  static int calc_radians_vector_expr(VECTOR_EVAL_FUNC_ARG_DECL);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const override;
private:
  const static double radians_ratio_;
  DISALLOW_COPY_AND_ASSIGN(ObExprRadians);
};

}
}
#endif /* OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_RADIANS_ */
