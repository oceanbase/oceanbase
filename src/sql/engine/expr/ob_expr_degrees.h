/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_EXPR_DEGREES_
#define OCEANBASE_SQL_ENGINE_EXPR_DEGREES_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{

class ObExprDegrees : public ObFuncExprOperator
{
public:
  explicit ObExprDegrees(common::ObIAllocator &alloc);
  virtual ~ObExprDegrees();
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &radian,
                                common::ObExprTypeCtx &type_ctx) const;
  static int check_overflow(const double res, const double val);
  static int calc_degrees_expr(const ObExpr &expr, ObEvalCtx &ctx,
                               ObDatum &res_datum);
  template <typename ArgVec, typename ResVec>
  static int vector_degrees(VECTOR_EVAL_FUNC_ARG_DECL);
  static int calc_degrees_vector_expr(VECTOR_EVAL_FUNC_ARG_DECL);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  const static double degrees_ratio_;
  DISALLOW_COPY_AND_ASSIGN(ObExprDegrees);
};

}
}
#endif
