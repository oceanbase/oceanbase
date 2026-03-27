/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_EXPR_BIT_NEG_
#define OCEANBASE_SQL_ENGINE_EXPR_BIT_NEG_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprBitNeg : public ObBitwiseExprOperator
{
public:
  explicit  ObExprBitNeg(common::ObIAllocator &alloc);
  virtual ~ObExprBitNeg() {};

  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int calc_bitneg_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum& res_datum);
  static int calc_bitneg_expr_vector(VECTOR_EVAL_FUNC_ARG_DECL);
  template<typename ArgVec, typename ResVec>
  static int vector_bitneg(VECTOR_EVAL_FUNC_ARG_DECL);
  template<typename ArgVec, typename ResVec, bool Fixed>
  static int vector_bitneg_int_specific(VECTOR_EVAL_FUNC_ARG_DECL);

  DECLARE_SET_LOCAL_SESSION_VARS;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprBitNeg);
};
}
}
#endif /* OCEANBASE_SQL_ENGINE_EXPR_BIT_NEG_ */

