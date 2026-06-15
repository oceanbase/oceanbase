/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_IFNULL_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_IFNULL_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprIfNull : public ObFuncExprOperator
{
public:
  explicit  ObExprIfNull(common::ObIAllocator &alloc);
  virtual ~ObExprIfNull();
  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const;
  static int calc_ifnull_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum);
  static int eval_ifnull_vector(VECTOR_EVAL_FUNC_ARG_DECL);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprIfNull);
  static OB_INLINE void build_arg0_skip(const ObIVector *arg0_vec, VectorFormat fmt, const ObBitVector &skip, ObBitVector &skip_bmp, const EvalBound &bound);
  static OB_INLINE void build_arg1_skip(const ObBitVector &skip, ObBitVector &skip_bmp, const EvalBound &bound);
private:
  // data members
};

} // namespace sql
} // namespace oceanbase
#endif // OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_IFNULL_