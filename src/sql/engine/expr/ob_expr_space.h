/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_SPACE_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_SPACE_

#include "sql/engine/expr/ob_expr_operator.h"
namespace oceanbase
{
namespace sql
{
class ObExprSpace : public ObStringExprOperator
{
public:

  explicit  ObExprSpace(common::ObIAllocator &alloc);
  virtual ~ObExprSpace() {};

  virtual int calc_result_type1(
      ObExprResType &type,
      ObExprResType &type1,
      common::ObExprTypeCtx &type_ctx) const;

  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;

  static int eval_space(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  static int eval_space_vector(VECTOR_EVAL_FUNC_ARG_DECL);
  DECLARE_SET_LOCAL_SESSION_VARS;
private:
  int calc_result_type(ObExprTypeCtx &type_ctx, const ObObj &obj, ObObjType &res_type) const;

  template <typename ArgVec, typename ResVec>
  static int space_vector(VECTOR_EVAL_FUNC_ARG_DECL);

  DISALLOW_COPY_AND_ASSIGN(ObExprSpace);
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_SPACE_
