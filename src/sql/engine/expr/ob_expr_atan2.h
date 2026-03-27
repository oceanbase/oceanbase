/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_ATAN2_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_ATAN2_

#include "sql/engine/expr/ob_expr_operator.h"
namespace oceanbase
{
namespace sql
{
class ObExprAtan2 : public ObFuncExprOperator
{
public:
  explicit  ObExprAtan2(common::ObIAllocator &alloc);
  virtual ~ObExprAtan2();
  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types,
                                int64_t type_num,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;

  static int eval_number_atan2_vector(VECTOR_EVAL_FUNC_ARG_DECL);
  static int eval_double_atan2_vector(VECTOR_EVAL_FUNC_ARG_DECL);

private:
  template <typename Arg0Vec, typename ResVec>
  static int calc_one_param_vector(VECTOR_EVAL_FUNC_ARG_DECL);

  template <typename Arg0Vec, typename Arg1Vec, typename ResVec, bool IS_DOUBLE>
  static int calc_two_param_vector(VECTOR_EVAL_FUNC_ARG_DECL);

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprAtan2);
};

}
}
#endif /* OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_HEX_ */
