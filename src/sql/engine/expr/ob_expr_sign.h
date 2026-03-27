/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef SRC_SQL_ENGINE_EXPR_OB_EXPR_SIGN_H_
#define SRC_SQL_ENGINE_EXPR_OB_EXPR_SIGN_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprSign : public ObFuncExprOperator
{
public:
  explicit  ObExprSign(common::ObIAllocator &alloc);
  virtual ~ObExprSign();
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &text,
                                common::ObExprTypeCtx &type_ctx) const;
  int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
              ObExpr &rt_expr) const;
  template <typename ArgVec, typename ResVec, bool IS_INT_RES>
  static int vector_sign(VECTOR_EVAL_FUNC_ARG_DECL);
  static int eval_sign_vector_int(VECTOR_EVAL_FUNC_ARG_DECL);
  static int eval_sign_vector_number(VECTOR_EVAL_FUNC_ARG_DECL);
private:
  //help func
  static int calc(common::ObObj &reult, double val);
  DISALLOW_COPY_AND_ASSIGN(ObExprSign);
};

}
}
#endif /* SRC_SQL_ENGINE_EXPR_OB_EXPR_SIGN_H_ */
