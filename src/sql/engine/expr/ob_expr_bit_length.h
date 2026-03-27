/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef SRC_SQL_ENGINE_EXPR_OB_EXPR_BIT_LENGTH_H_
#define SRC_SQL_ENGINE_EXPR_OB_EXPR_BIT_LENGTH_H_
#include "sql/engine/expr/ob_expr_operator.h"
namespace oceanbase
{
namespace sql
{
class ObExprBitLength : public ObFuncExprOperator {
public:
  ObExprBitLength();
  explicit ObExprBitLength(common::ObIAllocator& alloc);
  virtual ~ObExprBitLength();
  virtual int calc_result_type1(ObExprResType& type, 
                                ObExprResType& text,
                                common::ObExprTypeCtx& type_ctx) const override;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int calc_null(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  static int calc_bit_length(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  static int calc_null_vector(VECTOR_EVAL_FUNC_ARG_DECL);
  static int calc_bit_length_vector(VECTOR_EVAL_FUNC_ARG_DECL);
  template<typename ArgVec, typename ResVec, bool isNull>
  static int vector_bit_length(VECTOR_EVAL_FUNC_ARG_DECL);
  private:
    DISALLOW_COPY_AND_ASSIGN(ObExprBitLength);                         
};
}
}
#endif /* SRC_SQL_ENGINE_EXPR_OB_EXPR_BIT_LENGTH_H_ */
