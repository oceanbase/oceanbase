/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef SRC_SQL_ENGINE_EXPR_OB_EXPR_QUARTER_H_
#define SRC_SQL_ENGINE_EXPR_OB_EXPR_QUARTER_H_
#include "sql/engine/expr/ob_expr_operator.h"
namespace oceanbase
{
namespace sql
{
// QUARTER(date)
// Returns the quarter of date
// range from 1 to 4
class ObExprQuarter : public ObFuncExprOperator {
public:
  ObExprQuarter();
  explicit ObExprQuarter(common::ObIAllocator& alloc);
  virtual ~ObExprQuarter();
  virtual int calc_result_type1(ObExprResType& type, 
                                ObExprResType& date,
                                common::ObExprTypeCtx& type_ctx) const override;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int calc_quater(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  static int calc_quarter_vector(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound);
  DECLARE_SET_LOCAL_SESSION_VARS;
  private:
    DISALLOW_COPY_AND_ASSIGN(ObExprQuarter);                         
};
}
}
#endif /* SRC_SQL_ENGINE_EXPR_OB_EXPR_QUARTER_H_ */
