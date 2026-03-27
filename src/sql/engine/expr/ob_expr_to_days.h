/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef SRC_SQL_ENGINE_EXPR_OB_EXPR_TO_DAYS_H_
#define SRC_SQL_ENGINE_EXPR_OB_EXPR_TO_DAYS_H_
#include "sql/engine/expr/ob_expr_operator.h"
namespace oceanbase
{
namespace sql
{

class ObExprToDays: public ObFuncExprOperator
{
public:
  ObExprToDays();
  explicit  ObExprToDays(common::ObIAllocator &alloc);
  virtual ~ObExprToDays();
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &date,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                        ObExpr &rt_expr) const override;
  static int calc_to_days_vector(const ObExpr &expr, ObEvalCtx &ctx,
                                  const ObBitVector &skip, const EvalBound &bound);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprToDays);
};
}
}

#endif /* SRC_SQL_ENGINE_EXPR_OB_EXPR_TO_DAYS_H_ */
