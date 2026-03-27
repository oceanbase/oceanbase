/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_EXPR_USEC_TO_TIME_H_
#define OCEANBASE_SQL_OB_EXPR_USEC_TO_TIME_H_
#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprUsecToTime : public ObFuncExprOperator
{
public:
  explicit  ObExprUsecToTime(common::ObIAllocator &alloc);
  virtual ~ObExprUsecToTime();
  virtual int calc_result_type1(ObExprResType &type, ObExprResType &usec, common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                        ObExpr &rt_expr) const override;
  static int calc_usec_to_time_vector(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound);
  template <typename ArgVec, typename ResVec>
  static int do_calc_usec_to_time_vector(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound);
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprUsecToTime);
};

} //sql
} //oceanbase
#endif //OCEANBASE_SQL_OB_EXPR_USEC_TO_TIME_H_
