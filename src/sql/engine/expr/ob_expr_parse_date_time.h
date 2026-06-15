/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_EXPR_PARSE_DATE_TIME_H_
#define OCEANBASE_SQL_OB_EXPR_PARSE_DATE_TIME_H_

#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/expr/ob_expr_to_temporal_base.h"

namespace oceanbase
{
namespace sql
{
class ObExprParseDateTime : public ObFuncExprOperator
{
public:
  explicit  ObExprParseDateTime(common::ObIAllocator &alloc);
  virtual ~ObExprParseDateTime();
  virtual int calc_result_typeN(ObExprResType &type,
                                          ObExprResType *types,
                                          int64_t param_num,
                                          common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;

  DECLARE_SET_LOCAL_SESSION_VARS;

  static int calc(const ObExpr &expr, ObEvalCtx &ctx, bool &is_null, int64_t &res_int);
  static int calc_parse_date_time(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum);
  static int calc_parse_date_time_vector(const ObExpr &expr, ObEvalCtx &ctx,
                                        const ObBitVector &skip, const EvalBound &bound);


private:
  template <typename ArgVec1, typename ArgVec2, typename ResVec>
  static int vector_parse_date_time(const ObExpr &expr, ObEvalCtx &ctx,
                                   const ObBitVector &skip, const EvalBound &bound);
};


} //sql
} //oceanbase
#endif //OCEANBASE_SQL_OB_EXPR_PARSE_DATE_TIME_H_
