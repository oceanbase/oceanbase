/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_EXPR_FROM_TZ_H_
#define OCEANBASE_SQL_OB_EXPR_FROM_TZ_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprFromTz : public ObFuncExprOperator
{
public:
  explicit  ObExprFromTz(common::ObIAllocator &alloc);
  virtual ~ObExprFromTz() {}
  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &input1,
                                ObExprResType &input2,
                                common::ObExprTypeCtx &type_ctx) const;
  static int eval_from_tz_val(const common::ObOTimestampData &in_ts_val,
                              const common::ObString &tz_str,
                              const common::ObTimeZoneInfo *tz_info,
                              common::ObOTimestampData &out_ts_val);
  static int eval_from_tz(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                      ObExpr &expr) const override;
  DECLARE_SET_LOCAL_SESSION_VARS;
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprFromTz);
};

} //sql
} //oceanbase
#endif //OCEANBASE_SQL_OB_EXPR_FROM_TZ_H_
