/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_ENGINE_EXPR_LENGTHB_
#define OCEANBASE_SQL_ENGINE_EXPR_LENGTHB_

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/expr/ob_expr_operator.h"
#include "share/ob_errno.h"

namespace oceanbase
{
namespace sql
{
class ObExprLengthb : public ObFuncExprOperator
{
public:
  explicit  ObExprLengthb(common::ObIAllocator &alloc);
  virtual ~ObExprLengthb();
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &text,
                                common::ObExprTypeCtx &type_ctx) const;
  static int calc(common::ObObj &result,
                  const common::ObObj &text,
                  common::ObExprCtx &expr_ctx);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const override;
  static int calc_lengthb_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprLengthb);
};

}
}
#endif /* OCEANBASE_SQL_ENGINE_EXPR_LENGTHB_ */
