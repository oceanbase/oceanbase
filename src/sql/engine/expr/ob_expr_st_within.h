/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 * This file contains implementation for eval_st_within.
 */

#ifndef OCEANBASE_SQL_OB_EXPR_ST_WITHIN_H_
#define OCEANBASE_SQL_OB_EXPR_ST_WITHIN_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprSTWithin : public ObFuncExprOperator
{
public:
  explicit ObExprSTWithin(common::ObIAllocator &alloc);
  virtual ~ObExprSTWithin();
  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                common::ObExprTypeCtx &type_ctx) const override;
  static int eval_st_within(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  virtual bool need_rt_ctx() const override { return true; }

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprSTWithin);
};
} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_ST_WITHIN_H_