/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 * This file contains implementation for ob_expr_st_area.
 */

#ifndef OCEANBASE_SQL_OB_EXPR_ST_AREA_H_
#define OCEANBASE_SQL_OB_EXPR_ST_AREA_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprSTArea : public ObFuncExprOperator
{
public:
  explicit ObExprSTArea(common::ObIAllocator &alloc);
  virtual ~ObExprSTArea();
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx) const override;
  static int eval_st_area(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprSTArea);
};
} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_ST_AREA_H_