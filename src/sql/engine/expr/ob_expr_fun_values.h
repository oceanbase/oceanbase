/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_SQL_EXPR_FUN_VALUES_H_
#define _OB_SQL_EXPR_FUN_VALUES_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprFunValues : public ObFuncExprOperator
{
public:
  explicit  ObExprFunValues(common::ObIAllocator &alloc);
  virtual ~ObExprFunValues();
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &text,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int eval_values(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprFunValues);
};
}
}
#endif /* _OB_SQL_EXPR_FUN_VALUES_H_ */
