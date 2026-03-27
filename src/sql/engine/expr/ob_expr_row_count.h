/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_SQL_EXPR_ROW_COUNT_H_
#define _OB_SQL_EXPR_ROW_COUNT_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprRowCount : public ObFuncExprOperator
{
public:
  explicit  ObExprRowCount(common::ObIAllocator &alloc);
  virtual ~ObExprRowCount();
  virtual int calc_result_type0(ObExprResType &type, common::ObExprTypeCtx &type_ctx) const;
  static int eval_row_count(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprRowCount);
};

}
}
#endif /* _OB_SQL_EXPR_ROW_COUNT_H_ */
