/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_EXPR_TO_SINGLE_TYPE_H
#define _OB_EXPR_TO_SINGLE_TYPE_H

#include "sql/engine/expr/ob_expr_operator.h"
namespace oceanbase
{
namespace sql
{

class ObExprToSingleByte : public ObFuncExprOperator
{
public:
  explicit ObExprToSingleByte(common::ObIAllocator &alloc);
  virtual ~ObExprToSingleByte();
  int calc_result_type1(ObExprResType &type,
                        ObExprResType &type1,
                        common::ObExprTypeCtx &type_ctx) const;
  int cg_expr(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const;
  static int calc_to_single_byte(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprToSingleByte);
};

}
}

#endif // _OB_EXPR_TO_SINGLE_TYPE_H
