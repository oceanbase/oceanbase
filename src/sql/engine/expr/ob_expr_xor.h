/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_EXPR_XOR_H_
#define _OB_EXPR_XOR_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprXor: public ObLogicalExprOperator
{
public:
  ObExprXor();
  explicit  ObExprXor(common::ObIAllocator &alloc);
  virtual ~ObExprXor() {};

  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types_stack,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int eval_xor(const ObExpr &expr,
                      ObEvalCtx &ctx,
                      ObDatum &expr_datum);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprXor);

  static const double FLOAT_BOUND;
};
}
}
#endif  /* _OB_EXPR_OR_H_ */
