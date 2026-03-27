/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_SQL_EXPR_PAD_H_
#define _OB_SQL_EXPR_PAD_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprPad : public ObStringExprOperator
{
public:
  explicit  ObExprPad(common::ObIAllocator &alloc);
  virtual ~ObExprPad();
  virtual int calc_result_type3(ObExprResType &type, ObExprResType &source,
                                ObExprResType &padding_str,
                                ObExprResType &length,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const;
  static int calc_pad_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  DECLARE_SET_LOCAL_SESSION_VARS;
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprPad);
};
}
}
#endif /* _OB_SQL_EXPR_PAD_H_ */
