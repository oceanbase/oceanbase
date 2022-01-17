/*
 * Copyright 2014-2021 Alibaba Inc. All Rights Reserved.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * ob_expr_any_value.h is for any_value function
 *
 * Date: 2021/7/9
 *
 * Authors:
 *     ailing.lcq<ailing.lcq@alibaba-inc.com>
 *
 */
#ifndef _OB_EXPR_ANY_VALUE_H_
#define _OB_EXPR_ANY_VALUE_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase {
namespace sql {
class ObExprAnyValue : public ObFuncExprOperator {
public:
  explicit ObExprAnyValue(common::ObIAllocator &alloc);
  virtual ~ObExprAnyValue();

  virtual int calc_result_type1(
      ObExprResType &type, ObExprResType &arg, common::ObExprTypeCtx &type_ctx) const override;

  virtual int calc_result1(common::ObObj &result, const common::ObObj &arg, common::ObExprCtx &expr_ctx) const override;

  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const override;

  static int eval_any_value(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprAnyValue) const;
};
}  // namespace sql
}  // namespace oceanbase
#endif /* _OB_EXPR_ANY_VALUE_H_ */

// select any_value();