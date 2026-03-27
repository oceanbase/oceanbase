/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_EXPR_PRIOR_H_
#define _OB_EXPR_PRIOR_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprPrior : public ObExprOperator
{
  typedef common::ObExprStringBuf IAllocator;
public:
  explicit  ObExprPrior(common::ObIAllocator &alloc);
  virtual ~ObExprPrior() {};

  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx) const;

  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const;
  static int calc_prior_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
private:
  //  static int calc_(const common::ObObj &param, common::ObObj &res, common::ObCastCtx &cast_ctx);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprPrior) const;

};
}
}
#endif  /* _OB_EXPR_NEG_H_ */
