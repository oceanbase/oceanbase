/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
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
