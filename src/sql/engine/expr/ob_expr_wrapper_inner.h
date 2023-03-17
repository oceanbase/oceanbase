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

#ifndef OCEANBASE_EXPR_OB_EXPR_WRAPPER_INNER_H_
#define OCEANBASE_EXPR_OB_EXPR_WRAPPER_INNER_H_

#include "sql/engine/expr/ob_expr_operator.h"
namespace oceanbase
{
namespace sql
{

//
// remove_const() is added above const expr to resolve the const value overwrite problem
// in static typing engine. e.g.:
//
//   update t1 set (c1, c2) = (select 1, 2 from t2);
//
// will be rewrite to:
//   update t1 set (c1, c2) = (select remove_const(1), remove_const(2) from t2);
//
//
// remove_const(1), remove_const(2) will be overwrite when subquery is empty
// instead of const value: 1, 2
//
// see:
//
class ObExprWrapperInner : public ObFuncExprOperator
{
public:
  explicit ObExprWrapperInner(common::ObIAllocator &alloc);
  virtual ~ObExprWrapperInner() {}

  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &arg,
                                common::ObExprTypeCtx &type_ctx) const override;

  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;

  static int eval_wrapper_inner(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprWrapperInner);
};

} // namespace sql
} // namespace oceanbase
#endif // OCEANBASE_EXPR_OB_EXPR_WRAPPER_INNER_H_
