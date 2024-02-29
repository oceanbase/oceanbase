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

#ifndef _OB_EXPR_INNER_ROW_CMP_VAL_H_
#define _OB_EXPR_INNER_ROW_CMP_VAL_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprInnerRowCmpVal : public ObFuncExprOperator
{
public:
  explicit ObExprInnerRowCmpVal(common::ObIAllocator &alloc);
  virtual ~ObExprInnerRowCmpVal() {};

  virtual int calc_result_type3(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                ObExprResType &type3,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;

  static int eval_inner_row_cmp_val(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);

private:
  //disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprInnerRowCmpVal);
};
}
}
#endif  /* _OB_EXPR_INNER_ROW_CMP_VAL_H_ */
