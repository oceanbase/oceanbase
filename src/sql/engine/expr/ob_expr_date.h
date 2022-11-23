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

#ifndef SRC_SQL_ENGINE_EXPR_OB_EXPR_DATE_H_
#define SRC_SQL_ENGINE_EXPR_OB_EXPR_DATE_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprDate : public ObFuncExprOperator
{
public:
  explicit  ObExprDate(common::ObIAllocator &alloc);
  virtual ~ObExprDate();
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual common::ObCastMode get_cast_mode() const { return CM_NULL_ON_WARN;}
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int eval_date(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum);
private :
  //disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprDate);
};

inline int ObExprDate::calc_result_type1(ObExprResType &type,
                                         ObExprResType &type1,
                                         common::ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  UNUSED(type1);
  type.set_date();
  type.set_scale(common::DEFAULT_SCALE_FOR_DATE);
  //set calc type
  type1.set_calc_type(common::ObDateType);
  type_ctx.set_cast_mode(type_ctx.get_cast_mode() | CM_NULL_ON_WARN);
  return common::OB_SUCCESS;
}
}
}


#endif /* SRC_SQL_ENGINE_EXPR_OB_EXPR_DATE_H_ */
