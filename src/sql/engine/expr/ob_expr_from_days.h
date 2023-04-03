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

#ifndef SRC_SQL_ENGINE_EXPR_OB_EXPR_FROM_DAYS_H_
#define SRC_SQL_ENGINE_EXPR_OB_EXPR_FROM_DAYS_H_

#include "sql/engine/expr/ob_expr_operator.h"
namespace oceanbase
{
namespace sql
{
class ObExprFromDays : public ObFuncExprOperator
{
public:
  explicit  ObExprFromDays(common::ObIAllocator &alloc);
  virtual ~ObExprFromDays();
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &date,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int calc_fromdays(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprFromDays);
};

inline int ObExprFromDays::calc_result_type1(ObExprResType &type, ObExprResType &date, common::ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  UNUSED(date);
  type.set_date();
  type.set_scale(common::DEFAULT_SCALE_FOR_DATE);
  //set calc type
  date.set_calc_type(common::ObInt32Type);
  return common::OB_SUCCESS;
}
}
}


#endif /* SRC_SQL_ENGINE_EXPR_OB_EXPR_FROM_DAYS_H_ */
