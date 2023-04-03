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

#ifndef _OCEANBASE_SQL_OB_EXPR_DATE_DIFF_H_
#define _OCEANBASE_SQL_OB_EXPR_DATE_DIFF_H_
#include "lib/ob_name_def.h"
#include "share/object/ob_obj_cast.h"
#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprDateDiff : public ObFuncExprOperator
{
public:
  explicit  ObExprDateDiff(common::ObIAllocator &alloc);
  virtual ~ObExprDateDiff();
  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &left,
                                ObExprResType &right,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual common::ObCastMode get_cast_mode() const { return CM_NULL_ON_WARN;}
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int eval_date_diff(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum);
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprDateDiff);
};

inline int ObExprDateDiff::calc_result_type2(ObExprResType &type,
                                             ObExprResType &left,
                                             ObExprResType &right,
                                             common::ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  UNUSED(left);
  UNUSED(right);
  type.set_int();
  type.set_precision(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].precision_);
  type.set_scale(common::DEFAULT_SCALE_FOR_INTEGER);
  //set calc type
  left.set_calc_type(common::ObDateType);
  right.set_calc_type(common::ObDateType);
  return common::OB_SUCCESS;
}

class ObExprMonthsBetween : public ObFuncExprOperator
{
public:
  explicit ObExprMonthsBetween(common::ObIAllocator &alloc);
  virtual ~ObExprMonthsBetween();
  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int eval_months_between(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum);

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprMonthsBetween);
};

} //sql
} //oceanbase
#endif //_OCEANBASE_SQL_OB_EXPR_DATE_DIFF_H_
