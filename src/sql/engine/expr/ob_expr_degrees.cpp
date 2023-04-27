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

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_degrees.h"
#include "lib/number/ob_number_v2.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "objit/common/ob_item_type.h"
#include "sql/session/ob_sql_session_info.h"
#include <cmath>

using namespace oceanbase::common;
using namespace oceanbase::sql;
namespace oceanbase
{
namespace sql
{

const double ObExprDegrees::degrees_ratio_ = 180.0/std::acos(-1);

ObExprDegrees::ObExprDegrees(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_DEGREES, N_DEGREES, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprDegrees::~ObExprDegrees()
{
}

int ObExprDegrees::calc_result_type1(ObExprResType &type,
                                    ObExprResType &radian,
                                    ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (NOT_ROW_DIMENSION != row_dimension_ || ObMaxType == radian.get_type()) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
  } else {
    type.set_double();
    radian.set_calc_type(ObDoubleType);
    ObExprOperator::calc_result_flag1(type, radian);
  }
  return ret;
}

int ObExprDegrees::calc_degrees_expr(const ObExpr &expr, ObEvalCtx &ctx,
                      ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum * radian = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, radian))) {
    LOG_WARN("eval radian arg failed", K(ret), K(expr));
  } else if (radian->is_null()) {
    res_datum.set_null();
  } else {
    const double val = radian->get_double();
    // cal result;
    res_datum.set_double(val * degrees_ratio_);
  }
  return ret;
}

int ObExprDegrees::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  if (OB_UNLIKELY(1 != rt_expr.arg_cnt_) ||
      (ObDoubleType != rt_expr.args_[0]->datum_meta_.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arg_cnt_ or res type is invalid", K(ret), K(rt_expr));
  } else {
    rt_expr.eval_func_ = calc_degrees_expr;
  }
  return ret;
}

}
}
