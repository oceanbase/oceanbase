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

#include "objit/common/ob_item_type.h"
#include "lib/oblog/ob_log.h"
#include "lib/number/ob_number_v2.h"
#include "sql/engine/expr/ob_expr_cot.h"
#include "sql/session/ob_sql_session_info.h"
#include <math.h>

namespace oceanbase
{
using namespace common;
using namespace common::number;
namespace sql
{

ObExprCot::ObExprCot(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_COT, N_COT, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprCot::~ObExprCot()
{
}

int ObExprCot::calc_result_type1(ObExprResType &type,
                                 ObExprResType &radian,
                                 ObExprTypeCtx &type_ctx) const
{
  return calc_trig_function_result_type1(type, radian, type_ctx);
}

int ObExprCot::calc_cot_expr(const ObExpr &expr, ObEvalCtx &ctx,
                               ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *radian = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, radian))) {
    LOG_WARN("eval radian arg failed", K(ret), K(expr));
  } else if (radian->is_null()) {
    /* radian is already be cast to number type, no need to is_null_oracle */
    res_datum.set_null();
  } else if (ObDoubleType == expr.args_[0]->datum_meta_.type_) {
    const double arg = radian->get_double();
    double tan_out = tan(arg);
    // 检查 tan(arg)是否趋近于0，当太小时，就认为是0
    if (0.0 == tan_out) {
      ret = OB_DOUBLE_OVERFLOW;
      LOG_WARN("tan(x) is zero", K(ret));
    } else {
      double res = 1.0/tan_out;
      if (!std::isfinite(res)) {
        ret = OB_DOUBLE_OVERFLOW;
        LOG_WARN("tan(x) is zero", K(ret));
      } else {
        res_datum.set_double(res);
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
  }
  return ret;
}

int ObExprCot::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_cot_expr;
  return ret;
}

} /* sql */
} /* oceanbase */
