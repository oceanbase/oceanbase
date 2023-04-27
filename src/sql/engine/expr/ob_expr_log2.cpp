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
#include "ob_expr_log2.h"
#include <cmath>
#include "share/object/ob_obj_cast.h"
#include "objit/common/ob_item_type.h"
//#include "sql/engine/expr/ob_expr_promotion_util.h"
#include "sql/session/ob_sql_session_info.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
ObExprLog2::ObExprLog2(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_LOG_TWO, N_LOG2, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprLog2::~ObExprLog2()
{
}

int ObExprLog2::calc_result_type1(ObExprResType &type,
                                  ObExprResType &type1,
                                  common::ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (NOT_ROW_DIMENSION != row_dimension_ || ObMaxType == type1.get_type()) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
  } else {
    type.set_double();
    type1.set_calc_type(ObDoubleType);
    ObExprOperator::calc_result_flag1(type, type1);
  }
  return ret;
}

int calc_log2_expr(const ObExpr &expr, ObEvalCtx &ctx,
                                ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *arg = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, arg))) {
    LOG_WARN("eval arg failed", K(ret), K(expr));
  } else if (arg->is_null()) {
    res_datum.set_null();
  } else {
    double val = arg->get_double();
    if (val <= 0) {
      LOG_USER_WARN(OB_EER_INVALID_ARGUMENT_FOR_LOGARITHM);
      res_datum.set_null();
    } else {
      res_datum.set_double(std::log(val) / std::log(2));
    }
  }
  return ret;
}

int ObExprLog2::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  if (OB_UNLIKELY(1 != rt_expr.arg_cnt_) ||
      (ObDoubleType != rt_expr.args_[0]->datum_meta_.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arg_cnt_ or arg res type is invalid", K(ret), K(rt_expr));
  } else {
    rt_expr.eval_func_ = calc_log2_expr;
  }
  return ret;
}

} //namespace sql
} //namespace oceanbase
