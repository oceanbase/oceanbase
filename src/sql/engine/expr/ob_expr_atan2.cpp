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
#include "ob_expr_atan2.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "share/object/ob_obj_cast.h"
#include "objit/common/ob_item_type.h"
//#include "sql/engine/expr/ob_expr_promotion_util.h"
#include "sql/session/ob_sql_session_info.h"
#include <math.h>

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
ObExprAtan2::ObExprAtan2(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_ATAN2, N_ATAN2, ONE_OR_TWO, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
  param_lazy_eval_ = true;
}

ObExprAtan2::~ObExprAtan2()
{
}

int ObExprAtan2::calc_result_typeN(ObExprResType &type,
                                   ObExprResType *types,
                                   int64_t type_num,
                                   common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  if (lib::is_oracle_mode() && OB_UNLIKELY(NULL == types || type_num != 2)) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_WARN("Invalid argument.", K(ret), K(types), K(type_num));
  } else if (lib::is_mysql_mode()
             && OB_UNLIKELY(NULL == types || type_num <= 0 || type_num > 2)) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_WARN("Invalid argument.", K(ret), K(types), K(type_num));
  } else {
    if (1 == type_num) {
      ret = calc_trig_function_result_type1(type, types[0], type_ctx);
    } else {
      ret = calc_trig_function_result_type2(type, types[0], types[1], type_ctx);
    }
  }
  return ret;
}

int calc_atan2_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  if (1 == expr.arg_cnt_) {
    // only mysql mode
    ObDatum *radian = NULL;
    if (OB_FAIL(expr.args_[0]->eval(ctx, radian))) {
      LOG_WARN("eval radian arg failed", K(ret), K(expr));
    } else if (radian->is_null()) {
      res_datum.set_null();
    } else if (ObDoubleType == expr.args_[0]->datum_meta_.type_) {
      const double arg = radian->get_double();
      res_datum.set_double(atan(arg));
    }
  } else { // 2 == expr.arg_cnt_
    // calc atan2(y/x)
    ObExpr *arg0 = expr.args_[0];
    ObExpr *arg1 = expr.args_[1];
    ObDatum *y = NULL;
    ObDatum *x = NULL;
    if (OB_FAIL(arg0->eval(ctx, y))) {
      LOG_WARN("eval arg failed", K(ret), K(expr), KP(y));
    } else if (y->is_null()) {
      /* arg is already be cast to number type, no need to is_null_oracle */
      res_datum.set_null();
    } else if (OB_FAIL(arg1->eval(ctx, x))) {
      LOG_WARN("eval arg failed", K(ret), K(expr), KP(x));
    } else if (x->is_null()) {
      res_datum.set_null();
    } else if (ObNumberType == arg0->datum_meta_.type_
              && ObNumberType == arg1->datum_meta_.type_) {
      number::ObNumber y_nmb(y->get_number());
      number::ObNumber x_nmb(x->get_number());
      if (y_nmb.is_zero() && x_nmb.is_zero()) {
        ret = OB_NUMERIC_OVERFLOW;
        LOG_WARN("calc atan2(0,0) failed", K(ret));
      } else {
        number::ObNumber res_nmb;
        ObEvalCtx::TempAllocGuard alloc_guard(ctx);
        if (OB_FAIL(y_nmb.atan2(x_nmb, res_nmb, alloc_guard.get_allocator()))) {
          LOG_WARN("calc atan2 failed", K(ret), K(y_nmb), K(x_nmb), K(expr));
        } else {
          res_datum.set_number(res_nmb);
        }
      }
    } else if (ObDoubleType == arg0->datum_meta_.type_
              && ObDoubleType == arg1->datum_meta_.type_) {
      res_datum.set_double(atan2(y->get_double(), x->get_double()));
    } else {
      ret = OB_ERR_UNEXPECTED;
    }
  }
  return ret;
}

int ObExprAtan2::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                         ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  if (lib::is_oracle_mode() && OB_UNLIKELY(2 != rt_expr.arg_cnt_)) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_WARN("invalid arg cnt of expr", K(ret), K(rt_expr));
  } else if (lib::is_mysql_mode()
             && OB_UNLIKELY(1 != rt_expr.arg_cnt_ && 2 != rt_expr.arg_cnt_)) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_WARN("invalid arg cnt of expr", K(ret), K(rt_expr));
  } else {
    rt_expr.eval_func_ = calc_atan2_expr;
  }
  return ret;
}

} //namespace sql
} //namespace oceanbase
