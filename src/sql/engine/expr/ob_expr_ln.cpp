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
#include <math.h>
#include "share/object/ob_obj_cast.h"
#include "sql/engine/expr/ob_expr_ln.h"
#include "sql/session/ob_sql_session_info.h"
#include "lib/number/ob_number_v2.h"

namespace oceanbase
{
using namespace oceanbase::common;

namespace sql
{
ObExprLn::ObExprLn(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_LN, N_LN, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprLn::~ObExprLn()
{
}

int ObExprLn::calc_result_type1(ObExprResType &type,
                                  ObExprResType &type1,
                                  common::ObExprTypeCtx &type_ctx) const
{
  return calc_trig_function_result_type1(type, type1, type_ctx);
}

int calc_ln_expr_mysql(const ObExpr &expr, ObEvalCtx &ctx,
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
      res_datum.set_double(std::log(val));
    }
  }
  return ret;
}

int calc_ln_expr_oracle_double(const ObExpr &expr, ObEvalCtx &ctx,
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
    if (val == 0) {
      res_datum.set_double(-INFINITY);
    } else if (val < 0) {
      res_datum.set_double(NAN);
    } else {
      res_datum.set_double(std::log(val));
    }
  }
  return ret;
}

int calc_ln_expr_oracle_number(const ObExpr &expr, ObEvalCtx &ctx,
                                ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *arg = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, arg))) {
    LOG_WARN("eval arg failed", K(ret), K(expr));
  } else if (arg->is_null()) {
    res_datum.set_null();
  } else {
    const number::ObNumber arg_nmb(arg->get_number());
    number::ObNumber res_nmb;
    ObEvalCtx::TempAllocGuard alloc_guard(ctx);
    if (OB_FAIL(arg_nmb.ln(res_nmb, alloc_guard.get_allocator()))) {
      LOG_WARN("calc ln failed", K(ret), K(arg_nmb), K(res_nmb));
    } else {
      res_datum.set_number(res_nmb);
    }
  }
  return ret;
}

int ObExprLn::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  if (OB_UNLIKELY(1 != rt_expr.arg_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arg_cnt_ of expr", K(ret), K(rt_expr));
  } else {
    ObObjType arg_res_type = rt_expr.args_[0]->datum_meta_.type_;
    if (!lib::is_oracle_mode()) {
      if (ObDoubleType == arg_res_type) {
        rt_expr.eval_func_ = calc_ln_expr_mysql;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("arg_res_type must be double in mysql mode", K(ret), K(arg_res_type));
      }
    } else {
      if (ObDoubleType == arg_res_type) {
        rt_expr.eval_func_ = calc_ln_expr_oracle_double;
      } else if (ObNumberType == arg_res_type) {
        rt_expr.eval_func_ = calc_ln_expr_oracle_number;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("arg type must be double or number in oracle mode", K(ret),
            K(arg_res_type), K(rt_expr));
      }
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
