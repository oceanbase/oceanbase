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
#include "sql/engine/expr/ob_expr_log.h"
#include "sql/engine/expr/ob_expr_pow.h"
#include "sql/session/ob_sql_session_info.h"
#include "lib/number/ob_number_v2.h"
#include "sql/engine/expr/ob_expr_ln.h"

namespace oceanbase {
using namespace oceanbase::common;

namespace sql {
ObExprLog::ObExprLog(ObIAllocator& alloc) : ObExprOperator(alloc, T_FUN_SYS_LOG, N_LOG, 2, NOT_ROW_DIMENSION)
{}

int ObExprLog::calc_result_type2(
    ObExprResType& type, ObExprResType& type1, ObExprResType& type2, ObExprTypeCtx& type_ctx) const
{
  return calc_trig_function_result_type2(type, type1, type2, type_ctx);
}

int ObExprLog::calc_result2(ObObj& result, const ObObj& obj1, const ObObj& obj2, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;

  if (obj1.is_null() || obj2.is_null()) {
    result.set_null();
  } else if (obj1.is_number() && obj2.is_number()) {
    LOG_DEBUG("ObExprLog, both-ObNumber path start");
    const number::ObNumber base = obj1.get_number();
    const number::ObNumber x = obj2.get_number();
    number::ObNumber result_number;
    common::ObIAllocator& allocator = *(expr_ctx.calc_buf_);
    if (OB_FAIL(x.log(base, result_number, allocator))) {
      LOG_WARN("log failed", K(ret), K(x), K(base), K(result_number));
    } else {
      result.set_number(result_number);
      LOG_DEBUG("ObExprLog, ObNumber path end successfully");
    }
  } else if (obj1.is_double() && obj2.is_double()) {
    // same as oracle behavior: if at least one of the arguments is
    // binary_double, result type is double
    double double_base = obj1.get_double();
    double double_x = obj2.get_double();
    double result_double = std::log(double_x) / std::log(double_base);
    if (isinf(result_double) || isnan(result_double)) {
      ret = OB_OPERATE_OVERFLOW;
    } else {
      result.set_double(result_double);
    }
  }
  return ret;
}

int calc_log_expr_double(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum* base = NULL;
  ObDatum* x = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, base)) || OB_FAIL(expr.args_[1]->eval(ctx, x))) {
    LOG_WARN("eval arg failed", K(ret), K(expr));
  } else if (base->is_null() || x->is_null()) {
    res_datum.set_null();
  } else if (OB_FAIL(ObExprPow::safe_set_double(res_datum, std::log(x->get_double()) / std::log(base->get_double())))) {
    LOG_WARN("set double failed", K(ret), K(base->get_double()), K(x->get_double()));
  }
  return ret;
}

int calc_log_expr_number(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum* base = NULL;
  ObDatum* x = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, base)) || OB_FAIL(expr.args_[1]->eval(ctx, x))) {
    LOG_WARN("eval arg failed", K(ret), K(expr));
  } else if (base->is_null() || x->is_null()) {
    res_datum.set_null();
  } else {
    const number::ObNumber base_nmb(base->get_number());
    const number::ObNumber x_nmb(x->get_number());
    number::ObNumber res_nmb;
    if (OB_FAIL(x_nmb.log(base_nmb, res_nmb, ctx.get_reset_tmp_alloc()))) {
      LOG_WARN("calc log failed", K(ret), K(base_nmb), K(x_nmb), K(res_nmb));
    } else {
      res_datum.set_number(res_nmb);
    }
  }
  return ret;
}

int ObExprLog::cg_expr(ObExprCGCtx& expr_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  if (OB_UNLIKELY(2 != raw_expr.get_param_count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("raw_expr should got two arg", K(ret), K(raw_expr));
  } else {
    const ObObjType base_res_type = rt_expr.args_[0]->datum_meta_.type_;
    const ObObjType x_res_type = rt_expr.args_[1]->datum_meta_.type_;
    if (ObNumberType == base_res_type && ObNumberType == x_res_type) {
      rt_expr.eval_func_ = calc_log_expr_number;
    } else if (ObDoubleType == base_res_type && ObDoubleType == x_res_type) {
      rt_expr.eval_func_ = calc_log_expr_double;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid arg type", K(ret), K(rt_expr));
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
