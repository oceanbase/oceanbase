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
#include "sql/engine/expr/ob_expr_pow.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase {
using namespace oceanbase::common;

namespace sql {

ObExprPow::ObExprPow(ObIAllocator& alloc) : ObExprOperator(alloc, T_OP_POW, N_POW, 2, NOT_ROW_DIMENSION)
{}

int ObExprPow::calc_result_type2(
    ObExprResType& type, ObExprResType& type1, ObExprResType& type2, ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  UNUSED(type2);
  const ObSQLSessionInfo* session = dynamic_cast<const ObSQLSessionInfo*>(type_ctx.get_session());
  if (!lib::is_mysql_mode()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pow is only for mysql mode", K(ret));
  } else if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cast basic session to sql session failed", K(ret));
  } else if (NOT_ROW_DIMENSION == row_dimension_) {
    if (ObMaxType == type1.get_type()) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
    } else {
      type.set_double();
      if (session->use_static_typing_engine()) {
        type1.set_calc_type(ObDoubleType);
        type2.set_calc_type(ObDoubleType);
        // pow donot return error code in mysql mode
        //  eg: insert into t1 select pow('a', 2);
        type_ctx.set_cast_mode(type_ctx.get_cast_mode() | CM_WARN_ON_FAIL);
      }
    }
    ObExprOperator::calc_result_flag2(type, type1, type2);
    // just keep enumset as origin
  } else {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;  // arithmetic not support row
  }
  return ret;
}

int ObExprPow::calc_result2(ObObj& result, const ObObj& obj1, const ObObj& obj2, ObExprCtx& expr_ctx) const
{
  return calc(result, obj1, obj2, expr_ctx);
}

int ObExprPow::safe_set_double(ObObj& result, double value)
{
  int ret = OB_SUCCESS;
  if (isinfl(value) || isnan(static_cast<float>(value))) {
    ret = OB_OPERATE_OVERFLOW;
  } else {
    result.set_double(value);
  }
  return ret;
}

int ObExprPow::safe_set_double(ObDatum& datum, double value)
{
  int ret = OB_SUCCESS;
  if (isinfl(value) || isnan(static_cast<float>(value))) {
    ret = OB_OPERATE_OVERFLOW;
  } else {
    datum.set_double(value);
  }
  return ret;
}

int ObExprPow::calc(ObObj& result, const ObObj& obj1, const ObObj& obj2, ObExprCtx& expr_ctx)
{
  int ret = OB_SUCCESS;
  if (obj1.is_null() || obj2.is_null()) {
    result.set_null();
  } else {
    double double_v1 = 0;
    double double_v2 = 0;
    EXPR_DEFINE_CAST_CTX(expr_ctx, CM_WARN_ON_FAIL);
    if (OB_SUCC(ret)) {
      EXPR_GET_DOUBLE_V2(obj1, double_v1);
    }
    if (OB_SUCC(ret)) {
      EXPR_GET_DOUBLE_V2(obj2, double_v2);
    }

    if (OB_SUCC(ret)) {
      ret = safe_set_double(result, pow(double_v1, double_v2));
    }
  }
  return ret;
}

int ObExprPow::calc_pow_expr(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  // pow(base, exp)
  ObDatum* base = NULL;
  ObDatum* exp = NULL;
  int ret_base = expr.args_[0]->eval(ctx, base);
  int ret_exp = expr.args_[1]->eval(ctx, exp);
  if (OB_SUCCESS == ret_base && OB_SUCCESS == ret_exp) {
    if (base->is_null() || exp->is_null()) {
      res_datum.set_null();
    } else {
      ret = ObExprPow::safe_set_double(res_datum, std::pow(base->get_double(), exp->get_double()));
    }
  }
  return ret;
}

int ObExprPow::cg_expr(ObExprCGCtx& expr_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  CK(2 == raw_expr.get_param_count());
  OX(rt_expr.eval_func_ = calc_pow_expr);
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
