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

#define USING_LOG_PREFIX  SQL_ENG

#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_errno.h"
#include "lib/timezone/ob_time_convert.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObExprErrno::ObExprErrno(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_ERRNO, N_SYS_ERRNO, ONE_OR_TWO, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprErrno::~ObExprErrno()
{
}

int ObExprErrno::calc_result_typeN(ObExprResType &type,
                                   ObExprResType *types_array,
                                   int64_t param_num,
                                   ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  type.set_int();
  type.set_precision(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].precision_);
  type.set_scale(common::DEFAULT_SCALE_FOR_INTEGER);
  CK(OB_NOT_NULL(type_ctx.get_session()));
  for (int64_t i = 0; OB_SUCC(ret) && i < param_num; ++i) {
    auto &param = types_array[i];
    param.set_calc_type(ObNumberType);
  }
  return ret;
}

int ObExprErrno::cg_expr(ObExprCGCtx &ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  UNUSED(raw_expr);
  CK(1 == rt_expr.arg_cnt_ || 2 == rt_expr.arg_cnt_);
  OX(rt_expr.eval_func_ = eval_errno);
  return ret;
}

int ObExprErrno::get_value(const number::ObNumber &nmb, int64_t &value)
{
  int ret = OB_SUCCESS;
  value = 0;
  int64_t tmp = 0;
  if (!nmb.is_valid_int64(tmp)) { //based on the behaviour of mysql.
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("out of range", K(ret), K(nmb));
  } else {
    value = tmp;
  }
  return ret;
}

int ObExprErrno::conv2errno(int64_t value)
{
  int ret = OB_SUCCESS;
  if (value < 0) {
    ret = static_cast<int>(value);
  }
  return ret;
}

// trigger and exception using `select errno(-4038) from dual`
int ObExprErrno::eval_errno(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.eval_param_value(ctx))) {
    LOG_WARN("evaluate parameters failed", K(ret));
  } else {
    int64_t value = 0;
    res.set_null();
    if (1 == expr.arg_cnt_) {
      ObDatum *val = &expr.locate_param_datum(ctx, 0);
      if (val->is_null()) {
      } else if (OB_FAIL(get_value(number::ObNumber(val->get_number()), value))) {
        ret = OB_SUCCESS;
      } else {
        ret = conv2errno(value); // throw error if value < 0
        res.set_int(value); // abritary value is OK
      }
    } else if (2 == expr.arg_cnt_) {
      ObDatum *val = &expr.locate_param_datum(ctx, 0);
      ObDatum *timeout_val = &expr.locate_param_datum(ctx, 1);
      int64_t timeout = 0;
      if (val->is_null() || timeout_val->is_null()) {
      } else if (OB_FAIL(get_value(number::ObNumber(val->get_number()), value))) {
        ret = OB_SUCCESS;
      } else if (OB_FAIL(get_value(number::ObNumber(timeout_val->get_number()), timeout))) {
        ret = OB_SUCCESS;
      } else if (ctx.exec_ctx_.get_my_session()->get_query_start_time() + timeout > ObTimeUtility::current_time()) {
        ret = conv2errno(value); // throw error if value < 0
        res.set_int(value); // abritary value is OK
      } else {
        // time up! just eval the value and return the value, don't throw exception
        res.set_int(value);
      }
    }
  }
  return ret;
}


} //namespace sql
} //namespace oceanbase
