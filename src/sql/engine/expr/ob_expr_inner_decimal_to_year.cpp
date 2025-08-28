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
 * This file contains implementation for _st_asewkb expr.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "ob_expr_inner_decimal_to_year.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
using namespace common;
namespace sql
{
ObExprInnerDecimalToYear::ObExprInnerDecimalToYear(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_INNER_DECIMAL_TO_YEAR, N_INNER_DECIMAL_TO_YEAR, 1, 
                         NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION,
                         INTERNAL_IN_MYSQL_MODE, INTERNAL_IN_ORACLE_MODE)
{
}
const int64_t YEAR_MIN = 1901; 
const int64_t YEAR_MAX = 2155;
const int64_t YEAR_BASE = 1900;
const uint8_t OBJ_YEAR_MIN = 1;
const uint8_t OBJ_YEAR_MAX = 255;
const uint8_t OBJ_YEAR_LOW = 0;

int ObExprInnerDecimalToYear::eval_inner_decimal_to_year(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum) 
{
  int ret = OB_SUCCESS;
  ObDatum* val_datum = NULL;
  int64_t year_int = 0;
  bool is_start = (expr.extra_ & 1) == 1;
  bool is_equal = (expr.extra_ & 2) == 2;
  if (OB_FAIL(expr.args_[0]->eval(ctx, val_datum))) {
    LOG_WARN("fail to eval conv", K(ret), K(expr));
  } else if (val_datum->is_null()) {
    expr_datum.set_null();
  } else {
    const number::ObNumber& val = val_datum->get_number();
    if (val.is_negative()) {
      if (is_equal) {
        expr_datum.set_year(is_start ? (int8_t)OBJ_YEAR_MAX : (int8_t)OBJ_YEAR_LOW);
      } else {
        expr_datum.set_year((int8_t)OBJ_YEAR_LOW);
      }
    } else if (OB_FAIL(val.extract_valid_int64_with_trunc(year_int))) {
      LOG_WARN("extract_valid_int64_with_trunc fail.", K(ret), K(year_int));
    } else if (val.is_valid_int()) {
      if (year_int > YEAR_BASE && year_int <= YEAR_MAX) {
        expr_datum.set_year((int8_t)(year_int - YEAR_BASE));
      } else if (year_int == 0) {
        expr_datum.set_year((int8_t)OBJ_YEAR_LOW);
      } else if (is_equal) {
        expr_datum.set_year(is_start ? (int8_t)OBJ_YEAR_MAX : (int8_t)OBJ_YEAR_LOW);
      } else if (year_int > YEAR_MAX) {
        expr_datum.set_year((int8_t)OBJ_YEAR_MAX);
      } else if (year_int <= YEAR_BASE && year_int > 0) {
        expr_datum.set_year(is_start ? (int8_t)OBJ_YEAR_MIN : (int8_t)OBJ_YEAR_LOW);
      }
    } else {
      if (is_equal) {
        expr_datum.set_year(is_start ? (int8_t)OBJ_YEAR_MAX : (int8_t)OBJ_YEAR_LOW);
      } else {
        if (year_int >= YEAR_BASE && year_int < YEAR_MAX) {
          year_int = is_start ? year_int + 1 : year_int;
          expr_datum.set_year((int8_t)(year_int - YEAR_BASE));
        } else if (year_int >= YEAR_MAX) {
          expr_datum.set_year((int8_t)OBJ_YEAR_MAX);
        } else if (year_int < YEAR_BASE && year_int >= 0) {
          expr_datum.set_year(is_start ? (int8_t)OBJ_YEAR_MIN : (int8_t)OBJ_YEAR_LOW);
        }
      }
    }
  }
  return ret;
}

int ObExprInnerDecimalToYear::calc_result_type1(ObExprResType &type,
                                                ObExprResType &type1,
                                                common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  type.set_year();
  type1.set_calc_type(ObNumberType);
  return ret;
}

int ObExprInnerDecimalToYear::cg_expr(ObExprCGCtx &expr_cg_ctx, 
                                      const ObRawExpr &raw_expr,
                                      ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(1 != rt_expr.arg_cnt_)) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_WARN("invalid arg cnt of expr", K(ret), K(rt_expr));
  } else if (OB_ISNULL(rt_expr.args_[0])) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("the arg of inner double to int is null.", K(ret), K(rt_expr));
  } else {
    rt_expr.eval_func_ = ObExprInnerDecimalToYear::eval_inner_decimal_to_year;
    rt_expr.extra_ = raw_expr.get_range_flag();
  }
  return ret;
}

}
}