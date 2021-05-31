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
#include "sql/engine/expr/ob_expr_to_number.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "lib/number/ob_number_v2.h"
#include "lib/worker.h"
#include <ctype.h>

namespace oceanbase {
using namespace common;

namespace sql {

ObExprToNumberBase::ObExprToNumberBase(common::ObIAllocator& alloc, const ObExprOperatorType type, const char* name)
    : ObFuncExprOperator(alloc, type, name, ONE_OR_TWO, NOT_ROW_DIMENSION)
{}

ObExprToNumberBase::~ObExprToNumberBase()
{}

int ObExprToNumberBase::calc_result_typeN(
    ObExprResType& type, ObExprResType* types, int64_t param_num, ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!lib::is_oracle_mode())) {
    ret = OB_ERR_FUNCTION_UNKNOWN;
    LOG_WARN("to_char only support on oracle mode", K(type), K(ret));
  } else if (OB_UNLIKELY(NOT_ROW_DIMENSION != row_dimension_)) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("invalid row_dimension_", K(row_dimension_), K(ret));
  } else if (OB_UNLIKELY(param_num != 1 && param_num != 2) || OB_ISNULL(types)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(param_num), K(types));
  } else {
    if (types[0].is_varchar_or_char()) {
      types[0].set_calc_collation_utf8();
    } else if (param_num == 2 || (!types[0].is_number() && !types[0].is_number_float())) {
      types[0].set_calc_type_default_varchar();
    }
    if (param_num == 2) {
      if (types[1].is_varchar_or_char()) {
        types[1].set_calc_collation_utf8();
      } else {
        types[1].set_calc_type_default_varchar();
      }
    }
    type.set_type(ObNumberType);
    type.set_scale(ORA_NUMBER_SCALE_UNKNOWN_YET);
    type.set_precision(PRECISION_UNKNOWN_YET);
  }
  return ret;
}

ObExprToNumber::ObExprToNumber(ObIAllocator& alloc) : ObExprToNumberBase(alloc, T_FUN_SYS_TO_NUMBER, N_TO_NUMBER)
{}

ObExprToNumber::~ObExprToNumber()
{}

int ObExprToNumber::calc_resultN(ObObj& result, const ObObj* objs, int64_t param_num, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr_ctx.calc_buf_) || OB_UNLIKELY(param_num != 1 && param_num != 2) || OB_ISNULL(objs)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(param_num), K(expr_ctx.calc_buf_), K(objs));
  } else if (OB_UNLIKELY(param_num == 2 && objs[1].get_string_len() >= MAX_FMT_STR_LEN)) {
    ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
    LOG_WARN("fmt string is too long", K(ret), K(param_num), K(objs[1]));
  } else if (objs[0].is_null()) {
    result.set_null();
  } else {
    number::ObNumber res_nmb;
    const ObObjType& arg_type = objs[0].get_type();
    if (1 == param_num) {
      if (ob_is_varchar_or_char(arg_type, objs[0].get_collation_type())) {
        const ObString& str1 = objs[0].get_string();
        ObPrecision res_precision = -1;
        ObScale res_scale = -1;
        if (OB_FAIL(
                res_nmb.from_sci_opt(str1.ptr(), str1.length(), *(expr_ctx.calc_buf_), &res_precision, &res_scale))) {
          LOG_WARN("fail to calc function to_number", K(ret), K(str1));
        } else {
          result.set_number(res_nmb);
        }
      } else if (ObNumberTC == ob_obj_type_class(arg_type)) {
        if (OB_FAIL(res_nmb.from(objs[0].get_number(), *(expr_ctx.calc_buf_)))) {
          LOG_WARN("get nmb failed", K(ret));
        } else {
          result.set_number(res_nmb);
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected arg type", K(ret), K(arg_type));
      }
    } else {
      const ObString& str1 = objs[0].get_string();
      const ObString& str2 = objs[1].get_string();
      if (OB_FAIL(calc_(str1, str2, *(expr_ctx.calc_buf_), res_nmb))) {
        LOG_WARN("calc to number failed", K(ret), K(str1), K(str2));
      } else {
        result.set_number(res_nmb);
      }
    }
  }
  return ret;
}

int ObExprToNumber::calc_(
    const ObString& in_str, const ObString& in_fmt_str, ObIAllocator& alloc, number::ObNumber& res_nmb)
{
  int ret = OB_SUCCESS;
  ObPrecision res_precision = -1;
  ObScale res_scale = -1;
  number::ObNumberFmtModel fmt;
  int64_t length = in_fmt_str.length();
  const char* fmt_start = in_fmt_str.ptr();
  const char* fmt_end = fmt_start + length;
  char fmt_str[MAX_FMT_STR_LEN];
  int64_t i = 0;
  bool has_d = false;
  bool has_sign = false;
  bool has_currency = false;
  bool has_b = false;
  bool has_comma = false;
  bool has_x = false;
  int16_t dc_position = -1;  // dot character
  int16_t sign_position = -1;
  int16_t fmt_len = 0;
  fmt.fmt_str_ = &fmt_str[0];

  for (i = 0; ret == OB_SUCCESS && fmt_start < fmt_end; fmt_start++, ++i) {
    switch (fmt_start[0]) {
      case ',': {
        if (0 == i || has_d || has_x) {
          ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
          LOG_WARN("comma's position is invalid", K(ret), K(i), K(has_d));
        }
        fmt_str[fmt_len++] = ',';
        has_comma = true;
        break;
      }
      case 'D':
      case 'd':
      case '.': {
        if (has_d || has_x) {
          ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
          LOG_WARN("period apprears more than once", K(ret), K(i), K(has_d));
        } else {
          has_d = true;
          dc_position = fmt_len;
          fmt_str[fmt_len++] = '.';
        }
        break;
      }
      case '$': {
        if (has_currency || has_x) {
          ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
          LOG_WARN("dollar sign apprear more than once", K(ret), K(i), K(has_currency));
        } else {
          has_currency = true;
        }
        break;
      }
      case '0':
      case '9': {
        if (has_x) {
          ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
          LOG_WARN("dollar sign apprear more than once", K(ret), K(i), K(has_currency));
        }
        fmt_str[fmt_len++] = fmt_start[0];
        break;
      }
      case 'S':
      case 's': {
        /* sign can appear only in the first or last position of a number format model */
        if (has_sign || (i != 0 && i != length - 1) || has_x) {
          ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
          LOG_WARN("sign appears at wrong position ", K(ret), K(i), K(has_sign), K(length));
        } else {
          has_sign = true;
          sign_position = (i == 0 ? number::FmtFirst : number::FmtLast);
        }
        break;
      }
      case 'B':
      case 'b': {
        if (has_b || has_x) {
          ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
          LOG_WARN("B/b appear more than once ", K(ret), K(i), K(has_b));
        } else {
          has_b = true;
        }
        break;
      }
      case 'x':
      case 'X': {
        fmt_str[fmt_len++] = fmt_start[0];
        has_x = true;
        break;
      }
      default: {
        ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
        LOG_WARN("invalid fmt character ", K(ret), K(i), K(fmt_start[0]));
        break;
      }
    }
  }

  if (OB_SUCC(ret)) {
    fmt.dc_position_ = dc_position;
    fmt.sign_position_ = sign_position;
    fmt.fmt_len_ = fmt_len;
    fmt.has_b_ = has_b;
    fmt.has_currency_ = has_currency;
    fmt.has_d_ = has_d;
    fmt.has_sign_ = has_sign;
    fmt.has_comma_ = has_comma;
    fmt.has_x_ = has_x;
    LOG_DEBUG("user number format model",
        K(dc_position),
        K(sign_position),
        K(fmt_len),
        K(fmt_str),
        K(has_currency),
        K(has_d),
        K(has_sign),
        K(in_str.ptr()));
    if (OB_FAIL(res_nmb.from(in_str.ptr(), in_str.length(), alloc, &fmt, &res_precision, &res_scale))) {
      LOG_WARN("fail to calc function to_number with", K(ret), K(in_str), K(in_fmt_str));
    }
  }
  return ret;
}

int ObExprToNumber::calc_tonumber_expr(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  // to_number(ori, fmt)
  ObDatum* ori = NULL;
  ObDatum* fmt = NULL;
  bool is_null = false;
  if (OB_UNLIKELY(1 != expr.arg_cnt_ && 2 != expr.arg_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arg cnt", K(ret), K(expr.arg_cnt_));
  } else if (OB_FAIL(expr.eval_param_value(ctx, ori, fmt))) {
    LOG_WARN("eval param failed", K(ret));
  } else if (OB_UNLIKELY(expr.arg_cnt_ == 2 && fmt->get_string().length() >= MAX_FMT_STR_LEN)) {
    ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
  } else if (ori->is_null()) {
    res_datum.set_null();
  } else {
    const ObObjType& ori_type = expr.args_[0]->datum_meta_.type_;
    const ObCollationType& ori_cs_type = expr.args_[0]->datum_meta_.cs_type_;
    ObIAllocator& calc_alloc = ctx.get_reset_tmp_alloc();
    number::ObNumber res_nmb;
    if (1 == expr.arg_cnt_) {
      if (ob_is_varchar_or_char(ori_type, ori_cs_type)) {
        const ObString& ori_str = ori->get_string();
        ObNumStackOnceAlloc tmp_alloc;
        ObPrecision res_precision = -1;
        ObScale res_scale = -1;
        if (OB_FAIL(res_nmb.from_sci_opt(ori_str.ptr(), ori_str.length(), tmp_alloc, &res_precision, &res_scale))) {
          LOG_WARN("fail to calc function to_number", K(ret), K(ori_str));
        } else {
          res_datum.set_number(res_nmb);
        }
      } else if (ObNumberTC == ob_obj_type_class(ori_type)) {
        ObNumStackOnceAlloc tmp_alloc;
        if (OB_FAIL(res_nmb.from(ori->get_number(), tmp_alloc))) {
          LOG_WARN("get nmb failed", K(ret));
        } else {
          res_datum.set_number(res_nmb);
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected arg type", K(ret), K(ori_type));
      }
    } else {
      if (OB_ISNULL(fmt)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fmt datum is NULL", K(ret));
      } else {
        const ObString& ori_str = ori->get_string();
        const ObString& fmt_str = fmt->get_string();
        if (OB_FAIL(calc_(ori_str, fmt_str, calc_alloc, res_nmb))) {
          LOG_WARN("calc to number failed", K(ret), K(ori_str), K(fmt_str));
        } else {
          res_datum.set_number(res_nmb);
        }
      }
    }
  }
  return ret;
}

int ObExprToNumber::cg_expr(ObExprCGCtx& expr_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_tonumber_expr;
  return ret;
}

ObExprToBinaryFloat::ObExprToBinaryFloat(ObIAllocator& alloc)
    : ObExprToNumberBase(alloc, T_FUN_SYS_TO_BINARY_FLOAT, N_TO_BINARY_FLOAT)
{}

ObExprToBinaryFloat::~ObExprToBinaryFloat()
{}

int ObExprToBinaryFloat::calc_result_typeN(
    ObExprResType& type, ObExprResType* types, int64_t param_num, ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
  const ObSQLSessionInfo* session = type_ctx.get_session();
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else if (OB_UNLIKELY(param_num > 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(param_num), K(types));
  } else if (OB_FAIL(ObExprToNumberBase::calc_result_typeN(type, types, param_num, type_ctx))) {
    LOG_WARN("fail to calc_result_typeN", K(ret));
  } else if (OB_FAIL(ObObjCaster::can_cast_in_oracle_mode(ObFloatTC, types[0].get_type_class()))) {
    LOG_WARN("input type can not cast to binary_float", K(types[0].get_type()), K(ret));
  } else {
    if (session->use_static_typing_engine()) {
      types[0].set_calc_type(ObFloatType);
    } else {
      if (types[0].is_varchar_or_char()) {
        types[0].set_calc_collation_utf8();
      } else if (!types[0].is_float() && !types[0].is_double()) {
        types[0].set_calc_type_default_varchar();
      }
    }
    type.set_type(ObFloatType);
  }
  return ret;
}

int ObExprToBinaryFloat::calc_resultN(ObObj& result, const ObObj* objs, int64_t param_num, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr_ctx.calc_buf_) || OB_UNLIKELY(param_num != 1) || OB_ISNULL(objs)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(param_num), K(expr_ctx.calc_buf_), K(objs));
  } else if (objs[0].is_null()) {
    result.set_null();
  } else {
    float value = 0;
    EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
    EXPR_GET_FLOAT_V2(objs[0], value);
    if (OB_SUCC(ret)) {
      result.set_float(value);
    }
    LOG_DEBUG("finish to_binary_float", K(ret), K(param_num), K(objs[0]), K(value), K(result.get_float()));
  }
  return ret;
}

int ObExprToBinaryFloat::calc_to_binaryfloat_expr(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  // to_number(ori, fmt)
  ObDatum* ori = NULL;
  bool is_null = false;
  if (OB_UNLIKELY(1 != expr.arg_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arg cnt", K(ret), K(expr.arg_cnt_));
  } else if (OB_FAIL(expr.eval_param_value(ctx, ori))) {
    LOG_WARN("eval param failed", K(ret));
  } else if (ori->is_null()) {
    res_datum.set_null();
  } else {
    res_datum.set_float(ori->get_float());
  }
  return ret;
}

int ObExprToBinaryFloat::cg_expr(ObExprCGCtx& expr_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_to_binaryfloat_expr;
  return ret;
}

ObExprToBinaryDouble::ObExprToBinaryDouble(ObIAllocator& alloc)
    : ObExprToNumberBase(alloc, T_FUN_SYS_TO_BINARY_DOUBLE, N_TO_BINARY_DOUBLE)
{}

ObExprToBinaryDouble::~ObExprToBinaryDouble()
{}

int ObExprToBinaryDouble::calc_result_typeN(
    ObExprResType& type, ObExprResType* types, int64_t param_num, ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
  const ObSQLSessionInfo* session = type_ctx.get_session();
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else if (OB_UNLIKELY(param_num > 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(param_num), K(types));
  } else if (OB_FAIL(ObExprToNumberBase::calc_result_typeN(type, types, param_num, type_ctx))) {
    LOG_WARN("fail to calc_result_typeN", K(ret));
  } else if (OB_FAIL(ObObjCaster::can_cast_in_oracle_mode(ObDoubleTC, types[0].get_type_class()))) {
    LOG_WARN("input type can not cast to binary_double", K(types[0].get_type()), K(ret));
  } else {
    if (session->use_static_typing_engine()) {
      types[0].set_calc_type(ObDoubleType);
    } else {
      if (types[0].is_varchar_or_char()) {
        types[0].set_calc_collation_utf8();
      } else if (!types[0].is_float() && !types[0].is_double()) {
        types[0].set_calc_type_default_varchar();
      }
    }
    type.set_type(ObDoubleType);
  }
  return ret;
}

int ObExprToBinaryDouble::calc_resultN(ObObj& result, const ObObj* objs, int64_t param_num, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr_ctx.calc_buf_) || OB_UNLIKELY(param_num != 1) || OB_ISNULL(objs)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(param_num), K(expr_ctx.calc_buf_), K(objs));
  } else if (objs[0].is_null()) {
    result.set_null();
  } else {
    double value = 0;
    EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
    EXPR_GET_DOUBLE_V2(objs[0], value);
    if (OB_SUCC(ret)) {
      result.set_double(value);
    }
    LOG_DEBUG("finish to_binary_double", K(ret), K(param_num), K(objs[0]), K(value));
  }
  return ret;
}

int ObExprToBinaryDouble::calc_to_binarydouble_expr(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res_datum)
{
  int ret = OB_SUCCESS;
  // to_number(ori, fmt)
  ObDatum* ori = NULL;
  if (OB_UNLIKELY(1 != expr.arg_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arg cnt", K(ret), K(expr.arg_cnt_));
  } else if (OB_FAIL(expr.eval_param_value(ctx, ori))) {
    LOG_WARN("eval param failed", K(ret));
  } else if (ori->is_null()) {
    res_datum.set_null();
  } else {
    res_datum.set_double(ori->get_double());
  }
  return ret;
}

int ObExprToBinaryDouble::cg_expr(ObExprCGCtx& expr_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_to_binarydouble_expr;
  return ret;
}

} /* namespace sql */
} /* namespace oceanbase */
