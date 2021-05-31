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

#include "sql/engine/expr/ob_expr_oracle_to_char.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/timezone/ob_time_convert.h"
#include "lib/timezone/ob_oracle_format_models.h"
#include "lib/ob_name_def.h"
#include "sql/session/ob_sql_session_info.h"
#include "lib/number/ob_number_v2.h"
#include "lib/charset/ob_dtoa.h"
#include "ob_expr_util.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase {
using namespace common;
namespace sql {

const int64_t MAX_NUMBER_BUFFER_SIZE = 40;
const int64_t MAX_DATETIME_BUFFER_SIZE = 100;
const int64_t MAX_TO_CHAR_BUFFER_SIZE = 256;
const int64_t MAX_TO_CHAR_LARGE_BUFFER_SIZE = 512;
const int64_t MAX_INTERVAL_BUFFER_SIZE = 32;
const int64_t MAX_CLOB_BUFFER_SIZE = 4000;

static bool has_format_param(const int64_t param_num)
{
  return param_num > 1;
}

ObExprOracleToChar::ObExprOracleToChar(ObIAllocator& alloc)
    : ObExprToCharCommon(alloc, T_FUN_SYS_TO_CHAR, N_TO_CHAR, MORE_THAN_ZERO)
{}

ObExprOracleToChar::~ObExprOracleToChar()
{}

int ObExprOracleToChar::calc_result_typeN(
    ObExprResType& type, ObExprResType* type_array, int64_t params_count, ObExprTypeCtx& type_ctx) const
{
  // https://docs.oracle.com/cd/B19306_01/server.102/b14200/functions180.htm
  int ret = OB_SUCCESS;
  ObCollationType collation_connection = type_ctx.get_coll_type();
  CK(NULL != type_ctx.get_session());
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(NULL == type_array || params_count < 1 || params_count > 3)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument.", K(ret), K(type_array), K(params_count));
  } else if (OB_ISNULL(type_ctx.get_session())) {
    LOG_WARN("session is null", K(ret));
  } else {
    ObSessionNLSParams nls_param = type_ctx.get_session()->get_session_nls_params();
    const ObObjTypeClass type_class = type_array[0].get_type_class();
    switch (type_class) {
      case ObNullTC: {
        type.set_null();
        break;
      }
      case ObIntTC:
      case ObUIntTC:
      case ObFloatTC:
      case ObDoubleTC:
      case ObNumberTC:
      case ObStringTC:
      case ObTextTC:
      case ObDateTimeTC:
      case ObOTimestampTC:
      case ObIntervalTC: {
        if (type_array[0].is_varchar_or_char()) {
          type.set_type_simple(type_array[0].get_type());
          type.set_length_semantics(type_array[0].get_length_semantics());
        } else {
          type.set_varchar();
          type.set_length_semantics(nls_param.nls_length_semantics_);
        }
        type.set_collation_level(CS_LEVEL_IMPLICIT);
        type.set_collation_type(nls_param.nls_collation_);
        if (1 == params_count && (ob_is_numeric_tc(type_class))) {
          type.set_length(MAX_NUMBER_BUFFER_SIZE);
        } else if (ObStringTC == type_class) {
          if (OB_FAIL(calc_result_length_for_string_param(type, type_array[0]))) {
            LOG_WARN("calc reuslt length failed", K(ret));
          }
        } else if (ObTextTC == type_class) {
          type.set_length(MAX_CLOB_BUFFER_SIZE);
        } else {
          type.set_length(MAX_TO_CHAR_BUFFER_SIZE);
        }
        break;
      }
      default: {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("input type not supported", K(ret), K(type_class));
        break;
      }
    }
  }
  if (type_array[0].is_string_type()) {
    type_array[0].set_calc_meta(type);
  }
  if (OB_SUCC(ret) && type_ctx.get_session()->use_static_typing_engine() && params_count >= 2) {
    // in static typing engine, we use the implicit cast to cast string to number.
    const ObObjTypeClass tc = type_array[0].get_type_class();
    if (ObStringTC == tc || ObTextTC == tc || ObIntTC == tc || ObUIntTC == tc) {
      type_array[0].set_calc_type(ObNumberType);
      type_array[0].set_calc_scale(NUMBER_SCALE_UNKNOWN_YET);
    }
  }
  if (OB_SUCC(ret) && params_count >= 2) {
    // using null and '' as format string has different behaviors in oracle
    // however OB takes '' as null at the present
    // to avoid generating incorrect results, we do not support using null as format string.
    if (type_array[1].is_null() && !type_array[0].is_oracle_temporal_type()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("do not support use null or empty string as format string", K(ret));
    } else {
      type_array[1].set_calc_type_default_varchar();
    }
  }
  if (OB_SUCC(ret) && params_count == 3) {
    type_array[2].set_calc_type_default_varchar();
  }
  return ret;
}

int ObExprOracleToChar::calc_resultN(
    ObObj& result, const ObObj* objs_array, int64_t param_num, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  const ObObj& ori_value = objs_array[0];

  if (OB_UNLIKELY(NULL == objs_array || param_num < 1 || param_num > 3) || OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument.", K(ret), K(objs_array), K(param_num), K(expr_ctx.calc_buf_));
  } else if (ori_value.is_null()) {
    result.set_null();
  } else if (!has_format_param(param_num) && ori_value.is_string_type()) {
    // output without convert
    if (OB_FAIL(ob_write_obj(*expr_ctx.calc_buf_, ori_value, result))) {
      LOG_WARN("failed to write obj", K(ret));
    } else {
      result.set_collation(result_type_);
    }
  } else {
    switch (ori_value.get_type_class()) {
      case ObDateTimeTC:
      case ObOTimestampTC: {
        if (OB_FAIL(datetime_to_char(result, objs_array, param_num, expr_ctx))) {
          LOG_WARN("failed to convert datetime to char", K(ret));
        }
        break;
      }
      case ObIntervalTC: {
        if (OB_FAIL(interval_to_char(result, objs_array, param_num, expr_ctx))) {
          LOG_WARN("failed to convert interval to char", K(ret));
        }
        break;
      }
      case ObFloatTC:
      case ObDoubleTC:
      case ObStringTC:
      case ObTextTC:
      case ObIntTC:  // to support PLS_INTERGER type
      case ObNumberTC: {
        if (OB_FAIL(is_valid_to_char_number(objs_array, param_num))) {
          LOG_WARN("failed to check is valid to char number", K(ret));
        } else if (OB_FAIL(number_to_char(result, objs_array, param_num, expr_ctx))) {
          LOG_WARN("failed to convert number to char", K(ret));
        }
        break;
      }
      default:
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("unsupport to_char", "from type class", ori_value.get_type_class(), K(ret));
    }
    if (OB_SUCC(ret) && !result.is_null()) {
      result.set_collation_type(ObCharset::get_default_collation_oracle(CHARSET_UTF8MB4));
      if (OB_FAIL(convert_result_collation(result_type_, result, expr_ctx.calc_buf_))) {
        LOG_WARN("fail to calc result collation", K(ret));
      }
      result.set_collation(result_type_);
    }
  }

  return ret;
}

ObExprOracleToNChar::ObExprOracleToNChar(ObIAllocator& alloc)
    : ObExprToCharCommon(alloc, T_FUN_SYS_TO_NCHAR, N_TO_NCHAR, MORE_THAN_ZERO)
{}

ObExprOracleToNChar::~ObExprOracleToNChar()
{}

int ObExprOracleToNChar::calc_result_typeN(
    ObExprResType& type, ObExprResType* type_array, int64_t params_count, ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == type_array || params_count < 1 || params_count > 3)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument.", K(ret), K(type_array), K(params_count));
  } else if (OB_ISNULL(type_ctx.get_session())) {
    LOG_WARN("session is null", K(ret));
  } else {
    ObSessionNLSParams nls_param = type_ctx.get_session()->get_session_nls_params();
    const ObObjTypeClass type_class = type_array[0].get_type_class();
    switch (type_class) {
      case ObNullTC: {
        type.set_null();
        break;
      }
      case ObIntTC:
      case ObUIntTC:
      case ObFloatTC:
      case ObDoubleTC:
      case ObNumberTC:
      case ObStringTC:
      case ObTextTC:
      case ObDateTimeTC:
      case ObOTimestampTC:
      case ObIntervalTC: {
        if (type_array[0].is_nstring()) {
          type.set_type_simple(type_array[0].get_type());
        } else {
          type.set_nvarchar2();
        }
        type.set_length_semantics(LS_CHAR);
        type.set_collation_level(CS_LEVEL_IMPLICIT);
        type.set_collation_type(nls_param.nls_nation_collation_);
        if (1 == params_count && (ob_is_numeric_tc(type_class))) {
          type.set_length(MAX_NUMBER_BUFFER_SIZE);
        } else if (ObStringTC == type_class) {
          if (OB_FAIL(calc_result_length_for_string_param(type, type_array[0]))) {
            LOG_WARN("calc reuslt length failed", K(ret));
          }
        } else if (ObTextTC == type_class) {
          type.set_length(MAX_CLOB_BUFFER_SIZE);
        } else {
          type.set_length(MAX_TO_CHAR_BUFFER_SIZE);
        }
        break;
      }
      default: {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("input type not supported", K(ret), K(type_class));
        break;
      }
    }
  }
  if (type_array[0].is_string_type()) {
    type_array[0].set_calc_meta(type);
  }
  if (OB_SUCC(ret) && type_ctx.get_session()->use_static_typing_engine() && params_count >= 2) {
    // in static typing engine, we use the implicit cast to cast string to number.
    const ObObjTypeClass tc = type_array[0].get_type_class();
    if (ObStringTC == tc || ObTextTC == tc || ObIntTC == tc || ObUIntTC == tc) {
      type_array[0].set_calc_type(ObNumberType);
      type_array[0].set_calc_scale(NUMBER_SCALE_UNKNOWN_YET);
    }
  }
  if (OB_SUCC(ret) && params_count >= 2) {
    // using null and '' as format string has different behaviors in oracle
    // however OB takes '' as null at the present
    // to avoid generating incorrect results, we do not support using null as format string.
    if (type_array[1].is_null() && !type_array[0].is_oracle_temporal_type()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("do not support use null or empty string as format string", K(ret));
    } else {
      type_array[1].set_calc_type_default_varchar();
    }
  }
  if (OB_SUCC(ret) && params_count == 3) {
    type_array[2].set_calc_type_default_varchar();
  }
  return ret;
}

int ObExprOracleToNChar::calc_resultN(
    ObObj& result, const ObObj* objs_array, int64_t param_num, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  const ObObj& ori_value = objs_array[0];
  if (OB_UNLIKELY(NULL == objs_array || param_num < 1 || param_num > 3) || OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument.", K(ret), K(objs_array), K(param_num), K(expr_ctx.calc_buf_));
  } else if (ori_value.is_null()) {
    result.set_null();
  } else if (!has_format_param(param_num) && ori_value.is_string_type()) {
    // output without convert
    if (OB_FAIL(ob_write_obj(*expr_ctx.calc_buf_, ori_value, result))) {
      LOG_WARN("failed to write obj", K(ret));
    } else {
      result.set_collation(result_type_);
    }
  } else {
    switch (ori_value.get_type_class()) {
      case ObDateTimeTC:
      case ObOTimestampTC: {
        if (OB_FAIL(datetime_to_char(result, objs_array, param_num, expr_ctx))) {
          LOG_WARN("failed to convert datetime to char", K(ret));
        }
        break;
      }
      case ObIntervalTC: {
        if (OB_FAIL(interval_to_char(result, objs_array, param_num, expr_ctx))) {
          LOG_WARN("failed to convert interval to char", K(ret));
        }
        break;
      }
      case ObFloatTC:
      case ObDoubleTC:
      case ObStringTC:
      case ObTextTC:
      case ObIntTC:  // to support PLS_INTERGER type
      case ObNumberTC: {
        if (OB_FAIL(is_valid_to_char_number(objs_array, param_num))) {
          LOG_WARN("failed to check is valid to char number", K(ret));
        } else if (OB_FAIL(number_to_char(result, objs_array, param_num, expr_ctx))) {
          LOG_WARN("failed to convert number to char", K(ret));
        }
        break;
      }
      default:
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("unsupport to_char", "from type class", ori_value.get_type_class(), K(ret));
    }
    if (OB_SUCC(ret) && !result.is_null()) {
      result.set_collation_type(ObCharset::get_default_collation_oracle(CHARSET_UTF8MB4));
      if (OB_FAIL(convert_result_collation(result_type_, result, expr_ctx.calc_buf_))) {
        LOG_WARN("fail to calc result collation", K(ret));
      }
      result.set_collation(result_type_);
    }
  }

  return ret;
}

int ObExprToCharCommon::is_valid_to_char_number(const ObObj* objs_array, const int64_t param_num)
{
  int ret = OB_SUCCESS;
  bool is_valid = false;
  if (OB_ISNULL(objs_array) || OB_UNLIKELY(param_num < 1 || param_num > 3)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument.", K(ret), K(objs_array), K(param_num));
  } else if (OB_UNLIKELY(3 == param_num)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("does not support number to char with nlparam", K(ret));
  } else if (objs_array[0].is_numeric_type()) {
    is_valid = true;
  } else if (ObStringTC == objs_array[0].get_type_class() && param_num >= 2) {
    is_valid = (ObNullTC == objs_array[1].get_type_class() || ObStringTC == objs_array[1].get_type_class());

    if (ObStringTC == objs_array[1].get_type_class()) {
      ObString fmt_raw;
      if (OB_FAIL(objs_array[1].get_string(fmt_raw))) {
        LOG_WARN("failed to get string", K(ret));
      } else {
        int64_t i = 0;
        if (fmt_raw.length() >= 2 && ('F' == fmt_raw[0] || 'f' == fmt_raw[0]) &&
            ('M' == fmt_raw[1] || 'm' == fmt_raw[1])) {
          i = 2;
        }
        for (; is_valid && i < fmt_raw.length(); ++i) {
          is_valid = (fmt_raw[i] == '0' || fmt_raw[i] == '9' || fmt_raw[i] == '.');
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(!is_valid)) {
      ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
      LOG_WARN("failed to check is valid to char number", K(ret));
    }
  }
  return ret;
}

// the length of the deduced result used for to_char, to_nchar, translate using expressions
// when the parameter is string/raw tc. need to set the result type, collation_type and
// length_semantics before calling
int ObExprToCharCommon::calc_result_length_for_string_param(ObExprResType& type, const ObExprResType& param)
{
  int ret = OB_SUCCESS;
  int64_t mbmaxlen = 1;
  int64_t mbminlen = 1;
  int64_t param_length = (param.is_raw() ? 2 : 1) * param.get_accuracy().get_length();
  if (OB_UNLIKELY(CS_TYPE_INVALID == type.get_collation_type() || CS_TYPE_INVALID == param.get_collation_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("result collation type should be valid", K(ret));
  } else if (param.is_nstring() || LS_CHAR == param.get_length_semantics()) {
    if (type.is_nstring() || LS_CHAR == type.get_length_semantics()) {
      // the parameter is LS_CHAR, and when the result is LS_CHAR, directly assign length
      type.set_length(param_length);
    } else if (OB_FAIL(ObCharset::get_mbmaxlen_by_coll(type.get_collation_type(), mbmaxlen))) {
      LOG_WARN("get mbmaxlen by coll failed", K(ret), K(type));
    } else {
      // if the parameter is LS_CHAR, and the result is LS_BYTE
      // set length to param_length * mbmaxlen
      type.set_length(param_length * mbmaxlen);
    }
  } else {
    if (type.is_nstring() || LS_CHAR == type.get_length_semantics()) {
      if (OB_FAIL(ObCharset::get_mbminlen_by_coll(param.get_collation_type(), mbminlen))) {
        LOG_WARN("get mbminlen by coll failed", K(ret), K(type));
      } else {
        type.set_length((param_length + mbminlen - 1) / mbminlen);
      }
    } else {
      type.set_length(param_length);
    }
  }
  return ret;
}

int ObExprToCharCommon::interval_to_char(ObObj& result, const ObObj* objs_array, int64_t param_num, ObExprCtx& expr_ctx)
{
  int ret = OB_SUCCESS;
  char* buf = NULL;
  int64_t pos = 0;
  int64_t buf_len = MAX_INTERVAL_BUFFER_SIZE;
  ObString format_str;
  ObString nls_param_str;
  bool null_result = false;

  if (OB_UNLIKELY(NULL == objs_array) || OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid argument.", K(ret), KP(objs_array), KP(expr_ctx.calc_buf_));
  } else if (OB_ISNULL(buf = static_cast<char*>(expr_ctx.calc_buf_->alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate buff", K(ret), K(buf_len));
  }

  if (OB_SUCC(ret) && param_num > 1) {
    if (!objs_array[1].is_null()) {
      ObSEArray<ObDFMElem, ObDFMUtil::COMMON_ELEMENT_NUMBER> dfm_elems;
      if (OB_FAIL(objs_array[1].get_varchar(format_str))) {
        LOG_WARN("fail to get varchar", K(ret));
      } else if (!format_str.empty() && OB_FAIL(ObDFMUtil::parse_datetime_format_string(format_str, dfm_elems))) {
        LOG_WARN("fail to parse oracle datetime format string", K(ret), K(format_str));
      }
    }
    null_result |= format_str.empty();
  }

  if (OB_SUCC(ret) && param_num > 2) {
    if (!objs_array[2].is_null()) {
      if (OB_FAIL(objs_array[2].get_varchar(nls_param_str))) {
        LOG_WARN("fail to get varchar", K(ret));
      } else if (OB_UNLIKELY(!ObExprOperator::is_valid_nls_param(nls_param_str))) {
        ret = OB_ERR_INVALID_NLS_PARAMETER_STRING;
        LOG_WARN("date format is invalid", K(ret), K(nls_param_str));
      }
    }
    null_result |= nls_param_str.empty();
  }

  if (OB_FAIL(ret)) {
  } else if (null_result) {
    result.set_null();
  } else {
    const ObObj& input_value = objs_array[0];
    if (OB_UNLIKELY(ObIntervalTC != input_value.get_type_class())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument. unexpected obj type", K(ret));
    } else if (OB_FAIL(input_value.is_interval_ym()
                           ? ObTimeConverter::interval_ym_to_str(
                                 input_value.get_interval_ym(), input_value.get_scale(), buf, buf_len, pos, false)
                           : ObTimeConverter::interval_ds_to_str(
                                 input_value.get_interval_ds(), input_value.get_scale(), buf, buf_len, pos, false))) {
      LOG_WARN("invalid interval to string", K(ret));
    } else if (OB_UNLIKELY(0 == pos)) {
      result.set_null();
    } else {
      result.set_varchar(buf, static_cast<int32_t>(pos));
    }
  }
  return ret;
}

int ObExprToCharCommon::datetime_to_char(ObObj& result, const ObObj* objs_array, int64_t param_num, ObExprCtx& expr_ctx)
{
  int ret = OB_SUCCESS;

  ObString format_str;
  ObString nls_param_str;
  const ObObj& input_value = objs_array[0];
  bool null_result = false;

  if (OB_UNLIKELY(NULL == objs_array) || OB_ISNULL(expr_ctx.calc_buf_) || OB_ISNULL(expr_ctx.my_session_) ||
      OB_UNLIKELY(param_num < 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument.", K(ret), K(objs_array), KP(expr_ctx.my_session_), K(param_num), K(expr_ctx.calc_buf_));
  }

  // param2: format
  if (OB_SUCC(ret)) {
    if (param_num > 1) {
      if (!objs_array[1].is_null()) {
        if (OB_FAIL(objs_array[1].get_varchar(format_str))) {
          LOG_WARN("fail to get varchar", K(ret));
        }
      }
      null_result |= format_str.empty();
    } else {
      if (OB_FAIL(expr_ctx.my_session_->get_local_nls_format(input_value.get_type(), format_str))) {
        LOG_WARN("failed to get default format", K(ret), K(input_value));
      }
    }
  }

  // param3: NLS settings
  if (OB_SUCC(ret) && param_num > 2) {
    if (!objs_array[2].is_null()) {
      if (OB_FAIL(objs_array[2].get_varchar(nls_param_str))) {
        LOG_WARN("fail to get varchar", K(ret));
      } else if (OB_UNLIKELY(!ObExprOperator::is_valid_nls_param(nls_param_str))) {
        ret = OB_ERR_INVALID_NLS_PARAMETER_STRING;
        LOG_WARN("date format is invalid", K(ret), K(nls_param_str));
      }
    }
    null_result |= nls_param_str.empty();
  }

  if (null_result) {
    result.set_null();
  } else {
    ObTime ob_time;
    ObScale scale = input_value.get_scale();

    // determine type, get calc ob_time from input_value
    if (OB_SUCC(ret)) {
      const ObTimeZoneInfo* tz_info = get_timezone_info(expr_ctx.my_session_);
      switch (input_value.get_type_class()) {
        case ObOTimestampTC: {  // oracle timestamp / timestamp with time zone / timestamp with local time zone
          ret = ObTimeConverter::otimestamp_to_ob_time(
              input_value.get_type(), input_value.get_otimestamp_value(), tz_info, ob_time, false);
          break;
        }
        case ObDateTimeTC: {  // oracle date type
          ret = ObTimeConverter::datetime_to_ob_time(input_value.get_datetime(), NULL, ob_time);
          break;
        }
        default: {
          ret = OB_INVALID_DATE_VALUE;
          LOG_WARN("input value is invalid", K(ret));
        }
      }
    }

    // print result
    if (OB_SUCC(ret)) {
      char* result_buf = NULL;
      int64_t pos = 0;
      int64_t result_buf_len = MAX_DATETIME_BUFFER_SIZE;
      if (OB_ISNULL(result_buf = static_cast<char*>(expr_ctx.calc_buf_->alloc(result_buf_len)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate buff", K(ret), K(result_buf_len));
      } else if (OB_FAIL(ObTimeConverter::ob_time_to_str_oracle_dfm(
                     ob_time, scale, format_str, result_buf, result_buf_len, pos))) {
        LOG_WARN("failed to convert to varchar2", K(ret));
      } else if (OB_UNLIKELY(0 == pos)) {
        result.set_null();
      } else {
        result.set_varchar(result_buf, static_cast<int32_t>(pos));
      }
    }
    LOG_DEBUG(
        "oracle to char function finished", K(ob_time), K(input_value), K(format_str), K(nls_param_str), K(result));
  }

  return ret;
}

int ObExprToCharCommon::number_to_char(ObObj& result, const ObObj* objs_array, int64_t param_num, ObExprCtx& expr_ctx)
{
  int ret = OB_SUCCESS;
  ObString number_raw;
  ObString fmt_raw;
  int scale = -1;
  bool has_fm = false;
  if (OB_ISNULL(objs_array) || OB_ISNULL(expr_ctx.calc_buf_) || param_num < 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument.", K(ret), K(objs_array), K(expr_ctx.calc_buf_), K(param_num));
  } else if (objs_array[0].is_hex_string()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("invalid argument", K(ret), K(objs_array[0]), K(objs_array[0].meta_));
  } else if (1 == param_num) {
    if (OB_FAIL(process_number_sci_value(expr_ctx, objs_array[0], scale, number_raw))) {
      LOG_WARN("fail to get number string", K(ret));
    } else {
      result.set_string(ObVarcharType, number_raw);
    }
  } else if (2 == param_num) {
    if (!objs_array[1].is_null()) {
      TYPE_CHECK(objs_array[1], ObVarcharType);
      fmt_raw = objs_array[1].get_string();
    }
    if (OB_FAIL(ret)) {
    } else if (objs_array[1].is_null()) {
      result.set_null();
    } else if (OB_FAIL(process_number_format(fmt_raw, scale, has_fm))) {
      LOG_WARN("failed to handle number to char format", K(ret));
    } else if (OB_FAIL(process_number_value(expr_ctx, objs_array[0], scale, number_raw))) {
      LOG_WARN("failed to get number string", K(ret));
    } else {
      char* result_buf = NULL;
      int64_t result_size = fmt_raw.length() + 1;
      if (OB_ISNULL(result_buf = static_cast<char*>(expr_ctx.calc_buf_->alloc(result_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory for number result failed", K(ret));
      } else if (OB_FAIL(format_number(number_raw.ptr(),
                     trim_number(number_raw),
                     fmt_raw.ptr(),
                     fmt_raw.length(),
                     result_buf,
                     result_size,
                     has_fm))) {
        LOG_WARN("failed to convert number to char with format", K(ret));
      } else {
        result.set_string(ObVarcharType, result_buf, static_cast<int32_t>(result_size));
        LOG_DEBUG("succ to convert number to char with format", K(objs_array[0]), K(number_raw), K(fmt_raw), K(result));
      }
    }
  } else {
    result.set_string(ObVarcharType, number_raw);
  }
  return ret;
}

// '-0' => '-', '0' => ''
int64_t ObExprToCharCommon::trim_number(const ObString& number)
{
  int64_t trim_length = number.length();
  if (number.length() == 1 && number[0] == '0') {
    trim_length = 0;
  } else if (number.length() == 2 && number[0] == '-' && number[1] == '0') {
    trim_length = 1;
  }
  return trim_length;
}

int ObExprToCharCommon::process_number_value(
    ObExprCtx& expr_ctx, const ObObj& obj, const int scale, ObString& number_raw)
{
  int ret = OB_SUCCESS;
  char* number_str = NULL;
  int64_t number_str_len = 0;
  number::ObNumber number_value;
  const int64_t alloc_size = MAX_TO_CHAR_BUFFER_SIZE;
  if (OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("params are invalid", K(ret), K(expr_ctx.calc_buf_));
  } else if (OB_ISNULL(number_str = static_cast<char*>(expr_ctx.calc_buf_->alloc(alloc_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory for number string failed", K(ret));
  } else {
    EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
    char buf_alloc[number::ObNumber::MAX_BYTE_LEN];
    ObDataBuffer tmp_allocator(buf_alloc, number::ObNumber::MAX_BYTE_LEN);
    if (obj.is_float() || obj.is_double()) {
      if (obj.is_double()) {
        number_str_len = ob_gcvt_strict(
            obj.get_double(), OB_GCVT_ARG_DOUBLE, alloc_size, number_str, NULL, lib::is_oracle_mode(), FALSE);
      } else {
        number_str_len = ob_gcvt_strict(
            obj.get_float(), OB_GCVT_ARG_FLOAT, alloc_size, number_str, NULL, lib::is_oracle_mode(), FALSE);
      }
      if (OB_FAIL(number_value.from(number_str, number_str_len, tmp_allocator))) {
        LOG_WARN("number from str failed", K(ret));
      }
      number_str_len = 0;
    } else {
      EXPR_GET_NUMBER_V2(obj, number_value);
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to cast obj as number", K(ret));
    } else if (OB_FAIL(number_value.format(number_str, alloc_size, number_str_len, static_cast<int16_t>(scale)))) {
      LOG_WARN("failed to format number to string", K(ret));
    } else if (number_str_len == 1 && number_str[0] == '0' && number_value.is_negative()) {
      // -0.4 round to 0 => -0
      number_str[0] = '-';
      number_str[1] = '0';
      number_str_len = 2;
    }
    if (OB_SUCC(ret)) {
      number_raw.assign_ptr(number_str, static_cast<int32_t>(number_str_len));
      LOG_DEBUG("process_number_value", K(obj), K(number_raw), K(number_str_len));
    }
  }
  return ret;
}

// convert number to string, if the string length is greater than 40 bytes, use scientific notation
int ObExprToCharCommon::process_number_sci_value(
    ObExprCtx& expr_ctx, const ObObj& obj, const int scale, ObString& number_sci)
{
  int ret = OB_SUCCESS;
  char* buf = NULL;
  int64_t number_str_len = 0;
  int64_t str_len = 0;
  number::ObNumber number_value;
  const int64_t alloc_size = ((obj.is_float() || obj.is_double()) ? MAX_NUMBER_BUFFER_SIZE : MAX_TO_CHAR_BUFFER_SIZE);
  if (OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("params are invalid", K(ret), K(expr_ctx.calc_buf_));
  } else if (OB_ISNULL(buf = static_cast<char*>(expr_ctx.calc_buf_->alloc(alloc_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory for number string failed", K(ret));
  } else {
    if (obj.is_double()) {
      str_len = ob_gcvt_opt(obj.get_double(), OB_GCVT_ARG_DOUBLE, alloc_size, buf, NULL, lib::is_oracle_mode());
    } else if (obj.is_float()) {
      str_len = ob_gcvt_opt(obj.get_float(), OB_GCVT_ARG_FLOAT, alloc_size, buf, NULL, lib::is_oracle_mode());
    } else {
      EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
      EXPR_GET_NUMBER_V2(obj, number_value);
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ObObjCaster::oracle_number_to_char(
                     number_value, obj.is_number(), static_cast<int16_t>(scale), alloc_size, buf, str_len))) {
        LOG_WARN("fail to convert number to string", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      number_sci.assign_ptr(buf, static_cast<int32_t>(str_len));
      LOG_DEBUG("process_number_sci_value", K(obj), K(number_sci), K(str_len));
    }
  }
  return ret;
}

int ObExprToCharCommon::process_number_format(ObString& fmt_raw, int& scale, bool& has_fm)
{
  int ret = OB_SUCCESS;
  bool has_period = false;
  scale = 0;
  if (fmt_raw.length() >= 2 && ('F' == fmt_raw[0] || 'f' == fmt_raw[0]) && ('M' == fmt_raw[1] || 'm' == fmt_raw[1])) {
    has_fm = true;
    fmt_raw.assign_ptr(fmt_raw.ptr() + 2, fmt_raw.length() - 2);
  }

  if (OB_SUCC(ret) && fmt_raw.length() >= 64) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid format string", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < fmt_raw.length(); ++i) {
    if (fmt_raw[i] == '.') {
      if (has_period) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("period apperas twice in the format string", K(ret), K(fmt_raw), K(i));
      } else {
        has_period = true;
      }
    } else if (fmt_raw[i] == '9' || fmt_raw[i] == '0') {
      if (has_period) {
        ++scale;
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("find invalid character in the format string", K(ret), K(fmt_raw), K(i));
    }
  }
  LOG_DEBUG("format info", K(has_fm), K(fmt_raw), K(scale));
  return ret;
}

int ObExprToCharCommon::format_number(const char* number_str, const int64_t number_len, const char* format_str,
    const int64_t format_len, char* result_buf, int64_t& result_size, bool has_fm)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(number_str) || number_len < 0 || (format_len > 0 && OB_ISNULL(format_str)) || format_len < 0 ||
      OB_ISNULL(result_buf) || result_size < format_len + 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("params have null",
        K(ret),
        KP(number_str),
        K(number_len),
        KP(format_str),
        K(format_len),
        KP(result_buf),
        K(result_size));
  } else {
    const int64_t num_start = (number_len > 0 && number_str[0] == '-') ? 1 : 0;
    int64_t num_dot_pos = 0;
    int64_t fmt_dot_pos = 0;
    for (; num_dot_pos < number_len && number_str[num_dot_pos] != '.'; ++num_dot_pos)
      ;
    for (; fmt_dot_pos < format_len && format_str[fmt_dot_pos] != '.'; ++fmt_dot_pos)
      ;
    if (num_dot_pos - num_start > fmt_dot_pos || format_len == 0) {
      // fill # into result_buf
      for (int64_t i = 0; i < result_size; ++i) {
        result_buf[i] = '#';
      }
    } else {
      int64_t real_digits_num = 0;
      result_buf[fmt_dot_pos + 1] = '.';
      int64_t fmt_pos = 0;
      int64_t num_pos = 0;
      for (fmt_pos = fmt_dot_pos + 1, num_pos = num_dot_pos + 1; fmt_pos < format_len; ++fmt_pos, ++num_pos) {
        result_buf[fmt_pos + 1] = (num_pos < number_len ? number_str[num_pos] : '0');
        ++real_digits_num;
      }
      // we must have num_pos <= fmt_pos
      for (fmt_pos = fmt_dot_pos - 1, num_pos = num_dot_pos - 1; num_pos >= num_start; --fmt_pos, --num_pos) {
        result_buf[fmt_pos + 1] = number_str[num_pos];
        ++real_digits_num;
      }
      if (0 == real_digits_num) {
        result_buf[fmt_pos + 1] = '0';
        --fmt_pos;
      }
      int64_t leading_zero_pos = 0;
      result_buf[0] = ' ';
      for (; leading_zero_pos <= fmt_pos && format_str[leading_zero_pos] != '0'; ++leading_zero_pos) {
        result_buf[leading_zero_pos + 1] = ' ';
      }
      int64_t result_number_start = leading_zero_pos;
      if (number_len > 0 && number_str[0] == '-') {
        result_buf[leading_zero_pos] = '-';
      } else {
        result_buf[leading_zero_pos] = ' ';
        result_number_start++;
      }
      // fill leading zero
      for (; leading_zero_pos <= fmt_pos; ++leading_zero_pos) {
        result_buf[leading_zero_pos + 1] = '0';
      }
      if (has_fm) {
        int64_t remove_ending_zero_count = 0;
        for (int64_t i = format_len - 1; i > fmt_dot_pos && format_str[i] != '0'; i--) {
          remove_ending_zero_count++;
        }
        int64_t num_decimal_length = MAX(number_len - num_dot_pos - 1, 0);
        int64_t fmt_decimal_length = MAX(format_len - fmt_dot_pos - 1, 0);
        remove_ending_zero_count = MIN(remove_ending_zero_count, fmt_decimal_length - num_decimal_length);
        int64_t result_length = format_len + 1 - remove_ending_zero_count - result_number_start;
        MEMMOVE(result_buf, &result_buf[result_number_start], result_length);
        result_size -= result_number_start + remove_ending_zero_count;

        LOG_DEBUG("format numer with fm",
            K(remove_ending_zero_count),
            K(format_len),
            K(result_number_start),
            K(ObString(format_len + 1, result_buf)));
      }
    }
  }
  return ret;
}

int ObExprToCharCommon::cg_expr(ObExprCGCtx&, const ObRawExpr&, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  CK(rt_expr.arg_cnt_ >= 1 && rt_expr.arg_cnt_ <= 3);
  rt_expr.eval_func_ = &ObExprToCharCommon::eval_oracle_to_char;
  return ret;
}

int ObExprToCharCommon::eval_oracle_to_char(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  const ObObjTypeClass input_tc = ob_obj_type_class(expr.args_[0]->datum_meta_.type_);
  ObDatum* input = NULL;
  ObDatum* fmt_datum = NULL;
  ObDatum* nlsparam_datum = NULL;
  ObString fmt;
  ObString nlsparam;
  if (OB_FAIL(expr.eval_param_value(ctx, input, fmt_datum, nlsparam_datum))) {
    LOG_WARN("evaluate parameters failed", K(ret));
  } else if (input->is_null() || (NULL != fmt_datum && fmt_datum->is_null()) ||
             (NULL != nlsparam_datum && nlsparam_datum->is_null())) {
    expr_datum.set_null();
  } else if (1 == expr.arg_cnt_ && (ObStringTC == input_tc || ObTextTC == input_tc)) {
    expr_datum.set_datum(*input);
  } else {
    ObIAllocator& alloc = ctx.get_reset_tmp_alloc();
    // convert the fmt && nlsparam to utf8 first.
    if (NULL != fmt_datum) {
      OZ(ObExprUtil::convert_string_collation(
          fmt_datum->get_string(), expr.args_[1]->datum_meta_.cs_type_, fmt, CS_TYPE_UTF8MB4_BIN, alloc));
    }
    if (OB_SUCC(ret) && NULL != nlsparam_datum) {
      OZ(ObExprUtil::convert_string_collation(
          nlsparam_datum->get_string(), expr.args_[2]->datum_meta_.cs_type_, nlsparam, CS_TYPE_UTF8MB4_BIN, alloc));
    }

    ObString res;
    if (OB_SUCC(ret)) {
      switch (input_tc) {
        case ObDateTimeTC:
        case ObOTimestampTC: {
          OZ(datetime_to_char(expr, ctx, alloc, *input, fmt, nlsparam, res));
          break;
        }
        case ObIntervalTC: {
          OZ(interval_to_char(expr, ctx, alloc, *input, fmt, nlsparam, res));
          break;
        }
          // ObStringTC, ObIntTC, ObUIntTC are convert to number by implicit cast.
        case ObFloatTC:
        case ObDoubleTC:
        case ObNumberTC: {
          if (OB_FAIL(is_valid_to_char_number(expr, fmt))) {
            LOG_WARN("fail to check num format", K(ret));
          } else if (OB_FAIL(number_to_char(expr, alloc, *input, fmt, res))) {
            LOG_WARN("number to char failed", K(ret));
          }
          break;
        }
        default: {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("unsupported to_char", K(ret), K(input_tc));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObExprUtil::set_expr_ascii_result(expr, ctx, expr_datum, res))) {
        LOG_WARN("set expr ascii result failed", K(ret));
      }
    }
  }
  return ret;
}

int ObExprToCharCommon::is_valid_to_char_number(const ObExpr& expr, const ObString& fmt)
{
  int ret = OB_SUCCESS;
  bool is_valid = false;
  if (OB_UNLIKELY(3 == expr.arg_cnt_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("does not support number to char with nlparam", K(ret));
  } else if (fmt.empty()) {
    is_valid = true;
  } else {
    is_valid = true;
    int64_t i = 0;
    if (fmt.length() >= 2 && ('F' == fmt[0] || 'f' == fmt[0]) && ('M' == fmt[1] || 'm' == fmt[1])) {
      i = 2;
    }
    for (; is_valid && i < fmt.length(); ++i) {
      is_valid = (fmt[i] == '0' || fmt[i] == '9' || fmt[i] == '.');
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(!is_valid)) {
      ret = OB_ERR_INVALID_NUMBER_FORMAT_MODEL;
      LOG_WARN("failed to check is valid to char number", K(ret));
    }
  }
  return ret;
}

int ObExprToCharCommon::datetime_to_char(const ObExpr& expr, ObEvalCtx& ctx, ObIAllocator& alloc, const ObDatum& input,
    const ObString& fmt, const ObString& nlsparam, ObString& res)
{
  int ret = OB_SUCCESS;
  ObString format_str;
  ObSQLSessionInfo* session = ctx.exec_ctx_.get_my_session();
  CK(NULL != session);

  // param2: format
  if (OB_SUCC(ret)) {
    if (!fmt.empty()) {
      format_str = fmt;
    } else {
      if (OB_FAIL(session->get_local_nls_format(expr.args_[0]->datum_meta_.type_, format_str))) {
        LOG_WARN("failed to get default format", K(ret));
      }
    }
  }

  // param3: NLS settings
  if (OB_SUCC(ret) && !nlsparam.empty()) {
    if (!is_valid_nls_param(nlsparam)) {
      ret = OB_ERR_INVALID_NLS_PARAMETER_STRING;
      LOG_WARN("date format is invalid", K(ret), K(nlsparam));
    }
  }

  ObTime ob_time;
  const ObDatumMeta& input_meta = expr.args_[0]->datum_meta_;
  ObScale scale = input_meta.scale_;

  // determine type, get calc ob_time from input_value
  if (OB_SUCC(ret)) {
    const ObTimeZoneInfo* tz_info = get_timezone_info(session);
    switch (ob_obj_type_class(input_meta.type_)) {
      case ObOTimestampTC: {
        // oracle timestamp / timestamp with time zone / timestamp with local time zone
        const ObOTimestampData& otdata =
            (ObTimestampTZType == input_meta.type_) ? input.get_otimestamp_tz() : input.get_otimestamp_tiny();
        ret = ObTimeConverter::otimestamp_to_ob_time(input_meta.type_, otdata, tz_info, ob_time, false);
        break;
      }
      case ObDateTimeTC: {  // oracle date type
        ret = ObTimeConverter::datetime_to_ob_time(input.get_datetime(), NULL, ob_time);
        break;
      }
      default: {
        ret = OB_INVALID_DATE_VALUE;
        LOG_WARN("input value is invalid", K(ret));
      }
    }
  }

  // print result
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(format_str.empty())) {
      res.reset();
    } else {
      char* result_buf = NULL;
      int64_t pos = 0;
      int64_t result_buf_len = MAX_DATETIME_BUFFER_SIZE;
      if (OB_ISNULL(result_buf = static_cast<char*>(alloc.alloc(result_buf_len)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate buff", K(ret), K(result_buf_len));
      } else if (OB_FAIL(ObTimeConverter::ob_time_to_str_oracle_dfm(
                     ob_time, scale, format_str, result_buf, result_buf_len, pos))) {
        LOG_WARN("failed to convert to varchar2", K(ret));
      } else {
        res = ObString(pos, result_buf);
      }
    }
  }

  LOG_DEBUG("oracle to char function finished", K(ret), K(ob_time), K(input), K(format_str), K(nlsparam), K(res));

  return ret;
}

int ObExprToCharCommon::interval_to_char(const ObExpr& expr, ObEvalCtx& ctx, ObIAllocator& alloc, const ObDatum& input,
    const ObString& fmt, const ObString& nlsparam, ObString& res)
{
  int ret = OB_SUCCESS;
  char* buf = NULL;
  int64_t pos = 0;
  int64_t buf_len = MAX_INTERVAL_BUFFER_SIZE;

  ObSQLSessionInfo* session = ctx.exec_ctx_.get_my_session();
  CK(NULL != session);

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(buf = static_cast<char*>(alloc.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate buff", K(ret), K(buf_len));
  }

  if (OB_SUCC(ret) && !fmt.empty()) {
    ObSEArray<ObDFMElem, ObDFMUtil::COMMON_ELEMENT_NUMBER> dfm_elems;
    if (OB_FAIL(ObDFMUtil::parse_datetime_format_string(fmt, dfm_elems))) {
      LOG_WARN("fail to parse oracle datetime format string", K(ret), K(fmt));
    }
  }

  if (OB_SUCC(ret) && !nlsparam.empty()) {
    if (!is_valid_nls_param(nlsparam)) {
      ret = OB_ERR_INVALID_NLS_PARAMETER_STRING;
      LOG_WARN("date format is invalid", K(ret), K(nlsparam));
    }
  }

  if (OB_SUCC(ret)) {
    const ObDatumMeta& input_meta = expr.args_[0]->datum_meta_;
    if (OB_UNLIKELY(ObIntervalTC != ob_obj_type_class(input_meta.type_))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument. unexpected obj type", K(ret));
    } else if (OB_FAIL(ObIntervalYMType == input_meta.type_
                           ? ObTimeConverter::interval_ym_to_str(
                                 input.get_interval_ym(), input_meta.scale_, buf, buf_len, pos, false)
                           : ObTimeConverter::interval_ds_to_str(
                                 input.get_interval_ds(), input_meta.scale_, buf, buf_len, pos, false))) {
      LOG_WARN("invalid interval to string", K(ret));
    } else {
      res = ObString(pos, buf);
    }
  }
  return ret;
}

int ObExprToCharCommon::number_to_char(
    const ObExpr& expr, common::ObIAllocator& alloc, const ObDatum& input, common::ObString& fmt, common::ObString& res)
{
  int ret = OB_SUCCESS;
  ObString number_raw;
  int scale = -1;
  bool has_fm = false;
  if (1 == expr.arg_cnt_) {
    if (OB_FAIL(process_number_sci_value(expr, alloc, input, scale, res))) {
      LOG_WARN("fail to get number string", K(ret));
    }
  } else if (expr.arg_cnt_ > 1) {
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(process_number_format(fmt, scale, has_fm))) {
      LOG_WARN("failed to handle number to char format", K(ret));
    } else if (OB_FAIL(process_number_value(expr, alloc, input, scale, number_raw))) {
      LOG_WARN("failed to get number string", K(ret));
    } else {
      char* result_buf = NULL;
      int64_t result_size = fmt.length() + 1;
      if (OB_ISNULL(result_buf = static_cast<char*>(alloc.alloc(result_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory for number result failed", K(ret));
      } else if (OB_FAIL(format_number(number_raw.ptr(),
                     trim_number(number_raw),
                     fmt.ptr(),
                     fmt.length(),
                     result_buf,
                     result_size,
                     has_fm))) {
        LOG_WARN("failed to convert number to char with format", K(ret));
      } else {
        res = ObString(result_size, result_buf);
        LOG_DEBUG("succ to convert number to char with format", K(input), K(number_raw), K(fmt), K(res));
      }
    }
  }
  return ret;
}

int ObExprToCharCommon::process_number_sci_value(
    const ObExpr& expr, common::ObIAllocator& alloc, const ObDatum& input, const int scale, common::ObString& res)
{
  int ret = OB_SUCCESS;
  char* buf = NULL;
  int64_t str_len = 0;
  const bool is_float = expr.args_[0]->obj_meta_.is_float();
  const bool is_double = expr.args_[0]->obj_meta_.is_double();
  const int64_t alloc_size = ((is_float || is_double) ? MAX_NUMBER_BUFFER_SIZE : MAX_TO_CHAR_BUFFER_SIZE);
  if (OB_ISNULL(buf = static_cast<char*>(alloc.alloc(alloc_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory for number string failed", K(ret));
  } else {
    if (is_double) {
      str_len = ob_gcvt_opt(input.get_double(), OB_GCVT_ARG_DOUBLE, alloc_size, buf, NULL, lib::is_oracle_mode());
    } else if (is_float) {
      str_len = ob_gcvt_opt(input.get_float(), OB_GCVT_ARG_FLOAT, alloc_size, buf, NULL, lib::is_oracle_mode());
    } else {
      number::ObNumber number_value(input.get_number());
      if (OB_FAIL(ObObjCaster::oracle_number_to_char(
              number_value, true, static_cast<int16_t>(scale), alloc_size, buf, str_len))) {
        LOG_WARN("fail to convert number to string", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      res.assign_ptr(buf, static_cast<int32_t>(str_len));
      LOG_DEBUG("process_number_sci_value", K(input), K(res));
    }
  }
  return ret;
}

int ObExprToCharCommon::process_number_value(
    const ObExpr& expr, common::ObIAllocator& alloc, const ObDatum& input, const int scale, common::ObString& res)
{
  int ret = OB_SUCCESS;
  char* number_str = NULL;
  int64_t number_str_len = 0;
  number::ObNumber number_value;
  const bool is_float = expr.args_[0]->obj_meta_.is_float();
  const bool is_double = expr.args_[0]->obj_meta_.is_double();
  const int64_t alloc_size = MAX_TO_CHAR_BUFFER_SIZE;
  if (OB_ISNULL(number_str = static_cast<char*>(alloc.alloc(alloc_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory for number string failed", K(ret));
  } else {
    char buf_alloc[number::ObNumber::MAX_BYTE_LEN];
    ObDataBuffer tmp_allocator(buf_alloc, number::ObNumber::MAX_BYTE_LEN);
    if (is_float || is_double) {
      if (is_double) {
        number_str_len = ob_gcvt_strict(
            input.get_double(), OB_GCVT_ARG_DOUBLE, alloc_size, number_str, NULL, lib::is_oracle_mode(), FALSE);

      } else {
        number_str_len = ob_gcvt_strict(
            input.get_float(), OB_GCVT_ARG_FLOAT, alloc_size, number_str, NULL, lib::is_oracle_mode(), FALSE);
      }
      if (OB_FAIL(number_value.from(number_str, number_str_len, tmp_allocator))) {
        LOG_WARN("number from str failed", K(ret));
      }
      number_str_len = 0;
    } else {
      number_value.assign(input.get_number_desc().desc_, const_cast<uint32_t*>(input.get_number_digits()));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(number_value.format(number_str, alloc_size, number_str_len, static_cast<int16_t>(scale)))) {
      LOG_WARN("failed to format number to string", K(ret));
    } else if (number_str_len == 1 && number_str[0] == '0' && number_value.is_negative()) {
      // -0.4 round to 0 => -0
      number_str[0] = '-';
      number_str[1] = '0';
      number_str_len = 2;
    }
    if (OB_SUCC(ret)) {
      res.assign_ptr(number_str, static_cast<int32_t>(number_str_len));
      LOG_DEBUG("process_number_value", K(input), K(res));
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
