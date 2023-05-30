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
#include "sql/engine/expr/ob_number_format_models.h"
#include "sql/engine/expr/ob_expr_extra_info_factory.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

const int64_t MAX_NUMBER_BUFFER_SIZE = 40;
const int64_t MAX_DATETIME_BUFFER_SIZE = 100;
const int64_t MAX_TO_CHAR_BUFFER_SIZE = 256;
const int64_t MAX_INTERVAL_BUFFER_SIZE = 32;
const int64_t MAX_CLOB_BUFFER_SIZE = 4000;

ObExprOracleToChar::ObExprOracleToChar(ObIAllocator &alloc)
    : ObExprToCharCommon(alloc, T_FUN_SYS_TO_CHAR, N_TO_CHAR, MORE_THAN_ZERO, VALID_FOR_GENERATED_COL)
{
}

ObExprOracleToChar::~ObExprOracleToChar()
{
}


int ObExprOracleToChar::calc_result_typeN(ObExprResType &type,
                                          ObExprResType *type_array,
                                          int64_t params_count,
                                          ObExprTypeCtx &type_ctx) const
{
  //https://docs.oracle.com/cd/B19306_01/server.102/b14200/functions180.htm
  int ret = OB_SUCCESS;
  CK(NULL != type_ctx.get_session());
  ObSessionNLSParams nls_param = type_ctx.get_session()->get_session_nls_params();
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(NULL == type_array || params_count < 1 || params_count > 3)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument.", K(ret), K(type_array), K(params_count));
  } else if (OB_ISNULL(type_ctx.get_session())) {
    LOG_WARN("session is null", K(ret));
  } else {
    const ObObjTypeClass type_class = type_array[0].get_type_class();
    switch (type_class) {
      case ObNullTC: {
        type.set_null();
        break;
      }
      case ObFloatTC:
      case ObDoubleTC:
      case ObStringTC:
      case ObTextTC:
      case ObIntTC:
      case ObUIntTC:
      case ObNumberTC: {
        if (1 == params_count) {
          if (type_array[0].is_varchar_or_char()) {
            type.set_type_simple(type_array[0].get_type());
            type.set_length_semantics(type_array[0].get_length_semantics());
          } else {
            type.set_varchar();
            type.set_length_semantics(nls_param.nls_length_semantics_);
          }
          type.set_collation_level(CS_LEVEL_IMPLICIT);
          type.set_collation_type(nls_param.nls_collation_);
          if (ob_is_numeric_tc(type_class)) {
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
        } else {
          const ObObj &fmt_obj = type_array[1].get_param();
          ObNFMToChar nfm;
          int32_t length = 0;
          type.set_varchar();
          type.set_collation_level(CS_LEVEL_IMPLICIT);
          type.set_collation_type(nls_param.nls_collation_);
          type.set_length_semantics(nls_param.nls_length_semantics_);
          if (fmt_obj.is_null()) {
            length = OB_MAX_ORACLE_VARCHAR_LENGTH;
          } else if (OB_FAIL(nfm.calc_result_length(fmt_obj, length))) {
            // invalid format won't cause failure because the expr may not be executed
            LOG_WARN("calc reuslt length failed", K(fmt_obj), K(ret));
            ret = OB_SUCCESS;
            if (type_array[0].is_character_type() && 0 == type_array[0].get_length()) {
              length = 0;
            } else {
              length = OB_MAX_ORACLE_VARCHAR_LENGTH;
            }
          }
          OX (type.set_length(length));
        }
        break;
      }
      case ObDateTimeTC:
      case ObOTimestampTC:
      case ObIntervalTC: {
        type.set_varchar();
        type.set_length_semantics(nls_param.nls_length_semantics_);
        type.set_collation_level(CS_LEVEL_IMPLICIT);
        type.set_collation_type(nls_param.nls_collation_);
        type.set_length(MAX_TO_CHAR_BUFFER_SIZE);
        break;
      }
      default: {
        ret = OB_ERR_INVALID_TYPE_FOR_OP;
        LOG_WARN("input type not supported", K(ret), K(type_class));
        break;
      }
    }
  }
  if (type_array[0].is_string_type()) {
    type_array[0].set_calc_meta(type);
    type_array[0].set_calc_length(type.get_length());
    type_array[0].set_calc_length_semantics(type.get_length_semantics());
  }
  if (OB_SUCC(ret) && params_count >= 2) {
    const ObObjTypeClass tc = type_array[0].get_type_class();
    if (ObStringTC == tc || ObTextTC == tc || ObIntTC == tc || ObUIntTC == tc) {
      type_array[0].set_calc_type(ObNumberType);
      type_array[0].set_calc_scale(NUMBER_SCALE_UNKNOWN_YET);
    }
    if (type_array[0].is_oracle_temporal_type()) {
      type_array[1].set_calc_type(ObVarcharType);
      type_array[1].set_calc_collation_type(nls_param.nls_collation_);
    } else if (!type_array[1].is_varchar_or_char()) {
      type_array[1].set_calc_type_default_varchar();
    }
  }
  if (OB_SUCC(ret) && params_count == 3) {
    type_array[2].set_calc_type_default_varchar();
  }
  return ret;
}

ObExprOracleToNChar::ObExprOracleToNChar(ObIAllocator &alloc)
    : ObExprToCharCommon(alloc, T_FUN_SYS_TO_NCHAR, N_TO_NCHAR, MORE_THAN_ZERO, VALID_FOR_GENERATED_COL)
{
}

ObExprOracleToNChar::~ObExprOracleToNChar()
{
}


int ObExprOracleToNChar::calc_result_typeN(ObExprResType &type,
                                          ObExprResType *type_array,
                                          int64_t params_count,
                                          ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  ObSessionNLSParams nls_param = type_ctx.get_session()->get_session_nls_params();
  if (OB_UNLIKELY(NULL == type_array || params_count < 1 || params_count > 3)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument.", K(ret), K(type_array), K(params_count));
  } else if (OB_ISNULL(type_ctx.get_session())) {
    LOG_WARN("session is null", K(ret));
  } else {
    const ObObjTypeClass type_class = type_array[0].get_type_class();
    switch (type_class) {
      case ObNullTC: {
        type.set_null();
        break;
      }
      case ObFloatTC:
      case ObDoubleTC:
      case ObStringTC:
      case ObTextTC:
      case ObIntTC:
      case ObUIntTC:
      case ObNumberTC: {
        if (1 == params_count) {
          const ObObj fmt_obj = type_array[1].get_param();
          if (type_array[0].is_nstring()) {
            type.set_type_simple(type_array[0].get_type());
          } else {
            type.set_nvarchar2();
          }
          type.set_length_semantics(LS_CHAR);
          type.set_collation_level(CS_LEVEL_IMPLICIT);
          type.set_collation_type(nls_param.nls_nation_collation_);
          if (ob_is_numeric_tc(type_class)) {
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
        } else {
          const ObObj &fmt_obj = type_array[1].get_param();
          ObNFMToChar nfm;
          int32_t length = 0;
          int64_t mbminlen = 1;
          type.set_nvarchar2();
          type.set_collation_level(CS_LEVEL_IMPLICIT);
          type.set_collation_type(nls_param.nls_collation_);
          type.set_length_semantics(LS_CHAR);
          if (OB_FAIL(ObCharset::get_mbminlen_by_coll(type.get_collation_type(), mbminlen))) {
            LOG_WARN("get mbminlen by coll failed", K(ret), K(type));
          } else if (fmt_obj.is_null()) {
            length = OB_MAX_ORACLE_VARCHAR_LENGTH / mbminlen;
          } else if (OB_FAIL(nfm.calc_result_length(fmt_obj, length))) {
            // invalid format won't cause failure because the expr may not be executed
            LOG_WARN("calc reuslt length failed", K(fmt_obj), K(ret));
            ret = OB_SUCCESS;
            if (type_array[0].is_character_type() && 0 == type_array[0].get_length()) {
              length = 0;
            } else {
              length = OB_MAX_ORACLE_VARCHAR_LENGTH / mbminlen;
            }
          }
          OX (type.set_length(length));
        }
        break;
      }
      case ObDateTimeTC:
      case ObOTimestampTC:
      case ObIntervalTC: {
        type.set_nvarchar2();
        type.set_length_semantics(LS_CHAR);
        type.set_collation_level(CS_LEVEL_IMPLICIT);
        type.set_collation_type(nls_param.nls_nation_collation_);
        type.set_length(MAX_TO_CHAR_BUFFER_SIZE);
        break;
      }
      default: {
        ret = OB_ERR_INVALID_TYPE_FOR_OP;
        LOG_WARN("input type not supported", K(ret), K(type_class));
        break;
      }
    }
  }
  if (type_array[0].is_string_type()) {
    type_array[0].set_calc_meta(type);
  }
  if (OB_SUCC(ret) && params_count >= 2) {
    // in static typing engine, we use the implicit cast to cast string to number.
    const ObObjTypeClass tc = type_array[0].get_type_class();
    if (ObStringTC == tc || ObTextTC == tc || ObIntTC == tc || ObUIntTC == tc) {
      type_array[0].set_calc_type(ObNumberType);
      type_array[0].set_calc_scale(NUMBER_SCALE_UNKNOWN_YET);
    }
  }
  if (OB_SUCC(ret) && params_count >= 2) {
    if (type_array[0].is_oracle_temporal_type()) {
      type_array[1].set_calc_type(ObVarcharType);
      type_array[1].set_calc_collation_type(nls_param.nls_collation_);
    } else {
      type_array[1].set_calc_type_default_varchar();
    }
  }
  if (OB_SUCC(ret) && params_count == 3) {
    type_array[2].set_calc_type_default_varchar();
  }
  return ret;
}


/**
 * @brief ObExprOracleToChar::is_valid_to_char_number
 *  检查是不是合法的 to_char (number)
 */
int ObExprToCharCommon::is_valid_to_char_number(const ObObj *objs_array,
                                                const int64_t param_num)
{
  int ret = OB_SUCCESS;
  bool is_valid = false;
  if (OB_ISNULL(objs_array) || OB_UNLIKELY(param_num < 1 || param_num > 3)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument.", K(ret), K(objs_array), K(param_num));
  } else if (objs_array[0].is_numeric_type()) {
    is_valid = true;
  } else if (ObStringTC == objs_array[0].get_type_class()
             && param_num >= 2) {
    is_valid = (ObNullTC == objs_array[1].get_type_class()
        || ObStringTC == objs_array[1].get_type_class());
    if (is_valid && param_num == 3) {
      is_valid = (ObNullTC == objs_array[2].get_type_class()
          || ObStringTC == objs_array[2].get_type_class());
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

// 用于to_char, to_nchar, translate using表达式 参数为string/raw tc时推导结果的长度
// 调用前需要设置要结果的type, collation_type和length_semantics
int ObExprToCharCommon::calc_result_length_for_string_param(ObExprResType &type,
                                                            const ObExprResType &param)
{
  int ret = OB_SUCCESS;
  int64_t mbmaxlen = 1;
  int64_t mbminlen = 1;
  int64_t param_length = (param.is_raw() ? 2 : 1) * param.get_accuracy().get_length();
  if (OB_UNLIKELY(CS_TYPE_INVALID == type.get_collation_type()
      || CS_TYPE_INVALID == param.get_collation_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("result collation type should be valid", K(ret));
  } else if (param.is_nstring() || LS_CHAR == param.get_length_semantics()) {
    if (type.is_nstring() || LS_CHAR == type.get_length_semantics()) {
      // 参数是LS_CHAR， 结果是LS_CHAR时直接赋值length
      type.set_length(param_length);
    } else if (OB_FAIL(ObCharset::get_mbmaxlen_by_coll(type.get_collation_type(), mbmaxlen))) {
      LOG_WARN("get mbmaxlen by coll failed", K(ret), K(type));
    } else {
      // 参数是LS_CHAR， 结果是LS_BYTE时，设置length为param_length乘以结果字符集的mbmaxlen
      type.set_length(param_length * mbmaxlen);
    }
  } else {
    if (type.is_nstring() || LS_CHAR == type.get_length_semantics()) {
      if (OB_FAIL(ObCharset::get_mbminlen_by_coll(param.get_collation_type(), mbminlen))) {
        LOG_WARN("get mbminlen by coll failed", K(ret), K(type));
      } else {
        // 参数是LS_BYTE， 结果是LS_CHAR时，设置length为param_length除以参数字符集的mbminlen
        type.set_length((param_length + mbminlen - 1) / mbminlen);
      }
    } else {
      // 参数是LS_BYTE， 结果是LS_BYTE时直接赋值length
      type.set_length(param_length);
    }
  }
  return ret;
}

int ObExprToCharCommon::interval_to_char(ObObj &result,
                                         const ObObj *objs_array,
                                         int64_t param_num,
                                         ObExprCtx &expr_ctx)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  int64_t pos = 0;
  int64_t buf_len = MAX_INTERVAL_BUFFER_SIZE;
  ObString format_str;
  ObString nls_param_str;
  bool null_result = false;

  if (OB_UNLIKELY(NULL == objs_array)|| OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid argument.", K(ret), KP(objs_array), KP(expr_ctx.calc_buf_));
  } else if (OB_ISNULL(buf = static_cast<char *>(expr_ctx.calc_buf_->alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate buff", K(ret), K(buf_len));
  }

  if (OB_SUCC(ret) && param_num > 1) {
    if (!objs_array[1].is_null()) {
      ObSEArray<ObDFMElem, ObDFMUtil::COMMON_ELEMENT_NUMBER> dfm_elems;
      if (OB_FAIL(objs_array[1].get_varchar(format_str))) {
        LOG_WARN("fail to get varchar", K(ret));
      } else if (!format_str.empty()
                 && OB_FAIL(ObDFMUtil::parse_datetime_format_string(format_str, dfm_elems))) {
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
    const ObObj &input_value = objs_array[0];
    if (OB_UNLIKELY(ObIntervalTC != input_value.get_type_class())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument. unexpected obj type", K(ret));
    } else if (OB_FAIL(input_value.is_interval_ym() ?
                       ObTimeConverter::interval_ym_to_str(input_value.get_interval_ym(),
                                            input_value.get_scale(), buf, buf_len, pos, false)
                     : ObTimeConverter::interval_ds_to_str(input_value.get_interval_ds(),
                                            input_value.get_scale(), buf, buf_len, pos, false))) {
      LOG_WARN("invalid interval to string", K(ret));
    } else if (OB_UNLIKELY(0 == pos)) {
      result.set_null();
    } else {
      result.set_varchar(buf, static_cast<int32_t>(pos));
    }
  }
  return ret;
}

int ObExprToCharCommon::datetime_to_char(ObObj &result,
                                         const ObObj *objs_array,
                                         int64_t param_num,
                                         ObExprCtx &expr_ctx)
{
  int ret = OB_SUCCESS;

  ObString format_str;
  ObString nls_param_str;
  const ObObj &input_value = objs_array[0];
  bool null_result = false;

  if (OB_UNLIKELY(NULL == objs_array)|| OB_ISNULL(expr_ctx.calc_buf_)
      || OB_ISNULL(expr_ctx.my_session_) || OB_UNLIKELY(param_num < 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument.", K(ret), K(objs_array), KP(expr_ctx.my_session_),
        K(param_num), K(expr_ctx.calc_buf_));
  }

  //param2: format
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

  //param3: NLS settings
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

    //determine type, get calc ob_time from input_value
    if (OB_SUCC(ret)) {
      const ObTimeZoneInfo *tz_info = get_timezone_info(expr_ctx.my_session_);
      switch (input_value.get_type_class()) {
        case ObOTimestampTC: {  //oracle timestamp / timestamp with time zone / timestamp with local time zone
          ret = ObTimeConverter::otimestamp_to_ob_time(input_value.get_type(), input_value.get_otimestamp_value(), tz_info, ob_time, false);
          break;
        }
        case ObDateTimeTC: {   //oracle date type
          ret = ObTimeConverter::datetime_to_ob_time(input_value.get_datetime(), NULL, ob_time);
          break;
        }
        default: {
          ret = OB_INVALID_DATE_VALUE;
          LOG_WARN("input value is invalid", K(ret));
        }
      }
    }

    //print result
    if (OB_SUCC(ret)) {
      char *result_buf = NULL;
      int64_t pos = 0;
      int64_t result_buf_len = MAX_DATETIME_BUFFER_SIZE;
      if (OB_ISNULL(result_buf = static_cast<char *>(expr_ctx.calc_buf_->alloc(result_buf_len)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate buff", K(ret), K(result_buf_len));
      } else if (OB_FAIL(ObTimeConverter::ob_time_to_str_oracle_dfm(ob_time, scale, format_str, result_buf, result_buf_len, pos))) {
        LOG_WARN("failed to convert to varchar2", K(ret));
      } else if (OB_UNLIKELY(0 == pos)) {
        result.set_null();
      } else {
        result.set_varchar(result_buf, static_cast<int32_t>(pos));
      }
    }
    LOG_DEBUG("oracle to char function finished", K(ob_time), K(input_value), K(format_str), K(nls_param_str), K(result));
  }

  return ret;
}

/**
 * @brief ObExprOracleToChar::number_to_char
 * 1. 解析 format 要求
 * 2. 将 number 转成 string，保留指定的精度，去除前导 0
 * 3. 对 number 进行格式化
 * 4. 生成输出结果
 * @return
 */
int ObExprToCharCommon::number_to_char(ObObj &result,
                                       const ObObj *objs_array,
                                       int64_t param_num,
                                       ObExprCtx &expr_ctx)
{
  int ret = OB_SUCCESS;
  ObString format_str;
  ObString res_str;
  ObString nls_param_str;
  int scale = -1;
  bool null_result = false;
  if (OB_ISNULL(objs_array) || OB_ISNULL(expr_ctx.calc_buf_) || param_num < 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument.", K(ret), K(objs_array), K(expr_ctx.calc_buf_), K(param_num));
  } else if (objs_array[0].is_hex_string()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret), K(objs_array[0]), K(objs_array[0].meta_));
  } else if (1 == param_num) {
    if (OB_FAIL(process_number_sci_value(expr_ctx, objs_array[0], scale, res_str))) {
      LOG_WARN("fail to get number string", K(ret));
    } else {
      result.set_string(ObVarcharType, res_str);
    }
  } else {
    if (param_num > 1) {
      if (!objs_array[1].is_null()) {
        if (OB_FAIL(objs_array[1].get_varchar(format_str))) {
          LOG_WARN("fail to get varchar", K(ret));
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
    if (null_result) {
      result.set_null();
    } else {
      if (OB_SUCC(ret)) {
        ObNFMToChar nfm;
        if (OB_FAIL(nfm.convert_num_to_fmt_str(objs_array[0], format_str.ptr(),
                    format_str.length(), expr_ctx, res_str))) {
          LOG_WARN("fail to convert num to fmt str", K(ret), K(format_str));
        } else {
          result.set_string(ObVarcharType, res_str);
        }
      }
    }
  }
  return ret;
}

// '-0' => '-', '0' => ''
int64_t ObExprToCharCommon::trim_number(const ObString &number)
{
  int64_t trim_length = number.length();
  if (number.length() == 1 && number[0] == '0') {
    trim_length = 0;
  } else if (number.length() == 2 && number[0] == '-' && number[1] == '0') {
    trim_length = 1;
  }
  return trim_length;
}

/**
 * @brief ObExprOracleToChar::process_number_value
 * 读取 number 值，保留指定的精度，去除前导 0
 * 注意点：负数四舍五如为0后保留符号 -0.4 => -0
 */
int ObExprToCharCommon::process_number_value(ObExprCtx &expr_ctx,
                                             const ObObj &obj, const int scale,
                                             ObString &number_raw)
{
  int ret = OB_SUCCESS;
  char *number_str = NULL;
  int64_t number_str_len = 0;
  number::ObNumber number_value;
  const int64_t alloc_size = MAX_TO_CHAR_BUFFER_SIZE;
  if (OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("params are invalid", K(ret), K(expr_ctx.calc_buf_));
  } else if (OB_ISNULL(number_str = static_cast<char *>(
                         expr_ctx.calc_buf_->alloc(alloc_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory for number string failed", K(ret));
  } else {
    EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
    char buf_alloc[number::ObNumber::MAX_BYTE_LEN];
    ObDataBuffer tmp_allocator(buf_alloc, number::ObNumber::MAX_BYTE_LEN);
    if (obj.is_float() || obj.is_double()) {
      if (obj.is_double()) {
        number_str_len = ob_gcvt_strict(obj.get_double(), OB_GCVT_ARG_DOUBLE, alloc_size,
                                        number_str, NULL, lib::is_oracle_mode(), TRUE, FALSE);
      } else {
        number_str_len = ob_gcvt_strict(obj.get_float(), OB_GCVT_ARG_FLOAT, alloc_size,
                                        number_str, NULL, lib::is_oracle_mode(), TRUE, FALSE);
      }
      if (OB_FAIL(number_value.from(number_str, number_str_len, tmp_allocator))) {
        LOG_WARN("number from str failed", K(ret));
      }
      number_str_len = 0;
    } else {
      EXPR_GET_NUMBER_V2(obj, number_value);
    }
    // 将 number 四舍五入，保留一定的精度，转成 string
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to cast obj as number", K(ret));
    } else if (OB_FAIL(number_value.format(
                          number_str, alloc_size, number_str_len,
                          static_cast<int16_t>(scale)))) {
      LOG_WARN("failed to format number to string", K(ret));
    } else if (number_str_len == 1
                && number_str[0] == '0'
                && number_value.is_negative()) {
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

/**
 * @brief ObExprOracleToChar::process_number_sci_value
 * 读取number值,转为字符串.
 * 如果字符串大于40字节,将会被转为科学计数法.
 */
int ObExprToCharCommon::process_number_sci_value(ObExprCtx &expr_ctx,
                                                 const ObObj &obj, const int scale,
                                                 ObString &number_sci)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  int64_t str_len = 0;
  number::ObNumber number_value;
  const int64_t alloc_size = ((obj.is_float() || obj.is_double()) ? MAX_NUMBER_BUFFER_SIZE : MAX_TO_CHAR_BUFFER_SIZE);
  if (OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("params are invalid", K(ret), K(expr_ctx.calc_buf_));
  } else if (OB_ISNULL(buf = static_cast<char *>(
                         expr_ctx.calc_buf_->alloc(alloc_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory for number string failed", K(ret));
  } else {
    if (obj.is_double()) {
      str_len = ob_gcvt_opt(obj.get_double(), OB_GCVT_ARG_DOUBLE, static_cast<int32_t>(alloc_size),
                            buf, NULL, lib::is_oracle_mode(), TRUE);
    } else if (obj.is_float()) {
      str_len = ob_gcvt_opt(obj.get_float(), OB_GCVT_ARG_FLOAT, static_cast<int32_t>(alloc_size),
                            buf, NULL, lib::is_oracle_mode(), TRUE);
    } else {
      EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
      EXPR_GET_NUMBER_V2(obj, number_value);
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ObObjCaster::oracle_number_to_char(number_value, obj.is_number(),
          static_cast<int16_t>(scale), alloc_size, buf, str_len))) {
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
/**
 * @brief ObExprOracleToChar::process_number_format_string
 *  解析 format string，获得格式化后的精度
 */

int ObExprToCharCommon::process_number_format(ObString &fmt_raw,
                                              int &scale,
                                              bool& has_fm)
{
  int ret = OB_SUCCESS;
  bool has_period = false;
  scale = 0;
  if (fmt_raw.length() >= 2 &&
              ('F' == fmt_raw[0] || 'f' == fmt_raw[0]) &&
              ('M' == fmt_raw[1] || 'm' == fmt_raw[1])) {
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

/**
 * @brief ObExprOracleToChar::format_number
 * 将 number_str 按照 format_str 格式化后输出到 result_buf
 */
int ObExprToCharCommon::format_number(const char *number_str,
                                                const int64_t number_len,
                                                const char *format_str,
                                                const int64_t format_len,
                                                char *result_buf,
                                                int64_t &result_size,
                                                bool has_fm)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(number_str) || number_len < 0
      || (format_len > 0 && OB_ISNULL(format_str)) || format_len < 0
      || OB_ISNULL(result_buf) || result_size < format_len + 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("params have null", K(ret), KP(number_str), K(number_len),
             KP(format_str), K(format_len), KP(result_buf), K(result_size));
  } else {
    const int64_t num_start = (number_len > 0 && number_str[0] == '-') ? 1 : 0;
    int64_t num_dot_pos = 0;
    int64_t fmt_dot_pos = 0;
    for (; num_dot_pos < number_len && number_str[num_dot_pos] != '.'; ++ num_dot_pos);
    for (; fmt_dot_pos < format_len && format_str[fmt_dot_pos] != '.'; ++ fmt_dot_pos);
    // 1. 检查整数部分长度是不是超过了 fmt 允许的长度
    if (num_dot_pos - num_start > fmt_dot_pos || format_len == 0) {
      // fill # into result_buf
      for (int64_t i = 0; i < result_size; ++i) {
        result_buf[i] = '#';
      }
    } else {
      int64_t real_digits_num = 0;
      // 2. 填充小数部分
      result_buf[fmt_dot_pos + 1] = '.';
      int64_t fmt_pos = 0;
      int64_t num_pos = 0;
      for (fmt_pos = fmt_dot_pos + 1, num_pos = num_dot_pos + 1;
           fmt_pos < format_len; ++fmt_pos, ++num_pos) {
        result_buf[fmt_pos + 1] = (num_pos < number_len ? number_str[num_pos] : '0');
        ++real_digits_num;
      }
      // 3. 填充整数部分
      // we must have num_pos <= fmt_pos
      for (fmt_pos = fmt_dot_pos - 1, num_pos = num_dot_pos - 1;
           num_pos >= num_start; --fmt_pos, --num_pos) {
        result_buf[fmt_pos + 1] = number_str[num_pos];
        ++real_digits_num;
      }
      if (0 == real_digits_num) {
        // num = 0 时，如果没有添加小数部分，那么 result 中没有 digit，此时补 0
        result_buf[fmt_pos + 1] = '0';
        -- fmt_pos;
      }
      // 4. 填充left padding 部分的空格
      int64_t leading_zero_pos = 0;
      result_buf[0] = ' ';
      for (;leading_zero_pos <= fmt_pos && format_str[leading_zero_pos] != '0'; ++leading_zero_pos) {
        result_buf[leading_zero_pos + 1] = ' ';
      }
      int64_t result_number_start = leading_zero_pos;
      // 5. 填充减号或者空格
      if (number_len > 0 && number_str[0] == '-') {
        result_buf[leading_zero_pos] = '-';
      } else {
        result_buf[leading_zero_pos] = ' ';
        result_number_start++;
      }
      // 6. 填充 leading zeros
      for (;leading_zero_pos <= fmt_pos; ++leading_zero_pos) {
        result_buf[leading_zero_pos + 1] = '0';
      }
      if (has_fm) {
        // to_char(123.45,'999.999') = to_char(123.45,'999.000') = 123.450
        // format有fm前缀时，format中小数点之后，只有0会向结果补后缀0，9应该被忽略。
        // 因此要找到format中最右边的0，如果这个0右边还有n个字符，那么要从result小数部分的末尾移除n个0
        int64_t remove_ending_zero_count = 0;
        for(int64_t i = format_len - 1; i > fmt_dot_pos && format_str[i] != '0'; i--) {
          remove_ending_zero_count++;
        }
        int64_t num_decimal_length = MAX(number_len - num_dot_pos - 1, 0);
        int64_t fmt_decimal_length = MAX(format_len - fmt_dot_pos - 1, 0);
        // 去掉ending 0以后的小数部分不能比number的小数部分短
        remove_ending_zero_count = MIN(remove_ending_zero_count,
                                      fmt_decimal_length - num_decimal_length);
        // 将result_buf[result_number_start] ~ result_buf[format_len - remove_ending_zero_count]
        // 移动到result_buf的开头
        int64_t result_length = format_len + 1 - remove_ending_zero_count - result_number_start;
        MEMMOVE(result_buf, &result_buf[result_number_start], result_length);
        result_size -= result_number_start + remove_ending_zero_count;

        LOG_DEBUG("format numer with fm", K(remove_ending_zero_count), K(format_len),
            K(result_number_start), K(ObString(format_len + 1, result_buf)));
      }
    }
  }
  return ret;
}

int ObExprToCharCommon::cg_expr(ObExprCGCtx &expr_cg_ctx,
                                const ObRawExpr &raw_expr,
                                ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  CK(rt_expr.arg_cnt_ >= 1 && rt_expr.arg_cnt_ <= 3);

  rt_expr.eval_func_ = &ObExprToCharCommon::eval_oracle_to_char;

  // for static engine batch
  if ((1 == rt_expr.arg_cnt_ && rt_expr.args_[0]->is_batch_result())
      || (2 == rt_expr.arg_cnt_
          && rt_expr.args_[0]->is_batch_result()
          && !rt_expr.args_[1]->is_batch_result())
      || (3 == rt_expr.arg_cnt_
          && rt_expr.args_[0]->is_batch_result()
          && !rt_expr.args_[1]->is_batch_result()
          && !rt_expr.args_[2]->is_batch_result())) {
    rt_expr.eval_batch_func_ = eval_oracle_to_char_batch;
  }

  return ret;
}

int ObExprToCharCommon::eval_oracle_to_char(const ObExpr &expr,
                                            ObEvalCtx &ctx,
                                            ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  const ObObjTypeClass input_tc = ob_obj_type_class(expr.args_[0]->datum_meta_.type_);
  ObDatum *input = NULL;
  ObDatum *fmt_datum = NULL;
  ObDatum *nlsparam_datum = NULL;
  ObString fmt;
  ObString nlsparam;
  if (OB_FAIL(expr.eval_param_value(ctx, input, fmt_datum, nlsparam_datum))) {
    LOG_WARN("evaluate parameters failed", K(ret));
  } else if (input->is_null()
             || (NULL != fmt_datum && fmt_datum->is_null())
             || (NULL != nlsparam_datum && nlsparam_datum->is_null())) {
    expr_datum.set_null();
  } else if (1 == expr.arg_cnt_ && (ObStringTC == input_tc || ObTextTC == input_tc)) {
    expr_datum.set_datum(*input);
  } else {
    ObEvalCtx::TempAllocGuard alloc_guard(ctx);
    ObIAllocator &alloc = alloc_guard.get_allocator();
    // convert the fmt && nlsparam to utf8 first.
    if (NULL != fmt_datum) {
      fmt = fmt_datum->get_string();
    }
    if (OB_SUCC(ret) && NULL != nlsparam_datum) {
      OZ(ObExprUtil::convert_string_collation(nlsparam_datum->get_string(),
                                              expr.args_[2]->datum_meta_.cs_type_,
                                              nlsparam,
                                              CS_TYPE_UTF8MB4_BIN,
                                              alloc));
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
        case ObIntTC: // to support PLS_INTERGER type
        case ObFloatTC:
        case ObDoubleTC:
        case ObNumberTC: {
          if (NULL != nlsparam_datum) {
            nlsparam = nlsparam_datum->get_string();
          }
          if (OB_FAIL(is_valid_to_char_number(expr))) {
            LOG_WARN("fail to check num format", K(ret));
          } else if (OB_FAIL(number_to_char(expr, ctx, alloc, *input, fmt, nlsparam, res))) {
            LOG_WARN("number to char failed", K(ret));
          }
          break;
        }
        default: {
          ret = OB_ERR_INVALID_TYPE_FOR_OP;
          LOG_WARN("unsupported to_char", K(ret), K(input_tc));
        }
      }
    }

    if (OB_SUCC(ret)) {
      const bool is_ascii = (ObDateTimeTC == input_tc || ObOTimestampTC == input_tc) ? false : true;
      const ObCollationType src_coll_type = (ObDateTimeTC == input_tc || ObOTimestampTC == input_tc)
                                            ? ctx.exec_ctx_.get_my_session()->get_nls_collation()
                                            : CS_TYPE_UTF8MB4_BIN;
      if (OB_FAIL(ObExprUtil::set_expr_ascii_result(expr, ctx, expr_datum, res, is_ascii,
                                                    src_coll_type))) {
        LOG_WARN("set expr ascii result failed", K(ret));
      }
    }
  }
  return ret;
}

// for static engine batch
int ObExprToCharCommon::eval_oracle_to_char_batch(
    const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const int64_t batch_size)
{
  LOG_DEBUG("eval to_char in batch mode", K(batch_size));
  int ret = OB_SUCCESS;
  ObDatum *results = expr.locate_batch_datums(ctx);

  if (OB_ISNULL(results)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr results frame is not init", K(ret));
  } else {
    ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
    ObDatum *fmt_datum = NULL;
    ObDatum *nlsparam_datum = NULL;
    bool is_result_all_null = false;
    // if the second arg or the third arg is null, all the results are null
    if (2 == expr.arg_cnt_ || 3 == expr.arg_cnt_) {
      if (OB_FAIL(expr.args_[1]->eval(ctx, fmt_datum))) {
        LOG_WARN("eval fmt_datum failed", K(ret));
      } else if (fmt_datum->is_null()) {
        is_result_all_null = true;
      } else if (3 == expr.arg_cnt_) {
        if (OB_FAIL(expr.args_[2]->eval(ctx, nlsparam_datum))) {
          LOG_WARN("eval nlsparam_datum failed", K(ret));
        } else if (nlsparam_datum->is_null()) {
          is_result_all_null = true;
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (is_result_all_null) {
      for (int64_t j = 0; OB_SUCC(ret) && j < batch_size; ++j) {
        if (skip.at(j) || eval_flags.at(j)) {
          continue;
        } else {
          results[j].set_null();
          eval_flags.set(j);
        }
      }
    } else if (OB_FAIL(expr.args_[0]->eval_batch(ctx, skip, batch_size))) {
      LOG_WARN("failed to eval batch result args0", K(ret));
    } else {
      ObDatum *datum_array = expr.args_[0]->locate_batch_datums(ctx);
      ObString fmt;
      ObString nlsparam;
      ObEvalCtx::TempAllocGuard alloc_guard(ctx);
      ObIAllocator &alloc = alloc_guard.get_allocator();
      // convert the fmt && nlsparam to utf8 first.
      if (NULL != fmt_datum) {
        OZ(ObExprUtil::convert_string_collation(fmt_datum->get_string(),
                                                expr.args_[1]->datum_meta_.cs_type_,
                                                fmt, CS_TYPE_UTF8MB4_BIN, alloc));
      }
      if (OB_SUCC(ret) && NULL != nlsparam_datum) {
        OZ(ObExprUtil::convert_string_collation(nlsparam_datum->get_string(),
                                                expr.args_[2]->datum_meta_.cs_type_,
                                                nlsparam, CS_TYPE_UTF8MB4_BIN, alloc));
      }
      if (OB_SUCC(ret)) {
        const ObObjTypeClass input_tc = ob_obj_type_class(expr.args_[0]->datum_meta_.type_);
        if (1 == expr.arg_cnt_ && (ObStringTC == input_tc || ObTextTC == input_tc)) {
          for (int64_t j = 0; OB_SUCC(ret) && j < batch_size; ++j) {
            if (skip.at(j) || eval_flags.at(j)) {
              continue;
            } else if (datum_array[j].is_null()) {
              results[j].set_null();
              eval_flags.set(j);
            } else {
              results[j].set_datum(datum_array[j]);
              eval_flags.set(j);
            }
          }
        } else {
          ObString res;
          for (int64_t j = 0; OB_SUCC(ret) && j < batch_size; ++j) {
            if (skip.at(j) || eval_flags.at(j)) {
              continue;
            } else if (datum_array[j].is_null()) {
              results[j].set_null();
              eval_flags.set(j);
            } else {
              switch (input_tc) {
                case ObDateTimeTC:
                case ObOTimestampTC: {
                  OZ(datetime_to_char(expr, ctx, alloc, datum_array[j], fmt, nlsparam, res));
                  break;
                }
                case ObIntervalTC: {
                  OZ(interval_to_char(expr, ctx, alloc, datum_array[j], fmt, nlsparam, res));
                  break;
                }
                case ObIntTC: // to support PLS_INTERGER type
                case ObFloatTC:
                case ObDoubleTC:
                case ObNumberTC: {
                  if (NULL != nlsparam_datum) {
                    nlsparam = nlsparam_datum->get_string();
                  }
                  if (OB_FAIL(is_valid_to_char_number(expr))) {
                    LOG_WARN("fail to check num format", K(ret));
                  } else if (OB_FAIL(number_to_char(
                                     expr, ctx, alloc, datum_array[j], fmt, nlsparam, res))) {
                    // TODO:@xiaofeng.lby
                    // need to avoid calling ObNFMBase::parse_fmt in number_to_char more than once
                    LOG_WARN("number to char failed", K(ret));
                  }
                  break;
                }
                default: {
                  ret = OB_ERR_INVALID_TYPE_FOR_OP;
                  LOG_WARN("unsupported to_char", K(ret), K(input_tc));
                }
              }
              if (OB_SUCC(ret)) {
                const bool is_ascii = (ObDateTimeTC == input_tc || ObOTimestampTC == input_tc) ? false : true;
                const ObCollationType src_coll_type = (ObDateTimeTC == input_tc || ObOTimestampTC == input_tc)
                                                      ? ctx.exec_ctx_.get_my_session()->get_nls_collation()
                                                      : CS_TYPE_UTF8MB4_BIN;
                if (OB_FAIL(ObExprUtil::set_expr_ascii_result(expr, ctx, results[j], res, j,
                                                              is_ascii, src_coll_type))) {
                  LOG_WARN("set expr ascii result failed", K(ret));
                } else {
                  eval_flags.set(j);
                }
              }
            }
          }
        }
      }
    }
  }

  return ret;
}

int ObExprToCharCommon::is_valid_to_char_number(const ObExpr &expr)
{
  int ret = OB_SUCCESS;
  bool is_valid = false;
  if (OB_UNLIKELY(expr.arg_cnt_ < 1 || expr.arg_cnt_ > 3)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument.", K(ret), K(expr.arg_cnt_));
  } else if (expr.args_[0]->obj_meta_.is_numeric_type()) {
    is_valid = true;
  } else if (ObStringTC == expr.args_[0]->obj_meta_.get_type_class()
             && expr.arg_cnt_ >= 2) {
    is_valid = (ObNullTC == expr.args_[1]->obj_meta_.get_type_class()
        || ObStringTC == expr.args_[1]->obj_meta_.get_type_class());
    if (is_valid && expr.arg_cnt_ == 3) {
      is_valid = (ObNullTC == expr.args_[2]->obj_meta_.get_type_class()
          || ObStringTC == expr.args_[2]->obj_meta_.get_type_class());
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

int ObExprToCharCommon::convert_to_ob_time(ObEvalCtx &ctx,
                                           const ObDatum &input,
                                           const ObObjType input_type,
                                           ObTime &ob_time)
{
  int ret = OB_SUCCESS;
  const ObTimeZoneInfo *tz_info = get_timezone_info(ctx.exec_ctx_.get_my_session());
  switch (ob_obj_type_class(input_type)) {
    case ObOTimestampTC: {
      // oracle timestamp / timestamp with time zone / timestamp with local time zone
      const ObOTimestampData &otdata = (ObTimestampTZType == input_type)
          ? input.get_otimestamp_tz()
          : input.get_otimestamp_tiny();
      ret = ObTimeConverter::otimestamp_to_ob_time(input_type, otdata, tz_info, ob_time, false);
      break;
    }
    case ObDateTimeTC: {   //oracle date type
      ret = ObTimeConverter::datetime_to_ob_time(input.get_datetime(), NULL, ob_time);
      break;
    }
    default: {
      ret = OB_INVALID_DATE_VALUE;
      LOG_WARN("input value is invalid", K(ret));
    }
  }
  return ret;
}

int ObExprToCharCommon::datetime_to_char(const ObExpr &expr,
                                         ObEvalCtx &ctx,
                                         ObIAllocator &alloc,
                                         const ObDatum &input,
                                         const ObString &fmt,
                                         const ObString &nlsparam,
                                         ObString &res)
{
  int ret = OB_SUCCESS;
  ObString format_str;
  ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  CK(NULL != session);

  //param2: format
  if (OB_SUCC(ret)) {
    if (!fmt.empty()) {
      format_str = fmt;
    } else {
      if (OB_FAIL(session->get_local_nls_format(expr.args_[0]->datum_meta_.type_, format_str))) {
        LOG_WARN("failed to get default format", K(ret));
      }
    }
  }

  //param3: NLS settings
  if (OB_SUCC(ret) && !nlsparam.empty()) {
    if (!is_valid_nls_param(nlsparam)) {
      ret = OB_ERR_INVALID_NLS_PARAMETER_STRING;
      LOG_WARN("date format is invalid", K(ret), K(nlsparam));
    }
  }

  ObTime ob_time;
  const ObDatumMeta &input_meta = expr.args_[0]->datum_meta_;
  ObScale scale = input_meta.scale_;

  //determine type, get calc ob_time from input_value
  if (OB_SUCC(ret)) {
    if (OB_FAIL(convert_to_ob_time(ctx, input, input_meta.type_, ob_time))) {
      LOG_WARN("fail to convert to ob time", K(ret));
    }
  }

  //print result
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(format_str.empty())) {
      res.reset();
    } else {
      char *result_buf = NULL;
      int64_t pos = 0;
      int64_t result_buf_len = MAX_DATETIME_BUFFER_SIZE;
      if (OB_ISNULL(result_buf = static_cast<char *>(alloc.alloc(result_buf_len)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate buff", K(ret), K(result_buf_len));
      } else if (expr.arg_cnt_ > 1 && !!(expr.args_[1]->is_static_const_)) {
        auto rt_ctx_id = static_cast<uint64_t>(expr.expr_ctx_id_);
        ObExprDFMConvertCtx *dfm_convert_ctx = NULL;
        if (NULL == (dfm_convert_ctx = static_cast<ObExprDFMConvertCtx *>
                     (ctx.exec_ctx_.get_expr_op_ctx(rt_ctx_id)))) {
          if (OB_FAIL(ctx.exec_ctx_.create_expr_op_ctx(rt_ctx_id, dfm_convert_ctx))) {
            LOG_WARN("failed to create operator ctx", K(ret));
          } else if (OB_FAIL(dfm_convert_ctx->parse_format(format_str, expr.datum_meta_.type_,
                                                           false, ctx.exec_ctx_.get_allocator()))) {
            LOG_WARN("fail to parse format", K(ret), K(format_str));
          }
          LOG_DEBUG("new dfm convert ctx", K(ret), KPC(dfm_convert_ctx));
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(ObTimeConverter::ob_time_to_str_by_dfm_elems(ob_time, scale,
                                                                   dfm_convert_ctx->get_dfm_elems(),
                                                                   format_str, result_buf,
                                                                   result_buf_len, pos))) {
            LOG_WARN("failed to convert to string", K(ret), K(format_str));
          }
        }
      } else {
        if (OB_FAIL(ObTimeConverter::ob_time_to_str_oracle_dfm(
                  ob_time, scale, format_str, result_buf, result_buf_len, pos))) {
          LOG_WARN("failed to convert to varchar2", K(ret), K(format_str));
        }
      }
      if (OB_SUCC(ret)) {
        res = ObString(pos, result_buf);
      }
    }
  }

  LOG_DEBUG("oracle to char function finished",
            K(ret), K(ob_time), K(input), K(format_str), K(nlsparam), K(res));

  return ret;
}

int ObExprToCharCommon::interval_to_char(const ObExpr &expr,
                                         ObEvalCtx &ctx,
                                         ObIAllocator &alloc,
                                         const ObDatum &input,
                                         const ObString &fmt,
                                         const ObString &nlsparam,
                                         ObString &res)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  int64_t pos = 0;
  int64_t buf_len = MAX_INTERVAL_BUFFER_SIZE;

  ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  CK(NULL != session);


  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(buf = static_cast<char *>(alloc.alloc(buf_len)))) {
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
    const ObDatumMeta &input_meta = expr.args_[0]->datum_meta_;
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

int ObExprToCharCommon::number_to_char(const ObExpr &expr,
                                       ObEvalCtx &ctx,
                                       common::ObIAllocator &alloc,
                                       const ObDatum &input,
                                       common::ObString &fmt_str,
                                       const common::ObString &nlsparam,
                                       common::ObString &res)
{
  int ret = OB_SUCCESS;
  ObString number_raw;
  int scale = -1;
  const ObObjMeta &obj_meta = expr.args_[0]->obj_meta_;
  if (1 == expr.arg_cnt_) {
    if (OB_FAIL(process_number_sci_value(expr, alloc, input, scale, res))) {
      LOG_WARN("fail to get number string", K(ret));
    }
  } else {
    if (expr.arg_cnt_ > 2) {
      if (!nlsparam.empty() && OB_UNLIKELY(!ObExprOperator::is_valid_nls_param(nlsparam))) {
        ret = OB_ERR_INVALID_NLS_PARAMETER_STRING;
        LOG_WARN("date format is invalid", K(ret), K(nlsparam));
      }
    }
    if (OB_SUCC(ret)) {
      ObNFMToChar nfm;
      if (OB_FAIL(nfm.convert_num_to_fmt_str(obj_meta, input, alloc, fmt_str.ptr(),
                  fmt_str.length(), ctx, res))) {
        LOG_WARN("fail to convert num to fmt str", K(ret), K(fmt_str));
      }
    }
  }
  return ret;
}

int ObExprToCharCommon::process_number_sci_value(
    const ObExpr &expr, common::ObIAllocator &alloc, const ObDatum &input,
    const int scale, common::ObString &res)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  int64_t str_len = 0;
  const bool is_float = expr.args_[0]->obj_meta_.is_float();
  const bool is_double = expr.args_[0]->obj_meta_.is_double();
  const int64_t alloc_size = ((is_float || is_double)
                              ? MAX_NUMBER_BUFFER_SIZE
                              : MAX_TO_CHAR_BUFFER_SIZE);
  if (OB_ISNULL(buf = static_cast<char *>(alloc.alloc(alloc_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory for number string failed", K(ret));
  } else {
    if (is_double) {
      str_len = ob_gcvt_opt(input.get_double(), OB_GCVT_ARG_DOUBLE,
          static_cast<int32_t>(alloc_size), buf, NULL, lib::is_oracle_mode(), TRUE);
    } else if (is_float) {
      str_len = ob_gcvt_opt(input.get_float(), OB_GCVT_ARG_FLOAT,
          static_cast<int32_t>(alloc_size), buf, NULL, lib::is_oracle_mode(), TRUE);
    } else {
      number::ObNumber number_value;
      if (expr.args_[0]->obj_meta_.is_integer_type()) {
        if (OB_FAIL(number_value.from(input.get_int(), alloc))) {
          LOG_WARN("fail to int_number", K(ret));
        }
      } else {
        number_value.assign(input.get_number().desc_.desc_,
                            const_cast<uint32_t *>(&(input.get_number().digits_[0])));
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ObObjCaster::oracle_number_to_char(number_value, true,
          static_cast<int16_t>(scale), alloc_size, buf, str_len))) {
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

int ObExprToCharCommon::process_number_value(const ObExpr &expr,
                                             common::ObIAllocator &alloc,
                                             const ObDatum &input,
                                             const int scale,
                                             common::ObString &res)
{
  int ret = OB_SUCCESS;
  char *number_str = NULL;
  int64_t number_str_len = 0;
  number::ObNumber number_value;
  const bool is_float = expr.args_[0]->obj_meta_.is_float();
  const bool is_double = expr.args_[0]->obj_meta_.is_double();
  const int64_t alloc_size = MAX_TO_CHAR_BUFFER_SIZE;
  if (OB_ISNULL(number_str = static_cast<char *>(alloc.alloc(alloc_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory for number string failed", K(ret));
  } else {
    char buf_alloc[number::ObNumber::MAX_BYTE_LEN];
    ObDataBuffer tmp_allocator(buf_alloc, number::ObNumber::MAX_BYTE_LEN);
    if (is_float || is_double) {
      if (is_double) {
        number_str_len = ob_gcvt_strict(input.get_double(), OB_GCVT_ARG_DOUBLE, alloc_size,
                                        number_str, NULL, lib::is_oracle_mode(), TRUE, FALSE);

      } else {
        number_str_len = ob_gcvt_strict(input.get_float(), OB_GCVT_ARG_FLOAT, alloc_size,
                                        number_str, NULL, lib::is_oracle_mode(), TRUE, FALSE);
      }
      if (OB_FAIL(number_value.from(number_str, number_str_len, tmp_allocator))) {
        LOG_WARN("number from str failed", K(ret));
      }
      number_str_len = 0;
    } else {
      number_value.assign(input.get_number_desc().desc_, const_cast<uint32_t *>(input.get_number_digits()));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(number_value.format(
                  number_str, alloc_size, number_str_len, static_cast<int16_t>(scale)))) {
      LOG_WARN("failed to format number to string", K(ret));
    } else if (number_str_len == 1
                && number_str[0] == '0'
                && number_value.is_negative()) {
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


} // end of sql
} // end of oceanbase
