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
#include "sql/engine/expr/ob_expr_str_to_date.h"
#include "sql/engine/expr/ob_expr_date.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObExprStrToDate::ObExprStrToDate(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_STR_TO_DATE, N_STR_TO_DATE, 2, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprStrToDate::~ObExprStrToDate()
{
}


int ObExprStrToDate::calc_result_type2(ObExprResType &type,
                                       ObExprResType &date,
                                       ObExprResType &format,
                                       ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  UNUSED(date);
  int ret = OB_SUCCESS;
  ObObjType res_type = type_ctx.enable_mysql_compatible_dates() ?
      ObMySQLDateTimeType : ObDateTimeType;
  if (OB_UNLIKELY(ObNullTC == format.get_type_class())) {
    type.set_type(res_type);
    type.set_scale(MAX_SCALE_FOR_TEMPORAL);
    type.set_precision(DATETIME_MIN_LENGTH + MAX_SCALE_FOR_TEMPORAL);
  } else {
    ObObj format_obj = format.get_param();
    if (OB_UNLIKELY(format_obj.is_null())) {
      type.set_type(res_type);
      type.set_scale(MAX_SCALE_FOR_TEMPORAL);
      type.set_precision(DATETIME_MIN_LENGTH + MAX_SCALE_FOR_TEMPORAL);
    } else if (!format_obj.is_string_type()) {
      type.set_type(res_type);
      type.set_scale(DEFAULT_SCALE_FOR_INTEGER);
      type.set_precision(DATETIME_MIN_LENGTH + DEFAULT_SCALE_FOR_INTEGER);
    } else {
      ObString format_str = format_obj.get_varchar();
      bool has_date = false;
      bool has_time = false;
      uint32_t pos = 0;
      pos = ObCharset::locate(format_obj.get_collation_type(),
                              format_str.ptr(), format_str.length(), "%a", 2, 1)
            + ObCharset::locate(format_obj.get_collation_type(),
                                format_str.ptr(), format_str.length(), "%b", 2, 1)
            + ObCharset::locate(format_obj.get_collation_type(),
                                format_str.ptr(), format_str.length(), "%c", 2, 1)
            + ObCharset::locate(format_obj.get_collation_type(),
                                format_str.ptr(), format_str.length(), "%D", 2, 1)
            + ObCharset::locate(format_obj.get_collation_type(),
                                format_str.ptr(), format_str.length(), "%d", 2, 1)
            + ObCharset::locate(format_obj.get_collation_type(),
                                format_str.ptr(), format_str.length(), "%e", 2, 1)
            + ObCharset::locate(format_obj.get_collation_type(),
                                format_str.ptr(), format_str.length(), "%j", 2, 1)
            + ObCharset::locate(format_obj.get_collation_type(),
                                format_str.ptr(), format_str.length(), "%M", 2, 1)
            + ObCharset::locate(format_obj.get_collation_type(),
                                format_str.ptr(), format_str.length(), "%m", 2, 1)
            + ObCharset::locate(format_obj.get_collation_type(),
                                format_str.ptr(), format_str.length(), "%U", 2, 1)
            + ObCharset::locate(format_obj.get_collation_type(),
                                format_str.ptr(), format_str.length(), "%u", 2, 1)
            + ObCharset::locate(format_obj.get_collation_type(),
                                format_str.ptr(), format_str.length(), "%V", 2, 1)
            + ObCharset::locate(format_obj.get_collation_type(),
                                format_str.ptr(), format_str.length(), "%v", 2, 1)
            + ObCharset::locate(format_obj.get_collation_type(),
                                format_str.ptr(), format_str.length(), "%W", 2, 1)
            + ObCharset::locate(format_obj.get_collation_type(),
                                format_str.ptr(), format_str.length(), "%w", 2, 1)
            + ObCharset::locate(format_obj.get_collation_type(),
                                format_str.ptr(), format_str.length(), "%X", 2, 1)
            + ObCharset::locate(format_obj.get_collation_type(),
                                format_str.ptr(), format_str.length(), "%x", 2, 1)
            + ObCharset::locate(format_obj.get_collation_type(),
                                format_str.ptr(), format_str.length(), "%Y", 2, 1)
            + ObCharset::locate(format_obj.get_collation_type(),
                                format_str.ptr(), format_str.length(), "%y", 2, 1);
      if (pos > 0) {
        has_date = true;
      }

      pos = 0;
      pos = ObCharset::locate(format_obj.get_collation_type(),
                              format_str.ptr(), format_str.length(), "%f", 2, 1)
            + ObCharset::locate(format_obj.get_collation_type(),
                                format_str.ptr(), format_str.length(), "%H", 2, 1)
            + ObCharset::locate(format_obj.get_collation_type(),
                                format_str.ptr(), format_str.length(), "%h", 2, 1)
            + ObCharset::locate(format_obj.get_collation_type(),
                                format_str.ptr(), format_str.length(), "%I", 2, 1)
            + ObCharset::locate(format_obj.get_collation_type(),
                                format_str.ptr(), format_str.length(), "%i", 2, 1)
            + ObCharset::locate(format_obj.get_collation_type(),
                                format_str.ptr(), format_str.length(), "%k", 2, 1)
            + ObCharset::locate(format_obj.get_collation_type(),
                                format_str.ptr(), format_str.length(), "%l", 2, 1)
            + ObCharset::locate(format_obj.get_collation_type(),
                                format_str.ptr(), format_str.length(), "%p", 2, 1)
            + ObCharset::locate(format_obj.get_collation_type(),
                                format_str.ptr(), format_str.length(), "%r", 2, 1)
            + ObCharset::locate(format_obj.get_collation_type(),
                                format_str.ptr(), format_str.length(), "%S", 2, 1)
            + ObCharset::locate(format_obj.get_collation_type(),
                                format_str.ptr(), format_str.length(), "%s", 2, 1)
            + ObCharset::locate(format_obj.get_collation_type(),
                                format_str.ptr(), format_str.length(), "%T", 2, 1);
      if (pos > 0) {
        has_time = true;
      }

      pos = ObCharset::locate(format_obj.get_collation_type(),
                              format_str.ptr(), format_str.length(), "%f", 2, 1);

      if (0 == pos) {
        type.set_scale(DEFAULT_SCALE_FOR_INTEGER);
      } else {
        type.set_scale(MAX_SCALE_FOR_TEMPORAL);
      }

      if (!has_time) {
        type.set_type(type_ctx.enable_mysql_compatible_dates() ? ObMySQLDateType : ObDateType);
        type.set_precision(static_cast<ObPrecision>(DATE_MIN_LENGTH + type.get_scale()));
      } else if (!has_date) {
        type.set_time();
        type.set_precision(static_cast<ObPrecision>(TIME_MIN_LENGTH + type.get_scale()));
      } else {
        type.set_type(res_type);
        type.set_precision(static_cast<ObPrecision>(DATETIME_MIN_LENGTH + type.get_scale()));
      }


    }
  }
  if (OB_SUCC(ret)) {
    if (ob_is_enumset_tc(date.get_type())) {
      date.set_calc_type(ObVarcharType);
    }
    if (ob_is_enumset_tc(format.get_type())) {
      format.set_calc_type(ObVarcharType);
    }
    date.set_calc_type(ObVarcharType);
    format.set_calc_type(ObVarcharType);
  }
  return ret;
}

// Check if the error is a DFM (Date Format Model) range validation error
// These errors will be treated as invalid date values in MySQL mode
static bool is_dfm_range_error(int err_code)
{
  return err_code == OB_ERR_DAY_OF_MONTH_RANGE
      || err_code == OB_ERR_INVALID_YEAR_VALUE
      || err_code == OB_ERR_INVALID_MONTH
      || err_code == OB_ERR_INVALID_DAY_OF_THE_WEEK
      || err_code == OB_ERR_INVALID_DAY_OF_YEAR_VALUE
      || err_code == OB_ERR_INVALID_HOUR12_VALUE
      || err_code == OB_ERR_INVALID_HOUR24_VALUE
      || err_code == OB_ERR_INVALID_MINUTES_VALUE
      || err_code == OB_ERR_INVALID_SECONDS_VALUE
      || err_code == OB_ERR_INVALID_SECONDS_IN_DAY_VALUE
      || err_code == OB_ERR_INVALID_JULIAN_DATE_VALUE;
}

void print_user_warning(const int ret, ObString date_str, ObString func_name, ObString format_str = "")
{
  if (OB_INVALID_DATE_FORMAT == ret) {
    ObString date_type_str("date");
    LOG_USER_WARN(OB_ERR_TRUNCATED_WRONG_VALUE, date_type_str.length(), date_type_str.ptr(),
                  date_str.length(), date_str.ptr());
  } else if (OB_INVALID_DATE_VALUE == ret || OB_INVALID_ARGUMENT == ret || is_dfm_range_error(ret)) {
    ObString datetime_type_str("datetime");
    LOG_USER_WARN(OB_ERR_INCORRECT_VALUE_FOR_FUNCTION,
                  datetime_type_str.length(), datetime_type_str.ptr(),
                  date_str.length(), date_str.ptr(),
                  func_name.length(), func_name.ptr());
  } else if (OB_INVALID_DATE_FORMAT_END == ret) {
    ObString date_type_str("format");
    LOG_USER_WARN(OB_ERR_INCORRECT_VALUE_FOR_FUNCTION, date_type_str.length(), date_type_str.ptr(),
                  format_str.length(), format_str.ptr(),
                  func_name.length(), func_name.ptr());
  }
}

int set_error_code(const int ori_ret, ObString date_str, ObString func_name)
{
  int ret = ori_ret;
  if (OB_INVALID_DATE_FORMAT == ret) {
    ret = OB_ERR_TRUNCATED_WRONG_VALUE;
    ObString date_type_str("date");
    LOG_USER_ERROR(OB_ERR_TRUNCATED_WRONG_VALUE, date_type_str.length(), date_type_str.ptr(),
                  date_str.length(), date_str.ptr());
  } else if (OB_INVALID_DATE_VALUE == ret || OB_INVALID_ARGUMENT == ret) {
    ret = OB_ERR_INCORRECT_VALUE_FOR_FUNCTION;
    ObString datetime_type_str("datetime");
    ObString func_str("str_to_date");
    LOG_USER_ERROR(OB_ERR_INCORRECT_VALUE_FOR_FUNCTION,
                  datetime_type_str.length(), datetime_type_str.ptr(),
                  date_str.length(), date_str.ptr(),
                  func_name.length(), func_name.ptr());
  }
  return ret;
}

int ObExprOracleToDate::set_my_result_from_ob_time(ObExprCtx &expr_ctx,
                                                   ObTime &ob_time,
                                                   ObObj &result) const
{
  int ret = OB_SUCCESS;
  ObTimeConvertCtx time_cvrt_ctx(get_timezone_info(expr_ctx.my_session_), false);
  ObDateTime result_value;
  if (OB_FAIL(ObTimeConverter::ob_time_to_datetime(ob_time, time_cvrt_ctx, result_value))) {
    LOG_WARN("failed to convert ob time to datetime", K(ret));
  } else {
    result.set_datetime(result_value);
    result.set_scale(0);
  }
  return ret;
}

// If expr result type is ObMySQLDateType or ObMySQLDateTimeType, the value of `res_int` is
// ObMySQLDateTime, otherwise it is datetime value.
static int calc(const ObExpr &expr, ObEvalCtx &ctx, bool &is_null, int64_t &res_int)
{
  int ret = OB_SUCCESS;
  is_null = false;
  res_int = 0;
  ObDatum *date_datum = NULL;
  ObDatum *fmt_datum = NULL;
  const ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  ObSolidifiedVarsGetter helper(expr, ctx, ctx.exec_ctx_.get_my_session());
  ObSQLMode sql_mode = 0;
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else if (OB_FAIL(expr.args_[0]->eval(ctx, date_datum)) ||
             OB_FAIL(expr.args_[1]->eval(ctx, fmt_datum))) {
    LOG_WARN("eval arg failed", K(ret), KP(date_datum), KP(fmt_datum), K(expr));
  } else if (date_datum->is_null() || fmt_datum->is_null()) {
    is_null = true;
  } else if (OB_FAIL(helper.get_sql_mode(sql_mode))) {
    LOG_WARN("get sql mode failed", K(ret));
  } else {
    const bool is_mysql_datetime = ob_is_mysql_compact_dates_type(expr.datum_meta_.type_);
    const ObString &date_str = date_datum->get_string();
    const ObString &fmt_str = fmt_datum->get_string();
    ObTimeConvertCtx cvrt_ctx(TZ_INFO(session), false);
    ObDateSqlMode date_sql_mode;
    date_sql_mode.no_zero_in_date_ = is_no_zero_in_date(sql_mode);
    date_sql_mode.allow_incomplete_dates_ = !is_no_zero_in_date(sql_mode);
    date_sql_mode.allow_invalid_dates_ = is_allow_invalid_dates(sql_mode);
    if (FALSE_IT(date_sql_mode.init(sql_mode))) {
    } else if (is_mysql_datetime) {
      ObMySQLDateTime res_mdt = 0;
      ret = ObTimeConverter::str_to_mdatetime_format(date_str, fmt_str, cvrt_ctx, res_mdt, NULL,
                                                     date_sql_mode);
      res_int = res_mdt.datetime_;
    } else {
      ret = ObTimeConverter::str_to_datetime_format(date_str, fmt_str, cvrt_ctx, res_int, NULL,
                                                    date_sql_mode);
    }
    if (OB_FAIL(ret)) {
      int tmp_ret = ret;
      ObCastMode def_cast_mode = CM_NONE;
      ObSQLUtils::get_default_cast_mode(session->get_stmt_type(),
                                        session->is_ignore_stmt(),
                                        sql_mode,
                                        def_cast_mode);
      if (CM_IS_WARN_ON_FAIL(def_cast_mode)) {
        if (OB_INVALID_DATE_FORMAT == tmp_ret) {
          ret = OB_SUCCESS;
          // if res type is not datetime, will call ObTimeConverter::datetime_to_time()
          // or ObTimeConverter::datetime_to_date/mdate()
          res_int = is_mysql_datetime ?
            ObTimeConverter::MYSQL_ZERO_DATETIME : ObTimeConverter::ZERO_DATETIME;
          print_user_warning(OB_INVALID_DATE_FORMAT, date_str, ObString(N_STR_TO_DATE));
        } else if (OB_INVALID_DATE_VALUE == tmp_ret || OB_INVALID_ARGUMENT == tmp_ret) {
          ret = OB_SUCCESS;
          is_null = true;
          print_user_warning(tmp_ret, date_str, ObString(N_STR_TO_DATE));
        } else {
          ret = tmp_ret;
        }
      } else {
        ret = set_error_code(tmp_ret, date_str, ObString(N_STR_TO_DATE));
      }
    }
  }
  return ret;
}

int calc_str_to_date_expr_date(const ObExpr &expr, ObEvalCtx &ctx,
                                 ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  bool is_null = false;
  int64_t datetime_int = 0;
  if (OB_FAIL(calc(expr, ctx, is_null, datetime_int))) {
    LOG_WARN("calc str_to_date failed", K(ret), K(expr));
  } else if (is_null) {
    res_datum.set_null();
  } else if (expr.datum_meta_.type_ == ObMySQLDateType) {
    ObMySQLDate date_int = 0;
    if (OB_FAIL(ObTimeConverter::mdatetime_to_mdate(datetime_int, date_int))) {
      LOG_WARN("datetime_to_date failed", K(ret), K(datetime_int));
    } else {
      res_datum.set_mysql_date(date_int);
    }
  } else {
    int32_t date_int = 0;
    if (OB_FAIL(ObTimeConverter::datetime_to_date(datetime_int, NULL, date_int))) {
      LOG_WARN("datetime_to_date failed", K(ret), K(datetime_int));
    } else {
      res_datum.set_date(date_int);
    }
  }
  return ret;
}

int calc_str_to_date_expr_time(const ObExpr &expr, ObEvalCtx &ctx,
                                 ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  bool is_null = false;
  int64_t datetime_int = 0;
  int64_t time_int = 0;
  if (OB_FAIL(calc(expr, ctx, is_null, datetime_int))) {
    LOG_WARN("calc str_to_date failed", K(ret), K(expr));
  } else if (is_null) {
    res_datum.set_null();
  } else if (OB_FAIL(ObTimeConverter::datetime_to_time(datetime_int, NULL, time_int))) {
    LOG_WARN("datetime_to_time failed", K(ret), K(datetime_int));
  } else {
    res_datum.set_time(time_int);
  }
  return ret;
}

int calc_str_to_date_expr_datetime(const ObExpr &expr, ObEvalCtx &ctx,
                                 ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  bool is_null = false;
  int64_t datetime_int = 0;
  if (OB_FAIL(calc(expr, ctx, is_null, datetime_int))) {
    LOG_WARN("calc str_to_date failed", K(ret), K(expr));
  } else if (is_null) {
    res_datum.set_null();
  } else {
    res_datum.set_datetime(datetime_int);
  }
  return ret;
}

int ObExprStrToDate::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                             ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  if (ob_is_date_or_mysql_date(rt_expr.datum_meta_.type_)) {
    rt_expr.eval_func_ = calc_str_to_date_expr_date;
  } else if (ObTimeType == rt_expr.datum_meta_.type_) {
    rt_expr.eval_func_ = calc_str_to_date_expr_time;
  } else if (ob_is_datetime_or_mysql_datetime(rt_expr.datum_meta_.type_)) {
    rt_expr.eval_func_ = calc_str_to_date_expr_datetime;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected res type", K(ret), K(rt_expr.datum_meta_.type_));
  }
  return ret;
}

DEF_SET_LOCAL_SESSION_VARS(ObExprStrToDate, raw_expr) {
  int ret = OB_SUCCESS;
  SET_LOCAL_SYSVAR_CAPACITY(1);
  EXPR_ADD_LOCAL_SYSVAR(share::SYS_VAR_SQL_MODE);
  return ret;
}

ObExprToDate::ObExprToDate(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_TO_DATE, N_TO_DATE, MORE_THAN_ZERO, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

int ObExprToDate::calc_result_typeN(ObExprResType &type,
                                   ObExprResType *types_array,
                                   int64_t param_num,
                                   common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(param_num < 1) || OB_UNLIKELY(param_num > 2)) {
    ret = OB_INVALID_ARGUMENT_NUM;
    LOG_WARN("not enough params for function to_date", K(ret), K(param_num));
  } else if (OB_ISNULL(types_array) || OB_ISNULL(type_ctx.get_session())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(types_array), KP(type_ctx.get_session()));
  } else {
    ObExprResType &input_char = types_array[0];
    if (param_num == 1) {
      // 1个参数：使用默认格式，返回 DATE 类型
      // TO_DATE('2025-10-31') → 默认只有日期，返回 DATE
      type.set_type(type_ctx.enable_mysql_compatible_dates() ?
        ObMySQLDateType : ObDateType);
      input_char.set_calc_type(type.get_type());
      type.set_scale(DEFAULT_SCALE_FOR_INTEGER);
      type.set_precision(DATE_MIN_LENGTH);
      type_ctx.set_cast_mode(type_ctx.get_cast_mode() | CM_NULL_ON_WARN);
    } else if (param_num == 2) {
      // 情况2: 有format参数，返回 DATETIME 类型
      // to_date 的 format 参数已在 ob_sql_parameterization.cpp 中标记为不参数化
      // 保证不同的 format 值生成不同的scale
      input_char.set_calc_type(ObVarcharType);
      ObExprResType &format = types_array[1];
      format.set_calc_type(ObVarcharType);
      type.set_type(type_ctx.enable_mysql_compatible_dates() ?
        ObMySQLDateTimeType : ObDateTimeType);
      ObScale scale = 0;
      ObObj format_obj = format.get_param();
      if (OB_UNLIKELY(format_obj.is_null())) {
        scale = MAX_SCALE_FOR_TEMPORAL;
      } else if (!format_obj.is_string_type()) {
        scale = 0;
      } else {
        ObString format_str = format_obj.get_varchar();
        ObCollationType coll_type = format_obj.get_collation_type();
        static const char* ff_patterns[] = {"FF9", "FF8", "FF7", "FF6", "FF5",
                                            "FF4", "FF3", "FF2", "FF1"};
        bool has_ff = false;
        for (int i = 0; i < 9 && !has_ff; i++) {
          uint32_t pos = ObCharset::locate(coll_type,
                                           format_str.ptr(), format_str.length(),
                                           ff_patterns[i], 3, 1);
          if (pos > 0) {
            int precision = 9 - i;  // FF9->9, FF8->8, ..., FF1->1
            scale = static_cast<ObScale>(precision <= 6 ? precision : 6);  // MySQL 最多支持 6 位
            has_ff = true;
          }
        }
        if (!has_ff) {
          uint32_t pos = ObCharset::locate(coll_type,
                                           format_str.ptr(), format_str.length(),
                                           "FF", 2, 1);
          if (pos > 0) {
            scale = MAX_SCALE_FOR_TEMPORAL;  // 6
          }
        }
      }
      type.set_scale(scale);
      type.set_precision(DATETIME_MIN_LENGTH + scale);
    }
  }
  return ret;
}

int ObExprToDate::calc_to_date_with_format(const ObExpr &expr, ObEvalCtx &ctx,
                             ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *input_str = NULL;
  ObDatum *fmt = NULL;
  const ObSQLSessionInfo *session = NULL;

  if (OB_ISNULL(session = ctx.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  }

  OZ (expr.args_[0]->eval(ctx, input_str));
  OZ (expr.args_[1]->eval(ctx, fmt));

  if (OB_FAIL(ret)) {
  } else if (input_str->is_null() || fmt->is_null()) {
    res_datum.set_null();
  } else {
    const ObObjType target_type = expr.datum_meta_.type_;
    const bool is_mysql_datetime = ob_is_mysql_compact_dates_type(target_type);
    ObString format_str = fmt->get_string();
    ObTime ob_time;
    ObTimeConvertCtx time_cvrt_ctx(TZ_INFO(session), format_str, false);
    ObScale scale = 0; // not used for date/datetime
    // 获取并设置 date_sql_mode
    ObSolidifiedVarsGetter helper(expr, ctx, session);
    ObSQLMode sql_mode = 0;
    if (OB_FAIL(helper.get_sql_mode(sql_mode))) {
      LOG_WARN("get sql mode failed", K(ret));
    } else {
      ObDateSqlMode &date_sql_mode = time_cvrt_ctx.date_sql_mode_;
      date_sql_mode.no_zero_in_date_ = is_no_zero_in_date(sql_mode);
      date_sql_mode.allow_incomplete_dates_ = !is_no_zero_in_date(sql_mode);
      date_sql_mode.allow_invalid_dates_ = is_allow_invalid_dates(sql_mode);
      date_sql_mode.init(sql_mode);
    }

    // 优化：如果 format 是常量，缓存解析结果
    if (OB_FAIL(ret)) {
    } else if (expr.arg_cnt_ > 1 && expr.args_[1]->is_static_const_) {
      uint64_t rt_ctx_id = static_cast<uint64_t>(expr.expr_ctx_id_);
      ObExprDFMConvertCtx *dfm_convert_ctx = NULL;
      if (NULL == (dfm_convert_ctx = static_cast<ObExprDFMConvertCtx *>
                   (ctx.exec_ctx_.get_expr_op_ctx(rt_ctx_id)))) {
        if (OB_FAIL(ctx.exec_ctx_.create_expr_op_ctx(rt_ctx_id, dfm_convert_ctx))) {
          LOG_WARN("failed to create operator ctx", K(ret));
        } else if (OB_FAIL(dfm_convert_ctx->parse_format(format_str,
                                                         ObDateTimeType,
                                                         true,
                                                         ctx.exec_ctx_.get_allocator()))) {
          LOG_WARN("fail to parse format", K(ret), K(format_str));
        }
        LOG_DEBUG("new dfm convert ctx", K(ret), KPC(dfm_convert_ctx));
      }
      if (OB_SUCC(ret)) {
        ret = ObTimeConverter::str_to_ob_time_by_dfm_elems(input_str->get_string(),
                                                         dfm_convert_ctx->get_dfm_elems(),
                                                         dfm_convert_ctx->get_elem_flags(),
                                                         time_cvrt_ctx,
                                                         ObDateTimeType,
                                                         ob_time,
                                                         scale);
      }
    } else {
      // format 不是常量，每次都需要解析
      ret = ObTimeConverter::str_to_ob_time_oracle_dfm(input_str->get_string(),
                                                     time_cvrt_ctx,
                                                     ObDateTimeType,
                                                     ob_time,
                                                     scale);
    }

    if (OB_SUCC(ret)) {
      // 将 ob_time 转换为目标类型
      if (is_mysql_datetime) {
        ObMySQLDateTime mdt_value = 0;
        if (OB_FAIL(ObTimeConverter::ob_time_to_mdatetime(ob_time, mdt_value))) {
          LOG_WARN("failed to convert ob_time to mysql datetime", K(ret));
        } else {
          res_datum.set_mysql_datetime(mdt_value);
        }
      } else {
        ObTimeConvertCtx cvrt_ctx(TZ_INFO(session), false);
        ObDateTime result_value = 0;
        if (OB_FAIL(ObTimeConverter::ob_time_to_datetime(ob_time, cvrt_ctx, result_value))) {
          LOG_WARN("failed to convert ob_time to datetime", K(ret));
        } else {
          res_datum.set_datetime(result_value);
        }
      }
    }
    if (OB_FAIL(ret)) {
      int tmp_ret = ret;
      ObCastMode def_cast_mode = CM_NONE;
      ObSQLUtils::get_default_cast_mode(session->get_stmt_type(),
                                        session->is_ignore_stmt(),
                                        sql_mode,
                                        def_cast_mode);
      if (CM_IS_WARN_ON_FAIL(def_cast_mode)) {
        if (OB_INVALID_DATE_FORMAT == tmp_ret || OB_INVALID_DATE_VALUE == tmp_ret
          || OB_INVALID_ARGUMENT == tmp_ret || OB_INVALID_DATE_FORMAT_END == tmp_ret
          || is_dfm_range_error(tmp_ret)) {
          ret = OB_SUCCESS;
          res_datum.set_null();
          print_user_warning(tmp_ret, input_str->get_string(), ObString(N_TO_DATE), format_str);
        } else {
          ret = tmp_ret;
        }
      } else {
        ret = set_error_code(tmp_ret, input_str->get_string(), ObString(N_TO_DATE));
      }
    }
  }

  return ret;
}

template <typename InputVec, typename FmtVec, typename ResVec>
int vector_to_date_with_format(const ObExpr &expr, ObEvalCtx &ctx,
                                const ObBitVector &skip, const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  InputVec *input_vec = static_cast<InputVec *>(expr.args_[0]->get_vector(ctx));
  FmtVec *fmt_vec = static_cast<FmtVec *>(expr.args_[1]->get_vector(ctx));
  ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  const ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  ObSolidifiedVarsGetter helper(expr, ctx, ctx.exec_ctx_.get_my_session());
  const ObObjType target_type = expr.datum_meta_.type_;
  const bool is_mysql_datetime = ob_is_mysql_compact_dates_type(target_type);
  ObSQLMode sql_mode = 0;
  if (OB_ISNULL(session = ctx.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (OB_FAIL(helper.get_sql_mode(sql_mode))) {
    LOG_WARN("get sql mode failed", K(ret));
    return ret;
  } else {
    // 创建并设置 time_cvrt_ctx（循环外创建一次，避免重复设置 date_sql_mode）
    ObTimeConvertCtx time_cvrt_ctx(TZ_INFO(session), ObString(), false);
    ObDateSqlMode &date_sql_mode = time_cvrt_ctx.date_sql_mode_;
    ObExprDFMConvertCtx *dfm_convert_ctx = NULL;
    ObTimeZoneInfoPos literal_tz_info;
    date_sql_mode.no_zero_in_date_ = is_no_zero_in_date(sql_mode);
    date_sql_mode.allow_incomplete_dates_ = !is_no_zero_in_date(sql_mode);
    date_sql_mode.allow_invalid_dates_ = is_allow_invalid_dates(sql_mode);
    date_sql_mode.init(sql_mode);
    // 检查是否可以使用缓存的 format（format 是常量）
    if (expr.args_[1]->is_static_const_) {
      uint64_t rt_ctx_id = static_cast<uint64_t>(expr.expr_ctx_id_);
      if (NULL == (dfm_convert_ctx = static_cast<ObExprDFMConvertCtx *>
                  (ctx.exec_ctx_.get_expr_op_ctx(rt_ctx_id)))) {
        // 第一次执行，需要解析并缓存 format
        ObString format_str;
        if (!fmt_vec->is_null(bound.start())) {
          format_str = fmt_vec->get_string(bound.start());
          if (OB_FAIL(ctx.exec_ctx_.create_expr_op_ctx(rt_ctx_id, dfm_convert_ctx))) {
            LOG_WARN("failed to create operator ctx", K(ret));
          } else if (OB_FAIL(dfm_convert_ctx->parse_format(format_str,
                                                          ObDateTimeType,
                                                          true,
                                                          ctx.exec_ctx_.get_allocator()))) {
            LOG_WARN("fail to parse format", K(ret), K(format_str));
          }
        }
      }
    }
    for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
      if (skip.at(idx) || eval_flags.at(idx)) {
        continue;
      } else if (input_vec->is_null(idx) || fmt_vec->is_null(idx)) {
        res_vec->set_null(idx);
      } else {
        ObString input_str, format_str;
        input_str = input_vec->get_string(idx);
        format_str = fmt_vec->get_string(idx);
        // 更新 format_str（time_cvrt_ctx 本身可以复用）
        time_cvrt_ctx.oracle_nls_format_ = format_str;
        ObTime ob_time;
        ObScale scale = 0;
        // 解析字符串
        if (dfm_convert_ctx != NULL) {
          // 使用缓存的 format
          ret = ObTimeConverter::str_to_ob_time_by_dfm_elems(input_str,
                                                            dfm_convert_ctx->get_dfm_elems(),
                                                            dfm_convert_ctx->get_elem_flags(),
                                                            time_cvrt_ctx,
                                                            ObDateTimeType,
                                                            ob_time,
                                                            scale);
        } else {
          // 动态解析 format
          ret = ObTimeConverter::str_to_ob_time_oracle_dfm(input_str,
                                                          time_cvrt_ctx,
                                                          ObDateTimeType,
                                                          ob_time,
                                                          scale);
        }
        if (OB_SUCC(ret)) {
          // 转换为目标类型
          if (is_mysql_datetime) {
            ObMySQLDateTime mdt_value = 0;
            if (OB_FAIL(ObTimeConverter::ob_time_to_mdatetime(ob_time, mdt_value))) {
              LOG_WARN("failed to convert ob_time to mysql datetime", K(ret));
            } else {
              res_vec->set_datetime(idx, mdt_value.datetime_);
            }
          } else {
            ObTimeConvertCtx cvrt_ctx(TZ_INFO(session), false);
            ObDateTime result_value = 0;
            if (OB_FAIL(ObTimeConverter::ob_time_to_datetime(ob_time, cvrt_ctx, result_value, literal_tz_info))) {
              LOG_WARN("failed to convert ob_time to datetime", K(ret));
            } else {
              res_vec->set_datetime(idx, result_value);
            }
          }
        }
        if (OB_FAIL(ret)) {
          int tmp_ret = ret;
          ObCastMode def_cast_mode = CM_NONE;
          ObSQLUtils::get_default_cast_mode(session->get_stmt_type(),
                                            session->is_ignore_stmt(),
                                            sql_mode,
                                            def_cast_mode);
          if (CM_IS_WARN_ON_FAIL(def_cast_mode)) {
            if (OB_INVALID_DATE_FORMAT == tmp_ret || OB_INVALID_DATE_VALUE == tmp_ret
                || OB_INVALID_DATE_FORMAT_END == tmp_ret
                || OB_INVALID_ARGUMENT == tmp_ret
                || is_dfm_range_error(tmp_ret)) {
              ret = OB_SUCCESS;
              res_vec->set_null(idx);
              print_user_warning(tmp_ret, input_str, ObString(N_TO_DATE), format_str);
            } else {
              ret = tmp_ret;
            }
          } else {
            ret = set_error_code(tmp_ret, input_str, ObString(N_TO_DATE));
          }
        }
      }
    }
  }
  return ret;
}

#define DISPATCH_RES_FORMAT(InputVecType, FmtVecType) \
  if (VEC_FIXED == res_format) { \
    ret = vector_to_date_with_format<InputVecType, FmtVecType, DateTimeFixedVec> \
            (expr, ctx, skip, bound); \
  } else if (VEC_UNIFORM == res_format) { \
    ret = vector_to_date_with_format<InputVecType, FmtVecType, DateTimeUniVec> \
            (expr, ctx, skip, bound); \
  } else { \
    ret = vector_to_date_with_format<InputVecType, FmtVecType, ObVectorBase> \
            (expr, ctx, skip, bound); \
  }

#define DEF_TO_DATE_VECTOR_DISPATCH(input_fmt, fmt_fmt) \
  if (VEC_UNIFORM == input_fmt && VEC_UNIFORM == fmt_fmt) { \
    DISPATCH_RES_FORMAT(StrUniVec, StrUniVec); \
  } else if (VEC_UNIFORM == input_fmt && VEC_UNIFORM_CONST == fmt_fmt) { \
    DISPATCH_RES_FORMAT(StrUniVec, StrUniCVec); \
  } else if (VEC_UNIFORM == input_fmt && VEC_DISCRETE == fmt_fmt) { \
    DISPATCH_RES_FORMAT(StrUniVec, StrDiscVec); \
  } else if (VEC_UNIFORM_CONST == input_fmt && VEC_UNIFORM == fmt_fmt) { \
    DISPATCH_RES_FORMAT(StrUniCVec, StrUniVec); \
  } else if (VEC_UNIFORM_CONST == input_fmt && VEC_UNIFORM_CONST == fmt_fmt) { \
    DISPATCH_RES_FORMAT(StrUniCVec, StrUniCVec); \
  } else if (VEC_UNIFORM_CONST == input_fmt && VEC_DISCRETE == fmt_fmt) { \
    DISPATCH_RES_FORMAT(StrUniCVec, StrDiscVec); \
  } else if (VEC_DISCRETE == input_fmt && VEC_UNIFORM == fmt_fmt) { \
    DISPATCH_RES_FORMAT(StrDiscVec, StrUniVec); \
  } else if (VEC_DISCRETE == input_fmt && VEC_UNIFORM_CONST == fmt_fmt) { \
    DISPATCH_RES_FORMAT(StrDiscVec, StrUniCVec); \
  } else if (VEC_DISCRETE == input_fmt && VEC_DISCRETE == fmt_fmt) { \
    DISPATCH_RES_FORMAT(StrDiscVec, StrDiscVec); \
  } else { \
    DISPATCH_RES_FORMAT(ObVectorBase, ObVectorBase); \
  }

int ObExprToDate::calc_to_date_with_format_vector(const ObExpr &expr, ObEvalCtx &ctx,
                                     const ObBitVector &skip, const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("eval input string vector failed", K(ret));
  } else if (OB_FAIL(expr.args_[1]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("eval format vector failed", K(ret));
  } else {
    VectorFormat input_format = expr.args_[0]->get_format(ctx);
    VectorFormat fmt_format = expr.args_[1]->get_format(ctx);
    VectorFormat res_format = expr.get_format(ctx);
    DEF_TO_DATE_VECTOR_DISPATCH(input_format, fmt_format);
    if (OB_FAIL(ret)) {
      LOG_WARN("expr calculation failed", K(ret));
    }
  }
  return ret;
}

#undef DEF_TO_DATE_VECTOR_DISPATCH
#undef DISPATCH_RES_FORMAT

int ObExprToDate::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                          ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  if (rt_expr.arg_cnt_ == 1) {
    // 单参数：使用默认格式，返回 DATE 类型，直接复用 ObExprDate 的实现
    rt_expr.eval_func_ = ObExprDate::eval_date;
    rt_expr.eval_vector_func_ = ObExprDate::eval_date_vector;
  } else if (rt_expr.arg_cnt_ == 2) {
    // 双参数：使用 Oracle DFM 格式，返回 DATETIME 类型
    rt_expr.eval_func_ = calc_to_date_with_format;
    rt_expr.eval_vector_func_ = calc_to_date_with_format_vector;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected arg count", K(ret), K(rt_expr.arg_cnt_));
  }
  return ret;
}

DEF_SET_LOCAL_SESSION_VARS(ObExprToDate, raw_expr) {
  int ret = OB_SUCCESS;
  SET_LOCAL_SYSVAR_CAPACITY(1);
  EXPR_ADD_LOCAL_SYSVAR(share::SYS_VAR_SQL_MODE);
  return ret;
}

}
}
