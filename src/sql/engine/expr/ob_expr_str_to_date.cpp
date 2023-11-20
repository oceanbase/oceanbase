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
#include "lib/timezone/ob_time_convert.h"
#include "lib/ob_name_def.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_util.h"

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
  if (OB_UNLIKELY(ObNullTC == format.get_type_class())) {
    type.set_datetime();
    type.set_scale(MAX_SCALE_FOR_TEMPORAL);
    type.set_precision(DATETIME_MIN_LENGTH + MAX_SCALE_FOR_TEMPORAL);
  } else {
    ObObj format_obj = format.get_param();
    if (OB_UNLIKELY(format_obj.is_null())) {
      type.set_datetime();
      type.set_scale(MAX_SCALE_FOR_TEMPORAL);
      type.set_precision(DATETIME_MIN_LENGTH + MAX_SCALE_FOR_TEMPORAL);
    } else if (!format_obj.is_string_type()) {
      type.set_datetime();
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
        type.set_date();
        type.set_precision(static_cast<ObPrecision>(DATE_MIN_LENGTH + type.get_scale()));
      } else if (!has_date) {
        type.set_time();
        type.set_precision(static_cast<ObPrecision>(TIME_MIN_LENGTH + type.get_scale()));
      } else {
        type.set_datetime();
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

void print_user_warning(const int ret, ObString date_str)
{
  if (OB_INVALID_DATE_FORMAT == ret) {
    ObString date_type_str("date");
    LOG_USER_WARN(OB_ERR_TRUNCATED_WRONG_VALUE, date_type_str.length(), date_type_str.ptr(),
                  date_str.length(), date_str.ptr());
  } else if (OB_INVALID_DATE_VALUE == ret || OB_INVALID_ARGUMENT == ret) {
    ObString datetime_type_str("datetime");
    ObString func_str("str_to_date");
    LOG_USER_WARN(OB_ERR_INCORRECT_VALUE_FOR_FUNCTION,
                  datetime_type_str.length(), datetime_type_str.ptr(),
                  date_str.length(), date_str.ptr(),
                  func_str.length(), func_str.ptr());
  }
}

int set_error_code(const int ori_ret, ObString date_str)
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
                  func_str.length(), func_str.ptr());
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
    const ObString &date_str = date_datum->get_string();
    const ObString &fmt_str = fmt_datum->get_string();
    ObTimeConvertCtx cvrt_ctx(TZ_INFO(session), false);
    ObDateSqlMode date_sql_mode;
    date_sql_mode.no_zero_in_date_ = is_no_zero_in_date(sql_mode);
    date_sql_mode.allow_incomplete_dates_ = !is_no_zero_in_date(sql_mode);
    if (FALSE_IT(date_sql_mode.init(sql_mode))) {
    } else if (OB_FAIL(ObTimeConverter::str_to_datetime_format(date_str, fmt_str, cvrt_ctx, res_int,
                                                               NULL, date_sql_mode))) {
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
          // or ObTimeConverter::datetime_to_date()
          res_int = ObTimeConverter::ZERO_DATETIME;
          print_user_warning(OB_INVALID_DATE_FORMAT, date_str);
        } else if (OB_INVALID_DATE_VALUE == tmp_ret || OB_INVALID_ARGUMENT == tmp_ret) {
          ret = OB_SUCCESS;
          is_null = true;
          print_user_warning(tmp_ret, date_str);
        } else {
          ret = tmp_ret;
        }
      } else {
        ret = set_error_code(tmp_ret, date_str);
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
  int32_t date_int = 0;
  if (OB_FAIL(calc(expr, ctx, is_null, datetime_int))) {
    LOG_WARN("calc str_to_date failed", K(ret), K(expr));
  } else if (is_null) {
    res_datum.set_null();
  } else if (OB_FAIL(ObTimeConverter::datetime_to_date(datetime_int, NULL, date_int))) {
    LOG_WARN("datetime_to_date failed", K(ret), K(datetime_int));
  } else {
    res_datum.set_date(date_int);
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
  if (ObDateType == rt_expr.datum_meta_.type_) {
    rt_expr.eval_func_ = calc_str_to_date_expr_date;
  } else if (ObTimeType == rt_expr.datum_meta_.type_) {
    rt_expr.eval_func_ = calc_str_to_date_expr_time;
  } else if (ObDateTimeType == rt_expr.datum_meta_.type_) {
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

}
}
