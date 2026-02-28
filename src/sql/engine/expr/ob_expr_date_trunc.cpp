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
#include "ob_expr_date_trunc.h"
#include "sql/engine/expr/ob_datum_cast.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/ob_exec_context.h"
#include "lib/timezone/ob_time_convert.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{

ObExprDateTrunc::ObExprDateTrunc(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_DATE_TRUNC, N_DATE_TRUNC, 2, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprDateTrunc::~ObExprDateTrunc()
{
}

int ObExprDateTrunc::calc_result_type2(ObExprResType &type,
                                       ObExprResType &type1,
                                       ObExprResType &type2,
                                       common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  // type1 is the time_unit parameter (string) - PostgreSQL specification
  type1.set_calc_type(ObVarcharType);
  type1.set_calc_collation_utf8();
  type1.set_calc_collation_type(type_ctx.get_session()->get_nls_collation());

  // Set return type
  if (ob_is_datetime_or_mysql_datetime(type2.get_type())
      || ob_is_date_or_mysql_date(type2.get_type())) {
    type.set_type(type2.get_type());
  } else {
    type2.set_calc_type(ObDateTimeType);
    type.set_type(ObDateTimeType);
  }
  LOG_TRACE("print calc type", K(type2.get_type()), K(type2.get_calc_type()), K(type.get_type()));
  return ret;
}

int ObExprDateTrunc::cg_expr(ObExprCGCtx &op_cg_ctx,
                            const ObRawExpr &raw_expr,
                            ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = ObExprDateTrunc::eval_date_trunc;
  rt_expr.eval_vector_func_ = ObExprDateTrunc::eval_date_trunc_vector;
  return ret;
}

// Template version of process_time_param
template <ObObjType PARAM_TYPE>
int ObExprDateTrunc::process_time_param(const char *param_ptr,
                                        ObSolidifiedVarsGetter &helper,
                                        ObTime &ob_time)
{
  int ret = OB_SUCCESS;
  const common::ObTimeZoneInfo *tz_info = NULL;
  if constexpr (PARAM_TYPE == ObDateTimeType) {
    if (OB_FAIL(ObTimeConverter::datetime_to_ob_time(*reinterpret_cast<const int64_t *> (param_ptr), tz_info, ob_time))) {
      LOG_WARN("fail to convert datetime to ob_time", K(ret));
    }
  } else if constexpr (PARAM_TYPE == ObMySQLDateTimeType) {
    if (OB_FAIL(ObTimeConverter::mdatetime_to_ob_time<true>(*reinterpret_cast<const ObMySQLDateTime *> (param_ptr), ob_time))) {
      LOG_WARN("fail to convert datetime to ob_time", K(ret));
    }
  } else if constexpr (PARAM_TYPE == ObDateType) {
    if (OB_FAIL(ObTimeConverter::date_to_ob_time(*reinterpret_cast<const int32_t *> (param_ptr), ob_time))) {
      LOG_WARN("fail to convert datetime to ob_time", K(ret));
    }
  } else if constexpr (PARAM_TYPE == ObMySQLDateType) {
    if (OB_FAIL(ObTimeConverter::mdate_to_ob_time<true>(*reinterpret_cast<const ObMySQLDate *> (param_ptr), ob_time))) {
      LOG_WARN("fail to convert datetime to ob_time", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid param type", K(PARAM_TYPE), K(ret));
  }
  return ret;
}

// Runtime dispatch function for eval_date_trunc
int ObExprDateTrunc::process_time_param_dispatch(const ObObjType param_type,
                                                  const char *param_ptr,
                                                  ObSolidifiedVarsGetter &helper,
                                                  ObTime &ob_time)
{
  int ret = OB_SUCCESS;
  switch (param_type) {
    case ObDateTimeType: {
      ret = process_time_param<ObDateTimeType>(param_ptr, helper, ob_time);
      break;
    }
    case ObMySQLDateTimeType: {
      ret = process_time_param<ObMySQLDateTimeType>(param_ptr, helper, ob_time);
      break;
    }
    case ObDateType: {
      ret = process_time_param<ObDateType>(param_ptr, helper, ob_time);
      break;
    }
    case ObMySQLDateType: {
      ret = process_time_param<ObMySQLDateType>(param_ptr, helper, ob_time);
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get invalid param type", K(param_type), K(ret));
      break;
    }
  }
  return ret;
}

int ObExprDateTrunc::set_time_res(const ObObjType res_type, ObTime &ob_time, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObTimeConvertCtx cvrt_ctx(NULL, false);
  // Recalculate date-related fields (for DATE types, use DT_DATE directly since WEEK processing has already updated year/month/day via date_to_ob_time)
  if (ObDateType == res_type || ObMySQLDateType == res_type) {
    // For DATE types, DT_DATE is already correctly set by date_to_ob_time, no need to recalculate
  } else {
    // For DATETIME types, need to recalculate DT_DATE to ensure consistency
    ob_time.parts_[DT_DATE] = ObTimeConverter::ob_time_to_date(ob_time);
  }
  switch (res_type) {
    case (ObDateTimeType): {
      int64_t result_datetime = 0;
      if (OB_FAIL(ObTimeConverter::ob_time_to_datetime(ob_time, cvrt_ctx, result_datetime))) {
        LOG_WARN("fail to convert ob_time to datetime", K(ret), K(ob_time));
      } else {
        expr_datum.set_datetime(result_datetime);
      }
      break;
    }
    case (ObMySQLDateTimeType): {
      ObMySQLDateTime result_datetime;
      if (OB_FAIL(ObTimeConverter::ob_time_to_mdatetime(ob_time, result_datetime))) {
        LOG_WARN("fail to convert ob_time to datetime", K(ret), K(ob_time));
      } else {
        expr_datum.set_mysql_datetime(result_datetime);
      }
      break;
    }
    case (ObDateType): {
      expr_datum.set_date(ob_time.parts_[DT_DATE]);
      break;
    }
    case (ObMySQLDateType): {
      expr_datum.set_mysql_date(ObTimeConverter::ob_time_to_mdate(ob_time));
      break;
    }
    default :
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get invalid param type", K(res_type), K(ret));
  }
  return ret;
}

DateTruncTimeUnit ObExprDateTrunc::parse_time_unit(const ObString &time_unit)
{
  if (time_unit.case_compare_equal("microseconds")) {
    return DateTruncTimeUnit::MICROSECONDS;
  } else if (time_unit.case_compare_equal("milliseconds")) {
    return DateTruncTimeUnit::MILLISECONDS;
  } else if (time_unit.case_compare_equal("second")) {
    return DateTruncTimeUnit::SECOND;
  } else if (time_unit.case_compare_equal("minute")) {
    return DateTruncTimeUnit::MINUTE;
  } else if (time_unit.case_compare_equal("hour")) {
    return DateTruncTimeUnit::HOUR;
  } else if (time_unit.case_compare_equal("day")) {
    return DateTruncTimeUnit::DAY;
  } else if (time_unit.case_compare_equal("week")) {
    return DateTruncTimeUnit::WEEK;
  } else if (time_unit.case_compare_equal("month")) {
    return DateTruncTimeUnit::MONTH;
  } else if (time_unit.case_compare_equal("quarter")) {
    return DateTruncTimeUnit::QUARTER;
  } else if (time_unit.case_compare_equal("year")) {
    return DateTruncTimeUnit::YEAR;
  } else if (time_unit.case_compare_equal("decade")) {
    return DateTruncTimeUnit::DECADE;
  } else if (time_unit.case_compare_equal("century")) {
    return DateTruncTimeUnit::CENTURY;
  } else if (time_unit.case_compare_equal("millennium")) {
    return DateTruncTimeUnit::MILLENNIUM;
  } else {
    return DateTruncTimeUnit::INVALID;
  }
}

// Template version of do_date_trunc, accepts compile-time time_unit parameter
// 现在调用统一的模板函数实现，实现代码复用
template <DateTruncTimeUnit TIME_UNIT>
int ObExprDateTrunc::do_date_trunc_template(ObTime &ob_time)
{
  int ret = OB_SUCCESS;
  if constexpr (TIME_UNIT == DateTruncTimeUnit::MICROSECONDS) {
    ret = ObExprTRDateFormat::trunc_obtime_by_unit_template<
        ObExprTRDateFormat::TruncTimeUnit::MICROSECONDS>(ob_time, false);
  } else if constexpr (TIME_UNIT == DateTruncTimeUnit::MILLISECONDS) {
    ret = ObExprTRDateFormat::trunc_obtime_by_unit_template<
        ObExprTRDateFormat::TruncTimeUnit::MILLISECONDS>(ob_time, false);
  } else if constexpr (TIME_UNIT == DateTruncTimeUnit::SECOND) {
    ret = ObExprTRDateFormat::trunc_obtime_by_unit_template<
        ObExprTRDateFormat::TruncTimeUnit::SECOND>(ob_time, false);
  } else if constexpr (TIME_UNIT == DateTruncTimeUnit::MINUTE) {
    ret = ObExprTRDateFormat::trunc_obtime_by_unit_template<
        ObExprTRDateFormat::TruncTimeUnit::MINUTE>(ob_time, false);
  } else if constexpr (TIME_UNIT == DateTruncTimeUnit::HOUR) {
    ret = ObExprTRDateFormat::trunc_obtime_by_unit_template<
        ObExprTRDateFormat::TruncTimeUnit::HOUR>(ob_time, false);
  } else if constexpr (TIME_UNIT == DateTruncTimeUnit::DAY) {
    ret = ObExprTRDateFormat::trunc_obtime_by_unit_template<
        ObExprTRDateFormat::TruncTimeUnit::DAY>(ob_time, false);
  } else if constexpr (TIME_UNIT == DateTruncTimeUnit::WEEK) {
    ret = ObExprTRDateFormat::trunc_obtime_by_unit_template<
        ObExprTRDateFormat::TruncTimeUnit::WEEK>(ob_time, false);
  } else if constexpr (TIME_UNIT == DateTruncTimeUnit::MONTH) {
    ret = ObExprTRDateFormat::trunc_obtime_by_unit_template<
        ObExprTRDateFormat::TruncTimeUnit::MONTH>(ob_time, false);
  } else if constexpr (TIME_UNIT == DateTruncTimeUnit::QUARTER) {
    ret = ObExprTRDateFormat::trunc_obtime_by_unit_template<
        ObExprTRDateFormat::TruncTimeUnit::QUARTER>(ob_time, false);
  } else if constexpr (TIME_UNIT == DateTruncTimeUnit::YEAR) {
    ret = ObExprTRDateFormat::trunc_obtime_by_unit_template<
        ObExprTRDateFormat::TruncTimeUnit::YEAR>(ob_time, false);
  } else if constexpr (TIME_UNIT == DateTruncTimeUnit::DECADE) {
    ret = ObExprTRDateFormat::trunc_obtime_by_unit_template<
        ObExprTRDateFormat::TruncTimeUnit::DECADE>(ob_time, false);
  } else if constexpr (TIME_UNIT == DateTruncTimeUnit::CENTURY) {
    // PostgreSQL 的 century 计算方式：((year-1)/100)*100+1
    // 但 ObExprTRDateFormat 使用 Oracle 语义：year/100*100+1
    // 需要特殊处理：先保存原始年份，然后使用 PostgreSQL 语义
    int32_t original_year = ob_time.parts_[DT_YEAR];
    ret = ObExprTRDateFormat::trunc_obtime_by_unit_template<
        ObExprTRDateFormat::TruncTimeUnit::CENTURY>(ob_time, false);
    if (OB_SUCC(ret)) {
      // PostgreSQL 语义：((year-1)/100)*100+1
      int32_t century_start_year = ((original_year - 1) / 100) * 100 + 1;
      if (century_start_year != ob_time.parts_[DT_YEAR]) {
        ob_time.parts_[DT_YEAR] = century_start_year;
        ob_time.parts_[DT_DATE] = ObTimeConverter::ob_time_to_date(ob_time);
      }
    }
  } else if constexpr (TIME_UNIT == DateTruncTimeUnit::MILLENNIUM) {
    ret = ObExprTRDateFormat::trunc_obtime_by_unit_template<
        ObExprTRDateFormat::TruncTimeUnit::MILLENNIUM>(ob_time, false);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected time unit", K(ret), K(static_cast<int>(TIME_UNIT)));
  }

  return ret;
}

// Template function that directly operates on ObMySQLDateTime to avoid ObTime conversion
template <DateTruncTimeUnit TIME_UNIT>
int ObExprDateTrunc::do_date_trunc_mysql_datetime_template(ObMySQLDateTime &mysql_datetime)
{
  int ret = OB_SUCCESS;
  if constexpr (TIME_UNIT == DateTruncTimeUnit::MICROSECONDS) {
    // No truncation at microsecond level
  } else if constexpr (TIME_UNIT == DateTruncTimeUnit::MILLISECONDS) {
    // Truncate to millisecond level
    mysql_datetime.microseconds_ = (mysql_datetime.microseconds_ / 1000) * 1000;
  } else if constexpr (TIME_UNIT == DateTruncTimeUnit::SECOND) {
    // Truncate to second level
    mysql_datetime.microseconds_ = 0;
  } else if constexpr (TIME_UNIT == DateTruncTimeUnit::MINUTE) {
    // Truncate to minute level
    mysql_datetime.second_ = 0;
    mysql_datetime.microseconds_ = 0;
  } else if constexpr (TIME_UNIT == DateTruncTimeUnit::HOUR) {
    // Truncate to hour level
    mysql_datetime.minute_ = 0;
    mysql_datetime.second_ = 0;
    mysql_datetime.microseconds_ = 0;
  } else if constexpr (TIME_UNIT == DateTruncTimeUnit::DAY) {
    // Truncate to day level
    mysql_datetime.hour_ = 0;
    mysql_datetime.minute_ = 0;
    mysql_datetime.second_ = 0;
    mysql_datetime.microseconds_ = 0;
  } else if constexpr (TIME_UNIT == DateTruncTimeUnit::WEEK) {
    // Truncate to week level (using ObTime conversion)
    ObTime ob_time;
    if (OB_FAIL(ObTimeConverter::mdatetime_to_ob_time<true>(mysql_datetime, ob_time))) {
      LOG_WARN("fail to convert mdatetime to ob_time", K(ret));
    } else if (FALSE_IT(ob_time.parts_[DT_DATE] = ObTimeConverter::ob_time_to_date(ob_time))) {
    } else if (FALSE_IT(ob_time.parts_[DT_WDAY] = WDAY_OFFSET[ob_time.parts_[DT_DATE] % DAYS_PER_WEEK][EPOCH_WDAY])) {
    } else if (OB_FAIL(do_date_trunc_template<DateTruncTimeUnit::WEEK>(ob_time))) {
      LOG_WARN("fail to do date trunc on ob_time", K(ret));
    } else if (OB_FAIL(ObTimeConverter::ob_time_to_mdatetime(ob_time, mysql_datetime))) {
      LOG_WARN("fail to convert ob_time to mdatetime", K(ret));
    }
  } else if constexpr (TIME_UNIT == DateTruncTimeUnit::MONTH) {
    // Truncate to month level
    mysql_datetime.day_ = 1;
    mysql_datetime.hour_ = 0;
    mysql_datetime.minute_ = 0;
    mysql_datetime.second_ = 0;
    mysql_datetime.microseconds_ = 0;
  } else if constexpr (TIME_UNIT == DateTruncTimeUnit::QUARTER) {
    // Truncate to quarter level
    int32_t year = mysql_datetime.year();
    int32_t month = mysql_datetime.month();
    int32_t quarter_start_month = ((month - 1) / 3) * 3 + 1;
    mysql_datetime.year_month_ = ObMySQLDateTime::year_month(year, quarter_start_month);
    mysql_datetime.day_ = 1;
    mysql_datetime.hour_ = 0;
    mysql_datetime.minute_ = 0;
    mysql_datetime.second_ = 0;
    mysql_datetime.microseconds_ = 0;
  } else if constexpr (TIME_UNIT == DateTruncTimeUnit::YEAR) {
    // Truncate to year level
    int32_t year = mysql_datetime.year();
    mysql_datetime.year_month_ = ObMySQLDateTime::year_month(year, 1);
    mysql_datetime.day_ = 1;
    mysql_datetime.hour_ = 0;
    mysql_datetime.minute_ = 0;
    mysql_datetime.second_ = 0;
    mysql_datetime.microseconds_ = 0;
  } else if constexpr (TIME_UNIT == DateTruncTimeUnit::DECADE) {
    // Truncate to decade level
    int32_t year = mysql_datetime.year();
    int32_t decade_start_year = (year / 10) * 10;
    mysql_datetime.year_month_ = ObMySQLDateTime::year_month(decade_start_year, 1);
    mysql_datetime.day_ = 1;
    mysql_datetime.hour_ = 0;
    mysql_datetime.minute_ = 0;
    mysql_datetime.second_ = 0;
    mysql_datetime.microseconds_ = 0;
  } else if constexpr (TIME_UNIT == DateTruncTimeUnit::CENTURY) {
    // Truncate to century level
    int32_t year = mysql_datetime.year();
    int32_t century_start_year = ((year - 1) / 100) * 100 + 1;
    mysql_datetime.year_month_ = ObMySQLDateTime::year_month(century_start_year, 1);
    mysql_datetime.day_ = 1;
    mysql_datetime.hour_ = 0;
    mysql_datetime.minute_ = 0;
    mysql_datetime.second_ = 0;
    mysql_datetime.microseconds_ = 0;
  } else if constexpr (TIME_UNIT == DateTruncTimeUnit::MILLENNIUM) {
    // Truncate to millennium level
    int32_t year = mysql_datetime.year();
    int32_t millennium_start_year = ((year - 1) / 1000) * 1000 + 1;
    mysql_datetime.year_month_ = ObMySQLDateTime::year_month(millennium_start_year, 1);
    mysql_datetime.day_ = 1;
    mysql_datetime.hour_ = 0;
    mysql_datetime.minute_ = 0;
    mysql_datetime.second_ = 0;
    mysql_datetime.microseconds_ = 0;
  }
  return ret;
}

// Template function that directly operates on ObMySQLDate to avoid ObTime conversion
template <DateTruncTimeUnit TIME_UNIT>
int ObExprDateTrunc::do_date_trunc_mysql_date_template(ObMySQLDate &mysql_date)
{
  int ret = OB_SUCCESS;
  if constexpr (TIME_UNIT == DateTruncTimeUnit::MICROSECONDS ||
                TIME_UNIT == DateTruncTimeUnit::MILLISECONDS ||
                TIME_UNIT == DateTruncTimeUnit::SECOND ||
                TIME_UNIT == DateTruncTimeUnit::MINUTE ||
                TIME_UNIT == DateTruncTimeUnit::HOUR ||
                TIME_UNIT == DateTruncTimeUnit::DAY) {
    // For date types, these time units don't need truncation (already at date level)
  } else if constexpr (TIME_UNIT == DateTruncTimeUnit::WEEK) {
    // Truncate to week level (using ObTime conversion)
    ObTime ob_time;
    if (OB_FAIL(ObTimeConverter::mdate_to_ob_time<true>(mysql_date, ob_time))) {
      LOG_WARN("fail to convert mdate to ob_time", K(ret));
    } else if (OB_FAIL(do_date_trunc_template<DateTruncTimeUnit::WEEK>(ob_time))) {
      LOG_WARN("fail to do date trunc on ob_time", K(ret));
    } else {
      mysql_date = ObTimeConverter::ob_time_to_mdate(ob_time);
    }
  } else if constexpr (TIME_UNIT == DateTruncTimeUnit::MONTH) {
    // Truncate to month level
    mysql_date.day_ = 1;
  } else if constexpr (TIME_UNIT == DateTruncTimeUnit::QUARTER) {
    // Truncate to quarter level
    int32_t month = mysql_date.month_;
    int32_t quarter_start_month = ((month - 1) / 3) * 3 + 1;
    mysql_date.month_ = quarter_start_month;
    mysql_date.day_ = 1;
  } else if constexpr (TIME_UNIT == DateTruncTimeUnit::YEAR) {
    // Truncate to year level
    mysql_date.month_ = 1;
    mysql_date.day_ = 1;
  } else if constexpr (TIME_UNIT == DateTruncTimeUnit::DECADE) {
    // Truncate to decade level
    int32_t year = mysql_date.year_;
    int32_t decade_start_year = (year / 10) * 10;
    mysql_date.year_ = decade_start_year;
    mysql_date.month_ = 1;
    mysql_date.day_ = 1;
  } else if constexpr (TIME_UNIT == DateTruncTimeUnit::CENTURY) {
    // Truncate to century level
    int32_t year = mysql_date.year_;
    int32_t century_start_year = ((year - 1) / 100) * 100 + 1;
    mysql_date.year_ = century_start_year;
    mysql_date.month_ = 1;
    mysql_date.day_ = 1;
  } else if constexpr (TIME_UNIT == DateTruncTimeUnit::MILLENNIUM) {
    // Truncate to millennium level
    int32_t year = mysql_date.year_;
    int32_t millennium_start_year = ((year - 1) / 1000) * 1000 + 1;
    mysql_date.year_ = millennium_start_year;
    mysql_date.month_ = 1;
    mysql_date.day_ = 1;
  }
  return ret;
}

// Runtime dispatch function for eval_date_trunc
int ObExprDateTrunc::do_date_trunc_dispatch(const ObString &time_unit, ObTime &ob_time)
{
  int ret = OB_SUCCESS;
  DateTruncTimeUnit unit = parse_time_unit(time_unit);

  if (unit == DateTruncTimeUnit::INVALID) {
    ret = OB_INVALID_DATE_TRUNC_FORMAT;
    LOG_WARN("invalid time unit", K(ret), K(time_unit));
    LOG_USER_ERROR(OB_INVALID_DATE_TRUNC_FORMAT, time_unit.length(), time_unit.ptr());
  } else {
    switch (unit) {
      case DateTruncTimeUnit::MICROSECONDS:
        ret = do_date_trunc_template<DateTruncTimeUnit::MICROSECONDS>(ob_time);
        break;
      case DateTruncTimeUnit::MILLISECONDS:
        ret = do_date_trunc_template<DateTruncTimeUnit::MILLISECONDS>(ob_time);
        break;
      case DateTruncTimeUnit::SECOND:
        ret = do_date_trunc_template<DateTruncTimeUnit::SECOND>(ob_time);
        break;
      case DateTruncTimeUnit::MINUTE:
        ret = do_date_trunc_template<DateTruncTimeUnit::MINUTE>(ob_time);
        break;
      case DateTruncTimeUnit::HOUR:
        ret = do_date_trunc_template<DateTruncTimeUnit::HOUR>(ob_time);
        break;
      case DateTruncTimeUnit::DAY:
        ret = do_date_trunc_template<DateTruncTimeUnit::DAY>(ob_time);
        break;
      case DateTruncTimeUnit::WEEK:
        ret = do_date_trunc_template<DateTruncTimeUnit::WEEK>(ob_time);
        break;
      case DateTruncTimeUnit::MONTH:
        ret = do_date_trunc_template<DateTruncTimeUnit::MONTH>(ob_time);
        break;
      case DateTruncTimeUnit::QUARTER:
        ret = do_date_trunc_template<DateTruncTimeUnit::QUARTER>(ob_time);
        break;
      case DateTruncTimeUnit::YEAR:
        ret = do_date_trunc_template<DateTruncTimeUnit::YEAR>(ob_time);
        break;
      case DateTruncTimeUnit::DECADE:
        ret = do_date_trunc_template<DateTruncTimeUnit::DECADE>(ob_time);
        break;
      case DateTruncTimeUnit::CENTURY:
        ret = do_date_trunc_template<DateTruncTimeUnit::CENTURY>(ob_time);
        break;
      case DateTruncTimeUnit::MILLENNIUM:
        ret = do_date_trunc_template<DateTruncTimeUnit::MILLENNIUM>(ob_time);
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected time unit", K(ret), K(unit));
        break;
    }
  }
  return ret;
}

int ObExprDateTrunc::eval_date_trunc(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *time_unit_datum = NULL;
  ObDatum *datetime_datum = NULL;
  ObObjType meta_type = expr.args_[1]->datum_meta_.type_;

  // Get first parameter: time_unit (PostgreSQL specification)
  if (OB_FAIL(expr.args_[0]->eval(ctx, time_unit_datum))) {
    LOG_WARN("fail to eval time_unit expr", K(ret), K(expr));
  } else if (time_unit_datum->is_null()) {
    res_datum.set_null();
  } else if (OB_FAIL(expr.args_[1]->eval(ctx, datetime_datum))) {
    LOG_WARN("fail to eval datetime expr", K(ret), K(expr));
  } else if (datetime_datum->is_null()) {
    res_datum.set_null();
  } else {
    ObString time_unit = time_unit_datum->get_string();
    // Convert datetime to ObTime structure
    ObTime ob_time;
    ObSolidifiedVarsGetter helper(expr, ctx, ctx.exec_ctx_.get_my_session());
    if (OB_FAIL(process_time_param_dispatch(meta_type, datetime_datum->ptr_, helper, ob_time))) {
      LOG_WARN("failed to process time", K(ret));
    } else if (ob_time.parts_[DT_DATE] == common::ObTimeConverter::ZERO_DATE) {
      res_datum.set_null();
    } else if (OB_FAIL(do_date_trunc_dispatch(time_unit, ob_time))) {
      LOG_WARN("failed to do date trunc", K(ret));
    } else if (OB_FAIL(set_time_res(expr.datum_meta_.type_, ob_time, res_datum))) {
      LOG_WARN("failed to set time res", K(ret), K(expr.datum_meta_.type_));
    }
  }
  return ret;
}



int ObExprDateTrunc::eval_date_trunc_vector(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  ObObjType datetime_type = expr.args_[1]->datum_meta_.type_;

  // Choose different template based on whether time_unit is constant
  bool is_time_unit_const = expr.args_[0]->is_const_expr();

  if (is_time_unit_const) {
    // time_unit is constant, can use optimized template
    switch (datetime_type) {
      case ObDateTimeType:
        ret = eval_date_trunc_vector_template<true, ObDateTimeType>(expr, ctx, skip, bound);
        break;
      case ObMySQLDateTimeType:
        ret = eval_date_trunc_vector_template<true, ObMySQLDateTimeType>(expr, ctx, skip, bound);
        break;
      case ObDateType:
        ret = eval_date_trunc_vector_template<true, ObDateType>(expr, ctx, skip, bound);
        break;
      case ObMySQLDateType:
        ret = eval_date_trunc_vector_template<true, ObMySQLDateType>(expr, ctx, skip, bound);
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unsupported datetime type", K(datetime_type));
    }
  } else {
    // time_unit is variable, use general template
    switch (datetime_type) {
      case ObDateTimeType:
        ret = eval_date_trunc_vector_template<false, ObDateTimeType>(expr, ctx, skip, bound);
        break;
      case ObMySQLDateTimeType:
        ret = eval_date_trunc_vector_template<false, ObMySQLDateTimeType>(expr, ctx, skip, bound);
        break;
      case ObDateType:
        ret = eval_date_trunc_vector_template<false, ObDateType>(expr, ctx, skip, bound);
        break;
      case ObMySQLDateType:
        ret = eval_date_trunc_vector_template<false, ObMySQLDateType>(expr, ctx, skip, bound);
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unsupported datetime type", K(datetime_type));
    }
  }

  return ret;
}

// Template function implementation
template <bool IS_TIME_UNIT_CONST, ObObjType DATETIME_TYPE>
int ObExprDateTrunc::eval_date_trunc_vector_template(const ObExpr &expr, ObEvalCtx &ctx,
                                                   const ObBitVector &skip, const EvalBound &bound)
{
  int ret = OB_SUCCESS;

  if (IS_TIME_UNIT_CONST) {
    // time_unit is constant, get constant value and call constant template
    ObDatum *time_unit_datum = NULL;
    if (OB_FAIL(expr.args_[0]->eval(ctx, time_unit_datum))) {
      LOG_WARN("fail to eval time_unit expr", K(ret));
    } else if (time_unit_datum->is_null()) {
      // If time_unit is NULL, all results should be NULL
      ObIVector *res_vec = expr.get_vector(ctx);
      ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
      for (int64_t i = bound.start(); OB_SUCC(ret) && i < bound.end(); ++i) {
        if (skip.at(i)) {
          continue;
        }
        res_vec->set_null(i);
      }
    } else {
      ObString time_unit = time_unit_datum->get_string();
      ret = eval_date_trunc_vector_const_unit_dispatch<DATETIME_TYPE>(expr, ctx, skip, bound, time_unit);
    }
  } else {
    // time_unit is variable, call variable template
    ret = eval_date_trunc_vector_var_unit<DATETIME_TYPE>(expr, ctx, skip, bound);
  }

  return ret;
}

// Dispatch function: call corresponding template specialization based on time_unit string value
template <ObObjType DATETIME_TYPE>
int ObExprDateTrunc::eval_date_trunc_vector_const_unit_dispatch(const ObExpr &expr, ObEvalCtx &ctx,
                                                        const ObBitVector &skip, const EvalBound &bound,
                                                        const ObString &time_unit)
{
  int ret = OB_SUCCESS;
  DateTruncTimeUnit unit = parse_time_unit(time_unit);

  if (unit == DateTruncTimeUnit::INVALID) {
    ret = OB_INVALID_DATE_TRUNC_FORMAT;
    LOG_WARN("invalid time unit", K(ret), K(time_unit));
    LOG_USER_ERROR(OB_INVALID_DATE_TRUNC_FORMAT, time_unit.length(), time_unit.ptr());
  } else if (OB_FAIL(expr.args_[1]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("fail to eval datetime vector", K(ret));
  } else {
    ret = eval_date_trunc_vector_const_unit_dispatch_l1<DATETIME_TYPE>(expr, ctx, skip, bound, unit);
  }
  return ret;
}

// L1 dispatch function: dispatch based on DATETIME_TYPE, datetime_format, res_format
template <ObObjType DATETIME_TYPE>
int ObExprDateTrunc::eval_date_trunc_vector_const_unit_dispatch_l1(const ObExpr &expr, ObEvalCtx &ctx,
                                                           const ObBitVector &skip, const EvalBound &bound,
                                                           DateTruncTimeUnit unit)
{
  int ret = OB_SUCCESS;

  VectorFormat datetime_format = expr.args_[1]->get_format(ctx);
  VectorFormat res_format = expr.get_format(ctx);
  VecValueTypeClass datetime_vec_tc = expr.args_[1]->get_vec_value_tc();
  VecValueTypeClass res_vec_tc = expr.get_vec_value_tc();

  switch (unit) {
    case DateTruncTimeUnit::MICROSECONDS:
      ret = eval_date_trunc_vector_const_unit_dispatch_l2<DATETIME_TYPE, DateTruncTimeUnit::MICROSECONDS>(
          expr, ctx, skip, bound, datetime_format, res_format, datetime_vec_tc, res_vec_tc);
      break;
    case DateTruncTimeUnit::MILLISECONDS:
      ret = eval_date_trunc_vector_const_unit_dispatch_l2<DATETIME_TYPE, DateTruncTimeUnit::MILLISECONDS>(
          expr, ctx, skip, bound, datetime_format, res_format, datetime_vec_tc, res_vec_tc);
      break;
    case DateTruncTimeUnit::SECOND:
      ret = eval_date_trunc_vector_const_unit_dispatch_l2<DATETIME_TYPE, DateTruncTimeUnit::SECOND>(
          expr, ctx, skip, bound, datetime_format, res_format, datetime_vec_tc, res_vec_tc);
      break;
    case DateTruncTimeUnit::MINUTE:
      ret = eval_date_trunc_vector_const_unit_dispatch_l2<DATETIME_TYPE, DateTruncTimeUnit::MINUTE>(
          expr, ctx, skip, bound, datetime_format, res_format, datetime_vec_tc, res_vec_tc);
      break;
    case DateTruncTimeUnit::HOUR:
      ret = eval_date_trunc_vector_const_unit_dispatch_l2<DATETIME_TYPE, DateTruncTimeUnit::HOUR>(
          expr, ctx, skip, bound, datetime_format, res_format, datetime_vec_tc, res_vec_tc);
      break;
    case DateTruncTimeUnit::DAY:
      ret = eval_date_trunc_vector_const_unit_dispatch_l2<DATETIME_TYPE, DateTruncTimeUnit::DAY>(
          expr, ctx, skip, bound, datetime_format, res_format, datetime_vec_tc, res_vec_tc);
      break;
    case DateTruncTimeUnit::WEEK:
      ret = eval_date_trunc_vector_const_unit_dispatch_l2<DATETIME_TYPE, DateTruncTimeUnit::WEEK>(
          expr, ctx, skip, bound, datetime_format, res_format, datetime_vec_tc, res_vec_tc);
      break;
    case DateTruncTimeUnit::MONTH:
      ret = eval_date_trunc_vector_const_unit_dispatch_l2<DATETIME_TYPE, DateTruncTimeUnit::MONTH>(
          expr, ctx, skip, bound, datetime_format, res_format, datetime_vec_tc, res_vec_tc);
      break;
    case DateTruncTimeUnit::QUARTER:
      ret = eval_date_trunc_vector_const_unit_dispatch_l2<DATETIME_TYPE, DateTruncTimeUnit::QUARTER>(
          expr, ctx, skip, bound, datetime_format, res_format, datetime_vec_tc, res_vec_tc);
      break;
    case DateTruncTimeUnit::YEAR:
      ret = eval_date_trunc_vector_const_unit_dispatch_l2<DATETIME_TYPE, DateTruncTimeUnit::YEAR>(
          expr, ctx, skip, bound, datetime_format, res_format, datetime_vec_tc, res_vec_tc);
      break;
    case DateTruncTimeUnit::DECADE:
      ret = eval_date_trunc_vector_const_unit_dispatch_l2<DATETIME_TYPE, DateTruncTimeUnit::DECADE>(
          expr, ctx, skip, bound, datetime_format, res_format, datetime_vec_tc, res_vec_tc);
      break;
    case DateTruncTimeUnit::CENTURY:
      ret = eval_date_trunc_vector_const_unit_dispatch_l2<DATETIME_TYPE, DateTruncTimeUnit::CENTURY>(
          expr, ctx, skip, bound, datetime_format, res_format, datetime_vec_tc, res_vec_tc);
      break;
    case DateTruncTimeUnit::MILLENNIUM:
      ret = eval_date_trunc_vector_const_unit_dispatch_l2<DATETIME_TYPE, DateTruncTimeUnit::MILLENNIUM>(
          expr, ctx, skip, bound, datetime_format, res_format, datetime_vec_tc, res_vec_tc);
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected time unit", K(ret), K(unit));
      break;
  }

  return ret;
}

template <ObObjType DATETIME_TYPE, DateTruncTimeUnit TIME_UNIT>
int ObExprDateTrunc::eval_date_trunc_vector_const_unit_dispatch_l2(
    const ObExpr &expr, ObEvalCtx &ctx,
    const ObBitVector &skip, const EvalBound &bound,
    VectorFormat datetime_format, VectorFormat res_format,
    VecValueTypeClass datetime_vec_tc, VecValueTypeClass res_vec_tc)
{
  int ret = OB_SUCCESS;

  // Only dispatch when formats are exactly the same, otherwise use VectorBase
  if (datetime_format == res_format) {
    // Formats are the same, use specific vector type for dispatch
    switch (datetime_format) {
      case VEC_FIXED: {
        if (is_fixed_length_vec(datetime_vec_tc) && is_fixed_length_vec(res_vec_tc)
            && datetime_vec_tc == res_vec_tc) {
          switch (datetime_vec_tc) {
            case VEC_TC_DATETIME: {
              ret = eval_date_trunc_vector_const_unit_with_format<DATETIME_TYPE, TIME_UNIT,
                    ObFixedLengthFormat<RTCType<VEC_TC_DATETIME>>,
                    ObFixedLengthFormat<RTCType<VEC_TC_DATETIME>>>(expr, ctx, skip, bound);
              break;
            }
            case VEC_TC_DATE: {
              ret = eval_date_trunc_vector_const_unit_with_format<DATETIME_TYPE, TIME_UNIT,
                    ObFixedLengthFormat<RTCType<VEC_TC_DATE>>,
                    ObFixedLengthFormat<RTCType<VEC_TC_DATE>>>(expr, ctx, skip, bound);
              break;
            }
            case VEC_TC_MYSQL_DATETIME: {
              ret = eval_date_trunc_vector_const_unit_with_format<DATETIME_TYPE, TIME_UNIT,
                    ObFixedLengthFormat<RTCType<VEC_TC_MYSQL_DATETIME>>,
                    ObFixedLengthFormat<RTCType<VEC_TC_MYSQL_DATETIME>>>(expr, ctx, skip, bound);
              break;
            }
            case VEC_TC_MYSQL_DATE: {
              ret = eval_date_trunc_vector_const_unit_with_format<DATETIME_TYPE, TIME_UNIT,
                    ObFixedLengthFormat<RTCType<VEC_TC_MYSQL_DATE>>,
                    ObFixedLengthFormat<RTCType<VEC_TC_MYSQL_DATE>>>(expr, ctx, skip, bound);
              break;
            }
            default: {
              ret = eval_date_trunc_vector_const_unit_with_format<DATETIME_TYPE, TIME_UNIT, ObVectorBase, ObVectorBase>(expr, ctx, skip, bound);
              break;
            }
          }
        } else {
          ret = eval_date_trunc_vector_const_unit_with_format<DATETIME_TYPE, TIME_UNIT, ObVectorBase, ObVectorBase>(expr, ctx, skip, bound);
        }
        break;
      }
      default: {
        ret = eval_date_trunc_vector_const_unit_with_format<DATETIME_TYPE, TIME_UNIT, ObVectorBase, ObVectorBase>(expr, ctx, skip, bound);
        break;
      }
    }
  } else {
    // Formats are different, use VectorBase
    ret = eval_date_trunc_vector_const_unit_with_format<DATETIME_TYPE, TIME_UNIT, ObVectorBase, ObVectorBase>(expr, ctx, skip, bound);
  }

  return ret;
}

// Check for ZERO_DATE in the bound and set corresponding res_vec to NULL
// For ObDateType: check value == ZERO_DATE
// For ObMySQLDateType: check value.date_ == MYSQL_ZERO_DATE
// For ObDateTimeType: check value == ZERO_DATETIME
// For ObMySQLDateTimeType: check value.datetime_ == MYSQL_ZERO_DATETIME
template <ObObjType DATETIME_TYPE, typename ARG_VECTOR, typename RES_VECTOR>
int ObExprDateTrunc::check_zero_date(const ObBitVector &skip,
                                     ObBitVector &eval_flags,
                                     const EvalBound &bound,
                                     ARG_VECTOR *arg_vec,
                                     RES_VECTOR *res_vec)
{
  int ret = OB_SUCCESS;

  for (int64_t i = bound.start(); i < bound.end(); ++i) {
    if (skip.at(i) || eval_flags.at(i)) {
      continue;
    }
    // Check for NULL first
    if (arg_vec->is_null(i)) {
      res_vec->set_null(i);
      eval_flags.set(i);
      continue;
    }
    // Check for ZERO_DATE based on type
    bool is_zero_date = false;
    if constexpr (DATETIME_TYPE == ObDateType) {
      int32_t date_val = arg_vec->get_date(i);
      is_zero_date = (date_val == common::ObTimeConverter::ZERO_DATE);
    } else if constexpr (DATETIME_TYPE == ObMySQLDateType) {
      ObMySQLDate mysql_date = arg_vec->get_mysql_date(i);
      is_zero_date = (mysql_date.date_ == common::ObTimeConverter::MYSQL_ZERO_DATE);
    } else if constexpr (DATETIME_TYPE == ObDateTimeType) {
      int64_t datetime_val = arg_vec->get_datetime(i);
      is_zero_date = (datetime_val == common::ObTimeConverter::ZERO_DATETIME);
    } else if constexpr (DATETIME_TYPE == ObMySQLDateTimeType) {
      ObMySQLDateTime mysql_datetime = arg_vec->get_mysql_datetime(i);
      is_zero_date = (mysql_datetime.datetime_ == common::ObTimeConverter::MYSQL_ZERO_DATETIME);
    }

    if (is_zero_date) {
      res_vec->set_null(i);
      eval_flags.set(i);
    }
  }
  return ret;
}

// Constant time_unit implementation with format template parameters (for same format cases, removes virtual function calls)
template <ObObjType DATETIME_TYPE, DateTruncTimeUnit TIME_UNIT, typename DATETIME_VECTOR, typename RES_VECTOR>
int ObExprDateTrunc::eval_date_trunc_vector_const_unit_with_format(const ObExpr &expr, ObEvalCtx &ctx,
                                                           const ObBitVector &skip, const EvalBound &bound)
{
  int ret = OB_SUCCESS;

  DATETIME_VECTOR *datetime_vec = static_cast<DATETIME_VECTOR *>(expr.args_[1]->get_vector(ctx));
  RES_VECTOR *res_vec = static_cast<RES_VECTOR *>(expr.get_vector(ctx));
  ObSolidifiedVarsGetter helper(expr, ctx, ctx.exec_ctx_.get_my_session());
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  ret = check_zero_date<DATETIME_TYPE, DATETIME_VECTOR, RES_VECTOR>(
    skip, eval_flags, bound, datetime_vec, res_vec);
  // Optimize processing based on time_unit type
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to check zero date", K(ret));
  } else if constexpr (TIME_UNIT == DateTruncTimeUnit::MICROSECONDS) {
    // No truncation at microsecond level, directly copy
    for (int64_t i = bound.start(); OB_SUCC(ret) && i < bound.end(); ++i) {
      if (skip.at(i) || eval_flags.at(i)) {
        continue;
      }
      // Direct copy, no truncation needed
      if constexpr (DATETIME_TYPE == ObDateTimeType) {
        res_vec->set_datetime(i, datetime_vec->get_datetime(i));
      } else if constexpr (DATETIME_TYPE == ObMySQLDateTimeType) {
        res_vec->set_mysql_datetime(i, datetime_vec->get_mysql_datetime(i));
      } else if constexpr (DATETIME_TYPE == ObDateType) {
        res_vec->set_date(i, datetime_vec->get_date(i));
      } else if constexpr (DATETIME_TYPE == ObMySQLDateType) {
        res_vec->set_mysql_date(i, datetime_vec->get_mysql_date(i));
      }
    }
  } else {
    ObTime ob_time;
    ObTimeConvertCtx cvrt_ctx(NULL, false);
    bool no_skip_no_null = bound.get_all_rows_active() && !datetime_vec->has_null();
    if (no_skip_no_null) {
      // Fast path: no need to check skip/eval_flags/null, set all eval_flags at once at the end
      for (int64_t i = bound.start(); OB_SUCC(ret) && i < bound.end(); ++i) {
        if constexpr (DATETIME_TYPE == ObMySQLDateTimeType) {
          if constexpr (TIME_UNIT == DateTruncTimeUnit::WEEK) {
            // WEEK uses ObTime conversion
            ObTime ob_time;
            ObMySQLDateTime result_datetime = datetime_vec->get_mysql_datetime(i);
            if (OB_FAIL(ObTimeConverter::mdatetime_to_ob_time<true>(result_datetime, ob_time))) {
              LOG_WARN("failed to convert mdatetime to ob_time", K(ret), K(i));
            } else if (OB_FAIL(do_date_trunc_template<TIME_UNIT>(ob_time))) {
              LOG_WARN("failed to do date trunc on ob_time", K(ret), K(i));
            } else if (OB_FAIL(ObTimeConverter::ob_time_to_mdatetime(ob_time, result_datetime))) {
              LOG_WARN("failed to convert ob_time to mdatetime", K(ret), K(i));
            } else {
              res_vec->set_mysql_datetime(i, result_datetime);
            }
          } else {
            // Other time units directly operate on ObMySQLDateTime to avoid ObTime conversion
            ObMySQLDateTime result_datetime = datetime_vec->get_mysql_datetime(i);
            if (OB_FAIL(do_date_trunc_mysql_datetime_template<TIME_UNIT>(result_datetime))) {
              LOG_WARN("failed to do date trunc on mysql datetime", K(ret), K(i));
            } else {
              res_vec->set_mysql_datetime(i, result_datetime);
            }
          }
        } else if constexpr (DATETIME_TYPE == ObMySQLDateType) {
          if constexpr (TIME_UNIT == DateTruncTimeUnit::WEEK) {
            // WEEK uses ObTime conversion
            ObTime ob_time;
            ObMySQLDate result_date = datetime_vec->get_mysql_date(i);
            if (OB_FAIL(ObTimeConverter::mdate_to_ob_time<true>(result_date, ob_time))) {
              LOG_WARN("failed to convert mdate to ob_time", K(ret), K(i));
            } else if (OB_FAIL(do_date_trunc_template<TIME_UNIT>(ob_time))) {
              LOG_WARN("failed to do date trunc on ob_time", K(ret), K(i));
            } else {
              result_date = ObTimeConverter::ob_time_to_mdate(ob_time);
              res_vec->set_mysql_date(i, result_date);
            }
          } else {
            // Other time units directly operate on ObMySQLDate to avoid ObTime conversion
            ObMySQLDate result_date = datetime_vec->get_mysql_date(i);
            if (OB_FAIL(do_date_trunc_mysql_date_template<TIME_UNIT>(result_date))) {
              LOG_WARN("failed to do date trunc on mysql date", K(ret), K(i));
            } else {
              res_vec->set_mysql_date(i, result_date);
            }
          }
        } else {
          // Other types still use ObTime conversion
          if (OB_FAIL(process_time_param<DATETIME_TYPE>(datetime_vec->get_payload(i), helper, ob_time))) {
            LOG_WARN("failed to process time", K(ret), K(i));
          } else if (OB_FAIL(do_date_trunc_template<TIME_UNIT>(ob_time))) {
            LOG_WARN("failed to do date trunc", K(ret), K(i));
          } else {
            // Set result
            if constexpr (DATETIME_TYPE == ObDateTimeType) {
              int64_t result_datetime = 0;
              if (OB_FAIL(ObTimeConverter::ob_time_to_datetime(ob_time, cvrt_ctx, result_datetime))) {
                LOG_WARN("fail to convert ob_time to datetime", K(ret), K(ob_time));
              } else {
                res_vec->set_datetime(i, result_datetime);
              }
            } else if constexpr (DATETIME_TYPE == ObDateType) {
              res_vec->set_date(i, ob_time.parts_[DT_DATE]);
            }
          }
        }
      }
    } else {
      // Other time units need truncation processing
      for (int64_t i = bound.start(); OB_SUCC(ret) && i < bound.end(); ++i) {
        if (skip.at(i) || eval_flags.at(i)) {
          continue;
        }
        if constexpr (DATETIME_TYPE == ObMySQLDateTimeType) {
          if constexpr (TIME_UNIT == DateTruncTimeUnit::WEEK) {
            // WEEK uses ObTime conversion
            ObTime ob_time;
            ObMySQLDateTime result_datetime = datetime_vec->get_mysql_datetime(i);
            if (OB_FAIL(ObTimeConverter::mdatetime_to_ob_time<true>(result_datetime, ob_time))) {
              LOG_WARN("failed to convert mdatetime to ob_time", K(ret), K(i));
            } else if (OB_FAIL(do_date_trunc_template<TIME_UNIT>(ob_time))) {
              LOG_WARN("failed to do date trunc on ob_time", K(ret), K(i));
            } else if (OB_FAIL(ObTimeConverter::ob_time_to_mdatetime(ob_time, result_datetime))) {
              LOG_WARN("failed to convert ob_time to mdatetime", K(ret), K(i));
            } else {
              res_vec->set_mysql_datetime(i, result_datetime);
            }
          } else {
            // Other time units directly operate on ObMySQLDateTime to avoid ObTime conversion
            ObMySQLDateTime result_datetime = datetime_vec->get_mysql_datetime(i);
            if (OB_FAIL(do_date_trunc_mysql_datetime_template<TIME_UNIT>(result_datetime))) {
              LOG_WARN("failed to do date trunc on mysql datetime", K(ret), K(i));
            } else {
              res_vec->set_mysql_datetime(i, result_datetime);
            }
          }
        } else if constexpr (DATETIME_TYPE == ObMySQLDateType) {
          if constexpr (TIME_UNIT == DateTruncTimeUnit::WEEK) {
            // WEEK uses ObTime conversion
            ObTime ob_time;
            ObMySQLDate result_date = datetime_vec->get_mysql_date(i);
            if (OB_FAIL(ObTimeConverter::mdate_to_ob_time<true>(result_date, ob_time))) {
              LOG_WARN("failed to convert mdate to ob_time", K(ret), K(i));
            } else if (OB_FAIL(do_date_trunc_template<TIME_UNIT>(ob_time))) {
              LOG_WARN("failed to do date trunc on ob_time", K(ret), K(i));
            } else {
              result_date = ObTimeConverter::ob_time_to_mdate(ob_time);
              res_vec->set_mysql_date(i, result_date);
            }
          } else {
            // Other time units directly operate on ObMySQLDate to avoid ObTime conversion
            ObMySQLDate result_date = datetime_vec->get_mysql_date(i);
            if (OB_FAIL(do_date_trunc_mysql_date_template<TIME_UNIT>(result_date))) {
              LOG_WARN("failed to do date trunc on mysql date", K(ret), K(i));
            } else {
              res_vec->set_mysql_date(i, result_date);
            }
          }
        } else {
          // Other types still use ObTime conversion
          if (OB_FAIL(process_time_param<DATETIME_TYPE>(datetime_vec->get_payload(i), helper, ob_time))) {
            LOG_WARN("failed to process time", K(ret), K(i));
          } else if (OB_FAIL(do_date_trunc_template<TIME_UNIT>(ob_time))) {
            LOG_WARN("failed to do date trunc", K(ret), K(i));
          } else {
            // Set result
            if constexpr (DATETIME_TYPE == ObDateTimeType) {
              int64_t result_datetime = 0;
              if (OB_FAIL(ObTimeConverter::ob_time_to_datetime(ob_time, cvrt_ctx, result_datetime))) {
                LOG_WARN("fail to convert ob_time to datetime", K(ret), K(ob_time));
              } else {
                res_vec->set_datetime(i, result_datetime);
              }
            } else if constexpr (DATETIME_TYPE == ObDateType) {
              res_vec->set_date(i, ob_time.parts_[DT_DATE]);
            }
          }
        }
      }
    }
  }

  return ret;
}

// Template specialization implementation for variable time_unit
template <ObObjType DATETIME_TYPE>
int ObExprDateTrunc::eval_date_trunc_vector_var_unit(const ObExpr &expr, ObEvalCtx &ctx,
                                                   const ObBitVector &skip, const EvalBound &bound)
{
  int ret = OB_SUCCESS;

  // Evaluate all parameters first
  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("fail to eval time_unit vector", K(ret));
  } else if (OB_FAIL(expr.args_[1]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("fail to eval datetime vector", K(ret));
  } else {
    ObIVector *time_unit_vec = expr.args_[0]->get_vector(ctx);
    ObIVector *datetime_vec = expr.args_[1]->get_vector(ctx);
    ObIVector *res_vec = expr.get_vector(ctx);
    ObSolidifiedVarsGetter helper(expr, ctx, ctx.exec_ctx_.get_my_session());
    ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
    ObTimeConvertCtx cvrt_ctx(NULL, false);
    // For variable time_unit, use loop to call single processing function
    for (int64_t i = bound.start(); OB_SUCC(ret) && i < bound.end(); ++i) {
      if (skip.at(i) || eval_flags.at(i)) {
        continue;
      }

      if (time_unit_vec->is_null(i) || datetime_vec->is_null(i)) {
        res_vec->set_null(i);
      } else {
        // Get parameter values
        ObString time_unit = time_unit_vec->get_string(i);

        // Process time parameter
        ObTime ob_time;
        if (OB_FAIL(process_time_param<DATETIME_TYPE>(datetime_vec->get_payload(i), helper, ob_time))) {
          LOG_WARN("failed to process time", K(ret), K(i));
        } else if (OB_FAIL(do_date_trunc_dispatch(time_unit, ob_time))) {
          LOG_WARN("failed to do date trunc", K(ret), K(i));
        } else if (ob_time.parts_[DT_DATE] == common::ObTimeConverter::ZERO_DATE) {
          res_vec->set_null(i);
        } else {
          // Set result
          if (DATETIME_TYPE == ObDateTimeType) {
            int64_t result_datetime = 0;
            if (OB_FAIL(ObTimeConverter::ob_time_to_datetime(ob_time, cvrt_ctx, result_datetime))) {
              LOG_WARN("fail to convert ob_time to datetime", K(ret), K(ob_time));
            } else {
              res_vec->set_datetime(i, result_datetime);
            }
          } else if (DATETIME_TYPE == ObMySQLDateTimeType) {
            ObMySQLDateTime result_datetime;
            if (OB_FAIL(ObTimeConverter::ob_time_to_mdatetime(ob_time, result_datetime))) {
              LOG_WARN("fail to convert ob_time to mdatetime", K(ret), K(ob_time));
            } else {
              res_vec->set_mysql_datetime(i, result_datetime);
            }
          } else if (DATETIME_TYPE == ObDateType) {
            res_vec->set_date(i, ob_time.parts_[DT_DATE]);
          } else if (DATETIME_TYPE == ObMySQLDateType) {
            res_vec->set_mysql_date(i, ObTimeConverter::ob_time_to_mdate(ob_time));
          }
        }
      }
    }
  }

  return ret;
}

} //namespace sql
} //namespace oceanbase
