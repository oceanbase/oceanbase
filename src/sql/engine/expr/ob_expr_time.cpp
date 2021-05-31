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
#include "sql/engine/expr/ob_expr_time.h"
#include "lib/timezone/ob_time_convert.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_datum_cast.h"
#include "sql/engine/expr/ob_expr_day_of_func.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase {
namespace sql {

ObExprTime::ObExprTime(ObIAllocator& alloc) : ObFuncExprOperator(alloc, T_FUN_SYS_TIME, N_TIME, 1, NOT_ROW_DIMENSION)
{}

ObExprTime::~ObExprTime()
{}

int ObExprTime::calc_result_type1(ObExprResType& type, ObExprResType& type1, ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
  // param will be casted to ObTimeType before calculation
  // see ObExprOperator::cast_operand_type for more details if necessary
  type1.set_calc_type(ObTimeType);
  type.set_type(ObTimeType);
  // deduce scale now.
  int16_t scale1 = MIN(type1.get_scale(), MAX_SCALE_FOR_TEMPORAL);
  int16_t scale = (SCALE_UNKNOWN_YET == scale1) ? MAX_SCALE_FOR_TEMPORAL : scale1;
  type.set_scale(scale);
  UNUSED(type_ctx);
  return ret;
}

int ObExprTime::calc_result1(ObObj& result, const ObObj& obj, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(obj.is_null())) {
    result.set_null();
  } else {
    TYPE_CHECK(obj, ObTimeType);
    result = obj;
  }
  UNUSED(expr_ctx);
  return ret;
}

int ObExprTime::cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  if (rt_expr.arg_cnt_ != 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("time expr should have one param", K(ret), K(rt_expr.arg_cnt_));
  } else if (OB_ISNULL(rt_expr.args_) || OB_ISNULL(rt_expr.args_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children of time expr is null", K(ret), K(rt_expr.args_));
  } else {
    rt_expr.eval_func_ = ObExprTime::calc_time;
  }
  return ret;
}

int ObExprTime::calc_time(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum* param_datum = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, param_datum))) {
    LOG_WARN("eval param value failed");
  } else if (OB_UNLIKELY(param_datum->is_null())) {
    expr_datum.set_null();
  } else {
    expr_datum.set_time(param_datum->get_time());
  }
  return ret;
}

ObExprTimeBase::ObExprTimeBase(ObIAllocator& alloc, int32_t date_type, ObExprOperatorType type, const char* name)
    : ObFuncExprOperator(alloc, type, name, 1, NOT_ROW_DIMENSION)
{
  dt_type_ = date_type;
}

ObExprTimeBase::~ObExprTimeBase()
{}

static int ob_expr_convert_to_dt_or_time(
    const ObObj& obj, ObExprCtx& expr_ctx, ObTime& ot, bool with_date, bool is_dayofmonth)
{
  int ret = OB_SUCCESS;
  if (with_date) {
    ObTime ot1;
    if (OB_FAIL(ob_obj_to_ob_time_with_date(obj, get_timezone_info(expr_ctx.my_session_), ot1, is_dayofmonth))) {
      LOG_WARN("convert to obtime failed", K(ret));
    } else {
      ot = ot1;
    }
  } else {
    ObTime ot1(DT_TYPE_TIME);
    if (OB_FAIL(ob_obj_to_ob_time_without_date(obj, get_timezone_info(expr_ctx.my_session_), ot1))) {
      LOG_WARN("convert to obtime failed", K(ret));
    } else {
      ot = ot1;
    }
  }
  return ret;
}

int ObExprTimeBase::calc_result1(ObObj& result, const ObObj& obj, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(obj.is_null())) {
    result.set_null();
  } else {
    ObTime ot;
    bool with_date = false;
    switch (dt_type_) {
      case DT_HOUR:
      case DT_MIN:
      case DT_SEC:
      case DT_USEC:
        with_date = false;
        break;
      case DT_YDAY:
      case DT_WDAY:
      case DT_MDAY:
      case DT_YEAR:
      case DT_MON:
        with_date = true;
        break;
      case DT_MON_NAME:
        with_date = true;
        break;
      default:
        LOG_WARN("ObExprTimeBase calc result1 switch default", K(dt_type_));
    }
    bool ignoreZero = (DT_MDAY == dt_type_ || DT_MON == dt_type_ || DT_MON_NAME == dt_type_);
    if (OB_FAIL(ob_expr_convert_to_dt_or_time(obj, expr_ctx, ot, with_date, ignoreZero))) {
      LOG_WARN("cast to ob time failed", K(ret), K(obj), K(expr_ctx.cast_mode_));
      if (CM_IS_WARN_ON_FAIL(expr_ctx.cast_mode_)) {
        LOG_WARN("cast to ob time failed", K(ret), K(expr_ctx.cast_mode_));
        LOG_USER_WARN(OB_ERR_CAST_VARCHAR_TO_TIME);
        ret = OB_SUCCESS;
        result.set_null();
      }
    } else {
      if (with_date && DT_MDAY != dt_type_ && ot.parts_[DT_DATE] + DAYS_FROM_ZERO_TO_BASE < 0) {
        result.set_null();
      } else if (DT_WDAY == dt_type_) {
        int32_t res = ot.parts_[DT_WDAY];
        res = res % 7 + 1;
        result.set_int32(res);
      } else if (DT_MON_NAME == dt_type_) {
        int32_t mon = ot.parts_[DT_MON];
        if (mon < 1 || mon > 12) {
          LOG_WARN("invalid month value", K(ret), K(mon));
          result.set_null();
        } else {
          const char* month_name = ObExprMonthName::get_month_name(ot.parts_[DT_MON]);
          result.set_string(common::ObVarcharType, month_name, strlen(month_name));
          result.set_collation_type(result_type_.get_collation_type());
          result.set_collation_level(result_type_.get_collation_level());
        }
      } else {
        result.set_int32(ot.parts_[dt_type_]);
      }
    }
  }
  return ret;
}

int ObExprTimeBase::cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  if (rt_expr.arg_cnt_ != 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("hour/minute/second expr should have one param", K(ret), K(rt_expr.arg_cnt_));
  } else if (OB_ISNULL(rt_expr.args_) || OB_ISNULL(rt_expr.args_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children of hour/minute/second expr is null", K(ret), K(rt_expr.args_));
  } else {
    switch (dt_type_) {
      case DT_HOUR:
        rt_expr.eval_func_ = ObExprHour::calc_hour;
        break;
      case DT_MIN:
        rt_expr.eval_func_ = ObExprMinute::calc_minute;
        break;
      case DT_SEC:
        rt_expr.eval_func_ = ObExprSecond::calc_second;
        break;
      case DT_USEC:
        rt_expr.eval_func_ = ObExprMicrosecond::calc_microsecond;
        break;
      case DT_MDAY:
        rt_expr.eval_func_ = ObExprDayOfMonth::calc_dayofmonth;
        break;
      case DT_WDAY:
        rt_expr.eval_func_ = ObExprDayOfWeek::calc_dayofweek;
        break;
      case DT_YDAY:
        rt_expr.eval_func_ = ObExprDayOfYear::calc_dayofyear;
        break;
      case DT_YEAR:
        rt_expr.eval_func_ = ObExprYear::calc_year;
        break;
      case DT_MON:
        rt_expr.eval_func_ = ObExprMonth::calc_month;
        break;
      case DT_MON_NAME:
        rt_expr.eval_func_ = ObExprMonthName::calc_month_name;
        break;
      default:
        LOG_WARN("ObExprTimeBase cg_expr switch default", K(dt_type_));
        break;
    }
  }
  return ret;
}

static int ob_expr_convert_to_time(
    const ObDatum& datum, ObObjType type, bool with_date, bool is_dayofmonth, ObEvalCtx& ctx, ObTime& ot)
{
  int ret = OB_SUCCESS;
  if (with_date) {
    ObTime ot2;
    if (OB_FAIL(ob_datum_to_ob_time_with_date(datum,
            type,
            get_timezone_info(ctx.exec_ctx_.get_my_session()),
            ot2,
            get_cur_time(ctx.exec_ctx_.get_physical_plan_ctx()),
            is_dayofmonth))) {
      LOG_WARN("cast to ob time failed", K(ret));
    } else {
      ot = ot2;
    }
  } else {
    ObTime ot2(DT_TYPE_TIME);
    if (OB_FAIL(
            ob_datum_to_ob_time_without_date(datum, type, get_timezone_info(ctx.exec_ctx_.get_my_session()), ot2))) {
      LOG_WARN("cast to ob time failed", K(ret));
    } else {
      ot = ot2;
    }
  }
  return ret;
}

int ObExprTimeBase::calc(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum, int32_t type, bool with_date,
    bool is_dayofmonth /* false */)
{
  int ret = OB_SUCCESS;
  ObDatum* param_datum = NULL;
  const ObSQLSessionInfo* session = NULL;
  if (OB_ISNULL(session = ctx.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (OB_FAIL(expr.args_[0]->eval(ctx, param_datum))) {
    LOG_WARN("eval param value failed");
  } else if (OB_UNLIKELY(param_datum->is_null())) {
    expr_datum.set_null();
  } else {
    ObTime ot;
    if (OB_FAIL(ob_expr_convert_to_time(
            *param_datum, expr.args_[0]->datum_meta_.type_, with_date, is_dayofmonth, ctx, ot))) {
      LOG_WARN("cast to ob time failed", K(ret), K(lbt()), K(session->get_stmt_type()));
      LOG_USER_WARN(OB_ERR_CAST_VARCHAR_TO_TIME);
      uint64_t cast_mode = 0;
      ObSQLUtils::get_default_cast_mode(session->get_stmt_type(), session, cast_mode);
      if (CM_IS_WARN_ON_FAIL(cast_mode)) {
        ret = OB_SUCCESS;
        expr_datum.set_null();
      }
    } else if (with_date && !is_dayofmonth && ot.parts_[DT_DATE] + DAYS_FROM_ZERO_TO_BASE < 0) {
      expr_datum.set_null();
    } else {
      expr_datum.set_int32(ot.parts_[type]);
    }
  }
  return ret;
}

ObExprHour::ObExprHour(ObIAllocator& alloc) : ObExprTimeBase(alloc, DT_HOUR, T_FUN_SYS_HOUR, N_HOUR){};

ObExprHour::~ObExprHour()
{}

int ObExprHour::calc_hour(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  return ObExprTimeBase::calc(expr, ctx, expr_datum, DT_HOUR, false);
}

ObExprMinute::ObExprMinute(ObIAllocator& alloc) : ObExprTimeBase(alloc, DT_MIN, T_FUN_SYS_MINUTE, N_MINUTE){};

ObExprMinute::~ObExprMinute()
{}

int ObExprMinute::calc_minute(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  return ObExprTimeBase::calc(expr, ctx, expr_datum, DT_MIN, false);
}

ObExprSecond::ObExprSecond(ObIAllocator& alloc) : ObExprTimeBase(alloc, DT_SEC, T_FUN_SYS_SECOND, N_SECOND){};

ObExprSecond::~ObExprSecond()
{}

int ObExprSecond::calc_second(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  return ObExprTimeBase::calc(expr, ctx, expr_datum, DT_SEC, false);
}

ObExprMicrosecond::ObExprMicrosecond(ObIAllocator& alloc)
    : ObExprTimeBase(alloc, DT_USEC, T_FUN_SYS_MICROSECOND, N_MICROSECOND){};

ObExprMicrosecond::~ObExprMicrosecond()
{}

int ObExprMicrosecond::calc_microsecond(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  return ObExprTimeBase::calc(expr, ctx, expr_datum, DT_USEC, false);
}

ObExprYear::ObExprYear(ObIAllocator& alloc) : ObExprTimeBase(alloc, DT_YEAR, T_FUN_SYS_YEAR, N_YEAR){};

ObExprYear::~ObExprYear()
{}

int ObExprYear::calc_year(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  return ObExprTimeBase::calc(expr, ctx, expr_datum, DT_YEAR, true);
}

ObExprMonth::ObExprMonth(ObIAllocator& alloc) : ObExprTimeBase(alloc, DT_MON, T_FUN_SYS_MONTH, N_MONTH){};

ObExprMonth::~ObExprMonth()
{}

int ObExprMonth::calc_month(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  return ObExprTimeBase::calc(expr, ctx, expr_datum, DT_MON, true, true);
}

ObExprMonthName::ObExprMonthName(ObIAllocator& alloc)
    : ObExprTimeBase(alloc, DT_MON_NAME, T_FUN_SYS_MONTH_NAME, N_MONTH_NAME){};

ObExprMonthName::~ObExprMonthName()
{}

const ObExprMonthName::ObSqlMonthNameMap ObExprMonthName::MONTH_NAME_MAP[] = {{1, "January"},
    {2, "February"},
    {3, "March"},
    {4, "April"},
    {5, "May"},
    {6, "June"},
    {7, "July"},
    {8, "August"},
    {9, "September"},
    {10, "October"},
    {11, "November"},
    {12, "December"}};

OB_INLINE const char* ObExprMonthName::get_month_name(int month)
{
  return MONTH_NAME_MAP[month - 1].str_val;
}

int ObExprMonthName::calc_month_name(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  // NOTE: the last param should be true otherwise '2020-09-00' will not work
  if (OB_FAIL(calc(expr, ctx, expr_datum, DT_MON, true, true))) {
    LOG_WARN("eval month in monthname failed", K(ret), K(expr));
  } else {
    int32_t mon = expr_datum.get_int32();
    if (mon < 1 || mon > 12) {
      LOG_WARN("invalid month value", K(ret), K(expr));
      expr_datum.set_null();
    } else {
      const char* month_name = get_month_name(mon);
      expr_datum.set_string(month_name, strlen(month_name));
    }
  }

  return ret;
}

int ObExprMonthName::calc_result_type1(ObExprResType& type, ObExprResType& type1, common::ObExprTypeCtx& type_ctx) const
{
  type.set_varchar();
  type1.set_calc_type(common::ObVarcharType);

  // get collation type from session and set it in type for passing following
  // collation checking
  int ret = OB_SUCCESS;
  if (OB_ISNULL(type_ctx.get_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else {
    ObCollationType cs_type = type_ctx.get_coll_type();
    type.set_collation_type(cs_type);
    type.set_collation_level(CS_LEVEL_IMPLICIT);
    type1.set_calc_collation_type(cs_type);
  }

  return ret;
}

}  // namespace sql
}  // namespace oceanbase
