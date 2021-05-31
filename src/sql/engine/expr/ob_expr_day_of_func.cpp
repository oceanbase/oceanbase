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

#define USING_LOG_PREFIX SQL_EXE
#include "ob_expr_day_of_func.h"
#include "lib/timezone/ob_time_convert.h"
#include "share/object/ob_obj_cast.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"
#include "share/ob_time_utility2.h"
#include "sql/engine/expr/ob_datum_cast.h"

using namespace oceanbase::share;
using namespace oceanbase::sql;

namespace oceanbase {
namespace sql {

ObExprDayOfMonth::ObExprDayOfMonth(ObIAllocator& alloc)
    : ObExprTimeBase(alloc, DT_MDAY, T_FUN_SYS_DAY_OF_MONTH, N_DAY_OF_MONTH){};

ObExprDayOfMonth::~ObExprDayOfMonth()
{}

int ObExprDayOfMonth::calc_dayofmonth(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  return ObExprTimeBase::calc(expr, ctx, expr_datum, DT_MDAY, true, true);
}

ObExprDayOfWeek::ObExprDayOfWeek(ObIAllocator& alloc)
    : ObExprTimeBase(alloc, DT_WDAY, T_FUN_SYS_DAY_OF_WEEK, N_DAY_OF_WEEK){};

ObExprDayOfWeek::~ObExprDayOfWeek()
{}

int ObExprDayOfWeek::calc_dayofweek(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  if (ObExprTimeBase::calc(expr, ctx, expr_datum, DT_WDAY, true)) {
    LOG_WARN("calc day of week failed", K(ret));
  } else if (!expr_datum.is_null()) {
    expr_datum.set_int32(expr_datum.get_int32() % 7 + 1);
  }
  return ret;
}

ObExprDayOfYear::ObExprDayOfYear(ObIAllocator& alloc)
    : ObExprTimeBase(alloc, DT_YDAY, T_FUN_SYS_DAY_OF_YEAR, N_DAY_OF_YEAR){};

ObExprDayOfYear::~ObExprDayOfYear()
{}

int ObExprDayOfYear::calc_dayofyear(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  return ObExprTimeBase::calc(expr, ctx, expr_datum, DT_YDAY, true);
}

ObExprToSeconds::ObExprToSeconds(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_TO_SECONDS, N_TO_SECONDS, 1, NOT_ROW_DIMENSION){};

ObExprToSeconds::~ObExprToSeconds()
{}

int ObExprToSeconds::calc_result1(ObObj& result, const ObObj& obj, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
  const ObObj* p_obj = NULL;
  EXPR_CAST_OBJ_V2(ObDateType, obj, p_obj);
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(p_obj)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("p_obj is null", K(ret));
    } else if (OB_UNLIKELY(p_obj->is_null() || OB_INVALID_DATE_VALUE == cast_ctx.warning_)) {
      result.set_null();
    } else {
      ObTime ot;
      if (OB_FAIL(ob_obj_to_ob_time_with_date(obj, get_timezone_info(expr_ctx.my_session_), ot))) {
        LOG_WARN("cast to ob time failed", K(ret), K(obj), K(expr_ctx.cast_mode_));
        if (CM_IS_WARN_ON_FAIL(expr_ctx.cast_mode_)) {
          ret = OB_SUCCESS;
          result.set_null();
        } else { /* do nothing */
        }
      } else {
        int64_t days = p_obj->get_date() + DAYS_FROM_ZERO_TO_BASE;
        int64_t seconds =
            days * 60 * 60 * 24L + ot.parts_[DT_HOUR] * 60 * 60L + ot.parts_[DT_MIN] * 60L + ot.parts_[DT_SEC];
        if (seconds < 0) {
          result.set_null();
        } else {
          result.set_int(seconds);
        }
      }
    }
  } else {
    LOG_WARN("cast obj failed", K(ret));
  }
  return ret;
}

int ObExprToSeconds::cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  if (rt_expr.arg_cnt_ != 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("to_seconds expr should have one param", K(ret), K(rt_expr.arg_cnt_));
  } else if (OB_ISNULL(rt_expr.args_) || OB_ISNULL(rt_expr.args_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children of to_seconds expr is null", K(ret), K(rt_expr.args_));
  } else {
    rt_expr.eval_func_ = ObExprToSeconds::calc_toseconds;
  }
  return ret;
}

int ObExprToSeconds::calc_toseconds(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
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
    if (OB_FAIL(ob_datum_to_ob_time_with_date(*param_datum,
            expr.args_[0]->datum_meta_.type_,
            get_timezone_info(session),
            ot,
            get_cur_time(ctx.exec_ctx_.get_physical_plan_ctx())))) {
      LOG_WARN("cast to ob time failed", K(ret));
      uint64_t cast_mode = 0;
      ObSQLUtils::get_default_cast_mode(session->get_stmt_type(), session, cast_mode);
      if (CM_IS_WARN_ON_FAIL(cast_mode)) {
        ret = OB_SUCCESS;
        expr_datum.set_null();
      } else { /* do nothing */
      }
    } else {
      int64_t days = ObTimeConverter::ob_time_to_date(ot) + DAYS_FROM_ZERO_TO_BASE;
      int64_t seconds =
          days * 60 * 60 * 24L + ot.parts_[DT_HOUR] * 60 * 60L + ot.parts_[DT_MIN] * 60L + ot.parts_[DT_SEC];
      if (seconds < 0) {
        expr_datum.set_null();
      } else {
        expr_datum.set_int(seconds);
      }
    }
  }
  return ret;
}

ObExprSecToTime::ObExprSecToTime(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_SEC_TO_TIME, N_SEC_TO_TIME, 1, NOT_ROW_DIMENSION){};

ObExprSecToTime::~ObExprSecToTime()
{}

int ObExprSecToTime::calc_result1(ObObj& result, const ObObj& sec, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(sec.is_null())) {
    result.set_null();
  } else if (OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("calc buf is NULL", K(ret));
  } else {
    TYPE_CHECK(sec, ObNumberType);
    EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
    number::ObNumber num_usec;
    number::ObNumber num_sec;
    number::ObNumber million;
    int64_t int_usec = 0;
    if (OB_FAIL(num_sec.from(sec.get_number(), *expr_ctx.calc_buf_))) {
      LOG_WARN("failed to create number", K(ret), K(sec.get_number()));
    } else if (OB_FAIL(million.from(static_cast<int64_t>(1000000), *expr_ctx.calc_buf_))) {
      LOG_WARN("failed to create number million", K(ret));
    } else if (OB_FAIL(num_sec.mul(million, num_usec, *expr_ctx.calc_buf_))) {
      LOG_WARN("failed to mul number", K(ret), K(num_sec), K(million));
    } else if (OB_FAIL(num_usec.round(0))) {
      LOG_WARN("failed to round number", K(ret), K(num_usec));
    } else if (!num_usec.is_valid_int64(int_usec)) {
      int_usec = num_usec.is_negative() ? -INT64_MAX : INT64_MAX;
    }
    if (OB_SUCC(ret)) {
      if (num_usec.is_negative()) {
        int_usec = -int_usec;
      }
      if (OB_FAIL(ObTimeConverter::time_overflow_trunc(int_usec))) {
        if (CM_IS_WARN_ON_FAIL(cast_ctx.cast_mode_)) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("time value is out of range", K(ret), K(int_usec));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (num_usec.is_negative()) {
        int_usec = -int_usec;
      }
      result.set_time(int_usec);
    }
  }
  return ret;
}

int ObExprSecToTime::cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  if (rt_expr.arg_cnt_ != 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sectotime expr should have one param", K(ret), K(rt_expr.arg_cnt_));
  } else if (OB_ISNULL(rt_expr.args_) || OB_ISNULL(rt_expr.args_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children of sectotime expr is null", K(ret), K(rt_expr.args_));
  } else {
    CK(ObNumberType == rt_expr.args_[0]->datum_meta_.type_);
    rt_expr.eval_func_ = ObExprSecToTime::calc_sectotime;
  }
  return ret;
}

int ObExprSecToTime::calc_sectotime(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
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
    number::ObNumber num_usec;
    number::ObNumber num_sec;
    number::ObNumber million;
    int64_t int_usec = 0;
    char local_buff[number::ObNumber::MAX_BYTE_LEN * 3];
    ObDataBuffer local_alloc(local_buff, number::ObNumber::MAX_BYTE_LEN * 3);
    if (OB_FAIL(num_sec.from(param_datum->get_number(), local_alloc))) {
      LOG_WARN("failed to create number", K(ret), K(number::ObNumber(param_datum->get_number())));
    } else if (OB_FAIL(million.from(static_cast<int64_t>(1000000), local_alloc))) {
      LOG_WARN("failed to create number million", K(ret));
    } else if (OB_FAIL(num_sec.mul(million, num_usec, local_alloc))) {
      LOG_WARN("failed to mul number", K(ret), K(num_sec), K(million));
    } else if (OB_FAIL(num_usec.round(0))) {
      LOG_WARN("failed to round number", K(ret), K(num_usec));
    } else if (!num_usec.is_valid_int64(int_usec)) {
      int_usec = num_usec.is_negative() ? -INT64_MAX : INT64_MAX;
    }
    if (OB_SUCC(ret)) {
      if (num_usec.is_negative()) {
        int_usec = -int_usec;
      }
      if (OB_FAIL(ObTimeConverter::time_overflow_trunc(int_usec))) {
        uint64_t cast_mode = 0;
        ObSQLUtils::get_default_cast_mode(session->get_stmt_type(), session, cast_mode);
        if (CM_IS_WARN_ON_FAIL(cast_mode)) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("time value is out of range", K(ret), K(int_usec));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (num_usec.is_negative()) {
        int_usec = -int_usec;
      }
      expr_datum.set_time(int_usec);
    }
  }
  return ret;
}

ObExprTimeToSec::ObExprTimeToSec(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_TIME_TO_SEC, N_TIME_TO_SEC, 1, NOT_ROW_DIMENSION){};
ObExprTimeToSec::~ObExprTimeToSec()
{}

int ObExprTimeToSec::calc_result1(ObObj& result, const ObObj& obj, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(obj.is_null())) {
    result.set_null();
  } else {
    TYPE_CHECK(obj, ObTimeType);
    int64_t t_val = obj.get_time();
    result.set_int(USEC_TO_SEC(t_val));
  }
  UNUSED(expr_ctx);
  return ret;
}

int ObExprTimeToSec::cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  if (rt_expr.arg_cnt_ != 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("timetosec expr should have one param", K(ret), K(rt_expr.arg_cnt_));
  } else if (OB_ISNULL(rt_expr.args_) || OB_ISNULL(rt_expr.args_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children of timetosec expr is null", K(ret), K(rt_expr.args_));
  } else {
    CK(ObTimeType == rt_expr.args_[0]->datum_meta_.type_);
    rt_expr.eval_func_ = ObExprTimeToSec::calc_timetosec;
  }
  return ret;
}

int ObExprTimeToSec::calc_timetosec(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
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
    int64_t t_val = param_datum->get_time();
    expr_datum.set_int(USEC_TO_SEC(t_val));
  }
  return ret;
}

ObExprSubtime::ObExprSubtime(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_SUBTIME, N_SUB_TIME, 2, NOT_ROW_DIMENSION)
{}
int ObExprSubtime::calc_result_type2(
    ObExprResType& type, ObExprResType& date_arg, ObExprResType& time_arg, ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
  ObScale scale1 = date_arg.get_scale();
  ObScale scale2 = time_arg.get_scale();
  if (OB_UNLIKELY(ObNullTC == date_arg.get_type_class())) {
    type.set_datetime();
    type.set_scale(MAX_SCALE_FOR_TEMPORAL);
    type.set_precision(static_cast<ObPrecision>(DATETIME_MIN_LENGTH + MAX_SCALE_FOR_TEMPORAL));
  } else if (ObTimeTC == date_arg.get_type_class()) {
    type.set_time();
    type.set_scale(MAX(scale1, scale2));
    type.set_precision(static_cast<ObPrecision>(TIME_MIN_LENGTH + type.get_scale()));
  } else if (ObDateTimeType == date_arg.get_type() || ObTimestampType == date_arg.get_type()) {
    type.set_datetime();
    type.set_scale(MAX(scale1, scale2));
    type.set_precision(static_cast<ObPrecision>(DATETIME_MIN_LENGTH + type.get_scale()));
  } else {
    type.set_varchar();
    type.set_length(DATETIME_MAX_LENGTH);
    type.set_scale(-1);
    ret = aggregate_charsets_for_string_result(type, &date_arg, 1, type_ctx.get_coll_type());
  }
  if (OB_SUCC(ret)) {
    if (ob_is_enumset_tc(date_arg.get_type())) {
      date_arg.set_calc_type(ObVarcharType);
    }
    if (ob_is_enumset_tc(time_arg.get_type())) {
      time_arg.set_calc_type(ObVarcharType);
    } else {
      type_ctx.set_cast_mode(type_ctx.get_cast_mode() | CM_NULL_ON_WARN);
      time_arg.set_calc_type(ObTimeType);
    }
  }
  return ret;
}

int ObExprSubtime::calc_result2(common::ObObj& result, const common::ObObj& date_arg, const common::ObObj& time_arg,
    common::ObExprCtx& expr_ctx) const
{
  ObTime ot;
  ObTime ot2(DT_TYPE_TIME);
  int ret = OB_SUCCESS;
  int ret1 = OB_SUCCESS;
  int ret2 = OB_SUCCESS;
  int64_t t_val1 = 0;
  int64_t t_val2 = 0;
  bool is_string_with_time_fmt = false;
  EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
  if (date_arg.get_type() == ObVarcharType) {
    ret1 = ob_obj_to_ob_time_with_date(date_arg, get_timezone_info(expr_ctx.my_session_), ot);
    if (OB_SUCCESS != ret1) {
      ret2 = ob_obj_to_ob_time_without_date(date_arg, get_timezone_info(expr_ctx.my_session_), ot2);
      is_string_with_time_fmt = true;
    } else if (0 == ot.parts_[DT_YEAR] && 0 == ot.parts_[DT_MON] && 0 == ot.parts_[DT_MDAY]) {
      is_string_with_time_fmt = true;
    }
  } else { /* do nothing */
  }

  if (OB_SUCCESS != ret1 && OB_SUCCESS != ret2) {
    result.set_null();
  } else if (OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is null", K(ret));
  } else if (OB_UNLIKELY(date_arg.is_null() || time_arg.is_null())) {
    result.set_null();
  } else {
    if (ObVarcharType == time_arg.get_type()) {
      EXPR_GET_TIME_V2(time_arg, t_val2);
    } else {
      TYPE_CHECK(time_arg, ObTimeType);
      t_val2 = time_arg.get_time();
    }

    if (ObTimeType == date_arg.get_type() || is_string_with_time_fmt) {  // a#, subtime(t1, t2)
      if (ObVarcharType == date_arg.get_type()) {
        EXPR_GET_TIME_V2(date_arg, t_val1);
      } else {
        TYPE_CHECK(date_arg, ObTimeType);
        t_val1 = date_arg.get_time();
      }
      if (OB_SUCC(ret)) {
        int64_t int_usec = t_val1 - t_val2;
        // LOG_WARN("@songxin, ObExprSubtime#time-time", K(t_val1), K(t_val2), K(int_usec));
        if (OB_FAIL(ObTimeConverter::time_overflow_trunc(int_usec))) {
          if (CM_IS_WARN_ON_FAIL(cast_ctx.cast_mode_)) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("time value is out of range", K(ret), K(int_usec));
          }
        } else {
          result.set_time(int_usec);
        }
      }
    } else {  // b#, subtime(t1, t2)
      if (ObVarcharType == date_arg.get_type()) {
        EXPR_GET_DATETIME_V2(date_arg, t_val1);
      } else {
        TYPE_CHECK(date_arg, ObDateTimeType);
        t_val1 = date_arg.get_datetime();
      }
      if (OB_SUCC(ret)) {
        int64_t int_usec = t_val1 - t_val2;
        // LOG_WARN("@songxin, ObExprSubtime#datetime-time", K(t_val1), K(t_val2), K(int_usec));
        if (ObTimeConverter::is_valid_datetime(int_usec)) {
          result.set_datetime(int_usec);
        } else {
          result.set_null();
        }
      }
    }
    if (OB_SUCC(ret) && date_arg.get_type() == ObVarcharType) {
      if (OB_FAIL(ObObjCaster::to_type(ObVarcharType, cast_ctx, result, result))) {
        LOG_WARN("failed to cast object to ObVarcharType ", K(result), K(ret));
      }
    }
  }
  if (OB_FAIL(ret)) {
    result.set_null();
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObExprSubtime::cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  if (rt_expr.arg_cnt_ != 2) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("subtime expr should have two params", K(ret), K(rt_expr.arg_cnt_));
  } else if (OB_ISNULL(rt_expr.args_) || OB_ISNULL(rt_expr.args_[0]) || OB_ISNULL(rt_expr.args_[1])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children of subtime expr is null", K(ret), K(rt_expr.args_));
  } else {
    const ObObjType result_type = rt_expr.datum_meta_.type_;
    const ObObjType param1_type = rt_expr.args_[0]->datum_meta_.type_;
    const ObObjType param2_type = rt_expr.args_[1]->datum_meta_.type_;
    if (ObTimeType != param2_type && ObVarcharType != param2_type) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("second param of subtime expr is not time type", K(ret), K(param2_type));
    } else {
      switch (result_type) {
        case ObDateTimeType:
          if (ObNullType != param1_type && ObDateTimeType != param1_type && ObTimestampType != param1_type) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid type of first argument", K(ret), K(result_type), K(param1_type));
          } else {
            rt_expr.eval_func_ = ObExprSubtime::subtime_datetime;
          }
          break;
        case ObTimeType:
          if (ObTimeType != param1_type) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid type of first argument", K(ret), K(result_type), K(param1_type));
          } else {
            rt_expr.eval_func_ = ObExprSubtime::subtime_datetime;
          }
          break;
        case ObVarcharType:
          rt_expr.eval_func_ = ObExprSubtime::subtime_varchar;
          break;
        default:
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid expr type", K(rt_expr.datum_meta_.type_));
      }
    }
  }
  return ret;
}

int ObExprSubtime::subtime_common(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum, bool& null_res,
    ObDatum*& date_arg, ObDatum*& time_arg, int64_t& time_val)
{
  int ret = OB_SUCCESS;
  null_res = false;
  const ObSQLSessionInfo* session = NULL;
  if (OB_ISNULL(session = ctx.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (OB_FAIL(expr.args_[0]->eval(ctx, date_arg))) {
    LOG_WARN("eval the first param value failed");
  } else if (OB_FAIL(expr.args_[1]->eval(ctx, time_arg))) {
    LOG_WARN("eval the second param value failed");
  } else if (OB_UNLIKELY(date_arg->is_null() || time_arg->is_null())) {
    expr_datum.set_null();
    null_res = true;
  } else {
    ObTime ot2(DT_TYPE_TIME);
    if (ObTimeType == expr.args_[1]->datum_meta_.type_) {
      time_val = time_arg->get_time();
    } else if (OB_FAIL(ob_datum_to_ob_time_without_date(*time_arg,
                   expr.args_[1]->datum_meta_.type_,
                   get_timezone_info(ctx.exec_ctx_.get_my_session()),
                   ot2))) {
      LOG_WARN("cast the second param failed", K(ret));
      expr_datum.set_null();
      null_res = true;
      uint64_t cast_mode = 0;
      ObSQLUtils::get_default_cast_mode(session->get_stmt_type(), session, cast_mode);
      if (CM_IS_WARN_ON_FAIL(cast_mode)) {
        ret = OB_SUCCESS;
      }
    } else {
      time_val = ObTimeConverter::ob_time_to_time(ot2);
    }
  }
  return ret;
}

int ObExprSubtime::subtime_datetime(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum* date_arg = NULL;
  ObDatum* time_arg = NULL;
  int64_t t_val1 = 0;
  int64_t t_val2 = 0;
  bool null_res = false;
  const ObSQLSessionInfo* session = ctx.exec_ctx_.get_my_session();
  const ObTimeZoneInfo* tz_info = NULL;
  if (OB_FAIL(subtime_common(expr, ctx, expr_datum, null_res, date_arg, time_arg, t_val2))) {
    LOG_WARN("calc subtime failed", K(ret));
  } else if (OB_ISNULL(tz_info = get_timezone_info(session))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("timezone info of session is null", K(ret));
  } else if (!null_res) {
    int64_t offset = ObTimestampType == expr.args_[0]->datum_meta_.type_ ? tz_info->get_offset() : 0;
    t_val1 = date_arg->get_datetime() + offset * USECS_PER_SEC;
    int64_t int_usec = t_val1 - t_val2;
    if (ObTimeConverter::is_valid_datetime(int_usec)) {
      expr_datum.set_datetime(int_usec);
    } else {
      expr_datum.set_null();
    }
  }
  return ret;
}

int ObExprSubtime::subtime_varchar(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum* date_arg = NULL;
  ObDatum* time_arg = NULL;
  int64_t t_val1 = 0;
  int64_t t_val2 = 0;
  bool null_res = false;
  const ObSQLSessionInfo* session = ctx.exec_ctx_.get_my_session();
  const ObTimeZoneInfo* tz_info = NULL;
  if (OB_FAIL(subtime_common(expr, ctx, expr_datum, null_res, date_arg, time_arg, t_val2))) {
    LOG_WARN("calc subtime failed", K(ret));
  } else if (OB_ISNULL(tz_info = get_timezone_info(session))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("timezone info of session is null", K(ret));
  } else if (!null_res) {
    ObTime ot1(DT_TYPE_TIME);
    if (OB_FAIL(ob_datum_to_ob_time_without_date(*date_arg, expr.args_[0]->datum_meta_.type_, tz_info, ot1))) {
      LOG_WARN("cast the first param failed", K(ret));
      expr_datum.set_null();
    } else {
      bool param_with_date = true;
      if (0 == ot1.parts_[DT_YEAR] && 0 == ot1.parts_[DT_MON] && 0 == ot1.parts_[DT_MDAY]) {
        param_with_date = false;
      }

      if (param_with_date) {
        ObTimeConvertCtx cvrt_ctx(tz_info, false);
        if (OB_FAIL(ObTimeConverter::ob_time_to_datetime(ot1, cvrt_ctx, t_val1))) {
          LOG_WARN("ob_time_to_datetime failed", K(ret));
        }
      } else {
        t_val1 = ObTimeConverter::ob_time_to_time(ot1);
      }

      if (OB_SUCC(ret)) {
        int64_t int_usec = t_val1 - t_val2;
        const int64_t datetime_buf_len = DATETIME_MAX_LENGTH + 1;
        char* buf = expr.get_str_res_mem(ctx, datetime_buf_len);
        int64_t pos = 0;
        // compatiable with mysql. display last 6 digits if we have mill
        ObString format;
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate memory failed", K(ret));
        } else if (param_with_date && OB_FAIL(ObTimeConverter::datetime_to_str(
                                          int_usec, NULL, format, -1, buf, datetime_buf_len, pos, true))) {
          LOG_WARN("datetime to str failed", K(ret));
        } else if (!param_with_date &&
                   OB_FAIL(ObTimeConverter::time_to_str(int_usec, -1, buf, datetime_buf_len, pos, true))) {
          LOG_WARN("time to str failed", K(ret));
        } else {
          expr_datum.ptr_ = buf;
          expr_datum.pack_ = static_cast<uint32_t>(pos);
        }
      }
    }
    uint64_t cast_mode = 0;
    ObSQLUtils::get_default_cast_mode(session->get_stmt_type(), session, cast_mode);
    if (CM_IS_WARN_ON_FAIL(cast_mode) && OB_ALLOCATE_MEMORY_FAILED != ret) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
