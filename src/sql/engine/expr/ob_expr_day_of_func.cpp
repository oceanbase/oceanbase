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
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_datum_cast.h"
#include "lib/locale/ob_locale_type.h"

using namespace oceanbase::share;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{

ObExprDayOfMonth::ObExprDayOfMonth(ObIAllocator &alloc)
    : ObExprTimeBase(alloc, DT_MDAY, T_FUN_SYS_DAY_OF_MONTH, N_DAY_OF_MONTH, VALID_FOR_GENERATED_COL) {};

ObExprDayOfMonth::~ObExprDayOfMonth() {}

int ObExprDayOfMonth::calc_dayofmonth(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  return ObExprTimeBase::calc(expr, ctx, expr_datum, DT_MDAY, true, true);
}

ObExprDay::ObExprDay(ObIAllocator &alloc)
    : ObExprTimeBase(alloc, DT_MDAY, T_FUN_SYS_DAY, N_DAY, VALID_FOR_GENERATED_COL) {};

ObExprDay::~ObExprDay() {}

ObExprDayOfWeek::ObExprDayOfWeek(ObIAllocator &alloc)
    : ObExprTimeBase(alloc, DT_WDAY, T_FUN_SYS_DAY_OF_WEEK, N_DAY_OF_WEEK, VALID_FOR_GENERATED_COL) {};

ObExprDayOfWeek::~ObExprDayOfWeek() {}

int ObExprDayOfWeek::calc_dayofweek(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObExprTimeBase::calc(expr, ctx, expr_datum, DT_WDAY, true))) {
    LOG_WARN("calc day of week failed", K(ret));
  } else if (!expr_datum.is_null()) {
    expr_datum.set_int32(expr_datum.get_int32() % 7 + 1);
  }
  return ret;
}

ObExprDayOfYear::ObExprDayOfYear(ObIAllocator &alloc)
    : ObExprTimeBase(alloc, DT_YDAY, T_FUN_SYS_DAY_OF_YEAR, N_DAY_OF_YEAR, VALID_FOR_GENERATED_COL) {};

ObExprDayOfYear::~ObExprDayOfYear() { }

int ObExprDayOfYear::calc_dayofyear(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  return ObExprTimeBase::calc(expr, ctx, expr_datum, DT_YDAY, true);
}

ObExprToSeconds::ObExprToSeconds(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_TO_SECONDS, N_TO_SECONDS, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION) {};

ObExprToSeconds::~ObExprToSeconds() {}

int ObExprToSeconds::cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const
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

int ObExprToSeconds::calc_toseconds(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *param_datum = NULL;
  const ObSQLSessionInfo *session = NULL;
  ObSolidifiedVarsGetter helper(expr, ctx, ctx.exec_ctx_.get_my_session());
  ObSQLMode sql_mode = 0;
  const common::ObTimeZoneInfo *tz_info = NULL;
  if (OB_ISNULL(session = ctx.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (OB_FAIL(expr.args_[0]->eval(ctx, param_datum))) {
    LOG_WARN("eval param value failed");
  } else if (OB_UNLIKELY(param_datum->is_null())) {
    expr_datum.set_null();
  } else if (OB_FAIL(helper.get_sql_mode(sql_mode))) {
    LOG_WARN("get sql mode failed", K(ret));
  } else if (OB_FAIL(helper.get_time_zone_info(tz_info))) {
    LOG_WARN("get tz info failed", K(ret));
  } else {
    ObTime ot;
    ObDateSqlMode date_sql_mode;
    date_sql_mode.init(sql_mode);
    if (OB_FAIL(ob_datum_to_ob_time_with_date(*param_datum, expr.args_[0]->datum_meta_.type_,
                                              expr.args_[0]->datum_meta_.scale_,
                                              tz_info, ot,
                                              get_cur_time(ctx.exec_ctx_.get_physical_plan_ctx()),
                                              date_sql_mode,
                                              expr.args_[0]->obj_meta_.has_lob_header()))) {
      LOG_WARN("cast to ob time failed", K(ret));
      uint64_t cast_mode = 0;
      ObSQLUtils::get_default_cast_mode(session->get_stmt_type(),
                                        session->is_ignore_stmt(),
                                        sql_mode, cast_mode);
      if (CM_IS_WARN_ON_FAIL(cast_mode)) {
        ret = OB_SUCCESS;
        expr_datum.set_null();
      } else { /* do nothing */ }
    } else {
      int64_t days = ObTimeConverter::ob_time_to_date(ot) + DAYS_FROM_ZERO_TO_BASE;
      int64_t seconds = days * 60 * 60 * 24L + ot.parts_[DT_HOUR] * 60 * 60L
                        + ot.parts_[DT_MIN] * 60L + ot.parts_[DT_SEC];
      if (seconds < 0) {
        expr_datum.set_null();
      } else {
        expr_datum.set_int(seconds);
      }
    }
  }
  return ret;
}

DEF_SET_LOCAL_SESSION_VARS(ObExprToSeconds, raw_expr) {
  int ret = OB_SUCCESS;
  if (is_mysql_mode()) {
    SET_LOCAL_SYSVAR_CAPACITY(2);
    EXPR_ADD_LOCAL_SYSVAR(share::SYS_VAR_SQL_MODE);
    EXPR_ADD_LOCAL_SYSVAR(share::SYS_VAR_TIME_ZONE);
  } else {
    SET_LOCAL_SYSVAR_CAPACITY(1);
    EXPR_ADD_LOCAL_SYSVAR(share::SYS_VAR_TIME_ZONE);
  }
  return ret;
}

ObExprSecToTime::ObExprSecToTime(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_SEC_TO_TIME, N_SEC_TO_TIME, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION) {};

ObExprSecToTime::~ObExprSecToTime() {}

int ObExprSecToTime::cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const
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

int ObExprSecToTime::calc_sectotime(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *param_datum = NULL;
  const ObSQLSessionInfo *session = NULL;
  ObSolidifiedVarsGetter helper(expr, ctx, ctx.exec_ctx_.get_my_session());
  ObSQLMode sql_mode = 0;
  if (OB_ISNULL(session = ctx.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (OB_FAIL(expr.args_[0]->eval(ctx, param_datum))) {
    LOG_WARN("eval param value failed");
  } else if (OB_UNLIKELY(param_datum->is_null())) {
    expr_datum.set_null();
  } else if (OB_FAIL(helper.get_sql_mode(sql_mode))) {
    LOG_WARN("get sql mode failed", K(ret));
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
        ObSQLUtils::get_default_cast_mode(session->get_stmt_type(),
                                          session->is_ignore_stmt(),
                                          sql_mode, cast_mode);
        if (CM_IS_WARN_ON_FAIL(cast_mode)) {
          ret = OB_SUCCESS;
          expr_datum.set_null();
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

DEF_SET_LOCAL_SESSION_VARS(ObExprSecToTime, raw_expr) {
  int ret = OB_SUCCESS;
  if (is_mysql_mode()) {
    SET_LOCAL_SYSVAR_CAPACITY(1);
    EXPR_ADD_LOCAL_SYSVAR(share::SYS_VAR_SQL_MODE);
  }
  return ret;
}

ObExprTimeToSec::ObExprTimeToSec(ObIAllocator &alloc)
   : ObFuncExprOperator(alloc, T_FUN_SYS_TIME_TO_SEC, N_TIME_TO_SEC, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION) {};
ObExprTimeToSec::~ObExprTimeToSec() {}

int ObExprTimeToSec::cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const
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

int ObExprTimeToSec::calc_timetosec(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *param_datum = NULL;
  const ObSQLSessionInfo *session = NULL;
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

ObExprSubAddtime::ObExprSubAddtime(ObIAllocator &alloc, ObExprOperatorType type, const char *name, int32_t param_num, int32_t dimension)
    : ObFuncExprOperator(alloc, type, name, 2, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprSubtime::ObExprSubtime(ObIAllocator &alloc)
    : ObExprSubAddtime(alloc, T_FUN_SYS_SUBTIME, N_SUB_TIME, 2, NOT_ROW_DIMENSION)
{}

ObExprAddtime::ObExprAddtime(ObIAllocator &alloc)
    : ObExprSubAddtime(alloc, T_FUN_SYS_ADDTIME, N_ADD_TIME, 2, NOT_ROW_DIMENSION)
{}

int ObExprSubAddtime::calc_result_type2(ObExprResType &type,
                                     ObExprResType &date_arg,
                                     ObExprResType &time_arg,
                                     ObExprTypeCtx &type_ctx) const
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
  } else if (ObDateTimeType == date_arg.get_type() || ObTimestampType == date_arg.get_type()
             || ObMySQLDateTimeType == date_arg.get_type()) {
    ObObjType res_type = date_arg.get_type();
    if (ObTimestampType == res_type) {
      res_type = type_ctx.enable_mysql_compatible_dates() ? ObMySQLDateTimeType : ObDateTimeType;
    }
    type.set_type(res_type);
    type.set_scale(MAX(scale1, scale2));
    type.set_precision(static_cast<ObPrecision>(DATETIME_MIN_LENGTH + type.get_scale()));
  } else {
    type.set_varchar();
    type.set_length(DATETIME_MAX_LENGTH);
    type.set_scale(-1);
    ret = aggregate_charsets_for_string_result(type, &date_arg, 1, type_ctx);
    date_arg.set_calc_collation_type(type.get_collation_type());
    date_arg.set_calc_collation_level(type.get_collation_level());
  }
  if (OB_SUCC(ret)) {
    // date_arg无法设置calc_type的原因是，date_arg类型是varchar时，设置calc_type为time和datetime都不合适
    // 如果设置为time，那么当字符串中包含日期时，转换后日期信息直接丢失了
    // 如果设置为datetime，虽然保留了日期信息，但是当字符串不包含日期时，mysql会将字符串最右边视为秒，
    // 即'123'的含义是1分23秒，但是OB cast成datetime类型会把字符串右边视为日，即‘123’的含义是1月23日
    if (ob_is_enumset_tc(date_arg.get_type())) {
      date_arg.set_calc_type(ObVarcharType);
    }
    if (ob_is_enumset_tc(time_arg.get_type())) {
      time_arg.set_calc_type(get_enumset_calc_type(ObTimeType, 1));
    } else {
      type_ctx.set_cast_mode(type_ctx.get_cast_mode() | CM_NULL_ON_WARN);
      time_arg.set_calc_type(ObTimeType);
    }
  }
  return ret;
}

int ObExprSubAddtime::calc_result2(common::ObObj &result,
                                const common::ObObj &date_arg,
                                const common::ObObj &time_arg,
                                common::ObExprCtx &expr_ctx) const
{
  int ret = OB_SUCCESS;
  int64_t t_val1 = 0;
  int64_t t_val2 = 0;
  ObTime ot1(DT_TYPE_TIME);
  bool param_with_date = true;
  ObObjType result_type = get_result_type().get_type();
  const ObSQLSessionInfo *session = NULL;
  EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
  if (OB_UNLIKELY(date_arg.is_null() || time_arg.is_null())) {
    result.set_null();
  } else if (OB_ISNULL(expr_ctx.calc_buf_) || OB_ISNULL(session = expr_ctx.my_session_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator or session is null", K(ret), K(expr_ctx.calc_buf_));
  } else {
    if (ObVarcharType == result_type) {
      if (OB_FAIL(ob_obj_to_ob_time_without_date(date_arg,
            get_timezone_info(session), ot1))) {
        LOG_WARN("obj to ob time without date failed", K(ret), K(date_arg));
      } else {
        if (0 == ot1.parts_[DT_YEAR] && 0 == ot1.parts_[DT_MON] && 0 == ot1.parts_[DT_MDAY]) {
          param_with_date = false;
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (ObTimeType == time_arg.get_type()) {
        t_val2 = time_arg.get_time();
      } else {
        ObTime ot2(DT_TYPE_TIME);
        if (OB_FAIL(ob_obj_to_ob_time_without_date(time_arg, get_timezone_info(session), ot2))) {
          LOG_WARN("cast the second param failed", K(ret));
        } else {
          t_val2 = ObTimeConverter::ob_time_to_time(ot2);
        }
      }
      if (OB_FAIL(ret)) {
      } else if (ObTimeType == result_type || !param_with_date) { // a#, subaddtime(时间1, 时间2)
        if (ObVarcharType == result_type) {
          t_val1 = ObTimeConverter::ob_time_to_time(ot1);
          if (IS_NEG_TIME(ot1.mode_)) {
            t_val1 = -t_val1;
          }
        } else {
          TYPE_CHECK(date_arg, ObTimeType);
          t_val1 = date_arg.get_time();
        }
        if (OB_SUCC(ret)) {
          int64_t int_usec = (get_type() == T_FUN_SYS_SUBTIME) ? t_val1 - t_val2 : t_val1 + t_val2;
          if (OB_FAIL(ObTimeConverter::time_overflow_trunc(int_usec))) {
            if (CM_IS_WARN_ON_FAIL(cast_ctx.cast_mode_)) {
              ret = OB_SUCCESS;
              result.set_time(int_usec);
            } else {
              LOG_WARN("time value is out of range", K(ret), K(int_usec));
            }
          } else {
            result.set_time(int_usec);
          }
        }
      } else { // b#, subaddtime(日期时间1, 时间2)
        const ObTimeZoneInfo *tz_info = NULL;
        if (OB_ISNULL(tz_info = get_timezone_info(expr_ctx.my_session_))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("tz info is null", K(ret));
        } else if (ObVarcharType == result_type) {
          ObTimeConvertCtx cvrt_ctx(tz_info, false);
          if (OB_FAIL(ObTimeConverter::ob_time_to_datetime(ot1, cvrt_ctx, t_val1))) {
            LOG_WARN("ob_time_to_datetime failed", K(ret));
          }
        } else {
          if (ObDateTimeType != date_arg.get_type() && ObTimestampType != date_arg.get_type()) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid date arg type", K(ret));
          } else {
            int64_t offset = ObTimestampType == date_arg.get_type() ? tz_info->get_offset() : 0;
            t_val1 = date_arg.get_datetime() + offset * USECS_PER_SEC;
          }
        }
        if (OB_SUCC(ret)) {
          int64_t int_usec = (get_type() == T_FUN_SYS_SUBTIME) ? t_val1 - t_val2 : t_val1 + t_val2;
          if (ObTimeConverter::is_valid_datetime(int_usec)) {
            result.set_datetime(int_usec);
          } else {
            result.set_null();
          }
        }
      }
      if (OB_SUCC(ret) && ObVarcharType == result_type) {
        if (OB_FAIL(ObObjCaster::to_type(ObVarcharType, cast_ctx, result, result))) {
          LOG_WARN("failed to cast object to ObVarcharType ", K(result), K(ret));
        }
      }
    }
    if (OB_FAIL(ret)) {
      uint64_t cast_mode = 0;
      ObSQLUtils::get_default_cast_mode(session->get_stmt_type(), session, cast_mode);
      if (CM_IS_WARN_ON_FAIL(cast_mode) && OB_ALLOCATE_MEMORY_FAILED != ret) {
        ret = OB_SUCCESS;
        result.set_null();
      }
    }
  }
  return ret;
}

int ObExprSubAddtime::cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  if (rt_expr.arg_cnt_ != 2) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("subaddtime expr should have two params", K(ret), K(rt_expr.arg_cnt_));
  } else if (OB_ISNULL(rt_expr.args_) || OB_ISNULL(rt_expr.args_[0])
             || OB_ISNULL(rt_expr.args_[1])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children of subaddtime expr is null", K(ret), K(rt_expr.args_));
  } else {
    const ObObjType result_type = rt_expr.datum_meta_.type_;
    const ObObjType param1_type = rt_expr.args_[0]->datum_meta_.type_;
    const ObObjType param2_type = rt_expr.args_[1]->datum_meta_.type_;
    if (ObTimeType != param2_type && ObVarcharType != param2_type) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("second param of subaddtime expr is not time type", K(ret), K(param2_type));
    } else {
      switch (result_type) {
        case ObDateTimeType :
        case ObMySQLDateTimeType :
          if (ObNullType != param1_type && ObDateTimeType != param1_type
              && ObMySQLDateTimeType != param1_type && ObTimestampType != param1_type) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid type of first argument", K(ret), K(result_type), K(param1_type));
          } else {
            rt_expr.eval_func_ = ObExprSubAddtime::subaddtime_datetime;
          }
          break;
        case ObTimeType :
          if (ObTimeType != param1_type) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid type of first argument", K(ret), K(result_type), K(param1_type));
          } else {
            //这里虽然参数类型、结果类型是TimeType，和上面不同，但是get/set_time/datetime的含义是相同的，
            //两个计算函数的本质都是取第一个参数都int64_t值，与第二个参数相减，然后把结果放入expr_datum中
            rt_expr.eval_func_ = ObExprSubAddtime::subaddtime_datetime;
          }
          break;
        case ObVarcharType :
          rt_expr.eval_func_ = ObExprSubAddtime::subaddtime_varchar;
          break;
        default :
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid expr type", K(rt_expr.datum_meta_.type_));
      }
    }
  }
  return ret;
}

int ObExprSubAddtime::subaddtime_common(const ObExpr &expr,
                                  ObEvalCtx &ctx,
                                  ObDatum &expr_datum,
                                  bool &null_res,
                                  ObDatum *&date_arg,
                                  ObDatum *&time_arg,
                                  int64_t &time_val,
                                  const common::ObTimeZoneInfo *tz_info,
                                  ObSQLMode sql_mode)
{
  int ret = OB_SUCCESS;
  null_res = false;
  const ObSQLSessionInfo *session = NULL;
  ObSolidifiedVarsGetter helper(expr, ctx, ctx.exec_ctx_.get_my_session());
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
    } else if (OB_FAIL(ob_datum_to_ob_time_without_date(
                 *time_arg, expr.args_[1]->datum_meta_.type_, expr.args_[1]->datum_meta_.scale_,
                 tz_info, ot2,
                 expr.args_[1]->obj_meta_.has_lob_header()))) {
      LOG_WARN("cast the second param failed", K(ret));
      expr_datum.set_null();
      null_res = true;
      uint64_t cast_mode = 0;
      ObSQLUtils::get_default_cast_mode(session->get_stmt_type(),
                                      session->is_ignore_stmt(),
                                      sql_mode, cast_mode);
      if (CM_IS_WARN_ON_FAIL(cast_mode)) {
        ret = OB_SUCCESS;
      }
    } else {
      time_val = ObTimeConverter::ob_time_to_time(ot2);
    }
  }
  return ret;
}

int ObExprSubAddtime::subaddtime_datetime(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *date_arg = NULL;
  ObDatum *time_arg = NULL;
  int64_t t_val1 = 0;
  int64_t t_val2 = 0;
  bool null_res = false;
  const ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  ObSolidifiedVarsGetter helper(expr, ctx, session);
  const ObTimeZoneInfo *tz_info = NULL;
  ObSQLMode sql_mode = 0;
  if (OB_FAIL(helper.get_time_zone_info(tz_info))) {
    LOG_WARN("get time zone failed", K(ret));
  } else if (OB_FAIL(helper.get_sql_mode(sql_mode))) {
    LOG_WARN("get sql mode failed", K(ret));
  } else if (OB_FAIL(helper.get_time_zone_info(tz_info))) {
    LOG_WARN("failed to get time zone info", K(ret));
  } else if (OB_FAIL(subaddtime_common(expr, ctx, expr_datum, null_res, date_arg, time_arg, t_val2, tz_info, sql_mode))) {
    LOG_WARN("calc subaddtime failed", K(ret));
  } else if (!null_res) {
    int64_t offset = ObTimestampType == expr.args_[0]->datum_meta_.type_
                                        ? tz_info->get_offset() : 0;
    int64_t dt_value;
    if (ObMySQLDateTimeType == expr.args_[0]->datum_meta_.type_) {
      if (OB_FAIL(ObTimeConverter::mdatetime_to_datetime_without_check(date_arg->get_mysql_datetime(), dt_value))) {
        LOG_WARN("cast mdatetime to datetime fail", K(ret));
      }
    } else {
      dt_value = date_arg->get_datetime();
    }
    t_val1 = dt_value + offset * USECS_PER_SEC;
    int64_t int_usec = (expr.type_ == T_FUN_SYS_SUBTIME) ? t_val1 - t_val2 : t_val1 + t_val2;
    if (OB_SUCC(ret) && ObTimeConverter::is_valid_datetime(int_usec)) {
      if (ObMySQLDateTimeType == expr.datum_meta_.type_) {
        ObMySQLDateTime mdt_value;
        if (OB_FAIL(ObTimeConverter::datetime_to_mdatetime(int_usec, mdt_value))) {
          LOG_WARN("cast datetime to mdatetime fail", K(ret));
        } else {
          expr_datum.set_mysql_datetime(mdt_value);
        }
      } else {
        expr_datum.set_datetime(int_usec);
      }
    } else {
      expr_datum.set_null();
    }
  }
  return ret;
}

int ObExprSubAddtime::subaddtime_varchar(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *date_arg = NULL;
  ObDatum *time_arg = NULL;
  int64_t t_val1 = 0;
  int64_t t_val2 = 0;
  bool null_res = false;
  const ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  ObSolidifiedVarsGetter helper(expr, ctx, session);
  const ObTimeZoneInfo *tz_info = NULL;
  ObSQLMode sql_mode = 0;
  if (OB_FAIL(helper.get_time_zone_info(tz_info))) {
    LOG_WARN("get time zone failed", K(ret));
  } else if (OB_FAIL(helper.get_sql_mode(sql_mode))) {
    LOG_WARN("get sql mode failed", K(ret));
  } else if (OB_FAIL(subaddtime_common(expr, ctx, expr_datum, null_res, date_arg, time_arg, t_val2, tz_info, sql_mode))) {
    LOG_WARN("calc subaddtime failed", K(ret));
  } else if (!null_res) {
    ObTime ot1(DT_TYPE_TIME);
    if (OB_FAIL(ob_datum_to_ob_time_without_date(*date_arg, expr.args_[0]->datum_meta_.type_,
                                                 expr.args_[0]->datum_meta_.scale_, tz_info, ot1,
                                                 expr.args_[0]->obj_meta_.has_lob_header()))) {
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
        if (IS_NEG_TIME(ot1.mode_)) {
          t_val1 = -t_val1;
        }
      }
      if (OB_SUCC(ret)) {
        int64_t int_usec = (expr.type_ == T_FUN_SYS_SUBTIME) ? t_val1 - t_val2 : t_val1 + t_val2;
        const int64_t datetime_buf_len = DATETIME_MAX_LENGTH + 1;
        char *buf = expr.get_str_res_mem(ctx, datetime_buf_len);
        int64_t pos = 0;
				//兼容mysql行为，有毫秒则显示6位小数，否则不显示
        ObString format;
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate memory failed", K(ret));
        } else if (param_with_date && OB_FAIL(ObTimeConverter::datetime_to_str(int_usec, NULL,
                                                format, -1, buf,
                                                datetime_buf_len, pos, true))) {
          LOG_WARN("datetime to str failed", K(ret));
        } else if (!param_with_date && OB_FAIL(ObTimeConverter::time_to_str(int_usec, -1, buf,
                                                                datetime_buf_len, pos, true))) {
          LOG_WARN("time to str failed", K(ret));
        } else {
          expr_datum.ptr_ = buf;
          expr_datum.pack_ = static_cast<uint32_t>(pos);
        }
      }
    }
    if (OB_FAIL(ret) && OB_ALLOCATE_MEMORY_FAILED != ret) {
      uint64_t cast_mode = 0;
      ObSQLUtils::get_default_cast_mode(session->get_stmt_type(),
                                      session->is_ignore_stmt(),
                                      sql_mode, cast_mode);
      if (CM_IS_WARN_ON_FAIL(cast_mode) ) {
        ret = OB_SUCCESS;
        expr_datum.set_null();
      }
    }
  }
  return ret;
}

DEF_SET_LOCAL_SESSION_VARS(ObExprSubAddtime, raw_expr) {
  int ret = OB_SUCCESS;
  if (is_mysql_mode()) {
    SET_LOCAL_SYSVAR_CAPACITY(3);
    EXPR_ADD_LOCAL_SYSVAR(share::SYS_VAR_SQL_MODE);
    EXPR_ADD_LOCAL_SYSVAR(share::SYS_VAR_COLLATION_CONNECTION);
    EXPR_ADD_LOCAL_SYSVAR(share::SYS_VAR_TIME_ZONE);
  }
  return ret;
}

ObExprDayName::ObExprDayName(ObIAllocator &alloc)
    : ObExprTimeBase(alloc, DT_WDAY, T_FUN_SYS_DAY_NAME, N_DAY_NAME, VALID_FOR_GENERATED_COL) {};
ObExprDayName::~ObExprDayName() {}

int ObExprDayName::calc_dayname(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObExprTimeBase::calc(expr, ctx, expr_datum, DT_WDAY, true))) {
    LOG_WARN("dayname calc day of dayweek failed", K(ret));
  }
  return ret;
}

int ObExprDayName::calc_result_type1(ObExprResType &type,
                                     ObExprResType &type1,
                                     common::ObExprTypeCtx &type_ctx) const
{
  ObCollationType cs_type = type_ctx.get_coll_type();
  type.set_varchar();
  type.set_full_length(DAYNAME_MAX_LENGTH, type1.get_length_semantics());
  type.set_collation_type(cs_type);
  type.set_collation_level(CS_LEVEL_IMPLICIT);
  common::ObObjTypeClass tc1 = ob_obj_type_class(type1.get_type());
  if (ob_is_enumset_tc(type1.get_type())) {
    type1.set_calc_type(common::ObVarcharType);
    type1.set_collation_type(cs_type);
    type1.set_collation_level(CS_LEVEL_IMPLICIT);
  } else if ((common::ObFloatTC == tc1) || (common::ObDoubleTC == tc1)) {
    type1.set_calc_type(common::ObIntType);
  }
  return OB_SUCCESS;
}

#define EPOCH_WDAY    4       // 1970-1-1 is thursday.
#define CHECK_SKIP_NULL(idx) {                    \
  if (skip.at(idx) || eval_flags.at(idx)) {    \
    continue;                                  \
  } else if (arg_vec->is_null(idx)) {          \
    res_vec->set_null(idx);                    \
    eval_flags.set(idx);                       \
    continue;                                  \
  }                                            \
}

#define BATCH_CALC(BODY) {                                                 \
  if (OB_LIKELY(no_skip_no_null)) {                                               \
    for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) { \
      BODY;                                                                       \
    }                                                                             \
  } else {                                                                        \
    for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) { \
      CHECK_SKIP_NULL(idx);                                                       \
      BODY;                                                                       \
    }                                                                             \
  }                                                                               \
}


template <typename ArgVec, typename ResVec, typename IN_TYPE>
int vector_dayofyear(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  ArgVec *arg_vec = static_cast<ArgVec *>(expr.args_[0]->get_vector(ctx));
  ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  int64_t tz_offset = 0;
  const common::ObTimeZoneInfo *tz_info = NULL;
  ObSolidifiedVarsGetter helper(expr, ctx, ctx.exec_ctx_.get_my_session());
  if (OB_FAIL(helper.get_time_zone_info(tz_info))) {
    LOG_WARN("get tz info failed", K(ret));
  } else {
    ObSQLMode sql_mode = 0;
    const ObTimeZoneInfo *local_tz_info = (ObTimestampType == expr.args_[0]->datum_meta_.type_) ? tz_info : NULL;
    if (OB_FAIL(get_tz_offset(local_tz_info, tz_offset))) {
      LOG_WARN("get tz_info offset fail", K(ret));
    } else if (OB_FAIL(helper.get_sql_mode(sql_mode))) {
      LOG_WARN("get sql mode failed", K(ret));
    } else {
      DateType date = 0;
      DateType dt_yday = 0;
      YearType year = 0;
      UsecType usec = 0;
      bool no_skip_no_null = bound.get_all_rows_active() && !arg_vec->has_null()
                             && eval_flags.accumulate_bit_cnt(bound) == 0;
      ObTime ob_time;
      ObDateSqlMode date_sql_mode;
      date_sql_mode.init(sql_mode);
      BATCH_CALC({
        IN_TYPE in_val = *reinterpret_cast<const IN_TYPE*>(arg_vec->get_payload(idx));
        if (std::is_same<IN_TYPE, ObMySQLDate>::value || std::is_same<IN_TYPE, ObMySQLDateTime>::value) {
          if (OB_FAIL(ObTimeConverter::parse_ob_time<IN_TYPE>(in_val, ob_time))) {
            LOG_WARN("parse_ob_time fail", K(ret));
          } else if (OB_FAIL(ObTimeConverter::validate_datetime(ob_time, date_sql_mode))) {
            ret = OB_SUCCESS;
            res_vec->set_null(idx);
          } else {
            dt_yday = ObTimeConverter::calc_yday(ob_time);
            (dt_yday == 0) ? res_vec->set_null(idx) : res_vec->set_int(idx, dt_yday);
          }
        } else if (OB_FAIL(ObTimeConverter::parse_date_usec<IN_TYPE>(in_val, tz_offset, lib::is_oracle_mode(), date, usec))) {
          LOG_WARN("get date and usec from vec failed", K(ret));
        } else if (OB_UNLIKELY(ObTimeConverter::ZERO_DATE == date)) {
          res_vec->set_null(idx);
        } else {
          ObTimeConverter::days_to_year_ydays(date, year, dt_yday);
          res_vec->set_int(idx, dt_yday);
        }
        eval_flags.set(idx);
      });
    }
  }
  return ret;
}

#define DISPATCH_DAY_OF_EXPR_VECTOR(FUNC, TYPE) \
  if (VEC_FIXED == arg_format && VEC_FIXED == res_format) {\
    ret = FUNC<CONCAT(TYPE, FixedVec), IntegerFixedVec, CONCAT(TYPE, Type)>(expr, ctx, skip, bound);\
  } else if (VEC_FIXED == arg_format && VEC_UNIFORM == res_format) {\
    ret = FUNC<CONCAT(TYPE, FixedVec), IntegerUniVec, CONCAT(TYPE, Type)>(expr, ctx, skip, bound);\
  } else if (VEC_FIXED == arg_format && VEC_UNIFORM_CONST == res_format) {\
    ret = FUNC<CONCAT(TYPE, FixedVec), IntegerUniCVec, CONCAT(TYPE, Type)>(expr, ctx, skip, bound);\
  } else if (VEC_UNIFORM == arg_format && VEC_FIXED == res_format) {\
    ret = FUNC<CONCAT(TYPE, UniVec), IntegerFixedVec, CONCAT(TYPE, Type)>(expr, ctx, skip, bound);\
  } else if (VEC_UNIFORM == arg_format && VEC_UNIFORM == res_format) {\
    ret = FUNC<CONCAT(TYPE, UniVec), IntegerUniVec, CONCAT(TYPE, Type)>(expr, ctx, skip, bound);\
  } else if (VEC_UNIFORM == arg_format && VEC_UNIFORM_CONST == res_format) {\
    ret = FUNC<CONCAT(TYPE, UniVec), IntegerUniCVec, CONCAT(TYPE, Type)>(expr, ctx, skip, bound);\
  } else if (VEC_UNIFORM_CONST == arg_format && VEC_FIXED == res_format) {\
    ret = FUNC<CONCAT(TYPE, UniCVec), IntegerFixedVec, CONCAT(TYPE, Type)>(expr, ctx, skip, bound);\
  } else if (VEC_UNIFORM_CONST == arg_format && VEC_UNIFORM == res_format) {\
    ret = FUNC<CONCAT(TYPE, UniCVec), IntegerUniVec, CONCAT(TYPE, Type)>(expr, ctx, skip, bound);\
  } else if (VEC_UNIFORM_CONST == arg_format && VEC_UNIFORM_CONST == res_format) {\
    ret = FUNC<CONCAT(TYPE, UniCVec), IntegerUniCVec, CONCAT(TYPE, Type)>(expr, ctx, skip, bound);\
  } else {\
    ret = FUNC<ObVectorBase, ObVectorBase, CONCAT(TYPE, Type)>(expr, ctx, skip, bound);\
  }

int ObExprDayOfYear::calc_dayofyear_vector(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound) {
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("fail to eval date_format param", K(ret));
  } else {
    VectorFormat arg_format = expr.args_[0]->get_format(ctx);
    VectorFormat res_format = expr.get_format(ctx);
    const ObObjType arg_type = expr.args_[0]->datum_meta_.type_;
    if (ObMySQLDateTC == ob_obj_type_class(arg_type)) {
      DISPATCH_DAY_OF_EXPR_VECTOR(vector_dayofyear, MySQLDate);
    } else if (ObMySQLDateTimeTC == ob_obj_type_class(arg_type)) {
      DISPATCH_DAY_OF_EXPR_VECTOR(vector_dayofyear, MySQLDateTime);
    } else if (ObDateTC == ob_obj_type_class(arg_type)) {
      DISPATCH_DAY_OF_EXPR_VECTOR(vector_dayofyear, Date);
    } else if (ObDateTimeTC == ob_obj_type_class(arg_type)) {
      DISPATCH_DAY_OF_EXPR_VECTOR(vector_dayofyear, DateTime);
    }

    if (OB_FAIL(ret)) {
      LOG_WARN("expr calculation failed", K(ret));
    }
  }
  return ret;
}

template <typename ArgVec, typename ResVec, typename IN_TYPE>
int vector_dayofmonth(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  ArgVec *arg_vec = static_cast<ArgVec *>(expr.args_[0]->get_vector(ctx));
  ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  int64_t tz_offset = 0;
  const common::ObTimeZoneInfo *tz_info = NULL;
  ObSolidifiedVarsGetter helper(expr, ctx, ctx.exec_ctx_.get_my_session());
  if (OB_FAIL(helper.get_time_zone_info(tz_info))) {
    LOG_WARN("get tz info failed", K(ret));
  } else {
    const ObTimeZoneInfo *local_tz_info = (ObTimestampType == expr.args_[0]->datum_meta_.type_) ? tz_info : NULL;
    if (OB_FAIL(get_tz_offset(local_tz_info, tz_offset))) {
      LOG_WARN("get tz_info offset fail", K(ret));
    } else {
      DateType date = 0;
      DateType dt_yday = 0;
      DateType dt_mday = 0;
      YearType year = 0;
      UsecType usec = 0;
      MonthType month = 0;
      bool no_skip_no_null = bound.get_all_rows_active() && !arg_vec->has_null()
                             && eval_flags.accumulate_bit_cnt(bound) == 0;
      ObTime ob_time;
      BATCH_CALC({
        IN_TYPE in_val = *reinterpret_cast<const IN_TYPE*>(arg_vec->get_payload(idx));
        if (std::is_same<IN_TYPE, ObMySQLDate>::value || std::is_same<IN_TYPE, ObMySQLDateTime>::value) {
          ret = ObTimeConverter::parse_ob_time<IN_TYPE>(in_val, ob_time);
          dt_mday = ob_time.parts_[DT_MDAY];
        } else if (OB_FAIL(ObTimeConverter::parse_date_usec<IN_TYPE>(in_val, tz_offset, lib::is_oracle_mode(), date, usec))) {
          LOG_WARN("get date and usec from vec failed", K(ret));
        } else if (OB_UNLIKELY(ObTimeConverter::ZERO_DATE == date)) {
          dt_mday = 0;
        } else {
          ObTimeConverter::days_to_year_ydays(date, year, dt_yday);
          ObTimeConverter::ydays_to_month_mdays(year, dt_yday, month, dt_mday);
        }
        res_vec->set_int(idx, dt_mday);
        eval_flags.set(idx);
      });
    }
  }
  return ret;
}
int ObExprDayOfMonth::calc_dayofmonth_vector(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("fail to eval date_format param", K(ret));
  } else {
    VectorFormat arg_format = expr.args_[0]->get_format(ctx);
    VectorFormat res_format = expr.get_format(ctx);
    const ObObjType arg_type = expr.args_[0]->datum_meta_.type_;
    if (ObMySQLDateTC == ob_obj_type_class(arg_type)) {
      DISPATCH_DAY_OF_EXPR_VECTOR(vector_dayofmonth, MySQLDate);
    } else if (ObMySQLDateTimeTC == ob_obj_type_class(arg_type)) {
      DISPATCH_DAY_OF_EXPR_VECTOR(vector_dayofmonth, MySQLDateTime);
    } else if (ObDateTC == ob_obj_type_class(arg_type)) {
      DISPATCH_DAY_OF_EXPR_VECTOR(vector_dayofmonth, Date);
    } else if (ObDateTimeTC == ob_obj_type_class(arg_type)) {
      DISPATCH_DAY_OF_EXPR_VECTOR(vector_dayofmonth, DateTime);
    }

    if (OB_FAIL(ret)) {
      LOG_WARN("expr calculation failed", K(ret));
    }
  }
  return ret;
}

template <typename ArgVec, typename ResVec, typename IN_TYPE>
int vector_dayofweek(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  ArgVec *arg_vec = static_cast<ArgVec *>(expr.args_[0]->get_vector(ctx));
  ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  int64_t tz_offset = 0;
  const common::ObTimeZoneInfo *tz_info = NULL;
  ObSolidifiedVarsGetter helper(expr, ctx, ctx.exec_ctx_.get_my_session());
  if (OB_FAIL(helper.get_time_zone_info(tz_info))) {
    LOG_WARN("get tz info failed", K(ret));
  } else {
    const ObTimeZoneInfo *local_tz_info = (ObTimestampType == expr.args_[0]->datum_meta_.type_) ? tz_info : NULL;
    if (OB_FAIL(get_tz_offset(local_tz_info, tz_offset))) {
      LOG_WARN("get tz_info offset fail", K(ret));
    } else {
      DateType days = 0;
      YearType year = 0;
      UsecType usec = 0;
      MonthType month = 0;
      bool no_skip_no_null = bound.get_all_rows_active() && !arg_vec->has_null()
                             && eval_flags.accumulate_bit_cnt(bound) == 0;
      BATCH_CALC({
        IN_TYPE in_val = *reinterpret_cast<const IN_TYPE*>(arg_vec->get_payload(idx));
        if (OB_FAIL(ObTimeConverter::parse_date_usec<IN_TYPE>(in_val, tz_offset, lib::is_oracle_mode(), days, usec))) {
          LOG_WARN("get date and usec from vec failed", K(ret));
        } else if (OB_UNLIKELY(ObTimeConverter::ZERO_DATE == days)) {
          res_vec->set_null(idx);
        } else {
          days = WDAY_OFFSET[days % DAYS_PER_WEEK][EPOCH_WDAY];
          int64_t dayofweek = days % 7 + 1;  // start from sun. copy from calc_dayofweek
          res_vec->set_int(idx, dayofweek);
        }
        eval_flags.set(idx);
      });
    }
  }
  return ret;
}
int ObExprDayOfWeek::calc_dayofweek_vector(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("fail to eval date_format param", K(ret));
  } else {
    VectorFormat arg_format = expr.args_[0]->get_format(ctx);
    VectorFormat res_format = expr.get_format(ctx);
    const ObObjType arg_type = expr.args_[0]->datum_meta_.type_;
    if (ObMySQLDateTC == ob_obj_type_class(arg_type)) {
      DISPATCH_DAY_OF_EXPR_VECTOR(vector_dayofweek, MySQLDate);
    } else if (ObMySQLDateTimeTC == ob_obj_type_class(arg_type)) {
      DISPATCH_DAY_OF_EXPR_VECTOR(vector_dayofweek, MySQLDateTime);
    } else if (ObDateTC == ob_obj_type_class(arg_type)) {
      DISPATCH_DAY_OF_EXPR_VECTOR(vector_dayofweek, Date);
    } else if (ObDateTimeTC == ob_obj_type_class(arg_type)) {
      DISPATCH_DAY_OF_EXPR_VECTOR(vector_dayofweek, DateTime);
    }

    if (OB_FAIL(ret)) {
      LOG_WARN("expr calculation failed", K(ret));
    }
  }
  return ret;
}


template <typename ArgVec, typename ResVec, typename IN_TYPE>
int vector_dayname(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  ArgVec *arg_vec = static_cast<ArgVec *>(expr.args_[0]->get_vector(ctx));
  ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  int64_t tz_offset = 0;
  ObString locale_name;
  const ObSQLSessionInfo *session = NULL;
  const common::ObTimeZoneInfo *tz_info = NULL;
  ObSolidifiedVarsGetter helper(expr, ctx, ctx.exec_ctx_.get_my_session());
  if (OB_ISNULL(session = ctx.exec_ctx_.get_my_session())) {
    ret = OB_NOT_INIT;
    LOG_WARN("session is null", K(ret), K(session));
  } else if (OB_FAIL(helper.get_time_zone_info(tz_info))) {
    LOG_WARN("get tz info failed", K(ret));
  } else if (OB_FAIL(session->get_locale_name(locale_name))) {
    LOG_WARN("failed to get locale time name", K(expr));
  } else {
    const ObTimeZoneInfo *local_tz_info = (ObTimestampType == expr.args_[0]->datum_meta_.type_) ? tz_info : NULL;
    if (OB_FAIL(get_tz_offset(local_tz_info, tz_offset))) {
      LOG_WARN("get tz_info offset fail", K(ret));
    } else {
      DateType days = 0;
      UsecType usec = 0;
      bool no_skip_no_null = bound.get_all_rows_active() && !arg_vec->has_null()
                             && eval_flags.accumulate_bit_cnt(bound) == 0;
      OB_LOCALE *ob_cur_locale = ob_locale_by_name(locale_name);
      OB_LOCALE_TYPE *locale_type_day = ob_cur_locale->day_names_;
      const char ** locale_daynames = locale_type_day->type_names_;
      const char *const *day_name = nullptr;
      if (lib::is_mysql_mode()) {
        day_name = locale_daynames;
      } else {
        day_name = &(WDAY_NAMES+1)->ptr_;
      }
      BATCH_CALC({
        IN_TYPE in_val = *reinterpret_cast<const IN_TYPE*>(arg_vec->get_payload(idx));
        if (OB_FAIL(ObTimeConverter::parse_date_usec<IN_TYPE>(in_val, tz_offset, lib::is_oracle_mode(), days, usec))) {
          LOG_WARN("get date and usec from vec failed", K(ret));
        } else if (OB_UNLIKELY(ObTimeConverter::ZERO_DATE == days)) {
          res_vec->set_null(idx);
        } else {
          DateType dt_wday = WDAY_OFFSET[days % DAYS_PER_WEEK][EPOCH_WDAY];
          size_t len = strlen(day_name[dt_wday-1]);
          res_vec->set_string(idx, ObString(len, day_name[dt_wday-1]));
        }
        eval_flags.set(idx);
      });
    }
  }
  return ret;
}
#define DISPATCH_DAYNAME_EXPR_VECTOR(TYPE)\
if (VEC_FIXED == arg_format && VEC_DISCRETE == res_format) {\
  ret = vector_dayname<CONCAT(TYPE,FixedVec), StrDiscVec, CONCAT(TYPE, Type)>(expr, ctx, skip, bound);\
} else if (VEC_FIXED == arg_format && VEC_UNIFORM == res_format) {\
  ret = vector_dayname<CONCAT(TYPE,FixedVec), StrUniVec, CONCAT(TYPE, Type)>(expr, ctx, skip, bound);\
} else if (VEC_FIXED == arg_format && VEC_CONTINUOUS == res_format) {\
  ret = vector_dayname<CONCAT(TYPE,FixedVec), StrContVec, CONCAT(TYPE, Type)>(expr, ctx, skip, bound);\
} else if (VEC_FIXED == arg_format && VEC_UNIFORM_CONST == res_format) {\
  ret = vector_dayname<CONCAT(TYPE,FixedVec), StrUniCVec, CONCAT(TYPE, Type)>(expr, ctx, skip, bound);\
} else if (VEC_UNIFORM == arg_format && VEC_DISCRETE == res_format) {\
  ret = vector_dayname<CONCAT(TYPE,UniVec), StrDiscVec, CONCAT(TYPE, Type)>(expr, ctx, skip, bound);\
} else if (VEC_UNIFORM == arg_format && VEC_UNIFORM == res_format) {\
  ret = vector_dayname<CONCAT(TYPE,UniVec), StrUniVec, CONCAT(TYPE, Type)>(expr, ctx, skip, bound);\
} else if (VEC_UNIFORM == arg_format && VEC_UNIFORM_CONST == res_format) {\
  ret = vector_dayname<CONCAT(TYPE,UniVec), StrUniCVec, CONCAT(TYPE, Type)>(expr, ctx, skip, bound);\
} else if (VEC_UNIFORM == arg_format && VEC_CONTINUOUS == res_format) {\
  ret = vector_dayname<CONCAT(TYPE,UniVec), StrContVec, CONCAT(TYPE, Type)>(expr, ctx, skip, bound);\
} else if (VEC_UNIFORM_CONST == arg_format && VEC_DISCRETE == res_format) {\
  ret = vector_dayname<CONCAT(TYPE,UniCVec), StrDiscVec, CONCAT(TYPE, Type)>(expr, ctx, skip, bound);\
} else if (VEC_UNIFORM_CONST == arg_format && VEC_UNIFORM == res_format) {\
  ret = vector_dayname<CONCAT(TYPE,UniCVec), StrUniVec, CONCAT(TYPE, Type)>(expr, ctx, skip, bound);\
} else if (VEC_UNIFORM_CONST == arg_format && VEC_CONTINUOUS == res_format) {\
  ret = vector_dayname<CONCAT(TYPE,UniCVec), StrContVec, CONCAT(TYPE, Type)>(expr, ctx, skip, bound);\
} else if (VEC_UNIFORM_CONST == arg_format && VEC_UNIFORM_CONST == res_format) {\
  ret = vector_dayname<CONCAT(TYPE,UniCVec), StrUniCVec, CONCAT(TYPE, Type)>(expr, ctx, skip, bound);\
} else {\
  ret = vector_dayname<ObVectorBase, ObVectorBase, CONCAT(TYPE, Type)>(expr, ctx, skip, bound);\
}
int ObExprDayName::calc_dayname_vector(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("fail to eval date_format param", K(ret));
  } else {
    VectorFormat arg_format = expr.args_[0]->get_format(ctx);
    VectorFormat res_format = expr.get_format(ctx);
    const ObObjType arg_type = expr.args_[0]->datum_meta_.type_;
    if (ObMySQLDateTC == ob_obj_type_class(arg_type)) {
      DISPATCH_DAYNAME_EXPR_VECTOR(MySQLDate);
    } else if (ObMySQLDateTimeTC == ob_obj_type_class(arg_type)) {
      DISPATCH_DAYNAME_EXPR_VECTOR(MySQLDateTime);
    } else if (ObDateTC == ob_obj_type_class(arg_type)) {
      DISPATCH_DAYNAME_EXPR_VECTOR(Date);
    } else if (ObDateTimeTC == ob_obj_type_class(arg_type)) {
      DISPATCH_DAYNAME_EXPR_VECTOR(DateTime);
    }

    if (OB_FAIL(ret)) {
      LOG_WARN("expr calculation failed", K(ret));
    }
  }
  return ret;
}
#undef DISPATCH_DAYNAME_EXPR_VECTOR
#undef DISPATCH_DAY_OF_EXPR_VECTOR
#undef EPOCH_WDAY
#undef CHECK_SKIP_NULL
#undef BATCH_CALC


} //namespace sql
} //namespace oceanbase
