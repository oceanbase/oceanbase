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
#include "ob_expr_time.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_datum_cast.h"
#include "sql/engine/expr/ob_expr_day_of_func.h"
#include "lib/locale/ob_locale_type.h"
#include "common/ob_target_specific.h"
using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{

ObExprTime::ObExprTime(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_TIME, N_TIME, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{}

ObExprTime::~ObExprTime()
{
}

int ObExprTime::calc_result_type1(ObExprResType &type,
                                  ObExprResType &type1,
                                  ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  //param will be casted to ObTimeType before calculation
  //see ObExprOperator::cast_operand_type for more details if necessary
  type1.set_calc_type(ObTimeType);
  type.set_type(ObTimeType);
  //deduce scale now.
  int16_t scale1 = MIN(type1.get_scale(), MAX_SCALE_FOR_TEMPORAL);
  int16_t scale = (SCALE_UNKNOWN_YET == scale1) ? MAX_SCALE_FOR_TEMPORAL : scale1;
  type.set_scale(scale);
  type_ctx.set_cast_mode(type_ctx.get_cast_mode() | CM_NULL_ON_WARN);
  return ret;
}

int ObExprTime::cg_expr(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
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
    // The vectorization of other types for the expression not completed yet.
    if (ob_is_time_tc(rt_expr.args_[0]->datum_meta_.type_)) {
      rt_expr.eval_vector_func_ = ObExprTime::calc_time_vector;
    }
  }
  return ret;
}

int ObExprTime::calc_time(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *param_datum = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, param_datum))) {
    LOG_WARN("eval param value failed");
  } else if (OB_UNLIKELY(param_datum->is_null())) {
    expr_datum.set_null();
  } else {
    expr_datum.set_time(param_datum->get_time());
  }
  return ret;
}

ObExprTimeBase::ObExprTimeBase(ObIAllocator &alloc, int32_t date_type, ObExprOperatorType type,
                              const char *name, ObValidForGeneratedColFlag valid_for_generated_col)
    : ObFuncExprOperator(alloc, type, name, 1, valid_for_generated_col, NOT_ROW_DIMENSION)
{
  dt_type_ = date_type;
}

ObExprTimeBase::~ObExprTimeBase()
{
}

static const char* daynames[7] = {"Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"};
static const char* monthnames[12] = {"January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"};
int ObExprTimeBase::cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const
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
    const ObObjTypeClass arg_tc = ob_obj_type_class(rt_expr.args_[0]->datum_meta_.type_);
    if(get_type() == T_FUN_SYS_DAY_NAME) {
      rt_expr.eval_func_ = ObExprDayName::calc_dayname;
      // The vectorization of other types for the expression not completed yet.
      if (ob_is_date_or_mysql_date(rt_expr.args_[0]->datum_meta_.type_)
            || ob_is_datetime_or_mysql_datetime_tc(rt_expr.args_[0]->datum_meta_.type_)
            || (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_5_1_0 &&
                (ob_is_string_tc(rt_expr.args_[0]->datum_meta_.type_) ||
                 ob_is_int_uint_tc(rt_expr.args_[0]->datum_meta_.type_)))) {
        rt_expr.eval_vector_func_ = ObExprDayName::calc_dayname_vector;
      }
    } else {
      switch (dt_type_) {
        case DT_HOUR :
          rt_expr.eval_func_ = ObExprHour::calc_hour;
          // The vectorization of other types for the expression not completed yet.
          if (ob_is_date_or_mysql_date(rt_expr.args_[0]->datum_meta_.type_)
                || ob_is_datetime_or_mysql_datetime_tc(rt_expr.args_[0]->datum_meta_.type_)
                || (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_5_1_0 &&
                    (ob_is_string_tc(rt_expr.args_[0]->datum_meta_.type_) ||
                    ob_is_int_uint_tc(rt_expr.args_[0]->datum_meta_.type_)))) {
            rt_expr.eval_vector_func_ = ObExprHour::calc_hour_vector;
          }
          break;
        case DT_MIN :
          rt_expr.eval_func_ = ObExprMinute::calc_minute;
          // The vectorization of other types for the expression not completed yet.
          if (ob_is_date_or_mysql_date(rt_expr.args_[0]->datum_meta_.type_)
                || ob_is_datetime_or_mysql_datetime_tc(rt_expr.args_[0]->datum_meta_.type_)
                || (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_5_1_0 &&
                    (ob_is_string_tc(rt_expr.args_[0]->datum_meta_.type_) ||
                    ob_is_int_uint_tc(rt_expr.args_[0]->datum_meta_.type_)))) {
            rt_expr.eval_vector_func_ = ObExprMinute::calc_minute_vector;
          }
          break;
        case DT_SEC :
          rt_expr.eval_func_ = ObExprSecond::calc_second;
          if (ob_is_string_tc(rt_expr.args_[0]->datum_meta_.type_)
                || ob_is_int_uint_tc(rt_expr.args_[0]->datum_meta_.type_)) {
            rt_expr.eval_vector_func_ = ObExprSecond::calc_second_vector;
          }
          break;
        case DT_USEC :
          rt_expr.eval_func_ = ObExprMicrosecond::calc_microsecond;
          if (ob_is_string_tc(rt_expr.args_[0]->datum_meta_.type_)
                || ob_is_int_uint_tc(rt_expr.args_[0]->datum_meta_.type_)) {
            rt_expr.eval_vector_func_ = ObExprMicrosecond::calc_microsecond_vector;
          }
          break;
        case DT_MDAY :
          rt_expr.eval_func_ = ObExprDayOfMonth::calc_dayofmonth;
          // The vectorization of other types for the expression not completed yet.
          if (ob_is_date_or_mysql_date(rt_expr.args_[0]->datum_meta_.type_)
                || ob_is_datetime_or_mysql_datetime_tc(rt_expr.args_[0]->datum_meta_.type_)
                || (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_5_1_0 &&
                    (ob_is_string_tc(rt_expr.args_[0]->datum_meta_.type_) ||
                    ob_is_int_uint_tc(rt_expr.args_[0]->datum_meta_.type_)))) {
            rt_expr.eval_vector_func_ = ObExprDayOfMonth::calc_dayofmonth_vector;
          }
          break;
        case DT_WDAY :
          rt_expr.eval_func_ = ObExprDayOfWeek::calc_dayofweek;
          // The vectorization of other types for the expression not completed yet.
          if (ob_is_date_or_mysql_date(rt_expr.args_[0]->datum_meta_.type_)
                || ob_is_datetime_or_mysql_datetime_tc(rt_expr.args_[0]->datum_meta_.type_)
                || (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_5_1_0 &&
                    (ob_is_string_tc(rt_expr.args_[0]->datum_meta_.type_) ||
                    ob_is_int_uint_tc(rt_expr.args_[0]->datum_meta_.type_)))) {
            rt_expr.eval_vector_func_ = ObExprDayOfWeek::calc_dayofweek_vector;
          }
          break;
        case DT_YDAY :
          rt_expr.eval_func_ = ObExprDayOfYear::calc_dayofyear;
          // The vectorization of other types for the expression not completed yet.
          if (ob_is_date_or_mysql_date(rt_expr.args_[0]->datum_meta_.type_)
                || ob_is_datetime_or_mysql_datetime_tc(rt_expr.args_[0]->datum_meta_.type_)
                || (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_5_1_0 &&
                    (ob_is_string_tc(rt_expr.args_[0]->datum_meta_.type_) ||
                    ob_is_int_uint_tc(rt_expr.args_[0]->datum_meta_.type_)))) {
            rt_expr.eval_vector_func_ = ObExprDayOfYear::calc_dayofyear_vector;
          }
          break;
        case DT_YEAR :
          rt_expr.eval_func_ = ObExprYear::calc_year;
          // The vectorization of other types for the expression not completed yet.
          if (ob_is_date_or_mysql_date(rt_expr.args_[0]->datum_meta_.type_)
                || ob_is_datetime_or_mysql_datetime_tc(rt_expr.args_[0]->datum_meta_.type_)
                || (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_5_1_0 &&
                    (ob_is_string_tc(rt_expr.args_[0]->datum_meta_.type_) ||
                    ob_is_int_uint_tc(rt_expr.args_[0]->datum_meta_.type_)))) {
            rt_expr.eval_vector_func_ = ObExprYear::calc_year_vector;
          }
          break;
        case DT_MON :
          rt_expr.eval_func_ = ObExprMonth::calc_month;
          // The vectorization of other types for the expression not completed yet.
          if (ob_is_date_or_mysql_date(rt_expr.args_[0]->datum_meta_.type_)
                || ob_is_datetime_or_mysql_datetime_tc(rt_expr.args_[0]->datum_meta_.type_)
                || (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_5_1_0 &&
                    (ob_is_string_tc(rt_expr.args_[0]->datum_meta_.type_) ||
                    ob_is_int_uint_tc(rt_expr.args_[0]->datum_meta_.type_)))) {
            rt_expr.eval_vector_func_ = ObExprMonth::calc_month_vector;
          }
          break;
        case DT_MON_NAME :
          rt_expr.eval_func_ = ObExprMonthName::calc_month_name;
          // The vectorization of other types for the expression not completed yet.
          if (ob_is_date_or_mysql_date(rt_expr.args_[0]->datum_meta_.type_)
                || ob_is_datetime_or_mysql_datetime_tc(rt_expr.args_[0]->datum_meta_.type_)
                || (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_5_1_0 &&
                    (ob_is_string_tc(rt_expr.args_[0]->datum_meta_.type_) ||
                    ob_is_int_uint_tc(rt_expr.args_[0]->datum_meta_.type_)))) {
            rt_expr.eval_vector_func_ = ObExprMonthName::calc_month_name_vector;
          }
          break;
        default:
          LOG_WARN("ObExprTimeBase cg_expr switch default", K(dt_type_));
          break;
      }      
    }
  }
  return ret;
}

static int ob_expr_convert_to_time(const ObDatum &datum,
                                  ObObjType type,
                                  const ObScale scale,
                                  bool with_date,
                                  bool is_allow_incomplete_dates,
                                  ObEvalCtx &ctx,
                                  ObTime &ot,
                                  bool has_lob_header,
                                  const common::ObTimeZoneInfo *tz_info,
                                  ObSQLMode sql_mode)
{
  int ret = OB_SUCCESS;
  if (with_date) {
    ObTime ot2;
    ObDateSqlMode date_sql_mode;
    date_sql_mode.init(sql_mode);
    date_sql_mode.allow_incomplete_dates_ = is_allow_incomplete_dates;
    if (OB_FAIL(ob_datum_to_ob_time_with_date(datum, type, scale, tz_info,
        ot2, get_cur_time(ctx.exec_ctx_.get_physical_plan_ctx()), date_sql_mode,
        has_lob_header))) {
      LOG_WARN("cast to ob time failed", K(ret));
    } else {
      if (ob_is_mysql_datetime_tc(type) || ob_is_mysql_date_tc(type)) {
        if (OB_FAIL(ObTimeConverter::validate_datetime(ot2, date_sql_mode))) {
          ret = OB_SUCCESS;
          ot2.parts_[DT_DATE] = ObTimeConverter::ZERO_DATE;
        }
      }
      ot = ot2;
    }
  } else {
    ObTime ot2(DT_TYPE_TIME);
    if (OB_FAIL(ob_datum_to_ob_time_without_date(datum, type, scale, tz_info,
                                                 ot2, has_lob_header))) {
      LOG_WARN("cast to ob time failed", K(ret), K(type), K(datum));
    } else {
      ot = ot2;
    }
  }
  return ret;
}

int ObExprTimeBase::calc(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum,
                        int32_t type, bool with_date, bool is_allow_incomplete_dates /* false */)
{
  int ret = OB_SUCCESS;
  ObDatum *param_datum = NULL;
  const ObSQLSessionInfo *session = NULL;
  ObSolidifiedVarsGetter helper(expr, ctx, ctx.exec_ctx_.get_my_session());
  ObSQLMode sql_mode = 0;
  const ObTimeZoneInfo *tz_info = NULL;
  if (OB_ISNULL(session = ctx.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (OB_FAIL(expr.args_[0]->eval(ctx, param_datum))) {
    LOG_WARN("eval param value failed");
  } else if (OB_FAIL(helper.get_sql_mode(sql_mode))) {
    LOG_WARN("get sql mode failed", K(ret));
  } else if (OB_FAIL(helper.get_time_zone_info(tz_info))) {
    LOG_WARN("failed to get time zone info", K(ret));
  } else if (OB_UNLIKELY(param_datum->is_null())) {
    expr_datum.set_null();
  } else {
    ObTime ot;
    ObString locale_name;
    if (OB_FAIL(ob_expr_convert_to_time(*param_datum, expr.args_[0]->datum_meta_.type_,
                                        expr.args_[0]->datum_meta_.scale_, with_date, is_allow_incomplete_dates,
                                        ctx, ot, expr.args_[0]->obj_meta_.has_lob_header(),
                                        tz_info, sql_mode))) {
      LOG_WARN("cast to ob time failed", K(ret), K(lbt()), K(session->get_stmt_type()));
      LOG_USER_WARN(OB_ERR_CAST_VARCHAR_TO_TIME);
      uint64_t cast_mode = 0;
      ObSQLUtils::get_default_cast_mode(session->get_stmt_type(),
                                        session->is_ignore_stmt(),
                                        sql_mode,
                                        cast_mode);
      if (CM_IS_WARN_ON_FAIL(cast_mode) || CM_IS_WARN_ON_FAIL(expr.args_[0]->extra_)) {
        ret = OB_SUCCESS;
        expr_datum.set_null();
      }
    } else if (expr.type_ == T_FUN_SYS_DAY_NAME) {
      if(!ot.parts_[DT_YEAR] && !ot.parts_[DT_MON] && !ot.parts_[DT_MDAY]) {
        expr_datum.set_null();
      } else {
        int idx = ot.parts_[type] - 1;
        if (OB_LIKELY(0 <= idx  && idx < 7)) {
          if (OB_FAIL(session->get_locale_name(locale_name))) {
              LOG_WARN("failed to get locale time name", K(expr), K(expr_datum));
          } else {
            OB_LOCALE *ob_cur_locale = ob_locale_by_name(locale_name);
            OB_LOCALE_TYPE *locale_type = ob_cur_locale->day_names_;
            const char ** locale_daynames = locale_type->type_names_;
            const ObString &name = locale_daynames[idx];
            if (OB_FAIL(ObExprUtil::set_expr_ascii_result(expr, ctx, expr_datum, name))) {
              LOG_WARN("failed to exec set_expr_ascii_result", K(expr), K(ctx), K(expr_datum), K(name));
            }
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("the parameter idx should be within a reasonable range", K(idx));
        }
      }
    } else if (expr.type_ == T_FUN_SYS_MONTH_NAME) {
      if(!ot.parts_[DT_MON]) {
        expr_datum.set_null();
      } else {
        int idx = ot.parts_[type] - 1;
        if(OB_LIKELY(0 <= idx  && idx < 12)) {
          if (OB_FAIL(session->get_locale_name(locale_name))) {
              LOG_WARN("failed to get locale time name", K(expr), K(expr_datum));
          } else {
            OB_LOCALE *ob_cur_locale = ob_locale_by_name(locale_name);
            OB_LOCALE_TYPE *locale_type = ob_cur_locale->month_names_;
            const char ** locale_monthnames = locale_type->type_names_;
            const ObString &month_name = locale_monthnames[idx];
            if (OB_FAIL(ObExprUtil::set_expr_ascii_result(expr, ctx, expr_datum, month_name))) {
              LOG_WARN("failed to exec set_expr_ascii_result", K(expr), K(ctx), K(expr_datum), K(month_name));
            }
          }          
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("the parameter idx should be within a reasonable range", K(idx));
        }
      }
    } else if (with_date && !is_allow_incomplete_dates && ot.parts_[DT_DATE] + DAYS_FROM_ZERO_TO_BASE < 0) {
      expr_datum.set_null();
    } else {
      expr_datum.set_int32(ot.parts_[type]);
    }
  }
  return ret;
}

DEF_SET_LOCAL_SESSION_VARS(ObExprTimeBase, raw_expr) {
  int ret = OB_SUCCESS;
  if (is_mysql_mode()) {
    SET_LOCAL_SYSVAR_CAPACITY(2);
    EXPR_ADD_LOCAL_SYSVAR(SYS_VAR_SQL_MODE);
    EXPR_ADD_LOCAL_SYSVAR(SYS_VAR_TIME_ZONE);
  }
  return ret;
}

ObExprHour::ObExprHour(ObIAllocator &alloc)
    : ObExprTimeBase(alloc, DT_HOUR, T_FUN_SYS_HOUR, N_HOUR, VALID_FOR_GENERATED_COL) {};

ObExprHour::~ObExprHour() {}

int ObExprHour::calc_hour(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  return ObExprTimeBase::calc(expr, ctx, expr_datum, DT_HOUR, false);
}

ObExprMinute::ObExprMinute(ObIAllocator &alloc)
    : ObExprTimeBase(alloc, DT_MIN, T_FUN_SYS_MINUTE, N_MINUTE, VALID_FOR_GENERATED_COL) {};

ObExprMinute::~ObExprMinute() {}

int ObExprMinute::calc_minute(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  return ObExprTimeBase::calc(expr, ctx, expr_datum, DT_MIN, false);
}

ObExprSecond::ObExprSecond(ObIAllocator &alloc)
    : ObExprTimeBase(alloc, DT_SEC, T_FUN_SYS_SECOND, N_SECOND, VALID_FOR_GENERATED_COL) {};

ObExprSecond::~ObExprSecond() {}

int ObExprSecond::calc_second(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  return ObExprTimeBase::calc(expr, ctx, expr_datum, DT_SEC, false);
}

ObExprMicrosecond::ObExprMicrosecond(ObIAllocator &alloc)
    : ObExprTimeBase(alloc, DT_USEC, T_FUN_SYS_MICROSECOND, N_MICROSECOND, VALID_FOR_GENERATED_COL) {};

ObExprMicrosecond::~ObExprMicrosecond() {}

int ObExprMicrosecond::calc_microsecond(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  return ObExprTimeBase::calc(expr, ctx, expr_datum, DT_USEC, false);
}

ObExprYear::ObExprYear(ObIAllocator &alloc)
    : ObExprTimeBase(alloc, DT_YEAR, T_FUN_SYS_YEAR, N_YEAR, VALID_FOR_GENERATED_COL) {};

ObExprYear::~ObExprYear() {}

int ObExprYear::calc_year(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  return ObExprTimeBase::calc(expr, ctx, expr_datum, DT_YEAR, true, true);
}

ObExprMonth::ObExprMonth(ObIAllocator &alloc)
    : ObExprTimeBase(alloc, DT_MON, T_FUN_SYS_MONTH, N_MONTH, VALID_FOR_GENERATED_COL) {};

ObExprMonth::~ObExprMonth() {}

int ObExprMonth::calc_month(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  // 对于MONTH('2020-00-00')场景，为了完全兼容mysql，也需要容忍月和日为0
  return ObExprTimeBase::calc(expr, ctx, expr_datum, DT_MON, true, true);
}

ObExprMonthName::ObExprMonthName(ObIAllocator &alloc)
    : ObExprTimeBase(alloc, DT_MON_NAME, T_FUN_SYS_MONTH_NAME, N_MONTH_NAME, VALID_FOR_GENERATED_COL) {};

ObExprMonthName::~ObExprMonthName() {}

int ObExprMonthName::calc_month_name(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  // NOTE: the last param should be true otherwise '2020-09-00' will not work
  if (OB_FAIL(calc(expr, ctx, expr_datum, DT_MON, true, true))) {
    LOG_WARN("eval month in monthname failed", K(ret), K(expr));
  } 

  return ret;
}

int ObExprMonthName::calc_result_type1(ObExprResType &type,
                                       ObExprResType &type1,
                                       common::ObExprTypeCtx &type_ctx) const
{
  ObCollationType cs_type = type_ctx.get_coll_type();
  type.set_varchar();
  type.set_collation_type(cs_type);
  type.set_collation_level(CS_LEVEL_IMPLICIT);
  type.set_full_length(MONTHNAME_MAX_LENGTH, type1.get_length_semantics());
  common::ObObjTypeClass tc1 = ob_obj_type_class(type1.get_type());
  if (common::ObEnumSetTC == tc1) {
    type1.set_calc_type(common::ObVarcharType);
    type1.set_collation_type(cs_type);
    type1.set_collation_level(CS_LEVEL_IMPLICIT);
  } else if ((common::ObFloatTC == tc1) || (common::ObDoubleTC == tc1)) {
    type1.set_calc_type(common::ObIntType);
  }

  return OB_SUCCESS;
}

#define CHECK_SKIP_NULL(idx) {                 \
  if (skip.at(idx) || eval_flags.at(idx)) {    \
    continue;                                  \
  } else if (arg_vec->is_null(idx)) {          \
    res_vec->set_null(idx);                    \
    continue;                                  \
  }                                            \
}

#define BATCH_CALC(BODY) {                                                        \
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

// Common vector function for date/time parts extraction (YEAR, MONTH, HOUR, etc.)
// DT_PART specifies which part to extract: DT_YEAR, DT_MON, DT_HOUR, etc.
template <typename ArgVec, typename ResVec, typename IN_TYPE, int DT_PART>
int ObExprTimeBase::calc_for_date_vector(const ObExpr &expr, ObEvalCtx &ctx,
                         const ObBitVector &skip, const EvalBound &bound)
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
    const ObTimeZoneInfo *local_tz_info = (ObTimestampType == expr.args_[0]->datum_meta_.type_)
                                            ? tz_info : NULL;
    if (OB_FAIL(get_tz_offset(local_tz_info, tz_offset))) {
      LOG_WARN("get tz_info offset fail", K(ret));
    } else {
      DateType days = 0;
      YearType year = 0;
      DateType dt_yday = 0;
      DateType dt_mday = 0;
      MonthType month = 0;
      UsecType usec = 0;
      bool no_skip_no_null = bound.get_all_rows_active() && !arg_vec->has_null()
                             && eval_flags.accumulate_bit_cnt(bound) == 0;
      ObTime ob_time;
      BATCH_CALC({
        IN_TYPE in_val = *reinterpret_cast<const IN_TYPE*>(arg_vec->get_payload(idx));
         if (std::is_same<IN_TYPE, ObMySQLDate>::value || std::is_same<IN_TYPE, ObMySQLDateTime>::value) {
          ret = ObTimeConverter::parse_ob_time<IN_TYPE>(in_val, ob_time);
          if (OB_FAIL(ret)) {
            LOG_WARN("parse ob_time failed", K(ret));
            res_vec->set_null(idx);
          } else if (DT_PART < DT_USEC) {
            res_vec->set_int(idx, ob_time.parts_[DT_PART]);
          } else if (DT_PART == DT_QUARTER) {
            if (ob_time.parts_[DT_YEAR] <= 0 || ob_time.parts_[DT_MON] <= 0) {
              res_vec->set_null(idx);
            } else {
              int32_t quarter = (ob_time.parts_[DT_MON] + 2) / 3;
              res_vec->set_int(idx, quarter);
            }
          }
        } else if (OB_FAIL(ObTimeConverter::parse_date_usec<IN_TYPE>(in_val, tz_offset, lib::is_oracle_mode(), days, usec))) {
          LOG_WARN("get date and usec from vec failed", K(ret));
        } else {
          if (DT_PART < DT_USEC) {
            if (OB_UNLIKELY(ObTimeConverter::ZERO_DATE == days)) {
              res_vec->set_int(idx, 0);
            } else if (DT_PART == DT_YEAR) {
              if (ObTimeConverter::could_calc_days_to_year_quickly(days)) {
                ObTimeConverter::days_to_year(days, year);
              } else {
                ret = ObTimeConverter::date_to_ob_time(days, ob_time);
                year = ob_time.parts_[DT_YEAR];
              }
              res_vec->set_int(idx, year);
            } else if (DT_PART == DT_MON) {
              if (ObTimeConverter::could_calc_days_to_year_quickly(days)) {
                ObTimeConverter::days_to_year_ydays(days, year, dt_yday);
                ObTimeConverter::ydays_to_month_mdays(year, dt_yday, month, dt_mday);
              } else {
                ret = ObTimeConverter::date_to_ob_time(days, ob_time);
                month = ob_time.parts_[DT_MON];
              }
              res_vec->set_int(idx, month);
            } else if (DT_PART == DT_HOUR) {
              res_vec->set_int(idx, static_cast<int32_t>(usec / 3600000000));
            } else if (DT_PART == DT_MIN) {
              res_vec->set_int(idx, static_cast<int32_t>(usec % 3600000000 / 60000000));
            }
          } else if (DT_PART == DT_QUARTER) {
            if (OB_UNLIKELY(ObTimeConverter::ZERO_DATE == days)) {
              res_vec->set_null(idx);
            } else if (OB_FAIL(ObTimeConverter::date_to_ob_time(days, ob_time))) {
              LOG_WARN("failed to convert date to ob_time", K(ret));
            } else if (OB_FAIL(ObTimeConverter::time_to_ob_time(usec, ob_time))) {
              LOG_WARN("failed to convert time to ob_time", K(ret));
            } else {
              if (ob_time.parts_[DT_YEAR] <= 0 || ob_time.parts_[DT_MON] <= 0) {
                res_vec->set_null(idx);
              } else {
                int32_t quarter = (ob_time.parts_[DT_MON] + 2) / 3;
                res_vec->set_int(idx, quarter);
              }
            }
          }
        }
      });
    }
  }
  return ret;
}

template <typename ArgVec, typename ResVec, typename IN_TYPE>
int vector_year(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
{
  return ObExprTimeBase::calc_for_date_vector<ArgVec, ResVec, IN_TYPE, DT_YEAR>(expr, ctx, skip, bound);
}

template <typename ArgVec, typename ResVec, typename IN_TYPE>
int vector_month(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
{
  return ObExprTimeBase::calc_for_date_vector<ArgVec, ResVec, IN_TYPE, DT_MON>(expr, ctx, skip, bound);
}

template <typename ArgVec, typename ResVec, typename IN_TYPE>
int vector_hour(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
{
  return ObExprTimeBase::calc_for_date_vector<ArgVec, ResVec, IN_TYPE, DT_HOUR>(expr, ctx, skip, bound);
}

#define DISPATCH_TIME_EXPR_VECTOR(FUNC, TYPE)\
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
} else { \
  ret = FUNC<ObVectorBase, ObVectorBase, CONCAT(TYPE, Type)>(expr, ctx, skip, bound);\
}

template <typename ArgVec, typename ResVec, bool NeedCalcDate>
int ObExprTimeBase::calc_for_string_vector(const ObExpr &expr, ObEvalCtx &ctx,
    int32_t type, const ObBitVector &skip, const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  ArgVec *arg_vec = static_cast<ArgVec *>(expr.args_[0]->get_vector(ctx));
  ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  bool no_skip_no_null = bound.get_all_rows_active() && !arg_vec->has_null()
                          && eval_flags.accumulate_bit_cnt(bound) == 0;
  const ObSQLSessionInfo *session = NULL;
  ObSolidifiedVarsGetter helper(expr, ctx, ctx.exec_ctx_.get_my_session());
  ObSQLMode sql_mode = 0;
  ObDateSqlMode date_sql_mode;
  if (OB_ISNULL(session = ctx.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (OB_FAIL(helper.get_sql_mode(sql_mode))) {
    LOG_WARN("get sql mode failed", K(ret));
  } else {
    int64_t last_first_8digits = INT64_MAX;
    bool with_date = (type <= DT_MDAY || type > DT_USEC) || type == DT_QUARTER;
    bool is_time_type = type >= DT_HOUR && type <= DT_USEC;
    bool use_quick_parser = true;
    bool is_allow_incomplete_dates = type <= DT_MDAY;
    int quick_parser_failed_count = 0;
    ObTime ob_time(with_date ? 0 : DT_TYPE_TIME);
    ObScale res_scale = -1;
    ObTimeConverter::ObTimeDigits digits[DATETIME_PART_CNT];
    ObTimeConverter::QuickParserType last_quick_parser_type = ObTimeConverter::QuickParserType::QuickParserUnused;
    date_sql_mode.init(sql_mode);
    date_sql_mode.allow_incomplete_dates_ = is_allow_incomplete_dates;
    BATCH_CALC({
      ObString in_val = arg_vec->get_string(idx); // String
      bool datetime_valid = false;
      bool is_match_format = false;
      if (use_quick_parser) {
        ObTimeConverter::string_to_obtime_quick(in_val.ptr(),
                  in_val.length(),
                  ob_time,
                  datetime_valid,
                  is_match_format,
                  last_first_8digits,
                  last_quick_parser_type,
                  is_time_type);
      }
      if (datetime_valid && is_match_format) {
        if (NeedCalcDate) {
          // for dayof expr, its allow_incomplete_dates_ is false, must calc Date part
          // but quick parser has not calc DT_DATE part
          ob_time.parts_[DT_DATE] = ObTimeConverter::ob_time_to_date(ob_time);
          if (ob_time.parts_[DT_DATE] + DAYS_FROM_ZERO_TO_BASE < 0) {
            res_vec->set_null(idx);
          } else {
            res_vec->set_int(idx, type == DT_WDAY ? ob_time.parts_[type] % 7 + 1 : ob_time.parts_[type]);
          }
        } else {
          if (type == DT_QUARTER) {
            // Calculate quarter from month: (month + 2) / 3
            // Check year <= 0 to match non-vectorized behavior
            if (ob_time.parts_[DT_YEAR] <= 0 || ob_time.parts_[DT_MON] <= 0) {
              res_vec->set_null(idx);
            } else {
              int32_t quarter = (ob_time.parts_[DT_MON] + 2) / 3;
              res_vec->set_int(idx, quarter);
            }
          } else {
            res_vec->set_int(idx, ob_time.parts_[type]);
          }
        }
      } else {
        last_quick_parser_type = ObTimeConverter::QuickParserType::QuickParserUnused;
        if (use_quick_parser) {
          quick_parser_failed_count++;
          if (quick_parser_failed_count % 32 == 0) {
            if (quick_parser_failed_count * 2 > idx) {
              use_quick_parser = false;
            }
          }
        }
        if (with_date) {
          if (OB_FAIL(ObTimeConverter::str_to_ob_time_with_date(in_val, ob_time,
              &res_scale, date_sql_mode, false, digits))) {
            LOG_WARN("cast to ob time with date failed", K(ret), K(in_val));
          }
        } else {
          ob_time.mode_ = DT_TYPE_TIME; // reset mode in old way for time only extract expr
          if (OB_FAIL(ObTimeConverter::str_to_ob_time_without_date(in_val, ob_time, NULL, false, digits))) {
            LOG_WARN("cast to ob time without date failed", K(ret), K(in_val));
          } else {
            int64_t value = ObTimeConverter::ob_time_to_time(ob_time);
            int64_t tmp_value = value;
            ObTimeConverter::time_overflow_trunc(value);
            if (value != tmp_value) {
              ObTimeConverter::time_to_ob_time(value, ob_time);
            }
          }
        }
        if (OB_FAIL(ret)) {
          LOG_WARN("cast to ob time failed", K(ret), K(in_val));
          // For QUARTER, do not log user warning to align with scalar path behavior
          if (type != DT_QUARTER) {
            LOG_USER_WARN(OB_ERR_CAST_VARCHAR_TO_TIME);
          }
          uint64_t cast_mode = 0;
          ObSQLUtils::get_default_cast_mode(session->get_stmt_type(),
                                            session->is_ignore_stmt(),
                                            sql_mode,
                                            cast_mode);
          if (CM_IS_WARN_ON_FAIL(cast_mode) || CM_IS_WARN_ON_FAIL(expr.args_[0]->extra_)) {
            ret = OB_SUCCESS;
            res_vec->set_null(idx);
          }
        } else {
          if (NeedCalcDate) {
            // for dayof expr, which allow_incomplete_dates_ is false, must calc Date part
            ob_time.parts_[DT_DATE] = ObTimeConverter::ob_time_to_date(ob_time);
            if (ob_time.parts_[DT_DATE] + DAYS_FROM_ZERO_TO_BASE < 0) {
              res_vec->set_null(idx);
            } else {
              res_vec->set_int(idx, type == DT_WDAY ? ob_time.parts_[type] % 7 + 1 : ob_time.parts_[type]);
            }
          } else {
            if (type == DT_QUARTER) {
              // Calculate quarter from month: (month + 2) / 3
              // Check year <= 0 to match non-vectorized behavior
              if (ob_time.parts_[DT_YEAR] <= 0 || ob_time.parts_[DT_MON] <= 0) {
                res_vec->set_null(idx);
              } else {
                int32_t quarter = (ob_time.parts_[DT_MON] + 2) / 3;
                res_vec->set_int(idx, quarter);
              }
            } else {
              res_vec->set_int(idx, ob_time.parts_[type]);
            }
          }
        }
      }
    });
  }
  return ret;
}

#define SET_MONTH_DAY_NAME()\
ObString name;\
bool is_null = false;\
if (expr.type_ == T_FUN_SYS_DAY_NAME) {\
  if(!ob_time.parts_[DT_YEAR] && !ob_time.parts_[DT_MON] && !ob_time.parts_[DT_MDAY]) {\
    res_vec->set_null(idx);\
    is_null = true;\
  } else {\
    int idx = ob_time.parts_[type] - 1;\
    if (0 <= idx  && idx < 7) {\
      name = locale_daynames[idx];\
    } else {\
      ret = OB_ERR_UNEXPECTED;\
      LOG_WARN("the parameter idx should be within a reasonable range", K(idx));\
    }\
  }\
} else if (expr.type_ == T_FUN_SYS_MONTH_NAME) {\
  if(!ob_time.parts_[DT_MON]) {\
    res_vec->set_null(idx);\
    is_null = true;\
  } else {\
    int idx = ob_time.parts_[type] - 1;\
    if (0 <= idx  && idx < 12) {\
      name = locale_monthnames[idx];\
    } else {\
      ret = OB_ERR_UNEXPECTED;\
      LOG_WARN("the parameter idx should be within a reasonable range", K(idx));\
    }\
  }\
}\
if (OB_SUCC(ret) && !is_null) {\
  if (name.empty()) {\
    if (lib::is_oracle_mode()) {\
      res_vec->set_null(idx);\
    } else {\
      res_vec->set_string(idx, ObString());\
    }\
  } else {\
    ObString out;\
    char *buf = NULL;\
    if (!need_charset_convert) {\
      out = name;\
    } else if (OB_FAIL(ObCharset::charset_convert(temp_allocator, name, CS_TYPE_UTF8MB4_BIN,\
                                          expr.datum_meta_.cs_type_, out))) {\
      LOG_WARN("charset convert failed", K(ret));\
    }\
    if (OB_FAIL(ret)) {\
    } else if (OB_ISNULL(buf = expr.get_str_res_mem(ctx, out.length(), idx))) {\
      ret = OB_ALLOCATE_MEMORY_FAILED;\
      LOG_WARN("allocate memory failed", K(ret), K(out.length()));\
    } else {\
      MEMCPY(buf, out.ptr(), out.length());\
      res_vec->set_string(idx, buf, out.length());\
    }\
  }\
}

// calc_name_for_string_vector is only for T_FUN_SYS_DAY_NAME and T_FUN_SYS_MONTH_NAME
template <typename ArgVec, typename ResVec>
int ObExprTimeBase::calc_name_for_string_vector(const ObExpr &expr, ObEvalCtx &ctx,
    int32_t type, const ObBitVector &skip, const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  ArgVec *arg_vec = static_cast<ArgVec *>(expr.args_[0]->get_vector(ctx));
  ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  bool no_skip_no_null = bound.get_all_rows_active() && !arg_vec->has_null()
                          && eval_flags.accumulate_bit_cnt(bound) == 0;
  const ObSQLSessionInfo *session = NULL;
  ObSolidifiedVarsGetter helper(expr, ctx, ctx.exec_ctx_.get_my_session());
  ObSQLMode sql_mode = 0;
  ObDateSqlMode date_sql_mode;
  if (OB_ISNULL(session = ctx.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (OB_FAIL(helper.get_sql_mode(sql_mode))) {
    LOG_WARN("get sql mode failed", K(ret));
  } else {
    int64_t last_first_8digits = INT64_MAX;
    bool use_quick_parser = true;
    int quick_parser_failed_count = 0;
    ObTime ob_time;
    ObScale res_scale = -1;
    ObTimeConverter::ObTimeDigits digits[DATETIME_PART_CNT];
    ObTimeConverter::QuickParserType last_quick_parser_type = ObTimeConverter::QuickParserType::QuickParserUnused;
    ObString locale_name;
    OB_LOCALE *ob_cur_locale = NULL;
    OB_LOCALE_TYPE *locale_type = NULL;
    const char ** locale_monthnames = NULL;
    const char ** locale_daynames = NULL;
    ObArenaAllocator temp_allocator;
    bool need_charset_convert = ObCharset::is_cs_nonascii(expr.datum_meta_.cs_type_);
    date_sql_mode.init(sql_mode);
    date_sql_mode.allow_incomplete_dates_ = (expr.type_ == T_FUN_SYS_MONTH_NAME);

    if (OB_FAIL(session->get_locale_name(locale_name))) {
      LOG_WARN("failed to get locale time name", K(expr));
    } else {
      OB_LOCALE *ob_cur_locale = ob_locale_by_name(locale_name);
      if (expr.type_ == T_FUN_SYS_DAY_NAME) {
        locale_type = ob_cur_locale->day_names_;
        locale_daynames = locale_type->type_names_;
      } else if (expr.type_ == T_FUN_SYS_MONTH_NAME) {
        OB_LOCALE_TYPE *locale_type = ob_cur_locale->month_names_;
        locale_monthnames = locale_type->type_names_;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected func", K(expr.type_));
      }
    }
    BATCH_CALC({
      ObString in_val = arg_vec->get_string(idx); // String
      bool datetime_valid = false;
      bool is_match_format = false;
      if (use_quick_parser) {
        ObTimeConverter::string_to_obtime_quick(in_val.ptr(),
                  in_val.length(),
                  ob_time,
                  datetime_valid,
                  is_match_format,
                  last_first_8digits,
                  last_quick_parser_type,
                  false);
      }
      if (datetime_valid && is_match_format) {
        ob_time.parts_[DT_DATE] = ObTimeConverter::ob_time_to_date(ob_time);
      } else {
        last_quick_parser_type = ObTimeConverter::QuickParserType::QuickParserUnused;
        if (use_quick_parser) {
          quick_parser_failed_count++;
          if (quick_parser_failed_count % 32 == 0) {
            if (quick_parser_failed_count * 2 > idx) {
              use_quick_parser = false;
            }
          }
        }
        if (OB_FAIL(ObTimeConverter::str_to_ob_time_with_date(in_val, ob_time,
            &res_scale, date_sql_mode, false, digits))) {
          LOG_WARN("cast to ob time with date failed", K(ret), K(in_val));
        }
      }
      if (OB_FAIL(ret)) {
        LOG_WARN("cast to ob time failed", K(ret), K(in_val));
        LOG_USER_WARN(OB_ERR_CAST_VARCHAR_TO_TIME);
        uint64_t cast_mode = 0;
        ObSQLUtils::get_default_cast_mode(session->get_stmt_type(),
                                          session->is_ignore_stmt(),
                                          sql_mode,
                                          cast_mode);
        if (CM_IS_WARN_ON_FAIL(cast_mode) || CM_IS_WARN_ON_FAIL(expr.args_[0]->extra_)) {
          ret = OB_SUCCESS;
          res_vec->set_null(idx);
        }
      } else {
        SET_MONTH_DAY_NAME();
      }
    });
  }
  return ret;
}

template <typename ArgVec, typename ResVec>
int ObExprTimeBase::calc_for_integer_vector(const ObExpr &expr, ObEvalCtx &ctx,
    int32_t type, const ObBitVector &skip, const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  ArgVec *arg_vec = static_cast<ArgVec *>(expr.args_[0]->get_vector(ctx));
  ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  bool no_skip_no_null = bound.get_all_rows_active() && !arg_vec->has_null()
                          && eval_flags.accumulate_bit_cnt(bound) == 0;
  const ObSQLSessionInfo *session = NULL;
  ObSolidifiedVarsGetter helper(expr, ctx, ctx.exec_ctx_.get_my_session());
  ObSQLMode sql_mode = 0;
  ObDateSqlMode date_sql_mode;
  if (OB_ISNULL(session = ctx.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (OB_FAIL(helper.get_sql_mode(sql_mode))) {
    LOG_WARN("get sql mode failed", K(ret));
  } else {
    bool with_date = (type <= DT_MDAY || type > DT_USEC) || type == DT_QUARTER;
    bool is_allow_incomplete_dates = type <= DT_MDAY;
    ObTime ob_time(with_date ? 0 : DT_TYPE_TIME);
    ObTimeConverter::ObTimeDigits digits[DATETIME_PART_CNT];
    date_sql_mode.init(sql_mode);
    date_sql_mode.allow_incomplete_dates_ = is_allow_incomplete_dates;
    BATCH_CALC({
      int64_t in_val = arg_vec->get_int(idx);
      if (with_date) {
        ret = ObTimeConverter::int_to_ob_time_with_date(in_val, ob_time, date_sql_mode);
      } else {
        if (OB_FAIL(ObTimeConverter::int_to_ob_time_without_date(in_val, ob_time))) {
          LOG_WARN("int to ob time without date failed", K(ret));
        } else if (ob_time.parts_[DT_HOUR] > TIME_MAX_HOUR) {
          ret = OB_INVALID_DATE_VALUE;
        }
      }
      if (OB_FAIL(ret)) {
        LOG_WARN("cast to ob time failed", K(ret), K(lbt()), K(session->get_stmt_type()));
        // For QUARTER, do not log user warning to align with scalar path behavior
        if (type != DT_QUARTER) {
          LOG_USER_WARN(OB_ERR_CAST_VARCHAR_TO_TIME);
        }
        uint64_t cast_mode = 0;
        ObSQLUtils::get_default_cast_mode(session->get_stmt_type(),
                                          session->is_ignore_stmt(),
                                          sql_mode,
                                          cast_mode);
        if (CM_IS_WARN_ON_FAIL(cast_mode) || CM_IS_WARN_ON_FAIL(expr.args_[0]->extra_)) {
          ret = OB_SUCCESS;
          res_vec->set_null(idx);
        }
      } else if (with_date && !is_allow_incomplete_dates && ob_time.parts_[DT_DATE] + DAYS_FROM_ZERO_TO_BASE < 0) {
        res_vec->set_null(idx);
      } else if (type == DT_QUARTER) {
        // Calculate quarter from month: (month + 2) / 3
        // Check year <= 0 to match non-vectorized behavior
        if (ob_time.parts_[DT_YEAR] <= 0 || ob_time.parts_[DT_MON] <= 0) {
          res_vec->set_null(idx);
        } else {
          int32_t quarter = (ob_time.parts_[DT_MON] + 2) / 3;
          res_vec->set_int(idx, quarter);
        }
      } else {
        res_vec->set_int(idx, type == DT_WDAY ? ob_time.parts_[type] % 7 + 1 : ob_time.parts_[type]);
      }
    });
  }
  return ret;
}

// calc_name_for_string_vector is only for T_FUN_SYS_DAY_NAME and T_FUN_SYS_MONTH_NAME
template <typename ArgVec, typename ResVec>
int ObExprTimeBase::calc_name_for_integer_vector(const ObExpr &expr, ObEvalCtx &ctx,
    int32_t type, const ObBitVector &skip, const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  ArgVec *arg_vec = static_cast<ArgVec *>(expr.args_[0]->get_vector(ctx));
  ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  bool no_skip_no_null = bound.get_all_rows_active() && !arg_vec->has_null()
                          && eval_flags.accumulate_bit_cnt(bound) == 0;
  const ObSQLSessionInfo *session = NULL;
  ObSolidifiedVarsGetter helper(expr, ctx, ctx.exec_ctx_.get_my_session());
  ObSQLMode sql_mode = 0;
  ObDateSqlMode date_sql_mode;
  if (OB_ISNULL(session = ctx.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (OB_FAIL(helper.get_sql_mode(sql_mode))) {
    LOG_WARN("get sql mode failed", K(ret));
  } else {
    ObTime ob_time;
    ObScale res_scale = -1;
    ObString locale_name;
    OB_LOCALE *ob_cur_locale = NULL;
    OB_LOCALE_TYPE *locale_type = NULL;
    const char ** locale_monthnames = NULL;
    const char ** locale_daynames = NULL;
    ObArenaAllocator temp_allocator;
    bool need_charset_convert = ObCharset::is_cs_nonascii(expr.datum_meta_.cs_type_);
    date_sql_mode.init(sql_mode);
    date_sql_mode.allow_incomplete_dates_ = (expr.type_ == T_FUN_SYS_MONTH_NAME);

    if (OB_FAIL(session->get_locale_name(locale_name))) {
      LOG_WARN("failed to get locale time name", K(expr));
    } else {
      OB_LOCALE *ob_cur_locale = ob_locale_by_name(locale_name);
      if (expr.type_ == T_FUN_SYS_DAY_NAME) {
        locale_type = ob_cur_locale->day_names_;
        locale_daynames = locale_type->type_names_;
      } else if (expr.type_ == T_FUN_SYS_MONTH_NAME) {
        OB_LOCALE_TYPE *locale_type = ob_cur_locale->month_names_;
        locale_monthnames = locale_type->type_names_;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected func", K(expr.type_));
      }
    }
    if (OB_SUCC(ret)) {
      BATCH_CALC({
        int64_t in_val = arg_vec->get_int(idx);
        if (OB_FAIL(ObTimeConverter::int_to_ob_time_with_date(in_val, ob_time, date_sql_mode))) {
          LOG_WARN("cast to ob time failed", K(ret), K(lbt()), K(session->get_stmt_type()));
          LOG_USER_WARN(OB_ERR_CAST_VARCHAR_TO_TIME);
          uint64_t cast_mode = 0;
          ObSQLUtils::get_default_cast_mode(session->get_stmt_type(),
                                            session->is_ignore_stmt(),
                                            sql_mode,
                                            cast_mode);
          if (CM_IS_WARN_ON_FAIL(cast_mode) || CM_IS_WARN_ON_FAIL(expr.args_[0]->extra_)) {
            ret = OB_SUCCESS;
            res_vec->set_null(idx);
          }
        } else {
          SET_MONTH_DAY_NAME();
        }
      });
    }
  }
  return ret;
}

int ObExprYear::calc_year_vector(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("fail to eval date_format param", K(ret));
  } else {
    VectorFormat arg_format = expr.args_[0]->get_format(ctx);
    VectorFormat res_format = expr.get_format(ctx);
    const ObObjType arg_type = expr.args_[0]->datum_meta_.type_;
    // VEC_TC_INTEGER == arg_vec_tc
    if (ObMySQLDateTC == ob_obj_type_class(arg_type)) {
      DISPATCH_TIME_EXPR_VECTOR(vector_year, MySQLDate);
    } else if (ObMySQLDateTimeTC == ob_obj_type_class(arg_type)) {
      DISPATCH_TIME_EXPR_VECTOR(vector_year, MySQLDateTime);
    } else if (ObDateTC == ob_obj_type_class(arg_type)) {
      DISPATCH_TIME_EXPR_VECTOR(vector_year, Date);
    } else if (ObDateTimeTC == ob_obj_type_class(arg_type)) {
      DISPATCH_TIME_EXPR_VECTOR(vector_year, DateTime)
    } else if (ObStringTC == ob_obj_type_class(arg_type)) {
      DISPATCH_STRING_CALC_EXPR_VECTOR(ObExprTimeBase::calc_for_string_vector, DT_YEAR, false);
    } else if (ob_is_int_uint_tc(arg_type)) {
      DISPATCH_INTEGER_CALC_EXPR_VECTOR(ObExprTimeBase::calc_for_integer_vector, DT_YEAR);
    }

    if (OB_FAIL(ret)) {
      LOG_WARN("expr calculation failed", K(ret));
    }
  }
  return ret;
}

int ObExprMonth::calc_month_vector(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("fail to eval date_format param", K(ret));
  } else {
    VectorFormat arg_format = expr.args_[0]->get_format(ctx);
    VectorFormat res_format = expr.get_format(ctx);
    const ObObjType arg_type = expr.args_[0]->datum_meta_.type_;
    if (ObMySQLDateTC == ob_obj_type_class(arg_type)) {
      DISPATCH_TIME_EXPR_VECTOR(vector_month, MySQLDate);
    } else if (ObMySQLDateTimeTC == ob_obj_type_class(arg_type)) {
      DISPATCH_TIME_EXPR_VECTOR(vector_month, MySQLDateTime);
    } else if (ObDateTC == ob_obj_type_class(arg_type)) {
      DISPATCH_TIME_EXPR_VECTOR(vector_month, Date);
    } else if (ObDateTimeTC == ob_obj_type_class(arg_type)) {
      DISPATCH_TIME_EXPR_VECTOR(vector_month, DateTime);
    } else if (ObStringTC == ob_obj_type_class(arg_type)) {
      DISPATCH_STRING_CALC_EXPR_VECTOR(ObExprTimeBase::calc_for_string_vector, DT_MON, false);
    } else if (ob_is_int_uint_tc(arg_type)) {
      DISPATCH_INTEGER_CALC_EXPR_VECTOR(ObExprTimeBase::calc_for_integer_vector, DT_MON);
    }

    if (OB_FAIL(ret)) {
      LOG_WARN("expr calculation failed", K(ret));
    }
  }
  return ret;
}

template <typename ArgVec, typename ResVec, typename IN_TYPE>
int vector_month_name(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  ArgVec *arg_vec = static_cast<ArgVec *>(expr.args_[0]->get_vector(ctx));
  ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  int64_t tz_offset = 0;
  ObString locale_name;
  const common::ObTimeZoneInfo *tz_info = NULL;
  const ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  ObSolidifiedVarsGetter helper(expr, ctx, ctx.exec_ctx_.get_my_session());
  if (OB_UNLIKELY(eval_flags.is_all_true(bound.start(), bound.end()))) {
  } else if (OB_FAIL(session->get_locale_name(locale_name))) {
    LOG_WARN("failed to get locale time name", K(expr));
  } else if (OB_FAIL(helper.get_time_zone_info(tz_info))) {
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
      OB_LOCALE *ob_cur_locale = ob_locale_by_name(locale_name);
      OB_LOCALE_TYPE *locale_type_mon = ob_cur_locale->month_names_;
      const char ** locale_monthnames = locale_type_mon->type_names_;
      const char *const *month_name = nullptr;
      if (lib::is_mysql_mode()) {
        month_name = locale_monthnames;
      } else {
        month_name = &(MON_NAMES+1)->ptr_;
      }
      ObTime ob_time;
      BATCH_CALC({
        IN_TYPE in_val = *reinterpret_cast<const IN_TYPE*>(arg_vec->get_payload(idx));
        if (std::is_same<IN_TYPE, ObMySQLDate>::value || std::is_same<IN_TYPE, ObMySQLDateTime>::value) {
          ret = ObTimeConverter::parse_ob_time<IN_TYPE>(in_val, ob_time);
          month = ob_time.parts_[DT_MON];
        } else if (OB_FAIL(ObTimeConverter::parse_date_usec<IN_TYPE>(in_val, tz_offset, lib::is_oracle_mode(), date, usec))) {
          LOG_WARN("get date and usec from vec failed", K(ret));
        } else if (OB_UNLIKELY(ObTimeConverter::ZERO_DATE == date)) {
          month = 0;
        } else if (ObTimeConverter::could_calc_days_to_year_quickly(date)) {
          ObTimeConverter::days_to_year_ydays(date, year, dt_yday);
          ObTimeConverter::ydays_to_month_mdays(year, dt_yday, month, dt_mday);
        } else if (OB_FAIL(ObTimeConverter::date_to_ob_time(date, ob_time))) {
          LOG_WARN("failed date_to_ob_time", K(ret), K(date));
        } else {
          month = ob_time.parts_[DT_MON];
        }

        if (month == 0) {
          res_vec->set_null(idx);
        } else if (OB_UNLIKELY(month < 1 || month > 12)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("the parameter month should be within a reasonable range", K(month));
        } else {
          size_t len = strlen(month_name[month-1]);
          res_vec->set_string(idx, ObString(len, month_name[month-1]));
        }
      });
    }
  }
  return ret;
}

#define DISPATCH_YEAR_MON_NAME_EXPR_VECTOR(TYPE)\
if (VEC_FIXED == arg_format && VEC_DISCRETE == res_format) {\
  ret = vector_month_name<CONCAT(TYPE,FixedVec), StrDiscVec, CONCAT(TYPE,Type)>(expr, ctx, skip, bound);\
} else if (VEC_FIXED == arg_format && VEC_UNIFORM == res_format) {\
  ret = vector_month_name<CONCAT(TYPE,FixedVec), StrUniVec, CONCAT(TYPE,Type)>(expr, ctx, skip, bound);\
} else if (VEC_FIXED == arg_format && VEC_UNIFORM_CONST == res_format) {\
  ret = vector_month_name<CONCAT(TYPE,FixedVec), StrUniCVec, CONCAT(TYPE,Type)>(expr, ctx, skip, bound);\
} else if (VEC_FIXED == arg_format && VEC_CONTINUOUS == res_format) {\
  ret = vector_month_name<CONCAT(TYPE,FixedVec), StrContVec, CONCAT(TYPE,Type)>(expr, ctx, skip, bound);\
} else if (VEC_UNIFORM == arg_format && VEC_DISCRETE == res_format) {\
  ret = vector_month_name<CONCAT(TYPE,UniVec), StrDiscVec, CONCAT(TYPE,Type)>(expr, ctx, skip, bound);\
} else if (VEC_UNIFORM == arg_format && VEC_UNIFORM == res_format) {\
  ret = vector_month_name<CONCAT(TYPE,UniVec), StrUniVec, CONCAT(TYPE,Type)>(expr, ctx, skip, bound);\
} else if (VEC_UNIFORM == arg_format && VEC_UNIFORM_CONST == res_format) {\
  ret = vector_month_name<CONCAT(TYPE,UniVec), StrUniCVec, CONCAT(TYPE,Type)>(expr, ctx, skip, bound);\
} else if (VEC_UNIFORM == arg_format && VEC_CONTINUOUS == res_format) {\
  ret = vector_month_name<CONCAT(TYPE,UniVec), StrContVec, CONCAT(TYPE,Type)>(expr, ctx, skip, bound);\
} else if (VEC_UNIFORM_CONST == arg_format && VEC_DISCRETE == res_format) {\
  ret = vector_month_name<CONCAT(TYPE,UniCVec), StrDiscVec, CONCAT(TYPE,Type)>(expr, ctx, skip, bound);\
} else if (VEC_UNIFORM_CONST == arg_format && VEC_UNIFORM == res_format) {\
  ret = vector_month_name<CONCAT(TYPE,UniCVec), StrUniVec, CONCAT(TYPE,Type)>(expr, ctx, skip, bound);\
} else if (VEC_UNIFORM_CONST == arg_format && VEC_UNIFORM_CONST == res_format) {\
  ret = vector_month_name<CONCAT(TYPE,UniCVec), StrUniCVec, CONCAT(TYPE,Type)>(expr, ctx, skip, bound);\
} else if (VEC_UNIFORM_CONST == arg_format && VEC_CONTINUOUS == res_format) {\
  ret = vector_month_name<CONCAT(TYPE,UniCVec), StrContVec, CONCAT(TYPE,Type)>(expr, ctx, skip, bound);\
} else {\
  ret = vector_month_name<ObVectorBase, ObVectorBase, CONCAT(TYPE,Type)>(expr, ctx, skip, bound);\
}
int ObExprMonthName::calc_month_name_vector(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  const ObSQLSessionInfo *session = NULL;
  if (OB_ISNULL(session = ctx.exec_ctx_.get_my_session())) {
    ret = OB_NOT_INIT;
    LOG_WARN("session is null", K(ret), K(session));
  } else if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("fail to eval date_format param", K(ret));
  } else {
    VectorFormat arg_format = expr.args_[0]->get_format(ctx);
    VectorFormat res_format = expr.get_format(ctx);
    const ObObjType arg_type = expr.args_[0]->datum_meta_.type_;

    if (ObMySQLDateTC == ob_obj_type_class(arg_type)) {
      DISPATCH_YEAR_MON_NAME_EXPR_VECTOR(MySQLDate);
    } else if (ObMySQLDateTimeTC == ob_obj_type_class(arg_type)) {
      DISPATCH_YEAR_MON_NAME_EXPR_VECTOR(MySQLDateTime);
    } else if (ObDateTC == ob_obj_type_class(arg_type)) {
      DISPATCH_YEAR_MON_NAME_EXPR_VECTOR(Date);
    } else if (ObDateTimeTC == ob_obj_type_class(arg_type)) {
      DISPATCH_YEAR_MON_NAME_EXPR_VECTOR(DateTime);
    } else if (ObStringTC == ob_obj_type_class(arg_type)) {
      DISPATCH_STRING_NAME_EXPR_VECTOR(ObExprTimeBase::calc_name_for_string_vector, DT_MON);
    } else if (ob_is_int_uint_tc(arg_type)) {
      DISPATCH_INTEGER_NAME_EXPR_VECTOR(ObExprTimeBase::calc_name_for_integer_vector, DT_MON);
    }

    if (OB_FAIL(ret)) {
      LOG_WARN("expr calculation failed", K(ret));
    }
  }
  return ret;
}

int ObExprHour::calc_hour_vector(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("fail to eval date_format param", K(ret));
  } else {
    VectorFormat arg_format = expr.args_[0]->get_format(ctx);
    VectorFormat res_format = expr.get_format(ctx);
    const ObObjType arg_type = expr.args_[0]->datum_meta_.type_;

    if (ObMySQLDateTC == ob_obj_type_class(arg_type)) {
      DISPATCH_TIME_EXPR_VECTOR(vector_hour, MySQLDate);
    } else if (ObMySQLDateTimeTC == ob_obj_type_class(arg_type)) {
      DISPATCH_TIME_EXPR_VECTOR(vector_hour, MySQLDateTime);
    } else if (ObDateTC == ob_obj_type_class(arg_type)) {
      DISPATCH_TIME_EXPR_VECTOR(vector_hour, Date);
    } else if (ObDateTimeTC == ob_obj_type_class(arg_type)) {
      DISPATCH_TIME_EXPR_VECTOR(vector_hour, DateTime);
    } else if (ObStringTC == ob_obj_type_class(arg_type)) {
      DISPATCH_STRING_CALC_EXPR_VECTOR(ObExprTimeBase::calc_for_string_vector, DT_HOUR, false);
    } else if (ob_is_int_uint_tc(arg_type)) {
      DISPATCH_INTEGER_CALC_EXPR_VECTOR(ObExprTimeBase::calc_for_integer_vector, DT_HOUR);
    }

    if (OB_FAIL(ret)) {
      LOG_WARN("expr calculation failed", K(ret));
    }
  }
  return ret;
}

template <typename ArgVec, typename ResVec, typename IN_TYPE>
int vector_minute(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
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
      UsecType usec = 0;
      UsecType secs = 0;
      bool no_skip_no_null = bound.get_all_rows_active() && !arg_vec->has_null()
                             && eval_flags.accumulate_bit_cnt(bound) == 0;
      ObTime ob_time;
      BATCH_CALC({
        IN_TYPE in_val = *reinterpret_cast<const IN_TYPE*>(arg_vec->get_payload(idx));
        if (std::is_same<IN_TYPE, ObMySQLDate>::value || std::is_same<IN_TYPE, ObMySQLDateTime>::value) {
          ret = ObTimeConverter::parse_ob_time<IN_TYPE>(in_val, ob_time);
          res_vec->set_int(idx, ob_time.parts_[DT_MIN]);
        } else if (OB_FAIL(ObTimeConverter::parse_date_usec<IN_TYPE>(in_val, tz_offset, lib::is_oracle_mode(), days, usec))) {
          LOG_WARN("get date and usec from vec failed", K(ret));
        } else if (OB_UNLIKELY(ObTimeConverter::ZERO_DATE == days)) {
          res_vec->set_int(idx, 0);
        } else {
          // secs = USEC_TO_SEC(usec) % SECS_PER_HOUR;
          // res_vec->set_int(idx, static_cast<int32_t>(secs / SECS_PER_MIN));
          res_vec->set_int(idx, static_cast<int32_t>(usec % 3600000000 / 60000000));
        }
      });
    }
  }
  return ret;
}
int ObExprMinute::calc_minute_vector(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("fail to eval date_format param", K(ret));
  } else {
    VectorFormat arg_format = expr.args_[0]->get_format(ctx);
    VectorFormat res_format = expr.get_format(ctx);
    const ObObjType arg_type = expr.args_[0]->datum_meta_.type_;

    if (ObMySQLDateTC == ob_obj_type_class(arg_type)) {
      DISPATCH_TIME_EXPR_VECTOR(vector_minute, MySQLDate);
    } else if (ObMySQLDateTimeTC == ob_obj_type_class(arg_type)) {
      DISPATCH_TIME_EXPR_VECTOR(vector_minute, MySQLDateTime);
    } else if (ObDateTC == ob_obj_type_class(arg_type)) {
      DISPATCH_TIME_EXPR_VECTOR(vector_minute, Date);
    } else if (ObDateTimeTC == ob_obj_type_class(arg_type)) {
      DISPATCH_TIME_EXPR_VECTOR(vector_minute, DateTime);
    } else if (ObStringTC == ob_obj_type_class(arg_type)) {
      DISPATCH_STRING_CALC_EXPR_VECTOR(ObExprTimeBase::calc_for_string_vector, DT_MIN, false);
    } else if (ob_is_int_uint_tc(arg_type)) {
      DISPATCH_INTEGER_CALC_EXPR_VECTOR(ObExprTimeBase::calc_for_integer_vector, DT_MIN);
    }

    if (OB_FAIL(ret)) {
      LOG_WARN("expr calculation failed", K(ret));
    }
  }
  return ret;
}

int ObExprSecond::calc_second_vector(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("fail to eval date_format param", K(ret));
  } else {
    VectorFormat arg_format = expr.args_[0]->get_format(ctx);
    VectorFormat res_format = expr.get_format(ctx);
    const ObObjType arg_type = expr.args_[0]->datum_meta_.type_;
    if (ObStringTC == ob_obj_type_class(arg_type)) {
      DISPATCH_STRING_CALC_EXPR_VECTOR(ObExprTimeBase::calc_for_string_vector, DT_SEC, false);
    } else if (ob_is_int_uint_tc(arg_type)) {
      DISPATCH_INTEGER_CALC_EXPR_VECTOR(ObExprTimeBase::calc_for_integer_vector, DT_SEC);
    }

    if (OB_FAIL(ret)) {
      LOG_WARN("expr calculation failed", K(ret));
    }
  }
  return ret;
}

int ObExprMicrosecond::calc_microsecond_vector(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("fail to eval date_format param", K(ret));
  } else {
    VectorFormat arg_format = expr.args_[0]->get_format(ctx);
    VectorFormat res_format = expr.get_format(ctx);
    const ObObjType arg_type = expr.args_[0]->datum_meta_.type_;
    if (ObStringTC == ob_obj_type_class(arg_type)) {
      DISPATCH_STRING_CALC_EXPR_VECTOR(ObExprTimeBase::calc_for_string_vector, DT_USEC, false);
    } else if (ob_is_int_uint_tc(arg_type)) {
      DISPATCH_INTEGER_CALC_EXPR_VECTOR(ObExprTimeBase::calc_for_integer_vector, DT_USEC);
    }

    if (OB_FAIL(ret)) {
      LOG_WARN("expr calculation failed", K(ret));
    }
  }
  return ret;
}
DEFINE_ALL_DAYOF_STRING_EXPR_VECTORS();

// Vectorization for ObExprTime
template <typename ArgVec, typename ResVec>
int do_calc_time_vector(const ObExpr &expr, ObEvalCtx &ctx,
  const ObBitVector &skip, const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  ArgVec *arg_vec = static_cast<ArgVec *>(expr.args_[0]->get_vector(ctx));
  ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  bool no_skip_no_null = bound.get_all_rows_active() && !arg_vec->has_null()
                          && eval_flags.accumulate_bit_cnt(bound) == 0;
  BATCH_CALC({
    int64_t time_val = arg_vec->get_time(idx);
    // LOG_INFO("check for here", K(idx), K(time_val));
    res_vec->set_time(idx, time_val);
  });
  return ret;
}

#define DISPATCH_TIME_EXPR_VECTOR()\
if (VEC_FIXED == arg_format && VEC_FIXED == res_format) {\
  ret = do_calc_time_vector<CONCAT(Time, FixedVec), CONCAT(Time, FixedVec)>(expr, ctx, skip, bound);\
} else if (VEC_FIXED == arg_format && VEC_UNIFORM == res_format) {\
  ret = do_calc_time_vector<CONCAT(Time, FixedVec), CONCAT(Time, UniVec)>(expr, ctx, skip, bound);\
} else if (VEC_FIXED == arg_format && VEC_UNIFORM_CONST == res_format) {\
  ret = do_calc_time_vector<CONCAT(Time, FixedVec), CONCAT(Time, UniCVec)>(expr, ctx, skip, bound);\
} else if (VEC_UNIFORM == arg_format && VEC_FIXED == res_format) {\
  ret = do_calc_time_vector<CONCAT(Time, UniVec), CONCAT(Time, FixedVec)>(expr, ctx, skip, bound);\
} else if (VEC_UNIFORM == arg_format && VEC_UNIFORM == res_format) {\
  ret = do_calc_time_vector<CONCAT(Time, UniVec), CONCAT(Time, UniVec)>(expr, ctx, skip, bound);\
} else if (VEC_UNIFORM == arg_format && VEC_UNIFORM_CONST == res_format) {\
  ret = do_calc_time_vector<CONCAT(Time, UniVec), CONCAT(Time, UniCVec)>(expr, ctx, skip, bound);\
} else if (VEC_UNIFORM_CONST == arg_format && VEC_FIXED == res_format) {\
  ret = do_calc_time_vector<CONCAT(Time, UniCVec), CONCAT(Time, FixedVec)>(expr, ctx, skip, bound);\
} else if (VEC_UNIFORM_CONST == arg_format && VEC_UNIFORM == res_format) {\
  ret = do_calc_time_vector<CONCAT(Time, UniCVec), CONCAT(Time, UniVec)>(expr, ctx, skip, bound);\
} else if (VEC_UNIFORM_CONST == arg_format && VEC_UNIFORM_CONST == res_format) {\
  ret = do_calc_time_vector<CONCAT(Time, UniCVec), CONCAT(Time, UniCVec)>(expr, ctx, skip, bound);\
} else { \
  ret = do_calc_time_vector<ObVectorBase, ObVectorBase>(expr, ctx, skip, bound);\
}

int ObExprTime::calc_time_vector(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("fail to eval time param", K(ret));
  } else {
    VectorFormat arg_format = expr.args_[0]->get_format(ctx);
    VectorFormat res_format = expr.get_format(ctx);
    const ObObjType arg_type = expr.args_[0]->datum_meta_.type_;
    if (ObTimeTC == ob_obj_type_class(arg_type)) {
      DISPATCH_TIME_EXPR_VECTOR();
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("expr calculation failed", K(ret));
    }
  }
  return ret;
}

#undef DISPATCH_YEAR_MON_NAME_EXPR_VECTOR
#undef DISPATCH_TIME_EXPR_VECTOR
#undef CHECK_SKIP_NULL
#undef BATCH_CALC

// Explicit instantiation for DT_QUARTER to avoid linker errors
// These instantiations are needed because calc_for_date_vector is called from ob_expr_quarter.cpp
// with DT_QUARTER template parameter, and the template definition is in this .cpp file
#define INSTANTIATE_CALC_FOR_DATE_VECTOR_QUARTER(ArgVec, ResVec, IN_TYPE) \
  template int ObExprTimeBase::calc_for_date_vector<ArgVec, ResVec, IN_TYPE, DT_QUARTER>( \
    const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound);

#define INSTANTIATE_CALC_FOR_DATE_VECTOR_QUARTER_ALL_COMBINATIONS(ArgVecPrefix, IN_TYPE) \
  INSTANTIATE_CALC_FOR_DATE_VECTOR_QUARTER(ArgVecPrefix##FixedVec, IntegerFixedVec, IN_TYPE) \
  INSTANTIATE_CALC_FOR_DATE_VECTOR_QUARTER(ArgVecPrefix##FixedVec, IntegerUniVec, IN_TYPE) \
  INSTANTIATE_CALC_FOR_DATE_VECTOR_QUARTER(ArgVecPrefix##FixedVec, IntegerUniCVec, IN_TYPE) \
  INSTANTIATE_CALC_FOR_DATE_VECTOR_QUARTER(ArgVecPrefix##UniVec, IntegerFixedVec, IN_TYPE) \
  INSTANTIATE_CALC_FOR_DATE_VECTOR_QUARTER(ArgVecPrefix##UniVec, IntegerUniVec, IN_TYPE) \
  INSTANTIATE_CALC_FOR_DATE_VECTOR_QUARTER(ArgVecPrefix##UniVec, IntegerUniCVec, IN_TYPE) \
  INSTANTIATE_CALC_FOR_DATE_VECTOR_QUARTER(ArgVecPrefix##UniCVec, IntegerFixedVec, IN_TYPE) \
  INSTANTIATE_CALC_FOR_DATE_VECTOR_QUARTER(ArgVecPrefix##UniCVec, IntegerUniVec, IN_TYPE) \
  INSTANTIATE_CALC_FOR_DATE_VECTOR_QUARTER(ArgVecPrefix##UniCVec, IntegerUniCVec, IN_TYPE) \
  INSTANTIATE_CALC_FOR_DATE_VECTOR_QUARTER(ObVectorBase, ObVectorBase, IN_TYPE)

// Instantiate for all types
INSTANTIATE_CALC_FOR_DATE_VECTOR_QUARTER_ALL_COMBINATIONS(MySQLDate, ObMySQLDate)
INSTANTIATE_CALC_FOR_DATE_VECTOR_QUARTER_ALL_COMBINATIONS(MySQLDateTime, ObMySQLDateTime)
INSTANTIATE_CALC_FOR_DATE_VECTOR_QUARTER_ALL_COMBINATIONS(Date, DateType)
INSTANTIATE_CALC_FOR_DATE_VECTOR_QUARTER_ALL_COMBINATIONS(DateTime, DateTimeType)

#undef INSTANTIATE_CALC_FOR_DATE_VECTOR_QUARTER_ALL_COMBINATIONS
#undef INSTANTIATE_CALC_FOR_DATE_VECTOR_QUARTER

} //namespace sql
} //namespace oceanbase
