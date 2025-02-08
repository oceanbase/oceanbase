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
#include "sql/engine/expr/ob_expr_date_format.h"
#include "sql/engine/expr/ob_datum_cast.h"
#include "sql/engine/ob_exec_context.h"
#include "lib/locale/ob_locale_type.h"
namespace oceanbase
{
using namespace common;
namespace sql
{

ObExprDateFormat::ObExprDateFormat(ObIAllocator &alloc)
    : ObStringExprOperator(alloc, T_FUN_SYS_DATE_FORMAT, N_DATE_FORMAT, 2, VALID_FOR_GENERATED_COL)
{
}

ObExprDateFormat::~ObExprDateFormat()
{
}

void ObExprDateFormat::check_reset_status(ObExprCtx &expr_ctx,
                                          int &ret,
                                          ObObj &result)
{
  if (CM_IS_WARN_ON_FAIL(expr_ctx.cast_mode_)) {
    ret = OB_SUCCESS;
    result.set_null();
  }
}

int ObExprDateFormat::cg_expr(ObExprCGCtx &op_cg_ctx,
                              const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  if (rt_expr.arg_cnt_ != 2) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("date_format expr should have two params", K(ret), K(rt_expr.arg_cnt_));
  } else if (OB_ISNULL(rt_expr.args_) || OB_ISNULL(rt_expr.args_[0])
             || OB_ISNULL(rt_expr.args_[1])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children of date_format expr is null", K(ret), K(rt_expr.args_));
  } else if (ObStringTC != ob_obj_type_class(rt_expr.args_[1]->datum_meta_.type_)
             && ObNullType != rt_expr.args_[1]->datum_meta_.type_) {
    rt_expr.eval_func_ = ObExprDateFormat::calc_date_format_invalid;
  } else {
    rt_expr.eval_func_ = ObExprDateFormat::calc_date_format;
    // The vectorization of other types of expressions not completed yet.
    if ((ob_is_date_or_mysql_date(rt_expr.args_[0]->datum_meta_.type_)
         || ob_is_datetime_or_mysql_datetime_tc(rt_expr.args_[0]->datum_meta_.type_))
        && rt_expr.args_[1]->is_const_expr()) {
      rt_expr.eval_vector_func_ = ObExprDateFormat::calc_date_format_vector;
    }
  }
  return ret;
}

int ObExprDateFormat::calc_date_format(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObTime ob_time;
  char *buf = NULL;
  int64_t buf_len = OB_MAX_DATE_FORMAT_BUF_LEN;
  int64_t pos = 0;
  const ObSQLSessionInfo *session = NULL;
  ObDatum *date = NULL;
  ObDatum *format = NULL;
  uint64_t cast_mode = 0;
  bool res_null = false;
  ObDateSqlMode date_sql_mode;
  ObSolidifiedVarsGetter helper(expr, ctx, ctx.exec_ctx_.get_my_session());
  ObSQLMode sql_mode = 0;
  const ObTimeZoneInfo *tz_info = NULL;
  ObString locale_name;
  if (OB_ISNULL(session = ctx.exec_ctx_.get_my_session())) {
    ret = OB_NOT_INIT;
    LOG_WARN("session is null", K(ret), K(session));
  } else if (OB_FAIL(helper.get_sql_mode(sql_mode))) {
    LOG_WARN("get sql mode failed", K(ret));
  } else if (OB_FAIL(helper.get_time_zone_info(tz_info))) {
    LOG_WARN("get tz info failed", K(ret));
  } else if (FALSE_IT(ObSQLUtils::get_default_cast_mode(session->get_stmt_type(),
                                                       session->is_ignore_stmt(),
	                                                     sql_mode, cast_mode))) {
    LOG_WARN("get default cast mode failed", K(ret));
  } else if (OB_FAIL(expr.eval_param_value(ctx, date, format))) {
    LOG_WARN("calc param failed", K(ret));
  } else if (date->is_null() || format->is_null()) {
    expr_datum.set_null();
  } else if (OB_ISNULL(buf = expr.get_str_res_mem(ctx, buf_len))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("no more memory to alloc for buf");
  } else if (FALSE_IT(date_sql_mode.init(sql_mode))) {
  } else if (OB_FAIL(ob_datum_to_ob_time_with_date(*date,
                                            expr.args_[0]->datum_meta_.type_,
                                            expr.args_[0]->datum_meta_.scale_,
                                            tz_info,
                                            ob_time,
                                            get_cur_time(ctx.exec_ctx_.get_physical_plan_ctx()),
                                            date_sql_mode,
                                            expr.args_[0]->obj_meta_.has_lob_header()))) {
    LOG_WARN("failed to convert datum to ob time");
    if (CM_IS_WARN_ON_FAIL(cast_mode) && OB_ALLOCATE_MEMORY_FAILED != ret) {
      ret = OB_SUCCESS;
      expr_datum.set_null();
    }
  } else if (OB_UNLIKELY(format->get_string().empty())) {
    expr_datum.set_null();
  } else if (OB_FAIL(session->get_locale_name(locale_name))) {
      LOG_WARN("failed to get locale time name", K(expr), K(expr_datum));
  } else if (OB_FAIL(ObTimeConverter::ob_time_to_str_format(ob_time,
                                                            format->get_string(),
                                                            buf,
                                                            buf_len,
                                                            pos,
                                                            res_null,
                                                            locale_name))) {
      LOG_WARN("failed to convert ob time to str with format");
  } else if (res_null) {
    expr_datum.set_null();
  } else {
    expr_datum.set_string(buf, static_cast<int32_t>(pos));
  }
  return ret;
}

int ObExprDateFormat::calc_date_format_invalid(const ObExpr &expr, ObEvalCtx &ctx,
                                               ObDatum &expr_datum)
{
  UNUSED(expr);
  int ret = OB_SUCCESS;
  expr_datum.set_null();
  uint64_t cast_mode = 0;
  const ObSQLSessionInfo *session = NULL;
  ObSolidifiedVarsGetter helper(expr, ctx, ctx.exec_ctx_.get_my_session());
  ObSQLMode sql_mode = 0;
  if (OB_ISNULL(session = ctx.exec_ctx_.get_my_session())) {
    ret = OB_NOT_INIT;
    LOG_WARN("session is null", K(ret), K(session));
  } else if (OB_FAIL(helper.get_sql_mode(sql_mode))) {
    LOG_WARN("get sql mode failed", K(ret));
  } else if (FALSE_IT(ObSQLUtils::get_default_cast_mode(session->get_stmt_type(),
                                                        session->is_ignore_stmt(),
                                                        sql_mode, cast_mode))) {
  } else if (!CM_IS_WARN_ON_FAIL(cast_mode)) {
    ret =OB_INVALID_ARGUMENT;
  }
  return ret;
}

DEF_SET_LOCAL_SESSION_VARS(ObExprDateFormat, raw_expr) {
  int ret = OB_SUCCESS;
  if (is_mysql_mode()) {
    SET_LOCAL_SYSVAR_CAPACITY(3);
    EXPR_ADD_LOCAL_SYSVAR(SYS_VAR_SQL_MODE);
    EXPR_ADD_LOCAL_SYSVAR(SYS_VAR_TIME_ZONE);
    EXPR_ADD_LOCAL_SYSVAR(SYS_VAR_COLLATION_CONNECTION);
  }
  return ret;
}

const char* ObExprGetFormat::FORMAT_STR[FORMAT_MAX] =
{
  "EUR",
  "INTERNAL",
  "ISO",
  "JIS",
  "USA"
};

const char* ObExprGetFormat::DATE_FORMAT[FORMAT_MAX + 1] =
{
  "%d.%m.%Y",
  "%Y%m%d",
  "%Y-%m-%d",
  "%Y-%m-%d",
  "%m.%d.%Y",
  "invalid"
};

const char* ObExprGetFormat::TIME_FORMAT[FORMAT_MAX + 1] =
{
  "%H.%i.%s",
  "%H%i%s",
  "%H:%i:%s",
  "%H:%i:%s",
  "%h:%i:%s %p",
  "invalid"
};

const char* ObExprGetFormat::DATETIME_FORMAT[FORMAT_MAX + 1] =
{
  "%Y-%m-%d %H.%i.%s",
  "%Y%m%d%H%i%s",
  "%Y-%m-%d %H:%i:%s",
  "%Y-%m-%d %H:%i:%s",
  "%Y-%m-%d %H.%i.%s",
  "invalid"
};

ObExprGetFormat::ObExprGetFormat(ObIAllocator &alloc)
    : ObStringExprOperator(alloc, T_FUN_SYS_GET_FORMAT, N_GET_FORMAT, 2, VALID_FOR_GENERATED_COL)
{
}

ObExprGetFormat::~ObExprGetFormat()
{
}

inline int ObExprGetFormat::calc_result_type2(ObExprResType &type,
                                               ObExprResType &unit,
                                               ObExprResType &format,
                                               common::ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  UNUSED(unit);
  int ret = common::OB_SUCCESS;
//  common::ObCollationType collation_connection = common::CS_TYPE_INVALID;
  type.set_varchar();
  type.set_collation_type(type_ctx.get_coll_type());
  type.set_collation_level(common::CS_LEVEL_COERCIBLE);
  type.set_length(GET_FORMAT_MAX_LENGTH);
  format.set_calc_type(ObVarcharType);
  if (ObCharset::is_cs_nonascii(format.get_collation_type())) {
    format.set_calc_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  }
  return ret;
}

int ObExprGetFormat::cg_expr(ObExprCGCtx &op_cg_ctx,
                              const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  if (rt_expr.arg_cnt_ != 2) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get_format expr should have two params", K(ret), K(rt_expr.arg_cnt_));
  } else if (OB_ISNULL(rt_expr.args_) || OB_ISNULL(rt_expr.args_[0])
             || OB_ISNULL(rt_expr.args_[1])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children of get_format expr is null", K(ret), K(rt_expr.args_));
  } else if (ObIntType != rt_expr.args_[0]->datum_meta_.type_
            || (ObVarcharType != rt_expr.args_[1]->datum_meta_.type_
                && ObNullType != rt_expr.args_[1]->datum_meta_.type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument type", K(ret), K(rt_expr.args_[0]->datum_meta_),
            K(rt_expr.args_[1]->datum_meta_));
  } else {
    rt_expr.eval_func_ = ObExprGetFormat::calc_get_format;
  }
  return ret;
}

int ObExprGetFormat::calc_get_format(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *unit = NULL;
  ObDatum *format = NULL;
  if (OB_FAIL(expr.eval_param_value(ctx, unit, format))) {
    LOG_WARN("calc param failed", K(ret));
  } else if (OB_UNLIKELY(unit->is_null() || unit->get_int() < 0
             || unit->get_int() >= GET_FORMAT_MAX)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unexpected unit unit type", K(ret));
  } else if (format->is_null()) {
    expr_datum.set_null();
  } else {
    Format fm = FORMAT_MAX;
    ObGetFormatUnitType type = static_cast<ObGetFormatUnitType>(unit->get_int());
    const ObString str = format->get_string();
    for (int64_t i = 0; i < FORMAT_MAX; i++) {
      if (0 == str.case_compare(FORMAT_STR[i])) {
        fm = static_cast<Format>(i);
        break;
      }
    }
    if (OB_UNLIKELY(FORMAT_MAX == fm)) {
      expr_datum.set_null();
    } else {
      const char *res_str = NULL;
      const ObCollationType dest_cs_type = expr.datum_meta_.cs_type_;
      if (GET_FORMAT_DATE == type) {
        res_str = DATE_FORMAT[fm];
      } else if (GET_FORMAT_TIME == type) {
        res_str = TIME_FORMAT[fm];
      } else if (GET_FORMAT_DATETIME == type) {
        res_str = DATETIME_FORMAT[fm];
      }
      if (OB_LIKELY(!ObCharset::is_cs_nonascii(dest_cs_type))) {
        expr_datum.set_string(res_str);
      } else {
        ObExprStrResAlloc out_alloc(expr, ctx);
        ObString out;
        if (OB_FAIL(ObExprUtil::convert_string_collation(ObString::make_string(res_str),
                                                        CS_TYPE_UTF8MB4_GENERAL_CI,
                                                        out,
                                                        expr.datum_meta_.cs_type_,
                                                        out_alloc))) {
          LOG_WARN("convert string collation failed", K(ret));
        } else {
          expr_datum.set_string(out);
        }
      }
    }
  }
  return ret;
}

#define FORMAT_FUNC_NUM (32)
#define MODE_NONE (0)        // only string, like %%
#define MODE_USEC_ONLY (1)   // f,H,h,I,i,T,k,l,p,r,S,s
#define MODE_YEAR_ONLY (2)   // Y,y
#define MODE_YDAY_ONLY (3)   // j
#define MODE_WEEK_ONLY (4)   // a,W,w
#define MODE_YEAR_MONTH (5)  // b,c,D,d,e,M,m
#define MODE_YEAR_WEEK (6)   // U,u,X,x,V,v
#define MODE_YEAR_MONTH_WEEK (7)  // upgraded level
#define PROCESS_FORMAT_CASE(ch, func)                                                    \
    case ch: {                                                                           \
      if (no_skip_no_null) {                                                             \
        for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {    \
          ret = func(year[idx], month[idx], dt_yday[idx],                                \
                    dt_mday[idx], dt_wday[idx], hour[idx], minute[idx], sec[idx],        \
                    fsec[idx], week_sunday[idx], week_monday[idx],                       \
                    delta_sunday[idx], delta_monday[idx],                                \
                    buf[idx] + len[idx], len[idx], res_null[idx], no_null,               \
                    day_name, ab_day_name, month_name, ab_month_name);                   \
        }                                                                                \
      } else {                                                                           \
        for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {    \
          if (skip.at(idx) || eval_flags.at(idx) || arg_vec->is_null(idx)) { continue; } \
          ret = func(year[idx], month[idx], dt_yday[idx],                                \
                    dt_mday[idx], dt_wday[idx], hour[idx], minute[idx], sec[idx],        \
                    fsec[idx], week_sunday[idx], week_monday[idx],                       \
                    delta_sunday[idx], delta_monday[idx],                                \
                    buf[idx] + len[idx], len[idx], res_null[idx], no_null,               \
                    day_name, ab_day_name, month_name, ab_month_name);                   \
        }                                                                                \
      }                                                                                  \
      break;                                                                             \
    }

#define BATCH_CALCULATE_DECL  arg_vec, no_skip_no_null, tz_offset,                       \
                              year, month, date, usec, dt_yday, dt_mday,                 \
                              dt_wday, hour, minute, sec, fsec, ob_time,                 \
                              eval_flags, skip, bound

#define get_parts(ob_time)                                                                         \
  year = ob_time.parts_[DT_YEAR];                                                                  \
  month = ob_time.parts_[DT_MON];                                                                  \
  dt_mday = ob_time.parts_[DT_MDAY];                                                               \
  hour = ob_time.parts_[DT_HOUR];                                                                  \
  minute = ob_time.parts_[DT_MIN];                                                                 \
  sec = ob_time.parts_[DT_SEC];                                                                    \
  fsec = ob_time.parts_[DT_USEC];                                                                  \
  dt_yday = ob_time.parts_[DT_YDAY];                                                               \
  dt_wday = ob_time.parts_[DT_WDAY];

#define calc_time_parts(usec)                                                                            \
  int64_t secs = USEC_TO_SEC(usec);                                                                \
  hour = static_cast<int32_t>(secs / SECS_PER_HOUR);                                               \
  secs %= SECS_PER_HOUR;                                                                           \
  minute = static_cast<int32_t>(secs / SECS_PER_MIN);                                              \
  sec = static_cast<int32_t>(secs % SECS_PER_MIN);                                                 \
  fsec = static_cast<int32_t>(usec % USECS_PER_SEC);

template<typename Calc, typename ArgVec>
OB_INLINE static int batch_calc(
    Calc calc, ArgVec *arg_vec, bool no_skip_no_null, int64_t tz_offset,
    YearType* year, MonthType* month, DateType date,
    UsecType usec, DateType* dt_yday, DateType* dt_mday, DateType* dt_wday,
    int32_t* hour, int32_t* minute, int32_t* sec, int32_t* fesc, ObTime &ob_time,
    ObBitVector &eval_flags, const ObBitVector &skip, const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  if (no_skip_no_null) {
    for (int idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
      ret = calc(arg_vec, idx, tz_offset, year[idx], month[idx], date, usec, dt_yday[idx],
                 dt_mday[idx], dt_wday[idx], hour[idx], minute[idx], sec[idx], fesc[idx], ob_time);
    }
  } else {
    for (int idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
      if (skip.at(idx) || eval_flags.at(idx) || arg_vec->is_null(idx)) { continue; }
      ret = calc(arg_vec, idx, tz_offset, year[idx], month[idx], date, usec, dt_yday[idx],
                 dt_mday[idx], dt_wday[idx], hour[idx], minute[idx], sec[idx], fesc[idx], ob_time);
    }
  }
  return ret;
}


///@fn analyze format, get format sequnce and the common calculate part
int ObExprDateFormat::analyze_format(
    const char *format_ptr,
    const char *end_ptr,
    int32_t &mode)
{
  int ret = OB_SUCCESS;
  mode = MODE_NONE;
  while (format_ptr < end_ptr && OB_SUCC(ret)) {
    if ('%' == *format_ptr) {
      format_ptr++;
      if (format_ptr >= end_ptr) {  // '%' as tail not allowed
        ret = OB_INVALID_ARGUMENT;
        break;
      }
      switch (*format_ptr) {
        // MODE_USEC_ONLY
        case 'f': case 'H': case 'h': case 'I': case 'i': case 'T': case 'k':
            case 'l': case 'p': case 'r': case 'S': case 's': {
          if (MODE_NONE == mode) {
            mode = MODE_USEC_ONLY;
          }
        } break;
        // MODE_YEAR_ONLY
        case 'Y': case 'y': {
          if (MODE_YEAR_ONLY > mode) {
            mode = MODE_YEAR_ONLY;
          } else if (MODE_WEEK_ONLY == mode) {
            mode = MODE_YEAR_WEEK;
          }
        } break;
        // MODE_YDAY_ONLY
        case 'j': {
          if (MODE_YDAY_ONLY > mode) {
            mode = MODE_YDAY_ONLY;
          } else if (MODE_WEEK_ONLY == mode) {
            mode = MODE_YEAR_WEEK;
          }
        } break;
        // MODE_YEAR_MONTH
        case 'b': case 'c': case 'D': case 'd': case 'e': case 'M': case 'm': {
          if (MODE_WEEK_ONLY == mode || MODE_YEAR_WEEK == mode) {
            mode = MODE_YEAR_MONTH_WEEK;
          } else if (MODE_YEAR_MONTH_WEEK != mode) {
            mode = MODE_YEAR_MONTH;
          }
        } break;
        // MODE_WEEK_ONLY
        case 'a': case 'W': case 'w': {
          if (MODE_USEC_ONLY >= mode) {
            mode = MODE_WEEK_ONLY;
          } else if (MODE_YEAR_ONLY == mode || MODE_YDAY_ONLY == mode) {
            mode = MODE_YEAR_WEEK;
          } else if (MODE_YEAR_MONTH == mode) {
            mode = MODE_YEAR_MONTH_WEEK;
          }
        } break;
        // MODE_YEAR_WEEK
        case 'U': case 'u': case 'X': case 'x': case 'V': case 'v': {
          if (MODE_YEAR_MONTH == mode) {
            mode = MODE_YEAR_MONTH_WEEK;
          } else if (MODE_YEAR_MONTH_WEEK != mode) {
            mode = MODE_YEAR_WEEK;
          }
        } break;
        default: {
          break;
        }
      }
    }
    format_ptr++;
  }
  return ret;
}

int ObExprDateFormat::get_day_month_names(ObString locale_name,
                                          const char *const *&day_name,
                                          const char *const *&ab_day_name,
                                          const char *const *&month_name,
                                          const char *const *&ab_month_name)
{
  int ret = OB_SUCCESS;
  OB_LOCALE *ob_cur_locale = ob_locale_by_name(locale_name);
  OB_LOCALE_TYPE *locale_type_day = ob_cur_locale->day_names_;
  OB_LOCALE_TYPE *locale_type_ab_day = ob_cur_locale->ab_day_names_;
  OB_LOCALE_TYPE *locale_type_mon = ob_cur_locale->month_names_;
  OB_LOCALE_TYPE *locale_type_ab_mon = ob_cur_locale->ab_month_names_;
  const char ** locale_daynames = locale_type_day->type_names_;
  const char ** locale_ab_daynames = locale_type_ab_day->type_names_;
  const char ** locale_monthnames = locale_type_mon->type_names_;
  const char ** locale_ab_monthnames = locale_type_ab_mon->type_names_;
  if (lib::is_mysql_mode()) {
    day_name = locale_daynames;
    month_name = locale_monthnames;
    ab_day_name = locale_ab_daynames;
    ab_month_name = locale_ab_monthnames;
  } else {
    day_name = &(WDAY_NAMES+1)->ptr_;
    month_name = &(MON_NAMES+1)->ptr_;
    ab_day_name = &(WDAY_ABBR_NAMES+1)->ptr_;
    ab_month_name = &(MON_ABBR_NAMES+1)->ptr_;
  }
  return ret;
}

template <typename ArgVec, typename ResVec, typename IN_TYPE>
int ObExprDateFormat::vector_date_format(const ObExpr &expr,
                                         ObEvalCtx &ctx,
                                         const ObBitVector &skip,
                                         const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  ArgVec *arg_vec = static_cast<ArgVec *>(expr.args_[0]->get_vector(ctx));
  ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));
  int64_t tz_offset = 0;
  ObString locale_name;
  const char *const *day_name = nullptr;
  const char *const *ab_day_name = nullptr;
  const char *const *month_name = nullptr;
  const char *const *ab_month_name = nullptr;
  const ObSQLSessionInfo *session = NULL;
  const common::ObTimeZoneInfo *tz_info = NULL;
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  bool no_skip_no_null = bound.get_all_rows_active() && !arg_vec->has_null()
                         && eval_flags.accumulate_bit_cnt(bound) == 0;
  ObSolidifiedVarsGetter helper(expr, ctx, ctx.exec_ctx_.get_my_session());
  if (OB_ISNULL(session = ctx.exec_ctx_.get_my_session())) {
    ret = OB_NOT_INIT;
    LOG_WARN("session is null", K(ret), K(session));
  } else if (OB_FAIL(helper.get_time_zone_info(tz_info))) {
    LOG_WARN("get tz info failed", K(ret));
  } else if (OB_FAIL(session->get_locale_name(locale_name))) {
    LOG_WARN("failed to get locale time name", K(expr));
  } else if (OB_FAIL(get_day_month_names(locale_name, day_name, ab_day_name, month_name, ab_month_name))) {
    LOG_WARN("get day month names fail", K(ret));
  } else if (OB_FAIL(expr.args_[1]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("failed to get format", K(expr));
  } else if (expr.args_[1]->get_vector(ctx)->is_null(0) ||
             expr.args_[1]->get_vector(ctx)->get_string(0).empty()) {
    for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
      if (skip.at(idx) || eval_flags.at(idx)) { continue; }
      res_vec->set_null(idx);
      eval_flags.set(idx);
    }
  } else {
    const ObTimeZoneInfo *local_tz_info = (ObTimestampType == expr.args_[0]->datum_meta_.type_) ? tz_info : NULL;
    if (OB_FAIL(get_tz_offset(local_tz_info, tz_offset))) {
      LOG_WARN("get tz_info offset fail", K(ret));
    } else {
      // analyze format and get calculate mode
      int32_t mode = MODE_NONE;
      ObString format_string = expr.args_[1]->get_vector(ctx)->get_string(0);
      const char *format_ptr = format_string.ptr();
      const char *end_ptr = format_ptr + format_string.length();
      if (OB_FAIL(analyze_format(format_ptr, end_ptr, mode))) {
        LOG_WARN("invalid format", K(ret));
      } else {
        int batch_size = bound.end();
        // (4 + 4 + 1 + 8 + 4 + 4 + 4 +1+1+1+1 + 2 + ...) * 256 / 1024 = 33/4 = 8.75 KB
        int32_t year[batch_size];
        int8_t month[batch_size];
        int32_t hour[batch_size];
        int32_t minute[batch_size];
        int32_t sec[batch_size];
        int32_t fsec[batch_size];
        int32_t dt_yday[batch_size];
        int32_t dt_mday[batch_size];
        int32_t dt_wday[batch_size];
        int8_t week_sunday[batch_size];
        int8_t week_monday[batch_size];
        int8_t delta_sunday[batch_size];
        int8_t delta_monday[batch_size];
        int16_t len[batch_size];
        char *buf[batch_size];
        int buf_len = format_string.length() * 5; // september / 2 <= 5
        bool res_null[batch_size];
        bool no_null = true;
        memset(len, 0, sizeof(len));
        memset(res_null, false, sizeof(res_null));
        for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
          if (skip.at(idx) || eval_flags.at(idx) || arg_vec->is_null(idx)) {
            continue;
          } else if(OB_ISNULL(buf[idx] = expr.get_str_res_mem(ctx, buf_len, idx))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_ERROR("no more memory to alloc for buf");
          }
        }
        if (OB_SUCC(ret)) {
          // calculate common part
          ObTime ob_time;
          int32_t date = 0;
          int64_t usec = 0;
          if (MODE_USEC_ONLY == mode) {  // 1
            ret = batch_calc(ObExprDateFormat::calc_usec_only<ArgVec, IN_TYPE>, BATCH_CALCULATE_DECL);
          } else if (MODE_YEAR_ONLY == mode) {  // 2
            ret = batch_calc(ObExprDateFormat::calc_year_only<ArgVec, IN_TYPE>, BATCH_CALCULATE_DECL);
          } else if (MODE_YDAY_ONLY == mode) {  // 3
            ret = batch_calc(ObExprDateFormat::calc_yday_only<ArgVec, IN_TYPE>, BATCH_CALCULATE_DECL);
          } else if (MODE_WEEK_ONLY == mode) {  // 4
            ret = batch_calc(ObExprDateFormat::calc_week_only<ArgVec, IN_TYPE>, BATCH_CALCULATE_DECL);
          } else if (MODE_YEAR_MONTH == mode) {  // 5
            ret = batch_calc(ObExprDateFormat::calc_year_month<ArgVec, IN_TYPE>, BATCH_CALCULATE_DECL);
          } else {
            memset(week_sunday,  -1, sizeof(week_sunday));
            memset(week_monday,  -1, sizeof(week_monday));
            memset(delta_sunday, -2, sizeof(delta_sunday));
            memset(delta_monday, -2, sizeof(delta_monday));
            if (MODE_YEAR_WEEK == mode) {  // 6
              ret = batch_calc(ObExprDateFormat::calc_year_week<ArgVec, IN_TYPE>, BATCH_CALCULATE_DECL);
            } else if (MODE_YEAR_MONTH_WEEK == mode) {  // 7
              ret = batch_calc(ObExprDateFormat::calc_year_month_week<ArgVec, IN_TYPE>, BATCH_CALCULATE_DECL);
            }
          }

          // calculate format
          while (format_ptr < end_ptr && OB_SUCC(ret)) {
            if ('%' == *format_ptr) {
              format_ptr++;
              switch (*format_ptr) {
                PROCESS_FORMAT_CASE('Y', ObExprDateFormat::get_from_format_Y);
                PROCESS_FORMAT_CASE('y', ObExprDateFormat::get_from_format_y);
                PROCESS_FORMAT_CASE('f', ObExprDateFormat::get_from_format_f);
                PROCESS_FORMAT_CASE('j', ObExprDateFormat::get_from_format_j);
                PROCESS_FORMAT_CASE('M', ObExprDateFormat::get_from_format_M);
                PROCESS_FORMAT_CASE('m', ObExprDateFormat::get_from_format_m);
                PROCESS_FORMAT_CASE('a', ObExprDateFormat::get_from_format_a);
                PROCESS_FORMAT_CASE('H', ObExprDateFormat::get_from_format_H);
                PROCESS_FORMAT_CASE('h', ObExprDateFormat::get_from_format_h_I);
                PROCESS_FORMAT_CASE('I', ObExprDateFormat::get_from_format_h_I);
                PROCESS_FORMAT_CASE('i', ObExprDateFormat::get_from_format_i);
                PROCESS_FORMAT_CASE('T', ObExprDateFormat::get_from_format_T);
                PROCESS_FORMAT_CASE('k', ObExprDateFormat::get_from_format_k);
                PROCESS_FORMAT_CASE('l', ObExprDateFormat::get_from_format_l);
                PROCESS_FORMAT_CASE('p', ObExprDateFormat::get_from_format_p);
                PROCESS_FORMAT_CASE('r', ObExprDateFormat::get_from_format_r);
                PROCESS_FORMAT_CASE('S', ObExprDateFormat::get_from_format_S_s);
                PROCESS_FORMAT_CASE('s', ObExprDateFormat::get_from_format_S_s);
                PROCESS_FORMAT_CASE('b', ObExprDateFormat::get_from_format_b);
                PROCESS_FORMAT_CASE('c', ObExprDateFormat::get_from_format_c);
                PROCESS_FORMAT_CASE('D', ObExprDateFormat::get_from_format_D);
                PROCESS_FORMAT_CASE('d', ObExprDateFormat::get_from_format_d);
                PROCESS_FORMAT_CASE('e', ObExprDateFormat::get_from_format_e);
                PROCESS_FORMAT_CASE('W', ObExprDateFormat::get_from_format_W);
                PROCESS_FORMAT_CASE('w', ObExprDateFormat::get_from_format_w);
                PROCESS_FORMAT_CASE('U', ObExprDateFormat::get_from_format_U);
                PROCESS_FORMAT_CASE('u', ObExprDateFormat::get_from_format_u);
                PROCESS_FORMAT_CASE('X', ObExprDateFormat::get_from_format_X);
                PROCESS_FORMAT_CASE('x', ObExprDateFormat::get_from_format_x);
                PROCESS_FORMAT_CASE('V', ObExprDateFormat::get_from_format_V);
                PROCESS_FORMAT_CASE('v', ObExprDateFormat::get_from_format_v);
                default: {
                  if (no_skip_no_null) {
                    for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
                      buf[idx][len[idx]++] = *format_ptr;
                    }
                  } else {
                    for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
                      if (skip.at(idx) || eval_flags.at(idx) || arg_vec->is_null(idx)) { continue; }
                      buf[idx][len[idx]++] = *format_ptr;
                    }
                  }
                } break;
              }
            } else {
              if (no_skip_no_null) {
                for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
                  buf[idx][len[idx]++] = *format_ptr;
                }
              } else {
                for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
                  if (skip.at(idx) || eval_flags.at(idx) || arg_vec->is_null(idx)) { continue; }
                  buf[idx][len[idx]++] = *format_ptr;
                }
              }
            }
            format_ptr++;
          }

          // set_string
          no_skip_no_null &= no_null;
          if (OB_LIKELY(no_skip_no_null)) {
            for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
              res_vec->set_payload_shallow(idx, buf[idx], len[idx]);
            }
            eval_flags.set_all(bound.start(), bound.end());
          } else {
            for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
              if (skip.at(idx) || eval_flags.at(idx)) {
                continue;
              } else if (arg_vec->is_null(idx) || res_null[idx]) {
                res_vec->set_null(idx);
                eval_flags.set(idx);
                continue;
              }
              res_vec->set_payload_shallow(idx, buf[idx], len[idx]);
              eval_flags.set(idx);
            }
          }
        }
      }
    }
  }
  return ret;
}

#define DISPATCH_DATE_FORMAT_VECTOR_FUNC(FUNC, TYPE)                                               \
  if (VEC_FIXED == arg_format && VEC_DISCRETE == res_format) {                                     \
    ret = FUNC<CONCAT(TYPE, FixedVec), StrDiscVec, CONCAT(TYPE, Type)>(expr, ctx, skip, bound);    \
  } else if (VEC_FIXED == arg_format && VEC_UNIFORM == res_format) {                               \
    ret = FUNC<CONCAT(TYPE, FixedVec), StrUniVec, CONCAT(TYPE, Type)>(expr, ctx, skip, bound);     \
  } else if (VEC_FIXED == arg_format && VEC_UNIFORM_CONST == res_format) {                         \
    ret = FUNC<CONCAT(TYPE, FixedVec), StrUniCVec, CONCAT(TYPE, Type)>(expr, ctx, skip, bound);    \
  } else if (VEC_FIXED == arg_format && VEC_CONTINUOUS == res_format) {                            \
    ret = FUNC<CONCAT(TYPE, FixedVec), StrContVec, CONCAT(TYPE, Type)>(expr, ctx, skip, bound);    \
  } else if (VEC_UNIFORM == arg_format && VEC_DISCRETE == res_format) {                            \
    ret = FUNC<CONCAT(TYPE, UniVec), StrDiscVec, CONCAT(TYPE, Type)>(expr, ctx, skip, bound);      \
  } else if (VEC_UNIFORM == arg_format && VEC_UNIFORM == res_format) {                             \
    ret = FUNC<CONCAT(TYPE, UniVec), StrUniVec, CONCAT(TYPE, Type)>(expr, ctx, skip, bound);       \
  } else if (VEC_UNIFORM == arg_format && VEC_UNIFORM_CONST == res_format) {                       \
    ret = FUNC<CONCAT(TYPE, UniVec), StrUniCVec, CONCAT(TYPE, Type)>(expr, ctx, skip, bound);      \
  } else if (VEC_UNIFORM == arg_format && VEC_CONTINUOUS == res_format) {                          \
    ret = FUNC<CONCAT(TYPE, UniVec), StrContVec, CONCAT(TYPE, Type)>(expr, ctx, skip, bound);      \
  } else if (VEC_UNIFORM_CONST == arg_format && VEC_DISCRETE == res_format) {                      \
    ret = FUNC<CONCAT(TYPE, UniCVec), StrDiscVec, CONCAT(TYPE, Type)>(expr, ctx, skip, bound);     \
  } else if (VEC_UNIFORM_CONST == arg_format && VEC_UNIFORM == res_format) {                       \
    ret = FUNC<CONCAT(TYPE, UniCVec), StrUniVec, CONCAT(TYPE, Type)>(expr, ctx, skip, bound);      \
  } else if (VEC_UNIFORM_CONST == arg_format && VEC_UNIFORM_CONST == res_format) {                 \
    ret = FUNC<CONCAT(TYPE, UniCVec), StrUniCVec, CONCAT(TYPE, Type)>(expr, ctx, skip, bound);     \
  } else if (VEC_UNIFORM_CONST == arg_format && VEC_CONTINUOUS == res_format) {                    \
    ret = FUNC<CONCAT(TYPE, UniCVec), StrContVec, CONCAT(TYPE, Type)>(expr, ctx, skip, bound);     \
  } else {                                                                                         \
    ret = FUNC<ObVectorBase, ObVectorBase, CONCAT(TYPE, Type)>(expr, ctx, skip, bound);            \
  }

int ObExprDateFormat::calc_date_format_vector(const ObExpr &expr,
                                              ObEvalCtx &ctx,
                                              const ObBitVector &skip,
                                              const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("fail to eval date_format param", K(ret));
  } else {
    VectorFormat arg_format = expr.args_[0]->get_format(ctx);
    VectorFormat res_format = expr.get_format(ctx);
    ObObjTypeClass arg_tc = ob_obj_type_class(expr.args_[0]->datum_meta_.type_);
    if (ObMySQLDateTC == arg_tc) {
      DISPATCH_DATE_FORMAT_VECTOR_FUNC(vector_date_format, MySQLDate);
    } else if (ObMySQLDateTimeTC == arg_tc) {
      DISPATCH_DATE_FORMAT_VECTOR_FUNC(vector_date_format, MySQLDateTime);
    } else if (ObDateTC == arg_tc) {
      DISPATCH_DATE_FORMAT_VECTOR_FUNC(vector_date_format, Date);
    } else if (ObDateTimeTC == arg_tc) {
      DISPATCH_DATE_FORMAT_VECTOR_FUNC(vector_date_format, DateTime);
    }

    if (OB_FAIL(ret)) {
      LOG_WARN("expr calculation failed", K(ret));
    }
  }
  return ret;
}
#undef DISPATCH_DATE_FORMAT_VECTOR_FUNC

// common part function
template<typename ArgVec, typename IN_TYPE>
OB_INLINE int ObExprDateFormat::calc_usec_only(COMMON_PART_FORMAT_FUNC_ARG_DECL) {
  int ret = OB_SUCCESS;
  IN_TYPE in_val = *reinterpret_cast<const IN_TYPE*>(arg_vec->get_payload(idx));
  if (std::is_same<IN_TYPE, ObMySQLDate>::value) {
    hour = minute = sec = fsec = usec = 0;
  } else if (std::is_same<IN_TYPE, ObMySQLDateTime>::value) {
    ret = ObTimeConverter::parse_ob_time<IN_TYPE>(in_val, ob_time);
    hour = ob_time.parts_[DT_HOUR];
    minute = ob_time.parts_[DT_MIN];
    sec = ob_time.parts_[DT_SEC];
    fsec = ob_time.parts_[DT_USEC];
  } else if (OB_FAIL(ObTimeConverter::parse_date_usec<IN_TYPE>(in_val, tz_offset,lib::is_oracle_mode(), date, usec))) {
    LOG_WARN("get date and usec from vec failed", K(ret));
  } else {
    calc_time_parts(usec);
  }
  return ret;
}
template<typename ArgVec, typename IN_TYPE>
OB_INLINE int ObExprDateFormat::calc_year_only(COMMON_PART_FORMAT_FUNC_ARG_DECL) {
  int ret = OB_SUCCESS;
  IN_TYPE in_val = *reinterpret_cast<const IN_TYPE*>(arg_vec->get_payload(idx));
  if (std::is_same<IN_TYPE, ObMySQLDate>::value || std::is_same<IN_TYPE, ObMySQLDateTime>::value) {
    ret = ObTimeConverter::parse_ob_time<IN_TYPE>(in_val, ob_time);
    year = ob_time.parts_[DT_YEAR];
    hour = ob_time.parts_[DT_HOUR];
    minute = ob_time.parts_[DT_MIN];
    sec = ob_time.parts_[DT_SEC];
    fsec = ob_time.parts_[DT_USEC];
  } else if (OB_FAIL(ObTimeConverter::parse_date_usec<IN_TYPE>(in_val, tz_offset, lib::is_oracle_mode(), date, usec))) {
    LOG_WARN("get date and usec from vec failed", K(ret));
  } else if (OB_UNLIKELY(ObTimeConverter::ZERO_DATE == date)) {
    year = dt_yday = usec = hour = minute = sec = fsec = 0; // USEC_ONLY, YEAR_ONLY
  } else {
    ObTimeConverter::days_to_year(date, year);
    calc_time_parts(usec);
  }
  return ret;
}
template<typename ArgVec, typename IN_TYPE>
OB_INLINE int ObExprDateFormat::calc_yday_only(COMMON_PART_FORMAT_FUNC_ARG_DECL) {  // for j
  int ret = OB_SUCCESS;
  IN_TYPE in_val = *reinterpret_cast<const IN_TYPE*>(arg_vec->get_payload(idx));
  if (std::is_same<IN_TYPE, ObMySQLDate>::value || std::is_same<IN_TYPE, ObMySQLDateTime>::value) {
    ret = ObTimeConverter::parse_ob_time<IN_TYPE>(in_val, ob_time, false);
    dt_yday = ObTimeConverter::calc_yday(ob_time);
    hour = ob_time.parts_[DT_HOUR];
    minute = ob_time.parts_[DT_MIN];
    sec = ob_time.parts_[DT_SEC];
    fsec = ob_time.parts_[DT_USEC];
  } else if (OB_FAIL(ObTimeConverter::parse_date_usec<IN_TYPE>(in_val, tz_offset, lib::is_oracle_mode(), date, usec))) {
    LOG_WARN("get date and usec from vec failed", K(ret));
  } else if (OB_UNLIKELY(ObTimeConverter::ZERO_DATE == date)) {
    year = dt_yday = usec = hour = minute = sec = fsec =  0; // USEC_ONLY, YEAR_ONLY, YDAY_ONLY
  } else {
    ObTimeConverter::days_to_year_ydays(date, year, dt_yday);
    calc_time_parts(usec);
  }
  return ret;
}
template<typename ArgVec, typename IN_TYPE>
OB_INLINE int ObExprDateFormat::calc_week_only(COMMON_PART_FORMAT_FUNC_ARG_DECL) {
  int ret = OB_SUCCESS;
  IN_TYPE in_val = *reinterpret_cast<const IN_TYPE*>(arg_vec->get_payload(idx));
  if (std::is_same<IN_TYPE, ObMySQLDate>::value || std::is_same<IN_TYPE, ObMySQLDateTime>::value) {
    ret = ObTimeConverter::parse_ob_time<IN_TYPE>(in_val, ob_time, true);
    dt_wday = ob_time.parts_[DT_WDAY];
    hour = ob_time.parts_[DT_HOUR];
    minute = ob_time.parts_[DT_MIN];
    sec = ob_time.parts_[DT_SEC];
    fsec = ob_time.parts_[DT_USEC];
  } else if (OB_FAIL(ObTimeConverter::parse_date_usec<IN_TYPE>(in_val, tz_offset, lib::is_oracle_mode(), date, usec))) {
    LOG_WARN("get date and usec from vec failed", K(ret));
  } else if (OB_UNLIKELY(ObTimeConverter::ZERO_DATE == date)) {
    dt_wday = usec = hour = minute = sec = fsec = 0; // USEC_ONLY, WEEK_ONLY
  } else {
    dt_wday = WDAY_OFFSET[date % DAYS_PER_WEEK][4];
    calc_time_parts(usec);
  }
  return ret;
}
template<typename ArgVec, typename IN_TYPE>
OB_INLINE int ObExprDateFormat::calc_year_month(COMMON_PART_FORMAT_FUNC_ARG_DECL) {
  int ret = OB_SUCCESS;
  IN_TYPE in_val = *reinterpret_cast<const IN_TYPE*>(arg_vec->get_payload(idx));
  if (std::is_same<IN_TYPE, ObMySQLDate>::value || std::is_same<IN_TYPE, ObMySQLDateTime>::value) {
    ret = ObTimeConverter::parse_ob_time<IN_TYPE>(in_val, ob_time);
    get_parts(ob_time);
  } else if (OB_FAIL(ObTimeConverter::parse_date_usec<IN_TYPE>(in_val, tz_offset,lib::is_oracle_mode(), date, usec))) {
    LOG_WARN("get date and usec from vec failed", K(ret));
  } else if (OB_UNLIKELY(ObTimeConverter::ZERO_DATE == date)) {
    year = month = usec = dt_yday = dt_mday = hour = minute = sec = fsec = 0;  // USEC_ONLY, YEAR_ONLY, YEAR_MONTH
  } else {
    ObTimeConverter::days_to_year_ydays(date, year, dt_yday);
    ObTimeConverter::ydays_to_month_mdays(year, dt_yday, month, dt_mday);
    calc_time_parts(usec);
  }
  return ret;
}
template<typename ArgVec, typename IN_TYPE>
OB_INLINE int ObExprDateFormat::calc_year_week(COMMON_PART_FORMAT_FUNC_ARG_DECL) {
  int ret = OB_SUCCESS;
  IN_TYPE in_val = *reinterpret_cast<const IN_TYPE*>(arg_vec->get_payload(idx));
  if (std::is_same<IN_TYPE, ObMySQLDate>::value || std::is_same<IN_TYPE, ObMySQLDateTime>::value) {
    ret = ObTimeConverter::parse_ob_time<IN_TYPE>(in_val, ob_time, true);
    get_parts(ob_time);
  } else if (OB_FAIL(ObTimeConverter::parse_date_usec<IN_TYPE>(in_val, tz_offset, lib::is_oracle_mode(), date, usec))) {
    LOG_WARN("get date and usec from vec failed", K(ret));
  } else if (OB_UNLIKELY(ObTimeConverter::ZERO_DATE == date)) {
    year = date = usec = dt_yday = dt_wday = hour = minute = sec = fsec = 0; // USEC_ONLY, YEAR_ONLY, WEEK_ONLY, YEAR_WEEK
  } else {
    dt_wday = WDAY_OFFSET[date % DAYS_PER_WEEK][4];
    ObTimeConverter::days_to_year_ydays(date, year, dt_yday);
    calc_time_parts(usec);
  }
  return ret;
}
template<typename ArgVec, typename IN_TYPE>
OB_INLINE int ObExprDateFormat::calc_year_month_week(COMMON_PART_FORMAT_FUNC_ARG_DECL) {
  int ret = OB_SUCCESS;
  IN_TYPE in_val = *reinterpret_cast<const IN_TYPE*>(arg_vec->get_payload(idx));
  if (std::is_same<IN_TYPE, ObMySQLDate>::value || std::is_same<IN_TYPE, ObMySQLDateTime>::value) {
    ret = ObTimeConverter::parse_ob_time<IN_TYPE>(in_val, ob_time, true);
    get_parts(ob_time);
  } else if (OB_FAIL(ObTimeConverter::parse_date_usec<IN_TYPE>(in_val, tz_offset, lib::is_oracle_mode(), date, usec))) {
    LOG_WARN("get date and usec from vec failed", K(ret));
  } else if (OB_UNLIKELY(ObTimeConverter::ZERO_DATE == date)) {
    // USEC_ONLY, YEAR_ONLY, YEAR_MONTH, WEEK_ONLY, YEAR_WEEK, YEAR_MONTH_WEEK
    year = month = date = usec = dt_yday = dt_mday = dt_wday = hour = minute = sec = fsec = 0;
  } else {
    dt_wday = WDAY_OFFSET[date % DAYS_PER_WEEK][4];
    ObTimeConverter::days_to_year_ydays(date, year, dt_yday);
    ObTimeConverter::ydays_to_month_mdays(year, dt_yday, month, dt_mday);
    calc_time_parts(usec);
  }
  return ret;
}
// calc format function
OB_INLINE int ObExprDateFormat::get_from_format_Y(FORMAT_FUNC_ARG_DECL)
{
  int ret = OB_SUCCESS;
  memcpy(res_buf, &ObFastFormatInt::DIGITS[(year / 100) << 1], 2);
  memcpy(res_buf + 2, &ObFastFormatInt::DIGITS[(year % 100) << 1], 2);
  len += 4;
  return ret;
}
OB_INLINE int ObExprDateFormat::get_from_format_y(FORMAT_FUNC_ARG_DECL)
{
  int ret = OB_SUCCESS;
  memcpy(res_buf, &ObFastFormatInt::DIGITS[(year % 100) << 1], 2);
  len += 2;
  return ret;
}
OB_INLINE int ObExprDateFormat::get_from_format_M(FORMAT_FUNC_ARG_DECL)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(month - 1 >= 0)) {
    int monthname_length = strlen(month_name[month-1]);
    memcpy(res_buf, month_name[month-1], monthname_length);
    len += monthname_length;
  } else {
    res_null = true;
    no_null = false;
  }
  return ret;
}
OB_INLINE int ObExprDateFormat::get_from_format_m(FORMAT_FUNC_ARG_DECL)
{
  int ret = OB_SUCCESS;
  memcpy(res_buf, &ObFastFormatInt::DIGITS[month << 1], 2);
  len += 2;
  return ret;
}
OB_INLINE int ObExprDateFormat::get_from_format_D(FORMAT_FUNC_ARG_DECL)
{
  int ret = OB_SUCCESS;
  int mday_len = 3 + (dt_mday >= 10); //strlen(DAY_NAME[dt_mday]);
  memcpy(res_buf, DAY_NAME[dt_mday], mday_len);
  len += mday_len;
  return ret;
}
OB_INLINE int ObExprDateFormat::get_from_format_d(FORMAT_FUNC_ARG_DECL)
{
  int ret = OB_SUCCESS;
  memcpy(res_buf, &ObFastFormatInt::DIGITS[dt_mday << 1], 2);
  len += 2;
  return ret;
}
OB_INLINE int ObExprDateFormat::get_from_format_a(FORMAT_FUNC_ARG_DECL)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(dt_wday - 1 >= 0)) {
    int ab_dayname_length = strlen(ab_day_name[dt_wday-1]);
    memcpy(res_buf, ab_day_name[dt_wday-1], ab_dayname_length);
    len += ab_dayname_length;
  } else {
    res_null = true;
    no_null = false;
  }
  return ret;
}
OB_INLINE int ObExprDateFormat::get_from_format_b(FORMAT_FUNC_ARG_DECL)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(month - 1 >= 0)) {
    int ab_monthname_length = strlen(ab_month_name[month-1]);
    memcpy(res_buf, ab_month_name[month-1], ab_monthname_length);
    len += ab_monthname_length;
  } else {
    res_null = true;
    no_null = false;
  }
  return ret;
}
OB_INLINE int ObExprDateFormat::get_from_format_c(FORMAT_FUNC_ARG_DECL)
{
  int ret = OB_SUCCESS;
  if (month < 10) {
    *res_buf = month + '0';
    len += 1;
  } else {
    memcpy(res_buf, &ObFastFormatInt::DIGITS[month << 1], 2);
    len += 2;
  }
  return ret;
}
OB_INLINE int ObExprDateFormat::get_from_format_e(FORMAT_FUNC_ARG_DECL)
{
  int ret = OB_SUCCESS;
  if (dt_mday < 10) {
    *res_buf = dt_mday + '0';
    len += 1;
  } else {
    memcpy(res_buf, &ObFastFormatInt::DIGITS[dt_mday << 1], 2);
    len += 2;
  }
  return ret;
}
OB_INLINE int ObExprDateFormat::get_from_format_j(FORMAT_FUNC_ARG_DECL)
{
  int ret = OB_SUCCESS;
  memcpy(res_buf, &ObFastFormatInt::DIGITS[(dt_yday / 10) << 1], 2);
  res_buf[2] = dt_yday % 10 + '0';
  len += 3;
  return ret;
}
OB_INLINE int ObExprDateFormat::get_from_format_W(FORMAT_FUNC_ARG_DECL)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(dt_wday - 1 >= 0)) {
    int ab_dayname_length = strlen(day_name[dt_wday - 1]);
    memcpy(res_buf, day_name[dt_wday - 1], ab_dayname_length);
    len += ab_dayname_length;
  } else {
    res_null = true;
    no_null = false;
  }
  return ret;
}
OB_INLINE int ObExprDateFormat::get_from_format_w(FORMAT_FUNC_ARG_DECL)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 == dt_wday)) {
    res_null = true;
    no_null = false;
  }
  *res_buf = dt_wday % 7 + '0';
  len += 1;
  return ret;
}
OB_INLINE int ObExprDateFormat::get_from_format_U(FORMAT_FUNC_ARG_DECL)
{
  int ret = OB_SUCCESS;
  WeekType week = 0;
  int8_t temp_delta = 0;
  ObTimeConverter::to_week(WEEK_MODE[0], year, dt_yday, dt_wday, week, /*unused*/temp_delta);
  memcpy(res_buf, &ObFastFormatInt::DIGITS[week << 1], 2);
  len += 2;
  return ret;
}
OB_INLINE int ObExprDateFormat::get_from_format_u(FORMAT_FUNC_ARG_DECL)
{
  int ret = OB_SUCCESS;
  WeekType week = 0;
  int8_t temp_delta = 0;
  ObTimeConverter::to_week(WEEK_MODE[1], year, dt_yday, dt_wday, week, /*unused*/temp_delta);
  memcpy(res_buf, &ObFastFormatInt::DIGITS[week << 1], 2);
  len += 2;
  return ret;
}
OB_INLINE int ObExprDateFormat::get_from_format_X(FORMAT_FUNC_ARG_DECL)
{
  int ret = OB_SUCCESS;
  if (-2 == delta_sunday) {
    ObTimeConverter::to_week(WEEK_MODE[2], year, dt_yday, dt_wday, week_sunday, delta_sunday);
  }
  year += (int32_t)(delta_sunday);
  if (OB_UNLIKELY(-1 == year)) {
    year = 0;
  }
  memcpy(res_buf, &ObFastFormatInt::DIGITS[(year / 100) * 2], 2);
  memcpy(res_buf + 2, &ObFastFormatInt::DIGITS[(year % 100) * 2], 2);
  len += 4;
  return ret;
}
OB_INLINE int ObExprDateFormat::get_from_format_x(FORMAT_FUNC_ARG_DECL)
{
  int ret = OB_SUCCESS;
  if (-2 == delta_monday) {
    ObTimeConverter::to_week(WEEK_MODE[3], year, dt_yday, dt_wday, week_monday, delta_monday);
  }
  year += (int32_t)(delta_monday);
  if (OB_UNLIKELY(-1 == year)) {
    year = 0;
  }
  memcpy(res_buf, &ObFastFormatInt::DIGITS[(year / 100) * 2], 2);
  memcpy(res_buf + 2, &ObFastFormatInt::DIGITS[(year % 100) * 2], 2);
  len += 4;
  return ret;
}
OB_INLINE int ObExprDateFormat::get_from_format_V(FORMAT_FUNC_ARG_DECL)
{
  int ret = OB_SUCCESS;
  if (-1 == week_sunday) {
    ObTimeConverter::to_week(WEEK_MODE[2], year, dt_yday, dt_wday, week_sunday, delta_sunday);
  }
  memcpy(res_buf, &ObFastFormatInt::DIGITS[week_sunday << 1], 2);
  len += 2;
  return ret;
}
OB_INLINE int ObExprDateFormat::get_from_format_v(FORMAT_FUNC_ARG_DECL)
{
  int ret = OB_SUCCESS;
  if (-1 == week_monday) {
    ObTimeConverter::to_week(WEEK_MODE[3], year, dt_yday, dt_wday, week_monday, delta_monday);
  }
  memcpy(res_buf, &ObFastFormatInt::DIGITS[week_monday << 1], 2);
  len += 2;
  return ret;
}
OB_INLINE int ObExprDateFormat::get_from_format_f(FORMAT_FUNC_ARG_DECL) {
  int ret = OB_SUCCESS;
  memcpy(res_buf, &ObFastFormatInt::DIGITS[(fsec / 10000) * 2], 2);
  fsec %= 10000;
  memcpy(res_buf + 2, &ObFastFormatInt::DIGITS[(fsec / 100) * 2], 2);
  fsec %= 100;
  memcpy(res_buf + 4, &ObFastFormatInt::DIGITS[(fsec) * 2], 2);
  len += 6;
  return ret;
}
OB_INLINE int ObExprDateFormat::get_from_format_H(FORMAT_FUNC_ARG_DECL)
{
  int ret = OB_SUCCESS;
  memcpy(res_buf, &ObFastFormatInt::DIGITS[hour * 2], 2);
  len += 2;
  return ret;
}
OB_INLINE int ObExprDateFormat::get_from_format_h_I(FORMAT_FUNC_ARG_DECL)
{
  int ret = OB_SUCCESS;
  hour = hour_converter[hour];
  memcpy(res_buf, &ObFastFormatInt::DIGITS[hour * 2], 2);
  len += 2;
  return ret;
}
OB_INLINE int ObExprDateFormat::get_from_format_i(FORMAT_FUNC_ARG_DECL)
{
  int ret = OB_SUCCESS;
  memcpy(res_buf, &ObFastFormatInt::DIGITS[minute * 2], 2);
  len += 2;
  return ret;
}
OB_INLINE int ObExprDateFormat::get_from_format_k(FORMAT_FUNC_ARG_DECL)
{
  int ret = OB_SUCCESS;
  if (hour < 10) {
    *res_buf = hour + '0';
    len += 1;
  } else {
    memcpy(res_buf, &ObFastFormatInt::DIGITS[hour * 2], 2);
    len += 2;
  }
  return ret;
}
OB_INLINE int ObExprDateFormat::get_from_format_l(FORMAT_FUNC_ARG_DECL)
{
  int ret = OB_SUCCESS;
  hour = hour_converter[hour];
  if (hour < 10) {
    *res_buf = hour + '0';
    len += 1;
  } else {
    memcpy(res_buf, &ObFastFormatInt::DIGITS[hour * 2], 2);
    len += 2;
  }
  return ret;
}
OB_INLINE int ObExprDateFormat::get_from_format_p(FORMAT_FUNC_ARG_DECL)
{
  int ret = OB_SUCCESS;
  const char *ptr = hour < 12 ? "AM" : "PM";
  memcpy(res_buf, ptr, 2);
  len += 2;
  return ret;
}
OB_INLINE int ObExprDateFormat::get_from_format_r(FORMAT_FUNC_ARG_DECL)
{
  int ret = OB_SUCCESS;
  const char *ptr = hour < 12 ? "AM" : "PM";
  hour = hour_converter[hour];
  memcpy(res_buf, &ObFastFormatInt::DIGITS[hour * 2], 2);
  res_buf[2] = ':';
  memcpy(res_buf + 3, &ObFastFormatInt::DIGITS[minute * 2], 2);
  res_buf[5] = ':';
  memcpy(res_buf + 6, &ObFastFormatInt::DIGITS[sec * 2], 2);
  res_buf[8] = ' ';
  memcpy(res_buf + 9, ptr, 2);
  len += 11;
  return ret;
}
OB_INLINE int ObExprDateFormat::get_from_format_S_s(FORMAT_FUNC_ARG_DECL)
{
  int ret = OB_SUCCESS;
  memcpy(res_buf, &ObFastFormatInt::DIGITS[sec * 2], 2);
  len += 2;
  return ret;
}
OB_INLINE int ObExprDateFormat::get_from_format_T(FORMAT_FUNC_ARG_DECL)
{
  int ret = OB_SUCCESS;
  memcpy(res_buf, &ObFastFormatInt::DIGITS[hour * 2], 2);
  res_buf[2] = ':';
  memcpy(res_buf + 3, &ObFastFormatInt::DIGITS[minute * 2], 2);
  res_buf[5] = ':';
  memcpy(res_buf + 6, &ObFastFormatInt::DIGITS[sec * 2], 2);
  len += 8;
  return ret;
}

#undef CHECK_SKIP_NULL
#undef BATCH_CALC
#undef FORMAT_FUNC_NUM
#undef MODE_NONE
#undef MODE_USEC_ONLY
#undef MODE_YEAR_ONLY
#undef MODE_YDAY_ONLY
#undef MODE_WEEK_ONLY
#undef MODE_YEAR_MONTH
#undef MODE_YEAR_WEEK
#undef MODE_YEAR_MONTH_WEEK
#undef PROCESS_FORMAT_CASE
#undef PROCESS_COMMON_PART_CALCULATION

}
}
