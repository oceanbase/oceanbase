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

#include "sql/engine/expr/ob_expr_date_add.h"
#include "sql/engine/ob_exec_context.h"
#include "ob_datum_cast.h"
using namespace oceanbase::common;
using namespace oceanbase::sql;

ObExprDateAdjust::ObExprDateAdjust(ObIAllocator &alloc,
                                   ObExprOperatorType type,
                                   const char *name,
                                   int32_t param_num,
                                   int32_t dimension)
    : ObFuncExprOperator(alloc, type, name, param_num, VALID_FOR_GENERATED_COL, dimension)
{
}

ObExprDateAdjust::~ObExprDateAdjust()
{
}

int ObExprDateAdjust::calc_result_type3(ObExprResType &type,
                                        ObExprResType &date,
                                        ObExprResType &interval,
                                        ObExprResType &unit,
                                        ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  UNUSED(interval);
  //if date is ObDateType, and date_uint contains no hour / minute / second / microsecond,
  //the result type should be date, not datetime or timestamp.

  // date_add('2012-2-29', interval '-1000000' microsecond) => 2012-02-28 23:59:59 .
  // date_add('2012-2-29', interval '-100000'  microsecond) => 2012-02-28 23:59:59.900000 .
  // date_add('2012-2-29 00:00:00.100', interval '-100000' microsecond) => 2012-02-29 00:00:00 .
  // so scale of literal datetime or date should be -1.

  //literal -> cast to string -> is date or datetime format?

  int ret = OB_SUCCESS;
  ObDateUnitType unit_type = static_cast<ObDateUnitType>(unit.get_param().get_int());
  ObExprResType res_date = date;
  if (OB_UNLIKELY(ObNullType == date.get_type())) {
    type.set_null();
  } else {
    unit.set_calc_type(ObIntType);
    if (ObDateTimeType == date.get_type() || ObTimestampType == date.get_type()) {
      type.set_datetime();
      date.set_calc_type(ObDateTimeType);
    } else if (ObMySQLDateTimeType == date.get_type()) {
      type.set_mysql_datetime();
      date.set_calc_type(ObMySQLDateTimeType);
    } else if (ObDateType == date.get_type() || ObMySQLDateType == date.get_type()) {
      if (DATE_UNIT_YEAR == unit_type || DATE_UNIT_MONTH == unit_type
          || DATE_UNIT_DAY == unit_type || DATE_UNIT_YEAR_MONTH == unit_type
          || DATE_UNIT_WEEK == unit_type || DATE_UNIT_QUARTER == unit_type) {
        type.set_type(date.get_type());
      } else {
        type.set_type(date.get_type() == ObMySQLDateType ? ObMySQLDateTimeType : ObDateTimeType);
      }
    } else if (ObTimeType == date.get_type()) {
      type.set_time();
    } else {
      date.set_calc_type(ObVarcharType);
      type.set_varchar();
      type.set_length(DATETIME_MAX_LENGTH);
      ret = aggregate_charsets_for_string_result(type, &date, 1, type_ctx);
      date.set_calc_collation_type(type.get_collation_type());
      date.set_calc_collation_level(type.get_collation_level());
    }
    interval.set_calc_type(ObVarcharType);
    if (OB_SUCC(ret)) {
      if (lib::is_oracle_mode()) {
        type.set_scale(static_cast<ObScale>(OB_MAX_DATE_PRECISION));
      } else { // mysql mode
        ObScale scale = 0;
        if (DATE_UNIT_MICROSECOND == unit_type
            || DATE_UNIT_SECOND_MICROSECOND == unit_type || DATE_UNIT_MINUTE_MICROSECOND == unit_type
            || DATE_UNIT_HOUR_MICROSECOND == unit_type || DATE_UNIT_DAY_MICROSECOND == unit_type) {
          scale = OB_MAX_DATETIME_PRECISION;
        } else if (DATE_UNIT_SECOND == unit_type) {
          if (0 <= interval.get_scale()) {
            scale = std::min(static_cast<ObScale>(OB_MAX_DATETIME_PRECISION),
                             interval.get_scale());
          } else {
            scale = OB_MAX_DATETIME_PRECISION;
          }
        }
        if (date.is_datetime() || date.is_timestamp() || date.is_time()
              || date.is_mysql_datetime()) {
          scale = std::max(date.get_scale(), scale);
        } else if (date.is_date() || date.is_mysql_date()) {
          if ((DATE_UNIT_DAY <= unit_type && unit_type <= DATE_UNIT_YEAR)
              || DATE_UNIT_YEAR_MONTH == unit_type) {
            scale = 0;
          } else {
            // do nothing
          }
        } else {
          // here res type is varchar
          scale = -1;
        }
        type.set_scale(scale);
      }
    }
  }
  // todo(jiuren)/(yeti):
  // for column :
  // date_unit not has microsecond and no default scale but the result has mircrosecond, so the scale should be result_scale
  // such as: select date_add(a, INTERVAL 0.1 second);
  return ret;
}

int ObExprDateAdjust::calc_date_adjust(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum,
                                       bool is_add)
{
  int ret = OB_SUCCESS;
  const ObSQLSessionInfo *session = NULL;
  const ObObjType res_type = expr.datum_meta_.type_;
  const ObObjType date_type = expr.args_[0]->datum_meta_.type_;
  ObDatum *date = NULL;
  ObDatum *interval = NULL;
  ObDatum *unit = NULL;
  bool is_json = (expr.args_[0]->args_ != NULL) && (expr.args_[0]->args_[0]->datum_meta_.type_ == ObJsonType);
  ObSolidifiedVarsGetter helper(expr, ctx, ctx.exec_ctx_.get_my_session());
  ObSQLMode sql_mode = 0;
  const common::ObTimeZoneInfo *tz_info = NULL;
  if (OB_ISNULL(session = ctx.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (OB_FAIL(expr.eval_param_value(ctx, date, interval, unit))) {
    LOG_WARN("eval param value failed");
  } else if (OB_UNLIKELY(date->is_null() || interval->is_null())
             || ObNullType == res_type) {
    expr_datum.set_null();
  } else if (OB_FAIL(helper.get_sql_mode(sql_mode))) {
    LOG_WARN("get sql mode failed", K(ret));
  } else if (OB_FAIL(helper.get_time_zone_info(tz_info))) {
    LOG_WARN("get tz info failed", K(ret));
  } else {
    int64_t dt_val = 0;
    ObString interval_val = interval->get_string();
    int64_t unit_value = unit->get_int();
    ObDateUnitType unit_val = static_cast<ObDateUnitType>(unit_value);
    int64_t res_dt_val = 0;
    bool has_set_value = false;
    if (ObDateTimeType == date_type || ObTimestampType == date_type) {
      dt_val = date->get_datetime();
    } else {
      ObTime ob_time;
      ObDateSqlMode date_sql_mode;
      ObDateSqlMode mysql_date_sql_mode;
      ObTimeConvertCtx cvrt_ctx(tz_info, false);
      date_sql_mode.init(sql_mode);
      mysql_date_sql_mode.allow_invalid_dates_ = date_sql_mode.allow_invalid_dates_;
      if (is_json) { // json to string will add quote automatically, here should take off quote
        ObString str = date->get_string();
        if (str.length() >= 3) { // 2 quote and content length >= 1
          date->set_string(str.ptr() + 1, str.length() - 1);
        }
      }
      bool need_check_date = ob_is_mysql_compact_dates_type(date_type);
      if (OB_FAIL(ob_datum_to_ob_time_with_date(*date, date_type, expr.args_[0]->datum_meta_.scale_,
                                            tz_info,
                                            ob_time,
                                            get_cur_time(ctx.exec_ctx_.get_physical_plan_ctx()),
                                            date_sql_mode,
                                            expr.args_[0]->obj_meta_.has_lob_header()))) {
        uint64_t cast_mode = 0;
        ObSQLUtils::get_default_cast_mode(session->get_stmt_type(),
                                          session->is_ignore_stmt(),
                                          sql_mode,
                                          cast_mode);
        if (CM_IS_WARN_ON_FAIL(cast_mode)) {
          expr_datum.set_null();
          ret = OB_SUCCESS;
          has_set_value = true;
        } else {
          LOG_WARN("datum to ob time failed", K(ret), K(date->get_string()), K(date_type));
        }
      } else if (need_check_date
                 && OB_FAIL(ObTimeConverter::validate_datetime(ob_time, mysql_date_sql_mode))) {
        ret = OB_SUCCESS;
        dt_val = ObTimeConverter::ZERO_DATETIME;
      } else if (OB_FAIL(ObTimeConverter::ob_time_to_datetime(ob_time, cvrt_ctx, dt_val))) {
        LOG_WARN("ob time to datetime failed", K(ret));
      }
    }

    if (OB_SUCC(ret) && !has_set_value) {
      ObDateSqlMode date_sql_mode;
      date_sql_mode.init(sql_mode);
      if (OB_UNLIKELY(ObTimeConverter::ZERO_DATETIME == dt_val)) {
        expr_datum.set_null();
      } else if (OB_FAIL(ObTimeConverter::date_adjust(dt_val, interval_val, unit_val, res_dt_val,
                                                      is_add, date_sql_mode))) {
        if (OB_UNLIKELY(OB_INVALID_DATE_VALUE == ret || OB_TOO_MANY_DATETIME_PARTS == ret)) {
          expr_datum.set_null();
          if (OB_TOO_MANY_DATETIME_PARTS == ret) {
            LOG_USER_WARN(OB_TOO_MANY_DATETIME_PARTS);
          }
          ret = OB_SUCCESS;
        }
      } else if (ObDateType == res_type) {
        int32_t d_val = 0;
        if (OB_FAIL(ObTimeConverter::datetime_to_date(res_dt_val, NULL, d_val))) {
          LOG_WARN("failed to cast datetime  to date ", K(res_dt_val), K(ret));
        } else {
          expr_datum.set_date(d_val);
        }
      } else if (ObTimeType == res_type) {
        int64_t cur_date = 0;
        cur_date = (dt_val / USECS_PER_DAY) * USECS_PER_DAY;
        if (res_dt_val - cur_date > OB_MAX_TIME || res_dt_val - cur_date < -OB_MAX_TIME) {
          expr_datum.set_null();
        } else if (((unit_val >= DATE_UNIT_MONTH && unit_val <= DATE_UNIT_YEAR)
                     || DATE_UNIT_YEAR_MONTH == unit_val) && res_dt_val != dt_val) {
          expr_datum.set_null();
        } else {
          expr_datum.set_time(res_dt_val - cur_date);
        }
      } else if (ObDateTimeType == res_type) {
        expr_datum.set_datetime(res_dt_val);
      } else if (ObMySQLDateType == res_type) {
        ObMySQLDate mdate = 0;
        if (OB_FAIL(ObTimeConverter::datetime_to_mdate(res_dt_val, NULL, mdate))) {
          LOG_WARN("failed to cast datetime  to date ", K(res_dt_val), K(ret));
        } else {
          expr_datum.set_mysql_date(mdate);
        }
      } else if (ObMySQLDateTimeType == res_type) {
        ObMySQLDateTime mdatetime = 0;
        if (OB_FAIL(ObTimeConverter::datetime_to_mdatetime(res_dt_val, mdatetime))) {
          LOG_WARN("failed to cast datetime  to date ", K(res_dt_val), K(ret));
        } else {
          expr_datum.set_mysql_datetime(mdatetime);
        }
      } else {     //RETURN STRING TYPE
        ObObjType res_date;
        bool dt_flag = false;
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret =
            ObTimeConverter::str_is_date_format(date->get_string(), dt_flag))) {
          uint64_t cast_mode = 0;
          ObSQLUtils::get_default_cast_mode(session->get_stmt_type(),
                                            session->is_ignore_stmt(),
                                            sql_mode,
                                            cast_mode);
          if (CM_IS_WARN_ON_FAIL(cast_mode) && OB_INVALID_DATE_FORMAT == tmp_ret) {
            expr_datum.set_null();
            has_set_value = true;
          } else {
            ret = tmp_ret;
            LOG_WARN("failed to cast str to datetime format", K(ret));
          }
        } else {
          if (dt_flag) {
            res_date = ObDateType;
          } else {
            res_date = ObDateTimeType;
          }
        }

        if (OB_SUCC(ret) && !has_set_value) {
          if (ObDateType == res_date && (DATE_UNIT_YEAR == unit_val
              || DATE_UNIT_MONTH == unit_val
              || DATE_UNIT_DAY == unit_val || DATE_UNIT_YEAR_MONTH == unit_val
              || DATE_UNIT_WEEK == unit_val || DATE_UNIT_QUARTER == unit_val)) {
            int32_t d_val = 0;
            if (OB_FAIL(ObTimeConverter::datetime_to_date(res_dt_val, NULL, d_val))) {
              LOG_WARN("failed to cast datetime  to date ", K(res_dt_val), K(ret));
            } else {
              ObTime ob_time;
              const int64_t date_buf_len = DATETIME_MAX_LENGTH + 1;
              char *buf = expr.get_str_res_mem(ctx, date_buf_len);
              int64_t pos = 0;
              if (OB_ISNULL(buf)) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                LOG_WARN("allocate memory failed", K(ret));
              } else if (OB_FAIL(ObTimeConverter::date_to_ob_time(d_val, ob_time))) {
                LOG_WARN("failed to convert days to ob time", K(ret));
              } else if (OB_FAIL(ObTimeConverter::ob_time_to_str(ob_time, DT_TYPE_DATE, 0, buf,
                                                                date_buf_len, pos, true))) {
                LOG_WARN("failed to convert ob time to string", K(ret));
              } else {
                expr_datum.ptr_ = buf;
                expr_datum.pack_ = static_cast<uint32_t>(pos);
              }
            }
          } else {
            const int64_t datetime_buf_len = DATETIME_MAX_LENGTH + 1;
            char *buf = expr.get_str_res_mem(ctx, datetime_buf_len);
            int64_t pos = 0;
            ObString format;
            if (OB_ISNULL(buf)) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("allocate memory failed", K(ret));
            } else if (OB_FAIL(ObTimeConverter::datetime_to_str(res_dt_val, NULL,
                                              format, -1, buf,
                                              datetime_buf_len, pos, true))) {
              LOG_WARN("failed to cast object to ObVarcharType ", K(ret));
            } else {
              expr_datum.ptr_ = buf;
              expr_datum.pack_ = static_cast<uint32_t>(pos);
            }
          }
        }
      }
    }
  }
  return ret;
}

DEF_SET_LOCAL_SESSION_VARS(ObExprDateAdjust, raw_expr) {
  int ret = OB_SUCCESS;
  if (is_mysql_mode()) {
    SET_LOCAL_SYSVAR_CAPACITY(2);
    EXPR_ADD_LOCAL_SYSVAR(SYS_VAR_SQL_MODE);
    EXPR_ADD_LOCAL_SYSVAR(SYS_VAR_TIME_ZONE);
  } else {
    SET_LOCAL_SYSVAR_CAPACITY(1);
    EXPR_ADD_LOCAL_SYSVAR(SYS_VAR_TIME_ZONE);
  }
  return ret;
}

ObExprDateAdd::ObExprDateAdd(ObIAllocator &alloc)
    : ObExprDateAdjust(alloc, T_FUN_SYS_DATE_ADD, N_DATE_ADD, 3, NOT_ROW_DIMENSION)
{}

int ObExprDateAdd::cg_expr(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  if (rt_expr.arg_cnt_ != 3) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("date_add expr should have 3 params", K(ret), K(rt_expr.arg_cnt_));
  } else if (OB_ISNULL(rt_expr.args_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children of date_add expr is null", K(ret), K(rt_expr.args_));
  } else if (OB_ISNULL(rt_expr.args_[0]) ||
              OB_ISNULL(rt_expr.args_[1]) ||
              OB_ISNULL(rt_expr.args_[2])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child of date_add expr is null", K(ret), K(rt_expr.args_[0]),
                                              K(rt_expr.args_[1]), K(rt_expr.args_[2]));
  } else {
    rt_expr.eval_func_ = ObExprDateAdd::calc_date_add;
    const ObObjType arg_type = rt_expr.args_[0]->datum_meta_.type_;
    const ObObjType res_type = rt_expr.datum_meta_.type_;
    // The vectorization of other types for the expression not completed yet.
    if ((ob_is_datetime_or_mysql_datetime_tc(arg_type) || ob_is_date_or_mysql_date(arg_type))
        && (ob_is_datetime_or_mysql_datetime_tc(res_type) || ob_is_date_or_mysql_date(res_type))) {  // if arg_tc is dt/dtt, res_tc is dt/dtt, so it's no need actually
      rt_expr.eval_vector_func_ = ObExprDateAdd::calc_date_add_vector;
    }
  }
  return ret;
}

int ObExprDateAdd::calc_date_add(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  return ObExprDateAdjust::calc_date_adjust(expr, ctx, expr_datum, true /* is_add */);
}

ObExprDateSub::ObExprDateSub(ObIAllocator &alloc)
    : ObExprDateAdjust(alloc, T_FUN_SYS_DATE_SUB, N_DATE_SUB, 3, NOT_ROW_DIMENSION)
{}

int ObExprDateSub::cg_expr(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  if (rt_expr.arg_cnt_ != 3) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("date_sub expr should have 3 params", K(ret), K(rt_expr.arg_cnt_));
  } else if (OB_ISNULL(rt_expr.args_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children of date_sub expr is null", K(ret), K(rt_expr.args_));
  } else if (OB_ISNULL(rt_expr.args_[0]) ||
              OB_ISNULL(rt_expr.args_[1]) ||
              OB_ISNULL(rt_expr.args_[2])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child of date_sub expr is null", K(ret), K(rt_expr.args_[0]),
              K(rt_expr.args_[1]), K(rt_expr.args_[2]));
  } else {
    rt_expr.eval_func_ = ObExprDateSub::calc_date_sub;
    const ObObjType arg_type = rt_expr.args_[0]->datum_meta_.type_;
    const ObObjType res_type = rt_expr.datum_meta_.type_;
    // The vectorization of other types for the expression not completed yet.
    if ((ob_is_datetime_or_mysql_datetime_tc(arg_type) || ob_is_date_or_mysql_date(arg_type))
        && (ob_is_datetime_or_mysql_datetime_tc(res_type) || ob_is_date_or_mysql_date(res_type))) { // if arg_tc is dt/dtt, res_tc is dt/dtt, so it's no need actually
      rt_expr.eval_vector_func_ = ObExprDateSub::calc_date_sub_vector;
    }
  }
  return ret;
}

int ObExprDateSub::calc_date_sub(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  return ObExprDateAdjust::calc_date_adjust(expr, ctx, expr_datum, false /* is_add */);
}

ObExprAddMonths::ObExprAddMonths(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_ADD_MONTHS, N_ADD_MONTHS, 2, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

int ObExprAddMonths::calc_result_type2(ObExprResType &type,
                                       ObExprResType &type1,
                                       ObExprResType &type2,
                                       common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  type.set_datetime();
  type.set_scale(OB_MAX_DATE_PRECISION);
  type1.set_calc_type(ObDateTimeType);
  type1.set_calc_scale(OB_MAX_DATETIME_PRECISION);
  type2.set_calc_type(ObNumberType);
  return ret;
}

int ObExprAddMonths::cg_expr(ObExprCGCtx &op_cg_ctx,
                    const ObRawExpr &raw_expr,
                    ObExpr &rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  if (rt_expr.arg_cnt_ != 2) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("add_months expr should have 2 params", K(ret), K(rt_expr.arg_cnt_));
  } else if (OB_ISNULL(rt_expr.args_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children of add_months expr is null", K(ret), K(rt_expr.args_));
  } else if (OB_ISNULL(rt_expr.args_[0]) ||
              OB_ISNULL(rt_expr.args_[1])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child of add_months expr is null", K(ret), K(rt_expr.args_[0]), K(rt_expr.args_[1]));
  } else {
    rt_expr.eval_func_ = ObExprAddMonths::calc_add_months;
  }
  return ret;
}

int ObExprAddMonths::calc_add_months(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *param1 = NULL;
  ObDatum *param2 = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, param1))) {
    LOG_WARN("eval first param value failed");
  } else if (param1->is_null()) { //短路计算
    expr_datum.set_null();
  } else if (OB_FAIL(expr.args_[1]->eval(ctx, param2))) {
    LOG_WARN("eval second param value failed");
  } else if (param2->is_null()) {
    expr_datum.set_null();
  } else {
    int64_t ori_date_utc = param1->get_datetime();
    number::ObNumber months(param2->get_number());
    int64_t months_int = 0;
    int64_t res_date_utc = 0;

    if (OB_FAIL(months.extract_valid_int64_with_trunc(months_int))) {
      LOG_WARN("fail to do round", K(ret));
    } else if (OB_FAIL(ObTimeConverter::date_add_nmonth(ori_date_utc, months_int,
                                                        res_date_utc, true))) {
      LOG_WARN("fail to date add nmonth", K(ret));
    } else {
      expr_datum.set_datetime(res_date_utc);
    }
  }
  return ret;
}

ObExprLastDay::ObExprLastDay(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_LAST_DAY, N_LAST_DAY, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

int ObExprLastDay::calc_result_type1(ObExprResType &type,
                                     ObExprResType &type1,
                                     common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  bool accept_input_type = type1.is_null()
      || ob_is_string_tc(type1.get_type())
      || type1.is_datetime()
      || type1.is_otimestamp_type()
      || is_mysql_mode();
  if (OB_UNLIKELY(!accept_input_type)) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("inconsistent type", K(ret), K(type1));
  } else {
    if (is_oracle_mode()) {
      type.set_datetime();
    } else {
      type.set_type(type_ctx.enable_mysql_compatible_dates() ? ObMySQLDateType : ObDateType);
    }
    type.set_scale(OB_MAX_DATE_PRECISION);
    type1.set_calc_type(type_ctx.enable_mysql_compatible_dates() ?
      ObMySQLDateTimeType : ObDateTimeType);
    type1.set_calc_scale(OB_MAX_DATETIME_PRECISION);
  }
  return ret;
}

int ObExprLastDay::cg_expr(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  if (rt_expr.arg_cnt_ != 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("lastday expr should have 1 param", K(ret), K(rt_expr.arg_cnt_));
  } else if (OB_ISNULL(rt_expr.args_) || OB_ISNULL(rt_expr.args_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children of lastday expr is null", K(ret), K(rt_expr.args_), K(rt_expr.args_[0]));
  } else {
    rt_expr.eval_func_ = ObExprLastDay::calc_last_day;
  }
  return ret;
}

int ObExprLastDay::calc_last_day(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *param1 = NULL;
  ObSQLSessionInfo *session = NULL;
  ObSolidifiedVarsGetter helper(expr, ctx, ctx.exec_ctx_.get_my_session());
  ObSQLMode sql_mode = 0;
  if (OB_ISNULL(session = ctx.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (OB_FAIL(expr.args_[0]->eval(ctx, param1))) {
    LOG_WARN("eval first param value failed");
  } else if (param1->is_null()) {
    expr_datum.set_null();
  } else if (OB_FAIL(helper.get_sql_mode(sql_mode))) {
    LOG_WARN("get sql mode failed", K(ret));
  } else {
    const ObObjType res_type = expr.datum_meta_.type_;
    ObDateSqlMode date_sql_mode;
    date_sql_mode.init(sql_mode);
    date_sql_mode.no_zero_in_date_ = is_no_zero_in_date(sql_mode);
    date_sql_mode.allow_incomplete_dates_ = !is_no_zero_in_date(sql_mode);
    if (ObMySQLDateType == res_type) {
      ObMySQLDate res_date;
      if (OB_FAIL(ObTimeConverter::calc_last_mdate_of_the_month(param1->get_mysql_datetime(),
                                                                res_date, date_sql_mode))) {
        LOG_WARN("fail to calc last mday", K(ret), K(param1->get_mysql_datetime()));
      } else {
        expr_datum.set_mysql_date(res_date);
      }
    } else {
      int64_t ori_date_utc = param1->get_datetime();
      int64_t res_date_utc = 0;
      if (OB_FAIL(ObTimeConverter::calc_last_date_of_the_month(ori_date_utc, res_date_utc,
                  res_type, date_sql_mode))) {
        LOG_WARN("fail to calc last mday", K(ret), K(ori_date_utc), K(res_date_utc));
      } else {
        if (is_oracle_mode()) {
          expr_datum.set_datetime(res_date_utc);
        } else {
          expr_datum.set_date(static_cast<int32_t>(res_date_utc));
        }
      }
    }
    if (OB_FAIL(ret)) {
      if (!is_oracle_mode()) {
        uint64_t cast_mode = 0;
        ObSQLUtils::get_default_cast_mode(session->get_stmt_type(),
                                          session->is_ignore_stmt(),
                                          sql_mode,
                                          cast_mode);
        if (CM_IS_WARN_ON_FAIL(cast_mode)) {
          expr_datum.set_null();
          ret = OB_SUCCESS;
        }
      }
    }
  }
  return ret;
}

DEF_SET_LOCAL_SESSION_VARS(ObExprLastDay, raw_expr) {
  int ret = OB_SUCCESS;
  if (is_mysql_mode()) {
    SET_LOCAL_SYSVAR_CAPACITY(1);
    EXPR_ADD_LOCAL_SYSVAR(SYS_VAR_SQL_MODE);
  }
  return ret;
}

ObExprNextDay::ObExprNextDay(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_NEXT_DAY, N_NEXT_DAY, 2, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

int ObExprNextDay::calc_result_type2(ObExprResType &type,
                                     ObExprResType &type1,
                                     ObExprResType &type2,
                                     common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  bool accept_input_type = type1.is_null()
      || ob_is_string_tc(type1.get_type())
      || type1.is_datetime()
      || type1.is_otimestamp_type();
  if (OB_UNLIKELY(!accept_input_type)) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("inconsistent type", K(ret), K(type1));
  } else {
    type.set_datetime();
    type.set_scale(OB_MAX_DATE_PRECISION);
    type1.set_calc_type(ObDateTimeType);
    type1.set_calc_scale(OB_MAX_DATETIME_PRECISION);
    if (ob_is_numeric_tc(type2.get_type_class())) {
      if (lib::is_oracle_mode()) {
        type2.set_calc_type(ObNumberType);
      } else {
        type2.set_calc_type(ObUInt64Type);

      }
    } else {
      type2.set_calc_type(ObVarcharType);
    }
  }
  return ret;
}

int ObExprNextDay::cg_expr(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  if (rt_expr.arg_cnt_ != 2) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("next_day expr should have 2 params", K(ret), K(rt_expr.arg_cnt_));
  } else if (OB_ISNULL(rt_expr.args_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children of next_day expr is null", K(ret), K(rt_expr.args_));
  } else if (OB_ISNULL(rt_expr.args_[0]) ||
              OB_ISNULL(rt_expr.args_[1])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child of next_day expr is null", K(ret), K(rt_expr.args_[0]), K(rt_expr.args_[1]));
  } else {
    rt_expr.eval_func_ = ObExprNextDay::calc_next_day;
  }
  return ret;
}

int ObExprNextDay::calc_next_day(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *param1 = NULL;
  ObDatum *param2 = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, param1))) {
    LOG_WARN("eval first param value failed");
  } else if (param1->is_null()) { //短路计算
    expr_datum.set_null();
  } else if (OB_FAIL(expr.args_[1]->eval(ctx, param2))) {
    LOG_WARN("eval second param value failed");
  } else if (param2->is_null()) {
    expr_datum.set_null();
  } else if (ob_is_numeric_tc(ob_obj_type_class(expr.args_[1]->datum_meta_.type_))) {
    int64_t ori_date_utc = param1->get_datetime();
    ObString wday_name = ObString(0,"");//not use
    int64_t wday_count = 0;
    int64_t res_date_utc = 0;
    number::ObNumber wdaycount(param2->get_number());
    if (OB_FAIL(wdaycount.extract_valid_int64_with_trunc(wday_count))) {
      LOG_WARN("fail to do round", K(ret));
    } else if (OB_FAIL(ObTimeConverter::calc_next_date_of_the_wday(ori_date_utc,
                                                            wday_name.trim(),
                                                            wday_count,
                                                            res_date_utc))) {
      LOG_WARN("fail to calc next mday", K(ret));
    } else {
      expr_datum.set_datetime(res_date_utc);
    }
  } else {
    int64_t ori_date_utc = param1->get_datetime();
    ObString wday_name = param2->get_string();
    int64_t wday_count = 0; //not use
    int64_t res_date_utc = 0;
    if (OB_FAIL(ObTimeConverter::calc_next_date_of_the_wday(ori_date_utc,
                                                            wday_name.trim(),
                                                            wday_count,
                                                            res_date_utc))) {
      LOG_WARN("fail to calc next mday", K(ret));
    } else {
      expr_datum.set_datetime(res_date_utc);
    }
  }
  return ret;
}


template <typename IN_TYPE, typename ArgVec, typename UnitVec, typename IntervalVec, typename ResVec>
int vector_date_add(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound,
                    const bool is_add, ObDateSqlMode date_sql_mode)
{
  int ret = OB_SUCCESS;
  UnitVec *unit_type_vec = static_cast<UnitVec *>(expr.args_[2]->get_vector(ctx));
  IntervalVec *interval_vec = static_cast<IntervalVec *>(expr.args_[1]->get_vector(ctx));
  ArgVec *arg_vec = static_cast<ArgVec *>(expr.args_[0]->get_vector(ctx));
  ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  const ObObjType res_type = expr.datum_meta_.type_;
  const ObObjType date_type = expr.args_[0]->datum_meta_.type_;
  int64_t tz_offset = 0;
  ObTime ob_time;

  for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
    if (skip.at(idx) || eval_flags.at(idx)) {
      continue;
    } else if (arg_vec->is_null(idx) ||
               unit_type_vec->is_null(idx) ||
               interval_vec->is_null(idx)) {
      res_vec->set_null(idx);
      eval_flags.set(idx);
      continue;
    }
    DateType date = 0;
    UsecType usec = 0;
    IN_TYPE in_val = *reinterpret_cast<const IN_TYPE*>(arg_vec->get_payload(idx));
    if (OB_FAIL(ObTimeConverter::parse_date_usec<IN_TYPE>(in_val, tz_offset, oceanbase::lib::is_oracle_mode(), date, usec))) {
      LOG_WARN("get date usec from vec failed", K(ret), K(date), K(usec), K(tz_offset));
    } else if (OB_UNLIKELY(ObTimeConverter::ZERO_DATE == date)) {
      res_vec->set_null(idx);
      eval_flags.set(idx);
    } else {
      int64_t res_dt_val = 0;
      int64_t dt_val = date * USECS_PER_DAY + usec;
      bool need_check_date = ob_is_mysql_compact_dates_type(date_type);
      ObDateSqlMode tmp_date_sql_mode;
      tmp_date_sql_mode.allow_invalid_dates_ = date_sql_mode.allow_invalid_dates_;
      if (need_check_date) {
        if (OB_FAIL(ObTimeConverter::date_to_ob_time(date, ob_time))) {
          LOG_WARN("date_to_ob_time fail", K(ret), K(date));
        } else if (OB_FAIL(ObTimeConverter::validate_datetime(ob_time, tmp_date_sql_mode))) {
          ret = OB_SUCCESS;
          dt_val = ObTimeConverter::ZERO_DATETIME;
        }
      }
      if (OB_FAIL(ret)) {
        // do nothing
      } else if (OB_FAIL(ObTimeConverter::date_adjust(dt_val, interval_vec->get_string(idx),
                                               static_cast<ObDateUnitType>(unit_type_vec->get_int(idx)),
                                               res_dt_val, is_add, date_sql_mode))) {
        if (OB_UNLIKELY(OB_INVALID_DATE_VALUE == ret || OB_TOO_MANY_DATETIME_PARTS == ret)) {
          res_vec->set_null(idx);
          eval_flags.set(idx);
          if (OB_TOO_MANY_DATETIME_PARTS == ret) {
            // LOG_USER_WARN(OB_TOO_MANY_DATETIME_PARTS);
            LOG_WARN("OB_TOO_MANY_DATETIME_PARTS", K(ret));
          }
          ret = OB_SUCCESS;
        }
      } else if (ObDateType == res_type) {
        int32_t d_val = 0;
        if (OB_FAIL(ObTimeConverter::datetime_to_date(res_dt_val, NULL, d_val))) {
          LOG_WARN("failed to cast datetime  to date ", K(res_dt_val), K(ret));
        } else {
          res_vec->set_date(idx, d_val);
          eval_flags.set(idx);
        }
      } else if (ObDateTimeType == res_type) {
        res_vec->set_datetime(idx, res_dt_val);
        eval_flags.set(idx);
      } else if (ObMySQLDateType == res_type) {
        ObMySQLDate md_val = 0;
        if (OB_FAIL(ObTimeConverter::datetime_to_mdate(res_dt_val, NULL, md_val))) {
          LOG_WARN("failed to cast datetime  to date ", K(res_dt_val), K(ret));
        } else {
          res_vec->set_mysql_date(idx, md_val);
          eval_flags.set(idx);
        }
      } else if (ObMySQLDateTimeType == res_type) {
        ObMySQLDateTime mdatetime = 0;
        if (OB_FAIL(ObTimeConverter::datetime_to_mdatetime(res_dt_val, mdatetime))) {
          LOG_WARN("failed to cast datetime  to date ", K(res_dt_val), K(ret));
        } else {
          res_vec->set_mysql_datetime(idx, mdatetime);
          eval_flags.set(idx);
        }

      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid type of result", K(ret));
      }
    }
  }
  return ret;
}

template<bool IS_ADD>
int vector_date_adjust(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound) {
  int ret = OB_SUCCESS;
  const ObSQLSessionInfo *session = NULL;
  const ObObjType res_type = expr.datum_meta_.type_;
  const ObObjType arg_type = expr.args_[0]->datum_meta_.type_;
  ObSolidifiedVarsGetter helper(expr, ctx, ctx.exec_ctx_.get_my_session());
  ObSQLMode sql_mode = 0;
  const oceanbase::common::ObTimeZoneInfo *tz_info = NULL;
  if (OB_ISNULL(session = ctx.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))
          || OB_FAIL(expr.args_[1]->eval_vector(ctx, skip, bound))
          || OB_FAIL(expr.args_[2]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("eval arg failed", K(ret));
  } else if (OB_FAIL(helper.get_sql_mode(sql_mode))) {
    LOG_WARN("get sql mode failed", K(ret));
  } else if (OB_FAIL(helper.get_time_zone_info(tz_info))) {
    LOG_WARN("get tz info failed", K(ret));
  } else {
    VectorFormat arg_format = expr.args_[0]->get_format(ctx);
    VectorFormat interval_format = expr.args_[1]->get_format(ctx);
    VectorFormat unit_format = expr.args_[2]->get_format(ctx);
    VectorFormat res_format = expr.get_format(ctx);
    ObObjTypeClass arg_tc = ob_obj_type_class(expr.args_[0]->datum_meta_.type_);
    ObObjTypeClass res_tc = ob_obj_type_class(expr.datum_meta_.type_);
    ObDateSqlMode date_sql_mode;
    date_sql_mode.init(sql_mode);

#define DEF_DATE_ADD_VECTOR(IN_TYPE, arg_type, res_type)\
  if (VEC_FIXED == unit_format && VEC_FIXED == interval_format) {\
    ret = vector_date_add<IN_TYPE, arg_type, IntegerFixedVec, IntegerFixedVec, res_type>\
              (expr, ctx, skip, bound, IS_ADD, date_sql_mode);\
  } else if (VEC_FIXED == unit_format && VEC_UNIFORM == interval_format) {\
    ret = vector_date_add<IN_TYPE, arg_type, IntegerFixedVec, IntegerUniVec, res_type>\
              (expr, ctx, skip, bound, IS_ADD, date_sql_mode);\
  } else if (VEC_FIXED == unit_format && VEC_UNIFORM_CONST == interval_format) {\
    ret = vector_date_add<IN_TYPE, arg_type, IntegerFixedVec, IntegerUniCVec, res_type>\
              (expr, ctx, skip, bound, IS_ADD, date_sql_mode);\
  } else if (VEC_UNIFORM == unit_format && VEC_FIXED == interval_format) {\
    ret = vector_date_add<IN_TYPE, arg_type, IntegerUniVec, IntegerFixedVec, res_type>\
              (expr, ctx, skip, bound, IS_ADD, date_sql_mode);\
  } else if (VEC_UNIFORM == unit_format && VEC_UNIFORM == interval_format) {\
    ret = vector_date_add<IN_TYPE, arg_type, IntegerUniVec, IntegerUniVec, res_type>\
              (expr, ctx, skip, bound, IS_ADD, date_sql_mode);\
  } else if (VEC_UNIFORM == unit_format && VEC_UNIFORM_CONST == interval_format) {\
    ret = vector_date_add<IN_TYPE, arg_type, IntegerUniVec, IntegerUniCVec, res_type>\
              (expr, ctx, skip, bound, IS_ADD, date_sql_mode);\
  } else if (VEC_UNIFORM_CONST == unit_format && VEC_UNIFORM_CONST == interval_format) {\
    ret = vector_date_add<IN_TYPE, arg_type, IntegerUniCVec, IntegerUniCVec, res_type>\
              (expr, ctx, skip, bound, IS_ADD, date_sql_mode);\
  } else if (VEC_UNIFORM_CONST == unit_format && VEC_UNIFORM == interval_format) {\
    ret = vector_date_add<IN_TYPE, arg_type, IntegerUniCVec, IntegerUniVec, res_type>\
              (expr, ctx, skip, bound, IS_ADD, date_sql_mode);\
  } else if (VEC_UNIFORM_CONST == unit_format && VEC_FIXED == interval_format) {\
    ret = vector_date_add<IN_TYPE, arg_type, IntegerUniCVec, IntegerFixedVec, res_type>\
              (expr, ctx, skip, bound, IS_ADD, date_sql_mode);\
  } else {\
    ret = vector_date_add<IN_TYPE, ObVectorBase, ObVectorBase, ObVectorBase, ObVectorBase>\
              (expr, ctx, skip, bound, IS_ADD, date_sql_mode);\
  }
#define DEF_DATE_ADD_WRAP(IN_TYPE, arg_tc, res_tc)\
  if (VEC_UNIFORM == arg_format && VEC_FIXED == res_format) {\
    DEF_DATE_ADD_VECTOR(IN_TYPE, CONCAT(arg_tc, UniVec), CONCAT(res_tc, FixedVec));\
  } else if (VEC_UNIFORM == arg_format && VEC_UNIFORM == res_format) {\
    DEF_DATE_ADD_VECTOR(IN_TYPE, CONCAT(arg_tc, UniVec), CONCAT(res_tc, UniVec));\
  } else if (VEC_UNIFORM == arg_format && VEC_UNIFORM_CONST == res_format) {\
    DEF_DATE_ADD_VECTOR(IN_TYPE, CONCAT(arg_tc, UniVec), CONCAT(res_tc, UniCVec));\
  } else if (VEC_UNIFORM_CONST == arg_format && VEC_UNIFORM == res_format) {\
    DEF_DATE_ADD_VECTOR(IN_TYPE, CONCAT(arg_tc, UniCVec), CONCAT(res_tc, UniVec));\
  } else if (VEC_UNIFORM_CONST == arg_format && VEC_FIXED == res_format) {\
    DEF_DATE_ADD_VECTOR(IN_TYPE, CONCAT(arg_tc, UniCVec), CONCAT(res_tc, FixedVec));\
  } else if (VEC_UNIFORM_CONST == arg_format && VEC_UNIFORM_CONST == res_format) {\
    DEF_DATE_ADD_VECTOR(IN_TYPE, CONCAT(arg_tc, UniCVec), CONCAT(res_tc, UniCVec));\
  } else if (VEC_FIXED == arg_format && VEC_UNIFORM == res_format) {\
    DEF_DATE_ADD_VECTOR(IN_TYPE, CONCAT(arg_tc, FixedVec), CONCAT(res_tc, UniVec));\
  } else if (VEC_FIXED == arg_format && VEC_FIXED == res_format) {\
    DEF_DATE_ADD_VECTOR(IN_TYPE, CONCAT(arg_tc, FixedVec), CONCAT(res_tc, FixedVec));\
  } else if (VEC_FIXED == arg_format && VEC_UNIFORM_CONST == res_format) {\
    DEF_DATE_ADD_VECTOR(IN_TYPE, CONCAT(arg_tc, FixedVec), CONCAT(res_tc, UniCVec));\
  } else {\
    ret = vector_date_add<IN_TYPE, ObVectorBase, ObVectorBase, ObVectorBase, ObVectorBase>\
              (expr, ctx, skip, bound, IS_ADD, date_sql_mode);\
  }

    if (ObMySQLDateTimeTC == arg_tc && ObMySQLDateTimeTC == res_tc) {
      DEF_DATE_ADD_WRAP(ObMySQLDateTime, MySQLDateTime, MySQLDateTime)
    } else if (ObMySQLDateTC == arg_tc && ObMySQLDateTC == res_tc) {
      DEF_DATE_ADD_WRAP(ObMySQLDate, MySQLDate, MySQLDate)
    } else if (ObMySQLDateTC == arg_tc && ObMySQLDateTimeTC == res_tc) {
      DEF_DATE_ADD_WRAP(ObMySQLDate, MySQLDate, MySQLDateTime)
    } else if (ObDateTimeTC == arg_tc && ObDateTimeTC == res_tc) {
      DEF_DATE_ADD_WRAP(DateTimeType, DateTime, DateTime)
    } else if (ObDateTC == arg_tc && ObDateTC == res_tc) {
      DEF_DATE_ADD_WRAP(DateType, Date, Date)
    } else if (ObDateTC == arg_tc && ObDateTimeTC == res_tc) {
      DEF_DATE_ADD_WRAP(DateType, Date, DateTime)
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("DATA TYPE NOT SUPPORTED", K(ret));
    }
#undef DEF_DATE_ADD_WRAP
#undef DEF_DATE_ADD_VECTOR

    if (OB_FAIL(ret)) {
      LOG_WARN("expr calculation failed", K(ret));
    }
  }
  return ret;
}

int ObExprDateAdd::calc_date_add_vector(const ObExpr &expr,
                                        ObEvalCtx &ctx,
                                        const ObBitVector &skip,
                                        const EvalBound &bound) {
  return vector_date_adjust<true>(expr, ctx, skip, bound);
}
int ObExprDateSub::calc_date_sub_vector(const ObExpr &expr,
                                        ObEvalCtx &ctx,
                                        const ObBitVector &skip,
                                        const EvalBound &bound) {
  return vector_date_adjust<false>(expr, ctx, skip, bound);
}

#undef CHECK_SKIP_NULL
#undef BATCH_CALC