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
#include "lib/ob_date_unit_type.h"
#include "lib/timezone/ob_time_convert.h"
#include "lib/ob_name_def.h"
#include "share/object/ob_obj_cast.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"
#include "ob_datum_cast.h"
#include "common/object/ob_obj_type.h"
using namespace oceanbase::common;
using namespace oceanbase::sql;

ObExprDateAdjust::ObExprDateAdjust(ObIAllocator &alloc,
                                   ObExprOperatorType type,
                                   const char *name,
                                   int32_t param_num,
                                   int32_t dimension)
    : ObFuncExprOperator(alloc, type, name, param_num, NOT_VALID_FOR_GENERATED_COL, dimension)
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
  const ObSQLSessionInfo *session = static_cast<const ObSQLSessionInfo*>(type_ctx.get_session());
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (OB_UNLIKELY(ObNullType == date.get_type())) {
    type.set_null();
  } else {
    unit.set_calc_type(ObIntType);
    if (ObDateTimeType == date.get_type() || ObTimestampType == date.get_type()) {
      type.set_datetime();
      date.set_calc_type(ObDateTimeType);
    } else if (ObDateType == date.get_type()) {
      if (DATE_UNIT_YEAR == unit_type || DATE_UNIT_MONTH == unit_type
          || DATE_UNIT_DAY == unit_type || DATE_UNIT_YEAR_MONTH == unit_type
          || DATE_UNIT_WEEK == unit_type || DATE_UNIT_QUARTER == unit_type) {
        type.set_date();
      } else {
        type.set_datetime();
      }
    } else if (ObTimeType == date.get_type()) {
      type.set_time();
    } else {
      date.set_calc_type(ObVarcharType);
      type.set_varchar();
      type.set_length(DATETIME_MAX_LENGTH);
      ret = aggregate_charsets_for_string_result(type, &date, 1, type_ctx.get_coll_type());
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
        if (date.is_datetime() || date.is_timestamp() || date.is_time()) {
          scale = std::max(date.get_scale(), scale);
        } else if (date.is_date()) {
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
  if (OB_ISNULL(session = ctx.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (OB_FAIL(expr.eval_param_value(ctx, date, interval, unit))) {
    LOG_WARN("eval param value failed");
  } else if (OB_UNLIKELY(date->is_null() || interval->is_null())
             || ObNullType == res_type) {
    expr_datum.set_null();
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
      ObTimeConvertCtx cvrt_ctx(get_timezone_info(session), false);
      date_sql_mode.init(session->get_sql_mode());
      if (is_json) { // json to string will add quote automatically, here should take off quote
        ObString str = date->get_string();
        if (str.length() >= 3) { // 2 quote and content length >= 1
          date->set_string(str.ptr() + 1, str.length() - 1);
        }
      }
      if (OB_FAIL(ob_datum_to_ob_time_with_date(*date, date_type,
                                            get_timezone_info(session),
                                            ob_time,
                                            get_cur_time(ctx.exec_ctx_.get_physical_plan_ctx()),
                                            false, date_sql_mode,
                                            expr.args_[0]->obj_meta_.has_lob_header()))) {
        uint64_t cast_mode = 0;
        ObSQLUtils::get_default_cast_mode(session->get_stmt_type(), session, cast_mode);
        if (CM_IS_WARN_ON_FAIL(cast_mode)) {
          expr_datum.set_null();
          ret = OB_SUCCESS;
          has_set_value = true;
        } else {
          LOG_WARN("datum to ob time failed", K(ret), K(date->get_string()), K(date_type));
        }
      } else if (OB_FAIL(ObTimeConverter::ob_time_to_datetime(ob_time, cvrt_ctx, dt_val))) {
        LOG_WARN("ob time to datetime failed", K(ret));
      }
    }

    if (OB_SUCC(ret) && !has_set_value) {
      ObDateSqlMode date_sql_mode;
      date_sql_mode.init(session->get_sql_mode());
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
      } else {     //RETURN STRING TYPE
        ObObjType res_date;
        bool dt_flag = false;
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret =
            ObTimeConverter::str_is_date_format(date->get_string(), dt_flag))) {
          uint64_t cast_mode = 0;
          ObSQLUtils::get_default_cast_mode(session->get_stmt_type(), session, cast_mode);
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

int ObExprDateAdjust::is_valid_for_generated_column(const ObRawExpr*expr, const common::ObIArray<ObRawExpr *> &exprs, bool &is_valid) const {
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_first_param_not_time(exprs, is_valid))) {
    LOG_WARN("fail to check if first param is time", K(ret), K(exprs));
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
      type.set_date();
    }
    type.set_scale(OB_MAX_DATE_PRECISION);
    type1.set_calc_type(ObDateTimeType);
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
  if (OB_ISNULL(session = ctx.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (OB_FAIL(expr.args_[0]->eval(ctx, param1))) {
    LOG_WARN("eval first param value failed");
  } else if (param1->is_null()) {
    expr_datum.set_null();
  } else {
    const ObObjType res_type = is_oracle_mode() ? ObDateTimeType : ObDateType;
    int64_t ori_date_utc = param1->get_datetime();
    int64_t res_date_utc = 0;
    ObDateSqlMode date_sql_mode;
    date_sql_mode.init(session->get_sql_mode());
    if (OB_FAIL(ObTimeConverter::calc_last_date_of_the_month(ori_date_utc, res_date_utc,
                res_type, !is_no_zero_in_date(session->get_sql_mode()), date_sql_mode))) {
      LOG_WARN("fail to calc last mday", K(ret), K(ori_date_utc), K(res_date_utc));
      if (!is_oracle_mode()) {
        uint64_t cast_mode = 0;
        ObSQLUtils::get_default_cast_mode(session->get_stmt_type(), session, cast_mode);
        if (CM_IS_WARN_ON_FAIL(cast_mode)) {
          expr_datum.set_null();
          ret = OB_SUCCESS;
        }
      }
    } else {
      if (is_oracle_mode()) {
        expr_datum.set_datetime(res_date_utc);
      } else {
        expr_datum.set_date(static_cast<int32_t>(res_date_utc));
      }
    }
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
