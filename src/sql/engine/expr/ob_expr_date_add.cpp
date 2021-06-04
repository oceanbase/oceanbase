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

#include "sql/engine/expr/ob_expr_date_add.h"
#include "lib/ob_date_unit_type.h"
#include "lib/timezone/ob_time_convert.h"
#include "lib/ob_name_def.h"
#include "share/object/ob_obj_cast.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"
#include "ob_datum_cast.h"
using namespace oceanbase::common;
using namespace oceanbase::sql;

ObExprDateAdjust::ObExprDateAdjust(
    ObIAllocator& alloc, ObExprOperatorType type, const char* name, int32_t param_num, int32_t dimension)
    : ObFuncExprOperator(alloc, type, name, param_num, dimension)
{}

ObExprDateAdjust::~ObExprDateAdjust()
{}

int ObExprDateAdjust::calc_result_type3(ObExprResType& type, ObExprResType& date, ObExprResType& interval,
    ObExprResType& unit, ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  UNUSED(interval);
  // if date is ObDateType, and date_uint contains no hour / minute / second / microsecond,
  // the result type should be date, not datetime or timestamp.

  // date_add('2012-2-29', interval '-1000000' microsecond) => 2012-02-28 23:59:59 .
  // date_add('2012-2-29', interval '-100000'  microsecond) => 2012-02-28 23:59:59.900000 .
  // date_add('2012-2-29 00:00:00.100', interval '-100000' microsecond) => 2012-02-29 00:00:00 .
  // so scale of literal datetime or date should be -1.

  // literal -> cast to string -> is date or datetime format?

  int ret = OB_SUCCESS;
  ObDateUnitType unit_type = static_cast<ObDateUnitType>(unit.get_param().get_int());
  ObExprResType res_date = date;
  const ObSQLSessionInfo* session = static_cast<const ObSQLSessionInfo*>(type_ctx.get_session());
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (OB_UNLIKELY(ObNullType == date.get_type())) {
    type.set_null();
  } else {
    unit.set_calc_type(ObIntType);
    bool use_static_engine = session->use_static_typing_engine();
    if (ObDateTimeType == date.get_type() || ObTimestampType == date.get_type()) {
      type.set_datetime();
      if (use_static_engine) {
        date.set_calc_type(ObDateTimeType);
      }
    } else if (ObDateType == date.get_type()) {
      if (DATE_UNIT_YEAR == unit_type || DATE_UNIT_MONTH == unit_type || DATE_UNIT_DAY == unit_type ||
          DATE_UNIT_YEAR_MONTH == unit_type || DATE_UNIT_WEEK == unit_type || DATE_UNIT_QUARTER == unit_type) {
        type.set_date();
      } else {
        type.set_datetime();
      }
    } else if (ObTimeType == date.get_type()) {
      type.set_time();
    } else {
      if (use_static_engine) {
        date.set_calc_type(ObVarcharType);
      }
      type.set_varchar();
      type.set_length(DATETIME_MAX_LENGTH);
      ret = aggregate_charsets_for_string_result(type, &date, 1, type_ctx.get_coll_type());
    }
    if (use_static_engine) {
      interval.set_calc_type(ObVarcharType);
    }
    if (OB_SUCC(ret)) {
      if (share::is_oracle_mode()) {
        type.set_scale(static_cast<ObScale>(OB_MAX_DATE_PRECISION));
      } else {  // mysql mode
        ObScale scale = 0;
        if (DATE_UNIT_MICROSECOND == unit_type || DATE_UNIT_SECOND_MICROSECOND == unit_type ||
            DATE_UNIT_MINUTE_MICROSECOND == unit_type || DATE_UNIT_HOUR_MICROSECOND == unit_type ||
            DATE_UNIT_DAY_MICROSECOND == unit_type) {
          scale = OB_MAX_DATETIME_PRECISION;
        } else if (DATE_UNIT_SECOND == unit_type) {
          if (0 <= interval.get_scale()) {
            scale = std::min(static_cast<ObScale>(OB_MAX_DATETIME_PRECISION), interval.get_scale());
          } else {
            scale = OB_MAX_DATETIME_PRECISION;
          }
        }
        if (date.is_datetime() || date.is_timestamp() || date.is_time()) {
          scale = std::max(date.get_scale(), scale);
        } else if (date.is_date()) {
          if ((DATE_UNIT_DAY <= unit_type && unit_type <= DATE_UNIT_YEAR) || DATE_UNIT_YEAR_MONTH == unit_type) {
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
  // todo:
  // for column :
  // date_unit not has microsecond and no default scale but the result has mircrosecond, so the scale should be
  // result_scale such as: select date_add(a, INTERVAL 0.1 second);
  return ret;
}

int ObExprDateAdjust::calc_result3(ObObj& result, const ObObj& date, const ObObj& interval, const ObObj& unit,
    ObExprCtx& expr_ctx, bool is_add, const ObExprResType& res_type) const
{
  /**
   * TODO
   * the performance of this function is not so good in some cases, like date is varchar and
   * unit is year_month, because the calculation is based on ob_time. we will do three cast
   * operations for date: varchar -> ob_time -> datetime -> ob_time, in which we actually need
   * only one: varchar -> ob_time.
   * the optimization should expose ob_time and related functions to date add operation,
   * especially those with an ob_time struct as input, like ob_time_to_datetime, which is private
   * now to avoid extra ob_time validation.
   * so how to resolve this problem without exposing this private functions?
   * remember we have another time library named ObTimeUtility? we can expose these functions
   * to ObTimeUtility using inheritance and protected function, and keeps private to other classes.
   * BUT!!! this optimization will take lots of work, so I have to do it in future, not NOW.
   */

  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is null", K(ret));
  } else if (OB_UNLIKELY(date.is_null() || interval.is_null()) || ObNullType == res_type.get_type()) {
    result.set_null();
  } else {
    int64_t dt_val = 0;
    ObString interval_val;
    int64_t unit_value = 0;
    bool is_neg = false;
    number::ObNumber interval_num;
    if (OB_SUCC(unit.get_int(unit_value))) {
      ObDateUnitType unit_val = static_cast<ObDateUnitType>(unit_value);
      int64_t res_dt_val = 0;
      EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
      EXPR_GET_DATETIME_V2(date, dt_val);
      EXPR_GET_VARCHAR_V2(interval, interval_val);
      if (OB_SUCC(ret) && share::is_oracle_mode()) {
        if (OB_FAIL(interval_num.from(interval_val.ptr(), interval_val.length(), *expr_ctx.calc_buf_))) {
          ret = OB_ERR_INTERVAL_INVALID;
          LOG_WARN("the interval is invalid in oracle mode", K(ret));
        } else if (!interval_num.is_integer()) {
          is_neg = interval_num.is_negative();
          if (DATE_UNIT_SECOND != unit_val) {
            ret = OB_ERR_INTERVAL_INVALID;
            LOG_WARN("the interval is invalid in oracle mode", K(ret));
          } else {
            uint64_t sub_num = 1;
            number::ObNumber tmp_sub_num;
            // add a positive or minus a negative number, truncate decimal part.
            if (OB_FAIL(interval_num.trunc(0))) {
              LOG_WARN("trunc decimal failed", K(ret));
            } else if ((is_add && is_neg)) {
              if (OB_FAIL(tmp_sub_num.from(sub_num, *expr_ctx.calc_buf_))) {
                LOG_WARN("fail to trans sub_num to tmp_sub_num", K(ret));
              } else if (OB_FAIL(interval_num.sub(tmp_sub_num, interval_num, *expr_ctx.calc_buf_))) {
                LOG_WARN("fail to sub tmp_sub_num", K(ret));
              }
            } else if (!is_add && !is_neg) {
              if (OB_FAIL(tmp_sub_num.from(sub_num, *expr_ctx.calc_buf_))) {
                LOG_WARN("fail to trans sub_num to tmp_sub_num", K(ret));
              } else if (OB_FAIL(interval_num.add(tmp_sub_num, interval_num, *expr_ctx.calc_buf_))) {
                LOG_WARN("fail to add tmp_sub_num", K(ret));
              }
            }
            if (OB_SUCC(ret)) {
              ObObj tmp_obj;
              tmp_obj.set_number(interval_num);
              EXPR_GET_VARCHAR_V2(tmp_obj, interval_val);
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_UNLIKELY(ObTimeConverter::ZERO_DATETIME == dt_val)) {
          result.set_null();
        } else if (OB_FAIL(ObTimeConverter::date_adjust(dt_val, interval_val, unit_val, res_dt_val, is_add))) {
          if (OB_UNLIKELY(OB_INVALID_DATE_VALUE == ret)) {
            result.set_null();
            ret = OB_SUCCESS;
          }
        } else if (res_type.is_date()) {
          int32_t d_val = 0;
          if (OB_FAIL(ObTimeConverter::datetime_to_date(res_dt_val, NULL, d_val))) {
            LOG_WARN("failed to cast datetime  to date ", K(res_dt_val), K(ret));
          } else {
            result.set_date(d_val);
          }
        } else if (res_type.is_time()) {
          int64_t cur_date = 0;
          cur_date = (dt_val / USECS_PER_DAY) * USECS_PER_DAY;
          if (res_dt_val - cur_date > OB_MAX_TIME || res_dt_val - cur_date < -OB_MAX_TIME) {
            result.set_null();
          } else if (((unit_val >= DATE_UNIT_MONTH && unit_val <= DATE_UNIT_YEAR) ||
                         DATE_UNIT_YEAR_MONTH == unit_val) &&
                     res_dt_val != dt_val) {
            result.set_null();
          } else {
            result.set_time(res_dt_val - cur_date);
          }
        } else if (res_type.is_datetime()) {
          result.set_datetime(res_dt_val);
        } else {  // RETURN STRING TYPE
          ObObj date_obj = date;
          ObObj res_date;
          bool has_set_value = false;
          // cast in place
          if (OB_FAIL(ObObjCaster::to_type(ObVarcharType, cast_ctx, date_obj, date_obj))) {
            LOG_WARN("failed to cast to string", K(ret), K(date_obj));
          } else {
            bool dt_flag = false;
            int tmp_ret = OB_SUCCESS;
            if (OB_SUCCESS != (tmp_ret = ObTimeConverter::str_is_date_format(date_obj.get_string(), dt_flag))) {
              if (CM_IS_WARN_ON_FAIL(expr_ctx.cast_mode_) && OB_INVALID_DATE_FORMAT == tmp_ret) {
                result.set_null();
                has_set_value = true;
              } else {
                ret = tmp_ret;
                LOG_WARN("failed to cast str to datetime format", K(ret));
              }
            } else {
              if (dt_flag) {
                res_date.set_type(ObDateType);
              } else {
                res_date.set_type(ObDateTimeType);
              }
            }
          }
          if (OB_SUCC(ret) && !has_set_value) {
            if (res_date.is_date() &&
                (DATE_UNIT_YEAR == unit_val || DATE_UNIT_MONTH == unit_val || DATE_UNIT_DAY == unit_val ||
                    DATE_UNIT_YEAR_MONTH == unit_val || DATE_UNIT_WEEK == unit_val || DATE_UNIT_QUARTER == unit_val)) {
              int32_t d_val = 0;
              if (OB_FAIL(ObTimeConverter::datetime_to_date(res_dt_val, NULL, d_val))) {
                LOG_WARN("failed to cast datetime  to date ", K(res_dt_val), K(ret));
              } else {
                result.set_date(d_val);
                if (OB_FAIL(ObObjCaster::to_type(ObVarcharType, cast_ctx, result, result))) {
                  LOG_WARN("failed to cast object to ObVarcharType ", K(result), K(ret));
                }
              }
            } else {
              result.set_datetime(res_dt_val);
              if (OB_FAIL(ObObjCaster::to_type(ObVarcharType, cast_ctx, result, result))) {
                LOG_WARN("failed to cast object to ObVarcharType ", K(result), K(ret));
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObExprDateAdjust::calc_date_adjust(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum, bool is_add)
{
  int ret = OB_SUCCESS;
  const ObSQLSessionInfo* session = NULL;
  const ObObjType res_type = expr.datum_meta_.type_;
  const ObObjType date_type = expr.args_[0]->datum_meta_.type_;
  ObIAllocator& tmp_alloc = ctx.get_reset_tmp_alloc();
  ObDatum* date = NULL;
  ObDatum* interval = NULL;
  ObDatum* unit = NULL;
  if (OB_ISNULL(session = ctx.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (OB_FAIL(expr.eval_param_value(ctx, date, interval, unit))) {
    LOG_WARN("eval param value failed");
  } else if (OB_UNLIKELY(date->is_null() || interval->is_null()) || ObNullType == res_type) {
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
      ObTimeConvertCtx cvrt_ctx(get_timezone_info(session), false);
      if (OB_FAIL(ob_datum_to_ob_time_with_date(*date,
              date_type,
              get_timezone_info(session),
              ob_time,
              get_cur_time(ctx.exec_ctx_.get_physical_plan_ctx())))) {
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
      if (OB_UNLIKELY(ObTimeConverter::ZERO_DATETIME == dt_val)) {
        expr_datum.set_null();
      } else if (OB_FAIL(ObTimeConverter::date_adjust(dt_val, interval_val, unit_val, res_dt_val, is_add))) {
        if (OB_UNLIKELY(OB_INVALID_DATE_VALUE == ret)) {
          expr_datum.set_null();
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
        } else if (((unit_val >= DATE_UNIT_MONTH && unit_val <= DATE_UNIT_YEAR) || DATE_UNIT_YEAR_MONTH == unit_val) &&
                   res_dt_val != dt_val) {
          expr_datum.set_null();
        } else {
          expr_datum.set_time(res_dt_val - cur_date);
        }
      } else if (ObDateTimeType == res_type) {
        expr_datum.set_datetime(res_dt_val);
      } else {  // RETURN STRING TYPE
        ObObjType res_date;
        bool dt_flag = false;
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = ObTimeConverter::str_is_date_format(date->get_string(), dt_flag))) {
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
          if (ObDateType == res_date &&
              (DATE_UNIT_YEAR == unit_val || DATE_UNIT_MONTH == unit_val || DATE_UNIT_DAY == unit_val ||
                  DATE_UNIT_YEAR_MONTH == unit_val || DATE_UNIT_WEEK == unit_val || DATE_UNIT_QUARTER == unit_val)) {
            int32_t d_val = 0;
            if (OB_FAIL(ObTimeConverter::datetime_to_date(res_dt_val, NULL, d_val))) {
              LOG_WARN("failed to cast datetime  to date ", K(res_dt_val), K(ret));
            } else {
              ObTime ob_time;
              const int64_t date_buf_len = DATETIME_MAX_LENGTH + 1;
              char* buf = expr.get_str_res_mem(ctx, date_buf_len);
              int64_t pos = 0;
              if (OB_ISNULL(buf)) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                LOG_WARN("allocate memory failed", K(ret));
              } else if (OB_FAIL(ObTimeConverter::date_to_ob_time(d_val, ob_time))) {
                LOG_WARN("failed to convert days to ob time", K(ret));
              } else if (OB_FAIL(
                             ObTimeConverter::ob_time_to_str(ob_time, DT_TYPE_DATE, 0, buf, date_buf_len, pos, true))) {
                LOG_WARN("failed to convert ob time to string", K(ret));
              } else {
                expr_datum.ptr_ = buf;
                expr_datum.pack_ = static_cast<uint32_t>(pos);
              }
            }
          } else {
            const int64_t datetime_buf_len = DATETIME_MAX_LENGTH + 1;
            char* buf = expr.get_str_res_mem(ctx, datetime_buf_len);
            int64_t pos = 0;
            ObString format;
            if (OB_ISNULL(buf)) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("allocate memory failed", K(ret));
            } else if (OB_FAIL(ObTimeConverter::datetime_to_str(
                           res_dt_val, NULL, format, -1, buf, datetime_buf_len, pos, true))) {
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

ObExprDateAdd::ObExprDateAdd(ObIAllocator& alloc)
    : ObExprDateAdjust(alloc, T_FUN_SYS_DATE_ADD, N_DATE_ADD, 3, NOT_ROW_DIMENSION)
{}

int ObExprDateAdd::calc_result3(
    ObObj& result, const ObObj& date, const ObObj& interval, const ObObj& unit, ObExprCtx& expr_ctx) const
{
  return ObExprDateAdjust::calc_result3(result, date, interval, unit, expr_ctx, true, result_type_);
}

int ObExprDateAdd::cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
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
  } else if (OB_ISNULL(rt_expr.args_[0]) || OB_ISNULL(rt_expr.args_[1]) || OB_ISNULL(rt_expr.args_[2])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child of date_add expr is null", K(ret), K(rt_expr.args_[0]), K(rt_expr.args_[1]), K(rt_expr.args_[2]));
  } else {
    rt_expr.eval_func_ = ObExprDateAdd::calc_date_add;
  }
  return ret;
}

int ObExprDateAdd::calc_date_add(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  return ObExprDateAdjust::calc_date_adjust(expr, ctx, expr_datum, true /* is_add */);
}

ObExprDateSub::ObExprDateSub(ObIAllocator& alloc)
    : ObExprDateAdjust(alloc, T_FUN_SYS_DATE_SUB, N_DATE_SUB, 3, NOT_ROW_DIMENSION)
{}
int ObExprDateSub::calc_result3(
    ObObj& result, const ObObj& date, const ObObj& interval, const ObObj& unit, ObExprCtx& expr_ctx) const
{
  return ObExprDateAdjust::calc_result3(result, date, interval, unit, expr_ctx, false, result_type_);
}

int ObExprDateSub::cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
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
  } else if (OB_ISNULL(rt_expr.args_[0]) || OB_ISNULL(rt_expr.args_[1]) || OB_ISNULL(rt_expr.args_[2])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child of date_sub expr is null", K(ret), K(rt_expr.args_[0]), K(rt_expr.args_[1]), K(rt_expr.args_[2]));
  } else {
    rt_expr.eval_func_ = ObExprDateSub::calc_date_sub;
  }
  return ret;
}

int ObExprDateSub::calc_date_sub(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  return ObExprDateAdjust::calc_date_adjust(expr, ctx, expr_datum, false /* is_add */);
}

ObExprAddMonths::ObExprAddMonths(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_ADD_MONTHS, N_ADD_MONTHS, 2, NOT_ROW_DIMENSION)
{}

int ObExprAddMonths::calc_result_type2(
    ObExprResType& type, ObExprResType& type1, ObExprResType& type2, common::ObExprTypeCtx& type_ctx) const
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

int ObExprAddMonths::calc_result2(
    common::ObObj& result, const common::ObObj& obj1, const common::ObObj& obj2, common::ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_ctx);
  int64_t ori_date_utc = 0;
  number::ObNumber months;
  int64_t months_int = 0;
  int64_t res_date_utc = 0;

  if (obj1.is_null_oracle() || obj2.is_null_oracle()) {
    result.set_null();
  } else if (OB_FAIL(obj1.get_datetime(ori_date_utc))) {
    LOG_WARN("fail to get datetime", K(ret));
  } else if (OB_FAIL(obj2.get_number(months))) {
    LOG_WARN("fail to get number", K(ret));
  } else if (OB_FAIL(months.extract_valid_int64_with_trunc(months_int))) {
    LOG_WARN("fail to do round", K(ret));
  } else if (OB_FAIL(ObTimeConverter::date_add_nmonth(ori_date_utc, months_int, res_date_utc, true))) {
    LOG_WARN("fail to date add nmonth", K(ret));
  } else {
    result.set_datetime(res_date_utc);
    result.set_scale(OB_MAX_DATE_PRECISION);
  }
  return ret;
}

int ObExprAddMonths::cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
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
  } else if (OB_ISNULL(rt_expr.args_[0]) || OB_ISNULL(rt_expr.args_[1])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child of add_months expr is null", K(ret), K(rt_expr.args_[0]), K(rt_expr.args_[1]));
  } else {
    rt_expr.eval_func_ = ObExprAddMonths::calc_add_months;
  }
  return ret;
}

int ObExprAddMonths::calc_add_months(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum* param1 = NULL;
  ObDatum* param2 = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, param1))) {
    LOG_WARN("eval first param value failed");
  } else if (param1->is_null()) {
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
    } else if (OB_FAIL(ObTimeConverter::date_add_nmonth(ori_date_utc, months_int, res_date_utc, true))) {
      LOG_WARN("fail to date add nmonth", K(ret));
    } else {
      expr_datum.set_datetime(res_date_utc);
    }
  }
  return ret;
}

ObExprLastDay::ObExprLastDay(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_LAST_DAY, N_LAST_DAY, 1, NOT_ROW_DIMENSION)
{}

int ObExprLastDay::calc_result_type1(ObExprResType& type, ObExprResType& type1, common::ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  type.set_datetime();
  type.set_scale(OB_MAX_DATE_PRECISION);
  type1.set_calc_type(ObDateTimeType);
  type1.set_calc_scale(OB_MAX_DATETIME_PRECISION);
  return ret;
}

int ObExprLastDay::calc_result1(common::ObObj& result, const common::ObObj& obj, common::ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_ctx);
  int64_t ori_date_utc = 0;
  int64_t res_date_utc = 0;
  if (obj.is_null_oracle()) {
    result.set_null();
  } else if (OB_FAIL(obj.get_datetime(ori_date_utc))) {
    LOG_WARN("fail to get datetime", K(ret));
  } else if (OB_FAIL(ObTimeConverter::calc_last_date_of_the_month(ori_date_utc, res_date_utc))) {
    LOG_WARN("fail to calc last mday", K(ret));
  } else {
    result.set_datetime(res_date_utc);
    result.set_scale(OB_MAX_DATE_PRECISION);
  }
  return ret;
}

int ObExprLastDay::cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
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

int ObExprLastDay::calc_last_day(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum* param1 = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, param1))) {
    LOG_WARN("eval first param value failed");
  } else if (param1->is_null()) {
    expr_datum.set_null();
  } else {
    int64_t ori_date_utc = param1->get_datetime();
    int64_t res_date_utc = 0;
    if (OB_FAIL(ObTimeConverter::calc_last_date_of_the_month(ori_date_utc, res_date_utc))) {
      LOG_WARN("fail to calc last mday", K(ret));
    } else {
      expr_datum.set_datetime(res_date_utc);
    }
  }

  return ret;
}

ObExprNextDay::ObExprNextDay(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_NEXT_DAY, N_NEXT_DAY, 2, NOT_ROW_DIMENSION)
{}

int ObExprNextDay::calc_result_type2(
    ObExprResType& type, ObExprResType& type1, ObExprResType& type2, common::ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  type.set_datetime();
  type.set_scale(OB_MAX_DATE_PRECISION);
  type1.set_calc_type(ObDateTimeType);
  type1.set_calc_scale(OB_MAX_DATETIME_PRECISION);
  type2.set_calc_type(ObVarcharType);
  return ret;
}

int ObExprNextDay::calc_result2(
    common::ObObj& result, const common::ObObj& obj1, const common::ObObj& obj2, common::ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_ctx);
  int64_t ori_date_utc = 0;
  ObString wday_name;
  int64_t res_date_utc = 0;
  if (obj1.is_null_oracle() || obj2.is_null_oracle()) {
    result.set_null();
  } else if (OB_FAIL(obj1.get_datetime(ori_date_utc))) {
    LOG_WARN("fail to get datetime", K(ret));
  } else if (OB_FAIL(obj2.get_varchar(wday_name))) {
    LOG_WARN("fail to get varchar", K(ret));
  } else if (OB_FAIL(ObTimeConverter::calc_next_date_of_the_wday(ori_date_utc, wday_name.trim(), res_date_utc))) {
    LOG_WARN("fail to calc next mday", K(ret));
  } else {
    result.set_datetime(res_date_utc);
    result.set_scale(OB_MAX_DATE_PRECISION);
  }
  return ret;
}

int ObExprNextDay::cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
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
  } else if (OB_ISNULL(rt_expr.args_[0]) || OB_ISNULL(rt_expr.args_[1])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child of next_day expr is null", K(ret), K(rt_expr.args_[0]), K(rt_expr.args_[1]));
  } else {
    rt_expr.eval_func_ = ObExprNextDay::calc_next_day;
  }
  return ret;
}

int ObExprNextDay::calc_next_day(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum* param1 = NULL;
  ObDatum* param2 = NULL;
  if (OB_FAIL(expr.args_[0]->eval(ctx, param1))) {
    LOG_WARN("eval first param value failed");
  } else if (param1->is_null()) {
    expr_datum.set_null();
  } else if (OB_FAIL(expr.args_[1]->eval(ctx, param2))) {
    LOG_WARN("eval second param value failed");
  } else if (param2->is_null()) {
    expr_datum.set_null();
  } else {
    int64_t ori_date_utc = param1->get_datetime();
    ObString wday_name = param2->get_string();
    int64_t res_date_utc = 0;

    if (OB_FAIL(ObTimeConverter::calc_next_date_of_the_wday(ori_date_utc, wday_name.trim(), res_date_utc))) {
      LOG_WARN("fail to calc next mday", K(ret));
    } else {
      expr_datum.set_datetime(res_date_utc);
    }
  }
  return ret;
}
