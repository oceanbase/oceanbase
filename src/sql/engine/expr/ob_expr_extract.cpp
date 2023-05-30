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
#include "sql/engine/expr/ob_expr_extract.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/time/ob_time_utility.h"
#include "lib/timezone/ob_time_convert.h"
#include "lib/ob_name_def.h"
#include "lib/ob_date_unit_type.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_datum_cast.h"

#define STR_LEN 20

namespace oceanbase
{
using namespace common;
namespace sql
{
ObExprExtract::ObExprExtract(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_EXTRACT, N_EXTRACT, 2, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprExtract::~ObExprExtract()
{
}

int ObExprExtract::calc_result_type2(ObExprResType &type,
                                     ObExprResType &date_unit,
                                     ObExprResType &date,
                                     ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  if (lib::is_oracle_mode()) {
    if (OB_FAIL(set_result_type_oracle(type_ctx, date_unit, type))) {
      LOG_WARN("set_result_type_oracle failed", K(ret), K(type));
    }
  } else {
    if (ob_is_enumset_tc(date.get_type())) {
      date.set_calc_type(ObVarcharType);
    }
    type.set_int();
    type.set_scale(DEFAULT_SCALE_FOR_INTEGER);
    type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].precision_);
  }
  return ret;
}

int ObExprExtract::set_result_type_oracle(ObExprTypeCtx &type_ctx,
                                          const ObExprResType &date_unit, 
                                          ObExprResType &res_type) const
{
  int ret = OB_SUCCESS;
  bool is_varchar = false;
  const ObObj &date_unit_obj = date_unit.get_param();
  if (false == date_unit_obj.is_null_oracle()) {
    ObDateUnitType date_unit_val = static_cast<ObDateUnitType>(date_unit_obj.get_int());
    if (DATE_UNIT_TIMEZONE_ABBR == date_unit_val || DATE_UNIT_TIMEZONE_REGION == date_unit_val) {
      is_varchar = true;
    }
  }

  if (is_varchar) {
    ObCollationType collation_connection = type_ctx.get_coll_type();
    if (CS_TYPE_INVALID == collation_connection) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid connection cs_type", K(ret), K(collation_connection));
    } else {
      res_type.set_varchar();
      res_type.set_collation_type(collation_connection);
      res_type.set_collation_level(CS_LEVEL_IMPLICIT);
    }
  } else {
    res_type.set_number();
    res_type.set_precision(PRECISION_UNKNOWN_YET);
    res_type.set_scale(NUMBER_SCALE_UNKNOWN_YET);
  }
  return ret;
}
template <typename T, bool with_date>
inline int obj_to_time(const T &date, ObObjType type,
                              const ObTimeZoneInfo *tz_info, ObTime &ob_time,
                              const int64_t cur_ts_value,
                              const ObDateSqlMode date_sql_mode,
                              bool has_lob_header);
template <>
inline int obj_to_time<ObObj, true>(const ObObj &date, ObObjType type,
                                          const ObTimeZoneInfo *tz_info, ObTime& ob_time,
                                          const int64_t cur_ts_value,
                                          const ObDateSqlMode date_sql_mode,
                                          bool has_lob_header)
{
  UNUSED(type);
  UNUSED(has_lob_header);
  return  ob_obj_to_ob_time_with_date(date, tz_info, ob_time, cur_ts_value, false, date_sql_mode);
}
template <>
inline int obj_to_time<ObObj, false>(const ObObj &date, ObObjType type,
                                            const ObTimeZoneInfo *tz_info, ObTime &ob_time,
                                            const int64_t cur_ts_value,
                                            const ObDateSqlMode date_sql_mode,
                                            bool has_lob_header)
{
  UNUSED(type);
  UNUSED(cur_ts_value);
  UNUSED(date_sql_mode);
  UNUSED(has_lob_header);
  return  ob_obj_to_ob_time_without_date(date, tz_info, ob_time);
}
template <>
inline int obj_to_time<ObDatum, true>(const ObDatum &date, ObObjType type,
                                              const ObTimeZoneInfo *tz_info, ObTime &ob_time,
                                              const int64_t cur_ts_value,
                                              const ObDateSqlMode date_sql_mode,
                                              bool has_lob_header)
{
  return  ob_datum_to_ob_time_with_date(
          date, type, tz_info, ob_time, cur_ts_value, false, date_sql_mode, has_lob_header);
}
template <>
inline int obj_to_time<ObDatum, false>(const ObDatum &date, ObObjType type,
                                              const ObTimeZoneInfo *tz_info, ObTime &ob_time,
                                              const int64_t cur_ts_value,
                                              const ObDateSqlMode date_sql_mode,
                                              bool has_lob_header)
{
  UNUSED(cur_ts_value);
  UNUSED(date_sql_mode);
  return  ob_datum_to_ob_time_without_date(date, type, tz_info, ob_time, has_lob_header);
}
template <typename T>
int ObExprExtract::calc(T &result,
                        const int64_t date_unit,
                        const T &date,
                        ObObjType date_type,
                        const ObCastMode cast_mode,
                        const ObTimeZoneInfo *tz_info,
                        const int64_t cur_ts_value,
                        const ObDateSqlMode date_sql_mode,
                        bool has_lob_header)
{
  int ret = OB_SUCCESS;
  class ObTime ob_time;
  memset(&ob_time, 0, sizeof(ob_time));
  if (date.is_null()) {
    result.set_null();
  } else {
    int warning = OB_SUCCESS;
    int &cast_ret = CM_IS_ERROR_ON_FAIL(cast_mode) ? ret : warning;

    ObDateUnitType unit_val = static_cast<ObDateUnitType>(date_unit);
    switch (unit_val){
      case DATE_UNIT_DAY:
      case DATE_UNIT_WEEK:
      case DATE_UNIT_MONTH:
      case DATE_UNIT_QUARTER:
      case DATE_UNIT_YEAR:
      case DATE_UNIT_DAY_MICROSECOND:
      case DATE_UNIT_DAY_SECOND:
      case DATE_UNIT_DAY_MINUTE:
      case DATE_UNIT_DAY_HOUR:
      case DATE_UNIT_YEAR_MONTH:
        cast_ret =  obj_to_time<T, true>(
                    date, date_type, tz_info, ob_time, cur_ts_value, date_sql_mode, has_lob_header);
        break;
      default:
        cast_ret =  obj_to_time<T, false>(date, date_type, tz_info, ob_time, cur_ts_value, 0, has_lob_header);
     }

     if (OB_SUCC(ret)) {
       if (OB_LIKELY(OB_SUCCESS == warning)){
         ObDateUnitType unit_type = static_cast<ObDateUnitType>(date_unit);
         int64_t value = ObTimeConverter::ob_time_to_int_extract(ob_time, unit_type);
         result.set_int(value);
       } else {
         result.set_null();
       }
     }
  }
  return ret;
}

template <typename T>
void ob_set_string(T &result, ObString str);
template <>
void ob_set_string<ObDatum>(ObDatum &result, ObString str)
{
  result.set_string(str);
}
template <>
void ob_set_string<ObObj>(ObObj &result, ObString str)
{
  result.set_varchar(str);
}

template <typename T>
ObOTimestampData get_otimestamp_value(const T &param, ObObjType type);
template <>
ObOTimestampData get_otimestamp_value<ObObj>(const ObObj &param, ObObjType type)
{
  UNUSED(type);
  return param.get_otimestamp_value();
}
template <>
ObOTimestampData get_otimestamp_value<ObDatum>(const ObDatum &param, ObObjType type)
{
  return ObTimestampTZType == type ? param.get_otimestamp_tz()
                                   : param.get_otimestamp_tiny();
}

template <typename T>
int ObExprExtract::calc_oracle(T &result,
                               const int64_t date_unit,
                               const T &date,
                               ObObjType type,
                               const ObSQLSessionInfo *session,
                               ObIAllocator *calc_buf)
{
  int ret = OB_SUCCESS;
  if (date.is_null()) {
    result.set_null();
  } else {
    ObDateUnitType unit_val = static_cast<ObDateUnitType>(date_unit);
    bool extract_timezone_field =
        (DATE_UNIT_TIMEZONE_ABBR == unit_val)
        || (DATE_UNIT_TIMEZONE_HOUR == unit_val)
        || (DATE_UNIT_TIMEZONE_MINUTE == unit_val)
        || (DATE_UNIT_TIMEZONE_REGION == unit_val);
    bool extract_time_field =
        (DATE_UNIT_HOUR == unit_val)
        || (DATE_UNIT_MINUTE == unit_val)
        || (DATE_UNIT_SECOND == unit_val);
    bool extract_day_field = (DATE_UNIT_DAY == unit_val);
    bool extract_year_month_field =
        (DATE_UNIT_YEAR == unit_val)
        || (DATE_UNIT_MONTH == unit_val);
    bool valid_expr = false;

    //1. check the extraction field is valid
    switch (type) {
    //see oracle doc EXTRACT (datetime) : https://docs.oracle.com/cd/B28359_01/server.111/b28286/functions052.htm#SQLRF00639
    case ObDateTimeType:
      valid_expr = !extract_timezone_field && !extract_time_field;
      break;
    case ObTimestampNanoType:
      valid_expr = !extract_timezone_field;
      break;
    case ObTimestampLTZType:
    case ObTimestampTZType:
      valid_expr = true;
      break;
    case ObIntervalYMType:
      valid_expr = !extract_timezone_field && !extract_day_field && !extract_time_field;
      break;
    case ObIntervalDSType:
      valid_expr = !extract_timezone_field && !extract_year_month_field;
      break;
    default:
      valid_expr = false;
    }

    if (OB_UNLIKELY(!valid_expr)) {
      ret = OB_ERR_EXTRACT_FIELD_INVALID; //ORA-30076: invalid extract field for extract source
      LOG_WARN("invalid extract field for extract source", K(ret),
               "requested field", ob_date_unit_type_str(unit_val),
               "expr type", type);
    }

    ObTime ob_time;
    ObIntervalParts interval_part;

    //2. convert to time struct
    if (OB_SUCC(ret)) {
      switch (ob_obj_type_class(type)) {
      case ObDateTimeTC:
        if (OB_FAIL(ObTimeConverter::datetime_to_ob_time(date.get_datetime(),
                                                         NULL,
                                                         ob_time))) {
          LOG_WARN("fail to convert date to ob time", K(ret), K(date));
        }
        break;
      case ObOTimestampTC:
        if (OB_FAIL(ObTimeConverter::otimestamp_to_ob_time(type,
                                                           get_otimestamp_value(date, type),
                                                           get_timezone_info(session),
                                                           ob_time,
                                                           ObTimestampLTZType != type))) {
          LOG_WARN("fail to convert otimestamp to ob time", K(ret), K(date));
        }
        break;
      case ObIntervalTC:
        if (ObIntervalYMType == type) {
          ObTimeConverter::interval_ym_to_display_part(date.get_interval_ym(), interval_part);
        } else {
          ObTimeConverter::interval_ds_to_display_part(date.get_interval_ds(), interval_part);
        }
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected type class", K(ret), K(date));
      }
    }

    //3. get result interger or string
    int64_t result_value = INT64_MIN;
    int64_t fsecond = INT64_MIN;
    ObString result_str;

    if (OB_SUCC(ret)) {
      bool is_interval_tc = (ObIntervalTC == ob_obj_type_class(type));

      switch (unit_val) {
      case DATE_UNIT_YEAR:
        result_value = is_interval_tc ? interval_part.get_part_value(ObIntervalParts::YEAR_PART) : ob_time.parts_[DT_YEAR];
        break;
      case DATE_UNIT_MONTH:
        result_value = is_interval_tc ? interval_part.get_part_value(ObIntervalParts::MONTH_PART) : ob_time.parts_[DT_MON];
        break;
      case DATE_UNIT_DAY:
        result_value = is_interval_tc ? interval_part.get_part_value(ObIntervalParts::DAY_PART) : ob_time.parts_[DT_MDAY];
        break;
      case DATE_UNIT_HOUR:
        result_value = is_interval_tc ? interval_part.get_part_value(ObIntervalParts::HOUR_PART) : ob_time.parts_[DT_HOUR];
        break;
      case DATE_UNIT_MINUTE:
        result_value = is_interval_tc ? interval_part.get_part_value(ObIntervalParts::MINUTE_PART) : ob_time.parts_[DT_MIN];
        break;
      case DATE_UNIT_SECOND:
        result_value = is_interval_tc ? interval_part.get_part_value(ObIntervalParts::SECOND_PART) : ob_time.parts_[DT_SEC];
        fsecond = is_interval_tc ? interval_part.get_part_value(ObIntervalParts::FRECTIONAL_SECOND_PART) : ob_time.parts_[DT_USEC];
        break;
      case DATE_UNIT_TIMEZONE_HOUR:
        result_value = ob_time.parts_[DT_OFFSET_MIN] / 60;
        break;
      case DATE_UNIT_TIMEZONE_MINUTE:
        result_value = ob_time.parts_[DT_OFFSET_MIN] % 60;
        break;
      case DATE_UNIT_TIMEZONE_REGION:
        result_str = ob_time.get_tz_name_str();
        if (result_str.empty()) {
          result_str = ObString("UNKNOWN");
        }
        break;
      case DATE_UNIT_TIMEZONE_ABBR:
        result_str = ob_time.get_tzd_abbr_str();
        if (result_str.empty()) {
          result_str = ObString("UNK");
        }
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid field for extract", K(ret), K(unit_val));
      }
    }

    if (OB_SUCC(ret)) {
      if (result_value != INT64_MIN) {
        //interger result
        number::ObNumber res_number;
        number::ObNumber res_with_fsecond_number;
        number::ObNumber fs_value;
        number::ObNumber power10;
        number::ObNumber fs_number;
        ObIAllocator *allocator = NULL;
        if (OB_ISNULL(allocator = calc_buf)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("calc buf is null", K(ret));
        } else if (OB_FAIL(res_number.from(result_value, *allocator))) {
          LOG_WARN("fail to convert interger to ob number", K(ret));
        } else if (fsecond != INT64_MIN) {
          if (OB_FAIL(fs_value.from(fsecond, *allocator))) {
            LOG_WARN("fail to convert to ob number", K(ret));
          } else if (OB_FAIL(power10.from(static_cast<int64_t>(ObIntervalDSValue::MAX_FS_VALUE), *allocator))) {
            LOG_WARN("failed to round number", K(ret));
          } else if (OB_FAIL(fs_value.div(power10, fs_number, *allocator))) {
            LOG_WARN("fail to div fs", K(ret));
          } else if (OB_FAIL(res_number.add(fs_number, res_with_fsecond_number, *allocator))) {
            LOG_WARN("fail to add fsecond", K(ret));
          } else {
            result.set_number(res_with_fsecond_number);
          }
        } else {
          result.set_number(res_number);
        }
      } else {
        ob_set_string(result, result_str);
      }
    }
  }

  return ret;
}

int ObExprExtract::cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  if (rt_expr.arg_cnt_ != 2) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("extract expr should have 2 params", K(ret), K(rt_expr.arg_cnt_));
  } else if (OB_ISNULL(rt_expr.args_) || OB_ISNULL(rt_expr.args_[0])
            || OB_ISNULL(rt_expr.args_[1])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children of extract expr is null", K(ret), K(rt_expr.args_));
  } else {
    if (is_oracle_mode()) {
      rt_expr.eval_func_ = ObExprExtract::calc_extract_oracle;
    } else {
      rt_expr.eval_func_ = ObExprExtract::calc_extract_mysql;
    }
    // For static engine batch
    // Actually, the first param is always constant, can't be batch result
    if (!rt_expr.args_[0]->is_batch_result() && rt_expr.args_[1]->is_batch_result()) {
      if (is_oracle_mode()) {
        rt_expr.eval_batch_func_ = ObExprExtract::calc_extract_oracle_batch;
      } else {
        rt_expr.eval_batch_func_ = ObExprExtract::calc_extract_mysql_batch;
      }
    }
  }
  return ret;
}

int ObExprExtract::calc_extract_oracle(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *param_datum1 = NULL;
  ObDatum *param_datum2 = NULL;
  const ObSQLSessionInfo *session = NULL;
  if (OB_ISNULL(session = ctx.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (OB_FAIL(expr.args_[0]->eval(ctx, param_datum1))) {
    LOG_WARN("eval param1 value failed");
  } else if (OB_FAIL(expr.args_[1]->eval(ctx, param_datum2))) {
    LOG_WARN("eval param2 value failed");
  } else {
    ObDatum *result = static_cast<ObDatum *>(&expr_datum);
    ObEvalCtx::TempAllocGuard alloc_guard(ctx);
    ret = calc_oracle(*result, param_datum1->get_int(), *param_datum2,
                      expr.args_[1]->datum_meta_.type_, session, &alloc_guard.get_allocator());
  }
  return ret;
}

int ObExprExtract::calc_extract_mysql(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *param_datum1 = NULL;
  ObDatum *param_datum2 = NULL;
  const ObSQLSessionInfo *session = NULL;
  if (OB_ISNULL(session = ctx.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (OB_FAIL(expr.args_[0]->eval(ctx, param_datum1))) {
    LOG_WARN("eval param1 value failed");
  } else if (OB_FAIL(expr.args_[1]->eval(ctx, param_datum2))) {
    LOG_WARN("eval param2 value failed");
  } else {
    ObDatum *result = static_cast<ObDatum *>(&expr_datum);
    uint64_t cast_mode = 0;
    ObSQLUtils::get_default_cast_mode(session->get_stmt_type(), session, cast_mode);
    ObDateSqlMode date_sql_mode;
    date_sql_mode.init(session->get_sql_mode());
    ret = ObExprExtract::calc(*result, param_datum1->get_int(), *param_datum2,
                              expr.args_[1]->datum_meta_.type_,
                              cast_mode, get_timezone_info(session),
                              get_cur_time(ctx.exec_ctx_.get_physical_plan_ctx()),
                              date_sql_mode, expr.args_[1]->obj_meta_.has_lob_header());
  }
  return ret;
}

int ObExprExtract::calc_extract_mysql_batch(
    const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const int64_t batch_size)
{
  LOG_DEBUG("eval mysql extract in batch mode", K(batch_size));
  int ret = OB_SUCCESS;
  ObDatum *results = expr.locate_batch_datums(ctx);

  if (OB_ISNULL(results)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr results frame is not init", K(ret));
  } else {
    ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
    ObDatum *date_unit_datum = NULL;
    const ObSQLSessionInfo *session = NULL;
    if (OB_ISNULL(session = ctx.exec_ctx_.get_my_session())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session is null", K(ret));
    } else if (OB_FAIL(expr.args_[0]->eval(ctx, date_unit_datum))) {
      LOG_WARN("eval date_unit_datum failed", K(ret));
    } else if (OB_FAIL(expr.args_[1]->eval_batch(ctx, skip, batch_size))) {
      LOG_WARN("failed to eval batch result args0", K(ret));
    } else {
      uint64_t cast_mode = 0;
      ObSQLUtils::get_default_cast_mode(session->get_stmt_type(), session, cast_mode);
      ObDateUnitType unit_val = static_cast<ObDateUnitType>(date_unit_datum->get_int());
      bool is_with_date = false;
      int warning = OB_SUCCESS;
      int &cast_ret = CM_IS_ERROR_ON_FAIL(cast_mode) ? ret : warning;
      ObDatum *datum_array = expr.args_[1]->locate_batch_datums(ctx);
      const ObTimeZoneInfo *tz_info = get_timezone_info(session);
      const int64_t cur_ts_value = get_cur_time(ctx.exec_ctx_.get_physical_plan_ctx());
      class ObTime ob_time;
      switch (unit_val) {
        case DATE_UNIT_DAY:
        case DATE_UNIT_WEEK:
        case DATE_UNIT_MONTH:
        case DATE_UNIT_QUARTER:
        case DATE_UNIT_YEAR:
        case DATE_UNIT_DAY_MICROSECOND:
        case DATE_UNIT_DAY_SECOND:
        case DATE_UNIT_DAY_MINUTE:
        case DATE_UNIT_DAY_HOUR:
        case DATE_UNIT_YEAR_MONTH:
          is_with_date = true;
          break;
        default:
          is_with_date = false;
      }
      for (int64_t j = 0; OB_SUCC(ret) && j < batch_size; ++j) {
        if (skip.at(j) || eval_flags.at(j)) {
          continue;
        } else if (datum_array[j].is_null()) {
          results[j].set_null();
          eval_flags.set(j);
        } else {
          ObDateSqlMode date_sql_mode;
          date_sql_mode.init(session->get_sql_mode());
          memset(&ob_time, 0, sizeof(ob_time));
          if (is_with_date) {
            cast_ret = obj_to_time<ObDatum, true>(
                datum_array[j], expr.args_[1]->datum_meta_.type_, tz_info, ob_time, cur_ts_value,
                date_sql_mode, expr.args_[1]->obj_meta_.has_lob_header());
          } else {
            cast_ret = obj_to_time<ObDatum, false>(
                datum_array[j], expr.args_[1]->datum_meta_.type_, tz_info, ob_time, cur_ts_value,
                date_sql_mode, expr.args_[1]->obj_meta_.has_lob_header());
          }
          if (OB_SUCC(ret)) {
            if (OB_LIKELY(OB_SUCCESS == warning)){
              int64_t value = ObTimeConverter::ob_time_to_int_extract(ob_time, unit_val);
              results[j].set_int(value);
            } else {
              results[j].set_null();
            }
            eval_flags.set(j);
          }
        }
      }
    }
  }

  return ret;
}

int ObExprExtract::calc_extract_oracle_batch(
    const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const int64_t batch_size)
{
  LOG_DEBUG("eval oracle extract in batch mode", K(batch_size));
  int ret = OB_SUCCESS;
  ObDatum *results = expr.locate_batch_datums(ctx);

  if (OB_ISNULL(results)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr results frame is not init", K(ret));
  } else {
    ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
    ObDatum *date_unit_datum = NULL;
    const ObSQLSessionInfo *session = NULL;
    if (OB_ISNULL(session = ctx.exec_ctx_.get_my_session())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session is null", K(ret));
    } else if (OB_FAIL(expr.args_[0]->eval(ctx, date_unit_datum))) {
      LOG_WARN("eval date_unit_datum failed", K(ret));
    } else if (OB_FAIL(expr.args_[1]->eval_batch(ctx, skip, batch_size))) {
      LOG_WARN("failed to eval batch result args0", K(ret));
    } else {
      ObDatum *datum_array = expr.args_[1]->locate_batch_datums(ctx);
      common::ObObjType type = expr.args_[1]->datum_meta_.type_;
      ObDateUnitType unit_val = static_cast<ObDateUnitType>(date_unit_datum->get_int());
      // 1. check the extraction field is valid
      bool extract_timezone_field =
          (DATE_UNIT_TIMEZONE_ABBR == unit_val)
          || (DATE_UNIT_TIMEZONE_HOUR == unit_val)
          || (DATE_UNIT_TIMEZONE_MINUTE == unit_val)
          || (DATE_UNIT_TIMEZONE_REGION == unit_val);
      bool extract_time_field =
          (DATE_UNIT_HOUR == unit_val)
          || (DATE_UNIT_MINUTE == unit_val)
          || (DATE_UNIT_SECOND == unit_val);
      bool extract_day_field = (DATE_UNIT_DAY == unit_val);
      bool extract_year_month_field =
          (DATE_UNIT_YEAR == unit_val)
          || (DATE_UNIT_MONTH == unit_val);
      bool valid_expr = false;
      switch (type) {
      // see oracle doc EXTRACT (datetime) :
      // https://docs.oracle.com/cd/B28359_01/server.111/b28286/functions052.htm#SQLRF00639
      case ObDateTimeType:
        valid_expr = !extract_timezone_field && !extract_time_field;
        break;
      case ObTimestampNanoType:
        valid_expr = !extract_timezone_field;
        break;
      case ObTimestampLTZType:
      case ObTimestampTZType:
        valid_expr = true;
        break;
      case ObIntervalYMType:
        valid_expr = !extract_timezone_field && !extract_day_field && !extract_time_field;
        break;
      case ObIntervalDSType:
        valid_expr = !extract_timezone_field && !extract_year_month_field;
        break;
      default:
        valid_expr = false;
      }
      if (OB_UNLIKELY(!valid_expr)) {
        ret = OB_ERR_EXTRACT_FIELD_INVALID; // ORA-30076: invalid extract field for extract source
        LOG_WARN("invalid extract field for extract source", K(ret),
                 "requested field", ob_date_unit_type_str(unit_val),
                 "expr type", type);
      }
      for (int64_t j = 0; OB_SUCC(ret) && j < batch_size; ++j) {
        if (skip.at(j) || eval_flags.at(j)) {
          continue;
        } else if (datum_array[j].is_null()) {
          results[j].set_null();
          eval_flags.set(j);
        } else {
          // 2. convert to time struct
          ObTime ob_time;
          ObIntervalParts interval_part;
          if (OB_SUCC(ret)) {
            switch (ob_obj_type_class(type)) {
            case ObDateTimeTC:
              if (OB_FAIL(ObTimeConverter::datetime_to_ob_time(datum_array[j].get_datetime(),
                                                               NULL,
                                                               ob_time))) {
                LOG_WARN("fail to convert date to ob time", K(ret), K(datum_array[j]));
              }
              break;
            case ObOTimestampTC:
              if (OB_FAIL(ObTimeConverter::otimestamp_to_ob_time(type,
                                                                 get_otimestamp_value(datum_array[j], expr.args_[1]->datum_meta_.type_),
                                                                 get_timezone_info(session),
                                                                 ob_time,
                                                                 ObTimestampLTZType != expr.args_[1]->datum_meta_.type_))) {
                LOG_WARN("fail to convert otimestamp to ob time", K(ret), K(datum_array[j]));
              }
              break;
            case ObIntervalTC:
              if (ObIntervalYMType == type) {
                ObTimeConverter::interval_ym_to_display_part(datum_array[j].get_interval_ym(), interval_part);
              } else {
                ObTimeConverter::interval_ds_to_display_part(datum_array[j].get_interval_ds(), interval_part);
              }
              break;
            default:
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected type class", K(ret), K(datum_array[j]));
            }
          }
          // 3. get result interger or string
          int64_t result_value = INT64_MIN;
          int64_t fsecond = INT64_MIN;
          ObString result_str;
          if (OB_SUCC(ret)) {
            bool is_interval_tc = (ObIntervalTC == ob_obj_type_class(expr.args_[1]->datum_meta_.type_));
            switch (date_unit_datum->get_int()) {
            case DATE_UNIT_YEAR:
              result_value = is_interval_tc ? interval_part.get_part_value(ObIntervalParts::YEAR_PART) : ob_time.parts_[DT_YEAR];
              break;
            case DATE_UNIT_MONTH:
              result_value = is_interval_tc ? interval_part.get_part_value(ObIntervalParts::MONTH_PART) : ob_time.parts_[DT_MON];
              break;
            case DATE_UNIT_DAY:
              result_value = is_interval_tc ? interval_part.get_part_value(ObIntervalParts::DAY_PART) : ob_time.parts_[DT_MDAY];
              break;
            case DATE_UNIT_HOUR:
              result_value = is_interval_tc ? interval_part.get_part_value(ObIntervalParts::HOUR_PART) : ob_time.parts_[DT_HOUR];
              break;
            case DATE_UNIT_MINUTE:
              result_value = is_interval_tc ? interval_part.get_part_value(ObIntervalParts::MINUTE_PART) : ob_time.parts_[DT_MIN];
              break;
            case DATE_UNIT_SECOND:
              result_value = is_interval_tc ? interval_part.get_part_value(ObIntervalParts::SECOND_PART) : ob_time.parts_[DT_SEC];
              fsecond = is_interval_tc ? interval_part.get_part_value(ObIntervalParts::FRECTIONAL_SECOND_PART) : ob_time.parts_[DT_USEC];
              break;
            case DATE_UNIT_TIMEZONE_HOUR:
              result_value = ob_time.parts_[DT_OFFSET_MIN] / 60;
              break;
            case DATE_UNIT_TIMEZONE_MINUTE:
              result_value = ob_time.parts_[DT_OFFSET_MIN] % 60;
              break;
            case DATE_UNIT_TIMEZONE_REGION:
              result_str = ob_time.get_tz_name_str();
              if (result_str.empty()) {
                result_str = ObString("UNKNOWN");
              }
              break;
            case DATE_UNIT_TIMEZONE_ABBR:
              result_str = ob_time.get_tzd_abbr_str();
              if (result_str.empty()) {
                result_str = ObString("UNK");
              }
              break;
            default:
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("invalid field for extract", K(ret), K(unit_val));
            }
          }
          if (OB_SUCC(ret)) {
            if (result_value != INT64_MIN) {
              // interger result
              number::ObNumber res_number;
              number::ObNumber res_with_fsecond_number;
              number::ObNumber fs_value;
              number::ObNumber power10;
              number::ObNumber fs_number;
              ObIAllocator *allocator = NULL;
              ObEvalCtx::TempAllocGuard alloc_guard(ctx);
              if (OB_ISNULL(allocator = &alloc_guard.get_allocator())) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("calc buf is null", K(ret));
              } else if (OB_FAIL(res_number.from(result_value, *allocator))) {
                LOG_WARN("fail to convert interger to ob number", K(ret));
              } else if (fsecond != INT64_MIN) {
                if (OB_FAIL(fs_value.from(fsecond, *allocator))) {
                  LOG_WARN("fail to convert to ob number", K(ret));
                } else if (OB_FAIL(power10.from(static_cast<int64_t>(ObIntervalDSValue::MAX_FS_VALUE), *allocator))) {
                  LOG_WARN("failed to round number", K(ret));
                } else if (OB_FAIL(fs_value.div(power10, fs_number, *allocator))) {
                  LOG_WARN("fail to div fs", K(ret));
                } else if (OB_FAIL(res_number.add(fs_number, res_with_fsecond_number, *allocator))) {
                  LOG_WARN("fail to add fsecond", K(ret));
                } else {
                  results[j].set_number(res_with_fsecond_number);
                }
              } else {
                results[j].set_number(res_number);
              }
            } else {
              ob_set_string(results[j], result_str);
            }
            eval_flags.set(j);
          }
        }
      }
    }
  }

  return ret;
}

}
}
