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
    if (!ob_is_null(date.get_type()) &&
        !ob_is_temporal_type(date.get_type()) &&
        !ob_is_interval_tc(date.get_type()) &&
        !ob_is_otimestamp_type(date.get_type())) {
      ret = OB_ERR_EXTRACT_FIELD_INVALID; //ORA-30076: invalid extract field for extract source
      LOG_WARN("invalid extract field for extract source", K(ret), "expr type", date.get_type());
    } else if (OB_FAIL(set_result_type_oracle(type_ctx, date_unit, type))) {
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

int check_extract_field_valid_oracle(const ObExpr &expr, ObDateUnitType &unit_val)
{
  int ret = OB_SUCCESS;
  common::ObObjType type = expr.args_[1]->datum_meta_.type_;
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
    case ObNullType:
      valid_expr = true;
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
  return ret;
}

template <bool with_date>
inline int obj_to_time(const ObDatum &date, ObObjType type, const ObScale scale,
                       const ObTimeZoneInfo *tz_info, ObTime &ob_time, const int64_t cur_ts_value,
                       const ObDateSqlMode date_sql_mode, bool has_lob_header)
{
  if (with_date) {
    return ob_datum_to_ob_time_with_date(
          date, type, scale, tz_info, ob_time, cur_ts_value, date_sql_mode, has_lob_header);
  } else {
    return ob_datum_to_ob_time_without_date(date, type, scale, tz_info, ob_time, has_lob_header);
  }
}

int ObExprExtract::calc(
      ObObjType date_type,
      const ObDatum &date,
      const ObDateUnitType extract_field,
      const ObScale scale,
      const ObCastMode cast_mode,
      const ObTimeZoneInfo *tz_info,
      const int64_t cur_ts_value,
      const ObDateSqlMode date_sql_mode,
      bool has_lob_header,
      bool &is_null,
      int64_t &res)
{
  int ret = OB_SUCCESS;
  if (date.is_null()) {
    is_null = true;
  } else {
    ObTime ob_time;
    memset(&ob_time, 0, sizeof(ob_time));
    int warning = OB_SUCCESS;
    int &cast_ret = CM_IS_ERROR_ON_FAIL(cast_mode) ? ret : warning;
    switch (extract_field){
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
        cast_ret =  obj_to_time<true>(
                    date, date_type, scale, tz_info, ob_time, cur_ts_value, date_sql_mode, has_lob_header);
        break;
      default:
        cast_ret = obj_to_time<false>(date, date_type, scale, tz_info, ob_time, cur_ts_value, 0,
                                         has_lob_header);
     }

     if (OB_SUCC(ret)) {
       if (OB_LIKELY(OB_SUCCESS == warning)) {
         res = ObTimeConverter::ob_time_to_int_extract(ob_time, extract_field);
       } else {
         is_null = true;
       }
     }
  }
  return ret;
}

ObOTimestampData get_otimestamp_value(const ObDatum &param, ObObjType type)
{
  return ObTimestampTZType == type ? param.get_otimestamp_tz()
                                   : param.get_otimestamp_tiny();
}

int ObExprExtract::calc_oracle(const ObSQLSessionInfo *session,
                               ObEvalCtx &ctx,
                               const ObObjType date_type, 
                               const ObDatum &date,
                               const ObDateUnitType &extract_field,
                               ObIAllocator *allocator,
                               bool &is_null,
                               bool &is_number_res,
                               number::ObNumber &num_res,
                               ObString &str_res)
{
  int ret = OB_SUCCESS;
  ObTime ob_time;
  ObIntervalParts interval_part;
  is_number_res = false;
  if (date.is_null()) {
    is_null = true;
  } else {
    switch (ob_obj_type_class(date_type)) {
    case ObDateTimeTC:
      if (OB_FAIL(ObTimeConverter::datetime_to_ob_time(date.get_datetime(),
                                                        NULL,
                                                        ob_time))) {
        LOG_WARN("fail to convert date to ob time", K(ret), K(date));
      }
      break;
    case ObOTimestampTC:
      if (OB_FAIL(ObTimeConverter::otimestamp_to_ob_time(date_type,
                                                         get_otimestamp_value(date, date_type),
                                                         get_timezone_info(session),
                                                         ob_time,
                                                         ObTimestampLTZType != date_type))) {
        LOG_WARN("fail to convert otimestamp to ob time", K(ret), K(date));
      }
      break;
    case ObIntervalTC:
      if (ObIntervalYMType == date_type) {
        ObTimeConverter::interval_ym_to_display_part(date.get_interval_ym(), interval_part);
      } else {
        ObTimeConverter::interval_ds_to_display_part(date.get_interval_ds(), interval_part);
      }
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected type class", K(ret), K(date));
    }
    int64_t result_value = INT64_MIN;
    int64_t fsecond = INT64_MIN;

    if (OB_SUCC(ret)) {
      bool is_interval_tc = (ObIntervalTC == ob_obj_type_class(date_type));
      switch (extract_field) {

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
        str_res = ob_time.get_tz_name_str();
        if (str_res.empty()) {
          str_res = ObString("UNKNOWN");
        }
        break;
      case DATE_UNIT_TIMEZONE_ABBR:
        str_res = ob_time.get_tzd_abbr_str();
        if (str_res.empty()) {
          str_res = ObString("UNK");
        }
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid field for extract", K(ret), K(extract_field));
      }
    }

    if (OB_SUCC(ret)) {
      if (result_value != INT64_MIN) {
        //interger result
        number::ObNumber res_with_fsecond_number;
        number::ObNumber fs_value;
        number::ObNumber power10;
        number::ObNumber fs_number;
        if (OB_FAIL(num_res.from(result_value, *allocator))) {
          LOG_WARN("fail to convert interger to ob number", K(ret));
        } else if (fsecond != INT64_MIN) {
          if (OB_FAIL(fs_value.from(fsecond, *allocator))) {
            LOG_WARN("fail to convert to ob number", K(ret));
          } else if (OB_FAIL(power10.from(static_cast<int64_t>(ObIntervalDSValue::MAX_FS_VALUE), *allocator))) {
            LOG_WARN("failed to round number", K(ret));
          } else if (OB_FAIL(fs_value.div(power10, fs_number, *allocator))) {
            LOG_WARN("fail to div fs", K(ret));
          } else if (OB_FAIL(num_res.add(fs_number, res_with_fsecond_number, *allocator))) {
            LOG_WARN("fail to add fsecond", K(ret));
          } else {
            num_res = res_with_fsecond_number;
          }
        }
        is_number_res = true;
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
        // vectorizetion 2.0
        rt_expr.eval_vector_func_ = ObExprExtract::calc_extract_oracle_vector;
      } else {
        rt_expr.eval_batch_func_ = ObExprExtract::calc_extract_mysql_batch;
        // vectorizetion 2.0
        rt_expr.eval_vector_func_ = ObExprExtract::calc_extract_mysql_vector;
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
    ObDateUnitType extract_field = static_cast<ObDateUnitType>(param_datum1->get_int());
    // check the extraction field is valid
    if (OB_FAIL(check_extract_field_valid_oracle(expr, extract_field))) {
      ret = OB_ERR_EXTRACT_FIELD_INVALID;
      LOG_WARN("invalid extract field for extract source");
    } else {
      bool is_null = false;
      bool is_number_res = false;
      number::ObNumber num_res;
      ObString str_res;
      ObObjType date_type = expr.args_[1]->datum_meta_.type_;
      ObIAllocator *allocator = NULL;
      ObEvalCtx::TempAllocGuard alloc_guard(ctx);
      if (OB_ISNULL(allocator = &alloc_guard.get_allocator())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("calc buf is null", K(ret));
      } else if (OB_FAIL(ObExprExtract::calc_oracle(session, ctx, date_type, *param_datum2,
                                             extract_field, allocator, is_null, is_number_res,
                                             num_res, str_res))) {
        LOG_WARN("failed to calculate extract expression", K(ret));
      } else if (is_null) {
        expr_datum.set_null();
      } else if (is_number_res) {
        expr_datum.set_number(num_res);
      } else {
        expr_datum.set_string(str_res);
      }
    }
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
    ObDateUnitType extract_field = static_cast<ObDateUnitType>(param_datum1->get_int());
    ObObjType date_type = expr.args_[1]->datum_meta_.type_;
    const ObTimeZoneInfo *tz_info = get_timezone_info(session);
    const int64_t cur_ts_value = get_cur_time(ctx.exec_ctx_.get_physical_plan_ctx());
    const ObScale scale = expr.args_[1]->datum_meta_.scale_;
    bool has_lob_header = expr.args_[1]->obj_meta_.has_lob_header();
    uint64_t cast_mode = 0;
    ObSQLUtils::get_default_cast_mode(session->get_stmt_type(), session, cast_mode);
    ObDateSqlMode date_sql_mode;
    date_sql_mode.init(session->get_sql_mode());
    bool is_null = false;
    int64_t value = 0;
    if (OB_FAIL(ObExprExtract::calc(date_type, *param_datum2,
                                    extract_field, 
                                    scale, 
                                    cast_mode, tz_info,
                                    cur_ts_value, 
                                    date_sql_mode, has_lob_header, is_null, value))) {
      LOG_WARN("failed to calculate extract expression", K(ret));
    } else if (is_null) {
      expr_datum.set_null();
    } else {
      expr_datum.set_int(value);
    }
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
      ObObjType date_type = expr.args_[1]->datum_meta_.type_;
      ObDatum *datum_array = expr.args_[1]->locate_batch_datums(ctx);
      const ObTimeZoneInfo *tz_info = get_timezone_info(session);
      const int64_t cur_ts_value = get_cur_time(ctx.exec_ctx_.get_physical_plan_ctx());
      const ObScale scale = expr.args_[1]->datum_meta_.scale_;
      ObDateSqlMode date_sql_mode;
      date_sql_mode.init(session->get_sql_mode());
      bool has_lob_header = expr.args_[1]->obj_meta_.has_lob_header();
      ObDateUnitType extract_field = static_cast<ObDateUnitType>(date_unit_datum->get_int());
      for (int64_t j = 0; OB_SUCC(ret) && j < batch_size; ++j) {
        if (skip.at(j) || eval_flags.at(j)) {
          continue;
        } else if (datum_array[j].is_null()) {
          results[j].set_null();
          eval_flags.set(j);
        } else {
          bool is_null = false;
          int64_t value = 0;
          if (OB_FAIL(ObExprExtract::calc(
                  date_type, datum_array[j], extract_field, scale, cast_mode,
                  tz_info, cur_ts_value, date_sql_mode, has_lob_header, is_null, value))) {
            LOG_WARN("failed to calculate extract expression", K(ret));
          } else {
            if (is_null) {
              results[j].set_null();
            } else {
              results[j].set_int(value);
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
      ObObjType type = expr.args_[1]->datum_meta_.type_;
      ObDateUnitType extract_field = static_cast<ObDateUnitType>(date_unit_datum->get_int());
      ObIAllocator *allocator = NULL;
      ObEvalCtx::TempAllocGuard alloc_guard(ctx);
      if (OB_ISNULL(allocator = &alloc_guard.get_allocator())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("calc buf is null", K(ret));
      } else if (OB_FAIL(check_extract_field_valid_oracle(expr, extract_field))) {
        ret = OB_ERR_EXTRACT_FIELD_INVALID;
        LOG_WARN("invalid extract field for extract source");
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && j < batch_size; ++j) {
          bool is_null = false;
          bool is_number_res = false;
          number::ObNumber num_res;
          ObString str_res;
          if (skip.at(j) || eval_flags.at(j)) {
            continue;
          } else if (datum_array[j].is_null()) {
            results[j].set_null();
            eval_flags.set(j);
          } else if (OB_FAIL(ObExprExtract::calc_oracle(session,
                      ctx,
                      type,
                      datum_array[j],
                      extract_field,
                      allocator,
                      is_null,
                      is_number_res,
                      num_res,
                      str_res))) {
              LOG_WARN("failed to calculate extract expression", K(ret));
          } else {
            if (is_null) {
              results[j].set_null();
            } else if (is_number_res) {
              results[j].set_number(num_res);
            } else {
              results[j].set_string(str_res);
            }
            eval_flags.set(j);
          }
        }
      }
    }
  }

  return ret;
}

template <typename T_ARG_VEC, typename T_RES_VEC>
int process_vector_oracle(const ObExpr &expr, const T_ARG_VEC &arg_date_vec, ObObjType date_type,
                    const EvalBound &bound, const ObBitVector &skip, ObBitVector &eval_flags,
                    const ObSQLSessionInfo *session, ObEvalCtx &ctx,
                    ObDateUnitType extract_field, T_RES_VEC &res_vec)
{
  int ret = OB_SUCCESS;
  ObIAllocator *allocator = NULL;
  ObEvalCtx::TempAllocGuard alloc_guard(ctx);
  if (OB_ISNULL(allocator = &alloc_guard.get_allocator())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("calc buf is null", K(ret));
  }
  for (int64_t i = bound.start(); OB_SUCC(ret) && i < bound.end(); ++i) {
    if (skip.at(i) || eval_flags.at(i)) {
      continue;
    } else {
      const char *arg_payload = NULL;
      bool arg_is_null = false;
      ObLength arg_len = 0;
      arg_date_vec.get_payload(i, arg_is_null, arg_payload, arg_len);
      ObDatum date(arg_payload, arg_len, arg_is_null);
      bool is_null = false;
      bool is_number_res = false;
      number::ObNumber num_res;
      ObString str_res;
      if (OB_FAIL(ObExprExtract::calc_oracle(session,
                                            ctx,
                                            date_type,
                                            date,
                                            extract_field,
                                            allocator,
                                            is_null,
                                            is_number_res,
                                            num_res,
                                            str_res))) {
        LOG_WARN("failed to calculate extract expression", K(ret));
      } else {
        if (is_null) {
          res_vec.set_null(i);
        } else if (is_number_res) {
          res_vec.set_number(i, num_res);
        } else {
          res_vec.set_string(i, str_res);
        }
        eval_flags.set(i);
      }
    }
  }
  return ret;
}

int sql::ObExprExtract::calc_extract_oracle_vector(const ObExpr &expr, ObEvalCtx &ctx,
                                                   const ObBitVector &skip, const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  const ObSQLSessionInfo *session = NULL;
  if (OB_ISNULL(session = ctx.exec_ctx_.get_my_session())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session is null", K(ret));
  } else if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))
              || OB_FAIL(expr.args_[1]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("fail to eval params", K(ret));
  } else {
    ObIVector *arg_unit_vec = expr.args_[0]->get_vector(ctx);
    ObIVector *arg_date_vec = expr.args_[1]->get_vector(ctx);
    ObDateUnitType extract_field = static_cast<ObDateUnitType>(arg_unit_vec->get_int(0));
    ObIVector *res_vec = expr.get_vector(ctx);
    ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
    if (OB_FAIL(check_extract_field_valid_oracle(expr, extract_field))) {
      ret = OB_ERR_EXTRACT_FIELD_INVALID;
      LOG_WARN("invalid extract field for extract source");
    } else {
      ObObjType date_type = expr.args_[1]->datum_meta_.type_;
      VectorFormat arg_format = arg_date_vec->get_format();
      VectorFormat res_format = res_vec->get_format();
      ret = process_vector_oracle<ObVectorBase, ObVectorBase>(
          expr, static_cast<ObVectorBase &>(*arg_date_vec), date_type, bound,
          skip, eval_flags, session, ctx, extract_field,
          static_cast<ObVectorBase &>(*res_vec));
      if (OB_FAIL(ret)) {
        LOG_WARN("failed to calc extract expression in Oracle mode", K(ret));
      }
    }
  }
  return ret;
}

template <typename T_ARG_VEC, typename T_RES_VEC>
int process_vector_mysql(const ObExpr &expr, const T_ARG_VEC &arg_date_vec, ObObjType date_type,
                    const EvalBound &bound, const ObBitVector &skip, ObBitVector &eval_flags,
                    const ObSQLSessionInfo *session, ObEvalCtx &ctx,
                    ObDateUnitType extract_field, T_RES_VEC &res_vec)
{
  int ret = OB_SUCCESS;
  uint64_t cast_mode = 0;
  ObSQLUtils::get_default_cast_mode(session->get_stmt_type(), session, cast_mode);
  bool is_with_date = false;
  int warning = OB_SUCCESS;
  int &cast_ret = CM_IS_ERROR_ON_FAIL(cast_mode) ? ret : warning;
  const ObTimeZoneInfo *tz_info = get_timezone_info(session);
  const int64_t cur_ts_value = get_cur_time(ctx.exec_ctx_.get_physical_plan_ctx());
  const ObScale scale = expr.args_[1]->datum_meta_.scale_;
  ObDateSqlMode date_sql_mode;
  date_sql_mode.init(session->get_sql_mode());
  bool has_lob_header = expr.args_[1]->obj_meta_.has_lob_header();
  for (int64_t i = bound.start(); OB_SUCC(ret) && i < bound.end(); ++i) {
    if (skip.at(i) || eval_flags.at(i)) {
      continue;
    } else {
      const char *payload = NULL;
      bool is_null = false;
      ObLength len = 0;
      arg_date_vec.get_payload(i, is_null, payload, len);
      ObDatum date(payload, len, is_null);
      int64_t value = 0;
      if (OB_FAIL(ObExprExtract::calc(date_type,
                          date,
                          extract_field,
                          scale,
                          cast_mode,
                          tz_info,
                          cur_ts_value,
                          date_sql_mode,
                          has_lob_header,
                          is_null,
                          value))) {
        LOG_WARN("failed to calculate extract expression", K(ret));
      } else {
        if (is_null) {
          res_vec.set_null(i);
        } else {
          res_vec.set_int(i, value);
        }
        eval_flags.set(i);
      }
    }
  }

  return ret;
}
int sql::ObExprExtract::calc_extract_mysql_vector(const ObExpr &expr, ObEvalCtx &ctx,
                                                  const ObBitVector &skip, const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  const ObSQLSessionInfo *session = NULL;
  if (OB_ISNULL(session = ctx.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound)) ||
             OB_FAIL(expr.args_[1]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("fail to eval params", K(ret));
  } else {
    ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
    ObIVector *arg_unit_vector = expr.args_[0]->get_vector(ctx);
    ObDateUnitType extract_field = static_cast<ObDateUnitType>(arg_unit_vector->get_int(0));
    ObIVector *arg_date_vec = expr.args_[1]->get_vector(ctx);
    ObIVector *res_vec = expr.get_vector(ctx);
    ObObjType date_type = expr.args_[1]->datum_meta_.type_;
    VectorFormat arg_format = arg_date_vec->get_format();
    VectorFormat res_format = res_vec->get_format();
    if (arg_format == VEC_FIXED && res_format == VEC_FIXED) {
      VecValueTypeClass date_vec_tc = expr.args_[1]->get_vec_value_tc();
      VecValueTypeClass res_vec_tc = expr.get_vec_value_tc();
      if (res_vec_tc != VEC_TC_INTEGER) {
        ret = process_vector_mysql<ObVectorBase, ObVectorBase>(
            expr, static_cast<ObVectorBase &>(*arg_date_vec), date_type, bound,
            skip, eval_flags, session, ctx, extract_field,
            static_cast<ObVectorBase &>(*res_vec));
      } else {
        switch (date_vec_tc) {
        case (VEC_TC_DATETIME):
          ret = process_vector_mysql<
              ObFixedLengthFormat<RTCType<VEC_TC_DATETIME>>,
              ObFixedLengthFormat<RTCType<VEC_TC_INTEGER>>>(
              expr,
              static_cast<ObFixedLengthFormat<RTCType<VEC_TC_DATETIME>> &>(
                  *arg_date_vec),
              date_type, bound, skip, eval_flags, session, ctx, extract_field,
              static_cast<ObFixedLengthFormat<RTCType<VEC_TC_INTEGER>> &>(
                  *res_vec));
          break;
        case (VEC_TC_DATE):
          ret = process_vector_mysql<
              ObFixedLengthFormat<RTCType<VEC_TC_DATE>>,
              ObFixedLengthFormat<RTCType<VEC_TC_INTEGER>>>(
              expr,
              static_cast<ObFixedLengthFormat<RTCType<VEC_TC_DATE>> &>(
                  *arg_date_vec),
              date_type, bound, skip, eval_flags, session, ctx, extract_field,
              static_cast<ObFixedLengthFormat<RTCType<VEC_TC_INTEGER>> &>(
                  *res_vec));
          break;

        case (VEC_TC_TIME):
          ret = process_vector_mysql<
              ObFixedLengthFormat<RTCType<VEC_TC_TIME>>,
              ObFixedLengthFormat<RTCType<VEC_TC_INTEGER>>>(
              expr,
              static_cast<ObFixedLengthFormat<RTCType<VEC_TC_TIME>> &>(
                  *arg_date_vec),
              date_type, bound, skip, eval_flags, session, ctx, extract_field,
              static_cast<ObFixedLengthFormat<RTCType<VEC_TC_INTEGER>> &>(
                  *res_vec));
          break;
        case (VEC_TC_YEAR):
          ret = process_vector_mysql<
              ObFixedLengthFormat<RTCType<VEC_TC_YEAR>>,
              ObFixedLengthFormat<RTCType<VEC_TC_INTEGER>>>(
              expr,
              static_cast<ObFixedLengthFormat<RTCType<VEC_TC_YEAR>> &>(
                  *arg_date_vec),
              date_type, bound, skip, eval_flags, session, ctx, extract_field,
              static_cast<ObFixedLengthFormat<RTCType<VEC_TC_INTEGER>> &>(
                  *res_vec));
          break;

        case (VEC_TC_TIMESTAMP_TZ):
          ret = process_vector_mysql<
              ObFixedLengthFormat<RTCType<VEC_TC_TIMESTAMP_TZ>>,
              ObFixedLengthFormat<RTCType<VEC_TC_INTEGER>>>(
              expr,
              static_cast<ObFixedLengthFormat<RTCType<VEC_TC_TIMESTAMP_TZ>> &>(
                  *arg_date_vec),
              date_type, bound, skip, eval_flags, session, ctx, extract_field,
              static_cast<ObFixedLengthFormat<RTCType<VEC_TC_INTEGER>> &>(
                  *res_vec));
          break;
        case (VEC_TC_TIMESTAMP_TINY):
          ret = process_vector_mysql<
              ObFixedLengthFormat<RTCType<VEC_TC_TIMESTAMP_TINY>>,
              ObFixedLengthFormat<RTCType<VEC_TC_INTEGER>>>(
              expr,
              static_cast<ObFixedLengthFormat<RTCType<VEC_TC_TIMESTAMP_TINY>>
                              &>(*arg_date_vec),
              date_type, bound, skip, eval_flags, session, ctx, extract_field,
              static_cast<ObFixedLengthFormat<RTCType<VEC_TC_INTEGER>> &>(
                  *res_vec));
          break;
        case (VEC_TC_INTERVAL_YM):
          ret = process_vector_mysql<
              ObFixedLengthFormat<RTCType<VEC_TC_INTERVAL_YM>>,
              ObFixedLengthFormat<RTCType<VEC_TC_INTEGER>>>(
              expr,
              static_cast<ObFixedLengthFormat<RTCType<VEC_TC_INTERVAL_YM>> &>(
                  *arg_date_vec),
              date_type, bound, skip, eval_flags, session, ctx, extract_field,
              static_cast<ObFixedLengthFormat<RTCType<VEC_TC_INTEGER>> &>(
                  *res_vec));
          break;
        case (VEC_TC_INTERVAL_DS):
          ret = process_vector_mysql<
              ObFixedLengthFormat<RTCType<VEC_TC_INTERVAL_DS>>,
              ObFixedLengthFormat<RTCType<VEC_TC_INTEGER>>>(
              expr,
              static_cast<ObFixedLengthFormat<RTCType<VEC_TC_INTERVAL_DS>> &>(
                  *arg_date_vec),
              date_type, bound, skip, eval_flags, session, ctx, extract_field,
              static_cast<ObFixedLengthFormat<RTCType<VEC_TC_INTEGER>> &>(
                  *res_vec));
          break;
        default:
          ret = process_vector_mysql<
              ObVectorBase, ObFixedLengthFormat<RTCType<VEC_TC_INTEGER>>>(
              expr, static_cast<ObVectorBase &>(*arg_date_vec), date_type,
              bound, skip, eval_flags, session, ctx, extract_field,
              static_cast<ObFixedLengthFormat<RTCType<VEC_TC_INTEGER>> &>(
                  *res_vec));
        }
      }
    } else {
      ret = process_vector_mysql<ObVectorBase, ObVectorBase>(
          expr, static_cast<ObVectorBase &>(*arg_date_vec), date_type, bound,
          skip, eval_flags, session, ctx, extract_field,
          static_cast<ObVectorBase &>(*res_vec));
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to calculate extract expression in vector format", K(ret));
    }
  }

  return ret;
}
#undef EXTRACT_FUN_VAL
#undef EXTRACT_FUN_SWITCH_CASE_MSQ
#undef EXTRACT_FUN_SWITCH_CASE_ORA
} // namespace sql
} // namespace oceanbase
