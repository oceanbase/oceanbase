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

#include "sql/engine/expr/ob_expr_date_add_ck.h"
#include "sql/engine/ob_exec_context.h"
#include "lib/timezone/ob_time_convert.h"
#include "ob_datum_cast.h"
#include "sql/session/ob_sql_session_info.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{

ObExprDateAdjustClickhouse::ObExprDateAdjustClickhouse(common::ObIAllocator &alloc,
                                                       ObExprOperatorType type,
                                                       const char *name,
                                                       int32_t param_num,
                                                       int32_t dimension)
    : ObFuncExprOperator(alloc, type, name, param_num, VALID_FOR_GENERATED_COL, dimension)
{
}

ObExprDateAdjustClickhouse::~ObExprDateAdjustClickhouse()
{
}

int ObExprDateAdjustClickhouse::calc_result_type3(ObExprResType &type,
                                                   ObExprResType &date,
                                                   ObExprResType &interval,
                                                   ObExprResType &unit,
                                                   ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  ObDateUnitType unit_type = static_cast<ObDateUnitType>(unit.get_param().get_int());

  if (OB_UNLIKELY(ObNullType == date.get_type())) {
    type.set_null();
  } else if (OB_UNLIKELY(INTERVAL_INDEX[unit_type].begin_ != INTERVAL_INDEX[unit_type].end_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguemnt for unit type", K(ret), K(unit_type));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "interval type");
  } else {
    // Set calc types for parameters
    // unit is already int type (enum), no need to set calc_type

    // ClickHouse behavior: interval value is always truncated (floor) regardless of decimal
    // For all units, we set interval calc_type to IntType to automatically truncate
    interval.set_calc_type(ObIntType);

    bool is_subsecond_unit = (unit_type == DATE_UNIT_NANOSECOND ||
                              unit_type == DATE_UNIT_MICROSECOND ||
                              unit_type == DATE_UNIT_MILLISECOND);

    // ClickHouse rule: Date + sub-second unit must error
    if (OB_UNLIKELY(ob_is_date_or_mysql_date(date.get_type()) && is_subsecond_unit)) {
      ret = OB_INVALID_ARGUMENT;
      const char* unit_name = (unit_type == DATE_UNIT_NANOSECOND) ? "NANOSECOND" :
                             (unit_type == DATE_UNIT_MICROSECOND) ? "MICROSECOND" :
                             "MILLISECOND";
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "unit");
      LOG_WARN("Date type cannot be used with sub-second units in ClickHouse mode",
               K(ret), K(unit_type), K(unit_name));
    } else {
      // OceanBase types: date, datetime, timestamp (no MySQL-specific types)
      ObScale base_scale = 0;
      if (ob_is_datetime_or_mysql_datetime(date.get_type()) || date.is_timestamp()) {
        // For datetime/timestamp types, get current scale (0-6 in OceanBase)
        base_scale = date.get_scale();
      } else if (date.is_string_type()) {
        // For string type: ClickHouse assumes millisecond precision (3 digits)
        // This allows string values like '2024-01-15 10:00:00.123' to be preserved
        base_scale = 3;
        date.set_calc_scale(3);
      }
      ObScale required_scale = 0;
      if (unit_type == DATE_UNIT_NANOSECOND) {
        // ClickHouse uses precision 9, but OceanBase max is 6
        required_scale = 6;
      } else if (unit_type == DATE_UNIT_MICROSECOND) {
        required_scale = 6;
      } else if (unit_type == DATE_UNIT_MILLISECOND) {
        required_scale = 3;
      }
      ObScale final_scale = std::max(base_scale, required_scale);
      if (ob_is_date_or_mysql_date(date.get_type())) {
        if (unit_type == DATE_UNIT_YEAR || unit_type == DATE_UNIT_MONTH ||
            unit_type == DATE_UNIT_DAY || unit_type == DATE_UNIT_YEAR_MONTH ||
            unit_type == DATE_UNIT_WEEK || unit_type == DATE_UNIT_QUARTER) {
          // date + day-level or higher units -> remain date
          type.set_type(date.get_type());
          final_scale = 0;
        } else {
          // date + time units (HOUR, MINUTE, SECOND) -> datetime
          ObObjType target_type = type_ctx.enable_mysql_compatible_dates() ? ObMySQLDateTimeType : ObDateTimeType;
          type.set_type(target_type);
          date.set_calc_type(target_type);
        }
      } else {
        // datetime, timestamp, or string input -> all become datetime with appropriate scale
        ObObjType target_type = type_ctx.enable_mysql_compatible_dates() ? ObMySQLDateTimeType : ObDateTimeType;
        type.set_type(target_type);
        date.set_calc_type(target_type);
      }
      type.set_scale(final_scale);
      type_ctx.set_cast_mode(type_ctx.get_cast_mode() | CM_STRING_INTEGER_TRUNC);
    }
  }

  return ret;
}


/**
 * Core calculation function for ClickHouse-compatible date adjustment
 *
 * Note: At execution time, parameters are already converted by calc_type:
 * - date parameter: datetime (int64 microseconds)
 * - interval parameter: int64 (truncated)
 * - unit parameter: int (ObDateUnitType enum)
 */
int ObExprDateAdjustClickhouse::calc_date_adjust_ck(const ObExpr &expr,
                                                     ObEvalCtx &ctx,
                                                     ObDatum &expr_datum,
                                                     bool is_add)
{
  int ret = OB_SUCCESS;
  ObDatum *date = NULL;
  ObDatum *interval = NULL;
  ObDatum *unit = NULL;
  const ObSQLSessionInfo *session = NULL;

  if (OB_ISNULL(session = ctx.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (OB_FAIL(expr.eval_param_value(ctx, date, interval, unit))) {
    LOG_WARN("eval param value failed", K(ret));
  } else if (date->is_null() || interval->is_null()) {
    expr_datum.set_null();
  } else {
    int64_t dt_val = 0;
    int64_t interval_value = interval->get_int();
    ObDateUnitType unit_val = static_cast<ObDateUnitType>(unit->get_int());
    int64_t res_dt_val = 0;
    ObDateSqlMode date_sql_mode;
    switch (expr.args_[0]->datum_meta_.type_) {
      case ObDateType: {
        ObTimeConvertCtx cvrt_ctx(NULL, false);
        ret = ObTimeConverter::date_to_datetime(date->get_date(), cvrt_ctx, dt_val);
        break;
      }
      case ObDateTimeType: {
        dt_val = date->get_datetime();
        break;
      }
      case ObMySQLDateType: {
        ObTimeConvertCtx cvrt_ctx(NULL, false);
        ret = ObTimeConverter::mdate_to_datetime(date->get_mysql_date(), cvrt_ctx, dt_val, date_sql_mode);
        break;
      }
      case ObMySQLDateTimeType: {
        ret = ObTimeConverter::mdatetime_to_datetime(date->get_mysql_datetime(), dt_val, date_sql_mode);
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        break;
      }
    }
    if OB_FAIL(ret) {
      LOG_WARN("get datetime value failed", K(ret), K(expr.args_[0]->datum_meta_.type_));
    } else if (OB_FAIL(ObTimeConverter::date_adjust(dt_val, interval_value, unit_val,
                                             res_dt_val, is_add, date_sql_mode))) {
      uint64_t cast_mode = 0;
      ObSQLMode sql_mode = 0;
      ObSQLUtils::get_default_cast_mode(session->get_stmt_type(),
                                        session->is_ignore_stmt(),
                                        sql_mode,
                                        cast_mode);
      if (CM_IS_WARN_ON_FAIL(cast_mode)) {
        expr_datum.set_null();
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("date_adjust failed", K(ret), K(dt_val), K(interval_value), K(unit_val), K(is_add));
      }
    } else {
      const ObObjType res_type = expr.datum_meta_.type_;
      if (ObDateType == res_type) {
        int32_t d_val = 0;
        if (OB_FAIL(ObTimeConverter::datetime_to_date(res_dt_val, NULL, d_val))) {
          LOG_WARN("failed to convert datetime to date", K(ret), K(res_dt_val));
        } else {
          expr_datum.set_date(d_val);
        }
      } else if (ObDateTimeType == res_type) {
        expr_datum.set_datetime(res_dt_val);
      } else if (ObMySQLDateType == res_type) {
        ObMySQLDate md_val = 0;
        if (OB_FAIL(ObTimeConverter::datetime_to_mdate(res_dt_val, NULL, md_val))) {
          LOG_WARN("failed to convert datetime to mysql_date", K(ret), K(res_dt_val));
        } else {
          expr_datum.set_mysql_date(md_val);
        }
      } else if (ObMySQLDateTimeType == res_type) {
        ObMySQLDateTime mdatetime = 0;
        if (OB_FAIL(ObTimeConverter::datetime_to_mdatetime(res_dt_val, mdatetime))) {
          LOG_WARN("failed to convert datetime to mysql_datetime", K(ret), K(res_dt_val));
        } else {
          expr_datum.set_mysql_datetime(mdatetime);
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected result type", K(ret), K(res_type));
      }
    }
  }
  return ret;
}

// ========== ObExprDateAddClickhouse ==========
ObExprDateAddClickhouse::ObExprDateAddClickhouse(common::ObIAllocator &alloc)
    : ObExprDateAdjustClickhouse(alloc, T_FUN_SYS_DATE_ADD_CLICKHOUSE, N_DATE_ADD, 3, NOT_ROW_DIMENSION)
{
}

int ObExprDateAddClickhouse::cg_expr(ObExprCGCtx &op_cg_ctx,
                                     const ObRawExpr &raw_expr,
                                     ObExpr &rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(rt_expr.arg_cnt_ != 3)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument count", K(ret), K(rt_expr.arg_cnt_));
  } else if (OB_ISNULL(rt_expr.args_[0]) || OB_ISNULL(rt_expr.args_[1])
            || OB_ISNULL(rt_expr.args_[2])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child expr is null", K(ret));
  } else if (OB_UNLIKELY(rt_expr.datum_meta_.type_ != rt_expr.args_[0]->datum_meta_.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("result type and param type not match", K(ret), K(rt_expr), KPC(rt_expr.args_[0]));
  } else {
    rt_expr.eval_func_ = calc_date_add_ck;
    rt_expr.eval_vector_func_ = calc_date_add_ck_vector;
  }
  return ret;
}

int ObExprDateAddClickhouse::calc_date_add_ck(const ObExpr &expr,
                                              ObEvalCtx &ctx,
                                              ObDatum &expr_datum)
{
  return calc_date_adjust_ck(expr, ctx, expr_datum, true);
}

// ========== ObExprDateSubClickhouse ==========
ObExprDateSubClickhouse::ObExprDateSubClickhouse(common::ObIAllocator &alloc)
    : ObExprDateAdjustClickhouse(alloc, T_FUN_SYS_DATE_SUB_CLICKHOUSE, N_DATE_SUB, 3, NOT_ROW_DIMENSION)
{
}

int ObExprDateSubClickhouse::cg_expr(ObExprCGCtx &op_cg_ctx,
                                     const ObRawExpr &raw_expr,
                                     ObExpr &rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(rt_expr.arg_cnt_ != 3)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument count", K(ret), K(rt_expr.arg_cnt_));
  } else if (OB_ISNULL(rt_expr.args_[0]) || OB_ISNULL(rt_expr.args_[1])
            || OB_ISNULL(rt_expr.args_[2])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child expr is null", K(ret));
  } else if (OB_UNLIKELY(rt_expr.datum_meta_.type_ != rt_expr.args_[0]->datum_meta_.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("result type and param type not match", K(ret), K(rt_expr), KPC(rt_expr.args_[0]));
  } else {
    rt_expr.eval_func_ = calc_date_sub_ck;
    rt_expr.eval_vector_func_ = calc_date_sub_ck_vector;
  }

  return ret;
}

int ObExprDateSubClickhouse::calc_date_sub_ck(const ObExpr &expr,
                                              ObEvalCtx &ctx,
                                              ObDatum &expr_datum)
{
  return calc_date_adjust_ck(expr, ctx, expr_datum, false);
}

// Helper template function for vectorized date_adjust in ClickHouse mode
// ResType: ObDateType, ObDateTimeType, ObMySQLDateType, ObMySQLDateTimeType
template <typename DateVec, typename IntervalVec, typename ResVec, ObObjType ResType>
static int vector_date_adjust_ck(const ObExpr &expr,
                                  ObEvalCtx &ctx,
                                  const ObBitVector &skip,
                                  const EvalBound &bound,
                                  bool is_add)
{
  int ret = OB_SUCCESS;
  DateVec *date_vec = static_cast<DateVec *>(expr.args_[0]->get_vector(ctx));
  IntervalVec *interval_vec = static_cast<IntervalVec *>(expr.args_[1]->get_vector(ctx));
  IntegerUniCVec *unit_vec = static_cast<IntegerUniCVec *>(expr.args_[2]->get_vector(ctx));
  ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  ObDateSqlMode date_sql_mode;
  ObDateUnitType unit_val = static_cast<ObDateUnitType>(unit_vec->get_int(0));
  const ObSQLSessionInfo *session = NULL;
  if (OB_ISNULL(session = ctx.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  }
  for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
    if (skip.at(idx) || eval_flags.at(idx)) {
      continue;
    }
    if (date_vec->is_null(idx) || interval_vec->is_null(idx)) {
      res_vec->set_null(idx);
    } else {
      int64_t dt_val = 0;
      switch(ResType) {
        case ObDateType: {
          ObTimeConvertCtx cvrt_ctx(NULL, false);
          ret = ObTimeConverter::date_to_datetime(date_vec->get_date(idx), cvrt_ctx, dt_val);
          break;
        }
        case ObDateTimeType: {
          dt_val = date_vec->get_datetime(idx);
          break;
        }
        case ObMySQLDateType: {
          ObTimeConvertCtx cvrt_ctx(NULL, false);
          ret = ObTimeConverter::mdate_to_datetime(date_vec->get_mysql_date(idx), cvrt_ctx, dt_val, date_sql_mode);
          break;
        }
        case ObMySQLDateTimeType: {
          ret = ObTimeConverter::mdatetime_to_datetime(date_vec->get_mysql_datetime(idx), dt_val, date_sql_mode);
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          break;
        }
      }
      int64_t interval_value = interval_vec->get_int(idx);
      int64_t res_dt_val = 0;
      if (OB_FAIL(ret)) {
        LOG_WARN("get param datetime value failed", K(ret), K(ResType));
      } else if (OB_FAIL(ObTimeConverter::date_adjust(dt_val, interval_value, unit_val,
                                                res_dt_val, is_add, date_sql_mode))) {
        uint64_t cast_mode = 0;
        ObSQLMode sql_mode = 0;
        ObSQLUtils::get_default_cast_mode(session->get_stmt_type(),
                                          session->is_ignore_stmt(),
                                          sql_mode,
                                          cast_mode);
        if (CM_IS_WARN_ON_FAIL(cast_mode)) {
          res_vec->set_null(idx);
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("date_adjust failed", K(ret), K(dt_val), K(interval_value), K(unit_val), K(is_add));
        }
      } else {
        if (ObDateType == ResType) {
          int32_t d_val = 0;
          if (OB_FAIL(ObTimeConverter::datetime_to_date(res_dt_val, NULL, d_val))) {
            LOG_WARN("failed to convert datetime to date", K(ret), K(res_dt_val));
          } else {
            res_vec->set_date(idx, d_val);
          }
        } else if (ObDateTimeType == ResType) {
          res_vec->set_datetime(idx, res_dt_val);
        } else if (ObMySQLDateType == ResType) {
          ObMySQLDate md_val = 0;
          if (OB_FAIL(ObTimeConverter::datetime_to_mdate(res_dt_val, NULL, md_val))) {
            LOG_WARN("failed to convert datetime to mysql_date", K(ret), K(res_dt_val));
          } else {
            res_vec->set_mysql_date(idx, md_val);
          }
        } else if (ObMySQLDateTimeType == ResType) {
          ObMySQLDateTime mdatetime = 0;
          if (OB_FAIL(ObTimeConverter::datetime_to_mdatetime(res_dt_val, mdatetime))) {
            LOG_WARN("failed to convert datetime to mysql_datetime", K(ret), K(res_dt_val));
          } else {
            res_vec->set_mysql_datetime(idx, mdatetime);
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected result type", K(ret), K(ResType));
        }
      }
    }
  }

  return ret;
}

#define DISPATCH_CK_INTERVAL(date_vec_type, res_vec_type, res_type, is_add_param) \
      if (VEC_FIXED == interval_format) { \
        ret = vector_date_adjust_ck<date_vec_type, IntegerFixedVec, res_vec_type, res_type>( \
            expr, ctx, skip, bound, is_add_param); \
      } else if (VEC_UNIFORM == interval_format) { \
        ret = vector_date_adjust_ck<date_vec_type, IntegerUniVec, res_vec_type, res_type>( \
            expr, ctx, skip, bound, is_add_param); \
      } else if (VEC_UNIFORM_CONST == interval_format) { \
        ret = vector_date_adjust_ck<date_vec_type, IntegerUniCVec, res_vec_type, res_type>( \
            expr, ctx, skip, bound, is_add_param); \
      } else { \
        ret = vector_date_adjust_ck<date_vec_type, ObVectorBase, res_vec_type, res_type>( \
            expr, ctx, skip, bound, is_add_param); \
      }

#define DISPATCH_CK_DATE_RES(res_vec_prefix, res_type, is_add_param) \
      if (VEC_FIXED == date_format && VEC_FIXED == res_format) { \
        DISPATCH_CK_INTERVAL(CONCAT(res_vec_prefix, FixedVec), CONCAT(res_vec_prefix, FixedVec), res_type, is_add_param) \
      } else if (VEC_FIXED == date_format && VEC_UNIFORM == res_format) { \
        DISPATCH_CK_INTERVAL(CONCAT(res_vec_prefix, FixedVec), CONCAT(res_vec_prefix, UniVec), res_type, is_add_param) \
      } else if (VEC_FIXED == date_format && VEC_UNIFORM_CONST == res_format) { \
        DISPATCH_CK_INTERVAL(CONCAT(res_vec_prefix, FixedVec), CONCAT(res_vec_prefix, UniCVec), res_type, is_add_param) \
      } else if (VEC_UNIFORM == date_format && VEC_FIXED == res_format) { \
        DISPATCH_CK_INTERVAL(CONCAT(res_vec_prefix, UniVec), CONCAT(res_vec_prefix, FixedVec), res_type, is_add_param) \
      } else if (VEC_UNIFORM == date_format && VEC_UNIFORM == res_format) { \
        DISPATCH_CK_INTERVAL(CONCAT(res_vec_prefix, UniVec), CONCAT(res_vec_prefix, UniVec), res_type, is_add_param) \
      } else if (VEC_UNIFORM == date_format && VEC_UNIFORM_CONST == res_format) { \
        DISPATCH_CK_INTERVAL(CONCAT(res_vec_prefix, UniVec), CONCAT(res_vec_prefix, UniCVec), res_type, is_add_param) \
      } else if (VEC_UNIFORM_CONST == date_format && VEC_FIXED == res_format) { \
        DISPATCH_CK_INTERVAL(CONCAT(res_vec_prefix, UniCVec), CONCAT(res_vec_prefix, FixedVec), res_type, is_add_param) \
      } else if (VEC_UNIFORM_CONST == date_format && VEC_UNIFORM == res_format) { \
        DISPATCH_CK_INTERVAL(CONCAT(res_vec_prefix, UniCVec), CONCAT(res_vec_prefix, UniVec), res_type, is_add_param) \
      } else if (VEC_UNIFORM_CONST == date_format && VEC_UNIFORM_CONST == res_format) { \
        DISPATCH_CK_INTERVAL(CONCAT(res_vec_prefix, UniCVec), CONCAT(res_vec_prefix, UniCVec), res_type, is_add_param) \
      } else { \
        DISPATCH_CK_INTERVAL(ObVectorBase, ObVectorBase, res_type, is_add_param) \
      }

int ObExprDateAddClickhouse::calc_date_add_ck_vector(const ObExpr &expr,
                                                      ObEvalCtx &ctx,
                                                      const ObBitVector &skip,
                                                      const EvalBound &bound)
{
  int ret = OB_SUCCESS;

  // Evaluate all input vectors
  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound)) ||
      OB_FAIL(expr.args_[1]->eval_vector(ctx, skip, bound)) ||
      OB_FAIL(expr.args_[2]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("eval arg failed", K(ret));
  } else {
    VectorFormat date_format = expr.args_[0]->get_format(ctx);
    VectorFormat interval_format = expr.args_[1]->get_format(ctx);
    VectorFormat unit_format = expr.args_[2]->get_format(ctx);
    VectorFormat res_format = expr.get_format(ctx);

    // Unit parameter must be const (SQL keyword like DAY, MONTH, etc.)
    if (OB_UNLIKELY(VEC_UNIFORM_CONST != unit_format)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unit format should be const", K(ret), K(unit_format));
    } else {
      // Enumerate date, interval, and result format combinations (27 combinations per result type)
      // Unit is always IntegerUniCVec (constant)
      if (ObDateType == expr.datum_meta_.type_) {
        DISPATCH_CK_DATE_RES(Date, ObDateType, true/*is_add*/)
      } else if (ObMySQLDateType == expr.datum_meta_.type_) {
        DISPATCH_CK_DATE_RES(MySQLDate, ObMySQLDateType, true/*is_add*/)
      } else if (ObMySQLDateTimeType == expr.datum_meta_.type_) {
        DISPATCH_CK_DATE_RES(MySQLDateTime, ObMySQLDateTimeType, true/*is_add*/)
      } else {
        DISPATCH_CK_DATE_RES(DateTime, ObDateTimeType, true/*is_add*/)
      }
    }
  }

  return ret;
}

int ObExprDateSubClickhouse::calc_date_sub_ck_vector(const ObExpr &expr,
                                                      ObEvalCtx &ctx,
                                                      const ObBitVector &skip,
                                                      const EvalBound &bound)
{
  int ret = OB_SUCCESS;

  // Evaluate all input vectors
  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound)) ||
      OB_FAIL(expr.args_[1]->eval_vector(ctx, skip, bound)) ||
      OB_FAIL(expr.args_[2]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("eval arg failed", K(ret));
  } else {
    VectorFormat date_format = expr.args_[0]->get_format(ctx);
    VectorFormat interval_format = expr.args_[1]->get_format(ctx);
    VectorFormat unit_format = expr.args_[2]->get_format(ctx);
    VectorFormat res_format = expr.get_format(ctx);

    // Unit parameter must be const (SQL keyword)
    if (OB_UNLIKELY(VEC_UNIFORM_CONST != unit_format)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unit format should be const", K(ret), K(unit_format));
    } else {
      // Enumerate date, interval, and result format combinations (27 combinations per result type)
      // Unit is always IntegerUniCVec (constant)
      if (ObDateType == expr.datum_meta_.type_) {
        DISPATCH_CK_DATE_RES(Date, ObDateType, false/*is_add*/)
      } else if (ObMySQLDateType == expr.datum_meta_.type_) {
        DISPATCH_CK_DATE_RES(MySQLDate, ObMySQLDateType, false/*is_add*/)
      } else if (ObMySQLDateTimeType == expr.datum_meta_.type_) {
        DISPATCH_CK_DATE_RES(MySQLDateTime, ObMySQLDateTimeType, false/*is_add*/)
      } else {
        DISPATCH_CK_DATE_RES(DateTime, ObDateTimeType, false/*is_add*/)
      }
    }
  }

  return ret;
}

} // namespace sql
} // namespace oceanbase
