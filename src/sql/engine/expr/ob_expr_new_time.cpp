/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_new_time.h"
#include "sql/engine/ob_exec_context.h"
#include "lib/timezone/ob_time_convert.h"
#include "lib/timezone/ob_timezone_info.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/ob_name_def.h"
#include "share/ob_errno.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObExprNewTime::ObExprNewTime(common::ObIAllocator &alloc)
  : ObFuncExprOperator(alloc, T_FUN_SYS_NEW_TIME, N_NEW_TIME, 3,
                      NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

int ObExprNewTime::calc_result_type3(ObExprResType &type,
                                     ObExprResType &input1,
                                     ObExprResType &input2,
                                     ObExprResType &input3,
                                     common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  const ObSQLSessionInfo *session = NULL;

  if (OB_ISNULL(session = type_ctx.get_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else {
    // NEW_TIME always returns DATE type (ObDateTimeType with scale 0)
    type.set_datetime();
    type.set_scale(0);  // DATE type has no fractional seconds

    // First parameter: date (can be DATE, DATETIME, TIMESTAMP, etc.)
    input1.set_calc_type(ObDateTimeType);

    // Second and third parameters: timezone abbreviations (strings)
    input2.set_calc_type(ObVarcharType);
    input3.set_calc_type(ObVarcharType);
    input2.set_calc_collation_ascii_compatible();
    input3.set_calc_collation_ascii_compatible();
  }

  return ret;
}

int ObExprNewTime::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                           ObExpr &expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);

  if (3 != expr.arg_cnt_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument count", K(ret), K(expr.arg_cnt_));
  } else if (OB_ISNULL(expr.args_) || OB_ISNULL(expr.args_[0])
             || OB_ISNULL(expr.args_[1]) || OB_ISNULL(expr.args_[2])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children of NEW_TIME expr is null", K(ret), K(expr.args_));
  } else if (ObDateTimeType != expr.args_[0]->datum_meta_.type_
             || ObDateTimeType != expr.datum_meta_.type_
             || ObVarcharType != expr.args_[1]->datum_meta_.type_
             || ObVarcharType != expr.args_[2]->datum_meta_.type_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument type", K(ret),
             K(expr.args_[0]->datum_meta_.type_),
             K(expr.args_[1]->datum_meta_.type_),
             K(expr.args_[2]->datum_meta_.type_));
  } else {
    expr.eval_func_ = ObExprNewTime::eval_new_time;
    expr.eval_vector_func_ = ObExprNewTime::eval_new_time_vector;
  }

  return ret;
}

// Timezone Abbreviation to UTC Offset Mapping (Oracle Compatible)
// Abbr  | Full Name                           | UTC Offset
// ------+-------------------------------------+-----------
// AST   | Atlantic Standard Time              | -04:00
// ADT   | Atlantic Daylight Time              | -03:00
// BST   | Bering Standard Time                | -11:00
// BDT   | Bering Daylight Time                | -10:00
// CST   | Central Standard Time               | -06:00
// CDT   | Central Daylight Time               | -05:00
// EST   | Eastern Standard Time               | -05:00
// EDT   | Eastern Daylight Time               | -04:00
// GMT   | Greenwich Mean Time                 | +00:00
// HST   | Hawaii-Aleutian Standard Time       | -10:00
// HDT   | Hawaii-Aleutian Daylight Time       | -09:00
// MST   | Mountain Standard Time              | -07:00
// MDT   | Mountain Daylight Time              | -06:00
// NST   | Newfoundland Standard Time          | -03:30
// PST   | Pacific Standard Time               | -08:00
// PDT   | Pacific Daylight Time               | -07:00
// YST   | Yukon Standard Time                 | -09:00
// YDT   | Yukon Daylight Time                 | -08:00
int ObExprNewTime::get_timezone_offset(const ObString &tz_abbr, int32_t &offset)
{
  int ret = OB_SUCCESS;
  offset = 0;

  if (0 == tz_abbr.case_compare("AST")) {
    offset = -4 * 3600;  // Atlantic Standard Time: UTC-4
  } else if (0 == tz_abbr.case_compare("ADT")) {
    offset = -3 * 3600;  // Atlantic Daylight Time: UTC-3
  } else if (0 == tz_abbr.case_compare("BST")) {
    offset = -11 * 3600; // Bering Standard Time: UTC-11
  } else if (0 == tz_abbr.case_compare("BDT")) {
    offset = -10 * 3600; // Bering Daylight Time: UTC-10
  } else if (0 == tz_abbr.case_compare("CST")) {
    offset = -6 * 3600;  // Central Standard Time: UTC-6
  } else if (0 == tz_abbr.case_compare("CDT")) {
    offset = -5 * 3600;  // Central Daylight Time: UTC-5
  } else if (0 == tz_abbr.case_compare("EST")) {
    offset = -5 * 3600;  // Eastern Standard Time: UTC-5
  } else if (0 == tz_abbr.case_compare("EDT")) {
    offset = -4 * 3600;  // Eastern Daylight Time: UTC-4
  } else if (0 == tz_abbr.case_compare("GMT")) {
    offset = 0;           // Greenwich Mean Time: UTC+0
  } else if (0 == tz_abbr.case_compare("HST")) {
    offset = -10 * 3600; // Hawaii-Aleutian Standard Time: UTC-10
  } else if (0 == tz_abbr.case_compare("HDT")) {
    offset = -9 * 3600;  // Hawaii-Aleutian Daylight Time: UTC-9
  } else if (0 == tz_abbr.case_compare("MST")) {
    offset = -7 * 3600;  // Mountain Standard Time: UTC-7
  } else if (0 == tz_abbr.case_compare("MDT")) {
    offset = -6 * 3600;  // Mountain Daylight Time: UTC-6
  } else if (0 == tz_abbr.case_compare("NST")) {
    offset = -3 * 3600 - 1800; // Newfoundland Standard Time: UTC-3:30
  } else if (0 == tz_abbr.case_compare("PST")) {
    offset = -8 * 3600;  // Pacific Standard Time: UTC-8
  } else if (0 == tz_abbr.case_compare("PDT")) {
    offset = -7 * 3600;  // Pacific Daylight Time: UTC-7
  } else if (0 == tz_abbr.case_compare("YST")) {
    offset = -9 * 3600;  // Yukon Standard Time: UTC-9
  } else if (0 == tz_abbr.case_compare("YDT")) {
    offset = -8 * 3600;  // Yukon Daylight Time: UTC-8
  } else {
    ret = OB_ERR_NOT_A_VALID_TIME_ZONE;
    LOG_WARN("not a valid time zone", K(tz_abbr), K(ret));
  }

  return ret;
}

int ObExprNewTime::calc_new_time(int64_t timestamp_data,
                                 const ObString &from_tz,
                                 const ObString &to_tz,
                                 ObDatum &result)
{
  int ret = OB_SUCCESS;
  int32_t from_offset = 0;
  int32_t to_offset = 0;

  // Get offsets for both timezones
  if (OB_FAIL(get_timezone_offset(from_tz, from_offset))) {
    LOG_WARN("not a valid time zone", K(ret), K(from_tz));
  } else if (OB_FAIL(get_timezone_offset(to_tz, to_offset))) {
    LOG_WARN("not a valid time zone", K(ret), K(to_tz));
  } else {

    int32_t offset_diff = to_offset - from_offset;
    int64_t res_value = timestamp_data + static_cast<int64_t>(offset_diff) * USECS_PER_SEC;

    // Range check - return error if out of range
    // In Oracle mode, year 0 does not exist (1 BC is before 1 AD), so use ORACLE_DATETIME_MIN_VAL
    if (OB_UNLIKELY(res_value < ORACLE_DATETIME_MIN_VAL || res_value > DATETIME_MAX_VAL)) {
      ret = OB_ERR_INVALID_YEAR_VALUE;
      LOG_WARN("date field overflow for NEW_TIME", K(res_value), K(ORACLE_DATETIME_MIN_VAL), K(DATETIME_MAX_VAL), K(ret));
    } else {
      result.set_datetime(res_value);
    }
  }

  return ret;
}

int ObExprNewTime::eval_new_time(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObDatum *date_datum = NULL;
  ObDatum *from_tz_datum = NULL;
  ObDatum *to_tz_datum = NULL;

  if (OB_FAIL(expr.eval_param_value(ctx, date_datum, from_tz_datum, to_tz_datum))) {
    LOG_WARN("eval param value failed", K(ret));
  } else if (OB_UNLIKELY(date_datum->is_null() ||
                         from_tz_datum->is_null() ||
                         to_tz_datum->is_null())) {
    res.set_null();
  } else {
    int64_t timestamp_data = date_datum->get_datetime();
    const ObString &from_tz = from_tz_datum->get_string();
    const ObString &to_tz = to_tz_datum->get_string();

    if (OB_FAIL(calc_new_time(timestamp_data, from_tz, to_tz, res))) {
      LOG_WARN("calc NEW_TIME failed", K(ret), K(from_tz), K(to_tz));
    }
  }

  return ret;
}

// Helper function to apply timezone offset and set result
template <typename ResVec, typename IN_TYPE>
OB_INLINE int ObExprNewTime::apply_timezone_offset_and_set_result(IN_TYPE timestamp_val,
                                                                    int32_t from_offset,
                                                                    int32_t to_offset,
                                                                    ResVec *res_vec,
                                                                    int64_t idx)
{
  int32_t offset_diff = to_offset - from_offset;
  int64_t res_value = timestamp_val + static_cast<int64_t>(offset_diff) * USECS_PER_SEC;

  int ret = OB_SUCCESS;

  // Range check - return error if out of range
  // In Oracle mode, year 0 does not exist (1 BC is before 1 AD), so use ORACLE_DATETIME_MIN_VAL
    if (OB_UNLIKELY(res_value < ORACLE_DATETIME_MIN_VAL || res_value > DATETIME_MAX_VAL)) {
      ret = OB_ERR_INVALID_YEAR_VALUE;
      LOG_WARN("date field overflow for NEW_TIME", K(res_value), K(ORACLE_DATETIME_MIN_VAL), K(DATETIME_MAX_VAL), K(ret));
    } else {
    res_vec->set_datetime(idx, res_value);
  }

  return ret;
}


template <typename ArgVec, typename ResVec, typename IN_TYPE>
int ObExprNewTime::new_time_vector(const ObExpr &expr,
                                   ObEvalCtx &ctx,
                                   const ObBitVector &skip,
                                   const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  ArgVec *arg_vec = static_cast<ArgVec *>(expr.args_[0]->get_vector(ctx));
  ObIVector *from_tz_vec = expr.args_[1]->get_vector(ctx);
  ObIVector *to_tz_vec = expr.args_[2]->get_vector(ctx);
  ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);

  if (OB_FAIL(expr.args_[1]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("eval from_tz failed", K(ret));
  } else if (OB_FAIL(expr.args_[2]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("eval to_tz failed", K(ret));
  } else {
    // Fast path: no skip, no null, no need to check eval_flags
    const bool no_skip_no_null = bound.get_all_rows_active()
                                  && !arg_vec->has_null()
                                  && !from_tz_vec->has_null()
                                  && !to_tz_vec->has_null();

    if (no_skip_no_null) {
      // Fast path: skip all checks
      for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
        ObString from_tz = from_tz_vec->get_string(idx);
        ObString to_tz = to_tz_vec->get_string(idx);

        int32_t from_offset = 0;
        int32_t to_offset = 0;

        if (OB_FAIL(get_timezone_offset(from_tz, from_offset))) {
          LOG_WARN("not a valid time zone", K(ret), K(from_tz));
        } else if (OB_FAIL(get_timezone_offset(to_tz, to_offset))) {
          LOG_WARN("not a valid time zone", K(ret), K(to_tz));
        } else {
          IN_TYPE timestamp_val = *reinterpret_cast<const IN_TYPE*>(arg_vec->get_payload(idx));
          ret = ObExprNewTime::apply_timezone_offset_and_set_result<ResVec, IN_TYPE>(timestamp_val, from_offset, to_offset, res_vec, idx);
        }
      }
    } else {
      // Slow path: need to check skip, eval_flags, and nulls
      for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
        if (skip.at(idx) || eval_flags.at(idx)) {
          continue;
        }

        if (arg_vec->is_null(idx) || from_tz_vec->is_null(idx) || to_tz_vec->is_null(idx)) {
          res_vec->set_null(idx);
          continue;
        }

        IN_TYPE timestamp_val = *reinterpret_cast<const IN_TYPE*>(arg_vec->get_payload(idx));
        ObString from_tz = from_tz_vec->get_string(idx);
        ObString to_tz = to_tz_vec->get_string(idx);

        int32_t from_offset = 0;
        int32_t to_offset = 0;

        if (OB_FAIL(get_timezone_offset(from_tz, from_offset))) {
          LOG_WARN("not a valid time zone", K(ret), K(from_tz));
        } else if (OB_FAIL(get_timezone_offset(to_tz, to_offset))) {
          LOG_WARN("not a valid time zone", K(ret), K(to_tz));
        } else {
          IN_TYPE timestamp_val = *reinterpret_cast<const IN_TYPE*>(arg_vec->get_payload(idx));
          ret = ObExprNewTime::apply_timezone_offset_and_set_result<ResVec, IN_TYPE>(timestamp_val, from_offset, to_offset, res_vec, idx);
        }
      }


    }
  }
  return ret;
}

template <typename ArgVec, typename ResVec, typename IN_TYPE>
int ObExprNewTime::new_time_vector_const(const ObExpr &expr,
                                         ObEvalCtx &ctx,
                                         const ObBitVector &skip,
                                         const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  ArgVec *arg_vec = static_cast<ArgVec *>(expr.args_[0]->get_vector(ctx));
  ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);

  int64_t const_skip = 0;
  const ObBitVector *param_skip = to_bit_vector(&const_skip);

  if (OB_FAIL(expr.args_[1]->eval_vector(ctx, *param_skip, EvalBound(1)))) {
    LOG_WARN("eval from_tz failed", K(ret));
  } else if (OB_FAIL(expr.args_[2]->eval_vector(ctx, *param_skip, EvalBound(1)))) {
    LOG_WARN("eval to_tz failed", K(ret));
  } else {
    ObIVector *from_tz_vec = expr.args_[1]->get_vector(ctx);
    ObIVector *to_tz_vec = expr.args_[2]->get_vector(ctx);

    if (arg_vec->is_null(0) || from_tz_vec->is_null(0) || to_tz_vec->is_null(0)) {
      // If timezone is NULL, set all results to NULL
      for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
        if (skip.at(idx) || eval_flags.at(idx)) {
          continue;
        }
        res_vec->set_null(idx);
      }
    } else {
      ObString from_tz = from_tz_vec->get_string(0);
      ObString to_tz = to_tz_vec->get_string(0);

      int32_t from_offset = 0;
      int32_t to_offset = 0;

      if (OB_FAIL(get_timezone_offset(from_tz, from_offset))) {
        // Error message already logged, propagate error
      } else if (OB_FAIL(get_timezone_offset(to_tz, to_offset))) {
        // Error message already logged, propagate error
      } else {
        int32_t offset_diff = to_offset - from_offset;

        // Fast path: no skip, no null, no need to check eval_flags
        const bool no_skip_no_null = bound.get_all_rows_active() && !arg_vec->has_null();

        if (no_skip_no_null) {
          // Fast path: skip all checks
          for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
            IN_TYPE timestamp_val = *reinterpret_cast<const IN_TYPE*>(arg_vec->get_payload(idx));
            ret = ObExprNewTime::apply_timezone_offset_and_set_result<ResVec, IN_TYPE>(timestamp_val, from_offset, to_offset, res_vec, idx);
          }
        } else {
          // Slow path: need to check skip, eval_flags, and nulls
          for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
            if (skip.at(idx) || eval_flags.at(idx)) {
              continue;
            }

            if (arg_vec->is_null(idx)) {
              res_vec->set_null(idx);
              continue;
            }
            IN_TYPE timestamp_val = *reinterpret_cast<const IN_TYPE*>(arg_vec->get_payload(idx));
            ret = ObExprNewTime::apply_timezone_offset_and_set_result<ResVec, IN_TYPE>(timestamp_val, from_offset, to_offset, res_vec, idx);
          }
        }
      }
    }
  }
  return ret;
}

#define DISPATCH_NEW_TIME_VECTOR_FUNC(FUNC, TYPE)                                                             \
  if (VEC_FIXED == arg_format && VEC_FIXED == res_format) {                                                     \
    ret = FUNC<CONCAT(TYPE, FixedVec), CONCAT(TYPE, FixedVec), CONCAT(TYPE, Type)>(expr, ctx, skip, bound);     \
  } else if (VEC_FIXED == arg_format && VEC_UNIFORM == res_format) {                                            \
    ret = FUNC<CONCAT(TYPE, FixedVec), CONCAT(TYPE, UniVec), CONCAT(TYPE, Type)>(expr, ctx, skip, bound);       \
  } else if (VEC_FIXED == arg_format && VEC_UNIFORM_CONST == res_format) {                                      \
    ret = FUNC<CONCAT(TYPE, FixedVec), CONCAT(TYPE, UniCVec), CONCAT(TYPE, Type)>(expr, ctx, skip, bound);      \
  } else if (VEC_UNIFORM == arg_format && VEC_FIXED == res_format) {                                            \
    ret = FUNC<CONCAT(TYPE, UniVec), CONCAT(TYPE, FixedVec), CONCAT(TYPE, Type)>(expr, ctx, skip, bound);       \
  } else if (VEC_UNIFORM == arg_format && VEC_UNIFORM == res_format) {                                          \
    ret = FUNC<CONCAT(TYPE, UniVec), CONCAT(TYPE, UniVec), CONCAT(TYPE, Type)>(expr, ctx, skip, bound);         \
  } else if (VEC_UNIFORM == arg_format && VEC_UNIFORM_CONST == res_format) {                                    \
    ret = FUNC<CONCAT(TYPE, UniVec), CONCAT(TYPE, UniCVec), CONCAT(TYPE, Type)>(expr, ctx, skip, bound);        \
  } else if (VEC_UNIFORM_CONST == arg_format && VEC_FIXED == res_format) {                                      \
    ret = FUNC<CONCAT(TYPE, UniCVec), CONCAT(TYPE, FixedVec), CONCAT(TYPE, Type)>(expr, ctx, skip, bound);      \
  } else if (VEC_UNIFORM_CONST == arg_format && VEC_UNIFORM == res_format) {                                    \
    ret = FUNC<CONCAT(TYPE, UniCVec), CONCAT(TYPE, UniVec), CONCAT(TYPE, Type)>(expr, ctx, skip, bound);        \
  } else if (VEC_UNIFORM_CONST == arg_format && VEC_UNIFORM_CONST == res_format) {                              \
    ret = FUNC<CONCAT(TYPE, UniCVec), CONCAT(TYPE, UniCVec), CONCAT(TYPE, Type)>(expr, ctx, skip, bound);       \
  } else {                                                                                                      \
    ret = FUNC<ObVectorBase, ObVectorBase, CONCAT(TYPE, Type)>(expr, ctx, skip, bound);                         \
  }

int ObExprNewTime::eval_new_time_vector(const ObExpr &expr,
                                        ObEvalCtx &ctx,
                                        const ObBitVector &skip,
                                        const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("fail to eval date param", K(ret));
  } else {
    VectorFormat arg_format = expr.args_[0]->get_format(ctx);
    VectorFormat res_format = expr.get_format(ctx);
    ObObjTypeClass arg_tc = ob_obj_type_class(expr.args_[0]->datum_meta_.type_);

    if (expr.args_[1]->is_const_expr() && expr.args_[2]->is_const_expr()) {
      // Both timezone parameters are constants
      if (ObDateTimeTC == arg_tc) {
        DISPATCH_NEW_TIME_VECTOR_FUNC(new_time_vector_const, DateTime);
      }
    } else {
      // At least one timezone parameter is not constant
      if (ObDateTimeTC == arg_tc) {
        DISPATCH_NEW_TIME_VECTOR_FUNC(new_time_vector, DateTime);
      }
    }

    if (OB_FAIL(ret)) {
      LOG_WARN("expr calculation failed", K(ret));
    }
  }
  return ret;
}

#undef DISPATCH_NEW_TIME_VECTOR_FUNC

} // namespace sql
} // namespace oceanbase
