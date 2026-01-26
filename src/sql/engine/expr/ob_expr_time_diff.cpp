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

#include "sql/engine/expr/ob_expr_time_diff.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_datum_cast.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObExprTimeDiff::ObExprTimeDiff(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_TIME_DIFF, N_TIME_DIFF, 2, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprTimeDiff::~ObExprTimeDiff()
{
}

int ObExprTimeDiff::get_diff_value_with_ob_time(ObTime &ot1,
                                                ObTime &ot2,
                                                const ObTimeZoneInfo *tz_info,
                                                int64_t &diff,
                                                ObTimeZoneInfoPos *literal_tz_info)
{
  int ret = OB_SUCCESS;
  int64_t value1 = 0;
  int64_t value2 = 0;
  ObTimeConvertCtx cvrt_ctx(tz_info, true);
  if (OB_ISNULL(literal_tz_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("literal_tz_info is null", K(ret));
  } else if (OB_UNLIKELY(0 == ot1.parts_[DT_DATE] && 0 != ot2.parts_[DT_DATE])
      || (0 != ot1.parts_[DT_DATE] && 0 == ot2.parts_[DT_DATE])) {
       // invalid in mysql. result will be null
    ret = OB_INVALID_DATE_VALUE;
  } else {
    if (OB_UNLIKELY(0 == ot1.parts_[DT_DATE])) {
      value1 = ObTimeConverter::ob_time_to_time(ot1);
      value2 = ObTimeConverter::ob_time_to_time(ot2);
      diff = value1 - value2;
    } else if (OB_FAIL(ObTimeConverter::ob_time_to_datetime(ot1, cvrt_ctx, value1, *literal_tz_info))) {
      ret = OB_INVALID_DATE_VALUE;//in order to be compatible with mysql
    } else if (OB_FAIL(ObTimeConverter::ob_time_to_datetime(ot2, cvrt_ctx, value2, *literal_tz_info))) {
      ret = OB_INVALID_DATE_VALUE;//in order to be compatible with mysql
    } else {
      diff = value1 - value2;
    }
  }
  return ret;
}

int ObExprTimeDiff::get_diff_value(const ObObj &obj1,
                                   const ObObj &obj2,
                                   const ObTimeZoneInfo *tz_info,
                                   int64_t &diff)
{
  int ret = OB_INVALID_DATE_VALUE;
  ObTime ot1;
  ObTime ot2;
  ObTimeZoneInfoPos literal_tz_info;
  if (OB_LIKELY(OB_SUCCESS == ob_obj_to_ob_time_without_date(obj1, tz_info, ot1)
             && OB_SUCCESS == ob_obj_to_ob_time_without_date(obj2, tz_info, ot2))) {
    ret = get_diff_value_with_ob_time(ot1, ot2, tz_info, diff, &literal_tz_info);
  } /*else ret will be OB_INVALID_DATE_VALUE itself.
      NOT overwritten by last call for ob_obj_to_ob_time_without_date.
      by design*/
  return ret;
}

int ObExprTimeDiff::calc(ObObj &result,
                         const ObObj &left,
                         const ObObj &right,
                         ObCastCtx &cast_ctx)
{
  int ret = OB_SUCCESS;
  int64_t int64_diff = 0;
  if (OB_UNLIKELY(left.is_null() || right.is_null())) {
    result.set_null();
  } else if (OB_FAIL(get_diff_value(left, right, cast_ctx.dtc_params_.tz_info_, int64_diff))) {
    result.set_null();
  } else {
    int flag = int64_diff > 0 ? 1 : -1;
    int64_diff *= flag; //abs operation for time_overflow_trunc
    ret = ObTimeConverter::time_overflow_trunc(int64_diff);
    int64_diff = int64_diff * flag; //restore value
    result.set_time(int64_diff);
  }

  if (OB_SUCCESS != ret && CM_IS_WARN_ON_FAIL(cast_ctx.cast_mode_)) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObExprTimeDiff::cg_expr(ObExprCGCtx &op_cg_ctx,
                              const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  if (rt_expr.arg_cnt_ != 2) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("timediff expr should have two params", K(ret), K(rt_expr.arg_cnt_));
  } else if (OB_ISNULL(rt_expr.args_) || OB_ISNULL(rt_expr.args_[0])
            || OB_ISNULL(rt_expr.args_[1])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children of timediff expr is null", K(ret), K(rt_expr.args_));
  } else {
    rt_expr.eval_func_ = ObExprTimeDiff::calc_timediff;
    // The vectorization of other types for the expression not completed yet.
    if (ob_is_string_tc(rt_expr.args_[0]->datum_meta_.type_)
        && ob_is_string_tc(rt_expr.args_[1]->datum_meta_.type_)) {
      rt_expr.eval_vector_func_ = ObExprTimeDiff::calc_timediff_vector;
    }
  }
  return ret;
}

int ObExprTimeDiff::calc_timediff(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  bool calc_param_failure = false;
  ObDatum *param_datum1 = NULL;
  ObDatum *param_datum2 = NULL;
  const ObSQLSessionInfo *session = NULL;
  ObSolidifiedVarsGetter helper(expr, ctx, ctx.exec_ctx_.get_my_session());
  ObSQLMode sql_mode = 0;
  const common::ObTimeZoneInfo *tz_info = NULL;
  if (OB_ISNULL(session = ctx.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (OB_FAIL(expr.args_[0]->eval(ctx, param_datum1))) {
    calc_param_failure = true;
    LOG_WARN("eval param value failed", K(ret));
  } else if (OB_UNLIKELY(param_datum1->is_null())) {
    expr_datum.set_null();
  } else if (OB_FAIL(helper.get_sql_mode(sql_mode))) {
    LOG_WARN("get sql mode failed", K(ret));
  } else if (OB_FAIL(helper.get_time_zone_info(tz_info))) {
    LOG_WARN("get tz info failed", K(ret));
  } else {
    int64_t int64_diff = 0;
    ObTime ot1(DT_TYPE_TIME);
    ObTime ot2(DT_TYPE_TIME);
    ObTimeZoneInfoPos literal_tz_info;
    if (OB_FAIL(ob_datum_to_ob_time_without_date(
          *param_datum1, expr.args_[0]->datum_meta_.type_, expr.args_[0]->datum_meta_.scale_,
          tz_info, ot1, expr.args_[0]->obj_meta_.has_lob_header()))) {
      LOG_WARN("cast the first param failed", K(ret));
      ret = OB_INVALID_DATE_VALUE;
      expr_datum.set_null();
    } else if (OB_FAIL(expr.args_[1]->eval(ctx, param_datum2))) {
      calc_param_failure = true;
      LOG_WARN("eval param value failed", K(ret));
    } else if (param_datum2->is_null()) {
      expr_datum.set_null();
    } else if (OB_FAIL(ob_datum_to_ob_time_without_date(
                 *param_datum2, expr.args_[1]->datum_meta_.type_, expr.args_[1]->datum_meta_.scale_,
                 tz_info, ot2, expr.args_[1]->obj_meta_.has_lob_header()))) {
      LOG_WARN("cast the second param failed", K(ret));
      ret = OB_INVALID_DATE_VALUE;
      expr_datum.set_null();
    } else if (OB_FAIL(
                 get_diff_value_with_ob_time(ot1, ot2, tz_info, int64_diff, &literal_tz_info))) {
      LOG_WARN("get diff value with ob time failed", K(ret));
      expr_datum.set_null();
    } else {
      LOG_INFO("check for here", K(ot1), K(ot2), K(int64_diff));
      int flag = int64_diff > 0 ? 1 : -1;
      int64_diff *= flag; //abs operation for time_overflow_trunc
      ret = ObTimeConverter::time_overflow_trunc(int64_diff);
      int64_diff = int64_diff * flag; //restore value
      expr_datum.set_time(int64_diff);
    }

    if (OB_SUCCESS != ret) {
      uint64_t cast_mode = 0;
      ObSQLUtils::get_default_cast_mode(session->get_stmt_type(),
                                        session->is_ignore_stmt(),
                                        sql_mode,
                                        cast_mode);
      if (!calc_param_failure && CM_IS_WARN_ON_FAIL(cast_mode)) {
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

#define CHECK_SKIP_NULL_TIMEDIFF(idx) {                 \
  if (skip.at(idx) || eval_flags.at(idx)) {    \
    continue;                                  \
  } else if (arg_vec1->is_null(idx) || arg_vec2->is_null(idx)) {          \
    res_vec->set_null(idx);                    \
    continue;                                  \
  }                                            \
}

#define BATCH_CALC_TIMEDIFF(BODY) {                                                        \
  if (OB_LIKELY(no_skip_no_null)) {                                               \
    for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) { \
      BODY;                                                                       \
    }                                                                             \
  } else {                                                                        \
    for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) { \
      CHECK_SKIP_NULL_TIMEDIFF(idx);                                                       \
      BODY;                                                                       \
    }                                                                             \
  }                                                                               \
}

template <typename ArgVec1, typename ArgVec2, typename ResVec>
int ObExprTimeDiff::calc_timediff_for_string_vector(const ObExpr &expr, ObEvalCtx &ctx,
  const ObBitVector &skip, const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  ArgVec1 *arg_vec1 = static_cast<ArgVec1 *>(expr.args_[0]->get_vector(ctx));
  ArgVec2 *arg_vec2 = static_cast<ArgVec2 *>(expr.args_[1]->get_vector(ctx));
  ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  bool no_skip_no_null = bound.get_all_rows_active() && !arg_vec1->has_null() && !arg_vec2->has_null()
                          && eval_flags.accumulate_bit_cnt(bound) == 0;
  const ObSQLSessionInfo *session = NULL;
  ObSolidifiedVarsGetter helper(expr, ctx, ctx.exec_ctx_.get_my_session());
  ObSQLMode sql_mode = 0;
  const common::ObTimeZoneInfo *tz_info = NULL;
  if (OB_ISNULL(session = ctx.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (OB_FAIL(helper.get_sql_mode(sql_mode))) {
    LOG_WARN("get sql mode failed", K(ret));
  } else if (OB_FAIL(helper.get_time_zone_info(tz_info))) {
    LOG_WARN("get tz info failed", K(ret));
  } else {
    int64_t last_first_8digits1 = INT64_MAX;
    int64_t last_first_8digits2 = INT64_MAX;
    bool with_date = true;
    bool is_time_type1 = N_FALSE;
    bool is_time_type2 = N_FALSE;
    bool use_quick_parser = true;
    int quick_parser_failed_count = 0;
    ObTime ob_time1;
    ObTime ob_time2;
    ObScale res_scale1 = -1;
    ObScale res_scale2 = -1;
    ObTimeZoneInfoPos literal_tz_info;
    ObTimeConverter::ObTimeDigits digits1[DATETIME_PART_CNT];
    ObTimeConverter::ObTimeDigits digits2[DATETIME_PART_CNT];
    ObTimeConverter::QuickParserType last_quick_parser_type1 = ObTimeConverter::QuickParserType::QuickParserUnused;
    ObTimeConverter::QuickParserType last_quick_parser_type2 = ObTimeConverter::QuickParserType::QuickParserUnused;
    uint64_t cast_mode = 0;
    int64_t local_tz_offset_usec = 0;
    ObSQLUtils::get_default_cast_mode(session->get_stmt_type(),
                                  session->is_ignore_stmt(),
                                  sql_mode,
                                  cast_mode);
    if (tz_info != NULL) {
      if (tz_info->get_tz_id() > 0) {
        ret = OB_NOT_SUPPORTED;
      }
      local_tz_offset_usec = SEC_TO_USEC(tz_info->get_offset());
    }
    BATCH_CALC_TIMEDIFF({
      ObString in_val1 = arg_vec1->get_string(idx);
      ObString in_val2 = arg_vec2->get_string(idx);
      bool datetime_valid1 = false;
      bool datetime_valid2 = false;
      bool is_match_format1 = false;
      bool is_match_format2 = false;
      int64_t diff = 0;

      // Parse first time value
      if (use_quick_parser) {
        ObTimeConverter::string_to_obtime_quick(in_val1.ptr(),
                  in_val1.length(),
                  ob_time1,
                  datetime_valid1,
                  is_match_format1,
                  last_first_8digits1,
                  last_quick_parser_type1,
                  is_time_type1);
      }

      if (datetime_valid1 && is_match_format1) {
        ob_time1.parts_[DT_DATE] = ObTimeConverter::ob_time_to_date(ob_time1);
      } else {
        last_quick_parser_type1 = ObTimeConverter::QuickParserType::QuickParserUnused;
        if (use_quick_parser) {
          quick_parser_failed_count++;
          if (quick_parser_failed_count % 32 == 0) {
            if (quick_parser_failed_count * 2 > idx) {
              use_quick_parser = false;
            }
          }
        }
        if (OB_FAIL(ObTimeConverter::str_to_ob_time_without_date(in_val1, ob_time1,
            &res_scale1, false, digits1))) {
          LOG_WARN("cast first param to ob time failed", K(ret), K(in_val1));
          LOG_USER_WARN(OB_ERR_CAST_VARCHAR_TO_TIME);
        }
      }

      // Parse second time value
      if (OB_SUCC(ret)) {
        if (use_quick_parser) {
          ObTimeConverter::string_to_obtime_quick(in_val2.ptr(),
                    in_val2.length(),
                    ob_time2,
                    datetime_valid2,
                    is_match_format2,
                    last_first_8digits2,
                    last_quick_parser_type2,
                    is_time_type2);
        }

        if (datetime_valid2 && is_match_format2) {
          ob_time2.parts_[DT_DATE] = ObTimeConverter::ob_time_to_date(ob_time2);
        } else {
          last_quick_parser_type2 = ObTimeConverter::QuickParserType::QuickParserUnused;
          if (OB_FAIL(ObTimeConverter::str_to_ob_time_without_date(in_val2, ob_time2,
              &res_scale2, false, digits2))) {
            LOG_WARN("cast second param to ob time failed", K(ret), K(in_val2));
            LOG_USER_WARN(OB_ERR_CAST_VARCHAR_TO_TIME);
          }
        }
      }

      // Calculate time difference
      if (OB_SUCC(ret)) {
        if (datetime_valid2 && is_match_format2 && datetime_valid1 && is_match_format1) {
          int64_t value1 = ob_time1.parts_[DT_DATE] * USECS_PER_DAY
            + ObTimeConverter::ob_time_to_time(ob_time1) - local_tz_offset_usec;
          int64_t value2 = ob_time2.parts_[DT_DATE] * USECS_PER_DAY
            + ObTimeConverter::ob_time_to_time(ob_time2) - local_tz_offset_usec;
          diff = value1 - value2;
        } else if (OB_FAIL(ObExprTimeDiff::get_diff_value_with_ob_time(ob_time1, ob_time2, tz_info, diff, &literal_tz_info))) {
          LOG_WARN("get diff value with ob time failed", K(ret));
          res_vec->set_null(idx);
        }
        if (OB_SUCC(ret)) {
          int flag = diff > 0 ? 1 : -1;
          diff *= flag; //abs operation for time_overflow_trunc
          ret = ObTimeConverter::time_overflow_trunc(diff);
          diff = diff * flag; //restore value
          res_vec->set_time(idx, diff);
        }
      }
      if (OB_FAIL(ret)) {
        if (CM_IS_WARN_ON_FAIL(cast_mode)) {
          ret = OB_SUCCESS; // Reset ret for this row, continue processing
        }
        // If not warning mode, let ret propagate to stop batch processing
      }
    });
  }
  return ret;
}

#define DISPATCH_TIMEDIFF_STRING_EXPR_VECTOR()\
if (VEC_FIXED == res_format) {\
  if (VEC_DISCRETE == arg_format1 && VEC_DISCRETE == arg_format2) {\
    ret = calc_timediff_for_string_vector<CONCAT(Str, DiscVec), CONCAT(Str, DiscVec), CONCAT(Time, FixedVec)>(expr, ctx, skip, bound);\
  } else if (VEC_DISCRETE == arg_format1 && VEC_UNIFORM == arg_format2) {\
    ret = calc_timediff_for_string_vector<CONCAT(Str, DiscVec), CONCAT(Str, UniVec), CONCAT(Time, FixedVec)>(expr, ctx, skip, bound);\
  } else if (VEC_DISCRETE == arg_format1 && VEC_CONTINUOUS == arg_format2) {\
    ret = calc_timediff_for_string_vector<CONCAT(Str, DiscVec), CONCAT(Str, ContVec), CONCAT(Time, FixedVec)>(expr, ctx, skip, bound);\
  } else if (VEC_UNIFORM == arg_format1 && VEC_DISCRETE == arg_format2) {\
    ret = calc_timediff_for_string_vector<CONCAT(Str, UniVec), CONCAT(Str, DiscVec), CONCAT(Time, FixedVec)>(expr, ctx, skip, bound);\
  } else if (VEC_UNIFORM == arg_format1 && VEC_UNIFORM == arg_format2) {\
    ret = calc_timediff_for_string_vector<CONCAT(Str, UniVec), CONCAT(Str, UniVec), CONCAT(Time, FixedVec)>(expr, ctx, skip, bound);\
  } else if (VEC_UNIFORM == arg_format1 && VEC_CONTINUOUS == arg_format2) {\
    ret = calc_timediff_for_string_vector<CONCAT(Str, UniVec), CONCAT(Str, ContVec), CONCAT(Time, FixedVec)>(expr, ctx, skip, bound);\
  } else if (VEC_CONTINUOUS == arg_format1 && VEC_DISCRETE == arg_format2) {\
    ret = calc_timediff_for_string_vector<CONCAT(Str, ContVec), CONCAT(Str, DiscVec), CONCAT(Time, FixedVec)>(expr, ctx, skip, bound);\
  } else if (VEC_CONTINUOUS == arg_format1 && VEC_UNIFORM == arg_format2) {\
    ret = calc_timediff_for_string_vector<CONCAT(Str, ContVec), CONCAT(Str, UniVec), CONCAT(Time, FixedVec)>(expr, ctx, skip, bound);\
  } else if (VEC_CONTINUOUS == arg_format1 && VEC_CONTINUOUS == arg_format2) {\
    ret = calc_timediff_for_string_vector<CONCAT(Str, ContVec), CONCAT(Str, ContVec), CONCAT(Time, FixedVec)>(expr, ctx, skip, bound);\
  } else { \
    ret = calc_timediff_for_string_vector<ObVectorBase, ObVectorBase, ObVectorBase>(expr, ctx, skip, bound);\
  }\
} else { \
  ret = calc_timediff_for_string_vector<ObVectorBase, ObVectorBase, ObVectorBase>(expr, ctx, skip, bound);\
}

int ObExprTimeDiff::calc_timediff_vector(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.eval_vector_param_value_null_short_circuit(ctx, skip, bound))) {
    LOG_WARN("fail to eval timediff param", K(ret));
  } else {
    VectorFormat arg_format1 = expr.args_[0]->get_format(ctx);
    VectorFormat arg_format2 = expr.args_[1]->get_format(ctx);
    VectorFormat res_format = expr.get_format(ctx);
    const ObObjType arg_type1 = expr.args_[0]->datum_meta_.type_;
    const ObObjType arg_type2 = expr.args_[1]->datum_meta_.type_;
    if (ObStringTC == ob_obj_type_class(arg_type1) && ObStringTC == ob_obj_type_class(arg_type2)) {
      DISPATCH_TIMEDIFF_STRING_EXPR_VECTOR();
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("expr calculation failed", K(ret));
    }
  }
  return ret;
}

DEF_SET_LOCAL_SESSION_VARS(ObExprTimeDiff, raw_expr) {
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

#undef CHECK_SKIP_NULL_TIMEDIFF
#undef BATCH_CALC_TIMEDIFF

} //namespace sql
} //namespace oceanbase
