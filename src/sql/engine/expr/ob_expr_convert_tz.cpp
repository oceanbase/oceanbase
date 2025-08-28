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
#include "sql/engine/expr/ob_expr_convert_tz.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObExprConvertTZ::ObExprConvertTZ(common::ObIAllocator &alloc):
ObFuncExprOperator(alloc, T_FUN_SYS_CONVERT_TZ, "convert_TZ", 3, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION){
}

int ObExprConvertTZ::calc_result_type3(ObExprResType &type,
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
    int16_t scale1 = MIN(input1.get_scale(), MAX_SCALE_FOR_TEMPORAL);
    scale1 = (SCALE_UNKNOWN_YET == scale1) ? MAX_SCALE_FOR_TEMPORAL : scale1;
    type.set_scale(scale1);
    type.set_datetime();
    input1.set_calc_type(ObDateTimeType);
    input2.set_calc_type(ObVarcharType);
    input3.set_calc_type(ObVarcharType);
    input2.set_calc_collation_ascii_compatible();
    input3.set_calc_collation_ascii_compatible();
  }
  return ret;
}

int ObExprConvertTZ::calc_convert_tz(int64_t timestamp_data,
                                    const ObString &tz_str_s,//source time zone (input2)
                                    const ObString &tz_str_d,//destination time zone (input3)
                                    ObSQLSessionInfo *session,
                                    ObDatum &result)
{
  int ret = OB_SUCCESS;
  int32_t offset_couple =0;
  if (OB_FAIL(get_offset_by_couple_tz(timestamp_data, offset_couple, tz_str_s, tz_str_d, session))) {
    if (OB_ERR_UNKNOWN_TIME_ZONE == ret) {
      ret = OB_SUCCESS;
      if(OB_FAIL(ObExprConvertTZ::parse_string(timestamp_data, tz_str_s, session, false))){
        LOG_WARN("source time zone parse failed", K(ret), K(tz_str_s));
      } else if(OB_FAIL(ObExprConvertTZ::parse_string(timestamp_data, tz_str_d, session, true))){
        LOG_WARN("source time zone parse failed", K(ret), K(tz_str_d));
      }
    } else if (OB_SUCCESS != ret) {
      LOG_DEBUG("calc_convert_tz failed", K(ret), K(tz_str_s), K(tz_str_d));
    }
  }
  if (OB_FAIL(ret)) {
    ret = OB_SUCCESS;
    result.set_null();
  } else {
    int64_t res_value = timestamp_data + (static_cast<int64_t>(offset_couple)) * 1000000;
    LOG_DEBUG("calc_convert_tz succeed", K(offset_couple), K(timestamp_data));
    if (OB_UNLIKELY(res_value < MYSQL_TIMESTAMP_MIN_VAL || res_value > MYSQL_TIMESTAMP_MAX_VAL)) {
      result.set_null();
    } else {
      result.set_datetime(res_value);
    }
  }
  return ret;
}

int ObExprConvertTZ::calc_convert_tz_timestamp(const ObExpr &expr, 
                                               ObEvalCtx &ctx, 
                                               int64_t &timestamp_data, 
                                               const ObString &tz_str_s, 
                                               const ObString &tz_str_d, 
                                               ObSQLSessionInfo *session) {
  int ret = OB_SUCCESS;
  ObExecContext *exec_ctx = &ctx.exec_ctx_;
  ObExprConvertTZCtx *cvrt_ctx = nullptr;

  bool is_batched_multi_stmt = true;
  if (OB_NOT_NULL(ctx.exec_ctx_.get_sql_ctx())) {
    is_batched_multi_stmt = ctx.exec_ctx_.get_sql_ctx()->multi_stmt_item_.is_batched_multi_stmt();
  }

  cvrt_ctx = static_cast<ObExprConvertTZCtx*>(exec_ctx->get_expr_op_ctx(expr.expr_ctx_id_));
  
  // for batched_multi_stmt, expr_op_ctx may be shared, so we need get tz_info each time
  if (!is_batched_multi_stmt && OB_NOT_NULL(cvrt_ctx)) {
    // reuse existing expr_op_ctx
  } else {
    if (OB_ISNULL(cvrt_ctx) && OB_FAIL(exec_ctx->create_expr_op_ctx(expr.expr_ctx_id_, cvrt_ctx))) {
      LOG_WARN("create expr op ctx failed", K(ret));
    } else if (OB_FAIL(get_cvrt_tz_info(tz_str_s, session, cvrt_ctx->tz_info_wrap_src_))) {
      cvrt_ctx->find_tz_ret_ = ret;
      LOG_WARN("get tz_st_pos failed", K(ret));
    } else if (OB_FAIL(get_cvrt_tz_info(tz_str_d, session, cvrt_ctx->tz_info_wrap_dst_))) {
      cvrt_ctx->find_tz_ret_ = ret;
      LOG_WARN("get tz_dst_pos failed", K(ret));
    }
  }

  if (OB_FAIL(ret) || OB_FAIL(cvrt_ctx->find_tz_ret_)) {
  } else if (OB_FAIL(handle_timezone_offset(timestamp_data, cvrt_ctx->tz_info_wrap_src_, false))) {
    LOG_WARN("handle source timezone offset failed", K(ret));
  } else if (OB_FAIL(handle_timezone_offset(timestamp_data, cvrt_ctx->tz_info_wrap_dst_, true))) {
    LOG_WARN("handle destination timezone offset failed", K(ret));
  }

  return ret;
}

int ObExprConvertTZ::handle_timezone_offset(int64_t &timestamp_data, const ConvertTZInfoWrap &tz_info_wrap, bool is_destination) {
  int ret = OB_SUCCESS;
  if (tz_info_wrap.is_position_class()) {
    if (OB_FAIL(calc(timestamp_data, tz_info_wrap.get_tz_info_pos(), is_destination))) {
      LOG_WARN("calc failed", K(ret), K(timestamp_data));
    }
  } else if (tz_info_wrap.is_offset_class()) {
    int32_t offset = tz_info_wrap.get_tz_offset();
    if (is_destination) {
      timestamp_data += (offset * USECS_PER_SEC);
    } else {
      timestamp_data -= (offset * USECS_PER_SEC);
    }
    LOG_DEBUG(is_destination ? "dst to offset succeed" : "src to offset succeed", K(offset));
  }
  return ret;
}


int ObExprConvertTZ::calc_convert_tz_const(
    const ObExpr &expr, ObEvalCtx &ctx, int64_t &timestamp_data,
    const ObString &tz_str_s, // source time zone (input4)
    const ObString &tz_str_d, // destination time zone (input5)
    ObSQLSessionInfo *session, ObDatum &result) {
  int ret = OB_SUCCESS;
  if (OB_FAIL(calc_convert_tz_timestamp(expr, ctx, timestamp_data, tz_str_s, tz_str_d, session))) {
    LOG_WARN("calc_timestamp_value failed", K(ret), K(tz_str_s), K(tz_str_d));
    ret = OB_SUCCESS;
    result.set_null();
  } else {
    if (OB_UNLIKELY(timestamp_data < MYSQL_TIMESTAMP_MIN_VAL || timestamp_data > MYSQL_TIMESTAMP_MAX_VAL)) {
      result.set_null();
    } else {
      result.set_datetime(timestamp_data);
    }
  }
  return ret;
}

int ObExprConvertTZ::get_cvrt_tz_info(const ObString &tz_str,
                                         ObSQLSessionInfo *session,
                                         ConvertTZInfoWrap &tz_info_wrap) {
  int ret = OB_SUCCESS;
  int ret_more = 0;
  int32_t offset = 0;
  if (OB_FAIL(ObTimeConverter::str_to_offset(tz_str, offset, ret_more,
                              false /* oracle_mode */, true /* need_check_valid */))) {
    LOG_WARN("get time zone failed", K(ret), K(tz_str));
    if (OB_LIKELY(OB_ERR_UNKNOWN_TIME_ZONE == ret)){
      const ObTimeZoneInfo *tz_info = NULL;
      ObTimeZoneInfoPos *target_tz_pos = NULL;
      if (OB_ISNULL(tz_info = TZ_INFO(session))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tz info is null", K(ret), K(session));
      } else if (OB_FAIL(find_time_zone_pos(tz_str, *tz_info, tz_info_wrap.get_tz_info_pos()))){
        LOG_WARN("find time zone position failed", K(ret), K(ret_more));
        if (OB_ERR_UNKNOWN_TIME_ZONE == ret && OB_SUCCESS != ret_more) {
          ret = ret_more;
        }
      } else {
        ret = OB_SUCCESS;
        tz_info_wrap.set_position_class();
      }
    }
  } else {
    tz_info_wrap.set_tz_offset(offset);
  }
  return ret;
}

int ObExprConvertTZ::get_offset_by_couple_tz(int64_t timestamp_data, int32_t &offset, const ObString &tz_str_s, const ObString &tz_str_d, ObSQLSessionInfo *session)
{
  int ret = OB_SUCCESS;
  int32_t *tz_offset = NULL;
  const ObTimeZoneInfo *tz_info = NULL;
  ObTZInfoMap *tz_info_map = NULL;
  if (OB_ISNULL(tz_info = TZ_INFO(session))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tz info is null", K(ret), K(session));
  } else if (OB_ISNULL(tz_info_map = const_cast<ObTZInfoMap *>(tz_info->get_tz_info_map()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tz_info_map is NULL", K(ret));
  } else if (OB_FAIL(tz_info_map->get_offset_by_couple_tz_name(timestamp_data, tz_str_s, tz_str_d, offset))) {
    if (OB_ERR_UNKNOWN_TIME_ZONE != ret && OB_FAIL(ret)) {
      LOG_WARN("get offset by couple tz failed", K(ret), K(tz_str_s), K(tz_str_d));
    }
  } 
  return ret;
}

int ObExprConvertTZ::parse_string(int64_t &timestamp_data, const ObString &tz_str,
                                ObSQLSessionInfo *session, const bool input_utc_time)
{
  int ret = OB_SUCCESS;
  int ret_more = 0;
  int32_t offset = 0;
  const ObTimeZoneInfo *tz_info = NULL;
  ObTimeZoneInfoPos *target_tz_pos = NULL;

  if (OB_ISNULL(tz_info = TZ_INFO(session))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tz info is null", K(ret), K(session));
  } else if (OB_SUCC(find_time_zone_pos(tz_str, *tz_info, target_tz_pos))) {
    // Successfully found in timezone map, proceed with calculation
    if (OB_FAIL(calc(timestamp_data, *target_tz_pos, input_utc_time))) {
      LOG_WARN("calc failed", K(ret), K(timestamp_data));
    }
    if (NULL != target_tz_pos) {
      const_cast<ObTZInfoMap *>(tz_info->get_tz_info_map())->revert_tz_info_pos(target_tz_pos);
      target_tz_pos = NULL;
    }
  } else if (OB_ERR_UNKNOWN_TIME_ZONE == ret) {
    // Fallback to str_to_offset when timezone not found in table
    LOG_DEBUG("time zone not found in tz_info, try str_to_offset", K(tz_str));
    if (OB_FAIL(ObTimeConverter::str_to_offset(tz_str, offset, ret_more,
                                false /* oracle_mode */, true /* need_check_valid */))) {
      LOG_WARN("both time zone search and str_to_offset failed", K(ret), K(tz_str));
    } else if(OB_FAIL(ret_more)) {
      ret = ret_more;
    } else {
      // str_to_offset succeeded, apply offset directly
      ret = OB_SUCCESS;
      timestamp_data += (input_utc_time ? 1 : -1) * offset * USECS_PER_SEC;
      LOG_DEBUG("str to offset succeed", K(tz_str), K(offset));
    }
  } else {
    LOG_WARN("find_time_zone_pos failed with unexpected error", K(ret), K(tz_str));
  }

  return ret;
}

int ObExprConvertTZ::find_time_zone_pos(const ObString &tz_name,
                                        const ObTimeZoneInfo &tz_info,
                                        ObTimeZoneInfoPos *&tz_info_pos)
{
  int ret = OB_SUCCESS;
  ObTZInfoMap *tz_info_map = NULL;
  if (OB_ISNULL(tz_info_map = const_cast<ObTZInfoMap *>(tz_info.get_tz_info_map()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tz_info_map is NULL", K(ret));
  } else if (OB_FAIL(tz_info_map->get_tz_info_by_name(tz_name, tz_info_pos))) {
    LOG_WARN("fail to get_tz_info_by_name", K(tz_name), K(ret));
  } else {
    tz_info_pos->set_error_on_overlap_time(tz_info.is_error_on_overlap_time());
  }
  return ret;
}


int ObExprConvertTZ::find_time_zone_pos(const ObString &tz_name,
                                        const ObTimeZoneInfo &tz_info,
                                        ObTimeZoneInfoPos &tz_info_pos)
{
  int ret = OB_SUCCESS;
  ObTZInfoMap *tz_info_map = NULL;
  if (OB_ISNULL(tz_info_map = const_cast<ObTZInfoMap *>(tz_info.get_tz_info_map()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tz_info_map is NULL", K(ret));
  } else if (OB_FAIL(tz_info_map->get_tz_info_by_name(tz_name, tz_info_pos))) {
    LOG_WARN("fail to get_tz_info_by_name", K(tz_name), K(ret));
  } else {
    tz_info_pos.set_error_on_overlap_time(tz_info.is_error_on_overlap_time());
  }
  return ret;
}

int ObExprConvertTZ::calc(int64_t &timestamp_data, const ObTimeZoneInfoPos &tz_info_pos,
                          const bool input_utc_time)
{
  int ret = OB_SUCCESS;
  const int64_t input_value = timestamp_data;
  if (input_utc_time) {
    if (OB_FAIL(ObTimeConverter::timestamp_to_datetime(input_value, &tz_info_pos, timestamp_data))) {
      LOG_WARN("add timezone offset to utc time failed", K(ret), K(timestamp_data));
    }
  } else {
    if (OB_FAIL(ObTimeConverter::datetime_to_timestamp(input_value, &tz_info_pos, timestamp_data))) {
      LOG_WARN("sub timezone offset fail", K(ret));
    }
  }
  LOG_DEBUG("convert tz calc", K(timestamp_data), K(input_utc_time));
  return ret;
}

int ObExprConvertTZ::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
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
    LOG_WARN("children of convert_tz expr is null", K(ret), K(expr.args_));
  } else if (ObDateTimeType != expr.args_[0]->datum_meta_.type_
    || ObDateTimeType != expr.datum_meta_.type_
    || ObVarcharType != expr.args_[1]->datum_meta_.type_
    || ObVarcharType != expr.args_[2]->datum_meta_.type_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument type", K(ret), K(expr.args_[0]->datum_meta_),
             K(expr.args_[1]->datum_meta_), K(expr.args_[2]->datum_meta_));
  } else {
    expr.eval_func_ = ObExprConvertTZ::eval_convert_tz;
    expr.eval_vector_func_ = ObExprConvertTZ::calc_convert_tz_vector;
  }
  return ret;
}

int ObExprConvertTZ::eval_convert_tz(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObDatum *timestamp = NULL;
  ObDatum *time_zone_s = NULL;
  ObDatum *time_zone_d = NULL;
  if (OB_FAIL(expr.eval_param_value(ctx, timestamp, time_zone_s, time_zone_d))) {
    LOG_WARN("calc param value failed", K(ret));
  } else if (OB_UNLIKELY(timestamp->is_null() || time_zone_s->is_null() || time_zone_d->is_null())) {
    res.set_null();
  } else {
    int64_t timestamp_data = timestamp->get_datetime();
    if (expr.args_[1]->is_const_expr() && expr.args_[2]->is_const_expr()) {
      if(OB_FAIL(calc_convert_tz_const(expr, ctx, timestamp_data, time_zone_s->get_string(), time_zone_d->get_string(),
                                  ctx.exec_ctx_.get_my_session(), res))) {
        LOG_WARN("calc convert tz zone failed", K(ret));
      }
    } else {
      if (OB_FAIL(calc_convert_tz(timestamp_data, time_zone_s->get_string(), time_zone_d->get_string(),
                                    ctx.exec_ctx_.get_my_session(), res))) {
        LOG_WARN("calc convert tz zone failed", K(ret));
      }
    }
  }
  return ret;
}

template <typename ArgVec, typename ResVec, typename IN_TYPE>
int ObExprConvertTZ::convert_tz_vector(const ObExpr &expr,
                                      ObEvalCtx &ctx,
                                      const ObBitVector &skip,
                                      const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  ArgVec *arg_vec = static_cast<ArgVec *>(expr.args_[0]->get_vector(ctx));
  ObIVector *str_s_vec = expr.args_[1]->get_vector(ctx);
  ObIVector *str_d_vec = expr.args_[2]->get_vector(ctx);
  ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  if (OB_FAIL(expr.args_[1]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("eval fmt failed", K(ret));
  } else if (OB_FAIL(expr.args_[2]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("eval fmt failed", K(ret));
  } else {
    ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
    for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
      if (skip.at(idx) || eval_flags.at(idx)) { continue; }
      IN_TYPE left_val = *reinterpret_cast<const IN_TYPE*>(arg_vec->get_payload(idx));
      int64_t res_val = left_val;
      int32_t offset_couple =0;
      if (arg_vec->is_null(idx) || str_s_vec->is_null(idx) || str_s_vec->get_string(idx).empty() ||
              str_d_vec->is_null(idx) || str_d_vec->get_string(idx).empty()) {
         res_vec->set_null(idx);
      } else {
        ObString tz_str_s = str_s_vec->get_string(idx);
        ObString tz_str_d = str_d_vec->get_string(idx);
        if (OB_FAIL(get_offset_by_couple_tz(res_val, offset_couple, tz_str_s, tz_str_d, session))) {
          if (OB_ERR_UNKNOWN_TIME_ZONE == ret) {
            ret = OB_SUCCESS;
            if(OB_FAIL(ObExprConvertTZ::parse_string(res_val, tz_str_s, session, false))){
              LOG_WARN("source time zone parse failed", K(ret), K(tz_str_s));
            } else if(OB_FAIL(ObExprConvertTZ::parse_string(res_val, tz_str_d, session, true))){
              LOG_WARN("source time zone parse failed", K(ret), K(tz_str_d));
            }
          } else if (OB_SUCCESS != ret) {
            LOG_DEBUG("calc_convert_tz failed", K(ret), K(tz_str_s), K(tz_str_d));
          }
        }
        if (OB_FAIL(ret)){
          ret = OB_SUCCESS;
          res_vec->set_null(idx);
        } else {
          res_val += (static_cast<int64_t>(offset_couple)) * 1000000;
          if (OB_UNLIKELY(res_val < MYSQL_TIMESTAMP_MIN_VAL || res_val > MYSQL_TIMESTAMP_MAX_VAL)) {
            res_vec->set_null(idx);
          } else {
            res_vec->set_datetime(idx,res_val);
          }
        }
      }
      eval_flags.set(idx);
    }
  }
  return ret;
}

template <typename ArgVec, typename ResVec, typename IN_TYPE>
int ObExprConvertTZ::convert_tz_vector_const(const ObExpr &expr,
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
    LOG_WARN("eval fmt failed", K(ret));
  } else if (OB_FAIL(expr.args_[2]->eval_vector(ctx, *param_skip, EvalBound(1)))) {
    LOG_WARN("eval fmt failed", K(ret));
  } else if (expr.args_[1]->get_vector(ctx)->is_null(0) ||
              expr.args_[1]->get_vector(ctx)->get_string(0).empty() ||
              expr.args_[2]->get_vector(ctx)->is_null(0) ||
              expr.args_[2]->get_vector(ctx)->get_string(0).empty()) {
    for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
      if (skip.at(idx) || eval_flags.at(idx)) { continue; }
      res_vec->set_null(idx);
      eval_flags.set(idx);
    }
  } else {
    ObString tz_str_s = expr.args_[1]->get_vector(ctx)->get_string(0);
    ObString tz_str_d = expr.args_[2]->get_vector(ctx)->get_string(0);
    ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
    for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
      if (skip.at(idx) || eval_flags.at(idx)) { continue; }
      if (arg_vec->is_null(idx)) {
        res_vec->set_null(idx);
        eval_flags.set(idx);
        continue;
      }
      IN_TYPE left_val = *reinterpret_cast<const IN_TYPE*>(arg_vec->get_payload(idx));
      int64_t res_val = left_val;
      if (OB_FAIL(calc_convert_tz_timestamp(expr, ctx, res_val, tz_str_s, tz_str_d, session))) {
        LOG_WARN("calc_timestamp_value failed", K(ret), K(tz_str_s), K(tz_str_d));
        ret = OB_SUCCESS;
        res_vec->set_null(idx);
      } else {
        if (OB_UNLIKELY(res_val < MYSQL_TIMESTAMP_MIN_VAL || res_val > MYSQL_TIMESTAMP_MAX_VAL)) {
          res_vec->set_null(idx);
        } else {
          res_vec->set_datetime(idx,res_val);
        }
      }
      eval_flags.set(idx);
    }
  }
  return ret;
}

#define DISPATCH_CONVERT_TZ_VECTOR_FUNC(FUNC, TYPE)                                                             \
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

int ObExprConvertTZ::calc_convert_tz_vector(const ObExpr &expr,
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
    if (expr.args_[1]->is_const_expr() && expr.args_[2]->is_const_expr()) {
      if (ObDateTimeTC == arg_tc) {
        DISPATCH_CONVERT_TZ_VECTOR_FUNC(convert_tz_vector_const, DateTime);
      }
    } else {
      if (ObDateTimeTC == arg_tc) {
        DISPATCH_CONVERT_TZ_VECTOR_FUNC(convert_tz_vector, DateTime);
      }
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("expr calculation failed", K(ret));
    }
  }
  return ret;
}

#undef DISPATCH_CONVERT_TZ_VECTOR_FUNC




}
}
