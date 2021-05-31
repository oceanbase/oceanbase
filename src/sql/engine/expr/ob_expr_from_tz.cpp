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
#include "sql/engine/expr/ob_expr_from_tz.h"
#include "lib/time/ob_time_utility.h"
#include "lib/ob_name_def.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"
#include "lib/timezone/ob_time_convert.h"

namespace oceanbase {
using namespace common;
namespace sql {

ObExprFromTz::ObExprFromTz(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_FROM_TZ, N_FROM_TZ, 2, NOT_ROW_DIMENSION)
{}

int ObExprFromTz::calc_result_type2(
    ObExprResType& type, ObExprResType& input1, ObExprResType& input2, common::ObExprTypeCtx& type_ctx) const
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!input1.is_timestamp_nano())) {
    ret = common::OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_OP, "TIMESTAMP", ob_obj_type_str(input1.get_type()));
    LOG_WARN("invalid type", K(input1.get_type()));
  } else if (OB_UNLIKELY(!input2.is_varchar_or_char() && !input2.is_null())) {
    ret = common::OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_OP, "CHAR", ob_obj_type_str(input2.get_type()));
    LOG_WARN("invalid type", K(input2.get_type()));
  } else {
    type.set_timestamp_tz();
    type.set_scale(ObAccuracy::MAX_ACCURACY2[ORACLE_MODE][ObTimestampTZType].scale_);
  }
  CK(OB_NOT_NULL(type_ctx.get_session()));
  if (OB_SUCC(ret) && type_ctx.get_session()->use_static_typing_engine()) {
    // If the second parameter is nchar/nvarchar type, it seems Oracle not convert charset,
    // then report "invalid tz region"
    // Static engien is consistent with non-static engine,
    // the second parameter can only be char/varchar
    input2.set_calc_collation_type(ObCharset::get_system_collation());
    input2.set_calc_collation_level(CS_LEVEL_IMPLICIT);
  }
  return ret;
}

int ObExprFromTz::calc_result2(
    common::ObObj& result, const common::ObObj& input1, const common::ObObj& input2, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr_ctx.my_session_) || OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("argument is invalid_", K(expr_ctx.my_session_), K(ret));
  } else if (OB_UNLIKELY(!input1.is_timestamp_nano() && !input1.is_null())) {
    ret = common::OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_OP, "TIMESTAMP", ob_obj_type_str(input1.get_type()));
    LOG_WARN("invalid type", K(input1.get_type()));
  } else if (OB_UNLIKELY(!input2.is_varchar_or_char() && !input2.is_null())) {
    ret = common::OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_OP, "CHAR", ob_obj_type_str(input2.get_type()));
    LOG_WARN("invalid type", K(input2.get_type()));
  } else if (OB_UNLIKELY(input1.is_null() || input2.is_null())) {
    result.set_null();
  } else {
    ObOTimestampData tmp_ot_data = input1.get_otimestamp_value();
    tmp_ot_data.time_ctx_.tz_desc_ = 0;
    tmp_ot_data.time_ctx_.store_tz_id_ = 0;

    ObString input_string = input2.get_varchar().trim();
    int32_t offset_sec = 0;
    int32_t offset_min = 0;
    int ret_more = OB_SUCCESS;
    if (OB_FAIL(ObTimeConverter::str_to_offset(input_string, offset_sec, ret_more, is_oracle_mode(), true))) {
      if (ret != OB_ERR_UNKNOWN_TIME_ZONE) {
        LOG_WARN("fail to convert str_to_offset", K(input_string), K(ret));
      }
    } else {
      tmp_ot_data.time_ctx_.store_tz_id_ = 0;
      tmp_ot_data.time_ctx_.set_offset_min(offset_sec / SECS_PER_MIN);
      tmp_ot_data.time_us_ -= (offset_sec * USECS_PER_SEC);
    }

    if (OB_ERR_UNKNOWN_TIME_ZONE == ret) {
      ObTime ob_time(DT_TYPE_ORACLE_TTZ);
      MEMCPY(ob_time.tz_name_, input_string.ptr(), input_string.length());
      ob_time.tz_name_[input_string.length()] = '\0';
      ob_time.is_tz_name_valid_ = true;

      ObTimeConvertCtx cvrt_ctx(TZ_INFO(expr_ctx.my_session_), true);
      if (OB_FAIL(ObTimeConverter::otimestamp_to_ob_time(ObTimestampNanoType, tmp_ot_data, NULL, ob_time))) {
        LOG_WARN("failed to convert otimestamp_to_ob_time", K(ret));
      } else if (OB_FAIL(ObTimeConverter::str_to_tz_offset(cvrt_ctx, ob_time))) {
        LOG_WARN("failed to convert string to tz_offset", K(ret));
      } else if (OB_FAIL(ObTimeConverter::ob_time_to_utc(ObTimestampTZType, cvrt_ctx, ob_time))) {
        LOG_WARN("failed to convert ob_time to utc", K(ret));
      } else if (OB_FAIL(ObTimeConverter::ob_time_to_otimestamp(ob_time, tmp_ot_data))) {
        LOG_WARN("failed to convert obtime to timestamp_tz", K(ret));
      } else {
        LOG_DEBUG("finish ob_time_to_otimestamp", K(ob_time), K(tmp_ot_data));
      }

      if (OB_ERR_UNEXPECTED_TZ_TRANSITION == ret) {
        LOG_WARN("specified field not found in datetime or interval",
            K(ret),
            "new_ret",
            OB_ERR_FIELD_NOT_FOUND_IN_DATETIME_OR_INTERVAL);
        ret = OB_ERR_FIELD_NOT_FOUND_IN_DATETIME_OR_INTERVAL;
      }
    }
    if (OB_SUCC(ret)) {
      LOG_DEBUG("succ to from_tz", K(input1), K(input2), K(tmp_ot_data));
      result.set_timestamp_tz(tmp_ot_data);
    }
  }
  return ret;
}

int ObExprFromTz::eval_from_tz_val(const ObOTimestampData& in_ts_val, const ObString& tz_str,
    ObSQLSessionInfo& my_session, ObOTimestampData& out_ts_val)
{
  int ret = OB_SUCCESS;
  ObString trimed_tz_str = tz_str;
  trimed_tz_str = trimed_tz_str.trim();
  out_ts_val = in_ts_val;

  out_ts_val.time_ctx_.tz_desc_ = 0;
  out_ts_val.time_ctx_.store_tz_id_ = 0;
  int32_t offset_sec = 0;
  int32_t offset_min = 0;
  int ret_more = OB_SUCCESS;
  if (OB_FAIL(ObTimeConverter::str_to_offset(trimed_tz_str, offset_sec, ret_more, is_oracle_mode(), true))) {
    if (ret != OB_ERR_UNKNOWN_TIME_ZONE) {
      LOG_WARN("fail to convert str_to_offset", K(trimed_tz_str), K(ret));
    }
  } else {
    out_ts_val.time_ctx_.store_tz_id_ = 0;
    out_ts_val.time_ctx_.set_offset_min(offset_sec / SECS_PER_MIN);
    out_ts_val.time_us_ -= (offset_sec * USECS_PER_SEC);
  }

  if (OB_ERR_UNKNOWN_TIME_ZONE == ret) {
    ObTime ob_time(DT_TYPE_ORACLE_TTZ);
    MEMCPY(ob_time.tz_name_, trimed_tz_str.ptr(), trimed_tz_str.length());
    ob_time.tz_name_[trimed_tz_str.length()] = '\0';
    ob_time.is_tz_name_valid_ = true;

    ObTimeConvertCtx cvrt_ctx(TZ_INFO(&my_session), true);
    if (OB_FAIL(ObTimeConverter::otimestamp_to_ob_time(ObTimestampNanoType, out_ts_val, NULL, ob_time))) {
      LOG_WARN("failed to convert otimestamp_to_ob_time", K(ret));
    } else if (OB_FAIL(ObTimeConverter::str_to_tz_offset(cvrt_ctx, ob_time))) {
      LOG_WARN("failed to convert string to tz_offset", K(ret));
    } else if (OB_FAIL(ObTimeConverter::ob_time_to_utc(ObTimestampTZType, cvrt_ctx, ob_time))) {
      LOG_WARN("failed to convert ob_time to utc", K(ret));
    } else if (OB_FAIL(ObTimeConverter::ob_time_to_otimestamp(ob_time, out_ts_val))) {
      LOG_WARN("failed to convert obtime to timestamp_tz", K(ret));
    } else {
      LOG_DEBUG("finish ob_time_to_otimestamp", K(ob_time), K(out_ts_val));
    }

    if (OB_ERR_UNEXPECTED_TZ_TRANSITION == ret) {
      LOG_WARN("specified field not found in datetime or interval",
          K(ret),
          "new_ret",
          OB_ERR_FIELD_NOT_FOUND_IN_DATETIME_OR_INTERVAL);
      ret = OB_ERR_FIELD_NOT_FOUND_IN_DATETIME_OR_INTERVAL;
    }
  }
  return ret;
}

int ObExprFromTz::eval_from_tz(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res)
{
  int ret = OB_SUCCESS;
  ObDatum* ts = NULL;
  ObDatum* tz = NULL;
  if (OB_FAIL(expr.eval_param_value(ctx, ts, tz))) {
    LOG_WARN("eval param_value failed", K(ret));
  } else if (ts->is_null() || tz->is_null()) {
    res.set_null();
  } else {
    ObOTimestampData ts_val = ts->get_otimestamp_tiny();
    const ObString& tz_str = tz->get_string();
    ObOTimestampData res_val;
    CK(OB_NOT_NULL(ctx.exec_ctx_.get_my_session()));
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(eval_from_tz_val(ts_val, tz_str, *ctx.exec_ctx_.get_my_session(), res_val))) {
      LOG_WARN("eval_fromtz_val failed", K(ret), K(ts_val), K(tz_str));
    } else {
      res.set_otimestamp_tz(res_val);
    }
  }
  return ret;
}

int ObExprFromTz::cg_expr(ObExprCGCtx& expr_cg_ctx, const ObRawExpr& raw_expr, ObExpr& expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  CK(2 == expr.arg_cnt_);
  CK(OB_NOT_NULL(expr.args_) && OB_NOT_NULL(expr.args_[0]) && OB_NOT_NULL(expr.args_[1]));
  CK(ObTimestampNanoType == expr.args_[0]->datum_meta_.type_);
  CK(ob_is_varchar_or_char(expr.args_[1]->datum_meta_.type_, expr.args_[1]->datum_meta_.cs_type_) ||
      ObNullType == expr.args_[1]->datum_meta_.type_);
  CK(ObTimestampTZType == expr.datum_meta_.type_);
  OX(expr.eval_func_ = eval_from_tz);
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
