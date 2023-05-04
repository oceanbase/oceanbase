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

namespace oceanbase
{
using namespace common;
namespace sql
{

ObExprFromTz::ObExprFromTz(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_FROM_TZ, N_FROM_TZ, 2, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

int ObExprFromTz::calc_result_type2(ObExprResType &type,
                                    ObExprResType &input1,
                                    ObExprResType &input2,
                                    common::ObExprTypeCtx &type_ctx) const
{
  int ret = common::OB_SUCCESS;
  UNUSED(type_ctx);
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
  if (OB_SUCC(ret)) {
    // Oracle下第二个参数是nchar/nvarchar时，看起来像是没有转字符集，报错invalid tz region
    // 新引擎下保持老的实现,第二个参数只能是char/varchar, 同时转字符集
    input2.set_calc_collation_type(ObCharset::get_system_collation());
    input2.set_calc_collation_level(CS_LEVEL_IMPLICIT);
  }
  return ret;
}

int ObExprFromTz::eval_from_tz_val(const ObOTimestampData &in_ts_val,
                                   const ObString &tz_str,
                                   ObSQLSessionInfo &my_session,
                                   ObOTimestampData &out_ts_val)
{
  int ret = OB_SUCCESS;
  ObString trimed_tz_str = tz_str;
  trimed_tz_str = trimed_tz_str.trim();
  out_ts_val = in_ts_val;

  out_ts_val.time_ctx_.tz_desc_ = 0;
  out_ts_val.time_ctx_.store_tz_id_ = 0;
  int32_t offset_sec = 0;
  int ret_more = OB_SUCCESS;
  if (OB_FAIL(ObTimeConverter::str_to_offset(trimed_tz_str, offset_sec, ret_more,
                                             is_oracle_mode(), true))) {
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
    if (OB_FAIL(ObTimeConverter::otimestamp_to_ob_time(ObTimestampNanoType, out_ts_val,
                                                       NULL, ob_time))) {
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
      LOG_WARN("specified field not found in datetime or interval", K(ret), "new_ret",
                OB_ERR_FIELD_NOT_FOUND_IN_DATETIME_OR_INTERVAL);
      ret = OB_ERR_FIELD_NOT_FOUND_IN_DATETIME_OR_INTERVAL;
    }
  }
  return ret;
}

int ObExprFromTz::eval_from_tz(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObDatum *ts = NULL;
  ObDatum *tz = NULL;
  if (OB_FAIL(expr.eval_param_value(ctx, ts, tz))) {
    LOG_WARN("eval param_value failed", K(ret));
  } else if (ts->is_null() || tz->is_null()) {
    res.set_null();
  } else {
    ObOTimestampData ts_val = ts->get_otimestamp_tiny();
    const ObString &tz_str = tz->get_string();
    ObOTimestampData res_val;
    CK(OB_NOT_NULL(ctx.exec_ctx_.get_my_session()));
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(eval_from_tz_val(ts_val, tz_str,
                          *ctx.exec_ctx_.get_my_session(), res_val))) {
      LOG_WARN("eval_fromtz_val failed", K(ret), K(ts_val), K(tz_str));
    } else {
      res.set_otimestamp_tz(res_val);
    }
  }
  return ret;
}

int ObExprFromTz::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                          ObExpr &expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  CK(2 == expr.arg_cnt_);
  CK(OB_NOT_NULL(expr.args_) && OB_NOT_NULL(expr.args_[0]) && OB_NOT_NULL(expr.args_[1]));
  CK(ObTimestampNanoType == expr.args_[0]->datum_meta_.type_);
  CK(ob_is_varchar_or_char(expr.args_[1]->datum_meta_.type_,
                           expr.args_[1]->datum_meta_.cs_type_) ||
     ObNullType == expr.args_[1]->datum_meta_.type_);
  CK(ObTimestampTZType == expr.datum_meta_.type_);
  OX(expr.eval_func_ = eval_from_tz);
  return ret;
}

}
}
