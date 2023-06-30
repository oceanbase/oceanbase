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

#include "sql/engine/expr/ob_expr_scn_to_timestamp.h"
#include "lib/ob_name_def.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObExprScnToTimestamp::ObExprScnToTimestamp(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_SCN_TO_TIMESTAMP, N_SCN_TO_TIMESTAMP, 1, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprScnToTimestamp::~ObExprScnToTimestamp()
{
}

int ObExprScnToTimestamp::calc_result_type1(ObExprResType &type,
                                        ObExprResType &scn,
                                        common::ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = common::OB_SUCCESS;
  if (lib::is_oracle_mode()) {
    type.set_timestamp_nano();
    type.set_precision(ObAccuracy::MAX_ACCURACY[ObTimestampNanoType].precision_);
    type.set_scale(ObAccuracy::MAX_ACCURACY[ObTimestampNanoType].scale_);
    //set calc type
    scn.set_calc_type(ObNumberType);
    scn.set_scale(DEFAULT_SCALE_FOR_INTEGER);
  } else {
    type.set_datetime();
    type.set_precision(ObAccuracy::MAX_ACCURACY[ObDateTimeType].precision_);
    type.set_scale(ObAccuracy::MAX_ACCURACY[ObDateTimeType].scale_);
    //set calc type
    scn.set_calc_type(ObUInt64Type);
    type_ctx.set_cast_mode(type_ctx.get_cast_mode() & (~(CM_WARN_ON_FAIL)));
  }
  return ret;
}

int calc_scn_to_timestamp_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *usec_datum = NULL;
  ObTimeZoneInfoWrap tz_info_wrap;
  ObString sys_time_zone;
  const ObTimeZoneInfo *cur_tz_info = get_timezone_info(ctx.exec_ctx_.get_my_session());
  if (OB_FAIL(expr.args_[0]->eval(ctx, usec_datum))) {
    LOG_WARN("eval arg failed", K(ret));
  } else if (usec_datum->is_null()) {
    ret = lib::is_oracle_mode() ? common::OB_ERR_FETCH_COLUMN_NULL : common::OB_INVALID_ARGUMENT_FOR_SCN_TO_TIMESTAMP;
    LOG_WARN("null is not expected", K(ret));
  } else if (OB_ISNULL(ctx.exec_ctx_.get_my_session()) || OB_ISNULL(cur_tz_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN(" my_session_ or cur_tz_info is null", K(cur_tz_info), K(ret));
  } else if (OB_FAIL(ctx.exec_ctx_.get_my_session()->get_sys_variable(share::SYS_VAR_SYSTEM_TIME_ZONE,
                                                                      sys_time_zone))) {
    LOG_WARN("Get sys variable error", K(ret));
  } else if (OB_FAIL(tz_info_wrap.init_time_zone(sys_time_zone,
                                                 OB_INVALID_VERSION,
                                                 *(const_cast<ObTZInfoMap *>(cur_tz_info->get_tz_info_map()))))) {
        LOG_WARN("tz_info_wrap init_time_zone fail", KR(ret), K(sys_time_zone));
  } else {
    uint64_t in_value = 0;
    cur_tz_info = tz_info_wrap.get_time_zone_info();
    if (lib::is_oracle_mode()) {
      const number::ObNumber scn_nmb(usec_datum->get_number());
      if (OB_FAIL(scn_nmb.extract_valid_uint64_with_trunc(in_value))) {
        LOG_WARN("extract_valid_uint64_with_trunc failed", K(ret), K(scn_nmb));
      }
    } else {
      in_value = usec_datum->get_uint64();
    }

    if (OB_SUCC(ret)) {
      const int64_t utc_timestamp = in_value / 1000;
      int64_t dt_value = 0;
      if (OB_FAIL(ObTimeConverter::timestamp_to_datetime(utc_timestamp,
                                                         cur_tz_info,
                                                         dt_value))) {
        LOG_WARN("failed to convert timestamp to datetime", K(ret));
      } else if (OB_UNLIKELY(dt_value > DATETIME_MAX_VAL || dt_value < DATETIME_MIN_VAL)) {
        char expr_str[OB_MAX_TWO_OPERATOR_EXPR_LENGTH];
        int64_t pos = 0;
        ret = OB_OPERATE_OVERFLOW;
        pos = 0;
        databuff_printf(expr_str,
                        OB_MAX_TWO_OPERATOR_EXPR_LENGTH,
                        pos,
                        "'scn_to_timestamp(%lu)'", in_value);
        LOG_USER_ERROR(OB_OPERATE_OVERFLOW, "DATE", expr_str);
      } else if (lib::is_oracle_mode()) {
        const int32_t value_ns = in_value % 1000;
        ObOTimestampData out_value;
        out_value.time_us_ = dt_value;
        out_value.time_ctx_.tail_nsec_ = value_ns;
        res_datum.set_otimestamp_tiny(out_value);
      } else {
        res_datum.set_datetime(dt_value);
      }
    }
  }
  return ret;
}

int ObExprScnToTimestamp::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                        ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_scn_to_timestamp_expr;
  return ret;
}

}//end of namespace sql
}//end of namespace oceanbase
