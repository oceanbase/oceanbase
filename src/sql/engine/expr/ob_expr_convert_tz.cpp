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
#include "lib/timezone/ob_time_convert.h"
#include "lib/timezone/ob_timezone_info.h"
#include "lib/ob_name_def.h"
#include "sql/session/ob_sql_session_info.h"
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

template <typename T>
int ObExprConvertTZ::calc_convert_tz(int64_t timestamp_data,
                                    const ObString &tz_str_s,//source time zone (input2)
                                    const ObString &tz_str_d,//destination time zone (input3)
                                    ObSQLSessionInfo *session,
                                    T &result)
{
  int ret = OB_SUCCESS;
  int32_t offset_s = 0;
  int32_t offset_d = 0;
  if(OB_FAIL(ObExprConvertTZ::parse_string(timestamp_data, tz_str_s,session, false))){
    LOG_WARN("source time zone parse failed", K(ret), K(tz_str_s));
  } else if(OB_FAIL(ObExprConvertTZ::parse_string(timestamp_data, tz_str_d,session, true))){
    LOG_WARN("source time zone parse failed", K(ret), K(tz_str_d));
  }
  if (OB_FAIL(ret)) {
    ret = OB_SUCCESS;
    result.set_null();
  } else {
    int64_t res_value = timestamp_data + (static_cast<int64_t>(offset_d - offset_s)) * 1000000;
    if (OB_UNLIKELY(res_value < MYSQL_TIMESTAMP_MIN_VAL || res_value > MYSQL_TIMESTAMP_MAX_VAL)) {
      result.set_null();
    } else {
      result.set_datetime(res_value);
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
  if (OB_FAIL(ObTimeConverter::str_to_offset(tz_str, offset, ret_more,
                              false /* oracle_mode */, true /* need_check_valid */)))
  {
    LOG_WARN("direct str_to_offset failed");
    if (OB_LIKELY(OB_ERR_UNKNOWN_TIME_ZONE == ret)){
      const ObTimeZoneInfo *tz_info = NULL;
      ObTimeZoneInfoPos target_tz_pos;
      if (OB_ISNULL(tz_info = TZ_INFO(session))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tz info is null", K(ret), K(session));
      } else if (OB_FAIL(find_time_zone_pos(tz_str, *tz_info, target_tz_pos))){
        LOG_WARN("find time zone position failed", K(ret), K(ret_more));
        if (OB_ERR_UNKNOWN_TIME_ZONE == ret && OB_SUCCESS != ret_more) {
          ret = ret_more;
        }
      } else if (OB_FAIL(calc(timestamp_data, target_tz_pos, input_utc_time))) {
        LOG_WARN("calc failed", K(ret), K(timestamp_data));
      }
    } else {
      LOG_WARN("str to offset failed", K(ret));
    }
  } else {
    timestamp_data += (input_utc_time ? 1 : -1) * offset * USECS_PER_SEC;
    LOG_DEBUG("str to offset succeed", K(tz_str), K(offset));
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
  CK(3 == expr.arg_cnt_ );
  CK(OB_NOT_NULL(expr.args_) && OB_NOT_NULL(expr.args_[0])
    && OB_NOT_NULL(expr.args_[1]) && OB_NOT_NULL(expr.args_[2]));
  CK(ObDateTimeType == expr.args_[0]->datum_meta_.type_);
  CK(ObDateTimeType == expr.datum_meta_.type_);
  CK(ObVarcharType == expr.args_[1]->datum_meta_.type_);
  CK(ObVarcharType == expr.args_[2]->datum_meta_.type_);
  OX(expr.eval_func_ = ObExprConvertTZ::eval_convert_tz);
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
    if (OB_FAIL(calc_convert_tz(timestamp_data, time_zone_s->get_string(), time_zone_d->get_string(),
                                  ctx.exec_ctx_.get_my_session(), res))) {
      LOG_WARN("calc convert tz zone failed", K(ret));
    }
  }
  return ret;
}

}
}
