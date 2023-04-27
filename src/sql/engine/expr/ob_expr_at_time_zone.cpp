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
#include "sql/engine/expr/ob_expr_at_time_zone.h"
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
ObExprAtTimeZoneBase::ObExprAtTimeZoneBase(common::ObIAllocator &alloc, ObExprOperatorType type,
                                          const char *name, int32_t param_num, int32_t dimension)
 : ObFuncExprOperator(alloc, type, name, param_num, NOT_VALID_FOR_GENERATED_COL, dimension)
{
}

int ObExprAtTimeZoneBase::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                                  ObExpr &expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  CK(2 == expr.arg_cnt_ || 1 == expr.arg_cnt_);
  CK(OB_NOT_NULL(expr.args_) && OB_NOT_NULL(expr.args_[0]));
  CK(ObTimestampTZType == expr.args_[0]->datum_meta_.type_);
  CK(ObTimestampTZType == expr.datum_meta_.type_);
  if (1 == expr.arg_cnt_) {
    OX(expr.eval_func_ = ObExprAtLocal::eval_at_local);
  } else {
    CK(OB_NOT_NULL(expr.args_[1]));
    CK(ObVarcharType == expr.args_[1]->datum_meta_.type_);
    OX(expr.eval_func_ = ObExprAtTimeZone::eval_at_time_zone);
  }
  return ret;
}

int ObExprAtTimeZoneBase::calc(ObOTimestampData &timestamp_data,
                              const ObTimeZoneInfoPos &tz_info_pos)
{
  int ret = OB_SUCCESS;
  int32_t offset_sec = 0;
  int32_t tz_id = tz_info_pos.get_tz_id();
  int32_t tran_type_id = 0;
  ObString tz_abbr_str;
  if (OB_FAIL(tz_info_pos.get_timezone_offset(USEC_TO_SEC(timestamp_data.time_us_), offset_sec,
                                              tz_abbr_str,tran_type_id))) {
    LOG_WARN("get timezone sub offset failed", K(ret));
  } else {
    LOG_DEBUG("at time zone base calc", K(timestamp_data), K(tz_id), K(tran_type_id));
    timestamp_data.time_ctx_.store_tz_id_ = 1;
    timestamp_data.time_ctx_.set_tz_id(tz_id);
    timestamp_data.time_ctx_.set_tran_type_id(tran_type_id);
  }
  return ret;
}


ObExprAtTimeZone::ObExprAtTimeZone(ObIAllocator &alloc) :
  ObExprAtTimeZoneBase(alloc, T_FUN_SYS_AT_TIME_ZONE, "at_time_zone", 2, NOT_ROW_DIMENSION)
{
}

int ObExprAtTimeZone::find_time_zone_pos(const ObString &tz_name,
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
    // need to test overlap time
  }
  return ret;
}

int ObExprAtTimeZone::calc_at_time_zone(const ObString &tz_str,
                                        ObOTimestampData &timestamp_data,
                                        ObSQLSessionInfo *session)
{
  int ret = OB_SUCCESS;
  int32_t offset = 0;
  int ret_more = 0;
  if (OB_FAIL(ObTimeConverter::str_to_offset(tz_str, offset, ret_more,
                              true /* oracle_mode */, true /* need_check_valid */))) {
    if (OB_LIKELY(OB_ERR_UNKNOWN_TIME_ZONE == ret)) {
      const ObTimeZoneInfo *tz_info = NULL;
      ObTimeZoneInfoPos target_tz_pos;
      if (OB_ISNULL(tz_info = TZ_INFO(session))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tz info is null", K(ret), K(session));
      } else if (OB_FAIL(find_time_zone_pos(tz_str, *tz_info, target_tz_pos))) {
        LOG_WARN("find time zone position failed", K(ret), K(ret_more));
        if (OB_ERR_UNKNOWN_TIME_ZONE == ret && OB_SUCCESS != ret_more) {
          ret = ret_more;
        }
      } else if (OB_FAIL(ObExprAtTimeZoneBase::calc(timestamp_data, target_tz_pos))) {
        LOG_WARN("calc failed", K(ret), K(timestamp_data));
      }
    } else {
      LOG_WARN("str to offset failed", K(ret));
    }
  } else {
    timestamp_data.time_ctx_.store_tz_id_ = 0;
    timestamp_data.time_ctx_.set_offset_min(static_cast<int32_t>(SEC_TO_MIN(offset)));
  }
  return ret;
}

int ObExprAtTimeZone::eval_at_time_zone(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObDatum *timestamp = NULL;
  ObDatum *time_zone = NULL;
  if (OB_FAIL(expr.eval_param_value(ctx, timestamp, time_zone))) {
    LOG_WARN("calc param value failed", K(ret));
  } else if (OB_UNLIKELY(timestamp->is_null() || time_zone->is_null())) {
    res.set_null();
  } else {
    ObOTimestampData timestamp_data = timestamp->get_otimestamp_tz();
    if (OB_FAIL(calc_at_time_zone(time_zone->get_string(), timestamp_data,
                                  ctx.exec_ctx_.get_my_session()))) {
      LOG_WARN("calc at time zone failed", K(ret));
    } else {
      res.set_otimestamp_tz(timestamp_data);
    }
  }
  return ret;
}

int ObExprAtTimeZone::calc_result_type2(ObExprResType &type,
                                        ObExprResType &input1,
                                        ObExprResType &input2,
                                        common::ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  const ObSQLSessionInfo *session = NULL;
  if (OB_ISNULL(session = type_ctx.get_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (OB_UNLIKELY(!input1.is_otimestamp_type() && !input1.is_null())) {
    ret = OB_ERR_INVALID_DATA_TYPE_FOR_AT_TIME_ZONE;
    LOG_WARN("invalid data type, only timestamp (with time zone) allowed", K(ret));
  // weird, time zone expect string type, but interval day to second type also supported in Oracle.
  // But it always reports "not a valid time zone" in execution.
  } else if (!input2.is_character_type() && !input2.is_interval_ds() && !input2.is_null()) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("invalid time zone type, string type expected", K(ret), K(input2));
  } else {
    type.set_timestamp_tz();
    type.set_scale(input1.get_scale());
    input1.set_calc_type(ObTimestampTZType);
    input2.set_calc_type(ObVarcharType);
    input2.set_calc_collation_type(session->get_nls_collation());
  }
  return ret;
}



ObExprAtLocal::ObExprAtLocal(ObIAllocator &alloc) :
  ObExprAtTimeZoneBase(alloc, T_FUN_SYS_AT_LOCAL, "at_local", 1, NOT_ROW_DIMENSION)
{
}

int ObExprAtLocal::calc_result_type1(ObExprResType &type,
                                        ObExprResType &input1,
                                        common::ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!input1.is_otimestamp_type() && !input1.is_null())) {
    ret = OB_ERR_INVALID_DATA_TYPE_FOR_AT_TIME_ZONE;
    LOG_WARN("invalid data type, only timestamp (with time zone) allowed", K(ret));
  } else {
    type.set_timestamp_tz();
    type.set_scale(input1.get_scale());
    input1.set_calc_type(ObTimestampTZType);
  }
  return ret;
}

int ObExprAtLocal::calc_at_local(ObSQLSessionInfo *session, ObOTimestampData &timestamp_data)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else {
    ObTimeZoneInfoWrap &tz_info_wrap = const_cast<ObTimeZoneInfoWrap&>(session->get_tz_info_wrap());
    if (tz_info_wrap.is_position_class()) {
      const ObTimeZoneInfoPos &tz_info_pos = tz_info_wrap.get_tz_info_pos();
      if (OB_FAIL(ObExprAtTimeZoneBase::calc(timestamp_data, tz_info_pos))) {
        LOG_WARN("calc faild", K(ret), K(timestamp_data));
      }
    } else {
      const ObTimeZoneInfo &tz_info = tz_info_wrap.get_tz_info_offset();
      timestamp_data.time_ctx_.store_tz_id_ = 0;
      timestamp_data.time_ctx_.set_offset_min(static_cast<int32_t>
                                              (SEC_TO_MIN(tz_info.get_offset())));
    }
  }
  return ret;
}

int ObExprAtLocal::eval_at_local(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObDatum *timestamp = NULL;
  if (OB_FAIL(expr.eval_param_value(ctx, timestamp))) {
    LOG_WARN("calc param value failed", K(ret));
  } else if (OB_UNLIKELY(timestamp->is_null())) {
    res.set_null();
  } else {
    ObOTimestampData timestamp_data = timestamp->get_otimestamp_tz();
    if (OB_FAIL(calc_at_local(ctx.exec_ctx_.get_my_session(), timestamp_data))) {
      LOG_WARN("calc at local", K(ret));
    } else {
      res.set_otimestamp_tz(timestamp_data);
    }
  }
  return ret;
}

}
}
