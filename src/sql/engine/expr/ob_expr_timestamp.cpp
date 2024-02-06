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
#include "sql/engine/expr/ob_expr_timestamp.h"

#include "lib/ob_name_def.h"
#include "lib/timezone/ob_time_convert.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/engine/ob_exec_context.h"
#include "share/system_variable/ob_system_variable.h"

namespace oceanbase
{
using namespace common;
using namespace share;
namespace sql
{
ObExprSysTimestamp::ObExprSysTimestamp(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_SYSTIMESTAMP, N_SYSTIMESTAMP, 0, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}
ObExprSysTimestamp::~ObExprSysTimestamp()
{
}

int ObExprSysTimestamp::calc_result_type0(ObExprResType &type, ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  type.set_timestamp_tz();
  return OB_SUCCESS;
}

int ObExprSysTimestamp::eval_systimestamp(const ObExpr &expr, ObEvalCtx &ctx,
    ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  UNUSED(expr);
  int64_t utc_timestamp = 0;
  ObTimeZoneInfoWrap tz_info_wrap;
  const ObTimeZoneInfo *cur_tz_info = get_timezone_info(ctx.exec_ctx_.get_my_session());
  if (OB_ISNULL(ctx.exec_ctx_.get_physical_plan_ctx())
      || OB_ISNULL(ctx.exec_ctx_.get_my_session())
      || OB_ISNULL(cur_tz_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("phy_plan_ctx_ or my_session_ or cur_tz_info is null",
             "phy_plan_ctx", ctx.exec_ctx_.get_physical_plan_ctx(), K(cur_tz_info), K(ret));
  } else if (OB_UNLIKELY(!ctx.exec_ctx_.get_physical_plan_ctx()->has_cur_time())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("physical plan context don't have current time value");
  } else {
    utc_timestamp = ctx.exec_ctx_.get_physical_plan_ctx()->get_cur_time().get_datetime();
    ObString sys_time_zone;
    const ObString OLD_SERVER_SYSTIMEZONE("CST");
    if (OB_FAIL(ctx.exec_ctx_.get_my_session()->get_sys_variable(share::SYS_VAR_SYSTEM_TIME_ZONE,
        sys_time_zone))) {
      LOG_WARN("Get sys variable error", K(ret));
    } else if (0 == sys_time_zone.case_compare(OLD_SERVER_SYSTIMEZONE)) {
      LOG_DEBUG("sys_time_zone is CST, maybe from old server, use local value instead",
                K(sys_time_zone), "local value", ObSpecialSysVarValues::system_time_zone_str_);
      sys_time_zone = ObSpecialSysVarValues::system_time_zone_str_;
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(tz_info_wrap.init_time_zone(sys_time_zone,
          OB_INVALID_VERSION, *(const_cast<ObTZInfoMap *>(cur_tz_info->get_tz_info_map()))))) {
        LOG_WARN("tz_info_wrap init_time_zone fail", KR(ret), K(sys_time_zone));
      } else {
        cur_tz_info = tz_info_wrap.get_time_zone_info();
        LOG_DEBUG("succ to get tz_info", K(sys_time_zone), K(cur_tz_info));
      }
    }
  }

  if (OB_SUCC(ret)) {
    int64_t dt_value = 0;
    int64_t odate_value = 0;
    ObOTimestampData timestamp_value;
    if (OB_FAIL(ObTimeConverter::timestamp_to_datetime(utc_timestamp,
                                                       cur_tz_info,
                                                       dt_value))) {
      LOG_WARN("failed to convert timestamp to datetime", K(ret));
    } else if (FALSE_IT(ObTimeConverter::datetime_to_odate(dt_value, odate_value))) {
    } else if (OB_FAIL(ObTimeConverter::odate_to_otimestamp(odate_value,
                                                            cur_tz_info,
                                                            ObTimestampTZType,
                                                            timestamp_value))) {
      LOG_WARN("failed to convert timestamp to datetime", K(ret));
    } else {
      expr_datum.set_otimestamp_tz(timestamp_value);
    }
  }
  return ret;
}

int ObExprSysTimestamp::cg_expr(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr,
    ObExpr &rt_expr) const
{
  UNUSED(raw_expr);
  UNUSED(op_cg_ctx);
  rt_expr.eval_func_ = ObExprSysTimestamp::eval_systimestamp;
  return OB_SUCCESS;
}


ObExprLocalTimestamp::ObExprLocalTimestamp(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_LOCALTIMESTAMP, N_LOCALTIMESTAMP, 0, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}
ObExprLocalTimestamp::~ObExprLocalTimestamp()
{
}

int ObExprLocalTimestamp::calc_result_type0(ObExprResType &type, ObExprTypeCtx &type_ctx) const
{
  //https://docs.oracle.com/cd/B19306_01/server.102/b14200/functions079.htm
  UNUSED(type_ctx);
  type.set_timestamp_nano();
  return OB_SUCCESS;
}

int ObExprLocalTimestamp::eval_localtimestamp(const ObExpr &expr, ObEvalCtx &ctx,
    ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  UNUSED(expr);
  if (OB_ISNULL(ctx.exec_ctx_.get_physical_plan_ctx())
      || OB_ISNULL(ctx.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("phy_plan_ctx_ or my_session_ or cur_tz_info is null",
             "phy_plan_ctx", ctx.exec_ctx_.get_physical_plan_ctx(), K(ret));
  } else if (OB_UNLIKELY(!ctx.exec_ctx_.get_physical_plan_ctx()->has_cur_time())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("physical plan context don't have current time value");
  } else {
    int64_t utc_value = ctx.exec_ctx_.get_physical_plan_ctx()->get_cur_time_tardy_value();
    int64_t dt_value = 0;
    int64_t odate_value = 0;
    ObOTimestampData ts_value;
    if (OB_FAIL(ObTimeConverter::timestamp_to_datetime(utc_value,
        get_timezone_info(ctx.exec_ctx_.get_my_session()),
        dt_value))) {
      LOG_WARN("failed to convert timestamp to datetime", K(ret));
    } else if (FALSE_IT(ObTimeConverter::trunc_datetime(expr.datum_meta_.scale_, dt_value))) {
    } else if (FALSE_IT(ObTimeConverter::datetime_to_odate(dt_value, odate_value))) {
    } else if (OB_FAIL(ObTimeConverter::odate_to_otimestamp(odate_value,
        get_timezone_info(ctx.exec_ctx_.get_my_session()),
        ObTimestampNanoType,
        ts_value))) {
      LOG_WARN("failed to convert timestamp to datetime", K(ret));
    } else {
      expr_datum.set_otimestamp_tiny(ts_value);
    }
  }
  return ret;
}

int ObExprLocalTimestamp::cg_expr(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr,
    ObExpr &rt_expr) const
{
  UNUSED(raw_expr);
  UNUSED(op_cg_ctx);
  rt_expr.eval_func_ = ObExprLocalTimestamp::eval_localtimestamp;
  return OB_SUCCESS;
}


int ObExprToTimestamp::set_my_result_from_ob_time(ObExprCtx &expr_ctx, ObTime &ob_time, common::ObObj &result) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_ctx);
  ObOTimestampData result_value;
  if (OB_FAIL(ObTimeConverter::ob_time_to_otimestamp(ob_time, result_value))) {
    LOG_WARN("failed to convert ob time to datetime", K(ret));
  } else {
    result.set_otimestamp_value(get_my_target_obj_type(), result_value);
    result.set_scale(MAX_SCALE_FOR_ORACLE_TEMPORAL);
  }
  return ret;
}

int ObExprToTimestampTZ::set_my_result_from_ob_time(ObExprCtx &expr_ctx, ObTime &ob_time, common::ObObj &result) const
{
  int ret = OB_SUCCESS;
  ObTimeConvertCtx time_cvrt_ctx(get_timezone_info(expr_ctx.my_session_), false);
  ObOTimestampData result_value;
  if (OB_FAIL(ObTimeConverter::ob_time_to_utc(ObTimestampTZType, time_cvrt_ctx, ob_time))) {
    LOG_WARN("failed to convert ob_time to utc", K(ret));
  } else if (OB_FAIL(ObTimeConverter::ob_time_to_otimestamp(ob_time, result_value))) {
    LOG_WARN("failed to convert ob time to datetime", K(ret));
  } else {
    result.set_otimestamp_value(get_my_target_obj_type(), result_value);
    result.set_scale(MAX_SCALE_FOR_ORACLE_TEMPORAL);
  }
  return ret;
}


ObExprTimestamp::ObExprTimestamp(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_TIMESTAMP, N_TIMESTAMP, ONE_OR_TWO, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprTimestamp::~ObExprTimestamp()
{
}

int ObExprTimestamp::calc_result_typeN(ObExprResType &type,
                                  ObExprResType *types_array,
                                  int64_t param_num,
                                  ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(1 != param_num && 2 != param_num)) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_WARN("invalid argument count of funtion timestmap", K(ret));
  } else {
    //param will be casted to ObDatetimeType before calculation
    type.set_type(ObDateTimeType);
    types_array[0].set_calc_type(ObDateTimeType);
    if (2 == param_num) {
      types_array[1].set_calc_type(ObTimeType);
    }
    //deduce scale now.
    int16_t scale1 = MIN(types_array[0].get_scale(), MAX_SCALE_FOR_TEMPORAL);
    scale1 = (SCALE_UNKNOWN_YET == scale1) ? MAX_SCALE_FOR_TEMPORAL : scale1;
    int16_t scale2 = 0;
    if (2 == param_num) {
      scale2 = MIN(types_array[1].get_scale(), MAX_SCALE_FOR_TEMPORAL);
      scale2 = (SCALE_UNKNOWN_YET == scale2) ? MAX_SCALE_FOR_TEMPORAL : scale2;
    }
    type.set_scale(MAX(scale1, scale2));
    type_ctx.set_cast_mode(type_ctx.get_cast_mode() | CM_NULL_ON_WARN);
  }
  return ret;
}

int ObExprTimestamp::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                    ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  if (1 == rt_expr.arg_cnt_) {
    ObObjType type = rt_expr.args_[0]->datum_meta_.type_;
    CK(ObNullType == type || ObDateTimeType == type);
    if (OB_SUCC(ret)) {
      rt_expr.eval_func_ = ObExprTimestamp::calc_timestamp1;
    }
  } else if (2 == rt_expr.arg_cnt_) {
    ObObjType type1 = rt_expr.args_[0]->datum_meta_.type_;
    ObObjType type2 = rt_expr.args_[1]->datum_meta_.type_;
    CK(ObNullType == type1 || ObDateTimeType == type1);
    CK(ObNullType == type2 || ObTimeType == type2);
    if (OB_SUCC(ret)) {
      rt_expr.eval_func_ = ObExprTimestamp::calc_timestamp2;
    }
  } else {
    ret = OB_ERR_PARAM_SIZE;
    LOG_WARN("invalid argument count of funtion timestmap", K(ret));
  }
  return ret;
}

int ObExprTimestamp::calc_timestamp1(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &result)
{
  int ret = OB_SUCCESS;
  ObDatum *param = NULL;
  if (OB_FAIL(expr.eval_param_value(ctx, param))) {
    LOG_WARN("calc param failed", K(ret));
  } else if (param->is_null()) {
    result.set_null();
  } else {
    result.set_datetime(param->get_datetime());
  }
  return ret;
}

int ObExprTimestamp::calc_timestamp2(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &result)
{
  int ret = OB_SUCCESS;
  ObDatum *datetime = NULL;
  ObDatum *time = NULL;
  if (OB_FAIL(expr.eval_param_value(ctx, datetime, time))) {
    LOG_WARN("calc param failed", K(ret));
  } else if (datetime->is_null() || time->is_null()) {
    result.set_null();
  } else {
    result.set_datetime(datetime->get_datetime() + time->get_time());
  }
  return ret;
}

} //namespace sql
} //namespace oceanbase
