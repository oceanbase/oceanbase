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
#include "sql/engine/expr/ob_expr_cur_time.h"
#include "lib/ob_name_def.h"
#include "lib/timezone/ob_time_convert.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/session/ob_sql_session_info.h"
#include "share/system_variable/ob_system_variable.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
using namespace common;
using namespace share;
namespace sql
{
ObExprUtcTimestamp::ObExprUtcTimestamp(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_UTC_TIMESTAMP, N_UTC_TIMESTAMP, 0, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}
ObExprUtcTimestamp::~ObExprUtcTimestamp()
{
}

int ObExprUtcTimestamp::calc_result_type0(ObExprResType &type, ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  type.set_datetime();
  type.set_result_flag(NOT_NULL_FLAG);
  if (type.get_scale() < MIN_SCALE_FOR_TEMPORAL) {
    type.set_scale(MIN_SCALE_FOR_TEMPORAL);
  }
  type.set_precision(ObAccuracy::MAX_ACCURACY2[lib::is_oracle_mode()][type.get_type()].get_precision());
  return OB_SUCCESS;
}

int ObExprUtcTimestamp::eval_utc_timestamp(const ObExpr &expr, ObEvalCtx &ctx,
    ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx.exec_ctx_.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr_ctx.phy_plan_ctx_ is null", K(ret));
  } else if (OB_UNLIKELY(!ctx.exec_ctx_.get_physical_plan_ctx()->has_cur_time())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("physical plan context don't have current time value");
  } else {
    int64_t ts_value = ctx.exec_ctx_.get_physical_plan_ctx()->get_cur_time().get_timestamp();
    ObTimeConverter::trunc_datetime(expr.datum_meta_.scale_, ts_value);
    expr_datum.set_datetime(ts_value);
  }
  return ret;
}

int ObExprUtcTimestamp::cg_expr(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr,
    ObExpr &rt_expr) const
{
  UNUSED(raw_expr);
  UNUSED(op_cg_ctx);
  rt_expr.eval_func_ = ObExprUtcTimestamp::eval_utc_timestamp;
  return OB_SUCCESS;
}

ObExprUtcTime::ObExprUtcTime(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_UTC_TIME, N_UTC_TIME, 0, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}
ObExprUtcTime::~ObExprUtcTime()
{
}

int ObExprUtcTime::calc_result_type0(ObExprResType &type, ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  type.set_time();
  type.set_result_flag(NOT_NULL_FLAG);
  if (type.get_scale() < MIN_SCALE_FOR_TEMPORAL) {
    type.set_scale(MIN_SCALE_FOR_TEMPORAL);
  }
  type.set_precision(ObAccuracy::MAX_ACCURACY2[lib::is_oracle_mode()][type.get_type()].get_precision());
  return OB_SUCCESS;
}

int ObExprUtcTime::cg_expr(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr,
    ObExpr &rt_expr) const
{
  UNUSED(raw_expr);
  UNUSED(op_cg_ctx);
  rt_expr.eval_func_ = ObExprUtcTime::eval_utc_time;
  return OB_SUCCESS;
}

int ObExprUtcTime::eval_utc_time(const ObExpr &expr, ObEvalCtx &ctx,
    ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx.exec_ctx_.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr_ctx.phy_plan_ctx_ is null", K(ret));
  } else if (OB_UNLIKELY(!ctx.exec_ctx_.get_physical_plan_ctx()->has_cur_time())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("physical plan context don't have current time value");
  } else {
    int64_t ts_value = ctx.exec_ctx_.get_physical_plan_ctx()->get_cur_time().get_timestamp();
    int64_t t_value = 0;
    if (OB_FAIL(ObTimeConverter::datetime_to_time(ts_value,  NULL /* tz_info */, t_value))) {
      LOG_WARN("failed to convert datetime to time", K(ret));
    } else {
      ObTimeConverter::trunc_datetime(expr.datum_meta_.scale_, t_value);
      expr_datum.set_time(t_value);
    }
  }
  return ret;
}

ObExprUtcDate::ObExprUtcDate(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_UTC_DATE, N_UTC_DATE, 0, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}
ObExprUtcDate::~ObExprUtcDate()
{
}

int ObExprUtcDate::calc_result_type0(ObExprResType &type, ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  type.set_date();
  type.set_result_flag(NOT_NULL_FLAG);
  type.set_precision(ObAccuracy::MAX_ACCURACY2[lib::is_oracle_mode()][type.get_type()].get_precision());
  type.set_scale(0);
  return OB_SUCCESS;
}

int ObExprUtcDate::cg_expr(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr,
    ObExpr &rt_expr) const
{
  UNUSED(raw_expr);
  UNUSED(op_cg_ctx);
  rt_expr.eval_func_ = ObExprUtcDate::eval_utc_date;
  return OB_SUCCESS;
}

int ObExprUtcDate::eval_utc_date(const ObExpr &expr, ObEvalCtx &ctx,
    ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  UNUSED(expr);
  if (OB_ISNULL(ctx.exec_ctx_.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr_ctx.phy_plan_ctx_ is null", K(ret));
  } else if (OB_UNLIKELY(!ctx.exec_ctx_.get_physical_plan_ctx()->has_cur_time())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("physical plan context don't have current time value");
  } else {
    int64_t ts_value = ctx.exec_ctx_.get_physical_plan_ctx()->get_cur_time().get_timestamp();
    int32_t d_value = 0;
    if (OB_FAIL(ObTimeConverter::datetime_to_date(ts_value, NULL /* tz_info */, d_value))) {
      LOG_WARN("failed to convert datetime to date", K(ret));
    } else {
      expr_datum.set_date(d_value);
    }
  }
  return ret;
}

ObExprCurTimestamp::ObExprCurTimestamp(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_CUR_TIMESTAMP, N_CUR_TIMESTAMP, 0, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}
ObExprCurTimestamp::~ObExprCurTimestamp()
{
}

int ObExprCurTimestamp::calc_result_type0(ObExprResType &type, ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  if (lib::is_oracle_mode()) {
    type.set_timestamp_tz();
  } else {
    type.set_datetime();
  }
  if (type.get_scale() < MIN_SCALE_FOR_TEMPORAL) {
    type.set_scale(MIN_SCALE_FOR_TEMPORAL);
  }
  type.set_precision(ObAccuracy::MAX_ACCURACY2[lib::is_oracle_mode()][type.get_type()].get_precision());
  return OB_SUCCESS;
}

int ObExprCurTimestamp::eval_cur_timestamp(const ObExpr &expr, ObEvalCtx &ctx,
    ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx.exec_ctx_.get_physical_plan_ctx())
      || OB_ISNULL(ctx.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("phy_plan_ctx_ my_session_ or is null",
             "phy_plan_ctx", ctx.exec_ctx_.get_physical_plan_ctx(), K(ret));
  } else if (OB_UNLIKELY(!ctx.exec_ctx_.get_physical_plan_ctx()->has_cur_time())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("physical plan context don't have current time value");
  } else {

    int64_t ts_value = 0;
    int64_t dt_value = 0;
    if (lib::is_oracle_mode()) {
      ts_value = ctx.exec_ctx_.get_physical_plan_ctx()->get_cur_time_tardy_value();
      //oracle: return a timestamp with timezone value
      ObOTimestampData ts_tz_value;
      if (OB_FAIL(ObTimeConverter::timestamp_to_datetime(ts_value,
          get_timezone_info(ctx.exec_ctx_.get_my_session()),
          dt_value))) {
        LOG_WARN("failed to convert timestamp to datetime", K(ret));
      } else if (FALSE_IT(ObTimeConverter::trunc_datetime(expr.datum_meta_.scale_, dt_value))) {
      } else if (OB_FAIL(ObTimeConverter::odate_to_otimestamp(dt_value,
          get_timezone_info(ctx.exec_ctx_.get_my_session()),
          ObTimestampTZType,
          ts_tz_value))) {
          LOG_WARN("failed to convert timestamp to datetime", K(ret));
      } else {
        expr_datum.set_otimestamp_tz(ts_tz_value);
      }
    } else {
      ts_value = ctx.exec_ctx_.get_physical_plan_ctx()->get_cur_time().get_timestamp();
      if (OB_FAIL(ObTimeConverter::timestamp_to_datetime(ts_value,
          get_timezone_info(ctx.exec_ctx_.get_my_session()),
          dt_value))) {
        LOG_WARN("failed to convert timestamp to datetime", K(ret));
      } else {
        ObTimeConverter::trunc_datetime(expr.datum_meta_.scale_, dt_value);
        //mysql: return a datetime value
        expr_datum.set_datetime(dt_value);
      }
    }
  }
  return ret;
}

int ObExprCurTimestamp::cg_expr(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr,
    ObExpr &rt_expr) const
{
  UNUSED(raw_expr);
  UNUSED(op_cg_ctx);
  rt_expr.eval_func_ = ObExprCurTimestamp::eval_cur_timestamp;
  return OB_SUCCESS;
}

ObExprSysdate::ObExprSysdate(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_SYSDATE, N_SYSDATE, 0, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}
ObExprSysdate::~ObExprSysdate()
{
}

int ObExprSysdate::calc_result_type0(ObExprResType &type, ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  type.set_datetime();
  if (type.get_scale() < MIN_SCALE_FOR_TEMPORAL) {
    type.set_scale(MIN_SCALE_FOR_TEMPORAL);
  }
  type.set_precision(ObAccuracy::MAX_ACCURACY2[lib::is_oracle_mode()][type.get_type()].get_precision());
  return OB_SUCCESS;
}

int ObExprSysdate::eval_sysdate(const ObExpr &expr, ObEvalCtx &ctx,
    ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  UNUSED(expr);
  const ObTimeZoneInfo *cur_tz_info = get_timezone_info(ctx.exec_ctx_.get_my_session());
  if (OB_ISNULL(ctx.exec_ctx_.get_physical_plan_ctx())
      || OB_ISNULL(ctx.exec_ctx_.get_my_session())
      || OB_ISNULL(cur_tz_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("phy_plan_ctx_ or my_session_ or cur_tz_info is null",
             "phy_plan_ctx", ctx.exec_ctx_.get_physical_plan_ctx(), K(cur_tz_info), K(ret));
  } else {
    int64_t utc_timestamp = 0;
    ObTimeZoneInfoWrap tz_info_wrap;
    if (lib::is_oracle_mode()) {
      if (OB_UNLIKELY(!ctx.exec_ctx_.get_physical_plan_ctx()->has_cur_time())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("physical plan context don't have time value", K(ret));
      } else {
        utc_timestamp = ctx.exec_ctx_.get_physical_plan_ctx()->get_cur_time().get_datetime();
        ObString sys_time_zone;
        const ObString OLD_SERVER_SYSTIMEZONE("CST");
        if (OB_FAIL(ctx.exec_ctx_.get_my_session()->get_sys_variable(
            share::SYS_VAR_SYSTEM_TIME_ZONE, sys_time_zone))) {
          LOG_WARN("Get sys variable error", K(ret));
        } else if (0 == sys_time_zone.case_compare(OLD_SERVER_SYSTIMEZONE)) {
          LOG_DEBUG("sys_time_zone is CST, maybe from old server, use local value instead",
                    K(sys_time_zone), "local value", ObSpecialSysVarValues::system_time_zone_str_);
          sys_time_zone = ObSpecialSysVarValues::system_time_zone_str_;
        }

        if (OB_SUCC(ret)) {
          if (OB_FAIL(tz_info_wrap.init_time_zone(sys_time_zone, OB_INVALID_VERSION,
              *(const_cast<ObTZInfoMap *>(cur_tz_info->get_tz_info_map()))))) {
            LOG_WARN("tz_info_wrap init_time_zone fail", KR(ret), K(sys_time_zone));
          } else {
            cur_tz_info = tz_info_wrap.get_time_zone_info();
            LOG_DEBUG("succ to get tz_info", K(sys_time_zone), K(cur_tz_info));
          }
        }
      }
    } else {
      utc_timestamp = ObTimeUtility::current_time();
    }

    if (OB_SUCC(ret)) {
      int64_t dt_value = 0;
      if (OB_FAIL(ObTimeConverter::timestamp_to_datetime(utc_timestamp,
                                                         cur_tz_info,
                                                         dt_value))) {
        LOG_WARN("failed to convert timestamp to datetime", K(ret));
      } else {
        ObTimeConverter::trunc_datetime(expr.datum_meta_.scale_, dt_value);
        expr_datum.set_datetime(dt_value);
      }
    }
  }
  return ret;
}

int ObExprSysdate::cg_expr(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr,
    ObExpr &rt_expr) const
{
  UNUSED(raw_expr);
  UNUSED(op_cg_ctx);
  rt_expr.eval_func_ = ObExprSysdate::eval_sysdate;
  return OB_SUCCESS;
}


ObExprCurDate::ObExprCurDate(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_CUR_DATE, N_CUR_DATE, 0, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION,
                         INTERNAL_IN_MYSQL_MODE, INTERNAL_IN_ORACLE_MODE)
{
}
ObExprCurDate::~ObExprCurDate()
{
}

int ObExprCurDate::calc_result_type0(ObExprResType &type, ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  if (lib::is_mysql_mode()) {
    type.set_date();
  } else {
    type.set_datetime();
  }
  if (type.get_scale() < MIN_SCALE_FOR_TEMPORAL) {
    type.set_scale(MIN_SCALE_FOR_TEMPORAL);
  }
  type.set_precision(ObAccuracy::MAX_ACCURACY2[lib::is_oracle_mode()][type.get_type()].get_precision());
  return OB_SUCCESS;
}

int ObExprCurDate::eval_cur_date(const ObExpr &expr, ObEvalCtx &ctx,
    ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  UNUSED(expr);
  if (OB_ISNULL(ctx.exec_ctx_.get_physical_plan_ctx())
      || OB_ISNULL(ctx.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("phy_plan_ctx_ or my_session_ is null",
             "phy_plan_ctx", ctx.exec_ctx_.get_physical_plan_ctx(), K(ret));
  } else if (OB_UNLIKELY(!ctx.exec_ctx_.get_physical_plan_ctx()->has_cur_time())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("physical plan context don't have current time value");
  } else {
    int64_t ts_value = 0;
    if (lib::is_oracle_mode()) {
      ts_value = ctx.exec_ctx_.get_physical_plan_ctx()->get_cur_time_tardy_value();
      //use mysql datetime type as oracle date type
      int64_t dt_value = 0;
      if (OB_FAIL(ObTimeConverter::timestamp_to_datetime(ts_value,
          get_timezone_info(ctx.exec_ctx_.get_my_session()),
          dt_value))) {
        LOG_WARN("failed to convert timestamp to datetime", K(ret));
      } else {
        ObTimeConverter::trunc_datetime(expr.datum_meta_.scale_, dt_value);
        expr_datum.set_datetime(dt_value);
      }
    } else {
      ts_value = ctx.exec_ctx_.get_physical_plan_ctx()->get_cur_time().get_timestamp();
      int32_t d_value = 0;
      if (OB_FAIL(ObTimeConverter::datetime_to_date(ts_value,
          get_timezone_info(ctx.exec_ctx_.get_my_session()),
          d_value))) {
        LOG_WARN("failed to convert datetime to date", K(ret));
      } else {
        expr_datum.set_date(d_value);
      }
    }
  }
  return ret;
}

int ObExprCurDate::cg_expr(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr,
    ObExpr &rt_expr) const
{
  UNUSED(raw_expr);
  UNUSED(op_cg_ctx);
  rt_expr.eval_func_ = ObExprCurDate::eval_cur_date;
  return OB_SUCCESS;
}


ObExprCurTime::ObExprCurTime(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_CUR_TIME, N_CUR_TIME, 0, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprCurTime::~ObExprCurTime()
{
}

int ObExprCurTime::calc_result_type0(ObExprResType &type, ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  type.set_time();
  if (type.get_scale() < MIN_SCALE_FOR_TEMPORAL) {
    type.set_scale(MIN_SCALE_FOR_TEMPORAL);
  }
  type.set_precision(ObAccuracy::MAX_ACCURACY2[lib::is_oracle_mode()][type.get_type()].get_precision());
  return OB_SUCCESS;
}

int ObExprCurTime::eval_cur_time(const ObExpr &expr, ObEvalCtx &ctx,
    ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  UNUSED(expr);
  if (OB_ISNULL(ctx.exec_ctx_.get_physical_plan_ctx())
      || OB_ISNULL(ctx.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("phy_plan_ctx_ or my_session_ is null",
             "phy_plan_ctx", ctx.exec_ctx_.get_physical_plan_ctx(), K(ret));
  } else if (OB_UNLIKELY(!ctx.exec_ctx_.get_physical_plan_ctx()->has_cur_time())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("physical plan context don't have current time value");
  } else {
    int64_t ts_value = ctx.exec_ctx_.get_physical_plan_ctx()->get_cur_time().get_timestamp();
    int64_t t_value = 0;
    if (OB_FAIL(ObTimeConverter::datetime_to_time(ts_value,
                                                  get_timezone_info(ctx.exec_ctx_.get_my_session()),
                                                  t_value))) {
      LOG_WARN("failed to convert datetime to time", K(ret));
    } else {
      ObTimeConverter::trunc_datetime(expr.datum_meta_.scale_, t_value);
      expr_datum.set_time(t_value);
    }
  }
  return ret;
}

int ObExprCurTime::cg_expr(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr,
    ObExpr &rt_expr) const
{
  UNUSED(raw_expr);
  UNUSED(op_cg_ctx);
  rt_expr.eval_func_ = ObExprCurTime::eval_cur_time;
  return OB_SUCCESS;
}

} //namespace sql
} //namespace oceanbase
