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

#include "sql/engine/expr/ob_expr_timestamp_diff.h"
#include "lib/ob_date_unit_type.h"
#include "lib/ob_name_def.h"
#include "lib/timezone/ob_time_convert.h"
#include "share/object/ob_obj_cast.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_util.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObExprTimeStampDiff::ObExprTimeStampDiff(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_TIME_STAMP_DIFF, N_TIME_STAMP_DIFF, 3, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprTimeStampDiff::~ObExprTimeStampDiff()
{
}

int ObExprTimeStampDiff::calc(int64_t &res, bool &is_null, int64_t unit_value,
                              int64_t usec_left, int64_t usec_right,
                              const ObTimeZoneInfo *tz_info)
{
  int ret = OB_SUCCESS;
  is_null = false;
  if (ObTimeConverter::ZERO_DATETIME == usec_left
      || ObTimeConverter::ZERO_DATETIME == usec_right) {
    is_null = true;
  } else {
    int64_t delta = usec_right - usec_left;
    int64_t diff = 0;
    static const int64_t USECS_PER_WEEK = (USECS_PER_DAY * DAYS_PER_WEEK);
    static const int64_t USECS_PER_HOUR = (static_cast<int64_t>(USECS_PER_SEC) * SECS_PER_HOUR);
    static const int64_t USECS_PER_MIN = (USECS_PER_SEC * SECS_PER_MIN);
    switch(unit_value) {
    case DATE_UNIT_MICROSECOND: {
        res = delta;
        break;
      }
    case DATE_UNIT_SECOND: {
        res = delta / USECS_PER_SEC;
        break;
      }
    case DATE_UNIT_MINUTE: {
        res = delta / USECS_PER_MIN;
        break;
      }
    case DATE_UNIT_HOUR: {
        res = delta / USECS_PER_HOUR;
        break;
      }
    case DATE_UNIT_DAY: {
        res = delta / USECS_PER_DAY;
        break;
      }
    case DATE_UNIT_WEEK: {
        res = delta / USECS_PER_WEEK;
        break;
      }
    case DATE_UNIT_MONTH: {
        ret = calc_month_diff(usec_left, usec_right, tz_info, diff);
        res = diff;
        break;
      }
    case DATE_UNIT_QUARTER: {
        ret = calc_month_diff(usec_left, usec_right, tz_info, diff);
        res = diff / MONS_PER_QUAR;
        break;
      }
    case DATE_UNIT_YEAR: {
        ret = calc_month_diff(usec_left, usec_right, tz_info, diff);
        res = diff / MONS_PER_YEAR;
        break;
        }
    default: {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("Invalid argument", K(ret), K(usec_left), K(usec_right), K(unit_value));
        break;
      }
    }
  }
  return ret;
}

int ObExprTimeStampDiff::adjust_sub_one(const ObTime *p_min, const ObTime *p_max, int64_t &bias)
{
  int ret = OB_SUCCESS;
  bias = 0;
  if (OB_ISNULL(p_min) || OB_ISNULL(p_max)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("Pointer(s) is null!", K(ret), K(p_min), K(p_max));
  } else if (p_max->parts_[DT_MDAY] < p_min->parts_[DT_MDAY]) {
    //2010-02-10 VS 2010-02-09
    bias = -1;
  } else if (p_max->parts_[DT_MDAY] > p_min->parts_[DT_MDAY]) {
    //2010-02-10 VS 2010-02-19
    //do nothing
  } else if (p_max->parts_[DT_HOUR] < p_min->parts_[DT_HOUR]) {
    //2010-02-19 12:00 VS 2010-02-19 11:00
    bias = -1;
  } else if (p_max->parts_[DT_HOUR] > p_min->parts_[DT_HOUR]) {
    //2010-02-19 12:00 VS 2010-02-19 13:00
    //do nothing
  } else if (p_max->parts_[DT_MIN] < p_min->parts_[DT_MIN]) {
    //2010-02-19 12:10 VS 2010-02-19 12:08
    bias = -1;
  } else if (p_max->parts_[DT_MIN] > p_min->parts_[DT_MIN]) {
    //2010-02-19 12:10 VS 2010-02-19 12:15
    //do nothing
  } else if (p_max->parts_[DT_SEC] < p_min->parts_[DT_SEC]) {
    //2010-02-19 12:15:18 VS 2010-02-19 12:15:16
    bias = -1;
  } else if (p_max->parts_[DT_SEC] > p_min->parts_[DT_SEC]) {
    //2010-02-19 12:15:18 VS 2010-02-19 12:15:19
    //do nothing
  } else if (p_max->parts_[DT_USEC] < p_min->parts_[DT_USEC]) {
    bias = -1;
  }
  return ret;
}

int ObExprTimeStampDiff::calc_month_diff(const int64_t &left,
                                         const int64_t &right,
                                         const ObTimeZoneInfo *tz_info,
                                         int64_t &diff)
{
  int ret = OB_SUCCESS;
  diff = 0;
  ObTime ot_left;
  ObTime ot_right;
  if(OB_FAIL(ObTimeConverter::datetime_to_ob_time(left, tz_info, ot_left))) {
    LOG_WARN("failed to cast to ob_time", K(ret), K(left));
  } else if(OB_FAIL(ObTimeConverter::datetime_to_ob_time(right, tz_info, ot_right))) {
    LOG_WARN("failed to cast to ob_time", K(ret), K(right));
  } else {
    int64_t month_right = (ot_right.parts_[DT_YEAR]) * MONS_PER_YEAR + ot_right.parts_[DT_MON];
    int64_t month_left = (ot_left.parts_[DT_YEAR]) * MONS_PER_YEAR + ot_left.parts_[DT_MON];
    ObTime *p_min = NULL;
    ObTime *p_max = NULL;
    int sign = 1;
    if (month_right >= month_left) {
      //like 2010-01-01 VS 2011-01-01
      p_min = &ot_left;
      p_max = &ot_right;
      diff = month_right - month_left;
    } else {
      //like 2012-01-01 VS 2011-01-01
      p_min = &ot_right;
      p_max = &ot_left;
      diff = month_left - month_right;
      sign = -1;
    }
    //interval of 2011-03-01 and 2011-01-04 should be 2 not 3
    //so we have to adjust the diff to be right value
    int64_t bias = 0;
    if (0 != diff && OB_SUCC(adjust_sub_one(p_min, p_max, bias))) {
      diff += bias;
      diff *=sign;
    }
  }
  return ret;
}

int ObExprTimeStampDiff::eval_timestamp_diff(const ObExpr &expr, ObEvalCtx &ctx,
                                             ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObDatum *u = NULL;
  ObDatum *l = NULL;
  ObDatum *r = NULL;
  ObSolidifiedVarsGetter helper(expr, ctx, ctx.exec_ctx_.get_my_session());
  const common::ObTimeZoneInfo *tz_info = NULL;
  if (OB_FAIL(helper.get_time_zone_info(tz_info))) {
    LOG_WARN("get tz info failed", K(ret));
  } else if (OB_FAIL(expr.eval_param_value(ctx, u, l, r))) {
    LOG_WARN("eval arg failed", K(ret));
  } else if (u->is_null()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unit cannot be null", K(ret));
  } else if (l->is_null() || r->is_null()) {
    res.set_null();
  } else {
    int64_t res_int = 0;
    bool is_null = false;
    if (OB_FAIL(calc(res_int, is_null, u->get_int(), l->get_datetime(),
                     r->get_datetime(), tz_info))) {
      LOG_WARN("calc failed", K(ret));
    } else if (is_null) {
      res.set_null();
    } else {
      res.set_int(res_int);
    }
  }
  return ret;
}

int ObExprTimeStampDiff::cg_expr(ObExprCGCtx &ctx, const ObRawExpr &raw_expr,
                                 ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  UNUSED(raw_expr);
  CK(3 == rt_expr.arg_cnt_);
  CK(ObIntType == rt_expr.args_[0]->datum_meta_.type_ &&
     ObDateTimeType == rt_expr.args_[1]->datum_meta_.type_ &&
     ObDateTimeType == rt_expr.args_[2]->datum_meta_.type_);
  OX(rt_expr.eval_func_ = eval_timestamp_diff);
  return ret;
}

DEF_SET_LOCAL_SESSION_VARS(ObExprTimeStampDiff, raw_expr) {
  int ret = OB_SUCCESS;
  if (is_mysql_mode()) {
    SET_LOCAL_SYSVAR_CAPACITY(1);
    EXPR_ADD_LOCAL_SYSVAR(SYS_VAR_TIME_ZONE);
  }
  return ret;
}

} //namespace sql
} //namespace oceanbase
