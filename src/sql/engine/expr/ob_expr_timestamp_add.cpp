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

#include "sql/engine/expr/ob_expr_timestamp_add.h"
#include "sql/engine/expr/ob_expr_add.h"
#include "sql/engine/expr/ob_expr_mul.h"
#include "sql/engine/expr/ob_datum_cast.h"
#include "sql/engine/ob_exec_context.h"
#include "lib/ob_date_unit_type.h"
#include "lib/ob_name_def.h"
#include "lib/timezone/ob_time_convert.h"
#include "share/object/ob_obj_cast.h"
#include "sql/session/ob_sql_session_info.h"
#include "common/sql_mode/ob_sql_mode_utils.h"
#include "sql/engine/expr/ob_expr_util.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObExprTimeStampAdd::ObExprTimeStampAdd(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_TIME_STAMP_ADD, N_TIME_STAMP_ADD, 3, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprTimeStampAdd::~ObExprTimeStampAdd()
{
}


inline int ObExprTimeStampAdd::calc_result_type3(ObExprResType &type,
                                            ObExprResType &unit,
                                            ObExprResType &interval,
                                            ObExprResType &timestamp,
                                            common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;

  ObCompatibilityMode compat_mode = get_compatibility_mode();
  //not timestamp. compatible with mysql.
  type.set_varchar();
  //timestamp.set_calc_type(common::ObDateTimeType);
  type.set_length(common::ObAccuracy::MAX_ACCURACY2[compat_mode][common::ObDateTimeType].precision_
    + common::ObAccuracy::MAX_ACCURACY2[compat_mode][common::ObDateTimeType].scale_ + 1);
  type.set_collation_level(common::CS_LEVEL_IMPLICIT);
  //not connection collation. compatible with mysql.
  type.set_collation_type(common::ObCharset::get_default_collation(common::ObCharset::get_default_charset()));
  unit.set_calc_type(ObIntType);
  interval.set_calc_type(ObIntType);

  return ret;
}

void ObExprTimeStampAdd::check_reset_status(const ObCastMode cast_mode,
                                            int &ret,
                                            ObObj &result)
{
  if (OB_FAIL(ret)) {
    if (CM_IS_WARN_ON_FAIL(cast_mode)) {
      ret = OB_SUCCESS;
      result.set_null();
    }
  }
}

int ObExprTimeStampAdd::calc(const int64_t unit_value,
                             ObTime &ot,
                             const int64_t ts,
                             const ObTimeConvertCtx &cvrt_ctx,
                             int64_t interval,
                             int64_t &value)
{
  int ret = OB_SUCCESS;
  static const int64_t USECS_PER_WEEK = (USECS_PER_DAY * DAYS_PER_WEEK);
  static const int64_t USECS_PER_HOUR = (static_cast<int64_t>(USECS_PER_SEC) * SECS_PER_HOUR);
  static const int64_t USECS_PER_MIN = (USECS_PER_SEC * SECS_PER_MIN);
  static const int64_t MONTH_PER_QUARTER = 3;
  int64_t quota = -1;
  int64_t delta = 0;
  switch(unit_value) {
  case DATE_UNIT_MICROSECOND: {
    quota = 1;
    //fall through
  }
  case DATE_UNIT_SECOND: {
    quota = (-1 == quota ? USECS_PER_SEC : quota);
    //fall through
  }
  case DATE_UNIT_MINUTE: {
    quota = (-1 == quota ? USECS_PER_MIN : quota);
    //fall through
  }
  case DATE_UNIT_HOUR: {
    quota = (-1 == quota ? USECS_PER_HOUR : quota);
    //fall through
  }
  case DATE_UNIT_DAY: {
    quota = (-1 == quota ? USECS_PER_DAY : quota);
    //fall through
  }
  case DATE_UNIT_WEEK: {
    quota = (-1 == quota ? USECS_PER_WEEK : quota);
    if (ObExprMul::is_mul_out_of_range(interval, quota, delta)) {
      ret = OB_DATETIME_FUNCTION_OVERFLOW;
      LOG_WARN("timestamp value is out of range", K(ret), K(quota), K(interval), K(ts), K(delta), K(value));
    } else {
      if (ObExprAdd::is_add_out_of_range(ts, delta, value)) {
        ret = OB_DATETIME_FUNCTION_OVERFLOW;
        LOG_WARN("timestamp value is out of range", K(ret), K(quota), K(interval), K(ts), K(delta), K(value));
      }
    }
    break;
  }
  case DATE_UNIT_MONTH: {
    //select timestampadd(month, 3, "2010-08-01 11:11:11");
    //diff_month = 3
    //so, result == 2010-08-01 11:11:11 + 3 month == 2010-11-01 11:11:11
    delta = interval;
    int32_t month = static_cast<int32_t>((ot.parts_[DT_YEAR]) * (MONS_PER_YEAR) + ot.parts_[DT_MON] - 1);
    if (OB_UNLIKELY(ObExprAdd::is_add_out_of_range(month, delta, month))) {
      ret = OB_DATETIME_FUNCTION_OVERFLOW;
      LOG_WARN("timestamp value is out of range", K(ret), K(ts), K(month), K(interval));
    } else {
      ot.parts_[DT_YEAR] = month / 12;
      ot.parts_[DT_MON] = month % 12 + 1;
      int32_t days = ObTimeConverter::get_days_of_month(ot.parts_[DT_YEAR], ot.parts_[DT_MON]);
      if (ot.parts_[DT_MDAY] > days) {
        ot.parts_[DT_MDAY] = days;
      }
      ot.parts_[DT_DATE] = ObTimeConverter::ob_time_to_date(ot);
      if (OB_FAIL(ObTimeConverter::ob_time_to_datetime(ot, cvrt_ctx, value))) {
        LOG_WARN("ob time to datetime failed", K(ret));
      }
    }
    break;
  }
  case DATE_UNIT_QUARTER: {
    //select timestampadd(quarter, -2, "2010-11-01 11:11:11");
    //diff_month = -2 * MONTH_PER_QUARTER = -2 *3 = -6
    //so, result == 2010-11-01 11:11:11 + (- 6 month) == 2010-05-01 11:11:11
    int32_t month = static_cast<int32_t>((ot.parts_[DT_YEAR]) * MONS_PER_YEAR + (ot.parts_[DT_MON] - 1));
    if (OB_UNLIKELY(ObExprMul::is_mul_out_of_range(interval, MONTH_PER_QUARTER, delta))) {
      ret = OB_DATETIME_FUNCTION_OVERFLOW;
      LOG_WARN("timestamp value is out of range", K(ret), K(ts), K(month), K(interval));
    } else if (OB_UNLIKELY(ObExprAdd::is_add_out_of_range(month, delta, month))) {
      ret = OB_DATETIME_FUNCTION_OVERFLOW;
      LOG_WARN("timestamp value is out of range", K(ret), K(ts), K(month), K(interval));
    } else {
      //IMHO, no need to define 12 as  MONTH_PER_YEAR here.
      ot.parts_[DT_YEAR] = month / 12;
      ot.parts_[DT_MON] = month % 12 + 1;
      ot.parts_[DT_DATE] = ObTimeConverter::ob_time_to_date(ot);
      if (OB_FAIL(ObTimeConverter::ob_time_to_datetime(ot, cvrt_ctx, value))) {
        LOG_WARN("ob time to datetime failed", K(ret));
      }
    }
    break;
  }
  case DATE_UNIT_YEAR: {
    delta = interval;
    if (OB_UNLIKELY(ObExprAdd::is_add_out_of_range(ot.parts_[DT_YEAR], delta, ot.parts_[DT_YEAR]))) {
      ret = OB_DATETIME_FUNCTION_OVERFLOW;
      LOG_WARN("timestamp value is out of range", K(ret), K(ts), K(ot.parts_[DT_YEAR]), K(interval));
    } else {
      ot.parts_[DT_DATE] = ObTimeConverter::ob_time_to_date(ot);
      if (OB_FAIL(ObTimeConverter::ob_time_to_datetime(ot, cvrt_ctx, value))) {
        LOG_WARN("ob time to datetime failed", K(ret));
      }
    }
    break;
  }
  default: {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(unit_value));
    break;
    }
  }
  return ret;
}

int calc_timestampadd_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  const ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  ObDatum *unit_datum = NULL;
  ObDatum *interval_datum = NULL;
  ObDatum *timestamp_datum = NULL;
  ObSolidifiedVarsGetter helper(expr, ctx, ctx.exec_ctx_.get_my_session());
  ObSQLMode sql_mode = 0;
  const common::ObTimeZoneInfo *tz_info = NULL;
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else if (OB_FAIL(expr.args_[0]->eval(ctx, unit_datum)) ||
      OB_FAIL(expr.args_[1]->eval(ctx, interval_datum)) ||
      OB_FAIL(expr.args_[2]->eval(ctx, timestamp_datum))) {
    LOG_WARN("eval arg failed", K(ret), KP(unit_datum), KP(interval_datum),
              KP(timestamp_datum));
  } else if (unit_datum->is_null() || interval_datum->is_null() ||
             timestamp_datum->is_null()) {
    res_datum.set_null();
  } else if (OB_FAIL(helper.get_sql_mode(sql_mode))) {
    LOG_WARN("get sql mode failed", K(ret));
  } else if (OB_FAIL(helper.get_time_zone_info(tz_info))) {
    LOG_WARN("get tz info failed", K(ret));
  } else {
    int64_t ts = 0;
    int64_t interval_int = interval_datum->get_int();
    int64_t res = 0;
    ObTime ot;
    ObTimeConvertCtx cvrt_ctx(tz_info, false);
    char *buf = NULL;
    int64_t buf_len = OB_CAST_TO_VARCHAR_MAX_LENGTH;
    int64_t out_len = 0;
    if (OB_FAIL(ob_datum_to_ob_time_with_date(*timestamp_datum,
                expr.args_[2]->datum_meta_.type_,
                expr.args_[2]->datum_meta_.scale_,
                cvrt_ctx.tz_info_, ot,
                get_cur_time(ctx.exec_ctx_.get_physical_plan_ctx()), 0,
                expr.args_[2]->obj_meta_.has_lob_header()))) {
      LOG_WARN("cast to ob time failed", K(ret), K(*timestamp_datum));
    } else if (OB_FAIL(ObTimeConverter::ob_time_to_datetime(ot, cvrt_ctx, ts))) {
      LOG_WARN("ob time to datetime failed", K(ret));
    } else if (OB_FAIL(ObExprTimeStampAdd::calc(unit_datum->get_int(), ot, ts, cvrt_ctx,
                            interval_int, res))) {
      LOG_WARN("calc failed", K(ret), K(*unit_datum), K(ts), K(interval_int));
    } else if (OB_ISNULL(buf = expr.get_str_res_mem(ctx, buf_len))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret), K(buf_len));
    } else if (OB_FAIL(common_datetime_string(expr, ObDateTimeType, ObVarcharType,
                                              expr.args_[2]->datum_meta_.scale_, false,
                                              res, ctx, buf, buf_len, out_len))) {
      LOG_WARN("common_datetime_string failed", K(ret), K(res), K(expr));
    } else {
      res_datum.set_string(ObString(out_len, buf));
    }
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(session)) {
    ObCastMode cast_mode = CM_NONE;
    ObSQLUtils::get_default_cast_mode(session->get_stmt_type(),
                                      session->is_ignore_stmt(),
                                      sql_mode,
                                      cast_mode);
    if (CM_IS_WARN_ON_FAIL(cast_mode)) {
      ret = OB_SUCCESS;
    }
    res_datum.set_null();
  }
  return ret;
}

int ObExprTimeStampAdd::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                                ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_timestampadd_expr;
  return ret;
}

int ObExprTimeStampAdd::is_valid_for_generated_column(const ObRawExpr*expr, const common::ObIArray<ObRawExpr *> &exprs, bool &is_valid) const {
  int ret = OB_SUCCESS;
  is_valid = true;
  if (OB_UNLIKELY(exprs.count() != 3)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param num is invalid", K(ret), K(exprs.count()));
  } else if (OB_ISNULL(exprs.at(2))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the third param is invalid", K(ret), K(exprs.at(2)));
  } else if (ObTimeType == exprs.at(2)->get_result_type().get_type()) {
    is_valid = false;
  }
  return ret;
}

DEF_SET_LOCAL_SESSION_VARS(ObExprTimeStampAdd, raw_expr) {
  int ret = OB_SUCCESS;
  SET_LOCAL_SYSVAR_CAPACITY(2);
  EXPR_ADD_LOCAL_SYSVAR(share::SYS_VAR_SQL_MODE);
  EXPR_ADD_LOCAL_SYSVAR(share::SYS_VAR_TIME_ZONE);
  return ret;
}

} //namespace sql
} //namespace oceanbase
