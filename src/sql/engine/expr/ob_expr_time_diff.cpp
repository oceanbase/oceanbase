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

#include "sql/engine/expr/ob_expr_time_diff.h"
#include "lib/ob_date_unit_type.h"
#include "lib/ob_name_def.h"
#include "share/object/ob_obj_cast.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_datum_cast.h"
#include "sql/engine/expr/ob_expr_util.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObExprTimeDiff::ObExprTimeDiff(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_TIME_DIFF, N_TIME_DIFF, 2, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprTimeDiff::~ObExprTimeDiff()
{
}

int ObExprTimeDiff::get_diff_value_with_ob_time(ObTime &ot1,
                                                ObTime &ot2,
                                                const ObTimeZoneInfo *tz_info,
                                                int64_t &diff)
{
  int ret = OB_SUCCESS;
  int64_t value1 = 0;
  int64_t value2 = 0;
  ObTimeConvertCtx cvrt_ctx(tz_info, true);
  if (OB_UNLIKELY(0 == ot1.parts_[DT_DATE] && 0 != ot2.parts_[DT_DATE])
      || (0 != ot1.parts_[DT_DATE] && 0 == ot2.parts_[DT_DATE])) {
       // invalid in mysql. result will be null
    ret = OB_INVALID_DATE_VALUE;
  } else {
    if (OB_UNLIKELY(0 == ot1.parts_[DT_DATE])) {
      value1 = ObTimeConverter::ob_time_to_time(ot1);
      value2 = ObTimeConverter::ob_time_to_time(ot2);
      diff = value1 - value2;
    } else if (OB_FAIL(ObTimeConverter::ob_time_to_datetime(ot1, cvrt_ctx, value1))) {
      ret = OB_INVALID_DATE_VALUE;//in order to be compatible with mysql
    } else if (OB_FAIL(ObTimeConverter::ob_time_to_datetime(ot2, cvrt_ctx, value2))) {
      ret = OB_INVALID_DATE_VALUE;//in order to be compatible with mysql
    } else {
      diff = value1 - value2;
    }
  }
  return ret;
}

int ObExprTimeDiff::get_diff_value(const ObObj &obj1,
                                   const ObObj &obj2,
                                   const ObTimeZoneInfo *tz_info,
                                   int64_t &diff)
{
  int ret = OB_INVALID_DATE_VALUE;
  ObTime ot1;
  ObTime ot2;
  if (OB_LIKELY(OB_SUCCESS == ob_obj_to_ob_time_without_date(obj1, tz_info, ot1)
             && OB_SUCCESS == ob_obj_to_ob_time_without_date(obj2, tz_info, ot2))) {
    ret = get_diff_value_with_ob_time(ot1, ot2, tz_info, diff);
  } /*else ret will be OB_INVALID_DATE_VALUE itself.
      NOT overwritten by last call for ob_obj_to_ob_time_without_date.
      by design*/
  return ret;
}

int ObExprTimeDiff::calc(ObObj &result,
                         const ObObj &left,
                         const ObObj &right,
                         ObCastCtx &cast_ctx)
{
  int ret = OB_SUCCESS;
  int64_t int64_diff = 0;
  if (OB_UNLIKELY(left.is_null() || right.is_null())) {
    result.set_null();
  } else if (OB_FAIL(get_diff_value(left, right, cast_ctx.dtc_params_.tz_info_, int64_diff))) {
    result.set_null();
  } else {
    int flag = int64_diff > 0 ? 1 : -1;
    int64_diff *= flag; //abs operation for time_overflow_trunc
    ret = ObTimeConverter::time_overflow_trunc(int64_diff);
    int64_diff = int64_diff * flag; //restore value
    result.set_time(int64_diff);
  }

  if (OB_SUCCESS != ret && CM_IS_WARN_ON_FAIL(cast_ctx.cast_mode_)) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObExprTimeDiff::cg_expr(ObExprCGCtx &op_cg_ctx,
                              const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  if (rt_expr.arg_cnt_ != 2) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("timediff expr should have two params", K(ret), K(rt_expr.arg_cnt_));
  } else if (OB_ISNULL(rt_expr.args_) || OB_ISNULL(rt_expr.args_[0])
            || OB_ISNULL(rt_expr.args_[1])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children of timediff expr is null", K(ret), K(rt_expr.args_));
  } else {
    rt_expr.eval_func_ = ObExprTimeDiff::calc_timediff;
  }
  return ret;
}

int ObExprTimeDiff::calc_timediff(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  bool calc_param_failure = false;
  ObDatum *param_datum1 = NULL;
  ObDatum *param_datum2 = NULL;
  const ObSQLSessionInfo *session = NULL;
  ObSolidifiedVarsGetter helper(expr, ctx, ctx.exec_ctx_.get_my_session());
  ObSQLMode sql_mode = 0;
  const common::ObTimeZoneInfo *tz_info = NULL;
  if (OB_ISNULL(session = ctx.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (OB_FAIL(expr.args_[0]->eval(ctx, param_datum1))) {
    calc_param_failure = true;
    LOG_WARN("eval param value failed", K(ret));
  } else if (OB_UNLIKELY(param_datum1->is_null())) {
    expr_datum.set_null();
  } else if (OB_FAIL(helper.get_sql_mode(sql_mode))) {
    LOG_WARN("get sql mode failed", K(ret));
  } else if (OB_FAIL(helper.get_time_zone_info(tz_info))) {
    LOG_WARN("get tz info failed", K(ret));
  } else {
    int64_t int64_diff = 0;
    ObTime ot1(DT_TYPE_TIME);
    ObTime ot2(DT_TYPE_TIME);
    if (OB_FAIL(ob_datum_to_ob_time_without_date(
          *param_datum1, expr.args_[0]->datum_meta_.type_, expr.args_[0]->datum_meta_.scale_,
          tz_info, ot1, expr.args_[0]->obj_meta_.has_lob_header()))) {
      LOG_WARN("cast the first param failed", K(ret));
      ret = OB_INVALID_DATE_VALUE;
      expr_datum.set_null();
    } else if (OB_FAIL(expr.args_[1]->eval(ctx, param_datum2))) {
      calc_param_failure = true;
      LOG_WARN("eval param value failed", K(ret));
    } else if (param_datum2->is_null()) {
      expr_datum.set_null();
    } else if (OB_FAIL(ob_datum_to_ob_time_without_date(
                 *param_datum2, expr.args_[1]->datum_meta_.type_, expr.args_[1]->datum_meta_.scale_,
                 tz_info, ot2, expr.args_[1]->obj_meta_.has_lob_header()))) {
      LOG_WARN("cast the second param failed", K(ret));
      ret = OB_INVALID_DATE_VALUE;
      expr_datum.set_null();
    } else if (OB_FAIL(
                 get_diff_value_with_ob_time(ot1, ot2, tz_info, int64_diff))) {
      LOG_WARN("get diff value with ob time failed", K(ret));
      expr_datum.set_null();
    } else {
      int flag = int64_diff > 0 ? 1 : -1;
      int64_diff *= flag; //abs operation for time_overflow_trunc
      ret = ObTimeConverter::time_overflow_trunc(int64_diff);
      int64_diff = int64_diff * flag; //restore value
      expr_datum.set_time(int64_diff);
    }

    if (OB_SUCCESS != ret) {
      uint64_t cast_mode = 0;
      ObSQLUtils::get_default_cast_mode(session->get_stmt_type(),
                                        session->is_ignore_stmt(),
                                        sql_mode,
                                        cast_mode);
      if (!calc_param_failure && CM_IS_WARN_ON_FAIL(cast_mode)) {
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

DEF_SET_LOCAL_SESSION_VARS(ObExprTimeDiff, raw_expr) {
  int ret = OB_SUCCESS;
  if (is_mysql_mode()) {
    SET_LOCAL_SYSVAR_CAPACITY(2);
    EXPR_ADD_LOCAL_SYSVAR(share::SYS_VAR_SQL_MODE);
    EXPR_ADD_LOCAL_SYSVAR(share::SYS_VAR_TIME_ZONE);
  } else {
    SET_LOCAL_SYSVAR_CAPACITY(1);
    EXPR_ADD_LOCAL_SYSVAR(share::SYS_VAR_TIME_ZONE);
  }
  return ret;
}

} //namespace sql
} //namespace oceanbase
