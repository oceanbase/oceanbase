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

#define USING_LOG_PREFIX SQL_EXE
#include "ob_expr_week_of_func.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_datum_cast.h"

using namespace oceanbase::share;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{

ObExprWeekOfYear::ObExprWeekOfYear(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc,
                         T_FUN_SYS_WEEK_OF_YEAR,
                         N_WEEK_OF_YEAR,
                         1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}
ObExprWeekOfYear::~ObExprWeekOfYear() {}
int ObExprWeekOfYear::cg_expr(ObExprCGCtx &op_cg_ctx,
                              const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  if (rt_expr.arg_cnt_ != 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("weekofyear expr should have one param", K(ret), K(rt_expr.arg_cnt_));
  } else if (OB_ISNULL(rt_expr.args_) || OB_ISNULL(rt_expr.args_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children of weekofyear expr is null", K(ret), K(rt_expr.args_));
  } else {
    rt_expr.eval_func_ = ObExprWeekOfYear::calc_weekofyear;
    // The vectorization of other types for the expression not completed yet.
    if (ob_is_datetime_or_mysql_datetime_tc(rt_expr.args_[0]->datum_meta_.type_)
        || ob_is_date_or_mysql_date(rt_expr.args_[0]->datum_meta_.type_)) {
      rt_expr.eval_vector_func_ = ObExprWeekOfYear::calc_weekofyear_vector;
    }
  }
  return ret;
}

int ObExprWeekOfYear::calc_weekofyear(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *param_datum = NULL;
  int64_t week = 0;
  ObTime ot;
  ObDateSqlMode date_sql_mode;
  const ObSQLSessionInfo *session = NULL;
  ObSolidifiedVarsGetter helper(expr, ctx, ctx.exec_ctx_.get_my_session());
  ObSQLMode sql_mode = 0;
  const common::ObTimeZoneInfo *tz_info = NULL;
  if (OB_ISNULL(session = ctx.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (OB_FAIL(expr.args_[0]->eval(ctx, param_datum))) {
    LOG_WARN("eval param value failed");
  } else if (OB_UNLIKELY(param_datum->is_null())) {
    expr_datum.set_null();
  } else if (OB_FAIL(helper.get_sql_mode(sql_mode))) {
    LOG_WARN("get sql mode failed", K(ret));
  } else if (OB_FAIL(helper.get_time_zone_info(tz_info))) {
    LOG_WARN("get tz info failed", K(ret));
  } else if (FALSE_IT(date_sql_mode.init(sql_mode))) {
  } else if (OB_FAIL(ob_datum_to_ob_time_with_date(
                 *param_datum, expr.args_[0]->datum_meta_.type_,
                 expr.args_[0]->datum_meta_.scale_,
                 tz_info,
                 ot, get_cur_time(ctx.exec_ctx_.get_physical_plan_ctx()), date_sql_mode,
                 expr.args_[0]->obj_meta_.has_lob_header()))) {
    LOG_WARN("cast to ob time failed", K(ret));
    uint64_t cast_mode = 0;
    ObSQLUtils::get_default_cast_mode(session->get_stmt_type(),
                                      session->is_ignore_stmt(),
                                      sql_mode,
                                      cast_mode);
    if (CM_IS_WARN_ON_FAIL(cast_mode)) {
      ret = OB_SUCCESS;
    }
    expr_datum.set_null();
  } else if (ot.parts_[DT_YEAR] <= 0) {
    expr_datum.set_null();
  } else {
    week = ObTimeConverter::ob_time_to_week(ot, DT_WEEK_GE_4_BEGIN);
    expr_datum.set_int(week);
  }
  return ret;
}

DEF_SET_LOCAL_SESSION_VARS(ObExprWeekOfYear, raw_expr) {
  int ret = OB_SUCCESS;
  SET_LOCAL_SYSVAR_CAPACITY(2);
  EXPR_ADD_LOCAL_SYSVAR(share::SYS_VAR_SQL_MODE);
  EXPR_ADD_LOCAL_SYSVAR(share::SYS_VAR_TIME_ZONE);
  return ret;
}

ObExprWeekDay::ObExprWeekDay(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc,
                         T_FUN_SYS_WEEKDAY_OF_DATE,
                         N_WEEKDAY_OF_DATE,
                         1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}
ObExprWeekDay::~ObExprWeekDay() {}

int ObExprWeekDay::cg_expr(ObExprCGCtx &op_cg_ctx,
                              const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  if (rt_expr.arg_cnt_ != 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("weekday expr should have one param", K(ret), K(rt_expr.arg_cnt_));
  } else if (OB_ISNULL(rt_expr.args_) || OB_ISNULL(rt_expr.args_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children of weekday expr is null", K(ret), K(rt_expr.args_));
  } else {
    rt_expr.eval_func_ = ObExprWeekDay::calc_weekday;
  }
  return ret;
}

int ObExprWeekDay::calc_weekday(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *param_datum = NULL;
  ObTime ot;
  ObDateSqlMode date_sql_mode;
  const ObSQLSessionInfo *session = NULL;
  ObSolidifiedVarsGetter helper(expr, ctx, ctx.exec_ctx_.get_my_session());
  ObSQLMode sql_mode = 0;
  const common::ObTimeZoneInfo *tz_info = NULL;
  if (OB_ISNULL(session = ctx.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (OB_FAIL(expr.args_[0]->eval(ctx, param_datum))) {
    LOG_WARN("eval param value failed");
  } else if (OB_UNLIKELY(param_datum->is_null())) {
    expr_datum.set_null();
  } else if (OB_FAIL(helper.get_sql_mode(sql_mode))) {
    LOG_WARN("get sql mode failed", K(ret));
  } else if (OB_FAIL(helper.get_time_zone_info(tz_info))) {
    LOG_WARN("get tz info failed", K(ret));
  } else if (FALSE_IT(date_sql_mode.init(sql_mode))) {
  } else if (OB_FAIL(ob_datum_to_ob_time_with_date(
                 *param_datum, expr.args_[0]->datum_meta_.type_,
                 expr.args_[0]->datum_meta_.scale_,
                 tz_info,
                 ot, get_cur_time(ctx.exec_ctx_.get_physical_plan_ctx()), date_sql_mode,
                 expr.args_[0]->obj_meta_.has_lob_header()))) {
    LOG_WARN("cast to ob time failed", K(ret));
    uint64_t cast_mode = 0;
    ObSQLUtils::get_default_cast_mode(session->get_stmt_type(),
                                      session->is_ignore_stmt(),
                                      sql_mode,
                                      cast_mode);
    if (CM_IS_WARN_ON_FAIL(cast_mode)) {
      ret = OB_SUCCESS;
    }
    expr_datum.set_null();
  } else if (ot.parts_[DT_YEAR] <= 0) {
    expr_datum.set_null();
  } else {
    int64_t weekday = ot.parts_[DT_WDAY] - 1;
    expr_datum.set_int(weekday);
  }
  return ret;
}

DEF_SET_LOCAL_SESSION_VARS(ObExprWeekDay, raw_expr) {
  int ret = OB_SUCCESS;
  SET_LOCAL_SYSVAR_CAPACITY(2);
  EXPR_ADD_LOCAL_SYSVAR(share::SYS_VAR_SQL_MODE);
  EXPR_ADD_LOCAL_SYSVAR(share::SYS_VAR_TIME_ZONE);
  return ret;
}

ObExprYearWeek::ObExprYearWeek(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc,
                         T_FUN_SYS_YEARWEEK_OF_DATE,
                         N_YEARWEEK_OF_DATE,
                         ONE_OR_TWO,
                         VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}
ObExprYearWeek::~ObExprYearWeek() {}

int ObExprYearWeek::calc_result_typeN(ObExprResType& type,
                                      ObExprResType* types_stack,
                                      int64_t param_num,
                                      ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(types_stack)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null types",K(ret));
  } else if (OB_UNLIKELY(param_num > 2)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param num is not correct", K(param_num));
  } else {
    if (2 == param_num) {
      types_stack[1].set_calc_type(common::ObIntType);
    }
    if (ob_is_enumset_tc(types_stack[0].get_type())) {
      types_stack[0].set_calc_type(common::ObVarcharType);
    }
  }
  if(OB_SUCC(ret)) {
    type.set_int();
    type.set_scale(common::DEFAULT_SCALE_FOR_INTEGER);
    type.set_precision(common::ObAccuracy::
        DDL_DEFAULT_ACCURACY[common::ObIntType].precision_);
  }
  return ret;
}

//参考 https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html 了解更多mode的含义
template <typename T>
static int ob_expr_calc_yearweek(const int64_t &mode_value,
                                 const ObTime &ot,
                                 int64_t &week_value,
                                 ObDTMode mode,
                                 T& result)
{
  int ret = OB_SUCCESS;
  int64_t week = 0;
  int64_t year = 0;
  int32_t delta = 0;
  int64_t flag = 0;
  //在mode为负值时，和mysql结果保持一致。
  flag = (mode_value % 8 >= 0 ? mode_value % 8 : 8 + (mode_value % 8));
  switch (flag) {
  case 0:       //每年的第一周以星期天开始，每周以星期天开始
    week = ObTimeConverter::ob_time_to_week(
        ot, DT_WEEK_SUN_BEGIN + mode, delta);
    break;
  case 2:
    week = ObTimeConverter::ob_time_to_week(
        ot, DT_WEEK_SUN_BEGIN, delta);
    break;
  case 1:       //每年的第一周需要该周大于三天，每周以星期一开始
    week = ObTimeConverter::ob_time_to_week(
        ot, DT_WEEK_GE_4_BEGIN + mode, delta);
    break;
  case 3:
    week = ObTimeConverter::ob_time_to_week(
        ot, DT_WEEK_GE_4_BEGIN, delta);
    break;
  case 4:       //每年的第一周需要该周大于三天，每周以星期天开始
    week = ObTimeConverter::ob_time_to_week(
        ot, DT_WEEK_GE_4_BEGIN + DT_WEEK_SUN_BEGIN + mode, delta);
    break;
  case 6:
    week = ObTimeConverter::ob_time_to_week(
        ot, DT_WEEK_GE_4_BEGIN + DT_WEEK_SUN_BEGIN, delta);
    break;
  case 5:       //每年的第一周需要以星期一开始，每周以星期一开始
    week = ObTimeConverter::ob_time_to_week(ot, mode, delta);
    break;
  case 7:
    week = ObTimeConverter::ob_time_to_week(ot, 0, delta);
    break;
  default:
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected flag value",K(ret),K(flag), K(mode));
    break;
  }
  if(OB_SUCC(ret)) {
    year = ot.parts_[DT_YEAR] + delta;
    week_value = week;
    result.set_int(year * 100 + week);
  }
  return ret;
}

int ObExprYearWeek::cg_expr(ObExprCGCtx &op_cg_ctx,
                              const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  if (rt_expr.arg_cnt_ != 1 && rt_expr.arg_cnt_ != 2) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("yearweek expr should have one or two params", K(ret), K(rt_expr.arg_cnt_));
  } else if (OB_ISNULL(rt_expr.args_) || OB_ISNULL(rt_expr.args_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children of yearweek expr is null", K(ret), K(rt_expr.args_));
  } else if (2 == rt_expr.arg_cnt_ && OB_ISNULL(rt_expr.args_[1])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("second child of yearweek expr is null", K(ret), K(rt_expr.args_));
  } else {
    rt_expr.eval_func_ = ObExprYearWeek::calc_yearweek;
  }
  return ret;
}

int ObExprYearWeek::calc_yearweek(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *param_datum = NULL;
  int64_t mode_value = 0;
  int64_t week = 0;
  ObTime ot;
  ObDateSqlMode date_sql_mode;
  const ObSQLSessionInfo *session = NULL;
  ObSolidifiedVarsGetter helper(expr, ctx, ctx.exec_ctx_.get_my_session());
  ObSQLMode sql_mode = 0;
  const common::ObTimeZoneInfo *tz_info = NULL;
  if (OB_ISNULL(session = ctx.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (OB_FAIL(expr.args_[0]->eval(ctx, param_datum))) {
    LOG_WARN("eval param value failed");
  } else if (OB_UNLIKELY(param_datum->is_null())) {
    expr_datum.set_null();
  } else if (OB_FAIL(helper.get_sql_mode(sql_mode))) {
    LOG_WARN("get sql mode failed", K(ret));
  } else if (OB_FAIL(helper.get_time_zone_info(tz_info))) {
    LOG_WARN("get tz info failed", K(ret));
  } else if (FALSE_IT(date_sql_mode.init(sql_mode))) {
  } else if (OB_FAIL(ob_datum_to_ob_time_with_date(
                     *param_datum, expr.args_[0]->datum_meta_.type_,
                     expr.args_[0]->datum_meta_.scale_,
                     tz_info,
                     ot, get_cur_time(ctx.exec_ctx_.get_physical_plan_ctx()),
                     date_sql_mode, expr.args_[0]->obj_meta_.has_lob_header()))) {
    LOG_WARN("cast to ob time failed", K(ret));
    uint64_t cast_mode = 0;
    ObSQLUtils::get_default_cast_mode(session->get_stmt_type(),
                                      session->is_ignore_stmt(),
                                      sql_mode, cast_mode);
    if (CM_IS_WARN_ON_FAIL(cast_mode)) {
      ret = OB_SUCCESS;
    }
    expr_datum.set_null();
  } else if (ot.parts_[DT_YEAR] <= 0) {
    expr_datum.set_null();
  } else {
    // 短路计算
    if (2 == expr.arg_cnt_) {
      ObDatum *param_datum2 = NULL;
      if (OB_FAIL(expr.args_[1]->eval(ctx, param_datum2))) {
        LOG_WARN("eval param value failed");
      } else if (OB_LIKELY(!param_datum2->is_null())) {
        mode_value = param_datum2->get_int();
      }
    }
    if (OB_SUCC(ret)) {
      ret = ob_expr_calc_yearweek(mode_value, ot, week, 0, expr_datum);
    }
  }
  return ret;
}

DEF_SET_LOCAL_SESSION_VARS(ObExprYearWeek, raw_expr) {
  int ret = OB_SUCCESS;
  SET_LOCAL_SYSVAR_CAPACITY(2);
  EXPR_ADD_LOCAL_SYSVAR(share::SYS_VAR_SQL_MODE);
  EXPR_ADD_LOCAL_SYSVAR(share::SYS_VAR_TIME_ZONE);
  return ret;
}

ObExprWeek::ObExprWeek(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc,
                         T_FUN_SYS_WEEK,
                         N_WEEK,
                         ONE_OR_TWO, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}
ObExprWeek::~ObExprWeek() {}

int ObExprWeek::calc_result_typeN(ObExprResType& type,
                                  ObExprResType* types_stack,
                                  int64_t params_count,
                                  common::ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(types_stack)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null types",K(ret));
  } else if (OB_UNLIKELY(params_count > 2)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param num is not correct", K(params_count));
  } else {
    if (2 == params_count) {
      types_stack[1].set_calc_type(common::ObIntType);
    }
    if (ob_is_enumset_tc(types_stack[0].get_type())) {
      types_stack[0].set_calc_type(common::ObVarcharType);
    }
  }
  if(OB_SUCC(ret)) {
    type.set_int();
    type.set_scale(common::DEFAULT_SCALE_FOR_INTEGER);
    type.set_precision(common::ObAccuracy::
        DDL_DEFAULT_ACCURACY[common::ObIntType].precision_);
  }
  return ret;
}

int ObExprWeek::cg_expr(ObExprCGCtx &op_cg_ctx,
                              const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  if (rt_expr.arg_cnt_ != 1 && rt_expr.arg_cnt_ != 2) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("week expr should have one or two params", K(ret), K(rt_expr.arg_cnt_));
  } else if (OB_ISNULL(rt_expr.args_) || OB_ISNULL(rt_expr.args_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children of week expr is null", K(ret), K(rt_expr.args_));
  } else if (2 == rt_expr.arg_cnt_ && OB_ISNULL(rt_expr.args_[1])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("second child of week expr is null", K(ret), K(rt_expr.args_));
  } else {
    rt_expr.eval_func_ = ObExprWeek::calc_week;
    // The vectorization of other types for the expression not completed yet.
    if ((ob_is_date_or_mysql_date(rt_expr.args_[0]->datum_meta_.type_)
         || ob_is_datetime_or_mysql_datetime_tc(rt_expr.args_[0]->datum_meta_.type_))
        && ((2 == rt_expr.arg_cnt_ && ObIntType == rt_expr.args_[1]->datum_meta_.type_)
            || 1 == rt_expr.arg_cnt_)) {
      rt_expr.eval_vector_func_ = ObExprWeek::calc_week_vector;
    }
  }
  return ret;
}

int ObExprWeek::calc_week(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *param_datum = NULL;
  int64_t mode_value = 0;
  int64_t week = 0;
  ObTime ot;
  ObDateSqlMode date_sql_mode;
  const ObSQLSessionInfo *session = NULL;
  ObSolidifiedVarsGetter helper(expr, ctx, ctx.exec_ctx_.get_my_session());
  ObSQLMode sql_mode = 0;
  const common::ObTimeZoneInfo *tz_info = NULL;
  if (OB_ISNULL(session = ctx.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (OB_FAIL(expr.args_[0]->eval(ctx, param_datum))) {
    LOG_WARN("eval param value failed");
  } else if (OB_UNLIKELY(param_datum->is_null())) {
    expr_datum.set_null();
  } else if (OB_FAIL(helper.get_sql_mode(sql_mode))) {
    LOG_WARN("get sql mode failed", K(ret));
  } else if (OB_FAIL(helper.get_time_zone_info(tz_info))) {
    LOG_WARN("get tz info failed", K(ret));
  } else if (FALSE_IT(date_sql_mode.init(sql_mode))) {
  } else if (OB_FAIL(ob_datum_to_ob_time_with_date(
                 *param_datum, expr.args_[0]->datum_meta_.type_,
                 expr.args_[0]->datum_meta_.scale_,
                 tz_info,
                 ot, get_cur_time(ctx.exec_ctx_.get_physical_plan_ctx()), date_sql_mode,
                 expr.args_[0]->obj_meta_.has_lob_header()))) {
    LOG_WARN("cast to ob time failed", K(ret));
    uint64_t cast_mode = 0;
    ObSQLUtils::get_default_cast_mode(session->get_stmt_type(),
                                      session->is_ignore_stmt(),
                                      sql_mode,
                                      cast_mode);
    if (CM_IS_WARN_ON_FAIL(cast_mode)) {
      LOG_WARN("cast to ob time failed", K(ret));
      LOG_USER_WARN(OB_ERR_CAST_VARCHAR_TO_TIME);
      ret = OB_SUCCESS;
    }
    expr_datum.set_null();
  } else if (ot.parts_[DT_YEAR] <= 0) {
    expr_datum.set_null();
  } else {
    // 短路计算
    if (2 == expr.arg_cnt_) {
      ObDatum *param_datum2 = NULL;
      if (OB_FAIL(expr.args_[1]->eval(ctx, param_datum2))) {
        LOG_WARN("eval param value failed");
      } else if (OB_LIKELY(!param_datum2->is_null())) {
        mode_value = param_datum2->get_int();
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ob_expr_calc_yearweek(mode_value, ot, week, DT_WEEK_ZERO_BEGIN, expr_datum))) {
        LOG_WARN("cal yearweek failed", K(ret), K(mode_value), K(ot));
      } else {
        expr_datum.set_int(week);
      }
    }
  }
  return ret;
}

DEF_SET_LOCAL_SESSION_VARS(ObExprWeek, raw_expr) {
  int ret = OB_SUCCESS;
  SET_LOCAL_SYSVAR_CAPACITY(2);
  EXPR_ADD_LOCAL_SYSVAR(share::SYS_VAR_SQL_MODE);
  EXPR_ADD_LOCAL_SYSVAR(share::SYS_VAR_TIME_ZONE);
  return ret;
}

#define EPOCH_WDAY    4       // 1970-1-1 is thursday.
#define BATCH_CALC_WITH_MODE(BODY) {                        \
  if (OB_LIKELY(no_skip_no_null)) {                                                      \
    for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) { \
      BODY;                                                                       \
    }                                                                             \
  } else {                                                                        \
    for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) { \
      if (skip.at(idx) || eval_flags.at(idx)) {                                   \
        continue;                                                                 \
      } else if (arg_vec->is_null(idx) || mode_vec->is_null(idx)) {               \
        res_vec->set_null(idx);                                                   \
        eval_flags.set(idx);                                                      \
        continue;                                                                 \
      }                                                                           \
      BODY;                                                                       \
    }                                                                             \
  }                                                                               \
}

#define BATCH_CALC_WITHOUT_MODE(BODY) {                \
  if (OB_LIKELY(no_skip_no_null)) {                                                      \
    for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) { \
      BODY;                                                                       \
    }                                                                             \
  } else {                                                                        \
    for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) { \
      if (skip.at(idx) || eval_flags.at(idx)) {                                   \
        continue;                                                                 \
      } else if (arg_vec->is_null(idx)) {                                         \
        res_vec->set_null(idx);                                                   \
        eval_flags.set(idx);                                                      \
        continue;                                                                 \
      }                                                                           \
      BODY;                                                                       \
    }                                                                             \
  }                                                                               \
}

template <typename ArgVec, typename ResVec, typename IN_TYPE>
int vector_weekofyear(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  ArgVec *arg_vec = static_cast<ArgVec *>(expr.args_[0]->get_vector(ctx));
  ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  int64_t tz_offset = 0;
  const common::ObTimeZoneInfo *tz_info = NULL;
  ObSolidifiedVarsGetter helper(expr, ctx, ctx.exec_ctx_.get_my_session());
  if (OB_FAIL(helper.get_time_zone_info(tz_info))) {
    LOG_WARN("get tz info failed", K(ret));
  } else if (OB_UNLIKELY(eval_flags.is_all_true(bound.start(), bound.end()))) {
  } else {
    const ObTimeZoneInfo *local_tz_info = (ObTimestampType == expr.args_[0]->datum_meta_.type_) ? tz_info : NULL;
    if (OB_FAIL(get_tz_offset(local_tz_info, tz_offset))) {
      LOG_WARN("get tz_info offset fail", K(ret));
    } else {
      DateType date = 0;
      DateType dt_yday = 0;
      YearType year = 0;
      UsecType usec = 0;
      WeekType week = 0;
      int8_t delta = 0;
      bool no_skip_no_null = bound.get_all_rows_active() && !arg_vec->has_null()
                             && eval_flags.accumulate_bit_cnt(bound) == 0;
      BATCH_CALC_WITHOUT_MODE({
        IN_TYPE in_val = *reinterpret_cast<const IN_TYPE*>(arg_vec->get_payload(idx));
        if (OB_FAIL(ObTimeConverter::parse_date_usec<IN_TYPE>(in_val, tz_offset, oceanbase::lib::is_oracle_mode(), date, usec))) {
          LOG_WARN("get date and usec from vec failed", K(ret));
        } else if (OB_UNLIKELY(ObTimeConverter::ZERO_DATE == date)) {
          res_vec->set_null(idx);
        } else {
          DateType wday = WDAY_OFFSET[date % DAYS_PER_WEEK][EPOCH_WDAY];
          ObTimeConverter::days_to_year_ydays(date, year, dt_yday);
          if (year <= 0) {
            res_vec->set_null(idx);
          } else {
            ObTimeConverter::to_week(DT_WEEK_GE_4_BEGIN, year, dt_yday, wday, week, delta);
            res_vec->set_int(idx, week);
          }
        }
        eval_flags.set(idx);
      });
    }
  }
  return ret;
}
#define DISPATCH_WEEK_OF_YEAR_TYPE(TYPE)\
if (VEC_FIXED == arg_format && VEC_FIXED == res_format) {\
  ret = vector_weekofyear<CONCAT(TYPE,FixedVec), IntegerFixedVec, CONCAT(TYPE,Type)>(expr, ctx, skip, bound);\
} else if (VEC_UNIFORM == arg_format && VEC_FIXED == res_format) {\
  ret = vector_weekofyear<CONCAT(TYPE,UniVec), IntegerFixedVec, CONCAT(TYPE,Type)>(expr, ctx, skip, bound);\
} else if (VEC_UNIFORM_CONST == arg_format && VEC_FIXED == res_format) {\
  ret = vector_weekofyear<CONCAT(TYPE,UniCVec), IntegerFixedVec, CONCAT(TYPE,Type)>(expr, ctx, skip, bound);\
} else if (VEC_FIXED == arg_format && VEC_UNIFORM == res_format) {\
  ret = vector_weekofyear<CONCAT(TYPE,FixedVec), IntegerUniVec, CONCAT(TYPE,Type)>(expr, ctx, skip, bound);\
} else if (VEC_UNIFORM == arg_format && VEC_UNIFORM == res_format) {\
  ret = vector_weekofyear<CONCAT(TYPE,UniVec), IntegerUniVec, CONCAT(TYPE,Type)>(expr, ctx, skip, bound);\
} else if (VEC_UNIFORM_CONST == arg_format && VEC_UNIFORM == res_format) {\
  ret = vector_weekofyear<CONCAT(TYPE,UniCVec), IntegerUniVec, CONCAT(TYPE,Type)>(expr, ctx, skip, bound);\
} else if (VEC_FIXED == arg_format && VEC_FIXED == res_format) {\
  ret = vector_weekofyear<CONCAT(TYPE,FixedVec), IntegerFixedVec, CONCAT(TYPE,Type)>(expr, ctx, skip, bound);\
} else if (VEC_UNIFORM == arg_format && VEC_FIXED == res_format) {\
  ret = vector_weekofyear<CONCAT(TYPE,UniVec), IntegerFixedVec, CONCAT(TYPE,Type)>(expr, ctx, skip, bound);\
} else if (VEC_UNIFORM_CONST == arg_format && VEC_FIXED == res_format) {\
  ret = vector_weekofyear<CONCAT(TYPE,UniCVec), IntegerFixedVec, CONCAT(TYPE,Type)>(expr, ctx, skip, bound);\
} else {\
  ret = vector_weekofyear<ObVectorBase, ObVectorBase, CONCAT(TYPE,Type)>(expr, ctx, skip, bound);\
}
int ObExprWeekOfYear::calc_weekofyear_vector(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  const ObSQLSessionInfo *session = NULL;
  if (OB_ISNULL(session = ctx.exec_ctx_.get_my_session())) {
    ret = OB_NOT_INIT;
    LOG_WARN("session is null", K(ret), K(session));
  } else if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("fail to eval date_format param", K(ret));
  } else {
    VectorFormat arg_format = expr.args_[0]->get_format(ctx);
    VectorFormat res_format = expr.get_format(ctx);
    ObObjTypeClass arg_tc = ob_obj_type_class(expr.args_[0]->datum_meta_.type_);
    if (ObMySQLDateTC == ob_obj_type_class(expr.args_[0]->datum_meta_.type_)) {
      DISPATCH_WEEK_OF_YEAR_TYPE(MySQLDate);
    } else if (ObMySQLDateTimeTC == ob_obj_type_class(expr.args_[0]->datum_meta_.type_)) {
      DISPATCH_WEEK_OF_YEAR_TYPE(MySQLDateTime);
    } else if (ObDateTC == ob_obj_type_class(expr.args_[0]->datum_meta_.type_)) {
      DISPATCH_WEEK_OF_YEAR_TYPE(Date);
    } else if (ObDateTimeTC == ob_obj_type_class(expr.args_[0]->datum_meta_.type_)) {
      DISPATCH_WEEK_OF_YEAR_TYPE(DateTime);
    }

    if (OB_FAIL(ret)) {
      LOG_WARN("expr calculation failed", K(ret));
    }
  }
  return ret;
}
#undef DISPATCH_WEEK_OF_YEAR_TYPE

OB_INLINE int ObExprWeek::get_week_mode_value(int64_t mode_value, ObDTMode &mode)
{
  int ret = OB_SUCCESS;
  mode = DT_WEEK_ZERO_BEGIN;
  int64_t flag = (mode_value % 8 >= 0 ? mode_value % 8 : 8 + (mode_value % 8));
  switch (flag) {
    case 0:      //每年的第一周以星期天开始，每周以星期天开始
      mode += DT_WEEK_SUN_BEGIN;
      break;
    case 2:
      mode = DT_WEEK_SUN_BEGIN;
      break;
    case 1:       //每年的第一周需要该周大于三天，每周以星期一开始
      mode += DT_WEEK_GE_4_BEGIN;
      break;
    case 3:
      mode = DT_WEEK_GE_4_BEGIN;
      break;
    case 4:       //每年的第一周需要该周大于三天，每周以星期天开始
      mode += DT_WEEK_GE_4_BEGIN + DT_WEEK_SUN_BEGIN;
      break;
    case 6:
      mode = DT_WEEK_GE_4_BEGIN + DT_WEEK_SUN_BEGIN;
      break;
    case 5:       //每年的第一周需要以星期一开始，每周以星期一开始
      // mode = mode_value;
      break;
    case 7:
      mode = 0;
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected flag value",K(ret),K(flag), K(mode_value));
      break;
  }
  return ret;
}

template <typename ArgVec, typename ModeVec, typename ResVec, typename IN_TYPE>
int ObExprWeek::vector_week(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  ArgVec *arg_vec = static_cast<ArgVec *>(expr.args_[0]->get_vector(ctx));
  ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  const ObSQLSessionInfo *session = NULL;
  const common::ObTimeZoneInfo *tz_info = NULL;
  ObSolidifiedVarsGetter helper(expr, ctx, ctx.exec_ctx_.get_my_session());
  if (OB_ISNULL(session = ctx.exec_ctx_.get_my_session())) {
    ret = OB_NOT_INIT;
    LOG_WARN("session is null", K(ret), K(session));
  } else if (OB_FAIL(helper.get_time_zone_info(tz_info))) {
    LOG_WARN("get tz info failed", K(ret));
  } else if (OB_UNLIKELY(eval_flags.is_all_true(bound.start(), bound.end()))) {
  } else {
    int64_t tz_offset = 0;
    const ObTimeZoneInfo *local_tz_info = (ObTimestampType == expr.args_[0]->datum_meta_.type_) ? tz_info : NULL;
    if (OB_FAIL(get_tz_offset(local_tz_info, tz_offset))) {
      LOG_WARN("get tz_info offset fail", K(ret));
    } else {
      DateType date = 0;
      DateType dt_yday = 0;
      DateType wday = 0;
      YearType year = 0;
      UsecType usec = 0;
      WeekType week = 0;
      int8_t delta = 0;  // no use
      ObDTMode mode = 0;
      bool no_skip_no_null = bound.get_all_rows_active() && !arg_vec->has_null()
                             && eval_flags.accumulate_bit_cnt(bound) == 0;
      if (2 == expr.arg_cnt_) {
        ModeVec *mode_vec = static_cast<ModeVec *>(expr.args_[1]->get_vector(ctx));
        no_skip_no_null = no_skip_no_null && !res_vec->has_null();
        BATCH_CALC_WITH_MODE({
          IN_TYPE in_val = *reinterpret_cast<const IN_TYPE*>(arg_vec->get_payload(idx));
          if (OB_FAIL(ObTimeConverter::parse_date_usec<IN_TYPE>(in_val, tz_offset, oceanbase::lib::is_oracle_mode(), date, usec))) {
            LOG_WARN("get_date_usec_from_vec failed", K(ret), K(date), K(usec), K(tz_offset));
          } else if (OB_UNLIKELY(ObTimeConverter::ZERO_DATE == date)) {
            res_vec->set_null(idx);
          } else {
            int64_t mode_value = mode_vec->get_date(idx);
            if (OB_FAIL(ObExprWeek::get_week_mode_value(mode_value, mode))) {
              LOG_WARN("invalid mode", K(mode_value), K(mode), K(ret));
            } else {
              wday = WDAY_OFFSET[date % DAYS_PER_WEEK][EPOCH_WDAY];
              ObTimeConverter::days_to_year_ydays(date, year, dt_yday);
              if (year <= 0) {
                res_vec->set_null(idx);
              } else {
                ObTimeConverter::to_week(mode, year, dt_yday, wday, week, /*unused*/delta);
                res_vec->set_int(idx, week);
              }
            }
          }
          eval_flags.set(idx);
        });
      } else {  // 1 == expr.arg_cnt_
        BATCH_CALC_WITHOUT_MODE({
          IN_TYPE in_val = *reinterpret_cast<const IN_TYPE*>(arg_vec->get_payload(idx));
          if (OB_FAIL(ObTimeConverter::parse_date_usec<IN_TYPE>(in_val, tz_offset, oceanbase::lib::is_oracle_mode(), date, usec))) {
            LOG_WARN("get_date_usec_from_vec failed", K(ret), K(date), K(usec), K(tz_offset));
          } else if (OB_UNLIKELY(ObTimeConverter::ZERO_DATE == date)) {
            res_vec->set_null(idx);
          } else {
            DateType wday = WDAY_OFFSET[date % DAYS_PER_WEEK][EPOCH_WDAY];
            ObTimeConverter::days_to_year_ydays(date, year, dt_yday);
            if (year <= 0) {
              res_vec->set_null(idx);
            } else {
              ObTimeConverter::to_week(DT_WEEK_ZERO_BEGIN + DT_WEEK_SUN_BEGIN, year, dt_yday, wday, week, /*unused*/delta);
              res_vec->set_int(idx, week);
            }
          }
          eval_flags.set(idx);
        });
      }
    }
  }
  return ret;
}

#define DISPATCH_WEEK_EXPR_VECTOR(mode_type, TYPE)\
if (VEC_FIXED == arg_format && VEC_FIXED == res_format) {\
  ret = vector_week<CONCAT(TYPE,FixedVec), mode_type, IntegerFixedVec, CONCAT(TYPE,Type)>(expr, ctx, skip, bound);\
} else if (VEC_FIXED == arg_format && VEC_UNIFORM == res_format) {\
  ret = vector_week<CONCAT(TYPE,FixedVec), mode_type, IntegerUniVec, CONCAT(TYPE,Type)>(expr, ctx, skip, bound);\
} else if (VEC_FIXED == arg_format && VEC_UNIFORM_CONST == res_format) {\
  ret = vector_week<CONCAT(TYPE,FixedVec), mode_type, IntegerUniCVec, CONCAT(TYPE,Type)>(expr, ctx, skip, bound);\
} else if (VEC_UNIFORM == arg_format && VEC_FIXED == res_format) {\
  ret = vector_week<CONCAT(TYPE,UniVec), mode_type, IntegerFixedVec, CONCAT(TYPE,Type)>(expr, ctx, skip, bound);\
} else if (VEC_UNIFORM == arg_format && VEC_UNIFORM == res_format) {\
  ret = vector_week<CONCAT(TYPE,UniVec), mode_type, IntegerUniVec, CONCAT(TYPE,Type)>(expr, ctx, skip, bound);\
} else if (VEC_UNIFORM == arg_format && VEC_UNIFORM_CONST == res_format) {\
  ret = vector_week<CONCAT(TYPE,UniVec), mode_type, IntegerUniCVec, CONCAT(TYPE,Type)>(expr, ctx, skip, bound);\
} else if (VEC_UNIFORM_CONST == arg_format && VEC_FIXED == res_format) {\
  ret = vector_week<CONCAT(TYPE,UniCVec), mode_type, IntegerFixedVec, CONCAT(TYPE,Type)>(expr, ctx, skip, bound);\
} else if (VEC_UNIFORM_CONST == arg_format && VEC_UNIFORM == res_format) {\
  ret = vector_week<CONCAT(TYPE,UniCVec), mode_type, IntegerUniVec, CONCAT(TYPE,Type)>(expr, ctx, skip, bound);\
} else if (VEC_UNIFORM_CONST == arg_format && VEC_UNIFORM_CONST == res_format) {\
  ret = vector_week<CONCAT(TYPE,UniCVec), mode_type, IntegerUniCVec, CONCAT(TYPE,Type)>(expr, ctx, skip, bound);\
} else {\
  ret = vector_week<ObVectorBase, ObVectorBase, ObVectorBase, CONCAT(TYPE,Type)>(expr, ctx, skip, bound);\
}

#define DEF_WEEK_ARG_VECTOR(mode_type)\
if (ObMySQLDateTC == arg_tc) {\
  DISPATCH_WEEK_EXPR_VECTOR(mode_type, MySQLDate) \
} else if (ObMySQLDateTimeTC == arg_tc) {\
  DISPATCH_WEEK_EXPR_VECTOR(mode_type, MySQLDateTime)\
} else if (ObDateTC == arg_tc) {\
  DISPATCH_WEEK_EXPR_VECTOR(mode_type, Date)\
} else if (ObDateTimeTC == arg_tc) {\
  DISPATCH_WEEK_EXPR_VECTOR(mode_type, DateTime)\
}

int ObExprWeek::calc_week_vector(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const EvalBound &bound)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("fail to eval date_format param", K(ret));
  } else if (2 == expr.arg_cnt_) {
    if(OB_FAIL(expr.args_[1]->eval_vector(ctx, skip, bound))) {
      LOG_WARN("fail to eval date_format param", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    VectorFormat arg_format = expr.args_[0]->get_format(ctx);
    VectorFormat res_format = expr.get_format(ctx);
    ObObjTypeClass arg_tc = ob_obj_type_class(expr.args_[0]->datum_meta_.type_);
    VectorFormat mode_format;
    if (2 == expr.arg_cnt_) { // 短路计算
      mode_format = expr.args_[1]->get_format(ctx);
    }
    if (1 == expr.arg_cnt_) {
      DEF_WEEK_ARG_VECTOR(ObVectorBase)
    } else if (2 == expr.arg_cnt_) {
      if (VEC_UNIFORM == mode_format) {
        DEF_WEEK_ARG_VECTOR(IntegerUniVec)
      } else if (VEC_FIXED == mode_format) {
        DEF_WEEK_ARG_VECTOR(IntegerFixedVec)
      } else if (VEC_UNIFORM_CONST == mode_format) {
        DEF_WEEK_ARG_VECTOR(IntegerUniCVec)
      } else {
        ret = vector_week<ObVectorBase, ObVectorBase, ObVectorBase, DateTimeType>(expr, ctx, skip, bound);
      }
    } else {
      ret = vector_week<ObVectorBase, ObVectorBase, ObVectorBase, DateTimeType>(expr, ctx, skip, bound);
    }

    if (OB_FAIL(ret)) {
      LOG_WARN("expr calculation failed", K(ret));
    }
  }
  return ret;
}

#undef DEF_WEEK_ARG_VECTOR
#undef DISPATCH_WEEK_EXPR_VECTOR
#undef EPOCH_WDAY
#undef BATCH_CALC_WITH_MODE
#undef BATCH_CALC_WITHOUT_MODE

}
}
