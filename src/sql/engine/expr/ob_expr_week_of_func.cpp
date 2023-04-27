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
#include "lib/timezone/ob_time_convert.h"
#include "share/object/ob_obj_cast.h"
#include "ob_expr_extract.h"
#include "share/ob_time_utility2.h"
#include "sql/session/ob_sql_session_info.h"
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
                         1, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
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
  if (OB_ISNULL(session = ctx.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (OB_FAIL(expr.args_[0]->eval(ctx, param_datum))) {
    LOG_WARN("eval param value failed");
  } else if (OB_UNLIKELY(param_datum->is_null())) {
    expr_datum.set_null();
  } else if (FALSE_IT(date_sql_mode.init(session->get_sql_mode()))) {
  } else if (OB_FAIL(ob_datum_to_ob_time_with_date(
                 *param_datum, expr.args_[0]->datum_meta_.type_, get_timezone_info(session),
                 ot, get_cur_time(ctx.exec_ctx_.get_physical_plan_ctx()), false, date_sql_mode,
                 expr.args_[0]->obj_meta_.has_lob_header()))) {
    LOG_WARN("cast to ob time failed", K(ret));
    uint64_t cast_mode = 0;
    ObSQLUtils::get_default_cast_mode(session->get_stmt_type(), session, cast_mode);
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

int ObExprWeekOfYear::is_valid_for_generated_column(const ObRawExpr*expr, const common::ObIArray<ObRawExpr *> &exprs, bool &is_valid) const {
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_first_param_not_time(exprs, is_valid))) {
    LOG_WARN("fail to check if first param is time", K(ret), K(exprs));
  }
  return ret;
}

ObExprWeekDay::ObExprWeekDay(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc,
                         T_FUN_SYS_WEEKDAY_OF_DATE,
                         N_WEEKDAY_OF_DATE,
                         1, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
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
  if (OB_ISNULL(session = ctx.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (OB_FAIL(expr.args_[0]->eval(ctx, param_datum))) {
    LOG_WARN("eval param value failed");
  } else if (OB_UNLIKELY(param_datum->is_null())) {
    expr_datum.set_null();
  } else if (FALSE_IT(date_sql_mode.init(session->get_sql_mode()))) {
  } else if (OB_FAIL(ob_datum_to_ob_time_with_date(
                 *param_datum, expr.args_[0]->datum_meta_.type_, get_timezone_info(session),
                 ot, get_cur_time(ctx.exec_ctx_.get_physical_plan_ctx()), false, date_sql_mode,
                 expr.args_[0]->obj_meta_.has_lob_header()))) {
    LOG_WARN("cast to ob time failed", K(ret));
    uint64_t cast_mode = 0;
    ObSQLUtils::get_default_cast_mode(session->get_stmt_type(), session, cast_mode);
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

int ObExprWeekDay::is_valid_for_generated_column(const ObRawExpr*expr, const common::ObIArray<ObRawExpr *> &exprs, bool &is_valid) const {
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_first_param_not_time(exprs, is_valid))) {
    LOG_WARN("fail to check if first param is time", K(ret), K(exprs));
  }
  return ret;
}

ObExprYearWeek::ObExprYearWeek(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc,
                         T_FUN_SYS_YEARWEEK_OF_DATE,
                         N_YEARWEEK_OF_DATE,
                         ONE_OR_TWO,
                         NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
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
  if (OB_ISNULL(session = ctx.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (OB_FAIL(expr.args_[0]->eval(ctx, param_datum))) {
    LOG_WARN("eval param value failed");
  } else if (OB_UNLIKELY(param_datum->is_null())) {
    expr_datum.set_null();
  } else if (FALSE_IT(date_sql_mode.init(session->get_sql_mode()))) {
  } else if (OB_FAIL(ob_datum_to_ob_time_with_date(
                     *param_datum, expr.args_[0]->datum_meta_.type_, get_timezone_info(session),
                     ot, get_cur_time(ctx.exec_ctx_.get_physical_plan_ctx()), false,
                     date_sql_mode, expr.args_[0]->obj_meta_.has_lob_header()))) {
    LOG_WARN("cast to ob time failed", K(ret));
    uint64_t cast_mode = 0;
    ObSQLUtils::get_default_cast_mode(session->get_stmt_type(), session, cast_mode);
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

int ObExprYearWeek::is_valid_for_generated_column(const ObRawExpr*expr, const common::ObIArray<ObRawExpr *> &exprs, bool &is_valid) const {
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_first_param_not_time(exprs, is_valid))) {
    LOG_WARN("fail to check if first param is time", K(ret), K(exprs));
  }
  return ret;
}

ObExprWeek::ObExprWeek(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc,
                         T_FUN_SYS_WEEK,
                         N_WEEK,
                         ONE_OR_TWO, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
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
  if (OB_ISNULL(session = ctx.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (OB_FAIL(expr.args_[0]->eval(ctx, param_datum))) {
    LOG_WARN("eval param value failed");
  } else if (OB_UNLIKELY(param_datum->is_null())) {
    expr_datum.set_null();
  } else if (FALSE_IT(date_sql_mode.init(session->get_sql_mode()))) {
  } else if (OB_FAIL(ob_datum_to_ob_time_with_date(
                 *param_datum, expr.args_[0]->datum_meta_.type_, get_timezone_info(session),
                 ot, get_cur_time(ctx.exec_ctx_.get_physical_plan_ctx()), false, date_sql_mode,
                 expr.args_[0]->obj_meta_.has_lob_header()))) {
    LOG_WARN("cast to ob time failed", K(ret));
    uint64_t cast_mode = 0;
    ObSQLUtils::get_default_cast_mode(session->get_stmt_type(), session, cast_mode);
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

int ObExprWeek::is_valid_for_generated_column(const ObRawExpr*expr, const common::ObIArray<ObRawExpr *> &exprs, bool &is_valid) const {
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_first_param_not_time(exprs, is_valid))) {
    LOG_WARN("fail to check if first param is time", K(ret), K(exprs));
  }
  return ret;
}

}
}
