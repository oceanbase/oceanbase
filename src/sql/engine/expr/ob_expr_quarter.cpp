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
#include "sql/engine/expr/ob_expr_quarter.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_datum_cast.h"
namespace oceanbase {
namespace sql {
ObExprQuarter::ObExprQuarter(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_QUARTER, N_QUARTER, 1, NOT_ROW_DIMENSION)
{}
ObExprQuarter::~ObExprQuarter()
{}
int ObExprQuarter::calc_result_type1(ObExprResType& type, ObExprResType& date, common::ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  UNUSED(date);
  type.set_int();
  type.set_scale(common::DEFAULT_SCALE_FOR_INTEGER);
  type.set_precision(common::ObAccuracy ::DDL_DEFAULT_ACCURACY[common::ObIntType].precision_);
  if (ob_is_enumset_tc(date.get_type())) {
    date.set_calc_type(common::ObVarcharType);
  }
  return common::OB_SUCCESS;
}
int ObExprQuarter::calc_result1(ObObj& result, const ObObj& obj, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  int64_t quarter = 0;
  ObTime ot;
  if (OB_UNLIKELY(obj.is_null())) {
    result.set_null();
  } else if (OB_FAIL(ob_obj_to_ob_time_with_date(obj, get_timezone_info(expr_ctx.my_session_), ot, false))) {
    LOG_WARN("cast to ob time failed", K(ret), K(obj), K(expr_ctx.cast_mode_));
    if (CM_IS_WARN_ON_FAIL(expr_ctx.cast_mode_)) {
      ret = OB_SUCCESS;
    }
    result.set_null();
  } else if (ot.parts_[DT_YEAR] <= 0) {
    result.set_null();
  } else {
    quarter = ObTimeConverter::ob_time_to_int_extract(ot, ObDateUnitType::DATE_UNIT_QUARTER);
    result.set_int(quarter);
  }
  return ret;
}

int ObExprQuarter::cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  if (rt_expr.arg_cnt_ != 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("quarter expr should have one param", K(ret), K(rt_expr.arg_cnt_));
  } else if (OB_ISNULL(rt_expr.args_) || OB_ISNULL(rt_expr.args_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children of quater expr is null", K(ret), K(rt_expr.args_));
  } else {
    rt_expr.eval_func_ = &calc_quater;
  }
  return ret;
}

int ObExprQuarter::calc_quater(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum* param_datum = NULL;
  ObTime ot;
  const ObSQLSessionInfo* session = NULL;
  if (OB_ISNULL(session = ctx.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (OB_FAIL(expr.args_[0]->eval(ctx, param_datum))) {
    LOG_WARN("eval param value failed");
  } else if (OB_UNLIKELY(param_datum->is_null())) {
    expr_datum.set_null();
  } else if (OB_FAIL(ob_datum_to_ob_time_with_date(*param_datum,
                 expr.args_[0]->datum_meta_.type_,
                 get_timezone_info(session),
                 ot,
                 get_cur_time(ctx.exec_ctx_.get_physical_plan_ctx()),
                 false))) {
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
    int64_t quarter = ObTimeConverter::ob_time_to_int_extract(ot, ObDateUnitType::DATE_UNIT_QUARTER);
    expr_datum.set_int(quarter);
  }
  return ret;
}

}  // end namespace sql
}  // end namespace oceanbase
