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

#define USING_LOG_PREFIX SQL_REWRITE
#include "sql/rewrite/ob_transform_simplify_winfunc.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"

using namespace oceanbase::sql;

int ObTransformSimplifyWinfunc::transform_one_stmt(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                                   ObDMLStmt *&stmt,
                                                   bool &trans_happened)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *sel_stmt = static_cast<ObSelectStmt *>(stmt);
  bool is_removed = false;
  bool is_simplified = false;
  UNUSED(parent_stmts);
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (!sel_stmt->is_select_stmt()) {
    // do nothing
  } else if (OB_FAIL(remove_stmt_win(sel_stmt, is_removed))) {
    LOG_WARN("failed to remove stmt win", K(ret));
  } else if (OB_FAIL(simplify_win_exprs(sel_stmt, is_simplified))) {
    LOG_WARN("failed to simplify win exprs", K(ret));
  } else {
    trans_happened = is_removed || is_simplified;
    OPT_TRACE("remove stmt win func:", is_removed);
    OPT_TRACE("simplify win exprs:", is_simplified);
  }
  if (OB_SUCC(ret) && trans_happened) {
    if (OB_FAIL(add_transform_hint(*stmt))) {
      LOG_WARN("failed to add transform hint", K(ret));
    }
  }
  return ret;
}

/* window function partition by 唯一时, 消去window function
create table t(pk int primary key, c1 int);
select max(c1) over(partition by pk) from t;
=>
select c1 from t;
*/
int ObTransformSimplifyWinfunc::remove_stmt_win(ObSelectStmt *select_stmt, bool &trans_happened)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> win_exprs;
  ObWinFunRawExpr *win_expr = NULL;
  trans_happened = false;
  if (OB_ISNULL(select_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt->get_window_func_count(); ++i) {
    bool can_be = false;
    if (OB_ISNULL(win_expr = select_stmt->get_window_func_expr(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null window func", K(ret));
    } else if (OB_FAIL(check_stmt_win_can_be_removed(select_stmt, win_expr, can_be))) {
      LOG_WARN("check stmt window func can be removed failed", K(ret));
    } else if (!can_be) {
      /*do nothing*/
    } else if (OB_FAIL(win_exprs.push_back(win_expr))) {
      LOG_WARN("failed to push back expr", K(ret));
    }
  }
  if (OB_SUCC(ret) && !win_exprs.empty()) {
    if (OB_FAIL(do_remove_stmt_win(select_stmt, win_exprs))) {
      LOG_WARN("do transform remove group by failed", K(ret));
    } else {
      trans_happened = true;
    }
  }
  return ret;
}

int ObTransformSimplifyWinfunc::check_aggr_win_can_be_removed(const ObDMLStmt *stmt,
                                                              ObRawExpr *expr,
                                                              bool &can_remove)
{
  can_remove = false;
  int ret = OB_SUCCESS;
  ObItemType func_type = T_INVALID;
  ObAggFunRawExpr *aggr = NULL;
  ObWinFunRawExpr *win_func = NULL;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (expr->is_aggr_expr()) {
    aggr = static_cast<ObAggFunRawExpr*>(expr);
    func_type = aggr->get_expr_type();
  } else if (expr->is_win_func_expr()) {
    win_func = static_cast<ObWinFunRawExpr*>(expr);
    func_type = win_func->get_func_type();
    aggr = win_func->get_agg_expr();
    func_type = NULL == aggr ? win_func->get_func_type() : aggr->get_expr_type();
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected expr", K(ret), K(expr));
  }
  if (OB_SUCC(ret)) {
    switch (func_type) {
    // aggr func
    case T_FUN_COUNT: //case when 1 or 0
    case T_FUN_MAX: //return expr
    case T_FUN_MIN:
      //case T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS_MERGE: //不进行改写
    case T_FUN_AVG: //return expr
    case T_FUN_COUNT_SUM:
    case T_FUN_SUM:
      //case T_FUN_APPROX_COUNT_DISTINCT: // return 1 or 0 //不进行改写
      //case T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS://不进行改写
    case T_FUN_GROUP_CONCAT:// return expr
      // case T_FUN_GROUP_RANK:// return 1 or 2    需要考虑order desc、nulls first、多列条件，暂不改写
      // case T_FUN_GROUP_DENSE_RANK:
      // case T_FUN_GROUP_PERCENT_RANK:// return 1 or 0
      // case T_FUN_GROUP_CUME_DIST:// return 1 or 0.5
    case T_FUN_MEDIAN: // return expr
    case T_FUN_GROUP_PERCENTILE_CONT:
    case T_FUN_GROUP_PERCENTILE_DISC:
    case T_FUN_KEEP_MAX:
    case T_FUN_KEEP_MIN:
    case T_FUN_KEEP_COUNT: // return 1 or 0
    case T_FUN_KEEP_SUM: // return expr
      // 部分数学分析函数会在改写阶段进行展开:
      // ObExpandAggregateUtils::expand_aggr_expr
      // ObExpandAggregateUtils::expand_window_aggr_expr

      // window func
    case T_WIN_FUN_ROW_NUMBER: // return 1
    case T_WIN_FUN_RANK: // return 1
    case T_WIN_FUN_DENSE_RANK: // return 1
    case T_WIN_FUN_PERCENT_RANK: // return 0
    case T_FUN_SYS_BIT_AND: // return expr or UINT64MAX
    case T_FUN_SYS_BIT_OR: // return expr or 0
    case T_FUN_SYS_BIT_XOR: { // return expr or 0
      can_remove = true;
      break;
    }
    case T_WIN_FUN_NTILE: {//return 1
      //need check invalid param: ntile(-1)
      can_remove = false;
      ObRawExpr *expr = NULL;
      int64_t bucket_num = 0;
      bool is_valid = false;
      if (OB_ISNULL(win_func) || OB_UNLIKELY(win_func->get_func_params().empty())
          || OB_ISNULL(expr = win_func->get_func_params().at(0))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected func", K(ret));
      } else if (OB_FAIL(get_param_value(stmt, expr, is_valid, bucket_num))) {
        LOG_WARN("failed to get param value", K(ret));
      } else if (!is_valid) {
        can_remove = false;
      } else if (OB_UNLIKELY(bucket_num <= 0)) {
        if (is_oracle_mode()) {
          ret = OB_DATA_OUT_OF_RANGE;
          LOG_WARN("bucket_num out of range", K(ret), K(bucket_num));
        } else {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("bucket_num is invalid", K(ret), K(bucket_num));
        }
      } else {
        can_remove = true;
      }
      break;
    }
    case T_WIN_FUN_NTH_VALUE: { // nth_value(expr,1) return expr, else return null
      //need check invalid param: nth_value(expr, -1)
      can_remove = false;
      ObRawExpr *nth_expr = NULL;
      int64_t value = 0;
      bool is_valid = false;
      if (OB_ISNULL(win_func) || OB_UNLIKELY(2 > win_func->get_func_params().count())
          || OB_ISNULL(nth_expr = win_func->get_func_params().at(1))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected func", K(ret));
      } else if (OB_FAIL(get_param_value(stmt, nth_expr, is_valid, value))) {
        LOG_WARN("failed to get param value", K(ret));
      } else if (!is_valid) {
        can_remove = false;
      } else if (OB_UNLIKELY(value <= 0)) {
        ret = OB_DATA_OUT_OF_RANGE;
        LOG_WARN("invalid argument", K(ret), K(value));
      } else {
        can_remove = true;
      }
      break;
    }
    case T_WIN_FUN_FIRST_VALUE: // return expr (respect or ignore nulls)
    case T_WIN_FUN_LAST_VALUE: { // return expr (respect or ignore nulls)
      // first_value && last_value has been converted to nth_value when resolving
      can_remove = false;
      break;
    }
    case T_WIN_FUN_CUME_DIST: { // return 1
      can_remove = true;
      break;
    }
    case T_WIN_FUN_LEAD: // return null or default value
    case T_WIN_FUN_LAG: { // return null or default value
      can_remove = false;
      ObRawExpr *expr = NULL;
      int64_t value = 0;
      bool is_valid = false;
      if (OB_ISNULL(win_func)|| OB_UNLIKELY(win_func->get_func_params().empty())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected func", K(ret));
      } else if (1 == win_func->get_func_params().count()) {
        can_remove = true;
      } else if (OB_ISNULL(expr = win_func->get_func_params().at(1))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected NULL", K(ret));
      } else if (OB_FAIL(get_param_value(stmt, expr, is_valid, value))) {
        LOG_WARN("failed to get param value", K(ret));
      } else if (!is_valid) {
        can_remove = false;
      } else if (OB_UNLIKELY(value < 0)) {
        ret = OB_ERR_ARGUMENT_OUT_OF_RANGE;
        LOG_USER_ERROR(OB_ERR_ARGUMENT_OUT_OF_RANGE, value);
        LOG_WARN("lead/lag argument is out of range", K(ret), K(value));
      } else {
        can_remove = true;
      }
      break;
    }
    case T_WIN_FUN_RATIO_TO_REPORT: { //resolver 阶段被转化为 expr/sum（expr）
      can_remove = true;
      break;
    }
    case T_WIN_FUN_SUM: //无效
    case T_WIN_FUN_MAX: //无效
    case T_WIN_FUN_AVG: //无效
    default: {
      can_remove = false;
      break;
    }
    }
  }
  return ret;
}

/** 改写aggr/win为普通的expr,如:
 *   count(x) --> case when x is not null then 1 esle 0 end
 *   count(*) --> 1
**/
int ObTransformSimplifyWinfunc::transform_aggr_win_to_common_expr(ObSelectStmt *select_stmt,
                                                                  ObRawExpr *expr,
                                                                  ObRawExpr *&new_expr)
{
  int ret = OB_SUCCESS;
  new_expr = NULL;
  ObRawExpr *param_expr = NULL;
  ObItemType func_type = T_INVALID;
  ObAggFunRawExpr *aggr = NULL;
  ObWinFunRawExpr *win_func = NULL;
  if (OB_ISNULL(select_stmt) || OB_ISNULL(expr) || OB_ISNULL(ctx_)
      || OB_ISNULL(ctx_->expr_factory_) || OB_ISNULL(ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (expr->is_aggr_expr()) {
    aggr = static_cast<ObAggFunRawExpr*>(expr);
    func_type = aggr->get_expr_type();
  } else if (expr->is_win_func_expr()) {
    win_func = static_cast<ObWinFunRawExpr*>(expr);
    func_type = win_func->get_func_type();
    aggr = win_func->get_agg_expr();
    // to fix bug: win magic 可能导致 func type 与 aggr 不一致
    func_type = NULL == aggr ? win_func->get_func_type() : aggr->get_expr_type();
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected expr", K(ret), K(expr));
  }
  if (OB_SUCC(ret)) {
    switch (func_type) {
    // aggr
    case T_FUN_MAX:// return expr
    case T_FUN_MIN:
    case T_FUN_AVG:
    case T_FUN_SUM:
    case T_FUN_GROUP_CONCAT:
    case T_FUN_MEDIAN:
    case T_FUN_KEEP_MAX:
    case T_FUN_KEEP_MIN:
    case T_FUN_KEEP_SUM:
    case T_FUN_COUNT_SUM: {
      if (OB_ISNULL(aggr) || OB_ISNULL(param_expr = aggr->get_param_expr(0))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      }
      break;
    }
    case T_FUN_GROUP_PERCENTILE_CONT:// return expr
    case T_FUN_GROUP_PERCENTILE_DISC: {
      if (OB_ISNULL(aggr) || OB_UNLIKELY(aggr->get_order_items().empty())
          || OB_ISNULL(param_expr = aggr->get_order_items().at(0).expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected func", K(ret));
      }
      break;
    }
    case T_FUN_COUNT:
    case T_FUN_KEEP_COUNT: { // return 1 or 0
      ObConstRawExpr *const_one = NULL;
      ObConstRawExpr *const_zero = NULL;
      if (OB_ISNULL(aggr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(ObTransformUtils::build_const_expr_for_count(*ctx_->expr_factory_, 1,
                                                                      const_one))) {
        LOG_WARN("failed to build const expr for count", K(ret));
      } else if (0 == aggr->get_real_param_count()) { // count(*) --> 1
        param_expr = const_one;
      } else if (OB_FAIL(ObTransformUtils::build_const_expr_for_count(*ctx_->expr_factory_, 0,
                                                                      const_zero))) {
        LOG_WARN("failed to build const expr for count", K(ret));
        // count(c1) --> case when
      } else if (OB_FAIL(ObTransformUtils::build_case_when_expr(*select_stmt,
                                                                expr->get_param_expr(0),
                                                                const_one, const_zero,
                                                                param_expr, ctx_))) {
        LOG_WARN("failed to build case when expr", K(ret));
      }
      break;
    }
    case T_WIN_FUN_ROW_NUMBER: // return int 1
    case T_WIN_FUN_RANK:
    case T_WIN_FUN_DENSE_RANK: {
      ObConstRawExpr *const_one = NULL;
      if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*ctx_->expr_factory_, ObIntType,
                                                       1, const_one))) {
        LOG_WARN("failed to build const int expr", K(ret));
      } else {
        param_expr = const_one;
      }
      break;
    }
    case T_WIN_FUN_NTILE: { // return int 1 and add constraint
      ObConstRawExpr *const_one = NULL;
      if (OB_ISNULL(win_func) || OB_UNLIKELY(win_func->get_func_params().empty())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected func", K(ret), K(win_func));
      } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*ctx_->expr_factory_, ObIntType,
                                                              1, const_one))) {
        LOG_WARN("failed to build const int expr", K(ret));
      } else if (OB_FAIL(ObTransformUtils::add_const_param_constraints(
                           win_func->get_func_params().at(0),
                           ctx_))) {
        LOG_WARN("failed to add const param constraints", K(ret));
      } else {
        param_expr = const_one;
      }
      break;
    }
    case T_WIN_FUN_CUME_DIST: { // return number 1
      ObConstRawExpr *const_one = NULL;
      if (OB_FAIL(ObRawExprUtils::build_const_number_expr(*ctx_->expr_factory_, ObNumberType,
                                                          number::ObNumber::get_positive_one(),
                                                          const_one))) {
        LOG_WARN("failed to build const number expr", K(ret));
      } else {
        param_expr = const_one;
      }
      break;
    }
    case T_WIN_FUN_PERCENT_RANK: { // return number 0
      ObConstRawExpr *const_zero = NULL;
      if (OB_FAIL(ObRawExprUtils::build_const_number_expr(*ctx_->expr_factory_, ObNumberType,
                                                          number::ObNumber::get_zero(),
                                                          const_zero))) {
        LOG_WARN("failed to build const number expr", K(ret));
      } else {
        param_expr = const_zero;
      }
      break;
    }
    case T_WIN_FUN_NTH_VALUE: { // nth_value(expr,1) return expr, else return null
      // need add constraint for nth_expr
      ObRawExpr *nth_expr = NULL;
      int64_t value = 0;
      bool is_valid = false;
      if (OB_ISNULL(win_func) || OB_UNLIKELY(2 > win_func->get_func_params().count())
          || OB_ISNULL(nth_expr = win_func->get_func_params().at(1))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected func", K(ret));
      } else if (OB_FAIL(get_param_value(select_stmt, nth_expr, is_valid, value))) {
        LOG_WARN("failed to get param value", K(ret));
      } else if (OB_UNLIKELY(!is_valid || value <= 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected func", K(ret), K(*win_func));
      } else if (OB_FAIL(ObTransformUtils::add_const_param_constraints(nth_expr, ctx_))) {
        LOG_WARN("failed to add const param constraints", K(ret));
      } else if (1 == value) { // return expr
        param_expr = win_func->get_func_params().at(0);
      } else { //return null
        ret = ObRawExprUtils::build_null_expr(*ctx_->expr_factory_, param_expr);
      }
      break;
    }
    case T_WIN_FUN_LEAD: // return null or default value
    case T_WIN_FUN_LAG: {
      ObRawExpr *expr = NULL;
      int64_t value = 1;
      bool is_valid = false;
      if (OB_ISNULL(win_func)|| OB_UNLIKELY(win_func->get_func_params().empty())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected func", K(ret));
      } else if (1 == win_func->get_func_params().count()) {
        value = 1;
      } else if (OB_ISNULL(expr = win_func->get_func_params().at(1))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected NULL", K(ret));
      } else if (OB_FAIL(get_param_value(select_stmt, expr, is_valid, value))) {
        LOG_WARN("failed to get param value", K(ret));
      } else if (OB_UNLIKELY(!is_valid || value < 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected func", K(ret), K(*win_func));
      } else if (OB_FAIL(ObTransformUtils::add_const_param_constraints(expr, ctx_))) {
        LOG_WARN("failed to add const param constraints", K(ret));
      }

      if (OB_FAIL(ret)) {
      } else if (0 == value) { // return current value
        param_expr = win_func->get_func_params().at(0);
      } else if (2 < win_func->get_func_params().count()) { // return default value
        param_expr = win_func->get_func_params().at(2);
      } else if (OB_FAIL(ObRawExprUtils::build_null_expr(*ctx_->expr_factory_, param_expr))) {
        LOG_WARN("failed to build null expr", K(ret));
      }
      break;
    }
    case T_FUN_SYS_BIT_AND:
    case T_FUN_SYS_BIT_OR:
    case T_FUN_SYS_BIT_XOR: {
      if (OB_FAIL(ObTransformUtils::transform_bit_aggr_to_common_expr(*select_stmt, aggr, ctx_, param_expr))) {
        LOG_WARN("transform bit aggr to common expr failed", KR(ret), K(aggr), K(func_type), K(*select_stmt));
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected func", K(ret), K(*expr));
      break;
    }
    }

    //尝试添加类型转化
    if (OB_FAIL(ret)) {
      /*do nothing*/
    } else if (OB_ISNULL(param_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::try_add_cast_expr_above(ctx_->expr_factory_,
                                                               ctx_->session_info_,
                                                               *param_expr,
                                                               expr->get_result_type(),
                                                               new_expr))) {
      LOG_WARN("try add cast expr above failed", K(ret));
    }
  }
  return ret;
}

int ObTransformSimplifyWinfunc::get_param_value(const ObDMLStmt *stmt,
                                                ObRawExpr *param,
                                                bool &is_valid,
                                                int64_t &value)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  ObObj obj_value;
  bool got_result = false;
  ObPhysicalPlanCtx *plan_ctx = NULL;
  const ParamStore *param_store = NULL;
  if (OB_ISNULL(stmt) || OB_ISNULL(param) || OB_ISNULL(ctx_) ||
      OB_ISNULL(ctx_->allocator_) || OB_ISNULL(ctx_->exec_ctx_) ||
      OB_ISNULL(plan_ctx = ctx_->exec_ctx_->get_physical_plan_ctx()) ||
      OB_ISNULL(param_store = &plan_ctx->get_param_store())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null", K(ret), K(stmt), K(param), K(ctx_), K(plan_ctx));
  } else if (param->is_static_scalar_const_expr() &&
             (param->get_result_type().is_integer_type() ||
              param->get_result_type().is_number())) {
    if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(ctx_->exec_ctx_,
                                                          param, obj_value,
                                                          got_result,
                                                          *ctx_->allocator_))) {
      LOG_WARN("failed to calc const or calculable expr", K(ret));
    } else if (!got_result) {
      is_valid = false;
    }
  } else {
    is_valid = false;
  }
  if (OB_SUCC(ret) && is_valid) {
    number::ObNumber number;
    if (obj_value.is_null()) {
      is_valid = false;
    } else if (obj_value.is_integer_type()) {
      value = obj_value.get_int();
    } else if (!obj_value.is_number()) {
      is_valid = false;
    } else if (OB_FAIL(obj_value.get_number(number))) {
      LOG_WARN("unexpected value type", K(ret), K(obj_value));
    } else if (OB_UNLIKELY(!number.is_valid_int64(value))) {
      is_valid = false;
    }
  }
  return ret;
}

int ObTransformSimplifyWinfunc::check_stmt_win_can_be_removed(ObSelectStmt *select_stmt,
                                                              ObWinFunRawExpr *win_expr,
                                                              bool &can_be)
{
  int ret = OB_SUCCESS;
  can_be = false;
  bool is_unique = false;
  bool can_remove = false;
  bool contain = false;
  if (OB_ISNULL(select_stmt) || OB_ISNULL(win_expr) || OB_ISNULL(ctx_)
      || OB_ISNULL(ctx_->schema_checker_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL pointer error", K(ret));
  } else if (OB_FAIL(ObTransformUtils::check_stmt_unique(select_stmt, ctx_->session_info_,
                                                         ctx_->schema_checker_,
                                                         win_expr->get_partition_exprs(),
                                                         true,
                                                         is_unique,
                                                         FLAGS_IGNORE_DISTINCT))) {
    LOG_WARN("failed to check stmt unique", K(ret));
  } else if (!is_unique) {
    /*do nothing*/
  } else if (BoundType::BOUND_CURRENT_ROW != win_expr->get_lower().type_
             && BoundType::BOUND_CURRENT_ROW != win_expr->get_upper().type_
             && win_expr->get_upper().is_preceding_ == win_expr->get_lower().is_preceding_) {
    //upper 与 lower 均非 BOUND_CURRENT_ROW 且 is_preceding 相同时, 可能出现不包含当前行的窗口, 禁止消除
  } else if (OB_FAIL(check_aggr_win_can_be_removed(select_stmt, win_expr, can_remove))) {
    LOG_WARN("failed to check win can be removed", K(ret));
  } else if (!can_remove) {
    can_be = false;
  } else if (select_stmt->is_scala_group_by() &&
             OB_FAIL(check_window_contain_aggr(win_expr, contain))) {
    LOG_WARN("failed to check window contain aggr", K(ret));
  } else if (contain) {
    // scala group by 时, 若 win func 窗口的 order by/ partition by 中含有 aggr,
    // 为了避免移除所有 aggr 导致丢失 group by, 暂不做移除. 如下查询, 移除 win func 后无法保持 scala group by
    // select count(1) over (partition by max(a) order by min(a)) from t1;
    can_be = false;
  } else {
    can_be = true;
  }
  return ret;
}

int ObTransformSimplifyWinfunc::check_window_contain_aggr(ObWinFunRawExpr *win_expr,
                                                          bool &contain)
{
  int ret = OB_SUCCESS;
  contain = false;
  if (OB_ISNULL(win_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL pointer error", K(ret), K(win_expr));
  } else {
    ObRawExpr *expr = NULL;
    ObIArray<OrderItem> &win_order = win_expr->get_order_items();
    for (int64_t i = 0; !contain && OB_SUCC(ret) && i < win_order.count(); ++i) {
      if (OB_ISNULL(expr = win_order.at(i).expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret));
      } else if (expr->has_flag(CNT_AGG)) {
        contain = true;
      }
    }
    ObIArray<ObRawExpr*> &win_partition = win_expr->get_partition_exprs();
    for (int64_t i = 0; !contain && OB_SUCC(ret) && i < win_partition.count(); ++i) {
      if (OB_ISNULL(expr = win_partition.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret));
      } else if (expr->has_flag(CNT_AGG)) {
        contain = true;
      }
    }
    if (OB_SUCC(ret) && !contain) {
      if (NULL != win_expr->get_upper().interval_expr_ &&
          win_expr->get_upper().interval_expr_->has_flag(CNT_AGG)) {
        contain = true;
      } else if (NULL != win_expr->get_lower().interval_expr_ &&
                 win_expr->get_lower().interval_expr_->has_flag(CNT_AGG)) {
        contain = true;
      }
    }
  }
  return ret;
}

int ObTransformSimplifyWinfunc::do_remove_stmt_win(ObSelectStmt *select_stmt,
                                                   ObIArray<ObRawExpr*> &exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(select_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL pointer error", K(ret), K(select_stmt));
  } else {
    ObRawExpr *new_expr = NULL;
    ObSEArray<ObRawExpr*, 4> new_exprs;
    for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
      if (OB_ISNULL(exprs.at(i)) || !exprs.at(i)->is_win_func_expr()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected win expr", K(ret), K(exprs.at(i)));
      } else if (OB_FAIL(transform_aggr_win_to_common_expr(select_stmt, exprs.at(i), new_expr))) {
        LOG_WARN("transform aggr to common expr failed", K(ret));
      } else if (OB_FAIL(new_exprs.push_back(new_expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else if (OB_FAIL(select_stmt->remove_window_func_expr(
                           static_cast<ObWinFunRawExpr*>(exprs.at(i))))) {
        LOG_WARN("failed to remove window func expr", K(ret));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(select_stmt->replace_relation_exprs(exprs, new_exprs))) {
      LOG_WARN("select_stmt replace inner stmt expr failed", K(ret), K(select_stmt));
    }
    if (OB_SUCC(ret)) {
      //check qualify filters
      if (OB_FAIL(ObTransformUtils::pushdown_qualify_filters(select_stmt))) {
        LOG_WARN("check pushdown qualify filters failed", K(ret));
      }
    }
  }
  return ret;
}



int ObTransformSimplifyWinfunc::simplify_win_exprs(ObSelectStmt *stmt,
                                                   bool &trans_happened)
{
  int ret = OB_SUCCESS;
  bool is_happened = false;
  trans_happened = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_window_func_count(); ++i) {
    ObWinFunRawExpr *win_expr = NULL;
    if (OB_ISNULL(win_expr = stmt->get_window_func_expr(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("window function expr is null", K(ret));
    } else if (OB_FAIL(simplify_win_expr(*win_expr, is_happened))) {
      LOG_WARN("failed to simplify win expr", K(ret));
    } else {
      trans_happened |= is_happened;
    }
  }
  return ret;
}

int ObTransformSimplifyWinfunc::simplify_win_expr(ObWinFunRawExpr &win_expr,
                                                  bool &trans_happened)
{
  int ret = OB_SUCCESS;
  ObIArray<ObRawExpr *> &partition_exprs = win_expr.get_partition_exprs();
  ObIArray<OrderItem> &order_items = win_expr.get_order_items();
  ObSEArray<ObRawExpr *, 4> new_partition_exprs;
  ObSEArray<OrderItem, 4> new_order_items;
  ObSEArray<ObRawExpr *, 4> added_expr;
  trans_happened = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < partition_exprs.count(); ++i) {
    ObRawExpr *part_expr = partition_exprs.at(i);
    if (part_expr->is_const_expr()) {
    } else if (ObRawExprUtils::find_expr(added_expr, part_expr)) {
    } else if (OB_FAIL(new_partition_exprs.push_back(part_expr))) {
      LOG_WARN("failed to push back expr", K(ret));
    } else if (OB_FAIL(added_expr.push_back(part_expr))) {
      LOG_WARN("failed to push back expr", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < order_items.count(); ++i) {
    ObRawExpr *order_expr = order_items.at(i).expr_;
    if (order_expr->is_const_expr()) {
    } else if (ObRawExprUtils::find_expr(added_expr, order_expr)) {
    } else if (OB_FAIL(new_order_items.push_back(order_items.at(i)))) {
      LOG_WARN("failed to push back expr", K(ret));
    } else if (OB_FAIL(added_expr.push_back(order_expr))) {
      LOG_WARN("failed to push back expr", K(ret));
    }
  }
  if (OB_SUCC(ret)
      && new_order_items.count() == 0 && order_items.count() > 0) {
    // for computing range frame
    // at least one order item when executing
    if (win_expr.win_type_ == WINDOW_RANGE &&
        OB_FAIL(ObTransformUtils::rebuild_win_compare_range_expr(ctx_->expr_factory_, win_expr, order_items.at(0).expr_))) {
      LOG_WARN("failed to rebuild win compare range expr", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    trans_happened = (new_partition_exprs.count() < partition_exprs.count()
                      || new_order_items.count() < order_items.count());
    if (OB_FAIL(partition_exprs.assign(new_partition_exprs))) {
      LOG_WARN("failed to assign partition exprs", K(ret));
    } else if (OB_FAIL(order_items.assign(new_order_items))) {
      LOG_WARN("failed to assign order items", K(ret));
    }
  }
  return ret;
}

