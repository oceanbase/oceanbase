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

#define USING_LOG_PREFIX SQL_RESV
#include "sql/resolver/dml/ob_group_by_checker.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/plan_cache/ob_plan_cache_util.h"
#include "sql/ob_sql_context.h"
#include "sql/rewrite/ob_stmt_comparer.h"
#include "lib/utility/ob_macro_utils.h"
#include "common/ob_smart_call.h"

namespace oceanbase {
using namespace common;
namespace sql {

int ObGroupByChecker::check_analytic_function(
    ObSelectStmt* ref_stmt, common::ObIArray<ObRawExpr*>& exp1_arr, common::ObIArray<ObRawExpr*>& exp2_arr)
{
  int ret = OB_SUCCESS;
  if (share::is_oracle_mode()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < exp1_arr.count(); ++i) {
      ObRawExpr* expr = exp1_arr.at(i);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("argument is null", K(ret), K(i));
      } else if (expr->has_flag(CNT_SUB_QUERY)) {
        ret = OB_ERR_WIN_FUNC_ARG_NOT_IN_PARTITION_BY;
      }
    }
    if (OB_SUCCESS == ret &&
        OB_FAIL(check_by_expr(ref_stmt, exp2_arr, exp1_arr, OB_ERR_WIN_FUNC_ARG_NOT_IN_PARTITION_BY))) {
      LOG_WARN("argument should be a function of expressions in PARTITION BY", K(ret));
    }
  }
  return ret;
}

// ref_stmt: current stmt
// group_by_exprs: refed exprs
// checked_exprs: need to check by refed exprs
// check checked_exprs should reference refed exprs
//  refed exprs  : c1,c2
//  checked_exprs: c3  --it's wrong
//  checked_exprs: c2,c2+1  --it's ok
int ObGroupByChecker::check_by_expr(const ObSelectStmt* ref_stmt, ObIArray<ObRawExpr*>& group_by_exprs,
    ObIArray<ObRawExpr*>& checked_exprs, int err_code)
{
  int ret = OB_SUCCESS;
  ObGroupByChecker checker(&group_by_exprs);
  checker.set_level(0);
  ObSelectStmt* sel_stmt = const_cast<ObSelectStmt*>(ref_stmt);
  if (OB_ISNULL(sel_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("argument is null", K(ret));
  } else if (FALSE_IT(checker.set_query_ctx(sel_stmt->get_query_ctx()))) {
  } else if (OB_FAIL(checker.cur_stmts_.push_back(ref_stmt))) {
    LOG_WARN("failed to push back stmt", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < checked_exprs.count(); ++i) {
      ObRawExpr* expr = checked_exprs.at(i);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("argument is null", K(ret), K(i));
      } else if (OB_FAIL(expr->preorder_accept(checker))) {
        LOG_WARN("exp1 not based on exp_arr2", K(*expr), K(ret), K(err_code));
      }
    }
    if (OB_ERR_WRONG_FIELD_WITH_GROUP == ret) {
      ret = err_code;
      if (OB_ERR_WIN_FUNC_ARG_NOT_IN_PARTITION_BY == ret) {
        LOG_USER_ERROR(OB_ERR_WIN_FUNC_ARG_NOT_IN_PARTITION_BY);
      } else if (OB_ERR_NOT_SELECTED_EXPR == ret) {
        LOG_USER_ERROR(OB_ERR_NOT_SELECTED_EXPR);
      }
      LOG_WARN("exp1 not based on exp_arr2", K(ret), K(err_code));
    }
  }

  if (OB_SUCC(ret)) {
    const ObSelectStmt* pop_stmt = NULL;
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = checker.cur_stmts_.pop_back(pop_stmt))) {
      LOG_WARN("fail to pop back stmt", K(ret), K(err_code));
    }
  }
  return ret;
}

int ObGroupByChecker::check_groupby_valid(ObRawExpr* expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr should not be NULL", K(ret));
  } else if (share::is_oracle_mode() && expr->get_result_type().is_lob_locator()) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("inconsistent datatypes", K(ret));
  } else {
    switch (expr->get_expr_type()) {
      case T_OP_CASE: {
        ObCaseOpRawExpr* case_when_expr = static_cast<ObCaseOpRawExpr*>(expr);
        for (int64_t i = 0; OB_SUCC(ret) && i < case_when_expr->get_when_expr_size(); i++) {
          if (OB_ISNULL(case_when_expr->get_when_param_expr(i))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("expr should not be NULL", K(ret));
          } else if (case_when_expr->get_when_param_expr(i)->has_flag(CNT_SUB_QUERY)) {
            if (case_when_expr->get_when_param_expr(i)->has_flag(IS_WITH_ANY)) {
              ObRawExpr* subquery = case_when_expr->get_when_param_expr(i)->get_param_expr(1);
              if (OB_ISNULL(subquery)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("expr should not be NULL", K(ret));
              } else {
                ObSelectStmt* stmt = static_cast<ObQueryRefRawExpr*>(subquery)->get_ref_stmt();
                if (OB_ISNULL(stmt)) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("expr should not be NULL", K(ret));
                } else if (!stmt->has_group_by()) {
                  /*do nothing*/
                } else {
                  ret = OB_ERR_INVALID_SUBQUERY_USE;
                  LOG_WARN("subquery expressions not allowed in case expresssion.", K(ret));
                }
              }
            } else {
              ret = OB_ERR_INVALID_SUBQUERY_USE;
              LOG_WARN("subquery expressions not allowed in case expresssion.", K(ret));
            }
          } else { /* do nothing. */
          }
        }
        break;
      }
      default:
        break;
    }
  }
  return ret;
}

// check group by when stmt has group by or aggregate function or having that contains columns belongs to self stmt
// if select stmt has no group by and no aggregate function, but if has having clause
// then if having clause has columns belongs to current stmt, then need check group by
// eg:
//  select c1 +1 +2 from t1 having c1+1 >0;  // it need check, report error
//  select c1 from t1 having 1>0; // it don't need check, it will success
int ObGroupByChecker::check_group_by(ObSelectStmt* ref_stmt)
{
  int ret = OB_SUCCESS;
  // group by checker
  if (OB_ISNULL(ref_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (ref_stmt->has_group_by() || ref_stmt->has_rollup() || ref_stmt->has_grouping_sets() ||
             ref_stmt->has_having_self_column()) {
    ObSEArray<ObGroupbyExpr, 4> all_grouping_sets_exprs;
    ObSEArray<ObRawExpr*, 4> all_rollup_exprs;
    for (int64_t i = 0; OB_SUCC(ret) && i < ref_stmt->get_grouping_sets_items_size(); ++i) {
      if (OB_FAIL(append(all_grouping_sets_exprs, ref_stmt->get_grouping_sets_items().at(i).grouping_sets_exprs_))) {
        LOG_WARN("failed to append exprs", K(ret));
      } else {
        ObIArray<ObMultiRollupItem>& multi_rollup_items = ref_stmt->get_grouping_sets_items().at(i).multi_rollup_items_;
        for (int64_t j = 0; OB_SUCC(ret) && j < multi_rollup_items.count(); ++j) {
          if (OB_FAIL(append(all_grouping_sets_exprs, multi_rollup_items.at(j).rollup_list_exprs_))) {
            LOG_WARN("failed to append exprs", K(ret));
          }
        }
      }
    }
    if (OB_FAIL(append(all_rollup_exprs, ref_stmt->get_rollup_exprs()))) {
      LOG_WARN("failed to expr", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < ref_stmt->get_multi_rollup_items_size(); ++i) {
        ObIArray<ObGroupbyExpr>& rollup_list_exprs = ref_stmt->get_multi_rollup_items().at(i).rollup_list_exprs_;
        for (int64_t j = 0; OB_SUCC(ret) && j < rollup_list_exprs.count(); ++j) {
          if (OB_FAIL(append(all_rollup_exprs, rollup_list_exprs.at(j).groupby_exprs_))) {
            LOG_WARN("failed to append exprs", K(ret));
          } else { /*do nothing*/
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      ObGroupByChecker checker(&ref_stmt->get_group_exprs(), &all_rollup_exprs, &all_grouping_sets_exprs);
      checker.set_query_ctx(ref_stmt->get_query_ctx());
      checker.set_nested_aggr(ref_stmt->contain_nested_aggr());
      if (OB_FAIL(checker.check_select_stmt(ref_stmt))) {
        LOG_WARN("failed to check group by", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < ref_stmt->get_group_expr_size(); i++) {
          if (OB_FAIL(check_groupby_valid(ref_stmt->get_group_exprs().at(i)))) {
            LOG_WARN("failed to check groupby valid.", K(ret));
          } else { /* do nothing. */
          }
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < ref_stmt->get_rollup_expr_size(); i++) {
          if (OB_FAIL(check_groupby_valid(ref_stmt->get_rollup_exprs().at(i)))) {
            LOG_WARN("failed to check groupby valid.", K(ret));
          } else { /* do nothing. */
          }
        }
        if (OB_SUCC(ret) && ref_stmt->is_scala_group_by() && share::is_oracle_mode()) {
          for (int64_t i = 0; OB_SUCC(ret) && i < ref_stmt->get_select_item_size(); i++) {
            if (OB_FAIL(check_scalar_groupby_valid(ref_stmt->get_select_item(i).expr_))) {
              LOG_WARN("failed to check scalar groupby valid.", K(ret));
            } else { /* do nothing. */
            }
          }
        }
      }
    }
  } else {
    /* no group by && no aggregate function && no having that exists column belongs to self stmt */
  }
  return ret;
}

int ObGroupByChecker::add_pc_const_param_info(ObExprEqualCheckContext& check_ctx)
{
  int ret = OB_SUCCESS;
  // if oracle mode, constant of group by will be parameterlized
  // select a + 1 from t group by a + 1 => select a + ? from t group by a + ?
  // some plan sharing handling here
  ObPCConstParamInfo const_param_info;
  if (OB_ISNULL(query_ctx_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < check_ctx.param_expr_.count(); i++) {
      ObExprEqualCheckContext::ParamExprPair& param_pair = check_ctx.param_expr_.at(i);
      if (const_param_info.const_idx_.push_back(param_pair.param_idx_)) {
        LOG_WARN("failed to push back element", K(ret));
      } else {
        // do nothing
      }
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (const_param_info.const_idx_.count() > 0 &&
               OB_FAIL(query_ctx_->all_plan_const_param_constraints_.push_back(const_param_info))) {
      LOG_WARN("failed to push back element", K(ret));
    } else if (const_param_info.const_idx_.count() > 0 &&
               OB_FAIL(query_ctx_->all_possible_const_param_constraints_.push_back(const_param_info))) {
      LOG_WARN("failed to push back element", K(ret));
    } else {
      // do nothing
    }
  }
  return ret;
}

bool ObGroupByChecker::find_in_rollup(ObRawExpr& expr)
{
  bool found = false;
  ObStmtCompareContext check_ctx;
  int ret = OB_SUCCESS;
  if (OB_ISNULL(query_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null pointer.", K(ret));
  } else if (OB_FAIL(check_ctx.init(query_ctx_))) {
    LOG_WARN("failed to init the check context.", K(ret));
  } else if (nullptr != rollup_exprs_) {
    int64_t rollup_cnt = rollup_exprs_->count();
    for (int64_t nth_rollup = 0; !found && nth_rollup < rollup_cnt; ++nth_rollup) {
      check_ctx.reset();
      check_ctx.override_const_compare_ = true;
      if (expr.same_as(*rollup_exprs_->at(nth_rollup), &check_ctx)) {
        found = true;
        LOG_DEBUG("found in rollup exprs", K(expr));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (found && OB_SUCCESS == check_ctx.err_code_ && share::is_oracle_mode()) {
    if (OB_FAIL(append(query_ctx_->all_equal_param_constraints_, check_ctx.equal_param_info_))) {
      LOG_WARN("failed to append equal params constraints", K(ret));
    } else if (OB_FAIL(add_pc_const_param_info(check_ctx))) {
      LOG_WARN("failed to add pc const param info.", K(ret));
    } else { /*do nothing.*/
    }
  }
  return found;
}

bool ObGroupByChecker::find_in_group_by(ObRawExpr& expr)
{
  int ret = OB_SUCCESS;
  bool found = false;
  ObStmtCompareContext check_ctx;
  if (OB_ISNULL(query_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null pointer.", K(ret));
  } else if (OB_FAIL(check_ctx.init(query_ctx_))) {
    LOG_WARN("failed to init the check context.", K(ret));
  } else if (nullptr != group_by_exprs_) {
    int64_t group_by_cnt = group_by_exprs_->count();
    for (int64_t nth_group_by = 0; !found && nth_group_by < group_by_cnt; ++nth_group_by) {
      check_ctx.reset();
      check_ctx.override_const_compare_ = true;
      if (expr.same_as(*group_by_exprs_->at(nth_group_by), &check_ctx)) {
        found = true;
        LOG_DEBUG("found in group by exprs", K(expr));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (found && OB_SUCCESS == check_ctx.err_code_ && share::is_oracle_mode()) {
    // two constraints extracted here
    // 1. select a+1 from t1 group by a+1 --> select a+? from t1 group by a+?
    //    equality, eg.
    //    select a+3 from t1 group by a+3
    // 2. select a+1 from t1 group by a+1 order by a+1 -->
    //                            select a+? from t1 group by a+? order by a+1
    //    parameter equals to a constant
    if (OB_FAIL(append(query_ctx_->all_equal_param_constraints_, check_ctx.equal_param_info_))) {
      LOG_WARN("failed to append equal params constraints", K(ret));
    } else if (OB_FAIL(add_pc_const_param_info(check_ctx))) {
      LOG_WARN("failed to add pc const param info.", K(ret));
    } else { /*do nothing.*/
    }
  }
  return found;
}

bool ObGroupByChecker::find_in_grouping_sets(ObRawExpr& expr)
{
  int ret = OB_SUCCESS;
  bool found = false;
  ObStmtCompareContext check_ctx;
  if (OB_ISNULL(query_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null pointer.", K(ret));
  } else if (OB_FAIL(check_ctx.init(query_ctx_))) {
    LOG_WARN("failed to init the check context.", K(ret));
  } else if (nullptr != grouping_sets_exprs_) {
    int64_t gs_cnt = grouping_sets_exprs_->count();
    for (int64_t nth_gs = 0; !found && nth_gs < gs_cnt; ++nth_gs) {
      int64_t group_by_cnt = grouping_sets_exprs_->at(nth_gs).groupby_exprs_.count();
      for (int64_t nth_group_by = 0; !found && nth_group_by < group_by_cnt; ++nth_group_by) {
        check_ctx.reset();
        check_ctx.override_const_compare_ = true;
        if (expr.same_as(*grouping_sets_exprs_->at(nth_gs).groupby_exprs_.at(nth_group_by), &check_ctx)) {
          found = true;
          LOG_DEBUG("found in grouping sets exprs", K(expr));
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (found && OB_SUCCESS == check_ctx.err_code_ && share::is_oracle_mode()) {
    if (OB_FAIL(append(query_ctx_->all_equal_param_constraints_, check_ctx.equal_param_info_))) {
      LOG_WARN("failed to append equal params constraints", K(ret));
    } else if (OB_FAIL(add_pc_const_param_info(check_ctx))) {
      LOG_WARN("failed to add pc const param info.", K(ret));
    } else { /*do nothing.*/
    }
  }
  return found;
}

// check whether exprs belongs to stmt checked group by
// select c1,(select d1 from t1 b) from t2 a group by c1;
// when check subquery (select d1 from t1 b), d1 not belongs to the stmt need checked
int ObGroupByChecker::belongs_to_check_stmt(ObRawExpr& expr, bool& belongs_to)
{
  int ret = OB_SUCCESS;
  belongs_to = false;
  if (cur_stmts_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get stmt", K(ret));
  } else {
    // Stmt needed check
    // For ObPseudoColumnRawExpr only the expr belongs to top select stmt, it need check group by
    // eg: select prior c1 from t1 connect by prior c1=c2 group by prior c1; it's ok, because prior c1 exists in group
    // by
    //   But
    //     select prior c1,(select prior a.c1 from t1 b connect by nocycle prior c1=c2) as c2 from t1 a connect by
    //     nocycle prior c1=c2 group by prior c1;
    //  it report error, because "prior a.c1" in subquery, it's not belongs to refered subquery, so a.c1 not exists in
    //  group by, report error
    // and for aggregate function, if aggregate function belongs to checked select stmt, the arguemnt of aggregate
    // function don't need check group by eg: select count(c1) from t1 group by c2; But if aggregate function not
    // belongs to checked select stmt, then the argument of aggregate function need check eg: select count(c1), (select
    // count(a.c1) from t2 b) from t1 a group by c2; --then "count(a.c1)" in subquery should report error
    const ObSelectStmt* top_stmt = cur_stmts_.at(0);
    UNUSED(top_stmt);
    if (is_top_select_stmt()) {
      // the expr is not from checked stmt
      belongs_to = true;
      LOG_DEBUG("same level", K(ret), K(expr), K(expr.get_expr_level()), K(top_stmt->get_current_level()), K_(level));
    } else {
      LOG_DEBUG(
          "different level", K(ret), K(expr), K(expr.get_expr_level()), K(top_stmt->get_current_level()), K_(level));
    }
  }
  return ret;
}

// see if column belongs to current select stmt
// eg: select a.c1+1 from t1 a group by c1;  -- a.c1 belongs to "from a group by c1"
//  But
//     select (select a.c1 from t1 b where c1=10 group by b.c1) c1 from t1 a group by a.c1;
//     a.c1 belongs the select stmt that contains table a
//     so when check subquery that contains table b, don't check a.c1 whether exists in group by b.c1
int ObGroupByChecker::colref_belongs_to_check_stmt(ObRawExpr& expr, bool& belongs_to)
{
  int ret = OB_SUCCESS;
  belongs_to = false;
  if (cur_stmts_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get stmt", K(ret));
  } else {
    // the stmt needed check
    const ObSelectStmt* top_stmt = cur_stmts_.at(0);
    // For ObPseudoColumnRawExpr only the expr belongs to top select stmt, it need check group by
    // eg: select prior c1 from t1 connect by prior c1=c2 group by prior c1; it's ok, because prior c1 exists in group
    // by
    //   But
    //     select prior c1,(select prior a.c1 from t1 b connect by nocycle prior c1=c2) as c2 from t1 a connect by
    //     nocycle prior c1=c2 group by prior c1;
    //  it report error, because "prior a.c1" in subquery, it's not belongs to refered subquery, so a.c1 not exists in
    //  group by, report error
    // and for aggregate function, if aggregate function belongs to checked select stmt, the arguemnt of aggregate
    // function don't need check group by eg: select count(c1) from t1 group by c2; But if aggregate function not
    // belongs to checked select stmt, then the argument of aggregate function need check eg: select count(c1), (select
    // count(a.c1) from t2 b) from t1 a group by c2; --then "count(a.c1)" in subquery should report error
    if (expr.get_expr_level() == top_stmt->get_current_level()) {
      // the expr is not from checked stmt
      belongs_to = true;
      LOG_DEBUG("same level", K(ret), K(expr), K(expr.get_expr_level()), K(top_stmt->get_current_level()), K_(level));
    } else {
      LOG_DEBUG(
          "different level", K(ret), K(expr), K(expr.get_expr_level()), K(top_stmt->get_current_level()), K_(level));
    }
  }
  return ret;
}

int ObGroupByChecker::visit(ObConstRawExpr& expr)
{
  int ret = OB_SUCCESS;
  if (find_in_rollup(expr) || find_in_grouping_sets(expr)) {
    set_skip_expr(&expr);
  }
  return OB_SUCCESS;
}

int ObGroupByChecker::visit(ObVarRawExpr& expr)
{
  UNUSED(expr);
  return OB_SUCCESS;
}

int ObGroupByChecker::check_select_stmt(const ObSelectStmt* ref_stmt)
{
  int ret = OB_SUCCESS;
  ++level_;
  bool ref_query = false;
  LOG_DEBUG("check group by start stmt", K(ret));

  if (is_top_select_stmt()) {
    top_stmt_ = ref_stmt;
  }

  if (OB_ISNULL(ref_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ref_stmt should not be NULL", K(ret));
  } else if (OB_FAIL(cur_stmts_.push_back(ref_stmt))) {
    LOG_WARN("failed to push back stmt", K(ret));
  } else if (!is_top_select_stmt() && OB_FAIL(ObTransformUtils::is_ref_outer_block_relation(
                                          ref_stmt, ref_stmt->get_current_level(), ref_query))) {
    LOG_WARN("failed to get ref stmt", K(ret));
  } else if (!is_top_select_stmt() && !ref_query) {
    // non ref query
  } else {
    int32_t ignore_scope = 0;
    if (is_top_select_stmt()) {
      // in current top select stmt, only checks having, select item, order
      // eg:
      // select c1,c2,(select d2 from t2 where t1.c1=t2.d1) as c3 from t1 group by c1,c2;
      //  level_=0, the stmt is from "select c1,c2,(select d2 from t2 where t1.c1=t2.d1) as c3 from t1 group by c1,c2"
      //  level_=1, the stmt is from "(select d2 from t2 where t1.c1=t2.d1)"
      ignore_scope = RelExprCheckerBase::START_WITH_SCOPE | RelExprCheckerBase::JOIN_CONDITION_SCOPE |
                     RelExprCheckerBase::CONNECT_BY_SCOPE | RelExprCheckerBase::LIMIT_SCOPE |
                     RelExprCheckerBase::WHERE_SCOPE | RelExprCheckerBase::GROUP_SCOPE | RelExprCheckerBase::FROM_SCOPE;
    } else {
      // for subquery, need to check every expression
    }
    // if order by has siblings, then order by belongs to connect by, so don't check
    // eg: select max(c2) from t1 start with c1 = 1 connect by nocycle prior c1 = c2 order siblings by c1, c2;
    // special case: select count(*) from t1 order by c1; skip to check order by scope
    // distinct case: select distinct count(*) from t1 group by c1 order by c2;  --report error "not a SELECTed
    // expression" instead of "not a GROUP BY expression"
    //                it report error by order by check instead of group by check
    if (ref_stmt->is_order_siblings() ||
        (is_top_select_stmt() && (NULL == group_by_exprs_ || group_by_exprs_->empty() || ref_stmt->has_distinct()) &&
            (NULL == rollup_exprs_ || rollup_exprs_->empty() || ref_stmt->has_distinct()) &&
            (NULL == grouping_sets_exprs_ || grouping_sets_exprs_->empty() || ref_stmt->has_distinct()))) {
      ignore_scope |= RelExprCheckerBase::ORDER_SCOPE;
    }
    ObArray<ObRawExpr*> relation_expr_pointers;
    if (OB_FAIL(ref_stmt->get_relation_exprs(relation_expr_pointers, ignore_scope))) {
      LOG_WARN("get stmt relation exprs fail", K(ret));
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < relation_expr_pointers.count(); ++i) {
      ObRawExpr* expr = relation_expr_pointers.at(i);
      if (OB_FAIL(expr->preorder_accept(*this))) {
        LOG_WARN("fail to check group by", K(i), K(ret));
      }
    }

    // done, quit
    int tmp_ret = OB_SUCCESS;
    const ObSelectStmt* pop_stmt = NULL;
    tmp_ret = cur_stmts_.pop_back(pop_stmt);
    // even if ret is not success, it must
    if (OB_SUCCESS != tmp_ret) {
      ret = tmp_ret;
      LOG_WARN("failed to pop back stmt", K(ret));
    }
    if (OB_SUCC(ret)) {
      const ObIArray<ObSelectStmt*>& child_stmts = ref_stmt->get_set_query();
      for (int64_t i = 0; OB_SUCC(ret) && i < child_stmts.count(); ++i) {
        ret = check_select_stmt(child_stmts.at(i));
      }
    }
  }
  LOG_DEBUG("check group by end stmt", K(ret));
  --level_;
  return ret;
}

// for subquery
// if it's refered subquery, then check all expressions including select_items, where, connect by, group by, having,
// order by ref query also exists in group by eg:
//  select case when c1 in (select a.c1 from t1 b) then 1 else 0 end c1 from t1 a group by case when c1 in (select a.c1
//  from t1 b) then 1 else 0 end;
int ObGroupByChecker::visit(ObQueryRefRawExpr& expr)
{
  int ret = OB_SUCCESS;
  if (find_in_group_by(expr) || find_in_rollup(expr) || find_in_grouping_sets(expr)) {
    set_skip_expr(&expr);
  } else {
    const ObSelectStmt* ref_stmt = expr.get_ref_stmt();
    if (OB_FAIL(check_select_stmt(ref_stmt))) {
      LOG_WARN("failed to check select stmt", K(ret));
    }
  }
  return ret;
}

// check column ref
// eg: select case when 1 in(select d1 from t1 where c1=d2) then 1 else 0 end as c1 from t2 group by c1;
//   only check c1, but d1 and d2 are not checked
int ObGroupByChecker::visit(ObColumnRefRawExpr& expr)
{
  int ret = OB_SUCCESS;
  bool belongs_to = true;
  if (OB_FAIL(colref_belongs_to_check_stmt(expr, belongs_to))) {
    LOG_WARN("failed to get belongs to stmt", K(ret));
  } else if (belongs_to) {
    // expr is in the current  stmt
    if ((!find_in_group_by(expr) && !find_in_rollup(expr)) && !find_in_grouping_sets(expr)) {
      ret = OB_ERR_WRONG_FIELD_WITH_GROUP;
      ObString column_name =
          concat_qualified_name(expr.get_database_name(), expr.get_table_name(), expr.get_column_name());
      LOG_USER_ERROR(OB_ERR_WRONG_FIELD_WITH_GROUP, column_name.length(), column_name.ptr());
      LOG_DEBUG("column not in group by", K(*group_by_exprs_), K(expr));
    }
  }
  return ret;
}

// need check the whole expression
// eg:
//    prior c1
//    connect_by_root c1
//    SYS_CONNECT_BY_PATH(c1,'/')
// and it's different from oracle, I think it's oracle's bug
// eg:
//  1)select max(c1),prior c1 from t1 connect by nocycle prior c1 > c2 group by  prior c1;
//  2)select max(c1),prior c1 from t1 connect by nocycle prior c1 > c2 group by  c1;
// oracle will report error, but 1) is success in ob
int ObGroupByChecker::visit(ObOpRawExpr& expr)
{
  int ret = OB_SUCCESS;
  if (find_in_group_by(expr) || find_in_rollup(expr) || find_in_grouping_sets(expr)) {
    set_skip_expr(&expr);
  } else {
    switch (expr.get_expr_type()) {
      case T_OP_CONNECT_BY_ROOT:
      case T_OP_PRIOR: {
        bool belongs_to = true;
        if (OB_FAIL(belongs_to_check_stmt(expr, belongs_to))) {
          LOG_WARN("failed to get belongs to stmt", K(ret));
        } else if (belongs_to) {
          ret = OB_ERR_WRONG_FIELD_WITH_GROUP;
          // LOG_USER_ERROR(ret, column_name.length(), column_name.ptr());
          LOG_DEBUG("connect by column not in group by", K(*group_by_exprs_), K(expr));
        }
        break;
      }
      default:
        break;
    }
  }
  return ret;
}

int ObGroupByChecker::visit(ObCaseOpRawExpr& expr)
{
  int ret = OB_SUCCESS;
  if (find_in_group_by(expr) || find_in_rollup(expr) || find_in_grouping_sets(expr)) {
    set_skip_expr(&expr);
  }
  return ret;
}

int ObGroupByChecker::visit(ObAggFunRawExpr& expr)
{
  int ret = OB_SUCCESS;
  bool belongs_to = true;
  // expr is not in the current stmt, the expr is from subquery, it will check the expr from parent stmt
  // eg: select (select max(a.d2)from t1 b where b.c2=a.d1) from t2 b group by d1;
  //    then a.d2 that is in max(a.d2) is not in group by d1, it will report error
  if (find_in_group_by(expr) || find_in_rollup(expr) || find_in_grouping_sets(expr)) {
    set_skip_expr(&expr);
  } else if (OB_FAIL(belongs_to_check_stmt(expr, belongs_to))) {
    LOG_WARN("failed to get belongs to stmt", K(ret));
  } else if (belongs_to) {
    // expr is aggregate function in current stmt, then skip it
    if (expr.is_nested_aggr()) {
      set_skip_expr(&expr);
    } else if (has_nested_aggr_) {
      // do nothing.
    } else {
      set_skip_expr(&expr);
    }
  } else {
    set_skip_expr(&expr);
  }
  return ret;
}

int ObGroupByChecker::visit(ObSysFunRawExpr& expr)
{
  int ret = OB_SUCCESS;
  if (find_in_group_by(expr) || find_in_rollup(expr) || find_in_grouping_sets(expr)) {
    set_skip_expr(&expr);
  } else {
    if (T_FUN_SYS_ROWNUM == expr.get_expr_type()) {
      bool belongs_to = true;
      if (OB_FAIL(belongs_to_check_stmt(expr, belongs_to))) {
        LOG_WARN("failed to get belongs to stmt", K(ret));
      } else if (belongs_to) {
        ret = OB_ERR_WRONG_FIELD_WITH_GROUP;
        LOG_USER_ERROR(OB_ERR_WRONG_FIELD_WITH_GROUP, expr.get_func_name().length(), expr.get_func_name().ptr());
      }
    }
  }
  return ret;
}

// select (select c1 from t1 union select d1 from t2) as c1 from t3;
// then QueryRefRaw: (select c1 from t1 union select d1 from t2)
// and the select stmt->select_itmes are SetOpRawExpr
// then SetOpRawExpr is union(c1,d1)
int ObGroupByChecker::visit(ObSetOpRawExpr& expr)
{
  int ret = OB_SUCCESS;
  if (find_in_group_by(expr) || find_in_rollup(expr) || find_in_grouping_sets(expr)) {
    set_skip_expr(&expr);
  }
  return ret;
}

int ObGroupByChecker::visit(ObAliasRefRawExpr& expr)
{
  int ret = OB_SUCCESS;
  if (find_in_group_by(expr) || find_in_rollup(expr) || find_in_grouping_sets(expr)) {
    set_skip_expr(&expr);
  }
  return ret;
}

/*
 * full context
 * select * from ttt1 where match (full_name) against ("alipay + Jack MA" IN BOOLEAN MODE);
 * it will generate:
 *                ObSetIterRawExpr(intersect)
 *                  /                   \
 *     ObRowIterRawExpr('Alipay')       ObRowIterRawExpr('Jack Ma')
 */
int ObGroupByChecker::visit(ObFunMatchAgainst& expr)
{
  int ret = OB_SUCCESS;
  if (find_in_group_by(expr) || find_in_rollup(expr) || find_in_grouping_sets(expr)) {
    set_skip_expr(&expr);
  }
  return ret;
}

// don't check the whole window function, but visit all argument of window function
int ObGroupByChecker::visit(ObWinFunRawExpr& expr)
{
  int ret = OB_SUCCESS;
  if (find_in_group_by(expr) || find_in_rollup(expr) || find_in_grouping_sets(expr)) {
    set_skip_expr(&expr);
  }
  return ret;
}

int ObGroupByChecker::visit(ObPseudoColumnRawExpr& expr)
{
  int ret = OB_SUCCESS;
  bool belongs_to = true;
  if (OB_FAIL(belongs_to_check_stmt(expr, belongs_to))) {
    LOG_WARN("failed to get belongs to stmt", K(ret));
  } else if (belongs_to) {
    if (find_in_group_by(expr) || find_in_rollup(expr) || find_in_grouping_sets(expr)) {
      set_skip_expr(&expr);
    } else if (T_ORA_ROWSCN != expr.get_expr_type()) {
      ret = OB_ERR_WRONG_FIELD_WITH_GROUP;
      // LOG_USER_ERROR(ret, column_name.length(), column_name.ptr());
      LOG_DEBUG("pseudo column not in group by", K(*group_by_exprs_), K(expr));
    }
  }
  return ret;
}

// oracle doesn't allow: select sum(c1), (select 1 from dual) from t1;
// so in oracle mode, scalar group by needs to be treated separately
int ObGroupByChecker::check_scalar_groupby_valid(ObRawExpr* expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr should not be NULL", K(ret));
    // allow aggr to have subquery: select sum(c1), sum((select 1 from dual)) from t1;
  } else if (expr->is_aggr_expr()) {
    /*do nothing*/
  } else if (expr->is_query_ref_expr()) {
    ret = OB_ERR_NOT_A_SINGLE_GROUP_FUNCTION;
    LOG_WARN("not a single-group group function", K(ret));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(check_scalar_groupby_valid(expr->get_param_expr(i))))) {
        LOG_WARN("failed to check scalar groupby valid");
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
