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

#include "ob_transform_aggr_subquery.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/oblog/ob_log_module.h"
#include "share/ob_errno.h"
#include "common/ob_common_utility.h"
#include "common/ob_smart_call.h"
#include "ob_transformer_impl.h"
#include "sql/parser/ob_item_type.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/resolver/dml/ob_dml_stmt.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "sql/ob_sql_context.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/resolver/dml/ob_update_stmt.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

/**
 *    select * from A where c1 > (select min(B.c1) from B where A.c2 = B.c2);
 * => select * from A, (select min(A.c1) as x, B.c2 as y from B group by B.c2) v where c1 > x and c2 = y;
 *
 *    select * from A where c1 > (select count(B.c1) from B where A.c2 = B.c2);
 * => select * from A left join
 *                 (select count(B.c1) as x, B.c2 as y from B group by B.c2) v on c2 = y
 *             where c1 > (case when y is not null then x else 0 end);

 *    select A.c1, (select sum(B.c2) from B where A.c1 = B.c1 ) as sum_e from A where A.c1 > 0;
 * => select A.c1, temp.sum_e from B left join
 *                              (select A.c1, sum(B.c2) as sum_e from B group by B.c1) temp on A.c1 = temp.c1
 *                            where A.c1 > 0;
 *
 *    select A.c1, (select count(B.c2) from B where A.c1 = B.c2) as sum_e from A where A.c1 > 0;
 * => select A.c1, (case when y is not null then temp.sum_e else 0 end) from B left join
 *                              (select A.c1 as y, sum(B.c2) as sum_e from B group by B.c1) temp on A.c1 = temp.c1
 *                            where A.c1 > 0;
 */
int ObTransformAggrSubquery::transform_one_stmt(
    common::ObIArray<ObParentDMLStmt>& parent_stmts, ObDMLStmt*& stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  UNUSED(parent_stmts);
  bool aggr_first_happened = false;
  bool join_first_happened = false;
  trans_happened = false;
  /**
   * transform_with_aggregation_first unnest a subquery by transform the subquery as view
   * with aggregation, then join origin tables with view.
   *
   * transform_with_join_first unnest a subquery by join subquery's table with origin tables
   * first, then aggregate on origin tables' unique set.
   */
  if (OB_FAIL(extract_no_rewrite_select_exprs(stmt))) {
    LOG_WARN("failed to extract no rewrite select exprs", K(ret));
  } else if (OB_FAIL(transform_with_aggregation_first(stmt, aggr_first_happened))) {
    LOG_WARN("failed to transfrom with aggregation first", K(ret));
  } else if (OB_FAIL(transform_with_join_first(stmt, join_first_happened))) {
    LOG_WARN("failed to transform stmt with join first ja", K(ret));
  } else {
    trans_happened = aggr_first_happened | join_first_happened;
  }
  return ret;
}

int ObTransformAggrSubquery::transform_with_aggregation_first(ObDMLStmt*& stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> exprs;
  const bool with_vector_assgin = true;
  if (OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid params", K(ret), K(stmt));
  } else if (stmt->is_hierarchical_query() || stmt->is_set_stmt() || !stmt->is_sel_del_upd()) {
    // do nothing
  } else if (OB_FAIL(exprs.assign(stmt->get_condition_exprs()))) {
    LOG_WARN("failed to assign conditions", K(ret));
  } else if (OB_FAIL(ObTransformUtils::get_post_join_exprs(stmt, exprs, with_vector_assgin))) {
    LOG_WARN("failed to get post join exprs", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    if (OB_FAIL(do_aggr_first_transform(stmt, exprs.at(i), trans_happened))) {
      LOG_WARN("failed to transform one expr", K(ret));
    }
  }
  return ret;
}

/**
 * @brief ObTransformAggrSubquery::do_aggr_first_transform
 * STEP 1: find a valid subquery
 * STEP 2: check the type of subquery, and choose the rewrite way (OUTER/INNER JOIN)
 * STEP 3: do the transformation
 * @return
 */
int ObTransformAggrSubquery::do_aggr_first_transform(ObDMLStmt*& stmt, ObRawExpr* expr, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  ObSEArray<TransformParam, 4> transform_params;
  ObSEArray<ObRawExpr*, 4> filters;
  if (OB_ISNULL(expr) || OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("condition is null", K(ret), K(expr), K(stmt));
  } else if (OB_FAIL(gather_transform_params(expr, expr, AGGR_FIRST, transform_params))) {
    LOG_WARN("failed to check the condition is valid for transformation", K(ret));
  } else if (OB_FAIL(get_filters(*stmt, expr, filters))) {
    LOG_WARN("failed to get filters for the expr", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < transform_params.count(); ++i) {
    TransformParam& trans_param = transform_params.at(i);
    ObQueryRefRawExpr* query_ref = NULL;
    ObSelectStmt* subquery = NULL;
    if (OB_ISNULL(query_ref = trans_param.ja_query_ref_) ||
        OB_ISNULL(subquery = trans_param.ja_query_ref_->get_ref_stmt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid transform params", K(ret), K(trans_param.ja_query_ref_));
    } else if (!ObOptimizerUtil::find_item(stmt->get_subquery_exprs(), query_ref)) {
      // skip, the subquery has been pullup
    } else if (OB_FAIL(choose_pullup_method(filters, trans_param))) {
      LOG_WARN("failed to choose pullup method", K(ret));
    } else if (OB_FAIL(fill_query_refs(stmt, expr, trans_param))) {
      LOG_WARN("failed to fill query refs", K(ret));
    } else if (OB_FAIL(transform_child_stmt(stmt, *subquery, trans_param))) {
      LOG_WARN("failed to transform subquery", K(ret));
    } else if (OB_FAIL(transform_upper_stmt(*stmt, trans_param))) {
      LOG_WARN("failed to transform upper stmt", K(ret));
    } else {
      trans_happened = true;
    }
  }
  return ret;
}

/**
 * find valid JA-subquery in the root expr and select item expr.
 **/
int ObTransformAggrSubquery::gather_transform_params(
    ObRawExpr* root_expr, ObRawExpr* child_expr, int64_t pullup_strategy, ObIArray<TransformParam>& transform_params)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(child_expr) || OB_ISNULL(root_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret), K(child_expr), K(root_expr));
  } else if (IS_SUBQUERY_COMPARISON_OP(child_expr->get_expr_type()) || !child_expr->has_flag(CNT_SUB_QUERY)) {
    // do nothing
  } else if (child_expr->is_query_ref_expr()) {
    TransformParam trans_param(pullup_strategy);
    trans_param.ja_query_ref_ = static_cast<ObQueryRefRawExpr*>(child_expr);
    ObSelectStmt* subquery = trans_param.ja_query_ref_->get_ref_stmt();
    bool is_valid = false;
    if (ObOptimizerUtil::find_item(no_rewrite_exprs_, child_expr)) {
      LOG_TRACE("subquery in select expr and can use index");
    } else if (aggr_first(pullup_strategy)) {
      if (OB_FAIL(check_subquery_validity(
              subquery, root_expr->has_flag(CNT_ALIAS), trans_param.nested_conditions_, is_valid))) {
        LOG_WARN("failed to check subquery validity for aggr first", K(ret));
      }
    } else if (join_first(pullup_strategy)) {
      if (OB_FAIL(check_subquery_validity(
              subquery, root_expr->has_flag(CNT_ALIAS), is_exists_op(root_expr->get_expr_type()), is_valid))) {
        LOG_WARN("failed to check subquery validity for join first", K(ret));
      }
    }
    if (OB_SUCC(ret) && is_valid) {
      if (OB_FAIL(transform_params.push_back(trans_param))) {
        LOG_WARN("failed to push back transform parameters", K(ret));
      }
    }
  } else {
    // join-first ja can rewrite exists predicate (must be a root relation expr in where/having)
    bool is_exists_filter =
        is_exists_op(child_expr->get_expr_type()) && root_expr == child_expr && join_first(pullup_strategy);
    if (is_exists_filter || !is_exists_op(child_expr->get_expr_type())) {
      for (int64_t i = 0; OB_SUCC(ret) && i < child_expr->get_param_count(); ++i) {
        if (OB_FAIL(SMART_CALL(gather_transform_params(
                root_expr, child_expr->get_param_expr(i), pullup_strategy, transform_params)))) {
          LOG_WARN("failed to gather transform params", K(ret));
        }
      }
    }
  }
  return ret;
}

/**
 * @brief ObTransformAggrSubquery::check_subquery_validity
 *  check the valiaidty of the subquery
 */
int ObTransformAggrSubquery::check_subquery_validity(
    ObSelectStmt* subquery, const bool vector_assign, ObIArray<ObRawExpr*>& nested_conditions, bool& is_valid)
{
  int ret = OB_SUCCESS;
  bool has_rownum = false;
  bool has_ref_assign_user_var = false;
  nested_conditions.reuse();
  is_valid = true;
  // 1. check stmt components
  if (OB_ISNULL(subquery)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("subquery is null", K(ret));
  } else if (subquery->get_stmt_hint().enable_no_unnest()) {
    is_valid = false;
    LOG_TRACE("has no unnest hint.", K(is_valid));
  } else if (subquery->has_rollup() || subquery->has_having() || subquery->has_limit() ||
             subquery->has_window_function() || subquery->has_sequence() || subquery->has_set_op() ||
             subquery->is_hierarchical_query()) {
    is_valid = false;
    LOG_TRACE("invalid subquery", K(is_valid), K(*subquery));
  } else if (OB_FAIL(is_valid_group_by(*subquery, is_valid))) {
    LOG_WARN("failed to check is single set query", K(ret));
  } else if (!is_valid) {
    // do nothing
  } else if (OB_FAIL(subquery->has_rownum(has_rownum))) {
    LOG_WARN("failed to check subquery has rownum", K(ret));
  } else if (has_rownum) {
    is_valid = false;
    LOG_TRACE("has rownum expr", K(is_valid));
  } else if (OB_FAIL(check_subquery_aggr_item(*subquery, is_valid))) {
    LOG_WARN("failed to check subquery select item", K(ret));
  } else if (!is_valid) {
    // do nothing
  } else if (OB_FAIL(subquery->has_ref_assign_user_var(has_ref_assign_user_var))) {
    LOG_WARN("failed to check stmt has assignment ref user var", K(ret));
  } else if (has_ref_assign_user_var) {
    is_valid = false;
    LOG_TRACE("has assignment ref user variable", K(is_valid));
    // 2. check the select item
  } else if (!vector_assign && subquery->get_select_item_size() > 1) {
    is_valid = false;
    LOG_TRACE("select item size is invalid", K(vector_assign), K(subquery->get_select_item_size()));
  } else if (OB_FAIL(check_subquery_select(*subquery, is_valid))) {
    LOG_WARN("failed to check select validity", K(ret));
  } else if (!is_valid) {
    // do nothing
    LOG_TRACE("select list is invalid", K(is_valid));
    // 3. check from list is not correlated
  } else if (OB_FAIL(check_subquery_table_item(*subquery, is_valid))) {
    LOG_WARN("failed to check subquery table item", K(ret));
  } else if (!is_valid) {
    // do nothing
    // 4. check correlated join on contiditons
  } else if (OB_FAIL(check_subquery_on_conditions(*subquery, is_valid))) {
    LOG_WARN("failed to check subquery on conditions", K(ret));
  } else if (!is_valid) {
    // do nothing
    // 5. check correlated join contiditons
  } else if (OB_FAIL(check_subquery_conditions(*subquery, nested_conditions, is_valid))) {
    LOG_WARN("failed to check subquery conditions", K(ret));
  }
  return ret;
}

int ObTransformAggrSubquery::check_subquery_select(ObSelectStmt& subquery, bool& is_valid)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < subquery.get_select_item_size(); ++i) {
    bool is_correlated = false;
    ObRawExpr* select_expr = NULL;
    if (OB_ISNULL(select_expr = subquery.get_select_item(0).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("select expr is null", K(ret));
    } else if (OB_FAIL(ObTransformUtils::is_correlated_expr(
                   select_expr, subquery.get_current_level() - 1, is_correlated))) {
      LOG_WARN("failed to check is correlated expr", K(ret));
    } else if (is_correlated || select_expr->has_flag(CNT_SUB_QUERY)) {
      // if we need add_case_when_expr for the select output,
      // in this case, we are required to build case when expr,
      // but we have no idea the value returned by a query_ref_expr
      // when the subquery's where clause is always false
      is_valid = false;
    }
  }
  return ret;
}

int ObTransformAggrSubquery::check_subquery_table_item(const ObSelectStmt& subquery, bool& is_valid)
{
  int ret = OB_SUCCESS;
  bool is_correlated = false;
  int32_t stmt_level = subquery.get_current_level();
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < subquery.get_table_size(); ++i) {
    const TableItem* item;
    if (OB_ISNULL(item = subquery.get_table_item(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table item is null", K(ret));
    } else if (!item->is_generated_table()) {
      // do nothing
    } else if (OB_FAIL(ObTransformUtils::is_ref_outer_block_relation(item->ref_query_, stmt_level, is_correlated))) {
      LOG_WARN("failed to check the ref table is correlated", K(ret));
    } else if (is_correlated) {
      is_valid = false;
      LOG_TRACE("from view is corelated with upper stmt", K(is_valid), K(*item->ref_query_));
    }
  }
  return ret;
}

int ObTransformAggrSubquery::check_subquery_conditions(
    ObSelectStmt& subquery, ObIArray<ObRawExpr*>& nested_conds, bool& is_valid)
{
  int ret = OB_SUCCESS;
  bool is_correlated = false;
  is_valid = true;
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < subquery.get_condition_size(); ++i) {
    ObRawExpr* cond = NULL;
    if (OB_ISNULL(cond = subquery.get_condition_expr(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("condition expr is null", K(ret));
    } else if (OB_FAIL(ObTransformUtils::is_correlated_expr(cond, subquery.get_current_level() - 1, is_correlated))) {
      LOG_WARN("failed to check is correlated condition", K(ret));
    } else if (!is_correlated) {
      // do nothing
    } else if (!cond->get_expr_levels().has_member(subquery.get_current_level() - 1) ||
               !cond->get_expr_levels().has_member(subquery.get_current_level()) || cond->has_flag(CNT_SUB_QUERY)) {
      is_valid = false;
    } else if (OB_FAIL(ObTransformUtils::is_equal_correlation(cond, subquery.get_current_level(), is_valid))) {
      LOG_WARN("failed to check is equal correlation", K(ret));
    } else if (!is_valid) {
      // do nothing
    } else if (OB_FAIL(nested_conds.push_back(cond))) {
      LOG_WARN("failed to push back nested conditions", K(ret));
    }
  }
  return ret;
}

/**
 * @brief check the select item, and the subquery comparison operator
 *        determine the join method
 *  filters: filters on the ja query refs
 **/
int ObTransformAggrSubquery::choose_pullup_method(ObIArray<ObRawExpr*>& filters, TransformParam& trans_param)
{
  int ret = OB_SUCCESS;
  ObQueryRefRawExpr* query_ref = trans_param.ja_query_ref_;
  ObIArray<bool>& is_null_prop = trans_param.is_null_prop_;
  ObSelectStmt* subquery = NULL;
  ObSEArray<const ObRawExpr*, 4> vars;
  bool is_null_propagate = true;
  bool is_null_reject = false;
  bool is_scalar_aggr = false;
  if (OB_ISNULL(query_ref) || OB_ISNULL(subquery = query_ref->get_ref_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params are invalid", K(ret), K(query_ref), K(subquery));
  } else if (OB_FAIL(is_null_prop.prepare_allocate(subquery->get_select_item_size()))) {
    LOG_WARN("failed to prepare allocate case when array", K(ret));
  } else {
    is_scalar_aggr = subquery->is_scala_group_by();
  }
  // 1. scalar group by must return one row
  //    if join result is empty, we need deduce results for the scalar aggr items
  //    if join result is not empty, the scalar aggr items can be computed normally
  // 2. normal group by, return zero or one row
  //    if return zero, the main query gots null from the subquery
  //    if return one, the main query gots real values from the subquery
  for (int64_t i = 0; OB_SUCC(ret) && i < subquery->get_select_item_size(); ++i) {
    ObRawExpr* expr = NULL;
    is_null_prop.at(i) = false;
    if (!is_scalar_aggr) {
      is_null_prop.at(i) = true;
    } else if (OB_ISNULL(expr = subquery->get_select_item(i).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("select expr is null", K(ret), K(expr));
    } else if (OB_FAIL(extract_nullable_exprs(expr, vars))) {
      LOG_WARN("failed to extract nullable exprs", K(ret));
    } else if (vars.count() <= 0) {
      // do nothing
    } else if (OB_FAIL(ObTransformUtils::is_null_propagate_expr(expr, vars, is_null_prop.at(i)))) {
      LOG_WARN("failed to check is null propagate expr", K(ret));
    }
    if (OB_SUCC(ret) && !is_null_prop.at(i)) {
      is_null_propagate = false;
    }
  }
  if (OB_SUCC(ret) && is_null_propagate) {
    if (OB_FAIL(ObTransformUtils::has_null_reject_condition(filters, query_ref, is_null_reject))) {
      LOG_WARN("failed to check has null reject condition", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (aggr_first(trans_param.pullup_flag_) && trans_param.nested_conditions_.empty() && is_scalar_aggr) {
      // use inner join for non-correlated subquery
    } else if (!is_null_propagate || !is_null_reject) {
      trans_param.pullup_flag_ |= USE_OUTER_JOIN;
    }
  }
  return ret;
}

int ObTransformAggrSubquery::transform_child_stmt(ObDMLStmt* stmt, ObSelectStmt& subquery, TransformParam& param)
{
  int ret = OB_SUCCESS;
  ObIArray<ObRawExpr*>& nested_conditions = param.nested_conditions_;
  ObSEArray<ObRawExpr*, 4> old_group_exprs;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("transform context is invalid", K(ret), K(ctx_));
  } else if (OB_FAIL(old_group_exprs.assign(subquery.get_group_exprs()))) {
    LOG_WARN("failed to assign group exprs", K(ret));
  } else {
    subquery.get_group_exprs().reset();
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < nested_conditions.count(); ++i) {
    ObRawExpr* cond_expr = nested_conditions.at(i);
    bool left_is_correlated = false;
    if (OB_ISNULL(cond_expr) || OB_UNLIKELY(cond_expr->get_expr_type() != T_OP_EQ) ||
        OB_ISNULL(cond_expr->get_param_expr(0)) || OB_ISNULL(cond_expr->get_param_expr(1))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("nested expr is invalid", K(ret), K(cond_expr));
    } else if (OB_FAIL(ObOptimizerUtil::remove_item(subquery.get_condition_exprs(), cond_expr))) {
      LOG_WARN("failed to remove expr", K(ret));
    } else if (OB_FAIL(ObTransformUtils::is_correlated_expr(
                   cond_expr->get_param_expr(0), subquery.get_current_level() - 1, left_is_correlated))) {
      LOG_WARN("failed to check is correlated expr", K(ret));
    } else {
      // construct a pullup condition from a nested condition
      int64_t inner_param_id = left_is_correlated ? 1 : 0;
      ObRawExpr* group_expr = cond_expr->get_param_expr(inner_param_id);
      if (group_expr->has_flag(IS_CONST)) {
        // do nothing for const expr
      } else if (ObOptimizerUtil::find_equal_expr(subquery.get_group_exprs(), group_expr)) {
        // do nothing
      } else if (OB_FAIL(subquery.add_group_expr(group_expr))) {
        LOG_WARN("failed to add group expr", K(ret));
      } else if (OB_FAIL(ObTransformUtils::create_select_item(*ctx_->allocator_, group_expr, &subquery))) {
        LOG_WARN("failed to create select item", K(ret));
      } else if (param.not_null_expr_ == NULL) {
        param.not_null_expr_ = group_expr;
      }
    }
  }
  if (OB_SUCC(ret) && subquery.get_group_exprs().empty() && !old_group_exprs.empty()) {
    if (OB_FAIL(subquery.get_group_exprs().assign(old_group_exprs))) {
      /// select * from t1 where c1 > (select count(d1) from t2 group by 1.0);
      /// => select * from t1, (select count(d1) as aggr from t2 group by 1.0) V where c1 > V.aggr
      LOG_WARN("failed to assign group exprs", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(subquery.pullup_stmt_level())) {
      LOG_WARN("failed to pullup stmt level", K(ret));
    } else if (OB_FAIL(subquery.adjust_view_parent_namespace_stmt(stmt->get_parent_namespace_stmt()))) {
      LOG_WARN("failed to adjust view parent namespace stmt", K(ret));
    } else if (OB_FAIL(subquery.formalize_stmt(ctx_->session_info_))) {
      LOG_WARN("failed to formalize subquery", K(ret));
    }
  }
  return ret;
}

/**
 * @brief ObTransformAggrSubquery::transform_upper_stmt
 * 1. unnest the subquery, add it into the main query as a generated table
 * 2. given the output of generated table, deduce the results of the subquery,
 *    and replace the subquery expr with its results
 * 3. given the correlated conditions, use proper join method
 * @return
 */
int ObTransformAggrSubquery::transform_upper_stmt(ObDMLStmt& stmt, TransformParam& param)
{
  int ret = OB_SUCCESS;
  TableItem* view_table = NULL;
  ObSEArray<ObRawExpr*, 4> view_columns;
  ObSEArray<ObRawExpr*, 4> select_exprs;
  ObSEArray<ObRawExpr*, 4> real_values;
  ObQueryRefRawExpr* query_expr = param.ja_query_ref_;
  ObIArray<ObRawExpr*>& pullup_conds = param.nested_conditions_;
  ObSelectStmt* subquery = NULL;
  int64_t idx = OB_INVALID_INDEX;

  if (OB_ISNULL(ctx_) || OB_ISNULL(query_expr) || OB_ISNULL(subquery = query_expr->get_ref_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params are invalid", K(ret), K(ctx_), K(subquery));
  } else if (OB_FAIL(ObOptimizerUtil::remove_item(stmt.get_subquery_exprs(), query_expr))) {
    LOG_WARN("failed to remove subquery expr", K(ret));
  } else if (OB_FAIL(ObTransformUtils::add_new_table_item(ctx_, &stmt, subquery, view_table))) {
    LOG_WARN("failed to add new table item", K(ret));
  } else if (OB_ISNULL(view_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table item is invalid", K(ret));
  } else if (OB_FAIL(ObTransformUtils::create_columns_for_view(ctx_, *view_table, &stmt, view_columns))) {
    LOG_WARN("failed to create columns for view stmt", K(ret));
  } else if (OB_FAIL(subquery->get_select_exprs(select_exprs))) {
    LOG_WARN("failed to get select exprs", K(ret));
  } else if (ObOptimizerUtil::find_item(select_exprs, param.not_null_expr_, &idx)) {
    param.not_null_expr_ = view_columns.at(idx);
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(deduce_query_values(stmt, param, select_exprs, view_columns, real_values))) {
      LOG_WARN("failed to deduce subquery output", K(ret));
    } else if (OB_FAIL(stmt.replace_inner_stmt_expr(param.query_refs_, real_values))) {
      LOG_WARN("failed to replace inner stmt expr", K(ret));
    } else if (OB_FAIL(ObTransformUtils::replace_exprs(select_exprs, view_columns, pullup_conds))) {
      LOG_WARN("failed to replace pullup conditions", K(ret));
    } else if (OB_FAIL(transform_from_list(stmt, view_table, pullup_conds, param.pullup_flag_))) {
      LOG_WARN("failed to transform from list", K(ret));
      // 4. post process
    } else if (OB_FAIL(stmt.formalize_stmt(ctx_->session_info_))) {
      LOG_WARN("failed to formalize stmt", K(ret));
    } else if (stmt.is_update_stmt() && param.query_refs_.count() >= 2) {
      if (OB_FAIL(static_cast<ObUpdateStmt&>(stmt).check_assign())) {
        LOG_WARN("failed to check assign", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformAggrSubquery::deduce_query_values(ObDMLStmt& stmt, TransformParam& param,
    ObIArray<ObRawExpr*>& view_select, ObIArray<ObRawExpr*>& view_columns, ObIArray<ObRawExpr*>& real_values)
{
  int ret = OB_SUCCESS;
  ObRawExpr* not_null_expr = param.not_null_expr_;
  ObIArray<bool>& is_null_prop = param.is_null_prop_;
  if (OB_ISNULL(ctx_->session_info_) || OB_UNLIKELY(is_null_prop.count() > view_select.count()) ||
      OB_UNLIKELY(is_null_prop.count() > view_columns.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid session or invalid array size", K(ret));
  } else if (OB_FAIL(real_values.assign(view_columns))) {
    LOG_WARN("failed to assign real values", K(ret));
  } else if (use_outer_join(param.pullup_flag_)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < is_null_prop.count(); ++i) {
      // replace_columns_and_aggrs() may change expr result type, e.g.: sum() from ObNumberType
      // to ObNullType. This may cause operand implicit cast be added twice, so we erase it first.
      ObRawExpr* default_expr = NULL;
      if (is_null_prop.at(i)) {
        continue;
      } else if (OB_ISNULL(not_null_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("not null expr is null", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::copy_expr(
                     *ctx_->expr_factory_, view_select.at(i), default_expr, COPY_REF_DEFAULT))) {
        LOG_WARN("failed to copy select expr", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::erase_operand_implicit_cast(default_expr, default_expr))) {
        LOG_WARN("remove operand implicit cast failed", K(ret));
      } else if (OB_FAIL(replace_columns_and_aggrs(default_expr, ctx_))) {
        LOG_WARN("failed to replace variables", K(ret));
      }

      if (OB_SUCC(ret) && ctx_->session_info_->use_static_typing_engine()) {
        // After replace, result type of %default_expr may differ with original expr,
        // may cause unexpected implicit cast be added too, cast to original expr type first.
        if (OB_FAIL(default_expr->formalize(ctx_->session_info_))) {
          LOG_WARN("formalize expr failed", K(ret));
        } else if (OB_FAIL(ObRawExprUtils::try_add_cast_expr_above(ctx_->expr_factory_,
                       ctx_->session_info_,
                       *default_expr,
                       view_select.at(i)->get_result_type(),
                       default_expr))) {
          LOG_WARN("try add cast expr failed", K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(ObTransformUtils::build_case_when_expr(
                stmt, not_null_expr, view_columns.at(i), default_expr, real_values.at(i), ctx_))) {
          LOG_WARN("failed to build case when expr", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTransformAggrSubquery::replace_columns_and_aggrs(ObRawExpr*& expr, ObTransformerCtx* ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr) || OB_ISNULL(ctx) || OB_ISNULL(ctx->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret), K(expr), K(ctx));
  } else if (expr->get_expr_type() == T_FUN_COUNT) {
    ObConstRawExpr* const_zero = NULL;
    if (OB_FAIL(ObTransformUtils::build_const_expr_for_count(*ctx->expr_factory_, 0, const_zero))) {
      LOG_WARN("failed to replace count with 0", K(ret));
    } else {
      expr = const_zero;
    }
  } else if (expr->is_column_ref_expr() || expr->is_aggr_expr()) {
    // replace with NULL
    if (OB_FAIL(ObRawExprUtils::build_null_expr(*ctx->expr_factory_, expr))) {
      LOG_WARN("failed to replace variable with null", K(ret));
    }
  } else if (expr->has_flag(CNT_COLUMN) || expr->has_flag(CNT_AGG)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(replace_columns_and_aggrs(expr->get_param_expr(i), ctx)))) {
        LOG_WARN("failed to replace variables for param", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformAggrSubquery::transform_from_list(
    ObDMLStmt& stmt, TableItem* view_table_item, const ObIArray<ObRawExpr*>& joined_conds, const int64_t pullup_flag)
{
  int ret = OB_SUCCESS;
  if (!use_outer_join(pullup_flag)) {
    // create inner join for null-reject condition
    if (OB_FAIL(stmt.add_from_item(view_table_item->table_id_, false))) {
      LOG_WARN("failed to push back from item", K(ret));
    } else if (OB_FAIL(stmt.add_condition_exprs(joined_conds))) {
      LOG_WARN("failed to add condition exprs", K(ret));
    }
  } else {
    // create outer join
    TableItem* joined_table = NULL;
    if (OB_FAIL(ObTransformUtils::merge_from_items_as_inner_join(ctx_, stmt, joined_table))) {
      LOG_WARN("failed to merge from items as inner join", K(ret));
    } else if (OB_FAIL(ObTransformUtils::add_new_joined_table(
                   ctx_, stmt, LEFT_OUTER_JOIN, joined_table, view_table_item, joined_conds, joined_table))) {
      LOG_WARN("failed to add new joined table", K(ret));
    } else if (OB_ISNULL(joined_table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("joined table is null", K(ret));
    } else if (OB_FAIL(stmt.add_from_item(joined_table->table_id_, true))) {
      LOG_WARN("failed to add from item", K(ret));
    }
  }
  return ret;
}

int ObTransformAggrSubquery::extract_nullable_exprs(const ObRawExpr* expr, ObIArray<const ObRawExpr*>& vars)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (expr->is_column_ref_expr() || (expr->is_aggr_expr() && expr->get_expr_type() != T_FUN_COUNT)) {
    if (OB_FAIL(vars.push_back(expr))) {
      LOG_WARN("failed to push back expr", K(ret));
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(extract_nullable_exprs(expr->get_param_expr(i), vars)))) {
        LOG_WARN("failed to extract nullable expr", K(ret));
      }
    }
  }
  return ret;
}

/**
 * @brief ObTransformAggrSubquery::do_join_first_transform
 * STEP 1: find a valid subquery, choose the transformation method
 * STEP 2: create a spj from the origin stmt
 * STEP 3: rewrite the spj
 * @return
 */
int ObTransformAggrSubquery::transform_with_join_first(ObDMLStmt*& stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  bool is_valid = false;
  int64_t query_num = 0;
  ObDMLStmt* target_stmt = stmt;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null stmt", K(ret), K(stmt));
  } else if (OB_FAIL(check_stmt_valid(*stmt, is_valid))) {
    LOG_WARN("failed to check stmt valid", K(ret));
  } else if (is_valid) {
    query_num = stmt->get_subquery_expr_size();
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < query_num; ++i) {
    TransformParam param;
    ObSelectStmt* view_stmt = NULL;
    ObRawExpr* root_expr = NULL;
    bool post_group_by = false;
    if (OB_FAIL(get_trans_param(*target_stmt, param, root_expr, post_group_by))) {
      LOG_WARN("failed to find trans params", K(ret));
    } else if (NULL == root_expr) {
      break;
    } else if (OB_FAIL(fill_query_refs(target_stmt, root_expr, param))) {
      LOG_WARN("failed to fill query refs", K(ret));
    } else if (OB_FAIL(get_trans_view(*target_stmt, view_stmt, root_expr, post_group_by))) {
      LOG_WARN("failed to get transform view", K(ret));
    } else if (OB_UNLIKELY(!ObOptimizerUtil::find_item(view_stmt->get_subquery_exprs(), param.ja_query_ref_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the subquery is not found in view stmt", K(ret));
    } else if (OB_FAIL(do_join_first_transform(*view_stmt, param, root_expr))) {
      LOG_WARN("failed to do join first transform", K(ret));
    } else {
      target_stmt = view_stmt;
      trans_happened = true;
    }
  }
  return ret;
}

/**
 * @brief ObTransformAggrSubquery::get_trans_param
 * process subqueries from `where, group by, having, select, update assign`
 * @return
 */
int ObTransformAggrSubquery::get_trans_param(
    ObDMLStmt& stmt, TransformParam& param, ObRawExpr*& root_expr, bool& post_group_by)
{
  int ret = OB_SUCCESS;
  ObSEArray<TransformParam, 4> params;
  ObSEArray<ObRawExpr*, 4> pre_group_by_exprs;   // where, group exprs
  ObSEArray<ObRawExpr*, 4> post_group_by_exprs;  // having, select, assign values exprs
  ObSEArray<ObRawExpr*, 4> invalid_list;
  bool has_rownum = false;
  if (OB_FAIL(pre_group_by_exprs.assign(stmt.get_condition_exprs()))) {
    LOG_WARN("failed to assign conditions", K(ret));
  } else if (OB_FAIL(stmt.has_rownum(has_rownum))) {
    LOG_WARN("failed to check has rownum", K(ret));
  } else if (has_rownum) {
    // do nothing
  } else if (stmt.is_select_stmt()) {
    ObSelectStmt& sel_stmt = static_cast<ObSelectStmt&>(stmt);
    if (OB_FAIL(append(pre_group_by_exprs, sel_stmt.get_group_exprs()))) {
      LOG_WARN("failed to append group by exprs", K(ret));
    } else if (OB_FAIL(append(pre_group_by_exprs, sel_stmt.get_aggr_items()))) {
      LOG_WARN("failed to append aggr items", K(ret));
    } else if (sel_stmt.has_rollup() || sel_stmt.is_scala_group_by()) {
      // do nothing
      // 1. there is no group by clause, we can directly pullup subqueries
      // 1. there is normal group by, we can create spj view with group-by pushed down
    } else if (OB_FAIL(append(post_group_by_exprs, sel_stmt.get_having_exprs()))) {
      LOG_WARN("failed to append having exprs", K(ret));
    } else if (OB_FAIL(sel_stmt.get_select_exprs(post_group_by_exprs))) {
      LOG_WARN("failed to get select exprs", K(ret));
    }
  } else if (stmt.is_update_stmt()) {
    ObUpdateStmt& upd_stmt = static_cast<ObUpdateStmt&>(stmt);
    if (OB_FAIL(upd_stmt.get_assign_values(pre_group_by_exprs))) {
      LOG_WARN("failed to get assign values", K(ret));
    }
  }
  int64_t pre_count = pre_group_by_exprs.count();
  int64_t post_count = post_group_by_exprs.count();
  for (int64_t i = 0; OB_SUCC(ret) && NULL == root_expr && i < pre_count + post_count; ++i) {
    params.reset();
    post_group_by = (i >= pre_count);
    ObRawExpr* expr = !post_group_by ? pre_group_by_exprs.at(i) : post_group_by_exprs.at(i - pre_count);
    ObSEArray<ObRawExpr*, 4> filters;
    bool is_filter = check_is_filter(stmt, expr);
    if (is_exists_op(expr->get_expr_type()) && !is_filter) {
      // only rewrite [not] exists in where or having
    } else if (OB_FAIL(gather_transform_params(expr, expr, JOIN_FIRST, params))) {
      LOG_WARN("failed to gather transform params", K(ret));
    } else if (OB_FAIL(get_filters(stmt, expr, filters))) {
      LOG_WARN("failed to get filters", K(ret));
    }
    for (int64_t j = 0; OB_SUCC(ret) && NULL == root_expr && j < params.count(); ++j) {
      bool is_valid = true;
      if (ObOptimizerUtil::find_item(invalid_list, params.at(j).ja_query_ref_)) {
        is_valid = false;
      } else if (is_exists_op(expr->get_expr_type())) {
        if (OB_FAIL(choose_pullup_method_for_exists(params.at(j).ja_query_ref_,
                params.at(j).pullup_flag_,
                T_OP_NOT_EXISTS == expr->get_expr_type(),
                is_valid))) {
          LOG_WARN("failed to choose pullup method for exists", K(ret));
        }
      } else {
        if (OB_FAIL(choose_pullup_method(filters, params.at(j)))) {
          LOG_WARN("failed to choose pullup method", K(ret));
        }
      }
      if (OB_SUCC(ret) && is_valid && use_outer_join(params.at(j).pullup_flag_)) {
        if (OB_FAIL(check_can_use_outer_join(params.at(j), is_valid))) {
          LOG_WARN("failed to check use outer join", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (!is_valid) {
          if (OB_FAIL(invalid_list.push_back(params.at(j).ja_query_ref_))) {
            LOG_WARN("failed to push back invalid subquery", K(ret));
          }
        } else {
          root_expr = expr;
          post_group_by = (i >= pre_count);
          param = params.at(j);
        }
      }
    }
  }
  return ret;
}

int ObTransformAggrSubquery::get_trans_view(
    ObDMLStmt& stmt, ObSelectStmt*& view_stmt, ObRawExpr* root_expr, bool post_group_by)
{
  int ret = OB_SUCCESS;
  bool has_rownum = false;
  bool has_groupby = false;
  if (OB_FAIL(stmt.has_rownum(has_rownum))) {
    LOG_WARN("failed to check has rownum", K(ret));
  } else if (stmt.is_select_stmt() && static_cast<ObSelectStmt&>(stmt).has_group_by()) {
    has_groupby = true;
  }
  if (OB_SUCC(ret)) {
    TableItem* table = NULL;
    if (!has_rownum && !has_groupby && stmt.is_select_stmt()) {
      view_stmt = static_cast<ObSelectStmt*>(&stmt);
    } else if (OB_FAIL(ObTransformUtils::create_simple_view(ctx_, &stmt, view_stmt))) {
      LOG_WARN("failed to create simple view", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && NULL == table && i < stmt.get_table_size(); ++i) {
        if (OB_ISNULL(table = stmt.get_table_item(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("table item is null", K(ret));
        } else if (table->ref_query_ != view_stmt) {
          table = NULL;
        }
      }
      if (OB_SUCC(ret) && stmt.is_select_stmt()) {
        if (has_groupby && post_group_by) {
          if (OB_FAIL(ObTransformUtils::push_down_groupby(ctx_, static_cast<ObSelectStmt*>(&stmt), table))) {
            LOG_WARN("failed to push down group by", K(ret));
          } else {
            view_stmt = static_cast<ObSelectStmt*>(&stmt);
          }
        }
      }
      if (OB_SUCC(ret) && stmt.is_update_stmt()) {
        if (root_expr->has_flag(CNT_ALIAS)) {
          ObAliasRefRawExpr* alias = NULL;
          if (OB_FAIL(ObRawExprUtils::find_alias_expr(root_expr, alias))) {
            LOG_WARN("failed to find alias expr", K(ret));
          } else if (OB_FAIL(ObTransformUtils::push_down_vector_assign(
                         ctx_, static_cast<ObUpdateStmt*>(&stmt), alias, table))) {
            LOG_WARN("failed to push down vector query", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObTransformAggrSubquery::check_stmt_valid(ObDMLStmt& stmt, bool& is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  int32_t bit_id = OB_INVALID_INDEX;
  ObRelIds output_rel_ids;
  if (stmt.is_set_stmt() || stmt.is_hierarchical_query() || !stmt.is_sel_del_upd()) {
    is_valid = false;
  } else if (OB_FAIL(ObTransformUtils::get_from_tables(stmt, output_rel_ids))) {
    LOG_WARN("failed to get output rel ids", K(ret));
  }
  // check all output table is basic table
  // TODO check whether a view stmt has unique keys
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < stmt.get_table_items().count(); ++i) {
    const TableItem* cur_table = stmt.get_table_item(i);
    if (OB_ISNULL(cur_table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null table item", K(ret));
    } else if (OB_UNLIKELY(OB_INVALID_INDEX == (bit_id = stmt.get_table_bit_index(cur_table->table_id_)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid table bit index", K(ret), K(cur_table->table_id_), K(bit_id));
    } else if (output_rel_ids.has_member(bit_id) && !cur_table->is_basic_table()) {
      is_valid = false;
    }
  }
  return ret;
}

/**
 * @brief ObTransformAggrSubquery::check_subquery_validity
 *  check the valiaidty of the subquery
 */
int ObTransformAggrSubquery::check_subquery_validity(
    ObSelectStmt* subquery, const bool is_vector_assign, const bool in_exists, bool& is_valid)
{
  int ret = OB_SUCCESS;
  bool has_rownum = false;
  int32_t stmt_level = -1;
  is_valid = true;
  ObRawExpr* limit_expr = NULL;
  bool has_correlated_cond = false;
  bool has_ref_assign_user_var = false;
  ObSEArray<ObRawExpr*, 4> nested_conditions;
  // 1. check stmt components
  if (OB_ISNULL(subquery)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("subquery is null", K(ret));
  } else if (subquery->get_stmt_hint().enable_no_unnest()) {
    is_valid = false;
    LOG_TRACE("has no unnest hint.", K(is_valid));
  } else if (subquery->is_eliminated()) {
    is_valid = false;
    LOG_TRACE("subquery is eliminated", K(subquery->is_eliminated()));
  } else if (subquery->get_group_expr_size() > 0 || subquery->has_rollup() || subquery->has_window_function() ||
             subquery->has_sequence() || subquery->has_set_op() || subquery->is_hierarchical_query()) {
    is_valid = false;
    LOG_TRACE("invalid subquery", K(is_valid), K(*subquery));
  } else if (!subquery->is_scala_group_by()) {
    is_valid = false;
    LOG_TRACE("subquery is not scalar group by", K(is_valid));
  } else if (in_exists) {
    if (!subquery->has_having()) {
      is_valid = false;
      LOG_TRACE("invalid [not] exists subquery", K(is_valid), K(*subquery));
    } else if (OB_FAIL(check_subquery_having(*subquery, is_valid))) {
      LOG_WARN("failed to check subquery having", K(ret));
    } else if (!is_valid) {
      LOG_TRACE("invalid having clause", K(is_valid));
    } else if (!subquery->has_limit()) {
      /*do nothing*/
    } else if (OB_ISNULL(limit_expr = subquery->get_limit_expr()) ||
               OB_UNLIKELY(T_INT != limit_expr->get_expr_type())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected limit expr", K(ret));
    } else if (0 == static_cast<ObConstRawExpr*>(limit_expr)->get_value().get_int()) {
      is_valid = false;
      LOG_TRACE("get limit 0 in [not] exists expr", K(is_valid));
    }
  } else {
    if (subquery->has_having() || subquery->has_limit()) {
      is_valid = false;
      LOG_TRACE("invalid subquery", K(is_valid), K(*subquery));
    } else if (OB_FAIL(check_subquery_select(*subquery, is_valid))) {
      LOG_WARN("failed to check subquery select", K(ret));
    }
  }
  if (OB_FAIL(ret) || !is_valid) {
  } else if (OB_FAIL(subquery->has_rownum(has_rownum))) {
    LOG_WARN("failed to check subquery has rownum", K(ret));
  } else if (has_rownum) {
    is_valid = false;
    LOG_TRACE("has rownum expr", K(is_valid));
  } else if (OB_FAIL(subquery->has_ref_assign_user_var(has_ref_assign_user_var))) {
    LOG_WARN("failed to check stmt has assignment ref user var", K(ret));
  } else if (has_ref_assign_user_var) {
    is_valid = false;
    LOG_TRACE("has assignment ref user variable", K(is_valid));
  } else if (!is_vector_assign && !in_exists && subquery->get_select_item_size() > 1) {
    is_valid = false;
    LOG_TRACE("select item size is invalid", K(is_vector_assign), K(subquery->get_select_item_size()));
    // 2. check the aggr item
  } else if (OB_FAIL(check_subquery_aggr_item(*subquery, is_valid))) {
    LOG_WARN("failed to check subquery aggregation item", K(ret));
  } else if (!is_valid) {
    // do nothing
  } else if (OB_FAIL(check_count_const_validity(*subquery, is_valid))) {
    LOG_WARN("failed to check is valid count const", K(ret));
  } else if (!is_valid) {
    LOG_TRACE("aggr item is invalid", K(is_valid));
    // never reach
    // 3. check from list is not correlated
  } else if (OB_FAIL(check_subquery_table_item(*subquery, is_valid))) {
    LOG_WARN("failed to check subquery table item", K(ret));
  } else if (!is_valid) {
    // do nothing
    // 4. check correlated join on contiditons
  } else if (OB_FAIL(check_subquery_on_conditions(*subquery, is_valid))) {
    LOG_WARN("failed to check subquery on conditions", K(ret));
  } else {
    stmt_level = subquery->get_current_level();
  }
  // 5. check correlated join contiditons
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < subquery->get_condition_size(); ++i) {
    ObRawExpr* cond = subquery->get_condition_expr(i);
    if (OB_ISNULL(cond)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("condition expr is null", K(ret));
    } else if (IS_COMMON_COMPARISON_OP(cond->get_expr_type()) && T_OP_NSEQ != cond->get_expr_type() &&
               cond->get_expr_levels().has_member(stmt_level - 1) && cond->get_expr_levels().has_member(stmt_level)) {
      if (cond->has_flag(CNT_SUB_QUERY)) {
        is_valid = false;
        LOG_TRACE("is not valid equal or common comparsion correlation", K(i), K(*cond));
      } else if (OB_FAIL(nested_conditions.push_back(cond))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else {
        has_correlated_cond = true;
      }
    }
  }
  /**
   * for count(const), transfrom is valid if one of following condition is satisfied
   *  1. from item only has one basic table
   *  2. one side of correlated condition only has subquery's expr
   */
  if (!has_correlated_cond) {
    is_valid = false;
    LOG_TRACE("no correlated common comparsion condition found", K(is_valid), K(has_correlated_cond));
  }
  return ret;
}

/**
 * @brief check the select item, and the subquery comparison operator
 *        determine the join method
 **/
int ObTransformAggrSubquery::choose_pullup_method_for_exists(
    ObQueryRefRawExpr* query_ref, int64_t& pullup_flag, const bool with_not, bool& is_valid)
{
  int ret = OB_SUCCESS;
  ObSelectStmt* subquery = NULL;
  ObSEArray<const ObRawExpr*, 4> vars;
  is_valid = true;
  if (OB_ISNULL(query_ref) || OB_ISNULL(subquery = query_ref->get_ref_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params are invalid", K(ret), K(query_ref), K(subquery));
  } else if (with_not) {
    pullup_flag |= USE_OUTER_JOIN;
  } else {
    // use outer join if all having expr is not null reject
    bool all_not_null_reject = true;
    for (int64_t i = 0; OB_SUCC(ret) && all_not_null_reject && i < subquery->get_having_expr_size(); ++i) {
      bool is_null_reject = false;
      vars.reuse();
      ObRawExpr* cur_expr = subquery->get_having_exprs().at(i);
      if (OB_ISNULL(cur_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("having expr is null", K(ret));
      } else if (!(IS_COMMON_COMPARISON_OP(cur_expr->get_expr_type()) && T_OP_NSEQ != cur_expr->get_expr_type())) {
        is_valid = false;
      } else if (OB_FAIL(extract_nullable_exprs(cur_expr, vars))) {
        LOG_WARN("failed to extract nullable exprs", K(ret));
      } else if (vars.count() <= 0) {
        // do nothing
      } else if (OB_FAIL(ObTransformUtils::is_null_reject_condition(cur_expr, vars, is_null_reject))) {
        LOG_WARN("failed to check is null reject condition", K(ret));
      }
      if (OB_SUCC(ret) && is_null_reject) {
        all_not_null_reject = false;
      }
    }
    if (OB_SUCC(ret) && all_not_null_reject) {
      pullup_flag |= USE_OUTER_JOIN;
    }
  }
  return ret;
}

int ObTransformAggrSubquery::do_join_first_transform(
    ObSelectStmt& select_stmt, TransformParam& trans_param, ObRawExpr* root_expr)
{
  int ret = OB_SUCCESS;
  ObQueryRefRawExpr* query_ref_expr = NULL;
  ObSelectStmt* subquery = NULL;
  ObRawExprFactory* expr_factory = NULL;
  ObRelIds from_tables;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->session_info_) || OB_ISNULL(root_expr) ||
      OB_ISNULL(expr_factory = ctx_->expr_factory_) || OB_ISNULL(query_ref_expr = trans_param.ja_query_ref_) ||
      OB_ISNULL(subquery = trans_param.ja_query_ref_->get_ref_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret), K(ctx_), K(root_expr), K(query_ref_expr), K(subquery));
  } else if (OB_FAIL(ObTransformUtils::get_from_tables(select_stmt, from_tables))) {
    LOG_WARN("failed to get output rel ids", K(ret));
  } else if (OB_FAIL(get_unique_keys(select_stmt, select_stmt.get_group_exprs()))) {
    LOG_WARN("failed to get unique exprs", K(ret));
  }
  if (OB_SUCC(ret) && trans_param.not_null_expr_ != NULL) {
    for (int64_t i = 0; OB_SUCC(ret) && i < subquery->get_aggr_item_size(); ++i) {
      if (OB_FAIL(replace_count_const(subquery->get_aggr_item(i), trans_param.not_null_expr_))) {
        LOG_WARN("failed to replace count const expr", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObOptimizerUtil::remove_item(select_stmt.get_subquery_exprs(), query_ref_expr))) {
      LOG_WARN("failed to remove query ref expr", K(ret));
    } else if (is_exists_op(root_expr->get_expr_type())) {
      bool need_lnnvl = root_expr->get_expr_type() == T_OP_NOT_EXISTS;
      if (OB_FAIL(ObOptimizerUtil::remove_item(select_stmt.get_condition_exprs(), root_expr))) {
        LOG_WARN("failed to remove exprs", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < subquery->get_having_expr_size(); ++i) {
        ObRawExpr* cond = subquery->get_having_exprs().at(i);
        if (need_lnnvl && OB_FAIL(ObRawExprUtils::build_lnnvl_expr(*expr_factory, cond, cond))) {
          LOG_WARN("failed to build lnnvl expr", K(ret));
        } else if (OB_FAIL(select_stmt.add_having_expr(cond))) {
          LOG_WARN("failed to add having condition", K(ret));
        }
      }
    } else {
      ObSEArray<ObRawExpr*, 4> select_exprs;
      if (OB_FAIL(subquery->get_select_exprs(select_exprs))) {
        LOG_WARN("failed to get select exprs", K(ret));
      } else if (OB_FAIL(select_stmt.replace_inner_stmt_expr(trans_param.query_refs_, select_exprs))) {
        LOG_WARN("failed to update query ref value expr", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    // pullup subquery component
    subquery->set_eliminated(true);
    if (OB_FAIL(append(select_stmt.get_table_items(), subquery->get_table_items()))) {
      LOG_WARN("failed to append table items", K(ret));
    } else if (OB_FAIL(transform_from_list(select_stmt, *subquery, trans_param.pullup_flag_))) {
      LOG_WARN("failed to transform from list", K(ret));
    } else if (OB_FAIL(append(select_stmt.get_column_items(), subquery->get_column_items()))) {
      LOG_WARN("failed to append column items", K(ret));
    } else if (OB_FAIL(append(select_stmt.get_part_exprs(), subquery->get_part_exprs()))) {
      LOG_WARN("failed to append part exprs", K(ret));
    } else if (OB_FAIL(append(select_stmt.get_deduced_exprs(), subquery->get_deduced_exprs()))) {
      LOG_WARN("failed to append deduced exprs", K(ret));
    } else if (OB_FAIL(append(select_stmt.get_aggr_items(), subquery->get_aggr_items()))) {
      LOG_WARN("failed to append aggr items", K(ret));
    } else if (OB_FAIL(append(select_stmt.get_subquery_exprs(), subquery->get_subquery_exprs()))) {
      LOG_WARN("failed to append subquery exprs", K(ret));
    } else if (OB_FAIL(subquery->pullup_stmt_level())) {
      LOG_WARN("failed to pullup subquery", K(ret));
    } else if (OB_FAIL(subquery->adjust_view_parent_namespace_stmt(select_stmt.get_parent_namespace_stmt()))) {
      LOG_WARN("failed to adjust view parent namespace stmt", K(ret));
    } else if (OB_FAIL(select_stmt.rebuild_tables_hash())) {
      LOG_WARN("failed to rebuild table hash", K(ret));
    } else if (OB_FAIL(select_stmt.update_column_item_rel_id())) {
      LOG_WARN("failed to update colun item rel id", K(ret));
    } else if (OB_FAIL(append(select_stmt.get_semi_infos(), subquery->get_semi_infos()))) {
      LOG_WARN("failed to append semi infos", K(ret));
    } else if (OB_FAIL(select_stmt.formalize_stmt(ctx_->session_info_))) {
      LOG_WARN("failed to formalize stmt", K(ret));
    } else if (OB_FAIL(rebuild_conditon(select_stmt))) {
      LOG_WARN("failed to rebuild condition", K(ret));
    }
  }
  return ret;
}

int ObTransformAggrSubquery::get_unique_keys(ObDMLStmt& stmt, ObIArray<ObRawExpr*>& pkeys)
{
  int ret = OB_SUCCESS;
  ObRelIds from_tables;
  ObSEArray<ObRawExpr*, 2> tmp_pkeys;
  int32_t bit_id = OB_INVALID_INDEX;
  if (OB_FAIL(ObTransformUtils::get_from_tables(stmt, from_tables))) {
    LOG_WARN("failed to get from table set", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_table_items().count(); ++i) {
    tmp_pkeys.reuse();
    TableItem* cur_table = stmt.get_table_item(i);
    if (OB_ISNULL(cur_table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null table item", K(ret));
    } else if (OB_UNLIKELY(OB_INVALID_INDEX == (bit_id = stmt.get_table_bit_index(cur_table->table_id_)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid table bit index", K(ret), K(cur_table->table_id_), K(bit_id));
    } else if (!from_tables.has_member(bit_id)) {
      // do nothing
    } else if (cur_table->is_basic_table()) {
      if (OB_FAIL(ObTransformUtils::generate_unique_key(ctx_, &stmt, cur_table, tmp_pkeys))) {
        LOG_WARN("failed to generate unique key", K(ret));
      } else if (OB_FAIL(append(pkeys, tmp_pkeys))) {
        LOG_WARN("failed to append pkeys", K(ret));
      }
    } else if (cur_table->is_generated_table()) {
      ObSelectStmt* view_stmt = NULL;
      ObSEArray<ObRawExpr*, 4> select_list;
      ObSEArray<ObRawExpr*, 4> column_list;
      ObSEArray<ObRawExpr*, 4> group_keys;
      if (OB_ISNULL(view_stmt = cur_table->ref_query_) || OB_UNLIKELY(0 == view_stmt->get_group_expr_size())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("view is not a normal group by", K(ret), K(view_stmt));
      } else if (OB_FAIL(ObRawExprUtils::copy_exprs(
                     *ctx_->expr_factory_, view_stmt->get_group_exprs(), group_keys, COPY_REF_DEFAULT))) {
        LOG_WARN("failed to copy exprs", K(ret));
      } else if (OB_FAIL(stmt.get_view_output(*cur_table, select_list, column_list))) {
        LOG_WARN("failed to get view output", K(ret));
      } else if (OB_FAIL(ObTransformUtils::replace_exprs(select_list, column_list, group_keys))) {
        LOG_WARN("failed to replace group keys", K(ret));
      } else if (OB_FAIL(append(pkeys, group_keys))) {
        LOG_WARN("failed to append group keys", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected table item", K(ret), K(*cur_table));
    }
  }
  return ret;
}

/*
 *  merge from table of parent stmt and subquery stmt
 * */
int ObTransformAggrSubquery::transform_from_list(ObDMLStmt& stmt, ObSelectStmt& subquery, const int64_t pullup_flag)
{
  int ret = OB_SUCCESS;
  if (!use_outer_join(pullup_flag)) {
    // create inner join for null-reject condition
    if (OB_FAIL(append(stmt.get_joined_tables(), subquery.get_joined_tables()))) {
      LOG_WARN("failed to append joined tables", K(ret));
    } else if (OB_FAIL(append(stmt.get_from_items(), subquery.get_from_items()))) {
      LOG_WARN("failed to push back from items", K(ret));
    } else if (OB_FAIL(append(stmt.get_condition_exprs(), subquery.get_condition_exprs()))) {
      LOG_WARN("failed to push back condition exprs", K(ret));
    }
  } else {
    // create outer join
    TableItem* joined_table_main = NULL;
    TableItem* joined_table_sub = NULL;
    if (OB_FAIL(ObTransformUtils::merge_from_items_as_inner_join(ctx_, stmt, joined_table_main))) {
      LOG_WARN("failed to merge from items as inner join", K(ret));
    } else if (OB_FAIL(ObTransformUtils::merge_from_items_as_inner_join(ctx_, subquery, joined_table_sub))) {
      LOG_WARN("failed to merge from items as inner join", K(ret));
    } else if (OB_FAIL(ObTransformUtils::add_new_joined_table(ctx_,
                   stmt,
                   LEFT_OUTER_JOIN,
                   joined_table_main,
                   joined_table_sub,
                   subquery.get_condition_exprs(),
                   joined_table_main))) {
      LOG_WARN("failed to add new joined table", K(ret));
    } else if (OB_ISNULL(joined_table_main)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("joined table is null", K(ret));
    } else if (OB_FAIL(stmt.add_from_item(joined_table_main->table_id_, true))) {
      LOG_WARN("failed to add from item", K(ret));
    }
  }
  return ret;
}

int ObTransformAggrSubquery::rebuild_conditon(ObSelectStmt& stmt)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 8> where_conditions;
  if (OB_FAIL(where_conditions.assign(stmt.get_condition_exprs()))) {
    LOG_WARN("failed to assign where conditions", K(ret));
  }
  stmt.get_condition_exprs().reuse();
  for (int64_t i = 0; OB_SUCC(ret) && i < where_conditions.count(); ++i) {
    ObRawExpr* cur_expr = NULL;
    if (OB_ISNULL(cur_expr = where_conditions.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("condition expr is null", K(ret));
    } else if (cur_expr->has_flag(CNT_AGG) || cur_expr->has_flag(CNT_SUB_QUERY)) {
      if (OB_FAIL(stmt.get_having_exprs().push_back(cur_expr))) {
        LOG_WARN("failed to push back cur expr", K(ret));
      }
    } else if (OB_FAIL(stmt.get_condition_exprs().push_back(cur_expr))) {
      LOG_WARN("failed to push back where condition", K(ret));
    }
  }
  return ret;
}

int ObTransformAggrSubquery::check_subquery_having(const ObSelectStmt& subquery, bool& is_valid)
{
  int ret = OB_SUCCESS;
  bool is_correlated = false;
  is_valid = true;
  // check having expr valid for [not] exists subquery
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < subquery.get_having_expr_size(); ++i) {
    const ObRawExpr* cur_expr = subquery.get_having_exprs().at(i);
    if (OB_ISNULL(cur_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null expr", K(ret));
    } else if (OB_FAIL(
                   ObTransformUtils::is_correlated_expr(cur_expr, subquery.get_current_level() - 1, is_correlated))) {
      LOG_WARN("failed to check is correlated expr", K(ret));
    } else if (is_correlated || !cur_expr->has_flag(CNT_AGG) || cur_expr->has_flag(CNT_SUB_QUERY)) {
      is_valid = false;
      LOG_TRACE("invalid select item", K(is_valid));
    }
  }
  return ret;
}

int ObTransformAggrSubquery::check_subquery_on_conditions(ObSelectStmt& subquery, bool& is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  ObSEArray<ObRawExpr*, 8> on_conditions;
  if (OB_FAIL(ObTransformUtils::get_on_conditions(subquery, on_conditions))) {
    LOG_WARN("failed to get on conditions", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < on_conditions.count(); ++i) {
    bool is_correlated = false;
    ObRawExpr* cond = NULL;
    if (OB_ISNULL(cond = on_conditions.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("condition expr is null", K(ret));
    } else if (OB_FAIL(ObTransformUtils::is_correlated_expr(cond, subquery.get_current_level() - 1, is_correlated))) {
      LOG_WARN("failed to check is correlated condition", K(ret));
    } else if (is_correlated) {
      is_valid = false;
    }
  }
  return ret;
}

int ObTransformAggrSubquery::check_subquery_aggr_item(const ObSelectStmt& subquery, bool& is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < subquery.get_aggr_item_size(); i++) {
    const ObAggFunRawExpr* agg_expr = subquery.get_aggr_item(i);
    if (OB_ISNULL(agg_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("aggr expr is null", K(ret));
    } else {
      const ObItemType type = agg_expr->get_expr_type();
      is_valid = (type == T_FUN_SUM || type == T_FUN_COUNT || type == T_FUN_MIN || type == T_FUN_MAX);
    }
  }
  return ret;
}

/**
 * @brief ObTransformAggrSubquery::check_count_const_validity
 * @param subquery should not contain const(null) for join-first pullup
 * @param is_valid
 * @return
 */
int ObTransformAggrSubquery::check_count_const_validity(const ObSelectStmt& subquery, bool& is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < subquery.get_aggr_item_size(); i++) {
    bool is_count_const = false;
    bool is_const_null = false;
    const ObAggFunRawExpr* agg_expr = subquery.get_aggr_item(i);
    if (OB_ISNULL(agg_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("aggr expr is null", K(ret));
    } else if (OB_FAIL(is_count_const_expr(agg_expr, is_count_const))) {
      LOG_WARN("failed to check is count const expr", K(ret));
    } else if (!is_count_const || agg_expr->get_param_count() == 0) {
      // do nothing
    } else if (OB_FAIL(is_const_null_value(subquery, agg_expr->get_param_expr(0), is_const_null))) {
      LOG_WARN("failed to check is count null expr", K(ret));
    } else if (is_const_null) {
      is_valid = false;
    }
  }
  return ret;
}

int ObTransformAggrSubquery::is_count_const_expr(const ObRawExpr* expr, bool& is_count_const)
{
  int ret = OB_SUCCESS;
  is_count_const = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null expr", K(ret));
  } else if (T_FUN_COUNT != expr->get_expr_type()) {
    // do nothing
  } else if (0 == expr->get_param_count()) {
    is_count_const = true;
    LOG_TRACE("expr is count(*)", K(*expr));
  } else if (OB_ISNULL(expr->get_param_expr(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null expr", K(ret));
  } else if (!expr->get_param_expr(0)->has_flag(CNT_COLUMN) && !expr->get_param_expr(0)->has_flag(CNT_SUB_QUERY)) {
    is_count_const = true;
    LOG_TRACE("expr is count(const)", K(*expr));
  }
  return ret;
}

int ObTransformAggrSubquery::is_const_null_value(
    const ObDMLStmt& stmt, const ObRawExpr* param_expr, bool& is_const_null)
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* plan_ctx = NULL;
  is_const_null = true;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->exec_ctx_) || OB_ISNULL(param_expr) ||
      OB_ISNULL(plan_ctx = ctx_->exec_ctx_->get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null expr", K(ret), K(ctx_), K(plan_ctx));
  } else if (!param_expr->is_const_expr()) {
    // something like: case when 1 then null else 1 end;
  } else if (T_NULL == param_expr->get_expr_type()) {
    // do nothing, null expr
  } else if (T_QUESTIONMARK == param_expr->get_expr_type()) {
    const ObObj& value = static_cast<const ObConstRawExpr*>(param_expr)->get_value();
    bool is_pre_param = false;
    if (ObTransformUtils::is_question_mark_pre_param(stmt, value.get_unknown(), is_pre_param)) {
      LOG_WARN("failed to check is question mark pre param", K(ret));
    } else if (OB_UNLIKELY(!is_pre_param)) {
      // do nothing pre-calc expr
    } else if (plan_ctx->get_param_store().at(value.get_unknown()).is_null()) {
      // do nothing null param expr
    } else {
      // not-null param
      is_const_null = false;
    }
  } else {
    // not-null const
    is_const_null = false;
  }
  return ret;
}

int ObTransformAggrSubquery::replace_count_const(ObAggFunRawExpr* agg_expr, ObRawExpr* not_null_expr)
{
  int ret = OB_SUCCESS;
  bool is_count_const = false;
  if (OB_ISNULL(agg_expr) || OB_ISNULL(not_null_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null expr", K(ret), K(agg_expr), K(not_null_expr));
  } else if (OB_FAIL(is_count_const_expr(agg_expr, is_count_const))) {
    LOG_WARN("failed to check is count const expr", K(ret));
  } else if (!is_count_const) {
    // do nothing
  } else if (0 == agg_expr->get_param_count()) {
    if (OB_FAIL(agg_expr->add_real_param_expr(not_null_expr))) {
      LOG_WARN("failed to add real param expr", K(ret));
    }
  } else {
    agg_expr->get_param_expr(0) = not_null_expr;
  }
  return ret;
}

/**
 * @brief check_can_use_outer_join
 * check if can use outer join for join fisrt ja rewrite
 * following case is not valid:
 * 1. child_stmt has semi join or subquery in where condition
 * 2. aggr item's parameter is null propagate
 * 3. if there is count(1), count(*), the subquery should have a not-null expr
 */
int ObTransformAggrSubquery::check_can_use_outer_join(TransformParam& param, bool& is_valid)
{
  int ret = OB_SUCCESS;
  ObRawExpr*& not_null_expr = param.not_null_expr_;
  ObSelectStmt* stmt = NULL;
  if (OB_ISNULL(param.ja_query_ref_) || OB_ISNULL(stmt = param.ja_query_ref_->get_ref_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid transform param", K(ret));
  } else {
    is_valid = stmt->get_semi_info_size() == 0;
  }
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < stmt->get_condition_size(); ++i) {
    if (OB_ISNULL(stmt->get_condition_expr(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("condition expr is null", K(ret));
    } else {
      is_valid = !stmt->get_condition_expr(i)->has_flag(CNT_SUB_QUERY);
    }
  }
  ObSEArray<const ObRawExpr*, 4> columns;
  if (OB_SUCC(ret) && is_valid) {
    ObSEArray<ObRawExpr*, 4> tmp;
    if (OB_FAIL(stmt->get_column_exprs(tmp))) {
      LOG_WARN("failed to get column exprs", K(ret));
    } else if (OB_FAIL(append(columns, tmp))) {
      LOG_WARN("failed to convert column array", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < stmt->get_aggr_item_size(); ++i) {
    bool is_count_const = false;
    ObRawExpr* aggr = stmt->get_aggr_item(i);
    if (OB_ISNULL(aggr = stmt->get_aggr_item(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("aggregation expr is invalid", K(ret));
    } else if (OB_FAIL(is_count_const_expr(aggr, is_count_const))) {
      LOG_WARN("failed to check is count const expr", K(ret));
    } else if (is_count_const) {
      if (not_null_expr != NULL) {
        // do nothing
      } else if (OB_FAIL(ObTransformUtils::find_not_null_expr(*stmt, not_null_expr, is_valid))) {
        LOG_WARN("failed to find not null expr", K(ret));
      }
    } else {
      bool is_null_prop = false;
      if (OB_FAIL(ObTransformUtils::is_null_propagate_expr(aggr->get_param_expr(0), columns, is_null_prop))) {
        LOG_WARN("failed to check is null propagate expr", K(ret));
      } else {
        is_valid = is_null_prop;
      }
    }
  }
  return ret;
}

int ObTransformAggrSubquery::fill_query_refs(ObDMLStmt* stmt, ObRawExpr* expr, TransformParam& param)
{
  int ret = OB_SUCCESS;
  param.query_refs_.reuse();
  if (OB_ISNULL(stmt) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params are invalid", K(ret), K(stmt), K(expr));
  } else if (expr->has_flag(CNT_ALIAS)) {
    if (OB_UNLIKELY(!stmt->is_update_stmt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("stmt is expected to be update", K(ret));
    } else if (OB_FAIL(static_cast<ObUpdateStmt*>(stmt)->get_vector_assign_values(
                   param.ja_query_ref_, param.query_refs_))) {
      LOG_WARN("failed to get vector assign values", K(ret));
    }
  } else {
    if (OB_FAIL(param.query_refs_.push_back(param.ja_query_ref_))) {
      LOG_WARN("failed to push back query refs", K(ret));
    }
  }
  return ret;
}

// 1. scalar group by
// 2. group by const expr
int ObTransformAggrSubquery::is_valid_group_by(const ObSelectStmt& subquery, bool& is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  if (subquery.is_scala_group_by()) {
    is_valid = true;
  } else if (subquery.get_group_expr_size() > 0 && subquery.get_rollup_expr_size() == 0) {
    // check is group by const
    is_valid = true;
    ObSEArray<ObRawExpr*, 4> const_exprs;
    if (OB_FAIL(ObOptimizerUtil::compute_const_exprs(
            subquery.get_condition_exprs(), subquery.get_current_level(), const_exprs))) {
      LOG_WARN("failed to compute const exprs", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < subquery.get_group_expr_size(); ++i) {
      if (ObOptimizerUtil::is_const_expr(subquery.get_group_exprs().at(i), subquery.get_current_level())) {
        // do nothing
      } else if (ObOptimizerUtil::find_item(const_exprs, subquery.get_group_exprs().at(i))) {
        // do nothing
      } else {
        is_valid = false;
      }
    }
  }
  LOG_TRACE("check single set query", K(is_valid), K(subquery.is_scala_group_by()));
  return ret;
}

bool ObTransformAggrSubquery::check_is_filter(const ObDMLStmt& stmt, const ObRawExpr* expr)
{
  bool bret = false;
  bret = ObOptimizerUtil::find_item(stmt.get_condition_exprs(), expr);
  if (!bret && stmt.is_select_stmt()) {
    bret = ObOptimizerUtil::find_item(static_cast<const ObSelectStmt&>(stmt).get_having_exprs(), expr);
  }
  return bret;
}

int ObTransformAggrSubquery::get_filters(ObDMLStmt& stmt, ObRawExpr* expr, ObIArray<ObRawExpr*>& filters)
{
  int ret = OB_SUCCESS;
  if (ObOptimizerUtil::find_item(stmt.get_condition_exprs(), expr)) {
    // is where
    if (OB_FAIL(filters.assign(stmt.get_condition_exprs()))) {
      LOG_WARN("failed to assign condition exprs", K(ret));
    }
  } else if (stmt.is_select_stmt()) {
    ObSelectStmt& sel_stmt = static_cast<ObSelectStmt&>(stmt);
    if (ObOptimizerUtil::find_item(sel_stmt.get_group_exprs(), expr) ||
        ObOptimizerUtil::find_item(sel_stmt.get_aggr_items(), expr) ||
        ObOptimizerUtil::find_item(sel_stmt.get_having_exprs(), expr)) {
      if (OB_FAIL(filters.assign(sel_stmt.get_having_exprs()))) {
        LOG_WARN("failed to find items", K(ret));
      }
    }
  }
  return ret;
}

/**
 * given a select subquery, if we can push the correlated condition onto the index, we do not rewrite
 */
int ObTransformAggrSubquery::extract_no_rewrite_select_exprs(ObDMLStmt*& stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (stmt->is_select_stmt() && stmt->get_subquery_expr_size() > 0) {
    ObSelectStmt* sel_stmt = static_cast<ObSelectStmt*>(stmt);
    for (int64_t i = 0; OB_SUCC(ret) && i < sel_stmt->get_select_item_size(); ++i) {
      if (OB_FAIL(extract_no_rewrite_expr(sel_stmt->get_select_item(i).expr_))) {
        LOG_WARN("failed to extract no rewrite expr", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformAggrSubquery::extract_no_rewrite_expr(ObRawExpr* expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr) || OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (expr->is_query_ref_expr()) {
    ObSelectStmt* subquery = static_cast<ObQueryRefRawExpr*>(expr)->get_ref_stmt();
    bool is_match = false;
    if (OB_ISNULL(subquery)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (subquery->get_stmt_hint().enable_unnest()) {
      // do nothing
    } else if (OB_FAIL(ObTransformUtils::check_subquery_match_index(ctx_, subquery, is_match))) {
      LOG_WARN("failed to check subquery match index", K(ret));
    } else if (!is_match) {
      // do nothing
    } else if (OB_FAIL(add_var_to_array_no_dup(no_rewrite_exprs_, expr))) {
      LOG_WARN("failed to add var to array no dup", K(ret));
    }
  } else if (expr->has_flag(CNT_SUB_QUERY)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(extract_no_rewrite_expr(expr->get_param_expr(i)))) {
        LOG_WARN("failed to extract no rewrite expr", K(ret));
      }
    }
  }
  return ret;
}
