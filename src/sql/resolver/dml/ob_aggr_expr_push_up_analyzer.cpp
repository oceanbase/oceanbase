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

#include "sql/resolver/dml/ob_aggr_expr_push_up_analyzer.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/resolver/dml/ob_select_resolver.h"
#include "common/ob_smart_call.h"

namespace oceanbase {
using namespace common;
namespace sql {
int ObAggrExprPushUpAnalyzer::analyze_and_push_up_aggr_expr(ObAggFunRawExpr* aggr_expr, ObAggFunRawExpr*& final_aggr)
{
  int ret = OB_SUCCESS;
  ObExprLevels child_aggr_expr_levels;
  if (OB_ISNULL(aggr_expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("aggr expr is null");
  } else if (OB_FAIL(aggr_columns_checker_.init(AGGR_HASH_BUCKET_SIZE))) {
    LOG_WARN("init aggr columns checker failed", K(ret));
  } else if (OB_FAIL(level_exprs_checker_.init(AGGR_HASH_BUCKET_SIZE))) {
    LOG_WARN("init level exprs checker failed", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < aggr_expr->get_param_count(); ++i) {
    ObRawExpr*& param_expr = aggr_expr->get_param_expr(i);
    if (OB_ISNULL(param_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("param expr is null");
    } else if (OB_FAIL(analyze_aggr_param_expr(param_expr, child_aggr_expr_levels))) {
      LOG_WARN("analyze aggr param expr failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    // push up aggr to final aggr stmt
    final_aggr = aggr_expr;
    ObSelectResolver* final_aggr_resolver = fetch_final_aggr_resolver(&cur_resolver_, get_final_aggr_level());
    if (NULL == final_aggr_resolver || child_aggr_expr_levels.has_member(final_aggr_resolver->get_current_level())) {
      ret = OB_ERR_INVALID_GROUP_FUNC_USE;
      LOG_WARN("no resolver can produce aggregate function");
    } else if (OB_FAIL(final_aggr_resolver->add_aggr_expr(final_aggr))) {
      LOG_WARN("add aggr to final resolver failed", K(ret));
    } else if (OB_FAIL(push_up_aggr_column(final_aggr_resolver->get_current_level()))) {
      LOG_WARN("push up aggr column failed", K(ret));
    } else if (OB_FAIL(adjust_query_level(final_aggr_resolver->get_current_level()))) {
      LOG_WARN("adjust query level failed", K(ret));
    } else if (OB_FAIL(push_up_subquery_in_aggr(*final_aggr_resolver))) {
      LOG_WARN("push up subquery in aggr failed", K(ret));
    }
  }
  return ret;
}

int ObAggrExprPushUpAnalyzer::analyze_aggr_param_exprs(
    ObIArray<ObRawExpr*>& param_exprs, ObExprLevels& aggr_expr_levels)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < param_exprs.count(); ++i) {
    ObRawExpr*& param_expr = param_exprs.at(i);
    if (OB_FAIL(analyze_aggr_param_expr(param_expr, aggr_expr_levels))) {
      LOG_WARN("analyze aggr param expr failed", K(ret));
    }
  }
  return ret;
}

int ObAggrExprPushUpAnalyzer::analyze_aggr_param_expr(ObRawExpr*& param_expr, ObExprLevels& aggr_expr_levels)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(param_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param expr in aggregate is null");
  } else if (param_expr->is_column_ref_expr()) {
    if (OB_FAIL(aggr_columns_checker_.add_expr(param_expr))) {
      LOG_WARN("add expr to aggr columns failed", K(ret));
    }
  }
  while (OB_SUCC(ret) && T_REF_ALIAS_COLUMN == param_expr->get_expr_type()) {
    if (OB_FAIL(aggr_columns_checker_.add_expr(param_expr))) {
      LOG_WARN("add param expr to aggr columns failed", K(ret));
    } else if (OB_ISNULL(param_expr = static_cast<ObAliasRefRawExpr*>(param_expr)->get_ref_expr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("alias ref expr is null");
    }
  }
  if (OB_SUCC(ret) && param_expr->get_expr_level() >= 0) {
    if (param_expr->is_aggr_expr()) {
      if (OB_FAIL(aggr_expr_levels.add_member(param_expr->get_expr_level()))) {
        LOG_WARN("failed to add aggr levels", K(ret));
      } else if (param_expr->get_expr_level() == cur_resolver_.get_current_level()) {
        ret = OB_ERR_INVALID_GROUP_FUNC_USE;
        LOG_WARN("aggregate nested in the same level", K(param_expr->get_expr_level()));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(level_exprs_checker_.add_expr(param_expr))) {
      LOG_WARN("add expr to level exprs failed", K(ret));
    }
  }
  if (OB_SUCC(ret) && !param_expr->is_aggr_expr()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < param_expr->get_param_count(); ++i) {
      ObRawExpr*& param = param_expr->get_param_expr(i);
      if (OB_ISNULL(param)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("param expr is null");
      } else if (OB_FAIL(SMART_CALL(analyze_aggr_param_expr(param, aggr_expr_levels)))) {
        LOG_WARN("analyze child expr failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && param_expr->is_query_ref_expr()) {
    ObQueryRefRawExpr* query_ref = static_cast<ObQueryRefRawExpr*>(param_expr);
    if (OB_FAIL(analyze_child_stmt(query_ref->get_ref_stmt(), aggr_expr_levels))) {
      LOG_WARN("analyze child stmt failed", K(ret));
    }
  }
  return ret;
}

int ObAggrExprPushUpAnalyzer::analyze_joined_table_exprs(JoinedTable& joined_table, ObExprLevels& aggr_expr_levels)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("check stack overflow failed", K(ret));
  } else if (OB_UNLIKELY(is_stack_overflow)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("stack is overflow", K(ret));
  } else if (OB_FAIL(analyze_aggr_param_exprs(joined_table.join_conditions_, aggr_expr_levels))) {
    LOG_WARN("analyze aggr param exprs failed", K(ret));
  } else if (OB_FAIL(analyze_aggr_param_exprs(joined_table.coalesce_expr_, aggr_expr_levels))) {
    LOG_WARN("analyze aggr param exprs failed", K(ret));
  } else {
    if (NULL != joined_table.left_table_ && joined_table.left_table_->is_joined_table()) {
      JoinedTable& left_table = static_cast<JoinedTable&>(*joined_table.left_table_);
      if (OB_FAIL(SMART_CALL(analyze_joined_table_exprs(left_table, aggr_expr_levels)))) {
        LOG_WARN("get left join table condition expr failed", K(ret));
      }
    }
    if (OB_SUCC(ret) && joined_table.right_table_ != NULL && joined_table.right_table_->is_joined_table()) {
      JoinedTable& right_table = static_cast<JoinedTable&>(*joined_table.right_table_);
      if (OB_FAIL(SMART_CALL(analyze_joined_table_exprs(right_table, aggr_expr_levels)))) {
        LOG_WARN("get right join table condition expr failed", K(ret));
      }
    }
  }
  return ret;
}

int ObAggrExprPushUpAnalyzer::analyze_child_stmt(ObSelectStmt* child_stmt, ObExprLevels& aggr_expr_levels)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(child_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child stmt is null");
  } else if (OB_FAIL(child_stmts_.push_back(child_stmt))) {
    LOG_WARN("add child stmt failed", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < child_stmt->get_select_item_size(); ++i) {
    SelectItem& select_item = child_stmt->get_select_item(i);
    if (OB_FAIL(analyze_aggr_param_expr(select_item.expr_, aggr_expr_levels))) {
      LOG_WARN("analyze aggr param expr failed", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < child_stmt->get_joined_tables().count(); ++i) {
    if (OB_ISNULL(child_stmt->get_joined_tables().at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("joined table is null", K(ret));
    } else if (OB_FAIL(analyze_joined_table_exprs(*child_stmt->get_joined_tables().at(i), aggr_expr_levels))) {
      LOG_WARN("analyze joined table exprs failed", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < child_stmt->get_order_item_size(); ++i) {
    OrderItem& order_item = child_stmt->get_order_item(i);
    if (OB_FAIL(analyze_aggr_param_expr(order_item.expr_, aggr_expr_levels))) {
      LOG_WARN("analyze aggr param expr failed", K(ret));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(analyze_aggr_param_exprs(child_stmt->get_group_exprs(), aggr_expr_levels))) {
    LOG_WARN("analyze aggr param expr failed", K(ret));
  }
  if (OB_SUCC(ret) && OB_FAIL(analyze_aggr_param_exprs(child_stmt->get_rollup_exprs(), aggr_expr_levels))) {
    LOG_WARN("analyze aggr param expr failed", K(ret));
  }
  if (OB_SUCC(ret) && OB_FAIL(analyze_aggr_param_exprs(child_stmt->get_having_exprs(), aggr_expr_levels))) {
    LOG_WARN("analyze aggr param expr failed", K(ret));
  }
  if (OB_SUCC(ret) && OB_FAIL(analyze_aggr_param_exprs(child_stmt->get_condition_exprs(), aggr_expr_levels))) {
    LOG_WARN("analyze aggr param expr failed", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < child_stmt->get_table_size(); ++i) {
    TableItem* table_item = child_stmt->get_table_item(i);
    if (OB_ISNULL(table_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table item is null");
    } else if (!table_item->is_generated_table()) {
      // do nothing
    } else if (OB_FAIL(analyze_child_stmt(table_item->ref_query_, aggr_expr_levels))) {
      LOG_WARN("analyze child stmt failed", K(ret));
    }
  }
  return ret;
}

int32_t ObAggrExprPushUpAnalyzer::get_final_aggr_level() const
{
  int32_t cur_aggr_level = cur_resolver_.get_current_level();
  int32_t final_aggr_level = -1;
  // According to MySQL pullup aggregate function principle, position of aggregate functions is
  // resulted from position of columns it depends on. Level of pullup is nearest upper position.
  // select c1 from t1 having c1>(select c1 from t2 having c1 >(select count(t1.c1) from t3))
  // count(t1.c1) is at first layer.
  // select c1 from t1 having c1>(select c1 from t2 having c1 >(select count(t1.c1 + t2.c1) from t3))
  // count(t1.c1) is at second layer.
  for (int32_t i = 0; i < aggr_columns_.count(); ++i) {
    ObRawExpr* aggr_column = aggr_columns_.at(i);
    int32_t column_level = -1;
    if (aggr_column != NULL && aggr_column->is_column_ref_expr()) {
      column_level = static_cast<ObColumnRefRawExpr*>(aggr_column)->get_expr_level();
    }
    if (aggr_column != NULL && T_REF_ALIAS_COLUMN == aggr_column->get_expr_type()) {
      column_level = static_cast<ObAliasRefRawExpr*>(aggr_column)->get_expr_level();
    }
    if (column_level >= 0 && column_level <= cur_aggr_level && column_level > final_aggr_level) {
      final_aggr_level = column_level;
    }
  }
  // in mysql mode if the final_aggr_level < 0, the aggr's params are const. e.g., count(const_expr);
  return final_aggr_level;
}

ObSelectResolver* ObAggrExprPushUpAnalyzer::fetch_final_aggr_resolver(
    ObDMLResolver* cur_resolver, int32_t final_aggr_level)
{
  ObSelectResolver* final_resolver = NULL;
  if (cur_resolver != NULL) {
    /*
     * For mysql mode, it always pull subquery up to compute
     * For oracle mode, if it is in having scope, it will pull subquery up to compute
     * if it is in order scope (and subquery does not appear in where scope), it will pull subquery up to compute
     */
    if (final_aggr_level >= 0 && cur_resolver->get_current_level() > final_aggr_level &&
        NULL != cur_resolver->get_parent_namespace_resolver() &&
        (share::is_mysql_mode() ||
            T_HAVING_SCOPE == cur_resolver->get_parent_namespace_resolver()->get_current_scope() ||
            (T_ORDER_SCOPE == cur_resolver->get_parent_namespace_resolver()->get_current_scope() &&
                T_WHERE_SCOPE != cur_resolver->get_current_scope()))) {
      if (cur_resolver->is_select_resolver() && static_cast<ObSelectResolver*>(cur_resolver)->is_in_set_query()) {
        // The current resolver is located in the set query identified by keywords such as union,
        // and the aggr function is no longer laid on the query for the upper layer of the union
        // For example:
        //   SELECT (SELECT MAX(t1.b) from t2 union select 1 from t2 where 12 <3) FROM t1 GROUP BY t1.a;
        // MAX(t1.b) refers to the attributes of the first layer, but the entire
        // expression of MAX(t1.b) remains in the left branch of the union
      } else {
        ObDMLResolver* next_resolver = cur_resolver->get_parent_namespace_resolver();
        final_resolver = fetch_final_aggr_resolver(next_resolver, final_aggr_level);
      }
    }
    if (NULL == final_resolver && cur_resolver->is_select_resolver()) {
      ObSelectResolver* select_resolver = static_cast<ObSelectResolver*>(cur_resolver);
      if (select_resolver->can_produce_aggr()) {
        final_resolver = select_resolver;
      } else if (share::is_mysql_mode() && final_aggr_level < 0) {
        /* 
         * in mysql, a const aggr_expr(e.g., count(const_expr)), belongs to the nearest legal level.
         *
         * select 1 from t1 where  (select 1 from t1 group by pk having  (select 1 from t1 where count(1)));
         * --> count(1)'s level is 1; it belongs to select 1 from t1 group by pk having xxx;
         *
         * select 1 from t1 group by pk having  (select 1 from dual where (select 1 from t1 where count(1)));
         * --> count(1)'s level is 0;
         */
        ObDMLResolver *next_resolver = cur_resolver->get_parent_namespace_resolver();
        if (NULL != next_resolver) {
          final_resolver = fetch_final_aggr_resolver(next_resolver, final_aggr_level);
        }
      }
    }
  }
  return final_resolver;
}

int ObAggrExprPushUpAnalyzer::push_up_aggr_column(int32_t final_aggr_level)
{
  int ret = OB_SUCCESS;
  int32_t old_aggr_level = cur_resolver_.get_current_level();
  for (int32_t i = 0; OB_SUCC(ret) && i < aggr_columns_.count(); ++i) {
    ObRawExpr* aggr_column = aggr_columns_.at(i);
    if (OB_ISNULL(aggr_column)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("aggr_column is null");
    } else if (aggr_column->is_column_ref_expr()) {
      ObColumnRefRawExpr* column_ref = static_cast<ObColumnRefRawExpr*>(aggr_column);
      int32_t column_level = column_ref->get_expr_level();
      if (column_level <= old_aggr_level && column_level != final_aggr_level) {
        // if aggregate functions cites column at same level,
        // these columns not constraint by only full group by or having.
        ObDMLResolver* cur_resolver = &cur_resolver_;
        // get the real resolver that produce this column
        for (; OB_SUCC(ret) && cur_resolver != NULL && cur_resolver->get_current_level() > column_level;
             cur_resolver = cur_resolver->get_parent_namespace_resolver()) {}
        if (cur_resolver != NULL && cur_resolver->is_select_resolver()) {
          ObSelectResolver* select_resolver = static_cast<ObSelectResolver*>(cur_resolver);
          if (OB_FAIL(select_resolver->add_unsettled_column(column_ref))) {
            LOG_WARN("add unsettled column to select resolver failed", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObAggrExprPushUpAnalyzer::push_up_subquery_in_aggr(ObSelectResolver& final_resolver)
{
  int ret = OB_SUCCESS;
  ObSelectStmt* cur_stmt = cur_resolver_.get_select_stmt();
  ObSelectStmt* final_stmt = final_resolver.get_select_stmt();

  if (OB_ISNULL(cur_stmt) || OB_ISNULL(final_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cur_stmt or final_stmt is null", K(cur_stmt), K(final_stmt));
  } else if (&cur_resolver_ != &final_resolver) {
    ObIArray<ObQueryRefRawExpr*>& query_refs = cur_stmt->get_subquery_exprs();
    ObArray<ObQueryRefRawExpr*> remain_exprs;
    for (int64_t i = 0; OB_SUCC(ret) && i < query_refs.count(); ++i) {
      ObQueryRefRawExpr* query_ref = query_refs.at(i);
      if (OB_ISNULL(query_ref) || OB_ISNULL(query_ref->get_ref_stmt())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("query_ref is null", K(query_ref), K(i));
      } else if (query_ref->get_expr_level() != cur_stmt->get_current_level() &&
                 query_ref->get_expr_level() != final_stmt->get_current_level()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("query_ref expr level is invalid",
            K(query_ref->get_expr_level()),
            K(cur_stmt->get_current_level()),
            K(final_stmt->get_current_level()));
      } else if (query_ref->get_expr_level() == final_stmt->get_current_level()) {
        if (OB_FAIL(final_stmt->add_subquery_ref(query_ref))) {
          LOG_WARN("add subquery reference to final stmt failed", K(ret));
        } else if (OB_FAIL(query_ref->get_ref_stmt()->adjust_view_parent_namespace_stmt(final_stmt))) {
          LOG_WARN("adjust view parent namespace stmt failed", K(ret));
        }
      } else if (OB_FAIL(remain_exprs.push_back(query_ref))) {
        LOG_WARN("store query ref expr failed", K(ret));
      }
    }
    if (OB_SUCC(ret) && remain_exprs.count() < query_refs.count()) {
      cur_stmt->get_subquery_exprs().reset();
      for (int64_t i = 0; OB_SUCC(ret) && i < remain_exprs.count(); ++i) {
        if (OB_FAIL(cur_stmt->add_subquery_ref(remain_exprs.at(i)))) {
          LOG_WARN("add subquery reference failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObAggrExprPushUpAnalyzer::adjust_query_level(int32_t final_aggr_level)
{
  int ret = OB_SUCCESS;
  int32_t old_aggr_level = cur_resolver_.get_current_level();
  int32_t ascending_level = final_aggr_level - old_aggr_level;
  // If aggregate function is pushed up, level of subqueries in it should also be pushed up.
  for (int64_t i = 0; OB_SUCC(ret) && ascending_level < 0 && i < level_exprs_.count(); ++i) {
    if (OB_ISNULL(level_exprs_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("level expr is null");
    } else if (level_exprs_.at(i)->get_expr_level() >= 0 && level_exprs_.at(i)->get_expr_level() >= old_aggr_level) {
      level_exprs_.at(i)->set_expr_level(level_exprs_.at(i)->get_expr_level() + ascending_level);
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && ascending_level < 0 && i < child_stmts_.count(); ++i) {
    if (OB_ISNULL(child_stmts_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("child stmt is null");
    } else if (child_stmts_.at(i)->get_current_level() > old_aggr_level) {
      child_stmts_.at(i)->set_current_level(child_stmts_.at(i)->get_current_level() + ascending_level);
    }
  }
  return ret;
}
}  // namespace sql
}  // namespace oceanbase
