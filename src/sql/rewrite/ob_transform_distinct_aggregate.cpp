/**
 * Copyright (c) 2024 OceanBase
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
#include "ob_transform_distinct_aggregate.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "sql/rewrite/ob_expand_aggregate_utils.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

/**
 * @brief ObTransformDistinctAggregate::transform_one_stmt
 *
 * If all distinct aggregate functions have the same param expr, and all of the
 * other aggregate functions are pre-aggregatable. We can do the deduplication
 * and pre-aggregation in a subquery.
 *
 * e.g.
 * select count(distinct c1), sum(c2) from t1 group by c3;
 * ->
 * select count(view1.c1), sum(view1.sum(c2))
 * from (select c1, c3, sum(c2) from t1 group by c1, c3) view1
 * group by c3;
 */
int ObTransformDistinctAggregate::transform_one_stmt(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                                     ObDMLStmt *&stmt,
                                                     bool &trans_happened)
{
  int ret = OB_SUCCESS;
  bool is_valid = false;
  trans_happened = false;
  UNUSED(parent_stmts);
  if (OB_FAIL(check_transform_validity(stmt, is_valid))) {
    LOG_WARN("failed to check transform validity", K(ret));
  } else if (!is_valid) {
    // do nothing
  } else if (OB_FAIL(do_transform(static_cast<ObSelectStmt *>(stmt),
                                  trans_happened))) {
    LOG_WARN("failed to transform distinct aggregate", K(ret));
  } else if (trans_happened && OB_FAIL(add_transform_hint(*stmt))) {
    LOG_WARN("failed to add transform hint", K(ret));
  }
  return ret;
}

/**
 * @brief ObTransformDistinctAggregate::check_transform_validity
 *
 * All of non-distinct aggregate functions should be pre-aggregatable, and
 * all of distinct aggregate functions should have the same param expr.
 */
int ObTransformDistinctAggregate::check_transform_validity(const ObDMLStmt *stmt,
                                                           bool &is_valid)
{
  int ret = OB_SUCCESS;
  const ObSelectStmt *select_stmt = NULL;
  ObSEArray<ObRawExpr*, 4> distinct_exprs;
  ObStmtExprReplacer replacer; // to extract shared expr in param expr of distinct agg funcs
  replacer.set_relation_scope();
  is_valid = true;
  if (OB_ISNULL(stmt) || OB_ISNULL(stmt->get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(stmt));
  } else if (!stmt->get_query_ctx()->check_opt_compat_version(COMPAT_VERSION_4_3_5)) {
    is_valid = false;
    OPT_TRACE("can not do transform, optimizer feature version is lower than 4.3.5");
  } else if (!stmt->is_select_stmt()) {
    is_valid = false;
    OPT_TRACE("can not do transform, stmt is not a select stmt");
  } else if (OB_FALSE_IT(select_stmt = static_cast<const ObSelectStmt *>(stmt))) {
  } else if (select_stmt->get_from_item_size() <= 0) {
    // If stmt does not have any from item, ObTransformUtils::replace_with_empty_view
    // will not add new view into stmt. Hence we disable the transform for empty from
    // item stmt.
    is_valid = false;
    OPT_TRACE("can not do transform, stmt does not have from item");
  } else if (select_stmt->is_hierarchical_query()
             || select_stmt->is_contains_assignment()) {
    is_valid = false;
    OPT_TRACE("can not do transform, stmt is a hierarchical query or contains assignment");
  } else if (select_stmt->get_aggr_item_size() <= 0
             || select_stmt->get_rollup_expr_size() != 0
             || select_stmt->get_grouping_sets_items_size() != 0
             || select_stmt->get_rollup_items_size() != 0
             || select_stmt->get_cube_items_size() != 0) {
    is_valid = false;
    OPT_TRACE("can not do transform, stmt has no aggregate or has rollup");
  }
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < select_stmt->get_aggr_item_size(); ++i) {
    const ObAggFunRawExpr *aggr_expr = select_stmt->get_aggr_item(i);
    if (OB_ISNULL(aggr_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("agg expr is null", K(ret), K(i));
    } else if (aggr_expr->is_param_distinct()) {
      if (1 > aggr_expr->get_real_param_count()
          || (1 < aggr_expr->get_real_param_count()
              && T_FUN_COUNT != aggr_expr->get_expr_type()
              && T_FUN_GROUP_CONCAT != aggr_expr->get_expr_type())) {
        is_valid = false;
        OPT_TRACE("can not do transform, stmt has distinct aggregate functions with more than one params");
      } else if (!aggr_expr->get_order_items().empty() || aggr_expr->contain_nested_aggr()) {
        is_valid = false;
        OPT_TRACE("can not do transform, stmt has distinct aggregate functions with order item or has nested aggregate");
      } else if (distinct_exprs.empty()) {
        if (OB_FAIL(ObOptimizerUtil::append_exprs_no_dup(distinct_exprs,
                                                         aggr_expr->get_real_param_exprs()))) {
          LOG_WARN("failed to assign aggr param expr", K(ret));
        }
      } else {
        ObSqlBitSet<> visited_idx;
        for (int64_t j = 0; OB_SUCC(ret) && is_valid && j < aggr_expr->get_real_param_count(); ++j) {
          ObRawExpr *param_expr = aggr_expr->get_real_param_exprs().at(j);
          bool has_find = false;
          if (OB_ISNULL(param_expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("param of distinct aggregate function is NULL", K(ret), KPC(aggr_expr), K(j));
          }
          for (int64_t k = 0; OB_SUCC(ret) && !has_find && k < distinct_exprs.count(); ++k) {
            if (OB_ISNULL(distinct_exprs.at(k))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("param of distinct aggregate function is NULL", K(ret), K(distinct_exprs), K(k));
            } else if (param_expr->same_as(*distinct_exprs.at(k))) {
              has_find = true;
              if (OB_FAIL(visited_idx.add_member(k))) {
                LOG_WARN("failed to add member", K(ret));
              } else if (param_expr != distinct_exprs.at(k)
                         && OB_FAIL(replacer.add_replace_expr(param_expr, distinct_exprs.at(k)))) {
                LOG_WARN("failed to add replace expr", K(ret));
              }
              break;
            }
          }
          is_valid &= has_find;
        }
        is_valid &= (visited_idx.num_members() == distinct_exprs.count());
        if (!is_valid) {
          OPT_TRACE("can not do transform, stmt has aggregate functions with different distinct columns");
        }
      }
    } else if (OB_FAIL(ObOptimizerUtil::check_aggr_can_pre_aggregate(aggr_expr, is_valid))) {
      LOG_WARN("failed to check aggr can pre aggregate", K(ret), KPC(aggr_expr));
    } else if (!is_valid) {
      OPT_TRACE("can not do transform, stmt has aggregate function that can not pre aggregate");
    }
  }
  if (OB_SUCC(ret) && is_valid && distinct_exprs.empty()) {
    is_valid = false;
    OPT_TRACE("do not need to transform, stmt has no distinct aggregate function");
  }
  if (OB_SUCC(ret) && is_valid) {
    // replace the same exprs in distinct agg funcs with shared exprs
    for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt->get_aggr_item_size(); ++i) {
      ObRawExpr *aggr_expr = select_stmt->get_aggr_item(i);
      if (OB_ISNULL(aggr_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("agg expr is null", K(ret), K(i));
      } else if (!static_cast<ObAggFunRawExpr*>(aggr_expr)->is_param_distinct()) {
        // do nothing
      } else if (OB_FAIL(replacer.do_visit(aggr_expr))) {
        LOG_WARN("failed to replace param expr", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformDistinctAggregate::do_transform(ObSelectStmt *stmt,
                                               bool &trans_happened)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObAggFunRawExpr*, 8> non_distinct_aggr;
  ObSEArray<ObAggFunRawExpr*, 8> distinct_aggr;
  ObSEArray<TableItem*, 8> from_tables;
  ObSEArray<SemiInfo*, 8> semi_infos;
  ObSEArray<ObRawExpr*, 8> view_select_exprs;
  ObSEArray<ObRawExpr*, 8> view_group_exprs;
  ObSEArray<ObRawExpr*, 8> view_cond_exprs;
  TableItem *view_table = NULL;
  trans_happened = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(classify_aggr_exprs(stmt->get_aggr_items(),
                                         non_distinct_aggr,
                                         distinct_aggr))) {
    LOG_WARN("failed to classify aggr exprs", K(ret));
  } else if (OB_FAIL(construct_view_select_exprs(non_distinct_aggr,
                                                 view_select_exprs))) {
    LOG_WARN("failed to construct view select exprs", K(ret));
  } else if (OB_FAIL(construct_view_group_exprs(stmt->get_group_exprs(),
                                                distinct_aggr,
                                                view_group_exprs))) {
    LOG_WARN("failed to construct view group exprs", K(ret));
  } else if (OB_FAIL(stmt->get_from_tables(from_tables))) {
    LOG_WARN("failed to get from tables", K(ret));
  } else if (OB_FAIL(semi_infos.assign(stmt->get_semi_infos()))) {
    LOG_WARN("failed to assign semi infos", K(ret));
  } else if (OB_FAIL(view_cond_exprs.assign(stmt->get_condition_exprs()))) {
    LOG_WARN("failed to assign view cond exprs", K(ret));
  } else if (OB_FALSE_IT(stmt->get_condition_exprs().reset())) {
  } else if (OB_FAIL(ObTransformUtils::replace_with_empty_view(ctx_,
                                                               stmt,
                                                               view_table,
                                                               from_tables,
                                                               &semi_infos))) {
    LOG_WARN("failed to create empty view", K(ret));
  } else if (OB_FAIL(ObTransformUtils::create_inline_view(ctx_,
                                                          stmt,
                                                          view_table,
                                                          from_tables,
                                                          &view_cond_exprs,
                                                          &semi_infos,
                                                          &view_select_exprs,
                                                          &view_group_exprs))) {
    LOG_WARN("failed to create inline view", K(ret));
  } else if (OB_FAIL(replace_aggr_func(stmt, view_table, distinct_aggr))) {
    LOG_WARN("failed to replace aggregate functions", K(ret));
  } else {
    trans_happened = true;
  }
  return ret;
}

int ObTransformDistinctAggregate::classify_aggr_exprs(const ObIArray<ObAggFunRawExpr*> &aggr_exprs,
                                                      ObIArray<ObAggFunRawExpr*> &non_distinct_aggr,
                                                      ObIArray<ObAggFunRawExpr*> &distinct_aggr)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < aggr_exprs.count(); ++i) {
    ObAggFunRawExpr *aggr_expr = aggr_exprs.at(i);
    if (OB_ISNULL(aggr_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("aggr expr is null", K(ret), K(i));
    } else if (aggr_expr->is_param_distinct()) {
      if (OB_FAIL(distinct_aggr.push_back(aggr_expr))) {
        LOG_WARN("failed to push back distinct aggr", K(ret));
      }
    } else {
      if (OB_FAIL(non_distinct_aggr.push_back(aggr_expr))) {
        LOG_WARN("failed to push back non distinct aggr", K(ret));
      }
    }
  }
  return ret;
}

/**
 * @brief ObTransformDistinctAggregate::construct_view_select_exprs
 *
 * view select exprs = distinct columns + non distinct aggrs
 */
int ObTransformDistinctAggregate::construct_view_select_exprs(const ObIArray<ObAggFunRawExpr*> &non_distinct_aggr,
                                                              ObIArray<ObRawExpr*> &view_select_exprs)
{
  int ret = OB_SUCCESS;
  // We do not need to add distinct columns into view select exprs here, they
  // will be added by ObTransformUtils::create_inline_view automatically
  for (int64_t i = 0; OB_SUCC(ret) && i < non_distinct_aggr.count(); ++i) {
    if (OB_FAIL(add_var_to_array_no_dup(view_select_exprs, static_cast<ObRawExpr*>(non_distinct_aggr.at(i))))) {
      LOG_WARN("failed to add non distinct aggr expr", K(ret));
    }
  }
  return ret;
}

/**
 * @brief ObTransformDistinctAggregate::construct_view_group_exprs
 *
 * view group exprs = origin group exprs + distinct columns
 */
int ObTransformDistinctAggregate::construct_view_group_exprs(const ObIArray<ObRawExpr*> &ori_group_expr,
                                                             const ObIArray<ObAggFunRawExpr*> &distinct_aggr,
                                                             ObIArray<ObRawExpr*> &view_group_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(distinct_aggr.empty() || NULL == distinct_aggr.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected distinct aggr", K(ret), K(distinct_aggr));
  } else if (OB_FAIL(append(view_group_exprs, ori_group_expr))) {
    LOG_WARN("failed to append group exprs", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < distinct_aggr.at(0)->get_real_param_count(); ++i) {
    ObRawExpr *param_expr = distinct_aggr.at(0)->get_real_param_exprs().at(i);
    if (OB_ISNULL(param_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("param expr is null", K(ret), K(i), KPC(distinct_aggr.at(0)));
    } else if (param_expr->is_static_scalar_const_expr()) {
      // do nothing, do not need to add static const expr into group exprs
    } else if (OB_FAIL(add_var_to_array_no_dup(view_group_exprs, param_expr))) {
      LOG_WARN("failed to add distinct aggr param", K(ret));
    }
  }
  if (OB_SUCC(ret) && view_group_exprs.empty()) {
    // If all of the distinct params are static const exprs, we need to add one
    // param expr into view_group_exprs to keep the aggregate semantics.
    if (OB_UNLIKELY(1 > distinct_aggr.at(0)->get_real_param_count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("distinct aggr does not have param", K(ret), KPC(distinct_aggr.at(0)));
    } else if (OB_FAIL(view_group_exprs.push_back(distinct_aggr.at(0)->get_real_param_exprs().at(0)))) {
      LOG_WARN("failed to push back group exprs", K(ret));
    }
  }
  return ret;
}

/**
 * @brief ObTransformDistinctAggregate::replace_aggr_func
 *
 * For non distinct aggr, wrap it with the final aggregate,
 * For distinct aggr, remove distinct.
 */
int ObTransformDistinctAggregate::replace_aggr_func(ObSelectStmt *stmt,
                                                    TableItem *view_table,
                                                    const ObIArray<ObAggFunRawExpr*> &distinct_aggr)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *view_stmt = NULL;
  ObSEArray<ObRawExpr*, 8> view_exprs;
  ObStmtExprReplacer replacer;
  replacer.set_relation_scope();
  replacer.set_recursive(false);
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_)
      || OB_ISNULL(stmt) || OB_ISNULL(view_table)
      || OB_UNLIKELY(!view_table->is_generated_table())
      || OB_ISNULL(view_stmt = view_table->ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(ctx_), K(stmt), K(view_table), K(view_stmt));
  } else  if (OB_FAIL(view_stmt->get_select_exprs(view_exprs))) {
    LOG_WARN("failed to get origin select exprs", K(ret));
  } else {
    stmt->clear_aggr_item();
  }
  // create replaced non distinct aggr
  for (int64_t i = 0; OB_SUCC(ret) && i < view_exprs.count(); ++i) {
    ObRawExpr *view_expr = view_exprs.at(i);
    ObAggFunRawExpr *view_aggr_expr = NULL;
    ObColumnRefRawExpr *column_expr = NULL;
    ObAggFunRawExpr *new_aggr = NULL;
    ObRawExpr *new_expr = NULL;
    if (OB_ISNULL(view_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(i));
    } else if (!view_expr->is_aggr_expr()) {
      // do nothing
    } else if (OB_FALSE_IT(view_aggr_expr = static_cast<ObAggFunRawExpr*>(view_expr))) {
    } else if (OB_ISNULL(column_expr = stmt->get_column_expr_by_id(view_table->table_id_,
                                                                   OB_APP_MIN_COLUMN_ID + i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get column expr", K(ret), K(i), KPC(view_expr));
    } else if (OB_FAIL(ObOptimizerUtil::generate_pullup_aggr_expr(*ctx_->expr_factory_,
                                                                  ctx_->session_info_,
                                                                  view_aggr_expr->get_expr_type(),
                                                                  view_aggr_expr,
                                                                  column_expr,
                                                                  new_aggr))) {
      LOG_WARN("failed to build new aggregate funcion expr", K(ret));
    } else if (OB_FAIL(ObExpandAggregateUtils::add_aggr_item(stmt->get_aggr_items(),
                                                             new_aggr))) {
      LOG_WARN("failed to add aggr item", K(ret), KPC(new_aggr));
    } else if (OB_FALSE_IT(new_expr = static_cast<ObRawExpr*>(new_aggr))) {
    } else if (OB_FAIL(ObTransformUtils::add_cast_for_replace_if_need(*ctx_->expr_factory_,
                                                                      view_expr,
                                                                      new_expr,
                                                                      ctx_->session_info_))) {
      LOG_WARN("failed to add cast for new aggr expr", K(ret));
    } else if (OB_FAIL(replacer.add_replace_expr(column_expr, new_expr))) {
      LOG_WARN("failed to add replace expr", K(ret));
    }
  }
  // create replaced distinct aggr
  for (int64_t i = 0; OB_SUCC(ret) && i < distinct_aggr.count(); ++i) {
    ObAggFunRawExpr *aggr = distinct_aggr.at(i);
    ObAggFunRawExpr *new_aggr = NULL;
    if (OB_ISNULL(aggr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(i));
    } else if (OB_FAIL(ctx_->expr_factory_->create_raw_expr(aggr->get_expr_type(),
                                                            new_aggr))) {
      LOG_WARN("failed to create new aggr expr", K(ret));
    } else if (OB_ISNULL(new_aggr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("new aggr is null", K(ret));
    } else if (OB_FAIL(new_aggr->assign(*aggr))) {
      LOG_WARN("failed to assign aggr expr", K(ret));
    } else if (OB_FALSE_IT(new_aggr->set_param_distinct(false))) {
    } else if (OB_FAIL(new_aggr->formalize(ctx_->session_info_))) {
      LOG_WARN("failed to formalize aggr expr", K(ret));
    } else if (OB_FAIL(ObExpandAggregateUtils::add_aggr_item(stmt->get_aggr_items(),
                                                             new_aggr))) {
      LOG_WARN("failed to add aggr item", K(ret), KPC(new_aggr));
    } else if (OB_FAIL(replacer.add_replace_expr(aggr, new_aggr))) {
      LOG_WARN("failed to add replace expr", K(ret));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(stmt->iterate_stmt_expr(replacer))) {
    LOG_WARN("failed to iterate stmt expr", K(ret));
  }
  return ret;
}