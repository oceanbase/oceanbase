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
#include "ob_transform_min_max.h"
#include "ob_transformer_impl.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/oblog/ob_log_module.h"
#include "common/ob_common_utility.h"
#include "common/ob_smart_call.h"
#include "share/schema/ob_column_schema.h"
#include "share/schema/ob_table_schema.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/resolver/expr/ob_raw_expr_resolver_impl.h"
#include "sql/ob_sql_context.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/optimizer/ob_optimizer_util.h"
using namespace oceanbase::common;
using namespace oceanbase::share::schema;
namespace oceanbase
{
namespace sql
{

ObTransformMinMax::ObTransformMinMax(ObTransformerCtx *ctx)
    : ObTransformRule(ctx, TransMethod::POST_ORDER, T_FAST_MINMAX)
{
}

ObTransformMinMax::~ObTransformMinMax()
{
}

int ObTransformMinMax::transform_one_stmt(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                             ObDMLStmt *&stmt,
                                             bool &trans_happened)
{
  int ret = OB_SUCCESS;
  bool is_valid = false;
  ObAggFunRawExpr *aggr_expr = NULL;
  trans_happened = false;
  UNUSED(parent_stmts);
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(stmt), K(ctx_));
  } else if (!stmt->is_select_stmt()) {
    //do nothing
    OPT_TRACE("not select stmt");
  } else if (OB_FAIL(check_transform_validity(*ctx_,
                                              static_cast<ObSelectStmt *>(stmt),
                                              aggr_expr,
                                              is_valid))) {
    LOG_WARN("failed to check transform validity", K(ret));
  } else if (!is_valid) {
    //do nothing
    OPT_TRACE("can not transform");
  } else if (OB_FAIL(do_transform(static_cast<ObSelectStmt *>(stmt), aggr_expr))) {
    LOG_WARN("failed to transform column aggregate", K(ret));
  } else if (OB_FAIL(add_transform_hint(*stmt))) {
    LOG_WARN("failed to add transform hint", K(ret));
  } else {
    trans_happened = true;
  }
  return ret;
}

int ObTransformMinMax::check_transform_validity(ObTransformerCtx &ctx,
                                                ObSelectStmt *select_stmt,
                                                ObAggFunRawExpr *&aggr_expr,
                                                bool &is_valid)
{
  int ret = OB_SUCCESS;
  const ObAggFunRawExpr *expr = NULL;
  aggr_expr = NULL;
  is_valid = false;
  if (OB_ISNULL(select_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(select_stmt));
  } else if (select_stmt->has_recursive_cte() || select_stmt->has_hierarchical_query()) {
    OPT_TRACE("stmt has recusive cte or hierarchical query");
  } else if (select_stmt->get_from_item_size() != 1 ||
             select_stmt->get_from_item(0).is_joined_ ||
             select_stmt->get_aggr_item_size() != 1 ||
             !select_stmt->is_scala_group_by()) {
    OPT_TRACE("not a simple query");
  } else if (OB_ISNULL(expr = select_stmt->get_aggr_items().at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), KP(expr));
  } else if ((T_FUN_MAX != expr->get_expr_type() && T_FUN_MIN != expr->get_expr_type()) ||
             expr->get_real_param_count() != 1) {
    OPT_TRACE("aggr expr is not min/max expr");
  } else if (OB_FAIL(is_valid_index_column(ctx, select_stmt, expr->get_param_expr(0), is_valid))) {
    LOG_WARN("failed to check is valid index column", K(ret));
  } else if (!is_valid) {
    OPT_TRACE("aggr expr is not include index column");
  } else if (OB_FAIL(is_valid_select_list(*select_stmt, expr, is_valid))) {
    LOG_WARN("failed to check is valid select list", K(ret));
  } else if (!is_valid) {
    OPT_TRACE("select list is const or aggr_expr");
  } else if (OB_FAIL(is_valid_having(select_stmt, expr, is_valid))) {
    LOG_WARN("fail to check is valid having", K(ret));
  } else if (!is_valid) {
    OPT_TRACE("having condition is invalid");
  } else {
    aggr_expr = select_stmt->get_aggr_items().at(0);
    LOG_TRACE("Succeed to check transform validity", K(is_valid));
  }
  return ret;
}

int ObTransformMinMax::do_transform(ObSelectStmt *select_stmt, ObAggFunRawExpr *aggr_expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(select_stmt) || OB_ISNULL(aggr_expr) ||
      OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("params have null", K(ret), K(select_stmt), K(aggr_expr), K(ctx_));
  } else {
    ObSelectStmt *child_stmt = NULL;
    ObRawExpr *new_tmp_aggr_expr = NULL;
    ObSEArray<ObRawExpr*, 1> old_exprs;
    ObSEArray<ObRawExpr*, 1> new_exprs;
    ObRawExprCopier copier(*ctx_->expr_factory_);
    if (OB_FAIL(ObTransformUtils::create_simple_view(ctx_, select_stmt, child_stmt))) {
      LOG_WARN("failed to create simple view", K(ret));
    } else if (OB_FAIL(select_stmt->get_column_exprs(old_exprs))) {
      LOG_WARN("failed to get column exprs", K(ret));
    } else if (OB_FAIL(child_stmt->get_select_exprs(new_exprs))) {
      LOG_WARN("failed to get select exprs", K(ret));
    } else if (OB_FAIL(copier.add_replaced_expr(old_exprs, new_exprs))) {
      LOG_WARN("failed to add replace pair", K(ret));
    } else if (OB_FAIL(copier.copy(aggr_expr, new_tmp_aggr_expr))) {
      LOG_WARN("failed to copy expr", K(ret));
    } else if (OB_FAIL(set_child_condition(child_stmt, new_tmp_aggr_expr))) {
      LOG_WARN("fail to set child condition", K(ret));
    } else if (OB_FAIL(set_child_order_item(child_stmt, new_tmp_aggr_expr))) {
      LOG_WARN("fail to set child order item", K(ret));
    } else if (OB_FAIL(ObTransformUtils::set_limit_expr(child_stmt, ctx_))) {
      LOG_WARN("fail to set child limit item", K(ret));
    } else {
      LOG_TRACE("Succeed to do transform min max", K(*select_stmt));
    }
  }
  return ret;
}

int ObTransformMinMax::is_valid_select_list(const ObSelectStmt &stmt,
                                            const ObAggFunRawExpr *aggr_expr,
                                            bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  if (OB_ISNULL(aggr_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), KP(aggr_expr));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_select_item_size(); ++i) {
      const ObRawExpr *expr = stmt.get_select_item(i).expr_;
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr should not be NULL", K(ret));
      } else if (expr->is_const_expr()) {
        /* do nothing */
      } else if (OB_FAIL(is_valid_aggr_expr(stmt, expr, aggr_expr, is_valid))) {
        LOG_WARN("failed to check expr is valid aggr", K(ret));
      } else if (!is_valid) {
        break;
      }
    }
  }
  return ret;
}

int ObTransformMinMax::is_valid_aggr_expr(const ObSelectStmt &stmt,
                                          const ObRawExpr *expr,
                                          const ObAggFunRawExpr *aggr_expr,
                                          bool &is_valid)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  is_valid = false;
  const ObRawExpr *param = NULL;
  if (OB_ISNULL(aggr_expr) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr should not be NULL", K(ret), KP(expr), KP(aggr_expr));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_stack_overflow));
  } else if (expr == aggr_expr) {
    is_valid = true;
  } else if (expr->has_flag(CNT_AGG)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_ISNULL(param = expr->get_param_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("param is null", K(ret));
      } else if (param->is_const_expr()) {
        /* do nothing */
      } else if (param->has_flag(CNT_AGG)) {
        if (OB_FAIL(SMART_CALL(is_valid_aggr_expr(stmt, param, aggr_expr, is_valid)))) {
          LOG_WARN("failed to check is_valid_expr", K(ret));
        } else if (!is_valid) {
          break;
        }
      } else {
        break;
      }
    }
  }
  return ret;
}

int ObTransformMinMax::is_valid_having(const ObSelectStmt *stmt,
                                       const ObAggFunRawExpr *column_aggr_expr,
                                       bool &is_expected)
{
  // 改写时，having需要满足的条件:
  // having的表达式中如果含有column，只能够是select_item中的聚集表达式
  // e.g. select max(c1) from t1 having max(c1) > 1; 是满足改写条件的
  //      select max(c1) from t1 having c1 > 1; 是不满足改写条件的
  int ret = OB_SUCCESS;
  is_expected = true;
  bool is_unexpected = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && !is_unexpected && i < stmt->get_having_expr_size(); ++i) {
    if (OB_FAIL(find_unexpected_having_expr(column_aggr_expr,
                                            stmt->get_having_exprs().at(i),
                                            is_unexpected))) {
      LOG_WARN("fail to find unexpected having expr", K(ret), K(i));
    }
  }
  if (OB_SUCC(ret)) {
    is_expected = !is_unexpected;
  }
  return ret;
}

int ObTransformMinMax::find_unexpected_having_expr(const ObAggFunRawExpr *aggr_expr,
                                                   const ObRawExpr *cur_expr,
                                                   bool &is_unexpected)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cur_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("current expr is null", K(ret));
  } else if (aggr_expr == cur_expr) {
    // do nothing
  } else if (cur_expr->is_column_ref_expr() ||
             cur_expr->is_query_ref_expr()) {
    is_unexpected = true;
  } else if (cur_expr->has_flag(CNT_COLUMN) ||
             cur_expr->has_flag(CNT_SUB_QUERY)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < cur_expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(find_unexpected_having_expr(aggr_expr,
                                                         cur_expr->get_param_expr(i),
                                                         is_unexpected)))) {
        LOG_WARN("failed to find unexpected having expr", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformMinMax::is_valid_index_column(ObTransformerCtx &ctx,
                                             const ObSelectStmt *stmt,
                                             const ObRawExpr *expr,
                                             bool &is_valid)
{
  int ret = OB_SUCCESS;
  const TableItem *table_item = NULL;
  const ObColumnRefRawExpr *col_expr = NULL;
  bool is_match_index = false;
  ObArenaAllocator alloc;
  EqualSets &equal_sets = ctx.equal_sets_;
  ObSEArray<ObRawExpr *, 4> const_exprs;
  is_valid = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret), K(stmt), K(expr));
  } else if (!expr->is_column_ref_expr()) {
    /* do nothing */
  } else if (FALSE_IT(col_expr = static_cast<const ObColumnRefRawExpr *>(expr))) {
  } else if (OB_ISNULL(table_item = stmt->get_table_item_by_id(col_expr->get_table_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table item is null", K(ret));
  } else if (!table_item->is_basic_table()) {
    /* do nothing */
  } else if (OB_FAIL(const_cast<ObSelectStmt*>(stmt)->get_stmt_equal_sets(equal_sets, alloc, true))) {
    LOG_WARN("failed to get stmt equal sets", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::compute_const_exprs(stmt->get_condition_exprs(),
                                                          const_exprs))) {
    LOG_WARN("failed to compute const equivalent exprs", K(ret));
  } else if (OB_FAIL(ObTransformUtils::is_match_index(ctx.sql_schema_guard_,
                                                      stmt,
                                                      col_expr,
                                                      is_match_index,
                                                      &equal_sets, &const_exprs))) {
    LOG_WARN("failed to check whether column matches index", K(ret));
  } else if (is_match_index) {
    is_valid = true;
  }
  equal_sets.reuse();
  return ret;
}

int ObTransformMinMax::set_child_order_item(ObSelectStmt *stmt, ObRawExpr *aggr_expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt) || OB_ISNULL(aggr_expr)
      || OB_ISNULL(aggr_expr->get_param_expr(0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("params have null", K(ret), K(stmt), K(aggr_expr));
  } else {
    OrderItem new_order_item;
    new_order_item.expr_ = aggr_expr->get_param_expr(0);
    if (T_FUN_MAX == aggr_expr->get_expr_type()) {
      new_order_item.order_type_ = default_desc_direction();
    } else if (T_FUN_MIN == aggr_expr->get_expr_type()) {
      new_order_item.order_type_ = default_asc_direction();
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("aggregate function type must by max or min", K(ret));
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(stmt->add_order_item(new_order_item))) {
        LOG_WARN("fail to add order item", K(ret), K(stmt), K(new_order_item));
      }
    }
  }
  return ret;
}

int ObTransformMinMax::set_child_condition(ObSelectStmt *stmt, ObRawExpr *aggr_expr)
{
  int ret = OB_SUCCESS;
  ObOpRawExpr *not_null_expr = NULL;
  ObRawExpr *aggr_param = NULL;
  bool is_not_null = false;
  ObArray<ObRawExpr *> constraints;
  if (OB_ISNULL(stmt) || OB_ISNULL(aggr_expr)
      || OB_ISNULL(aggr_param = aggr_expr->get_param_expr(0))
      || OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(stmt), K(aggr_expr));
  } else if (OB_FAIL(ObTransformUtils::is_expr_not_null(ctx_, stmt, aggr_param, NULLABLE_SCOPE::NS_WHERE,
                                                        is_not_null, &constraints))) {
    LOG_WARN("failed to check expr not null", K(ret));
  } else if (is_not_null) {
    if (OB_FAIL(ObTransformUtils::add_param_not_null_constraint(*ctx_, constraints))) {
      LOG_WARN("failed to add param not null constraints", K(ret));
    }
  } else if (OB_FAIL(ObTransformUtils::add_is_not_null(ctx_, stmt, aggr_param, not_null_expr))) {
    LOG_WARN("failed to add is not null", K(ret));
  } else if (OB_FAIL(stmt->add_condition_expr(not_null_expr))) {
    LOG_WARN("failed to add condition expr", K(ret));
  }
  return ret;
}

} // sql
} // oceanbase
