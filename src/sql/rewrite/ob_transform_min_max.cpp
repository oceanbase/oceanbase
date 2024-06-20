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
                                              is_valid))) {
    LOG_WARN("failed to check transform validity", K(ret));
  } else if (!is_valid) {
    //do nothing
    OPT_TRACE("can not do min max transform");
  } else if (OB_FAIL(do_transform(static_cast<ObSelectStmt *>(stmt)))) {
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
                                                bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  bool has_rownum = false;
  if (OB_ISNULL(select_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(select_stmt));
  } else if (select_stmt->has_recursive_cte() || select_stmt->has_hierarchical_query()) {
    OPT_TRACE("stmt has recusive cte or hierarchical query");
  } else if (select_stmt->get_from_item_size() != 1 || select_stmt->get_from_item(0).is_joined_
             || select_stmt->get_aggr_item_size() < 1 || !select_stmt->is_scala_group_by()
             || select_stmt->is_contains_assignment()
             || (select_stmt->get_aggr_item_size() > 1 && select_stmt->get_semi_info_size() > 0)) {
    OPT_TRACE("not a simple aggr query");
  } else if (OB_FAIL(select_stmt->has_rownum(has_rownum))) {
    LOG_WARN("failed to check if select stmt has rownum", K(ret));
  } else if (has_rownum) {
    OPT_TRACE("stmt has rownum");
  } else if (OB_FAIL(is_valid_aggr_items(ctx, *select_stmt, is_valid))) {
    LOG_WARN("failed to check aggr items", K(ret));
  } else if (!is_valid) {
    OPT_TRACE("aggr expr is invalid");
  } else if (OB_FAIL(is_valid_select_list(*select_stmt, is_valid))) {
    LOG_WARN("failed to check is valid select list", K(ret));
  } else if (!is_valid) {
    OPT_TRACE("select list is not const or aggr_expr");
  } else if (OB_FAIL(is_valid_having_list(*select_stmt, is_valid))) {
    LOG_WARN("failed to check is valid having", K(ret));
  } else if (!is_valid) {
    OPT_TRACE("having condition is invalid");
  } else if (select_stmt->get_aggr_item_size() > 1 && OB_FAIL(is_valid_order_list(*select_stmt, is_valid))) {
    LOG_WARN("failed to check is valid order by", K(ret));
  } else if (!is_valid) {
    OPT_TRACE("order by is invalid for multi min/max");
  } else {
    LOG_TRACE("Succeed to check minmax transform validity", K(is_valid));
  }
  return ret;
}

int ObTransformMinMax::do_transform(ObSelectStmt *select_stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(select_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("params have null", K(ret), K(select_stmt));
  } else if (select_stmt->get_aggr_item_size() == 1) {
    if (OB_FAIL(do_single_minmax_transform(select_stmt))) {
      LOG_WARN("failed to transform single minmax", K(ret));
    }
  } else if (select_stmt->get_aggr_item_size() > 1) {
    if (OB_FAIL(do_multi_minmax_transform(select_stmt))) {
      LOG_WARN("failed to transform multi minmax", K(ret));
    }
  }
  return ret;
}

int ObTransformMinMax::do_single_minmax_transform(ObSelectStmt *select_stmt)
{
  int ret = OB_SUCCESS;
  ObRawExpr *aggr_param = NULL;
  ObRawExpr *new_aggr_param = NULL;
  ObSelectStmt *child_stmt = NULL;
  ObSEArray<ObRawExpr*, 1> old_exprs;
  ObSEArray<ObRawExpr*, 1> new_exprs;
  if (OB_ISNULL(select_stmt) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("params have null", K(ret), K(select_stmt), K(ctx_));
  } else if (select_stmt->get_aggr_item_size() != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected param", K(ret));
  } else {
    ObAggFunRawExpr *aggr_expr = select_stmt->get_aggr_item(0);
    ObRawExprCopier copier(*ctx_->expr_factory_);
    if (OB_ISNULL(aggr_expr) || OB_ISNULL(aggr_param = aggr_expr->get_param_expr(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("aggr expr unexpected null", K(ret));
    } else if (OB_FAIL(ObTransformUtils::create_simple_view(ctx_, select_stmt, child_stmt))) {
      LOG_WARN("failed to create simple view", K(ret));
    } else if (OB_FAIL(select_stmt->get_column_exprs(old_exprs))) {
      LOG_WARN("failed to get column exprs", K(ret));
    } else if (OB_FAIL(child_stmt->get_select_exprs(new_exprs))) {
      LOG_WARN("failed to get select exprs", K(ret));
    } else if (OB_FAIL(copier.add_replaced_expr(old_exprs, new_exprs))) {
      LOG_WARN("failed to add replace pair", K(ret));
    } else if (OB_FAIL(copier.copy(aggr_param, new_aggr_param))) {
      LOG_WARN("failed to copy expr", K(ret));
    } else if (OB_FAIL(set_child_condition(child_stmt, new_aggr_param))) {
      LOG_WARN("failed to set child condition", K(ret));
    } else if (OB_FAIL(set_child_order_item(child_stmt, new_aggr_param, aggr_expr->get_expr_type()))) {
      LOG_WARN("failed to set child order item", K(ret));
    } else if (OB_FAIL(ObTransformUtils::set_limit_expr(child_stmt, ctx_))) {
      LOG_WARN("failed to set child limit item", K(ret));
    } else {
      LOG_TRACE("Succeed to do transform min max", K(*select_stmt));
    }
  }
  return ret;
}

int ObTransformMinMax::do_multi_minmax_transform(ObSelectStmt *select_stmt)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *view_child_stmt = NULL;
  ObSelectStmt *child_stmt = NULL;
  ObAggFunRawExpr *aggr_expr = NULL;
  ObRawExpr *aggr_param = NULL;
  ObRawExpr *new_aggr_param = NULL;
  ObSEArray<ObRawExpr*, 1> old_exprs;
  ObSEArray<ObRawExpr*, 1> new_exprs;
  ObArray<ObRawExpr *> aggr_items;
  ObArray<ObRawExpr *> query_ref_exprs;
  ObQueryRefRawExpr *query_ref_expr = NULL;
  ObRawExpr *target_expr = NULL;
  if (OB_ISNULL(select_stmt) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("params have null", K(ret), K(select_stmt), K(ctx_));
  } else {
    ObRawExprCopier copier(*ctx_->expr_factory_);
    if (OB_FAIL(ObTransformUtils::create_simple_view(ctx_, select_stmt, view_child_stmt))) {
      LOG_WARN("failed to create simple view", K(ret));
    } else if (OB_FAIL(select_stmt->get_column_exprs(old_exprs))) {
      LOG_WARN("failed to get column exprs", K(ret));
    } else if (OB_FAIL(view_child_stmt->get_select_exprs(new_exprs))) {
      LOG_WARN("failed to get select exprs", K(ret));
    } else if (OB_FAIL(copier.add_replaced_expr(old_exprs, new_exprs))) {
      LOG_WARN("failed to add replace pair", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt->get_aggr_item_size(); ++i) {
      if (OB_ISNULL(aggr_expr = select_stmt->get_aggr_item(i))
          || OB_ISNULL(aggr_param = aggr_expr->get_param_expr(0))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(copier.copy(aggr_param, new_aggr_param))) {
        LOG_WARN("failed to copy expr", K(ret));
      } else if (OB_FAIL(deep_copy_subquery_for_aggr(*view_child_stmt,
                                                     new_aggr_param,
                                                     aggr_expr->get_expr_type(),
                                                     child_stmt))) {
        LOG_WARN("failed to deep copy subquery for aggr", K(ret));
      } else if (OB_FAIL(aggr_items.push_back(aggr_expr))) {
        LOG_WARN("failed to push back aggr item", K(ret));
      } else if (OB_FAIL(ctx_->expr_factory_->create_raw_expr(T_REF_QUERY, query_ref_expr))) {
        LOG_WARN("failed to create query ref expr", K(ret));
      } else if (OB_ISNULL(query_ref_expr) || OB_ISNULL(child_stmt)
                 || OB_UNLIKELY(child_stmt->get_select_item_size() != 1)
                 || OB_ISNULL(target_expr = child_stmt->get_select_item(0).expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null expr or select item size of child stmt", K(ret));
      } else {
        query_ref_expr->set_ref_stmt(child_stmt);
        query_ref_expr->set_output_column(1);
        if (OB_FAIL(query_ref_expr->add_column_type(target_expr->get_result_type()))) {
          LOG_WARN("add column type to subquery ref expr failed", K(ret));
        } else if (OB_FAIL(query_ref_expr->formalize(ctx_->session_info_))) {
          LOG_WARN("failed to formalize coalesce query expr", K(ret));
        } else if (OB_FAIL(query_ref_exprs.push_back(query_ref_expr))) {
          LOG_WARN("failed to push back query ref expr", K(ret));
        }
      }
    }
    // adjust select_stmt
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(select_stmt->replace_relation_exprs(aggr_items, query_ref_exprs))) {
      LOG_WARN("failed to replace aggr exprs to query ref exprs", K(ret));
    } else if (OB_FAIL(select_stmt->get_condition_exprs().assign(select_stmt->get_having_exprs()))) {
      LOG_WARN("failed to assign condition exprs", K(ret));
    } else {
      select_stmt->get_from_items().reset();
      select_stmt->get_table_items().reset();
      select_stmt->get_aggr_items().reset();
      select_stmt->get_having_exprs().reset();
      select_stmt->get_column_items().reset();
      if (OB_FAIL(select_stmt->adjust_subquery_list())) {
        LOG_WARN("failed to adjust subquery list", K(ret));
      } else if (OB_FAIL(select_stmt->formalize_stmt(ctx_->session_info_))) {
        LOG_WARN("failed to formalize stmt", K(ret));
      } else {
        LOG_TRACE("succeed to do transform min max", KPC(select_stmt));
      }
    }
  }
  return ret;
}

int ObTransformMinMax::deep_copy_subquery_for_aggr(const ObSelectStmt &copied_stmt,
                                                   ObRawExpr *aggr_param,
                                                   ObItemType aggr_type,
                                                   ObSelectStmt *&child_stmt)
{
  int ret = OB_SUCCESS;
  ObDMLStmt *dml_stmt = NULL;
  ObRawExpr *new_aggr_param = NULL;
  ObString qb_name;
  TableItem *table = NULL;
  ObArray<ObRawExpr *> select_exprs;
  ObArray<ObRawExpr *> new_select_exprs;
  int64_t new_aggr_param_index = -1;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_) || OB_ISNULL(ctx_->stmt_factory_)
      || OB_ISNULL(ctx_->allocator_) || OB_ISNULL(aggr_param)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("params have null", K(ret), K(aggr_param), K(ctx_));
  } else if (OB_FAIL(ObTransformUtils::deep_copy_stmt(*ctx_->stmt_factory_,
                                                      *ctx_->expr_factory_,
                                                      &copied_stmt,
                                                      dml_stmt))) {
    LOG_WARN("failed to deep copy stmt", K(ret));
  } else if (OB_ISNULL(child_stmt = static_cast<ObSelectStmt *>(dml_stmt))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child stmt is null", K(ret));
  } else if (OB_FAIL(child_stmt->adjust_statement_id(ctx_->allocator_,
                                                     ctx_->src_qb_name_,
                                                     ctx_->src_hash_val_))) {
    LOG_WARN("failed to adjust statement id", K(ret));
  } else if (OB_FAIL(child_stmt->get_qb_name(qb_name))) {
    LOG_WARN("failed to get qb name", K(ret));
  } else if (child_stmt->get_table_size() != 1
              || OB_ISNULL(table = child_stmt->get_table_item(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected table size or table item is null", K(ret), KPC(child_stmt));
  } else if (OB_FALSE_IT(table->qb_name_ = qb_name)) {
  } else if (OB_FAIL(child_stmt->update_stmt_table_id(ctx_->allocator_, copied_stmt))) {
    LOG_WARN("failed to update stmt table id", K(ret));
  } else if (OB_FAIL(copied_stmt.get_select_exprs(select_exprs))) {
    LOG_WARN("failed to get select exprs", K(ret));
  } else if (!ObOptimizerUtil::find_item(select_exprs, aggr_param, &new_aggr_param_index)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to find new aggr param", K(ret));
  } else if (OB_FAIL(child_stmt->get_select_exprs(new_select_exprs))) {
    LOG_WARN("failed to get select exprs", K(ret));
  } else if (new_aggr_param_index < 0 || new_aggr_param_index >= new_select_exprs.count()
             || OB_ISNULL(new_aggr_param = new_select_exprs.at(new_aggr_param_index))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to find new aggr param", K(ret));
  } else {
    child_stmt->get_select_items().reset();
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObTransformUtils::create_select_item(*ctx_->allocator_,
                                                          new_aggr_param,
                                                          child_stmt))) {
    LOG_WARN("failed to create select item", K(ret));
  } else if (OB_FAIL(set_child_condition(child_stmt, new_aggr_param))) {
    LOG_WARN("failed to set child condition", K(ret));
  } else if (OB_FAIL(set_child_order_item(child_stmt, new_aggr_param, aggr_type))) {
    LOG_WARN("failed to set child order item", K(ret));
  } else if (OB_FAIL(ObTransformUtils::set_limit_expr(child_stmt, ctx_))) {
    LOG_WARN("failed to set child limit item", K(ret));
  } else if (OB_FAIL(child_stmt->rebuild_tables_hash())) {
    LOG_WARN("failed to rebuild table hash", K(ret));
  } else if (OB_FAIL(child_stmt->update_column_item_rel_id())) {
    LOG_WARN("failed to update column item by id", K(ret));
  } else if (OB_FAIL(child_stmt->formalize_stmt(ctx_->session_info_))) {
    LOG_WARN("failed to formalize stmt", K(ret));
  }
  return ret;
}

int ObTransformMinMax::is_valid_aggr_items(ObTransformerCtx &ctx,
                                           const ObSelectStmt &stmt,
                                           bool &is_valid)
{
  int ret = OB_SUCCESS;
  const ObAggFunRawExpr *expr = NULL;
  bool valid = true;
  EqualSets &equal_sets = ctx.equal_sets_;
  ObSEArray<ObRawExpr *, 4> const_exprs;
  ObSEArray<const ObRawExpr *, 4> valid_col_exprs;
  ObArenaAllocator alloc(ObMemAttr(MTL_ID(), "RewriteMinMax"));
  if (OB_FAIL(stmt.get_stmt_equal_sets(equal_sets, alloc, true))) {
    LOG_WARN("failed to get stmt equal sets", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::compute_const_exprs(stmt.get_condition_exprs(),
                                                          const_exprs))) {
    LOG_WARN("failed to compute const equivalent exprs", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && valid && i < stmt.get_aggr_item_size(); ++i) {
    if (OB_ISNULL(expr = stmt.get_aggr_item(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("params have null", K(ret), KP(expr));
    } else if ((T_FUN_MAX != expr->get_expr_type() && T_FUN_MIN != expr->get_expr_type()) ||
                expr->get_real_param_count() != 1) {
      OPT_TRACE("aggr expr is not min/max expr");
      valid = false;
    } else if (OB_ISNULL(expr->get_param_expr(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null expr", K(ret));
    } else if (is_contain(valid_col_exprs, expr->get_param_expr(0))) {
      valid = true;
    } else if (OB_FAIL(is_valid_index_column(ctx,
                                             &stmt,
                                             expr->get_param_expr(0),
                                             &equal_sets,
                                             &const_exprs,
                                             valid))) {
      LOG_WARN("failed to check is valid index column", K(ret));
    } else if (!valid) {
      OPT_TRACE("aggr expr is not include index column");
    } else if (OB_FAIL(valid_col_exprs.push_back(expr->get_param_expr(0)))) {
      LOG_WARN("failed to push back valid column", K(ret));
    }
  }
  equal_sets.reuse();
  is_valid = valid;
  return ret;
}

int ObTransformMinMax::is_valid_select_list(const ObSelectStmt &stmt, bool &is_valid)
{
  int ret = OB_SUCCESS;
  bool valid = true;
  for (int64_t i = 0; OB_SUCC(ret) && valid && i < stmt.get_select_item_size(); ++i) {
    if (OB_FAIL(is_valid_select_expr(stmt.get_select_item(i).expr_, valid))) {
      LOG_WARN("failed to check expr is valid aggr", K(ret));
    }
  }
  is_valid = valid;
  return ret;
}

int ObTransformMinMax::is_valid_select_expr(const ObRawExpr *expr, bool &is_valid)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  bool valid = true;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr should not be NULL", K(ret), KP(expr));
  } else if (expr->has_flag(IS_AGG) || expr->is_const_expr()) {
    /* do nothing */
  } else if (expr->has_flag(CNT_AGG)) {
    for (int64_t i = 0; OB_SUCC(ret) && valid && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(is_valid_select_expr(expr->get_param_expr(i), valid)))) {
        LOG_WARN("failed to check is_valid_expr", K(ret));
      }
    }
  } else {
    valid = false;
  }
  is_valid = valid;
  return ret;
}

int ObTransformMinMax::is_valid_having_list(const ObSelectStmt &stmt, bool &is_valid)
{
  int ret = OB_SUCCESS;
  bool valid = true;
  for (int64_t i = 0; OB_SUCC(ret) && valid && i < stmt.get_having_expr_size(); ++i) {
    if (OB_FAIL(is_valid_having_expr(stmt.get_having_exprs().at(i), valid))) {
      LOG_WARN("failed to check having expr", K(ret), K(i));
    }
  }
  is_valid = valid;
  return ret;
}

int ObTransformMinMax::is_valid_having_expr(const ObRawExpr *expr, bool &is_valid)
{
  int ret = OB_SUCCESS;
  bool valid = true;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("current expr is null", K(ret));
  } else if (expr->has_flag(IS_AGG)) {
    /* do nothing*/
  } else if (expr->is_column_ref_expr() || expr->has_flag(CNT_SUB_QUERY)) {
    valid = false;
  } else if (expr->has_flag(CNT_COLUMN)) {
    for (int64_t i = 0; OB_SUCC(ret) && valid && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(is_valid_having_expr(expr->get_param_expr(i), valid)))) {
        LOG_WARN("failed to check having expr", K(ret));
      }
    }
  }
  is_valid = valid;
  return ret;
}

int ObTransformMinMax::is_valid_order_list(const ObSelectStmt &stmt, bool &is_valid)
{
  int ret = OB_SUCCESS;
  bool valid = true;
  for (int64_t i = 0; OB_SUCC(ret) && valid && i < stmt.get_order_item_size(); ++i) {
    if (OB_FAIL(is_valid_order_expr(stmt.get_order_item(i).expr_, valid))) {
      LOG_WARN("failed to check having expr", K(ret), K(i));
    }
  }
  is_valid = valid;
  return ret;
}

int ObTransformMinMax::is_valid_order_expr(const ObRawExpr *expr, bool &is_valid)
{
  int ret = OB_SUCCESS;
  bool valid = true;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("current expr is null", K(ret));
  } else if (expr->has_flag(IS_AGG)) {
    /* do nothing*/
  } else if (expr->is_column_ref_expr()) {
    valid = false;
  } else if (expr->has_flag(CNT_COLUMN)) {
    for (int64_t i = 0; OB_SUCC(ret) && valid && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(is_valid_order_expr(expr->get_param_expr(i), valid)))) {
        LOG_WARN("failed to check order expr", K(ret));
      }
    }
  }
  is_valid = valid;
  return ret;
}

int ObTransformMinMax::is_valid_index_column(ObTransformerCtx &ctx,
                                             const ObSelectStmt *stmt,
                                             const ObRawExpr *expr,
                                             EqualSets *equal_sets,
                                             ObIArray<ObRawExpr*> *const_exprs,
                                             bool &is_valid)
{
  int ret = OB_SUCCESS;
  const TableItem *table_item = NULL;
  const ObColumnRefRawExpr *col_expr = NULL;
  bool is_match_index = false;
  bool need_check_query_range = false;
  is_valid = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret), K(stmt), K(expr));
  } else if (!expr->is_column_ref_expr()) {
    /* do nothing */
  } else if (OB_FALSE_IT(col_expr = static_cast<const ObColumnRefRawExpr *>(expr))) {
  } else if (OB_ISNULL(table_item = stmt->get_table_item_by_id(col_expr->get_table_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table item is null", K(ret));
  } else if (!table_item->is_basic_table()) {
    /* do nothing */
  } else {
    if (stmt->get_aggr_item_size() > 1 && stmt->get_condition_size() > 0) {
      need_check_query_range = true;
    }
    if (OB_FAIL(ObTransformUtils::is_match_index(ctx.sql_schema_guard_,
                                                 stmt,
                                                 col_expr,
                                                 is_match_index,
                                                 equal_sets,
                                                 const_exprs,
                                                 NULL,
                                                 false,
                                                 need_check_query_range,
                                                 &ctx))) {
      LOG_WARN("failed to check whether column matches index", K(ret));
    } else if (is_match_index) {
      is_valid = true;
    }
  }
  return ret;
}

int ObTransformMinMax::set_child_order_item(ObSelectStmt *stmt,
                                            ObRawExpr *aggr_param,
                                            ObItemType aggr_type)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt) || OB_ISNULL(aggr_param)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("params have null", K(ret), K(stmt), K(aggr_param));
  } else {
    OrderItem new_order_item;
    new_order_item.expr_ = aggr_param;
    if (T_FUN_MAX == aggr_type) {
      new_order_item.order_type_ = default_desc_direction();
    } else if (T_FUN_MIN == aggr_type) {
      new_order_item.order_type_ = default_asc_direction();
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("aggregate function type must by max or min", K(ret));
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(stmt->add_order_item(new_order_item))) {
        LOG_WARN("failed to add order item", K(ret), K(stmt), K(new_order_item));
      }
    }
  }
  return ret;
}

int ObTransformMinMax::set_child_condition(ObSelectStmt *stmt, ObRawExpr *aggr_param)
{
  int ret = OB_SUCCESS;
  ObOpRawExpr *not_null_expr = NULL;
  bool is_not_null = false;
  ObArray<ObRawExpr *> constraints;
  if (OB_ISNULL(stmt) || OB_ISNULL(aggr_param) || OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(stmt), K(aggr_param));
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
