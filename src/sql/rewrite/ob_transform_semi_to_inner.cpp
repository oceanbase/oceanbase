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
#include "sql/rewrite/ob_transform_semi_to_inner.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/optimizer/ob_optimizer_util.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;

/**
 * @brief ObTransformSemiToInner::transform_one_stmt
 * @param parent_stmts
 * @param stmt
 * @param trans_happened
 * @return
 */
int ObTransformSemiToInner::transform_one_stmt(
    common::ObIArray<ObParentDMLStmt>& parent_stmts, ObDMLStmt*& stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  ObDMLStmt* root_stmt = NULL;
  bool accepted = false;
  ObSEArray<SemiInfo*, 4> semi_infos;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param has null", K(ret), K(stmt), K(ctx_));
  } else if (stmt->get_stmt_hint().enable_no_unnest()) {
    // do nothing
  } else if (OB_FAIL(semi_infos.assign(stmt->get_semi_infos()))) {
    LOG_WARN("failed to assign semi infos", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < semi_infos.count(); ++i) {
      SemiInfo* semi_info = semi_infos.at(i);
      ObDMLStmt* trans_stmt = NULL;
      bool need_check_cost = false;
      bool happened = false;
      if (!parent_stmts.empty()) {
        root_stmt = parent_stmts.at(parent_stmts.count() - 1).stmt_;
      } else {
        root_stmt = stmt;
      }
      if (OB_ISNULL(semi_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null semi info", K(ret));
      } else if (semi_info->is_anti_join()) {
        // do nothing
      } else if (OB_FAIL(transform_semi_to_inner(root_stmt, stmt, semi_info, trans_stmt, need_check_cost, happened))) {
        LOG_WARN("failed to transform semi join to inner join", K(ret));
      } else if (!happened) {
        LOG_TRACE("semi join can not transform to inner join", K(*semi_info));
      } else if (!need_check_cost) {
        trans_happened = true;
        LOG_TRACE("succeed to transform one semi join to inner join due to rule", K(*stmt), K(*semi_info));
      } else if (stmt->get_stmt_hint().enable_unnest()) {
        trans_happened = true;
        stmt = trans_stmt;
        LOG_TRACE("succeed to transform one semi join to inner join due to hint", K(*stmt), K(*semi_info));
      } else if (OB_FAIL(accept_transform(parent_stmts, stmt, trans_stmt, accepted))) {
        LOG_WARN("failed to accept transform", K(ret));
      } else if (!accepted) {
        if (OB_FAIL(add_ignore_semi_info(semi_info->semi_id_))) {
          LOG_WARN("failed to add ignore semi info", K(ret));
        } else {
          LOG_TRACE("semi join can not transform to inner join due to cost", K(*semi_info));
        }
      } else {
        ctx_->happened_cost_based_trans_ |= SEMI_TO_INNER;
        trans_happened = true;
        LOG_TRACE("succeed to transform one semi join to inner join due to cost", K(*stmt), K(*semi_info));
      }
    }
  }
  return ret;
}

int ObTransformSemiToInner::transform_semi_to_inner(ObDMLStmt* root_stmt, ObDMLStmt* stmt,
    const SemiInfo* pre_semi_info, ObDMLStmt*& trans_stmt, bool& need_check_cost, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  bool is_valid = false;
  SemiInfo* semi_info = NULL;
  bool need_add_distinct = false;
  bool is_all_equal_cond = false;
  bool ignore = false;
  bool need_add_limit_constraint = false;
  ObSEArray<ObRawExpr*, 4> left_exprs;
  ObSEArray<ObRawExpr*, 4> right_exprs;
  trans_stmt = NULL;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->stmt_factory_) || OB_ISNULL(ctx_->expr_factory_) || OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("has null param", K(ret));
  } else if (OB_FAIL(get_semi_info(*stmt, pre_semi_info->semi_id_, semi_info))) {
    LOG_WARN("failed to get semi info", K(ret));
  } else if (NULL == semi_info) {
    /* do nothing */
  } else if (OB_FAIL(check_basic_validity(root_stmt,
                 *stmt,
                 *semi_info,
                 is_valid,
                 need_add_distinct,
                 need_check_cost,
                 need_add_limit_constraint))) {
    LOG_WARN("failed to check basic validity", K(ret));
  } else if (!is_valid) {
  } else if (!need_check_cost && OB_FALSE_IT(trans_stmt = stmt)) {
  } else if (OB_FAIL(is_ignore_semi_info(pre_semi_info->semi_id_, ignore))) {
    LOG_WARN("failed to check is ignore semi info", K(ret));
  } else if (ignore) {
    LOG_TRACE("semi info has check cost", K(*semi_info));
  } else if (need_check_cost &&
             OB_FAIL(ObTransformUtils::deep_copy_stmt(*ctx_->stmt_factory_, *ctx_->expr_factory_, stmt, trans_stmt))) {
    LOG_WARN("failed to deep copy stmt", K(ret));
  } else if (OB_ISNULL(trans_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  } else if (OB_FAIL(get_semi_info(*trans_stmt, pre_semi_info->semi_id_, semi_info))) {
    LOG_WARN("failed to get semi info", K(ret));
  } else if (OB_ISNULL(semi_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null semi info", K(ret));
  } else if (OB_FAIL(check_semi_join_condition(*trans_stmt, *semi_info, left_exprs, right_exprs, is_all_equal_cond))) {
    LOG_WARN("failed to check semi join condition", K(ret));
  } else if (OB_FAIL(do_transform(*trans_stmt, semi_info, left_exprs, right_exprs, need_add_distinct))) {
    LOG_WARN("failed to do transform semi to inner", K(ret));
    // Just in case different parameters hit same plan, firstly we need add const param constraint
  } else if (need_add_limit_constraint && OB_FAIL(ObTransformUtils::add_const_param_constraints(
                                              stmt->get_limit_expr(), stmt->get_query_ctx(), ctx_))) {
    LOG_WARN("failed to add const param constraints", K(ret));
  } else {
    trans_happened = true;
  }
  return ret;
}

int ObTransformSemiToInner::check_basic_validity(ObDMLStmt* root_stmt, ObDMLStmt& stmt, SemiInfo& semi_info,
    bool& is_valid, bool& need_add_distinct, bool& need_check_cost, bool& need_add_limit_constraint)
{
  int ret = OB_SUCCESS;
  bool is_unique = false;
  bool is_all_equal_cond = false;
  bool is_one_row = false;
  bool is_non_sens_dup_vals = false;
  TableItem* right_table = NULL;
  ObSEArray<ObRawExpr*, 4> left_exprs;
  ObSEArray<ObRawExpr*, 4> right_exprs;
  is_valid = false;
  need_add_distinct = true;
  need_check_cost = false;
  need_add_limit_constraint = false;
  if (OB_ISNULL(right_table = stmt.get_table_item_by_id(semi_info.right_table_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get table items", K(ret), K(semi_info));
  } else if (OB_FAIL(check_semi_join_condition(stmt, semi_info, left_exprs, right_exprs, is_all_equal_cond))) {
    LOG_WARN("failed to check semi join condition", K(ret));
  } else if (OB_FAIL(check_right_table_output_one_row(stmt, *right_table, is_one_row))) {
    LOG_WARN("failed to check right tables output one row", K(ret));
  } else if (is_one_row) {
    is_valid = true;
    need_add_distinct = false;
    LOG_TRACE("semi right table output one row");
  } else if (OB_FAIL(check_right_exprs_unique(stmt, right_table, right_exprs, is_unique))) {
    LOG_WARN("failed to check exprs unique on table items", K(ret));
  } else if (is_unique) {
    is_valid = true;
    need_add_distinct = false;
    LOG_TRACE("semi right table output is unique");
  } else if (OB_FAIL(
                 check_stmt_is_non_sens_dul_vals(root_stmt, &stmt, is_non_sens_dup_vals, need_add_limit_constraint))) {
    LOG_WARN("failed to check stmt is non sens dul vals", K(ret));
  } else if (is_non_sens_dup_vals) {
    is_valid = true;
    need_add_distinct = false;
    LOG_TRACE("stmt isn't sensitive to result of subquery has duplicated values");
  } else if (!is_all_equal_cond) {
    is_valid = false;
    LOG_TRACE("semi right table output is not unique and semi condition is not all equal");
  } else if (OB_FAIL(check_can_add_distinct(left_exprs, right_exprs, is_valid))) {
    LOG_WARN("failed to check can add distinct", K(ret));
  } else if (!is_valid) {
    LOG_TRACE("semi right table output can not add distinct");
  } else if (OB_FAIL(
                 check_join_condition_match_index(root_stmt, stmt, semi_info, semi_info.semi_conditions_, is_valid))) {
    LOG_WARN("failed to check join condition match index", K(ret));
  } else {
    need_check_cost = is_valid | need_add_distinct;
    LOG_TRACE("semi condition match index", K(is_valid), K(need_add_distinct), K(need_check_cost));
  }
  return ret;
}

// check right_exprs is unique on right_table.
// if right_table is generate table, check right_table ref_query unique.
int ObTransformSemiToInner::check_right_exprs_unique(
    ObDMLStmt& stmt, TableItem* right_table, ObIArray<ObRawExpr*>& right_exprs, bool& is_unique)
{
  int ret = OB_SUCCESS;
  is_unique = false;
  ObSelectStmt* ref_query = NULL;
  if (OB_ISNULL(right_table) || OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null ctx", K(ret));
  } else if (!right_table->is_generated_table()) {
    // baisc table
    ObSEArray<TableItem*, 1> right_tables;
    ObSEArray<ObRawExpr*, 1> dummy_conds;
    if (OB_FAIL(right_tables.push_back(right_table))) {
      LOG_WARN("failed to push back table", K(ret));
    } else if (OB_FAIL(ObTransformUtils::check_exprs_unique_on_table_items(&stmt,
                   ctx_->session_info_,
                   ctx_->schema_checker_,
                   right_tables,
                   right_exprs,
                   dummy_conds,
                   false,
                   is_unique))) {
      LOG_WARN("failed to check exprs unique on table items", K(ret));
    }
  } else if (OB_ISNULL(ref_query = right_table->ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null ref query", K(ret));
  } else {
    ObSEArray<ObRawExpr*, 4> right_cols;
    ObSEArray<ObRawExpr*, 4> right_select_exprs;
    for (int64_t i = 0; OB_SUCC(ret) && i < right_exprs.count(); ++i) {
      if (OB_ISNULL(right_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null expr", K(ret));
      } else if (right_exprs.at(i)->is_column_ref_expr()) {
        ret = right_cols.push_back(right_exprs.at(i));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(
                   ObTransformUtils::convert_column_expr_to_select_expr(right_cols, *ref_query, right_select_exprs))) {
      LOG_WARN("failed to convert column expr to select expr", K(ret));
    } else if (OB_FAIL(ObTransformUtils::check_stmt_unique(
                   ref_query, ctx_->session_info_, ctx_->schema_checker_, right_select_exprs, false, is_unique))) {
      LOG_WARN("failed to check ref query unique", K(ret));
    }
  }
  return ret;
}

int ObTransformSemiToInner::check_semi_join_condition(ObDMLStmt& stmt, SemiInfo& semi_info,
    ObIArray<ObRawExpr*>& left_exprs, ObIArray<ObRawExpr*>& right_exprs, bool& is_all_equal_cond)
{
  int ret = OB_SUCCESS;
  ObSqlBitSet<> left_table_set;
  ObSqlBitSet<> right_table_set;
  ObIArray<ObRawExpr*>& semi_conditions = semi_info.semi_conditions_;
  is_all_equal_cond = true;
  if (OB_FAIL(ObTransformUtils::get_table_rel_ids(stmt, semi_info.left_table_ids_, left_table_set))) {
    LOG_WARN("failed to get table rel ids", K(ret));
  } else if (OB_FAIL(ObTransformUtils::get_table_rel_ids(stmt, semi_info.right_table_id_, right_table_set))) {
    LOG_WARN("failed to get table rel ids", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < semi_conditions.count(); ++i) {
    ObRawExpr* expr = semi_conditions.at(i);
    ObRawExpr* left = NULL;
    ObRawExpr* right = NULL;
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null expr", K(ret));
    } else if (left_table_set.is_superset(expr->get_relation_ids())) {
      // do nothing for left filters
    } else if (right_table_set.is_superset(expr->get_relation_ids())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected semi condition", K(ret), K(*expr));
    } else if (T_OP_EQ != expr->get_expr_type()) {
      is_all_equal_cond = false;
    } else if (OB_ISNULL(left = expr->get_param_expr(0)) || OB_ISNULL(right = expr->get_param_expr(1))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null param", K(*expr), K(ret));
    } else if (!right_table_set.overlap(left->get_relation_ids()) &&
               !left_table_set.overlap(right->get_relation_ids())) {
      if (OB_FAIL(left_exprs.push_back(left))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else if (OB_FAIL(right_exprs.push_back(right))) {
        LOG_WARN("failed to push back expr", K(ret));
      }
    } else if (!right_table_set.overlap(right->get_relation_ids()) &&
               !left_table_set.overlap(left->get_relation_ids())) {
      if (OB_FAIL(left_exprs.push_back(right))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else if (OB_FAIL(right_exprs.push_back(left))) {
        LOG_WARN("failed to push back expr", K(ret));
      }
    } else {
      is_all_equal_cond = false;
    }
  }
  return ret;
}

int ObTransformSemiToInner::check_right_table_output_one_row(ObDMLStmt& stmt, TableItem& right_table, bool& is_one_row)
{
  int ret = OB_SUCCESS;
  is_one_row = false;
  if (right_table.is_generated_table()) {
    ObPhysicalPlanCtx* plan_ctx = NULL;
    if (OB_ISNULL(right_table.ref_query_) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->exec_ctx_) ||
        OB_ISNULL(plan_ctx = ctx_->exec_ctx_->get_physical_plan_ctx())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null param", K(right_table), K(ret));
    } else if (OB_FAIL(ObTransformUtils::check_limit_value(*right_table.ref_query_,
                   plan_ctx->get_param_store(),
                   ctx_->session_info_,
                   ctx_->allocator_,
                   1,
                   is_one_row,
                   true,
                   stmt.get_query_ctx()))) {
      LOG_WARN("failed to check limit value", K(ret));
    }
  }
  return ret;
}

int ObTransformSemiToInner::get_semi_info(ObDMLStmt& stmt, const uint64_t semi_id, SemiInfo*& semi_info)
{
  int ret = OB_SUCCESS;
  semi_info = NULL;
  SemiInfo* cur_semi_info = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && NULL == semi_info && i < stmt.get_semi_info_size(); ++i) {
    if (OB_ISNULL(cur_semi_info = stmt.get_semi_infos().at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null", K(ret), K(stmt.get_semi_infos()));
    } else if (semi_id == cur_semi_info->semi_id_) {
      semi_info = cur_semi_info;
    }
  }
  return ret;
}

int ObTransformSemiToInner::check_can_add_distinct(
    const ObIArray<ObRawExpr*>& left_exprs, const ObIArray<ObRawExpr*>& right_exprs, bool& is_valid)
{
  int ret = OB_SUCCESS;
  bool need_add_cast = false;
  is_valid = true;
  if (left_exprs.count() != right_exprs.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect expr count", K(left_exprs), K(right_exprs), K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < left_exprs.count(); ++i) {
    ObRawExpr* left = left_exprs.at(i);
    ObRawExpr* right = right_exprs.at(i);
    if (OB_ISNULL(left) || OB_ISNULL(right)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null expr", K(ret));
    } else if (OB_FAIL(check_need_add_cast(left, right, need_add_cast, is_valid))) {
      LOG_WARN("failed to check need add cast", K(ret));
    }
  }
  return ret;
}

int ObTransformSemiToInner::check_need_add_cast(
    const ObRawExpr* left_arg, const ObRawExpr* right_arg, bool& need_add_cast, bool& is_valid)
{
  int ret = OB_SUCCESS;
  need_add_cast = false;
  is_valid = false;
  bool is_equal = false;
  if (OB_ISNULL(left_arg) || OB_ISNULL(right_arg)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("left arg and right arg should not be NULL", K(left_arg), K(right_arg), K(ret));
  } else if (OB_FAIL(ObRelationalExprOperator::is_equivalent(
                 left_arg->get_result_type(), left_arg->get_result_type(), right_arg->get_result_type(), is_valid))) {
    LOG_WARN("failed to check expr is equivalent", K(ret));
  } else if (!is_valid) {
    LOG_TRACE("can not use left expr type as the (left, right) compare type", K(is_valid));
  } else if (OB_FAIL(ObRelationalExprOperator::is_equivalent(
                 left_arg->get_result_type(), right_arg->get_result_type(), right_arg->get_result_type(), is_equal))) {
    LOG_WARN("failed to check expr is equivalent", K(ret));
  } else if (!is_equal) {
    need_add_cast = true;
  }
  return ret;
}

int ObTransformSemiToInner::check_join_condition_match_index(ObDMLStmt* root_stmt, ObDMLStmt& stmt, SemiInfo& semi_info,
    const ObIArray<ObRawExpr*>& semi_conditions, bool& is_match_index)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObDMLStmt*, 2> dummy_stmts;
  ObSqlBitSet<> left_table_set;
  is_match_index = false;
  if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx is null", K(ret));
  } else if (OB_FAIL(ObTransformUtils::get_table_rel_ids(stmt, semi_info.left_table_ids_, left_table_set))) {
    LOG_WARN("failed to get table rel ids", K(ret));
  }
  // check semi condition is match left table
  for (int64_t i = 0; OB_SUCC(ret) && !is_match_index && i < semi_conditions.count(); ++i) {
    ObRawExpr* expr = semi_conditions.at(i);
    ObSEArray<ObRawExpr*, 8> column_exprs;
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null expr", K(ret));
    } else if (OB_FAIL(ObTransformUtils::extract_column_exprs(
                   expr, stmt.get_current_level(), left_table_set, dummy_stmts, column_exprs))) {
      LOG_WARN("failed to extract column exprs", K(ret));
    } else {
      for (int64_t j = 0; OB_SUCC(ret) && !is_match_index && j < column_exprs.count(); ++j) {
        ObRawExpr* e = column_exprs.at(j);
        ObColumnRefRawExpr* col_expr = NULL;
        if (OB_ISNULL(e)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpect null expr", K(ret));
        } else if (!e->is_column_ref_expr()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expect column ref expr", K(*e), K(ret));
        } else if (OB_FALSE_IT(col_expr = static_cast<ObColumnRefRawExpr*>(e))) {
        } else if (OB_FAIL(ObTransformUtils::check_column_match_index(
                       root_stmt, &stmt, ctx_->sql_schema_guard_, col_expr, is_match_index))) {
          LOG_WARN("failed to check column expr is match index", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTransformSemiToInner::do_transform(ObDMLStmt& stmt, SemiInfo* semi_info, const ObIArray<ObRawExpr*>& left_exprs,
    const ObIArray<ObRawExpr*>& right_exprs, bool need_add_distinct)
{
  int ret = OB_SUCCESS;
  TableItem* view_table = NULL;
  ObSelectStmt* ref_query = NULL;
  ObSEArray<ObRawExpr*, 2> dummy_filters;
  if (OB_ISNULL(ctx_) || OB_ISNULL(semi_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null ctx", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::remove_item(stmt.get_semi_infos(), semi_info))) {
    LOG_WARN("failed to remove semi info", K(ret));
  } else if (OB_FAIL(append(stmt.get_condition_exprs(), semi_info->semi_conditions_))) {
    LOG_WARN("failed to append semi conditions", K(ret));
  } else if (!need_add_distinct) {
    ret = stmt.add_from_item(semi_info->right_table_id_, false);
  } else if (OB_FAIL(ObTransformUtils::create_view_with_from_items(&stmt,
                 ctx_,
                 stmt.get_table_item_by_id(semi_info->right_table_id_),
                 right_exprs,
                 dummy_filters,
                 view_table))) {
    LOG_WARN("failed to merge from items as inner join", K(ret));
  } else if (OB_ISNULL(view_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null table item", K(ret));
  } else if (!view_table->is_generated_table()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expect generated table item", K(*view_table), K(ret));
  } else if (OB_ISNULL(ref_query = view_table->ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null ref query", K(ret));
  } else if (OB_FAIL(add_distinct(*ref_query, left_exprs, right_exprs))) {
    LOG_WARN("failed to add distinct exprs", K(ret));
  } else if (OB_FAIL(ref_query->add_from_item(semi_info->right_table_id_, false))) {
    LOG_WARN("failed to add from items", K(ret));
  } else if (OB_FAIL(stmt.add_from_item(view_table->table_id_, false))) {
    LOG_WARN("failed to add from items", K(ret));
  }
  return ret;
}

int ObTransformSemiToInner::add_distinct(
    ObSelectStmt& view, const ObIArray<ObRawExpr*>& left_exprs, const ObIArray<ObRawExpr*>& right_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null ctx", K(ret));
  } else if (left_exprs.count() != right_exprs.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect expr count", K(left_exprs), K(right_exprs), K(ret));
  }
  view.assign_distinct();
  for (int64_t i = 0; OB_SUCC(ret) && i < view.get_select_item_size(); ++i) {
    SelectItem& item = view.get_select_item(i);
    ObRawExpr* left = NULL;
    ObRawExpr* right = NULL;
    ObSysFunRawExpr* cast_expr = NULL;
    bool need_add_cast = false;
    for (int64_t j = 0; OB_SUCC(ret) && !need_add_cast && j < left_exprs.count(); ++j) {
      left = left_exprs.at(j);
      right = right_exprs.at(j);
      bool is_valid = false;
      if (OB_ISNULL(left) || OB_ISNULL(right)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null expr", K(ret));
      } else if (right != item.expr_) {
        // do nothing
      } else if (OB_FAIL(check_need_add_cast(left, right, need_add_cast, is_valid))) {
        LOG_WARN("failed to check need add cast", K(ret));
      } else if (!is_valid) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expect valid cast expr", K(ret));
      } else if (need_add_cast) {
        LOG_TRACE("need cast expr", K(*left), K(*right));
      }
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (!need_add_cast) {
      // do nothing
    } else if (OB_ISNULL(left) || OB_ISNULL(right)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::create_cast_expr(
                   *ctx_->expr_factory_, right, left->get_result_type(), cast_expr, ctx_->session_info_))) {
      LOG_WARN("failed to create cast expr", K(ret));
    } else {
      item.expr_ = cast_expr;
    }
  }
  return ret;
}

int ObTransformSemiToInner::add_ignore_semi_info(const uint64_t semi_id)
{
  int ret = OB_SUCCESS;
  SemiInfo* semi_info = NULL;
  if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx has null param", K(ret));
  } else if (OB_FAIL(ctx_->ignore_semi_infos_.push_back(semi_id))) {
    LOG_WARN("failed to push back ignore semi info", K(ret));
  }
  return ret;
}

int ObTransformSemiToInner::is_ignore_semi_info(const uint64_t semi_id, bool& ignore)
{
  int ret = OB_SUCCESS;
  ignore = false;
  if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx has null param", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && !ignore && i < ctx_->ignore_semi_infos_.count(); ++i) {
    if (ctx_->ignore_semi_infos_.at(i) == semi_id) {
      ignore = true;
    }
  }
  return ret;
}

int ObTransformSemiToInner::check_stmt_is_non_sens_dul_vals(
    ObDMLStmt* root_stmt, ObDMLStmt* stmt, bool& is_match, bool& need_add_limit_constraint)
{
  int ret = OB_SUCCESS;
  is_match = false;
  need_add_limit_constraint = false;
  if (OB_ISNULL(root_stmt) || OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(root_stmt), K(stmt), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && !is_match && i < root_stmt->get_condition_size(); ++i) {
      const ObRawExpr* cond_expr = root_stmt->get_condition_expr(i);
      const ObRawExpr* param_expr = NULL;
      if (OB_ISNULL(cond_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null1", K(ret), K(cond_expr));
      } else if (T_OP_EXISTS == cond_expr->get_expr_type() || T_OP_NOT_EXISTS == cond_expr->get_expr_type()) {
        if (OB_ISNULL(param_expr = cond_expr->get_param_expr(0))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(param_expr), K(ret));
        } else if (!param_expr->is_query_ref_expr()) {
          /*do nothing*/
        } else if (static_cast<const ObQueryRefRawExpr*>(param_expr)->get_ref_stmt() == stmt) {
          if (static_cast<const ObQueryRefRawExpr*>(param_expr)->get_ref_stmt()->is_spj()) {
            is_match = true;
          } else if (OB_FAIL(
                         check_stmt_limit_validity(static_cast<const ObQueryRefRawExpr*>(param_expr)->get_ref_stmt(),
                             is_match,
                             need_add_limit_constraint))) {
            LOG_WARN("failed to check stmt limit validity", K(ret));
          } else { /*do nothing */
          }
        }
      } else if (cond_expr->has_flag(IS_WITH_ALL) || cond_expr->has_flag(IS_WITH_ANY)) {
        if (OB_ISNULL(param_expr = cond_expr->get_param_expr(1))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL pointer error", K(param_expr), K(ret));
        } else if (!param_expr->is_query_ref_expr()) {
          /*do nothing*/
        } else if (static_cast<const ObQueryRefRawExpr*>(param_expr)->get_ref_stmt() == stmt) {
          is_match = static_cast<const ObQueryRefRawExpr*>(param_expr)->get_ref_stmt()->is_spj();
        }
      } else {
        /*do nothing*/
      }
    }
  }
  return ret;
}

int ObTransformSemiToInner::check_stmt_limit_validity(
    const ObSelectStmt* select_stmt, bool& is_valid, bool& need_add_const_constraint)
{
  int ret = OB_SUCCESS;
  bool has_rownum = false;
  ObPhysicalPlanCtx* plan_ctx = NULL;
  is_valid = false;
  if (OB_ISNULL(select_stmt) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->exec_ctx_) ||
      OB_ISNULL(plan_ctx = ctx_->exec_ctx_->get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(select_stmt), K(ctx_), K(ctx_->exec_ctx_), K(plan_ctx));
  } else if (!select_stmt->has_limit() || select_stmt->get_offset_expr() != NULL ||
             select_stmt->get_limit_percent_expr() != NULL) {
    // do nothing
  } else if (select_stmt->has_distinct() || select_stmt->has_group_by() || select_stmt->has_set_op() ||
             select_stmt->is_hierarchical_query() || select_stmt->has_order_by() ||
             select_stmt->is_contains_assignment() || select_stmt->has_window_function() ||
             select_stmt->has_sequence()) {
    /*do nothing*/
  } else if (OB_FAIL(select_stmt->has_rownum(has_rownum))) {
    LOG_WARN("failed to has rownum", K(ret));
  } else if (has_rownum) {
    /*do nothing */
  } else {
    bool is_null_value = false;
    int64_t limit_value = 0;
    if (OB_FAIL(ObTransformUtils::get_limit_value(select_stmt->get_limit_expr(),
            select_stmt,
            &plan_ctx->get_param_store(),
            ctx_->session_info_,
            ctx_->allocator_,
            limit_value,
            is_null_value))) {
      LOG_WARN("failed to get_limit_value", K(ret));
    } else if (!is_null_value && limit_value >= 1) {
      is_valid = true;
      // Just in case different parameters hit same plan, firstly we need add const param constraint
      need_add_const_constraint = true;
    }
  }
  return ret;
}
