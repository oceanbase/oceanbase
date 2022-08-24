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
#include "sql/rewrite/ob_transform_predicate_move_around.h"
#include "sql/rewrite/ob_stmt_comparer.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/rewrite/ob_predicate_deduce.h"
#include "share/schema/ob_table_schema.h"
#include "common/ob_smart_call.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;

/**
 * @brief ObTransformPredicateMoveAround::transform_one_stmt
 * @param parent_stmts
 * @param stmt
 * @param trans_happened
 * @return
 */
int ObTransformPredicateMoveAround::transform_one_stmt(
    common::ObIArray<ObParentDMLStmt>& parent_stmts, ObDMLStmt*& stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  ObArray<ObRawExpr*> dummy_pullup;
  ObArray<ObRawExpr*> dummy_pushdown;
  ObArray<int64_t> dummy_list;
  UNUSED(parent_stmts);
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret), K(stmt));
  } else if (!stmt_map_.created() && OB_FAIL(stmt_map_.create(20, ObModIds::OB_SQL_COMPILE))) {
    LOG_WARN("failed to create stmt map", K(ret));
  } else if (!stmt->is_insert_stmt() || stmt->get_condition_size() != 0) {
    // do nothing
  } else if (OB_FAIL(create_equal_exprs_for_insert(static_cast<ObInsertStmt*>(stmt)))) {
    LOG_WARN("failed to create equal exprs for insert", K(ret));
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(pullup_predicates(stmt, dummy_list, dummy_pullup))) {
      LOG_WARN("failed to pull up predicates", K(ret));
    } else if (OB_FAIL(pushdown_predicates(stmt, dummy_pushdown))) {
      LOG_WARN("failed to push down predicates", K(ret));
    } else {
      trans_happened = trans_happened_;
    }
  }
  return ret;
}

ObTransformPredicateMoveAround::ObTransformPredicateMoveAround(ObTransformerCtx* ctx)
    : ObTransformRule(ctx, TransMethod::POST_ORDER), allocator_("PredDeduce")
{}

ObTransformPredicateMoveAround::~ObTransformPredicateMoveAround()
{
  for (int64_t i = 0; i < stmt_pullup_preds_.count(); ++i) {
    if (NULL != stmt_pullup_preds_.at(i)) {
      stmt_pullup_preds_.at(i)->~PullupPreds();
      stmt_pullup_preds_.at(i) = NULL;
    }
  }
}

/**
 * @brief ObTransformPredicateMoveAround::need_rewrite
 *   a stmt need rewrite if
 * 1. the stmt is not a generated table
 * @return
 */
bool ObTransformPredicateMoveAround::need_rewrite(
    const common::ObIArray<ObParentDMLStmt>& parent_stmts, const ObDMLStmt& stmt)
{
  bool bret = true;
  ObDMLStmt* parent = NULL;
  if (parent_stmts.empty()) {
    // do nothing
  } else if (OB_ISNULL(parent = parent_stmts.at(parent_stmts.count() - 1).stmt_)) {
    // do nothing
  } else if (parent->get_current_level() != stmt.get_current_level()) {
    // the stmt is a subquery, need rewrite
  } else {
    // the stmt is a generated table
    bret = false;
  }
  return bret;
}

int ObTransformPredicateMoveAround::pullup_predicates(
    ObDMLStmt* stmt, ObIArray<int64_t>& sel_ids, ObIArray<ObRawExpr*>& output_pullup_preds)
{
  int ret = OB_SUCCESS;
  bool is_overflow = false;
  ObIArray<ObRawExpr*>* input_pullup_preds = NULL;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret), K(stmt));
  } else if (OB_FAIL(check_stack_overflow(is_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (is_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_overflow));
  } else if (OB_FAIL(acquire_transform_params(stmt, input_pullup_preds))) {
    LOG_WARN("failed to acquire pullup preds", K(ret));
  } else if (OB_ISNULL(input_pullup_preds)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to acquire transform params", K(ret));
  } else if (stmt->is_set_stmt()) {
    ObSelectStmt* sel_stmt = static_cast<ObSelectStmt*>(stmt);
    ObSEArray<ObRawExpr*, 8> pullup_preds;
    if (OB_FAIL(SMART_CALL(pullup_predicates_from_set(sel_stmt, pullup_preds)))) {
      LOG_WARN("failed to pull up predicates from set stmt", K(ret));
    } else if (OB_FAIL(generate_set_pullup_predicates(*sel_stmt, sel_ids, pullup_preds, output_pullup_preds))) {
      LOG_WARN("generate set pullup preds failed", K(ret));
    } else if (OB_FAIL(acquire_transform_params(stmt, input_pullup_preds))) {
      LOG_WARN("failed to acquire pullup preds", K(ret));
    } else if (OB_ISNULL(input_pullup_preds)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to acquire transform params", K(ret));
    } else if (OB_FAIL(input_pullup_preds->assign(pullup_preds))) {
      LOG_WARN("assign pullup preds failed", K(ret));
    } else { /*do nothing*/
    }
  } else if (OB_FAIL(preprocess(*stmt))) {
    LOG_WARN("failed to preprocess stmt", K(ret));
  } else if (OB_FAIL(pullup_predicates_from_view(*stmt, sel_ids, *input_pullup_preds))) {
    LOG_WARN("failed to pullup predicates from view", K(ret));
  } else if (!stmt->is_select_stmt() || sel_ids.empty()) {
    // do nothing
  } else if (OB_FAIL(generate_pullup_predicates(
                 static_cast<ObSelectStmt&>(*stmt), sel_ids, *input_pullup_preds, output_pullup_preds))) {
    LOG_WARN("failed to generate pullup predicates", K(ret));
  }
  return ret;
}

int ObTransformPredicateMoveAround::preprocess(ObDMLStmt& stmt)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_semi_infos().count(); ++i) {
    if (OB_FAIL(preprocess_semi_info(stmt, stmt.get_semi_infos().at(i), stmt.get_condition_exprs()))) {
      LOG_WARN("failed to preprocess joined table", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_joined_tables().count(); ++i) {
    if (OB_FAIL(preprocess_joined_table(stmt.get_joined_tables().at(i), stmt.get_condition_exprs()))) {
      LOG_WARN("failed to preprocess joined table", K(ret));
    }
  }
  return ret;
}

int ObTransformPredicateMoveAround::preprocess_joined_table(JoinedTable* join_table, ObIArray<ObRawExpr*>& upper_conds)
{
  int ret = OB_SUCCESS;
  bool is_overflow = false;
  if (OB_ISNULL(join_table) || OB_ISNULL(join_table->left_table_) || OB_ISNULL(join_table->right_table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("joined table is null", K(ret));
  } else if (join_table->is_inner_join() && !join_table->join_conditions_.empty()) {
    if (OB_FAIL(append(upper_conds, join_table->join_conditions_))) {
      LOG_WARN("failed to append conditions", K(ret));
    } else {
      join_table->join_conditions_.reset();
      trans_happened_ = true;
    }
  }
  if (OB_SUCC(ret) && join_table->left_table_->is_joined_table() &&
      (join_table->is_inner_join() || join_table->is_left_join())) {
    if (OB_FAIL(SMART_CALL(preprocess_joined_table(static_cast<JoinedTable*>(join_table->left_table_), upper_conds)))) {
      LOG_WARN("failed to process joined table", K(ret));
    }
  }
  if (OB_SUCC(ret) && join_table->right_table_->is_joined_table() &&
      (join_table->is_inner_join() || join_table->is_right_join())) {
    if (OB_FAIL(
            SMART_CALL(preprocess_joined_table(static_cast<JoinedTable*>(join_table->right_table_), upper_conds)))) {
      LOG_WARN("failed to process joined table", K(ret));
    }
  }
  return ret;
}

int ObTransformPredicateMoveAround::preprocess_semi_info(
    ObDMLStmt& stmt, SemiInfo* semi_info, ObIArray<ObRawExpr*>& upper_conds)
{
  int ret = OB_SUCCESS;
  ObSqlBitSet<> left_rel_ids;
  if (OB_ISNULL(semi_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("semi info is null", K(ret));
  } else if (semi_info->is_anti_join()) {
    // do not pull up anti conditions to upper conds
  } else if (OB_FAIL(stmt.get_table_rel_ids(semi_info->left_table_ids_, left_rel_ids))) {
    LOG_WARN("failed to get table rel ids", K(ret));
  } else {
    ObRawExpr* expr = NULL;
    ObIArray<ObRawExpr*>& semi_conds = semi_info->semi_conditions_;
    ObSEArray<ObRawExpr*, 4> left_filters;
    for (int64_t i = 0; OB_SUCC(ret) && i < semi_conds.count(); ++i) {
      if (OB_ISNULL(expr = semi_conds.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret));
      } else if (!expr->get_expr_levels().has_member(stmt.get_current_level()) ||
                 !expr->get_relation_ids().is_subset(left_rel_ids)) {
        /*do nothing*/
      } else if (OB_FAIL(left_filters.push_back(expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      }
    }
    if (OB_FAIL(ret) || left_filters.empty()) {
      /*do nothing*/
    } else if (OB_FAIL(append(upper_conds, left_filters))) {
      LOG_WARN("failed to append conditions", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::remove_item(semi_conds, left_filters))) {
      LOG_WARN("failed to remove item", K(ret));
    } else {
      trans_happened_ = true;
    }
  }
  return ret;
}

int ObTransformPredicateMoveAround::pullup_predicates_from_view(
    ObDMLStmt& stmt, ObIArray<int64_t>& sel_ids, ObIArray<ObRawExpr*>& input_pullup_preds)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> filter_columns;
  if (stmt.get_stmt_hint().enable_no_pred_deduce()) {
    // does not pull up any predicates from views
  } else if (OB_FAIL(get_columns_in_filters(stmt, sel_ids, filter_columns))) {
    LOG_WARN("failed to get columns in filters", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_table_size(); ++i) {
    TableItem* table_item = NULL;
    ObSEArray<ObRawExpr*, 4> view_preds;
    ObSEArray<int64_t, 4> view_sel_list;
    bool on_null_side = false;
    if (OB_ISNULL(table_item = stmt.get_table_items().at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table item is null", K(ret));
    } else if (!table_item->is_generated_table() || table_item->is_fake_cte_table()) {
      // do nothing
    } else if (OB_FAIL(ObOptimizerUtil::is_table_on_null_side(&stmt, table_item->table_id_, on_null_side))) {
      LOG_WARN("failed to check is table on null side", K(ret));
    } else if (on_null_side) {
      // do nothing
    } else if (OB_FAIL(choose_pullup_columns(*table_item, filter_columns, view_sel_list))) {
      LOG_WARN("failed to get column exprs", K(ret));
    } else if (OB_FAIL(SMART_CALL(pullup_predicates(table_item->ref_query_, view_sel_list, view_preds)))) {
      LOG_WARN("failed to pull up predicate", K(ret));
    } else if (OB_FAIL(rename_pullup_predicates(stmt, *table_item, view_sel_list, view_preds))) {
      LOG_WARN("failed to rename pullup predicates", K(ret));
    } else if (OB_FAIL(append(input_pullup_preds, view_preds))) {
      LOG_WARN("failed to append expr", K(ret));
    }
    // select ... from (select c1 as a, c1 as b from ...),
    // potentially, we can pull up a predicate (a = b)
  }
  return ret;
}

int ObTransformPredicateMoveAround::generate_set_pullup_predicates(ObSelectStmt& stmt, ObIArray<int64_t>& select_list,
    ObIArray<ObRawExpr*>& input_pullup_preds, ObIArray<ObRawExpr*>& output_pullup_preds)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> select_exprs;
  for (int64_t i = 0; OB_SUCC(ret) && i < select_list.count(); ++i) {
    int64_t idx = select_list.at(i);
    ObRawExpr* sel_expr = NULL;
    if (OB_UNLIKELY(idx < 0 || idx >= stmt.get_select_item_size()) ||
        OB_ISNULL(sel_expr = stmt.get_select_item(idx).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("select expr is null", K(ret), K(sel_expr), K(idx));
    } else if (!sel_expr->is_set_op_expr()) {
    } else if (OB_FAIL(select_exprs.push_back(sel_expr))) {
      LOG_WARN("failed to push back select expr", K(ret));
    } else { /*do nothing*/
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < input_pullup_preds.count(); ++i) {
    bool can_be = true;
    if (OB_ISNULL(input_pullup_preds.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("input pullup predicate is null", K(ret));
    } else if (OB_FAIL(check_expr_pullup_validity(input_pullup_preds.at(i), select_exprs, can_be))) {
      LOG_WARN("failed to check pullup validity", K(ret));
    } else if (!can_be) {
      // do nothing
    } else if (OB_FAIL(output_pullup_preds.push_back(input_pullup_preds.at(i)))) {
      LOG_WARN("failed to push back predicates", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObTransformPredicateMoveAround::pullup_predicates_from_set(ObSelectStmt* stmt, ObIArray<ObRawExpr*>& pullup_preds)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (OB_UNLIKELY(!stmt->is_set_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is not a set stmt", K(ret));
  } else if (stmt->is_recursive_union()) {
    ObArray<int64_t> dummy_sels;
    ObArray<ObRawExpr*> dummy_preds;
    if (OB_FAIL(SMART_CALL(pullup_predicates(stmt->get_set_query(0), dummy_sels, dummy_preds)))) {
      LOG_WARN("failed to push down predicates", K(ret));
    } else if (OB_FAIL(SMART_CALL(pullup_predicates(stmt->get_set_query(1), dummy_sels, dummy_preds)))) {
      LOG_WARN("failed to push down predicates", K(ret));
    } else { /*do nothing*/
    }
  } else {
    ObIArray<ObSelectStmt*>& child_query = stmt->get_set_query();
    const int64_t child_num = child_query.count();
    ObSEArray<ObRawExpr*, 16> left_output_preds;
    ObSEArray<ObRawExpr*, 16> right_output_preds;
    ObSEArray<ObRawExpr*, 16> pullup_output_preds;
    ObArray<int64_t> all_sels;
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_select_item_size(); ++i) {
      if (OB_FAIL(all_sels.push_back(i))) {
        LOG_WARN("push back select idx failed", K(ret));
      } else { /*do nothing*/
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < child_num; ++i) {
      pullup_output_preds.reset();
      right_output_preds.reset();
      if (OB_ISNULL(child_query.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret));
      } else if (OB_FAIL(SMART_CALL(pullup_predicates(child_query.at(i), all_sels, right_output_preds)))) {
        LOG_WARN("pullup preds from set failed", K(ret));
      } else if (OB_FAIL(rename_set_op_predicates(*child_query.at(i), *stmt, right_output_preds, true))) {
        LOG_WARN("rename predicates failed", K(ret));
      } else if (0 == i) {
        ret = left_output_preds.assign(right_output_preds);
      } else if (OB_FAIL(check_pullup_predicates(stmt, left_output_preds, right_output_preds, pullup_output_preds))) {
        LOG_WARN("choose pullup predicates failed", K(ret));
      } else if (OB_FAIL(left_output_preds.assign(pullup_output_preds))) {
        LOG_WARN("failed to assign exprs", K(ret));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(pullup_preds.assign(pullup_output_preds))) {
      LOG_WARN("failed to assign exprs", K(ret));
    }
  }
  return ret;
}

int ObTransformPredicateMoveAround::check_pullup_predicates(ObSelectStmt* stmt, ObIArray<ObRawExpr*>& left_pullup_preds,
    ObIArray<ObRawExpr*>& right_pullup_preds, ObIArray<ObRawExpr*>& output_pullup_preds)
{
  int ret = OB_SUCCESS;
  ObStmtCompareContext context;
  if (OB_ISNULL(stmt) || OB_ISNULL(stmt->get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (stmt->get_stmt_hint().enable_no_pred_deduce()) {
    // does not pull up any predicates
  } else if (OB_UNLIKELY(!stmt->is_set_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is not a set stmt", K(ret));
  } else if (OB_FAIL(context.init(stmt->get_query_ctx()))) {
    LOG_WARN("init stmt compare context failed", K(ret));
  } else if (stmt->get_set_op() == ObSelectStmt::UNION && !stmt->is_recursive_union()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < left_pullup_preds.count(); ++i) {
      bool find = false;
      for (int64_t j = 0; OB_SUCC(ret) && !find && j < right_pullup_preds.count(); ++j) {
        if (left_pullup_preds.at(i)->same_as(*right_pullup_preds.at(j), &context)) {
          find = true;
        } else { /*do nothing*/
        }
      }
      if (OB_UNLIKELY(!find)) {
        /*do nothing*/
      } else if (OB_FAIL(output_pullup_preds.push_back(left_pullup_preds.at(i)))) {
        LOG_WARN("push back preds failed", K(ret));
      } else { /*do nothing*/
      }
    }
    if (OB_FAIL(append(stmt->get_query_ctx()->all_equal_param_constraints_, context.equal_param_info_))) {
      LOG_WARN("append equal param info failed", K(ret));
    } else { /*do nothing*/
    }
  } else if (stmt->get_set_op() == ObSelectStmt::INTERSECT) {
    for (int64_t i = 0; OB_SUCC(ret) && i < left_pullup_preds.count(); ++i) {
      bool find = false;
      for (int64_t j = 0; OB_SUCC(ret) && !find && j < output_pullup_preds.count(); ++j) {
        if (left_pullup_preds.at(i)->same_as(*output_pullup_preds.at(j), &context)) {
          find = true;
        } else { /*do nothing*/
        }
      }
      if (find) {
        /*do nothing*/
      } else if (OB_FAIL(output_pullup_preds.push_back(left_pullup_preds.at(i)))) {
        LOG_WARN("push back preds failed", K(ret));
      } else { /*do nothing*/
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < right_pullup_preds.count(); ++i) {
      bool find = false;
      for (int64_t j = 0; OB_SUCC(ret) && !find && j < output_pullup_preds.count(); ++j) {
        if (right_pullup_preds.at(i)->same_as(*output_pullup_preds.at(j), &context)) {
          find = true;
        } else { /*do nothing*/
        }
      }
      if (find) {
        /*do nothing*/
      } else if (OB_FAIL(output_pullup_preds.push_back(right_pullup_preds.at(i)))) {
        LOG_WARN("push back preds failed", K(ret));
      } else { /*do nothing*/
      }
    }
    if (OB_FAIL(append(stmt->get_query_ctx()->all_equal_param_constraints_, context.equal_param_info_))) {
      LOG_WARN("append equal param info failed", K(ret));
    } else { /*do nothing*/
    }
  } else if (stmt->get_set_op() == ObSelectStmt::EXCEPT) {
    if (OB_FAIL(append(output_pullup_preds, left_pullup_preds))) {
      LOG_WARN("append pullup preds failed", K(ret));
    } else { /*do nothing*/
    }
  } else { /*do nothing*/
  }
  return ret;
}

int ObTransformPredicateMoveAround::choose_pullup_columns(
    TableItem& table, ObIArray<ObRawExpr*>& columns, ObIArray<int64_t>& view_sel_list)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < columns.count(); ++i) {
    if (OB_ISNULL(columns.at(i)) || OB_UNLIKELY(!columns.at(i)->is_column_ref_expr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid column exprs", K(ret));
    } else {
      ObColumnRefRawExpr* col = static_cast<ObColumnRefRawExpr*>(columns.at(i));
      if (col->get_table_id() != table.table_id_) {
        // do nothing
      } else if (ObOptimizerUtil::find_item(view_sel_list, col->get_column_id() - OB_APP_MIN_COLUMN_ID)) {
        // do nothing
      } else if (OB_FAIL(view_sel_list.push_back(col->get_column_id() - OB_APP_MIN_COLUMN_ID))) {
        LOG_WARN("failed to push back select index", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformPredicateMoveAround::generate_pullup_predicates(ObSelectStmt& select_stmt, ObIArray<int64_t>& sel_ids,
    ObIArray<ObRawExpr*>& input_pullup_preds, ObIArray<ObRawExpr*>& output_pullup_preds)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> local_preds;
  if (OB_FAIL(append(local_preds, input_pullup_preds))) {
    LOG_WARN("failed to append pullup predicates", K(ret));
  } else if (OB_FAIL(append(local_preds, select_stmt.get_condition_exprs()))) {
    LOG_WARN("failed to append conditions", K(ret));
  } else if (OB_FAIL(append(local_preds, select_stmt.get_having_exprs()))) {
    LOG_WARN("failed to append having conditions", K(ret));
  } else if (OB_FAIL(compute_pullup_predicates(select_stmt, sel_ids, local_preds, output_pullup_preds))) {
    LOG_WARN("failed to deduce exported predicates", K(ret));
  }
  return ret;
}

int ObTransformPredicateMoveAround::compute_pullup_predicates(ObSelectStmt& view, const ObIArray<int64_t>& select_list,
    ObIArray<ObRawExpr*>& local_preds, ObIArray<ObRawExpr*>& pullup_preds)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> select_exprs;
  ObSEArray<ObRawExpr*, 4> deduced_preds;
  pullup_preds.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < select_list.count(); ++i) {
    int64_t idx = select_list.at(i);
    ObRawExpr* sel_expr = NULL;
    if (OB_UNLIKELY(idx < 0 || idx >= view.get_select_item_size()) ||
        OB_ISNULL(sel_expr = view.get_select_item(idx).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("select expr is null", K(ret), K(sel_expr), K(idx));
    } else if (!sel_expr->is_column_ref_expr() && !sel_expr->is_win_func_expr() && !sel_expr->is_aggr_expr()) {
    } else if (OB_FAIL(select_exprs.push_back(sel_expr))) {
      LOG_WARN("failed to push back select expr", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(transform_predicates(view, local_preds, select_exprs, deduced_preds, true))) {
      LOG_WARN("failed to deduce predicates", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < deduced_preds.count(); ++i) {
    bool can_be = true;
    if (OB_ISNULL(deduced_preds.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("deduced predicate is null", K(ret));
    } else if ((!ObPredicateDeduce::is_simple_condition(deduced_preds.at(i)->get_expr_type()) &&
                   !ObPredicateDeduce::is_general_condition(deduced_preds.at(i)->get_expr_type())) ||
               deduced_preds.at(i)->has_flag(IS_CONST)) {
      // do nothing
    } else if (OB_FAIL(check_expr_pullup_validity(deduced_preds.at(i), select_exprs, can_be))) {
      LOG_WARN("failed to check pullup validity", K(ret));
    } else if (!can_be) {
      // do nothing
    } else if (OB_FAIL(pullup_preds.push_back(deduced_preds.at(i)))) {
      LOG_WARN("failed to push back predicates", K(ret));
    }
  }
  return ret;
}

int ObTransformPredicateMoveAround::check_expr_pullup_validity(
    const ObRawExpr* expr, const ObIArray<ObRawExpr*>& pullup_list, bool& can_be)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret), K(expr));
  } else if (expr->is_aggr_expr() || expr->is_win_func_expr() || expr->is_column_ref_expr() || expr->is_set_op_expr()) {
    can_be = ObOptimizerUtil::find_item(pullup_list, expr);
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && can_be && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(check_expr_pullup_validity(expr->get_param_expr(i), pullup_list, can_be))) {
        LOG_WARN("failed to check pullup validity", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformPredicateMoveAround::rename_pullup_predicates(
    ObDMLStmt& stmt, TableItem& view, const ObIArray<int64_t>& sel_ids, ObIArray<ObRawExpr*>& preds)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> view_select_list;
  ObSEArray<ObRawExpr*, 4> view_column_list;
  ObSelectStmt* view_stmt = NULL;
  ObRawExprFactory* expr_factory = NULL;
  int64_t stmt_level = stmt.get_current_level();
  if (OB_ISNULL(view_stmt = view.ref_query_) || OB_ISNULL(ctx_) || OB_ISNULL(expr_factory = ctx_->expr_factory_) ||
      OB_ISNULL(ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < sel_ids.count(); ++i) {
    ObRawExpr* sel_expr = NULL;
    ObColumnRefRawExpr* col_expr = NULL;
    int64_t idx = sel_ids.at(i);
    if (OB_UNLIKELY(idx < 0 || idx >= view_stmt->get_select_item_size())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("select index is invalid", K(ret), K(idx));
    } else if (OB_ISNULL(sel_expr = view_stmt->get_select_item(idx).expr_) ||
               OB_ISNULL(col_expr = stmt.get_column_expr_by_id(view.table_id_, idx + OB_APP_MIN_COLUMN_ID))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("select expr/ column expr is not found", K(ret), K(sel_expr), K(col_expr));
    } else if (OB_FAIL(view_select_list.push_back(sel_expr))) {
      LOG_WARN("failed to push back select expr", K(ret));
    } else if (OB_FAIL(view_column_list.push_back(col_expr))) {
      LOG_WARN("failed to push back column expr", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < preds.count(); ++i) {
    ObRawExpr* new_pred = NULL;
    if (OB_FAIL(ObRawExprUtils::copy_expr(*expr_factory, preds.at(i), new_pred, COPY_REF_DEFAULT))) {
      LOG_WARN("failed to copy expr", K(ret));
    } else if (OB_FAIL(ObTransformUtils::replace_expr(view_select_list, view_column_list, new_pred))) {
      LOG_WARN("failed to replace expr", K(ret));
    } else if (OB_FAIL(new_pred->formalize(ctx_->session_info_))) {
      LOG_WARN("failed to formalize expr", K(ret));
    } else if (OB_FAIL(new_pred->pull_relation_id_and_levels(stmt_level))) {
      LOG_WARN("failed to pull relation id and levels", K(ret));
    } else {
      preds.at(i) = new_pred;
    }
  }
  return ret;
}

int ObTransformPredicateMoveAround::pushdown_predicates(ObDMLStmt* stmt, ObIArray<ObRawExpr*>& pushdown_preds)
{
  int ret = OB_SUCCESS;
  bool has_rownum = false;
  bool has_group = false;
  bool has_winfunc = false;
  bool has_rollup = false;
  ObSEArray<ObRawExpr*, 4> candi_preds;
  ObSEArray<ObRawExpr*, 4> old_where_preds;
  ObIArray<ObRawExpr*>* pullup_preds = NULL;
  ObSelectStmt* sel_stmt = static_cast<ObSelectStmt*>(stmt);
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret), K(stmt));
  } else if (OB_FAIL(acquire_transform_params(stmt, pullup_preds))) {
    LOG_WARN("failed to acquire pull up preds", K(ret));
  } else if (OB_FAIL(old_where_preds.assign(stmt->get_condition_exprs()))) {
    LOG_WARN("failed to assign conditions", K(ret));
  } else if (stmt->get_stmt_hint().enable_no_pred_deduce()) {
    // do nothing
  } else if (OB_ISNULL(pullup_preds)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pullup predicate array is null", K(ret));
  } else if (stmt->is_set_stmt()) {
    ObSelectStmt* sel_stmt = static_cast<ObSelectStmt*>(stmt);
    if (OB_FAIL(pushdown_into_set_stmt(sel_stmt, *pullup_preds, pushdown_preds))) {
      LOG_WARN("recursive pushdown preds into set stmt failed", K(ret));
    } else { /*do nothing*/
    }
  } else if (stmt->is_hierarchical_query()) {
    ObArray<ObRawExpr*> dummy_preds;
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_table_size(); ++i) {
      TableItem* table = stmt->get_table_item(i);
      if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null table item", K(ret));
      } else if (!table->is_generated_table()) {
        // do nothing
      } else if (OB_FAIL(SMART_CALL(pushdown_predicates(table->ref_query_, dummy_preds)))) {
        LOG_WARN("failed to push down predicates", K(ret));
      } else { /*do nothing*/
      }
    }
  } else {
    if (OB_FAIL(stmt->has_rownum(has_rownum))) {
      LOG_WARN("failed to check stmt has rownum", K(ret));
    } else if (stmt->is_select_stmt()) {
      has_group = sel_stmt->has_group_by();
      has_winfunc = sel_stmt->has_window_function();
      has_rollup = sel_stmt->has_rollup();
    }
    if (OB_SUCC(ret)) {
      if (stmt->has_limit() || stmt->has_sequence() || has_rollup || stmt->is_contains_assignment()) {
        // no exprs can be pushed down
      } else if (has_rownum && !has_group) {
        // predicates can only be pushed into where
        // but with rownum, it is impossible
      } else if (has_winfunc) {
        if (OB_FAIL(pushdown_through_winfunc(*sel_stmt, pushdown_preds, candi_preds))) {
          LOG_WARN("failed to push down predicates throught winfunc", K(ret));
        }
      } else if (OB_FAIL(candi_preds.assign(pushdown_preds))) {
        LOG_WARN("failed to assign push down predicates", K(ret));
      } else {
        pushdown_preds.reset();
      }
    }

    if (OB_SUCC(ret) && has_group) {
      ObSEArray<ObRawExpr*, 4> old_having_exprs;
      if (OB_FAIL(old_having_exprs.assign(sel_stmt->get_having_exprs()))) {
        LOG_WARN("failed to assign having exprs", K(ret));
      } else if (OB_FAIL(pushdown_into_having(*sel_stmt, *pullup_preds, candi_preds))) {
        LOG_WARN("failed to push down predicates into having", K(ret));
      } else if (sel_stmt->get_group_exprs().empty() || has_rownum || has_rollup) {
        candi_preds.reset();
      } else if (OB_FAIL(pushdown_through_groupby(*sel_stmt, candi_preds))) {
        LOG_WARN("failed to pushdown predicate", K(ret));
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(check_transform_happened(old_having_exprs, sel_stmt->get_having_exprs()))) {
        LOG_WARN("failed to check transform happened", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(pushdown_into_where(*stmt, *pullup_preds, candi_preds))) {
        LOG_WARN("failed to push down predicates into where", K(ret));
      }
    }
  }

  if (OB_SUCC(ret) && !stmt->is_set_stmt() && !stmt->is_hierarchical_query()) {
    ObIArray<FromItem>& from_items = stmt->get_from_items();
    ObIArray<SemiInfo*>& semi_infos = stmt->get_semi_infos();
    for (int64_t i = 0; OB_SUCC(ret) && i < semi_infos.count(); ++i) {
      if (OB_FAIL(pushdown_into_semi_info(stmt, semi_infos.at(i), *pullup_preds, stmt->get_condition_exprs()))) {
        LOG_WARN("failed to push down into semi info", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < from_items.count(); ++i) {
      if (OB_FAIL(pushdown_into_table(
              stmt, stmt->get_table_item(from_items.at(i)), *pullup_preds, stmt->get_condition_exprs()))) {
        LOG_WARN("failed to push down predicates", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(check_transform_happened(old_where_preds, stmt->get_condition_exprs()))) {
      LOG_WARN("failed to check transform happened", K(ret));
    }
  }
  return ret;
}

int ObTransformPredicateMoveAround::pushdown_into_set_stmt(
    ObSelectStmt* stmt, ObIArray<ObRawExpr*>& pullup_preds, ObIArray<ObRawExpr*>& pushdown_preds)
{
  int ret = OB_SUCCESS;
  ObIArray<ObSelectStmt*>& child_query = stmt->get_set_query();
  const int64_t child_num = child_query.count();
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (stmt->get_stmt_hint().enable_no_pred_deduce()) {
    // does not pull up any predicates
  } else if (OB_UNLIKELY(!stmt->is_set_stmt()) || OB_UNLIKELY(child_num < 2)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is not a set stmt", K(ret));
  } else if (stmt->is_recursive_union() || stmt->has_limit()) {
    ObArray<ObRawExpr*> dummy_preds;
    for (int64_t i = 0; OB_SUCC(ret) && i < child_num; ++i) {
      ret = SMART_CALL(pushdown_predicates(child_query.at(i), dummy_preds));
    }
  } else {
    ObSEArray<ObRawExpr*, 16> last_input_preds;
    ObSEArray<ObRawExpr*, 16> input_preds;
    ObSEArray<ObRawExpr*, 16> cur_pushdown_preds;
    for (int64_t i = 0; OB_SUCC(ret) && i < child_num; ++i) {
      if (OB_FAIL(input_preds.assign(pushdown_preds))) {
        LOG_WARN("assign right input preds failed", K(ret));
      } else if (OB_FAIL(pushdown_into_set_stmt(child_query.at(i), pullup_preds, input_preds, stmt))) {
        LOG_WARN("failed to pushdown into set stmt", K(ret));
      } else if (0 == i) {
        ret = last_input_preds.assign(input_preds);
      } else if (OB_FAIL(check_pushdown_predicates(stmt, last_input_preds, input_preds, cur_pushdown_preds))) {
        LOG_WARN("choose pushdown predicates failed", K(ret));
      } else if (OB_FAIL(last_input_preds.assign(cur_pushdown_preds))) {
        LOG_WARN("failed to assign exprs", K(ret));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(pushdown_preds.assign(cur_pushdown_preds))) {
      LOG_WARN("failed to assign exprs", K(ret));
    }
  }
  return ret;
}

int ObTransformPredicateMoveAround::check_pushdown_predicates(ObSelectStmt* stmt,
    ObIArray<ObRawExpr*>& left_pushdown_preds, ObIArray<ObRawExpr*>& right_pushdown_preds,
    ObIArray<ObRawExpr*>& output_pushdown_preds)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (stmt->get_stmt_hint().enable_no_pred_deduce()) {
    // does not pull up any predicates
  } else if (!stmt->is_set_stmt()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is not a set stmt", K(ret));
  } else if (stmt->is_recursive_union()) {
    /*do nothing*/
  } else if (stmt->get_set_op() == ObSelectStmt::UNION) {
    // if a predicate is not pushed down, add it back to the upper stmt
    for (int64_t i = 0; OB_SUCC(ret) && i < right_pushdown_preds.count(); ++i) {
      if (ObOptimizerUtil::find_equal_expr(left_pushdown_preds, right_pushdown_preds.at(i))) {
        /*do nothing*/
      } else if (OB_FAIL(left_pushdown_preds.push_back(right_pushdown_preds.at(i)))) {
        LOG_WARN("push back preds failed", K(ret));
      } else { /*do nothing*/
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(output_pushdown_preds.assign(left_pushdown_preds))) {
      LOG_WARN("assign predicated failed", K(ret));
    } else { /*do nothing*/
    }
  } else if (stmt->get_set_op() == ObSelectStmt::INTERSECT) {
    output_pushdown_preds.reset();
    for (int64_t i = 0; OB_SUCC(ret) && i < right_pushdown_preds.count(); ++i) {
      if (!ObOptimizerUtil::find_equal_expr(left_pushdown_preds, right_pushdown_preds.at(i))) {
        /*do nothing*/
      } else if (OB_FAIL(output_pushdown_preds.push_back(right_pushdown_preds.at(i)))) {
        LOG_WARN("push back preds failed", K(ret));
      } else { /*do nothing*/
      }
    }
  } else if (stmt->get_set_op() == ObSelectStmt::EXCEPT) {
    if (OB_FAIL(output_pushdown_preds.assign(left_pushdown_preds))) {
      LOG_WARN("assign predicated failed", K(ret));
    } else { /*do nothing*/
    }
  } else { /*do nothing*/
  }
  return ret;
}
int ObTransformPredicateMoveAround::pushdown_into_set_stmt(ObSelectStmt* stmt, ObIArray<ObRawExpr*>& pullup_preds,
    ObIArray<ObRawExpr*>& pushdown_preds, ObSelectStmt* parent_stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt) || OB_ISNULL(parent_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param has null", K(stmt), K(parent_stmt), K(ret));
  } else {
    ObSEArray<ObRawExpr*, 16> valid_preds;
    ObSEArray<ObRawExpr*, 16> rename_preds;
    ObSEArray<ObRawExpr*, 16> candi_preds;
    if (OB_FAIL(valid_preds.assign(pushdown_preds))) {
      LOG_WARN("failed to assign pushdown preds");
    } else if (OB_FAIL(append(valid_preds, pullup_preds))) {
      LOG_WARN("failed to append pullup preds", K(ret));
    } else if (OB_FAIL(rename_preds.assign(valid_preds))) {
      LOG_WARN("failed to assign valid preds", K(ret));
    } else if (OB_FAIL(rename_set_op_predicates(*stmt, *parent_stmt, rename_preds, false))) {
      LOG_WARN("rename pushdown predicates failed", K(ret));
    } else if (OB_FAIL(candi_preds.assign(rename_preds))) {
      LOG_WARN("failed to assign filters", K(ret));
    } else if (OB_FAIL(SMART_CALL(pushdown_predicates(stmt, candi_preds)))) {
      LOG_WARN("failed to push down predicates", K(ret));
    } else {
      ObSEArray<ObRawExpr*, 16> output_preds;
      for (int64_t i = 0; OB_SUCC(ret) && i < rename_preds.count(); ++i) {
        if (!ObOptimizerUtil::find_equal_expr(candi_preds, rename_preds.at(i))) {
        } else if (!ObOptimizerUtil::find_equal_expr(pushdown_preds, valid_preds.at(i))) {
        } else if (OB_FAIL(output_preds.push_back(valid_preds.at(i)))) {
          LOG_WARN("push back predicate failed", K(ret));
        } else { /*do nothing*/
        }
      }
      if (OB_FAIL(pushdown_preds.assign(output_preds))) {
        LOG_WARN("assign preds failed", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObTransformPredicateMoveAround::rename_set_op_predicates(
    ObSelectStmt& child_stmt, ObSelectStmt& parent_stmt, ObIArray<ObRawExpr*>& preds, bool is_pullup)
{
  int ret = OB_SUCCESS;
  int64_t child_stmt_level = child_stmt.get_current_level();
  int64_t parent_stmt_level = parent_stmt.get_current_level();
  ObSEArray<ObRawExpr*, 4> child_select_list;
  ObSEArray<ObRawExpr*, 4> parent_select_list;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_) || OB_ISNULL(ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param has null", K(ctx_), K(ret));
  } else if (OB_FAIL(child_stmt.get_select_exprs(child_select_list))) {
    LOG_WARN("get child stmt select exprs failed", K(ret));
  } else if (OB_FAIL(parent_stmt.get_select_exprs(parent_select_list))) {
    LOG_WARN("get parent stmt select exprs failed", K(ret));
  } else if (child_select_list.count() != parent_select_list.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child stmt select exprs size is incorrect", K(child_select_list.count()), K(parent_select_list), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < parent_select_list.count(); ++i) {
      ObRawExpr* expr = NULL;
      if (OB_ISNULL(expr = ObTransformUtils::get_expr_in_cast(parent_select_list.at(i)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("select expr is null", K(i), K(ret));
      } else {
        parent_select_list.at(i) = expr;
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < preds.count(); ++i) {
      ObRawExpr* new_pred = NULL;
      ObRawExpr* pred = preds.at(i);
      if (OB_FAIL(ObRawExprUtils::copy_expr(*(ctx_->expr_factory_), pred, new_pred, COPY_REF_DEFAULT))) {
        LOG_WARN("failed to copy expr", K(ret));
      } else if (OB_ISNULL(new_pred)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("copy expr failed", K(ret));
      } else if (is_pullup &&
                 OB_FAIL(ObTransformUtils::replace_expr(child_select_list, parent_select_list, new_pred))) {
        LOG_WARN("failed to replace child stmt expr", K(ret));
      } else if (!is_pullup &&
                 OB_FAIL(ObTransformUtils::replace_expr(parent_select_list, child_select_list, new_pred))) {
        LOG_WARN("failed to replace parent stmt expr", K(ret));
      } else if (OB_FAIL(new_pred->formalize(ctx_->session_info_))) {
        LOG_WARN("failed to formalize expr", K(ret));
      } else if (is_pullup && OB_FAIL(new_pred->pull_relation_id_and_levels(parent_stmt_level))) {
        LOG_WARN("failed to pull relation id and levels", K(ret));
      } else if (!is_pullup && OB_FAIL(new_pred->pull_relation_id_and_levels(child_stmt_level))) {
        LOG_WARN("failed to pull relation id and levels", K(ret));
      } else {
        preds.at(i) = new_pred;
      }
    }
  }
  return ret;
}

int ObTransformPredicateMoveAround::pushdown_into_having(
    ObSelectStmt& sel_stmt, ObIArray<ObRawExpr*>& pullup_preds, ObIArray<ObRawExpr*>& pushdown_preds)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 4> new_having_exprs;
  ObSEArray<ObRawExpr *, 4> target_exprs;
  ObSEArray<ObRawExpr *, 4> input_preds;
  ObSEArray<ObRawExpr *, 4> all_columns;
  ObSEArray<ObRawExpr *, 4> columns;
  ObSqlBitSet<> table_set;
  if (sel_stmt.get_having_exprs().empty() && pushdown_preds.empty()) {
    // do nothing
  } else if (OB_FAIL(sel_stmt.get_column_exprs(all_columns))) {
    LOG_WARN("failed to get column exprs", K(ret));
  } else if (OB_FAIL(sel_stmt.get_from_tables(table_set))) {
    LOG_WARN("failed to get from items rel ids", K(ret));
  } else if (OB_FAIL(ObTransformUtils::extract_table_exprs(sel_stmt, all_columns,
                                                           table_set, columns))) {
    LOG_WARN("failed to get related columns", K(ret));
  } else if (OB_FAIL(ObTransformUtils::extract_table_exprs(sel_stmt, pullup_preds,
                                                           table_set, input_preds))) {
    LOG_WARN("failed to get related pullup preds", K(ret));
  } else if (OB_FAIL(append(input_preds, sel_stmt.get_having_exprs()))) {
    LOG_WARN("failed to append having predicates", K(ret));
  } else if (OB_FAIL(append(input_preds, pushdown_preds))) {
    LOG_WARN("failed to append push down predicates", K(ret));
  } else if (OB_FAIL(append(target_exprs, columns))) {
    LOG_WARN("failed to append column exprs", K(ret));
  } else if (OB_FAIL(append(target_exprs, sel_stmt.get_aggr_items()))) {
    LOG_WARN("failed to append aggregation items", K(ret));
  } else if (OB_FAIL(transform_predicates(sel_stmt, input_preds, target_exprs, new_having_exprs))) {
    LOG_WARN("failed to transform having predicates", K(ret));
  } else if (OB_FAIL(accept_predicates(sel_stmt, sel_stmt.get_having_exprs(), pullup_preds, new_having_exprs))) {
    LOG_WARN("failed to check different", K(ret));
  }
  return ret;
}

int ObTransformPredicateMoveAround::pushdown_into_where(
    ObDMLStmt& stmt, ObIArray<ObRawExpr*>& pullup_preds, ObIArray<ObRawExpr*>& predicates)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 4> new_conds;
  ObSEArray<ObRawExpr *, 4> all_conds;
  ObSEArray<ObRawExpr *, 4> all_columns;
  ObSEArray<ObRawExpr *, 4> columns;
  ObSqlBitSet<> table_set;
  if (stmt.get_condition_exprs().empty() && predicates.empty()) {
    // do nothing
  } else if (OB_FAIL(stmt.get_column_exprs(all_columns))) {
    LOG_WARN("failed to get column exprs", K(ret));
  } else if (OB_FAIL(stmt.get_from_tables(table_set))) {
    LOG_WARN("failed to get from items rel ids", K(ret));
  } else if (OB_FAIL(ObTransformUtils::extract_table_exprs(stmt, all_columns,
                                                           table_set, columns))) {
    LOG_WARN("failed to get related columns", K(ret));
  } else if (OB_FAIL(ObTransformUtils::extract_table_exprs(stmt, pullup_preds,
                                                           table_set, all_conds))) {
    LOG_WARN("failed to get related pullup preds", K(ret));
  } else if (OB_FAIL(append(all_conds, predicates))) {
    LOG_WARN("failed to append push down predicates", K(ret));
  } else if (OB_FAIL(append(all_conds, stmt.get_condition_exprs()))) {
    LOG_WARN("failed to append where conditions", K(ret));
  } else if (OB_FAIL(transform_predicates(stmt, all_conds, columns, new_conds))) {
    LOG_WARN("failed to transform non-anti conditions", K(ret));
  } else if (OB_FAIL(accept_predicates(stmt, stmt.get_condition_exprs(), pullup_preds, new_conds))) {
    LOG_WARN("failed to accept predicate", K(ret));
  }
  return ret;
}

int ObTransformPredicateMoveAround::pushdown_through_winfunc(
    ObSelectStmt& sel_stmt, ObIArray<ObRawExpr*>& predicates, ObIArray<ObRawExpr*>& down_exprs)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> common_part_exprs;
  ObSEArray<ObRawExpr*, 4> remain_exprs;
  for (int64_t i = 0; OB_SUCC(ret) && i < sel_stmt.get_window_func_count(); ++i) {
    ObWinFunRawExpr* win_expr = NULL;
    if (OB_ISNULL(win_expr = sel_stmt.get_window_func_expr(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("window function expr is null", K(ret));
    } else if (i == 0) {
      if (OB_FAIL(common_part_exprs.assign(win_expr->get_partition_exprs()))) {
        LOG_WARN("failed to assign partition exprs", K(ret));
      }
    } else if (OB_FAIL(ObOptimizerUtil::intersect_exprs(
                   common_part_exprs, win_expr->get_partition_exprs(), common_part_exprs))) {
      LOG_WARN("failed to intersect expr array", K(ret));
    } else if (common_part_exprs.empty()) {
      break;
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && !common_part_exprs.empty() && i < predicates.count(); ++i) {
    ObRawExpr* pred = NULL;
    ObSEArray<ObRawExpr*, 4> column_exprs;
    bool pushed = false;
    if (OB_ISNULL(pred = predicates.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("predicate is null", K(ret));
    } else if (pred->has_flag(CNT_WINDOW_FUNC)) {
      // do nothing
    } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(pred, column_exprs))) {
      LOG_WARN("failed to extract column exprs", K(ret));
    } else if (!ObOptimizerUtil::subset_exprs(column_exprs, common_part_exprs)) {
      // do nothing
    } else if (OB_FAIL(down_exprs.push_back(pred))) {
      LOG_WARN("failed to push back predicate", K(ret));
    } else {
      pushed = true;
    }
    if (OB_SUCC(ret) && !pushed) {
      if (OB_FAIL(remain_exprs.push_back(pred))) {
        LOG_WARN("failed to push back predicate", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && !down_exprs.empty()) {
    if (OB_FAIL(predicates.assign(remain_exprs))) {
      LOG_WARN("failed to assign remain exprs", K(ret));
    }
  }
  return ret;
}

int ObTransformPredicateMoveAround::pushdown_through_groupby(
    ObSelectStmt& stmt, ObIArray<ObRawExpr*>& output_predicates)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> new_having_exprs;
  output_predicates.reuse();
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_having_expr_size(); ++i) {
    ObRawExpr* pred = NULL;
    ObSEArray<ObRawExpr*, 4> generalized_columns;
    bool pushed = false;
    bool has_ref_assign_user_var = false;
    if (OB_ISNULL(pred = stmt.get_having_exprs().at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("predicate is null", K(ret));
    } else if (pred->has_flag(CNT_AGG)) {
      ObRawExpr* new_pred = NULL;
      if (stmt.get_aggr_item_size() == 1 && pred->get_expr_type() >= T_OP_LE && pred->get_expr_type() <= T_OP_GT) {
        if (OB_FAIL(deduce_param_cond_from_aggr_cond(
                pred->get_expr_type(), pred->get_param_expr(0), pred->get_param_expr(1), new_pred))) {
          LOG_WARN("failed to deduce param condition from aggr cond", K(ret));
        } else if (NULL == new_pred) {
          // do nothing
        } else if (OB_FAIL(output_predicates.push_back(new_pred))) {
          LOG_WARN("failed to push back predicate", K(ret));
        } else {
          pushed = true;
        }
      }
    } else if (OB_FAIL(extract_generalized_column(pred, stmt.get_current_level(), generalized_columns))) {
      LOG_WARN("failed to extract generalized columns", K(ret));
    } else if (!ObOptimizerUtil::subset_exprs(generalized_columns, stmt.get_group_exprs())) {
      // do nothing
    } else if (pred->has_flag(CNT_SUB_QUERY) &&
               OB_FAIL(ObOptimizerUtil::check_subquery_has_ref_assign_user_var(pred, has_ref_assign_user_var))) {
      LOG_WARN("failed to check subquery has ref assign user var", K(ret));
    } else if (has_ref_assign_user_var) {
      // do nothing
    } else if (OB_FAIL(output_predicates.push_back(pred))) {
      LOG_WARN("failed to push back predicate", K(ret));
    } else {
      pushed = true;
    }
    if (OB_SUCC(ret) && !pushed) {
      if (OB_FAIL(new_having_exprs.push_back(pred))) {
        LOG_WARN("failed to push back new having expr", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(stmt.get_having_exprs().assign(new_having_exprs))) {
      LOG_WARN("failed to assign new having exprs", K(ret));
    }
  }
  return ret;
}

/**
 * @brief ObTransformPredicateMoveAround::deduce_param_cond_from_aggr_cond
 * select * from t group by c1 having max(c1) < 10
 * =>
 * select * from t where c1 < 10 group by c1 having max(c1) < 10
 * @return
 */
int ObTransformPredicateMoveAround::deduce_param_cond_from_aggr_cond(
    ObItemType expr_type, ObRawExpr* first, ObRawExpr* second, ObRawExpr*& new_predicate)
{
  int ret = OB_SUCCESS;
  int64_t current_level = -1;
  if (OB_ISNULL(first) || OB_ISNULL(second) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_) ||
      OB_ISNULL(ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param exprs are null", K(ret), K(first), K(second), K(ctx_));
  } else if (expr_type == T_OP_GT) {
    ret = deduce_param_cond_from_aggr_cond(T_OP_LT, second, first, new_predicate);
  } else if (expr_type == T_OP_GE) {
    ret = deduce_param_cond_from_aggr_cond(T_OP_LE, second, first, new_predicate);
  } else if ((first->get_expr_type() == T_FUN_MIN && second->has_flag(IS_CONST))) {
    // min(c) < const_val => c < const_val
    if (OB_FAIL(ObRawExprUtils::create_double_op_expr(
            *ctx_->expr_factory_, ctx_->session_info_, expr_type, new_predicate, first->get_param_expr(0), second))) {
      LOG_WARN("fail create compare expr", K(ret));
    } else {
      current_level = first->get_expr_level();
    }
  } else if (first->has_flag(IS_CONST) && second->get_expr_type() == T_FUN_MAX) {
    // const_val < max(c) => const_val < c
    if (OB_FAIL(ObRawExprUtils::create_double_op_expr(
            *ctx_->expr_factory_, ctx_->session_info_, expr_type, new_predicate, first, second->get_param_expr(0)))) {
      LOG_WARN("fail create compare expr", K(ret));
    } else {
      current_level = second->get_expr_level();
    }
  }
  if (OB_SUCC(ret) && NULL != new_predicate && current_level != -1) {
    if (OB_FAIL(new_predicate->formalize(ctx_->session_info_))) {
      LOG_WARN("failed to formalize expr", K(ret));
    } else if (OB_FAIL(new_predicate->pull_relation_id_and_levels(current_level))) {
      LOG_WARN("failed to pull relation id and levels", K(ret));
    }
  }
  return ret;
}

/**
 * @brief ObTransformPredicateMoveAround::pushdown_into_joined_table
 * @param stmt
 * @param joined_table
 * @param preds
 *   the preds are executed upon the results of joined table
 *   consider how to execute these predicates before join
 * @return
 */
int ObTransformPredicateMoveAround::pushdown_into_joined_table(ObDMLStmt* stmt, JoinedTable* joined_table,
    ObIArray<ObRawExpr*>& pullup_preds, ObIArray<ObRawExpr*>& pushdown_preds)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> all_preds;
  ObSEArray<ObRawExpr*, 4> old_join_condition;
  /// STEP 1. deduce new join conditions
  bool is_stack_overflow = false;
  if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_stack_overflow));
  } else if (OB_ISNULL(stmt) || OB_ISNULL(joined_table) || OB_ISNULL(joined_table->left_table_) ||
      OB_ISNULL(joined_table->right_table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params are invalid", K(ret), K(stmt), K(joined_table));
  } else if (OB_FAIL(old_join_condition.assign(joined_table->join_conditions_))) {
    LOG_WARN("failed to assign join condition", K(ret));
  } else if (stmt->get_stmt_hint().enable_no_pred_deduce()) {
    // do nothing
  } else if (joined_table->is_left_join() || joined_table->is_right_join() || joined_table->is_inner_join()) {
    // given {preds, join conditions}, try to deduce new predicates
    // inner join, deduce predicates for both left, right table
    // left join,  deduce predicates for only right table
    // right join, deduce predicates for only left  table
    ObSEArray<ObRawExpr*, 4> cols;
    ObSEArray<ObRawExpr*, 4> all_cols;
    ObSEArray<ObRawExpr*, 4> new_preds;
    TableItem* filterable_table = NULL;
    ObSqlBitSet<> filter_table_set;

    if (joined_table->is_left_join()) {
      filterable_table = joined_table->right_table_;
    } else if (joined_table->is_right_join()) {
      filterable_table = joined_table->left_table_;
    } else if (joined_table->is_inner_join()) {
      filterable_table = joined_table;
    }
    if (NULL != filterable_table) {
      if (OB_FAIL(stmt->get_table_rel_ids(*filterable_table, filter_table_set))) {
        LOG_WARN("failed to get table relation ids", K(ret));
      }
    }

    ObSEArray<ObRawExpr*, 4> properites;
    if (OB_FAIL(append(properites, pullup_preds))) {
      LOG_WARN("failed to push back predicates", K(ret));
    } else if (OB_FAIL(append(properites, pushdown_preds))) {
      LOG_WARN("failed to append predicates", K(ret));
    } else if (OB_FAIL(append(all_preds, properites))) {
      LOG_WARN("failed to append predicates", K(ret));
    } else if (OB_FAIL(append(all_preds, joined_table->join_conditions_))) {
      LOG_WARN("failed to append join conditions", K(ret));
    } else if (OB_FAIL(stmt->get_column_exprs(all_cols))) {
      LOG_WARN("failed to get all column exprs", K(ret));
    } else if (OB_FAIL(ObTransformUtils::extract_table_exprs(*stmt, all_cols, *filterable_table, cols))) {
      LOG_WARN("failed to get related columns", K(ret));
    } else if (OB_FAIL(transform_predicates(*stmt, all_preds, cols, new_preds))) {
      LOG_WARN("failed to deduce predicates", K(ret));
    } else if (joined_table->is_inner_join()) {
      if (OB_FAIL(accept_predicates(*stmt, joined_table->join_conditions_, properites, new_preds))) {
        LOG_WARN("failed to accept predicate for joined table", K(ret));
      }
    } else {
      ObSEArray<ObRawExpr*, 8> chosen_preds;
      for (int64_t i = 0; OB_SUCC(ret) && i < joined_table->join_conditions_.count(); ++i) {
        if (ObOptimizerUtil::find_equal_expr(chosen_preds, joined_table->join_conditions_.at(i))) {
          // do nothing
        } else if (OB_FAIL(chosen_preds.push_back(joined_table->join_conditions_.at(i)))) {
          LOG_WARN("push back join condition failed", K(ret));
        } else { /*do nothing*/
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(joined_table->join_conditions_.assign(chosen_preds))) {
        LOG_WARN("assign join conditions failed", K(ret));
      } else { /*do nothing*/
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < new_preds.count(); ++i) {
        if (OB_ISNULL(new_preds.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("new predicate is null", K(ret));
        } else if (!new_preds.at(i)->get_expr_levels().has_member(stmt->get_current_level()) ||
                   !new_preds.at(i)->get_relation_ids().is_subset2(filter_table_set)) {
          // do nothing
        } else if (ObOptimizerUtil::find_equal_expr(all_preds, new_preds.at(i))) {
          // do nothing
        } else if (OB_FAIL(joined_table->join_conditions_.push_back(new_preds.at(i)))) {
          LOG_WARN("failed to push back new predicate", K(ret));
        }
      }
    }
  }
  /// STEP 2: push down predicates
  if (OB_SUCC(ret)) {
    ObSEArray<ObRawExpr*, 8> left_down;
    ObSEArray<ObRawExpr*, 8> right_down;
    // consider the left table of the joined table
    // full outer join, can not push anything down
    // left outer join, push preds down
    // right outer join, push join conditions down, [potentially, we can also push preds down]
    // inner join,push preds and join condition down
    if (joined_table->is_left_join()) {
      if (OB_FAIL(append(left_down, pushdown_preds))) {
        LOG_WARN("failed to append preds", K(ret));
      } else if (OB_FAIL(append(right_down, joined_table->join_conditions_))) {
        LOG_WARN("failed to append preds", K(ret));
      }
    } else if (joined_table->is_right_join()) {
      if (OB_FAIL(append(left_down, joined_table->join_conditions_))) {
        LOG_WARN("failed to append preds", K(ret));
      } else if (OB_FAIL(append(right_down, pushdown_preds))) {
        LOG_WARN("failed to append preds", K(ret));
      }
    } else if (joined_table->is_inner_join()) {
      if (OB_FAIL(append(left_down, pushdown_preds))) {
        LOG_WARN("failed to append preds", K(ret));
      } else if (OB_FAIL(append(left_down, joined_table->join_conditions_))) {
        LOG_WARN("failed to append preds", K(ret));
      } else if (OB_FAIL(append(right_down, left_down))) {
        LOG_WARN("failed to append preds", K(ret));
      }
    } else {
      // can pushdown nothing
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(pushdown_into_table(stmt, joined_table->left_table_, pullup_preds, left_down))) {
      LOG_WARN("failed to push down predicates", K(ret));
    } else if (OB_FAIL(pushdown_into_table(stmt, joined_table->right_table_, pullup_preds, right_down))) {
      LOG_WARN("failed to push down predicates", K(ret));
    } else {
      if (joined_table->is_left_join()) {
        if (OB_FAIL(pushdown_preds.assign(left_down))) {
          LOG_WARN("failed to assign preds", K(ret));
        } else if (OB_FAIL(joined_table->join_conditions_.assign(right_down))) {
          LOG_WARN("failed to assign preds", K(ret));
        }
      } else if (joined_table->is_right_join()) {
        if (OB_FAIL(joined_table->join_conditions_.assign(left_down))) {
          LOG_WARN("failed to assign preds", K(ret));
        } else if (OB_FAIL(pushdown_preds.assign(right_down))) {
          LOG_WARN("failed to assign preds", K(ret));
        }
      } else if (joined_table->is_inner_join()) {
        ObSEArray<ObRawExpr*, 8> new_pushdown_preds;
        ObSEArray<ObRawExpr*, 8> new_join_conditions;
        for (int64_t i = 0; OB_SUCC(ret) && i < pushdown_preds.count(); ++i) {
          if (!ObOptimizerUtil::find_equal_expr(left_down, pushdown_preds.at(i)) ||
              !ObOptimizerUtil::find_equal_expr(right_down, pushdown_preds.at(i))) {
          } else if (OB_FAIL(new_pushdown_preds.push_back(pushdown_preds.at(i)))) {
            LOG_WARN("failed to push back pred", K(ret));
          }
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < joined_table->join_conditions_.count(); ++i) {
          if (!ObOptimizerUtil::find_equal_expr(left_down, joined_table->join_conditions_.at(i)) ||
              !ObOptimizerUtil::find_equal_expr(right_down, joined_table->join_conditions_.at(i))) {
          } else if (OB_FAIL(new_join_conditions.push_back(joined_table->join_conditions_.at(i)))) {
            LOG_WARN("failed to push back pred", K(ret));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(pushdown_preds.assign(new_pushdown_preds))) {
          LOG_WARN("failed to assign preds", K(ret));
        } else if (OB_FAIL(joined_table->join_conditions_.assign(new_join_conditions))) {
          LOG_WARN("failed to assign preds", K(ret));
        }
      } else {
        // do nothing for full join
      }
    }
    if (OB_FAIL(ret)) {
    } else if (check_transform_happened(old_join_condition, joined_table->join_conditions_)) {
      LOG_WARN("failed to check transform happened", K(ret));
    }
  }
  return ret;
}

int ObTransformPredicateMoveAround::check_transform_happened(
    ObIArray<ObRawExpr*>& old_conditions, ObIArray<ObRawExpr*>& new_conditions)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && !trans_happened_ && i < new_conditions.count(); ++i) {
    if (!ObOptimizerUtil::find_equal_expr(old_conditions, new_conditions.at(i))) {
      trans_happened_ = true;
    }
  }
  return ret;
}

int ObTransformPredicateMoveAround::get_pushdown_predicates(
    ObDMLStmt& stmt, TableItem& table, ObIArray<ObRawExpr*>& preds, ObIArray<ObRawExpr*>& table_filters)
{
  int ret = OB_SUCCESS;
  ObSqlBitSet<> table_set;
  TableItem* target_table = NULL;
  if (table.is_joined_table()) {
    JoinedTable& joined_table = static_cast<JoinedTable&>(table);
    if (joined_table.is_left_join()) {
      target_table = joined_table.left_table_;
    } else if (joined_table.is_right_join()) {
      target_table = joined_table.right_table_;
    } else if (joined_table.is_inner_join()) {
      target_table = &joined_table;
    }
  } else if (table.is_generated_table()) {
    target_table = &table;
  }
  if (OB_FAIL(ret) || NULL == target_table) {
  } else if (OB_FAIL(stmt.get_table_rel_ids(*target_table, table_set))) {
    LOG_WARN("failed to get table set", K(ret));
  } else if (OB_FAIL(get_pushdown_predicates(stmt, table_set, preds, table_filters))) {
    LOG_WARN("failed to get push down predicates", K(ret));
  }
  return ret;
}

int ObTransformPredicateMoveAround::get_pushdown_predicates(
    ObDMLStmt& stmt, ObSqlBitSet<>& table_set, ObIArray<ObRawExpr*>& preds, ObIArray<ObRawExpr*>& table_filters)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < preds.count(); ++i) {
    if (OB_ISNULL(preds.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("predicate is null", K(ret));
    } else if (ObPredicateDeduce::contain_special_expr(*preds.at(i))) {
      // do nothing
    } else if (!preds.at(i)->get_expr_levels().has_member(stmt.get_current_level()) ||
               !table_set.is_superset2(preds.at(i)->get_relation_ids())) {
      // do nothing
    } else if (OB_FAIL(table_filters.push_back(preds.at(i)))) {
      LOG_WARN("failed to push back predicate", K(ret));
    }
  }
  return ret;
}

/**
 * @brief ObTransformPredicateMoveAround::pushdown_into_semi_info
 * @param stmt
 * @param table_item
 * @param pullup_preds
 * @param preds
 * pushdown_preds is only used to deduce new preds
 * @return
 */
int ObTransformPredicateMoveAround::pushdown_into_semi_info(
    ObDMLStmt* stmt, SemiInfo* semi_info, ObIArray<ObRawExpr*>& pullup_preds, ObIArray<ObRawExpr*>& pushdown_preds)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> all_preds;
  ObSEArray<ObRawExpr*, 4> old_semi_conds;
  ObSqlBitSet<> left_rel_ids;
  ObSqlBitSet<> right_rel_ids;
  TableItem* right_table = NULL;
  ObSEArray<ObRawExpr*, 4> properites;
  ObSEArray<ObRawExpr*, 4> cols;
  ObSEArray<ObRawExpr*, 4> all_cols;
  ObSEArray<ObRawExpr*, 4> new_preds;
  ObSEArray<ObRawExpr*, 4> table_preds;
  if (OB_ISNULL(stmt) || OB_ISNULL(semi_info) ||
      OB_ISNULL(right_table = stmt->get_table_item_by_id(semi_info->right_table_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(stmt), K(semi_info), K(right_table));
  } else if (OB_FAIL(old_semi_conds.assign(semi_info->semi_conditions_))) {
    LOG_WARN("failed to assign exprs", K(ret));
  } else if (OB_FAIL(stmt->get_table_rel_ids(semi_info->left_table_ids_, left_rel_ids))) {
    LOG_WARN("failed to get left rel ids", K(ret));
  } else if (OB_FAIL(stmt->get_table_rel_ids(semi_info->right_table_id_, right_rel_ids))) {
    LOG_WARN("failed to get right semi rel ids", K(ret));
  } else if (OB_FAIL(ObTransformUtils::extract_table_exprs(*stmt, pullup_preds, left_rel_ids, properites)) ||
             OB_FAIL(get_pushdown_predicates(*stmt, left_rel_ids, pushdown_preds, properites))) {
    LOG_WARN("failed to extract left table filters", K(ret));
  } else if (OB_FAIL(ObTransformUtils::extract_table_exprs(*stmt, pullup_preds, right_rel_ids, properites))) {
    LOG_WARN("failed to extract right table filters", K(ret));
  } else if (OB_FAIL(all_preds.assign(properites))) {
    LOG_WARN("failed to assign predicates", K(ret));
  } else if (OB_FAIL(append(all_preds, semi_info->semi_conditions_))) {
    LOG_WARN("failed to append join conditions", K(ret));
  } else if (OB_FAIL(stmt->get_column_exprs(all_cols))) {
    LOG_WARN("failed to get all column exprs", K(ret));
  } else if (OB_FAIL(ObTransformUtils::extract_table_exprs(*stmt, all_cols, right_rel_ids, cols))) {
    LOG_WARN("failed to get related columns", K(ret));
  } else if (OB_FAIL(transform_predicates(*stmt, all_preds, cols, new_preds))) {
    LOG_WARN("failed to deduce predicates", K(ret));
  } else if (OB_FAIL(accept_predicates(*stmt, semi_info->semi_conditions_, properites, new_preds))) {
    LOG_WARN("failed to check different", K(ret));
  } else if (OB_FAIL(pushdown_into_table(stmt, right_table, pullup_preds, semi_info->semi_conditions_))) {
    LOG_WARN("failed to push down predicates", K(ret));
  } else if (OB_FAIL(get_pushdown_predicates(*stmt, right_rel_ids, semi_info->semi_conditions_,
                                             table_preds))) {
    LOG_WARN("failed to get push down predicates", K(ret));
  } else if (table_preds.empty()) {
    /* do nothing */
  } else if (OB_FAIL(ObTransformUtils::pushdown_semi_info_right_filter(stmt, ctx_, semi_info, table_preds))) {
    LOG_WARN("failed to pushdown semi info right filter", K(ret));
  }

  if (OB_SUCC(ret) && OB_FAIL(check_transform_happened(old_semi_conds,
                                                       semi_info->semi_conditions_))) {
    LOG_WARN("failed to check transform happened", K(ret));
  }
  return ret;
}

/**
 * @brief ObTransformPredicateMoveAround::pushdown_into_table
 * @param stmt
 * @param table_item
 * @param pullup_preds
 * @param preds
 * if a predicate in preds is pushed down into the table item,
 * it is removed from the preds
 * @return
 */
int ObTransformPredicateMoveAround::pushdown_into_table(
    ObDMLStmt* stmt, TableItem* table_item, ObIArray<ObRawExpr*>& pullup_preds, ObIArray<ObRawExpr*>& preds)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 8> rename_preds;
  ObSEArray<ObRawExpr*, 8> table_preds;
  ObSEArray<ObRawExpr*, 8> candi_preds;
  ObSEArray<ObRawExpr*, 8> table_pullup_preds;
  bool is_stack_overflow = false;
  if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_stack_overflow));
  } else if (OB_ISNULL(stmt) || OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(stmt), K(table_item));
  } else if (OB_FAIL(get_pushdown_predicates(*stmt, *table_item, preds, table_preds))) {
    LOG_WARN("failed to get push down predicates", K(ret));
  } else if (OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null after create_view_with_table", K(ret), K(table_item));
  } else if (!table_item->is_joined_table() && !table_item->is_generated_table()) {
    // do nothing
  } else if (OB_FAIL(rename_preds.assign(table_preds))) {
    LOG_WARN("failed to assgin exprs", K(ret));
  } else if (OB_FAIL(ObTransformUtils::extract_table_exprs(*stmt, pullup_preds, *table_item, table_pullup_preds))) {
    LOG_WARN("failed to extract table predicates", K(ret));
  }
  if (OB_SUCC(ret) && table_item->is_generated_table()) {
    // if predicates are pushed into the view, we can remove them from the upper stmt
    ObSEArray<ObRawExpr*, 8> invalid_preds;
    if (OB_FAIL(rename_pushdown_predicates(*stmt, *table_item, rename_preds))) {
      LOG_WARN("failed to rename predicates", K(ret));
    } else if (OB_FAIL(choose_pushdown_preds(rename_preds, invalid_preds, candi_preds))) {
      LOG_WARN("failed to choose predicates for pushdown", K(ret));
    } else if (OB_FAIL(SMART_CALL(pushdown_predicates(table_item->ref_query_, candi_preds)))) {
      LOG_WARN("failed to push down predicates", K(ret));
    } else if (OB_FAIL(append(candi_preds, invalid_preds))) {
      LOG_WARN("failed to append predicates", K(ret));
    }
  }
  if (OB_SUCC(ret) && table_item->is_joined_table()) {
    if (OB_FAIL(candi_preds.assign(rename_preds))) {
      LOG_WARN("failed to assgin exprs", K(ret));
    } else if (OB_FAIL(pushdown_into_joined_table(
                   stmt, static_cast<JoinedTable*>(table_item), table_pullup_preds, candi_preds))) {
      LOG_WARN("failed to push down predicates", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    // remove a pred from preds if it is pushed into a joined table or a generated table
    for (int64_t i = 0; OB_SUCC(ret) && i < rename_preds.count(); ++i) {
      // check whether a table filter is pushed into a view
      if (ObOptimizerUtil::find_equal_expr(candi_preds, rename_preds.at(i))) {
        // the filter is not pushed down
      } else if (OB_FAIL(ObOptimizerUtil::remove_item(preds, table_preds.at(i)))) {
        LOG_WARN("failed to remove pushed filter from preds", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformPredicateMoveAround::choose_pushdown_preds(
    ObIArray<ObRawExpr*>& preds, ObIArray<ObRawExpr*>& invalid_preds, ObIArray<ObRawExpr*>& valid_preds)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < preds.count(); ++i) {
    ObRawExpr* expr = NULL;
    if (OB_ISNULL(expr = preds.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("predicate is null", K(ret), K(expr));
    } else if (expr->has_flag(CNT_SUB_QUERY) || expr->has_flag(CNT_USER_VARIABLE)) {
      ret = invalid_preds.push_back(expr);
    } else {
      ret = valid_preds.push_back(expr);
    }
  }
  return ret;
}

int ObTransformPredicateMoveAround::rename_pushdown_predicates(
    ObDMLStmt& stmt, TableItem& view, ObIArray<ObRawExpr*>& preds)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> view_select_list;
  ObSEArray<ObRawExpr*, 4> view_column_list;
  ObSEArray<ObColumnRefRawExpr*, 4> table_columns;
  ObSelectStmt* view_stmt = NULL;
  ObRawExprFactory* expr_factory = NULL;
  int64_t stmt_level = -1;
  if (OB_ISNULL(view_stmt = view.ref_query_) || OB_ISNULL(ctx_) || OB_ISNULL(expr_factory = ctx_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("view stmt is null", K(ret));
  } else if (OB_FAIL(stmt.get_column_exprs(view.table_id_, table_columns))) {
    LOG_WARN("failed to get column exprs", K(ret));
  } else if (OB_FAIL(append(view_column_list, table_columns))) {
    LOG_WARN("failed to append column exprs", K(ret));
  } else {
    stmt_level = view_stmt->get_current_level();
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < table_columns.count(); ++i) {
    ObRawExpr* sel_expr = NULL;
    ObColumnRefRawExpr* col_expr = table_columns.at(i);
    int64_t idx = -1;
    if (OB_ISNULL(col_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column expr is null", K(ret), K(col_expr));
    } else if (FALSE_IT(idx = col_expr->get_column_id() - OB_APP_MIN_COLUMN_ID)) {
      // do nothing
    } else if (OB_UNLIKELY(idx < 0 || idx >= view_stmt->get_select_item_size())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("select index is invalid", K(ret), K(idx));
    } else if (OB_ISNULL(sel_expr = view_stmt->get_select_item(idx).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("select expr expr is not found", K(ret), K(sel_expr));
    } else if (OB_FAIL(view_select_list.push_back(sel_expr))) {
      LOG_WARN("failed to push back select expr", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < preds.count(); ++i) {
    ObRawExpr* new_pred = NULL;
    if (OB_FAIL(ObRawExprUtils::copy_expr(*expr_factory, preds.at(i), new_pred, COPY_REF_DEFAULT))) {
      LOG_WARN("failed to copy expr", K(ret));
    } else if (OB_FAIL(ObTransformUtils::replace_expr(view_column_list, view_select_list, new_pred))) {
      LOG_WARN("failed to replace expr", K(ret));
    } else if (OB_FAIL(new_pred->formalize(ctx_->session_info_))) {
      LOG_WARN("failed to formalize expr", K(ret));
    } else if (OB_FAIL(new_pred->pull_relation_id_and_levels(stmt_level))) {
      LOG_WARN("failed to pull relation id and levels", K(ret));
    } else {
      preds.at(i) = new_pred;
    }
  }
  return ret;
}

int ObTransformPredicateMoveAround::transform_predicates(ObDMLStmt& stmt, common::ObIArray<ObRawExpr*>& input_preds,
    common::ObIArray<ObRawExpr*>& target_exprs, common::ObIArray<ObRawExpr*>& output_preds, bool is_pullup /*= false*/)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> valid_preds;
  ObSEArray<ObRawExpr*, 4> other_preds;
  ObSEArray<ObRawExpr*, 4> simple_preds;
  ObSEArray<ObRawExpr*, 4> general_preds;
  ObSEArray<ObRawExpr*, 4> aggr_bound_preds;
  ObSqlBitSet<> visited;
  if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("transform context is null", K(ret), K(ctx_));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < input_preds.count(); ++i) {
    bool is_valid = false;
    if (OB_FAIL(ObPredicateDeduce::check_deduce_validity(input_preds.at(i), is_valid))) {
      LOG_WARN("failed to check condition validity", K(ret));
    } else if (is_valid) {
      if (OB_FAIL(valid_preds.push_back(input_preds.at(i)))) {
        LOG_WARN("failed to push back predicates for deduce", K(ret));
      }
    } else if (OB_FAIL(other_preds.push_back(input_preds.at(i)))) {
      LOG_WARN("failed to push back complex predicates", K(ret));
    }
  }
  while (OB_SUCC(ret) && visited.num_members() < valid_preds.count()) {
    // build a graph for comparing expr with the same comparing type
    ObPredicateDeduce deducer(stmt);
    for (int64_t i = 0; OB_SUCC(ret) && i < valid_preds.count(); ++i) {
      bool is_added = false;
      if (visited.has_member(i)) {
        // do nothing
      } else if (OB_FAIL(deducer.add_predicate(valid_preds.at(i), is_added))) {
        LOG_WARN("failed to add predicate into deducer", K(ret));
      } else if (is_added && OB_FAIL(visited.add_member(i))) {
        LOG_WARN("failed to mark predicate is deduced", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(deducer.deduce_simple_predicates(*ctx_, simple_preds))) {
        LOG_WARN("failed to deduce predicates for target", K(ret));
      } else if (OB_FAIL(deducer.deduce_general_predicates(*ctx_, target_exprs, other_preds, general_preds))) {
        LOG_WARN("failed to deduce special predicates", K(ret));
      } else if (!is_pullup) {
        // do nothing
      } else if (OB_FAIL(deducer.deduce_aggr_bound_predicates(*ctx_, target_exprs, aggr_bound_preds))) {
        LOG_WARN("faield to deduce semantic predicates", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(output_preds.assign(simple_preds))) {
      LOG_WARN("failed to assign result", K(ret));
    } else if (OB_FAIL(append(output_preds, other_preds))) {
      LOG_WARN("failed to append other predicates", K(ret));
    } else if (OB_FAIL(append(output_preds, general_preds))) {
      LOG_WARN("failed to append speical predicates", K(ret));
    } else if (OB_FAIL(append(output_preds, aggr_bound_preds))) {
      LOG_WARN("failed to deduce aggr bound predicates", K(ret));
    }
  }
  return ret;
}

int ObTransformPredicateMoveAround::accept_predicates(
    ObDMLStmt& stmt, ObIArray<ObRawExpr*>& conds, ObIArray<ObRawExpr*>& properties, ObIArray<ObRawExpr*>& new_conds)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> chosen_preds;
  ObExprParamCheckContext context;
  if (OB_FAIL(context.init(stmt.get_query_ctx()))) {
    LOG_WARN("init param check context failed", K(ret));
  } else { /*do nothing*/
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < new_conds.count(); ++i) {
    if (OB_ISNULL(new_conds.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("new condition is null", K(ret));
    } else if (ObOptimizerUtil::find_equal_expr(properties, new_conds.at(i), context)) {
      // the condition has been ensured
    } else if (ObOptimizerUtil::find_equal_expr(chosen_preds, new_conds.at(i), context)) {
      // the condition has been chosen
    } else if (OB_FAIL(chosen_preds.push_back(new_conds.at(i)))) {
      LOG_WARN("failed to push back new condition", K(ret));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(conds.assign(chosen_preds))) {
    LOG_WARN("failed to assign new conditions", K(ret));
  }
  return ret;
}

int ObTransformPredicateMoveAround::extract_generalized_column(
    ObRawExpr* expr, const int32_t expr_level, ObIArray<ObRawExpr*>& output)
{
  int ret = OB_SUCCESS;
  ObArray<ObRawExpr*> queue;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (!expr->get_expr_levels().has_member(expr_level)) {
    // do nothing
  } else if (OB_FAIL(queue.push_back(expr))) {
    LOG_WARN("failed to push back expr", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < queue.count(); ++i) {
    ObRawExpr* cur = queue.at(i);
    if (cur->is_query_ref_expr()) {
      if (OB_FAIL(
              extract_generalized_column(static_cast<ObQueryRefRawExpr*>(cur)->get_ref_stmt(), expr_level, output))) {
        LOG_WARN("failed to extract generalized column", K(ret));
      }
    } else if (!cur->is_set_op_expr()) {
      for (int64_t j = 0; OB_SUCC(ret) && j < cur->get_param_count(); ++j) {
        ObRawExpr* param = NULL;
        if (OB_ISNULL(param = cur->get_param_expr(j))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("param expr is null", K(ret), K(param));
        } else if (!param->get_expr_levels().has_member(expr_level)) {
          // do nothing
        } else if (OB_FAIL(queue.push_back(param))) {
          LOG_WARN("failed to push back param expr", K(ret));
        }
      }
      if (OB_SUCC(ret) && (cur->is_column_ref_expr() || cur->is_aggr_expr() || cur->is_win_func_expr())) {
        if (OB_FAIL(output.push_back(cur))) {
          LOG_WARN("failed to push back current expr", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTransformPredicateMoveAround::extract_generalized_column(
    ObSelectStmt* stmt, const int32_t expr_level, ObIArray<ObRawExpr*>& output)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 8> relation_exprs;
  ObSEArray<ObSelectStmt*, 8> select_stmts;
  bool is_overflow = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret), K(stmt));
  } else if (OB_FAIL(check_stack_overflow(is_overflow))) {
    LOG_WARN("failed to check stack over flow", K(ret));
  } else if (is_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_overflow));
  } else if (OB_FAIL(stmt->get_relation_exprs(relation_exprs))) {
    LOG_WARN("failed to get relation exprs", K(ret));
  } else if (OB_FAIL(stmt->get_from_subquery_stmts(select_stmts))) {
    LOG_WARN("failed to get from view stmts", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < relation_exprs.count(); ++i) {
    if (OB_FAIL(extract_generalized_column(relation_exprs.at(i), expr_level, output))) {
      LOG_WARN("failed to extract generalized column", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < select_stmts.count(); ++i) {
    if (OB_FAIL(extract_generalized_column(select_stmts.at(i), expr_level, output))) {
      LOG_WARN("failed to extract generalized columns", K(ret));
    }
  }
  return ret;
}

int ObTransformPredicateMoveAround::acquire_transform_params(ObDMLStmt* stmt, ObIArray<ObRawExpr*>*& preds)
{
  int ret = OB_SUCCESS;
  int64_t index = -1;
  preds = NULL;
  const uint64_t key = reinterpret_cast<const uint64_t>(stmt);
  if (OB_SUCCESS != (ret = stmt_map_.get_refactored(key, index))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    }
  } else if (OB_UNLIKELY(index >= stmt_pullup_preds_.count() || index < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("does not find pullup predicates", K(ret), K(index));
  } else {
    preds = stmt_pullup_preds_.at(index);
  }
  if (OB_SUCC(ret) && NULL == preds) {
    PullupPreds* new_preds = NULL;
    index = stmt_pullup_preds_.count();
    if (OB_ISNULL(new_preds = (PullupPreds*)allocator_.alloc(sizeof(PullupPreds)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to allocate pullup predicates array", K(ret));
    } else {
      new_preds = new (new_preds) PullupPreds();
      if (OB_FAIL(stmt_pullup_preds_.push_back(new_preds))) {
        LOG_WARN("failed to push back predicates", K(ret));
      } else if (OB_FAIL(stmt_map_.set_refactored(key, index))) {
        LOG_WARN("failed to add entry info hash map", K(ret));
      } else {
        preds = new_preds;
      }
    }
  }
  return ret;
}

int ObTransformPredicateMoveAround::get_columns_in_filters(
    ObDMLStmt& stmt, ObIArray<int64_t>& sel_items, ObIArray<ObRawExpr*>& columns)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> filter_exprs;
  ObSEArray<ObRawExpr*, 4> tmp_exprs;
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_joined_tables().count(); ++i) {
    if (OB_FAIL(ObTransformUtils::get_on_condition(stmt.get_joined_tables().at(i), filter_exprs))) {
      LOG_WARN("failed to get on conditions", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObTransformUtils::get_semi_conditions(stmt.get_semi_infos(), filter_exprs))) {
    LOG_WARN("failed to get semi conditions", K(ret));
  } else if (OB_FAIL(append(filter_exprs, stmt.get_condition_exprs()))) {
    LOG_WARN("failed to append condition exprs", K(ret));
  } else if (stmt.is_select_stmt()) {
    ObSelectStmt& sel_stmt = static_cast<ObSelectStmt&>(stmt);
    if (OB_FAIL(append(filter_exprs, sel_stmt.get_having_exprs()))) {
      LOG_WARN("failed to append having exprs", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < sel_items.count(); ++i) {
      int64_t idx = sel_items.at(i);
      if (OB_UNLIKELY(idx < 0 || idx >= sel_stmt.get_select_item_size())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid select index", K(ret), K(idx));
      } else if (OB_FAIL(filter_exprs.push_back(sel_stmt.get_select_item(idx).expr_))) {
        LOG_WARN("failed to push back select expr", K(ret));
      }
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < filter_exprs.count(); ++i) {
    tmp_exprs.reuse();
    if (OB_FAIL(ObRawExprUtils::extract_column_exprs(filter_exprs.at(i), tmp_exprs))) {
      LOG_WARN("failed to extract column exprs", K(ret));
    } else if (OB_FAIL(append_array_no_dup(columns, tmp_exprs))) {
      LOG_WARN("failed to append array without duplicate", K(ret));
    }
  }
  return ret;
}

/**
 * @brief ObTransformPredicateMoveAround::deduce_part_prune_filters
 * deduce partition pruning filters for the insert stmt
 * @param preds
 * @return
 */
int ObTransformPredicateMoveAround::create_equal_exprs_for_insert(ObInsertStmt* insert_stmt)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> part_exprs;
  ObSEArray<ObRawExpr*, 4> target_exprs;
  ObSEArray<ObRawExpr*, 4> source_exprs;
  if (OB_ISNULL(insert_stmt) || OB_ISNULL(insert_stmt->get_table_columns())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("insert stmt is null", K(ret), K(insert_stmt));
  } else if (!insert_stmt->value_from_select()) {
    // do nothing
  } else if (OB_FAIL(insert_stmt->get_value_exprs(source_exprs))) {
    LOG_WARN("failed to get source exprs", K(ret));
  } else if (OB_FAIL(append(target_exprs, *insert_stmt->get_table_columns()))) {
    LOG_WARN("failed to get target exprs", K(ret));
  } else if (OB_UNLIKELY(target_exprs.count() != source_exprs.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("array size does not match", K(ret), K(target_exprs.count()), K(source_exprs.count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < insert_stmt->get_part_exprs().count(); ++i) {
    ObRawExpr* expr = NULL;
    if (NULL != (expr = insert_stmt->get_part_exprs().at(i).part_expr_)) {
      if (OB_FAIL(part_exprs.push_back(expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      }
    }
    if (OB_SUCC(ret) && NULL != (expr = insert_stmt->get_part_exprs().at(i).subpart_expr_)) {
      if (OB_FAIL(part_exprs.push_back(expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      }
    }
  }
  // 1. check a target expr is a partition expr
  // 2. check the comparison meta between target and source expr
  // 3. mock equal expr between target and source
  for (int64_t i = 0; OB_SUCC(ret) && i < target_exprs.count(); ++i) {
    ObRawExpr* ret_expr = NULL;
    bool type_safe = false;
    if (OB_ISNULL(target_exprs.at(i)) || OB_ISNULL(source_exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("param exprs are null", K(ret));
    } else if (!ObOptimizerUtil::find_item(part_exprs, target_exprs.at(i))) {
      // do nothing
    } else if (OB_FAIL(ObRelationalExprOperator::is_equivalent(target_exprs.at(i)->get_result_type(),
                   target_exprs.at(i)->get_result_type(),
                   source_exprs.at(i)->get_result_type(),
                   type_safe))) {
      LOG_WARN("failed to check is type safe", K(ret));
    } else if (!type_safe) {
      // do nothing
    } else if (OB_FAIL(ObRawExprUtils::create_equal_expr(
                   *ctx_->expr_factory_, ctx_->session_info_, target_exprs.at(i), source_exprs.at(i), ret_expr))) {
      LOG_WARN("failed to create equal exprs", K(ret));
    } else if (OB_ISNULL(ret_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("equal expr is null", K(ret));
    } else if (OB_FAIL(ret_expr->pull_relation_id_and_levels(insert_stmt->get_current_level()))) {
      LOG_WARN("failed to pull relation id and levels", K(ret));
    } else if (OB_FAIL(insert_stmt->add_condition_expr(ret_expr))) {
      LOG_WARN("failed to add condition expr", K(ret));
    }
  }
  return ret;
}
