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

#include "ob_transform_full_outer_join.h"
#include "common/ob_common_utility.h"
#include "lib/oblog/ob_log_module.h"
#include "share/ob_errno.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "share/schema/ob_table_schema.h"

namespace oceanbase {
namespace sql {

int ObTransformFullOuterJoin::transform_one_stmt(
    common::ObIArray<ObParentDMLStmt>& parent_stmts, ObDMLStmt*& stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  UNUSED(parent_stmts);
  trans_happened = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt is null", K(ret));
  } else if (OB_FAIL(transform_full_outer_join(stmt, trans_happened))) {
    LOG_WARN("failed to transform full nl join.", K(ret));
  } else if (trans_happened) {
    LOG_TRACE("succeed to do nl outer join transformer", K(trans_happened));
  }
  return ret;
}

bool ObTransformFullOuterJoin::need_rewrite(
    const common::ObIArray<ObParentDMLStmt>& parent_stmts, const ObDMLStmt& stmt)
{
  UNUSED(parent_stmts);
  UNUSED(stmt);
  return true;
}

int ObTransformFullOuterJoin::transform_full_outer_join(ObDMLStmt*& stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->stmt_factory_) || OB_ISNULL(ctx_->allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(stmt), K(ctx_));
  } else {
    ObSEArray<JoinedTable*, 4> new_joined_tables;
    TableItem* table = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_from_item_size(); ++i) {
      FromItem& cur_from_item = stmt->get_from_item(i);
      if (cur_from_item.is_joined_) {
        if (OB_ISNULL(table = stmt->get_joined_table(cur_from_item.table_id_))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("params have null", K(ret));
        } else if (OB_FAIL(recursively_eliminate_full_join(stmt, table, trans_happened))) {
          LOG_WARN("failed to transform to left union right.", K(ret));
        } else if (table->is_joined_table()) {
          ret = new_joined_tables.push_back(static_cast<JoinedTable*>(table));
        } else {
          cur_from_item.is_joined_ = false;
          cur_from_item.table_id_ = table->table_id_;
        }
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(stmt->get_joined_tables().assign(new_joined_tables))) {
      LOG_WARN("faield to assign joined tables.", K(ret));
    }
  }
  return ret;
}

int ObTransformFullOuterJoin::recursively_eliminate_full_join(
    ObDMLStmt* stmt, TableItem*& table_item, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt) || OB_ISNULL(table_item) || OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null.", K(ret));
  } else if (table_item->type_ == TableItem::JOINED_TABLE) {
    bool has_equal = false;
    JoinedTable* joined_table = static_cast<JoinedTable*>(table_item);
    if (OB_ISNULL(joined_table->left_table_) || OB_ISNULL(joined_table->right_table_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null.", K(ret));
    } else if (OB_FAIL(recursively_eliminate_full_join(stmt, joined_table->left_table_, trans_happened))) {
      LOG_WARN("failed to transform full nl join.", K(ret));
    } else if (OB_FAIL(recursively_eliminate_full_join(stmt, joined_table->right_table_, trans_happened))) {
      LOG_WARN("failed to transform full nl join.", K(ret));
    } else if (OB_FAIL(ObTransformUtils::adjust_single_table_ids(joined_table))) {
      LOG_WARN("failed to adjust single table ids.", K(ret));
    } else if (!joined_table->is_full_join()) {
      /* do nothing */
    } else if (OB_FAIL(check_join_condition(stmt, joined_table, has_equal))) {
      LOG_WARN("failed to check join condition", K(ret));
    } else if (has_equal) {
      /* do nothing */
    } else if (OB_FAIL(create_view_for_full_nl_join(stmt, joined_table, table_item))) {
      LOG_WARN("failed to create view for full nl join.", K(ret));
    } else if (OB_FAIL(update_stmt_exprs_for_view(stmt, joined_table, table_item))) {
      LOG_WARN("failed to update stmt exprs for view.", K(ret));
    } else {
      trans_happened = true;
    }
  } else { /* do nothing. */
  }
  return ret;
}

int ObTransformFullOuterJoin::check_join_condition(ObDMLStmt* stmt, JoinedTable* table, bool& has_equal)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 8> left_tables;
  ObSEArray<uint64_t, 8> right_tables;
  ObSEArray<uint64_t, 8> table_ids;
  TableItem* left_table = NULL;
  TableItem* right_table = NULL;
  has_equal = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(table) || OB_ISNULL(left_table = table->left_table_) ||
      OB_ISNULL(right_table = table->right_table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null param", K(ret));
  } else if (left_table->is_joined_table() &&
             OB_FAIL(left_tables.assign(static_cast<JoinedTable*>(left_table)->single_table_ids_))) {
    LOG_WARN("failed to get table ids", K(ret));
  } else if (!left_table->is_joined_table() && OB_FAIL(left_tables.push_back(left_table->table_id_))) {
    LOG_WARN("failed to push back table id", K(ret));
  } else if (right_table->is_joined_table() &&
             OB_FAIL(right_tables.assign(static_cast<JoinedTable*>(right_table)->single_table_ids_))) {
    LOG_WARN("failed to get table ids", K(ret));
  } else if (!right_table->is_joined_table() && OB_FAIL(right_tables.push_back(right_table->table_id_))) {
    LOG_WARN("failed to push back table id", K(ret));
  }
  for (int64_t i = 0; i < table->join_conditions_.count(); i++) {
    ObRawExpr* cond = table->join_conditions_.at(i);
    table_ids.reuse();
    if (OB_ISNULL(cond)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null condition", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::extract_table_ids(cond, table_ids))) {
      LOG_WARN("failed to extract table ids", K(ret));
    } else if (table_ids.empty()) {
      // do nothing
    } else if (cond->has_flag(IS_JOIN_COND) && ObOptimizerUtil::overlap(table_ids, left_tables) &&
               ObOptimizerUtil::overlap(table_ids, right_tables)) {
      has_equal = true;
    }
  }
  return ret;
}

int ObTransformFullOuterJoin::create_view_for_full_nl_join(
    ObDMLStmt* stmt, JoinedTable* joined_table, TableItem*& view_table)
{
  int ret = OB_SUCCESS;
  ObSelectStmt* left_stmt = NULL;
  ObSelectStmt* right_stmt = NULL;
  ObSelectStmt* union_stmt = NULL;
  if (OB_ISNULL(stmt) || OB_ISNULL(joined_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null error.", K(ret));
  } else if (OB_FAIL(create_left_outer_join_stmt(
                 stmt, left_stmt, joined_table, stmt->query_ctx_, stmt->get_table_hash()))) {
    LOG_WARN("failed to extract left outer join.", K(ret));
  } else if (OB_FAIL(create_left_anti_join_stmt(left_stmt, stmt->query_ctx_, right_stmt))) {
    LOG_WARN("failed to create left anti join stmt.", K(ret));
  } else if (OB_FAIL(ObTransformUtils::create_union_stmt(ctx_, false, left_stmt, right_stmt, union_stmt))) {
    LOG_WARN("failed to create union stmt.", K(ret));
  } else if (OB_ISNULL(union_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(union_stmt));
  } else if (OB_FAIL(ObTransformUtils::add_new_table_item(ctx_, stmt, union_stmt, view_table))) {
    LOG_WARN("failed to add new table item.", K(ret));
  } else { /* do nothing. */
  }
  return ret;
}

int ObTransformFullOuterJoin::update_stmt_exprs_for_view(
    ObDMLStmt* stmt, JoinedTable* joined_table, TableItem* view_table)
{
  int ret = OB_SUCCESS;
  ObRelIds joined_table_ids;
  ObSEArray<ObRawExpr*, 4> old_exprs;
  ObSEArray<ObRawExpr*, 4> new_exprs;
  ObSEArray<ColumnItem, 4> temp_column_items;
  ObSEArray<TableItem*, 4> table_items;
  if (OB_ISNULL(stmt) || OB_ISNULL(joined_table) || OB_ISNULL(view_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null error.", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::remove_item(stmt->get_table_items(), view_table))) {
    LOG_WARN("failed to remove items.", K(ret));
  } else if (OB_FAIL(get_column_exprs_from_joined_table(stmt, joined_table, old_exprs))) {
    LOG_WARN("failed to get select exprs from stmt.", K(ret));
  } else if (OB_FAIL(ObTransformUtils::create_columns_for_view(ctx_, *view_table, stmt, new_exprs))) {
    LOG_WARN("failed to create columns for view.", K(ret));
  } else if (OB_FAIL(ObTransformUtils::get_rel_ids_from_join_table(stmt, joined_table, joined_table_ids))) {
    LOG_WARN("failed to extract idx from joined table.", K(ret));
  } else { /* do nothing. */
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_column_size(); i++) {
    if (!stmt->get_column_item(i)->expr_->get_relation_ids().overlap(joined_table_ids)) {
      if (OB_FAIL(temp_column_items.push_back(*stmt->get_column_item(i)))) {
        LOG_WARN("faield to push back into column items.", K(ret));
      } else { /* do nothing. */
      }
    } else { /* do nothing. */
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(stmt->get_column_items().assign(temp_column_items))) {
    LOG_WARN("failed to assign column items.", K(ret));
  } else if (OB_FAIL(stmt->replace_inner_stmt_expr(old_exprs, new_exprs))) {
    LOG_WARN("failed to replace inner stmt exprs.", K(ret));
  } else if (OB_FAIL(stmt->get_table_items().push_back(view_table))) {
    LOG_WARN("failed to push back into table items.", K(ret));
  } else if (OB_FAIL(ObTransformUtils::extract_table_items(joined_table, table_items))) {
    LOG_WARN("failed to extract table items.", K(ret));
  } else if (OB_FAIL(remove_tables_from_stmt(stmt, table_items))) {
    LOG_WARN("failed to remove tables from stmt.", K(ret));
  } else if (OB_FAIL(ObTransformUtils::replace_table_in_semi_infos(stmt, view_table, joined_table))) {
    LOG_WARN("failed to replace table in semi infos", K(ret));
  } else if (OB_FAIL(stmt->rebuild_tables_hash())) {
    LOG_WARN("failed to rebuild tables hash.", K(ret));
  } else if (OB_FAIL(stmt->update_column_item_rel_id())) {
    LOG_WARN("failed to update column items rel id.", K(ret));
  } else { /* do nothing. */
  }
  return ret;
}

int ObTransformFullOuterJoin::get_column_exprs_from_joined_table(
    ObDMLStmt* stmt, JoinedTable* joined_table, ObIArray<ObRawExpr*>& col_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt) || OB_ISNULL(joined_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null error.", K(ret));
  } else if (joined_table->type_ != TableItem::JOINED_TABLE) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected table item type.", K(ret));
  } else {
    int64_t N = joined_table->single_table_ids_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < N; i++) {
      uint64_t table_id = joined_table->single_table_ids_.at(i);
      for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_column_size(); ++i) {
        if (stmt->get_column_item(i)->table_id_ == table_id) {
          if (OB_FAIL(col_exprs.push_back(stmt->get_column_item(i)->expr_))) {
            LOG_WARN("failed to push back column exprs", K(ret));
          } else { /* do nothing. */
          }
        } else { /* do nothing. */
        }
      }
    }
  }
  return ret;
}

int ObTransformFullOuterJoin::create_select_items_for_left_join(
    ObDMLStmt* stmt, JoinedTable* joined_table, ObIArray<SelectItem>& select_items, ObIArray<ColumnItem>& column_items)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt) || OB_ISNULL(joined_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null error.", K(ret));
  } else if (joined_table->type_ != TableItem::JOINED_TABLE) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected table item type.", K(ret));
  } else {
    int64_t N = joined_table->single_table_ids_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < N; i++) {
      uint64_t table_id = joined_table->single_table_ids_.at(i);
      if (OB_FAIL(stmt->get_column_items(table_id, column_items))) {
        LOG_WARN("failed to get column exprs in stmt.", K(ret));
      } else {
      }
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < column_items.count(); j++) {
      SelectItem select_item;
      select_item.alias_name_ = column_items.at(j).expr_->get_column_name();
      select_item.expr_name_ = column_items.at(j).expr_->get_column_name();
      select_item.expr_ = const_cast<ObColumnRefRawExpr*>(column_items.at(j).expr_);
      if (OB_FAIL(select_items.push_back(select_item))) {
        LOG_WARN("failed to push back select_item", K(ret), K(select_item));
      } else {
      }
    }
  }
  return ret;
}

int ObTransformFullOuterJoin::create_left_outer_join_stmt(ObDMLStmt* stmt, ObSelectStmt*& subquery,
    JoinedTable* joined_table, ObQueryCtx* query_ctx, const ObRowDesc& table_hash)
{
  int ret = OB_SUCCESS;
  ObSelectStmt sub_stmt;
  FromItem cur_from_item;
  ObSEArray<SelectItem, 4> select_items;
  ObSEArray<ColumnItem, 4> column_items;
  ObSEArray<SelectItem, 8> output_select_items;
  ObSEArray<TableItem*, 4> table_items;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_) || OB_ISNULL(joined_table) || OB_ISNULL(query_ctx) ||
      OB_ISNULL(ctx_->stmt_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null error.", K(ret), K(ctx_));
  } else if (OB_FAIL(create_select_items_for_left_join(stmt, joined_table, select_items, column_items))) {
    LOG_WARN("failed to generate column items.", K(ret));
  } else if (FALSE_IT(sub_stmt.set_current_level(stmt->get_current_level()))) {
  } else if (FALSE_IT(sub_stmt.set_query_ctx(query_ctx))) {
  } else if (FALSE_IT(joined_table->joined_type_ = LEFT_OUTER_JOIN)) {
  } else if (OB_FAIL(sub_stmt.add_joined_table(joined_table))) {
    LOG_WARN("failed to add joined table.", K(ret));
  } else if (FALSE_IT(cur_from_item.is_joined_ = true)) {
  } else if (FALSE_IT(cur_from_item.table_id_ = joined_table->table_id_)) {
  } else if (OB_FAIL(sub_stmt.get_from_items().push_back(cur_from_item))) {
    LOG_WARN("failed to push back to sub stmt.", K(ret));
  } else if (OB_FAIL(sub_stmt.get_column_items().assign(column_items))) {
    LOG_WARN("failed to assign to column itmes.", K(ret));
  } else if (OB_FAIL(ObTransformUtils::extract_table_items(joined_table, table_items))) {
    LOG_WARN("faield to extract table items.", K(ret));
  } else if (OB_FAIL(sub_stmt.get_table_items().assign(table_items))) {
    LOG_WARN("failed to push back to table items.", K(ret));
  } else if (OB_FAIL(sub_stmt.get_table_hash().assign(table_hash))) {
    LOG_WARN("failed to rebuild tables hash.", K(ret));
  } else if (OB_FAIL(sub_stmt.get_select_items().assign(select_items))) {
    LOG_WARN("failed to assign to column itmes.", K(ret));
  } else if (sub_stmt.get_select_items().empty() &&
             OB_FAIL(ObTransformUtils::create_dummy_select_item(sub_stmt, ctx_))) {
    LOG_WARN("failed to create dummy select item", K(ret));
  } else if (OB_FAIL(sub_stmt.get_part_exprs().assign(stmt->get_part_exprs()))) {
    LOG_WARN("failed to assign to part expr items.", K(ret));
  } else if (OB_FAIL(sub_stmt.get_stmt_hint().assign(stmt->get_stmt_hint()))) {
    LOG_WARN("failed to assign to stmt hints.", K(ret));
  } else if (OB_FAIL(sub_stmt.adjust_view_parent_namespace_stmt(stmt->get_parent_namespace_stmt()))) {
    LOG_WARN("failed to adjust view parent namespace stmt", K(ret));
  } else if (OB_FAIL(ctx_->stmt_factory_->create_stmt<ObSelectStmt>(subquery))) {
    LOG_WARN("failed to create stmt.", K(ret));
  } else if (FALSE_IT(subquery->set_query_ctx(query_ctx))) {
  } else if (OB_FAIL(subquery->deep_copy(*ctx_->stmt_factory_, *ctx_->expr_factory_, sub_stmt))) {
    LOG_WARN("failed to deep copy stmt.", K(ret));
  } else if (OB_FAIL(subquery->update_stmt_table_id(sub_stmt))) {
    LOG_WARN("failed to update stmt table id.", K(ret));
  } else { /* do nothing. */
  }
  return ret;
}

int ObTransformFullOuterJoin::create_left_anti_join_stmt(
    ObSelectStmt* left_stmt, ObQueryCtx* query_ctx, ObSelectStmt*& right_stmt)
{
  int ret = OB_SUCCESS;
  ObSelectStmt* sub_stmt = NULL;
  if (OB_ISNULL(left_stmt) || OB_ISNULL(query_ctx) || OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null error.", K(ret), K(ctx_));
  } else if (OB_FAIL(ctx_->stmt_factory_->create_stmt<ObSelectStmt>(sub_stmt))) {
    LOG_WARN("failed to create stmt.", K(ret));
  } else if (FALSE_IT(sub_stmt->set_query_ctx(query_ctx))) {
  } else if (OB_FAIL(sub_stmt->deep_copy(*ctx_->stmt_factory_, *ctx_->expr_factory_, *left_stmt))) {
    LOG_WARN("failed to deep copy the left stmt.", K(ret));
  } else if (OB_FAIL(sub_stmt->update_stmt_table_id(*left_stmt))) {
    LOG_WARN("failed to updatew table id in stmt.", K(ret));
  } else if (sub_stmt->get_joined_tables().count() != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected joined table count.", K(ret), K(sub_stmt->get_joined_tables()));
  } else if (OB_FAIL(switch_left_outer_to_semi_join(
                 sub_stmt, sub_stmt->get_joined_tables().at(0), sub_stmt->get_select_items()))) {
    LOG_WARN("failed to switch join table to semi.", K(ret));
  } else {
    right_stmt = sub_stmt;
  }
  return ret;
}

int ObTransformFullOuterJoin::switch_left_outer_to_semi_join(
    ObSelectStmt*& sub_stmt, JoinedTable* joined_table, const ObIArray<SelectItem>& select_items)
{
  int ret = OB_SUCCESS;
  ObRelIds index_left;
  ObRelIds index_right;
  SemiInfo* semi_info = NULL;
  ObSEArray<SelectItem, 4> output_select_items;
  TableItem* view_item = NULL;
  ObSEArray<FromItem, 4> from_items;
  ObSEArray<SemiInfo*, 4> semi_infos;
  ObSEArray<JoinedTable*, 4> joined_tables;
  if (OB_ISNULL(joined_table) || OB_ISNULL(sub_stmt) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null pointer.", K(ret));
  } else if (OB_ISNULL(joined_table->left_table_) || OB_ISNULL(joined_table->right_table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null pointer.", K(ret));
  } else if (joined_table->left_table_->is_joined_table() &&
             OB_FAIL(ObTransformUtils::create_view_with_table(sub_stmt, ctx_, joined_table->left_table_, view_item))) {
    LOG_WARN("failed to create view with table", K(ret));
  } else if (OB_ISNULL(semi_info = static_cast<SemiInfo*>(ctx_->allocator_->alloc(sizeof(SemiInfo))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to alloc semi info", K(ret));
  } else if (FALSE_IT(semi_info = new (semi_info) SemiInfo())) {
  } else if (OB_FAIL(semi_infos.push_back(semi_info))) {
    LOG_WARN("failed to push back semi info", K(ret));
  } else if (joined_table->right_table_->type_ == TableItem::JOINED_TABLE &&
             OB_FAIL(joined_tables.push_back(static_cast<JoinedTable*>(joined_table->right_table_)))) {
    LOG_WARN("failed push back joined table.", K(ret));
  } else if (OB_FAIL(create_select_items_for_semi_join(
                 sub_stmt, joined_table->left_table_, select_items, output_select_items))) {
    LOG_WARN("failed to assign to column itmes.", K(ret));
  } else if (OB_FAIL(sub_stmt->get_select_items().assign(output_select_items))) {
    LOG_WARN("failed to assign select items.", K(ret));
  } else if (OB_FAIL(semi_info->semi_conditions_.assign(joined_table->join_conditions_))) {
    LOG_WARN("failed to assign to condition exprs.", K(ret));
  } else {
    FromItem from_item;
    from_item.is_joined_ = joined_table->right_table_->type_ == TableItem::JOINED_TABLE;
    from_item.table_id_ = joined_table->right_table_->table_id_;
    semi_info->join_type_ = LEFT_ANTI_JOIN;
    semi_info->right_table_id_ = joined_table->left_table_->table_id_;
    if (OB_FAIL(from_items.push_back(from_item))) {
      LOG_WARN("failed to push back from item", K(ret));
    } else if (from_item.is_joined_) {
      JoinedTable* table = static_cast<JoinedTable*>(joined_table->right_table_);
      ret = semi_info->left_table_ids_.assign(table->single_table_ids_);
    } else {
      ret = semi_info->left_table_ids_.push_back(from_item.table_id_);
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(sub_stmt->get_joined_tables().assign(joined_tables))) {
    LOG_WARN("failed to assign join table.", K(ret));
  } else if (OB_FAIL(sub_stmt->get_semi_infos().assign(semi_infos))) {
    LOG_WARN("failed to assign semi infos.", K(ret));
  } else if (OB_FAIL(sub_stmt->get_from_items().assign(from_items))) {
    LOG_WARN("failed to assign from items.", K(ret));
  } else if (OB_FAIL(sub_stmt->formalize_stmt(ctx_->session_info_))) {
    LOG_WARN("failed to formalize stmt", K(ret));
  } else if (OB_FAIL(ObTransformUtils::pushdown_semi_info_right_filter(sub_stmt, ctx_, semi_info))) {
    LOG_WARN("failed to pushdown semi info right filter", K(ret));
  }
  return ret;
}

int ObTransformFullOuterJoin::create_select_items_for_semi_join(ObDMLStmt* stmt, TableItem* table_item,
    const ObIArray<SelectItem>& select_items, ObIArray<SelectItem>& output_select_items)
{
  int ret = OB_SUCCESS;
  ObRelIds index_left;
  if (OB_ISNULL(stmt) || OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null error.", K(ret));
  } else if (OB_FAIL(extract_idx_from_table_items(stmt, table_item, index_left))) {
    LOG_WARN("failed to extract idx from join table.", K(ret));
  } else { /*do nothing.*/
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < select_items.count(); i++) {
    SelectItem select_item;
    if (OB_FAIL(select_item.deep_copy(*ctx_->expr_factory_, select_items.at(i), COPY_REF_DEFAULT))) {
      LOG_WARN("failed to deep copy select items.", K(ret));
    } else if (select_items.at(i).expr_->get_relation_ids().overlap(index_left)) {
      if (OB_FAIL(ObRawExprUtils::build_null_expr(*ctx_->expr_factory_, select_item.expr_))) {
        LOG_WARN("failed build null exprs.", K(ret));
      } else { /*do nothing.*/
      }
    } else { /*do nothing.*/
    }
    if (OB_FAIL(output_select_items.push_back(select_item))) {
      LOG_WARN("failed to push back to select items.", K(ret));
    } else { /*do nothing.*/
    }
  }
  return ret;
}

int ObTransformFullOuterJoin::extract_idx_from_table_items(
    ObDMLStmt* sub_stmt, const TableItem* table_item, ObRelIds& rel_ids)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_item) || OB_ISNULL(sub_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null error.", K(ret));
  } else if (table_item->is_joined_table()) {
    const JoinedTable* joined_table = static_cast<const JoinedTable*>(table_item);
    if (OB_FAIL(extract_idx_from_table_items(sub_stmt, joined_table->left_table_, rel_ids))) {
      LOG_WARN("failed to extract idx from join table.", K(ret));
    } else if (OB_FAIL(extract_idx_from_table_items(sub_stmt, joined_table->right_table_, rel_ids))) {
      LOG_WARN("failed to extract idx from join table.", K(ret));
    } else {
    }
  } else if (table_item->is_basic_table() || table_item->is_generated_table()) {
    if (OB_FAIL(rel_ids.add_member(sub_stmt->get_table_bit_index(table_item->table_id_)))) {
      LOG_WARN("failed to add member to rel ids.", K(ret));
    } else {
    }
  }
  return ret;
}

int ObTransformFullOuterJoin::remove_tables_from_stmt(ObDMLStmt* stmt, const ObIArray<TableItem*>& table_items)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null error.", K(ret));
  } else { /*do nothing.*/
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < table_items.count(); i++) {
    if (OB_FAIL(ObOptimizerUtil::remove_item(stmt->get_table_items(), table_items.at(i)))) {
      LOG_WARN("failed to remove item from table items.", K(ret));
    } else { /* do nothing. */
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
