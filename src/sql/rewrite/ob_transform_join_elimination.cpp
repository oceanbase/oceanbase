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

#include "ob_transform_join_elimination.h"
#include "common/ob_common_utility.h"
#include "common/ob_smart_call.h"
#include "lib/oblog/ob_log_module.h"
#include "share/ob_errno.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/rewrite/ob_stmt_comparer.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "share/schema/ob_table_schema.h"
#include "sql/rewrite/ob_equal_analysis.h"

namespace oceanbase {
using namespace common;
using namespace share::schema;
namespace sql {

int ObTransformJoinElimination::transform_one_stmt(
    common::ObIArray<ObParentDMLStmt>& parent_stmts, ObDMLStmt*& stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  UNUSED(parent_stmts);
  trans_happened = false;
  bool trans_happened_self_foreign_key = false;
  bool trans_happened_outer_join = false;
  bool trans_happened_self_foreign_semi_join = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt is null", K(ret));
  } else if (OB_FAIL(eliminate_join_self_foreign_key(stmt, trans_happened_self_foreign_key))) {
    LOG_WARN("failed to eliminate self join/join between primary key and foreign key", K(ret));
  } else if (OB_FAIL(eliminate_outer_join(stmt, trans_happened_outer_join))) {
    LOG_WARN("failed to eliminate outer join if need", K(ret));
  } else if (OB_FAIL(eliminate_semi_join_self_foreign_key(stmt, trans_happened_self_foreign_semi_join))) {
    LOG_WARN("failed to eliminated self semi join", K(ret));
  } else if (!trans_happened_self_foreign_key && !trans_happened_outer_join && !trans_happened_self_foreign_semi_join) {
    /* do nothing*/
  } else if (OB_FAIL(trans_self_equal_conds(stmt))) {
    LOG_WARN("failed to trans condition exprs", K(ret));
  } else {
    trans_happened = true;
    LOG_TRACE("succeed to do join elimination",
        K(trans_happened_self_foreign_key),
        K(trans_happened_outer_join),
        K(trans_happened_self_foreign_semi_join));
  }
  return ret;
}

int ObTransformJoinElimination::check_table_can_be_eliminated(const ObDMLStmt *stmt, uint64_t table_id, bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null stmt", K(ret));
  } else if (stmt->is_delete_stmt() || stmt->is_update_stmt()) {
    const ObDelUpdStmt *del_up_stmt = static_cast<const ObDelUpdStmt *>(stmt);
    for (uint64_t i = 0; OB_SUCC(ret) && i < del_up_stmt->get_all_table_columns().count(); ++i) {
      const TableColumns &tab_cols = del_up_stmt->get_all_table_columns().at(i);
      const IndexDMLInfo &index_info = tab_cols.index_dml_infos_.at(0);
      if (index_info.table_id_ == table_id) {
        is_valid = false;
      }
    }
  } else {
    // TODO: add more cases if necessary
  }
  return ret;
}

int ObTransformJoinElimination::eliminate_join_self_foreign_key(ObDMLStmt *stmt, bool &trans_happened)
{
  int ret = OB_SUCCESS;
  bool from_happedend = false;
  bool joined_happedend = false;
  trans_happened = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(stmt), K(ctx_), K(ret));
  } else if (OB_FAIL(eliminate_join_in_from_base_table(stmt, from_happedend))) {
    LOG_WARN("eliminate join in from base table failed", K(ret));
  } else if (OB_FAIL(eliminate_join_in_joined_table(stmt, joined_happedend))) {
    LOG_WARN("eliminate join in joined table failed", K(ret));
  } else {
    trans_happened = from_happedend | joined_happedend;
  }
  return ret;
}

int ObTransformJoinElimination::eliminate_join_in_from_base_table(ObDMLStmt* stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  ObSEArray<TableItem*, 16> child_candi_tables;
  ObSEArray<TableItem*, 16> candi_tables;
  ObSEArray<ObRawExpr*, 16> conds;
  trans_happened = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(stmt), K(ctx_), K(ret));
  } else if (stmt->get_equal_set_conditions(conds, true, SCOPE_WHERE)) {
    LOG_WARN("failed to get equal set conditions", K(ret));
  } else if (OB_FAIL(extract_candi_table(stmt, stmt->get_from_items(), candi_tables, child_candi_tables))) {
    LOG_WARN("failed to extract candi tables", K(ret));
  } else if (OB_FAIL(eliminate_candi_tables(stmt, conds, candi_tables, child_candi_tables, trans_happened))) {
    LOG_WARN("failed to eliminate candi tables", K(ret));
  }
  return ret;
}

int ObTransformJoinElimination::do_join_elimination_self_key(ObDMLStmt* stmt, TableItem* source_table,
    TableItem* target_table, bool& trans_happened,
    EqualSets* equal_sets)  // default value NULL
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  ObStmtMapInfo stmt_map_info;
  bool can_be_eliminated = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_) || OB_ISNULL(source_table) || OB_ISNULL(target_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(stmt), K(ctx_), K(ret));
  } else if (OB_FAIL(ObTransformUtils::check_loseless_join(stmt,
                 ctx_,
                 source_table,
                 target_table,
                 ctx_->session_info_,
                 ctx_->schema_checker_,
                 stmt_map_info,
                 can_be_eliminated,
                 equal_sets))) {
    LOG_WARN("failed to check whether transformation is possible", K(ret));
  } else if (!can_be_eliminated) {
    /*do nothing*/
  } else if (OB_FAIL(adjust_table_items(stmt, source_table, target_table, stmt_map_info))) {
    LOG_WARN("failed to adjust table items", K(ret));
  } else if (OB_FAIL(trans_table_item(stmt, source_table, target_table))) {
    LOG_WARN("failed to transform target into source", K(ret));
  } else if (OB_FAIL(append(stmt->get_query_ctx()->all_equal_param_constraints_, stmt_map_info.equal_param_map_))) {
    LOG_WARN("failed to append equal params constraints", K(ret));
  } else {
    trans_happened = true;
    LOG_TRACE("do join elimination for", K(*source_table), K(*target_table));
  }
  return ret;
}

int ObTransformJoinElimination::do_join_elimination_foreign_key(ObDMLStmt* stmt, const TableItem* child_table,
    const TableItem* parent_table, const ObForeignKeyInfo* foreign_key_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else {
    if (OB_FAIL(trans_column_items_foreign_key(stmt, child_table, parent_table, foreign_key_info))) {
      LOG_WARN("failed to transform columns of the eliminated table", K(ret));
    } else if (OB_FAIL(trans_table_item(stmt, child_table, parent_table))) {
      LOG_WARN("failed to transform target into source", K(ret));
    }
  }
  return ret;
}

int ObTransformJoinElimination::eliminate_join_in_joined_table(ObDMLStmt* stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  bool happened = false;
  trans_happened = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(stmt), K(ctx_), K(ret));
  } else {
    ObSEArray<JoinedTable*, 4> joined_tables;
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_from_item_size(); i++) {
      FromItem& cur_from_item = stmt->get_from_item(i);
      if (!cur_from_item.is_joined_) {
        /*do nothing*/
      } else if (OB_FAIL(eliminate_join_in_joined_table(stmt, cur_from_item, joined_tables, happened))) {
        LOG_WARN("failed to eliminate self key join in joined table.", K(ret));
      } else {
        trans_happened |= happened;
      }
    }
    if (OB_SUCC(ret) && trans_happened) {
      if (OB_FAIL(stmt->get_joined_tables().assign(joined_tables))) {
        LOG_WARN("failed to reset joined table container", K(ret));
      } else if (OB_FAIL(stmt->rebuild_tables_hash())) {
        LOG_WARN("failed to rebuild table hash", K(ret));
      } else if (OB_FAIL(stmt->update_column_item_rel_id())) {
        LOG_WARN("failed to update colun item rel id", K(ret));
      } else if (OB_FAIL(stmt->formalize_stmt(ctx_->session_info_))) {
        LOG_WARN("failed to formalize stmt", K(ret));
      } else {
        LOG_TRACE("succ to do self key join in joined table elimination to remove.");
      }
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObTransformJoinElimination::eliminate_join_in_joined_table(
    ObDMLStmt* stmt, FromItem& from_item, ObIArray<JoinedTable*>& joined_tables, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  bool can_trans = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null.", K(ret));
  } else if (from_item.is_joined_) {
    TableItem* joined_table = stmt->get_joined_table(from_item.table_id_);
    ObSEArray<TableItem*, 4> child_candi_tables;
    ObSEArray<ObRawExpr*, 16> trans_conditions;
    if (OB_ISNULL(joined_table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("joined table is null.", K(ret));
    } else if (OB_FAIL(eliminate_join_in_joined_table(
                   stmt, joined_table, child_candi_tables, trans_conditions, can_trans))) {
      LOG_WARN("failed to eliminate self key join in joined table.", K(ret));
    } else if (OB_ISNULL(joined_table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("joined table is null.", K(ret));
    } else if (!can_trans) {
      /* do nothing. */
    } else if (OB_FAIL(stmt->add_condition_exprs(trans_conditions))) {
      LOG_WARN("add trans conditions to where conditions failed", K(ret));
    } else {
      trans_happened = true;
      from_item.is_joined_ = joined_table->is_joined_table();
      from_item.table_id_ = joined_table->table_id_;
    }
    if (OB_FAIL(ret)) {
      /*do nothing*/
    } else if (!joined_table->is_joined_table()) {
      /*do nothing*/
    } else if (OB_FAIL(joined_tables.push_back(static_cast<JoinedTable*>(joined_table)))) {
      LOG_WARN("failed to push back joined table.", K(ret));
    } else { /*do nothing.*/
    }
  } else { /*do nothing.*/
  }
  return ret;
}

int ObTransformJoinElimination::extract_equal_join_columns(const ObIArray<ObRawExpr*>& join_conds,
    const TableItem* source_table, const TableItem* target_table, ObIArray<ObRawExpr*>& source_exprs,
    ObIArray<ObRawExpr*>& target_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(source_table) || OB_ISNULL(target_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parameter is null", K(ret), K(source_table), K(target_table));
  } else {
    const uint64_t source_tid = source_table->table_id_;
    const uint64_t target_tid = target_table->table_id_;
    for (int64_t i = 0; OB_SUCC(ret) && i < join_conds.count(); ++i) {
      ObRawExpr* expr = join_conds.at(i);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null", K(ret));
      } else if (T_OP_EQ == expr->get_expr_type()) {
        ObOpRawExpr* op = static_cast<ObOpRawExpr*>(expr);
        if (op->get_param_expr(0)->has_flag(IS_COLUMN) && op->get_param_expr(1)->has_flag(IS_COLUMN)) {
          ObColumnRefRawExpr* col1 = static_cast<ObColumnRefRawExpr*>(op->get_param_expr(0));
          ObColumnRefRawExpr* col2 = static_cast<ObColumnRefRawExpr*>(op->get_param_expr(1));
          if (col1->get_table_id() == source_tid && col2->get_table_id() == target_tid) {
            if (OB_FAIL(source_exprs.push_back(col1))) {
              LOG_WARN("failed to push back expr", K(ret));
            } else if (OB_FAIL(target_exprs.push_back(col2))) {
              LOG_WARN("failed to push back expr", K(ret));
            }
          } else if (col1->get_table_id() == target_tid && col2->get_table_id() == source_tid) {
            if (OB_FAIL(target_exprs.push_back(col1))) {
              LOG_WARN("failed to push back expr", K(ret));
            } else if (OB_FAIL(source_exprs.push_back(col2))) {
              LOG_WARN("failed to push back expr", K(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObTransformJoinElimination::adjust_table_items(
    ObDMLStmt* stmt, TableItem* source_table, TableItem* target_table, ObStmtMapInfo& stmt_map_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt) || OB_ISNULL(source_table) || OB_ISNULL(target_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(stmt), K(source_table), K(target_table), K(ret));
  } else if (source_table->is_basic_table() && target_table->is_basic_table()) {
    if (OB_FAIL(ObTransformUtils::merge_table_items(stmt, source_table, target_table, NULL))) {
      LOG_WARN("failed to check merge table items", K(ret));
    } else { /*do nothing*/
    }
  } else if (source_table->is_generated_table() && target_table->is_generated_table()) {
    ObSelectStmt* target_stmt = NULL;
    ObSelectStmt* source_stmt = NULL;
    ObSEArray<int64_t, 16> column_map;
    if (OB_ISNULL(source_stmt = source_table->ref_query_) || OB_ISNULL(target_stmt = target_table->ref_query_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(source_stmt), K(target_stmt), K(ret));
    } else if (OB_FAIL(
                   reverse_select_items_map(source_stmt, target_stmt, stmt_map_info.select_item_map_, column_map))) {
      LOG_WARN("failed to reverse select items map", K(ret));
    } else if (OB_FAIL(ObTransformUtils::merge_table_items(stmt, source_table, target_table, &column_map))) {
      LOG_WARN("failed to merge table items", K(ret));
    } else if (OB_FAIL(create_missing_select_items(source_stmt, target_stmt, column_map, stmt_map_info.table_map_))) {
      LOG_WARN("failed to create missing select items", K(ret));
    } else { /*do nothing*/
    }
  } else { /*do nothing*/
  }

  return ret;
}

int ObTransformJoinElimination::create_missing_select_items(ObSelectStmt* source_stmt, ObSelectStmt* target_stmt,
    const ObIArray<int64_t>& column_map, const ObIArray<int64_t>& table_map)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(source_stmt) || OB_ISNULL(target_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(source_stmt), K(target_stmt), K(ret));
  } else if (OB_UNLIKELY(column_map.count() != target_stmt->get_select_item_size())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected array count", K(column_map.count()), K(target_stmt->get_select_item_size()), K(ret));
  } else {
    ObSEArray<ObRawExpr*, 16> new_exprs;
    ObSEArray<ObRawExpr*, 16> old_exprs;
    ObSEArray<SelectItem*, 16> miss_select_items;
    for (int64_t i = 0; OB_SUCC(ret) && i < target_stmt->get_select_item_size(); i++) {
      if (OB_INVALID_ID != column_map.at(i)) {
        /*do nothing*/
      } else if (OB_FAIL(miss_select_items.push_back(&target_stmt->get_select_item(i)))) {
        LOG_WARN("failed to push back select item", K(ret));
      } else { /*do nothing*/
      }
    }

    if (OB_SUCC(ret) && !miss_select_items.empty()) {
      for (int64_t i = 0; OB_SUCC(ret) && i < table_map.count(); i++) {
        TableItem* temp_source_table = NULL;
        TableItem* temp_target_table = NULL;
        int64_t idx = table_map.at(i);
        if (idx < 0 || idx >= target_stmt->get_table_size() || 
            i >= source_stmt->get_table_size()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpect table idx", K(ret));
        } else if (OB_ISNULL(temp_source_table = source_stmt->get_table_item(i)) ||
            OB_ISNULL(temp_target_table = target_stmt->get_table_item(idx))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(temp_source_table), K(temp_target_table), K(ret));
        } else if ((temp_source_table->is_basic_table() && temp_target_table->is_basic_table()) ||
                   (temp_source_table->is_generated_table() && temp_target_table->is_generated_table())) {
          if (OB_FAIL(ObTransformUtils::merge_table_items(
                  source_stmt, target_stmt, temp_source_table, temp_target_table, old_exprs, new_exprs))) {
            LOG_WARN("failed to merge table items", K(ret));
          } else { /*do nothing*/
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected table type", K(temp_source_table->type_), K(temp_target_table->type_), K(ret));
        }
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < miss_select_items.count(); i++) {
      if (OB_ISNULL(miss_select_items.at(i)->expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(ObTransformUtils::replace_expr(old_exprs, new_exprs, miss_select_items.at(i)->expr_))) {
        LOG_WARN("failed to replace expr", K(ret));
      } else if (OB_FAIL(ObTransformUtils::adjust_agg_and_win_expr(source_stmt, miss_select_items.at(i)->expr_))) {
        LOG_WARN("failed to remove duplicated agg expr", K(ret));
      } else if (OB_FAIL(source_stmt->add_select_item(*miss_select_items.at(i)))) {
        LOG_WARN("failed to add select item", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObTransformJoinElimination::reverse_select_items_map(const ObSelectStmt* source_stmt,
    const ObSelectStmt* target_stmt, const ObIArray<int64_t>& column_map, ObIArray<int64_t>& reverse_column_map)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(source_stmt) || OB_ISNULL(target_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(source_stmt), K(target_stmt), K(ret));
  } else if (OB_UNLIKELY(column_map.count() != source_stmt->get_select_item_size())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected array count", K(column_map.count()), K(source_stmt->get_select_item_size()), K(ret));
  } else if (OB_FAIL(reverse_column_map.prepare_allocate(target_stmt->get_select_item_size()))) {
    LOG_WARN("failed to allocate memory", K(ret));
  } else {
    int64_t source_count = source_stmt->get_select_item_size();
    int64_t target_count = target_stmt->get_select_item_size();
    for (int64_t i = 0; OB_SUCC(ret) && i < target_count; i++) {
      reverse_column_map.at(i) = OB_INVALID_ID;
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < source_count; i++) {
      if (OB_INVALID_ID == column_map.at(i)) {
        /*do nothing*/
      } else if (OB_UNLIKELY(column_map.at(i) < 0 || column_map.at(i) >= target_count)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected array count", K(column_map.at(i)), K(target_count), K(ret));
      } else {
        reverse_column_map.at(column_map.at(i)) = i;
      }
    }
  }
  return ret;
}

int ObTransformJoinElimination::trans_table_item(
    ObDMLStmt* stmt, const TableItem* source_table, const TableItem* target_table)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt) || OB_ISNULL(source_table) || OB_ISNULL(target_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parameter is null", K(ret), K(stmt), K(source_table), K(target_table));
  } else {
    if (OB_FAIL(ObTransformUtils::replace_table_in_semi_infos(stmt, source_table, target_table))) {
      LOG_WARN("failed to replace table info in semi infos", K(ret));
    } else if (OB_FAIL(stmt->remove_table_item(target_table))) {
      LOG_WARN("failed to remove target table items", K(ret));
    } else if (OB_FAIL(stmt->remove_from_item(target_table->table_id_))) {
      LOG_WARN("failed to remove assoicated from item", K(ret));
    } else if (OB_FAIL(stmt->remove_part_expr_items(target_table->table_id_))) {
      LOG_WARN("failed to remove part expr item", K(ret));
    } else if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->session_info_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("transform context is null", K(ret));
    } else if (OB_FAIL(stmt->rebuild_tables_hash())) {
      LOG_WARN("rebuild table hash failed", K(ret));
    } else if (OB_FAIL(stmt->update_column_item_rel_id())) {
      LOG_WARN("failed to update columns' relation id", K(ret));
    } else if (OB_FAIL(stmt->formalize_stmt(ctx_->session_info_))) {
      LOG_WARN("formalize stmt is failed", K(ret));
    }
  }
  return ret;
}

int ObTransformJoinElimination::check_transform_validity_outer_join(
    ObDMLStmt* stmt, JoinedTable* joined_table, ObIArray<ObRawExpr*>& relation_exprs, bool& is_valid)
{
  int ret = OB_SUCCESS;
  bool is_unique = false;
  ObSqlBitSet<8, int64_t> rel_ids;
  ObSEArray<ObRawExpr*, 8> target_exprs;
  ObSEArray<ObRawExpr*, 8> join_conditions;
  ObSqlBitSet<> join_source_ids;
  ObSqlBitSet<> join_target_ids;
  if (OB_ISNULL(stmt) || OB_ISNULL(joined_table) || OB_ISNULL(joined_table->left_table_) ||
      OB_ISNULL(joined_table->right_table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt or joined table is null.", K(ret), K(stmt), K(joined_table));
  } else if (LEFT_OUTER_JOIN != joined_table->joined_type_) {
    is_valid = false;
  } else if (OB_FAIL(ObTransformUtils::get_table_joined_exprs(*stmt,
                 *joined_table->left_table_,
                 *joined_table->right_table_,
                 joined_table->join_conditions_,
                 target_exprs,
                 join_source_ids,
                 join_target_ids))) {
    LOG_WARN("failed to get table joined exprs.", K(ret));
  } else if (join_source_ids.num_members() != 1 || join_target_ids.num_members() != 1) {
    is_valid = false;
  } else if (OB_FAIL(ObTransformUtils::check_exprs_unique(*stmt,
                 joined_table->right_table_,
                 target_exprs,
                 ctx_->session_info_,
                 ctx_->schema_checker_,
                 is_unique))) {
    LOG_WARN("failed to check exptrs all row expr.", K(ret));
  } else if (!is_unique) {
    is_valid = false;
  } else if (OB_FAIL(join_conditions.assign(joined_table->join_conditions_))) {
    LOG_WARN("failed to push back to join conditions.", K(ret));
  } else if (OB_FAIL(extract_child_conditions(stmt, joined_table->right_table_, join_conditions, rel_ids))) {
    LOG_WARN("failed to extract right child exprs.", K(ret));
  } else if (OB_FAIL(adjust_relation_exprs(rel_ids,  // right table not use by any other expr
                 join_conditions,
                 relation_exprs,
                 is_valid))) {
    LOG_WARN("failed to check expr in select items.", K(ret));
  } else { /* do nothing. */
  }
  return ret;
}

int ObTransformJoinElimination::extract_child_conditions(
    ObDMLStmt* stmt, TableItem* table_item, ObIArray<ObRawExpr*>& join_conditions, ObSqlBitSet<8, int64_t>& rel_ids)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt or joined table is null.", K(table_item), K(ret));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_stack_overflow));
  } else if (table_item->type_ == TableItem::JOINED_TABLE) {
    JoinedTable* joined_table = static_cast<JoinedTable*>(table_item);
    if (OB_FAIL(SMART_CALL(extract_child_conditions(stmt, joined_table->left_table_, join_conditions, rel_ids)))) {
      LOG_WARN("failed to remove right tables from stmt", K(ret));
    } else if (OB_FAIL(
                   SMART_CALL(extract_child_conditions(stmt, joined_table->right_table_, join_conditions, rel_ids)))) {
      LOG_WARN("failed to remove right tables from stmt", K(ret));
    } else {
      int64_t N = joined_table->join_conditions_.count();
      for (int64_t i = 0; OB_SUCC(ret) && i < N; i++) {
        if (OB_FAIL(join_conditions.push_back(joined_table->join_conditions_.at(i)))) {
          LOG_WARN("failed to push back to join condition.", K(ret));
        } else { /* do nothing. */
        }
      }
    }
  } else if (OB_FAIL(rel_ids.add_member(stmt->get_table_bit_index(table_item->table_id_)))) {
    LOG_WARN("failed to add member to rel ids.", K(ret));
  } else { /* do nothing. */
  }
  return ret;
}

int ObTransformJoinElimination::adjust_relation_exprs(const ObSqlBitSet<8, int64_t>& rel_ids,
    const ObIArray<ObRawExpr*>& join_conditions, ObIArray<ObRawExpr*>& relation_exprs, bool& is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  ObSqlBitSet<> select_rel_ids;
  ObSEArray<ObRawExpr*, 8> all_exprs;
  if (OB_FAIL(all_exprs.assign(relation_exprs))) {
    LOG_WARN("failed to assign to all exprs.", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::remove_item(all_exprs, join_conditions))) {
    LOG_WARN("faile to remove item from all stmt exprs.", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < all_exprs.count(); i++) {
    if (OB_ISNULL(all_exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr in select item is null.", K(ret));
    } else if (OB_FAIL(select_rel_ids.add_members2(all_exprs.at(i)->get_relation_ids()))) {
      LOG_WARN("failed to add members into select items.", K(ret));
    } else { /*do nothing.*/
    }
  }
  if (OB_SUCC(ret)) {
    is_valid = select_rel_ids.overlap2(rel_ids) ? false : true;
    if (is_valid && OB_FAIL(relation_exprs.assign(all_exprs))) {
      LOG_WARN("failed to assign cross stmt exprs.", K(ret));
    } else { /* do nothing. */
    }
  }
  return ret;
}

int ObTransformJoinElimination::eliminate_outer_join_in_joined_table(ObDMLStmt* stmt, TableItem*& table_item,
    ObIArray<uint64_t>& table_ids, ObIArray<ObRawExpr*>& relation_exprs, bool& trans_happen)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt or joined table is null.", K(stmt), K(table_item), K(ret));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_stack_overflow));
  } else if (table_item->type_ == TableItem::JOINED_TABLE) {
    bool is_valid = false;
    JoinedTable* joined_table = static_cast<JoinedTable*>(table_item);
    if (OB_FAIL(check_transform_validity_outer_join(stmt, joined_table, relation_exprs, is_valid))) {
      LOG_WARN("failed to check transform validity outer join.", K(ret));
    } else if (!is_valid) {
      if (OB_FAIL(SMART_CALL(eliminate_outer_join_in_joined_table(
              stmt, joined_table->right_table_, table_ids, relation_exprs, trans_happen)))) {
        LOG_WARN("failed to eliminate ouer join in joined table.", K(ret));
      } else if (OB_FAIL(SMART_CALL(eliminate_outer_join_in_joined_table(
                     stmt, joined_table->left_table_, table_ids, relation_exprs, trans_happen)))) {
        LOG_WARN("failed to eliminate ouer join in joined table.", K(ret));
      } else if (OB_FAIL(ObTransformUtils::adjust_single_table_ids(joined_table))) {
        LOG_WARN("failed to construct single table ids.", K(ret));
      } else { /* do nothing. */
      }
    } else if (OB_ISNULL(joined_table->left_table_) || OB_ISNULL(joined_table->right_table_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("left child or right child is null.", K(ret));
    } else if (OB_FAIL(SMART_CALL(eliminate_outer_join_in_joined_table(
                   stmt, joined_table->left_table_, table_ids, relation_exprs, trans_happen)))) {
      LOG_WARN("failed to eliminate ouer join in joined table.", K(ret));
    } else if (OB_FAIL(ObTransformUtils::remove_tables_from_stmt(stmt, joined_table->right_table_, table_ids))) {
      LOG_WARN("failed to remove table items.", K(ret));
    } else {
      table_item = joined_table->left_table_;
      trans_happen = true;
    }
  }
  return ret;
}

int ObTransformJoinElimination::eliminate_outer_join_in_from_items(ObDMLStmt* stmt, FromItem& from_item,
    ObIArray<uint64_t>& table_ids, ObIArray<JoinedTable*>& joined_tables, ObIArray<ObRawExpr*>& relation_exprs,
    bool& trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  bool can_trans = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null.", K(ret));
  } else if (from_item.is_joined_) {
    TableItem* joined_table = stmt->get_joined_table(from_item.table_id_);
    if (OB_ISNULL(joined_table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("joined table is null.", K(ret));
    } else if (OB_FAIL(
                   eliminate_outer_join_in_joined_table(stmt, joined_table, table_ids, relation_exprs, can_trans))) {
      LOG_WARN("failed to eliminate outer join in joined table.", K(ret));
    } else if (OB_ISNULL(joined_table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("joined table is null.", K(ret));
    } else if (!can_trans) { /* do nothing. */
    } else {
      trans_happened = true;
      from_item.is_joined_ = joined_table->is_joined_table();
      from_item.table_id_ = joined_table->table_id_;
    }
    if (OB_FAIL(ret)) {
      /*do nothing*/
    } else if (!joined_table->is_joined_table()) {
      /*do nothing*/
    } else if (OB_FAIL(joined_tables.push_back(static_cast<JoinedTable*>(joined_table)))) {
      LOG_WARN("failed to push back joined table.", K(ret));
    } else { /*do nothing.*/
    }
  } else { /*do nothing.*/
  }
  return ret;
}

int ObTransformJoinElimination::eliminate_outer_join(ObDMLStmt* stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 8> relation_exprs;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->schema_checker_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(stmt), K(ctx_), K(ret));
  } else if (OB_FAIL(ObTransformUtils::right_join_to_left(stmt))) {
    LOG_WARN("failed to change right outer join to left.", K(ret));
  } else if (OB_FAIL(stmt->get_relation_exprs(relation_exprs))) {
    LOG_WARN("failed to get relations exprs all.", K(ret));
  } else {
    SemiInfo* semi_info = NULL;
    bool is_happend = false;
    common::ObArray<JoinedTable*> joined_tables;
    ObSEArray<uint64_t, 4> table_ids;
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_from_item_size(); i++) {
      FromItem& cur_from_item = stmt->get_from_item(i);
      if (OB_FAIL(eliminate_outer_join_in_from_items(
              stmt, cur_from_item, table_ids, joined_tables, relation_exprs, is_happend))) {
        LOG_WARN("failed to eliminate outer join in from items.", K(ret));
      } else {
        trans_happened |= is_happend;
      }
    }

    if (OB_SUCC(ret) && trans_happened) {
      ObIArray<SemiInfo*>& semi_infos = stmt->get_semi_infos();
      for (int64_t i = 0; OB_SUCC(ret) && i < semi_infos.count(); ++i) {
        if (OB_ISNULL(semi_infos.at(i) = semi_infos.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("semi info is null", K(ret));
        } else if (OB_FAIL(ObOptimizerUtil::remove_item(semi_infos.at(i)->left_table_ids_, table_ids))) {
          LOG_WARN("failed to eliminate outer join in from items.", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(stmt->get_joined_tables().assign(joined_tables))) {
        LOG_WARN("failed to reset joined table container", K(ret));
      } else if (OB_FAIL(stmt->rebuild_tables_hash())) {
        LOG_WARN("failed to rebuild table hash", K(ret));
      } else if (OB_FAIL(stmt->update_column_item_rel_id())) {
        LOG_WARN("failed to update colun item rel id", K(ret));
      } else if (OB_FAIL(stmt->formalize_stmt(ctx_->session_info_))) {
        LOG_WARN("failed to formalize stmt", K(ret));
      } else {
        LOG_TRACE("succ to do outer join elimination to remove.");
      }
    } else {
    }
  }
  return ret;
}

int ObTransformJoinElimination::get_eliminable_tables(const ObDMLStmt* stmt, const ObIArray<ObRawExpr*>& conds,
    const ObIArray<TableItem*>& candi_tables, const ObIArray<TableItem*>& child_candi_tables, EliminationHelper& helper)
{
  int ret = OB_SUCCESS;
  ObForeignKeyInfo* foreign_key_info = NULL;
  const uint64_t N_target = candi_tables.count();
  const uint64_t N_other = child_candi_tables.count();
  bool is_first_table_parent = false;
  bool can_be_eliminated = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < N_target; ++i) {
    TableItem* target_table = candi_tables.at(i);
    bool target_be_eliminated = false;
    for (int64_t j = i + 1; OB_SUCC(ret) && j < N_target; ++j) {
      TableItem* source_table = candi_tables.at(j);
      if (OB_FAIL(check_transform_validity_foreign_key(
              stmt, conds, source_table, target_table, can_be_eliminated, is_first_table_parent, foreign_key_info))) {
        LOG_WARN("failed to check whether transformation is possible", K(ret));
      } else if (can_be_eliminated) {
        TableItem* parent_table = NULL;
        TableItem* child_table = NULL;
        if (is_first_table_parent) {
          parent_table = source_table;
          child_table = target_table;
        } else {
          parent_table = target_table;
          child_table = source_table;
          target_be_eliminated = true;
        }
        if (ObOptimizerUtil::find_item(helper.parent_table_items_, parent_table)) {
        } else if (OB_FAIL(helper.push_back(child_table, parent_table, foreign_key_info))) {
          LOG_WARN("failed to push back table item or foreign key info", K(ret));
        }
      }
    }
    for (int64_t j = 0; OB_SUCC(ret) && !target_be_eliminated && j < N_other; ++j) {
      TableItem* source_table = child_candi_tables.at(j);
      if (OB_FAIL(check_transform_validity_foreign_key(
              stmt, conds, source_table, target_table, can_be_eliminated, is_first_table_parent, foreign_key_info))) {
        LOG_WARN("failed to check whether transformation is possible", K(ret));
      } else if (!can_be_eliminated || is_first_table_parent) {
      } else if (OB_FAIL(helper.push_back(source_table, target_table, foreign_key_info))) {
        LOG_WARN("failed to push back table item or foreign key info", K(ret));
      } else {
        target_be_eliminated = true;
      }
    }
  }
  return ret;
}

int ObTransformJoinElimination::check_transform_validity_foreign_key(const ObDMLStmt* stmt,
    const ObIArray<ObRawExpr*>& join_conds, const TableItem* source_table, const TableItem* target_table,
    bool& can_be_eliminated, bool& is_first_table_parent, ObForeignKeyInfo*& foreign_key_info)
{
  int ret = OB_SUCCESS;
  can_be_eliminated = false;
  bool is_foreign_primary_join = false;
  bool all_primary_key = false;
  bool is_rely_foreign_key = false;
  ObSEArray<ObRawExpr*, 8> source_exprs;
  ObSEArray<ObRawExpr*, 8> target_exprs;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_) || OB_ISNULL(source_table) || OB_ISNULL(target_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parameter have null", K(ctx_), K(source_table), K(target_table), K(ret));
  } else if (source_table->is_basic_table() && target_table->is_basic_table()) {
    if (OB_FAIL(extract_equal_join_columns(join_conds, source_table, target_table, source_exprs, target_exprs))) {
      LOG_WARN("failed to extract join columns", K(ret));
    } else if (source_exprs.empty()) {
      /* do nothing */
    } else if (OB_FAIL(ObTransformUtils::check_foreign_primary_join(source_table,
                   target_table,
                   source_exprs,
                   target_exprs,
                   ctx_->schema_checker_,
                   is_foreign_primary_join,
                   is_first_table_parent,
                   foreign_key_info))) {
      LOG_WARN("failed to check has foreign key constraint", K(ret));
    } else if (!is_foreign_primary_join) {
      /* do nothing */
    } else if (is_first_table_parent && OB_NOT_NULL((stmt->get_stmt_hint().get_part_hint(source_table->table_id_)))) {

    } else if (!is_first_table_parent && OB_NOT_NULL((stmt->get_stmt_hint().get_part_hint(target_table->table_id_)))) {

    } else if (OB_FAIL(
                   ObTransformUtils::is_foreign_key_rely(ctx_->session_info_, foreign_key_info, is_rely_foreign_key))) {
      LOG_WARN("check foreign key is rely failed", K(ret));
    } else if (!is_rely_foreign_key) {
    } else if (is_first_table_parent && OB_FAIL(check_all_column_primary_key(
                                            stmt, source_table->table_id_, foreign_key_info, all_primary_key))) {
      LOG_WARN("failed to check all column primary key", K(ret));
    } else if (!is_first_table_parent && OB_FAIL(check_all_column_primary_key(
                                             stmt, target_table->table_id_, foreign_key_info, all_primary_key))) {
      LOG_WARN("failed to check all column primary key", K(ret));
    } else if (all_primary_key) {
      can_be_eliminated = true;
    }
  }
  return ret;
}

int ObTransformJoinElimination::check_all_column_primary_key(
    const ObDMLStmt* stmt, const uint64_t table_id, const ObForeignKeyInfo* info, bool& all_primary_key)
{
  int ret = OB_SUCCESS;
  all_primary_key = true;
  if (OB_ISNULL(info) || OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parameters have null", K(ret), K(stmt), K(info));
  } else {
    const ObIArray<ColumnItem>& all_columns = stmt->get_column_items();
    for (int64_t i = 0; all_primary_key && i < all_columns.count(); ++i) {
      const ColumnItem& item = all_columns.at(i);
      if (item.table_id_ == table_id) {
        bool find = false;
        for (int64_t j = 0; !find && j < info->parent_column_ids_.count(); ++j) {
          if (info->parent_column_ids_.at(j) == item.column_id_) {
            find = true;
          }
        }
        all_primary_key = find;
      }
    }
  }
  return ret;
}

int ObTransformJoinElimination::trans_column_items_foreign_key(ObDMLStmt* stmt, const TableItem* child_table,
    const TableItem* parent_table, const ObForeignKeyInfo* foreign_key_info)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 16> from_col_exprs;
  ObSEArray<ObRawExpr*, 16> to_col_exprs;
  if (OB_ISNULL(stmt) || OB_ISNULL(child_table) || OB_ISNULL(parent_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parameters", K(ret), K(stmt), K(child_table), K(parent_table));
  } else {
    const uint64_t child_table_id = child_table->table_id_;
    const uint64_t parent_table_id = parent_table->table_id_;
    ObSEArray<ColumnItem, 16> origin_column_items;
    origin_column_items.assign(stmt->get_column_items());
    for (int64_t i = 0; OB_SUCC(ret) && i < origin_column_items.count(); ++i) {
      const ColumnItem& item = origin_column_items.at(i);
      if (parent_table_id == item.table_id_) {
        uint64_t child_column_id;
        if (OB_FAIL(get_child_column_id_by_parent_column_id(foreign_key_info, item.column_id_, child_column_id))) {
          LOG_WARN("get child column id by parent column id failed", K(ret));
        } else {
          ColumnItem* child_col = stmt->get_column_item_by_id(child_table_id, child_column_id);
          ColumnItem* parent_col = stmt->get_column_item_by_id(parent_table_id, item.column_id_);
          if (OB_ISNULL(parent_col) || OB_ISNULL(child_col)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("parent column or source column is null", K(ret));
          } else {
            // duplicate column item, to be replaced and remove
            if (OB_FAIL(from_col_exprs.push_back(parent_col->get_expr())) ||
                OB_FAIL(to_col_exprs.push_back(child_col->get_expr()))) {
              LOG_WARN("failed to push back expr", K(ret));
            } else if (OB_FAIL(stmt->remove_column_item(item.table_id_, item.column_id_))) {
              LOG_WARN("failed to remove column item", K(ret), K(item));
            }
          }
        }
      }
    }
    if (OB_SUCC(ret) && !from_col_exprs.empty()) {
      if (OB_FAIL(stmt->replace_inner_stmt_expr(from_col_exprs, to_col_exprs))) {
        LOG_WARN("failed to replace col exprs", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformJoinElimination::get_child_column_id_by_parent_column_id(
    const ObForeignKeyInfo* info, const uint64_t parent_column_id, uint64_t& child_column_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("foreign key info should not be null", K(info));
  } else {
    bool find = false;
    for (int64_t i = 0; !find && i < info->parent_column_ids_.count(); ++i) {
      if (parent_column_id == info->parent_column_ids_.at(i)) {
        child_column_id = info->child_column_ids_.at(i);
        find = true;
      }
    }
  }
  return ret;
}

int ObTransformJoinElimination::EliminationHelper::is_table_in_child_items(const TableItem* target, bool& find)
{
  int ret = OB_SUCCESS;
  find = false;
  if (OB_ISNULL(target)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("target is null", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && !find && i < count_; ++i) {
      if (bitmap_.at(i)) {
        /* table already eliminated */
      } else if (OB_ISNULL(child_table_items_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table item in child_table_items_ should not be null", K(ret));
      } else if (child_table_items_.at(i)->table_id_ == target->table_id_) {
        find = true;
      }
    }
  }
  return ret;
}

int ObTransformJoinElimination::eliminate_semi_join_self_foreign_key(ObDMLStmt* stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  ObSEArray<SemiInfo*, 2> semi_infos;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(stmt), K(ret));
  } else if (OB_FAIL(semi_infos.assign(stmt->get_semi_infos()))) {  // copy origin semi infos
    LOG_WARN("failed to assign semi infos", K(ret));
  } else {
    bool is_happened = false;
    ObSEArray<ObRawExpr*, 16> conds;
    SemiInfo* semi_info = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < semi_infos.count(); ++i) {
      conds.reuse();
      if (OB_ISNULL(semi_info = semi_infos.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(semi_info));
      } else if (semi_info->is_semi_join() && OB_FAIL(stmt->get_equal_set_conditions(conds, true, SCOPE_WHERE))) {
        LOG_WARN("failed to get equal set conditions", K(ret));
      } else if (semi_info->is_anti_join() && OB_FAIL(conds.assign(semi_info->semi_conditions_))) {
        LOG_WARN("failed to assign anti conditions", K(ret));
      } else if (OB_FAIL(eliminate_semi_join_self_key(stmt, semi_info, conds, is_happened))) {
        LOG_WARN("failed to eliminate semi join self key", K(ret));
      } else if (is_happened) {
        trans_happened = true;
      } else if (OB_FAIL(eliminate_semi_join_foreign_key(stmt, semi_info, conds, is_happened))) {
        LOG_WARN("failed to eliminate semi join foreign key", K(ret));
      } else {
        trans_happened |= is_happened;
      }
    }
  }
  return ret;
}

int ObTransformJoinElimination::eliminate_semi_join_self_key(
    ObDMLStmt* stmt, SemiInfo* semi_info, ObIArray<ObRawExpr*>& conds, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(semi_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("semi info is null", K(ret), K(stmt), K(semi_info));
  } else {
    ObSEArray<TableItem*, 16> left_tables, right_tables;
    ObSEArray<ObStmtMapInfo, 8> stmt_map_infos;
    ObSEArray<int64_t, 8> rel_map_info;
    bool can_be_eliminated = false;
    if (OB_FAIL(check_transform_validity_semi_self_key(
            stmt, semi_info, conds, left_tables, right_tables, stmt_map_infos, rel_map_info, can_be_eliminated))) {
      LOG_WARN("check whether tranformation is possibl failed", K(ret));
    } else if (!can_be_eliminated) {
      /*do nothing*/
    } else if (OB_FAIL(ObOptimizerUtil::remove_item(stmt->get_semi_infos(), semi_info))) {
      LOG_WARN("failed to remove item", K(ret));
    } else if (OB_FAIL(do_elimination_semi_join_self_key(
                   stmt, semi_info, left_tables, right_tables, stmt_map_infos, rel_map_info))) {
      LOG_WARN("do eliminate semi join self key failed", K(ret));
    } else {
      trans_happened = true;
    }
  }
  return ret;
}

int ObTransformJoinElimination::eliminate_semi_join_foreign_key(
    ObDMLStmt* stmt, SemiInfo* semi_info, ObIArray<ObRawExpr*>& conds, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(semi_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("semi info is null", K(ret), K(stmt), K(semi_info));
  } else {
    TableItem* left_table = NULL;
    TableItem* right_table = NULL;
    ObForeignKeyInfo* foreign_key_info = NULL;
    bool can_be_eliminated = false;
    if (OB_FAIL(check_transform_validity_semi_foreign_key(
            stmt, semi_info, conds, left_table, right_table, foreign_key_info, can_be_eliminated))) {
      LOG_WARN("check whether tranformation is possibl failed", K(ret));
    } else if (!can_be_eliminated) {
      /*do nothing*/
    } else if (OB_FAIL(ObOptimizerUtil::remove_item(stmt->get_semi_infos(), semi_info))) {
      LOG_WARN("failed to remove item", K(ret));
    } else if (OB_FAIL(trans_semi_condition_exprs(stmt, semi_info))) {
      LOG_WARN("transform anti condition failed", K(ret));
    } else if (OB_FAIL(trans_column_items_foreign_key(stmt, left_table, right_table, foreign_key_info))) {
      LOG_WARN("failed to transform columns of the eliminated table", K(ret));
    } else if (OB_FAIL(trans_semi_table_item(stmt, left_table, right_table))) {
      LOG_WARN("transform semi right table item failed", K(ret));
    } else {
      trans_happened = true;
    }
  }
  return ret;
}

int ObTransformJoinElimination::check_transform_validity_semi_self_key(ObDMLStmt* stmt, SemiInfo* semi_info,
    ObIArray<ObRawExpr*>& candi_conds, ObIArray<TableItem*>& left_tables, ObIArray<TableItem*>& right_tables,
    ObIArray<ObStmtMapInfo>& stmt_map_infos, ObIArray<int64_t>& rel_map_info, bool& can_be_eliminated)
{
  int ret = OB_SUCCESS;
  bool is_contain = false;
  bool source_unique = false;
  bool target_unique = false;
  bool is_simple_join_condition = false;
  bool right_tables_have_filter = false;
  bool is_simple_filter = false;
  can_be_eliminated = false;
  TableItem* right_table = NULL;
  ObSEArray<ObRawExpr*, 16> source_exprs;
  ObSEArray<ObRawExpr*, 16> target_exprs;
  ObSEArray<ObRawExpr*, 16> left_table_conditions;
  ObSEArray<ObRawExpr*, 16> right_table_conditions;
  if (OB_ISNULL(stmt) || OB_ISNULL(semi_info) || OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexcepted null", K(ret));
  } else if (1 != semi_info->left_table_ids_.count()) {
    /*do nothing*/
  } else if (OB_ISNULL(right_table = stmt->get_table_item_by_id(semi_info->right_table_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected semi right table", K(ret), K(*semi_info));
  } else if (OB_FAIL(right_tables.push_back(right_table))) {
    LOG_WARN("failed to push back table", K(ret));
  } else if (OB_FAIL(stmt->get_table_item_by_id(semi_info->left_table_ids_, left_tables))) {
    LOG_WARN("failed to get table items", K(ret));
  } else if (OB_FAIL(ObTransformUtils::check_relations_containment(
                 stmt, left_tables, right_tables, stmt_map_infos, rel_map_info, is_contain))) {
    LOG_WARN("check relations containment failed", K(ret));
  } else if (!is_contain) {
    /*do nothing*/
  } else if (OB_FAIL(check_semi_join_condition(stmt,
                 semi_info,
                 left_tables,
                 right_tables,
                 stmt_map_infos,
                 rel_map_info,
                 source_exprs,
                 target_exprs,
                 is_simple_join_condition,
                 right_tables_have_filter,
                 is_simple_filter))) {
    LOG_WARN("check semi join condition failed", K(ret));
  } else if (source_exprs.empty()) {
    /*do nothing*/
  } else if (is_simple_join_condition && !right_tables_have_filter) {
    can_be_eliminated = true;
  } else if (semi_info->is_anti_join() && !(is_simple_join_condition && is_simple_filter)) {

  } else if (OB_FAIL(ObTransformUtils::extract_table_exprs(*stmt, candi_conds, left_tables, left_table_conditions))) {
    LOG_WARN("failed to extract table exprs", K(ret));
  } else if (!semi_info->is_anti_join() &&  // anti join do not use right filter
             OB_FAIL(ObTransformUtils::extract_table_exprs(*stmt, candi_conds, right_tables,
                                                           right_table_conditions))) {
    LOG_WARN("failed to extract table exprs", K(ret));
  } else if (OB_FAIL(ObTransformUtils::check_exprs_unique_on_table_items(stmt,
                 ctx_->session_info_,
                 ctx_->schema_checker_,
                 left_tables,
                 source_exprs,
                 left_table_conditions,
                 false,
                 source_unique))) {
    LOG_WARN("check expr unique in semi left tables failed", K(ret));
  } else if (OB_FAIL(ObTransformUtils::check_exprs_unique_on_table_items(stmt,
                 ctx_->session_info_,
                 ctx_->schema_checker_,
                 right_tables,
                 target_exprs,
                 right_table_conditions,
                 false,
                 target_unique))) {
    LOG_WARN("check expr unique in semi right tables failed", K(ret));
  } else if (source_unique && target_unique) {
    can_be_eliminated = true;
    LOG_TRACE("succeed to check loseless semi join", K(source_unique), K(target_unique), K(can_be_eliminated));
  } else {
    can_be_eliminated = false;
    LOG_TRACE("succeed to check loseless semi join", K(source_unique), K(target_unique), K(can_be_eliminated));
  }
  return ret;
}

int ObTransformJoinElimination::check_semi_join_condition(ObDMLStmt* stmt, SemiInfo* semi_info,
    const ObIArray<TableItem*>& source_tables, const ObIArray<TableItem*>& target_tables,
    const ObIArray<ObStmtMapInfo>& stmt_map_infos, const ObIArray<int64_t>& rel_map_info,
    ObIArray<ObRawExpr*>& source_exprs, ObIArray<ObRawExpr*>& target_exprs, bool& is_simple_join_condition,
    bool& target_tables_have_filter, bool& is_simple_filter)
{
  int ret = OB_SUCCESS;
  is_simple_join_condition = true;
  target_tables_have_filter = false;
  is_simple_filter = true;
  ObSqlBitSet<> right_tables;
  if (OB_ISNULL(stmt) || OB_ISNULL(semi_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param has null", K(ret));
  } else if (OB_FAIL(stmt->get_table_rel_ids(target_tables, right_tables))) {
    LOG_WARN("failed to get rel ids", K(ret));
  } else {
    ObRawExpr* expr = NULL;
    ObOpRawExpr* op = NULL;
    const ObIArray<ObRawExpr*>& conditions = semi_info->semi_conditions_;
    for (int64_t i = 0; OB_SUCC(ret) && i < conditions.count(); ++i) {
      if (OB_ISNULL(expr = conditions.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null", K(ret));
      } else if (right_tables.overlap(expr->get_relation_ids())) {
        if (right_tables.is_superset(expr->get_relation_ids())) {
          target_tables_have_filter = true;
          if (T_OP_OR == expr->get_expr_type()) {
            is_simple_filter = false;
          } else { /*do nothing*/
          }
        } else if (T_OP_EQ != expr->get_expr_type()) {
          is_simple_join_condition = false;
        } else if (OB_FALSE_IT(op = static_cast<ObOpRawExpr*>(expr))) {
        } else if (OB_ISNULL(op->get_param_expr(0)) || OB_ISNULL(op->get_param_expr(1))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexcept null param", K(ret));
        } else if (!op->get_param_expr(0)->has_flag(IS_COLUMN) || !op->get_param_expr(1)->has_flag(IS_COLUMN)) {
          is_simple_join_condition = false;
        } else {
          ObColumnRefRawExpr* col1 = static_cast<ObColumnRefRawExpr*>(op->get_param_expr(0));
          ObColumnRefRawExpr* col2 = static_cast<ObColumnRefRawExpr*>(op->get_param_expr(1));
          bool is_equal = false;
          bool is_reverse = false;
          if (OB_FAIL(is_equal_column(
                  source_tables, target_tables, stmt_map_infos, rel_map_info, col1, col2, is_equal, is_reverse))) {
            LOG_WARN("check column ref table id is equal failed", K(ret));
          } else if (!is_equal) {
            is_simple_join_condition = false;
          } else if (is_reverse) {
            if (OB_FAIL(source_exprs.push_back(col2))) {
              LOG_WARN("push back column expr failed", K(ret));
            } else if (OB_FAIL(target_exprs.push_back(col1))) {
              LOG_WARN("push back column expr failed", K(ret));
            } else { /*do nothing*/
            }
          } else {
            if (OB_FAIL(source_exprs.push_back(col1))) {
              LOG_WARN("push back column expr failed", K(ret));
            } else if (OB_FAIL(target_exprs.push_back(col2))) {
              LOG_WARN("push back column expr failed", K(ret));
            } else { /*do nothing*/
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObTransformJoinElimination::is_equal_column(const ObIArray<TableItem*>& source_tables,
    const ObIArray<TableItem*>& target_tables, const ObIArray<ObStmtMapInfo>& stmt_map_infos,
    const ObIArray<int64_t>& rel_map_info, const ObColumnRefRawExpr* source_col, const ObColumnRefRawExpr* target_col,
    bool& is_equal, bool& is_reverse)
{
  int ret = OB_SUCCESS;
  is_equal = false;
  is_reverse = false;
  if (OB_ISNULL(source_col) || OB_ISNULL(target_col)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param has null", K(source_col), K(target_col));
  } else if (stmt_map_infos.count() != source_tables.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt map info is incorrect", K(ret));
  } else if (source_tables.count() != rel_map_info.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("relation map info is incorrect", K(ret));
  } else {
    uint64_t source_table_id = source_col->get_table_id();
    uint64_t target_table_id = target_col->get_table_id();
    TableItem* source_table = NULL;
    TableItem* target_table = NULL;
    bool find = false;
    int64_t pos = 0;
    for (int64_t i = 0; OB_SUCC(ret) && !find && i < source_tables.count(); ++i) {
      if (rel_map_info.at(i) < 0 || rel_map_info.at(i) >= target_tables.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("relation map info is not correct", K(ret), K(rel_map_info.at(i)), K(target_tables.count()));
      } else {
        source_table = source_tables.at(i);
        target_table = target_tables.at(rel_map_info.at(i));
        if (OB_ISNULL(source_table) || OB_ISNULL(target_table)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("table item is null", K(source_table), K(target_table));
        } else if (source_table->table_id_ == source_table_id && target_table->table_id_ == target_table_id) {
          find = true;
          is_reverse = false;
          pos = i;
        } else if (source_table->table_id_ == target_table_id && target_table->table_id_ == source_table_id) {
          find = true;
          is_reverse = true;
          pos = i;
          source_table = target_tables.at(rel_map_info.at(i));
          target_table = source_tables.at(i);
        } else { /*do nothing*/
        }
      }
    }
    if (OB_SUCC(ret) && find &&
        OB_FAIL(is_equal_column(source_table,
            target_table,
            stmt_map_infos.at(pos).select_item_map_,
            source_col->get_column_id(),
            target_col->get_column_id(),
            is_equal))) {
      LOG_WARN("check column is euqal failed", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObTransformJoinElimination::is_equal_column(const TableItem* source_table, const TableItem* target_table,
    const ObIArray<int64_t>& output_map, uint64_t source_col_id, uint64_t target_col_id, bool& is_equal)
{
  int ret = OB_SUCCESS;
  is_equal = false;
  if (OB_ISNULL(source_table) || OB_ISNULL(target_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param has null", K(source_table), K(target_table));
  } else {
    if (target_table->is_basic_table()) {
      is_equal = (source_col_id == target_col_id);
    } else if (target_table->is_generated_table()) {
      int64_t pos = source_col_id - OB_APP_MIN_COLUMN_ID;
      if (OB_UNLIKELY(pos < 0 || pos >= output_map.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(pos), K(output_map.count()), K(ret));
      } else if (OB_INVALID_ID == output_map.at(pos)) {
        /*do nothing*/
      } else {
        is_equal = (target_col_id == output_map.at(pos) + OB_APP_MIN_COLUMN_ID);
      }
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObTransformJoinElimination::do_elimination_semi_join_self_key(ObDMLStmt* stmt, SemiInfo* semi_info,
    const ObIArray<TableItem*>& source_tables, const ObIArray<TableItem*>& target_tables,
    ObIArray<ObStmtMapInfo>& stmt_map_infos, const ObIArray<int64_t>& rel_map_info)
{
  int ret = OB_SUCCESS;
  TableItem* source_table = NULL;
  TableItem* target_table = NULL;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (source_tables.count() != stmt_map_infos.count() || source_tables.count() != rel_map_info.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN(
        "stmt map info is incorrect", K(source_tables.count()), K(stmt_map_infos.count()), K(rel_map_info.count()));
  } else if (OB_FAIL(trans_semi_condition_exprs(stmt, semi_info))) {
    LOG_WARN("transform anti condition failed", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < source_tables.count(); ++i) {
    if (rel_map_info.at(i) < 0 || rel_map_info.at(i) >= target_tables.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("relation map info is not correct", K(ret), K(rel_map_info.at(i)), K(target_tables.count()));
    } else if (OB_ISNULL(target_table = target_tables.at(rel_map_info.at(i))) ||
               OB_ISNULL(source_table = source_tables.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table item is null", K(source_table), K(target_table));
    } else if (OB_FAIL(adjust_table_items(stmt, source_table, target_table, stmt_map_infos.at(i)))) {
      LOG_WARN("adjust table items failed", K(ret));
    } else if (OB_FAIL(trans_semi_table_item(stmt, source_table, target_table))) {
      LOG_WARN("transform semi right table item failed", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObTransformJoinElimination::trans_semi_table_item(
    ObDMLStmt* stmt, const TableItem* source_table, const TableItem* target_table)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt) || OB_ISNULL(source_table) || OB_ISNULL(target_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parameter is null", K(ret), K(stmt), K(source_table), K(target_table));
  } else if (OB_FAIL(stmt->remove_table_item(target_table))) {
    LOG_WARN("failed to remove target table items", K(ret));
  } else if (OB_FAIL(stmt->remove_part_expr_items(target_table->table_id_))) {
    LOG_WARN("failed to remove part expr item", K(ret));
  } else if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("transform context is null", K(ret));
  } else if (OB_FAIL(stmt->rebuild_tables_hash())) {
    LOG_WARN("rebuild table hash failed", K(ret));
  } else if (OB_FAIL(stmt->update_column_item_rel_id())) {
    LOG_WARN("failed to update columns' relation id", K(ret));
  } else if (OB_FAIL(stmt->formalize_stmt(ctx_->session_info_))) {
    LOG_WARN("formalize stmt is failed", K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

int ObTransformJoinElimination::trans_semi_condition_exprs(ObDMLStmt* stmt, SemiInfo* semi_info)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 16> new_lnnvl_conditions;
  ObSEArray<ObRawExpr*, 16> new_not_conditions;
  ObSqlBitSet<> left_rel_ids;
  ObSqlBitSet<> right_rel_ids;
  if (OB_ISNULL(stmt) || OB_ISNULL(semi_info) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param has null", K(stmt), K(ctx_), K(ret));
  } else if (semi_info->is_semi_join()) {
    ret = append(stmt->get_condition_exprs(), semi_info->semi_conditions_);
  } else if (stmt->get_table_rel_ids(semi_info->left_table_ids_, left_rel_ids)) {
    LOG_WARN("failed to get table rel ids", K(ret));
  } else if (stmt->get_table_rel_ids(semi_info->right_table_id_, right_rel_ids)) {
    LOG_WARN("failed to get table rel ids", K(ret));
  } else {
    const int64_t count = semi_info->semi_conditions_.count();
    ObRawExpr* expr = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      if (OB_ISNULL(expr = semi_info->semi_conditions_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null", K(ret));
      } else if (left_rel_ids.is_superset(expr->get_relation_ids())) {
        // const / left tables filter
        ret = new_lnnvl_conditions.push_back(expr);
      } else if (right_rel_ids.is_superset(expr->get_relation_ids())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected right table filter in semi info", K(ret), K(*semi_info));
      } else if (T_OP_EQ == expr->get_expr_type()) {
        ObOpRawExpr* op = static_cast<ObOpRawExpr*>(expr);
        if (OB_ISNULL(op->get_param_expr(0)) || OB_ISNULL(op->get_param_expr(1))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexcept null param", K(ret));
        } else if (op->get_param_expr(0)->has_flag(IS_COLUMN) && op->get_param_expr(1)->has_flag(IS_COLUMN)) {
          ObColumnRefRawExpr* col1 = static_cast<ObColumnRefRawExpr*>(op->get_param_expr(0));
          ObColumnRefRawExpr* col2 = static_cast<ObColumnRefRawExpr*>(op->get_param_expr(1));
          ObColumnRefRawExpr* col = NULL;
          if (right_rel_ids.is_superset(col1->get_relation_ids())) {
            col = col2;
          } else {
            col = col1;
          }
          // transform equal condition on same cols
          ObRawExpr* new_expr = NULL;
          if (OB_FAIL(trans_column_expr(stmt, col, new_expr))) {
            LOG_WARN("make is not null expr failed", K(ret));
          } else if (OB_ISNULL(new_expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexcept null new expr", K(new_expr));
          } else if (OB_FAIL(new_not_conditions.push_back(new_expr))) {
            LOG_WARN("push back new expr into new conditions failed", K(ret));
          } else { /*do nothing*/
          }
        } else { /*do nothing*/
        }
      } else { /*do nothing*/
      }
    }
    ObSEArray<ObRawExpr*, 16> new_conditions;
    for (int64_t i = 0; OB_SUCC(ret) && i < new_not_conditions.count(); ++i) {
      ObRawExpr* not_expr = NULL;
      if (OB_FAIL(ObRawExprUtils::build_not_expr(*(ctx_->expr_factory_), new_not_conditions.at(i), not_expr))) {
        LOG_WARN("make not expr failed", K(ret));
      } else if (OB_ISNULL(not_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("not expr is null", K(ret));
      } else if (OB_FAIL(new_conditions.push_back(not_expr))) {
        LOG_WARN("push back not expr to conditions failed", K(ret));
      } else { /*do nothing*/
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < new_lnnvl_conditions.count(); ++i) {
      ObRawExpr* lnnvl_expr = NULL;
      if (OB_FAIL(ObRawExprUtils::build_lnnvl_expr(*(ctx_->expr_factory_), new_lnnvl_conditions.at(i), lnnvl_expr))) {
        LOG_WARN("make lnnvl expr failed", K(ret));
      } else if (OB_ISNULL(lnnvl_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("lnnvl expr is null", K(ret));
      } else if (OB_FAIL(new_conditions.push_back(lnnvl_expr))) {
        LOG_WARN("push back lnnvl expr to conditions failed", K(ret));
      } else { /*do nothing*/
      }
    }
    if (OB_SUCC(ret) && !new_conditions.empty()) {
      ObRawExpr* or_expr = NULL;
      if (OB_FAIL(ObRawExprUtils::build_or_exprs(*ctx_->expr_factory_, new_conditions, or_expr))) {
        LOG_WARN("make or expr failed", K(ret));
      } else if (OB_ISNULL(or_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("or expr is null", K(ret));
      } else if (OB_FAIL(stmt->get_condition_exprs().push_back(or_expr))) {
        LOG_WARN("failed to push back cond", K(ret));
      } else { /*do nothing*/
      }
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObTransformJoinElimination::check_transform_validity_semi_foreign_key(ObDMLStmt* stmt, SemiInfo* semi_info,
    const ObIArray<ObRawExpr*>& conds, TableItem*& left_table, TableItem*& right_table,
    ObForeignKeyInfo*& foreign_key_info, bool& can_be_eliminated)
{
  int ret = OB_SUCCESS;
  bool is_first_table_parent = false;
  bool right_table_has_filter = false;
  foreign_key_info = NULL;
  can_be_eliminated = false;
  int64_t right_idx = OB_INVALID_INDEX;
  if (OB_ISNULL(stmt) || OB_ISNULL(semi_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param has null", K(ret));
  } else if (1 != semi_info->left_table_ids_.count()) {
    /* do nothing */
  } else if (OB_ISNULL(left_table = stmt->get_table_item_by_id(semi_info->left_table_ids_.at(0))) ||
             OB_ISNULL(right_table = stmt->get_table_item_by_id(semi_info->right_table_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(left_table), K(right_table));
  } else if (!left_table->is_basic_table() || !right_table->is_basic_table()) {
    /* do nothing */
  } else if (OB_FAIL(check_transform_validity_foreign_key(
                 stmt, conds, left_table, right_table, can_be_eliminated, is_first_table_parent, foreign_key_info))) {
    LOG_WARN("check transform validity with foreign key failed", K(ret));
  } else if (is_first_table_parent) {
    can_be_eliminated = false;
  } else if (!can_be_eliminated) {
    /*do nothing*/
  } else if (!semi_info->is_anti_join()) {
    /*do nothing*/
  } else if (OB_FAIL(stmt->get_table_item_idx(right_table, right_idx))) {
    LOG_WARN("failed to get table item idx", K(ret));
  } else {
    const ObIArray<ObRawExpr*>& conditions = semi_info->semi_conditions_;
    ObRawExpr* expr = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && can_be_eliminated && i < conditions.count(); ++i) {
      if (OB_ISNULL(expr = conditions.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null", K(ret));
      } else if (1 == expr->get_relation_ids().bit_count() && expr->get_relation_ids().has_member(right_idx)) {
        can_be_eliminated = false;
      }
    }
  }
  return ret;
}

int ObTransformJoinElimination::EliminationHelper::push_back(
    TableItem* child, TableItem* parent, ObForeignKeyInfo* info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(child_table_items_.push_back(child))) {
    LOG_WARN("failed to push back table item", K(ret));
  } else if (OB_FAIL(parent_table_items_.push_back(parent))) {
    LOG_WARN("failed to push back table item", K(ret));
  } else if (OB_FAIL(foreign_key_infos_.push_back(info))) {
    LOG_WARN("failed to push back foreign key info", K(ret));
  } else if (OB_FAIL(bitmap_.push_back(false))) {
    LOG_WARN("failed to push back bool", K(ret));
  }
  ++count_;
  ++remain_;
  return ret;
}

int ObTransformJoinElimination::EliminationHelper::get_eliminable_group(
    TableItem*& child, TableItem*& parent, ObForeignKeyInfo*& info, bool& find)
{
  int ret = OB_SUCCESS;
  bool in_child = false;
  find = false;
  if (OB_LIKELY(remain_ > 0)) {
    for (int64_t i = 0; OB_SUCC(ret) && !find && i < count_; ++i) {
      if (bitmap_.at(i)) {
        /* table already eliminated */
      } else if (OB_FAIL(is_table_in_child_items(parent_table_items_.at(i), in_child))) {
        LOG_WARN("failed to find table item", K(ret));
      } else if (in_child) {
        /* do nothing */
      } else {
        child = child_table_items_.at(i);
        parent = parent_table_items_.at(i);
        info = foreign_key_infos_.at(i);
        bitmap_.at(i) = true;
        --remain_;
        find = true;
      }
    }
  }
  return ret;
}

int ObTransformJoinElimination::extract_candi_table(ObDMLStmt* stmt, const ObIArray<FromItem>& from_items,
    ObIArray<TableItem*>& candi_tables, ObIArray<TableItem*>& child_candi_tables)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(stmt), K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < from_items.count(); ++i) {
    const FromItem& from_item = from_items.at(i);
    TableItem* table_item = NULL;
    if (from_item.is_joined_) {
      ret = extract_candi_table(stmt->get_joined_table(from_item.table_id_), child_candi_tables);
    } else if (OB_ISNULL(table_item = stmt->get_table_item_by_id(from_item.table_id_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid table id", K(ret), K(from_item.table_id_));
    } else if (table_item->is_basic_table() || table_item->is_generated_table()) {
      ret = candi_tables.push_back(table_item);
    }
  }
  return ret;
}

int ObTransformJoinElimination::extract_candi_table(JoinedTable* table, ObIArray<TableItem*>& child_candi_tables)
{
  int ret = OB_SUCCESS;
  TableItem* left_table = NULL;
  TableItem* right_table = NULL;
  if (OB_ISNULL(table) || OB_ISNULL(left_table = table->left_table_) || OB_ISNULL(right_table = table->right_table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (table->is_inner_join() || IS_OUTER_JOIN(table->joined_type_)) {
    if (left_table->is_joined_table()) {
      ret = SMART_CALL(extract_candi_table(static_cast<JoinedTable*>(left_table), child_candi_tables));
    } else if (left_table->is_basic_table() || left_table->is_generated_table()) {
      ret = child_candi_tables.push_back(left_table);
    }
    if (OB_FAIL(ret)) {
    } else if (right_table->is_joined_table()) {
      ret = SMART_CALL(extract_candi_table(static_cast<JoinedTable*>(right_table), child_candi_tables));
    } else if (right_table->is_basic_table() || right_table->is_generated_table()) {
      ret = child_candi_tables.push_back(right_table);
    }
  }
  return ret;
}

int ObTransformJoinElimination::classify_joined_table(JoinedTable* joined_table,
    ObIArray<JoinedTable*>& inner_join_tables, ObIArray<TableItem*>& outer_join_tables,
    ObIArray<TableItem*>& other_tables, ObIArray<ObRawExpr*>& inner_join_conds)
{
  int ret = OB_SUCCESS;
  TableItem* left_table = NULL;
  TableItem* right_table = NULL;
  ObSEArray<JoinedTable*, 8> tmp_joined_tables;
  JoinedTable* cur_joined_table = NULL;
  if (OB_FAIL(tmp_joined_tables.push_back(joined_table))) {
    LOG_WARN("failed to push back exprs", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < tmp_joined_tables.count(); i++) {
    if (OB_ISNULL(cur_joined_table = tmp_joined_tables.at(i)) ||
        OB_ISNULL(left_table = cur_joined_table->left_table_) ||
        OB_ISNULL(right_table = cur_joined_table->right_table_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret), K(cur_joined_table), K(left_table), K(right_table));
    } else if (IS_OUTER_JOIN(cur_joined_table->joined_type_)) {
      ret = outer_join_tables.push_back(cur_joined_table);
    } else if (!cur_joined_table->is_inner_join()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected joined table type", K(ret), K(*cur_joined_table));
    } else if (OB_FAIL(append(inner_join_conds, cur_joined_table->get_join_conditions()))) {
      LOG_WARN("failed to push back exprs", K(ret));
    } else if (cur_joined_table != joined_table && OB_FAIL(inner_join_tables.push_back(cur_joined_table))) {
      LOG_WARN("failed to push back table", K(ret));
    } else {  // inner join
      JoinedTable* left_joined_table = NULL;
      JoinedTable* right_joined_table = NULL;
      if (left_table->is_joined_table()) {
        left_joined_table = static_cast<JoinedTable*>(left_table);
        ret = tmp_joined_tables.push_back(left_joined_table);
      } else {
        ret = other_tables.push_back(left_table);
      }

      if (OB_FAIL(ret)) {
      } else if (right_table->is_joined_table()) {
        right_joined_table = static_cast<JoinedTable*>(right_table);
        ret = tmp_joined_tables.push_back(right_joined_table);
      } else {
        ret = other_tables.push_back(right_table);
      }
    }
  }
  return ret;
}

int ObTransformJoinElimination::eliminate_join_in_joined_table(ObDMLStmt* stmt, TableItem*& table_item,
    ObIArray<TableItem*>& child_candi_tables, ObIArray<ObRawExpr*>& trans_conditions, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  JoinedTable* joined_table = NULL;
  TableItem* left_table = NULL;
  TableItem* right_table = NULL;
  if (OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(table_item));
  } else if (!table_item->is_joined_table()) {
    if (!table_item->is_basic_table() && !table_item->is_generated_table()) {
      /*do nothing*/
    } else if (OB_FAIL(child_candi_tables.push_back(table_item))) {
      LOG_WARN("failed to push back table", K(ret));
    }
  } else if (OB_ISNULL(joined_table = static_cast<JoinedTable*>(table_item)) ||
             OB_ISNULL(left_table = joined_table->left_table_) || OB_ISNULL(right_table = joined_table->right_table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(joined_table), K(left_table), K(right_table));
  } else if (joined_table->is_full_join()) {
    ret = extract_candi_table(joined_table, child_candi_tables);
  } else if (IS_OUTER_JOIN(joined_table->joined_type_)) {
    bool left_happened = false;
    bool right_happened = false;
    ObSEArray<TableItem*, 4> left_child_candi_tables;
    ObSEArray<ObRawExpr*, 16> left_trans_conditions;
    ObSEArray<TableItem*, 4> right_child_candi_tables;
    ObSEArray<ObRawExpr*, 16> right_trans_conditions;
    if (OB_FAIL(SMART_CALL(eliminate_join_in_joined_table(
            stmt, joined_table->right_table_, right_child_candi_tables, right_trans_conditions, right_happened)))) {
      LOG_WARN("failed to eliminate join in joined table`s right table.", K(ret));
    } else if (OB_FAIL(SMART_CALL(eliminate_join_in_joined_table(
                   stmt, joined_table->left_table_, left_child_candi_tables, left_trans_conditions, left_happened)))) {
      LOG_WARN("failed to eliminate join in joined table`s left table.", K(ret));
    } else if (OB_FAIL(ObTransformUtils::adjust_single_table_ids(joined_table))) {
      LOG_WARN("failed to construct single table ids.", K(ret));
    } else if (OB_FAIL(append(child_candi_tables, left_child_candi_tables)) ||
               OB_FAIL(append(child_candi_tables, right_child_candi_tables))) {
      LOG_WARN("failed to append child candi tables", K(ret));
    } else if (joined_table->is_left_join() &&
               (OB_FAIL(append(trans_conditions, left_trans_conditions)) ||
                   OB_FAIL(append(joined_table->join_conditions_, right_trans_conditions)))) {
      LOG_WARN("failed to append trans conditions to left join", K(ret));
    } else if (joined_table->is_right_join() &&
               (OB_FAIL(append(joined_table->join_conditions_, left_trans_conditions)) ||
                   OB_FAIL(append(trans_conditions, right_trans_conditions)))) {
      LOG_WARN("failed to append trans conditions to right join", K(ret));
    } else if (left_happened || right_happened) {
      trans_happened = true;
    }
  } else if (!joined_table->is_inner_join()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected joined type", K(ret), K(joined_table));
  } else {
    ObSEArray<JoinedTable*, 4> inner_join_tables;
    ObSEArray<TableItem*, 4> outer_join_tables;
    ObSEArray<TableItem*, 4> other_tables;  // not joined table
    ObSEArray<ObRawExpr*, 8> inner_join_conds;
    ObSEArray<TableItem*, 4> tmp_child_candi_tables;
    ObSEArray<ObRawExpr*, 4> tmp_trans_conditions;
    bool is_happened = false;
    if (OB_FAIL(classify_joined_table(
            joined_table, inner_join_tables, outer_join_tables, other_tables, inner_join_conds))) {
      LOG_WARN("failed to classify joined table", K(ret), K(*joined_table));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < outer_join_tables.count(); i++) {
      tmp_child_candi_tables.reuse();
      tmp_trans_conditions.reuse();
      if (OB_FAIL(SMART_CALL(eliminate_join_in_joined_table(
              stmt, outer_join_tables.at(i), tmp_child_candi_tables, tmp_trans_conditions, is_happened)))) {
        LOG_WARN("failed to eliminate join in inner join", K(ret));
      } else if (OB_FAIL(append(child_candi_tables, tmp_child_candi_tables))) {
        LOG_WARN("failed to append child candi tables", K(ret));
      } else if (OB_FAIL(append(inner_join_conds, tmp_trans_conditions))) {
        LOG_WARN("failed to append inner join conds", K(ret));
      } else {
        trans_happened |= is_happened;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(eliminate_candi_tables(stmt, inner_join_conds, other_tables, child_candi_tables, is_happened))) {
      LOG_WARN("failed to eliminate candi tables", K(ret));
    } else if (OB_FAIL(append(outer_join_tables, other_tables))) {
      LOG_WARN("failed to append tables", K(ret));
    } else if (OB_FAIL(
                   rebuild_joined_tables(stmt, table_item, inner_join_tables, outer_join_tables, inner_join_conds))) {
      LOG_WARN("failed to rebuild joined tables", K(ret));
    } else if (OB_FALSE_IT(trans_happened |= is_happened)) {
    } else if (!table_item->is_joined_table() || !static_cast<JoinedTable*>(table_item)->is_inner_join()) {
      ret = trans_conditions.assign(inner_join_conds);
    } else if (!inner_join_conds.empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("join conditions not empty after rebuild joined table", K(ret), K(*table_item));
    }
  }
  return ret;
}

int ObTransformJoinElimination::eliminate_candi_tables(ObDMLStmt* stmt, ObIArray<ObRawExpr*>& conds,
    ObIArray<TableItem*>& candi_tables, ObIArray<TableItem*>& child_candi_tables, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  EqualSets* equal_sets = &ctx_->equal_sets_;
  ObArenaAllocator allocator;
  bool is_valid = true;
  if (conds.empty()) {
    /*do nothing*/
  } else if (OB_FAIL(ObEqualAnalysis::compute_equal_set(&allocator, conds, *equal_sets))) {
    LOG_WARN("failed to compute equal set", K(ret));
  } else {
    ObSqlBitSet<> removed_items;
    for (int64_t i = 0; OB_SUCC(ret) && i < candi_tables.count(); ++i) {
      if (removed_items.has_member(i)) {
        /*do nothing*/
      } else {
        bool is_happened = false;
        for (int64_t j = 0; OB_SUCC(ret) && j < candi_tables.count(); ++j) {
          if (i == j || removed_items.has_member(j)) {
            /*do nothing*/
          } else if (OB_FAIL(check_table_can_be_eliminated(stmt, candi_tables.at(j)->table_id_, is_valid))) {
            LOG_WARN("check table can be eliminated failed", K(ret));
          } else if (!is_valid) {
            // do nothing
          } else if (OB_FAIL(do_join_elimination_self_key(
                         stmt, candi_tables.at(i), candi_tables.at(j), is_happened, equal_sets))) {
            LOG_WARN("failed to eliminate self key join in base table", K(ret));
          } else if (!is_happened) {
            /*do nothing*/
          } else if (OB_FAIL(removed_items.add_member(j))) {
            LOG_WARN("failed to add removed items", K(ret));
          } else {
            trans_happened = true;
          }
        }
        is_happened = false;
        for (int64_t j = 0; OB_SUCC(ret) && !is_happened && j < child_candi_tables.count(); ++j) {
          is_valid = true;
          if (OB_FAIL(check_table_can_be_eliminated(stmt, candi_tables.at(i)->table_id_, is_valid))) {
            LOG_WARN("check table can be eliminated failed", K(ret));
          } else if (!is_valid) {
            // do nothing
          } else if (OB_FAIL(do_join_elimination_self_key(
                         stmt, child_candi_tables.at(j), candi_tables.at(i), is_happened, equal_sets))) {
            LOG_WARN("failed to do join elimination erlf key", K(ret));
          } else if (!is_happened) {
            /*do nothing*/
          } else if (OB_FAIL(removed_items.add_member(i))) {
            LOG_WARN("failed to add removed items", K(ret));
          } else {
            trans_happened = true;
          }
        }
      }
    }

    ObSEArray<TableItem*, 16> baisc_candi_tables;
    ObSEArray<TableItem*, 16> removed_tables;
    for (int64_t i = 0; OB_SUCC(ret) && i < candi_tables.count(); ++i) {
      if (removed_items.has_member(i)) {
        ret = removed_tables.push_back(candi_tables.at(i));
      } else if (candi_tables.at(i)->is_basic_table() && OB_FAIL(baisc_candi_tables.push_back(candi_tables.at(i)))) {
        LOG_WARN("failed to push back table", K(ret));
      }
    }

    equal_sets->reuse();
    EliminationHelper helper;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(get_eliminable_tables(stmt, conds, baisc_candi_tables, child_candi_tables, helper))) {
      LOG_WARN("failed to get eliminable tables", K(ret));
    } else {
      bool find = false;
      TableItem* child = NULL;
      TableItem* parent = NULL;
      ObForeignKeyInfo* info = NULL;
      while (OB_SUCC(ret) && helper.get_remain() > 0) {
        if (OB_FAIL(helper.get_eliminable_group(child, parent, info, find))) {
          LOG_WARN("failed to get eliminable_group", K(ret));
        } else if (!find) {
          LOG_WARN("can not find one eliminable group, circle elimination may exists");
          break;
        } else if (OB_FAIL(do_join_elimination_foreign_key(stmt, child, parent, info))) {
          LOG_WARN("failed to eliminate useless join", K(ret));
        } else if (OB_FAIL(removed_tables.push_back(parent))) {
          LOG_WARN("failed to push back table", K(ret));
        } else {
          trans_happened = true;
        }
      }
    }

    if (OB_SUCC(ret) && !removed_tables.empty() &&
        OB_FAIL(ObOptimizerUtil::remove_item(candi_tables, removed_tables))) {
      LOG_WARN("failed to remove item", K(ret));
    }
  }
  return ret;
}

int ObTransformJoinElimination::rebuild_joined_tables(ObDMLStmt* stmt, TableItem*& top_table,
    ObIArray<JoinedTable*>& inner_join_tables, ObIArray<TableItem*>& tables, ObIArray<ObRawExpr*>& join_conds)
{
  int ret = OB_SUCCESS;
  ObSqlBitSet<> table_set;
  if (tables.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("empty tables", K(ret), K(tables));
  } else if (1 == tables.count()) {
    top_table = tables.at(0);
  } else if (OB_FAIL(stmt->get_table_rel_ids(*tables.at(0), table_set))) {
    LOG_WARN("failed to get table rel ids", K(ret));
  } else {
    TableItem* left_table = NULL;
    TableItem* right_table = NULL;
    JoinedTable* tmp_joined_table = NULL;
    ObSEArray<ObRawExpr*, 8> cur_join_conds;
    TableItem* cur_table = tables.at(0);
    for (int64_t i = 1; OB_SUCC(ret) && i < tables.count(); i++) {
      cur_join_conds.reuse();
      if (OB_ISNULL(left_table = tables.at(i)) || OB_ISNULL(right_table = cur_table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret), K(left_table), K(right_table));
      } else if (OB_FAIL(stmt->get_table_rel_ids(*left_table, table_set))) {
        LOG_WARN("failed to get table rel ids", K(ret));
      } else if (OB_FAIL(ObTransformUtils::extract_table_exprs(*stmt, join_conds, table_set, cur_join_conds))) {
        LOG_WARN("failed to extract table exprs", K(ret));
      } else if (OB_FAIL(ObOptimizerUtil::remove_item(join_conds, cur_join_conds))) {
        LOG_WARN("failed to remove cur cond exprs", K(ret));
      } else if (i > inner_join_tables.count()) {
        ret = ObTransformUtils::add_new_joined_table(
            ctx_, *stmt, INNER_JOIN, left_table, right_table, cur_join_conds, cur_table, false);
      } else if (OB_ISNULL(tmp_joined_table = inner_join_tables.at(i - 1))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret), K(inner_join_tables.at(i - 1)));
      } else if (OB_FAIL(tmp_joined_table->get_join_conditions().assign(cur_join_conds))) {
        LOG_WARN("failed to assign exprs", K(ret));
      } else {
        tmp_joined_table->left_table_ = left_table;
        tmp_joined_table->right_table_ = right_table;
        tmp_joined_table->joined_type_ = INNER_JOIN;
        tmp_joined_table->type_ = TableItem::JOINED_TABLE;
        cur_table = tmp_joined_table;
        ret = ObTransformUtils::adjust_single_table_ids(tmp_joined_table);
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(append(static_cast<JoinedTable*>(cur_table)->get_join_conditions(), join_conds))) {
      LOG_WARN("failed to append exprs", K(ret));
    } else {
      join_conds.reuse();
      top_table = cur_table;
    }
  }
  return ret;
}

int ObTransformJoinElimination::trans_self_equal_conds(ObDMLStmt* stmt)
{
  int ret = OB_SUCCESS;
  ObSEArray<JoinedTable*, 16> joined_tables;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("input paramter is null", K(stmt));
  } else if (OB_FAIL(trans_self_equal_conds(stmt, stmt->get_condition_exprs()))) {
    LOG_WARN("failed to trans condition exprs", K(ret));
  } else if (OB_FAIL(joined_tables.assign(stmt->get_joined_tables()))) {
    LOG_WARN("failed to assign joined tables", K(ret));
  } else {
    JoinedTable* joined_table = NULL;
    TableItem* table = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < joined_tables.count(); ++i) {
      if (OB_ISNULL(joined_table = joined_tables.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret));
      } else if (OB_FAIL(trans_self_equal_conds(stmt, joined_table->get_join_conditions()))) {
        LOG_WARN("failed to trans self equal conds", K(ret));
      } else if (OB_NOT_NULL(table = joined_table->left_table_) && table->is_joined_table() &&
                 OB_FAIL(joined_tables.push_back(static_cast<JoinedTable*>(table)))) {
        LOG_WARN("failed to push back", K(ret));
      } else if (OB_NOT_NULL(table = joined_table->right_table_) && table->is_joined_table() &&
                 OB_FAIL(joined_tables.push_back(static_cast<JoinedTable*>(table)))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
    ObIArray<SemiInfo*>& semi_infos = stmt->get_semi_infos();
    for (int64_t i = 0; OB_SUCC(ret) && i < semi_infos.count(); ++i) {
      if (OB_ISNULL(semi_infos.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret));
      } else if (OB_FAIL(trans_self_equal_conds(stmt, semi_infos.at(i)->semi_conditions_))) {
        LOG_WARN("failed to trans self equal conds", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformJoinElimination::trans_self_equal_conds(ObDMLStmt* stmt, ObIArray<ObRawExpr*>& cond_exprs)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 16> new_cond_exprs;
  ObSEArray<ObColumnRefRawExpr*, 16> removed_col_exprs;
  ObRawExpr* expr = NULL;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("input paramter is null", K(ret), K(stmt));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < cond_exprs.count(); ++i) {
    if (OB_ISNULL(expr = cond_exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("condition expr is null", K(ret));
    } else if (T_OP_EQ == expr->get_expr_type()) {
      ObOpRawExpr* op = static_cast<ObOpRawExpr*>(expr);
      if (op->get_param_expr(0)->has_flag(IS_COLUMN) && op->get_param_expr(0) == op->get_param_expr(1)) {
        // equal condition on the same column
        if (OB_FAIL(removed_col_exprs.push_back(static_cast<ObColumnRefRawExpr*>(op->get_param_expr(0))))) {
          LOG_WARN("failed to push back col expr");
        }
      } else if (OB_FAIL(new_cond_exprs.push_back(expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      }
    } else if (OB_FAIL(new_cond_exprs.push_back(expr))) {
      LOG_WARN("failed to push back expr", K(ret));
    } else if (T_OP_OR == expr->get_expr_type()) {
      if (OB_FAIL(recursive_trans_equal_join_condition(stmt, expr))) {
        LOG_WARN("recursive trans equal join condition failed", K(ret));
      } else if (OB_FAIL(expr->formalize(ctx_->session_info_))) {
        LOG_WARN("formlize expr failed", K(ret));
      } else {
        /*do nothing*/
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(cond_exprs.assign(new_cond_exprs))) {  // assign cond exprs first
    LOG_WARN("failed to assign exprs", K(ret));
  } else if (OB_FAIL(add_is_not_null_if_needed(stmt, removed_col_exprs, cond_exprs))) {
    LOG_WARN("failed to add is not null condition", K(ret));
  }
  return ret;
}

int ObTransformJoinElimination::add_is_not_null_if_needed(
    ObDMLStmt* stmt, ObIArray<ObColumnRefRawExpr*>& col_exprs, ObIArray<ObRawExpr*>& cond_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < col_exprs.count(); ++i) {
    bool is_nullable = false;
    ObOpRawExpr* is_not_expr = NULL;
    if (OB_ISNULL(col_exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(ObTransformUtils::check_expr_nullable(stmt, col_exprs.at(i), is_nullable))) {
      LOG_WARN("failed to check whether expr is nullable", K(ret));
    } else if (!is_nullable) {
      /*do nothing*/
    } else if (OB_FAIL(ObTransformUtils::add_is_not_null(ctx_, stmt, col_exprs.at(i), is_not_expr))) {
      LOG_WARN("failed to add is not null for col", K(ret));
    } else if (OB_FAIL(cond_exprs.push_back(is_not_expr))) {
      LOG_WARN("failed to add is_not_expr into condition", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObTransformJoinElimination::recursive_trans_equal_join_condition(ObDMLStmt* stmt, ObRawExpr* expr)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param has null when trans equal join condition", K(ret));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_stack_overflow));
  } else if (T_OP_OR == expr->get_expr_type() || T_OP_AND == expr->get_expr_type()) {
    ObOpRawExpr* op = static_cast<ObOpRawExpr*>(expr);
    for (int64_t i = 0; OB_SUCC(ret) && i < op->get_param_count(); ++i) {
      if (OB_ISNULL(op->get_param_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("op expr has null param", K(ret));
      } else if (op->get_param_expr(i)->has_flag(CNT_COLUMN)) {
        bool has_trans = false;
        ObRawExpr* new_expr = NULL;
        if (OB_FAIL(do_trans_equal_join_condition(stmt, op->get_param_expr(i), has_trans, new_expr))) {
          LOG_WARN("do trans equal join condition failed", K(ret));
        } else if (has_trans && !(OB_ISNULL(new_expr))) {
          if (OB_FAIL(op->replace_param_expr(i, new_expr))) {
            LOG_WARN("replace param expr failed", K(ret), K(op), K(i));
          } else {
            LOG_TRACE("trans param expr succ", K(op), K(i));
          }
        } else if (OB_FAIL(SMART_CALL(recursive_trans_equal_join_condition(stmt, op->get_param_expr(i))))) {
          LOG_WARN("recursive trans equal join condition failed", K(ret));
        } else { /*do nothing*/
        }
      } else { /*do nothing*/
      }
    }
  } else { /*do nothing*/
  }
  return ret;
}

int ObTransformJoinElimination::do_trans_equal_join_condition(
    ObDMLStmt* stmt, ObRawExpr* expr, bool& has_trans, ObRawExpr*& new_expr)
{
  int ret = OB_SUCCESS;
  has_trans = false;
  new_expr = NULL;
  if (OB_ISNULL(stmt) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param has null when do trans equal join condition", K(ret));
  } else if (T_OP_EQ == expr->get_expr_type()) {
    ObOpRawExpr* op = static_cast<ObOpRawExpr*>(expr);
    if (OB_ISNULL(op->get_param_expr(0)) || OB_ISNULL(op->get_param_expr(1))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("op expr has null param", K(ret));
    } else if (op->get_param_expr(0)->has_flag(IS_COLUMN) && op->get_param_expr(1)->has_flag(IS_COLUMN) &&
               (op->get_param_expr(0) == op->get_param_expr(1) ||
                   op->get_param_expr(0)->same_as(*(op->get_param_expr(1))))) {
      if (OB_FAIL(trans_column_expr(stmt, op->get_param_expr(0), new_expr))) {
        LOG_WARN("make is not null expr failed", K(ret));
      } else {
        has_trans = true;
      }
    } else { /*do nothing*/
    }
  } else { /*do nothing*/
  }
  return ret;
}

int ObTransformJoinElimination::trans_column_expr(ObDMLStmt* stmt, ObRawExpr* expr, ObRawExpr*& new_expr)
{
  int ret = OB_SUCCESS;
  new_expr = NULL;
  bool is_nullable = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(expr) || OB_ISNULL(ctx_)) {
    LOG_WARN("param has null", K(ret), K(stmt), K(expr), K(ctx_));
  } else if (OB_FAIL(ObTransformUtils::check_expr_nullable(stmt, expr, is_nullable, ObTransformUtils::NULLABLE_SCOPE::NS_FROM))) {
    LOG_WARN("failed to check whether expr is nullable", K(ret));
  } else if (!is_nullable) {
    if (OB_FAIL(ObRawExprUtils::build_const_bool_expr(ctx_->expr_factory_, new_expr, true))) {
      LOG_WARN("build true expr failed", K(ret));
    } else { /*do nothing*/
    }
  } else {
    if (OB_FAIL(ObRawExprUtils::build_is_not_null_expr(*ctx_->expr_factory_, expr, new_expr))) {
      LOG_WARN("build is not null expr failed", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
