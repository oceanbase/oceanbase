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
#include "sql/rewrite/ob_transform_utils.h"
# include "sql/resolver/dml/ob_merge_stmt.h"
namespace oceanbase
{
using namespace common;
using namespace share::schema;
namespace sql
{

int ObTransformJoinElimination::transform_one_stmt(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                                   ObDMLStmt *&stmt,
                                                   bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  bool trans_happened_self_foreign_key = false;
  bool trans_happened_outer_join = false;
  bool trans_happened_left_outer_join = false;
  bool trans_happened_self_foreign_semi_join = false;
  ObSEArray<ObSEArray<TableItem *, 4>, 4> eliminated_tables;
  if (OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt is null", K(ret));
  } else if (OB_FAIL(eliminate_join_self_foreign_key(stmt, trans_happened_self_foreign_key, eliminated_tables))) {
    LOG_WARN("failed to eliminate self join/join between primary key and foreign key", K(ret));
  } else if (OB_FAIL(eliminate_outer_join(parent_stmts, stmt, trans_happened_outer_join, eliminated_tables))) {
    LOG_WARN("failed to eliminate outer join if need", K(ret));
  } else if (OB_FAIL(eliminate_left_outer_join(stmt, trans_happened_left_outer_join, eliminated_tables))) {
    LOG_WARN("failed to eliminate left outer join if join condition is always false", K(ret));
  } else if (OB_FAIL(eliminate_semi_join_self_foreign_key(stmt, trans_happened_self_foreign_semi_join, eliminated_tables))) {
    LOG_WARN("failed to eliminated self semi join", K(ret));
  } else if (!trans_happened_self_foreign_key && !trans_happened_outer_join &&
             !trans_happened_self_foreign_semi_join && !trans_happened_left_outer_join) {
    /* do nothing*/
  } else if (OB_FAIL(trans_self_equal_conds(stmt))) {
    LOG_WARN("failed to trans condition exprs", K(ret));
  } else {
    trans_happened = true;
    ret = add_transform_hint(*stmt, &eliminated_tables);
    LOG_TRACE("succeed to do join elimination", K(ret), K(trans_happened_self_foreign_key), K(trans_happened_left_outer_join),
                          K(trans_happened_outer_join), K(trans_happened_self_foreign_semi_join));
  }
  return ret;
}

int ObTransformJoinElimination::construct_transform_hint(ObDMLStmt &stmt, void *trans_params)
{
  int ret = OB_SUCCESS;
  typedef ObSEArray<TableItem *, 4> single_or_joined_table;
  ObIArray<single_or_joined_table> *eliminated_tables = NULL;
  ObEliminateJoinHint *hint = NULL;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->allocator_) || OB_ISNULL(trans_params) ||
      OB_FALSE_IT(eliminated_tables = static_cast<ObIArray<single_or_joined_table>*>(trans_params)) ||
      OB_UNLIKELY(eliminated_tables->empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(ctx_), K(eliminated_tables));
  } else if (OB_FAIL(ObQueryHint::create_hint(ctx_->allocator_, T_ELIMINATE_JOIN, hint))) {
    LOG_WARN("failed to create hint", K(ret));
  } else if (OB_FAIL(ctx_->outline_trans_hints_.push_back(hint))) {
    LOG_WARN("failed to push back hint", K(ret));
  } else if (OB_FAIL(ctx_->add_used_trans_hint(get_hint(stmt.get_stmt_hint())))) {
    LOG_WARN("failed to add used trans hint", K(ret));
  } else {
    hint->set_qb_name(ctx_->src_qb_name_);
    for (int64_t i = 0; OB_SUCC(ret) && i < eliminated_tables->count(); ++i) {
      ObSEArray<ObTableInHint, 4> single_or_joined_hint_table;
      if (OB_FAIL(ObTransformUtils::get_sorted_table_hint(eliminated_tables->at(i),
                                                          single_or_joined_hint_table))) {
        LOG_WARN("failed to get table hint", K(ret));
      } else if (OB_FAIL(hint->get_tb_name_list().push_back(single_or_joined_hint_table))) {
        LOG_WARN("failed to push back table name list", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformJoinElimination::eliminate_join_self_foreign_key(ObDMLStmt *stmt,
                                                                bool &trans_happened,
                                                                ObIArray<ObSEArray<TableItem *, 4>> &trans_tables)
{
  int ret = OB_SUCCESS;
  bool from_happedend = false;
  bool joined_happedend = false;
  trans_happened = false;
  if (OB_ISNULL(stmt) ) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(stmt), K(ctx_), K(ret));
  } else if (OB_FAIL(eliminate_join_in_from_base_table(stmt, from_happedend, trans_tables))) {
    LOG_WARN("eliminate join in from base table failed", K(ret));
  } else if (OB_FAIL(eliminate_join_in_joined_table(stmt, joined_happedend, trans_tables))) {
    LOG_WARN("eliminate join in joined table failed", K(ret));
  } else {
    trans_happened = from_happedend | joined_happedend;
    LOG_TRACE("succ to do self foreign key elimination", K(from_happedend), K(joined_happedend));
  }
  return ret;
}

int ObTransformJoinElimination::eliminate_join_in_from_base_table(ObDMLStmt *stmt,
                                                                  bool &trans_happened,
                                                                  ObIArray<ObSEArray<TableItem *, 4>> &trans_tables)
{
  int ret = OB_SUCCESS;
  ObSEArray<TableItem *, 16> child_candi_tables;
  ObSEArray<TableItem *, 16> candi_tables;
  ObSEArray<ObRawExpr*, 16> conds;
  trans_happened = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(stmt), K(ctx_), K(ret));
  } else if (stmt->get_equal_set_conditions(conds, true)) {
    LOG_WARN("failed to get equal set conditions", K(ret));
  } else if (OB_FAIL(extract_candi_table(stmt, stmt->get_from_items(), candi_tables,
                                         child_candi_tables))) {
      LOG_WARN("failed to extract candi tables", K(ret));
  } else if (OB_FAIL(eliminate_candi_tables(stmt, conds, candi_tables, child_candi_tables,
                                            true, trans_happened, trans_tables))) {
    LOG_WARN("failed to eliminate candi tables", K(ret));
  }
  return ret;
}

int ObTransformJoinElimination::do_join_elimination_self_key(ObDMLStmt *stmt,
                                                             TableItem *source_table,
                                                             TableItem *target_table,
                                                             bool is_from_base_table,
                                                             bool &trans_happened,
                                                             ObIArray<ObSEArray<TableItem *, 4>> &trans_tables,
                                                             EqualSets *equal_sets /*= NULL*/)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  ObStmtMapInfo stmt_map_info;
  bool can_be_eliminated = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_) || OB_ISNULL(source_table) || OB_ISNULL(target_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(stmt), K(ctx_), K(ret));
  } else if (OB_FAIL(check_eliminate_join_self_key_valid(stmt,
                                                         source_table,
                                                         target_table,
                                                         stmt_map_info,
                                                         is_from_base_table,
                                                         can_be_eliminated,
                                                         equal_sets))) {
    LOG_WARN("failed to check eliminate valid", K(ret));
  } else if (!can_be_eliminated) {
    /*do nothing*/
  } else if (OB_FAIL(construct_eliminated_table(stmt, target_table, trans_tables))) {
    LOG_WARN("failed to construct eliminated tables", K(ret));
  } else if (OB_FAIL(adjust_table_items(stmt, source_table, target_table, stmt_map_info))) {
    LOG_WARN("failed to adjust table items", K(ret));
  } else if (OB_FAIL(trans_table_item(stmt, source_table, target_table))) {
    LOG_WARN("failed to transform target into source", K(ret));
  } else if (OB_FAIL(append(ctx_->equal_param_constraints_, stmt_map_info.equal_param_map_))) {
    LOG_WARN("failed to append equal params constraints", K(ret));
  } else {
    trans_happened = true;
    LOG_TRACE("do join elimination for", K(*source_table), K(*target_table));
  }
  return ret;
}

int ObTransformJoinElimination::check_eliminate_join_self_key_valid(ObDMLStmt *stmt,
                                                                    TableItem *source_table,
                                                                    TableItem *target_table,
                                                                    ObStmtMapInfo &stmt_map_info,
                                                                    bool is_from_base_table,
                                                                    bool &is_valid,
                                                                    EqualSets *equal_sets)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  bool tmp_valid = false;
  bool is_on_null_side = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_) || OB_ISNULL(source_table) || OB_ISNULL(target_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(stmt), K(ctx_), K(ret), K(source_table), K(target_table));
  } else if (OB_FAIL(check_hint_valid(*stmt, *target_table, tmp_valid))) {
    LOG_WARN("failed to check hint valid", K(ret));
  } else if (!tmp_valid) {
    /*do nothing*/
    OPT_TRACE("hint disable transform");
  } else if ((stmt->is_delete_stmt() || stmt->is_update_stmt()) &&
             OB_FAIL(check_eliminate_delupd_table_valid(static_cast<ObDelUpdStmt *> (stmt),
                                                        target_table->table_id_,
                                                        tmp_valid))) {
    LOG_WARN("failed to check table can be eliminated", K(ret));
  } else if (!tmp_valid) {
    /*do nothing*/
    OPT_TRACE("dml target table can not be eliminated");
  } else if (OB_FAIL(check_on_null_side(stmt,
                                        source_table->table_id_,
                                        target_table->table_id_,
                                        is_from_base_table,
                                        is_on_null_side))) {
    LOG_WARN("failed to check table on null side", K(ret));
  } else if (OB_FAIL(ObTransformUtils::check_loseless_join(stmt, ctx_,
                                                           source_table,
                                                           target_table,
                                                           ctx_->session_info_,
                                                           ctx_->schema_checker_,
                                                           stmt_map_info,
                                                           is_on_null_side,
                                                           tmp_valid,
                                                           equal_sets))) {
    LOG_WARN("failed to check whether transformation is possible", K(ret));
  } else if (tmp_valid) {
    is_valid = tmp_valid;
  } else {
    OPT_TRACE("not loseless join");
  }
  if (OB_SUCC(ret) && !is_valid) {
    OPT_TRACE(source_table, "can not eliminate", target_table, "with loseless join");
  }
  return ret;
}

int ObTransformJoinElimination::check_eliminate_delupd_table_valid(const ObDelUpdStmt *del_up_stmt,
                                                                   uint64_t table_id,
                                                                   bool &is_valid)
{
  int ret = OB_SUCCESS;
  bool is_modified = false;
  is_valid = true;
  ObSEArray<const ObDmlTableInfo*, 2> table_infos;
  if (OB_ISNULL(del_up_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null stmt", K(ret));
  } else if (OB_FAIL(del_up_stmt->get_dml_table_infos(table_infos))) {
    LOG_WARN("failed to get dml table infos", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < table_infos.count(); ++i) {
      const ObDmlTableInfo* table_info = table_infos.at(i);
      if (OB_ISNULL(table_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null table info", K(ret), K(i), K(table_infos));
      } else {
        is_valid = table_info->table_id_ != table_id;
      }
    }
  }
  return ret;
}

int ObTransformJoinElimination::check_on_null_side(ObDMLStmt *stmt,
                                                   uint64_t source_table_id,
                                                   uint64_t target_table_id,
                                                   bool is_from_base_table,
                                                   bool &is_on_null_side)
{
  int ret = OB_SUCCESS;
  is_on_null_side = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (is_from_base_table) {
    if (OB_FAIL(ObOptimizerUtil::is_table_on_null_side(stmt,
                                                       source_table_id,
                                                       is_on_null_side))) {
      LOG_WARN("failed to check table is on null side", K(ret));
    }
  } else {
    if (OB_FAIL(ObOptimizerUtil::is_table_on_null_side_of_parent(stmt,
                                                                 source_table_id,
                                                                 target_table_id,
                                                                 is_on_null_side))) {
      LOG_WARN("failed to check table is on null side", K(ret));
    }
  }
  return ret;
}

int ObTransformJoinElimination::do_join_elimination_foreign_key(ObDMLStmt *stmt,
                                                                const TableItem *child_table,
                                                                const TableItem *parent_table,
                                                                const ObForeignKeyInfo *foreign_key_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else {
    if (OB_FAIL(trans_column_items_foreign_key(stmt, child_table, parent_table,
                                               foreign_key_info))) {
      LOG_WARN("failed to transform columns of the eliminated table", K(ret));
    } else if (OB_FAIL(trans_table_item(stmt, child_table, parent_table))) {
      LOG_WARN("failed to transform target into source", K(ret));
    }
  }
  return ret;
}

int ObTransformJoinElimination::eliminate_join_in_joined_table(ObDMLStmt *stmt,
                                                               bool &trans_happened,
                                                               ObIArray<ObSEArray<TableItem *, 4>> &trans_tables)
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
      FromItem &cur_from_item = stmt->get_from_item(i);
      if (!cur_from_item.is_joined_) {
        /*do nothing*/
      } else if (OB_FAIL(eliminate_join_in_joined_table(stmt, cur_from_item, joined_tables,
                                                        happened, trans_tables))) {
        LOG_WARN("failed to eliminate self key join in joined table.", K(ret));
      } else {
        trans_happened |= happened;
      }
    }
    if (OB_SUCC(ret) && trans_happened) {
      //更新stmt的joined table list
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
    } else {/*do nothing*/}
  }
  return ret;
}

int ObTransformJoinElimination::eliminate_join_in_joined_table(ObDMLStmt *stmt,
                                                               FromItem &from_item,
                                                               ObIArray<JoinedTable*> &joined_tables,
                                                               bool &trans_happened,
                                                               ObIArray<ObSEArray<TableItem *, 4>> &trans_tables)
{
  int ret = OB_SUCCESS;
  bool can_trans = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null.", K(ret));
  } else if (from_item.is_joined_) {
    TableItem *joined_table = stmt->get_joined_table(from_item.table_id_);
    ObSEArray<TableItem *, 4> child_candi_tables;
    ObSEArray<ObRawExpr *, 16> trans_conditions;
    if (OB_ISNULL(joined_table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("joined table is null.", K(ret));
    } else if (OB_FAIL(eliminate_join_in_joined_table(stmt, joined_table,
                                                      child_candi_tables,
                                                      trans_conditions,
                                                      can_trans,
                                                      trans_tables))) {
      LOG_WARN("failed to eliminate self key join in joined table.", K(ret));
    } else if (OB_ISNULL(joined_table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("joined table is null.", K(ret));
    } else if (!can_trans) {
      /* do nothing. */
    } else if (OB_FAIL(stmt->add_condition_exprs(trans_conditions))) {
      LOG_WARN("add trans conditions to where conditions failed", K(ret));
    } else {
      //更新joined table结构
      trans_happened = true;
      from_item.is_joined_ = joined_table->is_joined_table();
      from_item.table_id_ = joined_table->table_id_;
    }
    if (OB_FAIL(ret)) {
      /*do nothing*/
    } else if (!joined_table->is_joined_table()) {
      /*do nothing*/
    } else if (OB_FAIL(joined_tables.push_back(static_cast<JoinedTable *>(joined_table)))) {
      LOG_WARN("failed to push back joined table.", K(ret));
    } else { /*do nothing.*/ }
  } else { /*do nothing.*/ }
  return ret;
}

int ObTransformJoinElimination::extract_equal_join_columns(const ObIArray<ObRawExpr *> &join_conds,
                                                           const TableItem *source_table,
                                                           const TableItem *target_table,
                                                           ObIArray<ObRawExpr *> &source_exprs,
                                                           ObIArray<ObRawExpr *> &target_exprs,
                                                           ObIArray<int64_t> *unused_conds)
{
  int ret = OB_SUCCESS;
  source_exprs.reuse();
  target_exprs.reuse();
  if (OB_ISNULL(source_table) || OB_ISNULL(target_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parameter is null", K(ret), K(source_table), K(target_table));
  } else {
    const uint64_t source_tid = source_table->table_id_;
    const uint64_t target_tid = target_table->table_id_;
    for (int64_t i = 0; OB_SUCC(ret) && i < join_conds.count(); ++i) {
      ObRawExpr *expr = join_conds.at(i);
      bool used = false;
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null", K(ret));
      } else if (T_OP_EQ == expr->get_expr_type()) {
        ObOpRawExpr *op = static_cast<ObOpRawExpr *>(expr);
        if (op->get_param_expr(0)->is_column_ref_expr() &&
            op->get_param_expr(1)->is_column_ref_expr()) {
          ObColumnRefRawExpr *col1 = static_cast<ObColumnRefRawExpr *>(op->get_param_expr(0));
          ObColumnRefRawExpr *col2 = static_cast<ObColumnRefRawExpr *>(op->get_param_expr(1));
          if (col1->get_table_id() == source_tid && col2->get_table_id() == target_tid) {
            if (OB_FAIL(source_exprs.push_back(col1))) {
              LOG_WARN("failed to push back expr", K(ret));
            } else if (OB_FAIL(target_exprs.push_back(col2))) {
              LOG_WARN("failed to push back expr", K(ret));
            } else {
              used = true;
            }
          } else if (col1->get_table_id() == target_tid && col2->get_table_id() == source_tid) {
            if (OB_FAIL(target_exprs.push_back(col1))) {
              LOG_WARN("failed to push back expr", K(ret));
            } else if (OB_FAIL(source_exprs.push_back(col2))) {
              LOG_WARN("failed to push back expr", K(ret));
            } else {
              used = true;
            }
          }
        }
      }
      if (OB_SUCC(ret) && !used && NULL != unused_conds) {
        ret = unused_conds->push_back(i);
      }
    }
  }
  return ret;
}

int ObTransformJoinElimination::adjust_table_items(ObDMLStmt *stmt,
                                                   TableItem *source_table,
                                                   TableItem *target_table,
                                                   ObStmtMapInfo &stmt_map_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt) || OB_ISNULL(source_table) || OB_ISNULL(target_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(stmt), K(source_table),
        K(target_table), K(ret));
  } else if ((source_table->is_basic_table() &&
             target_table->is_basic_table()) ||
            (source_table->is_temp_table() &&
             target_table->is_temp_table())) {
    if (OB_FAIL(ObTransformUtils::merge_table_items(stmt,
                                                    source_table,
                                                    target_table,
                                                    NULL))) {
      LOG_WARN("failed to check merge table items", K(ret));
    } else { /*do nothing*/ }
  } else if (source_table->is_generated_table() &&
             target_table->is_generated_table()) {
    ObSelectStmt *target_stmt = NULL;
    ObSelectStmt *source_stmt = NULL;
    ObSEArray<int64_t, 16> column_map;
    if (OB_ISNULL(source_stmt = source_table->ref_query_) ||
        OB_ISNULL(target_stmt = target_table->ref_query_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(source_stmt), K(target_stmt),
          K(ret));
    } else if (OB_FAIL(reverse_select_items_map(source_stmt,
                                                target_stmt,
                                                stmt_map_info.select_item_map_,
                                                column_map))) {
      LOG_WARN("failed to reverse select items map", K(ret));
    } else if (OB_FAIL(create_missing_select_items(source_stmt, target_stmt,
                                                   column_map,
                                                   stmt_map_info.table_map_))) {
      LOG_WARN("failed to create missing select items", K(ret));
    } else if (OB_FAIL(ObTransformUtils::merge_table_items(stmt,
                                                           source_table,
                                                           target_table,
                                                           &column_map))) {
      LOG_WARN("failed to merge table items", K(ret));
    } else { /*do nothing*/ }
  } else { /*do nothing*/ }

  return ret;
}

int ObTransformJoinElimination::create_missing_select_items(ObSelectStmt *source_stmt,
                                                            ObSelectStmt *target_stmt,
                                                            ObIArray<int64_t> &column_map,
                                                            const ObIArray<int64_t> &table_map)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(source_stmt) || OB_ISNULL(target_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(source_stmt),
        K(target_stmt), K(ret));
  } else if (OB_UNLIKELY(column_map.count() != target_stmt->get_select_item_size())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected array count", K(column_map.count()),
        K(target_stmt->get_select_item_size()), K(ret));
  } else {
    ObSEArray<ObRawExpr*, 16> new_exprs;
    ObSEArray<ObRawExpr*, 16> old_exprs;
    ObSEArray<SelectItem*, 16> miss_select_items;
    ObSEArray<int64_t, 16> miss_select_item_idx_array;
    for (int64_t i = 0; OB_SUCC(ret) && i < target_stmt->get_select_item_size(); i++) {
      if (OB_INVALID_ID != column_map.at(i)) {
        /*do nothing*/
      } else if (OB_FAIL(miss_select_items.push_back(&target_stmt->get_select_item(i)))) {
        LOG_WARN("failed to push back select item", K(ret));
      } else if (OB_FAIL(miss_select_item_idx_array.push_back(i))) {
        LOG_WARN("failed to push back select item idx", K(ret));
      } else { /*do nothing*/ }
    }

    if (OB_SUCC(ret) && !miss_select_items.empty()) {
      for (int64_t i = 0; OB_SUCC(ret) && i < table_map.count(); i++) {
        TableItem *temp_source_table = NULL;
        TableItem *temp_target_table = NULL;
        if (OB_ISNULL(temp_source_table = source_stmt->get_table_item(i)) ||
            OB_ISNULL(temp_target_table = target_stmt->get_table_item(table_map.at(i)))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(temp_source_table), K(temp_target_table), K(ret));
        } else if ((temp_source_table->is_basic_table() && temp_target_table->is_basic_table()) ||
                    (temp_source_table->is_link_table() && temp_target_table->is_link_table()) ||
                   (temp_source_table->is_temp_table() && temp_target_table->is_temp_table()) ||
                   (temp_source_table->is_generated_table() && temp_target_table->is_generated_table())) {
          if (OB_FAIL(ObTransformUtils::merge_table_items(source_stmt,
                                                          target_stmt,
                                                          temp_source_table,
                                                          temp_target_table,
                                                          old_exprs,
                                                          new_exprs))) {
            LOG_WARN("failed to merge table items", K(ret));
          } else { /*do nothing*/ }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected table type", K(temp_source_table->type_),
              K(temp_target_table->type_), K(ret));
        }
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < miss_select_items.count(); i++) {
      if (OB_ISNULL(miss_select_items.at(i)->expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(ObTransformUtils::replace_expr(old_exprs,
                                                        new_exprs,
                                                        miss_select_items.at(i)->expr_))) {
        LOG_WARN("failed to replace expr", K(ret));
      } else if (OB_FAIL(ObTransformUtils::adjust_agg_and_win_expr(source_stmt,
                                                                   miss_select_items.at(i)->expr_))) {
        LOG_WARN("failed to remove duplicated agg expr", K(ret));
      } else if (OB_FAIL(source_stmt->add_select_item(*miss_select_items.at(i)))) {
        LOG_WARN("failed to add select item", K(ret));
      } else if (OB_FAIL(ObTransformUtils::extract_query_ref_expr(miss_select_items.at(i)->expr_,
                                                                  source_stmt->get_subquery_exprs()))) {
        LOG_WARN("failed to extract query ref exprs", K(ret));
      } else {
        int64_t miss_select_item_idx = miss_select_item_idx_array.at(i);
        if (OB_UNLIKELY(miss_select_item_idx < 0 || miss_select_item_idx >= column_map.count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected idx", K(miss_select_item_idx), K(column_map), K(ret));
        } else {
          column_map.at(miss_select_item_idx) = source_stmt->get_select_item_size() - 1;
        }
      }
    }
  }
  return ret;
}


int ObTransformJoinElimination::reverse_select_items_map(const ObSelectStmt *source_stmt,
                                                         const ObSelectStmt *target_stmt,
                                                         const ObIArray<int64_t> &column_map,
                                                         ObIArray<int64_t> &reverse_column_map)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(source_stmt) || OB_ISNULL(target_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(source_stmt),
        K(target_stmt), K(ret));
  } else if (OB_UNLIKELY(column_map.count() !=
                         source_stmt->get_select_item_size())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected array count", K(column_map.count()),
        K(source_stmt->get_select_item_size()), K(ret));
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
      } else if (OB_UNLIKELY(column_map.at(i) < 0 ||
                             column_map.at(i) >= target_count)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected array count", K(column_map.at(i)),
            K(target_count), K(ret));
      } else {
        reverse_column_map.at(column_map.at(i)) = i;
      }
    }
  }
  return ret;
}

int ObTransformJoinElimination::trans_table_item(ObDMLStmt *stmt,
                                                 const TableItem *source_table,
                                                 const TableItem *target_table)
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
    } else if ((stmt->is_update_stmt() || stmt->is_delete_stmt()) &&
               OB_FAIL(static_cast<ObDelUpdStmt*>(stmt)->remove_table_item_dml_info(target_table))) {
      LOG_WARN("failed to remove table item dml info", K(ret));
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
                                                          ObDMLStmt *stmt,
                                                          JoinedTable *joined_table,
                                                          const bool is_non_sens_dul_vals,
                                                          ObIArray<ObRawExprPointer> &relation_exprs,
                                                          bool &is_valid)
{
  int ret = OB_SUCCESS;
  bool is_unique = false;
  bool is_hint_valid = false;
  ObSEArray<ObRawExpr *, 8> target_exprs;
  ObSqlBitSet<> left_tables;
  ObSqlBitSet<> right_tables;
  ObSqlBitSet<> join_source_ids;
  ObSqlBitSet<> join_target_ids;
  is_valid = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(joined_table) ||
      OB_ISNULL(joined_table->left_table_) || OB_ISNULL(joined_table->right_table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt or joined table is null.", K(ret), K(stmt), K(joined_table));
  } else if (LEFT_OUTER_JOIN != joined_table->joined_type_) {
    is_valid = false;
  } else if (OB_FAIL(check_hint_valid(*stmt, *joined_table->right_table_, is_hint_valid))) {
    LOG_WARN("failed to check hint valid", K(ret));
  } else if (!is_hint_valid) {
    OPT_TRACE("hint disable transform");
  } else if (OB_FAIL(stmt->get_table_rel_ids(*joined_table->right_table_, right_tables))) {
    LOG_WARN("failed to get table ids", K(ret));
  } else if (is_non_sens_dul_vals) {
    is_valid = true;
  } else if (OB_FAIL(stmt->get_table_rel_ids(*joined_table->left_table_, left_tables))) {
    LOG_WARN("failed to get table ids", K(ret));
  } else if (OB_FAIL(ObTransformUtils::get_table_joined_exprs(left_tables,
                                                              right_tables,
                                                              joined_table->join_conditions_,
                                                              target_exprs,
                                                              join_source_ids,
                                                              join_target_ids))) {
    LOG_WARN("failed to get table joined exprs.", K(ret));
  } else if (join_source_ids.num_members() != 1 || join_target_ids.num_members() != 1) {
    // do nothing
  } else if (OB_FAIL(ObTransformUtils::check_exprs_unique(*stmt, joined_table->right_table_,
                                                          target_exprs, ctx_->session_info_,
                                                          ctx_->schema_checker_, is_unique))) {
    LOG_WARN("failed to check exptrs all row expr.", K(ret));
  } else if (is_unique) {
    is_valid = true;
  }
  if (OB_SUCC(ret) && is_valid) {
    bool contained = false;
    if (OB_FAIL(ObTransformUtils::check_table_contain_in_semi(stmt,
                                                              joined_table->right_table_,
                                                              contained))) {
      LOG_WARN("failed to check table contained in semi", K(ret));
    } else if (contained) {
      is_valid = false;
    }
  }

  if (OB_SUCC(ret) && is_valid) {
    ObSqlBitSet<> rel_ids;
    ObSEArray<ObRawExpr *, 8> join_conditions;
    int64_t join_ref_count = 0;
    int64_t total_ref_count = 0;
    if (OB_FAIL(join_conditions.assign(joined_table->join_conditions_))) {
      LOG_WARN("failed to push back to join conditions.", K(ret));
    } else if (OB_FAIL(extract_child_conditions(stmt,
                                                joined_table->right_table_,
                                                join_conditions,
                                                rel_ids))) {
      LOG_WARN("failed to extract right child exprs.", K(ret));
    } else if (OB_FAIL(compute_table_expr_ref_count(rel_ids, join_conditions, join_ref_count))) {
      LOG_WARN("failed to compute expr ref", K(ret));
    } else if (OB_FAIL(compute_table_expr_ref_count(rel_ids, relation_exprs, total_ref_count))) {
      LOG_WARN("failed to compute table expr ref count", K(ret));
    } else {
      is_valid = (join_ref_count == total_ref_count);
    }
  }
  return ret;
}

int ObTransformJoinElimination::compute_table_expr_ref_count(const ObSqlBitSet<> &rel_ids,
                                                             const ObIArray<ObRawExpr *> &exprs,
                                                             int64_t &total_ref_count)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    if (OB_FAIL(compute_table_expr_ref_count(rel_ids, exprs.at(i), total_ref_count))) {
      LOG_WARN("failed to compute table expr ref count", K(ret));
    }
  }
  return ret;
}

int ObTransformJoinElimination::compute_table_expr_ref_count(const ObSqlBitSet<> &rel_ids,
                                                             const ObIArray<ObRawExprPointer> &exprs,
                                                             int64_t &total_ref_count)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    const ObRawExprPointer &expr_ptr = exprs.at(i);
    ObRawExpr *expr = NULL;
    int64_t count = 0;
    if (OB_FAIL(expr_ptr.get(expr))) {
      LOG_WARN("failed to get expr", K(ret));
    } else if (OB_FAIL(compute_table_expr_ref_count(rel_ids, expr, count))) {
      LOG_WARN("failed to compute table expr ref count", K(ret));
    } else {
      total_ref_count += (count * expr_ptr.ref_count());
    }
  }
  return ret;
}

int ObTransformJoinElimination::compute_table_expr_ref_count(const ObSqlBitSet<> &rel_ids,
                                                             const ObRawExpr *expr,
                                                             int64_t &total_ref_count)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret), K(expr));
  } else if (expr->get_relation_ids().overlap2(rel_ids)) {
    if (expr->is_column_ref_expr()) {
      ++ total_ref_count;
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
        if (OB_FAIL(compute_table_expr_ref_count(rel_ids,
                                                 expr->get_param_expr(i),
                                                 total_ref_count))) {
          LOG_WARN("failed to compute table expr ref count", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTransformJoinElimination::extract_child_conditions(ObDMLStmt *stmt,
                                                         TableItem *table_item,
                                                         ObIArray<ObRawExpr *> &join_conditions,
                                                         ObSqlBitSet<> &rel_ids)
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
  } else if (table_item->is_joined_table()) {
    JoinedTable *joined_table = static_cast<JoinedTable*>(table_item);
    if (OB_FAIL(SMART_CALL(extract_child_conditions(stmt,
                                                    joined_table->left_table_,
                                                    join_conditions,
                                                    rel_ids)))) {
      LOG_WARN("failed to remove right tables from stmt", K(ret));
    } else if (OB_FAIL(SMART_CALL(extract_child_conditions(stmt,
                                                           joined_table->right_table_,
                                                           join_conditions,
                                                           rel_ids)))) {
      LOG_WARN("failed to remove right tables from stmt", K(ret));
    } else {
      int64_t N = joined_table->join_conditions_.count();
      for (int64_t i = 0; OB_SUCC(ret) && i < N; i++) {
        if (OB_FAIL(join_conditions.push_back(joined_table->join_conditions_.at(i)))) {
          LOG_WARN("failed to push back to join condition.", K(ret));
        } else { /* do nothing. */ }
      }
    }
  } else if (OB_FAIL(rel_ids.add_member(stmt->get_table_bit_index(table_item->table_id_)))) {
    LOG_WARN("failed to add member to rel ids.", K(ret));
  } else { /* do nothing. */ }
  return ret;
}

int ObTransformJoinElimination::eliminate_outer_join_in_joined_table(ObDMLStmt *stmt,
                                                                     TableItem *&table_item,
                                                                     const bool is_non_sens_dul_vals,
                                                                     ObIArray<uint64_t> &table_ids,
                                                                     ObIArray<ObRawExprPointer> &relation_exprs,
                                                                     bool &trans_happen,
                                                                     ObIArray<ObSEArray<TableItem *, 4>> &trans_tables)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  trans_happen = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt or joined table is null.", K(stmt), K(table_item), K(ret));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_stack_overflow));
  } else if (table_item->is_joined_table()) {
    bool is_valid = false;
    JoinedTable *joined_table = static_cast<JoinedTable*>(table_item);
    if (OB_FAIL(check_transform_validity_outer_join(stmt,
                                                    joined_table,
                                                    is_non_sens_dul_vals,
                                                    relation_exprs,
                                                    is_valid))) {
      LOG_WARN("failed to check transform validity outer join.", K(ret));
    } else if (!is_valid) {
      bool left_is_happend = false;
      bool right_is_happend = false;
      if (OB_FAIL(SMART_CALL(eliminate_outer_join_in_joined_table(stmt,
                                                                  joined_table->right_table_,
                                                                  is_non_sens_dul_vals,
                                                                  table_ids,
                                                                  relation_exprs,
                                                                  right_is_happend,
                                                                  trans_tables)))) {
        LOG_WARN("failed to eliminate ouer join in joined table.", K(ret));
      } else if (OB_FAIL(SMART_CALL(eliminate_outer_join_in_joined_table(stmt,
                                                                         joined_table->left_table_,
                                                                         is_non_sens_dul_vals,
                                                                         table_ids,
                                                                         relation_exprs,
                                                                         left_is_happend,
                                                                         trans_tables)))) {
        LOG_WARN("failed to eliminate ouer join in joined table.", K(ret));
      } else if (!left_is_happend && !right_is_happend) {
        /* do nothing */
      } else if (OB_FAIL(ObTransformUtils::adjust_single_table_ids(joined_table))) {
        LOG_WARN("failed to construct single table ids.", K(ret));
      } else {
        trans_happen = true;
      }
    } else if (OB_ISNULL(joined_table->left_table_) || OB_ISNULL(joined_table->right_table_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("left child or right child is null.", K(ret));
    } else if (OB_FAIL(SMART_CALL(eliminate_outer_join_in_joined_table(stmt,
                                                                       joined_table->left_table_,
                                                                       is_non_sens_dul_vals,
                                                                       table_ids,
                                                                       relation_exprs,
                                                                       trans_happen,
                                                                       trans_tables)))) {
      LOG_WARN("failed to eliminate ouer join in joined table.", K(ret));
    } else if (OB_FAIL(construct_eliminated_table(stmt, joined_table->right_table_, trans_tables))) {
      LOG_WARN("failed to construct eliminated tables", K(ret));
    } else if (OB_FAIL(ObTransformUtils::remove_tables_from_stmt(stmt,joined_table->right_table_,
                                                                 table_ids))) {
      LOG_WARN("failed to remove table items.", K(ret));
    } else {
      table_item = joined_table->left_table_;
      trans_happen = true;
    }
  }
  return ret;
}

int ObTransformJoinElimination::do_eliminate_left_outer_join_rec(ObDMLStmt *stmt,
                                                                 TableItem *&table,
                                                                 bool &trans_happened,
                                                                 ObIArray<ObSEArray<TableItem *, 4>> &trans_tables)
{
  int ret = OB_SUCCESS;
  JoinedTable *joined_table = NULL;
  bool can_be_eliminated = false;
  bool left_happened = false;
  bool right_happened = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (!table->is_joined_table()) {
    // do nothing
  } else if (OB_ISNULL(joined_table = static_cast<JoinedTable*>(table))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null joined table", K(ret));
  } else if (OB_FAIL(left_join_can_be_eliminated(stmt, table, can_be_eliminated))) {
    LOG_WARN("failed to check if the joined table can be eliminated", K(ret), K(*table));
  } else if (!can_be_eliminated) {
    if (OB_FAIL(SMART_CALL(do_eliminate_left_outer_join_rec(stmt,
                                                            joined_table->left_table_,
                                                            left_happened,
                                                            trans_tables)))) {
      LOG_WARN("failed to eliminate left child", K(ret), K(*joined_table));
    } else if (OB_FAIL(SMART_CALL(do_eliminate_left_outer_join_rec(stmt,
                                                                   joined_table->right_table_,
                                                                   right_happened,
                                                                   trans_tables)))) {
      LOG_WARN("failed to eliminate right child", K(ret), K(*joined_table));
    } else if (!left_happened && !right_happened) {
    } else {
      trans_happened = true;
    }
  } else if (OB_FAIL(do_eliminate_left_outer_join(stmt,
                                                  table,
                                                  trans_tables))) {
    LOG_WARN("failed to eliminate left outer join", K(ret), K(*table));
  } else if (OB_FAIL(SMART_CALL(do_eliminate_left_outer_join_rec(stmt,
                                                                 table,
                                                                 trans_happened,
                                                                 trans_tables)))) { // the trans_happened here is unused
    LOG_WARN("failed to eliminate current table", K(ret), K(*table));
  } else {
    trans_happened = true;
  }

  if (OB_FAIL(ret) || !trans_happened || !table->is_joined_table()) {
  } else if (OB_FAIL(ObTransformUtils::adjust_single_table_ids(static_cast<JoinedTable *>(table)))) {
    LOG_WARN("failed to adjust single table ids", K(ret), K(*joined_table));
  }
  return ret;
}

int ObTransformJoinElimination::eliminate_left_outer_join(ObDMLStmt *stmt,
                                                          bool &trans_happened,
                                                          ObIArray<ObSEArray<TableItem *, 4>> &trans_tables)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(stmt), K(ctx_), K(ret));
  } else {
    TableItem *root_table = NULL;
    JoinedTable *joined_table = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_from_item_size(); i++) {
      FromItem &cur_from_item = stmt->get_from_item(i);
      bool cur_trans_happened = false;
      if (!cur_from_item.is_joined_) {
        // do nothing
      } else if (OB_ISNULL(joined_table = stmt->get_joined_table(cur_from_item.table_id_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get joined table");
      } else if (OB_FALSE_IT(root_table = joined_table)) {
        // the root_table will be modified if it or its children can be eliminated
      } else if (OB_FAIL(do_eliminate_left_outer_join_rec(stmt,
                                                          root_table,
                                                          cur_trans_happened,
                                                          trans_tables))) {
        LOG_WARN("failed to eliminate left join elimination", K(ret));
      } else if (cur_trans_happened) {
        trans_happened = true;
        // the original table should be removed
        // if the root_table is still a joined table after transformation, we should add it
        if (OB_FAIL(stmt->remove_joined_table_item(joined_table))) {
          LOG_WARN("failed to remove joined table", K(ret));
        } else if (OB_ISNULL(root_table)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (!root_table->is_joined_table()) {
          // do nothing
        } else if (OB_FAIL(stmt->add_joined_table(static_cast<JoinedTable*>(root_table)))) {
          LOG_WARN("failed to add joined table");
        }
        // update the from item
        if (OB_SUCC(ret)) {
          cur_from_item.table_id_ = root_table->table_id_;
          cur_from_item.is_joined_ = root_table->is_joined_table();
        }
      }
    }
    ObIArray<SemiInfo*> &semi_infos = stmt->get_semi_infos();
    for (int64_t i = 0; OB_SUCC(ret) && trans_happened && i < semi_infos.count(); i++) {
      // if the left tables of semi info is empty, we should not raise an error 
      // add a table id to it instead
      if (OB_UNLIKELY(semi_infos.at(i)->left_table_ids_.empty())) {
        if (OB_ISNULL(root_table)) {
          // do nothing
        } else if (root_table->is_joined_table()) {
          ret = append(semi_infos.at(i)->left_table_ids_, static_cast<JoinedTable *>(root_table)->single_table_ids_);
        } else {
          ret = semi_infos.at(i)->left_table_ids_.push_back(root_table->table_id_);
        }
      }
    }
    if (OB_FAIL(ret) || !trans_happened) {
    } else if (OB_FAIL(stmt->rebuild_tables_hash())) {
      LOG_WARN("failed to rebuild tables hash", K(ret));
    } else if (OB_FAIL(stmt->update_column_item_rel_id())) {
      LOG_WARN("failed to update columns relation id", K(ret));
    } else if (OB_FAIL(stmt->formalize_stmt(ctx_->session_info_))) {
      LOG_WARN("failed to formalizae stmt", K(ret));
    }
  }
  return ret;
}

int ObTransformJoinElimination::do_eliminate_left_outer_join(ObDMLStmt *stmt,
                                                             TableItem *&table,
                                                             ObIArray<ObSEArray<TableItem *, 4>> &trans_tables)
{
  int ret = OB_SUCCESS;
  JoinedTable *joined_table = NULL;
  TableItem *right_table = NULL;
  ObSEArray<uint64_t, 16> right_table_ids;
  ObSEArray<TableItem *, 4> right_table_items;
  if (OB_ISNULL(stmt) || OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret), K(stmt), K(table));
  } else if (!table->is_joined_table()) {
  } else if (OB_FALSE_IT(joined_table = static_cast<JoinedTable*>(table))) {
  } else if (OB_ISNULL(joined_table->left_table_) ||
             OB_ISNULL(right_table = joined_table->right_table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("left table or right table in joined table is null", K(ret));
  } else if (OB_FAIL(get_table_items_and_ids(stmt,
                                             right_table,
                                             right_table_ids,
                                             right_table_items))) {
    LOG_WARN("failed to get table items and ids", K(ret));
  } else {
    // to eliminate the right table, we must:
    // 1. remove the table item
    // 2. remove the column item
    // 3. create T_NULL to replace the removed column exprs (a cast also should be be added)
    // 4. remove partition exprs
    ObSEArray<ObRawExpr*, 16> from_exprs;
    ObSEArray<ObRawExpr*, 16> to_exprs;
    for(int64_t i = 0; OB_SUCC(ret) && i < stmt->get_column_size(); ++i) {
      const ColumnItem *col_item = stmt->get_column_item(i);
      if (OB_ISNULL(col_item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (is_contain(right_table_ids, col_item->table_id_)) {
        // create new null raw expr
        // add a cast expr to the null expr, whose type is same to original expr
        ObRawExpr *from_expr = col_item->expr_;
        ObRawExpr *to_expr = NULL;
        if (OB_ISNULL(from_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (OB_FAIL(ObRawExprUtils::build_null_expr(*ctx_->expr_factory_, to_expr))) {
          LOG_WARN("failed to create new raw expr", K(ret));
        } else if (OB_FAIL(ObTransformUtils::add_cast_for_replace(*ctx_->expr_factory_,
                                                                  from_expr, to_expr,
                                                                  ctx_->session_info_))) {
          LOG_WARN("failed to add cast for replace", K(ret));
        } else if (OB_ISNULL(to_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null expr", K(ret));
        } else if (OB_FAIL(from_exprs.push_back(from_expr))) {
          LOG_WARN("failed to push back column item expr", K(ret));
        } else if (OB_FAIL(to_exprs.push_back(to_expr))) {
          LOG_WARN("failed to add T_NULL raw expr", K(ret));
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(construct_eliminated_table(stmt, right_table, trans_tables))) {
      LOG_WARN("failed to construct eliminated table", K(ret));
    } else if (OB_FAIL(stmt->remove_table_info(right_table_items))) {
      LOG_WARN("failed to remove table info", K(ret));
    } else if (OB_FAIL(stmt->replace_relation_exprs(from_exprs, to_exprs))) {
      LOG_WARN("failed to replace inner stmt expr", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_semi_infos().count(); ++i) {
        if (OB_ISNULL(stmt->get_semi_infos().at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("semi info is null", K(ret));
        } else if (OB_FAIL(ObOptimizerUtil::remove_item(stmt->get_semi_infos().at(i)->left_table_ids_,
                                                        right_table_ids))) {
          LOG_WARN("failed to eliminate outer join in from items.", K(ret));
        }
      }
      if (OB_SUCC(ret) && (stmt->is_update_stmt() || stmt->is_delete_stmt())) {
        ObDelUpdStmt* del_upd_stmt = static_cast<ObDelUpdStmt*>(stmt);
        for (int64_t i = 0; OB_SUCC(ret) && i < right_table_items.count(); ++i) {
          if (OB_FAIL(del_upd_stmt->remove_table_item_dml_info(right_table_items.at(i)))) {
            LOG_WARN("failed to remove table item dml info", K(ret));
          }
        }
      }
      if (OB_SUCC(ret)) {
        table = joined_table->left_table_;
      }
    }
  }
  return ret;
}

int ObTransformJoinElimination::get_table_items_and_ids(ObDMLStmt *stmt,
                                                        TableItem *table,
                                                        ObIArray<uint64_t> &table_ids,
                                                        ObIArray<TableItem *> &table_items)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt) || OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(stmt), K(table));
  } else if (table->is_joined_table()) {
    JoinedTable *joined_table = static_cast<JoinedTable *>(table);
    if (OB_FAIL(table_ids.assign(joined_table->single_table_ids_))) {
      LOG_WARN("failed to assign joined table ids", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < table_ids.count(); ++i) {
        TableItem *child_table = NULL;
        if (OB_ISNULL(child_table = stmt->get_table_item_by_id(table_ids.at(i)))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (OB_FAIL(table_items.push_back(child_table))) {
          LOG_WARN("failed to push back table item", K(ret));
        }
      }
    }
  } else {
    if (OB_FAIL(table_ids.push_back(table->table_id_))) {
      LOG_WARN("failed to push back table id", K(ret));
    } else if (OB_FAIL(table_items.push_back(table))) {
      LOG_WARN("failed to push back child table item", K(ret));
    }
  }
  return ret;
}
int ObTransformJoinElimination::left_join_can_be_eliminated(ObDMLStmt *stmt,
                                                            TableItem *table,
                                                            bool &can_be_eliminated)
{
  int ret = OB_SUCCESS;
  can_be_eliminated = false;
  JoinedTable *joined_table = NULL;
  ObPhysicalPlanCtx *plan_ctx = NULL;
  bool is_hint_valid = false;
  bool is_dml_table = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(table) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->exec_ctx_) || 
      OB_ISNULL(plan_ctx = ctx_->exec_ctx_->get_physical_plan_ctx()) ||
      OB_ISNULL(ctx_->allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(stmt), K(table), K(ctx_));
  } else if (!table->is_joined_table()) {
  } else if (OB_FALSE_IT(joined_table = static_cast<JoinedTable*>(table))) {
  } else if (!joined_table->is_left_join()) {
    // do nothing
  } else if (OB_ISNULL(joined_table->left_table_) || OB_ISNULL(joined_table->right_table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if ((stmt::T_MERGE == stmt->get_stmt_type() && 
            joined_table->right_table_->table_id_ == static_cast<ObMergeStmt*>(stmt)->get_target_table_id()) ||
            joined_table->right_table_->for_update_) {
    // Two cases that contains joined tables should not be eliminated:
    // 1. MERGE INTO
    //    eg: merge into t1 using t2 on (1 = 0) when matched then update set t1.a = 1;
    //    t2 is a source table and is not allowed to be eliminated
    // 2. MULTI FOR UPDATE
    //    eg: select * from t1 left join t2 on 1 = 0 for update;
    //    the MULTI FOR UPDATE operator will lock t1, t2. In this scenario, all the joined tables are not allowed to be eliminated
  } else if (ObStmt::is_dml_write_stmt(stmt->get_stmt_type()) &&
             OB_FAIL(static_cast<ObDelUpdStmt *>(stmt)->has_dml_table_info(joined_table->right_table_->table_id_,
                                                                           is_dml_table))) {
    LOG_WARN("failed to check is dml table", K(ret));
  } else if (is_dml_table) {
    // do nothing
  } else if (OB_FAIL(check_hint_valid(*stmt, *joined_table->right_table_, is_hint_valid))) {
    LOG_WARN("failed to check hint valid", K(ret));
  } else if (!is_hint_valid) {
    // do nothing
  } else {
    ObRawExpr *condition = NULL;
    for (int64_t i = 0; !can_be_eliminated && OB_SUCC(ret) &&
                        i < joined_table->join_conditions_.count(); ++i) {
      if (OB_ISNULL(condition = joined_table->join_conditions_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("condition is null", K(ret));
        // todo 
      } else if (condition->is_static_scalar_const_expr()) {
        ObObj value;
        bool got_result = false;
        if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(ctx_->exec_ctx_,
                                                              condition,
                                                              value,
                                                              got_result,
                                                              *ctx_->allocator_))) {
          LOG_WARN("failed to calc const or calculable expr", K(ret));
        } else if (got_result) {
          can_be_eliminated = value.is_false();
          if (can_be_eliminated && OB_FAIL(ObTransformUtils::add_param_bool_constraint(ctx_, condition, false))) {
            LOG_WARN("failed to add param bool constraint", K(ret));
          }
        }
      }
    }
  }
  LOG_TRACE("left join can be eliminated: ", K(can_be_eliminated), KPC(table));
  return ret;
}

// 1. stmt is generate table refquery in semi info right table
// 2. stmt is subquery is exists
// 2. stmt has distinct
int ObTransformJoinElimination::check_vaild_non_sens_dul_vals(ObIArray<ObParentDMLStmt> &parent_stmts,
                                                              ObDMLStmt *stmt,
                                                              bool &is_valid,
                                                              bool &need_add_limit_constraint)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  need_add_limit_constraint = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (stmt->is_select_stmt()) {
    ObSelectStmt *select_stmt = static_cast<ObSelectStmt*>(stmt);
    ObDMLStmt *parent_stmt = NULL;
    bool has_rownum_expr = false;
    if (select_stmt->has_group_by() ||
        select_stmt->is_set_stmt() ||
        select_stmt->has_rollup() ||
        select_stmt->has_order_by() ||
        select_stmt->has_limit() ||
        select_stmt->get_from_item_size() == 0 ||
        select_stmt->is_contains_assignment() ||
        select_stmt->has_window_function() ||
        select_stmt->has_sequence()) {
      is_valid = false;
    } else if (OB_FAIL(select_stmt->has_rownum(has_rownum_expr))) {
      LOG_WARN("failed to check has rownum", K(ret));
    } else if (has_rownum_expr) {
      is_valid = false;
    } else if (select_stmt->is_distinct()) {
      is_valid = true;
    }

    if (OB_FAIL(ret) || is_valid || parent_stmts.empty()) {
      /* do nothing */
    } else if (OB_ISNULL(parent_stmt = parent_stmts.at(parent_stmts.count() - 1).stmt_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(ObTransformUtils::check_stmt_is_non_sens_dul_vals(ctx_, parent_stmt, stmt,
                                                            is_valid, need_add_limit_constraint))) {
      LOG_WARN("failed to check stmt is non sens dul vals", K(ret));
    } else if (is_valid) {
      /* do nothing */
    } else if (!select_stmt->is_spj()) {
      /* do nothing */
    } else if (parent_stmt->is_select_stmt() &&
               static_cast<ObSelectStmt*>(parent_stmt)->is_set_distinct()) {
      ObSelectStmt *sel_parent_stmt = static_cast<ObSelectStmt*>(parent_stmt);
      for (int i = 0; !is_valid && i < sel_parent_stmt->get_set_query().count(); ++i) {
        is_valid = sel_parent_stmt->get_set_query(i) == select_stmt;
      }
    } else {
      SemiInfo* semi = NULL;
      TableItem *table = NULL;
      for (int64_t i = 0; OB_SUCC(ret) && !is_valid && i < parent_stmt->get_semi_info_size(); ++i) {
        if (OB_ISNULL(semi = parent_stmt->get_semi_infos().at(i)) ||
            OB_ISNULL(table = parent_stmt->get_table_item_by_id(semi->right_table_id_))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (table->ref_query_ == stmt) {
          is_valid = true;
        }
      }
    }
  }
  return ret;
}

int ObTransformJoinElimination::eliminate_outer_join(ObIArray<ObParentDMLStmt> &parent_stmts,
                                                     ObDMLStmt *stmt,
                                                     bool &trans_happened,
                                                     ObIArray<ObSEArray<TableItem *, 4>> &trans_tables)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExprPointer, 8> relation_exprs;
  bool is_non_sens_dul_vals = false;
  bool need_add_limit_constraint = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->schema_checker_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(stmt), K(ctx_), K(ret));
  } else if (OB_FAIL(stmt->get_relation_exprs(relation_exprs))) {
    LOG_WARN("failed to get relations exprs all.", K(ret));
  } else if (OB_FAIL(check_vaild_non_sens_dul_vals(parent_stmts, stmt, is_non_sens_dul_vals,
                                                   need_add_limit_constraint))) {
    LOG_WARN("failed to check valid", K(ret));
  } else {
    TableItem *table = NULL;
    bool is_happend = false;
    common::ObArray<JoinedTable*> joined_tables;
    ObSEArray<uint64_t, 4> table_ids;
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_from_item_size(); i++) {
      FromItem &from_item = stmt->get_from_item(i);
      if (!from_item.is_joined_) {
        /*do nothing*/
      } else if (OB_ISNULL(table = stmt->get_joined_table(from_item.table_id_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(table));
      } else if (OB_FAIL(eliminate_outer_join_in_joined_table(stmt,
                                                              table,
                                                              is_non_sens_dul_vals,
                                                              table_ids,
                                                              relation_exprs,
                                                              is_happend,
                                                              trans_tables))) {
        LOG_WARN("failed to eliminate outer join in from items.", K(ret));
      } else if (table->is_joined_table() &&
                 OB_FAIL(joined_tables.push_back(static_cast<JoinedTable*>(table)))) {
        LOG_WARN("failed to push back joined tables", K(ret), K(table));
      } else if (is_happend) {
        from_item.is_joined_ = table->is_joined_table();
        from_item.table_id_ = table->table_id_;
        trans_happened = true;
      }
    }

    if (OB_SUCC(ret) && trans_happened) {
      ObIArray<SemiInfo*> &semi_infos = stmt->get_semi_infos();
      for (int64_t i = 0; OB_SUCC(ret) && i < semi_infos.count(); ++i) {
        if (OB_ISNULL(semi_infos.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("semi info is null", K(ret));
        } else if (OB_FAIL(ObOptimizerUtil::remove_item(semi_infos.at(i)->left_table_ids_,
                                                        table_ids))) {
          LOG_WARN("failed to eliminate outer join in from items.", K(ret));
        } else if (OB_UNLIKELY(semi_infos.at(i)->left_table_ids_.empty())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("left_table_ids_ is empty", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (need_add_limit_constraint &&// todo: need not add??
                 OB_FAIL(ObTransformUtils::add_const_param_constraints(stmt->get_limit_expr(),
                                                                       ctx_))) {
        LOG_WARN("failed to add const param constraints", K(ret));
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
    }
  }
  return ret;
}

int ObTransformJoinElimination::get_eliminable_tables(const ObDMLStmt *stmt,
                                                      const ObIArray<ObRawExpr *> &conds,
                                                      const ObIArray<TableItem *> &candi_tables,
                                                      const ObIArray<TableItem *> &child_candi_tables,
                                                      EliminationHelper &helper)
{
  int ret = OB_SUCCESS;
  ObForeignKeyInfo *foreign_key_info = NULL;
  const uint64_t N_target = candi_tables.count();
  const uint64_t N_other = child_candi_tables.count();
  bool is_first_table_parent = false;
  bool can_be_eliminated = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < N_target; ++i) {
    TableItem *target_table = candi_tables.at(i);
    bool target_be_eliminated = false;
    for (int64_t j = i + 1; OB_SUCC(ret) && j < N_target; ++j) {
      TableItem *source_table = candi_tables.at(j);
      if (OB_FAIL(check_transform_validity_foreign_key(stmt, conds, source_table, target_table,
                                                       can_be_eliminated, is_first_table_parent,
                                                       foreign_key_info))) {
        LOG_WARN("failed to check whether transformation is possible", K(ret));
      } else if (can_be_eliminated) {
        TableItem *parent_table = NULL;
        TableItem *child_table = NULL;
        if (is_first_table_parent) {
          parent_table = source_table;
          child_table = target_table;
        } else {
          parent_table = target_table;
          child_table = source_table;
          target_be_eliminated = true;
        }
        if (ObOptimizerUtil::find_item(helper.parent_table_items_, parent_table)) {
          // 已经有其它表可以消除parent table了
        } else if (OB_FAIL(helper.push_back(child_table, parent_table, foreign_key_info))) {
          LOG_WARN("failed to push back table item or foreign key info", K(ret));
        }
      }
    }
    for (int64_t j = 0; OB_SUCC(ret) && !target_be_eliminated && j < N_other; ++j) {
      TableItem *source_table = child_candi_tables.at(j);
      if (OB_FAIL(check_transform_validity_foreign_key(stmt, conds, source_table, target_table,
                                                      can_be_eliminated, is_first_table_parent,
                                                      foreign_key_info))) {
        LOG_WARN("failed to check whether transformation is possible", K(ret));
      } else if (!can_be_eliminated || is_first_table_parent) {
        // 仅允许消除 candi_tables
      } else if (OB_FAIL(helper.push_back(source_table, target_table, foreign_key_info))) {
        LOG_WARN("failed to push back table item or foreign key info", K(ret));
      } else {
        target_be_eliminated = true;
      }
    }
  }
  return ret;
}

int ObTransformJoinElimination::check_transform_validity_foreign_key(const ObDMLStmt *stmt,
                                                              const ObIArray<ObRawExpr *> &join_conds,
                                                              const TableItem *source_table,
                                                              const TableItem *target_table,
                                                              bool &can_be_eliminated,
                                                              bool &is_first_table_parent,
                                                              ObForeignKeyInfo *&foreign_key_info)
{
  int ret = OB_SUCCESS;
  can_be_eliminated = false;
  bool is_foreign_primary_join = false;
  bool all_primary_key = false;
  bool is_rely_foreign_key = false;
  ObSEArray<ObRawExpr *, 8> source_exprs;
  ObSEArray<ObRawExpr *, 8> target_exprs;
  if ( OB_ISNULL(stmt) || OB_ISNULL(ctx_) || 
      OB_ISNULL(source_table) || OB_ISNULL(target_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parameter have null", K(ctx_), K(source_table), K(target_table), K(ret));
  } else if (source_table->is_basic_table() &&
             target_table->is_basic_table()) {
    OPT_TRACE(source_table, "try eliminate", target_table, "on", join_conds, "with foreign keys");
    if (OB_FAIL(extract_equal_join_columns(join_conds, source_table, target_table,
                                           source_exprs, target_exprs, NULL))) {
      LOG_WARN("failed to extract join columns", K(ret));
    } else if (source_exprs.empty()) {
      /* do nothing */
      OPT_TRACE("no equal join conditions");
    } else if (OB_FAIL(ObTransformUtils::check_foreign_primary_join(source_table,
                                                                    target_table,
                                                                    source_exprs,
                                                                    target_exprs,
                                                                    ctx_->schema_checker_,
                                                                    ctx_->session_info_,
                                                                    is_foreign_primary_join,
                                                                    is_first_table_parent,
                                                                    foreign_key_info))) {
      LOG_WARN("failed to check has foreign key constraint", K(ret));
    } else if (!is_foreign_primary_join) {
      /* do nothing */
      OPT_TRACE("is not foreign primary join");
    } else if (is_first_table_parent && OB_UNLIKELY(!source_table->access_all_part())) {
      /*父表有partition hint，不可消除*/
      /*TODO zhenling.zzg 之后可以完善对于父表、子表均有partition hint的情况*/
      OPT_TRACE("primary key table has partition hint");
    } else if (!is_first_table_parent && OB_UNLIKELY(!target_table->access_all_part())) {
      /*父表有partition hint，不可消除*/
      /*TODO zhenling.zzg 之后可以完善对于父表、子表均有partition hint的情况*/
      OPT_TRACE("primary key table has partition hint");
    } else if (OB_FAIL(ObTransformUtils::is_foreign_key_rely(ctx_->session_info_,
                                                            foreign_key_info,
                                                            is_rely_foreign_key))) {
      LOG_WARN("check foreign key is rely failed", K(ret));
    } else if (!is_rely_foreign_key) {
      /*非可靠主外键关系，不能消除，do nothing*/
      OPT_TRACE("foreign key is not rely");
    } else if (is_first_table_parent
                && OB_FAIL(check_all_column_primary_key(stmt,
                                                        source_table->table_id_,
                                                        foreign_key_info,
                                                        all_primary_key))) {
      LOG_WARN("failed to check all column primary key", K(ret));
    } else if (!is_first_table_parent
                && OB_FAIL(check_all_column_primary_key(stmt,
                                                        target_table->table_id_,
                                                        foreign_key_info,
                                                        all_primary_key))) {
      LOG_WARN("failed to check all column primary key", K(ret));
    } else if (all_primary_key) {
      can_be_eliminated = true;
    } else {
      OPT_TRACE("join condition not use all primary key");
    }
    if (!can_be_eliminated) {
      OPT_TRACE("can not be eliminated");
    }
  }
  return ret;
}

int ObTransformJoinElimination::check_all_column_primary_key(const ObDMLStmt *stmt,
                                                             const uint64_t table_id,
                                                             const ObForeignKeyInfo *info,
                                                             bool &all_primary_key)
{
  int ret = OB_SUCCESS;
  all_primary_key = true;
  if (OB_ISNULL(info) || OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parameters have null", K(ret), K(stmt), K(info));
  } else {
    const ObIArray<ColumnItem> &all_columns = stmt->get_column_items();
    for(int64_t i = 0; all_primary_key && OB_SUCC(ret) && i < all_columns.count(); ++i) {
      const ColumnItem &item = all_columns.at(i);
      if(item.table_id_ == table_id) {
        bool find = false;
        for (int64_t j = 0; !find && j < info->parent_column_ids_.count(); ++j) {
          if(info->parent_column_ids_.at(j) == item.column_id_){
            find = true;
          }
        }
        all_primary_key =  find;
      }
      if (all_primary_key) {
        const ObExprResType *res_type = item.get_column_type();
        if (OB_ISNULL(res_type)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null", K(ret));
        } else {
          // Only replace exprs with bin sort foreign keys
          all_primary_key = !ob_is_string_type(res_type->get_type()) ||
                            ObCharset::is_bin_sort(res_type->get_collation_type());
        }
      }
    }
  }
  return ret;
}

int ObTransformJoinElimination::trans_column_items_foreign_key(ObDMLStmt *stmt,
                                                               const TableItem *child_table,
                                                               const TableItem *parent_table,
                                                               const ObForeignKeyInfo *foreign_key_info)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 16> from_col_exprs;
  ObSEArray<ObRawExpr *, 16> to_col_exprs;
  if (OB_ISNULL(stmt) || OB_ISNULL(child_table) || OB_ISNULL(parent_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parameters", K(ret), K(stmt), K(child_table), K(parent_table));
  } else {
    const uint64_t child_table_id = child_table->table_id_;
    const uint64_t parent_table_id = parent_table->table_id_;
    ObSEArray<ColumnItem, 16> origin_column_items;
    origin_column_items.assign(stmt->get_column_items());
    for (int64_t i = 0; OB_SUCC(ret) && i < origin_column_items.count(); ++i) {
      const ColumnItem &item = origin_column_items.at(i);
      if (parent_table_id == item.table_id_) {
        uint64_t child_column_id;
        // 从ObForeignKeyInfo中，获取parent_column_id对应的child_column_id
        if(OB_FAIL(get_child_column_id_by_parent_column_id(foreign_key_info,
                                                           item.column_id_,
                                                           child_column_id))) {
          LOG_WARN("get child column id by parent column id failed", K(ret));
        } else {
          ColumnItem *child_col = stmt->get_column_item_by_id(child_table_id, child_column_id);
          ColumnItem *parent_col = stmt->get_column_item_by_id(parent_table_id, item.column_id_);
          if (OB_ISNULL(parent_col) || OB_ISNULL(child_col)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("parent column or source column is null", K(ret));
          } else {
            //duplicate column item, to be replaced and remove
            if (OB_FAIL(from_col_exprs.push_back(parent_col->get_expr()))
                || OB_FAIL(to_col_exprs.push_back(child_col->get_expr()))) {
              LOG_WARN("failed to push back expr", K(ret));
            } else if (OB_FAIL(stmt->remove_column_item(item.table_id_,
                                                        item.column_id_))) {
              LOG_WARN("failed to remove column item", K(ret), K(item));
            }
          }
        }
      }
    }
    if (OB_SUCC(ret) && !from_col_exprs.empty()) {
      if (OB_FAIL(stmt->replace_relation_exprs(from_col_exprs, to_col_exprs))) {
        LOG_WARN("failed to replace col exprs", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformJoinElimination::get_child_column_id_by_parent_column_id(const ObForeignKeyInfo *info,
                                                                        const uint64_t parent_column_id,
                                                                        uint64_t &child_column_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("foreign key info should not be null", K(info));
  } else {
    bool find = false;
    for (int64_t i = 0; !find && i < info->parent_column_ids_.count(); ++i) {
      if(parent_column_id == info->parent_column_ids_.at(i)){
        child_column_id = info->child_column_ids_.at(i);
        find = true;
      }
    }
  }
  return ret;
}

int ObTransformJoinElimination::EliminationHelper::is_table_in_child_items(const TableItem *target,
                                                                           bool &find)
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

int ObTransformJoinElimination::eliminate_semi_join_self_foreign_key(ObDMLStmt *stmt,
                                                                     bool &trans_happened,
                                                                     ObIArray<ObSEArray<TableItem *, 4>> &trans_tables)
{
  int ret = OB_SUCCESS;
  ObSEArray<SemiInfo*, 2> semi_infos;
  ObSEArray<ObRawExpr*, 16> candi_conds;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(stmt), K(ret));
  } else if (OB_FAIL(semi_infos.assign(stmt->get_semi_infos()))) {//copy origin semi infos
    LOG_WARN("failed to assign semi infos", K(ret));
  } else if (OB_FAIL(stmt->get_equal_set_conditions(candi_conds, true))) {
    LOG_WARN("failed to get equal set conditions", K(ret));
  } else {
    bool is_happened_self_key = false;
    bool is_happened_foreign_key = false;
    bool is_happened = false;
    SemiInfo *semi_info = NULL;
    bool has_removed_semi_info = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < semi_infos.count(); ++i) {
      if (OB_ISNULL(semi_info = semi_infos.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(semi_info));
      } else if (OB_FAIL(eliminate_semi_join_self_key(stmt,
                                                      semi_info,
                                                      candi_conds,
                                                      is_happened_self_key,
                                                      has_removed_semi_info,
                                                      trans_tables))) {
        LOG_WARN("failed to eliminate semi join self key", K(ret));
      } else if (has_removed_semi_info) {
        trans_happened = true;
      } else if (OB_FAIL(eliminate_semi_join_foreign_key(stmt,
                                                         semi_info,
                                                         is_happened_foreign_key,
                                                         has_removed_semi_info,
                                                         trans_tables))) {
        LOG_WARN("failed to eliminate semi join foreign key", K(ret));
      } else if (has_removed_semi_info) {
        trans_happened = true;
      } else if (!is_happened_self_key && !is_happened_foreign_key) {
        /* do nothing */
      } else if (OB_FAIL(try_remove_semi_info(stmt, semi_info))) {
        LOG_WARN("failed to try remove semi info", K(ret));
      } else {
        trans_happened = true;
      }
    }
  }
  return ret;
}

int ObTransformJoinElimination::eliminate_semi_join_self_key(ObDMLStmt *stmt,
                                                             SemiInfo *semi_info,
                                                             ObIArray<ObRawExpr*> &conds,
                                                             bool &trans_happened,
                                                             bool &has_removed_semi_info,
                                                             ObIArray<ObSEArray<TableItem *, 4>> &trans_tables)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  has_removed_semi_info = false;
  ObSEArray<ObRawExpr*, 4> candi_conds;
  ObSEArray<TableItem*, 16> left_tables;
  ObSEArray<TableItem*, 16> right_tables;
  TableItem *left_table = NULL;
  TableItem *right_table = NULL;
  ObStmtMapInfo stmt_map_info;
  ObSEArray<ObRawExpr*, 4> source_col_exprs;
  ObSEArray<ObRawExpr*, 4> target_col_exprs;
  ObDMLStmt *target_stmt = NULL;
  typedef ObSEArray<ObStmtMapInfo, 8> StmtMapInfoArray;
  SMART_VAR(StmtMapInfoArray, stmt_map_infos) {
    if (OB_ISNULL(stmt) || OB_ISNULL(ctx_) || OB_ISNULL(semi_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret), K(stmt), K(semi_info));
    } else if (OB_FAIL(candi_conds.assign(conds))) {
      LOG_WARN("failed to assign exprs", K(ret));
    } else if (semi_info->is_anti_join() && //conds do not contain anti join condition, here append it
              OB_FAIL(append(candi_conds, semi_info->semi_conditions_))) {
      LOG_WARN("failed to append exprs", K(ret));
    } else if (OB_FAIL(check_transform_validity_semi_self_key(stmt, semi_info, candi_conds,
                                                              left_table, right_table,
                                                              stmt_map_info))) {
      LOG_WARN("failed to check transform validity semi self key", K(ret));
    } else if (NULL != left_table) {
      if (OB_FAIL(ObOptimizerUtil::remove_item(stmt->get_semi_infos(), semi_info))) {
        LOG_WARN("failed to remove item", K(ret));
      } else if (OB_FAIL(construct_eliminated_table(stmt, right_table, trans_tables))) {
        LOG_WARN("failed to construct eliminated table", K(ret));
      } else if (OB_FAIL(trans_semi_condition_exprs(stmt, semi_info))) {
        LOG_WARN("transform anti condition failed", K(ret));
      } else if (OB_FAIL(adjust_table_items(stmt, left_table, right_table,
                                            stmt_map_info))) {
        LOG_WARN("adjust table items failed", K(ret));
      } else if (OB_FAIL(trans_semi_table_item(stmt, right_table))) {
        LOG_WARN("transform semi right table item failed", K(ret));
      } else if (OB_FAIL(append(ctx_->equal_param_constraints_, stmt_map_info.equal_param_map_))) {
        LOG_WARN("failed to append equal params constraints", K(ret));
      } else { 
        has_removed_semi_info = true;
        trans_happened = true;
      }
    } else if (OB_FAIL(check_transform_validity_semi_self_key(stmt, semi_info, conds,
                                                              left_tables, right_tables,
                                                              stmt_map_infos, target_stmt))) {
      LOG_WARN("check whether tranformation is possibl failed", K(ret));
    } else if (stmt_map_infos.empty()) {
      /*do nothing*/
    } else if (OB_FAIL(convert_target_table_column_exprs(stmt, target_stmt, left_tables,
                                                        right_tables, stmt_map_infos,
                                                        source_col_exprs, target_col_exprs))) {
      LOG_WARN("failed to convert parent table column exprs", K(ret));
    } else if (OB_FAIL(eliminate_semi_right_child_table(stmt,
                                                        semi_info,
                                                        right_tables,
                                                        source_col_exprs,
                                                        target_col_exprs,
                                                        trans_tables))) {
      LOG_WARN("failed to eliminate semi right child table", K(ret));
    } else {
      trans_happened = true;
      for (int64_t i = 0; OB_SUCC(ret) && i < stmt_map_infos.count(); ++i) {
        if (OB_FAIL(append(ctx_->equal_param_constraints_,
                           stmt_map_infos.at(i).equal_param_map_))) {
          LOG_WARN("failed to append equal params constraints", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTransformJoinElimination::eliminate_semi_join_foreign_key(ObDMLStmt *stmt,
                                                                SemiInfo *semi_info,
                                                                bool &trans_happened,
                                                                bool &has_removed_semi_info,
                                                                ObIArray<ObSEArray<TableItem *, 4>> &trans_tables)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  has_removed_semi_info = false;
  TableItem *right_table = NULL;
  ObSEArray<TableItem*, 4> left_tables;
  ObSEArray<TableItem*, 4> right_tables;
  ObSEArray<ObForeignKeyInfo*, 2> foreign_key_infos;
  ObSEArray<ObRawExpr*, 4> source_col_exprs;
  ObSEArray<ObRawExpr*, 4> target_col_exprs;
  ObSelectStmt *child_stmt = NULL;
  if (OB_ISNULL(stmt) || OB_ISNULL(semi_info) ||
      OB_ISNULL(right_table = stmt->get_table_item_by_id(semi_info->right_table_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(stmt), K(semi_info), K(right_table));
  } else if (right_table->is_basic_table()) {
    TableItem *left_table = NULL;
    ObForeignKeyInfo *foreign_key_info = NULL;
    if (OB_FAIL(check_transform_validity_semi_foreign_key(stmt, semi_info, right_table, left_table,
                                                          foreign_key_info))) {
      LOG_WARN("check whether tranformation is possibl failed", K(ret));
    } else if (NULL == left_table || NULL == foreign_key_info) {
      /* do nothing */
    } else if (OB_FAIL(construct_eliminated_table(stmt, right_table, trans_tables))) {
      LOG_WARN("failed to construct eliminated table", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::remove_item(stmt->get_semi_infos(), semi_info))) {
      LOG_WARN("failed to remove item", K(ret));
    } else if (OB_FAIL(trans_semi_condition_exprs(stmt, semi_info))) {
      LOG_WARN("transform anti condition failed", K(ret));
    } else if (OB_FAIL(trans_column_items_foreign_key(stmt, left_table,
                                                      right_table,
                                                      foreign_key_info))) {
      LOG_WARN("failed to transform columns of the eliminated table", K(ret));
    } else if (OB_FAIL(trans_semi_table_item(stmt, right_table))) {
      LOG_WARN("transform semi right table item failed", K(ret));
    } else {
      trans_happened = true;
      has_removed_semi_info = true;
    }
  } else if (!right_table->is_generated_table()) {
    /* do nothing */
  } else if (OB_ISNULL(child_stmt = right_table->ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected semi info", K(ret), K(*right_table), K(child_stmt));
  } else if (OB_FAIL(check_transform_validity_semi_foreign_key(stmt, semi_info, right_table,
                                                               left_tables, right_tables,
                                                               foreign_key_infos))) {
    LOG_WARN("check whether tranformation is possibl failed", K(ret));
  } else if (foreign_key_infos.empty()) {
    /*do nothing*/
  } else if (OB_UNLIKELY(foreign_key_infos.count() != left_tables.count() ||
                         foreign_key_infos.count() != right_tables.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected count num", K(ret), K(foreign_key_infos.count()),
                                     K(left_tables.count()), K(right_tables.count()));
  } else if (OB_FAIL(convert_target_table_column_exprs(stmt, child_stmt, left_tables,
                                                       right_tables, foreign_key_infos,
                                                       source_col_exprs, target_col_exprs))) {
    LOG_WARN("failed to convert parent table column exprs", K(ret));
  } else if (OB_FAIL(eliminate_semi_right_child_table(stmt, semi_info, right_tables,
                                                      source_col_exprs, target_col_exprs, trans_tables))) {
    LOG_WARN("failed to eliminate semi right child table", K(ret));
  } else {
    trans_happened = true;
  }
  return ret;
}

int ObTransformJoinElimination::eliminate_semi_right_child_table(ObDMLStmt *stmt,
                                                          SemiInfo *semi_info,
                                                          ObIArray<TableItem*> &right_tables,
                                                          ObIArray<ObRawExpr*> &source_col_exprs,
                                                          ObIArray<ObRawExpr*> &target_col_exprs,
                                                          ObIArray<ObSEArray<TableItem *, 4>> &trans_tables)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> pullup_conds;
  ObSEArray<ObRawExpr*, 4> column_exprs;
  ObSEArray<ObRawExpr*, 4> upper_column_exprs;
  ObSEArray<ObRawExpr*, 4> pullup_column_exprs;
  ObSEArray<ObRawExpr*, 4> pullup_select_exprs;
  TableItem *semi_right_table = NULL;
  ObSelectStmt *child_stmt = NULL;
  ObSqlBitSet<> right_rel_ids;
  ObSEArray<uint64_t, 4> right_table_ids;
  if (OB_ISNULL(stmt) || OB_ISNULL(semi_info) || OB_ISNULL(ctx_) ||
      OB_ISNULL(ctx_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(stmt), K(semi_info));
  } else if (OB_ISNULL(semi_right_table = stmt->get_table_item_by_id(semi_info->right_table_id_))
             || OB_UNLIKELY(!semi_right_table->is_generated_table())
             || OB_ISNULL(child_stmt = semi_right_table->ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected semi info", K(ret), K(semi_right_table), K(child_stmt));
  } else if (OB_FAIL(construct_eliminated_tables(child_stmt, right_tables, trans_tables))) {
    LOG_WARN("failed to construct eliminated tables", K(ret));
  } else if (OB_FAIL(child_stmt->get_table_rel_ids(right_tables, right_rel_ids))) {
    LOG_WARN("failed to get rel ids", K(ret));
  } else if (OB_FAIL(child_stmt->relids_to_table_ids(right_rel_ids, right_table_ids))) {
    LOG_WARN("failed to do relids to table ids", K(ret));
  } else if (OB_FAIL(child_stmt->remove_table_item(right_tables))) {
    LOG_WARN("failed to remove table items.", K(ret));
  } else if (OB_FAIL(child_stmt->remove_from_item(right_tables))) {
    LOG_WARN("failed to remove assoicated from item", K(ret));
  } else if (OB_FAIL(child_stmt->remove_part_expr_items(right_table_ids))) {
    LOG_WARN("failed to remove part expr item", K(ret));
  } else if (OB_FAIL(child_stmt->remove_column_item(right_table_ids))) {
    LOG_WARN("failed to remove column items.", K(ret));
  } else {
    // 1. pullup select exprs contains right_tables column exprs
    ObIArray<SelectItem> &select_items = child_stmt->get_select_items();
    ObSqlBitSet<> removed_idxs;
    ObRawExpr *expr = NULL;
    ObRawExpr *col = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < select_items.count(); ++i) {
      if (OB_ISNULL(expr = select_items.at(i).expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret), K(expr));
      } else if (!expr->get_relation_ids().overlap(right_rel_ids)) {
        /* do nothing */
      } else if (OB_FAIL(removed_idxs.add_member(i))) {
        LOG_WARN("failed to add member", K(ret));
      } else if (OB_ISNULL(col = stmt->get_column_expr_by_id(semi_right_table->table_id_,
                                                             i + OB_APP_MIN_COLUMN_ID))) {
        LOG_WARN("failed to get column expr by id", K(ret));
      } else if (OB_FAIL(pullup_column_exprs.push_back(col))) {
        LOG_WARN("failed to push back column expr", K(ret));
      } else if (OB_FAIL(pullup_select_exprs.push_back(expr))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObTransformUtils::remove_select_items(ctx_, semi_right_table->table_id_,
                                                             *child_stmt, *stmt, removed_idxs))) {
      LOG_WARN("failed to remove select items", K(ret));
    } else if (OB_FAIL(ObTransformUtils::replace_exprs(pullup_column_exprs,
                                                       pullup_select_exprs,
                                                       semi_info->semi_conditions_))) {
      LOG_WARN("failed to update semi condition", K(ret));
    }
  }

  // 2. pullup where condition to semi condition
  if (OB_SUCC(ret)) {
    ObIArray<ObRawExpr*> &cond_exprs = child_stmt->get_condition_exprs();
    ObSEArray<ObRawExpr*, 4> new_cond_epxrs;
    ObRawExpr *expr = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < cond_exprs.count(); ++i) {
      if (OB_ISNULL(expr = cond_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret), K(expr));
      } else if (!expr->get_relation_ids().overlap(right_rel_ids)) {
        ret = new_cond_epxrs.push_back(expr);
      } else if (OB_FAIL(pullup_conds.push_back(expr))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(append(semi_info->semi_conditions_, pullup_conds))) {
      LOG_WARN("failed to append", K(ret));
    } else if (OB_FAIL(child_stmt->get_condition_exprs().assign(new_cond_epxrs))) {
      LOG_WARN("failed to push assign", K(ret));
    }
  }

  // 3. add new select expr & column expr, repalce column exprs in semi_info->semi_conditions_
  if (OB_SUCC(ret)) {
    ObSqlBitSet<> table_set;
    ObSEArray<ObRawExpr *, 4> tmp;
    ObSEArray<ObRawExpr*, 4> pullup_exprs;
    ObSEArray<ObQueryRefRawExpr*, 2> pullup_subqueries;
    if (OB_FAIL(child_stmt->get_table_rel_ids(child_stmt->get_table_items(), table_set))) {
      LOG_WARN("failed to get rel ids", K(ret));
    } else if (OB_FAIL(table_set.del_members(right_rel_ids))) {
      LOG_WARN("failed to del members", K(ret));
    } else if (OB_FAIL(append(pullup_exprs, pullup_select_exprs)) ||
               OB_FAIL(append(pullup_exprs, pullup_conds))) {
      LOG_WARN("failed to append exprs", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(pullup_exprs, tmp))) {
      LOG_WARN("extract column exprs failed", K(ret));
    } else if (OB_FAIL(ObTransformUtils::extract_table_exprs(*child_stmt,
                                                             tmp,
                                                             table_set,
                                                             column_exprs))) {
      LOG_WARN("failed to extract table exprs", K(ret));
    } else if (OB_FAIL(ObTransformUtils::create_columns_for_view(ctx_, *semi_right_table, stmt,
                                                                 column_exprs,
                                                                 upper_column_exprs))) {
      LOG_WARN("failed to create columns for view", K(ret));
    } else if (OB_FAIL(ObTransformUtils::extract_query_ref_expr(pullup_exprs, pullup_subqueries))) {
      LOG_WARN("failed to adjust subquery list", K(ret));
    } else if (OB_FAIL(append_array_no_dup(stmt->get_subquery_exprs(), pullup_subqueries))) {
      LOG_WARN("failed to append array no dup", K(ret));
    } else if (OB_FAIL(child_stmt->adjust_subquery_list())) {
      LOG_WARN("failed to adjust subquery list", K(ret));
    }
  }

  // 4. remove right_tables
  if (OB_SUCC(ret)) {
    if (OB_FAIL(append(column_exprs, target_col_exprs)) ||
        OB_FAIL(append(upper_column_exprs, source_col_exprs))) {
      LOG_WARN("failed to append exprs", K(ret));
    } else if (OB_FAIL(stmt->remove_table_item(semi_right_table))) {
      LOG_WARN("failed to remove table item", K(ret));
    } else if (OB_FAIL(stmt->replace_relation_exprs(column_exprs, upper_column_exprs))) {
      LOG_WARN("failed to replace inner stmt expr", K(ret));
    } else if (OB_FAIL(stmt->get_table_items().push_back(semi_right_table))) {
      LOG_WARN("failed to push back table item", K(ret));
    } else if (OB_FAIL(child_stmt->rebuild_tables_hash())) {
      LOG_WARN("rebuild table hash failed", K(ret));
    } else if (OB_FAIL(child_stmt->update_column_item_rel_id())) {
      LOG_WARN("failed to update columns' relation id", K(ret));
    } else if (OB_FAIL(stmt->update_column_item_rel_id())) {
      LOG_WARN("failed to update columns' relation id", K(ret));
    } else if (OB_FAIL(stmt->formalize_stmt(ctx_->session_info_))) {
      LOG_WARN("formalize stmt is failed", K(ret));
    }
  }
  return ret;
}

int ObTransformJoinElimination::adjust_source_table(ObDMLStmt *source_stmt,
                                                    ObDMLStmt *target_stmt,
                                                    const TableItem *source_table,
                                                    const TableItem *target_table,
                                                    const ObIArray<int64_t> *output_map,
                                                    ObIArray<ObRawExpr*> &source_col_exprs,
                                                    ObIArray<ObRawExpr*> &target_col_exprs)
{
  int ret = OB_SUCCESS;
  ObSEArray<ColumnItem, 16> target_column_items;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_) || OB_ISNULL(source_stmt) ||
      OB_ISNULL(target_stmt) || OB_ISNULL(source_table) || OB_ISNULL(target_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parameters", K(ret), K(ctx_), K(source_stmt), K(target_stmt),
                                   K(source_table), K(target_table));
  } else if (OB_FAIL(target_stmt->get_column_items(target_table->table_id_,
                                                   target_column_items))) {
    LOG_WARN("failed to get column items", K(ret));
  } else {
    uint64_t source_table_id = source_table->table_id_;
    uint64_t target_table_id = target_table->table_id_;
    ColumnItem new_col;
    ObColumnRefRawExpr *col_expr = NULL;
    ObRawExprCopier expr_copier(*ctx_->expr_factory_);
    for (int64_t i = 0; OB_SUCC(ret) && i < target_column_items.count(); ++i) {
      ColumnItem *target_col = NULL;
      ColumnItem *source_col = NULL;
      uint64_t column_id = OB_INVALID_ID;
      if (OB_ISNULL(target_col = target_stmt->get_column_item_by_id(target_table_id,
                                                  target_column_items.at(i).column_id_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (NULL == output_map) {
        column_id = target_col->column_id_;
        source_col = source_stmt->get_column_item_by_id(source_table_id, column_id);
      } else {
        // generated table with output map
        int64_t output_id = target_col->column_id_ - OB_APP_MIN_COLUMN_ID;
        if (OB_UNLIKELY(output_id < 0 || output_id >= output_map->count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected array count", K(output_id),
              K(output_map->count()), K(ret));
        } else if (OB_UNLIKELY(OB_INVALID_ID == output_map->at(output_id))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid output idx", K(output_id), K(ret));
        } else {
          column_id = output_map->at(output_id) + OB_APP_MIN_COLUMN_ID;
          source_col = source_stmt->get_column_item_by_id(source_table_id, column_id);
        }
      }
      if (OB_FAIL(ret) || NULL != source_col) { // add new column to source_stmt
      } else if (OB_FAIL(new_col.deep_copy(expr_copier, *target_col))) {
        LOG_WARN("failed to deep copy column item", K(ret));
      } else if (OB_ISNULL(col_expr = new_col.get_expr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else {
        source_col = &new_col;
        new_col.table_id_ = source_table_id;
        new_col.column_id_ = column_id;
        col_expr->set_ref_id(source_table_id, column_id);
        col_expr->set_table_name(source_table->get_table_name());
        col_expr->get_relation_ids().reuse();
        int64_t rel_id = source_stmt->get_table_bit_index(source_table_id);
        if (OB_UNLIKELY(rel_id <= 0 || rel_id > source_stmt->get_table_items().count())) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid argument", K(ret), K(rel_id));
        } else if (OB_FAIL(col_expr->add_relation_id(rel_id))) {
          LOG_WARN("fail to add relation id", K(rel_id), K(ret));
        } else if (OB_FAIL(col_expr->pull_relation_id())) {
          LOG_WARN("failed to pull relation id and levels");
        } else if (OB_FAIL(col_expr->formalize(ctx_->session_info_))) {
          LOG_WARN("failed to formalize a new expr", K(ret));
        } else if (OB_FAIL(source_stmt->add_column_item(new_col))) {
          LOG_WARN("failed to add column item", K(new_col), K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(source_col) || OB_ISNULL(target_col)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(source_col_exprs.push_back(source_col->get_expr()))) {
        LOG_WARN("failed to push back epxr", K(ret));
      } else if (OB_FAIL(target_col_exprs.push_back(target_col->get_expr()))) {
        LOG_WARN("failed to push back expr", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformJoinElimination::convert_target_table_column_exprs(ObDMLStmt *source_stmt,
                                              ObDMLStmt *target_stmt,
                                              const ObIArray<TableItem*> &source_tables,
                                              const ObIArray<TableItem*> &target_tables,
                                              ObIArray<ObStmtMapInfo> &stmt_map_infos,
                                              ObIArray<ObRawExpr*> &source_col_exprs,
                                              ObIArray<ObRawExpr*> &target_col_exprs)
{
  int ret = OB_SUCCESS;
  source_col_exprs.reuse();
  target_col_exprs.reuse();
  TableItem *source_table = NULL;
  TableItem *target_table = NULL;
  if (OB_ISNULL(source_stmt) || OB_ISNULL(target_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parameters", K(ret), K(source_stmt), K(target_stmt));
  } else if (OB_UNLIKELY(stmt_map_infos.count() != source_tables.count() ||
                         stmt_map_infos.count() != target_tables.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected count num", K(ret), K(stmt_map_infos.count()),
                                     K(source_tables.count()), K(target_tables.count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt_map_infos.count(); ++i) {
    if (OB_ISNULL(source_table = source_tables.at(i))
        || OB_ISNULL(target_table = target_tables.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected count num", K(ret));
    } else if ((source_table->is_basic_table() &&
               target_table->is_basic_table()) ||
               (source_table->is_temp_table() &&
               target_table->is_temp_table())) {
      ret = adjust_source_table(source_stmt, target_stmt, source_table, target_table,
                                NULL, source_col_exprs, target_col_exprs);
    } else if (!source_table->is_generated_table() || !target_table->is_generated_table()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected table", K(ret), K(*source_table), K(*target_table));
    } else {
      ObSelectStmt *source_ref_query = NULL;
      ObSelectStmt *target_ref_query = NULL;
      ObSEArray<int64_t, 16> column_map;
      if (OB_ISNULL(source_ref_query = source_table->ref_query_) ||
          OB_ISNULL(target_ref_query = target_table->ref_query_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(source_ref_query), K(target_ref_query), K(ret));
      } else if (OB_FAIL(reverse_select_items_map(source_ref_query, target_ref_query,
                                                  stmt_map_infos.at(i).select_item_map_,
                                                  column_map))) {
        LOG_WARN("failed to reverse select items map", K(ret));
      } else if (OB_FAIL(create_missing_select_items(source_ref_query, target_ref_query,
                                                     column_map,
                                                     stmt_map_infos.at(i).table_map_))) {
      } else if (OB_FAIL(adjust_source_table(source_stmt, target_stmt, source_table, target_table,
                                             &column_map, source_col_exprs, target_col_exprs))) {
        LOG_WARN("failed to merge table items", K(ret));
        LOG_WARN("failed to create missing select items", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformJoinElimination::convert_target_table_column_exprs(ObDMLStmt *source_stmt,
                                              ObDMLStmt *target_stmt,
                                              const ObIArray<TableItem*> &child_tables,
                                              const ObIArray<TableItem*> &parent_tables,
                                              const ObIArray<ObForeignKeyInfo*> &foreign_key_infos,
                                              ObIArray<ObRawExpr*> &from_col_exprs,
                                              ObIArray<ObRawExpr*> &to_col_exprs)
{
  int ret = OB_SUCCESS;
  ObSqlBitSet<> parent_rel_ids;
  ObSEArray<uint64_t, 16> parent_table_ids;
  from_col_exprs.reuse();
  to_col_exprs.reuse();
  if (OB_ISNULL(source_stmt) || OB_ISNULL(target_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parameters", K(ret), K(source_stmt), K(target_stmt));
  } else if (OB_UNLIKELY(foreign_key_infos.count() != child_tables.count() ||
                         foreign_key_infos.count() != parent_tables.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected count num", K(ret), K(foreign_key_infos.count()),
                                     K(child_tables.count()), K(parent_tables.count()));
  } else if (OB_FAIL(target_stmt->get_table_rel_ids(parent_tables, parent_rel_ids))) {
    LOG_WARN("failed to get rel ids", K(ret));
  } else if (OB_FAIL(target_stmt->relids_to_table_ids(parent_rel_ids, parent_table_ids))) {
    LOG_WARN("failed to do relids to table ids", K(ret));
  } else {
    int64_t idx = OB_INVALID_INDEX;
    ObRawExpr* child_col = NULL;
    ObRawExpr* parent_col = NULL;
    ObIArray<ColumnItem> &origin_column_items = target_stmt->get_column_items();
    for (int64_t i = 0; OB_SUCC(ret) && i < parent_tables.count(); ++i) {
      if (OB_ISNULL(parent_tables.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret));
      } else if (OB_FAIL(parent_table_ids.push_back(parent_tables.at(i)->table_id_))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < origin_column_items.count(); ++i) {
      const ColumnItem &item = origin_column_items.at(i);
      uint64_t child_column_id;
      if (!ObOptimizerUtil::find_item(parent_table_ids, item.table_id_, &idx)) {
        /* do nothing */
      } else if (OB_UNLIKELY(idx < 0 || idx >= child_tables.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected idx", K(ret), K(parent_table_ids), K(item.table_id_));
      } else if(OB_FAIL(get_child_column_id_by_parent_column_id(foreign_key_infos.at(idx),
                                                                item.column_id_,
                                                                child_column_id))) {
          LOG_WARN("get child column id by parent column id failed", K(ret));
      } else if (OB_ISNULL(parent_col = item.expr_) || OB_ISNULL(child_tables.at(idx)) ||
                 OB_ISNULL(child_col = source_stmt->get_column_expr_by_id(
                                            child_tables.at(idx)->table_id_, child_column_id))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("parent column or source column is null", K(ret));
      } else if (OB_FAIL(from_col_exprs.push_back(parent_col))
                 || OB_FAIL(to_col_exprs.push_back(child_col))) {
        LOG_WARN("failed to push back expr", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformJoinElimination::try_remove_semi_info(ObDMLStmt *stmt,
                                                     SemiInfo *semi_info)
{
  int ret = OB_SUCCESS;
  TableItem *right_table = NULL;
  ObSelectStmt *child_stmt = NULL;
  ObSEArray<ObRawExpr*, 16> column_exprs;
  ObSEArray<ObRawExpr*, 16> child_select_exprs;
  if (OB_ISNULL(stmt) || OB_ISNULL(semi_info) || OB_ISNULL(ctx_) ||
      OB_ISNULL(right_table = stmt->get_table_item_by_id(semi_info->right_table_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexcepted null", K(ret), K(stmt), K(semi_info),
                                    K(ctx_), K(right_table));
  } else if (!right_table->is_generated_table()) {
    /* do nothing */
  } else if (OB_ISNULL(child_stmt = right_table->ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexcepted null", K(ret), K(child_stmt));
  } else if (!child_stmt->get_from_items().empty()) {
    /* do nothing */
  } else if (OB_UNLIKELY(!child_stmt->get_column_items().empty() ||
                         !child_stmt->get_table_items().empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexcepted child stmt", K(ret), K(*child_stmt));
  } else if (OB_FAIL(stmt->get_column_exprs(right_table->table_id_, column_exprs))) {
    LOG_WARN("failed to get column exprs", K(ret));
  } else if (OB_FAIL(ObTransformUtils::convert_column_expr_to_select_expr(column_exprs, *child_stmt,
                                                                          child_select_exprs))) {
    LOG_WARN("failed to convert column expr to select expr", K(ret));
  } else if (OB_FAIL(ObTransformUtils::replace_exprs(column_exprs, child_select_exprs,
                                                     semi_info->semi_conditions_))) {
    LOG_WARN("failed to replace expr", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::remove_item(stmt->get_semi_infos(), semi_info))) {
    LOG_WARN("failed to remove item", K(ret));
  } else if (OB_FAIL(ObTransformUtils::extract_query_ref_expr(child_stmt->get_condition_exprs(),
                                                              stmt->get_subquery_exprs()))) {
    LOG_WARN("failed to adjust subquery list", K(ret));
  } else if (OB_FAIL(append(semi_info->semi_conditions_, child_stmt->get_condition_exprs()))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else if (OB_FAIL(trans_semi_condition_exprs(stmt, semi_info))) {
    LOG_WARN("transform anti condition failed", K(ret));
  } else if (OB_FAIL(stmt->remove_table_item(right_table))) {
    LOG_WARN("failed to remove target table items", K(ret));
  } else if (OB_FAIL(stmt->remove_column_item(right_table->table_id_))) {
    LOG_WARN("failed to remove column items.", K(ret));
  } else if (OB_FAIL(stmt->remove_part_expr_items(right_table->table_id_))) {
    LOG_WARN("failed to remove part expr item", K(ret));
  } else if (OB_FAIL(stmt->rebuild_tables_hash())) {
    LOG_WARN("rebuild table hash failed", K(ret));
  } else if (OB_FAIL(stmt->update_column_item_rel_id())) {
    LOG_WARN("failed to update columns' relation id", K(ret));
  } else if (OB_FAIL(stmt->formalize_stmt(ctx_->session_info_))) {
    LOG_WARN("formalize stmt is failed", K(ret));
  }
  return ret;
}

int ObTransformJoinElimination::check_transform_validity_semi_self_key(ObDMLStmt *stmt,
                                                                       SemiInfo *semi_info,
                                                                       ObIArray<ObRawExpr*> &candi_conds,
                                                                       TableItem *&source_table,
                                                                       TableItem *&right_table,
                                                                       ObStmtMapInfo &stmt_map_info)
{
  int ret = OB_SUCCESS;
  source_table = NULL;
  right_table = NULL;
  bool is_contain = false;
  bool source_unique = false;
  bool target_unique = false;
  bool is_simple_join_condition = false;
  bool right_tables_have_filter = false;
  bool is_simple_filter = false;
  bool is_hint_valid = false;
  ObSEArray<ObRawExpr*, 16> source_exprs;
  ObSEArray<ObRawExpr*, 16> target_exprs;
  ObSEArray<TableItem*, 4> left_tables;
  TableItem *left_table = NULL;
  ObSEArray<ObRawExpr*, 16> dummy_exprs;
  if (OB_ISNULL(stmt) || OB_ISNULL(semi_info)) {
    LOG_WARN("get unexcepted null", K(ret), K(stmt), K(semi_info));
  } else if (OB_ISNULL(right_table = stmt->get_table_item_by_id(semi_info->right_table_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected semi right table", K(ret), K(*semi_info));
  } else if (OB_FAIL(check_hint_valid(*stmt, *right_table, is_hint_valid))) {
    LOG_WARN("failed to check hint valid", K(ret));
  } else if (!is_hint_valid) {
    // do nothing
  } else if (OB_FAIL(stmt->get_table_item_by_id(semi_info->left_table_ids_ , left_tables))) {
    LOG_WARN("failed to get table items", K(ret));
  } else {
    OPT_TRACE("try eliminate semi join", right_table, "with self key");
    for (int64_t i = 0; OB_SUCC(ret) && NULL == source_table && i < left_tables.count(); ++i) {
      source_exprs.reuse();
      target_exprs.reuse();
      stmt_map_info.reset();
      if (OB_ISNULL(left_table = left_tables.at(i))) {
        LOG_WARN("get unexcepted null", K(ret), K(left_table));
      } else if (OB_FAIL(ObTransformUtils::check_table_item_containment(stmt, left_table, stmt,
                                                                        right_table, stmt_map_info,
                                                                        is_contain))) {
        LOG_WARN("check table item containment failed", K(ret));
      } else if (!is_contain) {
        /*do nothing*/
        OPT_TRACE(left_table, "not contain", right_table);
      } else if(OB_FAIL(check_semi_join_condition(stmt, semi_info->semi_conditions_,
                                                  left_table, right_table,
                                                  stmt_map_info, source_exprs, target_exprs,
                                                  is_simple_join_condition,
                                                  right_tables_have_filter,
                                                  is_simple_filter))) {
        LOG_WARN("check semi join condition failed", K(ret));
      } else if (source_exprs.empty() && semi_info->is_anti_join()) {
        /*do nothing*/
        OPT_TRACE("anti join not have same join keys");
      } else if (is_simple_join_condition && !right_tables_have_filter) {
        /* if all semi join conditions are simple condition and there is no right table filter,
          can eliminate right table without checking unique. */
        source_table = left_table;
        OPT_TRACE("is simply equal join condition and right table do not have filter, semi join will be eliminated");
        LOG_TRACE("succeed to check loseless semi join", K(is_simple_join_condition),
                                                        K(right_tables_have_filter));
      } else if (semi_info->is_anti_join() && !(is_simple_join_condition && is_simple_filter)) {
        /* for anti join, all join conditions should be simple condition
            and all right filter should be simple filter */
        OPT_TRACE("anti join`s join condition is not simply or right table`s filter is not simple");
      } else if (OB_FAIL(ObTransformUtils::check_exprs_unique_on_table_items(stmt,
                                                          ctx_->session_info_, ctx_->schema_checker_,
                                                          left_table, source_exprs, candi_conds,
                                                          false, source_unique))) {
        LOG_WARN("check expr unique in semi left tables failed", K(ret));
      } else if (OB_FAIL(ObTransformUtils::check_exprs_unique_on_table_items(stmt,
                                                ctx_->session_info_, ctx_->schema_checker_,
                                                right_table, target_exprs,
                                                semi_info->is_anti_join() ? dummy_exprs : candi_conds,
                                                false, target_unique))) {
        LOG_WARN("check expr unique in semi right tables failed", K(ret));
      } else {
        source_table = source_unique && target_unique ? left_table : NULL;
        LOG_TRACE("succeed to check loseless semi join", K(source_unique), K(target_unique));
      }
    }
  }
  return ret;
}


int ObTransformJoinElimination::check_transform_validity_semi_self_key(ObDMLStmt *stmt,
                                                          SemiInfo *semi_info,
                                                          ObIArray<ObRawExpr*> &candi_conds,
                                                          ObIArray<TableItem*> &left_tables,
                                                          ObIArray<TableItem*> &right_tables,
                                                          ObIArray<ObStmtMapInfo> &stmt_map_infos,
                                                          ObDMLStmt *&target_stmt)
{
  int ret = OB_SUCCESS;
  target_stmt = NULL;
  TableItem *semi_right_table = NULL;
  ObSEArray<ObRawExpr*, 16> right_conds;
  ObSEArray<TableItem*, 4> all_left_tables;
  ObSEArray<ObSqlBitSet<>, 4> select_relids;
  ObSEArray<ObRawExpr*, 16> dummy_exprs;
  if (OB_ISNULL(stmt) || OB_ISNULL(semi_info) || OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexcepted null", K(ret));
  } else if (OB_ISNULL(semi_right_table = stmt->get_table_item_by_id(semi_info->right_table_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected semi right table", K(ret), K(*semi_info));
  } else if (!semi_right_table->is_generated_table()) {
    /*do nothing*/
  } else if (OB_ISNULL(semi_right_table->ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexcepted null", K(ret));
  } else if (!semi_right_table->ref_query_->is_spj()) {
    /*do nothing*/
  } else if (OB_FAIL(stmt->get_table_item_by_id(semi_info->left_table_ids_ , all_left_tables))) {
    LOG_WARN("failed to get table items", K(ret));
  } else if (OB_FAIL(get_epxrs_rel_ids_in_child_stmt(stmt, semi_right_table->ref_query_,
                                                     semi_info->semi_conditions_,
                                                     semi_info->right_table_id_, select_relids))) {
    LOG_WARN("failed to get expr rel ids", K(ret));
  } else if (OB_FAIL(semi_right_table->ref_query_->get_equal_set_conditions(right_conds, true))) {
    LOG_WARN("failed to extract table exprs", K(ret));
  } else {
    target_stmt = semi_right_table->ref_query_;
    ObStmtMapInfo stmt_map_info;
    bool is_contain = false;
    bool source_unique = false;
    bool target_unique = false;
    bool is_simple_join_condition = false;
    bool right_tables_have_filter = false;
    bool is_simple_filter = false;
    ObSEArray<ObRawExpr*, 16> source_exprs;
    ObSEArray<ObRawExpr*, 16> target_exprs;
    TableItem *left_table = NULL;
    TableItem *right_table = NULL;
    bool can_be_eliminated = false;
    bool is_hint_valid = false;
    bool used = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < target_stmt->get_from_item_size(); ++i) {
      FromItem item = target_stmt->get_from_item(i);
      can_be_eliminated = false;
      if (item.is_joined_ || target_stmt->is_semi_left_table(item.table_id_)) {
        /* do nothing */
      } else if (OB_ISNULL(right_table = target_stmt->get_table_item_by_id(item.table_id_))) {
        LOG_WARN("get unexcepted null", K(ret), K(right_table));
      } else if (OB_FAIL(check_hint_valid(*stmt, *right_table, is_hint_valid))) {
        LOG_WARN("failed to check hint valid", K(ret));
      } else if (!is_hint_valid) {
        /* do nothing */
      } else if (OB_FAIL(is_table_column_used_in_subquery(*semi_right_table->ref_query_,
                                                          item.table_id_, used))) {
        LOG_WARN("failed to check is table column used in subquery", K(ret));
      } else if (used) {
        /* do nothing */
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && !can_be_eliminated
                                         && j < all_left_tables.count(); ++j) {
          source_exprs.reuse();
          target_exprs.reuse();
          stmt_map_info.reset();
          if (OB_ISNULL(left_table = all_left_tables.at(j))) {
            LOG_WARN("get unexcepted null", K(ret), K(left_table));
          } else if (OB_FAIL(ObTransformUtils::check_table_item_containment(stmt,
                                                                            left_table,
                                                                            target_stmt,
                                                                            right_table,
                                                                            stmt_map_info,
                                                                            is_contain))) {
            LOG_WARN("check table item containment failed", K(ret));
          } else if (!is_contain) {
            /*do nothing*/
          } else if(OB_FAIL(check_semi_join_condition(stmt, semi_right_table->ref_query_,
                                                      semi_info->semi_conditions_, select_relids,
                                                      left_table, right_table, stmt_map_info,
                                                      source_exprs, target_exprs,
                                                      is_simple_join_condition,
                                                      right_tables_have_filter,
                                                      is_simple_filter))) {
            LOG_WARN("check semi join condition failed", K(ret));
          } else if (is_simple_join_condition && !right_tables_have_filter) {
            /* if all semi join conditions are simple condition and there is no right table filter,
                can eliminate right table without checking unique. */
            can_be_eliminated = true;
            LOG_TRACE("succeed to check validity semi self key", K(is_simple_join_condition),
                                                              K(right_tables_have_filter));
          } else if (semi_info->is_anti_join() && !(is_simple_join_condition && is_simple_filter)) {
            /* for anti join, all join conditions should be simple condition
                and all right filter should be simple filter */
          } else if (OB_FAIL(ObTransformUtils::check_exprs_unique_on_table_items(stmt,
                                                        ctx_->session_info_, ctx_->schema_checker_,
                                                        left_table, source_exprs, candi_conds,
                                                        false, source_unique))) {
            LOG_WARN("check expr unique in semi left tables failed", K(ret));
          } else if (OB_FAIL(ObTransformUtils::check_exprs_unique_on_table_items(target_stmt,
                                            ctx_->session_info_, ctx_->schema_checker_,
                                            right_table, target_exprs,
                                            semi_info->is_anti_join() ? dummy_exprs : right_conds,
                                            false, target_unique))) {
            LOG_WARN("check expr unique in semi right tables failed", K(ret));
          } else {
            can_be_eliminated = source_unique && target_unique;
            LOG_TRACE("succeed to check validity semi self key", K(source_unique),
                                                                 K(target_unique));
          }
        }
      }
      if (OB_SUCC(ret) && can_be_eliminated) {
        if (OB_FAIL(stmt_map_infos.push_back(stmt_map_info)) ||
            OB_FAIL(left_tables.push_back(left_table)) ||
            OB_FAIL(right_tables.push_back(right_table))) {
          LOG_WARN("failed to push back", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTransformJoinElimination::is_table_column_used_in_subquery(const ObSelectStmt &stmt,
                                                                 const uint64_t table_id,
                                                                 bool &used)
{
  int ret = OB_SUCCESS;
  used = false;
  const ObRawExpr *expr = NULL;
  int64_t table_idx = stmt.get_table_bit_index(table_id);
  for (int64_t i = 0; OB_SUCC(ret) && !used && i < stmt.get_select_item_size(); ++i) {
    if (OB_ISNULL(expr = stmt.get_select_item(i).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else if (expr->has_flag(CNT_SUB_QUERY)) {
      used = expr->get_relation_ids().has_member(table_idx);
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && !used && i < stmt.get_condition_size(); ++i) {
    if (OB_ISNULL(expr = stmt.get_condition_expr(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else if (expr->has_flag(CNT_SUB_QUERY)) {
      used = expr->get_relation_ids().has_member(table_idx);
    }
  }
  return ret;
}

// source_table and target_table come from the same stmt
int ObTransformJoinElimination::check_semi_join_condition(ObDMLStmt *stmt,
                                                          ObIArray<ObRawExpr*> &semi_conds,
                                                          const TableItem *source_table,
                                                          const TableItem *target_table,
                                                          const ObStmtMapInfo &stmt_map_info,
                                                          ObIArray<ObRawExpr*> &source_exprs,
                                                          ObIArray<ObRawExpr*> &target_exprs,
                                                          bool &is_simple_join_condition,
                                                          bool &target_tables_have_filter,
                                                          bool &is_simple_filter)
{
  int ret = OB_SUCCESS;
  is_simple_join_condition = true;
  target_tables_have_filter = false;
  is_simple_filter = true;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param has null", K(ret));
  } else {
    ObRawExpr *expr = NULL;
    ObOpRawExpr *op = NULL;
    int64_t left_idx = stmt->get_table_bit_index(source_table->table_id_);
    int64_t right_idx = stmt->get_table_bit_index(target_table->table_id_);
    for (int64_t i = 0; OB_SUCC(ret) && i < semi_conds.count(); ++i) {
      if (OB_ISNULL(expr = semi_conds.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null", K(ret));
      } else if (!expr->get_relation_ids().has_member(right_idx)) {
        /* do nohing */
      } else if (!expr->get_relation_ids().has_member(left_idx)) {
        target_tables_have_filter = true;
        if (T_OP_OR == expr->get_expr_type()) { // complex right table filter
          is_simple_filter = false;
        } else { /*do nothing*/ }
      } else if (T_OP_EQ != expr->get_expr_type()) {
        is_simple_join_condition = false;
      } else if (OB_FALSE_IT(op = static_cast<ObOpRawExpr *>(expr))) {
      } else if (OB_ISNULL(op->get_param_expr(0)) || OB_ISNULL(op->get_param_expr(1))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexcept null param", K(ret));
      } else if (!op->get_param_expr(0)->has_flag(IS_COLUMN)
                 || !op->get_param_expr(1)->has_flag(IS_COLUMN)) {
        is_simple_join_condition = false;
      } else {
        ObColumnRefRawExpr *col1 = static_cast<ObColumnRefRawExpr*>(op->get_param_expr(0));
        ObColumnRefRawExpr *col2 = static_cast<ObColumnRefRawExpr*>(op->get_param_expr(1));
        bool is_equal = false;
        if (col2->get_table_id() == source_table->table_id_) {
          ObColumnRefRawExpr *col = col1;
          col1 = col2;
          col2 = col;
        }
        if (OB_FAIL(is_equal_column(source_table, target_table, stmt_map_info.select_item_map_,
                                    col1->get_column_id(), col2->get_column_id(),
                                    is_equal))) {
          LOG_WARN("check column ref table id is equal failed", K(ret));
        } else if (!is_equal) { /*非相同列的等式*/
          is_simple_join_condition = false;
        } else  if (OB_FAIL(source_exprs.push_back(col1))) {
          LOG_WARN("push back column expr failed", K(ret));
        } else if (OB_FAIL(target_exprs.push_back(col2))) {
          LOG_WARN("push back column expr failed", K(ret));
        } else {/*do nothing*/}
      }
    }
  }
  return ret;
}

// source_table comes from stmt, target_table comes from child select stmt target_stmt
int ObTransformJoinElimination::check_semi_join_condition(ObDMLStmt *stmt,
                                                          ObSelectStmt *target_stmt,
                                                          ObIArray<ObRawExpr *> &semi_conds,
                                                          ObIArray<ObSqlBitSet<>> &select_relids,
                                                          const TableItem *source_table,
                                                          const TableItem *target_table,
                                                          const ObStmtMapInfo &stmt_map_info,
                                                          ObIArray<ObRawExpr *> &source_exprs,
                                                          ObIArray<ObRawExpr *> &target_exprs,
                                                          bool &is_simple_join_condition,
                                                          bool &target_tables_have_filter,
                                                          bool &is_simple_filter)
{
  int ret = OB_SUCCESS;
  is_simple_join_condition = true;
  target_tables_have_filter = false;
  is_simple_filter = true;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param has null", K(ret));
  } else {
    ObRawExpr *expr = NULL;
    ObOpRawExpr *op = NULL;
    int64_t left_idx = stmt->get_table_bit_index(source_table->table_id_);
    int64_t right_idx = target_stmt->get_table_bit_index(target_table->table_id_);
    for (int64_t i = 0; OB_SUCC(ret) && i < semi_conds.count(); ++i) {
      if (OB_ISNULL(expr = semi_conds.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null", K(ret));
      } else if (!select_relids.at(i).has_member(right_idx)) {
        /* do nohing */
      } else if (!expr->get_relation_ids().has_member(left_idx)) {
        target_tables_have_filter = true;
        if (T_OP_OR == expr->get_expr_type()) { // complex right table filter
          is_simple_filter = false;
        } else { /*do nothing*/ }
      } else if (T_OP_EQ != expr->get_expr_type()) {
        is_simple_join_condition = false;
      } else if (OB_FALSE_IT(op = static_cast<ObOpRawExpr *>(expr))) {
      } else if (OB_ISNULL(op->get_param_expr(0)) || OB_ISNULL(op->get_param_expr(1))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexcept null param", K(ret));
      } else if (!op->get_param_expr(0)->has_flag(IS_COLUMN)
                 || !op->get_param_expr(1)->has_flag(IS_COLUMN)) {
        is_simple_join_condition = false;
      } else {
        bool is_equal = false;
        int64_t pos = OB_INVALID_ID;
        ObRawExpr *child_expr = NULL;
        ObColumnRefRawExpr *col1 = static_cast<ObColumnRefRawExpr*>(op->get_param_expr(0));
        ObColumnRefRawExpr *col2 = static_cast<ObColumnRefRawExpr*>(op->get_param_expr(1));
        if (col2->get_table_id() == source_table->table_id_) {
          ObColumnRefRawExpr *col = col1;
          col1 = col2;
          col2 = col;
        }
        if (FALSE_IT(pos = col2->get_column_id() - OB_APP_MIN_COLUMN_ID)) {
        } else if (OB_UNLIKELY(pos < 0 || pos >= target_stmt->get_select_item_size())
                   || OB_ISNULL(child_expr = target_stmt->get_select_item(pos).expr_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid array pos",  K(ret), K(pos), K(target_stmt->get_select_item_size()),
                                         K(child_expr));
        } else if (!child_expr->is_column_ref_expr()) {
          is_simple_join_condition = false;
        } else if (OB_FALSE_IT(col2 = static_cast<ObColumnRefRawExpr*>(child_expr))) {
        } else if (OB_FAIL(is_equal_column(source_table, target_table, stmt_map_info.select_item_map_,
                                    col1->get_column_id(), col2->get_column_id(), is_equal))) {
          LOG_WARN("check column ref table id is equal failed", K(ret));
        } else if (!is_equal) {
          is_simple_join_condition = false;
        } else if (OB_FAIL(source_exprs.push_back(col1))) {
          LOG_WARN("push back column expr failed", K(ret));
        } else if (OB_FAIL(target_exprs.push_back(col2))) {
          LOG_WARN("push back column expr failed", K(ret));
        }
      }
    }
    
    /* check right table filters in target stmt */
    if (OB_SUCC(ret) && is_simple_join_condition) {
      ObIArray<ObRawExpr*> &conds = target_stmt->get_condition_exprs();
      for (int64_t i = 0; OB_SUCC(ret) && i < conds.count(); ++i) {
        if (OB_ISNULL(expr = conds.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expr is null", K(ret));
        } else if (expr->get_relation_ids().has_member(right_idx)) {
          target_tables_have_filter = true;
          if (T_OP_OR == expr->get_expr_type()) {
            /* not a simple right table filter */
            is_simple_filter = false;
          }
        }
      }
    }
  }
  return ret;
}


int ObTransformJoinElimination::get_epxrs_rel_ids_in_child_stmt(ObDMLStmt *stmt,
                                                                ObSelectStmt *child_stmt,
                                                                ObIArray<ObRawExpr *> &cond_exprs,
                                                                uint64_t table_id,
                                                                ObIArray<ObSqlBitSet<>> &rel_ids)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObSqlBitSet<>, 4> select_relids;
  ObRawExpr *expr = NULL;
  ObSEArray<ObRawExpr*, 4> column_exprs;
  int64_t pos = OB_INVALID_INDEX;
  rel_ids.reuse();
  if (OB_ISNULL(stmt) || OB_ISNULL(child_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexcepted null", K(ret), K(stmt), K(child_stmt));
  } else if (OB_FAIL(rel_ids.prepare_allocate(cond_exprs.count()))) {
    LOG_WARN("failed to prepare allocate array", K(ret));
  } else if (OB_FAIL(select_relids.prepare_allocate(child_stmt->get_select_item_size()))) {
    LOG_WARN("failed to prepare allocate array", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < child_stmt->get_select_item_size(); ++i) {
    if (OB_ISNULL(expr = child_stmt->get_select_item(i).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexcepted null", K(ret));
    } else if (OB_FAIL(select_relids.at(i).add_members(expr->get_relation_ids()))) {
      LOG_WARN("failed to add members", K(ret));
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < cond_exprs.count(); ++i) {
    column_exprs.reuse();
    if (OB_FAIL(ObRawExprUtils::extract_column_exprs(cond_exprs.at(i), column_exprs))) {
      LOG_WARN("failed to extract column exprs", K(ret));
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < column_exprs.count(); ++j) {
      ObColumnRefRawExpr *col = static_cast<ObColumnRefRawExpr *>(column_exprs.at(j));
      if (OB_ISNULL(column_exprs.at(j)) || !column_exprs.at(j)->is_column_ref_expr()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexcepted column expr", K(ret), K(column_exprs.at(j)));
      } else if (col->get_table_id() == table_id) {
        pos = col->get_column_id() - OB_APP_MIN_COLUMN_ID;
        if (OB_FALSE_IT(pos = col->get_column_id() - OB_APP_MIN_COLUMN_ID)) {
        } else if (OB_UNLIKELY(pos < 0 || pos >= select_relids.count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexcepted position", K(ret), K(pos), K(select_relids.count()));
        } else if (OB_FAIL(rel_ids.at(i).add_members(select_relids.at(pos)))) {
          LOG_WARN("failed to add members", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTransformJoinElimination::is_equal_column(const TableItem *source_table,
                                                const TableItem *target_table,
                                                const ObIArray<int64_t> &output_map,
                                                uint64_t source_col_id,
                                                uint64_t target_col_id,
                                                bool &is_equal)
{
  int ret = OB_SUCCESS;
  is_equal = false;
  if (OB_ISNULL(source_table) || OB_ISNULL(target_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param has null", K(source_table), K(target_table));
  } else {
    if (target_table->is_basic_table() || target_table->is_temp_table()) {
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
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObTransformJoinElimination::trans_semi_table_item(ObDMLStmt *stmt,
                                                      const TableItem *target_table)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt) || OB_ISNULL(target_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parameter is null", K(ret), K(stmt), K(target_table));
  } else  if (OB_FAIL(stmt->remove_table_item(target_table))) {
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
  } else {/*do nothing*/}
  return ret;
}

int ObTransformJoinElimination::trans_semi_condition_exprs(ObDMLStmt *stmt,
                                                           SemiInfo *semi_info)
{
  int ret = OB_SUCCESS;
  ObSqlBitSet<> left_rel_ids;
  ObSqlBitSet<> right_rel_ids;
  ObArray<ObRawExpr *> constraints;
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
    ObSEArray<ObRawExpr*, 16> new_conditions;
    ObRawExpr *expr = NULL;
    ObRawExpr *col_expr = NULL;
    ObRawExpr *new_cond = NULL;
    bool is_lnnvl_cond = false;
    bool is_not_null = true;
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      if (OB_ISNULL(expr = semi_info->semi_conditions_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null", K(ret));
      } else if (OB_FAIL(expr->pull_relation_id())) {
        LOG_WARN("failed to pull relation id and levels", K(ret));
      } else if (T_OP_EQ != expr->get_expr_type()) {
        is_lnnvl_cond = true;
      } else if (OB_ISNULL(expr->get_param_expr(0)) || OB_ISNULL(expr->get_param_expr(1))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexcept null param", K(ret));
      } else if (expr->get_param_expr(0) == expr->get_param_expr(1)) {
        is_lnnvl_cond = false;
        col_expr = expr->get_param_expr(0);
      } else if (left_rel_ids.is_superset(expr->get_relation_ids()) ||
                 right_rel_ids.is_superset(expr->get_relation_ids())) {
        is_lnnvl_cond = true;
      } else if (!expr->get_param_expr(0)->has_flag(IS_COLUMN) ||
                 !expr->get_param_expr(1)->has_flag(IS_COLUMN)) {
        is_lnnvl_cond = true;
      } else if (right_rel_ids.is_superset(expr->get_param_expr(0)->get_relation_ids())) {
        is_lnnvl_cond = false;
        col_expr = expr->get_param_expr(1);
      } else {
        is_lnnvl_cond = false;
        col_expr = expr->get_param_expr(0);
      }

      if (OB_FAIL(ret)) {
      } else if (is_lnnvl_cond) { // build lnnvl
        ret = ObRawExprUtils::build_lnnvl_expr(*ctx_->expr_factory_, expr, new_cond);
      } else if (OB_FAIL(ObTransformUtils::is_expr_not_null(ctx_, stmt, col_expr, 
                                                            NULLABLE_SCOPE::NS_WHERE,
                                                            is_not_null,
                                                            &constraints))) {
        LOG_WARN("failed to check whether expr is nullable", K(ret));
      } else if (is_not_null && OB_FAIL(ObTransformUtils::add_param_not_null_constraint(*ctx_, constraints))) {
        LOG_WARN("failed to add param not null constraints", K(ret));
      } else if (is_not_null) { // create const false
        if (OB_FAIL(ObTransformUtils::add_param_not_null_constraint(*ctx_, constraints))) {
          LOG_WARN("failed to add param not null constraint", K(ret));
        } else if (OB_FAIL(ObRawExprUtils::build_const_bool_expr(ctx_->expr_factory_, new_cond, false))) {
          LOG_WARN("failed to build const bool expr", K(ret));
        }
      } else { // create is null
        ret = ObRawExprUtils::build_is_not_null_expr(*ctx_->expr_factory_, col_expr,
                                                     false, new_cond);
      }

      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(new_cond)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("new expr is null", K(ret));
      } else if (OB_FAIL(new_conditions.push_back(new_cond))) {
        LOG_WARN("push back new expr to conditions failed", K(ret));
      }
    }
    /*将所有改写的condition转成NOT(join_cond1) OR NOT(join_cond2) OR ...
      OR LNNVL(expr1) OR LNNVL(expr2) OR ...*/
    if (OB_SUCC(ret) ) {
      ObRawExpr *filter_expr = NULL;
      if (!new_conditions.empty() && OB_FAIL(ObRawExprUtils::build_or_exprs(*ctx_->expr_factory_, new_conditions, filter_expr))) {
        LOG_WARN("make or expr failed", K(ret));
      } else if (new_conditions.empty() && OB_FAIL(ObRawExprUtils::build_const_bool_expr(ctx_->expr_factory_, filter_expr, false))) {
        LOG_WARN("make or expr failed", K(ret));
      } else if (OB_ISNULL(filter_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("or expr is null", K(ret));
      } else if (OB_FAIL(stmt->get_condition_exprs().push_back(filter_expr))) {
        LOG_WARN("failed to push back cond", K(ret));
      } else {/*do nothing*/}
    } else {/*do nothing*/}
  }
  return ret;
}

int ObTransformJoinElimination::get_column_exprs(ObDMLStmt &stmt,
                                                 ObSelectStmt &child_stmt,
                                                 const uint64_t upper_table_id,
                                                 const uint64_t child_table_id,
                                                 ObIArray<ObRawExpr*> &upper_columns,
                                                 ObIArray<ObRawExpr*> &child_columns)
{
  int ret = OB_SUCCESS;
  upper_columns.reuse();
  child_columns.reuse();
  ObRawExpr *select_expr = NULL;
  ObRawExpr *upper_expr = NULL;
  ObColumnRefRawExpr *column_expr = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < child_stmt.get_select_item_size(); ++i) {
    if (OB_ISNULL(select_expr = child_stmt.get_select_item(i).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else if (!select_expr->is_column_ref_expr()) {
      /* do nothing */
    } else if (FALSE_IT(column_expr = static_cast<ObColumnRefRawExpr*>(select_expr))) {
    } else if (column_expr->get_table_id() != child_table_id) {
      /* do nothing */
    } else if (OB_ISNULL(upper_expr = stmt.get_column_expr_by_id(upper_table_id,
                                                                 i + OB_APP_MIN_COLUMN_ID))) {
      // current select expr is not used by upper stmt
    } else if (OB_FAIL(upper_columns.push_back(upper_expr))) {
      LOG_WARN("failed to push back column expr", K(ret));
    } else if (OB_FAIL(child_columns.push_back(select_expr))) {
      LOG_WARN("failed to push back column expr", K(ret));
    }
  }
  return ret;
}

int ObTransformJoinElimination::check_transform_validity_semi_foreign_key(ObDMLStmt *stmt,
                                                                SemiInfo *semi_info,
                                                                TableItem *right_table,
                                                                TableItem *&left_table,
                                                                ObForeignKeyInfo *&foreign_key_info)
{
  int ret = OB_SUCCESS;
  foreign_key_info = NULL;
  left_table = NULL;
  ObSEArray<TableItem*, 4> left_tables;
  if (OB_ISNULL(stmt) || OB_ISNULL(semi_info) || OB_ISNULL(right_table) ||
      OB_UNLIKELY(!right_table->is_basic_table())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected input param", K(ret), K(stmt), K(semi_info), K(right_table));
  } else if (OB_FAIL(stmt->get_table_item_by_id(semi_info->left_table_ids_ , left_tables))) {
    LOG_WARN("failed to get table items", K(ret));
  } else {
    const ObIArray<ObRawExpr*>& semi_conds = semi_info->semi_conditions_;
    const int64_t right_idx = stmt->get_table_bit_index(right_table->table_id_);
    bool can_be_eliminated = false;
    ObSEArray<ObRawExpr*, 4> source_exprs;
    ObSEArray<ObRawExpr*, 4> target_exprs;
    ObForeignKeyInfo *tmp_foreign_key_info = NULL;
    ObSEArray<int64_t, 4> unused_conds;
    for (int64_t i = 0; OB_SUCC(ret) && NULL == left_table && i < left_tables.count(); ++i) {
      unused_conds.reuse();
      if (OB_FAIL(extract_equal_join_columns(semi_conds, left_tables.at(i), right_table,
                                             source_exprs, target_exprs, &unused_conds))) {
        LOG_WARN("failed to extract join columns", K(ret));
      } else if (OB_FAIL(check_transform_validity_foreign_key(stmt, left_tables.at(i), right_table,
                                                              source_exprs, target_exprs,
                                                              can_be_eliminated,
                                                              tmp_foreign_key_info))) {
        LOG_WARN("check transform validity with foreign key failed", K(ret));
      } else if (!can_be_eliminated) {
        /* do nothing */
      } else if (!semi_info->is_anti_join()) {
        /*do nothing*/
      } else {
        /* for anti join, should check whether right table exists table filter. */
        ObRawExpr *expr = NULL;
        for (int64_t i = 0; OB_SUCC(ret) && can_be_eliminated && i < unused_conds.count(); ++i) {
          if (OB_UNLIKELY(semi_conds.count() <= unused_conds.at(i)) ||
              OB_ISNULL(expr = semi_conds.at(unused_conds.at(i)))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected exprs", K(ret), K(semi_conds.count()), K(unused_conds.at(i)), K(expr));
          } else if (expr->get_relation_ids().has_member(right_idx)) {
            can_be_eliminated = false;
          }
        }
      }
      if (OB_SUCC(ret) && can_be_eliminated) {
        left_table = left_tables.at(i);
        foreign_key_info = tmp_foreign_key_info;
      }
    }
  }
  return ret;
}

int ObTransformJoinElimination::check_transform_validity_semi_foreign_key(ObDMLStmt *stmt,
                                                    SemiInfo *semi_info,
                                                    TableItem *right_table,
                                                    ObIArray<TableItem*> &left_tables,
                                                    ObIArray<TableItem*> &right_tables,
                                                    ObIArray<ObForeignKeyInfo*> &foreign_key_infos)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *child_stmt = NULL;
  ObSEArray<TableItem*, 4> all_left_tables;
  if (OB_ISNULL(stmt) || OB_ISNULL(semi_info) || OB_ISNULL(right_table) ||
      OB_UNLIKELY(!right_table->is_generated_table()) ||
      OB_ISNULL(child_stmt = right_table->ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected input param", K(ret), K(stmt), K(semi_info),
                                       K(right_table), K(child_stmt));
  } else if (!child_stmt->is_spj()) {
    /* do nothing */
  } else if (OB_FAIL(stmt->get_table_item_by_id(semi_info->left_table_ids_ , all_left_tables))) {
    LOG_WARN("failed to get table items", K(ret));
  } else {
    const ObIArray<ObRawExpr*>& semi_conds = semi_info->semi_conditions_;
    TableItem *right_child_table = NULL;
    ObForeignKeyInfo *foreign_key_info = NULL;
    ObSEArray<ObRawExpr*, 4> right_column_exprs;
    ObSEArray<ObRawExpr*, 4> right_child_column_exprs;
    ObSEArray<ObRawExpr*, 4> source_exprs;
    ObSEArray<ObRawExpr*, 4> target_exprs;
    ObSEArray<int64_t, 4> unused_conds;
    bool used = false;
    bool can_be_eliminated = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < child_stmt->get_from_item_size(); ++i) {
      FromItem item = child_stmt->get_from_item(i);
      unused_conds.reuse();
      can_be_eliminated = false;
      if (item.is_joined_ || child_stmt->is_semi_left_table(item.table_id_)) {
        /* do nothing */
      } else if (OB_ISNULL(right_child_table = child_stmt->get_table_item_by_id(item.table_id_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret), K(right_child_table));
      } else if (OB_FAIL(is_table_column_used_in_subquery(*child_stmt, item.table_id_, used))) {
        LOG_WARN("failed to check is table column used in subquery", K(ret));
      } else if (used) {
        /* do nothing */
      } else if (OB_FAIL(get_column_exprs(*stmt, *child_stmt, right_table->table_id_,
                                          right_child_table->table_id_, right_column_exprs,
                                          right_child_column_exprs))) {
        LOG_WARN("failed to get column exprs", K(ret));
      } else if (right_column_exprs.empty()) {
        /* do nothing */
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && !can_be_eliminated && j < all_left_tables.count(); ++j) {
          if (OB_ISNULL(all_left_tables.at(j))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected null", K(ret), K(all_left_tables.at(j)));
          } else if (OB_FAIL(extract_equal_join_columns(semi_conds,
                                                        all_left_tables.at(j)->table_id_,
                                                        right_table->table_id_, right_column_exprs,
                                                        right_child_column_exprs, source_exprs,
                                                        target_exprs, &unused_conds))) {
            LOG_WARN("failed to extract join columns", K(ret));
          } else if (OB_FAIL(check_transform_validity_foreign_key(child_stmt,
                                                                  all_left_tables.at(j),
                                                                  right_child_table,
                                                                  source_exprs,
                                                                  target_exprs,
                                                                  can_be_eliminated,
                                                                  foreign_key_info))) {
            LOG_WARN("check transform validity with foreign key failed", K(ret));
          } else if (!can_be_eliminated) {
            /*do nothing*/
          } else if (!semi_info->is_anti_join()) {
            /*do nothing*/
          } else {
            /* for anti join, should check whether right table exists table filter. */
            const ObIArray<ObRawExpr*>& child_conds = child_stmt->get_condition_exprs();
            const int64_t right_idx = stmt->get_table_bit_index(right_table->table_id_);
            const int64_t right_child_idx = child_stmt->get_table_bit_index(
                                                              right_child_table->table_id_);
            ObRawExpr *expr = NULL;
            for (int64_t i = 0; OB_SUCC(ret) && can_be_eliminated && i < unused_conds.count(); ++i) {
              if (OB_UNLIKELY(semi_conds.count() <= unused_conds.at(i)) ||
                  OB_ISNULL(expr = semi_conds.at(unused_conds.at(i)))) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("unexpected exprs", K(ret), K(semi_conds.count()),
                                             K(unused_conds.at(i)), K(expr));
              } else if (expr->get_relation_ids().has_member(right_idx)) {
                can_be_eliminated = false;
              }
            }
            for (int64_t i = 0; OB_SUCC(ret) && can_be_eliminated && i < child_conds.count(); ++i) {
              if (OB_ISNULL(expr = child_conds.at(i))) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("expr is null", K(ret));
              } else if (expr->get_relation_ids().has_member(right_child_idx)) {
                can_be_eliminated = false;
              }
            }
          }
          if (OB_FAIL(ret) || !can_be_eliminated) {
          } else if (OB_FAIL(left_tables.push_back(all_left_tables.at(j))) ||
                     OB_FAIL(right_tables.push_back(right_child_table)) ||
                     OB_FAIL(foreign_key_infos.push_back(foreign_key_info))) {
            LOG_WARN("failed to push back", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObTransformJoinElimination::check_transform_validity_foreign_key(const ObDMLStmt *target_stmt,
                                                    const TableItem *source_table,
                                                    const TableItem *target_table,
                                                    const ObIArray<ObRawExpr*> &source_exprs,
                                                    const ObIArray<ObRawExpr*> &target_exprs,
                                                    bool &can_be_eliminated,
                                                    ObForeignKeyInfo *&foreign_key_info)
{
  int ret = OB_SUCCESS;
  can_be_eliminated = false;
  foreign_key_info = NULL;
  bool is_foreign_primary_join = false;
  bool all_primary_key = false;
  bool is_rely_foreign_key = false;
  bool is_first_table_parent = false;
  bool is_hint_valid = false;
  if (OB_ISNULL(target_stmt) || OB_ISNULL(ctx_) || 
      OB_ISNULL(source_table) || OB_ISNULL(target_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parameter have null", K(ctx_), K(target_stmt), K(source_table),
                                    K(target_table), K(ret));
  } else if (OB_FAIL(check_hint_valid(*target_stmt, *target_table, is_hint_valid))) {
    LOG_WARN("failed to check hint valid", K(ret));
  } else if (!is_hint_valid) {
    /* do nothing */
    OPT_TRACE("hint disable transform");
  } else if (!source_table->is_basic_table() || !target_table->is_basic_table()
             || source_exprs.empty()) {
    /* do nothing */
    OPT_TRACE("not basic table");
  } else if (OB_FAIL(ObTransformUtils::check_foreign_primary_join(source_table,
                                                                  target_table,
                                                                  source_exprs,
                                                                  target_exprs,
                                                                  ctx_->schema_checker_,
                                                                  ctx_->session_info_,
                                                                  is_foreign_primary_join,
                                                                  is_first_table_parent,
                                                                  foreign_key_info))) {
    LOG_WARN("failed to check has foreign key constraint", K(ret));
  } else if (!is_foreign_primary_join || is_first_table_parent) {
    /* do nothing */
    OPT_TRACE("is not foreign primary join");
  } else if (OB_UNLIKELY(!target_table->access_all_part())) {
    /*父表有partition hint，不可消除*/
    /*TODO zhenling.zzg 之后可以完善对于父表、子表均有partition hint的情况*/
    OPT_TRACE("target table has partition hint");
  } else if (OB_FAIL(ObTransformUtils::is_foreign_key_rely(ctx_->session_info_,
                                                           foreign_key_info,
                                                           is_rely_foreign_key))) {
    LOG_WARN("check foreign key is rely failed", K(ret));
  } else if (!is_rely_foreign_key) {
    /*非可靠主外键关系，不能消除，do nothing*/
    OPT_TRACE("foreign key is not reliable");
  } else if (OB_FAIL(check_all_column_primary_key(target_stmt, target_table->table_id_,
                                                  foreign_key_info, all_primary_key))) {
    LOG_WARN("failed to check all column primary key", K(ret));
  } else if (all_primary_key) {
    can_be_eliminated = true;
  } else {
    OPT_TRACE("not all primary keys used");
  }
  if (OB_SUCC(ret) && !can_be_eliminated) {
    OPT_TRACE(source_table, "can not eliminate", target_table, "with foreign key");
  }
  return ret;
}

int ObTransformJoinElimination::extract_equal_join_columns(const ObIArray<ObRawExpr *> &join_conds,
                                                    const uint64_t source_tid,
                                                    const uint64_t upper_target_tid,
                                                    const ObIArray<ObRawExpr*> &upper_column_exprs,
                                                    const ObIArray<ObRawExpr*> &child_column_exprs,
                                                    ObIArray<ObRawExpr*> &source_exprs,
                                                    ObIArray<ObRawExpr*> &target_exprs,
                                                    ObIArray<int64_t> *unused_conds)
{
  int ret = OB_SUCCESS;
  ObRawExpr *expr = NULL;
  bool used = false;
  int64_t idx = OB_INVALID_INDEX;
  source_exprs.reuse();
  target_exprs.reuse();
  if (OB_UNLIKELY(upper_column_exprs.count() != child_column_exprs.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected exprs count", K(ret), K(upper_column_exprs.count()),
                                       K(child_column_exprs.count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < join_conds.count(); ++i) {
    used = false;
    if (OB_ISNULL(expr = join_conds.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is null", K(ret));
    } else if (T_OP_EQ == expr->get_expr_type()) {
      ObOpRawExpr *op = static_cast<ObOpRawExpr*>(expr);
      if (op->get_param_expr(0)->has_flag(IS_COLUMN)
          && op->get_param_expr(1)->has_flag(IS_COLUMN)) {
        ObColumnRefRawExpr *col1 = static_cast<ObColumnRefRawExpr *>(op->get_param_expr(0));
        ObColumnRefRawExpr *col2 = static_cast<ObColumnRefRawExpr *>(op->get_param_expr(1));
        if (col1->get_table_id() == source_tid && col2->get_table_id() == upper_target_tid) {
          if (!ObOptimizerUtil::find_item(upper_column_exprs, col1, &idx)) {
            /* do nothing */
          } else if (OB_FAIL(source_exprs.push_back(col1))) {
            LOG_WARN("failed to push back expr", K(ret));
          } else if (OB_FAIL(target_exprs.push_back(child_column_exprs.at(idx)))) {
            LOG_WARN("failed to push back expr", K(ret));
          } else {
            used = true;
          }
        } else if (col1->get_table_id() == upper_target_tid && col2->get_table_id() == source_tid) {
          if (!ObOptimizerUtil::find_item(upper_column_exprs, col2, &idx)) {
            /* do nothing */
          } else if (OB_FAIL(target_exprs.push_back(child_column_exprs.at(idx)))) {
            LOG_WARN("failed to push back expr", K(ret));
          } else if (OB_FAIL(source_exprs.push_back(col2))) {
            LOG_WARN("failed to push back expr", K(ret));
          } else {
            used = true;
          }
        }
      }
    }
    if (OB_SUCC(ret) && !used && NULL != unused_conds) {
      ret = unused_conds->push_back(i);
    }
  }
  return ret;
}

int ObTransformJoinElimination::EliminationHelper::push_back(TableItem *child,
                                                             TableItem *parent,
                                                             ObForeignKeyInfo *info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(child_table_items_.push_back(child))) {
    LOG_WARN("faild to push back table item", K(ret));
  } else if (OB_FAIL(parent_table_items_.push_back(parent))) {
    LOG_WARN("faild to push back table item", K(ret));
  } else if (OB_FAIL(foreign_key_infos_.push_back(info))) {
    LOG_WARN("failed to push back foreign key info", K(ret));
  } else if (OB_FAIL(bitmap_.push_back(false))) {
    LOG_WARN("failed to push back bool", K(ret));
  }
  ++count_;
  ++remain_;
  return ret;
}

int ObTransformJoinElimination::EliminationHelper::get_eliminable_group(TableItem *&child,
                                                                        TableItem *&parent,
                                                                        ObForeignKeyInfo *&info,
                                                                        bool &find)
{
  int ret = OB_SUCCESS;
  bool in_child = false;
  find = false;
  if(OB_LIKELY(remain_ > 0)) {
    for (int64_t i = 0; OB_SUCC(ret) && !find && i < count_; ++i) {
      if (bitmap_.at(i)) {
        /* table already eliminated */
      } else if (OB_FAIL(is_table_in_child_items(parent_table_items_.at(i), in_child))) {
        LOG_WARN("faild to find table item", K(ret));
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

int ObTransformJoinElimination::extract_candi_table(ObDMLStmt *stmt,
                                                    const ObIArray<FromItem> &from_items,
                                                    ObIArray<TableItem *> &candi_tables,
                                                    ObIArray<TableItem *> &child_candi_tables)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(stmt), K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < from_items.count(); ++i) {
    const FromItem &from_item = from_items.at(i);
    TableItem *table_item = NULL;
    if (from_item.is_joined_) {
      ret = extract_candi_table(stmt->get_joined_table(from_item.table_id_), child_candi_tables);
    } else if (OB_ISNULL(table_item = stmt->get_table_item_by_id(from_item.table_id_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid table id", K(ret), K(from_item.table_id_));
    } else if (table_item->is_basic_table() || table_item->is_temp_table() || table_item->is_generated_table()) {
      ret = candi_tables.push_back(table_item);
    }
  }
  return ret;
}

int ObTransformJoinElimination::extract_candi_table(JoinedTable *table,
                                                    ObIArray<TableItem *> &child_candi_tables)
{
  int ret = OB_SUCCESS;
  TableItem *left_table = NULL;
  TableItem *right_table = NULL;
  if (OB_ISNULL(table) || OB_ISNULL(left_table = table->left_table_)
      || OB_ISNULL(right_table = table->right_table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (table->is_inner_join() || IS_OUTER_JOIN(table->joined_type_)) {
    if (left_table->is_joined_table()) {
      ret = SMART_CALL(extract_candi_table(static_cast<JoinedTable*>(left_table),
                                           child_candi_tables));
    } else if (left_table->is_basic_table() || left_table->is_temp_table() || left_table->is_generated_table()) {
      ret = child_candi_tables.push_back(left_table);
    }
    if (OB_FAIL(ret)) {
    } else if (right_table->is_joined_table()) {
      ret = SMART_CALL(extract_candi_table(static_cast<JoinedTable*>(right_table),
                                           child_candi_tables));
    } else if (right_table->is_basic_table() || right_table->is_temp_table() || right_table->is_generated_table()) {
      ret = child_candi_tables.push_back(right_table);
    }
  }
  return ret;
}

int ObTransformJoinElimination::classify_joined_table(JoinedTable *joined_table,
                                                      ObIArray<JoinedTable *> &inner_join_tables,
                                                      ObIArray<TableItem *> &outer_join_tables,
                                                      ObIArray<TableItem *> &other_tables,
                                                      ObIArray<ObRawExpr *> &inner_join_conds)
{
  int ret = OB_SUCCESS;
  TableItem *left_table = NULL;
  TableItem *right_table = NULL;
  ObSEArray<JoinedTable*, 8> tmp_joined_tables;
  JoinedTable *cur_joined_table = NULL;
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
    } else if (cur_joined_table != joined_table &&
               OB_FAIL(inner_join_tables.push_back(cur_joined_table))) {
      LOG_WARN("failed to push back table", K(ret));
    } else { // inner join
      JoinedTable *left_joined_table = NULL;
      JoinedTable *right_joined_table = NULL;
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

int ObTransformJoinElimination::eliminate_join_in_joined_table(ObDMLStmt *stmt,
                                                        TableItem *&table_item,
                                                        ObIArray<TableItem *> &child_candi_tables,
                                                        ObIArray<ObRawExpr *> &trans_conditions,
                                                        bool &trans_happened,
                                                        ObIArray<ObSEArray<TableItem *, 4>> &trans_tables)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  JoinedTable *joined_table = NULL;
  TableItem *left_table = NULL;
  TableItem *right_table = NULL;
  if (OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null",  K(ret), K(table_item));
  } else if (!table_item->is_joined_table()) {
    if (!table_item->is_basic_table() && !table_item->is_generated_table() && !table_item->is_temp_table()) {
      /*do nothing*/
    } else if (OB_FAIL(child_candi_tables.push_back(table_item))) {
      LOG_WARN("failed to push back table", K(ret));
    }
  } else if (OB_ISNULL(joined_table = static_cast<JoinedTable*>(table_item))
             || OB_ISNULL(left_table = joined_table->left_table_)
             || OB_ISNULL(right_table = joined_table->right_table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(joined_table), K(left_table), K(right_table));
  } else if (joined_table->is_full_join()) {
    // full outer join 内部暂不进行消除, 连接消除后可能传出 trans_conditions
    // to do: full outer join 内部不会传递出 trans_conditions 时可以消除
    ret = extract_candi_table(joined_table, child_candi_tables);//允许使用 child table 消除外部表
  } else if (IS_OUTER_JOIN(joined_table->joined_type_)) {
    bool left_happened = false;
    bool right_happened = false;
    ObSEArray<TableItem *, 4> left_child_candi_tables;
    ObSEArray<ObRawExpr *, 16> left_trans_conditions;
    ObSEArray<TableItem *, 4> right_child_candi_tables;
    ObSEArray<ObRawExpr *, 16> right_trans_conditions;
    if (OB_FAIL(SMART_CALL(eliminate_join_in_joined_table(stmt, joined_table->right_table_,
                                                         right_child_candi_tables,
                                                         right_trans_conditions,
                                                         right_happened,
                                                         trans_tables)))) {
      LOG_WARN("failed to eliminate join in joined table`s right table.", K(ret));
    } else if (OB_FAIL(SMART_CALL(eliminate_join_in_joined_table(stmt, joined_table->left_table_,
                                                                left_child_candi_tables,
                                                                left_trans_conditions,
                                                                left_happened,
                                                                trans_tables)))) {
      LOG_WARN("failed to eliminate join in joined table`s left table.", K(ret));
    } else if (OB_FAIL(append(child_candi_tables, left_child_candi_tables))
               || OB_FAIL(append(child_candi_tables, right_child_candi_tables))) {
      LOG_WARN("failed to append child candi tables", K(ret));
    } else if (!left_happened && !right_happened) {
      /* do nothing */
    } else if (OB_FAIL(ObTransformUtils::adjust_single_table_ids(joined_table))) {
      LOG_WARN("failed to construct single table ids.", K(ret));
    } else if (joined_table->is_left_join() &&
               (OB_FAIL(append(trans_conditions, left_trans_conditions))
                || OB_FAIL(append(joined_table->join_conditions_, right_trans_conditions)))) {
      LOG_WARN("failed to append trans conditions to left join", K(ret));
    } else if (joined_table->is_right_join() &&
               (OB_FAIL(append(joined_table->join_conditions_, left_trans_conditions))
                || OB_FAIL(append(trans_conditions, right_trans_conditions)))) {
      LOG_WARN("failed to append trans conditions to right join", K(ret));
    } else {
      trans_happened = true;
    }
  } else if (!joined_table->is_inner_join()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected joined type",  K(ret), K(joined_table));
  } else {
    ObSEArray<JoinedTable*, 4> inner_join_tables;
    ObSEArray<TableItem*, 4> outer_join_tables;
    ObSEArray<TableItem*, 4> other_tables;// not joined table
    ObSEArray<ObRawExpr*, 8> inner_join_conds;
    ObSEArray<TableItem*, 4> tmp_child_candi_tables;
    ObSEArray<ObRawExpr*, 4> tmp_trans_conditions;
    bool is_happened = false;
    if (OB_FAIL(classify_joined_table(joined_table, inner_join_tables, outer_join_tables,
                                      other_tables, inner_join_conds))) {
      LOG_WARN("failed to classify joined table", K(ret), K(*joined_table));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < outer_join_tables.count(); i++) {// 消除子表
      tmp_child_candi_tables.reuse();
      tmp_trans_conditions.reuse();
      if (OB_FAIL(SMART_CALL(eliminate_join_in_joined_table(stmt, outer_join_tables.at(i),
                                                            tmp_child_candi_tables,
                                                            tmp_trans_conditions,
                                                            is_happened,
                                                            trans_tables)))) {
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
    } else if (OB_FAIL(eliminate_candi_tables(stmt, inner_join_conds, other_tables,
                                              child_candi_tables, false, is_happened, trans_tables))) {
      LOG_WARN("failed to eliminate candi tables", K(ret));
    } else if (false == (trans_happened |= is_happened)) {
      /* do nothing */
    } else if (OB_FAIL(append(outer_join_tables, other_tables))) {
      LOG_WARN("failed to append tables", K(ret));
    } else if (OB_FAIL(rebuild_joined_tables(stmt, table_item, inner_join_tables,
                                             outer_join_tables, inner_join_conds))) {
      LOG_WARN("failed to rebuild joined tables", K(ret));
    } else if (!table_item->is_joined_table() ||
               !static_cast<JoinedTable*>(table_item)->is_inner_join()) {
      ret = trans_conditions.assign(inner_join_conds);
    } else if (!inner_join_conds.empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("join conditions not empty after rebuild joined table", K(ret), K(*table_item));
    }
  }
  return ret;
}

int ObTransformJoinElimination::eliminate_candi_tables(ObDMLStmt *stmt,
                                                       ObIArray<ObRawExpr*> &conds,
                                                       ObIArray<TableItem*> &candi_tables,
                                                       ObIArray<TableItem*> &child_candi_tables,
                                                       bool is_from_base_table,
                                                       bool &trans_happened,
                                                       ObIArray<ObSEArray<TableItem *, 4>> &trans_tables)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  EqualSets *equal_sets = &ctx_->equal_sets_;
  ObArenaAllocator allocator;
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
          } else if (OB_FAIL(do_join_elimination_self_key(stmt, candi_tables.at(i),
                                                          candi_tables.at(j), is_from_base_table,
                                                          is_happened, trans_tables, equal_sets))) {
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
          if (OB_FAIL(do_join_elimination_self_key(stmt, child_candi_tables.at(j),
                                                   candi_tables.at(i), is_from_base_table,
                                                   is_happened, trans_tables, equal_sets))) {
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

    ObSEArray<TableItem *, 16> baisc_candi_tables;
    ObSEArray<TableItem *, 16> removed_tables;
    for (int64_t i = 0; OB_SUCC(ret) && i < candi_tables.count(); ++i) {
      if (removed_items.has_member(i)) {
        ret = removed_tables.push_back(candi_tables.at(i));
      } else if (candi_tables.at(i)->is_basic_table() &&
                 OB_FAIL(baisc_candi_tables.push_back(candi_tables.at(i)))) {
        LOG_WARN("failed to push back table", K(ret));
      }
    }

    equal_sets->reuse();
    EliminationHelper helper;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(get_eliminable_tables(stmt, conds, baisc_candi_tables,
                                             child_candi_tables, helper))) {
      LOG_WARN("failed to get eliminable tables", K(ret));
    } else {
      bool find = false;
      TableItem *child = NULL;
      TableItem *parent = NULL;
      ObForeignKeyInfo *info = NULL;
      while (OB_SUCC(ret) && helper.get_remain() > 0) {
        if (OB_FAIL(helper.get_eliminable_group(child, parent, info, find))) {
          LOG_WARN("failed to get eliminable_group", K(ret));
        } else if (!find) {
          LOG_WARN("can not find one eliminable group, circle elimination may exists");
          break;
        } else if (OB_FAIL(construct_eliminated_table(stmt, parent, trans_tables))) {
          LOG_WARN("failed to construct eliminated table", K(ret));
        } else if (OB_FAIL(do_join_elimination_foreign_key(stmt, child, parent, info))) {
          LOG_WARN("failed to eliminate useless join", K(ret));
        } else if (OB_FAIL(removed_tables.push_back(parent))) {
          LOG_WARN("failed to push back table", K(ret));
        } else {
          trans_happened = true;
        }
      }
    }

    if (OB_SUCC(ret) && !removed_tables.empty()
        && OB_FAIL(ObOptimizerUtil::remove_item(candi_tables, removed_tables))) {
      LOG_WARN("failed to remove item", K(ret));
    }
  }
  return ret;
}

int ObTransformJoinElimination::rebuild_joined_tables(ObDMLStmt *stmt,
                                                      TableItem *&top_table,
                                                      ObIArray<JoinedTable*> &inner_join_tables,
                                                      ObIArray<TableItem*> &tables,
                                                      ObIArray<ObRawExpr*> &join_conds)
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
    TableItem *left_table = NULL;
    TableItem *right_table = NULL;
    JoinedTable *tmp_joined_table = NULL;
    ObSEArray<ObRawExpr*, 8> cur_join_conds;
    TableItem *cur_table = tables.at(0);
    for (int64_t i = 1; OB_SUCC(ret) && i < tables.count(); i++) {
      cur_join_conds.reuse();
      if (OB_ISNULL(left_table = tables.at(i)) || OB_ISNULL(right_table = cur_table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret), K(left_table), K(right_table));
      } else if (OB_FAIL(stmt->get_table_rel_ids(*left_table, table_set))) {
        LOG_WARN("failed to get table rel ids", K(ret));
      } else if (OB_FAIL(ObTransformUtils::extract_table_exprs(*stmt, join_conds, table_set,
                                                                cur_join_conds))) {
        LOG_WARN("failed to extract table exprs", K(ret));
      } else if (OB_FAIL(ObOptimizerUtil::remove_item(join_conds, cur_join_conds))) {
        LOG_WARN("failed to remove cur cond exprs", K(ret));
      } else if (i > inner_join_tables.count()) {
        ret = ObTransformUtils::add_new_joined_table(ctx_, *stmt, INNER_JOIN, left_table,
                                                     right_table, cur_join_conds, cur_table, false);
      } else if (OB_ISNULL(tmp_joined_table = inner_join_tables.at(i-1))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret), K(inner_join_tables.at(i-1)));
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
    } else if (OB_FAIL(append(static_cast<JoinedTable*>(cur_table)->get_join_conditions(),
                              join_conds))) {
      LOG_WARN("failed to append exprs", K(ret));
    } else {
      join_conds.reuse();
      top_table = cur_table;
    }
  }
  return ret;
}

int ObTransformJoinElimination::trans_self_equal_conds(ObDMLStmt *stmt)
{
  int ret = OB_SUCCESS;
  ObSEArray<JoinedTable *, 16> joined_tables;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("input paramter is null", K(stmt));
  } else if (OB_FAIL(trans_self_equal_conds(stmt, stmt->get_condition_exprs()))) {
    LOG_WARN("failed to trans condition exprs", K(ret));
  } else if (OB_FAIL(joined_tables.assign(stmt->get_joined_tables()))) {
    LOG_WARN("failed to assign joined tables", K(ret));
  } else {
    JoinedTable *joined_table = NULL;
    TableItem *table = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < joined_tables.count(); ++i) {
      if (OB_ISNULL(joined_table = joined_tables.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret));
      } else if (OB_FAIL(trans_self_equal_conds(stmt, joined_table->get_join_conditions()))) {
        LOG_WARN("failed to trans self equal conds", K(ret));
      } else if (OB_NOT_NULL(table = joined_table->left_table_) && table->is_joined_table()
                 && OB_FAIL(joined_tables.push_back(static_cast<JoinedTable*>(table)))) {
        LOG_WARN("failed to push back", K(ret));
      } else if (OB_NOT_NULL(table = joined_table->right_table_) && table->is_joined_table()
                 && OB_FAIL(joined_tables.push_back(static_cast<JoinedTable*>(table)))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
    ObIArray<SemiInfo*> &semi_infos = stmt->get_semi_infos();
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

int ObTransformJoinElimination::trans_self_equal_conds(ObDMLStmt *stmt,
                                                       ObIArray<ObRawExpr*> &cond_exprs)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 16> new_cond_exprs;
  ObSEArray<ObColumnRefRawExpr *, 16> removed_col_exprs;
  ObRawExpr *expr = NULL;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("input paramter is null", K(ret), K(stmt));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < cond_exprs.count(); ++i) {
    if (OB_ISNULL(expr = cond_exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("condition expr is null", K(ret));
    } else if (T_OP_EQ == expr->get_expr_type()) {
      ObOpRawExpr *op = static_cast<ObOpRawExpr *>(expr);
      if (op->get_param_expr(0)->has_flag(IS_COLUMN)
          && op->get_param_expr(0) == op->get_param_expr(1)) {
        // equal condition on the same column
        if (OB_FAIL(removed_col_exprs.push_back(
                            static_cast<ObColumnRefRawExpr *>(op->get_param_expr(0))))) {
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
      } else if(OB_FAIL(expr->formalize(ctx_->session_info_))) {
        LOG_WARN("formlize expr failed", K(ret));
      } else {
        /*do nothing*/
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(cond_exprs.assign(new_cond_exprs))) {//assign cond exprs first
    LOG_WARN("failed to assign exprs", K(ret));
  } else if (OB_FAIL(add_is_not_null_if_needed(stmt, removed_col_exprs, cond_exprs))) {
    LOG_WARN("failed to add is not null condition", K(ret));
  }
  return ret;
}

int ObTransformJoinElimination::add_is_not_null_if_needed(ObDMLStmt *stmt,
                                                          ObIArray<ObColumnRefRawExpr *> &col_exprs,
                                                          ObIArray<ObRawExpr*> &cond_exprs)
{
  int ret = OB_SUCCESS;
  ObArray<ObRawExpr *> constraints;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < col_exprs.count(); ++i) {
    bool is_not_null = true;
    ObOpRawExpr *is_not_expr = NULL;
    if (OB_ISNULL(col_exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(ObTransformUtils::is_expr_not_null(ctx_, stmt, col_exprs.at(i), 
                                                          NULLABLE_SCOPE::NS_WHERE,
                                                          is_not_null,
                                                          &constraints))) {
      LOG_WARN("failed to check whether expr is nullable", K(ret));
    } else if (is_not_null) {
      if (OB_FAIL(ObTransformUtils::add_param_not_null_constraint(*ctx_, constraints))) {
        LOG_WARN("failed to add param not null constraint", K(ret));
      }
    } else if (OB_FAIL(ObTransformUtils::add_is_not_null(ctx_,
                                                          stmt,
                                                          col_exprs.at(i),
                                                          is_not_expr))) {
      LOG_WARN("failed to add is not null for col", K(ret));
    } else if (OB_FAIL(cond_exprs.push_back(is_not_expr))) {
      LOG_WARN("failed to add is_not_expr into condition", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObTransformJoinElimination::recursive_trans_equal_join_condition(ObDMLStmt *stmt,
                                                                     ObRawExpr *expr)
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
  } else if (T_OP_OR == expr->get_expr_type() ||
            T_OP_AND == expr->get_expr_type()) {//递归改写OR表达式的每一个包含COLUMN的参数表达式
    ObOpRawExpr *op = static_cast<ObOpRawExpr *>(expr);
    for (int64_t i = 0; OB_SUCC(ret) && i < op->get_param_count(); ++i) {
      if (OB_ISNULL(op->get_param_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("op expr has null param", K(ret));
      } else if (op->get_param_expr(i)->has_flag(CNT_COLUMN)) {
        bool has_trans = false;
        ObRawExpr* new_expr = NULL;
        //对于每一个参数表达式，尝试改写表达式
        if (OB_FAIL(do_trans_equal_join_condition(stmt,
                                                  op->get_param_expr(i), 
                                                  has_trans,
                                                  new_expr))) {
          LOG_WARN("do trans equal join condition failed", K(ret));
        } else if (has_trans && !(OB_ISNULL(new_expr))) {//改写成功了
          //将新的表达式替换掉原来的参数
          if (OB_FAIL(op->replace_param_expr(i, new_expr))) {
            LOG_WARN("replace param expr failed", K(ret), K(op), K(i));
          } else {
            LOG_TRACE("trans param expr succ", K(op), K(i));
          }
        } else if (OB_FAIL(SMART_CALL(recursive_trans_equal_join_condition(stmt, //改写失败后递归判断是否其子表达式可以改写
                                                                  op->get_param_expr(i))))) {
          LOG_WARN("recursive trans equal join condition failed", K(ret));
        } else {/*do nothing*/}
      } else {/*do nothing*/}
    }
  } else {/*do nothing*/}
  return ret;
}

int ObTransformJoinElimination::do_trans_equal_join_condition(ObDMLStmt *stmt,
                                                              ObRawExpr *expr, 
                                                              bool &has_trans,
                                                              ObRawExpr* &new_expr)
{
  int ret = OB_SUCCESS;
  has_trans = false;
  new_expr = NULL;
  ObRawExpr *param1 = NULL;
  ObRawExpr *param2 = NULL;
  bool is_not_null = true;
  ObArray<ObRawExpr *> constraints;
  if (OB_ISNULL(stmt) || OB_ISNULL(expr) || OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param has null when do trans equal join condition", K(ret));
  } else if (T_OP_EQ != expr->get_expr_type()) {
    /* do nothing */
  } else if (OB_ISNULL(param1 = expr->get_param_expr(0)) ||
             OB_ISNULL(param2 = expr->get_param_expr(1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("op expr has null param", K(ret));
  } else if (!param1->has_flag(IS_COLUMN) || !param2->has_flag(IS_COLUMN) ||
             (param1 != param2 && !param1->same_as(*param2))) {
    /* do nothing */
  } else if (OB_FAIL(ObTransformUtils::is_expr_not_null(ctx_, stmt, param1,
                                                        NULLABLE_SCOPE::NS_FROM,
                                                        is_not_null,
                                                        &constraints))) {
    LOG_WARN("failed to check whether expr is nullable", K(ret));
  } else if (is_not_null) { // create const true
    if (OB_FAIL(ObTransformUtils::add_param_not_null_constraint(*ctx_, constraints))) {
      LOG_WARN("failed to add param not null constraint", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_const_bool_expr(ctx_->expr_factory_, new_expr, true))) {
      LOG_WARN("failed to build const bool expr", K(ret));
    } else {
      has_trans = true;
    }
  } else { // create is not null
    has_trans = true;
    ret = ObRawExprUtils::build_is_not_null_expr(*ctx_->expr_factory_, param1, true, new_expr);
  }
  return ret;
}

int ObTransformJoinElimination::check_hint_valid(const ObDMLStmt &stmt,
                                                 const TableItem &table,
                                                 bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  const ObQueryHint *query_hint = NULL; 
  const ObEliminateJoinHint *hint = static_cast<const ObEliminateJoinHint*>(get_hint(stmt.get_stmt_hint()));
  if (NULL == hint) {
    is_valid = true;
  } else if (OB_ISNULL(query_hint = stmt.get_stmt_hint().query_hint_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(query_hint));
  } else {
    is_valid = hint->enable_eliminate_join(query_hint->cs_type_, table);
    LOG_TRACE("succeed to check eliminate_join hint valid", K(is_valid), K(table), K(*hint));
  }
  return ret;
}

int ObTransformJoinElimination::construct_eliminated_tables(const ObDMLStmt *stmt,
                                                            const ObIArray<TableItem *> &removed_tables,
                                                            ObIArray<ObSEArray<TableItem *, 4>> &trans_tables)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < removed_tables.count(); ++i) {
      if (OB_ISNULL(removed_tables.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(construct_eliminated_table(stmt,
                                                    removed_tables.at(i),
                                                    trans_tables))) {
        LOG_WARN("failed to construct eliminated table", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformJoinElimination::construct_eliminated_table(const ObDMLStmt *stmt,
                                                           const TableItem *table,
                                                           ObIArray<ObSEArray<TableItem *, 4>> &trans_tables)
{
  int ret = OB_SUCCESS;
  ObSEArray<TableItem *, 4> trans_table;
  if (OB_ISNULL(stmt) || OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (table->is_joined_table()) {
    const JoinedTable *joined_table = static_cast<const JoinedTable *>(table);
    for (int64_t i = 0; OB_SUCC(ret) && i < joined_table->single_table_ids_.count(); ++i) {
      TableItem *table = stmt->get_table_item_by_id(joined_table->single_table_ids_.at(i));
      if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(trans_table.push_back(table))) {
        LOG_WARN("failed to push back trans table", K(ret));
      }
    }
  } else if (OB_FAIL(trans_table.push_back(const_cast<TableItem *>(table)))) {
    LOG_WARN("failed to push back table", K(ret));
  }
  if (OB_SUCC(ret) && OB_FAIL(trans_tables.push_back(trans_table))) {
    LOG_WARN("failed to push back trans tables", K(ret));
  }
  return ret;
}

} //namespace sql
} //namespace oceanbase

