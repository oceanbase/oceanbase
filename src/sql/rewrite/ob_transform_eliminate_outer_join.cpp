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

#include "ob_transform_eliminate_outer_join.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/oblog/ob_log_module.h"
#include "common/ob_common_utility.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/resolver/dml/ob_dml_stmt.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "common/ob_smart_call.h"
#include "sql/optimizer/ob_optimizer_util.h"

using namespace oceanbase::common;

namespace oceanbase {
namespace sql {

int ObTransformEliminateOuterJoin::transform_one_stmt(
    common::ObIArray<ObParentDMLStmt>& parent_stmts, ObDMLStmt*& stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  UNUSED(parent_stmts);
  if (OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt is NULL", K(ret));
  } else if (OB_FAIL(eliminate_outer_join(parent_stmts, stmt, trans_happened))) {
    LOG_WARN("failed to eliminate_outer_join on stmt", K(ret));
  } else if (trans_happened) {
    LOG_TRACE("succeed to eliminate outer join", K(trans_happened));
  }
  return ret;
}

int ObTransformEliminateOuterJoin::eliminate_outer_join(
    ObIArray<ObParentDMLStmt>& parent_stmts, ObDMLStmt*& stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  SemiInfo* semi_info = NULL;
  ObSEArray<ObRawExpr*, 16> conditions;
  trans_happened = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt should not be null", K(ret));
  } else if (stmt->get_joined_tables().empty()) {
    /*do nothing*/
  } else if (OB_FAIL(stmt->get_equal_set_conditions(conditions, true, SCOPE_WHERE))) {
    LOG_WARN("failed to get equal set conditions", K(ret));
  } else if (OB_FAIL(get_extra_condition_from_parent(parent_stmts, stmt, conditions))) {
    LOG_WARN("failed to get null reject select", K(ret));
  } else if (OB_FAIL(ObTransformUtils::right_join_to_left(stmt))) {
    LOG_WARN("failed to change right outer join to left", K(ret));
  } else {
    common::ObArray<FromItem> from_item_list;
    common::ObArray<JoinedTable*> joined_table_list;
    // do transform in from items
    ObIArray<FromItem>& items = stmt->get_from_items();
    for (int64_t i = 0; OB_SUCC(ret) && i < items.count(); ++i) {
      ret = recursive_eliminate_outer_join_in_table_item(
          stmt, stmt->get_table_item(items.at(i)), from_item_list, joined_table_list, conditions, true, trans_happened);
    }
    // after trans, reset table item
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(stmt->reset_from_item(from_item_list))) {
      LOG_WARN("failed to reset table_items", K(ret));
    } else if (OB_FAIL(stmt->get_joined_tables().assign(joined_table_list))) {
      LOG_WARN("failed to reset joined table container", K(ret));
    } else {
      LOG_TRACE("succ to to do outer join elimination");
    }
  }
  return ret;
}

int ObTransformEliminateOuterJoin::recursive_eliminate_outer_join_in_table_item(ObDMLStmt* stmt,
    TableItem* cur_table_item, common::ObIArray<FromItem>& from_item_list,
    common::ObIArray<JoinedTable*>& joined_table_list, ObIArray<ObRawExpr*>& conditions, bool should_move_to_from_list,
    bool& trans_happened)
{
  int ret = OB_SUCCESS;
  bool do_trans = false;
  bool is_stack_overflow = false;
  bool is_my_joined_table_type = false;
  JoinedTable* process_join = NULL;
  if (OB_ISNULL(cur_table_item) || OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param has null", K(ret));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_stack_overflow));
  } else if (OB_FAIL(is_outer_joined_table_type(stmt,
                 cur_table_item,
                 from_item_list,
                 joined_table_list,
                 should_move_to_from_list,
                 is_my_joined_table_type))) {
    LOG_WARN("check joined table type failed", K(ret));
  } else if (is_my_joined_table_type) {
    process_join = static_cast<JoinedTable*>(cur_table_item);
    ret = do_eliminate_outer_join(
        stmt, process_join, from_item_list, joined_table_list, conditions, should_move_to_from_list, do_trans);
    if (OB_SUCC(ret)) {
      TableItem* l_child = process_join->left_table_;
      TableItem* r_child = process_join->right_table_;
      ObSEArray<ObRawExpr*, 4> left_child_conditions;
      if (OB_FAIL(append_array_no_dup(left_child_conditions, conditions))) {
        LOG_WARN("failed to append array no dup.", K(ret));
      } else if (process_join->joined_type_ == INNER_JOIN &&
                 OB_FAIL(append_array_no_dup(left_child_conditions, process_join->join_conditions_))) {
        LOG_WARN("failed to append array no dup.", K(ret));
      } else if (OB_FAIL(SMART_CALL(recursive_eliminate_outer_join_in_table_item(stmt,
                     l_child,
                     from_item_list,
                     joined_table_list,
                     left_child_conditions,
                     do_trans & should_move_to_from_list,
                     trans_happened)))) {
        LOG_WARN("failed to process the left child", K(ret), K(r_child));
      } else {
        ObSEArray<ObRawExpr*, 4> right_child_conditions;
        if (OB_FAIL(append_array_no_dup(right_child_conditions, conditions))) {
          LOG_WARN("failed to append array no dup.", K(ret));
        } else if (process_join->joined_type_ != FULL_OUTER_JOIN && process_join->joined_type_ != CONNECT_BY_JOIN &&
                   OB_FAIL(append_array_no_dup(right_child_conditions, process_join->join_conditions_))) {
          LOG_WARN("failed to append array no dup.", K(ret));
        } else if (OB_FAIL(SMART_CALL(recursive_eliminate_outer_join_in_table_item(stmt,
                       r_child,
                       from_item_list,
                       joined_table_list,
                       right_child_conditions,
                       do_trans & should_move_to_from_list,
                       trans_happened)))) {
          LOG_WARN("failed to process the right child", K(ret), K(r_child));
        } else {
          /*do nothing*/
        }
      }
      if (do_trans && should_move_to_from_list) {
        trans_happened = true;
      } else {
        /*do nothing*/
      }
    } else {
      /*do nothing*/
    }
  } else {
    /*do nothing*/
  }
  return ret;
}

int ObTransformEliminateOuterJoin::is_outer_joined_table_type(ObDMLStmt* stmt, TableItem* cur_table_item,
    common::ObIArray<FromItem>& from_item_list, common::ObIArray<JoinedTable*>& joined_table_list,
    bool should_move_to_from_list, bool& is_my_joined_table_type)
{
  int ret = OB_SUCCESS;
  JoinedTable* cur_joined_table_item = NULL;
  is_my_joined_table_type = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(cur_table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param has null", K(ret));
  } else if (!cur_table_item->is_joined_table()) {
    if (should_move_to_from_list) {
      FromItem from_item;
      from_item.is_joined_ = false;
      from_item.table_id_ = cur_table_item->table_id_;
      if (OB_FAIL(from_item_list.push_back(from_item))) {
        LOG_WARN("failed to push to from item list", K(ret));
      } else {
        /* do nothing */
      }
    } else {
      /*do nothing*/
    }
  } else {
    cur_joined_table_item = static_cast<JoinedTable*>(cur_table_item);
    if (OB_ISNULL(cur_joined_table_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table item should not be null", K(ret));
    } else if (LEFT_OUTER_JOIN == cur_joined_table_item->joined_type_ ||
               FULL_OUTER_JOIN == cur_joined_table_item->joined_type_ ||
               INNER_JOIN == cur_joined_table_item->joined_type_ ||
               CONNECT_BY_JOIN == cur_joined_table_item->joined_type_) {
      is_my_joined_table_type = true;
    } else {
      if (should_move_to_from_list) {
        FromItem from_item;
        from_item.is_joined_ = true;
        from_item.table_id_ = cur_table_item->table_id_;
        if (OB_FAIL(from_item_list.push_back(from_item))) {
          LOG_WARN("failed to push to from item list", K(ret));
        } else if (OB_FAIL(joined_table_list.push_back(cur_joined_table_item))) {
          LOG_WARN("failed to push to joined table list", K(ret));
        } else {
          /* do nothing */
        }
      } else {
        /*do nothing*/
      }
    }
  }
  return ret;
}

int ObTransformEliminateOuterJoin::do_eliminate_outer_join(ObDMLStmt* stmt, JoinedTable* cur_joined_table,
    common::ObIArray<FromItem>& from_item_list, common::ObIArray<JoinedTable*>& joined_table_list,
    ObIArray<ObRawExpr*>& conditions, bool should_move_to_from_list, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  bool can_eliminate = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(cur_joined_table) || OB_ISNULL(cur_joined_table->left_table_) ||
      OB_ISNULL(cur_joined_table->right_table_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param has null", K(ret));
  } else {
    if (FULL_OUTER_JOIN == cur_joined_table->joined_type_) {
      if (OB_FAIL(can_be_eliminated(stmt, cur_joined_table, conditions, can_eliminate))) {
        LOG_WARN("failed to test eliminated left join condition", K(ret));
      } else if (can_eliminate) {
        TableItem* temp = cur_joined_table->left_table_;
        cur_joined_table->left_table_ = cur_joined_table->right_table_;
        cur_joined_table->right_table_ = temp;
        cur_joined_table->joined_type_ = LEFT_OUTER_JOIN;
      } else {
        TableItem* temp = cur_joined_table->left_table_;
        cur_joined_table->left_table_ = cur_joined_table->right_table_;
        cur_joined_table->right_table_ = temp;
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(can_be_eliminated(stmt, cur_joined_table, conditions, can_eliminate))) {
          LOG_WARN("failed to test eliminated right join condition", K(ret));
        } else if (can_eliminate) {
          if (cur_joined_table->joined_type_ == LEFT_OUTER_JOIN) {
            cur_joined_table->joined_type_ = INNER_JOIN;
          } else {
            TableItem* temp = cur_joined_table->left_table_;
            cur_joined_table->left_table_ = cur_joined_table->right_table_;
            cur_joined_table->right_table_ = temp;
            cur_joined_table->joined_type_ = LEFT_OUTER_JOIN;
          }
        } else {
          if (cur_joined_table->joined_type_ == FULL_OUTER_JOIN) {
            TableItem* temp = cur_joined_table->left_table_;
            cur_joined_table->left_table_ = cur_joined_table->right_table_;
            cur_joined_table->right_table_ = temp;
          } else {
            /*do nothing*/
          }
        }
      } else {
        /*do nothing*/
      }
    } else if (LEFT_OUTER_JOIN == cur_joined_table->joined_type_) {
      if (OB_FAIL(can_be_eliminated(stmt, cur_joined_table, conditions, can_eliminate))) {
        LOG_WARN("failed to test eliminated left join condition", K(ret));
      } else if (can_eliminate) {
        cur_joined_table->joined_type_ = INNER_JOIN;
      } else {
        /*do nothing*/
      }
    } else {
      /*do nothing for inner join or connect by join*/
    }
    if (OB_SUCC(ret)) {
      if (INNER_JOIN == cur_joined_table->joined_type_) {
        if (should_move_to_from_list) {
          trans_happened = true;
          if (OB_FAIL(stmt->add_condition_exprs(cur_joined_table->join_conditions_))) {
            LOG_WARN("failed to move join conditions into where clause", K(ret));
          } else if (OB_FAIL(append_array_no_dup(conditions, cur_joined_table->join_conditions_))) {
            LOG_WARN("failed to append conditions.", K(ret));
          } else {
            /* do nothing. */
          }
        } else {
          /*do nothing*/
        }
      } else if (should_move_to_from_list) {
        FromItem from_item;
        from_item.is_joined_ = true;
        from_item.table_id_ = cur_joined_table->table_id_;
        if (OB_FAIL(from_item_list.push_back(from_item))) {
          LOG_WARN("faled to push back origin from item", K(ret));
        } else if (OB_FAIL(joined_table_list.push_back(cur_joined_table))) {
          LOG_WARN("faield to push back origin joined item", K(ret));
        } else {
          /* do nothing. */
        }
      } else {
        /*do nothing*/
      }
    } else {
      /*do nothing*/
    }
  }
  return ret;
}

int ObTransformEliminateOuterJoin::can_be_eliminated(
    ObDMLStmt* stmt, JoinedTable* joined_table, ObIArray<ObRawExpr*>& conditions, bool& can_eliminate)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(can_be_eliminated_with_null_reject(stmt, joined_table, conditions, can_eliminate))) {
    LOG_WARN("failed to test eliminated condition with null reject", K(ret));
  } else if (can_eliminate) {
    /*do nothing*/
  } else if (OB_FAIL(can_be_eliminated_with_foreign_primary_join(stmt, joined_table, conditions, can_eliminate))) {
    LOG_WARN("failed to test eliminated condition with foreign primary join", K(ret));
  } else {
    /*do nothing*/
  }
  return ret;
}

int ObTransformEliminateOuterJoin::can_be_eliminated_with_null_reject(
    ObDMLStmt* stmt, JoinedTable* joined_table, ObIArray<ObRawExpr*>& conditions, bool& has_null_reject)
{
  int ret = OB_SUCCESS;
  const TableItem* right_table;
  ObSEArray<const ObRawExpr*, 8> col_exprs;
  ObSqlBitSet<> right_table_ids;
  has_null_reject = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(joined_table)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("params have null", K(ret), K(stmt), K(joined_table));
  } else if (INNER_JOIN == joined_table->joined_type_) {
    has_null_reject = true;
  } else if (OB_ISNULL(right_table = joined_table->right_table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("right table is null", K(ret));
  } else if (OB_FAIL(ObTransformUtils::get_table_rel_ids(*stmt, *(joined_table->right_table_), right_table_ids))) {
    LOG_WARN("failed to get right table relation ids", K(ret));
  }

  // check whether any condition is null reject for any column of the right table
  for (int64_t i = 0; OB_SUCC(ret) && !has_null_reject && i < conditions.count(); ++i) {
    col_exprs.reuse();
    ObRawExpr* condition = conditions.at(i);
    bool is_valid_condition = false;
    if (OB_ISNULL(condition)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("condition is null", K(ret), K(condition));
    } else if (OB_FAIL(extract_columns(condition, right_table_ids, stmt->get_current_level(), col_exprs))) {
      LOG_WARN("failed to extract columns", K(ret));
    } else if (OB_FAIL(ObTransformUtils::is_null_reject_condition(condition, col_exprs, has_null_reject))) {
      LOG_WARN("failed to check whether the condition is null rejection", K(ret));
    } else { /* do nothing */
    }
  }
  return ret;
}

int ObTransformEliminateOuterJoin::can_be_eliminated_with_foreign_primary_join(
    ObDMLStmt* stmt, JoinedTable* joined_table, ObIArray<ObRawExpr*>& conditions, bool& can_eliminate)
{
  int ret = OB_SUCCESS;
  ObSEArray<const ObRawExpr*, 8> left_col_exprs, right_col_exprs;
  ObSqlBitSet<> left_table_ids;
  ObSqlBitSet<> right_table_ids;
  bool is_foreign_primary_join = false;
  bool is_first_table_parent = false;
  bool is_all_foreign_columns_not_null = false;
  bool is_simple_condition = false;
  bool is_foreign_rely = false;
  share::schema::ObForeignKeyInfo* foreign_key_info = NULL;
  can_eliminate = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_) || OB_ISNULL(joined_table) || OB_ISNULL(joined_table->left_table_) ||
      OB_ISNULL(joined_table->right_table_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("params have null", K(ret), K(stmt), K(joined_table));
  } else if (INNER_JOIN == joined_table->joined_type_) {
    can_eliminate = true;
  } else if (!joined_table->left_table_->is_basic_table() || !joined_table->right_table_->is_basic_table()) {
    /*do nothing*/
  } else if (OB_FAIL(is_simple_join_condition(joined_table->join_conditions_,
                 joined_table->left_table_,
                 joined_table->right_table_,
                 is_simple_condition))) {
    LOG_WARN("check is simple join condition failed", K(ret));
  } else if (!is_simple_condition) {
    // do nothing
  } else if (OB_FAIL(ObTransformUtils::get_table_rel_ids(*stmt, *(joined_table->left_table_), left_table_ids))) {
    LOG_WARN("failed to get left table rel ids", K(ret));
  } else if (OB_FAIL(ObTransformUtils::get_table_rel_ids(*stmt, *(joined_table->right_table_), right_table_ids))) {
    LOG_WARN("failed to get right table relation ids", K(ret));
  } else if (OB_FAIL(extract_columns_from_join_conditions(
                 joined_table->join_conditions_, left_table_ids, stmt->get_current_level(), left_col_exprs))) {
    LOG_WARN("failed to extract columns", K(ret));
  } else if (OB_FAIL(extract_columns_from_join_conditions(
                 joined_table->join_conditions_, right_table_ids, stmt->get_current_level(), right_col_exprs))) {
    LOG_WARN("failed to extract columns", K(ret));
  } else if (left_col_exprs.count() != right_col_exprs.count()) {
    /*do nothing*/
  } else if (OB_FAIL(ObTransformUtils::check_foreign_primary_join(joined_table->left_table_,
                 joined_table->right_table_,
                 left_col_exprs,
                 right_col_exprs,
                 ctx_->schema_checker_,
                 is_foreign_primary_join,
                 is_first_table_parent,
                 foreign_key_info))) {
    LOG_WARN("failed to check foreign primary join", K(ret));
  } else if (!is_foreign_primary_join || is_first_table_parent) {
    // do nothing
  } else if (OB_FAIL(ObTransformUtils::is_foreign_key_rely(ctx_->session_info_, foreign_key_info, is_foreign_rely))) {
    LOG_WARN("can not get foreign key info", K(ret));
  } else if (!is_foreign_rely) {
    // do nothing
  } else if (OB_NOT_NULL((stmt->get_stmt_hint().get_part_hint(joined_table->right_table_->table_id_)))) {
    // do nothing
  } else if (OB_FAIL(is_all_columns_not_null(stmt, left_col_exprs, conditions, is_all_foreign_columns_not_null))) {
    LOG_WARN("check foreign columns is not null failed", K(ret));
  } else if (is_all_foreign_columns_not_null) {
    can_eliminate = true;
  } else {
    can_eliminate = false;
  }
  return ret;
}

int ObTransformEliminateOuterJoin::is_all_columns_not_null(
    ObDMLStmt* stmt, const ObIArray<const ObRawExpr*>& col_exprs, ObIArray<ObRawExpr*>& conditions, bool& is_not_null)
{
  int ret = OB_SUCCESS;
  is_not_null = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt is null", K(ret));
  } else {
    bool is_nullable = false;
    bool has_null_reject = false;
    const ObRawExpr* col_expr = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && !is_nullable && i < col_exprs.count(); ++i) {
      if (OB_ISNULL(col_expr = col_exprs.at(i)) || OB_UNLIKELY(!col_expr->is_column_ref_expr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected column ref expr", K(ret), K(col_expr));
      } else if (OB_FAIL(ObTransformUtils::is_column_nullable(
                     stmt, ctx_->schema_checker_, static_cast<const ObColumnRefRawExpr*>(col_expr), is_nullable))) {
        LOG_WARN("check column is not null failed", K(ret));
      } else if (!is_nullable) {
        /*do nothing*/
      } else if (OB_FAIL(ObTransformUtils::has_null_reject_condition(conditions, col_expr, has_null_reject))) {
        LOG_WARN("failed to check whether the condition is null rejection", K(ret));
      } else {
        is_nullable = !has_null_reject;
      }
    }
    if (is_nullable) {
      is_not_null = false;
    } else {
      is_not_null = true;
    }
  }
  return ret;
}

int ObTransformEliminateOuterJoin::is_simple_join_condition(const ObIArray<ObRawExpr*>& join_condition,
    const TableItem* left_table, const TableItem* right_table, bool& is_simple)
{
  int ret = OB_SUCCESS;
  is_simple = true;
  for (int64_t i = 0; OB_SUCC(ret) && is_simple && i < join_condition.count(); ++i) {
    ObRawExpr* expr = join_condition.at(i);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is null", K(ret));
    } else if (T_OP_EQ == expr->get_expr_type()) {
      ObOpRawExpr* op = static_cast<ObOpRawExpr*>(expr);
      if (OB_ISNULL(op)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("op expr is null", K(ret));
      } else if (OB_ISNULL(op->get_param_expr(0)) || OB_ISNULL(op->get_param_expr(1))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("EQ expr has null param", K(ret));
      } else if (op->get_param_expr(0)->has_flag(IS_COLUMN) && op->get_param_expr(1)->has_flag(IS_COLUMN)) {
        ObColumnRefRawExpr* col1 = static_cast<ObColumnRefRawExpr*>(op->get_param_expr(0));
        ObColumnRefRawExpr* col2 = static_cast<ObColumnRefRawExpr*>(op->get_param_expr(1));
        if ((col1->get_table_id() == left_table->table_id_ && col2->get_table_id() == right_table->table_id_) ||
            (col1->get_table_id() == right_table->table_id_ && col2->get_table_id() == left_table->table_id_)) {
          /*do nothing*/
        } else {
          is_simple = false;
        }
      } else {
        is_simple = false;
      }
    } else {
      is_simple = false;
    }
  }
  return ret;
}

int ObTransformEliminateOuterJoin::extract_columns(const ObRawExpr* expr, const ObSqlBitSet<>& rel_ids,
    const int32_t expr_level, common::ObIArray<const ObRawExpr*>& col_exprs)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_stack_overflow));
  } else if (expr->is_column_ref_expr()) {
    if (rel_ids.is_superset(expr->get_relation_ids()) && expr_level == expr->get_expr_level()) {
      if (OB_FAIL(col_exprs.push_back(static_cast<const ObColumnRefRawExpr*>(expr)))) {
        LOG_WARN("failed to push back col expr", K(ret));
      } else { /*do nothing*/
      }
    } else { /*do nothing*/
    }
  } else if (expr->has_flag(CNT_COLUMN)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(extract_columns(expr->get_param_expr(i), rel_ids, expr_level, col_exprs)))) {
        LOG_WARN("failed to extract columns from param", K(ret));
      } else { /*do nothing*/
      }
    }
  } else { /*do nothing*/
  }
  return ret;
}

int ObTransformEliminateOuterJoin::extract_columns_from_join_conditions(const ObIArray<ObRawExpr*>& exprs,
    const ObSqlBitSet<>& rel_ids, const int32_t expr_level, common::ObIArray<const ObRawExpr*>& col_exprs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    if (OB_FAIL(extract_columns(exprs.at(i), rel_ids, expr_level, col_exprs))) {
      LOG_WARN("extract columns failed", K(ret));
    } else {
      /*do nothing*/
    }
  }
  return ret;
}

int ObTransformEliminateOuterJoin::get_extra_condition_from_parent(
    ObIArray<ObParentDMLStmt>& parent_stmts, ObDMLStmt*& stmt, ObIArray<ObRawExpr*>& conditions)
{
  int ret = OB_SUCCESS;
  bool has_rownum = false;
  ObDMLStmt* parent_stmt = NULL;
  TableItem* table_item = NULL;
  if (parent_stmts.empty()) {
    // do nothing
  } else if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(stmt->has_rownum(has_rownum))) {
    LOG_WARN("failed to check stmt has rownum", K(ret));
  } else if (stmt->has_limit() || stmt->has_sequence() || has_rownum) {
    // do nothing
  } else if (OB_ISNULL(parent_stmt = parent_stmts.at(parent_stmts.count() - 1).stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(get_generated_table_item(*parent_stmt, stmt, table_item))) {
    LOG_WARN("failed to get table_item", K(ret));
  } else if (OB_NOT_NULL(table_item)) {
    ObSEArray<ObColumnRefRawExpr*, 8> table_columns;
    ObSEArray<ObRawExpr*, 16> all_conditions;
    bool has_null_reject = false;
    ObColumnRefRawExpr* col = NULL;
    ObRawExpr* select_expr = NULL;
    if (OB_FAIL(parent_stmt->get_column_exprs(table_item->table_id_, table_columns))) {
      LOG_WARN("failed to get column exprs", K(ret));
    } else if (OB_FAIL(get_table_related_condition(*parent_stmt, table_item, all_conditions))) {
      LOG_WARN("failed to get table related condition", K(ret));
    } else if (OB_LIKELY(stmt->is_select_stmt())) {
      ObSelectStmt* child_stmt = static_cast<ObSelectStmt*>(stmt);
      for (int64_t i = 0; OB_SUCC(ret) && i < table_columns.count(); ++i) {
        if (OB_ISNULL(col = table_columns.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (OB_UNLIKELY(col->get_column_id() - 16 < 0 ||
                               col->get_column_id() - 16 >= child_stmt->get_select_item_size())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected view column", K(ret), K(*col));
        } else if (OB_ISNULL(select_expr = child_stmt->get_select_item(col->get_column_id() - 16).expr_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (select_expr->has_flag(CNT_AGG) || select_expr->has_flag(CNT_WINDOW_FUNC) ||
                   select_expr->has_flag(CNT_SUB_QUERY)) {
          // do nothing
        } else if (OB_FAIL(ObTransformUtils::has_null_reject_condition(all_conditions, col, has_null_reject))) {
          LOG_WARN("faield to check has null reject condition", K(ret));
        } else if (has_null_reject && OB_FAIL(conditions.push_back(select_expr))) {
          LOG_WARN("failed to push back expr", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTransformEliminateOuterJoin::get_generated_table_item(
    ObDMLStmt& parent_stmt, ObDMLStmt* child_stmt, TableItem*& table_item)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < parent_stmt.get_table_size(); ++i) {
    TableItem* table = parent_stmt.get_table_item(i);
    if (OB_ISNULL(table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(i));
    } else if (!table->is_generated_table()) {
      // do nothing
    } else if (table->ref_query_ == child_stmt) {
      table_item = table;
      break;
    }
  }
  return ret;
}

int ObTransformEliminateOuterJoin::get_table_related_condition(
    ObDMLStmt& stmt, const TableItem* table, ObIArray<ObRawExpr*>& conditions)
{
  int ret = OB_SUCCESS;
  bool is_semi_right_table = false;
  ;
  for (int64_t i = 0; OB_SUCC(ret) && !is_semi_right_table && i < stmt.get_semi_info_size(); ++i) {
    SemiInfo* semi_info = stmt.get_semi_infos().at(i);
    if (OB_ISNULL(semi_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (semi_info->right_table_id_ == table->table_id_) {
      // generated table is semi/anti join right table, table columns will only appear
      // in semi conditions.
      is_semi_right_table = true;
      ret = conditions.assign(semi_info->semi_conditions_);
    }
  }
  if (OB_SUCC(ret) && !is_semi_right_table) {
    // generated table is not semi/anti join right table, table columns will appear in
    // where, joined table and semi join condition
    if (OB_FAIL(conditions.assign(stmt.get_condition_exprs()))) {
      LOG_WARN("failed to get condition exprs", K(ret));
    } else if (OB_FAIL(get_condition_from_joined_table(stmt, table, conditions))) {
      LOG_WARN("failed to get extra conditions", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_semi_info_size(); ++i) {
        SemiInfo* semi_info = stmt.get_semi_infos().at(i);
        if (OB_ISNULL(semi_info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (semi_info->is_semi_join()) {
          // anti join condition is not null reject
          ret = append(conditions, semi_info->semi_conditions_);
        }
      }
    }
  }
  return ret;
}

int ObTransformEliminateOuterJoin::get_condition_from_joined_table(
    ObDMLStmt& stmt, const TableItem* target_table, ObIArray<ObRawExpr*>& conditions)
{
  int ret = OB_SUCCESS;
  bool add_on_condition = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_joined_tables().count(); ++i) {
    JoinedTable* joined_table = stmt.get_joined_tables().at(i);
    if (OB_ISNULL(joined_table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (!ObOptimizerUtil::find_item(joined_table->single_table_ids_, target_table->table_id_)) {
      // do nothing
    } else if (OB_FAIL(extract_joined_table_condition(joined_table, target_table, conditions, add_on_condition))) {
      LOG_WARN("faield to extract joined table condition", K(ret));
    }
  }
  return ret;
}

int ObTransformEliminateOuterJoin::extract_joined_table_condition(
    TableItem* table_item, const TableItem* target_table, ObIArray<ObRawExpr*>& conditions, bool& add_on_condition)
{
  int ret = OB_SUCCESS;
  add_on_condition = false;
  if (OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (table_item == target_table) {
    add_on_condition = true;
  } else if (table_item->is_joined_table()) {
    JoinedTable* joined_table = static_cast<JoinedTable*>(table_item);
    bool check_left = false;
    bool check_right = false;
    if (LEFT_OUTER_JOIN == joined_table->joined_type_) {
      check_right = true;
    } else if (RIGHT_OUTER_JOIN == joined_table->joined_type_) {
      check_left = true;
    } else if (INNER_JOIN == joined_table->joined_type_) {
      check_left = true;
      check_right = true;
    }
    if (OB_SUCC(ret) && check_left && !add_on_condition) {
      if (OB_FAIL(SMART_CALL(
              extract_joined_table_condition(joined_table->left_table_, target_table, conditions, add_on_condition)))) {
        LOG_WARN("failed to extract joined table condition", K(ret));
      }
    }

    if (OB_SUCC(ret) && check_right && !add_on_condition) {
      if (OB_FAIL(SMART_CALL(extract_joined_table_condition(
              joined_table->right_table_, target_table, conditions, add_on_condition)))) {
        LOG_WARN("failed to extract joined table condition", K(ret));
      }
    }

    if (OB_SUCC(ret) && add_on_condition && OB_FAIL(append(conditions, joined_table->get_join_conditions()))) {
      LOG_WARN("failed to append join conditions", K(ret));
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
