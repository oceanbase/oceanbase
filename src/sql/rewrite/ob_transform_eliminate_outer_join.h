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

#ifndef _OB_TRANSFORM_ELIMINATE_OUTER_JOIN_H
#define _OB_TRANSFORM_ELIMINATE_OUTER_JOIN_H

#include "sql/rewrite/ob_transform_rule.h"
#include "sql/resolver/dml/ob_select_stmt.h"

namespace oceanbase {
namespace sql {

class ObTransformEliminateOuterJoin : public ObTransformRule {
public:
  explicit ObTransformEliminateOuterJoin(ObTransformerCtx* ctx) : ObTransformRule(ctx, TransMethod::POST_ORDER)
  {}

  virtual ~ObTransformEliminateOuterJoin()
  {}

  virtual int transform_one_stmt(
      common::ObIArray<ObParentDMLStmt>& parent_stmts, ObDMLStmt*& stmt, bool& trans_happened) override;

private:
  int eliminate_outer_join(common::ObIArray<ObParentDMLStmt>& parent_stmts, ObDMLStmt*& stmt, bool& trans_happened);

  int recursive_eliminate_outer_join_in_table_item(ObDMLStmt* stmt, TableItem* cur_table_item,
      common::ObIArray<FromItem>& from_item_list, common::ObIArray<JoinedTable*>& joined_table_list,
      ObIArray<ObRawExpr*>& conditions, bool should_move_to_from_list, bool& trans_happened);

  int is_outer_joined_table_type(ObDMLStmt* stmt, TableItem* cur_table_item, common::ObIArray<FromItem>& from_item_list,
      common::ObIArray<JoinedTable*>& joined_table_list, bool should_move_to_from_list, bool& is_my_joined_table_type);

  int do_eliminate_outer_join(ObDMLStmt* stmt, JoinedTable* cur_joined_table,
      common::ObIArray<FromItem>& from_item_list, common::ObIArray<JoinedTable*>& joined_table_list,
      ObIArray<ObRawExpr*>& conditions, bool should_move_to_from_list, bool& trans_happened);

  int can_be_eliminated(
      ObDMLStmt* stmt, JoinedTable* joined_table, ObIArray<ObRawExpr*>& conditions, bool& can_eliminate);

  int can_be_eliminated_with_null_reject(
      ObDMLStmt* stmt, JoinedTable* joined_table, ObIArray<ObRawExpr*>& conditions, bool& has_null_reject);

  int can_be_eliminated_with_foreign_primary_join(
      ObDMLStmt* stmt, JoinedTable* joined_table, ObIArray<ObRawExpr*>& conditions, bool& can_eliminate);

  int is_all_columns_not_null(ObDMLStmt* stmt, const ObIArray<const ObRawExpr*>& col_exprs,
      ObIArray<ObRawExpr*>& conditions, bool& is_not_null);

  int is_simple_join_condition(const ObIArray<ObRawExpr*>& join_condition, const TableItem* left_table,
      const TableItem* right_table, bool& is_simple);

  int extract_columns(const ObRawExpr* expr, const ObSqlBitSet<>& rel_ids, const int32_t expr_level,
      common::ObIArray<const ObRawExpr*>& col_exprs);

  int extract_columns_from_join_conditions(const ObIArray<ObRawExpr*>& exprs, const ObSqlBitSet<>& rel_ids,
      const int32_t expr_level, common::ObIArray<const ObRawExpr*>& col_exprs);

  int get_generated_table_item(ObDMLStmt& parent_stmt, ObDMLStmt* child_stmt, TableItem*& table_item);

  int get_extra_condition_from_parent(
      ObIArray<ObParentDMLStmt>& parent_stmts, ObDMLStmt*& stmt, ObIArray<ObRawExpr*>& conditions);

  int get_table_related_condition(ObDMLStmt& stmt, const TableItem* table, ObIArray<ObRawExpr*>& conditions);

  int get_condition_from_joined_table(ObDMLStmt& stmt, const TableItem* target_table, ObIArray<ObRawExpr*>& conditions);

  int extract_joined_table_condition(
      TableItem* table_item, const TableItem* target_table, ObIArray<ObRawExpr*>& conditions, bool& add_on_condition);
};
}  // namespace sql
}  // namespace oceanbase

#endif
