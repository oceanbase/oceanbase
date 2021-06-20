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

#ifndef OB_TRANSFORM_FULL_OUTER_JOIN_H
#define OB_TRANSFORM_FULL_OUTER_JOIN_H

#include "sql/rewrite/ob_transform_rule.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/ob_sql_context.h"

namespace oceanbase {
namespace sql {

class ObTransformFullOuterJoin : public ObTransformRule {
public:
  explicit ObTransformFullOuterJoin(ObTransformerCtx* ctx) : ObTransformRule(ctx, TransMethod::POST_ORDER)
  {}
  virtual ~ObTransformFullOuterJoin()
  {}

  virtual int transform_one_stmt(
      common::ObIArray<ObParentDMLStmt>& parent_stmts, ObDMLStmt*& stmt, bool& trans_happened) override;

private:
  virtual bool need_rewrite(const common::ObIArray<ObParentDMLStmt>& parent_stmts, const ObDMLStmt& stmt) override;

  int transform_full_outer_join(ObDMLStmt*& stmt, bool& trans_happened);

  int check_join_condition(ObDMLStmt* stmt, JoinedTable* table, bool& has_equal);

  int recursively_eliminate_full_join(ObDMLStmt* stmt, TableItem*& table_item, bool& trans_happened);

  int create_view_for_full_nl_join(ObDMLStmt* stmt, JoinedTable* joined_table, TableItem*& view_table);

  int update_stmt_exprs_for_view(ObDMLStmt* stmt, JoinedTable* joined_table, TableItem* view_table);

  int get_column_exprs_from_joined_table(ObDMLStmt* stmt, JoinedTable* joined_table, ObIArray<ObRawExpr*>& col_exprs);

  int create_left_outer_join_stmt(ObDMLStmt* stmt, ObSelectStmt*& left_stmt, JoinedTable* joined_table,
      ObQueryCtx* query_ctx, const ObRowDesc& table_hash);

  int create_left_anti_join_stmt(ObSelectStmt* left_stmt, ObQueryCtx* query_ctx, ObSelectStmt*& right_stmt);

  int create_select_items_for_left_join(ObDMLStmt* stmt, JoinedTable* joined_table, ObIArray<SelectItem>& select_items,
      ObIArray<ColumnItem>& column_items);

  int create_select_items_for_semi_join(ObDMLStmt* stmt, TableItem* from_table_item,
      const ObIArray<SelectItem>& select_items, ObIArray<SelectItem>& output_select_items);

  int switch_left_outer_to_semi_join(
      ObSelectStmt*& sub_stmt, JoinedTable* joined_table, const ObIArray<SelectItem>& select_items);

  int extract_idx_from_table_items(ObDMLStmt* sub_stmt, const TableItem* table_item, ObRelIds& rel_ids);

  int remove_tables_from_stmt(ObDMLStmt* stmt, const ObIArray<TableItem*>& table_items);
};

}  // namespace sql
}  // namespace oceanbase

#endif /* OB_TRANSFORM_FULL_OUTER_JOIN_H */
