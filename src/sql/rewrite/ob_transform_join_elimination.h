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

#ifndef OB_TRANSFORM_JOIN_ELIMINATION_H
#define OB_TRANSFORM_JOIN_ELIMINATION_H

#include "sql/ob_sql_context.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/rewrite/ob_transform_rule.h"

namespace oceanbase {
namespace share {
namespace schema {
class ObForeignKeyInfo;
}
}  // namespace share
namespace sql {
struct ObStmtMapInfo;
class ObTransformJoinElimination : public ObTransformRule {
  struct EliminationHelper {
    EliminationHelper() : count_(0), remain_(0)
    {}
    int push_back(TableItem* child, TableItem* parent, share::schema::ObForeignKeyInfo* info);
    int get_eliminable_group(TableItem*& child, TableItem*& parent, share::schema::ObForeignKeyInfo*& info, bool& find);
    int is_table_in_child_items(const TableItem* target, bool& find);
    int64_t get_remain()
    {
      return remain_;
    }
    int64_t count_;
    int64_t remain_;
    ObSEArray<TableItem*, 16> child_table_items_;
    ObSEArray<TableItem*, 16> parent_table_items_;
    ObSEArray<share::schema::ObForeignKeyInfo*, 16> foreign_key_infos_;
    ObSEArray<bool, 16> bitmap_;
  };

public:
  explicit ObTransformJoinElimination(ObTransformerCtx* ctx) : ObTransformRule(ctx, TransMethod::POST_ORDER)
  {}

  virtual ~ObTransformJoinElimination()
  {}

  virtual int transform_one_stmt(
      common::ObIArray<ObParentDMLStmt>& parent_stmts, ObDMLStmt*& stmt, bool& trans_happened) override;

private:
  int eliminate_join_self_foreign_key(ObDMLStmt* stmt, bool& trans_happened);

  int eliminate_join_in_from_base_table(ObDMLStmt* stmt, bool& trans_happened);

  int eliminate_join_in_from_item(
      ObDMLStmt* stmt, const ObIArray<FromItem>& from_items, SemiInfo* semi_info, bool& trans_happened);

  int extract_candi_table(ObDMLStmt* stmt, const ObIArray<FromItem>& from_items, ObIArray<TableItem*>& candi_tables,
      ObIArray<TableItem*>& candi_child_tables);

  int extract_candi_table(JoinedTable* table, ObIArray<TableItem*>& candi_child_tables);

  int eliminate_join_in_joined_table(ObDMLStmt* stmt, bool& trans_happened);

  int eliminate_join_in_joined_table(
      ObDMLStmt* stmt, FromItem& from_item, ObIArray<JoinedTable*>& joined_tables, bool& trans_happened);

  int eliminate_join_in_joined_table(ObDMLStmt* stmt, TableItem*& table_item, ObIArray<TableItem*>& child_candi_tables,
      ObIArray<ObRawExpr*>& trans_conditions, bool& trans_happened);

  int eliminate_candi_tables(ObDMLStmt* stmt, ObIArray<ObRawExpr*>& conds, ObIArray<TableItem*>& candi_tables,
      ObIArray<TableItem*>& child_candi_tables, bool& trans_happened);

  int do_join_elimination_self_key(ObDMLStmt* stmt, TableItem* source_table, TableItem* target_table,
      bool& trans_happened, EqualSets* equal_sets = NULL);

  int do_join_elimination_foreign_key(ObDMLStmt* stmt, const TableItem* child_table, const TableItem* parent_table,
      const share::schema::ObForeignKeyInfo* foreign_key_info);

  int classify_joined_table(JoinedTable* joined_table, ObIArray<JoinedTable*>& inner_join_tables,
      ObIArray<TableItem*>& outer_join_tables, ObIArray<TableItem*>& other_tables,
      ObIArray<ObRawExpr*>& inner_join_conds);

  int rebuild_joined_tables(ObDMLStmt* stmt, TableItem*& top_table, ObIArray<JoinedTable*>& inner_join_tables,
      ObIArray<TableItem*>& tables, ObIArray<ObRawExpr*>& join_conds);

  int extract_lossless_join_columns(JoinedTable* joined_table, const ObIArray<int64_t>& output_map,
      ObIArray<ObRawExpr*>& source_exprs, ObIArray<ObRawExpr*>& target_exprs);

  int extract_child_conditions(ObDMLStmt* stmt, TableItem* source_table, ObIArray<ObRawExpr*>& join_conditions,
      ObSqlBitSet<8, int64_t>& rel_ids);

  int adjust_relation_exprs(const ObSqlBitSet<8, int64_t>& rel_ids, const ObIArray<ObRawExpr*>& join_conditions,
      ObIArray<ObRawExpr*>& relation_exprs, bool& is_valid);

  int extract_equal_join_columns(const ObIArray<ObRawExpr*>& join_conds, const TableItem* source_table,
      const TableItem* target_table, ObIArray<ObRawExpr*>& source_exprs, ObIArray<ObRawExpr*>& target_exprs);

  int adjust_table_items(ObDMLStmt* stmt, TableItem* source_table, TableItem* target_table, ObStmtMapInfo& info);

  int reverse_select_items_map(const ObSelectStmt* source_stmt, const ObSelectStmt* target_stmt,
      const ObIArray<int64_t>& column_map, ObIArray<int64_t>& reverse_column_map);

  int create_missing_select_items(ObSelectStmt* source_stmt, ObSelectStmt* target_stmt,
      const ObIArray<int64_t>& column_map, const ObIArray<int64_t>& table_map);

  int trans_table_item(ObDMLStmt* stmt, const TableItem* source_table, const TableItem* target_table);

  int eliminate_outer_join(ObDMLStmt* stmt, bool& trans_happened);

  int eliminate_outer_join_in_from_items(ObDMLStmt* stmt, FromItem& from_item, ObIArray<uint64_t>& table_ids,
      ObIArray<JoinedTable*>& joined_tables, ObIArray<ObRawExpr*>& relation_exprs, bool& trans_happened);

  int eliminate_outer_join_in_joined_table(ObDMLStmt* stmt, TableItem*& table_item, ObIArray<uint64_t>& table_ids,
      ObIArray<ObRawExpr*>& relation_exprs, bool& trans_happen);

  int get_eliminable_tables(const ObDMLStmt* stmt, const ObIArray<ObRawExpr*>& conds,
      const ObIArray<TableItem*>& candi_tables, const ObIArray<TableItem*>& candi_child_tables,
      EliminationHelper& helper);

  int check_transform_validity_foreign_key(const ObDMLStmt* stmt, const ObIArray<ObRawExpr*>& join_conds,
      const TableItem* source_table, const TableItem* target_table, bool& can_be_eliminated,
      bool& is_first_table_parent, share::schema::ObForeignKeyInfo*& foreign_key_info);

  int check_transform_validity_outer_join(
      ObDMLStmt* stmt, JoinedTable* joined_table, ObIArray<ObRawExpr*>& relation_exprs, bool& is_valid);

  int check_has_semi_join_conditions(ObDMLStmt* stmt, const ObSqlBitSet<8, int64_t>& rel_ids, bool& is_valid);

  int check_all_column_primary_key(const ObDMLStmt* stmt, const uint64_t table_id,
      const share::schema::ObForeignKeyInfo* info, bool& all_primary_key);

  int trans_column_items_foreign_key(ObDMLStmt* stmt, const TableItem* child_table, const TableItem* parent_table,
      const share::schema::ObForeignKeyInfo* foreign_key_info);

  int get_child_column_id_by_parent_column_id(
      const share::schema::ObForeignKeyInfo* info, const uint64_t parent_column_id, uint64_t& child_column_id);

  int eliminate_semi_join_self_key(ObDMLStmt* stmt, bool& trans_happened);

  int eliminate_semi_join_self_foreign_key(ObDMLStmt* stmt, bool& trans_happened);

  int eliminate_semi_join_self_key(
      ObDMLStmt* stmt, SemiInfo* semi_info, ObIArray<ObRawExpr*>& conds, bool& trans_happened);

  int eliminate_semi_join_foreign_key(
      ObDMLStmt* stmt, SemiInfo* semi_info, ObIArray<ObRawExpr*>& conds, bool& trans_happened);

  int check_transform_validity_semi_self_key(ObDMLStmt* stmt, SemiInfo* semi_info, ObIArray<ObRawExpr*>& candi_conds,
      ObIArray<TableItem*>& left_tables, ObIArray<TableItem*>& right_tables, ObIArray<ObStmtMapInfo>& stmt_map_infos,
      ObIArray<int64_t>& rel_map_info, bool& can_be_eliminated);

  int check_semi_join_condition(ObDMLStmt* stmt, SemiInfo* semi_info, const ObIArray<TableItem*>& source_tables,
      const ObIArray<TableItem*>& target_tables, const ObIArray<ObStmtMapInfo>& stmt_map_infos,
      const ObIArray<int64_t>& rel_map_info, ObIArray<ObRawExpr*>& source_exprs, ObIArray<ObRawExpr*>& target_exprs,
      bool& is_simple_join_condition, bool& target_tables_have_filter, bool& is_simple_filter);

  int is_equal_column(const ObIArray<TableItem*>& source_tables, const ObIArray<TableItem*>& target_tables,
      const ObIArray<ObStmtMapInfo>& stmt_map_infos, const ObIArray<int64_t>& rel_map_info,
      const ObColumnRefRawExpr* source_col, const ObColumnRefRawExpr* target_col, bool& is_equal, bool& is_reverse);

  int is_equal_column(const TableItem* source_table, const TableItem* target_table, const ObIArray<int64_t>& output_map,
      uint64_t source_col_id, uint64_t target_col_id, bool& is_equal);

  int do_elimination_semi_join_self_key(ObDMLStmt* stmt, SemiInfo* semi_info, const ObIArray<TableItem*>& source_tables,
      const ObIArray<TableItem*>& target_tables, ObIArray<ObStmtMapInfo>& stmt_map_infos,
      const ObIArray<int64_t>& rel_map_info);

  int trans_semi_table_item(ObDMLStmt* stmt, const TableItem* source_table, const TableItem* target_table);

  int trans_semi_condition_exprs(ObDMLStmt* stmt, SemiInfo* semi_info);

  int eliminate_semi_join_foreign_key(ObDMLStmt* stmt, bool& trans_happened);

  int check_transform_validity_semi_foreign_key(ObDMLStmt* stmt, SemiInfo* semi_info, const ObIArray<ObRawExpr*>& conds,
      TableItem*& left_table, TableItem*& right_table, share::schema::ObForeignKeyInfo*& foreign_key_info,
      bool& can_be_eliminated);

  // functions below trans self equal conditions to not null
  int trans_self_equal_conds(ObDMLStmt* stmt);
  int trans_self_equal_conds(ObDMLStmt* stmt, ObIArray<ObRawExpr*>& cond_exprs);
  int add_is_not_null_if_needed(
      ObDMLStmt* stmt, ObIArray<ObColumnRefRawExpr*>& col_exprs, ObIArray<ObRawExpr*>& cond_exprs);

  int recursive_trans_equal_join_condition(ObDMLStmt* stmt, ObRawExpr* expr);
  int do_trans_equal_join_condition(ObDMLStmt* stmt, ObRawExpr* expr, bool& has_trans, ObRawExpr*& new_expr);
  int trans_column_expr(ObDMLStmt* stmt, ObRawExpr* expr, ObRawExpr*& new_expr);
};

}  // namespace sql
}  // namespace oceanbase

#endif  // OB_TRANSFORM_JOIN_ELIMINATION_H
