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

#ifndef OCEANBASE_SQL_RESOLVER_DML_OB_INSERT_RESOLVER_H_
#define OCEANBASE_SQL_RESOLVER_DML_OB_INSERT_RESOLVER_H_
#include "lib/hash/ob_placement_hashset.h"
#include "sql/resolver/dml/ob_dml_resolver.h"
#include "sql/resolver/dml/ob_insert_stmt.h"
namespace oceanbase {
namespace sql {
class ObSelectResolver;
class ObInsertResolver : public ObDMLResolver {
public:
  static const int64_t target_relation = 0;  /* target relation */
  static const int64_t column_list = 1;      /* column list */
  static const int64_t value_list = 2;       /* value list */
  static const int64_t value_from_subq = 3;  /* value from sub-query */
  static const int64_t is_replacement = 4;   /* is replacement */
  static const int64_t on_duplicate = 5;     /* on duplicate key update */
  static const int64_t insert_field = 6;     /* insert_field_spec*/
  static const int64_t insert_ignore = 7;    /* opt_ignore */
  static const int64_t insert_hint = 8;      /* hint */
  static const int64_t insert_returning = 9; /* returning */
public:
  explicit ObInsertResolver(ObResolverParams& params);
  virtual ~ObInsertResolver();

  virtual int resolve(const ParseNode& parse_tree);
  const ObInsertStmt* get_insert_stmt() const
  {
    return static_cast<const ObInsertStmt*>(stmt_);
  }
  ObInsertStmt* get_insert_stmt()
  {
    return static_cast<ObInsertStmt*>(stmt_);
  }

protected:
  int add_rowkey_columns_for_insert_up();
  int add_relation_columns();
  int process_values_function(ObRawExpr*& expr);
  virtual int find_value_desc(uint64_t column_id, ObRawExpr*& column_ref, uint64_t index = 0);
  virtual int mock_values_column_ref(const ObColumnRefRawExpr* column_ref);
  int resolve_single_table_insert(const ParseNode& node);
  int resolve_insert_columns(const ParseNode* node);
  int resolve_insert_values(const ParseNode* node);
  int resolve_insert_field(const ParseNode& insert_into);
  int resolve_insert_assign(const ParseNode& assign_list);
  int fill_insert_stmt_default_value();
  int check_and_fill_default_value(int64_t col_index, const ColumnItem* column);
  int check_and_build_default_expr(const ColumnItem* column, ObRawExpr*& expr);
  int fill_default_value_for_empty_brackets();
  virtual int resolve_order_item(const ParseNode& sort_node, OrderItem& order_item);
  virtual int resolve_column_ref_expr(const ObQualifiedName& q_name, ObRawExpr*& real_ref_expr);
  int set_insert_columns();
  int set_insert_values();
  int replace_auto_increment_values();
  int check_column_value_pair(
      common::ObArray<ObRawExpr*>* value_row, const int64_t row_index, const uint64_t value_count);
  int check_insert_column_duplicate(uint64_t column_id, bool& is_duplicate);
  // similar to the ones defined in ObInsertStmt
  int add_value_row(common::ObIArray<ObRawExpr*>& value_row);
  virtual int add_column_conv_function(uint64_t index = 0);
  int get_value_row_size(uint64_t& count) const;
  int replace_column_ref_for_check_constraint(ObRawExpr*& expr);
  virtual int replace_column_ref(common::ObArray<ObRawExpr*>* value_row, ObRawExpr*& expr);
  int set_table_columns(uint64_t table_offset = 0);
  int check_has_unique_index();
  int build_row_for_empty_brackets(common::ObArray<ObRawExpr*>& value_row);
  int resolve_values(const ParseNode& value_node);
  int resolve_insert_update_assignment(const ParseNode* node);
  int save_autoinc_params(uint64_t table_offset = 0);
  int replace_column_to_default(ObRawExpr*& origin);
  virtual int check_returning_validity();
  int resolve_multi_table_dml_info(uint64_t table_offset = 0);
  int resolve_duplicate_key_checker(uint64_t table_offset = 0);
  int resolve_dupkey_scan_info(const share::schema::ObTableSchema& index_schema, bool is_gui_lookup,
      ObDupKeyScanInfo& dupkey_scan_info, uint64_t table_offset = 0);
  int add_new_value_for_oracle_temp_table(ObIArray<ObRawExpr*>& value_row);
  int add_new_column_for_oracle_temp_table(uint64_t table_id);
  int add_select_list_for_set_stmt(ObSelectStmt& select_stmt);
  int add_select_items(ObSelectStmt& select_stmt, const ObIArray<SelectItem>& select_items);
  int add_new_sel_item_for_oracle_temp_table(ObSelectStmt& select_stmt);
  int resolve_unique_index_constraint_info(const share::schema::ObTableSchema& index_schema,
      ObUniqueConstraintInfo& constraint_info, uint64_t table_offset = 0);
  int create_session_row_label_expr(uint64_t column_id, ObRawExpr*& expr);
  int check_view_insertable();
  int inner_cast(common::ObIArray<ObColumnRefRawExpr*>& target_columns, ObSelectStmt& select_stmt);
  int check_insert_select_field(ObInsertStmt& insert_stmt, ObSelectStmt& select_stmt);
  int fill_index_dml_info_column_conv_exprs();

  int replace_column_with_mock_column(ObColumnRefRawExpr* gen_col, ObColumnRefRawExpr* value_desc);
  int build_column_conv_function_with_value_desc(const int64_t idx, ObRawExpr* column_ref, uint64_t m_index);

  int build_column_conv_function_with_default_expr(const int64_t idx, uint64_t m_index);

  // replace generate column's dependent column with new inserted value (the column convert func).
  int replace_gen_col_dependent_col();
  int replace_col_with_new_value(ObRawExpr*& expr);
  int remove_dup_dep_cols_for_heap_table(ObInsertStmt& stmt);
  ObSelectResolver*& get_sub_select_resolver()
  {
    return sub_select_resolver_;
  }
  bool is_oracle_tmp_table()
  {
    return is_oracle_tmp_table_;
  }
  void set_is_oracle_tmp_table(bool is_temp_table)
  {
    is_oracle_tmp_table_ = is_temp_table;
  }
  common::hash::ObPlacementHashSet<uint64_t>& get_insert_column_ids()
  {
    return insert_column_ids_;
  }
  common::ObIArray<bool>& get_is_oracle_tmp_table_array()
  {
    return is_oracle_tmp_table_array_;
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObInsertResolver);

private:
  common::hash::ObPlacementHashSet<uint64_t> insert_column_ids_;
  int64_t row_count_;
  ObSelectResolver* sub_select_resolver_;
  bool autoinc_col_added_;
  bool is_column_specify_;
  bool is_all_default_;
  bool is_oracle_tmp_table_;
  // used for multi table insert
  common::ObSEArray<bool, 4> is_oracle_tmp_table_array_;
};
}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SQL_RESOLVER_DML_OB_INSERT_RESOLVER_H_ */
