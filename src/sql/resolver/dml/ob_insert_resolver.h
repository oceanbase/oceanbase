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
// Copyright 2014 Alibaba Inc. All RightsReserved.
// Author:
// Normalizer:

#ifndef OCEANBASE_SQL_RESOLVER_DML_OB_INSERT_RESOLVER_H_
#define OCEANBASE_SQL_RESOLVER_DML_OB_INSERT_RESOLVER_H_
#include "lib/hash/ob_placement_hashset.h"
#include "sql/resolver/dml/ob_del_upd_resolver.h"
#include "sql/resolver/dml/ob_insert_stmt.h"
namespace oceanbase
{
namespace sql
{
class ObSelectResolver;
class ObInsertResolver : public ObDelUpdResolver
{
public:
  static const int64_t INSERT_NODE = 0;
  static const int64_t REPLACE_NODE = 1;
  static const int64_t HINT_NODE = 2;
  static const int64_t IGNORE_NODE = 3;
  static const int64_t OVERWRITE_NODE = 4;
  static const int64_t INTO_NODE = 0;
  static const int64_t VALUE_NODE = 1;
  static const int64_t DUPLICATE_NODE = 2;
  static const int64_t RETURNING_NODE = 2;
  static const int64_t ERR_LOG_NODE = 3;
  static const int64_t target_relation = 0; /* target relation */
  static const int64_t column_list = 1; /* column list */
  static const int64_t value_list = 2; /* value list */
  static const int64_t value_from_subq = 3; /* value from sub-query */
  static const int64_t is_replacement = 4; /* is replacement */
  static const int64_t on_duplicate = 5; /* on duplicate key update */
  static const int64_t insert_field = 6; /* insert_field_spec*/
  static const int64_t insert_ignore = 7; /* opt_ignore */
  static const int64_t insert_hint = 8; /* hint */
  static const int64_t insert_returning = 9; /* returning */
public:
  explicit ObInsertResolver(ObResolverParams &params);
  virtual ~ObInsertResolver();
  virtual int resolve(const ParseNode &parse_tree);
  const ObInsertStmt *get_insert_stmt() const { return static_cast<const ObInsertStmt*>(stmt_); }
  ObInsertStmt *get_insert_stmt()
  {
    return static_cast<ObInsertStmt*>(stmt_);
  }
  bool is_mock_for_row_alias() {return is_mock_;}
protected:
  int process_values_function(ObRawExpr *&expr) override;
  virtual int mock_values_column_ref(const ObColumnRefRawExpr *column_ref) override;
  virtual int resolve_order_item(const ParseNode &sort_node, OrderItem &order_item) override;
  virtual int resolve_column_ref_expr(const ObQualifiedName &q_name, ObRawExpr *&real_ref_expr) override;
private:
  int resolve_insert_clause(const ParseNode &node);
  int resolve_insert_field(const ParseNode &insert_into, TableItem*& table_item);
  int resolve_insert_assign(const ParseNode &assign_list);
  int fill_insert_stmt_default_value();
  int check_and_fill_default_value(int64_t col_index, const ColumnItem *column);
  int check_and_build_default_expr(const ColumnItem *column, ObRawExpr* &expr);
  int fill_default_value_for_empty_brackets();
  int set_insert_columns();
  int set_insert_values();
  int replace_auto_increment_values();
  virtual int replace_column_ref(common::ObArray<ObRawExpr*> *value_row,
                                 ObRawExpr *&expr,
                                 bool in_generated_column = false);
  int set();
  int resolve_values(const ParseNode &value_node,
                     ObIArray<uint64_t>& label_se_columns,
                     TableItem* table_item,
                     const ParseNode *duplicate_node);
  int check_table_and_column_name(const ObIArray<ObColumnRefRawExpr*> & value_desc,
                                  ObSelectStmt *select_stmt,
                                  ObString &ori_table_name,
                                  ObIArray<ObString> &ori_column_names,
                                  ObString &row_alias_table_name,
                                  ObIArray<ObString> &row_alias_column_names);
  int check_validity_of_duplicate_node(const ParseNode* node,
                                       ObString &ori_table_name,
                                       ObIArray<ObString> &ori_column_names,
                                       ObString &row_alias_table_name,
                                       ObIArray<ObString> &row_alias_column_names);
  int check_ambiguous_column(ObString &column_name,
                             ObIArray<ObString> &ori_column_names,
                             ObIArray<ObString> &row_alias_column_names);
  bool find_in_column(ObString &column_name,
                      ObIArray<ObString> &column_names);
  virtual int resolve_insert_update_assignment(const ParseNode *node, ObInsertTableInfo& table_info) override;
  int replace_column_to_default(ObRawExpr *&origin);
  virtual int check_returning_validity() override;
  int resolve_insert_constraint();
  int check_view_insertable();
  int inner_cast(common::ObIArray<ObColumnRefRawExpr*> &target_columns, ObSelectStmt &select_stmt);
  int check_insert_select_field(ObInsertStmt &insert_stmt,
                                ObSelectStmt &select_stmt,
                                bool is_mock = false);

  int replace_column_with_mock_column(ObColumnRefRawExpr *gen_col,
                                      ObColumnRefRawExpr *value_desc);

  ObSelectResolver *&get_sub_select_resolver() {
    return sub_select_resolver_;
  }
  int try_expand_returning_exprs();
  DISALLOW_COPY_AND_ASSIGN(ObInsertResolver);
private:
  int64_t row_count_;
  ObSelectResolver *sub_select_resolver_;
  bool autoinc_col_added_;
  bool is_mock_;
};
}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SQL_RESOLVER_DML_OB_INSERT_RESOLVER_H_ */
