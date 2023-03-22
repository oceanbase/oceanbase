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

#ifndef OCEANBASE_SQL_RESOLVER_DML_OB_MULTI_TABLE_INSERT_RESOLVER_H_
#define OCEANBASE_SQL_RESOLVER_DML_OB_MULTI_TABLE_INSERT_RESOLVER_H_
#include "sql/resolver/dml/ob_del_upd_resolver.h"
#include "sql/resolver/dml/ob_del_upd_stmt.h"
#include "sql/resolver/dml/ob_insert_all_stmt.h"
namespace oceanbase
{
namespace share
{
namespace schema
{
class ObColumnSchemaV2;
}
}
namespace sql
{
  /**
   * INSERT ALL syntax from Oracle:
   *   (https://docs.oracle.com/cd/B19306_01/server.102/b14200/statements_9014.htm)
   *
   * 实现review:
   *
   *
   *   INSERT [ALL | FIRST ]
   *   [WHEN CONDITIONS THEN] INTO tbl_name [VALUES],INTO tbl_name [VALUES],....
   *   [WHEN CONDITIONS THEN] INTO tbl_name [VALUES],INTO tbl_name [VALUES],....
   *   ....
   *   [ELSE] [INTO tbl_name [VALUES],INTO tbl_name [VALUES],....]
   *   subquery
   */
class InsertValueNode
{
  public:
  InsertValueNode() : table_idx_(-1), insert_value_node_(NULL) {}
  ~InsertValueNode() {}
  TO_STRING_KV(K_(table_idx));
  int64_t table_idx_;
  const ParseNode *insert_value_node_;
};

class InsertConditionNode
{
  public:
  InsertConditionNode() : table_cnt_(0), insert_cond_node_(NULL) {}
  ~InsertConditionNode() {}
  TO_STRING_KV(K_(table_cnt));
  int64_t table_cnt_;
  const ParseNode *insert_cond_node_;
};

class ObMultiTableInsertResolver : public ObDelUpdResolver
{
public:
  explicit ObMultiTableInsertResolver(ObResolverParams &params);
  virtual ~ObMultiTableInsertResolver();
  virtual int resolve(const ParseNode &parse_tree);
  const ObInsertAllStmt* get_insert_all_stmt() const { return static_cast<const ObInsertAllStmt*>(stmt_); }
  ObInsertAllStmt *get_insert_all_stmt() { return static_cast<ObInsertAllStmt*>(stmt_); }
  common::ObIArray<bool> &get_is_oracle_tmp_table_array() {
    return is_oracle_tmp_table_array_;
  }
  common::ObIArray<common::ObSEArray<uint64_t, 4>> &get_the_missing_label_se_columns_array() {
    return the_missing_label_se_columns_array_;
  }
  common::ObIArray<uint64_t> &get_the_missing_label_se_columns() {
    return the_missing_label_se_columns_;
  }
protected:
  int mock_values_column_ref(const ObColumnRefRawExpr *column_ref, ObInsertAllTableInfo& table_info);
  virtual int find_value_desc(ObInsertTableInfo &table_info, uint64_t column_id, ObRawExpr *&column_ref) override;
private:
  int resolve_multi_table_insert(const ParseNode &node);
  int resolve_multi_insert_subquey(const ParseNode &subquery_node);
  int resolve_multi_insert_clause(const ParseNode &insert_list_node,
                                  common::ObIArray<InsertValueNode> &multi_insert_values_node,
                                  int64_t when_conds_idx = -1);
  int resolve_insert_table_node(const ParseNode &insert_table_node, bool has_value_node, int64_t when_conds_idx);
  int resolve_insert_table_columns(const ParseNode *node,
                                   ObInsertAllTableInfo& table_info,
                                   bool has_value_node);
  int resolve_insert_values_node(const ParseNode *insert_values_node, int64_t table_offset);
  int resolve_multi_conditions_insert_clause(
                                     const ParseNode &node,
                                     common::ObIArray<InsertValueNode> &multi_insert_values_node,
                                     common::ObIArray<InsertConditionNode> &multi_insert_cond_node);
  int set_base_info_for_multi_table_insert();
  int add_multi_table_column_conv_function();
  int get_new_columns_exprs(const ObInsertAllTableInfo& table_info,
                            ObIArray<ObRawExpr*> &old_column_exprs,
                            ObIArray<ObRawExpr*> &new_column_exprs);
  int check_exprs_sequence(const common::ObIArray<ObRawExpr*> &condition_exprs);
  int generate_insert_all_table_info(const TableItem& item,
                                     const int64_t when_cond_idx,
                                     ObInsertAllTableInfo*& table_info);
  int generate_insert_all_constraint();
  int refine_table_check_constraint(ObInsertAllTableInfo& table_info);
  int add_new_sel_item_for_oracle_label_security_table(ObSelectStmt &select_stmt);
private:
  common::ObSEArray<bool, 4> is_oracle_tmp_table_array_;
  common::ObSEArray<common::ObSEArray<uint64_t, 4>, 4> the_missing_label_se_columns_array_;
  common::ObSEArray<uint64_t, 4> the_missing_label_se_columns_;
};

}//sql
}//oceanbase
#endif /* OCEANBASE_SQL_RESOLVER_DML_OB_MULTI_TABLE_INSERT_RESOLVER_H_ */
