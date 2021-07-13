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
#include "sql/resolver/dml/ob_insert_resolver.h"
#include "sql/resolver/dml/ob_insert_stmt.h"
namespace oceanbase {
namespace share {
namespace schema {
class ObColumnSchemaV2;
}
}  // namespace share
namespace sql {
/**
 * INSERT ALL syntax from Oracle:
 *   (https://docs.oracle.com/cd/B19306_01/server.102/b14200/statements_9014.htm)
 *
 * implement review:
 *
 *   INSERT [ALL | FIRST ]
 *   [WHEN CONDITIONS THEN] INTO tbl_name [VALUES],INTO tbl_name [VALUES],....
 *   [WHEN CONDITIONS THEN] INTO tbl_name [VALUES],INTO tbl_name [VALUES],....
 *   ....
 *   [ELSE] [INTO tbl_name [VALUES],INTO tbl_name [VALUES],....]
 *   subquery
 */
class InsertValueNode {
public:
  InsertValueNode() : table_idx_(-1), insert_value_node_(NULL)
  {}
  ~InsertValueNode()
  {}
  TO_STRING_KV(K_(table_idx));
  int64_t table_idx_;
  const ParseNode* insert_value_node_;
};

class InsertConditionNode {
public:
  InsertConditionNode() : table_cnt_(0), insert_cond_node_(NULL)
  {}
  ~InsertConditionNode()
  {}
  TO_STRING_KV(K_(table_cnt));
  int64_t table_cnt_;
  const ParseNode* insert_cond_node_;
};

class ObMultiTableInsertResolver : public ObInsertResolver {
public:
  explicit ObMultiTableInsertResolver(ObResolverParams& params);
  virtual ~ObMultiTableInsertResolver();
  virtual int resolve(const ParseNode& parse_tree);

protected:
  virtual int mock_values_column_ref(const ObColumnRefRawExpr* column_ref);
  virtual int find_value_desc(uint64_t column_id, ObRawExpr*& column_ref, uint64_t index = 0);

private:
  int resolve_multi_table_insert(const ParseNode& node);
  int resolve_multi_insert_subquey(const ParseNode& subquery_node);
  int resolve_multi_insert_clause(const ParseNode& insert_list_node,
      common::ObIArray<InsertValueNode>& multi_insert_values_node, int64_t when_conds_idx = -1);
  int resolve_insert_table_node(const ParseNode& insert_table_node, int64_t when_conds_idx);
  int resolve_insert_table_columns(const ParseNode* node);
  int resolve_insert_values_node(const ParseNode* insert_values_node, int64_t table_offset);
  int resolve_multi_conditions_insert_clause(const ParseNode& node,
      common::ObIArray<InsertValueNode>& multi_insert_values_node,
      common::ObIArray<InsertConditionNode>& multi_insert_cond_node);
  int set_base_info_for_multi_table_insert();
  int add_multi_table_column_conv_function();
  int replace_gen_col_dependent_col(uint64_t index);
  int replace_col_with_new_value(ObRawExpr*& expr, uint64_t index);
  int get_new_columns_exprs(ObInsertStmt* insert_stmt, TableItem* table_item, ObIArray<ObRawExpr*>& old_column_exprs,
      ObIArray<ObRawExpr*>& new_column_exprs);
  int check_exprs_sequence(const common::ObIArray<ObRawExpr*>& condition_exprs);
};

}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SQL_RESOLVER_DML_OB_MULTI_TABLE_INSERT_RESOLVER_H_ */