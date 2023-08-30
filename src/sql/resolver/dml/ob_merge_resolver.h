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

#ifndef OCEANBASE_SQL_RESOLVER_DML_OB_MERGE_RESOLVER_H_
#define OCEANBASE_SQL_RESOLVER_DML_OB_MERGE_RESOLVER_H_
#include "sql/resolver/dml/ob_del_upd_resolver.h"
#include "sql/resolver/dml/ob_merge_stmt.h"
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
class ObMergeResolver : public ObDelUpdResolver
{
public:
  enum MergeNodeField
  {
    TARGET_NODE = 0,
    SOURCE_NODE,
    MATCH_COND_NODE,
    UPDATE_CLAUSE_NODE,
    INSERT_CLAUSE_NODE,
    HINT_NODE,
    MERGE_FILED_COUNT
  };
  enum MergeResolveClause
  {
    NONE_CLAUSE,
    MATCH_CLAUSE,
    INSERT_COLUMN_CLAUSE,
    INSERT_VALUE_CLAUSE,
    INSERT_WHEN_CLAUSE
  };
public:
  explicit ObMergeResolver(ObResolverParams &params);
  virtual ~ObMergeResolver();
  virtual int resolve(const ParseNode &parse_tree);
  ObMergeStmt *get_merge_stmt() { return static_cast<ObMergeStmt*>(stmt_);}
  int resolve_merge_generated_table_columns(ObDMLStmt *stmt,
                                            TableItem *table,
                                            ObIArray<ColumnItem> &column_items);
protected:
  virtual int add_assignment(common::ObIArray<ObTableAssignment> &table_assigns,
                             const TableItem *table_item,
                             const ColumnItem *col_item,
                             ObAssignment &assign) override;
  virtual int resolve_table(const ParseNode &parse_tree, TableItem *&table_item) override;
  virtual int resolve_generate_table(const ParseNode &table_node,
                                     const ParseNode *alias_node,
                                     TableItem *&table_item) override;
  virtual int mock_values_column_ref(const ObColumnRefRawExpr *column_ref) override;
  virtual int find_value_desc(ObInsertTableInfo &table_info, uint64_t column_id, ObRawExpr *&column_ref) override;
private:
  int resolve_target_relation(const ParseNode *target_node);
  int resolve_source_relation(const ParseNode *source_node);
  int resolve_match_condition(const ParseNode *condition_node);
  int resolve_insert_clause(const ParseNode *insert_node);
  int resolve_update_clause(const ParseNode *update_node);
  int resolve_where_conditon(const ParseNode *condition_node, common::ObIArray<ObRawExpr*> &condition_exprs);
  int check_insert_clause();
  int check_column_validity(ObColumnRefRawExpr *col);

  virtual int resolve_column_ref_expr(const ObQualifiedName &q_name, ObRawExpr *&real_ref_expr) override;
  virtual int resolve_column_ref_for_subquery(const ObQualifiedName &q_name, ObRawExpr *&real_ref_expr) override;
  int check_stmt_validity();
  int get_equal_columns(ObIArray<ObString> &equal_cols);
  int get_related_table_id();
  int check_target_generated_table(const ObSelectStmt &ref_stmt);
  int resolve_merge_constraint();
private:
  ObSEArray<ObRawExpr *, 4, common::ModulePageAllocator, true> join_column_exprs_;
  MergeResolveClause resolve_clause_;
  int64_t insert_select_items_count_;
};
}//sql
}//oceanbase
#endif /* OCEANBASE_SQL_RESOLVER_DML_OB_MERGE_RESOLVER_H_ */
