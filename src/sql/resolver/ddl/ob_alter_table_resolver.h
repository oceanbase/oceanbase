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

#ifndef OCEANBASE_SQL_OB_ALTER_TABLE_RESOLVER_
#define OCEANBASE_SQL_OB_ALTER_TABLE_RESOLVER_

#include "sql/resolver/ddl/ob_ddl_resolver.h"
#include "sql/resolver/ddl/ob_alter_table_stmt.h"

namespace oceanbase {
namespace sql {

typedef common::hash::ObPlacementHashSet<share::schema::ObColumnNameHashWrapper, common::OB_MAX_INDEX_PER_TABLE>
    ObReducedVisibleColSet;

class ObAlterTableResolver : public ObDDLResolver {
  static const int64_t ALTER_TABLE_NODE_COUNT = 2;
  static const int64_t TABLE = 0;        // 0. table_node
  static const int64_t ACTION_LIST = 1;  // 1. alter table action list
public:
  explicit ObAlterTableResolver(ObResolverParams& params);
  virtual ~ObAlterTableResolver();
  virtual int resolve(const ParseNode& parse_tree);
  int resolve_action_list(const ParseNode& node);
  int resolve_column_options(
      const ParseNode& node, bool& is_modify_column_visibility, ObReducedVisibleColSet& reduced_visible_col_set);
  int resolve_index_options_oracle(const ParseNode& node);
  int resolve_index_options(const ParseNode& node);
  int resolve_partition_options(const ParseNode& node);
  int resolve_constraint_options(const ParseNode& node);
  int resolve_modify_foreign_key_state(const ParseNode* node);
  int resolve_modify_check_constraint_state(const ParseNode* node);
  int resolve_tablegroup_options(const ParseNode& node);
  int resolve_foreign_key_options(const ParseNode& node);
  int resolve_add_column(const ParseNode& node);
  int resolve_alter_column(const ParseNode& node);
  int resolve_change_column(const ParseNode& node);
  int resolve_modify_column(
      const ParseNode& node, bool& is_modify_column_visibility, ObReducedVisibleColSet& reduced_visible_col_set);
  int resolve_drop_column(const ParseNode& node, ObReducedVisibleColSet& reduced_visible_col_set);
  int resolve_rename_column(const ParseNode& node);
  int fill_table_option(const share::schema::ObTableSchema* table_schema);
  // save table option to AlterTableArg
  int set_table_options();
  ObAlterTableStmt* get_alter_table_stmt()
  {
    return static_cast<ObAlterTableStmt*>(stmt_);
  };
  int resolve_column_index(const common::ObString& column_name);

private:
  int check_dup_foreign_keys_exist(
      share::schema::ObSchemaGetterGuard* schema_guard, const obrpc::ObCreateForeignKeyArg& foreign_key_arg);
  int resolve_alter_table_option_list(const ParseNode& node);
  int set_column_collation(share::schema::AlterColumnSchema& alter_column_schema);
  int resolve_index_column_list(const ParseNode& node, obrpc::ObCreateIndexArg& index_arg,
      const share::schema::ObTableSchema& tbl_schema, const bool is_fulltext_index, ParseNode* table_option_node,
      ObIArray<ObString>& input_index_columns_name);

  int add_sort_column(const obrpc::ObColumnSortItem& sort_column, obrpc::ObCreateIndexArg& index_arg);

  int process_timestamp_column(ObColumnResolveStat& stat, share::schema::AlterColumnSchema& alter_column_schema);
  int resolve_add_index(const ParseNode& node);
  int resolve_drop_index(const ParseNode& node);
  int resolve_drop_foreign_key(const ParseNode& node);
  int resolve_alter_index(const ParseNode& node);
  int resolve_rename_index(const ParseNode& node);
  int resolve_alter_index_parallel_oracle(const ParseNode& node);
  int resolve_alter_index_parallel_mysql(const ParseNode& node);
  int resolve_add_primary(const ParseNode& node);
  int generate_index_arg(obrpc::ObCreateIndexArg& index_arg, const bool is_unique_key);
  int resolve_alter_table_column_definition(share::schema::AlterColumnSchema& column, ParseNode* node,
      ObColumnResolveStat& stat, bool& is_modify_column_visibility, const bool is_add_column = false,
      const bool is_modify_column = false, const bool is_oracle_temp_table = false);
  int resolve_add_partition(const ParseNode& node, const share::schema::ObTableSchema& orig_table_schema);
  int inner_add_partition(ParseNode* part_elements_node, const share::schema::ObPartitionFuncType part_type,
      const share::schema::ObPartitionOption& part_option, ObTableStmt* alter_stmt,
      share::schema::ObTableSchema& alter_table_schema);
  int resolve_add_subpartition(const ParseNode& node, const share::schema::ObTableSchema& orig_table_schema);
  int resolve_drop_partition(const ParseNode& node, const share::schema::ObTableSchema& orig_table_schema);
  int resolve_drop_subpartition(const ParseNode& node, const share::schema::ObTableSchema& orig_table_schema);
  int check_subpart_name(
      const share::schema::ObPartition& partition, const share::schema::ObSubPartition& subpartition);
  int resolve_add_constraint(const ParseNode& node);
  int resolve_drop_constraint(const ParseNode& node);
  /**
   * mock (sub)part func expr node for table schema
   * @param is_sub_part: mock sub part func expr node or part func expr node
   */
  int mock_part_func_node(
      const share::schema::ObTableSchema& table_schema, const bool is_sub_part, ParseNode*& part_expr_node);
  int resolve_pos_column(const ParseNode* node, share::schema::AlterColumnSchema& alter_column_schema);
  int fill_column_schema_according_stat(
      const ObColumnResolveStat& stat, share::schema::AlterColumnSchema& alter_column_schema);
  int resolve_partitioned_partition(const ParseNode* node, const share::schema::ObTableSchema& origin_table_schema);
  int resolve_reorganize_partition(const ParseNode* node, const share::schema::ObTableSchema& origin_table_schema);
  int resolve_split_partition(const ParseNode* node, const share::schema::ObTableSchema& origin_table_schema);
  virtual int get_table_schema_for_check(share::schema::ObTableSchema& table_schema) override;
  // int generate_new_schema(const share::schema::ObTableSchema &origin_table_schema,
  //                        share::schema::AlterTableSchema &new_table_schema);
  int check_column_definition_node(const ParseNode* node);

  const share::schema::ObTableSchema* table_schema_;
  const share::schema::ObTableSchema* index_schema_;

  IndexNameSet current_index_name_set_;
  int64_t add_or_modify_check_cst_times_;
  int64_t modify_constraint_times_;
  DISALLOW_COPY_AND_ASSIGN(ObAlterTableResolver);
};

}  // end namespace sql
}  // end namespace oceanbase

#endif  // OCEANBASE_SQL_OB_ALTER_TABLE_RESOLVER_
