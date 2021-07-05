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

#ifndef OCEANBASE_SQL_RESOLVER_DML_OB_DML_RESOLVER_H_
#define OCEANBASE_SQL_RESOLVER_DML_OB_DML_RESOLVER_H_
#include "sql/resolver/dml/ob_dml_stmt.h"
#include "sql/resolver/dml/ob_column_namespace_checker.h"
#include "sql/resolver/dml/ob_sequence_namespace_checker.h"
#include "sql/resolver/ob_stmt_resolver.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/resolver/dml/ob_select_stmt.h"

namespace oceanbase {
namespace sql {
//#define PARSE_SELECT_WITH       0                    /* with */
//#define PARSE_SELECT_DISTINCT   1                /* distinct */
//#define PARSE_SELECT_SELECT     2           /* select clause */
//#define PARSE_SELECT_FROM       3             /* from clause */
//#define PARSE_SELECT_WHERE      4                   /* where */
//#define PARSE_SELECT_GROUP      5                /* group by */
//#define PARSE_SELECT_HAVING     6                  /* having */
//#define PARSE_SELECT_SET        7           /* set operation */
//#define PARSE_SELECT_ALL        8          /* all specified? */
//#define PARSE_SELECT_FORMER     9      /* former select stmt */
//#define PARSE_SELECT_LATER     10       /* later select stmt */
//#define PARSE_SELECT_ORDER     11                /* order by */
//#define PARSE_SELECT_LIMIT     12                   /* limit */
//#define PARSE_SELECT_FOR_UPD   13              /* for update */
//#define PARSE_SELECT_HINTS     14                   /* hints */
//#define PARSE_SELECT_WHEN      15             /* when clause */
//#define PARSE_SELECT_INTO      16             /* select into */
//#define PARSE_SELECT_START_WITH 17             /* start with */
//#define PARSE_SELECT_CONNECT_BY 18             /* connect by */

class ObEqualAnalysis;
class ObChildStmtResolver;
struct IndexDMLInfo;
class ObDelUpdStmt;

class ObDMLResolver : public ObStmtResolver {
public:
  explicit ObDMLResolver(ObResolverParams& params);
  virtual ~ObDMLResolver();
  friend class ObDefaultValueUtils;
  friend class ObTransformMaterializedView;

  inline void set_current_level(int32_t level)
  {
    current_level_ = level;
  }
  inline int32_t get_current_level() const
  {
    return current_level_;
  }
  inline ObStmtScope get_current_scope() const
  {
    return current_scope_;
  }
  inline void set_parent_namespace_resolver(ObDMLResolver* parent_namespace_resolver)
  {
    parent_namespace_resolver_ = parent_namespace_resolver;
  }
  inline ObDMLResolver* get_parent_namespace_resolver()
  {
    return parent_namespace_resolver_;
  }
  virtual int resolve_column_ref_for_subquery(const ObQualifiedName& q_name, ObRawExpr*& real_ref_expr);
  int resolve_generated_column_expr(const common::ObString& expr_str, const TableItem& table_item,
      const share::schema::ObColumnSchemaV2* column_schema, const ObColumnRefRawExpr& column, ObRawExpr*& ref_expr,
      const bool used_for_generated_column = true, ObDMLStmt* stmt = NULL);
  virtual int resolve_generate_table(const ParseNode& table_node, const ObString& alias_name, TableItem*& tbl_item);
  int do_resolve_generate_table(const ParseNode& table_node, const ObString& alias_name,
      ObChildStmtResolver& child_resolver, TableItem*& table_item);
  int resolve_generate_table_item(ObSelectStmt* ref_query, const ObString& alias_name, TableItem*& tbl_item);
  int resolve_joined_table(const ParseNode& parse_node, JoinedTable*& joined_table);
  int resolve_joined_table_item(const ParseNode& parse_node, JoinedTable*& joined_table);
  int resolve_function_table_item(const ParseNode& table_node, TableItem*& table_item);

  int fill_same_column_to_using(JoinedTable*& joined_table);
  int get_columns_from_table_item(const TableItem* table_item, common::ObIArray<common::ObString>& column_names);

  int resolve_using_columns(const ParseNode& using_node, common::ObIArray<common::ObString>& column_names);
  int transfer_using_to_on_expr(JoinedTable*& joined_table);
  int resolve_table_column_expr(const ObQualifiedName& q_name, ObRawExpr*& real_ref_expr);
  int resolve_join_table_column_item(
      const JoinedTable& joined_table, const common::ObString& column_name, ObRawExpr*& real_ref_expr);
  int resolve_single_table_column_item(
      const TableItem& table_item, const common::ObString& column_name, bool include_hidden, ColumnItem*& col_item);
  int transfer_to_inner_joined(const ParseNode& parse_node, JoinedTable*& joined_table);
  virtual int check_special_join_table(const TableItem& join_table, bool is_left_child, ObItemType join_type);
  virtual int init_stmt()
  {
    int ret = common::OB_SUCCESS;
    ObDMLStmt* stmt = get_stmt();
    if (OB_ISNULL(stmt)) {
      ret = common::OB_NOT_INIT;
      SQL_RESV_LOG(ERROR, "stmt_ is null");
    } else if (OB_FAIL(stmt->set_table_bit_index(common::OB_INVALID_ID))) {
      SQL_RESV_LOG(ERROR, "add invalid id to table index desc failed", K(ret));
    } else {
      stmt->set_current_level(current_level_);
      if (parent_namespace_resolver_ != NULL) {
        stmt->set_parent_namespace_stmt(parent_namespace_resolver_->get_stmt());
      }
    }
    return ret;
  }
  bool need_all_columns(const share::schema::ObTableSchema& table_schema, int64_t binlog_row_image);

  int alloc_joined_table_item(JoinedTable*& joined_table);

  int create_joined_table_item(const ObJoinType joined_type, const TableItem* left_table, const TableItem* right_table,
      JoinedTable*& joined_table);

  // materialized view related
  int make_materalized_view_schema(ObSelectStmt& select_stmt, share::schema::ObTableSchema& view_schema);

  int make_join_types_for_materalized_view(ObSelectStmt& select_stmt, share::schema::ObTableSchema& view_schema);

  int convert_join_types_for_schema(JoinedTable& join_table, share::schema::ObTableSchema& view_schema);

  int make_join_conds_for_materialized_view(ObSelectStmt& select_stmt, share::schema::ObTableSchema& view_schema);

  int convert_cond_exprs_for_schema(ObRawExpr& relation_exprs, share::schema::ObTableSchema& view_schema);
  int resolve_table_partition_expr(const TableItem& table_item, const share::schema::ObTableSchema& table_schema);
  virtual int resolve_column_ref_expr(const ObQualifiedName& q_name, ObRawExpr*& real_ref_expr);
  int resolve_sql_expr(const ParseNode& node, ObRawExpr*& expr, ObArray<ObQualifiedName>* input_columns = NULL);

protected:
  ObDMLStmt* get_stmt();
  int resolve_into_variables(const ParseNode* node, ObIArray<ObString>& user_vars, ObIArray<ObRawExpr*>& pl_vars);
  int resolve_sequence_object(const ObQualifiedName& q_name, ObRawExpr*& expr);
  int resolve_sys_vars(common::ObArray<ObVarInfo>& sys_vars);
  int check_expr_param(const ObRawExpr& expr);
  int resolve_columns_field_list_first(ObRawExpr*& expr, ObArray<ObQualifiedName>& columns, ObSelectStmt* sel_stmt);
  int resolve_columns(ObRawExpr*& expr, common::ObArray<ObQualifiedName>& columns);
  int resolve_qualified_identifier(ObQualifiedName& q_name, ObIArray<ObQualifiedName>& columns,
      ObIArray<ObRawExpr*>& real_exprs, ObRawExpr*& real_ref_expr);
  int resolve_basic_column_ref(const ObQualifiedName& q_name, ObRawExpr*& real_ref_expr);
  int resolve_basic_column_item(const TableItem& table_item, const common::ObString& column_name, bool include_hidden,
      ColumnItem*& col_item, ObDMLStmt* stmt = NULL);
  virtual int mock_values_column_ref(const ObColumnRefRawExpr* column_ref)
  {
    UNUSED(column_ref);
    return common::OB_SUCCESS;
  }
  virtual int resolve_table(const ParseNode& parse_tree, TableItem*& table_item);
  virtual int resolve_basic_table(const ParseNode& parse_tree, TableItem*& table_item);
  int resolve_table_drop_oracle_temp_table(TableItem*& table_item);
  int resolve_base_or_alias_table_item_normal(uint64_t tenant_id, const common::ObString& db_name,
      const bool& is_db_explicit, const common::ObString& tbl_name, const common::ObString& alias_name,
      const common::ObString& synonym_name, const common::ObString& synonym_db_name, TableItem*& tbl_item,
      bool cte_table_fisrt);
  int resolve_base_or_alias_table_item_dblink(uint64_t dblink_id, const common::ObString& dblink_name,
      const common::ObString& database_name, const common::ObString& table_name, const common::ObString& alias_name,
      TableItem*& table_item);
  int resolve_all_basic_table_columns(
      const TableItem& table_item, bool included_hidden, common::ObIArray<ColumnItem>* column_items);
  int resolve_all_generated_table_columns(const TableItem& table_item, common::ObIArray<ColumnItem>& column_items);
  int resolve_columns_for_partition_expr(
      ObRawExpr*& expr, ObIArray<ObQualifiedName>& columns, const TableItem& table_item, bool is_hidden);
  int resolve_partition_expr(const TableItem& table_item, const share::schema::ObTableSchema& table_schema,
      const share::schema::ObPartitionFuncType part_type, const common::ObString& part_str, ObRawExpr*& expr);
  int resolve_and_split_sql_expr(const ParseNode& node, common::ObIArray<ObRawExpr*>& and_exprs);
  int resolve_and_split_sql_expr_with_bool_expr(const ParseNode& node, ObIArray<ObRawExpr*>& and_exprs);
  int resolve_where_clause(const ParseNode* node);
  int resolve_order_clause(const ParseNode* node);
  int resolve_limit_clause(const ParseNode* node);
  int resolve_into_clause(const ParseNode* node);
  int resolve_hints(const ParseNode* node);
  virtual int process_values_function(ObRawExpr*& expr);
  virtual int recursive_values_expr(ObRawExpr*& expr);
  virtual int resolve_order_item(const ParseNode& sort_node, OrderItem& order_item);

  int add_column_to_stmt(const TableItem& table_item, const share::schema::ObColumnSchemaV2& col,
      common::ObIArray<ObColumnRefRawExpr*>& column_ids, ObDMLStmt* stmt = NULL);

  int add_all_columns_to_stmt(const TableItem& table_item, common::ObIArray<ObColumnRefRawExpr*>& column_ids);

  int add_all_index_rowkey_to_stmt(uint64_t table_id, const share::schema::ObTableSchema* index_schema,
      common::ObIArray<ObColumnRefRawExpr*>* column_ids = NULL, const bool is_push_array = false);
  int add_all_rowkey_columns_to_stmt(
      const TableItem& table_item, common::ObIArray<ObColumnRefRawExpr*>& column_ids, ObDMLStmt* stmt = NULL);

  int add_index_related_columns_to_stmt(
      const TableItem& table_item, const uint64_t column_id, common::ObIArray<ObColumnRefRawExpr*>& column_items);
  int add_all_index_rowkey_to_stmt(const TableItem& table_item, const share::schema::ObTableSchema* index_schema,
      common::ObIArray<ObColumnRefRawExpr*>& column_ids);
  // check the update view is key preserved
  int uv_check_key_preserved(const TableItem& table_item, bool& key_preserved);
  int build_nvl_expr(const share::schema::ObColumnSchemaV2* column_schema, ObRawExpr*& expr);
  int build_nvl_expr(const ColumnItem* column_schema, ObRawExpr*& expr);
  /*
   * Resolve some 'special' expressions, which include:
   * 1. aggregate functions
   * 2. 'in' function
   * 3. operator expr(recursively on its children)
   */
  virtual int resolve_special_expr(ObRawExpr*& expr, ObStmtScope scope);
  int build_autoinc_nextval_expr(ObRawExpr*& expr, uint64_t autoinc_col_id);
  int build_seq_nextval_expr(ObRawExpr*& expr, const ObQualifiedName& q_name, uint64_t seq_id);
  int build_partid_expr(ObRawExpr*& expr, const uint64_t table_id);
  virtual int resolve_subquery_info(const common::ObIArray<ObSubQueryInfo>& subquery_info);
  virtual int resolve_aggr_exprs(
      ObRawExpr*& expr, common::ObIArray<ObAggFunRawExpr*>& aggr_exprs, const bool need_analyze = true);
  virtual int resolve_win_func_exprs(ObRawExpr*& expr, common::ObIArray<ObWinFunRawExpr*>& win_exprs);
  int do_resolve_subquery_info(const ObSubQueryInfo& subquery_info, ObChildStmtResolver& child_resolver);
  int add_leading_table_ids(const ParseNode* hint_node, common::ObIArray<uint64_t>& table_ids);
  int add_table_hint(const ParseNode* hint_node, common::ObIArray<uint64_t>& table_ids);
  int add_ordered_hint(common::ObIArray<int64_t>& index_array);
  int resolve_index_hint(const uint64_t table_id, const ParseNode* index_hint_node);
  int resolve_partitions(
      const ParseNode* part_node, const uint64_t table_id, const share::schema::ObTableSchema& table_schema);
  int resolve_sample_clause(const ParseNode* part_node, const uint64_t table_id);

  int check_pivot_aggr_expr(ObRawExpr* expr) const;
  int resolve_transpose_table(const ParseNode* transpose_node, TableItem*& table_item);
  int resolve_transpose_clause(
      const ParseNode& transpose_node, TransposeItem& transpose_item, ObIArray<ObString>& columns_in_aggr);
  int resolve_transpose_columns(const ParseNode& transpose_node, ObIArray<ObString>& columns);
  int resolve_const_exprs(const ParseNode& transpose_node, ObIArray<ObRawExpr*>& const_exprs);
  int get_transpose_target_sql(const ObIArray<ObString>& columns_in_aggrs, TableItem& table_item,
      TransposeItem& transpose_item, ObSqlString& target_sql);
  int get_target_sql_for_pivot(const ObIArray<ColumnItem>& column_items, TableItem& table_item,
      TransposeItem& transpose_item, ObSqlString& target_sql);
  int get_target_sql_for_unpivot(const ObIArray<ColumnItem>& column_items, TableItem& table_item,
      TransposeItem& transpose_item, ObSqlString& target_sql);
  int format_from_subquery(
      const ObString& unpivot_alias_name, TableItem& table_item, char* expr_str_buf, ObSqlString& target_sql);
  int expand_transpose(const ObSqlString& transpose_def, TransposeItem& transpose_item, TableItem*& table_item);
  int mark_unpivot_table(TransposeItem& transpose_item, TableItem* table_item);
  int remove_orig_table_item(TableItem& table_item);
  //  static int print_unpivot_table(char *buf, const int64_t size, int64_t &pos,
  //                                 const TableItem &table_item);

  int parse_partition(const common::ObString& part_name, const share::schema::ObPartitionLevel part_level,
      const int64_t part_num, const int64_t subpart_num, int64_t& partition_id);
  int check_basic_column_generated(const ObColumnRefRawExpr* col_expr, ObDMLStmt* dml_stmt, bool& is_generated);
  // used by both UpdateResolver and InsertResolver
  int resolve_assignments(const ParseNode& parse_node, ObTablesAssignments& assigns, ObStmtScope scope);

  int resolve_additional_assignments(ObTablesAssignments& assigns, const ObStmtScope scope);
  // used for partition by key(), which use to build the sql expression info
  int build_partition_key_info(const share::schema::ObTableSchema& table_schema, ObRawExpr*& part_expr);
  virtual int expand_view(TableItem& view_item);
  int do_expand_view(TableItem& view_item, ObChildStmtResolver& view_resolver);
  int build_padding_expr(const ObSQLSessionInfo* session, const ColumnItem* column, ObRawExpr*& expr);

  int build_padding_expr(
      const ObSQLSessionInfo* session, const share::schema::ObColumnSchemaV2* column_schema, ObRawExpr*& expr);
  int add_rowkey_ids(const int64_t table_id, common::ObIArray<uint64_t>& array);
  int get_part_key_ids(const int64_t table_id, common::ObIArray<uint64_t>& array);

  virtual int check_need_use_sys_tenant(bool& use_sys_tenant) const;
  // check in sys view or show statement
  virtual int check_in_sysview(bool& in_sysview) const;
  int resolve_table_relation_factor_wrapper(const ParseNode* table_node, uint64_t& dblink_id, uint64_t& database_id,
      common::ObString& table_name, common::ObString& synonym_name, common::ObString& db_name,
      common::ObString& synonym_db_name, common::ObString& dblink_name, bool& is_db_explicit, bool& use_sys_tenant);
  int check_resolve_oracle_sys_view(const ParseNode* node, bool& is_oracle_sys_view);
  bool is_oracle_sys_view(const ObString& table_name);
  int inner_resolve_sys_view(const ParseNode* table_node, uint64_t& database_id, ObString& tbl_name, ObString& db_name,
      bool& is_db_explicit, bool& use_sys_tenant);
  int inner_resolve_sys_view(
      const ParseNode* table_node, uint64_t& database_id, ObString& tbl_name, ObString& db_name, bool& use_sys_tenant);
  int resolve_table_relation_factor(const ParseNode* node, uint64_t tenant_id, uint64_t& dblink_id,
      uint64_t& database_id, common::ObString& table_name, common::ObString& synonym_name,
      common::ObString& synonym_db_name, common::ObString& db_name, common::ObString& dblink_name,
      bool& is_db_explicit);
  int resolve_table_relation_factor(const ParseNode* node, uint64_t& dblink_id, uint64_t& database_id,
      common::ObString& table_name, common::ObString& synonym_name, common::ObString& synonym_db_name,
      common::ObString& db_name, common::ObString& dblink_name, bool& is_db_explicit);
  int resolve_table_relation_factor(const ParseNode* node, uint64_t tenant_id, uint64_t& dblink_id,
      uint64_t& database_id, common::ObString& table_name, common::ObString& synonym_name,
      common::ObString& synonym_db_name, common::ObString& db_name, common::ObString& dblink_name);
  int resolve_table_relation_factor(const ParseNode* node, uint64_t& dblink_id, uint64_t& database_id,
      common::ObString& table_name, common::ObString& synonym_name, common::ObString& synonym_db_name,
      common::ObString& db_name, common::ObString& dblink_name);
  int resolve_table_relation_factor(const ParseNode* node, uint64_t tenant_id, uint64_t& dblink_id,
      uint64_t& database_id, common::ObString& table_name, common::ObString& synonym_name,
      common::ObString& synonym_db_name, common::ObString& db_name, common::ObString& dblink_name,
      ObSynonymChecker& synonym_checker);
  int resolve_table_relation_factor(const ParseNode* node, uint64_t tenant_id, uint64_t& dblink_id,
      uint64_t& database_id, common::ObString& table_name, common::ObString& synonym_name,
      common::ObString& synonym_db_name, common::ObString& db_name, common::ObString& dblink_name,
      bool& is_db_expilicit, ObSynonymChecker& synonym_checker);
  int resolve_table_relation_factor_normal(const ParseNode* node, uint64_t tenant_id, uint64_t& database_id,
      common::ObString& table_name, common::ObString& synonym_name, common::ObString& synonym_db_name,
      common::ObString& db_name, ObSynonymChecker& synonym_checker);
  int resolve_table_relation_factor_normal(const ParseNode* node, uint64_t tenant_id, uint64_t& database_id,
      common::ObString& table_name, common::ObString& synonym_name, common::ObString& synonym_db_name,
      common::ObString& db_name, bool& is_db_expilicit, ObSynonymChecker& synonym_checker);
  int resolve_table_relation_factor_dblink(const ParseNode* table_node, const uint64_t tenant_id,
      const common::ObString& dblink_name, uint64_t& dblink_id, common::ObString& table_name,
      common::ObString& database_name);
  int add_synonym_obj_id(const ObSynonymChecker& synonym_checker, bool error_with_exist);

  /*
   *
   */
  int resolve_is_expr(ObRawExpr*& expr);
  virtual int add_assignment(
      ObTablesAssignments& assigns, const TableItem* table_item, const ColumnItem* col_item, ObAssignment& assign);

  virtual int replace_column_to_default(ObRawExpr*& origin);
  int check_whether_assigned(ObTablesAssignments& assigns, uint64_t table_id, uint64_t base_column_id, bool& exist);
  int check_whether_assigned(const ObAssignments& assigns, uint64_t table_id, uint64_t base_column_id, bool& exist);
  int check_need_assignment(const ObAssignments& assigns, uint64_t table_id,
      const share::schema::ObColumnSchemaV2& column, bool& need_assign);
  int create_force_index_hint(const uint64_t table_id, const common::ObString& index_name);
  int resolve_autoincrement_column_is_null(ObRawExpr*& expr);
  int session_variable_opt_influence();
  int resolve_partition_expr(
      const ParseNode& part_expr_node, ObRawExpr*& expr, common::ObIArray<ObQualifiedName>& columns);

  void report_user_error_msg(int& ret, const ObRawExpr* root_expr, const ObQualifiedName& q_name) const;
  ///////////functions for sql hint/////////////
  int resolve_parallel_in_hint(const ParseNode* parallel_node, int64_t& parallel);
  int resolve_tracing_hint(const ParseNode* tracing_node, common::ObIArray<uint64_t>& tracing_ids);
  int parse_tables_in_hint(const ParseNode* qb_name_node, const ParseNode* hint_table,
      common::ObIArray<ObTableInHint>& hint_table_arr,  // in stmt hint
      common::ObIArray<ObTablesInHint>& hint_tables_arr,
      common::ObIArray<std::pair<uint8_t, uint8_t>>& hint_pair_arr);  // in stmt hint
  int parse_trans_param_hint(
      const ParseNode* trans_param_name, const ParseNode* trans_param_value, ObStmtHint& stmt_hint);
  int resolve_tables_in_hint(const ParseNode* table_list, common::ObIArray<ObTableInHint>& hint_table_arr);
  int resolve_pq_distribute_node(const ParseNode* qb_name_node, const ParseNode* table_list,
      const ParseNode* distribute_method_left, const ParseNode* distribute_method_right,
      common::ObIArray<ObOrgPQDistributeHint>& pq_distribute,
      common::ObIArray<ObQBNamePQDistributeHint>& pq_distributes);
  int parse_qb_in_rewrite_hint(const ParseNode* hint_node, common::ObIArray<ObString>& qb_names,
      ObUseRewriteHint::Type& type, const ObUseRewriteHint::Type hint_type);

  // resolve query block name hint
  int resolve_qb_name_node(const ParseNode* qb_name_node, common::ObString& qb_name);

  int add_hint_table_list(const ParseNode* table_list, common::ObIArray<ObTableInHint>& hint_table_list,
      common::ObIArray<std::pair<uint8_t, uint8_t>>& hint_pair_arr, bool is_in_leading = true);
  int resolve_table_relation_in_hint(
      const ParseNode* hint_table, common::ObString& table_name, common::ObString& db_name, common::ObString& qb_name);
  //////////end of functions for sql hint/////////////
  bool is_need_add_additional_function(const ObRawExpr* expr);
  int add_additional_function_according_to_type(
      const ColumnItem* column, ObRawExpr*& expr, ObStmtScope scope, bool need_padding);
  int try_add_padding_expr_for_column_conv(const ColumnItem* column, ObRawExpr*& expr);
  int resolve_fun_match_against_expr(ObFunMatchAgainst& expr);
  int resolve_generated_column_expr_temp(const TableItem& table_item, const share::schema::ObTableSchema& table_schema);
  int find_generated_column_expr(ObRawExpr*& expr, bool& is_found);
  int deduce_generated_exprs(common::ObIArray<ObRawExpr*>& exprs);

  int build_prefix_index_compare_expr(ObRawExpr& column_expr, ObRawExpr* prefix_expr, ObItemType type,
      ObRawExpr& value_expr, ObRawExpr* escape_expr, ObRawExpr*& new_op_expr);
  // for materialized view
  int make_column_from_expr(ObRawExpr& expr, share::schema::ObColumnSchemaV2& col);

  int add_column_for_schema(share::schema::ObColumnSchemaV2& col, share::schema::ObTableSchema*& view_schema,
      const ObString* col_name, bool is_rowkey, bool is_depend_col);

  int resolve_function_table_column_item(const TableItem& table_item, ObIArray<ColumnItem>& col_items);
  int resolve_function_table_column_item(
      const TableItem& table_item, const common::ObString& column_name, ColumnItem*& col_item);
  int resolve_function_table_column_item(const TableItem& table_item, const ObDataType& data_type,
      const ObString& column_name, uint64_t column_id, ColumnItem*& col_item);
  int resolve_generated_table_column_item(const TableItem& table_item, const common::ObString& column_name,
      ColumnItem*& col_item, ObDMLStmt* stmt = NULL, const uint64_t column_id = OB_INVALID_ID);
  int set_base_table_for_updatable_view(
      TableItem& table_item, const ObColumnRefRawExpr& col_ref, const bool log_error = true);
  int set_base_table_for_view(TableItem& table_item, const bool log_error = true);
  int check_same_base_table(
      const TableItem& table_item, const ObColumnRefRawExpr& col_ref, const bool log_error = true);
  // for update view, add all columns to select item.
  int add_all_column_to_updatable_view(ObDMLStmt& stmt, const TableItem& table_item);
  //  check column is from the base table of updatable view
  bool in_updatable_view_path(const TableItem& table_item, const ObColumnRefRawExpr& col) const;
  // find column item by table_item (may be generated table) and base table's column id.
  ColumnItem* find_col_by_base_col_id(ObDMLStmt& stmt, const TableItem& table_item, const uint64_t base_column_id);
  ColumnItem* find_col_by_base_col_id(ObDMLStmt& stmt, const uint64_t table_id, const uint64_t base_column_id);
  int erase_redundant_generated_table_column_flag(
      const ObSelectStmt& ref_stmt, const ObRawExpr* ref_expr, ObColumnRefRawExpr& col_expr) const;
  int resolve_returning(const ParseNode* parse_tree);
  virtual int check_returning_validity();
  int check_returinng_expr(ObRawExpr* expr, bool& has_single_set_expr, bool& has_simple_expr, bool& has_sequenece);
  // for cte
  int add_cte_table_to_children(ObChildStmtResolver& child_resolver);
  void set_non_record(bool record)
  {
    with_clause_without_record_ = record;
  };
  int resolve_index_rowkey_exprs(uint64_t table_id, const share::schema::ObTableSchema& index_schema,
      common::ObIArray<ObColumnRefRawExpr*>& column_exprs, bool use_shared_spk = false);
  int resolve_index_all_column_exprs(uint64_t table_id, const share::schema::ObTableSchema& index_schema,
      common::ObIArray<ObColumnRefRawExpr*>& column_exprs);
  int resolve_index_related_column_exprs(uint64_t table_id, const share::schema::ObTableSchema& index_schema,
      const ObAssignments& assignments, common::ObIArray<ObColumnRefRawExpr*>& column_exprs);
  // for insert
  // Refer to the column_exprs -> column_convert_exprs mapping relationship of the main table,
  // The global index table fills column_convert_exprs in the order of its own column_exprs
  int fill_index_column_convert_exprs(bool use_static_engine, const IndexDMLInfo& primary_dml_info,
      const common::ObIArray<ObColumnRefRawExpr*>& column_exprs, common::ObIArray<ObRawExpr*>& column_convert_exprs);
  // for update
  int fill_index_column_convert_exprs(const ObAssignments& assignments, ObIArray<ObColumnRefRawExpr*>& column_exprs,
      ObIArray<ObRawExpr*>& column_convert_exprs);

  int resolve_shadow_pk_expr(uint64_t table_id, uint64_t column_id, const share::schema::ObTableSchema& index_schema,
      ObColumnRefRawExpr*& spk_expr);

  int check_oracle_outer_join_condition(const ObRawExpr* expr);
  int remove_outer_join_symbol(ObRawExpr*& expr);
  int resolve_outer_join_symbol(const ObStmtScope scope, ObRawExpr*& expr);
  int generate_outer_join_tables();

  int generate_outer_join_dependency(const common::ObIArray<TableItem*>& table_items,
      const common::ObIArray<ObRawExpr*>& exprs, common::ObIArray<common::ObBitSet<>>& table_dependencies);

  int extract_column_with_outer_join_symbol(
      const ObRawExpr* expr, common::ObIArray<uint64_t>& left_tables, common::ObIArray<uint64_t>& right_tables);

  int add_oracle_outer_join_dependency(const common::ObIArray<uint64_t>& all_tables,
      const common::ObIArray<uint64_t>& left_tables, uint64_t right_table_id,
      common::ObIArray<common::ObBitSet<>>& table_dependencies);

  int build_outer_join_table_by_dependency(
      const common::ObIArray<common::ObBitSet<>>& table_dependencies, ObDMLStmt& stmt);

  int deliver_outer_join_conditions(common::ObIArray<ObRawExpr*>& exprs, common::ObIArray<JoinedTable*>& joined_tables);

  int deliver_expr_to_outer_join_table(const ObRawExpr* expr, const common::ObIArray<uint64_t>& table_ids,
      JoinedTable* joined_table, bool& is_delivered);

  void set_has_ansi_join(bool has)
  {
    has_ansi_join_ = has;
  }
  bool has_ansi_join()
  {
    return has_ansi_join_;
  }

  void set_has_oracle_join(bool has)
  {
    has_oracle_join_ = has;
  }
  bool has_oracle_join()
  {
    return has_oracle_join_;
  }

  const TableItem* get_from_items_order(int64_t index) const
  {
    return from_items_order_.at(index);
  }
  TableItem* get_from_items_order(int64_t index)
  {
    return from_items_order_.at(index);
  }
  int add_from_items_order(TableItem* ti)
  {
    return from_items_order_.push_back(ti);
  }
  int view_pullup_column_ref_exprs_recursively(ObRawExpr*& expr, uint64_t table_id, const ObDMLStmt* stmt);
  int add_check_constraint_to_stmt(const share::schema::ObTableSchema* table_schema);
  int resolve_check_constraints(const TableItem* table_item);
  int view_pullup_generated_column_exprs();
  int view_pullup_part_exprs();

  int is_open_all_pdml_feature(ObDelUpdStmt* dml_stmt, bool& is_open) const;
  int check_unique_index(const ObIArray<ObColumnRefRawExpr*>& column_exprs, bool& has_unique_index) const;
  virtual const ObString get_view_db_name() const
  {
    return ObString();
  }
  virtual const ObString get_view_name() const
  {
    return ObString();
  }

private:
  int add_column_ref_to_set(ObRawExpr*& expr, ObIArray<TableItem*>* table_list);
  int check_table_exist_or_not(
      uint64_t tenant_id, uint64_t& database_id, common::ObString& table_name, common::ObString& db_name);
  int resolve_table_relation_recursively(uint64_t tenant_id, uint64_t& database_id, common::ObString& table_name,
      common::ObString& db_name, ObSynonymChecker& synonym_checker);
  int add_synonym_version(const common::ObIArray<uint64_t>& synonym_ids);

  int find_const_params_for_gen_column(const ObRawExpr& expr);
  int add_object_version_to_dependency(share::schema::ObDependencyTableType table_type,
      share::schema::ObSchemaType schema_type, const ObIArray<uint64_t>& object_ids);
  int add_sequence_id_to_stmt(uint64_t sequence_id, bool is_currval = false);
  int check_order_by_for_subquery_stmt(const ObSubQueryInfo& info);
  int check_stmt_order_by(const ObSelectStmt* stmt);

  int resolve_column_and_values(
      const ParseNode& assign_list, ObIArray<ObColumnRefRawExpr*>& target_list, ObIArray<ObRawExpr*>& value_list);

  int resolve_assign_columns(const ParseNode& assign_target, ObIArray<ObColumnRefRawExpr*>& column_list);

  int resolve_pseudo_column(const ObQualifiedName& q_name, ObRawExpr*& real_ref_expr);
  bool has_qb_name_in_hints(
      common::ObIArray<ObTablesInHint>& hint_tables_arr, ObTablesInHint*& tables_in_hint, const ObString& qb_name);
  int update_child_stmt_px_hint_for_dml();

  // Try add remove_const expr for const expr of select item.
  // Refer to comment of ObExprRemoveConst to see why we need this expr.
  int try_add_remove_const_epxr(ObSelectStmt& stmt);

  int construct_calc_rowid_expr(const share::schema::ObTableSchema* table_schema, const ObString& database_name,
      common::ObString& rowid_expr_str);
  int resolve_rowid_column_expr(const TableItem& table_item, ObDMLStmt* stmt, ObRawExpr*& ref_epxr);
  int recursive_replace_rowid_col(ObRawExpr*& raw_expr);

  int resolve_dblink_name(const ParseNode* table_node, ObString& dblink_name);

  int update_errno_if_sequence_object(const ObQualifiedName& q_name, int old_ret);
  int get_all_column_ref(ObRawExpr* expr, common::ObIArray<ObColumnRefRawExpr*>& arr);

  int process_part_str(ObIAllocator& calc_buf, const ObString& part_str, ObString& new_part_str);

  // a synonym may a dblink synonym, while failed to resolving a sysnonym to a table name,
  // wo should try to resolving to a dblink name.
  // in out param: table_name, in: may someting link "remote_schema.test@my_link", out: "test"
  // out param: dblink_name("my_link"), db_name("remote_schema"), dblink_id(id of my_link)
  int resolve_dblink_with_synonym(
      uint64_t tenant_id, ObString& table_name, ObString& dblink_name, ObString& db_name, uint64_t& dblink_id);

protected:
  typedef std::pair<TableItem, common::ObString> GenColumnNamePair;
  typedef std::pair<const ObRawExpr*, GenColumnNamePair> GenColumnExprPair;
  ObStmtScope current_scope_;
  int32_t current_level_;
  bool field_list_first_;
  ObDMLResolver* parent_namespace_resolver_;
  ObColumnNamespaceChecker column_namespace_checker_;
  ObSequenceNamespaceChecker sequence_namespace_checker_;
  // these generated column exprs are not the reference by query expression,
  // just some expr template in schema,
  // only the generated column expr referenced by query can be deposited to stmt
  common::ObArray<GenColumnExprPair> gen_col_exprs_;
  common::ObArray<TableItem*> from_items_order_;

  // for oracle style outer join
  bool has_ansi_join_;
  bool has_oracle_join_;

  /*
   * In the parsing of with clause, we cannot add the resolved table to
   * affect whether this plan needs to enter trans_service
   * Go to the globle_dependency_table.
   * E.g:
   * with cte (select * from t1) select 1 from dual;
   * If it is added, the above sentence will enter trans_service, and then an
   * error will be reported, because it actually detects the relevant table
   * The partition location is empty.
   * */
  bool with_clause_without_record_;

protected:
  DISALLOW_COPY_AND_ASSIGN(ObDMLResolver);
};

}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SQL_RESOLVER_DML_OB_DML_RESOLVER_H_ */
