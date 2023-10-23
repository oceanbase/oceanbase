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
#include "sql/resolver/expr/ob_raw_expr_resolver_impl.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/resolver/expr/ob_shared_expr_resolver.h"

namespace oceanbase
{
namespace sql
{
class ObEqualAnalysis;
class ObChildStmtResolver;
class ObDelUpdStmt;
class ObSelectResolver;
class ObInsertResolver;

static const char *err_log_default_columns_[] = { "ORA_ERR_NUMBER$", "ORA_ERR_MESG$", "ORA_ERR_ROWID$", "ORA_ERR_OPTYP$", "ORA_ERR_TAG$" };
static char *str_to_lower(char *pszBuf, int64_t length);
/*
 * ResolverJoinInfo is used to store temporary join information that would only be used in resolver.
 */
struct ResolverJoinInfo {
  uint64_t table_id_;
  // using columns in joined tables (moved from joined table)
  common::ObSEArray<common::ObString, 1, common::ModulePageAllocator, true> using_columns_;
  // coalesce_exprs in joined tables (moved from joined table)
  common::ObSEArray<ObRawExpr *, 1, common::ModulePageAllocator, true> coalesce_expr_;

  ResolverJoinInfo() :
    table_id_(common::OB_INVALID_ID),
    using_columns_(),
    coalesce_expr_()
  {}

  ResolverJoinInfo(int64_t table_id) :
    table_id_(table_id),
    using_columns_(),
    coalesce_expr_()
  {}

  int assign(const ResolverJoinInfo &other);

  TO_STRING_KV(K_(table_id),
               K_(using_columns),
               K_(coalesce_expr));
};

struct ObDmlJtColDef
{
  ObDmlJtColDef()
    : col_base_info_(),
      table_id_(common::OB_INVALID_ID),
      regular_cols_(),
      nested_cols_(),
      error_expr_(nullptr),
      empty_expr_(nullptr) {}

  ObJtColBaseInfo col_base_info_;
  int64_t table_id_;
  common::ObSEArray<ObDmlJtColDef*, 4, common::ModulePageAllocator, true> regular_cols_;
  common::ObSEArray<ObDmlJtColDef*, 4, common::ModulePageAllocator, true> nested_cols_;
  ObRawExpr* error_expr_;
  ObRawExpr* empty_expr_;
  TO_STRING_KV(K_(col_base_info));
};

class ObDMLResolver : public ObStmtResolver
{
  class ObCteResolverCtx
  {
    friend class ObSelectResolver;
    friend class ObDMLResolver;
  public:
    ObCteResolverCtx():
      left_select_stmt_(NULL),
      left_select_stmt_parse_node_(NULL),
      opt_col_alias_parse_node_(NULL),
      is_with_clause_resolver_(false),
      current_cte_table_name_(""),
      is_recursive_cte_(false),
      is_cte_subquery_(false),
      cte_resolve_level_(0),
      cte_branch_count_(0),
      is_set_left_resolver_(false),
      is_set_all_(true)
    {

    }
    virtual ~ObCteResolverCtx()
    {

    }
    inline void set_is_with_resolver(bool is_with_resolver) { is_with_clause_resolver_ = is_with_resolver; }
    inline void set_current_cte_table_name(const ObString& table_name) { current_cte_table_name_ = table_name; }
    inline bool is_with_resolver() const { return is_with_clause_resolver_; }
    inline void set_recursive(bool recursive) { is_recursive_cte_ = recursive; }
    inline void set_in_subquery() { is_cte_subquery_ = true; }
    inline bool is_in_subquery() { return cte_resolve_level_ >=2; }
    inline void reset_subquery_level() { cte_resolve_level_ = 0; }
    inline bool is_recursive() const { return is_recursive_cte_; }
    inline void set_left_select_stmt(ObSelectStmt* left_stmt) { left_select_stmt_ = left_stmt; }
    inline void set_left_parse_node(const ParseNode* node) { left_select_stmt_parse_node_ = node; }
    inline void set_set_all(bool all) { is_set_all_ = all; }
    inline bool invalid_recursive_union() { return  (nullptr != left_select_stmt_ && !is_set_all_); }
    inline bool more_than_two_branch() { return cte_branch_count_ >= 2; }
    inline void reset_branch_count() { cte_branch_count_ = 0; }
    inline void set_recursive_left_branch() { is_set_left_resolver_ = true; cte_branch_count_ ++; }
    inline void set_recursive_right_branch(ObSelectStmt* left_stmt, const ParseNode* node, bool all) {
      is_set_left_resolver_ = false;
      cte_branch_count_ ++;
      left_select_stmt_ = left_stmt;
      left_select_stmt_parse_node_ = node;
      is_set_all_ = all;
    }
    int assign(ObCteResolverCtx &cte_ctx) {
      left_select_stmt_ = cte_ctx.left_select_stmt_;
      left_select_stmt_parse_node_ = cte_ctx.left_select_stmt_parse_node_;
      opt_col_alias_parse_node_ = cte_ctx.opt_col_alias_parse_node_;
      is_with_clause_resolver_ = cte_ctx.is_with_clause_resolver_;
      current_cte_table_name_ = cte_ctx.current_cte_table_name_;
      is_recursive_cte_ = cte_ctx.is_recursive_cte_;
      is_cte_subquery_ = cte_ctx.is_cte_subquery_;
      cte_resolve_level_ = cte_ctx.cte_resolve_level_;
      cte_branch_count_ = cte_ctx.cte_branch_count_;
      is_set_left_resolver_ = cte_ctx.is_set_left_resolver_;
      is_set_all_ = cte_ctx.is_set_all_;
      return cte_col_names_.assign(cte_ctx.cte_col_names_);
    }
    TO_STRING_KV(K_(is_with_clause_resolver),
                 K_(current_cte_table_name),
                 K_(is_recursive_cte),
                 K_(is_cte_subquery),
                 K_(cte_resolve_level),
                 K_(cte_col_names));
  private:
    ObSelectStmt* left_select_stmt_;
    const ParseNode* left_select_stmt_parse_node_;
    const ParseNode* opt_col_alias_parse_node_;
    bool is_with_clause_resolver_;
    ObString current_cte_table_name_;
    bool is_recursive_cte_;
    bool is_cte_subquery_;
    int64_t cte_resolve_level_;
    int64_t cte_branch_count_;
    common::ObArray<ObString> cte_col_names_;
    bool is_set_left_resolver_;
    bool is_set_all_;
  };
public:
  explicit ObDMLResolver(ObResolverParams &params);
  virtual ~ObDMLResolver();
  friend class ObDefaultValueUtils;
  inline void set_current_level(int32_t level) { current_level_ = level; }
  inline int32_t get_current_level() const { return current_level_; }
  inline void set_current_view_level(int32_t level) { current_view_level_ = level; }
  inline int32_t get_current_view_level() const { return current_view_level_; }
  inline void set_view_ref_id(uint64_t ref_id) { view_ref_id_ = ref_id; }
  inline uint64_t get_view_ref_id() const { return view_ref_id_; }
  inline ObStmtScope get_current_scope() const { return current_scope_; }
  inline void set_parent_namespace_resolver(ObDMLResolver *parent_namespace_resolver)
  { parent_namespace_resolver_ = parent_namespace_resolver; }
  inline ObDMLResolver *get_parent_namespace_resolver() { return parent_namespace_resolver_; }
  virtual int resolve_column_ref_for_subquery(const ObQualifiedName &q_name, ObRawExpr *&real_ref_expr);
  int resolve_generated_column_expr(const common::ObString &expr_str,
                                    const TableItem &table_item,
                                    const share::schema::ObColumnSchemaV2 *column_schema,
                                    const ObColumnRefRawExpr &column,
                                    ObRawExpr *&ref_expr,
                                    const bool used_for_generated_column = true,
                                    ObDMLStmt *stmt = NULL);
  int do_resolve_generate_table(const ParseNode &table_node,
                                const ParseNode *alias_node,
                                ObChildStmtResolver &child_resolver,
                                TableItem *&table_item);
  int extract_var_init_exprs(ObSelectStmt *ref_query, common::ObIArray<ObRawExpr*> &assign_exprs);
  int resolve_generate_table_item(ObSelectStmt *ref_query, const ObString &alias_name, TableItem *&tbl_item);
  int resolve_joined_table(const ParseNode &parse_node, JoinedTable *&joined_table);
  int resolve_joined_table_item(const ParseNode &parse_node, JoinedTable *&joined_table);
  int resolve_function_table_item(const ParseNode &table_node,
                                  TableItem *&table_item);
  int resolve_json_table_item(const ParseNode &table_node,
                              TableItem *&table_item);
  int fill_same_column_to_using(JoinedTable* &joined_table);
  int get_columns_from_table_item(const TableItem *table_item, common::ObIArray<common::ObString> &column_names);

  int resolve_using_columns(const ParseNode &using_node, common::ObIArray<common::ObString> &column_names);
  int transfer_using_to_on_expr(JoinedTable *&joined_table);
  int resolve_table_column_expr(const ObQualifiedName &q_name, ObRawExpr *&real_ref_expr);
  int resolve_join_table_column_item(const JoinedTable &joined_table,
                                     const common::ObString &column_name,
                                     ObRawExpr *&real_ref_expr);
  int json_table_make_json_path(const ParseNode &parse_tree,
                                ObIAllocator* allocator,
                                ObString& path_str);
  int resolve_json_table_column_name_and_path(const ParseNode *name_node,
                                           const ParseNode *path_node,
                                           ObIAllocator* allocator,
                                           ObDmlJtColDef *col_def);
  int resolve_single_table_column_item(const TableItem &table_item,
                                       const common::ObString &column_name,
                                       bool include_hidden,
                                       ColumnItem *&col_item);
  // dot notation
  int pre_process_dot_notation(ParseNode &node);
  int print_json_path(ParseNode *&tmp_path, ObJsonBuffer &res_str);
  int check_depth_obj_access_ref(ParseNode *node, int8_t &depth, bool &exist_fun, ObJsonBuffer &sql_str, bool obj_check = true);  // obj_check : whether need check dot notaion
  int check_first_node_name(const ObString &node_name, bool &check_res);
  int transform_dot_notation2_json_query(ParseNode &node, const ObString &sql_str);
  int transform_dot_notation2_json_value(ParseNode &node, const ObString &sql_str);
  int check_column_json_type(ParseNode *tab_col, bool &is_json_col, int8_t only_is_json = 1);
  int check_size_obj_access_ref(ParseNode *node);
  /* json object resolve star */
  int get_target_column_list(ObSEArray<ColumnItem, 4> &target_list, ObString &tab_name, bool all_tab,
                            bool &tab_has_alias, TableItem *&tab_item, bool is_col = false);
  int add_column_expr_for_json_object_node(ParseNode *node,
                                common::ObIAllocator &allocator,
                                ObVector<ParseNode *> &t_vec,
                                ObString col_name,
                                ObString table_name);
  int expand_star_in_json_object(ParseNode *node, common::ObIAllocator &allocator, int64_t pos, int64_t& col_num);
  int check_is_json_constraint(common::ObIAllocator &allocator, ParseNode *col_node, bool& format_json, int8_t only_is_json = 0); // 1 is json & json type ; 0 is json; 2 json type
  bool is_array_json_expr(ParseNode *node);
  bool is_object_json_expr(ParseNode *node);
  int set_format_json_output(ParseNode *node);
  int pre_process_json_object_contain_star(ParseNode *node, common::ObIAllocator &allocator);
  int transfer_to_inner_joined(const ParseNode &parse_node, JoinedTable *&joined_table);
  virtual int check_special_join_table(const TableItem &join_table, bool is_left_child, ObItemType join_type);
  /**
   * 为一个 `JoinedTable` 分配内存
   * @param joined_table 新的`JoinedTable`
   */
  int alloc_joined_table_item(JoinedTable *&joined_table);

  /**
   * 创建一个 joined table item
   */
  int create_joined_table_item(const ObJoinType joined_type,
                               const TableItem *left_table,
                               const TableItem *right_table,
                               JoinedTable* &joined_table);

  int resolve_table_partition_expr(const TableItem &table_item, const share::schema::ObTableSchema &table_schema);
  int resolve_fk_table_partition_expr(const TableItem &table_item, const ObTableSchema &table_schema);

  int resolve_foreign_key_constraint(const TableItem *table_item);

  // map parent key column name to foreign key column name
  int map_to_fk_column_name(const ObTableSchema &child_table_schema,
                            const ObTableSchema &parent_table_schema,
                            const ObForeignKeyInfo &fk_info,
                            const ObString &pk_col_name,
                            ObString &fk_col_name);
  int resolve_columns_for_fk_partition_expr(ObRawExpr *&expr,
                                            ObIArray<ObQualifiedName> &columns,
                                            const TableItem &table_item, // table_item of dml table(child_table)
                                            const ObTableSchema &parent_table_schema,
                                            const ObForeignKeyInfo *fk_info);
  virtual int resolve_column_ref_expr(const ObQualifiedName &q_name, ObRawExpr *&real_ref_expr);
  int resolve_sql_expr(const ParseNode &node, ObRawExpr *&expr,
                       ObArray<ObQualifiedName> *input_columns = NULL);
  int resolve_partition_expr(const TableItem &table_item,
                             const share::schema::ObTableSchema &table_schema,
                             const share::schema::ObPartitionFuncType part_type,
                             const common::ObString &part_str,
                             ObRawExpr *&expr,
                             bool for_fk = false,
                             const ObForeignKeyInfo *fk_info = nullptr);
  static int resolve_special_expr_static(const ObTableSchema *table_schema,
                                         const ObSQLSessionInfo &session_info,
                                         ObRawExprFactory &expr_factory,
                                         ObRawExpr *&expr,
                                         bool& has_default,
                                         const ObResolverUtils::PureFunctionCheckStatus
                                               check_status);

  void set_query_ref_expr(ObQueryRefRawExpr *query_ref) { query_ref_ = query_ref; }
  ObQueryRefRawExpr *get_subquery() { return query_ref_; }
  int build_heap_table_hidden_pk_expr(ObRawExpr *&expr, const ObColumnRefRawExpr *ref_expr);
  static int copy_schema_expr(ObRawExprFactory &factory,
                              ObRawExpr *expr,
                              ObRawExpr *&new_expr);


  int add_sequence_id_to_stmt(uint64_t sequence_id, bool is_currval = false);
  int add_object_version_to_dependency(share::schema::ObDependencyTableType table_type,
                                       share::schema::ObSchemaType schema_type,
                                       uint64_t object_id,
                                       uint64_t database_id,
                                       uint64_t dep_obj_id);
  int add_object_versions_to_dependency(share::schema::ObDependencyTableType table_type,
                                       share::schema::ObSchemaType schema_type,
                                       const ObIArray<uint64_t> &object_ids,
                                       const ObIArray<uint64_t> &db_ids);
  ObDMLStmt *get_stmt();
  void set_upper_insert_resolver(ObInsertResolver *insert_resolver) {
    upper_insert_resolver_ = insert_resolver; }
protected:
  int generate_pl_data_type(ObRawExpr *expr, pl::ObPLDataType &pl_data_type);
  int resolve_into_variables(const ParseNode *node,
                             ObIArray<ObString> &user_vars,
                             ObIArray<ObRawExpr*> &pl_vars,
                             ObSelectStmt *select_stmt);
  int resolve_sys_vars(common::ObArray<ObVarInfo> &sys_vars);
  int check_expr_param(const ObRawExpr &expr);
  int check_col_param_on_expr(ObRawExpr *expr);
  int resolve_columns_field_list_first(ObRawExpr *&expr, ObArray<ObQualifiedName> &columns, ObSelectStmt* sel_stmt);
  int resolve_columns(ObRawExpr *&expr, common::ObArray<ObQualifiedName> &columns);
  int resolve_qualified_identifier(ObQualifiedName &q_name,
                                   ObIArray<ObQualifiedName> &columns,
                                   ObIArray<ObRawExpr*> &real_exprs,
                                   ObRawExpr *&real_ref_expr);
  int resolve_basic_column_ref(const ObQualifiedName &q_name, ObRawExpr *&real_ref_expr);
  int resolve_basic_column_item(const TableItem &table_item,
                                const common::ObString &column_name,
                                bool include_hidden,
                                ColumnItem *&col_item,
                                ObDMLStmt *stmt = NULL);
  int adjust_values_desc_position(ObInsertTableInfo& table_info,
                                  ObIArray<int64_t> &value_idxs);
public:
  virtual int resolve_table(const ParseNode &parse_tree, TableItem *&table_item);
protected:
  virtual int resolve_generate_table(const ParseNode &table_node,
                                     const ParseNode *alias_node,
                                     TableItem *&tbl_item);
  int check_stmt_has_flashback_query(ObDMLStmt *stmt, bool check_all, bool &has_fq);
  virtual int resolve_basic_table(const ParseNode &parse_tree, TableItem *&table_item);
  int resolve_flashback_query_node(const ParseNode *time_node, TableItem *table_item);
  int check_flashback_expr_validity(ObRawExpr *expr, bool &has_column);
  int set_flashback_info_for_view(ObSelectStmt *select_stmt, TableItem *table_item);
  int resolve_table_drop_oracle_temp_table(TableItem *&table_item);
  int resolve_base_or_alias_table_item_normal(uint64_t tenant_id,
                                              const common::ObString &db_name,
                                              const bool &is_db_explicit,
                                              const common::ObString &tbl_name,
                                              const common::ObString &alias_name,
                                              const common::ObString &synonym_name,
                                              const common::ObString &synonym_db_name,
                                              TableItem *&tbl_item,
                                              bool cte_table_fisrt,
                                              uint64_t real_dep_obj_id);
  int resolve_base_or_alias_table_item_dblink(uint64_t dblink_id,
                                              const common::ObString &dblink_name,
                                              const common::ObString &database_name,
                                              const common::ObString &table_name,
                                              const common::ObString &alias_name,
                                              const common::ObString &synonym_name,
                                              const common::ObString &synonym_db_name,
                                              TableItem *&table_item,
                                              bool is_reverse_link);
  int resolve_all_basic_table_columns(const TableItem &table_item,
                                      bool included_hidden,
                                      common::ObIArray<ColumnItem> *column_items);
  int resolve_all_generated_table_columns(const TableItem &table_item,
                                          common::ObIArray<ColumnItem> &column_items);
  int resolve_columns_for_partition_expr(ObRawExpr *&expr, ObIArray<ObQualifiedName> &columns,
                                         const TableItem &table_item,
                                         bool is_hidden);
  int resolve_and_split_sql_expr(const ParseNode &node, common::ObIArray<ObRawExpr*> &and_exprs);
  int resolve_and_split_sql_expr_with_bool_expr(const ParseNode &node,
                                                ObIArray<ObRawExpr*> &and_exprs);
  int resolve_where_clause(const ParseNode *node);
  int resolve_order_clause(const ParseNode *node, bool is_for_set_query = false);
  int resolve_limit_clause(const ParseNode *node);
  int resolve_into_clause(const ParseNode *node);
  int resolve_hints(const ParseNode *node);
  int resolve_outline_data_hints();
  const ParseNode *get_outline_data_hint_node();
  int inner_resolve_hints(const ParseNode &node,
                          const bool filter_embedded_hint,
                          bool &get_outline_data,
                          ObGlobalHint &global_hint,
                          ObIArray<ObHint*> &hints,
                          ObString &qb_name);
  int resolve_outline_hints();
  virtual int resolve_order_item(const ParseNode &sort_node, OrderItem &order_item);

  int add_column_to_stmt(const TableItem &table_item,
                         const share::schema::ObColumnSchemaV2 &col,
                         common::ObIArray<ObColumnRefRawExpr*> &column_ids,
                         ObDMLStmt *stmt = NULL);
  int add_all_rowkey_columns_to_stmt(const TableItem &table_item,
                                     common::ObIArray<ObColumnRefRawExpr*> &column_ids,
                                     ObDMLStmt *stmt = NULL);
  int build_nvl_expr(const ColumnItem *column_item, ObRawExpr *&expr1, ObRawExpr *&expr2);
  int build_nvl_expr(const share::schema::ObColumnSchemaV2 *column_schema, ObRawExpr *&expr);
  int build_nvl_expr(const ColumnItem *column_schema, ObRawExpr *&expr);
  /*
   * Resolve some 'special' expressions, which include:
   * 1. aggregate functions
   * 2. 'in' function
   * 3. operator expr(recursively on its children)
   */
  virtual int resolve_special_expr(ObRawExpr *&expr, ObStmtScope scope);
  int build_autoinc_nextval_expr(ObRawExpr *&expr,
                                 const uint64_t autoinc_table_id,
                                 const uint64_t autoinc_col_id,
                                 const ObString autoinc_table_name,
                                 const ObString autoinc_column_name);
  int build_partid_expr(ObRawExpr *&expr, const uint64_t table_id);
  virtual int resolve_subquery_info(const common::ObIArray<ObSubQueryInfo> &subquery_info);
  virtual int resolve_aggr_exprs(ObRawExpr *&expr, common::ObIArray<ObAggFunRawExpr*> &aggr_exprs,
                                 const bool need_analyze = true);
  virtual int resolve_win_func_exprs(ObRawExpr *&expr, common::ObIArray<ObWinFunRawExpr*> &win_exprs);
  int do_resolve_subquery_info(const ObSubQueryInfo &subquery_info, ObChildStmtResolver &child_resolver);
  int resolve_partitions(const ParseNode *part_node,
                         const share::schema::ObTableSchema &table_schema,
                         TableItem &table_item);
  int resolve_sample_clause(const ParseNode *part_node,
                            const uint64_t table_id);

  int check_pivot_aggr_expr(ObRawExpr *expr) const;
  int resolve_transpose_table(const ParseNode *transpose_node, TableItem *&table_item);
  int resolve_transpose_clause(const ParseNode &transpose_node, TransposeItem &transpose_item,
                               ObIArray<ObString> &columns_in_aggr);
  int resolve_transpose_columns(const ParseNode &transpose_node, ObIArray<ObString> &columns);
  int resolve_const_exprs(const ParseNode &transpose_node, ObIArray<ObRawExpr*> &const_exprs);
  int get_transpose_target_sql(const ObIArray<ObString> &columns_in_aggrs, TableItem &table_item,
                               TransposeItem &transpose_item, ObSqlString &target_sql);
  int get_target_sql_for_pivot(const ObIArray<ColumnItem> &column_items, TableItem &table_item,
                               TransposeItem &transpose_item, ObSqlString &target_sql);
  int get_target_sql_for_unpivot(const ObIArray<ColumnItem> &column_items, TableItem &table_item,
                                 TransposeItem &transpose_item, ObSqlString &target_sql);
  int format_from_subquery(const ObString &unpivot_alias_name, TableItem &table_item,
                           char *expr_str_buf, ObSqlString &target_sql);
  int get_partition_for_transpose(TableItem &table_item, ObSqlString &sql);
  int expand_transpose(const ObSqlString &transpose_def, TransposeItem &transpose_item,
                       TableItem *&table_item);
  int mark_unpivot_table(TransposeItem &transpose_item, TableItem *table_item);
  int remove_orig_table_item(TableItem &table_item);

  int check_basic_column_generated(const ObColumnRefRawExpr *col_expr,
                                  ObDMLStmt *dml_stmt,
                                  bool &is_generated);
  virtual int expand_view(TableItem &view_item);
  int do_expand_view(TableItem &view_item, ObChildStmtResolver &view_resolver);
  int check_pad_generated_column(const ObSQLSessionInfo &session_info,
                                 const share::schema::ObTableSchema &table_schema,
                                 const share::schema::ObColumnSchemaV2 &column_schema,
                                 bool is_link = false);
  int build_padding_expr(const ObSQLSessionInfo *session,
                         const ColumnItem *column,
                         ObRawExpr *&expr);

  int build_padding_expr(const ObSQLSessionInfo *session,
                         const share::schema::ObColumnSchemaV2 *column_schema,
                         ObRawExpr *&expr);

  virtual int check_need_use_sys_tenant(bool &use_sys_tenant) const;
  // check in sys view or show statement
  virtual int check_in_sysview(bool &in_sysview) const;
public:
  int resolve_table_relation_factor_wrapper(const ParseNode *table_node,
                                            uint64_t &dblink_id,
                                            uint64_t &database_id,
                                            common::ObString &table_name,
                                            common::ObString &synonym_name,
                                            common::ObString &db_name,
                                            common::ObString &synonym_db_name,
                                            common::ObString &dblink_name,
                                            bool &is_db_explicit,
                                            bool &use_sys_tenant,
                                            bool &is_reverse_link,
                                            common::ObIArray<uint64_t> &ref_obj_ids);
protected:
  int check_resolve_oracle_sys_view(const ParseNode *node, bool &is_oracle_sys_view);
  bool is_oracle_sys_view(const ObString &table_name);
  int inner_resolve_sys_view(const ParseNode *table_node,
                             uint64_t &database_id,
                             ObString &tbl_name,
                             ObString &db_name,
                             bool &is_db_explicit,
                             bool &use_sys_tenant);
  int inner_resolve_sys_view(const ParseNode *table_node,
                             uint64_t &database_id,
                             ObString &tbl_name,
                             ObString &db_name,
                             bool &use_sys_tenant);
  int resolve_table_relation_factor(const ParseNode *node,
                                    uint64_t tenant_id,
                                    uint64_t &dblink_id,
                                    uint64_t &database_id,
                                    common::ObString &table_name,
                                    common::ObString &synonym_name,
                                    common::ObString &synonym_db_name,
                                    common::ObString &db_name,
                                    common::ObString &dblink_name,
                                    bool &is_db_explicit,
                                    bool &is_reverse_link,
                                    common::ObIArray<uint64_t> &ref_obj_ids);
  int resolve_table_relation_factor(const ParseNode *node,
                                    uint64_t &dblink_id,
                                    uint64_t &database_id,
                                    common::ObString &table_name,
                                    common::ObString &synonym_name,
                                    common::ObString &synonym_db_name,
                                    common::ObString &db_name,
                                    common::ObString &dblink_name,
                                    bool &is_db_explicit,
                                    bool &is_reverse_link,
                                    common::ObIArray<uint64_t> &ref_obj_ids);
  int resolve_table_relation_factor(const ParseNode *node,
                                    uint64_t tenant_id,
                                    uint64_t &dblink_id,
                                    uint64_t &database_id,
                                    common::ObString &table_name,
                                    common::ObString &synonym_name,
                                    common::ObString &synonym_db_name,
                                    common::ObString &db_name,
                                    common::ObString &dblink_name,
                                    bool &is_reverse_link,
                                    common::ObIArray<uint64_t> &ref_obj_ids);
  int resolve_table_relation_factor(const ParseNode *node,
                                    uint64_t &dblink_id,
                                    uint64_t &database_id,
                                    common::ObString &table_name,
                                    common::ObString &synonym_name,
                                    common::ObString &synonym_db_name,
                                    common::ObString &db_name,
                                    common::ObString &dblink_name,
                                    bool &is_reverse_link,
                                    common::ObIArray<uint64_t> &ref_obj_ids);
  int resolve_table_relation_factor(const ParseNode *node,
                                    uint64_t tenant_id,
                                    uint64_t &dblink_id,
                                    uint64_t &database_id,
                                    common::ObString &table_name,
                                    common::ObString &synonym_name,
                                    common::ObString &synonym_db_name,
                                    common::ObString &db_name,
                                    common::ObString &dblink_name,
                                    ObSynonymChecker &synonym_checker,
                                    bool &is_reverse_link,
                                    common::ObIArray<uint64_t> &ref_obj_ids);
  int resolve_table_relation_factor(const ParseNode *node,
                                    uint64_t tenant_id,
                                    uint64_t &dblink_id,
                                    uint64_t &database_id,
                                    common::ObString &table_name,
                                    common::ObString &synonym_name,
                                    common::ObString &synonym_db_name,
                                    common::ObString &db_name,
                                    common::ObString &dblink_name,
                                    bool &is_db_expilicit,
                                    ObSynonymChecker &synonym_checker,
                                    bool &is_reverse_link,
                                    common::ObIArray<uint64_t> &ref_obj_ids);
  int resolve_table_relation_factor_normal(const ParseNode *node,
                                           uint64_t tenant_id,
                                           uint64_t &database_id,
                                           common::ObString &table_name,
                                           common::ObString &synonym_name,
                                           common::ObString &synonym_db_name,
                                           common::ObString &db_name,
                                           ObSynonymChecker &synonym_checker);
  int resolve_table_relation_factor_normal(const ParseNode *node,
                                           uint64_t tenant_id,
                                           uint64_t &database_id,
                                           common::ObString &table_name,
                                           common::ObString &synonym_name,
                                           common::ObString &synonym_db_name,
                                           common::ObString &db_name,
                                           bool &is_db_expilicit,
                                           ObSynonymChecker &synonym_checker);
  int resolve_table_relation_factor_dblink(const ParseNode *table_node,
                                           const uint64_t tenant_id,
                                           const common::ObString &dblink_name,
                                           uint64_t &dblink_id,
                                           common::ObString &table_name,
                                           common::ObString &database_name,
                                           bool is_reverse_link);
  int add_synonym_obj_id(const ObSynonymChecker &synonym_checker, bool error_with_exist);

  /*
   *
   */
  int resolve_is_expr(ObRawExpr *&expr, bool &replace_happened);
  int check_equal_conditions_for_resource_group(const ObIArray<ObRawExpr*> &filters);
  int recursive_check_equal_condition(const ObRawExpr &expr);
  int check_column_with_res_mapping_rule(const ObColumnRefRawExpr *col_expr,
                                         const ObConstRawExpr *const_expr);
  int resolve_autoincrement_column_is_null(ObRawExpr *&expr);
  int resolve_not_null_date_column_is_null(ObRawExpr *&expr, const ObExprResType* col_type);
  int resolve_partition_expr(const ParseNode &part_expr_node, ObRawExpr *&expr, common::ObIArray<ObQualifiedName> &columns);

  void report_user_error_msg(int &ret, const ObRawExpr *root_expr, const ObQualifiedName &q_name) const;
  bool is_need_add_additional_function(const ObRawExpr *expr);
  int add_additional_function_according_to_type(const ColumnItem *column,
                                                ObRawExpr *&expr,
                                                ObStmtScope scope,
                                                bool need_padding);
  int try_add_padding_expr_for_column_conv(const ColumnItem *column, ObRawExpr *&expr);
  int resolve_generated_column_expr_temp(TableItem *table_item);
  int find_generated_column_expr(ObRawExpr *&expr, bool &is_found);
  int deduce_generated_exprs(common::ObIArray<ObRawExpr*> &exprs);
  int resolve_external_name(ObQualifiedName &q_name,
                            ObIArray<ObQualifiedName> &columns,
                            ObIArray<ObRawExpr*> &real_exprs,
                            ObRawExpr *&expr);
  int resolve_geo_mbr_column();
  int build_prefix_index_compare_expr(ObRawExpr &column_expr,
                                      ObRawExpr *prefix_expr,
                                      ObItemType type,
                                      ObRawExpr &value_expr,
                                      ObRawExpr *escape_expr,
                                      ObRawExpr *&new_op_expr);

  int add_column_for_schema(
      share::schema::ObColumnSchemaV2 &col,
      share::schema::ObTableSchema *&view_schema,
      const ObString *col_name,
      bool is_rowkey,
      bool is_depend_col);

  int check_json_table_column_constrain(ObDmlJtColDef *col_def);
  bool check_generated_column_has_json_constraint(const ObSelectStmt *stmt,
                                                  const ObColumnRefRawExpr *col_expr);

  int resolve_json_table_check_dup_path(ObIArray<ObDmlJtColDef*>& columns,
                                            const ObString& column_name);
  int resolve_json_table_check_dup_name(const ObJsonTableDef* table_def,
                                            const ObString& column_name,
                                            bool& exists);

  int resolve_json_table_column_type(const ParseNode &parse_tree,
                                     const int col_type,
                                     ObDataType &data_type,
                                     ObDmlJtColDef *col_def);
  int generate_json_table_output_column_item(TableItem *table_item,
                                          const ObDataType &data_type,
                                          const ObString &column_name,
                                          int64_t column_id,
                                          ColumnItem *&col_item);
  int resolve_json_table_regular_column(const ParseNode &parse_tree,
                                        TableItem *table_item,
                                        ObDmlJtColDef *&col_def,
                                        int32_t parent,
                                        int32_t& id,
                                        int64_t& column_id);
  int resolve_json_table_nested_column(const ParseNode &parse_tree,
                                        TableItem *table_item,
                                        ObDmlJtColDef *&col_def,
                                        int32_t parent,
                                        int32_t& id,
                                        int64_t& column_id);
  int resolve_json_table_column_item(const ParseNode &parse_tree,
                                      TableItem *table_item,
                                      ObDmlJtColDef* col,
                                      int32_t parent,
                                      int32_t& id,
                                      int64_t& column_id);
  int resolve_json_table_column_item(const TableItem &table_item,
                                         const ObString &column_name,
                                         ColumnItem *&col_item);
  int resolve_json_table_column_all_items(const TableItem &table_item,
                                          ObIArray<ColumnItem> &col_items);

  int resolve_function_table_column_item(const TableItem &table_item,
                                         ObIArray<ColumnItem> &col_items);

  int resolve_function_table_column_item(const TableItem &table_item,
                                         const common::ObString &column_name,
                                         ColumnItem *&col_item);
  int resolve_function_table_column_item(const TableItem &table_item,
                                         const common::ObObjMeta &meta_type,
                                         const common::ObAccuracy &accuracy,
                                         const ObString &column_name,
                                         uint64_t column_id,
                                         ColumnItem *&col_item);
  int resolve_generated_table_column_item(const TableItem &table_item,
                                          const common::ObString &column_name,
                                          ColumnItem *&col_item,
                                          ObDMLStmt *stmt = NULL,
                                          const uint64_t column_id = OB_INVALID_ID,
                                          const int64_t select_item_offset = 0,
                                          const bool skip_check = false);
  int erase_redundant_generated_table_column_flag(const ObSelectStmt &ref_stmt, const ObRawExpr *ref_expr, ObColumnRefRawExpr &col_expr) const;

  // for cte
  int add_cte_table_to_children(ObChildStmtResolver& child_resolver);
  int add_parent_cte_table_to_children(ObChildStmtResolver& child_resolver);
  void set_non_record(bool record) { with_clause_without_record_ = record; };
  int check_current_CTE_name_exist(const ObString &var_name, bool &dup_name);
  int check_current_CTE_name_exist(const ObString &var_name, bool &dup_name, TableItem *&table_item);
  int check_parent_CTE_name_exist(const ObString &var_name, bool &dup_name);
  int check_parent_CTE_name_exist(const ObString &var_name, bool &dup_name, TableItem *&table_item);
  int set_cte_ctx(ObCteResolverCtx &cte_ctx, bool copy_col_name = true, bool in_subquery = false);
  int add_cte_table_item(TableItem *table_item,  bool &dup_name);
  int get_opt_alias_colnames_for_recursive_cte(ObIArray<ObString>& columns, const ParseNode *parse_tree);
  int init_cte_resolver(ObSelectResolver &select_resolver, const ParseNode *opt_col_node, ObString& table_name);
  int add_fake_schema(ObSelectStmt* left_stmt);
  int resolve_basic_table_without_cte(const ParseNode &parse_tree, TableItem *&table_item);
  int resolve_basic_table_with_cte(const ParseNode &parse_tree, TableItem *&table_item);
  int resolve_cte_table(const ParseNode &parse_tree, const TableItem *CTE_table_item, TableItem *&table_item);
  int resolve_recursive_cte_table(const ParseNode &parse_tree, TableItem *&table_item);
  int resolve_with_clause_opt_alias_colnames(const ParseNode *parse_tree, TableItem *&table_item);
  int set_parent_cte();
  int resolve_with_clause_subquery(const ParseNode &parse_tree, TableItem *&table_item);
  int resolve_with_clause(const ParseNode *node, bool same_level = false);

  int check_oracle_outer_join_condition(const ObRawExpr *expr);
  int check_oracle_outer_join_in_or_validity(const ObRawExpr * expr,
                                    ObIArray<uint64_t> &right_tables);
  int check_oracle_outer_join_expr_validity(const ObRawExpr *expr,
                                            ObIArray<uint64_t> &right_tables,
                                            ObItemType parent_type);
  int check_single_oracle_outer_join_expr_validity(const ObRawExpr *right_expr,
                                          ObIArray<uint64_t> &le_left_tables,
                                          ObIArray<uint64_t> &le_right_tables,
                                          ObIArray<uint64_t> &left_tables,
                                          ObIArray<uint64_t> &right_tables);
  int remove_outer_join_symbol(ObRawExpr* &expr);
  int resolve_outer_join_symbol(const ObStmtScope scope, ObRawExpr* &expr);
  int generate_outer_join_tables();

  int generate_outer_join_dependency(const common::ObIArray<TableItem*> &table_items,
                                     const common::ObIArray<ObRawExpr*> &exprs,
                                     common::ObIArray<common::ObBitSet<> > &table_dependencies);

  int extract_column_with_outer_join_symbol(const ObRawExpr *expr,
                                            common::ObIArray<uint64_t> &left_tables,
                                            common::ObIArray<uint64_t> &right_tables);
  int do_extract_column(const ObRawExpr *expr,
                        ObIArray<uint64_t> &left_tables,
                        ObIArray<uint64_t> &right_tables);
  int add_oracle_outer_join_dependency(const common::ObIArray<uint64_t> &all_tables,
                                       const common::ObIArray<uint64_t> &left_tables,
                                       uint64_t right_table_id,
                                       common::ObIArray<common::ObBitSet<> > &table_dependencies) ;

  int build_outer_join_table_by_dependency(
      const common::ObIArray<common::ObBitSet<> > &table_dependencies, ObDMLStmt &stmt);

  int deliver_outer_join_conditions(common::ObIArray<ObRawExpr*> &exprs,
                                     common::ObIArray<JoinedTable*> &joined_tables);

  int deliver_expr_to_outer_join_table(const ObRawExpr *expr,
                                       const common::ObIArray<uint64_t> &table_ids,
                                       JoinedTable *joined_table,
                                       bool &is_delivered);


  void set_has_ansi_join(bool has) { has_ansi_join_ = has; }
  bool has_ansi_join() { return has_ansi_join_; }

  void set_has_oracle_join(bool has) { has_oracle_join_ = has; }
  bool has_oracle_join() { return has_oracle_join_; }

  const TableItem *get_from_items_order(int64_t index) const { return from_items_order_.at(index); }
  TableItem *get_from_items_order(int64_t index) { return from_items_order_.at(index); }
  int add_from_items_order(TableItem *ti) { return from_items_order_.push_back(ti); }
  int add_check_constraint_to_stmt(const TableItem *table_item,
                                   const share::schema::ObTableSchema *table_schema,
                                   ObIArray<int64_t> *check_flags = NULL);
  virtual const ObString get_view_db_name() const { return ObString(); }
  virtual const ObString get_view_name() const { return ObString(); }

  int check_table_item_with_gen_col_using_udf(const TableItem *table_item, bool &ans);

  int resolve_rowid_expr(ObDMLStmt *stmt, const TableItem &table_item, ObRawExpr *&ref_epxr);

  int check_rowid_table_column_in_all_namespace(const ObQualifiedName &q_name,
                                                const TableItem *&table_item,
                                                ObDMLStmt *&dml_stmt,
                                                int32_t &cur_level,
                                                ObQueryRefRawExpr *&query_ref);
  int get_view_id_for_trigger(const TableItem &view_item, uint64_t &view_id);
  bool get_joininfo_by_id(int64_t table_id, ResolverJoinInfo *&join_info);
  int get_json_table_column_by_id(uint64_t table_id, ObDmlJtColDef *&col_def);

  int get_table_schema(const uint64_t table_id,
                       const uint64_t ref_table_id,
                       ObDMLStmt *stmt,
                       const ObTableSchema *&table_schema);
  int generate_check_constraint_exprs(const TableItem *table_item,
                                      const share::schema::ObTableSchema *table_schema,
                                      common::ObIArray<ObRawExpr*> &check_exprs,
                                      ObIArray<int64_t> *check_flags = NULL);
private:
  int resolve_function_table_column_item_udf(const TableItem &table_item,
                                             common::ObIArray<ColumnItem> &col_items);
  int resolve_function_table_column_item_sys_func(const TableItem &table_item,
                                                  common::ObIArray<ColumnItem> &col_items);
  int add_column_ref_to_set(ObRawExpr *&expr, ObIArray<TableItem*> *table_list);
  int check_table_exist_or_not(uint64_t tenant_id, uint64_t &database_id,
                                 common::ObString &table_name, common::ObString &db_name);
  int resolve_table_relation_recursively(uint64_t tenant_id,
                                         uint64_t &database_id,
                                         common::ObString &table_name,
                                         common::ObString &db_name,
                                         ObSynonymChecker &synonym_checker,
                                         bool is_db_explicit,
                                         bool &is_synonym_public);
  int add_synonym_version(const common::ObIArray<uint64_t> &synonym_ids);

  int find_const_params_for_gen_column(const ObRawExpr &expr);
  int check_order_by_for_subquery_stmt(const ObSubQueryInfo &info);
  int check_stmt_order_by(const ObSelectStmt *stmt);

  int resolve_ora_rowscn_pseudo_column(const ObQualifiedName &q_name, ObRawExpr *&real_ref_expr);
  int resolve_rowid_pseudo_column(const ObQualifiedName &q_name, ObRawExpr *&real_ref_expr);
  int resolve_pseudo_column(const ObQualifiedName &q_name, ObRawExpr *&real_ref_expr);
  int check_keystore_status();
  int resolve_current_of(const ParseNode &node, ObDMLStmt &stmt, ObIArray<ObRawExpr*> &and_exprs);
  int update_errno_if_sequence_object(const ObQualifiedName &q_name, int old_ret);
  int get_all_column_ref(ObRawExpr *expr, common::ObIArray<ObColumnRefRawExpr*> &arr);

  // a synonym may a dblink synonym, while failed to resolving a sysnonym to a table name,
  // wo should try to resolving to a dblink name.
  // in out param: table_name, in: may something link "remote_schema.test@my_link", out: "test"
  // out param: dblink_name("my_link"), db_name("remote_schema"), dblink_id(id of my_link)
  int resolve_dblink_with_synonym(uint64_t tenant_id, ObString &table_name, ObString &dblink_name,
                                  ObString &db_name, uint64_t &dblink_id);
  int process_part_str(ObIAllocator &calc_buf,
                       const ObString &part_str,
                       ObString &new_part_str);

  int convert_udf_to_agg_expr(ObRawExpr *&expr,
                              ObRawExpr *parent_expr,
                              ObExprResolveContext &ctx);

  int reset_calc_part_id_param_exprs(ObRawExpr *&expr, ObIArray<ObQualifiedName> &columns);

  int check_index_table_has_partition_keys(const ObTableSchema *index_schema,
                                           const ObPartitionKeyInfo &partition_keys,
                                           bool &has_part_key);

  ///////////functions for sql hint/////////////
  int resolve_global_hint(const ParseNode &hint_node,
                          ObGlobalHint &global_hint,
                          bool &resolved_hint);
  int resolve_transform_hint(const ParseNode &hint_node,
                             bool &resolved_hint,
                             ObIArray<ObHint*> &trans_hints);
  int resolve_optimize_hint(const ParseNode &hint_node,
                            bool &resolved_hint,
                            ObIArray<ObHint*> &opt_hints);
  int resolve_index_hint(const ParseNode &index_node,
                         ObOptHint *&opt_hint);
  int resolve_index_hint(const TableItem &table, // resolved mysql mode index hint after table
                         const ParseNode &index_hint_node);
  int resolve_table_parallel_hint(const ParseNode &hint_node, ObOptHint *&opt_hint);
  int resolve_join_order_hint(const ParseNode &hint_node, ObOptHint *&opt_hint);
  int resolve_join_hint(const ParseNode &join_node, ObIArray<ObHint*> &join_hints);
  int resolve_pq_map_hint(const ParseNode &hint_node, ObOptHint *&opt_hint);
  int resolve_pq_distribute_hint(const ParseNode &hint_node, ObOptHint *&opt_hint);
  int resolve_pq_set_hint(const ParseNode &hint_node, ObOptHint *&opt_hint);
  int resolve_join_filter_hint(const ParseNode &join_node, ObOptHint *&opt_hint);
  int resolve_aggregation_hint(const ParseNode &hint_node, ObOptHint *&hint);
  int resolve_normal_transform_hint(const ParseNode &hint_node, ObTransHint *&hint);
  int resolve_normal_optimize_hint(const ParseNode &hint_node, ObOptHint *&hint);
  int resolve_view_merge_hint(const ParseNode &hint_node, ObTransHint *&hint);
  int resolve_or_expand_hint(const ParseNode &hint_node, ObTransHint *&hint);
  int resolve_materialize_hint(const ParseNode &hint_node, ObTransHint *&hint);
  int resolve_semi_to_inner_hint(const ParseNode &hint_node, ObTransHint *&hint);
  int resolve_coalesce_sq_hint(const ParseNode &hint_node, ObTransHint *&hint);
  int resolve_count_to_exists_hint(const ParseNode &hint_node, ObTransHint *&hint);
  int resolve_left_to_anti_hint(const ParseNode &hint_node, ObTransHint *&hint);
  int resolve_eliminate_join_hint(const ParseNode &hint_node, ObTransHint *&hint);
  int resolve_win_magic_hint(const ParseNode &hint_node, ObTransHint *&hint);
  int resolve_place_group_by_hint(const ParseNode &hint_node, ObTransHint *&hint);
  int resolve_tb_name_list(const ParseNode *tb_name_list_node, ObIArray<ObSEArray<ObTableInHint, 4>> &tb_name_list);
  int resolve_monitor_ids(const ParseNode &tracing_node, ObIArray<ObMonitorHint> &monitoring_ids);
  int resolve_tables_in_leading_hint(const ParseNode *tables_node, ObLeadingTable &leading_table);
  int resolve_simple_table_list_in_hint(const ParseNode *table_list,
                                        common::ObIArray<ObTableInHint> &hint_tables);
  int resolve_table_relation_in_hint(const ParseNode &table_node, ObTableInHint &table_in_hint);
  int resolve_qb_name_node(const ParseNode *qb_name_node, common::ObString &qb_name);
  int resolve_multi_qb_name_list(const ParseNode *qb_name_node, common::ObIArray<QbNameList> &qb_name_list);
  int resolve_qb_name_list(const ParseNode *qb_name_node, ObIArray<ObString> &qb_name_list);
  int get_valid_dist_methods(const ParseNode *dist_methods_node,
                             ObIArray<ObItemType> &dist_methods,
                             bool &is_valid);
  int resolve_pq_distribute_window_hint(const ParseNode &hint_node, ObOptHint *&opt_hint);
  int resolve_win_dist_options(const ParseNode *option_list,
                               ObIArray<ObWindowDistHint::WinDistOption> &win_dist_options);
  int resolve_win_dist_option(const ParseNode *option,
                              ObWindowDistHint::WinDistOption &dist_option,
                              bool &is_valid);
  int resolve_table_dynamic_sampling_hint(const ParseNode &hint_node, ObOptHint *&opt_hint);
  //////////end of functions for sql hint/////////////


  int resolve_table_check_constraint_items(const TableItem *table_item,
                                           const ObTableSchema *table_schema);
  int find_table_index_infos(const ObString &index_name,
                             const TableItem *table_item,
                             bool &find_it,
                             int64_t &table_id,
                             int64_t &ref_id);
  int check_cast_multiset(const ObRawExpr *expr, const ObRawExpr *parent_expr = NULL);

  int replace_col_udt_qname(ObQualifiedName& q_name);
  int check_column_udt_type(ParseNode *root_node);

  int replace_pl_relative_expr_to_question_mark(ObRawExpr *&real_ref_expr);
  bool check_expr_has_colref(ObRawExpr *expr);

  int resolve_values_table_item(const ParseNode &table_node, TableItem *&table_item);
  int resolve_table_values_for_select(const ParseNode &table_node,
                                      ObIArray<ObRawExpr*> &table_values,
                                      int64_t &column_cnt);
  int resolve_table_values_for_insert(const ParseNode &table_node,
                                      ObIArray<ObRawExpr*> &table_values,
                                      int64_t &column_cnt);
  int gen_values_table_column_items(const int64_t column_cnt, TableItem &table_item);
  int get_values_res_types(const ObIArray<ObExprResType> &cur_values_types,
                           ObIArray<ObExprResType> &res_types);
  int try_add_cast_to_values(const ObIArray<ObExprResType> &res_types,
                             ObIArray<ObRawExpr*> &values_vector);
  int refine_generate_table_column_name(const ParseNode &column_alias_node,
                                        ObSelectStmt &select_stmt);
  int replace_column_ref(ObIArray<ObRawExpr*> &values_vector,
                         ObIArray<ObColumnRefRawExpr*> &values_desc,
                         ObRawExpr *&expr);
  int build_row_for_empty_values(ObIArray<ObRawExpr*> &values_vector);
protected:
  struct GenColumnExprInfo {
    GenColumnExprInfo():
      dependent_expr_(NULL),
      stmt_(NULL),
      table_item_(NULL),
      column_name_() {}
    TO_STRING_KV(
      K_(dependent_expr),
      K_(stmt),
      K_(table_item),
      K_(column_name)
    );
    ObRawExpr* dependent_expr_; //生成列的真实表达式
    ObDMLStmt* stmt_; //生成列所在的stmt
    TableItem* table_item_; //生成列所属表
    common::ObString column_name_;  //生成列的名称
  };
  int add_parent_gen_col_exprs(const ObArray<GenColumnExprInfo> &gen_col_exprs);
protected:
  ObStmtScope current_scope_;
  int32_t current_level_;
  bool field_list_first_;
  ObDMLResolver *parent_namespace_resolver_;
  ObColumnNamespaceChecker column_namespace_checker_;
  ObSequenceNamespaceChecker sequence_namespace_checker_;
  //these generated column exprs are not the reference by query expression,
  //just some expr template in schema,
  //only the generated column expr referenced by query can be deposited to stmt
  common::ObArray<GenColumnExprInfo > gen_col_exprs_;
  common::ObArray<TableItem*> from_items_order_;

  ObQueryRefRawExpr *query_ref_;

  // for oracle style outer join
  bool has_ansi_join_;
  bool has_oracle_join_;

  /*
   * 在with clause的解析中，我们不能将解析的表添加到影响此plan是否需要进入trans_service
   * 的global_dependency_table中去。
   * 例如：
   * with cte (select * from t1) select 1 from dual;
   * 如果加入了，上面这句将会进入trans_service，而之后则会报错，因为其实它检测到相关table的
   * partition location为空。
   * */
  bool with_clause_without_record_;
  bool is_prepare_stage_;
  bool in_pl_;
  bool resolve_alias_for_subquery_;
  int32_t current_view_level_;
  uint64_t view_ref_id_;
  bool is_resolving_view_;
  common::ObSEArray<ResolverJoinInfo, 1, common::ModulePageAllocator, true> join_infos_;
  //store parent cte tables
  common::ObSEArray<TableItem *, 4, common::ModulePageAllocator, true> parent_cte_tables_;
  //store cte tables of current level
  common::ObSEArray<TableItem *, 4, common::ModulePageAllocator, true> current_cte_tables_;

  ObSharedExprResolver expr_resv_ctx_;
  /*these member is only for with clause*/
  ObCteResolverCtx cte_ctx_;

  //store json table column info
  common::ObSEArray<ObDmlJtColDef *, 1, common::ModulePageAllocator, true> json_table_infos_;
  common::ObSEArray<ObRawExpr*, 4, common::ModulePageAllocator, true> pseudo_external_file_col_exprs_;
  //for validity check for on-condition with (+)
  common::ObSEArray<uint64_t, 4, common::ModulePageAllocator, true> ansi_join_outer_table_id_;

  //for values table used to insert stmt:insert into table values row()....
  ObInsertResolver *upper_insert_resolver_;
protected:
  DISALLOW_COPY_AND_ASSIGN(ObDMLResolver);
};

}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SQL_RESOLVER_DML_OB_DML_RESOLVER_H_ */
