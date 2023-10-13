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

#ifndef _OB_SELECT_RESOLVER_H
#define _OB_SELECT_RESOLVER_H
#include "sql/parser/ob_parser.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/resolver/dml/ob_dml_resolver.h"
#include "sql/resolver/dml/ob_standard_group_checker.h"
#include "sql/rewrite/ob_stmt_comparer.h"
#include "common/ob_smart_call.h"

namespace oceanbase
{
namespace sql
{
//提供一个接口类，用于解析各种子查询，包括where子查询，union子查询，view子查询，generated table子查询
//这样做的目的是屏蔽子查询解析的一些细节，不用将整个dml resolver的结构传递给子查询解析的函数
//避免调用过程中看到过多的属性，降低耦合
class ObChildStmtResolver
{
public:
  ObChildStmtResolver()
    : parent_aggr_level_(-1) {}
  virtual int resolve_child_stmt(const ParseNode &parse_tree) = 0;
  virtual int add_parent_cte_table_item(TableItem *table_item) = 0;
  virtual ObSelectStmt *get_child_stmt() = 0;
  void set_parent_aggr_level(int64_t parent_aggr_level) { parent_aggr_level_ = parent_aggr_level; }
protected:
  //当这个level不会-1的时候说明该child stmt是一个位于aggregate function中的subquery
  //注意这个时候，aggregate function还没有被上推,这个level不是aggregate function最终的level
  int64_t parent_aggr_level_;
};

class ObSelectResolver: public ObDMLResolver, public ObChildStmtResolver
{
public:
  explicit ObSelectResolver(ObResolverParams &params);
  virtual ~ObSelectResolver();

  virtual int resolve(const ParseNode &parse_tree);
  ObSelectStmt *get_select_stmt();
  void set_calc_found_rows(bool found_rows) { has_calc_found_rows_ = found_rows; }
  void set_has_top_limit(bool has_top_limit) {has_top_limit_ = has_top_limit; }
  virtual int resolve_child_stmt(const ParseNode &parse_tree)
  { return SMART_CALL(resolve(parse_tree)); }
  virtual ObSelectStmt *get_child_stmt() { return get_select_stmt(); }
  virtual bool is_select_resolver() const { return true; }
  inline bool can_produce_aggr() const
  { return T_FIELD_LIST_SCOPE == current_scope_ ||
           T_HAVING_SCOPE == current_scope_ ||
           T_ORDER_SCOPE == current_scope_ ||
           T_NAMED_WINDOWS_SCOPE == current_scope_; }
  int add_aggr_expr(ObAggFunRawExpr *&final_aggr_expr);
  int add_unsettled_column(ObRawExpr *column_expr);
  void set_in_set_query(bool in_set_query) { in_set_query_ = in_set_query; }
  bool is_in_set_query() const { return in_set_query_; }
  void set_is_sub_stmt(bool in_subquery) { is_sub_stmt_ = in_subquery; }
  bool is_substmt() const { return is_sub_stmt_; }
  void set_in_exists_subquery(bool in_exists_subquery) { in_exists_subquery_ = in_exists_subquery; }
  bool is_in_exists_subquery() const { return in_exists_subquery_; }
  virtual int resolve_column_ref_expr(const ObQualifiedName &q_name, ObRawExpr *&real_ref_expr);
  void set_transpose_item(const TransposeItem *transpose_item) { transpose_item_ = transpose_item; }
  void set_is_left_child(const bool is_left_child) { is_left_child_ = is_left_child; }
  void set_having_has_self_column() { having_has_self_column_ = true; }
  bool has_having_self_column() const { return having_has_self_column_; }
  void assign_grouping() { has_grouping_ = true; }
  void reassign_grouping() { has_grouping_ = false; }
  inline bool has_grouping() const { return has_grouping_; };
  void set_has_group_by_clause() { has_group_by_clause_ = true; }
  inline bool has_group_by_clause() const { return has_group_by_clause_; };
  int check_cte_pseudo(const ParseNode *search_node, const ParseNode *cycle_node);
  int get_current_recursive_cte_table(ObSelectStmt* ref_stmt);
  int resolve_cte_pseudo_column(const ParseNode *search_node,
                                const ParseNode *cycle_node,
                                const TableItem *table_item,
                                ObString &search_pseudo_column_name,
                                ObString &cycle_pseudo_column_name);
  void set_current_recursive_cte_table_item(TableItem *table_item) { current_recursive_cte_table_item_ = table_item; }
  void set_current_cte_involed_stmt(ObSelectStmt *stmt) { current_cte_involed_stmt_ = stmt; }

  // function members
  TO_STRING_KV(K_(has_calc_found_rows),
               K_(has_top_limit),
               K_(in_set_query),
               K_(is_sub_stmt));

protected:
  int resolve_set_query(const ParseNode &parse_node);
  int do_resolve_set_query_in_cte(const ParseNode &parse_tree, bool swap_branch);
  int do_resolve_set_query(const ParseNode &parse_tree);
  int resolve_set_query_hint();
  int do_resolve_set_query(const ParseNode &parse_tree,
                           common::ObIArray<ObSelectStmt*> &child_stmt,
                           const bool is_left_child = false);
  virtual int do_resolve_set_query(const ParseNode &parse_tree,
                                   ObSelectStmt *&child_stmt,
                                   const bool is_left_child = false);
  int check_cte_set_types(ObSelectStmt &left_stmt, ObSelectStmt &right_stmt);
  int set_stmt_set_type(ObSelectStmt *select_stmt, ParseNode *set_node);
  int is_set_type_same(const ObSelectStmt *select_stmt, ParseNode *set_node, bool &is_type_same);
  int check_recursive_cte_limited();
  int check_pseudo_column_name_legal(const ObString& name);
  int search_connect_group_by_clause(const ParseNode &parent,
                                     const ParseNode *&start_with,
                                     const ParseNode *&connect_by,
                                     const ParseNode *&group_by,
                                     const ParseNode *&having);
  int resolve_normal_query(const ParseNode &parse_node);
  int create_joined_table_item(JoinedTable *&joined_table);
  virtual int check_special_join_table(const TableItem &join_table, bool is_left_child, ObItemType join_type) override;
  int resolve_search_clause(const ParseNode &parse_tree, const TableItem* cte_table_item, ObString& name);
  int resolve_search_item(const ParseNode* sort_list, ObSelectStmt* r_union_stmt);
  int resolve_search_pseudo(const ParseNode* search_set_clause, ObSelectStmt* r_union_stmt, ObString& name);
  int resolve_cycle_clause(const ParseNode &parse_tree, const TableItem* cte_table_item, ObString& name);
  int resolve_cycle_item(const ParseNode* alias_list, ObSelectStmt* r_union_stmt);
  int resolve_cycle_pseudo(const ParseNode* cycle_set_clause,
                           ObSelectStmt* r_union_stmt,
                           const ParseNode* cycle_value,
                           const ParseNode* cycle_default_value,
                           ObString& cycle_pseudo_column_name);
  int generate_fake_column_expr(const share::schema::ObColumnSchemaV2 *column_schema, ObSelectStmt* left_stmt, ObColumnRefRawExpr*& fake_col_expr);
  int add_parent_cte_table_item(TableItem *table_item);
  int resolve_from_clause(const ParseNode *node);
  int resolve_field_list(const ParseNode &node);
  inline bool is_colum_without_alias(ParseNode *project_node);
  int resolve_star(const ParseNode *node);
  int resolve_group_clause(const ParseNode *node);
  int resolve_groupby_node(const ParseNode *group_node,
                           const ParseNode *group_sort_node,
                           common::ObIArray<ObRawExpr*> &groupby_exprs,
                           common::ObIArray<ObRawExpr*> &rollup_exprs,
                           common::ObIArray<OrderItem> &order_items,
                           bool &has_explicit_dir,
                           bool is_groupby_expr,
                           int group_expr_level);
  int resolve_group_by_sql_expr(const ParseNode *group_node,
                                const ParseNode *group_sort_node,
                                common::ObIArray<ObRawExpr*> &groupby_exprs,
                                common::ObIArray<ObRawExpr*> &rollup_exprs,
                                common::ObIArray<OrderItem> &order_items,
                                ObSelectStmt *select_stmt,
                                bool &has_explicit_dir,
                                bool is_groupby_expr);
  int check_rollup_clause(const ParseNode *node, bool &has_rollup);

  int resolve_group_by_list(const ParseNode *node,
                            common::ObIArray<ObRawExpr*> &groupby_exprs,
                            common::ObIArray<ObRawExpr*> &rollup_exprs,
                            common::ObIArray<OrderItem> &order_items,
                            bool &has_explicit_dir);
  int resolve_rollup_list(const ParseNode *node, ObRollupItem &rollup_item);
  int resolve_cube_list(const ParseNode *node, ObCubeItem &cube_item);
  int resolve_grouping_sets_list(const ParseNode *node,
                                 ObGroupingSetsItem &grouping_sets_item);
  int resolve_with_rollup_clause(const ParseNode *node,
                                 common::ObIArray<ObRawExpr*> &groupby_exprs,
                                 common::ObIArray<ObRawExpr*> &rollup_exprs,
                                 common::ObIArray<OrderItem> &order_items,
                                 bool &has_explicit_dir);
  int resolve_for_update_clause(const ParseNode *node);
  int resolve_for_update_clause_oracle(const ParseNode &node);
  int set_for_update_mysql(ObSelectStmt &stmt, const int64_t wait_us);
  int set_for_update_oracle(ObSelectStmt &stmt,
                            const int64_t wait_us,
                            bool skip_locked,
                            ObColumnRefRawExpr *col = NULL);
  int resolve_for_update_clause_mysql(const ParseNode &node);
  int resolve_all_fake_cte_table_columns(const TableItem &table_item, common::ObIArray<ColumnItem> *column_items);

  int check_cycle_clause(const ParseNode &node);
  int check_search_clause(const ParseNode &node);
  int check_search_cycle_set_column(const ParseNode &search_node, const ParseNode &cycle_node);
  int check_cycle_values(const ParseNode &cycle_node);
  int check_unsupported_operation_in_recursive_branch();
  int check_recursive_cte_usage(const ObSelectStmt &select_stmt);
  int gen_unpivot_target_column(const int64_t table_count, ObSelectStmt &select_stmt,
                                TableItem &table_item);

  //resolve select into
  int resolve_into_clause(const ParseNode *node);
  int resolve_into_const_node(const ParseNode *node, ObObj &obj);
  int resolve_into_filed_node(const ParseNode *node, ObSelectIntoItem &into_item);
  int resolve_into_line_node(const ParseNode *node, ObSelectIntoItem &into_item);
  int resolve_into_variable_node(const ParseNode *node, ObSelectIntoItem &into_item);

  // resolve_star related functions
  int resolve_star_for_table_groups();
  int find_joined_table_group_for_table(const uint64_t table_id, int64_t &jt_idx);
  int find_select_columns_for_join_group(const int64_t jt_idx, common::ObArray<SelectItem> *sorted_select_items);
  int find_select_columns_for_joined_table_recursive(const JoinedTable *jt,
                                                     common::ObIArray<SelectItem> *sorted_select_items);
  int coalesce_select_columns_for_joined_table(const common::ObIArray<SelectItem> *left,
                                               const common::ObIArray<SelectItem> *right,
                                               const ObJoinType type,
                                               const common::ObIArray<common::ObString> &using_columns,
                                               common::ObIArray<SelectItem> *coalesced_columns);
  int expand_target_list(const TableItem &table_item, common::ObIArray<SelectItem> &target_list);
  int recursive_find_coalesce_expr(const JoinedTable *&joined_table,
                                   const common::ObString &cname,
                                   ObRawExpr *&coalesce_expr);
  int resolve_having_clause(const ParseNode *node);
  int resolve_named_windows_clause(const ParseNode *node);
  int check_nested_aggr_in_having(ObRawExpr* expr);
  int resolve_start_with_clause(const ParseNode *node);
  int check_connect_by_expr_validity(const ObRawExpr *raw_expr, bool is_prior);
  int resolve_connect_by_clause(const ParseNode *node);
  int check_correlated_column_ref(const ObSelectStmt &select_stmt, ObRawExpr *expr, bool &correalted_query);
  virtual int resolve_order_item(const ParseNode &sort_node, OrderItem &order_item);
  virtual int resolve_order_item_by_pos(int64_t pos, OrderItem &order_item, ObSelectStmt *select_stmt);
  virtual int resolve_literal_order_item(const ParseNode &sort_node, ObRawExpr *expr, OrderItem &order_item, ObSelectStmt *select_stmt);
  virtual int resolve_aggr_exprs(ObRawExpr *&expr, common::ObIArray<ObAggFunRawExpr*> &aggr_exprs,
                                 const bool need_analyze = true);
  virtual int resolve_win_func_exprs(ObRawExpr *&expr, common::ObIArray<ObWinFunRawExpr*> &win_exprs);
  int resolve_column_ref_in_all_namespace(const ObQualifiedName &q_name, ObRawExpr *&real_ref_expr);
  /**
   * resolve column real ref expr, search order: alias name first, followed by table column
   * @param q_name, column name
   * @param real_ref_expr, column real ref expr
   */
  int resolve_column_ref_alias_first(const ObQualifiedName &q_name, ObRawExpr *&real_ref_expr);
  int resolve_column_ref_table_first(const ObQualifiedName& q_name, ObRawExpr*& real_ref_expr, bool need_further_match_alias = true);
  int resolve_column_ref_for_having(const ObQualifiedName &q_name, ObRawExpr *&real_ref_expr);
  int resolve_column_ref_for_search(const ObQualifiedName &q_name, ObRawExpr *&real_ref_expr);
  int resolve_table_column_ref(const ObQualifiedName &q_name, ObRawExpr *&real_ref_expr);
  int resolve_alias_column_ref(const ObQualifiedName &q_name, ObRawExpr *&real_ref_expr);
  int resolve_column_ref_in_group_by(const ObQualifiedName &q_name, ObRawExpr *&real_ref_expr);
  int resolve_all_function_table_columns(const TableItem &table_item, ObIArray<ColumnItem> *column_items);
  int resolve_all_json_table_columns(const TableItem &table_item, ObIArray<ColumnItem> *column_items);
  int resolve_all_generated_table_columns(const TableItem &table_item, common::ObIArray<ColumnItem> *column_items);
  virtual int set_select_item(SelectItem &select_item, bool is_auto_gen);
  int resolve_query_options(const ParseNode *node);
  virtual int resolve_subquery_info(const common::ObIArray<ObSubQueryInfo> &subquery_info);
  virtual int resolve_column_ref_for_subquery(const ObQualifiedName &q_name, ObRawExpr *&real_ref_expr);
  inline bool column_need_check_group_by(const ObQualifiedName &q_name) const;
  int check_column_ref_in_group_by_or_field_list(const ObRawExpr *column_ref) const;
  int wrap_alias_column_ref(const ObQualifiedName &q_name, ObRawExpr *&real_ref_expr);
  virtual int check_need_use_sys_tenant(bool &use_sys_tenant) const;
  virtual int check_in_sysview(bool &in_sysview) const override;
  int check_group_by();
  int check_order_by();
  int check_pseudo_columns();
  int check_grouping_columns();
  int check_grouping_columns(ObSelectStmt &stmt, ObRawExpr *&expr);
  int check_window_exprs();
  int check_sequence_exprs();
  int check_udt_set_query();
  int set_having_self_column(const ObRawExpr *real_ref_expr);
  int check_win_func_arg_valid(ObSelectStmt *select_stmt,
                               const ObItemType func_type,
                               common::ObIArray<ObRawExpr *> &arg_exp_arr,
                               common::ObIArray<ObRawExpr *> &partition_exp_arr);
  int check_query_is_recursive_union(const ParseNode &parse_tree, bool &recursive_union, bool &need_swap_child);
  int do_check_basic_table_in_cte_recursive_union(const ParseNode &parse_tree, bool &recursive_union);
  int do_check_node_in_cte_recursive_union(const ParseNode* node, bool &recursive_union);
  int resolve_fetch_clause(const ParseNode *node);
  int resolve_check_option_clause(const ParseNode *node);
private:
  int parameterize_fields_name(const ParseNode *project_node,
                               const ObString &org_alias_name,
                               ObString &paramed_name,
                               common::ObIArray<int64_t> &questions_pos,
                               common::ObIArray<int64_t> &params_idx,
                               common::ObBitSet<> &neg_param_idx,
                               bool &is_cp_str_value);
  int recursive_parameterize_field(const ParseNode *root_node,
                                   const ObString &org_expr_name,
                                   const int32_t buf_len,
                                   const int64_t expr_start_pos,
                                   char *str_buf,
                                   int64_t &str_pos,
                                   int64_t &sql_pos,
                                   common::ObIArray<int64_t> &questions_pos,
                                   common::ObIArray<int64_t> &params_idx,
                                   common::ObBitSet<> &neg_param_idx);
  int resolve_paramed_const(const ParseNode &const_node,
                            const common::ObIArray<ObPCParam *> &raw_params,
                            const ObString &org_expr_name,
                            const int32_t buf_len,
                            const int64_t expr_start_pos,
                            char *buf,
                            int64_t &buf_pos,
                            int64_t &expr_pos,
                            common::ObIArray<int64_t> &questions_pos,
                            common::ObIArray<int64_t> &params_idx,
                            common::ObBitSet<> &neg_param_idx);
  int resolve_not_paramed_const(const ParseNode &const_node,
                                const common::ObIArray<ObPCParam *> &raw_params,
                                const ObString &org_expr_name,
                                const int32_t buf_len,
                                const int64_t expr_start_pos,
                                char *buf,
                                int64_t &buf_pos,
                                int64_t &expr_pos);
  int get_refindex_from_named_windows(const ParseNode *ref_name_node,
                                      const ParseNode *node,
                                      int64_t& ref_idx);
  int check_ntile_compatiable_with_mysql(ObWinFunRawExpr *win_expr);
  int check_ntile_validity(const ObRawExpr *expr,
                           bool &is_valid);
  int check_ntile_validity(const ObSelectStmt *expr,
                           bool &is_valid);
  int check_orderby_type_validity(ObWinFunRawExpr *win_expr);
  int check_duplicated_name_window(ObString &name,
                                   const ObIArray<ObString> &resolved_name_list);
  int mock_to_named_windows(ObString &name,
                            ParseNode *win_node);
  int can_find_group_column(ObRawExpr *&col_expr,
                            const common::ObIArray<ObRawExpr*> &exprs,
                            bool &can_find,
                            ObStmtCompareContext *check_context = NULL);
  int can_find_group_column(ObRawExpr *&col_expr,
                            const common::ObIArray<ObGroupingSetsItem> &grouping_sets_items,
                            bool &can_find,
                            ObStmtCompareContext *check_context = NULL);
  int can_find_group_column(ObRawExpr *&col_expr,
                            const ObIArray<ObRollupItem> &rollup_items,
                            bool &can_find,
                            ObStmtCompareContext *check_context = NULL);
  int can_find_group_column(ObRawExpr *&col_expr,
                            const ObIArray<ObCubeItem> &cube_items,
                            bool &can_find,
                            ObStmtCompareContext *check_context = NULL);
  int check_subquery_return_one_column(const ObRawExpr &expr, bool is_exists_param = false);

  int mark_nested_aggr_if_required(const ObIArray<ObAggFunRawExpr*> &aggr_exprs);

  int check_rollup_items_valid(const common::ObIArray<ObRollupItem> &rollup_items);
  int check_cube_items_valid(const common::ObIArray<ObCubeItem> &cube_items);
  int recursive_check_grouping_columns(ObSelectStmt *stmt, ObRawExpr *expr);

  int add_name_for_anonymous_view();
  int add_name_for_anonymous_view_recursive(TableItem *table_item);

  int is_need_check_col_dup(const ObRawExpr *expr, bool &need_check);

  int resolve_shared_order_item(OrderItem &order_item, ObSelectStmt *select_stmt);
protected:
  // data members
  /*these member is only for with clause*/
  //由于search以及cycle解析的特殊性，需要解析儿子stmt中定义的CTE_TABLE类型
  TableItem* current_recursive_cte_table_item_;
  ObSelectStmt* current_cte_involed_stmt_;

  bool has_calc_found_rows_;
  bool has_top_limit_;
  //用于标识当前的query是否是set query(UNION/INTERSECT/EXCEPT)的左右支
  bool in_set_query_;
  //用于表示当前的query是否是sub query，用于sequence合法性检查等
  bool is_sub_stmt_;
  // query is subquery in exists
  bool in_exists_subquery_;
  ObStandardGroupChecker standard_group_checker_;
  const TransposeItem *transpose_item_;
  bool is_left_child_;
  uint64_t auto_name_id_;
  // denote having exists ref columns that belongs to current stmt
  bool having_has_self_column_;
  bool has_grouping_;
  //用于标识当前的query是否有group by子句
  bool has_group_by_clause_;
  bool has_nested_aggr_;
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObSelectResolver);
};



} // end namespace sql
} // end namespace oceanbase

#endif /* _OB_SELECT_RESOLVER_H */
