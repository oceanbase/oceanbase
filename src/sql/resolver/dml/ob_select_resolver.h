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
#include "common/ob_smart_call.h"

namespace oceanbase {
namespace sql {
// Provide an interface class for parsing various sub-queries, including where sub-queries
// union sub-queries, view sub-queries, generated table sub-queries
// The purpose of this is to shield some details of subquery parsing, without passing the entire
// dml resolver structure to the subquery parsing function
// Avoid seeing too many attributes during the call, reducing coupling
class ObChildStmtResolver {
public:
  ObChildStmtResolver() : parent_aggr_level_(-1)
  {}
  virtual int resolve_child_stmt(const ParseNode& parse_tree) = 0;
  virtual int add_cte_table_item(TableItem* table_item) = 0;
  virtual ObSelectStmt* get_child_stmt() = 0;
  void set_parent_aggr_level(int64_t parent_aggr_level)
  {
    parent_aggr_level_ = parent_aggr_level;
  }

protected:
  // When this level is not -1, it means that the child stmt is a subquery in the aggregate function
  // Note that at this time, the aggregate function has not been pushed up, this level is not the
  // final level of the aggregate function
  int64_t parent_aggr_level_;
};

class ObSelectResolver : public ObDMLResolver, public ObChildStmtResolver {
  class ObCteResolverCtx {
    friend class ObSelectResolver;

  public:
    ObCteResolverCtx()
        : left_select_stmt_(NULL),
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
    {}
    virtual ~ObCteResolverCtx()
    {}
    inline void set_is_with_resolver(bool is_with_resolver)
    {
      is_with_clause_resolver_ = is_with_resolver;
    }
    inline void set_current_cte_table_name(const ObString& table_name)
    {
      current_cte_table_name_ = table_name;
    }
    inline bool is_with_resolver() const
    {
      return is_with_clause_resolver_;
    }
    inline void set_recursive(bool recursive)
    {
      is_recursive_cte_ = recursive;
    }
    inline void set_in_subquery()
    {
      is_cte_subquery_ = true;
    }
    inline bool is_in_subquery()
    {
      return cte_resolve_level_ >= 2;
    }
    inline void reset_subquery_level()
    {
      cte_resolve_level_ = 0;
    }
    inline bool is_recursive() const
    {
      return is_recursive_cte_;
    }
    inline void set_left_select_stmt(ObSelectStmt* left_stmt)
    {
      left_select_stmt_ = left_stmt;
    }
    inline void set_left_parse_node(const ParseNode* node)
    {
      left_select_stmt_parse_node_ = node;
    }
    inline void set_set_all(bool all)
    {
      is_set_all_ = all;
    }
    inline bool invalid_recursive_union()
    {
      return (nullptr != left_select_stmt_ && !is_set_all_);
    }
    inline bool more_than_two_branch()
    {
      return cte_branch_count_ >= 2;
    }
    inline void reset_branch_count()
    {
      cte_branch_count_ = 0;
    }
    inline void set_recursive_left_branch()
    {
      is_set_left_resolver_ = true;
      cte_branch_count_++;
    }
    inline void set_recursive_right_branch(ObSelectStmt* left_stmt, const ParseNode* node, bool all)
    {
      is_set_left_resolver_ = false;
      cte_branch_count_++;
      left_select_stmt_ = left_stmt;
      left_select_stmt_parse_node_ = node;
      is_set_all_ = all;
    }
    int assign(ObCteResolverCtx& cte_ctx)
    {
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
    TO_STRING_KV(K_(is_with_clause_resolver), K_(current_cte_table_name), K_(is_recursive_cte), K_(is_cte_subquery),
        K_(cte_resolve_level), K_(cte_col_names));

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
  explicit ObSelectResolver(ObResolverParams& params);
  virtual ~ObSelectResolver();

  virtual int resolve(const ParseNode& parse_tree);
  ObSelectStmt* get_select_stmt();
  void set_calc_found_rows(bool found_rows)
  {
    has_calc_found_rows_ = found_rows;
  }
  void set_has_top_limit(bool has_top_limit)
  {
    has_top_limit_ = has_top_limit;
  }
  virtual int resolve_child_stmt(const ParseNode& parse_tree)
  {
    return SMART_CALL(resolve(parse_tree));
  }
  virtual ObSelectStmt* get_child_stmt()
  {
    return get_select_stmt();
  }
  virtual bool is_select_resolver() const
  {
    return true;
  }
  inline bool can_produce_aggr() const
  {
    return T_FIELD_LIST_SCOPE == current_scope_ || T_HAVING_SCOPE == current_scope_ || T_ORDER_SCOPE == current_scope_;
  }
  int add_aggr_expr(ObAggFunRawExpr*& final_aggr_expr);
  int add_unsettled_column(ObRawExpr* column_expr);
  void set_in_set_query(bool in_set_query)
  {
    in_set_query_ = in_set_query;
  }
  bool is_in_set_query() const
  {
    return in_set_query_;
  }
  void set_in_subquery(bool in_subquery)
  {
    in_subquery_ = in_subquery;
  }
  bool is_in_subquery() const
  {
    return in_subquery_;
  }
  void set_in_exists_subquery(bool in_exists_subquery)
  {
    in_exists_subquery_ = in_exists_subquery;
  }
  bool is_in_exists_subquery() const
  {
    return in_exists_subquery_;
  }
  virtual int resolve_column_ref_expr(const ObQualifiedName& q_name, ObRawExpr*& real_ref_expr);
  void set_transpose_item(const TransposeItem* transpose_item)
  {
    transpose_item_ = transpose_item;
  }
  // function members
  TO_STRING_KV(K_(has_calc_found_rows), K_(has_top_limit), K_(in_set_query), K_(in_subquery));

protected:
  int resolve_set_query(const ParseNode& parse_node);
  int do_resolve_set_query_in_cte(const ParseNode& parse_tree);
  int do_resolve_set_query(const ParseNode& parse_tree);
  int do_resolve_set_query(
      const ParseNode& parse_tree, common::ObIArray<ObSelectStmt*>& child_stmt, const bool is_left_child = false);
  virtual int do_resolve_set_query(
      const ParseNode& parse_tree, ObSelectStmt*& child_stmt, const bool is_left_child = false);
  int check_cte_set_types(ObSelectStmt& left_stmt, ObSelectStmt& right_stmt);
  int set_cte_ctx(ObCteResolverCtx& cte_ctx, bool copy_col_name = true, bool in_subquery = false);
  int set_stmt_set_type(ObSelectStmt* select_stmt, ParseNode* set_node);
  int is_set_type_same(const ObSelectStmt* select_stmt, ParseNode* set_node, bool& is_type_same);
  int check_recursive_cte_limited();
  int check_pseudo_column_name_legal(const ObString& name);
  int resolve_normal_query(const ParseNode& parse_node);
  virtual int resolve_generate_table(
      const ParseNode& table_node, const ObString& alias_name, TableItem*& tbl_item) override;
  int create_joined_table_item(JoinedTable*& joined_table);
  virtual int check_special_join_table(const TableItem& join_table, bool is_left_child, ObItemType join_type) override;
  virtual int resolve_basic_table(const ParseNode& parse_tree, TableItem*& table_item) override;
  int resolve_with_clause_subquery(const ParseNode& parse_tree, TableItem*& table_item);
  int resolve_cte_pseudo_column(const ParseNode* search_node, const ParseNode* cycle_node, const TableItem* table_item,
      ObString& search_pseudo_column_name, ObString& cycle_pseudo_column_name);
  int init_cte_resolver(ObSelectResolver& select_resolver, const ParseNode* opt_col_node, ObString& table_name);
  int get_current_recursive_cte_table(ObSelectStmt* ref_stmt);
  int resolve_search_clause(const ParseNode& parse_tree, const TableItem* cte_table_item, ObString& name);
  int resolve_search_item(const ParseNode* sort_list, ObSelectStmt* r_union_stmt);
  int resolve_search_pseudo(const ParseNode* search_set_clause, ObSelectStmt* r_union_stmt, ObString& name);
  int resolve_cycle_clause(const ParseNode& parse_tree, const TableItem* cte_table_item, ObString& name);
  int resolve_cycle_item(const ParseNode* alias_list, ObSelectStmt* r_union_stmt);
  int resolve_cycle_pseudo(const ParseNode* cycle_set_clause, ObSelectStmt* r_union_stmt, const ParseNode* cycle_value,
      const ParseNode* cycle_default_value, ObString& cycle_pseudo_column_name);
  int resolve_with_clause_opt_alias_colnames(const ParseNode* parse_tree, TableItem*& table_item);
  int add_fake_schema(ObSelectStmt* left_stmt);
  int set_parent_cte();
  int generate_fake_column_expr(const share::schema::ObColumnSchemaV2* column_schema, ObSelectStmt* left_stmt,
      ObColumnRefRawExpr*& fake_col_expr);
  /**
   * @bref Parse names in advance for column detection when cte recursive references
   */
  int get_opt_alias_colnames_for_recursive_cte(ObIArray<ObString>& columns, const ParseNode* parse_tree);
  int resolve_cte_table(const ParseNode& parse_tree, const TableItem* CTE_table_item, TableItem*& table_item);
  int resolve_recursive_cte_table(const ParseNode& parse_tree, TableItem*& table_item);
  int add_parent_cte_table_to_children(ObChildStmtResolver& child_resolver);
  int add_cte_table_item(TableItem* table_item);
  int resolve_with_clause(const ParseNode* node, bool same_level = false);
  int resolve_from_clause(const ParseNode* node);
  int resolve_field_list(const ParseNode& node);
  int resolve_star(const ParseNode* node);
  int resolve_group_clause(const ParseNode* node);
  int resolve_groupby_node(const ParseNode* group_node, const ParseNode* group_sort_node,
      common::ObIArray<ObRawExpr*>& groupby_exprs, common::ObIArray<ObRawExpr*>& rollup_exprs,
      common::ObIArray<OrderItem>& order_items, bool& has_explicit_dir, bool is_groupby_expr, int group_expr_level);
  int resolve_group_by_sql_expr(const ParseNode* group_node, const ParseNode* group_sort_node,
      common::ObIArray<ObRawExpr*>& groupby_exprs, common::ObIArray<ObRawExpr*>& rollup_exprs,
      common::ObIArray<OrderItem>& order_items, ObSelectStmt* select_stmt, bool& has_explicit_dir,
      bool is_groupby_expr);
  int resolve_group_by_list(const ParseNode* node, common::ObIArray<ObRawExpr*>& groupby_exprs,
      common::ObIArray<ObRawExpr*>& rollup_exprs, common::ObIArray<OrderItem>& order_items, bool& has_explicit_dir);
  int resolve_rollup_list(const ParseNode* node, ObMultiRollupItem& rollup_item, bool& can_conv_multi_rollup);
  int resolve_grouping_sets_list(const ParseNode* node, ObGroupingSetsItem& grouping_sets_item);
  int resolve_with_rollup_clause(const ParseNode* node, common::ObIArray<ObRawExpr*>& groupby_exprs,
      common::ObIArray<ObRawExpr*>& rollup_exprs, common::ObIArray<OrderItem>& order_items, bool& has_explicit_dir);
  int resolve_for_update_clause(const ParseNode* node);
  int resolve_for_update_clause_oracle(const ParseNode& node);
  int set_for_update_mysql(ObSelectStmt& stmt, const int64_t wait_us);
  int set_for_update_oracle(ObSelectStmt& stmt, const int64_t wait_us, ObColumnRefRawExpr* col = NULL);
  int resolve_for_update_clause_mysql(const ParseNode& node);
  int resolve_all_fake_cte_table_columns(const TableItem& table_item, common::ObIArray<ColumnItem>* column_items);

  int check_cycle_clause(const ParseNode& node);
  int check_search_clause(const ParseNode& node);
  int check_search_cycle_set_column(const ParseNode& search_node, const ParseNode& cycle_node);
  int check_cycle_values(const ParseNode& cycle_node);
  int check_cte_pseudo(const ParseNode* search_node, const ParseNode* cycle_node);
  int check_unsupported_operation_in_recursive_branch();
  int check_recursive_cte_usage(const ObSelectStmt& select_stmt);
  int gen_unpivot_target_column(const int64_t table_count, ObSelectStmt& select_stmt, TableItem& table_item);

  // resolve select into
  int resolve_into_clause(const ParseNode* node);
  int resolve_into_const_node(const ParseNode* node, ObObj& obj);
  int resolve_into_filed_node(const ParseNode* node, ObSelectIntoItem& into_item);
  int resolve_into_line_node(const ParseNode* node, ObSelectIntoItem& into_item);
  int resolve_into_variable_node(const ParseNode* node, ObSelectIntoItem& into_item);

  // resolve_star related functions
  int resolve_star_for_table_groups();
  int find_joined_table_group_for_table(const uint64_t table_id, int64_t& jt_idx);
  int find_select_columns_for_join_group(const int64_t jt_idx, common::ObArray<SelectItem>* sorted_select_items);
  int find_select_columns_for_joined_table_recursive(
      const JoinedTable* jt, common::ObIArray<SelectItem>* sorted_select_items);
  int coalesce_select_columns_for_joined_table(const common::ObIArray<SelectItem>* left,
      const common::ObIArray<SelectItem>* right, const ObJoinType type,
      const common::ObIArray<common::ObString>& using_columns, common::ObIArray<SelectItem>* coalesced_columns);
  int expand_target_list(const TableItem& table_item, common::ObIArray<SelectItem>& target_list);
  int recursive_find_coalesce_expr(
      const JoinedTable*& joined_table, const common::ObString& cname, ObRawExpr*& coalesce_expr);
  int resolve_having_clause(const ParseNode* node);
  int replace_having_expr_when_nested_aggr(ObSelectStmt* select_stmt, ObAggFunRawExpr* aggr_expr);
  int resolve_named_windows_clause(const ParseNode* node);
  int check_nested_aggr_in_having(ObRawExpr* expr);
  int check_sw_cby_node(const ParseNode* node_1, const ParseNode* node_2);
  int resolve_start_with_clause(const ParseNode* node_1, const ParseNode* node_2);
  int resolve_connect_by_clause(const ParseNode* node_1, const ParseNode* node_2);
  int check_correlated_column_ref(const ObSelectStmt& select_stmt, ObRawExpr* expr, bool& correalted_query);
  virtual int resolve_order_item(const ParseNode& sort_node, OrderItem& order_item);
  virtual int resolve_order_item_by_pos(int64_t pos, OrderItem& order_item, ObSelectStmt* select_stmt);
  virtual int resolve_literal_order_item(
      const ParseNode& sort_node, ObRawExpr* expr, OrderItem& order_item, ObSelectStmt* select_stmt);
  virtual int resolve_aggr_exprs(
      ObRawExpr*& expr, common::ObIArray<ObAggFunRawExpr*>& aggr_exprs, const bool need_analyze = true);
  virtual int resolve_win_func_exprs(ObRawExpr*& expr, common::ObIArray<ObWinFunRawExpr*>& win_exprs);
  int resolve_column_ref_in_all_namespace(const ObQualifiedName& q_name, ObRawExpr*& real_ref_expr);
  /**
   * resolve column real ref expr, search order: alias name first, followed by table column
   * @param q_name, column name
   * @param real_ref_expr, column real ref expr
   */
  int resolve_column_ref_alias_first(const ObQualifiedName& q_name, ObRawExpr*& real_ref_expr);
  int resolve_column_ref_table_first(const ObQualifiedName& q_name, ObRawExpr*& real_ref_expr);
  int resolve_column_ref_for_having(const ObQualifiedName& q_name, ObRawExpr*& real_ref_expr);
  int resolve_column_ref_for_search(const ObQualifiedName& q_name, ObRawExpr*& real_ref_expr);
  int resolve_table_column_ref(const ObQualifiedName& q_name, ObRawExpr*& real_ref_expr);
  int check_table_column_namespace(const ObQualifiedName& q_name, const TableItem*& table_item);
  int resolve_alias_column_ref(const ObQualifiedName& q_name, ObRawExpr*& real_ref_expr);
  int resolve_column_ref_in_group_by(const ObQualifiedName& q_name, ObRawExpr*& real_ref_expr);
  int resolve_all_function_table_columns(const TableItem& table_item, ObIArray<ColumnItem>* column_items);
  int resolve_all_generated_table_columns(const TableItem& table_item, common::ObIArray<ColumnItem>* column_items);
  virtual int set_select_item(SelectItem& select_item, bool is_auto_gen);
  int resolve_query_options(const ParseNode* node);
  virtual int resolve_subquery_info(const common::ObIArray<ObSubQueryInfo>& subquery_info);
  virtual int resolve_column_ref_for_subquery(const ObQualifiedName& q_name, ObRawExpr*& real_ref_expr);
  inline bool column_need_check_group_by(const ObQualifiedName& q_name) const;
  int check_column_ref_in_group_by_or_field_list(const ObRawExpr* column_ref) const;
  int wrap_alias_column_ref(const ObQualifiedName& q_name, ObRawExpr*& real_ref_expr);
  virtual int check_need_use_sys_tenant(bool& use_sys_tenant) const;
  virtual int check_in_sysview(bool& in_sysview) const override;
  int check_group_by();
  int check_order_by();
  int check_field_list();
  int check_pseudo_columns();
  int check_grouping_columns();
  int check_grouping_columns(ObSelectStmt& stmt, ObAggFunRawExpr& expr);
  int check_window_exprs();
  int check_sequence_exprs();
  int set_having_self_column(const ObRawExpr* real_ref_expr);
  static int check_win_func_arg_valid(ObSelectStmt* select_stmt, const ObItemType func_type,
      common::ObIArray<ObRawExpr*>& arg_exp_arr, common::ObIArray<ObRawExpr*>& partition_exp_arr);
  int identify_anchor_member(
      ObSelectResolver& identify_anchor_resolver, bool& need_swap_child, const ParseNode& parse_tree);
  int resolve_fetch_clause(const ParseNode* node);

private:
  int parameterize_fields_name(const ParseNode* project_node, const ObString& org_alias_name, ObString& paramed_name,
      common::ObIArray<int64_t>& questions_pos, common::ObIArray<int64_t>& params_idx,
      common::ObBitSet<>& neg_param_idx, bool& is_cp_str_value);
  int recursive_parameterize_field(const ParseNode* root_node, const ObString& org_expr_name, const int32_t buf_len,
      const int64_t expr_start_pos, char* str_buf, int64_t& str_pos, int64_t& sql_pos,
      common::ObIArray<int64_t>& questions_pos, common::ObIArray<int64_t>& params_idx,
      common::ObBitSet<>& neg_param_idx);
  int resolve_paramed_const(const ParseNode& const_node, const common::ObIArray<ObPCParam*>& raw_params,
      const ObString& org_expr_name, const int32_t buf_len, const int64_t expr_start_pos, char* buf, int64_t& buf_pos,
      int64_t& expr_pos, common::ObIArray<int64_t>& questions_pos, common::ObIArray<int64_t>& params_idx,
      common::ObBitSet<>& neg_param_idx);
  int resolve_not_paramed_const(const ParseNode& const_node, const common::ObIArray<ObPCParam*>& raw_params,
      const ObString& org_expr_name, const int32_t buf_len, const int64_t expr_start_pos, char* buf, int64_t& buf_pos,
      int64_t& expr_pos);
  int get_refindex_from_named_windows(const ParseNode* ref_name_node, const ParseNode* node, int64_t& ref_idx);
  int check_ntile_compatiable_with_mysql(ObWinFunRawExpr* win_expr);
  int check_duplicated_name_window(ObString& name, const ParseNode* node, int64_t resolved);
  int mock_to_named_windows(ObString& name, ParseNode* win_node);
  int can_find_group_column(ObRawExpr* col_expr, const common::ObIArray<ObRawExpr*>& exprs, bool& can_find);
  int can_find_group_column(
      ObRawExpr* col_expr, const common::ObIArray<ObGroupingSetsItem>& grouping_sets_items, bool& can_find);
  int can_find_group_column(ObRawExpr* col_expr, const ObIArray<ObMultiRollupItem>& multi_rollup_items, bool& can_find);
  int check_subquery_return_one_column(const ObRawExpr& expr, bool is_exists_param = false);

  int check_nested_aggr_valid(const ObIArray<ObAggFunRawExpr*>& aggr_exprs);

  int check_multi_rollup_items_valid(const common::ObIArray<ObMultiRollupItem>& multi_rollup_items);

  int recursive_check_grouping_columns(ObSelectStmt* stmt, ObRawExpr* expr);

protected:
  // data members
  /*these member is only for with clause*/
  ObCteResolverCtx cte_ctx_;
  common::ObSEArray<TableItem*, 4, common::ModulePageAllocator, true> parent_cte_tables_;
  // Due to the particularity of search and cycle analysis, the CTE_TABLE type defined
  // in the sub stmt needs to be parsed
  TableItem* current_recursive_cte_table_item_;
  ObSelectStmt* current_cte_involed_stmt_;

  bool has_calc_found_rows_;
  bool has_top_limit_;
  // Used to identify whether the current query is the left or right of set query (UNION/INTERSECT/EXCEPT)
  bool in_set_query_;
  // Used to indicate whether the current query is a sub query, used for sequence legality checking
  bool in_subquery_;
  // query is subquery in exists
  bool in_exists_subquery_;
  ObStandardGroupChecker standard_group_checker_;
  const TransposeItem* transpose_item_;

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObSelectResolver);
};

}  // end namespace sql
}  // end namespace oceanbase

#endif /* _OB_SELECT_RESOLVER_H */
