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

#ifndef OB_TRANSFORM_PRE_PROCESS_H_
#define OB_TRANSFORM_PRE_PROCESS_H_

#include "sql/rewrite/ob_transform_rule.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/resolver/dml/ob_merge_stmt.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/ob_sql_context.h"

namespace oceanbase
{

namespace sql
{

typedef std::pair<uint64_t, uint64_t> JoinTableIdPair;

class ObTransformPreProcess: public ObTransformRule
{
public:
  explicit ObTransformPreProcess(ObTransformerCtx *ctx)
     : ObTransformRule(ctx, TransMethod::POST_ORDER) { }
  virtual ~ObTransformPreProcess() {}

  virtual int transform_one_stmt(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                 ObDMLStmt *&stmt,
                                 bool &trans_happened) override;

  static int transform_expr(ObRawExprFactory &expr_factory,
                            const ObSQLSessionInfo &session,
                            ObRawExpr *&expr,
                            bool &trans_happened);
private:
  virtual int need_transform(const common::ObIArray<ObParentDMLStmt> &parent_stmts,
                             const int64_t current_level,
                             const ObDMLStmt &stmt,
                             bool &need_trans) override;
// used for transform in expr to or exprs
struct DistinctObjMeta
{
  ObObjType obj_type_;
  ObCollationType coll_type_;
  ObCollationLevel coll_level_;

  DistinctObjMeta(ObObjType obj_type, ObCollationType coll_type, ObCollationLevel coll_level)
    : obj_type_(obj_type), coll_type_(coll_type), coll_level_(coll_level)
  {
    if (!ObDatumFuncs::is_string_type(obj_type_)) {
      coll_type_ = CS_TYPE_MAX;
      coll_level_ = CS_LEVEL_INVALID;
    }
  }
  DistinctObjMeta()
    : obj_type_(common::ObMaxType), coll_type_(common::CS_TYPE_MAX) , coll_level_(CS_LEVEL_INVALID){}

  bool operator==(const DistinctObjMeta &other) const
  {
    bool cs_level_equal = lib::is_oracle_mode() ? true : (coll_level_ == other.coll_level_);
    return obj_type_ == other.obj_type_ && coll_type_ == other.coll_type_ && cs_level_equal;
  }
  TO_STRING_KV(K_(obj_type), K_(coll_type));
};

  /*
   * following functions are used to add all rowkey columns
   */
  int add_all_rowkey_columns_to_stmt(ObDMLStmt *stmt, bool &trans_happened);
  int add_all_rowkey_columns_to_stmt(const ObTableSchema &table_schema,
                                     const TableItem &table_item,
                                     ObRawExprFactory &expr_factory,
                                     ObDMLStmt &stmt,
                                     ObIArray<ColumnItem> &column_items);

  /*
   * following functions are for grouping sets and rollup and cube
   */
  int transform_groupingsets_rollup_cube(ObDMLStmt *&stmt, bool &trans_happened);
  int try_convert_rollup(ObDMLStmt *&stmt, ObSelectStmt *select_stmt);
  int remove_single_item_groupingsets(ObSelectStmt &stmt, bool &trans_happened);
  int create_set_view_stmt(ObSelectStmt *stmt, TableItem *view_table_item);
  int create_cte_for_groupby_items(ObSelectStmt &select_stmt);
  int expand_stmt_groupby_items(ObSelectStmt &select_stmt);

  int is_subquery_correlated(const ObSelectStmt *stmt,
                             bool &is_correlated);

  int is_subquery_correlated(const ObSelectStmt *stmt,
                             hash::ObHashSet<uint64_t> &param_set,
                             bool &is_correlated);

  int add_exec_params(const ObSelectStmt &stmt,
                      hash::ObHashSet<uint64_t> &param_set);

  int has_new_exec_param(const ObRawExpr *expr,
                         const hash::ObHashSet<uint64_t> &param_set,
                         bool &has_new);

  int add_generated_table_as_temp_table(ObTransformerCtx *ctx,
                                        ObDMLStmt *stmt);
  int get_groupby_exprs_list(ObIArray<ObGroupingSetsItem> &grouping_sets_items,
                             ObIArray<ObRollupItem> &rollup_items,
                             ObIArray<ObCubeItem> &cube_items,
                             ObIArray<ObGroupbyExpr> &groupby_exprs_list);
  int expand_grouping_sets_items(common::ObIArray<ObGroupingSetsItem> &grouping_sets_items,
                                 common::ObIArray<ObGroupbyExpr> &grouping_sets_exprs);
  int expand_rollup_items(ObIArray<ObRollupItem> &rollup_items,
                          ObIArray<ObGroupbyExpr> &rollup_list_exprs);
  int expand_cube_items(ObIArray<ObCubeItem> &cube_items,
                        ObIArray<ObRollupItem> &rollup_items_lists);
  int expand_cube_item(ObCubeItem &cube_item,
                       ObIArray<ObRollupItem> &rollup_items_lists);
  int combination_two_rollup_list(ObIArray<ObGroupbyExpr> &rollup_list_exprs1,
                                  ObIArray<ObGroupbyExpr> &rollup_list_exprs2,
                                  ObIArray<ObGroupbyExpr> &rollup_list_exprs);
  int check_pre_aggregate(const ObSelectStmt &select_stmt, bool &can_pre_aggr);
  int transform_grouping_sets_to_rollup_exprs(ObSelectStmt &origin_stmt,
                                              ObIArray<ObGroupbyExpr> &origin_groupby_exprs_list,
                                              ObIArray<ObGroupbyExpr> &groupby_exprs_list,
                                              ObIArray<ObGroupbyExpr> &rollup_exprs_list);
  int simple_expand_grouping_sets_items(ObIArray<ObGroupingSetsItem> &grouping_sets_items,
                                        ObIArray<ObGroupingSetsItem> &simple_grouping_sets_items);
  int create_child_stmts_for_groupby_sets(ObSelectStmt *origin_stmt,
                                          ObIArray<ObSelectStmt*> &child_stmts);
  int get_groupby_and_rollup_exprs_list(ObSelectStmt *origin_stmt,
                                        ObIArray<ObGroupbyExpr> &groupby_exprs_list,
                                        ObIArray<ObGroupbyExpr> &rollup_exprs_list);
  int create_groupby_substmt(const ObIArray<ObRawExpr*> &origin_groupby_exprs,
                             const ObIArray<ObRawExpr*> &origin_rollup_exprs,
                             ObSelectStmt &origin_stmt,
                             ObSelectStmt *&substmt,
                             ObIArray<ObRawExpr*> &groupby_exprs);
  int convert_select_having_in_groupby_stmt(const ObIArray<ObGroupbyExpr> &groupby_exprs_list,
                                            const ObIArray<ObGroupbyExpr> &rollup_exprs_list,
                                            const ObIArray<ObRawExpr*> &groupby_exprs,
                                            const int64_t cur_index,
                                            const ObSelectStmt &origin_stmt,
                                            ObSelectStmt &substmt);
  // calc 'grouping', 'grouping id', 'group id'
  int calc_group_type_aggr_func(const ObIArray<ObRawExpr*> &groupby_exprs,
                                const ObIArray<ObRawExpr*> &rollup_exprs,
                                const ObIArray<ObGroupbyExpr> &groupby_exprs_list,
                                const int64_t cur_index,
                                const int64_t origin_groupby_num,
                                ObAggFunRawExpr *expr,
                                ObRawExpr *&new_expr);
  int calc_grouping_in_grouping_sets(const ObIArray<ObRawExpr*> &groupby_exprs,
                                     const ObIArray<ObRawExpr*> &rollup_exprs,
                                     ObAggFunRawExpr *expr,
                                     ObRawExpr *&new_expr);
  int calc_grouping_id_in_grouping_sets(const ObIArray<ObRawExpr*> &groupby_exprs,
                                        const ObIArray<ObRawExpr*> &rollup_exprs,
                                        ObAggFunRawExpr *expr,
                                        ObRawExpr *&new_expr);
  int calc_group_id_in_grouping_sets(const ObIArray<ObRawExpr*> &groupby_exprs,
                                     const ObIArray<ObRawExpr*> &rollup_exprs,
                                     const ObIArray<ObGroupbyExpr> &groupby_exprs_list,
                                     const int64_t cur_index,
                                     const int64_t origin_groupby_num,
                                     ObAggFunRawExpr *expr,
                                     ObRawExpr *&new_expr);
  /*
   * following functions are for hierarchical query
   */
  int transform_for_hierarchical_query(ObDMLStmt *stmt, bool &trans_happened);

  int try_split_hierarchical_query(ObSelectStmt &stmt, ObSelectStmt *&hierarchical_stmt);

  int check_need_split_hierarchical_query(ObSelectStmt &stmt, bool &need_split);
  /**
   * @brief create_connect_by_view
   * 为层次查询构建一个spj，封装所有与层次查询相关的计算
   * 包括所有的from item、where condition内的join条件
   * 外层引用的connect by相关运算及伪列作为视图投影
   */
  int create_connect_by_view(ObSelectStmt &stmt);

  /**
   * @brief create_and_mock_join_view
   * 将所有的from item及其相关的连接谓词分离成一个spj
   * 并且copy一份构成connect by的右孩子节点
   */
  int create_and_mock_join_view(ObSelectStmt &stmt);

  /**
   * @brirf classify_join_conds
   * 将stmt的where condition分成普通的连接谓词和其他谓词
   * 普通的连接谓词：引用两张表以上的谓词，并且不含有LEVEL、CONNECT_BY_ISLEAF、
   * CONNECT_BY_ISCYCLE、PRIOR、CONNECT_BY_ROOT
   */
  int classify_join_conds(ObSelectStmt &stmt,
                          ObIArray<ObRawExpr *> &normal_join_conds,
                          ObIArray<ObRawExpr *> &other_conds);

  int is_cond_in_one_from_item(ObSelectStmt &stmt, ObRawExpr *expr, bool &in_from_item);

  int extract_connect_by_related_exprs(ObSelectStmt &stmt, ObIArray<ObRawExpr*> &special_exprs);

  int extract_connect_by_related_exprs(ObRawExpr *expr, ObIArray<ObRawExpr*> &special_exprs);

  int get_prior_exprs(ObIArray<ObRawExpr *> &expr, ObIArray<ObRawExpr *> &prior_exprs);
  int get_prior_exprs(ObRawExpr *expr, ObIArray<ObRawExpr *> &prior_exprs);

  /**
   * @brief modify_prior_exprs
   * replace prior exprs into left view columns
   */
  int modify_prior_exprs(ObRawExprFactory &expr_factory,
                         ObSelectStmt &stmt,
                         const ObIArray<ObRawExpr *> &parent_columns,
                         const ObIArray<ObRawExpr *> &left_columns,
                         const ObIArray<ObRawExpr *> &prior_exprs,
                         ObIArray<ObRawExpr *> &convert_exprs,
                         const bool only_columns);

  uint64_t get_real_tid(uint64_t tid, ObSelectStmt &stmt);

	/*
	 * follow functions are used for eliminate having
	 */
	int eliminate_having(ObDMLStmt *stmt, bool &trans_happened);

	/*
	 * following functions are used to replace func is serving tenant
	 */
	int replace_func_is_serving_tenant(ObDMLStmt *&stmt, bool &trans_happened);
	int recursive_replace_func_is_serving_tenant(ObDMLStmt &stmt,
                                               ObRawExpr *&cond_expr,
                                               bool &trans_happened);
	int calc_const_raw_expr_and_get_int(const ObStmt &stmt,
                                      ObRawExpr *const_expr,
                                      ObExecContext &exec_ctx,
                                      ObSQLSessionInfo *session,
                                      ObIAllocator &allocator,
                                      int64_t &result);

  /*
   * following functions are used to transform merge into stmt
   */
  int transform_for_merge_into(ObDMLStmt *&stmt, bool &trans_happened);
  int transform_merge_into_subquery(ObMergeStmt *merge_stmt);
  int create_matched_expr(ObMergeStmt &stmt,
                          ObRawExpr *&matched_flag,
                          ObRawExpr *&not_matched_flag);
  int generate_merge_conditions_subquery(ObRawExpr *matched_expr,
                                         ObIArray<ObRawExpr*> &condition_exprs);
  int get_update_insert_condition_subquery(ObMergeStmt *merge_stmt,
                                           ObRawExpr *matched_expr,
                                           ObRawExpr *not_matched_expr,
                                           bool &update_has_subquery,
                                           bool &insert_has_subquery,
                                           ObIArray<ObRawExpr*> &new_subquery_exprs);
  int get_update_insert_target_subquery(ObMergeStmt *merge_stmt,
                                        ObRawExpr *matched_expr,
                                        ObRawExpr *not_matched_expr,
                                        bool update_has_subquery,
                                        bool insert_has_subquery,
                                        ObIArray<ObRawExpr*> &new_subquery_exprs);
  int get_delete_condition_subquery(ObMergeStmt *merge_stmt,
                                    ObRawExpr *matched_expr,
                                    bool update_has_subquery,
                                    ObIArray<ObRawExpr*> &new_subquery_exprs);

  int transform_insert_only_merge_into(ObDMLStmt* stmt, ObDMLStmt*& out);

  int transform_update_only_merge_into(ObDMLStmt* stmt);

  int check_can_transform_insert_only_merge_into(const ObMergeStmt *merge_stmt, bool &is_valid);

  int create_source_view_for_merge_into(ObMergeStmt *merge_stmt, TableItem *&view_table);
	/*
	 * following functions are used for temporary and se table
	 */
	int transform_for_temporary_table(ObDMLStmt *&stmt, bool &trans_happened);
	int add_filter_for_temporary_table(ObDMLStmt &stmt,
	                                   const TableItem &table_item,
                                     bool is_trans_scope_temp_table);
#ifdef OB_BUILD_LABEL_SECURITY
	int transform_for_label_se_table(ObDMLStmt *stmt, bool &trans_happened);
  int add_filter_for_label_se_table(ObDMLStmt &stmt,
                                    const TableItem &table_item,
                                    const common::ObString &policy_name,
                                    const share::schema::ObColumnSchemaV2 &column_schema);
#endif
	int collect_all_tableitem(ObDMLStmt *stmt,
                            TableItem *table_item,
                            common::ObArray<TableItem*> &table_item_list);
  /*
   * following functions are used for row level security
   */
  int transform_for_rls_table(ObDMLStmt *stmt, bool &trans_happened);
  int check_exempt_rls_policy(bool &exempt_rls_policy);
  int check_need_transform_column_level(ObDMLStmt &stmt,
                                        const TableItem &table_item,
                                        const share::schema::ObRlsPolicySchema &policy_schema,
                                        bool &need_trans);
  int transform_for_single_rls_policy(ObDMLStmt &stmt,
                                      TableItem &table_item,
                                      const share::schema::ObRlsPolicySchema &policy_schema,
                                      bool &trans_happened);
  int calc_policy_function(ObDMLStmt &stmt,
                           const TableItem &table_item,
                           const share::schema::ObRlsPolicySchema &policy_schema,
                           ObRawExpr *&predicate_expr,
                           common::ObString &predicate_str);
  int build_policy_predicate_expr(const common::ObString &predicate_str,
                                  const TableItem &table_item,
                                  common::ObIArray<ObQualifiedName> &columns,
                                  ObRawExpr *&expr);
  int add_rls_policy_constraint(const ObRawExpr *expr, const common::ObString &predicate_str);
  int build_rls_filter_expr(ObDMLStmt &stmt,
                            const TableItem &table_item,
                            const common::ObIArray<ObQualifiedName> &columns,
                            ObRawExpr *predicate_expr,
                            ObRawExpr *&expr);
  int build_rls_constraint_expr(const ObDmlTableInfo &table_info,
                                const ObIArray<ObQualifiedName> &columns,
                                ObRawExpr *predicate_expr,
                                ObRawExpr *&expr);
  int add_filter_for_rls_select(ObDMLStmt &stmt,
                                TableItem &table_item,
                                const common::ObIArray<ObQualifiedName> &columns,
                                ObRawExpr *predicate_expr);
  int add_filter_for_rls_merge(ObDMLStmt &stmt,
                               const TableItem &table_item,
                               const common::ObIArray<ObQualifiedName> &columns,
                               ObRawExpr *predicate_expr);
  int add_filter_for_rls(ObDMLStmt &stmt,
                         const TableItem &table_item,
                         const common::ObIArray<ObQualifiedName> &columns,
                         ObRawExpr *predicate_expr);
  int add_constraint_for_rls(ObDMLStmt &stmt,
                             const TableItem &table_item,
                             const common::ObIArray<ObQualifiedName> &columns,
                             ObRawExpr *predicate_expr);
  int add_constraint_for_rls_insert(ObDMLStmt &stmt,
                                   const TableItem &table_item,
                                   const common::ObIArray<ObQualifiedName> &columns,
                                   ObRawExpr *predicate_expr);
  int add_constraint_for_rls_update(ObDMLStmt &stmt,
                                    const TableItem &table_item,
                                    const common::ObIArray<ObQualifiedName> &columns,
                                    ObRawExpr *predicate_expr);
  int add_constraint_for_rls_merge(ObDMLStmt &stmt,
                                   const TableItem &table_item,
                                   const common::ObIArray<ObQualifiedName> &columns,
                                   ObRawExpr *predicate_expr);
  int replace_expr_for_rls(ObDMLStmt &stmt,
                           const TableItem &table_item,
                           const share::schema::ObRlsPolicySchema &policy_schema,
                           const common::ObIArray<ObQualifiedName> &columns,
                           ObRawExpr *predicate_expr);

  int transform_exprs(ObDMLStmt *stmt, bool &trans_happened);
  int transform_for_nested_aggregate(ObDMLStmt *&stmt, bool &trans_happened);
  int generate_child_level_aggr_stmt(ObSelectStmt *stmt, ObSelectStmt *&sub_stmt);
  int get_first_level_output_exprs(ObSelectStmt *sub_stmt,
                                   common::ObIArray<ObRawExpr*>& inner_aggr_exprs);
  int generate_parent_level_aggr_stmt(ObSelectStmt *&stmt, ObSelectStmt *sub_stmt);
  int remove_nested_aggr_exprs(ObSelectStmt *stmt);
  int construct_column_items_from_exprs(const ObIArray<ObRawExpr*> &column_exprs,
                                        ObIArray<ColumnItem> &column_items);
  /*
   * following functions are used to transform in_expr to or_expr
   */
  static int transform_in_or_notin_expr_with_row(ObRawExprFactory &expr_factory,
                                                 const ObSQLSessionInfo &session,
                                                 const bool is_in_expr,
                                                 ObRawExpr *&in_expr,
                                                 bool &trans_happened);

  static int transform_in_or_notin_expr_without_row(ObRawExprFactory &expr_factory,
                                                 const ObSQLSessionInfo &session,
                                                 const bool is_in_expr,
                                                 ObRawExpr *&in_epxr,
                                                 bool &trans_happened);

  static int create_partial_expr(ObRawExprFactory &expr_factory,
                                 ObRawExpr *left_expr,
                                 ObIArray<ObRawExpr*> &same_type_exprs,
                                 const bool is_in_expr,
                                 ObIArray<ObRawExpr*> &transed_in_exprs);

  /*
   * following functions are used to transform arg_case_expr to case_expr
   */
  static int transform_arg_case_recursively(ObRawExprFactory &expr_factory,
                                                const ObSQLSessionInfo &session,
                                                ObRawExpr *&expr,
                                                bool &trans_happened);
  static int transform_arg_case_expr(ObRawExprFactory &expr_factory,
                                     const ObSQLSessionInfo &session,
                                     ObRawExpr *&expr,
                                     bool &trans_happened);
  static int create_equal_expr_for_case_expr(ObRawExprFactory &expr_factory,
                                             const ObSQLSessionInfo &session,
                                             ObRawExpr *arg_expr,
                                             ObRawExpr *when_expr,
                                             const ObExprResType &case_res_type,
                                             ObOpRawExpr *&equal_expr);
  static int add_row_type_to_array_no_dup(common::ObIArray<ObSEArray<DistinctObjMeta, 4>> &row_type_array,
                                          const ObSEArray<DistinctObjMeta, 4> &row_type);

  static bool is_same_row_type(const common::ObIArray<DistinctObjMeta> &left,
                               const common::ObIArray<DistinctObjMeta> &right);

  static int get_final_transed_or_and_expr(
      ObRawExprFactory &expr_factory,
      const ObSQLSessionInfo &session,
      const bool is_in_expr,
      common::ObIArray<ObRawExpr *> &transed_in_exprs,
      ObRawExpr *&final_or_expr);

  static int check_and_transform_in_or_notin(ObRawExprFactory &expr_factory,
                                             const ObSQLSessionInfo &session,
                                             ObRawExpr *&in_expr,
                                             bool &trans_happened);
  static int replace_in_or_notin_recursively(ObRawExprFactory &expr_factory,
                                             const ObSQLSessionInfo &session,
                                             ObRawExpr *&root_expr,
                                             bool &trans_happened);
  static ObItemType reverse_cmp_type_of_align_date4cmp(const ObItemType &cmp_type);
  static int replace_cast_expr_align_date4cmp(ObRawExprFactory &expr_factory,
                                              const ObItemType &cmp_type,
                                              ObRawExpr *&expr);
  static int check_and_transform_align_date4cmp(ObRawExprFactory &expr_factory,
                                                ObRawExpr *&in_expr,
                                                const ObItemType &cmp_type);
  static int replace_align_date4cmp_recursively(ObRawExprFactory &expr_factory,
                                                ObRawExpr *&root_expr);
  int transformer_aggr_expr(ObDMLStmt *stmt, bool &trans_happened);
  int transform_rownum_as_limit_offset(const ObIArray<ObParentDMLStmt> &parent_stmts,
                                       ObDMLStmt *&stmt,
                                       bool &trans_happened);
  int transform_common_rownum_as_limit(ObDMLStmt *&stmt, bool &trans_happened);
  int try_transform_common_rownum_as_limit_or_false(ObDMLStmt *stmt, ObRawExpr *&limit_expr, bool& is_valid);
  int transform_generated_rownum_as_limit(const ObIArray<ObParentDMLStmt> &parent_stmts,
                                          ObDMLStmt *stmt,
                                          bool &trans_happened);
  int try_transform_generated_rownum_as_limit_offset(ObDMLStmt *upper_stmt,
                                                     ObSelectStmt *select_stmt,
                                                     ObRawExpr *&limit_expr,
                                                     ObRawExpr *&offset_expr);
  int transform_generated_rownum_eq_cond(ObRawExpr *eq_value,
                                         ObRawExpr *&limit_expr,
                                         ObRawExpr *&offset_expr);

  int replace_group_id_in_stmt(ObSelectStmt *stmt);
  int replace_group_id_in_expr_recursive(ObRawExpr *&expr);
  int transform_udt_columns(const common::ObIArray<ObParentDMLStmt> &parent_stmts, ObDMLStmt *stmt, bool &trans_happened);
  int transform_udt_column_conv_function(ObDmlTableInfo &table_info,
                                         ObIArray<ObRawExpr*> &column_conv_exprs,
                                         ObColumnRefRawExpr &udt_col,
                                         ObColumnRefRawExpr &hidd_col);
  int transform_udt_column_value_expr_inner(ObDMLStmt *stmt, ObDmlTableInfo &table_info, ObRawExpr *&old_expr, ObRawExpr *hidd_expr = NULL);
  int transform_xml_binary(ObRawExpr *hidden_blob_expr, ObRawExpr *&new_expr);
  int transform_udt_column_value_expr(ObDMLStmt *stmt, ObDmlTableInfo &table_info, ObRawExpr *old_expr, ObRawExpr *&new_expr, ObRawExpr *hidd_expr = NULL);
  int transform_udt_column_conv_param_expr(ObDmlTableInfo &table_info, ObRawExpr *old_expr, ObRawExpr *&new_expr);
  int replace_udt_assignment_exprs(ObDMLStmt *stmt, ObDmlTableInfo &table_info, ObIArray<ObAssignment> &assignments, bool &trans_happened);
  int set_hidd_col_not_null_attr(const ObColumnRefRawExpr &udt_col, ObIArray<ObColumnRefRawExpr *> &column_exprs);
  int check_skip_child_select_view(const ObIArray<ObParentDMLStmt> &parent_stmts, ObDMLStmt *stmt, bool &skip_for_view_table);
  int transform_query_udt_columns_exprs(const ObIArray<ObParentDMLStmt> &parent_stmts, ObDMLStmt *stmt, bool &trans_happened);
  int transform_udt_columns_constraint_exprs(ObDMLStmt *stmt, bool &trans_happened);
  int get_update_generated_udt_in_parent_stmt(const ObIArray<ObParentDMLStmt> &parent_stmts, const ObDMLStmt *stmt,
                                              ObIArray<ObColumnRefRawExpr*> &col_exprs);
  int get_dml_view_col_exprs(const ObDMLStmt *stmt, ObIArray<ObColumnRefRawExpr*> &assign_col_exprs);

   /*
   * following functions are used for transform rowid in subquery
   */
  int transformer_rowid_expr(ObDMLStmt *stmt, bool &trans_happened);

  int do_transform_rowid_expr(ObDMLStmt &stmt,
                              ObColumnRefRawExpr *empty_rowid_col_expr,
                              ObRawExpr *&new_rowid_expr);
  int recursive_generate_rowid_select_item(ObSelectStmt *select_stmt,
                                           ObRawExpr *&rowid_expr);
  int check_can_gen_rowid_on_this_table(ObSelectStmt *select_stmt,
                                        TableItem *this_item,
                                        bool &can_gen_rowid);
  int build_rowid_expr(ObSelectStmt *stmt,
                       TableItem *table_item,
                       ObSysFunRawExpr *&rowid_expr);
  int create_rowid_item_for_stmt(ObDMLStmt *select_stmt,
                                 TableItem *table_item,
                                 ObRawExpr *&rowid_expr);
  int add_rowid_constraint(ObDMLStmt &stmt);

  int check_stmt_contain_param_expr(ObDMLStmt *stmt, bool &contain);

  int check_stmt_can_batch(ObDMLStmt *batch_stmt, bool &can_batch);

  int check_contain_param_expr(ObDMLStmt *stmt, TableItem *table_item, bool &contain_param);

  int transform_for_upd_del_batch_stmt(ObDMLStmt *batch_stmt,
                                       ObSelectStmt* inner_view_stmt,
                                       bool &trans_happened);
  int create_inner_view_stmt(ObDMLStmt *batch_stmt, ObSelectStmt*& inner_view_stmt);

  int transform_for_ins_batch_stmt(ObDMLStmt *batch_stmt, bool &trans_happened);
  int transform_for_batch_stmt(ObDMLStmt *batch_stmt, bool &trans_happened);

  int check_insert_can_batch(ObInsertStmt *insert_stmt, bool &can_batch);

  int formalize_batch_stmt(ObDMLStmt *batch_stmt,
                          ObSelectStmt* inner_view_stmt,
                          const ObIArray<ObRawExpr *> &other_exprs,
                          bool &trans_happened);

  int mock_select_list_for_upd_del(ObDMLStmt &batch_stmt, ObSelectStmt &inner_view);

  int mock_select_list_for_ins_values(ObDMLStmt &batch_stmt,
                                      ObSelectStmt &inner_view,
                                      bool &trans_happened);

  int mock_select_list_for_ins_select(ObDMLStmt &batch_stmt,
                                      ObSelectStmt &inner_view,
                                      bool &trans_happened);

  int create_stmt_id_expr(ObPseudoColumnRawExpr *&stmt_id_expr);

  int create_params_expr(ObPseudoColumnRawExpr *&pseudo_param_expr,
                         ObRawExpr *origin_param_expr,
                         int64_t name_id);
  int create_params_exprs(ObDMLStmt &batch_stmt,
                         ObIArray<ObRawExpr*> &params_exprs);

  int mock_select_list_for_inner_view(ObDMLStmt &batch_stmt, ObSelectStmt &inner_view);

  int transform_outerjoin_exprs(ObDMLStmt *stmt, bool &trans_happened);

  int remove_shared_expr(ObDMLStmt *stmt,
                         JoinedTable *joined_table,
                         hash::ObHashSet<uint64_t> &expr_set,
                         bool is_nullside);

  int do_remove_shared_expr(hash::ObHashSet<uint64_t> &expr_set,
                            ObIArray<ObRawExpr *> &padnull_exprs,
                            bool is_nullside,
                            ObRawExpr *&expr,
                            bool &has_nullside_column);

  int check_nullside_expr(ObRawExpr *expr, bool &bret);

  int transform_full_outer_join(ObDMLStmt *&stmt, bool &trans_happened);

  int check_join_condition(ObDMLStmt *stmt,
                           JoinedTable *table,
                           bool &has_equal,
                           bool &has_subquery);

  /**
   * @brief recursively_eliminate_full_join
   * 以左-右-后的方式后续遍历from item及semi from item中的joined_table结构
   */
  int recursively_eliminate_full_join(ObDMLStmt &stmt,
                                      TableItem *table_item,
                                      bool &trans_happened);

  /**
   * @brief expand_full_outer_join
   * for select stmt contains a single full outer join, expand to left join union all anti join
   */
  int expand_full_outer_join(ObSelectStmt *&ref_query);

  int create_select_items_for_semi_join(ObDMLStmt *stmt,
                                        TableItem *from_table_item,
                                        const ObIArray<SelectItem> &select_items,
                                        ObIArray<SelectItem> &output_select_items);

  int switch_left_outer_to_semi_join(ObSelectStmt *&sub_stmt,
                                     JoinedTable *joined_table,
                                     const ObIArray<SelectItem> &select_items);

  int extract_idx_from_table_items(ObDMLStmt *sub_stmt,
                                   const TableItem *table_item,
                                   ObSqlBitSet<> &rel_ids);

  int formalize_limit_expr(ObDMLStmt &stmt);
  int transform_rollup_exprs(ObDMLStmt *stmt, bool &trans_happened);
  int get_rollup_const_exprs(ObSelectStmt *stmt,
                             ObIArray<ObRawExpr*> &const_exprs,
                             ObIArray<ObRawExpr*> &const_remove_const_exprs,
                             ObIArray<ObRawExpr*> &exec_params,
                             ObIArray<ObRawExpr*> &exec_params_remove_const_exprs,
                             ObIArray<ObRawExpr*> &column_ref_exprs,
                             ObIArray<ObRawExpr*> &column_ref_remove_const_exprs,
                             ObIArray<ObRawExpr*> &query_ref_exprs,
                             ObIArray<ObRawExpr*> &query_ref_remove_const_exprs,
                             bool &trans_happened);
  int replace_remove_const_exprs(ObSelectStmt *stmt,
                                ObIArray<ObRawExpr*> &const_exprs,
                                ObIArray<ObRawExpr*> &const_remove_const_exprs,
                                ObIArray<ObRawExpr*> &exec_params,
                                ObIArray<ObRawExpr*> &exec_params_remove_const_exprs,
                                ObIArray<ObRawExpr*> &column_ref_exprs,
                                ObIArray<ObRawExpr*> &column_ref_remove_const_exprs,
                                ObIArray<ObRawExpr*> &query_ref_exprs,
                                ObIArray<ObRawExpr*> &query_ref_remove_const_exprs);

  int transform_cast_multiset_for_stmt(ObDMLStmt *&stmt, bool &is_happened);
  int transform_cast_multiset_for_expr(ObDMLStmt &stmt, ObRawExpr *&expr, bool &trans_happened);
  int add_constructor_to_multiset(ObDMLStmt &stmt,
                                  ObQueryRefRawExpr *multiset_expr,
                                  const pl::ObPLDataType &elem_type,
                                  bool& trans_happened);
  int add_column_conv_to_multiset(ObQueryRefRawExpr *multiset_expr,
                                  const pl::ObPLDataType &elem_type,
                                  bool& trans_happened);

  int transform_for_last_insert_id(ObDMLStmt *stmt, bool &trans_happened);
  int expand_for_last_insert_id(ObDMLStmt &stmt, ObIArray<ObRawExpr*> &exprs, bool &is_happended);
  int expand_last_insert_id_for_join(ObDMLStmt &stmt, JoinedTable *join_table, bool &is_happened);
  int remove_last_insert_id(ObRawExpr *&expr);
  int check_last_insert_id_removable(const ObRawExpr *expr, bool &is_removable);

  int expand_correlated_cte(ObDMLStmt *stmt, bool& trans_happened);
  int check_exec_param_correlated(const ObRawExpr *expr, bool &is_correlated);
  int check_is_correlated_cte(ObSelectStmt *stmt, ObIArray<ObSelectStmt *> &visited_cte, bool &is_correlated);
  int convert_join_preds_vector_to_scalar(JoinedTable &joined_table, bool &trans_happened);
private:
  DISALLOW_COPY_AND_ASSIGN(ObTransformPreProcess);
};

}
}

#endif /* OB_TRANSFORM_PRE_PROCESS_H_ */
