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
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/ob_sql_context.h"

namespace oceanbase {

namespace sql {

typedef std::pair<uint64_t, uint64_t> JoinTableIdPair;

struct MVDesc {
  uint64_t mv_tid_;
  uint64_t base_tid_;
  uint64_t dep_tid_;
  ObString table_name_;
  ObSelectStmt* mv_stmt_;

  MVDesc() : mv_tid_(OB_INVALID_ID), base_tid_(OB_INVALID_ID), dep_tid_(OB_INVALID_ID), mv_stmt_(NULL)
  {}
  TO_STRING_KV("mv_tid", mv_tid_, "base_tid", base_tid_, "dep_tid_", dep_tid_);
};

class ObTransformPreProcess : public ObTransformRule {
public:
  explicit ObTransformPreProcess(ObTransformerCtx* ctx)
      : ObTransformRule(ctx, TransMethod::POST_ORDER), mock_table_set_(), origin_table_set_()
  {}
  virtual ~ObTransformPreProcess()
  {}

  virtual int transform_one_stmt(
      common::ObIArray<ObParentDMLStmt>& parent_stmts, ObDMLStmt*& stmt, bool& trans_happened) override;

  static int transform_expr(
      ObRawExprFactory& expr_factory, const ObSQLSessionInfo& session, ObRawExpr*& expr, bool& trans_happened);

private:
  // used for transform in expr to or exprs
  struct DistinctObjMeta {
    ObObjType obj_type_;
    ObCollationType coll_type_;
    ObCollationLevel coll_level_;

    DistinctObjMeta(ObObjType obj_type, ObCollationType coll_type, ObCollationLevel coll_level)
        : obj_type_(obj_type), coll_type_(coll_type), coll_level_(coll_level)
    {
      if (!ObDatumFuncs::is_string_type(obj_type_) && !ObDatumFuncs::is_json(obj_type_)) {
        coll_type_ = CS_TYPE_MAX;
        coll_level_ = CS_LEVEL_INVALID;
      }
    }
    DistinctObjMeta() : obj_type_(common::ObMaxType), coll_type_(common::CS_TYPE_MAX), coll_level_(CS_LEVEL_INVALID)
    {}

    bool operator==(const DistinctObjMeta& other) const
    {
      bool cs_level_equal = share::is_oracle_mode() ? true : (coll_level_ == other.coll_level_);
      return obj_type_ == other.obj_type_ && coll_type_ == other.coll_type_ && cs_level_equal;
    }
    TO_STRING_KV(K_(obj_type), K_(coll_type));
  };
  /*
   * following functions are for grouping sets and multi rollup
   */
  int transform_for_grouping_sets_and_multi_rollup(ObDMLStmt*& stmt, bool& trans_happened);
  int add_generated_table_as_temp_table(ObTransformerCtx* ctx, ObDMLStmt* stmt);
  int replace_with_set_stmt_view(
      ObSelectStmt* origin_stmt, ObSelectStmt* grouping_sets_view, ObSelectStmt*& union_stmt);
  int create_set_view_stmt(ObSelectStmt* origin_stmt, ObSelectStmt*& set_view_stmt);
  int create_select_list_from_grouping_sets(ObSelectStmt* stmt, common::ObIArray<ObGroupbyExpr>& groupby_exprs_list,
      int64_t cur_index, ObIArray<ObRawExpr*>& old_exprs, ObIArray<ObRawExpr*>& new_exprs);
  int64_t get_total_count_of_groupby_stmt(
      ObIArray<ObGroupingSetsItem>& grouping_sets_items, ObIArray<ObMultiRollupItem>& multi_rollup_items);

  int get_groupby_exprs_list(ObIArray<ObGroupingSetsItem>& grouping_sets_items,
      ObIArray<ObMultiRollupItem>& multi_rollup_items, ObIArray<ObGroupbyExpr>& groupby_exprs_list);

  int expand_multi_rollup_items(
      ObIArray<ObMultiRollupItem>& multi_rollup_items, ObIArray<ObGroupbyExpr>& rollup_list_exprs);

  int combination_two_rollup_list(ObIArray<ObGroupbyExpr>& rollup_list_exprs1,
      ObIArray<ObGroupbyExpr>& rollup_list_exprs2, ObIArray<ObGroupbyExpr>& rollup_list_exprs);

  /*
   * following functions are for hierarchical query
   */
  int transform_for_hierarchical_query(ObDMLStmt* stmt, bool& trans_happened);

  int create_connect_by_view(ObSelectStmt& stmt);

  int create_and_mock_join_view(ObSelectStmt& stmt);

  int classify_join_conds(
      ObSelectStmt& stmt, ObIArray<ObRawExpr*>& normal_join_conds, ObIArray<ObRawExpr*>& other_conds);

  int is_cond_in_one_from_item(ObSelectStmt& stmt, ObRawExpr* expr, bool& in_from_item);

  int extract_connect_by_related_exprs(ObSelectStmt& stmt, ObIArray<ObRawExpr*>& special_exprs);

  int extract_connect_by_related_exprs(ObRawExpr* expr, ObIArray<ObRawExpr*>& special_exprs);

  int pushdown_start_with_connect_by(ObSelectStmt& stmt, ObSelectStmt& view);

  int replace_rownum_expr(ObSelectStmt& stmt, ObIArray<ObRawExpr*>& exprs);

  int check_has_rownum(ObIArray<ObRawExpr*>& exprs, bool& has_rownum);
  /*
   * following functions are for materialized view
   */
  int transform_for_materialized_view(ObDMLStmt* stmt, bool& trans_happened);
  uint64_t get_real_tid(uint64_t tid, ObSelectStmt& stmt);
  int is_col_in_mv(uint64_t tid, uint64_t cid, MVDesc mv_desc, bool& result);
  int is_all_column_covered(ObSelectStmt& stmt, MVDesc mv_desc, const JoinTableIdPair& idp, bool& result);
  int expr_equal(
      ObSelectStmt* select_stmt, const ObRawExpr* r1, ObSelectStmt* mv_stmt, const ObRawExpr* r2, bool& result);
  int check_where_condition_covered(
      ObSelectStmt* select_stmt, ObSelectStmt* mv_stmt, bool& result, ObBitSet<>& expr_idx);
  int get_column_id(uint64_t mv_tid, uint64_t tid, uint64_t cid, uint64_t& mv_cid);
  int rewrite_column_id(ObSelectStmt* select_stmt, MVDesc mv_desc, const JoinTableIdPair& idp);
  int rewrite_column_id2(ObSelectStmt* select_stmt);
  int check_can_transform(
      ObSelectStmt*& select_stmt, MVDesc mv_desc, bool& result, JoinTableIdPair& idp, ObBitSet<>& expr_idx);
  int reset_table_item(ObSelectStmt* select_stmt, uint64_t mv_tid, uint64_t base_tid, const JoinTableIdPair& idp);
  int reset_where_condition(ObSelectStmt* select_stmt, ObBitSet<>& expr_idx);
  int reset_from_item(ObSelectStmt* select_stmt, uint64_t mv_tid, const JoinTableIdPair& idp);
  int inner_transform(ObDMLStmt*& stmt, ObIArray<MVDesc>& mv_array, bool& trans_happened);
  int get_all_related_mv_array(ObDMLStmt*& stmt, ObIArray<MVDesc>& mv_array);
  int get_view_stmt(const share::schema::ObTableSchema& index_schema, ObSelectStmt*& view_stmt);
  int reset_part_expr(ObSelectStmt* select_stmt, uint64_t mv_tid, const JoinTableIdPair& idp);
  int is_rowkey_equal_covered(
      ObSelectStmt* select_stmt, uint64_t mv_tid, uint64_t rowkey_tid, uint64_t base_or_dep_tid, bool& is_covered);
  int get_join_tid_cid(
      uint64_t tid, uint64_t cid, uint64_t& out_tid, uint64_t& out_cid, const share::schema::ObTableSchema& mv_schema);

  /*
   * follow functions are used for eliminate having
   */
  int eliminate_having(ObDMLStmt* stmt, bool& trans_happened);

  /*
   * following functions are used to replace func is serving tenant
   */
  int replace_func_is_serving_tenant(ObDMLStmt*& stmt, bool& trans_happened);
  int recursive_replace_func_is_serving_tenant(ObDMLStmt& stmt, ObRawExpr*& cond_expr, bool& trans_happened);
  int calc_const_raw_expr_and_get_int(const ObStmt& stmt, ObRawExpr* const_expr, ObExecContext& exec_ctx,
      ObSQLSessionInfo* session, ObIAllocator& allocator, int64_t& result);
  int calc_const_raw_expr(const ObStmt& stmt, ObRawExpr* const_expr, ObExprCtx& expr_ctx, ObPhysicalPlan& phy_plan,
      ObIAllocator& allocator, ObObj& result);

  int transform_for_merge_into(ObDMLStmt* stmt, bool& trans_happened);

  /*
   * following functions are used for temporary and se table
   */
  int transform_for_temporary_table(ObDMLStmt*& stmt, bool& trans_happened);
  int add_filter_for_temporary_table(ObDMLStmt& stmt, const TableItem& table_item);
  int collect_all_tableitem(ObDMLStmt* stmt, TableItem* table_item, common::ObArray<TableItem*>& table_item_list);

  int transform_exprs(ObDMLStmt* stmt, bool& trans_happened);
  int transform_for_nested_aggregate(ObDMLStmt*& stmt, bool& trans_happened);
  int generate_child_level_aggr_stmt(ObSelectStmt* stmt, ObSelectStmt*& sub_stmt);
  int get_first_level_output_exprs(ObSelectStmt* sub_stmt, common::ObIArray<ObRawExpr*>& inner_aggr_exprs);
  int generate_parent_level_aggr_stmt(ObSelectStmt*& stmt, ObSelectStmt* sub_stmt);
  int remove_nested_aggr_exprs(ObSelectStmt* stmt);
  int construct_column_items_from_exprs(const ObIArray<ObRawExpr*>& column_exprs, ObIArray<ColumnItem>& column_items);
  /*
   * following functions are used to transform in_expr to or_expr
   */
  static int transform_in_or_notin_expr_with_row(ObRawExprFactory& expr_factory, const ObSQLSessionInfo& session,
      const bool is_in_expr, ObRawExpr*& in_expr, bool& trans_happened);

  static int transform_in_or_notin_expr_without_row(ObRawExprFactory& expr_factory, const ObSQLSessionInfo& session,
      const bool is_in_expr, ObRawExpr*& in_epxr, bool& trans_happened);

  static int create_partial_expr(ObRawExprFactory &expr_factory, ObRawExpr *left_expr,
      ObIArray<ObRawExpr *> &same_type_exprs, const bool is_in_expr, ObIArray<ObRawExpr *> &transed_in_exprs);

  /*
   * following functions are used to transform arg_case_expr to case_expr
   */
  static int transform_arg_case_recursively(
      ObRawExprFactory& expr_factory, const ObSQLSessionInfo& session, ObRawExpr*& expr, bool& trans_happened);
  static int transform_arg_case_expr(
      ObRawExprFactory& expr_factory, const ObSQLSessionInfo& session, ObRawExpr*& expr, bool& trans_happened);
  static int create_equal_expr_for_case_expr(ObRawExprFactory& expr_factory, const ObSQLSessionInfo& session,
      ObRawExpr* arg_expr, ObRawExpr* when_expr, const ObExprResType& case_res_type, ObOpRawExpr*& equal_expr);
  static int add_row_type_to_array_no_dup(
      common::ObIArray<ObSEArray<DistinctObjMeta, 4>>& row_type_array, const ObSEArray<DistinctObjMeta, 4>& row_type);

  static bool is_same_row_type(
      const common::ObIArray<DistinctObjMeta>& left, const common::ObIArray<DistinctObjMeta>& right);

  static int get_final_transed_or_and_expr(ObRawExprFactory& expr_factory, const ObSQLSessionInfo& session,
      const bool is_in_expr, common::ObIArray<ObRawExpr*>& transed_in_exprs, ObRawExpr*& final_or_expr);

  static int check_and_transform_in_or_notin(
      ObRawExprFactory& expr_factory, const ObSQLSessionInfo& session, ObRawExpr*& in_expr, bool& trans_happened);
  static int replace_in_or_notin_recursively(
      ObRawExprFactory& expr_factory, const ObSQLSessionInfo& session, ObRawExpr*& root_expr, bool& trans_happened);
  int transformer_aggr_expr(ObDMLStmt* stmt, bool& trans_happened);
  int transform_rownum_as_limit_offset(
      const ObIArray<ObParentDMLStmt>& parent_stmts, ObDMLStmt*& stmt, bool& trans_happened);
  int transform_common_rownum_as_limit(ObDMLStmt*& stmt, bool& trans_happened);
  int try_transform_common_rownum_as_limit(ObDMLStmt* stmt, ObRawExpr*& limit_expr);
  int transform_generated_rownum_as_limit(
      const ObIArray<ObParentDMLStmt>& parent_stmts, ObDMLStmt* stmt, bool& trans_happened);
  int try_transform_generated_rownum_as_limit_offset(
      ObDMLStmt* upper_stmt, ObSelectStmt* select_stmt, ObRawExpr*& limit_expr, ObRawExpr*& offset_expr);
  int transform_generated_rownum_eq_cond(ObRawExpr* eq_value, ObRawExpr*& limit_expr, ObRawExpr*& offset_expr);
  int expand_grouping_sets_items(
      common::ObIArray<ObGroupingSetsItem>& grouping_sets_items, common::ObIArray<ObGroupbyExpr>& grouping_sets_exprs);
  int replace_select_and_having_exprs(
      ObSelectStmt* select_stmt, ObIArray<ObRawExpr*>& old_exprs, ObIArray<ObRawExpr*>& new_exprs);
  int replace_stmt_special_exprs(ObSelectStmt* select_stmt, ObRawExpr*& expr, common::ObIArray<ObRawExpr*>& old_exprs,
      common::ObIArray<ObRawExpr*>& new_exprs, bool ignore_const = false);
  bool is_select_expr_in_other_groupby_exprs(
      ObRawExpr* expr, ObIArray<ObGroupbyExpr>& groupby_exprs_list, int64_t cur_index);
  bool is_expr_in_select_item(ObIArray<SelectItem>& select_items, ObRawExpr* expr);
  int extract_select_expr_and_replace_expr(ObRawExpr* expr, ObIArray<ObRawExpr*>& groupby_exprs,
      ObIArray<ObRawExpr*>& rollup_exprs, ObIArray<ObAggFunRawExpr*>& aggr_items,
      ObIArray<ObGroupbyExpr>& groupby_exprs_list, int64_t cur_index, ObIArray<SelectItem>& select_items,
      ObIArray<ObRawExpr*>& old_exprs, ObIArray<ObRawExpr*>& new_exprs, ObRelIds& rel_ids);
  int extract_stmt_replace_expr(ObSelectStmt* select_stmt, ObIArray<ObRawExpr*>& old_exprs);
  int extract_replace_expr_from_select_expr(
      ObRawExpr* expr, ObSelectStmt* select_stmt, ObIArray<ObRawExpr*>& old_exprs);

private:
  ObRelIds mock_table_set_;
  ObRelIds origin_table_set_;
  DISALLOW_COPY_AND_ASSIGN(ObTransformPreProcess);
};

}  // namespace sql
}  // namespace oceanbase

#endif /* OB_TRANSFORM_PRE_PROCESS_H_ */
