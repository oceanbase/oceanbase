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

#ifndef OCEANBASE_SQL_REWRITE_OB_TRANSFORM_SIMPLIFY_
#define OCEANBASE_SQL_REWRITE_OB_TRANSFORM_SIMPLIFY_

#include "sql/rewrite/ob_transform_rule.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/ob_sql_context.h"

namespace oceanbase {
namespace common {
class ObExprCtx;
class JoinedTable;
};  // namespace common

namespace sql {
class ObPhysicalPlan;
class ObTransformSimplify : public ObTransformRule {
public:
  explicit ObTransformSimplify(ObTransformerCtx* ctx) : ObTransformRule(ctx, TransMethod::POST_ORDER)
  {}
  virtual ~ObTransformSimplify()
  {}
  virtual int transform_one_stmt(
      common::ObIArray<ObParentDMLStmt>& parent_stmts, ObDMLStmt*& stmt, bool& trans_happened) override;

private:
  int having_filter_can_be_pushed_down(
      const ObRawExpr* having_filter, const ObSelectStmt& select_stmt, const bool has_rownum, bool& can_be);
  int replace_is_null_condition(ObDMLStmt* stmt, bool& trans_happened);
  int inner_replace_is_null_condition(ObDMLStmt* stmt, ObRawExpr*& expr, bool& trans_happened);
  int do_replace_is_null_condition(ObDMLStmt* stmt, ObRawExpr*& expr, bool& trans_happened);
  int replace_op_null_condition(ObDMLStmt* stmt, bool& trans_happened);
  int replace_cmp_null_condition(
      ObRawExpr*& expr, const ObDMLStmt& stmt, const ParamStore& param_store, bool& trans_happened);
  int extract_null_expr(ObRawExpr* expr, const ObDMLStmt& stmt, const ParamStore& param_store,
      ObIArray<const ObRawExpr*>& null_expr_lists);
  int is_expected_table_for_replace(ObDMLStmt* stmt, uint64_t table_id, bool& is_expected);
  int remove_stmt_order_by(ObDMLStmt*& stmt, bool& trans_happened);
  int remove_order_by_for_subquery(ObDMLStmt*& stmt, bool& trans_happened);
  int remove_order_by_for_view_stmt(ObDMLStmt*& stmt, bool& trans_happened);
  int do_remove_stmt_order_by(ObSelectStmt* select_stmt, bool& trans_happened);
  int remove_stmt_win(ObDMLStmt* stmt, bool& trans_happened);
  int check_stmt_win_can_be_removed(ObSelectStmt* select_stmt, ObWinFunRawExpr* win_expr, bool& can_be);
  int check_window_contain_aggr(ObWinFunRawExpr* win_expr, bool& contain);
  int do_remove_stmt_win(ObSelectStmt* select_stmt, ObIArray<ObRawExpr*>& exprs);
  int remove_order_by_duplicates(ObDMLStmt*& stmt, bool& trans_happened);
  int remove_stmt_group_by(ObDMLStmt*& stmt, bool& trans_happened);
  int check_stmt_group_by_can_be_removed(ObSelectStmt* select_stmt, bool& can_be);
  int inner_remove_stmt_group_by(ObSelectStmt* select_stmt, bool& trans_happened);

  int check_aggr_win_can_be_removed(const ObDMLStmt* stmt, ObRawExpr* expr, bool& can_remove);
  int transform_aggr_win_to_common_expr(ObSelectStmt* select_stmt, ObRawExpr* expr, ObRawExpr*& new_expr);
  int get_param_value(const ObDMLStmt* stmt, ObRawExpr* param, bool& is_vaild, int64_t& value);
  int remove_group_by_duplicates(ObDMLStmt*& stmt, bool& trans_happened);
  int exist_item_by_expr(ObRawExpr* expr, common::ObIArray<OrderItem>& order_items, bool& is_exist);
  int remove_redundent_group_by(ObDMLStmt* stmt, bool& trans_happened);
  int check_upper_stmt_validity(ObSelectStmt* upper_stmt, bool& is_valid);
  int get_valid_child_stmts(ObSelectStmt* upper_stmt, ObSelectStmt* stmt, ObArray<ObSelectStmt*>& valid_child_stmts);
  int remove_child_stmts_group_by(ObArray<ObSelectStmt*>& child_stmts);
  int get_upper_column_exprs(ObSelectStmt& upper_stmt, ObSelectStmt& stmt, ObIArray<ObRawExpr*>& aggr_column_exprs,
      ObIArray<ObRawExpr*>& no_aggr_column_exprs, ObIArray<ObRawExpr*>& child_aggr_exprs, bool& is_valid);
  int check_upper_group_by(ObSelectStmt& upper_stmt, ObIArray<ObRawExpr*>& group_exprs, bool& is_valid);
  int check_aggrs_matched(ObIArray<ObAggFunRawExpr*>& upper_aggrs, ObIArray<ObRawExpr*>& group_exprs,
      ObIArray<ObRawExpr*>& aggr_exprs, ObIArray<ObRawExpr*>& child_aggr_exprs, bool& is_valid);
  int check_upper_condition(ObIArray<ObRawExpr*>& cond_exprs, ObIArray<ObRawExpr*>& aggr_column_exprs, bool& is_valid);
  int exist_exprs_in_expr(const ObRawExpr* src_expr, ObIArray<ObRawExpr*>& dst_exprs, bool& is_exist);
  int remove_distinct_before_const(ObDMLStmt* stmt, bool& trans_happened);
  int remove_stmt_distinct(ObDMLStmt* stmt, bool& trans_happened);
  int remove_child_stmt_distinct(ObSelectStmt* stmt, bool& trans_happened);
  int try_remove_child_stmt_distinct(ObSelectStmt* stmt, bool& trans_happened);
  int distinct_can_be_eliminated(ObDMLStmt* stmt, bool& can_be);
  int distinct_prune_from_items(ObDMLStmt* stmt, bool& trans_happened);
  int distinct_need_prune(const ObSelectStmt* select_stmt, bool& need_prune);
  int fetch_pruned_rels(const ObSelectStmt* select_stmt, ObRelIds& pruned_rels);
  int prune_from_items(ObSelectStmt* select_stmt);
  int pullup_select_expr(ObDMLStmt*& stmt, bool& trans_happened);
  int get_pullup_select_expr(
      ObSelectStmt* stmt, ObIArray<ObRawExpr*>& pullup_select_exprs, ObIArray<ObRawExpr*>& new_select_exprs);
  int do_pullup_select_expr(
      ObSelectStmt*& select_stmt, ObIArray<ObRawExpr*>& pullup_select_exprs, ObIArray<ObRawExpr*>& new_select_exprs);

  int pushdown_limit_offset(ObDMLStmt* stmt, bool& trans_happened);
  int check_pushdown_limit_offset_validity(ObSelectStmt* stmt, ObSelectStmt*& view_stmt, bool& is_valid);
  int do_pushdown_limit_offset(ObSelectStmt* upper_stmt, ObSelectStmt* view_stmt);

  /*
   * `ROW = ROW` => `COL = COL AND COL = COL`
   */
  int convert_preds_vector_to_scalar(ObDMLStmt* stmt, bool& trans_happened);
  int convert_join_preds_vector_to_scalar(TableItem* table_item, bool& trans_happened);

  int inner_convert_preds_vector_to_scalar(ObRawExpr* expr, common::ObIArray<ObRawExpr*>& exprs, bool& trans_happened);

  /**
   * @brief eliminate_expr_subquery
   * subquery => expr
   *  select * from t1 where t1.pk > (select 1);
   * => select * from t1 where t1.pk > 1;
   *  select (select 1);
   * => select 1;
   */
  int transform_subquery_as_expr(ObDMLStmt* stmt, bool& trans_happened);

  /**
   * @brief try_trans_subquery_in_expr
   */
  int try_trans_subquery_in_expr(ObDMLStmt* stmt, ObRawExpr*& expr, bool& trans_happened);

  int do_trans_subquery_as_expr(ObDMLStmt* stmt, ObRawExpr*& query_ref, bool& trans_happened);

  int simplify_win_exprs(ObDMLStmt* stmt, bool& trans_happened);

  int simplify_win_expr(ObRawExpr* expr, bool& trans_happened);

  int remove_dummy_exprs(ObDMLStmt* stmt, bool& trans_happened);
  int remove_dummy_filter_exprs(common::ObIArray<ObRawExpr*>& exprs, bool& trans_happened);
  int remove_dummy_join_condition_exprs(JoinedTable& join_table, bool& trans_happened);
  int inner_remove_dummy_expr(ObRawExpr*& expr, bool& trans_happened);
  int adjust_dummy_expr(const common::ObIArray<int64_t>& return_exprs, const common::ObIArray<int64_t>& remove_exprs,
      common::ObIArray<ObRawExpr*>& adjust_exprs);
  int extract_dummy_expr_info(const common::ObIArray<ObRawExpr*>& exprs, common::ObIArray<int64_t>& true_exprs,
      common::ObIArray<int64_t>& false_filters);
  int is_valid_transform_type(ObRawExpr* expr, bool& is_valid);
  int remove_redundent_select(ObDMLStmt*& stmt, bool& trans_happened);
  int try_remove_redundent_select(ObSelectStmt& stmt, ObSelectStmt*& new_stmt);
  int check_subquery_valid(ObSelectStmt& stmt, bool& is_valid);
  int remove_aggr_distinct(ObDMLStmt* stmt, bool& trans_happened);
  int remove_aggr_duplicates(ObSelectStmt* select_stmt);
  int remove_win_func_duplicates(ObSelectStmt* select_stmt);

  /**
   * @brief remove_dummy_case_when
   * Given
   * case when expr then
   *            case when expr then value1
   *                 else value2
   *                 end
   *      else value3
   *      end
   * the above expr can be rewritten as:
   * case when expr then value1
   *      else value3
   *      end
   */
  int remove_dummy_case_when(ObDMLStmt* stmt, bool& trans_happened);

  int remove_dummy_case_when(ObQueryCtx* query_ctx, ObRawExpr* expr, bool& trans_happened);

  int inner_remove_dummy_case_when(ObQueryCtx* query_ctx, ObCaseOpRawExpr* case_expr, bool& trans_happened);
  int push_down_outer_join_condition(ObDMLStmt* stmt, bool& trans_happened);
  int push_down_outer_join_condition(ObDMLStmt* stmt, TableItem* join_table, bool& trans_happened);
  int try_push_down_outer_join_conds(ObDMLStmt* stmt, JoinedTable* join_table, bool& trans_happened);
  int create_view_with_left_table(ObDMLStmt* stmt, JoinedTable* joined_table);
  int push_down_on_condition(ObDMLStmt* stmt, JoinedTable* join_table, ObIArray<ObRawExpr*>& conds);

private:
  DISALLOW_COPY_AND_ASSIGN(ObTransformSimplify);
};

}  // namespace sql
}  // namespace oceanbase
#endif  // OCEANBASE_SQL_REWRITE_OB_TRANSFORM_SIMPLIFY_
