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

#ifndef OB_TRANSFORM_GROUPBY_PLACEMENT_H
#define OB_TRANSFORM_GROUPBY_PLACEMENT_H

#include "sql/rewrite/ob_transform_rule.h"
#include "sql/resolver/dml/ob_select_stmt.h"
namespace oceanbase {
namespace sql {

/**
 * @brief The ObTransformGroupByReplacement class
 * References:
 *  [1] Including Group-By in Query Optimization
 *  [2] Eager Aggregation and Lazy Aggregation
 */
class ObTransformGroupByPlacement : public ObTransformRule {
public:
  ObTransformGroupByPlacement(ObTransformerCtx* ctx) : ObTransformRule(ctx, TransMethod::POST_ORDER)
  {}

  virtual ~ObTransformGroupByPlacement()
  {}

  virtual int transform_one_stmt(
      common::ObIArray<ObParentDMLStmt>& parent_stmts, ObDMLStmt*& stmt, bool& trans_happened) override;

protected:
  virtual int adjust_transform_types(uint64_t& transform_types) override;

private:
  struct PullupHelper {
    PullupHelper()
        : parent_table_(NULL),
          table_id_(OB_INVALID),
          not_null_column_table_id_(OB_INVALID),
          not_null_column_id_(OB_INVALID),
          need_check_null_propagate_(false),
          need_check_having_(false),
          need_merge_(false)
    {}
    virtual ~PullupHelper()
    {}

    JoinedTable* parent_table_;
    uint64_t table_id_;
    uint64_t not_null_column_table_id_;
    uint64_t not_null_column_id_;
    bool need_check_null_propagate_;
    bool need_check_having_;
    bool need_merge_;

    TO_STRING_KV(K_(parent_table), K_(table_id), K_(not_null_column_id), K_(not_null_column_table_id),
        K_(need_check_null_propagate), K_(need_check_having), K_(need_merge));
  };

  struct PushDownParam {
    ObSqlBitSet<> table_bit_index_;
    ObSEArray<ObRawExpr*, 4> group_exprs_;
    ObSEArray<ObRawExpr*, 4> aggr_exprs_;
    ObSEArray<ObRawExpr*, 4> join_columns_;                /// filter processed 'when or after' join
    ObSEArray<ObRawExpr*, 4> filter_exprs_;                /// filter processed 'before' join
    ObSEArray<JoinedTable*, 4> correlated_joined_tables_;  // filter exprs correlated joined tables

    TO_STRING_KV(K_(table_bit_index), K_(group_exprs), K_(aggr_exprs), K_(join_columns), K_(filter_exprs),
        K_(correlated_joined_tables));

    int merge(PushDownParam& other);

    void reset()
    {
      table_bit_index_.reset();
      group_exprs_.reset();
      aggr_exprs_.reset();
      join_columns_.reset();
      filter_exprs_.reset();
      correlated_joined_tables_.reset();
    }
  };

  int check_groupby_push_down_validity(ObSelectStmt* stmt, bool& is_valid);

  int compute_push_down_param(
      ObSelectStmt* stmt, ObIArray<PushDownParam>& params, ObIArray<uint64_t>& flattern_joined_tables, bool& is_valid);

  int transform_groupby_push_down(
      common::ObIArray<ObParentDMLStmt>& parent_stmts, ObDMLStmt*& stmt, bool& trans_happened);

  int check_groupby_validity(const ObSelectStmt& stmt, bool& is_valid);

  int check_collation_validity(const ObDMLStmt& stmt, bool& is_valid);

  int is_filterable_join(ObSelectStmt* stmt, ObRawExpr* join_cond, ObIArray<PushDownParam>& params, bool& is_valid);

  int check_join_expr_validity(ObSelectStmt* stmt, ObIArray<PushDownParam>& params, ObRawExpr* expr, bool& is_valid);

  int check_outer_join_aggr(ObSelectStmt* stmt, JoinedTable* joined_table, bool& is_valid);

  int get_null_side_tables(ObDMLStmt& stmt, JoinedTable& joined_table, ObSqlBitSet<>& table_set);

  int do_groupby_push_down(ObSelectStmt* stmt, ObIArray<PushDownParam>& params,
      ObIArray<uint64_t>& flattern_joined_tables, ObSelectStmt*& trans_stmt, bool& trans_happend);

  int distribute_group_aggr(ObSelectStmt* stmt, ObIArray<PushDownParam>& params);

  int distribute_filter(
      ObSelectStmt* stmt, ObIArray<PushDownParam>& params, ObSqlBitSet<>& outer_table_set, ObRawExpr* cond);

  int distribute_joined_on_conds(ObDMLStmt* stmt, ObIArray<PushDownParam>& params, JoinedTable* joined_table);

  int transform_groupby_push_down(ObSelectStmt* stmt, ObIArray<uint64_t>& flattern_joined_tables,
      ObSqlBitSet<>& outer_join_tables, ObIArray<PushDownParam>& params);

  int push_down_group_by_into_view(ObSelectStmt* stmt, const ObIArray<TableItem*>& table_items,
      ObIArray<uint64_t>& flattern_joined_tables, PushDownParam& params, TableItem*& new_table_item);

  int transform_aggregation_expr(ObDMLStmt& stmt, ObAggFunRawExpr& aggr_expr, ObIArray<TableItem*>& eager_aggr_views,
      ObIArray<bool>& table_types, ObRawExpr*& new_aggr_expr);

  int convert_aggr_expr(ObDMLStmt* stmt, ObAggFunRawExpr* aggr_expr, ObRawExpr*& output_expr);

  int build_case_when(
      ObDMLStmt* stmt, ObRawExpr* when_expr, ObRawExpr* value_expr, ObRawExpr* else_expr, ObRawExpr*& case_when);

  int has_stmt_column(ObRawExpr* expr, const int64_t stmt_level, bool& has);

  int get_count_star(ObDMLStmt& stmt, TableItem* table_item, bool is_outer_join_table, ObRawExpr*& count_column);

  int get_view_column(
      ObDMLStmt& stmt, TableItem* table_item, bool is_outer_join_table, ObRawExpr* aggr_expr, ObRawExpr*& aggr_column);

  int wrap_case_when_for_count(
      ObDMLStmt* stmt, ObColumnRefRawExpr* view_count, ObRawExpr*& output, bool is_count_star = false);

  int update_joined_table(TableItem* table, const TableItem* old_table, TableItem* new_table, bool& is_found);

  int check_unique(ObSelectStmt* stmt, PushDownParam& param, bool& is_unique);

  int add_exprs(
      const ObIArray<ObRawExpr*>& exprs, int64_t stmt_level, ObSqlBitSet<>& table_set, ObIArray<ObRawExpr*>& dest);

  int extract_non_agg_columns(ObRawExpr* expr, const int64_t stmt_level, ObIArray<ObRawExpr*>& col_exprs);

  int merge_tables(ObIArray<PushDownParam>& params, const ObSqlBitSet<>& table_set);

  /////////////////////    transform group by pull up    //////////////////////////////

  int transform_groupby_pull_up(
      common::ObIArray<ObParentDMLStmt>& parent_stmts, ObDMLStmt*& stmt, bool& trans_happened);

  int check_groupby_pullup_validity(ObDMLStmt* stmt, ObIArray<PullupHelper>& valid_views);

  int check_groupby_pullup_validity(ObDMLStmt* stmt, TableItem* table, PullupHelper& helper, bool contain_inner_table,
      ObSqlBitSet<>& ignore_tables, ObIArray<PullupHelper>& valid_views, bool& is_valid);

  int check_ignore_views(ObDMLStmt& stmt, ObIArray<ObRawExpr*>& conditions, ObSqlBitSet<>& ignore_tables);

  int is_contain_aggr_item(ObColumnRefRawExpr* column_expr, TableItem* view_table, bool& is_contain);

  int is_valid_group_stmt(ObSelectStmt* sub_stmt, bool& is_valid_group);

  int check_null_propagate(ObDMLStmt* parent_stmt, ObSelectStmt* child_stmt, PullupHelper& helper, bool& is_valid);

  int find_not_null_column(ObDMLStmt& parent_stmt, ObSelectStmt& child_stmt, PullupHelper& helper,
      ObIArray<ObRawExpr*>& column_exprs, ObRawExpr*& not_null_column);

  int find_not_null_column_with_condition(ObDMLStmt& parent_stmt, ObSelectStmt& child_stmt, PullupHelper& helper,
      ObIArray<ObRawExpr*>& column_exprs, ObRawExpr*& not_null_column);

  int find_null_propagate_column(
      ObRawExpr* condition, ObIArray<ObRawExpr*>& columns, ObRawExpr*& null_propagate_column, bool& is_valid);

  int do_groupby_pull_up(ObSelectStmt* stmt, PullupHelper& helper);

  int get_trans_view(ObDMLStmt* stmt, ObSelectStmt*& view_stmt);

  int wrap_case_when_if_necessary(ObSelectStmt& child_stmt, PullupHelper& helper, ObIArray<ObRawExpr*>& exprs);

  int wrap_case_when(ObSelectStmt& child_stmt, ObRawExpr* not_null_column, ObRawExpr*& expr);

private:
  // help functions
  int64_t get_count_sum_num(const ObIArray<ObRawExpr*>& exprs)
  {
    int64_t num = 0;
    for (int64_t i = 0; i < exprs.count(); ++i) {
      if (OB_ISNULL(exprs.at(i))) {
        // do nothing
      } else if (exprs.at(i)->get_expr_type() == T_FUN_SUM || exprs.at(i)->get_expr_type() == T_FUN_COUNT) {
        ++num;
      }
    }
    return num;
  }

  int64_t get_valid_eager_aggr_num(const ObIArray<PushDownParam>& params)
  {
    int64_t num = 0;
    for (int64_t i = 0; i < params.count(); ++i) {
      if (!params.at(i).table_bit_index_.is_empty()) {
        ++num;
      }
    }
    return num;
  }
};

}  // namespace sql
}  // namespace oceanbase

#endif  // OB_TRANSFORM_GROUPBY_REPLACEMENT_H
