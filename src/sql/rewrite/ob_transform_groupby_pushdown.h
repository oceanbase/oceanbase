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

#ifndef OB_TRANSFORM_GROUPBY_PUSHDOWN_H
#define OB_TRANSFORM_GROUPBY_PUSHDOWN_H

#include "sql/rewrite/ob_transform_rule.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/optimizer/ob_log_join.h"

namespace oceanbase
{
namespace sql
{

/**
 * @brief The ObTransformGroupByReplacement class
 * References:
 *  [1] Including Group-By in Query Optimization
 *  [2] Eager Aggregation and Lazy Aggregation
 */
class ObTransformGroupByPushdown : public ObTransformRule
{
public:
  ObTransformGroupByPushdown(ObTransformerCtx *ctx)
    : ObTransformRule(ctx, TransMethod::POST_ORDER, T_PLACE_GROUP_BY)
  {}

  virtual ~ObTransformGroupByPushdown() {}
  virtual int construct_transform_hint(ObDMLStmt &stmt, void *trans_params) override;
  virtual int transform_one_stmt(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                 ObDMLStmt *&stmt,
                                 bool &trans_happened) override;
protected:
  virtual int adjust_transform_types(uint64_t &transform_types) override;
  virtual int is_expected_plan(ObLogPlan *plan, void *check_ctx, bool is_trans_plan, bool &is_valid) override;
private:
  struct PushDownParam {
    ObSqlBitSet<> table_bit_index_;
    ObSEArray<ObRawExpr *, 4> group_exprs_;
    ObSEArray<ObRawExpr *, 4> aggr_exprs_;
    ObSEArray<ObRawExpr *, 4> join_columns_;   // filter processed 'when or after' join
    ObSEArray<ObRawExpr *, 4> filter_exprs_; // filter processed 'before' join
    ObSEArray<JoinedTable *, 4> correlated_joined_tables_; // filter exprs correlated joined tables
    ObSEArray<ObRawExpr *, 4> having_exprs_; // filter after 'group by', for cross join case
    TO_STRING_KV(K_(table_bit_index),
                 K_(group_exprs),
                 K_(aggr_exprs),
                 K_(join_columns),
                 K_(filter_exprs),
                 K_(correlated_joined_tables),
                 K_(having_exprs));

    int merge(PushDownParam &other);

    int assign(PushDownParam &other);

    void reset() {
      table_bit_index_.reset();
      group_exprs_.reset();
      aggr_exprs_.reset();
      join_columns_.reset();
      filter_exprs_.reset();
      correlated_joined_tables_.reset();
      having_exprs_.reset();
    }
  };

  struct ObCostBasedPushDownCtx {
    ObCostBasedPushDownCtx() {};
    int64_t stmt_id_;
    ObSqlBitSet<> new_table_relids_;
    ObSEArray<int64_t, 2> new_stmt_ids_;
  };

  struct UnionPushdownParam {
    uint64_t col_id_;
    ObItemType aggr_func_type_;
    TO_STRING_KV(K_(col_id), K_(aggr_func_type));
  };

  int try_push_down_groupby_into_join(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                      ObDMLStmt *&stmt,
                                      bool &trans_happened);

  int try_push_down_groupby_into_union(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                       ObDMLStmt *&stmt,
                                       bool &trans_happened);

  int check_push_down_into_union_validity(ObSelectStmt *stmt,
                                          bool &is_valid);

  int check_union_stmt_valid(ObSelectStmt &stmt,
                             ObSelectStmt *&union_stmt,
                             bool &is_valid);

  int check_aggr_exprs_valid(ObSelectStmt &stmt,
                             bool &is_valid,
                             bool &only_min_max);

  int check_child_stmts_valid(ObSelectStmt &union_stmt,
                              ObIArray<ObSelectStmt *> &child_stmts,
                              bool &is_valid);

  int is_basic_select_stmt(ObSelectStmt *stmt, bool &is_basic);

  int get_union_stmt(ObSelectStmt *parent_stmt,
                     ObSelectStmt *&union_stmt,
                     bool &got);

  int get_union_pushdown_param(ObSelectStmt &stmt,
                               ObIArray<UnionPushdownParam> &param);

  int is_push_through_cross_join_enabled(ObDMLStmt *stmt, bool &support_cross_join)
  {
    int ret = OB_SUCCESS;
    support_cross_join = false;
    if (OB_ISNULL(stmt) || OB_ISNULL(stmt->get_query_ctx())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else {
      support_cross_join = stmt->get_query_ctx()->check_opt_compat_version(COMPAT_VERSION_4_3_5_BP2);
    }
    return ret;
  }

  int get_col_id_of_child(ObSelectStmt &union_stmt,
                          uint64_t union_col_id,
                          uint64_t &child_col_id);

  int do_groupby_push_down_into_union(ObSelectStmt *origin_stmt,
                                      ObIArray<UnionPushdownParam> &new_columns,
                                      ObSelectStmt *&trans_stmt,
                                      bool &trans_happened);

  int transform_basic_child_stmt(ObSelectStmt *stmt,
                                 ObIArray<UnionPushdownParam> &new_columns);

  int transform_non_basic_child_stmt(ObSelectStmt *stmt,
                                     ObIArray<UnionPushdownParam> &new_columns);

  int transform_union_stmt(ObSelectStmt *union_stmt,
                           ObIArray<ObSelectStmt *> &child_stmts);

  int transform_parent_stmt_of_union(ObSelectStmt *stmt,
                                     ObSelectStmt *union_stmt,
                                     ObIArray<UnionPushdownParam> &new_columns);

  int get_new_select_expr_of_basic_child(UnionPushdownParam &param,
                                         ObIArray<ObRawExpr *> &old_exprs,
                                         ObRawExpr *&new_expr);

  int get_new_select_expr_of_non_basic_child(UnionPushdownParam &param,
                                             ObIArray<ObRawExpr *> &old_exprs,
                                             ObRawExpr *&new_expr);

  int get_new_aggr_exprs(ObIArray<UnionPushdownParam> &param,
                        ObIArray<ObRawExpr *> &new_column_exprs,
                        ObIArray<ObAggFunRawExpr *> &aggr_exprs,
                        ObIArray<ObRawExpr *> &new_aggr_exprs);

  int get_new_aggr_expr(ObIArray<UnionPushdownParam> &param,
                        ObIArray<ObRawExpr *> &new_column_exprs,
                        ObAggFunRawExpr *aggr_expr,
                        ObAggFunRawExpr *&new_aggr_expr);

  int get_new_aggr_col_exprs(ObIArray<ObRawExpr *> &aggr_col_exprs,
                             ObIArray<UnionPushdownParam> &param,
                             ObIArray<ObRawExpr *> &new_column_exprs,
                             ObIArray<ObRawExpr *> &new_aggr_col_exprs);

  int find_new_column_expr(ObIArray<UnionPushdownParam> &param,
                           ObIArray<ObRawExpr *> &new_column_exprs,
                           ObItemType type,
                           uint64_t col_id,
                           ObColumnRefRawExpr *&col_ref_exp);

  int replace_aggr_and_aggr_col_exprs(
      ObSelectStmt *stmt,
      ObIArray<ObRawExpr *> &old_aggr_exprs,
      ObIArray<ObRawExpr *> &new_aggr_exprs,
      ObIArray<ObRawExpr *> &old_aggr_col_exprs,
      ObIArray<ObRawExpr *> &new_aggr_col_exprs);

  int check_push_down_into_join_validity(ObSelectStmt *stmt, bool &is_valid);

  int compute_push_down_param(ObSelectStmt *stmt,
                              ObIArray<PushDownParam> &params,
                              const ObGroupByPlacementHint *hint,
                              ObIArray<uint64_t> &flatten_joined_tables,
                              bool &is_valid);
  int merge_tables_by_aggr_exprs(ObSelectStmt *stmt,
                                 ObIArray<PushDownParam> &params,
                                 const ObGroupByPlacementHint *hint,
                                 bool &is_valid);
  int merge_tables_by_join_conds(ObSelectStmt *stmt,
                                 ObIArray<PushDownParam> &params,
                                 const ObGroupByPlacementHint *hint,
                                 bool &is_valid);
  int merge_cross_join_tables_by_joined_tables(ObSelectStmt *stmt,
                                  ObIArray<PushDownParam> &cross_join_params);
  int merge_tables_by_joined_tables(ObSelectStmt *stmt,
                                    ObIArray<PushDownParam> &params,
                                    const ObGroupByPlacementHint *hint,
                                    ObIArray<uint64_t> &flatten_joined_tables,
                                    bool &is_valid);

  int check_groupby_validity(const ObSelectStmt &stmt, bool &is_valid);

  int check_collation_validity(const ObDMLStmt &stmt, bool &is_valid);

  int is_filterable_join(ObSelectStmt *stmt,
                         ObRawExpr *join_cond,
                         ObIArray<PushDownParam> &params,
                         bool &is_valid);

  int check_join_expr_validity(ObSelectStmt *stmt,
                               ObIArray<PushDownParam> &params,
                               ObRawExpr *expr,
                               bool &is_valid);

  int is_lob_filter(ObRawExpr *expr, bool &has);

  int check_outer_join_aggr(ObSelectStmt *stmt,
                            JoinedTable *joined_table,
                            bool &is_valid);

  int get_null_side_tables(ObDMLStmt &stmt,
                           JoinedTable &joined_table,
                           ObSqlBitSet<> &table_set);

  int do_groupby_push_down_into_join(ObSelectStmt *stmt,
                           ObIArray<PushDownParam> &params,
                           ObIArray<PushDownParam> &cross_join_params,
                           ObSqlBitSet<> &outer_table_set,
                           ObIArray<uint64_t> &flatten_joined_tables,
                           ObSelectStmt *&trans_stmt,
                           ObCostBasedPushDownCtx &push_down_ctx,
                           bool has_cross_join,
                           bool &trans_happened);
  int distribute_stmt_context_to_params(ObSelectStmt *stmt,
                                        ObIArray<PushDownParam> &params,
                                        ObSqlBitSet<> &outer_table_set,
                                        ObIArray<uint64_t> &flatten_joined_tables,
                                        ObSelectStmt *&trans_stmt,
                                        bool &is_valid);
  int merge_params_by_cross_joins(ObSelectStmt *stmt,
                                  ObIArray<PushDownParam> &params,
                                  ObIArray<PushDownParam> &cross_join_params,
                                  ObSelectStmt *&trans_stmt,
                                  bool &is_valid,
                                  bool &force_rewrite);
  int distribute_group_aggr_to_cross_joins(ObIArray<PushDownParam> &cross_join_params);

  int distribute_group_aggr(ObSelectStmt *stmt,
                            ObIArray<PushDownParam> &params);

  int distribute_filter(ObSelectStmt *stmt,
                        ObIArray<PushDownParam> &params,
                        ObSqlBitSet<> &outer_table_set,
                        ObRawExpr *cond);

  int distribute_joined_on_conds(ObDMLStmt *stmt,
                                 ObIArray<PushDownParam> &params,
                                 JoinedTable *joined_table);

  int do_double_eager_rewrite(ObSelectStmt *stmt,
                                  ObIArray<uint64_t> &flatten_joined_tables,
                                  ObSqlBitSet<> &outer_join_tables,
                                  ObCostBasedPushDownCtx &push_down_ctx,
                                  ObIArray<PushDownParam> &params);
  int push_down_groupby_into_cross_join(ObSelectStmt *stmt,
                                        ObIArray<uint64_t> &flatten_joined_tables,
                                        ObSqlBitSet<> &outer_join_tables,
                                        ObCostBasedPushDownCtx &push_down_ctx,
                                        ObIArray<PushDownParam> &cross_join_params);

  int push_down_group_by_into_view(ObSelectStmt *stmt,
                                   const ObIArray<TableItem*> &table_items,
                                   ObIArray<uint64_t> &flatten_joined_tables,
                                   PushDownParam &params,
                                   TableItem *&new_table_item);

  int transform_aggregation_expr(ObDMLStmt &stmt,
                                 ObAggFunRawExpr &aggr_expr,
                                 ObIArray<TableItem *> &eager_aggr_views,
                                 ObIArray<bool> &table_types,
                                 ObRawExpr *&new_aggr_expr);

  int get_count_star(ObDMLStmt &stmt,
                     TableItem *table_item,
                     bool is_outer_join_table,
                     ObRawExpr *&count_column);

  int update_joined_table(TableItem *table,
                          const TableItem *old_table,
                          TableItem *new_table,
                          bool &is_found);

  int check_unique(ObSelectStmt *stmt,
                   PushDownParam &param,
                   bool &is_unique);

  int add_exprs(const ObIArray<ObRawExpr *> &exprs,
                ObSqlBitSet<> &table_set,
                ObIArray<ObRawExpr *> &dest);

  int merge_tables(ObIArray<PushDownParam> &params, const ObSqlBitSet<> &table_set);

  int check_nl_operator(ObLogicalOperator *op, 
                        ObCostBasedPushDownCtx *push_down_ctx, 
                        bool &is_valid);

  int has_group_by_op(ObLogicalOperator *op,
                      bool &bret);

  int check_cut_ratio(ObLogicalOperator *op,
                      ObCostBasedPushDownCtx *push_down_ctx,
                      bool &is_valid);
  int check_all_cut_ratio(ObLogicalOperator *op,
                          ObCostBasedPushDownCtx *push_down_ctx,
                          bool is_in_cartesian,
                          ObIArray<bool> &invalid_stmts);
  int check_single_cut_ratio(ObLogicalOperator *op,
                             bool &is_valid);
  int compute_group_by_cut_ratio(ObLogicalOperator *op,
                                 double &cut_ratio);

  int check_group_by_subset(ObRawExpr *expr, const ObIArray<ObRawExpr *> &group_exprs, bool &bret);

  int get_transed_table(ObIArray<PushDownParam> &params, ObIArray<TableItem *> &tables);

  int get_tables_from_params(ObDMLStmt &stmt,
                             ObIArray<PushDownParam> &params, 
                             ObIArray<PushDownParam> &cross_join_params, 
                             ObIArray<ObSEArray<TableItem *, 4>> &trans_tables,
                             const bool force_rewrite,
                             bool disassemble_join = true);

  int get_tables_from_params(ObDMLStmt &stmt, 
                             ObIArray<PushDownParam> &params,
                             ObIArray<ObSEArray<TableItem *, 4>> &trans_tables,
                             bool disassemble_join = true);
  int check_hint_valid(ObDMLStmt &stmt,
                       ObIArray<PushDownParam> &params,
                       bool &hint_force_pushdown,
                       bool &is_valid);
  /* whether sum(c1) split to sum(count(*) * c1)
   * YES case: 1 + 1 + 1 + 2 + 2 + 2 = 3 * 1 + 3 * 2
   * NO case: 0.1 + 0.1 + 0.1 + 0.1 + 0.1 + 0.1 != 6 * 0.1
  */
  bool can_sum_trans_to_sum_count(const ObRawExpr *expr);
private:
  // help functions
  int64_t get_count_sum_num(const ObIArray<ObRawExpr *> &exprs)
  {
    int64_t num = 0;
    for (int64_t i = 0; i < exprs.count(); ++i) {
      if (OB_ISNULL(exprs.at(i))) {
        // do nothing
      } else if (exprs.at(i)->get_expr_type() == T_FUN_SUM ||
                 exprs.at(i)->get_expr_type() == T_FUN_COUNT) {
        ++num;
      }
    }
    return num;
  }

  int64_t get_valid_eager_aggr_num(const ObIArray<PushDownParam> &params)
  {
    int64_t num = 0;
    for (int64_t i = 0; i < params.count(); ++i) {
      if (!params.at(i).table_bit_index_.is_empty()) {
        ++ num;
      }
    }
    return num;
  }

  // TODO 这个函数在更新的版本中已经存在于ObTransformUtils里面了，但是这个版本还没有
  // 所以先自己写一个，合并的时候再处理
  int create_aggr_expr(ObTransformerCtx *ctx, ObItemType type,
                       ObAggFunRawExpr *&agg_expr, ObRawExpr *child_expr);

};

}
}

#endif // OB_TRANSFORM_GROUPBY_REPLACEMENT_H
