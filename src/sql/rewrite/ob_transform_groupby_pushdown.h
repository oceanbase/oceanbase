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

#ifndef OB_TRANSFORM_GROUPBY_PUSHDWON_H
#define OB_TRANSFORM_GROUPBY_PUSHDWON_H

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
    ObSEArray<ObRawExpr *, 4> join_columns_;   /// filter processed 'when or after' join
    ObSEArray<ObRawExpr *, 4> filter_exprs_; /// filter processed 'before' join
    ObSEArray<JoinedTable *, 4> correlated_joined_tables_; // filter exprs correlated joined tables
    TO_STRING_KV(K_(table_bit_index),
                 K_(group_exprs),
                 K_(aggr_exprs),
                 K_(join_columns),
                 K_(filter_exprs),
                 K_(correlated_joined_tables));

    int merge(PushDownParam &other);

    void reset() {
      table_bit_index_.reset();
      group_exprs_.reset();
      aggr_exprs_.reset();
      join_columns_.reset();
      filter_exprs_.reset();
      correlated_joined_tables_.reset();
    }
  };

  struct ObCostBasedPushDownCtx {
    ObCostBasedPushDownCtx() {};
    int64_t stmt_id_;
    ObSqlBitSet<> new_table_relids_;
  };

  int check_groupby_push_down_validity(ObSelectStmt *stmt, bool &is_valid);

  int compute_push_down_param(ObSelectStmt *stmt,
                              ObIArray<PushDownParam> &params,
                              ObIArray<uint64_t> &flattern_joined_tables,
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

  int do_groupby_push_down(ObSelectStmt *stmt,
                           ObIArray<PushDownParam> &params,
                           ObIArray<uint64_t> &flattern_joined_tables,
                           ObSelectStmt *&trans_stmt,
                           ObCostBasedPushDownCtx &push_down_ctx,
                           bool &trans_happend);

  int distribute_group_aggr(ObSelectStmt *stmt,
                            ObIArray<PushDownParam> &params);

  int distribute_filter(ObSelectStmt *stmt,
                        ObIArray<PushDownParam> &params,
                        ObSqlBitSet<> &outer_table_set,
                        ObRawExpr *cond);

  int distribute_joined_on_conds(ObDMLStmt *stmt,
                                 ObIArray<PushDownParam> &params,
                                 JoinedTable *joined_table);

  int transform_groupby_push_down(ObSelectStmt *stmt,
                                  ObIArray<uint64_t> &flattern_joined_tables,
                                  ObSqlBitSet<> &outer_join_tables,
                                  ObCostBasedPushDownCtx &push_down_ctx,
                                  ObIArray<PushDownParam> &params);

  int push_down_group_by_into_view(ObSelectStmt *stmt,
                                   const ObIArray<TableItem*> &table_items,
                                   ObIArray<uint64_t> &flattern_joined_tables,
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

  int check_group_by_subset(ObRawExpr *expr, const ObIArray<ObRawExpr *> &group_exprs, bool &bret);

  int get_transed_table(ObIArray<PushDownParam> &params, ObIArray<TableItem *> &tables);

  int get_tables_from_params(ObDMLStmt &stmt, 
                             ObIArray<PushDownParam> &params, 
                             ObIArray<ObSEArray<TableItem *, 4>> &trans_tables,
                              bool disassemble_join = true);
  int check_hint_valid(ObDMLStmt &stmt,
                       ObIArray<PushDownParam> &params,
                       bool &hint_force_pushdown,
                       bool &is_valid);

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

};

}
}

#endif // OB_TRANSFORM_GROUPBY_REPLACEMENT_H
