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

#ifndef _OB_TRANSFORM_SEMI_TO_INNER_H_
#define _OB_TRANSFORM_SEMI_TO_INNER_H_

#include "sql/rewrite/ob_transform_rule.h"
#include "sql/resolver/dml/ob_dml_stmt.h"

namespace oceanbase
{
namespace sql
{

struct ObCostBasedRewriteCtx {
  uint64_t table_id_;
  bool is_multi_join_cond_;
  bool hint_force_;
  ObSEArray<uint64_t,4> view_table_id_;
  ObCostBasedRewriteCtx()
  :table_id_(OB_INVALID_ID),
  is_multi_join_cond_(false),
  hint_force_(false)
  {}
};

class ObTransformSemiToInner : public ObTransformRule
{
public:
  ObTransformSemiToInner(ObTransformerCtx *ctx)
    : ObTransformRule(ctx, TransMethod::POST_ORDER, T_SEMI_TO_INNER)
  {}
  virtual ~ObTransformSemiToInner() {}
  virtual int transform_one_stmt(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                 ObDMLStmt *&stmt,
                                 bool &trans_happened) override;
  virtual int construct_transform_hint(ObDMLStmt &stmt, void *trans_params) override;
protected:
  int is_expected_plan(ObLogPlan *plan, void *check_ctx, bool is_trans_plan, bool &is_valid) override;

private:
  enum TransformFlag {TO_INNER = 1, TO_AGGR_INNER = 2, TO_INNER_GBY = 4};
  struct TransformParam {
    TransformParam() : transform_flag_(0), need_add_distinct_(false), need_add_limit_constraint_(false),
          right_table_need_add_limit_(false), need_add_gby_(false), cmp_join_cond_(NULL),
          cmp_right_expr_(NULL), need_spj_view_(false) {}

    void set_transform_flag(TransformFlag flag) { transform_flag_ |= flag; }

    bool use_inner() { return (transform_flag_ & TO_INNER) != 0; }

    bool use_aggr_inner() { return (transform_flag_ & TO_AGGR_INNER) != 0; }

    bool use_inner_gby() { return (transform_flag_ & TO_INNER_GBY) != 0; }

    TO_STRING_KV(K_(transform_flag), K_(need_add_distinct), K_(need_add_limit_constraint), K_(right_table_need_add_limit), K_(need_add_gby), K_(need_spj_view), K_(cmp_join_cond), K_(cmp_right_expr));

    // record rewrite form, note that rewrite choices are mutually exclusive (select at most one)
    int64_t transform_flag_;

    // collection of param exprs related to left tables for equal join conditions
    ObSEArray<ObRawExpr*, 4> equal_left_exprs_;

    // collection of param exprs related to right table for equal join conditions (correspond to the left exprs one by one)
    ObSEArray<ObRawExpr*, 4> equal_right_exprs_;

    // for INNER form, mark if a distinct view is needed to eliminate possible duplicates
    bool need_add_distinct_;

    // for INNER form, whether const param constraint need to be added to context
    // (plan cache only hits for certain const value in LIMIT clause)
    bool need_add_limit_constraint_;

    bool right_table_need_add_limit_;

    // for AGGR INNER form, group by clause is needed to deal with equal join conds
    bool need_add_gby_;

    // for AGGR INNER form, crucial for the choice of aggregation function
    ObRawExpr* cmp_join_cond_;

    // for AGGR INNER form, record param expr related to right table of cmp_join_cond_
    ObRawExpr* cmp_right_expr_;

    // for AGGR INNER form, filter conditions on right table should be left in the view
    ObSEArray<ObRawExpr*, 4> filter_conds_on_right_;

    // for INNER GBY form, a column group with "unique" property on from items is needed in group by clause
    ObSEArray<ObRawExpr*, 4> unique_column_groups_;

    // for INNER GBY form, spj view is needed for adding group by clause
    bool need_spj_view_;
  };

  int collect_param_expr_related_to_right_table(ObDMLStmt& stmt,
                                                SemiInfo& semi_info,
                                                ObRawExpr* correlated_condition,
                                                ObRawExpr*& param_expr_related_to_right_table);

  int collect_filter_conds_related_to_right_table(ObDMLStmt& stmt,
                                                  SemiInfo& semi_info,
                                                  ObIArray<ObRawExpr*>& filter_conds,
                                                  ObIArray<ObRawExpr*>& filter_conds_on_right);
  int transform_semi_to_inner(ObDMLStmt* root_stmt,
                              ObDMLStmt* stmt,
                              const SemiInfo* pre_semi_info,
                              ObDMLStmt*& trans_stmt,
                              ObCostBasedRewriteCtx &ctx,
                              bool& need_check_cost,
                              bool& trans_happened,
                              bool& spj_view_added);

  int check_basic_validity(ObDMLStmt* root_stmt,
                           ObDMLStmt& stmt,
                           SemiInfo& semi_info,
                           ObCostBasedRewriteCtx &ctx,
                           bool& is_valid,
                           bool& need_check_cost,
                           TransformParam& trans_param);

  int check_query_from_dual(ObSelectStmt *stmt, bool& query_from_dual);

  bool is_less_or_greater_expr(ObItemType expr_type);

  int check_right_exprs_unique(ObDMLStmt& stmt,
                               TableItem* right_table,
                               ObIArray<ObRawExpr*>& right_exprs,
                               bool& is_unique);

  int check_semi_join_condition(ObDMLStmt& stmt,
                                SemiInfo& semi_info,
                                ObIArray<ObRawExpr*>& equal_left_exprs,
                                ObIArray<ObRawExpr*>& equal_right_exprs,
                                bool& is_all_left_filter,
                                bool& is_multi_join_cond,
                                int64_t& cmp_join_conds_count,
                                int64_t& invalid_conds_count,
                                int64_t& other_conds_count);

  int gather_params_by_rewrite_form(ObDMLStmt* trans_stmt,
                                    SemiInfo* semi_info,
                                    TransformParam& trans_param);

  int do_transform_by_rewrite_form(ObDMLStmt* stmt, SemiInfo* semi_info, ObCostBasedRewriteCtx &ctx, TransformParam& trans_param);

  int split_join_condition(ObDMLStmt& stmt,
                           SemiInfo& semi_info,
                           ObIArray<ObRawExpr*>& equal_join_conds,
                           ObIArray<ObRawExpr*>& cmp_join_conds,
                           ObIArray<ObRawExpr*>& filter_conds,
                           ObIArray<ObRawExpr*>& invalid_conds,
                           ObIArray<ObRawExpr*>& other_conds,
                           bool& has_multi_join_cond,
                           bool& is_all_left_filter);

  int collect_param_exprs_of_correlated_conds(ObDMLStmt& stmt,
                                              SemiInfo& semi_info,
                                              ObIArray<ObRawExpr*>& correlated_conds,
                                              ObIArray<ObRawExpr*>& left_exprs,
                                              ObIArray<ObRawExpr*>& right_exprs,
                                              bool collect_equal_info = false);

  int check_right_table_output_one_row(TableItem &right_table,
                                       bool &is_one_row);
  int do_transform_with_aggr(ObDMLStmt& stmt, SemiInfo* semi_info, ObCostBasedRewriteCtx &ctx, TransformParam& trans_param);

  int create_min_max_aggr_expr(ObDMLStmt* stmt,
                               ObRawExprFactory* expr_factory,
                               ObRawExpr* condition_expr,
                               ObRawExpr* target_param_expr,
                               ObAggFunRawExpr*& aggr_expr);

  int add_group_by_with_cast(ObSelectStmt& view,
                            const ObIArray<ObRawExpr*>& left_exprs,
                            const ObIArray<ObRawExpr*>& right_exprs);
  int check_can_add_deduplication(const ObIArray<ObRawExpr*> &left_exprs,
                             const ObIArray<ObRawExpr*> &right_exprs,
                             bool &is_valid);

  int check_need_add_cast(const ObRawExpr *left_arg,
                          const ObRawExpr *right_arg,
                          bool &need_add_cast,
                          bool &is_valid);

  int check_join_condition_match_index(ObDMLStmt *root_stmt,
                                       ObDMLStmt &stmt,
                                       SemiInfo &semi_info,
                                       const common::ObIArray<ObRawExpr*> &semi_conditions,
                                       bool &is_match_index);

  int do_transform(ObDMLStmt &stmt,
                   SemiInfo *semi_info,
                   ObCostBasedRewriteCtx &ctx,
                   TransformParam &trans_param);

  int find_basic_table(ObSelectStmt* stmt, uint64_t &table_id);

  int add_distinct(ObSelectStmt &view,
                   const ObIArray<ObRawExpr*> &left_exprs,
                   const ObIArray<ObRawExpr*> &right_exprs);

  int add_ignore_semi_info(const uint64_t semi_id);

  int is_ignore_semi_info(const uint64_t semi_id, bool &ignore);

  int find_operator(ObLogicalOperator* root,
                    ObIArray<ObLogicalOperator*> &parents,
                    uint64_t table_id,
                    ObLogicalOperator *&table_op);

  int check_is_semi_condition(ObIArray<ObExecParamRawExpr *> &nl_params,
                              ObIArray<uint64_t> & table_ids,
                              bool &is_valid);
    int check_hint_valid(const ObDMLStmt &stmt, 
                        const TableItem& table,
                        bool &force_trans,
                        bool &force_no_trans) const;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTransformSemiToInner);
};

}
}

#endif /* _OB_TRANSFORM_SEMI_TO_INNER_H_ */
