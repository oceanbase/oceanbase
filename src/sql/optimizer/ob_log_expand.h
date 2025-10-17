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

#ifndef OCEANBASE_SQL_OB_LOG_EXPAND_H_
#define OCEANBASE_SQL_OB_LOG_EXPAND_H_

#include "sql/optimizer/ob_logical_operator.h"
#include "lib/container/ob_tuple.h"

namespace oceanbase
{
namespace sql
{
class ObHashRollupInfo;
class ObGroupingSetInfo;
using DupRawExprPair = ObTuple<ObRawExpr *, ObRawExpr *>;

class ObLogExpand : public ObLogicalOperator
{
public:
  ObLogExpand(ObLogPlan &plan) : ObLogicalOperator(plan),grouping_set_info_(nullptr)
  {}
  virtual ~ObLogExpand()
  {}
  virtual int est_cost() override;
  virtual int do_re_est_cost(EstimateCostInfo &param, double &card, double &op_cost,
                             double &cost) override;
  virtual int get_plan_item_info(PlanText &plan_text, ObSqlPlanItem &plan_item) override;
  virtual int get_op_exprs(ObIArray<ObRawExpr *> &all_exprs) override;


  inline void set_grouping_set_info(ObGroupingSetInfo *grouping_set_info) { grouping_set_info_ = grouping_set_info; }

  ObGroupingSetInfo *get_grouping_set_info() { return grouping_set_info_; }

  virtual int is_my_fixed_expr(const ObRawExpr *expr, bool &is_fixed) override;

  virtual int inner_replace_op_exprs(ObRawExprReplacer &replacer) override;

  virtual int compute_const_exprs() override;

  virtual int compute_equal_set() override;

  virtual int compute_fd_item_set() override;

  virtual int compute_one_row_info() override;

  virtual int compute_op_ordering() override;

  static int dup_and_replace_exprs_within_aggrs(ObRawExprFactory &factory, ObSQLSessionInfo *sess,
                                                ObIArray<ObExprConstraint> &constraints,
                                                const ObIArray<ObRawExpr *> &rollup_exprs,
                                                const ObIArray<ObAggFunRawExpr *> &aggr_items,
                                                ObIArray<ObAggFunRawExpr *> &new_agg_items,
                                                ObIArray<DupRawExprPair> &dup_expr_pairs,
                                                ObIArray<DupRawExprPair> &replaced_aggr_pairs);

  static int unshare_constraints(ObRawExprCopier &copier, ObIArray<ObExprConstraint> &constraints);

  TO_STRING_KV(K(""));

private:
  static int create_aggr_with_dup_params(ObRawExprFactory &factory, ObAggFunRawExpr *aggr_item, ObSQLSessionInfo *sess,
                                         ObIArray<DupRawExprPair> &dup_expr_pairs,
                                         ObAggFunRawExpr *&new_agg);
  static int find_expr_within_aggr_item(ObAggFunRawExpr *aggr_item, const ObRawExpr *expected,
                                        bool &found);
  int gen_duplicate_expr_text(PlanText &plan_text, ObIArray<ObRawExpr *> &exprs);

  int gen_grouping_set_text(PlanText &plan_text, const int64_t n_group);
  int gen_pruned_grouping_set_text(PlanText &plan_text, ObIArray<ObRawExpr *> &pruned_gby_exprs);
  static int build_dup_expr(ObRawExprFactory &factory, ObSQLSessionInfo *sess,
                            ObRawExpr *input, ObIArray<DupRawExprPair> &dup_expr_pairs,
                            ObRawExpr *&output);

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogExpand);
  ObGroupingSetInfo *grouping_set_info_;
};
} // namespace sql
} // end oceanbase
#endif