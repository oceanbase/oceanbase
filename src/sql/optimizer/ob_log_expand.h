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
using DupRawExprPair = ObTuple<ObRawExpr *, ObRawExpr *>;

class ObLogExpand : public ObLogicalOperator
{
public:
  ObLogExpand(ObLogPlan &plan) : ObLogicalOperator(plan), grouping_id_expr_(nullptr)
  {}
  virtual ~ObLogExpand()
  {}
  virtual int est_cost() override;
  virtual int do_re_est_cost(EstimateCostInfo &param, double &card, double &op_cost,
                             double &cost) override;
  virtual int get_plan_item_info(PlanText &plan_text, ObSqlPlanItem &plan_item) override;
  virtual int get_op_exprs(ObIArray<ObRawExpr *> &all_exprs) override;

  int set_expand_exprs(const ObIArray<ObRawExpr *> &rollup_exprs)
  {
    return expand_exprs_.assign(rollup_exprs);
  }

  int set_groupby_exprs(const ObIArray<ObRawExpr *> &gby_exprs)
  {
    return gby_exprs_.assign(gby_exprs);
  }

  int set_dup_expr_pairs(const ObIArray<DupRawExprPair> &dup_pairs)
  {
    return dup_expr_pairs_.assign(dup_pairs);
  }
  void set_grouping_id(ObOpPseudoColumnRawExpr *grouping_id_expr)
  {
    grouping_id_expr_ = grouping_id_expr;
  }

  const ObOpPseudoColumnRawExpr *get_grouping_id() const
  {
    return grouping_id_expr_;
  }
  ObOpPseudoColumnRawExpr *&get_grouping_id()
  {
    return grouping_id_expr_;
  }
  const ObIArray<ObRawExpr *> &get_expand_exprs() const
  {
    return expand_exprs_;
  }
  const ObIArray<ObRawExpr *> &get_gby_exprs() const
  {
    return gby_exprs_;
  }
  virtual int is_my_fixed_expr(const ObRawExpr *expr, bool &is_fixed) override
  {
    int ret = OB_SUCCESS;
    is_fixed = (expr == grouping_id_expr_);
    for (int i = 0; !is_fixed && i < expand_exprs_.count(); i++) {
      is_fixed = (expr == expand_exprs_.at(i));
    }
    for (int i = 0; !is_fixed && i < dup_expr_pairs_.count(); i++) {
      is_fixed = (expr == dup_expr_pairs_.at(i).element<1>());
    }
    return ret;
  }

  virtual int inner_replace_op_exprs(ObRawExprReplacer &replacer) override;

  virtual int compute_const_exprs() override;

  virtual int compute_equal_set() override;

  virtual int compute_fd_item_set() override;

  virtual int compute_one_row_info() override;

  virtual int compute_op_ordering() override;

  int gen_hash_rollup_info(ObHashRollupInfo &info);
  const ObIArray<ObTuple<ObRawExpr *, ObRawExpr *>> &get_dup_expr_pairs() const
  {
    return dup_expr_pairs_;
  }

  static int gen_expand_exprs(ObRawExprFactory &factory, ObSQLSessionInfo *sess,
                              ObIArray<ObExprConstraint> &constraints,
                              ObIArray<ObRawExpr *> &rollup_exprs,
                              ObIArray<ObRawExpr *> &gby_exprs,
                              ObIArray<DupRawExprPair> &dup_expr_pairs);

  static int dup_and_replace_exprs_within_aggrs(ObRawExprFactory &factory, ObSQLSessionInfo *sess,
                                                ObIArray<ObExprConstraint> &constraints,
                                                const ObIArray<ObRawExpr *> &rollup_exprs,
                                                const ObIArray<ObAggFunRawExpr *> &aggr_items,
                                                ObIArray<DupRawExprPair> &dup_expr_pairs);

  static int unshare_constraints(ObRawExprCopier &copier, ObIArray<ObExprConstraint> &constraints);
  TO_STRING_KV(K_(expand_exprs));

private:
  static int find_expr(ObRawExpr *root, const ObRawExpr *expected, bool &found);
  static int find_expr_within_aggr_item(ObAggFunRawExpr *aggr_item, const ObRawExpr *expected,
                                        bool &found);
  static int replace_expr_with_aggr_item(ObAggFunRawExpr *aggr_item, const ObRawExpr *from, ObRawExpr *to);
  static int replace_expr(ObRawExpr *&root, const ObRawExpr *from, ObRawExpr *to);

  int gen_duplicate_expr_text(PlanText &plan_text, ObIArray<ObRawExpr *> &exprs);
private:
  DISALLOW_COPY_AND_ASSIGN(ObLogExpand);
  common::ObSEArray<ObRawExpr *, 8, common::ModulePageAllocator, true> expand_exprs_;
  common::ObSEArray<ObRawExpr *, 8, common::ModulePageAllocator, true> gby_exprs_;
  common::ObSEArray<ObTuple<ObRawExpr *, ObRawExpr *>, 8, common::ModulePageAllocator, true>
    dup_expr_pairs_;
  ObOpPseudoColumnRawExpr *grouping_id_expr_;
};
} // namespace sql
} // end oceanbase
#endif