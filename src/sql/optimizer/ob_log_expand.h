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
  ObLogExpand(ObLogPlan &plan) : ObLogicalOperator(plan),hash_rollup_info_(nullptr)
  {}
  virtual ~ObLogExpand()
  {}
  virtual int est_cost() override;
  virtual int do_re_est_cost(EstimateCostInfo &param, double &card, double &op_cost,
                             double &cost) override;
  virtual int get_plan_item_info(PlanText &plan_text, ObSqlPlanItem &plan_item) override;
  virtual int get_op_exprs(ObIArray<ObRawExpr *> &all_exprs) override;

  inline void set_hash_rollup_info(ObHashRollupInfo *hash_rollup_info) { hash_rollup_info_ = hash_rollup_info; }

  ObHashRollupInfo *get_hash_rollup_info() { return hash_rollup_info_; }

  virtual int is_my_fixed_expr(const ObRawExpr *expr, bool &is_fixed) override;

  virtual int inner_replace_op_exprs(ObRawExprReplacer &replacer) override;

  virtual int compute_const_exprs() override;

  virtual int compute_equal_set() override;

  virtual int compute_fd_item_set() override;

  virtual int compute_one_row_info() override;

  virtual int compute_op_ordering() override;

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
  TO_STRING_KV(K(""));

private:
  static int find_expr(ObRawExpr *root, const ObRawExpr *expected, bool &found);
  static int find_expr_within_aggr_item(ObAggFunRawExpr *aggr_item, const ObRawExpr *expected,
                                        bool &found);
  static int replace_expr_with_aggr_item(ObAggFunRawExpr *aggr_item, const ObRawExpr *from, ObRawExpr *to);
  static int replace_expr(ObRawExpr *&root, const ObRawExpr *from, ObRawExpr *to);

  int gen_duplicate_expr_text(PlanText &plan_text, ObIArray<ObRawExpr *> &exprs);
private:
  DISALLOW_COPY_AND_ASSIGN(ObLogExpand);
  ObHashRollupInfo *hash_rollup_info_;
};
} // namespace sql
} // end oceanbase
#endif