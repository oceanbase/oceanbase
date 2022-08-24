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

#ifndef OCEANBASE_SQL_OB_LOG_WINDOW_FUNCTION_H
#define OCEANBASE_SQL_OB_LOG_WINDOW_FUNCTION_H
#include "sql/optimizer/ob_logical_operator.h"
#include "sql/optimizer/ob_log_set.h"
namespace oceanbase {
namespace sql {
class ObLogWindowFunction : public ObLogicalOperator {
public:
  ObLogWindowFunction(ObLogPlan& plan) : ObLogicalOperator(plan), is_parallel_(false)
  {}
  virtual ~ObLogWindowFunction()
  {}
  virtual int allocate_exchange_post(AllocExchContext* ctx) override;
  virtual int copy_without_child(ObLogicalOperator*& out) override;
  virtual int print_my_plan_annotation(char* buf, int64_t& buf_len, int64_t& pos, ExplainType type) override;

  inline int add_window_expr(ObWinFunRawExpr* win_expr)
  {
    return win_exprs_.push_back(win_expr);
  }
  inline ObIArray<ObWinFunRawExpr*>& get_window_exprs()
  {
    return win_exprs_;
  }
  virtual int est_cost() override;
  virtual int re_est_cost(const ObLogicalOperator* parent, double need_row_count, bool& re_est) override;
  virtual int transmit_op_ordering() override;
  virtual int allocate_expr_pre(ObAllocExprContext& ctx) override;
  virtual int check_output_dep_specific(ObRawExprCheckDep& checker) override;
  int is_my_window_expr(const ObRawExpr* expr, bool& is_mine);
  virtual bool is_block_op() const override;
  virtual uint64_t hash(uint64_t seed) const override;
  virtual int allocate_granule_post(AllocGIContext& ctx) override;
  virtual int allocate_granule_pre(AllocGIContext& ctx) override;
  int get_win_partition_intersect_exprs(ObIArray<ObWinFunRawExpr*>& win_exprs, ObIArray<ObRawExpr*>& win_part_exprs);
  virtual int inner_append_not_produced_exprs(ObRawExprUniqueSet& raw_exprs) const override;
  bool is_parallel() const
  {
    return is_parallel_;
  }
  int match_parallel_condition(bool& can_parallel);

private:
  ObSEArray<ObWinFunRawExpr*, 4> win_exprs_;
  bool is_parallel_;
};
}  // namespace sql
}  // namespace oceanbase
#endif  // OCEANBASE_SQL_OB_LOG_WINDOW_FUNCTION_H
