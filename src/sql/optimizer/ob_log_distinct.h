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

#ifndef OCEANBASE_SQL_OPTIMITZER_OB_LOG_DISTINCT_
#define OCEANBASE_SQL_OPTIMITZER_OB_LOG_DISTINCT_
#include "lib/container/ob_se_array.h"
#include "sql/optimizer/ob_logical_operator.h"

namespace oceanbase {
namespace sql {
class ObLogDistinct : public ObLogicalOperator {
public:
  ObLogDistinct(ObLogPlan& plan) : ObLogicalOperator(plan), algo_(AGGREGATE_UNINITIALIZED), is_block_mode_(false)
  {}
  virtual ~ObLogDistinct()
  {}

  const char* get_name() const;

  virtual int copy_without_child(ObLogicalOperator*& out);
  virtual int allocate_exchange_post(AllocExchContext* ctx) override;
  int push_down_distinct(
      AllocExchContext* ctx, common::ObIArray<OrderItem>& sort_keys, ObLogicalOperator*& exchange_point);
  int print_my_plan_annotation(char* buf, int64_t& buf_len, int64_t& pos, ExplainType type);
  // this interface can be used for adding distinct expr
  inline ObIArray<ObRawExpr*>& get_distinct_exprs()
  {
    return distinct_exprs_;
  }
  inline const ObIArray<ObRawExpr*>& get_distinct_exprs() const
  {
    return distinct_exprs_;
  }
  int add_distinct_expr(ObRawExpr* expr)
  {
    return distinct_exprs_.push_back(expr);
  }
  int set_distinct_exprs(common::ObIArray<ObRawExpr*>& exprs)
  {
    return append(distinct_exprs_, exprs);
  }
  virtual int inner_replace_generated_agg_expr(
      const common::ObIArray<std::pair<ObRawExpr*, ObRawExpr*> >& to_replace_exprs);
  uint64_t hash(uint64_t seed) const;

  inline void set_hash_type()
  {
    algo_ = HASH_AGGREGATE;
  }
  inline void set_merge_type()
  {
    algo_ = MERGE_AGGREGATE;
  }
  inline void set_algo_type(AggregateAlgo type)
  {
    algo_ = type;
  }
  inline AggregateAlgo get_algo() const
  {
    return algo_;
  }
  inline void set_block_mode(bool is_block_mode)
  {
    is_block_mode_ = is_block_mode;
  }
  inline bool get_block_mode()
  {
    return is_block_mode_;
  }
  virtual int est_cost() override;
  virtual int re_est_cost(const ObLogicalOperator* parent, double need_row_count, bool& re_est) override;
  virtual int transmit_op_ordering();
  virtual bool is_block_op() const override
  {
    return false;
  }
  virtual int compute_fd_item_set() override;

  virtual int allocate_granule_post(AllocGIContext& ctx) override;
  virtual int allocate_granule_pre(AllocGIContext& ctx) override;
  virtual int allocate_expr_pre(ObAllocExprContext& ctx) override;
  virtual int compute_op_ordering() override;
  virtual int generate_link_sql_pre(GenLinkStmtContext& link_ctx) override;

private:
  common::ObSEArray<ObRawExpr*, 16, common::ModulePageAllocator, true> distinct_exprs_;
  AggregateAlgo algo_;
  bool is_block_mode_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogDistinct);
};

}  // namespace sql
}  // namespace oceanbase
#endif
