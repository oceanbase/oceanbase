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

#ifndef OCEANBASE_SQL_OB_LOG_SORT_H
#define OCEANBASE_SQL_OB_LOG_SORT_H
#include "lib/container/ob_array.h"
#include "sql/optimizer/ob_logical_operator.h"

namespace oceanbase {
namespace sql {
class ObRawExpr;
class ObLogSort;
class ObLogSort : public ObLogicalOperator {
  // for calc pushed down sort cost
  static constexpr double DISTRIBUTED_SORT_COST_RATIO = .8;

public:
  ObLogSort(ObLogPlan& plan)
      : ObLogicalOperator(plan),
        sort_keys_(),
        topn_count_(NULL),
        minimum_row_count_(0),
        topk_precision_(0),
        prefix_pos_(0),
        is_local_merge_sort_(false),
        topk_limit_count_(NULL),
        topk_offset_count_(NULL),
        is_fetch_with_ties_(false)
  {}
  virtual ~ObLogSort()
  {}
  int calc_cost();
  virtual int est_cost() override;
  virtual int re_est_cost(const ObLogicalOperator* parent, double need_row_count, bool& re_est) override;
  const common::ObIArray<OrderItem>& get_sort_keys() const
  {
    return sort_keys_;
  }
  common::ObIArray<OrderItem>& get_sort_keys()
  {
    return sort_keys_;
  }

  int get_sort_exprs(common::ObIArray<ObRawExpr*>& sort_exprs);
  int clone_sort_keys_for_topk(common::ObIArray<OrderItem>& topk_sort_keys,
      const common::ObIArray<std::pair<ObRawExpr*, ObRawExpr*>>& push_down_avg_arr);

  inline void set_topn_count(ObRawExpr* expr)
  {
    topn_count_ = expr;
  }
  inline void set_prefix_pos(int64_t prefix_pos)
  {
    prefix_pos_ = prefix_pos;
  }
  inline void set_local_merge_sort(bool is_local_merge_sort)
  {
    is_local_merge_sort_ = is_local_merge_sort;
  }
  inline void set_fetch_with_ties(bool is_fetch_with_ties)
  {
    is_fetch_with_ties_ = is_fetch_with_ties;
  }

  // check if the current sort is a pushed down
  inline bool is_prefix_sort() const
  {
    return prefix_pos_ != 0;
  }
  inline bool is_local_merge_sort() const
  {
    return is_local_merge_sort_;
  }
  inline bool is_fetch_with_ties() const
  {
    return is_fetch_with_ties_;
  }
  inline int64_t get_prefix_pos() const
  {
    return prefix_pos_;
  }
  inline ObRawExpr* get_topn_count() const
  {
    return topn_count_;
  }
  inline ObRawExpr* get_topk_limit_count()
  {
    return topk_limit_count_;
  }
  inline ObRawExpr* get_topk_offset_count()
  {
    return topk_offset_count_;
  }
  virtual int copy_without_child(ObLogicalOperator*& out);
  // @brief Set the sorting columns
  int set_sort_keys(const common::ObIArray<OrderItem>& order_keys);
  int check_prefix_sort();
  int check_local_merge_sort();
  int add_sort_key(const OrderItem& key);
  int gen_filters();
  int gen_output_columns();
  int allocate_exchange_post(AllocExchContext* ctx) override;
  int allocate_exchange(AllocExchContext* ctx, ObExchangeInfo& exch_info) override;
  int push_down_sort(ObLogicalOperator* consumer_exc);
  virtual int allocate_expr_pre(ObAllocExprContext& ctx) override;
  virtual uint64_t hash(uint64_t seed) const;
  int check_output_dep_specific(ObRawExprCheckDep& checker);
  virtual const char* get_name() const;
  int set_topk_params(
      ObRawExpr* limit_count, ObRawExpr* limit_offset, int64_t minimum_row_cuont, int64_t topk_precision);
  inline int64_t get_minimum_row_count() const
  {
    return minimum_row_count_;
  }
  inline int64_t get_topk_precision() const
  {
    return topk_precision_;
  }
  virtual int transmit_op_ordering();
  virtual bool is_block_op() const override
  {
    return !is_prefix_sort();
  }
  virtual int compute_op_ordering() override;
  virtual int inner_append_not_produced_exprs(ObRawExprUniqueSet& raw_exprs) const;
  virtual int generate_link_sql_pre(GenLinkStmtContext& link_ctx) override;

protected:
  virtual int inner_replace_generated_agg_expr(
      const common::ObIArray<std::pair<ObRawExpr*, ObRawExpr*>>& to_replace_exprs);

private:
  virtual int print_my_plan_annotation(char* buf, int64_t& buf_len, int64_t& pos, ExplainType type);

private:
  common::ObSEArray<OrderItem, 8, common::ModulePageAllocator, true> sort_keys_;
  ObRawExpr* topn_count_;
  int64_t minimum_row_count_;
  int64_t topk_precision_;
  int64_t prefix_pos_;  //  for prefix_sort
  bool is_local_merge_sort_;
  ObRawExpr* topk_limit_count_;
  ObRawExpr* topk_offset_count_;
  bool is_fetch_with_ties_;
};
}  // end of namespace sql
}  // end of namespace oceanbase

#endif  // OCEANBASE_SQL_OB_LOG_SORT_H
