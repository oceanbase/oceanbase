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

#ifndef OCEANBASE_SQL_OB_LOG_GROUP_BY_H
#define OCEANBASE_SQL_OB_LOG_GROUP_BY_H
#include "lib/allocator/page_arena.h"
#include "ob_logical_operator.h"
#include "ob_select_log_plan.h"
namespace oceanbase {
namespace sql {
class ObLogSort;
class ObLogGroupBy : public ObLogicalOperator {
public:
  ObLogGroupBy(ObLogPlan& plan)
      : ObLogicalOperator(plan),
        group_exprs_(),
        rollup_exprs_(),
        aggr_exprs_(),
        avg_div_exprs_(),
        approx_count_distinct_estimate_ndv_exprs_(),
        algo_(AGGREGATE_UNINITIALIZED),
        distinct_card_(0.0),
        from_pivot_(false)
  {}
  virtual ~ObLogGroupBy()
  {}

  // const char* get_name() const;
  virtual int32_t get_explain_name_length() const override;
  virtual int get_explain_name_internal(char* buf, const int64_t buf_len, int64_t& pos) override;
  // Get the 'group-by' expressions
  inline common::ObIArray<ObRawExpr*>& get_group_by_exprs()
  {
    return group_exprs_;
  }
  // Get the 'rollup' expressions
  inline common::ObIArray<ObRawExpr*>& get_rollup_exprs()
  {
    return rollup_exprs_;
  }
  // Get the aggregate expressions
  inline common::ObIArray<ObRawExpr*>& get_aggr_funcs()
  {
    return aggr_exprs_;
  }
  inline bool has_rollup()
  {
    return rollup_exprs_.count() > 0;
  }
  virtual int copy_without_child(ObLogicalOperator*& out) override;
  inline void set_hash_type()
  {
    algo_ = HASH_AGGREGATE;
  }
  inline void set_merge_type()
  {
    algo_ = MERGE_AGGREGATE;
  }
  inline void set_scalar_type()
  {
    algo_ = SCALAR_AGGREGATE;
  }
  inline void set_algo_type(AggregateAlgo type)
  {
    algo_ = type;
  }
  inline AggregateAlgo get_algo() const
  {
    return algo_;
  }

  // @brief SET the GROUP-BY COLUMNS
  int set_group_by_exprs(const common::ObIArray<ObRawExpr*>& group_by_exprs);
  // @brief SET the ROLLUP COLUMNS
  int set_rollup_exprs(const common::ObIArray<ObRawExpr*>& rollup_exprs);
  int set_aggr_exprs(const common::ObIArray<ObAggFunRawExpr*>& aggr_exprs);
  ObSelectLogPlan* get_plan() override
  {
    return static_cast<ObSelectLogPlan*>(my_plan_);
  }
  int gen_filters();
  int gen_output_columns();
  virtual int allocate_expr_pre(ObAllocExprContext& ctx) override;
  bool is_my_aggr_expr(const ObRawExpr* expr);
  int allocate_exchange_post(AllocExchContext* ctx) override;
  int allocate_groupby_below(const ObIArray<ObRawExpr*>& distinct_exprs, const bool can_push,
      ObLogicalOperator*& exchange_point,
      common::ObIArray<std::pair<ObRawExpr*, ObRawExpr*> >& group_push_down_replaced_exprs);
  int check_can_pullup_gi(const ObLogicalOperator *op, bool &can_pullup);
  int should_push_down_group_by(AllocExchContext& ctx, ObIArray<ObRawExpr*>& distinct_exprs, bool& should_push_groupby,
      bool& should_push_distinct);
  virtual uint64_t hash(uint64_t seed) const override;
  virtual int check_output_dep_specific(ObRawExprCheckDep& checker) override;
  virtual int est_cost() override;
  virtual int re_est_cost(const ObLogicalOperator* parent, double need_row_count, bool& re_est) override;
  virtual int transmit_op_ordering() override;
  virtual bool is_block_op() const override
  {
    return MERGE_AGGREGATE != get_algo();
  }
  virtual int generate_link_sql_pre(GenLinkStmtContext& link_ctx) override;

  virtual int compute_const_exprs() override;
  virtual int compute_fd_item_set() override;
  virtual int compute_op_ordering() override;
  double get_distinct_card() const
  {
    return distinct_card_;
  }
  void set_distinct_card(const double distinct_card)
  {
    distinct_card_ = distinct_card;
  }
  bool from_pivot() const
  {
    return from_pivot_;
  }
  void set_from_pivot(const bool value)
  {
    from_pivot_ = value;
  }
  int get_group_rollup_exprs(common::ObIArray<ObRawExpr*>& group_rollup_exprs) const;
  int allocate_startup_expr_post() override;
  VIRTUAL_TO_STRING_KV(K_(group_exprs), K_(rollup_exprs), K_(aggr_exprs), K_(avg_div_exprs),
      K_(approx_count_distinct_estimate_ndv_exprs), K_(algo), K_(distinct_card));

private:
  /**
   *  Aggregation functions analysis for child group-by operator
   *
   *  During the traverse, we might need to create new expressions. ie. avg to sum, count
   */
  int push_down_aggr_exprs_analyze(common::ObIArray<ObRawExpr*>& aggr_exprs,
      common::ObIArray<std::pair<ObRawExpr*, ObRawExpr*> >& push_down_avg_arr);
  /**
   *  Aggregation functions analysis for parent group-by operator
   *
   *  For the parent group-by, new expressions will be generated and used to replace the
   *  one ones.
   */
  int pull_up_aggr_exprs_analyze(
      common::ObIArray<ObRawExpr*>& aggr_exprs, common::ObIArray<std::pair<ObRawExpr*, ObRawExpr*> >& ctx_record_arr);
  virtual int print_my_plan_annotation(char* buf, int64_t& buf_len, int64_t& pos, ExplainType type) override;

  int get_child_groupby_algorithm(const bool is_distinct, const bool can_push_down_distinct, const bool need_sort,
      const bool child_need_sort, AggregateAlgo& aggr_algo);
  int get_child_groupby_expected_ordering(
      const ObIArray<ObRawExpr*>& distinct_exprs, ObIArray<OrderItem>& expected_ordering);
  // int reset_cost(ObLogicalOperator *op);
  int produce_pushed_down_expressions(ObLogGroupBy* child_group_by,
      common::ObIArray<std::pair<ObRawExpr*, ObRawExpr*> >& ctx_record_arr,
      common::ObIArray<std::pair<ObRawExpr*, ObRawExpr*> >& push_down_arr);
  virtual int inner_replace_generated_agg_expr(
      const common::ObIArray<std::pair<ObRawExpr*, ObRawExpr*> >& to_replace_exprs) override;
  virtual int print_outline(planText& plan) override;
  int is_need_print_agg_type(planText& plan_text, const ObStmtHint& stmt_hint, bool& is_need);
  int alloc_topk_if_needed();
  int allocate_topk_if_needed(ObLogicalOperator* exchange_point, const ObLogGroupBy* child_group_by,
      const common::ObIArray<std::pair<ObRawExpr*, ObRawExpr*> >& push_down_avg_arr);

  virtual int allocate_granule_post(AllocGIContext& ctx) override;
  virtual int allocate_granule_pre(AllocGIContext& ctx) override;
  virtual int inner_append_not_produced_exprs(ObRawExprUniqueSet& raw_exprs) const override;

  int create_fd_item_from_select_list(ObFdItemSet* fd_item_set);
  virtual int compute_one_row_info() override;

private:
  // the 'group-by' expressions
  common::ObSEArray<ObRawExpr*, 8, common::ModulePageAllocator, true> group_exprs_;
  // the rollup expressions
  common::ObSEArray<ObRawExpr*, 8, common::ModulePageAllocator, true> rollup_exprs_;
  // the aggregate expressions
  common::ObSEArray<ObRawExpr*, 8, common::ModulePageAllocator, true> aggr_exprs_;
  common::ObSEArray<ObRawExpr*, 8, common::ModulePageAllocator, true> avg_div_exprs_;
  common::ObSEArray<ObRawExpr*, 8, common::ModulePageAllocator, true> approx_count_distinct_estimate_ndv_exprs_;
  AggregateAlgo algo_;
  // if no having clause, distinct_card_ = card_
  double distinct_card_;
  bool from_pivot_;
};
}  // end of namespace sql
}  // end of namespace oceanbase

#endif  // OCEANBASE_SQL_OB_LOG_GROUP_BY_H
