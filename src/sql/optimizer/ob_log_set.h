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

#ifndef OCEANBASE_SQL_OB_LOG_SET_H
#define OCEANBASE_SQL_OB_LOG_SET_H
#include "sql/resolver/dml/ob_select_stmt.h"
#include "ob_logical_operator.h"
#include "ob_select_log_plan.h"
namespace oceanbase
{
namespace sql
{
struct ObBasicCostInfo;

class ObLogSet : public ObLogicalOperator
{
public:
  ObLogSet(ObLogPlan &plan):
      ObLogicalOperator(plan),
      is_distinct_(true),
      is_recursive_union_(false),
      is_breadth_search_(true),
      set_algo_(INVALID_SET_ALGO),
      set_dist_algo_(DIST_INVALID_METHOD),
      set_op_(ObSelectStmt::NONE),
      set_directions_(),
      identify_seq_expr_(nullptr)
  {
  }

  virtual ~ObLogSet()
  {
  }
  virtual int get_op_exprs(ObIArray<ObRawExpr*> &all_exprs) override;
  virtual int is_my_fixed_expr(const ObRawExpr *expr, bool &is_fixed) override;
  ObSelectLogPlan *get_left_plan() const;
  ObSelectLogPlan *get_right_plan() const;
  const ObSelectStmt *get_left_stmt() const;
  const ObSelectStmt *get_right_stmt() const;
  int get_my_set_exprs(ObIArray<ObRawExpr*> &set_exprs);
  const char *get_name() const;
  inline void assign_set_distinct(const bool is_distinct) { is_distinct_ = is_distinct; }
  inline void set_recursive_union(bool is_recursive_union) { is_recursive_union_ = is_recursive_union; }
  inline void set_is_breadth_search(bool is_breadth_search) { is_breadth_search_ = is_breadth_search; }
  inline bool is_recursive_union() { return is_recursive_union_; }
  inline bool is_breadth_search() { return is_breadth_search_; }
  inline bool is_set_distinct() const { return is_distinct_; }
  // 目前仅union支持先读完left然后读right，但merge_union的distinct不支持
  // 增加hash intersect 和hash except算子1by1的能力
  virtual bool is_consume_child_1by1() const
  { return (HASH_SET == set_algo_ || !is_distinct_); }
  //hash set 全部都是从left 建立hash表，0号孩子是block input
  virtual bool is_block_input(const int64_t child_idx) const override 
  {
    return HASH_SET == set_algo_ && 0 == child_idx && ObSelectStmt::UNION != get_set_op();
  }
  inline void assign_set_op(const ObSelectStmt::SetOperator set_op) { set_op_ = set_op; }
  inline ObSelectStmt::SetOperator get_set_op() const { return set_op_; }
  int calculate_sharding_info(ObIArray<ObRawExpr *> &left_keys,
                              ObIArray<ObRawExpr *> &right_keys,
                              ObShardingInfo &output_sharding);
  const common::ObIArray<ObOrderDirection> &get_set_directions() const { return set_directions_; }
  common::ObIArray<ObOrderDirection> &get_set_directions() { return set_directions_; }
  int set_set_directions(const common::ObIArray<ObOrderDirection> &directions) { return set_directions_.assign(directions); }
  int add_set_direction(const ObOrderDirection direction = default_asc_direction()) { return set_directions_.push_back(direction); }
  int get_set_exprs(ObIArray<ObRawExpr *> &set_exprs);
  int get_pure_set_exprs(ObIArray<ObRawExpr *> &set_exprs);
  virtual int est_cost() override;
  virtual int est_width() override;
  virtual int do_re_est_cost(EstimateCostInfo &param, double &card, double &op_cost, double &cost) override;
  int get_re_est_cost_infos(const EstimateCostInfo &param,
                            ObIArray<ObBasicCostInfo> &cost_infos,
                            double &child_cost,
                            double &card);
  int set_search_ordering(const common::ObIArray<OrderItem> &search_ordering);
  int set_cycle_items(const common::ObIArray<ColumnItem> &cycle_items);
  virtual uint64_t hash(uint64_t seed) const override;
  const common::ObIArray<OrderItem> &get_search_ordering() { return search_ordering_; }
  const common::ObIArray<ColumnItem> &get_cycle_items() { return cycle_items_; }
  ObRawExpr *get_identify_seq_expr() { return identify_seq_expr_; }
  inline void set_identify_seq_expr(ObRawExpr *expr) { identify_seq_expr_ = expr; }
  virtual int compute_const_exprs() override;
  virtual int compute_equal_set() override;
  virtual int compute_fd_item_set() override;
  virtual int deduce_const_exprs_and_ft_item_set(ObFdItemSet &fd_item_set) override;

  virtual int compute_op_ordering() override;
  virtual int compute_one_row_info() override;
  virtual int compute_sharding_info() override;
  virtual int compute_op_parallel_and_server_info() override;

  int get_equal_set_conditions(ObIArray<ObRawExpr*> &equal_conds);
  virtual int allocate_granule_post(AllocGIContext &ctx) override;
  virtual int allocate_granule_pre(AllocGIContext &ctx) override;
  ObIArray<int64_t> &get_map_array() { return map_array_; }
  int set_map_array(const ObIArray<int64_t> &map_array)
  {
    return map_array_.assign(map_array);
  }
  const ObIArray<int64_t> &get_map_array() const { return map_array_; }
  VIRTUAL_TO_STRING_KV(N_SET_OP, (int)set_op_,
                       "recursive union", is_recursive_union_,
                       "is breadth search", is_breadth_search_,
                       N_DISTINCT, is_distinct_);

  inline SetAlgo get_algo() const { return set_algo_; }
  inline void set_algo_type(const SetAlgo type) { set_algo_ = type; }
  inline void set_distributed_algo(const DistAlgo set_dist_algo) { set_dist_algo_ = set_dist_algo; }
  inline DistAlgo get_distributed_algo() { return set_dist_algo_; }
  int allocate_startup_expr_post() override;
  virtual int print_outline_data(PlanText &plan_text) override;
  virtual int print_used_hint(PlanText &plan_text) override;
  int get_used_pq_set_hint(const ObPQSetHint *&used_hint);
  int construct_pq_set_hint(ObPQSetHint &hint);
  int set_child_ndv(ObIArray<double> &ndv) { return child_ndv_.assign(ndv); }
  int add_child_ndv(double ndv) { return child_ndv_.push_back(ndv); }
  virtual int get_card_without_filter(double &card) override;
  int append_child_fd_item_set(ObFdItemSet &all_fd_item_set, const ObFdItemSet &child_fd_item_set);
  virtual int check_use_child_ordering(bool &used, int64_t &inherit_child_ordering_index)override;
private:
  bool is_distinct_;
  bool is_recursive_union_;
  bool is_breadth_search_;
  SetAlgo set_algo_;
  DistAlgo set_dist_algo_;
  ObSelectStmt::SetOperator set_op_;
  common::ObSEArray<ObOrderDirection, 8, common::ModulePageAllocator, true> set_directions_;
  common::ObSEArray<int64_t, 8, common::ModulePageAllocator, true> map_array_;
  //for cte search clause
  common::ObSEArray<OrderItem, 8, common::ModulePageAllocator, true>  search_ordering_;
  common::ObSEArray<ColumnItem, 8, common::ModulePageAllocator, true>  cycle_items_;
  common::ObSEArray<double, 4, common::ModulePageAllocator, true>  child_ndv_;
  //for batch search recursive cte
  ObRawExpr *identify_seq_expr_;
};

} // end of namespace sql
} // end of namespace oceanbase

#endif // OCEANBASE_SQL_OB_LOG_SET_H
