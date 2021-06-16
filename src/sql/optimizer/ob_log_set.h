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
namespace oceanbase {
namespace sql {
class ObBasicCostInfo;

class ObLogSet : public ObLogicalOperator {
public:
  ObLogSet(ObLogPlan& plan)
      : ObLogicalOperator(plan),
        is_distinct_(true),
        is_recursive_union_(false),
        is_breadth_search_(true),
        set_algo_(INVALID_SET_ALGO),
        set_dist_algo_(DIST_INVALID_METHOD),
        slave_mapping_type_(SM_NONE),
        set_op_(ObSelectStmt::NONE),
        set_directions_(),
        domain_index_exprs_()
  {}

  virtual ~ObLogSet()
  {}

  virtual int allocate_expr_post(ObAllocExprContext& ctx) override;
  virtual int allocate_expr_pre(ObAllocExprContext& ctx) override;
  int is_my_set_expr(const ObRawExpr* expr, bool& bret);
  ObSelectLogPlan* get_left_plan() const;
  ObSelectLogPlan* get_right_plan() const;
  ObSelectStmt* get_left_stmt() const;
  ObSelectStmt* get_right_stmt() const;
  const char* get_name() const;
  inline void assign_set_distinct(const bool is_distinct)
  {
    is_distinct_ = is_distinct;
  }
  inline void set_recursive_union(bool is_recursive_union)
  {
    is_recursive_union_ = is_recursive_union;
  }
  inline void set_is_breadth_search(bool is_breadth_search)
  {
    is_breadth_search_ = is_breadth_search;
  }
  inline bool is_recursive_union()
  {
    return is_recursive_union_;
  }
  inline bool is_breadth_search()
  {
    return is_breadth_search_;
  }
  inline bool is_set_distinct() const
  {
    return is_distinct_;
  }
  virtual bool is_consume_child_1by1() const
  {
    return ObSelectStmt::UNION == set_op_ && (HASH_SET == set_algo_ || !is_distinct_);
  }
  inline void assign_set_op(const ObSelectStmt::SetOperator set_op)
  {
    set_op_ = set_op;
  }
  inline ObSelectStmt::SetOperator get_set_op() const
  {
    return set_op_;
  }
  virtual int copy_without_child(ObLogicalOperator*& out);
  int calculate_sharding_info(
      ObIArray<ObRawExpr*>& left_keys, ObIArray<ObRawExpr*>& right_keys, ObShardingInfo& output_sharding);
  virtual int allocate_exchange_post(AllocExchContext* ctx);
  int get_set_child_exprs(common::ObIArray<ObRawExpr*>& left_keys, common::ObIArray<ObRawExpr*>& right_keys);
  int get_calc_types(common::ObIArray<ObExprCalcType>& calc_types);
  const common::ObIArray<ObOrderDirection>& get_set_directions() const
  {
    return set_directions_;
  }
  common::ObIArray<ObOrderDirection>& get_set_directions()
  {
    return set_directions_;
  }
  int set_set_directions(const common::ObIArray<ObOrderDirection>& directions)
  {
    return set_directions_.assign(directions);
  }
  int add_set_direction(const ObOrderDirection direction = default_asc_direction())
  {
    return set_directions_.push_back(direction);
  }
  int add_domain_index_expr(ObRawExpr* expr)
  {
    return domain_index_exprs_.push_back(expr);
  }
  int get_set_exprs(ObIArray<ObRawExpr*>& set_exprs);
  int extra_set_exprs(ObIArray<ObRawExpr*>& set_exprs);
  virtual int est_cost() override;
  virtual int re_est_cost(const ObLogicalOperator* parent, double need_row_count, bool& re_est) override;
  int get_children_cost_info(ObIArray<ObBasicCostInfo>& children_cost_info);
  virtual int transmit_op_ordering();
  int set_left_expected_ordering(const common::ObIArray<OrderItem>& left_expected_ordering);

  int set_right_expected_ordering(const common::ObIArray<OrderItem>& right_expected_ordering);

  common::ObIArray<OrderItem>& get_left_expected_ordering()
  {
    return left_expected_ordering_;
  }
  common::ObIArray<OrderItem>& get_right_expected_ordering()
  {
    return right_expected_ordering_;
  }

  int set_search_ordering(const common::ObIArray<OrderItem>& search_ordering);
  int set_cycle_items(const common::ObIArray<ColumnItem>& cycle_items);
  uint64_t hash(uint64_t seed) const;
  const common::ObIArray<OrderItem>& get_search_ordering()
  {
    return search_ordering_;
  }
  const common::ObIArray<ColumnItem>& get_cycle_items()
  {
    return cycle_items_;
  }
  virtual int compute_const_exprs() override;
  virtual int compute_equal_set() override;
  virtual int compute_fd_item_set() override;
  virtual int deduce_const_exprs_and_ft_item_set(ObFdItemSet& fd_item_set) override;

  virtual int compute_op_ordering() override;
  virtual int compute_one_row_info() override;

  int get_equal_set_conditions(ObIArray<ObRawExpr*>& equal_conds);

  int update_sharding_conds(ObIArray<ObRawExpr*>& sharding_conds);

  virtual int allocate_granule_post(AllocGIContext& ctx) override;
  virtual int allocate_granule_pre(AllocGIContext& ctx) override;
  virtual int generate_link_sql_pre(GenLinkStmtContext& link_ctx) override;
  ObIArray<int64_t>& get_map_array()
  {
    return map_array_;
  }
  int set_map_array(const ObIArray<int64_t>& map_array)
  {
    return map_array_.assign(map_array);
  }
  const ObIArray<int64_t>& get_map_array() const
  {
    return map_array_;
  }
  VIRTUAL_TO_STRING_KV(N_SET_OP, (int)set_op_, "recursive union", is_recursive_union_, "is breadth search",
      is_breadth_search_, N_DISTINCT, is_distinct_);

  inline SetAlgo get_algo() const
  {
    return set_algo_;
  }
  inline void set_algo_type(const SetAlgo type)
  {
    set_algo_ = type;
  }
  bool use_slave_mapping()
  {
    return SM_NONE != slave_mapping_type_;
  }

private:
  virtual int inner_replace_generated_agg_expr(
      const common::ObIArray<std::pair<ObRawExpr*, ObRawExpr*>>& to_replace_exprs) override;
  int allocate_implicit_sort_v2_for_set(const int64_t index, common::ObIArray<OrderItem>& order_keys);
  int insert_operator_for_set(log_op_def::ObLogOpType type, int64_t index);

  int make_sort_keys(common::ObIArray<ObRawExpr*>& sort_expr, common::ObIArray<OrderItem>& directions);
  int get_candidate_join_distribution_method(const EqualSets& equal_sets,
      const common::ObIArray<ObRawExpr*>& left_set_key, const common::ObIArray<ObRawExpr*>& right_set_key,
      uint64_t& candidate_method);
  int allocate_exchange_for_union_all(AllocExchContext* ctx);
  int check_if_match_partition_wise(bool& is_match);

  ObJoinType convert_set_op()
  {
    ObJoinType ret = UNKNOWN_JOIN;
    if (ObSelectStmt::UNION == set_op_) {
      ret = FULL_OUTER_JOIN;
    } else if (ObSelectStmt::INTERSECT == set_op_) {
      ret = INNER_JOIN;
    } else if (ObSelectStmt::EXCEPT == set_op_) {
      ret = LEFT_ANTI_JOIN;
    }
    return ret;
  }

private:
  bool is_distinct_;
  bool is_recursive_union_;
  bool is_breadth_search_;
  SetAlgo set_algo_;
  JoinDistAlgo set_dist_algo_;
  SlaveMappingType slave_mapping_type_;
  ObSelectStmt::SetOperator set_op_;
  common::ObSEArray<ObOrderDirection, 8, common::ModulePageAllocator, true> set_directions_;
  common::ObSEArray<OrderItem, 8, common::ModulePageAllocator, true> left_expected_ordering_;
  common::ObSEArray<OrderItem, 8, common::ModulePageAllocator, true> right_expected_ordering_;
  common::ObSEArray<int64_t, 8, common::ModulePageAllocator, true> map_array_;
  // for cte search clause
  common::ObSEArray<OrderItem, 8, common::ModulePageAllocator, true> search_ordering_;
  common::ObSEArray<ColumnItem, 8, common::ModulePageAllocator, true> cycle_items_;
  // for domain index
  common::ObSEArray<ObRawExpr*, 8, common::ModulePageAllocator, true> domain_index_exprs_;
};

}  // end of namespace sql
}  // end of namespace oceanbase

#endif  // OCEANBASE_SQL_OB_LOG_SET_H
