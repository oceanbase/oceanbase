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

#ifndef OCEANBASE_SQL_OB_LOG_JOIN_H
#define OCEANBASE_SQL_OB_LOG_JOIN_H
#include "ob_log_operator_factory.h"
#include "ob_logical_operator.h"

namespace oceanbase {
namespace sql {
class ObLogicalOperator;
class ObLogJoin : public ObLogicalOperator {
public:
  ObLogJoin(ObLogPlan& plan)
      : ObLogicalOperator(plan),
        join_conditions_(),
        join_filters_(),
        join_type_(UNKNOWN_JOIN),
        join_algo_(INVALID_JOIN_ALGO),
        join_dist_algo_(JoinDistAlgo::DIST_INVALID_METHOD),
        late_mat_(false),
        anti_or_semi_sel_(0),
        merge_directions_(),
        nl_params_(),
        pseudo_columns_(),
        connect_by_prior_exprs_(),
        use_distribute_hint_(false),
        exec_params_(),
        pq_map_hint_(false),
        partition_id_expr_(nullptr),
        slave_mapping_type_(SM_NONE),
        join_filter_selectivitiy_(0),
        connect_by_extra_exprs_()
  {}
  virtual ~ObLogJoin()
  {}

  inline void set_join_type(const ObJoinType join_type)
  {
    join_type_ = join_type;
  }
  inline ObJoinType get_join_type() const
  {
    return join_type_;
  }
  inline void set_join_algo(const JoinAlgo join_algo)
  {
    join_algo_ = join_algo;
  }
  inline JoinAlgo get_join_algo() const
  {
    return join_algo_;
  }
  inline void set_late_mat(const bool late_mat)
  {
    late_mat_ = late_mat;
  }
  inline bool is_late_mat() const
  {
    return late_mat_;
  }
  inline void set_join_distributed_method(const JoinDistAlgo dist_method)
  {
    join_dist_algo_ = dist_method;
  }
  inline JoinDistAlgo get_join_distributed_method() const
  {
    return join_dist_algo_;
  }
  inline bool is_cartesian() const
  {
    return join_conditions_.empty() && join_filters_.empty() && nl_params_.empty() && filter_exprs_.empty();
  }
  inline JoinDistAlgo get_dist_method() const
  {
    return join_dist_algo_;
  }
  inline void set_anti_or_semi_sel(double sel)
  {
    anti_or_semi_sel_ = sel;
  }
  inline double get_anti_or_semi_sel() const
  {
    return anti_or_semi_sel_;
  }
  inline bool used_pq_distribute_hint() const
  {
    return use_distribute_hint_;
  }
  inline void set_use_pq_map()
  {
    pq_map_hint_ = true;
  }
  int is_left_unique(bool& left_unique) const;

  // Get the join predicates
  // TODO: not sure how we handle join different join conditions yet, use the same impl
  // for now.
  inline int add_join_condition(ObRawExpr* expr)
  {
    return join_conditions_.push_back(expr);
  }
  inline int add_join_filter(ObRawExpr* expr)
  {
    return join_filters_.push_back(expr);
  }
  const common::ObIArray<ObRawExpr*>& get_equal_join_conditions() const
  {
    return join_conditions_;
  }
  const common::ObIArray<ObRawExpr*>& get_other_join_conditions() const
  {
    return join_filters_;
  }

  /**
   *  Get the nl params
   */
  inline common::ObIArray<std::pair<int64_t, ObRawExpr*> >& get_nl_params()
  {
    return nl_params_;
  }
  /**
   *  Set the nl params
   */
  int set_nl_params(const common::ObIArray<std::pair<int64_t, ObRawExpr*> >& params)
  {
    return append(nl_params_, params);
  }
  /**
   *  Get the exec params
   */
  inline common::ObIArray<std::pair<int64_t, ObRawExpr*> >& get_exec_params()
  {
    return exec_params_;
  }
  /**
   *  Set the exec params (at this time only for level pseudo column)
   */
  int set_exec_params(const common::ObIArray<std::pair<int64_t, ObRawExpr*> >& params)
  {
    return append(exec_params_, params);
  }

  inline common::ObIArray<ObPseudoColumnRawExpr*>& get_pseudo_columns()
  {
    return pseudo_columns_;
  }

  inline common::ObIArray<ObRawExpr*>& get_connect_by_prior_exprs()
  {
    return connect_by_prior_exprs_;
  }

  int set_connect_by_prior_exprs(const common::ObIArray<ObRawExpr*>& exprs)
  {
    return connect_by_prior_exprs_.assign(exprs);
  }

  inline ObLogicalOperator* get_left_table() const
  {
    return get_child(first_child);
  }
  inline ObLogicalOperator* get_right_table() const
  {
    return get_child(second_child);
  }
  virtual int copy_without_child(ObLogicalOperator*& out);
  int gen_filters();
  int gen_output_columns();
  /**
   * Allocate exchange node if needed
   */
  int allocate_exchange_post(AllocExchContext* ctx) override;

  int update_weak_part_exprs(AllocExchContext* ctx);

  virtual int allocate_expr_post(ObAllocExprContext& ctx);
  //@brief Set all the join predicates
  int set_join_conditions(const common::ObIArray<ObRawExpr*>& conditions)
  {
    return append(join_conditions_, conditions);
  }

  int set_join_filters(const common::ObIArray<ObRawExpr*>& filters)
  {
    return append(join_filters_, filters);
  }

  common::ObIArray<ObRawExpr*>& get_join_conditions()
  {
    return join_conditions_;
  }

  const common::ObIArray<ObRawExpr*>& get_join_conditions() const
  {
    return join_conditions_;
  }

  common::ObIArray<ObRawExpr*>& get_join_filters()
  {
    return join_filters_;
  }

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
  virtual int inner_replace_generated_agg_expr(
      const common::ObIArray<std::pair<ObRawExpr*, ObRawExpr*> >& to_replace_exprs);
  const common::ObIArray<ObOrderDirection>& get_merge_directions() const
  {
    return merge_directions_;
  }
  int set_merge_directions(const common::ObIArray<ObOrderDirection>& merge_directions)
  {
    return merge_directions_.assign(merge_directions);
  }

  /**
   *  Get the operator's hash value
   */
  virtual uint64_t hash(uint64_t seed) const;

  // const char* get_name() const;
  virtual int32_t get_explain_name_length() const;
  virtual int get_explain_name_internal(char* buf, const int64_t buf_len, int64_t& pos);
  virtual int check_output_dep_specific(ObRawExprCheckDep& checker);
  virtual int re_est_cost(const ObLogicalOperator* parent, double need_row_count, bool& re_est) override;
  virtual int re_calc_cost();
  virtual int transmit_op_ordering();
  /*
   * IN         right_child_sharding_info   the join's right child sharding info
   * IN         right_keys                  the right join equal condition
   * OUT        type                        the type of bloom partition filter
   * */
  int bloom_filter_partition_type(
      const ObShardingInfo& right_child_sharding_info, ObIArray<ObRawExpr*>& right_keys, PartitionFilterType& type);
  virtual bool is_block_input(const int64_t child_idx) const override;
  virtual bool is_consume_child_1by1() const
  {
    return HASH_JOIN == join_algo_;
  }
  int can_use_batch_nlj(bool& can_use_batch_nlj);
  virtual int generate_link_sql_pre(GenLinkStmtContext& link_ctx) override;

  inline bool is_nlj_with_param_down() const
  {
    return (NESTED_LOOP_JOIN == join_algo_) && !nl_params_.empty();
  }
  inline bool is_nlj_without_param_down() const
  {
    return (NESTED_LOOP_JOIN == join_algo_) && nl_params_.empty();
  }

  virtual int inner_append_not_produced_exprs(ObRawExprUniqueSet& raw_exprs) const;

  virtual int compute_table_set() override;
  bool is_enable_gi_partition_pruning() const
  {
    return nullptr != partition_id_expr_;
  }
  ObPseudoColumnRawExpr* get_partition_id_expr()
  {
    return partition_id_expr_;
  }
  double get_join_filter_selectivitiy() const
  {
    return join_filter_selectivitiy_;
  }
  int set_connect_by_extra_exprs(const common::ObIArray<ObRawExpr*>& exprs)
  {
    return connect_by_extra_exprs_.assign(exprs);
  }

private:
  inline bool can_enable_gi_partition_pruning()
  {
    return (NESTED_LOOP_JOIN == join_algo_) && join_dist_algo_ == JoinDistAlgo::DIST_PARTITION_NONE;
  }
  int get_left_exch_repartition_table_id(uint64_t& table_id);
  int build_gi_partition_pruning();
  int alloc_partition_id_column(ObAllocExprContext& ctx);
  int get_join_keys(
      const AllocExchContext& ctx, common::ObIArray<ObRawExpr*>& left_keys, common::ObIArray<ObRawExpr*>& right_keys);
  int get_hash_hash_distribution_info(common::ObIArray<ObRawExpr*>& left_keys, common::ObIArray<ObRawExpr*>& right_keys,
      ObIArray<ObExprCalcType>& calc_types);
  int make_sort_keys(common::ObIArray<ObRawExpr*>& sort_expr, common::ObIArray<OrderItem>& directions);
  virtual int allocate_expr_pre(ObAllocExprContext& ctx) override;
  virtual int print_my_plan_annotation(char* buf, int64_t& buf_len, int64_t& pos, ExplainType type);
  virtual int print_outline(planText& plan);
  int print_material_nl(planText& plan_text, JoinTreeType join_tree_type, bool is_need_print);
  int print_use_join(planText& plan_text, JoinTreeType join_tree_type, bool is_need_print);
  int print_pq_distribute(planText& plan_text, JoinTreeType join_tree_type, bool is_need_print);
  int print_pq_map(planText& plan_text, JoinTreeType join_tree_type, bool is_need_print);
  int print_leading(planText& plan_text, JoinTreeType join_tree_type);
  int is_need_print_use_join(planText& plan_text, ObStmtHint& stmt_hint, bool& is_need);
  int is_need_print_material_nl(planText& plan_text, ObStmtHint& stmt_hint, bool& is_need, bool& use_mat);
  int is_need_print_leading(planText& plan_text, const ObStmtHint& stmt_hint, bool& is_need);
  int is_need_print_ordered(planText& plan_text, const ObStmtHint& stmt_hint, bool& is_need);
  int is_need_print_pq_dist(planText& plan_text, const ObStmtHint& stmt_hint, bool& is_need);
  int is_need_print_pq_map(planText& plan_text, const ObStmtHint& stmt_hint, bool& is_need);
  int print_use_late_materialization(planText& plan_text);
  virtual int allocate_granule_post(AllocGIContext& ctx) override;
  virtual int allocate_granule_pre(AllocGIContext& ctx) override;
  int get_candidate_join_distribution_method(ObLogPlan& log_plan, const EqualSets& equal_sets,
      const common::ObIArray<ObRawExpr*>& left_join_keys, const common::ObIArray<ObRawExpr*>& right_join_keys,
      uint64_t& candidate_method);
  int check_if_match_join_partition_wise(ObLogPlan& log_plan, const EqualSets& equal_sets,
      const common::ObIArray<ObRawExpr*>& left_keys, const common::ObIArray<ObRawExpr*>& right_keys,
      bool& is_partition_wise);
  int get_hint_join_distribution_method(uint64_t& candidate_method, bool& is_slave_mapping);
  int get_pq_distribution_method(const JoinDistAlgo join_dist_algo, ObPQDistributeMethod::Type& left_dist_method,
      ObPQDistributeMethod::Type& right_dist_method);
  bool use_slave_mapping()
  {
    return SM_NONE != slave_mapping_type_;
  }

private:
  // all join predicates
  common::ObSEArray<ObRawExpr*, 8, common::ModulePageAllocator, true> join_conditions_;  // equal join condition, for
                                                                                         // merge-join
  common::ObSEArray<ObRawExpr*, 8, common::ModulePageAllocator, true> join_filters_;     // join filter.
                                                                                         // For NL-JOIN BLK-JOIN, all
                                                                                         // conditions here.
  ObJoinType join_type_;
  JoinAlgo join_algo_;
  JoinDistAlgo join_dist_algo_;
  bool late_mat_;
  double anti_or_semi_sel_;
  common::ObSEArray<ObOrderDirection, 8, common::ModulePageAllocator, true> merge_directions_;
  common::ObSEArray<std::pair<int64_t, ObRawExpr*>, 8, common::ModulePageAllocator, true> nl_params_;
  common::ObSEArray<ObPseudoColumnRawExpr*, 3, common::ModulePageAllocator, true> pseudo_columns_;
  common::ObSEArray<ObRawExpr*, 8, common::ModulePageAllocator, true> connect_by_prior_exprs_;
  common::ObSEArray<OrderItem, 8, common::ModulePageAllocator, true> left_expected_ordering_;
  common::ObSEArray<OrderItem, 8, common::ModulePageAllocator, true> right_expected_ordering_;
  // the the pq_distirbute hint go into effect, this var will be set to true.
  bool use_distribute_hint_;
  // For level pseudo column at this time. We store <param idx, ? expr>, and finally pass to log join.
  common::ObSEArray<std::pair<int64_t, ObRawExpr*>, 8, common::ModulePageAllocator, true> exec_params_;
  // pq map hint
  bool pq_map_hint_;
  ObPseudoColumnRawExpr* partition_id_expr_;
  SlaveMappingType slave_mapping_type_;

  double join_filter_selectivitiy_;
  ObSEArray<ObRawExpr*, 8, common::ModulePageAllocator, true> connect_by_extra_exprs_;
  DISALLOW_COPY_AND_ASSIGN(ObLogJoin);
};

}  // end of namespace sql
}  // end of namespace oceanbase

#endif  // OCEANBASE_SQL_OB_LOG_JOIN_H
