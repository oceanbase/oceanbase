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

#ifndef OCEANBASE_SQL_OPTIMIZER_OB_LOGICAL_PLAN_TREE_H
#define OCEANBASE_SQL_OPTIMIZER_OB_LOGICAL_PLAN_TREE_H 1
#include "lib/container/ob_array.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/optimizer/ob_log_plan.h"
#include "sql/optimizer/ob_sharding_info.h"

namespace oceanbase
{
namespace sql
{
class ObJoinOrder;
class AccessPath;
struct JoinInfo;
class Path;
class JoinPath;
class ObLogGroupBy;
class ObLogSet;
class ObLogWindowFunction;
struct MergeKeyInfo;
struct IndexDMLInfo;

/**
 *  Logical Plan for 'select' statement
 */
class ObSelectLogPlan : public ObLogPlan
{
  friend class ::test::ObLogPlanTest_ob_explain_test_Test;
public:
  ObSelectLogPlan(ObOptimizerContext &ctx, const ObSelectStmt *select_stmt);
  virtual ~ObSelectLogPlan();

  // @brief GENERATE the raw PLAN tree which have not traversed
  int allocate_link_scan_as_top(ObLogicalOperator *&old_top);
  const ObSelectStmt *get_stmt() const override
  { return reinterpret_cast<const ObSelectStmt*>(stmt_); }

  int perform_late_materialization(ObSelectStmt *stmt,
                                   ObLogicalOperator *&op);

protected:
  virtual int generate_normal_raw_plan() override;
  virtual int generate_dblink_raw_plan() override;

private:
  // @brief Allocate a hash group by on top of a plan tree
  // ObLogicalOperator * candi_allocate_hash_group_by();

  // @brief Allocate a merge group by on top of a plan tree
  // ObLogicalOperator * candi_allocate_merge_group_by(ObIArray<OrderItem> &ordering);

  // @brief Allocate GROUP BY on top of plan candidates
  int candi_allocate_group_by();

  // @brief Get all the group by exprs, rollup exprs and their directions.
  int get_groupby_rollup_exprs(const ObLogicalOperator *op,
                               common::ObIArray<ObRawExpr *> &reduce_exprs,
                               common::ObIArray<ObRawExpr *> &group_by_exprs,
                               common::ObIArray<ObRawExpr *> &rollup_exprs,
                               common::ObIArray<ObOrderDirection> &group_directions,
                               common::ObIArray<ObOrderDirection> &rollup_directions);

  int candi_allocate_rollup_group_by(const ObIArray<ObRawExpr*> &reduce_exprs,
                                     const ObIArray<ObRawExpr*> &group_by_exprs,
                                     const ObIArray<ObOrderDirection> &group_directions,
                                     const ObIArray<ObRawExpr*> &rollup_exprs,
                                     const ObIArray<ObOrderDirection> &rollup_directions,
                                     const ObIArray<ObRawExpr*> &having_exprs,
                                     const ObIArray<ObAggFunRawExpr*> &agg_items,
                                     const bool is_from_povit,
                                     ObIArray<CandidatePlan> &groupby_plans);

  int candi_allocate_merge_rollup(const ObIArray<ObRawExpr*> &reduce_exprs,
                                  const ObIArray<ObRawExpr*> &group_by_exprs,
                                  const ObIArray<ObOrderDirection> &group_directions,
                                  const ObIArray<ObRawExpr*> &rollup_exprs,
                                  const ObIArray<ObOrderDirection> &rollup_directions,
                                  const ObIArray<ObRawExpr*> &having_exprs,
                                  const ObIArray<ObAggFunRawExpr*> &agg_items,
                                  const bool is_from_povit,
                                  GroupingOpHelper &groupby_helper,
                                  ObIArray<CandidatePlan> &groupby_plans);

  int create_merge_rollup_plan(const ObIArray<ObRawExpr*> &reduce_exprs,
                              const ObIArray<ObRawExpr*> &group_by_exprs,
                              const ObIArray<ObOrderDirection> &group_directions,
                              const ObIArray<ObRawExpr*> &rollup_exprs,
                              const ObIArray<ObOrderDirection> &rollup_directions,
                              const ObIArray<ObAggFunRawExpr*> &aggr_items,
                              const ObIArray<ObRawExpr*> &having_exprs,
                              GroupingOpHelper &groupby_helper,
                              CandidatePlan &candidate_plan,
                              ObIArray<CandidatePlan> &candidate_plans,
                              bool part_sort_valid,
                              bool normal_sort_valid,
                              const DistAlgo algo,
                              bool can_ignore_merge_plan);

  int inner_create_merge_rollup_plan(const ObIArray<ObRawExpr*> &reduce_exprs,
                                    const ObIArray<ObRawExpr*> &group_by_exprs,
                                    const ObIArray<ObOrderDirection> &group_directions,
                                    const ObIArray<ObRawExpr*> &rollup_exprs,
                                    const ObIArray<ObOrderDirection> &rollup_directions,
                                    const ObIArray<ObAggFunRawExpr*> &aggr_items,
                                    const ObIArray<ObRawExpr*> &having_exprs,
                                    GroupingOpHelper &groupby_helper,
                                    ObLogicalOperator *&top,
                                    bool use_part_sort,
                                    const DistAlgo algo,
                                    bool &ignore_plan,
                                    bool can_ignore_merge);
  int candi_allocate_hash_rollup(const ObIArray<ObRawExpr*> &reduce_exprs,
                                  const ObIArray<ObRawExpr*> &group_by_exprs,
                                  const ObIArray<ObOrderDirection> &group_directions,
                                  const ObIArray<ObRawExpr*> &rollup_exprs,
                                  const ObIArray<ObOrderDirection> &rollup_directions,
                                  const ObIArray<ObRawExpr*> &having_exprs,
                                  const ObIArray<ObAggFunRawExpr*> &agg_items,
                                  const bool is_from_povit,
                                  GroupingOpHelper &groupby_helper,
                                  ObIArray<CandidatePlan> &groupby_plans);
  int candi_allocate_normal_group_by(const ObIArray<ObRawExpr*> &reduce_exprs,
                                     const ObIArray<ObRawExpr*> &group_by_exprs,
                                     const ObIArray<ObOrderDirection> &group_directions,
                                     const ObIArray<ObRawExpr*> &having_exprs,
                                     const ObIArray<ObAggFunRawExpr*> &agg_items,
                                     const bool is_from_povit,
                                     ObIArray<CandidatePlan> &candi_plans,
                                     ObIArray<CandidatePlan> &groupby_plans);

  int get_valid_aggr_algo(const ObIArray<ObRawExpr*> &group_by_exprs,
                          const ObIArray<ObRawExpr *> &rollup_exprs,
                          const GroupingOpHelper &groupby_helper,
                          bool &use_hash_valid,
                          bool &use_merge_valid,
                          bool &part_sort_valid,
                          bool &normal_sort_valid);
  int update_part_sort_method(bool &part_sort_valid, bool &normal_sort_valid);
  int candi_allocate_normal_group_by(const ObIArray<ObRawExpr*> &reduce_exprs,
                                     const ObIArray<ObRawExpr*> &group_by_exprs,
                                     const ObIArray<ObOrderDirection> &group_directions,
                                     const ObIArray<ObRawExpr*> &having_exprs,
                                     const ObIArray<ObAggFunRawExpr*> &aggr_items,
                                     GroupingOpHelper &groupby_helper,
                                     ObIArray<CandidatePlan> &candi_plans,
                                     ObIArray<CandidatePlan> &groupby_plans);

  int create_hash_group_plan(const ObIArray<ObRawExpr*> &reduce_exprs,
                             const ObIArray<ObRawExpr*> &group_by_exprs,
                             const ObIArray<ObAggFunRawExpr*> &aggr_items,
                             const ObIArray<ObRawExpr*> &having_exprs,
                             GroupingOpHelper &groupby_helper,
                             ObLogicalOperator *&top,
                             const DistAlgo algo);

  int allocate_topk_for_hash_group_plan(ObLogicalOperator *&top);

  int allocate_topk_sort_as_top(ObLogicalOperator *&top,
                                const ObIArray<OrderItem> &sort_keys,
                                ObRawExpr *limit_expr,
                                ObRawExpr *offset_expr,
                                int64_t minimum_row_count,
                                int64_t topk_precision);

  int clone_sort_keys_for_topk(const ObIArray<OrderItem> &sort_keys_,
                               ObIArray<OrderItem> &topk_sort_keys);

  int allocate_topk_as_top(ObLogicalOperator *&top,
                           ObRawExpr *topk_limit_count,
                           ObRawExpr *topk_limit_offset,
                           int64_t minimum_row_count,
                           int64_t topk_precision);

  int create_merge_group_plan(const ObIArray<ObRawExpr*> &reduce_exprs,
                              const ObIArray<ObRawExpr*> &group_by_exprs,
                              const ObIArray<ObOrderDirection> &group_directions,
                              const ObIArray<ObAggFunRawExpr*> &aggr_items,
                              const ObIArray<ObRawExpr*> &having_exprs,
                              GroupingOpHelper &groupby_helper,
                              CandidatePlan &candidate_plan,
                              ObIArray<CandidatePlan> &candidate_plans,
                              bool part_sort_valid,
                              bool normal_sort_valid,
                              const DistAlgo algo,
                              bool can_ignore_merge = false);

  int generate_merge_group_sort_keys(ObLogicalOperator *top,
                                     const ObIArray<ObRawExpr*> &group_by_exprs,
                                     const ObIArray<ObOrderDirection> &group_directions,
                                     const ObIArray<ObRawExpr*> &rollup_exprs,
                                     const ObIArray<ObOrderDirection> &rollup_directions,
                                     ObIArray<ObRawExpr*> &sort_exprs,
                                     ObIArray<ObOrderDirection> &sort_directions);

  int allocate_topk_for_merge_group_plan(ObLogicalOperator *&top);

  // @brief Allocate DISTINCT by on top of plan candidates
  int candi_allocate_distinct();

  // @brief Get all the distinct exprs

  int get_distinct_exprs(const ObLogicalOperator *top,
                         common::ObIArray <ObRawExpr *> &reduce_exprs,
                         common::ObIArray <ObRawExpr *> &distinct_exprs);

  int inner_candi_allocate_distinct(const GroupingOpHelper &distinct_helper,
                                    const ObIArray<ObRawExpr*> &reduce_exprs,
                                    const ObIArray<ObRawExpr*> &distinct_exprs,
                                    ObIArray<CandidatePlan> &distinct_plans);

  int check_need_merge_distinct_plan(const ObLogicalOperator *top,
                                     const ObIArray<ObRawExpr*> &ori_distinct_exprs,
                                     bool can_ignore_merge_plan,
                                     bool &need_sort,
                                     int64_t &prefix_pos,
                                     ObIArray<OrderItem> &sort_keys,
                                     ObIArray<ObRawExpr*> &new_distinct_exprs,
                                     bool &ignore_merge_plan);

  int get_distribute_distinct_method(ObLogicalOperator *top,
                                     const GroupingOpHelper &distinct_helper,
                                     const ObIArray<ObRawExpr*> &reduce_exprs,
                                     bool is_merge_without_sort,
                                     uint64_t &distinct_dist_methods);

  int create_hash_distinct_plan(ObLogicalOperator *&top,
                                const GroupingOpHelper &distinct_helper,
                                const ObIArray<ObRawExpr*> &distinct_exprs,
                                const DistAlgo algo);

  int create_merge_distinct_plan(ObLogicalOperator *&top,
                                 const GroupingOpHelper &distinct_helper,
                                 const ObIArray<ObRawExpr*> &distinct_exprs,
                                 bool need_sort,
                                 int64_t prefix_pos,
                                 ObIArray<OrderItem> &sort_keys,
                                 const DistAlgo algo);

  int allocate_distinct_as_top(ObLogicalOperator *&top,
                               const AggregateAlgo algo,
                               const DistAlgo dist_algo,
                               const ObIArray<ObRawExpr*> &distinct_exprs,
                               const double total_ndv,
                               const bool is_partition_wise = false,
                               const bool is_pushed_down = false,
                               const bool is_partition_gi = false);
  /**
   *  @brief  GENERATE the PLAN tree FOR "SET" operator (UNION/INTERSECT/EXCEPT)
   *  Warning:
   *  @param void
   *  @retval OB_SUCCESS execute success
   *  @retval otherwise, failed to generate the logical plan
   */
  int generate_raw_plan_for_set();

  int candi_allocate_set(const ObIArray<ObSelectLogPlan*> &child_plans);

  int update_set_sharding_info(const ObIArray<ObSelectLogPlan*> &child_plans);

  int candi_allocate_union_all(const ObIArray<ObSelectLogPlan*> &child_plans);

  int generate_union_all_plans(const ObIArray<ObSelectLogPlan*> &child_plans,
                               const bool ignore_hint,
                               ObIArray<CandidatePlan> &all_plans);
  int get_best_child_candidate_plans(const ObIArray<ObSelectLogPlan*> &child_plans,
                                     ObIArray<ObLogicalOperator*> &best_child_ops,
                                     ObIArray<ObLogicalOperator*> &best_das_child_ops,
                                     ObIArray<ObLogicalOperator*> &best_px_child_ops);

  int inner_generate_union_all_plan(const ObIArray<ObLogicalOperator*> &child_ops,
                                    const bool ignore_hint,
                                    ObIArray<CandidatePlan> &all_plans);

  int get_distibute_union_all_method(const ObIArray<ObLogicalOperator*> &child_ops,
                                    const bool ignore_hint,
                                    uint64_t &set_dist_methods,
                                    ObLogicalOperator* &largest_op);

  int create_union_all_plan(const ObIArray<ObLogicalOperator*> &child_ops,
                            const DistAlgo dist_set_method,
                            ObLogicalOperator* largest_op,
                            ObLogicalOperator *&top);

  int check_if_union_all_match_partition_wise(const ObIArray<ObLogicalOperator*> &child_ops,
                                              bool &is_partition_wise);

  int check_if_union_all_match_set_partition_wise(const ObIArray<ObLogicalOperator*> &child_ops,
                                                  bool &is_match_set_pw);

  int check_sharding_inherit_from_access_all(ObLogicalOperator* op, bool &is_inherit_from_access_all);

  int check_if_union_all_match_extended_partition_wise(const ObIArray<ObLogicalOperator*> &child_ops,
                                                       bool &is_match_ext_pw);

  int get_largest_sharding_child(const ObIArray<ObLogicalOperator*> &child_ops,
                                 const int64_t candi_pos,
                                 ObLogicalOperator *&largest_op);

  int allocate_union_all_as_top(const ObIArray<ObLogicalOperator*> &child_plans,
                                DistAlgo dist_set_method,
                                ObLogicalOperator *&top);

  int allocate_set_distinct_as_top(ObLogicalOperator *&top);

  int candi_allocate_recursive_union(const ObIArray<ObSelectLogPlan*> &child_plans);

  int create_recursive_union_plan(ObIArray<CandidatePlan> &left_best_plans,
                                      ObIArray<CandidatePlan> &right_best_plans,
                                      const ObIArray<OrderItem> &order_items,
                                      const bool ignore_hint,
                                      ObIArray<CandidatePlan> &all_plans);
  int get_recursive_union_all_distribute_method(ObLogicalOperator *left_child,
                                                ObLogicalOperator *right_child,
                                                const bool ignore_hint,
                                                DistAlgo &dist_set_method);
  int create_recursive_union_plan(ObLogicalOperator *left_child,
                                  ObLogicalOperator *right_child,
                                  const ObIArray<OrderItem> &candi_order_items,
                                  DistAlgo dist_set_method,
                                  ObLogicalOperator *&top);

  int allocate_recursive_union_as_top(ObLogicalOperator *left_child,
                                          ObLogicalOperator *right_child,
                                          DistAlgo dist_set_method,
                                          ObLogicalOperator *&top);

  int candi_allocate_distinct_set(const ObIArray<ObSelectLogPlan*> &child_plans);

  int get_allowed_branch_order(const bool ignore_hint,
                               const ObSelectStmt::SetOperator set_op,
                               bool &no_swap,
                               bool &swap);

  /**
   * @brief create_hash_set
   * create hash-based set operation
   * @return
   */
  int generate_hash_set_plans(const EqualSets &equal_sets,
                              const ObIArray<ObRawExpr*> &left_set_keys,
                              const ObIArray<ObRawExpr*> &right_set_keys,
                              const ObSelectStmt::SetOperator set_op,
                              const ObIArray<CandidatePlan> &left_best_plans,
                              const ObIArray<CandidatePlan> &right_best_plans,
                              const bool ignore_hint,
                              ObIArray<CandidatePlan> &hash_set_plans);

  int inner_generate_hash_set_plans(const EqualSets &equal_sets,
                                    const ObIArray<ObRawExpr*> &left_set_keys,
                                    const ObIArray<ObRawExpr*> &right_set_keys,
                                    const ObSelectStmt::SetOperator set_op,
                                    const ObIArray<CandidatePlan> &left_best_plans,
                                    const ObIArray<CandidatePlan> &right_best_plans,
                                    const bool ignore_hint,
                                    ObIArray<CandidatePlan> &hash_set_plans);

  int create_hash_set_plan(const EqualSets &equal_sets,
                           ObLogicalOperator *left_child,
                           ObLogicalOperator *right_child,
                           const ObIArray<ObRawExpr*> &left_set_keys,
                           const ObIArray<ObRawExpr*> &right_set_keys,
                           const ObSelectStmt::SetOperator set_op,
                           DistAlgo dist_set_method,
                           CandidatePlan &hash_plan);

  int compute_set_exchange_info(const EqualSets &equal_sets,
                                ObLogicalOperator &left_child,
                                ObLogicalOperator &right_child,
                                const ObIArray<ObRawExpr*> &left_set_keys,
                                const ObIArray<ObRawExpr*> &right_set_keys,
                                const ObSelectStmt::SetOperator set_op,
                                DistAlgo set_method,
                                ObExchangeInfo &left_exch_info,
                                ObExchangeInfo &right_exch_info);

  int compute_set_hash_hash_sharding(const EqualSets &equal_sets,
                                     const ObIArray<ObRawExpr*> &left_keys,
                                     const ObIArray<ObRawExpr*> &right_keys,
                                     ObShardingInfo *&sharding);

  int generate_merge_set_plans(const EqualSets &equal_sets,
                               const ObIArray<ObRawExpr*> &left_set_keys,
                               const ObIArray<ObRawExpr*> &right_set_keys,
                               const ObSelectStmt::SetOperator set_op,
                               ObIArray<ObSEArray<CandidatePlan, 16>> &left_candidate_list,
                               ObIArray<ObSEArray<CandidatePlan, 16>> &right_candidate_list,
                               const bool ignore_hint,
                               const bool no_hash_plans,
                               ObIArray<CandidatePlan> &merge_set_plans);

  int inner_generate_merge_set_plans(const EqualSets &equal_sets,
                                     const ObIArray<ObRawExpr*> &left_set_keys,
                                     const ObIArray<ObRawExpr*> &right_set_keys,
                                     const ObIArray<MergeKeyInfo*> &left_merge_keys,
                                     const ObSelectStmt::SetOperator set_op,
                                     ObIArray<CandidatePlan> &left_candidates,
                                     ObIArray<CandidatePlan> &right_candidates,
                                     const bool ignore_hint,
                                     const bool can_ignore_merge_plan,
                                     const bool no_hash_plans,
                                     ObIArray<CandidatePlan> &merge_set_plans);

  int get_minimal_cost_set_plan(const int64_t in_parallel,
                                const ObLogicalOperator &left_child,
                                const MergeKeyInfo &left_merge_key,
                                const ObIArray<ObRawExpr*> &right_set_exprs,
                                const ObIArray<CandidatePlan> &right_candi,
                                const DistAlgo set_dist_algo,
                                ObIArray<OrderItem> &best_order_items,
                                ObLogicalOperator *&best_plan,
                                bool &best_need_sort,
                                int64_t &best_prefix_pos,
                                const bool can_ignore_merge_plan);
  int decide_merge_set_sort_key(const ObIArray<OrderItem> &set_order_items,
                                const ObIArray<OrderItem> &input_ordering,
                                const ObFdItemSet &fd_item_set,
                                const EqualSets &equal_sets,
                                const ObIArray<ObRawExpr*> &const_exprs,
                                const ObIArray<ObRawExpr*> &exec_ref_exprs,
                                const bool is_at_most_one_row,
                                const ObIArray<ObRawExpr*> &merge_exprs,
                                const ObIArray<ObOrderDirection> &default_directions,
                                MergeKeyInfo &merge_key);
  int convert_set_order_item(const ObDMLStmt *stmt, const ObIArray<ObRawExpr*> &select_exprs, ObIArray<OrderItem> &order_items);
  int create_merge_set_key(const ObIArray<OrderItem> &set_order_items,
                           const ObIArray<ObRawExpr*> &merge_exprs,
                           const EqualSets &equal_sets,
                           MergeKeyInfo &merge_key);

  /**
   * @brief create_merge_set
   * create merge-based set operation
   * @return
   */
  int create_merge_set_plan(const EqualSets &equal_sets,
                            ObLogicalOperator *left_child,
                            ObLogicalOperator *right_child,
                            const ObIArray<ObRawExpr*> &left_set_keys,
                            const ObIArray<ObRawExpr*> &right_set_keys,
                            const ObSelectStmt::SetOperator set_op,
                            const DistAlgo dist_set_method,
                            const ObIArray<ObOrderDirection> &order_directions,
                            const ObIArray<int64_t> &map_array,
                            const ObIArray<OrderItem> &left_sort_keys,
                            bool left_need_sort,
                            int64_t left_prefix_pos,
                            const ObIArray<OrderItem> &right_sort_keys,
                            bool right_need_sort,
                            int64_t right_prefix_pos,
                            CandidatePlan &merge_plan);

  int check_need_pushdown_set_distinct(ObLogicalOperator *&child,
                                       const ObIArray<ObRawExpr*> &set_keys,
                                       bool is_set_op_parallel,
                                       bool &is_valid);

  int allocate_pushdown_set_distinct_as_top(ObLogicalOperator *&child,
                                            const ObIArray<ObRawExpr*> &set_keys,
                                            AggregateAlgo algo,
                                            const OptTableMeta *table_meta,
                                            bool is_partition_wise = false,
                                            bool is_partition_gi = false);

  int allocate_pushdown_merge_set_distinct_as_top(ObLogicalOperator *&child,
                                                  const ObIArray<ObRawExpr*> &set_keys,
                                                  const ObIArray<OrderItem> &sort_keys,
                                                  const OptTableMeta *table_meta,
                                                  bool &need_sort,
                                                  int64_t &prefix_pos);

  int allocate_distinct_set_as_top(ObLogicalOperator *left_child,
                                   ObLogicalOperator *right_child,
                                   SetAlgo set_method,
                                   DistAlgo dist_set_method,
                                   ObLogicalOperator *&top,
                                   const ObIArray<ObOrderDirection> *order_directions = NULL,
                                   const ObIArray<int64_t> *map_array = NULL);

  int get_distributed_set_methods(const EqualSets &equal_sets,
                                  const ObIArray<ObRawExpr*> &left_set_keys,
                                  const ObIArray<ObRawExpr*> &right_set_keys,
                                  const ObSelectStmt::SetOperator set_op,
                                  const SetAlgo set_method,
                                  ObLogicalOperator &left_child,
                                  ObLogicalOperator &right_child,
                                  const bool ignore_hint,
                                  int64_t &set_dist_methods);

  bool is_set_partition_wise_valid(const ObLogicalOperator &left_plan,
                                   const ObLogicalOperator &right_plan);

  bool is_set_repart_valid(const ObLogicalOperator &left_plan,
                           const ObLogicalOperator &right_plan,
                           const DistAlgo dist_algo);

  int check_if_set_match_repart(const EqualSets &equal_sets,
                                const ObIArray<ObRawExpr *> &src_join_keys,
                                const ObIArray<ObRawExpr *> &target_join_keys,
                                const ObLogicalOperator &target_child,
                                bool &is_match_repart);

  int check_if_set_match_rehash(const EqualSets &equal_sets,
                                const ObIArray<ObRawExpr *> &src_join_keys,
                                const ObIArray<ObRawExpr *> &target_join_keys,
                                const ObLogicalOperator &target_child,
                                bool &is_match_single_side_hash);
  /**
   * @brief generate_subquery_plan
   * generate sub query plan
   * @return
   */
  int generate_child_plan_for_set(const ObDMLStmt *sub_stmt,
                                  ObSelectLogPlan *&sub_plan,
                                  ObIArray<ObRawExpr*> &pushdown_filters,
                                  const uint64_t child_offset,
                                  const bool is_set_distinct,
                                  const bool need_accurate_cardinality,
                                  ObSelectLogPlan *nonrecursive_plan);

  /**
   *  @brief  GENERATE the PLAN tree FOR PLAIN SELECT stmt
   *  Warning:
   *  @param void
   *  @retval OB_SUCCESS execute success
   *  @retval otherwise, failed to generate the logical plan
   */
  int generate_raw_plan_for_plain_select();

  /**
   *  @brief  GENERATE the PLAN tree FOR select EXPR from dual
   *  Warning:
   *  @param void
   *  @retval OB_SUCCESS execute success
   *  @retval otherwise, failed to generate the logical plan
   */
  int generate_raw_plan_for_expr_values();

  /**
     *  @brief  GENERATE the PLAN TOP FOR select stmt
     *  Warning:
     *  @param void
     *  @retval OB_SUCCESS execute success
     *  @retval otherwise, failed to generate the top operators of plan tree
     */
  int allocate_plan_top();
  /**
   * 处理select子句里的子查询，生成SubPlan
   * @param
   * @return
   */
  //    int process_subplan();
  int candi_allocate_subplan_filter_for_select_item();

  struct WinFuncOpHelper
  {
    WinFuncOpHelper(const ObIArray<ObWinFunRawExpr*> &all_win_func_exprs,
                    const ObWindowDistHint *win_dist_hint,
                    const bool explicit_hint,
                    const ObFdItemSet &fd_item_set,
                    const EqualSets &equal_sets,
                    const ObIArray<ObRawExpr*> &const_exprs,
                    const double card,
                    const bool is_at_most_one_row,
                    const ObIArray<ObRawExpr*> &qualify_filters,
                    const ObIArray<double> &ambient_card)
      : all_win_func_exprs_(all_win_func_exprs),
        win_dist_hint_(win_dist_hint),
        explicit_hint_(explicit_hint),
        fd_item_set_(fd_item_set),
        equal_sets_(equal_sets),
        const_exprs_(const_exprs),
        card_(card),
        is_at_most_one_row_(is_at_most_one_row),
        win_dist_method_(WinDistAlgo::WIN_DIST_INVALID),
        force_normal_sort_(false),
        force_hash_sort_(false),
        force_no_pushdown_(false),
        force_pushdown_(false),
        part_cnt_(false),
        win_op_idx_(false),
        wf_aggr_status_expr_(NULL),
        need_qualify_filter_(false),
        qualify_filters_(qualify_filters),
        enable_topn_(false),
        topn_const_(NULL),
        is_fetch_with_ties_(false),
        origin_sort_card_(0.0),
        ambient_card_(ambient_card),
        is_set_(false),
        need_normal_sort_(false),
        use_topn_(false),
        need_hash_sort_(false),
        use_part_topn_(false)
    {
    }
    virtual ~WinFuncOpHelper() {}

    // is const during allocate multi window function op
    const ObIArray<ObWinFunRawExpr*> &all_win_func_exprs_;  // original ordered window functions from stmt
    const ObWindowDistHint *win_dist_hint_;
    const bool explicit_hint_;
    const ObFdItemSet &fd_item_set_;
    const EqualSets &equal_sets_;
    const ObIArray<ObRawExpr*> &const_exprs_;
    const double card_;
    const bool is_at_most_one_row_;

    // attribute for single window function op
    WinDistAlgo win_dist_method_;
    bool force_normal_sort_;
    bool force_hash_sort_;
    bool force_no_pushdown_;  // hint force pushdown, at least pushdown one window function
    bool force_pushdown_;  // hint force pushdown, at least pushdown one window function
    
    int64_t part_cnt_;
    int64_t win_op_idx_;
    ObOpPseudoColumnRawExpr *wf_aggr_status_expr_;
    ObArray<ObRawExpr*> partition_exprs_;
    ObArray<ObWinFunRawExpr*> ordered_win_func_exprs_;
    ObArray<double> sort_key_ndvs_;
    ObArray<std::pair<int64_t, int64_t>> pby_oby_prefixes_;
    ObArray<OrderItem> sort_keys_;
    //if true, add winfunc filters to winfunc operator
    bool need_qualify_filter_;
    const ObIArray<ObRawExpr*> &qualify_filters_;
    //if true, only generate partition topn plans. if false, only generate no partition topn plans.
    bool enable_topn_;
    ObRawExpr* topn_const_;
    bool is_fetch_with_ties_;
    double origin_sort_card_;
    const ObIArray<double> &ambient_card_;
    bool is_set_;
    bool need_normal_sort_;
    bool use_topn_;
    bool need_hash_sort_;
    bool use_part_topn_;
    void reset_plan_options()
    {
      is_set_ = false;
    }
    int set_plan_options(const bool need_normal_sort,
                          const bool use_topn,
                          const bool need_hash_sort,
                          const bool use_part_topn)
    {
      int ret = OB_SUCCESS;
      if (is_set_) {
        // unexpected
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("win plan options already set", K(ret));
      } else {
        need_normal_sort_ = need_normal_sort;
        use_topn_ = use_topn;
        need_hash_sort_ = need_hash_sort;
        use_part_topn_ = use_part_topn;
        is_set_ = true;
      }
      return ret;
    }
    TO_STRING_KV(K_(win_dist_method),
                 K_(win_op_idx),
                 K_(force_normal_sort),
                 K_(force_hash_sort),
                 K_(force_no_pushdown),
                 K_(force_pushdown),
                 K_(partition_exprs),
                 K_(part_cnt),
                 K_(ordered_win_func_exprs),
                 K_(win_dist_hint),
                 K_(explicit_hint),
                 K_(ambient_card),
                 K_(enable_topn),
                 K_(topn_const),
                 K_(is_fetch_with_ties),
                 K_(qualify_filters),
                 K_(need_normal_sort),
                 K_(use_topn),
                 K_(need_hash_sort),
                 K_(use_part_topn));
  };

private:
  int decide_sort_keys_for_runion(const common::ObIArray<OrderItem> &order_items,
                                  common::ObIArray<OrderItem> &new_order_items);
  int init_merge_set_structure(common::ObIAllocator &allocator,
                               const common::ObIArray<CandidatePlan> &plans,
                               const common::ObIArray<ObRawExpr*> &select_exprs,
                               common::ObIArray<MergeKeyInfo*> &merge_keys,
                               const bool can_ignore_merge_plan);

  int candi_allocate_window_function();
  int candi_allocate_window_function_with_hint(const ObIArray<ObWinFunRawExpr*> &win_func_exprs,
                                               const ObIArray<ObRawExpr*> &qualify_filters,
                                               common::ObIArray<CandidatePlan> &total_plans);
  int candi_allocate_window_function(const ObIArray<ObWinFunRawExpr*> &win_func_exprs,
                                     const ObIArray<ObRawExpr*> &qualify_filters,
                                     ObIArray<CandidatePlan> &total_plans);
  int create_one_window_function(CandidatePlan &candidate_plan,
                                 WinFuncOpHelper &win_func_helper,
                                 ObIArray<CandidatePlan> &all_plans);
  int get_distribute_window_method(ObLogicalOperator *top,
                                  const WinFuncOpHelper &win_func_helper,
                                  uint64_t &win_dist_methods,
                                  bool &single_part_parallel,
                                  bool &is_partition_wise);
  bool supported_wf_dist_list_func(ObWinFunRawExpr &win_expr);
  int create_dist_win_func(ObLogicalOperator *&top,
                           const WinFuncOpHelper &win_func_helper,
                           const ObExchangeInfo &exch_info,
                           const bool need_sort,
                           const int64_t prefix_pos,
                           const bool is_local_order,
                           ObRawExpr *topn_expr,
                           bool is_fetch_with_ties,
                           const OrderItem *hash_sortkey,
                           const WinDistAlgo dist_algo,
                           const bool match_parallel,
                           const bool is_partition_wise,
                           const bool use_hash_sort,
                           const bool use_topn_sort,
                           double origin_sort_card,
                           ObLogicalOperator *&new_top);
  int create_dist_win_func_no_pushdown(WinDistAlgo win_dist_algo,
                                       ObLogicalOperator *top,
                                       const WinFuncOpHelper &win_func_helper,
                                       const bool need_sort,
                                       const bool single_part_parallel,
                                       const bool is_partition_wise,
                                       const int64_t prefix_pos,
                                       const bool is_local_order,
                                       ObExchangeInfo * exch_info,
                                       ObIArray<CandidatePlan> &all_plans);
  int create_none_dist_win_func(ObLogicalOperator *top,
                                const WinFuncOpHelper &win_func_helper,
                                const bool need_sort,
                                const bool single_part_parallel,
                                const bool is_partition_wise,
                                const int64_t prefix_pos,
                                ObIArray<CandidatePlan> &all_plans);
  int create_range_list_dist_win_func(ObLogicalOperator *top,
                                      const WinFuncOpHelper &win_func_helper,
                                      ObIArray<CandidatePlan> &all_plans);
  int get_range_dist_keys(const WinFuncOpHelper &win_func_helper,
                          const ObWinFunRawExpr *win_func,
                          ObIArray<OrderItem> &range_dist_keys,
                          int64_t &pby_prefix);
  int get_range_list_win_func_exchange_info(const WinDistAlgo dist_method,
                                            const ObIArray<OrderItem> &range_dist_keys,
                                            ObExchangeInfo &exch_info,
                                            ObRawExpr *&random_expr);
  int set_exchange_random_expr(ObLogicalOperator *top, ObRawExpr *random_expr);

  int create_hash_dist_win_func(ObLogicalOperator *top,
                                const WinFuncOpHelper &win_func_helper,
                                const bool need_sort,
                                const int64_t prefix_pos,
                                ObIArray<CandidatePlan> &all_plans);

  int create_hash_local_dist_win_func(ObLogicalOperator *top,
                                      const WinFuncOpHelper &win_func_helper,
                                      const bool need_sort,
                                      const int64_t prefix_pos,
                                      ObIArray<CandidatePlan> &all_plans);

  int create_normal_hash_dist_win_func(ObLogicalOperator *&top,
                                       const ObIArray<ObWinFunRawExpr*> &win_func_exprs,
                                       const ObIArray<ObRawExpr*> &partition_exprs,
                                       const ObIArray<OrderItem> &sort_keys,
                                       const int64_t need_sort,
                                       const int64_t prefix_pos,
                                       OrderItem *hash_sortkey,
                                       ObRawExpr *topn_const,
                                       bool is_fetch_with_ties,
                                       const ObIArray<ObRawExpr*> *qualify_filters,
                                       double origin_sort_card);
  int create_pushdown_hash_dist_win_func(ObLogicalOperator *&top,
                                         const ObIArray<ObWinFunRawExpr*> &win_func_exprs,
                                         const ObIArray<OrderItem> &sort_keys,
                                         const ObIArray<bool> &pushdown_info,
                                         ObOpPseudoColumnRawExpr *wf_aggr_status_expr,
                                         const int64_t need_sort,
                                         const int64_t prefix_pos,
                                         OrderItem *hash_sortkey,
                                         const ObIArray<ObRawExpr*> *qualify_filters);
  int check_is_win_func_hint_valid(const ObIArray<ObWinFunRawExpr*> &all_win_exprs,
                                   const ObWindowDistHint *hint,
                                   bool &is_valid);
  int init_win_func_helper_with_hint(const ObIArray<CandidatePlan> &candi_plans,
                                     ObIArray<ObWinFunRawExpr*> &remaining_exprs,
                                     WinFuncOpHelper &win_func_helper,
                                     bool &is_valid);
  int calc_win_func_helper_with_hint(const ObLogicalOperator *op,
                                     WinFuncOpHelper &win_func_helper,
                                     bool &is_valid);
int generate_window_functions_plan(WinFuncOpHelper &win_func_helper,
                                   ObIArray<ObOpPseudoColumnRawExpr*> &status_exprs,
                                   ObIArray<CandidatePlan> &total_plans,
                                   CandidatePlan &orig_candidate_plan);
  int prune_win_func_plan_by_sort_method(ObIArray<CandidatePlan> &candi_plans,
                                         ObIArray<CandidatePlan> &final_plans);
  int prepare_next_group_win_funcs(const bool distributed,
                                   const ObIArray<OrderItem> &op_ordering,
                                   const int64_t dop,
                                   WinFuncOpHelper &win_func_helper,
                                   ObIArray<ObWinFunRawExpr*> &remaining_exprs,
                                   ObIArray<ObWinFunRawExpr*> &ordered_win_func_exprs,
                                   ObIArray<std::pair<int64_t, int64_t>> &pby_oby_prefixes,
                                   ObIArray<int64_t> &split,
                                   ObIArray<WinDistAlgo> &methods);
  int init_win_func_helper(const ObIArray<ObWinFunRawExpr*> &ordered_win_func_exprs,
                           const ObIArray<std::pair<int64_t, int64_t>> &pby_oby_prefixes,
                           const ObIArray<int64_t> &split,
                           const ObIArray<WinDistAlgo> &methods,
                           const int64_t splict_idx,
                           ObIArray<ObOpPseudoColumnRawExpr*> &status_exprs,
                           bool is_last_group,
                           WinFuncOpHelper &win_func_helper);
  int get_next_group_window_exprs(const ObIArray<OrderItem> &op_ordering,
                                  WinFuncOpHelper &win_func_helper,
                                  ObIArray<ObWinFunRawExpr*> &remaining_exprs,
                                  ObIArray<ObWinFunRawExpr*> &current_exprs);
  int gen_win_func_sort_keys(const ObIArray<OrderItem> &input_ordering,
                             WinFuncOpHelper &win_func_helper,
                             bool &is_valid);
  int classify_window_exprs(const WinFuncOpHelper &win_func_helper,
                            const ObIArray<OrderItem> &input_ordering,
                            const ObIArray<ObWinFunRawExpr*> &remaining_exprs,
                            ObIArray<ObWinFunRawExpr*> &no_need_sort_exprs,
                            ObIArray<ObWinFunRawExpr*> &no_need_order_exprs,
                            ObIArray<ObWinFunRawExpr*> &rest_win_func_exprs,
                            ObIArray<OrderItem> &best_sort_keys,
                            ObIArray<OrderItem> &possible_sort_keys);

  int calc_ndvs_and_pby_oby_prefix(const ObIArray<ObWinFunRawExpr*> &win_func_exprs,
                                   WinFuncOpHelper &win_func_helper,
                                   ObIArray<std::pair<int64_t, int64_t>> &pby_oby_prefixes);

  int check_win_func_pushdown(const int64_t dop,
                              const WinFuncOpHelper &win_func_helper,
                              ObIArray<bool> &pushdown_info);

  // Split adjusted window function (`sort_win_func_exprs()` called) into groups such that each
  // group hash same distribute method.
  //
  // @param distributed
  // @param win_func_exprs window functions to split, must adjusted by `sort_win_func_exprs()`
  // @param sort_key_ndvs
  // @param pby_oby_prefixes
  // @param[out] split split result which stores the %window_exprs array end positions
  // @param[out] methods window distribute method for each split array
  int split_win_funcs_by_dist_method(const bool distributed,
                                    const common::ObIArray<ObWinFunRawExpr *> &win_func_exprs,
                                    const ObIArray<double> &sort_key_ndvs,
                                    const ObIArray<std::pair<int64_t, int64_t>> &pby_oby_prefixes,
                                    const int64_t dop,
                                    const double card,
                                    const ObWindowDistHint *hint,
                                    const int64_t win_op_idx,
                                    common::ObIArray<int64_t> &split,
                                    common::ObIArray<WinDistAlgo> &methods);

  // Generate sort keys for window function
  // @param[out] order_item generated sort keys
  // @param[out] pby_prefix set the prefix count of %order_item for partition by exprs if not NULL
  // @return
  int get_sort_keys_for_window_function(const ObFdItemSet &fd_item_set,
                                        const EqualSets &equal_sets,
                                        const ObIArray<ObRawExpr*> &const_exprs,
                                        const ObWinFunRawExpr *win_expr,
                                        const ObIArray<OrderItem> &ordering,
                                        const ObIArray<ObWinFunRawExpr*> &winfunc_exprs,
                                        ObIArray<OrderItem> &order_items,
                                        int64_t *pby_prefix = NULL);

  int get_win_func_pby_oby_sort_prefix(const ObFdItemSet &fd_item_set,
                                       const EqualSets &equal_sets,
                                       const ObIArray<ObRawExpr*> &const_exprs,
                                       const ObWinFunRawExpr *win_expr,
                                       const ObIArray<OrderItem> &ordering,
                                       int64_t &pby_prefix,
                                       int64_t &pby_oby_prefix);

  // Get the PBY, PBY + OBY prefix count of %ordering
  // e.g.:
  //  sum(v) over (partition by a, b order by b, c)
  //  %ordering should start with [a, b, c] or [b, a, c]
  //  %pby_prefix will be 2 
  //  %pby_oby_prefix will b 3
  int get_winfunc_pby_oby_sort_prefix(const ObFdItemSet &fd_item_set,
                                      const EqualSets &equal_sets,
                                      const ObIArray<ObRawExpr*> &const_exprs,
                                      const ObWinFunRawExpr *win_expr,
                                      const ObIArray<OrderItem> &ordering,
                                      int64_t &pby_prefix,
                                      int64_t &pby_oby_prefix);

  int calc_partition_count(WinFuncOpHelper &win_func_helper);

  /**
   * @brief set_default_sort_directions
   * 确定窗口函数partition表达式的排序方向
   */
  int set_default_sort_directions(const ObIArray<ObWinFunRawExpr*> &winfunc_entries,
                                  const ObIArray<ObRawExpr *> &part_exprs,
                                  const EqualSets &equal_sets,
                                  ObIArray<ObOrderDirection> &directions);

  //init topn_filter,topn_const and is_fetch_with_ties flag
  int init_wf_topn_option(WinFuncOpHelper &win_func_helper, bool wf_topn_hint);

  int create_hash_window_function_plan(ObLogicalOperator *&top,
                                       const ObIArray<ObWinFunRawExpr*> &adjusted_winfunc_exprs,
                                       const ObIArray<ObRawExpr *> &qualify_filter_exprs,
                                       const ObRawExpr *limit_expr,
                                       const ObIArray<OrderItem> &sort_keys,
                                       const ObIArray<ObRawExpr*> &partition_exprs,
                                       const OrderItem &hash_sortkey,
                                       const bool is_pushdown,
                                       ObOpPseudoColumnRawExpr *wf_aggr_status_expr,
                                       const ObIArray<bool> &pushdown_info);

  int sort_window_functions(const ObIArray<ObWinFunRawExpr *> &win_func_exprs,
                            ObIArray<ObWinFunRawExpr *> &ordered_win_func_exprs,
                            const ObIArray<std::pair<int64_t, int64_t>> &pby_oby_prefixes,
                            ObIArray<std::pair<int64_t, int64_t>> &ordered_pby_oby_prefixes,
                            bool &ordering_changed);

  int match_window_function_parallel(const ObIArray<ObWinFunRawExpr *> &win_exprs,
                                     bool &can_parallel);

  int check_wf_range_dist_supported(ObWinFunRawExpr *win_expr,
                                    bool &supported);

  int check_wf_pushdown_supported(ObWinFunRawExpr *win_expr, bool &supported);

  int check_wf_part_topn_supported(const common::ObIArray<ObWinFunRawExpr *> &winfunc_exprs,
                                   const ObIArray<ObRawExpr*> &partition_exprs,
                                   bool &can_wf_topn);

  int get_pushdown_window_function_exchange_info(const ObIArray<ObWinFunRawExpr *> &win_exprs,
                                                 ObLogicalOperator *op,
                                                 ObExchangeInfo &exch_info);

  int extract_window_function_partition_exprs(WinFuncOpHelper &winfunc_helper);

  int allocate_window_function_as_top(const WinDistAlgo dist_algo,
                                      const ObIArray<ObWinFunRawExpr *> &win_exprs,
                                      const bool match_parallel,
                                      const bool is_partition_wise,
                                      const bool use_hash_sort,
                                      const bool use_topn_sort,
                                      const ObIArray<OrderItem> &sort_keys,
                                      ObLogicalOperator *&top,
                                      const ObIArray<ObRawExpr *> *qualify_filters,
                                      double origin_sort_card);
  int allocate_window_function_as_top(const WinDistAlgo dist_algo,
                                      const ObIArray<ObWinFunRawExpr *> &win_exprs,
                                      const bool match_parallel,
                                      const bool is_partition_wise,
                                      const bool use_hash_sort,
                                      const bool use_topn_sort,
                                      const int32_t role_type,
                                      const ObIArray<OrderItem> &sort_keys,
                                      const int64_t range_dist_keys_cnt,
                                      const int64_t range_dist_pby_prefix,
                                      ObLogicalOperator *&top,
                                      const ObIArray<ObRawExpr *> *qualify_filters,
                                      double origin_sort_card,
                                      ObOpPseudoColumnRawExpr *wf_aggr_status_expr = NULL,
                                      const ObIArray<bool> *pushdown_info = NULL);

  int candi_allocate_late_materialization();

  int get_late_materialization_operator(ObLogicalOperator *top,
                                        ObLogSort *&sort_op,
                                        ObLogTableScan *&table_scan);

  int adjust_late_materialization_structure(ObSelectStmt *stmt,
                                            ObLogicalOperator *join,
                                            ObLogTableScan *index_scan,
                                            ObLogTableScan *table_scan,
                                            TableItem *table_item);

  int convert_project_columns(ObSelectStmt *stmt,
                              ObIRawExprCopier &expr_copier,
                              uint64_t table_id,
                              TableItem *project_table_item,
                              ObIArray<uint64_t> &index_columns);

  int adjust_late_materialization_stmt_structure(ObSelectStmt *stmt,
                                                 ObLogTableScan *index_scan,
                                                 ObLogTableScan *table_scan,
                                                 TableItem *table_item);

  int adjust_late_materialization_plan_structure(ObLogicalOperator *join,
                                                 ObLogTableScan *index_scan,
                                                 ObLogTableScan *table_scan);

  int generate_late_materialization_info(ObSelectStmt *stmt,
                                         ObLogTableScan *index_scan,
                                         ObLogTableScan *&table_get,
                                         TableItem *&table_item);

  int generate_late_materialization_table_get(ObLogTableScan *index_scan,
                                              TableItem *table_item,
                                              uint64_t table_id,
                                              ObLogTableScan *&table_get);

  int generate_late_materialization_table_item(ObSelectStmt *stmt,
                                               uint64_t old_table_id,
                                               uint64_t new_table_id,
                                               TableItem *&new_table_item);

  int allocate_late_materialization_join_as_top(ObLogicalOperator *left_child,
                                                ObLogicalOperator *right_child,
                                                ObLogicalOperator *&join_path);

  int if_plan_need_late_materialization(ObLogicalOperator *top,
                                        ObLogTableScan *&index_scan,
                                        double &late_mater_cost,
                                        bool &need);

  int if_index_back_plan_need_late_materialization(ObLogSort *child_sort,
                                                  ObLogTableScan *table_scan,
                                                  ObIArray<uint64_t> &used_column_ids,
                                                  bool &need);

  int if_column_store_plan_need_late_materialization(ObLogSort *child_sort,
                                                    ObLogTableScan *table_scan,
                                                    ObIArray<uint64_t> &used_column_ids,
                                                    bool &need);

  int adjust_est_info_for_index_back_plan(ObLogTableScan *table_scan,
                                          ObIArray<uint64_t> &used_column_ids);

  int adjust_est_cost_info_for_column_store_plan(ObLogTableScan *table_scan,
                                                 ObIArray<uint64_t> &used_column_ids);

  int if_stmt_need_late_materialization(bool &need);

  int candi_allocate_unpivot();
  int allocate_unpivot_as_top(ObLogicalOperator *&old_top);

  int init_selectivity_metas_for_set(ObSelectLogPlan *sub_plan, const uint64_t child_offset);

  int inner_create_merge_group_plan(const ObIArray<ObRawExpr*> &reduce_exprs,
                                    const ObIArray<ObRawExpr*> &group_by_exprs,
                                    const ObIArray<ObOrderDirection> &group_directions,
                                    const ObIArray<ObAggFunRawExpr*> &aggr_items,
                                    const ObIArray<ObRawExpr*> &having_exprs,
                                    GroupingOpHelper &groupby_helper,
                                    ObLogicalOperator *&top,
                                    bool use_part_sort,
                                    const DistAlgo algo,
                                    bool &ignore_plan,
                                    bool can_ignore_merge = false);

  int check_external_table_scan(ObSelectStmt *stmt, bool &has_external_table);

  int contain_enum_set_rowkeys(const ObLogTableScan &table_scan, bool &contain);
  int candi_allocate_order_by_if_losted(ObIArray<OrderItem> &order_items);
  int check_aggr_with_keep(const ObIArray<ObAggFunRawExpr*>& aggr_items, bool &has_keep_aggr);

  DISALLOW_COPY_AND_ASSIGN(ObSelectLogPlan);
};
}//end of namespace sql
}//end of namespace oceanbase
#endif // _OB_LOGICAL_PLAN_TREE_H
