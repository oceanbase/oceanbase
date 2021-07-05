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

namespace oceanbase {
namespace sql {
class ObJoinOrder;
class AccessPath;
struct JoinInfo;
class Path;
class JoinPath;
class ObLogGroupBy;
class ObLogSet;
struct MergeKeyInfo;

/**
 *  Logical Plan for 'select' statement
 */
class ObSelectLogPlan : public ObLogPlan {
  friend class ::test::ObLogPlanTest_ob_explain_test_Test;

public:
  ObSelectLogPlan(ObOptimizerContext& ctx, const ObSelectStmt* select_stmt);
  virtual ~ObSelectLogPlan();

  // @brief Generate the logical plan
  virtual int generate_plan();

  // @brief GENERATE the raw PLAN tree which have not traversed
  int generate_raw_plan();

  // @brief Get all the group by exprs, rollup exprs and their directions.
  int get_groupby_rollup_exprs(const ObLogicalOperator* op, common::ObIArray<ObRawExpr*>& group_by_exprs,
      common::ObIArray<ObRawExpr*>& rollup_exprs, common::ObIArray<ObOrderDirection>& group_directions,
      common::ObIArray<ObOrderDirection>& rollup_directions);

  // @brief Get all the distinct exprs
  int get_distinct_exprs(ObLogicalOperator& op, common::ObIArray<ObRawExpr*>& distinct_exprs);

  // @brief Get all the select exprs
  int get_select_and_pullup_agg_subquery_exprs(common::ObIArray<ObRawExpr*>& subquery_exprs);
  int get_agg_subquery_exprs(ObRawExpr* expr, ObIArray<ObRawExpr*>& subquery_exprs);

  const ObSelectStmt* get_stmt() const
  {
    return reinterpret_cast<const ObSelectStmt*>(stmt_);
  }

  ObSelectStmt* get_stmt()
  {
    return const_cast<ObSelectStmt*>(static_cast<const ObSelectLogPlan&>(*this).get_stmt());
  }

private:
  // @brief Allocate a hash group by on top of a plan tree
  // ObLogicalOperator * candi_allocate_hash_group_by();

  // @brief Allocate a merge group by on top of a plan tree
  // ObLogicalOperator * candi_allocate_merge_group_by(ObIArray<OrderItem> &ordering);

  // @brief Allocate GROUP BY on top of plan candidates
  int candi_allocate_group_by();

  int create_merge_group_plan(const CandidatePlan& candidate_plan, common::ObIArray<ObRawExpr*>& group_exprs,
      common::ObIArray<ObRawExpr*>& rollup_exprs, common::ObIArray<ObRawExpr*>& having_exprs,
      common::ObIArray<ObOrderDirection>& group_directions, common::ObIArray<ObOrderDirection>& rollup_directions,
      CandidatePlan& merge_plan);

  // @brief Allocate DISTINCT by on top of plan candidates
  int candi_allocate_distinct();

  int create_merge_distinct_plan(const CandidatePlan& candidate_plan, ObIArray<ObRawExpr*>& distinct_exprs,
      ObIArray<ObOrderDirection>& directions, CandidatePlan& merge_plan);

  /**
   *  @brief  GENERATE the PLAN tree FOR "SET" operator (UNION/INTERSECT/EXCEPT)
   *  Warning:
   *  @param void
   *  @retval OB_SUCCESS execute success
   *  @retval otherwise, failed to generate the logical plan
   */
  int generate_plan_for_set();

  int allocate_set_distinct(CandidatePlan& candi_plan);

  /**
   * @brief candi_allocate_set
   * generate candidate plan for union/intersect/excpet stmt
   * @return
   */
  int candi_allocate_set(ObIArray<ObSelectLogPlan*>& child_plans);

  /**
   * @brief create_hash_set
   * create hash-based set operation
   * @return
   */
  int create_hash_set_plan(ObLogicalOperator* left_child, ObLogicalOperator* right_child, CandidatePlan& hash_plan);

  int generate_merge_set_plans(
      ObSelectLogPlan& left_log_plan, ObSelectLogPlan& right_log_plan, ObIArray<CandidatePlan>& merge_set_plans);

  int get_minimal_cost_set_plan(const MergeKeyInfo& left_merge_key, const ObIArray<ObRawExpr*>& right_set_exprs,
      const ObIArray<CandidatePlan>& right_candi, ObIArray<OrderItem>& best_order_items, ObLogicalOperator*& best_plan,
      bool& best_need_sort);

  int compute_sort_cost_for_set_plan(ObLogicalOperator& plan, ObIArray<OrderItem>& expected_ordering, double& cost);
  /**
   * @brief create_merge_set
   * create merge-based set operation
   * @return
   */
  int create_merge_set_plan(ObLogicalOperator* left_child, ObLogicalOperator* right_child,
      const ObIArray<ObOrderDirection>& order_directions, const ObIArray<int64_t>& map_array,
      const common::ObIArray<OrderItem>& left_sort_keys, const common::ObIArray<OrderItem>& right_sort_keys,
      const bool left_need_sort, const bool right_need_sort, CandidatePlan& merge_plan);

  int create_recursive_union_plan(
      ObLogicalOperator* left_child, ObLogicalOperator* right_child, CandidatePlan& union_all_plan);

  int create_union_all_plan(ObIArray<ObSelectLogPlan*>& child_plans, CandidatePlan& union_all_plan);

  /**
   * @brief generate_subquery_plan
   * generate sub query plan
   * @return
   */
  int generate_child_plan_for_set(
      const ObDMLStmt* sub_stmt, ObSelectLogPlan*& sub_plan, ObIArray<ObRawExpr*>& pushdown_filters);

  /**
   *  @brief  GENERATE the PLAN tree FOR PLAIN SELECT stmt
   *  Warning:
   *  @param void
   *  @retval OB_SUCCESS execute success
   *  @retval otherwise, failed to generate the logical plan
   */
  int generate_plan_for_plain_select();

  /**
   *  @brief  GENERATE the PLAN tree FOR select EXPR from dual
   *  Warning:
   *  @param void
   *  @retval OB_SUCCESS execute success
   *  @retval otherwise, failed to generate the logical plan
   */
  int generate_plan_expr_values();

  /**
   *  @brief  GENERATE the PLAN TOP FOR select stmt
   *  Warning:
   *  @param void
   *  @retval OB_SUCCESS execute success
   *  @retval otherwise, failed to generate the top operators of plan tree
   */
  int allocate_plan_top();

  int candi_allocate_subplan_filter_for_select_item();

  int allocate_distinct_as_top(ObLogicalOperator*& top, const AggregateAlgo algo, ObIArray<ObRawExpr*>& distinct_exprs,
      const ObIArray<OrderItem>& expected_ordering);

private:
  int decide_sort_keys_for_runion(
      const common::ObIArray<OrderItem>& order_items, common::ObIArray<OrderItem>& new_order_items);
  int init_merge_set_structure(common::ObIAllocator& allocator, const common::ObIArray<CandidatePlan>& plans,
      const common::ObIArray<ObRawExpr*>& select_exprs, const common::ObIArray<ObOrderDirection>& select_directions,
      common::ObIArray<MergeKeyInfo*>& merge_keys);

  int candi_allocate_window_function();

  int candi_allocate_temp_table_insert();

  int allocate_temp_table_insert_as_top(ObLogicalOperator*& top, const ObSqlTempTableInfo* temp_table_info);

  int candi_allocate_temp_table_transformation();

  int allocate_temp_table_transformation_as_top(
      ObLogicalOperator*& top, const ObIArray<ObLogicalOperator*>& temp_table_insert);

  typedef std::pair<ObWinFunRawExpr*, ObArray<OrderItem> > ObWinFuncEntry;

  int merge_and_allocate_window_functions(ObLogicalOperator*& top, const ObIArray<ObWinFunRawExpr*>& winfunc_exprs);

  int decide_sort_keys_for_window_function(const ObIArray<OrderItem>& ordering,
      const ObIArray<ObWinFuncEntry>& winfunc_entries, const EqualSets& equal_sets,
      const ObIArray<ObRawExpr*>& const_exprs, ObWinFuncEntry& win_entry);

  int set_default_sort_directions(const ObIArray<ObWinFuncEntry>& winfunc_entries,
      const ObIArray<ObRawExpr*>& part_exprs, const EqualSets& equal_sets, ObIArray<ObOrderDirection>& directions);

  int allocate_window_function_group(ObLogicalOperator*& top, const ObIArray<ObWinFunRawExpr*>& winfunc_exprs,
      const ObIArray<OrderItem>& sort_keys, bool need_sort);

  int allocate_window_function_above_node(const ObIArray<ObWinFunRawExpr*>& win_exprs, ObLogicalOperator*& node);

  int adjust_sort_expr_ordering(ObIArray<ObRawExpr*>& sort_exprs, ObIArray<ObOrderDirection>& sort_directions,
      ObLogicalOperator& child_op, bool check_win_func);

  int adjust_exprs_by_win_func(ObIArray<ObRawExpr*>& exprs, ObWinFunRawExpr& win_expr, const EqualSets& equal_sets,
      const ObIArray<ObRawExpr*>& const_exprs, ObIArray<ObOrderDirection>& directions);

  int generate_default_directions(const int64_t direction_num, ObIArray<ObOrderDirection>& directions);

  int candi_allocate_late_materialization();

  int adjust_late_materialization_structure(
      ObLogicalOperator* join, ObLogTableScan* index_scan, ObLogTableScan* table_scan, TableItem* table_item);

  int convert_project_columns(
      uint64_t table_id, uint64_t project_id, const ObString& project_table_name, ObIArray<uint64_t>& index_columns);

  int adjust_late_materialization_stmt_structure(
      ObLogTableScan* index_scan, ObLogTableScan* table_scan, TableItem* table_item);

  int adjust_late_materialization_plan_structure(
      ObLogicalOperator* join, ObLogTableScan* index_scan, ObLogTableScan* table_scan);

  int generate_late_materialization_info(
      ObLogTableScan* index_scan, ObLogTableScan*& table_get, TableItem*& table_item);

  int generate_late_materialization_table_get(
      ObLogTableScan* index_scan, TableItem* table_item, uint64_t table_id, ObLogTableScan*& table_get);

  int generate_late_materialization_table_item(
      uint64_t old_table_id, uint64_t new_table_id, TableItem*& new_table_item);

  int allocate_late_materialization_join_as_top(
      ObLogicalOperator* left_child, ObLogicalOperator* right_child, ObLogicalOperator*& join_path);

  int if_plan_need_late_materialization(ObLogicalOperator* top, ObLogTableScan*& index_scan, bool& need);

  int if_stmt_need_late_materialization(bool& need);

  int candi_allocate_unpivot();
  int allocate_unpivot_as_top(ObLogicalOperator*& old_top);

  /**
   * @brief candi_allocate_for_update
   * allocate for update operator
   * @return
   */
  int candi_allocate_for_update();

  int allocate_for_update_as_top(ObLogicalOperator*& top);

  int8_t group_type_;  // record group type allocated, for keep distinct type same with group type
  DISALLOW_COPY_AND_ASSIGN(ObSelectLogPlan);
};
}  // end of namespace sql
}  // end of namespace oceanbase
#endif  // _OB_LOGICAL_PLAN_TREE_H
