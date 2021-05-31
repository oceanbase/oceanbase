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

#define USING_LOG_PREFIX SQL_OPT
#include "sql/optimizer/ob_select_log_plan.h"
#include "lib/container/ob_array_iterator.h"
#include "lib/hash/ob_placement_hashmap.h"
#include "lib/hash/ob_placement_hashset.h"
#include "share/ob_cluster_version.h"
#include "sql/ob_sql_utils.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/rewrite/ob_query_range.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/optimizer/ob_log_operator_factory.h"
#include "sql/optimizer/ob_log_table_scan.h"
#include "sql/optimizer/ob_log_join.h"
#include "sql/optimizer/ob_log_group_by.h"
#include "sql/optimizer/ob_log_sort.h"
#include "sql/optimizer/ob_log_exchange.h"
#include "sql/optimizer/ob_log_subplan_filter.h"
#include "sql/optimizer/ob_join_order.h"
#include "sql/optimizer/ob_log_for_update.h"
#include "sql/optimizer/ob_log_limit.h"
#include "sql/optimizer/ob_log_set.h"
#include "sql/optimizer/ob_log_distinct.h"
#include "sql/optimizer/ob_log_expr_values.h"
#include "sql/optimizer/ob_log_window_function.h"
#include "sql/optimizer/ob_log_temp_table_insert.h"
#include "sql/optimizer/ob_log_temp_table_transformation.h"
#include "sql/optimizer/ob_log_plan_factory.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "sql/optimizer/ob_opt_est_cost.h"
#include "sql/optimizer/ob_optimizer_context.h"
#include "sql/optimizer/ob_opt_est_cost.h"
#include "sql/optimizer/ob_log_unpivot.h"
#include "common/ob_smart_call.h"
using namespace oceanbase;
using namespace sql;
using namespace oceanbase::common;
using namespace oceanbase::sql::log_op_def;
using share::schema::ObTableSchema;

namespace oceanbase {
namespace sql {
ObSelectLogPlan::ObSelectLogPlan(ObOptimizerContext& ctx, const ObSelectStmt* stmt)
    : ObLogPlan(ctx, stmt), group_type_(OB_UNSET_AGGREGATE_TYPE)
{}

ObSelectLogPlan::~ObSelectLogPlan()
{}

int ObSelectLogPlan::get_groupby_rollup_exprs(const ObLogicalOperator* op, ObIArray<ObRawExpr*>& group_by_exprs,
    common::ObIArray<ObRawExpr*>& rollup_exprs, ObIArray<ObOrderDirection>& group_directions,
    common::ObIArray<ObOrderDirection>& rollup_directions)
{
  int ret = OB_SUCCESS;
  // gather all group by columns
  const ObSelectStmt* stmt = get_stmt();
  if (OB_ISNULL(stmt) || OB_ISNULL(op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt from select log plan, operator is NULL", K(ret), K(stmt), K(op));
  } else {
    int64_t num = stmt->get_group_expr_size();
    for (int64_t i = 0; OB_SUCC(ret) && i < num; ++i) {
      ObRawExpr* group_expr = stmt->get_group_exprs().at(i);
      bool is_const = false;
      if (OB_ISNULL(group_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("group expr is NULL", K(ret), K(stmt->get_group_exprs()));
      } else if (OB_FAIL(ObOptimizerUtil::is_const_expr(group_expr, op->get_output_const_exprs(), is_const))) {
        LOG_WARN("check is const expr failed", K(ret));
      } else if (is_const || (group_expr->has_flag(CNT_COLUMN) && !group_expr->has_flag(CNT_SUB_QUERY) &&
                                 !group_expr->get_expr_levels().has_member(stmt->get_current_level()))) {
        // no need to group a const expr, skip it
      } else if (OB_FAIL(group_by_exprs.push_back(group_expr))) {
        LOG_WARN("failed to push array", K(ret));
      } else { /*do nothing*/
      }
    }
    if (OB_SUCC(ret)) {
      ObSEArray<ObRawExpr*, 4> opt_group_exprs;
      if (OB_FAIL(op->simplify_exprs(group_by_exprs, opt_group_exprs))) {
        LOG_WARN("failed to simplify group exprs", K(ret));
      } else if (OB_FAIL(group_by_exprs.assign(opt_group_exprs))) {
        LOG_WARN("failed to appen exprs", K(ret));
      } else if (OB_FAIL(ObOptimizerUtil::find_stmt_expr_direction(
                     *stmt, group_by_exprs, op->get_ordering_output_equal_sets(), group_directions))) {
      } else if (OB_FAIL(rollup_exprs.assign(stmt->get_rollup_exprs()))) {
        LOG_WARN("failed to assign to rollop exprs.", K(ret));
      } else if (rollup_exprs.count() > 0) {
        bool has_rollup_dir = stmt->has_rollup_dir();
        if (OB_UNLIKELY(has_rollup_dir && (stmt->get_rollup_dir_size() != rollup_exprs.count()))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to check rollup exprs and directions count.", K(ret));
        } else { /* do nothing. */
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < rollup_exprs.count(); i++) {
          ObOrderDirection dir = has_rollup_dir ? stmt->get_rollup_dirs().at(i) : default_asc_direction();
          if (OB_FAIL(rollup_directions.push_back(dir))) {
            LOG_WARN("failed to push back into directions.", K(ret));
          } else { /* do nothing. */
          }
        }
      }  // do nothing
    }
  }
  return ret;
}

int ObSelectLogPlan::candi_allocate_group_by()
{
  int ret = OB_SUCCESS;
  ObSelectStmt* stmt = get_stmt();
  ObSEArray<ObRawExpr*, 8> candi_subquery_exprs;
  ObSEArray<ObRawExpr*, 8> having_subquery_exprs;
  ObSEArray<ObRawExpr*, 8> having_normal_exprs;
  ObLogicalOperator* best_plan = NULL;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null stmt", K(ret));
  } else if (OB_FAIL(append(candi_subquery_exprs, stmt->get_group_exprs())) ||
             OB_FAIL(append(candi_subquery_exprs, stmt->get_rollup_exprs())) ||
             OB_FAIL(append(candi_subquery_exprs, stmt->get_aggr_items()))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else if (OB_FAIL(candi_allocate_subplan_filter_for_exprs(candi_subquery_exprs))) {
    LOG_WARN("failed to allocate subplan filter for exprs", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::classify_subquery_exprs(
                 stmt->get_having_exprs(), having_subquery_exprs, having_normal_exprs))) {
    LOG_WARN("failed to classify subquery exprs", K(ret));
  } else if (OB_FAIL(get_current_best_plan(best_plan))) {
    LOG_WARN("failed to get best plan", K(ret));
  } else if (OB_ISNULL(best_plan)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (stmt->get_group_expr_size() > 0 || stmt->get_rollup_expr_size() > 0) {
    // Note: if stmt->get_group_expr_size() > 0 and group_by_exprs.count() == 0, doesn't mean we
    // will choose scalar group by, because SGB outputs NULL when input is empty, while HGB/MGB outputs
    //'empty set' in the same scenario. In this scenario, we choose MGB, and output row count
    // should be fixed to 1 row, and sort should not be allocated.
    ObSEArray<ObRawExpr*, 4> group_by_exprs;
    ObSEArray<ObRawExpr*, 4> rollup_exprs;
    ObSEArray<ObOrderDirection, 4> group_directions;
    ObSEArray<ObOrderDirection, 4> rollup_directions;
    bool is_unique = false;
    bool having_has_rownum = false;
    if (OB_FAIL(ObTransformUtils::check_has_rownum(stmt->get_having_exprs(), having_has_rownum))) {
      LOG_WARN("check has_rownum error", K(ret));
    } else if (OB_FAIL(get_groupby_rollup_exprs(
                   best_plan, group_by_exprs, rollup_exprs, group_directions, rollup_directions))) {
      LOG_WARN("Get group by exprs error", K(ret));
    } else if (0 == stmt->get_aggr_item_size() && !stmt->has_rollup() &&
               OB_FAIL(ObOptimizerUtil::is_exprs_unique(group_by_exprs,
                   best_plan->get_table_set(),
                   best_plan->get_fd_item_set(),
                   best_plan->get_ordering_output_equal_sets(),
                   best_plan->get_output_const_exprs(),
                   is_unique))) {
      LOG_WARN("failed to check group by exprs is unique", K(ret));
    } else if (is_unique && !having_has_rownum) {
      LOG_TRACE("group by expr is unique, no need group by", K(group_by_exprs));
      if (OB_FAIL(candi_allocate_filter(having_normal_exprs))) {
        LOG_WARN("failed to allocate filter", K(ret));
      } else { /*do nothing*/
      }
    } else {
      bool const_group_by = (stmt->get_group_expr_size() > 0) && (group_by_exprs.count() == 0);
      ObSEArray<CandidatePlan, 4> merge_aggregate_plan;
      ObSEArray<CandidatePlan, 4> hash_aggregate_plan;
      const ObStmtHint& stmt_hint = stmt->get_stmt_hint();
      int64_t group_by_type = stmt_hint.aggregate_;

      if (!((group_by_type & OB_NO_USE_HASH_AGGREGATE) || (group_by_type & OB_USE_HASH_AGGREGATE))) {
        group_by_type = OB_NO_USE_HASH_AGGREGATE | OB_USE_HASH_AGGREGATE;
      }
      if (0 == stmt->get_group_expr_size() || const_group_by || stmt->has_rollup() ||
          stmt->has_distinct_or_concat_agg()) {
        // Group_concat and distinct aggregation hold all input rows temporary,
        // too much memory consumption for hash aggregate.
        group_by_type = OB_NO_USE_HASH_AGGREGATE;
      }

      // create hash group plans
      if (OB_SUCC(ret) && (group_by_type & OB_USE_HASH_AGGREGATE)) {
        ObSEArray<OrderItem, 4> dummy_expected_ordering;
        if (OB_FAIL(allocate_group_by_as_top(best_plan,
                HASH_AGGREGATE,
                group_by_exprs,
                rollup_exprs,
                stmt->get_aggr_items(),
                having_normal_exprs,
                dummy_expected_ordering,
                stmt->is_from_pivot()))) {
          LOG_WARN("failed to allocate hash group by operator", K(ret));
        } else if (OB_FAIL(hash_aggregate_plan.push_back(CandidatePlan(best_plan)))) {
          LOG_WARN("Failed to push back hash group plan", K(ret));
        } else {
          group_type_ |= OB_USE_HASH_AGGREGATE;
        }
      }

      // create merge group plans
      if (OB_SUCC(ret) && (group_by_type & OB_NO_USE_HASH_AGGREGATE)) {
        CandidatePlan merge_plan;
        for (int64_t i = 0; OB_SUCC(ret) && i < candidates_.candidate_plans_.count(); i++) {
          merge_plan.reset();
          if (OB_FAIL(create_merge_group_plan(candidates_.candidate_plans_.at(i),
                  group_by_exprs,
                  rollup_exprs,
                  having_normal_exprs,
                  group_directions,
                  rollup_directions,
                  merge_plan))) {
            LOG_WARN("Failed to allocate merge group plan", K(ret));
          } else if (OB_FAIL(merge_aggregate_plan.push_back(merge_plan))) {
            LOG_WARN("failed to add merge group candidate plan", K(ret));
          } else { /*do nothing*/
          }
        }
        if (OB_SUCC(ret)) {
          group_type_ |= OB_NO_USE_HASH_AGGREGATE;
        }
      }
      // add plans to candidates
      if (OB_SUCC(ret)) {
        ObSEArray<CandidatePlan, 4> all_plans;
        if (group_by_type & OB_NO_USE_HASH_AGGREGATE) {
          if (OB_FAIL(append(all_plans, merge_aggregate_plan))) {
            LOG_WARN("failed to append merge plan", K(ret));
          } else { /*do nothing*/
          }
        }
        if (OB_SUCC(ret) && (group_by_type & OB_USE_HASH_AGGREGATE)) {
          if (OB_FAIL(append(all_plans, hash_aggregate_plan))) {
            LOG_WARN("failed to append hash plan", K(ret));
          } else { /*do nothing*/
          }
        }
        if (OB_SUCC(ret)) {
          int64_t check_scope = OrderingCheckScope::CHECK_WINFUNC | OrderingCheckScope::CHECK_DISTINCT |
                                OrderingCheckScope::CHECK_SET | OrderingCheckScope::CHECK_ORDERBY;
          if (OB_FAIL(update_plans_interesting_order_info(all_plans, check_scope))) {
            LOG_WARN("failed to update plans interesting order info", K(ret));
          } else if (OB_FAIL(prune_and_keep_best_plans(all_plans))) {
            LOG_WARN("failed to add plan", K(ret));
          } else { /*do nothing*/
          }
        }
      }
    }
  } else {
    // generate scala group by plan
    ObSEArray<OrderItem, 1> dummy_expected_ordering;
    if (OB_FAIL(allocate_group_by_as_top(best_plan,
            SCALAR_AGGREGATE,
            stmt->get_group_exprs(),
            stmt->get_rollup_exprs(),
            stmt->get_aggr_items(),
            having_normal_exprs,
            dummy_expected_ordering,
            stmt->is_from_pivot()))) {
      LOG_WARN("failed to allocate group by operator", K(ret));
    } else {
      candidates_.candidate_plans_.reset();
      if (OB_FAIL(candidates_.candidate_plans_.push_back(CandidatePlan(best_plan)))) {
        LOG_WARN("Add merge group plan error", K(ret));
      } else {
        candidates_.plain_plan_ = std::pair<double, int64_t>(best_plan->get_cost(), 0);
      }
    }
  }
  // allocate subquery in having expr
  if (OB_SUCC(ret) && !having_subquery_exprs.empty()) {
    if (OB_FAIL(candi_allocate_subplan_filter(having_subquery_exprs, true))) {
      LOG_WARN("failed to allocate having exprs", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObSelectLogPlan::allocate_window_function_above_node(
    const ObIArray<ObWinFunRawExpr*>& win_exprs, ObLogicalOperator*& node)
{
  int ret = OB_SUCCESS;
  ObLogWindowFunction* window_function = NULL;
  if (OB_ISNULL(node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("NULL ptr", K(ret), K(node));
  } else if (OB_ISNULL(window_function = static_cast<ObLogWindowFunction*>(
                           get_log_op_factory().allocate(*this, LOG_WINDOW_FUNCTION)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("allocate memory for ObLogWindowFunction failed", K(ret));
  } else if (OB_FAIL(append(window_function->get_window_exprs(), win_exprs))) {
    LOG_WARN("failed to add window expr", K(ret));
  } else {
    window_function->set_child(ObLogicalOperator::first_child, node);
    if (node->is_plan_root()) {
      node->set_is_plan_root(false);
      window_function->mark_is_plan_root();
      set_plan_root(window_function);
    }
    if (OB_FAIL(window_function->compute_property())) {
      LOG_WARN("failed to compute property", K(ret));
    } else {
      node = window_function;
    }
  }
  return ret;
}

int ObSelectLogPlan::candi_allocate_distinct()
{
  int ret = OB_SUCCESS;
  const ObSelectStmt* stmt = get_stmt();
  ObSEArray<ObRawExpr*, 8> distinct_exprs;
  ObLogicalOperator* best_plan = NULL;
  bool is_unique = false;
  ObSEArray<ObRawExpr*, 8> candi_subquery_exprs;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(stmt->get_select_exprs(candi_subquery_exprs))) {
    LOG_WARN("failed to get select exprs", K(ret));
  } else if (OB_FAIL(candi_allocate_subplan_filter_for_exprs(candi_subquery_exprs))) {
    LOG_WARN("failed to allocate subplan filter for exprs", K(ret));
  } else if (OB_FAIL(get_current_best_plan(best_plan))) {
    LOG_WARN("failed to get current best plan", K(ret));
  } else if (OB_ISNULL(best_plan)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(get_distinct_exprs(*best_plan, distinct_exprs))) {
    LOG_WARN("Failed to get select columns", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::is_exprs_unique(distinct_exprs,
                 best_plan->get_table_set(),
                 best_plan->get_fd_item_set(),
                 best_plan->get_ordering_output_equal_sets(),
                 best_plan->get_output_const_exprs(),
                 is_unique))) {
    LOG_WARN("failed to check is unique", K(ret));
  } else if (is_unique) {
    LOG_TRACE("distinct exprs is unique, no need distinct", K(distinct_exprs));
  } else if (distinct_exprs.empty()) {
    // if all the distinct exprs are const, we add limit operator instead of distinct operator
    ObConstRawExpr* limit_expr = NULL;
    ObSEArray<OrderItem, 4> dummy_order_items;
    if (OB_FAIL(ObRawExprUtils::build_const_int_expr(
            get_optimizer_context().get_expr_factory(), ObIntType, 1, limit_expr))) {
      LOG_WARN("failed to create const expr", K(ret));
    } else if (OB_ISNULL(limit_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("limit_expr is null", K(ret));
    } else if (OB_FAIL(candi_allocate_limit(limit_expr, NULL, NULL, dummy_order_items, false, false, false, false))) {
      LOG_WARN("failed to allocate limit operator", K(ret));
    } else { /*do nothing*/
    }
  } else {
    ObSEArray<CandidatePlan, 4> merge_distinct_plan;
    ObSEArray<CandidatePlan, 4> hash_distinct_plan;
    const ObStmtHint& stmt_hint = stmt->get_stmt_hint();
    int8_t distinct_type = stmt_hint.aggregate_;

    // use the same aggregate type with group
    if (group_type_ != OB_UNSET_AGGREGATE_TYPE) {
      distinct_type = group_type_;
    }
    if (!((distinct_type & OB_NO_USE_HASH_AGGREGATE) || (distinct_type & OB_USE_HASH_AGGREGATE))) {
      distinct_type = OB_NO_USE_HASH_AGGREGATE | OB_USE_HASH_AGGREGATE;
    }
    // create hash distinct plan
    if (OB_SUCC(ret) && (distinct_type & OB_USE_HASH_AGGREGATE)) {
      ObSEArray<OrderItem, 4> dummy_expected_ordering;
      if (OB_FAIL(allocate_distinct_as_top(best_plan, HASH_AGGREGATE, distinct_exprs, dummy_expected_ordering))) {
        LOG_WARN("failed to allocate hash distinct operator", K(ret));
      } else if (OB_FAIL(hash_distinct_plan.push_back(CandidatePlan(best_plan)))) {
        LOG_WARN("failed to push back hash distinct candidate plan", K(ret));
      } else { /*do nothing*/
      }
    }

    // create merge distinct plan
    if (OB_SUCC(ret) && (distinct_type & OB_NO_USE_HASH_AGGREGATE)) {
      CandidatePlan merge_plan;
      ObSEArray<ObOrderDirection, 4> distinct_directions;
      if (OB_FAIL(generate_default_directions(distinct_exprs.count(), distinct_directions))) {
        LOG_WARN("failed to generate default directions", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < candidates_.candidate_plans_.count(); i++) {
        merge_plan.reset();
        if (OB_FAIL(create_merge_distinct_plan(
                candidates_.candidate_plans_.at(i), distinct_exprs, distinct_directions, merge_plan))) {
          LOG_WARN("failed to allocate merge distinct plan", K(ret));
        } else if (OB_FAIL(merge_distinct_plan.push_back(merge_plan))) {
          LOG_WARN("failed to add merge distinct candidate plan", K(ret));
        } else { /*do nothing*/
        }
      }
    }

    // add plans to candidates
    if (OB_SUCC(ret)) {
      ObSEArray<CandidatePlan, 4> all_plans;
      if (distinct_type & OB_NO_USE_HASH_AGGREGATE) {
        if (OB_FAIL(append(all_plans, merge_distinct_plan))) {
          LOG_WARN("failed to append merge plan", K(ret));
        } else { /*do nothing*/
        }
      }
      if (OB_SUCC(ret) && (distinct_type & OB_USE_HASH_AGGREGATE)) {
        if (OB_FAIL(append(all_plans, hash_distinct_plan))) {
          LOG_WARN("failed to append hash plan", K(ret));
        } else { /*do nothing*/
        }
      }
      if (OB_SUCC(ret)) {
        int64_t check_scope = OrderingCheckScope::CHECK_SET | OrderingCheckScope::CHECK_ORDERBY;
        if (OB_FAIL(update_plans_interesting_order_info(all_plans, check_scope))) {
          LOG_WARN("failed to update plans interesting order info", K(ret));
        } else if (OB_FAIL(prune_and_keep_best_plans(all_plans))) {
          LOG_WARN("Failed to add plans", K(ret));
        } else { /* do nothing*/
        }
      }
    }
  }
  return ret;
}

int ObSelectLogPlan::generate_plan_for_set()
{
  int ret = OB_SUCCESS;
  ObSelectStmt* select_stmt = get_stmt();
  ObSEArray<ObSelectLogPlan*, 2> child_plans;
  ObSEArray<ObRawExpr*, 8> remain_filters;
  ObSQLSessionInfo* session_info = NULL;
  ObRawExprFactory* expr_factory = NULL;
  if (OB_ISNULL(select_stmt) || OB_ISNULL(session_info = get_optimizer_context().get_session_info()) ||
      OB_ISNULL(expr_factory = &get_optimizer_context().get_expr_factory())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(select_stmt), K(ret));
  } else if (OB_UNLIKELY(!select_stmt->has_set_op()) || OB_UNLIKELY(2 > select_stmt->get_set_query().count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected set_op stmt", K(ret));
  } else {
    ObIArray<ObSelectStmt*>& child_stmts = select_stmt->get_set_query();
    const int64_t child_size = child_stmts.count();
    const bool is_set_distinct = select_stmt->is_set_distinct();
    ObSEArray<ObRawExpr*, 8> child_input_filters;
    ObSEArray<ObRawExpr*, 8> child_candi_filters;
    ObSEArray<ObRawExpr*, 8> child_rename_filters;
    ObSEArray<ObRawExpr*, 8> child_remain_filters;
    bool can_pushdown = false;
    ObSelectStmt* child_stmt = NULL;
    ObSelectLogPlan* child_plan = NULL;
    for (int64 i = 0; OB_SUCC(ret) && i < child_size; ++i) {
      child_input_filters.reuse();
      child_candi_filters.reuse();
      child_rename_filters.reuse();
      child_remain_filters.reuse();
      if (OB_ISNULL(child_stmt = child_stmts.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null stmt", K(child_stmt), K(ret));
      } else if (OB_FALSE_IT(child_stmt->set_parent_set_distinct(is_set_distinct))) {
      } else if (!pushdown_filters_.empty() && OB_FAIL(child_input_filters.assign(pushdown_filters_))) {
        LOG_WARN("failed to copy exprs", K(ret));
      } else if (!child_input_filters.empty() && OB_FAIL(ObOptimizerUtil::pushdown_filter_into_subquery(*select_stmt,
                                                     *child_stmt,
                                                     get_optimizer_context(),
                                                     child_input_filters,
                                                     child_candi_filters,
                                                     child_remain_filters,
                                                     can_pushdown))) {
        LOG_WARN("pushdown filters into left query failed", K(ret));
      } else if (OB_FAIL(ObOptimizerUtil::get_set_op_remain_filter(
                     *select_stmt, child_remain_filters, remain_filters, 0 == i))) {
        LOG_WARN("get remain filters failed", K(ret));
      } else if (!child_candi_filters.empty() && OB_FAIL(ObOptimizerUtil::rename_pushdown_filter(*select_stmt,
                                                     *child_stmt,
                                                     OB_INVALID,
                                                     session_info,
                                                     *expr_factory,
                                                     child_candi_filters,
                                                     child_rename_filters))) {
        LOG_WARN("failed to rename pushdown filter", K(ret));
      } else if (OB_FAIL(generate_child_plan_for_set(child_stmt, child_plan, child_rename_filters))) {
        LOG_WARN("failed to generate left subquery plan", K(ret));
      } else if (OB_FAIL(child_plans.push_back(child_plan))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(candi_allocate_set(child_plans))) {
      LOG_WARN("failed to allocate 'UNION/INTERSECT/EXCEPT' operator", "sql", select_stmt->get_sql_stmt(), K(ret));
    } else if (!remain_filters.empty() && OB_FAIL(candi_allocate_filter(remain_filters))) {
      LOG_WARN("failed to allocate filter", K(ret));
    } else {
      LOG_TRACE("'UNION/INTERSECT/EXCEPT' operator is allocated", "sql", select_stmt->get_sql_stmt(), K(ret));
    }

    // 1.5 allocate 'order-by' if needed
    ObSEArray<OrderItem, 4> order_items;
    if (OB_SUCC(ret) && select_stmt->has_order_by()) {
      if (OB_FAIL(candi_allocate_order_by(order_items))) {
        LOG_WARN("failed to allocate 'ORDER-BY' operator", "sql", select_stmt->get_sql_stmt(), K(ret));
      } else {
        LOG_TRACE("'ORDER-BY' operator is allocated", "sql", select_stmt->get_sql_stmt(), K(ret));
      }
    }

    // 1.6 allocate 'limit' if needed
    if (OB_SUCC(ret) && select_stmt->has_limit()) {
      if (OB_FAIL(candi_allocate_limit(order_items))) {
        LOG_WARN("failed to allocate 'LIMIT' operator", "sql", select_stmt->get_sql_stmt(), K(ret));
      } else {
        LOG_TRACE("'LIMIT' operator is allocated", "sql", select_stmt->get_sql_stmt(), K(ret));
      }
    }

    // 1.7 allocate 'select_into' if needed
    if (OB_SUCC(ret)) {
      if (select_stmt->has_select_into()) {
        LOG_TRACE("SQL has select_into clause", "sql", select_stmt->get_sql_stmt(), K(ret));
        if (OB_FAIL(candi_allocate_select_into())) {
          LOG_WARN("failed to allocate 'SELECT-INTO' operator", "sql", select_stmt->get_sql_stmt(), K(ret));
        } else {
          // do nothing
        }
      }
    }

    if (OB_SUCC(ret)) {
      ObLogicalOperator* best_plan = NULL;
      if (OB_FAIL(get_current_best_plan(best_plan))) {
        LOG_WARN("failed to choose final plan", K(ret));
      } else if (OB_FAIL(set_final_plan_root(best_plan))) {
        LOG_WARN("failed to use final plan", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObSelectLogPlan::candi_allocate_set(ObIArray<ObSelectLogPlan*>& child_plans)
{
  int ret = OB_SUCCESS;
  const ObSelectStmt* select_stmt = get_stmt();
  ObLogicalOperator* left_best_plan_root = NULL;
  ObLogicalOperator* right_best_plan_root = NULL;
  CandidatePlan candi_plan;
  ObSEArray<CandidatePlan, 2> hash_set_plans;
  ObSEArray<CandidatePlan, 4> merge_set_plans;
  ObSEArray<CandidatePlan, 4> all_plans;
  if (OB_ISNULL(select_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (!select_stmt->is_recursive_union() && ObSelectStmt::UNION == select_stmt->get_set_op() &&
             !(select_stmt->is_set_distinct() && 2 == child_plans.count())) {  /// union
    if (OB_FAIL(create_union_all_plan(child_plans, candi_plan))) {
      LOG_WARN("failed to create plan for union all", K(ret));
    } else if (2 < child_plans.count() && select_stmt->is_set_distinct() &&
               OB_FAIL(allocate_set_distinct(candi_plan))) {
      LOG_WARN("failed to allocate set distinct", K(ret));
    } else if (OB_FAIL(all_plans.push_back(candi_plan))) {
      LOG_WARN("failed to pushback", K(ret));
    }
  } else if (2 != child_plans.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected child count", K(ret), K(child_plans.count()), K(select_stmt->get_set_op()));
  } else if (OB_ISNULL(child_plans.at(0)) || OB_ISNULL(child_plans.at(1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parameters have null", K(ret), K(child_plans.at(0)), K(child_plans.at(1)));
  } else if (OB_ISNULL(left_best_plan_root = child_plans.at(0)->get_plan_root()) ||
             OB_ISNULL(right_best_plan_root = child_plans.at(1)->get_plan_root())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("plans' roots have null", K(ret), K(left_best_plan_root), K(right_best_plan_root));
  } else if (select_stmt->is_recursive_union()) {  /// recusive union all
    if (OB_FAIL(create_recursive_union_plan(left_best_plan_root, right_best_plan_root, candi_plan))) {
      LOG_WARN("failed to create plan for recursive union", K(ret));
    } else if (OB_FAIL(all_plans.push_back(candi_plan))) {
      LOG_WARN("failed to push back merge set plan", K(ret));
    }
  } else {
    if (select_stmt->get_set_op() == ObSelectStmt::INTERSECT &&
        left_best_plan_root->get_card() < right_best_plan_root->get_card()) {
      ObLogicalOperator* tmp = left_best_plan_root;
      left_best_plan_root = right_best_plan_root;
      right_best_plan_root = tmp;
    }

    if (OB_FAIL(create_hash_set_plan(left_best_plan_root, right_best_plan_root, candi_plan))) {
      LOG_WARN("failed to create hash set", K(ret));
    } else if (OB_FAIL(all_plans.push_back(candi_plan))) {
      LOG_WARN("failed to add hash plan", K(ret));
    } else if (OB_FAIL(generate_merge_set_plans(*child_plans.at(0), *child_plans.at(1), all_plans))) {
      // generate merge set plans
      LOG_WARN("failed to generate merge set plans", K(ret));
    } else { /*do nothing*/
    }
  }
  if (OB_SUCC(ret)) {
    int64_t check_scope = OrderingCheckScope::CHECK_SET | OrderingCheckScope::CHECK_ORDERBY;
    if (OB_FAIL(update_plans_interesting_order_info(all_plans, check_scope))) {
      LOG_WARN("failed to update plans interesting order info", K(ret));
    } else if (OB_FAIL(prune_and_keep_best_plans(all_plans))) {
      LOG_WARN("failed to add all plans", K(ret));
    }
  }
  return ret;
}

int ObSelectLogPlan::allocate_set_distinct(CandidatePlan& candi_plan)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 8> distinct_exprs;
  ObLogicalOperator* top_op = candi_plan.plan_tree_;
  ObSEArray<OrderItem, 4> dummy_expected_ordering;
  ObLogicalOperator* distinct_op = NULL;
  if (OB_ISNULL(get_stmt()) || OB_ISNULL(top_op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(get_stmt()->get_select_exprs(distinct_exprs))) {
    LOG_WARN("failed to get current best plan", K(ret));
  } else if (OB_ISNULL(distinct_op = get_log_op_factory().allocate(*this, LOG_DISTINCT))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to allocate distinct operator", K(ret));
  } else {
    ObLogDistinct* distinct = static_cast<ObLogDistinct*>(distinct_op);
    distinct->set_child(ObLogicalOperator::first_child, top_op);
    distinct->set_algo_type(HASH_AGGREGATE);
    if (OB_FAIL(distinct->set_distinct_exprs(distinct_exprs))) {
      LOG_WARN("failed to set group by columns", K(ret));
    } else if (OB_FAIL(distinct->set_expected_ordering(dummy_expected_ordering))) {
      LOG_WARN("failed to set expected ordering", K(ret));
    } else if (OB_FAIL(distinct->compute_property())) {
      LOG_WARN("failed to compute property", K(ret));
    } else {
      double distinct_cost = 0.0;
      distinct->set_card(top_op->get_card());
      distinct_cost = ObOptEstCost::cost_hash_distinct(top_op->get_card(), top_op->get_card(), distinct_exprs);
      distinct->set_cost(top_op->get_cost() + distinct_cost);
      distinct->set_op_cost(distinct_cost);
      candi_plan.plan_tree_ = distinct;
    }
  }
  return ret;
}

int ObSelectLogPlan::create_union_all_plan(ObIArray<ObSelectLogPlan*>& child_plans, CandidatePlan& union_all_plan)
{
  int ret = OB_SUCCESS;
  const ObSelectStmt* select_stmt = get_stmt();
  ObLogSet* set_op = NULL;
  if (OB_ISNULL(select_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(select_stmt));
  } else if (child_plans.count() < 2) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected child_plans", K(ret), K(child_plans));
  } else if (GET_MIN_CLUSTER_VERSION() <= CLUSTER_VERSION_2274 && child_plans.count() > 2) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected child_plans", K(ret), K(child_plans.count()), K(GET_MIN_CLUSTER_VERSION()));
  } else if (OB_ISNULL((set_op = static_cast<ObLogSet*>(get_log_op_factory().allocate(*this, LOG_SET))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("Allocate memory for ObLogSet failed", K(ret));
  } else {
    set_op->assign_set_distinct(false);
    set_op->assign_set_op(ObSelectStmt::UNION);
    set_op->set_algo_type(MERGE_SET);
    for (int64_t i = 0; OB_SUCC(ret) && i < child_plans.count(); ++i) {
      if (OB_ISNULL(child_plans.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret), K(select_stmt));
      } else {
        set_op->set_child(i, child_plans.at(i)->get_plan_root());
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(set_op->compute_property())) {
      LOG_WARN("failed to compute property", K(ret));
    } else {
      union_all_plan.plan_tree_ = set_op;
    }
  }
  return ret;
}

int ObSelectLogPlan::create_recursive_union_plan(
    ObLogicalOperator* left_child, ObLogicalOperator* right_child, CandidatePlan& union_all_plan)
{
  int ret = OB_SUCCESS;
  const ObSelectStmt* select_stmt = get_stmt();
  ObLogSet* set_op = NULL;
  ObSEArray<OrderItem, 8> candi_order_items;
  ObSEArray<OrderItem, 8> opt_order_items;
  ObSelectLogPlan* left_plan = NULL;
  bool need_sort = false;
  if (OB_ISNULL(left_child) || OB_ISNULL(right_child) || OB_ISNULL(select_stmt) ||
      OB_UNLIKELY(select_stmt->is_set_distinct()) || OB_UNLIKELY(!select_stmt->is_recursive_union())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(left_child), K(right_child), K(select_stmt));
  } else if (OB_ISNULL((set_op = static_cast<ObLogSet*>(get_log_op_factory().allocate(*this, LOG_SET))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("Allocate memory for ObLogSet failed", K(ret));
  } else if (OB_FAIL(set_op->set_search_ordering(select_stmt->get_search_by_items()))) {
    LOG_WARN("set search order failed", K(ret));
  } else if (OB_FAIL(set_op->set_cycle_items(select_stmt->get_cycle_items()))) {
    LOG_WARN("set cycle item failed", K(ret));
  } else if (OB_ISNULL(left_plan = static_cast<ObSelectLogPlan*>(left_child->get_plan()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("left plan is null", K(ret));
  } else if (OB_FAIL(left_plan->decide_sort_keys_for_runion(select_stmt->get_search_by_items(), candi_order_items))) {
    LOG_WARN("failed to allocate sort as root", K(ret));
  } else if (OB_FAIL(left_child->simplify_ordered_exprs(candi_order_items, opt_order_items))) {
    LOG_WARN("failed to simplify ordered exprs", K(ret));
  } else if (opt_order_items.count() > 0 &&
             OB_FAIL(left_child->check_need_sort_above_node(opt_order_items, need_sort))) {
    LOG_WARN("failed to check need sort above node", K(ret));
  } else if (need_sort && OB_FAIL(left_plan->allocate_sort_as_top(left_child, opt_order_items))) {
    LOG_WARN("failed to allocate sort as top", K(ret));
  } else if (OB_ISNULL(left_child)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to allocate sort for left child", K(ret));
  } else {
    set_op->set_left_child(left_child);
    set_op->set_right_child(right_child);
    set_op->assign_set_distinct(false);
    set_op->assign_set_op(select_stmt->get_set_op());
    set_op->set_algo_type(MERGE_SET);
    set_op->set_recursive_union(true);
    set_op->set_is_breadth_search(select_stmt->is_breadth_search());
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(set_op->compute_property())) {
    LOG_WARN("failed to compute property", K(ret));
  } else {
    union_all_plan.plan_tree_ = set_op;
  }
  return ret;
}

int ObSelectLogPlan::generate_merge_set_plans(
    ObSelectLogPlan& left_log_plan, ObSelectLogPlan& right_log_plan, ObIArray<CandidatePlan>& merge_set_plans)
{
  int ret = OB_SUCCESS;
  CandidatePlan candi_plan;
  ObArenaAllocator allocator;
  bool best_need_sort = false;
  MergeKeyInfo* merge_key = NULL;
  ObSEArray<OrderItem, 4> best_order_items;
  ObSelectStmt* left_stmt = NULL;
  ObSelectStmt* right_stmt = NULL;
  ObLogicalOperator* left_plan = NULL;
  ObLogicalOperator* right_plan = NULL;
  ObSEArray<MergeKeyInfo*, 8> left_merge_keys;
  ObSEArray<MergeKeyInfo*, 8> right_merge_keys;
  ObSEArray<ObRawExpr*, 8> left_select_exprs;
  ObSEArray<ObRawExpr*, 8> right_select_exprs;
  ObSEArray<ObOrderDirection, 8> left_select_directions;
  ObSEArray<ObOrderDirection, 8> right_select_directions;
  ObIArray<CandidatePlan>& left_candi = left_log_plan.candidates_.candidate_plans_;
  ObIArray<CandidatePlan>& right_candi = right_log_plan.candidates_.candidate_plans_;
  if (OB_ISNULL(left_stmt = left_log_plan.get_stmt()) || OB_ISNULL(right_stmt = right_log_plan.get_stmt()) ||
      OB_ISNULL(left_log_plan.get_plan_root()) || OB_ISNULL(right_log_plan.get_plan_root())) {
    LOG_WARN("get unexpected null",
        K(left_stmt),
        K(right_stmt),
        K(left_log_plan.get_plan_root()),
        K(right_log_plan.get_plan_root()),
        K(ret));
  } else if (OB_FAIL(left_stmt->get_select_exprs(left_select_exprs))) {
    LOG_WARN("failed to get select exprs", K(ret));
  } else if (OB_FAIL(right_stmt->get_select_exprs(right_select_exprs))) {
    LOG_WARN("failed to get select exprs", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::find_stmt_expr_direction(*left_stmt,
                 left_select_exprs,
                 left_log_plan.get_plan_root()->get_ordering_output_equal_sets(),
                 left_select_directions))) {
    LOG_WARN("failed to find expr direction", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::find_stmt_expr_direction(*right_stmt,
                 right_select_exprs,
                 right_log_plan.get_plan_root()->get_ordering_output_equal_sets(),
                 right_select_directions))) {
    LOG_WARN("failed to find expr direction", K(ret));
  } else if (OB_FAIL(init_merge_set_structure(
                 allocator, left_candi, left_select_exprs, left_select_directions, left_merge_keys))) {
    LOG_WARN("failed to initialize merge key", K(ret));
  } else if (OB_FAIL(init_merge_set_structure(
                 allocator, right_candi, right_select_exprs, right_select_directions, right_merge_keys))) {
    LOG_WARN("failed to initialize merge key", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < left_candi.count(); ++i) {
      if (OB_ISNULL(left_plan = left_candi.at(i).plan_tree_) || OB_ISNULL(merge_key = left_merge_keys.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(left_plan), K(merge_key), K(ret));
      } else if (OB_FAIL(get_minimal_cost_set_plan(
                     *merge_key, right_select_exprs, right_candi, best_order_items, right_plan, best_need_sort))) {
        LOG_WARN("failed to get minimal cost set path", K(ret));
      } else if (OB_FAIL(create_merge_set_plan(left_plan,
                     right_plan,
                     merge_key->order_directions_,
                     merge_key->map_array_,
                     merge_key->order_items_,
                     best_order_items,
                     merge_key->need_sort_,
                     best_need_sort,
                     candi_plan))) {
        LOG_WARN("failed to create merge set", K(ret));
      } else if (OB_FAIL(merge_set_plans.push_back(candi_plan))) {
        LOG_WARN("failed to add merge plan", K(ret));
      } else { /*do nothing*/
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < right_candi.count(); i++) {
      if (OB_ISNULL(right_plan = right_candi.at(i).plan_tree_) || OB_ISNULL(merge_key = right_merge_keys.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(right_plan), K(merge_key), K(ret));
      } else if (OB_FAIL(get_minimal_cost_set_plan(
                     *merge_key, left_select_exprs, left_candi, best_order_items, left_plan, best_need_sort))) {
        LOG_WARN("failed to get minimal cost set path", K(ret));
      } else if (OB_FAIL(create_merge_set_plan(left_plan,
                     right_plan,
                     merge_key->order_directions_,
                     merge_key->map_array_,
                     best_order_items,
                     merge_key->order_items_,
                     best_need_sort,
                     merge_key->need_sort_,
                     candi_plan))) {
        LOG_WARN("failed to create merge set", K(ret));
      } else if (OB_FAIL(merge_set_plans.push_back(candi_plan))) {
        LOG_WARN("failed to add merge plan", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObSelectLogPlan::get_minimal_cost_set_plan(const MergeKeyInfo& left_merge_key,
    const ObIArray<ObRawExpr*>& right_set_exprs, const ObIArray<CandidatePlan>& right_candi,
    ObIArray<OrderItem>& best_order_items, ObLogicalOperator*& best_plan, bool& best_need_sort)
{
  int ret = OB_SUCCESS;
  double best_cost = 0.0;
  double right_path_cost = 0.0;
  double right_sort_cost = 0.0;
  bool right_need_sort = false;
  ObSEArray<ObRawExpr*, 8> right_order_exprs;
  ObSEArray<OrderItem, 8> right_order_items;
  best_plan = NULL;
  best_need_sort = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < right_candi.count(); i++) {
    ObLogicalOperator* right_plan = NULL;
    right_order_exprs.reset();
    right_order_items.reset();
    if (OB_ISNULL(right_plan = right_candi.at(i).plan_tree_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::adjust_exprs_by_mapping(
                   right_set_exprs, left_merge_key.map_array_, right_order_exprs))) {
      LOG_WARN("failed to adjust exprs by mapping", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::make_sort_keys(
                   right_order_exprs, left_merge_key.order_directions_, right_order_items))) {
      LOG_WARN("failed to make sort keys", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::check_need_sort(right_order_items,
                   right_plan->get_op_ordering(),
                   right_plan->get_fd_item_set(),
                   right_plan->get_ordering_output_equal_sets(),
                   right_plan->get_output_const_exprs(),
                   right_plan->get_is_at_most_one_row(),
                   right_need_sort))) {
      LOG_WARN("failed to check need sort", K(ret));
    } else if (right_need_sort &&
               OB_FAIL(compute_sort_cost_for_set_plan(*right_plan, right_order_items, right_sort_cost))) {
      LOG_WARN("failed to compute sort cost", K(ret));
    } else {
      right_path_cost = right_need_sort ? (right_sort_cost + right_plan->get_cost()) : right_plan->get_cost();
      if (NULL == best_plan || right_path_cost < best_cost) {
        if (OB_FAIL(best_order_items.assign(right_order_items))) {
          LOG_WARN("failed to assign exprs", K(ret));
        } else {
          best_plan = right_plan;
          best_need_sort = right_need_sort;
          best_cost = right_path_cost;
        }
      }
    }
  }
  return ret;
}

int ObSelectLogPlan::compute_sort_cost_for_set_plan(
    ObLogicalOperator& plan, ObIArray<OrderItem>& expected_ordering, double& cost)
{
  int ret = OB_SUCCESS;
  int64_t left_match_count = 0;
  int64_t prefix_pos = 0;
  if (OB_FAIL(ObOptimizerUtil::find_common_prefix_ordering(plan.get_op_ordering(),
          expected_ordering,
          plan.get_ordering_output_equal_sets(),
          plan.get_output_const_exprs(),
          left_match_count,
          prefix_pos))) {
    LOG_WARN("failed to find common prefix ordering", K(ret));
  } else {
    prefix_pos = std::min(prefix_pos, plan.get_op_ordering().count());
    ObSortCostInfo sort_cost_info(plan.get_card(), plan.get_width(), prefix_pos, NULL);
    if (OB_FAIL(ObOptEstCost::cost_sort(sort_cost_info, expected_ordering, cost))) {
      LOG_WARN("failed to calc cost", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObSelectLogPlan::init_merge_set_structure(ObIAllocator& allocator, const ObIArray<CandidatePlan>& plans,
    const ObIArray<ObRawExpr*>& select_exprs, const ObIArray<ObOrderDirection>& select_directions,
    ObIArray<MergeKeyInfo*>& merge_keys)
{
  int ret = OB_SUCCESS;
  const ObLogicalOperator* child = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < plans.count(); ++i) {
    MergeKeyInfo* merge_key = NULL;
    if (OB_ISNULL(merge_key = static_cast<MergeKeyInfo*>(allocator.alloc(sizeof(MergeKeyInfo))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to alloc merge key info", K(ret));
    } else if (OB_ISNULL(child = plans.at(i).plan_tree_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(child));
    } else {
      merge_key = new (merge_key) MergeKeyInfo(allocator, select_exprs.count());
      if (OB_FAIL(ObOptimizerUtil::decide_sort_keys_for_merge_style_op(child->get_op_ordering(),
              child->get_fd_item_set(),
              child->get_ordering_output_equal_sets(),
              child->get_output_const_exprs(),
              child->get_is_at_most_one_row(),
              select_exprs,
              select_directions,
              *merge_key))) {
        LOG_WARN("failed to decide sort key for merge set", K(ret));
      } else if (OB_FAIL(merge_keys.push_back(merge_key))) {
        LOG_WARN("failed to push back merge key", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObSelectLogPlan::create_merge_set_plan(ObLogicalOperator* left_child, ObLogicalOperator* right_child,
    const ObIArray<ObOrderDirection>& order_directions, const ObIArray<int64_t>& map_array,
    const ObIArray<OrderItem>& left_sort_keys, const ObIArray<OrderItem>& right_sort_keys, const bool left_need_sort,
    const bool right_need_sort, CandidatePlan& merge_plan)
{
  int ret = OB_SUCCESS;
  const ObSelectStmt* select_stmt = get_stmt();
  ObLogSet* set_op = NULL;
  if (OB_ISNULL(left_child) || OB_ISNULL(right_child) || OB_ISNULL(select_stmt) || OB_ISNULL(left_child->get_plan()) ||
      OB_ISNULL(right_child->get_plan()) || OB_UNLIKELY(!select_stmt->is_set_distinct())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(left_child), K(right_child), K(select_stmt));
  } else if (OB_ISNULL((set_op = static_cast<ObLogSet*>(get_log_op_factory().allocate(*this, LOG_SET))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("Allocate memory for ObLogSet failed", K(ret));
  } else if (OB_FAIL(set_op->set_left_expected_ordering(left_sort_keys))) {
    LOG_WARN("failed to set expected ordering", K(ret));
  } else if (OB_FAIL(set_op->set_right_expected_ordering(right_sort_keys))) {
    LOG_WARN("failed to set right expected ordering", K(ret));
  } else if (OB_FAIL(set_op->set_set_directions(order_directions))) {
    LOG_WARN("failed to set order diections", K(ret));
  } else if (OB_FAIL(set_op->set_map_array(map_array))) {
    LOG_WARN("failed to set map array", K(ret));
  } else if (left_need_sort && OB_FAIL(left_child->get_plan()->allocate_sort_as_top(left_child, left_sort_keys))) {
    LOG_WARN("faield to allocate sort for left plan", K(ret));
  } else if (right_need_sort && OB_FAIL(right_child->get_plan()->allocate_sort_as_top(right_child, right_sort_keys))) {
    LOG_WARN("failed to allocate sort for right plan", K(ret));
  } else if (OB_ISNULL(left_child) || OB_ISNULL(right_child)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("left/right child is null", K(ret), K(left_child), K(right_child));
  } else {
    set_op->set_left_child(left_child);
    set_op->set_right_child(right_child);
    set_op->assign_set_distinct(select_stmt->is_set_distinct());
    set_op->assign_set_op(select_stmt->get_set_op());
    set_op->set_algo_type(MERGE_SET);
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(set_op->compute_property())) {
    LOG_WARN("failed to compute property", K(ret));
  } else {
    merge_plan.plan_tree_ = set_op;
    LOG_TRACE("succeed to create merge set plan",
        K(left_sort_keys),
        K(right_sort_keys),
        K(order_directions),
        K(map_array),
        K(left_need_sort),
        K(right_need_sort));
  }
  return ret;
}

int ObSelectLogPlan::create_hash_set_plan(
    ObLogicalOperator* left_child, ObLogicalOperator* right_child, CandidatePlan& hash_plan)
{
  int ret = OB_SUCCESS;
  const ObSelectStmt* select_stmt = get_stmt();
  ObLogSet* set_op = NULL;
  if (OB_ISNULL(left_child) || OB_ISNULL(right_child) || OB_ISNULL(left_child->get_plan()) ||
      OB_ISNULL(right_child->get_plan()) || OB_ISNULL(select_stmt) || OB_UNLIKELY(!select_stmt->is_set_distinct())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(left_child), K(right_child), K(select_stmt));
  } else if (OB_ISNULL((set_op = static_cast<ObLogSet*>(get_log_op_factory().allocate(*this, LOG_SET))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("Allocate memory for ObLogSet failed", K(ret));
  } else {
    set_op->set_left_child(left_child);
    set_op->set_right_child(right_child);

    set_op->assign_set_distinct(select_stmt->is_set_distinct());
    set_op->assign_set_op(select_stmt->get_set_op());
    set_op->set_algo_type(HASH_SET);

    if (OB_FAIL(set_op->compute_property())) {
      LOG_WARN("failed to compute property", K(ret));
    } else {
      hash_plan.plan_tree_ = set_op;
    }
  }
  return ret;
}

int ObSelectLogPlan::generate_raw_plan()
{
  int ret = OB_SUCCESS;
  const ObSelectStmt* select_stmt = get_stmt();
  if (OB_ISNULL(select_stmt)) {
    ret = OB_NOT_INIT;
    LOG_WARN("stmt_ not been inited", K(ret));
  } else if (select_stmt->has_set_op()) {
    // UNION [ALL], INTERCEPT, EXCEPT
    ret = SMART_CALL(generate_plan_for_set());
  } else if (0 == select_stmt->get_from_item_size()) {
    ret = generate_plan_expr_values();
  } else if (OB_FAIL(SMART_CALL(generate_plan_for_plain_select()))) {
    LOG_WARN("failed to generate plan for plin select.", K(ret));
  } else { /*do nothing.*/
  }
  return ret;
}

int ObSelectLogPlan::generate_plan_for_plain_select()
{
  int ret = OB_SUCCESS;
  const ObSelectStmt* select_stmt = get_stmt();
  ObLogicalOperator* best_plan = NULL;
  if (OB_ISNULL(select_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(select_stmt), K(ret));
  } else if (OB_FAIL(generate_plan_tree())) {
    LOG_WARN("failed to generate plan tree for plain select", K(ret));
  } else if (OB_FAIL(allocate_plan_top())) {
    LOG_WARN("failed to allocate top operator of plan tree for plain select", K(ret));
  } else if (OB_FAIL(get_current_best_plan(best_plan))) {
    LOG_WARN("failed to choose the best plan", K(ret));
  } else if (OB_FAIL(set_final_plan_root(best_plan))) {
    LOG_WARN("failed to use best plan", K(ret));
  } else {
    LOG_TRACE("succeed to generate best plan", K(best_plan));
  }
  return ret;
}

int ObSelectLogPlan::allocate_plan_top()
{
  int ret = OB_SUCCESS;
  const ObSelectStmt* select_stmt = get_stmt();
  if (OB_ISNULL(select_stmt)) {
    ret = OB_NOT_INIT;
    LOG_WARN("stmt_ not been inited", K(ret));
  } else {
    // step. allocate subplan filter if needed, mainly for the subquery in where statement
    if (get_subquery_filters().count() > 0) {
      LOG_TRACE("start to allocate subplan filter for where statement", K(ret));
      if (OB_FAIL(candi_allocate_subplan_filter_for_where())) {
        LOG_WARN("failed to allocate subplan filter for where statement", K(ret));
      } else {
        LOG_TRACE("succeed to allocate subplan filter for where statement", K(ret));
      }
    }
    // step. allocate 'count' if needed
    if (OB_SUCC(ret)) {
      bool has_rownum = false;
      if (OB_FAIL(select_stmt->has_rownum(has_rownum))) {
        LOG_WARN("failed to get rownum info", K(ret));
      } else if (has_rownum) {
        LOG_TRACE("SQL has rownum expr", "sql", select_stmt->get_sql_stmt(), K(ret));
        if (OB_FAIL(candi_allocate_count())) {
          LOG_WARN("failed to allocate rownum(count)", K(ret));
        } else {
          LOG_TRACE("'COUNT' operator is allocated", "sql", select_stmt->get_sql_stmt(), K(ret));
        }
      } else {
        LOG_TRACE("SQL has no rownum expr", "sql", select_stmt->get_sql_stmt(), K(ret));
      }
    }
    // step. allocate 'group-by' if needed
    if (OB_SUCC(ret)) {
      // group-by or rollup both allocate group by logical operator.
      if (select_stmt->has_group_by() || select_stmt->has_rollup()) {
        LOG_DEBUG("SQL has group-by clause", "sql", select_stmt->get_sql_stmt(), K(ret));
        if (OB_FAIL(candi_allocate_group_by())) {
          LOG_WARN("failed to allocate 'GROUP-BY' OPERATOR", "sql", select_stmt->get_sql_stmt(), K(ret));
        } else {
          LOG_TRACE("'GROUP-BY' operator is allocated", "sql", select_stmt->get_sql_stmt(), K(ret));
        }
      } else {
        LOG_TRACE("SQL has no group-by clause", "sql", select_stmt->get_sql_stmt(), K(ret));
      }
    }

    // step. allocate 'window-sort' if needed
    if (OB_SUCC(ret)) {  // keep ordering
      if (select_stmt->has_window_function()) {
        LOG_TRACE("SQL has window-func clause", "sql", select_stmt->get_sql_stmt(), K(ret));
        if (OB_FAIL(candi_allocate_window_function())) {
          LOG_WARN("failed to allocate 'WINDOW-FUNCTION' OPERATOR", "sql", select_stmt->get_sql_stmt(), K(ret));
        }
      } else {
        LOG_TRACE("SQL has no window-func clause", "sql", select_stmt->get_sql_stmt(), K(ret));
      }
    }

    // step. allocate 'distinct' if needed
    if (OB_SUCC(ret)) {  // keep ordering
      if (select_stmt->has_distinct()) {
        LOG_TRACE("SQL has distinct clause", "sql", select_stmt->get_sql_stmt(), K(ret));
        if (OB_FAIL(candi_allocate_distinct())) {
          LOG_WARN("failed to allocate 'DISTINCT' operator", "sql", select_stmt->get_sql_stmt(), K(ret));
        } else {
          LOG_TRACE("'DISTINCT' operator is allocated", "sql", select_stmt->get_sql_stmt(), K(ret));
        }
      } else {
        LOG_TRACE("SQL has no distinct clause", "sql", select_stmt->get_sql_stmt(), K(ret));
      }
    } else { /* do nothing */
    }

    // step. allocate 'sequence' if needed
    if (OB_SUCC(ret)) {
      if (select_stmt->has_sequence()) {
        LOG_TRACE("SQL has sequence expr", "sql", select_stmt->get_sql_stmt(), K(ret));
        if (OB_FAIL(candi_allocate_sequence())) {
          LOG_WARN("failed to allocate sequence operator", "sql", select_stmt->get_sql_stmt(), K(ret));
        }
      } else {
        LOG_TRACE("SQL has no sequence expr", "sql", select_stmt->get_sql_stmt(), K(ret));
      }
    } else { /* do nothing */
    }

    // step. allocate 'order-by' if needed
    ObSEArray<OrderItem, 4> order_items;
    if (OB_SUCC(ret)) {
      if (select_stmt->has_order_by() && false == select_stmt->is_order_siblings()) {
        LOG_TRACE("SQL has order-by clause", "sql", select_stmt->get_sql_stmt(), K(ret));
        if (OB_FAIL(candi_allocate_order_by(order_items))) {
          LOG_WARN("failed to allocate 'ORDER-BY'group-by operator", "sql", select_stmt->get_sql_stmt(), K(ret));
        }
      } else {
        LOG_TRACE("SQL has no order-by clause", "sql", select_stmt->get_sql_stmt(), K(ret));
      }
    } else { /* do nothing */
    }

    // step. allocate 'limit' if needed
    if (OB_SUCC(ret)) {
      if (select_stmt->has_limit()) {
        LOG_TRACE("SQL has limit clause", "sql", select_stmt->get_sql_stmt(), K(ret));
        if (OB_FAIL(candi_allocate_limit(order_items))) {
          LOG_WARN("failed to allocate 'LIMIT' operator", "sql", select_stmt->get_sql_stmt(), K(ret));
        } else {
          LOG_TRACE("succeed to allocate 'LIMIT' operator", "sql", select_stmt->get_sql_stmt(), K(ret));
        }
      } else {
        LOG_TRACE("SQL has no limit clause", "sql", select_stmt->get_sql_stmt(), K(ret));
      }
    }

    // step. allocate late materialization if needed
    if (OB_SUCC(ret)) {
      if (select_stmt->has_limit()) {
        LOG_TRACE("SQL has limit clause, try to do late materialization", "sql", select_stmt->get_sql_stmt(), K(ret));
        if (OB_FAIL(candi_allocate_late_materialization())) {
          LOG_WARN("failed to allocate 'LATE-MATERIALIZATION' operator", "sql", select_stmt->get_sql_stmt(), K(ret));
        } else { /*do nothing*/
        }
      } else {
        LOG_TRACE("SQL has no limit clause, no need late materialization", "sql", select_stmt->get_sql_stmt(), K(ret));
      }
    }

    // step. allocate subplan filter if needed, mainly for subquery in select item
    if (OB_SUCC(ret)) {
      if (OB_FAIL(candi_allocate_subplan_filter_for_select_item())) {
        LOG_WARN("failed to allocate 'SubPlanFilter' OPERATOR", "sql", select_stmt->get_sql_stmt(), K(ret));
      } else { /* do nothing */
      }
    } else { /* do nothing */
    }

    // step. allocate 'unpivot' if needed
    if (OB_SUCC(ret)) {
      // group-by or rollup both allocate group by logical operator.
      if (select_stmt->is_unpivot_select()) {
        LOG_DEBUG("SQL has unpviot", "sql", select_stmt->get_sql_stmt(), K(ret));
        if (OB_FAIL(candi_allocate_unpivot())) {
          LOG_WARN("failed to allocate 'UNPIVOT' OPERATOR", "sql", select_stmt->get_sql_stmt(), K(ret));
        } else {
          LOG_TRACE("'UNPIVOT' operator is allocated", "sql", select_stmt->get_sql_stmt(), K(ret));
        }
      } else {
        LOG_TRACE("SQL has no UNPIVOT clause", "sql", select_stmt->get_sql_stmt(), K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (select_stmt->has_for_update() && share::is_oracle_mode() &&
          GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_2260) {
        LOG_TRACE("SQL has for update", "sql", select_stmt->get_sql_stmt(), K(ret));
        if (OB_FAIL(candi_allocate_for_update())) {
          LOG_WARN("failed to allocate for update operator", K(ret));
        }
      }
    }

    // allocate temp-table insert if needed
    if (OB_SUCC(ret)) {
      if (select_stmt->is_temp_table()) {
        LOG_TRACE("SQL is temp table, try to allocate temp-table insert op", "sql", select_stmt->get_sql_stmt());
        if (OB_FAIL(candi_allocate_temp_table_insert())) {
          LOG_WARN("failed to allocate temp-table insert", K(ret));
        } else {
          LOG_TRACE("succeed to allocate temp-table insert");
        }
      } else {
        LOG_TRACE("SQL is not a temp table", "sql", select_stmt->get_sql_stmt());
      }
    }

    // allocate temp-table transformation if needed.
    if (OB_SUCC(ret)) {
      if (select_stmt->need_temp_table_trans()) {
        LOG_TRACE("SQL has temp-table, try to do allocate transformation", "sql", select_stmt->get_sql_stmt(), K(ret));
        if (OB_FAIL(candi_allocate_temp_table_transformation())) {
          LOG_WARN("failed to allocate 'transformation' operator", "sql", select_stmt->get_sql_stmt(), K(ret));
        } else {
          LOG_TRACE("succeed to allocate temp-table transformation", K(ret));
        }
      } else {
        LOG_TRACE("SQL has no temp table", "sql", select_stmt->get_sql_stmt(), K(ret));
      }
    }

    // step. allocate 'select_into' if needed
    if (OB_SUCC(ret)) {
      if (select_stmt->has_select_into()) {
        LOG_TRACE("SQL has select_into clause", "sql", select_stmt->get_sql_stmt(), K(ret));
        if (OB_FAIL(candi_allocate_select_into())) {
          LOG_WARN("failed to allocate 'SELECT-INTO' operator", "sql", select_stmt->get_sql_stmt(), K(ret));
        } else {
          // do nothing
        }
      }
    }
  }
  return ret;
}

int ObSelectLogPlan::generate_plan()
{
  int ret = OB_SUCCESS;
  /*
   *  After the basic plan is generate, we perform the follow actions:
   *     1. allocate exchange nodes
   *     2. generate graunle iterator
   *     3. remove useless sort operator,set merge sort and task order
   *     4. allocate material for buffering operators.
   *     5. recalculate op_cost post
   *     6. numbering all operators(depth-first)
   *     7. allocate output exprs
   *     8. allocate dummy output for operators in case no output is 'actually' needed
   *     9. project pruning
   *     10. generate plan signature
   */
  if (OB_FAIL(generate_raw_plan())) {
    LOG_WARN("fail to generate raw plan", K(ret));
  } else if (OB_ISNULL(get_plan_root())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null root plan", K(get_plan_root()), K(ret));
  } else if (OB_FAIL(get_plan_root()->adjust_parent_child_relationship())) {
    LOG_WARN("failed to adjust parent-child relationship", K(ret));
  } else if (OB_FAIL(plan_traverse_loop(ALLOC_LINK,
                 ALLOC_EXCH,
                 ALLOC_GI,
                 ADJUST_SORT_OPERATOR,
                 PX_PIPE_BLOCKING,
                 PX_RESCAN,
                 RE_CALC_OP_COST,
                 ALLOC_MONITORING_DUMP,
                 OPERATOR_NUMBERING,
                 EXCHANGE_NUMBERING,
                 ALLOC_EXPR,
                 PROJECT_PRUNING,
                 ALLOC_DUMMY_OUTPUT,
                 CG_PREPARE,
                 GEN_SIGNATURE,
                 GEN_LOCATION_CONSTRAINT,
                 PX_ESTIMATE_SIZE,
                 GEN_LINK_STMT))) {
    LOG_WARN("failed to do plan traverse", K(ret));
  } else if (location_type_ != ObPhyPlanType::OB_PHY_PLAN_UNCERTAIN) {
    location_type_ = phy_plan_type_;
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(calc_plan_resource())) {
      LOG_WARN("fail calc plan resource", K(ret));
    }
  }
  LOG_TRACE("succ to do all plan traversals", K(location_type_));
  return ret;
}

int ObSelectLogPlan::generate_plan_expr_values()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* expr_values = get_log_op_factory().allocate(*this, LOG_EXPR_VALUES);
  const ObSelectStmt* select_stmt = get_stmt();
  if (OB_ISNULL(select_stmt)) {
    ret = OB_NOT_INIT;
    LOG_WARN("stmt_ not been inited", K(ret));
  } else if (OB_ISNULL(expr_values)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to allocate expr values", K(ret));
  } else {
    // classify subquery filters and rownum filters
    const ObIArray<ObRawExpr*>& condition_exprs = select_stmt->get_condition_exprs();
    for (int64_t i = 0; OB_SUCC(ret) && i < condition_exprs.count(); i++) {
      ObRawExpr* temp = NULL;
      if (OB_ISNULL(temp = condition_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null exprs", K(ret));
      } else if (temp->has_flag(CNT_ROWNUM)) {
        if (OB_FAIL(add_rownum_expr(temp))) {
          LOG_WARN("failed to add rownum expr", K(ret));
        } else { /*do nothing*/
        }
      } else if (temp->has_flag(CNT_SUB_QUERY)) {
        if (OB_FAIL(add_subquery_filter(temp))) {
          LOG_WARN("failed to add subquery filter", K(ret));
        } else { /*do nothing*/
        }
      } else {
        if (OB_FAIL(expr_values->get_filter_exprs().push_back(temp))) {
          LOG_WARN("failed to push back exprs", K(ret));
        } else { /*do nothing*/
        }
      }
    }
    // make candidate plan
    if (OB_SUCC(ret)) {
      if (OB_FAIL(expr_values->compute_property())) {
        LOG_WARN("failed to compute property", K(ret));
      } else {
        CandidatePlan cp;
        cp.plan_tree_ = expr_values;
        if (OB_FAIL(candidates_.candidate_plans_.push_back(cp))) {
          LOG_WARN("push back error", K(ret));
        } else {
          candidates_.plain_plan_.first = expr_values->get_cost();
          candidates_.plain_plan_.second = 0;
          LOG_TRACE("succeed to generate expr value plan", K(ret));
        }
      }
    }

    // allocate top operator of plan tree
    if (OB_SUCC(ret)) {
      if (OB_FAIL(allocate_plan_top())) {
        LOG_WARN("failed to allocate top operators for expr select", K(ret));
      } else {
        LOG_TRACE("succeed to allcoate top operators for expr select", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      set_phy_plan_type(OB_PHY_PLAN_LOCAL);
      if (OB_UNLIKELY(candidates_.plain_plan_.second >= candidates_.candidate_plans_.count() ||
                      candidates_.plain_plan_.second < 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid candidate plans idx",
            K(ret),
            K(candidates_.plain_plan_.second),
            K(candidates_.candidate_plans_.count()));
      } else if (FALSE_IT(set_plan_root(candidates_.candidate_plans_.at(candidates_.plain_plan_.second).plan_tree_))) {
      } else if (OB_ISNULL(get_plan_root())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("plan root is NULL", K(ret));
      } else {
        get_plan_root()->mark_is_plan_root();
      }
    }
  }
  return ret;
}

int ObSelectLogPlan::candi_allocate_subplan_filter_for_select_item()
{
  int ret = OB_SUCCESS;
  ObSelectStmt* select_stmt = NULL;
  ObSEArray<ObRawExpr*, 4> select_exprs;
  ObSEArray<ObRawExpr*, 4> subquery_exprs;
  if (OB_ISNULL(select_stmt = get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null stmt", K(select_stmt), K(ret));
  } else if (OB_FAIL(select_stmt->get_select_exprs(select_exprs))) {
    LOG_WARN("failed to get select exprs", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::get_subquery_exprs(select_exprs, subquery_exprs))) {
    LOG_WARN("failed to get subquery exprs", K(ret));
  } else if (subquery_exprs.empty()) {
    /*do nothing*/
  } else if (OB_FAIL(candi_allocate_subplan_filter(subquery_exprs, false))) {
    LOG_WARN("failed to allocate subplan filter for select item", K(ret));
  } else {
    LOG_TRACE("succeed to allocate subplan filter for select item", K(select_stmt->get_current_level()));
  }
  return ret;
}

int ObSelectLogPlan::generate_child_plan_for_set(
    const ObDMLStmt* sub_stmt, ObSelectLogPlan*& sub_plan, ObIArray<ObRawExpr*>& pushdown_filters)
{
  int ret = OB_SUCCESS;
  sub_plan = NULL;
  if (OB_ISNULL(sub_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sub_stmt is null", K(ret), K(sub_stmt));
  } else if (OB_ISNULL(sub_plan = static_cast<ObSelectLogPlan*>(
                           optimizer_context_.get_log_plan_factory().create(optimizer_context_, *sub_stmt)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("Failed to create logcial plan", K(sub_plan), K(ret));
  } else if (OB_FAIL(sub_plan->add_pushdown_filters(pushdown_filters))) {
    LOG_WARN("failed to add pushdown filters", K(ret));
  } else if (OB_FAIL(sub_plan->init_plan_info())) {
    LOG_WARN("failed to init equal sets", K(ret));
  } else if (OB_FAIL(sub_plan->generate_raw_plan())) {
    LOG_WARN("Failed to generate plan for sub_stmt", K(ret));
  }
  return ret;
}

int ObSelectLogPlan::get_select_and_pullup_agg_subquery_exprs(ObIArray<ObRawExpr*>& subquery_exprs)
{
  int ret = OB_SUCCESS;
  ObSelectStmt* select_stmt = get_stmt();
  if (OB_ISNULL(select_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null stmt", K(select_stmt), K(ret));
  } else {
    // get select aggr subqueries
    ObRawExpr* raw_expr = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt->get_select_item_size(); ++i) {
      if (OB_ISNULL(raw_expr = select_stmt->get_select_item(i).expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null select expr", K(ret));
      } else if (OB_FAIL(get_agg_subquery_exprs(raw_expr, subquery_exprs))) {
        LOG_WARN("failed to get agg subquery exprs", K(ret));
      } else { /*do nothing*/
      }
    }
    // get pull up aggr subqueries
    for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt->get_aggr_item_size(); i++) {
      if (OB_ISNULL(raw_expr = select_stmt->get_aggr_item(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (raw_expr->get_expr_levels().bit_count() > 1 &&
                 OB_FAIL(get_agg_subquery_exprs(raw_expr, subquery_exprs))) {
        LOG_WARN("failed to get agg subquery exprs", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObSelectLogPlan::get_agg_subquery_exprs(ObRawExpr* expr, ObIArray<ObRawExpr*>& subquery_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null expr", K(ret));
  } else if (!expr->has_flag(CNT_SUB_QUERY) || !expr->has_flag(CNT_AGG)) {
    /* do nothing*/
  } else if (expr->is_aggr_expr()) {
    ObAggFunRawExpr* agg_expr = static_cast<ObAggFunRawExpr*>(expr);
    for (int64_t i = 0; OB_SUCC(ret) && i < agg_expr->get_param_count(); i++) {
      ObRawExpr* temp_expr = agg_expr->get_param_expr(i);
      if (OB_ISNULL(temp_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null expr", K(temp_expr), K(ret));
      } else if (temp_expr->has_flag(CNT_SUB_QUERY) && OB_FAIL(add_var_to_array_no_dup(subquery_exprs, temp_expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else { /*do nothing*/
      }
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); i++) {
      if (OB_ISNULL(expr->get_param_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null expr", K(ret));
      } else if (OB_FAIL(get_agg_subquery_exprs(expr->get_param_expr(i), subquery_exprs))) {
        LOG_WARN("failed to get subquery exprs", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObSelectLogPlan::get_distinct_exprs(ObLogicalOperator& op, ObIArray<ObRawExpr*>& distinct_exprs)
{
  int ret = OB_SUCCESS;
  const ObSelectStmt* select_stmt = get_stmt();
  if (OB_ISNULL(select_stmt)) {
    ret = OB_NOT_INIT;
    LOG_WARN("stmt_ is NULL, ObSelectLogPlan should be inited", K(select_stmt), K(ret));
  } else if (select_stmt->has_distinct()) {
    ObRawExpr* select_expr = NULL;
    ObSEArray<ObRawExpr*, 8> candi_exprs;
    for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt->get_select_item_size(); ++i) {
      bool is_const = false;
      select_expr = select_stmt->get_select_item(i).expr_;
      if (OB_ISNULL(select_expr)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("select expr is null");
      } else if (OB_FAIL(ObOptimizerUtil::is_const_expr(select_expr, op.get_output_const_exprs(), is_const))) {
        LOG_WARN("check expr whether is const failed", K(ret));
      } else if (is_const) {
        // skip it
      } else if (OB_FAIL(candi_exprs.push_back(select_expr))) {
        LOG_WARN("push expr to distinct exprs failed", K(ret));
      } else { /*do nothing*/
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(op.simplify_exprs(candi_exprs, distinct_exprs))) {
      LOG_WARN("fail to simplify exprs", K(ret));
    }
  } else { /*do nothing*/
  }
  return ret;
}

int ObSelectLogPlan::create_merge_group_plan(const CandidatePlan& candidate_plan, ObIArray<ObRawExpr*>& group_exprs,
    ObIArray<ObRawExpr*>& rollup_exprs, ObIArray<ObRawExpr*>& having_exprs,
    ObIArray<ObOrderDirection>& group_directions, common::ObIArray<ObOrderDirection>& rollup_directions,
    CandidatePlan& merge_plan)
{
  int ret = OB_SUCCESS;
  bool need_sort = false;
  ObSEArray<OrderItem, 4> sort_keys;
  ObSEArray<ObRawExpr*, 4> group_rollup_exprs;
  ObSEArray<ObOrderDirection, 4> group_rollup_directions;
  merge_plan.plan_tree_ = candidate_plan.plan_tree_;
  const ObSelectStmt* select_stmt = get_stmt();
  if (OB_ISNULL(select_stmt) || OB_ISNULL(merge_plan.plan_tree_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(select_stmt), K(merge_plan.plan_tree_), K(ret));
  } else if (OB_FAIL(adjust_sort_expr_ordering(group_exprs, group_directions, *merge_plan.plan_tree_, true))) {
    LOG_WARN("failed to get group expr ordering", K(ret));
  } else if (OB_FAIL(append(group_rollup_exprs, group_exprs))) {
    LOG_WARN("failed to append group exprs to group rollup exprs.", K(ret));
  } else if (OB_FAIL(append(group_rollup_exprs, rollup_exprs))) {
    LOG_WARN("failed to append rollup exprs to group rollup exprs.", K(ret));
  } else if (OB_FAIL(append(group_rollup_directions, group_directions))) {
    LOG_WARN("failed to append directions.", K(ret));
  } else if (OB_FAIL(append(group_rollup_directions, rollup_directions))) {
    LOG_WARN("failed to append directions.", K(ret));
  } else if (OB_FAIL(merge_plan.plan_tree_->check_need_sort_above_node(
                 group_rollup_exprs, &group_rollup_directions, need_sort))) {
    LOG_WARN("failed to check need sort above node", K(ret));
  } else if (OB_FAIL(make_order_items(group_rollup_exprs, group_rollup_directions, sort_keys))) {
    LOG_WARN("failed to make order items from exprs", K(ret));
  } else if (need_sort && OB_FAIL(allocate_sort_as_top(merge_plan.plan_tree_, sort_keys))) {
    LOG_WARN("failed to allocate sort operator", K(ret));
  } else if (OB_FAIL(allocate_group_by_as_top(merge_plan.plan_tree_,
                 MERGE_AGGREGATE,
                 group_exprs,
                 rollup_exprs,
                 select_stmt->get_aggr_items(),
                 having_exprs,
                 sort_keys,
                 select_stmt->is_from_pivot()))) {
    LOG_WARN("failed to allocate merge group by operator", K(ret));
  } else {
    LOG_TRACE("succeed to allocate merge group by operator", K(ret));
  }
  return ret;
}

int ObSelectLogPlan::create_merge_distinct_plan(const CandidatePlan& candidate_plan,
    ObIArray<ObRawExpr*>& distinct_exprs, ObIArray<ObOrderDirection>& directions, CandidatePlan& merge_plan)
{
  int ret = OB_SUCCESS;
  bool need_sort = false;
  ObSEArray<OrderItem, 4> sort_keys;
  merge_plan.plan_tree_ = candidate_plan.plan_tree_;
  if (OB_ISNULL(merge_plan.plan_tree_) || OB_ISNULL(merge_plan.plan_tree_->get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(merge_plan.plan_tree_), K(ret));
  } else if (OB_FAIL(adjust_sort_expr_ordering(distinct_exprs, directions, *merge_plan.plan_tree_, false))) {
    LOG_WARN("failed to adjust sort expr ordering", K(ret));
  } else if (OB_FAIL(merge_plan.plan_tree_->check_need_sort_above_node(distinct_exprs, &directions, need_sort))) {
    LOG_WARN("failed to check need sort above node", K(ret));
  } else if (OB_FAIL(make_order_items(distinct_exprs, &directions, sort_keys))) {
    LOG_WARN("failed to make order items from exprs", K(ret));
  } else if (need_sort && OB_FAIL(allocate_sort_as_top(merge_plan.plan_tree_, sort_keys))) {
    LOG_WARN("failed to allocate sort operator", K(ret));
  } else if (OB_FAIL(allocate_distinct_as_top(merge_plan.plan_tree_, MERGE_AGGREGATE, distinct_exprs, sort_keys))) {
    LOG_WARN("failed to allocate merge distinct op", K(ret));
  } else {
    LOG_TRACE("succeed to allocate merge distinct operator", K(ret));
  }
  return ret;
}

int ObSelectLogPlan::allocate_distinct_as_top(ObLogicalOperator*& top, const AggregateAlgo algo,
    ObIArray<ObRawExpr*>& distinct_exprs, const ObIArray<OrderItem>& expected_ordering)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* distinct_op = NULL;
  if (OB_ISNULL(top)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(top), K(ret));
  } else if (OB_ISNULL(distinct_op = get_log_op_factory().allocate(*this, LOG_DISTINCT))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to allocate distinct operator", K(ret));
  } else {
    ObLogDistinct* distinct = static_cast<ObLogDistinct*>(distinct_op);
    distinct->set_child(ObLogicalOperator::first_child, top);
    distinct->set_algo_type(algo);
    if (OB_FAIL(distinct->set_distinct_exprs(distinct_exprs))) {
      LOG_WARN("failed to set group by columns", K(ret));
    } else if (OB_FAIL(distinct->set_expected_ordering(expected_ordering))) {
      LOG_WARN("failed to set expected ordering", K(ret));
    } else if (OB_FAIL(distinct->compute_property())) {
      LOG_WARN("failed to compute property", K(ret));
    } else {
      top = distinct;
    }
  }
  return ret;
}

int ObSelectLogPlan::decide_sort_keys_for_runion(
    const common::ObIArray<OrderItem>& order_items, common::ObIArray<OrderItem>& new_order_items)
{
  int ret = OB_SUCCESS;
  ObSelectStmt* select_stmt = NULL;
  ObSEArray<ObRawExpr*, 8> select_exprs;
  if (OB_ISNULL(select_stmt = get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null stmt", K(select_stmt), K(ret));
  } else if (OB_FAIL(select_stmt->get_select_exprs(select_exprs))) {
    LOG_WARN("failed to get select exprs", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < order_items.count(); ++i) {
      ObRawExpr* raw_expr = order_items.at(i).expr_;
      if (OB_ISNULL(raw_expr) || OB_UNLIKELY(!raw_expr->is_column_ref_expr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null", K(ret));
      } else if (raw_expr->is_column_ref_expr()) {
        ObColumnRefRawExpr* col_expr = static_cast<ObColumnRefRawExpr*>(raw_expr);
        if (OB_UNLIKELY(!col_expr->is_cte_generated_column())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("the column is not a cte generate column", K(ret));
        } else {
          int64_t projector_offset = col_expr->get_cte_generate_column_projector_offset();
          ObRawExpr* real_expr = select_exprs.at(projector_offset);
          if (OB_ISNULL(real_expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("convert recursive union all generate operator sort failed");
          } else if (real_expr->has_const_or_const_expr_flag()) {
            // do nothing.
          } else {
            OrderItem real_item = order_items.at(i);
            real_item.expr_ = real_expr;
            if (OB_FAIL(new_order_items.push_back(real_item))) {
              LOG_WARN("add real sort item error");
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObSelectLogPlan::candi_allocate_window_function()
{
  int ret = OB_SUCCESS;
  ObSelectStmt* stmt = get_stmt();
  ObSEArray<ObRawExpr*, 8> candi_subquery_exprs;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is NULL", K(ret));
  } else if (OB_FAIL(append(candi_subquery_exprs, stmt->get_window_func_exprs()))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else if (OB_FAIL(candi_allocate_subplan_filter_for_exprs(candi_subquery_exprs))) {
    LOG_WARN("failed to do allocate subplan filter", K(ret));
  } else if (stmt->get_window_func_count() > 0) {
    ObSEArray<CandidatePlan, 8> winfunc_plans;
    CandidatePlan winfunc_plan;
    for (int64_t i = 0; OB_SUCC(ret) && i < candidates_.candidate_plans_.count(); ++i) {
      winfunc_plan.plan_tree_ = candidates_.candidate_plans_.at(i).plan_tree_;
      if (OB_FAIL(merge_and_allocate_window_functions(winfunc_plan.plan_tree_, stmt->get_window_func_exprs()))) {
        LOG_WARN("failed to allocate window functions", K(ret));
      } else if (OB_ISNULL(winfunc_plan.plan_tree_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("window function plan tree is null", K(ret));
      } else if (OB_FAIL(winfunc_plans.push_back(winfunc_plan))) {
        LOG_WARN("failed to push back window function plan", K(ret));
      } else { /*do nothing*/
      }
    }
    // choose the best plan
    if (OB_SUCC(ret)) {
      int64_t check_scope =
          OrderingCheckScope::CHECK_DISTINCT | OrderingCheckScope::CHECK_SET | OrderingCheckScope::CHECK_ORDERBY;
      if (OB_FAIL(update_plans_interesting_order_info(winfunc_plans, check_scope))) {
        LOG_WARN("failed to update plans interesting order info", K(ret));
      } else if (OB_FAIL(prune_and_keep_best_plans(winfunc_plans))) {
        LOG_WARN("failed to add winfunc plans", K(ret));
      }
    }
  }
  return ret;
}

int ObSelectLogPlan::candi_allocate_temp_table_insert()
{
  int ret = OB_SUCCESS;
  ObSelectStmt* select_stmt = NULL;
  if (OB_ISNULL(select_stmt = get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    double min_cost = 0.0;
    int64_t min_idx = OB_INVALID_INDEX;
    for (int64_t i = 0; OB_SUCC(ret) && i < candidates_.candidate_plans_.count(); ++i) {
      CandidatePlan& plain_plan = candidates_.candidate_plans_.at(i);
      if (OB_FAIL(allocate_temp_table_insert_as_top(plain_plan.plan_tree_, select_stmt->get_temp_table_info()))) {
        LOG_WARN("failed to allocate temp table insert", K(ret));
      } else if (OB_ISNULL(plain_plan.plan_tree_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null plan tree", K(ret));
      } else if (plain_plan.plan_tree_->get_cost() < min_cost || OB_INVALID_INDEX == min_idx) {
        min_cost = plain_plan.plan_tree_->get_cost();
        min_idx = i;
      } else { /*do nothing*/
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(OB_INVALID_INDEX == min_idx)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Min idx is invalid", K(ret), K(min_idx));
      } else {
        candidates_.plain_plan_.second = min_idx;
        candidates_.plain_plan_.first = min_cost;
      }
    }
  }
  return ret;
}

int ObSelectLogPlan::allocate_temp_table_insert_as_top(
    ObLogicalOperator*& top, const ObSqlTempTableInfo* temp_table_info)
{
  int ret = OB_SUCCESS;
  ObSelectStmt* stmt = NULL;
  ObLogicalOperator* op = NULL;
  if (OB_ISNULL(top) || OB_ISNULL(temp_table_info) || OB_ISNULL(stmt = get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(top), K(temp_table_info), K(get_stmt()), K(ret));
  } else if (OB_ISNULL(op = log_op_factory_.allocate(*this, LOG_TEMP_TABLE_INSERT))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate temp table operator", K(ret));
  } else {
    bool need_expected_ordering = stmt->has_order_by() && !stmt->has_limit();
    ObLogTempTableInsert* temp_table_insert = static_cast<ObLogTempTableInsert*>(op);
    temp_table_insert->set_ref_table_id(temp_table_info->ref_table_id_);
    temp_table_insert->get_table_name().assign_ptr(
        temp_table_info->table_name_.ptr(), temp_table_info->table_name_.length());
    if (OB_FAIL(temp_table_insert->add_child(top))) {
      LOG_WARN("failed to add one children", K(ret));
    } else if (need_expected_ordering && OB_FAIL(temp_table_insert->set_expected_ordering(stmt->get_order_items()))) {
      LOG_WARN("failed to set expected ordering", K(ret));
    } else if (OB_FAIL(temp_table_insert->compute_property())) {
      LOG_WARN("failed to compute property", K(ret));
    } else {
      top = temp_table_insert;
      top->set_is_plan_root(true);
    }
  }
  return ret;
}

int ObSelectLogPlan::candi_allocate_temp_table_transformation()
{
  int ret = OB_SUCCESS;
  ObQueryCtx* query_ctx = NULL;
  ObSelectStmt* select_stmt = NULL;
  ObSEArray<uint64_t, 8> temp_table_ids;
  if (OB_ISNULL(select_stmt = get_stmt()) || OB_ISNULL(query_ctx = select_stmt->get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(select_stmt->get_temp_table_ids(temp_table_ids))) {
    LOG_WARN("failed to get temp table ids", K(ret));
  } else {
    double min_cost = 0.0;
    int64_t min_idx = OB_INVALID_INDEX;
    ObSEArray<ObLogicalOperator*, 8> temp_table_insert;
    ObIArray<ObSqlTempTableInfo*>& temp_table_info = query_ctx->temp_table_infos_;
    for (int64_t i = 0; OB_SUCC(ret) && i < temp_table_ids.count(); i++) {
      ObLogicalOperator* temp_table_plan = NULL;
      if (OB_FAIL(query_ctx->get_temp_table_plan(temp_table_ids.at(i), temp_table_plan))) {
        LOG_WARN("failed to get temp table plan", K(ret));
      } else if (OB_ISNULL(temp_table_plan)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(temp_table_insert.push_back(temp_table_plan))) {
        LOG_WARN("failed to push back temp table plan", K(ret));
      } else { /*do nothing*/
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < candidates_.candidate_plans_.count(); ++i) {
      CandidatePlan& plain_plan = candidates_.candidate_plans_.at(i);
      if (OB_FAIL(allocate_temp_table_transformation_as_top(plain_plan.plan_tree_, temp_table_insert))) {
        LOG_WARN("failed to allocate temp table transformation", K(ret));
      } else if (OB_ISNULL(plain_plan.plan_tree_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null plan tree", K(ret));
      } else if (plain_plan.plan_tree_->get_cost() < min_cost || OB_INVALID_INDEX == min_idx) {
        min_cost = plain_plan.plan_tree_->get_cost();
        min_idx = i;
      } else { /*do nothing*/
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(OB_INVALID_INDEX == min_idx)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Min idx is invalid", K(ret), K(min_idx));
      } else {
        candidates_.plain_plan_.second = min_idx;
        candidates_.plain_plan_.first = min_cost;
      }
    }
  }
  return ret;
}

int ObSelectLogPlan::allocate_temp_table_transformation_as_top(
    ObLogicalOperator*& top, const ObIArray<ObLogicalOperator*>& temp_table_insert)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* temp_table_transformation = NULL;
  if (OB_ISNULL(top)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(top));
  } else if (OB_ISNULL(temp_table_transformation = log_op_factory_.allocate(*this, LOG_TEMP_TABLE_TRANSFORMATION))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to allocate temp table operator", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < temp_table_insert.count(); i++) {
      if (OB_FAIL(temp_table_transformation->add_child(temp_table_insert.at(i)))) {
        LOG_WARN("failed to add children", K(ret));
      } else { /*do nothing*/
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(temp_table_transformation->add_child(top))) {
        LOG_WARN("failed to add children", K(ret));
      } else if (OB_FAIL(temp_table_transformation->compute_property())) {
        LOG_WARN("failed to compute property", K(ret));
      } else {
        top = temp_table_transformation;
      }
    }
  }
  return ret;
}

int ObSelectLogPlan::merge_and_allocate_window_functions(
    ObLogicalOperator*& top, const ObIArray<ObWinFunRawExpr*>& winfunc_exprs)
{
  int ret = OB_SUCCESS;
  bool is_prefix = false;
  int64_t left_match_count = 0;
  int64_t right_match_count = 0;
  int64_t best_match_count = -1;
  int64_t next_best_idx = -1;
  int64_t best_idx = -1;
  bool need_sort = false;
  bool next_need_sort = false;
  ObSEArray<ObWinFuncEntry, 8> winfunc_entries;
  ObSEArray<ObWinFuncEntry, 8> unused_entries;
  ObSEArray<ObWinFunRawExpr*, 8> group_exprs;
  if (OB_ISNULL(get_stmt()) || OB_ISNULL(top)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(get_stmt()), K(top));
  } else {
    const EqualSets& equal_sets = top->get_ordering_output_equal_sets();
    const ObIArray<ObRawExpr*>& const_exprs = top->get_output_const_exprs();
    for (int64_t i = 0; OB_SUCC(ret) && i < winfunc_exprs.count(); ++i) {
      ObWinFuncEntry entry;
      entry.first = winfunc_exprs.at(i);
      if (OB_FAIL(winfunc_entries.push_back(entry))) {
        LOG_WARN("failed to push back win func entries", K(ret));
      }
    }
    while (OB_SUCC(ret) && winfunc_entries.count() > 0) {
      next_best_idx = -1;
      best_match_count = -1;
      ObIArray<OrderItem>& group_sort_keys =
          best_idx == -1 ? top->get_op_ordering() : winfunc_entries.at(best_idx).second;
      unused_entries.reuse();
      group_exprs.reuse();
      bool order_unique = false;
      if (OB_FAIL(ObOptimizerUtil::is_exprs_unique(
              group_sort_keys, top->get_table_set(), top->get_fd_item_set(), equal_sets, const_exprs, order_unique))) {
        LOG_WARN("failed to check is order unique", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < winfunc_entries.count(); ++i) {
        bool can_merge = false;
        if (best_idx == i) {
          can_merge = true;
        } else if (OB_FAIL(decide_sort_keys_for_window_function(
                       group_sort_keys, winfunc_entries, equal_sets, const_exprs, winfunc_entries.at(i)))) {
          LOG_WARN("failed to decide sort keys", K(ret));
        } else if (OB_FAIL(ObOptimizerUtil::find_common_prefix_ordering(winfunc_entries.at(i).second,
                       group_sort_keys,
                       equal_sets,
                       const_exprs,
                       left_match_count,
                       right_match_count))) {
          LOG_WARN("failed to find common prefix ordering", K(ret));
        } else if (FALSE_IT(is_prefix = (left_match_count == winfunc_entries.at(i).second.count()))) {
          /*do nothing*/
        } else if (-1 != best_idx) {
          can_merge = is_prefix || (order_unique && right_match_count >= group_sort_keys.count());
        }
        if (OB_SUCC(ret)) {
          if (can_merge) {
            if (OB_FAIL(group_exprs.push_back(winfunc_entries.at(i).first))) {
              LOG_WARN("failed to add winfun into group", K(ret));
            }
          } else if (OB_FAIL(unused_entries.push_back(winfunc_entries.at(i)))) {
            LOG_WARN("failed to push back unused entry", K(ret));
          } else if ((right_match_count == best_match_count &&
                         winfunc_entries.at(i).second.count() > unused_entries.at(next_best_idx).second.count()) ||
                     right_match_count > best_match_count) {
            best_match_count = right_match_count;
            next_best_idx = unused_entries.count() - 1;
            next_need_sort = !is_prefix && !(order_unique && right_match_count >= group_sort_keys.count());
          }
        }
      }
      if (OB_SUCC(ret) && -1 != best_idx) {
        if (OB_FAIL(allocate_window_function_group(top, group_exprs, group_sort_keys, need_sort))) {
          LOG_WARN("failed to rearrange window functions", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(winfunc_entries.assign(unused_entries))) {
          LOG_WARN("failed to assign unused exprs", K(ret));
        } else {
          best_idx = next_best_idx;
          need_sort = next_need_sort;
        }
      }
    }
  }
  return ret;
}

int ObSelectLogPlan::decide_sort_keys_for_window_function(const ObIArray<OrderItem>& ordering,
    const ObIArray<ObWinFuncEntry>& winfunc_entries, const EqualSets& equal_sets,
    const ObIArray<ObRawExpr*>& const_exprs, ObWinFuncEntry& win_entry)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 8> part_exprs;
  ObSEArray<ObOrderDirection, 8> part_directions;
  ObSEArray<OrderItem, 8> output_sort_keys;
  const ObWinFunRawExpr* win_expr = win_entry.first;
  ObIArray<OrderItem>& sort_keys = win_entry.second;
  bool is_const = false;
  bool dummy_ordering_used = false;
  sort_keys.reuse();
  if (OB_ISNULL(win_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(win_expr));
  } else if (OB_FAIL(part_exprs.assign(win_expr->get_partition_exprs()))) {
    LOG_WARN("failed to assign partition exprs", K(ret));
  } else if (OB_FAIL(set_default_sort_directions(winfunc_entries, part_exprs, equal_sets, part_directions))) {
    LOG_WARN("failed to get default sort directions", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::adjust_exprs_by_ordering(
                 part_exprs, ordering, equal_sets, const_exprs, dummy_ordering_used, part_directions))) {
    LOG_WARN("failed to adjust exprs by ordering", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::make_sort_keys(part_exprs, part_directions, output_sort_keys))) {
    LOG_WARN("failed to make sort keys", K(ret));
  } else if (OB_FAIL(append(output_sort_keys, win_expr->get_order_items()))) {
    LOG_WARN("faield to append order items", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < output_sort_keys.count(); ++i) {
    if (OB_FAIL(ObOptimizerUtil::is_const_expr(output_sort_keys.at(i).expr_, equal_sets, const_exprs, is_const))) {
      LOG_WARN("failed to check is const expr", K(ret));
    } else if (is_const) {
    } else if (OB_FAIL(sort_keys.push_back(output_sort_keys.at(i)))) {
      LOG_WARN("failed to push back sort keys", K(ret));
    }
  }
  return ret;
}

int ObSelectLogPlan::set_default_sort_directions(const ObIArray<ObWinFuncEntry>& win_entries,
    const ObIArray<ObRawExpr*>& part_exprs, const EqualSets& equal_sets, ObIArray<ObOrderDirection>& directions)
{
  int ret = OB_SUCCESS;
  directions.reset();
  if (OB_ISNULL(get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < part_exprs.count(); ++i) {
    const ObRawExpr* expr = part_exprs.at(i);
    ObOrderDirection direction = default_asc_direction();
    bool found_dir = false;
    for (int64_t j = 0; OB_SUCC(ret) && !found_dir && j < win_entries.count(); ++j) {
      if (OB_ISNULL(win_entries.at(j).first)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("win func is null", K(ret));
      } else {
        found_dir = ObOptimizerUtil::find_expr_direction(
            win_entries.at(j).first->get_order_items(), expr, equal_sets, direction);
      }
    }
    if (OB_SUCC(ret)) {
      if (!found_dir) {
        found_dir = ObOptimizerUtil::find_expr_direction(get_stmt()->get_order_items(), expr, equal_sets, direction);
      }
      if (OB_FAIL(directions.push_back(direction))) {
        LOG_WARN("failed to push back directions", K(ret));
      }
    }
  }
  return ret;
}

int ObSelectLogPlan::allocate_window_function_group(ObLogicalOperator*& top,
    const ObIArray<ObWinFunRawExpr*>& winfunc_exprs, const ObIArray<OrderItem>& sort_keys, bool need_sort)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_stmt()) || OB_ISNULL(top)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpecated null", K(top), K(ret));
  } else {
    const EqualSets& equal_sets = top->get_ordering_output_equal_sets();

    ObSEArray<std::pair<int64_t, int64_t>, 8> expr_entries;
    for (int64_t i = 0; OB_SUCC(ret) && i < winfunc_exprs.count(); ++i) {
      int64_t non_const_exprs = 0;
      if (OB_FAIL(ObOptimizerUtil::get_non_const_expr_size(winfunc_exprs.at(i)->get_partition_exprs(),
              equal_sets,
              top->get_output_const_exprs(),
              non_const_exprs))) {
        LOG_WARN("failed to get non const expr size", K(ret));
      } else if (OB_FAIL(expr_entries.push_back(std::pair<int64_t, int64_t>(-non_const_exprs, i)))) {
        LOG_WARN("faield to push back expr entry", K(ret));
      }
    }
    ObSEArray<ObWinFunRawExpr*, 8> adjusted_exprs;
    if (OB_SUCC(ret)) {
      std::pair<int64_t, int64_t>* first = &expr_entries.at(0);
      std::sort(first, first + expr_entries.count());
      for (int64_t i = 0; OB_SUCC(ret) && i < expr_entries.count(); ++i) {
        if (OB_FAIL(adjusted_exprs.push_back(winfunc_exprs.at(expr_entries.at(i).second)))) {
          LOG_WARN("failed to push back window function expr", K(ret));
        }
      }
    }
    if (OB_SUCC(ret) && need_sort) {
      if (OB_FAIL(allocate_sort_as_top(top, sort_keys))) {
        LOG_WARN("failed to allocate sort above node", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(allocate_window_function_above_node(adjusted_exprs, top))) {
        LOG_WARN("failed to allocate window function above top", K(ret));
      } else if (OB_FAIL(top->set_expected_ordering(sort_keys))) {
        LOG_WARN("failed to set expectedd ordering", K(ret));
      }
    }
  }
  return ret;
}

int ObSelectLogPlan::adjust_sort_expr_ordering(ObIArray<ObRawExpr*>& sort_exprs,
    ObIArray<ObOrderDirection>& sort_directions, ObLogicalOperator& child_op, bool check_win_func)
{
  int ret = OB_SUCCESS;
  ObDMLStmt* stmt = NULL;
  bool ordering_used = false;
  const EqualSets& equal_sets = child_op.get_ordering_output_equal_sets();
  const ObIArray<ObRawExpr*>& const_exprs = child_op.get_output_const_exprs();
  if (OB_ISNULL(stmt = child_op.get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null stmt", K(ret), K(stmt));
  } else if (!child_op.get_op_ordering().empty() &&
             OB_FAIL(ObOptimizerUtil::adjust_exprs_by_ordering(
                 sort_exprs, child_op.get_op_ordering(), equal_sets, const_exprs, ordering_used, sort_directions))) {
    LOG_WARN("failed to adjust exprs by ordering", K(ret));
  } else if (ordering_used) {
    // do nothing
  } else {
    bool adjusted = false;
    if (stmt->is_select_stmt() && check_win_func) {
      ObSelectStmt* sel_stmt = static_cast<ObSelectStmt*>(stmt);
      for (int64_t i = 0; OB_SUCC(ret) && !adjusted && i < sel_stmt->get_window_func_count(); ++i) {
        ObWinFunRawExpr* cur_expr = sel_stmt->get_window_func_expr(i);
        if (OB_ISNULL(cur_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get null window function expr", K(ret));
        } else if (cur_expr->get_partition_exprs().count() == 0 && cur_expr->get_order_items().count() == 0) {
          // win_func over(), do nothing
        } else if (OB_FAIL(adjust_exprs_by_win_func(sort_exprs, *cur_expr, equal_sets, const_exprs, sort_directions))) {
          LOG_WARN("failed to adjust exprs by win func", K(ret));
        } else {
          adjusted = true;
        }
      }
    }
    if (OB_SUCC(ret) && !adjusted && stmt->get_order_item_size() > 0) {
      if (OB_FAIL(ObOptimizerUtil::adjust_exprs_by_ordering(
              sort_exprs, stmt->get_order_items(), equal_sets, const_exprs, ordering_used, sort_directions))) {
        LOG_WARN("failed to adjust exprs by ordering", K(ret));
      }
    }
  }
  return ret;
}

int ObSelectLogPlan::adjust_exprs_by_win_func(ObIArray<ObRawExpr*>& exprs, ObWinFunRawExpr& win_expr,
    const EqualSets& equal_sets, const ObIArray<ObRawExpr*>& const_exprs, ObIArray<ObOrderDirection>& directions)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 8> adjusted_exprs;
  ObSEArray<ObOrderDirection, 8> order_types;
  ObSEArray<ObRawExpr*, 8> rest_exprs;
  ObSEArray<ObOrderDirection, 8> rest_order_types;
  ObBitSet<64> expr_idxs;
  bool all_part_used = true;
  for (int64_t i = 0; OB_SUCC(ret) && i < win_expr.get_partition_exprs().count(); ++i) {
    bool find = false;
    ObRawExpr* cur_expr = win_expr.get_partition_exprs().at(i);
    for (int64_t j = 0; OB_SUCC(ret) && !find && j < exprs.count(); ++j) {
      if (expr_idxs.has_member(j)) {
        // already add into adjusted_exprs
      } else if (ObOptimizerUtil::is_expr_equivalent(cur_expr, exprs.at(j), equal_sets)) {
        find = true;
        if (OB_FAIL(adjusted_exprs.push_back(exprs.at(j)))) {
          LOG_WARN("store ordered expr failed", K(ret), K(i), K(j));
        } else if (OB_FAIL(order_types.push_back(directions.at(j)))) {
          LOG_WARN("failed to push back order type");
        } else if (OB_FAIL(expr_idxs.add_member(j))) {
          LOG_WARN("add expr idxs member failed", K(ret), K(j));
        }
      }
    }
    if (!find) {
      all_part_used = false;
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    if (expr_idxs.has_member(i)) {
      // already add into adjusted_exprs
    } else if (OB_FAIL(rest_exprs.push_back(exprs.at(i)))) {
      LOG_WARN("store ordered expr failed", K(ret), K(i));
    } else if (OB_FAIL(rest_order_types.push_back(directions.at(i)))) {
      LOG_WARN("failed to push back order type", K(ret));
    }
  }
  if (OB_SUCC(ret) && all_part_used && win_expr.get_order_items().count() > 0 && rest_exprs.count() > 0) {
    bool dummy_order_used = false;
    if (OB_FAIL(ObOptimizerUtil::adjust_exprs_by_ordering(
            rest_exprs, win_expr.get_order_items(), equal_sets, const_exprs, dummy_order_used, rest_order_types))) {
      LOG_WARN("failed to adjust exprs by ordering", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(append(adjusted_exprs, rest_exprs))) {
      LOG_WARN("failed to append expr", K(ret));
    } else if (OB_FAIL(append(order_types, rest_order_types))) {
      LOG_WARN("failed to append order direction", K(ret));
    } else if (adjusted_exprs.count() != exprs.count() || order_types.count() != exprs.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("exprs don't covered completely", K(adjusted_exprs.count()), K(exprs.count()), K(order_types.count()));
    } else {
      exprs.reuse();
      if (OB_FAIL(exprs.assign(adjusted_exprs))) {
        LOG_WARN("assign adjusted exprs failed", K(ret));
      } else if (OB_FAIL(directions.assign(order_types))) {
        LOG_WARN("failed to assign order types", K(ret));
      }
    }
  }
  return ret;
}

int ObSelectLogPlan::generate_default_directions(const int64_t direction_num, ObIArray<ObOrderDirection>& directions)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < direction_num; ++i) {
    if (OB_FAIL(directions.push_back(default_asc_direction()))) {
      LOG_WARN("failed to push back default asc direction", K(ret));
    }
  }
  return ret;
}

int ObSelectLogPlan::candi_allocate_late_materialization()
{
  int ret = OB_SUCCESS;
  bool need_late_mat = false;
  if (OB_FAIL(if_stmt_need_late_materialization(need_late_mat))) {
    LOG_WARN("failed to check if stmt need late materialization", K(ret));
  } else if (!need_late_mat) {
    /*do nothing*/
  } else {
    bool use_late_mat = false;
    ObLogicalOperator* best_plan = NULL;
    ObLogTableScan* index_scan = NULL;
    TableItem* nl_table_item = NULL;
    ObLogTableScan* nl_table_get = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < candidates_.candidate_plans_.count(); ++i) {
      bool need = false;
      CandidatePlan& plain_plan = candidates_.candidate_plans_.at(i);
      if (OB_FAIL(if_plan_need_late_materialization(plain_plan.plan_tree_, index_scan, need))) {
        LOG_WARN("failed to check if need late materialization", K(ret));
      } else if (!need) {
        /*do nothing*/
      } else if (NULL == nl_table_get &&
                 OB_FAIL(generate_late_materialization_info(index_scan, nl_table_get, nl_table_item))) {
        LOG_WARN("failed to generate later materialization table get", K(ret));
      } else if (OB_FAIL(allocate_late_materialization_join_as_top(
                     plain_plan.plan_tree_, nl_table_get, plain_plan.plan_tree_))) {
        LOG_WARN("failed to generate late materialization join", K(ret));
      } else { /*do nothing*/
      }

      if (OB_FAIL(ret)) {
        /*do nothing*/
      } else if (OB_ISNULL(plain_plan.plan_tree_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (NULL == best_plan || plain_plan.plan_tree_->get_cost() < best_plan->get_cost()) {
        best_plan = plain_plan.plan_tree_;
        use_late_mat = need;
      } else { /*do nothing*/
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(best_plan)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(best_plan), K(ret));
      } else {
        candidates_.candidate_plans_.reset();
        if (OB_FAIL(candidates_.candidate_plans_.push_back(CandidatePlan(best_plan)))) {
          LOG_WARN("failed to push back candidate plan", K(ret));
        } else if (use_late_mat && !get_optimizer_context().is_cost_evaluation() &&
                   OB_FAIL(adjust_late_materialization_structure(best_plan, index_scan, nl_table_get, nl_table_item))) {
          LOG_WARN("failed to adjust late materialization stmt", K(ret));
        } else {
          candidates_.plain_plan_.first = best_plan->get_cost();
          candidates_.plain_plan_.second = 0;
        }
      }
    }
  }
  return ret;
}

int ObSelectLogPlan::adjust_late_materialization_structure(
    ObLogicalOperator* join, ObLogTableScan* index_scan, ObLogTableScan* table_scan, TableItem* table_item)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(join) || OB_ISNULL(index_scan) || OB_ISNULL(table_scan) || OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(join), K(index_scan), K(table_scan), K(table_item), K(ret));
  } else if (OB_FAIL(adjust_late_materialization_stmt_structure(index_scan, table_scan, table_item))) {
    LOG_WARN("failed to adjust late materialization stmt structure", K(ret));
  } else if (OB_FAIL(adjust_late_materialization_plan_structure(join, index_scan, table_scan))) {
    LOG_WARN("failed to adjust latematerialization plan structure", K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

int ObSelectLogPlan::convert_project_columns(
    uint64_t table_id, uint64_t project_id, const ObString& project_table_name, ObIArray<uint64_t>& index_columns)
{
  int ret = OB_SUCCESS;
  ObDMLStmt* stmt = NULL;
  if (OB_ISNULL(stmt = get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get_stmt() returns null", K(ret));
  } else {
    ColumnItem* item = NULL;
    ObColumnRefRawExpr* expr = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_column_size(); ++i) {
      if (OB_ISNULL(item = stmt->get_column_item(i)) || OB_ISNULL(expr = item->get_expr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected NULL pointer", K(item), K(ret));
      } else if (item->table_id_ == table_id && !ObOptimizerUtil::find_item(index_columns, item->column_id_)) {
        item->set_ref_id(project_id, item->column_id_);
        expr->set_ref_id(project_id, expr->get_column_id());
        expr->set_table_name(project_table_name);
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObSelectLogPlan::adjust_late_materialization_stmt_structure(
    ObLogTableScan* index_scan, ObLogTableScan* table_scan, TableItem* table_item)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 8> old_exprs;
  ObSEArray<ObRawExpr*, 8> new_exprs;
  ObSEArray<uint64_t, 4> old_column_ids;
  ObSEArray<uint64_t, 4> new_column_ids;
  ObSEArray<ObRawExpr*, 4> temp_exprs;
  ObSEArray<ObRawExpr*, 8> rowkeys;
  ObSEArray<ObRawExpr*, 8> ordering;
  ObSEArray<ObDMLStmt::PartExprItem, 8> new_part_expr_items;
  ObSelectStmt* stmt = NULL;
  if (OB_ISNULL(stmt = get_stmt()) || OB_ISNULL(index_scan) || OB_ISNULL(index_scan->get_est_cost_info()) ||
      OB_ISNULL(table_scan) || OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(stmt), K(index_scan), K(table_scan), K(table_item), K(ret));
  } else if (OB_FAIL(stmt->get_table_items().push_back(table_item))) {
    LOG_WARN("failed to push back table item", K(ret));
  } else if (OB_FAIL(stmt->set_table_bit_index(table_item->table_id_))) {
    LOG_WARN("failed to set table bit index", K(ret));
  } else if (OB_FAIL(old_column_ids.assign(index_scan->get_est_cost_info()->table_scan_param_.column_ids_))) {
    LOG_WARN("failed to assign column ids", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::generate_rowkey_exprs(stmt,
                 get_optimizer_context(),
                 index_scan->get_table_id(),
                 index_scan->get_ref_table_id(),
                 rowkeys,
                 ordering))) {
    LOG_WARN("failed to generate rowkey exprs", K(ret));
  } else if (OB_FAIL(stmt->get_select_exprs(temp_exprs))) {
    LOG_WARN("failed to get select exprs", K(ret));
  } else if (OB_FAIL(append(temp_exprs, rowkeys))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else {
    // mark rowkeys as referenced
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkeys.count(); i++) {
      if (OB_ISNULL(rowkeys.at(i)) || OB_UNLIKELY(!rowkeys.at(i)->is_column_ref_expr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (is_shadow_column(static_cast<ObColumnRefRawExpr*>(rowkeys.at(i))->get_column_id())) {
        /*do nothing*/
      } else {
        rowkeys.at(i)->set_explicited_reference();
      }
    }
    ObIArray<ColumnItem>& range_columns = index_scan->get_est_cost_info()->range_columns_;
    for (int64_t i = 0; OB_SUCC(ret) && i < range_columns.count(); ++i) {
      ObRawExpr* new_col_expr = NULL;
      ObColumnRefRawExpr* col_expr = range_columns.at(i).expr_;
      if (OB_FAIL(ObRawExprUtils::copy_expr(
              get_optimizer_context().get_expr_factory(), col_expr, new_col_expr, COPY_REF_SHARED))) {
        LOG_WARN("failed to copy expr", K(ret));
      } else if (OB_ISNULL(new_col_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("copy expr failed", K(ret));
      } else if (!new_col_expr->is_column_ref_expr()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expect column ref expr", K(ret));
      } else {
        range_columns.at(i).expr_ = static_cast<ObColumnRefRawExpr*>(new_col_expr);
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(index_scan->set_range_columns(range_columns))) {
      LOG_WARN("failed to set range columns", K(ret));
    } else if (OB_FAIL(convert_project_columns(index_scan->get_table_id(),
                   table_scan->get_table_id(),
                   table_scan->get_table_name(),
                   old_column_ids))) {
      LOG_WARN("failed to convert project columns", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::generate_rowkey_exprs(stmt,
                   get_optimizer_context(),
                   table_scan->get_table_id(),
                   table_scan->get_ref_table_id(),
                   rowkeys,
                   ordering))) {
      LOG_WARN("failed to generate rowkeys", K(table_scan->get_table_id()), K(ret));
    }
  }
  return ret;
}

int ObSelectLogPlan::adjust_late_materialization_plan_structure(
    ObLogicalOperator* join, ObLogTableScan* index_scan, ObLogTableScan* table_scan)
{
  int ret = OB_SUCCESS;
  ObSelectStmt* stmt = NULL;
  const ObTableSchema* table_schema = NULL;
  ObSqlSchemaGuard* schema_guard = NULL;
  ObSEArray<uint64_t, 8> rowkey_ids;
  if (OB_ISNULL(stmt = get_stmt()) || OB_ISNULL(join) || OB_ISNULL(index_scan) || OB_ISNULL(table_scan)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(stmt), K(join), K(index_scan), K(table_scan), K(ret));
  } else if (OB_UNLIKELY(log_op_def::LOG_JOIN != join->get_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected join type", K(join->get_type()), K(ret));
  } else if (OB_ISNULL(schema_guard = get_optimizer_context().get_sql_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(index_scan->get_ref_table_id(), table_schema))) {
    LOG_WARN("failed to get table schema", K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(table_schema), K(ret));
  } else if (OB_FAIL(table_schema->get_rowkey_column_ids(rowkey_ids))) {
    LOG_WARN("failed to get rowkey ids", K(ret));
  } else {
    ObLogJoin* log_join = static_cast<ObLogJoin*>(join);
    ObSEArray<ColumnItem, 4> range_columns;
    ObSEArray<ObRawExpr*, 4> join_conditions;
    ObSEArray<std::pair<int64_t, ObRawExpr*>, 4> scan_params;
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_ids.count(); i++) {
      ColumnItem* col_item = NULL;
      ObRawExpr* equal_expr = NULL;
      std::pair<int64_t, ObRawExpr*> init_expr;
      ObColumnRefRawExpr* left_expr = NULL;
      ObColumnRefRawExpr* right_expr = NULL;
      if (OB_ISNULL(left_expr = stmt->get_column_expr_by_id(index_scan->get_table_id(), rowkey_ids.at(i))) ||
          OB_ISNULL(right_expr = stmt->get_column_expr_by_id(table_scan->get_table_id(), rowkey_ids.at(i)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(left_expr), K(right_expr), K(ret));
      } else if (OB_FAIL(ObRawExprUtils::create_exec_param_expr(get_stmt(),
                     optimizer_context_.get_expr_factory(),
                     reinterpret_cast<ObRawExpr*&>(left_expr),
                     optimizer_context_.get_session_info(),
                     init_expr))) {
        LOG_WARN("create param for stmt error in extract_params_for_nl", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::create_equal_expr(optimizer_context_.get_expr_factory(),
                     optimizer_context_.get_session_info(),
                     right_expr,
                     left_expr,
                     equal_expr))) {
        LOG_WARN("failed to crerate equal expr", K(ret));
      } else if (OB_ISNULL(equal_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(scan_params.push_back(init_expr))) {
        LOG_WARN("failed to push back scan param", K(ret));
      } else if (OB_FAIL(join_conditions.push_back(equal_expr))) {
        LOG_WARN("failed to push back equal expr", K(ret));
      } else if (OB_ISNULL(col_item = stmt->get_column_item_by_id(table_scan->get_table_id(), rowkey_ids.at(i)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(table_scan->get_table_id()), K(rowkey_ids.at(i)), K(col_item), K(ret));
      } else if (OB_FAIL(range_columns.push_back(*col_item))) {
        LOG_WARN("failed to push back column item", K(ret));
      } else { /*do nothing*/
      }
    }
    if (OB_SUCC(ret)) {
      const ObDataTypeCastParams dtc_params =
          ObBasicSessionInfo::create_dtc_params(optimizer_context_.get_session_info());
      ObQueryRange* query_range = static_cast<ObQueryRange*>(get_allocator().alloc(sizeof(ObQueryRange)));
      const ParamStore* params = get_optimizer_context().get_params();
      if (OB_ISNULL(query_range)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory for query range", K(ret));
      } else if (OB_ISNULL(params)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else {
        query_range = new (query_range) ObQueryRange(get_allocator());
        if (OB_FAIL(query_range->preliminary_extract_query_range(range_columns, join_conditions, dtc_params, params))) {
          LOG_WARN("failed to preliminary extract query range", K(ret));
        } else if (OB_FAIL(table_scan->set_range_columns(range_columns))) {
          LOG_WARN("failed to set range columns", K(ret));
        } else {
          table_scan->set_pre_query_range(query_range);
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(table_scan->set_filters(join_conditions))) {
        LOG_WARN("failed to set filters", K(ret));
      } else if (OB_FAIL(log_join->set_nl_params(scan_params))) {
        LOG_WARN("failed to set nl params", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObSelectLogPlan::generate_late_materialization_info(
    ObLogTableScan* index_scan, ObLogTableScan*& table_get, TableItem*& table_item)
{
  int ret = OB_SUCCESS;
  table_get = NULL;
  table_item = NULL;
  if (OB_ISNULL(get_stmt()) || OB_ISNULL(get_stmt()->get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(get_stmt()), K(ret));
  } else {
    uint64_t new_table_id = get_stmt()->get_query_ctx()->available_tb_id_--;
    if (OB_FAIL(generate_late_materialization_table_item(index_scan->get_table_id(), new_table_id, table_item))) {
      LOG_WARN("failed to generate late materialization table item", K(ret));
    } else if (OB_FAIL(generate_late_materialization_table_get(index_scan, table_item, new_table_id, table_get))) {
      LOG_WARN("failed to generate late materialization table get", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObSelectLogPlan::generate_late_materialization_table_get(
    ObLogTableScan* index_scan, TableItem* table_item, uint64_t table_id, ObLogTableScan*& table_get)
{
  int ret = OB_SUCCESS;
  ObDMLStmt* stmt = NULL;
  ObLogTableScan* table_scan = NULL;
  ObTablePartitionInfo* table_scan_part_info = NULL;
  const ObTablePartitionInfo* index_scan_part_info = NULL;
  ObIAllocator& allocator = get_allocator();
  table_get = NULL;
  if (OB_ISNULL(index_scan) || OB_ISNULL(table_item) || OB_ISNULL(stmt = get_stmt()) ||
      OB_ISNULL(stmt->get_query_ctx()) || OB_ISNULL(index_scan_part_info = index_scan->get_table_partition_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(index_scan), K(table_item), K(stmt), K(index_scan_part_info), K(ret));
  } else if (OB_ISNULL(
                 table_scan = static_cast<ObLogTableScan*>(get_log_op_factory().allocate(*this, LOG_TABLE_SCAN)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate table scan operator", K(ret));
  } else if (OB_ISNULL(table_scan_part_info =
                           static_cast<ObTablePartitionInfo*>(allocator.alloc(sizeof(ObTablePartitionInfo))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate table partition info", K(ret));
  } else if (FALSE_IT(table_scan_part_info = new (table_scan_part_info) ObTablePartitionInfo(allocator))) {
  } else if (OB_FAIL(table_scan_part_info->assign(*index_scan_part_info))) {
    LOG_WARN("failed to assigin table partition info", K(ret));
  } else {
    table_scan->set_index_back(false);
    table_scan->set_table_id(table_id);
    table_scan->set_ref_table_id(index_scan->get_ref_table_id());
    table_scan->set_index_table_id(index_scan->get_ref_table_id());
    table_scan->set_scan_direction(index_scan->get_scan_direction());
    table_scan_part_info->get_table_location().set_table_id(table_scan->get_table_id());
    table_scan_part_info->get_phy_tbl_location_info_for_update().set_table_location_key(
        table_scan->get_table_id(), table_scan->get_ref_table_id());
    table_scan->set_table_partition_info(table_scan_part_info);
    table_scan->set_for_update(index_scan->is_for_update(), index_scan->get_for_update_wait_us());
    table_scan->set_part_hint(index_scan->get_part_hint());
    table_scan->set_exist_hint(index_scan->exist_hint());
    table_scan->get_hint().read_consistency_ = index_scan->get_hint().read_consistency_;
    table_scan->get_hint().frozen_version_ = index_scan->get_hint().frozen_version_;
    table_scan->get_table_name() =
        table_item->alias_name_.length() > 0 ? table_item->alias_name_ : table_item->table_name_;
    // set card and cost
    table_scan->set_card(1.0);
    table_scan->set_op_cost(ObOptEstCost::cost_late_materialization_table_get(stmt->get_column_size()));
    table_scan->set_cost(table_scan->get_op_cost());
    table_scan->set_table_row_count(index_scan->get_table_row_count());
    table_scan->set_output_row_count(1.0);
    table_scan->set_phy_query_range_row_count(1.0);
    table_scan->set_query_range_row_count(1.0);
    table_get = table_scan;
  }
  return ret;
}

int ObSelectLogPlan::generate_late_materialization_table_item(
    uint64_t old_table_id, uint64_t new_table_id, TableItem*& new_table_item)
{
  int ret = OB_SUCCESS;
  ObDMLStmt* stmt = NULL;
  TableItem* temp_item = NULL;
  TableItem* old_table_item = NULL;
  new_table_item = NULL;
  if (OB_ISNULL(stmt = get_stmt()) || OB_ISNULL(old_table_item = stmt->get_table_item_by_id(old_table_id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_ISNULL(temp_item = stmt->create_table_item(get_allocator()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to create temp table item", K(ret));
  } else {
    temp_item->type_ = TableItem::ALIAS_TABLE;
    temp_item->database_name_ = old_table_item->database_name_;
    temp_item->table_name_ = old_table_item->table_name_;
    temp_item->is_system_table_ = old_table_item->is_system_table_;
    temp_item->is_view_table_ = old_table_item->is_view_table_;
    temp_item->table_id_ = new_table_id;
    temp_item->ref_id_ = old_table_item->ref_id_;

    const char* str = "_alias";
    char* buf = static_cast<char*>(get_allocator().alloc(old_table_item->table_name_.length() + strlen(str)));
    if (OB_UNLIKELY(NULL == buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to allocate string buffer", K(ret));
    } else {
      MEMCPY(buf, old_table_item->table_name_.ptr(), old_table_item->table_name_.length());
      MEMCPY(buf + old_table_item->table_name_.length(), str, strlen(str));
      temp_item->alias_name_.assign_ptr(buf, old_table_item->table_name_.length() + strlen(str));
    }

    if (OB_SUCC(ret)) {
      new_table_item = temp_item;
    }
  }
  return ret;
}

int ObSelectLogPlan::allocate_late_materialization_join_as_top(
    ObLogicalOperator* left_child, ObLogicalOperator* right_child, ObLogicalOperator*& join_op)
{
  int ret = OB_SUCCESS;
  ObLogJoin* join = NULL;
  if (OB_ISNULL(left_child) || OB_ISNULL(right_child)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_ISNULL(join = static_cast<ObLogJoin*>(get_log_op_factory().allocate(*this, LOG_JOIN)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to allocate join_op operator", K(ret));
  } else if (OB_ISNULL(join)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    join->set_left_child(left_child);
    join->set_right_child(right_child);
    join->set_join_type(INNER_JOIN);
    join->set_join_algo(NESTED_LOOP_JOIN);
    join->set_late_mat(true);
    join->set_card(left_child->get_card());
    join->set_width(left_child->get_width());
    ObOptEstCost::cost_late_materialization_table_join(left_child->get_card(),
        left_child->get_cost(),
        right_child->get_card(),
        right_child->get_cost(),
        join->get_op_cost(),
        join->get_cost());
    if (OB_FAIL(join->set_op_ordering(left_child->get_op_ordering()))) {
      LOG_WARN("failed to set op ordering", K(ret));
    } else if (OB_FAIL(join->init_est_sel_info())) {
      LOG_WARN("failed to init est sel info", K(ret));
    } else if (OB_FAIL(join->est_cost())) {
      LOG_WARN("failed to estimate cost", K(ret));
    } else {
      join->set_interesting_order_info(left_child->get_interesting_order_info());
      join->set_fd_item_set(&left_child->get_fd_item_set());
      join_op = join;
    }
  }
  return ret;
}

int ObSelectLogPlan::if_plan_need_late_materialization(ObLogicalOperator* top, ObLogTableScan*& index_scan, bool& need)
{
  int ret = OB_SUCCESS;
  ObDMLStmt* stmt = NULL;
  ObLogicalOperator* child_sort = NULL;
  ObLogicalOperator* child_scan = NULL;
  need = false;
  index_scan = NULL;
  if (OB_ISNULL(top) || OB_ISNULL(stmt = get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(top), K(get_stmt()), K(ret));
  } else if (log_op_def::LOG_LIMIT != top->get_type()) {
    /*do nothing*/
  } else if (OB_ISNULL(child_sort = top->get_child(ObLogicalOperator::first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (log_op_def::LOG_SORT != child_sort->get_type()) {
    /*do nothing*/
  } else if (OB_ISNULL(child_scan = child_sort->get_child(ObLogicalOperator::first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (log_op_def::LOG_TABLE_SCAN != child_scan->get_type() &&
             log_op_def::LOG_MV_TABLE_SCAN != child_scan->get_type()) {
    /*do nothing*/
  } else {
    ObSEArray<ObRawExpr*, 4> temp_exprs;
    ObSEArray<ObRawExpr*, 4> table_keys;
    ObSEArray<ObRawExpr*, 4> key_ordering;
    ObSEArray<uint64_t, 4> index_column_ids;
    ObSEArray<uint64_t, 4> used_column_ids;
    const ObTableSchema* index_schema = NULL;
    ObSqlSchemaGuard* schema_guard = NULL;
    ObLogTableScan* table_scan = static_cast<ObLogTableScan*>(child_scan);
    const ObTableLocationType location_type = table_scan->get_phy_location_type();
    // check whether index key cover filter exprs, sort exprs and part exprs
    if (table_scan->is_index_scan() && table_scan->get_index_back() &&
        (OB_TBL_LOCATION_LOCAL == location_type || OB_TBL_LOCATION_REMOTE == location_type)) {
      if (OB_FAIL(ObOptimizerUtil::generate_rowkey_exprs(get_stmt(),
              get_optimizer_context(),
              table_scan->get_table_id(),
              table_scan->get_ref_table_id(),
              table_keys,
              key_ordering))) {
        LOG_WARN("failed to generate rowkey exprs", K(ret));
      } else if (OB_FAIL(static_cast<ObLogSort*>(child_sort)->get_sort_exprs(temp_exprs))) {
        LOG_WARN("failed to get sort exprs", K(ret));
      } else if (OB_FAIL(append(temp_exprs, table_scan->get_filter_exprs())) ||
                 OB_FAIL(append(temp_exprs, table_keys))) {
        LOG_WARN("failed to append exprs", K(ret));
      } else if (NULL != table_scan->get_pre_query_range() &&
                 OB_FAIL(append(temp_exprs, table_scan->get_pre_query_range()->get_range_exprs()))) {
        LOG_WARN("failed to append exprs", K(ret));
      } else if (OB_ISNULL(schema_guard = get_optimizer_context().get_sql_schema_guard())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected nul", K(ret));
      } else if (OB_FAIL(schema_guard->get_table_schema(table_scan->get_index_table_id(), index_schema))) {
        LOG_WARN("failed to get table schema", K(ret));
      } else if (OB_ISNULL(index_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(index_schema->get_column_ids(index_column_ids))) {
        LOG_WARN("failed to get column ids", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::extract_column_ids(temp_exprs, used_column_ids))) {
        LOG_WARN("failed to extract column ids", K(ret));
      } else {
        need = ObOptimizerUtil::is_subset(used_column_ids, index_column_ids);
      }
    }
    // update cost for late materialization
    if (OB_SUCC(ret) && need) {
      if (OB_ISNULL(table_scan->get_est_cost_info())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(table_scan->get_est_cost_info()->table_scan_param_.column_ids_.assign(used_column_ids))) {
        LOG_WARN("failed to assign column ids", K(ret));
      } else {
        table_scan->get_est_cost_info()->index_meta_info_.is_index_back_ = false;
        table_scan->set_index_back(false);
        table_scan->set_index_back_row_count(0.0);
        double query_range_row_count = table_scan->get_query_range_row_count();
        double phy_query_range_row_count = table_scan->get_phy_query_range_row_count();
        double op_cost = 0.0;
        double index_back_cost = 0.0;
        // estimate cost
        if (OB_FAIL(ObOptEstCost::cost_table(*table_scan->get_est_cost_info(),
                query_range_row_count,
                phy_query_range_row_count,
                op_cost,
                index_back_cost))) {
          LOG_WARN("failed to get index access info", K(ret));
        } else {
          table_scan->set_cost(op_cost);
          table_scan->set_op_cost(op_cost);
        }
        // estimate width
        if (OB_FAIL(ret)) {
          /*do nothing*/
        } else {
          double width = 0.0;
          ObSEArray<ObRawExpr*, 8> column_exprs;
          for (int64_t i = 0; OB_SUCC(ret) && i < used_column_ids.count(); i++) {
            ColumnItem* col_item = NULL;
            if (OB_ISNULL(col_item = stmt->get_column_item_by_id(table_scan->get_table_id(), used_column_ids.at(i))) ||
                OB_ISNULL(col_item->expr_)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("get unexpected null", K(col_item), K(ret));
            } else if (OB_FAIL(column_exprs.push_back(col_item->expr_))) {
              LOG_WARN("failed to push back column expr", K(ret));
            } else { /*do nothing*/
            }
          }
          if (OB_FAIL(ret)) {
            /*do nothing*/
          } else if (OB_FAIL(ObOptEstCost::estimate_width_for_columns(column_exprs, width))) {
            LOG_WARN("failed to estimate width for columns", K(ret));
          } else {
            table_scan->set_width(width);
          }
        }
        if (OB_FAIL(ret)) {
          /*do nothing*/
        } else if (OB_FAIL(child_sort->est_cost())) {
          LOG_WARN("failed to compute property", K(ret));
        } else if (OB_FAIL(top->est_cost())) {
          LOG_WARN("failed to compute property", K(ret));
        } else {
          index_scan = table_scan;
        }
      }
    }
  }
  return ret;
}

int ObSelectLogPlan::if_stmt_need_late_materialization(bool& need)
{
  int ret = OB_SUCCESS;
  ObSelectStmt* select_stmt = NULL;
  int64_t child_stmt_size = 0;
  need = false;
  if (OB_ISNULL(select_stmt = get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(select_stmt->get_child_stmt_size(child_stmt_size))) {
    LOG_WARN("failed to get child stmt size", K(ret));
  } else {
    need = (OB_NO_USE_LATE_MATERIALIZATION != select_stmt->get_stmt_hint().use_late_mat_) && select_stmt->has_limit() &&
           select_stmt->has_order_by() && !select_stmt->has_hierarchical_query() && !select_stmt->has_group_by() &&
           !select_stmt->has_rollup() && !select_stmt->has_having() && !select_stmt->has_window_function() &&
           !select_stmt->has_distinct() && !select_stmt->is_unpivot_select() &&
           1 == select_stmt->get_from_item_size() && 1 == select_stmt->get_table_size() &&
           NULL != select_stmt->get_table_item(0) && select_stmt->get_table_item(0)->is_basic_table() &&
           !select_stmt->get_table_item(0)->is_system_table_ &&
           NULL == get_stmt()->get_part_expr(
                       select_stmt->get_table_item(0)->table_id_, select_stmt->get_table_item(0)->ref_id_) &&
           0 == child_stmt_size && !select_stmt->is_calc_found_rows();
  }
  return ret;
}

int ObSelectLogPlan::candi_allocate_unpivot()
{
  int ret = OB_SUCCESS;
  double min_cost = 0.0;
  int64_t min_idx = OB_INVALID_INDEX;
  for (int64_t idx = 0; OB_SUCC(ret) && idx < candidates_.candidate_plans_.count(); ++idx) {
    CandidatePlan& plain_plan = candidates_.candidate_plans_.at(idx);
    if (OB_FAIL(allocate_unpivot_as_top(plain_plan.plan_tree_))) {
      LOG_WARN("fail alloc candi sequence op", K(idx), K(ret));
    } else if (OB_ISNULL(plain_plan.plan_tree_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null plan tree", K(ret));
    } else if (plain_plan.plan_tree_->get_cost() < min_cost || OB_INVALID_INDEX == min_idx) {
      min_cost = plain_plan.plan_tree_->get_cost();
      min_idx = idx;
    } else { /*do nothing*/
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(OB_INVALID_INDEX == min_idx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Min idx is invalid", K(ret), K(min_idx));
    } else {
      candidates_.plain_plan_.first = min_cost;
      candidates_.plain_plan_.second = min_idx;
    }
  }
  return ret;
}

int ObSelectLogPlan::allocate_unpivot_as_top(ObLogicalOperator*& old_top)
{
  int ret = OB_SUCCESS;
  ObDMLStmt* stmt = static_cast<ObDMLStmt*>(get_stmt());
  ObLogUnpivot* unpivot = NULL;
  if (OB_ISNULL(old_top) || OB_ISNULL(stmt) || OB_UNLIKELY(!stmt->is_unpivot_select()) ||
      OB_UNLIKELY(stmt->get_table_items().empty()) || OB_ISNULL(stmt->get_table_item(0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Get unexpected null", K(ret), K(old_top), KPC(stmt));
  } else {
    if (OB_ISNULL(unpivot = static_cast<ObLogUnpivot*>(get_log_op_factory().allocate(*this, LOG_UNPIVOT)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to allocate subquery operator", K(ret));
    } else {
      const TableItem* table_item = stmt->get_table_item(0);
      unpivot->unpivot_info_ = stmt->get_unpivot_info();
      unpivot->set_subquery_id(table_item->table_id_);
      unpivot->get_subquery_name().assign_ptr(table_item->table_name_.ptr(), table_item->table_name_.length());
      unpivot->set_child(ObLogicalOperator::first_child, old_top);
      if (OB_FAIL(unpivot->compute_property())) {
        LOG_WARN("failed to compute property", K(ret));
      } else {
        old_top = unpivot;
      }
    }
  }
  return ret;
}

int ObSelectLogPlan::candi_allocate_for_update()
{
  int ret = OB_SUCCESS;
  ObSelectStmt* sel_stmt = NULL;
  ObLogicalOperator* best_plan = NULL;
  ObSEArray<CandidatePlan, 1> for_update_plans;
  if (OB_ISNULL(sel_stmt = get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params are invalid", K(ret), K(sel_stmt));
  } else if (sel_stmt->has_distinct() || sel_stmt->has_group_by() || sel_stmt->is_set_stmt()) {
    ret = OB_ERR_FOR_UPDATE_SELECT_VIEW_CANNOT;
    LOG_WARN("for update can not exists in stmt with distint, groupby", K(ret));
  } else if (OB_FAIL(get_current_best_plan(best_plan))) {
    LOG_WARN("failed to get best plan", K(ret));
  } else if (OB_ISNULL(best_plan)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(allocate_for_update_as_top(best_plan))) {
    LOG_WARN("failed to allocate for update as top", K(ret));
  } else if (OB_FAIL(allocate_material_as_top(best_plan))) {
    LOG_WARN("failed to allocate material as top", K(ret));
  } else if (OB_FAIL(for_update_plans.push_back(CandidatePlan(best_plan)))) {
    LOG_WARN("failed to push back for update plan", K(ret));
  } else if (OB_FAIL(prune_and_keep_best_plans(for_update_plans))) {
    LOG_WARN("failed to prune and keep best plans", K(ret));
  }
  return ret;
}

int ObSelectLogPlan::allocate_for_update_as_top(ObLogicalOperator*& top)
{
  int ret = OB_SUCCESS;
  ObSelectStmt* sel_stmt = NULL;
  ObLogForUpdate* for_update_op = NULL;
  if (OB_ISNULL(sel_stmt = get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("select stmt is null", K(ret), K(sel_stmt));
  } else if (OB_ISNULL(
                 for_update_op = static_cast<ObLogForUpdate*>(get_log_op_factory().allocate(*this, LOG_FOR_UPD)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to allocate for update operator", K(ret));
  } else if (OB_FAIL(for_update_op->add_child(top))) {
    LOG_WARN("failed to add child for for_update operator", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < sel_stmt->get_table_size(); ++i) {
    TableItem* table = NULL;
    ObSEArray<ObRawExpr*, 4> rowkeys;
    ObSEArray<ObRawExpr*, 4> index_ordering;
    if (OB_ISNULL(table = sel_stmt->get_table_item(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table item is null", K(ret), K(i), K(table));
    } else if (!table->for_update_) {
      // do nothing
    } else if (OB_UNLIKELY(!table->is_basic_table()) || OB_UNLIKELY(is_virtual_table(table->ref_id_))) {
      // invalid usage
      ret = OB_ERR_FOR_UPDATE_SELECT_VIEW_CANNOT;
      LOG_USER_ERROR(OB_ERR_FOR_UPDATE_SELECT_VIEW_CANNOT);
    } else if (OB_FAIL(ObOptimizerUtil::generate_rowkey_exprs(
                   sel_stmt, get_optimizer_context(), table->table_id_, table->ref_id_, rowkeys, index_ordering))) {
      LOG_WARN("failed to generate rowkey exprs", K(ret));
    } else {
      ObSEArray<ObColumnRefRawExpr*, 4> keys;
      for (int64_t i = 0; OB_SUCC(ret) && i < rowkeys.count(); ++i) {
        if (OB_ISNULL(rowkeys.at(i)) || OB_UNLIKELY(!rowkeys.at(i)->is_column_ref_expr())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid rowkey expr", K(ret), K(rowkeys.at(i)));
        } else if (OB_FAIL(keys.push_back(static_cast<ObColumnRefRawExpr*>(rowkeys.at(i))))) {
          LOG_WARN("failed to push back column expr", K(ret));
        } else {
          rowkeys.at(i)->set_explicited_reference();
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(for_update_op->add_for_update_table(table->table_id_, keys))) {
          LOG_WARN("failed to add for update table", K(ret));
        } else {
          for_update_op->set_wait_ts(table->for_update_wait_us_);
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(for_update_op->compute_property())) {
      LOG_WARN("failed to compute property", K(ret));
    } else {
      top = for_update_op;
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
