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
#include "sql/optimizer/ob_log_topk.h"
#include "lib/utility/ob_tracepoint.h"
#include "sql/optimizer/ob_log_plan_factory.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "sql/optimizer/ob_opt_est_cost.h"
#include "sql/optimizer/ob_optimizer_context.h"
#include "sql/optimizer/ob_opt_est_cost.h"
#include "sql/optimizer/ob_log_unpivot.h"
#include "sql/optimizer/ob_log_link_scan.h"
#include "common/ob_smart_call.h"
#include "share/system_variable/ob_sys_var_class_type.h"

using namespace oceanbase;
using namespace sql;
using namespace oceanbase::common;
using namespace oceanbase::sql::log_op_def;
using namespace oceanbase::jit::expr;
using share::schema::ObTableSchema;

namespace oceanbase
{
namespace sql
{
ObSelectLogPlan::ObSelectLogPlan(ObOptimizerContext &ctx, const ObSelectStmt *stmt)
    : ObLogPlan(ctx, stmt)
{
}

ObSelectLogPlan::~ObSelectLogPlan()
{
}

int ObSelectLogPlan::candi_allocate_group_by()
{
  int ret = OB_SUCCESS;
  bool is_unique = false;
  bool having_has_rownum = false;
  const ObSelectStmt *stmt = NULL;
  ObLogicalOperator *best_plan = NULL;
  ObSEArray<ObRawExpr*, 4> reduce_exprs;
  ObSEArray<ObRawExpr*, 4> group_by_exprs;
  ObSEArray<ObRawExpr*, 4> rollup_exprs;
  ObSEArray<ObOrderDirection, 4> group_by_directions;
  ObSEArray<ObOrderDirection, 4> rollup_directions;
  ObSEArray<ObRawExpr*, 8> having_subquery_exprs;
  ObSEArray<ObRawExpr*, 8> having_normal_exprs;
  ObSEArray<ObRawExpr*, 8> candi_subquery_exprs;
  OPT_TRACE_TITLE("start generate group by");
  if (OB_ISNULL(stmt = get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(append(candi_subquery_exprs, stmt->get_group_exprs())) ||
             OB_FAIL(append(candi_subquery_exprs, stmt->get_rollup_exprs())) ||
             OB_FAIL(append(candi_subquery_exprs, stmt->get_aggr_items()))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else if (OB_FAIL(candi_allocate_subplan_filter_for_exprs(candi_subquery_exprs))) {
    LOG_WARN("failed to allocate subplan filter for exprs", K(ret));
  } else if (stmt->is_scala_group_by()) {
    if (OB_FAIL(ObOptimizerUtil::classify_subquery_exprs(stmt->get_having_exprs(),
                                                         having_subquery_exprs,
                                                         having_normal_exprs))) {
      LOG_WARN("failed to classify subquery exprs", K(ret));
    } else if (OB_FAIL(candi_allocate_scala_group_by(stmt->get_aggr_items(),
                                                     having_normal_exprs,
                                                     stmt->is_from_pivot()))) {
      LOG_WARN("failed to allocate scala group by", K(ret));
    }
  } else if (OB_FAIL(ObOptimizerUtil::classify_subquery_exprs(stmt->get_having_exprs(),
                                                              having_subquery_exprs,
                                                              having_normal_exprs))) {
    LOG_WARN("failed to classify subquery exprs", K(ret));
  } else if (OB_FAIL(ObTransformUtils::check_has_rownum(stmt->get_having_exprs(),
                                                        having_has_rownum))) {
    LOG_WARN("check has_rownum error", K(ret));
  } else if (OB_FAIL(candidates_.get_best_plan(best_plan))) {
    LOG_WARN("failed to get best plan", K(ret));
  } else if (OB_ISNULL(best_plan)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(best_plan), K(ret));
  } else if (OB_FAIL(get_groupby_rollup_exprs(best_plan,
                                              reduce_exprs,
                                              group_by_exprs,
                                              rollup_exprs,
                                              group_by_directions,
                                              rollup_directions))) {
    LOG_WARN("failed to get groupby rollup exprs", K(ret));
  } else if (0 == stmt->get_aggr_item_size() && !stmt->has_rollup() &&
             OB_FAIL(ObOptimizerUtil::is_exprs_unique(group_by_exprs,
                                                      best_plan->get_table_set(),
                                                      best_plan->get_fd_item_set(),
                                                      best_plan->get_output_equal_sets(),
                                                      best_plan->get_output_const_exprs(),
                                                      is_unique))) {
    LOG_WARN("failed to check group by exprs is unique", K(ret));
  } else if (is_unique && !having_has_rownum) {
    OPT_TRACE("group by expr is unique, no need group by", group_by_exprs);
    LOG_TRACE("group by expr is unique, no need group by", K(group_by_exprs));
    if (!having_normal_exprs.empty() && OB_FAIL(candi_allocate_filter(having_normal_exprs))) {
      LOG_WARN("failed to allocate filter", K(ret));
    } else { /*do nothing*/ }
  } else {
    if (OB_FAIL(candi_allocate_normal_group_by(reduce_exprs,
                                               group_by_exprs,
                                               group_by_directions,
                                               rollup_exprs,
                                               rollup_directions,
                                               having_normal_exprs,
                                               stmt->get_aggr_items(),
                                               stmt->is_from_pivot()))) {
      LOG_WARN("failed to allocate normal group by", K(ret));
    } else { /*do nothing*/ }
  }

  if (OB_SUCC(ret) && !having_subquery_exprs.empty()) {
    if (OB_FAIL(candi_allocate_subplan_filter(having_subquery_exprs, &having_subquery_exprs))) {
      LOG_WARN("failed to allocate subplan filter", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObSelectLogPlan::get_groupby_rollup_exprs(const ObLogicalOperator *top,
                                              ObIArray<ObRawExpr*> &reduce_exprs,
                                              ObIArray<ObRawExpr *> &group_by_exprs,
                                              common::ObIArray<ObRawExpr *> &rollup_exprs,
                                              ObIArray<ObOrderDirection> &group_directions,
                                              common::ObIArray<ObOrderDirection> &rollup_directions)
{
  int ret = OB_SUCCESS;
  // gather all group by columns
  const ObSelectStmt *stmt = get_stmt();
  reduce_exprs.reuse();
  if (OB_ISNULL(stmt) || OB_ISNULL(top)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(stmt), K(top));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_group_expr_size(); ++i) {
      ObRawExpr *group_expr = stmt->get_group_exprs().at(i);
      bool is_const = false;
      if (OB_ISNULL(group_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("group expr is NULL", K(ret), K(stmt->get_group_exprs()));
      } else if (OB_FAIL(ObOptimizerUtil::is_const_expr(group_expr,
                                                        top->get_output_equal_sets(),
                                                        top->get_output_const_exprs(),
                                                        get_onetime_query_refs(),
                                                        is_const))) {
        LOG_WARN("check is const expr failed", K(ret));
      } else if (is_const) {
        //no need to group a const expr, skip it
      } else if (OB_FAIL(reduce_exprs.push_back(group_expr))) {
        LOG_WARN("failed to push array", K(ret));
      } else { /*do nothing*/ }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObOptimizerUtil::simplify_exprs(top->get_fd_item_set(),
                                                  top->get_output_equal_sets(),
                                                  top->get_output_const_exprs(),
                                                  reduce_exprs,
                                                  group_by_exprs))) {
        LOG_WARN("failed to simplify group exprs", K(ret));
      } else if (OB_FAIL(ObOptimizerUtil::find_stmt_expr_direction(*stmt,
                                                                   group_by_exprs,
                                                                   top->get_output_equal_sets(),
                                                                   group_directions))) {
      } else if (OB_FAIL(rollup_exprs.assign(stmt->get_rollup_exprs()))) {
        LOG_WARN("failed to assign to rollup exprs.", K(ret));
      } else if (rollup_exprs.count() > 0) {
        bool has_rollup_dir = stmt->has_rollup_dir();
        if (OB_UNLIKELY(has_rollup_dir && (stmt->get_rollup_dir_size() != rollup_exprs.count()))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to check rollup exprs and directions count.", K (ret));
        } else {/* do nothing. */}
        for (int64_t i = 0; OB_SUCC(ret) && i < rollup_exprs.count(); i++) {
          ObOrderDirection dir = has_rollup_dir ? stmt->get_rollup_dirs().at(i) : default_asc_direction();
          if (OB_FAIL(rollup_directions.push_back(dir))) {
            LOG_WARN("failed to push back into directions.", K(ret));
          } else { /* do nothing. */ }
        }
      } // do nothing
    }
    if (OB_SUCC(ret)) {
      LOG_TRACE("succeed to get group by exprs and rollup exprs", K(reduce_exprs), K(group_by_exprs), K(rollup_exprs));
    }
  }
  return ret;
}

int ObSelectLogPlan::candi_allocate_normal_group_by(const ObIArray<ObRawExpr*> &reduce_exprs,
                                                    const ObIArray<ObRawExpr*> &group_by_exprs,
                                                    const ObIArray<ObOrderDirection> &group_directions,
                                                    const ObIArray<ObRawExpr*> &rollup_exprs,
                                                    const ObIArray<ObOrderDirection> &rollup_directions,
                                                    const ObIArray<ObRawExpr*> &having_exprs,
                                                    const ObIArray<ObAggFunRawExpr*> &aggr_items,
                                                    const bool is_from_povit)
{
  int ret = OB_SUCCESS;
  SMART_VAR(GroupingOpHelper, groupby_helper) {
    ObSEArray<CandidatePlan, 4> groupby_plans;
    if (OB_FAIL(init_groupby_helper(group_by_exprs,
                                    rollup_exprs,
                                    aggr_items,
                                    is_from_povit,
                                    groupby_helper))) {
      LOG_WARN("failed to init group by helper", K(ret));
    } else if (groupby_helper.can_three_stage_pushdown_) {
      OPT_TRACE("generate three stage group by");
      if (OB_FAIL(candi_allocate_three_stage_group_by(reduce_exprs,
                                                      group_by_exprs,
                                                      group_directions,
                                                      rollup_exprs,
                                                      rollup_directions,
                                                      aggr_items,
                                                      having_exprs,
                                                      is_from_povit,
                                                      groupby_helper,
                                                      groupby_plans))) {
        LOG_WARN("failed to candi allocate three stage group by", K(ret));
      }
    } else if (OB_FAIL(candi_allocate_normal_group_by(reduce_exprs,
                                                      group_by_exprs,
                                                      group_directions,
                                                      rollup_exprs,
                                                      rollup_directions,
                                                      having_exprs,
                                                      aggr_items,
                                                      is_from_povit,
                                                      groupby_helper,
                                                      false,
                                                      groupby_plans))) {
      LOG_WARN("failed to inner allocate normal group by", K(ret));
    } else if (!groupby_plans.empty()) {
      LOG_TRACE("succeed to allocate group by using hint", K(groupby_plans.count()), K(groupby_helper));
      OPT_TRACE("success to generate group plan with hint");
    } else if (OB_FAIL(get_log_plan_hint().check_status())) {
      LOG_WARN("failed to generate plans with hint", K(ret));
    } else if (OB_FAIL(candi_allocate_normal_group_by(reduce_exprs,
                                                      group_by_exprs,
                                                      group_directions,
                                                      rollup_exprs,
                                                      rollup_directions,
                                                      having_exprs,
                                                      aggr_items,
                                                      is_from_povit,
                                                      groupby_helper,
                                                      true,
                                                      groupby_plans))) {
      LOG_WARN("failed to inner allocate normal group by", K(ret));
    } else {
      LOG_TRACE("succeed to allocate group by ignore hint", K(groupby_plans.count()), K(groupby_helper));
      OPT_TRACE("success to generate group plan without hint");
    }

    //add plans to candidates
    if (OB_SUCC(ret)) {
      int64_t check_scope = OrderingCheckScope::CHECK_WINFUNC |
                            OrderingCheckScope::CHECK_DISTINCT |
                            OrderingCheckScope::CHECK_SET |
                            OrderingCheckScope::CHECK_ORDERBY;
      if (OB_FAIL(update_plans_interesting_order_info(groupby_plans, check_scope))) {
        LOG_WARN("failed to update plans interesting order info", K(ret));
      } else if (OB_FAIL(prune_and_keep_best_plans(groupby_plans))) {
        LOG_WARN("failed to add plan", K(ret));
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

// create three-stage push down plan
int ObSelectLogPlan::candi_allocate_three_stage_group_by(const ObIArray<ObRawExpr*> &reduce_exprs,
                                                         const ObIArray<ObRawExpr*> &group_by_exprs,
                                                         const ObIArray<ObOrderDirection> &group_directions,
                                                         const ObIArray<ObRawExpr*> &rollup_exprs,
                                                         const ObIArray<ObOrderDirection> &rollup_directions,
                                                         const ObIArray<ObAggFunRawExpr*> &aggr_items,
                                                         const ObIArray<ObRawExpr*> &having_exprs,
                                                         const bool is_from_povit,
                                                         GroupingOpHelper &groupby_helper,
                                                         ObIArray<CandidatePlan> &groupby_plans)
{
  int ret = OB_SUCCESS;
  ObSEArray<CandidatePlan, 16> best_plans;
  bool is_partition_wise = false;
  if (OB_FAIL(get_minimal_cost_candidates(candidates_.candidate_plans_, best_plans))) {
    LOG_WARN("failed to get minimal cost candidate", K(ret));
  } else {
    CandidatePlan candidate_plan;
    for (int64_t i = 0; OB_SUCC(ret) && i < best_plans.count(); i++) {
      candidate_plan = best_plans.at(i);
      if (OB_ISNULL(candidate_plan.plan_tree_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (candidate_plan.plan_tree_->is_distributed() && !reduce_exprs.empty() &&
          OB_FAIL(candidate_plan.plan_tree_->check_sharding_compatible_with_reduce_expr(reduce_exprs,
                                                                  is_partition_wise))) {
        LOG_WARN("failed to check if sharding compatible with distinct expr", K(ret));
      } else if (!candidate_plan.plan_tree_->is_distributed() || is_partition_wise) {
        bool part_sort_valid = !groupby_helper.force_normal_sort_ && !group_by_exprs.empty();
        bool normal_sort_valid = !groupby_helper.force_part_sort_;
        bool can_ignore_merge_plan = !(groupby_plans.empty() || groupby_helper.force_use_merge_);
        if (OB_FAIL(update_part_sort_method(part_sort_valid, normal_sort_valid))) {
          LOG_WARN("fail to update part sort method", K(ret));
        } else if (OB_FAIL(create_merge_group_plan(reduce_exprs,
                                                  group_by_exprs,
                                                  group_directions,
                                                  rollup_exprs,
                                                  rollup_directions,
                                                  aggr_items,
                                                  having_exprs,
                                                  is_from_povit,
                                                  groupby_helper,
                                                  candidate_plan,
                                                  groupby_plans,
                                                  part_sort_valid,
                                                  normal_sort_valid,
                                                  can_ignore_merge_plan))) {
          LOG_WARN("failed to create merge group by plan", K(ret));
        }
      } else {
        if (NULL == groupby_helper.aggr_code_expr_ &&
                  OB_FAIL(prepare_three_stage_info(group_by_exprs, rollup_exprs, groupby_helper))) {
          LOG_WARN("failed to prepare three stage info", K(ret));
        } else if (OB_FAIL(create_three_stage_group_plan(group_by_exprs,
                                                          rollup_exprs,
                                                          having_exprs,
                                                          groupby_helper,
                                                          candidate_plan.plan_tree_))) {
          LOG_WARN("failed to create hash group by plan", K(ret));
        } else if (OB_FAIL(groupby_plans.push_back(candidate_plan))) {
          LOG_WARN("failed to push merge group by", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObSelectLogPlan::get_valid_aggr_algo(const ObIArray<ObRawExpr*> &group_by_exprs,
                                         const GroupingOpHelper &groupby_helper,
                                         const bool ignore_hint,
                                         bool &use_hash_valid,
                                         bool &use_merge_valid,
                                         bool &part_sort_valid,
                                         bool &normal_sort_valid)
{
  int ret = OB_SUCCESS;
  if (ignore_hint) {
    use_hash_valid = true;
    use_merge_valid = true;
    part_sort_valid = true;
    normal_sort_valid = true;
  } else {
    use_hash_valid = !groupby_helper.force_use_merge_;
    use_merge_valid = !groupby_helper.force_use_hash_;
    part_sort_valid = !groupby_helper.force_normal_sort_;
    normal_sort_valid = !groupby_helper.force_part_sort_;
  }
  if (OB_ISNULL(get_stmt()) || OB_ISNULL(optimizer_context_.get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(get_stmt()), K(optimizer_context_.get_query_ctx()), K(ret));
  } else if (get_stmt()->has_rollup()
             || group_by_exprs.empty()
             || get_stmt()->has_distinct_or_concat_agg()) {
    //group_concat and distinct aggregation hold all input rows temporary,
    //too much memory consumption for hash aggregate.
    use_hash_valid = false;
  }

  if (OB_FAIL(ret)) {
  } else if (!use_merge_valid) {
    part_sort_valid = false;
    normal_sort_valid = false;
  } else if (group_by_exprs.empty()) {
    part_sort_valid = false;
    /* if normal_sort_valid set as false by hint, will retry by ignore hint */
  } else if (OB_FAIL(update_part_sort_method(part_sort_valid, normal_sort_valid))) {
    LOG_WARN("fail to update part sort method", K(ret));
  }
  return ret;
}

//  update partition sort method with opt_param hint
int ObSelectLogPlan::update_part_sort_method(bool &part_sort_valid,
                                             bool &normal_sort_valid)
{
  int ret = OB_SUCCESS;
  if (!part_sort_valid || !normal_sort_valid) {
    /* has sort method in no_use_hash_aggregation hint, ignore opt_param hint */
  } else {
    bool use_part_sort = false;
    bool is_exists_opt = false;
    if (OB_FAIL(optimizer_context_.get_query_ctx()->get_global_hint()
                        .opt_params_.get_bool_opt_param(
                        ObOptParamHint::USE_PART_SORT_MGB, use_part_sort, is_exists_opt))) {
      LOG_WARN("fail to check partition sort merge group by enabled", K(ret));
    } else if (!is_exists_opt) {
      /* has no opt_param hint */
    } else {
      part_sort_valid = use_part_sort;
      normal_sort_valid = !use_part_sort;
    }
  }
  return ret;
}

int ObSelectLogPlan::candi_allocate_normal_group_by(const ObIArray<ObRawExpr*> &reduce_exprs,
                                                    const ObIArray<ObRawExpr*> &group_by_exprs,
                                                    const ObIArray<ObOrderDirection> &group_directions,
                                                    const ObIArray<ObRawExpr*> &rollup_exprs,
                                                    const ObIArray<ObOrderDirection> &rollup_directions,
                                                    const ObIArray<ObRawExpr*> &having_exprs,
                                                    const ObIArray<ObAggFunRawExpr*> &aggr_items,
                                                    const bool is_from_povit,
                                                    GroupingOpHelper &groupby_helper,
                                                    const bool ignore_hint,
                                                    ObIArray<CandidatePlan> &groupby_plans)
{
  int ret = OB_SUCCESS;
  CandidatePlan candidate_plan;
  bool use_hash_valid = false;
  bool use_merge_valid = false;
  bool part_sort_valid = false;
  bool normal_sort_valid = false;
  if (OB_FAIL(get_valid_aggr_algo(group_by_exprs, groupby_helper, ignore_hint,
                                  use_hash_valid, use_merge_valid,
                                  part_sort_valid, normal_sort_valid))) {
    LOG_WARN("failed to get valid aggr algo", K(ret));
  }
  // create hash group by plans
  if (OB_SUCC(ret) && use_hash_valid) {
    ObSEArray<CandidatePlan, 16> best_plans;
    if (OB_FAIL(get_minimal_cost_candidates(candidates_.candidate_plans_, best_plans))) {
      LOG_WARN("failed to get minimal cost candidate", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < best_plans.count(); i++) {
        candidate_plan = best_plans.at(i);
        OPT_TRACE("generate hash group by for plan:", candidate_plan);
        if (OB_FAIL(create_hash_group_plan(reduce_exprs,
                                            group_by_exprs,
                                            rollup_exprs,
                                            aggr_items,
                                            having_exprs,
                                            is_from_povit,
                                            groupby_helper,
                                            candidate_plan.plan_tree_))) {
          LOG_WARN("failed to create hash group by plan", K(ret));
        } else if (NULL != candidate_plan.plan_tree_ &&
                    OB_FAIL(groupby_plans.push_back(candidate_plan))) {
          LOG_WARN("failed to push merge group by", K(ret));
        } else { /*do nothing*/ }
      }
    }
  }
  // create merge group by plans
  if (OB_SUCC(ret) && use_merge_valid) {
    bool can_ignore_merge_plan = !groupby_plans.empty();
    for (int64_t i = 0; OB_SUCC(ret) && i < candidates_.candidate_plans_.count(); i++) {
      candidate_plan = candidates_.candidate_plans_.at(i);
      bool is_needed = false;
      OPT_TRACE("generate merge group by for plan:", candidate_plan);
      if (OB_FAIL(should_create_rollup_pushdown_plan(candidate_plan.plan_tree_,
                                                      reduce_exprs,
                                                      rollup_exprs,
                                                      groupby_helper,
                                                      is_needed))) {
        LOG_WARN("failed to check should create rollup pushdown plan", K(ret));
      } else if (is_needed) {
        if (OB_FAIL(create_rollup_pushdown_plan(group_by_exprs,
                                                rollup_exprs,
                                                aggr_items,
                                                having_exprs,
                                                groupby_helper,
                                                candidate_plan.plan_tree_))) {
          LOG_WARN("failed to create rollup pushdown plan", K(ret));
        } else if (NULL != candidate_plan.plan_tree_ &&
                   OB_FAIL(groupby_plans.push_back(candidate_plan))) {
          LOG_WARN("failed to push merge group by", K(ret));
        }
      } else if (OB_FAIL(create_merge_group_plan(reduce_exprs,
                                                 group_by_exprs,
                                                 group_directions,
                                                 rollup_exprs,
                                                 rollup_directions,
                                                 aggr_items,
                                                 having_exprs,
                                                 is_from_povit,
                                                 groupby_helper,
                                                 candidate_plan,
                                                 groupby_plans,
                                                 part_sort_valid,
                                                 normal_sort_valid,
                                                 can_ignore_merge_plan))) {
        LOG_WARN("failed to create merge group by plan", K(ret));
      }
    }
  }
  return ret;
}

int ObSelectLogPlan::should_create_rollup_pushdown_plan(ObLogicalOperator *top,
                                                        const ObIArray<ObRawExpr *> &reduce_exprs,
                                                        const ObIArray<ObRawExpr *> &rollup_exprs,
                                                        GroupingOpHelper &groupby_helper,
                                                        bool &is_needed)
{
  int ret = OB_SUCCESS;
  bool is_partition_wise = false;
  is_needed = false;
  ObSQLSessionInfo *session = get_optimizer_context().get_session_info();
  if (OB_ISNULL(top) || OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("logical operator is null", K(ret), K(top), K(session));
  } else if (rollup_exprs.empty() || !groupby_helper.can_rollup_pushdown_) {
    // do nothing
  } else if (top->is_distributed() &&
             OB_FAIL(top->check_sharding_compatible_with_reduce_expr(reduce_exprs,
                                                                     is_partition_wise))) {
    LOG_WARN("failed to check is partition wise", K(ret));
  } else if (!top->is_distributed() || is_partition_wise ) {
    // do nothing
  } else if (NULL == groupby_helper.rollup_id_expr_ &&
             OB_FAIL(ObRawExprUtils::build_pseudo_rollup_id(get_optimizer_context().get_expr_factory(),
                                                            *session,
                                                            groupby_helper.rollup_id_expr_))) {
    LOG_WARN("failed to build rollup id expr", K(ret));
  } else {
    is_needed = true;
  }
  return ret;
}


int ObSelectLogPlan::create_rollup_pushdown_plan(const ObIArray<ObRawExpr*> &group_by_exprs,
                                                 const ObIArray<ObRawExpr*> &rollup_exprs,
                                                 const ObIArray<ObAggFunRawExpr*> &aggr_items,
                                                 const ObIArray<ObRawExpr*> &having_exprs,
                                                 GroupingOpHelper &groupby_helper,
                                                 ObLogicalOperator *&top)
{
  int ret = OB_SUCCESS;
  // 1. allocate pre group by
  ObSEArray<ObRawExpr *, 4> pre_group_exprs;
  ObSEArray<ObRawExpr *, 4> rc_group_exprs; // rollup collector
  ObSEArray<ObRawExpr *, 4> exch_keys;
  ObSEArray<OrderItem, 4> rd_sort_keys;
  ObSEArray<OrderItem, 4> rd_ecd_sort_keys;
  OrderItem encode_sort_key;
  bool enable_encode_sort = false;
  ObSEArray<OrderItem, 4> sort_keys;
  ObArray<ObRawExpr *> dummy;
  ObExchangeInfo exch_info;
  ObLogGroupBy *rollup_distributor = NULL;
  ObLogGroupBy *rollup_collector = NULL;
  bool can_sort_opt = true;
  if (OB_ISNULL(top)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(append(pre_group_exprs, group_by_exprs)) ||
      OB_FAIL(append(pre_group_exprs, rollup_exprs))) {
    LOG_WARN("failed to append pre group exprs", K(ret));
  } else if (OB_FAIL(allocate_group_by_as_top(top,
                                              AggregateAlgo::HASH_AGGREGATE,
                                              pre_group_exprs,
                                              dummy,
                                              aggr_items,
                                              dummy,
                                              false,
                                              groupby_helper.group_ndv_,
                                              top->get_card(),
                                              false,
                                              true,
                                              false))) {
    LOG_WARN("failed to allocate pre group by operator", K(ret));
  } else if (OB_FAIL(allocate_group_by_as_top(top,
                                              AggregateAlgo::MERGE_AGGREGATE,
                                              group_by_exprs,
                                              rollup_exprs,
                                              aggr_items,
                                              dummy,
                                              false,
                                              groupby_helper.group_ndv_,
                                              top->get_card(),
                                              false,
                                              true,
                                              false,
                                              ObRollupStatus::ROLLUP_DISTRIBUTOR))) {
    LOG_WARN("failed to allocate rollup distributor", K(ret));
  } else if (OB_ISNULL(rollup_distributor = dynamic_cast<ObLogGroupBy *>(top))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rollup distributor is null", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::make_sort_keys(pre_group_exprs,
                                                    default_asc_direction(),
                                                    rd_sort_keys))) {
    LOG_WARN("failed to make sort keys", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::check_can_encode_sortkey(rd_sort_keys,
                                                                can_sort_opt, *this, top->get_card()))) {
    LOG_WARN("failed to check encode sortkey expr", K(ret));
  } else if (can_sort_opt
      && (OB_FAIL(ObSQLUtils::create_encode_sortkey_expr(get_optimizer_context().get_expr_factory(),
                                                              get_optimizer_context().get_exec_ctx(),
                                                              rd_sort_keys,
                                                              0,
                                                              encode_sort_key)
      || FALSE_IT(enable_encode_sort = true)
      || OB_FAIL(rd_ecd_sort_keys.push_back(encode_sort_key))))) {
    LOG_WARN("failed to create encode sortkey expr", K(ret));
  } else if (OB_FAIL(rollup_distributor->set_rollup_info(ObRollupStatus::ROLLUP_DISTRIBUTOR,
                                                         groupby_helper.rollup_id_expr_,
                                                         rd_sort_keys,
                                                         rd_ecd_sort_keys,
                                                         enable_encode_sort))) {
    LOG_WARN("failed to set rollup distributor info", K(ret));
  } else if (OB_FAIL(append(exch_keys, group_by_exprs)) ||
             OB_FAIL(exch_keys.push_back(groupby_helper.rollup_id_expr_)) ||
             OB_FAIL(append(exch_keys, rollup_exprs))) {
    LOG_WARN("failed to append exchange keys", K(ret));
  } else if (OB_FAIL(get_grouping_style_exchange_info(exch_keys,
                                                      top->get_output_equal_sets(),
                                                      exch_info))) {
    LOG_WARN("failed to get rollup exchange info", K(ret));
  } else if (FALSE_IT(exch_info.is_rollup_hybrid_ = true)) {
    // do nothing
  } else if (OB_FAIL(ObOptimizerUtil::make_sort_keys(exch_keys, default_asc_direction(), sort_keys))) {
    LOG_WARN("failed to generate sort keys", K(ret));
  } else if (OB_FAIL(allocate_sort_and_exchange_as_top(top,
                                                       exch_info,
                                                       sort_keys,
                                                       true,
                                                       0,
                                                       false))) {
    LOG_WARN("failed to allocate sort and exchange as top", K(ret));
  } else if (OB_FAIL(append(rc_group_exprs, group_by_exprs)) ||
             OB_FAIL(rc_group_exprs.push_back(groupby_helper.rollup_id_expr_))) {
    LOG_WARN("failed to create rollup collector group by keys", K(ret));
  } else if (OB_FAIL(allocate_group_by_as_top(top,
                                              AggregateAlgo::MERGE_AGGREGATE,
                                              rc_group_exprs,
                                              rollup_exprs,
                                              aggr_items,
                                              having_exprs,
                                              false,
                                              groupby_helper.group_ndv_,
                                              top->get_card(),
                                              false,
                                              false,
                                              false,
                                              ObRollupStatus::ROLLUP_COLLECTOR))) {
    LOG_WARN("failed to allocate rollup collector", K(ret));
  } else if (OB_ISNULL(rollup_collector = dynamic_cast<ObLogGroupBy *>(top))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rollup collector is null", K(ret));
  } else if (rollup_collector->set_rollup_info(ObRollupStatus::ROLLUP_COLLECTOR,
                                                        groupby_helper.rollup_id_expr_)) {
    LOG_WARN("failed to set rollup id expr", K(ret));
  } else {
    rollup_collector->set_group_by_outline_info(false, true);
  }
  return ret;
}

int ObSelectLogPlan::create_hash_group_plan(const ObIArray<ObRawExpr*> &reduce_exprs,
            const ObIArray<ObRawExpr*> &group_by_exprs,
				    const ObIArray<ObRawExpr*> &rollup_exprs,
				    const ObIArray<ObAggFunRawExpr*> &aggr_items,
				    const ObIArray<ObRawExpr*> &having_exprs,
				    const bool is_from_povit,
				    GroupingOpHelper &groupby_helper,
				    ObLogicalOperator *&top)
{
  int ret = OB_SUCCESS;
  bool is_partition_wise = false;
  double origin_child_card = 0.0;
  if (OB_ISNULL(top)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(top), K(get_stmt()), K(ret));
  } else if (OB_FALSE_IT(origin_child_card = top->get_card())) {
  } else if (top->is_distributed() &&
             OB_FAIL(top->check_sharding_compatible_with_reduce_expr(reduce_exprs,
                                                                     is_partition_wise))) {
    LOG_WARN("failed to check if sharding compatible", K(ret));
  } else if (!top->is_distributed() || is_partition_wise) {
    if (OB_FAIL(allocate_group_by_as_top(top,
                                         AggregateAlgo::HASH_AGGREGATE,
                                         group_by_exprs,
                                         rollup_exprs,
                                         aggr_items,
                                         having_exprs,
                                         is_from_povit,
                                         groupby_helper.group_ndv_,
                                         origin_child_card,
                                         is_partition_wise))) {
      LOG_WARN("failed to allocate group by as top", K(ret));
    }
    static_cast<ObLogGroupBy*>(top)->set_group_by_outline_info(true, false);
  } else {
    // allocate push down group by
    if (groupby_helper.can_basic_pushdown_) {
      /*
      * create table t1(a int, b int, c int) partition by hash(a) partitions 4;
      * select sum(a) from t1 group by c;
      * push group by and aggregation
      */
      ObSEArray<ObRawExpr*, 1> dummy_exprs;
      ObSEArray<ObRawExpr*, 8> group_rollup_exprs;
      if (OB_FAIL(append(group_rollup_exprs, group_by_exprs)) ||
          OB_FAIL(append(group_rollup_exprs, rollup_exprs))) {
        LOG_WARN("failed to append exprs", K(ret));
      } else if (OB_FAIL(allocate_group_by_as_top(top,
                                                  AggregateAlgo::HASH_AGGREGATE,
                                                  group_rollup_exprs,
                                                  dummy_exprs,
                                                  aggr_items,
                                                  dummy_exprs,
                                                  is_from_povit,
                                                  groupby_helper.group_ndv_,
                                                  origin_child_card,
                                                  is_partition_wise,
                                                  true,
                                                  true))) {
        LOG_WARN("failed to allocate group by as top", K(ret));
      } else if (OB_FAIL(allocate_topk_for_hash_group_plan(top))) {  // allocate top-k
        LOG_WARN("failed to allocate topk for hash group by plan", K(ret));
      } else { /*do nothing*/ }
    }

    // allocate exchange
    if (OB_SUCC(ret)) {
      ObExchangeInfo exch_info;
      if (OB_FAIL(get_grouping_style_exchange_info(group_by_exprs,
                                                  top->get_output_equal_sets(),
                                                  exch_info))) {
        LOG_WARN("failed to get grouping style exchange info", K(ret));
      } else if (OB_FAIL(allocate_exchange_as_top(top, exch_info))) {
        LOG_WARN("failed to allocate exchange as top", K(ret));
      } else { /*do nothing*/ }
    }
    // allocate final group by
    if (OB_SUCC(ret)) {
      if (OB_FAIL(allocate_group_by_as_top(top,
                                          AggregateAlgo::HASH_AGGREGATE,
                                          group_by_exprs,
                                          rollup_exprs,
                                          aggr_items,
                                          having_exprs,
                                          is_from_povit,
                                          groupby_helper.group_ndv_,
                                          origin_child_card))) {
      LOG_WARN("failed to allocate scala group by as top", K(ret));
      } else {
        static_cast<ObLogGroupBy*>(top)->set_group_by_outline_info(true, groupby_helper.can_basic_pushdown_);
      }
    }
  }
  return ret;
}

int ObSelectLogPlan::allocate_topk_for_hash_group_plan(ObLogicalOperator *&top)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 16> order_by_exprs;
  ObSEArray<ObOrderDirection, 16> directions;
  const ObSelectStmt *select_stmt = NULL;
  const ObGlobalHint &global_hint = get_optimizer_context().get_global_hint();
  if (OB_ISNULL(top) || OB_ISNULL(select_stmt = get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(top), K(select_stmt), K(ret));
  } else if (!select_stmt->is_match_topk()) {
    /*do nothing*/
  } else if (OB_FAIL(get_order_by_exprs(top, order_by_exprs, &directions))) {
    LOG_WARN("failed to get order by exprs", K(ret));
  } else if (order_by_exprs.empty()) {
    // no need order by, directly allocate topk
    if (OB_FAIL(allocate_topk_as_top(top,
                                    select_stmt->get_limit_expr(),
                                    select_stmt->get_offset_expr(),
                                    global_hint.sharding_minimum_row_count_,
                                    global_hint.topk_precision_))) {
      LOG_WARN("failed to allocate topk as top", K(ret));
    } else { /*do nothing*/ }
  } else {
    // allocate sort
    ObSEArray<OrderItem, 8> sort_keys;
    ObSEArray<OrderItem, 8> topk_sort_keys;
    if (OB_FAIL(make_order_items(order_by_exprs, directions, sort_keys))) {
    LOG_WARN("failed to make order items", K(ret));
    } else if (OB_FAIL(clone_sort_keys_for_topk(sort_keys,
              topk_sort_keys))) {
    LOG_WARN("failed to clone sort keys for topk", K(ret));
    } else if (OB_FAIL(allocate_topk_sort_as_top(top,
                                                topk_sort_keys,
                                                select_stmt->get_limit_expr(),
                                                select_stmt->get_offset_expr(),
                                                global_hint.sharding_minimum_row_count_,
                                                global_hint.topk_precision_))) {
    LOG_WARN("failed to allocate topk sort as top", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObSelectLogPlan::allocate_topk_sort_as_top(ObLogicalOperator *&top,
                                              const ObIArray<OrderItem> &sort_keys,
                                              ObRawExpr *limit_expr,
                                              ObRawExpr *offset_expr,
                                              int64_t minimum_row_count,
                                              int64_t topk_precision)
{
  int ret = OB_SUCCESS;
  ObLogSort *sort = NULL;
  if (OB_ISNULL(top) || OB_ISNULL(limit_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(top), K(limit_expr), K(ret));
  } else if (OB_ISNULL(sort = static_cast<ObLogSort*>(get_log_op_factory().allocate(*this, LOG_SORT)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to allocate sort for order by", K(ret));
  } else {
    sort->set_child(ObLogicalOperator::first_child, top);
    sort->set_topk_limit_expr(limit_expr);
    sort->set_topk_offset_expr(offset_expr);
    sort->set_minimal_row_count(minimum_row_count);
    sort->set_topk_precision(topk_precision);
    if (OB_FAIL(sort->set_sort_keys(sort_keys))) {
      LOG_WARN("failed to set sort keys", K(ret));
    } else if (OB_FAIL(sort->compute_property())) {
      LOG_WARN("failed to compute property", K(ret));
    } else {
      top = sort;
    }
  }
  return ret;
}

int ObSelectLogPlan::clone_sort_keys_for_topk(const ObIArray<OrderItem> &sort_keys,
				                                      ObIArray<OrderItem> &topk_sort_keys)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < sort_keys.count(); ++i) {
    ObRawExpr *new_sort_expr = NULL;
    if (OB_FAIL(ObOptimizerUtil::clone_expr_for_topk(get_optimizer_context().get_expr_factory(),
                                                    sort_keys.at(i).expr_,
                                                    new_sort_expr))) {
      LOG_WARN("failed to copy expr", K(ret));
    } else if (OB_ISNULL(new_sort_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(new_sort_expr), K(ret));
    } else {
      OrderItem new_order_item;
      new_order_item.expr_ = new_sort_expr;
      new_order_item.order_type_ = sort_keys.at(i).order_type_;
      if (OB_FAIL(topk_sort_keys.push_back(new_order_item))) {
        LOG_WARN("failed to push back order_item", K(ret));
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

int ObSelectLogPlan::allocate_topk_as_top(ObLogicalOperator *&top,
                                          ObRawExpr *topk_limit_count,
                                          ObRawExpr *topk_limit_offset,
                                          int64_t minimum_row_count,
                                          int64_t topk_precision)
{
  int ret = OB_SUCCESS;
  ObLogTopk *topk_op = NULL;
  if (OB_ISNULL(top)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_ISNULL(topk_op = static_cast<ObLogTopk*>(log_op_factory_.allocate(
        *this, log_op_def::LOG_TOPK)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate topk operator", K(ret));
  } else {
    topk_op->set_child(ObLogicalOperator::first_child, top);
    topk_op->set_topk_params(topk_limit_count, topk_limit_offset,
                            minimum_row_count, topk_precision);
    if (OB_FAIL(topk_op->compute_property())) {
      LOG_WARN("failed to compute property", K(ret));
    } else {
      top = topk_op;
    }
  }
  return ret;
}

int ObSelectLogPlan::create_merge_group_plan(const ObIArray<ObRawExpr*> &reduce_exprs,
                                             const ObIArray<ObRawExpr*> &group_by_exprs,
                                             const ObIArray<ObOrderDirection> &group_directions,
                                             const ObIArray<ObRawExpr*> &rollup_exprs,
                                             const ObIArray<ObOrderDirection> &rollup_directions,
                                             const ObIArray<ObAggFunRawExpr*> &aggr_items,
                                             const ObIArray<ObRawExpr*> &having_exprs,
                                             const bool is_from_povit,
                                             GroupingOpHelper &groupby_helper,
                                             CandidatePlan &candidate_plan,
                                             ObIArray<CandidatePlan> &candidate_plans,
                                             bool part_sort_valid,
                                             bool normal_sort_valid,
                                             bool can_ignore_merge_plan)
{
  int ret = OB_SUCCESS;
  CandidatePlan part_sort_mgb_plan = candidate_plan;
  bool can_ignore_no_part_sort_plan = can_ignore_merge_plan;
  if (part_sort_valid && OB_FAIL(inner_create_merge_group_plan(reduce_exprs,
                                                              group_by_exprs,
                                                              group_directions,
                                                              rollup_exprs,
                                                              rollup_directions,
                                                              aggr_items,
                                                              having_exprs,
                                                              is_from_povit,
                                                              groupby_helper,
                                                              part_sort_mgb_plan.plan_tree_,
                                                              true,
                                                              can_ignore_merge_plan))) {
        LOG_WARN("failed to create partition sort merge group by plan", K(ret));
  } else if (part_sort_valid && NULL != part_sort_mgb_plan.plan_tree_
             && OB_FAIL(candidate_plans.push_back(part_sort_mgb_plan))) {
    LOG_WARN("failed to push merge group by", K(ret));
  } else if (OB_FALSE_IT(can_ignore_no_part_sort_plan |= part_sort_valid && NULL != part_sort_mgb_plan.plan_tree_)) {
  } else if (normal_sort_valid && OB_FAIL(inner_create_merge_group_plan(reduce_exprs,
                                                                        group_by_exprs,
                                                                        group_directions,
                                                                        rollup_exprs,
                                                                        rollup_directions,
                                                                        aggr_items,
                                                                        having_exprs,
                                                                        is_from_povit,
                                                                        groupby_helper,
                                                                        candidate_plan.plan_tree_,
                                                                        false,
                                                                        can_ignore_no_part_sort_plan))) {
    LOG_WARN("failed to create merge group by plan", K(ret));
  } else if (normal_sort_valid && NULL != candidate_plan.plan_tree_ &&
             OB_FAIL(candidate_plans.push_back(candidate_plan))) {
    LOG_WARN("failed to push merge group by", K(ret));
  }
  return ret;
}

int ObSelectLogPlan::inner_create_merge_group_plan(const ObIArray<ObRawExpr*> &reduce_exprs,
                                                   const ObIArray<ObRawExpr*> &group_by_exprs,
                                                   const ObIArray<ObOrderDirection> &group_directions,
                                                   const ObIArray<ObRawExpr*> &rollup_exprs,
                                                   const ObIArray<ObOrderDirection> &rollup_directions,
                                                   const ObIArray<ObAggFunRawExpr*> &aggr_items,
                                                   const ObIArray<ObRawExpr*> &having_exprs,
                                                   const bool is_from_povit,
                                                   GroupingOpHelper &groupby_helper,
                                                   ObLogicalOperator *&top,
                                                   bool use_part_sort,
                                                   bool can_ignore_merge_plan)
{
  int ret = OB_SUCCESS;
  int64_t prefix_pos = 0;
  int64_t push_prefix_pos = 0;
  bool need_sort = false;
  bool push_need_sort = false;
  bool is_partition_wise = false;
  bool is_fetch_with_ties = false;
  int64_t part_cnt = use_part_sort ? group_by_exprs.count() : 0;
  OrderItem hash_sortkey;
  ObSEArray<ObRawExpr*, 1> dummy_exprs;
  ObSEArray<ObAggFunRawExpr*, 1> dummy_aggrs;
  ObSEArray<OrderItem, 4> sort_keys;
  ObSEArray<OrderItem, 4> push_sort_keys;
  ObSEArray<ObRawExpr*, 4> sort_exprs;
  ObSEArray<ObOrderDirection, 4> sort_directions;
  ObSEArray<ObRawExpr*, 4> adjusted_group_by_exprs;
  ObSEArray<ObOrderDirection, 4> adjusted_group_directions;
  int64_t interesting_order_info = OrderingFlag::NOT_MATCH;
  double origin_child_card = 0.0;
  if (OB_ISNULL(top) || OB_UNLIKELY(0 == part_cnt && use_part_sort)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected params", K(top), K(part_cnt), K(use_part_sort), K(ret));
  } else if (OB_FALSE_IT(origin_child_card = top->get_card())) {
  } else if (OB_FAIL(adjusted_group_by_exprs.assign(group_by_exprs)) ||
             OB_FAIL(adjusted_group_directions.assign(group_directions))) {
    LOG_WARN("failed to assign expr", K(ret));
  } else if (OB_FAIL(adjust_sort_expr_ordering(adjusted_group_by_exprs,
                                               adjusted_group_directions,
                                               *top,
                                               true))) {
    LOG_WARN("failed to get group expr ordering", K(ret));
  } else if (OB_FAIL(generate_merge_group_sort_keys(top,
                                                    adjusted_group_by_exprs,
                                                    adjusted_group_directions,
                                                    rollup_exprs,
                                                    rollup_directions,
                                                    sort_exprs,
                                                    sort_directions))) {
    LOG_WARN("failed to generate merge group sort keys", K(ret));                                                      
  } else if (OB_FAIL(make_order_items(sort_exprs,
                                      sort_directions,
                                      sort_keys))) {
    LOG_WARN("failed to make order items", K(ret));
  } else if (can_ignore_merge_plan && OB_FAIL(ObOptimizerUtil::compute_stmt_interesting_order(sort_keys,
                                                    get_stmt(),
                                                    false,
                                                    const_cast<EqualSets &>(top->get_output_equal_sets()),
                                                    top->get_output_const_exprs(),
                                                    get_is_parent_set_distinct(),
                                                    OrderingCheckScope::CHECK_ALL & (~OrderingCheckScope::CHECK_GROUP),
                                                    interesting_order_info))) {
    LOG_WARN("failed to compute stmt interesting order", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::check_need_sort(sort_exprs,
                                                      &sort_directions,
                                                      top->get_op_ordering(),
                                                      top->get_fd_item_set(),
                                                      top->get_output_equal_sets(),
                                                      top->get_output_const_exprs(),
                                                      get_onetime_query_refs(),
                                                      top->get_is_at_most_one_row(),
                                                      need_sort,
                                                      prefix_pos,
                                                      part_cnt))) {
    LOG_WARN("failed to check if need sort", K(ret));
  } else if ((!need_sort && use_part_sort) ||
             (need_sort && can_ignore_merge_plan &&
              OrderingFlag::NOT_MATCH == interesting_order_info)) {
    // 1. need generate partition sort plan, but need not sort
    // 2. if need sort and no further op needs the output order, not generate merge groupby
    top = NULL;
  } else if (top->is_distributed() &&
             OB_FAIL(top->check_sharding_compatible_with_reduce_expr(reduce_exprs,
                                                                     is_partition_wise))) {
    LOG_WARN("failed to check if sharding compatible with reduce expr", K(ret));
  } else if (!top->is_distributed() || is_partition_wise) {
    if (OB_FAIL(try_allocate_sort_as_top(top, sort_keys, need_sort, prefix_pos, part_cnt))) {
      LOG_WARN("failed to allocate sort as top", K(ret));
    } else if (OB_FAIL(allocate_group_by_as_top(top,
                                                MERGE_AGGREGATE,
                                                adjusted_group_by_exprs,
                                                rollup_exprs,
                                                aggr_items,
                                                having_exprs,
                                                is_from_povit,
                                                groupby_helper.group_ndv_,
                                                origin_child_card,
                                                is_partition_wise))) {
      LOG_WARN("failed to allocate group by as top", K(ret));
    } else {
      static_cast<ObLogGroupBy*>(top)->set_group_by_outline_info(false, false, use_part_sort);
    }
  } else if (use_part_sort &&
            OB_FAIL(create_hash_sortkey(part_cnt, sort_keys, hash_sortkey))) {
    LOG_WARN("failed to create hash sort key", K(ret), K(part_cnt), K(sort_keys));
  } else {
    // allocate push down group by
    if (groupby_helper.can_basic_pushdown_) {
      /*
      * create table t1(a int, b int, c int) partition by hash(a) partitions 4;
      * select sum(a) from t1 group by c;
      * push group by and aggregation
      */
     /* If the child has not allocated exchange,
        then the down-pressed group by operator must be full partition wise,
        and the gi operator can be allocated on the group by operator.
        At this time, the child's order is globally ordered.*/
      bool should_pullup_gi = false;
      bool top_is_local_order = false;
      bool is_partition_gi = false;
      if (OB_FAIL(check_can_pullup_gi(*top,
                                      is_partition_wise,
                                      need_sort || sort_exprs.empty(),
                                      should_pullup_gi))) {
        LOG_WARN("failed to check can pullup gi", K(ret));
      } else if (OB_FALSE_IT(is_partition_gi = top->is_partition_wise())) {
      } else if (OB_FALSE_IT(top_is_local_order = top->get_is_local_order() && !should_pullup_gi)) {
      } else if ((need_sort || top_is_local_order) && !sort_exprs.empty() &&
                 OB_FAIL(allocate_sort_as_top(top,
                                              sort_keys,
                                              top_is_local_order ? 0 : prefix_pos,
                                              !need_sort && top_is_local_order,
                                              nullptr,
                                              is_fetch_with_ties,
                                              use_part_sort ? &hash_sortkey : NULL))) {
        LOG_WARN("failed to allocate sort as top", K(ret));
      } else if (OB_FAIL(allocate_group_by_as_top(top,
                                                  MERGE_AGGREGATE,
                                                  sort_exprs,
                                                  dummy_exprs,
                                                  aggr_items,
                                                  dummy_exprs,
                                                  is_from_povit,
                                                  groupby_helper.group_ndv_,
                                                  origin_child_card,
                                                  should_pullup_gi,
                                                  true,
                                                  is_partition_gi))) {
        LOG_WARN("failed to allocate group by as top", K(ret));
      } else if (OB_FAIL(allocate_topk_for_merge_group_plan(top))) {// allocate top-k
        LOG_WARN("failed to allocate topk for merge group plan", K(ret));
      } else {
        need_sort = (need_sort && part_cnt > 0) ? true : false;
        prefix_pos = 0;
      }
    }

    // allocate final sort and group by
    if (OB_SUCC(ret)) {
      ObExchangeInfo exch_info;
      if (OB_FAIL(get_grouping_style_exchange_info(group_by_exprs,
                                                   top->get_output_equal_sets(),
                                                   exch_info))) {
        LOG_WARN("failed to get grouping style exchange info", K(ret));
      } else if (OB_FAIL(allocate_sort_and_exchange_as_top(top,
                                                           exch_info,
                                                           sort_keys,
                                                           need_sort,
                                                           prefix_pos,
                                                           top->get_is_local_order(),
                                                           nullptr,
                                                           is_fetch_with_ties,
                                                           use_part_sort ? &hash_sortkey : NULL))) {
        LOG_WARN("failed to allocate sort as top", K(ret));
      } else if (OB_FAIL(allocate_group_by_as_top(top,
                                                  MERGE_AGGREGATE,
                                                  adjusted_group_by_exprs,
                                                  rollup_exprs,
                                                  aggr_items,
                                                  having_exprs,
                                                  is_from_povit,
                                                  groupby_helper.group_ndv_,
                                                  origin_child_card))) {
        LOG_WARN("failed to allocate group by as top", K(ret));
      } else {
        static_cast<ObLogGroupBy*>(top)->set_group_by_outline_info(false,
                                         groupby_helper.can_basic_pushdown_, use_part_sort);
      }
    }
  }
  return ret;
}

int ObSelectLogPlan::generate_merge_group_sort_keys(ObLogicalOperator *top,
                                                    const ObIArray<ObRawExpr*> &group_by_exprs,
                                                    const ObIArray<ObOrderDirection> &group_directions,
                                                    const ObIArray<ObRawExpr*> &rollup_exprs,
                                                    const ObIArray<ObOrderDirection> &rollup_directions,
                                                    ObIArray<ObRawExpr*> &sort_exprs,
                                                    ObIArray<ObOrderDirection> &sort_directions)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(top)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_UNLIKELY(group_by_exprs.count() != group_directions.count()) ||
             OB_UNLIKELY(rollup_exprs.count() != rollup_directions.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected array count", K(group_by_exprs.count()), K(group_directions.count()),
    K(rollup_exprs.count()), K(rollup_directions.count()), K(ret));
  } else if (OB_FAIL(append(sort_exprs, group_by_exprs)) ||
             OB_FAIL(append(sort_directions, group_directions))) {
    LOG_WARN("failed to append sort keys", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < rollup_exprs.count(); i++) {
      bool is_const = false;
      ObRawExpr *expr = NULL;
      if (OB_ISNULL(expr = rollup_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (T_FUN_SYS_REMOVE_CONST == expr->get_expr_type()) {
        /*do nothing*/
      } else if (OB_FAIL(ObOptimizerUtil::is_const_expr(expr,
                                                        top->get_output_equal_sets(),
                                                        top->get_output_const_exprs(),
                                                        get_onetime_query_refs(),
                                                        is_const))) {
        LOG_WARN("failed to check whether expr is const", K(ret));
      } else if (is_const) {
        /*do nothing*/
      } else if (OB_FAIL(sort_exprs.push_back(expr)) ||
                 OB_FAIL(sort_directions.push_back(rollup_directions.at(i)))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

int ObSelectLogPlan::allocate_topk_for_merge_group_plan(ObLogicalOperator *&top)
{
  int ret = OB_SUCCESS;
  bool need_sort = false;
  int64_t prefix_pos = 0;
  ObSEArray<ObRawExpr*, 16> order_by_exprs;
  ObSEArray<ObOrderDirection, 16> directions;
  const ObSelectStmt *select_stmt = NULL;
  if (OB_ISNULL(top) || OB_ISNULL(select_stmt = get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(top), K(select_stmt), K(ret));
  } else if (!select_stmt->is_match_topk()) {
    /*do nothing*/
  } else if (OB_FAIL(get_order_by_exprs(top, order_by_exprs, &directions))) {
    LOG_WARN("failed to get order by exprs", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::check_need_sort(order_by_exprs,
                                                      &directions,
                                                      top->get_op_ordering(),
                                                      top->get_fd_item_set(),
                                                      top->get_output_equal_sets(),
                                                      top->get_output_const_exprs(),
                                                      get_onetime_query_refs(),
                                                      top->get_is_at_most_one_row(),
                                                      need_sort,
                                                      prefix_pos))) {
    LOG_WARN("failed to check need sort", K(ret));
  } else {
    if (need_sort) {
      ObSEArray<OrderItem, 8> sort_keys;
      ObSEArray<OrderItem, 8> topk_sort_keys;
      if (OB_FAIL(make_order_items(order_by_exprs, directions, sort_keys))) {
        LOG_WARN("failed to make order items", K(ret));
      } else if (OB_FAIL(clone_sort_keys_for_topk(sort_keys,
                                                  topk_sort_keys))) {
        LOG_WARN("failed to clone sort keys for topk", K(ret));
      } else if (OB_FAIL(allocate_sort_as_top(top, topk_sort_keys))) {
        LOG_WARN("failed to allocate sort as top", K(ret));
      } else { /*do nothing*/}
    } else {
      if (OB_FAIL(allocate_material_as_top(top))) {
        LOG_WARN("failed to allocate material as top", K(ret));
      } else { /*do nothing*/ }
    }
    if (OB_SUCC(ret)) {
      const ObGlobalHint &global_hint = get_optimizer_context().get_global_hint();
      if (OB_FAIL(allocate_topk_as_top(top,
                                      select_stmt->get_limit_expr(),
                                      select_stmt->get_offset_expr(),
                                      global_hint.sharding_minimum_row_count_,
                                      global_hint.topk_precision_))) {
        LOG_WARN("failed to allocate topk as top", K(ret));
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

int ObSelectLogPlan::allocate_window_function_as_top(const WinDistAlgo dist_algo,
                                                     const ObIArray<ObWinFunRawExpr *> &win_exprs,
                                                     const bool match_parallel,
                                                     const bool is_partition_wise,
                                                     const bool use_hash_sort,
                                                     const ObIArray<OrderItem> &sort_keys,
                                                     ObLogicalOperator *&top)
{
  const int32_t role_type = ObLogWindowFunction::WindowFunctionRoleType::NORMAL;
  const int64_t range_dist_keys_cnt = 0;
  const int64_t range_dist_pby_prefix = 0;
  return allocate_window_function_as_top(dist_algo, win_exprs, match_parallel, is_partition_wise,
                                         use_hash_sort, role_type, sort_keys, range_dist_keys_cnt,
                                         range_dist_pby_prefix, top, NULL, NULL);
}

int ObSelectLogPlan::allocate_window_function_as_top(const WinDistAlgo dist_algo,
                                                     const ObIArray<ObWinFunRawExpr *> &win_exprs,
                                                     const bool match_parallel,
                                                     const bool is_partition_wise,
                                                     const bool use_hash_sort,
                                                     const int32_t role_type,
                                                     const ObIArray<OrderItem> &sort_keys,
                                                     const int64_t range_dist_keys_cnt,
                                                     const int64_t range_dist_pby_prefix,
                                                     ObLogicalOperator *&top,
                                                     ObOpPseudoColumnRawExpr *wf_aggr_status_expr,  /* default null */
                                                     const ObIArray<bool> *pushdown_info /* default null */)
{
  int ret = OB_SUCCESS;
  ObLogWindowFunction *window_function = NULL;
  if (OB_ISNULL(top)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get unexpected null", K(ret), K(top));
  } else if (OB_ISNULL(window_function = static_cast<ObLogWindowFunction *>(
                                          get_log_op_factory().allocate(*this, LOG_WINDOW_FUNCTION)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("allocate memory for ObLogWindowFunction failed", K(ret));
  } else if (OB_FAIL(append(window_function->get_window_exprs(), win_exprs))) {
    LOG_WARN("failed to add window expr", K(ret));
  } else if (OB_FAIL(window_function->set_sort_keys(sort_keys))) {
    LOG_WARN("set range distribution sort keys failed", K(ret));
  } else {
    window_function->set_win_dist_algo(dist_algo);
    window_function->set_rd_sort_keys_cnt(range_dist_keys_cnt);
    window_function->set_single_part_parallel(match_parallel);
    window_function->set_is_partition_wise(is_partition_wise);
    window_function->set_child(ObLogicalOperator::first_child, top);
    window_function->set_role_type(ObLogWindowFunction::WindowFunctionRoleType(role_type));
    window_function->set_use_hash_sort(use_hash_sort);
    if (range_dist_keys_cnt > 0) {
      window_function->set_ragne_dist_parallel(true);
      window_function->set_rd_pby_sort_cnt(range_dist_pby_prefix);
    }
    if (OB_SUCC(ret) && window_function->is_push_down()) {
      if (OB_ISNULL(wf_aggr_status_expr) || OB_ISNULL(pushdown_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(wf_aggr_status_expr), K(pushdown_info));
      } else if (OB_FAIL(window_function->set_pushdown_info(*pushdown_info))) {
        LOG_WARN("set_pushdown_info failed", K(ret));
      } else {
        // use the value of wf_aggr_status_expr to decide how to compute in consilidator wf op
        window_function->set_aggr_status_expr(wf_aggr_status_expr);
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(window_function->compute_property())) {
      LOG_WARN("failed to compute property", K(ret));
    } else {
      top = window_function;
    }
  }
  return ret;
}

int ObSelectLogPlan::candi_allocate_distinct()
{
  int ret = OB_SUCCESS;
  bool is_unique = false;
  const ObSelectStmt *stmt = NULL;
  ObLogicalOperator *best_plan = NULL;
  ObSEArray<ObRawExpr*, 8> reduce_exprs;
  ObSEArray<ObRawExpr*, 8> distinct_exprs;
  ObSEArray<ObRawExpr*, 8> candi_subquery_exprs;
  OPT_TRACE_TITLE("start to generate distinct operator");
  if (OB_ISNULL(stmt = get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(candidates_.get_best_plan(best_plan))) {
    LOG_WARN("failed to get current best plan", K(ret));
  } else if (OB_ISNULL(best_plan)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(get_distinct_exprs(best_plan, reduce_exprs, distinct_exprs))) {
    LOG_WARN("failed to get select columns", K(ret));
  } else if (OB_FAIL(candi_allocate_subplan_filter_for_exprs(distinct_exprs))) {
        LOG_WARN("failed to allocate subplan filter for exprs", K(ret));
  } else if (distinct_exprs.empty()) {
    // if all the distinct exprs are const, we add limit operator instead of distinct operator
    ObConstRawExpr *limit_expr = NULL;
    if (OB_FAIL(ObRawExprUtils::build_const_int_expr(get_optimizer_context().get_expr_factory(),
                                                     ObIntType,
                                                     1,
                                                     limit_expr))) {
      LOG_WARN("failed to create const expr", K(ret));
    } else if (OB_FAIL(candi_allocate_limit(limit_expr))) {
      LOG_WARN("failed to allocate limit operator", K(ret));
    } else {
      OPT_TRACE("distinct exprs is const, need limit op instead of distinct op");
      LOG_TRACE("distinct exprs is const, need limit op instead of distinct op", K(distinct_exprs));
    }
  } else if (OB_FAIL(ObOptimizerUtil::is_exprs_unique(distinct_exprs,
                                                      best_plan->get_table_set(),
                                                      best_plan->get_fd_item_set(),
                                                      best_plan->get_output_equal_sets(),
                                                      best_plan->get_output_const_exprs(),
                                                      is_unique))) {
    LOG_WARN("failed to check whether distinct exprs is unique", K(ret));
  } else if (is_unique) {
    OPT_TRACE("distinct exprs is unique, no need distinct");
    LOG_TRACE("distinct exprs is unique, no need distinct", K(distinct_exprs));
  } else {
    SMART_VAR(GroupingOpHelper, distinct_helper) {
      CandidatePlan candidate_plan;
      ObSEArray<CandidatePlan, 4> distinct_plans;
      ObSEArray<ObOrderDirection, 4> distinct_directions;
      if (OB_FAIL(init_distinct_helper(distinct_exprs, distinct_helper))) {
        LOG_WARN("failed to init distinct helper", K(ret));
      } else if (OB_FAIL(ObOptimizerUtil::get_default_directions(distinct_exprs.count(),
                                                                 distinct_directions))) {
        LOG_WARN("failed to generate default directions", K(ret));
      }
      // create hash distinct
      if (OB_SUCC(ret) && !distinct_helper.force_use_merge_) {
        ObSEArray<CandidatePlan, 16> best_candidates;
        if (OB_FAIL(get_minimal_cost_candidates(candidates_.candidate_plans_, best_candidates))) {
          LOG_WARN("failed to get minimal cost candidates", K(ret));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < best_candidates.count(); i++) {
            candidate_plan = best_candidates.at(i);
            OPT_TRACE("generate hash distinct for plan:", candidate_plan);
            if (OB_FAIL(create_hash_distinct_plan(candidate_plan.plan_tree_,
                                                  distinct_helper,
                                                  reduce_exprs,
                                                  distinct_exprs))) {
              LOG_WARN("failed to create hash distinct plan", K(ret));
            } else if (OB_FAIL(distinct_plans.push_back(candidate_plan))) {
              LOG_WARN("failed to push back hash distinct candidate plan", K(ret));
            } else { /*do nothing*/ }
          }
        }
      }

      //create merge distinct plan
      if (OB_SUCC(ret) && !distinct_helper.force_use_hash_) {
        bool can_ignore_merge_plan = !(distinct_plans.empty() || distinct_helper.force_use_merge_);
        bool is_plan_valid = false;
        for(int64_t i = 0; OB_SUCC(ret) && i < candidates_.candidate_plans_.count(); i++) {
          candidate_plan = candidates_.candidate_plans_.at(i);
            OPT_TRACE("generate merge distinct for plan:", candidate_plan);
          if (OB_FAIL(create_merge_distinct_plan(candidate_plan.plan_tree_,
                                                distinct_helper,
                                                reduce_exprs,
                                                distinct_exprs,
                                                distinct_directions,
                                                is_plan_valid,
                                                can_ignore_merge_plan))) {
            LOG_WARN("failed to allocate merge distinct plan", K(ret));
          } else if (is_plan_valid && OB_FAIL(distinct_plans.push_back(candidate_plan))) {
            LOG_WARN("failed to add merge distinct candidate plan", K(ret));
          } else { /*do nothing*/ }
        }
      }
      if (OB_SUCC(ret)) {
        int64_t check_scope = OrderingCheckScope::CHECK_SET | OrderingCheckScope::CHECK_ORDERBY;
        if (OB_FAIL(update_plans_interesting_order_info(distinct_plans, check_scope))) {
          LOG_WARN("failed to update plans interesting order info", K(ret));
        } else if (OB_FAIL(prune_and_keep_best_plans(distinct_plans))) {
          LOG_WARN("Failed to add plans", K(ret));
        } else { /* do nothing*/ }
      }
    }
  }
  return ret;
}

int ObSelectLogPlan::get_distinct_exprs(const ObLogicalOperator *top,
				                                ObIArray<ObRawExpr *> &reduce_exprs,
                                        ObIArray<ObRawExpr *> &distinct_exprs)
{
  int ret = OB_SUCCESS;
  const ObSelectStmt *select_stmt = get_stmt();
  reduce_exprs.reuse();
  if (OB_ISNULL(select_stmt) || OB_ISNULL(top)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(select_stmt), K(top), K(ret));
  } else {
    ObRawExpr *select_expr = NULL;
    ObSEArray<ObRawExpr *, 8> new_distinct_exprs;
    for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt->get_select_item_size(); ++i) {
      bool is_const = false;
      select_expr = select_stmt->get_select_item(i).expr_;
      if (OB_ISNULL(select_expr)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(ObOptimizerUtil::is_const_expr(select_expr,
                                                        top->get_output_equal_sets(),
                                                        top->get_output_const_exprs(),
                                                        get_onetime_query_refs(),
                                                        is_const))) {
        LOG_WARN("failed to check whether is const expr", K(ret));
      } else if (is_const) {
        //skip it
      } else if (OB_FAIL(reduce_exprs.push_back(select_expr))) {
        LOG_WARN("push expr to distinct exprs failed", K(ret));
      } else { /*do nothing*/ }
    }
    if (OB_FAIL(ret)) {
      /*do nothing*/
    } else if (OB_FAIL(ObOptimizerUtil::simplify_exprs(top->get_fd_item_set(),
                                                       top->get_output_equal_sets(),
                                                       top->get_output_const_exprs(),
                                                       reduce_exprs,
                                                       distinct_exprs))) {
      LOG_WARN("failed to simplify exprs", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::get_minset_of_exprs(distinct_exprs,new_distinct_exprs))) {
      LOG_WARN("fail to get minset of distinct exprs", K(ret));
    } else if (new_distinct_exprs.count() >= distinct_exprs.count()) {
      //do nothing
    } else if (OB_FAIL(distinct_exprs.assign(new_distinct_exprs))) {
      LOG_WARN("fail to assign new distinct expr", K(ret));
    }
  }
  return ret;
}

int ObSelectLogPlan::create_hash_distinct_plan(ObLogicalOperator *&top,
				       GroupingOpHelper &distinct_helper,
				       ObIArray<ObRawExpr*> &reduce_exprs,
				       ObIArray<ObRawExpr*> &distinct_exprs)
{
  int ret = OB_SUCCESS;
  bool is_partition_wise = false;
  ObExchangeInfo exch_info;
  if (OB_ISNULL(top)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(top), K(ret));
  } else if (top->is_distributed() &&
             OB_FAIL(top->check_sharding_compatible_with_reduce_expr(reduce_exprs,
                                                                     is_partition_wise))) {
    LOG_WARN("failed to check sharding compatible with reduce expr", K(ret));
  } else if (!top->is_distributed() || is_partition_wise) {
    OPT_TRACE("is basic distinct:", !top->is_distributed());
    OPT_TRACE("is partition wise distinct", is_partition_wise);
    if (OB_FAIL(allocate_distinct_as_top(top,
                                         AggregateAlgo::HASH_AGGREGATE,
                                         distinct_exprs,
                                         distinct_helper.group_ndv_,
                                         is_partition_wise))) {
      LOG_WARN("failed to allocate distinct as top", K(ret));
    }
  } else if (distinct_helper.can_basic_pushdown_ && //allocate push down distinct if necessary
             OB_FAIL(allocate_distinct_as_top(top,
                                              AggregateAlgo::HASH_AGGREGATE,
                                              distinct_exprs,
                                              distinct_helper.group_ndv_,
                                              false, /* is_partition_wise */
                                              true, /* is_push_down */
                                              false /* is_partition_gi */))) {
    LOG_WARN("failed to allocate distinct as top", K(ret));
  } else if (OB_FAIL(get_grouping_style_exchange_info(distinct_exprs,
                                                      top->get_output_equal_sets(),
                                                      exch_info))) {
    LOG_WARN("failed to get grouping style exchange info", K(ret));
  } else if (OB_FAIL(allocate_exchange_as_top(top, exch_info))) {
    LOG_WARN("failed to allocate exchange as top", K(ret));
  } else if (OB_FAIL(allocate_distinct_as_top(top, // allocate final distinct
                                              AggregateAlgo::HASH_AGGREGATE,
                                              distinct_exprs,
                                              distinct_helper.group_ndv_))) {
    LOG_WARN("failed to allocate distinct as top", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObSelectLogPlan::create_merge_distinct_plan(ObLogicalOperator *&top,
                                                GroupingOpHelper &distinct_helper,
                                                ObIArray<ObRawExpr*> &reduce_exprs,
                                                ObIArray<ObRawExpr*> &distinct_exprs,
                                                ObIArray<ObOrderDirection> &directions,
                                                bool &is_plan_valid,
                                                bool can_ignore_merge_plan)
{
  int ret = OB_SUCCESS;
  int64_t prefix_pos = 0;
  bool need_sort = false;
  bool is_partition_wise = false;
  ObSEArray<OrderItem, 4> sort_keys;
  int64_t interesting_order_info = OrderingFlag::NOT_MATCH;
  is_plan_valid = true;
  OPT_TRACE("start generate merge distinct plan");
  if (OB_ISNULL(top)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(adjust_sort_expr_ordering(distinct_exprs,
                                               directions,
                                               *top,
                                               false))) {
    LOG_WARN("failed to adjust sort expr ordering", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::check_need_sort(distinct_exprs,
                                                      &directions,
                                                      top->get_op_ordering(),
                                                      top->get_fd_item_set(),
                                                      top->get_output_equal_sets(),
                                                      top->get_output_const_exprs(),
                                                      get_onetime_query_refs(),
                                                      top->get_is_at_most_one_row(),
                                                      need_sort,
                                                      prefix_pos))) {
    LOG_WARN("failed to check need sort", K(ret));
  } else if (OB_FAIL(make_order_items(distinct_exprs, &directions, sort_keys))) {
    LOG_WARN("failed to make order items from exprs", K(ret));
  } else if (can_ignore_merge_plan && OB_FAIL(ObOptimizerUtil::compute_stmt_interesting_order(sort_keys,
                                                      get_stmt(),
                                                      false,
                                                      const_cast<EqualSets &>(top->get_output_equal_sets()),
                                                      top->get_output_const_exprs(),
                                                      get_is_parent_set_distinct(),
                                                      OrderingCheckScope::CHECK_SET | OrderingCheckScope::CHECK_ORDERBY,
                                                      interesting_order_info))) {
    LOG_WARN("failed to compute stmt interesting order", K(ret));
  } else if (need_sort && can_ignore_merge_plan && OrderingFlag::NOT_MATCH == interesting_order_info) {
    // if no further order needed, not generate merge style distinct
    is_plan_valid = false;
  } else if (top->is_distributed() &&
             OB_FAIL(top->check_sharding_compatible_with_reduce_expr(reduce_exprs,
                                                                     is_partition_wise))) {
    LOG_WARN("failed to check sharding compatible with reduce exprs", K(ret));
  } else if (!top->is_distributed() || is_partition_wise) {
    OPT_TRACE("is basic distinct:", !top->is_distributed());
    OPT_TRACE("is partition wise distinct", is_partition_wise);
    if (OB_FAIL(try_allocate_sort_as_top(top, sort_keys, need_sort, prefix_pos))) {
      LOG_WARN("failed to allocate sort operator", K(ret));
    } else if (OB_FAIL(allocate_distinct_as_top(top,
                                                MERGE_AGGREGATE,
                                                distinct_exprs,
                                                distinct_helper.group_ndv_,
                                                is_partition_wise))) {
      LOG_WARN("failed to allocate distinct as top", K(ret));
    }
  } else {
    // allocate push down distinct if necessary
    if (distinct_helper.can_basic_pushdown_) {
      /* If the child has not allocated exchange,
        then the down-pressed distinct operator must be full partition wise,
        and the gi operator can be allocated on the distinct operator.
        At this time, the child's order is globally ordered.*/
      OPT_TRACE("generate pushdown distinct plan");
      bool should_pullup_gi = false;
      bool top_is_local_order = false;
      bool is_partition_gi = false;
      if (OB_FAIL(check_can_pullup_gi(*top, false, need_sort, should_pullup_gi))) {
        LOG_WARN("failed to check can pullup gi", K(ret));
      } else if (OB_FALSE_IT(is_partition_gi = top->is_partition_wise())) {
      } else if (OB_FALSE_IT(top_is_local_order = top->get_is_local_order() && !should_pullup_gi)) {
      } else if ((need_sort || top_is_local_order) &&
                 OB_FAIL(allocate_sort_as_top(top,
                                              sort_keys,
                                              need_sort && top_is_local_order ? 0 : prefix_pos,
                                              !need_sort && top_is_local_order))) {
        LOG_WARN("failed to allocate sort as top", K(ret));
      } else if (OB_FAIL(allocate_distinct_as_top(top,
                                                  MERGE_AGGREGATE,
                                                  distinct_exprs,
                                                  distinct_helper.group_ndv_,
                                                  should_pullup_gi,
                                                  true,
                                                  is_partition_gi))) {
        LOG_WARN("failed to allocate distinct as top", K(ret));
      } else {
        prefix_pos = 0;
        need_sort = false;
      }
    }
    // allocate final distinct if necessary
    if (OB_SUCC(ret)) {
      ObExchangeInfo exch_info;
      if (OB_FAIL(get_grouping_style_exchange_info(distinct_exprs,
                                                   top->get_output_equal_sets(),
                                                   exch_info))) {
        LOG_WARN("failed to get grouping style exchange info", K(ret));
      } else if (OB_FAIL(allocate_sort_and_exchange_as_top(top,
                                                           exch_info,
                                                           sort_keys,
                                                           need_sort,
                                                           prefix_pos,
                                                           top->get_is_local_order()))) {
        LOG_WARN("failed to allocate operator for join style op", K(ret));
      } else if (OB_FAIL(allocate_distinct_as_top(top,
                                                  MERGE_AGGREGATE,
                                                  distinct_exprs,
                                                  distinct_helper.group_ndv_))) {
        LOG_WARN("failed to allocate distinct as top", K(ret));
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

int ObSelectLogPlan::allocate_distinct_as_top(ObLogicalOperator *&top,
              const AggregateAlgo algo,
              const ObIArray<ObRawExpr*> &distinct_exprs,
              const double total_ndv,
              const bool is_partition_wise,
              const bool is_pushed_down,
              const bool is_partition_gi)
{
int ret = OB_SUCCESS;
ObLogDistinct *distinct_op = NULL;
if (OB_ISNULL(top)) {
ret = OB_ERR_UNEXPECTED;
LOG_WARN("get unexpected null", K(top), K(ret));
} else if (OB_ISNULL(distinct_op = static_cast<ObLogDistinct*>(get_log_op_factory().
           allocate(*this, LOG_DISTINCT)))) {
ret = OB_ALLOCATE_MEMORY_FAILED;
LOG_ERROR("failed to allocate distinct operator", K(ret));
} else {
distinct_op->set_child(ObLogicalOperator::first_child, top);
distinct_op->set_algo_type(algo);
distinct_op->set_push_down(is_pushed_down);
distinct_op->set_is_partition_gi(is_partition_gi);
distinct_op->set_total_ndv(total_ndv);
distinct_op->set_is_partition_wise(is_partition_wise);
distinct_op->set_force_push_down(FORCE_GPD & get_optimizer_context().get_aggregation_optimization_settings());
if (OB_FAIL(distinct_op->set_distinct_exprs(distinct_exprs))) {
LOG_WARN("failed to set group by columns", K(ret));
} else if (OB_FAIL(distinct_op->compute_property())) {
LOG_WARN("failed to compute property", K(ret));
} else {
top = distinct_op;
}
}
return ret;
}

int ObSelectLogPlan::generate_raw_plan_for_set()
{
  int ret = OB_SUCCESS;
  const ObSelectStmt *select_stmt = get_stmt();
  ObSEArray<ObSelectLogPlan*, 2> child_plans;
  ObSEArray<ObRawExpr *, 8> remain_filters;
  ObSQLSessionInfo *session_info = NULL;
  ObRawExprFactory *expr_factory = NULL;
  if (OB_ISNULL(select_stmt) ||
      OB_ISNULL(session_info = get_optimizer_context().get_session_info()) ||
      OB_ISNULL(expr_factory = &get_optimizer_context().get_expr_factory())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(select_stmt), K(ret));
  } else if (OB_UNLIKELY(!select_stmt->is_set_stmt()) ||
            OB_UNLIKELY(2 > select_stmt->get_set_query().count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected set_op stmt", K(ret));
  } else {
    const ObIArray<ObSelectStmt*> &child_stmts = select_stmt->get_set_query();
    const int64_t child_size = child_stmts.count();
    const bool is_set_distinct = select_stmt->is_set_distinct();
    ObSEArray<ObRawExpr *, 8> child_input_filters;
    ObSEArray<ObRawExpr *, 8> child_rename_filters;
    ObSEArray<ObRawExpr *, 8> child_remain_filters;
    const ObSelectStmt *child_stmt = NULL;
    ObSelectLogPlan *child_plan = NULL;
    for (int64 i = 0; OB_SUCC(ret) && i < child_size; ++i) {
      child_input_filters.reuse();
      child_rename_filters.reuse();
      child_remain_filters.reuse();
      if (OB_ISNULL(child_stmt = child_stmts.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null stmt", K(child_stmt), K(ret));
      } else if (!pushdown_filters_.empty() &&
                 OB_FAIL(child_input_filters.assign(pushdown_filters_))) {
        LOG_WARN("failed to copy exprs", K(ret));
      } else if (OB_FAIL(ObOptimizerUtil::pushdown_and_rename_filter_into_subquery(*select_stmt,
                                                                                   *child_stmt,
                                                                                   OB_INVALID_ID,
                                                                                   get_optimizer_context(),
                                                                                   child_input_filters,
                                                                                   child_rename_filters,
                                                                                   child_remain_filters,
                                                                                   false))) {
        LOG_WARN("failed to push down filter into subquery", K(ret));
      } else if (OB_FAIL(ObOptimizerUtil::get_set_op_remain_filter(*select_stmt,
                                                                   child_remain_filters,
                                                                   remain_filters,
                                                                   0 == i))) {
        LOG_WARN("get remain filters failed", K(ret));
      } else if (OB_FAIL(generate_child_plan_for_set(child_stmt, child_plan,
                                                     child_rename_filters, i,
                                                     select_stmt->is_set_distinct()))) {
        LOG_WARN("failed to generate left subquery plan", K(ret));
      } else if (OB_FAIL(child_plans.push_back(child_plan))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(candi_allocate_set(child_plans))) {
      LOG_WARN("failed to allocate set op", K(ret));
    } else if (!remain_filters.empty() && OB_FAIL(candi_allocate_filter(remain_filters))) {
      LOG_WARN("failed to allocate filter", K(ret));
    } else if (OB_FAIL(allocate_plan_top())) {
      LOG_WARN("failed to allocate plan top", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObSelectLogPlan::candi_allocate_set(const ObIArray<ObSelectLogPlan*> &child_plans)
{
  int ret = OB_SUCCESS;
  const ObSelectStmt *select_stmt = NULL;
  ObSEArray<CandidatePlan, 4> all_plans;
  if (OB_ISNULL(select_stmt = get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (select_stmt->is_recursive_union()) {
    // generate recursive union all plans
    if (OB_FAIL(candi_allocate_recursive_union_all(child_plans))) {
      LOG_WARN("failed to allocate recursive union all", K(ret));
    } else { /*do nothing*/ }
  } else if (ObSelectStmt::UNION == select_stmt->get_set_op() &&
            (!select_stmt->is_set_distinct() || child_plans.count() > 2)) {
    // generate union all or union distinct with more than two children plans
    if (OB_FAIL(candi_allocate_union_all(child_plans))) {
      LOG_WARN("failed to allocate union all", K(ret));
    } else if (select_stmt->is_set_distinct() &&
               OB_FAIL(candi_allocate_distinct())) {
      LOG_WARN("failed to allocate distinct", K(ret));
    } else { /*do nothing*/ }
  } else if (OB_FAIL(candi_allocate_distinct_set(child_plans))) {
    // generate plans for intersect/expect/union distinct with two children
    LOG_WARN("failed to allocate distinct set op", K(ret));
  } else { /*do nothing*/ }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(update_set_sharding_info(child_plans))) {
    LOG_WARN("failed to update set sharding info", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObSelectLogPlan::update_set_sharding_info(const ObIArray<ObSelectLogPlan*> &child_plans)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 8> pure_set_exprs;
  ObSEArray<ObRawExpr*, 8> old_exprs;
  ObSEArray<ObRawExpr*, 8> new_exprs;
  ObSEArray<ObRawExpr*, 8> partition_keys;
  const ObSelectStmt *sel_stmt = NULL;
  if (OB_ISNULL(sel_stmt = get_stmt()) || OB_UNLIKELY(!sel_stmt->is_select_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret));
  } else if (OB_FAIL(sel_stmt->get_pure_set_exprs(pure_set_exprs))) {
    LOG_WARN("failed to get pure set exprs", K(ret));
  } else {
    ObSEArray<ObShardingInfo*, 8> dist_shardings;
    for (int64_t i = 0; OB_SUCC(ret) && i < candidates_.candidate_plans_.count(); i++) {
      ObShardingInfo *sharding = NULL;
      ObLogicalOperator *root = NULL;
      if (OB_ISNULL(root = candidates_.candidate_plans_.at(i).plan_tree_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(root), K(ret));
      } else {
        sharding = root->get_strong_sharding();
        if (NULL != sharding && sharding->is_distributed_with_partitioning() &&
            OB_FAIL(add_var_to_array_no_dup(dist_shardings, sharding))) {
          LOG_WARN("failed to push back sharding", K(ret));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < root->get_weak_sharding().count(); i++) {
            if (OB_ISNULL(sharding = root->get_weak_sharding().at(i))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("get unexpected null", K(ret));
            } else if (sharding->is_distributed_with_partitioning() &&
                       OB_FAIL(add_var_to_array_no_dup(dist_shardings, sharding))) {
              LOG_WARN("failed to push back sharding info", K(ret));
            } else { /*do nothing*/ }
          }
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < dist_shardings.count(); i++) {
      ObShardingInfo *sharding = NULL;
      partition_keys.reuse();
      if (OB_ISNULL(sharding = dist_shardings.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(sharding->get_all_partition_keys(partition_keys))) {
        LOG_WARN("failed to get partition keys", K(ret));
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && j < child_plans.count(); j++) {
          const ObSelectStmt *child_stmt = NULL;
          ObSelectLogPlan *child_log_plan = NULL;
          ObLogicalOperator *child_best_plan = NULL;
          ObRawExprCopier copier(get_optimizer_context().get_expr_factory());
          old_exprs.reuse();
          new_exprs.reuse();
          if (OB_ISNULL(child_log_plan = child_plans.at(j)) ||
              OB_ISNULL(child_stmt = child_log_plan->get_stmt())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null", K(child_log_plan), K(child_stmt), K(ret));
          } else if (OB_FAIL(child_log_plan->candidates_.get_best_plan(child_best_plan))) {
            LOG_WARN("failed to get best plan", K(ret));
          } else if (OB_ISNULL(child_best_plan)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null", K(ret));
          } else if (OB_FAIL(child_stmt->get_select_exprs(old_exprs))) {
            LOG_WARN("failed to get select exprs", K(ret));
          } else if (OB_FAIL(new_exprs.assign(pure_set_exprs))) {
            LOG_WARN("failed to find new exprs", K(ret));
          } else if (OB_UNLIKELY(old_exprs.count() != new_exprs.count())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected array count", K(new_exprs.count()),
                K(new_exprs.count()), K(ret));
          } else {
            for (int64_t k = 0; OB_SUCC(ret) && k < partition_keys.count(); k++) {
              int64_t idx = -1;
              ObRawExpr *expr = NULL;
              if (OB_ISNULL(expr = partition_keys.at(k))) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("get unexpected null", K(ret));
              } else if (ObOptimizerUtil::find_item(old_exprs, expr)) {
                /*do nothing*/
              } else if (!ObOptimizerUtil::find_equal_expr(old_exprs,
                                                           expr,
                                                           child_best_plan->get_output_equal_sets(),
                                                           idx)) {
                /*do nothing*/
              } else if (OB_UNLIKELY(idx < 0 || idx >= pure_set_exprs.count())) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("get unexpected array index", K(idx), K(ret));
              } else if (OB_FAIL(old_exprs.push_back(expr)) ||
                         OB_FAIL(new_exprs.push_back(pure_set_exprs.at(idx)))) {
                LOG_WARN("failed to push back exprs", K(ret));
              } else { /*do nothing*/ }
            }
            if (OB_SUCC(ret)) {
              ObArray<ObRawExpr *> new_partition_keys;
              ObArray<ObRawExpr *> new_subpartition_keys;
              ObArray<ObRawExpr *> new_partition_funcs;
              if (OB_FAIL(copier.add_replaced_expr(old_exprs, new_exprs))) {
                LOG_WARN("failed to add exprs", K(ret));
              } else if (OB_FAIL(copier.copy_on_replace(sharding->get_partition_keys(),
                                                        new_partition_keys))) {
                LOG_WARN("failed to copy on replace exprs", K(ret));
              } else if (OB_FAIL(sharding->get_partition_keys().assign(new_partition_keys))) {
                LOG_WARN("failed to assign new partition keys", K(ret));
              } else if (OB_FAIL(copier.copy_on_replace(sharding->get_sub_partition_keys(),
                                                        new_subpartition_keys))) {
                LOG_WARN("failed to copy on replace exprs", K(ret));
              } else if (OB_FAIL(sharding->get_sub_partition_keys().assign(new_subpartition_keys))) {
                LOG_WARN("failed to assign sub partition keys", K(ret));
              } else if (OB_FAIL(copier.copy_on_replace(sharding->get_partition_func(),
                                                        new_partition_funcs))) {
                LOG_WARN("failed to copy on replace exprs", K(ret));
              } else if (OB_FAIL(sharding->get_partition_func().assign(new_partition_funcs))) {
                LOG_WARN("failed to assign partition funcs", K(ret));
              } else { /*do nothing*/ }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObSelectLogPlan::candi_allocate_union_all(const ObIArray<ObSelectLogPlan*> &child_plans)
{
  int ret = OB_SUCCESS;
  ObSEArray<CandidatePlan, 8> all_plans;
  int64_t check_scope = OrderingCheckScope::CHECK_SET | OrderingCheckScope::CHECK_ORDERBY;
  if (OB_FAIL(generate_union_all_plans(child_plans, false, all_plans))) {
    LOG_WARN("failed to generate union all plans", K(ret));
  } else if (!all_plans.empty()) {
    LOG_TRACE("succeed to allocate union all using hint", K(all_plans.count()));
  } else if (OB_FAIL(get_log_plan_hint().check_status())) {
    LOG_WARN("failed to generate plans with hint", K(ret));
  } else if (OB_FAIL(generate_union_all_plans(child_plans, true, all_plans))) {
    LOG_WARN("failed to generate union all plans", K(ret));
  } else {
    LOG_TRACE("succeed to allocate union all ignore hint", K(all_plans.count()));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(update_plans_interesting_order_info(all_plans, check_scope))) {
    LOG_WARN("failed to update plans interesting order info", K(ret));
  } else if (OB_FAIL(prune_and_keep_best_plans(all_plans))) {
    LOG_WARN("failed to add all plans", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObSelectLogPlan::generate_union_all_plans(const ObIArray<ObSelectLogPlan*> &child_plans,
                                              const bool ignore_hint,
                                              ObIArray<CandidatePlan> &all_plans)
{
  int ret = OB_SUCCESS;
  if (child_plans.count() > 2) {
    ObSEArray<ObLogicalOperator*, 2> child_ops;
    // get best children plan
    for (int64_t i = 0; OB_SUCC(ret) && i < child_plans.count(); i++) {
      ObLogicalOperator *best_plan = NULL;
      if (OB_ISNULL(child_plans.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(child_plans.at(i)->get_candidate_plans().get_best_plan(best_plan))) {
        LOG_WARN("failed to get best plan", K(ret));
      } else if (OB_ISNULL(best_plan)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(child_ops.push_back(best_plan))) {
        LOG_WARN("failed to push back child ops", K(ret));
      } else { /*do nothing*/ }
    }
    // generate union all plan
    if (OB_SUCC(ret)) {
      CandidatePlan candidate_plan;
      if (OB_FAIL(create_union_all_plan(child_ops, ignore_hint, candidate_plan.plan_tree_))) {
        LOG_WARN("failed to create union all plan", K(ret));
      } else if (NULL == candidate_plan.plan_tree_) {
        /*do nothing*/
      } else if (OB_FAIL(all_plans.push_back(candidate_plan))) {
        LOG_WARN("failed to push back candidate plan", K(ret));
      } else { /*do nothing*/ }
    }
  } else {
    bool has_next = true;
    ObSEArray<int64_t, 8> move_pos;
    ObSEArray<CandidatePlan, 8> best_plan;
    ObSEArray<ObSEArray<CandidatePlan, 8>, 8> best_plan_list;
    ObSEArray<ObLogicalOperator*, 8> child_ops;
    for (int64_t i = 0; OB_SUCC(ret) && i < child_plans.count(); i++) {
      ObSelectLogPlan *select_plan = NULL;
      best_plan.reuse();
      if (OB_ISNULL(select_plan = child_plans.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(get_minimal_cost_candidates(select_plan->get_candidate_plans().candidate_plans_,
                                                     best_plan))) {
        LOG_WARN("failed to get minimal cost plans", K(ret));
      } else if (OB_UNLIKELY(best_plan.empty())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(ret));
      } else if (OB_FAIL(best_plan_list.push_back(best_plan))) {
        LOG_WARN("failed to push back best plan", K(ret));
      } else if (OB_FAIL(move_pos.push_back(0))) {
        LOG_WARN("failed to push back move pos", K(ret));
      } else { /*do nothing*/ }
    }
    while (OB_SUCC(ret) && has_next) {
      child_ops.reuse();
      // get child ops to generate plan
      for (int64_t i = 0; OB_SUCC(ret) && i < move_pos.count(); i++) {
        ObLogicalOperator *child_op = NULL;
        int64_t size = best_plan_list.at(i).count();
        if (OB_UNLIKELY(move_pos.at(i) < 0 || move_pos.at(i) >= size)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected array count", K(size), K(move_pos.at(i)), K(ret));
        } else if (OB_ISNULL(child_op = best_plan_list.at(i).at(move_pos.at(i)).plan_tree_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (OB_FAIL(child_ops.push_back(child_op))) {
          LOG_WARN("failed to push back child ops", K(ret));
        } else { /*do nothing*/ }
      }
      // generate union all plan
      if (OB_SUCC(ret)) {
        CandidatePlan candidate_plan;
        if (OB_FAIL(create_union_all_plan(child_ops, ignore_hint, candidate_plan.plan_tree_))) {
          LOG_WARN("failed to create union all plan", K(ret));
        } else if (NULL == candidate_plan.plan_tree_) {
          /*do nothing*/
        } else if (OB_FAIL(all_plans.push_back(candidate_plan))) {
          LOG_WARN("failed to push back candidate plan", K(ret));
        } else { /*do nothing*/ }
      }
      // reset pos for next generation
      if (OB_SUCC(ret)) {
        has_next = false;
        for (int64_t i = move_pos.count() - 1; !has_next && OB_SUCC(ret) && i >= 0; i--) {
          if (move_pos.at(i) < best_plan_list.at(i).count() - 1) {
            ++move_pos.at(i);
            has_next = true;
            for (int64_t j = i + 1; j < move_pos.count(); j++) {
              move_pos.at(j) = 0;
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObSelectLogPlan::create_union_all_plan(const ObIArray<ObLogicalOperator*> &child_ops,
                                           const bool ignore_hint,
                                           ObLogicalOperator *&top)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObLogicalOperator*, 8> set_child_ops;
  top = NULL;
  const ObSelectStmt* select_stmt = NULL;
  uint64_t set_dist_methods = DistAlgo::DIST_BASIC_METHOD
                              | DistAlgo::DIST_PARTITION_WISE
                              | DistAlgo::DIST_SET_PARTITION_WISE
                              | DistAlgo::DIST_EXT_PARTITION_WISE
                              | DistAlgo::DIST_PULL_TO_LOCAL
                              | DistAlgo::DIST_SET_RANDOM;
  int64_t random_none_idx = OB_INVALID_INDEX;
  bool is_partition_wise = false;
  bool is_ext_partition_wise = false;
  bool is_set_partition_wise = false;
  DistAlgo hint_dist_methods = get_log_plan_hint().get_valid_set_dist_algo(&random_none_idx);
  if (OB_ISNULL(select_stmt = get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(select_stmt), K(ret));
  } else if (!get_optimizer_context().is_var_assign_only_in_root_stmt() &&
      get_optimizer_context().has_var_assign()) {
    set_dist_methods &= DistAlgo::DIST_PULL_TO_LOCAL | DistAlgo::DIST_BASIC_METHOD;
  } else if (!ignore_hint && DistAlgo::DIST_INVALID_METHOD != hint_dist_methods) {
    set_dist_methods &= hint_dist_methods;
  } else {
    random_none_idx = OB_INVALID_INDEX;
  }
  if (OB_FAIL(ret)) {
    //do nothing
  } else if (!ignore_hint && DistAlgo::DIST_INVALID_METHOD != hint_dist_methods) {
    //do nothing
  } else if (select_stmt->is_set_stmt() && !select_stmt->is_set_distinct() && get_optimizer_context().force_serial_set_order()) {
    //for union all to keep child branches execute serially from left to right
    set_dist_methods &= (DistAlgo::DIST_PULL_TO_LOCAL | DistAlgo::DIST_BASIC_METHOD);
  }
  OPT_TRACE("start create union all plan");
  if (OB_SUCC(ret) && (set_dist_methods & DistAlgo::DIST_BASIC_METHOD)) {
    bool is_basic = false;
    OPT_TRACE("check match basic method");
    if (OB_FAIL(ObOptimizerUtil::check_basic_sharding_info(get_optimizer_context().get_local_server_addr(),
                                                           child_ops,
                                                           is_basic))) {
      LOG_WARN("failed to check basic sharding info", K(ret));
    } else if (!is_basic) {
      set_dist_methods &= ~DIST_BASIC_METHOD;
      OPT_TRACE("will not use basic method");
    } else if (OB_FAIL(set_child_ops.assign(child_ops))) {
      LOG_WARN("failed to assign child ops", K(ret));
    } else {
      set_dist_methods = DistAlgo::DIST_BASIC_METHOD;
      OPT_TRACE("will use basic method, ignore other method");
    }
  }

  if (OB_SUCC(ret) && (set_dist_methods & DistAlgo::DIST_PARTITION_WISE)) {
    OPT_TRACE("check match partition wise");
    if (OB_FAIL(check_if_union_all_match_partition_wise(child_ops, is_partition_wise))) {
      LOG_WARN("failed to check if union all match partition wise", K(ret));
    } else if (!is_partition_wise) {
      set_dist_methods &= ~DIST_PARTITION_WISE;
      OPT_TRACE("will not use partition wise");
    } else if (OB_FAIL(set_child_ops.assign(child_ops))) {
      LOG_WARN("failed to assign child ops", K(ret));
    } else {
      set_dist_methods = DistAlgo::DIST_PARTITION_WISE;
      OPT_TRACE("will use partition wise, ignore other method");
    }
  }

  if (OB_SUCC(ret) && (set_dist_methods & DistAlgo::DIST_EXT_PARTITION_WISE)) {
    OPT_TRACE("check match extend partition wise");
    if (OB_FAIL(check_if_union_all_match_extended_partition_wise(child_ops, is_ext_partition_wise))) {
      LOG_WARN("failed to check if union all match extended partition wise", K(ret));
    } else if (!is_ext_partition_wise) {
      set_dist_methods &= ~DIST_EXT_PARTITION_WISE;
      OPT_TRACE("will not use extend partition wise");
    } else if (OB_FAIL(set_child_ops.assign(child_ops))) {
      LOG_WARN("failed to assign child ops", K(ret));
    } else {
      set_dist_methods = DistAlgo::DIST_EXT_PARTITION_WISE;
      OPT_TRACE("will use extend partition wise, ignore other method");
    }
  }

  if (OB_SUCC(ret) && (set_dist_methods & DistAlgo::DIST_SET_PARTITION_WISE)) {
    OPT_TRACE("check match set partition wise");
    if (OB_FAIL(check_if_union_all_match_set_partition_wise(child_ops, is_set_partition_wise))) {
      LOG_WARN("failed to check if union all match set partition wise", K(ret));
    } else if (!is_set_partition_wise) {
      set_dist_methods &= ~DIST_SET_PARTITION_WISE;
      OPT_TRACE("will not use set partition wise");
    } else if (OB_FAIL(set_child_ops.assign(child_ops))) {
      LOG_WARN("failed to assign child ops", K(ret));
    } else {
      set_dist_methods = DistAlgo::DIST_SET_PARTITION_WISE;
      OPT_TRACE("will use set partition wise, ignore other method");
    }
  }

  if (OB_SUCC(ret) && (set_dist_methods & DistAlgo::DIST_SET_RANDOM)) {
    OPT_TRACE("check match set random method");
    // has distributed child, use random distribution
    ObLogicalOperator *largest_op = NULL;
    ObExchangeInfo exch_info;
    if (OB_FAIL(get_largest_sharding_child(child_ops, random_none_idx, largest_op))) {
      LOG_WARN("failed to get largest sharding children", K(ret));
    } else if (NULL == largest_op) {
      set_dist_methods &= ~DistAlgo::DIST_SET_RANDOM;
      OPT_TRACE("all children`s sharding is distribute, no need shuffle");
      OPT_TRACE("will not use set random");
    } else if (OB_FAIL(exch_info.server_list_.assign(largest_op->get_server_list()))) {
      LOG_WARN("failed to assign server list", K(ret));
    } else {
      OPT_TRACE("will use set random, ignore other method");
      exch_info.parallel_ = largest_op->get_parallel();
      exch_info.server_cnt_ = largest_op->get_server_cnt();
      exch_info.dist_method_ = ObPQDistributeMethod::RANDOM;
      set_dist_methods = DistAlgo::DIST_SET_RANDOM;
      for (int64_t i = 0; OB_SUCC(ret) && i < child_ops.count(); i++) {
        ObLogicalOperator *child_op = NULL;
        if (OB_ISNULL(child_op = child_ops.at(i)) ||
            OB_ISNULL(child_op->get_plan())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (child_op != largest_op &&
                   OB_FAIL(child_op->get_plan()->allocate_exchange_as_top(child_op, exch_info))) {
          LOG_WARN("failed to allocate exchange as top", K(ret));
        } else if (OB_FAIL(set_child_ops.push_back(child_op))) {
          LOG_WARN("failed to push back child ops", K(ret));
        } else { /*do nothing*/ }
      }
    }
  }

  if (OB_SUCC(ret) && (set_dist_methods & DistAlgo::DIST_PULL_TO_LOCAL)) {
    OPT_TRACE("will use pull to local method");
    ObExchangeInfo exch_info;
    set_dist_methods = DistAlgo::DIST_PULL_TO_LOCAL;
    for (int64_t i = 0; OB_SUCC(ret) && i < child_ops.count(); i++) {
      ObLogicalOperator *child_op = NULL;
      if (OB_ISNULL(child_op = child_ops.at(i)) ||
          OB_ISNULL(child_op->get_plan())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (child_op->is_sharding() &&
                OB_FAIL(child_op->get_plan()->allocate_exchange_as_top(child_op, exch_info))) {
        LOG_WARN("failed to allocate exchange as top", K(ret));
      } else if (OB_ISNULL(child_op)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(child_op), K(ret));
      } else if (OB_FAIL(set_child_ops.push_back(child_op))) {
        LOG_WARN("failed to push back child ops", K(ret));
      } else { /*do nothing*/ }
    } 
  }

  if (OB_SUCC(ret)) {
    DistAlgo dist_set_method = get_dist_algo(set_dist_methods);
    if (DistAlgo::DIST_INVALID_METHOD == dist_set_method) {
      /*do nothing*/
    } else if (OB_FAIL(allocate_union_all_as_top(set_child_ops, dist_set_method, top))) {
      LOG_WARN("failed to allocate union all as top", K(ret));
    } else {
      LOG_TRACE("succeed to allocate union all as top", K(dist_set_method));
    }
  }
  return ret;
}

int ObSelectLogPlan::check_if_union_all_match_partition_wise(const ObIArray<ObLogicalOperator*> &child_ops,
                                                             bool &is_partition_wise)
{
  int ret = OB_SUCCESS;
  EqualSets equal_sets;
  EqualSets first_equal_sets;
  ObShardingInfo *first_sharding = NULL;
  ObShardingInfo *child_sharding = NULL;
  const ObSelectStmt *child_stmt = NULL;
  ObSEArray<ObRawExpr*, 4> first_select_exprs;
  ObSEArray<ObRawExpr*, 4> child_select_exprs;
  is_partition_wise = true;
  for (int64_t i = 0; OB_SUCC(ret) && is_partition_wise && i < child_ops.count(); i++) {
    equal_sets.reset();
    child_select_exprs.reset();
    if (OB_ISNULL(child_ops.at(i)) ||
        OB_ISNULL(child_sharding = child_ops.at(i)->get_sharding()) ||
        OB_ISNULL(child_ops.at(i)->get_plan()) ||
        OB_ISNULL(child_ops.at(i)->get_plan()->get_stmt()) ||
        !child_ops.at(i)->get_plan()->get_stmt()->is_select_stmt() ||
        OB_ISNULL(child_stmt = static_cast<const ObSelectStmt*>(child_ops.at(i)->get_plan()->get_stmt()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(child_ops.at(i)), K(child_sharding), K(ret));
    } else if (child_ops.at(i)->is_exchange_allocated()) {
      is_partition_wise = false;
    } else if (i == 0) {
      first_sharding = child_sharding;
      first_equal_sets = child_ops.at(i)->get_output_equal_sets();
      if (OB_FAIL(child_stmt->get_select_exprs(first_select_exprs))) {
        LOG_WARN("failed to get select exprs", K(ret));
      }
    } else if (OB_FAIL(child_stmt->get_select_exprs(child_select_exprs))) {
      LOG_WARN("failed to get select exprs", K(ret));
    } else if (first_select_exprs.count() != child_select_exprs.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("select exprs count doesn't match", K(first_select_exprs), K(child_select_exprs));
    } else if (OB_FAIL(append(equal_sets, first_equal_sets))) {
      LOG_WARN("failed to append equal sets", K(ret));
    } else if (OB_FAIL(append(equal_sets, child_ops.at(i)->get_output_equal_sets()))) {
      LOG_WARN("failed to append equal sets", K(ret));
    } else if (OB_FAIL(ObShardingInfo::check_if_match_partition_wise(equal_sets,
                                                                     first_select_exprs,
                                                                     child_select_exprs,
                                                                     first_sharding,
                                                                     child_sharding,
                                                                     is_partition_wise))) {
      LOG_WARN("failed to check if union all match strict partition-wise", K(ret));
    } else {
      LOG_TRACE("succ to check union all matching pw", K(is_partition_wise));
    }
  }
  return ret;
}

int ObSelectLogPlan::check_if_union_all_match_set_partition_wise(const ObIArray<ObLogicalOperator*> &child_ops,
                                                                 bool &is_union_all_set_pw)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObAddr, 4> first_server_list;
  bool is_inherit_from_access_all = false;
  is_union_all_set_pw = true;
  for (int64_t i = 0; OB_SUCC(ret) && is_union_all_set_pw && i < child_ops.count(); i++) {
    ObLogicalOperator *child_op = NULL;
    ObShardingInfo *child_sharding = NULL;
    const ObSelectStmt *child_stmt = NULL;
    bool has_external_table_scan = false;
    if (OB_ISNULL(child_op = child_ops.at(i))
        || OB_ISNULL(child_sharding = child_op->get_sharding())
        || !child_ops.at(i)->get_plan()->get_stmt()->is_select_stmt()
        || OB_ISNULL(child_stmt = static_cast<const ObSelectStmt*>(child_ops.at(i)->get_plan()->get_stmt()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid input", K(ret), K(child_op), K(child_sharding));
    } else if (!child_sharding->is_distributed()) {
      is_union_all_set_pw = false;
      OPT_TRACE("not distributed sharding, can not use set partition wise");
    } else if (OB_FAIL(check_external_table_scan(const_cast<ObSelectStmt*>(child_stmt), has_external_table_scan))) {
      LOG_WARN("fail to check has external table scan", K(ret));
    } else if (has_external_table_scan) {
      is_union_all_set_pw = false;
      OPT_TRACE("has external table scan, can not use set partition wise");
    } else if (i == 0) {
      if (OB_FAIL(check_sharding_inherit_from_access_all(child_op,
                                                         is_inherit_from_access_all))) {
        LOG_WARN("failed to check sharding inherit from access all", K(ret));
      } else if (is_inherit_from_access_all) {
        //partition wise flag conflict with access all flag
        is_union_all_set_pw = false;
      } else if (OB_FAIL(first_server_list.assign(child_op->get_server_list()))) {
        LOG_WARN("failed to get first server list", K(ret));
      } else {
        LOG_TRACE("succ to check union all matching set pw", K(first_server_list));
      }
    } else if (OB_FAIL(check_sharding_inherit_from_access_all(child_op,
                                                             is_inherit_from_access_all))) {
      LOG_WARN("failed to check sharding inherit from access all", K(ret));
    } else if (is_inherit_from_access_all) {
      //partition wise flag conflict with access all flag
      is_union_all_set_pw = false;
    } else if (OB_FAIL(ObShardingInfo::is_physically_equal_serverlist(first_server_list,
                                                                      child_op->get_server_list(),
                                                                      is_union_all_set_pw))) {
      LOG_WARN("failed to check if equal server list", K(ret));
    } else {
      if (!is_union_all_set_pw) {
        OPT_TRACE("server list not equal, can not use set partition wise");
      }
      LOG_TRACE("succ to check union all matching set pw",
                K(first_server_list), K(child_op->get_server_list()), K(is_union_all_set_pw));
    }
  }
  return ret;
}

int ObSelectLogPlan::check_sharding_inherit_from_access_all(ObLogicalOperator* op,
                                                            bool &is_inherit_from_access_all)
{
  int ret = OB_SUCCESS;
  is_inherit_from_access_all = false;
  if (OB_ISNULL(op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null op", K(ret));
  } else if (log_op_def::LOG_JOIN == op->get_type()) {
    ObLogJoin *log_join = static_cast<ObLogJoin*>(op);
    if (DIST_BC2HOST_NONE == log_join->get_dist_method() &&
        log_join->is_nlj_with_param_down()) {
      is_inherit_from_access_all = true;
    }
  }
  if (OB_SUCC(ret) &&
      !is_inherit_from_access_all &&
      op->get_inherit_sharding_index() != -1) {
    int64_t idx = op->get_inherit_sharding_index();
    if (idx < 0 || idx >= op->get_num_of_child()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect inherit sharding index", K(ret));
    } else if (OB_FAIL(SMART_CALL(check_sharding_inherit_from_access_all(op->get_child(idx),
                                                                         is_inherit_from_access_all)))) {
      LOG_WARN("failed to check sharding inherit from bc2host", K(ret));
    }
  }
  return ret;
}

int ObSelectLogPlan::check_if_union_all_match_extended_partition_wise(const ObIArray<ObLogicalOperator*> &child_ops,
                                                                      bool &is_union_all_ext_pw)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObAddr, 4> first_server_list;
  is_union_all_ext_pw = true;
  for (int64_t i = 0; OB_SUCC(ret) && is_union_all_ext_pw && i < child_ops.count(); i++) {
    ObLogicalOperator *child_op = NULL;
    ObShardingInfo *child_sharding = NULL;
    if (OB_ISNULL(child_op = child_ops.at(i)) ||
        OB_ISNULL(child_sharding = child_op->get_sharding())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid input", K(ret), K(child_op), K(child_sharding));
    } else if (!child_sharding->is_distributed()) {
      is_union_all_ext_pw = false;
      OPT_TRACE("not distribute sharding, can not use extend partition wise");
    } else if (i == 0) {
      if (OB_FAIL(first_server_list.assign(child_op->get_server_list()))) {
        LOG_WARN("failed to get first server list", K(ret));
      } else {
        LOG_TRACE("succ to check union all matching ext pw", K(first_server_list));
      }
    } else if (OB_FAIL(ObShardingInfo::is_physically_both_shuffled_serverlist(first_server_list,
                                                                              child_op->get_server_list(),
                                                                              is_union_all_ext_pw))) {
      LOG_WARN("failed to check if both are shuffled server list", K(ret));
    } else {
      if (!is_union_all_ext_pw) {
        OPT_TRACE("server list not match, can not use extend partition wise");
      }
      LOG_TRACE("succ to check union all matching ext pw",
                K(first_server_list), K(child_op->get_server_list()), K(is_union_all_ext_pw));
    }
  }
  return ret;
}

int ObSelectLogPlan::get_largest_sharding_child(const ObIArray<ObLogicalOperator*> &child_ops,
                                                const int64_t candi_pos,
                                                ObLogicalOperator *&largest_op)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *child_op = NULL;
  ObShardingInfo *child_sharding = NULL;
  largest_op = NULL;
  if (OB_INVALID_INDEX != candi_pos) {
    if (candi_pos < 0 || candi_pos >= child_ops.count()) {
      /*do nothing*/
    } else if (OB_ISNULL(child_op = child_ops.at(candi_pos)) ||
               OB_ISNULL(child_sharding = child_op->get_sharding())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(child_op), K(child_sharding), K(ret));
    } else if (child_sharding->is_distributed()) {
      largest_op = child_op;
    }
  } else {
    double largest_card = -1.0;
    for (int64_t i = 0; OB_SUCC(ret) && i < child_ops.count(); i++) {
      if (OB_ISNULL(child_op = child_ops.at(i)) ||
          OB_ISNULL(child_sharding = child_op->get_sharding())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(child_op), K(child_sharding), K(ret));
      } else if (child_op->get_parallel() <= ObGlobalHint::DEFAULT_PARALLEL) {
        /* choose a parallel child */
      } else if (child_sharding->is_distributed() && child_op->get_card() > largest_card) {
        largest_op = child_op;
        largest_card = child_op->get_card();
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

int ObSelectLogPlan::allocate_union_all_as_top(const ObIArray<ObLogicalOperator*> &child_ops,
                                               DistAlgo dist_set_method,
                                               ObLogicalOperator *&top)
{
  int ret = OB_SUCCESS;
  ObLogSet *set_op = NULL;
  if (OB_ISNULL((set_op = static_cast<ObLogSet*>(get_log_op_factory().allocate(*this, LOG_SET))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("Allocate memory for ObLogSet failed", K(ret));
  } else {
    set_op->assign_set_distinct(false);
    set_op->assign_set_op(ObSelectStmt::UNION);
    set_op->set_algo_type(SetAlgo::MERGE_SET);
    set_op->set_distributed_algo(dist_set_method);
    if (OB_FAIL(set_op->add_child(child_ops))) {
      LOG_WARN("failed to add child ops", K(ret));
    } else if (OB_FAIL(set_op->compute_property())) {
      LOG_WARN("failed to compute property", K(ret));
    } else {
      top = set_op;
    }
  }
  return ret;
}

// 将多元 union 拆分为 union all 和 hash distinct, 这里分配 hash distinct, 并调整估行
int ObSelectLogPlan::allocate_set_distinct_as_top(ObLogicalOperator *&top)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 8> distinct_exprs;
  ObLogicalOperator *distinct_op = NULL;
  if (OB_ISNULL(get_stmt()) || OB_ISNULL(top)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(get_stmt()->get_select_exprs(distinct_exprs))) {
    LOG_WARN("failed to get current best plan", K(ret));
  } else if (OB_ISNULL(distinct_op = get_log_op_factory().allocate(*this, LOG_DISTINCT))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to allocate distinct operator", K(ret));
  } else {
    ObLogDistinct *distinct = static_cast<ObLogDistinct*>(distinct_op);
    distinct->set_child(ObLogicalOperator::first_child, top);
    distinct->set_algo_type(HASH_AGGREGATE);
    if (OB_FAIL(distinct->set_distinct_exprs(distinct_exprs))) {
      LOG_WARN("failed to set group by columns", K(ret));
    } else if (OB_FAIL(distinct->compute_property())) {
      LOG_WARN("failed to compute property", K(ret));
    } else {
      double distinct_cost = 0.0;
      distinct->set_card(top->get_card());
      distinct_cost = ObOptEstCost::cost_hash_distinct(top->get_card(),
                                                       top->get_card(),
                                                       top->get_width(),
                                                       distinct_exprs,
                                                       get_optimizer_context().get_cost_model_type());
      distinct->set_cost(top->get_cost() + distinct_cost);
      distinct->set_op_cost(distinct_cost);
      top = distinct_op;
    }
  }
  return ret;
}

int ObSelectLogPlan::candi_allocate_recursive_union_all(const ObIArray<ObSelectLogPlan*> &child_plans)
{
  int ret = OB_SUCCESS;
  ObSEArray<CandidatePlan, 8> all_plans;
  ObSelectLogPlan *left_plan = NULL;
  ObSelectLogPlan *right_plan = NULL;
  ObSEArray<CandidatePlan, 8> left_best_plans;
  ObSEArray<CandidatePlan, 8> right_best_plans;
  const ObSelectStmt *select_stmt = NULL;
  ObSEArray<OrderItem, 8> candi_order_items;
  if (OB_UNLIKELY(2 != child_plans.count()) || OB_ISNULL(left_plan = child_plans.at(0)) ||
      OB_ISNULL(right_plan = child_plans.at(1)) || OB_ISNULL(select_stmt = get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(child_plans.count()), K(left_plan), K(right_plan),
        K(select_stmt), K(ret));
  } else if (OB_FAIL(left_plan->decide_sort_keys_for_runion(select_stmt->get_search_by_items(),
                                                            candi_order_items))) {
    LOG_WARN("failed to allocate sort as root", K(ret));
  } else if (OB_FAIL(get_minimal_cost_candidates(left_plan->get_candidate_plans().candidate_plans_,
                                                 left_best_plans))) {
    LOG_WARN("failed to get minimal cost candidates", K(ret));
  } else if (OB_FAIL(get_minimal_cost_candidates(right_plan->get_candidate_plans().candidate_plans_,
                                                 right_best_plans))) {
    LOG_WARN("failed to get minimal cost candidates", K(ret));
  } else if (OB_FAIL(create_recursive_union_all_plan(left_best_plans,
                                                     right_best_plans,
                                                     candi_order_items,
                                                     false,
                                                     all_plans))) {
    LOG_WARN("failed to create recursive union all plan", K(ret));
  } else if (!all_plans.empty()) {
    LOG_TRACE("succeed to generate set plans using hint", K(all_plans.count()));
  } else if (OB_FAIL(get_log_plan_hint().check_status())) {
    LOG_WARN("failed to generate plans with hint", K(ret));
  } else if (OB_FAIL(create_recursive_union_all_plan(left_best_plans,
                                                     right_best_plans,
                                                     candi_order_items,
                                                     true,
                                                     all_plans))) {
    LOG_WARN("failed to create recursive union all plan", K(ret));
  } else {
    LOG_TRACE("succeed to generate set plans ignore hint", K(all_plans.count()));
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

int ObSelectLogPlan::create_recursive_union_all_plan(ObIArray<CandidatePlan> &left_best_plans,
                                                     ObIArray<CandidatePlan> &right_best_plans,
                                                     const ObIArray<OrderItem> &order_items,
                                                     const bool ignore_hint,
                                                     ObIArray<CandidatePlan> &all_plans)
{
  int ret = OB_SUCCESS;
  CandidatePlan candidate_plan;
  for (int64_t i = 0; OB_SUCC(ret) && i < left_best_plans.count(); i++) {
    for (int64_t j = 0; OB_SUCC(ret) && j < right_best_plans.count(); j++) {
      if (OB_FAIL(create_recursive_union_all_plan(left_best_plans.at(i).plan_tree_,
                                                  right_best_plans.at(j).plan_tree_,
                                                  order_items,
                                                  ignore_hint,
                                                  candidate_plan.plan_tree_))) {
        LOG_WARN("failed to create recursive union all plan", K(ret));
      } else if (NULL == candidate_plan.plan_tree_) {
        /*do nothing*/
      } else if (OB_FAIL(all_plans.push_back(candidate_plan))) {
        LOG_WARN("failed to push back candidate plan", K(ret));
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

int ObSelectLogPlan::create_recursive_union_all_plan(ObLogicalOperator *left_child,
                                                     ObLogicalOperator *right_child,
                                                     const ObIArray<OrderItem> &order_items,
                                                     const bool ignore_hint,
                                                     ObLogicalOperator *&top)
{
  int ret = OB_SUCCESS;
  bool is_basic = false;
  bool need_sort = false;
  int64_t prefix_pos = 0;
  ObLogPlan *left_log_plan = NULL;
  ObSEArray<ObLogicalOperator*, 2> child_ops;
  DistAlgo dist_set_method = DistAlgo::DIST_INVALID_METHOD;
  ObExchangeInfo left_exch_info;
  left_exch_info.dist_method_ = ObPQDistributeMethod::NONE;
  top = NULL;
  uint64_t set_dist_methods = DistAlgo::DIST_BASIC_METHOD | DistAlgo::DIST_PULL_TO_LOCAL;
  DistAlgo hint_dist_methods = get_log_plan_hint().get_valid_set_dist_algo();
  if (!ignore_hint && DistAlgo::DIST_INVALID_METHOD != hint_dist_methods) {
    set_dist_methods &= hint_dist_methods;
  }
  if (OB_ISNULL(left_child) || OB_ISNULL(right_child) ||
      OB_ISNULL(left_log_plan = left_child->get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(left_child), K(right_child), K(left_log_plan), K(ret));
  } else if (OB_FAIL(child_ops.push_back(left_child)) ||
             OB_FAIL(child_ops.push_back(right_child))) {
    LOG_WARN("failed to push back operator", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::check_basic_sharding_info(get_optimizer_context().get_local_server_addr(),
                                                                child_ops,
                                                                is_basic))) {
    LOG_WARN("failed to check basic sharding info", K(ret));
  } else if (is_basic) {
    if (DistAlgo::DIST_BASIC_METHOD & set_dist_methods) {
      dist_set_method = DistAlgo::DIST_BASIC_METHOD;
    }
  } else if (DistAlgo::DIST_PULL_TO_LOCAL & set_dist_methods) {
    // pull to local
    dist_set_method = DistAlgo::DIST_PULL_TO_LOCAL;
    if (left_child->is_sharding()) {
      left_exch_info.dist_method_ = ObPQDistributeMethod::LOCAL;
    }
  }
  if (OB_SUCC(ret) && DistAlgo::DIST_INVALID_METHOD != dist_set_method) {
    if (OB_FAIL(ObOptimizerUtil::check_need_sort(order_items,
                                                 left_child->get_op_ordering(),
                                                 left_child->get_fd_item_set(),
                                                 left_child->get_output_equal_sets(),
                                                 left_child->get_output_const_exprs(),
                                                 get_onetime_query_refs(),
                                                 left_child->get_is_at_most_one_row(),
                                                 need_sort,
                                                 prefix_pos))) {
      LOG_WARN("failed to check need sort", K(ret));
    } else if (OB_FAIL(left_log_plan->allocate_sort_and_exchange_as_top(left_child,
                                                              left_exch_info,
                                                              order_items,
                                                              need_sort,
                                                              prefix_pos,
                                                              left_child->get_is_local_order()))) {
      LOG_WARN("failed to allocate operator for join style op", K(ret));
    } else if (OB_FAIL(allocate_recursive_union_all_as_top(left_child, right_child, dist_set_method, top))) {
      LOG_WARN("failed to allocate recursive union all as top", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObSelectLogPlan::allocate_recursive_union_all_as_top(ObLogicalOperator *left_child,
                                                         ObLogicalOperator *right_child,
                                                         DistAlgo dist_set_method,
                                                         ObLogicalOperator *&top)
{
  int ret = OB_SUCCESS;
  ObLogSet *set_op = NULL;
  const ObSelectStmt *select_stmt = NULL;
  top = NULL;
  if (OB_ISNULL(select_stmt = get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_ISNULL((set_op = static_cast<ObLogSet*>(
                                 get_log_op_factory().allocate(*this, LOG_SET))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("Allocate memory for ObLogSet failed", K(ret));
  } else {
    set_op->set_left_child(left_child);
    set_op->set_right_child(right_child);
    set_op->assign_set_distinct(false);
    set_op->assign_set_op(select_stmt->get_set_op());
    set_op->set_algo_type(MERGE_SET);
    set_op->set_distributed_algo(dist_set_method);
    set_op->set_recursive_union(true);
    set_op->set_is_breadth_search(select_stmt->is_breadth_search());
    if (OB_FAIL(set_op->set_search_ordering(select_stmt->get_search_by_items()))) {
      LOG_WARN("set search order failed", K(ret));
    } else if (OB_FAIL(set_op->set_cycle_items(select_stmt->get_cycle_items()))) {
      LOG_WARN("set cycle item failed", K(ret));
    } else if (OB_FAIL(set_op->compute_property())) {
      LOG_WARN("failed to compute property", K(ret));
    } else {
      top = set_op;
    }
  }
  return ret;
}

int ObSelectLogPlan::candi_allocate_distinct_set(const ObIArray<ObSelectLogPlan*> &child_plans)
{
  int ret = OB_SUCCESS;
  EqualSets equal_sets;
  ObSelectLogPlan *left_plan = NULL;
  ObSelectLogPlan *right_plan = NULL;
  const ObSelectStmt *left_stmt = NULL;
  const ObSelectStmt *right_stmt = NULL;
  const ObSelectStmt *current_stmt = NULL;
  ObLogicalOperator *left_best_plan = NULL;
  ObLogicalOperator *right_best_plan = NULL;
  ObSEArray<ObRawExpr*, 4> left_select_exprs;
  ObSEArray<ObRawExpr*, 4> right_select_exprs;
  ObSEArray<CandidatePlan, 2> hash_set_plans;
  ObSEArray<CandidatePlan, 4> merge_set_plans;
  ObSEArray<CandidatePlan, 4> all_plans;
  ObSEArray<CandidatePlan, 4> left_best_plans;
  ObSEArray<CandidatePlan, 4> right_best_plans;
  typedef ObSEArray<ObSEArray<CandidatePlan, 16>, 4> CandidatePlanArray;
  const SetAlgo set_algo = get_log_plan_hint().get_valid_set_algo();
  SMART_VARS_2((CandidatePlanArray, left_candidate_list),
               (CandidatePlanArray, right_candidate_list)) {
    int64_t check_scope = OrderingCheckScope::CHECK_SET | OrderingCheckScope::CHECK_ORDERBY;
    if (OB_ISNULL(current_stmt = get_stmt()) || OB_UNLIKELY(2 != child_plans.count()) ||
        OB_ISNULL(left_plan = child_plans.at(0)) || OB_ISNULL(right_plan = child_plans.at(1)) ||
        OB_ISNULL(left_stmt = left_plan->get_stmt()) ||
        OB_ISNULL(right_stmt = right_plan->get_stmt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected stmt type", K(current_stmt), K(left_plan), K(right_plan),
          K(left_stmt), K(right_stmt), K(ret));
    } else if (OB_FAIL(classify_candidates_based_on_sharding(left_plan->candidates_.candidate_plans_,
                                                             left_candidate_list))) {
      LOG_WARN("failed to classify candidates based on sharding", K(ret));
    } else if (OB_FAIL(classify_candidates_based_on_sharding(right_plan->candidates_.candidate_plans_,
                                                             right_candidate_list))) {
      LOG_WARN("failed to classify candidates based on sharding", K(ret));
    } else if (OB_FAIL(get_minimal_cost_candidates(left_candidate_list,
                                                  left_best_plans))) {
      LOG_WARN("failed to get minimal cost candidates", K(ret));
    } else if (OB_FAIL(get_minimal_cost_candidates(right_candidate_list,
                                                  right_best_plans))) {
      LOG_WARN("failed to get minimal cost candidates", K(ret));
    } else if (OB_FAIL(left_stmt->get_select_exprs(left_select_exprs))) {
      LOG_WARN("failed to get select exprs", K(ret));
    } else if (OB_FAIL(right_stmt->get_select_exprs(right_select_exprs))) {
      LOG_WARN("failed to get select exprs", K(ret));
    } else if (OB_FAIL(left_plan->get_candidate_plans().get_best_plan(left_best_plan))) {
      LOG_WARN("failed to get best plan", K(ret));
    } else if (OB_FAIL(right_plan->get_candidate_plans().get_best_plan(right_best_plan))) {
      LOG_WARN("failed to get best plan", K(ret));
    } else if (OB_ISNULL(left_best_plan) || OB_ISNULL(right_best_plan)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(left_best_plan), K(right_best_plan), K(ret));
    } else if (OB_FAIL(append(equal_sets, left_best_plan->get_output_equal_sets()))) {
      LOG_WARN("failed to append equal sets", K(ret));
    } else if (OB_FAIL(append(equal_sets, right_best_plan->get_output_equal_sets()))) {
      LOG_WARN("failed to append equal sets", K(ret));
    } else if (MERGE_SET != set_algo &&
               OB_FAIL(generate_hash_set_plans(equal_sets,
                                              left_select_exprs,
                                              right_select_exprs,
                                              current_stmt->get_set_op(),
                                              left_best_plans,
                                              right_best_plans,
                                              false,
                                              hash_set_plans))) {
      // generate hash set plans use hint
      LOG_WARN("failed to generate hash set plans", K(ret));
    } else if (HASH_SET != set_algo &&
               OB_FAIL(generate_merge_set_plans(equal_sets,
                                                left_select_exprs,
                                                right_select_exprs,
                                                current_stmt->get_set_op(),
                                                left_candidate_list,
                                                right_candidate_list,
                                                false,
                                                hash_set_plans.empty(),
                                                merge_set_plans))) {
      // generate merge set plans use hint
      LOG_WARN("failed to generate merge set plans", K(ret));
    } else if (!hash_set_plans.empty() || !merge_set_plans.empty()) {
      OPT_TRACE("succeed to generate set plans using hint");
      OPT_TRACE("   hash set plan count:", hash_set_plans.count());
      OPT_TRACE("   merge set plan count:", merge_set_plans.count());
      LOG_TRACE("succeed to generate set plans using hint", K(set_algo), K(hash_set_plans.count()), K(merge_set_plans.count()));
    } else if (OB_FAIL(get_log_plan_hint().check_status())) {
      LOG_WARN("failed to generate plans with hint", K(ret));
    } else if (OB_FAIL(generate_hash_set_plans(equal_sets,
                                              left_select_exprs,
                                              right_select_exprs,
                                              current_stmt->get_set_op(),
                                              left_best_plans,
                                              right_best_plans,
                                              true,
                                              hash_set_plans))) {
      // generate hash set plans ignore hint
      LOG_WARN("failed to generate hash set plans", K(ret));
    } else if (OB_FAIL(generate_merge_set_plans(equal_sets,
                                                left_select_exprs,
                                                right_select_exprs,
                                                current_stmt->get_set_op(),
                                                left_candidate_list,
                                                right_candidate_list,
                                                true,
                                                hash_set_plans.empty(),
                                                merge_set_plans))) {
      // generate merge set plans ignore hint
      LOG_WARN("failed to generate merge set plans", K(ret));
    } else {
      OPT_TRACE("succeed to generate set plans ignore hint");
      OPT_TRACE("   hash set plan count:", hash_set_plans.count());
      OPT_TRACE("   merge set plan count:", merge_set_plans.count());
      LOG_TRACE("succeed to generate set plans ignore hint", K(set_algo), K(hash_set_plans.count()),
                                                             K(merge_set_plans.count()));
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(append(all_plans, merge_set_plans))) {
      LOG_WARN("failed to append plans", K(ret));
    } else if (OB_FAIL(append(all_plans, hash_set_plans))) {
      LOG_WARN("failed to append plans", K(ret));
    } else if (OB_FAIL(update_plans_interesting_order_info(all_plans, check_scope))) {
      LOG_WARN("failed to update plans interesting order info", K(ret));
    } else if (OB_FAIL(prune_and_keep_best_plans(all_plans))) {
      LOG_WARN("failed to add all plans", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObSelectLogPlan::get_distributed_set_methods(const EqualSets &equal_sets,
                                                 const ObIArray<ObRawExpr*> &left_set_keys,
                                                 const ObIArray<ObRawExpr*> &right_set_keys,
                                                 const ObSelectStmt::SetOperator set_op,
                                                 const SetAlgo set_method,
                                                 ObLogicalOperator &left_child,
                                                 ObLogicalOperator &right_child,
                                                 const bool ignore_hint,
                                                 int64_t &set_dist_methods)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObLogicalOperator*, 2> child_ops;
  ObShardingInfo *left_sharding = NULL;
  ObShardingInfo *right_sharding = NULL;
  const ObSelectStmt *select_stmt = NULL;
  bool need_pull_to_local = false;
  set_dist_methods = ignore_hint ? DistAlgo::DIST_INVALID_METHOD
                                 : get_log_plan_hint().get_valid_set_dist_algo();
  if (OB_ISNULL(left_sharding = left_child.get_sharding()) ||
      OB_ISNULL(right_sharding = right_child.get_sharding()) ||
      OB_ISNULL(select_stmt = get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid sharding", K(left_sharding), K(right_sharding), K(ret), K(left_child.get_type()), K(left_child.get_type()), K(get_op_name(left_child.get_type())));
  } else if (OB_FAIL(child_ops.push_back(&left_child)) ||
      OB_FAIL(child_ops.push_back(&right_child))) {
    LOG_WARN("failed to push back child info", K(ret));
  } else if (DistAlgo::DIST_INVALID_METHOD == set_dist_methods) {
    set_dist_methods |= DistAlgo::DIST_BASIC_METHOD;
    set_dist_methods |= DistAlgo::DIST_PARTITION_WISE;
    set_dist_methods |= DistAlgo::DIST_NONE_ALL;
    set_dist_methods |= DistAlgo::DIST_ALL_NONE;
    if (MERGE_SET != set_method) {
      // disable dist algo for merge set except basic & pwj
      set_dist_methods |= DistAlgo::DIST_NONE_PARTITION;
      set_dist_methods |= DistAlgo::DIST_NONE_HASH;
      set_dist_methods |= DistAlgo::DIST_PARTITION_NONE;
      set_dist_methods |= DistAlgo::DIST_HASH_NONE;
      set_dist_methods |= DistAlgo::DIST_PULL_TO_LOCAL;
      if (left_child.get_parallel() > 1 || right_child.get_parallel() > 1) {
       set_dist_methods |= DistAlgo::DIST_HASH_HASH;
       OPT_TRACE("candi hash set dist method:basic,partition wise, none all, all none, none partition,partition none,pull to local,hash hash");
      } else {
        OPT_TRACE("candi hash set dist method:basic,partition wise, none all, all none, none partition,partition none,pull to local");
      }
    } else {
      OPT_TRACE("candi merge set dist method:basic, partition wise,  none all, all none, ");
    }
  } else {
    OPT_TRACE("use dist method with hint");
  }
  if (OB_SUCC(ret) && !get_optimizer_context().is_var_assign_only_in_root_stmt() &&
      get_optimizer_context().has_var_assign()) {
    set_dist_methods &= DIST_PULL_TO_LOCAL | DIST_BASIC_METHOD;
  }
  if (OB_SUCC(ret) && (set_dist_methods & DistAlgo::DIST_NONE_ALL)) {
    if (left_sharding->is_distributed() && right_sharding->is_match_all() &&
        !right_child.get_contains_das_op() && !right_child.get_contains_fake_cte() &&
        ObSelectStmt::UNION != set_op) {
      set_dist_methods = DistAlgo::DIST_NONE_ALL;
    } else {
      set_dist_methods &= ~DistAlgo::DIST_NONE_ALL;
    }
  }

  if (OB_SUCC(ret) && (set_dist_methods & DistAlgo::DIST_ALL_NONE)) {
    if (right_sharding->is_distributed() && left_sharding->is_match_all() &&
        !left_child.get_contains_das_op() && !left_child.get_contains_fake_cte() &&
        ObSelectStmt::UNION != set_op) {
      set_dist_methods = DistAlgo::DIST_ALL_NONE;
    } else {
      set_dist_methods &= ~DistAlgo::DIST_ALL_NONE;
    }
  }

  if (OB_SUCC(ret) && (set_dist_methods & DistAlgo::DIST_BASIC_METHOD)) {
    OPT_TRACE("check basic method");
    bool is_basic = false;
    ObAddr &local_addr = get_optimizer_context().get_local_server_addr();
    if (OB_FAIL(ObOptimizerUtil::check_basic_sharding_info(local_addr, child_ops, is_basic))) {
      LOG_WARN("failed to check basic sharding info", K(ret));
    } else if (is_basic) {
      set_dist_methods = DistAlgo::DIST_BASIC_METHOD;
      OPT_TRACE("plan will use basic method");
    } else {
      set_dist_methods &= ~DistAlgo::DIST_BASIC_METHOD;
      OPT_TRACE("plan will not use basic method");
    }
  }

  if (OB_SUCC(ret) && (set_dist_methods & DistAlgo::DIST_PARTITION_WISE)) {
    OPT_TRACE("check partition wise method");
    bool is_partition_wise = false;
    if (OB_FAIL(ObShardingInfo::check_if_match_partition_wise(equal_sets,
                                                              left_set_keys,
                                                              right_set_keys,
                                                              left_child.get_strong_sharding(),
                                                              right_child.get_strong_sharding(),
                                                              is_partition_wise))) {
      LOG_WARN("failed to check if match partition wise join", K(ret));
    } else if (is_partition_wise) {
      if (left_child.is_exchange_allocated() == right_child.is_exchange_allocated()) {
        set_dist_methods = DistAlgo::DIST_PARTITION_WISE;
        OPT_TRACE("plan will use partition wise method and prune other method");
      }
    } else {
      set_dist_methods &= ~DistAlgo::DIST_PARTITION_WISE;
      OPT_TRACE("plan will not use partition wise method");
    }
  }

  if (OB_SUCC(ret) && (set_dist_methods & DistAlgo::DIST_EXT_PARTITION_WISE)) {
    OPT_TRACE("check extended partition wise method");
    bool is_ext_partition_wise = false;
    if (!left_sharding->is_distributed_without_table_location_with_partitioning() ||
        !ObShardingInfo::is_shuffled_server_list(left_child.get_server_list()) ||
        !right_sharding->is_distributed_without_table_location_with_partitioning() ||
        !ObShardingInfo::is_shuffled_server_list(right_child.get_server_list())) {
      set_dist_methods &= ~DistAlgo::DIST_EXT_PARTITION_WISE;
      OPT_TRACE("sharding or exchange is not expected, plan will not use extended partition wise method");
    } else if (OB_FAIL(ObShardingInfo::check_if_match_extended_partition_wise(equal_sets,
                                                                       left_child.get_server_list(),
                                                                       right_child.get_server_list(),
                                                                       left_set_keys,
                                                                       right_set_keys,
                                                                       left_child.get_strong_sharding(),
                                                                       right_child.get_strong_sharding(),
                                                                       is_ext_partition_wise))) {
      LOG_WARN("failed to check if match partition wise join", K(ret));
    } else if (is_ext_partition_wise) {
      set_dist_methods = DistAlgo::DIST_EXT_PARTITION_WISE;
      OPT_TRACE("plan will use extended partition wise method and prune other method");
    } else {
      set_dist_methods &= ~DistAlgo::DIST_EXT_PARTITION_WISE;
      OPT_TRACE("plan will not use extended partition wise method");
    }
  }

  if (OB_SUCC(ret) && (set_dist_methods & DistAlgo::DIST_PARTITION_NONE)) {
    OPT_TRACE("check partition none method");
    bool left_match_repart = false;
    if (OB_FAIL(check_if_set_match_repart(equal_sets,
                                          left_set_keys,
                                          right_set_keys,
                                          right_child,
                                          left_match_repart))) {
      LOG_WARN("failed to check if match repart", K(ret));
    } else if (!left_match_repart) {
      set_dist_methods &= ~DistAlgo::DIST_PARTITION_NONE;
      OPT_TRACE("plan will not use partition none method");
    } else {
      need_pull_to_local = false;
      set_dist_methods &= ~DistAlgo::DIST_HASH_NONE;
      OPT_TRACE("plan will use partition none method");
    }
  }

  if (OB_SUCC(ret) && (set_dist_methods & DistAlgo::DIST_HASH_NONE)) {
    OPT_TRACE("check hash none method");
    bool is_match_left_side_hash = false;
    if (OB_FAIL(check_if_set_match_rehash(equal_sets,
                                          left_set_keys,
                                          right_set_keys,
                                          right_child,
                                          is_match_left_side_hash))) {
      LOG_WARN("failed to check if match left side hash", K(ret));
    } else if (!is_match_left_side_hash) {
      set_dist_methods &= ~DistAlgo::DIST_HASH_NONE;
      OPT_TRACE("plan will not use hash none method");
    } else {
      OPT_TRACE("plan will use hash none method");
    }
  }

  if (OB_SUCC(ret) && (set_dist_methods & DistAlgo::DIST_NONE_PARTITION)) {
    OPT_TRACE("check none partition method");
    bool right_match_repart = false;
    if (OB_FAIL(check_if_set_match_repart(equal_sets,
                                                  right_set_keys,
                                                  left_set_keys,
                                                  left_child,
                                                  right_match_repart))) {
      LOG_WARN("failed to check_and_extract_repart_info", K(ret));
    } else if (!right_match_repart) {
      set_dist_methods &= ~DistAlgo::DIST_NONE_PARTITION;
      OPT_TRACE("plan will not use none partition method");
    } else {
      need_pull_to_local = false;
      set_dist_methods &= ~DistAlgo::DIST_NONE_HASH;
      OPT_TRACE("plan will use none partition method");
    }
  }

  if (OB_SUCC(ret) && (set_dist_methods & DistAlgo::DIST_NONE_HASH)) {
    OPT_TRACE("check none hash method");
    bool is_match_right_side_hash = false;
    if (OB_FAIL(check_if_set_match_rehash(equal_sets,
                                          left_set_keys,
                                          right_set_keys,
                                          left_child,
                                          is_match_right_side_hash))) {
      LOG_WARN("failed to check if match left side hash", K(ret));
    } else if (!is_match_right_side_hash) {
      set_dist_methods &= ~DistAlgo::DIST_NONE_HASH;
      OPT_TRACE("plan will not use none hash method");
    } else {
      OPT_TRACE("plan will use none hash method");
    }
  }

  if (OB_SUCC(ret) && (set_dist_methods & DIST_PULL_TO_LOCAL) && !need_pull_to_local) {
    if ((set_dist_methods & DIST_PARTITION_NONE) ||
        (set_dist_methods & DIST_NONE_PARTITION) ||
        (set_dist_methods & DIST_HASH_NONE) ||
        (set_dist_methods & DIST_NONE_HASH) ||
        (set_dist_methods & DIST_HASH_HASH)) {
      set_dist_methods &= ~DIST_PULL_TO_LOCAL;
      OPT_TRACE("plan will not use pull to local");
    }
  }
  return ret;
}

bool ObSelectLogPlan::is_set_partition_wise_valid(const ObLogicalOperator &left_plan,
                                                  const ObLogicalOperator &right_plan)
{
  bool is_valid = true;
  if ((left_plan.is_exchange_allocated() || right_plan.is_exchange_allocated()) &&
      (left_plan.get_contains_pw_merge_op() || right_plan.get_contains_pw_merge_op())) {
    is_valid = false;
  } else { /*do nothing*/ }
  return is_valid;
}


bool ObSelectLogPlan::is_set_repart_valid(const ObLogicalOperator &left_plan,
                                      const ObLogicalOperator &right_plan,
                                      const DistAlgo dist_algo)
{
  bool is_valid = true;
  if (DistAlgo::DIST_PARTITION_NONE == dist_algo && right_plan.get_contains_pw_merge_op()) {
    is_valid = true;
  } else if (DistAlgo::DIST_NONE_PARTITION == dist_algo && left_plan.get_contains_pw_merge_op()) {
    is_valid = true;
  } else {
    is_valid = true;
  }
  return is_valid;
}

int ObSelectLogPlan::check_if_set_match_repart(const EqualSets &equal_sets,
                                               const ObIArray<ObRawExpr *> &src_join_keys,
                                               const ObIArray<ObRawExpr *> &target_join_keys,
                                               const ObLogicalOperator &target_child,
                                               bool &is_match_repart)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 8> target_part_keys;
  ObShardingInfo *target_sharding = NULL;
  bool is_base_table_scan = false;
  is_match_repart = false;
  if (NULL == target_child.get_strong_sharding()) {
    /* do nothing */
  } else if (OB_ISNULL(target_sharding = target_child.get_sharding())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (!target_child.is_distributed() || NULL == target_sharding->get_phy_table_location_info()) {
    /* do nothing */
  } else if (OB_FAIL(target_sharding->get_all_partition_keys(target_part_keys, true))) {
    LOG_WARN("failed to get partition keys", K(ret));
  } else if (target_part_keys.empty()) {
    /*do nothing*/
  } else if (OB_FAIL(ObShardingInfo::check_if_match_repart_or_rehash(equal_sets,
                                                                    src_join_keys,
                                                                    target_join_keys,
                                                                    target_part_keys,
                                                                    is_match_repart))) {
    LOG_WARN("failed to check if match repartition", K(ret));
  } else {
    LOG_TRACE("succeed to check whether matching repartition", K(is_match_repart));
  }
  return ret;
}

int ObSelectLogPlan::check_if_set_match_rehash(const EqualSets &equal_sets,
                                              const ObIArray<ObRawExpr *> &src_join_keys,
                                              const ObIArray<ObRawExpr *> &target_join_keys,
                                              const ObLogicalOperator &target_child,
                                              bool &is_match_single_side_hash)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 8> target_part_keys;
  ObShardingInfo *target_sharding = NULL;
  is_match_single_side_hash = false;
  if (NULL == target_child.get_strong_sharding()) {
    /* do nothing */
  } else if (OB_ISNULL(target_sharding = target_child.get_sharding())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (!target_sharding->is_distributed_without_table_location() ||
             !ObShardingInfo::is_shuffled_server_list(target_child.get_server_list())) {
    /* do nothing */
  } else if (OB_FAIL(target_sharding->get_all_partition_keys(target_part_keys, true))) {
    LOG_WARN("failed to get partition keys", K(ret));
  } else if (target_part_keys.empty()) {
    /* do nothing */
  } else if (OB_FAIL(ObShardingInfo::check_if_match_repart_or_rehash(equal_sets,
                                                                    src_join_keys,
                                                                    target_join_keys,
                                                                    target_part_keys,
                                                                    is_match_single_side_hash))) {
    LOG_WARN("failed to check if set match single side hash", K(ret));
  } else {
    LOG_TRACE("succeed to check whether set matching single side hash", K(is_match_single_side_hash));
  }
  return ret;
}

int ObSelectLogPlan::generate_merge_set_plans(const EqualSets &equal_sets,
                                              const ObIArray<ObRawExpr*> &left_set_keys,
                                              const ObIArray<ObRawExpr*> &right_set_keys,
                                              const ObSelectStmt::SetOperator set_op,
                                              ObIArray<ObSEArray<CandidatePlan, 16>> &left_candidate_list,
                                              ObIArray<ObSEArray<CandidatePlan, 16>> &right_candidate_list,
                                              const bool ignore_hint,
                                              const bool no_hash_plans,
                                              ObIArray<CandidatePlan> &merge_set_plans)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObSEArray<MergeKeyInfo*, 8> merge_key_list;
  ObSEArray<ObSEArray<MergeKeyInfo*, 8>, 8> left_merge_keys;
  ObSEArray<ObSEArray<MergeKeyInfo*, 8>, 8> right_merge_keys;
  bool force_merge = MERGE_SET == get_log_plan_hint().get_valid_set_algo();
  //if can_ignore_merge_plan = true, need to check sort_order for merge plan
  bool can_ignore_merge_plan = ((ignore_hint && !no_hash_plans) || (!ignore_hint && !force_merge));
  OPT_TRACE("start generate merge set plans");
  bool no_swap = false;
  bool swap = false;
  if (OB_FAIL(get_allowed_branch_order(ignore_hint, set_op, no_swap, swap))) {
    LOG_WARN("failed to get allowed branch order", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < left_candidate_list.count(); i++) {
    merge_key_list.reuse();
    if (OB_FAIL(init_merge_set_structure(allocator,
                                         left_candidate_list.at(i),
                                         left_set_keys,
                                         merge_key_list,
                                         can_ignore_merge_plan))) {
      LOG_WARN("failed to initialize merge key", K(ret));
    } else if (OB_FAIL(left_merge_keys.push_back(merge_key_list))) {
      LOG_WARN("failed to push back merge keys", K(ret));
    } else { /*do nothing*/ }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < right_candidate_list.count(); i++) {
    merge_key_list.reuse();
    if (OB_FAIL(init_merge_set_structure(allocator,
                                         right_candidate_list.at(i),
                                         right_set_keys,
                                         merge_key_list,
                                         can_ignore_merge_plan))) {
      LOG_WARN("failed to init merge key", K(ret));
    } else if (OB_FAIL(right_merge_keys.push_back(merge_key_list))) {
      LOG_WARN("failed to push back merge keys", K(ret));
    } else { /*do nothing*/ }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < left_candidate_list.count(); i++) {
    for (int64_t j = 0; OB_SUCC(ret) && j < right_candidate_list.count(); j++) {
      if (no_swap && OB_FAIL(inner_generate_merge_set_plans(equal_sets,
                                                            left_set_keys,
                                                            right_set_keys,
                                                            left_merge_keys.at(i),
                                                            set_op,
                                                            left_candidate_list.at(i),
                                                            right_candidate_list.at(j),
                                                            ignore_hint,
                                                            can_ignore_merge_plan,
                                                            no_hash_plans,
                                                            merge_set_plans))) {
        LOG_WARN("failed to generate merge set plans", K(ret));
      } else if (swap && OB_FAIL(inner_generate_merge_set_plans(equal_sets,
                                                                right_set_keys,
                                                                left_set_keys,
                                                                right_merge_keys.at(j),
                                                                set_op,
                                                                right_candidate_list.at(j),
                                                                left_candidate_list.at(i),
                                                                ignore_hint,
                                                                can_ignore_merge_plan,
                                                                no_hash_plans,
                                                                merge_set_plans))) {
        LOG_WARN("failed to inner generate merge set plans", K(ret));
      } else { /*do nothing*/}
    }
  }
  return ret;
}

int ObSelectLogPlan::inner_generate_merge_set_plans(const EqualSets &equal_sets,
                                                    const ObIArray<ObRawExpr*> &left_set_keys,
                                                    const ObIArray<ObRawExpr*> &right_set_keys,
                                                    const ObIArray<MergeKeyInfo*> &left_merge_keys,
                                                    const ObSelectStmt::SetOperator set_op,
                                                    ObIArray<CandidatePlan> &left_candidates,
                                                    ObIArray<CandidatePlan> &right_candidates,
                                                    const bool ignore_hint,
                                                    const bool can_ignore_merge_plan,
                                                    const bool no_hash_plans,
                                                    ObIArray<CandidatePlan> &merge_set_plans)
{
  int ret = OB_SUCCESS;
  int64_t set_methods = 0;
  int64_t best_prefix_pos = 0;
  bool best_need_sort = false;
  MergeKeyInfo *merge_key = NULL;
  ObSEArray<OrderItem, 4> best_order_items;
  ObLogicalOperator *left_child = NULL;
  ObLogicalOperator *right_child = NULL;
  CandidatePlan candidate_plan;
  if (OB_UNLIKELY(left_candidates.empty()) || OB_UNLIKELY(right_candidates.empty()) ||
      OB_ISNULL(left_child = left_candidates.at(0).plan_tree_) ||
      OB_ISNULL(right_child = right_candidates.at(0).plan_tree_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(left_child), K(right_child), K(ret));
  } else if (OB_FAIL(get_distributed_set_methods(equal_sets,
                                                 left_set_keys,
                                                 right_set_keys,
                                                 set_op,
                                                 MERGE_SET,
                                                 *left_child,
                                                 *right_child,
                                                 ignore_hint,
                                                 set_methods))) {
    LOG_WARN("failed to get distributed set method", K(ret));
  } else if (set_methods == 0) {
    LOG_TRACE("no distributed merge set methods");
  } else {
    LOG_TRACE("distributed merge set methods", K(set_methods));
    for (int64_t i = 0; OB_SUCC(ret) && i < left_candidates.count(); i++) {
      if (OB_ISNULL(left_child = left_candidates.at(i).plan_tree_) ||
          OB_ISNULL(merge_key = left_merge_keys.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(left_child), K(merge_key), K(ret));
      } else if (merge_key->need_sort_ && !merge_key->order_needed_) {
        // when merge_plan can not be ignore, need to check merge_key->need_sort_&merge_key->order_needed_s
        // if no further order needed, not generate merge style plan
        OPT_TRACE("no further order needed, merge set plans is not generated");
        OPT_TRACE("   can_ignore merge plan", can_ignore_merge_plan);
        OPT_TRACE("   merge_key need sort", merge_key->need_sort_);
        OPT_TRACE("   merge_key order is needed", merge_key->order_needed_);
      } else {
        for (int64_t j = DistAlgo::DIST_BASIC_METHOD;
             OB_SUCC(ret) && j <= DistAlgo::DIST_MAX_JOIN_METHOD; j = (j << 1)) {
          if (set_methods & j) {
            DistAlgo dist_algo = get_dist_algo(j);
            const int64_t in_parallel = ObOptimizerUtil::get_join_style_parallel(left_child->get_parallel(),
                                                  right_candidates.at(0).plan_tree_->get_parallel(),
                                                  dist_algo, true);
            if (OB_FAIL(get_minimal_cost_set_plan(in_parallel,
                                                  *left_child,
                                                  *merge_key,
                                                  right_set_keys,
                                                  right_candidates,
                                                  dist_algo,
                                                  best_order_items,
                                                  right_child,
                                                  best_need_sort,
                                                  best_prefix_pos,
                                                  can_ignore_merge_plan))) {
              LOG_WARN("failed to get minimal cost set path", K(ret));
            } else if (NULL == right_child) {
              /*do nothing*/
            } else if (OB_FAIL(create_merge_set_plan(equal_sets,
                                                     left_child,
                                                     right_child,
                                                     left_set_keys,
                                                     right_set_keys,
                                                     set_op,
                                                     dist_algo,
                                                     merge_key->order_directions_,
                                                     merge_key->map_array_,
                                                     merge_key->order_items_,
                                                     merge_key->need_sort_,
                                                     merge_key->prefix_pos_,
                                                     best_order_items,
                                                     best_need_sort,
                                                     best_prefix_pos,
                                                     candidate_plan))) {
              LOG_WARN("failed to create merge set", K(ret));
            } else if (OB_FAIL(merge_set_plans.push_back(candidate_plan))) {
              LOG_WARN("failed to add merge plan", K(ret));
            } else { /*do nothing*/ }
          }
        }
      }
    }
  }
  return ret;
}

int ObSelectLogPlan::get_minimal_cost_set_plan(const int64_t in_parallel,
                                               const ObLogicalOperator &left_child,
                                               const MergeKeyInfo &left_merge_key,
                                               const ObIArray<ObRawExpr*> &right_set_exprs,
                                               const ObIArray<CandidatePlan> &right_candidates,
                                               const DistAlgo set_dist_algo,
                                               ObIArray<OrderItem> &best_order_items,
                                               ObLogicalOperator *&best_plan,
                                               bool &best_need_sort,
                                               int64_t &best_prefix_pos,
                                               const bool can_ignore_merge_plan)
{
  int ret = OB_SUCCESS;
  double best_cost = 0.0;
  double right_path_cost = 0.0;
  int64_t right_prefix_pos = 0;
  bool right_need_sort = false;
  int64_t out_parallel = ObGlobalHint::UNSET_PARALLEL;
  ObSEArray<ObRawExpr*, 8> right_order_exprs;
  ObSEArray<OrderItem, 8> temp_order_items;
  ObSEArray<OrderItem, 8> right_order_items;
  best_plan = NULL;
  best_need_sort = false;
  best_prefix_pos = 0;
  EstimateCostInfo info;
  double right_output_rows = 0.0;
  double right_orig_cost = 0.0;
  for (int64_t i = 0; OB_SUCC(ret) && i < right_candidates.count(); i++) {
    ObLogicalOperator *right_child = NULL;
    ObLogPlan *right_plan = NULL;
    right_order_exprs.reset();
    temp_order_items.reset();
    right_order_items.reset();
    if (OB_ISNULL(right_child = right_candidates.at(i).plan_tree_) ||
        OB_ISNULL(right_plan = right_child->get_plan())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(right_child), K(right_plan), K(ret));
    } else if (DistAlgo::DIST_PARTITION_WISE == set_dist_algo &&
               !is_set_partition_wise_valid(left_child, *right_child)) {
      /*do nothing*/
    } else if (!is_set_repart_valid(left_child, *right_child, set_dist_algo)) {
      /*do nothing*/
    } else if (OB_UNLIKELY(ObGlobalHint::DEFAULT_PARALLEL > (out_parallel = right_child->get_parallel())
                           || ObGlobalHint::DEFAULT_PARALLEL > in_parallel)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected parallel degree", K(ret), K(out_parallel), K(in_parallel));
    } else if (OB_FAIL(ObOptimizerUtil::adjust_exprs_by_mapping(right_set_exprs,
                                                                left_merge_key.map_array_,
                                                                right_order_exprs))) {
      LOG_WARN("failed to adjust exprs by mapping", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::make_sort_keys(right_order_exprs,
                                                       left_merge_key.order_directions_,
                                                       temp_order_items))) {
      LOG_WARN("failed to make sort keys", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::simplify_ordered_exprs(right_child->get_fd_item_set(),
                                                               right_child->get_output_equal_sets(),
                                                               right_child->get_output_const_exprs(),
                                                               get_onetime_query_refs(),
                                                               temp_order_items,
                                                               right_order_items))) {
      LOG_WARN("failed to simplify ordered exprs", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::check_need_sort(right_order_items,
                                                        right_child->get_op_ordering(),
                                                        right_child->get_fd_item_set(),
                                                        right_child->get_output_equal_sets(),
                                                        right_child->get_output_const_exprs(),
                                                        get_onetime_query_refs(),
                                                        right_child->get_is_at_most_one_row(),
                                                        right_need_sort,
                                                        right_prefix_pos))) {
      LOG_WARN("failed to check need sort", K(ret));
    } else if ((DistAlgo::DIST_PARTITION_WISE == set_dist_algo ||
                DistAlgo::DIST_BASIC_METHOD == set_dist_algo) &&
                can_ignore_merge_plan && left_merge_key.need_sort_ && right_need_sort) {
      // when bosh side need sort and merge key order is not needed, do not generer merge set
      OPT_TRACE("bosh side need sort but merge key order is not needed, merge set plans is not generated");
    } else {
      bool is_fully_partition_wise = DistAlgo::DIST_PARTITION_WISE == set_dist_algo &&
                                     !left_child.is_exchange_allocated() && !right_child->is_exchange_allocated();
      bool is_local_order = right_child->get_is_local_order() && !is_fully_partition_wise;
      ObPQDistributeMethod::Type dist_method = ObOptimizerUtil::get_right_dist_method
                                        (*right_child->get_sharding(), set_dist_algo);
      right_plan->get_selectivity_ctx().init_op_ctx(
          &right_child->get_output_equal_sets(), right_child->get_card());
      info.reset();
      // is single, may allocate exchange above, set need_parallel_ as 1 and compute exchange cost in cost_sort_and_exchange
      info.need_parallel_ = right_child->is_single() ? ObGlobalHint::DEFAULT_PARALLEL : in_parallel;
      if (OB_FAIL(right_child->re_est_cost(info, right_output_rows, right_orig_cost))) {
        LOG_WARN("failed to re estimate cost", K(ret));
      } else if (OB_FAIL(ObOptEstCost::cost_sort_and_exchange(&right_plan->get_update_table_metas(),
                                                       &right_plan->get_selectivity_ctx(),
                                                       dist_method,
                                                       right_child->is_distributed(),
                                                       is_local_order,
                                                       right_output_rows,
                                                       right_child->get_width(),
                                                       right_orig_cost,
                                                       out_parallel,
                                                       left_child.get_server_cnt(),
                                                       in_parallel,
                                                       right_order_items,
                                                       right_need_sort,
                                                       right_prefix_pos,
                                                       right_path_cost,
                                                       get_optimizer_context().get_cost_model_type()))) {
        LOG_WARN("failed to compute cost for merge join style op", K(ret));
      } else if (NULL == best_plan || right_path_cost < best_cost) {
        if (OB_FAIL(best_order_items.assign(right_order_items))) {
          LOG_WARN("failed to assign exprs", K(ret));
        } else {
          best_plan = right_child;
          best_need_sort = right_need_sort;
          best_prefix_pos = right_prefix_pos;
          best_cost = right_path_cost;
        }
      }
    }
  }
  return ret;
}

int ObSelectLogPlan::convert_set_order_item(const ObDMLStmt *stmt,
                                             const ObIArray<ObRawExpr*> &select_exprs,
                                             ObIArray<OrderItem> &order_items)
{
  /*
  *the output order_items may have following case
  *  1. select c1,c2,c3 from t1 union select c1,c2,c3 from t2 order by c2,c1,c3
  *  --> full match, the merge sort key could be [c2,c1,c3]
  *  2. select c1,c2,c3 from t1 union select c1,c2,c3 from t2 order by c2,c1+c3,c1,
  *  --> pre match, the merge sort key could use pre match [c2] to adjust its merge sort key as [c2,c1,c3]
  *  3. select c1,c2,c3 from t1 union select c1,c2,c3 from t2 order by c2,c1+c3,c1,
  *  --> no match, set order item not match any, merge sort key directly use it child select expr as [c1,c2,c3]
  */
  int ret = OB_SUCCESS;
  bool found = true;
  ObSEArray<ObRawExpr*, 4> order_exprs;
  ObSEArray<ObOrderDirection, 2> directions;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null pointer", K(ret));
  } else if (OB_UNLIKELY(!(stmt->is_select_stmt() && static_cast<const ObSelectStmt*>(stmt)->is_set_stmt()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("only use for set stmt to convert its order items expr with child order expr", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && found && i < stmt->get_order_items().count(); ++i) {
      int64_t idx = -1;
      ObRawExpr *order_expr = NULL;
      if (OB_ISNULL(order_expr = stmt->get_order_items().at(i).expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(stmt->get_order_items().at(i)));
      } else if (!order_expr->is_set_op_expr()) {
        found = false;
      } else if (-1 == (idx = static_cast<ObSetOpRawExpr*>(order_expr)->get_idx())){
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected index", K(idx), K(ret));
      } else if (OB_FAIL(order_exprs.push_back(select_exprs.at(idx)))){
        LOG_WARN("fail to push back expr", K(ret));
      } else if (OB_FAIL(directions.push_back(stmt->get_order_items().at(i).order_type_))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
      //do nothing
    } else if (OB_FAIL(ObOptimizerUtil::make_sort_keys(order_exprs, directions, order_items))) {
      LOG_WARN("failed to make sort keys", K(ret));
    }
  }
  return ret;
}

int ObSelectLogPlan::create_merge_set_key(const ObIArray<OrderItem> &set_order_items,
                                          const ObIArray<ObRawExpr*> &merge_exprs,
                                          const EqualSets &equal_sets,
                                          MergeKeyInfo &merge_key)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 2> sort_exprs;
  ObSEArray<ObOrderDirection, 2> directions;
  ObSEArray<int64_t, 2> sort_map;
  if (sort_exprs.empty() && !set_order_items.empty()) {
    // find direction in order by exprs
    if (OB_FAIL(ObOptimizerUtil::create_interesting_merge_key(merge_exprs, set_order_items, equal_sets, sort_exprs, directions, sort_map))) {
      LOG_WARN("failed to create interesting key", K(ret));
    } else {
      LOG_TRACE("succeed to create merge key use order by items", K(sort_exprs), K(directions));
    }
  }
  if (OB_SUCC(ret) && sort_exprs.empty()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < merge_exprs.count(); ++i) {
      if (OB_FAIL(sort_exprs.push_back(merge_exprs.at(i)))) {
        LOG_WARN("failed to push back", K(ret));
      } else if (OB_FAIL(directions.push_back(default_asc_direction()))) {
        LOG_WARN("failed to push back", K(ret));
      } else if (OB_FAIL(sort_map.push_back(i))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(merge_key.order_exprs_.assign(sort_exprs))) {
    LOG_WARN("failed to assign exprs", K(ret));
  } else if (OB_FAIL(merge_key.order_directions_.assign(directions))) {
    LOG_WARN("failed to assign exprs", K(ret));
  } else if (OB_FAIL(merge_key.map_array_.assign(sort_map))) {
    LOG_WARN("failed to assign exprs", K(ret));
  }
  return ret;
}

int ObSelectLogPlan::decide_merge_set_sort_key(const ObIArray<OrderItem> &set_order_items,
                                               const ObIArray<OrderItem> &input_ordering,
                                               const ObFdItemSet &fd_item_set,
                                               const EqualSets &equal_sets,
                                               const ObIArray<ObRawExpr*> &const_exprs,
                                               const ObIArray<ObRawExpr*> &exec_ref_exprs,
                                               const bool is_at_most_one_row,
                                               const ObIArray<ObRawExpr*> &merge_exprs,
                                               const ObIArray<ObOrderDirection> &default_directions,
                                               MergeKeyInfo &merge_key)
{
  int ret = OB_SUCCESS;
  int64_t prefix_count = -1;
  bool input_ordering_all_used = false;
  ObSEArray<OrderItem, 8> order_items;
  ObSEArray<OrderItem, 8> final_items;
  if (OB_FAIL(merge_key.order_exprs_.assign(merge_exprs))) {
    LOG_WARN("failed to assign exprs", K(ret));
  } else if (OB_FAIL(merge_key.order_directions_.assign(default_directions))) {
    LOG_WARN("failed to assign exprs", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::adjust_exprs_by_ordering(merge_key.order_exprs_,
                                                               input_ordering,
                                                               equal_sets,
                                                               const_exprs,
                                                               exec_ref_exprs,
                                                               prefix_count,
                                                               input_ordering_all_used,
                                                               merge_key.order_directions_,
                                                               &merge_key.map_array_))) {
    LOG_WARN("failed to adjust expr by ordering", K(ret));
  } else if (!input_ordering_all_used && prefix_count <= 0 && OB_FAIL(create_merge_set_key(set_order_items, merge_exprs, equal_sets, merge_key))) {
    LOG_WARN("failed to create merge set key", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::make_sort_keys(merge_key.order_exprs_,
                                              merge_key.order_directions_,
                                              order_items))) {
    LOG_WARN("failed to make sort keys", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::simplify_ordered_exprs(fd_item_set,
                                                             equal_sets,
                                                             const_exprs,
                                                             exec_ref_exprs,
                                                             order_items,
                                                             final_items))) {
    LOG_WARN("failed to simply ordered exprs", K(ret));
  } else if (OB_FAIL(merge_key.order_items_.assign(final_items))) {
    LOG_WARN("failed to assign final items", K(ret));
  } else if (input_ordering_all_used) {
    merge_key.need_sort_ = false;
  } else if (OB_FAIL(ObOptimizerUtil::check_need_sort(merge_key.order_items_,
                                                    input_ordering,
                                                    fd_item_set,
                                                    equal_sets,
                                                    const_exprs,
                                                    exec_ref_exprs,
                                                    is_at_most_one_row,
                                                    merge_key.need_sort_,
                                                    merge_key.prefix_pos_))) {
    LOG_WARN("failed to check need sort", K(ret));
  }
  return ret;
}

int ObSelectLogPlan::init_merge_set_structure(ObIAllocator &allocator,
                                              const ObIArray<CandidatePlan> &plans,
                                              const ObIArray<ObRawExpr*> &select_exprs,
                                              ObIArray<MergeKeyInfo*> &merge_keys,
                                              const bool can_ignore_merge_plan)
{
  int ret = OB_SUCCESS;
  const ObLogicalOperator *child = NULL;
  ObSEArray<ObOrderDirection, 8> default_directions;
  MergeKeyInfo *interesting_key = NULL;
  MergeKeyInfo *merge_key = NULL;
  ObSEArray<OrderItem, 8> order_items;
  if (OB_FAIL(convert_set_order_item(get_stmt(), select_exprs, order_items))) {
    LOG_WARN("failed to convert order item", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::get_default_directions(select_exprs.count(), default_directions))) {
    LOG_WARN("failed to get default directions", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < plans.count(); ++i) {
    if (OB_ISNULL(merge_key = static_cast<MergeKeyInfo*>(allocator.alloc(sizeof(MergeKeyInfo))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("failed to alloc merge key info", K(ret));
    } else if (OB_ISNULL(child = plans.at(i).plan_tree_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(child));
    } else if (OB_FALSE_IT(merge_key = new (merge_key) MergeKeyInfo(allocator,
                                                                    select_exprs.count()))) {
    } else if (OB_FAIL(decide_merge_set_sort_key(order_items,
                                                 child->get_op_ordering(),
                                                 child->get_fd_item_set(),
                                                 child->get_output_equal_sets(),
                                                 child->get_output_const_exprs(),
                                                 get_onetime_query_refs(),
                                                 child->get_is_at_most_one_row(),
                                                 select_exprs,
                                                 default_directions,
                                                 *merge_key))) {
      LOG_WARN("failed to decide sort key for merge set", K(ret));
    } else if (OB_FAIL(merge_keys.push_back(merge_key))) {
      LOG_WARN("failed to push back merge key", K(ret));
    } else if (can_ignore_merge_plan) {
      bool is_match = false;
      if (OB_FAIL(ObOptimizerUtil::is_order_by_match(order_items,
                                                     merge_key->order_items_,
                                                     child->get_output_equal_sets(),
                                                     child->get_output_const_exprs(),
                                                     is_match))) {
        LOG_WARN("failed to check is order by match", K(ret));
      } else if (!is_match) {
        merge_key->order_needed_ = false;
        LOG_TRACE("ordering is not math order by");
      }
    }
  }
  return ret;
}

int ObSelectLogPlan::create_merge_set_plan(const EqualSets &equal_sets,
                                           ObLogicalOperator *left_child,
                                           ObLogicalOperator *right_child,
                                           const ObIArray<ObRawExpr*> &left_set_keys,
                                           const ObIArray<ObRawExpr*> &right_set_keys,
                                           const ObSelectStmt::SetOperator set_op,
                                           const DistAlgo dist_set_method,
                                           const ObIArray<ObOrderDirection> &order_directions,
                                           const ObIArray<int64_t> &map_array,
                                           const ObIArray<OrderItem> &left_sort_keys,
                                           const bool left_need_sort,
                                           const int64_t left_prefix_pos,
                                           const ObIArray<OrderItem> &right_sort_keys,
                                           const bool right_need_sort,
                                           const int64_t right_prefix_pos,
                                           CandidatePlan &merge_plan)
{
  int ret = OB_SUCCESS;
  ObLogPlan *left_plan = NULL;
  ObLogPlan *right_plan = NULL;
  ObExchangeInfo left_exch_info;
  ObExchangeInfo right_exch_info;
  if (OB_ISNULL(left_child) || OB_ISNULL(right_child) ||
      OB_ISNULL(left_plan = left_child->get_plan()) ||
      OB_ISNULL(right_plan = right_child->get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(left_child), K(right_child), K(left_plan), K(right_plan));
  } else {
    bool is_fully_partition_wise = DistAlgo::DIST_PARTITION_WISE == dist_set_method &&
                                   !left_child->is_exchange_allocated() && !right_child->is_exchange_allocated();
    bool is_left_local_order = left_child->get_is_local_order() && !is_fully_partition_wise;
    bool is_right_local_order = right_child->get_is_local_order() && !is_fully_partition_wise;
    if (OB_FAIL(compute_set_exchange_info(equal_sets,
                                          *left_child,
                                          *right_child,
                                          left_set_keys,
                                          right_set_keys,
                                          set_op,
                                          dist_set_method,
                                          left_exch_info,
                                          right_exch_info))) {
      LOG_WARN("failed to compute set exchange info", K(ret));
    } else if (OB_FAIL(left_plan->allocate_sort_and_exchange_as_top(left_child,
                                                                    left_exch_info,
                                                                    left_sort_keys,
                                                                    left_need_sort,
                                                                    left_prefix_pos,
                                                                    is_left_local_order))) {
      LOG_WARN("failed to allocate operator for set path", K(ret));
    } else if (OB_FAIL(right_plan->allocate_sort_and_exchange_as_top(right_child,
                                                                     right_exch_info,
                                                                     right_sort_keys,
                                                                     right_need_sort,
                                                                     right_prefix_pos,
                                                                     is_right_local_order))) {
      LOG_WARN("failed to allocate operator for set path", K(ret));
    } else if (OB_FAIL(allocate_distinct_set_as_top(left_child,
                                                    right_child,
                                                    SetAlgo::MERGE_SET,
                                                    dist_set_method,
                                                    merge_plan.plan_tree_,
                                                    &order_directions,
                                                    &map_array))) {
      LOG_WARN("failed to allocate distinct set as top", K(ret));
    } else {
      LOG_TRACE("succeed to create merge set plan", K(left_sort_keys), K(right_sort_keys),
          K(order_directions), K(map_array), K(left_need_sort), K(right_need_sort));
    }
  }
  return ret;
}

int ObSelectLogPlan::get_allowed_branch_order(const bool ignore_hint,
                                              const ObSelectStmt::SetOperator set_op,
                                              bool &no_swap,
                                              bool &swap)
{
  int ret = OB_SUCCESS;
  no_swap = true;
  swap = ObSelectStmt::EXCEPT != set_op;
  bool hint_valid = false;
  bool need_swap = false;
  if (ignore_hint) {
    /* do nothing */
  } else if (OB_FAIL(get_log_plan_hint().check_valid_set_left_branch(get_stmt(),
                                                                     hint_valid,
                                                                     need_swap))) {
    LOG_WARN("failed to check valid set left branch", K(ret));
  } else if (!hint_valid) {
    /* do nothing */
  } else {
    no_swap &= !need_swap;
    swap &= need_swap;
  }
  return ret;
}

int ObSelectLogPlan::generate_hash_set_plans(const EqualSets &equal_sets,
                                             const ObIArray<ObRawExpr*> &left_set_keys,
                                             const ObIArray<ObRawExpr*> &right_set_keys,
                                             const ObSelectStmt::SetOperator set_op,
                                             const ObIArray<CandidatePlan> &left_best_plans,
                                             const ObIArray<CandidatePlan> &right_best_plans,
                                             const bool ignore_hint,
                                             ObIArray<CandidatePlan> &hash_set_plans)
{
  int ret = OB_SUCCESS;
  OPT_TRACE("start generate hash set plans");
  bool no_swap = false;
  bool swap = false;
  if (OB_FAIL(get_allowed_branch_order(ignore_hint, set_op, no_swap, swap))) {
    LOG_WARN("failed to get allowed branch order", K(ret));
  } else if (no_swap && OB_FAIL(inner_generate_hash_set_plans(equal_sets,
                                                              left_set_keys,
                                                              right_set_keys,
                                                              set_op,
                                                              left_best_plans,
                                                              right_best_plans,
                                                              ignore_hint,
                                                              hash_set_plans))) {
    LOG_WARN("failed to generate hash set plans", K(ret));
  } else if (swap && OB_FAIL(inner_generate_hash_set_plans(equal_sets,
                                                           right_set_keys,
                                                           left_set_keys,
                                                           set_op,
                                                           right_best_plans,
                                                           left_best_plans,
                                                           ignore_hint,
                                                           hash_set_plans))) {
    LOG_WARN("failed to generate hash set plans", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObSelectLogPlan::inner_generate_hash_set_plans(const EqualSets &equal_sets,
                                                   const ObIArray<ObRawExpr*> &left_set_keys,
                                                   const ObIArray<ObRawExpr*> &right_set_keys,
                                                   const ObSelectStmt::SetOperator set_op,
                                                   const ObIArray<CandidatePlan> &left_best_plans,
                                                   const ObIArray<CandidatePlan> &right_best_plans,
                                                   const bool ignore_hint,
                                                   ObIArray<CandidatePlan> &hash_set_plans)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *tmp = NULL;
  CandidatePlan candidate_plan;
  for (int64_t i = 0; OB_SUCC(ret) && i < left_best_plans.count(); i++) {
    ObLogicalOperator *left_best_plan = NULL;
    if (OB_ISNULL(left_best_plan = left_best_plans.at(i).plan_tree_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else {
      for (int64_t j = 0; OB_SUCC(ret) && j < right_best_plans.count(); j++) {
        ObLogicalOperator *right_best_plan = NULL;
        int64_t set_methods = 0;
        if (OB_ISNULL(right_best_plan = right_best_plans.at(j).plan_tree_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (OB_FAIL(get_distributed_set_methods(equal_sets,
                                                       left_set_keys,
                                                       right_set_keys,
                                                       set_op,
                                                       HASH_SET,
                                                       *left_best_plan,
                                                       *right_best_plan,
                                                       ignore_hint,
                                                       set_methods))) {
          LOG_WARN("failed to get distributed set methods", K(ret));
        } else {
          LOG_TRACE("distributed hash set methods", K(set_methods));
          for (int64_t k = DistAlgo::DIST_BASIC_METHOD;
               OB_SUCC(ret) && k < DistAlgo::DIST_MAX_JOIN_METHOD; k = (k << 1)) {
            DistAlgo dist_algo = get_dist_algo(k);
            if ((set_methods & k) && (DistAlgo::DIST_PARTITION_WISE != dist_algo ||
                 is_set_partition_wise_valid(*left_best_plan, *right_best_plan)) &&
                 is_set_repart_valid(*left_best_plan, *right_best_plan, dist_algo)) {
              if (OB_FAIL(create_hash_set_plan(equal_sets,
                                               left_best_plan,
                                               right_best_plan,
                                               left_set_keys,
                                               right_set_keys,
                                               set_op,
                                               dist_algo,
                                               candidate_plan))) {
                LOG_WARN("failed to create hash set", K(ret));
              } else if (OB_FAIL(hash_set_plans.push_back(candidate_plan))) {
                LOG_WARN("failed to add hash plan", K(ret));
              } else { /*do nothing*/ }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObSelectLogPlan::create_hash_set_plan(const EqualSets &equal_sets,
                                          ObLogicalOperator *left_child,
                                          ObLogicalOperator *right_child,
                                          const ObIArray<ObRawExpr*> &left_set_keys,
                                          const ObIArray<ObRawExpr*> &right_set_keys,
                                          const ObSelectStmt::SetOperator set_op,
                                          DistAlgo dist_set_method,
                                          CandidatePlan &hash_plan)
{
  int ret = OB_SUCCESS;
  ObExchangeInfo left_exch_info;
  ObExchangeInfo right_exch_info;
  ObLogPlan *left_log_plan = NULL;
  ObLogPlan *right_log_plan = NULL;
  const ObSelectStmt *select_stmt = get_stmt();
  if (OB_ISNULL(left_child) || OB_ISNULL(right_child) ||
      OB_ISNULL(left_log_plan = left_child->get_plan()) ||
      OB_ISNULL(right_log_plan = right_child->get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(left_child), K(right_child),
        K(left_log_plan), K(right_log_plan), K(ret));
  } else if (OB_FAIL(compute_set_exchange_info(equal_sets,
                                               *left_child,
                                               *right_child,
                                               left_set_keys,
                                               right_set_keys,
                                               set_op,
                                               dist_set_method,
                                               left_exch_info,
                                               right_exch_info))) {
    LOG_WARN("failed to compute set exchange info", K(ret));
  } else if (left_exch_info.need_exchange() &&
             OB_FAIL(left_log_plan->allocate_exchange_as_top(left_child, left_exch_info))) {
    LOG_WARN("failed to allocate exchange as top", K(ret));
  } else if (right_exch_info.need_exchange() &&
             OB_FAIL(right_log_plan->allocate_exchange_as_top(right_child, right_exch_info))) {
    LOG_WARN("failed to allocate exchange as top", K(ret));
  } else if (OB_FAIL(allocate_distinct_set_as_top(left_child,
                                                  right_child,
                                                  SetAlgo::HASH_SET,
                                                  dist_set_method,
                                                  hash_plan.plan_tree_))) {
    LOG_WARN("failed to allocate distinct set as top", K(ret));
  } else {
    LOG_TRACE("succeed to create hash set plan");
  }
  return ret;
}

int ObSelectLogPlan::allocate_distinct_set_as_top(ObLogicalOperator *left_child,
                                                  ObLogicalOperator *right_child,
                                                  SetAlgo set_method,
                                                  DistAlgo dist_set_method,
                                                  ObLogicalOperator *&top,
                                                  const ObIArray<ObOrderDirection> *order_directions,
                                                  const ObIArray<int64_t> *map_array)
{
  int ret = OB_SUCCESS;
  ObLogSet *set_op = NULL;
  const ObSelectStmt *select_stmt = NULL;
  top = NULL;
  if (OB_ISNULL(select_stmt = get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_ISNULL((set_op = static_cast<ObLogSet*>(get_log_op_factory().
                                 allocate(*this, LOG_SET))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("Allocate memory for ObLogSet failed", K(ret));
  } else if (NULL != order_directions && OB_FAIL(set_op->set_set_directions(*order_directions))) {
    LOG_WARN("failed to set order directions", K(ret));
  } else if (NULL != map_array && OB_FAIL(set_op->set_map_array(*map_array))) {
    LOG_WARN("failed to set map array", K(ret));
  } else {
    set_op->set_left_child(left_child);
    set_op->set_right_child(right_child);
    set_op->assign_set_distinct(select_stmt->is_set_distinct());
    set_op->assign_set_op(select_stmt->get_set_op());
    set_op->set_algo_type(set_method);
    set_op->set_distributed_algo(dist_set_method);
    if (OB_FAIL(set_op->compute_property())) {
      LOG_WARN("failed to compute property", K(ret));
    } else {
      top = set_op;
    }
  }
  return ret;
}

int ObSelectLogPlan::compute_set_exchange_info(const EqualSets &equal_sets,
                                               ObLogicalOperator &left_child,
                                               ObLogicalOperator &right_child,
                                               const ObIArray<ObRawExpr*> &left_set_keys,
                                               const ObIArray<ObRawExpr*> &right_set_keys,
                                               const ObSelectStmt::SetOperator set_op,
                                               DistAlgo set_method,
                                               ObExchangeInfo &left_exch_info,
                                               ObExchangeInfo &right_exch_info)
{
  int ret = OB_SUCCESS;
  const ObLogicalOperator *parallel_source = NULL;
  left_exch_info.dist_method_ = ObPQDistributeMethod::NONE;
  right_exch_info.dist_method_ = ObPQDistributeMethod::NONE;
  if (DistAlgo::DIST_BASIC_METHOD == set_method ||
      DistAlgo::DIST_PARTITION_WISE == set_method ||
      DistAlgo::DIST_SET_PARTITION_WISE == set_method ||
      DistAlgo::DIST_NONE_ALL == set_method ||
      DistAlgo::DIST_ALL_NONE == set_method) {
    /*do nothing*/
  } else if (DistAlgo::DIST_PULL_TO_LOCAL == set_method) {
    if (left_child.is_sharding()) {
      left_exch_info.dist_method_ = ObPQDistributeMethod::LOCAL;
    }
    if (right_child.is_sharding()) {
      right_exch_info.dist_method_ = ObPQDistributeMethod::LOCAL;
    }
  } else if (DistAlgo::DIST_HASH_HASH == set_method) {
    parallel_source = left_child.get_parallel() > right_child.get_parallel()
                      ? &left_child : &right_child;
    ObShardingInfo *sharding_info = NULL;
    if (OB_FAIL(compute_set_hash_hash_sharding(equal_sets,
                                               left_set_keys,
                                               right_set_keys,
                                               sharding_info))) {
      LOG_WARN("failed to compute set hash-hash sharding", K(ret));
    } else if (OB_ISNULL(sharding_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(left_exch_info.append_hash_dist_expr(left_set_keys))) {
      LOG_WARN("failed to append hash dist expr", K(ret));
    } else if (OB_FAIL(right_exch_info.append_hash_dist_expr(right_set_keys))) {
      LOG_WARN("failed to append hash dist expr", K(ret));
    } else {
      left_exch_info.strong_sharding_ = sharding_info;
      left_exch_info.dist_method_ = ObPQDistributeMethod::HASH;
      right_exch_info.strong_sharding_ = sharding_info;
      right_exch_info.dist_method_ = ObPQDistributeMethod::HASH;
    }
  } else if (DistAlgo::DIST_HASH_NONE == set_method) {
    parallel_source = &right_child;
    if (OB_FAIL(compute_single_side_hash_distribution_info(equal_sets,
                                                          left_set_keys,
                                                          right_set_keys,
                                                          right_child,
                                                          left_exch_info))) {
      LOG_WARN("failed to compute repartition distribution info", K(ret));
    } else if (ObSelectStmt::SetOperator::UNION == set_op ||
               ObSelectStmt::SetOperator::EXCEPT == set_op) {
      ObPQDistributeMethod::Type dist_method = ObPQDistributeMethod::RANDOM;
      bool is_unique = false;
      if (OB_FAIL(ObOptimizerUtil::is_exprs_unique(left_set_keys,
                                              left_child.get_table_set(),
                                              left_child.get_fd_item_set(),
                                              left_child.get_output_equal_sets(),
                                              left_child.get_output_const_exprs(),
                                              is_unique))) {
        LOG_WARN("failed to check is left unique", K(ret));
      } else if (!is_unique) {
        dist_method = ObPQDistributeMethod::HASH;
      }
      if (OB_SUCC(ret)) {
        left_exch_info.unmatch_row_dist_method_ = dist_method;
        left_exch_info.strong_sharding_ = get_optimizer_context().get_distributed_sharding();
      }
    } else {
      if (OB_FAIL(left_exch_info.weak_sharding_.assign(right_child.get_weak_sharding()))) {
        LOG_WARN("failed to assign weak sharding", K(ret));
      } else {
        left_exch_info.unmatch_row_dist_method_ = ObPQDistributeMethod::DROP;
        left_exch_info.strong_sharding_ = right_child.get_strong_sharding();
      }
    }
  } else if (DistAlgo::DIST_NONE_HASH == set_method) {
    parallel_source = &left_child;
    if (OB_FAIL(compute_single_side_hash_distribution_info(equal_sets,
                                                           right_set_keys,
                                                           left_set_keys,
                                                           left_child,
                                                           right_exch_info))) {
      LOG_WARN("failed to compute repartition distribution info", K(ret));
    } else if (ObSelectStmt::SetOperator::UNION == set_op) {
      ObPQDistributeMethod::Type dist_method = ObPQDistributeMethod::RANDOM;
      bool is_unique = false;
      if (OB_FAIL(ObOptimizerUtil::is_exprs_unique(right_set_keys,
                                              right_child.get_table_set(),
                                              right_child.get_fd_item_set(),
                                              right_child.get_output_equal_sets(),
                                              right_child.get_output_const_exprs(),
                                              is_unique))) {
        LOG_WARN("failed to check is right unique", K(ret));
      } else if (!is_unique) {
        dist_method = ObPQDistributeMethod::HASH;
      }
      if (OB_SUCC(ret)) {
        right_exch_info.unmatch_row_dist_method_ = dist_method;
        right_exch_info.strong_sharding_ = get_optimizer_context().get_distributed_sharding();
      }
    } else {
      if (OB_FAIL(right_exch_info.weak_sharding_.assign(left_child.get_weak_sharding()))) {
        LOG_WARN("failed to assign weak sharding", K(ret));
      } else {
        right_exch_info.unmatch_row_dist_method_ = ObPQDistributeMethod::DROP;
        right_exch_info.strong_sharding_ = left_child.get_strong_sharding();
      }
    }
  } else if (DistAlgo::DIST_PARTITION_NONE == set_method) {
    parallel_source = &right_child;
    if (OB_FAIL(compute_repartition_distribution_info(equal_sets,
                                                      left_set_keys,
                                                      right_set_keys,
                                                      right_child,
                                                      left_exch_info))) {
      LOG_WARN("failed to compute repartition distribution info", K(ret));
    } else if (ObSelectStmt::SetOperator::UNION == set_op ||
               ObSelectStmt::SetOperator::EXCEPT == set_op) {
      ObPQDistributeMethod::Type dist_method = ObPQDistributeMethod::RANDOM;
      bool is_unique = false;
      if (OB_FAIL(ObOptimizerUtil::is_exprs_unique(left_set_keys,
                                              left_child.get_table_set(),
                                              left_child.get_fd_item_set(),
                                              left_child.get_output_equal_sets(),
                                              left_child.get_output_const_exprs(),
                                              is_unique))) {
        LOG_WARN("failed to check is left unique", K(ret));
      } else if (!is_unique) {
        dist_method = ObPQDistributeMethod::HASH;
        if (OB_FAIL(left_exch_info.append_hash_dist_expr(left_set_keys))) {
          LOG_WARN("failed to append hash dist expr", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        left_exch_info.unmatch_row_dist_method_ = dist_method;
        left_exch_info.strong_sharding_ = get_optimizer_context().get_distributed_sharding();
      }
    } else {
      if (OB_FAIL(left_exch_info.weak_sharding_.assign(right_child.get_weak_sharding()))) {
        LOG_WARN("failed to assign weak sharding", K(ret));
      } else {
        left_exch_info.unmatch_row_dist_method_ = ObPQDistributeMethod::DROP;
        left_exch_info.strong_sharding_ = right_child.get_strong_sharding();
      }
    }
  } else if (DistAlgo::DIST_NONE_PARTITION == set_method) {
    parallel_source = &left_child;
    if (OB_FAIL(compute_repartition_distribution_info(equal_sets,
                                                      right_set_keys,
                                                      left_set_keys,
                                                      left_child,
                                                      right_exch_info))) {
      LOG_WARN("failed to compute repartition distribution info", K(ret));
    } else if (ObSelectStmt::SetOperator::UNION == set_op) {
      ObPQDistributeMethod::Type dist_method = ObPQDistributeMethod::RANDOM;
      bool is_unique = false;
      if (OB_FAIL(ObOptimizerUtil::is_exprs_unique(right_set_keys,
                                              right_child.get_table_set(),
                                              right_child.get_fd_item_set(),
                                              right_child.get_output_equal_sets(),
                                              right_child.get_output_const_exprs(),
                                              is_unique))) {
        LOG_WARN("failed to check is right unique", K(ret));
      } else if (!is_unique) {
        dist_method = ObPQDistributeMethod::HASH;
        if (OB_FAIL(right_exch_info.append_hash_dist_expr(right_set_keys))) {
          LOG_WARN("failed to append hash dist expr", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        right_exch_info.unmatch_row_dist_method_ = dist_method;
        right_exch_info.strong_sharding_ = get_optimizer_context().get_distributed_sharding();
      }
    } else {
      if (OB_FAIL(right_exch_info.weak_sharding_.assign(left_child.get_weak_sharding()))) {
        LOG_WARN("failed to assign weak sharding", K(ret));
      } else {
        right_exch_info.unmatch_row_dist_method_ = ObPQDistributeMethod::DROP;
        right_exch_info.strong_sharding_ = left_child.get_strong_sharding();
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  }

  if (OB_FAIL(ret) || NULL == parallel_source) {
  } else if (OB_FAIL(left_exch_info.server_list_.assign(parallel_source->get_server_list()))
             || OB_FAIL(right_exch_info.server_list_.assign(parallel_source->get_server_list()))) {
    LOG_WARN("failed to assign server list", K(ret));
  } else {
    left_exch_info.parallel_ = parallel_source->get_parallel();
    left_exch_info.server_cnt_ = parallel_source->get_server_cnt();
    right_exch_info.parallel_ = parallel_source->get_parallel();
    right_exch_info.server_cnt_ = parallel_source->get_server_cnt();
  }
  return ret;
}

int ObSelectLogPlan::compute_set_hash_hash_sharding(const EqualSets &equal_sets,
                                                    const ObIArray<ObRawExpr*> &left_keys,
                                                    const ObIArray<ObRawExpr*> &right_keys,
                                                    ObShardingInfo *&sharding)
{
  int ret = OB_SUCCESS;
  sharding = NULL;
  if (OB_FAIL(get_cached_hash_sharding_info(left_keys, equal_sets, sharding))) {
    LOG_WARN("failed to get cached hash sharding info", K(ret));
  } else if (NULL != sharding) {
    /*do nothing*/
  } else if (OB_FAIL(get_cached_hash_sharding_info(right_keys, equal_sets, sharding))) {
    LOG_WARN("failed to get cached hash sharding", K(ret));
  } else if (NULL != sharding) {
    /*do nothing*/
  } else if (OB_ISNULL(sharding = reinterpret_cast<ObShardingInfo*>(get_allocator().alloc(sizeof(ObShardingInfo))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else {
    sharding = new (sharding) ObShardingInfo();
    sharding->set_distributed();
    if (OB_FAIL(sharding->get_partition_keys().assign(left_keys))) {
      LOG_WARN("failed to assign partition exprs", K(ret));
    } else if (OB_FAIL(get_hash_dist_info().push_back(sharding))) {
      LOG_WARN("failed to push back sharding info", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObSelectLogPlan::generate_normal_raw_plan()
{
  int ret = OB_SUCCESS;
  const ObSelectStmt *select_stmt = get_stmt();
  OPT_TRACE("generate plan for ", select_stmt);
  if (OB_ISNULL(select_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (select_stmt->is_set_stmt()) {
    ret = SMART_CALL(generate_raw_plan_for_set());
  } else if (0 == select_stmt->get_from_item_size()) {
    ret = generate_raw_plan_for_expr_values();
  } else {
    ret = SMART_CALL(generate_raw_plan_for_plain_select());
  }
  return ret;
}

int ObSelectLogPlan::generate_dblink_raw_plan()
{
  int ret = OB_SUCCESS;
  ObQueryCtx *query_ctx = NULL;
  // dblink_info hint
  int64_t tx_id = -1;
  int64_t tm_sessid = -1;
  uint64_t dblink_id = OB_INVALID_ID;
  const ObSelectStmt *stmt = get_stmt();
  ObLogicalOperator *top = NULL;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null ptr", K(ret));
  } else if (NULL == (query_ctx = stmt->get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (FALSE_IT(dblink_id = stmt->get_dblink_id())) {
  } else if (0 == dblink_id) { //dblink id = 0 means @!/@xxxx!
    ObSQLSessionInfo *session = get_optimizer_context().get_session_info();
    oceanbase::sql::ObReverseLink *reverse_dblink_info = NULL;
    if (NULL == session) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), KP(session));
    } else if (OB_FAIL(session->get_dblink_context().get_reverse_link(reverse_dblink_info))) {
      LOG_WARN("failed to get reverse link info from session", K(ret), K(session->get_sessid()));
    } else if (NULL == reverse_dblink_info) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else {
      // set dblink_info, to unparse a link sql with dblink_info hint
      query_ctx->get_query_hint_for_update().get_global_hint().merge_dblink_info_hint(
                                                    reverse_dblink_info->get_tx_id(),
                                                    reverse_dblink_info->get_tm_sessid());
      LOG_DEBUG("set tx_id_ and tm_sessid to stmt",
                                                    K(reverse_dblink_info->get_tx_id()),
                                                    K(reverse_dblink_info->get_tm_sessid()));
    }
  } else {
    // save dblink_info hint
    tx_id = query_ctx->get_query_hint_for_update().get_global_hint().get_dblink_tx_id_hint();
    tm_sessid = query_ctx->get_query_hint_for_update().get_global_hint().get_dblink_tm_sessid_hint();
    // reset dblink hint, to unparse a link sql without dblink_info hint
    query_ctx->get_query_hint_for_update().get_global_hint().reset_dblink_info_hint();
  }
  if (OB_FAIL(ret)) {
    //do nothing
  } else if (OB_FAIL(allocate_link_scan_as_top(top))) {
    LOG_WARN("failed to allocate link dml as top", K(ret));
  } else {
    top->set_dblink_id(dblink_id);
    ObLogLinkScan *link_scan = static_cast<ObLogLinkScan *>(top);
    if (OB_FAIL(link_scan->set_link_stmt(stmt))) {
      LOG_WARN("failed to set link stmt", K(ret));
    } else if (0 == dblink_id) {
      link_scan->set_reverse_link(true);
    }
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (optimizer_context_.has_multiple_link_stmt()
             && OB_FAIL(allocate_material_as_top(top))) {
    LOG_WARN("allocate material above link scan failed", K(ret));
  } else if (OB_FAIL(make_candidate_plans(top))) {
    LOG_WARN("failed to make candidate plans", K(ret));
  } else {
    top->mark_is_plan_root();
    top->get_plan()->set_plan_root(top);
    if (0 == dblink_id) {
      // reset dblink info, to avoid affecting the next execution flow
      query_ctx->get_query_hint_for_update().get_global_hint().reset_dblink_info_hint();
    } else {
      // restore dblink_info hint, ensure that the next execution process can get the correct dblink_info
      query_ctx->get_query_hint_for_update().get_global_hint().merge_dblink_info_hint(tx_id, tm_sessid);
    }
    LOG_TRACE("succeed to allocate loglinkscan", K(dblink_id));
  }
  return ret;
}

int ObSelectLogPlan::allocate_link_scan_as_top(ObLogicalOperator *&old_top)
{
  int ret = OB_SUCCESS;
  ObLogLinkScan *link_scan = NULL;
  const ObSelectStmt *stmt = get_stmt();
  ObSEArray<ObRawExpr*, 4> select_exprs;
  if (NULL != old_top) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("old_top should be null", K(ret), K(get_stmt()));
  } else if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null ptr", K(ret), K(stmt));
  } else if (OB_ISNULL(link_scan = static_cast<ObLogLinkScan *>(get_log_op_factory().
                                  allocate(*this, LOG_LINK_SCAN)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate link dml operator", K(ret));
  } else if (OB_FAIL(stmt->get_select_exprs(select_exprs))) {
    LOG_WARN("failed to get select exprs", K(ret));
  } else if (OB_FAIL(link_scan->get_select_exprs().assign(select_exprs))) {
    LOG_WARN("failed to assign select exprs", K(ret));
  } else if (OB_FAIL(link_scan->compute_property())) {
    LOG_WARN("failed to compute property", K(ret));
  } else {
    old_top = link_scan;
  }
  return ret;
}

// 1. 生成log plan tree, 包括Join/TableScan/SubplanScan/Sort/Material算子
// 2. 分配top算子，包括Count/Group By/SubPlanFilter/Window Sort/Distinct/Order By
//                     Limit/Late Materialization/Select Into
// 3. 选出最终代价最小的plan
int ObSelectLogPlan::generate_raw_plan_for_plain_select()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(generate_plan_tree())) {
    LOG_WARN("failed to generate plan tree for plain select", K(ret));
  } else if (OB_FAIL(allocate_plan_top())) {
    LOG_WARN("failed to allocate top operator of plan tree for plain select", K(ret));
  } else {
    LOG_TRACE("succeed to generate best plan");
  }
  return ret;
}

int ObSelectLogPlan::allocate_plan_top()
{
  int ret = OB_SUCCESS;
  const ObSelectStmt *select_stmt = NULL;
  if (OB_ISNULL(select_stmt = get_stmt()) || OB_ISNULL(optimizer_context_.get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    bool need_limit = true;
    bool for_update_is_allocated = false;
    ObSEArray<OrderItem, 4> order_items;
    LOG_TRACE("start to allocate operators for ", "sql", optimizer_context_.get_query_ctx()->get_sql_stmt());
    // step. allocate subplan filter if needed, mainly for the subquery in where statement
    if (OB_SUCC(ret)) {
      if (get_subquery_filters().count() > 0) {
        LOG_TRACE("start to allocate subplan filter for where statement", K(ret));
        if (OB_FAIL(candi_allocate_subplan_filter_for_where())) {
          LOG_WARN("failed to allocate subplan filter for where statement", K(ret));
        } else {
          LOG_TRACE("succeed to allocate subplan filter for where statement",
               K(candidates_.candidate_plans_.count()));
        }
      }
    }

    // step. allocate 'count' if needed
    if (OB_SUCC(ret)) {
      bool has_rownum = false;
      if (OB_FAIL(select_stmt->has_rownum(has_rownum))) {
        LOG_WARN("failed to get rownum info", K(ret));
      } else if (has_rownum) {
        if (OB_FAIL(candi_allocate_count())) {
          LOG_WARN("failed to allocate count operator", K(ret));
        } else {
          LOG_TRACE("succeed to allocate count operator",
              K(candidates_.candidate_plans_.count()));
        }
      }
    }
    // step. allocate 'group-by' if needed
    if (OB_SUCC(ret) && (select_stmt->has_group_by() || select_stmt->has_rollup())) {
      // group-by or rollup both allocate group by logical operator.
      // mysql mode for update need allocate before group by because group by isn't pk preserving.
      if (lib::is_mysql_mode() && select_stmt->has_for_update()) {
        if (OB_FAIL(candi_allocate_for_update())) {
          LOG_WARN("failed to allocate for update operator", K(ret));
        } else {
          for_update_is_allocated = true;
          LOG_TRACE("succeed to allocate for update", K(candidates_.candidate_plans_.count()));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(candi_allocate_group_by())) {
          LOG_WARN("failed to allocate group-by operator", K(ret));
        } else {
          LOG_TRACE("succeed to allocate group-by operator",
              K(candidates_.candidate_plans_.count()));
        }
      }
    }

    // step. allocate 'window-sort' if needed
    if (OB_SUCC(ret) && select_stmt->has_window_function()) {
      if (OB_FAIL(candi_allocate_window_function())) {
        LOG_WARN("failed to allocate window function", K(ret));
      } else {
        LOG_TRACE("succeed to allocate window function",
            K(candidates_.candidate_plans_.count()));
      }
    }

    // step. allocate 'distinct' if needed
    if (OB_SUCC(ret) && select_stmt->has_distinct()) {
      // mysql mode for update need allocate before distinct because distinct isn't pk preserving.
      if (lib::is_mysql_mode() && select_stmt->has_for_update() && !for_update_is_allocated) {
        if (OB_FAIL(candi_allocate_for_update())) {
          LOG_WARN("failed to allocate for update operator", K(ret));
        } else {
          for_update_is_allocated = true;
          LOG_TRACE("succeed to allocate for update", K(candidates_.candidate_plans_.count()));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(candi_allocate_distinct())) {
          LOG_WARN("failed to allocate distinct operator", K(ret));
        } else {
          LOG_TRACE("succeed to allocate distinct operator",
              K(candidates_.candidate_plans_.count()));
        }
      }
    }

    // step. allocate 'sequence' if needed
    if (OB_SUCC(ret) && select_stmt->has_sequence()) {
      if (OB_FAIL(candi_allocate_sequence())) {
        LOG_WARN("failed to allocate sequence operator", K(ret));
      } else {
        LOG_TRACE("succeed to allocate sequence operator",
            K(candidates_.candidate_plans_.count()));
      }
    }

    // step. allocate 'order-by' if needed
    if (OB_SUCC(ret) && select_stmt->has_order_by() && !select_stmt->is_order_siblings() &&
        !get_optimizer_context().is_online_ddl()) {
      candidates_.is_final_sort_ = true;
      if (OB_FAIL(candi_allocate_order_by(need_limit, order_items))) {
        LOG_WARN("failed to allocate order by operator", K(ret));
      } else {
        candidates_.is_final_sort_ = false;
        LOG_TRACE("succeed to allocate order by operator",
            K(candidates_.candidate_plans_.count()));
      }
    }

    // step. allocate 'limit' if needed
    if (OB_SUCC(ret) && select_stmt->has_limit() && need_limit) {
      if (OB_FAIL(candi_allocate_limit(order_items))) {
        LOG_WARN("failed to allocate limit operator", K(ret));
      } else {
        LOG_TRACE("succeed to allocate limit operator",
            K(candidates_.candidate_plans_.count()));
      }
    }

    // step. allocate late materialization if needed
    if (OB_SUCC(ret) && select_stmt->has_limit()) {
      if (OB_FAIL(candi_allocate_late_materialization())) {
        LOG_WARN("failed to allocate late-materialization operator", K(ret));
      } else {
        LOG_TRACE("succeed to allocate late-materialization operator",
            K(candidates_.candidate_plans_.count()));
      }
    }

    // step. allocate subplan filter if needed, mainly for subquery in select item
    if (OB_SUCC(ret)) {
      if (OB_FAIL(candi_allocate_subplan_filter_for_select_item())) {
        LOG_WARN("failed to allocate subplan filter for subquery in select item", K(ret));
      } else {
        LOG_TRACE("succeed to allocate subplan filter for subquery in select item",
            K(candidates_.candidate_plans_.count()));
      }
    }

    // step. allocate 'unpivot' if needed
    if (OB_SUCC(ret) && select_stmt->is_unpivot_select()) {
      // group-by or rollup both allocate group by logical operator.
      if (OB_FAIL(candi_allocate_unpivot())) {
        LOG_WARN("failed to allocate unpovit operator", K(ret));
      } else {
        LOG_TRACE("succeed to allocate unpovit operator",
            K(candidates_.candidate_plans_.count()));
      }
    }

    if (OB_SUCC(ret) && select_stmt->has_for_update() && !for_update_is_allocated) {
      if (OB_FAIL(candi_allocate_for_update())) {
        LOG_WARN("failed to allocate for update operator", K(ret));
      } else {
        LOG_TRACE("succeed to allocate for update", K(candidates_.candidate_plans_.count()));
      }
    }

    // step. allocate 'select_into' if needed
    if (OB_SUCC(ret) && select_stmt->has_select_into()) {
      if (OB_FAIL(candi_allocate_select_into())) {
        LOG_WARN("failed to allocate select into operator", K(ret));
      } else {
        LOG_TRACE("succeed to allocate select into clause",
            K(candidates_.candidate_plans_.count()));
      }
    }

    if (OB_SUCC(ret) && NULL != get_insert_stmt() && get_insert_stmt()->is_error_logging()) {
      if (OB_FAIL(candi_allocate_err_log(get_insert_stmt()))) {
        LOG_WARN("failed to allocate err log", K(ret));
      } else {
        LOG_TRACE("succeed to allocate err log", K(candidates_.candidate_plans_.count()));
      }
    }

    // allocate root exchange
    if (OB_SUCC(ret) && is_final_root_plan()) {
      // allocate material if there is for update.
      if (optimizer_context_.has_for_update() && OB_FAIL(candi_allocate_for_update_material())) {
        LOG_WARN("failed to allocate material", K(ret));
        //allocate temp-table transformation if needed.
      } else if (!get_optimizer_context().get_temp_table_infos().empty() &&
                 OB_FAIL(candi_allocate_temp_table_transformation())) {
        LOG_WARN("failed to allocate transformation operator", K(ret));
      } else if (OB_FAIL(candi_allocate_root_exchange())) {
        LOG_WARN("failed to allocate root exchange", K(ret));
      } else {
        LOG_TRACE("succeed to allocate root exchange", K(candidates_.candidate_plans_.count()));
      }
    }
  }
  return ret;
}

int ObSelectLogPlan::generate_raw_plan_for_expr_values()
{
  int ret = OB_SUCCESS;
  const ObSelectStmt *select_stmt = get_stmt();
  if (OB_ISNULL(select_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    // classify subquery filters and rownum filters
    ObSEArray<ObRawExpr*, 8> filter_exprs;
    const ObIArray<ObRawExpr *> &condition_exprs = select_stmt->get_condition_exprs();
    for (int64_t i = 0; OB_SUCC(ret) && i < condition_exprs.count(); i++) {
      ObRawExpr *temp = NULL;
      if (OB_ISNULL(temp = condition_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null exprs", K(ret));
      } else if (temp->has_flag(CNT_ROWNUM)) {
        if (OB_FAIL(add_rownum_expr(temp))) {
          LOG_WARN("failed to add rownum expr", K(ret));
        } else { /*do nothing*/ }
      } else if (temp->has_flag(CNT_SUB_QUERY)) {
        if (OB_FAIL(add_subquery_filter(temp))) {
          LOG_WARN("failed to add subquery filter", K(ret));
        } else { /*do nothing*/ }
      } else {
        if (OB_FAIL(filter_exprs.push_back(temp))) {
          LOG_WARN("failed to push back expr", K(ret));
        } else if (!temp->has_flag(CNT_ONETIME)) {
          // do nothing
        } else if (OB_FAIL(add_subquery_filter(temp))) {
          LOG_WARN("failed to add onetime filter", K(ret));
        }
      }
    }
    // make candidate plan
    if (OB_SUCC(ret)) {
      ObLogicalOperator *top = NULL;
      if (OB_FAIL(allocate_expr_values_as_top(top, &filter_exprs))) {
        LOG_WARN("failed to allocate expr values", K(ret));
      } else if (OB_FAIL(make_candidate_plans(top))) {
        LOG_WARN("failed to make candidate plans", K(ret));
      } else {
        LOG_TRACE("succeed to allocate expr values operator", K(ret));
      }
    }

    // allocate top operator of plan tree
    if (OB_SUCC(ret)) {
      if (OB_FAIL(allocate_plan_top())) {
        LOG_WARN("failed to allocate top operators for expr select", K(ret));
      } else {
        LOG_TRACE("succeed to allocate top operators for expr select", K(ret));
      }
    }
  }
  return ret;
}

int ObSelectLogPlan::candi_allocate_subplan_filter_for_select_item()
{
  int ret = OB_SUCCESS;
  const ObSelectStmt *select_stmt = NULL;
  ObSEArray<ObRawExpr*, 4> select_exprs;
  ObSEArray<ObRawExpr*, 4> subquery_exprs;
  if (OB_ISNULL(select_stmt = get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null stmt", K(select_stmt), K(ret));
  } else if (OB_FAIL(select_stmt->get_select_exprs(select_exprs))) {
    LOG_WARN("failed to get select exprs", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::get_subquery_exprs(select_exprs,
                                                         subquery_exprs))) {
    LOG_WARN("failed to get subquery exprs", K(ret));
  } else if (subquery_exprs.empty()) {
    /*do nothing*/
  } else if (OB_FAIL(candi_allocate_subplan_filter(subquery_exprs))) {
    LOG_WARN("failed to allocate subplan filter for select item", K(ret));
  } else {
    LOG_TRACE("succeed to allocate subplan filter for select item", K(select_stmt->get_stmt_id()));
  }
  return ret;
}

int ObSelectLogPlan::generate_child_plan_for_set(const ObDMLStmt *sub_stmt,
                                                 ObSelectLogPlan *&sub_plan,
                                                 ObIArray<ObRawExpr*> &pushdown_filters,
                                                 const uint64_t child_offset,
                                                 const bool is_set_distinct)
{
  int ret = OB_SUCCESS;
  sub_plan = NULL;
  OPT_TRACE_BEGIN_SECTION;
  OPT_TRACE_TITLE("start generate child plan for set");
  if (OB_ISNULL(sub_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sub_stmt is null", K(ret), K(sub_stmt));
  } else if (OB_ISNULL(sub_plan = static_cast<ObSelectLogPlan*>
                       (optimizer_context_.get_log_plan_factory().create(optimizer_context_,
                                                                         *sub_stmt)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("Failed to create logical plan", K(sub_plan), K(ret));
  } else if (FALSE_IT(sub_plan->set_is_parent_set_distinct(is_set_distinct))) {
    // do nothing
  } else if (OB_FAIL(sub_plan->add_pushdown_filters(pushdown_filters))) {
    LOG_WARN("failed to add pushdown filters", K(ret));
  } else if (OB_FAIL(sub_plan->generate_raw_plan())) {
    LOG_WARN("Failed to generate plan for sub_stmt", K(ret));
  } else if (OB_FAIL(init_selectivity_metas_for_set(sub_plan, child_offset))) {
    LOG_WARN("failed to init selectivity metas for set", K(ret));
  }
  OPT_TRACE_TITLE("end generate child plan for set");
  OPT_TRACE_END_SECTION;
  return ret;
}

int ObSelectLogPlan::decide_sort_keys_for_runion(const common::ObIArray<OrderItem> &order_items,
                                                 common::ObIArray<OrderItem> &new_order_items)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *best_plan = NULL;
  const ObSelectStmt *select_stmt = NULL;
  ObSEArray<ObRawExpr*, 8> select_exprs;
  ObSEArray<OrderItem, 8> candi_order_items;
  if (OB_ISNULL(select_stmt = get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null stmt", K(select_stmt), K(ret));
  } else if (OB_FAIL(select_stmt->get_select_exprs(select_exprs))) {
    LOG_WARN("failed to get select exprs", K(ret));
  } else if (OB_FAIL(candidates_.get_best_plan(best_plan))) {
    LOG_WARN("failed to get best plan", K(ret));
  } else if (OB_ISNULL(best_plan)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
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
          } else if (real_expr->is_const_expr()) {
            // do nothing.
          } else {
            OrderItem real_item = order_items.at(i);
            real_item.expr_ = real_expr;
            if (OB_FAIL(candi_order_items.push_back(real_item))) {
              LOG_WARN("add real sort item error");
            }
          }
        }
      }
    }
    if (OB_SUCC(ret) && !candi_order_items.empty()) {
      if (OB_FAIL(ObOptimizerUtil::simplify_ordered_exprs(best_plan->get_fd_item_set(),
                                                          best_plan->get_output_equal_sets(),
                                                          best_plan->get_output_const_exprs(),
                                                          get_onetime_query_refs(),
                                                          candi_order_items,
                                                          new_order_items))) {
        LOG_WARN("failed to simplify exprs", K(ret));
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

int ObSelectLogPlan::candi_allocate_window_function()
{
  int ret = OB_SUCCESS;
  const ObSelectStmt *stmt = get_stmt();
  ObSEArray<ObRawExpr*, 8> candi_subquery_exprs;
  ObSEArray<CandidatePlan, 8> win_func_plans;
  if (OB_ISNULL(stmt) || OB_UNLIKELY(!stmt->has_window_function())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected params", K(ret), K(stmt), K(stmt->has_window_function()));
  } else if (OB_FAIL(append(candi_subquery_exprs, stmt->get_window_func_exprs()))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else if (OB_FAIL(candi_allocate_subplan_filter_for_exprs(candi_subquery_exprs))) {
    LOG_WARN("failed to do allocate subplan filter", K(ret));
  } else if (OB_FAIL(candi_allocate_window_function_with_hint(stmt->get_window_func_exprs(),
                                                              win_func_plans))) {
    LOG_WARN("failed to allocate window function with hint", K(ret));
  } else if (!win_func_plans.empty()) {
    LOG_TRACE("succeed to allocate window function using hint", K(win_func_plans.count()));
  } else if (OB_FAIL(get_log_plan_hint().check_status())) {
    LOG_WARN("failed to generate plans with hint", K(ret));
  } else if (OB_FAIL(candi_allocate_window_function(stmt->get_window_func_exprs(),
                                                    win_func_plans))) {
    LOG_WARN("failed to allocate window function", K(ret));
  } else {
    LOG_TRACE("succeed to allocate window function without hint", K(win_func_plans.count()));
  }

  // choose the best plan
  if (OB_SUCC(ret)) {
    int64_t check_scope = OrderingCheckScope::CHECK_DISTINCT |
                          OrderingCheckScope::CHECK_SET |
                          OrderingCheckScope::CHECK_ORDERBY;
    if (OB_UNLIKELY(win_func_plans.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected empty window function plans", K(ret));
    } else if (OB_FAIL(update_plans_interesting_order_info(win_func_plans, check_scope))) {
      LOG_WARN("failed to update plans interesting order info", K(ret));
    } else if (OB_FAIL(prune_and_keep_best_plans(win_func_plans))) {
      LOG_WARN("failed to add win func plans", K(ret));
    }
  }
  return ret;
}

// 1. adjust ordering
// 2. find an ordering match
// 3. calc ndv and pby oby count
// 4. method matched
// window functions in win_func_exprs must keep the same ordering with win_func_exprs_ in stmt
int ObSelectLogPlan::candi_allocate_window_function_with_hint(const ObIArray<ObWinFunRawExpr*> &win_func_exprs,
                                                              common::ObIArray<CandidatePlan> &total_plans)
{
  int ret = OB_SUCCESS;
  total_plans.reuse();
  const ObWindowDistHint *win_dist_hint = get_log_plan_hint().get_window_dist();
  bool is_valid = false;
  ObSEArray<CandidatePlan, 16> candi_plans;
  ObSEArray<ObWinFunRawExpr*, 8> remaining_exprs;
  ObLogicalOperator *orig_top = NULL;
  if (OB_FAIL(check_is_win_func_hint_valid(win_func_exprs, win_dist_hint, is_valid))) {
    LOG_WARN("failed to assign candidate plans", K(ret));
  } else if (!is_valid) {
    /* do nothing */
  } else if (OB_FAIL(candi_plans.assign(candidates_.candidate_plans_))) {
    LOG_WARN("failed to assign candidate plans", K(ret));
  } else if (OB_FAIL(remaining_exprs.assign(win_func_exprs))) {
    LOG_WARN("failed to assign remaining exprs", K(ret));
  } else if (OB_UNLIKELY(candi_plans.empty()) || OB_ISNULL(orig_top = candi_plans.at(0).plan_tree_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(candi_plans.count()), K(orig_top));
  } else {
    ObSEArray<CandidatePlan, 16> tmp_plans;
    WinFuncOpHelper win_func_helper(win_func_exprs,
                                    win_dist_hint,
                                    true,
                                    orig_top->get_fd_item_set(),
                                    orig_top->get_output_equal_sets(),
                                    orig_top->get_output_const_exprs(),
                                    orig_top->get_card(),
                                    orig_top->get_is_at_most_one_row());
    while (OB_SUCC(ret) && !candi_plans.empty() && !remaining_exprs.empty()) {
      tmp_plans.reuse();
      if (OB_FAIL(init_win_func_helper_with_hint(candi_plans,
                                                remaining_exprs,
                                                win_func_helper,
                                                is_valid))) {
        LOG_WARN("failed to init win_func_helper with hint", K(ret));
      } else if (!is_valid) {
        candi_plans.reuse();
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < candi_plans.count(); ++i) {
          if (OB_FAIL(calc_win_func_helper_with_hint(candi_plans.at(i).plan_tree_,
                                                    win_func_helper,
                                                    is_valid))) {
            LOG_WARN("failed to calc win_func_helper with hint", K(ret));
          } else if (!is_valid) {
            /* do nothing */
          } else if (OB_FAIL(create_one_window_function(candi_plans.at(i),
                                                        win_func_helper,
                                                        tmp_plans))) {
            LOG_WARN("failed to create one window function", K(ret));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(candi_plans.assign(tmp_plans))) {
          LOG_WARN("failed to assign candidate plans", K(ret));
        } else {
          ++win_func_helper.win_op_idx_;
        }
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(total_plans.assign(candi_plans))) {
      LOG_WARN("failed to assign candidate plans", K(ret));
    }
  }
  return ret;
}

// precheck the window function hint is valid
int ObSelectLogPlan::check_is_win_func_hint_valid(const ObIArray<ObWinFunRawExpr*> &all_win_exprs,
                                                  const ObWindowDistHint *hint,
                                                  bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  ObSEArray<ObWinFunRawExpr*, 8> win_exprs;
  if (NULL == hint) {
    is_valid = false;
  } else if (OB_FAIL(win_exprs.assign(all_win_exprs))) {
    LOG_WARN("failed to assign window functions", K(ret));
  } else {
    const ObIArray<ObWindowDistHint::WinDistOption> &dist_options = hint->get_win_dist_options();
    int64_t win_func_cnt = 0;
    for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < dist_options.count(); ++i) {
      const ObWindowDistHint::WinDistOption &option = dist_options.at(i);
      if (!option.is_valid()) {
        is_valid = false;
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && is_valid && j < option.win_func_idxs_.count(); ++j) {
          const int64_t idx = option.win_func_idxs_.at(j);
          if (idx < 0 || idx >= win_exprs.count() || NULL == win_exprs.at(idx)) {
            is_valid = false;
          } else {
            ++win_func_cnt;
            win_exprs.at(idx) = NULL;
          }
        }
      }
    }
    if (OB_SUCC(ret) && is_valid) {
      is_valid = all_win_exprs.count() == win_func_cnt;
    }
  }
  LOG_TRACE("finish check is win_func_hint valid. ", K(is_valid));
  return ret;
}

// init WinFuncOpHelper by outline hint:
//  1. get one group window functions described in hint
//  2. decide current group using hint, include: dist method/sort method/need pushdown
int ObSelectLogPlan::init_win_func_helper_with_hint(const ObIArray<CandidatePlan> &candi_plans,
                                                    ObIArray<ObWinFunRawExpr*> &remaining_exprs,
                                                    WinFuncOpHelper &win_func_helper,
                                                    bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  if (win_func_helper.all_win_func_exprs_.count() == remaining_exprs.count()) {
    win_func_helper.win_op_idx_ = 0;
  }
  int64_t &win_op_idx = win_func_helper.win_op_idx_;
  const ObIArray<ObWinFunRawExpr*> &all_win_exprs = win_func_helper.all_win_func_exprs_;
  const ObWindowDistHint *hint = win_func_helper.win_dist_hint_;
  ObRawExpr *wf_aggr_status_expr = NULL;
  if (OB_ISNULL(hint) || OB_ISNULL(get_optimizer_context().get_session_info())
      || OB_UNLIKELY(remaining_exprs.empty() || win_op_idx < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected params", K(ret), K(hint), K(remaining_exprs.empty()), K(win_op_idx));
  } else if (win_op_idx >= hint->get_win_dist_options().count()) {
    is_valid = false;
  } else {
    const ObWindowDistHint::WinDistOption &option = hint->get_win_dist_options().at(win_op_idx);
    win_func_helper.win_dist_method_ = option.algo_;
    win_func_helper.force_normal_sort_ = !option.use_hash_sort_;
    win_func_helper.force_hash_sort_ = option.use_hash_sort_;
    win_func_helper.force_no_pushdown_ = !option.is_push_down_;
    win_func_helper.force_pushdown_ = option.is_push_down_;
    win_func_helper.wf_aggr_status_expr_ = NULL;
    bool ordering_changed = false;
    ObSEArray<ObWinFunRawExpr*, 8> current_exprs;
    for (int64_t i = 0; OB_SUCC(ret) && i < option.win_func_idxs_.count(); ++i) {
      const int64_t idx = option.win_func_idxs_.at(i);
      if (OB_UNLIKELY(idx < 0 || idx >= all_win_exprs.count())) {
        ret = OB_ERR_UNEXPECTED; // check_is_win_func_hint_valid has checked, throw out error here
        LOG_WARN("unexpected params", K(ret), K(all_win_exprs.count()), K(idx));
      } else if (OB_FAIL(current_exprs.push_back(all_win_exprs.at(idx)))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(sort_window_functions(win_func_helper.fd_item_set_,
                                             win_func_helper.equal_sets_,
                                             win_func_helper.const_exprs_,
                                             current_exprs,
                                             win_func_helper.ordered_win_func_exprs_,
                                             ordering_changed))) {
      LOG_WARN("adjust window functions failed", K(ret));
    } else if (ordering_changed || current_exprs.empty()) {
      is_valid = false;
    } else if (OB_FAIL(ObOptimizerUtil::remove_item(remaining_exprs, current_exprs))) {
      LOG_WARN("failed to remove items", K(ret));
    } else if (OB_FAIL(extract_window_function_partition_exprs(win_func_helper))) {
      LOG_WARN("failed to extract partition exprs", K(ret));
    } else if (WinDistAlgo::WIN_DIST_HASH == win_func_helper.win_dist_method_ &&
               !win_func_helper.force_no_pushdown_ &&
               OB_FAIL(ObRawExprUtils::build_inner_wf_aggr_status_expr(
                                            get_optimizer_context().get_expr_factory(),
                                            *get_optimizer_context().get_session_info(),
                                            win_func_helper.wf_aggr_status_expr_))) {
      LOG_WARN("failed to build inner wf aggr status expr", K(ret));
    }
    LOG_TRACE("finish init win_func_helper with hint. ", K(is_valid), K(win_func_helper));
  }
  return ret;
}

int ObSelectLogPlan::calc_win_func_helper_with_hint(const ObLogicalOperator *op,
                                                    WinFuncOpHelper &win_func_helper,
                                                    bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  if (OB_ISNULL(op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (OB_FAIL(gen_win_func_sort_keys(op->get_op_ordering(), win_func_helper, is_valid))) {
    LOG_WARN("failed to gen win func sort keys", K(ret));
  } else if (!is_valid) {
    /* do nothing */
  } else if (OB_FAIL(calc_ndvs_and_pby_oby_prefix(win_func_helper.ordered_win_func_exprs_,
                                                  win_func_helper,
                                                  win_func_helper.pby_oby_prefixes_))) {
    LOG_WARN("failed to calc ndvs and pby oby prefix", K(ret));
  } else if (OB_FAIL(calc_partition_count(win_func_helper))) {
    LOG_WARN("failed to get partition count", K(ret));
  }
  return ret;
}

int ObSelectLogPlan::check_win_dist_method_valid(const WinFuncOpHelper &win_func_helper,
                                                 bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  const WinDistAlgo method = win_func_helper.win_dist_method_;
  if (WinDistAlgo::WIN_DIST_NONE == method) {
    /* do nothing */
  } else {
    const ObIArray<ObWinFunRawExpr*> &win_func_exprs = win_func_helper.ordered_win_func_exprs_;
    const ObIArray<std::pair<int64_t,int64_t>> &pby_oby_prefixes = win_func_helper.pby_oby_prefixes_;
    const std::pair<int64_t,int64_t> *first_pby_oby_prefix = NULL;
    bool range_dist_suppored = false;
    for (int64_t i = 0; is_valid && OB_SUCC(ret) && i < win_func_exprs.count(); ++i) {
      const int64_t pby_cnt = pby_oby_prefixes.at(i).first;
      const int64_t pby_oby_cnt = pby_oby_prefixes.at(i).second;
      switch (method) {
        case WinDistAlgo::WIN_DIST_HASH:  {
          is_valid = pby_cnt > 0;
          break;
        }
        case WIN_DIST_RANGE:
        case WIN_DIST_LIST: {
          if (pby_oby_cnt <= 0 || pby_oby_cnt == pby_cnt) {
            is_valid = false;
          } else if (OB_FAIL(check_wf_range_dist_supported(win_func_exprs.at(i), range_dist_suppored))) {
            LOG_WARN("check window function range distribute parallel supported failed", K(ret));
          } else if (NULL == first_pby_oby_prefix) {
            first_pby_oby_prefix = &pby_oby_prefixes.at(i);
          } else {
            is_valid = pby_oby_prefixes.at(i) == *first_pby_oby_prefix;
          }
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected win dist type", K(ret), K(method));
        }
      }
    }
  }
  return ret;
}

int ObSelectLogPlan::candi_allocate_window_function(const ObIArray<ObWinFunRawExpr*> &win_func_exprs,
                                                    ObIArray<CandidatePlan> &total_plans)
{
  int ret = OB_SUCCESS;
  total_plans.reuse();
  ObLogicalOperator *orig_top = NULL;
  ObIArray<CandidatePlan> &candi_plans = candidates_.candidate_plans_;
  if (OB_UNLIKELY(candi_plans.empty()) || OB_ISNULL(orig_top = candi_plans.at(0).plan_tree_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(candi_plans.count()), K(orig_top));
  } else {
    OPT_TRACE_TITLE("start generate window function");
    ObSEArray<ObOpPseudoColumnRawExpr*, 4> status_exprs;
    WinFuncOpHelper win_func_helper(win_func_exprs,
                                    get_log_plan_hint().get_window_dist(),
                                    false,
                                    orig_top->get_fd_item_set(),
                                    orig_top->get_output_equal_sets(),
                                    orig_top->get_output_const_exprs(),
                                    orig_top->get_card(),
                                    orig_top->get_is_at_most_one_row());
    for (int64_t i = 0; OB_SUCC(ret) && i < candi_plans.count(); ++i) {
      OPT_TRACE("generate window function for plan:", candi_plans.at(i));
      if (OB_FAIL(generate_window_functions_plan(win_func_helper,
                                                 status_exprs,
                                                 total_plans,
                                                 candi_plans.at(i)))) {
        LOG_WARN("failed to allocate window functions", K(ret));
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

int ObSelectLogPlan::generate_window_functions_plan(WinFuncOpHelper &win_func_helper,
                                                    ObIArray<ObOpPseudoColumnRawExpr*> &status_exprs,
                                                    ObIArray<CandidatePlan> &total_plans,
                                                    CandidatePlan &orig_candidate_plan)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObWinFunRawExpr*, 8> remaining_exprs;
  ObSEArray<CandidatePlan, 8> local_plans;
  if (OB_ISNULL(orig_candidate_plan.plan_tree_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("got NULL plan tree", K(ret), K(orig_candidate_plan));
  } else if (OB_FAIL(local_plans.push_back(orig_candidate_plan))) {
    LOG_WARN("failed to push back original candidate plan", K(ret));
  } else if (OB_FAIL(remaining_exprs.assign(win_func_helper.all_win_func_exprs_))) {
    LOG_WARN("failed to assign remaining exprs", K(ret));
  } else {
    int64_t stmt_func_idx = 0;
    ObSEArray<CandidatePlan, 8> tmp_plans;
    ObSEArray<ObWinFunRawExpr*, 8> ordered_win_func_exprs;
    ObSEArray<std::pair<int64_t, int64_t>, 8> pby_oby_prefixes;
    ObSEArray<int64_t, 8> split;
    ObSEArray<WinDistAlgo, 8> methods;
    const bool distributed = orig_candidate_plan.plan_tree_->is_distributed();
    while (OB_SUCC(ret) && !remaining_exprs.empty()) {
      if (OB_UNLIKELY(local_plans.empty()) || OB_ISNULL(local_plans.at(0).plan_tree_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected params", K(ret), K(local_plans.empty()));
      } else if (OB_FAIL(prepare_next_group_win_funcs(distributed,
                                                      local_plans.at(0).plan_tree_->get_op_ordering(),
                                                      local_plans.at(0).plan_tree_->get_parallel(),
                                                      win_func_helper,
                                                      remaining_exprs,
                                                      ordered_win_func_exprs,
                                                      pby_oby_prefixes,
                                                      split,
                                                      methods))) {
        LOG_WARN("failed to prepare next group window functions", K(ret), K(win_func_helper));
      }
      for (int64_t si = 0; OB_SUCC(ret) && si < split.count(); si++) {
        if (OB_FAIL(init_win_func_helper(ordered_win_func_exprs,
                                         pby_oby_prefixes,
                                         split,
                                         methods,
                                         si,
                                         status_exprs,
                                         win_func_helper))) {
          LOG_WARN("failed to init win func helper", K(ret));
        } else {
          tmp_plans.reuse();
          for (int64_t i = 0; OB_SUCC(ret) && i < local_plans.count(); ++i) {
            if (OB_FAIL(create_one_window_function(local_plans.at(i), win_func_helper, tmp_plans))) {
              LOG_WARN("failed to create one window function", K(ret));
            }
          }
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(local_plans.assign(tmp_plans))) {
            LOG_WARN("array assign failed", K(ret));
          } else {
            ++win_func_helper.win_op_idx_;
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(append(total_plans, local_plans))) {
        LOG_WARN("failed to append local plans");
      }
    }
  }
  return ret;
}

/**
 * @brief 假设 win_func_exprs 都采用 sort_keys 进行排序
 * 同一个分组中的 win_expr 应该按照一定的顺序排序 (`sort_window_functions()`)
 * 同一个ObWindowFunction中的多个 win_expr 要按照一定的顺序组织，譬如：
 * w1(c) over (partition by e1, e2, e3)
 * w2(c) over (partition by e1)
 * w3(c) over (partition by e1 e2)
 * 一定要按照 w1, w3, w2 的顺序来组织。即排在后面的win_expr的partition表达式是前面对象的子集。
 * 这里窗口函数会按照分组表达式数量进行排序（只统计非常量的分组表达式数量，数量多的排在前）。
 */
int ObSelectLogPlan::create_one_window_function(CandidatePlan &candidate_plan,
                                                const WinFuncOpHelper &win_func_helper,
                                                ObIArray<CandidatePlan> &all_plans)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *top = NULL;
  int64_t prefix_pos = 0;
  bool need_sort = false;
  int64_t part_cnt = 0;
  if (OB_ISNULL(top = candidate_plan.plan_tree_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("got NULL plan tree", K(ret));
  } else if (OB_FAIL(check_win_func_need_sort(*top,
                                              win_func_helper,
                                              need_sort,
                                              prefix_pos,
                                              part_cnt))) {
    LOG_WARN("failed to check win func need sort", K(ret));
  } else if (OB_FAIL(create_range_list_dist_win_func(top,
                                                     win_func_helper,
                                                     part_cnt,
                                                     all_plans))) {
    LOG_WARN("failed to create range list dist window functions", K(ret));
  } else if (OB_FAIL(create_none_dist_win_func(top,
                                               win_func_helper,
                                               need_sort,
                                               prefix_pos,
                                               part_cnt,
                                               all_plans))) {
    LOG_WARN("failed to create push down window function", K(ret));
  } else if (OB_FAIL(create_hash_dist_win_func(top,
                                               win_func_helper,
                                               need_sort,
                                               prefix_pos,
                                               part_cnt,
                                               all_plans))) {
    LOG_WARN("failed to create hash dist win func window function", K(ret));
  }
  return ret;
}

int ObSelectLogPlan::check_win_func_need_sort(const ObLogicalOperator &top,
                                              const WinFuncOpHelper &win_func_helper,
                                              bool &need_sort,
                                              int64_t &prefix_pos,
                                              int64_t &part_cnt)
{
  int ret = OB_SUCCESS;
  part_cnt = win_func_helper.part_cnt_;
  need_sort = true;
  prefix_pos = 0;
  if (OB_FAIL(ObOptimizerUtil::check_need_sort(win_func_helper.sort_keys_,
                                               top.get_op_ordering(),
                                               top.get_fd_item_set(),
                                               top.get_output_equal_sets(),
                                               top.get_output_const_exprs(),
                                               get_onetime_query_refs(),
                                               top.get_is_at_most_one_row(),
                                               need_sort,
                                               prefix_pos))) {
    LOG_WARN("failed to check need sort", K(ret));
  } else if (prefix_pos > 0) {
    /* if has prefix, disable hash sort */
    part_cnt = 0;
  } else if (!need_sort || 0 >= part_cnt) {
    /* need not hash sort */
    part_cnt = 0;
  } else if (OB_FAIL(ObOptimizerUtil::check_need_sort(win_func_helper.sort_keys_,
                                                      top.get_op_ordering(),
                                                      top.get_fd_item_set(),
                                                      top.get_output_equal_sets(),
                                                      top.get_output_const_exprs(),
                                                      get_onetime_query_refs(),
                                                      top.get_is_at_most_one_row(),
                                                      need_sort,
                                                      prefix_pos,
                                                      part_cnt,
                                                      true))) {
    LOG_WARN("failed to check need sort", K(ret));
  }
  return ret;
}

int ObSelectLogPlan::prepare_next_group_win_funcs(const bool distributed,
                                                  const ObIArray<OrderItem> &op_ordering,
                                                  const int64_t dop,
                                                  WinFuncOpHelper &win_func_helper,
                                                  ObIArray<ObWinFunRawExpr*> &remaining_exprs,
                                                  ObIArray<ObWinFunRawExpr*> &ordered_win_func_exprs,
                                                  ObIArray<std::pair<int64_t, int64_t>> &pby_oby_prefixes,
                                                  ObIArray<int64_t> &split,
                                                  ObIArray<WinDistAlgo> &methods)
{
  int ret = OB_SUCCESS;
  bool ordering_changed = false;
  ObSEArray<ObWinFunRawExpr*, 8> current_exprs;
  ordered_win_func_exprs.reuse();
  pby_oby_prefixes.reuse();
  split.reuse();
  methods.reuse();
  if (win_func_helper.all_win_func_exprs_.count() == remaining_exprs.count()) {
    win_func_helper.win_op_idx_ = 0;
  }
  if (OB_UNLIKELY(remaining_exprs.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected params", K(ret), K(remaining_exprs.count()));
  } else if (OB_FAIL(get_next_group_window_exprs(op_ordering,
                                                win_func_helper,
                                                remaining_exprs,
                                                current_exprs))) {
    LOG_WARN("failed to get next window exprs", K(ret));
  } else if (OB_FAIL(sort_window_functions(win_func_helper.fd_item_set_,
                                           win_func_helper.equal_sets_,
                                           win_func_helper.const_exprs_,
                                           current_exprs,
                                           ordered_win_func_exprs,
                                           ordering_changed))) {
    LOG_WARN("adjust window functions failed", K(ret));
  } else if (OB_FAIL(calc_ndvs_and_pby_oby_prefix(ordered_win_func_exprs,
                                                  win_func_helper,
                                                  pby_oby_prefixes))) {
    LOG_WARN("failed to calc ndvs and pby oby prefix", K(ret));
  } else if (OB_FAIL(split_win_funcs_by_dist_method(distributed,
                                                    ordered_win_func_exprs,
                                                    win_func_helper.sort_key_ndvs_,
                                                    pby_oby_prefixes,
                                                    dop,
                                                    win_func_helper.card_,
                                                    win_func_helper.win_dist_hint_,
                                                    win_func_helper.win_op_idx_,
                                                    split,
                                                    methods))) {
    LOG_WARN("split window function by distribute method failed", K(ret));
  }
  return ret;
}

int ObSelectLogPlan::init_win_func_helper(const ObIArray<ObWinFunRawExpr*> &ordered_win_func_exprs,
                                          const ObIArray<std::pair<int64_t, int64_t>> &pby_oby_prefixes,
                                          const ObIArray<int64_t> &split,
                                          const ObIArray<WinDistAlgo> &methods,
                                          const int64_t splict_idx,
                                          ObIArray<ObOpPseudoColumnRawExpr*> &status_exprs,
                                          WinFuncOpHelper &win_func_helper)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(splict_idx < 0 || splict_idx >= split.count()
                  || split.count() != methods.count()
                  || WinDistAlgo::WIN_DIST_INVALID == methods.at(splict_idx)
                  || pby_oby_prefixes.count() != ordered_win_func_exprs.count()
                  || split.at(splict_idx) > ordered_win_func_exprs.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected params", K(ret), K(splict_idx), K(split), K(methods),
                                  K(pby_oby_prefixes), K(ordered_win_func_exprs.count()));
  } else {
    win_func_helper.win_dist_method_ = methods.at(splict_idx);
    win_func_helper.ordered_win_func_exprs_.reuse();
    win_func_helper.pby_oby_prefixes_.reuse();
    win_func_helper.wf_aggr_status_expr_ = NULL;
    ObOpPseudoColumnRawExpr *status_expr = NULL;
    const int64_t start = 0 == splict_idx ? 0 : split.at(splict_idx - 1);
    const int64_t end = split.at(splict_idx);
    for (int64_t i = start; OB_SUCC(ret) && i < end; ++i) {
      if (OB_FAIL(win_func_helper.ordered_win_func_exprs_.push_back(ordered_win_func_exprs.at(i)))
          || OB_FAIL(win_func_helper.pby_oby_prefixes_.push_back(pby_oby_prefixes.at(i)))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(extract_window_function_partition_exprs(win_func_helper))) {
      LOG_WARN("failed to extract partition exprs", K(ret));
    } else if (OB_FAIL(calc_partition_count(win_func_helper))) {
      LOG_WARN("failed to get partition count", K(ret));
    } else if (WinDistAlgo::WIN_DIST_HASH != win_func_helper.win_dist_method_) {
      /* do nothing */
    } else if (OB_UNLIKELY(win_func_helper.win_op_idx_ < 0)
               || OB_ISNULL(get_optimizer_context().get_session_info())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected params", K(ret), K(win_func_helper.win_op_idx_),
                                          K(get_optimizer_context().get_session_info()));
    } else if (OB_FAIL(status_exprs.prepare_allocate(win_func_helper.win_op_idx_ + 1))) {
      LOG_WARN("array prepare allocate failed", K(ret));
    } else if (NULL != (win_func_helper.wf_aggr_status_expr_ = status_exprs.at(win_func_helper.win_op_idx_))) {
      /* do nothing */
    } else if (OB_FAIL(ObRawExprUtils::build_inner_wf_aggr_status_expr(
                                              get_optimizer_context().get_expr_factory(),
                                              *get_optimizer_context().get_session_info(),
                                              win_func_helper.wf_aggr_status_expr_))) {
      LOG_WARN("failed to build inner wf aggr status expr", K(ret));
    } else {
      status_exprs.at(win_func_helper.win_op_idx_) = win_func_helper.wf_aggr_status_expr_;
    }
    LOG_TRACE("finish init win_func_helper. ", K(win_func_helper));
  }
  return ret;
}

int ObSelectLogPlan::calc_partition_count(WinFuncOpHelper &win_func_helper)
{
  int ret = OB_SUCCESS;
  win_func_helper.part_cnt_ = 0;
  int64_t part_cnt = 0;
  const ObIArray<std::pair<int64_t, int64_t>> &pby_oby_prefixes = win_func_helper.pby_oby_prefixes_;
  if (OB_UNLIKELY(pby_oby_prefixes.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get empty pby_oby_prefixes", K(ret), K(pby_oby_prefixes.count()));
  } else if (win_func_helper.partition_exprs_.empty()) {
    part_cnt = 0;
  } else {
    part_cnt = pby_oby_prefixes.at(0).first;
    for (int64_t i = 1; OB_SUCC(ret) && i < pby_oby_prefixes.count(); ++i) {
      if (pby_oby_prefixes.at(i).first < part_cnt) {
        part_cnt = pby_oby_prefixes.at(i).first;
      }
    }
  }
  if (OB_SUCC(ret)) {
    win_func_helper.part_cnt_ = part_cnt;
  }
  return ret;
}

int ObSelectLogPlan::get_next_group_window_exprs(const ObIArray<OrderItem> &op_ordering,
                                                 WinFuncOpHelper &win_func_helper,
                                                 ObIArray<ObWinFunRawExpr*> &remaining_exprs,
                                                 ObIArray<ObWinFunRawExpr*> &current_exprs)
{
  int ret = OB_SUCCESS;
  ObIArray<OrderItem> &sort_keys = win_func_helper.sort_keys_;
  current_exprs.reuse();
  sort_keys.reuse();
  ObSEArray<ObWinFunRawExpr*, 8> no_need_sort_exprs;
  ObSEArray<ObWinFunRawExpr*, 8> no_need_order_exprs;
  ObSEArray<ObWinFunRawExpr*, 8> rest_win_func_exprs;
  ObSEArray<OrderItem, 8> best_sort_keys;
  ObSEArray<OrderItem, 8> possible_sort_keys;
  ObSEArray<OrderItem, 8> tmp_sort_keys;
  // zhanyuetodo: this op_ordering is from from the first plan, need adjust this later
  if (OB_FAIL(classify_window_exprs(win_func_helper,
                                    op_ordering,
                                    remaining_exprs,
                                    no_need_sort_exprs,
                                    no_need_order_exprs,
                                    rest_win_func_exprs,
                                    best_sort_keys,
                                    possible_sort_keys))) {
    LOG_WARN("failed to classify window exprs", K(ret));
  } else if (!no_need_sort_exprs.empty()) {
    // get window function group need not allocate sort
    if (OB_FAIL(sort_keys.assign(best_sort_keys))) {
      LOG_WARN("failed to assign sort keys", K(ret));
    } else if (OB_FAIL(current_exprs.assign(no_need_sort_exprs))) {
      LOG_WARN("failed to assign win func exprs", K(ret));
    }
  } else if (rest_win_func_exprs.empty()) {
    // do nothing, get window function group has not expected ordering
  } else if (OB_FAIL(classify_window_exprs(win_func_helper,
                                          possible_sort_keys,
                                          remaining_exprs,
                                          no_need_sort_exprs,
                                          no_need_order_exprs,
                                          rest_win_func_exprs,
                                          best_sort_keys,
                                          tmp_sort_keys))) {
    LOG_WARN("failed to classify window exprs", K(ret));
  } else if (OB_FAIL(sort_keys.assign(possible_sort_keys))) {
    LOG_WARN("failed to assign sort keys", K(ret));
  } else if (OB_FAIL(current_exprs.assign(no_need_sort_exprs))) {
    LOG_WARN("failed to assign win func exprs", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(remaining_exprs.assign(rest_win_func_exprs))) {
    LOG_WARN("failed to assign win func exprs", K(ret));
  } else if (OB_FAIL(append(rest_win_func_exprs.empty() ? current_exprs : remaining_exprs,
                            no_need_order_exprs))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else if (OB_UNLIKELY(current_exprs.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get empty next group window exprs", K(ret));
  } else {
    LOG_TRACE("succeed to get current group window exprs", K(current_exprs), K(remaining_exprs),
                K(sort_keys));
  }
  return ret;
}

int ObSelectLogPlan::gen_win_func_sort_keys(const ObIArray<OrderItem> &input_ordering,
                                            WinFuncOpHelper &win_func_helper,
                                            bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  const ObIArray<ObWinFunRawExpr*> &win_func_exprs = win_func_helper.ordered_win_func_exprs_;
  ObSEArray<ObWinFunRawExpr*, 8> no_need_sort_exprs;
  ObSEArray<ObWinFunRawExpr*, 8> no_need_order_exprs;
  ObSEArray<ObWinFunRawExpr*, 8> rest_win_func_exprs;
  ObSEArray<OrderItem, 8> best_sort_keys;
  ObSEArray<OrderItem, 8> possible_sort_keys;
  ObSEArray<OrderItem, 8> tmp_sort_keys;
  if (OB_FAIL(classify_window_exprs(win_func_helper,
                                    input_ordering,
                                    win_func_exprs,
                                    no_need_sort_exprs,
                                    no_need_order_exprs,
                                    rest_win_func_exprs,
                                    best_sort_keys,
                                    possible_sort_keys))) {
    LOG_WARN("failed to classify window exprs", K(ret));
  } else if (rest_win_func_exprs.empty()) {
    if (OB_FAIL(win_func_helper.sort_keys_.assign(best_sort_keys))) {
      LOG_WARN("failed to assign exprs", K(ret));
    }
  } else if (OB_FAIL(classify_window_exprs(win_func_helper,
                                          possible_sort_keys,
                                          win_func_exprs,
                                          no_need_sort_exprs,
                                          no_need_order_exprs,
                                          rest_win_func_exprs,
                                          best_sort_keys,
                                          tmp_sort_keys))) {
    LOG_WARN("failed to classify window exprs", K(ret));
  } else if (OB_UNLIKELY(!rest_win_func_exprs.empty())) {
    is_valid = false;
    LOG_TRACE("can not get a sort keys. ", K(input_ordering), K(win_func_exprs));
  } else if (OB_FAIL(win_func_helper.sort_keys_.assign(possible_sort_keys))) {
    LOG_WARN("failed to assign exprs", K(ret));
  }

  return ret;
}

int ObSelectLogPlan::classify_window_exprs(const WinFuncOpHelper &win_func_helper,
                                           const ObIArray<OrderItem> &input_ordering,
                                           const ObIArray<ObWinFunRawExpr*> &remaining_exprs,
                                           ObIArray<ObWinFunRawExpr*> &no_need_sort_exprs,
                                           ObIArray<ObWinFunRawExpr*> &no_need_order_exprs,
                                           ObIArray<ObWinFunRawExpr*> &rest_win_func_exprs,
                                           ObIArray<OrderItem> &best_sort_keys,
                                           ObIArray<OrderItem> &possible_sort_keys)
{
  int ret = OB_SUCCESS;
  no_need_sort_exprs.reuse();
  no_need_order_exprs.reuse();
  rest_win_func_exprs.reuse();
  best_sort_keys.reuse();
  possible_sort_keys.reuse();
  bool need_sort = false;
  int64_t prefix_pos = 0;
  int64_t best_prefix_pos = -1;
  ObWinFunRawExpr* win_expr = NULL;
  ObSEArray<OrderItem, 8> win_sort_keys;
  for (int64_t i = 0; OB_SUCC(ret) && i < remaining_exprs.count(); ++i) {
    win_sort_keys.reuse();
    prefix_pos = 0;
    need_sort = false;
    if (OB_ISNULL(win_expr = remaining_exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(get_sort_keys_for_window_function(win_func_helper.fd_item_set_,
                                                        win_func_helper.equal_sets_,
                                                        win_func_helper.const_exprs_,
                                                        win_expr,
                                                        input_ordering,
                                                        remaining_exprs,
                                                        win_sort_keys))) {
      LOG_WARN("failed to decide sort keys", K(ret));
    } else if (win_sort_keys.empty()) {
      if (OB_FAIL(no_need_order_exprs.push_back(win_expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else { /*do nothing*/ }
    } else if (OB_FAIL(ObOptimizerUtil::check_need_sort(win_sort_keys,
                                                        input_ordering,
                                                        win_func_helper.fd_item_set_,
                                                        win_func_helper.equal_sets_,
                                                        win_func_helper.const_exprs_,
                                                        get_onetime_query_refs(),
                                                        win_func_helper.is_at_most_one_row_,
                                                        need_sort,
                                                        prefix_pos))) {
      LOG_WARN("failed to check if need sort", K(ret));
    } else if (!need_sort) {
      if (OB_FAIL(no_need_sort_exprs.push_back(win_expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else if (best_sort_keys.count() < win_sort_keys.count()
                 && OB_FAIL(best_sort_keys.assign(win_sort_keys))) {
        LOG_WARN("failed to assign sort keys", K(ret));
      }
    } else if (OB_FAIL(rest_win_func_exprs.push_back(win_expr))) {
      LOG_WARN("failed to push back expr", K(ret));
    } else if (best_prefix_pos < prefix_pos
               || (best_prefix_pos == prefix_pos
                   && possible_sort_keys.count() < win_sort_keys.count())) {
      if (OB_FAIL(possible_sort_keys.assign(win_sort_keys))) {
        LOG_WARN("failed to assign exprs", K(ret));
      } else {
        best_prefix_pos = prefix_pos;
      }
    }
  }
  return ret;
}

int ObSelectLogPlan::get_sort_keys_for_window_function(const ObFdItemSet &fd_item_set,
                                                       const EqualSets &equal_sets,
                                                       const ObIArray<ObRawExpr*> &const_exprs,
                                                       const ObWinFunRawExpr *win_expr,
                                                       const ObIArray<OrderItem> &ordering,
                                                       const ObIArray<ObWinFunRawExpr*> &remaining_exprs,
                                                       ObIArray<OrderItem> &sort_keys,
                                                       int64_t *pby_prefix /* = NULL */)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 8> part_exprs;
  ObSEArray<ObOrderDirection, 8> part_directions;
  ObSEArray<OrderItem, 8> output_sort_keys;
  bool is_const = false;
  int64_t prefix_count = -1;
  bool input_ordering_all_used = false;
  sort_keys.reuse();
  if (OB_ISNULL(win_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(win_expr));
  } else if (OB_FAIL(part_exprs.assign(win_expr->get_partition_exprs()))) {
    LOG_WARN("failed to assign partition exprs", K(ret));
  } else if (OB_FAIL(set_default_sort_directions(remaining_exprs,
                                                 part_exprs,
                                                 equal_sets,
                                                 part_directions))) {
    LOG_WARN("failed to get default sort directions", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::adjust_exprs_by_ordering(part_exprs,
                                                               ordering,
                                                               equal_sets,
                                                               const_exprs,
                                                               get_onetime_query_refs(),
                                                               prefix_count,
                                                               input_ordering_all_used,
                                                               part_directions))) {
    LOG_WARN("failed to adjust exprs by ordering", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::make_sort_keys(part_exprs,
                                                     part_directions,
                                                     output_sort_keys))) {
    LOG_WARN("failed to make sort keys", K(ret));
  } else {
    if (NULL != pby_prefix) {
      if (OB_FAIL(ObOptimizerUtil::simplify_ordered_exprs(fd_item_set,
                                                          equal_sets,
                                                          const_exprs,
                                                          get_onetime_query_refs(),
                                                          output_sort_keys,
                                                          sort_keys))) {
        LOG_WARN("failed to simplify ordered exprs", K(ret));
      } else {
        *pby_prefix = sort_keys.count();
        sort_keys.reuse();
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(append(output_sort_keys, win_expr->get_order_items()))) {
    LOG_WARN("failed to append order items", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::simplify_ordered_exprs(fd_item_set,
                                                             equal_sets,
                                                             const_exprs,
                                                             get_onetime_query_refs(),
                                                             output_sort_keys,
                                                             sort_keys))) {
    LOG_WARN("failed to simplify ordered exprs", K(ret));
  } else { /*do nothing*/ }

  return ret;
}

int ObSelectLogPlan::get_win_func_pby_oby_sort_prefix(const ObFdItemSet &fd_item_set,
                                                      const EqualSets &equal_sets,
                                                      const ObIArray<ObRawExpr*> &const_exprs,
                                                      const ObWinFunRawExpr *win_expr,
                                                      const ObIArray<OrderItem> &ordering,
                                                      int64_t &pby_prefix,
                                                      int64_t &pby_oby_prefix)
{
  int ret = OB_SUCCESS;
  ObSEArray<OrderItem, 8> sort_keys;
  ObArray<ObWinFunRawExpr *> remaining;
  if (OB_FAIL(get_sort_keys_for_window_function(fd_item_set,
                                                equal_sets,
                                                const_exprs,
                                                win_expr,
                                                ordering,
                                                remaining,
                                                sort_keys,
                                                &pby_prefix))) {
    LOG_WARN("get sort keys for window function failed", K(ret));
  } else {
    pby_oby_prefix = sort_keys.count();
    if (pby_prefix > pby_oby_prefix
        || pby_oby_prefix > sort_keys.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected prefix count",
               K(ret), K(pby_prefix), K(pby_oby_prefix), K(sort_keys), KPC(win_expr));
    }
  }
  return ret;
}

int ObSelectLogPlan::set_default_sort_directions(const ObIArray<ObWinFunRawExpr*> &win_exprs,
                                                 const ObIArray<ObRawExpr *> &part_exprs,
                                                 const EqualSets &equal_sets,
                                                 ObIArray<ObOrderDirection> &directions)
{
  int ret = OB_SUCCESS;
  directions.reset();
  if (OB_ISNULL(get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < part_exprs.count(); ++i) {
    const ObRawExpr *expr = part_exprs.at(i);
    ObOrderDirection direction = default_asc_direction();
    bool found_dir = false;
    for (int64_t j = 0; OB_SUCC(ret) && !found_dir && j < win_exprs.count(); ++j) {
      if (OB_ISNULL(win_exprs.at(j))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("win func is null", K(ret));
      } else {
        found_dir = ObOptimizerUtil::find_expr_direction(win_exprs.at(j)->get_order_items(),
                                                         expr,
                                                         equal_sets,
                                                         direction);
      }
    }
    if (OB_SUCC(ret)) {
      if (!found_dir) {
        found_dir = ObOptimizerUtil::find_expr_direction(get_stmt()->get_order_items(),
                                                         expr,
                                                         equal_sets,
                                                         direction);
      }
      if (OB_FAIL(directions.push_back(direction))) {
        LOG_WARN("failed to push back directions", K(ret));
      }
    }
  }
  return ret;
}

int ObSelectLogPlan::check_win_func_pushdown(const int64_t dop,
                                             const WinFuncOpHelper &win_func_helper,
                                             ObIArray<bool> &pushdown_info)
{
  int ret = OB_SUCCESS;
  pushdown_info.reuse();
  const ObIArray<double> &sort_key_ndvs = win_func_helper.sort_key_ndvs_;
  const ObIArray<std::pair<int64_t,int64_t>> &pby_oby_prefixes = win_func_helper.pby_oby_prefixes_;
  ObWinfuncOptimizationOpt win_opt;
  const int64_t WF_CARD_DOP_RADIO = 256;
  const int64_t WF_PBY_DOP_RADIO = 16;
  if (WinDistAlgo::WIN_DIST_HASH != win_func_helper.win_dist_method_
      || win_func_helper.force_no_pushdown_) {
    /* do nothing */
  } else if (OB_ISNULL(get_optimizer_context().get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(get_optimizer_context().get_session_info()));
  } else if (OB_FAIL(get_optimizer_context().get_session_info()->get_sys_variable(
                              share::SYS_VAR__WINDOWFUNC_OPTIMIZATION_SETTINGS, win_opt.v_))) {
    LOG_WARN("get sys variable failed", K(ret));
  } else if (win_opt.disable_reporting_wf_pushdown_ && !win_func_helper.force_pushdown_) {
    /* do nothing */
  } else {
    bool has_pushdown_wf_exprs = false;
    int64_t min_pby_ndv_idx = OB_INVALID_INDEX;
    double min_pby_ndv = 0.0;
    const ObIArray<ObWinFunRawExpr*> &win_func_exprs = win_func_helper.ordered_win_func_exprs_;
    // use trace point to enforce pushdown window function regardless of ndv and dop
    // use trace point : alter system set_tp tp_no = 247, error_code = 4000, frequency = 1;
    const bool tract_point_pushdown = (OB_SUCCESS != (OB_E(EventTable::EN_ENFORCE_PUSH_DOWN_WF) OB_SUCCESS));
    bool can_pushdown = win_func_helper.card_ >= WF_CARD_DOP_RADIO * dop
                        || win_func_helper.force_pushdown_
                        || tract_point_pushdown;
    for (int64_t idx = 0; OB_SUCC(ret) && can_pushdown && idx < win_func_exprs.count(); ++idx) {
      const int64_t pby_cnt = pby_oby_prefixes.at(idx).first;
      const int64_t pby_oby_cnt = pby_oby_prefixes.at(idx).second;
      if (OB_FAIL(check_wf_pushdown_supported(win_func_exprs.at(idx), can_pushdown))) {
        LOG_WARN("check window function range distribute parallel supported failed", K(ret));
      } else if (!can_pushdown || pby_cnt <= 0 || pby_cnt != pby_oby_cnt) {
        can_pushdown = false;
      } else if (OB_FAIL(pushdown_info.push_back(true))) {
        LOG_WARN("push_back to push_down_array failed", K(ret));
      } else if (win_func_helper.force_pushdown_) {
        // hint force pushdown, at least pushdown one window function
        has_pushdown_wf_exprs = true;
        const double pby_ndv = sort_key_ndvs.at(pby_cnt - 1);
        const bool pushdwon_cur_wf = pby_ndv < WF_PBY_DOP_RADIO * dop;
        if (OB_INVALID_INDEX == min_pby_ndv_idx || pby_ndv < min_pby_ndv) {
          min_pby_ndv_idx = idx;
          min_pby_ndv = pby_ndv;
        }
        pushdown_info.at(pushdown_info.count() - 1) = pushdwon_cur_wf;
      } else {
        // 1. caculate whether need to pushdown by NDV and CARD
        // 2. trace point force pushdown, pushdown all window function
        const bool pushdwon_cur_wf = sort_key_ndvs.at(pby_cnt - 1) < WF_PBY_DOP_RADIO * dop
                                     || tract_point_pushdown;
        pushdown_info.at(pushdown_info.count() - 1) = pushdwon_cur_wf;
        has_pushdown_wf_exprs |= pushdwon_cur_wf;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (!can_pushdown || !has_pushdown_wf_exprs) {
      pushdown_info.reuse();
    } else if (OB_INVALID_INDEX == min_pby_ndv_idx) {
      /* do nothing */
    } else if (OB_UNLIKELY(pushdown_info.count() <= min_pby_ndv_idx || 0 > min_pby_ndv_idx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected idx", K(ret), K(pushdown_info.count()), K(min_pby_ndv_idx));
    } else {
      pushdown_info.at(min_pby_ndv_idx) = true;
    }
  }

  return ret;
}

int ObSelectLogPlan::calc_ndvs_and_pby_oby_prefix(const ObIArray<ObWinFunRawExpr*> &win_func_exprs,
                                                  WinFuncOpHelper &win_func_helper,
                                                  ObIArray<std::pair<int64_t, int64_t>> &pby_oby_prefixes)
{
  int ret = OB_SUCCESS;
  const ObIArray<OrderItem> &sort_keys = win_func_helper.sort_keys_;
  const double card = win_func_helper.card_;
  ObIArray<double> &sort_key_ndvs = win_func_helper.sort_key_ndvs_;
  ObSEArray<ObRawExpr *, 8> sort_key_exprs;
  sort_key_ndvs.reuse();
  pby_oby_prefixes.reuse();

  // Get NDV for each sort_keys prefix first.
  for (int64_t i = 0; i < sort_keys.count() && OB_SUCC(ret); i++) {
    double sort_key_ndv = 0.0;
    if (OB_FAIL(sort_key_exprs.push_back(sort_keys.at(i).expr_))) {
      LOG_WARN("array push back failed", K(ret));
    } else if (OB_FAIL(ObOptSelectivity::calculate_distinct(get_update_table_metas(),
                                                            get_selectivity_ctx(),
                                                            sort_key_exprs,
                                                            card,
                                                            sort_key_ndv))) {
      LOG_WARN("calculate NDV failed", K(ret));
    } else if (OB_FAIL(sort_key_ndvs.push_back(sort_key_ndv))) {
      LOG_WARN("array push back failed", K(ret));
    }
  }

  // Get each window function's partition by (PBY) and order by (OBY)'s prefix of %sort_keys.
  for (int64_t i = 0; OB_SUCC(ret) && i < win_func_exprs.count(); i++) {
    int64_t pby_prefix = 0;
    int64_t pby_oby_prefix = 0;
    if (OB_FAIL(get_win_func_pby_oby_sort_prefix(win_func_helper.fd_item_set_,
                                                win_func_helper.equal_sets_,
                                                win_func_helper.const_exprs_,
                                                win_func_exprs.at(i),
                                                sort_keys,
                                                pby_prefix,
                                                pby_oby_prefix))) {
      LOG_WARN("get sort keys for window function failed", K(ret));
    } else if (OB_FAIL(pby_oby_prefixes.push_back(
                std::make_pair(pby_prefix, pby_oby_prefix)))) {
      LOG_WARN("array push back failed", K(ret));
    }
  }
  return ret;
}

// split window functions by distribute method
// split the window function expressions of one group (group is detected by get_next_group_window_exprs()) into multi window function operator by distribute method (HASH or RANGE)
int ObSelectLogPlan::split_win_funcs_by_dist_method(const bool distributed,
    const common::ObIArray<ObWinFunRawExpr *> &win_func_exprs,
    const ObIArray<double> &sort_key_ndvs,
    const ObIArray<std::pair<int64_t, int64_t>> &pby_oby_prefixes,
    const int64_t dop,
    const double card,
    const ObWindowDistHint *hint,
    const int64_t win_op_idx,
    common::ObIArray<int64_t> &split,
    common::ObIArray<WinDistAlgo> &methods)
{
  int ret = OB_SUCCESS;
  split.reuse();
  methods.reuse();
  ObWinfuncOptimizationOpt win_opt;
  if (OB_ISNULL(get_optimizer_context().get_session_info()) || OB_UNLIKELY(dop < 1 || card < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(get_optimizer_context().get_session_info()), K(dop), K(card));
  } else if (OB_UNLIKELY(win_func_exprs.count() != pby_oby_prefixes.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected params", K(ret), K(pby_oby_prefixes), K(win_func_exprs));
  } else if (OB_FAIL(get_optimizer_context().get_session_info()->get_sys_variable(
                              share::SYS_VAR__WINDOWFUNC_OPTIMIZATION_SETTINGS, win_opt.v_))) {
    LOG_WARN("get sys variable failed", K(ret));
  }

  // Main split logic, get current window function's distribute method (%cur_method) by
  // auto detected method and hint method. Then compare it with the previous window function's
  // distribute method to check need split or not.
  const int64_t WF_PBY_DOP_RADIO = 16;
  const int64_t WF_CARD_DOP_RADIO = 256;
  bool force_one_group = !distributed;
  uint64_t prev_method = force_one_group ? WinDistAlgo::WIN_DIST_NONE : WinDistAlgo::WIN_DIST_INVALID;
  for (int64_t idx = 0; OB_SUCC(ret) && !force_one_group && idx < win_func_exprs.count(); ++idx) {
    const int64_t pby_cnt = pby_oby_prefixes.at(idx).first;
    const int64_t pby_oby_cnt = pby_oby_prefixes.at(idx).second;
    bool range_dist_allowed = false;
    if (OB_UNLIKELY(pby_cnt > sort_key_ndvs.count() || pby_oby_cnt > sort_key_ndvs.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected params", K(ret), K(pby_cnt), K(pby_oby_cnt), K(sort_key_ndvs));
    } else if (OB_FAIL(check_wf_range_dist_supported(win_func_exprs.at(idx), range_dist_allowed))) {
      LOG_WARN("check window function range distribute parallel supported failed", K(ret));
    } else if (pby_oby_cnt <= 0 || (pby_cnt <= 0 && !range_dist_allowed)) {
      // no partition by && can not do range distribution
      force_one_group = true;
      split.reuse();
      methods.reuse();
      prev_method = WinDistAlgo::WIN_DIST_NONE;
    } else {
      // <1> detect all valid distribute method
      uint64_t cur_method = WinDistAlgo::WIN_DIST_INVALID;
      uint64_t valid_method = WinDistAlgo::WIN_DIST_NONE;
      if (pby_cnt > 0) {
        valid_method |= WinDistAlgo::WIN_DIST_HASH;
      }
      if (range_dist_allowed) {
        valid_method |= WinDistAlgo::WIN_DIST_RANGE;
        valid_method |= WinDistAlgo::WIN_DIST_LIST;
      }

      // <2> update method by hint
      if (NULL != hint && win_op_idx + split.count() < hint->get_win_dist_options().count()) {
        cur_method = valid_method & hint->get_win_dist_options().at(win_op_idx + split.count()).algo_;
      }

      // <3> update distribute method by NDV and CARD
      if (WinDistAlgo::WIN_DIST_INVALID == cur_method) {
        LOG_DEBUG("update distribute method by NDV and CARD", K(pby_cnt), K(pby_oby_cnt),
                                                    K(sort_key_ndvs), K(valid_method), K(card));
        const double pby_ndv = pby_cnt == 0 ? 0 : sort_key_ndvs.at(pby_cnt - 1);
        const double pby_oby_ndv = pby_oby_cnt == 0 ? 0 : sort_key_ndvs.at(pby_oby_cnt - 1);
        cur_method = WinDistAlgo::WIN_DIST_NONE | (valid_method & WinDistAlgo::WIN_DIST_HASH);
        if (!win_opt.disable_range_distribution_ && card > WF_CARD_DOP_RADIO * dop
            && pby_ndv < WF_PBY_DOP_RADIO * dop) {
          cur_method |= valid_method & WinDistAlgo::WIN_DIST_RANGE;
          if (pby_oby_ndv < WF_PBY_DOP_RADIO * dop) {
            cur_method |= valid_method & WinDistAlgo::WIN_DIST_LIST;
          }
        }
      }

      // <4> get current distribute method and compare previous method to split win_func_exprs
      if (WinDistAlgo::WIN_DIST_INVALID == prev_method) {
        prev_method = cur_method;
      } else {
        uint64_t tmp_method = prev_method & cur_method;
        if (tmp_method >= WinDistAlgo::WIN_DIST_RANGE && pby_oby_prefixes.at(idx - 1) != pby_oby_prefixes.at(idx)) {
          tmp_method &= ~WinDistAlgo::WIN_DIST_RANGE;
          tmp_method &= ~WinDistAlgo::WIN_DIST_LIST;
        }
        const WinDistAlgo pre_algo = get_win_dist_algo(prev_method);
        const WinDistAlgo tmp_algo = get_win_dist_algo(tmp_method);
        if (tmp_algo == pre_algo || WinDistAlgo::WIN_DIST_HASH <= tmp_algo ) { // need check next win func
          prev_method = tmp_method;
        } else if (OB_FAIL(split.push_back(idx)) || OB_FAIL(methods.push_back(pre_algo))) {
          LOG_WARN("array push back failed", K(ret));
        } else {
          prev_method = cur_method;
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(split.push_back(win_func_exprs.count()))
             || OB_FAIL(methods.push_back(get_win_dist_algo(prev_method)))) {
    LOG_WARN("array push back failed", K(ret));
  }
  return ret;
}

// partition wise or single partition parallel or serialize
int ObSelectLogPlan::create_none_dist_win_func(ObLogicalOperator *top,
                                               const WinFuncOpHelper &win_func_helper,
                                               const int64_t need_sort,
                                               const int64_t prefix_pos,
                                               const int64_t part_cnt,
                                               ObIArray<CandidatePlan> &all_plans)
{
  int ret = OB_SUCCESS;
  ObExchangeInfo exch_info;
  bool need_hash_sort = false;
  bool need_normal_sort = false;
  bool is_partition_wise = false;
  bool single_part_parallel = false;
  const ObIArray<ObWinFunRawExpr*> &win_func_exprs = win_func_helper.ordered_win_func_exprs_;
  const ObIArray<OrderItem> &sort_keys = win_func_helper.sort_keys_;
  bool is_local_order = false;
  if (OB_ISNULL(top)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (WinDistAlgo::WIN_DIST_NONE == win_func_helper.win_dist_method_
             || (!win_func_helper.explicit_hint_ && WinDistAlgo::WIN_DIST_HASH == win_func_helper.win_dist_method_)) {
    if (top->is_distributed() &&
        OB_FAIL(top->check_sharding_compatible_with_reduce_expr(win_func_helper.partition_exprs_,
                                                                is_partition_wise))) {
      LOG_WARN("failed to check if sharding compatible", K(ret));
    } else if (top->is_distributed() &&
               OB_FAIL(match_window_function_parallel(win_func_exprs, single_part_parallel))) {
      LOG_WARN("failed to check match window function parallel", K(ret));
    } else if (WinDistAlgo::WIN_DIST_NONE == win_func_helper.win_dist_method_
               || !top->is_distributed()
               || is_partition_wise) {
      LOG_TRACE("begin to create none dist window function", K(top->is_distributed()),
                          K(single_part_parallel), K(is_partition_wise), K(need_sort), K(part_cnt),
                          K(win_func_helper.force_hash_sort_), K(win_func_helper.force_normal_sort_));
      need_normal_sort = !win_func_helper.force_hash_sort_;
      need_hash_sort = need_sort && part_cnt > 0 && !win_func_helper.force_normal_sort_;
      is_local_order = top->get_is_local_order();
      if (!top->is_distributed() || single_part_parallel || is_partition_wise) {
        exch_info.dist_method_ = ObPQDistributeMethod::NONE;
        is_local_order &= top->is_single() || (top->is_distributed() && top->is_exchange_allocated());
      }
    }
  }

  if (OB_SUCC(ret) && need_normal_sort) {
    ObLogicalOperator *normal_sort_top= top;
    if (OB_FAIL(allocate_sort_and_exchange_as_top(normal_sort_top, exch_info, sort_keys, need_sort,
                                                  prefix_pos, is_local_order))) {
      LOG_WARN("failed to allocate sort and exchange as top", K(ret));
    } else if (OB_FAIL(allocate_window_function_as_top(WinDistAlgo::WIN_DIST_NONE,
                                                       win_func_exprs,
                                                       single_part_parallel,
                                                       is_partition_wise,
                                                       false, /* use hash sort */
                                                       sort_keys,
                                                       normal_sort_top))) {
      LOG_WARN("failed to allocate window function as top", K(ret));
    } else if (OB_FAIL(all_plans.push_back(CandidatePlan(normal_sort_top)))) {
      LOG_WARN("failed to push back", K(ret));
    }
  }

  if (OB_SUCC(ret) && need_hash_sort) {
    ObLogicalOperator *hash_sort_top= top;
    OrderItem hash_sortkey;
    if (OB_FAIL(create_hash_sortkey(part_cnt, sort_keys, hash_sortkey))) {
      LOG_WARN("failed to create hash sort key", K(ret), K(part_cnt), K(sort_keys));
    } else if (OB_FAIL(allocate_sort_and_exchange_as_top(hash_sort_top, exch_info, sort_keys, need_sort,
                                                         prefix_pos, is_local_order,
                                                         NULL, /* topn_expr */
                                                         false, /* is_fetch_with_ties */
                                                         &hash_sortkey))) {
      LOG_WARN("failed to allocate sort and exchange as top", K(ret));
    } else if (OB_FAIL(allocate_window_function_as_top(WinDistAlgo::WIN_DIST_NONE,
                                                       win_func_exprs,
                                                       single_part_parallel,
                                                       is_partition_wise,
                                                       true, /* use hash sort */
                                                       sort_keys,
                                                       hash_sort_top))) {
      LOG_WARN("failed to allocate window function as top", K(ret));
    } else if (OB_FAIL(all_plans.push_back(CandidatePlan(hash_sort_top)))) {
      LOG_WARN("failed to push back", K(ret));
    }
  }
  return ret;
}

int ObSelectLogPlan::create_range_list_dist_win_func(ObLogicalOperator *top,
                                                     const WinFuncOpHelper &win_func_helper,
                                                     const int64_t part_cnt,
                                                     ObIArray<CandidatePlan> &all_plans)
{
  int ret = OB_SUCCESS;
  bool need_sort = false;
  int64_t prefix_pos = 0;
  ObSEArray<OrderItem, 8> range_dist_keys;
  int64_t pby_prefix = 0;
  ObRawExpr *random_expr = NULL;
  ObLogicalOperator *receive = NULL;
  ObExchangeInfo exch_info;
  const ObIArray<ObWinFunRawExpr*> &win_func_exprs = win_func_helper.ordered_win_func_exprs_;
  const ObIArray<OrderItem> &sort_keys = win_func_helper.sort_keys_;
  bool single_part_parallel = false;
  bool is_partition_wise = false;
  if (WinDistAlgo::WIN_DIST_RANGE != win_func_helper.win_dist_method_
      && WinDistAlgo::WIN_DIST_LIST != win_func_helper.win_dist_method_) {
    /* do nothing */
  } else if (OB_ISNULL(top)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected params", K(ret), K(top));
  } else if (OB_FAIL(get_range_dist_keys(win_func_helper, win_func_exprs.at(0),
                                         range_dist_keys, pby_prefix))) {
    LOG_WARN("failed to get range list keys", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::check_need_sort(range_dist_keys,
                                                      top->get_op_ordering(),
                                                      top->get_fd_item_set(),
                                                      top->get_output_equal_sets(),
                                                      top->get_output_const_exprs(),
                                                      get_onetime_query_refs(),
                                                      top->get_is_at_most_one_row(),
                                                      need_sort,
                                                      prefix_pos,
                                                      part_cnt))) {
    LOG_WARN("failed to check if need sort", K(ret));
  } else if (OB_FAIL(get_range_list_win_func_exchange_info(win_func_helper.win_dist_method_,
                                                           range_dist_keys,
                                                           exch_info,
                                                           random_expr))) {
    LOG_WARN("failed to get range list win func exchange info", K(ret));
  } else if (OB_FAIL(allocate_sort_and_exchange_as_top(top,
                                                      exch_info,
                                                      range_dist_keys,
                                                      need_sort,
                                                      prefix_pos,
                                                      top->get_is_local_order()))) {
    LOG_WARN("failed to allocate sort and exchange as top", K(ret));
  } else if (OB_FAIL(set_exchange_random_expr(top, random_expr))) {
    LOG_WARN("failed to set exchange random expr", K(ret));
  } else if (OB_FAIL(allocate_window_function_as_top(win_func_helper.win_dist_method_,
                                                    win_func_exprs,
                                                    single_part_parallel,
                                                    is_partition_wise,
                                                    false, /* use hash sort */
                                                    ObLogWindowFunction::WindowFunctionRoleType::NORMAL,
                                                    sort_keys,
                                                    range_dist_keys.count(),
                                                    pby_prefix,
                                                    top))) {
    LOG_WARN("failed to allocate window function as top", K(ret));
  } else if (OB_FAIL(all_plans.push_back(CandidatePlan(top)))) {
    LOG_WARN("failed to push back", K(ret));
  }
  return ret;
}

int ObSelectLogPlan::get_range_dist_keys(const WinFuncOpHelper &win_func_helper,
                                         const ObWinFunRawExpr *win_func,
                                         ObIArray<OrderItem> &range_dist_keys,
                                         int64_t &pby_prefix)
{
  int ret = OB_SUCCESS;
  range_dist_keys.reuse();
  pby_prefix = 0;
  const ObIArray<std::pair<int64_t,int64_t>> &pby_oby_prefixes = win_func_helper.pby_oby_prefixes_;
  const ObIArray<OrderItem> &sort_keys = win_func_helper.sort_keys_;
  int64_t idx = OB_INVALID_INDEX;
  if (OB_UNLIKELY(!ObOptimizerUtil::find_item(win_func_helper.ordered_win_func_exprs_,
                                              win_func, &idx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(win_func_helper.ordered_win_func_exprs_), KPC(win_func));
  } else if (OB_UNLIKELY(idx < 0 || idx >= pby_oby_prefixes.count() ||
                         sort_keys.count() < pby_oby_prefixes.at(idx).second)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected params", K(ret), K(idx), K(pby_oby_prefixes), K(sort_keys.count()));
  } else {
    pby_prefix = pby_oby_prefixes.at(idx).first;
    const int64_t pby_oby_prefix = pby_oby_prefixes.at(idx).second;
    for (int64_t i = 0; OB_SUCC(ret) && i < pby_oby_prefix; i++) {
      OZ(range_dist_keys.push_back(sort_keys.at(i)));
    }
  }
  return ret;
}

int ObSelectLogPlan::get_range_list_win_func_exchange_info(const WinDistAlgo dist_method,
                                                           const ObIArray<OrderItem> &range_dist_keys,
                                                           ObExchangeInfo &exch_info,
                                                           ObRawExpr *&random_expr)
{
  int ret = OB_SUCCESS;
  exch_info.dist_method_ = ObPQDistributeMethod::RANGE;
  exch_info.sample_type_ = FULL_INPUT_SAMPLE;
  if (OB_FAIL(exch_info.sort_keys_.assign(range_dist_keys))) {
    LOG_WARN("failed to assign", K(ret));
  } else if (WinDistAlgo::WIN_DIST_RANGE == dist_method) {
    /* do nothing */
  } else if (OB_ISNULL(get_optimizer_context().get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(get_optimizer_context().get_session_info()));
  } else if (OB_FAIL(ObRawExprUtils::build_pseudo_random(get_optimizer_context().get_expr_factory(),
                                                         *get_optimizer_context().get_session_info(),
                                                         random_expr))) {
    LOG_WARN("failed to build pseudo random", K(ret));
  } else if (OB_FAIL(exch_info.sort_keys_.push_back(OrderItem(random_expr)))) {
    LOG_WARN("failed to push back", K(ret));
  }
  LOG_TRACE("get range list win func exchange info", K(dist_method), KPC(random_expr), K(range_dist_keys));
  return ret;
}

int ObSelectLogPlan::set_exchange_random_expr(ObLogicalOperator *top,
                                              ObRawExpr *random_expr)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *receive = NULL;
  ObLogicalOperator *transmit = NULL;
  if (NULL == random_expr) {
    /* do nothing */
  } else if (OB_ISNULL(top)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(top));
  } else if (OB_FAIL(top->find_first_recursive(LOG_EXCHANGE, receive))) {
    LOG_WARN("failed to find exchange", K(ret));
  } else if (OB_ISNULL(receive) || OB_ISNULL(transmit = receive->get_child(0))
             || OB_UNLIKELY(LOG_EXCHANGE != transmit->get_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected params", K(ret), K(receive), K(transmit));
  } else {
    static_cast<ObLogExchange *>(transmit)->set_random_expr(random_expr);
  }
  return ret;
}

int ObSelectLogPlan::create_hash_dist_win_func(ObLogicalOperator *top,
                                               const WinFuncOpHelper &win_func_helper,
                                               const int64_t need_sort,
                                               const int64_t prefix_pos,
                                               const int64_t part_cnt,
                                               ObIArray<CandidatePlan> &all_plans)
{
  int ret = OB_SUCCESS;
  ObSEArray<bool, 8> pushdown_info;
  bool need_pushdown = false;
  bool need_hash_sort = false;
  bool need_normal_sort = false;
  bool is_partition_wise = false;
  const ObIArray<ObWinFunRawExpr*> &win_func_exprs = win_func_helper.ordered_win_func_exprs_;
  const ObIArray<OrderItem> &sort_keys = win_func_helper.sort_keys_;
  if (OB_ISNULL(top)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (WinDistAlgo::WIN_DIST_HASH != win_func_helper.win_dist_method_) {
    /* do nothing */
  } else if (OB_FAIL(check_win_func_pushdown(top->get_parallel(), win_func_helper, pushdown_info))) {
    LOG_WARN("failed to check window function pushdown", K(ret));
  } else if (pushdown_info.empty() ? win_func_helper.force_pushdown_
                                   : win_func_helper.force_no_pushdown_) {
    LOG_TRACE("ignore allocate window function with pushdown info due to hint", K(win_func_helper));
  } else if (top->is_distributed() &&
             OB_FAIL(top->check_sharding_compatible_with_reduce_expr(win_func_helper.partition_exprs_,
                                                                     is_partition_wise))) {
    LOG_WARN("failed to check if sharding compatible", K(ret));
  } else if (!top->is_distributed() || is_partition_wise) {
    LOG_TRACE("ignore allocate hash window function for local or partition wise",
                                            K(top->is_distributed()), K(is_partition_wise));
  } else {
    need_pushdown = !pushdown_info.empty();
    need_normal_sort = !win_func_helper.force_hash_sort_;
    need_hash_sort = need_sort && part_cnt > 0 && !win_func_helper.force_normal_sort_;
    LOG_TRACE("begin to create hash dist window function", K(need_pushdown), K(need_sort), K(part_cnt),
                K(win_func_helper.force_hash_sort_), K(win_func_helper.force_normal_sort_));
  }
  if (OB_SUCC(ret) && need_normal_sort) {
    ObLogicalOperator *normal_sort_top= top;
    if (!need_pushdown &&
        OB_FAIL(create_normal_hash_dist_win_func(normal_sort_top,
                                                win_func_exprs,
                                                win_func_helper.partition_exprs_,
                                                sort_keys,
                                                need_sort,
                                                prefix_pos,
                                                NULL))) {
      LOG_WARN("failed to create normal hash dist window function", K(ret));
    } else if (need_pushdown &&
               OB_FAIL(create_pushdown_hash_dist_win_func(normal_sort_top,
                                                          win_func_exprs,
                                                          sort_keys,
                                                          pushdown_info,
                                                          win_func_helper.wf_aggr_status_expr_,
                                                          need_sort,
                                                          prefix_pos,
                                                          NULL))) {
      LOG_WARN("failed to create push down hash dist window function", K(ret));
    } else if (OB_FAIL(all_plans.push_back(CandidatePlan(normal_sort_top)))) {
      LOG_WARN("failed to push back", K(ret));
    }
  }

  if (OB_SUCC(ret) && need_hash_sort) {
    ObLogicalOperator *hash_sort_top= top;
    OrderItem hash_sortkey;
    if (OB_FAIL(create_hash_sortkey(part_cnt, sort_keys, hash_sortkey))) {
      LOG_WARN("failed to create hash sort key", K(ret), K(part_cnt), K(sort_keys));
    } else if (!need_pushdown &&
               OB_FAIL(create_normal_hash_dist_win_func(hash_sort_top,
                                                        win_func_exprs,
                                                        win_func_helper.partition_exprs_,
                                                        sort_keys,
                                                        need_sort,
                                                        prefix_pos,
                                                        &hash_sortkey))) {
      LOG_WARN("failed to create normal hash dist window function", K(ret));
    } else if (need_pushdown &&
               OB_FAIL(create_pushdown_hash_dist_win_func(hash_sort_top,
                                                        win_func_exprs,
                                                        sort_keys,
                                                        pushdown_info,
                                                        win_func_helper.wf_aggr_status_expr_,
                                                        need_sort,
                                                        prefix_pos,
                                                        &hash_sortkey))) {
      LOG_WARN("failed to create push down hash dist window function", K(ret));
    } else if (OB_FAIL(all_plans.push_back(CandidatePlan(hash_sort_top)))) {
      LOG_WARN("failed to push back", K(ret));
    }
  }
  return ret;
}

int ObSelectLogPlan::create_normal_hash_dist_win_func(ObLogicalOperator *&top,
                                                      const ObIArray<ObWinFunRawExpr*> &win_func_exprs,
                                                      const ObIArray<ObRawExpr*> &partition_exprs,
                                                      const ObIArray<OrderItem> &sort_keys,
                                                      const int64_t need_sort,
                                                      const int64_t prefix_pos,
                                                      OrderItem *hash_sortkey)
{
  int ret = OB_SUCCESS;
  ObExchangeInfo exch_info;
  if (OB_FAIL(get_grouping_style_exchange_info(partition_exprs,
                                               top->get_output_equal_sets(),
                                               exch_info))) {
    LOG_WARN("failed to get grouping style exchange info", K(ret));
  } else if (OB_FAIL(allocate_sort_and_exchange_as_top(top,
                                                      exch_info,
                                                      sort_keys,
                                                      need_sort,
                                                      prefix_pos,
                                                      top->get_is_local_order(),
                                                      NULL, /* topn_expr */
                                                      false, /* is_fetch_with_ties */
                                                      hash_sortkey))) {
    LOG_WARN("failed to allocate sort and exchange as top", K(ret));
  } else if (OB_FAIL(allocate_window_function_as_top(WinDistAlgo::WIN_DIST_HASH,
                                                    win_func_exprs,
                                                    false, /* match_parallel */
                                                    false, /*is_partition_wise*/
                                                    NULL != hash_sortkey, /* use hash sort */
                                                    sort_keys,
                                                    top))) {
    LOG_WARN("failed to allocate window function as top", K(ret));
  }
  return ret;
}

int ObSelectLogPlan::create_pushdown_hash_dist_win_func(ObLogicalOperator *&top,
                                                        const ObIArray<ObWinFunRawExpr*> &win_func_exprs,
                                                        const ObIArray<OrderItem> &sort_keys,
                                                        const ObIArray<bool> &pushdown_info,
                                                        ObOpPseudoColumnRawExpr *wf_aggr_status_expr,
                                                        const int64_t need_sort,
                                                        const int64_t prefix_pos,
                                                        OrderItem *hash_sortkey)
{
  int ret = OB_SUCCESS;
  ObExchangeInfo exch_info;
  ObSEArray<OrderItem, 8> tmp_sort_keys; // sort key + aggr_status desc, for EXCHANGE IN MERGE SORT DISTR
  const int64_t range_dist_keys_cnt = 0;
  const int64_t range_dist_pby_prefix = 0;
  bool top_is_local_order = false;
  if (OB_ISNULL(top) || OB_UNLIKELY(pushdown_info.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected params", K(ret), K(top), K(pushdown_info));
  } else if (OB_FALSE_IT(top_is_local_order = top->get_is_local_order())) {
  } else if ((need_sort || top_is_local_order) && !sort_keys.empty()
             && OB_FAIL(allocate_sort_as_top(top,
                                             sort_keys,
                                             need_sort && !top_is_local_order ? prefix_pos : 0,
                                             need_sort ? false : top_is_local_order,
                                             NULL,
                                             false, // is_fetch_with_ties
                                             hash_sortkey))) {
    LOG_WARN("failed to allocate sort as top", K(ret));
  } else if (OB_FAIL(allocate_window_function_as_top(WinDistAlgo::WIN_DIST_HASH,
                                                     win_func_exprs,
                                                     false, /* match_parallel */
                                                     false, /* is_partition_wise */
                                                     NULL != hash_sortkey, /* use hash sort */
                                                     ObLogWindowFunction::WindowFunctionRoleType::PARTICIPATOR,
                                                     sort_keys,
                                                     range_dist_keys_cnt,
                                                     range_dist_pby_prefix,
                                                     top,
                                                     wf_aggr_status_expr,
                                                     &pushdown_info))) {
    LOG_WARN("failed to allocate window function as top", K(ret));
  } else if (OB_FAIL(get_pushdown_window_function_exchange_info(win_func_exprs,
                                                                top, exch_info))) {
    LOG_WARN("failed to get pushdown window function exchange info", K(ret));
  } else if (NULL != hash_sortkey && OB_FAIL(tmp_sort_keys.push_back(*hash_sortkey))) {
    LOG_WARN("failed to push back hash sort key", K(ret));
  } else if (OB_FAIL(append(tmp_sort_keys, sort_keys))) {
    LOG_WARN("failed to append tmp_sort_keys", K(ret));
  } else if (OB_FAIL(tmp_sort_keys.push_back(OrderItem(wf_aggr_status_expr, default_desc_direction())))) {
    LOG_WARN("failed to push_back extra sort key of aggr status to tmp_sort_keys", K(ret));
  } else if (OB_FAIL(allocate_sort_and_exchange_as_top(top,
                                                       exch_info,
                                                       tmp_sort_keys,
                                                       false,
                                                       0,
                                                       top->get_is_local_order()))) {
    LOG_WARN("failed to allocate sort and exchange as top", K(ret));
  } else if (OB_FAIL(allocate_window_function_as_top(WinDistAlgo::WIN_DIST_HASH,
                                                     win_func_exprs,
                                                     false, /* match_parallel */
                                                     false, /* is_partition_wise */
                                                     NULL != hash_sortkey, /* use hash sort */
                                                     ObLogWindowFunction::WindowFunctionRoleType::CONSOLIDATOR,
                                                     sort_keys,
                                                     range_dist_keys_cnt,
                                                     range_dist_pby_prefix,
                                                     top,
                                                     wf_aggr_status_expr,
                                                     &pushdown_info))) {
    LOG_WARN("failed to allocate window function as top", K(ret));
  }
  return ret;
}

/**
 * @brief 假设 win_func_exprs 都采用 sort_keys 进行排序
 * 同一个分组中的 win_expr 应该按照一定的顺序排序。
 * 同一个ObWindowFunction中的多个 win_expr 要按照一定的顺序组织，譬如：
 * w1(c) over (partition by e1, e2, e3)
 * w2(c) over (partition by e1)
 * w3(c) over (partition by e1 e2)
 * 一定要按照 w1, w3, w2 的顺序来组织。即排在后面的win_expr的partition表达式是前面对象的子集。
 * 这里窗口函数会按照分组表达式数量进行排序（只统计非常量的分组表达式数量，数量多的排在前）。
 */
int ObSelectLogPlan::sort_window_functions(const ObFdItemSet &fd_item_set,
                                          const EqualSets &equal_sets,
                                          const ObIArray<ObRawExpr *> &const_exprs,
                                          const ObIArray<ObWinFunRawExpr *> &win_func_exprs,
                                          ObIArray<ObWinFunRawExpr *> &ordered_win_func_exprs,
                                          bool &ordering_changed)
{
  int ret = OB_SUCCESS;
  ordering_changed = false;
  ordered_win_func_exprs.reuse();
  ObSEArray<std::pair<int64_t, int64_t>, 8> expr_entries;
  bool is_const = false;
  ObSEArray<ObRawExpr*, 4> simplified_exprs;
  if (OB_UNLIKELY(win_func_exprs.empty())) {
     ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected params", K(ret), K(win_func_exprs.count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < win_func_exprs.count(); ++i) {
    int64_t non_const_exprs = 0;
    simplified_exprs.reuse();
    if (OB_ISNULL(win_func_exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::simplify_exprs(fd_item_set,
                                                        equal_sets,
                                                        const_exprs,
                                                        win_func_exprs.at(i)->get_partition_exprs(),
                                                        simplified_exprs))) {
      LOG_WARN("failed to simplify exprs", K(ret));
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < simplified_exprs.count(); ++j) {
      if (OB_FAIL(ObOptimizerUtil::is_const_expr(simplified_exprs.at(j),
                                                  equal_sets,
                                                  const_exprs,
                                                  get_onetime_query_refs(),
                                                  is_const))) {
        LOG_WARN("failed to check is const expr", K(ret));
      } else if (!is_const) {
        ++non_const_exprs;
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(expr_entries.push_back(std::pair<int64_t, int64_t>(-non_const_exprs, i)))) {
      LOG_WARN("failed to push back expr entry", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    std::pair<int64_t, int64_t> *first = &expr_entries.at(0);
    std::sort(first, first + expr_entries.count());
    for (int64_t i = 0; OB_SUCC(ret) && i < expr_entries.count(); ++i) {
      ordering_changed |= i != expr_entries.at(i).second;
      if (OB_FAIL(ordered_win_func_exprs.push_back(win_func_exprs.at(expr_entries.at(i).second)))) {
        LOG_WARN("failed to push back window function expr", K(ret));
      }
    }
  }
  return ret;
}

int ObSelectLogPlan::match_window_function_parallel(const ObIArray<ObWinFunRawExpr *> &win_exprs,
                                                    bool &can_parallel)
{
  int ret = OB_SUCCESS;
  can_parallel = true;
  for (int64_t i = 0; OB_SUCC(ret) && can_parallel && i < win_exprs.count(); ++i) {
    ObWinFunRawExpr *win_expr = win_exprs.at(i);
    switch(win_expr->get_func_type()) {
      case T_FUN_COUNT:
      case T_FUN_SUM:
      case T_FUN_MAX:
      case T_FUN_MIN: {
        if (OB_ISNULL(win_expr->get_agg_expr())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null agg expr", K(ret));
        } else if (win_expr->get_agg_expr()->is_param_distinct()) {
          can_parallel = false;
        }
        break;
      }
      default: {
        can_parallel = false;
        break;
      }
    }
    if (win_expr->get_upper().type_ != BoundType::BOUND_UNBOUNDED ||
        win_expr->get_lower().type_ != BoundType::BOUND_UNBOUNDED ||
        !win_expr->get_partition_exprs().empty()) {
      can_parallel = false;
    }
  }
  return ret;
}

int ObSelectLogPlan::check_wf_range_dist_supported(ObWinFunRawExpr *win_expr,
                                                   bool &can_rd_parallel)
{
  int ret = OB_SUCCESS;
  can_rd_parallel = false;
  if (NULL == win_expr) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("NULL expr", K(ret));
  } else {
    can_rd_parallel = win_expr->get_upper().type_ == BoundType::BOUND_UNBOUNDED
        && win_expr->get_lower().type_ == BoundType::BOUND_CURRENT_ROW;
    switch(win_expr->get_func_type()) {
      case T_WIN_FUN_RANK:
      case T_WIN_FUN_DENSE_RANK: {
        can_rd_parallel = true;
        break;
      }
      case T_FUN_COUNT:
      case T_FUN_SUM:
      case T_FUN_MAX:
      case T_FUN_MIN: {
        break;
      }
      default: {
        can_rd_parallel = false;
      }
    }
  }
  return ret;
}

int ObSelectLogPlan::check_wf_pushdown_supported(ObWinFunRawExpr *win_expr,
                                                 bool &can_wf_pushdown)
{
  int ret = OB_SUCCESS;
  can_wf_pushdown = false;
  if (OB_ISNULL(win_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    // only reporting window function can pushdown
    // If win_expr->get_partition_exprs().empty(), use window_function_parallel in single partition by yishen instead
    can_wf_pushdown = !win_expr->get_partition_exprs().empty()
                      && win_expr->get_upper().type_ == BoundType::BOUND_UNBOUNDED
                      && win_expr->get_lower().type_ == BoundType::BOUND_UNBOUNDED;
    switch(win_expr->get_func_type()) {
      case T_FUN_SYS_BIT_AND:
      case T_FUN_SYS_BIT_OR:
      case T_FUN_SYS_BIT_XOR:
      case T_FUN_MIN:
      case T_FUN_MAX: {
        break;
      }
      case T_FUN_COUNT:
      case T_FUN_SUM: {
        if (OB_ISNULL(win_expr->get_agg_expr())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret), KPC(win_expr));
        } else if (win_expr->get_agg_expr()->is_param_distinct()) {
          // If aggr expr is count distinct or sum distinct, wf can't be parallel.
          can_wf_pushdown = false;
        }
        break;
      }
      default: {
        can_wf_pushdown = false;
      }
    }
  }
  return ret;
}

int ObSelectLogPlan::get_pushdown_window_function_exchange_info(
    const ObIArray<ObWinFunRawExpr *> &win_exprs,
    ObLogicalOperator *op,
    ObExchangeInfo &exch_info)
{
  int ret = OB_SUCCESS;
  const ObWinFunRawExpr *win_func = NULL;
  ObLogWindowFunction *log_win_func = NULL;
  ObOpPseudoColumnRawExpr *wf_aggr_status_expr = NULL;
  if (OB_ISNULL(log_win_func = dynamic_cast<ObLogWindowFunction*>(op)) ||
      OB_UNLIKELY(win_exprs.empty()) || OB_ISNULL(win_func = win_exprs.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected params", K(ret));
  } else if (OB_UNLIKELY(ObLogWindowFunction::WindowFunctionRoleType::PARTICIPATOR != log_win_func->get_role_type())
             || OB_ISNULL(wf_aggr_status_expr = log_win_func->get_aggr_status_expr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected log window function", K(ret), K(log_win_func->get_role_type()), K(wf_aggr_status_expr));
  } else if (OB_FAIL(get_grouping_style_exchange_info(win_func->get_partition_exprs(),
                                                      log_win_func->get_output_equal_sets(),
                                                      exch_info))) {
    // get the pby expr of the first win_expr, the pby col count of first pby expr is the most
    LOG_WARN("failed to get grouping style exchange info", K(ret));
  } else {
    exch_info.is_wf_hybrid_ = true;
    // use the value of wf_aggr_status_expr to decide how to distribute in ex op
    exch_info.wf_hybrid_aggr_status_expr_ = wf_aggr_status_expr;
    for (int64_t i = 0; OB_SUCC(ret) && i < win_exprs.count(); ++i) {
      // wf_hybrid_pby_exprs_cnt_array_ is used for hybrid dist
      // take the value of aggr_status to decide to use which member of wf_hybrid_pby_exprs_cnt_array_ as nkey of hash dist
      if (OB_ISNULL(win_func = win_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret), K(i), K(win_exprs));
      } else if (OB_FAIL(exch_info.wf_hybrid_pby_exprs_cnt_array_.push_back(win_func->get_partition_exprs().count()))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
  }
  return ret;
}

int ObSelectLogPlan::extract_window_function_partition_exprs(WinFuncOpHelper &win_func_helper)
{
  int ret = OB_SUCCESS;
  win_func_helper.partition_exprs_.reuse();
  const ObIArray<ObWinFunRawExpr *> &win_exprs = win_func_helper.ordered_win_func_exprs_;
  ObSEArray<ObRawExpr*, 8> partition_exprs;
  for (int64_t i = 0; OB_SUCC(ret) && i < win_exprs.count(); ++i) {
    if (OB_ISNULL(win_exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (i == 0) {
      ret = partition_exprs.assign(win_exprs.at(i)->get_partition_exprs());
    } else {
      ret = ObOptimizerUtil::intersect_exprs(partition_exprs,
                                             win_exprs.at(i)->get_partition_exprs(),
                                             win_func_helper.equal_sets_,
                                             partition_exprs);
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(win_func_helper.partition_exprs_.assign(partition_exprs))) {
    LOG_WARN("failed to assign partition exprs", K(ret));
  }
  return ret;
}

int ObSelectLogPlan::candi_allocate_late_materialization()
{
  int ret = OB_SUCCESS;
  bool need_late_mat = false;
  if (OB_FAIL(if_stmt_need_late_materialization(need_late_mat))) {
    LOG_WARN("failed to check if stmt need late materialization", K(ret));
  } else if (need_late_mat) {
    OPT_TRACE_TITLE("start generate late materialization plan");
    for (int64_t i = 0; OB_SUCC(ret) && i < candidates_.candidate_plans_.count(); ++i) {
      bool need = false;
      ObLogTableScan *index_scan = NULL;
      CandidatePlan &plain_plan = candidates_.candidate_plans_.at(i);
      double cost = 0.0;
      OPT_TRACE("try to generate late materialization for plan:", plain_plan);
      if (OB_FAIL(if_plan_need_late_materialization(plain_plan.plan_tree_,
                                                    index_scan,
                                                    cost,
                                                    need))) {
        LOG_WARN("failed to check if need late materialization", K(ret));
      } else if (need) {
        plain_plan.plan_tree_->set_late_materialization(true);
        plain_plan.plan_tree_->set_cost(cost);
      }
    }
  }
  return ret;
}

int ObSelectLogPlan::get_late_materialization_operator(ObLogicalOperator *top,
                                                       ObLogSort *&sort_op,
                                                       ObLogTableScan *&table_scan)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *child_sort = NULL;
  ObLogicalOperator *child_scan = NULL;
  if (OB_ISNULL(top)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("operator is null", K(ret));
  } else if (log_op_def::LOG_LIMIT == top->get_type()) {
    child_sort = top->get_child(ObLogicalOperator::first_child);
  } else if (log_op_def::LOG_SORT == top->get_type()) {
    child_sort = top;
  } else { /*do nothing*/ }

  if (NULL == child_sort) {
    /*do nothing*/
  } else if (log_op_def::LOG_SORT != child_sort->get_type()) {
    /*do nothing*/
  } else if (OB_ISNULL(child_scan = child_sort->get_child(ObLogicalOperator::first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (log_op_def::LOG_TABLE_SCAN != child_scan->get_type()) {
    /*do nothing*/
  } else if (OB_FALSE_IT(table_scan = static_cast<ObLogTableScan *>(child_scan))) {
  } else if (NULL != table_scan->get_limit_expr() || NULL != table_scan->get_offset_expr()) {
    table_scan = NULL;
  } else {
    sort_op = static_cast<ObLogSort *>(child_sort);
  }
  return ret;
}

int ObSelectLogPlan::perform_late_materialization(ObSelectStmt *stmt,
                                                  ObLogicalOperator *&op)
{
  int ret = OB_SUCCESS;
  ObLogSort *sort = NULL;
  ObLogTableScan *index_scan = NULL;
  TableItem *nl_table_item = NULL;
  ObLogTableScan *nl_table_get = NULL;
  ObLogicalOperator *join_op = NULL;
  if (OB_ISNULL(stmt) || OB_ISNULL(op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(op), K(stmt));
  } else if (OB_FAIL(get_late_materialization_operator(op,
                                                       sort,
                                                       index_scan))) {
    LOG_WARN("failed to get late materialization operator", K(ret));
  } else if (OB_FAIL(generate_late_materialization_info(stmt,
                                                        index_scan,
                                                        nl_table_get,
                                                        nl_table_item))) {
    LOG_WARN("failed to generate later materialization table get", K(ret));
  } else if (OB_FAIL(allocate_late_materialization_join_as_top(op,
                                                               nl_table_get,
                                                               join_op))) {
    LOG_WARN("failed to generate late materialization join", K(ret));
  } else if (OB_FAIL(adjust_late_materialization_structure(stmt,
                                                           join_op,
                                                           index_scan,
                                                           nl_table_get,
                                                           nl_table_item))) {
    LOG_WARN("failed to adjust late materialization structure", K(ret));
  } else {
    op = join_op;
  }
  return ret;
}

int ObSelectLogPlan::adjust_late_materialization_structure(ObSelectStmt *stmt,
                                                           ObLogicalOperator *join,
                                                           ObLogTableScan *index_scan,
                                                           ObLogTableScan *table_scan,
                                                           TableItem *table_item)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(adjust_late_materialization_stmt_structure(stmt,
                                                         index_scan,
                                                         table_scan,
                                                         table_item))) {
    LOG_WARN("failed to adjust late materialization stmt structure", K(ret));
  } else if (OB_FAIL(adjust_late_materialization_plan_structure(join,
                                                                index_scan,
                                                                table_scan))) {
    LOG_WARN("failed to adjust latematerialization plan structure", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObSelectLogPlan::convert_project_columns(ObSelectStmt *stmt,
                                             ObIRawExprCopier &expr_copier,
                                             uint64_t table_id,
                                             TableItem *project_table_item,
                                             ObIArray<uint64_t> &index_columns)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get_stmt() returns null", K(ret));
  } else {
    ColumnItem *item = NULL;
    ObColumnRefRawExpr *expr = NULL;
    common::ObSEArray<ColumnItem, 16> new_col_items;
    common::ObSEArray<ObRawExpr *, 4> gen_exprs;
    common::ObSEArray<ObRawExpr *, 4> dependant_exprs;
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_column_size(); ++i) {
      if (OB_ISNULL(item = stmt->get_column_item(i)) || OB_ISNULL(expr = item->get_expr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected NULL pointer", K(item), K(ret));
      } else if (item->table_id_ == table_id &&
                 !ObOptimizerUtil::find_item(index_columns, item->column_id_)) {
        item->set_ref_id(project_table_item->table_id_, item->column_id_);
        expr->set_ref_id(project_table_item->table_id_, expr->get_column_id());
        expr->set_table_name(project_table_item->get_table_name());
        if (expr->is_virtual_generated_column()) {
          if (item->is_geo_ == true && expr->get_srs_id() != SPATIAL_COLUMN_SRID_MASK) {
            // spatial index generated column, cannot projet from main table
            if (OB_FAIL(new_col_items.push_back(*item))) {
              LOG_WARN("failed to push back column item", K(ret));
            }
          } else if (OB_FAIL(gen_exprs.push_back(expr))) {
            LOG_WARN("failed to push back", K(ret));
          } else if (OB_FAIL(dependant_exprs.push_back(expr->get_dependant_expr()))) {
            LOG_WARN("failed to push back", K(ret));
          }
        } else {
          if (OB_FAIL(new_col_items.push_back(*item))) {
            LOG_WARN("failed to push back column item", K(ret));
          }
        }
      } else if (OB_FAIL(new_col_items.push_back(*item))){
        LOG_WARN("failed to push back column item", K(ret));
      }
    }

    common::ObSEArray<ObRawExprPointer, 16> relation_exprs;
    if (OB_FAIL(ret) || new_col_items.count() == stmt->get_column_size()) {
    } else if (OB_FAIL(stmt->get_column_items().assign(new_col_items))) {
      LOG_WARN("failed to assign", K(ret));
    } else if (OB_FAIL(stmt->get_relation_exprs(relation_exprs))) {
      LOG_WARN("failed to get exprs", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < relation_exprs.count(); i++) {
      ObRawExpr *replace_expr = NULL;
      if (OB_FAIL(relation_exprs.at(i).get(replace_expr))) {
        LOG_WARN("failed to get expr", K(ret));
      } else if (OB_FAIL(ObTransformUtils::replace_expr(gen_exprs, dependant_exprs, replace_expr))) {
        LOG_WARN("failed to replace", K(ret));
      } else if (OB_FAIL(relation_exprs.at(i).set(replace_expr))) {
        LOG_WARN("failed to set expr", K(ret));
      }
    }
  }
  return ret;
}

int ObSelectLogPlan::adjust_late_materialization_stmt_structure(ObSelectStmt *stmt,
                                                                ObLogTableScan *index_scan,
                                                                ObLogTableScan *table_scan,
                                                                TableItem *table_item)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 4> old_column_ids;
  ObSEArray<ObRawExpr*, 4> temp_exprs;
  ObSEArray<ObRawExpr*, 8> rowkeys;
  ObRawExprCopier expr_copier(get_optimizer_context().get_expr_factory());
  if (OB_ISNULL(stmt) ||
      OB_ISNULL(index_scan) ||
      OB_ISNULL(index_scan->get_est_cost_info()) ||
      OB_ISNULL(table_scan) || OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(stmt), K(index_scan), K(table_scan), K(table_item), K(ret));
  } else if (OB_FAIL(stmt->get_table_items().push_back(table_item))) {
    LOG_WARN("failed to push back table item", K(ret));
  } else if (OB_FAIL(stmt->set_table_bit_index(table_item->table_id_))) {
    LOG_WARN("failed to set table bit index", K(ret));
  } else if (OB_FAIL(old_column_ids.assign(index_scan->get_est_cost_info()->access_columns_))) {
    LOG_WARN("failed to assign column ids", K(ret));
  } else if (OB_FAIL(get_rowkey_exprs(index_scan->get_table_id(),
                                      index_scan->get_ref_table_id(),
                                      rowkeys))) {
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
      } else if (is_shadow_column(static_cast<ObColumnRefRawExpr *>(rowkeys.at(i))->get_column_id())) {
        /*do nothing*/
      } else {
        rowkeys.at(i)->set_explicited_reference();
      }
    }
    ObIArray<ColumnItem> &range_columns = index_scan->get_est_cost_info()->range_columns_;
    for (int64_t i = 0; OB_SUCC(ret) && i < range_columns.count(); ++i) {
      ObRawExpr *new_col_expr = NULL;
      ObColumnRefRawExpr *col_expr = range_columns.at(i).expr_;
      if (OB_FAIL(expr_copier.copy(col_expr, new_col_expr))) {
        LOG_WARN("failed to copy expr", K(ret));
      } else if (OB_ISNULL(new_col_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("copy expr failed", K(ret));
      } else if (!new_col_expr->is_column_ref_expr()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expect column ref expr", K(ret));
      } else {
        range_columns.at(i).expr_ = static_cast<ObColumnRefRawExpr *>(new_col_expr);
      }
    }

    //索引列、rowkey列由原表投影，剩下需要回表的列由复制表投影
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(index_scan->set_range_columns(range_columns))) {
      LOG_WARN("failed to set range columns", K(ret));
    } else if (OB_FAIL(convert_project_columns(stmt,
                                               expr_copier,
                                               index_scan->get_table_id(),
                                               table_item,
                                               old_column_ids))) {
      LOG_WARN("failed to convert project columns", K(ret));
    } else if (OB_FAIL(get_rowkey_exprs(table_scan->get_table_id(),
                                        table_scan->get_ref_table_id(),
                                        rowkeys))) {
      LOG_WARN("failed to generate rowkeys", K(table_scan->get_table_id()), K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObSelectLogPlan::adjust_late_materialization_plan_structure(ObLogicalOperator *join,
                                                                ObLogTableScan *index_scan,
                                                                ObLogTableScan *table_scan)
{
  int ret = OB_SUCCESS;
  const ObSelectStmt *stmt = NULL;
  const ObTableSchema *table_schema = NULL;
  ObSqlSchemaGuard *schema_guard = NULL;
  ObSEArray<uint64_t, 8> rowkey_ids;
  if (OB_ISNULL(stmt = get_stmt()) || OB_ISNULL(join) ||
      OB_ISNULL(index_scan) || OB_ISNULL(table_scan) ||
      OB_ISNULL(optimizer_context_.get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(stmt), K(join), K(index_scan), K(table_scan), K(ret));
  } else if (OB_UNLIKELY(log_op_def::LOG_JOIN != join->get_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected join type", K(join->get_type()), K(ret));
  } else if (OB_ISNULL(schema_guard = get_optimizer_context().get_sql_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(
             index_scan->get_table_id(),
             index_scan->get_ref_table_id(),
             stmt,
             table_schema))) {
    LOG_WARN("failed to get table schema", K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(table_schema), K(ret));
  } else if (OB_FAIL(table_schema->get_rowkey_column_ids(rowkey_ids))) {
    LOG_WARN("failed to get rowkey ids", K(ret));
  } else {
    ObLogJoin *log_join = static_cast<ObLogJoin*>(join);
    ObSEArray<ColumnItem, 4> range_columns;
    ObSEArray<ObRawExpr*, 4> join_conditions;
    ObSEArray<ObExecParamRawExpr *, 4> scan_params;
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_ids.count(); i++) {
      const ColumnItem *col_item = NULL;
      ObRawExpr *equal_expr = NULL;
      ObRawExpr *left_expr = NULL;
      ObRawExpr *right_expr = NULL;
      if (OB_ISNULL(left_expr = get_column_expr_by_id(index_scan->get_table_id(),
                                                      rowkey_ids.at(i))) ||
          OB_ISNULL(right_expr = get_column_expr_by_id(table_scan->get_table_id(),
                                                       rowkey_ids.at(i)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(left_expr), K(right_expr), K(ret));
      } else if (OB_FAIL(ObRawExprUtils::create_new_exec_param(optimizer_context_.get_query_ctx(),
                                                               optimizer_context_.get_expr_factory(),
                                                               left_expr))) {
        LOG_WARN("create param for stmt error in extract_params_for_nl", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::create_equal_expr(optimizer_context_.get_expr_factory(),
                                                           optimizer_context_.get_session_info(),
                                                           right_expr,
                                                           left_expr,
                                                           equal_expr))) {
        LOG_WARN("failed to create equal expr", K(ret));
      } else if (OB_ISNULL(equal_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(scan_params.push_back(static_cast<ObExecParamRawExpr *>(left_expr)))) {
        LOG_WARN("failed to push back scan param", K(ret));
      } else if (OB_FAIL(join_conditions.push_back(equal_expr))) {
        LOG_WARN("failed to push back equal expr", K(ret));
      } else if (OB_ISNULL(col_item = get_column_item_by_id(table_scan->get_table_id(),
                                                            rowkey_ids.at(i)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(table_scan->get_table_id()),
            K(rowkey_ids.at(i)), K(col_item), K(ret));
      } else if (OB_FAIL(range_columns.push_back(*col_item))) {
        LOG_WARN("failed to push back column item", K(ret));
      } else { /*do nothing*/ }
    }
    //生成pre-query-range
    if (OB_SUCC(ret)) {
      const ObDataTypeCastParams dtc_params =
            ObBasicSessionInfo::create_dtc_params(optimizer_context_.get_session_info());
      ObQueryRange *query_range = static_cast<ObQueryRange*>(get_allocator().alloc(sizeof(ObQueryRange)));
      const ParamStore *params = get_optimizer_context().get_params();
      if (OB_ISNULL(query_range)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory for query range", K(ret));
      } else if (OB_ISNULL(params)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else {
        query_range = new(query_range)ObQueryRange(get_allocator());
        bool is_in_range_optimization_enabled = false;
        if (OB_FAIL(ObOptimizerUtil::is_in_range_optimization_enabled(optimizer_context_.get_global_hint(),
                                                                      optimizer_context_.get_session_info(),
                                                                      is_in_range_optimization_enabled))) {
          LOG_WARN("failed to check in range optimization enabled", K(ret));
        } else if (OB_FAIL(query_range->preliminary_extract_query_range(range_columns,
                                                                 join_conditions,
                                                                 dtc_params,
                                                                 optimizer_context_.get_exec_ctx(),
                                                                 NULL,
                                                                 params,
                                                                 false,
                                                                 true,
                                                                 is_in_range_optimization_enabled))) {
          LOG_WARN("failed to preliminary extract query range", K(ret));
        } else if (OB_FAIL(table_scan->set_range_columns(range_columns))) {
          LOG_WARN("failed to set range columns", K(ret));
        } else {
          table_scan->set_pre_query_range(query_range);
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(table_scan->set_table_scan_filters(join_conditions))) {
        LOG_WARN("failed to set filters", K(ret));
      } else if (OB_FAIL(log_join->set_nl_params(scan_params))) {
        LOG_WARN("failed to set nl params", K(ret));
      } else {
        // set index scan need late materialization, used to print outline.
        index_scan->set_late_materialization(true);
      }
    }
  }
  return ret;
}

int ObSelectLogPlan::generate_late_materialization_info(ObSelectStmt *stmt,
                                                        ObLogTableScan *index_scan,
                                                        ObLogTableScan *&table_get,
                                                        TableItem *&table_item)
{
  int ret = OB_SUCCESS;
  table_get = NULL;
  table_item = NULL;
  if (OB_ISNULL(stmt) || OB_ISNULL(optimizer_context_.get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(stmt), K(ret));
  } else {
    uint64_t new_table_id = optimizer_context_.get_query_ctx()->available_tb_id_--;
    if (OB_FAIL(generate_late_materialization_table_item(stmt,
                                                         index_scan->get_table_id(),
                                                         new_table_id,
                                                         table_item))) {
      LOG_WARN("failed to generate late materialization table item", K(ret));
    } else if (OB_FAIL(generate_late_materialization_table_get(index_scan,
                                                               table_item,
                                                               new_table_id,
                                                               table_get))) {
      LOG_WARN("failed to generate late materialization table get", K(ret));
    } else { /*do nothing*/}
  }
  return ret;
}

int ObSelectLogPlan::generate_late_materialization_table_get(ObLogTableScan *index_scan,
                                                             TableItem *table_item,
                                                             uint64_t table_id,
                                                             ObLogTableScan *&table_get)
{
  int ret = OB_SUCCESS;
  const ObDMLStmt *stmt = NULL;
  ObLogTableScan *table_scan = NULL;
  ObTablePartitionInfo *table_scan_part_info = NULL;
  const ObTablePartitionInfo *index_scan_part_info = NULL;
  ObIAllocator &allocator = get_allocator();
  table_get = NULL;
  if (OB_ISNULL(index_scan) || OB_ISNULL(index_scan->get_sharding()) || OB_ISNULL(table_item) ||
      OB_ISNULL(stmt = get_stmt()) || OB_ISNULL(optimizer_context_.get_query_ctx()) ||
      OB_ISNULL(index_scan_part_info = index_scan->get_table_partition_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(index_scan), K(table_item), K(stmt),
        K(index_scan_part_info), K(ret));
  } else if (OB_ISNULL(table_scan = static_cast<ObLogTableScan*>(get_log_op_factory().allocate(
                                    *this, LOG_TABLE_SCAN)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate table scan operator", K(ret));
  } else if (OB_ISNULL(table_scan_part_info = static_cast<ObTablePartitionInfo*>(allocator.alloc(
                                              sizeof(ObTablePartitionInfo))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate table partition info", K(ret));
  } else if (FALSE_IT(table_scan_part_info = new (table_scan_part_info) ObTablePartitionInfo(allocator))) {
  } else if (OB_FAIL(table_scan_part_info->assign(*index_scan_part_info))) {
    LOG_WARN("failed to assign table partition info", K(ret));
  } else {
    table_scan->set_index_back(false);
    table_scan->set_table_id(table_id);
    table_scan->set_ref_table_id(index_scan->get_ref_table_id());
    table_scan->set_index_table_id(index_scan->get_ref_table_id());
    table_scan->set_scan_direction(index_scan->get_scan_direction());
    // 改掉project table partition info的table_id
    table_scan_part_info->get_table_location().set_table_id(table_scan->get_table_id());
    table_scan_part_info->get_phy_tbl_location_info_for_update().set_table_location_key(
        table_scan->get_table_id(), table_scan->get_ref_table_id());
    table_scan->set_table_partition_info(table_scan_part_info);
    table_scan->set_strong_sharding(index_scan->get_strong_sharding());
    table_scan->set_phy_plan_type(index_scan->get_phy_plan_type());
    table_scan->set_location_type(index_scan->get_location_type());
    table_scan->set_flashback_query_expr(index_scan->get_flashback_query_expr());
    table_scan->set_flashback_query_type(index_scan->get_flashback_query_type());
    table_scan->set_fq_read_tx_uncommitted(index_scan->get_fq_read_tx_uncommitted());
    table_scan->set_part_ids(index_scan->get_part_ids());
    table_scan->get_table_name() = table_item->alias_name_.length() > 0 ?
                                   table_item->alias_name_ : table_item->table_name_;
    // set card and cost
    table_scan->set_card(1.0);
    table_scan->set_op_cost(ObOptEstCost::cost_late_materialization_table_get(stmt->get_column_size(),
                                                                              get_optimizer_context().get_cost_model_type()));
    table_scan->set_cost(table_scan->get_op_cost());
    table_scan->set_table_row_count(index_scan->get_table_row_count());
    table_scan->set_output_row_count(1.0);
    table_scan->set_phy_query_range_row_count(1.0);
    table_scan->set_query_range_row_count(1.0);
    table_get = table_scan;
  }
  return ret;
}

int ObSelectLogPlan::generate_late_materialization_table_item(ObSelectStmt *stmt,
                                                              uint64_t old_table_id,
                                                              uint64_t new_table_id,
                                                              TableItem *&new_table_item)
{
  int ret = OB_SUCCESS;
  TableItem *temp_item = NULL;
  TableItem *old_table_item = NULL;
  new_table_item = NULL;
  if (OB_ISNULL(stmt) ||
      OB_ISNULL(old_table_item = stmt->get_table_item_by_id(old_table_id))) {
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
    temp_item->table_type_ = old_table_item->table_type_;

    const char *str = "_alias";
    char *buf = static_cast<char*>(get_allocator().alloc(
        old_table_item->table_name_.length() + strlen(str)));
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

int ObSelectLogPlan::allocate_late_materialization_join_as_top(ObLogicalOperator *left_child,
                                                               ObLogicalOperator *right_child,
                                                               ObLogicalOperator *&join_op)
{
  int ret = OB_SUCCESS;
  ObLogJoin *join = NULL;
  ObLogicalOperator *parent = NULL;
  if (OB_ISNULL(left_child) || OB_ISNULL(right_child)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_ISNULL(join = static_cast<ObLogJoin *>(get_log_op_factory().allocate(*this, LOG_JOIN)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to allocate join_op operator", K(ret));
  } else if (OB_ISNULL(join)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(left_child->est_cost())) {
    LOG_WARN("failed to estimate cost for left child", K(ret));
  } else {
    parent = left_child->get_parent();
    join->set_left_child(left_child);
    join->set_right_child(right_child);
    if (NULL != parent) {
      for (int64_t i = 0; i < parent->get_num_of_child(); i++) {
        if (left_child == parent->get_child(i)) {
          parent->set_child(i, join);
          join->set_parent(parent);
          break;
        }
      }
    }
    left_child->set_parent(join);
    right_child->set_parent(join);
    if (left_child->is_plan_root()) {
      join->mark_is_plan_root();
      left_child->set_is_plan_root(false);
      set_plan_root(join);
    }
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
                                                       join->get_cost(),
                                                       get_optimizer_context().get_cost_model_type());
    if (OB_FAIL(join->set_op_ordering(left_child->get_op_ordering()))) {
      LOG_WARN("failed to set op ordering", K(ret));
    } else {
      join->set_location_type(left_child->get_location_type());
      join->set_phy_plan_type(left_child->get_phy_plan_type());
      join->set_strong_sharding(left_child->get_sharding());
      join->set_interesting_order_info(left_child->get_interesting_order_info());
      join->set_fd_item_set(&left_child->get_fd_item_set());
      join_op = join;
    }
  }
  return ret;
}

int ObSelectLogPlan::if_plan_need_late_materialization(ObLogicalOperator *top,
                                                       ObLogTableScan *&index_scan,
                                                       double &late_mater_cost,
                                                       bool &need)
{
  int ret = OB_SUCCESS;
  const ObDMLStmt *stmt = NULL;
  need = false;
  index_scan = NULL;
  ObLogSort *child_sort = NULL;
  ObLogTableScan *table_scan = NULL;
  if (OB_ISNULL(top) || OB_ISNULL(stmt = get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(top), K(get_stmt()), K(ret));
  } else if (OB_FAIL(get_late_materialization_operator(top,
                                                       child_sort,
                                                       table_scan))) {
    LOG_WARN("failed to get late materialization operator", K(ret));
  } else if (NULL != table_scan && NULL != table_scan->get_plan() && NULL != child_sort) {
    ObSEArray<ObRawExpr*, 4> temp_exprs;
    ObSEArray<ObRawExpr*, 4> table_keys;
    ObSEArray<uint64_t, 4> index_column_ids;
    ObSEArray<uint64_t, 4> used_column_ids;
    const ObTableSchema *index_schema = NULL;
    ObSqlSchemaGuard *schema_guard = NULL;
    // check whether index key cover filter exprs, sort exprs and part exprs
    if (table_scan->is_index_scan() && table_scan->get_index_back() &&
        (table_scan->is_local() || table_scan->is_remote())) {
      if (OB_FAIL(get_rowkey_exprs(table_scan->get_table_id(),
                                   table_scan->get_ref_table_id(),
                                   table_keys))) {
        LOG_WARN("failed to generate rowkey exprs", K(ret));
      } else if (OB_FAIL(child_sort->get_sort_exprs(temp_exprs))) {
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
      } else if (OB_FAIL(schema_guard->get_table_schema(
                 table_scan->get_table_id(),
                 table_scan->get_index_table_id(),
                 stmt,
                 index_schema))) {
        LOG_WARN("failed to get table schema", K(ret));
      } else if (OB_ISNULL(index_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(index_schema->get_column_ids(index_column_ids))) {
        LOG_WARN("failed to get column ids", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::extract_column_ids(temp_exprs, used_column_ids))) {
        LOG_WARN("failed to extract column ids", K(ret));
      } else if (ObOptimizerUtil::is_subset(used_column_ids, index_column_ids)){
        bool has_other_col = false;
        for (int64_t i = 0; OB_SUCC(ret) && !has_other_col && i < stmt->get_column_size(); i++) {
          const ColumnItem *item = stmt->get_column_item(i);
          if (OB_ISNULL(item)) {
            ret = OB_ERR_UNEXPECTED;
          } else if (item->get_expr()->is_virtual_generated_column()) {
            // do nothing
          } else if (!ObOptimizerUtil::find_item(index_column_ids, item->base_cid_)) {
            has_other_col = true;
          }
        }
        need = has_other_col;
      }
    }
    // update cost for late materialization
    if (OB_SUCC(ret) && need) {
      OPT_TRACE("try late materialization plan, normal plan cost:", top->get_cost());
      if (OB_ISNULL(table_scan->get_est_cost_info())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(table_scan->get_est_cost_info()->access_columns_.assign(used_column_ids))) {
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
                                             table_scan->get_parallel(),
                                             query_range_row_count,
                                             phy_query_range_row_count,
                                             op_cost,
                                             index_back_cost,
                                             get_optimizer_context().get_cost_model_type()))) {
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
            const ColumnItem *col_item = NULL;
            if (OB_ISNULL(col_item = get_column_item_by_id(table_scan->get_table_id(),
                                                           used_column_ids.at(i))) ||
                OB_ISNULL(col_item->expr_)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("get unexpected null", K(col_item), K(ret));
            } else if (OB_FAIL(column_exprs.push_back(col_item->expr_))) {
              LOG_WARN("failed to push back column expr", K(ret));
            } else { /*do nothing*/ }
          }
          if (OB_FAIL(ret)) {
            /*do nothing*/
          } else if (OB_FAIL(ObOptEstCost::estimate_width_for_exprs(table_scan->get_plan()->get_basic_table_metas(),
                                                                    table_scan->get_plan()->get_selectivity_ctx(),
                                                                    column_exprs,
                                                                    width))) {
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
          ObOptEstCost::cost_late_materialization(top->get_card(),
                                                  top->get_cost(),
                                                  stmt->get_column_size(),
                                                  late_mater_cost,
                                                  get_optimizer_context().get_cost_model_type());

          OPT_TRACE("late materialization plan cost:", late_mater_cost);
        }
      }
    }
  }
  return ret;
}

int ObSelectLogPlan::if_stmt_need_late_materialization(bool &need)
{
  int ret = OB_SUCCESS;
  const ObSelectStmt *select_stmt = NULL;
  int64_t child_stmt_size = 0;
  need = false;
  if (OB_ISNULL(select_stmt = get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(select_stmt->get_child_stmt_size(child_stmt_size))) {
    LOG_WARN("failed to get child stmt size", K(ret));
  } else {
    need = !get_log_plan_hint().no_use_late_material()
        && select_stmt->has_limit()
        && select_stmt->has_order_by()
        && !select_stmt->has_hierarchical_query()
        && !select_stmt->has_group_by()
        && !select_stmt->has_rollup()
        && !select_stmt->has_having()
        && !select_stmt->has_window_function()
        && !select_stmt->has_distinct()
        && !select_stmt->is_unpivot_select()
        && 1 == select_stmt->get_from_item_size()
        && 1 == select_stmt->get_table_size()
        && NULL != select_stmt->get_table_item(0)
        && select_stmt->get_table_item(0)->is_basic_table()
        && !select_stmt->get_table_item(0)->is_system_table_ //不允许系统表
        && NULL == get_stmt()->get_part_expr(select_stmt->get_table_item(0)->table_id_,
                                             select_stmt->get_table_item(0)->ref_id_) //不允许分区表
        && 0 == child_stmt_size //不允许有子查询
        && !select_stmt->is_calc_found_rows();
  }
  return ret;
}

int ObSelectLogPlan::candi_allocate_unpivot()
{
  int ret = OB_SUCCESS;
  CandidatePlan candidate_plan;
  ObSEArray<CandidatePlan, 16> unpivot_plans;
  for (int64_t i = 0; OB_SUCC(ret) && i < candidates_.candidate_plans_.count(); ++i) {
    candidate_plan = candidates_.candidate_plans_.at(i);
    if (OB_FAIL(allocate_unpivot_as_top(candidate_plan.plan_tree_))) {
      LOG_WARN("failed to allocate unpovit as top", K(i), K(ret));
    } else if (OB_FAIL(unpivot_plans.push_back(candidate_plan))) {
      LOG_WARN("failed to push back candidate plan", K(ret));
    } else { /*do nothing*/ }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(prune_and_keep_best_plans(unpivot_plans))) {
      LOG_WARN("failed to prune and keep best plans", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObSelectLogPlan::allocate_unpivot_as_top(ObLogicalOperator *&old_top)
{
  int ret = OB_SUCCESS;
  const ObDMLStmt *stmt = static_cast<const ObDMLStmt *>(get_stmt());
  ObLogUnpivot *unpivot = NULL;
  if (OB_ISNULL(old_top)
      || OB_ISNULL(stmt)
      || OB_UNLIKELY(!stmt->is_unpivot_select())
      || OB_UNLIKELY(stmt->get_table_items().empty())
      || OB_ISNULL(stmt->get_table_item(0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Get unexpected null", K(ret), K(old_top), KPC(stmt));
  } else if (OB_ISNULL(unpivot = static_cast<ObLogUnpivot*>
                      (get_log_op_factory().allocate(*this, LOG_UNPIVOT)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to allocate subquery operator", K(ret));
  } else {
    const TableItem *table_item = stmt->get_table_item(0);
    unpivot->unpivot_info_ = stmt->get_unpivot_info();
    unpivot->set_subquery_id(table_item->table_id_);
    unpivot->get_subquery_name().assign_ptr(table_item->table_name_.ptr(),
                                            table_item->table_name_.length());
    unpivot->set_child(ObLogicalOperator::first_child, old_top);
    if (OB_FAIL(unpivot->compute_property())) {
      LOG_WARN("failed to compute property", K(ret));
    } else {
      old_top = unpivot;
    }
  }
  return ret;
}

int ObSelectLogPlan::init_selectivity_metas_for_set(ObSelectLogPlan *sub_plan,
                                                    const uint64_t child_offset)
{
  int ret = OB_SUCCESS;
  const ObSelectStmt *sub_stmt = NULL;
  ObLogicalOperator *best_plan = NULL;
  if (OB_ISNULL(sub_plan) || OB_ISNULL(sub_stmt = sub_plan->get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(sub_plan), K(sub_stmt));
  } else if (OB_INVALID_ID != sub_stmt->get_dblink_id()) {
    // skip
  } else if (OB_FAIL(sub_plan->get_candidate_plans().get_best_plan(best_plan))) {
    LOG_WARN("failed to get best plan", K(ret));
  } else if (OB_ISNULL(best_plan)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(get_basic_table_metas().add_set_child_stmt_meta_info(
              get_stmt(), sub_stmt, child_offset,
              sub_plan->get_update_table_metas(),
              sub_plan->get_selectivity_ctx(),
              best_plan->get_card()))) {
    LOG_WARN("failed to add set child stmt meta info", K(ret));
  } else if (OB_FAIL(get_update_table_metas().copy_table_meta_info(get_basic_table_metas(),
                                                                   child_offset))) {
    LOG_WARN("failed to copy table meta info", K(ret));
  }
  return ret;
}


int ObSelectLogPlan::check_external_table_scan(ObSelectStmt *stmt, bool &has_external_table)
{
  int ret = OB_SUCCESS;
  ObArray<ObSelectStmt*> child_stmts;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (stmt->has_external_table()) {
    has_external_table = true;
  } else {
    if (OB_FAIL(stmt->get_child_stmts(child_stmts))) {
      LOG_WARN("fail to get child stmt", K(ret));
    }
    for (int i = 0; OB_SUCC(ret) && !has_external_table && i < child_stmts.count(); i++) {
      ret = SMART_CALL(check_external_table_scan(child_stmts.at(i), has_external_table));
    }
  }
  return ret;
}

}//sql
}//oceanbase
