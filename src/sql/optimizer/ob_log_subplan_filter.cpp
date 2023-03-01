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
#include "sql/optimizer/ob_log_subplan_filter.h"
#include "sql/optimizer/ob_opt_est_cost.h"
#include "sql/optimizer/ob_log_plan.h"
#include "sql/optimizer/ob_log_table_scan.h"
#include "common/ob_smart_call.h"
#include "sql/optimizer/ob_opt_selectivity.h"
#include "sql/optimizer/ob_join_order.h"
#include "sql/optimizer/ob_log_granule_iterator.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;

int ObLogSubPlanFilter::get_op_exprs(ObIArray<ObRawExpr*> &all_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_exists_style_exprs(all_exprs))) {
    LOG_WARN("failed to get vector expr", K(ret));
  } else if (OB_FAIL(append(all_exprs, subquery_exprs_))) {
    LOG_WARN("failed to append subquery exprs", K(ret));
  } else if (OB_FAIL(append(all_exprs, onetime_exprs_))) {
    LOG_WARN("failed to append onetime exprs", K(ret));
  } else if (OB_FAIL(append(all_exprs, exec_params_))) {
    LOG_WARN("failed to get subquery exprs", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::get_exec_ref_expr(onetime_exprs_, all_exprs))) {
    LOG_WARN("failed to get exec ref exprs", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::get_exec_ref_expr(exec_params_, all_exprs))) {
    LOG_WARN("failed to get exec ref exprs", K(ret));
  } else if (OB_FAIL(ObLogicalOperator::get_op_exprs(all_exprs))) {
    LOG_WARN("failed to get exprs", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObLogSubPlanFilter::get_exists_style_exprs(ObIArray<ObRawExpr*> &subquery_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    const ObIArray<ObRawExpr*> &all_exprs = get_plan()->get_optimizer_context().get_all_exprs().get_expr_array();
    for (int64_t i = 0; OB_SUCC(ret) && i < all_exprs.count(); i++) {
      ObRawExpr *expr = NULL;
      if (OB_ISNULL(expr = all_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (expr->has_flag(CNT_SUB_QUERY) &&
                 OB_FAIL(extract_exist_style_subquery_exprs(expr, subquery_exprs))) {
        LOG_WARN("failed to extract exist style subquery exprs", K(ret));
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

int ObLogSubPlanFilter::extract_exist_style_subquery_exprs(ObRawExpr *expr,
                                                           ObIArray<ObRawExpr*> &exist_style_exprs)
{
  int ret = OB_SUCCESS;
  bool contains = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (!expr->has_flag(CNT_SUB_QUERY)) {
    /*do nothing*/
  } else if (T_OP_EXISTS == expr->get_expr_type() || T_OP_NOT_EXISTS == expr->get_expr_type()) {
    ObRawExpr *param_expr = NULL;
    if (OB_ISNULL(param_expr = expr->get_param_expr(0)) ||
        OB_UNLIKELY(!param_expr->is_query_ref_expr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid [not] exist predicate", K(*expr), K(ret));
    } else if (ObOptimizerUtil::find_item(subquery_exprs_, param_expr) &&
               OB_FAIL(exist_style_exprs.push_back(expr))) {
      LOG_WARN("failed to push back exprs", K(ret));
    } else { /*do nothing*/ }
  } else if (expr->has_flag(IS_WITH_ALL) || expr->has_flag(IS_WITH_ANY)) {
    if (OB_ISNULL(expr->get_param_expr(0)) || OB_ISNULL(expr->get_param_expr(1)) ||
        OB_UNLIKELY(!expr->get_param_expr(1)->is_query_ref_expr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid anyall predicate", K(*expr), K(ret));
    } else if ((ObOptimizerUtil::find_item(subquery_exprs_, expr->get_param_expr(0)) ||
                ObOptimizerUtil::find_item(subquery_exprs_, expr->get_param_expr(1))) &&
               OB_FAIL(exist_style_exprs.push_back(expr))) {
      LOG_WARN("failed to push back expr", K(ret));
    } else { /*do nothing*/ }
  } else if (OB_FAIL(check_expr_contain_row_subquery(expr, contains))) {
    LOG_WARN("failed to check whether expr contains op row subquery", K(ret));
  } else if (contains) {
    if (OB_FAIL(exist_style_exprs.push_back(expr))) {
      LOG_WARN("faield to push back exprs", K(ret));
    } else { /*do nothing*/ }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); i++) {
      if (OB_FAIL(SMART_CALL(extract_exist_style_subquery_exprs(expr->get_param_expr(i),
                                                                exist_style_exprs)))) {
        LOG_WARN("failed to extract exist or anyall subquery", K(ret));
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

int ObLogSubPlanFilter::check_expr_contain_row_subquery(const ObRawExpr *expr,
                                                        bool &contains)
{
  int ret = OB_SUCCESS;
  contains = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    bool has_row = false;
    bool has_subquery = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); i++) {
      const ObRawExpr *temp_expr = NULL;
      if (OB_ISNULL(temp_expr = expr->get_param_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (T_OP_ROW == temp_expr->get_expr_type()) {
        has_row = true;
      } else if (temp_expr->has_flag(IS_SUB_QUERY) &&
                 ObOptimizerUtil::find_item(subquery_exprs_, temp_expr)) {
        has_subquery = true;
      } else { /*do nothing*/ }
    }
    if (OB_SUCC(ret)) {
      contains = has_row && has_subquery;
    }
  }
  return ret;
}

bool ObLogSubPlanFilter::is_my_exec_expr(const ObRawExpr *expr)
{
  return ObOptimizerUtil::find_item(exec_params_, expr) || is_my_onetime_expr(expr);
}

bool ObLogSubPlanFilter::is_my_onetime_expr(const ObRawExpr *expr)
{
  return ObOptimizerUtil::find_item(onetime_exprs_, expr);
}

bool ObLogSubPlanFilter::is_my_subquery_expr(const ObQueryRefRawExpr *query_expr)
{
  return ObOptimizerUtil::find_item(subquery_exprs_, query_expr);
}


int ObLogSubPlanFilter::get_plan_item_info(PlanText &plan_text,
                                           ObSqlPlanItem &plan_item)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObLogicalOperator::get_plan_item_info(plan_text, plan_item))) {
    LOG_WARN("failed to get plan item info", K(ret));
  }
  BEGIN_BUF_PRINT;
  EXPLAIN_PRINT_EXEC_EXPRS(exec_params_, type);
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(BUF_PRINTF(", "))) {
    LOG_WARN("BUF_PRINTF fails", K(ret));
  } else {
    EXPLAIN_PRINT_EXEC_EXPRS(onetime_exprs_, type);
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(BUF_PRINTF(", "))) {
    LOG_WARN("BUF_PRINTF fails", K(ret));
  } else {
    EXPLAIN_PRINT_IDXS(init_plan_idxs_);
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(BUF_PRINTF(", "))) {
    LOG_WARN("BUF_PRINTF fails", K(ret));
  } else if (OB_FAIL(BUF_PRINTF("batch_das=%s",
            enable_das_batch_rescans_ ? "true" : "false"))) {
    LOG_WARN("BUF_PRINTF fails", K(ret));
  } else { /* Do nothing */ }
  END_BUF_PRINT(plan_item.special_predicates_,
                plan_item.special_predicates_len_);
  return ret;
}

int ObLogSubPlanFilter::est_cost()
{
  int ret = OB_SUCCESS;
  double first_child_card = 0.0;
  const ObLogicalOperator *first_child = NULL;
  if (OB_ISNULL(first_child = get_child(ObLogicalOperator::first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get child", K(ret), K(first_child));
  } else if (OB_FALSE_IT(first_child_card = first_child->get_card())) {
  } else if (OB_FAIL(inner_est_cost(first_child_card, op_cost_))) {
    LOG_WARN("failed to est cost", K(ret));
  } else {
    set_cost(op_cost_ + first_child->get_cost());
    set_card(first_child_card);
  }
  return ret;
}

int ObLogSubPlanFilter::re_est_cost(EstimateCostInfo &param, double &card, double &cost)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *child = NULL;
  double child_cost = 0.0;
  double op_cost = op_cost_;
  double first_child_card = 0.0;
  double sel = 1.0;
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get child", K(ret));
  } else if (OB_FAIL(ObOptSelectivity::calculate_selectivity(get_plan()->get_basic_table_metas(),
                                                            get_plan()->get_selectivity_ctx(),
                                                            get_filter_exprs(),
                                                            sel,
                                                            get_plan()->get_predicate_selectivities()))) {
    LOG_WARN("failed to calc selectivity", K(ret));
  } else if (OB_ISNULL(child = get_child(ObLogicalOperator::first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (param.need_row_count_ >= 0 &&
             param.need_row_count_ < get_card() &&
             sel > OB_DOUBLE_EPSINON &&
             OB_FALSE_IT(param.need_row_count_ /= sel)) {
  } else if (OB_FAIL(SMART_CALL(child->re_est_cost(param,
                                        first_child_card,
                                        child_cost)))) {
    LOG_WARN("failed to re-estimate cost", K(ret));
  } else if (OB_FAIL(inner_est_cost(first_child_card, op_cost))) {
    LOG_WARN("failed to est cost", K(ret));
  } else {
    cost = op_cost + child_cost;
    card = first_child_card;
    if (param.override_) {
      set_op_cost(op_cost);
      set_cost(cost);
      set_card(card);
    }
  }
  return ret;
}


int ObLogSubPlanFilter::inner_est_cost(double &first_child_card, double &op_cost)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *first_child = NULL;
  ObSEArray<ObBasicCostInfo, 4> children_cost_info;
  if (OB_ISNULL(first_child = get_child(ObLogicalOperator::first_child)) || OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get child", K(ret), K(first_child));
  } else if (OB_FALSE_IT(get_plan()->get_selectivity_ctx().init_op_ctx(
      &first_child->get_output_equal_sets(), first_child->get_card()))) {
  } else if (OB_FAIL(get_children_cost_info(first_child_card, children_cost_info))) {
    LOG_WARN("failed to get children cost info from subplan filter", K(ret));
  } else {
    double sel = 1.0;
    ObOptimizerContext &opt_ctx = get_plan()->get_optimizer_context();
    ObSubplanFilterCostInfo info(children_cost_info,
                                 get_onetime_idxs(),
                                 get_initplan_idxs());
    if (OB_FAIL(ObOptEstCost::cost_subplan_filter(info, op_cost,
                                                  opt_ctx.get_cost_model_type()))) {
      LOG_WARN("failed to calculate  the cost of subplan filter", K(ret));
    } else if (OB_FAIL(ObOptSelectivity::calculate_selectivity(get_plan()->get_update_table_metas(),
                                                               get_plan()->get_selectivity_ctx(),
                                                               get_filter_exprs(),
                                                               sel,
                                                               get_plan()->get_predicate_selectivities()))) {
      LOG_WARN("failed to calc selectivity", K(ret));
    } else {
      first_child_card *= sel;
    }
  }
  return ret;
}

int ObLogSubPlanFilter::get_children_cost_info(double &first_child_refine_card, ObIArray<ObBasicCostInfo> &children_cost_info)
{
  int ret = OB_SUCCESS;
  int64_t parallel = 0.0;
  if (OB_UNLIKELY((parallel = get_parallel()) < 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected parallel degree", K(parallel), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < get_num_of_child(); ++i) {
      const ObLogicalOperator *child = get_child(i);
      if (OB_ISNULL(child)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(i));
      } else {
        double child_card = (i == 0) ? first_child_refine_card / parallel : child->get_card();
        ObBasicCostInfo info(child_card, child->get_cost(), child->get_width(), child->is_exchange_allocated());
        if (OB_FAIL(children_cost_info.push_back(info))) {
          LOG_WARN("push back child's cost info failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObLogSubPlanFilter::allocate_granule_pre(AllocGIContext &ctx)
{
  int ret = OB_SUCCESS;
  if (!ctx.exchange_above()) {
    LOG_TRACE("no exchange above, do nothing");
  } else if (!ctx.is_in_partition_wise_state() &&
             !ctx.is_in_pw_affinity_state() &&
             is_partition_wise()) {
    ctx.set_in_partition_wise_state(this);
    LOG_TRACE("in find partition wise state", K(*this));
  } else if (ctx.is_in_partition_wise_state()) {
    if (DIST_PARTITION_NONE == dist_algo_) {
      if (OB_FAIL(ctx.set_pw_affinity_state())) {
        LOG_WARN("set affinity state failed", K(ret), K(ctx));
      }
      LOG_TRACE("partition wise affinity", K(ret));
    }
  }
  return ret;
}

int ObLogSubPlanFilter::allocate_granule_post(AllocGIContext &ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(pw_allocate_granule_post(ctx))) {
    LOG_WARN("failed to do pw allocate granule post", K(ret));
  } else if (DistAlgo::DIST_NONE_ALL == dist_algo_) {
    ObLogicalOperator *op = NULL;
    bool cnt_pd_range_cond = false;
    if (OB_ISNULL(get_child(first_child))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid child", K(ret));
    } else if (OB_FAIL(get_child(first_child)->find_first_recursive(log_op_def::LOG_GRANULE_ITERATOR, op))) {
      LOG_WARN("find granule iterator in right failed", K(ret));
    } else if (NULL == op) {
      // granule iterator not found, do nothing
    } else if (OB_FAIL(ObOptimizerUtil::check_pushdown_range_cond(get_child(first_child), cnt_pd_range_cond))) {
      LOG_WARN("failed to check any push down range cond", K(ret));
    } else if (cnt_pd_range_cond) {
      static_cast<ObLogGranuleIterator *>(op)->add_flag(GI_NLJ_PARAM_DOWN);
      static_cast<ObLogGranuleIterator *>(op)->add_flag(GI_FORCE_PARTITION_GRANULE);
    }
  } else if (DistAlgo::DIST_PARTITION_NONE == dist_algo_) {
    ObLogicalOperator *op = NULL;
    ObLogicalOperator *child = NULL;
    for (int i = 1; OB_SUCC(ret) && i < get_num_of_child(); ++i) {
      if (OB_FAIL(set_granule_nodes_affinity(ctx, i))) {
        LOG_WARN("set granule nodes affinity failed", K(ret));
      } else if (OB_ISNULL(child = get_child(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null child", K(ret), K(child));
      } else if (OB_FAIL(child->find_first_recursive(log_op_def::LOG_GRANULE_ITERATOR, op))) {
        LOG_WARN("find granule iterator in right failed", K(ret));
      } else if (NULL == op) {
        // granule iterator not found, do nothing
      } else {
        static_cast<ObLogGranuleIterator *>(op)->add_flag(GI_PARTITION_WISE);
        LOG_TRACE("set right child gi to affinity", K(i));
      }
    }
  }
  return ret;
}

int ObLogSubPlanFilter::compute_one_row_info()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *child = NULL;
  if (OB_ISNULL(child = get_child(ObLogicalOperator::first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(child), K(ret));
  } else {
    set_is_at_most_one_row(child->get_is_at_most_one_row());
  }
  return ret;
}

int ObLogSubPlanFilter::compute_sharding_info()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(get_plan()), K(ret));
  } else if (DistAlgo::DIST_BASIC_METHOD == dist_algo_) {
    if (OB_FAIL(ObOptimizerUtil::compute_basic_sharding_info(
                                    get_plan()->get_optimizer_context().get_local_server_addr(),
                                    get_child_list(),
                                    get_plan()->get_allocator(),
                                    dup_table_pos_,
                                    strong_sharding_))) {
      LOG_WARN("failed to compute basic sharding info", K(ret));
    } else { /*do nothing*/ }
  } else if (DistAlgo::DIST_PULL_TO_LOCAL == dist_algo_) {
    strong_sharding_ = get_plan()->get_optimizer_context().get_local_sharding();
  } else if (DistAlgo::DIST_NONE_ALL == dist_algo_) {
    ObShardingInfo *sharding = NULL;
    if (OB_ISNULL(get_child(0)) ||
        OB_ISNULL(sharding = get_child(0)->get_sharding())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(sharding), K(ret));
    } else if (OB_FAIL(weak_sharding_.assign(get_child(0)->get_weak_sharding()))) {
      LOG_WARN("failed to assign weak sharding", K(ret));
    } else {
      strong_sharding_ = get_child(0)->get_strong_sharding();
    }
  } else if (DistAlgo::DIST_PARTITION_WISE == dist_algo_) {
    for (int64_t i = 0; OB_SUCC(ret) && i < get_num_of_child(); i++) {
      ObShardingInfo *sharding = NULL;
      if (OB_ISNULL(get_child(i)) ||
          OB_ISNULL(sharding = get_child(i)->get_sharding())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(sharding), K(ret));
      } else if (!sharding->is_match_all()) {
        if (OB_FAIL(weak_sharding_.assign(get_child(i)->get_weak_sharding()))) {
          LOG_WARN("failed to assign weak sharding", K(ret));
        } else {
          strong_sharding_ = get_child(i)->get_strong_sharding();
          break;
        }
      } else { /*do nothing*/}
    }
    if (OB_SUCC(ret)) {
      is_partition_wise_ = true;
    }
  } else if (DistAlgo::DIST_PARTITION_NONE == dist_algo_) {
    ObShardingInfo *sharding = NULL;
    if (OB_ISNULL(get_child(1)) ||
        OB_ISNULL(sharding = get_child(1)->get_sharding())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(sharding), K(ret));
    } else if (OB_FAIL(weak_sharding_.assign(get_child(1)->get_weak_sharding()))) {
      LOG_WARN("failed to assign weak sharding", K(ret));
    } else {
      strong_sharding_ = get_child(1)->get_strong_sharding();
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected type", K(ret));
  }
  return ret;
}

int ObLogSubPlanFilter::check_if_match_das_batch_rescan(ObLogicalOperator *root,
                                                        bool &enable_das_batch_rescans)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(root)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (root->is_table_scan()) {
    bool is_valid = false;
    ObLogTableScan *tsc = static_cast<ObLogTableScan*>(root);
    if (!tsc->use_das()) {
      enable_das_batch_rescans = false;
    } else if (OB_FAIL(ObOptimizerUtil::check_contribute_query_range(root,
                                                                     get_exec_params(),
                                                                     is_valid))) {
      LOG_WARN("failed to check query range contribution", K(ret));
    } else if (!is_valid) {
      enable_das_batch_rescans = false;
    } else if (tsc->get_scan_direction() != default_asc_direction()) {
      enable_das_batch_rescans = false;
    } else if (tsc->has_index_scan_filter() && tsc->get_index_back() && tsc->get_is_index_global()) {
      // For the global index lookup, if there is a pushdown filter when scanning the index,
      // batch cannot be used.
      enable_das_batch_rescans = false;
    } else {/*do nothing*/}
  } else if (root->get_num_of_child() == 1) {
    if (OB_SUCC(ret)) {
      if (OB_FAIL(SMART_CALL(check_if_match_das_batch_rescan(root->get_child(0),
                                                             enable_das_batch_rescans)))) {
        LOG_WARN("failed to check match das batch rescan", K(ret));
      }
    }
  } else {/*do nothing*/}
  return ret;
}

int ObLogSubPlanFilter::set_use_das_batch(ObLogicalOperator* root)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(root)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid input", K(ret));
  } else if (root->is_table_scan()) {
    ObLogTableScan *ts = static_cast<ObLogTableScan*>(root);
    if (!ts->get_range_conditions().empty()) {
      ts->set_use_batch(enable_das_batch_rescans_);
    }
  } else if (root->get_num_of_child() == 1) {
    if(OB_FAIL(SMART_CALL(set_use_das_batch(root->get_child(first_child))))) {
      LOG_WARN("failed to check use das batch", K(ret));
    }
  } else { /*do nothing*/ }
  return ret;
}

int ObLogSubPlanFilter::check_and_set_use_batch()
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session_info = NULL;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(session_info = get_plan()->get_optimizer_context().get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (OB_FAIL(session_info->get_nlj_batching_enabled(enable_das_batch_rescans_))) {
    LOG_WARN("failed to get enable batch variable", K(ret));
  }
  // check use batch
  for (int64_t i = 1; OB_SUCC(ret) && enable_das_batch_rescans_ && i < get_num_of_child(); i++) {
    ObLogicalOperator *child = get_child(i);
    if (OB_ISNULL(child)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else if (get_initplan_idxs().has_member(i) || get_onetime_idxs().has_member(i)) {
      enable_das_batch_rescans_ = false;
    } else if (!(child->get_type() == log_op_def::LOG_TABLE_SCAN
                 || child->get_type() == log_op_def::LOG_SUBPLAN_SCAN)) {
      enable_das_batch_rescans_ = false;
    } else if (OB_FAIL(check_if_match_das_batch_rescan(child, enable_das_batch_rescans_))) {
      LOG_WARN("failed to check match das batch rescan", K(ret));
    } else {
      // do nothing
    }
  }
  // set use batch
  for (int64_t i = 1; OB_SUCC(ret) && i < get_num_of_child(); i++) {
    ObLogicalOperator *child = get_child(i);
    if (OB_ISNULL(child)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else if (OB_FAIL(set_use_das_batch(child))) {
      LOG_WARN("failed to set use das batch rescan", K(ret));
    }
  }
  LOG_TRACE("spf das batch rescan", K(ret), K(enable_das_batch_rescans_));
  return ret;
}

int ObLogSubPlanFilter::allocate_startup_expr_post()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(allocate_startup_expr_post(first_child))) {
    LOG_WARN("failed to allocate startup expr post", K(ret));
  }
  return ret;
}

int ObLogSubPlanFilter::inner_replace_op_exprs(
    const ObIArray<std::pair<ObRawExpr *, ObRawExpr *> > &to_replace_exprs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exec_params_.count(); ++i) {
    if (OB_ISNULL(exec_params_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("exec param is null", K(ret));
    } else if (OB_FAIL(replace_expr_action(to_replace_exprs,
                                           exec_params_.at(i)->get_ref_expr()))) {
      LOG_WARN("failed to replace expr action", K(ret));
    }
  }
  return ret;
}

int ObLogSubPlanFilter::allocate_subquery_id()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < subquery_exprs_.count(); ++i) {
    if (OB_ISNULL(subquery_exprs_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("subquery expr is null", K(ret));
    } else {
      subquery_exprs_.at(i)->set_ref_id(i + 1);
    }
  }
  return ret;
}

int ObLogSubPlanFilter::replace_nested_subquery_exprs(
    const common::ObIArray<std::pair<ObRawExpr *, ObRawExpr*>> &to_replace_exprs)
{
  int ret = OB_SUCCESS;
  ObLogPlan *plan = NULL;
  if (OB_ISNULL(plan = get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null plan", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < subquery_exprs_.count(); ++i) {
    ObRawExpr *expr = subquery_exprs_.at(i);
    if (ObOptimizerUtil::find_item(plan->get_onetime_query_refs(), expr)) {
      // do not replace onetime expr ref query, only adjust nested subquery
    } else if (OB_FAIL(replace_expr_action(to_replace_exprs, expr))) {
      LOG_WARN("failed to replace nested subquery expr", K(ret));
    } else if (expr == subquery_exprs_.at(i)) {
      // do nothing
    } else if (OB_UNLIKELY(!expr->is_query_ref_expr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected expr type", K(ret));
    } else {
      subquery_exprs_.at(i) = static_cast<ObQueryRefRawExpr*>(expr);
    }
  }
  return ret;
}

int ObLogSubPlanFilter::get_equal_set_conditions(ObIArray<ObRawExpr*> &equal_conds)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *right_child = NULL;
  ObRawExprFactory *expr_factory = NULL;
  ObSQLSessionInfo *session_info = NULL;
  ObSEArray<ObRawExpr*, 4> left_keys;
  ObSEArray<ObRawExpr*, 4> right_keys;
  ObSEArray<bool, 4> null_safe_info;

  if (OB_ISNULL(get_plan()) ||
      OB_ISNULL(session_info = get_plan()->get_optimizer_context().get_session_info()) ||
      OB_ISNULL(expr_factory = &get_plan()->get_optimizer_context().get_expr_factory())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(get_plan()), K(session_info), K(ret));
  } else {
    for (int64_t i = 1; OB_SUCC(ret) && i < get_num_of_child(); ++i) {
      left_keys.reuse();
      right_keys.reuse();
      null_safe_info.reuse();
      if (OB_ISNULL(right_child = get_child(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("child is null", K(ret), K(right_child));
      } else if (OB_FAIL(get_plan()->get_subplan_filter_equal_keys(right_child,
                                                                   exec_params_,
                                                                   left_keys,
                                                                   right_keys,
                                                                   null_safe_info))) {
        LOG_WARN("failed to get equal set conditions", K(ret));
      } else if (OB_UNLIKELY(left_keys.count() != right_keys.count()) ||
                 OB_UNLIKELY(left_keys.count() != null_safe_info.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("left keys should equal right keys", K(ret));
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && j < left_keys.count(); ++j) {
          ObRawExpr* lexpr;
          ObRawExpr* rexpr;
          ObRawExpr* equal_expr = NULL;
          if (OB_ISNULL(lexpr = left_keys.at(j)) ||
              OB_ISNULL(rexpr = right_keys.at(j))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null", K(ret));
          } else if (OB_FAIL(ObRawExprUtils::create_equal_expr(*expr_factory,
                                                              session_info,
                                                              lexpr,
                                                              rexpr,
                                                              equal_expr))) {
            LOG_WARN("failed to create equal expr", K(ret));
          } else if (OB_FAIL(equal_conds.push_back(equal_expr))) {
            LOG_WARN("failed to push back equal conds", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObLogSubPlanFilter::compute_equal_set()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *child = NULL;
  EqualSets *ordering_esets = NULL;
  EqualSets input_equal_sets;
  ObSEArray<ObRawExpr*, 8> equal_set_conditions;
  if (OB_ISNULL(my_plan_) || OB_UNLIKELY(get_num_of_child() < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("operator is invalid", K(ret), K(get_num_of_child()), K(my_plan_));
  } else if (OB_UNLIKELY(get_num_of_child() == 0)) {
    // do nothing
  } else if (OB_ISNULL(child = get_child(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child is null", K(ret), K(child));
  } else if (OB_FAIL(get_equal_set_conditions(equal_set_conditions))) {
    LOG_WARN("failed to get equal set conditions", K(ret));
  } else if (append(equal_set_conditions, filter_exprs_)) {
    LOG_WARN("failed to append", K(ret));
  } else if (equal_set_conditions.empty()) {
    // inherit equal sets from the first child directly
    set_output_equal_sets(&child->get_output_equal_sets());
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < get_num_of_child(); ++i) {
      if (OB_ISNULL(child)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("child is null", K(ret), K(child));
      } else if (append(input_equal_sets, child->get_output_equal_sets())) {
        LOG_WARN("failed to init input equal sets", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(ordering_esets = get_plan()->create_equal_sets())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to create equal sets", K(ret));
    } else if (OB_FAIL(ObEqualAnalysis::compute_equal_set(
                       &my_plan_->get_allocator(),
                       equal_set_conditions,
                       input_equal_sets,
                       *ordering_esets))) {
      LOG_WARN("failed to compute ordering output equal set", K(ret));
    } else {
      set_output_equal_sets(ordering_esets);
    }
  }
  return ret;
}

int ObLogSubPlanFilter::allocate_startup_expr_post(int64_t child_idx)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *child = get_child(child_idx);
  if (OB_ISNULL(child)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null child", K(ret));
  } else if (child->get_startup_exprs().empty()) {
    //do nothing
  } else {
    ObSEArray<ObRawExpr*, 4> non_startup_exprs, new_startup_exprs;
    ObIArray<ObRawExpr*> &startup_exprs = child->get_startup_exprs();
    ObSEArray<ObExecParamRawExpr*, 4> my_exec_params;
    if (OB_FAIL(my_exec_params.assign(onetime_exprs_))) {
      LOG_WARN("fail to push back onetime exprs", K(ret));
    } else if (OB_FAIL(append(my_exec_params, exec_params_))) {
      LOG_WARN("fail to push back exec param exprs", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < startup_exprs.count(); ++i) {
      if (OB_ISNULL(startup_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null expr", K(ret));
      } else if (startup_exprs.at(i)->has_flag(CNT_ROWNUM)) {
        if (OB_FAIL(non_startup_exprs.push_back(startup_exprs.at(i)))) {
          LOG_WARN("fail to push back non startup expr",K(ret));
        }
      } else if (startup_exprs.at(i)->has_flag(CNT_DYNAMIC_PARAM)) {
        bool found = false;
        if (!my_exec_params.empty()
            && OB_FAIL(ObOptimizerUtil::check_contain_my_exec_param(startup_exprs.at(i), my_exec_params, found))) {
          LOG_WARN("fail to check if contain onetime exec param", K(ret));
        } else if (found && OB_FAIL(non_startup_exprs.push_back(startup_exprs.at(i)))) {
          LOG_WARN("fail to push back non startup expr",K(ret));
        } else if (!found && OB_FAIL(new_startup_exprs.push_back(startup_exprs.at(i)))) {
          LOG_WARN("fail to push back non startup expr",K(ret));
        }
      } else if (OB_FAIL(new_startup_exprs.push_back(startup_exprs.at(i)))) {
        LOG_WARN("failed to push back expr", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(append_array_no_dup(get_startup_exprs(), new_startup_exprs))) {
        LOG_WARN("failed to add startup exprs", K(ret));
      } else if (OB_FAIL(child->get_startup_exprs().assign(non_startup_exprs))) {
        LOG_WARN("failed to assign exprs", K(ret));
      }
    }
  }
  return ret;
}
