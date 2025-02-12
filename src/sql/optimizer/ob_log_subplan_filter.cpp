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
#include "sql/optimizer/ob_log_table_scan.h"
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
  } else if (OB_FAIL(BUF_PRINTF("use_batch=%s",
            enable_das_group_rescan_ ? "true" : "false"))) {
    LOG_WARN("BUF_PRINTF fails", K(ret));
  } else { /* Do nothing */ }
  END_BUF_PRINT(plan_item.special_predicates_,
                plan_item.special_predicates_len_);
  return ret;
}

int ObLogSubPlanFilter::est_ambient_card()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(inner_est_ambient_card_by_child(ObLogicalOperator::first_child))) {
    LOG_WARN("failed to est ambient cards by first child", K(ret), K(get_type()));
  }
  return ret;
}

int ObLogSubPlanFilter::est_cost()
{
  int ret = OB_SUCCESS;
  double card = 0.0;
  double op_cost = 0.0;
  double cost = 0.0;
  EstimateCostInfo param;
  param.need_parallel_ = get_parallel();
  if (OB_FAIL(do_re_est_cost(param, card, op_cost, cost))) {
    LOG_WARN("failed to get re est cost infos", K(ret));
  } else {
    set_card(card);
    set_op_cost(op_cost);
    set_cost(cost);
  }
  return ret;
}

int ObLogSubPlanFilter::do_re_est_cost(EstimateCostInfo &param, double &card, double &op_cost, double &cost)
{
  int ret = OB_SUCCESS;
  card = 0.0;
  op_cost = 0.0;
  cost = 0.0;
  ObLogicalOperator *child = NULL;
  double sel = 1.0;
  ObSEArray<ObBasicCostInfo, 4> cost_infos;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(child = get_child(ObLogicalOperator::first_child))
      || OB_UNLIKELY(param.need_parallel_ < 1 || param.need_batch_rescan_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected params", K(ret), K(get_plan()), K(child), K(param));
  } else if (param.need_row_count_ < 0 || param.need_row_count_ >= child->get_card()) {
    param.need_row_count_ = -1;
  } else if (OB_FALSE_IT(get_plan()->get_selectivity_ctx().init_op_ctx(child))) {
  } else if (OB_FAIL(ObOptSelectivity::calculate_selectivity(get_plan()->get_basic_table_metas(),
                                                             get_plan()->get_selectivity_ctx(),
                                                             get_filter_exprs(),
                                                             sel,
                                                             get_plan()->get_predicate_selectivities()))) {
    LOG_WARN("failed to calc selectivity", K(ret));
  } else if (sel <= OB_DOUBLE_EPSINON || param.need_row_count_ >= child->get_card() * sel) {
    param.need_row_count_ = -1;
  } else {
    param.need_row_count_ /= sel;
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_re_est_cost_infos(param, cost_infos))) {
    LOG_WARN("failed to get children cost info from subplan filter", K(ret));
  } else if (OB_UNLIKELY(cost_infos.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected cost infos", K(ret), K(cost_infos.count()), K(get_num_of_child()));
  } else if (OB_FALSE_IT(get_plan()->get_selectivity_ctx().init_op_ctx(
                              &child->get_output_equal_sets(), cost_infos.at(0).rows_))) {
  } else if (OB_FAIL(ObOptSelectivity::calculate_selectivity(get_plan()->get_update_table_metas(),
                                                             get_plan()->get_selectivity_ctx(),
                                                             get_filter_exprs(),
                                                             sel,
                                                             get_plan()->get_predicate_selectivities()))) {
    LOG_WARN("failed to calc selectivity", K(ret));
  } else {
    ObBasicCostInfo &cost_info = cost_infos.at(0);
    card = cost_info.rows_ * sel;
    cost_info.rows_ = ObJoinOrder::calc_single_parallel_rows(cost_info.rows_, param.need_parallel_);
    ObOptimizerContext &opt_ctx = get_plan()->get_optimizer_context();
    ObSubplanFilterCostInfo info(cost_infos, get_onetime_idxs(), get_initplan_idxs());
    if (OB_FAIL(ObOptEstCost::cost_subplan_filter(info, op_cost, opt_ctx))) {
      LOG_WARN("failed to calculate  the cost of subplan filter", K(ret));
    } else {
      cost = op_cost + cost_info.cost_;
    }
  }
  return ret;
}

int ObLogSubPlanFilter::get_re_est_cost_infos(const EstimateCostInfo &param,
                                              ObIArray<ObBasicCostInfo> &cost_infos)
{
  int ret = OB_SUCCESS;
  EstimateCostInfo cur_param;
  double cur_child_card = 0.0;
  double cur_child_cost = 0.0;
  bool first_child_is_match_all = false;
  const common::ObIArray<common::ObAddr> *rescan_left_server_list = param.rescan_left_server_list_;
  for (int64_t i = 0; OB_SUCC(ret) && i < get_num_of_child(); ++i) {
    const ObLogicalOperator *child = get_child(i);
    cur_param.reset();
    if (OB_ISNULL(child)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("set operator i-th child is null", K(ret), K(i));
    } else if (OB_FAIL(cur_param.assign(param))) {
      LOG_WARN("failed to assign param", K(ret));
    } else if (0 == i) {
      if (!child->is_match_all()) {
        rescan_left_server_list = &child->get_server_list();
      }
    } else {
      cur_param.need_row_count_ = -1;
      cur_param.need_batch_rescan_ = enable_das_group_rescan()
                                     && !init_plan_idxs_.has_member(i)
                                     && !one_time_idxs_.has_member(i);
      cur_param.rescan_left_server_list_ = rescan_left_server_list;
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(SMART_CALL(get_child(i)->re_est_cost(cur_param, cur_child_card, cur_child_cost)))) {
      LOG_WARN("failed to re-est child cost", K(ret), K(i));
    } else if (OB_FAIL(cost_infos.push_back(ObBasicCostInfo(cur_child_card, cur_child_cost,
                                                            child->get_width(),
                                                            child->is_exchange_allocated())))) {
      LOG_WARN("push back child's cost info failed", K(ret));
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
    ObShardingInfo *sharding = NULL;
    if (OB_FAIL(ObOptimizerUtil::compute_basic_sharding_info(
                                    get_plan()->get_optimizer_context().get_local_server_addr(),
                                    get_child_list(),
                                    get_plan()->get_allocator(),
                                    strong_sharding_,
                                    inherit_sharding_index_))) {
      LOG_WARN("failed to compute basic sharding info", K(ret));
    }
  } else if (DistAlgo::DIST_PULL_TO_LOCAL == dist_algo_) {
    strong_sharding_ = get_plan()->get_optimizer_context().get_local_sharding();
  } else if (DistAlgo::DIST_NONE_ALL == dist_algo_||
             DistAlgo::DIST_HASH_ALL == dist_algo_) {
    ObShardingInfo *sharding = NULL;
    if (OB_ISNULL(get_child(0)) ||
        OB_ISNULL(sharding = get_child(0)->get_sharding())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(sharding), K(ret));
    } else if (OB_FAIL(weak_sharding_.assign(get_child(0)->get_weak_sharding()))) {
      LOG_WARN("failed to assign weak sharding", K(ret));
    } else {
      strong_sharding_ = get_child(0)->get_strong_sharding();
      inherit_sharding_index_ = 0;
    }
  } else if (DistAlgo::DIST_RANDOM_ALL == dist_algo_) {
    strong_sharding_ = get_plan()->get_optimizer_context().get_distributed_sharding();
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
          inherit_sharding_index_ = i;
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
    } else if (OB_FAIL(get_repart_sharding_info(get_child(1),
                                                strong_sharding_,
                                                weak_sharding_))) {
      LOG_WARN("failed to rebuild sharding info", K(ret));
    } else {
      inherit_sharding_index_ = 1;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected type", K(ret));
  }
  return ret;
}

int ObLogSubPlanFilter::compute_spf_batch_rescan()
{
  int ret = OB_SUCCESS;
  enable_das_group_rescan_ = false;
  bool can_batch = false;
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(get_plan()));
  } else if (!get_plan()->get_optimizer_context().enable_spf_batch_rescan()) {
    /* subplan filter group rescan is disabled */
  } else if (get_plan()->get_disable_child_batch_rescan()) {
    /* do nothing */
  } else if (get_plan()->get_optimizer_context().enable_425_exec_batch_rescan()
             && OB_FAIL(compute_spf_batch_rescan(can_batch))) {
    LOG_WARN("failed to compute group rescan", K(ret));
  } else if (!get_plan()->get_optimizer_context().enable_425_exec_batch_rescan()
             && OB_FAIL(compute_spf_batch_rescan_compat(can_batch))) {
    LOG_WARN("failed to compute group rescan compat", K(ret));
  } else {
    enable_das_group_rescan_ = can_batch;
  }
  return ret;
}

int ObLogSubPlanFilter::compute_spf_batch_rescan(bool &can_batch)
{
  int ret = OB_SUCCESS;
  can_batch = true;
  const ObRawExpr *ref_expr = NULL;
  const ObShardingInfo *sharding = NULL;
  const ObLogicalOperator *child = NULL;
  const ObDMLStmt *stmt = NULL;
  const ObLogPlan *plan = NULL;
  bool has_ref_assign_user_var = false;
  bool left_allocated_exchange = false;
  bool right_allocated_exchange = false;
  bool has_rescan_subquery = false;
  if (OB_ISNULL(plan = get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(plan));
  } else if ((!init_plan_idxs_.is_empty() || !one_time_idxs_.is_empty()) &&
      !plan->get_optimizer_context().enable_onetime_initplan_batch()) {
    /* spf contains onetime expr or init plan, enabled after 4.2.5 */
    can_batch = false;
  }

  // check if exec params contain sub_query/rownum
  for (int64_t i = 0; OB_SUCC(ret) && can_batch && i < exec_params_.count(); i++) {
    if (OB_ISNULL(exec_params_.at(i)) || OB_ISNULL(ref_expr = exec_params_.at(i)->get_ref_expr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret), K(i), K(ref_expr), KPC(exec_params_.at(i)));
    } else {
      can_batch &= !ref_expr->has_flag(CNT_ROWNUM);
      if (can_batch && !plan->get_optimizer_context().enable_contains_subquery_batch()) {
        can_batch &= !ref_expr->has_flag(CNT_SUB_QUERY);
      }
    }
  }

  // check if child can batch rescan
  for (int64_t i = 0; OB_SUCC(ret) && can_batch && i < get_num_of_child(); i++) {
    if (OB_ISNULL(child = get_child(i)) || OB_ISNULL(sharding = child->get_sharding())
        || OB_ISNULL(stmt= child->get_stmt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(i), K(sharding), K(stmt));
    } else if (0 >= i) {
      left_allocated_exchange = child->is_exchange_allocated();
    } else if (init_plan_idxs_.has_member(i) || one_time_idxs_.has_member(i)) {
      can_batch = sharding->is_single();
    } else if (OB_FAIL(ObOptimizerUtil::check_can_batch_rescan(child, exec_params_, false, can_batch))) {
      LOG_WARN("failed to check plan can batch rescan", K(ret));
    } else if (OB_FAIL(stmt->has_ref_assign_user_var(has_ref_assign_user_var))) {
      LOG_WARN("faield to check stmt has assignment ref user var", K(ret));
    } else {
      can_batch &= !has_ref_assign_user_var;
      has_rescan_subquery = true;
      right_allocated_exchange |= child->is_exchange_allocated();
    }
  }
  if (OB_FAIL(ret) || !can_batch || !has_rescan_subquery) {
    can_batch = false;
  } else if (DistAlgo::DIST_BASIC_METHOD == dist_algo_
             || DistAlgo::DIST_NONE_ALL == dist_algo_
             || (DistAlgo::DIST_PARTITION_WISE == dist_algo_
                 && !left_allocated_exchange
                 && !right_allocated_exchange)) {
    can_batch = true;
  } else if (DistAlgo::DIST_PULL_TO_LOCAL == dist_algo_ && !right_allocated_exchange) {
    can_batch = true;
  } else {
    can_batch = false;
  }
  return ret;
}

int ObLogSubPlanFilter::compute_spf_batch_rescan_compat(bool &can_batch)
{
  int ret = OB_SUCCESS;
  can_batch = false;
  if (!init_plan_idxs_.is_empty() || !one_time_idxs_.is_empty()) {
    /* disable group rescan for onetime/init plan */
  } else {
    can_batch = true;
    const ObRawExpr *ref_expr = NULL;
    const ObDMLStmt *stmt = NULL;
    bool has_ref_assign_user_var = false;
    // check if exec params contain sub_query/rownum
    for (int64_t i = 0; OB_SUCC(ret) && can_batch && i < exec_params_.count(); i++) {
      if (OB_ISNULL(exec_params_.at(i)) || OB_ISNULL(ref_expr = exec_params_.at(i)->get_ref_expr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret), K(i), K(ref_expr), KPC(exec_params_.at(i)));
      } else {
        can_batch = !ref_expr->has_flag(CNT_SUB_QUERY) && !ref_expr->has_flag(CNT_ROWNUM);
      }
    }
    for (int64_t i = 1; OB_SUCC(ret) && can_batch && i < get_num_of_child(); i++) {
      if (OB_ISNULL(get_child(i)) || OB_ISNULL(stmt= get_child(i)->get_stmt())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(i), K(get_child(i)), K(stmt));
      } else if (OB_FAIL(ObOptimizerUtil::check_can_batch_rescan_compat(get_child(i),
                                                                        exec_params_,
                                                                        false,
                                                                        can_batch))) {
        LOG_WARN("failed to check plan can batch rescan", K(ret));
      } else if (init_plan_idxs_.has_member(i) || one_time_idxs_.has_member(i)) {
        /* do nothing */
      } else if (OB_FAIL(stmt->has_ref_assign_user_var(has_ref_assign_user_var))) {
        LOG_WARN("faield to check stmt has assignment ref user var", K(ret));
      } else {
        can_batch &= !has_ref_assign_user_var;
      }
    }
  }
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

int ObLogSubPlanFilter::inner_replace_op_exprs(ObRawExprReplacer &replacer)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exec_params_.count(); ++i) {
    if (OB_ISNULL(exec_params_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("exec param is null", K(ret));
    } else if (OB_FAIL(replace_expr_action(replacer, exec_params_.at(i)->get_ref_expr()))) {
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

int ObLogSubPlanFilter::replace_nested_subquery_exprs(ObRawExprReplacer &replacer)
{
  int ret = OB_SUCCESS;
  ObLogPlan *plan = NULL;
  if (OB_ISNULL(plan = get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null plan", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < subquery_exprs_.count(); ++i) {
    ObRawExpr *expr = subquery_exprs_.at(i);
    int64_t ref_id = subquery_exprs_.at(i)->get_ref_id();
    if (ObOptimizerUtil::find_item(plan->get_onetime_query_refs(), expr)) {
      // do not replace onetime expr ref query, only adjust nested subquery
    } else if (OB_FAIL(replace_expr_action(replacer, expr))) {
      LOG_WARN("failed to replace nested subquery expr", K(ret));
    } else if (expr == subquery_exprs_.at(i)) {
      // do nothing
    } else if (OB_UNLIKELY(!expr->is_query_ref_expr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected expr type", K(ret));
    } else {
      subquery_exprs_.at(i) = static_cast<ObQueryRefRawExpr*>(expr);
      subquery_exprs_.at(i)->set_ref_id(ref_id);
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
      if (OB_FAIL(ObOptimizerUtil::append_exprs_no_dup(get_startup_exprs(), new_startup_exprs))) {
        LOG_WARN("failed to add startup exprs", K(ret));
      } else if (OB_FAIL(child->get_startup_exprs().assign(non_startup_exprs))) {
        LOG_WARN("failed to assign exprs", K(ret));
      }
    }
  }
  return ret;
}

int ObLogSubPlanFilter::is_my_fixed_expr(const ObRawExpr *expr, bool &is_fixed)
{
  int ret = OB_SUCCESS;
  is_fixed = ObOptimizerUtil::find_item(subquery_exprs_, expr);
  return OB_SUCCESS;
}

int ObLogSubPlanFilter::get_repart_sharding_info(ObLogicalOperator* child_op,
                                                 ObShardingInfo *&strong_sharding,
                                                 ObIArray<ObShardingInfo*> &weak_sharding)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *child = NULL;
  ObSEArray<ObRawExpr*, 4> src_keys;
  ObSEArray<ObRawExpr*, 4> target_keys;
  ObSEArray<bool, 4> null_safe_info;
  EqualSets input_esets;

  for (int64_t i = 1; OB_SUCC(ret) && i < get_num_of_child(); i++) {
    if (OB_ISNULL(child = get_child(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(append(input_esets, child->get_output_equal_sets()))) {
      LOG_WARN("failed to append input equal sets", K(ret));
    } else { /*do nothing*/ }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(get_plan()) ||
             OB_ISNULL(child_op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(get_plan()->get_subplan_filter_equal_keys(child_op,
                                                               exec_params_,
                                                               src_keys,
                                                               target_keys,
                                                               null_safe_info))) {
    LOG_WARN("failed to get repartition keys", K(ret));
  } else if (OB_UNLIKELY(NULL == child_op->get_strong_sharding())) {
    strong_sharding = NULL;
  } else if (OB_FAIL(rebuild_repart_sharding_info(child_op->get_strong_sharding(),
                                                  src_keys,
                                                  target_keys,
                                                  input_esets,
                                                  strong_sharding))) {
    LOG_WARN("failed to rebuild repart sharding info", K(ret));
  } else if (NULL == strong_sharding) {
    strong_sharding = get_plan()->get_optimizer_context().get_distributed_sharding();
  }

  if (OB_SUCC(ret)) {
    weak_sharding.reuse();
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < child_op->get_weak_sharding().count(); ++i) {
    ObShardingInfo* out_sharding = NULL;
    if (OB_FAIL(rebuild_repart_sharding_info(child_op->get_weak_sharding().at(i),
                                             src_keys,
                                             target_keys,
                                             input_esets,
                                             out_sharding))) {
      LOG_WARN("failed to rebuild repart sharding info", K(ret));
    } else if (NULL == out_sharding) {
      /* do nothing */
    } else if (OB_FAIL(weak_sharding.push_back(out_sharding))) {
      LOG_WARN("failed to push back sharding", K(ret));
    }
  }
  return ret;
}

int ObLogSubPlanFilter::rebuild_repart_sharding_info(const ObShardingInfo *input_sharding,
                                                     ObIArray<ObRawExpr*> &src_keys,
                                                     ObIArray<ObRawExpr*> &target_keys,
                                                     EqualSets &input_esets,
                                                     ObShardingInfo *&out_sharding)
{
  int ret = OB_SUCCESS;
  out_sharding = NULL;
  ObSEArray<ObRawExpr*, 4> repart_exprs;
  ObSEArray<ObRawExpr*, 4> repart_sub_exprs;
  ObSEArray<ObRawExpr*, 4> repart_func_exprs;
  if (OB_ISNULL(get_plan()) ||
      OB_ISNULL(input_sharding)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(get_plan()->get_repartition_keys(input_esets,
                                                      src_keys,
                                                      target_keys,
                                                      input_sharding->get_partition_keys(),
                                                      repart_exprs,
                                                      true))) {
    LOG_WARN("failed to get repartition keys", K(ret));
  } else if (OB_FAIL(get_plan()->get_repartition_keys(input_esets,
                                                      src_keys,
                                                      target_keys,
                                                      input_sharding->get_sub_partition_keys(),
                                                      repart_sub_exprs,
                                                      true))) {
    LOG_WARN("failed to get sub repartition keys", K(ret));
  } else if (input_sharding->get_partition_keys().count() != repart_exprs.count()
             || input_sharding->get_sub_partition_keys().count() != repart_sub_exprs.count()) {
    out_sharding = NULL;
  } else if (OB_ISNULL(out_sharding = reinterpret_cast<ObShardingInfo*>(
                       get_plan()->get_allocator().alloc(sizeof(ObShardingInfo))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else if (OB_FALSE_IT(out_sharding = new(out_sharding) ObShardingInfo())) {
  } else if (OB_FAIL(out_sharding->copy_without_part_keys(*input_sharding))) {
    LOG_WARN("failed to assign sharding info", K(ret));
  } else {
    ObRawExprCopier copier(get_plan()->get_optimizer_context().get_expr_factory());
    if (OB_FAIL(copier.add_replaced_expr(input_sharding->get_partition_keys(), repart_exprs))) {
      LOG_WARN("failed to add replace pair", K(ret));
    } else if (OB_FAIL(copier.add_replaced_expr(input_sharding->get_sub_partition_keys(),
                                                repart_sub_exprs))) {
      LOG_WARN("failed to add replace pair", K(ret));
    } else if (OB_FAIL(copier.copy_on_replace(input_sharding->get_partition_func(),
                                              repart_func_exprs))) {
      LOG_WARN("failed to copy partition function", K(ret));
    } else if (OB_FAIL(out_sharding->get_partition_keys().assign(repart_exprs)) ||
               OB_FAIL(out_sharding->get_sub_partition_keys().assign(repart_sub_exprs)) ||
               OB_FAIL(out_sharding->get_partition_func().assign(repart_func_exprs))) {
      LOG_WARN("failed to assign partition keys", K(ret));
    }
  }
  return ret;
}

int ObLogSubPlanFilter::compute_op_parallel_and_server_info()
{
  int ret = OB_SUCCESS;
  get_server_list().reuse();
  ObLogicalOperator *child = NULL;;
  ObLogicalOperator *first_op = get_child(first_child);
  ObLogicalOperator *sub_query_op = get_child(second_child);
  if (OB_ISNULL(get_plan()) || OB_ISNULL(first_op) || OB_ISNULL(sub_query_op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null child op", K(ret), K(get_plan()), K(first_op), K(sub_query_op));
  } else if (DistAlgo::DIST_BASIC_METHOD == dist_algo_
             || DistAlgo::DIST_PULL_TO_LOCAL == dist_algo_) {
    int64_t available_parallel = ObGlobalHint::DEFAULT_PARALLEL;
    for (int64_t i = 0; i < get_num_of_child(); ++i) {
      if (OB_NOT_NULL(child = get_child(i)) && child->get_available_parallel() > available_parallel) {
        available_parallel = child->get_available_parallel();
      }
    }
    set_parallel(1);
    set_server_cnt(1);
    set_available_parallel(available_parallel);
    if (DistAlgo::DIST_BASIC_METHOD == dist_algo_ &&
        OB_FAIL(get_server_list().assign(first_op->get_server_list()))) {
      LOG_WARN("failed to assign server list", K(ret));
    } else if (DistAlgo::DIST_PULL_TO_LOCAL == dist_algo_
               && OB_FAIL(get_server_list().push_back(get_plan()->get_optimizer_context().get_local_server_addr()))) {
      LOG_WARN("failed to push back server list", K(ret));
    }
  } else if (DistAlgo::DIST_NONE_ALL == dist_algo_
             || DistAlgo::DIST_RANDOM_ALL == dist_algo_
             || DistAlgo::DIST_HASH_ALL == dist_algo_) {
    set_parallel(first_op->get_parallel());
    set_server_cnt(first_op->get_server_cnt());
    if (OB_FAIL(get_server_list().assign(first_op->get_server_list()))) {
      LOG_WARN("failed to assign server list", K(ret));
    }
  } else if (DistAlgo::DIST_PARTITION_WISE == get_distributed_algo()
             || DistAlgo::DIST_PARTITION_NONE == get_distributed_algo()) {
    int64_t parallel = ObGlobalHint::DEFAULT_PARALLEL;
    for (int64_t i = 0; i < get_num_of_child(); ++i) {
      if (OB_NOT_NULL(child = get_child(i)) && child->get_parallel() > parallel) {
        parallel = child->get_parallel();
      }
    }
    if (sub_query_op->get_part_cnt() > 0 && sub_query_op->get_part_cnt() < parallel) {
      parallel = child->get_part_cnt() < 2 ? 2 : child->get_part_cnt();
      need_re_est_child_cost_ = true;
    }
    set_parallel(parallel);
    set_server_cnt(sub_query_op->get_server_cnt());
    if (OB_FAIL(get_server_list().assign(sub_query_op->get_server_list()))) {
      LOG_WARN("failed to assign server list", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected dist algo", K(ret), K(dist_algo_));
  }
  return ret;
}

int ObLogSubPlanFilter::print_outline_data(PlanText &plan_text)
{
  int ret = OB_SUCCESS;
  ObPQSubqueryHint hint;
  const ObDMLStmt *stmt = NULL;
  ObString qb_name;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(stmt = get_plan()->get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULl", K(ret), K(get_plan()), K(stmt));
  } else if (DistAlgo::DIST_BASIC_METHOD == get_distributed_algo()) {
    /* do not print data for basic. need remove this when support split subplan filter op */
  } else if (OB_FAIL(get_sub_qb_names(hint.get_sub_qb_names()))) {
    LOG_WARN("fail to get subplan qb_names", K(ret));
  } else if (OB_FAIL(stmt->get_qb_name(qb_name))) {
    LOG_WARN("fail to get qb_name", K(ret), K(stmt->get_stmt_id()));
  } else {
    hint.set_dist_algo(get_distributed_algo());
    hint.set_qb_name(qb_name);
    if (OB_FAIL(hint.print_hint(plan_text))) {
      LOG_WARN("failed to print pq subquery hint", K(ret));
    }
  }
  return ret;
}

int ObLogSubPlanFilter::print_used_hint(PlanText &plan_text)
{
  int ret = OB_SUCCESS;
  const ObPQSubqueryHint *explicit_hint = NULL;
  const ObPQSubqueryHint *implicit_hint = NULL;
  const ObPQSubqueryHint *hint = NULL;
  ObSEArray<ObString, 4> sub_qb_names;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(get_plan()->get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULl", K(ret), K(get_plan()));
  } else if (OB_FAIL(get_sub_qb_names(sub_qb_names))) {
    LOG_WARN("fail to get subplan qb_names", K(ret));
  } else if (OB_FAIL(get_plan()->get_log_plan_hint().get_valid_pq_subquery_hint(sub_qb_names,
                                                                                explicit_hint,
                                                                                implicit_hint))) {
    LOG_WARN("fail to get valid pq subquery hint", K(ret));
  } else if (NULL == (hint = (NULL == explicit_hint
                              && sub_qb_names.count() == get_plan()->get_stmt()->get_subquery_expr_size())
                             ? implicit_hint : explicit_hint)) {
    /* do nothing */
  } else if (get_distributed_algo() != hint->get_dist_algo()) {
    /* do nothing */
  } else if (OB_FAIL(hint->print_hint(plan_text))) {
    LOG_WARN("failed to print used pq pq subquery hint", K(ret));
  }
  return ret;
}

int ObLogSubPlanFilter::get_sub_qb_names(ObIArray<ObString> &sub_qb_names)
{
  int ret = OB_SUCCESS;
  sub_qb_names.reuse();
  ObLogicalOperator *child = NULL;
  const ObDMLStmt *stmt = NULL;
  ObString qb_name;
  for (int64_t i = 1; OB_SUCC(ret) && i < get_num_of_child(); i++) {
    if (OB_ISNULL(child = get_child(i)) || OB_ISNULL(child->get_plan())
        || OB_ISNULL(stmt = child->get_plan()->get_stmt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(child), K(stmt));
    } else if (OB_FAIL(stmt->get_qb_name(qb_name))) {
      LOG_WARN("fail to get qb_name", K(ret), K(stmt->get_stmt_id()));
    } else if (OB_FAIL(sub_qb_names.push_back(qb_name))) {
      LOG_WARN("failed to push back", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObLogSubPlanFilter::check_right_is_local_scan(int64_t &local_scan_type) const
{
  int ret = OB_SUCCESS;
  local_scan_type = 2;  // 0: dist scan, 1: local das scan, 2: local scan
  const ObLogicalOperator *first_child = NULL;
  const ObLogicalOperator *child = NULL;
  bool contain_dist_das = false;
  if (OB_ISNULL(first_child = get_child(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(first_child));
  }
  for (int64_t i = 1; 0 < local_scan_type && OB_SUCC(ret) && i < get_num_of_child(); i++) {
    if (init_plan_idxs_.has_member(i) || one_time_idxs_.has_member(i)) {
      /* do nothing */
    } else if (OB_ISNULL(child = get_child(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(i));
    } else if (child->is_exchange_allocated()) {
      local_scan_type = 0;
    } else if (!child->get_contains_das_op()) {
      /* do nothing */
    } else if (1 != first_child->get_server_list().count()
               || ObShardingInfo::is_shuffled_server_list(first_child->get_server_list())) {
      local_scan_type = 0;
    } else if (OB_FAIL(child->check_contain_dist_das(first_child->get_server_list(), contain_dist_das))) {
      LOG_WARN("failed to check contain dist das", K(ret));
    } else if (contain_dist_das) {
      local_scan_type = 0;
    } else {
      local_scan_type = 1;
    }
  }
  return ret;
}

int ObLogSubPlanFilter::pre_check_spf_can_px_batch_rescan(bool &can_px_batch_rescan,
                                                          bool &rescan_contain_match_all) const
{
  int ret = OB_SUCCESS;
  can_px_batch_rescan = false;
  rescan_contain_match_all = false;
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(get_plan()));
  } else if (1 < get_available_parallel() || 1 < get_parallel()
             || get_exec_params().empty()
             || !get_plan()->get_optimizer_context().enable_px_batch_rescan()) {
    /* do nothing */
  } else {
    bool find_nested_rescan = false;
    bool find_rescan_px = false;
    bool tmp_find_nested_rescan = false;
    bool tmp_find_rescan_px = false;
    const ObLogicalOperator *child = NULL;
    for (int i = 1; !find_nested_rescan && OB_SUCC(ret) && i < get_num_of_child(); ++i) {
      tmp_find_nested_rescan = false;
      tmp_find_rescan_px = false;
      if (OB_ISNULL(child = get_child(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(i));
      } else if (one_time_idxs_.has_member(i)) {
        /* do nothing */
      } else if (OB_FAIL(child->pre_check_can_px_batch_rescan(tmp_find_nested_rescan, tmp_find_rescan_px, false))) {
        LOG_WARN("fail to pre check can px batch rescan", K(ret));
      } else {
        find_nested_rescan |= tmp_find_nested_rescan;
        find_rescan_px |= tmp_find_rescan_px;
        rescan_contain_match_all |= child->is_match_all() && child->get_contains_das_op();
      }
    }
    if (OB_SUCC(ret)) {
      can_px_batch_rescan = !find_nested_rescan && find_rescan_px;
    }
  }
  return ret;
}

int ObLogSubPlanFilter::open_px_resource_analyze(OPEN_PX_RESOURCE_ANALYZE_DECLARE_ARG)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *child = NULL;
  // prepare onetime exprs first.
  for (int64_t i = 0; i < get_num_of_child() && OB_SUCC(ret); i++) {
    if (!get_onetime_idxs().has_member(i)) {
      // do nothing if it's not onetime expr
    } else if (OB_ISNULL(child = get_child(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("child op is null", K(ret));
    } else if (OB_FAIL(SMART_CALL(child->open_px_resource_analyze(OPEN_PX_RESOURCE_ANALYZE_ARG)))) {
      LOG_WARN("open px resource analyze failed", K(ret));
    } else if (OB_FAIL(SMART_CALL(child->close_px_resource_analyze(CLOSE_PX_RESOURCE_ANALYZE_ARG)))) {
      LOG_WARN("open px resource analyze failed", K(ret));
    }
  }
  // then schedule all other children
  for (int64_t i = 0; i < get_num_of_child() && OB_SUCC(ret); i++) {
    if (get_onetime_idxs().has_member(i)) {
      // do nothing if it's onetime expr
    } else if (OB_ISNULL(child = get_child(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("child op is null", K(ret));
    } else if (OB_FAIL(SMART_CALL(child->open_px_resource_analyze(OPEN_PX_RESOURCE_ANALYZE_ARG)))) {
      LOG_WARN("open px resource analyze failed", K(ret), K(i));
    }
  }
  return ret;
}

int ObLogSubPlanFilter::close_px_resource_analyze(CLOSE_PX_RESOURCE_ANALYZE_DECLARE_ARG)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *child = NULL;
  // close all non-onetime-expr children
  for (int64_t i = 0; i < get_num_of_child() && OB_SUCC(ret); i++) {
    if (get_onetime_idxs().has_member(i)) {
      // do nothing if it's onetime expr
    } else if (OB_ISNULL(child = get_child(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("child op is null", K(ret));
    } else if (OB_FAIL(SMART_CALL(child->close_px_resource_analyze(CLOSE_PX_RESOURCE_ANALYZE_ARG)))) {
      LOG_WARN("open px resource analyze failed", K(ret));
    }
  }
  return ret;
}

bool ObLogSubPlanFilter::is_px_batch_rescan_enabled()
{
  bool enable_px_batch_rescan = false;
  if (!enable_px_batch_rescans_.empty()) {
    for (int i = 0; i < enable_px_batch_rescans_.count() && !enable_px_batch_rescan; i++) {
      enable_px_batch_rescan = enable_px_batch_rescans_.at(i);
    }
  }
  return enable_px_batch_rescan;
}