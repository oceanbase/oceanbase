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
  } else if (OB_FAIL(BUF_PRINTF("use_batch=%s",
            enable_das_group_rescan_ ? "true" : "false"))) {
    LOG_WARN("BUF_PRINTF fails", K(ret));
  } else { /* Do nothing */ }
  END_BUF_PRINT(plan_item.special_predicates_,
                plan_item.special_predicates_len_);
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
      || OB_UNLIKELY(param.need_parallel_ < 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected params", K(ret), K(get_plan()), K(child), K(param.need_parallel_));
  } else if (param.need_row_count_ < 0 || param.need_row_count_ >= child->get_card()) {
    param.need_row_count_ = -1;
  } else if (OB_FALSE_IT(get_plan()->get_selectivity_ctx().init_op_ctx(&child->get_output_equal_sets(), child->get_card()))) {
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
    if (OB_FAIL(ObOptEstCost::cost_subplan_filter(info, op_cost, opt_ctx.get_cost_model_type()))) {
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
  for (int64_t i = 0; OB_SUCC(ret) && i < get_num_of_child(); ++i) {
    const ObLogicalOperator *child = get_child(i);
    cur_param.reset();
    if (OB_ISNULL(child)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("set operator i-th child is null", K(ret), K(i));
    } else if (OB_FAIL(cur_param.assign(param))) {
      LOG_WARN("failed to assign param", K(ret));
    } else if (0 != i && OB_FALSE_IT(cur_param.need_row_count_ = -1)) {
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
      inherit_sharding_index_ = 0;
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

int ObLogSubPlanFilter::check_if_match_das_group_rescan(ObLogicalOperator *root,
                                                        bool &group_rescan)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(root)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (root->is_table_scan()) {
    bool is_valid = false;
    ObLogTableScan *tsc = NULL;
    ObLogPlan *plan = NULL;
    const AccessPath *ap = NULL;
    const TableItem *table_item = NULL;
    if (OB_ISNULL(tsc = static_cast<ObLogTableScan*>(root))
        // tsc might belong to a different subquery
        // with its own plan
        || OB_ISNULL(plan = tsc->get_plan())
        || OB_ISNULL(ap = tsc->get_access_path())
        || OB_ISNULL(table_item = plan->get_stmt()->get_table_item_by_id(ap->table_id_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else if (!tsc->use_das()) {
      group_rescan = false;
    }
    if (OB_SUCC(ret) && group_rescan) {
      group_rescan = !(is_virtual_table(ap->ref_table_id_)
                       || table_item->is_link_table()
                       || ap->is_cte_path()
                       || ap->is_function_table_path()
                       || ap->is_temp_table_path()
                       || ap->is_json_table_path()
                       || table_item->for_update_
                       || !ap->subquery_exprs_.empty()
                       || EXTERNAL_TABLE == table_item->table_type_
                       );
    }
    if (OB_SUCC(ret) && group_rescan) {
      if (OB_FAIL(ObOptimizerUtil::check_contribute_query_range(root,
                                                                get_exec_params(),
                                                                is_valid))) {
        LOG_WARN("failed to check query range contribution", K(ret));
      } else if (!is_valid) {
        group_rescan = false;
      } else if (tsc->get_scan_direction() != default_asc_direction()) {
        group_rescan = false;
      } else if (tsc->has_index_scan_filter() && tsc->get_index_back() && tsc->get_is_index_global()) {
        // For the global index lookup, if there is a pushdown filter when scanning the index,
        // batch cannot be used.
        group_rescan = false;
      } else {/*do nothing*/}
    }
  } else if (log_op_def::LOG_SUBPLAN_SCAN == root->get_type()) {
    if (1 != root->get_num_of_child()) {
      group_rescan = false;
    } else if (OB_ISNULL(root->get_child(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else if (!root->get_child(0)->is_table_scan()) {
      group_rescan = false;
    } else if (OB_FAIL(SMART_CALL(check_if_match_das_group_rescan(root->get_child(0),
                                                                  group_rescan)))) {
      LOG_WARN("failed to check match das batch rescan", K(ret));
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
      ts->set_use_batch(enable_das_group_rescan_);
    }
  } else if (root->get_num_of_child() == 1) {
    if(OB_FAIL(SMART_CALL(set_use_das_batch(root->get_child(first_child))))) {
      LOG_WARN("failed to check use das batch", K(ret));
    }
  } else { /*do nothing*/ }
  return ret;
}

int ObLogSubPlanFilter::check_and_set_das_group_rescan()
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session_info = NULL;
  ObLogPlan *plan = NULL;
  if (OB_ISNULL(plan = get_plan())
      || OB_ISNULL(session_info = plan->get_optimizer_context().get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (OB_FAIL(session_info->get_nlj_batching_enabled(enable_das_group_rescan_))) {
    LOG_WARN("failed to get enable batch variable", K(ret));
  }
  // check use batch
  for (int64_t i = 1; OB_SUCC(ret) && enable_das_group_rescan_ && i < get_num_of_child(); i++) {
    ObLogicalOperator *child = get_child(i);
    bool contains_invalid_startup = false;
    if (OB_ISNULL(child)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else if (get_initplan_idxs().has_member(i) || get_onetime_idxs().has_member(i)) {
      enable_das_group_rescan_ = false;
    } else if (!(child->get_type() == log_op_def::LOG_TABLE_SCAN
                 || child->get_type() == log_op_def::LOG_SUBPLAN_SCAN)) {
      enable_das_group_rescan_ = false;
    } else if (OB_FAIL(check_if_match_das_group_rescan(child, enable_das_group_rescan_))) {
      LOG_WARN("failed to check match das batch rescan", K(ret));
    } else if (enable_das_group_rescan_) {
      if (OB_FAIL(plan->contains_startup_with_exec_param(child, contains_invalid_startup))) {
        LOG_WARN("failed to check contains invalid startup", K(ret));
      } else if (contains_invalid_startup) {
        enable_das_group_rescan_ = false;
      }
    }
  }
  // check if exec params contain sub_query
  for (int64_t i = 0; OB_SUCC(ret) && enable_das_group_rescan_ && i < exec_params_.count(); i++) {
    const ObExecParamRawExpr *exec_param = exec_params_.at(i);
    if (OB_ISNULL(exec_param)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("exec param is nullptr", K(ret), K(i));
    } else if (OB_NOT_NULL(exec_param->get_ref_expr()) && exec_param->get_ref_expr()->has_flag(CNT_SUB_QUERY)) {
      enable_das_group_rescan_ = false;
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
  LOG_TRACE("spf das batch rescan", K(ret), K(enable_das_group_rescan_));
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
  } else if (OB_ISNULL(out_sharding = reinterpret_cast<ObShardingInfo*>(
                       get_plan()->get_allocator().alloc(sizeof(ObShardingInfo))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else if (OB_FALSE_IT(out_sharding = new(out_sharding) ObShardingInfo())) {
  } else if (OB_FAIL(out_sharding->copy_without_part_keys(*input_sharding))) {
    LOG_WARN("failed to assign sharding info", K(ret));
  } else {
    ObRawExprCopier copier(get_plan()->get_optimizer_context().get_expr_factory());
    if (OB_FAIL(get_plan()->get_repartition_keys(input_esets,
                                                 src_keys,
                                                 target_keys,
                                                 input_sharding->get_partition_keys(),
                                                 repart_exprs))) {
      LOG_WARN("failed to get repartition keys", K(ret));
    } else if (OB_FAIL(get_plan()->get_repartition_keys(input_esets,
                                                        src_keys,
                                                        target_keys,
                                                        input_sharding->get_sub_partition_keys(),
                                                        repart_sub_exprs))) {
      LOG_WARN("failed to get sub repartition keys", K(ret));
    } else if (OB_FAIL(copier.add_replaced_expr(input_sharding->get_partition_keys(),
                                                repart_exprs))) {
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
  bool is_partition_wise = DistAlgo::DIST_PARTITION_WISE == get_distributed_algo();
  if (OB_FAIL(compute_normal_multi_child_parallel_and_server_info(is_partition_wise))) {
    LOG_WARN("failed to compute multi child parallel and server info", K(ret), K(get_distributed_algo()));
  }
  return ret;
}
