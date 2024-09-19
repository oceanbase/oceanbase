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

#include "sql/optimizer/ob_log_statistics_collector.h"
#include "ob_log_operator_factory.h"
#include "sql/optimizer/ob_opt_est_cost.h"
#include "sql/optimizer/ob_join_order.h"
#include "common/ob_smart_call.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

ERRSIM_POINT_DEF(EN_STATISTICS_COLLECTOR_ROW_THRESHOLD);
ObLogStatisticsCollector::ObLogStatisticsCollector(ObLogPlan &plan)
    : ObLogicalOperator(plan),
      adaptive_threshold_(0)
{
  if (OB_SUCCESS != EN_STATISTICS_COLLECTOR_ROW_THRESHOLD) {
    adaptive_threshold_ = abs(EN_STATISTICS_COLLECTOR_ROW_THRESHOLD);
    LOG_INFO("tracepoint adaptive_threshold_", K(adaptive_threshold_));
  }
}

const char *ObLogStatisticsCollector::get_name() const
{
  static const char *name[1] =
  {
    "STATISTICS COLLECTOR",
  };
  return name[0];
}

int ObLogStatisticsCollector::get_plan_item_info(PlanText &plan_text, ObSqlPlanItem &plan_item)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObLogicalOperator::get_plan_item_info(plan_text, plan_item))) {
    LOG_WARN("failed to get plan item info", K(ret));
  } else {
    BEGIN_BUF_PRINT;
    ret = BUF_PRINTF("threshold: %ld", adaptive_threshold_);
    END_BUF_PRINT(plan_item.special_predicates_,
                  plan_item.special_predicates_len_);
  }
  return ret;
}

int ObLogStatisticsCollector::est_cost()
{
  int ret = OB_SUCCESS;
  int64_t parallel = 0;
  ObLogicalOperator *child = get_child(ObLogicalOperator::first_child);
  if (OB_ISNULL(get_plan()) ||
      OB_ISNULL(child)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_UNLIKELY((parallel = get_parallel()) < 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected parallel degree", K(parallel), K(ret));
  } else {
    double op_cost = 0.0;
    ObOptimizerContext &opt_ctx = get_plan()->get_optimizer_context();
    op_cost += ObOptEstCost::cost_material(adaptive_threshold_ / parallel,
                                           child->get_width(),
                                           opt_ctx);
    set_op_cost(op_cost);
    set_cost(child->get_cost() + op_cost);
    set_card(child->get_card());
  }
  return ret;
}

int ObLogStatisticsCollector::do_re_est_cost(EstimateCostInfo &param, double &card, double &op_cost, double &cost)
{
  int ret = OB_SUCCESS;
  const int64_t parallel = param.need_parallel_;
  double child_card = 0.0;
  double child_cost = 0.0;
  ObLogicalOperator *child = get_child(ObLogicalOperator::first_child);
  if (OB_ISNULL(get_plan()) ||
      OB_ISNULL(child)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_UNLIKELY(parallel < 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected parallel degree", K(parallel), K(ret));
  } else if (OB_FALSE_IT(param.need_row_count_ = -1)) {
  } else if (OB_FAIL(SMART_CALL(child->re_est_cost(param, child_card, child_cost)))) {
    LOG_WARN("failed to re est cost", K(ret));
  } else {
    ObOptimizerContext &opt_ctx = get_plan()->get_optimizer_context();
    op_cost = ObOptEstCost::cost_material(adaptive_threshold_ / parallel,
                                          child->get_width(),
                                          opt_ctx);
    cost = child_cost + op_cost;
    card = child_card;
  }
  return ret;
}

void ObLogStatisticsCollector::set_adaptive_threshold(int64_t threshold)
{
  if (OB_LIKELY(OB_SUCCESS == EN_STATISTICS_COLLECTOR_ROW_THRESHOLD)) {
    adaptive_threshold_ = threshold;
  }
}