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
#include "ob_log_limit.h"
#include "ob_log_group_by.h"
#include "ob_log_operator_factory.h"
#include "ob_log_sort.h"
#include "ob_log_table_scan.h"
#include "ob_optimizer_util.h"
#include "ob_opt_est_cost.h"
#include "ob_log_exchange.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/optimizer/ob_join_order.h"
#include "common/ob_smart_call.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::sql::log_op_def;

int ObLogLimit::get_op_exprs(ObIArray<ObRawExpr*> &all_exprs)
{
  int ret = OB_SUCCESS;
  //支持with ties功能，如果存在order by item,需要添加对应的order by item
  ObSEArray<ObOrderDirection, 6> order_directions;
  if (is_fetch_with_ties() && OB_FAIL(ObOptimizerUtil::split_expr_direction(
                                                       order_items_,
                                                       all_exprs,
                                                       order_directions))) {
    LOG_WARN("failed to split expr and direction", K(ret));
  } else if (NULL != limit_expr_ && OB_FAIL(all_exprs.push_back(limit_expr_))) {
    LOG_WARN("failed to push back expr", K(ret));
  } else if (NULL != offset_expr_ && OB_FAIL(all_exprs.push_back(offset_expr_))) {
    LOG_WARN("failed to push back expr", K(ret));
  } else if (NULL != percent_expr_ && OB_FAIL(all_exprs.push_back(percent_expr_))) {
    LOG_WARN("failed to push back expr", K(ret));
  } else if (OB_FAIL(ObLogicalOperator::get_op_exprs(all_exprs))) {
    LOG_WARN("failed to push back expr", K(ret));
  } else {/*do nothing*/ }
  return ret;
}

int ObLogLimit::est_cost()
{
  int ret = OB_SUCCESS;
  double child_cost = 0.0;
  ObLogicalOperator *child = NULL;
  if (OB_ISNULL(child = get_child(first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(get_stmt()), K(child), K(ret));
  } else if (OB_FALSE_IT(child_cost = child->get_cost())) {
  } else if (OB_FAIL(inner_est_cost(child->get_card(), child_cost, true, card_, op_cost_))) {
    LOG_WARN("failed to est cost", K(ret));
  } else {
    cost_ = op_cost_ + child_cost;
  }
  LOG_TRACE("succeed to estimate limit-k cost", K(card_),
      K(op_cost_), K(cost_));
  return ret;
}

int ObLogLimit::re_est_cost(EstimateCostInfo &param, double &card, double &cost)
{
  int ret = OB_SUCCESS;
  int64_t offset_count = 0;
  double limit_percent = 0.0;
  int64_t limit_count = 0;
  bool is_null_value = false;
  ObLogicalOperator *child = NULL;
  if (OB_ISNULL(child = get_child(ObLogicalOperator::first_child)) ||
      OB_ISNULL(get_stmt()) || OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(child), K(ret));
  } else if (limit_expr_ != NULL && limit_expr_->has_flag(CNT_DYNAMIC_PARAM)) {
    //limit ? do nothing
  } else if (NULL != percent_expr_ &&
            OB_FAIL(ObTransformUtils::get_percentage_value(percent_expr_,
                                                            get_stmt(),
                                                            get_plan()->get_optimizer_context().get_params(),
                                                            get_plan()->get_optimizer_context().get_exec_ctx(),
                                                            &get_plan()->get_optimizer_context().get_allocator(),
                                                            limit_percent,
                                                            is_null_value))) {
    LOG_WARN("failed to get limit value", K(ret));
  } else if (!is_null_value && limit_expr_ != NULL &&
             OB_FAIL(ObTransformUtils::get_limit_value(limit_expr_,
                                                       get_plan()->get_optimizer_context().get_params(),
                                                       get_plan()->get_optimizer_context().get_exec_ctx(),
                                                       &get_plan()->get_optimizer_context().get_allocator(),
                                                       limit_count,
                                                       is_null_value))) {
    LOG_WARN("failed to get limit value", K(ret));
  } else if (!is_null_value && offset_expr_ != NULL &&
            OB_FAIL(ObTransformUtils::get_limit_value(offset_expr_,
                                                      get_plan()->get_optimizer_context().get_params(),
                                                      get_plan()->get_optimizer_context().get_exec_ctx(),
                                                      &get_plan()->get_optimizer_context().get_allocator(),
                                                      offset_count,
                                                      is_null_value))) {
    LOG_WARN("failed to get limit value", K(ret));
  } else {
    double child_card = child->get_card();
    double child_cost = child->get_cost();
    double op_cost = 0.0;
    if (param.need_row_count_ >= 0 && param.need_row_count_ <= card) {
      if (is_null_value) {
        param.need_row_count_ = 0;
      } else if (NULL != percent_expr_) {
        param.need_row_count_ = param.need_row_count_ * 100 / limit_percent;
      } else if (NULL != offset_expr_) {
        param.need_row_count_ += offset_count;
      }
    } else {
      if (is_null_value) {
        param.need_row_count_ = 0;
      } else if (NULL != percent_expr_) {
        param.need_row_count_ = child_card * limit_percent / 100;
      } else if (NULL == limit_expr_) {
        param.need_row_count_ = child_card;
      } else {
        param.need_row_count_ = static_cast<double>(limit_count + offset_count);
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(SMART_CALL(child->re_est_cost(param, child_card, child_cost)))) {
      LOG_WARN("failed to est child cost", K(ret));
    } else if (OB_FAIL(inner_est_cost(child_card, child_cost, false, card, op_cost))) {
      LOG_WARN("failed to est count cost", K(ret));
    } else {
      if (param.override_) {
        cost = op_cost + child_cost;
        set_op_cost(op_cost);
        set_cost(cost);
        set_card(card);
      }
    }
  }
  return ret;
}

int ObLogLimit::inner_est_cost(double child_card, 
                               double &child_cost,
                               bool need_re_est_child_cost, 
                               double &card, 
                               double &op_cost)
{
  int ret = OB_SUCCESS;
  int64_t parallel = 0;
  int64_t limit_count = -1;
  int64_t offset_count = 0;
  double limit_percent = 0.0;
  bool is_null_value = false;
  ObLogicalOperator *child = NULL;
  double re_estimate_card = 0.0;
  card = 0.0;
  if (OB_ISNULL(get_stmt()) || OB_ISNULL(get_plan()) ||
      OB_ISNULL(child = get_child(first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(get_stmt()), K(get_plan()),
        K(child), K(ret));
  } else if (OB_UNLIKELY((parallel = get_parallel()) < 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected parallel degree", K(ret));
  } else if (limit_expr_ != NULL && limit_expr_->has_flag(CNT_DYNAMIC_PARAM)) {
    //limit ? do nothing
  } else if (NULL != percent_expr_ &&
             OB_FAIL(ObTransformUtils::get_percentage_value(percent_expr_,
                                                            get_stmt(),
                                                            get_plan()->get_optimizer_context().get_params(),
                                                            get_plan()->get_optimizer_context().get_exec_ctx(),
                                                            &get_plan()->get_optimizer_context().get_allocator(),
                                                            limit_percent,
                                                            is_null_value))) {
    LOG_WARN("failed to get limit value", K(ret));
  } else if (!is_null_value && limit_expr_ != NULL &&
             OB_FAIL(ObTransformUtils::get_limit_value(limit_expr_,
                                                       get_plan()->get_optimizer_context().get_params(),
                                                       get_plan()->get_optimizer_context().get_exec_ctx(),
                                                       &get_plan()->get_optimizer_context().get_allocator(),
                                                       limit_count,
                                                       is_null_value))) {
    LOG_WARN("failed to get limit value", K(ret));
  } else if (!is_null_value && offset_expr_ != NULL &&
             OB_FAIL(ObTransformUtils::get_limit_value(offset_expr_,
                                                       get_plan()->get_optimizer_context().get_params(),
                                                       get_plan()->get_optimizer_context().get_exec_ctx(),
                                                       &get_plan()->get_optimizer_context().get_allocator(),
                                                       offset_count,
                                                       is_null_value))) {
    LOG_WARN("failed to get limit value", K(ret));
  } else if (is_null_value) {
    re_estimate_card = 0;
    card = 0;
  } else if (NULL != percent_expr_) {
    re_estimate_card = child_card * limit_percent / 100;
    card = re_estimate_card - offset_count;
  } else if (NULL == limit_expr_) {
    re_estimate_card = child_card;
    card = child_card - offset_count;
  } else {
    re_estimate_card = static_cast<double>(limit_count + offset_count) * parallel;
    card = std::min(static_cast<double>(limit_count) * parallel, child_card - offset_count);
  }
  if (OB_SUCC(ret)) {
    ObOptimizerContext &opt_ctx = get_plan()->get_optimizer_context();
    re_estimate_card = std::min(re_estimate_card, child_card);
    card = std::max(card, 0.0);
    if (!is_calc_found_rows_ && NULL == percent_expr_ && need_re_est_child_cost) {
      EstimateCostInfo param;
      param.need_row_count_ = re_estimate_card;
      param.override_ = false;
      if (OB_FAIL(child->re_est_cost(param, child_card, child_cost))) {
        LOG_WARN("failed to re-est cost", K(ret));
      } else {
        op_cost = ObOptEstCost::cost_get_rows(re_estimate_card, opt_ctx.get_cost_model_type());
      }
    } else {
      op_cost = ObOptEstCost::cost_get_rows(child_card, opt_ctx.get_cost_model_type());
    }
  }
  return ret;
}

int ObLogLimit::check_output_dep_specific(ObRawExprCheckDep &checker)
{
  int ret = OB_SUCCESS;

  if (NULL != limit_expr_) {
    if (OB_FAIL(checker.check(*limit_expr_))) {
      LOG_WARN("failed to check limit_count_", K(*limit_expr_), K(ret));
    }
  } else { /* Do nothing */ }

  if (OB_SUCC(ret)) {
    if (NULL != offset_expr_) {
      if (OB_FAIL(checker.check(*offset_expr_))) {
        LOG_WARN("failed to check limit_offset_", K(offset_expr_), K(ret));
      }
    } else { /* Do nothing */ }
  }

  if (OB_SUCC(ret)) {
    if (NULL != percent_expr_) {
      if (OB_FAIL(checker.check(*percent_expr_))) {
        LOG_WARN("failed to check limit_percent_", K(percent_expr_), K(ret));
      }
    }
  }

  if (OB_SUCC(ret) && is_fetch_with_ties()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < order_items_.count(); ++i) {
      ObRawExpr *cur_expr = order_items_.at(i).expr_;
      if (OB_ISNULL(cur_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null ptr", K(cur_expr), K(ret));
      } else if (OB_FAIL(checker.check(*cur_expr))) {
        LOG_WARN("failed to check expr", K(cur_expr), K(ret));
      } else {/*do nothing*/}
    }
  }
  return ret;
}

int ObLogLimit::get_plan_item_info(PlanText &plan_text,
                                   ObSqlPlanItem &plan_item)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObLogicalOperator::get_plan_item_info(plan_text, plan_item))) {
    LOG_WARN("failed to get plan item info", K(ret));
  } else if (NULL != limit_expr_ || NULL != offset_expr_ ||  NULL != percent_expr_) {
    BEGIN_BUF_PRINT;
    ObRawExpr *limit = limit_expr_;
    ObRawExpr *offset = offset_expr_;
    ObRawExpr *percent = percent_expr_;
    EXPLAIN_PRINT_EXPR(limit, type);
    ret = BUF_PRINTF(", ");
    EXPLAIN_PRINT_EXPR(offset, type);
    if (percent_expr_ != NULL) {
      BUF_PRINTF(", ");
      EXPLAIN_PRINT_EXPR(percent, type);
    }
    if (is_fetch_with_ties_) {
      BUF_PRINTF(", ");
      BUF_PRINTF("with_ties(%s)", "true");
    }
    END_BUF_PRINT(plan_item.special_predicates_,
                  plan_item.special_predicates_len_);
  }
  return ret;
}

int ObLogLimit::inner_replace_op_exprs(
    const common::ObIArray<std::pair<ObRawExpr *, ObRawExpr*>> &to_replace_exprs)
{
  int ret = OB_SUCCESS;
  if (NULL != limit_expr_ && OB_FAIL(replace_expr_action(to_replace_exprs, limit_expr_))) {
    LOG_WARN("failed to replace limit expr", K(ret));
  } else if (NULL != offset_expr_ && OB_FAIL(replace_expr_action(to_replace_exprs, offset_expr_))) {
    LOG_WARN("failed to replace offset expr", K(ret));
  } else if (NULL != percent_expr_ && OB_FAIL(replace_expr_action(to_replace_exprs, percent_expr_))) {
    LOG_WARN("failed to replace percent expr", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < order_items_.count(); ++i) {
    if (OB_ISNULL(order_items_.at(i).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null ", K(ret));
    } else if (OB_FAIL(replace_expr_action(to_replace_exprs, order_items_.at(i).expr_))) {
      LOG_WARN("failed to adjust order expr with onetime", K(ret));
    }
  }
  return ret;
}
