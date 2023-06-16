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
  double card = 0.0;
  double op_cost = 0.0;
  double cost = 0.0;
  EstimateCostInfo param;
  param.need_parallel_ = get_parallel();
  if (OB_FAIL(do_re_est_cost(param, card_, op_cost_, cost_))) {
    LOG_WARN("failed to est cost", K(ret));
  } else {
    LOG_TRACE("succeed to estimate limit-k cost", K(card_), K(op_cost_), K(cost_));
  }
  return ret;
}

int ObLogLimit::do_re_est_cost(EstimateCostInfo &param, double &card, double &op_cost, double &cost)
{
  int ret = OB_SUCCESS;
  card = 0.0;
  op_cost = 0.0;
  cost = 0.0;
  int64_t limit_count = -1;
  int64_t offset_count = 0;
  double limit_percent = -1;
  ObLogicalOperator *child = NULL;
  const int64_t parallel = param.need_parallel_;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(child = get_child(first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(get_plan()), K(child), K(ret));
  } else if (OB_UNLIKELY((parallel > 1 && (NULL != percent_expr_ || NULL != offset_expr_))
                         || parallel < 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected parallel degree", K(ret), K(param), K(percent_expr_), K(offset_expr_));
  } else if (!is_calc_found_rows_ &&
             OB_FAIL(get_limit_offset_value(percent_expr_, limit_expr_, offset_expr_,
                                            limit_percent, limit_count, offset_count))) {
    LOG_WARN("failed to get limit offset value", K(ret));
  } else {
    card = 0 > param.need_row_count_ ? child->get_card() : param.need_row_count_;
    if (0 <= limit_count) {
      if (parallel > 1) {
        card = std::min(card, child->get_card());
        card = std::min(card, static_cast<double>(limit_count * parallel));
      } else {
        card = std::min(card, child->get_card() - offset_count);
        card = std::min(card, static_cast<double>(limit_count));
      }
      card = std::max(0.0, card);
      param.need_row_count_ = std::min(card + offset_count, child->get_card());
    } else if (0 <= limit_percent) {
      // reset need_row_count_ when exists limit_percent
      card = std::min(card, child->get_card());
      param.need_row_count_ = -1;
    } else {
      // 1. exists dynamic param in limit/offset or is_calc_found_rows_
      // 2. has offset without limit / percent
      card = std::min(card, child->get_card() - offset_count);
      card = std::max(0.0, card);
      param.need_row_count_ = std::min(card + offset_count, child->get_card());
    }

    if (OB_SUCC(ret)) {
      double child_card = 0.0;
      double child_cost = 0.0;
      ObOptimizerContext &opt_ctx = get_plan()->get_optimizer_context();
      if (OB_FAIL(child->re_est_cost(param, child_card, child_cost))) {
        LOG_WARN("failed to re-est cost", K(ret));
      } else {
        op_cost = ObOptEstCost::cost_get_rows(child_card, opt_ctx.get_cost_model_type());
        cost = op_cost + child_cost;
        child_card = 0 <= limit_percent
                     ? std::max(child_card * limit_percent / 100 - offset_count, 0.0)
                     : std::max(child_card - offset_count, 0.0);
        child_card = std::max(0.0, child_card);
        card = std::min(card, child_card);
      }
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

int ObLogLimit::inner_replace_op_exprs(ObRawExprReplacer &replacer)
{
  int ret = OB_SUCCESS;
  if (NULL != limit_expr_ && OB_FAIL(replace_expr_action(replacer, limit_expr_))) {
    LOG_WARN("failed to replace limit expr", K(ret));
  } else if (NULL != offset_expr_ && OB_FAIL(replace_expr_action(replacer, offset_expr_))) {
    LOG_WARN("failed to replace offset expr", K(ret));
  } else if (NULL != percent_expr_ && OB_FAIL(replace_expr_action(replacer, percent_expr_))) {
    LOG_WARN("failed to replace percent expr", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < order_items_.count(); ++i) {
    if (OB_ISNULL(order_items_.at(i).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null ", K(ret));
    } else if (OB_FAIL(replace_expr_action(replacer, order_items_.at(i).expr_))) {
      LOG_WARN("failed to adjust order expr with onetime", K(ret));
    }
  }
  return ret;
}
