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
using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::sql::log_op_def;

int ObLogLimit::copy_without_child(ObLogicalOperator*& out)
{
  int ret = OB_SUCCESS;
  out = NULL;
  ObLogicalOperator* op = NULL;
  ObLogLimit* limit = NULL;
  if (OB_FAIL(clone(op))) {
    LOG_WARN("failed to clone ObLogLimit", K(ret));
  } else if (OB_ISNULL(limit = static_cast<ObLogLimit*>(op))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to cast ObLogicalOperator * to ObLogLimit *", K(ret));
  } else {
    limit->set_calc_found_rows(is_calc_found_rows_);
    limit->set_has_union_child(has_union_child_);
    limit->set_fetch_with_ties(is_fetch_with_ties());
    limit->set_limit_count(limit_count_);
    limit->set_limit_offset(limit_offset_);
    limit->set_limit_percent(limit_percent_);
    out = limit;
  }
  return ret;
}

int ObLogLimit::allocate_exchange_post(AllocExchContext* ctx)
{
  int ret = OB_SUCCESS;
  bool is_basic = false;
  bool should_push_limit = (!is_calc_found_rows_ && (limit_percent_ == NULL));
  ObLogicalOperator* exchange_point = NULL;
  ObExchangeInfo exch_info;
  if (OB_ISNULL(ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ctx), K(ret));
  } else if (OB_FAIL(compute_basic_sharding_info(ctx, is_basic))) {
    LOG_WARN("failed to compute basic sharding info", K(ret));
  } else if (is_basic) {
    LOG_TRACE("is basic sharding info", K(sharding_info_));
  } else if (OB_FAIL(push_down_limit(
                 ctx, limit_count_, limit_offset_, should_push_limit, is_fetch_with_ties(), exchange_point))) {
    LOG_WARN("failed to push down limit", K(ret));
  } else if (OB_ISNULL(exchange_point)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(exchange_point), K(ret));
  } else if (OB_FAIL(exchange_point->allocate_exchange_nodes_below(first_child, *ctx, exch_info))) {
    LOG_WARN("failed to allocate exchange nodes below", K(ret));
  } else {
    sharding_info_.set_location_type(OB_TBL_LOCATION_LOCAL);
  }
  return ret;
}

int ObLogLimit::transmit_op_ordering()
{
  int ret = OB_SUCCESS;
  bool need_sort = false;
  if (OB_FAIL(check_need_sort_below_node(0, expected_ordering_, need_sort))) {
    LOG_WARN("failed to check need sort", K(ret));
  } else if (!need_sort) {
    // do nothing
  } else if (OB_FAIL(allocate_sort_below(0, expected_ordering_))) {
    LOG_WARN("failed to allocate sort", K(ret));
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObLogicalOperator::transmit_op_ordering())) {
      LOG_WARN("failed to transmit op ordering", K(ret));
    }
  }
  return ret;
}

int ObLogLimit::allocate_expr_pre(ObAllocExprContext& ctx)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 6> order_exprs;
  ObSEArray<ObOrderDirection, 6> order_directions;
  if (is_fetch_with_ties() &&
      OB_FAIL(ObOptimizerUtil::split_expr_direction(get_expected_ordering(), order_exprs, order_directions))) {
    LOG_WARN("failed to split expr and direction", K(ret));
  } else if (OB_FAIL(add_exprs_to_ctx(ctx, order_exprs))) {
    LOG_WARN("fail to add expr to ctx", K(ret));
  } else if (OB_FAIL(ObLogicalOperator::allocate_expr_pre(ctx))) {
    LOG_WARN("failed to add parent need expr", K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

int ObLogLimit::est_cost()
{
  int ret = OB_SUCCESS;
  int64_t limit_count = -1;
  int64_t offset_count = 0;
  double percentage = 0.0;
  bool is_null_value = false;
  if (OB_ISNULL(get_child(first_child)) || OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Limit's child not expected to be NULL", K(ret));
  } else if (limit_count_ != NULL && OB_FAIL(ObTransformUtils::get_limit_value(limit_count_,
                                         get_plan()->get_stmt(),
                                         get_plan()->get_optimizer_context().get_params(),
                                         get_plan()->get_optimizer_context().get_session_info(),
                                         &get_plan()->get_optimizer_context().get_allocator(),
                                         limit_count,
                                         is_null_value))) {
    LOG_WARN("Get limit count num error", K(ret));
  } else if (!is_null_value && limit_offset_ != NULL &&
             OB_FAIL(ObTransformUtils::get_limit_value(limit_offset_,
                 get_plan()->get_stmt(),
                 get_plan()->get_optimizer_context().get_params(),
                 get_plan()->get_optimizer_context().get_session_info(),
                 &get_plan()->get_optimizer_context().get_allocator(),
                 offset_count,
                 is_null_value))) {
    LOG_WARN("Get limit offset num error", K(ret));
  } else if (!is_null_value && limit_percent_ != NULL &&
             OB_FAIL(get_percentage_value(limit_percent_, percentage, is_null_value))) {
    LOG_WARN("failed to get percentage value", K(ret));
  } else {
    double child_card = get_child(first_child)->get_card();
    double limit_count_double = 0.0;
    double offset_count_double = 0.0;
    double scan_count_double = 0.0;
    if (limit_count_ != NULL || limit_percent_ != NULL) {
      limit_count = is_null_value ? 0 : limit_count;
      offset_count = is_null_value ? 0 : offset_count;
      if (limit_percent_ != NULL) {
        limit_count_double = child_card * percentage / 100.0;
        offset_count_double = static_cast<double>(offset_count);
      } else {
        limit_count_double = static_cast<double>(limit_count);
        offset_count_double = static_cast<double>(offset_count);
      }
      scan_count_double = limit_count_double + offset_count_double;
      if (scan_count_double <= child_card) {
        set_card(limit_count_double);
      } else {
        limit_count_double = child_card - offset_count_double;
        set_card(limit_count_double >= 0.0 ? limit_count_double : 0.0);
      }
      if (!is_calc_found_rows_ && NULL == limit_percent_) {
        bool re_est = false;
        if (OB_FAIL(get_child(first_child)->re_est_cost(this, scan_count_double, re_est))) {
          LOG_WARN("Failed to re est cost", K(ret));
        }
      }
    } else {
      offset_count_double = static_cast<double>(offset_count);
      if (OB_UNLIKELY(offset_count_double < 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("offset count double should greater than 0", K(ret), K(offset_count_double));
      } else {
        limit_count_double = is_null_value ? 0 : child_card - offset_count_double;
        set_card(limit_count_double >= 0.0 ? limit_count_double : 0.0);
      }
    }
    if (OB_SUCC(ret)) {
      double op_cost = ObOptEstCost::cost_limit(get_child(first_child)->get_card());
      set_op_cost(op_cost);
      set_cost(get_child(first_child)->get_cost() + op_cost);
      set_width(get_child(first_child)->get_width());
    }
  }
  return ret;
}

int ObLogLimit::re_est_cost(const ObLogicalOperator* parent, double need_row_count, bool& re_est)
{
  int ret = OB_SUCCESS;
  UNUSED(parent);
  re_est = false;
  if (OB_UNLIKELY(need_row_count < 0.0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Need row count should not less than 0", K(ret));
  } else if (need_row_count < get_card()) {
    card_ = need_row_count;
    int64_t offset_count = 0;
    ObLogicalOperator* child = NULL;
    bool is_null_value = false;
    if (OB_ISNULL(child = get_child(first_child)) || OB_ISNULL(get_plan())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(child));
    } else if (OB_FAIL(ObTransformUtils::get_limit_value(limit_offset_,
                   get_plan()->get_stmt(),
                   get_plan()->get_optimizer_context().get_params(),
                   get_plan()->get_optimizer_context().get_session_info(),
                   &get_plan()->get_optimizer_context().get_allocator(),
                   offset_count,
                   is_null_value))) {
      LOG_WARN("Get limit offset num error", K(ret));
    } else if (OB_FAIL(child->re_est_cost(this, static_cast<double>(offset_count) + need_row_count, re_est))) {
      LOG_WARN("Failed to re_est child cost", K(ret));
    } else {
      re_est = true;
      double op_cost = ObOptEstCost::cost_limit(child->get_card());
      set_op_cost(op_cost);
      set_cost(child->get_cost() + op_cost);
    }
  }
  return ret;
}

uint64_t ObLogLimit::hash(uint64_t seed) const
{
  uint64_t hash_value = seed;
  hash_value = do_hash(is_calc_found_rows_, hash_value);
  hash_value = do_hash(has_union_child_, hash_value);
  hash_value = do_hash(is_top_limit_, hash_value);
  hash_value = ObOptimizerUtil::hash_expr(limit_count_, hash_value);
  hash_value = ObOptimizerUtil::hash_expr(limit_offset_, hash_value);
  hash_value = ObLogicalOperator::hash(hash_value);

  return hash_value;
}

int ObLogLimit::check_output_dep_specific(ObRawExprCheckDep& checker)
{
  int ret = OB_SUCCESS;

  if (NULL != limit_count_) {
    if (OB_FAIL(checker.check(*limit_count_))) {
      LOG_WARN("failed to check limit_count_", K(*limit_count_), K(ret));
    }
  } else { /* Do nothing */
  }

  if (OB_SUCC(ret)) {
    if (NULL != limit_offset_) {
      if (OB_FAIL(checker.check(*limit_offset_))) {
        LOG_WARN("failed to check limit_offset_", K(limit_offset_), K(ret));
      }
    } else { /* Do nothing */
    }
  }

  if (OB_SUCC(ret)) {
    if (NULL != limit_percent_) {
      if (OB_FAIL(checker.check(*limit_percent_))) {
        LOG_WARN("failed to check limit_percent_", K(limit_percent_), K(ret));
      }
    }
  }

  if (OB_SUCC(ret) && is_fetch_with_ties()) {
    ObIArray<OrderItem>& order_items = get_expected_ordering();
    for (int64_t i = 0; OB_SUCC(ret) && i < order_items.count(); ++i) {
      ObRawExpr* cur_expr = order_items.at(i).expr_;
      if (OB_ISNULL(cur_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null ptr", K(cur_expr), K(ret));
      } else if (OB_FAIL(checker.check(*cur_expr))) {
        LOG_WARN("failed to check expr", K(cur_expr), K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObLogLimit::print_my_plan_annotation(char* buf, int64_t& buf_len, int64_t& pos, ExplainType type)
{
  int ret = OB_SUCCESS;
  if (NULL != limit_count_ || NULL != limit_offset_ || NULL != limit_percent_) {
    if (OB_FAIL(BUF_PRINTF(", "))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else {
      ObRawExpr* limit = limit_count_;
      ObRawExpr* offset = limit_offset_;
      ObRawExpr* percent = limit_percent_;
      EXPLAIN_PRINT_EXPR(limit, type);
      BUF_PRINTF(", ");
      EXPLAIN_PRINT_EXPR(offset, type);
      if (limit_percent_ != NULL) {
        BUF_PRINTF(", ");
        EXPLAIN_PRINT_EXPR(percent, type);
      }
      if (is_fetch_with_ties_) {
        BUF_PRINTF(", ");
        BUF_PRINTF("with_ties(%s)", "true");
      }
    }
  }
  return ret;
}

int ObLogLimit::inner_append_not_produced_exprs(ObRawExprUniqueSet& raw_exprs) const
{
  int ret = OB_SUCCESS;
  OZ(raw_exprs.append(limit_count_));
  OZ(raw_exprs.append(limit_offset_));
  if (NULL != limit_percent_) {
    OZ(raw_exprs.append(limit_percent_));
  }

  return ret;
}
