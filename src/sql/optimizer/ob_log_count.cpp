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
#include "sql/optimizer/ob_log_count.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "sql/optimizer/ob_opt_est_cost.h"
#include "sql/optimizer/ob_log_plan.h"
#include "sql/rewrite/ob_transform_utils.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::sql::log_op_def;

namespace oceanbase {
namespace sql {
int ObLogCount::copy_without_child(ObLogicalOperator*& out)
{
  int ret = OB_SUCCESS;
  out = NULL;
  ObLogicalOperator* op = NULL;
  ObLogCount* count = NULL;
  if (OB_FAIL(clone(op))) {
    LOG_WARN("failed to clone child operator", K(ret));
  } else if (OB_ISNULL(count = static_cast<ObLogCount*>(op))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(count), K(ret));
  } else {
    count->set_rownum_limit_expr(rownum_limit_expr_);
    out = count;
  }
  return ret;
}

int ObLogCount::allocate_exchange_post(AllocExchContext* ctx)
{
  int ret = OB_SUCCESS;
  bool is_basic = false;
  ObExchangeInfo exch_info;
  ObLogicalOperator* exchange_point = NULL;
  bool should_push_limit = (get_filter_exprs().count() == 0 && NULL != rownum_limit_expr_);
  if (OB_ISNULL(ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ctx), K(ret));
  } else if (OB_FAIL(compute_basic_sharding_info(ctx, is_basic))) {
    LOG_WARN("failed to compute basic sharding info", K(ret));
  } else if (is_basic) {
    LOG_TRACE("is basic sharding info", K(sharding_info_));
  } else if (OB_FAIL(push_down_limit(ctx, rownum_limit_expr_, NULL, should_push_limit, false, exchange_point))) {
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

int ObLogCount::generate_link_sql_pre(GenLinkStmtContext& link_ctx)
{
  int ret = OB_SUCCESS;
  ObLinkStmt* link_stmt = link_ctx.link_stmt_;
  if (OB_ISNULL(link_stmt) || !link_stmt->is_inited()) {
    // do nothing.
  } else if (dblink_id_ != link_ctx.dblink_id_) {
    link_ctx.dblink_id_ = OB_INVALID_ID;
    link_ctx.link_stmt_ = NULL;
  } else if (OB_FAIL(link_stmt->try_fill_select_strs(output_exprs_))) {
    LOG_WARN("failed to fill link stmt select strs", K(ret), K(output_exprs_));
  } else if (OB_FAIL(link_stmt->fill_where_rownum(rownum_limit_expr_))) {
    LOG_WARN("failed to fill link stmt where rownum", K(ret));
  }
  return ret;
}

uint64_t ObLogCount::hash(uint64_t seed) const
{
  uint64_t hash_value = seed;
  hash_value = ObOptimizerUtil::hash_expr(rownum_limit_expr_, hash_value);
  hash_value = ObLogicalOperator::hash(hash_value);

  return hash_value;
}

int ObLogCount::set_limit_size()
{
  int ret = OB_SUCCESS;
  double limit_count_double = 0.0;
  double child_card = 0.0;
  if (OB_ISNULL(get_child(first_child)) || OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Limit's child not expected to be NULL", K(ret));
  } else if (FALSE_IT(child_card = get_child(first_child)->get_card())) {
    /*do nothing*/
  } else if (NULL == rownum_limit_expr_) {
    limit_count_double = static_cast<double>(child_card);
  } else if (rownum_limit_expr_->has_flag(CNT_EXEC_PARAM)) {
    limit_count_double = static_cast<double>(child_card);
  } else {
    int64_t limit_count = 0;
    bool is_null_value = false;
    if (OB_FAIL(ObTransformUtils::get_limit_value(rownum_limit_expr_,
            get_plan()->get_stmt(),
            get_plan()->get_optimizer_context().get_params(),
            get_plan()->get_optimizer_context().get_session_info(),
            &get_plan()->get_optimizer_context().get_allocator(),
            limit_count,
            is_null_value))) {
      LOG_WARN("Get limit count num error", K(ret));
    } else if (limit_count < 0) {
      limit_count_double = 0.0;
    } else if (limit_count >= child_card) {
      limit_count_double = static_cast<double>(child_card);
    } else {
      limit_count_double = static_cast<double>(limit_count);
    }
  }

  if (OB_SUCC(ret) && limit_count_double < child_card) {
    bool re_est = false;
    if (OB_FAIL(get_child(first_child)->re_est_cost(this, limit_count_double, re_est))) {
      LOG_WARN("Failed to re est cost", K(limit_count_double), K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    double op_cost = ObOptEstCost::cost_count(get_child(first_child)->get_card());
    set_card(limit_count_double);
    set_op_cost(op_cost);
    set_cost(get_child(first_child)->get_cost() + op_cost);
    set_width(get_child(first_child)->get_width() + 4);
  }
  return ret;
}

int ObLogCount::re_est_cost(const ObLogicalOperator* parent, double need_row_count, bool& re_est)
{
  int ret = OB_SUCCESS;
  UNUSED(parent);
  re_est = false;
  if (OB_UNLIKELY(need_row_count < 0.0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Need row count should not less than 0", K(ret));
  } else if (need_row_count < get_card()) {
    set_card(need_row_count);
    ObLogicalOperator* child = NULL;
    if (OB_ISNULL(child = get_child(first_child))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Count's child not expected to be NULL", K(ret));
    } else if (OB_FAIL(child->re_est_cost(this, need_row_count, re_est))) {
      LOG_WARN("Failed to re_est child cost", K(ret));
    } else {
      re_est = true;
      double op_cost = ObOptEstCost::cost_count(get_card());
      set_op_cost(op_cost);
      set_cost(child->get_cost() + op_cost);
    }
  }
  return ret;
}

int ObLogCount::check_output_dep_specific(ObRawExprCheckDep& checker)
{
  int ret = OB_SUCCESS;
  if (NULL != rownum_limit_expr_) {
    if (OB_FAIL(checker.check(*rownum_limit_expr_))) {
      LOG_WARN("failed to check limit_count_", K(*rownum_limit_expr_), K(ret));
    }
  } else { /* Do nothing */
  }
  return ret;
}

int ObLogCount::allocate_expr_pre(ObAllocExprContext& ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObLogicalOperator::allocate_expr_pre(ctx))) {
    LOG_WARN("failed to add exprs to ctx", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ctx.expr_producers_.count(); ++i) {
      ExprProducer& producer = ctx.expr_producers_.at(i);
      if (OB_ISNULL(producer.expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_INVALID_ID == producer.producer_id_ && producer.expr_->has_flag(IS_ROWNUM)) {
        producer.producer_id_ = id_;
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObLogCount::print_my_plan_annotation(char* buf, int64_t& buf_len, int64_t& pos, ExplainType type)
{
  int ret = OB_SUCCESS;
  if (NULL != rownum_limit_expr_) {
    if (OB_FAIL(BUF_PRINTF(", "))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else {
      EXPLAIN_PRINT_EXPR(rownum_limit_expr_, type);
    }
  }
  return ret;
}

int ObLogCount::inner_append_not_produced_exprs(ObRawExprUniqueSet& raw_exprs) const
{
  int ret = OB_SUCCESS;
  OZ(raw_exprs.append(rownum_limit_expr_));

  return ret;
}

} /* namespace sql */
} /* namespace oceanbase */
