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
#include "sql/optimizer/ob_log_distinct.h"
#include "sql/optimizer/ob_log_exchange.h"
#include "sql/optimizer/ob_log_sort.h"

#include "sql/optimizer/ob_log_operator_factory.h"
#include "ob_opt_est_cost.h"
#include "ob_select_log_plan.h"
#include "ob_optimizer_util.h"

using namespace oceanbase;
using namespace sql;
using namespace oceanbase::common;

namespace oceanbase {
namespace sql {

const char* ObLogDistinct::get_name() const
{
  return MERGE_AGGREGATE == algo_ ? "MERGE DISTINCT" : "HASH DISTINCT";
}

int ObLogDistinct::copy_without_child(ObLogicalOperator*& out)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* op = NULL;
  ObLogDistinct* distinct = NULL;
  if (OB_FAIL(clone(op))) {
    SQL_OPT_LOG(WARN, "failed to clone ObLogDistinct", K(ret));
  } else if (OB_ISNULL(distinct = static_cast<ObLogDistinct*>(op))) {
    ret = OB_ERR_UNEXPECTED;
    SQL_OPT_LOG(WARN, "failed to cast ObLogicalOperator * to ObLogDistinct *", K(ret));
  } else if (OB_FAIL(distinct->set_distinct_exprs(distinct_exprs_))) {
    LOG_WARN("Failed to set distinct exprs", K(ret));
  } else {
    distinct->set_algo_type(algo_);
    out = distinct;
  }
  return ret;
}

int ObLogDistinct::push_down_distinct(
    AllocExchContext* ctx, ObIArray<OrderItem>& sort_keys, ObLogicalOperator*& exchange_point)
{
  // distinct         distinct
  //    |                 |
  //  (sort)     =>     (sort)
  //    |                 |
  //   other         distinct(child)
  //                      |
  //                 (sort(child))
  //                      |
  //                    other
  int ret = OB_SUCCESS;
  bool need_sort = false;
  bool can_push_distinct = false;
  ObLogicalOperator* child = NULL;
  ObLogicalOperator* child_sort = NULL;
  ObLogicalOperator* child_distinct = NULL;
  exchange_point = NULL;
  if (OB_ISNULL(child = get_child(first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(child), K(ctx));
  } else if (MERGE_AGGREGATE == get_algo() && log_op_def::LOG_SORT == child->get_type()) {

    need_sort = true;
    exchange_point = child;
  } else {
    exchange_point = this;
  }
  // push down distinct
  if (OB_FAIL(ret)) {
    /*do nothing*/
  } else if (OB_FAIL(check_fulfill_cut_ratio_condition(ctx->parallel_, get_card(), can_push_distinct))) {
    LOG_WARN("failed to check fulfill cut ratio condition", K(ret));
  } else if (!can_push_distinct) {
    /*do nothing*/
  } else if (need_sort && OB_FAIL(exchange_point->allocate_sort_below(0, sort_keys))) {
    LOG_WARN("failed to allocate child sort", K(ret));
  } else if (OB_FAIL(exchange_point->allocate_distinct_below(first_child, distinct_exprs_, get_algo()))) {
    LOG_WARN("failed to allocate child distinct", K(ret));
  } else if (OB_ISNULL(child_distinct = exchange_point->get_child(first_child)) ||
             OB_ISNULL(child = child_distinct->get_child(first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(child_distinct), K(child), K(ret));
  } else if (OB_FAIL(child_distinct->set_expected_ordering(get_expected_ordering()))) {
    LOG_WARN("failed to set expected ordering", K(ret));
  } else if (OB_FAIL(child_distinct->replace_generated_agg_expr(ctx->group_push_down_replaced_exprs_))) {
    LOG_WARN("failed to replace agg expr", K(ret));
  } else if (OB_FAIL(child_distinct->get_sharding_info().copy_with_part_keys(child->get_sharding_info()))) {
    LOG_WARN("failed to copy sharding info", K(ret));
  } else {
    child_distinct->set_card(get_card());
    child_distinct->set_op_cost(get_op_cost());
    child_distinct->set_width(get_width());
    if (need_sort || HASH_AGGREGATE == get_algo() || log_op_def::LOG_TABLE_SCAN != child->get_type()) {
      /*do nothing*/
    } else {
      child_distinct->set_is_partition_wise(true);
      child_distinct->set_is_block_gi_allowed(true);
    }
  }
  return ret;
}

int ObLogDistinct::allocate_exchange_post(AllocExchContext* ctx)
{
  int ret = OB_SUCCESS;
  bool is_basic = false;
  bool sharding_compatible = false;
  ObLogicalOperator* child = NULL;
  ObLogicalOperator* exchange_point = NULL;
  ObArray<OrderItem> order_keys;
  ObSEArray<ObRawExpr*, 4> key_exprs;
  if (OB_ISNULL(ctx) || OB_ISNULL(child = get_child(first_child)) || OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ctx), K(child), K(get_plan()), K(ret));
  } else if (OB_FAIL(compute_basic_sharding_info(ctx, is_basic))) {
    LOG_WARN("failed to compute basic sharding info", K(ret));
  } else if (is_basic) {
    LOG_TRACE("is basic sharding info", K(sharding_info_));
  } else if (OB_FAIL(prune_weak_part_exprs(*ctx, get_distinct_exprs(), key_exprs))) {
    LOG_WARN("failed to prune weak part exprs", K(ret));
  } else if (OB_FAIL(check_sharding_compatible_with_reduce_expr(child->get_sharding_info(),
                 key_exprs,
                 child->get_sharding_output_equal_sets(),
                 sharding_compatible))) {
    LOG_WARN("Failed to check sharding compatible with reduce expr", K(ret), K(*child));
  } else if (sharding_compatible) {
    if (OB_FAIL(sharding_info_.copy_with_part_keys(child->get_sharding_info()))) {
      LOG_WARN("failed to copy sharding info", K(ret));
    } else {
      is_partition_wise_ = (NULL != sharding_info_.get_phy_table_location_info());
    }
  } else if (OB_FAIL(make_order_keys(get_distinct_exprs(), order_keys))) {
    LOG_WARN("failed to make order items", K(ret));
  } else if (OB_FAIL(push_down_distinct(ctx, order_keys, exchange_point))) {
    LOG_WARN("failed to push down distinct", K(ret));
  } else if (OB_ISNULL(exchange_point)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(exchange_point->allocate_grouping_style_exchange_below(ctx, distinct_exprs_))) {
    LOG_WARN("failed to allocate grouping style exchange", K(ret));
  } else if (OB_FAIL(sharding_info_.copy_with_part_keys(exchange_point->get_sharding_info()))) {
    LOG_WARN("failed to copy sharding info", K(ret));
  } else { /*do nothing*/
  }

  return ret;
}

int ObLogDistinct::print_my_plan_annotation(char* buf, int64_t& buf_len, int64_t& pos, ExplainType type)
{
  int ret = OB_SUCCESS;
  // print access
  if (OB_FAIL(BUF_PRINTF(", "))) {
    LOG_WARN("BUF_PRINTF fails", K(ret));
  } else if (OB_FAIL(BUF_PRINTF("\n      "))) {
    LOG_WARN("BUF_PRINTF fails", K(ret));
  } else { /* Do nothing */
  }
  const ObIArray<ObRawExpr*>& distinct = distinct_exprs_;
  EXPLAIN_PRINT_EXPRS(distinct, type);
  if (OB_SUCC(ret) && is_block_mode_) {
    ret = BUF_PRINTF(", block");
  }
  return ret;
}

int ObLogDistinct::inner_replace_generated_agg_expr(
    const ObIArray<std::pair<ObRawExpr*, ObRawExpr*> >& to_replace_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(replace_exprs_action(to_replace_exprs, distinct_exprs_))) {
    SQL_OPT_LOG(WARN, "failed to replace agg exprs in distinct op", K(ret));
  }
  return ret;
}

int ObLogDistinct::transmit_op_ordering()
{
  int ret = OB_SUCCESS;
  bool need_sort = false;
  reset_op_ordering();
  reset_local_ordering();
  if (HASH_AGGREGATE == get_algo()) {
    /*do nothing*/
  } else {
    ObLogicalOperator* child = get_child(first_child);
    bool need_sort = false;
    if (OB_FAIL(check_need_sort_for_grouping_op(0, expected_ordering_, need_sort))) {
      LOG_WARN("failed to check need sort", K(ret));
    } else if (need_sort) {  // need alloc sort
      if (OB_FAIL(allocate_sort_below(0, expected_ordering_))) {
        LOG_WARN("failed to allocate implicit sort", K(ret));
      } else if (OB_FAIL(set_op_ordering(expected_ordering_))) {
        LOG_WARN("failed to set op ordering", K(ret));
      }
    } else if (OB_FAIL(set_op_ordering(child->get_op_ordering()))) {
      LOG_WARN("failed to set op's ordering", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObLogDistinct::compute_op_ordering()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* child = NULL;
  if (algo_ == HASH_AGGREGATE) {
    // do nothing
    reset_op_ordering();
  } else if (OB_ISNULL(child = get_child(ObLogicalOperator::first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child is null", K(ret));
  } else if (OB_FAIL(set_op_ordering(child->get_op_ordering()))) {
    LOG_WARN("failed to set op ordering", K(ret));
  }
  return ret;
}

int ObLogDistinct::est_cost()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* first_child = get_child(ObLogicalOperator::first_child);
  double distinct_card = 0.0;
  double distinct_cost = 0.0;
  if (OB_ISNULL(first_child) || OB_ISNULL(get_est_sel_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret));
  } else if (OB_FAIL(ObOptEstSel::calculate_distinct(
                 first_child->get_card(), *get_est_sel_info(), distinct_exprs_, distinct_card))) {
    LOG_WARN("failed to calculate distinct", K(ret));
  } else if (MERGE_AGGREGATE == algo_) {
    distinct_cost = ObOptEstCost::cost_merge_distinct(first_child->get_card(), distinct_card, distinct_exprs_);
  } else {
    distinct_cost = ObOptEstCost::cost_hash_distinct(first_child->get_card(), distinct_card, distinct_exprs_);
  }

  if (OB_SUCC(ret)) {
    set_card(distinct_card);
    set_cost(first_child->get_cost() + distinct_cost);
    set_op_cost(distinct_cost);
    set_width(first_child->get_width());
  }
  return ret;
}

int ObLogDistinct::re_est_cost(const ObLogicalOperator* parent, double need_row_count, bool& re_est)
{
  int ret = OB_SUCCESS;
  UNUSED(parent);
  re_est = false;
  ObLogicalOperator* child = NULL;
  if (need_row_count >= get_card()) {
    /* do nothing */
  } else if (OB_ISNULL(child = get_child(first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child is NULL", K(ret), K(child));
  } else if (OB_FAIL(child->re_est_cost(this, child->get_card() * need_row_count / get_card(), re_est))) {
    LOG_WARN("re-estimate cost of child failed", K(ret));
  } else {
    double distinct_cost = op_cost_;
    if (MERGE_AGGREGATE == algo_) {
      distinct_cost = ObOptEstCost::cost_merge_distinct(child->get_card(), need_row_count, distinct_exprs_);
    } else if (HASH_AGGREGATE == algo_) {
      distinct_cost = ObOptEstCost::cost_hash_distinct(child->get_card(), need_row_count, distinct_exprs_);
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unrecognized algorithm detected which is unexpected", K(ret), K(algo_));
    }
    if (OB_SUCC(ret)) {
      card_ = need_row_count;
      op_cost_ = distinct_cost;
      cost_ = (child->get_cost() + distinct_cost);
      re_est = true;
    }
  }
  return ret;
}

uint64_t ObLogDistinct::hash(uint64_t seed) const
{
  seed = ObOptimizerUtil::hash_exprs(seed, distinct_exprs_);
  seed = do_hash(algo_, seed);
  seed = ObLogicalOperator::hash(seed);

  return seed;
}

int ObLogDistinct::compute_fd_item_set()
{
  int ret = OB_SUCCESS;
  const ObLogicalOperator* child = NULL;
  ObFdItemSet* fd_item_set = NULL;
  ObTableFdItem* fd_item = NULL;
  if (OB_ISNULL(child = get_child(ObLogicalOperator::first_child)) || OB_ISNULL(my_plan_) || OB_ISNULL(get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpect null", K(ret), K(child), K(my_plan_), K(get_stmt()));
  } else if (OB_FAIL(my_plan_->get_fd_item_factory().create_fd_item_set(fd_item_set))) {
    LOG_WARN("failed to create fd item set", K(ret));
  } else if (OB_FAIL(fd_item_set->assign(child->get_fd_item_set()))) {
    LOG_WARN("failed to assign fd item set", K(ret));
  } else if (OB_FAIL(my_plan_->get_fd_item_factory().create_table_fd_item(
                 fd_item, true, distinct_exprs_, get_stmt()->get_current_level(), get_table_set()))) {
    LOG_WARN("failed to create fd item", K(ret));
  } else if (OB_FAIL(fd_item_set->push_back(fd_item))) {
    LOG_WARN("failed to push back fd item", K(ret));
  } else if (OB_FAIL(deduce_const_exprs_and_ft_item_set(*fd_item_set))) {
    LOG_WARN("falied to deduce fd item set", K(ret));
  } else {
    set_fd_item_set(fd_item_set);
  }
  return ret;
}

int ObLogDistinct::allocate_granule_pre(AllocGIContext& ctx)
{
  return pw_allocate_granule_pre(ctx);
}

int ObLogDistinct::allocate_granule_post(AllocGIContext& ctx)
{
  return pw_allocate_granule_post(ctx);
}

int ObLogDistinct::allocate_expr_pre(ObAllocExprContext& ctx)
{
  int ret = OB_SUCCESS;
  uint64_t producer_id = OB_INVALID_ID;
  if (OB_FAIL(get_next_producer_id(get_child(first_child), producer_id))) {
    LOG_WARN("failed to get next producer id", K(ret));
  } else if (OB_FAIL(add_exprs_to_ctx(ctx, distinct_exprs_, producer_id))) {
    LOG_WARN("failed to add exprs to ctx", K(ret));
  } else if (OB_FAIL(ObLogicalOperator::allocate_expr_pre(ctx))) {
    LOG_WARN("failed to allocate parent expr pre", K(ret));
  } else { /*do nothing*/
  }

  return ret;
}

int ObLogDistinct::generate_link_sql_pre(GenLinkStmtContext& link_ctx)
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
  } else if (OB_FAIL(link_stmt->set_select_distinct())) {
    LOG_WARN("failed to set link stmt select distinct", K(ret));
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
