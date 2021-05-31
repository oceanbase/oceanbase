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
#include "sql/optimizer/ob_log_sort.h"
#include "ob_optimizer_context.h"
#include "ob_opt_est_cost.h"
#include "ob_optimizer_util.h"
#include "sql/optimizer/ob_log_plan.h"
#include "ob_log_exchange.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;

int ObLogSort::set_topk_params(
    ObRawExpr* limit_count, ObRawExpr* limit_offset, int64_t minimum_row_count, int64_t topk_precision)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(limit_count)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("limit_count is NULL", K(ret));
  } else {
    topk_limit_count_ = limit_count;
    topk_offset_count_ = limit_offset;
    minimum_row_count_ = minimum_row_count;
    topk_precision_ = topk_precision;
  }
  return ret;
}

int ObLogSort::transmit_op_ordering()
{
  int ret = OB_SUCCESS;
  // do nothing ,keep self ordering
  return ret;
}

int ObLogSort::copy_without_child(ObLogicalOperator*& out)
{
  int ret = OB_SUCCESS;
  out = NULL;
  ObLogicalOperator* op = NULL;
  ObLogSort* sort = NULL;
  if (OB_FAIL(clone(op))) {
    LOG_WARN("failed to clone sort operator", K(ret));
  } else if (OB_ISNULL(sort = static_cast<ObLogSort*>(op))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to cast ObLogicalOperator * to ObLogSort *", K(ret));
  } else if ((NULL != topk_limit_count_) &&
             OB_FAIL(
                 sort->set_topk_params(topk_limit_count_, topk_offset_count_, minimum_row_count_, topk_precision_))) {
    LOG_WARN("failed to set_topk_params", K(ret));
  } else {
    sort->sort_keys_.reset();
    for (int64_t i = 0; OB_SUCC(ret) && i < sort_keys_.count(); ++i) {
      if (OB_FAIL(sort->sort_keys_.push_back(sort_keys_.at(i)))) {
        LOG_WARN("failed to push back item", K(ret));
      }
    }
    sort->prefix_pos_ = prefix_pos_;
    out = sort;
  }
  return ret;
}

int ObLogSort::add_sort_key(const OrderItem& key)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(key.expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("key expr is null");
  } else if (key.expr_->has_const_or_const_expr_flag()) {
    // do nothing
  } else if (OB_FAIL(sort_keys_.push_back(key))) {
    LOG_WARN("push back sort key failed", K(ret));
  } else if (OB_FAIL(get_op_ordering().push_back(key))) {
    LOG_WARN("push back sort item failed", K(ret));
  }
  return ret;
}

int ObLogSort::set_sort_keys(const common::ObIArray<OrderItem>& order_keys)
{
  int ret = OB_SUCCESS;
  reset_op_ordering();
  reset_local_ordering();
  for (int64_t i = 0; OB_SUCC(ret) && i < order_keys.count(); ++i) {
    if (OB_ISNULL(order_keys.at(i).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("order key expr is null");
    } else if (order_keys.at(i).expr_->has_const_or_const_expr_flag()) {
      // do nothing
    } else if (OB_FAIL(sort_keys_.push_back(order_keys.at(i)))) {
      LOG_WARN("push back order key expr failed", K(ret));
    } else if (OB_FAIL(get_op_ordering().push_back(order_keys.at(i)))) {
      LOG_WARN("failed to push back order item", K(ret));
    } else {
      // do nothing
    }
  }
  return ret;
}

int ObLogSort::check_prefix_sort()
{
  int ret = OB_SUCCESS;
  int64_t left_match_count = 0;
  ObLogicalOperator* child = NULL;
  if (OB_ISNULL(child = get_child(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(child), K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::find_common_prefix_ordering(child->get_op_ordering(),
                 sort_keys_,
                 child->get_ordering_output_equal_sets(),
                 child->get_output_const_exprs(),
                 left_match_count,
                 prefix_pos_))) {
    LOG_WARN("failed to find common prefix ordering", K(ret));
  } else {
    prefix_pos_ = std::min(prefix_pos_, child->get_op_ordering().count());
    LOG_TRACE("succeed to check prefix sort", K(left_match_count), K(prefix_pos_), K(child->get_output_const_exprs()));
  }
  return ret;
}

int ObLogSort::check_local_merge_sort()
{
  int ret = OB_SUCCESS;
  bool need_sort = false;
  if (OB_FAIL(check_need_sort_for_local_order(0, &get_op_ordering(), need_sort))) {
    LOG_WARN("failed to check need sort for local order", K(ret));
  } else if (!need_sort) {
    // If child can retain local ordering for sort, then convert sort to sort with local order for optimization
    set_local_merge_sort(true);
  }
  return ret;
}

int ObLogSort::get_sort_exprs(common::ObIArray<ObRawExpr*>& sort_exprs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < sort_keys_.count(); ++i) {
    if (OB_FAIL(sort_exprs.push_back(sort_keys_.at(i).expr_))) {
      LOG_WARN("push back order key expr failed", K(ret));
    }
  }
  return ret;
}

int ObLogSort::clone_sort_keys_for_topk(
    ObIArray<OrderItem>& topk_sort_keys, const ObIArray<std::pair<ObRawExpr*, ObRawExpr*>>& push_down_avg_arr)
{
  int ret = OB_SUCCESS;
  ObOptimizerContext* opt_ctx = NULL;
  ObLogPlan* plan = get_plan();
  if (OB_ISNULL(plan) || OB_ISNULL(opt_ctx = &(plan->get_optimizer_context()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("plan or opt_ctx is NULL", KP(plan), KP(opt_ctx), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < sort_keys_.count(); ++i) {
      OrderItem& cur_order_item = sort_keys_.at(i);
      ObRawExpr* sort_expr = cur_order_item.expr_;
      ObRawExpr* new_sort_expr = NULL;
      if (OB_FAIL(ObOptimizerUtil::clone_expr_for_topk(opt_ctx->get_expr_factory(), sort_expr, new_sort_expr))) {
        LOG_WARN("failed to copy expr", K(cur_order_item), K(ret));
      } else if (OB_ISNULL(new_sort_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("new_sort_expr is NULL", K(ret));
      } else if (OB_FAIL(replace_expr_action(push_down_avg_arr, new_sort_expr))) {
        LOG_WARN("failed to replace_expr_action", K(ret));
      } else {
        OrderItem new_order_item = sort_keys_.at(i);
        new_order_item.expr_ = new_sort_expr;
        if (OB_FAIL(topk_sort_keys.push_back(new_order_item))) {
          LOG_WARN("failed to push back order_item", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObLogSort::push_down_sort(ObLogicalOperator* consumer_exch)
{
  int ret = OB_SUCCESS;

  //     sort                      sort
  //      |                          |
  //     exchange  => push down  exchange
  //      |                          |
  //     exchange                 exchange
  //                                 |
  //                               sort

  ObLogicalOperator* child = NULL;
  ObLogicalOperator* producer_exch = NULL;
  bool need_sort = true;
  if (OB_ISNULL(consumer_exch) || OB_ISNULL(producer_exch = consumer_exch->get_child(first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(consumer_exch), K(producer_exch));
  } else if (OB_FAIL(producer_exch->check_need_sort_below_node(0, get_sort_keys(), need_sort))) {
    LOG_WARN("failed to check need sort", K(ret));
  } else if (!need_sort) {
    // no need alloc pushed_down sort
  } else if (OB_FAIL(producer_exch->allocate_sort_below(first_child, get_sort_keys()))) {
    LOG_WARN("failed to insert LOG_SORT operator", K(ret));
  } else {
    LOG_TRACE("succeed to push down sort");
  }

  return ret;
}

int ObLogSort::allocate_exchange_post(AllocExchContext* ctx)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* child = NULL;
  if (OB_ISNULL(ctx) || OB_ISNULL(child = get_child(first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ctx), K(child), K(ret));
  } else if (OB_FAIL(sharding_info_.copy_with_part_keys(child->get_sharding_info()))) {
    LOG_WARN("failed to deep copy sharding info from child", K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

int ObLogSort::allocate_exchange(AllocExchContext* ctx, ObExchangeInfo& exch_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx) || OB_ISNULL(get_child(first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(ctx), K(get_child(first_child)));
  } else if (OB_FAIL(get_child(first_child)->allocate_exchange(ctx, exch_info))) {
    LOG_WARN("failed to allocate exchange", K(ret));
  } else if (OB_ISNULL(get_child(first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (!exch_info.is_pq_dist() && OB_FAIL(push_down_sort(get_child(first_child)))) {
    LOG_WARN("failed to push down sort", K(ret));
  } else {
    LOG_TRACE("succeed to allocate exchange for sort operator");
  }
  return ret;
}

int ObLogSort::allocate_expr_pre(ObAllocExprContext& ctx)
{
  int ret = OB_SUCCESS;
  uint64_t producer_id = OB_INVALID_ID;
  if (OB_FAIL(get_next_producer_id(get_child(first_child), producer_id))) {
    LOG_WARN("failed to get next producer id", K(ret));
  } else {
    ObSEArray<ObRawExpr*, 8> exprs;
    for (int64_t i = 0; OB_SUCC(ret) && i < sort_keys_.count(); i++) {
      if (OB_ISNULL(sort_keys_.at(i).expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(i));
      } else if (OB_FAIL(exprs.push_back(sort_keys_.at(i).expr_))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else { /*do nothing*/
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(add_exprs_to_ctx(ctx, exprs, producer_id))) {
        LOG_WARN("failed to add exprs to ctx", K(ret));
      } else if (OB_FAIL(ObLogicalOperator::allocate_expr_pre(ctx))) {
        LOG_WARN("failed to add exprs to ctx", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

uint64_t ObLogSort::hash(uint64_t seed) const
{
  uint64_t hash_value = seed;
  HASH_ARRAY(sort_keys_, hash_value);
  hash_value = ObOptimizerUtil::hash_expr(topn_count_, hash_value);
  hash_value = do_hash(minimum_row_count_, hash_value);
  hash_value = do_hash(topk_precision_, hash_value);
  hash_value = do_hash(prefix_pos_, hash_value);
  hash_value = ObOptimizerUtil::hash_expr(topk_limit_count_, hash_value);
  hash_value = ObOptimizerUtil::hash_expr(topk_offset_count_, hash_value);
  hash_value = ObLogicalOperator::hash(hash_value);

  return hash_value;
}

int ObLogSort::print_my_plan_annotation(char* buf, int64_t& buf_len, int64_t& pos, ExplainType type)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(BUF_PRINTF(", "))) {
    LOG_WARN("BUF_PRINTF fails", K(ret));
  } else {
    const ObIArray<OrderItem>& sort_keys = get_sort_keys();
    EXPLAIN_PRINT_SORT_ITEMS(sort_keys, type);
    if (NULL != topn_count_) {
      ObRawExpr* topn = topn_count_;
      BUF_PRINTF(", ");
      EXPLAIN_PRINT_EXPR(topn, type);
    }
    ObRawExpr* limit = topk_limit_count_;
    if (NULL != limit) {
      if (OB_FAIL(BUF_PRINTF(", minimum_row_count:%ld top_precision:%ld ", minimum_row_count_, topk_precision_))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      } else {
        ObRawExpr* offset = topk_offset_count_;
        BUF_PRINTF(", ");
        EXPLAIN_PRINT_EXPR(limit, type);
        BUF_PRINTF(", ");
        EXPLAIN_PRINT_EXPR(offset, type);
      }
    } else { /* Do nothing */
    }

    if (OB_SUCC(ret) && prefix_pos_ > 0) {
      BUF_PRINTF(", prefix_pos(");
      if (OB_FAIL(BUF_PRINTF("%ld)", prefix_pos_))) {
        LOG_WARN("BUF_PRINTF fails", K(ret), K(prefix_pos_));
      }
    }
    if (OB_SUCC(ret) && is_local_merge_sort_) {
      BUF_PRINTF(", local merge sort");
    }
  }
  return ret;
}

int ObLogSort::check_output_dep_specific(ObRawExprCheckDep& checker)
{
  int ret = OB_SUCCESS;
  // sort keys
  for (int64_t i = 0; OB_SUCC(ret) && i < sort_keys_.count(); i++) {
    if (OB_ISNULL(sort_keys_.at(i).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sort_keys_.at(i).expr_ is null", K(ret));
    } else if (OB_FAIL(checker.check(*sort_keys_.at(i).expr_))) {
      LOG_WARN("failed to check sort_keys_.at(i).expr_", K(i), K(ret));
    } else {
    }
  }

  if (OB_SUCC(ret) && NULL != topn_count_) {
    if (OB_FAIL(checker.check(*topn_count_))) {
      LOG_WARN("failed to check limit_count_", K(*topn_count_), K(ret));
    }
  }

  if (OB_SUCC(ret) && NULL != topk_limit_count_) {
    if (OB_FAIL(checker.check(*topk_limit_count_))) {
      LOG_WARN("failed to check limit_count_", K(*topk_limit_count_), K(ret));
    } else if (NULL != topk_offset_count_) {
      if (OB_FAIL(checker.check(*topk_offset_count_))) {
        LOG_WARN("failed to check limit_offset_", KPC(topk_offset_count_), K(ret));
      }
    } else { /* Do nothing */
    }
  }

  return ret;
}

int ObLogSort::inner_replace_generated_agg_expr(const ObIArray<std::pair<ObRawExpr*, ObRawExpr*>>& to_replace_exprs)
{
  int ret = OB_SUCCESS;
  int64_t N = sort_keys_.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
    OrderItem& cur_order_item = sort_keys_.at(i);
    if (OB_FAIL(replace_expr_action(to_replace_exprs, cur_order_item.expr_))) {
      LOG_WARN("failed to resolve ref params in sort key ", K(cur_order_item), K(ret));
    } else { /* Do nothing */
    }
  }
  return ret;
}

const char* ObLogSort::get_name() const
{
  return NULL != topn_count_ ? "TOP-N SORT" : log_op_def::get_op_name(type_);
}

int ObLogSort::est_cost()
{
  int ret = OB_SUCCESS;
  double sort_cost = 0.0;
  ObLogicalOperator* first_child = get_child(ObLogicalOperator::first_child);
  if (OB_ISNULL(first_child)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("first child is null", K(ret));
  } else {
    ObSortCostInfo cost_info(first_child->get_card(), first_child->get_width(), get_prefix_pos(), get_est_sel_info());
    if (OB_FAIL(ObOptEstCost::cost_sort(cost_info, get_sort_keys(), sort_cost))) {
      LOG_WARN("failed to calc cost", K(ret), K(first_child->get_type()));
    } else {
      set_op_cost(sort_cost);
      set_cost(first_child->get_cost() + sort_cost);
      set_card(first_child->get_card());
      set_width(first_child->get_width());
      LOG_TRACE("cost for sort operator", K(sort_cost), K(get_cost()), K(get_card()), K(get_width()));
    }
  }
  return ret;
}

int ObLogSort::re_est_cost(const ObLogicalOperator* parent, double need_row_count, bool& re_est)
{
  int ret = OB_SUCCESS;
  re_est = false;
  double new_op_cost = op_cost_;
  ObLogicalOperator* child = get_child(first_child);
  if (OB_ISNULL(child) || OB_ISNULL(parent)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(child), K(parent), K(ret));
  } else if (need_row_count < get_card()) {
    if (parent->get_type() != log_op_def::LOG_LIMIT) {
    } else {
      ObSortCostInfo cost_info(child->get_card(), child->get_width(), 0, NULL, need_row_count);
      if (OB_FAIL(ObOptEstCost::cost_sort(cost_info, get_sort_keys(), new_op_cost))) {
        LOG_WARN("failed to cost top-n sort", K(ret), K_(card), K(need_row_count));
      } else { /*do nothing*/
      }
    }
    if (OB_SUCC(ret)) {
      re_est = true;
      set_op_cost(new_op_cost);
      set_cost(new_op_cost + child->get_cost());
      set_card(need_row_count);
      LOG_TRACE("succeed to re-estimate cost for sort operator", K(new_op_cost), K(need_row_count), K(get_cost()));
    }
  }
  return ret;
}

int ObLogSort::compute_op_ordering()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(set_op_ordering(sort_keys_))) {
    LOG_WARN("failed to set op ordering", K(ret));
  }
  return ret;
}

int ObLogSort::inner_append_not_produced_exprs(ObRawExprUniqueSet& raw_exprs) const
{
  int ret = OB_SUCCESS;
  OZ(raw_exprs.append(topn_count_));
  OZ(raw_exprs.append(topk_limit_count_));
  OZ(raw_exprs.append(topk_offset_count_));

  return ret;
}

int ObLogSort::generate_link_sql_pre(GenLinkStmtContext& link_ctx)
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
  }
  return ret;
}
