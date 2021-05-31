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
#include "sql/optimizer/ob_log_exchange.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "sql/optimizer/ob_log_plan.h"
#include "sql/optimizer/ob_opt_est_cost.h"
#include "sql/optimizer/ob_log_distinct.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

int32_t ObLogExchange::get_explain_name_length() const
{
  int32_t length = 0;

  length += (int32_t)strlen(get_name());
  ++length;

  if (is_remote_) {
    length += (int32_t)strlen("REMOTE");
  } else {
    if (is_rescanable()) {
      // PX COORDINATOR, do nothing
    } else {
      length += (int32_t)strlen("DISTR");
    }
  }

  if (is_producer() && exch_info_.is_repart_exchange()) {
    ++length;  // whitespace
    if (exch_info_.dist_method_ == ObPQDistributeMethod::PARTITION_RANDOM) {
      length += (int32_t)strlen("(PKEY RANDOM)");
    } else if (exch_info_.dist_method_ == ObPQDistributeMethod::PARTITION_HASH) {
      length += (int32_t)strlen("(PKEY HASH)");
    } else {
      length += (int32_t)strlen("(PKEY)");
    }
  }

  if (is_producer()) {
    if (exch_info_.is_repart_exchange()) {
      if (is_slave_mapping()) {
        length += static_cast<int32_t>(strlen(" LOCAL"));
      }
    } else if (exch_info_.is_pq_dist()) {
      auto print_method = (ObPQDistributeMethod::SM_BROADCAST == exch_info_.dist_method_)
                              ? ObPQDistributeMethod::BROADCAST
                              : exch_info_.dist_method_;
      print_method =
          (ObPQDistributeMethod::PARTITION_HASH == exch_info_.dist_method_) ? ObPQDistributeMethod::HASH : print_method;
      const char* str = ObPQDistributeMethod::get_type_string(print_method);
      length += static_cast<int32_t>(strlen(" ()") + strlen(str));
      if (is_slave_mapping()) {
        length += static_cast<int32_t>(strlen(" LOCAL"));
      }
    }
  }

  return length;
}

int ObLogExchange::get_explain_name_internal(char* buf, const int64_t buf_len, int64_t& pos)
{
  int ret = OB_SUCCESS;

  ret = BUF_PRINTF("%s", get_name());
  if (OB_SUCC(ret)) {
    ret = BUF_PRINTF(" ");
  }

  if (OB_SUCC(ret)) {
    if (is_remote_) {
      ret = BUF_PRINTF("REMOTE");
    } else {
      if (is_rescanable()) {
        // PX COORDINATOR, do nothing
      } else {
        ret = BUF_PRINTF("DISTR");
      }
    }
  } else { /* Do nothing */
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("BUF_PRINTF failed", K(ret));
  } else { /* Do nothing */
  }

  if (OB_SUCC(ret) && is_producer()) {
    if (exch_info_.is_repart_exchange()) {
      if (OB_SUCC(ret)) {
        ret = BUF_PRINTF(" ");
      }
      if (OB_SUCC(ret) && exch_info_.dist_method_ == ObPQDistributeMethod::PARTITION_RANDOM) {
        ret = BUF_PRINTF("(PKEY RANDOM");
      } else if (OB_SUCC(ret) && exch_info_.dist_method_ == ObPQDistributeMethod::PARTITION_HASH) {
        ret = BUF_PRINTF("(PKEY HASH");
      } else {
        ret = BUF_PRINTF("(PKEY");
      }
    } else {
      if (exch_info_.is_pq_dist()) {
        auto print_method = (ObPQDistributeMethod::SM_BROADCAST == exch_info_.dist_method_)
                                ? ObPQDistributeMethod::BROADCAST
                                : exch_info_.dist_method_;
        print_method = (ObPQDistributeMethod::PARTITION_HASH == exch_info_.dist_method_) ? ObPQDistributeMethod::HASH
                                                                                         : print_method;
        const char* str = ObPQDistributeMethod::get_type_string(print_method);
        ret = BUF_PRINTF(" (%s", str);
      }
    }

    if (OB_SUCC(ret) && (exch_info_.is_pq_dist() || exch_info_.is_repart_exchange())) {
      if (is_slave_mapping()) {
        ret = BUF_PRINTF(" LOCAL)");
      } else {
        ret = BUF_PRINTF(")");
      }
    }
  }
  return ret;
}

const char* ObLogExchange::get_name() const
{
  static const char* exchange_type[7] = {"EXCHANGE OUT",
      "EXCHANGE IN",
      "EXCHANGE IN TASK ORDER",
      "EXCHANGE IN MERGE SORT",
      "PX COORDINATOR",
      "PX COORDINATOR TASK ORDER",
      "PX COORDINATOR MERGE SORT"};
  int offset = is_rescanable() && !is_remote_ ? 3 : 0;
  return is_producer() ? exchange_type[0]
                       : ((is_task_order() && !is_merge_sort())
                                 ? exchange_type[2 + offset]
                                 : (is_merge_sort() ? exchange_type[3 + offset] : exchange_type[1 + offset]));
}

int ObLogExchange::copy_without_child(ObLogicalOperator*& out)
{
  int ret = OB_SUCCESS;
  ObLogExchange* exchange = NULL;
  if (OB_FAIL(clone(out))) {
    LOG_WARN("Failed to clone basic logical op", K(ret));
  } else if (OB_ISNULL(exchange = static_cast<ObLogExchange*>(out))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Exchange is NULL", K(ret));
  } else if (OB_FAIL(exchange->set_exchange_info(exch_info_))) {
    LOG_WARN("Failed to set exchange info", K(ret));
  } else {
    exchange->is_producer_ = is_producer_;
    exchange->is_remote_ = is_remote_;
    exchange->is_merge_sort_ = is_merge_sort_;
    if (is_merge_sort_) {
      if (OB_FAIL(exchange->sort_keys_.assign(sort_keys_))) {
        LOG_WARN("Failed to assign sort keys", K(ret));
      }
    }
  }
  return ret;
}

int ObLogExchange::check_output_dep_specific(ObRawExprCheckDep& checker)
{
  int ret = OB_SUCCESS;
  if (is_merge_sort_) {
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
  }
  return ret;
}

int ObLogExchange::inner_replace_generated_agg_expr(
    const common::ObIArray<std::pair<ObRawExpr*, ObRawExpr*> >& to_replace_exprs)
{
  int ret = OB_SUCCESS;
  if (is_merge_sort_) {
    int64_t N = sort_keys_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
      OrderItem& cur_order_item = sort_keys_.at(i);
      if (OB_FAIL(replace_expr_action(to_replace_exprs, cur_order_item.expr_))) {
        LOG_WARN("failed to resolve ref params in sort key ", K(cur_order_item), K(ret));
      } else { /* Do nothing */
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(replace_exprs_action(to_replace_exprs, exch_info_.repartition_keys_))) {
      LOG_WARN("failed to replace agg exprs", K(ret));
    } else if (OB_FAIL(replace_exprs_action(to_replace_exprs, exch_info_.repartition_sub_keys_))) {
      LOG_WARN("failed to replace agg exprs", K(ret));
    } else if (OB_FAIL(replace_exprs_action(to_replace_exprs, exch_info_.repartition_func_exprs_))) {
      LOG_WARN("failed to replace agg exprs", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < exch_info_.hash_dist_exprs_.count(); i++) {
        if (OB_ISNULL(exch_info_.hash_dist_exprs_.at(i).expr_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (OB_FAIL(replace_expr_action(to_replace_exprs, exch_info_.hash_dist_exprs_.at(i).expr_))) {
          LOG_WARN("failed to replace agg exprs", K(ret));
        } else { /*do nothing*/
        }
      }
    }
  }
  return ret;
}

int ObLogExchange::est_cost()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* child = NULL;
  if (OB_ISNULL(child = get_child(ObLogicalOperator::first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child is null", K(ret), K(child));
  } else {
    set_card(child->get_card());
    set_width(child->get_width());
    if (is_producer()) {
      set_op_cost(0.0);
    } else {
      double transmit_cost = ObOptEstCost::cost_transmit(get_card(), get_width());
      set_op_cost(transmit_cost);
    }
    set_cost(child->get_cost() + get_op_cost());
  }
  return ret;
}

int ObLogExchange::set_sort_keys(const common::ObIArray<OrderItem>& order_keys)
{
  int ret = OB_SUCCESS;
  sort_keys_.reset();
  reset_op_ordering();
  common::ObIArray<OrderItem>& op_ordering = get_op_ordering();
  for (int64_t i = 0; OB_SUCC(ret) && i < order_keys.count(); ++i) {
    if (OB_ISNULL(order_keys.at(i).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("order key expr is null");
    } else if (order_keys.at(i).expr_->has_const_or_const_expr_flag()) {
      // do nothing
    } else if (OB_FAIL(sort_keys_.push_back(order_keys.at(i)))) {
      LOG_WARN("push back order key expr failed", K(ret));
    } else if (OB_FAIL(op_ordering.push_back(order_keys.at(i)))) {
      LOG_WARN("failed to push back item", K(ret));
    }
  }
  return ret;
}

int ObLogExchange::allocate_exchange_post(AllocExchContext* ctx)
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  return ret;
}

int ObLogExchange::print_plan_head_annotation(char* buf, int64_t& buf_len, int64_t& pos, ExplainType type)
{
  int ret = OB_SUCCESS;
  if (is_producer()) {
    if (exch_info_.is_repart_exchange()) {
      int64_t repart_key_count = exch_info_.repartition_keys_.count();
      int64_t key_count = repart_key_count + exch_info_.repartition_sub_keys_.count();
      if (OB_FAIL(BUF_PRINTF("(#keys=%ld", key_count))) {
        LOG_WARN("failed to BUF_PRINTF", K(ret));
      } else {
        if (key_count == 0) {
          if (OB_FAIL(BUF_PRINTF("), "))) {
            LOG_WARN("failed print ),", K(ret));
          }
        } else {
          if (OB_FAIL(BUF_PRINTF(", "))) {
            LOG_WARN("failed print , ", K(ret));
          }
        }
        const ObIArray<ObRawExpr*>& repart_keys = exch_info_.repartition_keys_;
        const ObIArray<ObRawExpr*>& repart_sub_keys = exch_info_.repartition_sub_keys_;
        for (int64_t i = 0; OB_SUCC(ret) && i < key_count; ++i) {
          ObRawExpr* key = i < repart_key_count ? repart_keys.at(i) : repart_sub_keys.at(i - repart_key_count);
          if (OB_FAIL(BUF_PRINTF("["))) {
            LOG_WARN("failed print [", K(ret));
          }
          if (OB_ISNULL(key)) {
            ret = BUF_PRINTF("nil");
          } else if (OB_FAIL(key->get_name(buf, buf_len, pos, type))) {
            LOG_WARN("print expr name failed", K(ret));
          } else if (key->get_result_type().is_numeric_type()) {
          } else if (OB_FAIL(key->get_type_and_length(buf, buf_len, pos, type))) {
            LOG_WARN("print expr type and length failed", K(ret));
          } else { /*Do nothing*/
          }
          if (OB_FAIL(BUF_PRINTF("]"))) {
            LOG_WARN("failed print ]", K(ret));
          }
          if (OB_SUCC(ret)) {
            if (i == key_count - 1) {
              if (OB_FAIL(BUF_PRINTF("), "))) {
                LOG_WARN("failed to BUF_PRINTF", K(ret));
              }
            } else if (OB_FAIL(BUF_PRINTF(", "))) {
              LOG_WARN("failed to BUF_PRINTF", K(ret));
            }
          }
        }
      }
    }
    if (OB_SUCC(ret) && exch_info_.is_pq_hash_dist()) {
      if (OB_FAIL(BUF_PRINTF("(#keys=%ld, ", exch_info_.hash_dist_exprs_.count()))) {
        LOG_WARN("buf print failed", K(ret));
      } else {
        auto& keys = exch_info_.hash_dist_exprs_;
        for (int64_t i = 0; i < keys.count() && OB_SUCC(ret); i++) {
          auto key = keys.at(i).expr_;
          if (OB_FAIL(BUF_PRINTF("["))) {
            LOG_WARN("failed print [", K(ret));
          }
          if (OB_FAIL(ret)) {
          } else if (OB_ISNULL(key)) {
            ret = BUF_PRINTF("nil");
          } else if (OB_FAIL(key->get_name(buf, buf_len, pos, type))) {
            LOG_WARN("print expr name failed", K(ret));
          } else if (key->get_result_type().is_numeric_type()) {
          } else if (OB_FAIL(key->get_type_and_length(buf, buf_len, pos, type))) {
            LOG_WARN("print expr type and length failed", K(ret));
          } else { /*Do nothing*/
          }

          if (OB_SUCC(ret)) {
            if (OB_FAIL(BUF_PRINTF("]"))) {
              LOG_WARN("failed print ]", K(ret));
            }
          }

          if (i == keys.count() - 1) {
            if (OB_FAIL(BUF_PRINTF("), "))) {
              LOG_WARN("failed to BUF_PRINTF", K(ret));
            }
          } else if (OB_FAIL(BUF_PRINTF(", "))) {
            LOG_WARN("failed to BUF_PRINTF", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObLogExchange::print_my_plan_annotation(char* buf, int64_t& buf_len, int64_t& pos, ExplainType type)
{
  int ret = OB_SUCCESS;
  if (is_producer()) {
    // some information is print by function print_plan_head_annotation
    if (exch_info_.px_single_ && OB_PHY_PLAN_REMOTE != get_plan()->get_phy_plan_type()) {
      ret = BUF_PRINTF(", is_single");
    }
    if (OB_SUCC(ret) && exch_info_.px_dop_ > 0 && OB_PHY_PLAN_REMOTE != get_plan()->get_phy_plan_type()) {
      ret = BUF_PRINTF(", dop=%ld", exch_info_.px_dop_);
    }
  } else {
    if (exch_info_.is_task_order_) {
      ret = BUF_PRINTF(", task_order");
    } else if (is_merge_sort_) {
      ret = BUF_PRINTF(", ");
      if (OB_SUCC(ret)) {
        const ObIArray<OrderItem>& sort_keys = get_sort_keys();
        EXPLAIN_PRINT_SORT_ITEMS(sort_keys, type);
      }
      if (OB_SUCC(ret) && is_local_order()) {
        ret = BUF_PRINTF(", Local Order");
      }
    }
  }
  return ret;
}

int ObLogExchange::compute_op_ordering()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* child = NULL;
  if (OB_ISNULL(child = get_child(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child is null");
  } else if (is_producer() || exch_info_.is_keep_order()) {
    if (OB_FAIL(set_op_ordering(child->get_op_ordering()))) {
      LOG_WARN("failed to set op ordering", K(ret));
    }
  } else {
    reset_op_ordering();
  }
  return ret;
}

int ObLogExchange::transmit_op_ordering()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* child = NULL;
  if (OB_ISNULL(child = get_child(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL child", K(ret));
  } else if (is_producer()) {
    reset_op_ordering();
    reset_local_ordering();
    ret = set_op_ordering(child->get_op_ordering());
    LOG_TRACE("set op ordering", K(ret), K(get_op_ordering()));
  } else if (exch_info_.is_keep_order()) {
    reset_op_ordering();
    reset_local_ordering();
    // for repart exchange, if repart node is a single node, keep child's order
    // or if grand child is local or remote, then exchange can keep grand child's order
    LOG_TRACE("origin op ordering", K(ret), K(get_op_ordering()));
    if (OB_FAIL(set_op_ordering(child->get_op_ordering()))) {
      LOG_WARN("failed to set op ordering", K(ret));
    } else {
      LOG_TRACE("set op ordering", K(ret), K(get_op_ordering()));
    }
  } else {
    LOG_TRACE("op ordering", K(ret), K(get_op_ordering()));
  }
  return ret;
}

int ObLogExchange::update_sharding_conds(AllocExchContext& ctx)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> new_weak_exprs;
  ObSEArray<ObRawExpr*, 4> new_sharding_conds;
  for (int64_t i = 0; OB_SUCC(ret) && i < ctx.weak_part_exprs_.count(); ++i) {
    ObRawExpr* expr = ctx.weak_part_exprs_.at(i);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is null", K(ret), K(expr));
    } else if (get_table_set().is_superset(expr->get_relation_ids())) {
      // remove
    } else if (OB_FAIL(new_weak_exprs.push_back(expr))) {
      LOG_WARN("failed to push back expr", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < ctx.sharding_conds_.count(); ++i) {
    ObRawExpr* expr = ctx.sharding_conds_.at(i);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is null", K(ret), K(expr));
    } else if (get_table_set().is_superset(expr->get_relation_ids()) && !expr->has_flag(CNT_SET_OP)) {
      // remove
    } else if (OB_FAIL(new_sharding_conds.push_back(expr))) {
      LOG_WARN("failed to push back expr", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ctx.weak_part_exprs_.assign(new_weak_exprs))) {
      LOG_WARN("failed to assign new weak part exprs", K(ret));
    } else if (OB_FAIL(ctx.sharding_conds_.assign(new_sharding_conds))) {
      LOG_WARN("failed to assign new sharding conditions", K(ret));
    }
  }
  LOG_TRACE("exchange info", K(exch_info_));
  return ret;
}

int ObLogExchange::set_exchange_info(ObExchangeInfo& exch_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(exch_info_.clone(exch_info))) {
    LOG_WARN("failed to clone exchange info", K(exch_info), K(ret));
  }
  LOG_TRACE("exchange info", K(exch_info.hash_dist_exprs_), K(exch_info_.hash_dist_exprs_));
  return ret;
}

int ObLogExchange::allocate_expr_pre(ObAllocExprContext& ctx)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 8> exprs;
  uint64_t producer_id = OB_INVALID_ID;
  if (OB_FAIL(get_next_producer_id(get_child(first_child), producer_id))) {
    LOG_WARN("failed to get next producer id", K(ret));
  } else if (OB_FAIL(append(exprs, exch_info_.repartition_keys_))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else if (OB_FAIL(append(exprs, exch_info_.repartition_sub_keys_))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else {

    for (int64_t i = 0; OB_SUCC(ret) && i < exch_info_.hash_dist_exprs_.count(); i++) {
      if (OB_ISNULL(exch_info_.hash_dist_exprs_.at(i).expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(exprs.push_back(exch_info_.hash_dist_exprs_.at(i).expr_))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else { /*do nothing*/
      }
    }
    if (OB_SUCC(ret) && is_merge_sort_) {
      for (int64_t i = 0; OB_SUCC(ret) && i < sort_keys_.count(); i++) {
        if (OB_ISNULL(sort_keys_.at(i).expr_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (OB_FAIL(exprs.push_back(sort_keys_.at(i).expr_))) {
          LOG_WARN("failed to push back exprs", K(ret));
        } else { /*do nothing*/
        }
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
    if (OB_SUCC(ret) && is_producer_) {
      ObIArray<ExprProducer>& exprs = ctx.expr_producers_;
      for (int64_t i = 0; i < exprs.count() && OB_SUCC(ret); ++i) {
        if (T_PDML_PARTITION_ID == exprs.at(i).expr_->get_expr_type()) {
          if (exprs.at(i).producer_id_ == OB_INVALID_ID) {
            const ObPseudoColumnRawExpr* pseudo_expr = static_cast<const ObPseudoColumnRawExpr*>(exprs.at(i).expr_);
            if (exch_info_.repartition_ref_table_id_ == pseudo_expr->get_table_id()) {
              exprs.at(i).producer_id_ = id_;
              LOG_TRACE("find pdml partition id expr producer", K(id_), K(*pseudo_expr), K(get_name()));
            }
          } else {
            LOG_TRACE("pdml partition id expr has be produced", K(id_), K(exprs.at(i)));
          }
        }
      }
    }
  }
  return ret;
}

int ObLogExchange::px_pipe_blocking_pre(ObPxPipeBlockingCtx& ctx)
{
  typedef ObPxPipeBlockingCtx::OpCtx OpCtx;
  int ret = OB_SUCCESS;
  if (!is_producer()) {
    ret = ObLogicalOperator::px_pipe_blocking_pre(ctx);
  } else {
    auto child = get_child(first_child);
    OpCtx* op_ctx = static_cast<OpCtx*>(traverse_ctx_);
    if (OB_ISNULL(op_ctx) || OB_ISNULL(child)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("traverse ctx is NULL or first child of exchange is NULL", K(ret));
    } else if (OB_ISNULL(child->get_traverse_ctx())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("traverse ctx is NULL", K(ret));
    } else {
      auto child_op_ctx = static_cast<OpCtx*>(child->get_traverse_ctx());
      op_ctx->dfo_depth_ += 1;
      child_op_ctx->dfo_depth_ = op_ctx->dfo_depth_;
      child_op_ctx->out_.set_exch(true);
    }
  }
  return ret;
}

int ObLogExchange::px_pipe_blocking_post(ObPxPipeBlockingCtx& ctx)
{
  typedef ObPxPipeBlockingCtx::OpCtx OpCtx;
  int ret = OB_SUCCESS;
  if (!is_producer()) {
    ret = ObLogicalOperator::px_pipe_blocking_post(ctx);
  } else {
    OpCtx* op_ctx = static_cast<OpCtx*>(traverse_ctx_);
    auto child = get_child(first_child);
    if (OB_ISNULL(op_ctx) || OB_ISNULL(child)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("traverse ctx is NULL or first child of exchange is NULL", K(ret));
    } else if (OB_ISNULL(child->get_traverse_ctx())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("traverse ctx is NULL", K(ret));
    } else {
      op_ctx->in_.set_exch(true);
      op_ctx->has_dfo_below_ = true;
      auto child_op_ctx = static_cast<OpCtx*>(child->get_traverse_ctx());
      // Top DFO (dfo_depth_ == 0) write to PX, no need to add material.
      if (child_op_ctx->in_.is_exch() && op_ctx->dfo_depth_ > 0) {
        if (child->get_type() == log_op_def::LOG_DISTINCT &&
            static_cast<ObLogDistinct*>(child)->get_algo() == HASH_AGGREGATE) {
          static_cast<ObLogDistinct*>(child)->set_block_mode(true);
          LOG_DEBUG("distinct block mode", K(lbt()));
        } else if (OB_FAIL(allocate_material(first_child))) {
          LOG_WARN("allocate material failed", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        LOG_TRACE("pipe blocking ctx", K(get_name()), K(*op_ctx));
      }
    }
  }

  return ret;
}

int ObLogExchange::allocate_granule_pre(AllocGIContext& ctx)
{
  int ret = OB_SUCCESS;
  if (is_producer() && my_plan_->get_optimizer_context().get_parallel_rule() == PXParallelRule::MANUAL_TABLE_DOP) {
    int64_t current_dfo_dop = exch_info_.px_dop_;
    if (current_dfo_dop < 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("current dfo dop is invalid", K(ret), K(current_dfo_dop));
    } else if (OB_FAIL(ctx.push_current_dfo_dop(current_dfo_dop))) {
      LOG_WARN("failed to push current dfo dop", K(ret), K(current_dfo_dop));
    } else {
      LOG_TRACE("current dfo dop is ", K(current_dfo_dop));
    }
  }

  if (OB_SUCC(ret)) {
    gi_info_.set_info(ctx);
    LOG_TRACE("GI pre store state", K(gi_info_));
    IGNORE_RETURN ctx.reset_info();
    // remote exchange operator don't need GI
    if (!is_remote_) {
      ctx.add_exchange_op_count();
    }
  }

  return ret;
}

int ObLogExchange::allocate_granule_post(AllocGIContext& ctx)
{
  int ret = OB_SUCCESS;
  if (is_producer() && my_plan_->get_optimizer_context().get_parallel_rule() == PXParallelRule::MANUAL_TABLE_DOP) {
    if (OB_FAIL(ctx.pop_current_dfo_dop())) {
      LOG_WARN("failed to pop current dfo dop", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    gi_info_.get_info(ctx);
    LOG_TRACE("GI post reset store state", K(gi_info_));
    if (!is_remote_) {
      ctx.delete_exchange_op_count();
    }
  }

  return ret;
}

uint64_t ObLogExchange::hash(uint64_t seed) const
{
  seed = do_hash(is_producer_, seed);
  seed = do_hash(is_remote_, seed);
  seed = do_hash(is_merge_sort_, seed);
  HASH_ARRAY(sort_keys_, seed);
  seed = do_hash(exch_info_, seed);
  seed = ObLogicalOperator::hash(seed);

  return seed;
}

int ObLogExchange::inner_append_not_produced_exprs(ObRawExprUniqueSet& raw_exprs) const
{
  int ret = OB_SUCCESS;
  OZ(raw_exprs.append(exch_info_.calc_part_id_expr_));

  return ret;
}
