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
#include "sql/optimizer/ob_log_table_scan.h"
#include "sql/optimizer/ob_log_subplan_filter.h"
#include "sql/optimizer/ob_log_join.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "sql/optimizer/ob_log_plan.h"
#include "sql/optimizer/ob_opt_est_cost.h"
#include "sql/optimizer/ob_log_distinct.h"
#include "common/ob_smart_call.h"
#include "sql/optimizer/ob_join_order.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

int ObLogExchange::get_explain_name_internal(char *buf,
                                             const int64_t buf_len,
                                             int64_t &pos)
{
  int ret = OB_SUCCESS;
  ret = BUF_PRINTF("%s ", get_name());
  if (OB_SUCC(ret)) {
    if (is_remote_) {
      ret = BUF_PRINTF("REMOTE");
    } else if (is_rescanable()) {
      //PX COORDINATOR, do nothing
    } else {
      ret = BUF_PRINTF("DISTR");
    }
  }
  if (OB_SUCC(ret) && is_producer() &&
      (is_repart_exchange() || is_pq_dist())) {
    ret = BUF_PRINTF(" (");
    if (OB_FAIL(ret)){
    } else if (is_repart_exchange()) {
      if (OB_SUCC(ret) && dist_method_ == ObPQDistributeMethod::PARTITION_RANDOM) {
        ret = BUF_PRINTF("PKEY RANDOM");
      } else if (OB_SUCC(ret) && dist_method_ == ObPQDistributeMethod::PARTITION_HASH) {
        ret = BUF_PRINTF("PKEY HASH");
      } else if (OB_SUCC(ret) && dist_method_ == ObPQDistributeMethod::PARTITION_RANGE) {
        ret = BUF_PRINTF("PKEY RANGE");
      } else if (OB_SUCC(ret) && dist_method_ == ObPQDistributeMethod::RANGE) {
        ret = BUF_PRINTF("RANGE");
      } else {
        ret = BUF_PRINTF("PKEY");
      }
    } else if (is_pq_dist()) {
      auto print_method = (ObPQDistributeMethod::SM_BROADCAST == dist_method_)
          ? ObPQDistributeMethod::BROADCAST : dist_method_;
      print_method = (ObPQDistributeMethod::PARTITION_HASH == dist_method_)
          ? ObPQDistributeMethod::HASH : print_method;
      print_method = (ObPQDistributeMethod::HYBRID_HASH_BROADCAST == dist_method_)
            ? ObPQDistributeMethod::HASH : print_method;
      print_method = (ObPQDistributeMethod::HYBRID_HASH_RANDOM == dist_method_)
            ? ObPQDistributeMethod::HASH : print_method;
      const char *str = ObPQDistributeMethod::get_type_string(print_method);
      ret = BUF_PRINTF("%s", str);
    }
    if (OB_SUCC(ret)) {
      if (is_slave_mapping()) {
        ret = BUF_PRINTF(" LOCAL");
      } else if (is_rollup_hybrid_ || is_wf_hybrid_
                || ObPQDistributeMethod::HYBRID_HASH_RANDOM == dist_method_
                || ObPQDistributeMethod::HYBRID_HASH_BROADCAST == dist_method_) {
        ret = BUF_PRINTF(" HYBRID");
      }
    }
    if (OB_SUCC(ret)) {
      ret = BUF_PRINTF(")");
    }
  }
  return ret;
}

const char *ObLogExchange::get_name() const
{
  static const char *exchange_type[8] =
  {
    "EXCHANGE OUT",
    "EXCHANGE IN",
    "EXCHANGE IN TASK ORDER",
    "EXCHANGE IN MERGE SORT",
    "PX COORDINATOR",
    "PX COORDINATOR TASK ORDER",
    "PX COORDINATOR MERGE SORT",
    "PX ORDERED COORDINATOR"
  };
  int offset = is_rescanable() && !is_remote_ ? 3 : 0;
  const char *result = exchange_type[0];
  if (is_producer()) {
    result = exchange_type[0];
  } else if (is_merge_sort()) {
    result = exchange_type[3 + offset];
  } else if (is_task_order()) {
    result = exchange_type[7];
  } else {
    result = exchange_type[1 + offset];
  }
  return result;
}

int ObLogExchange::inner_replace_op_exprs(ObRawExprReplacer &replacer)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(replace_exprs_action(replacer, repartition_keys_))) {
    LOG_WARN("failed to replace agg exprs", K(ret));
  } else if (OB_FAIL(replace_exprs_action(replacer, repartition_sub_keys_))) {
    LOG_WARN("failed to replace agg exprs", K(ret));
  } else if (OB_FAIL(replace_exprs_action(replacer, repartition_func_exprs_))) {
    LOG_WARN("failed to replace agg exprs", K(ret));
  } else if (calc_part_id_expr_ != NULL
             && OB_FAIL(replace_expr_action(replacer, calc_part_id_expr_))) {
    LOG_WARN("failed to replace calc part id expr", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < hash_dist_exprs_.count(); i++) {
      if (OB_ISNULL(hash_dist_exprs_.at(i).expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(replace_expr_action(replacer, hash_dist_exprs_.at(i).expr_))) {
        LOG_WARN("failed to replace agg exprs", K(ret));
      } else { /*do nothing*/ }
    }
    for(int64_t i = 0; OB_SUCC(ret) && i < sort_keys_.count(); ++i) {
      OrderItem &cur_order_item = sort_keys_.at(i);
      if (OB_FAIL(replace_expr_action(replacer, cur_order_item.expr_))) {
        LOG_WARN("failed to resolve ref params in sort key ", K(cur_order_item), K(ret));
      } else { /* Do nothing */ }
    }
  }
  return ret;
}

int ObLogExchange::get_plan_item_info(PlanText &plan_text,
                                      ObSqlPlanItem &plan_item)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObLogicalOperator::get_plan_item_info(plan_text, plan_item))) {
    LOG_WARN("failed to get plan item info", K(ret));
  } else if (OB_FAIL(get_plan_special_expr_info(plan_text, plan_item))) {
    LOG_WARN("failed to get plan special expr info", K(ret));
  } else {
    BEGIN_BUF_PRINT;
    if (OB_FAIL(get_explain_name_internal(buf, buf_len, pos))) {
      LOG_WARN("failed to get explain name", K(ret));
    }
    END_BUF_PRINT(plan_item.operation_, plan_item.operation_len_);
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(get_plan_distribution(plan_text, plan_item))) {
      LOG_WARN("failed to get plan distribution", K(ret));
    }
  }
  if (OB_SUCC(ret) && OB_INVALID_ID != get_dfo_id()) {
    BEGIN_BUF_PRINT;
    if (OB_FAIL(BUF_PRINTF(":EX%ld%04ld",
                            get_px_id(),
                            get_dfo_id()))) {
      LOG_WARN("failed to print str", K(ret));
    }
    END_BUF_PRINT(plan_item.object_alias_,
                  plan_item.object_alias_len_);
  }
  return ret;
}


int ObLogExchange::get_plan_special_expr_info(PlanText &plan_text,
                                              ObSqlPlanItem &plan_item)
{
  int ret = OB_SUCCESS;
  int parallel = get_parallel();
  BEGIN_BUF_PRINT;
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (is_producer()) {
    if (is_repart_exchange()) {
      ObSEArray<ObRawExpr *, 16> exprs;
      OZ(append(exprs, repartition_keys_));
      OZ(append(exprs, repartition_sub_keys_));
      if (OB_SUCC(ret)) {
        ret = print_annotation_keys(buf,
                                    buf_len,
                                    pos,
                                    type,
                                    exprs);
      }
    }
    if (OB_SUCC(ret) && is_pq_hash_dist()) {
      ObSEArray<ObRawExpr *, 16> exprs;
      FOREACH_CNT_X(e, hash_dist_exprs_, OB_SUCC(ret)) {
        OZ(exprs.push_back(e->expr_));
      }
      if (OB_SUCC(ret)) {
        ret = print_annotation_keys(buf,
                                    buf_len,
                                    pos,
                                    type,
                                    exprs);
      }
    }
    if (OB_SUCC(ret) && is_pq_range()) {
      ObSEArray<ObRawExpr *, 16> exprs;
      FOREACH_CNT_X(sk, sort_keys_, OB_SUCC(ret)) {
        OZ(exprs.push_back(sk->expr_));
      }
      if (OB_SUCC(ret)) {
        ret = print_annotation_keys(buf,
                                    buf_len,
                                    pos,
                                    type,
                                    exprs);
      }
    }
    if (is_px_single() &&
        OB_PHY_PLAN_REMOTE != get_plan()->get_optimizer_context().get_phy_plan_type()) {
      ret = BUF_PRINTF("is_single, ");
    }
    if (OB_SUCC(ret) && parallel > 0 &&
        OB_PHY_PLAN_REMOTE != get_plan()->get_optimizer_context().get_phy_plan_type()) {
      ret = BUF_PRINTF("dop=%d", parallel);
    }
    if (OB_SUCC(ret) && EXPLAIN_EXTENDED == type && popular_values_.count() > 0) {
      if (OB_FAIL(BUF_PRINTF(",\n      "))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      } else {
        EXPLAIN_PRINT_POPULAR_VALUES(popular_values_);
      }
    }
  } else {
    if (is_task_order_) {
      ret = BUF_PRINTF("task_order");
    } else if (is_merge_sort_) {
      const ObIArray<OrderItem> &sort_keys = get_sort_keys();
      EXPLAIN_PRINT_SORT_ITEMS(sort_keys, type);
      if (OB_SUCC(ret) && is_sort_local_order_) {
        ret = BUF_PRINTF(", Local Order");
      }
    }
  }
  END_BUF_PRINT(plan_item.special_predicates_,
                plan_item.special_predicates_len_);
  return ret;
}

int ObLogExchange::get_plan_distribution(PlanText &plan_text,
                                         ObSqlPlanItem &plan_item)
{
  int ret = OB_SUCCESS;
  if (is_producer()) {
    BEGIN_BUF_PRINT;
    if (is_repart_exchange()) {
      if (OB_SUCC(ret) && dist_method_ == ObPQDistributeMethod::PARTITION_RANDOM) {
        ret = BUF_PRINTF("PKEY RANDOM");
      } else if (OB_SUCC(ret) && dist_method_ == ObPQDistributeMethod::PARTITION_HASH) {
        ret = BUF_PRINTF("PKEY HASH");
      } else if (OB_SUCC(ret) && dist_method_ == ObPQDistributeMethod::PARTITION_RANGE) {
        ret = BUF_PRINTF("PKEY RANGE");
      } else if (OB_SUCC(ret) && dist_method_ == ObPQDistributeMethod::RANGE) {
        ret = BUF_PRINTF("RANGE)");
      } else {
        ret = BUF_PRINTF("PKEY");
      }
    } else {
      if (is_pq_dist()) {
        auto print_method = (ObPQDistributeMethod::SM_BROADCAST == dist_method_)
            ? ObPQDistributeMethod::BROADCAST : dist_method_;
        print_method = (ObPQDistributeMethod::PARTITION_HASH == dist_method_)
            ? ObPQDistributeMethod::HASH : print_method;
        const char *str = ObPQDistributeMethod::get_type_string(print_method);
        ret = BUF_PRINTF("%s", str);
      }
    }

    if (OB_SUCC(ret) && (is_pq_dist() || is_repart_exchange())) {
      if (is_slave_mapping()) {
        ret = BUF_PRINTF(" LOCAL)");
      } else {
        if (is_rollup_hybrid_ || is_wf_hybrid_) {
          ret = BUF_PRINTF(" HYBRID");
        }
      }
    }
    END_BUF_PRINT(plan_item.distribution_, plan_item.distribution_len_);
  }
  return ret;
}

int ObLogExchange::print_annotation_keys(char *buf,
                                        int64_t &buf_len,
                                        int64_t &pos,
                                        ExplainType type,
                                        const ObIArray<ObRawExpr *> &keys)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(BUF_PRINTF("(#keys=%ld", keys.count()))) {
    LOG_WARN("buf print failed", K(ret));
  } else if (keys.count() == 0) {
    if (OB_FAIL(BUF_PRINTF("), "))) {
      LOG_WARN("failed print ),", K(ret));
    }
  } else {
    if (OB_FAIL(BUF_PRINTF(", "))) {
      LOG_WARN("failed print , ", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < keys.count() && OB_SUCC(ret); i++) {
    auto key = keys.at(i);
    if (OB_FAIL(BUF_PRINTF("["))) {
      LOG_WARN("failed print [", K(ret));
    } else if (OB_ISNULL(key)) {
      ret = BUF_PRINTF("nil");
    } else if (OB_FAIL(key->get_name(buf, buf_len, pos, type))) {
      LOG_WARN("print expr name failed", K(ret));
    } else if (key->get_result_type().is_numeric_type()) {
      //屏蔽numeric的打印
    } else if (OB_FAIL(key->get_type_and_length(buf, buf_len, pos, type))) {
      LOG_WARN("print expr type and length failed", K(ret));
    } else { /*Do nothing*/ }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(BUF_PRINTF("]"))) {
      LOG_WARN("failed print ]", K(ret));
    } else if(i == keys.count() - 1) {
      if (OB_FAIL(BUF_PRINTF("), "))) {
        LOG_WARN("failed to BUF_PRINTF", K(ret));
      }
    } else if (OB_FAIL(BUF_PRINTF(", "))) {
      LOG_WARN("failed to BUF_PRINTF", K(ret));
    }
  }
  return ret;
}

int ObLogExchange::compute_op_ordering()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *child = NULL;
  if (OB_ISNULL(child = get_child(ObLogicalOperator::first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(ObLogicalOperator::compute_op_ordering())) {
    LOG_WARN("failed to compute op ordering", K(ret));
  } else if (is_producer()) {
    // for FULL_INPUT_SAMPLE, we cache all rows in transmit and send in random range
    // to avoid send to one worker at one time if input order is the same with %sort_keys_
    is_local_order_ = FULL_INPUT_SAMPLE == sample_type_;
  } else if (is_consumer()) {
    if (is_merge_sort_) {
      if (OB_UNLIKELY(sort_keys_.empty())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("empty sort keys", K(ret));
      } else if (OB_FAIL(set_op_ordering(sort_keys_))) {
        LOG_WARN("failed to assign exprs", K(ret));
      } else {
        is_local_order_ = false;
        is_range_order_ = false;
      }
    } else if (!get_op_ordering().empty() && child->is_distributed()) {
      is_local_order_ = true;
      is_range_order_ = false;
    } else { /*do nothing*/ }
  } else { /*do nothing*/ }
  return ret;
}

int ObLogExchange::compute_op_parallel_and_server_info()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* child = NULL;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(child = get_child(first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(get_plan()), K(child));
  } else if (is_producer()) {
    if (OB_FAIL(ObLogicalOperator::compute_op_parallel_and_server_info())) {
      LOG_WARN("failed to compute sharding info for producer exchange", K(ret));
    } else { /*do nothing*/ }
  } else if (is_pq_local()) {
    set_parallel(ObGlobalHint::DEFAULT_PARALLEL);
    set_available_parallel(child->get_parallel());
    set_server_cnt(1);
    static_cast<ObLogExchange*>(child)->set_in_server_cnt(1);
    get_server_list().reuse();
    if (OB_FAIL(get_server_list().push_back(get_plan()->get_optimizer_context().get_local_server_addr()))) {
      LOG_WARN("failed to push back server list", K(ret));
    }
  } else {
    // set_exchange_info not set these info, use child parallel and server info.
    if (OB_FAIL(ret)) {
    } else if (is_pq_hash_dist()) {
      server_list_.reuse();
      common::ObAddr all_server_list;
      all_server_list.set_max(); // a special ALL server list indicating hash data distribution
      if (OB_FAIL(server_list_.push_back(all_server_list))) {
        LOG_WARN("failed to assign all server list", K(ret));
      }
    } else if (get_server_list().empty() && OB_FAIL(get_server_list().assign(child->get_server_list()))) {
      LOG_WARN("failed to assign server list", K(ret));
    }
    if (OB_FAIL(ret)) {
    } else if (0 == get_server_cnt()) {
      set_server_cnt(child->get_server_cnt());
    } else {
      static_cast<ObLogExchange*>(child)->set_in_server_cnt(get_server_cnt());
    }
    if (OB_SUCC(ret) && ObGlobalHint::DEFAULT_PARALLEL > get_parallel()) {
      // parallel not set when allocate exchange, and not pull to local
      if (OB_UNLIKELY(child->is_match_all())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected exchange above match all sharding", K(ret));
      } else if (child->is_single()) {
        set_parallel(child->get_available_parallel());
      } else {
        set_parallel(child->get_parallel());
      }
    }
  }
  return ret;
}

int ObLogExchange::est_cost()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *child = NULL;
  if (OB_ISNULL(child = get_child(ObLogicalOperator::first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(child), K(ret));
  } else if (OB_FAIL(inner_est_cost(get_parallel(), child->get_card(), op_cost_))) {
    LOG_WARN("failed to est exchange cost", K(ret));
  } else {
    set_cost(op_cost_ + child->get_cost());
    set_card(child->get_card());
  }
  return ret;
}

int ObLogExchange::do_re_est_cost(EstimateCostInfo &param, double &card, double &op_cost, double &cost)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *child = NULL;
  if (OB_ISNULL(child = get_child(ObLogicalOperator::first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(child), K(ret));
  } else {
    double child_card = child->get_card();
    double child_cost = child->get_cost();
    const int64_t parallel = param.need_parallel_;
    param.need_parallel_ = ObGlobalHint::UNSET_PARALLEL;
    if (is_block_op()) {
      param.need_row_count_ = -1; //reset need row count
    }
    if (OB_FAIL(SMART_CALL(child->re_est_cost(param, child_card, child_cost)))) {
      LOG_WARN("failed to re est exchange cost", K(ret));
    } else if (OB_FAIL(inner_est_cost(parallel, child_card, op_cost))) {
      LOG_WARN("failed to est exchange cost", K(ret));
    } else {
      cost = child_cost + op_cost;
      card = child_card;
    }
  }
  return ret;
}

int ObLogExchange::inner_est_cost(int64_t parallel, double child_card, double &op_cost)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *child = NULL;
  if (OB_ISNULL(get_plan()) ||
      OB_ISNULL(child = get_child(ObLogicalOperator::first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(child), K(ret));
  } else if (OB_UNLIKELY(1 > parallel)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected parallel degree", K(parallel), K(ret));
  } else if (is_producer()) {
    ObOptimizerContext &opt_ctx = get_plan()->get_optimizer_context();
    ObExchOutCostInfo est_cost_info(child_card,
                                    child->get_width(),
                                    dist_method_,
                                    parallel,
                                    get_in_server_cnt());
    if (OB_FAIL(ObOptEstCost::cost_exchange_out(est_cost_info,
                                                op_cost,
                                                opt_ctx.get_cost_model_type()))) {
      LOG_WARN("failed to cost exchange out", K(ret));
    }
  } else {
    ObOptimizerContext &opt_ctx = get_plan()->get_optimizer_context();
    ObExchInCostInfo est_cost_info(child_card,
                                   child->get_width(),
                                   dist_method_,
                                   parallel,
                                   get_server_cnt(),
                                   is_local_order_,
                                   sort_keys_);
    if (OB_FAIL(ObOptEstCost::cost_exchange_in(est_cost_info,
                                               op_cost,
                                               opt_ctx.get_cost_model_type()))) {
      LOG_WARN("failed to cost exchange in", K(ret));
    }
  }
  return ret;
}

int ObLogExchange::compute_sharding_info()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (is_producer()) {
    ObLogicalOperator *child = NULL;
    if (OB_FAIL(ObLogicalOperator::compute_sharding_info())) {
      LOG_WARN("failed to compute sharding info for producer exchange", K(ret));
    } else { /*do nothing*/ }
  } else {
    if (NULL != get_sharding()) {
      /* for hash-hash, repartition, broadcast, will be set separately*/
    } else if (is_pq_local()) {
      // for pull to local
      strong_sharding_ = get_plan()->get_optimizer_context().get_local_sharding();
    } else {
      // for pdml and union-all
      strong_sharding_ = get_plan()->get_optimizer_context().get_distributed_sharding();
    }
  }
  return ret;
}

int ObLogExchange::compute_plan_type()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *child = NULL;
  if (OB_ISNULL(child = get_child(first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    exchange_allocated_ = true;
    location_type_ = child->get_location_type();
    phy_plan_type_ =
        get_is_remote() ? ObPhyPlanType::OB_PHY_PLAN_REMOTE : ObPhyPlanType::OB_PHY_PLAN_DISTRIBUTED;
  }
  return ret;
}

int ObLogExchange::set_exchange_info(const ObExchangeInfo &exch_info)
{
  int ret = OB_SUCCESS;
  is_remote_ = exch_info.is_remote_;
  dist_method_ = exch_info.dist_method_;
  unmatch_row_dist_method_ = exch_info.unmatch_row_dist_method_;
  null_row_dist_method_ = exch_info.null_row_dist_method_;
  slave_mapping_type_ = exch_info.slave_mapping_type_;
  if (is_producer()) {
    in_server_cnt_ = exch_info.server_cnt_;
    slice_count_ = exch_info.slice_count_;
    repartition_type_ = exch_info.repartition_type_;
    repartition_ref_table_id_ = exch_info.repartition_ref_table_id_;
    repartition_table_id_ = exch_info.repartition_table_id_;
    repartition_table_name_ = exch_info.repartition_table_name_;
    if (OB_FAIL(repart_all_tablet_ids_.assign(exch_info.repart_all_tablet_ids_))) {
      LOG_WARN("failed to assign repart part ids", K(ret));
    } else if (OB_FAIL(repartition_keys_.assign(exch_info.repartition_keys_))) {
      LOG_WARN("failed to assign repartition keys", K(ret));
    } else if (OB_FAIL(repartition_sub_keys_.assign(exch_info.repartition_sub_keys_))) {
      LOG_WARN("failed to assign repartition sub keys", K(ret));
    } else if (OB_FAIL(repartition_func_exprs_.assign(exch_info.repartition_func_exprs_))) {
      LOG_WARN("failed to assign part func exprs", K(ret));
    } else if (OB_FAIL(hash_dist_exprs_.assign(exch_info.hash_dist_exprs_))) {
      LOG_WARN("array assign failed", K(ret));
    } else if (OB_FAIL(popular_values_.assign(exch_info.popular_values_))) {
      LOG_WARN("array assign failed", K(ret));
    } else if ((dist_method_ == ObPQDistributeMethod::RANGE ||
                dist_method_ == ObPQDistributeMethod::PARTITION_RANGE) &&
                OB_FAIL(sort_keys_.assign(exch_info.sort_keys_))) {
      LOG_WARN("failed to assign sort keys", K(ret));
    } else {
      is_rollup_hybrid_ = exch_info.is_rollup_hybrid_;
      need_null_aware_shuffle_ = exch_info.need_null_aware_shuffle_;
      calc_part_id_expr_ = exch_info.calc_part_id_expr_;
      is_wf_hybrid_ = exch_info.is_wf_hybrid_;
      if (is_wf_hybrid_) {
        wf_hybrid_aggr_status_expr_ = exch_info.wf_hybrid_aggr_status_expr_;
        if (OB_FAIL(wf_hybrid_pby_exprs_cnt_array_.assign(
                    exch_info.wf_hybrid_pby_exprs_cnt_array_))) {
          LOG_WARN("array assign failed", K(ret), K(exch_info.wf_hybrid_pby_exprs_cnt_array_));
        }
      }
    }
  } else { // consumer
    if ((exch_info.is_merge_sort_
         || (dist_method_ != ObPQDistributeMethod::RANGE
             && dist_method_ != ObPQDistributeMethod::PARTITION_RANGE))
        && OB_FAIL(sort_keys_.assign(exch_info.sort_keys_))) {
      LOG_WARN("failed to assign sort keys", K(ret));
    } else if (OB_FAIL(weak_sharding_.assign(exch_info.weak_sharding_))) {
      LOG_WARN("failed to assign weak sharding", K(ret));
    } else if (OB_FAIL(server_list_.assign(exch_info.server_list_))) {
      LOG_WARN("failed to assign server list", K(ret));
    } else {
      if (exch_info.is_wf_hybrid_) {
        strong_sharding_ = get_plan()->get_optimizer_context().get_distributed_sharding();
      } else {
        strong_sharding_ = exch_info.strong_sharding_;
      }
      parallel_ = exch_info.parallel_;
      server_cnt_ = exch_info.server_cnt_;
      is_task_order_ = exch_info.is_task_order_;
      is_merge_sort_ = exch_info.is_merge_sort_;
      is_sort_local_order_ = exch_info.is_sort_local_order_;
      repartition_table_id_ = exch_info.repartition_table_id_;
    }
  }
  return ret;
}

int ObLogExchange::get_op_exprs(ObIArray<ObRawExpr*> &all_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(append(all_exprs, repartition_keys_))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else if (OB_FAIL(append_array_no_dup(all_exprs, repartition_sub_keys_))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else if (NULL != calc_part_id_expr_ && OB_FAIL(all_exprs.push_back(calc_part_id_expr_))) {
    LOG_WARN("failed to push back exprs", K(ret));
  } else if (NULL != partition_id_expr_ && OB_FAIL(all_exprs.push_back(partition_id_expr_))) {
    LOG_WARN("failed to push back expr", K(ret));
  } else if (NULL != random_expr_ && OB_FAIL(all_exprs.push_back(random_expr_))) {
    LOG_WARN("failed to push back expr", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < hash_dist_exprs_.count(); i++) {
      if (OB_ISNULL(hash_dist_exprs_.at(i).expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(add_var_to_array_no_dup(all_exprs, hash_dist_exprs_.at(i).expr_))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else { /*do nothing*/ }
    }
    if (OB_SUCC(ret) && !sort_keys_.empty() && !is_task_order_) {
      for (int64_t i = 0; OB_SUCC(ret) && i < sort_keys_.count(); i++) {
        if (OB_ISNULL(sort_keys_.at(i).expr_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (OB_FAIL(add_var_to_array_no_dup(all_exprs, sort_keys_.at(i).expr_))) {
          LOG_WARN("failed to push back exprs", K(ret));
        } else { /*do nothing*/ }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObLogicalOperator::get_op_exprs(all_exprs))) {
        LOG_WARN("failed to get op exprs", K(ret));
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

int ObLogExchange::px_pipe_blocking_pre(ObPxPipeBlockingCtx &ctx)
{
  typedef ObPxPipeBlockingCtx::OpCtx OpCtx;
  int ret = OB_SUCCESS;
  if (!is_producer()) {
    ret = ObLogicalOperator::px_pipe_blocking_pre(ctx);
  } else {
    auto child = get_child(first_child);
    OpCtx *op_ctx = static_cast<OpCtx *>(traverse_ctx_);
    if (OB_ISNULL(op_ctx) || OB_ISNULL(child)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("traverse ctx is NULL or first child of exchange is NULL", K(ret));
    } else if (OB_ISNULL(child->get_traverse_ctx())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("traverse ctx is NULL", K(ret));
    } else {
      auto child_op_ctx = static_cast<OpCtx *>(child->get_traverse_ctx());
      op_ctx->dfo_depth_ += 1;
      child_op_ctx->dfo_depth_ = op_ctx->dfo_depth_;
      child_op_ctx->out_.set_exch(true);
      LOG_TRACE("pipe blocking ctx", K(get_name()), K(*op_ctx));
    }
  }
  return ret;
}

int ObLogExchange::px_pipe_blocking_post(ObPxPipeBlockingCtx &ctx)
{
  typedef ObPxPipeBlockingCtx::OpCtx OpCtx;
  int ret = OB_SUCCESS;
  if (!is_producer()) {
    ret = ObLogicalOperator::px_pipe_blocking_post(ctx);
  } else {
    OpCtx *op_ctx = static_cast<OpCtx *>(traverse_ctx_);
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
      auto child_op_ctx = static_cast<OpCtx *>(child->get_traverse_ctx());
      // Top DFO (dfo_depth_ == 0) write to PX, no need to add material.
      // it's TOP DFO and the exchange is merge sort coord, we need add material to unblock channel
      ObLogExchange *exchange_in = static_cast<ObLogExchange*>(get_parent());
      if (OB_ISNULL(exchange_in)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected status: the parent of exchange out is not exchange in", K(ret));
      } else if (child_op_ctx->in_.is_exch() &&
          (op_ctx->dfo_depth_ > 0 || exchange_in->is_merge_sort())) {
        if (child->get_type() == log_op_def::LOG_DISTINCT
            && static_cast<ObLogDistinct*>(child)->get_algo() ==  HASH_AGGREGATE
            && !static_cast<ObLogDistinct *>(child)->is_push_down()) {
          static_cast<ObLogDistinct*>(child)->set_block_mode(true);
          LOG_DEBUG("distinct block mode", K(lbt()));
        } else if (OB_FAIL(allocate_material(first_child))) {
          LOG_WARN("allocate material failed", K(ret));
        }
      } else if (!child_op_ctx->in_.is_exch() && dist_method_ == ObPQDistributeMethod::RANGE) {
        if (child->get_type() == log_op_def::LOG_JOIN) {
          bool has_non_block_shj = false;
          if (OB_FAIL(child->has_block_parent_for_shj(has_non_block_shj))) {
            LOG_WARN("failed to proces block parent", K(ret));
          } else if (has_non_block_shj) {
            if (OB_FAIL(allocate_material(first_child))) {
              LOG_WARN("allocate material failed", K(ret));
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        // only for compatibility low level version
        exchange_in->set_old_unblock_mode(false);
        LOG_TRACE("pipe blocking ctx", K(get_name()), K(*op_ctx));
      }
    }
  }
  return ret;
}

int ObLogExchange::allocate_granule_pre(AllocGIContext &ctx)
{
  int ret = OB_SUCCESS;
  gi_info_.set_info(ctx);
  LOG_TRACE("GI pre store state", K(gi_info_));
  IGNORE_RETURN ctx.reset_info();
  // remote exchange operator don't need GI
  if (!is_remote_) {
    ctx.add_exchange_op_count();
  }
  return ret;
}

int ObLogExchange::allocate_granule_post(AllocGIContext &ctx)
{
  int ret = OB_SUCCESS;
  gi_info_.get_info(ctx);
  LOG_TRACE("GI post reset store state", K(gi_info_));
  if (!is_remote_) {
    ctx.delete_exchange_op_count();
  }
  return ret;
}

uint64_t ObLogExchange::hash(uint64_t seed) const
{
   seed = do_hash(is_producer_, seed);
   seed = do_hash(dist_method_, seed);
   seed = ObLogicalOperator::hash(seed);
   return seed;
}

int ObLogExchange::gen_px_pruning_table_locations()
{
  int ret = OB_SUCCESS;
  int64_t count = 0;
  ObSEArray<const ObDMLStmt *, 2>stmts;
  ObSEArray<int64_t, 2>drop_expr_idxs;
  ObArray<ObTableLocation> table_locations;
  int64_t cur_idx = 0;
  if (OB_ISNULL(get_child(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child is null", K(ret));
  } else if (OB_FAIL(prepare_px_pruning_param(get_child(0), count, stmts, drop_expr_idxs))) {
    LOG_WARN("fail to calc px table location count", K(ret));
  } else if (count <= 0) {
    /*do nothing*/
  } else if (OB_FAIL(add_px_table_location(get_child(0), table_locations, drop_expr_idxs, stmts, cur_idx))) {
    LOG_WARN("fail to add px table locations", K(ret));
  } else if (!table_locations.empty()) {
    if (OB_FAIL(table_locations_.prepare_allocate(table_locations.count(),
        get_plan()->get_optimizer_context().get_allocator()))) {
      LOG_WARN("fail to init table locations", K(ret), K(count));
    } else {
      for (int i = 0; i < table_locations.count() && OB_SUCC(ret); ++i) {
        OZ(table_locations_.at(i).assign(table_locations.at(i)));
      }
    }
  }
  return ret;
}

int ObLogExchange::prepare_px_pruning_param(ObLogicalOperator *op,
    int64_t &count, common::ObIArray<const ObDMLStmt *> &stmts,
    common::ObIArray<int64_t> &drop_expr_idxs)
{
  int ret = OB_SUCCESS;
  bool stop = false;
  if (OB_ISNULL(op)) {
  } else if (log_op_def::LOG_EXCHANGE == op->get_type() &&
      static_cast<ObLogExchange *>(op)->is_rescanable()) {
    stop = true;
  } else if (log_op_def::LOG_TABLE_SCAN == op->get_type()) {
    if (OB_ISNULL(op->get_stmt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("stmt is null", K(ret));
    } else if (OB_FAIL(stmts.push_back(op->get_stmt()))) {
      LOG_WARN("fail to push back stmt", K(ret));
    } else {
      count++;
    }
    stop = true;
  } else if (log_op_def::LOG_SUBPLAN_FILTER == op->get_type() &&
             !static_cast<ObLogSubPlanFilter*>(op)->get_exec_params().empty()) {
    OZ(find_need_drop_expr_idxs(op, drop_expr_idxs, log_op_def::LOG_SUBPLAN_FILTER));
    stop = false;
  } else if (log_op_def::LOG_JOIN == op->get_type() &&
             JoinAlgo::NESTED_LOOP_JOIN == static_cast<ObLogJoin*>(op)->get_join_algo() &&
             static_cast<ObLogJoin*>(op)->is_nlj_with_param_down()) {
     OZ(find_need_drop_expr_idxs(op, drop_expr_idxs, log_op_def::LOG_JOIN));
     stop = false;
  }
  if (OB_SUCC(ret) && !stop) {
    for (int i = 0; i < op->get_num_of_child() && OB_SUCC(ret); ++i) {
      if (OB_FAIL(SMART_CALL(prepare_px_pruning_param(op->get_child(i),
          count, stmts, drop_expr_idxs)))) {
        LOG_WARN("fail to calc px table location count", K(ret));
      }
    }
  }
  return ret;
}

int ObLogExchange::add_px_table_location(ObLogicalOperator *op,
    ObIArray<ObTableLocation> &table_locations,
    common::ObIArray<int64_t> &drop_expr_idxs,
    const common::ObIArray<const ObDMLStmt *> &stmts,
    int64_t &cur_idx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(op)) {
  } else if (log_op_def::LOG_EXCHANGE == op->get_type() &&
      static_cast<ObLogExchange *>(op)->is_rescanable()) {
    /*do nothing*/
  } else if (log_op_def::LOG_TABLE_SCAN == op->get_type()) {
    ObLogTableScan *table_scan = static_cast<ObLogTableScan* >(op);
    ObSEArray<ObRawExpr *, 8>exprs;
    bool has_exec_param = false;
    OZ(find_table_location_exprs(drop_expr_idxs, table_scan->get_range_conditions(), exprs, has_exec_param));
    OZ(find_table_location_exprs(drop_expr_idxs, table_scan->get_filter_exprs(), exprs, has_exec_param));
    if (OB_SUCC(ret) && !exprs.empty() && has_exec_param) {
      SMART_VAR(ObTableLocation, table_location) {
        const ObDMLStmt *cur_stmt = NULL;
        const ObDataTypeCastParams dtc_params = ObBasicSessionInfo::create_dtc_params(
            get_plan()->get_optimizer_context().get_session_info());
        int64_t ref_table_id = table_scan->get_is_index_global() ? table_scan->get_index_table_id() :
            table_scan->get_ref_table_id();
        if (cur_idx >= stmts.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("stmts idx is unexpected", K(cur_idx), K(stmts.count()));
        } else if (FALSE_IT(cur_stmt = stmts.at(cur_idx))) {
        } else if (OB_FAIL(table_location.init(
              *get_plan()->get_optimizer_context().get_sql_schema_guard(),
              *cur_stmt,
               get_plan()->get_optimizer_context().get_exec_ctx(),
               exprs,
               table_scan->get_table_id(),
               ref_table_id,
               NULL,
               dtc_params,
               false))) {
          LOG_WARN("fail to init table location", K(ret), K(exprs));
        } else if (table_location.is_all_partition()) {
          /*do nothing*/
        } else if (FALSE_IT(table_location.set_has_dynamic_exec_param(true))) {
        } else if (OB_FAIL(table_locations.push_back(table_location))) {
          LOG_WARN("fail to push back table locations", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      cur_idx++;
    }
  } else {
    for (int i = 0; i < op->get_num_of_child() && OB_SUCC(ret); ++i) {
      if (OB_FAIL(SMART_CALL(add_px_table_location(op->get_child(i),
          table_locations, drop_expr_idxs, stmts, cur_idx)))) {
        LOG_WARN("fail to calc px table location count", K(ret));
      }
    }
  }
  return ret;
}

int ObLogExchange::find_need_drop_expr_idxs(ObLogicalOperator *op,
    common::ObIArray<int64_t> &drop_expr_idxs, log_op_def::ObLogOpType type)
{
  int ret = OB_SUCCESS;
  bool left_has_exchange = false;
  if (OB_ISNULL(op->get_child(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null op", K(ret));
  } else if (OB_FAIL(op->get_child(0)->check_has_exchange_below(left_has_exchange))) {
    LOG_WARN("fail to check has exchange below");
  } else if (!left_has_exchange) {
    if (type == log_op_def::LOG_SUBPLAN_FILTER) {
      const auto &exec_params = static_cast<ObLogSubPlanFilter *>(op)->get_exec_params();
      for (int i = 0; i < exec_params.count() && OB_SUCC(ret); ++i) {
        if (OB_FAIL(drop_expr_idxs.push_back(exec_params.at(i)->get_param_index()))) {
          LOG_WARN("fail to push back exec param expr", K(ret));
        }
      }
    } else if (type == log_op_def::LOG_JOIN) {
      const auto &nl_params = static_cast<ObLogJoin *>(op)->get_nl_params();
      for (int i = 0; i < nl_params.count() && OB_SUCC(ret); ++i) {
        if (OB_FAIL(drop_expr_idxs.push_back(nl_params.at(i)->get_param_index()))) {
          LOG_WARN("fail to push back exec param expr", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObLogExchange::check_expr_is_need(const ObRawExpr *expr,
    const common::ObIArray<int64_t> &drop_expr_idxs,
    bool &is_need)
{
  int ret = OB_SUCCESS;
  is_need = true;
  ObSEArray<int64_t, 2>param_idxs;
  ObSEArray<ObRawExpr *, 8> params;
  if (OB_ISNULL(expr)) {
    /*do nothing*/
  } else if (expr->has_flag(CNT_DYNAMIC_PARAM)) {
    if (OB_FAIL(ObRawExprUtils::extract_params(const_cast<ObRawExpr *>(expr), params))) {
      LOG_WARN("fail to extract params", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    int64_t param_value = OB_INVALID_INDEX;
    for (int i = 0; i < params.count() && OB_SUCC(ret); ++i) {
      if (OB_ISNULL(params.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("params.at(i) returns null", K(ret), K(i));
      } else {
        param_value = static_cast<ObConstRawExpr *>(params.at(i))->get_value().get_unknown();
        OZ(param_idxs.push_back(param_value));
      }
    }
  }
  if (OB_SUCC(ret)) {
    for (int i = 0; i < param_idxs.count() && is_need; ++i) {
      for (int j = 0; j < drop_expr_idxs.count() && is_need; ++j) {
        if (param_idxs.at(i) == drop_expr_idxs.at(j)) {
          is_need = false;
          break;
        }
      }
    }
  }
  return ret;
}

int ObLogExchange::find_table_location_exprs(const common::ObIArray<int64_t> &drop_exprs_idxs,
    const common::ObIArray<ObRawExpr *> &filters,
    common::ObIArray<ObRawExpr *> &exprs,
    bool &has_exec_param)
{
  int ret = OB_SUCCESS;
  bool is_need = true;
  for (int i = 0; i < filters.count() && OB_SUCC(ret); ++i) {
    is_need = true;
    ObRawExpr *expr = filters.at(i);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null expr", K(ret));
    } else if (expr->has_flag(CNT_DYNAMIC_PARAM)) {
      if (OB_FAIL(check_expr_is_need(expr, drop_exprs_idxs, is_need))) {
        LOG_WARN("fail to check expr is need", K(ret));
      } else if (is_need) {
        has_exec_param = true;
      }
    }
    if (OB_SUCC(ret) && is_need) {
      OZ(exprs.push_back(expr));
    }
  }
  return ret;
}

int ObLogExchange::allocate_startup_expr_post()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null plan", K(ret));
  } else if (OB_PHY_PLAN_REMOTE == get_phy_plan_type()) {
    //do nothing
  } else if (OB_FAIL(ObLogicalOperator::allocate_startup_expr_post())) {
    LOG_WARN("failed to allocate startup exprs post", K(ret));
  }
  return ret;
}

int ObLogExchange::is_my_fixed_expr(const ObRawExpr *expr, bool &is_fixed)
{
  int ret = OB_SUCCESS;
  is_fixed = expr == calc_part_id_expr_ ||
             expr == partition_id_expr_ ||
             expr == random_expr_;
  return OB_SUCCESS;
}
