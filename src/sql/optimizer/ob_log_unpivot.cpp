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
#include "sql/optimizer/ob_log_unpivot.h"
#include "sql/optimizer/ob_join_order.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;

int ObLogUnpivot::get_op_exprs(ObIArray<ObRawExpr*> &all_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(append(all_exprs, origin_exprs_))
      || OB_FAIL(append(all_exprs, label_exprs_))
      || OB_FAIL(append(all_exprs, value_exprs_))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else if (OB_FAIL(ObLogicalOperator::get_op_exprs(all_exprs))) {
    LOG_WARN("failed to get op exprs", K(ret));
  }
  return ret;
}

int ObLogUnpivot::is_my_fixed_expr(const ObRawExpr *expr, bool &is_fixed)
{
  is_fixed = ObOptimizerUtil::find_item(label_exprs_, expr) ||
             ObOptimizerUtil::find_item(value_exprs_, expr);
  return OB_SUCCESS;
}

int ObLogUnpivot::get_plan_item_info(PlanText &plan_text, 
                                     ObSqlPlanItem &plan_item)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObLogicalOperator::get_plan_item_info(plan_text, plan_item))) {
    LOG_WARN("failed to get plan item info", K(ret));
  } else {
    BEGIN_BUF_PRINT; 
    if (OB_FAIL(BUF_PRINTF("unpivot(%s)",
        is_include_null_ ? "include null" : "exclude null"))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else { /* Do nothing */ }
    END_BUF_PRINT(plan_item.special_predicates_,
                  plan_item.special_predicates_len_);
  }
  return ret;
}

int ObLogUnpivot::do_re_est_cost(EstimateCostInfo &param, double &card, double &op_cost, double &cost)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *child = NULL;
  const int64_t parallel = param.need_parallel_;
  int part_count = label_exprs_.at(0)->get_param_count();
  if (OB_ISNULL(child = get_child(ObLogicalOperator::first_child)) || OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(child), K(get_plan()));
  } else {
    double child_card = child->get_card();
    double child_cost = child->get_cost();
    if (param.need_row_count_ >= 0) {
      if (param.need_row_count_ < get_card()) {
        param.need_row_count_ /= part_count;
      } else {
        param.need_row_count_ = -1;
      }
    }
    if (OB_FAIL(SMART_CALL(child->re_est_cost(param, child_card, child_cost)))) {
      LOG_WARN("failed to re est exchange cost", K(ret));
    } else {
      ObOptimizerContext &opt_ctx = get_plan()->get_optimizer_context();
      op_cost = ObOptEstCost::cost_filter_rows(child_card / parallel, 
                                               get_filter_exprs(),
                                               opt_ctx);
      cost = child_cost + op_cost;
      card = child_card * part_count;
    }
  }
  return ret;
}

int ObLogUnpivot::est_width()
{
  int ret = OB_SUCCESS;
  double width = 0.0;
  ObSEArray<ObRawExpr*, 16> output_exprs;
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid plan", K(ret));
  } else if (OB_FAIL(append_array_no_dup(output_exprs,
                                         get_plan()->get_select_item_exprs_for_width_est()))) {
    LOG_WARN("failed to add into output exprs", K(ret));
  } else if (OB_FAIL(ObOptEstCost::estimate_width_for_exprs(get_plan()->get_basic_table_metas(),
                                                            get_plan()->get_selectivity_ctx(),
                                                            output_exprs,
                                                            width))) {
    LOG_WARN("failed to estimate width for output unpivot exprs", K(ret));
  } else {
    set_width(width);
    LOG_TRACE("est width for unpivot", K(output_exprs), K(width));
  }
  return ret;
}

int ObLogUnpivot::est_cost()
{
  int ret = OB_SUCCESS;
  int64_t parallel = 0;
  ObLogicalOperator *child = NULL;
  if (OB_ISNULL(get_plan()) ||
      OB_ISNULL(child = get_child(first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_UNLIKELY((parallel = get_parallel()) < 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected parallel degree", K(parallel), K(ret));  
  } else {
    card_ = child->get_card() * (label_exprs_.at(0)->get_param_count());
    ObOptimizerContext &opt_ctx = get_plan()->get_optimizer_context();
    op_cost_ = ObOptEstCost::cost_filter_rows(card_ /parallel, 
                                              get_filter_exprs(),
                                              opt_ctx);
    cost_ = op_cost_ + child->get_cost();
  }
  return ret;
}

int ObLogUnpivot::inner_replace_op_exprs(ObRawExprReplacer &replacer)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(replace_exprs_action(replacer, origin_exprs_))) {
    LOG_WARN("failed to replace key exprs", K(ret));
  } else if (OB_FAIL(replace_exprs_action(replacer, label_exprs_))) {
    LOG_WARN("failed to replace for exprs", K(ret));
  } else if (OB_FAIL(replace_exprs_action(replacer, value_exprs_))) {
    LOG_WARN("failed to replace val exprs", K(ret));
  }
  return ret;
}

int ObLogUnpivot::compute_op_ordering()
{
  int ret = OB_SUCCESS;
  reset_op_ordering();
  return ret;
}

int ObLogUnpivot::compute_fd_item_set()
{

  int ret = OB_SUCCESS;
  set_fd_item_set(NULL);
  return ret;
}

int ObLogUnpivot::compute_one_row_info()
{
  int ret = OB_SUCCESS;
  set_is_at_most_one_row(false);
  return ret;
}

uint64_t ObLogUnpivot::hash(uint64_t seed) const
{
  uint64_t hash_value = seed;
  hash_value = do_hash(is_include_null_, hash_value);
  hash_value = ObLogicalOperator::hash(hash_value);
  return hash_value;
}

int ObLogUnpivot::compute_sharding_info()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *child = NULL;
  if (OB_ISNULL(child = get_child(first_child)) || OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(child), K(get_plan()));
  } else if (!child->is_distributed()) {
    ret = ObLogicalOperator::compute_sharding_info();
  } else {
    for (int64_t i = -1; OB_SUCC(ret) && i < child->get_weak_sharding().count(); ++i) {
      bool is_inherited_sharding = false;
      ObShardingInfo *sharding = (i == -1) ? child->get_strong_sharding() : child->get_weak_sharding().at(i);
      bool keep_origin = OB_ISNULL(sharding) ? false : 
                         (ObOptimizerUtil::subset_exprs(sharding->get_partition_keys(), origin_exprs_) &&
                          ObOptimizerUtil::subset_exprs(sharding->get_sub_partition_keys(), origin_exprs_));
      // Only inherit the sharding when partition keys do not exist in the unpivot
      // "for" clause, because those columns will be output by unpivot op directly.
      if (-1 == i) {
        // strong sharding
        if (keep_origin) {
          // can inherit
          strong_sharding_ = sharding;
          inherit_sharding_index_ = ObLogicalOperator::first_child;
        } else {
          // can not inherit
          strong_sharding_ = get_plan()->get_optimizer_context().get_distributed_sharding();
          inherit_sharding_index_ = -1;
        }
      } else {
        // weak sharding
        if (keep_origin) {
          // can inherit
          if (OB_FAIL(weak_sharding_.push_back(sharding))) {
            LOG_WARN("failed to push back weak sharding", K(ret));
          } else {
            inherit_sharding_index_ = ObLogicalOperator::first_child;
          }
        } else {
          // weak sharding, can not inherit, do nothing
        }
      }
    }
    if (OB_SUCC(ret)) {
      LOG_TRACE("succeed to convert unpovit sharding info", KPC(strong_sharding_), K(weak_sharding_), K(inherit_sharding_index_));
    }
  }
  return ret;
}
