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
#include "ob_log_group_by.h"
#include "lib/allocator/page_arena.h"
#include "sql/resolver/expr/ob_raw_expr_replacer.h"
#include "ob_log_operator_factory.h"
#include "ob_log_exchange.h"
#include "ob_log_sort.h"
#include "ob_log_topk.h"
#include "ob_log_material.h"
#include "ob_log_table_scan.h"
#include "ob_optimizer_context.h"
#include "ob_optimizer_util.h"
#include "ob_opt_est_cost.h"
#include "ob_select_log_plan.h"
#include "common/ob_smart_call.h"
#include "ob_opt_selectivity.h"
#include "ob_log_operator_factory.h"
#include "sql/optimizer/ob_join_order.h"
#include "sql/rewrite/ob_transform_utils.h"

using namespace oceanbase;
using namespace sql;
using namespace oceanbase::common;

int ObThreeStageAggrInfo::assign(const ObThreeStageAggrInfo &info)
{
  int ret = OB_SUCCESS;
  aggr_stage_ = info.aggr_stage_;
  distinct_aggr_count_ = info.distinct_aggr_count_;
  aggr_code_idx_ = info.aggr_code_idx_;
  aggr_code_expr_ = info.aggr_code_expr_;
  aggr_code_ndv_ = info.aggr_code_ndv_;
  if (OB_FAIL(distinct_exprs_.assign(info.distinct_exprs_))) {
    LOG_WARN("failed to assign distinct exprs", K(ret));
  } else if (OB_FAIL(distinct_aggr_batch_.assign(info.distinct_aggr_batch_))) {
    LOG_WARN("failed to assign distinct col idx", K(ret));
  }
  return ret;
}

int ObRollupAdaptiveInfo::assign(const ObRollupAdaptiveInfo &info)
{
  int ret = OB_SUCCESS;
  rollup_id_expr_ = info.rollup_id_expr_;
  rollup_status_ = info.rollup_status_;
  enable_encode_sort_ = info.enable_encode_sort_;
  sort_keys_.reset();
  ecd_sort_keys_.reset();
  if (OB_FAIL(append(sort_keys_, info.sort_keys_))) {
    LOG_WARN("failed to assign distinct col idx", K(ret));
  } else if (OB_FAIL(append(ecd_sort_keys_, info.ecd_sort_keys_))) {
    LOG_WARN("failed to append sort keys", K(ret));
  }
  return ret;
}

int ObLogGroupBy::get_explain_name_internal(char *buf,
                                            const int64_t buf_len,
                                            int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (SCALAR_AGGREGATE == algo_) {
    ret = BUF_PRINTF("SCALAR ");
  } else if (HASH_AGGREGATE == algo_) {
    ret = BUF_PRINTF("HASH ");
  } else {
    if (ObRollupStatus::ROLLUP_DISTRIBUTOR != rollup_adaptive_info_.rollup_status_) {
      ret = BUF_PRINTF("MERGE ");
    } else {
      // inner sort in groupby
      ret = BUF_PRINTF("SORT ");
    }
  }
  if (OB_SUCC(ret)) {
    ret = BUF_PRINTF("%s", get_name());
  }
  if (OB_FAIL(ret)) {
  } else if (ObRollupStatus::ROLLUP_DISTRIBUTOR == rollup_adaptive_info_.rollup_status_) {
    ret = BUF_PRINTF(" ROLLUP DISTRIBUTOR");
  } else if (ObRollupStatus::ROLLUP_COLLECTOR == rollup_adaptive_info_.rollup_status_) {
    ret = BUF_PRINTF(" ROLLUP COLLECTOR");
  }

  if (OB_SUCC(ret) && from_pivot_) {
    ret = BUF_PRINTF(" PIVOT");
  }

  if (OB_FAIL(ret)) {
    LOG_WARN("BUF_PRINTF fails", K(ret));
  }

  return ret;
}

int ObLogGroupBy::set_group_by_exprs(const common::ObIArray<ObRawExpr *> &group_by_exprs)
{
  return group_exprs_.assign(group_by_exprs);
}

int ObLogGroupBy::set_rollup_exprs(const common::ObIArray<ObRawExpr *> &rollup_exprs)
{
  return rollup_exprs_.assign(rollup_exprs);
}

int ObLogGroupBy::set_aggr_exprs(const common::ObIArray<ObAggFunRawExpr *> &aggr_exprs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < aggr_exprs.count(); i++) {
    ObAggFunRawExpr *aggr_expr = aggr_exprs.at(i);
    if (OB_ISNULL(aggr_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("aggr_expr is null", K(ret));
    } else {
      ret = aggr_exprs_.push_back(aggr_expr);
    }
  }
  return ret;
}

int ObLogGroupBy::get_group_rollup_exprs(common::ObIArray<ObRawExpr *> &group_rollup_exprs) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(append(group_rollup_exprs, group_exprs_))) {
    LOG_WARN("failed to append group exprs into group rollup exprs.", K(ret));
  } else if (OB_FAIL(append(group_rollup_exprs, rollup_exprs_))) {
    LOG_WARN("failed to append rollup exprs into group rollup exprs.", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObLogGroupBy::get_op_exprs(ObIArray<ObRawExpr*> &all_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(append(all_exprs, group_exprs_))) {
    LOG_WARN("failed to add exprs to ctx", K(ret));
  } else if (OB_FAIL(append_array_no_dup(all_exprs, rollup_exprs_))) {
    LOG_WARN("failed to add exprs to ctx", K(ret));
  } else if (OB_FAIL(append(all_exprs, aggr_exprs_))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else if (is_three_stage_aggr() && all_exprs.push_back(three_stage_info_.aggr_code_expr_)) {
    LOG_WARN("failed to push back exprs", K(ret));
  } else if (NULL != rollup_adaptive_info_.rollup_id_expr_ &&
             OB_FAIL(all_exprs.push_back(rollup_adaptive_info_.rollup_id_expr_))) {
    LOG_WARN("failed to add rollup id expr", K(ret));
  } else if (OB_FAIL(ObLogicalOperator::get_op_exprs(all_exprs))) {
    LOG_WARN("failed to get op exprs", K(ret));
  } else { /*do nothing*/ }

  if (OB_SUCC(ret) && is_first_stage()) {
    for (int64_t i = 0; i < three_stage_info_.distinct_aggr_batch_.count() && OB_SUCC(ret); ++i) {
      const ObDistinctAggrBatch &distinct_batch = three_stage_info_.distinct_aggr_batch_.at(i);
      for (int64_t j = 0; j < distinct_batch.mocked_params_.count() && OB_SUCC(ret); ++j) {
        if (OB_FAIL(all_exprs.push_back(distinct_batch.mocked_params_.at(j).first))) {
          LOG_WARN("failed to push back distinct expr", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret) && rollup_adaptive_info_.enable_encode_sort_) {
    for (int64_t i = 0; i < rollup_adaptive_info_.sort_keys_.count() && OB_SUCC(ret); ++i) {
      if (OB_FAIL(all_exprs.push_back(rollup_adaptive_info_.sort_keys_.at(i).expr_))) {
        LOG_WARN("failed to push back distinct expr", K(ret));
      }
    }
    for (int64_t i = 0; i < rollup_adaptive_info_.ecd_sort_keys_.count() && OB_SUCC(ret); ++i) {
      if (OB_FAIL(all_exprs.push_back(rollup_adaptive_info_.ecd_sort_keys_.at(i).expr_))) {
        LOG_WARN("failed to push back distinct expr", K(ret));
      }
    }
  }
  return ret;
}

uint64_t ObLogGroupBy::hash(uint64_t seed) const
{
  uint64_t hash_value = seed;
  hash_value = do_hash(algo_, hash_value);
  hash_value = ObLogicalOperator::hash(hash_value);

  return hash_value;
}

int ObLogGroupBy::get_plan_item_info(PlanText &plan_text,
                                     ObSqlPlanItem &plan_item)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObLogicalOperator::get_plan_item_info(plan_text, plan_item))) {
    LOG_WARN("failed to get plan item info", K(ret));
  } else {
    BEGIN_BUF_PRINT;
    if (OB_FAIL(get_explain_name_internal(buf, buf_len, pos))) {
      LOG_WARN("failed to get explain name", K(ret));
    }
    END_BUF_PRINT(plan_item.operation_, plan_item.operation_len_);
  }
  if (OB_SUCC(ret)) {
    BEGIN_BUF_PRINT;
    const ObIArray<ObRawExpr *> &group = get_group_by_exprs();
    EXPLAIN_PRINT_EXPRS(group, type);
    const ObIArray<ObRawExpr *> &rollup = get_rollup_exprs();
    if (OB_FAIL(ret) || (rollup.count() <= 0)) {
    } else if(OB_FAIL(BUF_PRINTF(", "))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else {
      EXPLAIN_PRINT_EXPRS(rollup, type);
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(BUF_PRINTF(", "))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else {
      const ObIArray<ObRawExpr *> &agg_func = get_aggr_funcs();
      EXPLAIN_PRINT_EXPRS(agg_func, type);
    }
    END_BUF_PRINT(plan_item.special_predicates_,
                  plan_item.special_predicates_len_);
  }
  return ret;
}

int ObLogGroupBy::est_cost()
{
  int ret = OB_SUCCESS;
  double child_card = 0.0;
  double child_ndv = 0.0;
  double selectivity = 1.0;
  double group_cost = 0.0;
  ObLogicalOperator *child = get_child(ObLogicalOperator::first_child);
  if (OB_ISNULL(child)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(child));
  } else if (OB_FAIL(get_child_est_info(get_parallel(), child_card, child_ndv, selectivity))) {
    LOG_WARN("failed to get chidl est info", K(ret));
  } else if (OB_FAIL(inner_est_cost(get_parallel(),
                                    child_card,
                                    child_ndv,
                                    distinct_per_dop_,
                                    group_cost))) {
    LOG_WARN("failed to est group by cost", K(ret));
  } else {
    distinct_card_ = child_ndv;
    set_card(distinct_card_ * selectivity);
    set_cost(child->get_cost() + group_cost);
    set_op_cost(group_cost);
  }
  return ret;
}

int ObLogGroupBy::do_re_est_cost(EstimateCostInfo &param, double &card, double &op_cost, double &cost)
{
  int ret = OB_SUCCESS;
  double child_card = 0.0;
  double child_ndv = 0.0;
  double selectivity = 1.0;
  ObLogicalOperator *child = get_child(ObLogicalOperator::first_child);
  const int64_t parallel = param.need_parallel_;
  double number_of_copies = get_number_of_copies();
  if (OB_ISNULL(child) || OB_UNLIKELY(number_of_copies < 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(child), K(number_of_copies));
  } else if (OB_FAIL(get_child_est_info(parallel, child_card, child_ndv, selectivity))) {
    LOG_WARN("failed to get chidl est info", K(ret));
  } else {
    double child_cost = child->get_cost();
    double need_ndv = child_ndv;
    double origin_child_card = child_card;
    bool need_scale_ndv = false;
    if (param.need_row_count_ >= 0 && 
        child->get_card() > 0 &&
        child_ndv > 0 &&
        param.need_row_count_ < child_ndv) {
      need_ndv = param.need_row_count_;
      if (selectivity > OB_DOUBLE_EPSINON) {
        need_ndv /= selectivity;
      }
      if (child_card > 0) {
        param.need_row_count_ = child_card * need_ndv / child_ndv;
        param.need_row_count_ /= number_of_copies;
      } else {
        param.need_row_count_ = 0;
      }
    } else {
      param.need_row_count_ = -1;
      need_scale_ndv = true;
    }
    if (is_block_op()) {
      param.need_row_count_ = -1; //reset need row count
    }
    if (OB_FAIL(SMART_CALL(child->re_est_cost(param, child_card, child_cost)))) {
      LOG_WARN("failed to re est child cost", K(ret));
    } else {
      // At the first stage, child output will be replicated
      child_card = child_card * number_of_copies;
      if (need_scale_ndv) {
        need_ndv = std::min(child_ndv, ObOptSelectivity::scale_distinct(child_card, origin_child_card, child_ndv));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(inner_est_cost(parallel,
                                      child_card,
                                      need_ndv,
                                      distinct_per_dop_,
                                      op_cost))) {
      LOG_WARN("failed to est distinct cost", K(ret));
    } else {
      cost = child_cost + op_cost;
      card = need_ndv * selectivity;
      if (param.override_) {
        set_total_ndv(need_ndv);
      }
    }
  }
  return ret;
}

int ObLogGroupBy::inner_est_cost(const int64_t parallel, double child_card, double &child_ndv, double &per_dop_ndv, double &op_cost)
{
  int ret = OB_SUCCESS;
  double per_dop_card = 0.0;
  per_dop_ndv = 0.0;
  common::ObSEArray<ObRawExpr *, 8> group_rollup_exprs;
  ObLogicalOperator *child = get_child(ObLogicalOperator::first_child);
  if (OB_ISNULL(get_plan()) ||
      OB_ISNULL(child)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(child));
  } else if (OB_UNLIKELY(parallel < 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(child));
  } else if (OB_FAIL(get_group_rollup_exprs(group_rollup_exprs))) {
    LOG_WARN("failed to get group rollup exprs", K(ret));
  } else {
    per_dop_card = child_card / parallel;
    if ((get_group_by_exprs().empty() && get_rollup_exprs().empty()) || SCALAR_AGGREGATE == algo_) {
      per_dop_ndv = 1.0;
    } else if (parallel > 1) {
      if (is_push_down()) {
        per_dop_ndv = ObOptSelectivity::scale_distinct(per_dop_card, child_card, child_ndv);
      } else {
        per_dop_ndv = child_ndv / parallel;
      }
    } else {
      per_dop_ndv = child_ndv;
    }
  }
  if (OB_SUCC(ret)) {
    ObOptimizerContext &opt_ctx = get_plan()->get_optimizer_context();
    if (SCALAR_AGGREGATE == algo_) {
      op_cost = ObOptEstCost::cost_scalar_group(per_dop_card,
                                                get_aggr_funcs().count(),
                                                opt_ctx);
    } else if (MERGE_AGGREGATE == algo_) {
      op_cost = ObOptEstCost::cost_merge_group(per_dop_card,
                                              per_dop_ndv,
                                              child->get_width(),
                                              group_rollup_exprs,
                                              get_aggr_funcs().count(),
                                              opt_ctx);
    } else {
      op_cost = ObOptEstCost::cost_hash_group(per_dop_card,
                                              per_dop_ndv,
                                              child->get_width(),
                                              group_exprs_,
                                              get_aggr_funcs().count(),
                                              opt_ctx);
    }

    child_ndv = std::min(child_card, per_dop_ndv * parallel);
    if (SCALAR_AGGREGATE == algo_) {
      child_ndv = std::max(1.0, child_ndv);
    }
  }
  return ret;
}

int ObLogGroupBy::get_child_est_info(const int64_t parallel, double &child_card, double &child_ndv, double &selectivity)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *child = get_child(ObLogicalOperator::first_child);
  if (OB_ISNULL(child) || OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(child));
  } else if (OB_UNLIKELY(parallel < 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(child));
  } else if ((get_group_by_exprs().empty() && get_rollup_exprs().empty())
             || SCALAR_AGGREGATE == algo_) {
    child_card = child->get_card();
    child_ndv = parallel;
  } else {
    child_card = child->get_card();
    child_ndv = get_total_ndv();
  }
  //having filter selectivity
  if (OB_SUCC(ret)) {
    // At the first stage, child output will be replicated
    child_card = child_card * get_number_of_copies();
    get_plan()->get_selectivity_ctx().init_row_count(get_origin_child_card(), child_ndv);
    if (OB_FAIL(ObOptSelectivity::calculate_selectivity(get_plan()->get_update_table_metas(),
                                                        get_plan()->get_selectivity_ctx(),
                                                        get_filter_exprs(),
                                                        selectivity,
                                                        get_plan()->get_predicate_selectivities()))) {
      LOG_WARN("failed to calculate selectivity", K(ret));
    }
  }
  return ret;
}

int ObLogGroupBy::est_width()
{
  int ret = OB_SUCCESS;
  double width = 0.0;
  ObSEArray<ObRawExpr*, 16> output_exprs;
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid plan", K(ret));
  } else if (OB_FAIL(get_gby_output_exprs(output_exprs))) {
    LOG_WARN("failed to compute gby output column exprs", K(ret));
  } else if (OB_FAIL(ObOptEstCost::estimate_width_for_exprs(get_plan()->get_basic_table_metas(),
                                                            get_plan()->get_selectivity_ctx(),
                                                            output_exprs,
                                                            width))) {
    LOG_WARN("failed to estimate width for output gby column exprs", K(ret));
  } else {
    set_width(width);
    LOG_TRACE("est width for gby", K(output_exprs), K(width));
  }
  return ret;
}

int ObLogGroupBy::get_gby_output_exprs(ObIArray<ObRawExpr *> &output_exprs)
{
  int ret = OB_SUCCESS;
  ObLogPlan *plan = NULL;
  ObSEArray<ObRawExpr*, 16> candi_exprs;
  ObSEArray<ObRawExpr*, 16> extracted_col_or_aggr_exprs;
  if (OB_ISNULL(plan = get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid input", K(ret));
  } else if (OB_FAIL(append_array_no_dup(candi_exprs, plan->get_winfunc_exprs_for_width_est()))) {
    LOG_WARN("failed to add into output exprs", K(ret));
  } else if (OB_FAIL(append_array_no_dup(candi_exprs, plan->get_select_item_exprs_for_width_est()))) {
    LOG_WARN("failed to add into output exprs", K(ret));
  } else if (OB_FAIL(append_array_no_dup(candi_exprs, plan->get_orderby_exprs_for_width_est()))) {
    LOG_WARN("failed to add into output exprs", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::extract_col_aggr_exprs(candi_exprs,
                                                            extracted_col_or_aggr_exprs))) {
    LOG_WARN("failed to extract exprs", K(ret));
  } else if (OB_FAIL(append_array_no_dup(output_exprs, extracted_col_or_aggr_exprs))) {
    LOG_WARN("failed to add into output exprs", K(ret));
  } else {/*do nothing*/}
  return ret;
}

int ObLogGroupBy::inner_replace_op_exprs(ObRawExprReplacer &replacer)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(replace_exprs_action(replacer, get_group_by_exprs()))) {
    LOG_WARN("failed to extract subplan params in log group by exprs", K(ret));
  } else if (OB_FAIL(replace_exprs_action(replacer, get_rollup_exprs()))) {
    LOG_WARN("failed to extract subplan params in log rollup exprs", K(ret));
  } else if (OB_FAIL(replace_exprs_action(replacer, get_aggr_funcs()))) {
    LOG_WARN("failed to extract subplan params in log agg funcs", K(ret));
  } else {
    for(int64_t i = 0; OB_SUCC(ret) && i < rollup_adaptive_info_.sort_keys_.count(); ++i) {
      OrderItem &cur_order_item = rollup_adaptive_info_.sort_keys_.at(i);
      if (OB_FAIL(replace_expr_action(replacer, cur_order_item.expr_))) {
        LOG_WARN("failed to resolve ref params in sort key ", K(cur_order_item), K(ret));
      } else { /* Do nothing */ }
    }
    for(int64_t i = 0; OB_SUCC(ret) && i < rollup_adaptive_info_.ecd_sort_keys_.count(); ++i) {
      OrderItem &cur_order_item = rollup_adaptive_info_.ecd_sort_keys_.at(i);
      if (OB_FAIL(replace_expr_action(replacer, cur_order_item.expr_))) {
        LOG_WARN("failed to resolve ref params in sort key ", K(cur_order_item), K(ret));
      } else { /* Do nothing */ }
    }
  }
  if (OB_SUCC(ret) && is_three_stage_aggr()) {
    if (OB_FAIL(replace_exprs_action(replacer, three_stage_info_.distinct_exprs_))) {
      LOG_WARN("failed to replace three stage info distinct exprs", K(ret));
    } else {
      for(int64_t i = 0; OB_SUCC(ret) && i < three_stage_info_.distinct_aggr_batch_.count(); ++i) {
        ObDistinctAggrBatch &distinct_batch = three_stage_info_.distinct_aggr_batch_.at(i);
        for (int64_t j = 0; OB_SUCC(ret) && j < distinct_batch.mocked_params_.count(); ++j) {
          if (OB_FAIL(replace_expr_action(replacer, distinct_batch.mocked_params_.at(j).first))) {
            LOG_WARN("failed to replace distinct expr", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObLogGroupBy::print_outline_data(PlanText &plan_text)
{
  int ret = OB_SUCCESS;
  const ObDMLStmt *stmt = NULL;
  ObString qb_name;
  if (is_push_down()) {
    /* print outline in top group by */
  } else if (OB_ISNULL(get_plan()) || OB_ISNULL(stmt = get_plan()->get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL", K(ret), K(get_plan()), K(stmt));
  } else if (OB_FAIL(stmt->get_qb_name(qb_name))) {
    LOG_WARN("fail to get qb_name", K(ret), K(stmt->get_stmt_id()));
  } else {
    if (OB_SUCC(ret) && has_push_down_) {
      ObOptHint hint(T_GBY_PUSHDOWN);
      hint.set_qb_name(qb_name);
      if (OB_FAIL(hint.print_hint(plan_text))) {
        LOG_WARN("failed to print hint", K(ret), K(hint));
      }
    }
    if (OB_SUCC(ret) && (use_hash_aggr_ || use_part_sort_)) {
      ObAggHint hint(use_hash_aggr_ ? T_USE_HASH_AGGREGATE : T_NO_USE_HASH_AGGREGATE);
      hint.set_qb_name(qb_name);
      hint.set_use_partition_sort(use_part_sort_);
      if (OB_FAIL(hint.print_hint(plan_text))) {
        LOG_WARN("failed to print hint", K(ret), K(hint));
      }
    }
  }
  return ret;
}

int ObLogGroupBy::print_used_hint(PlanText &plan_text)
{
  int ret = OB_SUCCESS;
  const ObHint *hint = NULL;
  if (is_push_down()) {
    /* print outline in top group by */
  } else if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL", K(ret), K(get_plan()));
  } else if (NULL != (hint = get_plan()->get_log_plan_hint().get_normal_hint(T_GBY_PUSHDOWN))
             && hint->is_enable_hint() == has_push_down_
             && OB_FAIL(hint->print_hint(plan_text))) {
    LOG_WARN("failed to print used hint for group by", K(ret), K(*hint));
  } else if (NULL != (hint = get_plan()->get_log_plan_hint().get_normal_hint(T_USE_HASH_AGGREGATE))
             && hint->is_enable_hint() == use_hash_aggr_
             && static_cast<const ObAggHint*>(hint)->force_partition_sort() == use_part_sort_
             && OB_FAIL(hint->print_hint(plan_text))) {
    LOG_WARN("failed to print used hint for group by", K(ret), K(*hint));
  }
  return ret;
}

int ObLogGroupBy::compute_const_exprs()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *child = NULL;
  if (OB_ISNULL(my_plan_) || OB_UNLIKELY(get_num_of_child() < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("operator is invalid", K(ret), K(get_num_of_child()), K(my_plan_));
  } else if (OB_ISNULL(child = get_child(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child is null", K(ret), K(child));
  } else if (!has_rollup() &&
             OB_FAIL(append(get_output_const_exprs(), child->get_output_const_exprs()))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::compute_const_exprs(get_filter_exprs(), get_output_const_exprs()))) {
    LOG_WARN("failed to compute const conditionexprs", K(ret));
  } else {/*do nothing*/}
  return ret;
}

int ObLogGroupBy::compute_equal_set()
{
  int ret = OB_SUCCESS;
  EqualSets *ordering_esets = NULL;
  if (OB_ISNULL(my_plan_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("operator is invalid", K(ret), K(my_plan_));
  } else if (!has_rollup()) {
    if (OB_FAIL(ObLogicalOperator::compute_equal_set())) {
      LOG_WARN("failed to compute equal set", K(ret));
    }
  } else if (filter_exprs_.empty()) {
    set_output_equal_sets(&empty_expr_sets_);
  } else if (OB_ISNULL(ordering_esets = get_plan()->create_equal_sets())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to create equal sets", K(ret));
  } else if (OB_FAIL(ObEqualAnalysis::compute_equal_set(
                       &my_plan_->get_allocator(),
                       filter_exprs_,
                       *ordering_esets))) {
    LOG_WARN("failed to compute ordering output equal set", K(ret));
  } else {
    set_output_equal_sets(ordering_esets);
  }
  return ret;
}

int ObLogGroupBy::compute_fd_item_set()
{
  int ret = OB_SUCCESS;
  const ObLogicalOperator *child = NULL;
  ObFdItemSet *fd_item_set = NULL;
  ObTableFdItem *fd_item = NULL;
  if (OB_ISNULL(child = get_child(ObLogicalOperator::first_child)) || OB_ISNULL(my_plan_) ||
      OB_ISNULL(get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpect null", K(ret), K(child), K(my_plan_), K(get_stmt()));
  } else if (has_rollup()) {
    // do nothing
  } else if (OB_FAIL(my_plan_->get_fd_item_factory().create_fd_item_set(fd_item_set))) {
    LOG_WARN("failed to create fd item set", K(ret));
  } else if (OB_FAIL(fd_item_set->assign(child->get_fd_item_set()))) {
    LOG_WARN("failed to assign fd item set", K(ret));
  } else if (group_exprs_.empty()) {
    // scalar group by
    if (get_stmt()->is_select_stmt() && OB_FAIL(create_fd_item_from_select_list(fd_item_set))) {
      LOG_WARN("failed to create fd item from select list", K(ret));
    }
  } else if (!ObTransformUtils::need_compute_fd_item_set(group_exprs_)) {
    //do nothing
  } else if (OB_FAIL(my_plan_->get_fd_item_factory().create_table_fd_item(
      fd_item,
      true,
      group_exprs_,
      get_table_set()))) {
    LOG_WARN("failed to create fd item", K(ret));
  } else if (OB_FAIL(fd_item_set->push_back(fd_item))) {
    LOG_WARN("failed to push back fd item", K(ret));
  }

  if (OB_FAIL(ret)) {
    /*do nothing*/
  } else if (OB_NOT_NULL(fd_item_set) && // rollup 时 fd_item_set is null
             OB_FAIL(deduce_const_exprs_and_ft_item_set(*fd_item_set))) {
    LOG_WARN("falied to deduce fd item set", K(ret));
  } else {
    set_fd_item_set(fd_item_set);
  }
  return ret;
}

int ObLogGroupBy::create_fd_item_from_select_list(ObFdItemSet *fd_item_set)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 8> select_exprs;
  ObTableFdItem *fd_item = NULL;
  if (OB_ISNULL(fd_item_set) || OB_ISNULL(my_plan_) || OB_ISNULL(get_stmt()) ||
      OB_UNLIKELY(!get_stmt()->is_select_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpect parameter", K(ret), K(fd_item_set), K(my_plan_), K(get_stmt()));
  } else if (OB_FAIL(static_cast<const ObSelectStmt *>(get_stmt())->get_select_exprs(select_exprs))) {
    LOG_WARN("failed to get select exprs", K(ret));
  } else if (ObTransformUtils::need_compute_fd_item_set(select_exprs)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < select_exprs.count(); ++i) {
      ObSEArray<ObRawExpr *, 1> value_exprs;
      if (OB_FAIL(value_exprs.push_back(select_exprs.at(i)))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else if (OB_FAIL(my_plan_->get_fd_item_factory().create_table_fd_item(fd_item,
                                                                              true,
                                                                              value_exprs,
                                                                              get_table_set()))) {
        LOG_WARN("failed to create fd item", K(ret));
      } else if (OB_FAIL(fd_item_set->push_back(fd_item))) {
        LOG_WARN("failed to push back fd item", K(ret));
      }
    }
  }
  return ret;
}

int ObLogGroupBy::compute_op_ordering()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *child = NULL;
  if (HASH_AGGREGATE == algo_) {
    // do nothing
    reset_op_ordering();
  } else if (OB_ISNULL(child = get_child(ObLogicalOperator::first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child is null", K(ret));
  } else if (has_rollup()) {
    ObSEArray<OrderItem, 4> ordering;
    // for rollup distributor, sort key is inner
    if (ObRollupStatus::ROLLUP_DISTRIBUTOR != rollup_adaptive_info_.rollup_status_) {
      for (int64_t i = 0; OB_SUCC(ret) && i < group_exprs_.count(); i++) {
        if (i < child->get_op_ordering().count() &&
            child->get_op_ordering().at(i).expr_ == group_exprs_.at(i) &&
            OB_FAIL(ordering.push_back(child->get_op_ordering().at(i)))) {
          LOG_WARN("failed to push back into ordering.", K(ret));
        } else {}
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(set_op_ordering(ordering))) {
      LOG_WARN("failed to set op ordering.", K(ret));
    } else {
      is_range_order_ = child->get_is_range_order();
      is_local_order_ = is_fully_partition_wise() && !get_op_ordering().empty() && !is_range_order_;
    }
  } else if (OB_FAIL(set_op_ordering(child->get_op_ordering()))) {
    LOG_WARN("failed to set op ordering", K(ret));
  } else {
    is_range_order_ = child->get_is_range_order();
    is_local_order_ = is_fully_partition_wise() && !get_op_ordering().empty() && !is_range_order_;
  }
  return ret;
}

int ObLogGroupBy::allocate_granule_pre(AllocGIContext &ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(pw_allocate_granule_pre(ctx))) {
    LOG_WARN("failed to allocate partition wise granule", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObLogGroupBy::allocate_granule_post(AllocGIContext &ctx)
{
  return pw_allocate_granule_post(ctx);
}

int ObLogGroupBy::compute_one_row_info()
{
  int ret = OB_SUCCESS;
  if (group_exprs_.empty() && rollup_exprs_.empty()) {
    set_is_at_most_one_row(true);
  } else if (has_rollup()) {
    set_is_at_most_one_row(false);
  } else if (OB_FAIL(ObLogicalOperator::compute_one_row_info())) {
    LOG_WARN("failed to compute one row info", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObLogGroupBy::allocate_startup_expr_post()
{
  int ret = OB_SUCCESS;
  if (SCALAR_AGGREGATE == algo_) {
    //do nothing
  } else if (OB_FAIL(ObLogicalOperator::allocate_startup_expr_post())) {
    LOG_WARN("failed to allocate startup exprs post", K(ret));
  }
  return ret;
}

int ObLogGroupBy::set_rollup_info(
  const ObRollupStatus rollup_status,
  ObRawExpr *rollup_id_expr)
{
  int ret = OB_SUCCESS;
  rollup_adaptive_info_.rollup_id_expr_ = rollup_id_expr;
  rollup_adaptive_info_.rollup_status_ = rollup_status;
  return ret;
}

int ObLogGroupBy::set_rollup_info(
  const ObRollupStatus rollup_status,
  ObRawExpr *rollup_id_expr,
  ObIArray<OrderItem> &sort_keys,
  ObIArray<OrderItem> &ecd_sort_keys,
  bool enable_encode_sort)
{
  int ret = OB_SUCCESS;
  rollup_adaptive_info_.rollup_id_expr_ = rollup_id_expr;
  rollup_adaptive_info_.rollup_status_ = rollup_status;
  rollup_adaptive_info_.enable_encode_sort_ = enable_encode_sort;
  rollup_adaptive_info_.sort_keys_.reset();
  rollup_adaptive_info_.ecd_sort_keys_.reset();
  if (OB_FAIL(append(rollup_adaptive_info_.sort_keys_, sort_keys))) {
    LOG_WARN("failed to append sort keys", K(ret));
  } else if (OB_FAIL(append(rollup_adaptive_info_.ecd_sort_keys_, ecd_sort_keys))) {
    LOG_WARN("failed to append sort keys", K(ret));
  }
  return ret;
}

int ObThreeStageAggrInfo::set_first_stage_info(ObRawExpr *aggr_code_expr,
                                               ObIArray<ObDistinctAggrBatch> &batch,
                                               double aggr_code_ndv)
{
  int ret = OB_SUCCESS;
  reuse();
  aggr_stage_ = ObThreeStageAggrStage::FIRST_STAGE;
  aggr_code_expr_ = aggr_code_expr;
  distinct_aggr_count_ = 0;
  aggr_code_ndv_ = aggr_code_ndv;
  if (OB_FAIL(distinct_aggr_batch_.assign(batch))) {
    LOG_WARN("failed to assign batch", K(ret));
  } else {
    for (int64_t i = 0; i < batch.count(); ++i) {
      distinct_aggr_count_ += batch.at(i).mocked_aggrs_.count();
    }
  }
  return ret;
}

int ObThreeStageAggrInfo::set_second_stage_info(ObRawExpr *aggr_code_expr,
                                               ObIArray<ObDistinctAggrBatch> &batch,
                                               ObIArray<ObRawExpr *> &distinct_exprs)
{
  int ret = OB_SUCCESS;
  reuse();
  aggr_stage_ = ObThreeStageAggrStage::SECOND_STAGE;
  aggr_code_expr_ = aggr_code_expr;
  distinct_aggr_count_ = 0;
  if (OB_FAIL(distinct_aggr_batch_.assign(batch))) {
    LOG_WARN("failed to assign batch", K(ret));
  } else if (OB_FAIL(distinct_exprs_.assign(distinct_exprs))) {
    LOG_WARN("failed to assign distinct", K(ret));
  } else {
    for (int64_t i = 0; i < batch.count(); ++i) {
      distinct_aggr_count_ += batch.at(i).mocked_aggrs_.count();
    }
  }
  return ret;
}

int ObThreeStageAggrInfo::set_third_stage_info(ObRawExpr *aggr_code_expr,
                                               ObIArray<ObDistinctAggrBatch> &batch)
{
  int ret = OB_SUCCESS;
  reuse();
  aggr_stage_ = ObThreeStageAggrStage::THIRD_STAGE;
  aggr_code_expr_ = aggr_code_expr;
  distinct_aggr_count_ = 0;
  if (OB_FAIL(distinct_aggr_batch_.assign(batch))) {
    LOG_WARN("failed to assign batch", K(ret));
  } else {
    for (int64_t i = 0; i < batch.count(); ++i) {
      distinct_aggr_count_ += batch.at(i).mocked_aggrs_.count();
    }
  }
  return ret;
}

int ObLogGroupBy::set_three_stage_info(const ObThreeStageAggrInfo &info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(three_stage_info_.assign(info))) {
    LOG_WARN("failed to assgin", K(ret));
  } else if (is_third_stage()) {
    // do nothing
  } else if (!ObOptimizerUtil::find_item(group_exprs_,
                                         three_stage_info_.aggr_code_expr_,
                                         &three_stage_info_.aggr_code_idx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("aggr code expr is not found", K(ret));
  }
  return ret;
}

int ObLogGroupBy::is_my_fixed_expr(const ObRawExpr *expr, bool &is_fixed)
{
  int ret = OB_SUCCESS;
  is_fixed = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else {
    is_fixed = ObOptimizerUtil::find_item(aggr_exprs_, expr) ||
        (T_FUN_SYS_REMOVE_CONST == expr->get_expr_type() && ObOptimizerUtil::find_item(rollup_exprs_, expr));
  }
  return ret;
}

int ObLogGroupBy::compute_sharding_info()
{
  int ret = OB_SUCCESS;
  if (ObRollupStatus::ROLLUP_COLLECTOR == rollup_adaptive_info_.rollup_status_) {
    ObLogicalOperator *child = NULL;
    if (get_num_of_child() == 0) {
      /*do nothing*/
    } else if (OB_ISNULL(child = get_child(ObLogicalOperator::first_child))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (child->get_strong_sharding() != NULL &&
               OB_FAIL(weak_sharding_.push_back(child->get_strong_sharding()))) {
      LOG_WARN("failed to push back weak sharding");
    } else if (OB_FAIL(append(weak_sharding_, child->get_weak_sharding()))) {
      LOG_WARN("failed to assign sharding info", K(ret));
    } else {
      inherit_sharding_index_ = ObLogicalOperator::first_child;
      strong_sharding_ = NULL;
    }
  } else if (OB_FAIL(ObLogicalOperator::compute_sharding_info())) {
    LOG_WARN("failed to compute sharding info", K(ret));
  }
  return ret;
}

int ObLogGroupBy::get_card_without_filter(double &card)
{
  int ret = OB_SUCCESS;
  card = get_distinct_card();
  return ret;
}

int ObLogGroupBy::check_use_child_ordering(bool &used, int64_t &inherit_child_ordering_index)
{
  int ret = OB_SUCCESS;
  used = true;
  inherit_child_ordering_index = first_child;
  if (HASH_AGGREGATE == get_algo()) {
    inherit_child_ordering_index = -1;
    used = false;
  } else if (get_group_by_exprs().empty() &&
             get_rollup_exprs().empty()) {
    used = false;
  }
  return ret;
}