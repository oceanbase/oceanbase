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
#include "ob_log_topk.h"
#include "ob_log_group_by.h"
#include "ob_log_operator_factory.h"
#include "ob_log_sort.h"
#include "ob_log_table_scan.h"
#include "ob_optimizer_util.h"
#include "ob_opt_est_cost.h"
#include "sql/rewrite/ob_transform_utils.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::sql::log_op_def;

int ObLogTopk::set_topk_params(ObRawExpr *limit_count, ObRawExpr *limit_offset,
                               int64_t minimum_row_count, int64_t topk_precision)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(limit_count)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("limit_count is NULL", K(ret));
  } else {
    topk_limit_count_ = limit_count;
    topk_limit_offset_ = limit_offset;
    minimum_row_count_ = minimum_row_count;
    topk_precision_ = topk_precision;
  }
  return ret;
}

int ObLogTopk::get_op_exprs(ObIArray<ObRawExpr*> &all_exprs)
{
  int ret = OB_SUCCESS;
  if (NULL != topk_limit_count_ && OB_FAIL(all_exprs.push_back(topk_limit_count_))) {
    LOG_WARN("failed to push back exprs", K(ret));
  } else if (NULL != topk_limit_offset_ && OB_FAIL(all_exprs.push_back(topk_limit_offset_))) {
    LOG_WARN("failed to push back exprs", K(ret));
  } else if (OB_FAIL(ObLogicalOperator::get_op_exprs(all_exprs))) {
    LOG_WARN("failed to get exprs", K(ret));
  } else { /*do nothing*/ }

  return ret;
}

int ObLogTopk::est_width()
{
  int ret = OB_SUCCESS;
  double width = 0.0;
  ObSEArray<ObRawExpr*, 16> output_exprs;
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid plan", K(ret));
  } else if (OB_FAIL(get_topk_output_exprs(output_exprs))) {
    LOG_WARN("failed to get topk output exprs", K(ret));
  } else if (OB_FAIL(ObOptEstCost::estimate_width_for_exprs(get_plan()->get_basic_table_metas(),
                                                            get_plan()->get_selectivity_ctx(),
                                                            output_exprs,
                                                            width))) {
    LOG_WARN("failed to estimate width for output topk exprs", K(ret));
  } else {
    set_width(width);
    LOG_TRACE("est width for topk", K(output_exprs), K(width));
  }
  return ret;
}

int ObLogTopk::get_topk_output_exprs(ObIArray<ObRawExpr *> &output_exprs)
{
  int ret = OB_SUCCESS;
  ObLogPlan *plan = NULL;
  ObSEArray<ObRawExpr*, 16> candi_exprs;
  ObSEArray<ObRawExpr*, 16> extracted_col_aggr_winfunc_exprs;
  if (OB_ISNULL(plan = get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid input", K(ret));
  } else if (OB_FAIL(append_array_no_dup(candi_exprs, plan->get_select_item_exprs_for_width_est()))) {
    LOG_WARN("failed to add into output exprs", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::extract_col_aggr_winfunc_exprs(candi_exprs,
                                                                    extracted_col_aggr_winfunc_exprs))) {
  } else if (OB_FAIL(append_array_no_dup(output_exprs, extracted_col_aggr_winfunc_exprs))) {
    LOG_WARN("failed to add into output exprs", K(ret));
  } else {/*do nothing*/}
  return ret;
}

int ObLogTopk::est_cost()
{
  int ret = OB_SUCCESS;
  int64_t limit_count = 0;
  int64_t offset_count = 0;
  bool is_null_value = false;
  int64_t parallel = 0.0;
  ObLogicalOperator *child = NULL;
  if (OB_ISNULL(child = get_child(first_child)) || OB_ISNULL(get_stmt()) ||
      OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(child), K(get_stmt()), K(get_plan()),K(ret));
  } else if (OB_UNLIKELY((parallel = get_parallel()) < 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected parallel degree", K(ret)); 
  } else if (OB_FAIL(ObTransformUtils::get_limit_value(topk_limit_count_,
                                                       get_plan()->get_optimizer_context().get_params(),
                                                       get_plan()->get_optimizer_context().get_exec_ctx(),
                                                       &get_plan()->get_optimizer_context().get_allocator(),
                                                       limit_count,
                                                       is_null_value))) {
    LOG_WARN("get limit count num error", K(ret));
  } else if (!is_null_value &&
             OB_FAIL(ObTransformUtils::get_limit_value(topk_limit_offset_,
                                                       get_plan()->get_optimizer_context().get_params(),
                                                       get_plan()->get_optimizer_context().get_exec_ctx(),
                                                       &get_plan()->get_optimizer_context().get_allocator(),
                                                       offset_count,
                                                       is_null_value))) {
    LOG_WARN("Get limit offset num error", K(ret));  
  } else {
    limit_count = is_null_value ? 0 : limit_count + offset_count;
    double topk_card = static_cast<double>(std::max(minimum_row_count_, limit_count));
    double row_count = child->get_card() * static_cast<double>(topk_precision_) / 100.0;
    topk_card = std::max(topk_card, row_count);
    topk_card = std::min(topk_card, child->get_card());
    ObOptimizerContext &opt_ctx = get_plan()->get_optimizer_context();
    double op_cost = ObOptEstCost::cost_get_rows(topk_card / parallel,
                                                 opt_ctx);
    set_card(topk_card);
    set_op_cost(op_cost);
    set_cost(child->get_cost() + op_cost);
  }
  return ret;
}

int ObLogTopk::get_plan_item_info(PlanText &plan_text,
                                  ObSqlPlanItem &plan_item)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObLogicalOperator::get_plan_item_info(plan_text, plan_item))) {
    LOG_WARN("failed to get plan item info", K(ret));
  }
  BEGIN_BUF_PRINT;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(BUF_PRINTF("minimum_row_count:%ld top_precision:%ld ",
                                minimum_row_count_, topk_precision_))) {
    LOG_WARN("BUF_PRINTF fails", K(ret));
  } else {
    ObRawExpr *limit = topk_limit_count_;
    ObRawExpr *offset = topk_limit_offset_;
    EXPLAIN_PRINT_EXPR(limit, type);
    BUF_PRINTF(", ");
    EXPLAIN_PRINT_EXPR(offset, type);
    END_BUF_PRINT(plan_item.special_predicates_,
                  plan_item.special_predicates_len_);
  }
  return ret;
}
