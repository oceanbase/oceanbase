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
#include "sql/optimizer/ob_opt_selectivity.h"
#include "sql/optimizer/ob_join_order.h"
#include "common/ob_smart_call.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::sql::log_op_def;

namespace oceanbase
{
namespace sql
{

int ObLogCount::est_cost()
{
  int ret = OB_SUCCESS;
  double sel = 1.0;
  ObLogicalOperator *child = NULL;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(child = get_child(ObLogicalOperator::first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(get_plan()), K(child), K(ret));
  } else if (OB_FALSE_IT(get_plan()->get_selectivity_ctx().init_op_ctx(
      &child->get_output_equal_sets(), child->get_card()))) {
  } else if (OB_FAIL(ObOptSelectivity::calculate_selectivity(get_plan()->get_update_table_metas(),
                                                             get_plan()->get_selectivity_ctx(),
                                                             get_filter_exprs(),
                                                             sel,
                                                             get_plan()->get_predicate_selectivities()))) {
    LOG_WARN("failed to calculate selectivity", K(ret));
  } else {
    double child_card = child->get_card();
    double child_cost = child->get_cost();
    if (OB_FAIL(inner_est_cost(child_card, child_cost, true, sel, op_cost_))) {
      LOG_WARN("failed to est count cost", K(ret));
    } else {
      set_cost(op_cost_ + child_cost);
      set_card(child_card * sel);
    }
  }
  return ret;
}

int ObLogCount::est_width()
{
  int ret = OB_SUCCESS;
  double width = 0.0;
  ObLogicalOperator *child = NULL;
  if (OB_ISNULL(child = get_child(ObLogicalOperator::first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(child), K(ret));
  } else {
    width = child->get_width() + 4;
    set_width(width);
  }
  return ret;
}


int ObLogCount::do_re_est_cost(EstimateCostInfo &param, double &card, double &op_cost, double &cost)
{
  int ret = OB_SUCCESS;
  double sel = 1.0;
  ObLogicalOperator *child = NULL;
  if (OB_ISNULL(get_plan()) ||
      OB_ISNULL(child = get_child(ObLogicalOperator::first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(get_plan()), K(child), K(ret));
  } else if (OB_FAIL(ObOptSelectivity::calculate_selectivity(get_plan()->get_basic_table_metas(),
                                                            get_plan()->get_selectivity_ctx(),
                                                            get_filter_exprs(),
                                                            sel,
                                                            get_plan()->get_predicate_selectivities()))) {
    LOG_WARN("failed to calc selectivity", K(ret));
  } else {
    double child_card = child->get_card();
    double child_cost = child->get_cost();
    if (param.need_row_count_ >= 0 && param.need_row_count_ < get_card()) {
      //child need row count
      if (sel > OB_DOUBLE_EPSINON) {
        param.need_row_count_ /= sel;
      }
    }
    if (OB_FAIL(SMART_CALL(child->re_est_cost(param, child_card, child_cost)))) {
      LOG_WARN("failed to est child cost", K(ret));
    } else if (OB_FAIL(inner_est_cost(child_card, child_cost, false, sel, op_cost))) {
      LOG_WARN("failed to est count cost", K(ret));
    } else {
      cost = op_cost + child_cost;
      card = child_card * sel;
    }
  }
  return ret;
}

int ObLogCount::inner_est_cost(double &child_card, 
                               double &child_cost, 
                               bool need_re_est_child_cost,  
                               double sel, 
                               double &op_cost)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *child = NULL;
  int64_t limit_count = 0.0;
  bool is_null_value = false;
  if (OB_ISNULL(get_plan()) ||
      OB_ISNULL(child = get_child(ObLogicalOperator::first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(get_plan()), K(child), K(ret));
  } else {
    ObOptimizerContext &opt_ctx = get_plan()->get_optimizer_context();
    if (NULL != rownum_limit_expr_ && 
        !rownum_limit_expr_->has_flag(CNT_DYNAMIC_PARAM)) {
      if (OB_FAIL(ObTransformUtils::get_limit_value(rownum_limit_expr_,
                                                    opt_ctx.get_params(),
                                                    opt_ctx.get_exec_ctx(),
                                                    &opt_ctx.get_allocator(),
                                                    limit_count,
                                                    is_null_value))) {
        LOG_WARN("Get limit count num error", K(ret));
      } else if (limit_count > child_card) {
        //do nothing
      } else if (sel > 0 && need_re_est_child_cost) {
        EstimateCostInfo param;
        param.need_row_count_ = limit_count / sel;
        param.override_ = false;
        if (OB_FAIL(child->re_est_cost(param, child_card, child_cost))) {
          LOG_WARN("failed to est child cost", K(ret));
        }
      }
    }
    op_cost = ObOptEstCost::cost_filter_rows(child_card, get_filter_exprs(),
                                             opt_ctx.get_cost_model_type());
  }
  return ret;
}

int ObLogCount::get_op_exprs(ObIArray<ObRawExpr*> &all_exprs)
{
  int ret = OB_SUCCESS;
  if (NULL != rownum_expr_ && OB_FAIL(all_exprs.push_back(rownum_expr_))) {
    LOG_WARN("failed to add exprs", K(ret));
  } else if (NULL != rownum_limit_expr_ && OB_FAIL(all_exprs.push_back(rownum_limit_expr_))) {
    LOG_WARN("failed to add exprs", K(ret));
  } else if (OB_FAIL(ObLogicalOperator::get_op_exprs(all_exprs))) {
    LOG_WARN("failed to get op exprs", K(ret));
  } else { /*do nothing*/ }

  return ret;
}

int ObLogCount::get_plan_item_info(PlanText &plan_text,
                                   ObSqlPlanItem &plan_item)
{
	int ret = OB_SUCCESS;
  if (OB_FAIL(ObLogicalOperator::get_plan_item_info(plan_text, plan_item))) {
    LOG_WARN("failed to get base plan item info", K(ret));
  } else if (NULL != rownum_limit_expr_) {
    BEGIN_BUF_PRINT;
    EXPLAIN_PRINT_EXPR(rownum_limit_expr_, type);
    END_BUF_PRINT(plan_item.special_predicates_,
                  plan_item.special_predicates_len_);
  }
	return ret;
}

int ObLogCount::inner_replace_op_exprs(ObRawExprReplacer &replacer)
{
  int ret = OB_SUCCESS;
  if (NULL != rownum_limit_expr_ && OB_FAIL(replace_expr_action(replacer, rownum_limit_expr_))) {
    LOG_WARN("failed to replace expr", K(ret));
  }
  return ret;
}

int ObLogCount::is_my_fixed_expr(const ObRawExpr *expr, bool &is_fixed)
{
  int ret = OB_SUCCESS;
  is_fixed = false;
  ObRawExpr *rownum_expr = NULL;
  if (OB_ISNULL(get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(get_stmt()));
  } else if (OB_FAIL(get_stmt()->get_rownum_expr(rownum_expr))) {
    LOG_WARN("failed to get rownum expr", K(ret));
  } else if (OB_ISNULL(rownum_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (expr == rownum_expr) {
    is_fixed = true;
  }
  return ret;
}

} /* namespace sql */
} /* namespace oceanbase */
