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

#include "sql/optimizer/ob_log_values_table_access.h"
#include "sql/optimizer/ob_opt_est_cost.h"
#include "sql/optimizer/ob_log_plan.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{
/**
 *  Print log info with expressions
 */
#define EXPLAIN_PRINT_VALUES_TABLE(access, type)                                   \
  {                                                                                \
    if (OB_FAIL(BUF_PRINTF(#access"("))) {                                         \
      LOG_WARN("fail to print to buf", K(ret));                                    \
    } else  {                                                                      \
      int64_t N = access.count();                                                  \
      if (N == 0) {                                                                \
        if (OB_FAIL(BUF_PRINTF("nil"))) {                                          \
          LOG_WARN("fail to print to buf", K(ret));                                \
        }                                                                          \
      } else {                                                                     \
        for (int64_t i = 0; OB_SUCC(ret) && i < N; i++) {                          \
          if (OB_ISNULL(access.at(i))) {                                           \
            ret = OB_ERR_UNEXPECTED;                                               \
          } else if (OB_FAIL(BUF_PRINTF("["))) {                                   \
            LOG_WARN("fail to print to buf", K(ret));                              \
          } else if (OB_FAIL(access.at(i)->get_name(buf, buf_len, pos, type))) {   \
          } else {                                                                 \
            if (i < N - 1) {                                                       \
              if (OB_FAIL(BUF_PRINTF("], "))) {                                   \
                LOG_WARN("fail to print to buf", K(ret));                          \
              }                                                                    \
            } else if (OB_FAIL(BUF_PRINTF("]"))) {                                 \
              LOG_WARN("fail to print to buf", K(ret));                            \
            }                                                                      \
          }                                                                        \
        }                                                                          \
      }                                                                            \
      if (OB_SUCC(ret)) {                                                          \
        if (OB_FAIL(BUF_PRINTF(")"))) {                                            \
          LOG_WARN("fail to print to buf", K(ret));                                \
        }                                                                          \
      }                                                                            \
    }                                                                              \
  }

int ObLogValuesTableAccess::compute_equal_set()
{
  int ret = OB_SUCCESS;
  set_output_equal_sets(&empty_expr_sets_);
  return ret;
}

int ObLogValuesTableAccess::compute_table_set()
{
  int ret = OB_SUCCESS;
  set_table_set(&empty_table_set_);
  return ret;
}

int ObLogValuesTableAccess::est_cost()
{
  int ret = OB_SUCCESS;
  double card = 0.0;
  double op_cost = 0.0;
  double cost = 0.0;
  EstimateCostInfo param;
  param.need_parallel_ = get_parallel();
  if (OB_FAIL(do_re_est_cost(param, card, op_cost, cost))) {
    LOG_WARN("failed to get re est cost infos", K(ret));
  } else {
    set_card(card);
    set_op_cost(op_cost);
    set_cost(cost);
  }
  return ret;
}

int ObLogValuesTableAccess::do_re_est_cost(EstimateCostInfo &param, double &card, double &op_cost, double &cost)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(get_values_path()) || OB_ISNULL(table_def_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    ObOptimizerContext &opt_ctx = get_plan()->get_optimizer_context();
    double read_rows = table_def_->row_cnt_;
    card = get_values_path()->get_path_output_rows();
    if (param.need_row_count_ >= 0 && param.need_row_count_ < card) {
      read_rows = read_rows * param.need_row_count_ / card;
      card = param.need_row_count_;
    }
    op_cost = ObOptEstCost::cost_values_table(read_rows, filter_exprs_, opt_ctx);
    cost = op_cost;
  }
  return ret;
}

int ObLogValuesTableAccess::get_op_exprs(ObIArray<ObRawExpr*> &all_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_def_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(ObLogicalOperator::get_op_exprs(all_exprs))) {
    LOG_WARN("failed to get op exprs", K(ret));
  } else if (OB_FAIL(append(all_exprs, table_def_->access_exprs_))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else if (OB_FAIL(append(all_exprs, column_exprs_))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObLogValuesTableAccess::allocate_expr_post(ObAllocExprContext &ctx)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < column_exprs_.count(); ++i) {
    ObColumnRefRawExpr *value_col = column_exprs_.at(i);
    if (OB_FAIL(mark_expr_produced(value_col, branch_id_, id_, ctx))) {
      LOG_WARN("makr expr produced failed", K(ret));
    } else if (!is_plan_root() && OB_FAIL(output_exprs_.push_back(value_col))) {
      LOG_WARN("failed to push back exprs", K(ret));
    } else { /*do nothing*/ }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObLogicalOperator::allocate_expr_post(ctx))) {
    LOG_WARN("failed to allocate expr post", K(ret));
  } else if (get_output_exprs().empty() && OB_FAIL(allocate_dummy_output())) {
    LOG_WARN("failed to allocate dummy output", K(ret));
  } else if (OB_FAIL(mark_probably_local_exprs())) {
    LOG_WARN("failed to mark local exprs", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObLogValuesTableAccess::get_plan_item_info(PlanText &plan_text,
                                        ObSqlPlanItem &plan_item)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObLogicalOperator::get_plan_item_info(plan_text, plan_item))) {
    LOG_WARN("failed to get plan item info", K(ret));
  } else {
    const ObIArray<ObColumnRefRawExpr*> &access = get_column_exprs();
    BEGIN_BUF_PRINT;
    EXPLAIN_PRINT_VALUES_TABLE(access, type);
    END_BUF_PRINT(plan_item.special_predicates_,
                  plan_item.special_predicates_len_);
    if (OB_SUCC(ret)) {
      const ObString &name = get_table_name();
      BUF_PRINT_OB_STR(name.ptr(),
                       name.length(),
                       plan_item.object_alias_,
                       plan_item.object_alias_len_);
      BUF_PRINT_STR("VALUES TABLE", plan_item.object_type_, plan_item.object_type_len_);
    }
  }
  return ret;
}

int ObLogValuesTableAccess::mark_probably_local_exprs()
{
  int ret = OB_SUCCESS;
  FOREACH_CNT_X(e, table_def_->access_exprs_, OB_SUCC(ret)) {
    CK(NULL != *e);
    OZ((*e)->add_flag(IS_PROBABLY_LOCAL));
  }

  return ret;
}

int ObLogValuesTableAccess::allocate_dummy_output()
{
  int ret = OB_SUCCESS;
  ObConstRawExpr *dummy_expr = NULL;
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (is_oracle_mode() && ObRawExprUtils::build_const_number_expr(
                                             get_plan()->get_optimizer_context().get_expr_factory(),
                                             ObNumberType,
                                             number::ObNumber::get_positive_one(),
                                             dummy_expr)) {
    LOG_WARN("failed to build const expr", K(ret));
  } else if (!is_oracle_mode() && ObRawExprUtils::build_const_int_expr(
                                             get_plan()->get_optimizer_context().get_expr_factory(),
                                             ObIntType,
                                             1,
                                             dummy_expr)) {
    LOG_WARN("failed to build const expr", K(ret));
  } else if (OB_ISNULL(dummy_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(dummy_expr->extract_info())) {
    LOG_WARN("failed to extract info for dummy expr", K(ret));
  } else if (OB_FAIL(output_exprs_.push_back(dummy_expr))) {
    LOG_WARN("failed to push back expr", K(ret));
  } else if (OB_FAIL(get_plan()->get_optimizer_context().get_all_exprs().append(dummy_expr))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObLogValuesTableAccess::inner_replace_op_exprs(ObRawExprReplacer &replacer)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_def_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(replace_exprs_action(replacer, table_def_->access_exprs_))) {
    LOG_WARN("failed to replace exprs", K(ret));
  }
  return ret;
}

int ObLogValuesTableAccess::is_my_fixed_expr(const ObRawExpr *expr, bool &is_fixed)
{
  is_fixed = ObOptimizerUtil::find_item(column_exprs_, expr);
  return OB_SUCCESS;
}

} // namespace sql
}// namespace oceanbase
