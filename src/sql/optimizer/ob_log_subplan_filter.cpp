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
#include "sql/optimizer/ob_log_subplan_filter.h"
#include "sql/optimizer/ob_opt_est_cost.h"
#include "sql/optimizer/ob_log_plan.h"
#include "sql/optimizer/ob_log_table_scan.h"
#include "common/ob_smart_call.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;

int ObLogSubPlanFilter::copy_without_child(ObLogicalOperator*& out)
{
  int ret = OB_SUCCESS;
  out = NULL;
  ObLogicalOperator* op = NULL;
  ObLogSubPlanFilter* sub_filter = NULL;
  if (OB_FAIL(clone(op))) {
    LOG_WARN("failed to clone subplan filter", K(ret));
  } else if (OB_ISNULL(sub_filter = static_cast<ObLogSubPlanFilter*>(op))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to cast ObLogicalOperator * to ObLogSubPlanFilter *", K(ret));
  } else {
    if (OB_FAIL(append(sub_filter->exec_params_, exec_params_))) {
      LOG_WARN("Append for exec_params_ fails", K(ret));
    } else if (OB_FAIL(append(sub_filter->onetime_exprs_, onetime_exprs_))) {
      LOG_WARN("Append for onetime_exprs_ fails", K(ret));
    } else {
      sub_filter->one_time_idxs_ = one_time_idxs_;
      sub_filter->init_plan_idxs_ = init_plan_idxs_;
      out = sub_filter;
    }
  }
  return ret;
}

int ObLogSubPlanFilter::re_calc_cost()
{
  int ret = OB_SUCCESS;
  const ObLogicalOperator* child = NULL;
  ObSEArray<ObBasicCostInfo, 4> children_cost_info;
  if (OB_ISNULL(child = get_child(ObLogicalOperator::first_child)) || OB_ISNULL(get_est_sel_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get child", K(ret), K(child), K(get_est_sel_info()));
  } else if (OB_FAIL(get_children_cost_info(children_cost_info))) {
    LOG_WARN("failed to get children cost info from subplan filter", K(ret));
  } else {
    ObSubplanFilterCostInfo info(children_cost_info, get_onetime_exprs(), get_initplan_idxs());
    double op_cost = 0.0;
    double cost = 0.0;
    if (OB_FAIL(ObOptEstCost::cost_subplan_filter(info, op_cost, cost))) {
      LOG_WARN("failed to calculate  the cost of subplan filter", K(ret));
    } else {
      set_op_cost(op_cost);
      set_cost(cost);
      set_width(child->get_width());
    }
  }
  return ret;
}

int ObLogSubPlanFilter::allocate_exchange_post(AllocExchContext* ctx)
{
  int ret = OB_SUCCESS;
  bool is_basic = false;
  bool is_partition_wise = false;
  ObLogicalOperator* child = NULL;
  if (OB_ISNULL(ctx) || OB_ISNULL(get_plan()) || OB_ISNULL(child = get_child(first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ctx), K(get_plan()), K(child), K(ret));
  } else if (OB_FAIL(compute_basic_sharding_info(ctx, is_basic))) {
    LOG_WARN("failed to compute basic sharding info", K(ret));
  } else if (is_basic) {
    LOG_TRACE("is basic sharding info", K(sharding_info_));
  } else if (OB_FAIL(check_if_match_partition_wise(*ctx, is_partition_wise))) {
    LOG_WARN("failed to check if match partition wise", K(ret));
  } else if (is_partition_wise) {
    if (OB_FAIL(sharding_info_.copy_with_part_keys(child->get_sharding_info()))) {
      LOG_WARN("copy with part keys fails", K(ret));
    } else {
      is_partition_wise_ = (NULL != sharding_info_.get_phy_table_location_info());
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < get_num_of_child(); ++i) {
      ObExchangeInfo exch_info;
      if (OB_ISNULL(child = get_child(i)) || OB_ISNULL(child->get_stmt())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(child), K(ret));
      } else if (child->get_sharding_info().is_sharding() && OB_FAIL(child->allocate_exchange(ctx, exch_info))) {
        LOG_WARN("failed to allocate exchange", K(i), K(ret));
      } else if (i > 0 && get_exec_params().empty() && child->get_stmt()->is_contains_assignment() &&
                 OB_FAIL(check_and_allocate_material(i))) {
        LOG_WARN("failed to check and allocate material", K(i), K(ret));
      } else { /*do nothing*/
      }
    }
    if (OB_SUCC(ret)) {
      sharding_info_.set_location_type(OB_TBL_LOCATION_LOCAL);
    }
  }
  return ret;
}

int ObLogSubPlanFilter::check_if_match_partition_wise(const AllocExchContext& ctx, bool& is_partition_wise)
{
  int ret = OB_SUCCESS;
  EqualSets sharding_input_esets;
  is_partition_wise = false;
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(get_sharding_input_equal_sets(sharding_input_esets))) {
    LOG_WARN("failed to get sharding input equal sets", K(ret));
  } else {
    ObLogicalOperator* child = NULL;
    ObSEArray<ObRawExpr*, 4> left_key;
    ObSEArray<ObRawExpr*, 4> right_key;
    is_partition_wise = true;
    for (int64_t i = 0; OB_SUCC(ret) && is_partition_wise && i < get_num_of_child(); i++) {
      left_key.reset();
      right_key.reset();
      if (OB_ISNULL(child = get_child(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (i == 0) {
        if (!child->get_sharding_info().is_distributed()) {
          is_partition_wise = false;
        } else { /*do nothing*/
        }
      } else {
        if (child->get_sharding_info().is_match_all()) {
          is_partition_wise = false;
        } else if (OB_FAIL(get_equal_key(ctx, child, left_key, right_key))) {
          LOG_WARN("failed to get join key", K(ret));
        } else if (OB_FAIL(ObShardingInfo::check_if_match_partition_wise(*get_plan(),
                       sharding_input_esets,
                       left_key,
                       right_key,
                       get_child(0)->get_sharding_info(),
                       child->get_sharding_info(),
                       is_partition_wise))) {
          LOG_WARN("failed to check match partition wise join", K(ret));
        } else { /*do nothing*/
        }
      }
    }
    if (OB_SUCC(ret)) {
      LOG_TRACE("succeed to check subplan filter matchs partition wise", K(is_partition_wise));
    }
  }
  return ret;
}

int ObLogSubPlanFilter::get_equal_key(const AllocExchContext& ctx, ObLogicalOperator* child,
    ObIArray<ObRawExpr*>& left_key, ObIArray<ObRawExpr*>& right_key)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 8> normal_conds;
  ObSEArray<ObRawExpr*, 8> nullsafe_conds;
  ObSEArray<ObRawExpr*, 8> nullsafe_left_key;
  ObSEArray<ObRawExpr*, 8> nullsafe_right_key;
  if (OB_FAIL(ObOptimizerUtil::classify_equal_conds(child->get_filter_exprs(), normal_conds, nullsafe_conds))) {
    LOG_WARN("failed to classify equal conds", K(ret));
  } else if (OB_FAIL(inner_get_equal_key(child, normal_conds, left_key, right_key))) {
    LOG_WARN("failed to get equal key", K(ret));
  } else if (OB_FAIL(inner_get_equal_key(child, nullsafe_conds, nullsafe_left_key, nullsafe_right_key))) {
    LOG_WARN("failed to get equal key", K(ret));
  }

  if (OB_SUCC(ret) && get_exec_params().count() > 0) {
    if (OB_FAIL(extract_correlated_keys(child, left_key, right_key, nullsafe_left_key, nullsafe_right_key))) {
      LOG_WARN("extract_correlated_keys error", K(ret));
    } else { /* Do nothing */
    }
  } else { /* Do nothing */
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(prune_weak_part_exprs(ctx, nullsafe_left_key, nullsafe_right_key, left_key, right_key))) {
      LOG_WARN("failed to prune weak part exprs", K(ret));
    }
  }
  return ret;
}

int ObLogSubPlanFilter::inner_get_equal_key(ObLogicalOperator* child, ObIArray<ObRawExpr*>& filters,
    ObIArray<ObRawExpr*>& left_key, ObIArray<ObRawExpr*>& right_key)
{
  int ret = OB_SUCCESS;
  ObSelectStmt* right_stmt = NULL;
  if (OB_ISNULL(child) || OB_ISNULL(child->get_stmt()) || OB_UNLIKELY(!child->get_stmt()->is_select_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid child opeartor", K(ret));
  } else {
    right_stmt = static_cast<ObSelectStmt*>(child->get_stmt());
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < filters.count(); ++i) {
    ObRawExpr* expr = filters.at(i);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is null", K(ret));
    } else if (T_OP_SQ_EQ == expr->get_expr_type() || T_OP_SQ_NSEQ == expr->get_expr_type() ||
               T_OP_EQ == expr->get_expr_type() || T_OP_NSEQ == expr->get_expr_type()) {
      ObRawExpr* left_hand = NULL;
      ObRawExpr* right_hand = NULL;
      if (OB_ISNULL(left_hand = expr->get_param_expr(0)) || OB_ISNULL(right_hand = expr->get_param_expr(1))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is invalid", K(ret), K(left_hand), K(right_hand));
      } else if (!right_hand->is_query_ref_expr()) {
        // do nothing
      } else if (static_cast<ObQueryRefRawExpr*>(right_hand)->get_ref_operator() != child) {
        // do nothing
      } else if (T_OP_ROW == left_hand->get_expr_type()) {
        ObOpRawExpr* row_expr = static_cast<ObOpRawExpr*>(left_hand);
        if (row_expr->get_param_count() != right_stmt->get_select_item_size()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expr size does not match", K(ret), K(*row_expr), K(right_stmt->get_select_items()));
        } else { /* Do nothing */
        }
        for (int64_t j = 0; OB_SUCC(ret) && j < row_expr->get_param_count(); ++j) {
          if (OB_FAIL(left_key.push_back(row_expr->get_param_expr(j))) ||
              OB_FAIL(right_key.push_back(right_stmt->get_select_item(j).expr_))) {
            LOG_WARN("push back error", K(ret));
          } else { /* Do nothing */
          }
        }
      } else {
        if (OB_UNLIKELY(1 != right_stmt->get_select_item_size())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("select item size should be 1", K(ret), K(right_stmt->get_select_item_size()));
        } else if (OB_FAIL(left_key.push_back(left_hand)) ||
                   OB_FAIL(right_key.push_back(right_stmt->get_select_item(0).expr_))) {
          LOG_WARN("push back error", K(ret));
        } else { /* Do nothing */
        }
      }
    } else { /* Do nothing */
    }
  }
  return ret;
}

int ObLogSubPlanFilter::extract_correlated_keys(const ObLogicalOperator* op, ObIArray<ObRawExpr*>& left_key,
    ObIArray<ObRawExpr*>& right_key, ObIArray<ObRawExpr*>& nullsafe_left_key, ObIArray<ObRawExpr*>& nullsafe_right_key)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  ObSEArray<ObRawExpr*, 8> conditions;
  ObSEArray<ObRawExpr*, 8> normal_conds;
  ObSEArray<ObRawExpr*, 8> nullsafe_conds;
  if (OB_ISNULL(op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("op is null", K(ret));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("check stack overflow failed", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret));
  } else if (OB_FAIL(append(conditions, op->get_filter_exprs()))) {
    LOG_WARN("failed to append filter exprs", K(ret));
  } else if (log_op_def::LOG_TABLE_SCAN == op->get_type()) {
    const ObLogTableScan* table_scan = static_cast<const ObLogTableScan*>(op);
    if (NULL == table_scan->get_pre_query_range()) {
      // do nothing
    } else if (OB_FAIL(append(conditions, table_scan->get_pre_query_range()->get_range_exprs()))) {
      LOG_WARN("failed to append range exprs", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(ObOptimizerUtil::classify_equal_conds(conditions, normal_conds, nullsafe_conds))) {
    LOG_WARN("failed to classify equal conditions", K(ret));
  } else if (OB_FAIL(
                 ObOptimizerUtil::extract_equal_exec_params(normal_conds, get_exec_params(), left_key, right_key))) {
    LOG_WARN("failed to extract equal exec params", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::extract_equal_exec_params(
                 nullsafe_conds, get_exec_params(), nullsafe_left_key, nullsafe_right_key))) {
    LOG_WARN("failed to extract equal exec params", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < op->get_num_of_child(); ++i) {
    if (OB_FAIL(SMART_CALL(
            extract_correlated_keys(op->get_child(i), left_key, right_key, nullsafe_left_key, nullsafe_right_key)))) {
      LOG_WARN("extract_correlated_keys error", K(ret));
    } else { /* Do nothing */
    }
  }
  return ret;
}

int ObLogSubPlanFilter::allocate_expr_pre(ObAllocExprContext& ctx)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 16> subquery_exprs;
  if (OB_FAIL(get_subquery_exprs(subquery_exprs))) {
    LOG_WARN("failed to get subquery exprs", K(ret));
  } else if (OB_FAIL(ObLogicalOperator::add_exprs_to_ctx(ctx, subquery_exprs))) {
    LOG_WARN("failed to add exprs to ctx", K(ret));
  } else if (OB_FAIL(ObLogicalOperator::allocate_expr_pre(ctx))) {
    LOG_WARN("failed to add parent need expr", K(ret));
  } else {
    // add operator exec params
    ObSEArray<ObRawExpr*, 8> exprs;
    for (int64_t i = 0; OB_SUCC(ret) && i < exec_params_.count(); ++i) {
      if (OB_ISNULL(exec_params_.at(i).second)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(exprs.push_back(exec_params_.at(i).second))) {
        LOG_WARN("failed to push back exprs", K(ret));
      } else { /*do nothing*/
      }
    }
    // Add onetime_exprs
    for (int64_t i = 0; OB_SUCC(ret) && i < onetime_exprs_.count(); ++i) {
      if (OB_ISNULL(onetime_exprs_.at(i).second)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(exprs.push_back(onetime_exprs_.at(i).second))) {
        LOG_WARN("failed to push back exprs", K(ret));
      } else { /*do nothing*/
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(add_exprs_to_ctx(ctx, exprs))) {
        LOG_WARN("failed to add exprs to ctx", K(ret));
      } else { /*do nothing*/
      }
    }
    // set producer id for subquery expr
    for (int64_t i = 0; OB_SUCC(ret) && i < ctx.expr_producers_.count(); ++i) {
      ExprProducer& producer = ctx.expr_producers_.at(i);
      if (OB_ISNULL(producer.expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (producer.expr_->has_flag(IS_SUB_QUERY) &&
                 is_my_subquery_expr(static_cast<const ObQueryRefRawExpr*>(producer.expr_)) &&
                 (OB_INVALID_ID == producer.producer_id_ || id_ != producer.producer_id_)) {
        producer.producer_id_ = id_;
        for (int64_t j = 0; OB_SUCC(ret) && j < ctx.expr_producers_.count(); j++) {
          ExprProducer& other = ctx.expr_producers_.at(j);
          if (OB_ISNULL(other.expr_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null", K(ret));
          } else if (other.expr_ == producer.expr_ || other.producer_id_ == id_ ||
                     !should_be_produced_by_subplan_filter(producer.expr_, other.expr_)) {
            /*do nothing*/
          } else {
            other.producer_id_ = id_;
          }
        }
      } else if (is_my_onetime_expr(producer.expr_)) {
        producer.producer_id_ = id_;
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

bool ObLogSubPlanFilter::should_be_produced_by_subplan_filter(const ObRawExpr* subexpr, const ObRawExpr* expr)
{
  bool bret = false;
  if (NULL != expr && NULL != subexpr) {
    if (is_fix_producer_expr(*expr) && !expr->is_query_ref_expr()) {
      /*do nothing*/
    } else if (subexpr == expr) {
      bret = true;
    } else {
      for (int64_t i = 0; !bret && i < expr->get_param_count(); i++) {
        bret = should_be_produced_by_subplan_filter(subexpr, expr->get_param_expr(i));
      }
    }
  }
  return bret;
}

int ObLogSubPlanFilter::get_subquery_exprs(ObIArray<ObRawExpr*>& subquery_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_stmt()) || OB_ISNULL(get_stmt()->get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    ObIArray<ObQueryRefRawExpr*>& all_subquery_exprs = get_stmt()->get_subquery_exprs();
    ObIArray<ObRawExpr*>& all_exec_exprs = get_stmt()->get_query_ctx()->exec_param_ref_exprs_;
    for (int64_t i = 0; OB_SUCC(ret) && i < all_subquery_exprs.count(); i++) {
      if (is_my_subquery_expr(all_subquery_exprs.at(i))) {
        ret = subquery_exprs.push_back(all_subquery_exprs.at(i));
      } else { /*do nothing*/
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < all_exec_exprs.count(); i++) {
      if (is_my_onetime_expr(all_exec_exprs.at(i))) {
        ret = subquery_exprs.push_back(all_exec_exprs.at(i));
      }
    }
  }
  return ret;
}

bool ObLogSubPlanFilter::is_my_onetime_expr(const ObRawExpr* expr)
{
  bool bret = false;
  if (NULL != expr) {
    if (expr->has_flag(IS_EXEC_PARAM)) {
      int64_t param_value = static_cast<const ObConstRawExpr*>(expr)->get_value().get_unknown();
      if (NULL != ObOptimizerUtil::find_exec_param(onetime_exprs_, param_value)) {
        bret = true;
      } else {
        bret = false;
      }
    } else {
      bret = false;
    }
  }
  return bret;
}

bool ObLogSubPlanFilter::is_my_subquery_expr(const ObQueryRefRawExpr* query_expr)
{
  bool bret = false;
  if (NULL != query_expr) {
    const ObLogicalOperator* ref_op = query_expr->get_ref_operator();
    for (int64_t i = 1; !bret && i < get_num_of_child(); ++i) {
      if (OB_NOT_NULL(get_child(i)) && query_expr->get_ref_stmt() == get_child(i)->get_stmt()) {
        bret = true;
      }
    }
  }
  return bret;
}

int ObLogSubPlanFilter::check_output_dep_specific(ObRawExprCheckDep& checker)
{
  int ret = OB_SUCCESS;
  // subplan filter related param expr
  for (int64_t i = 0; OB_SUCC(ret) && i < exec_params_.count(); ++i) {
    if (OB_ISNULL(exec_params_.at(i).second)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("exec_params_.at(i).second is null", K(ret));
    } else if (OB_FAIL(checker.check(*exec_params_.at(i).second))) {
      LOG_WARN("failed to check exec_params_.at(i)", K(i), K(ret));
    } else {
    }
  }
  // onetime exprs
  for (int64_t j = 0; OB_SUCC(ret) && j < onetime_exprs_.count(); ++j) {
    if (OB_ISNULL(onetime_exprs_.at(j).second)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("onetime_exprs_.at(j).second is null", K(ret));
    } else if (OB_FAIL(checker.check(*onetime_exprs_.at(j).second))) {
      LOG_WARN("failed to check onetime_exprs_.at(j)", K(j), K(ret));
    } else {
    }
  }
  return ret;
}

int ObLogSubPlanFilter::print_my_plan_annotation(char* buf, int64_t& buf_len, int64_t& pos, ExplainType type)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(BUF_PRINTF(", "))) {
    LOG_WARN("BUF_PRINTF fails", K(ret));
  } else if (OB_FAIL(BUF_PRINTF("\n      "))) {
    LOG_WARN("BUF_PRINTF fails", K(ret));
  } else { /* Do nothing */
  }
  if (OB_SUCC(ret)) {
    EXPLAIN_PRINT_EXEC_PARAMS(exec_params_, type);
  } else { /* Do nothing */
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(BUF_PRINTF(", "))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else { /* Do nothing */
    }
  } else { /* Do nothing */
  }
  if (OB_SUCC(ret)) {
    EXPLAIN_PRINT_EXEC_PARAMS(onetime_exprs_, type);
  } else { /* Do nothing */
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(BUF_PRINTF(", "))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else { /* Do nothing */
    }
  } else { /* Do nothing */
  }
  if (OB_SUCC(ret)) {
    EXPLAIN_PRINT_IDXS(init_plan_idxs_);
  } else { /* Do nothing */
  }

  return ret;
}

int ObLogSubPlanFilter::gen_filters()
{
  return OB_SUCCESS;
}
int ObLogSubPlanFilter::gen_output_columns()
{
  return OB_SUCCESS;
}

int ObLogSubPlanFilter::transmit_local_ordering()
{
  int ret = OB_SUCCESS;
  reset_local_ordering();
  ObLogicalOperator* child = get_child(first_child);
  if (OB_ISNULL(child)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("first child is null", K(ret));
  } else if (OB_FAIL(set_local_ordering(child->get_local_ordering()))) {
    LOG_WARN("fail to set local ordering", K(ret));
  }
  return ret;
}

int ObLogSubPlanFilter::transmit_op_ordering()
{
  int ret = OB_SUCCESS;
  reset_op_ordering();
  if (OB_ISNULL(get_child(first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("first child is null", K(ret));
  } else if (OB_FAIL(set_op_ordering(get_child(first_child)->get_op_ordering()))) {
    LOG_WARN("failed to set op ordering", K(ret));
  } else if (OB_FAIL(transmit_local_ordering())) {
    LOG_WARN("failed to transmit local ordering", K(ret));
  } else {
  }
  return ret;
}

int ObLogSubPlanFilter::est_cost()
{
  int ret = OB_SUCCESS;
  const ObLogicalOperator* child = NULL;
  ObSEArray<ObBasicCostInfo, 4> children_cost_info;
  if (OB_ISNULL(child = get_child(ObLogicalOperator::first_child)) || OB_ISNULL(get_est_sel_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get child", K(ret), K(child), K(get_est_sel_info()));
  } else if (OB_FAIL(get_children_cost_info(children_cost_info))) {
    LOG_WARN("failed to get children cost info from subplan filter", K(ret));
  } else {
    ObSubplanFilterCostInfo info(children_cost_info, get_onetime_exprs(), get_initplan_idxs());
    double op_cost = 0.0;
    double cost = 0.0;
    double sel = 1.0;
    if (OB_FAIL(ObOptEstCost::cost_subplan_filter(info, op_cost, cost))) {
      LOG_WARN("failed to calculate  the cost of subplan filter", K(ret));
    } else if (OB_FAIL(ObOptEstSel::calculate_selectivity(
                   *get_est_sel_info(), get_filter_exprs(), sel, &get_plan()->get_predicate_selectivities()))) {
      LOG_WARN("failed to calc selectivity", K(ret));
    } else {
      set_op_cost(op_cost);
      set_cost(cost);
      set_width(child->get_width());
      set_card(child->get_card() * sel);
    }
  }
  return ret;
}

int ObLogSubPlanFilter::re_est_cost(const ObLogicalOperator* parent, double need_row_count, bool& re_est)
{
  int ret = OB_SUCCESS;
  UNUSED(parent);
  re_est = false;
  if (need_row_count >= get_card()) {
    /*do nothing*/
  } else {
    ObLogicalOperator* first = get_child(first_child);
    if (OB_ISNULL(first)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("first or second child is NULL", K(ret), K(first));
    } else if (OB_FAIL(first->re_est_cost(this, first->get_card() * need_row_count / get_card(), re_est))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("re-estimate cost child failed", K(ret));
    } else if (OB_FAIL(est_cost())) {
      LOG_WARN("failed to estimate cost", K(ret));
    } else {
      set_card(need_row_count);
    }
  }
  return ret;
}

int ObLogSubPlanFilter::get_children_cost_info(ObIArray<ObBasicCostInfo>& children_cost_info)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < get_num_of_child(); ++i) {
    const ObLogicalOperator* child = get_child(i);
    if (OB_ISNULL(child)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("SubPlanFilter has no i-th child", K(ret), K(i));
    } else {
      ObBasicCostInfo info(child->get_card(), child->get_cost(), child->get_width());
      if (OB_FAIL(children_cost_info.push_back(info))) {
        LOG_WARN("push back child's cost info failed", K(ret));
      }
    }
  }

  return ret;
}

uint64_t ObLogSubPlanFilter::hash(uint64_t seed) const
{
  for (int64_t i = 0; i < exec_params_.count(); i++) {
    seed = do_hash(exec_params_.at(i).first, seed);
    if (NULL != exec_params_.at(i).second) {
      seed = do_hash(*exec_params_.at(i).second, seed);
    }
  }
  for (int64_t i = 0; i < onetime_exprs_.count(); i++) {
    seed = do_hash(onetime_exprs_.at(i).first, seed);
    if (NULL != onetime_exprs_.at(i).second) {
      seed = do_hash(*onetime_exprs_.at(i).second, seed);
    }
  }
  ObArray<int64_t> init_plan_idx_array;
  ObArray<int64_t> one_time_idx_array;
  int tmp_ret = 0;
  if (OB_SUCCESS != (tmp_ret = init_plan_idxs_.to_array(init_plan_idx_array))) {
    LOG_WARN("fail to convert to array", K(init_plan_idxs_), K(tmp_ret));
  } else {
    for (int64_t i = 0; i < init_plan_idx_array.count(); i++) {
      seed = do_hash(init_plan_idx_array.at(i), seed);
    }
  }
  if (OB_SUCCESS != (tmp_ret = one_time_idxs_.to_array(one_time_idx_array))) {
    LOG_WARN("fail to convert to array", K(one_time_idxs_), K(tmp_ret));
  } else {
    for (int64_t i = 0; i < one_time_idx_array.count(); i++) {
      seed = do_hash(one_time_idx_array.at(i), seed);
    }
  }
  seed = ObLogicalOperator::hash(seed);

  return seed;
}

int ObLogSubPlanFilter::allocate_granule_pre(AllocGIContext& ctx)
{
  return pw_allocate_granule_pre(ctx);
}

int ObLogSubPlanFilter::allocate_granule_post(AllocGIContext& ctx)
{
  return pw_allocate_granule_post(ctx);
}

int ObLogSubPlanFilter::inner_append_not_produced_exprs(ObRawExprUniqueSet& raw_exprs) const
{
  int ret = OB_SUCCESS;
  // We replace expr with onetime expr in resolve_subplan_params() after ALLOC_EXPR,
  // we need to add onetime expr here.
  CK(NULL != get_stmt());
  FOREACH_CNT_X(e, onetime_exprs_, OB_SUCC(ret))
  {
    CK(e->first >= 0);
    if (OB_SUCC(ret)) {
      ObRawExpr* param_expr = ObOptimizerUtil::find_param_expr(get_stmt()->get_exec_param_ref_exprs(), e->first);
      CK(NULL != param_expr);
      OZ(raw_exprs.append(param_expr));
    }
  }
  FOREACH_CNT_X(e, exec_params_, OB_SUCC(ret))
  {
    CK(e->first >= 0);
    if (OB_SUCC(ret)) {
      ObRawExpr* param_expr = ObOptimizerUtil::find_param_expr(get_stmt()->get_exec_param_ref_exprs(), e->first);
      CK(NULL != param_expr);
      OZ(raw_exprs.append(param_expr));
    }
  }
  return ret;
}

int ObLogSubPlanFilter::compute_one_row_info()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* child = NULL;
  if (OB_ISNULL(child = get_child(ObLogicalOperator::first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(child), K(ret));
  } else {
    set_is_at_most_one_row(child->get_is_at_most_one_row());
  }
  return ret;
}
