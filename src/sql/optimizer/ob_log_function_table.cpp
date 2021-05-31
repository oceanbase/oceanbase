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

#include "sql/optimizer/ob_log_function_table.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "sql/optimizer/ob_opt_est_cost.h"
#include "sql/optimizer/ob_log_plan.h"

using namespace oceanbase::common;

namespace oceanbase {
namespace sql {

int ObLogFunctionTable::copy_without_child(ObLogicalOperator*& out)
{
  int ret = OB_SUCCESS;
  out = NULL;
  ObLogicalOperator* op = NULL;
  ObLogFunctionTable* function_table = NULL;
  if (OB_FAIL(clone(op))) {
    LOG_WARN("failed to clone ObLogFunctionTable", K(ret));
  } else if (OB_ISNULL(function_table = static_cast<ObLogFunctionTable*>(op))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to cast ObLogicalOperator * to ObLogFunctionTable *", K(ret));
  } else {
    function_table->add_values_expr(value_expr_);
    function_table->set_table_id(table_id_);
    out = function_table;
  }
  return ret;
}

int ObLogFunctionTable::allocate_exchange_post(AllocExchContext* ctx)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* child = NULL;
  if (OB_ISNULL(ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ctx), K(child), K(ret));
  } else {
    child = get_child(first_child);
    if (child != NULL) {
      if (OB_FAIL(sharding_info_.copy_with_part_keys(child->get_sharding_info()))) {
        LOG_WARN("failed to deep copy sharding info from child", K(ret));
      } else { /*do nothing*/
      }
    } else {
      sharding_info_.set_location_type(OB_TBL_LOCATION_ALL);
    }
  }
  return ret;
}

int ObLogFunctionTable::compute_fd_item_set()
{
  int ret = OB_SUCCESS;
  set_fd_item_set(NULL);
  return ret;
}

int ObLogFunctionTable::compute_equal_set()
{
  int ret = OB_SUCCESS;
  set_ordering_output_equal_sets(&empty_expr_sets_);
  set_sharding_output_equal_sets(&empty_expr_sets_);
  return ret;
}

int ObLogFunctionTable::compute_op_ordering()
{
  int ret = OB_SUCCESS;
  ObArray<ObRawExpr*> select_exprs;
  if (OB_ISNULL(get_stmt()) || OB_UNLIKELY(!get_stmt()->is_select_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is invalid", K(ret));
  } else if (OB_FAIL(static_cast<ObSelectStmt*>(get_stmt())->get_select_exprs(select_exprs))) {
    LOG_WARN("failed to get select exprs", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::copy_sort_keys(select_exprs, op_ordering_))) {
    LOG_WARN("failed to copy sort keys", K(ret));
  }
  return ret;
}

int ObLogFunctionTable::compute_table_set()
{
  int ret = OB_SUCCESS;
  set_table_set(&empty_table_set_);
  return ret;
}

int ObLogFunctionTable::est_cost()
{
  int ret = OB_SUCCESS;
  // set cost and card
  set_card(10.0);
  set_cost(ObOptEstCost::get_cost_params().CPU_OPERATOR_COST *
               static_cast<double>(get_stmt()->get_condition_exprs().count()) +
           ObOptEstCost::get_cost_params().CPU_TUPLE_COST);
  set_op_cost(get_cost());
  return ret;
}

int ObLogFunctionTable::generate_access_exprs(ObIArray<ObRawExpr*>& access_exprs) const
{
  int ret = OB_SUCCESS;
  const ObDMLStmt* stmt = get_stmt();
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_column_size(); ++i) {
      const ColumnItem* col_item = stmt->get_column_item(i);
      if (OB_ISNULL(col_item) || OB_ISNULL(col_item->expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (col_item->table_id_ == table_id_ && col_item->expr_->is_explicited_reference()) {
        ret = access_exprs.push_back(col_item->expr_);
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObLogFunctionTable::allocate_expr_pre(ObAllocExprContext& ctx)
{
  int ret = OB_SUCCESS;
  if (this->get_num_of_child() > 0) {
    uint64_t producer_id = OB_INVALID_ID;
    ObArray<ObRawExpr*> needed_exprs;
    if (OB_FAIL(needed_exprs.push_back(value_expr_))) {
      LOG_WARN("failed to push back", K(ret));
    } else if (OB_FAIL(get_next_producer_id(get_child(first_child), producer_id))) {
      LOG_WARN("failed to get next producer id", K(ret));
    } else if (OB_FAIL(add_exprs_to_ctx(ctx, needed_exprs, producer_id))) {
      LOG_WARN("failed to add exprs to ctx", K(ret));
    } else { /*do nothing*/
    }
  }

  if (OB_SUCC(ret) && OB_FAIL(ObLogicalOperator::allocate_expr_pre(ctx))) {
    LOG_WARN("failed to allocate parent expr pre", K(ret));
  }
  return ret;
}

int ObLogFunctionTable::allocate_expr_post(ObAllocExprContext& ctx)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 8> access_exprs;
  if (OB_FAIL(generate_access_exprs(access_exprs))) {
    LOG_WARN("failed to generate access exprs", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < access_exprs.count(); ++i) {
      ObRawExpr* value_col = access_exprs.at(i);
      bool expr_is_required = false;
      if (OB_FAIL(mark_expr_produced(value_col, branch_id_, id_, ctx, expr_is_required))) {
        LOG_WARN("makr expr produced failed", K(ret));
      } else if (!is_plan_root() && expr_is_required) {
        if (OB_FAIL(add_var_to_array_no_dup(output_exprs_, value_col))) {
          LOG_WARN("add expr no duplicate key failed", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObLogicalOperator::allocate_expr_post(ctx))) {
        LOG_WARN("failed to allocate expr post", K(ret));
      }
    }
  }
  return ret;
}

int ObLogFunctionTable::check_output_dep_specific(ObRawExprCheckDep& checker)
{
  int ret = OB_SUCCESS;
  if (NULL != value_expr_) {
    if (OB_FAIL(checker.check(*value_expr_))) {
      LOG_WARN("failed to check limit_count_", K(*value_expr_), K(ret));
    }
  }
  return ret;
}

int ObLogFunctionTable::print_my_plan_annotation(char* buf, int64_t& buf_len, int64_t& pos, ExplainType type)
{
  int ret = OB_SUCCESS;
  const ObRawExpr* value = get_value_expr();
  int64_t tmp_pos = pos;
  CK(OB_NOT_NULL(buf));
  CK(OB_NOT_NULL(value));
  OZ(BUF_PRINTF("\n      "));
  OZ(BUF_PRINTF("TABLE("));
  OZ(value->get_name(buf, buf_len, pos, type));
  OZ(BUF_PRINTF(")"));
  if (OB_FAIL(ret)) {
    pos = tmp_pos;
  }
  return ret;
}

uint64_t ObLogFunctionTable::hash(uint64_t seed) const
{
  seed = do_hash(table_id_, seed);
  seed = ObOptimizerUtil::hash_expr(value_expr_, seed);
  seed = ObLogicalOperator::hash(seed);
  return seed;
}

int ObLogFunctionTable::inner_append_not_produced_exprs(ObRawExprUniqueSet& raw_exprs) const
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 8> access_exprs;
  OZ(generate_access_exprs(access_exprs));
  OZ(raw_exprs.append(access_exprs));

  return ret;
}

}  // namespace sql
}  // namespace oceanbase
