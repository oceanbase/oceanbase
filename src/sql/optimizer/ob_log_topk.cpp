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

int ObLogTopk::set_topk_params(
    ObRawExpr* limit_count, ObRawExpr* limit_offset, int64_t minimum_row_count, int64_t topk_precision)
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

int ObLogTopk::copy_without_child(ObLogicalOperator*& out)
{
  int ret = OB_SUCCESS;
  out = NULL;
  ObLogicalOperator* op = NULL;
  ObLogTopk* topk = NULL;
  if (OB_FAIL(clone(op))) {
    LOG_WARN("failed to clone ObLogTopk", K(ret));
  } else if (OB_ISNULL(topk = static_cast<ObLogTopk*>(op))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to cast ObLogicalOperator * to ObLogTopk *", K(ret));
  } else if (OB_FAIL(
                 topk->set_topk_params(topk_limit_count_, topk_limit_offset_, minimum_row_count_, topk_precision_))) {
    LOG_WARN("failed to set_topk_params", K(ret));
  } else {
    out = topk;
  }
  return ret;
}

int ObLogTopk::allocate_exchange_post(AllocExchContext* ctx)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* child = NULL;
  if (OB_ISNULL(ctx) || OB_ISNULL(child = get_child(first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(sharding_info_.copy_with_part_keys(child->get_sharding_info()))) {
    LOG_WARN("failed to deep copy sharding info from child", K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

int ObLogTopk::allocate_exchange(AllocExchContext* ctx, bool parts_order)
{
  UNUSED(ctx);
  UNUSED(parts_order);
  int ret = OB_ERR_UNEXPECTED;
  LOG_WARN("should not come here", K(ret));
  return ret;
}

int ObLogTopk::set_topk_size()
{
  int ret = OB_SUCCESS;
  int64_t limit_count = 0;
  int64_t offset_count = 0;
  double child_card = 0;
  bool is_null_value = false;
  if (OB_ISNULL(get_child(first_child)) || OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpeced null", K(ret));
  } else if (FALSE_IT(child_card = get_child(first_child)->get_card())) {
  } else if (OB_FAIL(ObTransformUtils::get_limit_value(topk_limit_count_,
                 get_plan()->get_stmt(),
                 get_plan()->get_optimizer_context().get_params(),
                 get_plan()->get_optimizer_context().get_session_info(),
                 &get_plan()->get_optimizer_context().get_allocator(),
                 limit_count,
                 is_null_value))) {
    LOG_WARN("Get limit count num error", K(ret));
  } else if (!is_null_value && OB_FAIL(ObTransformUtils::get_limit_value(topk_limit_offset_,
                                   get_plan()->get_stmt(),
                                   get_plan()->get_optimizer_context().get_params(),
                                   get_plan()->get_optimizer_context().get_session_info(),
                                   &get_plan()->get_optimizer_context().get_allocator(),
                                   offset_count,
                                   is_null_value))) {
    LOG_WARN("Get limit offset num error", K(ret));
  } else {
    limit_count = is_null_value ? 0 : limit_count + offset_count;
    double topk_card = static_cast<double>(std::max(minimum_row_count_, limit_count));
    double row_count = child_card * static_cast<double>(topk_precision_) / 100.0;
    topk_card = std::max(topk_card, row_count);
    topk_card = std::min(topk_card, child_card);
    set_card(topk_card);
    if (OB_SUCC(ret)) {
      double op_cost = ObOptEstCost::cost_limit(get_card());
      set_op_cost(op_cost);
      set_cost(get_child(first_child)->get_cost() + op_cost);
    }
  }
  return ret;
}

uint64_t ObLogTopk::hash(uint64_t seed) const
{
  uint64_t hash_value = seed;
  hash_value = do_hash(minimum_row_count_, topk_precision_);
  hash_value = ObOptimizerUtil::hash_expr(topk_limit_count_, hash_value);
  hash_value = ObOptimizerUtil::hash_expr(topk_limit_offset_, hash_value);
  hash_value = ObLogicalOperator::hash(hash_value);
  return hash_value;
}

int ObLogTopk::check_output_dep_specific(ObRawExprCheckDep& checker)
{
  int ret = OB_SUCCESS;

  if (NULL != topk_limit_count_) {
    if (OB_FAIL(checker.check(*topk_limit_count_))) {
      LOG_WARN("failed to check limit_count_", K(*topk_limit_count_), K(ret));
    }
  } else { /* Do nothing */
  }

  if (OB_SUCC(ret)) {
    if (NULL != topk_limit_offset_) {
      if (OB_FAIL(checker.check(*topk_limit_offset_))) {
        LOG_WARN("failed to check limit_offset_", KPC(topk_limit_offset_), K(ret));
      }
    } else { /* Do nothing */
    }
  }
  return ret;
}

int ObLogTopk::print_my_plan_annotation(char* buf, int64_t& buf_len, int64_t& pos, ExplainType type)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(BUF_PRINTF(", minimum_row_count:%ld top_precision:%ld ", minimum_row_count_, topk_precision_))) {
    LOG_WARN("BUF_PRINTF fails", K(ret));
  } else {
    ObRawExpr* limit = topk_limit_count_;
    ObRawExpr* offset = topk_limit_offset_;
    EXPLAIN_PRINT_EXPR(limit, type);
    BUF_PRINTF(", ");
    EXPLAIN_PRINT_EXPR(offset, type);
  }
  return ret;
}

int ObLogTopk::inner_append_not_produced_exprs(ObRawExprUniqueSet& raw_exprs) const
{
  int ret = OB_SUCCESS;
  OZ(raw_exprs.append(topk_limit_count_));
  OZ(raw_exprs.append(topk_limit_offset_));

  return ret;
}
