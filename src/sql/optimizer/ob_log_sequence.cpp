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
#include "ob_log_sequence.h"
#include "ob_log_operator_factory.h"
#include "ob_optimizer_util.h"
#include "sql/optimizer/ob_opt_est_cost.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::sql::log_op_def;

int ObLogSequence::allocate_exchange_post(AllocExchContext* ctx)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* child = NULL;
  ObDMLStmt* stmt = NULL;
  ObExchangeInfo exch_info;
  if (OB_ISNULL(ctx) || OB_ISNULL(stmt = get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ctx), K(stmt), K(ret));
  } else if (stmt::T_INSERT == stmt->get_stmt_type() && 0 == get_num_of_child()) {
  } else if (OB_ISNULL(child = get_child(first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (child->get_sharding_info().is_local() || child->get_sharding_info().is_match_all()) {
    if (OB_FAIL(sharding_info_.copy_with_part_keys(child->get_sharding_info()))) {
      LOG_WARN("failed to deep copy sharding info from first child", K(ret));
    } else { /*do nothing*/
    }
  } else {
    // remote or distribution
    if (OB_FAIL(child->allocate_exchange(ctx, exch_info))) {
      LOG_WARN("failed to allocate exchange", K(ret));
    } else {
      sharding_info_.set_location_type(ObTableLocationType::OB_TBL_LOCATION_LOCAL);
    }
  }
  return ret;
}

int ObLogSequence::copy_without_child(ObLogicalOperator*& out)
{
  int ret = OB_SUCCESS;
  out = NULL;
  ObLogicalOperator* op = NULL;
  ObLogSequence* sequence = NULL;
  if (OB_FAIL(clone(op))) {
    LOG_WARN("failed to clone ObLogSequence", K(ret));
  } else if (OB_ISNULL(sequence = static_cast<ObLogSequence*>(op))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to cast ObLogicalOperator * to ObLogSequence *", K(ret));
  } else {
    ARRAY_FOREACH_X(nextval_seq_ids_, idx, cnt, OB_SUCC(ret))
    {
      ret = sequence->nextval_seq_ids_.push_back(nextval_seq_ids_.at(idx));
    }
    out = sequence;
  }
  return ret;
}

uint64_t ObLogSequence::hash(uint64_t seed) const
{
  uint64_t hash_value = seed;
  hash_value = ObOptimizerUtil::hash_array(hash_value, nextval_seq_ids_);
  hash_value = ObLogicalOperator::hash(hash_value);
  return hash_value;
}

int ObLogSequence::print_my_plan_annotation(char* buf, int64_t& buf_len, int64_t& pos, ExplainType type)
{
  int ret = OB_SUCCESS;
  UNUSED(buf);
  UNUSED(buf_len);
  UNUSED(pos);
  UNUSED(type);
  return ret;
}

int ObLogSequence::est_cost()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* child = NULL;
  if (0 == get_num_of_child()) {
    op_cost_ = ObOptEstCost::cost_sequence(0, nextval_seq_ids_.count());
    cost_ = op_cost_;
    card_ = 0.0;
    width_ = 0.0;
  } else if (OB_ISNULL(child = get_child(first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    op_cost_ = ObOptEstCost::cost_sequence(child->get_card(), nextval_seq_ids_.count());
    cost_ = op_cost_ + child->get_cost();
    card_ = child->get_card();
    width_ = child->get_width() + nextval_seq_ids_.count() * 8;
  }
  return ret;
}
