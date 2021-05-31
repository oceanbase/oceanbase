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
#include "ob_log_select_into.h"
#include "ob_log_table_scan.h"
#include "ob_optimizer_util.h"
#include "ob_opt_est_cost.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::sql::log_op_def;

int ObLogSelectInto::allocate_expr_pre(ObAllocExprContext& ctx)
{
  int ret = OB_SUCCESS;
  if (into_type_ == T_INTO_OUTFILE) {
    ObSelectStmt* stmt = static_cast<ObSelectStmt*>(get_stmt());
    ObSelectIntoItem* into_item = NULL;
    if (OB_ISNULL(stmt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_ISNULL(into_item = stmt->get_select_into())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("into item is null", K(ret));
    } else {
      common::ObSEArray<ObRawExpr*, 16> select_items_exprs;
      ObRawExpr* to_outfile_expr = NULL;
      uint64_t producer_id = OB_INVALID_ID;
      if (OB_FAIL(stmt->get_select_exprs(select_items_exprs))) {
        LOG_WARN("failed to get_select_exprs", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::build_to_outfile_expr(get_plan()->get_optimizer_context().get_expr_factory(),
                     get_plan()->get_optimizer_context().get_session_info(),
                     into_item,
                     select_items_exprs,
                     to_outfile_expr))) {
        LOG_WARN("failed to build_to_outfile_expr", K(*into_item), K(ret));
      } else if (OB_FAIL(output_exprs_.push_back(to_outfile_expr))) {
        LOG_WARN("failed to append to_outfile expr", K(ret));
      } else if (OB_FAIL(get_next_producer_id(get_child(first_child), producer_id))) {
        LOG_WARN("failed to get non exchange child id", K(ret));
      } else if (OB_FAIL(add_exprs_to_ctx(ctx, output_exprs_, producer_id))) {
        LOG_WARN("failed to add exprs to ctx", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObLogicalOperator::allocate_expr_pre(ctx))) {
      LOG_WARN("failed to allocate parent expr pre", K(ret));
    }
  }
  return ret;
}

int ObLogSelectInto::allocate_exchange_post(AllocExchContext* ctx)
{
  int ret = OB_SUCCESS;
  bool is_basic = false;
  ObLogicalOperator* child = NULL;
  ObExchangeInfo exch_info;
  if (OB_ISNULL(ctx) || OB_ISNULL(child = get_child(first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(compute_basic_sharding_info(ctx, is_basic))) {
    LOG_WARN("failed to compute basic sharding info", K(ret));
  } else if (is_basic && !sharding_info_.is_remote()) {
    // select into outfile must output into the local file, so need to execute local
    LOG_TRACE("is basic sharding info", K(sharding_info_));
  } else if (OB_FAIL(child->allocate_exchange(ctx, exch_info))) {
    LOG_WARN("failed to allocate exchange", K(ret));
  } else {
    sharding_info_.set_location_type(OB_TBL_LOCATION_LOCAL);
    get_plan()->set_location_type(ObPhyPlanType::OB_PHY_PLAN_UNCERTAIN);
  }
  return ret;
}

int ObLogSelectInto::transmit_op_ordering()
{
  int ret = OB_SUCCESS;
  bool need_sort = false;
  if (OB_FAIL(check_need_sort_below_node(0, expected_ordering_, need_sort))) {
    LOG_WARN("failed to check need sort", K(ret));
  } else if (!need_sort) {
    // do nothing
  } else if (OB_FAIL(allocate_sort_below(0, expected_ordering_))) {
    LOG_WARN("failed to allocate sort", K(ret));
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObLogicalOperator::transmit_op_ordering())) {
      LOG_WARN("failed to transmit op ordering", K(ret));
    }
  }
  return ret;
}

int ObLogSelectInto::copy_without_child(ObLogicalOperator*& out)
{
  int ret = OB_SUCCESS;
  out = NULL;
  ObLogicalOperator* op = NULL;
  ObLogSelectInto* select_into = NULL;
  if (OB_FAIL(clone(op))) {
    LOG_WARN("failed to clone ObLogLimit", K(ret));
  } else if (OB_ISNULL(select_into = static_cast<ObLogSelectInto*>(op))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to cast to ObLogSelectInto *", K(ret));
  } else {
    select_into->set_outfile_name(outfile_name_);
    select_into->set_filed_str(filed_str_);
    select_into->set_line_str(line_str_);
    select_into->set_user_vars(user_vars_);
    select_into->set_is_optional(is_optional_);
    select_into->set_closed_cht(closed_cht_);
  }
  return ret;
}

uint64_t ObLogSelectInto::hash(uint64_t seed) const
{
  uint64_t hash_value = seed;
  hash_value = do_hash(into_type_, hash_value);
  hash_value = outfile_name_.hash(hash_value);
  hash_value = filed_str_.hash(hash_value);
  hash_value = line_str_.hash(hash_value);
  for (int i = 0; i < user_vars_.count(); ++i) {
    ObString var_str = user_vars_.at(i);
    hash_value = var_str.hash(hash_value);
  }
  hash_value = do_hash(closed_cht_, hash_value);
  hash_value = do_hash(is_optional_, hash_value);
  hash_value = ObLogicalOperator::hash(hash_value);

  return hash_value;
}
