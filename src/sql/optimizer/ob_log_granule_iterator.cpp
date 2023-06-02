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
#include "sql/optimizer/ob_log_granule_iterator.h"

#include "sql/optimizer/ob_log_table_scan.h"
#include "ob_opt_est_cost.h"
#include "ob_select_log_plan.h"

using namespace oceanbase;
using namespace sql;
using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

const char *ObLogGranuleIterator::get_name() const
{
  static const char *gi_type[5] =
  {
    "LIGHT PARTITION ITERATOR",
    "PX PARTITION ITERATOR",
    "PX BLOCK ITERATOR",
    "PX PARTITION HASH JOIN-FILTER",
    "PX BLOCK HASH JOIN-FILTER"
  };
  const char *result = nullptr;
  bool is_part_gi = false;
  int tmp_ret = OB_SUCCESS;
  int64_t index = 0;
  if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = is_partition_gi(is_part_gi)))) {
    LOG_ERROR_RET(tmp_ret, "failed to check is partition gi", K(tmp_ret));
    index = 1;
  } else if (is_part_gi) {
    index = 1;
  } else {
    index = 2;
  }
  if (bf_info_.is_inited_ && index > 0) {
    index += 2;
  }
  result = gi_type[index];
  return result;
}

int ObLogGranuleIterator::get_op_exprs(ObIArray<ObRawExpr*> &all_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObLogicalOperator::get_op_exprs(all_exprs))) {
    LOG_WARN("failed to get exprs", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObLogGranuleIterator::get_plan_item_info(PlanText &plan_text,
                                             ObSqlPlanItem &plan_item)
{
  int ret = OB_SUCCESS;
  bool has_first = false;
  static const int64_t FLAG_NEED_PRINT_COUNT = 8;
  static const int32_t MAX_GI_FLAG_NAME_LENGTH = 30;
  static const char gi_flag_name[FLAG_NEED_PRINT_COUNT][MAX_GI_FLAG_NAME_LENGTH] =
      { "affinitize", "partition wise", "access all", "param down",
        "force partition granule", "slave mapping",
        "desc", "asc" };
  bool gi_flag[FLAG_NEED_PRINT_COUNT] =
      { affinitize(), pwj_gi(), access_all(), with_param_down(),
        force_partition_granule(), slave_mapping_granule(),
        desc_order(), asc_order() };
  if (OB_FAIL(ObLogicalOperator::get_plan_item_info(plan_text, plan_item))) {
    LOG_WARN("failed to get plan item info", K(ret));
  }
  BEGIN_BUF_PRINT;
  for (int64_t i = 0; OB_SUCC(ret) && i < FLAG_NEED_PRINT_COUNT; ++i) {
    if (!gi_flag[i]) {
      continue;
    } else if (has_first && OB_FAIL(BUF_PRINTF(", "))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (OB_FAIL(BUF_PRINTF("%.*s",
                                  MAX_GI_FLAG_NAME_LENGTH,
                                  gi_flag_name[i]))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else {
      has_first = true;
    }
  }
  END_BUF_PRINT(plan_item.special_predicates_,
                plan_item.special_predicates_len_);
  if (OB_SUCC(ret) &&
      get_join_filter_info().is_inited_ &&
      OB_INVALID_ID != get_join_filter_info().filter_id_) {
    BEGIN_BUF_PRINT;
    if (OB_FAIL(BUF_PRINTF(":RF%04ld", get_join_filter_info().filter_id_))) {
      LOG_WARN("failed to print str", K(ret));
    }
    END_BUF_PRINT(plan_item.object_alias_,
                  plan_item.object_alias_len_);
  }
  return ret;
}

int ObLogGranuleIterator::allocate_expr_post(ObAllocExprContext &ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObLogicalOperator::allocate_expr_post(ctx))) {
    LOG_WARN("failed to allocate expr post", K(ret));
  } else if (NULL != tablet_id_expr_ &&
            OB_FAIL(get_plan()->get_optimizer_context().get_all_exprs().append(tablet_id_expr_))) {
    LOG_WARN("failed to append expr", K(ret));
  }
  return ret;
}

int ObLogGranuleIterator::compute_op_ordering()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *child = NULL;
  if (OB_ISNULL(child = get_child(ObLogicalOperator::first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(ObLogicalOperator::compute_op_ordering())) {
    LOG_WARN("failed to compute ordering info", K(ret));
  } else if (!child->is_exchange_allocated() && child->get_is_range_order() &&
             OB_FAIL(set_range_order())) {
    LOG_WARN("failed to set partition order", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObLogGranuleIterator::set_range_order()
{
  int ret = OB_SUCCESS;
  common::ObIArray<OrderItem> &op_ordering = get_op_ordering();
  if (!op_ordering.empty()) {
    // Suppose (range) partition order is asc, so first order is same partition order
    bool is_asc_order = is_ascending_direction(op_ordering.at(0).order_type_);
    if (is_asc_order) {
      add_flag(GI_ASC_ORDER);
    } else {
      add_flag(GI_DESC_ORDER);
    }
    LOG_TRACE("partition order", K(is_asc_order), K(gi_attri_flag_), K(ret));
  } else if (affinitize()) {
    ObLogicalOperator *child = get_child(first_child);
    if (OB_ISNULL(child)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null operator", K(child), K(ret));
    } else if (child->get_op_ordering().count() <= 0) {
      //do nothing
    } else {
      common::ObIArray<OrderItem> &child_ordering = child->get_op_ordering();
      bool is_asc_order = is_ascending_direction(child_ordering.at(0).order_type_);
      if (is_asc_order) {
        add_flag(GI_ASC_ORDER);
      } else {
        add_flag(GI_DESC_ORDER);
      }
      LOG_TRACE("affinitize partition order", K(is_asc_order), K(gi_attri_flag_), K(ret));
    }
  }
  return ret;
}

int ObLogGranuleIterator::est_cost()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *child = NULL;
  if (OB_ISNULL(child = get_child(ObLogicalOperator::first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(child), K(ret));
  } else {
    card_ = child->get_card();
    op_cost_ = 0;
    cost_ = op_cost_ + child->get_cost();
  }
  return ret;
}

int ObLogGranuleIterator::is_partition_gi(bool &partition_granule) const
{
  int ret = OB_SUCCESS;
  ObOptimizerContext *optimizer_context = NULL;
  ObSQLSessionInfo *session_info = NULL;
  partition_granule = false;
  int64_t partition_scan_hold = 0;
  int64_t hash_partition_scan_hold = 0;
  if (OB_ISNULL(get_plan())
      || OB_ISNULL(optimizer_context = &get_plan()->get_optimizer_context())
      || OB_ISNULL(session_info = optimizer_context->get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Get unexpected null", K(get_plan()), K(session_info), K(ret));
  } else if (OB_FAIL(session_info->get_sys_variable(share::SYS_VAR__PX_PARTITION_SCAN_THRESHOLD, partition_scan_hold))) {
    LOG_WARN("failed to get sys variable px partition scan threshold", K(ret));
  } else if (OB_FAIL(session_info->get_sys_variable(share::SYS_VAR__PX_MIN_GRANULES_PER_SLAVE, hash_partition_scan_hold))) {
    LOG_WARN("failed to get sys variable px min granule per slave", K(ret));
  } else if (is_used_by_external_table()) {
  //external table only support block iter
    partition_granule = false;
  } else {
    partition_granule = ObGranuleUtil::is_partition_granule_flag(gi_attri_flag_)
        || ObGranuleUtil::is_partition_granule(partition_count_, parallel_, partition_scan_hold, hash_partition_scan_hold, hash_part_);
  }
  return ret;
}

void ObLogGranuleIterator::add_flag(uint64_t attri)
{
  gi_attri_flag_ |= attri;
}

}
}
