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
#include "sql/optimizer/ob_log_join.h"
#include "sql/optimizer/ob_log_subplan_filter.h"

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
  bool is_part_gi = is_partition_gi();
  int64_t index = 0;
  if (is_part_gi) {
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
  const ObDMLStmt *stmt = NULL;
  common::ObIArray<OrderItem> &op_ordering = get_op_ordering();
  if (OB_ISNULL(stmt = get_plan()->get_stmt()) || OB_ISNULL(stmt->get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null param", K(ret));
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
  } else if (!op_ordering.empty()) {
    // Suppose (range) partition order is asc, so first order is same partition order
    bool is_asc_order = is_ascending_direction(op_ordering.at(0).order_type_);
    bool used = true;
    if (stmt->get_query_ctx()->check_opt_compat_version(COMPAT_VERSION_4_2_3, COMPAT_VERSION_4_3_0,
                                                        COMPAT_VERSION_4_3_2) &&
        OB_FAIL(check_op_orderding_used_by_parent(used))) {
      LOG_WARN("failed to check op ordering used by parent", K(ret));
    } else if (!used) {
      //do nothing
    } else if (is_asc_order) {
      add_flag(GI_ASC_ORDER);
    } else {
      add_flag(GI_DESC_ORDER);
    }
    LOG_TRACE("partition order", K(is_asc_order), K(gi_attri_flag_), K(ret));
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

bool ObLogGranuleIterator::is_partition_gi() const
{
  bool partition_granule = true;
  if (is_used_by_external_table() && !is_used_by_lake_table()) {
    // external table only support block iter
    partition_granule = false;
  } else {
    partition_granule = ObGranuleUtil::is_partition_granule_flag(gi_attri_flag_) || parallel_ == 1;
  }
  return partition_granule;
}

void ObLogGranuleIterator::add_flag(uint64_t attri)
{
  gi_attri_flag_ |= attri;
  // when gi_attri_flag_ changed, px adaptive task splitting may lose effectiveness.
  if (enable_adaptive_task_splitting_ && !ObGranuleUtil::can_resplit_gi_task(gi_attri_flag_)) {
    enable_adaptive_task_splitting_ = false;
    if (OB_NOT_NULL(controlled_tsc_)) {
      static_cast<ObLogTableScan *>(controlled_tsc_)->set_scan_resumable(false);
    }
  }
}

int ObLogGranuleIterator::check_adaptive_task_splitting(ObLogTableScan *tsc)
{
  int ret = OB_SUCCESS;
  int64_t tenant_id =
        get_plan()->get_optimizer_context().get_session_info()->get_effective_tenant_id();
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  bool exist_deadlock_condition = false;
  uint64_t min_version = GET_MIN_CLUSTER_VERSION();
  bool version_check =
      min_version >= CLUSTER_VERSION_4_5_1_0
      || (min_version >= MOCK_CLUSTER_VERSION_4_4_2_0 && min_version < CLUSTER_VERSION_4_5_0_0)
      || (min_version >= MOCK_CLUSTER_VERSION_4_3_5_3 && min_version < CLUSTER_VERSION_4_4_0_0);
  if (!version_check) {
  } else if (!tenant_config.is_valid() || !tenant_config->_enable_px_task_rebalance) {
  } else if (!ObGranuleUtil::can_resplit_gi_task(gi_attri_flag_)) {
  } else if (is_rescanable()) {
    // for rescanable gi, we can not handle the rescan process among all workers since gi task
    // changes cross several rescan tasks
  } else if (OB_FAIL(check_exist_deadlock_condition(this, exist_deadlock_condition))) {
    LOG_WARN("failed to check_exist_deadlock_condition");
  } else if (exist_deadlock_condition) {
    // adaptive task splitting will add a synchronize point which may cause deadlock with other
    // synchronize point so disable this feature in some cases
  } else if (OB_ISNULL(tsc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr");
  } else {
    // find the table scan which can be paused
    SampleInfo::SampleMethod sample_method = tsc->get_sample_info().method_;
    bool is_table_get = false;
    // pre_graph maybe null
    const ObQueryRangeProvider *pre_graph = tsc->get_pre_graph();
    if (tsc->get_scan_order() == common::ObQueryFlag::ScanOrder::NoOrder) {
      // not support for delete insert noorder scan
    } else if (!tsc->get_pushdown_aggr_exprs().empty()
               || !tsc->get_pushdown_groupby_columns().empty()) {
      // not support for aggregate/group by push down
    } else if (tsc->get_index_back()) {
      // not support for index back
    } else if (tsc->is_tsc_with_domain_id()) {
      // not support for access domain id in full text index
    } else if (tsc->use_das()) {
      // not support das split
    } else if (tsc->get_table_type() == share::schema::EXTERNAL_TABLE) {
      // not support external table now
    } else if (nullptr != pre_graph && tsc->get_pre_graph()->is_ss_range()) {
      // not support in skip scan scene
    } else if (nullptr != pre_graph && OB_FAIL(pre_graph->is_get(is_table_get))) {
      LOG_WARN("failed to do is_table_get");
    } else if (is_table_get) {
      // meaningless to split task again
    } else if (sample_method != SampleInfo::NO_SAMPLE) {
      // not support for sample scan
    } else {
      controlled_tsc_ = tsc;
      tsc->set_scan_resumable(true);
      // gi attribution may changed, here is not the final decision.
      enable_adaptive_task_splitting_ = true;
    }
  }
  return ret;
}

bool ObLogGranuleIterator::is_rescanable()
{
  bool is_rescanable = false;
  ObLogicalOperator *cur_op = this;
  while (cur_op != nullptr && cur_op->get_parent() != nullptr) {
    ObLogicalOperator *parent_op = cur_op->get_parent();
    ObLogicalOperator *child_op = cur_op;
    if (log_op_def::LOG_JOIN == parent_op->get_type()) {
      ObLogJoin *log_join = static_cast<ObLogJoin *>(parent_op);
      if (JoinAlgo::NESTED_LOOP_JOIN == log_join->get_join_algo()
          && log_join->get_child(second_child) == child_op
          && OB_NOT_NULL(log_join->get_join_path())
          && !log_join->get_join_path()->need_mat_) {
        // GI in NLJ right branch without material is rescanable
        is_rescanable = true;
        break;
      }
    } else if (log_op_def::LOG_SUBPLAN_FILTER == parent_op->get_type()) {
      ObLogSubPlanFilter *sub_plan_filter = static_cast<ObLogSubPlanFilter *>(parent_op);
      for (int64_t i = 1; i < parent_op->get_num_of_child(); ++i) {
        if (parent_op->get_child(i) != child_op) {
        } else if (sub_plan_filter->get_onetime_idxs().has_member(i)
                   || sub_plan_filter->get_initplan_idxs().has_member(i)) {
          // onetime expr / initial plan is not rescanable
        } else {
          is_rescanable = true;
          break;
        }
      }
    }
    cur_op = parent_op;
  }
  return is_rescanable;
}

int ObLogGranuleIterator::branch_has_exchange(const ObLogicalOperator *op, bool &has_exchange)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr");
  } else if (op->get_type() == log_op_def::LOG_EXCHANGE) {
    has_exchange = true;
  } else {
    for (int64_t i = 0; i < op->get_num_of_child() && OB_SUCC(ret) && !has_exchange; ++i) {
      if (OB_FAIL(SMART_CALL(branch_has_exchange(op->get_child(i), has_exchange)))) {
        LOG_WARN("failed to branch_has_exchange");
      }
    }
  }
  return ret;
}


// see
// if gi on the left of join, and there is a receive on the right of join,
// it will generate a deadlock scene.
int ObLogGranuleIterator::check_exist_deadlock_condition(const ObLogicalOperator *op, bool &exist)
{
  // 1. find parent who has more than one child
  // 2. find if there is any receive op in the right branch of this parent
  int ret = OB_SUCCESS;
  const ObLogicalOperator *parent_op = op->get_parent();
  if (OB_ISNULL(op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr");
  } else if (op->is_block_op()) {
    // block op will block all child, stop finding deadlock recursively
  } else if (OB_ISNULL(parent_op)) {
  } else if (parent_op->get_type() == log_op_def::LOG_EXCHANGE) {
    // no crossing dfo search
  } else if (parent_op->get_num_of_child() == 1) {
    if (OB_FAIL(SMART_CALL(check_exist_deadlock_condition(parent_op, exist)))) {
      LOG_WARN("failed to check_exist_deadlock_condition");
    }
  } else {
    bool continue_check = true;
    int64_t this_child_idx = -1;
    // find this op is which child of parent
    for (int64_t i = 0; i < parent_op->get_num_of_child() && OB_SUCC(ret); ++i) {
      if (parent_op->get_child(i) == op) {
        this_child_idx = i;
        if (parent_op->is_block_input(i)) {
          continue_check = false;
        }
        break;
      }
    }
    // find if there is exchange node in another right branch
    if (continue_check && this_child_idx != -1) {
      bool has_exchange = false;
      for (int64_t i = this_child_idx + 1; i < parent_op->get_num_of_child() && OB_SUCC(ret); ++i) {
        if (OB_FAIL(branch_has_exchange(parent_op->get_child(i), has_exchange))) {
          LOG_WARN("failed to branch_has_exchange");
        } else if (has_exchange) {
          exist = true;
          break;
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (exist || !continue_check) {
    } else if (OB_FAIL(SMART_CALL(check_exist_deadlock_condition(parent_op, exist)))) {
      LOG_WARN("failed to check_exist_deadlock_condition");
    }
  }
  return ret;
}

}
}
