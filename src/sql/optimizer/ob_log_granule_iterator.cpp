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

namespace oceanbase {
namespace sql {

const char* ObLogGranuleIterator::get_name() const
{
  static const char* gi_type[3] = {"LIGHT PARTITION ITERATOR", "PX PARTITION ITERATOR", "PX BLOCK ITERATOR"};
  const char* result = nullptr;
  bool is_part_gi = false;
  int tmp_ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = is_partition_gi(is_part_gi)))) {
    LOG_ERROR("failed to check is partition gi", K(tmp_ret));
    result = gi_type[1];
  } else if (is_part_gi) {
    result = gi_type[1];
  } else {
    result = gi_type[2];
  }
  return result;
}

int ObLogGranuleIterator::copy_without_child(ObLogicalOperator*& out)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* op = NULL;
  ObLogGranuleIterator* granule_iterator = NULL;
  if (OB_FAIL(clone(op))) {
    SQL_OPT_LOG(WARN, "failed to clone ObLogGranuleIterator", K(ret));
  } else if (OB_ISNULL(granule_iterator = static_cast<ObLogGranuleIterator*>(op))) {
    ret = OB_ERR_UNEXPECTED;
    SQL_OPT_LOG(WARN, "failed to cast ObLogicalOperator * to ObLogGranuleIterator *", K(ret));
  } else {
    granule_iterator->tablet_size_ = tablet_size_;
    granule_iterator->gi_attri_flag_ = gi_attri_flag_;
    granule_iterator->parallel_ = parallel_;
    granule_iterator->partition_count_ = partition_count_;
    granule_iterator->hash_part_ = hash_part_;
    out = granule_iterator;
  }
  return ret;
}

int ObLogGranuleIterator::allocate_expr_pre(ObAllocExprContext& ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("argument is invalid", K(ret), K(get_plan()));
  } else if (OB_FAIL(ObLogicalOperator::allocate_expr_pre(ctx))) {
    LOG_WARN("allocate expr post failed", K(ret));
  } else if (get_plan()->get_optimizer_context().is_batched_multi_stmt()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < ctx.expr_producers_.count(); i++) {
      ExprProducer& expr_producer = ctx.expr_producers_.at(i);
      const ObRawExpr* expr = expr_producer.expr_;
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null", K(ret));
      } else if (T_FUN_SYS_STMT_ID == expr->get_expr_type() && OB_INVALID_ID == expr_producer.producer_id_) {
        // stmt_id_expr is only produced by granule iterator
        expr_producer.producer_id_ = id_;
        LOG_DEBUG("expr is marked by granule operator.",
            K(ret),
            KPC(expr),
            K(branch_id_),
            K(expr_producer.producer_id_),
            K(expr_producer.consumer_id_));
      }
    }
  }
  return ret;
}

int ObLogGranuleIterator::allocate_exchange_post(AllocExchContext* ctx)
{
  int ret = OB_NOT_SUPPORTED;
  UNUSED(ctx);
  return ret;
}

int ObLogGranuleIterator::print_my_plan_annotation(char* buf, int64_t& buf_len, int64_t& pos, ExplainType type)
{
  int ret = OB_SUCCESS;

  if (OB_SUCC(ret) && (EXPLAIN_EXTENDED == type || EXPLAIN_EXTENDED_NOADDR == type) &&
      (affinitize() || pwj_gi() || access_all() || with_param_down() || desc_partition_order() ||
          force_partition_granule())) {
    if (OB_FAIL(BUF_PRINTF(", "))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (OB_FAIL(BUF_PRINTF("\n      "))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    }
    if (OB_SUCC(ret) && affinitize()) {
      if (OB_FAIL(BUF_PRINTF("affinitize, "))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      }
    }
    if (OB_SUCC(ret) && pwj_gi()) {
      if (OB_FAIL(BUF_PRINTF("partition wise, "))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      }
    }
    if (OB_SUCC(ret) && access_all()) {
      if (OB_FAIL(BUF_PRINTF("access all, "))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      }
    }
    if (OB_SUCC(ret) && with_param_down()) {
      if (OB_FAIL(BUF_PRINTF("param down, "))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      }
    }
    if (OB_SUCC(ret) && force_partition_granule()) {
      if (OB_FAIL(BUF_PRINTF("force partition granule, "))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      }
    }
    if (OB_SUCC(ret) && slave_mapping_granule()) {
      if (OB_FAIL(BUF_PRINTF("slave mapping, "))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (desc_partition_order()) {
        if (OB_FAIL(BUF_PRINTF("desc."))) {
          LOG_WARN("BUF_PRINTF fails", K(ret));
        }
      } else {
        if (OB_FAIL(BUF_PRINTF("asc."))) {
          LOG_WARN("BUF_PRINTF fails", K(ret));
        }
      }
    }
  } else { /* Do nothing */
  }
  return ret;
}

int ObLogGranuleIterator::transmit_local_ordering()
{
  int ret = OB_SUCCESS;
  reset_local_ordering();
  ObLogicalOperator* child = get_child(first_child);
  if (NULL == child) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get child for granule iterator", K(ret));
  } else if (OB_FAIL(set_local_ordering(child->get_local_ordering()))) {
    LOG_WARN("failed to set local ordering", K(ret));
  } else if (0 == get_local_ordering().count() && OB_FAIL(set_local_ordering(child->get_op_ordering()))) {
    LOG_WARN("failed to set local ordering", K(ret));
  }
  return ret;
}

// If tablescan can keep partitions order, and GI can output partition order
// then GI will keep partitions order, and don't care the internal process operator
// eg:
//   Granule iterator
//     sort(c1)
//       hash join(c2=d2)
//         t1(c1,c2) and partition order keys is (c1)
//         t2(d2)
// it's also keep partition orders
// now the plan may not be generated, but GI can support
// traverse the all children of granule iterator
// if all operator can keep ordering then set op ordering for GI and set table scan need keep partition order
int ObLogGranuleIterator::is_partitions_ordering(bool& partition_order)
{
  int ret = OB_SUCCESS;
  // 1. find the order items
  ObLogicalOperator* child = get_child(first_child);
  partition_order = false;
  if (OB_ISNULL(child)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get child for granule iterator", K(ret));
  } else {
    common::ObIArray<OrderItem>& orderings = child->get_op_ordering();
    // 2. find the tablescan that order items belongs to
    bool is_same = false;
    uint64_t table_id = UINT64_MAX;
    if (OB_FAIL(ObOptimizerUtil::is_same_table(orderings, table_id, is_same))) {
      LOG_WARN("failed to judge same table", K(ret));
    } else if (is_same) {
      // 3. find the leftmost tablescan
      ObLogicalOperator* tsc = nullptr;
      if (OB_FAIL(child->get_table_scan(tsc, table_id))) {
        LOG_WARN("failed to get table scan", K(ret));
      } else if (nullptr != tsc &&
                 OB_FAIL(static_cast<ObLogTableScan*>(tsc)->is_prefix_of_partition_key(orderings, partition_order))) {
        LOG_WARN("failed to judge prefix order", K(ret));
      } else {
        LOG_TRACE("gi partition order", K(orderings), K(partition_order));
      }
    } else {
      LOG_TRACE("order items are same table", K(is_same));
    }
  }
  return ret;
}

int ObLogGranuleIterator::set_partition_order()
{
  int ret = OB_SUCCESS;
  common::ObIArray<OrderItem>& op_ordering = get_op_ordering();
  if (0 >= op_ordering.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("op ordering is empty", K(ret));
  } else {
    // Suppose (range) partition order is asc, so first order is same partition order
    bool is_asc_partition_order = is_ascending_direction(op_ordering.at(0).order_type_);
    if (is_asc_partition_order) {
      add_flag(GI_ASC_PARTITION_ORDER);
    } else {
      add_flag(GI_DESC_PARTITION_ORDER);
    }
    add_flag(GI_FORCE_PARTITION_GRANULE);
    LOG_TRACE("partition order", K(is_asc_partition_order), K(gi_attri_flag_), K(ret));
  }
  return ret;
}

int ObLogGranuleIterator::transmit_op_ordering()
{
  int ret = OB_SUCCESS;
  bool partition_order = false;
  // Granule iterator don't keep ordering but maybe retain local ordering
  reset_op_ordering();
  ObLogicalOperator* child = get_child(first_child);
  if (NULL == child) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get child for granule iterator", K(ret));
  } else if (OB_FAIL(is_partitions_ordering(partition_order))) {
    LOG_WARN("failed to set partitions order for granule iterator", K(ret));
  } else if (partition_order) {
    // task order, then keep ordering
    if (OB_FAIL(set_op_ordering(child->get_op_ordering()))) {
      LOG_WARN("failed to set op ordering", K(ret));
    } else if (OB_FAIL(set_partition_order())) {
      LOG_WARN("failed to set op ordering", K(get_op_ordering().count()), K(ret));
    } else {
      LOG_TRACE("success to set op ordering", K(get_op_ordering().count()), K(ret));
    }
  } else if (OB_FAIL(transmit_local_ordering())) {
    LOG_WARN("failed to set op local ordering", K(ret));
  } else {
    LOG_TRACE("success to set op local ordering", K(get_local_ordering().count()), K(ret));
  }
  return ret;
}

int ObLogGranuleIterator::re_est_cost(const ObLogicalOperator* parent, double need_row_count, bool& re_est)
{
  int ret = OB_SUCCESS;
  UNUSED(parent);
  re_est = false;
  ObLogicalOperator* child = NULL;
  if (need_row_count >= card_) {
    /* do nothing */
  } else if (OB_ISNULL(child = get_child(first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child is NULL", K(ret), K(child));
  } else if (OB_FAIL(child->re_est_cost(this, child->get_card() * need_row_count / card_, re_est))) {
    LOG_WARN("re-estimate cost of child failed", K(ret));
  } else {
    card_ = need_row_count;
    cost_ = child->get_cost();
    re_est = true;
  }
  return ret;
}

int ObLogGranuleIterator::compute_op_ordering()
{
  int ret = OB_SUCCESS;
  reset_op_ordering();
  return ret;
}

int ObLogGranuleIterator::is_partition_gi(bool& partition_granule) const
{
  int ret = OB_SUCCESS;
  ObOptimizerContext* optimizer_context = NULL;
  ObSQLSessionInfo* session_info = NULL;
  partition_granule = false;
  int64_t partition_scan_hold = 0;
  int64_t hash_partition_scan_hold = 0;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(optimizer_context = &get_plan()->get_optimizer_context()) ||
      OB_ISNULL(session_info = optimizer_context->get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Get unexpected null", K(get_plan()), K(session_info), K(ret));
  } else if (OB_FAIL(
                 session_info->get_sys_variable(share::SYS_VAR__PX_PARTITION_SCAN_THRESHOLD, partition_scan_hold))) {
    LOG_WARN("failed to get sys variable px partition scan threshold", K(ret));
  } else if (OB_FAIL(
                 session_info->get_sys_variable(share::SYS_VAR__PX_MIN_GRANULES_PER_SLAVE, hash_partition_scan_hold))) {
    LOG_WARN("failed to get sys variable px min granule per slave", K(ret));
  } else {
    partition_granule = ObGranuleUtil::partition_task_mode(gi_attri_flag_) ||
                        ObGranuleUtil::is_partition_granule(
                            partition_count_, parallel_, partition_scan_hold, hash_partition_scan_hold, hash_part_);
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
