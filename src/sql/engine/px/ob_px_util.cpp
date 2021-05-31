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

#define USING_LOG_PREFIX SQL_ENG

#include "lib/net/ob_addr.h"
#include "lib/hash/ob_hashset.h"
#include "lib/container/ob_array.h"
#include "sql/dtl/ob_dtl_channel_group.h"
#include "sql/dtl/ob_dtl.h"
#include "sql/engine/px/ob_px_util.h"
#include "sql/engine/table/ob_table_lookup.h"
#include "sql/engine/table/ob_table_scan_with_index_back.h"
#include "sql/executor/ob_task_spliter.h"
#include "sql/executor/ob_receive.h"
#include "observer/ob_server_struct.h"
#include "sql/engine/px/ob_px_sqc_handler.h"
#include "sql/engine/px/ob_granule_iterator_op.h"
#include "sql/engine/expr/ob_expr.h"
#include "share/schema/ob_part_mgr_util.h"
#include "sql/engine/dml/ob_table_insert_op.h"
#include "sql/engine/table/ob_table_lookup_op.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::sql::dtl;
using namespace oceanbase::share;

#define ENG_OP typename ObEngineOpTraits<NEW_ENG>

// distribution strategy:
// For leaf nodes, dfo distribution is generally directly based on data distribution
// Note: If there are two or more scans in the dfo, only the first one needs
//  to be considered. And, request the rest scan The copy distribution is
//  exactly the same as the first scan, otherwise an error will be reported.
int ObPXServerAddrUtil::alloc_by_data_distribution(ObExecContext& ctx, ObDfo& dfo)
{
  int ret = OB_SUCCESS;
  if (nullptr != dfo.get_root_op_spec()) {
    if (OB_FAIL(alloc_by_data_distribution_inner<true>(ctx, dfo))) {
      LOG_WARN("failed to alloc data distribution", K(ret));
    }
  } else {
    if (OB_FAIL(alloc_by_data_distribution_inner<false>(ctx, dfo))) {
      LOG_WARN("failed to alloc data distribution", K(ret));
    }
  }
  return ret;
}

template <bool NEW_ENG>
int ObPXServerAddrUtil::alloc_by_data_distribution_inner(ObExecContext& ctx, ObDfo& dfo)
{
  int ret = OB_SUCCESS;
  ObSEArray<const ENG_OP::TSC*, 2> scan_ops;
  ObSEArray<const ENG_OP::TableModify*, 1> dml_ops;
  const ENG_OP::TableModify* dml_op = nullptr;
  const ENG_OP::TSC* scan_op = nullptr;
  const ENG_OP::Root* root_op = NULL;
  dfo.get_root(root_op);
  if (OB_ISNULL(root_op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr or sqc is not empty", K(ret), K(dfo));
  } else if (0 != dfo.get_sqcs_count()) {
    /**
     * this dfo has been build. do nothing.
     */
  } else if (OB_FAIL(ObTaskSpliter::find_scan_ops(scan_ops, *root_op))) {
    LOG_WARN("fail find scan ops in dfo", K(dfo), K(ret));
  } else if (OB_FAIL(ObPXServerAddrUtil::find_dml_ops(dml_ops, *root_op))) {
    LOG_WARN("failed find insert op in dfo", K(ret), K(dfo));
  } else if (1 < dml_ops.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the count of insert ops is not right", K(ret), K(dml_ops.count()));
  } else if (0 == scan_ops.count() && 0 == dml_ops.count()) {
    /**
     * some dfo may not contain tsc and dml. for example, select 8 from union all select t1.c1 from t1.
     */
    if (OB_FAIL(alloc_by_local_distribution(ctx, dfo))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("alloc SQC on local failed", K(ret));
    }
  } else {
    const ObPhyTableLocation* table_loc = NULL;
    uint64_t table_location_key = OB_INVALID_INDEX;
    uint64_t ref_table_id = OB_INVALID_ID;
    if (scan_ops.count() > 0) {
      scan_op = scan_ops.at(0);
      table_location_key = scan_op->get_table_location_key();
      ref_table_id = scan_op->get_location_table_id();
    }
    if (dml_ops.count() > 0) {
      dml_op = dml_ops.at(0);
      table_location_key = dml_op->get_table_id();
      ref_table_id = dml_op->get_index_tid();
    }
    if (dml_op && dml_op->is_table_location_uncertain()) {
      bool is_weak = false;
      if (OB_FAIL(ObTaskExecutorCtxUtil::get_full_table_phy_table_location(
              ctx, table_location_key, ref_table_id, is_weak, table_loc))) {
        LOG_WARN("fail to get phy table location", K(ret));
      }
    } else {
      if (OB_FAIL(ObTaskExecutorCtxUtil::get_phy_table_location(ctx, table_location_key, ref_table_id, table_loc))) {
        LOG_WARN("fail to get phy table location", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
      // bypass
    } else if (OB_ISNULL(table_loc)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get phy table location", K(ret));
    } else {
      const ObPartitionReplicaLocationIArray& locations = table_loc->get_partition_location_list();
      if (locations.count() <= 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("the location array is empty", K(locations.count()), K(ret));
      } else if (OB_FAIL(build_dfo_sqc(ctx, locations, dfo))) {
        LOG_WARN("fail fill dfo with sqc infos", K(dfo), K(ret));
      } else if (OB_FAIL(set_dfo_accessed_location<NEW_ENG>(ctx, dfo, scan_ops, dml_op))) {
        LOG_WARN("fail to set all table partition for tsc", K(ret));
      }
      LOG_TRACE("allocate sqc by data distribution", K(dfo), K(locations));
    }
  }
  return ret;
}

int ObPXServerAddrUtil::find_dml_ops(common::ObIArray<const ObTableModify*>& insert_ops, const ObPhyOperator& op)
{
  return find_dml_ops_inner<false>(insert_ops, op);
}
int ObPXServerAddrUtil::find_dml_ops(common::ObIArray<const ObTableModifySpec*>& insert_ops, const ObOpSpec& op)
{
  return find_dml_ops_inner<true>(insert_ops, op);
}
template <bool NEW_ENG>
int ObPXServerAddrUtil::find_dml_ops_inner(
    common::ObIArray<const ENG_OP::TableModify*>& insert_ops, const ENG_OP::Root& op)
{
  int ret = OB_SUCCESS;
  if (IS_DML(op.get_type())) {
    if (OB_FAIL(insert_ops.push_back(static_cast<const ENG_OP::TableModify*>(&op)))) {
      LOG_WARN("fail to push back table insert op", K(ret));
    }
  }
  if (OB_SUCC(ret) && !IS_RECEIVE(op.get_type())) {
    for (int32_t i = 0; OB_SUCC(ret) && i < op.get_child_num(); ++i) {
      const ENG_OP::Root* child_op = op.get_child(i);
      if (OB_ISNULL(child_op)) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(find_dml_ops(insert_ops, *child_op))) {
        LOG_WARN("fail to find child insert ops", K(ret), K(i), "op_id", op.get_id(), "child_id", child_op->get_id());
      }
    }
  }
  return ret;
}

int ObPXServerAddrUtil::build_dfo_sqc(ObExecContext& ctx, const ObPartitionReplicaLocationIArray& locations, ObDfo& dfo)
{
  int ret = OB_SUCCESS;
  ObArray<ObAddr> addrs;
  const ObPhysicalPlanCtx* phy_plan_ctx = GET_PHY_PLAN_CTX(ctx);
  const ObPhysicalPlan* phy_plan = NULL;
  ObArray<int64_t> sqc_max_task_count;
  ObArray<int64_t> sqc_part_count;
  int64_t parallel = 0;
  if (OB_ISNULL(phy_plan_ctx) || OB_ISNULL(phy_plan = phy_plan_ctx->get_phy_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL phy plan ptr", K(ret));
  } else if (OB_FAIL(get_location_addrs(locations, addrs))) {
    LOG_WARN("fail get location addrs", K(ret));
  } else if (OB_FAIL(sqc_part_count.prepare_allocate(addrs.count()))) {
    LOG_WARN("Failed to pre allocate sqc part count");
  } else {
    parallel = dfo.get_assigned_worker_count();
    if (0 >= parallel) {
      parallel = 1;
      LOG_TRACE("parallel not set in query hint. set default to 1");
    }
  }
  if (OB_SUCC(ret) && addrs.count() > 0) {
    common::ObArray<ObPxSqcMeta*> sqcs;
    int64_t total_part_cnt = 0;
    ObArray<int64_t> location_idxs;
    for (int64_t i = 0; OB_SUCC(ret) && i < addrs.count(); ++i) {
      location_idxs.reset();
      ObPxSqcMeta sqc;
      sqc.set_dfo_id(dfo.get_dfo_id());
      sqc.set_sqc_id(i);
      sqc.set_exec_addr(addrs.at(i));
      sqc.set_qc_addr(GCTX.self_addr_);
      sqc.set_execution_id(dfo.get_execution_id());
      sqc.set_px_sequence_id(dfo.get_px_sequence_id());
      sqc.set_qc_id(dfo.get_qc_id());
      sqc.set_interrupt_id(dfo.get_interrupt_id());
      sqc.set_fulltree(dfo.is_fulltree());
      sqc.set_rpc_worker(dfo.is_rpc_worker());
      sqc.set_qc_server_id(dfo.get_qc_server_id());
      sqc.set_parent_dfo_id(dfo.get_parent_dfo_id());
      // Find all the location & ranges information under the current addr,
      // save it in the sqc structure, and send it to addr,
      // Aggregate requests from multiple partitions to reduce the number of RPCs
      for (int64_t loc_idx = 0; OB_SUCC(ret) && loc_idx < locations.count(); ++loc_idx) {
        if (addrs.at(i) == locations.at(loc_idx).get_replica_location().server_) {
          if (OB_FAIL(sqc.get_locations().push_back(locations.at(loc_idx)))) {
            LOG_WARN("fail add location to sqc", K(ret));
          } else if (OB_FAIL(location_idxs.push_back(loc_idx))) {
            LOG_WARN("fail to push back loc idx", K(ret));
          }
        }
      }
      if (OB_SUCC(ret) && dfo.has_expr_values() && phy_plan->is_dist_insert_or_replace_plan() &&
          !phy_plan->is_insert_select()) {
        CK(location_idxs.count() == sqc.get_locations().count());
        const common::ObIArray<int64_t>* row_id_list = NULL;
        int64_t value_begin_idx = 0;
        for (int k = 0; OB_SUCC(ret) && k < sqc.get_locations().count(); ++k) {
          if (OB_ISNULL(row_id_list = ctx.get_part_row_manager().get_row_id_list(location_idxs.at(k)))) {
            // only insert into values (select)
            LOG_INFO("do not find row id list", K(ret));
          } else if (OB_FAIL(sqc.add_partition_id_values(sqc.get_locations().at(k).get_partition_id(),
                         value_begin_idx,
                         location_idxs.at(k),
                         row_id_list->count()))) {
            LOG_WARN("fail to add locatition idx params", K(ret));
          } else {
            value_begin_idx += row_id_list->count();
          }
        }
      }
      total_part_cnt += sqc.get_locations().count();
      sqc_part_count.at(i) = sqc.get_locations().count();
      if (OB_SUCC(ret)) {
        if (OB_FAIL(dfo.add_sqc(sqc))) {
          LOG_WARN("Failed to add sqc", K(ret), K(sqc));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(split_parallel_into_task(parallel, sqc_part_count, sqc_max_task_count))) {
        LOG_WARN("Failed to get sqc max task count", K(ret));
      } else if (OB_FAIL(dfo.get_sqcs(sqcs))) {
        LOG_WARN("Failed to get sqcs", K(ret));
      } else if (sqcs.count() != sqc_max_task_count.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN(
            "Unexpected sqcs count and sqc max task count", K(ret), K(sqcs.count()), K(sqc_max_task_count.count()));
      }
    }
    int64_t total_task_count = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < sqc_max_task_count.count(); ++i) {
      sqcs.at(i)->set_min_task_count(1);
      sqcs.at(i)->set_max_task_count(sqc_max_task_count.at(i));
      total_task_count += sqc_max_task_count.at(i);
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < sqc_max_task_count.count(); ++i) {
      sqcs.at(i)->set_total_task_count(total_task_count);
      sqcs.at(i)->set_total_part_count(total_part_cnt);
    }
  }
  return ret;
}

int ObPXServerAddrUtil::get_location_addrs(const ObPartitionReplicaLocationIArray& locations, ObIArray<ObAddr>& addrs)
{
  int ret = OB_SUCCESS;
  hash::ObHashSet<ObAddr> addr_set;
  if (OB_FAIL(addr_set.create(locations.count()))) {
    LOG_WARN("fail creat addr set", "size", locations.count(), K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < locations.count(); ++i) {
    const ObPartitionReplicaLocation& location = locations.at(i);
    ret = addr_set.exist_refactored(location.get_replica_location().server_);
    if (OB_HASH_EXIST == ret) {
      ret = OB_SUCCESS;
    } else if (OB_HASH_NOT_EXIST == ret) {
      if (OB_FAIL(addrs.push_back(location.get_replica_location().server_))) {
        LOG_WARN("fail push back server", K(ret));
      } else if (OB_FAIL(addr_set.set_refactored(location.get_replica_location().server_))) {
        LOG_WARN("fail set addr to addr_set", K(ret));
      }
    } else {
      LOG_WARN("fail check server exist in addr_set", K(ret));
    }
  }
  (void)addr_set.destroy();
  return ret;
}

int ObPXServerAddrUtil::alloc_by_temp_child_distribution(ObExecContext& ctx, ObDfo& child)
{
  int ret = OB_SUCCESS;
  ObIArray<ObSqlTempTableCtx>& temp_ctx = ctx.get_temp_table_ctx();
  for (int64_t i = 0; OB_SUCC(ret) && i < temp_ctx.count(); i++) {
    if (child.get_temp_table_id() == temp_ctx.at(i).temp_table_id_) {
      for (int64_t j = 0; j < temp_ctx.at(i).temp_table_infos_.count(); j++) {
        ObPxSqcMeta sqc;
        ObTempTableSqcInfo& info = temp_ctx.at(i).temp_table_infos_.at(j);
        sqc.set_exec_addr(info.temp_sqc_addr_);
        sqc.set_qc_addr(GCTX.self_addr_);
        sqc.set_dfo_id(child.get_dfo_id());
        sqc.set_min_task_count(info.min_task_count_);
        sqc.set_max_task_count(info.max_task_count_);
        sqc.set_total_task_count(info.task_count_);
        sqc.set_total_part_count(info.part_count_);
        sqc.set_sqc_id(info.sqc_id_);
        sqc.set_execution_id(child.get_execution_id());
        sqc.set_px_sequence_id(child.get_px_sequence_id());
        sqc.set_qc_id(child.get_qc_id());
        sqc.set_interrupt_id(child.get_interrupt_id());
        sqc.set_fulltree(child.is_fulltree());
        sqc.set_rpc_worker(child.is_rpc_worker());
        sqc.set_qc_server_id(child.get_qc_server_id());
        sqc.set_parent_dfo_id(child.get_parent_dfo_id());
        if (OB_FAIL(sqc.get_interm_results().assign(temp_ctx.at(i).temp_table_infos_.at(j).interm_result_ids_))) {
          LOG_WARN("failed to assign to iterm result ids.", K(ret));
        } else if (OB_FAIL(child.add_sqc(sqc))) {
          LOG_WARN("fail add sqc", K(sqc), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObPXServerAddrUtil::alloc_by_child_distribution(const ObDfo& child, ObDfo& parent)
{
  int ret = OB_SUCCESS;
  ObArray<const ObPxSqcMeta*> sqcs;
  if (OB_FAIL(child.get_sqcs(sqcs))) {
    LOG_WARN("fail get sqcs", K(ret));
  } else {
    for (int64_t i = 0; i < sqcs.count() && OB_SUCC(ret); ++i) {
      const ObPxSqcMeta& child_sqc = *sqcs.at(i);
      ObPxSqcMeta sqc;
      sqc.set_exec_addr(child_sqc.get_exec_addr());
      sqc.set_qc_addr(GCTX.self_addr_);
      sqc.set_dfo_id(parent.get_dfo_id());
      sqc.set_min_task_count(child_sqc.get_min_task_count());
      sqc.set_max_task_count(child_sqc.get_max_task_count());
      sqc.set_total_task_count(child_sqc.get_total_task_count());
      sqc.set_total_part_count(child_sqc.get_total_part_count());
      sqc.set_sqc_id(i);
      sqc.set_execution_id(parent.get_execution_id());
      sqc.set_px_sequence_id(parent.get_px_sequence_id());
      sqc.set_qc_id(parent.get_qc_id());
      sqc.set_interrupt_id(parent.get_interrupt_id());
      sqc.set_fulltree(parent.is_fulltree());
      sqc.set_rpc_worker(parent.is_rpc_worker());
      sqc.set_qc_server_id(parent.get_qc_server_id());
      sqc.set_parent_dfo_id(parent.get_parent_dfo_id());
      if (OB_FAIL(parent.add_sqc(sqc))) {
        LOG_WARN("fail add sqc", K(sqc), K(ret));
      }
    }
    LOG_TRACE("allocate by child distribution", K(sqcs));
  }
  return ret;
}

int ObPXServerAddrUtil::alloc_by_local_distribution(ObExecContext& exec_ctx, ObDfo& dfo)
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* plan_ctx = GET_PHY_PLAN_CTX(exec_ctx);
  if (OB_ISNULL(plan_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL phy plan ctx", K(ret));
  } else {
    ObPxSqcMeta sqc;
    sqc.set_exec_addr(GCTX.self_addr_);
    sqc.set_qc_addr(GCTX.self_addr_);
    sqc.set_dfo_id(dfo.get_dfo_id());
    sqc.set_min_task_count(1);
    sqc.set_max_task_count(dfo.get_assigned_worker_count());
    sqc.set_sqc_id(0);
    sqc.set_execution_id(dfo.get_execution_id());
    sqc.set_px_sequence_id(dfo.get_px_sequence_id());
    sqc.set_qc_id(dfo.get_qc_id());
    sqc.set_interrupt_id(dfo.get_interrupt_id());
    sqc.set_fulltree(dfo.is_fulltree());
    sqc.set_rpc_worker(dfo.is_rpc_worker());
    sqc.set_parent_dfo_id(dfo.get_parent_dfo_id());
    sqc.set_qc_server_id(dfo.get_qc_server_id());
    if (OB_FAIL(dfo.add_sqc(sqc))) {
      LOG_WARN("fail add sqc", K(sqc), K(ret));
    }
  }
  return ret;
}

/**
 * There are two types of hash-local ppwj slave mapping plans. The first one is:
 *                  |
 *               hash join (dfo3)
 *                  |
 * ----------------------------------
 * |                                |
 * |                                |
 * |                                |
 * TSC(dfo1:partition-hash)         TSC(dfo2:hash-local)
 *  When encountering this type of scheduling tree, when we schedule dfo1 and dfo3,
 *  dfo2 has not yet been scheduled. In pkey's plan, dfo2 is the reference table.
 *  In the construction of slave mapping, the parent (that is, dfo3) is constructed
 *  Depends on the end of the reference, so when alloc parent, we alloc dfo2 in advance,
 *  and then the parent follows Sqc of dfo2 to build.
 *
 */
int ObPXServerAddrUtil::alloc_by_reference_child_distribution(ObExecContext& exec_ctx, ObDfo& child, ObDfo& parent)
{
  int ret = OB_SUCCESS;
  ObDfo* reference_child = nullptr;
  if (2 != parent.get_child_count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parent should has two child", K(ret));
  } else if (OB_FAIL(parent.get_child_dfo(0, reference_child))) {
    LOG_WARN("failed to get reference_child", K(ret));
  } else if (reference_child->get_dfo_id() == child.get_dfo_id() && OB_FAIL(parent.get_child_dfo(1, reference_child))) {
    LOG_WARN("failed to get reference_child", K(ret));
  } else if (OB_FAIL(alloc_by_data_distribution(exec_ctx, *reference_child))) {
    LOG_WARN("failed to alloc by data", K(ret));
  } else if (OB_FAIL(alloc_by_child_distribution(*reference_child, parent))) {
    LOG_WARN("failed to alloc by child distribution", K(ret));
  }
  return ret;
}

int ObPXServerAddrUtil::check_partition_wise_location_valid(ObPartitionReplicaLocationIArray& tsc_locations)
{
  int ret = OB_SUCCESS;
  common::ObAddr exec_addr;
  if (tsc_locations.count() <= 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tsc locatition ", K(ret));
  } else {
    exec_addr = tsc_locations.at(0).get_replica_location().server_;
  }
  ARRAY_FOREACH_X(tsc_locations, idx, cnt, OB_SUCC(ret))
  {
    ObPartitionReplicaLocation& partition_rep_location = tsc_locations.at(idx);
    const common::ObAddr& addr = partition_rep_location.get_replica_location().server_;
    if (exec_addr != addr) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("some partition not belong to this server", K(ret), K(exec_addr), K(addr));
    }
  }
  return ret;
}

template <bool NEW_ENG>
int ObPXServerAddrUtil::get_access_partition_order(ObDfo& dfo, const ENG_OP::Root* phy_op, bool& asc_order)
{
  int ret = OB_SUCCESS;
  const ENG_OP::Root* root = NULL;
  dfo.get_root(root);
  if (OB_ISNULL(root)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get root phy op", K(ret));
  } else if (OB_FAIL(get_access_partition_order_recursively<NEW_ENG>(root, phy_op, asc_order))) {
    LOG_WARN("fail to get table scan partition", K(ret));
  }
  return ret;
}

template <bool NEW_ENG>
int ObPXServerAddrUtil::get_access_partition_order_recursively(
    const ENG_OP::Root* root, const ENG_OP::Root* phy_op, bool& asc_order)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(root) || OB_ISNULL(phy_op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the root or phy op is null", K(ret), K(root), K(phy_op));
  } else if (root == phy_op) {
    asc_order = true;
    LOG_DEBUG("No GI in this dfo");
  } else if (PHY_GRANULE_ITERATOR == phy_op->get_type()) {
    const ENG_OP::GI* gi = static_cast<const ENG_OP::GI*>(phy_op);
    asc_order = !gi->desc_partition_order();
  } else if (OB_FAIL(get_access_partition_order_recursively<NEW_ENG>(root, phy_op->get_parent(), asc_order))) {
    LOG_WARN("fail to access partition order", K(ret));
  }
  return ret;
}

template <bool NEW_ENG>
int ObPXServerAddrUtil::set_dfo_accessed_location(
    ObExecContext& ctx, ObDfo& dfo, ObIArray<const ENG_OP::TSC*>& scan_ops, const ENG_OP::TableModify* dml_op)
{
  int ret = OB_SUCCESS;

  ObSEArray<int64_t, 2> base_order;
  if (OB_FAIL(ret) || OB_ISNULL(dml_op)) {
    // pass
  } else {
    const ObPhyTableLocation* table_loc = nullptr;
    uint64_t table_location_key = common::OB_INVALID_ID;
    uint64_t ref_table_id = common::OB_INVALID_ID;
    if (FALSE_IT(table_location_key = dml_op->get_table_id())) {
    } else if (FALSE_IT(ref_table_id = dml_op->get_index_tid())) {
    } else {
      if (dml_op->is_table_location_uncertain()) {
        bool is_weak = false;
        if (OB_FAIL(ObTaskExecutorCtxUtil::get_full_table_phy_table_location(
                ctx, table_location_key, ref_table_id, is_weak, table_loc))) {
          LOG_WARN("fail to get phy table location", K(ret));
        }
      } else {
        if (OB_FAIL(ObTaskExecutorCtxUtil::get_phy_table_location(ctx, table_location_key, ref_table_id, table_loc))) {
          LOG_WARN("fail to get phy table location", K(ret));
        }
      }
    }
    if (OB_FAIL(ret)) {
      // bypass
    } else if (OB_ISNULL(table_loc)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table loc is null", K(ret));
    } else if (OB_FAIL(set_sqcs_accessed_location<NEW_ENG>(ctx, dfo, base_order, table_loc, dml_op))) {
      LOG_WARN("failed to set sqc accessed location", K(ret));
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < scan_ops.count(); ++i) {
    const ObPhyTableLocation* table_loc = nullptr;
    const ENG_OP::TSC* scan_op = nullptr;
    uint64_t table_location_key = common::OB_INVALID_ID;
    uint64_t ref_table_id = common::OB_INVALID_ID;
    if (OB_ISNULL(scan_op = scan_ops.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("scan op can't be null", K(ret));
    } else if (FALSE_IT(table_location_key = scan_op->get_table_location_key())) {
    } else if (FALSE_IT(ref_table_id = scan_op->get_location_table_id())) {
    } else if (OB_FAIL(
                   ObTaskExecutorCtxUtil::get_phy_table_location(ctx, table_location_key, ref_table_id, table_loc))) {
      LOG_WARN("fail to get phy table location", K(ret));
    } else if (OB_ISNULL(table_loc)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get phy table location", K(ret));
    } else if (OB_FAIL(set_sqcs_accessed_location<NEW_ENG>(ctx, dfo, base_order, table_loc, scan_op))) {
      LOG_WARN("failed to set sqc accessed location", K(ret));
    }
  }  // end for
  return ret;
}

template <bool NEW_ENG>
int ObPXServerAddrUtil::set_sqcs_accessed_location(ObExecContext& ctx, ObDfo& dfo, ObIArray<int64_t>& base_order,
    const ObPhyTableLocation* table_loc, const ENG_OP::Root* phy_op)
{
  int ret = OB_SUCCESS;
  common::ObArray<ObPxSqcMeta*> sqcs;
  int n_locations = 0;
  const ObPartitionReplicaLocationIArray& locations = table_loc->get_partition_location_list();
  ObPartitionReplicaLocationSEArray temp_locations;
  if (OB_ISNULL(table_loc) || OB_ISNULL(phy_op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null table_loc or phy_op", K(phy_op), K(table_loc));
  } else if (OB_FAIL(dfo.get_sqcs(sqcs))) {
    LOG_WARN("fail to get sqcs", K(ret));
  } else {
    int64_t table_location_key = table_loc->get_table_location_key();
    bool asc_order = true;
    if (locations.count() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the locations can not be zero", K(ret), K(locations.count()));
    } else if (OB_FAIL(get_access_partition_order<NEW_ENG>(dfo, phy_op, asc_order))) {
      LOG_WARN("fail to get table scan partition order", K(ret));
    } else if (OB_FAIL(ObPXServerAddrUtil::reorder_all_partitions(
                   table_location_key, locations, temp_locations, asc_order, ctx, base_order))) {
      LOG_WARN("fail to reorder all partitions", K(ret));
    } else {
      LOG_TRACE("sqc partition order is", K(asc_order));
    }
  }

  ARRAY_FOREACH_X(sqcs, sqc_idx, sqc_cnt, OB_SUCC(ret))
  {
    ObPxSqcMeta* sqc_meta = sqcs.at(sqc_idx);
    ObPartitionReplicaLocationIArray& sqc_locations = sqc_meta->get_access_table_locations();
    if (OB_ISNULL(sqc_meta)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the sqc is null", K(ret));
    } else {
      const common::ObAddr& sqc_server = sqc_meta->get_exec_addr();
      ARRAY_FOREACH_X(temp_locations, idx, cnt, OB_SUCC(ret))
      {
        const common::ObAddr& server = temp_locations.at(idx).get_replica_location().server_;
        if (server == sqc_server) {
          if (OB_FAIL(sqc_locations.push_back(temp_locations.at(idx)))) {
            LOG_WARN("sqc push back table location failed", K(ret));
          } else {
            ++n_locations;
          }
        }
      }
    }
  }
  if (n_locations != locations.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("we do not find this addr's execution sqc", K(ret), K(n_locations), K(locations.count()));
  }
  return ret;
}

int ObPXServerAddrUtil::reorder_all_partitions(int64_t table_location_key,
    const ObPartitionReplicaLocationIArray& src_locations, ObPartitionReplicaLocationIArray& dst_locations, bool asc,
    ObExecContext& exec_ctx, ObIArray<int64_t>& base_order)
{
  int ret = OB_SUCCESS;
  dst_locations.reset();
  if (src_locations.count() > 1) {
    for (int i = 0; i < src_locations.count() && OB_SUCC(ret); ++i) {
      if (OB_FAIL(dst_locations.push_back(src_locations.at(i)))) {
        LOG_WARN("fail to push dst locations", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      std::sort(&dst_locations.at(0),
          &dst_locations.at(0) + dst_locations.count(),
          asc ? ObPartitionReplicaLocation::compare_part_loc_asc : ObPartitionReplicaLocation::compare_part_loc_desc);
      PWJPartitionIdMap* pwj_map = NULL;
      if (OB_NOT_NULL(pwj_map = exec_ctx.get_pwj_map())) {
        PartitionIdArray partition_id_array;
        if (OB_FAIL(pwj_map->get_refactored(table_location_key, partition_id_array))) {
          if (OB_HASH_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
          }
        } else if (0 == base_order.count()) {
          for (int i = 0; i < dst_locations.count() && OB_SUCC(ret); ++i) {
            for (int j = 0; j < partition_id_array.count() && OB_SUCC(ret); ++j) {
              if (dst_locations.at(i).get_partition_id() == partition_id_array.at(j)) {
                if (OB_FAIL(base_order.push_back(j))) {
                  LOG_WARN("fail to push idx into base order", K(ret));
                }
                break;
              }
            }
          }
        } else {
          int index = 0;
          for (int i = 0; i < base_order.count() && OB_SUCC(ret); ++i) {
            for (int j = 0; j < dst_locations.count() && OB_SUCC(ret); ++j) {
              if (dst_locations.at(j).get_partition_id() == partition_id_array.at(base_order.at(i))) {
                std::swap(dst_locations.at(j), dst_locations.at(index++));
                break;
              }
            }
          }
        }
      }
    }
  } else if (1 == src_locations.count() && OB_FAIL(dst_locations.push_back(src_locations.at(0)))) {
    LOG_WARN("fail to push dst locations", K(ret));
  }
  return ret;
}

/**
 * n is the total number of threads, p is the total number of partitions involved, ni
 * is the number of threads for which the i-th sqc is calculated, and pi is the number
 * of partitions of the i-th sqc.
 * a. An adjust function that recursively adjusts the number of threads in sqc.
 *    Obtain the value of ni = n*pi/p, and ensure that each is greater than or equal to 1.
 * b. Calculate the execution time of sqc and sort according to it.
 * c. The remaining threads are added to sqc one by one from a long execution time to a short time.
 */
int ObPXServerAddrUtil::split_parallel_into_task(
    const int64_t parallel, const common::ObIArray<int64_t>& sqc_part_count, common::ObIArray<int64_t>& results)
{
  int ret = OB_SUCCESS;
  common::ObArray<ObPxSqcTaskCountMeta> sqc_task_metas;
  int64_t total_part_count = 0;
  int64_t total_thread_count = 0;
  int64_t thread_remain = 0;
  results.reset();
  if (parallel <= 0 || sqc_part_count.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid input argument", K(ret), K(parallel), K(sqc_part_count.count()));
  } else if (OB_FAIL(results.prepare_allocate(sqc_part_count.count()))) {
    LOG_WARN("Failed to prepare allocate array", K(ret));
  }
  // prepare
  ARRAY_FOREACH(sqc_part_count, idx)
  {
    ObPxSqcTaskCountMeta meta;
    meta.idx_ = idx;
    meta.partition_count_ = sqc_part_count.at(idx);
    meta.thread_count_ = 0;
    meta.time_ = 0;
    if (OB_FAIL(sqc_task_metas.push_back(meta))) {
      LOG_WARN("Failed to push back sqc partition count", K(ret));
    } else if (sqc_part_count.at(idx) < 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Invalid partition count", K(ret));
    } else {
      total_part_count += sqc_part_count.at(idx);
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(adjust_sqc_task_count(sqc_task_metas, parallel, total_part_count))) {
      LOG_WARN("Failed to adjust sqc task count", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; i < sqc_task_metas.count(); ++i) {
      ObPxSqcTaskCountMeta& meta = sqc_task_metas.at(i);
      total_thread_count += meta.thread_count_;
      meta.time_ = static_cast<double>(meta.partition_count_) / static_cast<double>(meta.thread_count_);
    }
    auto compare_fun_long_time_first = [](ObPxSqcTaskCountMeta a, ObPxSqcTaskCountMeta b) -> bool {
      return a.time_ > b.time_;
    };
    std::sort(sqc_task_metas.begin(), sqc_task_metas.end(), compare_fun_long_time_first);
    thread_remain = parallel - total_thread_count;
    if (thread_remain <= 0) {
    } else if (thread_remain > sqc_task_metas.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Thread remain is invalid", K(ret), K(thread_remain), K(sqc_task_metas.count()));
    } else {
      for (int64_t i = 0; i < thread_remain; ++i) {
        ObPxSqcTaskCountMeta& meta = sqc_task_metas.at(i);
        meta.thread_count_ += 1;
        total_thread_count += 1;
        meta.time_ = static_cast<double>(meta.partition_count_) / static_cast<double>(meta.thread_count_);
      }
    }
  }
  if (OB_SUCC(ret)) {
    int64_t idx = 0;
    for (int64_t i = 0; i < sqc_task_metas.count(); ++i) {
      ObPxSqcTaskCountMeta meta = sqc_task_metas.at(i);
      idx = meta.idx_;
      results.at(idx) = meta.thread_count_;
    }
  }
  if (OB_SUCC(ret) && parallel > sqc_task_metas.count() && total_thread_count > parallel) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Failed to allocate expected parallel", K(ret));
  }
  LOG_TRACE("Sqc max task count", K(ret), K(results), K(sqc_task_metas));
  return ret;
}

int ObPXServerAddrUtil::adjust_sqc_task_count(
    common::ObIArray<ObPxSqcTaskCountMeta>& sqc_tasks, int64_t parallel, int64_t partition)
{
  int ret = OB_SUCCESS;
  int64_t thread_used = 0;
  int64_t partition_remain = partition;
  int64_t real_partition = NON_ZERO_VALUE(partition);
  ARRAY_FOREACH(sqc_tasks, idx)
  {
    ObPxSqcTaskCountMeta& meta = sqc_tasks.at(idx);
    if (!meta.finish_) {
      meta.thread_count_ = meta.partition_count_ * parallel / real_partition;
      if (0 >= meta.thread_count_) {
        thread_used++;
        partition_remain -= meta.partition_count_;
        meta.finish_ = true;
        meta.thread_count_ = 1;
      }
    }
  }
  if (thread_used != 0) {
    if (OB_FAIL(adjust_sqc_task_count(sqc_tasks, parallel - thread_used, partition_remain))) {
      LOG_WARN("Failed to adjust sqc task count", K(ret), K(sqc_tasks));
    }
  }
  return ret;
}

int ObPxPartitionLocationUtil::get_all_tables_partitions(const int64_t& tsc_dml_op_count,
    ObPartitionReplicaLocationIArray& all_locations, int64_t& tsc_location_idx,
    common::ObIArray<common::ObPartitionArray>& partitions, int64_t part_count)
{
  int ret = OB_SUCCESS;
  ObPartitionArray partition_keys;
  ObPartitionKey pkey;
  int step_num = 0;
  int total_count = 0;
  for (int i = tsc_location_idx; i < all_locations.count() && OB_SUCC(ret); ++i) {
    if (OB_FAIL(all_locations.at(i).get_partition_key(pkey))) {
      LOG_WARN("fail to get pkey", K(i), K(ret));
    } else if (OB_FAIL(add_var_to_array_no_dup(partition_keys, pkey))) {
      LOG_WARN("fail to push back pkey", K(pkey), K(ret));
    } else {
      step_num++;
      if (step_num == part_count) {
        total_count++;
        if (partition_keys.count() > 0 && OB_FAIL(partitions.push_back(partition_keys))) {
          LOG_WARN("push back tsc partitions failed", K(partition_keys), K(ret));
        } else if (total_count == tsc_dml_op_count) {
          tsc_location_idx = i + 1;
          break;
        } else {
          step_num = 0;
          partition_keys.reset();
        }
      }
    }
  }
  LOG_TRACE("add partition in table by tscs", K(ret), K(tsc_dml_op_count), K(all_locations), K(partitions));
  return ret;
}

int ObPxOperatorVisitor::visit(ObExecContext& ctx, ObPhyOperator& root, ApplyFunc& func)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(func.apply(ctx, root))) {
    LOG_WARN("fail apply func to input", "op_id", root.get_id(), K(ret));
  } else if (!IS_PX_RECEIVE(root.get_type())) {
    for (int32_t i = 0; OB_SUCC(ret) && i < root.get_child_num(); ++i) {
      ObPhyOperator* child_op = root.get_child(i);
      if (OB_ISNULL(child_op)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null child operator", K(i), K(root.get_type()));
      } else if (OB_FAIL(visit(ctx, *child_op, func))) {
        LOG_WARN("fail to apply func", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(func.reset(root))) {
    LOG_WARN("Failed to reset", K(ret));
  }
  return ret;
}

int ObPxOperatorVisitor::visit(ObExecContext& ctx, ObOpSpec& root, ApplyFunc& func)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(func.apply(ctx, root))) {
    LOG_WARN("fail apply func to input", "op_id", root.id_, K(ret));
  } else if (!IS_PX_RECEIVE(root.type_)) {
    for (int32_t i = 0; OB_SUCC(ret) && i < root.get_child_cnt(); ++i) {
      ObOpSpec* child_op = root.get_child(i);
      if (OB_ISNULL(child_op)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null child operator", K(i), K(root.type_));
      } else if (OB_FAIL(visit(ctx, *child_op, func))) {
        LOG_WARN("fail to apply func", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(func.reset(root))) {
    LOG_WARN("Failed to reset", K(ret));
  }
  return ret;
}

int ObPxTreeSerializer::serialize_tree(
    char* buf, int64_t buf_len, int64_t& pos, ObPhyOperator& root, bool is_fulltree, ObPhyOpSeriCtx* seri_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(serialization::encode_vi32(buf, buf_len, pos, root.get_type()))) {
    LOG_WARN("fail to encode op type", K(ret));
  } else if (OB_FAIL((seri_ctx == NULL ? root.serialize(buf, buf_len, pos)
                                       : root.serialize(buf, buf_len, pos, *seri_ctx)))) {
    LOG_WARN("fail to serialize root", K(ret), "type", root.get_type(), "root", to_cstring(root));
  } else if ((PHY_TABLE_SCAN_WITH_DOMAIN_INDEX == root.get_type() || PHY_TABLE_LOOKUP == root.get_type()) &&
             OB_FAIL(serialize_sub_plan(buf, buf_len, pos, root))) {
    LOG_WARN("fail to serialize sub plan", K(ret));
  }
  // Terminate serialization when meet ObReceive, as this op indicates
  bool serialize_child = is_fulltree || (!IS_RECEIVE(root.get_type()));
  if (OB_SUCC(ret) && serialize_child) {
    for (int32_t i = 0; OB_SUCC(ret) && i < root.get_child_num(); ++i) {
      ObPhyOperator* child_op = root.get_child(i);
      if (OB_ISNULL(child_op)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null child operator", K(i), K(root.get_type()));
      } else if (OB_FAIL(serialize_tree(buf, buf_len, pos, *child_op, is_fulltree, seri_ctx))) {
        LOG_WARN("fail to serialize tree", K(ret));
      }
    }
  }
  return ret;
}

int ObPxTreeSerializer::deserialize_tree(const char* buf, int64_t data_len, int64_t& pos, ObPhysicalPlan& phy_plan,
    ObPhyOperator*& root, bool is_fulltree, ObIArray<const ObTableScan*>& tsc_ops)
{
  int ret = OB_SUCCESS;
  int32_t phy_operator_type = 0;

  ObPhyOperator* op = NULL;
  if (OB_FAIL(serialization::decode_vi32(buf, data_len, pos, &phy_operator_type))) {
    LOG_WARN("fail to decode phy operator type", K(ret));
  } else {
    LOG_DEBUG("deserialize phy_operator",
        K(phy_operator_type),
        "type_str",
        ob_phy_operator_type_str(static_cast<ObPhyOperatorType>(phy_operator_type)),
        K(pos));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(phy_plan.alloc_operator_by_type(static_cast<ObPhyOperatorType>(phy_operator_type), op))) {
      LOG_WARN("alloc physical operator failed", K(ret));
    } else {
      if (OB_FAIL(op->deserialize(buf, data_len, pos))) {
        LOG_WARN("fail to deserialize operator", K(ret), N_TYPE, phy_operator_type);
      } else if ((PHY_TABLE_SCAN_WITH_DOMAIN_INDEX == op->get_type() || PHY_TABLE_LOOKUP == op->get_type()) &&
                 OB_FAIL(deserialize_sub_plan(buf, data_len, pos, phy_plan, op))) {
        LOG_WARN("fail to deserialize sub plan", K(ret));
      } else {
        LOG_DEBUG("deserialize phy operator",
            K(op->get_type()),
            "serialize_size",
            op->get_serialize_size(),
            "data_len",
            data_len,
            "pos",
            pos,
            "takes",
            data_len - pos);
      }
    }
  }

  if (OB_SUCC(ret)) {
    if ((op->get_type() <= PHY_INVALID || op->get_type() >= PHY_END)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid operator type", N_TYPE, op->get_type());
    } else if (PHY_TABLE_SCAN == op->get_type() && OB_FAIL(tsc_ops.push_back(static_cast<ObTableScan*>(op)))) {
      LOG_WARN("Failed to push back table scan operator", K(ret));
    }
  }

  // Terminate serialization when meet ObReceive, as this op indicates
  if (OB_SUCC(ret)) {
    bool serialize_child = is_fulltree || (!IS_RECEIVE(op->get_type()));
    if (serialize_child) {
      if (OB_FAIL(op->create_child_array(op->get_child_num()))) {
        LOG_WARN("create child array failed", K(ret), K(op->get_child_num()));
      }
      for (int32_t i = 0; OB_SUCC(ret) && i < op->get_child_num(); i++) {
        ObPhyOperator* child = NULL;
        if (OB_FAIL(deserialize_tree(buf, data_len, pos, phy_plan, child, is_fulltree, tsc_ops))) {
          LOG_WARN("fail to deserialize tree", K(ret));
        } else if (OB_FAIL(op->set_child(i, *child))) {
          LOG_WARN("fail to set child", K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    root = op;
  }
  return ret;
}

int ObPxTreeSerializer::deserialize_sub_plan(
    const char* buf, int64_t data_len, int64_t& pos, ObPhysicalPlan& phy_plan, ObPhyOperator*& op)
{
  int ret = OB_SUCCESS;
  if (PHY_TABLE_SCAN_WITH_DOMAIN_INDEX == op->get_type()) {
    ObPhyOperator* index_scan_tree = nullptr;
    ObTableScanWithIndexBack* table_scan = static_cast<ObTableScanWithIndexBack*>(op);
    if (OB_FAIL(deserialize_op(buf, data_len, pos, phy_plan, index_scan_tree))) {
      LOG_WARN("deserialize tree failed", K(ret), K(data_len), K(pos));
    } else {
      table_scan->set_index_scan_tree(index_scan_tree);
    }
  }

  if (OB_SUCC(ret) && PHY_TABLE_LOOKUP == op->get_type()) {
    ObPhyOperator* multi_partition_scan_tree = nullptr;
    ObTableLookup* table_scan = static_cast<ObTableLookup*>(op);
    if (OB_FAIL(deserialize_op(buf, data_len, pos, phy_plan, multi_partition_scan_tree))) {
      LOG_WARN("deserialize tree failed", K(ret), K(data_len), K(pos));
    } else {
      table_scan->set_remote_plan(multi_partition_scan_tree);
    }
  }
  return ret;
}

int ObPxTreeSerializer::deserialize_op(
    const char* buf, int64_t data_len, int64_t& pos, ObPhysicalPlan& phy_plan, ObPhyOperator*& root)
{
  int ret = OB_SUCCESS;
  int32_t phy_operator_type = 0;

  ObPhyOperator* op = NULL;
  if (OB_FAIL(serialization::decode_vi32(buf, data_len, pos, &phy_operator_type))) {
    LOG_WARN("fail to decode phy operator type", K(ret));
  } else {
    LOG_DEBUG("deserialize phy_operator",
        K(phy_operator_type),
        "type_str",
        ob_phy_operator_type_str(static_cast<ObPhyOperatorType>(phy_operator_type)),
        K(pos));
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(phy_plan.alloc_operator_by_type(static_cast<ObPhyOperatorType>(phy_operator_type), op))) {
      LOG_WARN("alloc physical operator failed", K(ret));
    } else if (OB_FAIL(op->deserialize(buf, data_len, pos))) {
      LOG_WARN("fail to deserialize operator", K(ret), N_TYPE, phy_operator_type);
    } else {
      LOG_DEBUG("deserialize phy operator",
          K(op->get_type()),
          "serialize_size",
          op->get_serialize_size(),
          "data_len",
          data_len,
          "pos",
          pos,
          "takes",
          data_len - pos);
      root = op;
    }
  }
  return ret;
}

int ObPxTreeSerializer::deserialize_tree(
    const char* buf, int64_t data_len, int64_t& pos, ObPhysicalPlan& phy_plan, ObPhyOperator*& root, bool is_fulltree)
{
  int ret = OB_SUCCESS;
  ObSEArray<const ObTableScan*, 8> tsc_ops;
  ret = deserialize_tree(buf, data_len, pos, phy_plan, root, is_fulltree, tsc_ops);
  return ret;
}

int64_t ObPxTreeSerializer::get_tree_serialize_size(ObPhyOperator& root, bool is_fulltree, ObPhyOpSeriCtx* seri_ctx)
{
  int ret = OB_SUCCESS;
  int64_t len = 0;

  len += serialization::encoded_length_vi32(root.get_type());
  len += (NULL == seri_ctx ? root.get_serialize_size() : root.get_serialize_size(*seri_ctx));
  len += get_sub_plan_serialize_size(root);

  // Terminate serialization when meet ObReceive, as this op indicates
  bool serialize_child = is_fulltree || (!IS_RECEIVE(root.get_type()));
  if (serialize_child) {
    for (int32_t i = 0; OB_SUCC(ret) && i < root.get_child_num(); ++i) {
      ObPhyOperator* child_op = root.get_child(i);
      if (OB_ISNULL(child_op)) {
        LOG_ERROR("null child op", K(i), K(root.get_child_num()), K(root.get_type()));
      } else {
        len += get_tree_serialize_size(*child_op, is_fulltree, seri_ctx);
      }
    }
  }

  return len;
}

int ObPxTreeSerializer::serialize_sub_plan(char* buf, int64_t buf_len, int64_t& pos, ObPhyOperator& root)
{
  int ret = OB_SUCCESS;
  if (PHY_TABLE_SCAN_WITH_DOMAIN_INDEX == root.get_type()) {
    ObPhyOperator* index_scan_tree =
        const_cast<ObPhyOperator*>(static_cast<ObTableScanWithIndexBack&>(root).get_index_scan_tree());
    if (OB_ISNULL(index_scan_tree)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("index scan tree is null");
    } else if (OB_FAIL(serialize_tree(buf, buf_len, pos, *index_scan_tree, false))) {
      LOG_WARN("serialize index scan tree failed", K(ret));
    }
  } else if (PHY_TABLE_LOOKUP == root.get_type()) {
    ObPhyOperator* multi_partition_scan_tree = static_cast<const ObTableLookup&>(root).get_remote_plan();
    if (OB_ISNULL(multi_partition_scan_tree)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("index scan tree is null", K(ret));
    } else if (OB_FAIL(serialize_tree(buf, buf_len, pos, *multi_partition_scan_tree, false))) {
      LOG_WARN("serialize tree failed", K(ret), K(buf_len), K(pos));
    }
  }
  return ret;
}

int64_t ObPxTreeSerializer::get_sub_plan_serialize_size(ObPhyOperator& root)
{
  int64_t len = 0;
  // some op contain inner mini plan
  if (PHY_TABLE_SCAN_WITH_DOMAIN_INDEX == root.get_type()) {
    ObPhyOperator* index_scan_tree =
        const_cast<ObPhyOperator*>(static_cast<ObTableScanWithIndexBack&>(root).get_index_scan_tree());
    if (index_scan_tree != NULL) {
      len += get_tree_serialize_size(*index_scan_tree, false);
    }
  }
  if (PHY_TABLE_LOOKUP == root.get_type()) {
    ObPhyOperator* multi_partition_scan_tree = static_cast<ObTableLookup&>(root).get_remote_plan();
    if (multi_partition_scan_tree != NULL) {
      len += get_tree_serialize_size(*multi_partition_scan_tree, false);
    }
  }
  return len;
}

// serialize plan tree for engine3.0
int ObPxTreeSerializer::serialize_frame_info(char* buf, int64_t buf_len, int64_t& pos,
    ObIArray<ObFrameInfo>& all_frames, char** frames, const int64_t frame_cnt, bool no_ser_data /* = false*/)
{
  int ret = OB_SUCCESS;
  int64_t need_extra_mem_size = 0;
  const int64_t datum_eval_info_size = sizeof(ObDatum) + sizeof(ObEvalInfo);
  OB_UNIS_ENCODE(all_frames.count());
  // OB_UNIS_ENCODE(all_frames);
  for (int64_t i = 0; i < all_frames.count() && OB_SUCC(ret); ++i) {
    OB_UNIS_ENCODE(all_frames.at(i));
  }
  for (int64_t i = 0; i < all_frames.count() && OB_SUCC(ret); ++i) {
    ObFrameInfo& frame_info = all_frames.at(i);
    if (frame_info.frame_idx_ >= frame_cnt) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("frame index exceed frame count", K(ret), K(frame_cnt), K(frame_info.frame_idx_));
    } else {
      char* frame_buf = frames[frame_info.frame_idx_];
      int64_t expr_mem_size = no_ser_data ? 0 : frame_info.expr_cnt_ * datum_eval_info_size;
      OB_UNIS_ENCODE(expr_mem_size);
      if (pos + expr_mem_size > buf_len) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("ser frame info size overflow", K(ret), K(pos), K(expr_mem_size), K(buf_len));
      } else if (!no_ser_data && 0 < expr_mem_size) {
        MEMCPY(buf + pos, frame_buf, expr_mem_size);
        pos += expr_mem_size;
      }
      for (int64_t j = 0; j < frame_info.expr_cnt_ && OB_SUCC(ret); ++j) {
        ObDatum* expr_datum = reinterpret_cast<ObDatum*>(frame_buf + j * datum_eval_info_size);
        need_extra_mem_size += no_ser_data ? 0 : (expr_datum->null_ ? 0 : expr_datum->len_);
      }
    }
  }
  OB_UNIS_ENCODE(need_extra_mem_size);
  int64_t expr_datum_size = 0;
  int64_t ser_mem_size = 0;
  for (int64_t i = 0; i < all_frames.count() && OB_SUCC(ret); ++i) {
    ObFrameInfo& frame_info = all_frames.at(i);
    char* frame_buf = frames[frame_info.frame_idx_];
    for (int64_t j = 0; j < frame_info.expr_cnt_ && OB_SUCC(ret); ++j) {
      ObDatum* expr_datum = reinterpret_cast<ObDatum*>(frame_buf + j * datum_eval_info_size);
      expr_datum_size = no_ser_data ? 0 : (expr_datum->null_ ? 0 : expr_datum->len_);
      OB_UNIS_ENCODE(expr_datum_size);
      if (pos + expr_datum_size > buf_len) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("ser frame info size overflow", K(ret), K(pos), K(expr_datum_size), K(buf_len));
      } else if (0 < expr_datum_size) {
        MEMCPY(buf + pos, expr_datum->ptr_, expr_datum_size);
        pos += expr_datum_size;
        ser_mem_size += expr_datum_size;
      }
    }
  }
  if (OB_SUCC(ret) && ser_mem_size != need_extra_mem_size) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: serialize size is not match", K(ret), K(ser_mem_size), K(need_extra_mem_size));
  }
  return ret;
}

int ObPxTreeSerializer::serialize_expr_frame_info(
    char* buf, int64_t buf_len, int64_t& pos, ObExecContext& ctx, ObExprFrameInfo& expr_frame_info)
{
  int ret = OB_SUCCESS;
  LOG_TRACE("trace start ser expr frame info", K(ret), K(buf_len), K(pos));
  ObIArray<ObExpr>& exprs = expr_frame_info.rt_exprs_;
  OB_UNIS_ENCODE(expr_frame_info.need_ctx_cnt_);
  // rt exprs
  ObExpr::get_serialize_array() = &exprs;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(serialization::encode_i32(buf, buf_len, pos, exprs.count()))) {
    LOG_WARN("fail to encode op type", K(ret));
  } else if (nullptr == ObExpr::get_serialize_array()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("serialize array is null", K(ret), K(pos), K(exprs.count()));
  } else {
    LOG_TRACE("trace get serialize array", K(ObExpr::get_serialize_array()), K(exprs.count()));
    for (int64_t i = 0; i < exprs.count() && OB_SUCC(ret); ++i) {
      ObExpr& expr = exprs.at(i);
      if (OB_FAIL(expr.serialize(buf, buf_len, pos))) {
        LOG_WARN("failed to serialize expr", K(ret), K(i), K(exprs.count()));
      }
    }
  }
  // frames
  OB_UNIS_ENCODE(ctx.get_frame_cnt());
  OZ(serialize_frame_info(buf, buf_len, pos, expr_frame_info.const_frame_, ctx.get_frames(), ctx.get_frame_cnt()));
  OZ(serialize_frame_info(buf, buf_len, pos, expr_frame_info.param_frame_, ctx.get_frames(), ctx.get_frame_cnt()));
  OZ(serialize_frame_info(buf, buf_len, pos, expr_frame_info.dynamic_frame_, ctx.get_frames(), ctx.get_frame_cnt()));
  OZ(serialize_frame_info(
      buf, buf_len, pos, expr_frame_info.datum_frame_, ctx.get_frames(), ctx.get_frame_cnt(), true));
  LOG_DEBUG("trace end ser expr frame info", K(ret), K(buf_len), K(pos));
  return ret;
}

int ObPxTreeSerializer::deserialize_frame_info(const char* buf, int64_t data_len, int64_t& pos, ObIAllocator& allocator,
    ObIArray<ObFrameInfo>& all_frames, ObIArray<char*>* char_ptrs, char**& frames, int64_t& frame_cnt,
    bool no_deser_data)
{
  int ret = OB_SUCCESS;
  int64_t frame_info_cnt = 0;
  const int64_t datum_eval_info_size = sizeof(ObDatum) + sizeof(ObEvalInfo);
  OB_UNIS_DECODE(frame_info_cnt);
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(all_frames.reserve(frame_info_cnt))) {
    LOG_WARN("failed to reserve const frame", K(ret));
  } else if (nullptr != char_ptrs && OB_FAIL(char_ptrs->reserve(frame_info_cnt))) {
    LOG_WARN("failed to reserve const frame", K(ret));
  } else {
    // OB_UNIS_DECODE(all_frames);
    ObFrameInfo frame_info;
    for (int64_t i = 0; i < frame_info_cnt && OB_SUCC(ret); ++i) {
      OB_UNIS_DECODE(frame_info);
      if (OB_FAIL(all_frames.push_back(frame_info))) {
        LOG_WARN("failed to push back frame", K(ret), K(i));
      }
    }
  }
  int64_t need_extra_mem_size = 0;
  for (int64_t i = 0; i < all_frames.count() && OB_SUCC(ret); ++i) {
    ObFrameInfo& frame_info = all_frames.at(i);
    int64_t expr_mem_size = 0;
    OB_UNIS_DECODE(expr_mem_size);
    if (frame_info.frame_idx_ >= frame_cnt) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("frame index exceed frame count", K(ret), K(frame_cnt), K(frame_info.frame_idx_));
    } else if (0 < frame_info.expr_cnt_) {
      char* frame_buf = nullptr;
      if (nullptr == (frame_buf = static_cast<char*>(allocator.alloc(frame_info.frame_size_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate frame buf", K(ret));
      } else if (pos + expr_mem_size > data_len) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("ser frame info size overflow", K(ret), K(pos), K(frame_info.frame_size_), K(data_len));
      } else {
        MEMSET(frame_buf, 0, frame_info.frame_size_);
        frames[frame_info.frame_idx_] = frame_buf;
        if (nullptr != char_ptrs && OB_FAIL(char_ptrs->push_back(frame_buf))) {
          LOG_WARN("failed to push back frame buf", K(ret));
        }
        if (!no_deser_data && 0 < expr_mem_size) {
          MEMCPY(frame_buf, buf + pos, expr_mem_size);
          pos += expr_mem_size;
        }
      }
    }
  }
  OB_UNIS_DECODE(need_extra_mem_size);
  int64_t expr_datum_size = 0;
  int64_t des_mem_size = 0;
  char* expr_datum_buf = nullptr;
  if (0 < need_extra_mem_size &&
      nullptr == (expr_datum_buf = static_cast<char*>(allocator.alloc(need_extra_mem_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret));
  }
  for (int64_t i = 0; i < all_frames.count() && OB_SUCC(ret); ++i) {
    ObFrameInfo& frame_info = all_frames.at(i);
    char* frame_buf = frames[frame_info.frame_idx_];
    for (int64_t j = 0; j < frame_info.expr_cnt_ && OB_SUCC(ret); ++j) {
      ObDatum* expr_datum = reinterpret_cast<ObDatum*>(frame_buf + j * datum_eval_info_size);
      OB_UNIS_DECODE(expr_datum_size);
      if (pos + expr_datum_size > data_len) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("ser frame info size overflow", K(ret), K(pos), K(expr_datum_size), K(data_len));
      } else if (0 == expr_datum_size) {
      } else {
        MEMCPY(expr_datum_buf, buf + pos, expr_datum_size);
        expr_datum->ptr_ = expr_datum_buf;
        pos += expr_datum_size;
        des_mem_size += expr_datum_size;
        expr_datum_buf += expr_datum_size;
      }
    }
  }
  if (OB_SUCC(ret) && des_mem_size != need_extra_mem_size) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: serialize size is not match", K(ret), K(des_mem_size), K(need_extra_mem_size));
  }
  return ret;
}

int ObPxTreeSerializer::deserialize_expr_frame_info(
    const char* buf, int64_t data_len, int64_t& pos, ObExecContext& ctx, ObExprFrameInfo& expr_frame_info)
{
  int ret = OB_SUCCESS;
  int32_t expr_cnt = 0;
  OB_UNIS_DECODE(expr_frame_info.need_ctx_cnt_);
  ObIArray<ObExpr>& exprs = expr_frame_info.rt_exprs_;
  ObExpr::get_serialize_array() = &exprs;
  if (OB_FAIL(serialization::decode_i32(buf, data_len, pos, &expr_cnt))) {
    LOG_WARN("fail to encode op type", K(ret));
  } else if (OB_FAIL(exprs.prepare_allocate(expr_cnt))) {
    LOG_WARN("failed to prepare allocator expr", K(ret));
  } else {
    for (int64_t i = 0; i < expr_cnt && OB_SUCC(ret); ++i) {
      ObExpr& expr = exprs.at(i);
      if (OB_FAIL(expr.deserialize(buf, data_len, pos))) {
        LOG_WARN("failed to serialize expr", K(ret));
      }
    }
  }
  // frames
  int64_t frame_cnt = 0;
  char** frames = nullptr;
  ObPhysicalPlanCtx* plan_ctx = ctx.get_physical_plan_ctx();
  const ObIArray<char*>* param_frame_ptrs = &plan_ctx->get_param_frame_ptrs();
  OB_UNIS_DECODE(frame_cnt);
  if (nullptr == (frames = static_cast<char**>(ctx.get_allocator().alloc(sizeof(char*) * frame_cnt)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed allocate frames", K(ret));
  } else if (FALSE_IT(MEMSET(frames, 0, sizeof(char*) * frame_cnt))) {
  } else if (OB_FAIL(deserialize_frame_info(buf,
                 data_len,
                 pos,
                 ctx.get_allocator(),
                 expr_frame_info.const_frame_,
                 &expr_frame_info.const_frame_ptrs_,
                 frames,
                 frame_cnt))) {
    LOG_WARN("failed to deserialize const frame", K(ret));
  } else if (OB_FAIL(deserialize_frame_info(buf,
                 data_len,
                 pos,
                 ctx.get_allocator(),
                 expr_frame_info.param_frame_,
                 const_cast<ObIArray<char*>*>(param_frame_ptrs),
                 frames,
                 frame_cnt))) {
    LOG_WARN("failed to deserialize const frame", K(ret));
  } else if (OB_FAIL(deserialize_frame_info(buf,
                 data_len,
                 pos,
                 ctx.get_allocator(),
                 expr_frame_info.dynamic_frame_,
                 nullptr,
                 frames,
                 frame_cnt))) {
    LOG_WARN("failed to deserialize const frame", K(ret));
  } else if (OB_FAIL(deserialize_frame_info(buf,
                 data_len,
                 pos,
                 ctx.get_allocator(),
                 expr_frame_info.datum_frame_,
                 nullptr,
                 frames,
                 frame_cnt,
                 true))) {
    LOG_WARN("failed to deserialize const frame", K(ret));
  } else {
    ctx.set_frames(frames);
    ctx.set_frame_cnt(frame_cnt);
  }
  return ret;
}

int64_t ObPxTreeSerializer::get_serialize_frame_info_size(
    ObIArray<ObFrameInfo>& all_frames, char** frames, int64_t frame_cnt, bool no_ser_data /* = false*/)
{
  int ret = OB_SUCCESS;
  int64_t len = 0;
  int64_t need_extra_mem_size = 0;
  const int64_t datum_eval_info_size = sizeof(ObDatum) + sizeof(ObEvalInfo);
  OB_UNIS_ADD_LEN(all_frames.count());
  // OB_UNIS_ADD_LEN(all_frames);
  for (int64_t i = 0; i < all_frames.count() && OB_SUCC(ret); ++i) {
    OB_UNIS_ADD_LEN(all_frames.at(i));
  }
  for (int64_t i = 0; i < all_frames.count() && OB_SUCC(ret); ++i) {
    ObFrameInfo& frame_info = all_frames.at(i);
    if (frame_info.frame_idx_ >= frame_cnt) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("frame index exceed frame count", K(ret), K(frame_cnt), K(frame_info.frame_idx_));
    } else {
      int64_t expr_mem_size = no_ser_data ? 0 : frame_info.expr_cnt_ * datum_eval_info_size;
      OB_UNIS_ADD_LEN(expr_mem_size);
      len += expr_mem_size;
      char* frame_buf = frames[frame_info.frame_idx_];
      for (int64_t j = 0; j < frame_info.expr_cnt_ && OB_SUCC(ret); ++j) {
        ObDatum* expr_datum = reinterpret_cast<ObDatum*>(frame_buf + j * datum_eval_info_size);
        need_extra_mem_size += no_ser_data ? 0 : (expr_datum->null_ ? 0 : expr_datum->len_);
      }
    }
  }
  OB_UNIS_ADD_LEN(need_extra_mem_size);
  int64_t expr_datum_size = 0;
  int64_t ser_mem_size = 0;
  for (int64_t i = 0; i < all_frames.count() && OB_SUCC(ret); ++i) {
    ObFrameInfo& frame_info = all_frames.at(i);
    char* frame_buf = frames[frame_info.frame_idx_];
    for (int64_t j = 0; j < frame_info.expr_cnt_ && OB_SUCC(ret); ++j) {
      ObDatum* expr_datum = reinterpret_cast<ObDatum*>(frame_buf + j * datum_eval_info_size);
      expr_datum_size = no_ser_data ? 0 : (expr_datum->null_ ? 0 : expr_datum->len_);
      OB_UNIS_ADD_LEN(expr_datum_size);
      if (0 < expr_datum_size) {
        len += expr_datum_size;
        ser_mem_size += expr_datum_size;
      }
    }
  }
  if (OB_SUCC(ret) && ser_mem_size != need_extra_mem_size) {
    LOG_ERROR("unexpected status: serialize size is not match", K(ret), K(ser_mem_size), K(need_extra_mem_size));
  }
  return len;
}

int64_t ObPxTreeSerializer::get_serialize_expr_frame_info_size(ObExecContext& ctx, ObExprFrameInfo& expr_frame_info)
{
  int ret = OB_SUCCESS;
  int64_t len = 0;
  OB_UNIS_ADD_LEN(expr_frame_info.need_ctx_cnt_);
  ObIArray<ObExpr>& exprs = expr_frame_info.rt_exprs_;
  int32_t expr_cnt = exprs.count();
  ObExpr::get_serialize_array() = &exprs;
  len += serialization::encoded_length_i32(expr_cnt);
  for (int64_t i = 0; i < exprs.count(); ++i) {
    ObExpr& expr = exprs.at(i);
    len += expr.get_serialize_size();
  }
  OB_UNIS_ADD_LEN(ctx.get_frame_cnt());
  len += get_serialize_frame_info_size(expr_frame_info.const_frame_, ctx.get_frames(), ctx.get_frame_cnt());
  len += get_serialize_frame_info_size(expr_frame_info.param_frame_, ctx.get_frames(), ctx.get_frame_cnt());
  len += get_serialize_frame_info_size(expr_frame_info.dynamic_frame_, ctx.get_frames(), ctx.get_frame_cnt());
  len += get_serialize_frame_info_size(expr_frame_info.datum_frame_, ctx.get_frames(), ctx.get_frame_cnt(), true);
  LOG_DEBUG("trace end get ser expr frame info size", K(ret), K(len));
  return len;
}

int ObPxTreeSerializer::serialize_tree(
    char* buf, int64_t buf_len, int64_t& pos, ObOpSpec& root, bool is_fulltree, ObPhyOpSeriCtx* seri_ctx)
{
  int ret = OB_SUCCESS;
  int32_t child_cnt = (!is_fulltree && IS_RECEIVE(root.type_)) ? 0 : root.get_child_cnt();
  if (OB_FAIL(serialization::encode_vi32(buf, buf_len, pos, root.type_))) {
    LOG_WARN("fail to encode op type", K(ret));
  } else if (OB_FAIL(serialization::encode(buf, buf_len, pos, child_cnt))) {
    LOG_WARN("fail to encode op type", K(ret));
  } else if (OB_FAIL((seri_ctx == NULL ? root.serialize(buf, buf_len, pos)
                                       : root.serialize(buf, buf_len, pos, *seri_ctx)))) {
    LOG_WARN("fail to serialize root", K(ret), "type", root.type_, "root", to_cstring(root));
  } else if ((PHY_TABLE_SCAN_WITH_DOMAIN_INDEX == root.type_ || PHY_TABLE_LOOKUP == root.type_) &&
             OB_FAIL(serialize_sub_plan(buf, buf_len, pos, root))) {
    LOG_WARN("fail to serialize sub plan", K(ret));
  }
  // Terminate serialization when meet ObReceive, as this op indicates
  for (int32_t i = 0; OB_SUCC(ret) && i < child_cnt; ++i) {
    ObOpSpec* child_op = root.get_child(i);
    if (OB_ISNULL(child_op)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null child operator", K(i), K(root.type_));
    } else if (OB_FAIL(serialize_tree(buf, buf_len, pos, *child_op, is_fulltree, seri_ctx))) {
      LOG_WARN("fail to serialize tree", K(ret));
    }
  }
  LOG_DEBUG("end trace serialize tree", K(pos), K(buf_len));
  return ret;
}

int ObPxTreeSerializer::deserialize_tree(const char* buf, int64_t data_len, int64_t& pos, ObPhysicalPlan& phy_plan,
    ObOpSpec*& root, ObIArray<const ObTableScanSpec*>& tsc_ops)
{
  int ret = OB_SUCCESS;
  int32_t phy_operator_type = 0;
  uint32_t child_cnt = 0;
  ObOpSpec* op = NULL;
  if (OB_FAIL(serialization::decode_vi32(buf, data_len, pos, &phy_operator_type))) {
    LOG_WARN("fail to decode phy operator type", K(ret));
  } else if (OB_FAIL(serialization::decode(buf, data_len, pos, child_cnt))) {
    LOG_WARN("fail to encode op type", K(ret));
  } else {
    LOG_DEBUG("deserialize phy_operator",
        K(phy_operator_type),
        "type_str",
        ob_phy_operator_type_str(static_cast<ObPhyOperatorType>(phy_operator_type)),
        K(pos));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(phy_plan.alloc_op_spec(static_cast<ObPhyOperatorType>(phy_operator_type), child_cnt, op, 0))) {
      LOG_WARN("alloc physical operator failed", K(ret));
    } else {
      if (OB_FAIL(op->deserialize(buf, data_len, pos))) {
        LOG_WARN("fail to deserialize operator", K(ret), N_TYPE, phy_operator_type);
      } else if ((PHY_TABLE_SCAN_WITH_DOMAIN_INDEX == op->type_ || PHY_TABLE_LOOKUP == op->type_) &&
                 OB_FAIL(deserialize_sub_plan(buf, data_len, pos, phy_plan, op))) {
        LOG_WARN("fail to deserialize sub plan", K(ret));
      } else {
        LOG_DEBUG("deserialize phy operator",
            K(op->type_),
            "serialize_size",
            op->get_serialize_size(),
            "data_len",
            data_len,
            "pos",
            pos,
            "takes",
            data_len - pos);
      }
    }
  }

  if (OB_SUCC(ret)) {
    if ((op->type_ <= PHY_INVALID || op->type_ >= PHY_END)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid operator type", N_TYPE, op->type_);
    } else if (PHY_TABLE_SCAN == op->type_ && OB_FAIL(tsc_ops.push_back(static_cast<ObTableScanSpec*>(op)))) {
      LOG_WARN("Failed to push back table scan operator", K(ret));
    }
  }

  // Terminate serialization when meet ObReceive, as this op indicates
  if (OB_SUCC(ret)) {
    for (int32_t i = 0; OB_SUCC(ret) && i < child_cnt; i++) {
      ObOpSpec* child = NULL;
      if (OB_FAIL(deserialize_tree(buf, data_len, pos, phy_plan, child, tsc_ops))) {
        LOG_WARN("fail to deserialize tree", K(ret));
      } else if (OB_FAIL(op->set_child(i, child))) {
        LOG_WARN("fail to set child", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    root = op;
  }
  return ret;
}

int ObPxTreeSerializer::deserialize_sub_plan(
    const char* buf, int64_t data_len, int64_t& pos, ObPhysicalPlan& phy_plan, ObOpSpec*& op)
{
  int ret = OB_SUCCESS;
  if (PHY_TABLE_SCAN_WITH_DOMAIN_INDEX == op->get_type()) {
    // ObOpSpec *index_scan_tree = nullptr;
    // ObTableScanWithIndexBack *table_scan = static_cast<ObTableScanWithIndexBack*>(op);
    // if (OB_FAIL(deserialize_op(buf, data_len, pos, phy_plan, index_scan_tree))) {
    //   LOG_WARN("deserialize tree failed", K(ret), K(data_len), K(pos));
    // } else {
    //   table_scan->set_index_scan_tree(index_scan_tree);
    // }
    ret = OB_NOT_SUPPORTED;
  }

  if (OB_SUCC(ret) && PHY_TABLE_LOOKUP == op->get_type()) {
    ObOpSpec* multi_partition_scan_tree = nullptr;
    ObTableLookupSpec* table_lookup = static_cast<ObTableLookupSpec*>(op);
    if (OB_FAIL(deserialize_op(buf, data_len, pos, phy_plan, multi_partition_scan_tree))) {
      LOG_WARN("deserialize tree failed", K(ret), K(data_len), K(pos));
    } else {
      table_lookup->remote_tsc_spec_ = multi_partition_scan_tree;
    }
  }
  return ret;
}

int ObPxTreeSerializer::deserialize_op(
    const char* buf, int64_t data_len, int64_t& pos, ObPhysicalPlan& phy_plan, ObOpSpec*& root)
{
  int ret = OB_SUCCESS;
  int32_t phy_operator_type = 0;
  uint32_t child_cnt = 0;
  ObOpSpec* op = NULL;
  if (OB_FAIL(serialization::decode_vi32(buf, data_len, pos, &phy_operator_type))) {
    LOG_WARN("fail to decode phy operator type", K(ret));
  } else if (OB_FAIL(serialization::decode(buf, data_len, pos, child_cnt))) {
    LOG_WARN("fail to decode phy operator type", K(ret));
  } else {
    LOG_DEBUG("deserialize phy_operator",
        K(phy_operator_type),
        "type_str",
        ob_phy_operator_type_str(static_cast<ObPhyOperatorType>(phy_operator_type)),
        K(pos));
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(phy_plan.alloc_op_spec(static_cast<ObPhyOperatorType>(phy_operator_type), child_cnt, op, 0))) {
      LOG_WARN("alloc physical operator failed", K(ret));
    } else if (OB_FAIL(op->deserialize(buf, data_len, pos))) {
      LOG_WARN("fail to deserialize operator", K(ret), N_TYPE, phy_operator_type);
    } else {
      LOG_DEBUG("deserialize phy operator",
          K(op->get_type()),
          "serialize_size",
          op->get_serialize_size(),
          "data_len",
          data_len,
          "pos",
          pos,
          "takes",
          data_len - pos);
      root = op;
    }
  }
  return ret;
}

int ObPxTreeSerializer::deserialize_tree(
    const char* buf, int64_t data_len, int64_t& pos, ObPhysicalPlan& phy_plan, ObOpSpec*& root)
{
  int ret = OB_SUCCESS;
  ObSEArray<const ObTableScanSpec*, 8> tsc_ops;
  ret = deserialize_tree(buf, data_len, pos, phy_plan, root, tsc_ops);
  return ret;
}

int64_t ObPxTreeSerializer::get_tree_serialize_size(ObOpSpec& root, bool is_fulltree, ObPhyOpSeriCtx* seri_ctx)
{
  int ret = OB_SUCCESS;
  int64_t len = 0;
  int32_t child_cnt = (!is_fulltree && IS_RECEIVE(root.type_)) ? 0 : root.get_child_cnt();
  len += serialization::encoded_length_vi32(root.get_type());
  len += serialization::encoded_length(child_cnt);
  len += (NULL == seri_ctx ? root.get_serialize_size() : root.get_serialize_size(*seri_ctx));
  len += get_sub_plan_serialize_size(root);

  // Terminate serialization when meet ObReceive, as this op indicates
  for (int32_t i = 0; OB_SUCC(ret) && i < child_cnt; ++i) {
    ObOpSpec* child_op = root.get_child(i);
    if (OB_ISNULL(child_op)) {
      LOG_ERROR("null child op", K(i), K(root.get_child_cnt()), K(root.get_type()));
    } else {
      len += get_tree_serialize_size(*child_op, is_fulltree, seri_ctx);
    }
  }
  return len;
}

int ObPxTreeSerializer::serialize_sub_plan(char* buf, int64_t buf_len, int64_t& pos, ObOpSpec& root)
{
  int ret = OB_SUCCESS;
  UNUSED(buf);
  UNUSED(buf_len);
  UNUSED(pos);
  if (PHY_TABLE_SCAN_WITH_DOMAIN_INDEX == root.get_type()) {
    // FIXME: TODO:
    // ObPhyOperator *index_scan_tree =
    //   const_cast<ObPhyOperator *>(static_cast<ObTableScanWithIndexBack&>
    //       (root).get_index_scan_tree());
    // if (OB_ISNULL(index_scan_tree)) {
    //   ret = OB_ERR_UNEXPECTED;
    //   LOG_WARN("index scan tree is null");
    // } else if (OB_FAIL(serialize_tree(buf, buf_len, pos, *index_scan_tree, false))) {
    //   LOG_WARN("serialize index scan tree failed", K(ret));
    // }
    ret = OB_NOT_SUPPORTED;
  } else if (PHY_TABLE_LOOKUP == root.get_type()) {
    ObOpSpec* remote_tsc_spec = static_cast<const ObTableLookupSpec&>(root).remote_tsc_spec_;
    if (OB_ISNULL(remote_tsc_spec)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("index scan tree is null", K(ret));
    } else if (OB_FAIL(serialize_tree(buf, buf_len, pos, *remote_tsc_spec, false))) {
      LOG_WARN("serialize tree failed", K(ret), K(buf_len), K(pos));
    }
  }
  return ret;
}

int64_t ObPxTreeSerializer::get_sub_plan_serialize_size(ObOpSpec& root)
{
  int64_t len = 0;
  // some op contain inner mini plan
  if (PHY_TABLE_SCAN_WITH_DOMAIN_INDEX == root.get_type()) {
    // FIXME: TODO:
    // ObPhyOperator *index_scan_tree =
    //     const_cast<ObPhyOperator *>(static_cast<ObTableScanWithIndexBack &>
    //         (root).get_index_scan_tree());
    // if (index_scan_tree != NULL) {
    //   len += get_tree_serialize_size(*index_scan_tree, false);
    // }
  }
  if (PHY_TABLE_LOOKUP == root.get_type()) {
    ObOpSpec* remote_tsc_spec = static_cast<ObTableLookupSpec&>(root).remote_tsc_spec_;
    if (remote_tsc_spec != NULL) {
      len += get_tree_serialize_size(*remote_tsc_spec, false);
    }
  }
  return len;
}

int ObPxTreeSerializer::deserialize_op_input(
    const char* buf, int64_t data_len, int64_t& pos, ObOpKitStore& op_kit_store)
{
  int ret = OB_SUCCESS;
  int32_t real_input_count = 0;
  if (OB_FAIL(serialization::decode_i32(buf, data_len, pos, &real_input_count))) {
    LOG_WARN("decode int32_t", K(ret), K(data_len), K(pos));
  }
  ObOperatorKit* kit = nullptr;
  ObPhyOperatorType phy_op_type;
  int64_t tmp_phy_op_type = 0;
  int64_t index = 0;
  int64_t ser_input_cnt = 0;
  for (int32_t i = 0; OB_SUCC(ret) && i < real_input_count; ++i) {
    OB_UNIS_DECODE(index);
    OB_UNIS_DECODE(tmp_phy_op_type);
    phy_op_type = static_cast<ObPhyOperatorType>(tmp_phy_op_type);
    kit = op_kit_store.get_operator_kit(index);
    if (nullptr == kit) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("op input is NULL", K(ret), K(index), K(phy_op_type), K(kit), K(real_input_count));
    } else if (nullptr == kit->input_) {
      LOG_WARN("op input is NULL", K(ret), K(index), K(phy_op_type), K(real_input_count));
    } else if (phy_op_type != kit->input_->get_type()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the type of op input is not match", K(ret), K(index), K(phy_op_type), K(kit->input_->get_type()));
    } else {
      OB_UNIS_DECODE(*kit->input_);
      ++ser_input_cnt;
    }
  }
  if (OB_SUCC(ret) && ser_input_cnt != real_input_count) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: ser input is not match", K(ret), K(ser_input_cnt), K(real_input_count));
  }
  return ret;
}

int64_t ObPxTreeSerializer::get_serialize_op_input_size(ObOpSpec& op_spec, ObOpKitStore& op_kit_store, bool is_fulltree)
{
  int64_t len = 0;
  int32_t real_input_count = 0;
  len += serialization::encoded_length_i32(real_input_count);
  len += get_serialize_op_input_tree_size(op_spec, op_kit_store, is_fulltree);
  return len;
}

int64_t ObPxTreeSerializer::get_serialize_op_input_subplan_size(
    ObOpSpec& op_spec, ObOpKitStore& op_kit_store, bool is_fulltree)
{
  int ret = OB_SUCCESS;
  UNUSED(is_fulltree);
  int64_t len = 0;
  if (PHY_TABLE_LOOKUP == op_spec.type_) {
    ObOpSpec* remote_tsc_spec = static_cast<ObTableLookupSpec&>(op_spec).remote_tsc_spec_;
    if (remote_tsc_spec != NULL) {
      len += get_serialize_op_input_tree_size(*remote_tsc_spec, op_kit_store, true /*is_fulltree*/);
    }
  }
  if (OB_SUCC(ret) && PHY_TABLE_SCAN_WITH_DOMAIN_INDEX == op_spec.type_) {
    // do nothing
  }
  return len;
}

int64_t ObPxTreeSerializer::get_serialize_op_input_tree_size(
    ObOpSpec& op_spec, ObOpKitStore& op_kit_store, bool is_fulltree)
{
  int ret = OB_SUCCESS;
  ObOpInput* op_spec_input = NULL;
  int64_t index = op_spec.id_;
  ObOperatorKit* kit = op_kit_store.get_operator_kit(index);
  int64_t len = 0;
  if (nullptr == kit) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("op input is NULL", K(ret), K(index));
  } else if (nullptr != (op_spec_input = kit->input_)) {
    OB_UNIS_ADD_LEN(index);                                            // serialize index
    OB_UNIS_ADD_LEN(static_cast<int64_t>(op_spec_input->get_type()));  // serialize operator type
    OB_UNIS_ADD_LEN(*op_spec_input);                                   // serialize input parameter
  }
  if (PHY_TABLE_SCAN_WITH_DOMAIN_INDEX == op_spec.type_ || PHY_TABLE_LOOKUP == op_spec.type_) {
    len += get_serialize_op_input_subplan_size(op_spec, op_kit_store, true /*is_fulltree*/);
    // do nothing
  }
  // Terminate serialization when meet ObReceive, as this op indicates
  bool serialize_child = is_fulltree || (!IS_RECEIVE(op_spec.type_));
  if (OB_SUCC(ret) && serialize_child) {
    for (int32_t i = 0; OB_SUCC(ret) && i < op_spec.get_child_cnt(); ++i) {
      ObOpSpec* child_op = op_spec.get_child(i);
      if (OB_ISNULL(child_op)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("null child operator", K(i), K(op_spec.type_));
      } else {
        len += get_serialize_op_input_tree_size(*child_op, op_kit_store, is_fulltree);
      }
    }
  }
  return len;
}

int ObPxTreeSerializer::serialize_op_input_subplan(char* buf, int64_t buf_len, int64_t& pos, ObOpSpec& op_spec,
    ObOpKitStore& op_kit_store, bool is_fulltree, int32_t& real_input_count)
{
  int ret = OB_SUCCESS;
  if (PHY_TABLE_LOOKUP == op_spec.type_) {
    ObOpSpec* remote_tsc_spec = static_cast<ObTableLookupSpec&>(op_spec).remote_tsc_spec_;
    if (remote_tsc_spec != NULL) {
      if (OB_FAIL(serialize_op_input_tree(
              buf, buf_len, pos, *remote_tsc_spec, op_kit_store, is_fulltree, real_input_count))) {
        LOG_WARN("fail to serialize_op input tree", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && PHY_TABLE_SCAN_WITH_DOMAIN_INDEX == op_spec.type_) {
    // do nothing
  }
  return ret;
}

int ObPxTreeSerializer::serialize_op_input_tree(char* buf, int64_t buf_len, int64_t& pos, ObOpSpec& op_spec,
    ObOpKitStore& op_kit_store, bool is_fulltree, int32_t& real_input_count)
{
  int ret = OB_SUCCESS;
  ObOpInput* op_spec_input = NULL;
  int64_t index = op_spec.id_;
  ObOperatorKit* kit = op_kit_store.get_operator_kit(index);
  if (nullptr == kit) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("op input is NULL", K(ret), K(index));
  } else if (nullptr != (op_spec_input = kit->input_)) {
    OB_UNIS_ENCODE(index);                                            // serialize index
    OB_UNIS_ENCODE(static_cast<int64_t>(op_spec_input->get_type()));  // serialize operator type
    OB_UNIS_ENCODE(*op_spec_input);                                   // serialize input parameter
    if (OB_SUCC(ret)) {
      ++real_input_count;
    }
  }
  if (OB_SUCC(ret) && (PHY_TABLE_SCAN_WITH_DOMAIN_INDEX == op_spec.type_ || PHY_TABLE_LOOKUP == op_spec.type_) &&
      OB_FAIL(serialize_op_input_subplan(
          buf, buf_len, pos, op_spec, op_kit_store, true /*is_fulltree*/, real_input_count))) {
    LOG_WARN("fail to serialize sub plan", K(ret));
  }
  // Terminate serialization when meet ObReceive, as this op indicates
  bool serialize_child = is_fulltree || (!IS_RECEIVE(op_spec.type_));
  if (OB_SUCC(ret) && serialize_child) {
    for (int32_t i = 0; OB_SUCC(ret) && i < op_spec.get_child_cnt(); ++i) {
      ObOpSpec* child_op = op_spec.get_child(i);
      if (OB_ISNULL(child_op)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null child operator", K(i), K(op_spec.type_));
      } else if (OB_FAIL(serialize_op_input_tree(
                     buf, buf_len, pos, *child_op, op_kit_store, is_fulltree, real_input_count))) {
        LOG_WARN("fail to serialize tree", K(ret));
      }
    }
  }
  return ret;
}

int ObPxTreeSerializer::serialize_op_input(
    char* buf, int64_t buf_len, int64_t& pos, ObOpSpec& op_spec, ObOpKitStore& op_kit_store, bool is_fulltree)
{
  int ret = OB_SUCCESS;
  int64_t input_start_pos = pos;
  int32_t real_input_count = 0;
  pos += serialization::encoded_length_i32(real_input_count);
  if (OB_FAIL(serialize_op_input_tree(buf, buf_len, pos, op_spec, op_kit_store, is_fulltree, real_input_count))) {
    LOG_WARN("failed to serialize spec tree", K(ret));
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(serialization::encode_i32(buf, buf_len, input_start_pos, real_input_count))) {
      LOG_WARN("encode int32_t", K(buf_len), K(input_start_pos), K(real_input_count));
    }
    LOG_TRACE(
        "trace end ser input cnt", K(ret), K(real_input_count), K(op_kit_store.size_), K(pos), K(input_start_pos));
  }
  LOG_DEBUG("end trace ser kit store", K(buf_len), K(pos));
  return ret;
}

//------ end serialize plan tree for engine3.0

// first out timestamp of channel is the first in timestamp of receive
// last out timestamp of channel is the last in timestamp of receive
int ObPxChannelUtil::set_receive_metric(ObIArray<ObDtlChannel*>& channels, ObOpMetric& metric)
{
  int ret = OB_SUCCESS;
  int64_t first_in_ts = INT64_MAX;
  int64_t last_in_ts = 0;
  for (int64_t nth_ch = 0; nth_ch < channels.count(); ++nth_ch) {
    ObDtlChannel* channel = channels.at(nth_ch);
    ObOpMetric& ch_metric = channel->get_op_metric();
    if (first_in_ts > ch_metric.get_first_out_ts()) {
      first_in_ts = ch_metric.get_first_out_ts();
    }
    if (last_in_ts < ch_metric.get_last_out_ts()) {
      last_in_ts = ch_metric.get_last_out_ts();
    }
  }
  if (INT64_MAX != first_in_ts) {
    metric.set_first_in_ts(first_in_ts);
  }
  if (0 != last_in_ts) {
    metric.set_last_in_ts(last_in_ts);
  }
  return ret;
}

// transmit is same as channel
// eg: first in timestamp of channel is same as first in timestamp of transmit
int ObPxChannelUtil::set_transmit_metric(ObIArray<ObDtlChannel*>& channels, ObOpMetric& metric)
{
  int ret = OB_SUCCESS;
  int64_t first_in_ts = INT64_MAX;
  int64_t first_out_ts = INT64_MAX;
  int64_t last_in_ts = 0;
  int64_t last_out_ts = 0;
  for (int64_t nth_ch = 0; nth_ch < channels.count(); ++nth_ch) {
    ObDtlChannel* channel = channels.at(nth_ch);
    ObOpMetric& ch_metric = channel->get_op_metric();
    if (first_in_ts > ch_metric.get_first_in_ts()) {
      first_in_ts = ch_metric.get_first_in_ts();
    }
    if (first_out_ts > ch_metric.get_first_out_ts()) {
      first_out_ts = ch_metric.get_first_out_ts();
    }
    if (last_in_ts < ch_metric.get_last_in_ts()) {
      last_in_ts = ch_metric.get_last_in_ts();
    }
    if (last_out_ts < ch_metric.get_last_out_ts()) {
      last_out_ts = ch_metric.get_last_out_ts();
    }
  }
  if (INT64_MAX != first_in_ts) {
    metric.set_first_in_ts(first_in_ts);
  }
  if (INT64_MAX != first_out_ts) {
    metric.set_first_out_ts(first_out_ts);
  }
  if (0 != last_in_ts) {
    metric.set_last_in_ts(last_in_ts);
  }
  if (0 != last_out_ts) {
    metric.set_last_out_ts(last_out_ts);
  }
  return ret;
}

int ObPxChannelUtil::unlink_ch_set(ObPxTaskChSet& ch_set, ObDtlFlowControl* dfc)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  dtl::ObDtlChannelInfo ci;
  /* ignore error, try to relase all channels */
  if (nullptr == dfc) {
    for (int64_t idx = 0; idx < ch_set.count(); ++idx) {
      if (OB_SUCCESS != (tmp_ret = ch_set.get_channel_info(idx, ci))) {
        ret = tmp_ret;
        LOG_ERROR("fail get channel info", K(idx), K(ret));
      }
      if (OB_SUCCESS != (tmp_ret = dtl::ObDtlChannelGroup::unlink_channel(ci))) {
        ret = tmp_ret;
        LOG_WARN("fail unlink channel", K(ci), K(ret));
      }
    }
  } else {
    ObSEArray<ObDtlChannel*, 16> chans;
    ObDtlChannel* ch = nullptr;
    for (int64_t idx = 0; idx < ch_set.count(); ++idx) {
      if (OB_SUCCESS != (tmp_ret = ch_set.get_channel_info(idx, ci))) {
        ret = tmp_ret;
        LOG_ERROR("fail get channel info", K(idx), K(ret));
      }
      if (OB_SUCCESS != (tmp_ret = dtl::ObDtlChannelGroup::remove_channel(ci, ch))) {
        ret = tmp_ret;
        LOG_WARN("fail unlink channel", K(ci), K(ret));
      } else if (OB_ISNULL(ch)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("channel is null", K(ci), K(ret));
      } else if (OB_SUCCESS != (tmp_ret = chans.push_back(ch))) {
        ret = tmp_ret;
        if (OB_SUCCESS != (tmp_ret = DTL.get_dfc_server().unregister_dfc_channel(*dfc, ch))) {
          ret = tmp_ret;
          LOG_ERROR("failed to unregister dfc channel", K(ci), K(ret), K(ret));
        }
        ob_delete(ch);
        LOG_WARN("failed to push back channels", K(ci), K(ret));
      }
    }
    if (OB_SUCCESS != (tmp_ret = DTL.get_dfc_server().deregister_dfc(*dfc))) {
      ret = tmp_ret;
      // the following unlink actions is not safe is any unregister failure happened
      LOG_ERROR("fail deregister all channel from dfc server", KR(tmp_ret));
    }
    if (0 < chans.count()) {
      for (int64_t idx = chans.count() - 1; 0 <= idx; --idx) {
        if (OB_SUCCESS != (tmp_ret = chans.pop_back(ch))) {
          ret = tmp_ret;
          LOG_ERROR("failed to unregister channel", K(ret));
        } else {
          ob_delete(ch);
        }
      }
    }
  }
  return ret;
}

int ObPxChannelUtil::flush_rows(common::ObIArray<dtl::ObDtlChannel*>& channels)
{
  int ret = OB_SUCCESS;
  dtl::ObDtlChannel* ch = NULL;
  /* ignore error, try to flush all channels */
  for (int64_t slice_idx = 0; slice_idx < channels.count(); ++slice_idx) {
    if (NULL == (ch = channels.at(slice_idx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected NULL ptr", K(ret));
    } else {
      if (OB_FAIL(ch->flush())) {
        LOG_WARN("Fail to flush row to slice channel."
                 "The peer side may not set up as expected.",
            K(slice_idx),
            "peer",
            ch->get_peer(),
            K(ret));
      }
    }
  }
  return ret;
}

int ObPxChannelUtil::dtl_channles_asyn_wait(common::ObIArray<dtl::ObDtlChannel*>& channels, bool ignore_error)
{
  int ret = OB_SUCCESS;
  ARRAY_FOREACH_X(channels, idx, cnt, OB_SUCC(ret))
  {
    ObDtlChannel* ch = channels.at(idx);
    if (OB_ISNULL(ch)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: channel is null", K(ret), K(idx));
    } else if (OB_FAIL(ch->flush())) {
      if (ignore_error) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to wait for channel", K(ret), K(idx), "peer", ch->get_peer());
      }
    }
  }
  return ret;
}

int ObPxChannelUtil::sqcs_channles_asyn_wait(ObIArray<ObPxSqcMeta*>& sqcs)
{
  int ret = OB_SUCCESS;
  ARRAY_FOREACH_X(sqcs, idx, cnt, OB_SUCC(ret))
  {
    ObDtlChannel* ch = sqcs.at(idx)->get_qc_channel();
    if (OB_ISNULL(ch)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: channel is null", K(ret), K(idx));
    } else if (OB_FAIL(ch->flush())) {
      LOG_WARN("failed to wait for channel", K(ret), K(idx), "peer", ch->get_peer());
    }
  }
  return ret;
}

int ObPxAffinityByRandom::add_partition(int64_t partition_id, int64_t partition_idx, int64_t worker_cnt,
    uint64_t tenant_id, ObPxPartitionInfo& partition_row_info)
{
  int ret = OB_SUCCESS;
  if (0 >= worker_cnt) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("The worker cnt is invalid", K(ret), K(worker_cnt));
  } else {
    PartitionHashValue part_hash_value;
    uint64_t value = (tenant_id << 32 | partition_idx);
    part_hash_value.hash_value_ = common::murmurhash(&value, sizeof(value), worker_cnt);
    part_hash_value.partition_idx_ = partition_idx;
    part_hash_value.partition_id_ = partition_id;
    part_hash_value.partition_info_ = partition_row_info;
    worker_cnt_ = worker_cnt;
    if (OB_FAIL(partition_hash_values_.push_back(part_hash_value))) {
      LOG_WARN("Failed to push back item", K(ret));
    }
  }
  return ret;
}

int ObPxAffinityByRandom::do_random(bool use_partition_info)
{
  int ret = OB_SUCCESS;
  if (!partition_hash_values_.empty()) {
    if (!use_partition_info) {
      int64_t worker_id = 0;
      for (int64_t idx = 0; idx < partition_hash_values_.count(); ++idx) {
        if (worker_id > worker_cnt_ - 1) {
          worker_id = 0;
        }
        partition_hash_values_.at(idx).worker_id_ = worker_id++;
      }
    } else {
      common::ObArray<int64_t> workers_load;
      if (OB_FAIL(workers_load.prepare_allocate(worker_cnt_))) {
        LOG_WARN("Failed to do prepare allocate", K(ret));
      }
      ARRAY_FOREACH(workers_load, idx)
      {
        workers_load.at(idx) = 0;
      }
      /**
       * Why do we need to keep the order of partitions designed in the same worker?
       * Because in the case of global order, the parition has been performed on the qc side
       * In order to sort, if the order of the partition in a single worker is out of order, it does not match
       * The expectation of global order, that is, the data in a single worker is out of order.
       */
      bool asc_order = true;
      if (partition_hash_values_.count() > 1 &&
          (partition_hash_values_.at(0).partition_idx_ > partition_hash_values_.at(1).partition_idx_)) {
        asc_order = false;
      }

      auto compare_fun = [](PartitionHashValue a, PartitionHashValue b) -> bool {
        return a.hash_value_ > b.hash_value_;
      };
      std::sort(partition_hash_values_.begin(), partition_hash_values_.end(), compare_fun);

      ARRAY_FOREACH(partition_hash_values_, idx)
      {
        int64_t min_load_index = 0;
        int64_t min_load = INT64_MAX;
        ARRAY_FOREACH(workers_load, i)
        {
          if (workers_load.at(i) < min_load) {
            min_load_index = i;
            min_load = workers_load.at(i);
          }
        }
        partition_hash_values_.at(idx).worker_id_ = min_load_index;
        workers_load.at(min_load_index) += partition_hash_values_.at(idx).partition_info_.physical_row_count_ + 1;
      }
      LOG_DEBUG("Workers load", K(workers_load));

      if (asc_order) {
        auto compare_fun_order_by_part_asc = [](PartitionHashValue a, PartitionHashValue b) -> bool {
          return a.partition_idx_ < b.partition_idx_;
        };
        std::sort(partition_hash_values_.begin(), partition_hash_values_.end(), compare_fun_order_by_part_asc);
      } else {
        auto compare_fun_order_by_part_desc = [](PartitionHashValue a, PartitionHashValue b) -> bool {
          return a.partition_idx_ > b.partition_idx_;
        };
        std::sort(partition_hash_values_.begin(), partition_hash_values_.end(), compare_fun_order_by_part_desc);
      }
    }
  }
  return ret;
}

int ObPxAffinityByRandom::get_partition_info(
    int64_t partition_id, ObIArray<ObPxPartitionInfo>& partitions_info, ObPxPartitionInfo& partition_info)
{
  int ret = OB_SUCCESS;
  bool find = false;
  if (!partitions_info.empty()) {
    ARRAY_FOREACH(partitions_info, idx)
    {
      if (partitions_info.at(idx).partition_key_.get_partition_id() == partition_id) {
        find = true;
        partition_info.assign(partitions_info.at(idx));
      }
    }
    if (!find) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Failed to find partition in partitions_info", K(ret), K(partition_id), K(partitions_info));
    }
  }
  return ret;
}

int ObPxAdmissionUtil::check_parallel_max_servers_value(
    const common::ObIArray<share::ObUnitInfo>& units, const int64_t user_max_servers, int64_t& max_servers)
{
  int ret = OB_SUCCESS;
  bool hit = false;
  int64_t cpu = INT64_MAX;
  for (int i = 0; i < units.count() && OB_SUCC(ret); ++i) {
    if (REPLICA_TYPE_FULL == units.at(i).unit_.replica_type_) {
      if (cpu > units.at(i).config_.min_cpu_) {
        cpu = units.at(i).config_.min_cpu_;
        hit = true;
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (hit) {
      max_servers = std::min(cpu * static_cast<int64_t>(GCONF.px_workers_per_cpu_quota.get()), user_max_servers);
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("no valid unit for current tenant", K(units), K(ret));
    }
  }
  return ret;
}

double ObPxSqcUtil::get_sqc_partition_ratio(ObExecContext* exec_ctx)
{
  double ratio = 1.0;
  ObPxSqcHandler* sqc_handle = exec_ctx->get_sqc_handler();
  if (OB_NOT_NULL(sqc_handle)) {
    ObPxRpcInitSqcArgs& sqc_args = sqc_handle->get_sqc_init_arg();
    int64_t total_part_cnt = sqc_args.sqc_.get_total_part_count();
    int64_t sqc_part_cnt = sqc_args.sqc_.get_locations().count();
    if (0 < total_part_cnt && 0 < sqc_part_cnt) {
      ratio = sqc_part_cnt * 1.0 / total_part_cnt;
    }
    LOG_TRACE("trace sqc partition ratio", K(total_part_cnt), K(sqc_part_cnt));
  }
  return ratio;
}

double ObPxSqcUtil::get_sqc_est_worker_ratio(ObExecContext* exec_ctx)
{
  double ratio = 1.0;
  ObPxSqcHandler* sqc_handle = exec_ctx->get_sqc_handler();
  if (OB_NOT_NULL(sqc_handle)) {
    ObPxRpcInitSqcArgs& sqc_args = sqc_handle->get_sqc_init_arg();
    int64_t est_total_task_cnt = sqc_args.sqc_.get_total_task_count();
    int64_t est_sqc_task_cnt = sqc_args.sqc_.get_max_task_count();
    if (0 < est_total_task_cnt && 0 < est_sqc_task_cnt) {
      ratio = est_sqc_task_cnt * 1.0 / est_total_task_cnt;
    }
    LOG_TRACE("trace sqc estimate worker ratio", K(est_sqc_task_cnt), K(est_total_task_cnt));
  }
  return ratio;
}

int64_t ObPxSqcUtil::get_est_total_worker_count(ObExecContext* exec_ctx)
{
  int64_t worker_cnt = 1;
  ObPxSqcHandler* sqc_handle = exec_ctx->get_sqc_handler();
  if (OB_NOT_NULL(sqc_handle)) {
    ObPxRpcInitSqcArgs& sqc_args = sqc_handle->get_sqc_init_arg();
    worker_cnt = sqc_args.sqc_.get_total_task_count();
  }
  return worker_cnt;
}

int64_t ObPxSqcUtil::get_est_sqc_worker_count(ObExecContext* exec_ctx)
{
  int64_t worker_cnt = 1;
  ObPxSqcHandler* sqc_handle = exec_ctx->get_sqc_handler();
  if (OB_NOT_NULL(sqc_handle)) {
    ObPxRpcInitSqcArgs& sqc_args = sqc_handle->get_sqc_init_arg();
    worker_cnt = sqc_args.sqc_.get_max_task_count();
  }
  return worker_cnt;
}

int64_t ObPxSqcUtil::get_total_partition_count(ObExecContext* exec_ctx)
{
  int64_t total_part_cnt = 1;
  ObPxSqcHandler* sqc_handle = exec_ctx->get_sqc_handler();
  if (OB_NOT_NULL(sqc_handle)) {
    ObPxRpcInitSqcArgs& sqc_args = sqc_handle->get_sqc_init_arg();
    int64_t tmp_total_part_cnt = sqc_args.sqc_.get_total_part_count();
    if (0 < tmp_total_part_cnt) {
      total_part_cnt = tmp_total_part_cnt;
    }
  }
  return total_part_cnt;
}

int64_t ObPxSqcUtil::get_sqc_total_partition_count(ObExecContext* exec_ctx)
{
  int64_t total_part_cnt = 1;
  ObPxSqcHandler* sqc_handle = exec_ctx->get_sqc_handler();
  if (OB_NOT_NULL(sqc_handle)) {
    ObPxRpcInitSqcArgs& sqc_args = sqc_handle->get_sqc_init_arg();
    total_part_cnt = sqc_args.sqc_.get_locations().count();
  }
  return total_part_cnt;
}

// SQC that belongs to leaf dfo should estimate size by GI or Tablescan
int64_t ObPxSqcUtil::get_actual_worker_count(ObExecContext* exec_ctx)
{
  int64_t sqc_actual_worker_cnt = 1;
  ObPxSqcHandler* sqc_handle = exec_ctx->get_sqc_handler();
  if (OB_NOT_NULL(sqc_handle)) {
    ObPxRpcInitSqcArgs& sqc_args = sqc_handle->get_sqc_init_arg();
    sqc_actual_worker_cnt = sqc_args.sqc_.get_task_count();
  }
  return sqc_actual_worker_cnt;
}

uint64_t ObPxSqcUtil::get_plan_id(ObExecContext* exec_ctx)
{
  uint64_t plan_id = UINT64_MAX;
  if (OB_NOT_NULL(exec_ctx)) {
    ObPhysicalPlanCtx* plan_ctx = exec_ctx->get_physical_plan_ctx();
    if (OB_NOT_NULL(plan_ctx) && OB_NOT_NULL(plan_ctx->get_phy_plan())) {
      plan_id = plan_ctx->get_phy_plan()->get_plan_id();
    }
  }
  return plan_id;
}

const char* ObPxSqcUtil::get_sql_id(ObExecContext* exec_ctx)
{
  const char* sql_id = nullptr;
  if (OB_NOT_NULL(exec_ctx)) {
    ObPhysicalPlanCtx* plan_ctx = exec_ctx->get_physical_plan_ctx();
    if (OB_NOT_NULL(plan_ctx) && OB_NOT_NULL(plan_ctx->get_phy_plan())) {
      sql_id = plan_ctx->get_phy_plan()->get_sql_id();
    }
  }
  return sql_id;
}

uint64_t ObPxSqcUtil::get_exec_id(ObExecContext* exec_ctx)
{
  uint64_t exec_id = UINT64_MAX;
  if (OB_NOT_NULL(exec_ctx)) {
    exec_id = exec_ctx->get_execution_id();
  }
  return exec_id;
}

uint64_t ObPxSqcUtil::get_session_id(ObExecContext* exec_ctx)
{
  uint64_t session_id = UINT64_MAX;
  if (OB_NOT_NULL(exec_ctx)) {
    if (OB_NOT_NULL(exec_ctx->get_my_session())) {
      session_id = exec_ctx->get_my_session()->get_sessid_for_table();
    }
  }
  return session_id;
}

int ObPxEstimateSizeUtil::get_px_size(
    ObExecContext* exec_ctx, const PxOpSizeFactor factor, const int64_t total_size, int64_t& ret_size)
{
  int ret = OB_SUCCESS;
  int64_t actual_worker = 0;
  double sqc_part_ratio = 0;
  int64_t total_part_cnt = 0;
  double sqc_est_worker_ratio = 0;
  int64_t total_actual_worker_cnt = 0;
  ret_size = total_size;
  if (factor.has_leaf_granule()) {
    // leaf
    if (!factor.has_exchange()) {
      if (factor.has_block_granule()) {
        actual_worker = ObPxSqcUtil::get_actual_worker_count(exec_ctx);
        sqc_est_worker_ratio = ObPxSqcUtil::get_sqc_est_worker_ratio(exec_ctx);
        ret_size = sqc_est_worker_ratio * total_size / actual_worker;
      } else if (factor.partition_granule_child_) {
        total_part_cnt = ObPxSqcUtil::get_total_partition_count(exec_ctx);
        ret_size = total_size / total_part_cnt;
      } else if (factor.partition_granule_parent_) {
        // total_size / total_part_cnt * sqc_part_cnt
        actual_worker = ObPxSqcUtil::get_actual_worker_count(exec_ctx);
        sqc_part_ratio = ObPxSqcUtil::get_sqc_partition_ratio(exec_ctx);
        ret_size = sqc_part_ratio * ret_size / actual_worker;
      }
    } else {
      if (factor.pk_exchange_) {
        if (factor.has_block_granule()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpect status: block granule with PK");
        } else {
          // total_size / total_part_cnt * sqc_part_cnt / sqc_autual_worker_count
          actual_worker = ObPxSqcUtil::get_actual_worker_count(exec_ctx);
          sqc_part_ratio = ObPxSqcUtil::get_sqc_partition_ratio(exec_ctx);
          ret_size = sqc_part_ratio * ret_size / actual_worker;
        }
      }
    }
  } else {
    if (factor.broadcast_exchange_) {
    } else {
      actual_worker = ObPxSqcUtil::get_actual_worker_count(exec_ctx);
      sqc_est_worker_ratio = ObPxSqcUtil::get_sqc_est_worker_ratio(exec_ctx);
      ret_size = total_size * sqc_est_worker_ratio / actual_worker;
    }
  }
  if (ret_size > total_size || OB_FAIL(ret)) {
    LOG_WARN("unpexpect status: estimate size is greater than total size", K(ret_size), K(total_size), K(ret));
    ret_size = total_size;
    ret = OB_SUCCESS;
  }
  if (0 >= ret_size) {
    ret_size = 1;
  }
  LOG_TRACE("trace get px size",
      K(ret_size),
      K(total_size),
      K(factor),
      K(actual_worker),
      K(sqc_part_ratio),
      K(total_part_cnt),
      K(sqc_est_worker_ratio),
      K(total_actual_worker_cnt));
  return ret;
}

int ObSlaveMapUtil::build_mn_channel(
    ObPxChTotalInfos* dfo_ch_total_infos, ObDfo& child, ObDfo& parent, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(dfo_ch_total_infos)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("transmit or receive mn channel info is null", KP(dfo_ch_total_infos));
  } else {
    OZ(dfo_ch_total_infos->prepare_allocate(1));
    if (OB_SUCC(ret)) {
      ObDtlChTotalInfo& transmit_ch_info = dfo_ch_total_infos->at(0);
      OZ(ObDfo::fill_channel_info_by_sqc(transmit_ch_info.transmit_exec_server_, child.get_sqcs()));
      OZ(ObDfo::fill_channel_info_by_sqc(transmit_ch_info.receive_exec_server_, parent.get_sqcs()));
      transmit_ch_info.channel_count_ = transmit_ch_info.transmit_exec_server_.total_task_cnt_ *
                                        transmit_ch_info.receive_exec_server_.total_task_cnt_;
      transmit_ch_info.start_channel_id_ =
          ObDtlChannel::generate_id(transmit_ch_info.channel_count_) - transmit_ch_info.channel_count_ + 1;
      transmit_ch_info.tenant_id_ = tenant_id;
    }
  }
  return ret;
}

int ObSlaveMapUtil::build_bf_mn_channel(
    ObDtlChTotalInfo& transmit_ch_info, ObDfo& child, ObDfo& parent, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  OZ(ObDfo::fill_channel_info_by_sqc(transmit_ch_info.transmit_exec_server_, child.get_sqcs()));
  OZ(ObDfo::fill_channel_info_by_sqc(transmit_ch_info.receive_exec_server_, parent.get_sqcs()));
  transmit_ch_info.channel_count_ = transmit_ch_info.transmit_exec_server_.exec_addrs_.count() *
                                    transmit_ch_info.receive_exec_server_.exec_addrs_.count();
  transmit_ch_info.start_channel_id_ =
      ObDtlChannel::generate_id(transmit_ch_info.channel_count_) - transmit_ch_info.channel_count_ + 1;
  transmit_ch_info.tenant_id_ = tenant_id;
  return ret;
}

int ObSlaveMapUtil::build_mn_channel_per_sqcs(
    ObPxChTotalInfos* dfo_ch_total_infos, ObDfo& child, ObDfo& parent, int64_t sqc_count, uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(dfo_ch_total_infos)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("transmit or receive mn channel info is null", KP(dfo_ch_total_infos));
  } else {
    OZ(dfo_ch_total_infos->prepare_allocate(sqc_count));
    if (OB_SUCC(ret)) {
      for (int64_t i = 0; i < sqc_count && OB_SUCC(ret); ++i) {
        ObDtlChTotalInfo& transmit_ch_info = dfo_ch_total_infos->at(i);
        OZ(ObDfo::fill_channel_info_by_sqc(transmit_ch_info.transmit_exec_server_, child.get_sqcs()));
        OZ(ObDfo::fill_channel_info_by_sqc(transmit_ch_info.receive_exec_server_, parent.get_sqcs()));
        transmit_ch_info.channel_count_ = transmit_ch_info.transmit_exec_server_.total_task_cnt_ *
                                          transmit_ch_info.receive_exec_server_.total_task_cnt_;
        transmit_ch_info.start_channel_id_ =
            ObDtlChannel::generate_id(transmit_ch_info.channel_count_) - transmit_ch_info.channel_count_ + 1;
        transmit_ch_info.tenant_id_ = tenant_id;
      }
    }
  }
  return ret;
}

// Plan:
// GI(Partition)
//    Hash Join           Hash Join
//      hash   ->>          Exchange(hash local)
//      hash                Exchange(hash local)
// Hash local refers to only doing hash hash inside the server, and will not shuffle across servers
int ObSlaveMapUtil::build_pwj_slave_map_mn_group(ObDfo& parent, ObDfo& child, uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObPxChTotalInfos* dfo_ch_total_infos = &child.get_dfo_ch_total_infos();
  int64_t child_dfo_idx = -1;
  if (parent.get_sqcs_count() != child.get_sqcs_count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pwj must have some sqc count", K(ret));
  } else if (OB_FAIL(ObDfo::check_dfo_pair(parent, child, child_dfo_idx))) {
    LOG_WARN("failed to check dfo pair", K(ret));
  } else if (OB_FAIL(build_mn_channel_per_sqcs(dfo_ch_total_infos, child, parent, child.get_sqcs_count(), tenant_id))) {
    LOG_WARN("failed to build mn channel per sqc", K(ret));
  } else {
    LOG_DEBUG("build pwj slave map group", K(child.get_dfo_id()));
  }
  return ret;
}

int ObSlaveMapUtil::build_partition_map_by_sqcs(
    const common::ObIArray<const ObPxSqcMeta*>& sqcs, ObIArray<int64_t>& prefix_task_counts, ObPxPartChMapArray& map)
{
  int ret = OB_SUCCESS;
  UNUSED(prefix_task_counts);
  if (prefix_task_counts.count() != sqcs.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("prefix task counts is not match sqcs count", K(ret));
  }
  for (int64_t i = 0; i < sqcs.count() && OB_SUCC(ret); i++) {
    const ObPxSqcMeta* sqc = sqcs.at(i);
    const ObPartitionReplicaLocationIArray& locations = sqc->get_locations();
    if (locations.count() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the size of location is zero in one sqc", K(i), K(sqcs.count()), K(ret), K(*sqc));
    }
    ARRAY_FOREACH_X(locations, loc_idx, loc_cnt, OB_SUCC(ret))
    {
      const ObPartitionReplicaLocation& location = locations.at(loc_idx);
      int64_t partition_id = location.get_partition_id();
      // int64_t prefix_task_count = prefix_task_counts.at(i);
      OZ(map.push_back(ObPxPartChMapItem(partition_id, i)));
      LOG_DEBUG("debug push partition map", K(partition_id), K(i), K(sqc->get_sqc_id()));
    }
  }
  LOG_DEBUG("debug push partition map", K(map));
  return ret;
}

int ObSlaveMapUtil::build_affinitized_partition_map_by_sqcs(const common::ObIArray<const ObPxSqcMeta*>& sqcs,
    ObIArray<int64_t>& prefix_task_counts, int64_t total_task_cnt, ObPxPartChMapArray& map)
{
  int ret = OB_SUCCESS;
  if (sqcs.count() <= 0 || prefix_task_counts.count() != sqcs.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("prefix task counts is not match sqcs count", K(sqcs.count()), K(ret));
  }
  for (int64_t i = 0; i < sqcs.count() && OB_SUCC(ret); i++) {
    // There are locations.count() partitions on the target sqc
    // There are sqc_task_count threads on the target sqc to process these partitions
    // You can use Round-Robin to mix these partitions onto threads
    // However, due to the use of the map side to make a hypothesis of the same part of the processing thread, it must
    // be adjacent It is easier to do hash operation here.
    //
    // [p0][p1][p2][p3][p4]
    // [t0][t1][t0][t1][t0]
    //
    // [p0][p0][p1][p1][p2]
    // [t0][t1][t2][t3][t4]
    //
    // [p0][p1][p2][p3][p4]
    // [t0][t0][t1][t1][t2]
    //
    // double task_per_part = (double)sqc_task_count / locations.count();
    // for (int64_t idx = 0; idx < locations.count(); ++idx) {
    //  for (int j = 0; j < task_per_part; j++) {
    //    int64_t p = idx;
    //    int64_t t = idx * task_per_part + j;
    //    if (t >= sqc_task_count) {
    //      break;
    //    }
    //    emit(idx, t);
    //  }
    // }
    int64_t sqc_task_count = 0;
    int64_t prefix_task_count = prefix_task_counts.at(i);
    if (i + 1 == prefix_task_counts.count()) {
      sqc_task_count = total_task_cnt - prefix_task_counts.at(i);
    } else {
      sqc_task_count = prefix_task_counts.at(i + 1) - prefix_task_counts.at(i);
    }
    const ObPxSqcMeta* sqc = sqcs.at(i);
    const ObPartitionReplicaLocationIArray& locations = sqc->get_locations();
    if (locations.count() <= 0 || sqc_task_count <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the size of location is zero in one sqc", K(ret), K(sqc_task_count), K(*sqc));
      break;
    }

    // task_per_part: How many thread services does each partition have,
    // rest_task: each of these tasks is responsible for one more partition, don't waste cpu

    // eg:
    // 13 threads, 3 partitions, task_per_part = 4, rest_task = 1 to do the first partition
    // 3 threads, 5 partitions, task_per_part = 0, rest_task = 3 to do 1, 2, 3 partitions
    int64_t task_per_part = sqc_task_count / locations.count();
    int64_t rest_task = sqc_task_count % locations.count();
    if (task_per_part > 0) {
      int64_t t = 0;
      for (int64_t p = 0; OB_SUCC(ret) && p < locations.count(); ++p) {
        int64_t partition_id = locations.at(p).get_partition_id();
        int64_t next = (p >= rest_task) ? task_per_part : task_per_part + 1;
        for (int64_t loop = 0; OB_SUCC(ret) && loop < next; ++loop) {
          // first:partition_id, second: prefix_task_count, third: sqc_task_idx
          OZ(map.push_back(ObPxPartChMapItem(partition_id, prefix_task_count, t)));
          LOG_DEBUG("XXXXX:t>p: push partition map", K(partition_id), "sqc", i, "g_t", prefix_task_count + t, K(t));
          t++;
        }
      }
    } else {

      // 2: Number of threads <number of partitions
      // Important: Ensure that each partition has at least one thread processing
      for (int64_t p = 0; OB_SUCC(ret) && p < locations.count(); ++p) {
        int64_t t = p % rest_task;
        int64_t partition_id = locations.at(p).get_partition_id();
        // first:partition_id, second: prefix_task_count, third: sqc_task_idx
        OZ(map.push_back(ObPxPartChMapItem(partition_id, prefix_task_count, t)));
        LOG_DEBUG("XXXXX:t<=p: push partition map", K(partition_id), "sqc", i, "g_t", prefix_task_count + t, K(t));
      }
    }
  }
  LOG_DEBUG("debug push partition map", K(map));
  return ret;
}

int ObSlaveMapUtil::build_ppwj_bcast_slave_mn_map(ObDfo& parent, ObDfo& child, uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  int64_t child_dfo_idx = -1;
  ObPxChTotalInfos* dfo_ch_total_infos = &child.get_dfo_ch_total_infos();
  ObPxPartChMapArray& map = child.get_part_ch_map();
  LOG_DEBUG("build ppwj bcast slave map", K(parent.get_dfo_id()), K(parent.get_sqcs_count()), K(parent.get_tasks()));
  common::ObSEArray<const ObPxSqcMeta*, 8> sqcs;
  if (OB_FAIL(parent.get_sqcs(sqcs))) {
    LOG_WARN("failed to get sqcs", K(ret));
  } else if (sqcs.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the count of sqc is unexpected", K(ret), K(sqcs.count()));
  } else if (OB_FAIL(ObDfo::check_dfo_pair(parent, child, child_dfo_idx))) {
    LOG_WARN("failed to check dfo pair", K(ret));
  } else if (OB_FAIL(build_mn_channel(dfo_ch_total_infos, child, parent, tenant_id))) {
    LOG_WARN("failed to build mn channels", K(ret));
  } else if (OB_ISNULL(dfo_ch_total_infos) || 1 != dfo_ch_total_infos->count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: receive ch info is error", K(ret), KP(dfo_ch_total_infos));
  } else if (OB_FAIL(build_partition_map_by_sqcs(
                 sqcs, dfo_ch_total_infos->at(0).receive_exec_server_.prefix_task_counts_, map))) {
    LOG_WARN("failed to build channel map by sqc", K(ret));
  } else if (map.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the size of channel map is unexpected", K(ret), K(map.count()));
  }
  return ret;
}

int ObSlaveMapUtil::build_ppwj_slave_mn_map(ObDfo& parent, ObDfo& child, uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (2 != parent.get_child_count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected dfo", K(ret), K(parent));
  } else if (ObPQDistributeMethod::PARTITION_HASH == child.get_dist_method()) {
    ObDfo* reference_child = nullptr;
    int64_t child_dfo_idx = -1;
    common::ObSEArray<const ObPxSqcMeta*, 8> sqcs;
    ObPxChTotalInfos* dfo_ch_total_infos = &child.get_dfo_ch_total_infos();
    ObPxPartChMapArray& map = child.get_part_ch_map();
    if (OB_FAIL(parent.get_child_dfo(0, reference_child))) {
      LOG_WARN("failed to get dfo", K(ret));
    } else if (reference_child->get_dfo_id() == child.get_dfo_id() &&
               OB_FAIL(parent.get_child_dfo(1, reference_child))) {
      LOG_WARN("failed to get dfo", K(ret));
    } else if (OB_FAIL(ObDfo::check_dfo_pair(parent, child, child_dfo_idx))) {
      LOG_WARN("failed to check dfo pair", K(ret));
    } else if (OB_FAIL(build_mn_channel(dfo_ch_total_infos, child, parent, tenant_id))) {
      LOG_WARN("failed to build mn channels", K(ret));
    } else if (OB_ISNULL(dfo_ch_total_infos) || 1 != dfo_ch_total_infos->count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: receive ch info is error", K(ret), KP(dfo_ch_total_infos));
    } else if (OB_FAIL(reference_child->get_sqcs(sqcs))) {
      LOG_WARN("failed to get sqcs", K(ret));
    } else if (sqcs.count() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the count of sqc is unexpected", K(ret), K(sqcs.count()));
    } else if (OB_FAIL(build_partition_map_by_sqcs(
                   sqcs, dfo_ch_total_infos->at(0).receive_exec_server_.prefix_task_counts_, map))) {
      LOG_WARN("failed to build channel map by sqc", K(ret));
    }
  } else if (OB_FAIL(build_pwj_slave_map_mn_group(parent, child, tenant_id))) {
    LOG_WARN("failed to build ppwj slave map", K(ret));
  }
  return ret;
}

int ObSlaveMapUtil::build_pkey_affinitized_ch_mn_map(ObDfo& parent, ObDfo& child, uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (1 != parent.get_child_count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected dfo", K(ret), K(parent));
  } else if (ObPQDistributeMethod::PARTITION_HASH == child.get_dist_method()) {
    LOG_TRACE("build pkey affinitiezed channel map",
        K(parent.get_dfo_id()),
        K(parent.get_sqcs_count()),
        K(child.get_dfo_id()),
        K(child.get_sqcs_count()));
    ObPxPartChMapArray& map = child.get_part_ch_map();
    common::ObSEArray<const ObPxSqcMeta*, 8> sqcs;
    ObPxChTotalInfos* dfo_ch_total_infos = &child.get_dfo_ch_total_infos();
    int64_t child_dfo_idx = -1;
    if (OB_FAIL(parent.get_sqcs(sqcs))) {
      LOG_WARN("failed to get sqcs", K(ret));
    } else if (sqcs.count() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the count of sqc is unexpected", K(ret), K(sqcs.count()));
    } else if (OB_FAIL(ObDfo::check_dfo_pair(parent, child, child_dfo_idx))) {
      LOG_WARN("failed to check dfo pair", K(ret));
    } else if (OB_FAIL(build_mn_channel(dfo_ch_total_infos, child, parent, tenant_id))) {
      LOG_WARN("failed to build mn channels", K(ret));
    } else if (OB_ISNULL(dfo_ch_total_infos) || 1 != dfo_ch_total_infos->count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: receive ch info is error", K(ret), KP(dfo_ch_total_infos));
    } else if (OB_FAIL(build_affinitized_partition_map_by_sqcs(sqcs,
                   dfo_ch_total_infos->at(0).receive_exec_server_.prefix_task_counts_,
                   dfo_ch_total_infos->at(0).receive_exec_server_.total_task_cnt_,
                   map))) {
      LOG_WARN("failed to build channel map by sqc", K(ret));
    } else if (map.count() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the size of channel map is unexpected", K(ret), K(map.count()), K(parent.get_tasks()), K(sqcs));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected child dfo", K(ret), K(child.get_dist_method()));
  }
  return ret;
}

int ObSlaveMapUtil::build_pkey_random_ch_mn_map(ObDfo& parent, ObDfo& child, uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (1 != parent.get_child_count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected dfo", K(ret), K(parent));
  } else if (ObPQDistributeMethod::PARTITION_RANDOM == child.get_dist_method()) {
    LOG_TRACE("build pkey random channel map",
        K(parent.get_dfo_id()),
        K(parent.get_sqcs_count()),
        K(child.get_dfo_id()),
        K(child.get_sqcs_count()));
    ObPxPartChMapArray& map = child.get_part_ch_map();
    common::ObSEArray<const ObPxSqcMeta*, 8> sqcs;
    ObPxChTotalInfos* dfo_ch_total_infos = &child.get_dfo_ch_total_infos();
    int64_t child_dfo_idx = -1;
    if (OB_FAIL(parent.get_sqcs(sqcs))) {
      LOG_WARN("failed to get sqcs", K(ret));
    } else if (sqcs.count() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the count of sqc is unexpected", K(ret), K(sqcs.count()));
    } else if (OB_FAIL(ObDfo::check_dfo_pair(parent, child, child_dfo_idx))) {
      LOG_WARN("failed to check dfo pair", K(ret));
    } else if (OB_FAIL(build_mn_channel(dfo_ch_total_infos, child, parent, tenant_id))) {
      LOG_WARN("failed to build mn channels", K(ret));
    } else if (OB_ISNULL(dfo_ch_total_infos) || 1 != dfo_ch_total_infos->count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: receive ch info is error", K(ret), KP(dfo_ch_total_infos));
    } else if (OB_FAIL(build_partition_map_by_sqcs(
                   sqcs, dfo_ch_total_infos->at(0).receive_exec_server_.prefix_task_counts_, map))) {
      LOG_WARN("failed to build channel map by sqc", K(ret));
    } else if (map.count() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the size of channel map is unexpected", K(ret), K(map.count()), K(parent.get_tasks()), K(sqcs));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected child dfo", K(ret), K(child.get_dist_method()));
  }
  return ret;
}

int ObSlaveMapUtil::build_ppwj_ch_mn_map(ObExecContext& ctx, ObDfo& parent, ObDfo& child, uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  // All dfo related partitions can be fetched in dfo
  // All relevant partition id can be calculated from it
  // Calculate the mapping relationship between id and channel according to partition
  // id using the determined hash algorithm

  // 1. Traverse all sqc in dfo, which contains partition id
  // 2. Use partition id as hash to calculate task_id
  // 3. Traverse tasks, find i, satisfy:
  // tasks[i].server = sqc.server && tasks[i].task_id = hash(partition_id)
  // 4. Record (partition_id, i) in the map
  ObArray<ObPxSqcMeta*> sqcs;
  ObPxPartChMapArray& map = child.get_part_ch_map();
  int64_t child_dfo_idx = -1;
  ObPxChTotalInfos* dfo_ch_total_infos = &child.get_dfo_ch_total_infos();
  if (OB_FAIL(parent.get_sqcs(sqcs))) {
    LOG_WARN("fail get dfo sqc", K(parent), K(ret));
  } else if (OB_FAIL(ObDfo::check_dfo_pair(parent, child, child_dfo_idx))) {
    LOG_WARN("failed to check dfo pair", K(ret));
  } else if (OB_FAIL(build_mn_channel(dfo_ch_total_infos, child, parent, tenant_id))) {
    LOG_WARN("failed to build mn channels", K(ret));
  } else if (OB_ISNULL(dfo_ch_total_infos) || 1 != dfo_ch_total_infos->count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: receive ch info is error", K(ret), KP(dfo_ch_total_infos));
  } else {
    share::schema::ObSchemaGetterGuard schema_guard;
    const share::schema::ObTableSchema* table_schema = NULL;
    common::ObIArray<int64_t>& prefix_task_counts = dfo_ch_total_infos->at(0).receive_exec_server_.prefix_task_counts_;
    if (prefix_task_counts.count() != sqcs.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: prefix task count is not match sqcs count",
          K(ret),
          KP(prefix_task_counts.count()),
          K(sqcs.count()));
    }
    ARRAY_FOREACH_X(sqcs, idx, cnt, OB_SUCC(ret))
    {
      ObPxAffinityByRandom affinitize_rule;
      ObPxSqcMeta& sqc = *sqcs.at(idx);
      ObPxPartitionInfo partition_row_info;
      const ObPartitionReplicaLocationIArray& locations = sqc.get_locations();
      ARRAY_FOREACH_X(locations, loc_idx, loc_cnt, OB_SUCC(ret))
      {
        const ObPartitionReplicaLocation& location = locations.at(loc_idx);
        if (NULL == table_schema) {
          uint64_t table_id = location.get_table_id();
          if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
                  ctx.get_my_session()->get_effective_tenant_id(), schema_guard))) {
            LOG_WARN("faile to get schema guard", K(ret));
          } else if (OB_FAIL(schema_guard.get_table_schema(table_id, table_schema))) {
            LOG_WARN("faile to get table schema", K(ret), K(table_id));
          } else if (OB_ISNULL(table_schema)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("table schema is null", K(ret), K(table_id));
          }
        }
        int64_t partition_idx = 0;  // TODO: fill it with location info
        bool check_dropped_partition = false;
        if (OB_FAIL(ret)) {
          // pass
        } else if (OB_FAIL(share::schema::ObPartMgrUtils::get_partition_idx_by_id(
                       *table_schema, check_dropped_partition, location.get_partition_id(), partition_idx))) {
          LOG_WARN("fail calc partition idx", K(location), K(ret));
        } else if (OB_FAIL(ObPxAffinityByRandom::get_partition_info(
                       location.get_partition_id(), sqc.get_partitions_info(), partition_row_info))) {
          LOG_WARN("Failed to get partition info", K(ret));
        } else if (OB_FAIL(affinitize_rule.add_partition(location.get_partition_id(),
                       partition_idx,
                       sqc.get_task_count(),
                       ctx.get_my_session()->get_effective_tenant_id(),
                       partition_row_info))) {
          LOG_WARN("fail calc task_id", K(partition_idx), K(sqc), K(ret));
        }
      }
      affinitize_rule.do_random(!sqc.get_partitions_info().empty());
      const ObIArray<ObPxAffinityByRandom::PartitionHashValue>& partition_worker_pairs = affinitize_rule.get_result();
      int64_t prefix_task_count = prefix_task_counts.at(idx);
      ARRAY_FOREACH(partition_worker_pairs, idx)
      {
        int64_t partition_idx = partition_worker_pairs.at(idx).partition_idx_;
        int64_t partition_id = partition_worker_pairs.at(idx).partition_id_;
        int64_t task_id = partition_worker_pairs.at(idx).worker_id_;
        OZ(map.push_back(ObPxPartChMapItem(partition_id, prefix_task_count, task_id)));
        LOG_DEBUG("debug push partition map", K(partition_id), K(partition_idx), K(task_id));
      }
      LOG_DEBUG("Get all partition rows info", K(ret), K(sqc.get_partitions_info()));
    }
  }
  return ret;
}

int ObSlaveMapUtil::build_mn_ch_map(ObExecContext& ctx, ObDfo& child, ObDfo& parent, uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  SlaveMappingType slave_type = parent.get_slave_mapping_type();
  switch (slave_type) {
    case SlaveMappingType::SM_PWJ_HASH_HASH: {
      if (OB_FAIL(build_pwj_slave_map_mn_group(parent, child, tenant_id))) {
        LOG_WARN("fail to build pwj slave map", K(ret));
      }
      LOG_DEBUG("partition wise join slave mapping", K(ret));
      break;
    }
    case SlaveMappingType::SM_PPWJ_BCAST_NONE: {
      if (OB_FAIL(build_ppwj_bcast_slave_mn_map(parent, child, tenant_id))) {
        LOG_WARN("fail to build pwj slave map", K(ret));
      }
      LOG_DEBUG("partial partition wise join slave mapping", K(ret));
      break;
    }
    case SlaveMappingType::SM_PPWJ_NONE_BCAST: {
      if (OB_FAIL(build_ppwj_bcast_slave_mn_map(parent, child, tenant_id))) {
        LOG_WARN("fail to build pwj slave map", K(ret));
      }
      LOG_DEBUG("partial partition wise join slave mapping", K(ret));
      break;
    }
    case SlaveMappingType::SM_PPWJ_HASH_HASH: {
      if (OB_FAIL(build_ppwj_slave_mn_map(parent, child, tenant_id))) {
        LOG_WARN("fail to build pwj slave map", K(ret));
      }
      LOG_DEBUG("partial partition wise join slave mapping", K(ret));
      break;
    }
    case SlaveMappingType::SM_NONE: {
      LOG_DEBUG("none slave mapping", K(ret));
      if (OB_SUCC(ret) && ObPQDistributeMethod::Type::PARTITION == child.get_dist_method()) {
        if (OB_FAIL(build_ppwj_ch_mn_map(ctx, parent, child, tenant_id))) {
          LOG_WARN("failed to build partial partition wise join channel map", K(ret));
        } else {
          LOG_DEBUG("partial partition wise join build ch map successfully");
        }
      }
      if (OB_SUCC(ret) && ObPQDistributeMethod::Type::PARTITION_HASH == child.get_dist_method()) {
        if (OB_FAIL(build_pkey_affinitized_ch_mn_map(parent, child, tenant_id))) {
          LOG_WARN("failed to build pkey random channel map", K(ret));
        } else {
          LOG_DEBUG("partition random build ch map successfully");
        }
      }
      if (OB_SUCC(ret) && ObPQDistributeMethod::Type::PARTITION_RANDOM == child.get_dist_method()) {
        if (OB_FAIL(build_pkey_random_ch_mn_map(parent, child, tenant_id))) {
          LOG_WARN("failed to build pkey random channel map", K(ret));
        } else {
          LOG_DEBUG("partition random build ch map successfully");
        }
      }
      break;
    }
  }
  LOG_DEBUG("debug distribute type", K(slave_type), K(child.get_dist_method()));
  return ret;
}

int ObDtlChannelUtil::get_receive_dtl_channel_set(
    const int64_t sqc_id, const int64_t task_id, ObDtlChTotalInfo& ch_total_info, ObDtlChSet& ch_set)
{
  int ret = OB_SUCCESS;
  // receive
  int64_t ch_cnt = 0;
  if (0 > sqc_id || sqc_id >= ch_total_info.receive_exec_server_.prefix_task_counts_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid sqc id", K(sqc_id), K(ret), K(ch_total_info.receive_exec_server_.prefix_task_counts_.count()));
  } else if (ch_total_info.transmit_exec_server_.prefix_task_counts_.count() !=
             ch_total_info.transmit_exec_server_.exec_addrs_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: prefix task count is not match with execute address",
        K(ret),
        "prefix task count",
        ch_total_info.transmit_exec_server_.prefix_task_counts_.count(),
        "execute address count",
        ch_total_info.transmit_exec_server_.exec_addrs_.count());
  } else {
    int64_t start_task_id = ch_total_info.receive_exec_server_.prefix_task_counts_.at(sqc_id);
    int64_t receive_task_cnt = ch_total_info.receive_exec_server_.total_task_cnt_;
    int64_t base_chid = ch_total_info.start_channel_id_ + (start_task_id + task_id);
    int64_t chid = 0;
    ObIArray<int64_t>& prefix_task_counts = ch_total_info.transmit_exec_server_.prefix_task_counts_;
    int64_t pre_prefix_task_count = 0;
    for (int64_t i = 0; i < prefix_task_counts.count() && OB_SUCC(ret); ++i) {
      int64_t prefix_task_count = 0;
      if (i + 1 == prefix_task_counts.count()) {
        prefix_task_count = ch_total_info.transmit_exec_server_.total_task_cnt_;
      } else {
        prefix_task_count = prefix_task_counts.at(i + 1);
      }
      ObAddr& dst_addr = ch_total_info.transmit_exec_server_.exec_addrs_.at(i);
      bool is_local = dst_addr == GCONF.self_addr_;
      for (int64_t j = pre_prefix_task_count; j < prefix_task_count && OB_SUCC(ret); ++j) {
        ObDtlChannelInfo ch_info;
        chid = base_chid + receive_task_cnt * j;
        ObDtlChannelGroup::make_receive_channel(ch_total_info.tenant_id_, dst_addr, chid, ch_info, is_local);
        OZ(ch_set.add_channel_info(ch_info));
        LOG_DEBUG(
            "debug receive channel", KP(chid), K(ch_info), K(sqc_id), K(task_id), K(ch_total_info.start_channel_id_));
        ++ch_cnt;
      }
      pre_prefix_task_count = prefix_task_count;
    }
    if (ch_cnt != ch_total_info.transmit_exec_server_.total_task_cnt_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: channel count is not match",
          K(ch_cnt),
          K(ch_total_info.channel_count_),
          K(receive_task_cnt),
          K(prefix_task_counts),
          K(ch_total_info.transmit_exec_server_.total_task_cnt_),
          K(sqc_id),
          K(task_id));
    }
  }
  return ret;
}

int ObDtlChannelUtil::get_transmit_dtl_channel_set(
    const int64_t sqc_id, const int64_t task_id, ObDtlChTotalInfo& ch_total_info, ObDtlChSet& ch_set)
{
  int ret = OB_SUCCESS;
  if (0 > sqc_id || sqc_id >= ch_total_info.transmit_exec_server_.prefix_task_counts_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid sqc id", K(sqc_id), K(ret), K(ch_total_info.transmit_exec_server_.prefix_task_counts_.count()));
  } else {
    int64_t start_task_id = ch_total_info.transmit_exec_server_.prefix_task_counts_.at(sqc_id);
    int64_t base_chid = ch_total_info.start_channel_id_ +
                        (start_task_id + task_id) * ch_total_info.receive_exec_server_.total_task_cnt_;
    ObIArray<int64_t>& prefix_task_counts = ch_total_info.receive_exec_server_.prefix_task_counts_;
    int64_t ch_cnt = 0;
    int64_t pre_prefix_task_count = 0;
    int64_t chid = 0;
    for (int64_t i = 0; i < prefix_task_counts.count() && OB_SUCC(ret); ++i) {
      int64_t prefix_task_count = 0;
      if (i + 1 == prefix_task_counts.count()) {
        prefix_task_count = ch_total_info.receive_exec_server_.total_task_cnt_;
      } else {
        prefix_task_count = prefix_task_counts.at(i + 1);
      }
      ObAddr& dst_addr = ch_total_info.receive_exec_server_.exec_addrs_.at(i);
      bool is_local = dst_addr == GCONF.self_addr_;
      for (int64_t j = pre_prefix_task_count; j < prefix_task_count && OB_SUCC(ret); ++j) {
        ObDtlChannelInfo ch_info;
        chid = base_chid + j;
        ObDtlChannelGroup::make_transmit_channel(ch_total_info.tenant_id_, dst_addr, chid, ch_info, is_local);
        OZ(ch_set.add_channel_info(ch_info));
        ++ch_cnt;
        LOG_DEBUG(
            "debug transmit channel", KP(chid), K(ch_info), K(sqc_id), K(task_id), K(ch_total_info.start_channel_id_));
      }
      pre_prefix_task_count = prefix_task_count;
    }
    if (ch_cnt != ch_total_info.receive_exec_server_.total_task_cnt_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: channel count is not match",
          K(ch_cnt),
          K(ch_total_info.transmit_exec_server_.total_task_cnt_));
    }
  }
  return ret;
}

int ObDtlChannelUtil::link_ch_set(
    ObDtlChSet& ch_set, common::ObIArray<dtl::ObDtlChannel*>& channels, ObDtlFlowControl* dfc)
{
  int ret = OB_SUCCESS;
  dtl::ObDtlChannelInfo ci;
  for (int64_t idx = 0; idx < ch_set.count() && OB_SUCC(ret); ++idx) {
    dtl::ObDtlChannel* ch = NULL;
    if (OB_FAIL(ch_set.get_channel_info(idx, ci))) {
      LOG_WARN("fail get channel info", K(idx), K(ret));
    } else if (OB_FAIL(dtl::ObDtlChannelGroup::link_channel(ci, ch, dfc))) {
      LOG_WARN("fail link channel", K(ci), K(ret));
    } else if (OB_ISNULL(ch)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail add qc channel", K(ret));
    } else if (OB_FAIL(channels.push_back(ch))) {
      LOG_WARN("fail push back channel ptr", K(ci), K(ret));
    }
  }
  return ret;
}
