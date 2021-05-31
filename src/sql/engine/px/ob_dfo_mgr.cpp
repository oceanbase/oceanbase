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

#include "sql/engine/px/ob_dfo_mgr.h"
#include "sql/engine/px/ob_px_util.h"
#include "sql/executor/ob_receive.h"
#include "sql/executor/ob_transmit.h"
#include "sql/engine/basic/ob_temp_table_access.h"
#include "sql/engine/basic/ob_temp_table_insert.h"
#include "sql/engine/basic/ob_temp_table_access_op.h"
#include "sql/engine/basic/ob_temp_table_insert_op.h"
#include "sql/engine/px/exchange/ob_transmit_op.h"
#include "lib/utility/ob_tracepoint.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

int ObDfoTreeNormalizer::normalize(ObDfo& root)
{
  int ret = OB_SUCCESS;
  int64_t non_leaf_cnt = 0;
  int64_t non_leaf_pos = -1;
  ARRAY_FOREACH_X(root.child_dfos_, idx, cnt, OB_SUCC(ret))
  {
    ObDfo* dfo = root.child_dfos_.at(idx);
    if (0 < dfo->get_child_count()) {
      non_leaf_cnt++;
      if (-1 == non_leaf_pos) {
        non_leaf_pos = idx;
      }
    }
  }
  if (non_leaf_cnt > 1) {
  } else if (0 < non_leaf_pos) {
    /*
     * swap dfos to reorder schedule seq
     *
     * simplest case:
     *
     *      inode                 inode
     *      /   \       ===>      /   \
     *    leaf  inode           inode  leaf
     *
     * [*] inode means non-leaf node.
     *
     * complicated case:
     *
     * root node has 4 children, third child is middle and others are leaf nodes.
     *
     *      root  --------+-----+
     *      |      |      |     |
     *      leaf0  leaf1  inode leaf2
     *
     * dependence relationship: inode rely on leaf0 and leaf1,
     *  so expect to schedule leaf0 first,then schedule leaf1.
     *
     *  After convert:
     *
     *     root  --------+-----+
     *      |     |      |     |
     *      inode leaf0  leaf1 leaf2
     */
    ObDfo* inode = root.child_dfos_.at(non_leaf_pos);
    for (int64_t i = 1; i < non_leaf_pos; ++i) {
      root.child_dfos_.at(i - 1)->set_depend_sibling(root.child_dfos_.at(i));
    }
    inode->set_depend_sibling(root.child_dfos_.at(0));

    for (int64_t i = non_leaf_pos; i > 0; --i) {
      root.child_dfos_.at(i) = root.child_dfos_.at(i - 1);
    }
    root.child_dfos_.at(0) = inode;
  }
  if (OB_SUCC(ret)) {
    ARRAY_FOREACH_X(root.child_dfos_, idx, cnt, OB_SUCC(ret))
    {
      if (OB_ISNULL(root.child_dfos_.at(idx))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(idx), K(cnt), K(ret));
      } else if (OB_FAIL(normalize(*root.child_dfos_.at(idx)))) {
        LOG_WARN("fail normalize dfo", K(idx), K(cnt), K(ret));
      }
    }
  }
  return ret;
}

int ObDfoSchedOrderGenerator::generate_sched_order(ObDfoMgr& dfo_mgr)
{
  int ret = OB_SUCCESS;
  ObDfo* dfo_tree = dfo_mgr.get_root_dfo();
  if (OB_ISNULL(dfo_tree)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL unexpected", K(ret));
  } else if (OB_FAIL(ObDfoTreeNormalizer::normalize(*dfo_tree))) {
    LOG_WARN("fail normalize dfo tree", K(ret));
  } else if (OB_FAIL(do_generate_sched_order(dfo_mgr, *dfo_tree))) {
    LOG_WARN("fail generate dfo edges", K(ret));
  }
  return ret;
}

int ObDfoSchedOrderGenerator::do_generate_sched_order(ObDfoMgr& dfo_mgr, ObDfo& root)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < root.get_child_count(); ++i) {
    ObDfo* child = NULL;
    if (OB_FAIL(root.get_child_dfo(i, child))) {
      LOG_WARN("fail get child dfo", K(i), K(root), K(ret));
    } else if (OB_ISNULL(child)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(do_generate_sched_order(dfo_mgr, *child))) {
      LOG_WARN("fail do generate edge", K(*child), K(ret));
    } else if (OB_FAIL(dfo_mgr.add_dfo_edge(child))) {
      LOG_WARN("fail add edge to array", K(ret));
    }
  }
  return ret;
}

int ObDfoWorkerAssignment::assign_worker(
    ObDfoMgr& dfo_mgr, int64_t expected_worker_count, int64_t allocated_worker_count)
{
  int ret = OB_SUCCESS;

  // Algorithm:
  // 1. allocate worker to each dfo based on dop calculated by optimizer.
  // 2. ideal result: dop = num of worker.
  // 3. If there are not enough workers, each dfo gets fewer worker.
  // 4. In order to calcute the ratio, we need to find the max dop scheduled together. We calculate
  //   ratio with this max dop and available worker count, and this ensures all dfo can get enough workers.

  const ObIArray<ObDfo*>& dfos = dfo_mgr.get_all_dfos();

  double scale_rate = 1.0;
  if (allocated_worker_count < 0 || expected_worker_count <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should have at least one worker", K(allocated_worker_count), K(expected_worker_count), K(ret));
  } else if (0 <= allocated_worker_count && allocated_worker_count < expected_worker_count) {
    scale_rate = static_cast<double>(allocated_worker_count) / static_cast<double>(expected_worker_count);
  }
  ARRAY_FOREACH_X(dfos, idx, cnt, OB_SUCC(ret))
  {
    ObDfo* child = dfos.at(idx);
    int64_t val = std::max(1L, static_cast<int64_t>(static_cast<double>(child->get_dop()) * scale_rate));
    child->set_assigned_worker_count(val);
    if (child->is_single() && val > 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("local dfo do should not have more than 1", K(*child), K(val), K(ret));
    }
    LOG_TRACE("assign worker count to dfo",
        "dfo_id",
        child->get_dfo_id(),
        K(allocated_worker_count),
        K(expected_worker_count),
        "dop",
        child->get_dop(),
        K(scale_rate),
        K(val));
  }

  int64_t total_assigned = 0;
  ARRAY_FOREACH_X(dfos, idx, cnt, OB_SUCC(ret))
  {
    const ObDfo* child = dfos.at(idx);
    const ObDfo* parent = child->parent();
    if (OB_ISNULL(parent)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dfo edges expect to have parent", K(*child), K(ret));
    } else {
      int64_t assigned = parent->get_assigned_worker_count() + child->get_assigned_worker_count();
      int64_t max_depend_sibling_assigned_worker = 0;
      while (child->has_depend_sibling()) {
        child = child->depend_sibling();
        if (max_depend_sibling_assigned_worker < child->get_assigned_worker_count()) {
          max_depend_sibling_assigned_worker = child->get_assigned_worker_count();
        }
      }
      assigned += max_depend_sibling_assigned_worker;
      if (assigned > total_assigned) {
        total_assigned = assigned;
      }
    }
  }
  if (OB_SUCC(ret) && total_assigned > allocated_worker_count && allocated_worker_count != 0) {
    ret = OB_ERR_SCHEDULER_THREAD_NOT_ENOUGH;
    LOG_WARN(
        "fail assign worker to dfos", K(total_assigned), K(allocated_worker_count), K(expected_worker_count), K(ret));
  }
  return ret;
}

void ObDfoMgr::destroy()
{
  // release all dfos
  for (int64_t i = 0; i < edges_.count(); ++i) {
    ObDfo* dfo = edges_.at(i);
    ObDfo::reset_resource(dfo);
  }
  edges_.reset();
  // release root dfo
  ObDfo::reset_resource(root_dfo_);
  root_dfo_ = nullptr;
  inited_ = false;
}

int ObDfoMgr::init(ObExecContext& exec_ctx, const ObPhyOperator& root_op, int64_t expected_worker_count,
    int64_t allocated_worker_count, const ObDfoInterruptIdGen& dfo_int_gen)
{
  int ret = OB_SUCCESS;
  root_dfo_ = NULL;
  ObDfo* rpc_dfo = nullptr;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("dfo mgr init twice", K(ret));
  } else if (OB_FAIL(do_split(exec_ctx, allocator_, &root_op, root_dfo_, dfo_int_gen))) {
    LOG_WARN("fail split ops into dfo", K(ret));
  } else if (OB_ISNULL(root_dfo_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL dfo unexpected", K(ret));
  } else if (OB_FAIL(ObDfoSchedOrderGenerator::generate_sched_order(*this))) {
    LOG_WARN("fail init dfo mgr", K(ret));
  } else if (OB_FAIL(ObDfoWorkerAssignment::assign_worker(*this, expected_worker_count, allocated_worker_count))) {
    LOG_WARN("fail assign worker to dfos", K(allocated_worker_count), K(expected_worker_count), K(ret));
  } else {
    if (1 == edges_.count() && OB_NOT_NULL(rpc_dfo = edges_.at(0))) {
      rpc_dfo->set_rpc_worker(1 == rpc_dfo->get_dop());
    }
    inited_ = true;
  }
  return ret;
}

// parent_dfo is out parameter only if first op is coord, otherwise it's a input parameter.
int ObDfoMgr::do_split(ObExecContext& exec_ctx, ObIAllocator& allocator, const ObPhyOperator* phy_op,
    ObDfo*& parent_dfo, const ObDfoInterruptIdGen& dfo_int_gen) const
{
  int ret = OB_SUCCESS;
  bool top_px = (nullptr == parent_dfo);
  bool got_fulltree_dfo = false;
  ObDfo* dfo = NULL;
  bool is_stack_overflow = false;
  if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("stack overflow, maybe too deep recursive", K(ret));
  } else if (OB_ISNULL(phy_op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL unexpected", K(ret));
  } else if (NULL == parent_dfo && !IS_PX_COORD(phy_op->get_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the first phy_op must be a coord op", K(ret));
  } else if (phy_op->is_table_scan() && NULL != parent_dfo) {
    parent_dfo->set_scan(true);
  } else if (phy_op->is_dml_operator() && NULL != parent_dfo) {
    parent_dfo->set_dml_op(true);
  } else if (phy_op->get_type() == PHY_TEMP_TABLE_ACCESS && NULL != parent_dfo) {
    parent_dfo->set_temp_table_scan(true);
    const ObTempTableAccess* access = static_cast<const ObTempTableAccess*>(phy_op);
    parent_dfo->set_temp_table_id(access->get_table_id());
  } else if (phy_op->get_type() == PHY_TEMP_TABLE_INSERT && NULL != parent_dfo) {
    parent_dfo->set_temp_table_insert(true);
    const ObTempTableInsert* insert = static_cast<const ObTempTableInsert*>(phy_op);
    parent_dfo->set_temp_table_id(insert->get_table_id());
  } else if (phy_op->get_type() == PHY_EXPR_VALUES && NULL != parent_dfo) {
    parent_dfo->set_has_expr_values(true);
  } else if (IS_PX_COORD(phy_op->get_type())) {
    if (top_px) {
      if (OB_FAIL(create_dfo(allocator, phy_op, dfo))) {
        LOG_WARN("fail create dfo", K(ret));
      }
    } else {
      got_fulltree_dfo = true;
    }
  } else if (IS_PX_TRANSMIT(phy_op->get_type())) {
    if (OB_FAIL(create_dfo(allocator, phy_op, dfo))) {
      LOG_WARN("fail create dfo", K(ret));
    } else {
      dfo->set_parent(parent_dfo);
      if (NULL != parent_dfo) {
        if (OB_FAIL(parent_dfo->append_child_dfo(dfo))) {
          LOG_WARN("fail append child dfo", K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret) && nullptr != dfo) {
    if (IS_PX_COORD(phy_op->get_type())) {
      dfo->set_root_dfo(true);
      dfo->set_single(true);
      dfo->set_dop(1);
      dfo->set_execution_id(exec_ctx.get_my_session()->get_current_execution_id());
      dfo->set_px_sequence_id(dfo_int_gen.get_px_sequence_id());
      if (OB_INVALID_ID == dfo->get_dfo_id()) {
        if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_2272) {
          dfo->set_dfo_id(128);
        } else {
          dfo->set_dfo_id(ObDfo::MAX_DFO_ID);
        }
      }
      if (OB_INVALID_ID == dfo->get_qc_id()) {
        const ObTransmit* transmit =
            static_cast<const ObTransmit*>((static_cast<const ObReceive*>(phy_op))->get_child(0));
        if (OB_INVALID_ID != transmit->get_px_id()) {
          dfo->set_qc_id(transmit->get_px_id());
        }
      }
      // for root dfo, it's not a real dfo so it's no allocated an id.
      if (OB_FAIL(dfo_int_gen.gen_id(dfo->get_dfo_id(), dfo->get_interrupt_id()))) {
        LOG_WARN("fail gen dfo int id", K(ret));
      }
      LOG_TRACE("cur dfo info", K(dfo->get_qc_id()), K(dfo->get_dfo_id()), K(dfo->get_dop()));
    } else {
      const ObTransmit* transmit = static_cast<const ObTransmit*>(phy_op);
      dfo->set_single(transmit->is_px_single());
      dfo->set_dop(transmit->get_px_dop());
      dfo->set_qc_id(transmit->get_px_id());
      dfo->set_dfo_id(transmit->get_dfo_id());
      dfo->set_execution_id(exec_ctx.get_my_session()->get_current_execution_id());
      dfo->set_px_sequence_id(dfo_int_gen.get_px_sequence_id());
      dfo->set_dist_method(transmit->get_dist_method());
      dfo->set_slave_mapping_type(transmit->get_slave_mapping_type());
      parent_dfo->set_slave_mapping_type(transmit->get_slave_mapping_type());
      if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_2272 && dfo->get_dfo_id() >= 128) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("dfo id is not invalid in upgrade mode", K(ret));
      } else if (OB_ISNULL(parent_dfo)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("parent dfo should not be null", K(ret));
      } else if (transmit->get_px_dop() <= 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("should have dop set by optimizer", K(ret), K(transmit->get_px_dop()));
      } else if (OB_FAIL(dfo_int_gen.gen_id(transmit->get_dfo_id(), dfo->get_interrupt_id()))) {
        LOG_WARN("fail gen dfo int id", K(ret));
      } else {
        dfo->set_qc_server_id(GCTX.server_id_);
        dfo->set_parent_dfo_id(parent_dfo->get_dfo_id());
        LOG_TRACE("cur dfo dop",
            "dfo_id",
            dfo->get_dfo_id(),
            "is_single",
            transmit->is_px_single(),
            "dop",
            transmit->get_px_dop(),
            K(dfo->get_qc_id()),
            "parent dfo_id",
            parent_dfo->get_dfo_id(),
            "slave mapping",
            transmit->is_slave_mapping());
      }
    }
  }

  if (nullptr != dfo) {
    parent_dfo = dfo;
  }

  if (OB_SUCC(ret)) {
    if (got_fulltree_dfo) {
      if (OB_ISNULL(parent_dfo)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("inner px coord op should be in a dfo", K(ret));
      } else {
        parent_dfo->set_fulltree(true);
        parent_dfo->set_single(true);
        parent_dfo->set_dop(1);
      }
      // we have reach to a inner qc operator,
      // no more monkeys jumping on the bed!
    } else {
      for (int32_t i = 0; OB_SUCC(ret) && i < phy_op->get_child_num(); ++i) {
        ObDfo* tmp_parent_dfo = parent_dfo;
        if (OB_FAIL(do_split(exec_ctx, allocator, phy_op->get_child(i), tmp_parent_dfo, dfo_int_gen))) {
          LOG_WARN("fail split op into dfo", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObDfoMgr::create_dfo(ObIAllocator& allocator, const ObPhyOperator* dfo_root_op, ObDfo*& dfo) const
{
  int ret = OB_SUCCESS;
  void* tmp = NULL;
  dfo = NULL;
  if (OB_ISNULL(dfo_root_op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL unexpected", K(ret));
  } else if (OB_ISNULL(tmp = allocator.alloc(sizeof(ObDfo)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("fail to alloc ObDfo", K(ret));
  } else if (OB_ISNULL(dfo = new (tmp) ObDfo(allocator))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to new ObDfo", K(ret));
  } else {
    dfo->set_root_op(dfo_root_op);
    dfo->set_phy_plan(dfo_root_op->get_phy_plan());
  }
  return ret;
}

int ObDfoMgr::init(ObExecContext& exec_ctx, const ObOpSpec& root_op_spec, int64_t expected_worker_count,
    int64_t allocated_worker_count, const ObDfoInterruptIdGen& dfo_int_gen)
{
  int ret = OB_SUCCESS;
  root_dfo_ = NULL;
  ObDfo* rpc_dfo = nullptr;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("dfo mgr init twice", K(ret));
  } else if (OB_FAIL(do_split(exec_ctx, allocator_, &root_op_spec, root_dfo_, dfo_int_gen))) {
    LOG_WARN("fail split ops into dfo", K(ret));
  } else if (OB_ISNULL(root_dfo_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL dfo unexpected", K(ret));
  } else if (OB_FAIL(ObDfoSchedOrderGenerator::generate_sched_order(*this))) {
    LOG_WARN("fail init dfo mgr", K(ret));
  } else if (OB_FAIL(ObDfoWorkerAssignment::assign_worker(*this, expected_worker_count, allocated_worker_count))) {
    LOG_WARN("fail assign worker to dfos", K(allocated_worker_count), K(expected_worker_count), K(ret));
  } else {
    if (1 == edges_.count() && OB_NOT_NULL(rpc_dfo = edges_.at(0))) {
      rpc_dfo->set_rpc_worker(1 == rpc_dfo->get_dop());
    }
    inited_ = true;
  }
  return ret;
}

// parent_dfo is out parameter only if first op is coord, otherwise it's a input parameter.
int ObDfoMgr::do_split(ObExecContext& exec_ctx, ObIAllocator& allocator, const ObOpSpec* phy_op, ObDfo*& parent_dfo,
    const ObDfoInterruptIdGen& dfo_int_gen) const
{
  int ret = OB_SUCCESS;
  bool top_px = (nullptr == parent_dfo);
  bool got_fulltree_dfo = false;
  ObDfo* dfo = NULL;
  bool is_stack_overflow = false;
  if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("stack overflow, maybe too deep recursive", K(ret));
  } else if (OB_ISNULL(phy_op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL unexpected", K(ret));
  } else if (NULL == parent_dfo && !IS_PX_COORD(phy_op->type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the first phy_op must be a coord op", K(ret));
  } else if (phy_op->is_table_scan() && NULL != parent_dfo) {
    parent_dfo->set_scan(true);
  } else if (phy_op->is_dml_operator() && NULL != parent_dfo) {
    parent_dfo->set_dml_op(true);
  } else if (phy_op->get_type() == PHY_TEMP_TABLE_ACCESS && NULL != parent_dfo) {
    parent_dfo->set_temp_table_scan(true);
    const ObTempTableAccessOpSpec* access = static_cast<const ObTempTableAccessOpSpec*>(phy_op);
    parent_dfo->set_temp_table_id(access->get_table_id());
  } else if (phy_op->get_type() == PHY_TEMP_TABLE_INSERT && NULL != parent_dfo) {
    parent_dfo->set_temp_table_insert(true);
    const ObTempTableInsertOpSpec* insert = static_cast<const ObTempTableInsertOpSpec*>(phy_op);
    parent_dfo->set_temp_table_id(insert->get_table_id());
  } else if (phy_op->get_type() == PHY_EXPR_VALUES && NULL != parent_dfo) {
    parent_dfo->set_has_expr_values(true);
  } else if (IS_PX_COORD(phy_op->type_)) {
    if (top_px) {
      if (OB_FAIL(create_dfo(allocator, phy_op, dfo))) {
        LOG_WARN("fail create dfo", K(ret));
      }
    } else {
      got_fulltree_dfo = true;
    }
  } else if (IS_PX_TRANSMIT(phy_op->type_)) {
    if (OB_FAIL(create_dfo(allocator, phy_op, dfo))) {
      LOG_WARN("fail create dfo", K(ret));
    } else {
      dfo->set_parent(parent_dfo);
      if (NULL != parent_dfo) {
        if (OB_FAIL(parent_dfo->append_child_dfo(dfo))) {
          LOG_WARN("fail append child dfo", K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret) && nullptr != dfo) {
    if (IS_PX_COORD(phy_op->type_)) {
      dfo->set_root_dfo(true);
      dfo->set_single(true);
      dfo->set_dop(1);
      dfo->set_execution_id(exec_ctx.get_my_session()->get_current_execution_id());
      dfo->set_px_sequence_id(dfo_int_gen.get_px_sequence_id());
      if (OB_INVALID_ID == dfo->get_dfo_id()) {
        if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_2272) {
          dfo->set_dfo_id(128);
        } else {
          dfo->set_dfo_id(ObDfo::MAX_DFO_ID);
        }
      }
      if (OB_INVALID_ID == dfo->get_qc_id()) {
        const ObTransmitSpec* transmit = static_cast<const ObTransmitSpec*>(phy_op->get_child());
        if (OB_INVALID_ID != transmit->get_px_id()) {
          dfo->set_qc_id(transmit->get_px_id());
        }
      }
      // for root dfo, it's not a real dfo so it's no allocated an id.
      if (OB_FAIL(dfo_int_gen.gen_id(dfo->get_dfo_id(), dfo->get_interrupt_id()))) {
        LOG_WARN("fail gen dfo int id", K(ret));
      }
      LOG_TRACE("cur dfo info", K(dfo->get_qc_id()), K(dfo->get_dfo_id()), K(dfo->get_dop()));
    } else {
      const ObTransmitSpec* transmit = static_cast<const ObTransmitSpec*>(phy_op);
      dfo->set_single(transmit->is_px_single());
      dfo->set_dop(transmit->get_px_dop());
      dfo->set_qc_id(transmit->get_px_id());
      dfo->set_dfo_id(transmit->get_dfo_id());
      dfo->set_execution_id(exec_ctx.get_my_session()->get_current_execution_id());
      dfo->set_px_sequence_id(dfo_int_gen.get_px_sequence_id());
      dfo->set_dist_method(transmit->dist_method_);
      dfo->set_slave_mapping_type(transmit->get_slave_mapping_type());
      parent_dfo->set_slave_mapping_type(transmit->get_slave_mapping_type());
      if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_2272 && dfo->get_dfo_id() >= 128) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("dfo id is not invalid in upgrade mode", K(ret));
      } else if (OB_ISNULL(parent_dfo)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("parent dfo should not be null", K(ret));
      } else if (transmit->get_px_dop() <= 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("should have dop set by optimizer", K(ret), K(transmit->get_px_dop()));
      } else if (OB_FAIL(dfo_int_gen.gen_id(transmit->get_dfo_id(), dfo->get_interrupt_id()))) {
        LOG_WARN("fail gen dfo int id", K(ret));
      } else {
        dfo->set_qc_server_id(GCTX.server_id_);
        dfo->set_parent_dfo_id(parent_dfo->get_dfo_id());
        LOG_TRACE("cur dfo dop",
            "dfo_id",
            dfo->get_dfo_id(),
            "is_local",
            transmit->is_px_single(),
            "dop",
            transmit->get_px_dop(),
            K(dfo->get_qc_id()),
            "parent dfo_id",
            parent_dfo->get_dfo_id(),
            "slave mapping",
            transmit->is_slave_mapping());
      }
    }
  }

  if (nullptr != dfo) {
    parent_dfo = dfo;
  }

  if (OB_SUCC(ret)) {
    if (got_fulltree_dfo) {
      if (OB_ISNULL(parent_dfo)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("inner px coord op should be in a dfo", K(ret));
      } else {
        parent_dfo->set_fulltree(true);
        parent_dfo->set_single(true);
        parent_dfo->set_dop(1);
      }
      // we have reach to a inner qc operator,
      // no more monkeys jumping on the bed!
    } else {
      for (int32_t i = 0; OB_SUCC(ret) && i < phy_op->get_child_cnt(); ++i) {
        ObDfo* tmp_parent_dfo = parent_dfo;
        if (OB_FAIL(do_split(exec_ctx, allocator, phy_op->get_child(i), tmp_parent_dfo, dfo_int_gen))) {
          LOG_WARN("fail split op into dfo", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObDfoMgr::create_dfo(ObIAllocator& allocator, const ObOpSpec* dfo_root_op, ObDfo*& dfo) const
{
  int ret = OB_SUCCESS;
  void* tmp = NULL;
  dfo = NULL;
  if (OB_ISNULL(dfo_root_op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL unexpected", K(ret));
  } else if (OB_ISNULL(tmp = allocator.alloc(sizeof(ObDfo)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("fail to alloc ObDfo", K(ret));
  } else if (OB_ISNULL(dfo = new (tmp) ObDfo(allocator))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to new ObDfo", K(ret));
  } else {
    dfo->set_root_op_spec(dfo_root_op);
    dfo->set_phy_plan(dfo_root_op->get_phy_plan());
  }
  return ret;
}

// get_ready_dfo is only used in single dfo schedule.
int ObDfoMgr::get_ready_dfo(ObDfo*& dfo) const
{
  int ret = OB_SUCCESS;
  bool all_finish = true;
  dfo = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < edges_.count(); ++i) {
    ObDfo* edge = edges_.at(i);
    if (edge->is_thread_finish()) {
      continue;
    } else {
      all_finish = false;
      if (!edge->is_active()) {
        dfo = edge;
        dfo->set_active();
        break;
      }
    }
  }
  if (OB_SUCC(ret) && all_finish) {
    ret = OB_ITER_END;
  }
  return ret;
}

// - if some edge not finish, and can't schedule more dfo, return empty set.
// - if all edge have finished, return ITER_END
int ObDfoMgr::get_ready_dfos(ObIArray<ObDfo*>& dfos) const
{
  int ret = OB_SUCCESS;
  bool all_finish = true;
  bool got_pair_dfo = false;
  dfos.reset();

  LOG_TRACE("ready dfos", K(edges_.count()));
  // edges have been ordered according to priority.
  for (int64_t i = 0; OB_SUCC(ret) && i < edges_.count(); ++i) {
    ObDfo* edge = edges_.at(i);
    ObDfo* root_edge = edges_.at(edges_.count() - 1);
    if (edge->is_thread_finish()) {
      LOG_TRACE("finish dfo", K(*edge));
      continue;
    } else {
      // edge not finished, target of schedule is to make this edge finish quickly,
      // including schedule DFO it depends on.
      all_finish = false;
      if (!edge->is_active()) {
        if (OB_FAIL(dfos.push_back(edge))) {
          LOG_WARN("fail push dfo", K(ret));
        } else if (NULL == edge->parent()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("parent is NULL, unexpected", K(ret));
        } else if (OB_FAIL(dfos.push_back(edge->parent()))) {
          LOG_WARN("fail push dfo", K(ret));
        } else {
          edge->set_active();
          got_pair_dfo = true;
        }
      } else if (edge->has_depend_sibling()) {
        ObDfo* sibling_edge = edge->depend_sibling();
        for (/* nop */; nullptr != sibling_edge && sibling_edge->is_thread_finish();
             sibling_edge = sibling_edge->depend_sibling()) {
          // search forward: [leaf] --> [leaf] --> [leaf]
        }
        if (OB_UNLIKELY(nullptr == sibling_edge)) {
          // nop, all sibling finish
        } else if (sibling_edge->is_active()) {
          // nop, wait for a sibling finish.
          // after then can we shedule next edge
        } else if (OB_FAIL(dfos.push_back(sibling_edge))) {
          LOG_WARN("fail push dfo", K(ret));
        } else if (NULL == sibling_edge->parent()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("parent is NULL, unexpected", K(ret));
        } else if (OB_FAIL(dfos.push_back(sibling_edge->parent()))) {
          LOG_WARN("fail push dfo", K(ret));
        } else {
          sibling_edge->set_active();
          got_pair_dfo = true;
          LOG_TRACE("start schedule dfo", K(*sibling_edge), K(*sibling_edge->parent()));
        }
      } else {
      }
      // If one of root_edge's child has scheduled, we try to start the root_dfo.
      if (!got_pair_dfo) {
        if (edge->is_active() && !root_edge->is_active() && edge->has_parent() && edge->parent() == root_edge) {
          if (OB_ISNULL(root_edge->parent()) || root_edge->parent() != root_dfo_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("The root edge is null or it's parent not root dfo", K(ret));
          } else if (OB_FAIL(dfos.push_back(root_edge))) {
            LOG_WARN("Fail to push dfo", K(ret));
          } else if (OB_FAIL(dfos.push_back(root_dfo_))) {
            LOG_WARN("Fail to push dfo", K(ret));
          } else {
            root_edge->set_active();
            LOG_TRACE("Try to schedule root dfo", KP(root_edge), KP(root_dfo_));
          }
        }
      }
      break;
    }
  }
  if (all_finish && OB_SUCCESS == ret) {
    ret = OB_ITER_END;
  }
  return ret;
}

int ObDfoMgr::add_dfo_edge(ObDfo* edge)
{
  int ret = OB_SUCCESS;
  if (edges_.count() >= ObDfo::MAX_DFO_ID) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "plan with more than 128 DFOs");
  } else if (OB_FAIL(edges_.push_back(edge))) {
    LOG_WARN("fail to push back dfo", K(*edge), K(ret));
    // release the memory
    ObDfo::reset_resource(edge);
    edge = nullptr;
  }
  return ret;
}

int ObDfoMgr::find_dfo_edge(int64_t id, ObDfo*& edge)
{
  int ret = OB_SUCCESS;
  if (id < 0 || id >= ObDfo::MAX_DFO_ID || id >= edges_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid dfo id", K(id), K(edges_.count()), K(ret));
  } else {
    bool found = false;
    int64_t cnt = edges_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < cnt; ++i) {
      if (id == edges_.at(i)->get_dfo_id()) {
        edge = edges_.at(i);
        found = true;
        break;
      }
    }
    if (!found) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("not found dfo", K(id), K(cnt), K(ret));
    }
  }
  return ret;
}

int ObDfoMgr::get_active_dfos(ObIArray<ObDfo*>& dfos) const
{
  int ret = OB_SUCCESS;
  dfos.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < edges_.count(); ++i) {
    ObDfo* edge = edges_.at(i);
    if (edge->is_active()) {
      if (OB_FAIL(dfos.push_back(edge))) {
        LOG_WARN("fail push back edge", K(ret));
      }
    }
  }
  return ret;
}

int ObDfoMgr::get_scheduled_dfos(ObIArray<ObDfo*>& dfos) const
{
  int ret = OB_SUCCESS;
  dfos.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < edges_.count(); ++i) {
    ObDfo* edge = edges_.at(i);
    // dfo is set scheduled after calling schedule_dfo, no matter success or fail.
    if (edge->is_scheduled()) {
      if (OB_FAIL(dfos.push_back(edge))) {
        LOG_WARN("fail push back edge", K(ret));
      }
    }
  }
  return ret;
}

int ObDfoMgr::get_running_dfos(ObIArray<ObDfo*>& dfos) const
{
  int ret = OB_SUCCESS;
  dfos.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < edges_.count(); ++i) {
    ObDfo* edge = edges_.at(i);
    // dfo is set scheduled after calling schedule_dfo, no matter success or fail.
    if (edge->is_scheduled() && !edge->is_thread_finish()) {
      if (OB_FAIL(dfos.push_back(edge))) {
        LOG_WARN("fail push back edge", K(ret));
      }
    }
  }
  return ret;
}
