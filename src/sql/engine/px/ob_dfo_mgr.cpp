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
#include "sql/engine/basic/ob_temp_table_access_op.h"
#include "sql/engine/basic/ob_temp_table_insert_op.h"
#include "sql/engine/px/exchange/ob_transmit_op.h"
#include "sql/engine/basic/ob_material_op.h"
#include "lib/utility/ob_tracepoint.h"
#include "sql/engine/join/ob_join_filter_op.h"
#include "sql/engine/px/exchange/ob_px_repart_transmit_op.h"
#include "sql/optimizer/ob_px_resource_analyzer.h"
#include "sql/engine/px/ob_px_scheduler.h"
#include "share/detect/ob_detect_manager_utils.h"
#include "sql/engine/px/ob_px_coord_op.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

int ObDfoSchedOrderGenerator::generate_sched_order(ObDfoMgr &dfo_mgr)
{
   int ret = OB_SUCCESS;
   ObDfo *dfo_tree = dfo_mgr.get_root_dfo();
   if (OB_ISNULL(dfo_tree)) {
     ret = OB_ERR_UNEXPECTED;
     LOG_WARN("NULL unexpected", K(ret));
   } else if (OB_FAIL(DfoTreeNormalizer<ObDfo>::normalize(*dfo_tree))) {
     LOG_WARN("fail normalize dfo tree", K(ret));
   } else if (OB_FAIL(do_generate_sched_order(dfo_mgr, *dfo_tree))) {
     LOG_WARN("fail generate dfo edges", K(ret));
   }
   return ret;
}

// 正规化后的 dfo_tree 后序遍历顺序，即为调度顺序
// 用 edge 数组表示这种顺序
int ObDfoSchedOrderGenerator::do_generate_sched_order(ObDfoMgr &dfo_mgr, ObDfo &root)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < root.get_child_count(); ++i) {
    ObDfo *child = NULL;
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

int ObDfoSchedDepthGenerator::generate_sched_depth(ObExecContext &exec_ctx,
                                                   ObDfoMgr &dfo_mgr)
{
  int ret = OB_SUCCESS;
  if (GCONF._px_max_pipeline_depth > 2) {
    ObDfo *dfo_tree = dfo_mgr.get_root_dfo();
    if (OB_ISNULL(dfo_tree)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL unexpected", K(ret));
    } else if (OB_FAIL(do_generate_sched_depth(exec_ctx, dfo_mgr, *dfo_tree))) {
      LOG_WARN("fail generate dfo edges", K(ret));
    }
  }
  return ret;
}

// dfo_tree 后序遍历，定出哪些 dfo 可以做 material op bypass
int ObDfoSchedDepthGenerator::do_generate_sched_depth(ObExecContext &exec_ctx,
                                                      ObDfoMgr &dfo_mgr,
                                                      ObDfo &parent)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < parent.get_child_count(); ++i) {
    ObDfo *child = NULL;
    if (OB_FAIL(parent.get_child_dfo(i, child))) {
      LOG_WARN("fail get child dfo", K(i), K(parent), K(ret));
    } else if (OB_ISNULL(child)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(do_generate_sched_depth(exec_ctx, dfo_mgr, *child))) {
      LOG_WARN("fail do generate edge", K(*child), K(ret));
    } else {
      bool need_earlier_sched = check_if_need_do_earlier_sched(*child);
      if (need_earlier_sched) {
        // child 里面的 material 被改造成了 bypass 的，所以 parent 必须提前调度起来
        // 同时，parent 中如果也有 material，必须标记为 block，不可 bypass。否则会 hang。
        if (OB_FAIL(try_set_dfo_unblock(exec_ctx, *child))) {
          // 既然 parent 被提前调度了，那么 parent 千万要阻塞
          // 否则 parent 往外吐数据，就会卡住
          LOG_WARN("fail set dfo block", K(ret), K(*child), K(parent));
        } else if (OB_FAIL(try_set_dfo_block(exec_ctx, parent))) {
          // 既然 parent 被提前调度了，那么 parent 千万要阻塞
          // 否则 parent 往外吐数据，就会卡住
          LOG_WARN("fail set dfo block", K(ret), K(*child), K(parent));
        } else {
          parent.set_earlier_sched(true);
          LOG_DEBUG("parent dfo can do earlier scheduling", K(*child), K(parent));
        }
      }
    }
  }
  return ret;
}

bool ObDfoSchedDepthGenerator::check_if_need_do_earlier_sched(ObDfo &child)
{
  bool do_earlier_sched = false;
  if (child.is_earlier_sched() == false) {
    const ObOpSpec *phy_op = child.get_root_op_spec();
    if (OB_NOT_NULL(phy_op) && IS_PX_TRANSMIT(phy_op->type_)) {
      phy_op = static_cast<const ObTransmitSpec *>(phy_op)->get_child();
      do_earlier_sched = phy_op && PHY_MATERIAL == phy_op->type_;
    }
  } else {
    // dfo (child) 是 earlier sched，那么可以知道 dfo 的 material 会阻塞对外吐数据.
    // 此时 dfo 的 parent 没有必要提前调度，因为没有任何数据给它消费. parent 依靠
    // 稍后的 2-DFO 普通调度即可。
  }
  return do_earlier_sched;
}

int ObDfoSchedDepthGenerator::try_set_dfo_unblock(ObExecContext &exec_ctx, ObDfo &dfo)
{
  return try_set_dfo_block(exec_ctx, dfo, false/*unblock*/);
}

int ObDfoSchedDepthGenerator::try_set_dfo_block(ObExecContext &exec_ctx, ObDfo &dfo, bool block)
{
  int ret = OB_SUCCESS;
  const ObOpSpec *phy_op = dfo.get_root_op_spec();
  if (OB_ISNULL(phy_op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("phy_op is null", K(ret));
  } else {
    const ObTransmitSpec *transmit = static_cast<const ObTransmitSpec *>(phy_op);
    const ObOpSpec *child = transmit->get_child();
    if (OB_ISNULL(child)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("phy_op is null", K(ret));
    } else if (PHY_MATERIAL == child->type_) {
      const ObMaterialSpec *mat = static_cast<const ObMaterialSpec *>(child);
      ObOperatorKit *kit = exec_ctx.get_operator_kit(mat->id_);
      if (OB_ISNULL(kit) || OB_ISNULL(kit->input_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("operator is NULL", K(ret), KP(kit));
      } else {
        ObMaterialOpInput *mat_input = static_cast<ObMaterialOpInput *>(kit->input_);
        mat_input->set_bypass(!block); // so that this dfo will have a blocked material op
      }
    }
  }
  return ret;
}

int ObDfoWorkerAssignment::calc_admited_worker_count(const ObIArray<ObDfo*> &dfos,
                                                     ObExecContext &exec_ctx,
                                                     const ObOpSpec &root_op_spec,
                                                     int64_t &px_expected,
                                                     int64_t &px_minimal,
                                                     int64_t &px_admited)
{
  int ret = OB_SUCCESS;
  px_admited = 0;
  const ObTaskExecutorCtx *task_exec_ctx = NULL;
  if (OB_ISNULL(task_exec_ctx = GET_TASK_EXECUTOR_CTX(exec_ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task exec ctx NULL", K(ret));
  } else if (OB_FAIL(ObDfoWorkerAssignment::get_dfos_worker_count(dfos, true, px_minimal))) {
    LOG_WARN("failed to get dfos worker count", K(ret));
  } else {
    // px 级, 表示 optimizer 计算的数量，当前 px 理论上需要多少线程
    px_expected = static_cast<const ObPxCoordSpec*>(&root_op_spec)->get_expected_worker_count();
    // query 级, 表示 optimizer 计算的数量
    const int64_t query_expected = task_exec_ctx->get_expected_worker_cnt();
    // query 级, 表示调度需要最小数量
    const int64_t query_minimal = task_exec_ctx->get_minimal_worker_cnt();
    // query 级, 表示 admission 实际分配的数量
    const int64_t query_admited = task_exec_ctx->get_admited_worker_cnt();
    if (query_expected > 0 && 0 >= query_admited) {
      ret = OB_ERR_INSUFFICIENT_PX_WORKER;
      LOG_WARN("not enough thread resource", K(ret), K(px_expected), K(query_admited), K(query_expected));
    } else if (0 == query_expected) {
      // note: 对于单表、dop=1的查询，会走 fast dfo，此时 query_expected = 0
      px_admited = 0;
    } else if (query_admited >= query_expected) {
      px_admited = px_expected;
    } else if (OB_UNLIKELY(query_minimal <= 0)) {
      // compatible with version before 4.2
      px_admited = static_cast<int64_t>((double) query_admited * (double)px_expected / (double) query_expected);
    } else if (OB_UNLIKELY(query_admited < query_minimal || query_expected <= query_minimal)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected query admited worker count", K(ret), K(query_minimal), K(query_admited), K(query_expected));
    } else {
      const int64_t extra_worker = query_admited - query_minimal;
      const int64_t extra_expected = px_expected - px_minimal;
      px_admited = px_minimal + extra_expected * extra_worker / (query_expected - query_minimal);
    }
    LOG_TRACE("calc px worker count", K(query_expected), K(query_minimal), K(query_admited),
                            K(px_expected), K(px_minimal), K(px_admited));
  }
  return ret;
}

int ObDfoWorkerAssignment::assign_worker(ObDfoMgr &dfo_mgr,
                                         int64_t expected_worker_count,
                                         int64_t minimal_worker_count,
                                         int64_t admited_worker_count)
{
  int ret = OB_SUCCESS;
  /*  算法： */
  /*  1. 基于优化器给出的 dop，给每个 dfo 分配 worker */
  /*  2. 理想情况是 dop = worker 数 */
  /*  3. 但是，如果 worker 数不足，则需要降级。降级策略: 每个 dfo 等比少用 worker */
  /*  4. 为了确定比例，需要找到同时调度时占用 worker 数最多的一组 dop，以它为基准 */
  /*     计算比例，才能保证其余 dfo 都能获得足够 worker 数 */

  /*  算法可提升点（TODO）： */
  /*  考虑 expected_worker_count > admited_worker_count 场景， */
  /*  本算法中计算出 scale rate < 1 ，于是会导致每个 dfo 会做 dop 降级 */
  /*  而实际上，可以让部分 dfo 的执行不降级。考虑下面这种场景： */
  /*  */
  /*          dfo5 */
  /*         /   \ */
  /*       dfo1  dfo4 */
  /*               \ */
  /*                dfo3 */
  /*                 \ */
  /*                 dfo2 */
  /*  */
  /*  假设 dop = 5, 那么 expected_worker_count = 3 * 5 = 15 */
  /*  */
  /*  考虑线程不足场景，设 admited_worker_count = 10， */
  /*  那么，算法最优的情况下，我们可以这样分配： */
  /*  */
  /*          dfo5 (3 threads) */
  /*         /   \ */
  /*   (3)dfo1  dfo4 (4) */
  /*               \ */
  /*                dfo3 (5) */
  /*                 \ */
  /*                 dfo2 (5) */
  /*  */
  /*  当前的实现，由于 dop 等比降低，降低后的 dop = 5 * 10 / 15 = 3，实际分配结果为： */
  /*  */
  /*          dfo5 (3) */
  /*         /   \ */
  /*   (3)dfo1  dfo4 (3) */
  /*               \ */
  /*                dfo3 (3) */
  /*                 \ */
  /*                 dfo2 (3) */
  /*  */
  /*  显然，当前实现对 CPU 资源的利用不是最高效的。这部分工作可以留到稍后完善。暂时先这样 */

  const ObIArray<ObDfo *> & dfos = dfo_mgr.get_all_dfos();

  // 基于优化器给出的 dop，给每个 dfo 分配 worker
  // 实际分配的 worker 数一定不大于 dop，但可能小于 dop 给定值
  // admited_worker_count在rpc作为worker的场景下，值为0.
  double scale_rate = 1.0;
  bool match_expected = false;
  bool compatible_before_420 = false;
  if (OB_UNLIKELY(admited_worker_count < 0 || expected_worker_count <= 0 || minimal_worker_count <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should have at least one worker",  K(ret), K(admited_worker_count),
                                        K(expected_worker_count), K(minimal_worker_count));
  } else if (admited_worker_count >= expected_worker_count) {
    match_expected = true;
  } else if (minimal_worker_count <= 0) {
    // compatible with version before 4.2
    compatible_before_420 = true;
    scale_rate = static_cast<double>(admited_worker_count) / static_cast<double>(expected_worker_count);
  } else if (0 <= admited_worker_count || minimal_worker_count == admited_worker_count) {
    scale_rate = 0.0;
  } else if (OB_UNLIKELY(minimal_worker_count > admited_worker_count
                         || minimal_worker_count >= expected_worker_count)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected params", K(ret), K(minimal_worker_count), K(admited_worker_count), K(expected_worker_count));
  } else {
    scale_rate = static_cast<double>(admited_worker_count - minimal_worker_count)
                 / static_cast<double>(expected_worker_count - minimal_worker_count);
  }
  ARRAY_FOREACH_X(dfos, idx, cnt, OB_SUCC(ret)) {
    ObDfo *child = dfos.at(idx);
    int64_t val = 0;
    if (match_expected) {
      val = child->get_dop();
    } else if (compatible_before_420) {
      val = std::max(1L, static_cast<int64_t>(static_cast<double>(child->get_dop()) * scale_rate));
    } else {
      val = 1L + static_cast<int64_t>(std::max(static_cast<double>(child->get_dop() - 1), 0.0) * scale_rate);
    }
    child->set_assigned_worker_count(val);
    if (child->is_single() && val > 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("local dfo do should not have more than 1", K(*child), K(val), K(ret));
    }
    LOG_TRACE("assign worker count to dfo",
              "dfo_id", child->get_dfo_id(), K(admited_worker_count),
              K(expected_worker_count), "dop", child->get_dop(), K(scale_rate), K(val));
  }

  // 因为上面取了 max，所以可能实际 assigned 的会超出 admission 数，这时应该报错
  int64_t total_assigned = 0;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_dfos_worker_count(dfos, false, total_assigned))) {
    LOG_WARN("failed to get dfos worker count", K(ret));
  } else if (total_assigned > admited_worker_count && admited_worker_count != 0) {
    // 意味着某些 dfo 理论上一个线程都分不到
    ret = OB_ERR_PARALLEL_SERVERS_TARGET_NOT_ENOUGH;
    LOG_WARN("total assigned worker to dfos is more than admited_worker_count",
             K(total_assigned),
             K(admited_worker_count),
             K(minimal_worker_count),
             K(expected_worker_count),
             K(ret));
  }
  return ret;
}

int ObDfoWorkerAssignment::get_dfos_worker_count(const ObIArray<ObDfo*> &dfos,
                                                 const bool get_minimal,
                                                 int64_t &total_assigned)
{
  int ret = OB_SUCCESS;
  total_assigned = 0;
  ARRAY_FOREACH_X(dfos, idx, cnt, OB_SUCC(ret)) {
    const ObDfo *child  = dfos.at(idx);
    const ObDfo *parent = child->parent();
    // 计算当前 dfo 和“孩子们”一起调度时消耗的线程数
    // 找到 expected worker cnt 值最大的一组
    if (OB_ISNULL(parent) || OB_ISNULL(child)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dfo edges expect to have parent", KPC(parent), KPC(child), K(ret));
    } else {
      int64_t child_assigned = get_minimal ? 1 : child->get_assigned_worker_count();
      int64_t parent_assigned = get_minimal ? 1 : parent->get_assigned_worker_count();
      int64_t assigned = parent_assigned + child_assigned;
      // 局部右深树的场景，depend_sibling 和当前 child dfo 都会被调度
      /* Why need extra flag has_depend_sibling_? Why not use NULL != depend_sibling_?
       *               dfo5 (dop=2)
       *         /      |      \
       *      dfo1(2) dfo2 (4) dfo4 (1)
       *                        |
       *                      dfo3 (2)
       * Schedule order: (4,3) => (4,5) => (4,5,1) => (4,5,2) => (4,5) => (5). max_dop = (4,5,2) = 1 + 2 + 4 = 7.
       * Depend sibling: dfo4 -> dfo1 -> dfo2.
       * Depend sibling is stored as a list.
       * We thought dfo2 is depend sibling of dfo1, and calculated incorrect max_dop = (1,2,5) = 2 + 4 + 2 = 8.
       * Actually, dfo1 and dfo2 are depend sibling of dfo4, but dfo2 is not depend sibling of dfo1.
       * So we use has_depend_sibling_ record whether the dfo is the header of list.
      */
      int64_t max_depend_sibling_assigned_worker = 0;
      if (child->has_depend_sibling()) {
        while (NULL != child->depend_sibling()) {
          child = child->depend_sibling();
          child_assigned = get_minimal ? 1 : child->get_assigned_worker_count();
          if (max_depend_sibling_assigned_worker < child_assigned) {
            max_depend_sibling_assigned_worker = child_assigned;
          }
        }
      }
      assigned += max_depend_sibling_assigned_worker;
      if (assigned > total_assigned) {
        total_assigned = assigned;
        LOG_TRACE("update total assigned", K(idx), K(get_minimal), K(parent_assigned),
                K(child_assigned), K(max_depend_sibling_assigned_worker), K(total_assigned));
      }
    }
  }
  return ret;
}

void ObDfoMgr::destroy()
{
  // release all dfos
  for (int64_t i = 0; i < edges_.count(); ++i) {
    ObDfo *dfo = edges_.at(i);
    ObDfo::reset_resource(dfo);
  }
  edges_.reset();
  // release root dfo
  ObDfo::reset_resource(root_dfo_);
  root_dfo_ = nullptr;
  inited_ = false;
}

int ObDfoMgr::init(ObExecContext &exec_ctx,
                   const ObOpSpec &root_op_spec,
                   const ObDfoInterruptIdGen &dfo_int_gen,
                   ObPxCoordInfo &px_coord_info)
{
  int ret = OB_SUCCESS;
  root_dfo_ = NULL;
  ObDfo *rpc_dfo = nullptr;
  int64_t px_expected = 0;
  int64_t px_minimal = 0;
  int64_t px_admited = 0;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("dfo mgr init twice", K(ret));
  } else if (OB_FAIL(do_split(exec_ctx, allocator_, &root_op_spec, root_dfo_, dfo_int_gen, px_coord_info))) {
    LOG_WARN("fail split ops into dfo", K(ret));
  } else if (OB_ISNULL(root_dfo_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL dfo unexpected", K(ret));
  } else if (!px_coord_info.rf_dpd_info_.is_empty()
      && OB_FAIL(px_coord_info.rf_dpd_info_.describe_dependency(root_dfo_))) {
    LOG_WARN("failed to describe rf dependency");
  } else if (OB_FAIL(ObDfoSchedOrderGenerator::generate_sched_order(*this))) {
    LOG_WARN("fail init dfo mgr", K(ret));
  } else if (OB_FAIL(ObDfoSchedDepthGenerator::generate_sched_depth(exec_ctx, *this))) {
    LOG_WARN("fail init dfo mgr", K(ret));
  } else if (OB_FAIL(ObDfoWorkerAssignment::calc_admited_worker_count(get_all_dfos(),
                                                                      exec_ctx,
                                                                      root_op_spec,
                                                                      px_expected,
                                                                      px_minimal,
                                                                      px_admited))) {
    LOG_WARN("fail to calc admited worler count", K(ret));
  } else if (OB_FAIL(ObDfoWorkerAssignment::assign_worker(*this, px_expected, px_minimal, px_admited))) {
    LOG_WARN("fail assign worker to dfos", K(ret),  K(px_expected), K(px_minimal), K(px_admited));
  } else {
    inited_ = true;
  }
  return ret;
}

// parent_dfo 作为输入输出参数，仅仅在第一个 op 为 coord 时才作为输出参数，其余时候都作为输入参数
int ObDfoMgr::do_split(ObExecContext &exec_ctx,
                       ObIAllocator &allocator,
                       const ObOpSpec *phy_op,
                       ObDfo *&parent_dfo,
                       const ObDfoInterruptIdGen &dfo_int_gen,
                       ObPxCoordInfo &px_coord_info) const
{
  int ret = OB_SUCCESS;
  bool top_px = (nullptr == parent_dfo);
  bool got_fulltree_dfo = false;
  ObDfo *dfo = NULL;
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
    parent_dfo->inc_tsc_op_cnt();
    auto tsc_op = static_cast<const ObTableScanSpec *>(phy_op);
    if (TableAccessType::HAS_USER_TABLE == px_coord_info.table_access_type_){
      // nop
    } else if (!is_virtual_table(tsc_op->get_ref_table_id())) {
      px_coord_info.table_access_type_ = TableAccessType::HAS_USER_TABLE;
    } else {
      px_coord_info.table_access_type_ = TableAccessType::PURE_VIRTUAL_TABLE;
    }
    if (parent_dfo->need_p2p_info_ && parent_dfo->get_p2p_dh_addrs().empty()) {
      ObDASTableLoc *table_loc = nullptr;
       if (OB_ISNULL(table_loc = DAS_CTX(exec_ctx).get_table_loc_by_id(
            tsc_op->get_table_loc_id(), tsc_op->get_loc_ref_table_id()))) {
         OZ(ObTableLocation::get_full_leader_table_loc(DAS_CTX(exec_ctx).get_location_router(),
                                                       exec_ctx.get_allocator(),
                                                       exec_ctx.get_my_session()->get_effective_tenant_id(),
                                                       tsc_op->get_table_loc_id(),
                                                       tsc_op->get_loc_ref_table_id(),
                                                       table_loc));
      }
      if (OB_FAIL(ret)) {
      } else {
        const DASTabletLocList &locations = table_loc->get_tablet_locs();
        parent_dfo->set_p2p_dh_loc(table_loc);
        if (OB_FAIL(get_location_addrs<DASTabletLocList>(locations,
            parent_dfo->get_p2p_dh_addrs()))) {
          LOG_WARN("fail get location addrs", K(ret));
        }
      }
    }
  } else if (phy_op->is_dml_operator() && NULL != parent_dfo) {
    // 当前op是一个dml算子，需要设置dfo的属性
    parent_dfo->set_dml_op(true);
  } else if (phy_op->get_type() == PHY_TEMP_TABLE_ACCESS && NULL != parent_dfo) {
    parent_dfo->set_temp_table_scan(true);
    const ObTempTableAccessOpSpec *access = static_cast<const ObTempTableAccessOpSpec*>(phy_op);
    parent_dfo->set_temp_table_id(access->get_table_id());
    if (parent_dfo->need_p2p_info_ && parent_dfo->get_p2p_dh_addrs().empty()) {
      OZ(px_coord_info.p2p_temp_table_info_.temp_access_ops_.push_back(phy_op));
      OZ(px_coord_info.p2p_temp_table_info_.dfos_.push_back(parent_dfo));
    }
  } else if (IS_PX_GI(phy_op->get_type()) && NULL != parent_dfo) {
    const ObGranuleIteratorSpec *gi_spec =
        static_cast<const ObGranuleIteratorSpec *>(phy_op);
    if (gi_spec->bf_info_.is_inited_) {
      ObP2PDfoMapNode node;
      node.target_dfo_id_ = parent_dfo->get_dfo_id();
      if (OB_FAIL(px_coord_info.p2p_dfo_map_.set_refactored(
          gi_spec->bf_info_.p2p_dh_id_,
          node))) {
        LOG_WARN("fail to set p2p dh id to map", K(ret));
      } else if (OB_FAIL(px_coord_info.rf_dpd_info_.rf_use_ops_.push_back(phy_op))) {
        LOG_WARN("failed to push back parition filter gi op");
      } else {
        parent_dfo->set_need_p2p_info(true);
      }
    }
  } else if (IS_PX_JOIN_FILTER(phy_op->get_type()) && NULL != parent_dfo) {
    const ObJoinFilterSpec *filter_spec = static_cast<const ObJoinFilterSpec *>(phy_op);
    if (filter_spec->is_create_mode() && OB_FAIL(px_coord_info.rf_dpd_info_.rf_create_ops_.push_back(phy_op))) {
      LOG_WARN("failed to push back create op");
    } else if (filter_spec->is_use_mode() && OB_FAIL(px_coord_info.rf_dpd_info_.rf_use_ops_.push_back(phy_op))) {
      LOG_WARN("failed to push back use op");
    }
    if (OB_SUCC(ret) && filter_spec->is_shared_join_filter() && filter_spec->is_shuffle_) {
      ObP2PDfoMapNode node;
      node.target_dfo_id_ = parent_dfo->get_dfo_id();
      for (int i = 0; i < filter_spec->rf_infos_.count() && OB_SUCC(ret); ++i) {
        if (filter_spec->is_create_mode()) {
          if (OB_FAIL(parent_dfo->add_p2p_dh_ids(
              filter_spec->rf_infos_.at(i).p2p_datahub_id_))) {
            LOG_WARN("fail to add p2p dh ids", K(ret));
          }
        } else if (OB_FAIL(px_coord_info.p2p_dfo_map_.set_refactored(
              filter_spec->rf_infos_.at(i).p2p_datahub_id_,
              node))) {
          LOG_WARN("fail to set p2p dh id to map", K(ret));
        } else {
          parent_dfo->set_need_p2p_info(true);
        }
      }
    }
  } else if (IS_PX_COORD(phy_op->type_)) {
    if (top_px) {
      if (OB_FAIL(create_dfo(allocator, phy_op, dfo))) {
        LOG_WARN("fail create dfo", K(ret));
      }
    } else {
      // 不为嵌套 px coord 在这里分配 dfo
      // 对于嵌套 px coord，它此时只作为一个普通算子被 leaf dfo 调用
      // leaf dfo 会调用它的 next_row 接口，驱动嵌套 px coord 开启调度
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
      dfo->set_coord_info_ptr(&px_coord_info);
      dfo->set_root_dfo(true);
      dfo->set_single(true);
      dfo->set_dop(1);
      dfo->set_execution_id(exec_ctx.get_my_session()->get_current_execution_id());
      dfo->set_px_sequence_id(dfo_int_gen.get_px_sequence_id());
      if (OB_NOT_NULL(phy_op->get_phy_plan()) && phy_op->get_phy_plan()->is_enable_px_fast_reclaim()) {
        ObDetectableId sqc_detectable_id;
        // if generate_detectable_id failed, means that server id is not ready
        if (OB_FAIL(ObDetectManagerUtils::generate_detectable_id(sqc_detectable_id, GET_TENANT_ID()))) {
          LOG_WARN("[DM] failed to generate_detectable_id for sqc");
        } else {
          ObPxDetectableIds px_detectable_ids(px_coord_info.qc_detectable_id_, sqc_detectable_id);
          dfo->set_px_detectable_ids(px_detectable_ids);
        }
      }
      if (OB_SUCC(ret)) {
        // 存在嵌套情况，则dfo可能已经被设置过一些信息，所以这里不会覆盖
        if (OB_INVALID_ID == dfo->get_dfo_id()) {
          //只有顶层的dfo的receive才没有设置dfo id，即使嵌套dfo，也会设置，因为会根据transmit进行设置
          dfo->set_dfo_id(ObDfo::MAX_DFO_ID);
        }
        if (OB_INVALID_ID == dfo->get_qc_id()) {
          // receive的px记录在了transmit上
          const ObTransmitSpec *transmit = static_cast<const ObTransmitSpec *>(phy_op->get_child());
          if (OB_INVALID_ID != transmit->get_px_id()) {
            dfo->set_qc_id(transmit->get_px_id());
          }
        }
        // 对于 root dfo 来说，它并不是一个真实的 dfo，没有分配 id
        // 所以使用 ObDfo::MAX_DFO_ID表示
        if (OB_FAIL(dfo_int_gen.gen_id(dfo->get_dfo_id(), dfo->get_interrupt_id()))) {
          LOG_WARN("fail gen dfo int id", K(ret));
        }
        LOG_TRACE("cur dfo info", K(dfo->get_qc_id()), K(dfo->get_dfo_id()), K(dfo->get_dop()));
      }
    } else {
      const ObTransmitSpec *transmit = static_cast<const ObTransmitSpec *>(phy_op);
      // 如果 transmit 下面的子树里包含 px coord 算子，那么下面这些设置都会被
      // 修改成 is_local = true, dop = 1
      dfo->set_coord_info_ptr(&px_coord_info);
      dfo->set_single(transmit->is_px_single());
      dfo->set_dop(transmit->get_px_dop());
      dfo->set_qc_id(transmit->get_px_id());
      dfo->set_dfo_id(transmit->get_dfo_id());
      dfo->set_execution_id(exec_ctx.get_my_session()->get_current_execution_id());
      dfo->set_px_sequence_id(dfo_int_gen.get_px_sequence_id());
      if (OB_NOT_NULL(phy_op->get_phy_plan()) && phy_op->get_phy_plan()->is_enable_px_fast_reclaim()) {
        ObDetectableId sqc_detectable_id;
        // if generate_detectable_id failed, means that server id is not ready
        if (OB_FAIL(ObDetectManagerUtils::generate_detectable_id(sqc_detectable_id, GET_TENANT_ID()))) {
          LOG_WARN("[DM] failed to generate_detectable_id for sqc");
        } else {
          ObPxDetectableIds px_detectable_ids(px_coord_info.qc_detectable_id_, sqc_detectable_id);
          dfo->set_px_detectable_ids(px_detectable_ids);
        }
      }
      if (OB_SUCC(ret)) {
        dfo->set_dist_method(transmit->dist_method_);
        dfo->set_slave_mapping_type(transmit->get_slave_mapping_type());
        parent_dfo->set_slave_mapping_type(transmit->get_slave_mapping_type());
        dfo->set_pkey_table_loc_id(
          (reinterpret_cast<const ObPxTransmitSpec *>(transmit))->repartition_table_id_);
        if (OB_ISNULL(parent_dfo)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("parent dfo should not be null", K(ret));
        } else if (transmit->get_px_dop() <= 0) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("should have dop set by optimizer", K(ret), K(transmit->get_px_dop()));
        } else if (OB_FAIL(dfo_int_gen.gen_id(transmit->get_dfo_id(),
                                              dfo->get_interrupt_id()))) {
          LOG_WARN("fail gen dfo int id", K(ret));
        } else {
          dfo->set_qc_server_id(GCTX.server_id_);
          dfo->set_parent_dfo_id(parent_dfo->get_dfo_id());
          LOG_TRACE("cur dfo dop",
                    "dfo_id", dfo->get_dfo_id(),
                    "is_local", transmit->is_px_single(),
                    "dop", transmit->get_px_dop(),
                    K(dfo->get_qc_id()),
                    "parent dfo_id", parent_dfo->get_dfo_id(),
                    "slave mapping", transmit->is_slave_mapping());
        }
      }
    }
  }


  if (nullptr != dfo) {
    parent_dfo = dfo;
  }

  if (OB_SUCC(ret)) {
    if (got_fulltree_dfo) {
      // 序列化包含嵌套 px coord 算子的 dfo 时，需要将它下面所有的子 dfo
      // 都序列化出去，也就是要包含整个子树 (fulltree)
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
        ObDfo *tmp_parent_dfo = parent_dfo;
        if (OB_FAIL(do_split(exec_ctx, allocator, phy_op->get_child(i),
                             tmp_parent_dfo, dfo_int_gen, px_coord_info))) {
          LOG_WARN("fail split op into dfo", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObDfoMgr::create_dfo(ObIAllocator &allocator,
                         const ObOpSpec *dfo_root_op,
                         ObDfo *&dfo) const
{
  int ret = OB_SUCCESS;
  void *tmp = NULL;
  dfo = NULL;
  if (OB_ISNULL(dfo_root_op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL unexpected", K(ret));
  } else if (OB_ISNULL(tmp = allocator.alloc(sizeof(ObDfo)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("fail to alloc ObDfo", K(ret));
  } else if (OB_ISNULL(dfo = new(tmp) ObDfo(allocator))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to new ObDfo", K(ret));
  } else {
    dfo->set_root_op_spec(dfo_root_op);
    dfo->set_phy_plan(dfo_root_op->get_phy_plan());
  }
  return ret;
}

// get_ready_dfo接口仅用于单层dfo调度.
// 每次迭代一个dfo出来.
int ObDfoMgr::get_ready_dfo(ObDfo *&dfo) const
{
  int ret = OB_SUCCESS;
  bool all_finish = true;
  dfo = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < edges_.count(); ++i) {
    ObDfo *edge = edges_.at(i);
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

// 注意区别两种返回状态：
//   - 如果有 edge 还没有 finish，且不能调度更多 dfo，则返回空集合
//   - 如果所有 edge 都已经 finish，则返回 ITER_END
// 每次只迭代出一对 DFO，child & parent
int ObDfoMgr::get_ready_dfos(ObIArray<ObDfo*> &dfos) const
{
  int ret = OB_SUCCESS;
  bool all_finish = true;
  bool got_pair_dfo = false;
  dfos.reset();

  LOG_TRACE("ready dfos", K(edges_.count()));
  // edges 已经按照调度顺序排序，排在前面的优先调度
  for (int64_t i = 0; OB_SUCC(ret) && i < edges_.count(); ++i) {
    ObDfo *edge = edges_.at(i);
    ObDfo *root_edge = edges_.at(edges_.count() - 1);
    if (edge->is_thread_finish()) {
      LOG_TRACE("finish dfo", K(*edge));
      continue;
    } else {
      // edge 没有完成，调度的目标就是促成这条边尽快完成，包括调度起它所依赖的 DFO，即：
      //  - edge 没有调度起来，立即调度
      //  - edge 已经调度起来，则看这个 edge 是否依赖其它 dfo 才能完成执行
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
        // 找到 dependence 链条中优先依赖的 dfo，
        // 要求这个 dfo 为未完成状态
        ObDfo *sibling_edge = edge->depend_sibling();
        for (/* nop */;
             nullptr != sibling_edge && sibling_edge->is_thread_finish();
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
      }else {
        // 当前 edge 还没有完成，也没有 sibling edge 需要调度，返回 dfos 空集，继续等待
      }

      // 三层 DFO 调度逻辑
      // 注意：即使上面有 sibling 被调度起来了，已经调度了 3 个 DFO
      // 也还是会去尝试调度第 4 个 depend parent dfo
      if (OB_SUCC(ret) && !got_pair_dfo && GCONF._px_max_pipeline_depth > 2) {
        ObDfo *parent_edge = edge->parent();
        if (NULL != parent_edge &&
            !parent_edge->is_active() &&
            NULL != parent_edge->parent() &&
            parent_edge->parent()->is_earlier_sched()) {
          /* 为了便于描述，考虑如下场景：
           *       parent-parent
           *           |
           *         parent
           *           |
           *          edge
           * 当代码运行到这个分支时，edge 已经 active，edge、parent 两个 dfo 已经被调度
           * 并且，parent 的执行依赖于 parent-parent 也被调度（2+dfo调度优化，hash join 的
           * 结果可以直接输出，无需在上面加 material 算子）
           */
          if (OB_FAIL(dfos.push_back(parent_edge))) {
            LOG_WARN("fail push dfo", K(ret));
          } else if (OB_FAIL(dfos.push_back(parent_edge->parent()))) {
            LOG_WARN("fail push dfo", K(ret));
          } else {
            parent_edge->set_active();
            got_pair_dfo = true;
            LOG_DEBUG("dfo do earlier scheduling", K(*parent_edge->parent()));
          }
        }
      }

      // If one of root_edge's child has scheduled, we try to start the root_dfo.
      if (OB_SUCC(ret) && !got_pair_dfo) {
        if (edge->is_active() &&
            !root_edge->is_active() &&
            edge->has_parent() &&
            edge->parent() == root_edge) {
          // 本分支是一个优化，提前调度起 root dfo，使得 root dfo
          // 可以及时拉取下面的数据。在某些场景下，可以避免下层 dfo 添加
          // 不必要的 material 算子阻塞数据流动
          //
          // 之所以可以这么做，是因为 root dfo 无论调度与否，都是占着资源的，
          // 不调白不调
          if (OB_ISNULL(root_edge->parent()) || root_edge->parent() != root_dfo_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("The root edge is null or it's parent not root dfo", K(ret));
          } else if (OB_FAIL(dfos.push_back(root_edge))) {
            LOG_WARN("Fail to push dfo", K(ret));
          } else if (OB_FAIL(dfos.push_back(root_dfo_))) {
            LOG_WARN("Fail to push dfo", K(ret));
          } else {
            root_edge->set_active();
            got_pair_dfo = true;
            LOG_TRACE("Try to schedule root dfo", KP(root_edge), KP(root_dfo_));
          }
        }
      }
      // 每次只迭代一对儿结果返回出去
      //
      // 如果：
      //   - 当前 edge 还没有完成，
      //   - 也没有 sibling edge 需要调度，
      //   - 没有 depend parent edge 需要调度
      //   - root dfo 也不需要调度
      // 则返回 dfos 空集，继续等待
      break;
    }
  }
  if (all_finish && OB_SUCCESS == ret) {
    ret = OB_ITER_END;
  }
  return ret;
}

int ObDfoMgr::add_dfo_edge(ObDfo *edge)
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

int ObDfoMgr::find_dfo_edge(int64_t id, ObDfo *&edge)
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

int ObDfoMgr::get_active_dfos(ObIArray<ObDfo*> &dfos) const
{
  int ret = OB_SUCCESS;
  dfos.reset();
  // edges 已经按照调度顺序排序，排在前面的优先调度
  for (int64_t i = 0; OB_SUCC(ret) && i < edges_.count(); ++i) {
    ObDfo *edge = edges_.at(i);
    // edge 没有完成，调度的目标就是促成这条边尽快完成
    if (edge->is_active()) {
      if (OB_FAIL(dfos.push_back(edge))) {
        LOG_WARN("fail push back edge", K(ret));
      }
    }
  }
  return ret;
}

int ObDfoMgr::get_scheduled_dfos(ObIArray<ObDfo*> &dfos) const
{
  int ret = OB_SUCCESS;
  dfos.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < edges_.count(); ++i) {
    ObDfo *edge = edges_.at(i);
    // 调用过 schedule_dfo 接口，无论成功失败，dfo 就会被设置为 scheduled 状态
    if (edge->is_scheduled()) {
      if (OB_FAIL(dfos.push_back(edge))) {
        LOG_WARN("fail push back edge", K(ret));
      }
    }
  }
  return ret;
}

int ObDfoMgr::get_running_dfos(ObIArray<ObDfo*> &dfos) const
{
  int ret = OB_SUCCESS;
  dfos.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < edges_.count(); ++i) {
    ObDfo *edge = edges_.at(i);
    // 调用过 schedule_dfo 接口，无论成功失败，dfo 就会被设置为 scheduled 状态
    if (edge->is_scheduled() && !edge->is_thread_finish()) {
      if (OB_FAIL(dfos.push_back(edge))) {
        LOG_WARN("fail push back edge", K(ret));
      }
    }
  }
  return ret;
}
