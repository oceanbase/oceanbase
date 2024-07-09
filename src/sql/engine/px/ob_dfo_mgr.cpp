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
#include "sql/engine/basic/ob_temp_table_access_vec_op.h"
#include "sql/engine/basic/ob_temp_table_insert_vec_op.h"
#include "sql/engine/px/exchange/ob_transmit_op.h"
#include "sql/engine/basic/ob_material_op.h"
#include "lib/utility/ob_tracepoint.h"
#include "sql/engine/join/ob_join_filter_op.h"
#include "sql/engine/px/exchange/ob_px_repart_transmit_op.h"
#include "sql/optimizer/ob_px_resource_analyzer.h"
#include "sql/engine/px/ob_px_scheduler.h"
#include "share/detect/ob_detect_manager_utils.h"
#include "sql/engine/px/ob_px_coord_op.h"
#include "sql/engine/basic/ob_material_vec_op.h"

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

int ObDfoSchedDepthGenerator::generate_sched_depth(ObExecContext &exec_ctx, ObDfoMgr &dfo_mgr, const int64_t pipeline_depth) {
  int ret = OB_SUCCESS;
  if (pipeline_depth > 2) {
    ObDfo *dfo_tree = dfo_mgr.get_root_dfo();
    if (OB_ISNULL(dfo_tree)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL unexpected", K(ret));
    } else if (OB_FAIL(do_generate_sched_depth(exec_ctx, dfo_mgr, *dfo_tree, pipeline_depth))) {
      LOG_WARN("fail generate dfo edges", K(ret));
    }
  }
  return ret;
}

// dfo_tree 后序遍历，定出哪些 dfo 可以做 material op bypass
int ObDfoSchedDepthGenerator::do_generate_sched_depth(ObExecContext &exec_ctx, ObDfoMgr &dfo_mgr,
                                                      ObDfo &parent, const int64_t pipeline_depth)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < parent.get_child_count(); ++i) {
    ObDfo *child = NULL;
    if (OB_FAIL(parent.get_child_dfo(i, child))) {
      LOG_WARN("fail get child dfo", K(i), K(parent), K(ret));
    } else if (OB_ISNULL(child)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(do_generate_sched_depth(exec_ctx, dfo_mgr, *child, pipeline_depth))) {
      LOG_WARN("fail do generate edge", K(*child), K(ret));
    } else {
      bool need_earlier_sched = check_if_need_do_earlier_sched(*child, pipeline_depth);
      if (need_earlier_sched) {
        // current pair of DFOs are earlier scheduled, block the parent DFO's material
        // (parent DFO maybe unblocked later) to prevent unexpected hang
        if (OB_FAIL(bypass_material(exec_ctx, child->get_root_op_spec()->get_child(), true))) {
          LOG_WARN("fail set dfo block", K(ret), K(*child), K(parent));
        } else if (OB_FAIL(bypass_material(exec_ctx, parent.get_root_op_spec()->get_child(), false))) {
          LOG_WARN("fail set dfo block", K(ret), K(*child), K(parent));
        } else {
          /*
            pipeline        pipeline_pos_

            +------------+
            |            |
            |  ancestor  |          2 (earlier scheduled)
            |            |
            +------+-----+
                   ^
                   |
                   |
            +------+-----+
            |            |
            |   parent   |          1
            |            |
            +------+-----+
                   ^
                   |
                   |
            +------+-----+
            |            |
            |    child   |          0
            |            |
            +------------+
          */
          if (child->get_pipeline_pos() == 0 && !child->is_leaf_dfo()) {
            child->set_pipeline_pos(1);
          }

          // to match the optimizer, disable the early scheduling of root DFO
          if (parent.get_pipeline_pos() == 0 && parent.get_dfo_id() != ObDfo::MAX_DFO_ID) {
            parent.set_pipeline_pos(child->get_pipeline_pos() + 1);
          }
          LOG_INFO("parent dfo can do earlier scheduling", K(child->get_dfo_id()), K(parent.get_dfo_id()));
        }
      }
    }
  }
  return ret;
}

bool ObDfoSchedDepthGenerator::check_if_need_do_earlier_sched(ObDfo &dfo,
                                                              const int64_t pipeline_depth)
{
  bool do_earlier_sched = false;
  if (dfo.get_pipeline_pos() + 1 < pipeline_depth) {
    const ObOpSpec *phy_op = dfo.get_root_op_spec();
    const ObPxTransmitSpec *trans_op = NULL;
    if (OB_NOT_NULL(phy_op) && IS_PX_TRANSMIT(phy_op->type_)
        && OB_NOT_NULL(trans_op = static_cast<const ObPxTransmitSpec *>(phy_op))) {
      do_earlier_sched = trans_op->need_early_sched_;
      if (do_earlier_sched) { LOG_INFO("dfo could be early scheduled", K(phy_op->get_id())); }
    }
  }
  return do_earlier_sched;
}

int ObDfoSchedDepthGenerator::bypass_material(ObExecContext &exec_ctx, const ObOpSpec *phy_op,
                                              bool bypass)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(phy_op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("phy_op is null", K(ret));
  } else if (PHY_MATERIAL == phy_op->get_type()) {
    const ObMaterialSpec *mat = static_cast<const ObMaterialSpec *>(phy_op);
    ObOperatorKit *kit = exec_ctx.get_operator_kit(mat->id_);
    if (OB_ISNULL(kit) || OB_ISNULL(kit->input_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("operator is NULL", K(ret), KP(kit));
    } else {
      ObMaterialOpInput *mat_input = static_cast<ObMaterialOpInput *>(kit->input_);
      if (bypass) {
        // only bypass material marked as bypassable
        mat_input->set_bypass(mat->get_bypassable());
      } else {
        mat_input->set_bypass(false);
      }
    }
  } else if (PHY_VEC_MATERIAL == phy_op->get_type()) {
    const ObMaterialVecSpec *mat = static_cast<const ObMaterialVecSpec *>(phy_op);
    ObOperatorKit *kit = exec_ctx.get_operator_kit(mat->id_);
    if (OB_ISNULL(kit) || OB_ISNULL(kit->input_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("operator is NULL", K(ret), KP(kit));
    } else {
      ObMaterialVecOpInput *mat_input = static_cast<ObMaterialVecOpInput *>(kit->input_);
      if (bypass) {
        // only bypass material marked as bypassable
        mat_input->set_bypass(mat->get_bypassable());
      } else {
        mat_input->set_bypass(false);
      }
    }
  }

  if (OB_SUCC(ret) && !IS_PX_TRANSMIT(phy_op->get_type())) {
    for (int64_t i = 0; OB_SUCC(ret) && i < phy_op->get_child_num(); ++i) {
      const ObOpSpec *child = phy_op->get_child(i);
      if (OB_FAIL(bypass_material(exec_ctx, child, bypass))) {
        LOG_WARN("failed to bypass material of child", K(ret));
      }
    } // end for
  }

  return ret;
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
    } else if (PHY_VEC_MATERIAL == child->type_) {
      const ObMaterialVecSpec *mat = static_cast<const ObMaterialVecSpec *>(child);
      ObOperatorKit *kit = exec_ctx.get_operator_kit(mat->id_);
      if (OB_ISNULL(kit) || OB_ISNULL(kit->input_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("operator is NULL", K(ret), KP(kit));
      } else {
        ObMaterialVecOpInput *mat_input = static_cast<ObMaterialVecOpInput *>(kit->input_);
        mat_input->set_bypass(!block); // so that this dfo will have a blocked material op
      }
    }
  }
  return ret;
}

int64_t ObDfoSchedSimulator::ObFinalCountGetter::operator()(const ObDfo &dfo) const
{
  return dfo.get_assigned_worker_count();
}

int64_t ObDfoSchedSimulator::ObDopCountGetter::operator()(const ObDfo &dfo) const
{
  return dfo.get_dop();
}

int ObDfoSchedSimulator::create(const ObDfoAssignGetter &dfo_assign_getter,
                                bool collect_maximum_worker)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(dfo_state_map_.create(dfos_.count(), ObModIds::OB_SQL_PX, ObModIds::OB_SQL_PX))) {
    LOG_WARN("fail to create hash map", K(ret));
  } else if (OB_FAIL(dfo_start_ts_map_.create(dfos_.count() << 1, ObModIds::OB_SQL_PX,
                                              ObModIds::OB_SQL_PX))) {
    LOG_WARN("fail to create hash map", K(ret));
  } else if (OB_FAIL(dfo_max_wokrer_cnt_map_.create(dfos_.count(), ObModIds::OB_SQL_PX,
                                                    ObModIds::OB_SQL_PX))) {
    LOG_WARN("fail to create hash map", K(ret));
  } else {
    inited_ = true;
    dfo_assign_getter_ = dfo_assign_getter;
    collect_maximum_worker_ = collect_maximum_worker;
  }

  return ret;
}

int ObDfoSchedSimulator::destroy()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(dfo_state_map_.destroy())) {
    LOG_WARN("fail to destroy hash map");
  } else if (OB_FAIL(dfo_start_ts_map_.destroy())) {
    LOG_WARN("fail to destroy hash map");
  } else if (OB_FAIL(dfo_max_wokrer_cnt_map_.destroy())) {
    LOG_WARN("fail to destroy hash map");
  } else {
    ts_worker_cnt_map_.destroy();
  }
  return ret;
}

int ObDfoSchedSimulator::get_max_assigned_worker(int64_t &max_assigned) const
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dfo scheduling simulator is not inited", K(ret));
  } else {
    max_assigned = max_assigned_;
  }
  return ret;
}

int ObDfoSchedSimulator::get_max_concurrent_workers(const ObDfo *dfo,
                                                    int64_t &max_concurrent_worker) const
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dfo scheduling simulator is not inited", K(ret));
  } else if (OB_FAIL(
               dfo_max_wokrer_cnt_map_.get_refactored(dfo->get_dfo_id(), max_concurrent_worker))) {
    LOG_WARN("fail to get hash map", K(ret));
  }
  return ret;
}

int ObDfoSchedSimulator::schedule()
{
  int ret = OB_SUCCESS;

  if (!is_inited()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dfo scheduling simulator is not inited", K(ret));
  } else {
    int64_t assigned = 0;
    ARRAY_FOREACH_X(dfos_, idx, cnt, OB_SUCC(ret))
    {
      if (OB_FAIL(
            dfo_state_map_.set_refactored(dfos_.at(idx)->get_dfo_id(), ObDfoState::WAIT, 1))) {
        LOG_WARN("fail to set hash map", K(ret));
      }
    }

    ARRAY_FOREACH_X(dfos_, idx, cnt, OB_SUCC(ret))
    {
      const ObDfo *child = dfos_.at(idx);
      const ObDfo *parent = child->parent();
      // Simulate the scheduling and get the maximum of concurrent worker count
      // TODO: nested PX is not considered now
      if (OB_ISNULL(parent) || OB_ISNULL(child)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("dfo edges expect to have parent", KPC(parent), KPC(child), K(ret));
      } else if (OB_FAIL(schedule_dfo(child, assigned, max_assigned_))) {
        LOG_WARN("fail to schedule dfo", K(ret));
      } else if (OB_FAIL(schedule_dfo(parent, assigned, max_assigned_))) {
        LOG_WARN("fail to schedule dfo", K(ret));
      } else if (child->has_depend_sibling()) {
        // In the extreme scenario, all ancestor and sibling DFOs will be scheduled concurrently
        if (OB_FAIL(schedule_ancester_dfos(parent, assigned, max_assigned_))) {
          LOG_WARN("fail to schedule ancester dfos", K(ret));
        } else if (OB_FAIL(schedule_sibling_dfos(child, assigned, max_assigned_))) {
          LOG_WARN("fail to schedule sibling dfos", K(ret));
        }
      } else if (OB_FAIL(schedule_ancester_dfos(parent, assigned, max_assigned_))) {
        LOG_WARN("fail to schedule ancester dfos", K(ret));
      }
      
      if (OB_SUCC(ret) && OB_FAIL(finish_dfo(child, assigned))) {
        LOG_WARN("fail to finish dfo", K(ret));
      }
    }
  }
  return ret;
}

int ObDfoSchedSimulator::all_child_dfo_finish(const ObDfo *dfo, bool &all_finish)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObDfo *> &child_dfos = dfo->get_child_dfos();
  ObDfoState child_state;
  all_finish = true;
  ARRAY_FOREACH(child_dfos, idx)
  {
    if (OB_ISNULL(child_dfos.at(idx))) {
      LOG_WARN("unexpected null child dfo", K(ret));
    } else if (OB_FAIL(
                 dfo_state_map_.get_refactored(child_dfos.at(idx)->get_dfo_id(), child_state))) {
      LOG_WARN("fail to get hash map", K(ret));
    } else if (child_state != ObDfoState::FINISH) {
      LOG_INFO("child is not finished", K(child_dfos.at(idx)->get_dfo_id()));
      all_finish = false;
    }
  }

  return ret;
}

int ObDfoSchedSimulator::schedule_dfo(const ObDfo *dfo, int64_t &assigned, int64_t &max_assigned)
{
  int ret = OB_SUCCESS;
  ObDfoState state;
  if (OB_FAIL(dfo_state_map_.get_refactored(dfo->get_dfo_id(), state))) {
    // ignore the TOP dfo to match the scheduling of optimizer
    if (ret == OB_HASH_NOT_EXIST
        && dfo->get_dfo_id() == ObDfo::MAX_DFO_ID) { // use ObDfo::MAX_DFO_ID instead
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get hash map", K(dfo->get_dfo_id()), K(ret));
    }
  } else if (state == ObDfoState::WAIT) {
    // current dfo hasn't been scheduled yet, schedule it
    if (OB_FAIL(dfo_state_map_.set_refactored(dfo->get_dfo_id(), ObDfoState::RUNNING, 1))) {
      LOG_WARN("fail to set hash map", K(dfo->get_dfo_id()), K(ret));
    } else {
      assigned += dfo_assign_getter_(*dfo);
      LOG_INFO("schedule a dfo", K(assigned), K(dfo->get_dfo_id()),
               K(dfo->get_assigned_worker_count()));

      // when a DFO is scheduled, record its timestamp (currently, ts is the index
      // when the DFO is inserted into the ts_worker_cnt_map_)
      if (collect_maximum_worker_) {
        int64_t start_timestamp = ts_worker_cnt_map_.count();
        if (OB_FAIL(ts_worker_cnt_map_.push_back(assigned))) {
          LOG_WARN("fail to record the worker count", K(ret));
        } else if (OB_FAIL(dfo_start_ts_map_.set_refactored(dfo->get_dfo_id(), start_timestamp))) {
          LOG_WARN("fail to record the dfo's timestamp", K(ret));
        }
      }

      if (assigned > max_assigned) {
        max_assigned = assigned;
        LOG_TRACE("update total assigned by sibling dfos", K(dfo->get_dfo_id()),
                  K(dfo->get_assigned_worker_count()), K(max_assigned));
      }
    }
  } else {
    LOG_INFO("dfo is not for scheduling", K(dfo->get_dfo_id()), K(state));
  }
  return ret;
}

int ObDfoSchedSimulator::finish_dfo(const ObDfo *dfo, int64_t &assigned)
{
  int ret = OB_SUCCESS;
  ObDfoState state;
  if (OB_FAIL(dfo_state_map_.get_refactored(dfo->get_dfo_id(), state))) {
    LOG_WARN("fail to get hash map", K(ret));
  } else if (state == ObDfoState::RUNNING) {
    bool all_child_finish;
    // if DFO has no child dfos or all child DFOs are finished, stop currentDFO
    if (OB_FAIL(all_child_dfo_finish(dfo, all_child_finish))) {
      LOG_WARN("fail to get child states", K(ret));
    } else if (all_child_finish) {
      if (OB_FAIL(dfo_state_map_.set_refactored(dfo->get_dfo_id(), ObDfoState::FINISH, 1))) {
        LOG_WARN("fail to set hash map", K(ret));
      } else {
        assigned -= dfo_assign_getter_(*dfo);

        if (collect_maximum_worker_) {
          int64_t start_timestamp;
          if (OB_FAIL(dfo_start_ts_map_.get_refactored(dfo->get_dfo_id(), start_timestamp))) {
            LOG_WARN("fail to get the dfo's timestamp", K(ret));
          } else {
            int64_t max_worker_cnt = -1;
            // the following method is O(n), so the total time complexity
            // is O(n^2), optimize it iterate [start_timestamp,
            // current_timestamp] to find the max worker count
            for (int64_t idx = start_timestamp; idx < ts_worker_cnt_map_.count(); ++idx) {
              if (ts_worker_cnt_map_.at(idx) > max_worker_cnt) {
                max_worker_cnt = ts_worker_cnt_map_.at(idx);
              }
            }

            if (-1 == max_worker_cnt) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("fail to get max worker count for current DFO");
            } else if (OB_FAIL(dfo_max_wokrer_cnt_map_.set_refactored(dfo->get_dfo_id(),
                                                                      max_worker_cnt))) {
              LOG_WARN("fail to set hashmap");
            }
          }
        }

        if (OB_SUCC(ret)) {
          LOG_INFO("finish a dfo", K(assigned), K(dfo->get_dfo_id()),
                   K(dfo->get_assigned_worker_count()));
        }
      }
    }
  }
  return ret;
}

int ObDfoSchedSimulator::schedule_ancester_dfos(const ObDfo *dfo, int64_t &assigned,
                                                int64_t &max_assigned)
{
  int ret = OB_SUCCESS;
  // To bypass MATERIAL operators, ancestor DFOs will be scheduled.
  const ObDfo *cur = dfo->parent();
  ObDfoState state;

  while (OB_SUCC(ret) && OB_NOT_NULL(cur) && cur->is_earlier_sched()) {
    if (OB_FAIL(schedule_dfo(cur, assigned, max_assigned))) {
      LOG_WARN("fail to schedule dfo", K(cur->get_dfo_id()), K(ret));
    } else {
      cur = cur->parent();
    }
  }
  return ret;
}

int ObDfoSchedSimulator::schedule_sibling_dfos(const ObDfo *dfo, int64_t &assigned,
                                               int64_t &max_assigned)
{
  // 局部右深树的场景，depend_sibling 和当前 child dfo 都会被调度
  /* Why need extra flag has_depend_sibling_? Why not use NULL !=
   * depend_sibling_? dfo5 (dop=2)
   *         /      |      \
   *      dfo1(2) dfo2 (4) dfo4 (1)
   *                        |
   *                      dfo3 (2)
   * Schedule order: (4,3) => (4,5) => (4,5,1) => (4,5,2) => (4,5) => (5).
   * max_dop = (4,5,2) = 1 + 2 + 4 = 7. Depend sibling: dfo4 -> dfo1 ->
   * dfo2. Depend sibling is stored as a list. We thought dfo2 is depend
   * sibling of dfo1, and calculated incorrect max_dop = (1,2,5) = 2 + 4 +
   * 2 = 8. Actually, dfo1 and dfo2 are depend sibling of dfo4, but dfo2
   * is not depend sibling of dfo1. So we use has_depend_sibling_ record
   * whether the dfo is the header of list.
   */
  int ret = OB_SUCCESS;
  const ObDfo *child = dfo;
  while (OB_NOT_NULL(child->depend_sibling())) {
    child = child->depend_sibling();
    // sibling dfo are leaf dfos, so they will end quickly
    if (OB_FAIL(schedule_dfo(child, assigned, max_assigned))) {
      LOG_WARN("fail to schedule dfo", K(ret));
    } else if (OB_FAIL(finish_dfo(child, assigned))) {
      LOG_WARN("fail to finish dfo", K(ret));
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

    LOG_INFO("calc px worker count", K(query_expected), K(query_minimal), K(query_admited),
                            K(px_expected), K(px_minimal), K(px_admited));
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

  /*
   * Dynamically allocate DOP for each worker.
   * Based on the most extreme scheduling sequence in the actual scenario,
   * record the maximum number of concurrent workers that may be scheduled simultaneously for each DFO.
   * Then, perform downgrading based on that number.
   */
  const ObIArray<ObDfo *> & dfos = dfo_mgr.get_all_dfos();
  common::hash::ObHashMap<int64_t, int64_t> dfo_start_ts_map;
  ObSEArray<int64_t, 16> ts_worker_cnt_map;
   
  double scale_rate = 1.0;
  bool match_expected = false;
  bool compatible_before_420 = false;
  int64_t total_assigned = 0;
  if (OB_FAIL(dfo_start_ts_map.create(dfos.count(), ObModIds::OB_SQL_PX,
                                      ObModIds::OB_SQL_PX))) {
    LOG_WARN("fail to create hash map", K(ret));
  } else if (OB_UNLIKELY(admited_worker_count < 0 ||
                         expected_worker_count <= 0 ||
                         minimal_worker_count <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should have at least one worker",  K(ret), K(admited_worker_count),
                                        K(expected_worker_count), K(minimal_worker_count));
  } else if (admited_worker_count >= expected_worker_count) {
    match_expected = true;
  } else if (minimal_worker_count <= 0) {
    // compatible with version before 4.2
    compatible_before_420 = true;
    scale_rate = static_cast<double>(admited_worker_count) / static_cast<double>(expected_worker_count);
  } else if (0 >= admited_worker_count ||
             minimal_worker_count == admited_worker_count) {
    scale_rate = 0.0;
  } else if (OB_UNLIKELY(minimal_worker_count > admited_worker_count ||
                         minimal_worker_count >= expected_worker_count)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected params", K(ret), K(minimal_worker_count), K(admited_worker_count), K(expected_worker_count));
  } else {
    // simulate the schedule and get every worker's max concurrent worker number
    scale_rate = static_cast<double>(admited_worker_count - minimal_worker_count)
                 / static_cast<double>(expected_worker_count - minimal_worker_count);
  }

  ObDfoSchedSimulator dfo_sched_simulator(dfos);
  if (OB_FAIL(dfo_sched_simulator.create(ObDfoSchedSimulator::ObDopCountGetter(), true))) {
    LOG_WARN("fail to create dfo schedule simulator");
  } else if (OB_FAIL(dfo_sched_simulator.schedule())) {
    LOG_WARN("fail to simulate dfo scheduling");
  } else if (OB_FAIL(
                 dfo_sched_simulator.get_max_assigned_worker(total_assigned))) {
    LOG_WARN("fail to get max assigned worker");
  }

  ARRAY_FOREACH_X(dfos, idx, cnt, OB_SUCC(ret)) {
    ObDfo *child = dfos.at(idx);
    int64_t val = 0;
    int64_t max_concurrent_worker = 0;
    if (match_expected) {
      val = child->get_dop();
    } else if (compatible_before_420) {
      val = std::max(1L, static_cast<int64_t>(static_cast<double>(child->get_dop()) * scale_rate));
    } else {
      if (OB_FAIL(dfo_sched_simulator.get_max_concurrent_workers(child, max_concurrent_worker))) {
        LOG_WARN("fail to get max concurrent workers for DFO", K(ret), K(child->get_dfo_id())); 
      } else {
        if (admited_worker_count >= max_concurrent_worker) {
          val = child->get_dop(); 
        } else {
          val = 1L + static_cast<int64_t>(std::max(static_cast<double>(child->get_dop() - 1), 0.0) * static_cast<double>(admited_worker_count - minimal_worker_count)
                 / static_cast<double>(max_concurrent_worker - minimal_worker_count));
        }
        LOG_TRACE("get max concurrent worker count", K(child->get_dfo_id()), K(max_concurrent_worker), K(admited_worker_count), K(val));
      }
    }
    child->set_assigned_worker_count(val);
    if (child->is_single() && val > 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("local dfo do should not have more than 1", K(*child), K(val), K(ret));
    }
    LOG_TRACE("assign worker count to dfo",
              "dfo_id", child->get_dfo_id(), K(admited_worker_count),
              K(expected_worker_count), K(minimal_worker_count),
              "dop", child->get_dop(), K(scale_rate), K(val));
  }

  // 因为上面取了 max，所以可能实际 assigned 的会超出 admission 数，这时应该报错
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_dfos_worker_count(dfos, false, total_assigned))) {
    LOG_WARN("failed to get dfos worker count", K(ret));
  } else if (total_assigned > admited_worker_count && admited_worker_count != 0) {
    // 意味着某些 dfo 理论上一个线程都分不到
    ret = OB_ERR_PARALLEL_SERVERS_TARGET_NOT_ENOUGH;
    LOG_USER_ERROR(OB_ERR_PARALLEL_SERVERS_TARGET_NOT_ENOUGH, total_assigned);
    LOG_WARN("total assigned worker to dfos is more than admited_worker_count",
             K(total_assigned),
             K(admited_worker_count),
             K(minimal_worker_count),
             K(expected_worker_count),
             K(ret));
  }
  return ret;
}

int ObDfoWorkerAssignment::get_dfos_worker_count(const ObIArray<ObDfo *> &dfos,
                                                 const bool get_minimal,
                                                 int64_t &total_assigned) {
  int ret = OB_SUCCESS;
  ObDfoSchedSimulator dfo_sched_simulator(dfos);
  if (get_minimal && OB_FAIL(dfo_sched_simulator.create(
                         ObDfoSchedSimulator::ObMinimalCountGetter(), false))) {
    LOG_WARN("fail to create dfo schedule simulator");
  } else if (!get_minimal &&
             OB_FAIL(dfo_sched_simulator.create(
                 ObDfoSchedSimulator::ObFinalCountGetter(), false))) {
    LOG_WARN("fail to create dfo schedule simulator");
  } else if (OB_FAIL(dfo_sched_simulator.schedule())) {
    LOG_WARN("fail to simulate dfo scheduling");
  } else if (OB_FAIL(
                 dfo_sched_simulator.get_max_assigned_worker(total_assigned))) {
    LOG_WARN("fail to get max assigned worker");
  } else if (OB_FAIL(dfo_sched_simulator.destroy())) {
    LOG_WARN("fail to destroy dfo schedule simulator");
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
  int64_t px_max_pipeline_depth =
    min(static_cast<const ObPxCoordSpec *>(&root_op_spec)->get_pipeline_depth(),
        GCONF._px_max_pipeline_depth);
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("dfo mgr init twice", K(ret));
  } else if (OB_FAIL(do_split(exec_ctx, allocator_, &root_op_spec, root_dfo_, dfo_int_gen,
                              px_coord_info))) {
    LOG_WARN("fail split ops into dfo", K(ret));
  } else if (OB_ISNULL(root_dfo_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL dfo unexpected", K(ret));
  } else if (!px_coord_info.rf_dpd_info_.is_empty()
             && OB_FAIL(px_coord_info.rf_dpd_info_.describe_dependency(root_dfo_))) {
    LOG_WARN("failed to describe rf dependency");
  } else if (OB_FAIL(ObDfoSchedOrderGenerator::generate_sched_order(*this))) {
    LOG_WARN("fail init dfo mgr", K(ret));
  } else if (OB_FAIL(ObDfoSchedDepthGenerator::generate_sched_depth(exec_ctx, *this,
                                                                    px_max_pipeline_depth))) {
    LOG_WARN("fail init dfo mgr", K(ret));
  } else if (OB_FAIL(ObDfoWorkerAssignment::calc_admited_worker_count(
               get_all_dfos(), exec_ctx, root_op_spec, px_expected, px_minimal, px_admited))) {
    LOG_WARN("fail to calc admited worler count", K(ret));
  } else if (OB_FAIL(ObDfoWorkerAssignment::assign_worker(*this, px_expected, px_minimal,
                                                                 px_admited))) {
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
    LOG_WARN("the first phy_op must be a coord op", K(ret), K(phy_op->type_));
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
    const ObPhyOperatorType op_type = phy_op->get_type();
    LOG_TRACE("set DFO need_branch_id", K(op_type));
    parent_dfo->set_need_branch_id_op(op_type == PHY_INSERT_ON_DUP
                                      || op_type == PHY_REPLACE
                                      || op_type == PHY_LOCK);
  } else if (phy_op->get_type() == PHY_TEMP_TABLE_ACCESS && NULL != parent_dfo) {
    parent_dfo->set_temp_table_scan(true);
    const ObTempTableAccessOpSpec *access = static_cast<const ObTempTableAccessOpSpec*>(phy_op);
    parent_dfo->set_temp_table_id(access->get_table_id());
    if (parent_dfo->need_p2p_info_ && parent_dfo->get_p2p_dh_addrs().empty()) {
      OZ(px_coord_info.p2p_temp_table_info_.temp_access_ops_.push_back(phy_op));
      OZ(px_coord_info.p2p_temp_table_info_.dfos_.push_back(parent_dfo));
    }
  } else if (phy_op->get_type() == PHY_VEC_TEMP_TABLE_ACCESS && NULL != parent_dfo) {
    parent_dfo->set_temp_table_scan(true);
    const ObTempTableAccessVecOpSpec *access = static_cast<const ObTempTableAccessVecOpSpec*>(phy_op);
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

// schdule child dfo and its parent dfo
int ObDfoMgr::schedule_child_parent(ObIArray<ObDfo *> &dfos, ObDfo *child) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(dfos.push_back(child))) {
    LOG_WARN("fail to push dfo", K(ret));
  } else if (NULL == child->parent()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parent is NULL, unexpected", K(ret));
  } else if (OB_FAIL(dfos.push_back(child->parent()))) {
    LOG_WARN("fail push dfo", K(ret));
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
      // if the DFO is not scheduled, schedule it immediately
      // else check if this DFO depends on other DFOs to complete execution
      all_finish = false;
      if (!edge->is_active()) {
        if (OB_FAIL(schedule_child_parent(dfos, edge))) {
          LOG_WARN("fail to schedule current dfo and its parent", K(ret));
        } else {
          edge->set_active();
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
        } else if (OB_FAIL(schedule_child_parent(dfos, sibling_edge))) {
          LOG_WARN("fail to schedule current dfo and its parent", K(ret));
        } else {
          sibling_edge->set_active();
          LOG_TRACE("start schedule dfo", K(*sibling_edge), K(*sibling_edge->parent()));
        }
      }

      // Attempt to schedule the parent's parent DFO earlier. Note that the DFO being scheduled 
      // earlier may run concurrently with the depend sibling DFOs.
      if (OB_SUCC(ret) && dfos.empty() && GCONF._px_max_pipeline_depth > 2) {
        ObDfo *parent_edge = edge->parent();
        while (OB_NOT_NULL(parent_edge) &&
            OB_NOT_NULL(parent_edge->parent()) &&
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
          if (parent_edge->is_active()) {
            // check if current parent's parent depends on ancester
            parent_edge = parent_edge->parent();
            continue;
          } else if (OB_FAIL(schedule_child_parent(dfos, parent_edge))) {
            LOG_WARN("fail to schedule current dfo and its parent", K(ret));
          } else {
            parent_edge->set_active();
            LOG_INFO("dfo do earlier scheduling", K(parent_edge->parent()->get_root_op_spec()->get_id()));
          }
          break;
        }
      }

      // If one of root_edge's child has scheduled, we try to start the root_dfo.
      if (OB_SUCC(ret) && dfos.empty()) {
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
