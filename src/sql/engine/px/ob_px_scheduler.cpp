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

#include "sql/engine/px/ob_px_scheduler.h"
#include "sql/engine/px/ob_dfo_scheduler.h"
#include "lib/random/ob_random.h"
#include "share/ob_rpc_share.h"
#include "share/schema/ob_part_mgr_util.h"
#include "sql/ob_sql.h"
#include "sql/engine/px/ob_px_util.h"
#include "sql/dtl/ob_dtl_channel_group.h"
#include "sql/dtl/ob_dtl_channel_loop.h"
#include "sql/dtl/ob_dtl_msg_type.h"
#include "sql/dtl/ob_dtl.h"
#include "sql/dtl/ob_dtl_task.h"
#include "sql/dtl/ob_op_metric.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/px/ob_px_dtl_msg.h"
#include "sql/engine/px/ob_px_dtl_proc.h"
#include "sql/engine/px/ob_px_util.h"
#include "sql/engine/px/ob_px_interruption.h"
#include "share/config/ob_server_config.h"
#include "sql/engine/px/ob_px_sqc_async_proxy.h"
#include "sql/engine/px/datahub/ob_dh_dtl_proc.h"
#include "sql/engine/px/datahub/components/ob_dh_winbuf.h"

namespace oceanbase {
using namespace common;
using namespace share;
using namespace omt;
using namespace share::schema;
using namespace sql;
using namespace sql::dtl;
namespace sql {

template <typename PieceMsg>
class ObDhPieceMsgProc {
public:
  ObDhPieceMsgProc() = default;
  ~ObDhPieceMsgProc() = default;
  int on_piece_msg(ObPxCoordInfo& coord_info, ObExecContext& ctx, const PieceMsg& pkt)
  {
    int ret = OB_SUCCESS;
    UNUSED(ctx);
    ObArray<ObPxSqcMeta*> sqcs;
    ObDfo* dfo = nullptr;
    ObPieceMsgCtx* piece_ctx = nullptr;
    // FIXME (TODO): the dfo id is not necessary, a mapping from op_id to dfo id can be maintained locally
    if (OB_FAIL(coord_info.dfo_mgr_.find_dfo_edge(pkt.dfo_id_, dfo))) {
      LOG_WARN("fail find dfo", K(pkt), K(ret));
    } else if (OB_ISNULL(dfo)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr or null session ptr", KP(dfo), K(pkt), K(ret));
    } else if (OB_FAIL(coord_info.piece_msg_ctx_mgr_.find_piece_ctx(pkt.op_id_, piece_ctx))) {
      // create a ctx if not found
      // NOTE: The method of creating a piece_ctx here will not cause concurrency problems.
      // because QC is a single-threaded message loop, the messages sent by SQC are processed one by one
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("fail get ctx", K(pkt), K(ret));
      } else if (OB_FAIL(
                     PieceMsg::PieceMsgCtx::alloc_piece_msg_ctx(pkt, ctx, dfo->get_total_task_count(), piece_ctx))) {
        LOG_WARN("fail to alloc piece msg", K(ret));
      } else if (nullptr != piece_ctx) {
        if (OB_FAIL(coord_info.piece_msg_ctx_mgr_.add_piece_ctx(piece_ctx))) {
          LOG_WARN("fail add barrier piece ctx", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      typename PieceMsg::PieceMsgCtx* ctx = static_cast<typename PieceMsg::PieceMsgCtx*>(piece_ctx);
      if (OB_FAIL(dfo->get_sqcs(sqcs))) {
        LOG_WARN("fail get qc-sqc channel for QC", K(ret));
      } else if (OB_FAIL(PieceMsg::PieceMsgListener::on_message(*ctx, sqcs, pkt))) {
        LOG_WARN("fail process piece msg", K(pkt), K(ret));
      }
    }
    return ret;
  }
};

int ObPxMsgProc::on_process_end(ObExecContext& ctx)
{
  UNUSED(ctx);
  int ret = OB_SUCCESS;
  return ret;
}

// entry function
int ObPxMsgProc::startup_msg_loop(ObExecContext& ctx)
{
  int ret = OB_SUCCESS;
  LOG_TRACE("TIMERECORD ", "reserve:=-1 name:=QC dfoid:=-1 sqcid:=-1 taskid:=-1 start:", ObTimeUtility::current_time());
  if (OB_FAIL(scheduler_->init_all_dfo_channel(ctx))) {
    LOG_WARN("fail to init all dfo channel", K(ret));
  } else if (OB_FAIL(scheduler_->try_schedule_next_dfo(ctx))) {
    LOG_WARN("fail to sched next one dfo", K(ret));
  }
  return ret;
}

// 1. Find the corresponding dfo, sqc according to the pkt information, and mark the current sqc thread allocation is
// completed
// 2. Determine whether all sqc under the dfo have been allocated threads
//    if it is completed, mark dfo as thread_inited, and go to step 3, otherwise end processing
// 3. Check whether the parent of the current dfo is in thread_inited state:
//    if true:
//       - call on_dfo_pair_thread_inited to trigger channel pairing and distribution of two dfos
//    else:
//       - check whether any child of the current dfo is in thread_inited state
//          - if true, call on_dfo_pair_thread_inited to trigger channel pairing and distribution of two dfos
//          - else nop
int ObPxMsgProc::on_sqc_init_msg(ObExecContext& ctx, const ObPxInitSqcResultMsg& pkt)
{
  int ret = OB_SUCCESS;

  LOG_TRACE("on_sqc_init_msg", K(pkt));

  ObDfo* edge = NULL;
  ObPxSqcMeta* sqc = NULL;
  if (OB_SUCCESS != pkt.rc_) {
    ret = pkt.rc_;
    update_error_code(coord_info_.first_error_code_, pkt.rc_);
    LOG_WARN("fail init sqc", K(pkt), K(ret));
  } else if (pkt.task_count_ <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task count returned by sqc invalid. expect 1 or more", K(pkt), K(ret));
  } else if (OB_FAIL(coord_info_.dfo_mgr_.find_dfo_edge(pkt.dfo_id_, edge))) {
    LOG_WARN("fail find dfo", K(pkt), K(ret));
  } else if (OB_ISNULL(edge)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", KP(edge), K(ret));
  } else if (OB_FAIL(edge->get_sqc(pkt.sqc_id_, sqc))) {
    LOG_WARN("fail find sqc", K(pkt), K(ret));
  } else if (OB_ISNULL(sqc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", KP(sqc), K(ret));
  } else if (OB_FAIL(sqc->get_partitions_info().assign(pkt.partitions_info_))) {
    LOG_WARN("Failed to assign partitions info", K(ret));
  } else {
    sqc->set_task_count(pkt.task_count_);
    sqc->set_thread_inited(true);
  }

  if (OB_SUCC(ret)) {
    ObArray<ObPxSqcMeta*> sqcs;
    if (OB_FAIL(edge->get_sqcs(sqcs))) {
      LOG_WARN("fail get qc-sqc channel for QC", K(ret));
    } else {
      bool sqc_threads_inited = true;
      ARRAY_FOREACH_X(sqcs, idx, cnt, OB_SUCC(ret))
      {
        ObPxSqcMeta* sqc = sqcs.at(idx);
        if (OB_ISNULL(sqc)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL unexpected sqc", K(ret));
        } else if (!sqc->is_thread_inited()) {
          sqc_threads_inited = false;
          break;
        }
      }
      if (OB_SUCC(ret) && sqc_threads_inited) {
        LOG_TRACE("on_sqc_init_msg: all sqc returned task count. ready to do on_sqc_threads_inited", K(*edge));
        edge->set_thread_inited(true);
        ret = scheduler_->on_sqc_threads_inited(ctx, *edge);
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (edge->is_thread_inited()) {
      // Try to notify parent and child at the same time
      if (edge->has_parent() && edge->parent()->is_thread_inited()) {
        if (OB_FAIL(on_dfo_pair_thread_inited(ctx, *edge, *edge->parent()))) {
          LOG_WARN("fail co-schedule parent-edge", K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        int64_t cnt = edge->get_child_count();
        for (int64_t idx = 0; idx < cnt && OB_SUCC(ret); ++idx) {
          ObDfo* child = NULL;
          if (OB_FAIL(edge->get_child_dfo(idx, child))) {
            LOG_WARN("fail get child dfo", K(idx), K(cnt), K(ret));
          } else if (OB_ISNULL(child)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("NULL unexpected", K(ret));
          } else if (child->is_thread_inited()) {
            if (OB_FAIL(on_dfo_pair_thread_inited(ctx, *child, *edge))) {
              LOG_WARN("fail co-schedule edge-child", K(ret));
            }
          }
        }
      }
    }
  }

  return ret;
}

// 1. Find dfo, sqc according to pkt information, mark the current sqc has been executed
// 2. Determine whether all sqc under the dfo have been executed
//    if completed, mark dfo as thread_finish
// 3. Determine whether the current dfo is marked as thread_finish state
//    if true:
//      - send release thread message to dfo
//      - schedule the next dfo
//        - if all dfos have been scheduled to complete (regardless of Coord), nop
//    else:
//      - nop
int ObPxMsgProc::on_sqc_finish_msg(ObExecContext& ctx, const ObPxFinishSqcResultMsg& pkt)
{
  int ret = OB_SUCCESS;
  int sqc_ret = OB_SUCCESS;
  ObDfo* edge = NULL;
  ObPxSqcMeta* sqc = NULL;
  ObSQLSessionInfo* session = NULL;

  if (OB_ISNULL(session = ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr session", K(ret));
  } else if (OB_FAIL(session->get_trans_result().merge_result(pkt.get_trans_result()))) {
    LOG_WARN("fail merge result",
        K(ret),
        "session_trans_result",
        session->get_trans_result(),
        "packet_trans_result",
        pkt.get_trans_result());
  } else {
    LOG_DEBUG("on_sqc_finish_msg trans_result",
        "session_trans_result",
        session->get_trans_result(),
        "packet_trans_result",
        pkt.get_trans_result());
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(coord_info_.dfo_mgr_.find_dfo_edge(pkt.dfo_id_, edge))) {
    LOG_WARN("fail find dfo", K(pkt), K(ret));
  } else if (OB_FAIL(edge->get_sqc(pkt.sqc_id_, sqc))) {
    LOG_WARN("fail find sqc", K(pkt), K(ret));
  } else if (OB_ISNULL(edge) || OB_ISNULL(sqc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", KP(edge), KP(sqc), K(ret));
  } else if (FALSE_IT(sqc->set_need_report(false))) {
  } else if (OB_FAIL(sqc->set_task_monitor_info_array(pkt.task_monitor_info_array_))) {
    LOG_WARN("fail to copy task monitor info array", K(ret));
  } else if (common::OB_INVALID_ID != pkt.temp_table_id_ && pkt.interm_result_ids_.count() > 0) {
    if (OB_FAIL(ctx.add_temp_table_interm_result_ids(pkt.temp_table_id_, pkt.sqc_id_, pkt.interm_result_ids_))) {
      LOG_WARN("failed to add temp table interm resuld ids.", K(ret));
    } else { /*do nothing.*/
    }
  } else { /*do nothing.*/
  }
  if (OB_SUCC(ret)) {
    sqc->set_thread_finish(true);

    NG_TRACE_EXT(sqc_finish, OB_ID(dfo_id), sqc->get_dfo_id(), OB_ID(sqc_id), sqc->get_sqc_id());

    LOG_TRACE("[MSG] sqc finish", K(*edge), K(*sqc));
  }

  if (OB_SUCC(ret)) {
    ObArray<ObPxSqcMeta*> sqcs;
    if (OB_FAIL(edge->get_sqcs(sqcs))) {
      LOG_WARN("fail get qc-sqc channel for QC", K(ret));
    } else {
      bool sqc_threads_finish = true;
      int64_t dfo_used_worker_count = 0;
      ARRAY_FOREACH_X(sqcs, idx, cnt, OB_SUCC(ret))
      {
        ObPxSqcMeta* sqc = sqcs.at(idx);
        if (OB_ISNULL(sqc)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL unexpected sqc", K(ret));
        } else if (!sqc->is_thread_finish()) {
          sqc_threads_finish = false;
          break;
        } else {
          // the value 1 is accounted for sqc thread
          dfo_used_worker_count += sqc->get_task_count();
        }
      }
      if (OB_SUCC(ret) && sqc_threads_finish) {
        edge->set_thread_finish(true);
        edge->set_used_worker_count(dfo_used_worker_count);
        LOG_TRACE("[MSG] dfo finish", K(*edge));
      }
    }
  }

  /**
   * The reason for judging the error code here is that the status of the sqc and dfo that
   * has the error needs to be updated. the sqc (including its workers) that sent the finish
   * message has actually ended. but because of an error, the subsequent scheduling process
   * does not need to continue, and the subsequent process will perform error handling
   */
  update_error_code(coord_info_.first_error_code_, pkt.rc_);
  if (OB_SUCC(ret)) {
    if (OB_FAIL(pkt.rc_)) {
      LOG_WARN("sqc fail, abort qc", K(pkt), K(ret));
    } else {
      // pkt rc_ == OB_SUCCESS
      if (OB_ISNULL(ctx.get_physical_plan_ctx())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("phy plan ctx is null", K(ret));
      } else {
        ctx.get_physical_plan_ctx()->add_affected_rows(pkt.sqc_affected_rows_);
        ctx.get_physical_plan_ctx()->add_px_dml_row_info(pkt.dml_row_info_);
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (edge->is_thread_finish()) {
      ret = scheduler_->try_schedule_next_dfo(ctx);
      if (OB_ITER_END == ret) {
        coord_info_.all_threads_finish_ = true;
        LOG_TRACE(
            "TIMERECORD ", "reserve:=-1 name:=QC dfoid:=-1 sqcid:=-1 taskid:=-1 end:", ObTimeUtility::current_time());
        ret = OB_SUCCESS;  // ignore error
      }
    }
  }

  return ret;
}

int ObPxMsgProc::on_piece_msg(ObExecContext& ctx, const ObBarrierPieceMsg& pkt)
{
  ObDhPieceMsgProc<ObBarrierPieceMsg> proc;
  return proc.on_piece_msg(coord_info_, ctx, pkt);
}

int ObPxMsgProc::on_piece_msg(ObExecContext& ctx, const ObWinbufPieceMsg& pkt)
{
  ObDhPieceMsgProc<ObWinbufPieceMsg> proc;
  return proc.on_piece_msg(coord_info_, ctx, pkt);
}

int ObPxMsgProc::on_eof_row(ObExecContext& ctx)
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  // 1. mark some channel as eof
  // 2. see if all PX data channel eof, if yes, return OB_ITER_END
  return ret;
}

int ObPxMsgProc::on_dfo_pair_thread_inited(ObExecContext& ctx, ObDfo& child, ObDfo& parent)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(scheduler_->build_data_mn_xchg_ch(ctx, child, parent))) {
      LOG_WARN("fail init dtl data channel", K(ret));
    } else {
      LOG_TRACE("build data xchange channel for dfo pair ok", K(parent), K(child));
    }
  }
  // Distribute the dtl channel information to the parent and child DFOs so that
  // they can start sending and receiving data
  if (OB_SUCC(ret)) {
    if (OB_FAIL(scheduler_->dispatch_dtl_data_channel_info(ctx, child, parent))) {
      LOG_WARN("fail setup dtl data channel for child-parent pair", K(ret));
    } else {
      LOG_TRACE("dispatch dtl data channel for pair ok", K(parent), K(child));
    }
  }
  return ret;
}

int ObPxMsgProc::on_interrupted(ObExecContext& ctx, const ObInterruptCode& ic)
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  // override ret code
  ret = ic.code_;
  LOG_TRACE("qc received a interrupt and throw out of msg proc", K(ic), K(ret));
  return ret;
}

int ObPxMsgProc::on_sqc_init_fail(ObDfo& dfo, ObPxSqcMeta& sqc)
{
  int ret = OB_SUCCESS;
  UNUSED(dfo);
  UNUSED(sqc);
  return ret;
}

//////////// END /////////

int ObPxTerminateMsgProc::on_sqc_init_msg(ObExecContext& ctx, const ObPxInitSqcResultMsg& pkt)
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  ObDfo* edge = NULL;
  ObPxSqcMeta* sqc = NULL;
  LOG_TRACE("terminate msg proc on sqc init msg", K(pkt.rc_));
  if (pkt.task_count_ <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task count returned by sqc invalid. expect 1 or more", K(pkt), K(ret));
  } else if (OB_FAIL(coord_info_.dfo_mgr_.find_dfo_edge(pkt.dfo_id_, edge))) {
    LOG_WARN("fail find dfo", K(pkt), K(ret));
  } else if (OB_ISNULL(edge)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", KP(edge), K(ret));
  } else if (OB_FAIL(edge->get_sqc(pkt.sqc_id_, sqc))) {
    LOG_WARN("fail find sqc", K(pkt), K(ret));
  } else if (OB_ISNULL(sqc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", KP(sqc), K(ret));
  } else {
    sqc->set_task_count(pkt.task_count_);
    sqc->set_thread_inited(true);

    if (pkt.rc_ != OB_SUCCESS) {
      LOG_DEBUG("receive error code from sqc init msg", K(coord_info_.first_error_code_), K(pkt.rc_));
    }
    update_error_code(coord_info_.first_error_code_, pkt.rc_);
  }

  if (OB_SUCC(ret)) {
    ObArray<ObPxSqcMeta*> sqcs;
    if (OB_FAIL(edge->get_sqcs(sqcs))) {
      LOG_WARN("fail get qc-sqc channel for QC", K(ret));
    } else {
      bool sqc_threads_inited = true;
      ARRAY_FOREACH_X(sqcs, idx, cnt, OB_SUCC(ret))
      {
        ObPxSqcMeta* sqc = sqcs.at(idx);
        if (OB_ISNULL(sqc)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL unexpected sqc", K(ret));
        } else if (!sqc->is_thread_inited()) {
          sqc_threads_inited = false;
          break;
        }
      }
      if (OB_SUCC(ret) && sqc_threads_inited) {
        LOG_TRACE("sqc terminate msg: all sqc returned task count. ready to do on_sqc_threads_inited", K(*edge));
        edge->set_thread_inited(true);
      }
    }
  }

  return ret;
}

int ObPxTerminateMsgProc::on_sqc_finish_msg(ObExecContext& ctx, const ObPxFinishSqcResultMsg& pkt)
{
  int ret = OB_SUCCESS;
  LOG_TRACE("terminate msg : proc on sqc finish msg", K(pkt.rc_));
  ObDfo* edge = NULL;
  ObPxSqcMeta* sqc = NULL;
  UNUSED(ctx);
  if (OB_FAIL(coord_info_.dfo_mgr_.find_dfo_edge(pkt.dfo_id_, edge))) {
    LOG_WARN("fail find dfo", K(pkt), K(ret));
  } else if (OB_FAIL(edge->get_sqc(pkt.sqc_id_, sqc))) {
    LOG_WARN("fail find sqc", K(pkt), K(ret));
  } else if (OB_ISNULL(edge) || OB_ISNULL(sqc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", KP(edge), KP(sqc), K(ret));
  } else if (FALSE_IT(sqc->set_need_report(false))) {
  } else if (OB_FAIL(sqc->set_task_monitor_info_array(pkt.task_monitor_info_array_))) {
    LOG_WARN("fail to copy task monitor info array", K(ret));
  } else {
    sqc->set_thread_finish(true);
    if (pkt.rc_ != OB_SUCCESS) {
      LOG_DEBUG("receive error code from sqc finish msg", K(coord_info_.first_error_code_), K(pkt.rc_));
    }
    update_error_code(coord_info_.first_error_code_, pkt.rc_);

    NG_TRACE_EXT(sqc_finish, OB_ID(dfo_id), sqc->get_dfo_id(), OB_ID(sqc_id), sqc->get_sqc_id());

    LOG_TRACE("terminate msg : sqc finish", K(*edge), K(*sqc));
  }

  if (OB_SUCC(ret)) {
    ObArray<ObPxSqcMeta*> sqcs;
    if (OB_FAIL(edge->get_sqcs(sqcs))) {
      LOG_WARN("fail get qc-sqc channel for QC", K(ret));
    } else {
      bool sqc_threads_finish = true;
      int64_t dfo_used_worker_count = 0;
      ARRAY_FOREACH_X(sqcs, idx, cnt, OB_SUCC(ret))
      {
        ObPxSqcMeta* sqc = sqcs.at(idx);
        if (OB_ISNULL(sqc)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL unexpected sqc", K(ret));
        } else if (!sqc->is_thread_finish()) {
          sqc_threads_finish = false;
          break;
        } else {
          // the value 1 is accounted for sqc thread
          dfo_used_worker_count += sqc->get_task_count();
        }
      }
      if (OB_SUCC(ret) && sqc_threads_finish) {
        edge->set_thread_finish(true);
        edge->set_used_worker_count(dfo_used_worker_count);
        LOG_TRACE("terminate msg : dfo finish", K(*edge));
      }
    }
  }

  return ret;
}

int ObPxTerminateMsgProc::on_eof_row(ObExecContext& ctx)
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  LOG_WARN("terminate msg proc on sqc eof msg", K(ret));
  return ret;
}

int ObPxTerminateMsgProc::on_sqc_init_fail(ObDfo& dfo, ObPxSqcMeta& sqc)
{
  int ret = OB_SUCCESS;
  UNUSED(dfo);
  UNUSED(sqc);
  LOG_WARN("terminate msg proc on sqc init fail", K(ret));
  return ret;
}

int ObPxTerminateMsgProc::on_interrupted(ObExecContext& ctx, const common::ObInterruptCode& pkt)
{
  int ret = OB_SUCCESS;
  UNUSED(pkt);
  UNUSED(ctx);
  LOG_WARN("terminate msg proc on sqc interrupted", K(ret));
  return ret;
}

int ObPxTerminateMsgProc::startup_msg_loop(ObExecContext& ctx)
{
  int ret = OB_ERR_UNEXPECTED;
  UNUSED(ctx);
  LOG_WARN("terminate msg proc on sqc startup loop", K(ret));
  return ret;
}

int ObPxTerminateMsgProc::on_piece_msg(ObExecContext& ctx, const ObBarrierPieceMsg& pkt)
{
  int ret = common::OB_SUCCESS;
  UNUSED(ctx);
  UNUSED(pkt);
  return ret;
}

int ObPxTerminateMsgProc::on_piece_msg(ObExecContext& ctx, const ObWinbufPieceMsg& pkt)
{
  int ret = common::OB_SUCCESS;
  UNUSED(ctx);
  UNUSED(pkt);
  return ret;
}

}  // end namespace sql
}  // end namespace oceanbase
