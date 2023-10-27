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
#include "sql/engine/px/ob_dfo_mgr.h"
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
#include "sql/engine/px/datahub/components/ob_dh_rollup_key.h"
#include "sql/engine/px/datahub/components/ob_dh_winbuf.h"
#include "sql/engine/px/datahub/components/ob_dh_sample.h"
#include "sql/engine/px/ob_px_sqc_proxy.h"
#include "storage/tx/ob_trans_service.h"
#include "share/detect/ob_detect_manager_utils.h"
#include "sql/engine/join/ob_join_filter_op.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace omt;
using namespace share::schema;
using namespace sql;
using namespace sql::dtl;
namespace sql
{


// 仅用于本 cpp 文件，所以可以放在这里
// 专用于处理 datahub piece 消息的逻辑
template <typename PieceMsg>
class ObDhPieceMsgProc
{
public:
  ObDhPieceMsgProc() = default;
  ~ObDhPieceMsgProc() = default;
  int on_piece_msg(ObPxCoordInfo &coord_info, ObExecContext &ctx, const PieceMsg &pkt)
  {
    int ret = OB_SUCCESS;
    ObArray<ObPxSqcMeta *> sqcs;
    ObDfo *source_dfo = nullptr;
    ObDfo *target_dfo = nullptr;
    ObPieceMsgCtx *piece_ctx = nullptr;
    ObDfo *child_dfo = nullptr;
    // FIXME (TODO xiaochu)：这个 dfo id 不是必须的，本地可以维护一个 op_id 到 dfo id 的映射
    if (OB_FAIL(coord_info.dfo_mgr_.find_dfo_edge(pkt.source_dfo_id_, source_dfo))) {
      LOG_WARN("fail find dfo", K(pkt), K(ret));
    } else if (OB_FAIL(coord_info.dfo_mgr_.find_dfo_edge(pkt.target_dfo_id_, target_dfo))) {
      LOG_WARN("fail find dfo", K(pkt), K(ret));
    } else if (OB_ISNULL(source_dfo) || OB_ISNULL(target_dfo)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr or null session ptr", KP(source_dfo), KP(target_dfo), K(pkt), K(ret));
    } else if (OB_FAIL(coord_info.piece_msg_ctx_mgr_.find_piece_ctx(pkt.op_id_, pkt.type(), piece_ctx))) {
      // 如果找不到则创建一个 ctx
      // NOTE: 这里新建一个 piece_ctx 的方式不会出现并发问题，
      // 因为 QC 是单线程消息循环，逐个处理 SQC 发来的消息
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("fail get ctx", K(pkt), K(ret));
      } else if (OB_FAIL(PieceMsg::PieceMsgCtx::alloc_piece_msg_ctx(pkt, coord_info, ctx,
            source_dfo->get_total_task_count(), piece_ctx))) {
        LOG_WARN("fail to alloc piece msg", K(ret));
      } else if (nullptr != piece_ctx) {
        if (OB_FAIL(coord_info.piece_msg_ctx_mgr_.add_piece_ctx(piece_ctx, pkt.type()))) {
          LOG_WARN("fail add barrier piece ctx", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      typename PieceMsg::PieceMsgCtx *ctx = static_cast<typename PieceMsg::PieceMsgCtx *>(piece_ctx);
      if (OB_FAIL(target_dfo->get_sqcs(sqcs))) {
        LOG_WARN("fail get qc-sqc channel for QC", K(ret));
      } else if (OB_FAIL(PieceMsg::PieceMsgListener::on_message(*ctx, sqcs, pkt))) {
        LOG_WARN("fail process piece msg", K(pkt), K(ret));
      }
    }
    return ret;
  }
};

int ObPxMsgProc::on_process_end(ObExecContext &ctx)
{
  UNUSED(ctx);
  int ret = OB_SUCCESS;
  // 处理扫尾工作
  return ret;
}

// entry function
// 调度入口函数
int ObPxMsgProc::startup_msg_loop(ObExecContext &ctx)
{
  int ret = OB_SUCCESS;
  LOG_TRACE("TIMERECORD ",
            "reserve:=-1 name:=QC dfoid:=-1 sqcid:=-1 taskid:=-1 start:",
            ObTimeUtility::current_time());
  if (OB_FAIL(scheduler_->prepare_schedule_info(ctx))) {
    LOG_WARN("fail to prepare schedule info", K(ret));
  } else if (OB_FAIL(scheduler_->init_all_dfo_channel(ctx))) {
    LOG_WARN("fail to init all dfo channel", K(ret));
  } else if (OB_FAIL(scheduler_->try_schedule_next_dfo(ctx))) {
    LOG_WARN("fail to sched next one dfo", K(ret));
  }
  return ret;
}

void ObPxMsgProc::clean_dtl_interm_result(ObExecContext &ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(scheduler_)) {
    LOG_WARN("dfo scheduler is null");
  } else {
    scheduler_->clean_dtl_interm_result(ctx);
  }
}

// 1. 根据 pkt 信息找到对应 dfo, sqc，标记当前 sqc 线程分配完成
// 2. 判断该 dfo 下是否所有 sqc 都分配线程完成
//    如果完成，则标记 dfo 为 thread_inited, 进入第 3 步，否则结束处理
// 3. 判断当前 dfo 的 parent 是否处于 thread_inited 状态，
//    如果是:
//       - 调用 on_dfo_pair_thread_inited 来触发 两个 dfo 的 channel 配对和分发，
//    否则:
//       - 判断当前 dfo 的**任意** child 是否有处于 thread_inited 状态，
//          - 如果是，则调用 on_dfo_pair_thread_inited 来触发 两个 dfo 的 channel 配对和分发
//          - 否则 nop
int ObPxMsgProc::on_sqc_init_msg(ObExecContext &ctx, const ObPxInitSqcResultMsg &pkt)
{
  int ret = OB_SUCCESS;

  LOG_TRACE("on_sqc_init_msg", K(pkt));

  ObDfo *edge = NULL;
  ObPxSqcMeta *sqc = NULL;
  if (OB_FAIL(coord_info_.dfo_mgr_.find_dfo_edge(pkt.dfo_id_, edge))) {
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
    if (OB_SUCCESS != pkt.rc_) {
      ret = pkt.rc_;
      ObPxErrorUtil::update_qc_error_code(coord_info_.first_error_code_, pkt.rc_, pkt.err_msg_);
      LOG_WARN("fail init sqc, please check remote server log for details",
               "remote_server", sqc->get_exec_addr(), K(pkt), KP(ret));
    } else if (pkt.task_count_ <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("task count returned by sqc invalid. expect 1 or more", K(pkt), K(ret));
    } else if (OB_FAIL(sqc->get_partitions_info().assign(pkt.tablets_info_))) {
      LOG_WARN("Failed to assign partitions info", K(ret));
    } else {
      sqc->set_task_count(pkt.task_count_);
      sqc->set_thread_inited(true);
      sqc->set_sqc_order_gi_tasks(pkt.sqc_order_gi_tasks_);
      LOG_TRACE("set sqc support_order_gi_tasks", K(sqc->get_dfo_id()), K(sqc->get_sqc_id()),
                K(pkt.sqc_order_gi_tasks_));
    }
  }

  if (OB_SUCC(ret)) {
    ObArray<ObPxSqcMeta *> sqcs;
    if (OB_FAIL(edge->get_sqcs(sqcs))) {
      LOG_WARN("fail get qc-sqc channel for QC", K(ret));
    } else {
      bool sqc_threads_inited = true;
      ARRAY_FOREACH_X(sqcs, idx, cnt, OB_SUCC(ret)) {
        ObPxSqcMeta *sqc = sqcs.at(idx);
        if (OB_ISNULL(sqc)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL unexpected sqc", K(ret));
        } else if (!sqc->is_thread_inited()) {
          sqc_threads_inited = false;
          break;
        }
      }
      if (OB_SUCC(ret) && sqc_threads_inited) {
        LOG_TRACE("on_sqc_init_msg: all sqc returned task count. ready to do on_sqc_threads_inited",
                  K(*edge));
        edge->set_thread_inited(true);
        ret = scheduler_->on_sqc_threads_inited(ctx, *edge);
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (edge->is_thread_inited()) {

      /* 要同时尝试通知 parent 和 child，考虑情况：
       *
       *  parent (thread inited)
       *    |
       *   self
       *    |
       *  child  (thread inited)
       *
       *  在这个情况下，self 线程被全部调度起来之前，
       *  child 和 parent 的 thread inited 消息都无法
       *  触发 on_dfo_pair_thread_inited 事件。
       *  self 的 thread inited 之后，必须同时触发
       *  同 parent 和 child 的 on_dfo_pair_thread_inited 事件
       */

      // 尝试调度 self-parent 对
      if (edge->has_parent() && edge->parent()->is_thread_inited()) {
        if (OB_FAIL(on_dfo_pair_thread_inited(ctx, *edge, *edge->parent()))) {
          LOG_WARN("fail co-schedule parent-edge", K(ret));
        }
      }

      // 尝试调度 self-child 对
      if (OB_SUCC(ret)) {
        int64_t cnt = edge->get_child_count();
        for (int64_t idx = 0; idx < cnt && OB_SUCC(ret); ++idx) {
          ObDfo *child= NULL;
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

// 1. 根据 pkt 信息找到 dfo, sqc，标记当前 sqc 已经执行完成
// 2. 判断该 dfo 下是否所有 sqc 都执行完成
//    如果完成，则标记 dfo 为 thread_finish
// 3. 判断当前 dfo 被标记成为 thread_finish 状态，
//    如果是：
//      - 给 dfo 发送释放线程消息
//      - 调度下一个 dfo
//        - 如果所有 dfo 都已调度完成 (不考虑 Coord)，nop
//    否则：
//      - nop
int ObPxMsgProc::on_sqc_finish_msg(ObExecContext &ctx,
                                   const ObPxFinishSqcResultMsg &pkt)
{
  int ret = OB_SUCCESS;
  ObDfo *edge = NULL;
  ObPxSqcMeta *sqc = NULL;
  ObSQLSessionInfo *session = NULL;
  ObPhysicalPlanCtx *phy_plan_ctx = NULL;
  if (OB_ISNULL(session = ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr session", K(ret));
  } else if (OB_ISNULL(phy_plan_ctx = GET_PHY_PLAN_CTX(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("phy plan ctx NULL", K(ret));
  } else if (OB_FAIL(ctx.get_feedback_info().merge_feedback_info(pkt.fb_info_))) {
    LOG_WARN("fail to merge feedback info", K(ret));
  } else if (OB_ISNULL(session->get_tx_desc())) {
  } else if (OB_FAIL(MTL(transaction::ObTransService*)
                     ->add_tx_exec_result(*session->get_tx_desc(),
                                          pkt.get_trans_result()))) {
    LOG_WARN("fail merge result", K(ret),
             "packet_trans_result", pkt.get_trans_result(),
             "tx_desc", *session->get_tx_desc());
  } else {
    LOG_TRACE("on_sqc_finish_msg trans_result",
              "packet_trans_result", pkt.get_trans_result(),
              "tx_desc", *session->get_tx_desc());
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
  } else if (common::OB_INVALID_ID != pkt.temp_table_id_) {
    if (OB_FAIL(ctx.add_temp_table_interm_result_ids(pkt.temp_table_id_,
                                                     sqc->get_exec_addr(),
                                                     pkt.interm_result_ids_))) {
      LOG_WARN("failed to add temp table interm result ids.", K(ret));
    } else { /*do nothing.*/ }
  } else { /*do nothing.*/ }
  if (OB_SUCC(ret)) {
    if (OB_NOT_NULL(edge->get_detect_cb())) {
#ifdef ERRSIM
      if (OB_FAIL(OB_E(EventTable::EN_PX_SLOW_PROCESS_SQC_FINISH_MSG) OB_SUCCESS)) {
        LOG_WARN("qc slow process sqc finish msg by desgin", K(ret));
        usleep(100 * 1000L);
        ret = OB_SUCCESS;
      }
#endif
      int set_finish_ret = edge->get_detect_cb()->atomic_set_finished(sqc->get_sqc_addr());
      if (OB_SUCCESS != set_finish_ret) {
        LOG_WARN("[DM] failed to atomic_set_finished", K(set_finish_ret), K(sqc->get_sqc_addr()));
      }
    }
    sqc->set_thread_finish(true);
    if (sqc->is_ignore_vtable_error() && OB_SUCCESS != pkt.rc_
        && ObVirtualTableErrorWhitelist::should_ignore_vtable_error(pkt.rc_)) {
       // 如果收到一个sqc finish消息, 如果该sqc涉及虚拟表, 需要忽略所有错误码
       // 如果该dfo是root_dfo的child_dfo, 为了让px走出数据channel的消息循环
       // 需要mock一个eof dtl buffer本地发送至px(实际未经过rpc, attach即可)
      const_cast<ObPxFinishSqcResultMsg &>(pkt).rc_ = OB_SUCCESS;
      OZ(root_dfo_action_.notify_peers_mock_eof(edge,
          phy_plan_ctx->get_timeout_timestamp(),
          sqc->get_exec_addr()));
    }
    NG_TRACE_EXT(sqc_finish,
                 OB_ID(dfo_id), sqc->get_dfo_id(),
                 OB_ID(sqc_id), sqc->get_sqc_id());

    LOG_TRACE("[MSG] sqc finish", K(*edge), K(*sqc));
    LOG_TRACE("on_sqc_finish_msg update feedback info",
        K(pkt.fb_info_), K(ctx.get_feedback_info()));
  }

  if (OB_SUCC(ret)) {
    ObArray<ObPxSqcMeta *> sqcs;
    if (OB_FAIL(edge->get_sqcs(sqcs))) {
      LOG_WARN("fail get qc-sqc channel for QC", K(ret));
    } else {
      bool sqc_threads_finish = true;
      int64_t dfo_used_worker_count = 0;
      ARRAY_FOREACH_X(sqcs, idx, cnt, OB_SUCC(ret)) {
        ObPxSqcMeta *sqc = sqcs.at(idx);
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
   * 为什么在这里判断错误码？因为需要将这个出错的sqc以及dfo的状态更新，
   * 发送这个finish消息的sqc（包括它的worker）其实已经结束了，需要将它
   * 但是因为出错了，后续的调度流程不需要继续了，后面流程会进行错误处理。
   */
  ObPxErrorUtil::update_qc_error_code(coord_info_.first_error_code_, pkt.rc_, pkt.err_msg_);
  if (OB_SUCC(ret)) {
    if (OB_FAIL(pkt.rc_)) {
      DAS_CTX(ctx).get_location_router().save_cur_exec_status(pkt.rc_);
      LOG_WARN("sqc fail, abort qc", K(pkt), K(ret), "sqc_addr", sqc->get_exec_addr());
    } else {
      // pkt rc_ == OB_SUCCESS
      // 处理 dml + px 框架下的affected row
      DAS_CTX(ctx).get_location_router().save_cur_exec_status(pkt.das_retry_rc_);
      if (OB_ISNULL(ctx.get_physical_plan_ctx())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("phy plan ctx is null", K(ret));
      } else  {
        ctx.get_physical_plan_ctx()->add_affected_rows(pkt.sqc_affected_rows_);
        ctx.get_physical_plan_ctx()->add_px_dml_row_info(pkt.dml_row_info_);
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (edge->is_thread_finish()) {
      if (OB_NOT_NULL(ctx.get_physical_plan_ctx()->get_phy_plan()) &&
          ctx.get_physical_plan_ctx()->get_phy_plan()->is_enable_px_fast_reclaim()) {
        (void)ObDetectManagerUtils::qc_unregister_check_item_from_dm(edge);
      }
      ret = scheduler_->try_schedule_next_dfo(ctx);
      if (OB_ITER_END == ret) {
        coord_info_.all_threads_finish_ = true;
        LOG_TRACE("TIMERECORD ",
                  "reserve:=-1 name:=QC dfoid:=-1 sqcid:=-1 taskid:=-1 end:",
                  ObTimeUtility::current_time());
        ret = OB_SUCCESS; // 需要覆盖，否则无法跳出 loop
      }
    }
  }

  return ret;
}

int ObPxMsgProc::on_piece_msg(
    ObExecContext &ctx,
    const ObBarrierPieceMsg &pkt)
{
  ObDhPieceMsgProc<ObBarrierPieceMsg> proc;
  return proc.on_piece_msg(coord_info_, ctx, pkt);
}

int ObPxMsgProc::on_piece_msg(
    ObExecContext &ctx,
    const ObWinbufPieceMsg &pkt)
{
  ObDhPieceMsgProc<ObWinbufPieceMsg> proc;
  return proc.on_piece_msg(coord_info_, ctx, pkt);
}

int ObPxMsgProc::on_piece_msg(
    ObExecContext &ctx,
    const ObDynamicSamplePieceMsg &pkt)
{
  ObDhPieceMsgProc<ObDynamicSamplePieceMsg> proc;
  return proc.on_piece_msg(coord_info_, ctx, pkt);
}

int ObPxMsgProc::on_piece_msg(
    ObExecContext &ctx,
    const ObRollupKeyPieceMsg &pkt)
{
  ObDhPieceMsgProc<ObRollupKeyPieceMsg> proc;
  return proc.on_piece_msg(coord_info_, ctx, pkt);
}

int ObPxMsgProc::on_piece_msg(
    ObExecContext &ctx,
    const ObRDWFPieceMsg &pkt)
{
  ObDhPieceMsgProc<ObRDWFPieceMsg> proc;
  return proc.on_piece_msg(coord_info_, ctx, pkt);
}

int ObPxMsgProc::on_piece_msg(
    ObExecContext &ctx,
    const ObInitChannelPieceMsg &pkt)
{
  ObDhPieceMsgProc<ObInitChannelPieceMsg> proc;
  return proc.on_piece_msg(coord_info_, ctx, pkt);
}

int ObPxMsgProc::on_piece_msg(
    ObExecContext &ctx,
    const ObReportingWFPieceMsg &pkt)
{
  ObDhPieceMsgProc<ObReportingWFPieceMsg> proc;
  return proc.on_piece_msg(coord_info_, ctx, pkt);
}

int ObPxMsgProc::on_piece_msg(
    ObExecContext &ctx,
    const ObOptStatsGatherPieceMsg &pkt)
{
  ObDhPieceMsgProc<ObOptStatsGatherPieceMsg> proc;
  return proc.on_piece_msg(coord_info_, ctx, pkt);
}

int ObPxMsgProc::on_eof_row(ObExecContext &ctx)
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  // 1. mark some channel as eof
  // 2. see if all PX data channel eof, if yes, return OB_ITER_END
  return ret;
}

int ObPxMsgProc::on_dfo_pair_thread_inited(ObExecContext &ctx, ObDfo &child, ObDfo &parent)
{
  int ret = OB_SUCCESS;
  // 位置分配好了，建立 DTL 映射
  //
  // NOTE: 这里暂时简化实现，如果 child/parent 的线程分配失败
  // 则失败退出。更细致的实现里，可以修改不同 server 上线程分配数量，
  // 然后重新尝试分配线程。在这种情形下， DTL Map 也要跟着变化
  //
  if (OB_SUCC(ret)) {
    if (OB_FAIL(scheduler_->build_data_xchg_ch(ctx, child, parent))) {
      LOG_WARN("fail init dtl data channel", K(ret));
    } else {
      LOG_TRACE("build data xchange channel for dfo pair ok", K(parent), K(child));
    }
  }
  // 将 dtl 通道信息分发给 parent， child 两个 DFO，使得它们能够开始收发数据
  if (OB_SUCC(ret)) {
    if (OB_FAIL(scheduler_->dispatch_dtl_data_channel_info(ctx, child, parent))) {
      LOG_WARN("fail setup dtl data channel for child-parent pair", K(ret));
    } else {
      LOG_TRACE("dispatch dtl data channel for pair ok", K(parent), K(child));
    }
  }

  return ret;
}

int ObPxMsgProc::on_interrupted(ObExecContext &ctx, const ObInterruptCode &ic)
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  // override ret code
  // 抛出错误码到主处理路程
  ret = ic.code_;
  LOG_TRACE("qc received a interrupt and throw out of msg proc", K(ic), K(ret));
  return ret;
}

// TODO
int ObPxMsgProc::on_sqc_init_fail(ObDfo &dfo, ObPxSqcMeta &sqc)
{
  int ret = OB_SUCCESS;
  UNUSED(dfo);
  UNUSED(sqc);
  return ret;
}

int ObPxTerminateMsgProc::on_sqc_init_msg(ObExecContext &ctx, const ObPxInitSqcResultMsg &pkt)
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  ObDfo *edge = NULL;
  ObPxSqcMeta *sqc = NULL;
  /**
   * 标记sqc，dfo已经为启动状态。
   */
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
    // 标记sqc已经完整启动了
    sqc->set_thread_inited(true);

    if (pkt.rc_ != OB_SUCCESS) {
      LOG_DEBUG("receive error code from sqc init msg", K(coord_info_.first_error_code_), K(pkt.rc_));
    }
    ObPxErrorUtil::update_qc_error_code(coord_info_.first_error_code_, pkt.rc_, pkt.err_msg_);
  }

  if (OB_SUCC(ret)) {
    ObArray<ObPxSqcMeta *> sqcs;
    if (OB_FAIL(edge->get_sqcs(sqcs))) {
      LOG_WARN("fail get qc-sqc channel for QC", K(ret));
    } else {
      bool sqc_threads_inited = true;
      ARRAY_FOREACH_X(sqcs, idx, cnt, OB_SUCC(ret)) {
        ObPxSqcMeta *sqc = sqcs.at(idx);
        if (OB_ISNULL(sqc)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL unexpected sqc", K(ret));
        } else if (!sqc->is_thread_inited()) {
          sqc_threads_inited = false;
          break;
        }
      }
      if (OB_SUCC(ret) && sqc_threads_inited) {
        LOG_TRACE("sqc terminate msg: all sqc returned task count. ready to do on_sqc_threads_inited",
                  K(*edge));
        // 标记dfo已经完整启动了
        edge->set_thread_inited(true);
      }
    }
  }

  return ret;
}

int ObPxTerminateMsgProc::on_sqc_finish_msg(ObExecContext &ctx, const ObPxFinishSqcResultMsg &pkt)
{
  int ret = OB_SUCCESS;
  LOG_TRACE("terminate msg : proc on sqc finish msg", K(pkt.rc_));
  ObDfo *edge = NULL;
  ObPxSqcMeta *sqc = NULL;
  ObSQLSessionInfo *session = NULL;
  if (OB_ISNULL(session = ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr session", K(ret));
  } else if (OB_FAIL(ctx.get_feedback_info().merge_feedback_info(pkt.fb_info_))) {
    LOG_WARN("fail to merge feedback info", K(ret));
  } else if (OB_ISNULL(session->get_tx_desc())) {
  } else if (OB_FAIL(MTL(transaction::ObTransService*)
                     ->add_tx_exec_result(*session->get_tx_desc(),
                                          pkt.get_trans_result()))) {
    LOG_WARN("fail report tx result", K(ret),
             "packet_trans_result", pkt.get_trans_result(),
             "tx_desc", *session->get_tx_desc());
  } else {
    LOG_TRACE("on_sqc_finish_msg trans_result",
              "packet_trans_result", pkt.get_trans_result(),
              "tx_desc", *session->get_tx_desc());
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
  } else {
    sqc->set_thread_finish(true);
    if (pkt.rc_ != OB_SUCCESS) {
      LOG_DEBUG("receive error code from sqc finish msg", K(coord_info_.first_error_code_), K(pkt.rc_));
    }
    ObPxErrorUtil::update_qc_error_code(coord_info_.first_error_code_, pkt.rc_, pkt.err_msg_);

    NG_TRACE_EXT(sqc_finish,
                 OB_ID(dfo_id), sqc->get_dfo_id(),
                 OB_ID(sqc_id), sqc->get_sqc_id());

    LOG_TRACE("terminate msg : sqc finish", K(*edge), K(*sqc));
    LOG_TRACE("on_sqc_finish_msg update feedback info",
        K(pkt.fb_info_), K(ctx.get_feedback_info()));
  }

  if (OB_SUCC(ret)) {
    ObArray<ObPxSqcMeta *> sqcs;
    if (OB_FAIL(edge->get_sqcs(sqcs))) {
      LOG_WARN("fail get qc-sqc channel for QC", K(ret));
    } else {
      bool sqc_threads_finish = true;
      int64_t dfo_used_worker_count = 0;
      ARRAY_FOREACH_X(sqcs, idx, cnt, OB_SUCC(ret)) {
        ObPxSqcMeta *sqc = sqcs.at(idx);
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

int ObPxTerminateMsgProc::on_eof_row(ObExecContext &ctx)
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  LOG_WARN("terminate msg proc on sqc eof msg", K(ret));
  return ret;
}

int ObPxTerminateMsgProc::on_sqc_init_fail(ObDfo &dfo, ObPxSqcMeta &sqc)
{
  int ret = OB_SUCCESS;
  UNUSED(dfo);
  UNUSED(sqc);
  LOG_WARN("terminate msg proc on sqc init fail", K(ret));
  return ret;
}

int ObPxTerminateMsgProc::on_interrupted(ObExecContext &ctx, const common::ObInterruptCode &pkt)
{
  int ret = OB_SUCCESS;
  UNUSED(pkt);
  UNUSED(ctx);
  // 已经是在回收流程了，对中断不再响应.
  LOG_WARN("terminate msg proc on sqc interrupted", K(ret));
  return ret;
}

int ObPxTerminateMsgProc::startup_msg_loop(ObExecContext &ctx)
{
  // 一个dfo都没有掉，在上层就直接返回了，不应该到这里。
  int ret = OB_ERR_UNEXPECTED;
  UNUSED(ctx);
  LOG_WARN("terminate msg proc on sqc startup loop", K(ret));
  return ret;
}

int RuntimeFilterDependencyInfo::describe_dependency(ObDfo *root_dfo)
{
  int ret = OB_SUCCESS;
  // for each rf create op, find its pair rf use op,
  // then get the lowest common ancestor of them, mark force_bushy of the dfo which the ancestor belongs to.
  for (int64_t i = 0; i < rf_create_ops_.count() && OB_SUCC(ret); ++i) {
    const ObJoinFilterSpec *create_op = static_cast<const ObJoinFilterSpec *>(rf_create_ops_.at(i));
    for (int64_t j = 0; j < rf_use_ops_.count() && OB_SUCC(ret); ++j) {
      int64_t use_filter_id = common::OB_INVALID_ID;
      if (IS_PX_GI(rf_use_ops_.at(j)->get_type())) {
        const ObGranuleIteratorSpec *use_op =
            static_cast<const ObGranuleIteratorSpec *>(rf_use_ops_.at(j));
        use_filter_id = use_op->bf_info_.filter_id_;
      } else {
        const ObJoinFilterSpec *use_op = static_cast<const ObJoinFilterSpec *>(rf_use_ops_.at(j));
        use_filter_id = use_op->get_filter_id();
      }
      if (create_op->get_filter_id() == use_filter_id) {
        const ObOpSpec *ancestor_op = nullptr;
        ObDfo *op_dfo = nullptr;;
        if (OB_FAIL(LowestCommonAncestorFinder::find_op_common_ancestor(
            create_op, rf_use_ops_.at(j), ancestor_op))) {
          LOG_WARN("failed to find op common ancestor");
        } else if (OB_ISNULL(ancestor_op)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("op common ancestor not found");
        } else if (OB_FAIL(LowestCommonAncestorFinder::get_op_dfo(ancestor_op, root_dfo, op_dfo))) {
          LOG_WARN("failed to find op common ancestor");
        } else if (OB_ISNULL(op_dfo)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("the dfo of ancestor_op not found");
        } else {
          // Once the DFO which the ancestor belongs to has set the flag "force_bushy",
          // the DfoTreeNormalizer will not attempt to transform a right-deep DFO tree
          // into a left-deep DFO tree. Consequently, the "join filter create" operator
          // can be scheduled earlier than the "join filter use" operator.
          op_dfo->set_force_bushy(true);
        }
        break;
      }
    }
  }
  return ret;
}

int ObPxCoordInfo::init()
{
  int ret = OB_SUCCESS;
  const int64_t bucket_num = 32;
  if (OB_FAIL(p2p_dfo_map_.create(bucket_num,
      "PxDfoMapKey",
      "PxDfoMapNode"))) {
    LOG_WARN("create hash table failed", K(ret));
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase
