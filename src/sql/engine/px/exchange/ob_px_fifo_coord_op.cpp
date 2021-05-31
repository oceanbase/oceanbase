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

#include "ob_px_fifo_coord_op.h"
#include "share/ob_rpc_share.h"
#include "share/schema/ob_part_mgr_util.h"
#include "sql/engine/px/ob_px_util.h"
#include "sql/dtl/ob_dtl_channel_group.h"
#include "sql/dtl/ob_dtl_channel_loop.h"
#include "sql/dtl/ob_dtl_msg_type.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/px/ob_px_dtl_msg.h"
#include "sql/engine/px/ob_px_dtl_proc.h"
#include "sql/engine/px/ob_px_util.h"

namespace oceanbase {
using namespace common;
using namespace sql;
using namespace sql::dtl;
namespace sql {

OB_SERIALIZE_MEMBER((ObPxFifoCoordOpInput, ObPxReceiveOpInput));

OB_SERIALIZE_MEMBER((ObPxFifoCoordSpec, ObPxCoordSpec));

ObPxFifoCoordOp::ObPxFifoCoordOp(ObExecContext& exec_ctx, const ObOpSpec& spec, ObOpInput* input)
    : ObPxCoordOp(exec_ctx, spec, input),
      serial_scheduler_(coord_info_, *this, listener_),
      parallel_scheduler_(coord_info_, *this, listener_, msg_proc_),
      msg_proc_(coord_info_, listener_, *this),
      sqc_finish_msg_proc_(exec_ctx, msg_proc_),
      sqc_init_msg_proc_(exec_ctx, msg_proc_),
      barrier_piece_msg_proc_(exec_ctx, msg_proc_),
      winbuf_piece_msg_proc_(exec_ctx, msg_proc_),
      interrupt_proc_(exec_ctx, msg_proc_)
{}

int ObPxFifoCoordOp::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObPxCoordOp::inner_open())) {
    LOG_WARN("fail open op", K(MY_SPEC.id_), K(ret));
  } else if (OB_FAIL(setup_loop_proc())) {
    LOG_WARN("fail setup loop proc", K(ret));
  } else {
    if (1 == px_dop_) {
      msg_proc_.set_scheduler(&serial_scheduler_);
    } else {
      msg_proc_.set_scheduler(&parallel_scheduler_);
    }
    metric_.set_id(MY_SPEC.id_);
  }
  return ret;
}

int ObPxFifoCoordOp::inner_close()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObPxCoordOp::inner_close())) {
    LOG_WARN("fail close op", K(MY_SPEC.id_), K(ret));
  }
  return ret;
}

int ObPxFifoCoordOp::setup_loop_proc()
{
  int ret = OB_SUCCESS;
  msg_loop_.clear_all_proc();
  (void)msg_loop_.register_processor(px_row_msg_proc_)
      .register_processor(sqc_init_msg_proc_)
      .register_processor(sqc_finish_msg_proc_)
      .register_processor(barrier_piece_msg_proc_)
      .register_processor(winbuf_piece_msg_proc_)
      .register_interrupt_processor(interrupt_proc_);
  return ret;
}

int ObPxFifoCoordOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* phy_plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  if (iter_end_) {
    ret = OB_ITER_END;
  } else if (OB_UNLIKELY(!first_row_fetched_)) {
    if (OB_FAIL(msg_proc_.startup_msg_loop(ctx_))) {
      LOG_WARN("initial dfos NOT dispatched successfully", K(ret));
    }
    msg_loop_.set_tenant_id(ctx_.get_my_session()->get_effective_tenant_id());
    first_row_fetched_ = true;
  }

  bool wait_next_msg = true;
  while (OB_SUCC(ret) && wait_next_msg) {
    // SQC-QC control channel and TASKs-QC data channel are registered in loop.
    // In order to receive orderlym TASKs-QC channel should be added in loop one by one.
    int64_t timeout_us = 0;
    clear_evaluated_flag();
    if (FALSE_IT(timeout_us = phy_plan_ctx->get_timeout_timestamp() - get_timestamp())) {
    } else if (OB_FAIL(THIS_WORKER.check_status())) {
      LOG_WARN("fail check status, maybe px query timeout", K(ret));
      // TODO: cleanup
    } else if (OB_FAIL(msg_loop_.process_one(timeout_us))) {
      if (OB_EAGAIN == ret) {
        LOG_TRACE("no message, try again", K(ret));
        ret = OB_SUCCESS;
      } else if (OB_ITER_END != ret) {
        LOG_WARN("fail process message", K(ret));
      }
    } else {
      ObDtlMsgType msg_type = msg_loop_.get_last_msg_type();
      switch (msg_type) {
        case ObDtlMsgType::PX_NEW_ROW:
          // got one row, return
          ret = next_row(wait_next_msg);
          if (!first_row_sent_) {
            first_row_sent_ = true;
            LOG_TRACE("TIMERECORD ",
                "reserve:=1 name:=RQC dfoid:=-1 sqcid:=-1 taskid:=-1 start:",
                ObTimeUtility::current_time());
          }
          break;
        case ObDtlMsgType::INIT_SQC_RESULT:
        case ObDtlMsgType::FINISH_SQC_RESULT:
        case ObDtlMsgType::DH_BARRIER_PIECE_MSG:
        case ObDtlMsgType::DH_WINBUF_PIECE_MSG:
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected msg type", K(msg_type));
          break;
      }
      // When data of all channels have been output to user and all SQCs have report execution finished to QC,
      // send clean msg to all SQCs.
      if (OB_SUCCESS == ret) {
        if (all_rows_finish_ && coord_info_.all_threads_finish_) {
          (void)msg_proc_.on_process_end(ctx_);
          ret = OB_ITER_END;
          LOG_TRACE("all rows received, all sqcs reported, qc says: byebye!", K(ret));
          LOG_TRACE(
              "TIMERECORD ", "reserve:=1 name:=RQC dfoid:=-1 sqcid:=-1 taskid:=-1 end:", ObTimeUtility::current_time());
        }
      }
    }
  }
  if (ret == OB_ITER_END && !iter_end_) {
    iter_end_ = true;
    LOG_TRACE("RECORDTIME", K(msg_loop_.time_recorder_), K(time_recorder_));
  } else if (OB_UNLIKELY(OB_SUCCESS != ret)) {
    int ret_terminate = terminate_running_dfos(coord_info_.dfo_mgr_);
    LOG_WARN("QC get error code", K(ret), K(ret_terminate));
    if (OB_ERR_SIGNALED_IN_PARALLEL_QUERY_SERVER == ret && OB_SUCCESS != ret_terminate) {
      ret = ret_terminate;
    }
  }
  return ret;
}

int ObPxFifoCoordOp::next_row(bool& wait_next_msg)
{
  int ret = OB_SUCCESS;
  wait_next_msg = true;
  LOG_DEBUG("Begin next_row");
  if (finish_ch_cnt_ >= task_channels_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("All data received. SHOULD NOT see more rows comming",
        "finish_task_cnt",
        finish_ch_cnt_,
        "total_task_chan_cnt",
        task_channels_.count(),
        K(ret));
  } else {
    metric_.mark_interval_start();
    ret = px_row_.get_next_row(MY_SPEC.child_exprs_, eval_ctx_);
    metric_.mark_interval_end(&time_recorder_);
    if (OB_ITER_END == ret) {
      finish_ch_cnt_++;
      if (finish_ch_cnt_ < task_channels_.count()) {
        ret = OB_SUCCESS;
      } else {
        LOG_TRACE("All channel finish", "finish_ch_cnt", finish_ch_cnt_, K(ret));
        all_rows_finish_ = true;
        ret = OB_SUCCESS;
      }
    } else if (OB_SUCCESS == ret) {
      wait_next_msg = false;
    } else {
      LOG_WARN("fail get row from row store", K(ret));
    }
  }

  LOG_DEBUG("Inner_get_next_row px coord receive done",
      K(ret),
      "finish_task_cnt",
      finish_ch_cnt_,
      "total_task_ch_cnt",
      task_channels_.count(),
      K(wait_next_msg));
  return ret;
}

}  // end namespace sql
}  // end namespace oceanbase
