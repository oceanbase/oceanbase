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

#include "sql/engine/px/ob_px_fifo_coord.h"
#include "share/ob_rpc_share.h"
#include "share/schema/ob_part_mgr_util.h"
#include "sql/engine/px/ob_px_util.h"
#include "sql/dtl/ob_dtl_channel_group.h"
#include "sql/dtl/ob_dtl_channel_loop.h"
#include "sql/dtl/ob_dtl_msg_type.h"
#include "sql/executor/ob_transmit.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/px/ob_px_dtl_msg.h"
#include "sql/engine/px/ob_px_dtl_proc.h"
#include "sql/engine/px/exchange/ob_px_receive.h"
#include "sql/engine/px/ob_px_util.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::sql;
using namespace oceanbase::sql::dtl;

OB_SERIALIZE_MEMBER((ObPxFifoCoordInput, ObPxReceiveInput));

ObPxFifoCoord::ObPxFifoCoord(ObIAllocator& alloc) : ObPxCoord(alloc)
{}

ObPxFifoCoord::~ObPxFifoCoord()
{}

////////////////////////////////////////////////////////////////////////////////////////////

int ObPxFifoCoord::init_op_ctx(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPxFifoCoordCtx* op_ctx = NULL;
  if (OB_FAIL(CREATE_PHY_OPERATOR_CTX(ObPxFifoCoordCtx, ctx, get_id(), get_type(), op_ctx))) {
    LOG_WARN("create physical operator context failed", K(ret));
  } else if (OB_ISNULL(op_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("op_ctx is null");
  }
  return ret;
}

int ObPxFifoCoord::inner_open(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPxFifoCoordCtx* px_ctx = NULL;
  if (OB_FAIL(ObPxCoord::inner_open(ctx))) {
    LOG_WARN("fail open op", K(get_id()), K(ret));
  } else if (OB_ISNULL(px_ctx = GET_PHY_OPERATOR_CTX(ObPxFifoCoordCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get physical operator context failed", K(ret));
  } else if (OB_FAIL(setup_loop_proc(ctx, *px_ctx))) {
    LOG_WARN("fail setup loop proc", K(ret));
  } else if (OB_FAIL(setup_scheduler(*px_ctx))) {
    LOG_WARN("fail setup scheduler", K(ret));
  } else {
    px_ctx->metric_.set_id(get_id());
  }
  return ret;
}
int ObPxFifoCoord::setup_scheduler(ObPxFifoCoordCtx& px_ctx) const
{
  int ret = OB_SUCCESS;
  ObPxMsgProc& proc = px_ctx.get_msg_proc();
  if (1 == px_ctx.px_dop_ && GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_2250) {
    proc.set_scheduler(&px_ctx.get_serial_scheduler());
  } else {
    proc.set_scheduler(&px_ctx.get_parallel_scheduler());
  }
  return ret;
}

int ObPxFifoCoord::inner_close(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObPxCoord::inner_close(ctx))) {
    LOG_WARN("fail close op", K(get_id()), K(ret));
  }
  return ret;
}

int ObPxFifoCoord::setup_loop_proc(ObExecContext& ctx, ObPxCoordCtx& px_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  ObPxFifoCoordCtx& px_fifo_coord_ctx = static_cast<ObPxFifoCoordCtx&>(px_ctx);
  px_fifo_coord_ctx.msg_loop_.clear_all_proc();
  (void)px_fifo_coord_ctx.msg_loop_.register_processor(px_fifo_coord_ctx.px_row_msg_proc_)
      .register_processor(px_fifo_coord_ctx.sqc_init_msg_proc_)
      .register_processor(px_fifo_coord_ctx.barrier_piece_msg_proc_)
      .register_processor(px_fifo_coord_ctx.winbuf_piece_msg_proc_)
      .register_processor(px_fifo_coord_ctx.sqc_finish_msg_proc_)
      .register_interrupt_processor(px_fifo_coord_ctx.interrupt_proc_);
  return ret;
}

int ObPxFifoCoord::inner_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ObPxFifoCoordCtx* px_ctx = NULL;
  ObPhysicalPlanCtx* phy_plan_ctx = NULL;
  if (OB_ISNULL(px_ctx = GET_PHY_OPERATOR_CTX(ObPxFifoCoordCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail get op ctx", K(get_id()), K(ret));
  } else if (px_ctx->iter_end_) {
    ret = OB_ITER_END;
  } else if (OB_UNLIKELY(!px_ctx->first_row_fetched_)) {
    if (OB_FAIL(px_ctx->msg_proc_.startup_msg_loop(ctx))) {
      LOG_WARN("initial dfos NOT dispatched successfully", K(ret));
    }
    px_ctx->msg_loop_.set_tenant_id(ctx.get_my_session()->get_effective_tenant_id());
    px_ctx->first_row_fetched_ = true;
  }

  bool wait_next_msg = true;
  while (OB_SUCC(ret) && wait_next_msg) {
    ObDtlChannelLoop& loop = px_ctx->msg_loop_;
    int64_t timeout_us = 0;
    if (OB_ISNULL(phy_plan_ctx = GET_PHY_PLAN_CTX(ctx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("phy plan ctx NULL", K(ret));
    } else if (FALSE_IT(timeout_us = phy_plan_ctx->get_timeout_timestamp() - px_ctx->get_timestamp())) {
    } else if (OB_FAIL(THIS_WORKER.check_status())) {
      LOG_WARN("fail check status, maybe px query timeout", K(ret));
      // TODO: cleanup
    } else if (OB_FAIL(loop.process_one(timeout_us))) {
      if (OB_EAGAIN == ret) {
        LOG_TRACE("no message, try again", K(ret));
        ret = OB_SUCCESS;
      } else if (OB_ITER_END != ret) {
        LOG_WARN("fail process message", K(ret));
      }
    } else {
      ObDtlMsgType msg_type = loop.get_last_msg_type();
      switch (msg_type) {
        case ObDtlMsgType::PX_NEW_ROW:
          // got one row, return
          ret = next_row(ctx, *px_ctx, row, wait_next_msg);
          if (!px_ctx->first_row_sent_) {
            px_ctx->first_row_sent_ = true;
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
      if (OB_SUCCESS == ret) {
        if (px_ctx->all_rows_finish_ && px_ctx->coord_info_.all_threads_finish_) {
          (void)px_ctx->msg_proc_.on_process_end(ctx);
          ret = OB_ITER_END;
          LOG_TRACE("all rows received, all sqcs reported, qc says: byebye!", K(ret));
          LOG_TRACE(
              "TIMERECORD ", "reserve:=1 name:=RQC dfoid:=-1 sqcid:=-1 taskid:=-1 end:", ObTimeUtility::current_time());
        }
      }
    }
  }
  if (ret == OB_ITER_END && !px_ctx->iter_end_) {
    px_ctx->iter_end_ = true;
    LOG_TRACE("RECORDTIME", K(px_ctx->msg_loop_.time_recorder_), K(px_ctx->time_recorder_));
  } else if (OB_UNLIKELY(OB_SUCCESS != ret && OB_NOT_NULL(px_ctx))) {
    int ret_terminate = terminate_running_dfos(ctx, px_ctx->coord_info_.dfo_mgr_);
    LOG_WARN("QC get error code", K(ret), K(ret_terminate));
    if (OB_ERR_SIGNALED_IN_PARALLEL_QUERY_SERVER == ret && OB_SUCCESS != ret_terminate) {
      ret = ret_terminate;
    }
  }

  return ret;
}

int ObPxFifoCoord::next_row(
    ObExecContext& ctx, ObPxFifoCoordCtx& px_ctx, const common::ObNewRow*& row, bool& wait_next_msg) const
{
  UNUSED(ctx);
  int ret = OB_SUCCESS;

  row = NULL;
  wait_next_msg = true;

  LOG_DEBUG("Begin next_row");
  if (px_ctx.finish_ch_cnt_ >= px_ctx.task_channels_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("All data received. SHOULD NOT see more rows comming",
        "finish_task_cnt",
        px_ctx.finish_ch_cnt_,
        "total_task_chan_cnt",
        px_ctx.task_channels_.count(),
        K(ret));
  } else {
    px_ctx.metric_.mark_interval_start();
    ret = px_ctx.px_row_.get_row(px_ctx.get_cur_row());
    px_ctx.metric_.mark_interval_end(&px_ctx.time_recorder_);
    if (OB_ITER_END == ret) {
      px_ctx.finish_ch_cnt_++;
      if (px_ctx.finish_ch_cnt_ < px_ctx.task_channels_.count()) {
        ret = OB_SUCCESS;
      } else {
        LOG_TRACE("All channel finish", "finish_ch_cnt", px_ctx.finish_ch_cnt_, K(ret));
        px_ctx.all_rows_finish_ = true;
        ret = OB_SUCCESS;
      }
    } else if (OB_SUCCESS == ret) {
      row = &px_ctx.get_cur_row();
      wait_next_msg = false;
    } else {
      LOG_WARN("fail get row from row store", K(ret));
    }
  }

  LOG_DEBUG("Inner_get_next_row px coord receive done",
      K(ret),
      "finish_task_cnt",
      px_ctx.finish_ch_cnt_,
      "total_task_ch_cnt",
      px_ctx.task_channels_.count(),
      K(wait_next_msg));
  if (NULL != row) {
    LOG_DEBUG("Row value:", K(*row));
  }

  return ret;
}
