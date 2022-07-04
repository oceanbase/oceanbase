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

#include "sql/engine/px/ob_px_merge_sort_coord.h"
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

OB_SERIALIZE_MEMBER((ObPxMergeSortCoordInput, ObPxReceiveInput));
OB_SERIALIZE_MEMBER((ObPxMergeSortCoord, ObPxCoord), sort_columns_);

int ObPxMergeSortCoord::ObPxMergeSortCoordEventListener::on_root_data_channel_setup()
{
  int ret = OB_SUCCESS;
  // direct register root dfo receive channel sets to local msg_loop
  int64_t cnt = px_ctx_.task_ch_set_.count();
  // dtl data channel register when create channel, so msg_loop_ contains data channels
  px_ctx_.receive_order_.set_data_channel_idx_range(
      px_ctx_.msg_loop_.get_channel_count() - cnt, px_ctx_.msg_loop_.get_channel_count());
  px_ctx_.row_heap_.set_capacity(cnt);
  if (OB_FAIL(px_ctx_.row_heap_.init())) {
    LOG_WARN("fail init row heap for root dfo", "heap", px_ctx_.row_heap_, K(cnt), K(ret));
  }
  return ret;
}

int ObPxMergeSortCoord::init_op_ctx(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPxMergeSortCoordCtx* op_ctx = NULL;
  if (OB_FAIL(CREATE_PHY_OPERATOR_CTX(ObPxMergeSortCoordCtx, ctx, get_id(), get_type(), op_ctx))) {
    LOG_WARN("create physical operator context failed", K(ret));
  } else if (OB_ISNULL(op_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("op_ctx is null");
  }
  return ret;
}

int ObPxMergeSortCoord::inner_open(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPxMergeSortCoordCtx* px_ctx = NULL;
  if (OB_FAIL(ObPxCoord::inner_open(ctx))) {
    LOG_WARN("fail close op", K(get_id()), K(ret));
  } else if (OB_ISNULL(px_ctx = GET_PHY_OPERATOR_CTX(ObPxMergeSortCoordCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get physical operator context failed", K(ret));
  } else if (OB_FAIL(setup_loop_proc(ctx, *px_ctx))) {
    LOG_WARN("fail setup loop proc", K(ret));
  } else if (OB_FAIL(px_ctx->fifo_alloc_.init(&px_ctx->row_allocator_, OB_MALLOC_BIG_BLOCK_SIZE))) {
    LOG_WARN("fail init fifo allocator", K(ret));
  } else if (OB_FAIL(setup_scheduler(*px_ctx))) {
    LOG_WARN("fail setup scheduler", K(ret));
  } else if (sort_columns_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sort_column could not be empty", K(ret));
  } else {
    px_ctx->row_heap_.set_sort_columns(sort_columns_);
    px_ctx->metric_.set_id(get_id());
  }
  return ret;
}

int ObPxMergeSortCoord::setup_scheduler(ObPxMergeSortCoordCtx& px_ctx) const
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

int ObPxMergeSortCoord::setup_loop_proc(ObExecContext& ctx, ObPxCoordCtx& px_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  ObPxMergeSortCoordCtx& px_merge_coord_ctx = static_cast<ObPxMergeSortCoordCtx&>(px_ctx);
  px_merge_coord_ctx.msg_loop_.clear_all_proc();
  (void)px_merge_coord_ctx.msg_loop_.register_processor(px_merge_coord_ctx.px_row_msg_proc_)
      .register_processor(px_merge_coord_ctx.sqc_init_msg_proc_)
      .register_processor(px_merge_coord_ctx.barrier_piece_msg_proc_)
      .register_processor(px_merge_coord_ctx.winbuf_piece_msg_proc_)
      .register_processor(px_merge_coord_ctx.sqc_finish_msg_proc_)
      .register_interrupt_processor(px_merge_coord_ctx.interrupt_proc_);
  px_merge_coord_ctx.msg_loop_.set_tenant_id(ctx.get_my_session()->get_effective_tenant_id());
  return ret;
}

////////////////////////////////////////////////////////////////////////////////////////////

int ObPxMergeSortCoord::free_allocator(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObPxMergeSortCoordCtx* px_ctx = NULL;
  if (OB_ISNULL(px_ctx = GET_PHY_OPERATOR_CTX(ObPxMergeSortCoordCtx, ctx, get_id()))) {
    LOG_DEBUG("The operator has not been opened.", K(ret), K_(id), "op_type", ob_phy_operator_type_str(get_type()));
  } else {
    if (OB_LIKELY(NULL != px_ctx->last_row_)) {
      px_ctx->fifo_alloc_.free(static_cast<void*>(const_cast<ObNewRow*>(px_ctx->last_row_)));
      px_ctx->last_row_ = NULL;
    }
    ObRowHeap<>& row_heap = px_ctx->row_heap_;
    while (OB_SUCC(ret) && row_heap.count() > 0) {
      const common::ObNewRow* row = NULL;
      if (OB_SUCC(row_heap.raw_pop(row))) {
        px_ctx->fifo_alloc_.free(static_cast<void*>(const_cast<ObNewRow*>(row)));
      } else {
        LOG_WARN("pop data fail", K(row_heap), K(ret));
      }
    }
    px_ctx->fifo_alloc_.reset();
  }
  return ret;
}

int ObPxMergeSortCoord::inner_close(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObPxCoord::inner_close(ctx))) {
    LOG_WARN("fail close op", K(get_id()), K(ret));
  }

  int tmp_ret = OB_SUCCESS;
  if (OB_SUCCESS != (tmp_ret = free_allocator(ctx))) {
    LOG_WARN("failed to free allcator", K(ret), K(tmp_ret), K(ret));
    ret = tmp_ret;
  }
  LOG_TRACE("byebye. exit MergeSort QC Coord");
  return ret;
}

////////////////////////////////////////////////////////////////////////////////////////////

int ObPxMergeSortCoord::inner_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ObPxMergeSortCoordCtx* px_ctx = NULL;
  ObPhysicalPlanCtx* phy_plan_ctx = NULL;
  if (OB_ISNULL(px_ctx = GET_PHY_OPERATOR_CTX(ObPxMergeSortCoordCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail get op ctx", K(get_id()), K(ret));
  } else if (px_ctx->iter_end_) {
    ret = OB_ITER_END;
  } else if (OB_UNLIKELY(!px_ctx->first_row_fetched_)) {
    if (OB_FAIL(px_ctx->msg_proc_.startup_msg_loop(ctx))) {
      LOG_WARN("initial dfos NOT dispatched successfully", K(ret));
    }
    px_ctx->first_row_fetched_ = true;
  }

  bool wait_next_msg = true;
  while (OB_SUCC(ret) && wait_next_msg) {
    ObDtlChannelLoop& loop = px_ctx->msg_loop_;
    int64_t timeout_us = 0;
    int64_t nth_channel = OB_INVALID_INDEX_INT64;
    if (OB_ISNULL(phy_plan_ctx = GET_PHY_PLAN_CTX(ctx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("phy plan ctx NULL", K(ret));
    } else if (FALSE_IT(timeout_us = phy_plan_ctx->get_timeout_timestamp() - px_ctx->get_timestamp())) {
    } else if (OB_FAIL(ctx.fast_check_status())) {
      LOG_WARN("fail check status, maybe px query timeout", K(ret));
    } else if (OB_FAIL(loop.process_one_if(&px_ctx->receive_order_, timeout_us, nth_channel))) {
      if (OB_EAGAIN == ret) {
        LOG_TRACE("no message, try again", K(ret));
        ret = OB_SUCCESS;
        // if no data, then unblock blocked data channel, if not, it maybe hang
        // bug#28253162
        if (0 < px_ctx->row_heap_.capacity()) {
          if (OB_FAIL(loop.unblock_channels(px_ctx->receive_order_.get_data_channel_start_idx()))) {
            LOG_WARN("failed to unblock channels", K(ret));
          }
        }
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
          // msg already processed in callback functions
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

int ObPxMergeSortCoord::next_row(
    ObExecContext& ctx, ObPxMergeSortCoordCtx& px_ctx, const common::ObNewRow*& row, bool& wait_next_msg) const
{
  UNUSED(ctx);
  int ret = OB_SUCCESS;
  ObRowHeap<>& row_heap = px_ctx.row_heap_;

  row = NULL;
  wait_next_msg = true;

  LOG_TRACE("Begin next_row");

  if (OB_LIKELY(NULL != px_ctx.last_row_)) {
    px_ctx.fifo_alloc_.free(static_cast<void*>(const_cast<ObNewRow*>(px_ctx.last_row_)));
    px_ctx.last_row_ = NULL;
  }

  px_ctx.metric_.mark_interval_start();
  ret = px_ctx.px_row_.get_row(px_ctx.get_cur_row());
  px_ctx.metric_.mark_interval_end(&px_ctx.time_recorder_);
  if (OB_ITER_END == ret) {
    px_ctx.finish_ch_cnt_++;
    row_heap.shrink();
    // if not last EOF, continue receiving data; if last EOF, return ITER_END
    if (OB_LIKELY(px_ctx.finish_ch_cnt_ < px_ctx.task_channels_.count())) {
      ret = OB_SUCCESS;
    } else if (OB_UNLIKELY(px_ctx.finish_ch_cnt_ > px_ctx.task_channels_.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("All data received. SHOULD NOT see more rows comming",
          "finish_task_cnt",
          px_ctx.finish_ch_cnt_,
          "total_task_chan_cnt",
          px_ctx.task_channels_.count(),
          K(ret));
    } else {
      LOG_TRACE("All channel finish", "finish_ch_cnt", px_ctx.finish_ch_cnt_, K(ret));
      px_ctx.all_rows_finish_ = true;
      ret = OB_SUCCESS;
      // all data received, need wait SQC report.
      // can not return ITER_END
    }
  } else if (OB_SUCCESS == ret) {
    const ObNewRow* in_row = &px_ctx.get_cur_row();
    int64_t len = in_row->get_deep_copy_size() + sizeof(ObNewRow);
    char* buf = static_cast<char*>(px_ctx.fifo_alloc_.alloc(len));
    if (OB_UNLIKELY(NULL == buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      ObNewRow* row_stored = new (buf) ObNewRow;
      int64_t pos = sizeof(ObNewRow);
      if (OB_FAIL(row_stored->deep_copy(*in_row, buf, len, pos))) {
        LOG_WARN("fail deep copy row", K(len), K(ret));
        px_ctx.fifo_alloc_.free(buf);
      } else if (OB_FAIL(row_heap.push(row_stored))) {
        LOG_WARN("fail push row to heap", K(ret));
        px_ctx.fifo_alloc_.free(buf);
      }
    }
  } else {
    LOG_WARN("fail get row from row store", K(ret));
  }

  if (OB_SUCC(ret)) {
    if (0 == row_heap.capacity()) {
      px_ctx.all_rows_finish_ = true;
      px_ctx.metric_.mark_last_out();
    } else if (row_heap.capacity() == row_heap.count()) {
      if (OB_SUCC(row_heap.pop(row))) {
        px_ctx.last_row_ = row;
        wait_next_msg = false;
      }
      px_ctx.metric_.count();
      px_ctx.metric_.mark_first_out();
    } else if (row_heap.capacity() > row_heap.count()) {
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid row heap state", K(row_heap), K(ret));
    }
  }

  LOG_DEBUG("Inner_get_next_row px merge sort coord receive done",
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
