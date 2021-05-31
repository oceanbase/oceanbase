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

#include "ob_px_ms_coord_op.h"
#include "share/ob_rpc_share.h"
#include "share/schema/ob_part_mgr_util.h"
#include "sql/engine/px/ob_px_util.h"
#include "sql/dtl/ob_dtl_channel_group.h"
#include "sql/dtl/ob_dtl_channel_loop.h"
#include "sql/dtl/ob_dtl_msg_type.h"
#include "sql/engine/px/ob_px_dtl_msg.h"
#include "sql/engine/px/ob_px_dtl_proc.h"

namespace oceanbase {
using namespace common;
using namespace sql;
using namespace sql::dtl;
namespace sql {

OB_SERIALIZE_MEMBER((ObPxMSCoordOpInput, ObPxReceiveOpInput));

OB_SERIALIZE_MEMBER((ObPxMSCoordSpec, ObPxCoordSpec), all_exprs_, sort_collations_, sort_cmp_funs_);

int ObPxMSCoordOp::ObPxMSCoordOpEventListener::on_root_data_channel_setup()
{
  int ret = OB_SUCCESS;
  // The receive channel sets of the root dfo are used on this machine and do not need to be sent via DTL
  // Register directly to msg_loop to receive data
  int64_t cnt = px_coord_op_.task_ch_set_.count();
  // dtl data channel register when create channel, so msg_loop_ contains data channels
  px_coord_op_.receive_order_.set_data_channel_idx_range(
      px_coord_op_.msg_loop_.get_channel_count() - cnt, px_coord_op_.msg_loop_.get_channel_count());
  // The row heap must be initialized here,
  // because the row heap can only be established after the data channel is obtained.
  // But px coord inner_get_next_row logic must start to come, that is,
  // control msg must be received, etc.
  // So the data channel is a bit lagging
  if (OB_FAIL(px_coord_op_.init_row_heap(cnt))) {
    LOG_WARN("failed to init row heap", K(ret), K(cnt));
  } else {
    LOG_TRACE("px coord setup", K(cnt), K(px_coord_op_.msg_loop_.get_channel_count()));
  }
  return ret;
}

ObPxMSCoordOp::ObPxMSCoordOp(ObExecContext& exec_ctx, const ObOpSpec& spec, ObOpInput* input)
    : ObPxCoordOp(exec_ctx, spec, input),
      listener_(*this),
      serial_scheduler_(coord_info_, *this, listener_),
      parallel_scheduler_(coord_info_, *this, listener_, msg_proc_),
      msg_proc_(coord_info_, listener_, *this),
      sqc_finish_msg_proc_(exec_ctx, msg_proc_),
      sqc_init_msg_proc_(exec_ctx, msg_proc_),
      barrier_piece_msg_proc_(exec_ctx, msg_proc_),
      winbuf_piece_msg_proc_(exec_ctx, msg_proc_),
      interrupt_proc_(exec_ctx, msg_proc_),
      store_rows_(),
      last_pop_row_(nullptr),
      row_heap_(),
      receive_order_(row_heap_),
      alloc_()
{}

void ObPxMSCoordOp::destroy()
{
  last_pop_row_ = nullptr;
  row_heap_.reset();
  store_rows_.reset();
  alloc_.reset();
  // no need to reset receive_order_
  // no need to reset interrupt_proc_
  // no need to reset sqc_init_msg_proc_
  // no need to reset sqc_finish_msg_proc_
  // no need to reset msg_proc_
  // no need to reset listener_
  ObPxCoordOp::destroy();
}

int ObPxMSCoordOp::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObPxCoordOp::inner_open())) {
    LOG_WARN("fail close op", K(MY_SPEC.id_), K(ret));
  } else if (OB_FAIL(setup_loop_proc())) {
    LOG_WARN("fail setup loop proc", K(ret));
  } else {
    if (1 == px_dop_) {
      msg_proc_.set_scheduler(&serial_scheduler_);
    } else {
      msg_proc_.set_scheduler(&parallel_scheduler_);
    }
    alloc_.set_label(common::ObModIds::OB_SQL_PX);
    alloc_.set_tenant_id(ctx_.get_my_session()->get_effective_tenant_id());
    metric_.set_id(MY_SPEC.id_);
  }
  return ret;
}

int ObPxMSCoordOp::setup_loop_proc()
{
  int ret = OB_SUCCESS;
  msg_loop_.clear_all_proc();
  (void)msg_loop_.register_processor(px_row_msg_proc_)
      .register_processor(sqc_init_msg_proc_)
      .register_processor(sqc_finish_msg_proc_)
      .register_processor(barrier_piece_msg_proc_)
      .register_processor(winbuf_piece_msg_proc_)
      .register_interrupt_processor(interrupt_proc_);
  msg_loop_.set_tenant_id(ctx_.get_my_session()->get_effective_tenant_id());
  return ret;
}

int ObPxMSCoordOp::init_store_rows(int64_t n_ways)
{
  int ret = OB_SUCCESS;
  if (0 < store_rows_.count() || 0 == n_ways) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: store rows is not empty", K(ret), K(n_ways));
  }
  for (int64_t i = 0; i < n_ways && OB_SUCC(ret); ++i) {
    void* buf = alloc_.alloc(sizeof(ObChunkDatumStore::LastStoredRow<>));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloca memory", K(ret));
    } else {
      ObChunkDatumStore::LastStoredRow<>* store_row = new (buf) ObChunkDatumStore::LastStoredRow<>(alloc_);
      if (OB_FAIL(store_rows_.push_back(store_row))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
  }
  return ret;
}

int ObPxMSCoordOp::init_row_heap(int64_t n_ways)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_store_rows(n_ways))) {
    LOG_WARN("failed to init store rows", K(ret));
  } else if (OB_FAIL(row_heap_.init(n_ways, &MY_SPEC.sort_collations_, &MY_SPEC.sort_cmp_funs_))) {
    LOG_WARN("fail init row heap for root dfo", "heap", row_heap_, K(ret));
  }
  return ret;
}

int ObPxMSCoordOp::free_allocator()
{
  int ret = OB_SUCCESS;
  last_pop_row_ = nullptr;
  // the heap shoud be empty before store_rows_.reset();
  while (OB_SUCC(ret) && row_heap_.count() > 0) {
    const ObChunkDatumStore::LastStoredRow<>* pop_row = nullptr;
    if (OB_SUCC(row_heap_.raw_pop(pop_row))) {
      row_heap_.shrink();
    } else {
      LOG_ERROR("pop data fail", K(row_heap_), K(ret), K(common::lbt()));
    }
  }
  store_rows_.reset();
  alloc_.reset();
  return ret;
}

int ObPxMSCoordOp::inner_close()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObPxCoordOp::inner_close())) {
    LOG_WARN("fail close op", K(MY_SPEC.id_), K(ret));
  }
  int tmp_ret = OB_SUCCESS;
  if (OB_SUCCESS != (tmp_ret = free_allocator())) {
    LOG_WARN("failed to free allcator", K(ret), K(tmp_ret), K(ret));
    ret = tmp_ret;
  }
  LOG_TRACE("byebye. exit MergeSort QC Coord");
  return ret;
}

int ObPxMSCoordOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* phy_plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  if (iter_end_) {
    ret = OB_ITER_END;
  } else if (OB_UNLIKELY(!first_row_fetched_)) {
    // drive inital DFO dispatch
    if (OB_FAIL(msg_proc_.startup_msg_loop(ctx_))) {
      LOG_WARN("initial dfos NOT dispatched successfully", K(ret));
    }
    first_row_fetched_ = true;
  }
  bool wait_next_msg = true;
  while (OB_SUCC(ret) && wait_next_msg) {
    // SQC-QC control channel and TASKs-QC data channel are registered in loop
    // In order to achieve orderly receive, TASKs-QC channels need to be added to the loop one by one
    int64_t timeout_us = 0;
    int64_t nth_channel = OB_INVALID_INDEX_INT64;
    clear_evaluated_flag();
    if (FALSE_IT(timeout_us = phy_plan_ctx->get_timeout_timestamp() - get_timestamp())) {
    } else if (OB_FAIL(THIS_WORKER.check_status())) {
      LOG_WARN("fail check status, maybe px query timeout", K(ret));
      // TODO: cleanup
    } else if (OB_FAIL(msg_loop_.process_one_if(&receive_order_, timeout_us, nth_channel))) {
      if (OB_EAGAIN == ret) {
        LOG_TRACE("no message, try again", K(ret));
        ret = OB_SUCCESS;
        // if no data, then unblock blocked data channel, if not, dtl maybe hang
        // bug#28253162
        if (0 < row_heap_.capacity()) {
          if (OB_FAIL(msg_loop_.unblock_channels(receive_order_.get_data_channel_start_idx()))) {
            LOG_WARN("failed to unblock channels", K(ret));
          }
        }
      } else if (OB_ITER_END != ret) {
        LOG_WARN("fail process message", K(ret));
      }
    } else {
      ObDtlMsgType msg_type = msg_loop_.get_last_msg_type();
      switch (msg_type) {
          // row must be collected through PX Coord, because if it is collected through receive,
          // will fall into it, PX Coord loses control
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
      // When satisfied:
      //- All data on the channel is collected and spit out to the user
      //- And all sqc have reported that the thread execution is complete
      //  The finishing process begins:
      //- Send cleanup message to all SQC
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

int ObPxMSCoordOp::next_row(bool& wait_next_msg)
{
  int ret = OB_SUCCESS;
  wait_next_msg = true;
  LOG_TRACE("Begin next_row");
  metric_.mark_interval_start();
  ret = px_row_.get_next_row(MY_SPEC.child_exprs_, eval_ctx_);
  metric_.mark_interval_end(&time_recorder_);
  if (OB_ITER_END == ret) {
    finish_ch_cnt_++;
    row_heap_.shrink();
    // Receiving an EOF line, handle it in two cases:
    //- If it is not the last EOF, discard and return to SUCCESS,
    //  enter the message loop to receive the next message
    //- The last EOF is discarded and returned to ITER_END.
    //  After the execution environment is cleaned up by PX,
    //  ITER_END is returned to the upper operator

    if (OB_LIKELY(finish_ch_cnt_ < task_channels_.count())) {
      ret = OB_SUCCESS;
    } else if (OB_UNLIKELY(finish_ch_cnt_ > task_channels_.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("All data received. SHOULD NOT see more rows comming",
          "finish_task_cnt",
          finish_ch_cnt_,
          "total_task_chan_cnt",
          task_channels_.count(),
          K(ret));
    } else {
      LOG_TRACE("All channel finish", "finish_ch_cnt", finish_ch_cnt_, K(ret));
      all_rows_finish_ = true;
      ret = OB_SUCCESS;
      // Here ret = OB_ITER_END, it means that all channels have been received,
      // but don't exit the msg_loop loop yet
      // Next, you need to wait for SQC to report the execution of each task and notify each SQC
      // Release the resources before returning ITER_END to the upper operator
    }
  } else if (OB_SUCCESS == ret) {
    if (nullptr != last_pop_row_) {
      if (OB_FAIL(last_pop_row_->save_store_row(MY_SPEC.all_exprs_, eval_ctx_, 0))) {
        LOG_WARN("failed to save store row", K(ret));
      } else if (OB_FAIL(row_heap_.push(last_pop_row_))) {
        LOG_WARN("fail push row to heap", K(ret));
      }
    } else {
      ObChunkDatumStore::LastStoredRow<>* cur_row = nullptr;
      if (OB_FAIL(store_rows_.at(row_heap_.writable_channel_idx(), cur_row))) {
        LOG_WARN("failed to get store row", K(ret), K(row_heap_.writable_channel_idx()));
      } else if (OB_FAIL(cur_row->save_store_row(MY_SPEC.all_exprs_, eval_ctx_, 0))) {
        LOG_WARN("failed to save store row", K(ret));
      } else if (OB_FAIL(row_heap_.push(cur_row))) {
        LOG_WARN("fail push row to heap", K(ret));
      }
    }
  } else {
    LOG_WARN("fail get row from row store", K(ret));
  }

  if (OB_SUCC(ret)) {
    if (0 == row_heap_.capacity()) {
      all_rows_finish_ = true;
      metric_.mark_last_out();
    } else if (row_heap_.capacity() == row_heap_.count()) {
      const ObChunkDatumStore::LastStoredRow<>* pop_row = nullptr;
      if (OB_FAIL(row_heap_.pop(pop_row))) {
        LOG_WARN("failed to pop row", K(ret));
      } else if (OB_FAIL(pop_row->store_row_->to_expr(MY_SPEC.all_exprs_, eval_ctx_))) {
        LOG_WARN("failed to to exprs", K(ret));
      } else {
        wait_next_msg = false;
        last_pop_row_ = const_cast<ObChunkDatumStore::LastStoredRow<>*>(pop_row);
      }
      metric_.count();
      metric_.mark_first_out();
    } else if (row_heap_.capacity() > row_heap_.count()) {
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid row heap state", K(row_heap_), K(ret));
    }
  }

  LOG_DEBUG("Inner_get_next_row px merge sort coord receive done",
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
