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

#include "ob_px_ordered_coord_op.h"
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

namespace oceanbase
{
using namespace common;
using namespace sql;
using namespace sql::dtl;
namespace sql
{

OB_SERIALIZE_MEMBER((ObPxOrderedCoordOpInput, ObPxReceiveOpInput));

OB_SERIALIZE_MEMBER((ObPxOrderedCoordSpec, ObPxCoordSpec));

ObPxOrderedCoordOp::ObPxOrderedCoordOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
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
    sample_piece_msg_proc_(exec_ctx, msg_proc_),
    rollup_key_piece_msg_proc_(exec_ctx, msg_proc_),
    rd_wf_piece_msg_proc_(exec_ctx, msg_proc_),
    init_channel_piece_msg_proc_(exec_ctx, msg_proc_),
    reporting_wf_piece_msg_proc_(exec_ctx, msg_proc_),
    opt_stats_gather_piece_msg_proc_(exec_ctx, msg_proc_),
    readers_(NULL),
    receive_order_(),
    reader_cnt_(0),
    channel_idx_(0),
    finish_ch_cnt_(0),
    all_rows_finish_(false)
  {}

int ObPxOrderedCoordOp::ObPxOrderedCoordOpEventListener::on_root_data_channel_setup()
{
  int ret = OB_SUCCESS;
  int64_t cnt = px_coord_op_.task_ch_set_.count();
  px_coord_op_.receive_order_.set_data_channel_idx_range(
      px_coord_op_.msg_loop_.get_channel_count() - cnt,
      px_coord_op_.msg_loop_.get_channel_count());
  px_coord_op_.channel_idx_ = 0;
  LOG_TRACE("px coord setup", K(cnt), K(px_coord_op_.msg_loop_.get_channel_count()));
  return ret;
}

int ObPxOrderedCoordOp::inner_open()
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

int ObPxOrderedCoordOp::inner_close()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObPxCoordOp::inner_close())) {
    LOG_WARN("fail close op", K(MY_SPEC.id_), K(ret));
  }
  return ret;
}

int ObPxOrderedCoordOp::setup_loop_proc()
{
  int ret = OB_SUCCESS;
  msg_loop_.clear_all_proc();
  (void)msg_loop_
      .register_processor(px_row_msg_proc_)
      .register_processor(sqc_init_msg_proc_)
      .register_processor(sqc_finish_msg_proc_)
      .register_processor(barrier_piece_msg_proc_)
      .register_processor(winbuf_piece_msg_proc_)
      .register_processor(sample_piece_msg_proc_)
      .register_processor(rollup_key_piece_msg_proc_)
      .register_processor(rd_wf_piece_msg_proc_)
      .register_processor(init_channel_piece_msg_proc_)
      .register_processor(reporting_wf_piece_msg_proc_)
      .register_processor(opt_stats_gather_piece_msg_proc_)
      .register_interrupt_processor(interrupt_proc_);
  return ret;
}

int ObPxOrderedCoordOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *phy_plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  if (iter_end_) {
    ret = OB_ITER_END;
  } else if (OB_UNLIKELY(!first_row_fetched_)) {
    // 驱动初始 DFO 的分发
    if (OB_FAIL(msg_proc_.startup_msg_loop(ctx_))) {
      LOG_WARN("initial dfos NOT dispatched successfully", K(ret));
    }
    first_row_fetched_ = true; // 控制不再主动调用 startup_msg_loop，后继 loop 都消息触发
  }
  bool wait_next_msg = true; // 控制是否退出 while 循环，返回 row 给上层算子
  while (OB_SUCC(ret) && wait_next_msg) {
    // loop 中注册了 SQC-QC 控制通道，以及 TASKs-QC 数据通道
    // 为了实现 orderly receive， TASKs-QC 通道需要逐个加入到 loop 中
    int64_t timeout_us = 0;
    int64_t nth_channel = OB_INVALID_INDEX_INT64;
    // Note:
    //   inner_get_next_row is invoked in two pathes (batch vs
    //   non-batch). The eval flag should be cleared with seperated flags
    //   under each invoke path (batch vs non-batch). Therefore call the
    //   overriding API do_clear_datum_eval_flag() to replace
    //   clear_evaluated_flag
    // TODO qubin.qb: Implement seperated inner_get_next_batch to
    // isolate them
    do_clear_datum_eval_flag();
    clear_dynamic_const_parent_flag();
    if (channel_idx_ < task_ch_set_.count()) {
      int64_t idx = channel_idx_;
      ObReceiveRowReader *reader = (NULL != readers_ && idx < reader_cnt_) ? &readers_[idx] : NULL;
      ObDtlChannel *ch = msg_loop_.get_channel(idx + receive_order_.get_data_channel_start_idx());
      if (NULL == ch || NULL == reader) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("reader or channel is NULL");
      } else if (OB_FAIL(ctx_.fast_check_status())) {
        LOG_WARN("failed to check status", K(ret));
      } else {
        if (reader->has_more() || ch->is_eof()) {
          ret = next_row(*reader, wait_next_msg);
          if (!first_row_sent_) {
            first_row_sent_ = true;
            LOG_TRACE("TIMERECORD ",
                     "reserve:=1 name:=RQC dfoid:=-1 sqcid:=-1 taskid:=-1 start:",
                     ObTimeUtility::current_time());
          }
          continue;
        } else {
          // fetch row for writable index reader
          px_row_msg_proc_.set_reader(reader);
        }
      }
    }

    // process end condition:
    // 1. all rows returned
    // 2. all SQC report worker execution finish
    if (OB_SUCCESS == ret) {
      if (all_rows_finish_ && coord_info_.all_threads_finish_) {
        (void) msg_proc_.on_process_end(ctx_);
        ret = OB_ITER_END;
        LOG_TRACE("all rows received, all sqcs reported, qc says: byebye!", K(ret));
        LOG_TRACE("TIMERECORD ",
                 "reserve:=1 name:=RQC dfoid:=-1 sqcid:=-1 taskid:=-1 end:",
                 ObTimeUtility::current_time());
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(THIS_WORKER.check_status())) {
      LOG_WARN("fail check status, maybe px query timeout", K(ret));
    } else if (OB_FAIL(msg_loop_.process_one_if(&receive_order_, nth_channel))) {
      if (OB_EAGAIN == ret) {
        LOG_TRACE("no message, try again", K(ret));
        ret = OB_SUCCESS;
        if (channel_idx_ < task_ch_set_.count() && first_row_sent_) {
          if (OB_FAIL(msg_loop_.unblock_channel(receive_order_.get_data_channel_start_idx(),
                                                channel_idx_))) {
            LOG_WARN("failed to unblock channels", K(ret));
          }
        }
      } else if (OB_ITER_END != ret) {
        LOG_WARN("fail process message", K(ret));
      }
    } else {
      ObDtlMsgType msg_type = msg_loop_.get_last_msg_type();
      switch (msg_type) {
        case ObDtlMsgType::PX_NEW_ROW:
          // got rows
          break;
        case ObDtlMsgType::INIT_SQC_RESULT:
        case ObDtlMsgType::FINISH_SQC_RESULT:
        case ObDtlMsgType::DH_BARRIER_PIECE_MSG:
        case ObDtlMsgType::DH_WINBUF_PIECE_MSG:
        case ObDtlMsgType::DH_DYNAMIC_SAMPLE_PIECE_MSG:
        case ObDtlMsgType::DH_ROLLUP_KEY_PIECE_MSG:
        case ObDtlMsgType::DH_RANGE_DIST_WF_PIECE_MSG:
        case ObDtlMsgType::DH_INIT_CHANNEL_PIECE_MSG:
        case ObDtlMsgType::DH_SECOND_STAGE_REPORTING_WF_PIECE_MSG:
        case ObDtlMsgType::DH_OPT_STATS_GATHER_PIECE_MSG:
          // 这几种消息都在 process 回调函数里处理了
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected msg type", K(msg_type));
          break;
      }
    }
  }
  if (ret == OB_ITER_END && !iter_end_) {
    iter_end_ = true;
    LOG_TRACE("RECORDTIME", K(time_recorder_));
  } else if (OB_UNLIKELY(OB_SUCCESS != ret)) {
    int ret_terminate = terminate_running_dfos(coord_info_.dfo_mgr_);
    LOG_WARN("QC get error code", K(ret), K(ret_terminate));
    if (OB_ERR_SIGNALED_IN_PARALLEL_QUERY_SERVER == ret
        && OB_SUCCESS != ret_terminate) {
      ret = ret_terminate;
    }
  }
  return ret;
}

int ObPxOrderedCoordOp::inner_get_next_batch(const int64_t max_row_cnt)
{
  return wrap_get_next_batch(max_row_cnt);
}

int ObPxOrderedCoordOp::next_row(ObReceiveRowReader &reader, bool &wait_next_msg)
{
  int ret = OB_SUCCESS;
  wait_next_msg = true;
  LOG_TRACE("Begin next_row");
  metric_.mark_interval_start();
  ret = reader.get_next_row(MY_SPEC.child_exprs_, MY_SPEC.dynamic_const_exprs_, eval_ctx_);
  metric_.mark_interval_end(&time_recorder_);
  if (OB_ITER_END == ret) {
    finish_ch_cnt_++;
    channel_idx_++;
    if (OB_LIKELY(finish_ch_cnt_ < task_channels_.count())) {
      receive_order_.set_current_ch_idx(channel_idx_);
      ret = OB_SUCCESS;
    } else if (OB_UNLIKELY(finish_ch_cnt_ > task_channels_.count())) {
      // 本分支是一个防御分支
      // 所有 channel 上的数据都收取成功
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("All data received. SHOULD NOT see more rows comming",
               "finish_task_cnt", finish_ch_cnt_,
               "total_task_chan_cnt", task_channels_.count(),
               K(ret));
    } else {
      LOG_TRACE("All channel finish", "finish_ch_cnt", finish_ch_cnt_, K(ret));
      all_rows_finish_ = true;
      ret = OB_SUCCESS;
    }
  } else if (OB_SUCCESS == ret) {
    wait_next_msg = false;
  }
  return ret;
}

int ObPxOrderedCoordOp::receive_channel_root_dfo(ObExecContext &ctx,
                                            ObDfo &parent,
                                            ObPxTaskChSets &parent_ch_sets)
{
  int ret = OB_SUCCESS;
  OZ(ObPxCoordOp::receive_channel_root_dfo(ctx, parent, parent_ch_sets));
  OZ(setup_readers());
  return ret;
}

int ObPxOrderedCoordOp::receive_channel_root_dfo(ObExecContext &ctx,
                                            ObDfo &parent,
                                            dtl::ObDtlChTotalInfo &ch_info)
{
  int ret = OB_SUCCESS;
  OZ(ObPxCoordOp::receive_channel_root_dfo(ctx, parent, ch_info));
  OZ(setup_readers());
  return ret;
}

int ObPxOrderedCoordOp::setup_readers()
{
  int ret = OB_SUCCESS;
  CK(task_channels_.count() > 0);
  CK(NULL == readers_);
  if (OB_SUCC(ret)) {
    readers_ = static_cast<ObReceiveRowReader *>(ctx_.get_allocator().alloc(
            sizeof(*readers_) * task_channels_.count()));
    if (NULL == readers_) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    } else {
      reader_cnt_ = task_channels_.count();
      for (int64_t i = 0; i < reader_cnt_; i++) {
        new (&readers_[i]) ObReceiveRowReader();
      }
    }
  }
  if (OB_SUCC(ret)) {
    px_row_msg_proc_.set_reader(readers_);
  }
  return ret;
}

void ObPxOrderedCoordOp::destroy_readers()
{
  if (NULL != readers_) {
    for (int64_t i = 0; i < reader_cnt_; i++) {
      readers_[i].~ObReceiveRowReader();
    }
    readers_ = NULL;
    reader_cnt_ = 0;
  }
  px_row_msg_proc_.set_reader(NULL);
}

void ObPxOrderedCoordOp::destroy()
{
  destroy_readers();
  ObPxCoordOp::destroy();
}


} // end namespace sql
} // end namespace oceanbase
