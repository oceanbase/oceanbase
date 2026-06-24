/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG

#include "ob_px_receive_op.h"
#include "ob_px_ordered_receive_op.h"
#include "sql/dtl/ob_dtl_channel_group.h"
#include "sql/dtl/ob_dtl_rpc_channel.h"
#include "share/ob_rpc_share.h"
#include "sql/engine/px/exchange/ob_px_ms_receive_op.h"
#include "sql/engine/px/ob_px_sqc_handler.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace sql;
using namespace sql::dtl;
namespace sql
{
OB_SERIALIZE_MEMBER((ObPxOrderedReceiveOpInput, ObPxReceiveOpInput));
OB_SERIALIZE_MEMBER((ObPxOrderedReceiveSpec, ObPxReceiveSpec));

ObPxOrderedReceiveOp::ObPxOrderedReceiveOp(ObExecContext &exec_ctx,
                                             const ObOpSpec &spec,
                                             ObOpInput *input)
  : ObPxReceiveOp(exec_ctx, spec, input),
    readers_(NULL),
    receive_order_(),
    reader_cnt_(0),
    channel_idx_(0),
    finish_ch_cnt_(0),
    all_rows_finish_(false),
    interrupt_proc_()
{
}

int ObPxOrderedReceiveOp::inner_open()
{
  return ObPxReceiveOp::inner_open();
}

void ObPxOrderedReceiveOp::destroy()
{
  destroy_readers();
  ObPxReceiveOp::destroy();
}

int ObPxOrderedReceiveOp::inner_close()
{
  int ret = OB_SUCCESS;
  row_reader_.reset();
  int release_channel_ret = common::OB_SUCCESS;
  if (channel_linked_) {
    release_channel_ret = ObPxChannelUtil::flush_rows(task_channels_);
    if (release_channel_ret != common::OB_SUCCESS) {
      LOG_WARN("release dtl channel failed", K(release_channel_ret));
    }
    int64_t recv_cnt = 0;
    op_monitor_info_.otherstat_3_id_ = ObSqlMonitorStatIds::DTL_SEND_RECV_COUNT;
    op_monitor_info_.otherstat_3_value_ = recv_cnt;
    release_channel_ret = msg_loop_.unregister_all_channel();
    if (release_channel_ret != common::OB_SUCCESS) {
      LOG_ERROR("fail unregister all channel from msg_loop", KR(release_channel_ret));
    }
    release_channel_ret = ObPxChannelUtil::unlink_ch_set(get_ch_set(), &dfc_, true);
    if (release_channel_ret != common::OB_SUCCESS) {
      LOG_WARN("release dtl channel failed", K(release_channel_ret));
    }
  }
  release_channel_ret = erase_dtl_interm_result();
  if (release_channel_ret != common::OB_SUCCESS) {
    LOG_TRACE("release interm result failed", KR(release_channel_ret));
  }
  destroy_readers();
  return ret;
}

int ObPxOrderedReceiveOp::inner_rescan()
{
  int ret = OB_SUCCESS;
  reset_readers();
  receive_order_.set_current_ch_idx(0);
  channel_idx_ = 0;
  finish_ch_cnt_ = 0;
  all_rows_finish_ = false;
  ret = ObPxReceiveOp::inner_rescan();
  return ret;
}

int ObPxOrderedReceiveOp::try_link_channel()
{
  int ret = OB_SUCCESS;
  if (!channel_linked_) {
    ObPxReceiveOpInput *recv_input = reinterpret_cast<ObPxReceiveOpInput*>(input_);
    if (OB_FAIL(init_channel(*recv_input, task_ch_set_, task_channels_,
                              msg_loop_, px_row_msg_proc_, interrupt_proc_))) {
      LOG_WARN("Fail to init channel", K(ret));
    } else if (OB_FAIL(setup_readers())) {
      LOG_WARN("Fail to setup readers", K(ret));
    } else {
      receive_order_.set_data_channel_idx_range(msg_loop_.get_channel_count() - task_channels_.count(),
                                                msg_loop_.get_channel_count());
      channel_idx_ = 0;
      metric_.set_id(get_spec().id_);
    }
  }
  return ret;
}

int ObPxOrderedReceiveOp::setup_readers()
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
      uint64_t plan_min_cluster_version = ctx_.get_physical_plan_ctx()->get_phy_plan()
          ->get_min_cluster_version();
      common::ObIAllocator *allocator = &ctx_.get_allocator();
      for (int64_t i = 0; i < reader_cnt_; i++) {
        new (&readers_[i]) ObReceiveRowReader(get_spec().id_,
              &(static_cast<const ObPxReceiveSpec &>(spec_).child_exprs_),
              true,
              allocator);
      }
    }
  }
  if (OB_SUCC(ret)) {
    px_row_msg_proc_.set_reader(readers_);
  }
  return ret;
}

void ObPxOrderedReceiveOp::reset_readers()
{
  if (NULL != readers_) {
    for (int64_t i = 0; i < reader_cnt_; i++) {
      readers_[i].reset();
    }
    px_row_msg_proc_.set_reader(readers_);
  }
}

void ObPxOrderedReceiveOp::destroy_readers()
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

int ObPxOrderedReceiveOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  if (iter_end_) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(try_link_channel())) {
    LOG_WARN("failed to init channel", K(ret));
  } else {
    bool wait_next_msg = true;
    while (OB_SUCC(ret) && wait_next_msg) {
      int64_t nth_channel = OB_INVALID_INDEX_INT64;
      do_clear_datum_eval_flag();
      clear_dynamic_const_parent_flag();
      if (channel_idx_ < task_channels_.count()) {
        int64_t idx = channel_idx_;
        ObReceiveRowReader *reader = (NULL != readers_ && idx < reader_cnt_) ? &readers_[idx] : NULL;
        ObDtlChannel *ch = msg_loop_.get_channel(idx + receive_order_.get_data_channel_start_idx());
        if (NULL == ch || NULL == reader) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("reader or channel is NULL");
        } else if (OB_FAIL(ctx_.fast_check_status())) {
          LOG_WARN("failed to check status", K(ret));
        } else if (reader->has_more() || ch->is_eof()) {
          ret = next_row(*reader, wait_next_msg);
          continue;
        } else {
          px_row_msg_proc_.set_reader(reader);
        }
      }

      if (OB_SUCCESS == ret && all_rows_finish_) {
        ret = OB_ITER_END;
        break;
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(msg_loop_.process_one_if(&receive_order_, nth_channel))) {
        if (OB_DTL_WAIT_EAGAIN == ret) {
          LOG_TRACE("no message, try again", K(ret));
          ret = OB_SUCCESS;
          if (channel_idx_ < task_channels_.count()) {
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
        if (ObDtlMsgType::PX_NEW_ROW != msg_type) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected msg type", K(msg_type));
        }
      }
    }
  }

  if (ret == OB_ITER_END && !iter_end_) {
    iter_end_ = true;
  } else if (OB_UNLIKELY(OB_SUCCESS != ret && OB_ITER_END != ret)) {
    LOG_WARN("ordered receive get error", K(ret));
  }
  return ret;
}

int ObPxOrderedReceiveOp::inner_get_next_batch(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  clear_dynamic_const_parent_flag();
  if (iter_end_) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(try_link_channel())) {
    LOG_WARN("failed to init channel", K(ret));
  }
  int64_t read_rows = 0;
  while (OB_SUCC(ret) && 0 == read_rows) {
    int64_t nth_channel = OB_INVALID_INDEX_INT64;
    if (channel_idx_ < task_channels_.count()) {
      int64_t idx = channel_idx_;
      ObReceiveRowReader *reader = (NULL != readers_ && idx < reader_cnt_) ? &readers_[idx] : NULL;
      ObDtlChannel *ch = msg_loop_.get_channel(idx + receive_order_.get_data_channel_start_idx());
      if (NULL == ch || NULL == reader) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("reader or channel is NULL");
      } else if (OB_FAIL(ctx_.fast_check_status())) {
        LOG_WARN("failed to check status", K(ret));
      } else if (reader->has_more() || ch->is_eof()) {
        if (OB_FAIL(next_rows(*reader, max_row_cnt, read_rows))) {
          LOG_WARN("next rows failed", K(ret));
        }
        continue;
      } else {
        px_row_msg_proc_.set_reader(reader);
      }
    }

    if (OB_SUCC(ret) && all_rows_finish_) {
      ret = OB_ITER_END;
      break;
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(msg_loop_.process_one_if(&receive_order_, nth_channel))) {
      if (OB_DTL_WAIT_EAGAIN == ret) {
        LOG_TRACE("no message, try again", K(ret));
        ret = OB_SUCCESS;
        if (channel_idx_ < task_channels_.count()) {
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
      if (ObDtlMsgType::PX_NEW_ROW != msg_type) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected msg type", K(msg_type));
      }
    }
  }
  if (ret == OB_ITER_END) {
    if (!iter_end_) {
      iter_end_ = true;
    }
    ret = OB_SUCCESS;
  }
  brs_.size_ = read_rows;
  brs_.end_ = iter_end_;
  brs_.skip_->reset(brs_.size_);
  brs_.all_rows_active_ = true;
  return ret;
}
int ObPxOrderedReceiveOp::next_row(ObReceiveRowReader &reader, bool &wait_next_msg)
{
  int ret = OB_SUCCESS;
  wait_next_msg = true;
  ret = reader.get_next_row(MY_SPEC.child_exprs_, MY_SPEC.dynamic_const_exprs_, eval_ctx_);
  if (OB_ITER_END == ret) {
    finish_ch_cnt_++;
    channel_idx_++;
    if (OB_LIKELY(finish_ch_cnt_ < task_channels_.count())) {
      receive_order_.set_current_ch_idx(channel_idx_);
      ret = OB_SUCCESS;
    } else if (OB_UNLIKELY(finish_ch_cnt_ > task_channels_.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("All data received. SHOULD NOT see more rows coming",
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

int ObPxOrderedReceiveOp::next_rows(ObReceiveRowReader &reader, int64_t max_row_cnt, int64_t &read_rows)
{
  int ret = OB_SUCCESS;
  read_rows = 0;
  if (MY_SPEC.use_rich_format_) {
    ret = reader.get_next_batch_vec(MY_SPEC.child_exprs_, MY_SPEC.dynamic_const_exprs_, eval_ctx_,
                                    max_row_cnt, read_rows, vector_rows_);
  } else {
    ret = reader.get_next_batch(MY_SPEC.child_exprs_, MY_SPEC.dynamic_const_exprs_, eval_ctx_,
                                max_row_cnt, read_rows, stored_rows_);
  }
  if (OB_ITER_END == ret) {
    finish_ch_cnt_++;
    channel_idx_++;
    if (OB_LIKELY(finish_ch_cnt_ < task_channels_.count())) {
      receive_order_.set_current_ch_idx(channel_idx_);
      ret = OB_SUCCESS;
    } else if (OB_UNLIKELY(finish_ch_cnt_ > task_channels_.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("All data received. SHOULD NOT see more rows coming",
               "finish_task_cnt", finish_ch_cnt_,
               "total_task_chan_cnt", task_channels_.count(),
               K(ret));
    } else {
      LOG_TRACE("All channel finish", "finish_ch_cnt", finish_ch_cnt_, K(ret));
      all_rows_finish_ = true;
      ret = OB_SUCCESS;
    }
  } else if (OB_FAIL(ret)) {
    LOG_WARN("get next batch from row reader failed", K(ret));
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase
