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

#define USING_LOG_PREFIX SQL_DTL

#include "ob_dtl_utils.h"
#include "sql/engine/px/ob_px_row_store.h"
#include "ob_dtl_flow_control.h"

using namespace oceanbase::common;

namespace oceanbase {
namespace sql {
namespace dtl {

int ObDtlAsynSender::calc_batch_buffer_cnt(int64_t &max_batch_size, int64_t &max_loop_cnt)
{
  int ret = OB_SUCCESS;
  int64_t dop = 0;
  int64_t server_cnt = 0;
  int64_t total_task_cnt = 0;
  ObIArray<int64_t> *prefix_task_counts = nullptr;
  if (is_transmit_) {
    dop = ch_info_->transmit_exec_server_.total_task_cnt_;
    prefix_task_counts = &ch_info_->receive_exec_server_.prefix_task_counts_;
    server_cnt = ch_info_->receive_exec_server_.exec_addrs_.count();
    total_task_cnt = ch_info_->receive_exec_server_.total_task_cnt_;
  } else {
    dop = ch_info_->receive_exec_server_.total_task_cnt_;
    prefix_task_counts = &ch_info_->transmit_exec_server_.prefix_task_counts_;
    server_cnt = ch_info_->transmit_exec_server_.exec_addrs_.count();
    total_task_cnt = ch_info_->transmit_exec_server_.total_task_cnt_;
  }
  if (server_cnt == 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: ", K(server_cnt), K(total_task_cnt), K(prefix_task_counts), K(ret));
  } else {
    int64_t queue_size = common::ObServerConfig::get_instance().tenant_task_queue_size;
    int64_t max_buffer_cnt = (queue_size + 1) / 4;
    int64_t dop_per_server = dop / server_cnt + 1;
    max_batch_size = 1;
    int64_t buffer_cnt = dop_per_server * server_cnt;
    while (buffer_cnt * (max_batch_size + 1) < max_buffer_cnt) {
      ++max_batch_size;
    }
    max_loop_cnt = 0;
    for (int64_t i = 0; i < prefix_task_counts->count() && OB_SUCC(ret); ++i) {
      int64_t count = 0;
      if (i == prefix_task_counts->count() - 1) {
        count = total_task_cnt - prefix_task_counts->at(i);
      } else {
        count = prefix_task_counts->at(i + 1) - prefix_task_counts->at(i);
      }
      if (max_loop_cnt < count) {
        max_loop_cnt = count;
      }
    }
    max_batch_size = min(max_batch_size, max_loop_cnt);
    LOG_DEBUG("calc batch size", K_(is_transmit), K(prefix_task_counts),
              K(server_cnt), K(total_task_cnt), K(prefix_task_counts->count()),
              K(max_batch_size), K(dop), K(server_cnt),
              K(max_loop_cnt), K(max_buffer_cnt), K(dop_per_server), K(lbt()));
  }
  return ret;
}

int ObDtlAsynSender::syn_send()
{
  int ret = OB_SUCCESS;
  dtl::ObDtlChannel *ch = NULL;
  for (int64_t slice_idx = 0; (OB_SUCCESS == ret) && slice_idx < channels_.count(); ++slice_idx) {
    if (NULL == (ch = channels_.at(slice_idx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL ptr", K(ret));
    } else if (OB_FAIL(action(ch))) {
      LOG_WARN("failed to send message", K(ret));
    } else if (OB_FAIL(ch->wait_response())) {
      LOG_WARN("failed to wait response", K(ret));
    }
  }
  return ret;
}

int ObDtlAsynSender::asyn_send()
{
  int ret = OB_SUCCESS;
  int64_t max_batch_size = 0;
  int64_t max_loop_times = 0;
  LOG_TRACE("Send eof/drain row", "ch_cnt", channels_.count(), K(ret));
  if (OB_ISNULL(ch_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: ch info is null", K(ret));
  } else if (0 == channels_.count() || OB_FAIL(calc_batch_buffer_cnt(max_batch_size, max_loop_times))) {
    // px coord rescan, channels is cleared and it's empty
    // max_loop_times表示需要进行多少轮才能把所有channels发送完，该值等于接收端每个sqc最大的线程个数
    // max_batch_size表示每次批量发送的channel个数
    // 如 假设接收端2个server，sqc0: 10个task sqc1: 12个task,即channel有22(10+12)
    //    则max_loop_times = 22, 如果max_batch_size=5,则表示需要发送3轮(12/5+1)
    //    如果max_batch_size等于(不会大于max_loop_times)max_loop_times，表示一次性发送完所有channel的数据
    // 回退到同步发送
    if (OB_FAIL(syn_send())) {
      LOG_WARN("failed to syn send message", K(ret));
    }
    LOG_TRACE("failed to calc batch buffer cnt", K(ret));
  } else {
    dtl::ObDtlChannel *ch = NULL;
    int tmp_ret = OB_SUCCESS;
    ObArray<ObDtlChannel*> wait_channels;
    ObIArray<int64_t> *prefix_task_counts = nullptr;
    int64_t total_task_cnt = 0;
    if (is_transmit_) {
      prefix_task_counts = &ch_info_->receive_exec_server_.prefix_task_counts_;
      total_task_cnt = ch_info_->receive_exec_server_.total_task_cnt_;
    } else {
      prefix_task_counts = &ch_info_->transmit_exec_server_.prefix_task_counts_;
      total_task_cnt = ch_info_->transmit_exec_server_.total_task_cnt_;
    }
    if (OB_FAIL(wait_channels.prepare_allocate(prefix_task_counts->count() * max_batch_size))) {
      LOG_WARN("fail alloc memory", K(max_batch_size), K(prefix_task_counts->count()), K(ret));
    }
    int64_t send_eof_cnt = 0;
    for (int64_t loop = 0; loop < max_loop_times && OB_SUCC(ret); loop += max_batch_size) {
      ch = nullptr;
      int64_t nth_ch = 0;
      for (int64_t i = 0; i < prefix_task_counts->count() && OB_SUCC(ret); ++i) {
        int64_t count = 0;
        if (i == prefix_task_counts->count() - 1) {
          count = total_task_cnt - prefix_task_counts->at(i);
        } else {
          count = prefix_task_counts->at(i + 1) - prefix_task_counts->at(i);
        }
        for (int64_t batch_idx = 0; batch_idx < max_batch_size && OB_SUCC(ret); ++batch_idx) {
          if (loop + batch_idx < count) {
            int64_t slice_idx = prefix_task_counts->at(i) + loop + batch_idx;
            if (NULL == (ch = channels_.at(slice_idx))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected NULL ptr", K(ret));
            } else {
              wait_channels.at(nth_ch) = ch;
              ++nth_ch;
              ++send_eof_cnt;
              if (OB_FAIL(action(ch))) {
                tmp_ret = ret;
                ret = OB_SUCCESS;
                LOG_WARN("failed to send", K(ret));
              }
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        for (int64_t wait = 0; wait < nth_ch && OB_SUCC(ret); wait++) {
          ch = wait_channels.at(wait);
          if (OB_NOT_NULL(ch) && OB_FAIL(ch->flush())) {
            tmp_ret = ret;
            ret = OB_SUCCESS;
            LOG_WARN("failed to wait", K(ret), K(loop), K(max_loop_times), K(max_batch_size),
              K(channels_.count()));
          }
        }
      }
    }
    if (OB_SUCC(ret) && send_eof_cnt != channels_.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: send eof failed", K(ret),
        K(send_eof_cnt), K(channels_.count()), K(max_batch_size), K(max_loop_times));
    }
    if (OB_SUCC(ret) && OB_SUCCESS != tmp_ret) {
      ret = tmp_ret;
    }
  }
  return ret;
}

int ObTransmitEofAsynSender::action(ObDtlChannel* ch)
{
  int ret = OB_SUCCESS;
  ObPxNewRow px_eof_row;
  px_eof_row.set_eof_row();
  px_eof_row.set_data_type(type_);
  if (OB_FAIL(ch->send(px_eof_row, timeout_ts_, eval_ctx_, true))) {
    LOG_WARN("fail send eof row to slice channel", K(px_eof_row), K(ret));
  } else if (OB_FAIL(ch->flush(true, false))) {
    LOG_WARN("failed to flush send msg", K(px_eof_row), K(ret));
  }
  return ret;
}

int ObDfcDrainAsynSender::action(ObDtlChannel* ch)
{
  int ret = OB_SUCCESS;
  ObDtlDrainMsg drain_msg;
  LOG_TRACE("drain channel", K(ret), KP(ch->get_id()), K(ch->get_peer()));
  if (OB_FAIL(ch->send(drain_msg, timeout_ts_))) {
    LOG_WARN("failed to push data to channel", K(ret), KP(ch->get_id()), K(ch->get_peer()));
  } else if (OB_FAIL(ch->flush(true, false))) {
    LOG_WARN("failed to drain msg", K(ret));
  }
  return ret;
}

int ObDfcUnblockAsynSender::action(ObDtlChannel *ch)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(dfc_.notify_channel_unblocking(ch, unblock_cnt_))) {
    LOG_WARN("failed to notify channel unblocking", K(ret));
  }
  return ret;
}

}  // dtl
}  // sql
}  // oceanbase
