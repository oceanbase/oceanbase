/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_DTL

#include "ob_dtl_utils.h"
#include "ob_dtl_flow_control.h"
#include "sql/dtl/ob_dtl_rpc_channel.h"
#include "share/ob_cluster_version.h"
#include "sql/dtl/ob_dtl.h"
#include "observer/ob_server.h"
#include "sql/dtl/ob_dtl_channel_agent.h"

using namespace oceanbase::common;

namespace oceanbase {
namespace sql {
namespace dtl {

// Accumulated EOF payload size per batch RPC before moving to the next peer slot in one sweep.
static const int64_t DTL_EOF_BATCH_BUFFER_BYTE_THRESHOLD = 32LL << 20;//32MB

int ObDtlAsynSender::calc_batch_buffer_cnt(int64_t &max_batch_size, int64_t &max_loop_cnt)
{
  int ret = OB_SUCCESS;
  int64_t dop = 0;
  int64_t server_cnt = 0;
  int64_t total_task_cnt = 0;
  ObIArray<int64_t> *prefix_task_counts = nullptr;
  const ObAddr &self_addr = GCTX.self_addr();
  bool has_local_channel = false;

  ObDtlExecServer *self_exec_server = nullptr;
  ObDtlExecServer *peer_exec_server = nullptr;
  if (is_transmit_) {
    self_exec_server = &ch_info_->transmit_exec_server_;
    peer_exec_server = &ch_info_->receive_exec_server_;
  } else {
    self_exec_server = &ch_info_->receive_exec_server_;
    peer_exec_server = &ch_info_->transmit_exec_server_;
  }
  dop = self_exec_server->total_task_cnt_;
  prefix_task_counts = &peer_exec_server->prefix_task_counts_;
  server_cnt = peer_exec_server->exec_addrs_.count();
  total_task_cnt = peer_exec_server->total_task_cnt_;

  for (int64_t i = 0; i < server_cnt; ++i) {
    if (peer_exec_server->exec_addrs_.at(i) == self_addr) {
      has_local_channel = true;
      break;
    }
  }
  if (server_cnt == 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: ", K(server_cnt), K(total_task_cnt), K(prefix_task_counts), K(ret));
  } else {
    int64_t queue_size = common::ObServerConfig::get_instance().tenant_task_queue_size;
    int64_t max_buffer_cnt = (queue_size + 1) / 4;
    int64_t dop_per_server = dop / server_cnt + 1;
    // if there is local channel, reduce the server count by 1 since
    // these buffers will not enter tenant queue
    int64_t buffer_cnt = dop_per_server * (has_local_channel ? max(server_cnt - 1, 1) : server_cnt);
    max_batch_size = max(max_buffer_cnt / buffer_cnt, 1);
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
    LOG_TRACE("calc batch size", K_(is_transmit), K(prefix_task_counts),
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

ERRSIM_POINT_DEF(DISABLE_DTL_BATCH_SEND);
int ObDtlAsynSender::asyn_send()
{
  int ret = OB_SUCCESS;
  int64_t max_batch_size = 0;
  int64_t max_loop_times = 0;
  LOG_TRACE("Send eof/drain row", "ch_cnt", channels_.count(), K(ret));
  if (channels_.count() > 1 && !DISABLE_DTL_BATCH_SEND &&
      server_groups_ != nullptr && !server_groups_->empty()) {
    if (OB_FAIL(async_send_batch())) {
      LOG_WARN("failed to async send batch", K(ret));
    }
  } else if (OB_ISNULL(ch_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: ch info is null", K(ret));
  } else if (0 == channels_.count() || OB_FAIL(calc_batch_buffer_cnt(max_batch_size, max_loop_times))) {
    // px coord rescan, channels is cleared and it's empty
    // max_loop_times表示需要进行多少轮才能把所有channels发送完，该值等于接收端每个sqc最大的线程个数
    // max_batch_size表示每次批量发送的channel个数
    // 如 假设接收端2个server，sqc0: 10个task sqc1: 12个task,即channel有22(10+12)
    //    则max_loop_times = 12, 如果max_batch_size=5,则表示需要发送3轮(12/5+1)
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

int ObDtlBatchAsyncSender::async_send_batch()
{
  int ret = OB_SUCCESS;
  ObArray<ObDtlPeerCtlBatch> slots;
  bool need = false;
  if (OB_ISNULL(server_groups_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: server groups is null", K(ret));
  } else if (OB_FAIL(slots.reserve(server_groups_->count()))) {
    LOG_WARN("failed to reserve slots", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < server_groups_->count(); ++i) {
    ObDtlServerChannelGroup *server_group = server_groups_->at(i);
    ObDtlPeerCtlBatch *slot = nullptr;
    if (OB_ISNULL(server_group)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null server group in batch async send", K(ret), K(i), KPC(server_groups_));
    } else if (OB_FAIL(slots.push_back(ObDtlPeerCtlBatch()))) {
      LOG_WARN("push slot failed", K(ret));
    } else {
      slot = &slots.at(slots.count() - 1);
    }
    for (int64_t j = 0; j < server_group->basic_channels_.count() && OB_SUCC(ret); ++j) {
      ObDtlBasicChannel *ch = server_group->basic_channels_.at(j);
      if (OB_ISNULL(ch)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null channel", K(ret));
      } else if (OB_FAIL(check_need_send(ch, need))) {
        LOG_WARN("failed to check channel need send", K(ret));
      } else if (!need) {
      } else if (OB_FAIL(slot->chs_.push_back(ch))) {
        LOG_WARN("failed to push channel", K(ret));
      } else if (OB_FAIL(slot->ch_ids_.push_back(ch->get_peer_id()))) {
        LOG_WARN("failed to push ch id", K(ret));
      } else if (OB_FAIL(slot->posted_flags_.push_back(false))) {
        LOG_WARN("failed to push ch eof posted", K(ret));
      }
    }
  }

  while (OB_SUCC(ret)) {
    ObTMArray<ObWaitChannelInfo> round_wait_channels;
    bool made_progress = false;
    for (int64_t si = 0; OB_SUCC(ret) && si < slots.count(); ++si) {
      ObDtlPeerCtlBatch &slot = slots.at(si);
      if (slot.all_done()) {
        continue;
      } else if (IS_LOCAL_CHANNEL(slot.chs_.at(0))) {
        // process local channel
        for (int64_t i = slot.next_send_ch_idx_; i < slot.ch_ids_.count() && OB_SUCC(ret); ++i) {
          ObDtlChannel *peer_ch = nullptr;
          ObDtlChannel *self_ch = slot.chs_.at(i);
          const int64_t peer_id = slot.ch_ids_.at(i);
          if (OB_FAIL(DTL.get_channel(peer_id, peer_ch))) {
            if (OB_HASH_NOT_EXIST == ret) {
              // channel maybe unregistered, or use interm result
              ret = OB_SUCCESS;
            } else {
              LOG_WARN("failed to get channel", K(ret));
            }
          }
          if (OB_SUCC(ret)) {
            if (OB_FAIL(process_local(self_ch, peer_ch))) {
              LOG_WARN("failed to process local channel", K(ret));
            }
          }
          if (OB_NOT_NULL(peer_ch)) {
            DTL.release_channel(peer_ch);
          }
        }
        if (OB_SUCC(ret)) {
          slot.next_send_ch_idx_ = slot.chs_.count();
          made_progress = true;
        }
      } else {
        batch_msg_.reset();
        if (OB_FAIL(fill_batch_msg(slot))) {
          LOG_WARN("failed fill batch msg", K(ret), K(slot));
        } else if (batch_msg_.ch_ids_.count() == 0) {
          LOG_TRACE("[DTL BATCH] empty batch, skip post", K(si), K(slot.next_send_ch_idx_),
              K(slot.chs_.count()));
        } else if (OB_UNLIKELY(batch_msg_.ch_ids_.count() != slot.cur_batch_ch_slot_indices_.count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected batch ch_ids vs slot index count", K(ret),
              K(batch_msg_.ch_ids_.count()), K(slot.cur_batch_ch_slot_indices_.count()));
        } else {
          ObDtlChannel *carrier_ch = nullptr;
          if (OB_FAIL(post_batch_dtl_msg(slot, carrier_ch))) {
            LOG_WARN("failed to post batch msg", K(ret));
          } else {
            slot.total_sent_payload_bytes_ += batch_msg_.get_accum_payload_bytes();
            if (OB_FAIL(after_send_batch(slot))) {
              LOG_WARN("failed to after send batch", K(ret));
            } else if (OB_ISNULL(carrier_ch)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected null batch carrier channel", K(ret));
            } else {
              ObWaitChannelInfo wait_channel_info;
              wait_channel_info.ch = carrier_ch;
              wait_channel_info.slot = &slot;
              if (OB_FAIL(round_wait_channels.push_back(wait_channel_info))) {
                LOG_WARN("failed to push wait channel", K(ret));
              } else {
                for (int64_t k = 0; k < slot.cur_batch_ch_slot_indices_.count(); ++k) {
                  const int64_t wi = slot.cur_batch_ch_slot_indices_.at(k);
                  slot.posted_flags_.at(wi) = true;
                }
                slot.advance_next_send_ch_idx_prefix();
                made_progress = true;
                LOG_TRACE("[DTL BATCH] send batch msg success", K(si), K(slot.next_send_ch_idx_),
                    K(slot.cur_batch_ch_slot_indices_), K(slot.chs_.count()), K(batch_msg_.get_type()),
                    K(batch_msg_.ch_ids_.count()), K(batch_msg_.get_accum_payload_bytes()));
              }
            }
          }
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(wait_batch_response(round_wait_channels))) {
      LOG_WARN("failed to wait response", K(ret));
    }

    bool all_done = true;
    for (int64_t si = 0; si < slots.count() && OB_SUCC(ret); ++si) {
      const ObDtlPeerCtlBatch &slot = slots.at(si);
      if (!slot.all_done()) {
        all_done = false;
        break;
      }
    }
    if (OB_SUCC(ret) && all_done) {
      break;
    }
    if (OB_SUCC(ret) && !made_progress) {
      bool waited = false;
      for (int64_t si = 0; OB_SUCC(ret) && !waited && si < slots.count(); ++si) {
        ObDtlPeerCtlBatch &slot = slots.at(si);
        if (!slot.all_done() && !IS_LOCAL_CHANNEL(slot.chs_.at(0))) {
          if (OB_FAIL(wait_one_pending_channel(slot, waited))) {
            LOG_WARN("failed to wait for batch progress", K(ret), K(si));
          }
        }
      }
      if (OB_SUCC(ret) && !waited) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected: no progress and nothing to wait on", K(slots.count()), K(ret));
      }
    }
  } // end of while (OB_SUCC(ret))

  if (OB_UNLIKELY(OB_LOGGER.get_log_level() >= OB_LOG_LEVEL_TRACE)) {
    uint64_t cpu_khz = OBSERVER_FREQUENCE.get_cpu_frequency_khz();
    for (int64_t i = 0; i < slots.count(); ++i) {
      ObDtlPeerCtlBatch &slot = slots.at(i);
      slot.max_wait_time_ = slot.max_wait_time_ * 1000 / cpu_khz; //us
      slot.total_wait_time_ = slot.total_wait_time_ * 1000 / cpu_khz; //us
      LOG_TRACE("[DTL BATCH] batch async send finished", K(ret), K(slot), K(batch_msg_));
    }
  }
  return ret;
}

int ObDtlBatchAsyncSender::wait_one_pending_channel(ObDtlPeerCtlBatch &slot, bool &waited)
{
  int ret = OB_SUCCESS;
  waited = false;
  for (int64_t ch_idx = slot.next_send_ch_idx_;
       OB_SUCC(ret) && !waited && ch_idx < slot.chs_.count(); ++ch_idx) {
    if (slot.posted_flags_.at(ch_idx)) {
      continue;
    }
    ObDtlBasicChannel *ch = static_cast<ObDtlBasicChannel *>(slot.chs_.at(ch_idx));
    if (ch->is_in_process() && OB_FAIL(ch->wait_response())) {
      LOG_WARN("wait response while batch stalled", K(ret), K(ch_idx), K(ch->get_id()));
    } else if (ch->is_blocked() && OB_FAIL(ch->wait_unblocking_if_blocked())) {
      LOG_WARN("wait unblocking while batch stalled", K(ret), K(ch_idx), K(ch->get_id()));
    } else {
      waited = true;
      LOG_TRACE("[DTL BATCH] waited blocked channel", K(ch_idx), K(ch->get_id()));
    }
  }
  return ret;
}

int ObDtlBatchAsyncSender::post_batch_dtl_msg(ObDtlPeerCtlBatch &slot, ObDtlChannel *&out_carrier_ch) {
  int ret = OB_SUCCESS;
  out_carrier_ch = nullptr;
  const ObIArray<int64_t> &batch_ch_slot_indices = slot.cur_batch_ch_slot_indices_;
  const int64_t span = batch_ch_slot_indices.count();
  if (OB_UNLIKELY(span <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid empty batch slot indices for post", K(ret));
  }
  for (int64_t j = 0; OB_SUCC(ret) && j < span; ++j) {
    const int64_t idx = batch_ch_slot_indices.at(j);
    if (OB_UNLIKELY(idx < 0 || idx >= slot.chs_.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid batch ch slot index", K(idx), K(slot.chs_.count()), K(ret));
    }
  }

  const int64_t r_offset = ObRandom::rand(0, span - 1);
  ObDtlChannel *chosen_ch = nullptr;
  for (int64_t i = 0; i < span && OB_SUCC(ret); ++i) {
    const int64_t idx = batch_ch_slot_indices.at((i + r_offset) % span);
    ObDtlRpcChannel *ch = static_cast<ObDtlRpcChannel *>(slot.chs_.at(idx));
    if (ch->is_in_process()) {
    } else if (ch->belong_to_transmit_data() && OB_NOT_NULL(ch->get_dfc()) && ch->get_dfc()->is_block(ch)) {
    } else {
      chosen_ch = ch;
      LOG_TRACE("[DTL BATCH] choose the channel that is not in process",
                K(chosen_ch->get_id()), K(ch->belong_to_transmit_data()));
      break;
    }
  }
  if (nullptr == chosen_ch) {
    const int64_t idx = batch_ch_slot_indices.at(r_offset % span);
    chosen_ch = slot.chs_.at(idx);
    LOG_TRACE("[DTL BATCH] all channels are in process, choose the random one", K(chosen_ch->get_id()));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(chosen_ch->send(batch_msg_, timeout_ts_))) {
    LOG_WARN("batch send failed", K(ret));
  } else if (OB_FAIL(chosen_ch->flush(true, false))) {
    LOG_WARN("batch flush failed", K(ret));
  } else {
    out_carrier_ch = chosen_ch;
  }
  return ret;
}

int ObDtlBatchAsyncSender::after_send_batch(ObDtlPeerCtlBatch &slot)
{
  int ret = OB_SUCCESS;
  // free payload buffers
  ObDtlBasicChannel *ch = nullptr;
  for (int64_t i = 0; i < batch_msg_.payload_bufs_.count() && OB_SUCC(ret); ++i) {
    ObDtlLinkedBuffer *&buffer = batch_msg_.payload_bufs_.at(i);
    ch = static_cast<ObDtlBasicChannel *>(batch_msg_.payload_channels_.at(i));
    if (OB_ISNULL(buffer)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL ptr", K(ret));
    } else if (OB_ISNULL(ch)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null payload channel in batch async send",
          K(ret), K(i), K(batch_msg_.payload_bufs_.count()), K(batch_msg_.payload_channels_.count()));
    } else {
      ch->free_linked_buffer(buffer);
      buffer = nullptr;
    }
  }
  return ret;
}

int ObDtlBatchAsyncSender::wait_batch_response(ObIArray<ObWaitChannelInfo> &wait_channels) {
  int ret = OB_SUCCESS;
  int64_t begin_time = 0;
  int64_t end_time = 0;
  bool is_trace_log_level = OB_LOGGER.get_log_level() >= OB_LOG_LEVEL_TRACE;
  for (int64_t w = 0; w < wait_channels.count() && OB_SUCC(ret); ++w) {
    ObWaitChannelInfo &wait_channel_info = wait_channels.at(w);
    ObDtlBasicChannel *w_ch = static_cast<ObDtlBasicChannel *>(wait_channel_info.ch);
    ObDtlPeerCtlBatch *w_slot = wait_channel_info.slot;
    const ObIArray<int64_t> &batch_ch_slot_indices = w_slot->cur_batch_ch_slot_indices_;
    w_slot->wait_count_++;
    if (OB_UNLIKELY(is_trace_log_level)) {
      begin_time = rdtsc();
    }
    if (OB_NOT_NULL(w_ch) && OB_FAIL(w_ch->flush())) {
      LOG_WARN("failed to flush after batch round", K(ret));
    }
    if (OB_UNLIKELY(is_trace_log_level)) {
      end_time = rdtsc();
      int64_t delta = end_time - begin_time;
      w_slot->total_wait_time_ += delta;
      if (delta > w_slot->max_wait_time_) {
        w_slot->max_wait_time_ = delta;
      }
    }
  }
  return ret;
}

int ObTransmitEofAsynSender::check_need_send(ObDtlChannel *ch, bool &need)
{
  int ret = OB_SUCCESS;
  ObPxNewRow px_eof_row;
  px_eof_row.set_eof_row();
  px_eof_row.set_data_type(type_);
  if (OB_FAIL(ch->send(px_eof_row, timeout_ts_, eval_ctx_, true))) { // 这个send会保证send list里面最后都清空
    LOG_WARN("fail send eof row to slice channel", K(px_eof_row), K(ret));
  } else {
    need = true;
  }
  return ret;
}

ERRSIM_POINT_DEF(BATCH_THRESHOLD);
int ObTransmitEofAsynSender::fill_batch_msg(ObDtlPeerCtlBatch &slot)
{
  int ret = OB_SUCCESS;
  slot.cur_batch_ch_slot_indices_.reset();
  const int64_t byte_threshold = BATCH_THRESHOLD
                                     ? abs(BATCH_THRESHOLD)
                                     : DTL_EOF_BATCH_BUFFER_BYTE_THRESHOLD;
  for (int64_t ch_idx = slot.next_send_ch_idx_; OB_SUCC(ret) && ch_idx < slot.chs_.count(); ++ch_idx) {
    if (slot.posted_flags_.at(ch_idx)) {
      continue;
    }
    ObDtlRpcChannel *ch = static_cast<ObDtlRpcChannel *>(slot.chs_.at(ch_idx));
    if (ch->is_in_process() && OB_FAIL(ch->wait_response())) {
      LOG_WARN("failed to wait response", K(ret));
    }
    if (ch->is_blocked()) {
      LOG_TRACE("[DTL BATCH] skip blocked channel in eof batch fill", K(ch_idx), K(ch->get_id()));
      continue;
    }
    ObDtlLinkedBuffer *buffer = nullptr;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ch->pop_write_buffer(buffer))) {
      LOG_WARN("fail force push last buffer", K(ret));
    } else if (buffer == nullptr || !buffer->is_eof()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected buffer", K(buffer));
    } else if (OB_FAIL(batch_msg_.append_payload_buffer(buffer, ch))) {
      LOG_WARN("failed to append payload buffer", K(ret));
    } else if (OB_FAIL(batch_msg_.ch_ids_.push_back(slot.ch_ids_.at(ch_idx)))) {
      LOG_WARN("failed to push back ch_id", K(ret));
    } else if (OB_FAIL(slot.cur_batch_ch_slot_indices_.push_back(ch_idx))) {
      LOG_WARN("failed to push cur batch slot index", K(ret));
    } else if (batch_msg_.get_accum_payload_bytes() >= byte_threshold) {
      LOG_TRACE("[DTL BATCH] fill batch msg success", K(batch_msg_), K(slot));
      break;
    }
  }
  if (OB_FAIL(ret)) {
    for (int64_t i = 0; i < batch_msg_.payload_bufs_.count(); ++i) {
      ObDtlLinkedBuffer *buffer = batch_msg_.payload_bufs_.at(i);
      if (OB_NOT_NULL(buffer)) {
        ObDtlBasicChannel *ch = static_cast<ObDtlBasicChannel *>(slot.chs_.at(0));
        ch->free_linked_buffer(buffer);
      }
    }
    batch_msg_.reset();
  }
  return ret;
}

int ObDfcDrainAsynSender::process_local(ObDtlChannel *self_ch, ObDtlChannel *peer_ch)
{
  int ret = OB_SUCCESS;
  if (self_ch->use_interm_result()) {
    // if use dtl interm result peer channel is not exist
  } else if (OB_NOT_NULL(peer_ch) && OB_NOT_NULL(peer_ch->get_dfc())) {
    peer_ch->get_dfc()->set_drain(static_cast<ObDtlBasicChannel *>(peer_ch));
  }
  return ret;
}

int ObTransmitEofAsynSender::process_local(ObDtlChannel *self_ch, ObDtlChannel *peer_ch)
{
  // for local channel directly send
  UNUSED(peer_ch);
  int ret = OB_SUCCESS;
  if (OB_FAIL(self_ch->flush(true, false))) {
    LOG_WARN("failed to flush send msg", K(ret));
  }
  return ret;
}

int ObDfcUnblockAsynSender::check_need_send(ObDtlChannel *ch, bool &need)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(dfc_.need_unblock_peer(ch, unblock_cnt_, need))) {
    LOG_WARN("failed to try stage unblock for batch", K(ret));
  }
  return ret;
}

int ObDfcUnblockAsynSender::process_local(ObDtlChannel *self_ch, ObDtlChannel *peer_ch)
{
  int ret = OB_SUCCESS;
  if (self_ch->use_interm_result()) {
    // if use dtl interm result peer channel is not exist
  } else if (OB_NOT_NULL(peer_ch) && OB_NOT_NULL(peer_ch->get_dfc())) {
    if (peer_ch->get_dfc()->is_block(peer_ch)) {
      // transmit's message response is already processed,
      // we can unblock the channel directly
      peer_ch->get_dfc()->unblock_channel(peer_ch);
      LOG_TRACE("[DTL BATCH BLOCK] unblock channel", K(peer_ch->get_id()), K(peer_ch->get_peer()));
    } else {
      // response message is not processed yet, we should send a unblocking msg
      // for later processing
      ObDtlUnblockingMsg unblocking_msg;
      if (self_ch->is_drain()) {
      } else if (OB_FAIL(self_ch->send(unblocking_msg, timeout_ts_))) {
        LOG_WARN("failed to push data to channel", K(ret), KP(self_ch->get_id()), K(self_ch->get_peer()));
      } else if (OB_FAIL(self_ch->flush(true, false))) {
        LOG_WARN("failed to flush unblocking msg", K(ret), KP(self_ch->get_id()), K(self_ch->get_peer()));
      }
      LOG_TRACE("[DTL BATCH BLOCK] send local unblocking msg", K(peer_ch->get_id()), K(peer_ch->get_peer()));
    }
  }
  return ret;
}

}  // dtl
}  // sql
}  // oceanbase
