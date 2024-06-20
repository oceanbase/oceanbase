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

#include "ob_dtl_flow_control.h"
#include "share/ob_errno.h"
#include "ob_dtl_basic_channel.h"
#include "sql/engine/px/ob_sqc_ctx.h"
#include "ob_dtl_channel_loop.h"
#include "ob_dtl_utils.h"
#include "observer/omt/ob_tenant_config_mgr.h"

using namespace oceanbase::common;
using namespace oceanbase::omt;
using namespace oceanbase::sql;
using namespace oceanbase::sql::dtl;

OB_SERIALIZE_MEMBER(ObDtlUnblockingMsg);

OB_SERIALIZE_MEMBER(ObDtlDrainMsg);


int ObDtlUnblockingMsgP::process(const ObDtlUnblockingMsg &pkt)
{
  int ret = OB_SUCCESS;
  UNUSED(pkt);
  LOG_TRACE("unblocking data flow start", K(lbt()), K(ret), K(&dfc_), K(dfc_.is_block()));
  LOG_TRACE("unblocking data flow end", K(lbt()), K(ret), K(&dfc_), K(dfc_.is_block()));
  return ret;
}

int ObDtlDrainMsgP::process(const ObDtlDrainMsg &pkt)
{
  int ret = OB_SUCCESS;
  UNUSED(pkt);
  LOG_ERROR("drain data flow start", K(lbt()), K(ret), K(&dfc_));
  return ret;
}

int ObDtlFlowControl::init(uint64_t tenant_id, int64_t chan_cnt)
{
  int ret = OB_SUCCESS;
  if (is_init_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("init again", K(ret));
  } else if (OB_FAIL(chans_.reserve(chan_cnt))) {
    LOG_WARN("failed to reserve data", K(ret));
  } else if (OB_FAIL(blocks_.reserve(chan_cnt))) {
    LOG_WARN("failed to reserve data", K(ret));
  } else {
    ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
    if (tenant_config.is_valid() && true == tenant_config->_px_message_compression) {
      compressor_type_ = ObCompressorType::LZ4_COMPRESSOR;
    }
    is_init_ = true;
    tenant_id_ = tenant_id;
    timeout_ts_ = 0;
    communicate_flag_ = 0;
    block_ch_cnt_ = 0;
    total_memory_size_ = 0;
    total_buffer_cnt_ = 0;
    accumulated_blocked_cnt_ = 0;
  }
  return ret;
}

bool ObDtlFlowControl::is_all_channel_act()
{
  bool all_act = true;
  for (int64_t i = 0; i < chans_.count(); ++i) {
    if (OB_NOT_NULL(chans_.at(i))) {
      if ((static_cast<ObDtlBasicChannel *>(chans_.at(i)))->get_recv_buffer_cnt() == 0) {
        all_act = false;
        break;
      }
    }
  }
  return all_act;
}

int ObDtlFlowControl::register_channel(ObDtlChannel* ch)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(chans_.push_back(ch))) {
    LOG_WARN("failed to register channel", K(ret));
  } else if (OB_FAIL(blocks_.push_back(false))) {
    LOG_WARN("failed to register channel", K(ret));
  } else if (OB_ISNULL(chan_loop_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: channel loop is null", K(ret));
  } else {
    ch->set_dfc(this);
    ch->set_dfc_idx(chans_.count() - 1);
    chan_loop_->register_channel(*ch);
  }
  return ret;
}

int ObDtlFlowControl::unregister_channel(ObDtlChannel* channel)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t find_idx = -1;
  for (int64_t i = 0; i < chans_.count(); ++i) {
    if (channel == chans_[i]) {
      find_idx = i;
      break;
    }
  }
  if (find_idx >= 0) {
    if (OB_FAIL(channel->clean_recv_list())) {
      LOG_WARN("failed to clean channel", K(ret));
    }
    channel->set_dfc(nullptr);
    if (OB_SUCCESS != (tmp_ret = chans_.remove(find_idx))) {
      ret = tmp_ret;
      LOG_WARN("failed to remove channel", K(ret));
    }
  } else {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("channel not exist", K(channel), K(ret));
  }
  bool block = false;
  while (blocks_.count() > chans_.count()) {
    if (OB_SUCCESS != (tmp_ret = blocks_.pop_back(block))) {
      ret = tmp_ret;
      LOG_WARN("failed to pop back block flag", K(ret), K(tmp_ret));
    }
  }
  return ret;
}

int ObDtlFlowControl::unregister_all_channel()
{
  int ret = OB_SUCCESS;
  ObDtlChannel* ch = nullptr;
  // 这里不能同时pop出来，否则clean recv list时，根据ch去clean
  for (int i = 0; i < chans_.count(); ++i) {
    if (nullptr == (ch = chans_.at(i))) {
      LOG_WARN("failed to unregister channel", K(ret));
    } else if (OB_FAIL(ch->clean_recv_list())) {
      LOG_WARN("failed to clean channel", K(ret));
    }
  }
  for (int64_t i = chans_.count() - 1; 0 <= i; --i) {
    if (OB_FAIL(chans_.pop_back(ch))) {
      // overwrite ret
      LOG_WARN("failed to unregister channel", K(ret));
    }
  }
  if (is_receive() && (0 != get_blocked_cnt() || 0 != get_total_buffer_cnt() || 0 != get_used())) {
    LOG_WARN("unexpected dfc status", K(chans_.count()), K(ret), K(get_blocked_cnt()), K(get_total_buffer_cnt()), K(get_used()), K(get_accumulated_blocked_cnt()));
  }
  LOG_TRACE("unregister all channel", K(chans_.count()), K(ret), K(get_blocked_cnt()), K(get_total_buffer_cnt()), K(get_used()), K(get_accumulated_blocked_cnt()));
  return ret;
}

int ObDtlFlowControl::get_channel(int64_t idx, ObDtlChannel *&ch)
{
  int ret = OB_SUCCESS;
  ch = nullptr;
  if (0 > idx || idx >= chans_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get channel", K(ret));
  } else {
    ch = chans_.at(idx);
  }
  return ret;
}

int ObDtlFlowControl::find(ObDtlChannel* ch, int64_t &out_idx)
{
  int ret = OB_SUCCESS;
  out_idx = OB_INVALID_ID;
  ARRAY_FOREACH_X(chans_, idx, cnt, OB_INVALID_ID == out_idx) {
    if (ch == chans_.at(idx)) {
      out_idx = idx;
    }
  }
  if (OB_INVALID_ID == out_idx) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("channel not found", K(ch), KP(ch->get_id()), K(ch->get_peer()), K(out_idx),
      K(chans_.count()));
  }
  return ret;
}

bool ObDtlFlowControl::is_block(ObDtlChannel* ch)
{
  int ret = OB_SUCCESS;
  bool blocked = false;
  if (OB_ISNULL(ch)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("channel is null", K(ret));
  } else {
    int64_t idx =  OB_INVALID_ID;
    if (OB_FAIL(find(ch, idx))) {
      LOG_WARN("channel not exists in channel loop", K(ret));
    } else {
      blocked = is_block(idx);
    }
  }
  return blocked;
}

int ObDtlFlowControl::block_channel(ObDtlChannel* ch)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ch)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("channel is null", K(ret));
  } else {
    int64_t idx = OB_INVALID_ID;
    if (OB_FAIL(find(ch, idx))) {
      LOG_WARN("channel not exists in channel loop", K(ret));
    } else if (is_block(idx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("channel is blocked", K(ret), K(idx));
    } else {
      set_block(idx);
      ch->set_blocked();
      LOG_TRACE("transmit set channel block trace", K(ch), KP(ch->get_id()), K(ch->get_peer()), K(idx));
    }
  }
  return ret;
}

int ObDtlFlowControl::unblock_channel(ObDtlChannel* ch)
{
  int ret = OB_SUCCESS;
  int64_t idx = OB_INVALID_ID;
  if (OB_FAIL(find(ch, idx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("channel is null", K(ret), K(idx), KP(ch->get_id()), K(ch->get_peer()));
  } else {
    // 必须等待该channel的response先回包后才能处理block消息，否则可能导致unblocking msg先到达，处理后，response再到达
    // 这样channel的状态是unblock，所以没有执行unblock状态，后面response到达时，发现response为is_block设置了block状态，之后永远等待不到unblocking msg
    // 这里需要将response的is_block还原，即unblocking msg，则response的block状态应该清理掉
    if (OB_FAIL(ch->clear_response_block())) {
      LOG_WARN("failed to clear response block info", K(ret));
    } else if (is_block(idx)) {
      unblock(idx);
      ch->unset_blocked();
    }
    LOG_TRACE("channel unblock", K(ch), KP(ch->get_id()), K(ch->get_peer()), K(idx), K(is_block(idx)));
  }
  return ret;
}

int ObDtlFlowControl::notify_channel_unblocking(
  ObDtlChannel *ch, int64_t &block_cnt, bool asyn_send)
{
  int ret = OB_SUCCESS;
  int64_t idx = OB_INVALID_ID;
  ObDtlUnblockingMsg unblocking_msg;
  if (OB_FAIL(find(ch, idx))) {
    LOG_WARN("failed to find channel", K(ret), KP(ch->get_id()), K(ch->get_peer()), K(idx), K(get_blocked_cnt()));
  } else if (!is_block(idx)) {
    LOG_TRACE("channel is unblock", K(ret), KP(ch->get_id()), K(ch->get_peer()),
        K(get_nth_block(idx)), K(get_blocked_cnt()));
  } else {
    if (OB_FAIL(ch->wait_response())) {
      if (OB_ERR_SIGNALED_IN_PARALLEL_QUERY_SERVER == ret) {
        ret = OB_SUCCESS;
      }
    }
    ++block_cnt;
    unblock(idx);
    LOG_TRACE("trace unblocking channel", K(ret), KP(ch->get_id()), K(ch->get_peer()),
      K(idx), K(is_block(idx)), K(get_nth_block(idx)), K(get_blocked_cnt()));
    // control msg
    if (OB_FAIL(ret)) {
    } else if (ch->is_drain()) {
    } else if (OB_FAIL(ch->send(unblocking_msg, timeout_ts_))) {
      LOG_WARN("failed to push data to channel", K(ret), KP(ch->get_id()), K(ch->get_peer()));
    } else if (OB_FAIL(ch->flush(true, false))) {
      LOG_TRACE("failed to flush unblocking msg", K(ret), KP(ch->get_id()), K(ch->get_peer()));
    } else if (!asyn_send && OB_FAIL(ch->wait_response())) {
      LOG_TRACE("failed to wait response", K(ret), K(ch->get_peer()));
    }
  }
  LOG_TRACE("channel status", K(this), K(ret), KP(ch->get_id()), K(ch->get_peer()), K(idx),
    K(is_block(idx)), K(get_nth_block(idx)), K(get_blocked_cnt()));
  // 之前通过OB_ERR_SIGNALED_IN_PARALLEL_QUERY_SERVER状态来决定是否结束，但可能收到msg后，下游可能还有线程没有起来，感觉有风险
  if (OB_ERR_SIGNALED_IN_PARALLEL_QUERY_SERVER == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObDtlFlowControl::sync_send_drain(int64_t &unblock_cnt)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  unblock_cnt = 0;
  LOG_TRACE("unblocking dfc", K(ret), K(is_block()));
  // broadcast send unblocking msg
  // if return OB_ERR_SIGNALED_IN_PARALLEL_QUERY_SERVER, channel already finish
  ARRAY_FOREACH_X(chans_, idx, cnt, OB_SUCC(ret)) {
    ObDtlChannel *ch = chans_.at(idx);
    if (!is_block(idx)) {
      // it's no block
      LOG_TRACE("channel unblock", K(ret), KP(ch->get_id()), K(ch->get_peer()),
        K(get_nth_block(idx)), K(get_blocked_cnt()));
    } else {
      // 这里必须先设置unblock，再发送unblocking消息
      // 否则可能被覆盖：
      // 如:
      //  ch1 操作                                                                ch2操作
      //  ch1 发送msg1 to ch2  blocked住
      //                                                                        ch2 unblocking msg to ch1
      //  ch1 unblock后再发送msg2 ,blocked
      //                                                                        ch2 设置unblock状态
      //  这样ch1的这个msg2的block就无法再收到ch2的unblocking msg，因为flag为false了
      LOG_TRACE("unblocking channel", K(ret), KP(ch->get_id()), K(ch->get_peer()),
        K(idx), K(cnt), K(is_block(idx)), K(get_nth_block(idx)), K(get_blocked_cnt()));
      if (OB_FAIL(notify_channel_unblocking(ch, unblock_cnt, false))) {
        LOG_WARN("failed to unblocking channel", K(ret), KP(ch->get_id()), K(ch->get_peer()));
      }
    }
    LOG_TRACE("channel status", K(this), K(ret), KP(ch->get_id()), K(ch->get_peer()), K(idx),
      K(cnt), K(is_block(idx)), K(get_nth_block(idx)), K(get_blocked_cnt()));
    // 之前通过OB_ERR_SIGNALED_IN_PARALLEL_QUERY_SERVER状态来决定是否结束，但可能收到msg后，下游可能还有线程没有起来，感觉有风险
    if (OB_FAIL(ret)) {
      tmp_ret = ret;
      ret = OB_SUCCESS;
    }
  }
  if (OB_FAIL(tmp_ret)) {
    ret = tmp_ret;
  }
  return ret;
}

int ObDtlFlowControl::notify_all_blocked_channels_unblocking(int64_t &unblock_cnt)
{
  int ret = OB_SUCCESS;
  unblock_cnt = 0;
  LOG_TRACE("unblocking dfc", K(ret), K(is_block()));
  if (OB_ISNULL(ch_info_)) {
    if (OB_FAIL(sync_send_drain(unblock_cnt))) {
      LOG_WARN("failed to sync send drain", K(ret));
    }
  } else {
    ObDfcUnblockAsynSender asyn_sender(chans_, ch_info_, is_transmit(), *this);
    if (OB_FAIL(asyn_sender.asyn_send())) {
      LOG_WARN("failed to asyn send unblocking msg", K(ret));
    }
    unblock_cnt = asyn_sender.get_unblocked_cnt();
  }
  return ret;
}

int ObDtlFlowControl::drain_all_channels()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (OB_NOT_NULL(ch_info_)) {
    if (ch_info_->transmit_exec_server_.total_task_cnt_ != chans_.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: ch info is null", K(ret), KP(ch_info_), K(chans_.count()));
    } else {
      ObDfcDrainAsynSender drain_asyn_sender(chans_, ch_info_, false, timeout_ts_);
      if (OB_FAIL(drain_asyn_sender.asyn_send())) {
        LOG_WARN("failed to asyn send drain", K(ret), K(lbt()));
      }
      ARRAY_FOREACH_X(chans_, idx, cnt, OB_SUCC(ret)) {
        ObDtlChannel *ch = chans_.at(idx);
        LOG_TRACE("drain channel", K(ret), KP(ch->get_id()), K(ch->get_peer()), K(idx));
        ch->set_drain();
      }
    }
  } else {
    ARRAY_FOREACH_X(chans_, idx, cnt, OB_SUCC(ret)) {
      ObDtlChannel *ch = chans_.at(idx);
      ObDtlDrainMsg drain_msg;
      LOG_TRACE("drain channel", K(ret), KP(ch->get_id()), K(ch->get_peer()), K(idx));
      if (OB_FAIL(ch->send(drain_msg, timeout_ts_))) {
        LOG_WARN("failed to push data to channel", K(ret), KP(ch->get_id()), K(ch->get_peer()));
      } else if (OB_FAIL(ch->flush(true))) {
        LOG_WARN("failed to drain msg", K(ret));
      }
      // 这里必须先发送，然后set因为channel如果已经drain不会再发数据了
      ch->set_drain();
      if (OB_FAIL(ret)) {
        tmp_ret = ret;
        ret = OB_SUCCESS;
      }
    }
    if (OB_FAIL(tmp_ret)) {
      ret = tmp_ret;
    }
  }
  return ret;
}

// If run successfully, then both total_cnt_ total_memory_size_ are zero
int ObDtlFlowControl::final_check()
{
  int ret = OB_SUCCESS;
  if (0 != total_buffer_cnt_ || 0 != total_memory_size_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Expect 0 when run over", K(ret));
  }
  return ret;
}

int ObDtlFlowControl::set_drain(ObDtlBasicChannel *channel)
{
  int ret = OB_SUCCESS;
  channel->set_drain();
  ATOMIC_FAA(&drain_ch_cnt_, 1);
  return ret;
}

bool ObDtlFlowControl::is_drain(ObDtlBasicChannel *channel)
{
  return channel->is_drain();
}

