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
#include "ob_dtl_rpc_channel.h"
#include "share/interrupt/ob_global_interrupt_call.h"
#include "lib/oblog/ob_log.h"
#include "lib/lock/ob_thread_cond.h"
#include "common/row/ob_row.h"
#include "sql/dtl/ob_dtl_rpc_proxy.h"
#include "sql/dtl/ob_dtl.h"
#include "sql/dtl/ob_dtl_flow_control.h"
#include "sql/dtl/ob_dtl_channel_agent.h"
#include "share/rc/ob_context.h"
#include "sql/dtl/ob_dtl_channel_watcher.h"

using namespace oceanbase::common;
using namespace oceanbase::share;

namespace oceanbase {
namespace sql {
namespace dtl {

void ObDtlRpcChannel::SendMsgCB::on_invalid()
{
  LOG_WARN_RET(OB_ERROR, "SendMsgCB invalid, check object serialization impl or oom",
           K_(trace_id));
  AsyncCB::on_invalid();
  const ObDtlRpcDataResponse &resp = result_;
  int ret = response_.on_finish(resp.is_block_, OB_RPC_PACKET_INVALID);
  if (OB_FAIL(ret)) {
    LOG_WARN("set finish failed", K_(trace_id), K(ret));
  }
}

void ObDtlRpcChannel::SendMsgCB::on_timeout()
{
  const ObDtlRpcDataResponse &resp = result_;
  int tmp_ret = OB_TIMEOUT;
  int64_t cur_timestamp = ::oceanbase::common::ObTimeUtility::current_time();
  if (timeout_ts_ - cur_timestamp > 100 * 1000) {
    LOG_DEBUG("rpc return OB_TIMEOUT, but it is actually not timeout, "
              "change error code to OB_CONNECT_ERROR", K(tmp_ret),
              K(timeout_ts_), K(cur_timestamp));
    tmp_ret = OB_RPC_CONNECT_ERROR;
  }
  int ret = response_.on_finish(resp.is_block_, tmp_ret);
  LOG_WARN("SendMsgCB timeout, if negtive timeout, check peer cpu load, network packet drop rate",
           K_(trace_id), K(ret));
  if (OB_FAIL(ret)) {
    LOG_WARN("set finish failed", K_(trace_id), K(ret), K(get_error()));
  }
}

int ObDtlRpcChannel::SendMsgCB::process()
{
  const ObDtlRpcDataResponse &resp = result_;
  // if request queue is full or serialize faild, then rcode is set, and rpc process is not called
  int tmp_ret = OB_SUCCESS != rcode_.rcode_ ? rcode_.rcode_ : resp.recode_;
  int ret = response_.on_finish(resp.is_block_, tmp_ret);
  if (OB_FAIL(ret)) {
    LOG_WARN("set finish failed", K_(trace_id), K(ret));
  }
  return ret;
}

rpc::frame::ObReqTransport::AsyncCB *ObDtlRpcChannel::SendMsgCB::clone(
    const rpc::frame::SPAlloc &alloc) const
{
  SendMsgCB *cb = NULL;
  void *mem = alloc(sizeof(*this));
  if (NULL != mem) {
    cb = new(mem)SendMsgCB(response_, trace_id_, timeout_ts_);
  }
  return cb;
}

///////////////////////////////////////////////////////////////////////////////

void ObDtlRpcChannel::SendBCMsgCB::on_invalid()
{
  LOG_WARN_RET(OB_ERROR, "SendBCMsgCB invalid, check object serialization impl or oom",
           K_(trace_id));
  AsyncCB::on_invalid();
  ObIArray<ObDtlRpcDataResponse> &resps = result_.resps_;
  for (int64_t i = 0; i < responses_.count(); ++i) {
    int ret_code = (OB_SUCCESS != rcode_.rcode_)
                    ? rcode_.rcode_
                    : (i < resps.count() ? resps.at(i).recode_ : OB_RPC_PACKET_INVALID);
    int ret = responses_.at(i)->on_finish(false, ret_code);
    if (OB_FAIL(ret)) {
      LOG_WARN("set finish failed", K(ret), K(resps.count()), K(responses_.count()), K(trace_id_));
    }
  }
  resps.reset();
}

void ObDtlRpcChannel::SendBCMsgCB::on_timeout()
{
  LOG_WARN_RET(OB_TIMEOUT, "SendBCMsgCB timeout, if negtive timeout, check peer cpu load, network packet drop rate",
           K_(trace_id));
  ObIArray<ObDtlRpcDataResponse> &resps = result_.resps_;
  for (int64_t i = 0; i < responses_.count(); ++i) {
    int ret_code = (OB_SUCCESS != rcode_.rcode_)
                    ? rcode_.rcode_
                    : (i < resps.count() ? resps.at(i).recode_ : OB_TIMEOUT);
    int ret = responses_.at(i)->on_finish(false, ret_code);
    if (OB_FAIL(ret)) {
      LOG_WARN("set finish failed", K(ret), K(resps.count()), K(responses_.count()), K(trace_id_));
    }
  }
  resps.reset();
  destroy();
}

int ObDtlRpcChannel::SendBCMsgCB::process()
{
  int ret = OB_SUCCESS;
  ObIArray<ObDtlRpcDataResponse> &resps = result_.resps_;
  if (resps.count() != responses_.count()) {
    LOG_WARN("unexpected status: response count is not match",
      K(resps.count()), K(responses_.count()));
  }
  // if request queue is full or serialize faild, then rcode is set, and rpc process is not called
  for (int64_t i = 0; i < responses_.count(); ++i) {
    bool is_block = i < resps.count() ? resps.at(i).is_block_ : false;
    int ret_code = (OB_SUCCESS != rcode_.rcode_)
                  ? rcode_.rcode_
                  : (i < resps.count() ? resps.at(i).recode_ : OB_ERR_UNEXPECTED);
    int tmp_ret = responses_.at(i)->on_finish(is_block, ret_code);
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("set finish failed", K(ret), K(resps.count()), K(responses_.count()), K(trace_id_));
    }
    if (OB_SUCC(ret)) {
      ret = tmp_ret;
    }
    LOG_TRACE("rpc clone sendbcmsg cb", K(responses_.at(i)), K(trace_id_));
  }
  resps.reset();
  destroy();
  return ret;
}

rpc::frame::ObReqTransport::AsyncCB *ObDtlRpcChannel::SendBCMsgCB::clone(
    const rpc::frame::SPAlloc &alloc) const
{
  LOG_DEBUG(" clone sendbcmsg cb", K(lbt()));
  SendBCMsgCB *cb = NULL;
  void *mem = alloc(sizeof(*this));
  if (NULL != mem) {
    cb = new(mem)SendBCMsgCB(trace_id_);
    int ret = cb->assign_resp(responses_);
    if (OB_SUCCESS != ret) {
      LOG_ERROR("failed to clone responses", K(ret));
    }
  }
  return cb;
}

void ObDtlRpcChannel::SendBCMsgCB::destroy()
{
  responses_.reset();
}
///////////////////////////////////////////////////////////////////////////////

ObDtlRpcChannel::ObDtlRpcChannel(
    const uint64_t tenant_id,
    const uint64_t id,
    const ObAddr &peer)
    : ObDtlBasicChannel(tenant_id, id, peer), recv_sqc_fin_res_(false)
{}

ObDtlRpcChannel::ObDtlRpcChannel(
    const uint64_t tenant_id,
    const uint64_t id,
    const ObAddr &peer,
    const int64_t hash_val)
    : ObDtlBasicChannel(tenant_id, id, peer, hash_val), recv_sqc_fin_res_(false)
{}

ObDtlRpcChannel::~ObDtlRpcChannel()
{
  destroy();
  LOG_TRACE("dtl use time", K(times_), K(write_buf_use_time_), K(send_use_time_), K(lbt()));
}

int ObDtlRpcChannel::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDtlBasicChannel::init())) {
    LOG_WARN("Initialize fifo allocator fail", K(ret));
  }
  return ret;
}

void ObDtlRpcChannel::destroy()
{
  recv_sqc_fin_res_ = false;
}

int ObDtlRpcChannel::feedup(ObDtlLinkedBuffer *&buffer)
{
  int ret = OB_SUCCESS;
  ObDtlLinkedBuffer *linked_buffer = nullptr;
  ObDtlMsgHeader header;
  const bool keep_buffer_pos = true;
  MTL_SWITCH(tenant_id_) {
    if (OB_FAIL(ObDtlLinkedBuffer::deserialize_msg_header(*buffer, header, keep_buffer_pos))) {
      LOG_WARN("failed to deserialize msg", K(ret));
    } else if (header.is_drain()) {
      // drain msg
      if (dfc_) {
        dfc_->set_drain(this);
        LOG_TRACE("a RPC channel has been drained", K(this), KP(this->id_), KP(this->peer_id_));
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("control channel can't drain msg", K(ret));
      }
    } else if (is_drain()) {
      // do nothing
    } else if (OB_ISNULL(linked_buffer = alloc_buf(buffer->size()))){
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate buffer", K(ret));
    } else {
      LOG_TRACE("DTL feedup a new msg to msg loop", K(buffer->size()), KP(id_), K(peer_));
      ObDtlLinkedBuffer::assign(*buffer, linked_buffer);
      if (1 == linked_buffer->seq_no() && linked_buffer->is_data_msg()
          && 0 != get_recv_buffer_cnt()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("first buffer is not first", K(ret), K(get_id()), K(get_peer_id()),
                K(get_recv_buffer_cnt()), K(linked_buffer->seq_no()));
        free_buf(linked_buffer);
        linked_buffer = nullptr;
      } else if (OB_FAIL(block_on_increase_size(linked_buffer->size()))) {
        free_buf(linked_buffer);
        linked_buffer = nullptr;
        LOG_WARN("failed to increase buffer size for dfc", K(ret), K(header));
      } else if (OB_FAIL(recv_list_.push(linked_buffer))) {
        LOG_WARN("push buffer into channel recv list fail", K(ret));
        free_buf(linked_buffer);
        linked_buffer = nullptr;
      } else if (FALSE_IT(inc_recv_buffer_cnt())) {
      } else {
        if (static_cast<uint16_t>(ObDtlMsgType::FINISH_SQC_RESULT) == header.type_) {
          recv_sqc_fin_res_ = true;
        }
        if (buffer->is_data_msg()) {
          metric_.mark_first_in();
          if (buffer->is_eof()) {
            metric_.mark_eof();
          }
          metric_.set_last_in_ts(::oceanbase::common::ObTimeUtility::current_time());
        }
        IGNORE_RETURN recv_sem_.signal();
        if (msg_watcher_ != nullptr) {
          msg_watcher_->notify(*this);
        }
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: failed to switch tenant gurad", K(ret), K(tenant_id_));
  }
  return ret;
}

int ObDtlRpcChannel::send_message(ObDtlLinkedBuffer *&buf)
{
  int ret = OB_SUCCESS;
  ObCurTraceId::TraceId *cur_trace_id = NULL;
  bool is_first = false;
  bool is_eof = false;
  bool bcast_mode = OB_NOT_NULL(bc_service_);

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_ISNULL(cur_trace_id = ObCurTraceId::get_trace_id())) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    is_first = buf->is_data_msg() && 1 == buf->seq_no();
    is_eof = buf->is_eof();

    if (OB_FAIL(wait_response())) {
      LOG_WARN("failed to wait for response", K(ret));
    }
    if (OB_SUCC(ret) && OB_FAIL(wait_unblocking_if_blocked())) {
      LOG_WARN("failed to block data flow", K(ret));
    }
  }
  LOG_TRACE("send message:", K(buf->tenant_id()), K(buf->size()), KP(get_id()), K_(peer), K(ret),
    K(get_send_buffer_cnt()), K(belong_to_receive_data()), K(belong_to_transmit_data()),
    K(bcast_mode), K(get_msg_seq_no()));

  if (OB_FAIL(ret)) {
  } else if (bcast_mode) {
    if (OB_FAIL(bc_service_->send_message(buf, is_drain()))) {
      LOG_WARN("failed to send message by bc service", K(ret));
    }
  } else if (!is_drain() || buf->is_eof()) {
    // The peer may not setup when the first message arrive,
    // we wait first message return and retry until peer setup.
    int64_t timeout_us = buf->timeout_ts() - ObTimeUtility::current_time();
    SendMsgCB cb(msg_response_, *cur_trace_id, buf->timeout_ts());
    if (timeout_us <= 0) {
      ret = OB_TIMEOUT;
      LOG_WARN("send dtl message timeout", K(ret), K(peer_),
          K(buf->timeout_ts()));
    } else if (OB_FAIL(msg_response_.start())) {
      LOG_WARN("start message process fail", K(ret));
    } else if (OB_FAIL(DTL.get_rpc_proxy().to(peer_).timeout(timeout_us)
        .compressed(compressor_type_)
        .ap_send_message(ObDtlSendArgs{peer_id_, *buf}, &cb))) {
      LOG_WARN("send message failed", K_(peer), K(ret));
      int tmp_ret = msg_response_.on_start_fail();
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("set start fail failed", K(tmp_ret));
      }
    }
    // 1) for data message, if dtl channel is not built, it's cached by first buffer manage,
    //    it's processed rightly, or it's drain
    //    so don't wait first response
    // 2) control message SQC and QC channel also must be linked
    // 3) bloom filter message rpc processor process, don't need channel
    // so channel is linked and don't retry
    if (OB_SUCC(ret)) {
      if (is_first) {
        metric_.mark_first_out();
      }
      metric_.set_last_out_ts(::oceanbase::common::ObTimeUtility::current_time());
      if (is_eof) {
        metric_.mark_eof();
        set_eof();
      }
    }
  }
  return ret;
}

}  // dtl
}  // sql
}  // oceanbase
