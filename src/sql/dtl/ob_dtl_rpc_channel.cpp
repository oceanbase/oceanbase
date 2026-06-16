/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_DTL
#include "ob_dtl_rpc_channel.h"
#include "sql/dtl/ob_dtl_channel_agent.h"
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
  int ret = OB_SUCCESS;
  ret = response_.on_finish(resp.is_block_, tmp_ret);
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
// SendBatchMsgCB implementation

void ObDtlRpcChannel::SendBatchMsgCB::on_invalid()
{
  LOG_WARN_RET(OB_ERROR,
               "SendBatchMsgCB invalid, check object serialization impl or oom",
               K_(trace_id));
  AsyncCB::on_invalid();
  const ObDtlBatchRpcDataResponse &result = result_;
  if (result.is_pure_control_msg_) {
    int ret_code = (OB_SUCCESS != rcode_.rcode_) ? rcode_.rcode_ : result.recode_;
    if (!responses_.empty() && OB_NOT_NULL(responses_.at(0))) {
      responses_.at(0)->on_finish(false, ret_code);
    }
  } else {
    ObIArray<ObDtlRpcDataResponse> &resps = result_.resps_;
    for (int64_t i = 0; i < responses_.count(); ++i) {
      if (OB_ISNULL(responses_.at(i))) {
        continue;
      }
      int ret_code = (OB_SUCCESS != rcode_.rcode_)
                      ? rcode_.rcode_
                      : (i < resps.count() ? resps.at(i).recode_ : OB_RPC_PACKET_INVALID);
      int ret = responses_.at(i)->on_finish(false, ret_code);
      if (OB_FAIL(ret)) {
        LOG_WARN("set finish failed", K(ret), K(resps.count()), K(responses_.count()), K(trace_id_));
      }
    }
  }
  responses_.reset();
  destroy();
}

void ObDtlRpcChannel::SendBatchMsgCB::on_timeout()
{
  LOG_WARN_RET(OB_TIMEOUT,
               "SendBatchMsgCB timeout, if negative timeout, check peer cpu "
               "load, network packet drop rate",
               K_(trace_id));
  AsyncCB::on_timeout();
  const ObDtlBatchRpcDataResponse &result = result_;
  if (result.is_pure_control_msg_) {
    int ret_code = (OB_SUCCESS != rcode_.rcode_) ? rcode_.rcode_ : result.recode_;
    if (!responses_.empty() && OB_NOT_NULL(responses_.at(0))) {
      responses_.at(0)->on_finish(false, ret_code);
    }
  } else {
    ObIArray<ObDtlRpcDataResponse> &resps = result_.resps_;
    for (int64_t i = 0; i < responses_.count(); ++i) {
      if (OB_ISNULL(responses_.at(i))) {
        continue;
      }
      int ret_code = (OB_SUCCESS != rcode_.rcode_)
                      ? rcode_.rcode_
                      : (i < resps.count() ? resps.at(i).recode_ : OB_TIMEOUT);
      int ret = responses_.at(i)->on_finish(false, ret_code);
      if (OB_FAIL(ret)) {
        LOG_WARN("set finish failed", K(ret), K(resps.count()), K(responses_.count()), K(trace_id_));
      }
    }
    resps.reset();
  }
  destroy();
}

int ObDtlRpcChannel::SendBatchMsgCB::process()
{
  int ret = OB_SUCCESS;
  const ObDtlBatchRpcDataResponse &result = result_;
  if (result.is_pure_control_msg_) {
    int ret_code = (OB_SUCCESS != rcode_.rcode_) ? rcode_.rcode_ : result.recode_;
    if (!responses_.empty() && OB_NOT_NULL(responses_.at(0))) {
      responses_.at(0)->on_finish(false, ret_code);
    }
  } else {
    ObIArray<ObDtlRpcDataResponse> &resps = result_.resps_;
    if (resps.count() != responses_.count()) {
      LOG_WARN("unexpected status: response count is not match",
        K(resps.count()), K(responses_.count()));
    }
    for (int64_t i = 0; i < responses_.count(); ++i) {
      if (OB_ISNULL(responses_.at(i))) {
        // nullptr entry: multi-buffer channel, response handled by the first entry
        continue;
      }
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
      LOG_TRACE("rpc clone sendbatchmsg cb", K(responses_.at(i)), K(trace_id_));
    }
    resps.reset();
  }
  destroy();
  return ret;
}


rpc::frame::ObReqTransport::AsyncCB *ObDtlRpcChannel::SendBatchMsgCB::clone(
  const rpc::frame::SPAlloc &alloc) const
{
  LOG_DEBUG(" clone sendbatchmsg cb", K(lbt()));
  SendBatchMsgCB *cb = NULL;
  void *mem = alloc(sizeof(*this));
  if (NULL != mem) {
    cb = new(mem)SendBatchMsgCB(trace_id_);
    int ret = cb->assign_resp(responses_);
    if (OB_SUCCESS != ret) {
      LOG_ERROR("failed to clone responses", K(ret));
    }
  }
  return cb;
}


void ObDtlRpcChannel::SendBatchMsgCB::destroy()
{
  responses_.reset();
}

///////////////////////////////////////////////////////////////////////////////

ObDtlRpcChannel::ObDtlRpcChannel(
    const uint64_t tenant_id,
    const uint64_t id,
    const ObAddr &peer,
    DtlChannelType type)
    : ObDtlBasicChannel(tenant_id, id, peer, type), recv_sqc_fin_res_(false)
{}

ObDtlRpcChannel::ObDtlRpcChannel(
    const uint64_t tenant_id,
    const uint64_t id,
    const ObAddr &peer,
    const int64_t hash_val,
    DtlChannelType type)
    : ObDtlBasicChannel(tenant_id, id, peer, hash_val, type), recv_sqc_fin_res_(false)
{}

ObDtlRpcChannel::~ObDtlRpcChannel()
{
  destroy();
  LOG_TRACE("dtl use time", K(times_), K(write_buf_use_time_), K(send_use_time_), K(lbt()));
}

int ObDtlRpcChannel::init(ObDtlFlowControl *dfc)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDtlBasicChannel::init(dfc))) {
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
    if (!buffer->is_data_msg() && OB_FAIL(ObDtlLinkedBuffer::deserialize_msg_header(*buffer, header, keep_buffer_pos))) {
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
    } else if (OB_FAIL(block_on_increase_size(buffer->size()))) {
      LOG_WARN("failed to increase buffer size for dfc", K(ret), K(header));
    } else if (OB_ISNULL(linked_buffer = alloc_buf(buffer->size()))){
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate buffer", K(ret));
    } else if (OB_FAIL(ObDtlLinkedBuffer::assign(*buffer, linked_buffer))) {
      LOG_WARN("failed to assign buffer", K(ret));
    } else {
      if (1 == linked_buffer->seq_no() && linked_buffer->is_data_msg()
          && 0 != get_recv_buffer_cnt()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("first buffer is not first", K(ret), K(get_id()), K(get_peer_id()),
                K(get_recv_buffer_cnt()), K(linked_buffer->seq_no()));
        free_buf(linked_buffer);
        linked_buffer = nullptr;
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
    if (!buf->is_batch_send()) {
      if (OB_FAIL(do_single_send(buf, *cur_trace_id))) {
        LOG_WARN("failed to do single send", K(ret));
      }
    } else {
      // Control message batch send path
      int64_t timeout_us = buf->timeout_ts() - ObTimeUtility::current_time();
      SendBatchMsgCB cb(*cur_trace_id);
      ObDtlBatchSendArgs args;
      if (buf->get_payload_channels().empty()) {
        args.is_pure_control_msg_ = true;
        if (OB_FAIL(cb.get_responses().push_back(&msg_response_))) {
          LOG_WARN("failed to push back message response", K(ret));
        }
      } else {
        if (OB_FAIL(cb.get_responses().reserve(buf->get_payload_channels().count()))) {
          LOG_WARN("failed to reserve message response", K(ret));
        } else if (OB_FAIL(args.arg_refs_.reserve(buf->get_payload_channels().count()))) {
          LOG_WARN("failed to reserve send args", K(ret));
        }
        for (int64_t i = 0; i < buf->get_payload_channels().count() && OB_SUCC(ret); ++i) {
          ObDtlRpcChannel *channel = static_cast<ObDtlRpcChannel *>(buf->get_payload_channels().at(i));
          ObDtlLinkedBuffer *payload_buffer = buf->get_payload_buffers().at(i);
          if (OB_FAIL(cb.get_responses().push_back(channel->get_msg_response()))) {
            LOG_WARN("failed to push back message response", K(ret));
          } else if (OB_FAIL(args.arg_refs_.push_back(
                         ObDtlSendArgRef{channel->get_peer_id(), payload_buffer}))) {
            LOG_WARN("failed to push back send arg", K(ret));
          }
        }
      }
      if (OB_FAIL(ret)) {
        LOG_WARN("failed to prepare batch message", K(ret));
      } else if (args.is_pure_control_msg_ && OB_FAIL(args.batch_buffer_.shallow_copy(*buf))) {
        LOG_WARN("failed to shallow copy batch buffer", K(ret));
      }
      // Start all responses before sending batch RPC.
      // This ensures all channels' msg_response_ have in_process_=true,
      // so their destructors will wait for the RPC callback to complete.
      for (int64_t i = 0; OB_SUCC(ret) && i < cb.get_responses().count(); ++i) {
        if (OB_NOT_NULL(cb.get_responses().at(i))) {
          if (OB_FAIL(cb.get_responses().at(i)->start())) {
            LOG_WARN("start message process fail", K(ret), K(i));
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(DTL.get_rpc_proxy().to(peer_).timeout(timeout_us)
                      .group_id(share::OBCG_DTL)
                      .compressed(compressor_type_)
                      .by(tenant_id_)
                      .ap_send_batch_message(args, &cb))) {
        LOG_WARN("send message failed", K_(peer), K(ret));
        // On RPC send failure, mark all started responses as failed
        for (int64_t i = 0; i < cb.get_responses().count(); ++i) {
          if (OB_NOT_NULL(cb.get_responses().at(i))) {
            int tmp_ret = cb.get_responses().at(i)->on_start_fail();
            if (OB_SUCCESS != tmp_ret) {
              LOG_WARN("set start fail failed", K(tmp_ret));
            }
          }
        }
      }
    }
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

// Single channel send (original path, no batching)
int ObDtlRpcChannel::do_single_send(
    ObDtlLinkedBuffer *&buf,
    const ObCurTraceId::TraceId &trace_id)
{
  int ret = OB_SUCCESS;
  int64_t timeout_us = buf->timeout_ts() - ObTimeUtility::current_time();
  SendMsgCB cb(msg_response_, trace_id, buf->timeout_ts());
  if (timeout_us <= 0) {
    ret = OB_TIMEOUT;
    LOG_WARN("send dtl message timeout", K(ret), K(peer_), K(buf->timeout_ts()));
  } else if (OB_FAIL(msg_response_.start())) {
    LOG_WARN("start message process fail", K(ret));
  } else if (OB_FAIL(DTL.get_rpc_proxy().to(peer_).timeout(timeout_us)
        .group_id(share::OBCG_DTL)
        .compressed(compressor_type_)
        .by(tenant_id_)
        .ap_send_message(ObDtlSendArgs{peer_id_, *buf}, &cb))) {
    LOG_WARN("send message failed", K_(peer), K(ret));
    int tmp_ret = msg_response_.on_start_fail();
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("set start fail failed", K(tmp_ret));
    }
  }
  return ret;
}

// Seal the current write_buffer_ and push it to send_list_.
// Used by batch send to collect partially-filled buffers from sibling channels.
int ObDtlRpcChannel::seal_write_buffer()
{
  int ret = OB_SUCCESS;
  if (write_buffer_ != nullptr) {
    // The fast path in PX transmit (keep_order_send_batch) writes rows directly
    // into the block buffer via fill_batch_ptrs + to_rows + fast_update_head,
    // without updating write_buffer_->pos(). We must sync pos from the writer's
    // actual usage before checking it.
    if (nullptr != msg_writer_) {
      write_buffer_->pos() = msg_writer_->used();
    }
    if (write_buffer_->pos() != 0) {
      if (CHUNK_DATUM_WRITER != msg_writer_->type()
          && OB_FAIL(msg_writer_->serialize())) {
        LOG_WARN("serialize failed in seal_write_buffer", K(ret));
      } else if (OB_FAIL(push_back_send_list())) {
        LOG_WARN("failed to push back send list in seal_write_buffer", K(ret));
      } else {
        // push_back_send_list() calls msg_writer_->reset() which clears meta_ to nullptr.
        // We must restore meta_ so that need_new_buffer() can calculate row size
        // before switch_buffer() allocates a new buffer and re-inits the writer.
        if (VECTOR_ROW_WRITER == msg_writer_->type()) {
          (static_cast<ObDtlVectorRowMsgWriter *>(msg_writer_))->set_row_meta(meta_);
        }
      }
      // write_buffer_ is set to nullptr by push_back_send_list()
      // Next write_msg() will trigger switch_buffer() to allocate a new one
    }
  }
  return ret;
}

// Called when defer_flush_ is true and a buffer was pushed to send_list_.
// Triggers batch flush to collect and send all pending buffers in the server group.
// Best-effort aggregation: collects whatever buffers are available and sends them.
int ObDtlRpcChannel::on_buffer_pending()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(server_group_)) {
    // Always trigger flush - batch_flush_server_group will collect all available
    // buffers and send them (single buffer uses regular path, multiple uses batch)
    if (OB_FAIL(batch_flush_server_group())) {
      LOG_WARN("batch flush server group failed", K(ret));
    }
  } else {
    // No server group, fall back to immediate flush
    if (OB_FAIL(flush(false))) {
      LOG_WARN("flush failed", K(ret));
    }
  }
  return ret;
}

int ObDtlRpcChannel::batch_flush_server_group()
{
  int ret = OB_SUCCESS;
  ObDtlServerChannelGroup *group = server_group_;
  ObCurTraceId::TraceId *cur_trace_id = ObCurTraceId::get_trace_id();
  if (OB_ISNULL(group)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("server group is null", K(ret));
  } else if (OB_ISNULL(cur_trace_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("trace id is null", K(ret));
  } else {
    ObDtlBatchSendArgs batch_args;
    ObSEArray<SendMsgResponse *, 8> batch_resps;
    ObSEArray<ObDtlRpcChannel *, 8> batch_channels;
    ObSEArray<ObDtlLinkedBuffer *, 8> batch_buffers;
    int64_t total_batch_size = 0;
    int64_t timeout_ts = 0;

    // Collect pending buffers from all channels in the server group and send
    // them in a single batch RPC. All channels share the worker thread so no
    // locking is needed; sibling buffer sealing already happened in the transmit
    // layer (try_seal_group_buffers) — here we only collect from send_list_.
    //
    // Two-phase wait policy — avoids cross-DFC dead-wait while still piggybacking
    // siblings onto the same RPC:
    //
    //   Phase 1 (self): unbounded wait_response + wait_unblocking_if_blocked,
    //     matching send_message. self triggered the flush, its buffer must go out.
    //
    //   Phase 2 (siblings): bounded per-flush wait budget. A sibling may belong
    //     to a different receive-side DFC whose unblock depends on self's
    //     progress, so unconditional waiting can deadlock. Each blocked sibling
    //     gets a per-channel cap (PRIORITY_WAIT_US for the first PRIORITY_WAIT_CNT
    //     entries, NORMAL_WAIT_US after); total wait is capped by WAIT_BUDGET_US.
    //
    //   Budget accounting:
    //     - On timeout, charge the FULL cap so a slow sibling exhausts the budget
    //       quickly and later siblings don't see a stale remainder; then skip —
    //       the sibling's own next flush handles it.
    //     - On early unblock, charge actual elapsed.
    //     - Only siblings we actually attempt to wait on count toward the tier
    //       quota; budget-exhausted skips do not.

    // Phase 1: self
    if (!IS_LOCAL_CHANNEL(this) && !this->send_list_.is_empty()) {
      if (OB_FAIL(this->wait_response())) {
        LOG_WARN("wait response failed during batch collect", K(ret));
      } else if (OB_FAIL(this->wait_unblocking_if_blocked())) {
        LOG_WARN("wait unblocking failed during batch collect", K(ret));
      }
    }

    // Phase 2: siblings
    int64_t wait_used_us = 0;
    int64_t blocked_ch_cnt = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < group->basic_channels_.count(); ++i) {
      ObDtlBasicChannel *basic_ch = group->basic_channels_.at(i);
      // Skip local channels - they don't use RPC and don't have msg_response_ for batch send
      if (OB_ISNULL(basic_ch) || IS_LOCAL_CHANNEL(basic_ch)) {
        continue;
      }
      ObDtlRpcChannel *ch = static_cast<ObDtlRpcChannel *>(basic_ch);
      if (ch->send_list_.is_empty()) {
        continue;
      }
      if (ch != this) {
        if (OB_FAIL(ch->wait_response())) {
          LOG_WARN("wait response failed during batch collect", K(ch->get_id()), K(ret));
          break;
        }

        const bool need_wait_unblocking = ch->msg_response_.is_block() || ch->is_blocked();
        if (need_wait_unblocking) {
          int64_t remain_budget_us = BATCH_FLUSH_WAIT_BUDGET_US - wait_used_us;
          if (remain_budget_us <= 0) {
            continue;
          }
          ++blocked_ch_cnt;
          int64_t wait_cap_us = blocked_ch_cnt <= BATCH_FLUSH_PRIORITY_WAIT_CNT
              ? BATCH_FLUSH_PRIORITY_WAIT_US
              : BATCH_FLUSH_NORMAL_WAIT_US;
          if (wait_cap_us > remain_budget_us) {
            wait_cap_us = remain_budget_us;
          }
          const int64_t wait_start_us = ObTimeUtility::current_time();
          const int wait_ret = ch->try_wait_unblocking_if_blocked(wait_cap_us);
          // Always charge actual elapsed time to the budget — works for both
          // early unblock and EAGAIN (slow sibling) paths without dual accounting.
          wait_used_us += ObTimeUtility::current_time() - wait_start_us;
          if (OB_SUCCESS != wait_ret) {
            if (OB_DTL_WAIT_EAGAIN == wait_ret) {
              continue;  // sibling still blocked, skip its buffer this round
            } else {
              ret = wait_ret;
              LOG_WARN("failed to wait unblocking if blocked", K(ch->get_id()), K(ret));
              break;
            }
          }
        }
      }

      // Pop ALL buffers from this channel's send_list_ (typically 0 or 1,
      // but can be more if multiple seals happened before flush).
      // Register msg_response_ once per channel for the callback.
      bool resp_pushed = false;
      ObLink *link = nullptr;
      int pop_ret = OB_SUCCESS;
      while (OB_SUCC(ret) && OB_SUCCESS == (pop_ret = ch->send_list_.pop(link)) && OB_NOT_NULL(link)) {
        ObDtlLinkedBuffer *buf = static_cast<ObDtlLinkedBuffer *>(link);
        if (OB_FAIL(batch_args.arg_refs_.push_back(ObDtlSendArgRef{ch->peer_id_, buf}))) {
          LOG_WARN("failed to push to batch args", K(ret));
        } else if (!resp_pushed && OB_FAIL(batch_resps.push_back(&ch->msg_response_))) {
          LOG_WARN("failed to push response", K(ret));
        } else if (resp_pushed && OB_FAIL(batch_resps.push_back(nullptr))) {
          LOG_WARN("failed to push null response", K(ret));
        } else if (OB_FAIL(batch_channels.push_back(ch))) {
          LOG_WARN("failed to push channel", K(ret));
        } else if (OB_FAIL(batch_buffers.push_back(buf))) {
          LOG_WARN("failed to push buffer", K(ret));
        } else {
          resp_pushed = true;
          total_batch_size += buf->size();
          if (timeout_ts == 0 || buf->timeout_ts() < timeout_ts) {
            timeout_ts = buf->timeout_ts();
          }
        }
        if (OB_FAIL(ret)) {
          ch->free_buf(buf);
        }
      }
      // Stop if batch is large enough
      if (total_batch_size >= MAX_BATCH_SIZE) {
        break;
      }
    }
    LOG_TRACE("print aggr buffer size", K(total_batch_size), K(batch_channels.count()), K(group->basic_channels_.count()));
    if (OB_FAIL(ret) || batch_args.arg_refs_.count() == 0) {
      // Error or nothing collected - free any collected buffers
      for (int64_t i = 0; i < batch_buffers.count(); ++i) {
        if (OB_NOT_NULL(batch_buffers.at(i)) && OB_NOT_NULL(batch_channels.at(i))) {
          batch_channels.at(i)->free_buf(batch_buffers.at(i));
        }
      }
    } else if (batch_args.arg_refs_.count() == 1) {
      // Single buffer: use regular send path
      ObDtlRpcChannel *ch = batch_channels.at(0);
      const common::ObAddr &dest = group->server_addr_;
      int64_t timeout_us = timeout_ts - ObTimeUtility::current_time();
      SendMsgCB cb(ch->msg_response_, *cur_trace_id, timeout_ts);
      if (timeout_us <= 0) {
        ret = OB_TIMEOUT;
        LOG_WARN("batch flush timeout", K(ret));
      } else if (OB_FAIL(ch->msg_response_.start())) {
        LOG_WARN("start message process fail", K(ret));
      } else if (OB_ISNULL(batch_buffers.at(0))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("batch buffers at 0 is null", K(ret));
      } else if (OB_FAIL(DTL.get_rpc_proxy()
                          .to(dest).timeout(timeout_us)
                          .group_id(share::OBCG_DTL)
                          .compressed(ch->compressor_type_)
                          .by(ch->tenant_id_)
                          .ap_send_message(ObDtlSendArgs{batch_args.arg_refs_.at(0).chid_,
                                                         *batch_buffers.at(0)}, &cb))) {
        LOG_WARN("send message failed", K(ret));
        int tmp_ret = ch->msg_response_.on_start_fail();
        if (OB_SUCCESS != tmp_ret) {
          LOG_WARN("set start fail failed", K(tmp_ret));
        }
      }
      // Free buffer and increment send count
      ch->free_buf(batch_buffers.at(0));
      batch_buffers.at(0) = nullptr;
      batch_args.arg_refs_.at(0).buf_ = nullptr;
      ch->inc_send_buffer_cnt();
      group->total_buffer_cnt_++;
      group->batch_send_cnt_++;
      LOG_TRACE("batch flush: single send", K(ret), K(total_batch_size));
    } else {
      // Multiple buffers: batch send
      const common::ObAddr &dest = group->server_addr_;
      int64_t timeout_us = timeout_ts - ObTimeUtility::current_time();
      SendBatchMsgCB cb(*cur_trace_id);
      if (timeout_us <= 0) {
        ret = OB_TIMEOUT;
        LOG_WARN("batch send timeout", K(ret));
      } else if (OB_FAIL(cb.assign_resp(batch_resps))) {
        LOG_WARN("failed to assign responses to callback", K(ret));
      }
      // Start all responses (skip nullptr entries from multi-buffer channels)
      for (int64_t i = 0; OB_SUCC(ret) && i < batch_resps.count(); ++i) {
        if (OB_NOT_NULL(batch_resps.at(i))) {
          if (OB_FAIL(batch_resps.at(i)->start())) {
            LOG_WARN("start message process fail", K(ret), K(i));
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(DTL.get_rpc_proxy()
                          .to(dest)
                          .group_id(share::OBCG_DTL)
                          .by(group->tenant_id_)
                          .timeout(timeout_us)
                          .ap_send_batch_message(batch_args, &cb))) {
        LOG_WARN("batch send message failed", K(dest), K(ret));
      }
      if (OB_FAIL(ret)) {
        for (int64_t i = 0; i < batch_resps.count(); ++i) {
          if (OB_NOT_NULL(batch_resps.at(i))) {
            int tmp_ret = batch_resps.at(i)->on_start_fail();
            if (OB_SUCCESS != tmp_ret) {
              LOG_WARN("set start fail failed", K(tmp_ret));
            }
          }
        }
      }
      // Free all buffers and increment send counts
      for (int64_t i = 0; i < batch_buffers.count(); ++i) {
        if (OB_NOT_NULL(batch_buffers.at(i)) && OB_NOT_NULL(batch_channels.at(i))) {
          batch_channels.at(i)->free_buf(batch_buffers.at(i));
          batch_buffers.at(i) = nullptr;
          batch_channels.at(i)->inc_send_buffer_cnt();
        }
      }
      if (OB_SUCC(ret)) {
        group->total_buffer_cnt_ += batch_args.arg_refs_.count();
        group->batch_send_cnt_++;
      }
      LOG_TRACE("batch flush: BATCH SEND", K(ret), K(batch_args.arg_refs_.count()),
        K(total_batch_size));
    }
  }
  return ret;
}

int ObDtlRpcChannel::pop_write_buffer(ObDtlLinkedBuffer *&buffer)
{
  int ret = OB_SUCCESS;
  ObLink *link = nullptr;
  // 1. flush all buffers in send list to making send_list_ empty
  // 2. push the current write_buffer_ to send_list_
  // 3 pop the last buffer from send_list_
  do {
    // reset return code every turn.
    ret = OB_SUCCESS;
    ObLink *link = nullptr;
    if (OB_SUCC(send_list_.pop(link))) {
      ObDtlLinkedBuffer *buffer = static_cast<ObDtlLinkedBuffer *>(link);
      if (OB_FAIL(send_message(buffer))) {
        LOG_WARN("send message failed", K_(peer), K(ret), K(buffer));
        if (nullptr != buffer) {
          free_buf(buffer);
        }
      } else if (nullptr != buffer) {
        free_buf(buffer);
      }
      if (OB_SUCC(ret)) {
        inc_send_buffer_cnt();
      }
    } else if (OB_EAGAIN == ret) {
      ret = OB_SUCCESS;
      break;
    } else {
      LOG_WARN("failed to pop send list", K(ret));
    }
  } while (OB_SUCC(ret));

  if (OB_FAIL(ret)) {
  } else if (write_buffer_ != nullptr && write_buffer_->pos() != 0) {
    if (OB_FAIL(push_back_send_list())) {
      LOG_WARN("failed to push back send list", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_SUCC(send_list_.pop(link))) {
    buffer = static_cast<ObDtlLinkedBuffer *>(link);
  } else if (OB_EAGAIN == ret) {
    ret = OB_SUCCESS;
  } else {
    LOG_WARN("failed to pop send list", K(ret));
  }
  return ret;
}


}  // dtl
}  // sql
}  // oceanbase
