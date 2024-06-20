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
#include "ob_dtl_channel_agent.h"
#include "sql/dtl/ob_dtl_channel.h"
#include "sql/dtl/ob_dtl_basic_channel.h"
#include "sql/dtl/ob_dtl_rpc_channel.h"
#include "sql/engine/px/ob_px_row_store.h"
#include "common/row/ob_row.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::sql::dtl;

int ObDtlBufEncoder::switch_writer(const ObDtlMsg &msg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == msg_writer_)) {
    if (msg.is_data_msg()) {
      const ObPxNewRow &px_row = static_cast<const ObPxNewRow&>(msg);
      if (DtlWriterType::CHUNK_ROW_WRITER == msg_writer_map[px_row.get_data_type()]) {
        msg_writer_ = &row_msg_writer_;
      } else if (DtlWriterType::CHUNK_DATUM_WRITER == msg_writer_map[px_row.get_data_type()]) {
        msg_writer_ = &datum_msg_writer_;
      } else if (DtlWriterType::VECTOR_FIXED_WRITER == msg_writer_map[px_row.get_data_type()]) {
        msg_writer_ = &vector_fixed_msg_writer_;
      } else if (DtlWriterType::VECTOR_ROW_WRITER == msg_writer_map[px_row.get_data_type()]) {
        msg_writer_ = &vector_row_msg_writer_;
      } else if (DtlWriterType::VECTOR_WRITER == msg_writer_map[px_row.get_data_type()]) {
        //TODO : support local channel shuffle in vector mode
        msg_writer_ = &vector_row_msg_writer_;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unkown msg writer", K(msg.get_type()), K(msg_writer_->type()));
      }
      LOG_TRACE("msg writer", K(px_row.get_data_type()), K(msg_writer_->type()));
    } else {
      if (DtlWriterType::CONTROL_WRITER == msg_writer_map[msg.get_type()]) {
        msg_writer_ = &ctl_msg_writer_;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unkown msg writer", K(msg.get_type()), K(msg_writer_->type()));
      }
    }
  } else {
// #ifndef NDEBUG
    // if (msg.is_data_msg() && msg_writer_->type() != DtlWriterType::VECTOR_ROW_WRITER) {
    //   const ObPxNewRow &px_row = static_cast<const ObPxNewRow&>(msg);
    //   if (msg_writer_map[px_row.get_data_type()] != msg_writer_->type()) {
    //     ret = OB_ERR_UNEXPECTED;
    //   }
    // } else {
    //   if (msg_writer_map[msg.get_type()] != msg_writer_->type()) {
    //     ret = OB_ERR_UNEXPECTED;
    //   }
    // }
// #endif
  }
  return ret;
}

int ObDtlBufEncoder::need_new_buffer(
  const ObDtlMsg &msg, ObEvalCtx *eval_ctx, int64_t &need_size, bool &need_new)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(msg_writer_->need_new_buffer(msg, eval_ctx, need_size, need_new))) {
    LOG_WARN("failed to calc need new buffer", K(ret));
  }
  return ret;
}

int ObDtlBufEncoder::write_data_msg(const ObDtlMsg &msg, ObEvalCtx *eval_ctx, bool is_eof)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(msg_writer_->write(msg, eval_ctx, is_eof))) {
    if (OB_BUF_NOT_ENOUGH != ret) {
      LOG_WARN("failed to add row", K(ret));
    }
  } else {
    LOG_DEBUG("write row", K(ret),
      K(msg_writer_->rows()), K(msg_writer_->used()), KP(buffer_));
    buffer_->pos() = (msg_writer_->rows() > 0 || is_eof) ? msg_writer_->used() : 0;
    buffer_->is_eof() = is_eof;
  }
  return ret;
}

int ObDtlBcastService::send_message(ObDtlLinkedBuffer *&bcast_buf, bool drain)
{
  int ret = OB_SUCCESS;
  /**
   * 一个broadcast组被接收端在同一台机器的发送channel共享。
   * 假设三个发送channel共享此bcast service。一轮send消息，三个channel一定是在
   * 发送同样的一个消息，如果是不同的消息则要报错。
   * 前面两个channel send消息的时候，一定是计数的，并不真正的发送数据。
   * 第三个channel发送数据的时候才会真正的发送数据。
   * 发送的动作以后三个channel都会收到异步回包。
   * 异步回报的对channel造成的状态更改会在channel下一次send的动作的时候得到生效。
   */
  ObCurTraceId::TraceId *cur_trace_id = NULL;
  if (OB_ISNULL(cur_trace_id = ObCurTraceId::get_trace_id()) || active_chs_count_ < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid trace id / invalid active count", K(ret), K(active_chs_count_));
  } else if (0 == active_chs_count_) {
    // all channel has been drain, do nothing.
  } else if (nullptr == bcast_buf_ && 0 == send_count_) {
    // a new buffer come into this broadcast group.
    bcast_buf_ = bcast_buf;
    send_count_ = bcast_ch_count_ - 1;
    // 这里每次发送msg都会--，所以每次都需要重置active_chs_count_
    active_chs_count_ = bcast_ch_count_;
    bcast_buf = nullptr;
    if (drain) {
      // this channel has been drained.
      active_chs_count_--;
    }
  } else if (bcast_buf_ == bcast_buf) {
    send_count_--;
    bcast_buf = nullptr;
    if (drain) {
      // this channel has been drained.
      active_chs_count_--;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("this channel write a msg to other bcast service", K(bcast_buf), K(bcast_buf_), K(send_count_));
  }
  if (OB_SUCC(ret)) {
    if (0 == send_count_ && active_chs_count_ != 0) {
      ObDtlBCSendArgs args;
      ObDtlLinkedBuffer empty_dtl_buf;
      ObDtlRpcChannel::SendBCMsgCB cb(*cur_trace_id);
      args.bc_buffer_.shallow_copy(*bcast_buf_);
      int64_t timeout_us = args.bc_buffer_.timeout_ts() - ObTimeUtility::current_time();
      if (timeout_us <= 0) {
        ret = OB_TIMEOUT;
        LOG_WARN("send message timeout", K(ret), K(args.bc_buffer_.timeout_ts()));
      } else if (OB_FAIL(cb.assign_resp(resps_))) {
        LOG_WARN("failed to assign resp", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < peer_ids_.count(); ++i) {
        if (OB_FAIL(args.args_.push_back(ObDtlSendArgs{peer_ids_.at(i), empty_dtl_buf}))) {
          LOG_WARN("failed to push back send arg", K(ret));
        }
      }
      // set msg response in process
      for (int64_t i = 0; i < resps_.count() && OB_SUCC(ret); ++i) {
        if (OB_FAIL(resps_.at(i)->start())) {
          LOG_WARN("start message process fail", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(DTL.get_rpc_proxy()
                       .to(server_addr_)
                       .timeout(timeout_us)
                       .ap_send_bc_message(args, &cb))) {
          LOG_WARN("failed to seed message", K(ret));
        } else {
          // all rpc channel in this service has send this msg. this buffer will be release in agent.
          bcast_buf_ = nullptr;
        }
      }
      // if start or rpc failed, we reset response.
      if (OB_FAIL(ret)) {
        for (int64_t i = 0; i < resps_.count(); ++i) {
          int tmp_ret = resps_.at(i)->on_start_fail();
          if (OB_SUCCESS != tmp_ret) {
            LOG_WARN("set start fail failed", K(tmp_ret));
          }
        }
      }
    } else if (0 == active_chs_count_) {
      bcast_buf_ = nullptr;
    }
  }
  LOG_TRACE("send message", K(ret), K(this), K(bcast_ch_count_), K(send_count_), K(bcast_buf),
    K(bcast_buf_), K(peer_ids_), K(send_count_), K(active_chs_count_));
  return ret;
}

int ObDtlChanAgent::init(dtl::ObDtlFlowControl &dfc,
                         ObPxTaskChSet &task_ch_set,
                         ObIArray<ObDtlChannel *> &channels,
                         int64_t tenant_id,
                         int64_t time_ts)
{
  int ret = OB_SUCCESS;
  dtl_buf_allocator_.set_tenant_id(tenant_id);
  dtl_buf_allocator_.set_timeout_ts(time_ts);
  dtl_buf_encoder_.set_tenant_id(tenant_id);
  sys_dtl_buf_size_ = GCONF.dtl_buffer_size;
  dfo_key_ = dfc.get_dfo_key();

  if (init_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("this channel agent has been initiated", K(ret));
  }

  for (int64_t i = 0; i < channels.count() && OB_SUCC(ret); ++i) {
    bool find_bc_service = false;

    ObDtlBasicChannel *data_ch = (ObDtlBasicChannel*)channels.at(i);
    int64_t sys_buffer_size = data_ch->get_send_buffer_size();

    ObDtlChannelInfo ch_info;
    if (OB_FAIL(task_ch_set.get_channel_info(i, ch_info))) {
      LOG_WARN("failed to get channel info", K(ret));
    }
    dtl_buf_allocator_.set_sys_buffer_size(sys_buffer_size);
    if (OB_FAIL(ret)) {
    } else if (ObDtlChannel::DtlChannelType::RPC_CHANNEL == data_ch->get_channel_type()) {
      if (OB_FAIL(rpc_channels_.push_back((ObDtlRpcChannel *)data_ch))) {
        LOG_WARN("failed to push back rpc channels", K(ret));
      }
      for (int64_t i = 0; i < bc_services_.count() && OB_SUCC(ret); ++i) {
        if (ch_info.peer_ == bc_services_.at(i)->server_addr_) {
          bc_services_.at(i)->bcast_ch_count_++;
          bc_services_.at(i)->active_chs_count_++;
          data_ch->set_bc_service(bc_services_.at(i));
          if (OB_FAIL(bc_services_.at(i)->ch_infos_.push_back(ch_info))) {
            LOG_WARN("failed to push channel info", K(ret));
          } else if (OB_FAIL(bc_services_.at(i)->peer_ids_.push_back(data_ch->get_peer_id()))) {
            LOG_WARN("failed to push peer id", K(ret));
          } else if (OB_FAIL(bc_services_.at(i)->resps_.push_back(data_ch->get_msg_response()))) {
            LOG_WARN("failed to push resp", K(ret));
          } else {
            data_ch->set_bc_service(bc_services_.at(i));
          }
          find_bc_service = true;
          break;
        }
      }
      if (!find_bc_service && OB_SUCC(ret)) {
        ObDtlBcastService *bc_service = nullptr;
        void *buf = allocator_.alloc(sizeof(ObDtlBcastService));
        if (nullptr == buf) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("no momery", K(ret));
        } else {
          bc_service = new(buf) ObDtlBcastService();
          bc_service->bcast_ch_count_++;
          bc_service->active_chs_count_++;
          bc_service->server_addr_ = data_ch->peer_;
          if (OB_FAIL(bc_service->ch_infos_.push_back(ch_info))) {
            LOG_WARN("failed to push channel info", K(ret));
          } else if (OB_FAIL(bc_service->peer_ids_.push_back(data_ch->get_peer_id()))) {
            LOG_WARN("failed to push peer id", K(ret));
          } else if (OB_FAIL(bc_service->resps_.push_back(data_ch->get_msg_response()))) {
            LOG_WARN("failed to push resp", K(ret));
          } else if (OB_FAIL(bc_services_.push_back(bc_service))) {
            LOG_WARN("failed to push bc service", K(ret));
          } else {
            data_ch->set_bc_service(bc_service);
          }
        }
      }
    } else if (ObDtlChannel::DtlChannelType::LOCAL_CHANNEL == data_ch->get_channel_type()) {
      if (OB_FAIL(local_channels_.push_back((ObDtlLocalChannel *)data_ch))) {
        LOG_WARN("failed to push back server_ch", K(ret));
      }
      LOG_DEBUG("channel info by server", KP(data_ch->get_id()), K(data_ch->get_channel_type()));
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected channel type", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (!local_channels_.empty()) {
      bcast_channel_ = local_channels_.at(BROADCAST_CH_IDX);
    } else if (!rpc_channels_.empty()) {
      bcast_channel_ = rpc_channels_.at(BROADCAST_CH_IDX);
    }
  }

  LOG_TRACE("use shared broadcast msg optimizer", K(bc_services_), K(local_channels_.count()), K(rpc_channels_.count()), KP(bcast_channel_->get_id()));
  return ret;
}

int ObDtlChanAgent::inner_broadcast_row(
  const ObDtlMsg &msg, ObEvalCtx *eval_ctx, bool is_eof)
{
  int ret = OB_SUCCESS;
  int64_t need_size = 0;
  bool need_new = false;
  LOG_DEBUG("[DTL BROADCAST] broadcast", K(is_eof), K(msg.get_type()));
  if (OB_FAIL(dtl_buf_encoder_.switch_writer(msg))) {
    LOG_WARN("failed to switch msg writer", K(ret));
  } else if (OB_FAIL(dtl_buf_encoder_.need_new_buffer(msg, eval_ctx, need_size, need_new))) {
    LOG_WARN("failed to calc need new buffer", K(ret));
  } else if (need_new) {
    if (OB_FAIL(switch_buffer(need_size))) {
      LOG_WARN("failed to switch buffer", K(ret));
    } else {
      dtl_buf_encoder_.write_msg_type(current_buffer_);
      current_buffer_->set_data_msg(msg.is_data_msg());
      current_buffer_->is_eof() = is_eof;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(dtl_buf_encoder_.write_data_msg(msg, eval_ctx, is_eof))) {
      if (OB_BUF_NOT_ENOUGH != ret) {
        LOG_WARN("failed to write msg", K(ret));
        dtl_buf_allocator_.free_buf(*bcast_channel_, current_buffer_);
      }
    }
  }
  return ret;
}

int ObDtlChanAgent::broadcast_row(const ObDtlMsg &msg, ObEvalCtx *eval_ctx, bool is_eof)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(inner_broadcast_row(msg, eval_ctx, is_eof))) {
    if (OB_BUF_NOT_ENOUGH == ret) {
      if (OB_FAIL(inner_broadcast_row(msg, eval_ctx, is_eof))) {
        LOG_WARN("failed to broadcast row", K(ret));
      }
    } else {
      LOG_WARN("failed to broadcast row", K(ret));
    }
  }
  return ret;
}

int ObDtlChanAgent::switch_buffer(int64_t need_size)
{
  int ret = OB_SUCCESS;
  ObDtlBasicChannel *bcast_ch = bcast_channel_;
  ObDtlLinkedBuffer *last_buffer = dtl_buf_encoder_.get_buffer();
  current_buffer_ = dtl_buf_allocator_.alloc_buf(*bcast_ch, std::max(sys_dtl_buf_size_, need_size));
  LOG_DEBUG("[DTL BROADCAST] encoder need a new buffer", KP(bcast_ch->get_id()), K(need_size));
  if (nullptr == current_buffer_) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  }

  // send last buffer
  if (OB_SUCC(ret) && OB_NOT_NULL(last_buffer)) {
    if (0 != last_buffer->pos()) {
      if (OB_FAIL(dtl_buf_encoder_.serialize())) {
        LOG_WARN("failed to do serialize", K(ret));
      } else if (OB_FAIL(send_last_buffer(last_buffer))) {
        LOG_WARN("failed to send last buffer", K(ret));
      } else {
        dtl_buf_encoder_.reset_writer();
      }
    } else {
      dtl_buf_allocator_.free_buf(*bcast_ch, last_buffer);
    }
  }

  // set new buffer
  if (OB_SUCC(ret)) {
    current_buffer_->set_bcast();
    dtl_buf_encoder_.set_new_buffer(current_buffer_);
  } else if (nullptr != current_buffer_) {
    dtl_buf_allocator_.free_buf(*bcast_ch, current_buffer_);
  }

  return ret;
}

int ObDtlChanAgent::flush()
{
  int ret = OB_SUCCESS;
  ObDtlLinkedBuffer *last_buffer = dtl_buf_encoder_.get_buffer();
  if (nullptr == current_buffer_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("you should send a row before use this interface", K(ret));
  } else if (last_buffer != current_buffer_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("send buffer must be equal to last buffer", K(ret), K(last_buffer), K(current_buffer_));
  // } else if (OB_FAIL(dtl_buf_encoder_.serialize())) {
  //   LOG_WARN("failed to do serialize", K(ret));
  } else if (OB_FAIL(send_last_buffer(last_buffer))) {
    LOG_WARN("failed to send last buffer", K(ret));
  } else {
    dtl_buf_encoder_.reset_writer();
    current_buffer_ = nullptr;
  }
  return ret;

}

int ObDtlChanAgent::send_last_buffer(ObDtlLinkedBuffer *&last_buffer)
{
  int ret = OB_SUCCESS;
  ObDtlBasicChannel *ch = nullptr;
  ObDtlRpcChannel *rpc_ch = nullptr;
  last_buffer->set_dfo_key(dfo_key_);
  ObDtlBasicChannel *bcast_ch = bcast_channel_;
  const int64_t size = last_buffer->pos(); // yes, it is pos()
  const int64_t pos = last_buffer->pos();
  for (int64_t i = 0; i < local_channels_.count() && OB_SUCC(ret); ++i) {
    ch = local_channels_.at(i);
    if (!ch->is_drain() || last_buffer->is_eof()) {
      ObDtlLinkedBuffer *buf = dtl_buf_allocator_.alloc_buf(*ch, last_buffer->size());
      if (nullptr == buf) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory", K(ret));
      } else {
        last_buffer->size() = size;
        last_buffer->pos() = pos;
        ObDtlLinkedBuffer::assign(*last_buffer, buf);
        if (OB_FAIL(ch->send_buffer(buf))) {
          LOG_WARN("failed to send buffer", K(ret));
        }
        if (nullptr != buf) {
          dtl_buf_allocator_.free_buf(*ch, buf);
        }
      }
    }
  }

  for (int64_t i = 0; i < rpc_channels_.count() && OB_SUCC(ret); ++i) {
    rpc_ch = rpc_channels_.at(i);
    last_buffer->size() = size;
    last_buffer->pos() = pos;
    ObDtlLinkedBuffer *current_ptr = last_buffer;
    if (OB_FAIL(rpc_ch->send_buffer(last_buffer))) {
      rpc_ch->clean_broadcast_buffer();
      LOG_WARN("failed to send buffer", K(ret));
    }
    last_buffer = current_ptr;
  }

  if (nullptr != last_buffer) {
    if (last_buffer == current_buffer_) {
      current_buffer_ = nullptr;
    }
    dtl_buf_allocator_.free_buf(*bcast_ch, last_buffer);
  }
  return ret;
}

int ObDtlChanAgent::destroy()
{
  int ret = OB_SUCCESS;
  if (nullptr != bcast_channel_ && nullptr != current_buffer_) {
    dtl_buf_allocator_.free_buf(*bcast_channel_, current_buffer_);
  }
  for (int64_t i = 0; i < local_channels_.count(); ++i) {
    int temp_ret = local_channels_.at(i)->wait_response();
    if (OB_SUCCESS != temp_ret) {
      ret = temp_ret;
    }
  }
  for (int64_t i = 0; i < rpc_channels_.count(); ++i) {
    int temp_ret = rpc_channels_.at(i)->wait_response();
    if (OB_SUCCESS != temp_ret) {
      ret = temp_ret;
    }
  }
  for (int64_t i = 0; i < bc_services_.count(); ++i) {
    bc_services_.at(i)->~ObDtlBcastService();
  }
  return ret;
}
