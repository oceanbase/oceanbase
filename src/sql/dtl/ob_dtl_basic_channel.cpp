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
#include "ob_dtl_basic_channel.h"
#include "share/interrupt/ob_global_interrupt_call.h"
#include "lib/oblog/ob_log.h"
#include "lib/allocator/ob_mod_define.h"
#include "lib/lock/ob_thread_cond.h"
#include "lib/random/ob_random.h"
#include "common/row/ob_row.h"
#include "share/ob_cluster_version.h"
#include "sql/dtl/ob_dtl_rpc_proxy.h"
#include "sql/dtl/ob_dtl.h"
#include "sql/dtl/ob_dtl_flow_control.h"
#include "sql/engine/px/ob_px_row_store.h"
#include "sql/engine/px/ob_px_dtl_msg.h"
#include "sql/dtl/ob_dtl_interm_result_manager.h"
#include "sql/dtl/ob_dtl_channel_loop.h"
#include "sql/dtl/ob_dtl_channel_watcher.h"

using namespace oceanbase::common;

namespace oceanbase {
namespace sql {
namespace dtl {

SendMsgResponse::SendMsgResponse()
    : inited_(false), ret_(OB_SUCCESS), in_process_(false), finish_(true), is_block_(false), cond_(), ch_id_(-1)
{}

SendMsgResponse::~SendMsgResponse()
{
  int ret = OB_SUCCESS;
  if (in_process_) {
    if (OB_FAIL(wait())) {
      LOG_DEBUG("send dtl message failed", K(ret));
    }
  }
}

int SendMsgResponse::init()
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice");
  } else if (OB_FAIL(cond_.init(common::ObWaitEventIds::DEFAULT_COND_WAIT))) {
    LOG_WARN("thread condition init failed", K(ret));
  } else {
    inited_ = true;
  }
  return ret;
}

int SendMsgResponse::start()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ret_ = OB_SUCCESS;
    in_process_ = true;
    finish_ = false;
    is_block_ = false;
  }
  return ret;
}

int SendMsgResponse::on_start_fail()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    in_process_ = false;
    finish_ = true;
    is_block_ = false;
    LOG_TRACE("dtl response fail", K(is_block_), K(ret), KP(ch_id_));
  }
  return ret;
}

int SendMsgResponse::on_finish(const bool is_block, const int return_code)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), KP(ch_id_));
  } else {
    ObThreadCondGuard guard(cond_);
    ret_ = return_code;
    finish_ = true;
    is_block_ = is_block;
    LOG_TRACE("dtl response finish", KP(this), K(is_block_), K(ret), KP(ch_id_));
    cond_.broadcast();
  }
  return ret;
}

int SendMsgResponse::wait()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), KP(ch_id_));
  } else {
    ObThreadCondGuard guard(cond_);
    while (!finish_) {
      // need to wait for callback to finish, otherwise it might get sigsegv
      cond_.wait(1);
    }
    in_process_ = false;
  }
  return OB_SUCCESS == ret ? ret_ : ret;
}

ObDtlBasicChannel::ObDtlBasicChannel(const uint64_t tenant_id, const uint64_t id, const ObAddr& peer)
    : ObDtlChannel(id, peer),
      is_inited_(false),
      local_id_(id),
      peer_id_(id ^ 1),
      write_buffer_(nullptr),
      process_buffer_(nullptr),
      send_failed_buffer_(nullptr),
      alloc_new_buf_(false),
      send_buffer_cnt_(0),
      recv_buffer_cnt_(0),
      processed_buffer_cnt_(0),
      tenant_id_(tenant_id),
      is_data_msg_(false),
      hash_val_(0),
      dfc_idx_(OB_INVALID_ID),
      got_from_dtl_cache_(true),
      msg_writer_(nullptr),
      msg_reader_(&datum_row_iter_),
      channel_is_eof_(false),
      bc_service_(nullptr),
      times_(0),
      write_buf_use_time_(0),
      send_use_time_(0),
      msg_count_(0)
{
  ObRandom rand;
  hash_val_ = rand.get();
  // some backward compatibility handling
  use_crs_writer_ = !(GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_2200);
  msg_response_.set_id(id_);
}

ObDtlBasicChannel::~ObDtlBasicChannel()
{
  destroy();
  LOG_TRACE("dtl use time", KP(id_), K(times_), K(write_buf_use_time_), K(send_use_time_), K(msg_count_));
}

int ObDtlBasicChannel::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(msg_response_.init())) {
    LOG_WARN("int message response failed", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObDtlBasicChannel::wait_response()
{
  int ret = OB_SUCCESS;
  if (msg_response_.is_in_process()) {
    if (OB_FAIL(msg_response_.wait())) {
      LOG_WARN("send previous message fail", K(ret));
    }
  }
  return ret;
}

int ObDtlBasicChannel::clear_response_block()
{
  int ret = OB_SUCCESS;
  if (msg_response_.is_in_process()) {
    if (OB_FAIL(msg_response_.wait())) {
      LOG_WARN("send previous message fail", K(ret));
    }
  }
  msg_response_.reset_block();
  return ret;
}

void ObDtlBasicChannel::destroy()
{
  if (is_inited_) {
    if (nullptr != msg_writer_) {
      msg_writer_->reset();
      msg_writer_ = nullptr;
    }
    msg_reader_->reset();
    is_inited_ = false;
    if (send_failed_buffer_ != nullptr) {
      free_buf(send_failed_buffer_);
      send_failed_buffer_ = nullptr;
    }
    if (process_buffer_ != nullptr) {
      free_buf(process_buffer_);
      process_buffer_ = nullptr;
    }
    if (write_buffer_ != nullptr) {
      free_buf(write_buffer_);
      write_buffer_ = nullptr;
    }
    ObLink* link = NULL;
    while (OB_SUCCESS == send_list_.pop(link) && NULL != link) {
      auto p = static_cast<ObDtlLinkedBuffer*>(link);
      if (nullptr != p) {
        free_buf(p);
      }
    }
    ObSpLinkQueue* queues[] = {&recv_list_, &free_list_};
    for (int64_t i = 0; i < ARRAYSIZEOF(queues); i++) {
      while (OB_SUCCESS == queues[i]->pop(link) && NULL != link) {
        auto p = static_cast<ObDtlLinkedBuffer*>(link);
        if (nullptr != p) {
          free_buf(p);
        }
      }
    }
  }
  if (alloc_buffer_cnt_ != free_buffer_cnt_) {
    int tmp_ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("channel may exists buffer to free",
        KP(id_),
        K(peer_id_),
        K(tmp_ret),
        K(alloc_buffer_cnt_),
        K(free_buffer_cnt_));
  }
}

int ObDtlBasicChannel::send(const ObDtlMsg& msg, int64_t timeout_ts, ObEvalCtx* eval_ctx, bool is_eof)
{
  int ret = OB_SUCCESS;
  // serialize message and link it to send buffer list.
  //
  // | ObDtlLinkedBuffer structure | message serialize size | serialized message |
  //
  is_data_msg_ = belong_to_transmit_data() && msg.is_data_msg();
  is_eof_ = is_eof;
  if (is_data_msg_) {
    metric_.mark_first_in();
    if (is_eof_) {
      metric_.mark_last_in();
    }
  }
  if (OB_FAIL(write_msg(msg, timeout_ts, eval_ctx, is_eof))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("write buffer fail", K(ret));
    }
  }
  if (!send_list_.is_empty()) {
    // If writer buffer is large enough, we should switch buffer and
    // send it out.
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = flush(false))) {
      if (OB_SUCCESS == ret) {
        ret = tmp_ret;
      }
      LOG_WARN("flush fail. ignore. may retry flush again very soon", K(ret), K(tmp_ret));
    }
  }
  return ret;
}

int ObDtlBasicChannel::feedup(ObDtlLinkedBuffer*& buffer)
{
  UNUSED(buffer);
  return common::OB_NOT_IMPLEMENT;
}

int ObDtlBasicChannel::attach(ObDtlLinkedBuffer*& linked_buffer, bool is_first_buffer_cached)
{
  int ret = OB_SUCCESS;
  ObDtlMsgHeader header;
  const bool keep_pos = true;
  LOG_DEBUG("local attach attach buffer", KP(id_), K(linked_buffer), KP(linked_buffer));
  if (!linked_buffer->is_data_msg() &&
      OB_FAIL(ObDtlLinkedBuffer::deserialize_msg_header(*linked_buffer, header, keep_pos))) {
    LOG_WARN("failed to deserialize msg header", K(ret));
  } else if (header.is_drain()) {
    alloc_buffer_count();
    inc_recv_buffer_cnt();
    dfc_->set_drain(this);
    LOG_TRACE(
        "transmit receive drain cmd", KP(linked_buffer), K(this), KP(id_), KP(peer_id_), K(recv_list_.is_empty()));
    free_buf(linked_buffer);
    linked_buffer = nullptr;
  } else if (1 == linked_buffer->seq_no() && linked_buffer->is_data_msg() && 0 != get_recv_buffer_cnt()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("first buffer is not first", K(ret), K(get_id()), K(get_peer_id()));
  } else if (OB_FAIL(block_on_increase_size(linked_buffer->size()))) {
    LOG_WARN("failed to increase buffer size for dfc", K(ret));
  } else if (OB_FAIL(recv_list_.push(linked_buffer))) {
    LOG_WARN("push buffer into channel recv list fail", K(ret));
  } else {
    alloc_buffer_count();
    inc_recv_buffer_cnt();
    if (linked_buffer->is_data_msg()) {
      metric_.mark_first_in();
      if (linked_buffer->is_eof()) {
        metric_.mark_last_in();
      }
    }
    if (is_first_buffer_cached) {
      set_first_buffer();
    }
    linked_buffer = nullptr;
    IGNORE_RETURN recv_sem_.post();
    if (msg_watcher_ != nullptr) {
      msg_watcher_->notify(*this);
    }
  }
  return ret;
}

int ObDtlBasicChannel::block_on_increase_size(int64_t size)
{
  int ret = OB_SUCCESS;
  if (belong_to_receive_data()) {
    ObDfcServer& dfc_server = DTL.get_dfc_server();
    int64_t ch_idx = OB_INVALID_ID;
    if (OB_FAIL(dfc_->find(this, ch_idx))) {
      LOG_WARN("failed to find channel", K(ret));
    } else if (OB_FAIL(dfc_server.block_on_increase_size(dfc_, ch_idx, size))) {
      LOG_WARN("failed to block channel", K(ret), KP(id_), K(peer_), K(dfc_), K(ch_idx));
    }
  }
  return ret;
}

bool ObDtlBasicChannel::has_less_buffer_cnt()
{
  return ATOMIC_LOAD(&recv_sem_.futex_.uval()) <= MAX_BUFFER_CNT;
}

int ObDtlBasicChannel::unblock_on_decrease_size(int64_t size)
{
  int ret = OB_SUCCESS;
  if (belong_to_receive_data()) {
    ObDfcServer& dfc_server = DTL.get_dfc_server();
    int64_t ch_idx = OB_INVALID_ID;
    if (OB_FAIL(dfc_->find(this, ch_idx))) {
      LOG_WARN("failed to find channel", K(ret));
    } else if (OB_FAIL(dfc_server.unblock_on_decrease_size(dfc_, ch_idx, size))) {
      LOG_WARN("failed to block channel", K(ret), KP(id_), K(peer_), K(dfc_), K(ch_idx));
    }
  }
  return ret;
}

// clean process_buffer and recv_list when failed
int ObDtlBasicChannel::clean_recv_list()
{
  int ret = OB_SUCCESS;
  LOG_TRACE("clean recv list",
      K(belong_to_receive_data()),
      KP(id_),
      K_(peer),
      K(ret),
      K(get_processed_buffer_cnt()),
      K(get_recv_buffer_cnt()));
  if (belong_to_receive_data()) {
    LOG_TRACE(
        "clean process buffer", KP(id_), K_(peer), K(ret), K(get_processed_buffer_cnt()), K(get_recv_buffer_cnt()));
    if (nullptr != process_buffer_) {
      auto& buffer = process_buffer_;
      LOG_TRACE("free process buffer for dfc",
          K(buffer->size()),
          KP(id_),
          K_(peer),
          K(ret),
          K(get_processed_buffer_cnt()),
          K(get_recv_buffer_cnt()));
      if (OB_FAIL(unblock_on_decrease_size(buffer->size()))) {
        LOG_WARN("failed to decrease buffer size for dfc",
            KP(id_),
            K_(peer),
            K(ret),
            K(get_processed_buffer_cnt()),
            K(get_recv_buffer_cnt()));
      }
      free_buf(buffer);
      process_buffer_ = nullptr;
    }
    ObLink* link = nullptr;
    while (OB_SUCC(recv_list_.pop(link))) {
      process_buffer_ = static_cast<ObDtlLinkedBuffer*>(link);
      auto& buffer = process_buffer_;
      LOG_TRACE("free recv list buffer for dfc",
          K(buffer->size()),
          KP(id_),
          K_(peer),
          K(ret),
          K(get_processed_buffer_cnt()),
          K(get_recv_buffer_cnt()),
          K(lbt()));
      if (OB_FAIL(unblock_on_decrease_size(buffer->size()))) {
        LOG_WARN("failed to decrease buffer size for dfc",
            KP(id_),
            K_(peer),
            K(ret),
            K(get_processed_buffer_cnt()),
            K(get_recv_buffer_cnt()));
      }
      free_buf(buffer);
      process_buffer_ = nullptr;
    }
  }
  ret = OB_SUCCESS;
  return ret;
}

int ObDtlBasicChannel::get_processed_buffer(int64_t timeout)
{
  int ret = OB_SUCCESS;
  bool has_first_buffer = false;
  if (OB_LIKELY(nullptr == process_buffer_)) {
    // process first msg, it's maybe cached by dfc server
    if (got_from_dtl_cache_ && nullptr != msg_watcher_ &&
        OB_FAIL(msg_watcher_->has_first_buffer(id_, has_first_buffer))) {
      LOG_WARN("failed to get first buffer", K(ret));
    } else if (has_first_buffer) {
      bool need_processed_first_msg = got_from_dtl_cache_ && belong_to_receive_data();
      if (need_processed_first_msg && recv_list_.is_empty()) {
        ObDfcServer& dfc_server = DTL.get_dfc_server();
        if (OB_UNLIKELY(OB_INVALID_ID == dfc_idx_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to find channel", K(ret), K(dfc_idx_));
        } else if (OB_FAIL(dfc_server.try_process_first_buffer(dfc_, dfc_idx_))) {
          LOG_WARN("failed to process first cache buffer", K(ret), K(dfc_idx_));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (true == recv_sem_.wait(timeout)) {
        ObLink* link = nullptr;
        if (OB_SUCC(recv_list_.pop(link))) {
          LOG_TRACE("pop recv list",
              KP(id_),
              K_(peer),
              K(ret),
              K(get_processed_buffer_cnt()),
              K(get_recv_buffer_cnt()),
              K(link));
          process_buffer_ = static_cast<ObDtlLinkedBuffer*>(link);
          if (belong_to_receive_data()) {
            if (1 == process_buffer_->seq_no()) {
              metric_.mark_first_out();
              first_recv_msg_ = false;
              got_from_dtl_cache_ = false;
              if (nullptr != msg_watcher_) {
                msg_watcher_->set_first_no_data(this);
                msg_watcher_->add_last_data_list(this);
              }
            } else if (process_buffer_->is_eof()) {
              // eof message only when drain
              // and other message is droped when send
              first_recv_msg_ = false;
            } else if (first_recv_msg_) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("it's not the first msg",
                  KP(id_),
                  K_(peer),
                  K(ret),
                  K(get_processed_buffer_cnt()),
                  K(get_recv_buffer_cnt()),
                  K(process_buffer_->seq_no()),
                  K(process_buffer_->tenant_id()),
                  K(got_from_dtl_cache_),
                  K(dfc_idx_),
                  K(process_buffer_->is_eof()));
            }
          }
          if (OB_SUCC(ret)) {
            if (ObDtlMsgType::PX_CHUNK_ROW == process_buffer_->msg_type()) {
              if (msg_reader_ != &px_row_iter_) {
                msg_reader_->reset();
                msg_reader_ = &px_row_iter_;
              }
              if (OB_FAIL(msg_reader_->load_buffer(*process_buffer_))) {
                LOG_WARN("failed to init px row iter",
                    KP(id_),
                    K_(peer),
                    K(ret),
                    K(get_processed_buffer_cnt()),
                    K(get_recv_buffer_cnt()));
              }
            } else if (ObDtlMsgType::PX_DATUM_ROW == process_buffer_->msg_type()) {
              if (msg_reader_ != &datum_row_iter_) {
                msg_reader_->reset();
                msg_reader_ = &datum_row_iter_;
              }
              if (OB_FAIL(msg_reader_->load_buffer(*process_buffer_))) {
                LOG_WARN("failed to init px row iter",
                    KP(id_),
                    K_(peer),
                    K(ret),
                    K(get_processed_buffer_cnt()),
                    K(get_recv_buffer_cnt()));
              }
            }
          }
        } else {
          LOG_TRACE("failed to pop recv list",
              KP(id_),
              K_(peer),
              K(ret),
              K(get_processed_buffer_cnt()),
              K(get_recv_buffer_cnt()));
        }
      } else {
        ret = OB_EAGAIN;
        if (nullptr != msg_watcher_) {
          msg_watcher_->remove_data_list(this);
        }
      }
    }
  }
  return ret;
}

int ObDtlBasicChannel::process1(ObIDtlChannelProc* proc, int64_t timeout, bool& last_row_in_buffer)
{
  int ret = OB_SUCCESS;
  last_row_in_buffer = false;
  if (timeout < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("timeout is invalid", K(ret));
  } else {
    if (!use_interm_result()) {
      do {
        if (nullptr == process_buffer_ && OB_FAIL(get_processed_buffer(timeout))) {
          if (OB_EAGAIN == ret) {
          } else {
            LOG_WARN("failed to get buffer", K(ret));
          }
        } else if (nullptr != process_buffer_) {
          auto& buffer = process_buffer_;
          if (ObDtlMsgType::PX_DATUM_ROW == process_buffer_->msg_type()) {
            if (!msg_reader_->is_inited()) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("px row iter is not init", K(ret));
            } else {
              ret = proc->process(*buffer, msg_reader_);
            }
          } else if (ObDtlMsgType::PX_CHUNK_ROW != process_buffer_->msg_type()) {
            ret = proc->process(*buffer, nullptr);
          } else {
            if (!msg_reader_->is_inited()) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("px row iter is not init", K(ret));
            } else {
              ret = proc->process(*buffer, msg_reader_);
            }
          }
          if (OB_ITER_END == ret) {
            inc_processed_buffer_cnt();
            if (buffer->is_eof()) {
              if (buffer->is_data_msg()) {
                metric_.mark_last_out();
              }
              set_eof();
              ret = OB_SUCCESS;
              if (nullptr != msg_watcher_) {
                msg_watcher_->remove_data_list(this);
              }
            }
            LOG_TRACE("process one piece",
                KP(id_),
                K_(peer),
                K(ret),
                K(get_processed_buffer_cnt()),
                K(get_recv_buffer_cnt()),
                K(buffer->is_data_msg()),
                K(buffer->is_eof()));
            int tmp_ret = ret;
            if (OB_SUCCESS != (tmp_ret = unblock_on_decrease_size(buffer->size()))) {
              ret = tmp_ret;
              LOG_WARN("failed to decrease buffer size for dfc",
                  KP(id_),
                  K_(peer),
                  K(ret),
                  K(get_processed_buffer_cnt()),
                  K(get_recv_buffer_cnt()));
            }
            free_buf(buffer);
            buffer = nullptr;
            // last_row_in_buffer = true;
          }
        }
      } while (OB_ITER_END == ret);
    } else {
      ObDTLIntermResultInfo result_info;
      ObDTLIntermResultKey key;
      key.channel_id_ = id_;
      if (channel_is_eof_) {
        ret = OB_EAGAIN;
      } else if (NULL != msg_reader_ && msg_reader_->is_inited()) {
        /*do nothing*/
      } else if (OB_FAIL(ObDTLIntermResultManager::getInstance().atomic_get_interm_result_info(key, result_info))) {
        LOG_WARN("fail to get row store", K(ret));
      } else if (!result_info.is_store_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("there is no row store in internal result", K(ret));
      } else if (OB_FAIL(DTL_IR_STORE_DO(result_info, finish_add_row, true))) {
        LOG_WARN("failed to finish add row", K(ret));
      } else {
        if (!result_info.is_datum_) {
          if (OB_FAIL(result_info.row_store_->begin(px_row_iter_.get_row_store_it()))) {
            LOG_WARN("begin iterator failed", K(ret));
          } else {
            px_row_iter_.set_rows(result_info.row_store_->get_row_cnt());
            px_row_iter_.set_inited();
            msg_reader_ = &px_row_iter_;
          }
        } else {
          if (OB_FAIL(result_info.datum_store_->begin(datum_row_iter_.get_row_store_it()))) {
            LOG_WARN("begin iterator failed", K(ret));
          } else {
            datum_row_iter_.set_rows(result_info.datum_store_->get_row_cnt());
            datum_row_iter_.set_inited();
            msg_reader_ = &datum_row_iter_;
          }
        }
      }
      if (OB_SUCC(ret)) {
        ObDtlLinkedBuffer mock_buffer;
        mock_buffer.set_data_msg(true);
        mock_buffer.set_msg_type(result_info.is_datum_ ? ObDtlMsgType::PX_DATUM_ROW : ObDtlMsgType::PX_CHUNK_ROW);
        ret = proc->process(mock_buffer, msg_reader_);
        if (OB_ITER_END == ret) {
          if (!channel_is_eof_) {
            msg_reader_->set_end();
            channel_is_eof_ = true;
            ret = OB_SUCCESS;
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected error", K(ret));
          }
        }
      }
    }
  }
  // If some error happen at upper layer function, we should release process_buffer_ at function destroy.
  return ret;
}

int ObDtlBasicChannel::send1(std::function<int(const ObDtlLinkedBuffer&)>& proc, int64_t timeout)
{
  int ret = OB_SUCCESS;
  if (timeout < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("timeout is invalid", K(ret));
  } else if (true == send_sem_.wait(timeout)) {
    ObLink* link = nullptr;
    if (OB_SUCC(send_list_.pop(link))) {
      auto buffer = static_cast<ObDtlLinkedBuffer*>(link);
      ret = proc(*buffer);
      free_buf(buffer);
    }
  } else {
    ret = OB_TIMEOUT;
  }
  return ret;
}

int ObDtlBasicChannel::flush(bool force_flush, bool wait_response)
{
  int ret = OB_SUCCESS;
  if (force_flush == true) {
    if (write_buffer_ != nullptr && write_buffer_->pos() != 0) {
      if (OB_FAIL(push_back_send_list())) {
        LOG_WARN("failed to push back send list", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (send_failed_buffer_ != nullptr) {
      LOG_TRACE("send message failed", K(id_), K_(peer), K(ret));
      if (OB_FAIL(send_message(send_failed_buffer_))) {
        LOG_WARN("send message failed", K_(peer), K(ret), K(send_failed_buffer_));
      } else if (nullptr != send_failed_buffer_) {
        free_buf(send_failed_buffer_);
        send_failed_buffer_ = nullptr;
      }
    }
  }
  if (OB_SUCC(ret)) {
    do {
      // reset return code every turn.
      ret = OB_SUCCESS;
      ObLink* link = nullptr;
      if (OB_SUCC(send_list_.pop(link))) {
        auto buffer = static_cast<ObDtlLinkedBuffer*>(link);
        if (OB_FAIL(send_message(buffer))) {
          LOG_WARN("send message failed", K_(peer), K(ret), K(buffer));
          send_failed_buffer_ = buffer;
        } else if (nullptr != buffer) {
          free_buf(buffer);
        }
      } else {
        ret = OB_SUCCESS;
        break;
      }
    } while (OB_SUCC(ret));
  }
  if (OB_SUCC(ret) && force_flush && wait_response && msg_response_.is_in_process()) {
    if (OB_FAIL(msg_response_.wait())) {
      LOG_WARN("send previous message fail", K(ret), K(peer_), K(peer_id_), K(lbt()));
    }
  }
  if (OB_HASH_NOT_EXIST == ret) {
    if (is_drain()) {
      ret = OB_SUCCESS;
    } else {
      ret = OB_ERR_SIGNALED_IN_PARALLEL_QUERY_SERVER;
    }
  }
  return ret;
}

int ObDtlBasicChannel::wait_unblocking_if_blocked()
{
  int ret = OB_SUCCESS;
  if (belong_to_transmit_data()) {
    if (!msg_response_.is_init()) {
      ret = OB_NOT_INIT;
      LOG_WARN("not init", K(ret));
    } else if (msg_response_.is_block()) {
      // receive response, then set block info for dfc
      msg_response_.reset_block();
      if (OB_ISNULL(dfc_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("channel loop is null", K(ret));
      } else if (OB_FAIL(dfc_->block_channel(this))) {
        LOG_WARN("failed to set block", K(ret));
      } else {
        // only block this channel, for other channels, it will also set block, or other channel can't be blocked
        LOG_TRACE("set block dfc", K(ret), KP(id_), K(peer_));
      }
    }
    if (OB_SUCC(ret)) {
      LOG_TRACE("wait unblocking if blocked",
          K(msg_response_.is_block()),
          K(dfc_->is_block(this)),
          K(ret),
          KP(id_),
          KP(peer_id_),
          K(peer_),
          K(ret));
      if (dfc_->is_block(this)) {
        if (OB_FAIL(wait_unblocking())) {
          LOG_WARN("failed to block", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObDtlBasicChannel::wait_unblocking()
{
  int ret = OB_SUCCESS;
  if (belong_to_transmit_data()) {
    int64_t idx = OB_INVALID_ID;
    LOG_TRACE("blocking:", K(ret), K(dfc_->is_block()), K(dfc_));
    int64_t timeout_ts = 0;
    if (OB_ISNULL(channel_loop_) || OB_ISNULL(dfc_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("channel loop is null", K(ret));
    } else if (FALSE_IT(timeout_ts = dfc_->get_timeout_ts())) {
    } else if (0 == timeout_ts) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("timeout is not set", K(ret));
    } else if (OB_FAIL(channel_loop_->find(this, idx))) {
      LOG_WARN("channel not exists in channel loop", K(ret));
    } else {
      block_proc_.set_ch_idx_var(&idx);
      LOG_TRACE("wait unblocking", K(ret), K(dfc_->is_block()), KP(id_), K(peer_));
      do {
        int64_t got_channel_idx = idx;
        if (is_drain()) {
          // if drain,then ignore blocking
          if (OB_FAIL(dfc_->unblock_channel(this))) {
            LOG_WARN("fail to unblock channel", K(ret), K(dfc_->is_block()), KP(id_), K(peer_));
          }
          break;
        } else if (OB_FAIL(channel_loop_->process_one_if(
                       &block_proc_, timeout_ts - ObTimeUtility::current_time(), got_channel_idx))) {
          // no msg, then don't process
          if (OB_EAGAIN == ret) {
            if (ObTimeUtility::current_time() > timeout_ts) {
              ret = OB_TIMEOUT;
              LOG_WARN("get row from channel timeout", K(ret), K(timeout_ts));
            } else {
              int tmp_ret = THIS_WORKER.check_status();
              if (OB_SUCCESS != tmp_ret) {
                ret = tmp_ret;
                LOG_WARN("worker interrupt", K(tmp_ret), K(ret));
                break;
              }
              ret = OB_SUCCESS;
            }
          } else {
            LOG_WARN("fail to process unblocking msg", K(ret), K(dfc_->is_block()), KP(id_), K(peer_));
          }
        } else {
          // receive unblocking msg:
          // channel is unblock, then skip
          // channel is blocked, then unblock the channel
          if (idx != got_channel_idx) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("no channel is processed", K(ret), K(got_channel_idx), K(idx));
          } else if (OB_FAIL(dfc_->unblock_channel(this))) {
            LOG_WARN("fail to unblock channel", K(ret), K(dfc_->is_block()), KP(id_), K(peer_));
          }
        }
      } while (dfc_->is_block(this) && OB_SUCC(ret));
      LOG_TRACE("unblocking successfully", K(ret), K(dfc_->is_block()), KP(id_), K(peer_));
    }
    LOG_TRACE("unblocking:", K(ret), K(dfc_->is_block()), K(dfc_->is_block(this)), KP(id_), K(peer_));
  }
  return ret;
}

int ObDtlBasicChannel::send_message(ObDtlLinkedBuffer*& buf)
{
  UNUSED(buf);
  return common::OB_NOT_IMPLEMENT;
}

ObDtlLinkedBuffer* ObDtlBasicChannel::alloc_buf(const int64_t payload_size)
{
  int ret = OB_SUCCESS;
  ObDtlLinkedBuffer* buf = nullptr;
  ObDtlTenantMemManager* tenant_mem_mgr = DTL.get_dfc_server().get_tenant_mem_manager(tenant_id_);
  if (nullptr == tenant_mem_mgr) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("tenant_mem_mgr is null", K(ret), K(tenant_id_));
  } else {
    // int64_t hash = nullptr != dfc_ ? reinterpret_cast<int64_t>(dfc_) : id_;
    // int64_t hash = reinterpret_cast<int64_t>(this);
    // int64_t hash = GETTID();
    buf = tenant_mem_mgr->alloc(hash_val_, payload_size);
    if (nullptr != buf) {
      alloc_buffer_count();
    }
  }
  return buf;
}

void ObDtlBasicChannel::free_buf(ObDtlLinkedBuffer* buf)
{
  int ret = OB_SUCCESS;
  ObDtlTenantMemManager* tenant_mem_mgr = DTL.get_dfc_server().get_tenant_mem_manager(tenant_id_);

  if (OB_NOT_NULL(buf) && buf->is_bcast() && belong_to_transmit_data() && OB_NOT_NULL(bc_service_)) {
    // do nothing, this buffer is allocated by bcast channel agent.
    // bcast channel agent will release this buffer.
  } else {
    if (nullptr == tenant_mem_mgr) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("tenant_mem_mgr is null", K(lbt()), K(tenant_id_), K(ret));
    } else if (OB_FAIL(tenant_mem_mgr->free(buf))) {
      LOG_WARN("failed to free buffer", K(ret), K(lbt()), K(tenant_id_));
    }
    if (nullptr != buf) {
      free_buffer_count();
    }
  }
}

void ObDtlBasicChannel::clean_broadcast_buffer()
{
  int ret = OB_SUCCESS;
  bool done = false;
  if (nullptr != process_buffer_ && process_buffer_->is_bcast()) {
    done = true;
    process_buffer_ = nullptr;
  }
  if (nullptr != send_failed_buffer_ && send_failed_buffer_->is_bcast()) {
    done = true;
    send_failed_buffer_ = nullptr;
  }
  if (!done) {
    ObLink* link = nullptr;
    if (OB_SUCC(recv_list_.top(link))) {
      ObDtlLinkedBuffer* linked_buffer = static_cast<ObDtlLinkedBuffer*>(link);
      if (linked_buffer->is_bcast()) {
        done = true;
        recv_list_.pop(link);
      }
    }
  }
  LOG_TRACE("trace clean broadcast dtl buffer", K(done), K(*this));
}

int ObDtlBasicChannel::push_back_send_list()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(send_list_.push(write_buffer_))) {
    LOG_WARN("failed to push back send list", K(ret));
  } else {
    msg_writer_->reset();
    inc_send_buffer_cnt();
    write_buffer_->size() = write_buffer_->pos();
    write_buffer_->pos() = 0;
    write_buffer_->seq_no() = get_send_buffer_cnt();
    write_buffer_->tenant_id() = tenant_id_;
    if (write_buffer_->is_data_msg()) {
      write_buffer_->set_use_interm_result(use_interm_result_);
    }
    LOG_TRACE("push message to send list",
        K(write_buffer_->seq_no()),
        K(ret),
        KP(id_),
        KP(peer_id_),
        K(write_buffer_->tenant_id()),
        K(is_data_msg_),
        K(write_buffer_->size()),
        K(write_buffer_->pos()),
        K(get_send_buffer_cnt()),
        K(write_buffer_->is_data_msg()),
        K(write_buffer_->is_eof()));
    write_buffer_ = nullptr;
  }
  return ret;
}

int ObDtlBasicChannel::switch_writer(const ObDtlMsg& msg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == msg_writer_)) {
    if (msg.is_data_msg()) {
      const ObPxNewRow& px_row = static_cast<const ObPxNewRow&>(msg);
      if (DtlWriterType::CHUNK_ROW_WRITER == msg_writer_map[px_row.get_data_type()]) {
        msg_writer_ = &row_msg_writer_;
      } else if (DtlWriterType::CHUNK_DATUM_WRITER == msg_writer_map[px_row.get_data_type()]) {
        msg_writer_ = &datum_msg_writer_;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unkown msg writer", K(msg.get_type()), K(px_row.get_data_type()), K(msg_writer_->type()), K(ret));
      }
      LOG_TRACE("msg writer", K(px_row.get_data_type()), K(msg_writer_->type()), K(ret));
    } else {
      if (DtlWriterType::CONTROL_WRITER == msg_writer_map[msg.get_type()]) {
        msg_writer_ = &ctl_msg_writer_;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unkown msg writer", K(msg.get_type()), K(msg_writer_->type()));
      }
      LOG_TRACE("msg writer", K(msg.get_type()), K(msg_writer_->type()), K(ret));
    }
  } else {
#ifndef NDEBUG
    if (msg.is_data_msg()) {
      const ObPxNewRow& px_row = static_cast<const ObPxNewRow&>(msg);
      if (msg_writer_map[px_row.get_data_type()] != msg_writer_->type()) {
        ret = OB_ERR_UNEXPECTED;
      }
    } else {
      if (msg_writer_map[msg.get_type()] != msg_writer_->type()) {
        ret = OB_ERR_UNEXPECTED;
      }
    }
#endif
  }
  return ret;
}

int ObDtlBasicChannel::inner_write_msg(const ObDtlMsg& msg, int64_t timeout_ts, ObEvalCtx* eval_ctx, bool is_eof)
{
  int ret = OB_SUCCESS;
  int64_t need_size = 0;
  bool need_new = false;
  if (OB_FAIL(switch_writer(msg))) {
  } else if (OB_FAIL(msg_writer_->need_new_buffer(msg, eval_ctx, need_size, need_new))) {
    LOG_WARN("failed to judge need new buffer", K(ret));
  } else if (OB_UNLIKELY(need_new)) {
    if (OB_FAIL(switch_buffer(need_size, is_eof, timeout_ts))) {
      LOG_WARN("failed to switch buffer", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(msg_writer_->write(msg, eval_ctx, is_eof))) {
      if (OB_BUF_NOT_ENOUGH != ret) {
        LOG_WARN("failed to write msg", K(ret));
      }
    } else {
      LOG_DEBUG(
          "trace msg write", K(is_eof), K(is_data_msg_), K(msg.get_type()), K(write_buffer_->msg_type()), K(need_new));
    }
  }
  return ret;
}

int ObDtlBasicChannel::write_msg(const ObDtlMsg& msg, int64_t timeout_ts, ObEvalCtx* eval_ctx, bool is_eof)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(inner_write_msg(msg, timeout_ts, eval_ctx, is_eof))) {
    if (OB_BUF_NOT_ENOUGH == ret) {
      if (OB_FAIL(inner_write_msg(msg, timeout_ts, eval_ctx, is_eof))) {
        LOG_WARN("failed to write msg", K(ret));
      }
    } else {
      LOG_WARN("failed to write msg", K(ret));
    }
  }
  return ret;
}

int ObDtlBasicChannel::switch_buffer(const int64_t min_size, const bool is_eof, const int64_t timeout_ts)
{
  int ret = OB_SUCCESS;
  if (write_buffer_ != nullptr && write_buffer_->pos() != 0) {
    if (OB_FAIL(msg_writer_->serialize())) {
      LOG_WARN("convert block to copyable failed", K(ret));
    } else if (OB_FAIL(push_back_send_list())) {
      LOG_WARN("failed to push back send list", K(ret));
    }
  } else if (write_buffer_ != nullptr) {
    /*
     * If write_buffer_ isn't null, write_buffer_'s pos == 0, and the need size > write_buffer_ size,
     * we will request a new write_buffer_.
     * We should recycle write_buffer_ before we allocate a new write_buffer_.
     * */
    free_buf(write_buffer_);
    write_buffer_ = nullptr;
  } else {
    // write_buffer_ is nullptr, do nothing.
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(write_buffer_ = alloc_buf(std::max(send_buffer_size_, min_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc buffer failed", K(ret));
    } else if (OB_FAIL(msg_writer_->init(write_buffer_, tenant_id_))) {
      free_buf(write_buffer_);
      write_buffer_ = nullptr;
      LOG_WARN("failed to init message writer", K(ret));
    } else {
      if (OB_NOT_NULL(dfc_)) {
        write_buffer_->set_dfo_key(dfc_->get_dfo_key());
      }
      write_buffer_->timeout_ts() = timeout_ts;
      msg_writer_->write_msg_type(write_buffer_);
      write_buffer_->set_data_msg(is_data_msg_);
      write_buffer_->is_eof() = is_eof;
      LOG_TRACE("trace new buffer", K(is_data_msg_), K(is_eof), KP(id_), KP(peer_id_));
    }
  }
  return ret;
}

int ObDtlBasicChannel::send_buffer(ObDtlLinkedBuffer*& buffer)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(send_list_.push(buffer))) {
    LOG_WARN("failed to push back send list", K(ret));
  } else {
    inc_send_buffer_cnt();
    buffer->size() = buffer->pos();
    buffer->pos() = 0;
    buffer->seq_no() = get_send_buffer_cnt();
    buffer->tenant_id() = tenant_id_;
    if (buffer->is_data_msg()) {
      buffer->set_use_interm_result(use_interm_result_);
    }
    LOG_TRACE("push buffer to send list",
        K(buffer->seq_no()),
        K(ret),
        KP(id_),
        KP(peer_id_),
        K(buffer->tenant_id()),
        K(is_data_msg_),
        K(buffer->size()),
        K(buffer->pos()),
        K(get_send_buffer_cnt()),
        K(buffer->is_data_msg()),
        K(buffer->is_eof()));
    buffer = nullptr;
  }

  if (OB_SUCC(ret) && !send_list_.is_empty()) {
    if (OB_FAIL(flush(false))) {
      LOG_WARN("flush fail. ignore. may retry flush again very soon", K(ret));
    }
  }

  return ret;
}

//----ObDtlRowMsgWriter
ObDtlRowMsgWriter::ObDtlRowMsgWriter() : type_(CHUNK_ROW_WRITER), row_store_(), block_(nullptr), write_buffer_(nullptr)
{}

ObDtlRowMsgWriter::~ObDtlRowMsgWriter()
{
  reset();
}

int ObDtlRowMsgWriter::init(ObDtlLinkedBuffer* buffer, uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (nullptr == buffer) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("write buffer is null", K(ret));
  } else {
    reset();
    row_store_.init(0, tenant_id, common::ObCtxIds::DEFAULT_CTX_ID, common::ObModIds::OB_SQL_DTL);
    if (OB_FAIL(row_store_.init_block_buffer(static_cast<void*>(buffer->buf()), buffer->size(), block_))) {
      LOG_WARN("init shrink buffer failed", K(ret));
    } else {
      row_store_.add_block(block_, false);
      write_buffer_ = buffer;
    }
  }
  return ret;
}

void ObDtlRowMsgWriter::reset()
{
  row_store_.remove_added_blocks();
  row_store_.reset();
  block_ = nullptr;
  write_buffer_ = nullptr;
}

int ObDtlRowMsgWriter::need_new_buffer(const ObDtlMsg& msg, ObEvalCtx* ctx, int64_t& need_size, bool& need_new)
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  int64_t serialize_need_size = 0;
  const ObPxNewRow& px_row = static_cast<const ObPxNewRow&>(msg);
  const ObNewRow* row = px_row.get_row();
  if (nullptr == row) {
    serialize_need_size = ObChunkRowStore::Block::min_buf_size(0);
    need_size = serialize_need_size;
  } else {
    serialize_need_size = ObChunkRowStore::Block::row_store_size(*row);
    need_size = ObChunkRowStore::Block::min_buf_size(serialize_need_size);
  }
  need_new = nullptr == write_buffer_ || (remain() < serialize_need_size);
  if (need_new && nullptr != write_buffer_) {
    write_buffer_->pos() = used();
  }
  return ret;
}

int ObDtlRowMsgWriter::serialize()
{
  return block_->unswizzling();
}
//-----------------end ObDtlRowMsgWriter----------------

//-----------------start ObDtlDatumMsgWrite-------------
ObDtlDatumMsgWriter::ObDtlDatumMsgWriter()
    : type_(CHUNK_DATUM_WRITER), write_buffer_(nullptr), block_(nullptr), write_ret_(OB_SUCCESS)
{}

ObDtlDatumMsgWriter::~ObDtlDatumMsgWriter()
{
  reset();
}

int ObDtlDatumMsgWriter::init(ObDtlLinkedBuffer* buffer, uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  UNUSED(tenant_id);
  if (nullptr == buffer) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("write buffer is null", K(ret));
  } else {
    reset();
    if (OB_FAIL(ObChunkDatumStore::init_block_buffer(static_cast<void*>(buffer->buf()), buffer->size(), block_))) {
      LOG_WARN("init shrink buffer failed", K(ret));
    } else {
      write_buffer_ = buffer;
    }
  }
  return ret;
}

int ObDtlDatumMsgWriter::need_new_buffer(const ObDtlMsg& msg, ObEvalCtx* ctx, int64_t& need_size, bool& need_new)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(OB_BUF_NOT_ENOUGH != write_ret_ && nullptr != write_buffer_)) {
    need_new = false;
  } else {
    int64_t serialize_need_size = 0;
    const ObPxNewRow& px_row = static_cast<const ObPxNewRow&>(msg);
    const ObIArray<ObExpr*>* row = px_row.get_exprs();
    if (nullptr == row) {
      serialize_need_size = ObChunkDatumStore::Block::min_buf_size(0);
      need_size = serialize_need_size;
    } else {
      if (OB_FAIL(ObChunkDatumStore::Block::row_store_size(*row, *ctx, serialize_need_size))) {
        LOG_WARN("failed to calc row store size", K(ret));
      }
      need_size = ObChunkDatumStore::Block::min_buf_size(serialize_need_size);
    }
    need_new = nullptr == write_buffer_ || (remain() < serialize_need_size);
    if (need_new && nullptr != write_buffer_) {
      write_buffer_->pos() = rows() > 0 ? used() : 0;
    }
  }
  write_ret_ = OB_SUCCESS;
  return ret;
}

void ObDtlDatumMsgWriter::reset()
{
  block_ = nullptr;
  write_buffer_ = nullptr;
}

int ObDtlDatumMsgWriter::serialize()
{
  return block_->unswizzling();
}
//--------------end ObDtlDatumMsgWriter---------------

//----------------start ObDtlControlMsgWriter----------
int ObDtlControlMsgWriter::write(const ObDtlMsg& msg, ObEvalCtx* eval_ctx, const bool is_eof)
{
  int ret = OB_SUCCESS;
  UNUSED(eval_ctx);
  ObDtlMsgHeader header;
  header.nbody_ = static_cast<int32_t>(msg.get_serialize_size());
  header.type_ = static_cast<int16_t>(msg.get_type());
  auto buf = write_buffer_->buf();
  auto size = write_buffer_->size();
  auto& pos = write_buffer_->pos();
  write_buffer_->set_data_msg(false);
  write_buffer_->is_eof() = is_eof;
  if (OB_FAIL(serialization::encode(buf, size, pos, header))) {
    LOG_WARN("serialize RPC channel message type fail", K(size), K(pos), K(ret));
  } else if (OB_FAIL(serialization::encode(buf, size, pos, msg))) {
    LOG_WARN("serialize RPC channel message fail", K(size), K(pos), K(ret));
  }
  return ret;
}

//----------------start ObDtlControlMsgWriter----------

}  // namespace dtl
}  // namespace sql
}  // namespace oceanbase
