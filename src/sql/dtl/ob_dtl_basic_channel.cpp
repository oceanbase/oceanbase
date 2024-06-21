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
#include "share/rc/ob_context.h"
#include "share/rc/ob_tenant_base.h"
#include "sql/dtl/ob_dtl_rpc_proxy.h"
#include "sql/dtl/ob_dtl.h"
#include "sql/dtl/ob_dtl_flow_control.h"
#include "sql/engine/px/ob_px_row_store.h"
#include "sql/engine/px/ob_px_bloom_filter.h"
#include "sql/engine/px/ob_px_dtl_msg.h"
#include "sql/dtl/ob_dtl_interm_result_manager.h"
#include "sql/dtl/ob_dtl_channel_loop.h"
#include "sql/dtl/ob_dtl_channel_watcher.h"
#include "observer/omt/ob_th_worker.h"
#include "sql/engine/px/ob_px_util.h"
#include "sql/session/ob_sql_session_info.h"

using namespace oceanbase::common;
using namespace oceanbase::share;

namespace oceanbase {
namespace sql {
namespace dtl {
SendMsgResponse::SendMsgResponse()
    : inited_(false), ret_(OB_SUCCESS), in_process_(false), finish_(true), is_block_(false),
    cond_(), ch_id_(-1)
{
}

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
    int64_t count_v = 0;
    int64_t start_t = 0;
    int64_t end_t = 0;
    int64_t interval = 60000; // ms
    while (!finish_ && OB_SUCC(ret)) {
      // 这里为什么是while true等待，因为callback引用了当前线程一些变量，
      // 如果采用中断，提前退出，会导致callback如果晚于线程退出，则引用非法东西而core掉
      cond_.wait(1);
      ++count_v;
      // 60s
      if (count_v > interval) {
        if (0 == start_t) {
          start_t = ObTimeUtility::current_time();
        }
        count_v = 0;
      }
      if (0 != start_t && 10 < count_v) {
        end_t = ObTimeUtility::current_time();
        if (end_t - interval * 1000  >= start_t) {
          LOG_WARN("channel can't receive response", K(ch_id_));
          start_t = end_t;
        }
        count_v = 0;
      }
    }
    in_process_ = false;
  }
  return OB_SUCCESS == ret ? ret_ : ret;
}

ObDtlBasicChannel::ObDtlBasicChannel(
    const uint64_t tenant_id,
    const uint64_t id,
    const ObAddr &peer,
    DtlChannelType type)
    : ObDtlChannel(id, peer, type),
      is_inited_(false),
      local_id_(id),
      peer_id_(id ^ 1),
      write_buffer_(nullptr),
      process_buffer_(nullptr),
      alloc_new_buf_(false),
      seq_no_(0),
      send_buffer_cnt_(0),
      recv_buffer_cnt_(0),
      processed_buffer_cnt_(0),
      tenant_id_(tenant_id),
      is_data_msg_(false),
      hash_val_(0),
      dfc_idx_(OB_INVALID_ID),
      got_from_dtl_cache_(true),
      msg_writer_(nullptr),
      bc_service_(nullptr),
      times_(0),
      write_buf_use_time_(0),
      send_use_time_(0),
      msg_count_(0),
      result_info_guard_()
{
  ObRandom rand;
  hash_val_ = rand.get();
  // dtl创建时候的server版本决定发送老的ser方式还是新的chunk row store方式
  use_crs_writer_ = true;
  msg_response_.set_id(id_);
}

ObDtlBasicChannel::ObDtlBasicChannel(
    const uint64_t tenant_id,
    const uint64_t id,
    const ObAddr &peer,
    const int64_t hash_val,
    DtlChannelType type)
    : ObDtlChannel(id, peer, type),
          is_inited_(false),
          local_id_(id),
          peer_id_(id ^ 1),
          write_buffer_(nullptr),
          process_buffer_(nullptr),
          alloc_new_buf_(false),
          seq_no_(0),
          send_buffer_cnt_(0),
          recv_buffer_cnt_(0),
          processed_buffer_cnt_(0),
          tenant_id_(tenant_id),
          is_data_msg_(false),
          hash_val_(hash_val),
          dfc_idx_(OB_INVALID_ID),
          got_from_dtl_cache_(true),
          msg_writer_(nullptr),
          bc_service_(nullptr),
          times_(0),
          write_buf_use_time_(0),
          send_use_time_(0),
          msg_count_(0)
{
  // dtl创建时候的server版本决定发送老的ser方式还是新的chunk row store方式
  use_crs_writer_ = true;
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
    if (OB_HASH_NOT_EXIST == ret) {
      if (is_drain()) {
        ret = OB_SUCCESS;
      } else {
        ret = OB_ERR_SIGNALED_IN_PARALLEL_QUERY_SERVER;
      }
    }
  }
  return ret;
}

int ObDtlBasicChannel::clear_response_block()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(wait_response())) {
    LOG_WARN("failed to wait response", K(ret));
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
    is_inited_ = false;
    if (process_buffer_ != nullptr) {
      free_buf(process_buffer_);
      process_buffer_ = nullptr;
    }
    if (write_buffer_ != nullptr) {
      free_buf(write_buffer_);
      write_buffer_ = nullptr;
    }
    ObLink *link = NULL;
    while (OB_SUCCESS == send_list_.pop(link) && NULL != link) {
      auto p = static_cast<ObDtlLinkedBuffer *>(link);
      if (nullptr != p) {
        free_buf(p);
      }
    }
    ObSpLinkQueue *queues[] = { &recv_list_, &free_list_ };
    for (int64_t i = 0; i < ARRAYSIZEOF(queues); i++) {
      while (OB_SUCCESS == queues[i]->pop(link) && NULL != link) {
        auto p = static_cast<ObDtlLinkedBuffer *>(link);
        if (nullptr != p) {
          free_buf(p);
        }
      }
    }
    reset_px_row_iterator();
  }
  if (alloc_buffer_cnt_ != free_buffer_cnt_) {
    int tmp_ret = OB_ERR_UNEXPECTED;
    LOG_ERROR_RET(tmp_ret, "channel may exists buffer to free", KP(id_), K(peer_id_), K(tmp_ret), K(alloc_buffer_cnt_), K(free_buffer_cnt_));
  }
}

int ObDtlBasicChannel::send(const ObDtlMsg &msg, int64_t timeout_ts,
    ObEvalCtx *eval_ctx, bool is_eof)
{
  int ret = OB_SUCCESS;
  // serialize message and link it to send buffer list.
  //
  // | ObDtlLinkedBuffer structure | message serialize size | serialized message |
  //
  is_data_msg_ = belong_to_transmit_data() && msg.is_data_msg();
  // 这里直接与发送eof row正交化了，即没有通过判断是eof row来决定是否是last msg
  channel_is_eof_ = is_eof;
  if (is_data_msg_) {
    metric_.mark_first_in();
    if (channel_is_eof_) {
      metric_.mark_eof();
    }
    //TODO : opt time in batch
    //metric_.set_last_in_ts(::oceanbase::common::ObTimeUtility::current_time());
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

int ObDtlBasicChannel::feedup(ObDtlLinkedBuffer *&buffer)
{
  UNUSED(buffer);
  return common::OB_NOT_IMPLEMENT;
}

int ObDtlBasicChannel::mock_eof_buffer(int64_t timeout_ts)
{
  int ret = OB_SUCCESS;
  int64_t min_buf_size = ObChunkDatumStore::Block::min_buf_size(0);
  ObDtlLinkedBuffer *buffer = NULL;
  MTL_SWITCH(tenant_id_) {
    ObChunkDatumStore row_store("MockDtlStore");
    ObChunkDatumStore::Block* block = NULL;
    if (OB_ISNULL(buffer = alloc_buf(min_buf_size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc buffer failed", K(ret));
    } else if (OB_FAIL(row_store.init_block_buffer(static_cast<void *>(buffer->buf()), buffer->size(), block))) {
      LOG_WARN("init block buffer failed", K(ret));
    } else {
      buffer->msg_type() = ObDtlMsgType::PX_DATUM_ROW;
      buffer->is_eof() = true;
      buffer->tenant_id() = tenant_id_;
      buffer->timeout_ts() = timeout_ts;
      buffer->size() = block->data_size();
      buffer->set_data_msg(true);
      buffer->seq_no() = 1;
      buffer->pos() = 0;
      if (use_interm_result_) {
        if (OB_FAIL(MTL(ObDTLIntermResultManager*)->process_interm_result(buffer, id_))) {
          LOG_WARN("fail to process internal result", K(ret));
        }
      } else {
        bool inc_recv_buf_cnt = false;
        if (OB_FAIL(attach(buffer, inc_recv_buf_cnt))) {
          LOG_WARN("fail to attach buffer", K(ret));
        } else {
          free_buffer_count();
        }
      }
    }
    if (NULL != buffer) {
      free_buffer_count();
    }
  }
  return ret;
}

int ObDtlBasicChannel::attach(ObDtlLinkedBuffer *&linked_buffer, bool inc_recv_buf_cnt)
{
  int ret = OB_SUCCESS;
  ObDtlMsgHeader header;
  const bool keep_pos = true;
  LOG_DEBUG("local attach attach buffer", KP(id_), K(linked_buffer), KP(linked_buffer));
  bool is_data_msg = linked_buffer->is_data_msg();
  bool is_eof = linked_buffer->is_eof();
  if (!is_data_msg
      && OB_FAIL(ObDtlLinkedBuffer::deserialize_msg_header(*linked_buffer, header, keep_pos))) {
    LOG_WARN("failed to deserialize msg header", K(ret));
  } else if (header.is_drain()) {
    alloc_buffer_count();
    if (inc_recv_buf_cnt) {
      inc_recv_buffer_cnt();
    }
    dfc_->set_drain(this);
    LOG_TRACE("transmit receive drain cmd", KP(linked_buffer), K(this), KP(id_), KP(peer_id_),
        K(recv_list_.is_empty()));
    free_buf(linked_buffer);
    linked_buffer = nullptr;
  } else if (header.is_px_bloom_filter_data()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("can't attach bloom filter message", K(ret));
  } else if (OB_FAIL(block_on_increase_size(linked_buffer->size()))) {
    LOG_WARN("failed to increase buffer size for dfc", K(ret));
  } else if (OB_FAIL(recv_list_.push(linked_buffer))) {
    LOG_WARN("push buffer into channel recv list fail", K(ret));
  } else {
    // after push back linked buffer, cannot use the linked buffer again
    // because the buffer may have been released(finished using) by the receiver
    linked_buffer = nullptr;
    // 将attach收到的是自己申请的，因为释放权交给了当前channel，所以认为这次申请也是自己，与free保持一致，方便统计
    alloc_buffer_count();
    if (inc_recv_buf_cnt) {
      inc_recv_buffer_cnt();
    }
    if (is_data_msg) {
      metric_.mark_first_in();
      if (is_eof) {
        metric_.mark_eof();
      }
      metric_.set_last_in_ts(::oceanbase::common::ObTimeUtility::current_time());
    }
    linked_buffer = nullptr;
    IGNORE_RETURN recv_sem_.signal();
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
    ObDfcServer &dfc_server = DTL.get_dfc_server();
    int64_t ch_idx = OB_INVALID_ID;
    // block 和unblock在可能在register和unregister时进行，所以idx会动态修改
    if (OB_FAIL(dfc_->find(this, ch_idx))) {
      LOG_WARN("failed to find channel", K(ret));
    } else if (OB_FAIL(dfc_server.block_on_increase_size(dfc_, ch_idx, size))) {
      LOG_WARN("failed to block channel", K(ret), KP(id_), K(peer_), K(dfc_), K(ch_idx));
    }
  }
  return ret;
}

int ObDtlBasicChannel::unblock_on_decrease_size(int64_t size)
{
  int ret = OB_SUCCESS;
  if (belong_to_receive_data()) {
    ObDfcServer &dfc_server = DTL.get_dfc_server();
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
  LOG_TRACE("clean recv list", K(belong_to_receive_data()), KP(id_), K_(peer), K(ret), K(get_processed_buffer_cnt()), K(get_recv_buffer_cnt()));
  if (belong_to_receive_data()) {
    LOG_TRACE("clean process buffer", KP(id_), K_(peer), K(ret), K(get_processed_buffer_cnt()), K(get_recv_buffer_cnt()));
    if (nullptr != process_buffer_) {
      auto &buffer = process_buffer_;
      LOG_TRACE("free process buffer for dfc", K(buffer->size()), KP(id_), K_(peer), K(ret), K(get_processed_buffer_cnt()), K(get_recv_buffer_cnt()));
      if (OB_FAIL(unblock_on_decrease_size(buffer->size()))) {
        LOG_WARN("failed to decrease buffer size for dfc", KP(id_), K_(peer), K(ret), K(get_processed_buffer_cnt()), K(get_recv_buffer_cnt()));
      }
      free_buf(buffer);
      process_buffer_ = nullptr;
    }
    ObLink *link = nullptr;
    while (OB_SUCC(recv_list_.pop(link))) {
      process_buffer_ = static_cast<ObDtlLinkedBuffer *>(link);
      auto &buffer = process_buffer_;
      LOG_TRACE("free recv list buffer for dfc", K(buffer->size()), KP(id_), K_(peer), K(ret),
        K(get_processed_buffer_cnt()), K(get_recv_buffer_cnt()), K(lbt()));
      if (OB_FAIL(unblock_on_decrease_size(buffer->size()))) {
        LOG_WARN("failed to decrease buffer size for dfc", KP(id_), K_(peer), K(ret), K(get_processed_buffer_cnt()), K(get_recv_buffer_cnt()));
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
  if (OB_LIKELY(nullptr == process_buffer_)) {
    auto key = recv_sem_.get_key();
    recv_sem_.wait(key, timeout);
    ObLink *link = nullptr;
    if (OB_SUCC(recv_list_.pop(link))) {
      LOG_TRACE("pop recv list", KP(id_), K_(peer), K(ret), K(get_processed_buffer_cnt()), K(get_recv_buffer_cnt()), K(link));
      process_buffer_ = static_cast<ObDtlLinkedBuffer *>(link);
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
          LOG_WARN("it's not the first msg", KP(id_), K_(peer), K(ret),
            K(get_processed_buffer_cnt()), K(get_recv_buffer_cnt()),
            K(process_buffer_->seq_no()), K(process_buffer_->tenant_id()),
            K(got_from_dtl_cache_), K(dfc_idx_), K(process_buffer_->is_eof()));
        }
      }
    } else {
      if (nullptr != msg_watcher_) {
        msg_watcher_->remove_data_list(this);
      }
      LOG_TRACE("failed to pop recv list", KP(id_), K_(peer), K(ret), K(get_processed_buffer_cnt()), K(get_recv_buffer_cnt()));
    }
  }
  return ret;
}

int ObDtlBasicChannel::process1(
    ObIDtlChannelProc *proc,
    int64_t timeout,
    bool &last_row_in_buffer)
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
          auto &buffer = process_buffer_;
          bool transferred = false;
          ret = proc->process(*buffer, transferred);
          LOG_DEBUG("process buffer", K(ret), KP(buffer), K(transferred));
          if (buffer->is_data_msg()) {
            metric_.set_last_out_ts(::oceanbase::common::ObTimeUtility::current_time());
          }
          if (OB_ITER_END == ret || transferred) {
            inc_processed_buffer_cnt();
            if (buffer->is_eof()) {
              if (buffer->is_data_msg()) {
                metric_.mark_eof();
              }
              if (!is_eof()) {
                if (NULL != channel_loop_) {
                  channel_loop_->inc_eof_cnt();
                }
                set_eof();
                ret = OB_SUCCESS;
                if (nullptr != msg_watcher_) {
                  msg_watcher_->remove_data_list(this);
                }
              } else {
                ret = OB_EAGAIN;
              }
            }
            LOG_TRACE("process one piece", KP(id_), K_(peer), K(ret),
                      K(get_processed_buffer_cnt()), K(get_recv_buffer_cnt()),
                      K(buffer->is_data_msg()), K(buffer->is_eof()),
                      K(transferred));
            int tmp_ret = ret;
            if (OB_SUCCESS != (tmp_ret = unblock_on_decrease_size(buffer->size()))) {
              ret = tmp_ret;
              LOG_WARN("failed to decrease buffer size for dfc",
                       KP(id_), K_(peer), K(ret), K(get_processed_buffer_cnt()),
                       K(get_recv_buffer_cnt()));
            }
            if (transferred) {
              // only increase the statist
              free_buffer_count();
            } else {
              free_buf(buffer);
            }
            buffer = nullptr;
            // 测试发现每次一个channel读数据，性能更好，将之前由读一个buffer改为读一个channel所有buffer
            // last_row_in_buffer = true;
          }
        }
      } while (OB_ITER_END == ret);
    } else {
      ObDTLIntermResultInfo *result_info = NULL;
      ObDTLIntermResultKey key;
      key.channel_id_ = id_;
      key.batch_id_ = batch_id_;
      MTL_SWITCH(tenant_id_) {
        if (channel_is_eof_) {
          ret = OB_EAGAIN;
        } else if (OB_FAIL(MTL(ObDTLIntermResultManager*)->atomic_get_interm_result_info(
              key, result_info_guard_))) {
          if (is_px_channel()) {
            ret = OB_EAGAIN;
          } else if (ignore_error()) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("fail to get row store", K(ret));
          }
          LOG_TRACE("fail to get row store", K(ret), K(key.batch_id_), K(key.channel_id_));
        } else if (FALSE_IT(result_info = result_info_guard_.result_info_)) {
        } else if (OB_SUCCESS != result_info->ret_) {
          ret = result_info->ret_;
          LOG_WARN("the interm result info meet a error", K(ret));
        } else if (!result_info->is_store_valid()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("there is no row store in internal result", K(ret));
        } else if (OB_FAIL(DTL_IR_STORE_DO(*result_info, finish_add_row, true))) {
          LOG_WARN("failed to finish add row", K(ret));
        } else {
          if (OB_FAIL(result_info->datum_store_->begin(datum_iter_))) {
            LOG_WARN("begin iterator failed", K(ret));
          }
        }
      }
      if (OB_SUCC(ret) && !channel_is_eof()) {
        ObDtlLinkedBuffer mock_buffer;
        mock_buffer.set_data_msg(true);
        ObDtlMsgType type = ObDtlMsgType::PX_DATUM_ROW;
        type = static_cast<ObDtlMsgType>(-type);
        // set type to negative value for interm result mock buffer.
        mock_buffer.set_msg_type(type);
        mock_buffer.set_buf(reinterpret_cast<char *>(&datum_iter_));
        bool transferred = false;
        ret = proc->process(mock_buffer, transferred);
        // EOF after send iterator to processor.
        if (NULL != channel_loop_) {
          channel_loop_->inc_eof_cnt();
        }
        set_eof();
        channel_is_eof_ = true;
      }
    }
  }
  // If some error happen at upper layer function, we should release process_buffer_ at function destroy.
  return ret;
}

int ObDtlBasicChannel::send1(
    std::function<int(const ObDtlLinkedBuffer &)> &proc,
    int64_t timeout)
{
  int ret = OB_SUCCESS;
  if (timeout < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("timeout is invalid", K(ret));
  } else {
    auto key = send_sem_.get_key();
    send_sem_.wait(key, timeout);
    ObLink *link = nullptr;
    if (OB_SUCC(send_list_.pop(link))) {
      auto buffer = static_cast<ObDtlLinkedBuffer *>(link);
      ret = proc(*buffer);
      free_buf(buffer);
    }
  }
  return ret;
}

// force_flush 表示需要把channel所有的msg全部发送
// wait_resp 表示是否要等RPC的回包，为了与之前语义兼容，必须强制发送msg以及wait_resp才会等待回包
int ObDtlBasicChannel::flush(bool force_flush, bool wait_resp)
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
    do {
      // reset return code every turn.
      ret = OB_SUCCESS;
      ObLink *link = nullptr;
      if (OB_SUCC(send_list_.pop(link))) {
        auto buffer = static_cast<ObDtlLinkedBuffer *>(link);
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
      } else {
        ret = OB_SUCCESS;
        break;
      }
    } while (OB_SUCC(ret));
  }
  if (OB_SUCC(ret) && force_flush && wait_resp) {
    if (OB_FAIL(wait_response())) {
      LOG_WARN("send previous message fail", K(ret), K(peer_), K(peer_id_), K(lbt()));
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
        LOG_WARN("failed to set block", K(ret));
      } else {
        // only block this channel, for other channels, it will also set block, or other channel can't be blocked
        LOG_TRACE("set block dfc", K(ret), KP(id_), K(peer_));
      }
    }
    if (OB_SUCC(ret)) {
      LOG_TRACE("wait unblocking if blocked", K(msg_response_.is_block()), K(dfc_->is_block(this)), K(ret), KP(id_), KP(peer_id_),
        K(peer_), K(ret));
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
      int64_t start_t = ObTimeUtility::current_time();
      int64_t interval_t = 1 * 60 * 1000000; // 1min
      int64_t last_t = 0;
      int64_t print_log_t = 10 * 60 * 1000000;
      block_proc_.set_ch_idx_var(&idx);
      LOG_TRACE("wait unblocking", K(ret), K(dfc_->is_block()), KP(id_), K(peer_));
      oceanbase::lib::Thread::WaitGuard guard(oceanbase::lib::Thread::WAIT_FOR_PX_MSG);
      do {
        int64_t got_channel_idx = idx;
        if (is_drain()) {
          // if drain,then ignore blocking
          if (OB_FAIL(dfc_->unblock_channel(this))) {
            LOG_WARN("fail to unblock channel", K(ret), K(dfc_->is_block()), KP(id_), K(peer_));
          }
          break;
        } else if (OB_FAIL(channel_loop_->process_one_if(
            &block_proc_,
            got_channel_idx))) {
          // no msg, then don't process
          if (OB_EAGAIN == ret) {
            // 这里需要检查是否超时以及worker是否已经处于异常状态(退出等)，否则当worker退出时，一直陷入在while中
            int64_t end_t = ObTimeUtility::current_time();
            if (end_t > timeout_ts) {
              ret = OB_TIMEOUT;
              LOG_WARN("get row from channel timeout", K(ret), K(timeout_ts));
            } else {
              int tmp_ret = THIS_WORKER.check_status();
              if (OB_SUCCESS != tmp_ret) {
                ret = tmp_ret;
                LOG_WARN("worker interrupt", K(tmp_ret), K(ret));
                break;
              }
              if (OB_UNLIKELY(ObPxCheckAlive::is_in_blacklist(peer_,
                              channel_loop_->get_process_query_time()))) {
                ret = OB_RPC_CONNECT_ERROR;
                LOG_WARN("peer no in communication, maybe crashed", K(ret), K(peer_),
                         K(static_cast<int64_t>(GCONF.cluster_id)));
                break;
              }
              if (end_t - start_t > print_log_t) {
                bool print_log = false;
                if (0 == last_t || end_t - last_t > interval_t) {
                  print_log = true;
                  last_t = end_t;
                }
                if (print_log) {
                  LOG_INFO("wait unblocking", K(id_), K(peer_id_), K(peer_id_),
                    K(recv_buffer_cnt_), K(processed_buffer_cnt_), K(send_buffer_cnt_),
                    K(idx), K(got_channel_idx));
                }
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

int ObDtlBasicChannel::send_message(ObDtlLinkedBuffer *&buf)
{
  UNUSED(buf);
  return common::OB_NOT_IMPLEMENT;
}

ObDtlLinkedBuffer *ObDtlBasicChannel::alloc_buf(const int64_t payload_size)
{
  int ret = OB_SUCCESS;
  ObDtlLinkedBuffer *buf = nullptr;
  ObDtlTenantMemManager *tenant_mem_mgr = DTL.get_dfc_server().get_tenant_mem_manager(tenant_id_);
  if (nullptr == tenant_mem_mgr) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("tenant_mem_mgr is null", K(ret), K(tenant_id_));
  } else {
    //int64_t hash = nullptr != dfc_ ? reinterpret_cast<int64_t>(dfc_) : id_;
    // int64_t hash = reinterpret_cast<int64_t>(this);
    // int64_t hash = GETTID();
    // 通过duplicate_join_table sysbench压测，发现channel申请buffer时
    // 如果采用channel id、线程id(GETTID())，或者dfc_和channel内存地址，都无法很好散列
    // 而采用random则可以很好散列
    buf = tenant_mem_mgr->alloc(hash_val_, payload_size);
    if (nullptr != buf) {
      alloc_buffer_count();
    }
  }
  return buf;
}

void ObDtlBasicChannel::free_buf(ObDtlLinkedBuffer *buf)
{
  int ret = OB_SUCCESS;
  ObDtlTenantMemManager *tenant_mem_mgr = DTL.get_dfc_server().get_tenant_mem_manager(tenant_id_);

  if (OB_NOT_NULL(buf)
      && buf->is_bcast()
      && belong_to_transmit_data()
      && OB_NOT_NULL(bc_service_)) {
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
  // 理论上只有1个
  if (nullptr != process_buffer_ && process_buffer_->is_bcast()) {
    done = true;
    process_buffer_ = nullptr;
  }
  if (!done) {
    ObLink *link = nullptr;
    if (OB_SUCC(recv_list_.top(link))) {
      ObDtlLinkedBuffer *linked_buffer = static_cast<ObDtlLinkedBuffer *>(link);
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
    // 为了清理上一次的内部write buffer
    inc_msg_seq_no();
    write_buffer_->size() = write_buffer_->pos();
    write_buffer_->seq_no() = get_msg_seq_no();
    write_buffer_->tenant_id() = tenant_id_;
    if (write_buffer_->is_data_msg()) {
      write_buffer_->set_use_interm_result(use_interm_result_);
      write_buffer_->set_enable_channel_sync(enable_channel_sync_);
      if (OB_FAIL(write_buffer_->push_batch_id(batch_id_, msg_writer_->rows()))) {
        LOG_WARN("push batch id failed", K(ret));
      }
    }
    msg_writer_->reset();
    write_buffer_->pos() = 0;
    LOG_TRACE("push message to send list", K(write_buffer_->seq_no()), K(ret),
      KP(id_), KP(peer_id_), K(write_buffer_->tenant_id()), K(is_data_msg_),
      K(write_buffer_->size()), K(write_buffer_->pos()), K(get_send_buffer_cnt()),
      K(write_buffer_->is_data_msg()), K(write_buffer_->is_eof()), K(get_msg_seq_no()),
      K(write_buffer_->msg_type()), K(get_channel_type()));
    write_buffer_ = nullptr;
  }
  return ret;
}

int ObDtlBasicChannel::switch_writer(const ObDtlMsg &msg)
{
  int ret = OB_SUCCESS;
  switch_msg_type(msg);
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
        msg_writer_ = &vector_msg_writer_;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unkown msg writer", K(msg.get_type()),
          K(px_row.get_data_type()), K(msg_writer_->type()), K(ret));
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
      const ObPxNewRow &px_row = static_cast<const ObPxNewRow&>(msg);
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

int ObDtlBasicChannel::inner_write_msg(const ObDtlMsg &msg, int64_t timeout_ts,
    ObEvalCtx *eval_ctx, bool is_eof)
{
  int ret = OB_SUCCESS;
  int64_t need_size = 0;
  bool need_new = false;
  if (OB_FAIL(switch_writer(msg))) {
    LOG_WARN("fail to switch writer", K(ret));
  } else if (is_eof && OB_FAIL(msg_writer_->handle_eof())) {
    LOG_WARN("fail to handle eof", K(ret));
  } else if (OB_FAIL(msg_writer_->need_new_buffer(msg, eval_ctx, need_size, need_new))) {
    LOG_WARN("failed to judge need new buffer", K(ret));
  } else if (OB_UNLIKELY(need_new)) {
    if (OB_FAIL(switch_buffer(need_size, is_eof, timeout_ts,
                              eval_ctx))) {
      LOG_WARN("failed to switch buffer", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(msg_writer_->write(msg, eval_ctx, is_eof))) {
      if (OB_BUF_NOT_ENOUGH != ret) {
        LOG_WARN("failed to write msg", K(ret), K(is_eof));
      }
    } else {
      LOG_DEBUG("trace msg write", K(is_eof), K(is_data_msg_), K(msg.get_type()),
        K(write_buffer_->msg_type()), K(need_new));
    }
  }
  return ret;
}

int ObDtlBasicChannel::write_msg(const ObDtlMsg &msg, int64_t timeout_ts,
    ObEvalCtx *eval_ctx, bool is_eof)
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

int ObDtlBasicChannel::switch_buffer(const int64_t min_size, const bool is_eof,
    const int64_t timeout_ts, ObEvalCtx *eval_ctx)
{
  int ret = OB_SUCCESS;
  if (write_buffer_ != nullptr && write_buffer_->pos() != 0) {
    if (CHUNK_DATUM_WRITER != msg_writer_->type()
        && OB_FAIL(msg_writer_->serialize())) {
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
        write_buffer_->set_sqc_id(dfc_->get_sender_sqc_info().sqc_id_);
        write_buffer_->set_dfo_id(dfc_->get_sender_sqc_info().dfo_id_);
      }
      if (OB_NOT_NULL(eval_ctx)) {
        write_buffer_->set_dop(ObPxSqcUtil::get_actual_worker_count(&eval_ctx->exec_ctx_));
        write_buffer_->set_plan_id(ObPxSqcUtil::get_plan_id(&eval_ctx->exec_ctx_));
        write_buffer_->set_exec_id(ObPxSqcUtil::get_exec_id(&eval_ctx->exec_ctx_));
        write_buffer_->set_session_id(ObPxSqcUtil::get_session_id(&eval_ctx->exec_ctx_));
        ObPhysicalPlanCtx *plan_ctx = eval_ctx->exec_ctx_.get_physical_plan_ctx();
        if (OB_NOT_NULL(plan_ctx) && OB_NOT_NULL(plan_ctx->get_phy_plan())) {
          write_buffer_->set_sql_id(plan_ctx->get_phy_plan()->get_sql_id());
          write_buffer_->set_disable_auto_mem_mgr(plan_ctx->get_phy_plan()->
                                                is_disable_auto_memory_mgr());
        }
        ObSQLSessionInfo *sql_session = eval_ctx->exec_ctx_.get_my_session();
        if (OB_NOT_NULL(sql_session)) {
          write_buffer_->set_database_id(sql_session->get_database_id());
        }
        write_buffer_->set_op_id(get_op_id());
        ObOperatorKit *kit = eval_ctx->exec_ctx_.get_operator_kit(get_op_id());
        if (OB_NOT_NULL(kit) && OB_NOT_NULL(kit->op_)) {
          write_buffer_->set_input_rows(kit->op_->get_spec().rows_);
          write_buffer_->set_input_width(kit->op_->get_spec().width_);
        }
      }
      write_buffer_->timeout_ts() = timeout_ts;
      msg_writer_->write_msg_type(write_buffer_);
      write_buffer_->set_data_msg(is_data_msg_);
      write_buffer_->is_eof() = is_eof;
      if (is_data_msg_ && register_dm_info_.is_valid()) {
        write_buffer_->set_register_dm_info(register_dm_info_);
      }
      LOG_TRACE("trace new buffer", K(is_data_msg_), K(is_eof), KP(id_), KP(peer_id_));
    }
  }
  return ret;
}

int ObDtlBasicChannel::send_buffer(ObDtlLinkedBuffer *&buffer)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(send_list_.push(buffer))) {
    LOG_WARN("failed to push back send list", K(ret));
  } else {
    inc_msg_seq_no();
    buffer->size() = buffer->pos();
    buffer->seq_no() = get_msg_seq_no();
    buffer->tenant_id() = tenant_id_;
    if (buffer->is_data_msg()) {
      buffer->set_use_interm_result(use_interm_result_);
      buffer->set_enable_channel_sync(enable_channel_sync_);
      if (buffer->is_batch_info_valid() && OB_ISNULL(msg_writer_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("msg_writer_ is null", K(ret));
      } else {
        const int64_t row_cnt = buffer->is_batch_info_valid() ? msg_writer_->rows() : 0;
        if (OB_FAIL(buffer->push_batch_id(batch_id_,row_cnt))) {
          LOG_WARN("push batch id failed", K(ret));
        }
      }
    }
    buffer->pos() = 0;
    LOG_TRACE("push buffer to send list", K(buffer->seq_no()), K(ret),
      KP(id_), KP(peer_id_), K(buffer->tenant_id()), K(is_data_msg_),
      K(buffer->size()), K(buffer->pos()), K(get_send_buffer_cnt()),
      K(buffer->is_data_msg()), K(buffer->is_eof()), K(get_msg_seq_no()));
    buffer = nullptr;
  }

  if (OB_SUCC(ret) && !send_list_.is_empty()) {
    if (OB_FAIL(flush(false))) {
      LOG_WARN("flush fail. ignore. may retry flush again very soon", K(ret));
    }
  }

  return ret;
}

int ObDtlBasicChannel::push_buffer_batch_info()
{
  int ret = OB_SUCCESS;
  if (NULL == write_buffer_) {
    // means last buffer has been flushed, do nothing.
  } else if (OB_ISNULL(msg_writer_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("msg_writer_ is null", K(ret));
  } else if (OB_FAIL(msg_writer_->handle_eof())) {
    LOG_WARN("write buffer handle eof failed", K(ret));
  } else if (OB_FAIL(write_buffer_->add_batch_info(batch_id_, msg_writer_->rows()))) {
    LOG_WARN("linked buffer push batch failed", K(ret));
  }
  return ret;
}

void ObDtlBasicChannel::switch_msg_type(const ObDtlMsg &msg)
{
  if (msg.is_data_msg()
      && static_cast<const ObPxNewRow &> (msg).get_data_type() == PX_VECTOR
       && DtlChannelType::LOCAL_CHANNEL == get_channel_type()) {
    static_cast<ObPxNewRow &> (const_cast<ObDtlMsg &> (msg)).set_data_type(PX_VECTOR_ROW);
  }
}

//----ObDtlRowMsgWriter
ObDtlRowMsgWriter::ObDtlRowMsgWriter() :
  type_(CHUNK_ROW_WRITER), row_store_(), block_(nullptr), write_buffer_(nullptr)
{}

ObDtlRowMsgWriter::~ObDtlRowMsgWriter()
{
  reset();
}

int ObDtlRowMsgWriter::init(ObDtlLinkedBuffer *buffer, uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (nullptr == buffer) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("write buffer is null", K(ret));
  } else {
    reset();
    row_store_.init(0, tenant_id, common::ObCtxIds::DEFAULT_CTX_ID, "SqlDtlRowMsg");
    if (OB_FAIL(row_store_.init_block_buffer(static_cast<void *>(buffer->buf()), buffer->size(), block_))) {
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

int ObDtlRowMsgWriter::need_new_buffer(
  const ObDtlMsg &msg, ObEvalCtx *ctx, int64_t &need_size, bool &need_new)
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  int64_t serialize_need_size = 0;
  const ObPxNewRow &px_row = static_cast<const ObPxNewRow&>(msg);
  const ObNewRow *row = px_row.get_row();
  if (nullptr == row) {
    serialize_need_size = ObChunkRowStore::Block::min_buf_size(0);
    need_size = serialize_need_size;
  } else {
    serialize_need_size = ObChunkRowStore::Block::row_store_size(*row);
    need_size = ObChunkRowStore::Block::min_buf_size(serialize_need_size);
  }
  need_new = nullptr == write_buffer_ || (remain() < serialize_need_size);
  if(need_new && nullptr != write_buffer_) {
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
ObDtlDatumMsgWriter::ObDtlDatumMsgWriter() :
  type_(CHUNK_DATUM_WRITER), write_buffer_(nullptr), block_(nullptr),
  register_block_ptr_(NULL), register_block_buf_ptr_(NULL), write_ret_(OB_SUCCESS)
{}

ObDtlDatumMsgWriter::~ObDtlDatumMsgWriter()
{
  reset();
}

int ObDtlDatumMsgWriter::init(ObDtlLinkedBuffer *buffer, uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  UNUSED(tenant_id);
  if (nullptr == buffer) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("write buffer is null", K(ret));
  } else {
    reset();
    if (OB_FAIL(ObChunkDatumStore::init_block_buffer(
        static_cast<void *>(buffer->buf()), buffer->size(), block_))) {
      LOG_WARN("init shrink buffer failed", K(ret));
    } else {
      write_buffer_ = buffer;
      if (NULL != register_block_ptr_) {
        *register_block_ptr_ = block_;
      }
      if (NULL != register_block_buf_ptr_) {
        register_block_buf_ptr_->set_block(block_);
        register_block_buf_ptr_->set_data_size(block_->data_size());
        register_block_buf_ptr_->set_capacity(block_->blk_size_);
      }
    }
  }
  return ret;
}

int ObDtlDatumMsgWriter::need_new_buffer(
  const ObDtlMsg &msg, ObEvalCtx *ctx, int64_t &need_size, bool &need_new)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(OB_BUF_NOT_ENOUGH != write_ret_ && nullptr != write_buffer_)) {
    need_new = false;
  } else {
    int64_t serialize_need_size = 0;
    const ObPxNewRow &px_row = static_cast<const ObPxNewRow&>(msg);
    const ObIArray<ObExpr *> *row = px_row.get_exprs();
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
    if(need_new && nullptr != write_buffer_) {
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
  if (NULL != register_block_buf_ptr_) {
    register_block_buf_ptr_->reset();
  }
  if (NULL != register_block_ptr_) {
    *register_block_ptr_ = NULL;
  }
}

int ObDtlDatumMsgWriter::serialize()
{
  return block_->unswizzling();
}
//--------------end ObDtlDatumMsgWriter---------------


//-----------------start ObDtlVectorRowMsgWrite-------------
ObDtlVectorRowMsgWriter::ObDtlVectorRowMsgWriter() :
  type_(VECTOR_ROW_WRITER), write_buffer_(nullptr), block_(nullptr),
  block_buffer_(nullptr), row_meta_(), row_cnt_(0), write_ret_(OB_SUCCESS)
{}

ObDtlVectorRowMsgWriter::~ObDtlVectorRowMsgWriter()
{
  reset();
}

int ObDtlVectorRowMsgWriter::init(ObDtlLinkedBuffer *buffer, uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  UNUSED(tenant_id);
  if (nullptr == buffer) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("write buffer is null", K(ret));
  } else {
    reset();
    ObTempBlockStore::Block *blk = NULL;
    if (OB_FAIL(ObTempBlockStore::init_dtl_block_buffer(buffer->buf(), buffer->size(), blk))) {
      LOG_WARN("fail to init block buffer", K(ret));
    } else {
      block_ = static_cast<ObTempRowStore::DtlRowBlock *>(blk);
      block_buffer_ = static_cast<ObTempBlockStore::ShrinkBuffer *>(
        static_cast<void *>(reinterpret_cast<char *>(blk) + blk->buf_off_));
      write_buffer_ = buffer;
    }
  }
  return ret;
}

int ObDtlVectorRowMsgWriter::need_new_buffer(
  const ObDtlMsg &msg, ObEvalCtx *ctx, int64_t &need_size, bool &need_new)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(OB_BUF_NOT_ENOUGH != write_ret_ && nullptr != write_buffer_)) {
    need_new = false;
  } else {
    int64_t serialize_need_size = 0;
    const ObPxNewRow &px_row = static_cast<const ObPxNewRow&>(msg);
    const ObIArray<ObExpr *> *row = px_row.get_exprs();
    if (nullptr == row) {
      serialize_need_size = ObTempRowStore::Block::min_blk_size<true>(0);
      need_size = serialize_need_size;
    } else {
      if (OB_FAIL(ObTempRowStore::RowBlock::calc_row_size(*row, row_meta_, *ctx, serialize_need_size))) {
        LOG_WARN("failed to calc row store size", K(ret));
      }
      need_size = ObTempRowStore::Block::min_blk_size<true>(serialize_need_size);
    }
    need_new = nullptr == write_buffer_ || (remain() < serialize_need_size);
    if(need_new && nullptr != write_buffer_) {
      write_buffer_->pos() = rows() > 0 ? used() : 0;
    }
  }
  write_ret_ = OB_SUCCESS;
  return ret;
}

void ObDtlVectorRowMsgWriter::reset()
{
  block_ = nullptr;
  write_buffer_ = nullptr;
  row_cnt_ = 0;
  block_buffer_ = nullptr;
  row_meta_.reset();
}

int ObDtlVectorRowMsgWriter::serialize()
{
  return OB_SUCCESS;
}
//--------------end ObDtlVectorRowMsgWriter---------------

//--------------start ObDtlVectorsMsgWriter---------------
ObDtlVectorMsgWriter::ObDtlVectorMsgWriter() :
  type_(VECTOR_WRITER), write_buffer_(nullptr), block_(nullptr),
  block_buffer_(nullptr), write_ret_(OB_SUCCESS)
{}

ObDtlVectorMsgWriter::~ObDtlVectorMsgWriter()
{
  reset();
}

int ObDtlVectorMsgWriter::init(ObDtlLinkedBuffer *buffer, uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  UNUSED(tenant_id);
  if (nullptr == buffer) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("write buffer is null", K(ret));
  } else {
    reset();
    if (OB_FAIL(ObDtlVectorsBuffer::init_vector_buffer(
        static_cast<void *>(buffer->buf()), buffer->size(), block_))) {
      LOG_WARN("init shrink buffer failed", K(ret));
    } else {
      block_buffer_ = block_->get_buffer();
      write_buffer_ = buffer;
    }
  }
  return ret;
}

int ObDtlVectorMsgWriter::need_new_buffer(
  const ObDtlMsg &msg, ObEvalCtx *ctx, int64_t &need_size, bool &need_new)
{
  int ret = OB_SUCCESS;
  need_size = 0;
  if (OB_LIKELY(OB_BUF_NOT_ENOUGH != write_ret_ && nullptr != write_buffer_)) {
    need_new = false;
  } else {
    int64_t serialize_need_size = 0;
    const ObPxNewRow &px_row = static_cast<const ObPxNewRow&>(msg);
    const ObIArray<ObExpr *> *row = px_row.get_exprs();
    if (nullptr == write_buffer_) {
      need_new = true;
      ObDtlVectorsBuffer::calc_new_buffer_size(row, ctx->get_batch_idx(), *ctx, need_size);
    } else if (nullptr == row) {
      serialize_need_size = ObDtlVectorsBuffer::min_buf_size();
      need_size = serialize_need_size;
      if (block_buffer_->remain() < serialize_need_size) {
        need_new = true;
      }
    } else if (!block_buffer_->can_append_row(*row, ctx->get_batch_idx(), *ctx, need_size)) {
      need_new = true;
    }
    if(need_new && nullptr != write_buffer_) {
      write_buffer_->pos() = rows() > 0 ? used() : 0;
    }
  }
  write_ret_ = OB_SUCCESS;
  return ret;
}

void ObDtlVectorMsgWriter::reset()
{
  block_ = nullptr;
  block_buffer_ = nullptr;
  write_buffer_ = nullptr;
}

int ObDtlVectorMsgWriter::serialize()
{
  return OB_SUCCESS;
}
//--------------end ObDtlVectorsMsgWriter---------------

//--------------end ObDtlVectorsFixedMsgWriter---------------
ObDtlVectorFixedMsgWriter::ObDtlVectorFixedMsgWriter() :
  type_(VECTOR_FIXED_WRITER), write_buffer_(nullptr), vector_buffer_(),
  write_ret_(OB_SUCCESS)
{}

ObDtlVectorFixedMsgWriter::~ObDtlVectorFixedMsgWriter()
{
  reset();
}

int ObDtlVectorFixedMsgWriter::init(ObDtlLinkedBuffer *buffer, uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  UNUSED(tenant_id);
  if (nullptr == buffer) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("write buffer is null", K(ret));
  } else {
    reset();
    vector_buffer_.set_buf(buffer->buf(), buffer->size());
    write_buffer_ = buffer;
  }
  return ret;
}

int ObDtlVectorFixedMsgWriter::need_new_buffer(
  const ObDtlMsg &msg, ObEvalCtx *ctx, int64_t &need_size, bool &need_new)
{
  int ret = OB_SUCCESS;
  need_size = 0;
  if (OB_LIKELY(OB_BUF_NOT_ENOUGH != write_ret_ && nullptr != write_buffer_)) {
    need_new = false;
  } else {
    int64_t serialize_need_size = 0;
    const ObPxNewRow &px_row = static_cast<const ObPxNewRow&>(msg);
    const ObIArray<ObExpr *> *row = px_row.get_exprs();
    if (nullptr == write_buffer_) {
      need_new = true;
    } else if (nullptr == row) {
      serialize_need_size = ObDtlVectors::min_buf_size();
      need_size = serialize_need_size;
      need_new = false;
    } else if (vector_buffer_.get_row_cnt() >= vector_buffer_.get_row_limit()) {
      need_new = true;
    }
    if(need_new && nullptr != write_buffer_) {
      write_buffer_->pos() = rows() > 0 ? vector_buffer_.get_mem_used() : 0;
    }
  }
  write_ret_ = OB_SUCCESS;
  return ret;
}

void ObDtlVectorFixedMsgWriter::reset()
{
  vector_buffer_.reset();
  write_buffer_ = nullptr;
}

int ObDtlVectorFixedMsgWriter::serialize()
{
  return OB_SUCCESS;
}

//--------------end ObDtlVectorsFixedMsgWriter---------------

//----------------start ObDtlControlMsgWriter----------
int ObDtlControlMsgWriter::write(const ObDtlMsg &msg, ObEvalCtx *eval_ctx, const bool is_eof)
{
  int ret = OB_SUCCESS;
  UNUSED(eval_ctx);
  ObDtlMsgHeader header;
  header.nbody_ = static_cast<int32_t>(msg.get_serialize_size());
  header.type_ = static_cast<int16_t>(msg.get_type());
  auto buf = write_buffer_->buf();
  auto size = write_buffer_->size();
  auto &pos = write_buffer_->pos();
  write_buffer_->set_data_msg(false);
  write_buffer_->is_eof() = is_eof;
  if (OB_FAIL(serialization::encode(buf, size, pos, header))) {
    LOG_WARN(
        "serialize RPC channel message type fail",
        K(size), K(pos), K(ret));
  } else if (OB_FAIL(serialization::encode(buf, size, pos, msg))) {
    LOG_WARN(
        "serialize RPC channel message fail",
        K(size), K(pos), K(ret));
  }
  return ret;
}

//----------------start ObDtlControlMsgWriter----------

}  // dtl
}  // sql
}  // oceanbase
