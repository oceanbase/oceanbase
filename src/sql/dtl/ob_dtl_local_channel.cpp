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

#include "ob_dtl_local_channel.h"
#include "lib/oblog/ob_log_module.h"
#include "sql/dtl/ob_dtl_flow_control.h"
#include "sql/engine/basic/ob_chunk_row_store.h"
#include "ob_dtl_interm_result_manager.h"

using namespace oceanbase::common;

namespace oceanbase {
namespace sql {
namespace dtl {

ObDtlLocalChannel::ObDtlLocalChannel(const uint64_t tenant_id, const uint64_t id, const ObAddr& peer)
    : ObDtlBasicChannel(tenant_id, id, peer)
{}

ObDtlLocalChannel::~ObDtlLocalChannel()
{
  destroy();
  LOG_TRACE("dtl use time", K(times_), K(write_buf_use_time_), K(send_use_time_));
}

int ObDtlLocalChannel::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDtlBasicChannel::init())) {
    LOG_WARN("Initialize fifo allocator fail", K(ret));
  }
  return ret;
}

void ObDtlLocalChannel::destroy()
{}

int ObDtlLocalChannel::feedup(ObDtlLinkedBuffer*& linked_buffer)
{
  return attach(linked_buffer);
}

// every return path needs to have on_finish
int ObDtlLocalChannel::send_shared_message(ObDtlLinkedBuffer*& buf)
{
  int ret = OB_SUCCESS;
  bool is_block = false;
  ObDtlChannel* chan = nullptr;
  bool is_first = false;
  bool is_eof = false;
  if (nullptr == buf) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sended buffer is null", KP(id_), KP(peer_id_), K(ret));
  } else {
    is_first = buf->is_data_msg() && 1 == buf->seq_no();
    is_eof = buf->is_eof();
    if (OB_FAIL(DTL.get_channel(peer_id_, chan))) {
      int tmp_ret = ret;
      // cache handling during mixed version runnin scenario during online upgrade
      // 1) new|old -> old: no handling needed
      // 2) old -> new: is_data_msg is false, same with old logic
      // 3) new -> new: is_data_msg might be true, codes between new nodes share the same logic
      // only buffer data msg
      ObDtlMsgHeader header;
      const bool keep_pos = true;
      if (!buf->is_data_msg() && OB_FAIL(ObDtlLinkedBuffer::deserialize_msg_header(*buf, header, keep_pos))) {
        LOG_WARN("failed to deserialize msg header", K(ret));
      } else if (header.is_drain()) {
        LOG_TRACE("receive drain cmd, unregister local channel", KP(peer_id_));
        ret = OB_SUCCESS;
        tmp_ret = OB_SUCCESS;
      } else if (buf->is_data_msg() && buf->use_interm_result()) {
        ret = OB_SUCCESS;
        if (OB_FAIL(process_interm_result(buf))) {
          LOG_WARN("fail to process internal result", K(ret));
        }
      } else if (buf->is_data_msg() && 1 == buf->seq_no()) {
        LOG_TRACE("first msg blocked",
            K(is_block),
            KP(peer_id_),
            K(ret),
            K(buf->tenant_id()),
            K(buf->seq_no()),
            K(buf->is_data_msg()),
            KP(buf));
        bool is_eof = buf->is_eof();
        if (OB_FAIL(DTL.get_dfc_server().cache(/*buf->tenant_id(), */ peer_id_, buf, true))) {
          ret = tmp_ret;
          LOG_WARN("get DTL channel fail", KP(peer_id_), "peer", get_peer(), K(ret), K(tmp_ret));
        } else {
          // return block after cache first msg
          is_block = !is_eof;
          if (nullptr != buf) {
            free_buf(buf);
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("expect buffer is null", KP(peer_id_), "peer", get_peer(), K(ret), K(tmp_ret));
          }
          buf = nullptr;
        }
      } else {
        LOG_TRACE("get DTL channel fail",
            K(buf->seq_no()),
            KP(peer_id_),
            "peer",
            get_peer(),
            K(ret),
            K(tmp_ret),
            K(buf->is_data_msg()));
      }
    } else {
      ObDtlLocalChannel* local_chan = reinterpret_cast<ObDtlLocalChannel*>(chan);
      if (OB_FAIL(local_chan->feedup(buf))) {
        LOG_WARN("feed up DTL channel fail", KP(peer_id_), "peer", get_peer(), K(ret));
      } else if (OB_ISNULL(local_chan->get_dfc())) {
        LOG_TRACE("dfc of rpc channel is null",
            K(msg_response_.is_block()),
            KP(peer_id_),
            K(ret),
            KP(local_chan->get_id()),
            K(local_chan->get_peer()));
      } else if (local_chan->belong_to_receive_data()) {
        is_block = local_chan->get_dfc()->is_block(local_chan);
        LOG_TRACE("need blocking",
            K(msg_response_.is_block()),
            KP(peer_id_),
            K(ret),
            KP(local_chan->get_id()),
            K(local_chan->get_peer()),
            K(local_chan->belong_to_receive_data()),
            K(local_chan->get_processed_buffer_cnt()),
            K(local_chan->get_recv_buffer_cnt()));
      }
      DTL.release_channel(local_chan);
    }
    if (nullptr == buf) {
      free_buffer_count();
    }
  }
  if (OB_SUCC(ret)) {
    if (is_first) {
      metric_.mark_first_out();
    }
    if (is_eof) {
      metric_.mark_last_out();
    }
  }
  msg_response_.on_finish(is_block, ret);
  return ret;
}

int ObDtlLocalChannel::send_message(ObDtlLinkedBuffer*& buf)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    if (msg_response_.is_in_process()) {
      if (OB_FAIL(msg_response_.wait())) {
        LOG_WARN("send previous message fail", K(ret));
      } else if (OB_HASH_NOT_EXIST == ret) {
        if (is_drain()) {
          ret = OB_SUCCESS;
        } else {
          ret = OB_ERR_SIGNALED_IN_PARALLEL_QUERY_SERVER;
        }
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(wait_unblocking_if_blocked())) {
      LOG_WARN("failed to block data flow", K(ret));
    }
  }
  LOG_TRACE("local channel send message", KP(get_id()), K_(peer), K(ret), K(get_send_buffer_cnt()));

  if (OB_SUCC(ret) && (!is_drain() || buf->is_eof())) {
    // The peer may not setup when the first message arrive,
    // we wait first message return and retry until peer setup.
    bool is_eof = buf->is_eof();
    if (OB_FAIL(msg_response_.start())) {
      LOG_WARN("start message process fail", K(ret));
    } else if (OB_FAIL(send_shared_message(buf))) {
      // 1) for data message, if dtl channel is not built, it's cached by first buffer manage,
      //    it's processed rightly, or it's drain
      //    so don't wait first response
      // 2) control message SQC and QC channel also must be linked
      // 3) bloom filter message rpc processor process, don't need channel
      // so channel is linked and don't retry
      LOG_DEBUG("Data channel linked", K_(peer), K(ret), KP(peer_id_));
    }
    LOG_TRACE("local channel status", K_(peer), K(ret), KP(peer_id_));
    if (is_eof) {
      set_eof();
    }
  }
  return ret;
}

int ObDtlLocalChannel::process_interm_result(ObDtlLinkedBuffer* buffer)
{
  int ret = OB_SUCCESS;
  int64_t channel_id = peer_id_;
  bool need_free = false;
  ObDTLIntermResultKey key;
  ObDTLIntermResultInfo result_info;
  key.channel_id_ = channel_id;
  if (OB_ISNULL(buffer)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to process buffer", K(ret));
  } else if (FALSE_IT(key.time_us_ = buffer->timeout_ts())) {
  } else if (OB_FAIL(ObDTLIntermResultManager::getInstance().get_interm_result_info(key, result_info))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ObMemAttr attr(buffer->tenant_id(), "DtlIntermRes", common::ObCtxIds::WORK_AREA);
      key.start_time_ = oceanbase::common::ObTimeUtility::current_time();
      ret = OB_SUCCESS;
      result_info.is_datum_ = PX_DATUM_ROW == buffer->msg_type();
      if (OB_FAIL(ObDTLIntermResultManager::getInstance().create_interm_result_info(attr, result_info))) {
        LOG_WARN("fail to create chunk row store", K(ret));
      } else if (OB_FAIL(DTL_IR_STORE_DO(
                     result_info, init, 0, buffer->tenant_id(), common::ObCtxIds::WORK_AREA, "DtlIntermRes"))) {
        LOG_WARN("fail to init buffer", K(ret));
      } else if (OB_FAIL(ObDTLIntermResultManager::getInstance().insert_interm_result_info(key, result_info))) {
        LOG_WARN("fail to insert row store", K(ret));
      }
      need_free = true;
    }
  }
  if (OB_SUCC(ret)) {
    if (!result_info.is_store_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("fail to get row store", K(ret));
    } else {
      ObAtomicAppendBlockCall call(buffer->buf(), buffer->size());
      if (OB_FAIL(ObDTLIntermResultManager::getInstance().atomic_append_block(key, call))) {
        LOG_WARN("fail to append block", K(ret));
      } else {
        ret = call.ret_;
        if (OB_SUCCESS != ret) {
          LOG_WARN("fail to append block", K(ret));
        }
      }
    }
  }

  if (OB_FAIL(ret) && need_free) {
    int tmp_ret = OB_SUCCESS;
    // there won't be a concurrency issue here as it is a point to point serial transmition within
    // a channel
    if (OB_SUCCESS != (tmp_ret = ObDTLIntermResultManager::getInstance().erase_interm_result_info(key))) {
      ObDTLIntermResultManager::getInstance().free_interm_result_info(result_info);
    } else {
      // in hash table, erase will clean up the memory
    }
  }
  return ret;
}

}  // namespace dtl
}  // namespace sql
}  // namespace oceanbase
