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
#include "sql/engine/px/datahub/components/ob_dh_init_channel.h"

using namespace oceanbase::common;

namespace oceanbase {
namespace sql {
namespace dtl {

ObDtlLocalChannel::ObDtlLocalChannel(
    const uint64_t tenant_id,
    const uint64_t id,
    const ObAddr &peer)
    : ObDtlBasicChannel(tenant_id, id, peer)
{}

ObDtlLocalChannel::ObDtlLocalChannel(
    const uint64_t tenant_id,
    const uint64_t id,
    const ObAddr &peer,
    const int64_t hash_val)
    : ObDtlBasicChannel(tenant_id, id, peer, hash_val)
{}

ObDtlLocalChannel::~ObDtlLocalChannel()
{
  destroy();
  LOG_TRACE("dtl use time", K(times_), K(write_buf_use_time_), K(send_use_time_),
    KP(id_), KP(peer_id_));
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
{
}

// 共享内存方式
int ObDtlLocalChannel::feedup(ObDtlLinkedBuffer *&linked_buffer)
{
  return attach(linked_buffer);
}

// 每一条return路径都必须设置on_finish否则后续会卡死
int ObDtlLocalChannel::send_shared_message(ObDtlLinkedBuffer *&buf)
{
  int ret = OB_SUCCESS;
  bool is_block = false;
  ObDtlChannel *chan = nullptr;
  bool is_first = false;
  bool is_eof = false;
  if (nullptr == buf) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sended buffer is null", KP(id_), KP(peer_id_), K(ret));
  } else {
    is_first = buf->is_data_msg() && 1 == buf->seq_no();
    is_eof = buf->is_eof();
    if (buf->is_data_msg() && buf->use_interm_result()) {
      MTL_SWITCH(buf->tenant_id()) {
        if (OB_FAIL(MTL(ObDTLIntermResultManager*)->process_interm_result(buf, peer_id_))) {
          LOG_WARN("fail to process internal result", K(ret));
        }
      }
    } else if (OB_FAIL(DTL.get_channel(peer_id_, chan))) {
      int tmp_ret = ret;
      // cache版本升级不需要处理，数据发送到receive端大致几种情况：
      // 1) new|old -> old 无需处理，不涉及到cache问题
      // 2) old -> new，is_data_msg 为false，和老逻辑一致
      // 3) new -> new, is_data_msg可能为true，则new版本之间采用blocking逻辑，不影响其他
      // only buffer data msg
      ObDtlMsgHeader header;
      const bool keep_pos = true;
      if (!buf->is_data_msg() && OB_FAIL(ObDtlLinkedBuffer::deserialize_msg_header(*buf, header, keep_pos))) {
        LOG_WARN("failed to deserialize msg header", K(ret));
      } else if (header.is_drain()) {
        LOG_TRACE("receive drain cmd, unregister local channel", KP(peer_id_));
        ret = OB_SUCCESS;
        tmp_ret = OB_SUCCESS;
      } else if (buf->is_data_msg() && 1 == buf->seq_no()) {
        if (!ObInitChannelPieceMsgCtx::enable_dh_channel_sync(buf->enable_channel_sync())) {
          LOG_TRACE("first msg blocked", K(is_block), KP(peer_id_), K(ret),
              K(buf->tenant_id()), K(buf->seq_no()), K(buf->is_data_msg()), KP(buf),
              K(buf->is_eof()));
          bool is_eof = buf->is_eof();
          if (OB_FAIL(DTL.get_dfc_server().cache(/*buf->tenant_id(), */peer_id_, buf, true))) {
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
          ret = tmp_ret;
          LOG_WARN("failed to get channel", K(ret));
        }
      } else {
        LOG_TRACE("get DTL channel fail", K(buf->seq_no()), KP(peer_id_), "peer", get_peer(), K(ret), K(tmp_ret), K(buf->is_data_msg()));
      }
    } else {
      ObDtlLocalChannel *local_chan = reinterpret_cast<ObDtlLocalChannel*>(chan);
      if (OB_FAIL(local_chan->feedup(buf))) {
        LOG_WARN("feed up DTL channel fail", KP(peer_id_), "peer", get_peer(), K(ret));
      } else if (OB_ISNULL(local_chan->get_dfc())) {
        LOG_TRACE("dfc of rpc channel is null", K(msg_response_.is_block()), KP(peer_id_), K(ret), KP(local_chan->get_id()), K(local_chan->get_peer()));
      } else if (local_chan->belong_to_receive_data()) {
        // 必须是receive端，才可以主动回包让transmit端block
        is_block = local_chan->get_dfc()->is_block(local_chan);
        LOG_TRACE("need blocking", K(msg_response_.is_block()), KP(peer_id_), K(ret), KP(local_chan->get_id()),
          K(local_chan->get_peer()), K(local_chan->belong_to_receive_data()), K(local_chan->get_processed_buffer_cnt()), K(local_chan->get_recv_buffer_cnt()));
      }
      DTL.release_channel(local_chan);
    }
    if (nullptr == buf) {
      // 成功attach后，认为申请成功了，由于申请动作由自己申请，但释放权交给了另外一个channel，所以这里假设也是自己申请的，方便统计申请与释放一致
      free_buffer_count();
    }
  }
  if (OB_SUCC(ret)) {
    if (is_first) {
      metric_.mark_first_out();
    }
    if (is_eof) {
      metric_.mark_eof();
    }
    metric_.set_last_out_ts(::oceanbase::common::ObTimeUtility::current_time());
  }
  //统一返回消息
  msg_response_.on_finish(is_block, ret);
  return ret;
}

int ObDtlLocalChannel::send_message(ObDtlLinkedBuffer *&buf)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    if (OB_FAIL(wait_response())) {
      LOG_WARN("failed to wait response", K(ret));
    }
    if (OB_SUCC(ret) && OB_FAIL(wait_unblocking_if_blocked())) {
      LOG_WARN("failed to block data flow", K(ret));
    }
  }
  LOG_TRACE("local channel send message", KP(get_id()), K_(peer), K(ret),
    K(get_send_buffer_cnt()), K(get_msg_seq_no()));

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
  // it may return 4201 after send_message and it don't call wait_response
  if (OB_HASH_NOT_EXIST == ret) {
    if (is_drain()) {
      ret = OB_SUCCESS;
    } else {
      ret = OB_ERR_SIGNALED_IN_PARALLEL_QUERY_SERVER;
    }
  }
  return ret;
}

}  // dtl
}  // sql
}  // oceanbase
