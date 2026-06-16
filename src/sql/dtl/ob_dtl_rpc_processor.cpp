/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_DTL
#include "ob_dtl_rpc_processor.h"
#include "sql/dtl/ob_dtl.h"
#include "sql/dtl/ob_dtl_flow_control.h"
#include "sql/dtl/ob_dtl_rpc_channel.h"
using namespace oceanbase::common;

namespace oceanbase {
namespace sql {
namespace dtl {

static int feedup_unblocking_msg(ObDtlBasicChannel *bc)
{
  static constexpr int64_t MAX_UNBLOCKING_MSG_SIZE = 128;
  static_assert(sizeof(ObDtlUnblockingMsg) + sizeof(ObDtlMsgHeader) <= MAX_UNBLOCKING_MSG_SIZE,
      "unblocking msg is too large");
  int ret = OB_SUCCESS;
  char local_buf[MAX_UNBLOCKING_MSG_SIZE];
  ObDtlRpcChannel *rpc_chan = nullptr;
  ObDtlLinkedBuffer mock_buffer(local_buf, sizeof(local_buf), sizeof(local_buf));
  ObDtlLinkedBuffer *mock_buffer_ptr = &mock_buffer;
  ObDtlUnblockingMsg unblocking_msg;
  ObDtlMsgHeader header;
  header.nbody_ = static_cast<int32_t>(unblocking_msg.get_serialize_size());
  header.type_ = static_cast<int16_t>(unblocking_msg.get_type());
  if (OB_ISNULL(bc)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unexpected null basic channel", K(ret));
  } else if (ObDtlChannel::DtlChannelType::RPC_CHANNEL != bc->get_channel_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected non-rpc channel for unblocking batch msg", K(ret), KPC(bc));
  } else if (FALSE_IT(rpc_chan = static_cast<ObDtlRpcChannel *>(bc))) {
  } else if (OB_FAIL(serialization::encode(
                 mock_buffer.buf(), mock_buffer.size(), mock_buffer.pos(), header))) {
    LOG_WARN("failed to serialize unblocking msg header", K(ret), K(header), KPC(bc));
  } else if (OB_FAIL(serialization::encode(
                 mock_buffer.buf(), mock_buffer.size(), mock_buffer.pos(), unblocking_msg))) {
    LOG_WARN("failed to serialize unblocking msg", K(ret), K(header), KPC(bc));
  } else {
    mock_buffer.set_data_msg(false);
    mock_buffer.set_msg_type(ObDtlMsgType::UNBLOCKING_DATA_FLOW);
    mock_buffer.tenant_id() = bc->get_tenant_id();
    mock_buffer.set_size(mock_buffer.pos());
    mock_buffer.set_pos(0);
    if (OB_FAIL(rpc_chan->feedup(mock_buffer_ptr))) {
      LOG_WARN("failed to feed mock unblocking msg to rpc channel", K(ret), KPC(bc));
    }
  }
  return ret;
}

int ObDtlSendMessageP::process()
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("on dtl rpc process");
  if (OB_FAIL(process_msg(result_, arg_))) {
    LOG_WARN("failed to process msg", K(ret), "peer", get_peer());
  }
  return ret;
}

int ObDtlSendMessageP::process_msg(ObDtlRpcDataResponse &response, ObDtlSendArgs &arg)
{
  int ret = OB_SUCCESS;
  ObDtlChannel *chan = nullptr;
  response.is_block_ = false;
  if (arg.buffer_.is_data_msg() && arg.buffer_.use_interm_result()) {
    MTL_SWITCH(arg.buffer_.tenant_id()) {
      if (OB_FAIL(MTL(ObDTLIntermResultManager*)->process_interm_result(&arg.buffer_, arg.chid_))) {
        LOG_WARN("fail to process internal result", K(ret));
      }
    }
  } else if (OB_FAIL(DTL.get_channel(arg.chid_, chan))) {
    int tmp_ret = ret;
    // cache版本升级不需要处理，数据发送到receive端大致几种情况：
    // 1) new|old -> old 无需处理，不涉及到cache问题
    // 2) old -> new，is_data_msg 为false，和老逻辑一致
    // 3) new -> new, is_data_msg可能为true，则new版本之间采用blocking逻辑，不影响其他
    // only buffer data msg
    ObDtlMsgHeader header;
    const bool keep_pos = true;
    if (!arg.buffer_.is_data_msg() && OB_FAIL(ObDtlLinkedBuffer::deserialize_msg_header(arg.buffer_, header, keep_pos))) {
      LOG_WARN("failed to deserialize msg header", K(ret));
    } else if (header.is_drain()) {
      LOG_TRACE("receive drain cmd, unregister rpc channel", KP(arg.chid_));
      ret = OB_SUCCESS;
      tmp_ret = OB_SUCCESS;
    } else if (arg.buffer_.is_data_msg() && 1 == arg.buffer_.seq_no()) {
      ret = tmp_ret;
      LOG_WARN("failed to get channel", K(ret));
    } else {
      // 太多的get channel fail，用trace
      LOG_TRACE("get DTL channel fail", KP(arg.chid_), K(ret), K(tmp_ret));
    }
  } else {
    // 添加tenant上下文，方便后续如果需要release channel，可以调用MTL_GET拿到dtl manager相关信息
    ObDtlRpcChannel *rpc_chan = reinterpret_cast<ObDtlRpcChannel*>(chan);
    ObDtlLinkedBuffer *buf = &arg.buffer_;
    if (OB_FAIL(rpc_chan->feedup(buf))) {
      LOG_WARN("feed up DTL channel fail", KP(arg.chid_), K(buf), KPC(buf), K(ret));
    } else if (OB_ISNULL(rpc_chan->get_dfc())) {
      LOG_TRACE("dfc of rpc channel is null", K(response.is_block_), KP(arg.chid_), K(ret),
        KP(rpc_chan->get_id()), K(rpc_chan->get_peer()));
    } else if (rpc_chan->belong_to_receive_data()) {
      // 必须是receive端，才可以主动回包让transmit端block
      response.is_block_ = rpc_chan->get_dfc()->is_block(rpc_chan);
      LOG_TRACE("need blocking", K(response.is_block_), KP(arg.chid_), K(ret),
        K(arg.buffer_.seq_no()),
        KP(rpc_chan->get_id()),
        K(rpc_chan->get_peer()),
        K(rpc_chan->belong_to_receive_data()),
        K(rpc_chan->get_processed_buffer_cnt()),
        K(rpc_chan->get_recv_buffer_cnt()));
    }
    DTL.release_channel(rpc_chan);
  }
  response.recode_ = ret;
  return OB_SUCCESS;
}

int ObDtlBCSendMessageP::process()
{
  int ret = OB_SUCCESS;
  ObIArray<ObDtlRpcDataResponse> &resps = result_.resps_;
  ObIArray<ObDtlSendArgs> &args = arg_.args_;
  LOG_TRACE("receive broadcast msg", K(resps), K(args));
  if (args.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected args", K(ret));
  } else if (OB_FAIL(resps.prepare_allocate(args.count()))) {
    LOG_WARN("prepare allocate failed", K(ret));
  } else {
    int tmp_ret = OB_SUCCESS;
    for (int64_t i = 0; i < args.count(); ++i) {
      args.at(i).buffer_.shallow_copy(arg_.bc_buffer_);
      ObDtlSendMessageP::process_msg(resps.at(i), args.at(i));
      tmp_ret = resps.at(i).recode_;
      // it must be data message
      if (OB_HASH_NOT_EXIST == tmp_ret) {
        // this channel has been drained or some error happened in some sqc.
        // we don't care this error, change to success.
        tmp_ret = OB_SUCCESS;
        resps.at(i).recode_ = OB_SUCCESS;
      } else if (tmp_ret != OB_SUCCESS) {
        LOG_WARN("failed to process_msg", K(tmp_ret), K(arg_.bc_buffer_.seq_no()),
          K(resps.count()), K(args.count()), K(i), KP(&arg_.bc_buffer_));
      }
      // record first error
      if (OB_SUCCESS == ret) {
        ret = tmp_ret;
      }
    }
  }
  LOG_TRACE("finish process broadcast msg", K(ret), K(arg_.bc_buffer_), K(resps), K(args));
  // rpc的回包一定要返回OB_SUCCESS，不然response不会被序列化
  return OB_SUCCESS;
}

int ObDtlBatchSendMessageP::process()
{
  int ret = OB_SUCCESS;
  ObIArray<ObDtlSendArgs> &args = arg_.args_;
  ObIArray<ObDtlRpcDataResponse> &resps = result_.resps_;
  if (arg_.is_pure_control_msg_) {
    ObDtlMsgHeader header;
    ObDtlBatchMsg msg;
    if (OB_FAIL(ObDtlLinkedBuffer::deserialize_msg_header(arg_.batch_buffer_, header, false))) {
      LOG_WARN("failed to deserialize msg header", K(ret));
    } else  if (!header.is_dtl_batch_msg()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected msg type", K(header.type_));
    } else {
      const char *buf = arg_.batch_buffer_.buf();
      const int64_t sz = arg_.batch_buffer_.size();
      int64_t &pos = arg_.batch_buffer_.pos();
      if (OB_FAIL(common::serialization::decode(buf, sz, pos, msg))) {
        LOG_WARN("batch eof decode body failed", K(ret));
      } else if (OB_FAIL(process_batch_control_msg(msg))) {
        LOG_WARN("failed to process batch control msg", K(ret));
      }
    }
    LOG_TRACE("[DTL BATCH] process dtl batch msg", K(ret), K(msg));
    result_.is_pure_control_msg_ = true;
    result_.recode_ = ret;
  } else if (args.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected empty batch args", K(ret));
  } else if (OB_FAIL(resps.prepare_allocate(args.count()))) {
    LOG_WARN("prepare allocate failed", K(ret));
  } else {
    int tmp_ret = OB_SUCCESS;
    for (int64_t i = 0; i < args.count(); ++i) {
      ObDtlSendMessageP::process_msg(resps.at(i), args.at(i));
      tmp_ret = resps.at(i).recode_;
      if (OB_HASH_NOT_EXIST == tmp_ret) {
        // channel has been drained
        tmp_ret = OB_SUCCESS;
        resps.at(i).recode_ = OB_SUCCESS;
      } else if (tmp_ret != OB_SUCCESS) {
        LOG_WARN("failed to process_msg in batch", K(tmp_ret), K(i), K(args.count()));
      }
      if (OB_SUCCESS == ret) {
        ret = tmp_ret;
      }
    }
    LOG_TRACE("[DTL BATCH] finish process batch msg", K(ret), K(resps), K(args.count()));
  }
  return OB_SUCCESS;
}

int ObDtlBatchSendMessageP::process_batch_control_msg(ObDtlBatchMsg &msg)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < msg.ch_ids_.count(); ++i) {
    ObDtlChannel *chan = nullptr;
    if (OB_FAIL(DTL.get_channel(msg.ch_ids_.at(i), chan))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to get channel", K(ret));
      }
    } else {
      ObDtlBasicChannel *bc = static_cast<ObDtlBasicChannel *>(chan);
      if (msg.contained_msg_type_ == ObDtlMsgType::DRAIN_DATA_FLOW) {
        if (OB_NOT_NULL(bc->get_dfc())) {
          bc->get_dfc()->set_drain(bc);
        }
      } else if (msg.contained_msg_type_ == ObDtlMsgType::UNBLOCKING_DATA_FLOW) {
        if (OB_NOT_NULL(bc->get_dfc())) {
          if (bc->get_dfc()->is_block(bc)) {
            // transmit's rpc response is already processed,
            // we can unblock the channel directly
            bc->get_dfc()->unblock_channel(bc);
            LOG_TRACE("[DTL BATCH BLOCK] unblock channel", K(bc->get_id()), K(bc->get_peer()));
          } else {
            // response message is not processed yet, we should feedup a unblocking msg
            // for later processing
            if (OB_FAIL(feedup_unblocking_msg(bc))) {
              LOG_WARN("failed to feedup unblocking msg", K(ret));
            }
            LOG_TRACE("[DTL BATCH BLOCK] feedup unblocking msg", K(bc->get_id()), K(bc->get_peer()));
          }
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected msg type", K(msg.contained_msg_type_));
      }
      DTL.release_channel(chan);
    }
  }
  return ret;
}

}  // dtl
}  // sql
}  // oceanbase
