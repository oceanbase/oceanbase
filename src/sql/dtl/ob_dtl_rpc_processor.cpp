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
#include "ob_dtl_rpc_processor.h"
#include "sql/dtl/ob_dtl.h"
#include "sql/dtl/ob_dtl_linked_buffer.h"
#include "sql/dtl/ob_dtl_rpc_channel.h"
#include "sql/dtl/ob_dtl_flow_control.h"
#include "sql/engine/basic/ob_chunk_row_store.h"
#include "sql/dtl/ob_dtl_fc_server.h"
#include "ob_dtl_interm_result_manager.h"
#include "sql/engine/px/datahub/components/ob_dh_init_channel.h"
using namespace oceanbase::common;

namespace oceanbase {
namespace sql {
namespace dtl {

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
    } else if (header.is_px_bloom_filter_data()) {
      ObDtlLinkedBuffer *buf = &arg.buffer_;
      if (OB_FAIL(process_px_bloom_filter_data(buf))) {
        LOG_WARN("fail to process px bloom filter data", K(ret));
      } else {
        tmp_ret = OB_SUCCESS;
      }
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

int ObDtlSendMessageP::process_px_bloom_filter_data(ObDtlLinkedBuffer *&buffer)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buffer)) {
    ret = OB_NOT_INIT;
    LOG_WARN("linked buffer is null", K(ret));
  } else {
    ObPxBloomFilterData bf_data;
    ObDtlMsgHeader header;
    ObPxBloomFilter *filter = NULL;
    if (OB_FAIL(ObDtlLinkedBuffer::deserialize_msg_header(*buffer, header))) {
      LOG_WARN("fail to decode header of buffer", K(ret));
    }
    if (OB_SUCC(ret)) {
      const char *buf = buffer->buf();
      int64_t size = buffer->size();
      int64_t &pos = buffer->pos();
      if (OB_FAIL(common::serialization::decode(buf, size, pos, bf_data))) {
        LOG_WARN("fail to decode bloom filter data", K(ret));
      } else {
        ObPXBloomFilterHashWrapper bf_key(bf_data.tenant_id_, bf_data.filter_id_,
            bf_data.server_id_, bf_data.px_sequence_id_, 0/*task_id*/);
        if (OB_FAIL(ObPxBloomFilterManager::instance().get_px_bf_for_merge_filter(
            bf_key, filter))) {
          LOG_WARN("fail to get px bloom filter", K(ret));
        }
        // get_px_bf_for_merge_filter只有在成功后会增加filter的引用计数
        if (OB_SUCC(ret) && OB_NOT_NULL(filter)) {
          if (OB_FAIL(filter->merge_filter(&bf_data.filter_))) {
            LOG_WARN("fail to merge filter", K(ret));
          } else if (OB_FAIL(filter->process_recieve_count(bf_data.bloom_filter_count_))) {
            LOG_WARN("fail to process recieve count", K(ret));
          }
          // merge以及process操作完成之后, 需要减少其引用计数.
          (void)filter->dec_merge_filter_count();
        }
      }
    }
  }
  return ret;
}

}  // dtl
}  // sql
}  // oceanbase
