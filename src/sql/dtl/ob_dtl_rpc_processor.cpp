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

int ObDtlSendMessageP::process_msg(ObDtlRpcDataResponse& response, ObDtlSendArgs& arg)
{
  int ret = OB_SUCCESS;
  ObDtlChannel* chan = nullptr;
  response.is_block_ = false;
  if (OB_FAIL(DTL.get_channel(arg.chid_, chan))) {
    int tmp_ret = ret;
    // cache handling during mixed version runnin scenario during online upgrade
    // 1) new|old -> old: no handling needed
    // 2) old -> new:is_data_msg is false, same with old logic
    // 3) new -> new: is_data_msg might be true, codes between new nodes share the same logic
    // only buffer data msg
    ObDtlMsgHeader header;
    const bool keep_pos = true;
    if (!arg.buffer_.is_data_msg() &&
        OB_FAIL(ObDtlLinkedBuffer::deserialize_msg_header(arg.buffer_, header, keep_pos))) {
      LOG_WARN("failed to deserialize msg header", K(ret));
    } else if (header.is_drain()) {
      LOG_TRACE("receive drain cmd, unregister rpc channel", KP(arg.chid_));
      ret = OB_SUCCESS;
      tmp_ret = OB_SUCCESS;
    } else if (arg.buffer_.is_data_msg() && arg.buffer_.use_interm_result()) {
      if (OB_FAIL(process_interm_result(arg))) {
        LOG_WARN("fail to process internal result", K(ret));
      }
    } else if (arg.buffer_.is_data_msg() && 1 == arg.buffer_.seq_no()) {
      ObDtlLinkedBuffer* buf = &arg.buffer_;
      if (OB_FAIL(DTL.get_dfc_server().cache(arg.buffer_.tenant_id(), arg.chid_, buf))) {
        ret = tmp_ret;
        LOG_WARN("get DTL channel fail",
            KP(arg.chid_),
            K(ret),
            K(tmp_ret),
            K(arg.buffer_.tenant_id()),
            K(arg.buffer_.seq_no()));
      } else {
        // return block after cache first msg
        // if only one row, don't block
        response.is_block_ = !arg.buffer_.is_eof();
        LOG_TRACE("first msg blocked",
            K(response.is_block_),
            KP(arg.chid_),
            K(ret),
            K(arg.buffer_.tenant_id()),
            K(arg.buffer_.seq_no()));
      }
    } else {
      LOG_TRACE("get DTL channel fail", KP(arg.chid_), K(ret), K(tmp_ret));
    }
  } else {
    // have tenant context set up
    ObDtlRpcChannel* rpc_chan = reinterpret_cast<ObDtlRpcChannel*>(chan);
    ObDtlLinkedBuffer* buf = &arg.buffer_;
    if (OB_FAIL(rpc_chan->feedup(buf))) {
      LOG_WARN("feed up DTL channel fail", KP(arg.chid_), K(buf), KPC(buf), K(ret));
    } else if (OB_ISNULL(rpc_chan->get_dfc())) {
      LOG_TRACE("dfc of rpc channel is null",
          K(response.is_block_),
          KP(arg.chid_),
          K(ret),
          KP(rpc_chan->get_id()),
          K(rpc_chan->get_peer()));
    } else if (rpc_chan->belong_to_receive_data()) {
      response.is_block_ = rpc_chan->get_dfc()->is_block(rpc_chan);
      LOG_TRACE("need blocking",
          K(response.is_block_),
          KP(arg.chid_),
          K(ret),
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
  ObIArray<ObDtlRpcDataResponse>& resps = result_.resps_;
  ObIArray<ObDtlSendArgs>& args = arg_.args_;
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
        LOG_WARN("failed to process_msg",
            K(tmp_ret),
            K(arg_.bc_buffer_.seq_no()),
            K(resps.count()),
            K(args.count()),
            K(i),
            KP(&arg_.bc_buffer_));
      }
      // record first error
      if (OB_SUCCESS == ret) {
        ret = tmp_ret;
      }
    }
  }
  LOG_TRACE("finish process broadcast msg", K(ret), K(arg_.bc_buffer_), K(resps), K(args));
  return OB_SUCCESS;
}

int ObDtlSendMessageP::process_interm_result(ObDtlSendArgs& arg)
{
  int ret = OB_SUCCESS;
  int64_t channel_id = arg.chid_;
  ObDTLIntermResultKey key;
  ObDTLIntermResultInfo result_info;
  key.channel_id_ = channel_id;
  key.time_us_ = arg.buffer_.timeout_ts();
  bool need_free = false;
  if (OB_FAIL(ObDTLIntermResultManager::getInstance().get_interm_result_info(key, result_info))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ObMemAttr attr(arg.buffer_.tenant_id(), "DtlIntermRes", common::ObCtxIds::WORK_AREA);
      key.start_time_ = oceanbase::common::ObTimeUtility::current_time();
      ret = OB_SUCCESS;
      result_info.is_datum_ = PX_DATUM_ROW == arg.buffer_.msg_type();
      if (OB_FAIL(ObDTLIntermResultManager::getInstance().create_interm_result_info(attr, result_info))) {
        LOG_WARN("fail to create chunk row store", K(ret));
      } else if (OB_FAIL(DTL_IR_STORE_DO(
                     result_info, init, 0, arg.buffer_.tenant_id(), common::ObCtxIds::WORK_AREA, "DtlIntermRes"))) {
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
      ObAtomicAppendBlockCall call(arg.buffer_.buf(), arg.buffer_.size());
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
