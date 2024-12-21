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

#define USING_LOG_PREFIX SERVER
#include "ob_table_rpc_response_sender.h"

using namespace oceanbase::observer;
using namespace oceanbase::common;
using namespace oceanbase::table;
using namespace oceanbase::share;
using namespace oceanbase::obrpc;

char *ObTableRpcResponseSender::easy_alloc(int64_t size) const
{
  void *buf = NULL;
  if (OB_ISNULL(req_)) {
    RPC_OBRPC_LOG_RET(ERROR, OB_INVALID_ARGUMENT, "request is invalid", KP(req_));
  } else {
    buf = RPC_REQ_OP.alloc_response_buffer(req_, static_cast<uint32_t>(size));
  }
  return static_cast<char*>(buf);
}

int ObTableRpcResponseSender::serialize()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(using_buffer_)) {
    ret = OB_ERR_UNEXPECTED;
    RPC_OBRPC_LOG(ERROR, "using_buffer_ should not be NULL", K(ret));
  } else if (OB_ISNULL(result_)) {
    ret = OB_ERR_UNEXPECTED;
    RPC_OBRPC_LOG(ERROR, "result_ should not be NULL", K(ret));
  } else if (OB_FAIL(serialization::encode(
        using_buffer_->get_data(), using_buffer_->get_capacity(),
        using_buffer_->get_position(), *result_))) {
    RPC_OBRPC_LOG(WARN, "encode data error", K(ret));
  } else {
    //do nothing
  }
  return ret;
}

int ObTableRpcResponseSender::do_response(ObRpcPacket *response_pkt, bool require_rerouting)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(req_)) {
    ret = OB_ERR_NULL_VALUE;
    RPC_OBRPC_LOG(WARN, "req is NULL", K(ret));
  } else if (ObRpcPacketCode::OB_INVALID_RPC_CODE == pcode_) {
    ret = OB_ERR_UNEXPECTED;
    RPC_OBRPC_LOG(WARN, "pcode is invalid", K(ret), K_(pcode), KPC_(req));
  } else {
    const ObRpcPacket *rpc_pkt = &reinterpret_cast<const ObRpcPacket&>(req_->get_packet());
    // TODO: fufeng, make force_destroy_second as a configure item
    // static const int64_t RESPONSE_RESERVED_US = 20 * 1000 * 1000;
    // int64_t rts = static_cast<int64_t>(req_->get_request()->start_time) * 1000 * 1000;
    // todo(fufeng): get 'force destroy second' from eio?
    // if (rts > 0 && eio_->force_destroy_second > 0
    //     && ::oceanbase::ObTimeUtility::current_time() - rts + RESPONSE_RESERVED_US > eio_->force_destroy_second * 1000000) {
    //   _OB_LOG(ERROR, "pkt process too long time: pkt_receive_ts=%ld, pkt_code=%d", rts, pcode);
    // }
    //copy packet into req buffer
    ObRpcPacket *packet = NULL;
    req_->set_trace_point(rpc::ObRequest::OB_EASY_REQUEST_RPC_ASYNC_RSP);
    if (OB_SUCC(ret)) {
      packet = response_pkt;
      packet->set_pcode(pcode_);
      packet->set_chid(rpc_pkt->get_chid());
      packet->set_session_id(0);  // not stream
      packet->set_trace_id(rpc_pkt->get_trace_id());
      packet->set_resp();

#ifdef ERRSIM
      packet->set_module_type(rpc_pkt->get_module_type());
#endif

      packet->set_request_arrival_time(req_->get_request_arrival_time());
      packet->set_arrival_push_diff(req_->get_arrival_push_diff());
      packet->set_push_pop_diff(req_->get_push_pop_diff());
      packet->set_pop_process_start_diff(req_->get_pop_process_start_diff());
      packet->set_process_start_end_diff(req_->get_process_start_end_diff());
      packet->set_process_end_response_diff(req_->get_process_end_response_diff());
      if (require_rerouting) {
        packet->set_require_rerouting();
      }
      packet->calc_checksum();
    }
    //just set request retcode, wakeup in ObSingleServer::handlePacketQueue()
    RPC_REQ_OP.response_result(req_, packet);
  }
  return ret;
}

int ObTableRpcResponseSender::response(const int cb_param)
{
  int ret = OB_SUCCESS;
  int retcode = (cb_param == OB_SUCCESS ? exec_ret_code_ : cb_param);
  if (OB_ISNULL(req_)) {
    ret = OB_INVALID_ARGUMENT;
    RPC_OBRPC_LOG(WARN, "invalid req, maybe stream rpc timeout", K(ret), K(retcode), KP_(req));
  } else if (OB_ISNULL(result_)) {
    ret = OB_INVALID_ARGUMENT;
    RPC_OBRPC_LOG(WARN, "result_ is null", K(ret));
  } else {
    obrpc::ObRpcResultCode rcode;
    rcode.rcode_ = retcode;

    // add warning buffer into result code buffer if rpc fails.
    ObWarningBuffer *wb = ob_get_tsi_warning_buffer();
    if (wb) {
      if (retcode != OB_SUCCESS) {
        (void)snprintf(rcode.msg_, OB_MAX_ERROR_MSG_LEN, "%s", wb->get_err_msg());
      }
      //always add warning buffer
      bool not_null = true;
      for (uint32_t idx = 0; OB_SUCC(ret) && not_null && idx < wb->get_readable_warning_count(); idx++) {
        const ObWarningBuffer::WarningItem *item = wb->get_warning_item(idx);
        if (item != NULL) {
          if (OB_FAIL(rcode.warnings_.push_back(*item))) {
            RPC_OBRPC_LOG(WARN, "Failed to add warning", K(ret));
          }
        } else {
          not_null = false;
        }
      }
    }

    ObDataBuffer data_buf;
    uint32_t rpc_header_size = static_cast<uint32_t>(obrpc::ObRpcPacket::get_header_size());
    uint32_t ez_rpc_header_size = OB_NET_HEADER_LENGTH + rpc_header_size;
    int64_t content_size = serialization::encoded_length(*result_) +
        serialization::encoded_length(rcode);

    char *pkt_buf = NULL;
    if (OB_FAIL(ret)) {
      //do nothing
    } else if (content_size > OB_MAX_PACKET_LENGTH) {
      ret = OB_RPC_PACKET_TOO_LONG;
      RPC_OBRPC_LOG(WARN, "response content size bigger than OB_MAX_PACKET_LENGTH", K(ret));
    } else {
      /*
       *                   RPC response packet buffer format
       *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
       *  |  ObRpcPacket  |  easy header |  RPC header  | rcode | RPC response |
       *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
       */
      int64_t pkt_buf_size = sizeof(ObRpcPacket) + ez_rpc_header_size + content_size;
      pkt_buf = static_cast<char*>(easy_alloc(pkt_buf_size));
      if (NULL == pkt_buf) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        RPC_OBRPC_LOG(WARN, "allocate rpc data buffer fail", K(ret), K(pkt_buf_size));
      } else {
        using_buffer_ = &data_buf;
        if (!(using_buffer_->set_data(pkt_buf + sizeof(ObRpcPacket) + ez_rpc_header_size,
            content_size))) {
          ret = OB_INVALID_ARGUMENT;
          RPC_OBRPC_LOG(WARN, "invalid parameters", K(ret));
        }
      }
    }

    if (OB_FAIL(ret)) {
      //do nothing
    } else if (OB_ISNULL(using_buffer_)) {
      ret = OB_ERR_UNEXPECTED;
      RPC_OBRPC_LOG(ERROR, "using_buffer_ is NULL", K(ret));
    } else if (OB_FAIL(rcode.serialize(using_buffer_->get_data(),
        using_buffer_->get_capacity(),
        using_buffer_->get_position()))) {
      RPC_OBRPC_LOG(WARN, "serialize result code fail", K(ret));
    } else {
      // 1. send result if process successfully.
      // 2. send result if require rerouting
      if (OB_SUCCESS == retcode
          || observer::ObTableRpcProcessorUtil::is_require_rerouting_err(retcode)) {
        if (OB_FAIL(serialize())) {
          RPC_OBRPC_LOG(WARN, "serialize result fail", K(ret));
        }
      }
    }

    // rerouting: whether client should refresh location cache and retry
    // Now, following the same logic as in ../mysql/ob_query_retry_ctrl.cpp
    bool require_rerouting = false;
    if (OB_SUCC(ret)) {
      if (OB_SUCCESS != retcode
          && observer::ObTableRpcProcessorUtil::is_require_rerouting_err(retcode)) {
        require_rerouting = true;
        RPC_OBRPC_LOG(DEBUG, "require rerouting", K(retcode), K(require_rerouting));
      }
    }

    if (OB_SUCC(ret)) {
      ObRpcPacket *pkt = new (pkt_buf) ObRpcPacket();
      //Response rsp(sessid, is_stream_, is_last, pkt);
      pkt->set_content(using_buffer_->get_data(), using_buffer_->get_position());
      if (OB_FAIL(do_response(pkt, require_rerouting))) {
        RPC_OBRPC_LOG(WARN, "response data fail", K(ret), K(retcode));
      }
    }

    using_buffer_ = NULL;
  }
  return ret;
}
