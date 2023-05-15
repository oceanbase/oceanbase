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

#ifndef _OB_RPC_ASYNC_RESPONSE_H
#define _OB_RPC_ASYNC_RESPONSE_H 1
#include "rpc/ob_request.h"
#include "rpc/obrpc/ob_rpc_packet.h"
#include "rpc/frame/ob_req_processor.h"
#include "rpc/obmysql/ob_mysql_request_utils.h"
#include "rpc/obrpc/ob_rpc_result_code.h"
#include "lib/oblog/ob_warning_buffer.h"
#include "ob_table_rpc_processor_util.h"
namespace oceanbase
{
namespace obrpc
{
// this class is copied from ObRpcProcessor
template <class T>
class ObRpcAsyncResponse
{
public:
  ObRpcAsyncResponse(rpc::ObRequest *req, T &result)
      :req_(req),
       result_(result),
       using_buffer_(NULL)
  {}
  virtual ~ObRpcAsyncResponse() = default;
  int response(const int retcode);
private:
  int serialize();
  int do_response(ObRpcPacket *response_pkt, bool bad_routing);
  char *easy_alloc(int64_t size) const;
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObRpcAsyncResponse);
private:
  rpc::ObRequest *req_;
  T &result_;
  common::ObDataBuffer *using_buffer_;
};

template <class T>
char *ObRpcAsyncResponse<T>::easy_alloc(int64_t size) const
{
  void *buf = NULL;
  if (OB_ISNULL(req_)) {
    RPC_OBRPC_LOG(ERROR, "request is invalid", KP(req_));
  } else if (OB_ISNULL(req_->get_request())
             || OB_ISNULL(req_->get_request()->ms)
             || OB_ISNULL(req_->get_request()->ms->pool)) {
    RPC_OBRPC_LOG(ERROR, "request is invalid", K(req_));
  } else {
    buf = easy_pool_alloc(
        req_->get_request()->ms->pool, static_cast<uint32_t>(size));
  }
  return static_cast<char*>(buf);
}

template <class T>
int ObRpcAsyncResponse<T>::serialize()
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(using_buffer_)) {
    ret = common::OB_ERR_UNEXPECTED;
    RPC_OBRPC_LOG(ERROR, "using_buffer_ should not be NULL", K(ret));
  } else if (OB_FAIL(common::serialization::encode(
        using_buffer_->get_data(), using_buffer_->get_capacity(),
        using_buffer_->get_position(), result_))) {
    RPC_OBRPC_LOG(WARN, "encode data error", K(ret));
  } else {
    //do nothing
  }
  return ret;
}

template <class T>
int ObRpcAsyncResponse<T>::do_response(ObRpcPacket *response_pkt, bool bad_routing)
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(req_)) {
    ret = common::OB_ERR_NULL_VALUE;
    RPC_OBRPC_LOG(WARN, "req is NULL", K(ret));
  } else if (OB_ISNULL(req_->get_request())) {
    ret = common::OB_ERR_NULL_VALUE;
    RPC_OBRPC_LOG(WARN, "req is NULL", K(ret));
  } else {
    const ObRpcPacket *rpc_pkt = &reinterpret_cast<const ObRpcPacket&>(req_->get_packet());
    // TODO: fufeng, make force_destroy_second as a configure item
    // static const int64_t RESPONSE_RESERVED_US = 20 * 1000 * 1000;
    // int64_t rts = static_cast<int64_t>(req_->get_request()->start_time) * 1000 * 1000;
    // todo(fufeng): get 'force destroy second' from eio?
    // if (rts > 0 && eio_->force_destroy_second > 0
    //     && ::oceanbase::common::ObTimeUtility::current_time() - rts + RESPONSE_RESERVED_US > eio_->force_destroy_second * 1000000) {
    //   _OB_LOG(ERROR, "pkt process too long time: pkt_receive_ts=%ld, pkt_code=%d", rts, pcode);
    // }
    //copy packet into req buffer
    ObRpcPacketCode pcode = rpc_pkt->get_pcode();
    if (OB_SUCC(ret)) {
      ObRpcPacket *packet = response_pkt;
      packet->set_pcode(pcode);
      packet->set_chid(rpc_pkt->get_chid());
      packet->set_session_id(0);  // not stream
      packet->set_trace_id(rpc_pkt->get_trace_id());
      packet->set_resp();

      packet->set_request_arrival_time(req_->get_request_arrival_time());
      packet->set_arrival_push_diff(req_->get_arrival_push_diff());
      packet->set_push_pop_diff(req_->get_push_pop_diff());
      packet->set_pop_process_start_diff(req_->get_pop_process_start_diff());
      packet->set_process_start_end_diff(req_->get_process_start_end_diff());
      packet->set_process_end_response_diff(req_->get_process_end_response_diff());
      if (bad_routing) {
        packet->set_bad_routing();
      }
      packet->calc_checksum();
      req_->get_request()->opacket = packet;
    }
    //just set request retcode, wakeup in ObSingleServer::handlePacketQueue()
    req_->set_request_rtcode(EASY_OK);
    obmysql::ObMySQLRequestUtils::wakeup_request(req_);
  }
  return ret;
}

template <class T>
int ObRpcAsyncResponse<T>::response(const int retcode)
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(req_)) {
    ret = common::OB_INVALID_ARGUMENT;
    RPC_OBRPC_LOG(WARN, "invalid req, maybe stream rpc timeout", K(ret), K(retcode),
                  KP_(req));
  } else {
    obrpc::ObRpcResultCode rcode;
    rcode.rcode_ = retcode;

    // add warning buffer into result code buffer if rpc fails.
    common::ObWarningBuffer *wb = common::ob_get_tsi_warning_buffer();
    if (wb) {
      if (retcode != common::OB_SUCCESS) {
        (void)snprintf(rcode.msg_, common::OB_MAX_ERROR_MSG_LEN, "%s", wb->get_err_msg());
      }
      //always add warning buffer
      bool not_null = true;
      for (uint32_t idx = 0; OB_SUCC(ret) && not_null && idx < wb->get_readable_warning_count(); idx++) {
        const common::ObWarningBuffer::WarningItem *item = wb->get_warning_item(idx);
        if (item != NULL) {
          if (OB_FAIL(rcode.warnings_.push_back(*item))) {
            RPC_OBRPC_LOG(WARN, "Failed to add warning", K(ret));
          }
        } else {
          not_null = false;
        }
      }
    }

    int64_t content_size = common::serialization::encoded_length(result_) +
        common::serialization::encoded_length(rcode);

    char *buf = NULL;
    if (OB_FAIL(ret)) {
      //do nothing
    } else if (content_size > common::OB_MAX_PACKET_LENGTH) {
      ret = common::OB_RPC_PACKET_TOO_LONG;
      RPC_OBRPC_LOG(WARN, "response content size bigger than OB_MAX_PACKET_LENGTH", K(ret));
    } else {
      //allocate memory from easy
      //[ ObRpcPacket ... ObDatabuffer ... serilized content ...]
      int64_t size = (content_size) + sizeof (common::ObDataBuffer) + sizeof(ObRpcPacket);
      buf = static_cast<char*>(easy_alloc(size));
      if (NULL == buf) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        RPC_OBRPC_LOG(WARN, "allocate rpc data buffer fail", K(ret), K(size));
      } else {
        using_buffer_ = new (buf + sizeof(ObRpcPacket)) common::ObDataBuffer();
        if (!(using_buffer_->set_data(buf + sizeof(ObRpcPacket) + sizeof (*using_buffer_),
            content_size))) {
          ret = common::OB_INVALID_ARGUMENT;
          RPC_OBRPC_LOG(WARN, "invalid parameters", K(ret));
        }
      }
    }

    if (OB_FAIL(ret)) {
      //do nothing
    } else if (OB_ISNULL(using_buffer_)) {
      ret = common::OB_ERR_UNEXPECTED;
      RPC_OBRPC_LOG(ERROR, "using_buffer_ is NULL", K(ret));
    } else if (OB_FAIL(rcode.serialize(using_buffer_->get_data(),
        using_buffer_->get_capacity(),
        using_buffer_->get_position()))) {
      RPC_OBRPC_LOG(WARN, "serialize result code fail", K(ret));
    } else {
      // also send result if process successfully.
      if (common::OB_SUCCESS == retcode) {
        if (OB_FAIL(serialize())) {
          RPC_OBRPC_LOG(WARN, "serialize result fail", K(ret));
        }
      }
    }

    // routing check : whether client should refresh location cache and retry
    // Now, following the same logic as in ../mysql/ob_query_retry_ctrl.cpp
    bool bad_routing = false;
    if (OB_SUCC(ret)) {
      if (common::OB_SUCCESS != retcode && observer::is_bad_routing_err(retcode)) {
        bad_routing = true;
        RPC_OBRPC_LOG(WARN, "bad routing", K(retcode), K(bad_routing));
      }
    }

    if (OB_SUCC(ret)) {
      ObRpcPacket *pkt = new (buf) ObRpcPacket();
      //Response rsp(sessid, is_stream_, is_last, pkt);
      pkt->set_content(using_buffer_->get_data(), using_buffer_->get_position());
      if (OB_FAIL(do_response(pkt, bad_routing))) {
        RPC_OBRPC_LOG(WARN, "response data fail", K(ret));
      }
    }

    using_buffer_ = NULL;
  }
  return ret;
}
} // end namespace obrpc
} // end namespace oceanbase

#endif /* _OB_RPC_ASYNC_RESPONSE_H */
