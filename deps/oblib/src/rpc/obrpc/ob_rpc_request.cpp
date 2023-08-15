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

#define USING_LOG_PREFIX RPC_OBRPC
#include "rpc/obrpc/ob_rpc_request.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace obrpc
{
int ObRpcRequest::response_fail(
    common::ObDataBuffer *buffer,
    const ObRpcResultCode &res_code,
    const int64_t &sessid)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buffer)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("input buffer is null", K(ret));
  } else if (OB_ISNULL(buffer->get_data())) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("input buffer is null", K(ret));
  } else if (OB_ISNULL(ez_req_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ez_req_ is null", K(ret));
  } else if (OB_ISNULL(pkt_)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("pkt_ pointer is null", K(ret));
  } else if (OB_ISNULL(ez_req_->ms)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ez_req_'s message is null", K(ret));
  } else if (OB_FAIL(do_serilize_fail(buffer, res_code))) {
    LOG_WARN("failed to serilize fail info", K(ret));
  } else if (OB_FAIL(do_flush_buffer(buffer, reinterpret_cast<const obrpc::ObRpcPacket*>(pkt_),
                                     sessid))) {
    LOG_WARN("failed to flush buffer", K(ret));
  } else {
    //do nothing
  }
  return ret;
}

int ObRpcRequest::do_flush_buffer(common::ObDataBuffer *buffer, const ObRpcPacket *pkt,
                                      int64_t sess_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buffer)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("input buffer is null", K(ret));
  } else if (OB_ISNULL(buffer->get_data())) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("input buffer is null", K(ret));
  } else if (OB_ISNULL(pkt_)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("pkt_ pointer is null", K(ret));
  } else {
    //flush buffer to caller
    const obrpc::ObRpcPacket *rpc_pkt = pkt;
    obrpc::ObRpcPacketCode pcode = rpc_pkt->get_pcode();
    int64_t size = buffer->get_position() + sizeof (obrpc::ObRpcPacket);
    char *pbuf = NULL;
    char *ibuf = static_cast<char *>(RPC_REQ_OP.alloc_response_buffer(
        this, static_cast<uint32_t>(size)));
    if (OB_ISNULL(ibuf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc buffer from req->ms->pool failed", K(ret));
    } else {
      pbuf = ibuf + sizeof (obrpc::ObRpcPacket);
      MEMCPY(pbuf, buffer->get_data(), buffer->get_position());
    }

    obrpc::ObRpcPacket *res_packet = NULL;
    if (OB_SUCC(ret)) {
      res_packet = new(ibuf) obrpc::ObRpcPacket();
      res_packet->set_pcode(pcode);
      res_packet->set_chid(rpc_pkt->get_chid());
      res_packet->set_content(pbuf, buffer->get_position());
      res_packet->set_session_id(sess_id);
      res_packet->set_trace_id(common::ObCurTraceId::get());
      res_packet->set_resp();
#ifdef ERRSIM
      res_packet->set_module_type(THIS_WORKER.get_module_type());
#endif
      res_packet->calc_checksum();
    }
    RPC_REQ_OP.response_result(this, res_packet);
  }
  return ret;
}

int ObRpcRequest::do_serilize_fail(common::ObDataBuffer *buffer,
                                       const ObRpcResultCode &res_code)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buffer)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("buffer is nul", K(ret));
  } else if (OB_ISNULL(buffer->get_data())) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("buffer is null", K(ret));
  } else if (OB_FAIL(res_code.serialize(buffer->get_data(),
                                        buffer->get_capacity(),
                                        buffer->get_position()))) {
    LOG_WARN("serialize result code fail", K(ret));
  } else {
    //do nothing
  }
  return ret;
}

} //end of namespace rpc
} //end of namespace oceanbase
