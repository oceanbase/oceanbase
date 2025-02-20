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

#include "rpc/obrpc/ob_rpc_stream_cond.h"

#include "rpc/obrpc/ob_rpc_session_handler.h"
#include "rpc/obrpc/ob_poc_rpc_server.h"
#include "rpc/obrpc/ob_rpc_reverse_keepalive_struct.h"

using namespace oceanbase::common;
using namespace oceanbase::rpc;
using namespace oceanbase::obrpc;

ObRpcStreamCond::ObRpcStreamCond(ObRpcSessionHandler &handler)
    : sessid_(0), handler_(handler),
      first_pkt_id_(INVALID_RPC_PKT_ID), first_send_ts_(OB_INVALID_TIMESTAMP), src_addr_()
{
}

ObRpcStreamCond::~ObRpcStreamCond()
{
  destroy();
}

int ObRpcStreamCond::prepare(const ObAddr *src_addr, const ObRpcPacket *packet)
{
  int ret = OB_SUCCESS;
  if (0 == sessid_) {
    // generate session id first if there's no session id.
    sessid_ = handler_.generate_session_id();
    LOG_INFO("generate session id", K_(sessid));

    if (OB_FAIL(handler_.prepare_for_next_request(sessid_))) {
      LOG_WARN("preapre stream rpc fail", K(ret));
    } else if (src_addr != NULL) {
      src_addr_ = *src_addr;
      first_pkt_id_ = packet->get_packet_id();
      first_send_ts_ = packet->get_timestamp();
    }
  }
  return ret;
}

int ObRpcStreamCond::wait(ObRequest *&req, int64_t timeout)
{
  int ret = OB_SUCCESS;
  ObRpcReverseKeepaliveArg reverse_keepalive_arg(src_addr_, first_send_ts_, first_pkt_id_);
  if (sessid_ <= 0) {
    LOG_WARN("sessid is invalied", K_(sessid));
  } else if (OB_FAIL(handler_.wait_for_next_request(
                        sessid_, req, timeout, reverse_keepalive_arg))) {
    LOG_WARN("wait for next request failed", K(ret), K_(sessid));
  } else {
    //do nothing
  }
  return ret;
}

int ObRpcStreamCond::wakeup(rpc::ObRequest &req)
{
  return handler_.wakeup_next_thread(req);
}

int ObRpcStreamCond::destroy()
{
  int ret = OB_SUCCESS;
  if (sessid_ != 0) {
    if (OB_FAIL(handler_.destroy_session(sessid_))) {
      LOG_WARN("Failed to destroy session", K(ret));
    }
    sessid_ = 0;
  }
  return ret;
}

void ObRpcStreamCond::reuse()
{
  sessid_ = 0;
}
