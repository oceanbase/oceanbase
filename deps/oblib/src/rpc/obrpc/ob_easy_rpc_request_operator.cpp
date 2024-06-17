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

#include "rpc/obrpc/ob_easy_rpc_request_operator.h"
#include "lib/utility/utility.h"

using namespace oceanbase::rpc;
using namespace oceanbase::common;
namespace oceanbase
{

namespace obmysql
{
  void __attribute__((weak)) request_finish_callback() {}
}

namespace obrpc
{
void* ObEasyRpcRequestOperator::alloc_response_buffer(ObRequest* req, int64_t size)
{
  void* buf = NULL;
  easy_request_t* ez_req = NULL;
  if (OB_ISNULL(req) || OB_ISNULL(ez_req = req->get_ez_req())) {
    RPC_LOG_RET(ERROR, common::OB_INVALID_ARGUMENT, "request is invalid", KP(req));
  } else if (OB_ISNULL(ez_req->ms)
             || OB_ISNULL(ez_req->ms->pool)) {
    RPC_LOG_RET(ERROR, common::OB_INVALID_ARGUMENT, "request is invalid", K(req));
  } else {
    buf = easy_pool_alloc(
        ez_req->ms->pool, static_cast<uint32_t>(size));
  }
  return buf;
}

void ObEasyRpcRequestOperator::response_result(ObRequest* req, obrpc::ObRpcPacket* pkt)
{
  //just set request retcode, wakeup in ObSingleServer::handlePacketQueue()
  easy_request_t* ez_req = NULL;
  if (OB_ISNULL(req) || OB_ISNULL(ez_req = req->get_ez_req())) {
    RPC_LOG_RET(ERROR, common::OB_INVALID_ARGUMENT, "reqest is NULL or request has wokenup");
  } else {
    ez_req->opacket = pkt;
    ez_req->retcode = EASY_OK;
    obmysql::request_finish_callback();
    if (NULL == EASY_IOTH_SELF) { // ErrorP response in io thread, need not wakeup easy request.
      easy_request_wakeup(ez_req);
    }
  }
}

ObAddr ObEasyRpcRequestOperator::get_peer(const ObRequest* req)
{
  ObAddr addr;
  easy_request_t* ez_req = req->ez_req_;
  if (OB_ISNULL(ez_req) || OB_ISNULL(ez_req->ms)
      || OB_ISNULL(ez_req->ms->c)) {
    RPC_LOG_RET(ERROR, common::OB_INVALID_ARGUMENT, "invalid argument", K(ez_req));
  } else {
    easy_addr_t &ez = ez_req->ms->c->addr;
    if (!ez2ob_addr(addr, ez)) {
      RPC_LOG_RET(ERROR, common::OB_INVALID_ARGUMENT, "fail to convert easy_addr to ob_addr", K(ez_req));
    } // otherwise leave addr be zeros
  }
  return addr;
}

}; // end namespace rpc
}; // end namespace oceanbase

