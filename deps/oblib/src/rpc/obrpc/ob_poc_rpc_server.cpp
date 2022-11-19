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

#include "rpc/obrpc/ob_poc_rpc_server.h"
#include "rpc/obrpc/ob_rpc_endec.h"

namespace oceanbase
{
namespace obrpc
{
ObPocRpcServer global_poc_server;
}; // end namespace obrpc
}; // end namespace oceanbase

using namespace oceanbase::common;
using namespace oceanbase::obrpc;
using namespace oceanbase::rpc;

ObRequest* ObPocServerHandleContext::create(ObINio& nio, int64_t resp_id, char* buf, int64_t sz)
{
  int ret = OB_SUCCESS;
  ObRequest* req = NULL;
  ObPocServerHandleContext* ctx = NULL;
  ObRpcMemPool* pool = ObRpcMemPool::create(sizeof(ObPocServerHandleContext) + sizeof(ObRequest));
  if (NULL != pool) {
    ctx = new(pool + 1)ObPocServerHandleContext(nio, *pool, resp_id);
    req = new(ctx + 1)ObRequest(ObRequest::OB_RPC, 1);
    ObRpcPacket* pkt = NULL;
    if (OB_FAIL(rpc_decode_ob_packet(*pool, buf, sz, pkt))) {
      RPC_LOG(ERROR, "decode packet fail", K(ret));
    } else {
      req->set_server_handle_context(ctx);
      req->set_packet(pkt);
    }
  }
  return req;
}

void ObPocServerHandleContext::resp(ObRpcPacket* pkt)
{
  int ret = OB_SUCCESS;
  char* buf = NULL;
  int64_t sz = 0;
  if (OB_FAIL(rpc_encode_ob_packet(pool_, pkt, buf, sz))) {
    RPC_LOG(WARN, "rpc_encode_ob_packet fail", K(pkt));
    buf = NULL;
    sz = 0;
  }
  nio_.resp(resp_id_, buf, sz);
}

int ObPocRpcServer::start(int port, frame::ObReqDeliver* deliver)
{
  int ret = OB_SUCCESS;
  uint64_t POC_RPC_MAGIC = 0;
  server_req_handler_.init(deliver, &nio_);
  int accept_queue_fd = listener_.regist(POC_RPC_MAGIC, 0, NULL);
  nio_.init(&server_req_handler_, accept_queue_fd);
  if (OB_FAIL( nio_.start())) {
    RPC_LOG(ERROR, "poc nio start fail", K(ret));
  } else if (OB_FAIL(listener_.start())) {
    RPC_LOG(ERROR, "listen fail", K(ret), K(port));
  } else {
    RPC_LOG(INFO, "poc rpc server listen succ");
  }
  return ret;
}
