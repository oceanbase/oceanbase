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

#include "rpc/obrpc/ob_rpc_handler.h"

#include "io/easy_io.h"

#include "lib/utility/utility.h"
#include "rpc/obrpc/ob_rpc_packet.h"
#include "rpc/obrpc/ob_rpc_request.h"
#include "rpc/ob_packet.h"
using namespace oceanbase::rpc;
using namespace oceanbase::rpc::frame;
using namespace oceanbase::obrpc;
using namespace oceanbase::common;

ObRpcHandler::ObRpcHandler(ObReqDeliver &deliver)
    : deliver_(deliver)
{
  EZ_ADD_CB(process);
}

ObRpcHandler::~ObRpcHandler()
{
  // empty
}

int ObRpcHandler::init()
{
  return OB_SUCCESS;
}

int ObRpcHandler::process(easy_request_t *r)
{
  int eret = EASY_OK;
  ObTimeGuard timeguard("ObRpcHandler::process", OB_EASY_HANDLER_COST_TIME);
  if (OB_ISNULL(r) || OB_ISNULL(r->ipacket)) {
    eret = EASY_ERROR;
    LOG_ERROR_RET(common::OB_ERR_UNEXPECTED, "request is empty", K(eret), K(r));
  } else if (OB_ISNULL(r->ms) || OB_ISNULL(r->ms->pool)) {
    eret = EASY_ERROR;
    LOG_ERROR_RET(common::OB_ERR_UNEXPECTED, "Easy message session(ms) or ms->pool is NULL", K(eret), "ms", r->ms);
  } else if (OB_ISNULL(r->ms->c) || OB_ISNULL(r->ms->c->pool)) {
    eret = EASY_ERROR;
    LOG_ERROR_RET(common::OB_ERR_UNEXPECTED, "Easy ms connect(c) or (c->pool)is NULL", K(eret), "connect", r->ms->c);
  } else {
    // TODO: fufeng, check copy new request is necessary.
    timeguard.click();
    void *buf = easy_alloc(r->ms->pool, sizeof (ObRequest));
    timeguard.click();
    if (OB_UNLIKELY(NULL == buf)) {
      eret = EASY_ERROR;
      LOG_WARN_RET(common::OB_ALLOCATE_MEMORY_FAILED, "alloc easy memory fail", K(eret));
    } else {
      ObRpcRequest *req = new (buf) ObRpcRequest(ObRpcRequest::OB_RPC);
      const ObRpcPacket *pkt = reinterpret_cast<ObRpcPacket*>(r->ipacket);
      req->set_receive_timestamp(pkt->get_receive_ts());
      req->set_packet(pkt);
      req->set_ez_req(r);
      r->protocol = EASY_REQQUEST_TYPE_RPC;
      easy_request_sleeping(r); // set alloc lock && inc ref count
      timeguard.click();
      req->set_request_arrival_time(r->request_arrival_time);
      req->set_arrival_push_diff(common::ObTimeUtility::current_time());
      eret = deliver_.deliver(*req);
      timeguard.click();
      if (EASY_OK == eret) {
        //deliver success, easy need to hold resource.
        eret = EASY_AGAIN;
      } else {
        //deliver fail, easy can free resource.
        LOG_DEBUG("Deliver failed", K(eret));
        easy_atomic_dec(&r->ms->c->pool->ref);
        easy_atomic_dec(&r->ms->pool->ref);
        if (OB_QUEUE_OVERFLOW == eret) {
          LOG_WARN_RET(common::OB_ERR_UNEXPECTED, "deliver request fail", K(eret), K(*req));
        }
        eret = EASY_OK;
      }
      timeguard.click();
    }
  }
  return eret;
}
