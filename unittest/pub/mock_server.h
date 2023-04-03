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

#include "common/server_framework/ob_net_easy.h"
#include "common/server_framework/ob_net_rpc_handler.h"
#include "observer/ob_rpc_processor.h"

using oceanbase::common::ObNetEasy;
using oceanbase::common::ObNetRpcHandler;
using oceanbase::common::ObNetOptions;
using namespace oceanbase::common;
using namespace oceanbase::observer;

namespace oceanbase
{
namespace unittest
{
namespace pub
{

template <PacketCode pcode>
class MockP
    : public ObRpcProcessor<pcode>
{
public:
  int process()
  {
    return ObRpcProcessor<pcode>::response_result();
  }
};

class MockPacketHandler
    : public ObNetRpcHandler
{
public:
  int process(easy_request_t *r)
  {
    int ret = EASY_OK;
    if (NULL == r || NULL == r->ipacket)
    {
      SERVER_LOG(ERROR, "request is empty", K(r), K(r->ipacket));
      ret = EASY_BREAK;
    }
    else
    {
      ObPacket *pkt = (ObPacket*) r->ipacket;
      if (NULL == pkt) {
        SERVER_LOG(ERROR, "receive NULL packet");
      } else {
        pkt->set_request(r);
        easy_request_sleeping(r); // set alloc lock && inc ref count
        ret = process_pkt(*pkt);
        if (OB_SUCC(ret)) {
          ret = EASY_AGAIN;
        } else {
          easy_atomic_dec(&r->ms->c->pool->ref);
          easy_atomic_dec(&r->ms->pool->ref);
          if (OB_QUEUE_OVERFLOW == ret)
          {
            char buffer[32];
            SERVER_LOG(WARN, "can not push packet to packet queue",
                       "src", inet_ntoa_s(buffer, 32, r->ms->c->addr), "pcode", pkt->get_pcode());
          }
          ret = EASY_OK;
        }
      }
    }
    return ret;
  }

private:

#define REG_PKT(pcode)                          \
  case pcode: {                                 \
    p = new MockP<pcode>();                     \
    break;                                      \
}

  int process_pkt(ObPacket &pkt)
  {
    ObRequestProcessor *p = NULL;
    switch (pkt.get_pcode()) {
      REG_PKT(OB_GET_CONFIG);
      REG_PKT(OB_SET_CONFIG);
      REG_PKT(OB_BOOTSTRAP);
      REG_PKT(OB_EXECUTE_BOOTSTRAP);
      REG_PKT(OB_IS_EMPTY_SERVER);
      REG_PKT(OB_FETCH_SCHEMA);
      default: { }
    }
    if (NULL != p) {
      ObDataBuffer buf;
      alloc_buffer(buf);
      p->set_buffer(&buf);
      p->set_ob_packet(&pkt);
      p->process();
      free_buffer(buf);
      delete p;
    }
    return OB_SUCCESS;
  }

  void alloc_buffer(ObDataBuffer &buf) {
    buf.set_data(new char[OB_MAX_PACKET_LENGTH], OB_MAX_PACKET_LENGTH);
  }

  void free_buffer(ObDataBuffer &buf) {
    delete [] buf.get_data();
  }
}; // end of class MockPacketHandler

class MockServer
    : public ObNetEasy
{
public:
  int init(int port) {
    int ret = OB_SUCCESS;
    ObNetOptions opts;
    opts.type_ = ObNetOptions::SERVER;
    opts.lport_ = port;
    opts.io_cnt_ = 1;
    if (OB_SUCCESS != (ret = ObNetEasy::init(phandler_, opts))) {
      SERVER_LOG(ERROR, "init client network fail", K(ret));
    }
    return ret;
  }
private:
  MockPacketHandler phandler_;
}; // end of class MockServer

} // end of namespace public
} // end of namespace unittest
} // end of namespace oceanbase
