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
#include "test_net_performance.h"

#include "omt/all_mock.h"
#include "observer/ob_srv_network_frame.h"
#include "observer/omt/ob_multi_tenant.h"
#include "observer/omt/ob_worker_processor.h"

using namespace oceanbase::common;
using namespace oceanbase::observer;
using namespace oceanbase::omt;
using namespace oceanbase::obrpc;
using namespace oceanbase::unittest;

static ObGlobalContext gctx;

int64_t ccnt = 6;
int64_t iocnt = 6;
int64_t scnt = 6;
int64_t msgcnt = 10000;

int main(int argc, char *argv[])
{
  int ret = OB_SUCCESS;

  all_mock_init();

  if (!parse_arg(argc, argv)) {
    return 1;
  }

  static ObSrvNetworkFrame frame(gctx);
  if (OB_FAIL(start_frame(frame))) {
    LOG_ERROR("start frame fail");
  } else {
    //frame.wait();
  }

  TestProxy proxy;
  frame.get_proxy(proxy);

  Client c(proxy);
  c.set_thread_count((int)ccnt);
  c.start();
  c.wait();

  //frame.wait();
  return 0;
}

namespace oceanbase
{
namespace unittest
{

class Processor
    : public ObIWorkerProcessor
{
public:
  Processor()
  {
    ObRpcResultCode rcode;
    rcode.serialize(pbuf_, 1024L, pbuf_len_);
  }

  virtual int process(rpc::ObRequest &req)
  {
    int ret = OB_SUCCESS;

    const ObRpcPacket &rpacket = reinterpret_cast<const ObRpcPacket&>(req.get_packet());

    ObRpcPacketCode pcode = rpacket.get_pcode();

    void *buf = easy_pool_alloc(req.get_request()->ms->pool, sizeof (ObRpcPacket));
    ObRpcPacket *packet = new (buf) ObRpcPacket();
    packet->set_pcode(pcode);
    packet->set_chid(rpacket.get_chid());
    packet->set_content(pbuf_, pbuf_len_);
    packet->set_session_id(0);
    packet->set_trace_id(common::ObCurTraceId::get());
    packet->set_resp();
    packet->calc_checksum();
    req.get_request()->opacket = packet;

    easy_request_wakeup(req.get_request());
    return ret;
  };

private:
  char pbuf_[1024];
  int64_t pbuf_len_;
}; // end of class Processor

void Client::run(obsys::CThread *thread, void *arg)
{

  UNUSED(arg);

  int64_t cnt = msgcnt;
  while (cnt--) {
    proxy_.to(ObAddr(ObAddr::IPV4, "127.0.0.1", 2500))
        .by(500)
        .test();
  }
}

int start_frame(ObSrvNetworkFrame &frame)
{
  int ret = OB_SUCCESS;

  static Processor p;
  static ObMultiTenant mt(p);
  mt.init(ObAddr());
  mt.add_tenant(500, (double)scnt / 4, (double)scnt / 4);
  mt.start();

  gctx.config_ = &ObServerConfig::get_instance();
  gctx.omt_ = &mt;

  GCONF.net_thread_count = iocnt;

  if (OB_FAIL(frame.init())) {
    LOG_ERROR("init net frame fail", K(ret));
  } else if (OB_FAIL(frame.start())) {
    LOG_ERROR("start net frame fail", K(ret));
  }

  return ret;
}

bool parse_arg(int argc, char *argv[])
{
  int idx = 0;
  if (++idx < argc) {
    ccnt = strtol(argv[idx], NULL, 10);
  }
  if (++idx < argc) {
    iocnt = strtol(argv[idx], NULL, 10);
  }
  if (++idx < argc) {
    scnt = strtol(argv[idx], NULL, 10);
  }
  if (++idx < argc) {
    msgcnt = strtol(argv[idx], NULL, 10);
  }

  return ccnt > 0 && iocnt > 0 && scnt > 0 && msgcnt > 0;
}


} // end of namespace unittest
} // end of namespace oceanbase
