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

#ifndef RPC_TESTING_H
#define RPC_TESTING_H

#include <memory>
#include "rpc/frame/ob_net_easy.h"
#include "rpc/obrpc/ob_rpc_handler.h"
#include "rpc/obrpc/ob_rpc_packet.h"
#include "rpc/frame/ob_req_deliver.h"
#include "rpc/ob_request.h"
// for convenience of outer use
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "rpc/obrpc/ob_rpc_processor.h"

namespace rpctesting {
using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::rpc;
using namespace oceanbase::rpc::frame;
using namespace oceanbase::obrpc;

class Service
{
  // Deliver is the class that transfers the result of unpacking RpcHandler to the corresponding queue. Here will be countered
  // The deliver method of Service facilitates the integration of logic into the Service class.
  class Deliver
      : public ObReqQDeliver {
  public:
    Deliver(ObiReqQHandler &qhandler, Service &service)
        : ObReqQDeliver(qhandler),
          service_(service)
    {}
    int init() override { return OB_SUCCESS; }
    void stop() override {}
    int deliver(rpc::ObRequest &req) override;
  private:
    Service &service_;
  };
  // Translator is responsible for translating an ObRequest request into the corresponding Processor. Here will
  // Reverse the translate method of Service, so that the logic is concentrated in the Service.
  class Translator
      : public ObReqTranslator {
  public:
    Translator(Service &service)
        : service_(service)
    {}
  protected:
    ObReqProcessor *get_processor(ObRequest &req) override;
  private:
    Service &service_;
  };

public:
  Service(int listen_port=33244)
      : easy_(),
        translator_(*this),
        qhandler_(translator_),
        deliver_(qhandler_, *this),
        handler_(deliver_),
        transport_(nullptr),
        listen_port_(listen_port),
        try_listen_cnt_(0),
        queue_(),
        proc_map_()
  {}
  virtual ~Service() {}

  int init();
  int get_listen_port() const { return listen_port_; }
  int get_proxy(ObRpcProxy &proxy) { return proxy.init(transport_); }
  const common::ObAddr get_dst() const
  { return common::ObAddr(common::ObAddr::IPV4, "127.0.0.1", get_listen_port()); }

  template <class Proc>
  int reg_processor(Proc *p);


protected:
  ObReqProcessor *translate(ObRequest &req);
  int deliver(ObRequest &req);

private:
  ObNetEasy easy_;
  Translator translator_;
  ObReqQHandler qhandler_;
  Deliver deliver_;
  ObRpcHandler handler_;
  ObReqTransport *transport_;

  int listen_port_;
  int try_listen_cnt_;

  ObReqQueueThread queue_;
  ObReqProcessor* proc_map_[65536];
};

int Service::Deliver::deliver(ObRequest &req)
{
  return service_.deliver(req);
}

ObReqProcessor *Service::Translator::get_processor(ObRequest &req)
{
  return service_.translate(req);
}

int Service::init()
{
  int ret = OB_SUCCESS;
  queue_.set_qhandler(&qhandler_);
  if (OB_FAIL(queue_.get_thread().start())) {
  }
  if (OB_SUCC(ret)) {
    ObNetOptions opts;
    opts.rpc_io_cnt_ = 1;
    opts.mysql_io_cnt_ = 1;
    if (OB_SUCC(easy_.init(opts))) {
      ret = easy_.start();
    }
  }
  if (OB_SUCC(ret)) {
    ret = easy_.add_rpc_listen(listen_port_, handler_, transport_);
    while (try_listen_cnt_++ < 1000 && OB_FAIL(ret)) {
      ret = easy_.add_rpc_listen(++listen_port_, handler_, transport_);
    }
  }
  return ret;
}

ObReqProcessor *Service::translate(ObRequest &req)
{
  const ObRpcPacket &pkt
      = reinterpret_cast<const ObRpcPacket&>(req.get_packet());
  return proc_map_[pkt.get_pcode()];
}

int Service::deliver(ObRequest &req)
{
  static constexpr int64_t MAX_QUEUE_LEN = 65536;
  int ret = OB_SUCCESS;
  if (!queue_.push(&req, MAX_QUEUE_LEN)) {
    ret = OB_QUEUE_OVERFLOW;
  }
  return ret;
}

template <class Proc>
int Service::reg_processor(Proc *p)
{
  int ret = OB_SUCCESS;
  proc_map_[Proc::PCODE] = p;
  return ret;
}



}  // rpctesting

#endif /* RPC_TESTING_H */
