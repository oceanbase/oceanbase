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

#define USING_LOG_PREFIX RPC
#include <iostream>
#include "rpc/ob_request.h"
#include "rpc/frame/ob_net_easy.h"
#include "rpc/frame/ob_req_deliver.h"
#include "rpc/obrpc/ob_rpc_handler.h"
#include "rpc/obrpc/ob_rpc_packet.h"
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "rpc/obrpc/ob_rpc_processor.h"
#include "common/data_buffer.h"

using namespace oceanbase;
using namespace oceanbase::lib;
using namespace oceanbase::rpc;
using namespace oceanbase::rpc::frame;
using namespace oceanbase::obrpc;
using namespace oceanbase::common;
using namespace std;
using namespace oceanbase::obsys;

#define PORT 3124
#define IO_CNT 1
#define SEND_CNT 1
#define PROC_TH_CNT 4

int client_count = 1;
int io_count = 1;
int worker_count = 1;


class TestProxy
    : public ObRpcProxy
{
public:
  DEFINE_TO(TestProxy);

  RPC_S(@PR5 test, OB_TEST_PCODE);
  RPC_AP(@PR5 test2, OB_TEST2_PCODE);
};

int64_t total_fly_time = 0;
int64_t total_net_time = 0;
int64_t total_count = 0;
int64_t total_send_count = 0;

class MyProcessor
    : public TestProxy::Processor<OB_TEST_PCODE>
{
public:
  int process()
  {
    int ret = OB_SUCCESS;
    const int64_t fly_time
        = get_receive_timestamp() - get_send_timestamp();
    const int64_t net_time
        = get_run_timestamp() - get_receive_timestamp();

    ATOMIC_FAA(&total_fly_time, fly_time);
    ATOMIC_FAA(&total_net_time, net_time);
    ATOMIC_FAA(&total_count, 1);

    return ret;
  }
};

class QHandler :
    public ObiReqQHandler
{
public:
  bool handlePacketQueue(ObRequest *req, void *args)
  {
    UNUSED(args);
    MyProcessor p;
    p.set_ob_request(*req);
    p.run();

    return true;
  }

  int onThreadCreated(obsys::CThread*) { return OB_SUCCESS; }
  int onThreadDestroy(obsys::CThread*) { return OB_SUCCESS; }
};

class ObTestDeliver
    : public rpc::frame::ObReqQDeliver
{
public:
  ObTestDeliver()
      : ObReqQDeliver(qhandler_)
  {
  }

  int init() {
    int ret = OB_SUCCESS;

    queue_.get_thread().set_thread_count(worker_count);
    queue_.set_qhandler(&qhandler_);

    return ret;
  }

  int deliver(rpc::ObRequest &req)
  {
    int ret = OB_SUCCESS;

    req.get_request()->opacket = const_cast<rpc::ObPacket*>(&req.get_packet());
    req.set_request_rtcode(EASY_OK);
    easy_request_wakeup(req.get_request());

    const int64_t fly_time
        = req.get_receive_timestamp() - req.get_send_timestamp();
    // const int64_t net_time
    //     = req.get_run_timestamp() - req.get_receive_timestamp();

    ATOMIC_FAA(&total_fly_time, fly_time);
    // ATOMIC_FAA(&total_net_time, net_time);
    ATOMIC_FAA(&total_count, 1);

    // if (!queue_->push(&req, 100000)) {
    //   ret = OB_ERR_UNEXPECTED;
    // }

    return ret;
  }

  void stop() {};

private:
  ObReqQueueThread queue_;
  QHandler qhandler_;
};

class Client
    : public Threads
{
public:
  Client(const TestProxy &proxy, int th_cnt, ObAddr dst, uint32_t sleep_time)
      : Threads(th_cnt),
        proxy_(proxy), dst_(dst), sleep_time_(sleep_time)
  {
  }

  void run(int64_t)
  {
    while (!has_set_stop()) {
      proxy_.to(dst_).test2(NULL);
      ATOMIC_FAA(&total_send_count, 1);
      ::usleep(sleep_time_);
    }
  }

private:
  const TestProxy &proxy_;
  const ObAddr dst_;
  const uint32_t sleep_time_;
};

int main(int argc, char *argv[])
{
  UNUSED(argc);
  UNUSED(argv);
  int ret = OB_SUCCESS;

  if (argc > 1) {
    client_count = atoi(argv[1]);
  }
  if (argc > 2) {
    io_count = atoi(argv[2]);
  }
  if (argc > 3) {
    worker_count = atoi(argv[3]);
  }

  rpc::frame::ObNetEasy net_;
  ObTestDeliver server;
  ObRpcHandler handler_(server);
  rpc::frame::ObReqTransport *transport_;

  if (OB_FAIL(server.init())) {
    exit(1);
  }

  ObNetOptions opts;
  opts.rpc_io_cnt_ = io_count;
  net_.init(opts);

  if (OB_FAIL(net_.add_rpc_listen(PORT, handler_, transport_))) {
    exit(1);
  }
  net_.start();

  TestProxy proxy;
  proxy.init(transport_);

  ObAddr dst(ObAddr::IPV4, "127.0.0.1", PORT);
  Client client(proxy, client_count, dst, 1);
  client.start();

  // ObAddr dst2(ObAddr::IPV4, "127.0.0.1", PORT+1);
  // Client client2(proxy, 1, dst2, 1);
  // client2.start();

  const int64_t start_ts = ObTimeUtility::current_time();
  sleep(10);
  const int64_t end_ts = ObTimeUtility::current_time();

  client.stop();
  client.wait();

  // client2.stop();
  // client2.wait();

  net_.stop();
  net_.wait();

  cout << endl
       << "==== RESULT ====" << endl
       << "total send: \t" << total_send_count << endl
       << "total proc: \t" << total_count << endl
       << "elapse sec: \t" << (end_ts - start_ts) / 1000000L << endl
       << "avg fly time: " << total_fly_time / (total_count+1) << endl
       << "avg net time: " << total_net_time / (total_count+1) << endl
       << endl;

  return 0;
}
