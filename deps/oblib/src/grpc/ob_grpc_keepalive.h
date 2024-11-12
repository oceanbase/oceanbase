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
#include <grpcpp/grpcpp.h>
#include "lib/net/ob_addr.h"
#include "grpc/health.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::Status;
using grpc::StatusCode;
using grpc::health::v1::Health;
using grpc::health::v1::HealthCheckRequest;
using grpc::health::v1::HealthCheckResponse;
namespace oceanbase
{
namespace obgrpc
{

class ObGrpcKeepAliveClient {
public:
  ObGrpcKeepAliveClient()
  {
    memset(client_arr_, 0, sizeof(client_arr_));
  }
  int send_request();
  int receive_response();
  bool in_black(ObAddr &addr);
  int in_black(ObAddr &addr, bool &in_blacklist, int64_t &last_active_time);
  void dump_status();
  class LogStoreCheckClient {
  public:
    enum {
      RESP_CREDIBLE_TIMEOUT = 3000000,
      LOGSTORE_EXPIRED_TIME = 600L * 1000 * 1000
    };
    LogStoreCheckClient() : dst_(), wait_resp_(false),
                            last_send_ts_(0), last_recv_ts_(0),
                            in_black_(false)
                            {}
    int init(const ObAddr& addr);
    void update_dst();
    void try_send();
    void try_receive();
    ObAddr dst_;
    bool wait_resp_;
    int64_t last_send_ts_;
    int64_t last_recv_ts_;
    bool in_black_ CACHE_ALIGNED;
    TO_STRING_KV(K_(dst), K_(wait_resp), K_(last_send_ts), K_(last_recv_ts), K_(in_black));
  private:
    std::unique_ptr<Health::Stub> stub_;
    HealthCheckRequest request_;
    HealthCheckResponse response_;
    ClientContext context_;
    grpc::CompletionQueue cq_;
    Status status_;
  };
  void try_check_status();
private:
  static const int MAX_LOGSTORE_COUNT = 1;
  LogStoreCheckClient *client_arr_[MAX_LOGSTORE_COUNT];
};

ObGrpcKeepAliveClient& get_grpc_ka_instance();


} // end of namespace obgrpc
} // end of namespace oceanbase