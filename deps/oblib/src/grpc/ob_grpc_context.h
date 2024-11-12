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
#ifndef OCEANBASE_GRPC_CONTEXT_H_
#define OCEANBASE_GRPC_CONTEXT_H_

#include <chrono>
#include "lib/net/ob_addr.h"
#include "lib/ob_define.h"
#include <grpcpp/grpcpp.h>

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

#define GRPC_CALL(grpc_client, func, args...) \
  ({                                                                                              \
    int call_ret = oceanbase::OB_SUCCESS;                                                         \
    lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(common::OB_SERVER_TENANT_ID, "grpc")); \
    int64_t start_ts = oceanbase::ObTimeUtility::current_time();                                  \
    ClientContext context;                                                                        \
    call_ret = grpc_client.ctx_.set_grpc_context(context, start_ts);                              \
    if (OB_LIKELY(oceanbase::OB_SUCCESS == call_ret)) {                                           \
      Status status = grpc_client.stub_->func(&context, args);                                    \
      call_ret = grpc_client.ctx_.translate_error(status, start_ts, #func);                       \
    }                                                                                             \
    call_ret;                                                                                     \
  })

#define GRPC_CALL_TIMEOUT(grpc_client, timeout, func, args...) \
  ({                                                                                              \
    int call_ret = oceanbase::OB_SUCCESS;                                                         \
    lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(common::OB_SERVER_TENANT_ID, "grpc")); \
    int64_t start_ts = oceanbase::ObTimeUtility::current_time();                                  \
    ClientContext context;                                                                        \
    call_ret = grpc_client.ctx_.set_grpc_context(context, start_ts, timeout);                     \
    if (OB_LIKELY(oceanbase::OB_SUCCESS == call_ret)) {                                           \
      Status status = grpc_client.stub_->func(&context, args);                                    \
      call_ret = grpc_client.ctx_.translate_error(status, start_ts, #func);                       \
    }                                                                                             \
    call_ret;                                                                                     \
  })

namespace oceanbase
{
namespace obgrpc
{
class ObGrpcContext {
public:
  static const int64_t MAX_RPC_TIMEOUT = 9000 * 1000;
  static const int64_t REPORT_COUNT_INTERVAL = 2000;
  ObGrpcContext();
  int init(const ObAddr &addr, int64_t timeout, const int64_t src_cluster_id, const uint64_t tenant_id);
  int set_grpc_context(ClientContext &context, const int64_t start_ts);
  int set_grpc_context(ClientContext &context, const int64_t start_ts, const int64_t timeout);
  int translate_error(const Status &status, const int64_t start_ts, const char *func_name);
  const static uint32_t VERSION = 1;
private:
  int set_grpc_context_(ClientContext &context, const int64_t start_ts, const int64_t timeout);
public:
  ObAddr dst_;
  int64_t timeout_;
  uint64_t tenant_id_;
  int64_t src_cluster_id_;
  struct Statistics {
    Statistics(): send_cnt_(0), failed_cnt_(0), wait_time_(0) {}
    uint64_t send_cnt_;
    uint64_t failed_cnt_;
    uint64_t wait_time_;
  } statistics_info;
};

template <typename Service>
class ObGrpcClient {
public:
  ObGrpcClient() {};
  int init(const ObAddr& addr, int64_t timeout, const int64_t src_cluster_id, const uint64_t tenant_id);
  ObGrpcContext ctx_;
  std::shared_ptr<grpc::ChannelInterface> channel_;
  std::unique_ptr<typename Service::Stub> stub_;
};

template <typename Service>
int ObGrpcClient<Service>::init(const ObAddr& addr, int64_t timeout, const int64_t src_cluster_id, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  char addr_str[common::MAX_IP_PORT_LENGTH] = {0};
  if (OB_FAIL(ctx_.init(addr, timeout, src_cluster_id, tenant_id))) {
    RPC_LOG(WARN, "grpc ctx init failed", K(addr));
  } else if (OB_FAIL(addr.ip_port_to_string(addr_str, sizeof(addr_str)))) {
    RPC_LOG(WARN, "translate addr failed", K(addr));
  } else {
    grpc::ChannelArguments channel_args;
    channel_args.SetInt(GRPC_ARG_USE_LOCAL_SUBCHANNEL_POOL, 1);
    channel_args.SetInt(GRPC_ARG_MAX_RECONNECT_BACKOFF_MS, 1000);
    channel_args.SetInt(GRPC_ARG_MIN_RECONNECT_BACKOFF_MS, 1000);
    channel_ = grpc::CreateCustomChannel(addr_str, grpc::InsecureChannelCredentials(), channel_args);
    stub_ = Service::NewStub(channel_);
  }
  return ret;
}

} // end of namespace obgrpc
} // end of namespace oceanbase

#endif /* OCEANBASE_GRPC_CONTEXT_H_ */
