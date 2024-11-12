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
#include <stdio.h>
#include <iostream>
#include <memory>
#include <string>
#include <grpcpp/grpcpp.h>
#include <sys/syscall.h>
#include <unistd.h>
#include "lib/ob_running_mode.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/serialization.h"
#include "lib/net/ob_net_util.h"
#include "share/config/ob_server_config.h"
#include "grpc/ob_grpc_context.h"
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "grpc/newlogstorepb.grpc.pb.h"
#include "grpc/ob_grpc_keepalive.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::Status;
using grpc::StatusCode;
using grpc::health::v1::Health;
using grpc::health::v1::HealthCheckRequest;
using grpc::health::v1::HealthCheckResponse;

namespace oceanbase {
namespace obgrpc {

ObGrpcContext::ObGrpcContext() : dst_(), timeout_(MAX_RPC_TIMEOUT),
        tenant_id_(common::OB_SERVER_TENANT_ID), src_cluster_id_(common::OB_INVALID_CLUSTER_ID) {
}

int ObGrpcContext::init(const ObAddr &addr, int64_t timeout, const int64_t src_cluster_id, const uint64_t tenant_id) {
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!addr.is_valid())) {
    RPC_LOG(WARN, "invalid addr", K(addr));
    ret = OB_INVALID_ARGUMENT;
  } else {
    dst_ = addr;
    timeout_ = timeout;
    tenant_id_ = tenant_id;
    src_cluster_id_ = src_cluster_id;
  }
  return ret;
}

int ObGrpcContext::set_grpc_context(ClientContext &context, const int64_t start_ts)
{
  return set_grpc_context_(context, start_ts, timeout_);
}

int ObGrpcContext::set_grpc_context(ClientContext &context, const int64_t start_ts, const int64_t timeout)
{
  return set_grpc_context_(context, start_ts, timeout);
}

int ObGrpcContext::translate_error(const Status &status, const int64_t start_ts, const char *func_name)
{
  int ret = OB_SUCCESS;
  StatusCode err_code = status.error_code();
  int64_t send_cnt = ATOMIC_AAF(&statistics_info.send_cnt_, 1);
  if (OB_UNLIKELY(StatusCode::OK != err_code)) {
    if (StatusCode::DEADLINE_EXCEEDED == err_code) {
      ret = OB_TIMEOUT;
    } else if (StatusCode::NOT_FOUND == err_code) {
      ret = OB_NOT_SUPPORTED;
    } else if (StatusCode::CANCELLED == err_code) {
      ret = OB_CANCELED;
    } else {
      ret = OB_RPC_SEND_ERROR;
    }
    ATOMIC_INC(&statistics_info.failed_cnt_);
    char msg[64];
    std::snprintf(msg, sizeof(msg), "%s", status.error_message().c_str());
    RPC_LOG(WARN, "grpc call faild", K(func_name), K(err_code), K(msg), K(dst_), K(timeout_));
  }
  int64_t end_ts = oceanbase::ObTimeUtility::current_time();
  int64_t wait_time = ATOMIC_FAA(&statistics_info.wait_time_, end_ts - start_ts);
  if (OB_UNLIKELY(send_cnt % REPORT_COUNT_INTERVAL == 0)) {
    int64_t failed_cnt = ATOMIC_LOAD(&statistics_info.failed_cnt_);
    int64_t avg_cost = wait_time / REPORT_COUNT_INTERVAL;
    RPC_LOG(INFO, "[grpc report]", KP(this), K(dst_), K(send_cnt), K(failed_cnt), K(wait_time), K(avg_cost));
    ATOMIC_FAA(&statistics_info.wait_time_, -wait_time);
  }
  return ret;
}

int ObGrpcContext::set_grpc_context_(ClientContext &context, const int64_t start_ts, const int64_t timeout)
{
  int ret = OB_SUCCESS;
  if (get_grpc_ka_instance().in_black(dst_)) {
    ret = OB_RPC_POST_ERROR;
    RPC_LOG(WARN, "check_blacklist failed", K_(dst));
  } else {
    char str_buf[512] = {0};
    char trace_id_buf[OB_MAX_TRACE_ID_BUFFER_SIZE] = {'\0'};
    const char* trace_id_str = NULL;
    const uint64_t* trace_id = common::ObCurTraceId::get();
    if (0 == trace_id[0]) {
      common::ObCurTraceId::TraceId temp;
      temp.init(obrpc::ObRpcProxy::myaddr_);
      temp.to_string(trace_id_buf, sizeof(trace_id_buf));
      trace_id_str = trace_id_buf;
    } else {
      trace_id_str = common::ObCurTraceId::get_trace_id_str(trace_id_buf, sizeof(trace_id_buf));
    }
    snprintf(str_buf, sizeof(str_buf), "%X,%s,%lX,%lX,%lX", VERSION, trace_id_str,
              tenant_id_, src_cluster_id_, start_ts);
    context.AddMetadata("custom-header", str_buf);
    int64_t abs_timeout_us = start_ts + timeout;
    std::chrono::microseconds ts(abs_timeout_us);
    std::chrono::time_point<std::chrono::system_clock> tp(ts);
    context.set_deadline(tp);
  }
  return ret;
}

ObGrpcKeepAliveClient& get_grpc_ka_instance()
{
  static ObGrpcKeepAliveClient the_one;
  return the_one;
}

void ObGrpcKeepAliveClient::try_check_status()
{
  for (int i = 0; i < MAX_LOGSTORE_COUNT; i++) {
    LogStoreCheckClient *&client = client_arr_[i];
    if (NULL != client) {
      client->try_send();
      client->try_receive();
    }
  }
}

int ObGrpcKeepAliveClient::in_black(ObAddr &addr, bool &in_blacklist, int64_t &last_active_time)
{
  int ret = OB_SUCCESS;
  LogStoreCheckClient *&client = client_arr_[0]; //there is only one logstore address in the observer in LOgStoreV1
  if (OB_UNLIKELY(!addr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    RPC_LOG(WARN, "grpc addr invalid");
  } else if (!client) {
    void *buffer = ob_malloc(sizeof(LogStoreCheckClient), "LogstoreCheck");
    if (NULL == buffer) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      RPC_LOG(ERROR, "alloc LogStoreCheckClient failed");
    } else {
      LogStoreCheckClient *new_client = new(buffer)LogStoreCheckClient();
      if (OB_FAIL(new_client->init(addr))) {
        RPC_LOG(ERROR, "init LogStoreCheckClient failed");
        new_client->~LogStoreCheckClient();
        ob_free(buffer);
      } else {
        LogStoreCheckClient *old_client = ATOMIC_VCAS(&client, NULL, new_client);
        if (NULL != old_client) {
          new_client->~LogStoreCheckClient();
          ob_free(buffer);
        } else {
          RPC_LOG(INFO, "add new logstore check client", K(addr));
        }
      }
    }
  }
  if (OB_LIKELY(OB_SUCCESS == ret)) {
    if (OB_UNLIKELY(0 != addr.compare(client->dst_))) {
      ret = OB_ERR_UNEXPECTED;
      RPC_LOG(ERROR, "logstore addr should not changed when running", K(addr));
    } else {
      in_blacklist = ATOMIC_LOAD(&client->in_black_);
      last_active_time = client->last_recv_ts_;
    }
  }
  return ret;
}

bool ObGrpcKeepAliveClient::in_black(ObAddr& addr)
{
  bool bret = false;
  int64_t last_active_time = 0;
  IGNORE_RETURN in_black(addr, bret, last_active_time);
  return bret;
}

void ObGrpcKeepAliveClient::dump_status()
{
  for (int i = 0; i < MAX_LOGSTORE_COUNT; i++) {
    LogStoreCheckClient *&client = client_arr_[i];
    if (NULL != client) {
      RPC_LOG(INFO, "dump grpc dest status", K(*client));
    }
  }
}

int ObGrpcKeepAliveClient::LogStoreCheckClient::init(const ObAddr& addr)
{
  int ret = OB_SUCCESS;
  char addr_str[common::MAX_IP_PORT_LENGTH] = {0};
  if (!addr.is_valid()) {
    RPC_LOG(WARN, "invalid addr", K(addr));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(addr.ip_port_to_string(addr_str, sizeof(addr_str)))) {
    RPC_LOG(WARN, "translate addr failed", K(addr));
  } else {
    dst_ = addr;
    last_send_ts_ = 0;
    last_recv_ts_ = 0;
    grpc::ChannelArguments channel_args;
    channel_args.SetInt(GRPC_ARG_USE_LOCAL_SUBCHANNEL_POOL, 1);
    channel_args.SetInt(GRPC_ARG_MAX_RECONNECT_BACKOFF_MS, 1000);
    channel_args.SetInt(GRPC_ARG_MIN_RECONNECT_BACKOFF_MS, 1000);
    stub_ = Health::NewStub(grpc::CreateCustomChannel(addr_str, grpc::InsecureChannelCredentials(), channel_args));
    RPC_LOG(INFO, "LogStoreCheckClient init success", K(addr));
  }
  return ret;
}

void ObGrpcKeepAliveClient::LogStoreCheckClient::try_send()
{
  int ret = OB_SUCCESS;
  if (wait_resp_) {
    // still wait for response, not to send repeatly
  } else {
    int64_t send_ts = ObTimeUtility::current_time();
    int64_t abs_timeout_us = send_ts + RESP_CREDIBLE_TIMEOUT;
    std::chrono::microseconds ts(abs_timeout_us);
    std::chrono::time_point<std::chrono::system_clock> tp(ts);
    context_.set_deadline(tp);
    std::unique_ptr<grpc::ClientAsyncResponseReader<HealthCheckResponse>> async_rpc(
      stub_->AsyncCheck(&context_, request_, &cq_));
    async_rpc->Finish(&response_, &status_, (void*)1);
    wait_resp_ = true;
    last_send_ts_ = send_ts;
  }
}

void ObGrpcKeepAliveClient::LogStoreCheckClient::try_receive()
{
  int ret = OB_SUCCESS;
  if (wait_resp_) {
    void* got_tag = NULL;
    bool ok = false;
    gpr_timespec deadline = gpr_now(GPR_CLOCK_REALTIME);
    const CompletionQueue::NextStatus status = cq_.AsyncNext(&got_tag, &ok, deadline);
    int64_t current_time = ObTimeUtility::current_time();
    if (CompletionQueue::GOT_EVENT == status) {
      bool in_black_old = ATOMIC_LOAD(&in_black_);
      if (status_.ok()) {
        // keepalive success
        if (in_black_old) {
          RPC_LOG(INFO, "logstore white wash", K(dst_));
          ATOMIC_STORE(&in_black_, false);
        }
        last_recv_ts_ = current_time;
      } else if (current_time - last_recv_ts_ > RESP_CREDIBLE_TIMEOUT) {
        if (!in_black_old) {
          int err_code = status_.error_code();
          RPC_LOG(WARN, "logstore mark black", K(dst_), K(err_code), K_(last_recv_ts));
          ATOMIC_STORE(&in_black_, true);
        }
      }

      // reset grpc context
      context_.~ClientContext();
      new (&context_) ClientContext();
      response_.Clear();
      wait_resp_ = false;
    } else if (CompletionQueue::TIMEOUT != status) {
      ret = OB_ERR_UNEXPECTED;
      RPC_LOG(WARN, "The completion queue has been shutdown and fully-drained", K(status));
    }
  }
}

} // end of namespace obgrpc
} // end of namespace oceanbase
