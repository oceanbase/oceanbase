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

#ifndef OCEANBASE_LIBOBLOG_OB_LOG_RPC_H_
#define OCEANBASE_LIBOBLOG_OB_LOG_RPC_H_

#include "lib/net/ob_addr.h"              // ObAddr
#include "rpc/obrpc/ob_net_client.h"      // ObNetClient
#include "clog/ob_log_external_rpc.h"     // obrpc
#include "rpc/obrpc/ob_rpc_packet.h"      // OB_LOG_OPEN_STREAM
#include "rpc/obrpc/ob_rpc_proxy.h"       // ObRpcProxy

#include "ob_log_utils.h"                 // _SEC_

namespace oceanbase
{
namespace liboblog
{

// RPC interface
//
// all asynchronous rpc start with "async"
class IObLogRpc
{
public:
  virtual ~IObLogRpc() { }

  // reuest start log id by timestamp
  virtual int req_start_log_id_by_tstamp(const common::ObAddr &svr,
      const obrpc::ObLogReqStartLogIdByTsRequestWithBreakpoint& req,
      obrpc::ObLogReqStartLogIdByTsResponseWithBreakpoint& res,
      const int64_t timeout) = 0;

  // reuest heartbeat from partition leader
  virtual int req_leader_heartbeat(const common::ObAddr &svr,
      const obrpc::ObLogLeaderHeartbeatReq &req,
      obrpc::ObLogLeaderHeartbeatResp &res,
      const int64_t timeout) = 0;

  // open stream with synchronous RPC
  virtual int open_stream(const common::ObAddr &svr,
      const obrpc::ObLogOpenStreamReq &req,
      obrpc::ObLogOpenStreamResp &resp,
      const int64_t timeout) = 0;

  // get logs based on log stream
  // Asynchronous RPC
  virtual int async_stream_fetch_log(const common::ObAddr &svr,
      const obrpc::ObLogStreamFetchLogReq &req,
      obrpc::ObLogExternalProxy::AsyncCB<obrpc::OB_LOG_STREAM_FETCH_LOG> &cb,
      const int64_t timeout) = 0;
};

//////////////////////////////////////////// ObLogRpc //////////////////////////////////////

class ObLogConfig;
class ObLogRpc : public IObLogRpc
{
public:
  static int64_t g_rpc_process_handler_time_upper_limit;
  const char *const OB_CLIENT_SSL_CA_FILE = "wallet/ca.pem";
  const char *const OB_CLIENT_SSL_CERT_FILE = "wallet/client-cert.pem";
  const char *const OB_CLIENT_SSL_KEY_FILE = "wallet/client-key.pem";

public:
  ObLogRpc();
  virtual ~ObLogRpc();

  static void configure(const ObLogConfig &cfg);

public:
  int req_start_log_id_by_tstamp(const common::ObAddr &svr,
      const obrpc::ObLogReqStartLogIdByTsRequestWithBreakpoint& req,
      obrpc::ObLogReqStartLogIdByTsResponseWithBreakpoint& res,
      const int64_t timeout);

  int req_leader_heartbeat(const common::ObAddr &svr,
      const obrpc::ObLogLeaderHeartbeatReq &req,
      obrpc::ObLogLeaderHeartbeatResp &res,
      const int64_t timeout);

  int open_stream(const common::ObAddr &svr,
      const obrpc::ObLogOpenStreamReq &req,
      obrpc::ObLogOpenStreamResp &resp,
      const int64_t timeout);

  int async_stream_fetch_log(const common::ObAddr &svr,
      const obrpc::ObLogStreamFetchLogReq &req,
      obrpc::ObLogExternalProxy::AsyncCB<obrpc::OB_LOG_STREAM_FETCH_LOG> &cb,
      const int64_t timeout);

public:
  int init(const uint64_t tenant_id, const int64_t io_thread_num);
  void destroy();
  int reload_ssl_config();

private:
  bool                inited_;
  uint64_t            tenant_id_;
  obrpc::ObNetClient  net_client_;
  uint64_t            last_ssl_info_hash_;
  int64_t             ssl_key_expired_time_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogRpc);
};

}
}

#endif
