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

#ifndef OCEANBASE_LIBOBCDC_OB_LOG_RPC_H_
#define OCEANBASE_LIBOBCDC_OB_LOG_RPC_H_

#include "lib/net/ob_addr.h"              // ObAddr
#include "rpc/obrpc/ob_net_client.h"      // ObNetClient
#include "rpc/obrpc/ob_rpc_packet.h"      // OB_LOG_OPEN_STREAM
#include "rpc/obrpc/ob_rpc_proxy.h"       // ObRpcProxy
#include "logservice/cdcservice/ob_cdc_req.h"
#include "logservice/cdcservice/ob_cdc_rpc_proxy.h"    // ObCdcProxy

#include "ob_log_utils.h"                 // _SEC_

namespace oceanbase
{
namespace libobcdc
{

// RPC interface
//
// all asynchronous rpc start with "async"
class IObLogRpc
{
public:
  virtual ~IObLogRpc() { }

  // Request start LSN by timestamp
  virtual int req_start_lsn_by_tstamp(const uint64_t tenant_id,
      const common::ObAddr &svr,
      obrpc::ObCdcReqStartLSNByTsReq &req,
      obrpc::ObCdcReqStartLSNByTsResp &resp,
      const int64_t timeout) = 0;

  // Get logs(GroupLogEntry) based on log stream
  // Asynchronous RPC
  virtual int async_stream_fetch_log(const uint64_t tenant_id,
      const common::ObAddr &svr,
      obrpc::ObCdcLSFetchLogReq &req,
      obrpc::ObCdcProxy::AsyncCB<obrpc::OB_LS_FETCH_LOG2> &cb,
      const int64_t timeout) = 0;

  // Get missing logs(LogEntry) based on log stream
  // Asynchronous RPC
  virtual int async_stream_fetch_missing_log(const uint64_t tenant_id,
      const common::ObAddr &svr,
      obrpc::ObCdcLSFetchMissLogReq &req,
      obrpc::ObCdcProxy::AsyncCB<obrpc::OB_LS_FETCH_MISSING_LOG> &cb,
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
  const char *const OB_CLIENT_SSL_SM_SIGN_CERT_FILE = "wallet/CS.cert.pem";
  const char *const OB_CLIENT_SSL_SM_SIGN_KEY_FILE = "wallet/CS.key.pem";
  const char *const OB_CLIENT_SSL_SM_ENC_CERT_FILE = "wallet/CE.cert.pem";
  const char *const OB_CLIENT_SSL_SM_ENC_KEY_FILE = "wallet/CE.key.pem";

public:
  ObLogRpc();
  virtual ~ObLogRpc();

  static void configure(const ObLogConfig &cfg);

public:
  int req_start_lsn_by_tstamp(const uint64_t tenant_id,
      const common::ObAddr &svr,
      obrpc::ObCdcReqStartLSNByTsReq &req,
      obrpc::ObCdcReqStartLSNByTsResp &resp,
      const int64_t timeout);

  int async_stream_fetch_log(const uint64_t tenant_id,
      const common::ObAddr &svr,
      obrpc::ObCdcLSFetchLogReq &req,
      obrpc::ObCdcProxy::AsyncCB<obrpc::OB_LS_FETCH_LOG2> &cb,
      const int64_t timeout);

  int async_stream_fetch_missing_log(const uint64_t tenant_id,
      const common::ObAddr &svr,
      obrpc::ObCdcLSFetchMissLogReq &req,
      obrpc::ObCdcProxy::AsyncCB<obrpc::OB_LS_FETCH_MISSING_LOG> &cb,
      const int64_t timeout);

public:
  int init(const int64_t io_thread_num);
  void destroy();
  int reload_ssl_config();

private:
  int init_client_id_();

private:
  bool                is_inited_;
  obrpc::ObNetClient  net_client_;
  uint64_t            last_ssl_info_hash_;
  int64_t             ssl_key_expired_time_;
  ObCdcRpcId          client_id_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogRpc);
};

}
}

#endif
