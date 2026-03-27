/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LOGSERVICE_OB_LOG_RESTORE_RPC_H_
#define OCEANBASE_LOGSERVICE_OB_LOG_RESTORE_RPC_H_
#include "rpc/frame/ob_req_transport.h"    // ObReqTransport
#include "ob_log_restore_rpc_define.h"     // ObRemoteFetchLog*
namespace oceanbase
{
namespace logservice
{
class ObLogResSvrRpc
{
  static const int64_t MAX_PROCESS_HANDLER_TIME = 100 * 1000L;
public:
  ObLogResSvrRpc() : inited_(false), proxy_() {}
  ~ObLogResSvrRpc() {}

  int init(const rpc::frame::ObReqTransport *transport);
  void destroy();
  int fetch_log(const ObAddr &server,
      const obrpc::ObRemoteFetchLogRequest &req,
      obrpc::ObRemoteFetchLogResponse &res);
private:
  bool inited_;
  obrpc::ObLogResSvrProxy proxy_;
};
} // namespace logservice
} // namespace oceanbase
#endif /* OCEANBASE_LOGSERVICE_OB_LOG_RESTORE_RPC_H_ */
