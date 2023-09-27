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

#ifndef OCEANBASE_PALF_CLUSTER_LOG_REQUEST_HANDLER_
#define OCEANBASE_PALF_CLUSTER_LOG_REQUEST_HANDLER_

#include "lib/ob_errno.h"                           // OB_SUCCESS...
#include "logservice/ob_reporter_adapter.h"         // ObLogReporterAdapter
#include "logservice/palf_handle_guard.h"           // palf::PalfHandleGuard
#include "palf_cluster_rpc_req.h"                         // Req...
#include "mittest/palf_cluster/logservice/log_service.h"

namespace oceanbase
{

namespace palf
{
class PalfEnv;
}

namespace obrpc
{
class PalfClusterRpcProxy;
}

namespace palfcluster
{
class ObLogHandler;
class ObLogReplayService;

class LogRequestHandler
{
public:
  LogRequestHandler();
  ~LogRequestHandler();
  template <typename ReqType, typename RespType>
  int handle_sync_request(const ReqType &req, RespType &resp);
  template <typename ReqType>
  int handle_request(const ReqType &req);
  void set_log_service(palfcluster::LogService *log_service) { log_service_ = log_service; }
private:
  int get_self_addr_(common::ObAddr &self) const;
  int get_rpc_proxy_(obrpc::PalfClusterRpcProxy *&rpc_proxy) const;
  palfcluster::LogService *log_service_;
};

class ConfigChangeCmdHandler{
public:
  explicit ConfigChangeCmdHandler(palf::PalfHandle *palf_handle)
  {
    if (NULL != palf_handle) {
      palf_handle_ = palf_handle;
    }
  }
  ~ConfigChangeCmdHandler()
  {
    palf_handle_ = NULL;
  }
private:
  palf::PalfHandle *palf_handle_;
};
} // end namespace logservice
} // end namespace oceanbase

#endif
