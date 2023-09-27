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

#ifndef OCEANBASE_PALF_CLUSTER_RPC_PROCESSOR_H_
#define OCEANBASE_PALF_CLUSTER_RPC_PROCESSOR_H_

#include "palf_cluster_rpc_req.h"
#include "palf_cluster_request_handler.h"
#include "palf_cluster_rpc_proxy.h"
#include "lib/ob_errno.h"

namespace oceanbase
{
namespace palfcluster
{
class LogService;

#define DEFINE_LOGSERVICE_RPC_PROCESSOR(CLASS, PROXY, REQTYPE, PCODE)                                             \
class CLASS : public obrpc::ObRpcProcessor<PROXY::ObRpc<PCODE>>                                                   \
{                                                                                                                 \
public:                                                                                                           \
  CLASS() : filter_(NULL) {}                                                                                      \
  virtual ~CLASS() { filter_ = NULL; }                                                                            \
  int process()                                                                                                   \
  {                                                                                                               \
    int ret = OB_SUCCESS;                                                                                         \
    const REQTYPE &req = arg_;                                                                                    \
    const common::ObAddr server = req.src_;                                                                       \
    const uint64_t tenant_id = rpc_pkt_->get_tenant_id();                                                         \
    LogRequestHandler handler;                                                                                    \
    handler.set_log_service(log_service_);                                                                        \
    if (OB_UNLIKELY(NULL != filter_ && true == (*filter_)(server))) {                                             \
    } else if (OB_FAIL(handler.handle_request(req))) {                                                            \
      CLOG_LOG(WARN, "Processor handle_request failed", K(ret), K(req));                                          \
    } else {                                                                                                      \
      CLOG_LOG(INFO, "Processor handle_request success", K(ret), K(req));                                         \
    }                                                                                                             \
    return ret;                                                                                                   \
  }                                                                                                               \
  void set_filter(void *filter)                                                                                   \
  {                                                                                                               \
    filter_ = reinterpret_cast<ObFunction<bool(const ObAddr &src)> *>(filter);                                    \
  }                                                                                                               \
  void set_log_service(void *log_service)                                                                         \
  {                                                                                                               \
    log_service_ = reinterpret_cast<palfcluster::LogService *>(log_service);                                                 \
  }                                                                                                               \
private:                                                                                                          \
  ObFunction<bool(const ObAddr &src)> *filter_;                                                                   \
  palfcluster::LogService *log_service_;                                                                         \
}

DEFINE_LOGSERVICE_RPC_PROCESSOR(LogCreateReplicaCmdP,
                                obrpc::PalfClusterRpcProxy,
                                palfcluster::LogCreateReplicaCmd,
                                obrpc::OB_LOG_CREATE_REPLICA_CMD);

DEFINE_LOGSERVICE_RPC_PROCESSOR(LogSubmitLogP,
                                obrpc::PalfClusterRpcProxy,
                                palfcluster::SubmitLogCmd,
                                obrpc::OB_LOG_SUBMIT_LOG_CMD);

DEFINE_LOGSERVICE_RPC_PROCESSOR(LogSubmitLogRespP,
                                obrpc::PalfClusterRpcProxy,
                                SubmitLogCmdResp,
                                obrpc::OB_LOG_SUBMIT_LOG_CMD_RESP);
} // end namespace palfcluster
} // end namespace oceanbase


#endif
