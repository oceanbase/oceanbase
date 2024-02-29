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

#ifndef OCEANBASE_LOGSERVICE_OB_LOG_RPC_PROCESSOR_H_
#define OCEANBASE_LOGSERVICE_OB_LOG_RPC_PROCESSOR_H_

#include "ob_log_rpc_req.h"
#include "ob_log_request_handler.h"
#include "ob_log_rpc_proxy.h"
#include "lib/ob_errno.h"

namespace oceanbase
{
namespace logservice
{
class ObArbitrationService;

#define DEFINE_LOGSERVICE_SYNC_RPC_PROCESSOR(CLASS, PROXY, REQTYPE, RESPTYPE, PCODE)                              \
class CLASS: public                                                                                               \
      obrpc::ObRpcProcessor<PROXY::ObRpc<PCODE>>                                                                  \
{                                                                                                                 \
public:                                                                                                           \
  CLASS() : filter_(NULL) {}                                                                                      \
  virtual ~CLASS() { filter_ = NULL; }                                                                            \
  int process()                                                                                                   \
  {                                                                                                               \
    int ret = OB_SUCCESS;                                                                                         \
    const REQTYPE &req = arg_;                                                                                    \
    const common::ObAddr server = req.src_;                                                                       \
    RESPTYPE &resp = result_;                                                                                     \
    const uint64_t tenant_id = rpc_pkt_->get_tenant_id();                                                         \
    LogRequestHandler handler;                                                                                    \
    if (tenant_id != MTL_ID()) {                                                                                  \
      ret = OB_ERR_UNEXPECTED;                                                                                    \
      CLOG_LOG(ERROR, "mtl id not match", K_(tenant_id), K(MTL_ID()), K(ret));                                    \
    } else if (OB_UNLIKELY(NULL != filter_ && true == (*filter_)(server))) {                                      \
      CLOG_LOG(INFO, "need filter this packet", K(req));                                                          \
    } else if (OB_FAIL(handler.handle_sync_request(req, resp))) {                                                 \
      CLOG_LOG(WARN, "Processor handle_sync_request failed", K(ret), K(req), K(resp));                            \
    } else {                                                                                                      \
      CLOG_LOG(INFO, "Processor handle_sync_request success", K(ret), K(req), K(resp));                           \
    }                                                                                                             \
    return ret;                                                                                                   \
  }                                                                                                               \
  void set_filter(void *filter)                                                                                   \
  {                                                                                                               \
    filter_ = reinterpret_cast<ObFunction<bool(const ObAddr &src)> *>(filter);                                    \
  }                                                                                                               \
private:                                                                                                          \
  ObFunction<bool(const ObAddr &src)> *filter_;                                                                   \
}

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
    if (tenant_id != MTL_ID()) {                                                                                  \
      ret = OB_ERR_UNEXPECTED;                                                                                    \
      CLOG_LOG(ERROR, "mtl id not match", K_(tenant_id), K(MTL_ID()), K(ret));                                    \
    } else if (OB_UNLIKELY(NULL != filter_ && true == (*filter_)(server))) {                                      \
      CLOG_LOG(INFO, "need filter this packet", K(req));                                                          \
    } else if (OB_FAIL(handler.handle_request(req))) {                                                            \
      CLOG_LOG(WARN, "Processor handle_request failed", K(ret), K(req));                                          \
    } else {                                                                                                      \
      CLOG_LOG(TRACE, "Processor handle_request success", K(ret), K(req));                                         \
    }                                                                                                             \
    return ret;                                                                                                   \
  }                                                                                                               \
  void set_filter(void *filter)                                                                                   \
  {                                                                                                               \
    filter_ = reinterpret_cast<ObFunction<bool(const ObAddr &src)> *>(filter);                                    \
  }                                                                                                               \
private:                                                                                                          \
  ObFunction<bool(const ObAddr &src)> *filter_;                                                                   \
}

DEFINE_LOGSERVICE_SYNC_RPC_PROCESSOR(LogMembershipChangeP,
                                     obrpc::ObLogServiceRpcProxy,
                                     LogConfigChangeCmd,
                                     LogConfigChangeCmdResp,
                                     obrpc::OB_LOG_CONFIG_CHANGE_CMD);

DEFINE_LOGSERVICE_SYNC_RPC_PROCESSOR(LogGetPalfStatReqP,
                                     obrpc::ObLogServiceRpcProxy,
                                     LogGetPalfStatReq,
                                     LogGetPalfStatResp,
                                     obrpc::OB_LOG_GET_PALF_STAT);

#ifdef OB_BUILD_ARBITRATION
DEFINE_LOGSERVICE_RPC_PROCESSOR(LogServerProbeP,
                                obrpc::ObLogServiceRpcProxy,
                                LogServerProbeMsg,
                                obrpc::OB_LOG_ARB_PROBE_MSG);
#endif

DEFINE_LOGSERVICE_RPC_PROCESSOR(LogChangeAccessModeP,
                                obrpc::ObLogServiceRpcProxy,
                                LogChangeAccessModeCmd,
                                obrpc::OB_LOG_CHANGE_ACCESS_MODE_CMD);

DEFINE_LOGSERVICE_RPC_PROCESSOR(LogFlashbackMsgP,
                                obrpc::ObLogServiceRpcProxy,
                                LogFlashbackMsg,
                                obrpc::OB_LOG_FLASHBACK_CMD);
} // end namespace logservice
} // end namespace oceanbase


#endif
