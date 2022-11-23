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
#include "rpc/obrpc/ob_rpc_processor.h"
#include "logservice/palf/palf_env.h"                     // palf::PalfEnv

namespace oceanbase
{
namespace logservice
{
#define DEFINE_LOGSERVICE_SYNC_RPC_PROCESSOR(CLASS, PROXY, REQTYPE, RESPTYPE, PCODE) \
class CLASS: public                                                       \
      obrpc::ObRpcProcessor<PROXY::ObRpc<PCODE>>                          \
{                                                                         \
public:                                                                   \
  CLASS() {}                                                              \
  virtual ~CLASS() {}                                                     \
  int process()                                                           \
  {                                                                       \
    int ret = OB_SUCCESS;                                                 \
    palf::PalfEnv *palf_env = NULL;                                       \
    palf::PalfHandle palf_handle;                                         \
    const REQTYPE &req = arg_;                                            \
    const ObAddr server = req.src_;                                       \
    const int64_t palf_id = req.palf_id_;                                 \
    RESPTYPE &resp = result_;                                             \
    if (OB_ISNULL(palf_env) && OB_FAIL(logservice::__get_palf_env(rpc_pkt_->get_tenant_id(), palf_env))) {\
      PALF_LOG(WARN, "__get_palf_env failed", K(ret), KPC(rpc_pkt_));     \
    } else if (OB_FAIL(palf_env->open(palf_id, palf_handle))) {           \
      PALF_LOG(WARN, "open palf failed", K(ret), K(palf_id), KP(palf_env));\
    } else {                                                              \
      LogRequestHandler handler(&palf_handle);                            \
      ret = handler.handle_sync_request(palf_id, server, req, resp);      \
      PALF_LOG(TRACE, "Processor handle_sync_request success", K(ret), K(palf_id), K(req), K(resp)); \
      palf_env->close(palf_handle);                                       \
    }                                                                     \
    return ret;                                                           \
  }                                                                       \
}

int __get_palf_env(uint64_t tenant_id, palf::PalfEnv *&palf_env);


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
} // end namespace logservice
} // end namespace oceanbase


#endif