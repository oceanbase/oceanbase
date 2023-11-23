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

#ifndef OCEANBASE_LOGSERVICE_OB_LOG_REQUEST_HANDLER_
#define OCEANBASE_LOGSERVICE_OB_LOG_REQUEST_HANDLER_

#include "lib/ob_errno.h"                           // OB_SUCCESS...
#include "logservice/ob_reporter_adapter.h"         // ObLogReporterAdapter
#include "logservice/palf_handle_guard.h"           // palf::PalfHandleGuard
#include "ob_log_rpc_req.h"                         // Req...

namespace oceanbase
{

namespace palf
{
class PalfEnv;
}

namespace obrpc
{
class ObLogServiceRpcProxy;
}

namespace logservice
{
#ifdef OB_BUILD_ARBITRATION
class ObArbitrationService;
#endif
class ObLogFlashbackService;
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
private:
  int get_palf_handle_guard_(const int64_t palf_id, palf::PalfHandleGuard &palf_handle_guard) const;
#ifdef OB_BUILD_ARBITRATION
  int get_arb_service_(ObArbitrationService *&arb_service) const;
#endif
  int get_self_addr_(common::ObAddr &self) const;
  int get_rpc_proxy_(obrpc::ObLogServiceRpcProxy *&rpc_proxy) const;
  int get_flashback_service_(ObLogFlashbackService *&flashback_srv) const;
  int get_replay_service_(ObLogReplayService *&replay_srv) const;
  int get_log_handler_(const int64_t palf_id,
                       storage::ObLSHandle &ls_handle,
                       logservice::ObLogHandler *&log_handler) const;
  int change_access_mode_(const LogChangeAccessModeCmd &req);
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
  int handle_config_change_cmd(const LogConfigChangeCmd &req,
                               LogConfigChangeCmdResp &resp) const;
private:
  int get_reporter_(ObLogReporterAdapter *&reporter) const;
private:
  palf::PalfHandle *palf_handle_;
};
} // end namespace logservice
} // end namespace oceanbase

#endif
