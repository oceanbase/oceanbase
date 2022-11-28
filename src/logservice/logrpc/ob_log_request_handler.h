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

#include "lib/ob_errno.h"                   // OB_SUCCESS...
#include "logservice/palf/palf_handle.h"                     // palf::PalfHandle
#include "logservice/ob_reporter_adapter.h"          // ObLogReporterAdapter
#include "ob_log_rpc_req.h"                     // Req...

namespace oceanbase
{
namespace logservice
{
class LogRequestHandler
{
public:
  LogRequestHandler(palf::PalfHandle *palf_handle);
  ~LogRequestHandler();
  template <typename ReqType, typename RespType>
  int handle_sync_request(const int64_t palf_id,
                          const common::ObAddr &server,
                          const ReqType &req,
                          RespType &resp);
private:
  palf::PalfHandle *palf_handle_;
  int64_t handle_request_print_time_;
};

class ConfigChangeCmdHandler{
public:
  explicit ConfigChangeCmdHandler(palf::PalfHandle *palf_handle)
  {
    if (NULL != palf_handle) {
      palf_handle_ = palf_handle;
    }
  }
  int handle_config_change_cmd(const LogConfigChangeCmd &req) const;
private:
  int get_reporter_(ObLogReporterAdapter *&reporter) const;
private:
  palf::PalfHandle *palf_handle_;
};
} // end namespace logservice
} // end namespace oceanbase

#endif
