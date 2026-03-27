/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LOGSERVICE_LOG_REQUEST_HANDLER_
#define OCEANBASE_LOGSERVICE_LOG_REQUEST_HANDLER_

#include "lib/ob_errno.h"                   // OB_SUCCESS...
#include <stdint.h>

namespace oceanbase
{
namespace common
{
class ObAddr;
}
namespace palf
{
class IPalfEnvImpl;
class LogRequestHandler
{
public:
  LogRequestHandler(IPalfEnvImpl *palf_env_impl);
  ~LogRequestHandler();
  template <typename ReqType>
  int handle_request(const int64_t palf_id,
                     const common::ObAddr &server,
                     const ReqType &req);
  template <typename ReqType, typename RespType>
  int handle_sync_request(const int64_t palf_id,
                          const common::ObAddr &server,
                          const ReqType &req,
                          RespType &resp);
private:
  IPalfEnvImpl *palf_env_impl_;
};
} // end namespace palf
} // end namespace oceanbase

#endif
