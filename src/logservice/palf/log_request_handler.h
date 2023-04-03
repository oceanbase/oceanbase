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
