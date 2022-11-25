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

#include "log_rpc.h"
#include "lib/net/ob_addr.h"                          // ObAddr
#include "log_rpc_proxy.h"                         // LogRpcProxyV2
#include "log_rpc_packet.h"                        // LogRpcPaket
#include "log_req.h"                               // LogPushReq...

namespace oceanbase
{
using namespace common;
using namespace obrpc;
namespace palf
{
LogRpc::LogRpc() : rpc_proxy_(NULL),
                   is_inited_(false)
{
}

LogRpc::~LogRpc()
{
  destroy();
}

int LogRpc::init(const ObAddr &self, rpc::frame::ObReqTransport *transport)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
  } else if (OB_FAIL(rpc_proxy_.init(transport))) {
    PALF_LOG(ERROR, "LogRpcProxyV2 init failed", K(ret));
  } else {
    self_ = self;
    is_inited_ = true;
  }
  return ret;
}

void LogRpc::destroy()
{
  if (IS_INIT) {
    is_inited_ = false;
    rpc_proxy_.destroy();
    PALF_LOG(INFO, "LogRpc destroy success");
  }
}
} // end namespace palf
} // end namespace oceanbase
