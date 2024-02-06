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
#include "lib/net/ob_addr.h"                       // ObAddr
#include "log_rpc_proxy.h"                         // LogRpcProxyV2
#include "log_rpc_packet.h"                        // LogRpcPaket
#include "log_req.h"                               // LogPushReq...
#include "observer/ob_server_struct.h"             // GCTX
namespace oceanbase
{
using namespace common;
using namespace obrpc;
namespace palf
{
LogRpc::LogRpc() : rpc_proxy_(NULL),
                   opt_lock_(),
                   options_(),
                   tenant_id_(0),
                   cluster_id_(0),
                   is_inited_(false)
{
}

LogRpc::~LogRpc()
{
  destroy();
}

int LogRpc::init(const ObAddr &self,
                 const int64_t cluster_id,
                 const int64_t tenant_id,
                 rpc::frame::ObReqTransport *transport,
                 obrpc::ObBatchRpc *batch_rpc)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
  } else if (OB_FAIL(rpc_proxy_.init(transport, cluster_id))) {
    PALF_LOG(ERROR, "LogRpcProxyV2 init failed", K(ret));
  } else {
    self_ = self;
    tenant_id_ = tenant_id;
    batch_rpc_ = batch_rpc;
    cluster_id_ = cluster_id;
    is_inited_ = true;
    PALF_LOG(INFO, "LogRpc init success", K(tenant_id), K(self));
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

int LogRpc::update_transport_compress_options(const PalfTransportCompressOptions &compress_opt)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(opt_lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "LogRpc not inited");
  } else {
    options_ = compress_opt;
    PALF_LOG(INFO, "update_transport_compress_options success", K(compress_opt));
  }
  return ret;
}

const PalfTransportCompressOptions& LogRpc::get_compress_opts() const
{
  ObSpinLockGuard guard(opt_lock_);
  return options_;
}

} // end namespace palf
} // end namespace oceanbase
