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

#include "ob_log_restore_rpc.h"
#include "lib/ob_errno.h"

namespace oceanbase
{
namespace logservice
{
int ObLogResSvrRpc::init(const rpc::frame::ObReqTransport *transport)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(transport)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(proxy_.init(transport))) {
    CLOG_LOG(WARN, "init rpc proxy failed", K(ret), K(transport));
  } else {
    inited_ = true;
  }
  return ret;
}

void ObLogResSvrRpc::destroy()
{
  inited_ = false;
  proxy_.destroy();
}

int ObLogResSvrRpc::fetch_log(const ObAddr &server,
    const obrpc::ObRemoteFetchLogRequest &req,
    obrpc::ObRemoteFetchLogResponse &res)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(! server.is_valid() || ! req.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    ret = proxy_.to(server)
          .trace_time(true)
          .max_process_handler_time(MAX_PROCESS_HANDLER_TIME)
          .by(req.tenant_id_)
          .dst_cluster_id(1)
          .remote_fetch_log(req, res);
  }
  return ret;
}

} // namespace logservice
} // namespace oceanbase
