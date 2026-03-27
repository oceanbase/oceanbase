/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "ob_blacklist_req_processor.h"
#include "share/ob_server_blacklist.h"

namespace oceanbase
{
using namespace common;

namespace obrpc
{
class ObServerBlacklist;

int ObBlacklistReqP::process()
{
  int ret = OB_SUCCESS;
  const int64_t src_cluster_id = get_src_cluster_id();
  if (OB_FAIL(share::ObServerBlacklist::get_instance().handle_req(src_cluster_id, arg_))) {
    RPC_LOG(WARN, "handle_msg failed", K(ret));
  }
  req_->set_trace_point();
  return ret;
}

}; // end namespace rpc
}; // end namespace oceanbase
