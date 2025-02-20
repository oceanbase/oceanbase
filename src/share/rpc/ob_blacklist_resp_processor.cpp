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

#include "ob_blacklist_resp_processor.h"
#include "share/ob_server_blacklist.h"

namespace oceanbase
{
using namespace common;

namespace obrpc
{
class ObServerBlacklist;

int ObBlacklistRespP::process()
{
  int ret = OB_SUCCESS;
  const int64_t src_cluster_id = get_src_cluster_id();
  if (OB_FAIL(share::ObServerBlacklist::get_instance().handle_resp(arg_, src_cluster_id))) {
    RPC_LOG(WARN, "handle_msg failed", K(ret));
  }
  req_->set_trace_point();
  return ret;
}

}; // end namespace rpc
}; // end namespace oceanbase
