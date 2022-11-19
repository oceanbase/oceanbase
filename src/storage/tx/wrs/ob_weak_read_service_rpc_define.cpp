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

#define USING_LOG_PREFIX TRANS

#include "share/ob_errno.h"
#include "ob_weak_read_service_rpc.h"

namespace oceanbase
{
using namespace common;
namespace obrpc
{

OB_SERIALIZE_MEMBER(ObWrsGetClusterVersionRequest, req_server_);
OB_SERIALIZE_MEMBER(ObWrsGetClusterVersionResponse, err_code_, version_, version_duration_us_);
OB_SERIALIZE_MEMBER(ObWrsClusterHeartbeatRequest,
    req_server_,
    version_,
    valid_part_count_,
    total_part_count_,
    generate_timestamp_);
OB_SERIALIZE_MEMBER(ObWrsClusterHeartbeatResponse, err_code_);


int ObWrsGetClusterVersionP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(wrs_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("weak read service is NULL", K(wrs_));
  } else if (OB_ISNULL(rpc_pkt_)) {
    ret = OB_INVALID_ERROR;
    LOG_WARN("invalid rpc packet which is NULL", KR(ret), K(rpc_pkt_));
  } else {
    wrs_->process_get_cluster_version_rpc(rpc_pkt_->get_tenant_id(), arg_, result_);
  }
  return ret;
}

int ObWrsClusterHeartbeatP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(wrs_)) {
    LOG_WARN("weak read service is NULL", K(wrs_));
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(rpc_pkt_)) {
    ret = OB_INVALID_ERROR;
    LOG_WARN("invalid rpc packet which is NULL", KR(ret), K(rpc_pkt_));
  } else {
    wrs_->process_cluster_heartbeat_rpc(rpc_pkt_->get_tenant_id(), arg_, result_);
  }
  return ret;
}

}
}
