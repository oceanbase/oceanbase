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

#include "ob_mock_location_service.h"

namespace oceanbase
{
namespace share
{

int MockLocationService::init(const common::ObAddr &local_addr, const int64_t sql_port)
{
  int ret = OB_SUCCESS;
  local_addr_ = local_addr;
  sql_port_ = sql_port;
  return ret;
}

int MockLocationService::nonblock_get(
    const uint64_t tenant_id,
    const ObTabletID &tablet_id,
    ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  // todo: support multi ls id to mock remote sharding
  ls_id = ObLSID(ObLSID::SYS_LS_ID);
  return ret;
}

int MockLocationService::nonblock_get(
    const int64_t cluster_id,
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    ObLSLocation &location)
{
  int ret = OB_SUCCESS;
  ObLSReplicaLocation replica_location;
  int64_t ctime = ObTimeUtility::current_time();
  if (OB_FAIL(location.init(cluster_id, tenant_id, ls_id, ctime))) {
    COMMON_LOG(WARN, "failed to init location", K(ret));
  } else if (OB_FAIL(replica_location.init(local_addr_,
                                           ObRole::LEADER,
                                           sql_port_,
                                           ObReplicaType::REPLICA_TYPE_FULL,
                                           ObReplicaProperty(),
                                           ObLSRestoreStatus(),
                                           ctime))) {
    COMMON_LOG(WARN, "failed to init replaca location", K(ret));
  } else if (OB_FAIL(location.add_replica_location(replica_location))) {
    COMMON_LOG(WARN, "failed to add replica location", K(ret));
  }
  return ret;
}

} // share
} // oceanbase
