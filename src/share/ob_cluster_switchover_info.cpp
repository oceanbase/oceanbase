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

#include "ob_cluster_switchover_info.h"

namespace oceanbase {
namespace share {

const char* switchover_info_to_str(const ObClusterSwitchoverInfo so_info)
{
  const char* cstr = "";
  switch (so_info) {
    case SWITCHOVER_INFO_NONE:
      cstr = "";
      break;

    case SWITCHOVER_INFO_CHECK_PRIMARY_CLUSTER_SERVER_STATUS:
      cstr = "CHECK PRIMARY CLUSTER SERVER STATUS";
      break;

    case SWITCHOVER_INFO_CHECK_STANDBY_CLUSTER_SERVER_STATUS:
      cstr = "CHECK STANDBY CLUSTER SERVER STATUS";
      break;

    case SWITCHOVER_INFO_CHECK_PRIMARY_CLUSTER_REBALANCE_TASK_STATUS:
      cstr = "CHECK PRIMARY CLUSTER REBALANCE TASK STATUS";
      break;

    case SWITCHOVER_INFO_CHECK_STANDBY_CLUSTER_REBALANCE_TASK_STATUS:
      cstr = "CHECK STANDBY CLUSTER REBALANCE TASK STATUS";
      break;

    case SWITCHOVER_INFO_CHECK_MERGE_STATUS:
      cstr = "CHECK MERGE STATUS";
      break;

    case SWITCHOVER_INFO_NONE_SYNCED_STANDBY_CLUSTER:
      cstr = "NONE SYNCED STANDBY CLUSTER";
      break;

    case SWITCHOVER_INFO_HAS_SYNCED_STANDBY_CLUSTER:
      cstr = "SYNCED STANDBY CLUSTERS: ";
      break;

    case SWITCHOVER_INFO_CHECK_SYS_SCHEMA_SYNC_STATUS:
      cstr = "CHECK SYS SCHEMA SYNC STATUS";
      break;

    case SWITCHOVER_INFO_CHECK_USER_SCHEMA_SYNC_STATUS:
      cstr = "CHECK USER SCHEMA SYNC STATUS";
      break;

    case SWITCHOVER_INFO_CHECK_FREEZE_INFO_SYNC_STATUS:
      cstr = "CHECK FREEZE INFO SYNC STATUS";
      break;

    case SWITCHOVER_INFO_CHECK_ENOUGH_REPLICA:
      cstr = "CHECK ENOUGH REPLICA";
      break;

    case SWITCHOVER_INFO_CHECK_REDO_LOG_SYNC_STATUS:
      cstr = "CHECK REDO LOG SYNC STATUS";
      break;

    case SWITCHOVER_INFO_INNER_ERROR:
      cstr = "INNER ERROR";
      break;

    case SWITCHOVER_INFO_CLUSTER_CAN_NOT_ACCESS:
      cstr = "CAN NOT ACCESS CLUSTERS: ";
      break;

    case SWITCHOVER_INFO_NO_CLUSTER_IN_SWITCHING:
      cstr = "NO CLUSTER IN SWITCHING";
      break;

    case SWITCHOVER_INFO_PRIMARY_CLUSTER_NOT_IN_SYNC_MODE:
      cstr = "PRIMARY CLUSTER NOT IN SYNC MODE";
      break;

    case SWITCHOVER_INFO_STANDBY_CLUSTER_NOT_IN_SYNC_MODE:
      cstr = "STANDBY CLUSTER NOT IN SYNC MODE";
      break;

    case SWITCHOVER_INFO_PRIMARY_CLUSTER_IN_PRE_MAXIMUM_PROTECTION_MODE:
      cstr = "IN PRE MAXIMUM PROTECTION MODE";
      break;

    case SWITCHOVER_INFO_PRIMARY_CLUSTER_HAS_REPLICA_IN_RESTORE:
      cstr = "PRIMARY CLUSTER HAS REPLICA IN RESTORE";
      break;

    case SWITCHOVER_INFO_STANDBY_CLUSTER_HAS_REPLICA_IN_RESTORE:
      cstr = "STANDBY CLUSTER HAS REPLICA IN RESTORE";
      break;

    case SWITCHOVER_INFO_PRIMARY_CLUSTER_DOING_BACKUP:
      cstr = "PRIMARY CLUSTER DOING BACKUP";
      break;

    case SWITCHOVER_INFO_EXIST_OTHER_PRIMARY_CLUSTER:
      cstr = "CHECK OTHER PRIMARY CLUSTER";
      break;

    default:
      cstr = "";
      break;
  }
  return cstr;
}

OB_SERIALIZE_MEMBER(ObClusterSwitchoverInfoWrap, info_, synced_cluster_id_array_, can_not_access_cluster_);

}  // namespace share
}  // namespace oceanbase
