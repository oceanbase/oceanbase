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

#define USING_LOG_PREFIX SHARE

#include "share/ob_cluster_sync_status.h"  //class ObClusterSyncStatusHelp

namespace oceanbase {
using namespace common;

namespace share {

const char* ObClusterSyncStatusHelp::cluster_sync_status_strs[] = {"NOT AVAILABLE",
    "CLUSTER VERSION NOT MATCH",
    "SYS SCHEMA NOT SYNC",
    "FAILED CHECK PARTITION LOG",
    "PARTITION LOG NOT SYNC",
    "FAILED CHECK ENOUGH MEMBER",
    "FAILED CHECK SYS SCHEMA",
    "REPLICA NOT ENOUGH",
    "FAILED CHECK FAILOVER INFO",
    "FAILOVER INFO NOT LATEST",
    "FAILED CHECK ALL CLUSTER INFO",
    "ALL CLUSTER INFO NOT SYNC",
    "FAILED CHECK RESTORE REPLICA",
    "REPLICA IN RESTORE",
    "CHECK USER SCHEMA SYNC STATUS",
    "CHECK FREEZE INFO SYNC STATUS",
    "FAILED CHECK MERGE STATUS",
    "CHECK MERGE STATUS",
    "FAILED CHECK META VALID",
    "FAILED CHECK USER TENANT SCHEMA",
    "FAILED CHECK FREEZE INFO",
    "FAILED CHECK ALL SCHEMA EFFECTIVE",
    "CHECK ALL SCHEMA EFFECTIVE",
    "TENANT NOT MATCH",
    "OK"};

const char* ObClusterSyncStatusHelp::cluster_sync_status_to_str(
    const share::ObClusterSyncStatus sync_status, const int64_t last_hb_ts)
{
  STATIC_ASSERT(ARRAYSIZEOF(cluster_sync_status_strs) == SYNC_STATUS_MAX,
      "type string array size mismatch with enum ObClusterSyncStatus count");

  const char* sync_status_str = "";

  if (cluster_sync_status_is_valid(last_hb_ts)) {
    if (OB_UNLIKELY(sync_status > share::ObClusterSyncStatus::OK) ||
        OB_UNLIKELY(sync_status < share::ObClusterSyncStatus::NOT_AVAILABLE)) {
      LOG_ERROR("fatal error, unknown cluster sync status", K(sync_status));
    } else {
      sync_status_str = cluster_sync_status_strs[sync_status];
    }
  } else {
    sync_status_str = cluster_sync_status_strs[share::ObClusterSyncStatus::NOT_AVAILABLE];
  }

  return sync_status_str;
}

bool ObClusterSyncStatusHelp::cluster_sync_status_is_valid(const int64_t last_ts)
{
  const int64_t SYNCSTATUS_TIMEOUT_US = 30L * 1000 * 1000;  // 30s
  bool bret = false;

  if (common::OB_INVALID_TIMESTAMP == last_ts || (ObTimeUtility::current_time() - last_ts) > SYNCSTATUS_TIMEOUT_US) {
    // More than 30s, status is invalid
    bret = false;
  } else {
    bret = true;
  }

  return bret;
}

}  // end namespace share
}  // namespace oceanbase
