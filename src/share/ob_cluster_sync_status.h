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

#ifndef OCEANBASE_SHARE_OB_CLUSTER_SYNC_STATUS_H_
#define OCEANBASE_SHARE_OB_CLUSTER_SYNC_STATUS_H_

#include "share/ob_define.h"

namespace oceanbase {
namespace share {
// Synchronization status of standby cluster
enum ObClusterSyncStatus {
  NOT_AVAILABLE = 0,
  CLUSTER_VERSION_NOT_MATCH,
  SYS_SCHEMA_NOT_SYNC,
  FAILED_CHECK_PARTITION_LOG,
  PARTITION_LOG_NOT_SYNC,
  FAILED_CHECK_ENOUGH_MEMBER,
  FAILED_CHECK_SYS_SCHEMA,
  REPLICA_NOT_ENOUGH,
  FAILED_CHECK_FAILOVER_INFO,
  FAILOVER_INFO_NOT_LATEST,
  FAILED_CHECK_ALL_CLUSTER_INFO,
  ALL_CLUSTER_INFO_NOT_SYNC,
  FAILED_CHECK_RESTORE_REPLICA,
  REPLICA_IN_RESTORE,
  CHECK_USER_SCHEMA_SYNC_STATUS,
  CHECK_FREEZE_INFO_SYNC_STATUS,
  FAILED_CHECK_MERGE_STATUS,
  CHECK_MERGE_STATUS,
  FAILED_CHECK_META_VALID,
  FAILED_CHECK_USER_TENANT_SCHEMA,
  FAILED_CHECK_FREEZE_INFO,
  FAILED_CHECK_ALL_SCHEMA_EFFECTIVE,
  CHECK_ALL_SCHEMA_EFFECTIVE,
  TENANT_NOT_MATCH,
  OK,
  SYNC_STATUS_MAX
};

class ObClusterSyncStatusHelp {
public:
  static const char* cluster_sync_status_strs[];
  static const char* cluster_sync_status_to_str(const share::ObClusterSyncStatus sync_status, const int64_t last_hb_ts);
  static bool cluster_sync_status_is_valid(const int64_t last_ts);
};

}  // end namespace share
}  // namespace oceanbase

#endif  // OCEANBASE_SHARE_OB_CLUSTER_SYNC_STATUS_H_
