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

namespace oceanbase
{
namespace share
{
// Synchronization status of standby cluster
enum ObClusterSyncStatus
{
  // To maintain compatibility, do not change the value of the field
  NOT_AVAILABLE = 0,                
  CLUSTER_VERSION_NOT_MATCH = 1,
  SYS_SCHEMA_NOT_SYNC = 2,
  FAILED_CHECK_PARTITION_LOG = 3,
  PARTITION_LOG_NOT_SYNC = 4,
  FAILED_CHECK_ENOUGH_MEMBER = 5,
  FAILED_CHECK_SYS_SCHEMA = 6,
  REPLICA_NOT_ENOUGH = 7,
  FAILED_CHECK_FAILOVER_INFO = 8,
  FAILOVER_INFO_NOT_LATEST = 9,
  FAILED_CHECK_ALL_CLUSTER_INFO = 10,
  ALL_CLUSTER_INFO_NOT_SYNC = 11,
  FAILED_CHECK_RESTORE_REPLICA = 12,
  REPLICA_IN_RESTORE = 13,
  CHECK_USER_SCHEMA_SYNC_STATUS = 14,
  CHECK_FREEZE_INFO_SYNC_STATUS = 15,
  FAILED_CHECK_MERGE_STATUS = 16,
  CHECK_MERGE_STATUS = 17,
  FAILED_CHECK_META_VALID = 18,
  FAILED_CHECK_USER_TENANT_SCHEMA = 19,
  FAILED_CHECK_FREEZE_INFO = 20,
  FAILED_CHECK_ALL_SCHEMA_EFFECTIVE = 21,
  CHECK_ALL_SCHEMA_EFFECTIVE = 22,
  TENANT_NOT_MATCH = 23,
  OK = 24,
  CLUSTER_IS_DISABLED = 25,
  SYNC_STATUS_MAX
};

class ObClusterSyncStatusHelp
{
public:
  static const char * cluster_sync_status_strs[];
  static const char * cluster_sync_status_to_str(const share::ObClusterSyncStatus sync_status, 
                                                 const int64_t last_hb_ts);
  static bool cluster_sync_status_is_valid(const int64_t last_ts);
};

} // end namespace share
} // end oceanbase

#endif // OCEANBASE_SHARE_OB_CLUSTER_SYNC_STATUS_H_
