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

#ifndef OCEANBASE_SHARE_OB_CLUSTER_SWITCHOVER_INFO_H_
#define OCEANBASE_SHARE_OB_CLUSTER_SWITCHOVER_INFO_H_

#include "lib/container/ob_se_array.h"  // ObSEArray

namespace oceanbase {
namespace share {
/**
 * SWITCHOVER_INFO column of V$OB_CLUSTER view
 *
 * Show switchover information, which is detailed descriptions of the SWITCHOVER_STATUS column
 * 1. When show NOT ALLOWED, why is it not allowed to switchover
 * 2. When show TO STANDBY, which standby cluster can switchover to primary cluster
 * 3. When in other status, this column value is empty
 */
enum ObClusterSwitchoverInfo {
  // Empty
  SWITCHOVER_INFO_NONE = 0,

  // Need to check the status of servers on primary/standby cluster, there may be some server offline
  // Reason of SWITCHOVER_STATUS = 'NOT ALLOWED'
  SWITCHOVER_INFO_CHECK_PRIMARY_CLUSTER_SERVER_STATUS = 1,  // Both displayed on primary and standby cluster
  SWITCHOVER_INFO_CHECK_STANDBY_CLUSTER_SERVER_STATUS = 2,  // Displayed on standby cluster

  // Obsolete: Need to check whether load balancing task is completed on primary/standby cluster
  // Reason of SWITCHOVER_STATUS = 'NOT ALLOWED'
  SWITCHOVER_INFO_CHECK_PRIMARY_CLUSTER_REBALANCE_TASK_STATUS = 3,  // Both displayed on primary and standby cluster
  SWITCHOVER_INFO_CHECK_STANDBY_CLUSTER_REBALANCE_TASK_STATUS = 4,  // Displayed on standby cluster

  // Need to check whether merge status is ERROR on primary/standby cluster
  // Reason of SWITCHOVER_STATUS = 'NOT ALLOWED'
  SWITCHOVER_INFO_CHECK_MERGE_STATUS = 5,  // Both displayed on primary and standby cluster

  // No synchronized standby cluster
  // Reason of SWITCHOVER_STATUS = 'NOT ALLOWED'
  SWITCHOVER_INFO_NONE_SYNCED_STANDBY_CLUSTER = 6,  // Displayed on primary cluster

  // List of standby clusters which synchronized with primary
  // SWITCHOVER_STATUS = 'TO STANDBY', Display the list of standby clusters which can switchover
  // Example:
  // "SYNCED STANDBY CLUSTERS: cluster1, cluster2"
  SWITCHOVER_INFO_HAS_SYNCED_STANDBY_CLUSTER = 7,  // Displayed on primary cluster

  // Need to check whether SYS schema of standby clusteris synchronized
  // Reason of SWITCHOVER_STATUS = 'NOT ALLOWED'
  SWITCHOVER_INFO_CHECK_SYS_SCHEMA_SYNC_STATUS = 8,  // Displayed on standby cluster

  // Need to check whether USER schema of standby cluster is synchronized
  // Reason of SWITCHOVER_STATUS = 'NOT ALLOWED'
  SWITCHOVER_INFO_CHECK_USER_SCHEMA_SYNC_STATUS = 9,  // Displayed on standby cluster

  // Need to check whether FREEZE INFO of standby cluster is synchronized
  // Reason of SWITCHOVER_STATUS = 'NOT ALLOWED'
  SWITCHOVER_INFO_CHECK_FREEZE_INFO_SYNC_STATUS = 10,  // Displayed on standby cluster

  // Obsolete: Need to check whether the number of replicas on standby cluster is enough
  // Reason of SWITCHOVER_STATUS = 'NOT ALLOWED'
  SWITCHOVER_INFO_CHECK_ENOUGH_REPLICA = 11,  // Displayed on standby cluster

  // Need to check whether REDO LOG of standby cluster is synchronized
  // Reason of SWITCHOVER_STATUS = 'NOT ALLOWED'
  SWITCHOVER_INFO_CHECK_REDO_LOG_SYNC_STATUS = 12,  // Displayed on standby cluster

  // INNER ERROR
  SWITCHOVER_INFO_INNER_ERROR = 13,  // Both displayed on primary and standby cluster

  // There are inaccessible server, switchover requires all clusters to be online
  // Reason of SWITCHOVER_STATUS = 'NOT ALLOWED'
  SWITCHOVER_INFO_CLUSTER_CAN_NOT_ACCESS = 14,  // Both displayed on primary and standby cluster

  // Currently there are no clusters in SWITCHING state
  // Typical scenario: When primary and standby databases are normal, the standby cluster
  // displays "NOT ALLOWED", and switchover_info will display this status
  // Reason of SWITCHOVER_STATUS = 'NOT ALLOWED'
  SWITCHOVER_INFO_NO_CLUSTER_IN_SWITCHING = 15,  // Displayed on standby cluster

  // Displayed on primary cluster, primary cluster is in maximum protection mode, but
  // redo_transport_options is not SYNC, if do switchover, the new primary and standby cluster
  // cannot be maximum protection mode as there is no SYNC redo_transport_options standby cluster
  SWITCHOVER_INFO_PRIMARY_CLUSTER_NOT_IN_SYNC_MODE = 16,

  // Displayed on standby cluster, standby cluster is in maximum protection mode, but
  // redo_transport_options is not SYNC, this standby cluster cannot switchover to primary cluster,
  // need choose other SYNC redo_transport_options standby cluster to do switchover
  SWITCHOVER_INFO_STANDBY_CLUSTER_NOT_IN_SYNC_MODE = 17,

  // Changing protection mode, cannot switchover
  SWITCHOVER_INFO_PRIMARY_CLUSTER_IN_PRE_MAXIMUM_PROTECTION_MODE = 18,

  // There is replica in restore status, cannot switchover
  SWITCHOVER_INFO_PRIMARY_CLUSTER_HAS_REPLICA_IN_RESTORE = 19,

  // There is replica in restore status, cannot switchover
  SWITCHOVER_INFO_STANDBY_CLUSTER_HAS_REPLICA_IN_RESTORE = 20,

  // Doing backup, cannot switchover
  SWITCHOVER_INFO_PRIMARY_CLUSTER_DOING_BACKUP = 21,

  // There is other primary cluster
  SWITCHOVER_INFO_EXIST_OTHER_PRIMARY_CLUSTER = 22,

  SWITCHOVER_INFO_MAX
};

const char* switchover_info_to_str(const ObClusterSwitchoverInfo so_info);

struct ObClusterSwitchoverInfoWrap {
  OB_UNIS_VERSION(1);

public:
  static const int64_t DEFAULT_SYNCED_CLUSTER_COUNT = 5;
  typedef common::ObSEArray<int64_t, DEFAULT_SYNCED_CLUSTER_COUNT> ClusterIdArray;

  ObClusterSwitchoverInfo info_;
  ClusterIdArray synced_cluster_id_array_;
  ClusterIdArray can_not_access_cluster_;

  void reset()
  {
    info_ = SWITCHOVER_INFO_NONE;
    synced_cluster_id_array_.reset();
    can_not_access_cluster_.reset();
  }

  TO_STRING_KV(
      K_(info), "info_str", switchover_info_to_str(info_), K_(synced_cluster_id_array), K_(can_not_access_cluster));
};

}  // namespace share
}  // namespace oceanbase
#endif
