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

#include "lib/ob_define.h"                            // ObReplicaType
#include "share/partition_table/ob_partition_info.h"  // ObReplicaStatus
#include "share/ob_replica_wrs_info.h"                // ObReplicaWrsInfoList
#include "storage/ob_i_partition_group.h"             // ObPartitionState

#ifndef OCEANBASE_STORAGE_OB_WRS_UTILS_H_
#define OCEANBASE_STORAGE_OB_WRS_UTILS_H_

namespace oceanbase {
namespace storage {

/**
 * Get weak_read_timestamp of all replicas on local observer
 *
 * @param [out] all_replica_wrs_info          Weak_read_timestamps of all replicas
 * @param [in]  tenant_id                     tenant id should be valid, that is, to obtain
 *                                            Weak_read_timestamp of replica for specified tenant
 * @param [in]  need_filter_invalid_replica   Whether need to filter invalid replica
 * @param [in]  need_inner_replica            Whether need to sys table replica
 * @param [in]  need_user_replica             Whether need to user table replica
 */
int get_tenant_replica_wrs_info(share::ObReplicaWrsInfoList& all_replica_wrs_info, const int64_t tenant_id,
    const bool need_filter_invalid_replica = false, const bool need_inner_replica = true,
    const bool need_user_replica = true);

// check whether it is a weak readable replica
bool is_weak_readable_replica(const ObPartitionState partition_state, const common::ObReplicaType replica_type);

// Check whether it is not a normal and weak readable replica, the main purpose is to exclude
// unfinished migration, offline and other abnormal status replicas
bool is_normal_weak_readable_replica(const ObPartitionState partition_state, const common::ObReplicaType replica_type,
    const share::ObReplicaStatus replica_status);

}  // namespace storage
}  // namespace oceanbase
#endif
