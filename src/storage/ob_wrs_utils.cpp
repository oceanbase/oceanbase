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

#define USING_LOG_PREFIX STORAGE

#include "ob_wrs_utils.h"

#include "lib/oblog/ob_log.h"              // LOG_INFO, LOG_WARN, LOG_ERROR
#include "storage/ob_pg_mgr.h"             // ObIPartitionGroupIterator
#include "storage/ob_partition_service.h"  // ObPartitionService

namespace oceanbase {
using namespace common;
using namespace share;
namespace storage {

bool is_weak_readable_replica(const ObPartitionState partition_state, const ObReplicaType replica_type)
{
  return is_working_state(partition_state) && ObReplicaTypeCheck::can_slave_read_replica(replica_type);
}

bool is_normal_weak_readable_replica(
    const ObPartitionState partition_state, const ObReplicaType replica_type, const ObReplicaStatus replica_status)
{
  return is_weak_readable_replica(partition_state, replica_type) && REPLICA_STATUS_NORMAL == replica_status;
}

int get_tenant_replica_wrs_info(ObReplicaWrsInfoList& all_replica_wrs_info, const int64_t target_tenant_id,
    const bool need_filter_invalid_replica /* = false */, const bool need_inner_replica /* = true */,
    const bool need_user_replica /* = true */)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupIterator* iter = NULL;
  ObPartitionService& ps = ObPartitionService::get_instance();
  if (OB_ISNULL(iter = ps.alloc_pg_iter())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("fail to alloc partition scan iter", KR(ret));
  } else {
    while (OB_SUCCESS == ret) {
      ObIPartitionGroup* part = NULL;
      int64_t wrs = 0;
      ObReplicaStatus replica_status = REPLICA_STATUS_NORMAL;
      if (OB_FAIL(iter->get_next(part))) {
        if (OB_ITER_END == ret) {
        } else {
          LOG_WARN("iterate next partition fail", KR(ret));
        }
      } else if (OB_ISNULL(part)) {
        ret = OB_PARTITION_NOT_EXIST;
        LOG_WARN("iterate partition fail", K(part), KR(ret));
      } else if (target_tenant_id != part->get_partition_key().get_tenant_id()) {
        // Filter by tenant
      } else if (OB_FAIL(part->get_replica_status(replica_status))) {
        LOG_WARN("fail to get replica status", KR(ret), KPC(part));
        // Failure of this partition does not affect other partitions
        ret = OB_SUCCESS;
      } else if (OB_FAIL(part->get_weak_read_timestamp(wrs))) {
        LOG_WARN("fail to get weak read timestamp", KR(ret), KPC(part));
        // Failure of this partition does not affect other partitions
        ret = OB_SUCCESS;
      } else {
        const ObPartitionKey& pkey = part->get_partition_key();
        const bool is_sys_part = is_inner_table(part->get_partition_key().get_table_id());
        const ObPartitionState partition_state = part->get_partition_state();
        const ObReplicaType replica_type = part->get_replica_type();
        const bool be_normal_weak_readable_replica =
            is_normal_weak_readable_replica(partition_state, replica_type, replica_status);
        bool need_filter = false;

        if ((!need_inner_replica && is_sys_part) || (!need_user_replica && !is_sys_part)) {
          // Filter by SYS table replica or user table replica
          need_filter = true;
        } else if (!be_normal_weak_readable_replica) {
          // If it is not a normal and weak readable replica, filter it according to parameter
          need_filter = need_filter_invalid_replica;
        }

        LOG_DEBUG("[GET_WRS_INFO] get replica",
            K(need_filter),
            K(target_tenant_id),
            K(pkey),
            K(wrs),
            K(is_sys_part),
            K(be_normal_weak_readable_replica),
            K(need_inner_replica),
            K(need_user_replica),
            K(need_filter_invalid_replica),
            K(partition_state),
            K(replica_type),
            K(replica_status));

        if (OB_FAIL(ret)) {
        } else if (need_filter) {
        } else {
          ObReplicaWrsInfo wrs_info(pkey, wrs, partition_state, replica_type, replica_status);
          if (OB_FAIL(all_replica_wrs_info.push_back(wrs_info))) {
            LOG_WARN("fail to push back all replica wrs info list", KR(ret), K(wrs_info));
          }
        }
      }
    }  // while

    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }

  if (NULL != iter) {
    ps.revert_pg_iter(iter);
    iter = NULL;
  }
  return ret;
}

}  // namespace storage
}  // namespace oceanbase
