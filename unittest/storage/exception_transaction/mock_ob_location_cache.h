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

#ifndef OCEANBASE_SHARE_MOCK_OB_LOCATION_CACHE_H_
#define OCEANBASE_SHARE_MOCK_OB_LOCATION_CACHE_H_

#include "common/ob_partition_key.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/container/ob_iarray.h"
#include "lib/net/ob_addr.h"
#include "share/partition_table/ob_partition_location_cache.h"

namespace oceanbase {

using namespace common;
using namespace common::hash;

namespace share {

class MockObLocationCache : public ObIPartitionLocationCache {
public:
  MockObLocationCache()
  {}
  virtual ~MockObLocationCache()
  {}
  int init();

public:
  int add(const ObPartitionKey& partition, const ObAddr& leader);
  int add_overwrite(const ObPartitionKey& partition, const ObAddr& leader);
  int remove(const ObPartitionKey& partition);

public:
  virtual ObIPartitionLocationCache::PartitionLocationCacheType get_type() const
  {
    return static_cast<ObIPartitionLocationCache::PartitionLocationCacheType>(PART_LOC_CACHE_TYPE_NORMAL);
  }
  int get(const uint64_t table_id, const int64_t partition_id, ObPartitionLocation& location,
      const int64_t last_renew_time, bool& is_cache_hit, const bool auto_update = true);
  int get(const ObPartitionKey& partition, ObPartitionLocation& location, const int64_t last_renew_time,
      bool& is_cache_hit);
  int get(const uint64_t table_id, common::ObIArray<ObPartitionLocation>& locations, const int64_t last_renew_time,
      bool& is_cache_hit, const bool auto_update = true);
  int get_leader(const common::ObPartitionKey& partition, common::ObAddr& leader, const bool force_renew = false);
  int nonblock_get(const uint64_t table_id, const int64_t partition_id, ObPartitionLocation& location,
      const int64_t cluster_id = -1);
  int nonblock_get(const ObPartitionKey& partition, ObPartitionLocation& location, const int64_t cluster_id = -1);
  int nonblock_get(const uint64_t table_id, const int64_t partition_num, ObIArray<ObPartitionLocation>& locations);
  int nonblock_get_leader(const ObPartitionKey& partition, ObAddr& leader);
  int nonblock_get_leader_across_cluster(const ObPartitionKey& partition, ObAddr& leader, int64_t& cluster_id);
  virtual int nonblock_get_restore_leader(const common::ObPartitionKey& partition, common::ObAddr& leader)
  {
    UNUSED(partition);
    UNUSED(leader);
    return common::OB_OP_NOT_ALLOW;
  }
  virtual int nonblock_get_across_cluster(
      const ObPartitionKey& partition, ObPartitionLocation& location, int64_t& cluster_id)
  {
    UNUSED(partition);
    UNUSED(location);
    UNUSED(cluster_id);
    return common::OB_OP_NOT_ALLOW;
  }
  virtual int get_across_cluster(const common::ObPartitionKey& partition, const int64_t expire_renew_time,
      ObPartitionLocation& location, int64_t& cluster_id)
  {
    UNUSED(partition);
    UNUSED(expire_renew_time);
    UNUSED(location);
    UNUSED(cluster_id);
    return common::OB_OP_NOT_ALLOW;
  }

  virtual int get_leader_across_cluster(const common::ObPartitionKey& partition, common::ObAddr& leader,
      int64_t& cluster_id, const bool force_renew = false)
  {
    UNUSED(partition);
    UNUSED(leader);
    UNUSED(cluster_id);
    UNUSED(force_renew);
    return common::OB_OP_NOT_ALLOW;
  }

  virtual int get_leader_across_cluster_without_renew(
      const common::ObPartitionKey& partition, common::ObAddr& leader, int64_t& cluster_id)
  {
    UNUSED(partition);
    UNUSED(leader);
    UNUSED(cluster_id);
    return common::OB_OP_NOT_ALLOW;
  }

  int nonblock_renew(const ObPartitionKey& partition, const int64_t last_renew_time);
  int nonblock_renew_across_cluster(const ObPartitionKey& partition, const int64_t last_renew_time);
  int nonblock_renew_with_limiter(
      const common::ObPartitionKey& partition, const int64_t expire_renew_time, bool& is_limited);
  int batch_process_tasks(const common::ObIArray<ObLocationAsyncUpdateTask>& tasks, bool& stopped)
  {
    UNUSEDx(tasks, stopped);
    return common::OB_NOT_SUPPORTED;
  }
  int process_barrier(const ObLocationAsyncUpdateTask& task, bool& stopped)
  {
    UNUSEDx(task, stopped);
    return common::OB_NOT_SUPPORTED;
  }

private:
  static const int64_t BUCKET_NUM = 137;

private:
  ObHashMap<ObPartitionKey, ObAddr> partition_addr_map_;
};

}  // namespace share
}  // namespace oceanbase

#endif
