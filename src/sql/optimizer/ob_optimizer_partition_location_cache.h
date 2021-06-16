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

#ifndef OB_SQL_OPTIMIZER_PARTITION_LOCATION_CACHE_H_
#define OB_SQL_OPTIMIZER_PARTITION_LOCATION_CACHE_H_

#include "lib/hash/ob_hashmap.h"
#include "share/partition_table/ob_partition_location_cache.h"
namespace oceanbase {
namespace sql {
class ObOptimizerPartitionLocationCache : public share::ObIPartitionLocationCache {
public:
  static const int64_t LOCATION_CACHE_BUCKET_NUM = 32;
  ObOptimizerPartitionLocationCache(common::ObIAllocator& allocator, share::ObIPartitionLocationCache* location_cache);
  virtual ~ObOptimizerPartitionLocationCache();

  virtual PartitionLocationCacheType get_type() const
  {
    return ObOptimizerPartitionLocationCache::PART_LOC_CACHE_TYPE_OPTIMIZER;
  }

  // get partition location of a partition
  // return OB_LOCATION_NOT_EXIST if not record in partition table
  virtual int get(const uint64_t table_id, const int64_t partition_id, share::ObPartitionLocation& location,
      const int64_t expire_renew_time, bool& is_cache_hit, const bool auto_update = true) override;

  // get partition location of a partition
  // return OB_LOCATION_NOT_EXIST if not record in partition table
  virtual int get(const common::ObPartitionKey& partition, share::ObPartitionLocation& location,
      const int64_t expire_renew_time, bool& is_cache_hit) override;

  // get all partition locations of a table, virtual table should set partition_num to 1
  // return OB_LOCATION_NOT_EXIST if some partition doesn't have record in partition table
  virtual int get(const uint64_t table_id, common::ObIArray<share::ObPartitionLocation>& locations,
      const int64_t expire_renew_time, bool& is_cache_hit, const bool auto_update = true) override;

  // get leader addr of a partition, return OB_LOCATION_NOT_EXIST if not replica in partition
  // table, return OB_LOCATION_LEADER_NOT_EXIST if leader not exist
  virtual int get_strong_leader(
      const common::ObPartitionKey& partition, common::ObAddr& leader, const bool force_renew = false) override;

  // return OB_LOCATION_NOT_EXIST if partition's location not in cache
  virtual int nonblock_get(const uint64_t table_id, const int64_t partition_id, share::ObPartitionLocation& location,
      const int64_t cluster_id = common::OB_INVALID_ID) override;

  // return OB_LOCATION_NOT_EXIST if partition's location not in cache
  virtual int nonblock_get(const common::ObPartitionKey& partition, share::ObPartitionLocation& location,
      const int64_t cluster_id = common::OB_INVALID_ID) override;

  // return OB_LOCATION_NOT_EXIST if partition's location not in cache,
  // return OB_LOCATION_LEADER_NOT_EXIST if partition's location in cache, but leader not exist
  virtual int nonblock_get_strong_leader(const common::ObPartitionKey& partition, common::ObAddr& leader) override;

  // trigger a location update task and clear location in cache
  virtual int nonblock_renew(const common::ObPartitionKey& partition, const int64_t expire_renew_time,
      const int64_t cluster_id = common::OB_INVALID_ID) override;

  // try to trigger a location update task and clear location in cache,
  // if it is limited by the limiter and not be done, is_limited will be set to true
  virtual int nonblock_renew_with_limiter(
      const common::ObPartitionKey& partition, const int64_t expire_renew_time, bool& is_limited) override;
  // link table.
  virtual int get_link_table_location(const uint64_t table_id, share::ObPartitionLocation& location);
  inline share::ObIPartitionLocationCache* get_location_cache()
  {
    return location_cache_;
  }

private:
  int insert_or_replace_optimizer_cache(share::ObLocationCacheKey& key, share::ObPartitionLocation* location);

private:
  typedef common::hash::ObHashMap<share::ObLocationCacheKey, share::ObPartitionLocation*,
      common::hash::NoPthreadDefendMode>
      PartitionLocationMap;
  common::ObIAllocator& allocator_;
  share::ObIPartitionLocationCache* location_cache_;
  PartitionLocationMap optimizer_cache_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObOptimizerPartitionLocationCache);
};

} /* namespace sql */
} /* namespace oceanbase */

#endif /* OB_SQL_OPTIMIZER_OB_OPTIMIZER_PARTITION_LOCATION_CACHE_H_ */
