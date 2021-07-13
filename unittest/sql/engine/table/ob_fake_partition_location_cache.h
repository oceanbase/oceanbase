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

#ifndef OCEANBASE_FAKE_PARTITION_LOCATION_CACHE_
#define OCEANBASE_FAKE_PARTITION_LOCATION_CACHE_

#include "share/partition_table/ob_partition_location_cache.h"
#include "lib/hash/ob_hashmap.h"

namespace oceanbase {
namespace share {

class ObFakePartitionKey {
public:
  uint64_t table_id_;
  int64_t partition_id_;
  inline int64_t hash() const
  {
    return table_id_ + partition_id_;
  }
  inline bool operator==(const ObFakePartitionKey& other) const
  {
    return table_id_ == other.table_id_ && partition_id_ == other.partition_id_;
  }
};

class ObFakePartitionLocationCache : public ObPartitionLocationCache {
public:
  ObFakePartitionLocationCache();
  virtual ~ObFakePartitionLocationCache();

  // get partition location of a partition
  virtual int get(const uint64_t table_id, const int64_t partition_id, ObPartitionLocation& location,
      const int64_t expire_renew_time, bool&);

  int add_location(ObFakePartitionKey key, ObPartitionLocation location);

private:
  common::hash::ObHashMap<ObFakePartitionKey, ObPartitionLocation> partition_loc_map_;
  DISALLOW_COPY_AND_ASSIGN(ObFakePartitionLocationCache);
};
}  // namespace share
}  // namespace oceanbase
#endif /* OCEANBASE_FAKE_PARTITION_LOCATION_CACHE_ */
