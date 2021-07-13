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

#ifndef _OB_MOCK_PARTITION_LOCATION_CACHE_H
#define _OB_MOCK_PARTITION_LOCATION_CACHE_H 1
#include "share/partition_table/ob_partition_location_cache.h"
#include "share/partition_table/ob_partition_location.h"
namespace test {
using oceanbase::share::ObPartitionLocation;
using oceanbase::share::ObReplicaLocation;

class MockPartitionLocationCache : public oceanbase::share::ObPartitionLocationCache {
public:
  MockPartitionLocationCache() : ObPartitionLocationCache(fetcher_)
  {}
  virtual ~MockPartitionLocationCache()
  {}

  // get partition location of a partition
  virtual int get(const uint64_t table_id, const int64_t partition_id, ObPartitionLocation& location,
      const int64_t expire_renew_time, bool& is_cache_hit, const bool auto_update = true);

  virtual int nonblock_get(const uint64_t table_id, const int64_t partition_id, ObPartitionLocation& location,
      const int64_t cluster_id = -1);

private:
  oceanbase::share::ObLocationFetcher fetcher_;
};

}  // end namespace test

#endif /* _OB_MOCK_PARTITION_LOCATION_CACHE_H */
