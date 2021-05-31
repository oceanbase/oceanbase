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

#include "ob_mock_partition_location_cache.h"
using namespace oceanbase::common;
namespace test {
int MockPartitionLocationCache::get(const uint64_t table_id, const int64_t partition_id, ObPartitionLocation& location,
    const int64_t expire_renew_time, bool& is_cache_hit, const bool auto_update /*=true*/)
{
  int ret = OB_SUCCESS;
  location.set_table_id(table_id);
  location.set_partition_id(partition_id);
  location.set_partition_cnt(partition_id + 1);
  UNUSEDx(expire_renew_time, auto_update);
  is_cache_hit = true;
  ObReplicaLocation replica_location;
  switch (partition_id) {
    case 0: {
      replica_location.server_.set_ip_addr("1.1.1.1", 8888);
      break;
    }
    case 1: {
      replica_location.server_.set_ip_addr("2.2.2.2", 8888);
      break;
    }
    case 2: {
      replica_location.server_.set_ip_addr("3.3.3.3", 8888);
      break;
    }
    case 3: {
      replica_location.server_.set_ip_addr("3.3.3.3", 8888);
      break;
    }
    case 4: {
      replica_location.server_.set_ip_addr("4.4.4.4", 8888);
      break;
    }
    case 5: {
      replica_location.server_.set_ip_addr("5.5.5.5", 8888);
      break;
    }
    default: {
      replica_location.server_.set_ip_addr("1.1.1.1", 8888);
      break;
    }
  }
  replica_location.role_ = LEADER;
  replica_location.replica_type_ = REPLICA_TYPE_FULL;
  location.add(replica_location);
  return ret;
}

int MockPartitionLocationCache::nonblock_get(
    const uint64_t table_id, const int64_t partition_id, ObPartitionLocation& location, const int64_t cluster_id)
{
  bool is_cache_hit = true;
  UNUSED(cluster_id);
  return get(table_id, partition_id, location, 0, is_cache_hit);
}

}  // end of namespace test
