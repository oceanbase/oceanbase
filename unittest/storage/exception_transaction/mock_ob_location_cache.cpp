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

#include "mock_ob_location_cache.h"

namespace oceanbase {

using namespace common;
using namespace common::hash;

namespace share {

int MockObLocationCache::init()
{
  int ret = OB_SUCCESS;
  int tmp_ret = 0;

  if (0 != (tmp_ret = partition_addr_map_.create(BUCKET_NUM, ObModIds::OB_HASH_NODE))) {
    OB_LOG(ERROR, "create hashmap for MockObLocationCache error", K(tmp_ret));
    ret = OB_ERR_UNEXPECTED;
  } else {
    ret = OB_SUCCESS;
  }

  return ret;
}

int MockObLocationCache::add(const ObPartitionKey& partition, const ObAddr& leader)
{
  int ret = OB_SUCCESS;
  int tmp_ret = 0;

  if (!partition.is_valid() || !leader.is_valid()) {
    OB_LOG(WARN, "invalid argument", K(partition), K(leader));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_SUCCESS != (tmp_ret = partition_addr_map_.set_refactored(partition, leader))) {
    if (OB_HASH_EXIST != tmp_ret) {
      OB_LOG(WARN, "add partition leader pair error", K(tmp_ret), K(partition), K(leader));
      ret = OB_ERR_UNEXPECTED;
    }
  } else {
    // do nothing
  }

  return ret;
}

// overwrite
int MockObLocationCache::add_overwrite(const ObPartitionKey& partition, const ObAddr& leader)
{
  int ret = OB_SUCCESS;
  int tmp_ret = 0;

  if (!partition.is_valid() || !leader.is_valid()) {
    OB_LOG(WARN, "invalid argument", K(partition), K(leader));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_SUCCESS != (tmp_ret = partition_addr_map_.set_refactored(partition, leader, 1))) {
    if (OB_SUCCESS != tmp_ret) {
      OB_LOG(WARN, "add partition leader pair error", K(tmp_ret), K(partition), K(leader));
      ret = OB_ERR_UNEXPECTED;
    }
  } else {
    // do nothing
  }

  return ret;
}

int MockObLocationCache::remove(const ObPartitionKey& partition)
{
  int ret = OB_SUCCESS;
  int tmp_ret = 0;

  if (!partition.is_valid()) {
    OB_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_SUCCESS != (tmp_ret = partition_addr_map_.erase_refactored(partition))) {
    OB_LOG(WARN, "erase partition leader pair error", K(tmp_ret), K(partition));
  } else if (OB_SUCCESS == tmp_ret) {
    OB_LOG(INFO, "erase partition leader pair success", K(tmp_ret), K(partition));
  } else if (OB_HASH_NOT_EXIST == tmp_ret) {
    OB_LOG(INFO, "erase partition leader pair not exist", K(tmp_ret), K(partition));
  } else {
    // do nothing
  }

  return ret;
}

int MockObLocationCache::get(const uint64_t table_id, const int64_t partition_id, ObPartitionLocation& location,
    const int64_t expire_renew_time, bool& is_cache_hit, const bool auto_update /*=true*/)
{
  UNUSEDx(table_id, partition_id, location, expire_renew_time, is_cache_hit, auto_update);
  return OB_SUCCESS;
}

int MockObLocationCache::get(
    const ObPartitionKey& partition, ObPartitionLocation& location, const int64_t expire_renew_time, bool& is_cache_hit)
{
  UNUSEDx(partition, location, is_cache_hit);
  return OB_SUCCESS;
}

int MockObLocationCache::get(const uint64_t table_id, common::ObIArray<ObPartitionLocation>& locations,
    const int64_t expire_renew_time, bool& is_cache_hit, const bool auto_update /*=true*/)
{
  UNUSEDx(table_id, locations, expire_renew_time, is_cache_hit, auto_update);
  return OB_SUCCESS;
}

int MockObLocationCache::get_leader(const ObPartitionKey& partition, ObAddr& leader, const bool force_renew)
{
  int ret = OB_SUCCESS;
  int tmp_ret = 0;

  UNUSED(force_renew);

  if (!partition.is_valid()) {
    OB_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_SUCCESS != (tmp_ret = partition_addr_map_.get_refactored(partition, leader))) {
    OB_LOG(WARN, "get leader by partition error", K(tmp_ret), K(partition));
    ret = OB_LOCATION_LEADER_NOT_EXIST;
  } else {
    // do nothing
  }

  return ret;
}

int MockObLocationCache::nonblock_get(
    const uint64_t table_id, const int64_t partition_id, ObPartitionLocation& location, const int64_t cluster_id)
{
  UNUSED(table_id);
  UNUSED(partition_id);
  UNUSED(location);
  UNUSED(cluster_id);

  return OB_SUCCESS;
}

int MockObLocationCache::nonblock_get(
    const ObPartitionKey& partition, ObPartitionLocation& location, const int64_t cluster_id)
{
  UNUSED(partition);
  UNUSED(location);
  UNUSED(cluster_id);

  return OB_SUCCESS;
}

int MockObLocationCache::nonblock_get(
    const uint64_t table_id, const int64_t partition_num, ObIArray<ObPartitionLocation>& locations)
{
  UNUSED(table_id);
  UNUSED(partition_num);
  UNUSED(locations);

  return OB_SUCCESS;
}
int MockObLocationCache::nonblock_get_leader_across_cluster(
    const ObPartitionKey& partition, ObAddr& leader, int64_t& cluster_id)
{
  UNUSED(partition);
  UNUSED(leader);
  UNUSED(cluster_id);
  return OB_SUCCESS;
}

int MockObLocationCache::nonblock_get_leader(const ObPartitionKey& partition, ObAddr& leader)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  UNUSED(leader);

  if (!partition.is_valid()) {
    OB_LOG(WARN, "invalid argument", K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_SUCCESS != (tmp_ret = partition_addr_map_.get_refactored(partition, leader))) {
    OB_LOG(WARN, "get leader by partition error", K(tmp_ret), K(partition));
    ret = OB_LOCATION_LEADER_NOT_EXIST;
  } else {
    // do nothing
  }

  return ret;
}
int MockObLocationCache::nonblock_renew(const ObPartitionKey& partition, const int64_t expire_time)
{
  UNUSED(partition);
  UNUSED(expire_time);
  return OB_SUCCESS;
}
int MockObLocationCache::nonblock_renew_across_cluster(const ObPartitionKey& partition, const int64_t expire_time)
{
  UNUSED(partition);
  UNUSED(expire_time);
  return OB_SUCCESS;
}

int MockObLocationCache::nonblock_renew_with_limiter(
    const ObPartitionKey& partition, const int64_t expire_renew_time, bool& is_limited)
{
  UNUSED(partition);
  UNUSED(expire_renew_time);
  UNUSED(is_limited);
  return OB_SUCCESS;
}
}  // namespace share
}  // namespace oceanbase
