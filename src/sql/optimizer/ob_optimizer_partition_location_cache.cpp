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

#define USING_LOG_PREFIX SQL_OPT
#include "sql/optimizer/ob_optimizer_partition_location_cache.h"

namespace oceanbase {
namespace sql {
using namespace share;
using namespace common;
using namespace share::schema;

ObOptimizerPartitionLocationCache::ObOptimizerPartitionLocationCache(
    ObIAllocator& allocator, ObIPartitionLocationCache* location_cache)
    : allocator_(allocator), location_cache_(location_cache), optimizer_cache_()
{
  // create hash map
  int ret = OB_SUCCESS;
  if (OB_FAIL(optimizer_cache_.create(LOCATION_CACHE_BUCKET_NUM, ObModIds::OB_RS_SYS_LOCATION_CACHE))) {
    BACKTRACE(ERROR, true, "failed to create hash map");
  } else {
    // do nothing
  }
}

ObOptimizerPartitionLocationCache::~ObOptimizerPartitionLocationCache()
{
  for (PartitionLocationMap::iterator it = optimizer_cache_.begin(); it != optimizer_cache_.end(); it++) {
    if (OB_ISNULL(it->second)) {
      BACKTRACE(ERROR, true, "partition location is null");
    } else {
      it->second->~ObPartitionLocation();
      it->second = NULL;
    }
  }
  optimizer_cache_.destroy();
}

int ObOptimizerPartitionLocationCache::get(const uint64_t table_id, const int64_t partition_id,
    ObPartitionLocation& location, const int64_t expire_renew_time, bool& is_cache_hit,
    const bool auto_update /*=true*/)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == table_id || partition_id == OB_INVALID_ID) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid input", K(ret), K(table_id), K(partition_id));
  } else if (is_link_table_id(table_id)) {
    if (OB_FAIL(get_link_table_location(table_id, location))) {
      LOG_WARN("failed to get link table location", K(ret), K(table_id));
    }
  } else {
    ObLocationCacheKey key(table_id, partition_id);
    ObPartitionLocation* partition_location = NULL;
    ret = optimizer_cache_.get_refactored(key, partition_location);
    if (expire_renew_time != 0 || OB_HASH_NOT_EXIST == ret) {
      if (OB_FAIL(
              location_cache_->get(table_id, partition_id, location, expire_renew_time, is_cache_hit, auto_update))) {
        LOG_WARN("failed to get location from location cache", K(table_id), K(partition_id), K(ret));
      } else if (OB_FAIL(ObPartitionLocation::alloc_new_location(allocator_, partition_location))) {
        LOG_WARN("fail to alloc location", KR(ret), K(table_id), K(partition_id), K(expire_renew_time));
      } else {
        if (OB_ISNULL(partition_location)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("null partition location", K(ret));
        } else if (OB_FAIL(partition_location->assign(location))) {
          LOG_WARN("failed to assign partition location", K(location), K(*partition_location), K(ret));
        } else if (OB_FAIL(insert_or_replace_optimizer_cache(key, partition_location))) {
          LOG_WARN("failed to insert/replace location into optimizer cache", K(key), K(*partition_location), K(ret));
        } else {
          // do nothing
        }
        if (OB_FAIL(ret) && NULL != partition_location) {
          partition_location->~ObPartitionLocation();
          partition_location = NULL;
        }
      }

    } else if (OB_FAIL(ret)) {
      LOG_WARN("failed to get key from optimizer cache", K(ret));
    } else {
      is_cache_hit = true;
      if (OB_ISNULL(partition_location)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null partition location", K(ret));
      } else if (OB_FAIL(location.assign(*partition_location))) {
        LOG_WARN("failed to assign partition location cache", K(*partition_location), K(location), K(ret));
      }
    }
  }
  return ret;
}

int ObOptimizerPartitionLocationCache::insert_or_replace_optimizer_cache(
    ObLocationCacheKey& key, ObPartitionLocation* location)
{
  int ret = OB_SUCCESS;
  ObPartitionLocation* temp_location = NULL;
  ret = optimizer_cache_.get_refactored(key, temp_location);
  if (OB_SUCC(ret)) {
    // delete key from optimizer_cache
    if (OB_ISNULL(temp_location)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null partition location", K(ret));
    } else {
      temp_location->~ObPartitionLocation();
      temp_location = NULL;
      if (OB_FAIL(optimizer_cache_.erase_refactored(key))) {
        LOG_WARN("failed to erase key from optimizer cache", K(key), K(ret));
      }
    }
  } else if (OB_HASH_NOT_EXIST == ret) {
    ret = OB_SUCCESS;
  } else {
    LOG_WARN("failed to get key from location cache", K(key), K(ret));
  }
  // insert key into optimizer cache
  if (OB_SUCC(ret)) {
    if (OB_FAIL(optimizer_cache_.set_refactored(key, location))) {
      LOG_WARN("failed to put partition location into optimizer cache", K(key), K(*location), K(ret));
    }
  }

  return ret;
}

int ObOptimizerPartitionLocationCache::get(
    const ObPartitionKey& partition, ObPartitionLocation& location, const int64_t expire_renew_time, bool& is_cache_hit)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(location_cache_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null location cache", K(ret));
  } else {
    if (OB_FAIL(location_cache_->get(partition, location, expire_renew_time, is_cache_hit))) {
      LOG_WARN("failed to get location cache", K(partition), K(expire_renew_time), K(ret));
    } else {
      // do nothing
    }
  }
  return ret;
}

int ObOptimizerPartitionLocationCache::get(const uint64_t table_id, ObIArray<share::ObPartitionLocation>& locations,
    const int64_t expire_renew_time, bool& is_cache_hit, const bool auto_update /*=true*/)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(location_cache_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null location cache", K(ret));
  } else {
    if (OB_FAIL(location_cache_->get(table_id, locations, expire_renew_time, is_cache_hit, auto_update))) {
      LOG_WARN("failed to get location cache", K(table_id), K(ret));
    } else {
      // do nothing
    }
  }
  return ret;
}

int ObOptimizerPartitionLocationCache::get_strong_leader(
    const ObPartitionKey& partition, ObAddr& leader, const bool force_renew)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(location_cache_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null location cache", K(ret));
  } else {
    if (OB_FAIL(location_cache_->get_strong_leader(partition, leader, force_renew))) {
      LOG_WARN("failed to get location leader", K(partition), K(ret));
    } else {
      // do nothing
    }
  }
  return ret;
}

// return OB_LOCATION_NOT_EXIST if partition's location not in cache
int ObOptimizerPartitionLocationCache::nonblock_get(
    const uint64_t table_id, const int64_t partition_id, ObPartitionLocation& location, const int64_t cluster_id)
{
  int ret = OB_SUCCESS;
  UNUSED(cluster_id);
  if (OB_ISNULL(location_cache_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null location cache", K(ret));
  } else {
    if (OB_FAIL(location_cache_->nonblock_get(table_id, partition_id, location))) {
      LOG_WARN("failed to do nonblock location get", K(table_id), K(partition_id), K(ret));
    } else {
      // do nothing
    }
  }
  return ret;
}

// return OB_LOCATION_NOT_EXIST if partition's location not in cache
int ObOptimizerPartitionLocationCache::nonblock_get(
    const ObPartitionKey& partition, ObPartitionLocation& location, const int64_t cluster_id)
{
  int ret = OB_SUCCESS;
  UNUSED(cluster_id);
  if (OB_ISNULL(location_cache_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null location cache", K(ret));
  } else {
    if (OB_FAIL(location_cache_->nonblock_get(partition, location))) {
      LOG_WARN("failed to do nonblock location get", K(partition), K(ret));
    } else {
      // do nothing
    }
  }
  return ret;
}

// return OB_LOCATION_NOT_EXIST if partition's location not in cache,
// return OB_LOCATION_LEADER_NOT_EXIST if partition's location in cache, but leader not exist
int ObOptimizerPartitionLocationCache::nonblock_get_strong_leader(const ObPartitionKey& partition, ObAddr& leader)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(location_cache_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null location cache", K(ret));
  } else {
    if (OB_FAIL(location_cache_->nonblock_get_strong_leader(partition, leader))) {
      LOG_WARN("failed to do nonblock leader get", K(partition), K(ret));
    } else {
      // do nothing
    }
  }
  return ret;
}

// trigger a location update task and clear location in cache
int ObOptimizerPartitionLocationCache::nonblock_renew(
    const ObPartitionKey& partition, const int64_t expire_renew_time, const int64_t cluster_id)
{
  int ret = OB_SUCCESS;
  UNUSED(cluster_id);
  if (OB_ISNULL(location_cache_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null location cache", K(ret));
  } else if (OB_FAIL(location_cache_->nonblock_renew(partition, expire_renew_time))) {
    LOG_WARN("failed to do nonblock renew", K(partition), K(ret));
  } else {
    // do nothing
  }
  return ret;
}

int ObOptimizerPartitionLocationCache::nonblock_renew_with_limiter(
    const ObPartitionKey& partition, const int64_t expire_renew_time, bool& is_limited)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(location_cache_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null location cache", K(ret));
  } else if (OB_FAIL(location_cache_->nonblock_renew_with_limiter(partition, expire_renew_time, is_limited))) {
    LOG_WARN("failed to do nonblock renew with limiter", K(ret), K(partition), K(expire_renew_time), K(is_limited));
  }
  return ret;
}

// link table.

int ObOptimizerPartitionLocationCache::get_link_table_location(const uint64_t table_id, ObPartitionLocation& location)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(location_cache_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("location cache is NULL", K(ret));
  } else if (OB_FAIL(location_cache_->get_link_table_location(table_id, location))) {
    LOG_WARN("get link table location failed", K(ret), K(table_id));
  }
  return ret;
}

} /* namespace sql */
} /* namespace oceanbase */
