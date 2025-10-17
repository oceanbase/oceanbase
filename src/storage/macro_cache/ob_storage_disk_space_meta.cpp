/**
 * Copyright (c) 2024 OceanBase
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

#include "storage/macro_cache/ob_storage_disk_space_meta.h"
#include "lib/atomic/ob_atomic.h"

namespace oceanbase
{
namespace storage
{

/*-----------------------------------------ObStorageCacheHitStat-----------------------------------------*/
ObStorageCacheHitStat::ObStorageCacheHitStat()
{
  reset();
}

ObStorageCacheHitStat::~ObStorageCacheHitStat()
{
  reset();
}

void ObStorageCacheHitStat::reset()
{
  cache_hit_cnt_ = 0;
  cache_hit_bytes_ = 0;
  cache_miss_cnt_ = 0;
  cache_miss_bytes_ = 0;
}

void ObStorageCacheHitStat::update_cache_hit(const int64_t delta_cnt, const int64_t delta_size)
{
  ATOMIC_AAF(&cache_hit_cnt_, delta_cnt);
  ATOMIC_AAF(&cache_hit_bytes_, delta_size);
}

void ObStorageCacheHitStat::update_cache_miss(const int64_t delta_cnt, const int64_t delta_size)
{
  ATOMIC_AAF(&cache_miss_cnt_, delta_cnt);
  ATOMIC_AAF(&cache_miss_bytes_, delta_size);
}

/*-----------------------------------------ObStorageCacheStat-----------------------------------------*/
ObStorageCacheStat::ObStorageCacheStat()
{
  reset();
}

ObStorageCacheStat::~ObStorageCacheStat()
{
  reset();
}

void ObStorageCacheStat::reset()
{
  macro_count_ = 0;
  allocated_size_ = 0;
  used_size_ = 0;
}

void ObStorageCacheStat::update_cache_stat(
    const int64_t delta_macro_count,
    const int64_t delta_allocated_size,
    const int64_t delta_used_size)
{
  ATOMIC_AAF(&macro_count_, delta_macro_count);
  ATOMIC_AAF(&allocated_size_, delta_allocated_size);
  ATOMIC_AAF(&used_size_, delta_used_size);
}

} // namespace storage
} // namespace oceanbase