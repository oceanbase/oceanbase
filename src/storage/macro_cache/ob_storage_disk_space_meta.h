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

#ifndef OCEANBASE_STORAGE_MACRO_CACHE_OB_STORAGE_DISK_SPACE_META_H_
#define OCEANBASE_STORAGE_MACRO_CACHE_OB_STORAGE_DISK_SPACE_META_H_

#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace storage
{
struct ObStorageCacheHitStat
{
  ObStorageCacheHitStat();
  ~ObStorageCacheHitStat();
  ObStorageCacheHitStat(const bool is_hit, const int64_t delta_cnt, const int64_t delta_size)
  {
    reset();
    if (is_hit) {
      update_cache_hit(delta_cnt, delta_size);
    } else {
      update_cache_miss(delta_cnt, delta_size);
    }
  }
  void reset();

  void update_cache_hit(const int64_t delta_cnt, const int64_t delta_size);
  void update_cache_miss(const int64_t delta_cnt, const int64_t delta_size);

  int64_t get_hit_cnt() const
  {
    return ATOMIC_LOAD(&cache_hit_cnt_);
  }
  int64_t get_hit_bytes() const
  {
    return ATOMIC_LOAD(&cache_hit_bytes_);
  }
  int64_t get_miss_cnt() const
  {
    return ATOMIC_LOAD(&cache_miss_cnt_);
  }
  int64_t get_miss_bytes() const
  {
    return ATOMIC_LOAD(&cache_miss_bytes_);
  }

  TO_STRING_KV(K(cache_hit_cnt_), K(cache_miss_cnt_), K(cache_hit_bytes_), K(cache_miss_bytes_));

  int64_t cache_hit_cnt_;
  int64_t cache_hit_bytes_;
  int64_t cache_miss_cnt_;
  int64_t cache_miss_bytes_;
};

struct ObStorageCacheStat
{
  ObStorageCacheStat();
  ~ObStorageCacheStat();
  void reset();

  void update_cache_stat(
      const int64_t delta_macro_count,
      const int64_t delta_allocated_size,
      const int64_t delta_used_size);

  TO_STRING_KV(K(macro_count_), K(allocated_size_), K(used_size_));

  int64_t macro_count_;
  int64_t allocated_size_;
  int64_t used_size_;
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_MACRO_CACHE_OB_STORAGE_DISK_SPACE_META_H_
