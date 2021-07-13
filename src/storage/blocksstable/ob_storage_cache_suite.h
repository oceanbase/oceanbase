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

#ifndef __OCEANBASE_BLOCKSSTABLE_STORAGE_CACHE_SUITE_H__
#define __OCEANBASE_BLOCKSSTABLE_STORAGE_CACHE_SUITE_H__

#include "share/schema/ob_table_schema.h"
#include "ob_micro_block_index_cache.h"
#include "ob_micro_block_cache.h"
#include "ob_block_cache_working_set.h"
#include "ob_row_cache.h"
#include "ob_fuse_row_cache.h"
#include "ob_bloom_filter_cache.h"

#define OB_STORE_CACHE oceanbase::blocksstable::ObStorageCacheSuite::get_instance()

namespace oceanbase {
namespace blocksstable {
struct ObStorageCacheContext;

class ObStorageCacheSuite {
public:
  static ObStorageCacheSuite& get_instance();
  int init(const int64_t index_cache_priority, const int64_t user_block_cache_priority,
      const int64_t user_row_cache_priority, const int64_t fuse_row_cache_priority, const int64_t bf_cache_priority,
      const int64_t bf_cache_miss_count_threshold);
  int reset_priority(const int64_t index_cache_priority, const int64_t user_block_cache_priority,
      const int64_t user_row_cache_priority, const int64_t fuse_row_cache_priority, const int64_t bf_cache_priority);
  int set_bf_cache_miss_count_threshold(const int64_t bf_cache_miss_count_threshold);
  ObMicroBlockCache& get_block_cache()
  {
    return user_block_cache_;
  }
  ObMicroBlockIndexCache& get_micro_index_cache()
  {
    return block_index_cache_;
  }
  ObRowCache& get_row_cache()
  {
    return user_row_cache_;
  }
  ObBloomFilterCache& get_bf_cache()
  {
    return bf_cache_;
  }
  ObFuseRowCache& get_fuse_row_cache()
  {
    return fuse_row_cache_;
  }
  void destroy();
  inline bool is_inited() const
  {
    return is_inited_;
  }
  TO_STRING_KV(K(is_inited_));

private:
  ObStorageCacheSuite();
  virtual ~ObStorageCacheSuite();
  friend class ObStorageCacheContext;
  ObMicroBlockIndexCache block_index_cache_;
  ObMicroBlockCache user_block_cache_;
  ObRowCache user_row_cache_;
  ObBloomFilterCache bf_cache_;
  ObFuseRowCache fuse_row_cache_;
  bool is_inited_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObStorageCacheSuite);
};

struct ObStorageCacheContext {
  ObMicroBlockIndexCache* block_index_cache_;
  ObMicroBlockCache* block_cache_;
  ObBlockCacheWorkingSet* block_cache_ws_;
  ObRowCache* row_cache_;
  ObBloomFilterCache* bf_cache_;
  ObStorageCacheContext()
      : block_index_cache_(NULL), block_cache_(NULL), block_cache_ws_(NULL), row_cache_(NULL), bf_cache_(NULL)
  {}
  void set(ObBlockCacheWorkingSet& block_cache_ws);
  bool is_valid() const;
  void reset();
  TO_STRING_KV(KP_(block_index_cache), KP_(block_cache), KP_(block_cache_ws), KP_(row_cache), KP_(bf_cache));
};
}  // namespace blocksstable
}  // namespace oceanbase

#endif  //__OCEANBASE_BLOCKSSTABLE_STORAGE_CACHE_SUITE_H__
