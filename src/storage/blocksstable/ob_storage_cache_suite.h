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

#include "storage/meta_mem/ob_storage_meta_cache.h"
#include "share/schema/ob_table_schema.h"
#include "ob_micro_block_cache.h"
#include "ob_row_cache.h"
#include "ob_fuse_row_cache.h"
#include "ob_bloom_filter_cache.h"

#define OB_STORE_CACHE oceanbase::blocksstable::ObStorageCacheSuite::get_instance()

namespace oceanbase
{
namespace blocksstable
{

class ObStorageCacheSuite
{
public:
  static ObStorageCacheSuite &get_instance();
  int init(
      const int64_t index_block_cache_priority,
      const int64_t user_block_cache_priority,
      const int64_t user_row_cache_priority,
      const int64_t fuse_row_cache_priority,
      const int64_t bf_cache_priority,
      const int64_t bf_cache_miss_count_threshold,
      const int64_t storage_meta_cache_priority);
  int reset_priority(
      const int64_t index_block_cache_priority,
      const int64_t user_block_cache_priority,
      const int64_t user_row_cache_priority,
      const int64_t fuse_row_cache_priority,
      const int64_t bf_cache_priority,
      const int64_t storage_meta_cache_priority);
  int set_bf_cache_miss_count_threshold(const int64_t bf_cache_miss_count_threshold);
  ObDataMicroBlockCache &get_block_cache() { return user_block_cache_; }
  ObIndexMicroBlockCache &get_index_block_cache() { return index_block_cache_; }
  ObDataMicroBlockCache &get_micro_block_cache(const bool is_data_block)
  { return is_data_block ? user_block_cache_ : index_block_cache_; }
  ObRowCache &get_row_cache() { return user_row_cache_; }
  ObBloomFilterCache &get_bf_cache() { return bf_cache_; }
  ObFuseRowCache &get_fuse_row_cache() { return fuse_row_cache_; }
  ObStorageMetaCache &get_storage_meta_cache() { return storage_meta_cache_; }
  void destroy();
  inline bool is_inited() const { return is_inited_; }
  TO_STRING_KV(K(is_inited_));
private:
  ObStorageCacheSuite();
  virtual ~ObStorageCacheSuite();
  ObIndexMicroBlockCache index_block_cache_;
  ObDataMicroBlockCache user_block_cache_;
  ObRowCache user_row_cache_;
  ObBloomFilterCache bf_cache_;
  ObFuseRowCache fuse_row_cache_;
  ObStorageMetaCache storage_meta_cache_;
  bool is_inited_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObStorageCacheSuite);
};

}
}
#endif //__OCEANBASE_BLOCKSSTABLE_STORAGE_CACHE_SUITE_H__
