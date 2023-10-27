/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_COMMON_KVCACHE_PRE_WARMER_H_
#define OCEANBASE_COMMON_KVCACHE_PRE_WARMER_H_

#include "storage/blocksstable/ob_micro_block_cache.h"
#include "storage/blocksstable/ob_storage_cache_suite.h"


namespace oceanbase
{
namespace common
{

class ObDataBlockCachePreWarmer
{
public:
  ObDataBlockCachePreWarmer();
  virtual ~ObDataBlockCachePreWarmer();
  void reset();
  void reuse();
  void init();
  OB_INLINE bool is_valid() const { return nullptr != cache_; }
  int reserve_kvpair(const blocksstable::ObMicroBlockDesc &micro_block_desc, const int64_t level = 0);
  int update_and_put_kvpair(const blocksstable::ObMicroBlockDesc &micro_block_desc);
protected:
  int inner_init(const int64_t ratio, blocksstable::ObIMicroBlockCache &block_cache);
  void update_rest();
  void inner_update_rest();
  virtual void calculate_base_percentage(const int64_t free_memory);
private:
  bool warm_block(const int64_t level);
protected:
  int64_t base_percentage_;
private:
  static const int64_t DATA_BLOCK_CACHE_PERCENTAGE = 5;
  static const int64_t UPDATE_INTERVAL = 50;
  static const int64_t TOP_LEVEL = 6;

  blocksstable::ObIMicroBlockCache *cache_;
  int64_t rest_size_;
  int64_t warm_size_percentage_;
  int64_t update_step_;
  ObKVCachePair *kvpair_;
  ObKVCacheInstHandle inst_handle_;
  ObKVCacheHandle cache_handle_;
};

class ObIndexBlockCachePreWarmer : public ObDataBlockCachePreWarmer
{
public:
  ObIndexBlockCachePreWarmer();
  virtual ~ObIndexBlockCachePreWarmer();
  void init();
protected:
  virtual void calculate_base_percentage(const int64_t free_memory) override;
private:
  static const int64_t INDEX_BLOCK_CACHE_PERCENTAGE = 2;
  static const int64_t INDEX_BLOCK_BASE_PERCENTAGE = 30;
};


};  // common
};  // oceanbase

#endif  // OCEANBASE_COMMON_KVCACHE_PRE_WARMER_H_