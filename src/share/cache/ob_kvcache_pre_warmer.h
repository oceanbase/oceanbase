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
#include "share/storage/ob_i_pre_warmer.h"


namespace oceanbase
{
namespace common
{

class ObDataBlockCachePreWarmer : public share::ObIPreWarmer
{
public:
  ObDataBlockCachePreWarmer(const int64_t fixed_percentage = 0);
  virtual ~ObDataBlockCachePreWarmer();
  void reset();
  virtual void reuse() override;
  virtual int init(const ObITableReadInfo *table_read_info) override;
  virtual int reserve(const blocksstable::ObMicroBlockDesc &micro_block_desc,
                      bool &reserve_succ_flag,
                      const int64_t level = 0) override;
  virtual int add(const blocksstable::ObMicroBlockDesc &micro_block_desc, const bool reserve_succ_flag) override;
  virtual int close() override { return OB_SUCCESS; }
  VIRTUAL_TO_STRING_KV(K_(is_inited), K_(fixed_percentage), K_(base_percentage), K_(rest_size), K_(warm_size_percentage), K_(update_step));
protected:
  void update_rest();
  void inner_update_rest();
  virtual void calculate_base_percentage(const int64_t free_memory);
  virtual int do_reserve_kvpair(
      const blocksstable::ObMicroBlockDesc &micro_block_desc,
      int64_t &kvpair_size);
  virtual int do_put_kvpair(
      const blocksstable::ObMicroBlockDesc &micro_block_desc,
      blocksstable::ObIMicroBlockCache::BaseBlockCache &kvcache);
private:
  bool warm_block_for_memory(const int64_t level);
  bool warm_block_for_percentage();
protected:
  static const int64_t DATA_BLOCK_CACHE_PERCENTAGE = 5;
  static const int64_t UPDATE_INTERVAL = 50;
  static const int64_t TOP_LEVEL = 6;

  int64_t fixed_percentage_;
  int64_t base_percentage_;
  blocksstable::ObIMicroBlockCache *cache_;
  int64_t rest_size_;
  int64_t warm_size_percentage_;
  int64_t update_step_;
  ObKVCachePair *kvpair_;
  ObKVCacheInstHandle inst_handle_;
  ObKVCacheHandle cache_handle_;
  const ObITableReadInfo *table_read_info_;
};

class ObIndexBlockCachePreWarmer : public ObDataBlockCachePreWarmer
{
public:
  ObIndexBlockCachePreWarmer(const int64_t fixed_percentage = 0);
  virtual ~ObIndexBlockCachePreWarmer();
  virtual int init(const ObITableReadInfo *table_read_info) override;
protected:
  virtual void calculate_base_percentage(const int64_t free_memory) override;
  virtual int do_reserve_kvpair(
      const blocksstable::ObMicroBlockDesc &micro_block_desc,
      int64_t &kvpair_size) override;
  virtual int do_put_kvpair(
      const blocksstable::ObMicroBlockDesc &micro_block_desc,
      blocksstable::ObIMicroBlockCache::BaseBlockCache &kvcache) override;
private:
  static const int64_t INDEX_BLOCK_CACHE_PERCENTAGE = 2;
  static const int64_t INDEX_BLOCK_BASE_PERCENTAGE = 30;
  ObArenaAllocator allocator_;
  blocksstable::ObIndexBlockDataTransformer idx_transformer_;
  blocksstable::ObMicroBlockCacheKey key_;
  blocksstable::ObMicroBlockCacheValue value_;
};


};  // common
};  // oceanbase

#endif  // OCEANBASE_COMMON_KVCACHE_PRE_WARMER_H_
