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


#include "ob_kvcache_pre_warmer.h"

namespace oceanbase
{
namespace common
{
/*
 * -------------------------------------------- ObDataBlockCachePreWarmer --------------------------------------------
 */
ObDataBlockCachePreWarmer::ObDataBlockCachePreWarmer()
  : base_percentage_(0),
    cache_(nullptr),
    rest_size_(0),
    warm_size_percentage_(100),
    update_step_(0),
    kvpair_(nullptr),
    inst_handle_(),
    cache_handle_(),
    col_descs_(nullptr)
{
}

ObDataBlockCachePreWarmer::~ObDataBlockCachePreWarmer()
{
  reset();
}

void ObDataBlockCachePreWarmer::reset()
{
  cache_ = nullptr;
  base_percentage_ = 0;
  rest_size_ = 0;
  warm_size_percentage_ = 100;
  update_step_ = 0;
  col_descs_ = nullptr;
  reuse();
}

void ObDataBlockCachePreWarmer::reuse()
{
  kvpair_ = nullptr;
  inst_handle_.reset();
  cache_handle_.reset();
}

int ObDataBlockCachePreWarmer::init(const ObIArray<share::schema::ObColDesc> *col_desc_array)
{
  int ret = OB_SUCCESS;
  col_descs_ = col_desc_array;
  cache_ = &OB_STORE_CACHE.get_block_cache();
  warm_size_percentage_ = DATA_BLOCK_CACHE_PERCENTAGE;
  inner_update_rest();
  return ret;
}

int ObDataBlockCachePreWarmer::reserve_kvpair(const blocksstable::ObMicroBlockDesc &micro_block_desc,
                                              const int64_t level)
{
  int ret = OB_SUCCESS;

  int64_t kvpair_size = 0;
  if (OB_UNLIKELY(nullptr == cache_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The block cache pre warmer is not inited", K(ret), KP(cache_));
  } else if (OB_UNLIKELY(!micro_block_desc.is_valid() || level < 0)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid argument", K(ret), K(micro_block_desc), K(level));
  } else {
    if (level < TOP_LEVEL && (rest_size_ <= 0 || !warm_block(level))) {
      ret = OB_BUF_NOT_ENOUGH;
    } else if (FALSE_IT(reuse())) {
    } else if (OB_FAIL(do_reserve_kvpair(micro_block_desc, kvpair_size))) {
      COMMON_LOG(WARN, "Fail to reserve block cache value", K(ret), K(micro_block_desc));
    } else {
      rest_size_ = MAX(0, rest_size_ - kvpair_size);
    }
    update_rest();
  }
  COMMON_LOG(DEBUG, "pre warmer reserve cache value details", K(ret), K(kvpair_size), K(level),
                                                              K(micro_block_desc));

  return ret;
}

int ObDataBlockCachePreWarmer::update_and_put_kvpair(const blocksstable::ObMicroBlockDesc &micro_block_desc)
{
  int ret = OB_SUCCESS;

  blocksstable::ObIMicroBlockCache::BaseBlockCache *kvcache = nullptr;
  if (OB_ISNULL(cache_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The block cache pre warmer is not inited", K(ret), KP(cache_));
  } else if (OB_FAIL(cache_->get_cache(kvcache))) {
    COMMON_LOG(WARN, "Fail to get block kvcache", K(ret));
  } else if (OB_FAIL(do_put_kvpair(micro_block_desc, *kvcache))) {
    COMMON_LOG(WARN, "Fail to put kvpair into kvcache", K(ret));
  }
  COMMON_LOG(DEBUG, "pre warmer build cache key and put details", K(ret), K(MTL_ID()), K(rest_size_), K(update_step_),
                                                                  K(micro_block_desc), KPC(micro_block_desc.header_));
  // reuse handles outside

  return ret;
}

void ObDataBlockCachePreWarmer::update_rest()
{
  if (OB_UNLIKELY(update_step_++ >= UPDATE_INTERVAL)) {
    inner_update_rest();
  }
}

void ObDataBlockCachePreWarmer::inner_update_rest()
{
  int64_t free_memory = lib::get_tenant_memory_limit(MTL_ID()) - lib::get_tenant_memory_hold(MTL_ID());
  rest_size_ = free_memory / 100 * warm_size_percentage_;
  calculate_base_percentage(free_memory);
  update_step_ = 0;
  COMMON_LOG(DEBUG, "pre warmer update rest", K(rest_size_), K(base_percentage_), K(free_memory));
}

void ObDataBlockCachePreWarmer::calculate_base_percentage(const int64_t free_memory)
{
  base_percentage_ = MIN(free_memory * 200 / lib::get_tenant_memory_limit(MTL_ID()), 50);
}

bool ObDataBlockCachePreWarmer::warm_block(const int64_t level)
{
  bool bret = true;

  int64_t threshold = MIN(base_percentage_ + 5 * level, 100);
  int64_t random_value = ObTimeUtility::fast_current_time() % 100;
  if (100 > threshold) {
    bret = random_value <= threshold;
  }
  COMMON_LOG(DEBUG, "block cache pre warmer filter", K(bret), K(base_percentage_), K(level), K(update_step_),
                                                     K(random_value), K(threshold));

  return bret;
}

int ObDataBlockCachePreWarmer::do_reserve_kvpair(
    const blocksstable::ObMicroBlockDesc &micro_block_desc,
    int64_t &kvpair_size)
{
  return cache_->reserve_kvpair(micro_block_desc, inst_handle_,cache_handle_, kvpair_, kvpair_size);
}

int ObDataBlockCachePreWarmer::do_put_kvpair(
    const blocksstable::ObMicroBlockDesc &micro_block_desc,
    blocksstable::ObIMicroBlockCache::BaseBlockCache &kvcache)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!micro_block_desc.is_valid() || !inst_handle_.is_valid()
                         || !cache_handle_.is_valid() || nullptr == kvpair_)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid argument", K(ret), K(micro_block_desc), K(inst_handle_), K(cache_handle_), K(kvpair_));
  } else {
    static_cast<blocksstable::ObMicroBlockCacheKey *>(kvpair_->key_)->set(
      MTL_ID(),
      micro_block_desc.macro_id_,
      micro_block_desc.block_offset_,
      micro_block_desc.buf_size_ + micro_block_desc.header_->header_size_);
    if (OB_FAIL(kvcache.put_kvpair(inst_handle_, kvpair_, cache_handle_))) {
      COMMON_LOG(WARN, "failed to put kvpair to block cache", K(ret));
    }
  }
  return ret;
}

/*
 * -------------------------------------------- ObIndexBlockCachePreWarmer --------------------------------------------
 */

ObIndexBlockCachePreWarmer::ObIndexBlockCachePreWarmer()
  : ObDataBlockCachePreWarmer(),
    allocator_("IdxBlkPreWarmer", OB_MALLOC_MIDDLE_BLOCK_SIZE, MTL_ID()),
    idx_transformer_(),
    key_(),
    value_()
{
}

ObIndexBlockCachePreWarmer::~ObIndexBlockCachePreWarmer()
{
}

int ObIndexBlockCachePreWarmer::init(const ObIArray<share::schema::ObColDesc> *col_desc_array)
{
  int ret = OB_SUCCESS;
  col_descs_ = col_desc_array;
  cache_ = &OB_STORE_CACHE.get_index_block_cache();
  warm_size_percentage_ = INDEX_BLOCK_BASE_PERCENTAGE;
  inner_update_rest();
  return ret;
}

void ObIndexBlockCachePreWarmer::calculate_base_percentage(const int64_t free_memory)
{
  ObDataBlockCachePreWarmer::calculate_base_percentage(free_memory);
  base_percentage_ += INDEX_BLOCK_BASE_PERCENTAGE;
}

int ObIndexBlockCachePreWarmer::do_reserve_kvpair(
    const blocksstable::ObMicroBlockDesc &micro_block_desc,
    int64_t &kvpair_size)
{
  int ret = OB_SUCCESS;
  allocator_.reuse();
  blocksstable::ObMicroBlockData micro_data(micro_block_desc.get_block_buf(), micro_block_desc.get_block_size());
  char *allocated_buf = nullptr;
  if (OB_FAIL(idx_transformer_.transform(micro_data, value_.get_block_data(), allocator_, allocated_buf, col_descs_))) {
    COMMON_LOG(WARN, "Fail to transform index block to memory format", K(ret));
  } else {
    kvpair_size = sizeof(blocksstable::ObMicroBlockCacheKey) + value_.size();
  }
  return ret;
}

int ObIndexBlockCachePreWarmer::do_put_kvpair(
    const blocksstable::ObMicroBlockDesc &micro_block_desc,
    blocksstable::ObIMicroBlockCache::BaseBlockCache &kvcache)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!micro_block_desc.is_valid() || !value_.get_block_data().is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid argument", K(ret), K(micro_block_desc), K_(value));
  } else {
    key_.set(
        MTL_ID(),
        micro_block_desc.macro_id_,
        micro_block_desc.block_offset_,
        micro_block_desc.buf_size_ + micro_block_desc.header_->header_size_);
    if (OB_FAIL(kvcache.put(key_, value_))) {
      COMMON_LOG(WARN, "failed to put index block to cache", K(ret), K_(key), K_(value));
    }
  }
  return ret;
}

};  // common
};  // oceanbase
