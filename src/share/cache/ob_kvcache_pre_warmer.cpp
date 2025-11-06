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
ObDataBlockCachePreWarmer::ObDataBlockCachePreWarmer(const int64_t fixed_percentage)
  : share::ObIPreWarmer(),
    fixed_percentage_(fixed_percentage),
    base_percentage_(0),
    cache_(nullptr),
    rest_size_(0),
    warm_size_percentage_(100),
    update_step_(0),
    kvpair_(nullptr),
    inst_handle_(),
    cache_handle_(),
    table_read_info_(nullptr)
{
}

ObDataBlockCachePreWarmer::~ObDataBlockCachePreWarmer()
{
  reset();
}

void ObDataBlockCachePreWarmer::reset()
{
  cache_ = nullptr;
  fixed_percentage_ = 0;
  base_percentage_ = 0;
  rest_size_ = 0;
  warm_size_percentage_ = 100;
  update_step_ = 0;
  table_read_info_ = nullptr;
  is_inited_ = false;
  reuse();
}

void ObDataBlockCachePreWarmer::reuse()
{
  kvpair_ = nullptr;
  inst_handle_.reset();
  cache_handle_.reset();
}

int ObDataBlockCachePreWarmer::init(const ObITableReadInfo *table_read_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "pre warmer init twice", K(ret));
  } else {
    table_read_info_ = table_read_info;
    cache_ = &OB_STORE_CACHE.get_block_cache();
    warm_size_percentage_ = DATA_BLOCK_CACHE_PERCENTAGE;
    inner_update_rest();
    is_inited_ = true;
  }
  return ret;
}

int ObDataBlockCachePreWarmer::reserve(const blocksstable::ObMicroBlockDesc &micro_block_desc,
                                       bool &reserve_succ_flag, const int64_t level)
{
  int ret = OB_SUCCESS;
  reserve_succ_flag = false;
  bool warm_block = false;
  int64_t kvpair_size = 0;
  if (IS_NOT_INIT) {
    // do nothing, and do not return errno
  } else if (OB_UNLIKELY(nullptr == cache_)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "The block cache pre warmer is not inited", K(ret), KP(cache_));
  } else if (OB_UNLIKELY(!micro_block_desc.is_complete_micro_block_memory())) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid micro_block_desc argument, header does not point to a complete micro block buffer", K(ret), K(micro_block_desc));
  } else if (OB_UNLIKELY(!micro_block_desc.is_valid() || level < 0)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Invalid argument", K(ret), K(micro_block_desc), K(level));
  } else if (FALSE_IT(warm_block = fixed_percentage_ > 0 ? warm_block_for_percentage() : warm_block_for_memory(level))) {
  } else if (!warm_block) {
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    if (FALSE_IT(reuse())) {
    } else if (OB_FAIL(do_reserve_kvpair(micro_block_desc, kvpair_size))) {
      COMMON_LOG(WARN, "Fail to reserve block cache value", K(ret), K(micro_block_desc));
    } else {
      rest_size_ = MAX(0, rest_size_ - kvpair_size);
      reserve_succ_flag = true;
    }
    update_rest();
  }
  COMMON_LOG(DEBUG, "pre warmer reserve cache value details", K(ret), K(kvpair_size), K(level),                                                            K(micro_block_desc));
  return ret;
}

int ObDataBlockCachePreWarmer::add(
  const blocksstable::ObMicroBlockDesc &micro_block_desc,
  const bool reserve_succ_flag)
{
  int ret = OB_SUCCESS;

  blocksstable::ObIMicroBlockCache::BaseBlockCache *kvcache = nullptr;
  if (IS_NOT_INIT) {
    // do nothing, and do not return errno
  } else if (!reserve_succ_flag) {
    // do nothing
  } else if (OB_ISNULL(cache_)) {
    ret = OB_ERR_UNEXPECTED;
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

bool ObDataBlockCachePreWarmer::warm_block_for_memory(const int64_t level)
{
  bool bret = level >= TOP_LEVEL;
  if (!bret && rest_size_ > 0) {
    int64_t threshold = MIN(base_percentage_ + 5 * level, 100);
    int64_t random_value = ObTimeUtility::fast_current_time() % 100;
    if (100 > threshold) {
      bret = random_value <= threshold;
    }
    COMMON_LOG(DEBUG, "block cache pre warmer filter", K(bret), K(base_percentage_), K(level), K(update_step_),
                                                       K(random_value), K(threshold));
  }
  return bret;
}

bool ObDataBlockCachePreWarmer::warm_block_for_percentage()
{
  const int64_t random_value = ObRandom::rand(0, 99);
  const bool bret = random_value < fixed_percentage_;
  COMMON_LOG(DEBUG, "warm block for percentage", K(bret), K(random_value), K_(fixed_percentage));
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
    if (micro_block_desc.logic_micro_id_.is_valid()) {
      static_cast<blocksstable::ObMicroBlockCacheKey *>(kvpair_->key_)->set(
        MTL_ID(),
        micro_block_desc.logic_micro_id_,
        micro_block_desc.header_->data_checksum_);
    } else {
      static_cast<blocksstable::ObMicroBlockCacheKey *>(kvpair_->key_)->set(
        MTL_ID(),
        micro_block_desc.macro_id_,
        micro_block_desc.block_offset_,
        micro_block_desc.buf_size_ + micro_block_desc.header_->header_size_);
    }
    if (OB_FAIL(kvcache.put_kvpair(inst_handle_, kvpair_, cache_handle_))) {
      COMMON_LOG(WARN, "failed to put kvpair to block cache", K(ret));
    }
  }
  return ret;
}

/*
 * -------------------------------------------- ObIndexBlockCachePreWarmer --------------------------------------------
 */

ObIndexBlockCachePreWarmer::ObIndexBlockCachePreWarmer(const int64_t fixed_percentage)
  : ObDataBlockCachePreWarmer(fixed_percentage),
    allocator_("IdxBlkPreWarmer", OB_MALLOC_MIDDLE_BLOCK_SIZE, MTL_ID()),
    idx_transformer_(),
    key_(),
    value_()
{
}

ObIndexBlockCachePreWarmer::~ObIndexBlockCachePreWarmer()
{
}

int ObIndexBlockCachePreWarmer::init(const ObITableReadInfo *table_read_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "pre warmer init twice", K(ret));
  } else {
    table_read_info_ = table_read_info;
    cache_ = &OB_STORE_CACHE.get_index_block_cache();
    warm_size_percentage_ = INDEX_BLOCK_BASE_PERCENTAGE;
    inner_update_rest();
    is_inited_ = true;
  }
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
  if (OB_FAIL(idx_transformer_.transform(micro_data, value_.get_block_data(), allocator_, allocated_buf, table_read_info_))) {
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
    if (micro_block_desc.logic_micro_id_.is_valid()) {
      key_.set(
        MTL_ID(),
        micro_block_desc.logic_micro_id_,
        micro_block_desc.header_->data_checksum_);
    } else {
      key_.set(
        MTL_ID(),
        micro_block_desc.macro_id_,
        micro_block_desc.block_offset_,
        micro_block_desc.buf_size_ + micro_block_desc.header_->header_size_);
    }
    if (OB_FAIL(kvcache.put(key_, value_))) {
      COMMON_LOG(WARN, "failed to put index block to cache", K(ret), K_(key), K_(value));
    }
  }
  return ret;
}

};  // common
};  // oceanbase
