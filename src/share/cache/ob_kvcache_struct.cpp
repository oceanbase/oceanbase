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

#include "ob_kvcache_struct.h"

namespace oceanbase
{
namespace common
{
/**
 * ------------------------------------------------------------ObKVCacheConfig---------------------------------------------------------
 */
ObKVCacheConfig::ObKVCacheConfig()
  : is_valid_(false),
    priority_(0)
{
  MEMSET(cache_name_, 0, MAX_CACHE_NAME_LENGTH);
}

void ObKVCacheConfig::reset()
{
  is_valid_ = false;
  priority_ = 0;
  mem_limit_pct_ = 100;
  MEMSET(cache_name_, 0, MAX_CACHE_NAME_LENGTH);
}

/**
 * ------------------------------------------------------------ObKVCacheStatus----------------------------------------------------------
 */
ObKVCacheStatus::ObKVCacheStatus()
{
  reset();
}

double ObKVCacheStatus::get_hit_ratio() const
{
  double hit_ratio = 0;
  int64_t total_hit_cnt = total_hit_cnt_.value();
  int64_t get_cnt = total_hit_cnt + total_miss_cnt_;
  if (get_cnt > 0) {
    hit_ratio = double(total_hit_cnt) / double(get_cnt);
  }
  return hit_ratio;
}

void ObKVCacheStatus::reset()
{
  config_ = NULL;
  kv_cnt_ = 0;
  store_size_ = 0;
  map_size_ = 0;
  lru_mb_cnt_ = 0;
  lfu_mb_cnt_ = 0;
  total_put_cnt_.reset();
  total_hit_cnt_.reset();
  total_miss_cnt_ = 0;
  last_hit_cnt_ = 0;
  base_mb_score_ = 0;
  hold_size_ = 0;
  total_miss_cnt_ = 0;
}

/*
 * -------------------------------------------------------------ObKVStoreMemBlock--------------------------------------------------------
 */
ObKVStoreMemBlock::ObKVStoreMemBlock(char *buffer, const int64_t size)
    : payload_size_(size), buffer_(buffer)
{
  atomic_pos_.buffer = 0;
  atomic_pos_.pairs = 0;
}

ObKVStoreMemBlock::~ObKVStoreMemBlock()
{
}

void ObKVStoreMemBlock::reuse()
{
  if (NULL != buffer_) {
    int64_t pos = 0;
    ObKVCachePair *kvpair = NULL;

    for (uint32_t i = 0; i < atomic_pos_.pairs; ++i) {
      kvpair = reinterpret_cast<ObKVCachePair*>(buffer_ + pos);
      if (NULL != kvpair->key_) {
        kvpair->key_->~ObIKVCacheKey();
      }
      if (NULL != kvpair->value_) {
        kvpair->value_->~ObIKVCacheValue();
      }
      pos += kvpair->size_;
    }
  }
  atomic_pos_.buffer = 0;
  atomic_pos_.pairs = 0;
}

int64_t ObKVStoreMemBlock::upper_align(int64_t input, int64_t align)
{
  return (input + align - 1) & ~(align - 1);
}

/**
 * we could create key and value struct instance in memblock, if
 * we can ensure that the start address of each block data is 8
 * bytes aligned, and the compiler can ensure the struct size is
 * 8 bytes aligned on 64 bit system, then we create key and
 * value struct instance at the beginning of block data, so the
 * start address of key instance or value instance is 8 bytes
 * aligned. it can make the cpu run faster than unaligned
 * instance.
 */
int64_t ObKVStoreMemBlock::get_align_size(const ObIKVCacheKey &key, const ObIKVCacheValue &value)
{
  return get_align_size(key.size(), value.size());
}

int64_t ObKVStoreMemBlock::get_align_size(const int64_t key_size, const int64_t value_size)
{
  return upper_align(sizeof(ObKVCachePair) + key_size + value_size, ALIGN_SIZE);
}

int ObKVStoreMemBlock::store(
  const ObIKVCacheKey &key,
  const ObIKVCacheValue &value,
  ObKVCachePair *&kvpair)
{
  int ret = OB_SUCCESS;
  int32_t align_kv_size = static_cast<int32_t>(get_align_size(key, value));
  AtomicInt64 old_atomic_pos = { 0 };
  AtomicInt64 new_atomic_pos = { 0 };
  kvpair = NULL;

  //check parameters
  if (OB_UNLIKELY(NULL == buffer_)) {
    ret = OB_INVALID_DATA;
    COMMON_LOG(WARN, "The mem block is invalid, ", KP_(buffer), K(ret));
  } else if (OB_UNLIKELY(key.size() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "The size of key is 0, ", K(ret));
  } else if (OB_UNLIKELY(value.size() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "The size of value is 0, ", K(ret));
  }

  //find write position
  while (OB_SUCC(ret)) {
    old_atomic_pos.atomic = ATOMIC_LOAD(&atomic_pos_.atomic);
    new_atomic_pos.atomic = old_atomic_pos.atomic;
    if (old_atomic_pos.buffer + align_kv_size > payload_size_) {
      ret = OB_BUF_NOT_ENOUGH;
      break;
    } else {
      new_atomic_pos.buffer += align_kv_size;
      new_atomic_pos.pairs += 1;
      if (ATOMIC_BCAS(&(atomic_pos_.atomic), old_atomic_pos.atomic, new_atomic_pos.atomic)) {
        break;
      }
    }
  }

  //if has found store pos, then store the kv
  if (OB_SUCC(ret)) {
    ObKVCachePair store_pair;
    store_pair.size_ = align_kv_size;

    if (OB_FAIL(key.deep_copy(&(buffer_[old_atomic_pos.buffer + sizeof(ObKVCachePair)]),
            key.size(),
            store_pair.key_))) {
      COMMON_LOG(WARN, "Fail to deep copy key, ", K(ret));
    } else if (OB_FAIL(value.deep_copy(&buffer_[old_atomic_pos.buffer
            + sizeof(ObKVCachePair)
            + key.size()],
            value.size(),
            store_pair.value_))) {
      COMMON_LOG(WARN, "Fail to deep copy value, ", K(ret));
    } else {
      MEMCPY(&(buffer_[old_atomic_pos.buffer]), &store_pair, sizeof(ObKVCachePair));
      kvpair = reinterpret_cast<ObKVCachePair*>(&(buffer_[old_atomic_pos.buffer]));
    }

    if (OB_FAIL(ret)) {
      if (NULL != store_pair.key_) {
        store_pair.key_->~ObIKVCacheKey();
      }
      if (NULL != store_pair.value_) {
        store_pair.value_->~ObIKVCacheValue();
      }
      store_pair.key_ = NULL;
      store_pair.value_ = NULL;
      /**
       * NOTE: even through copy key or value failed, we could assign
       * the size and magic field of KVPair instance correctly and
       * copy KVPair instance into memblock, because we could traverse
       * the memblock by KVPair instance, it stores the size of
       * current kv data, (sizeof(KVPair) + size(key) + size(value))
       */
      MEMCPY(&(buffer_[old_atomic_pos.buffer]), &store_pair, sizeof(ObKVCachePair));
    }
  }

  return ret;
}


int ObKVStoreMemBlock::alloc(
    const int64_t key_size,
    const int64_t value_size,
    const int64_t align_kv_size,
    ObKVCachePair *&kvpair)
{
  int ret = OB_SUCCESS;
  AtomicInt64 old_atomic_pos = { 0 };
  AtomicInt64 new_atomic_pos = { 0 };
  kvpair = NULL;

  //check parameters
  if (OB_UNLIKELY(NULL == buffer_)) {
    ret = OB_INVALID_DATA;
    COMMON_LOG(WARN, "The mem block is invalid, ", KP_(buffer), K(ret));
  } else if (OB_UNLIKELY(key_size <= 0 || value_size <= 0 || align_kv_size < 0 || align_kv_size > INT32_MAX)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid args", K(ret), K(key_size), K(value_size), K(align_kv_size));
  }

  //find write position
  while (OB_SUCC(ret)) {
    old_atomic_pos.atomic = atomic_pos_.atomic;
    new_atomic_pos.atomic = old_atomic_pos.atomic;
    if (old_atomic_pos.buffer + align_kv_size > payload_size_) {
      ret = OB_BUF_NOT_ENOUGH;
      break;
    } else {
      new_atomic_pos.buffer += static_cast<int32_t>(align_kv_size);
      new_atomic_pos.pairs += 1;
      if (ATOMIC_BCAS(&(atomic_pos_.atomic), old_atomic_pos.atomic, new_atomic_pos.atomic)) {
        break;
      }
    }
  }

  //if has found store pos, then store the kv
  if (OB_SUCC(ret)) {
    kvpair = reinterpret_cast<ObKVCachePair *>(&(buffer_[old_atomic_pos.buffer]));
    kvpair->size_ = static_cast<int32_t>(align_kv_size);
    kvpair->key_ = reinterpret_cast<ObIKVCacheKey *>(&(buffer_[old_atomic_pos.buffer + sizeof(ObKVCachePair)]));
    kvpair->value_ = reinterpret_cast<ObIKVCacheValue *>(&(buffer_[old_atomic_pos.buffer
        + sizeof(ObKVCachePair) + key_size]));
  }

  return ret;
}

/*
 * -----------------------------------------------------------ObKVMemBlockHandle------------------------------------------------
 */
ObKVMemBlockHandle::ObKVMemBlockHandle()
    : mem_block_(NULL),
      status_(FREE),
      inst_(NULL),
      policy_(LRU),
      get_cnt_(0),
      recent_get_cnt_(0),
      score_(0),
      kv_cnt_(0),
      working_set_(NULL)
{
}

ObKVMemBlockHandle::~ObKVMemBlockHandle()
{
}

void ObKVMemBlockHandle::reset()
{
  status_ = FREE;
  inst_ = NULL;
  policy_ = LRU;
  get_cnt_ = 0;
  recent_get_cnt_ = 0;
  score_ = 0;
  kv_cnt_ = 0;
  handle_ref_.reset();
  prev_ = NULL;
  next_ = NULL;
  retire_link_.reset();
  working_set_ = NULL;
}

int ObKVMemBlockHandle::store(const ObIKVCacheKey &key, const ObIKVCacheValue &value,
                              ObKVCachePair *&pair)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(mem_block_->store(key, value, pair))) {
    if (OB_BUF_NOT_ENOUGH != ret) {
      COMMON_LOG(WARN, "store failed", K(ret));
    }
  }
  return ret;
}

int ObKVMemBlockHandle::alloc(const int64_t key_size, const int64_t value_size,
    const int64_t align_kv_size, ObKVCachePair *&pair)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(mem_block_->alloc(key_size, value_size, align_kv_size, pair))) {
    if (OB_UNLIKELY(OB_BUF_NOT_ENOUGH != ret)) {
      COMMON_LOG(WARN, "alloc failed", K(ret));
    }
  }
  return ret;
}

void ObKVMemBlockHandle::set_full(const double base_mb_score)
{
  score_ += base_mb_score;
  ATOMIC_STORE((uint32_t*)(&status_), FULL);
}


/*
 * -------------------------------------------------ObKVCacheStoreMemblockInfo------------------------------------------------
 */
bool ObKVCacheStoreMemblockInfo::is_valid() const
{
  return tenant_id_ != OB_INVALID_TENANT_ID && cache_id_ >= 0;
}


}//end namespace common
}//end namespace oceanbase

