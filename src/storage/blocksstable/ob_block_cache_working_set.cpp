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

#define USING_LOG_PREFIX STORAGE

#include "ob_block_cache_working_set.h"
#include "ob_storage_cache_suite.h"

namespace oceanbase
{
using namespace common;
namespace blocksstable
{
ObBlockCacheWorkingSet::ObBlockCacheWorkingSet()
  : inited_(false), tenant_id_(OB_INVALID_ID), block_cache_(NULL),
    put_size_(0), use_working_set_(false), working_set_(), lock_()
{
}

ObBlockCacheWorkingSet::~ObBlockCacheWorkingSet()
{
}

int ObBlockCacheWorkingSet::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    block_cache_ = &OB_STORE_CACHE.get_block_cache();
    put_size_ = 0;
    use_working_set_ = false;
    working_set_.reset();
    inited_ = true;
  }
  return ret;
}

void ObBlockCacheWorkingSet::reset()
{
  tenant_id_ = OB_INVALID_ID;
  block_cache_ = NULL;
  put_size_ = 0;
  use_working_set_ = false;
  working_set_.reset();
  inited_ = false;
}

int ObBlockCacheWorkingSet::add_put_size(const int64_t put_size)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (put_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(put_size));
  } else {
    ATOMIC_AAF(&put_size_, put_size);
  }
  return ret;
}

int ObBlockCacheWorkingSet::get_cache(BaseBlockCache *&cache)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(create_working_set_if_need())) {
    LOG_WARN("create_working_set_if_need failed", K(ret));
  } else if (!use_working_set_) {
    cache = block_cache_;
  } else {
    cache = &working_set_;
  }
  return ret;
}

int ObBlockCacheWorkingSet::get_allocator(ObIAllocator *&allocator)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(block_cache_->get_allocator(allocator))) {
    LOG_WARN("get_allocator failed", K(ret));
  }
  return ret;
}

int ObBlockCacheWorkingSet::put(const Key &key, const Value &value, bool overwrite)
{
  int ret = OB_SUCCESS;
  BaseBlockCache *cache = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(get_cache(cache))) {
    LOG_WARN("get_cache failed", K(ret));
  } else if (OB_FAIL(cache->put(key, value, overwrite))) {
    LOG_WARN("cache put failed", K(ret));
  } else {
    const int64_t put_size = ObKVStoreMemBlock::get_align_size(key, value);
    if (OB_FAIL(add_put_size(put_size))) {
      STORAGE_LOG(WARN, "add_put_size failed", K(ret), K(put_size));
    }
  }
  return ret;
}

int ObBlockCacheWorkingSet::put_and_fetch(const Key &key, const Value &value, const Value *&pvalue,
    ObKVCacheHandle &handle, bool overwrite)
{
  int ret = OB_SUCCESS;
  BaseBlockCache *cache = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(get_cache(cache))) {
    LOG_WARN("get_cache failed", K(ret));
  } else if (OB_FAIL(cache->put_and_fetch(key, value, pvalue, handle, overwrite))) {
    LOG_WARN("cache put failed", K(ret));
  } else {
    const int64_t put_size = ObKVStoreMemBlock::get_align_size(key, value);
    if (OB_FAIL(add_put_size(put_size))) {
      STORAGE_LOG(WARN, "add_put_size failed", K(ret), K(put_size));
    }
  }
  return ret;
}

int ObBlockCacheWorkingSet::get(const Key &key, const Value *&pvalue, ObKVCacheHandle &handle)
{
  int ret = OB_SUCCESS;
  BaseBlockCache *cache = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(get_cache(cache))) {
    LOG_WARN("get_cache failed", K(ret));
  } else if (OB_FAIL(cache->get(key, pvalue, handle))) {
    LOG_WARN("cache get failed", K(ret));
  }
  return ret;
}

int ObBlockCacheWorkingSet::erase(const Key &key)
{
  int ret = OB_SUCCESS;
  BaseBlockCache *cache = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(get_cache(cache))) {
    LOG_WARN("get_cache failed", K(ret));
  } else if (OB_FAIL(cache->erase(key))) {
    LOG_WARN("cache erase failed", K(ret));
  }
  return ret;
}

int ObBlockCacheWorkingSet::create_working_set_if_need()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!use_working_set_) {
    if (put_size_ > USE_WORKING_SET_THRESHOLD) {
      int64_t lock_ret = OB_SUCCESS;
      if (OB_SUCCESS == (lock_ret = (lock_.trylock()))) {
        if (!use_working_set_) {
          if (OB_FAIL(working_set_.init(tenant_id_, *block_cache_))) {
            LOG_WARN("init working_set failed", K(ret), K_(tenant_id));
          } else {
            use_working_set_ = true;
          }
        }
        lock_.unlock();
      } else if (OB_EAGAIN != lock_ret) {
        LOG_ERROR("trylock failed", K(lock_ret));
      }
    }
  }
  return ret;
}

int ObBlockCacheWorkingSet::alloc(const uint64_t tenant_id, const int64_t key_size, const int64_t value_size,
      ObKVCachePair *&kvpair, ObKVCacheHandle &handle, ObKVCacheInstHandle &inst_handle)
{
  int ret = OB_SUCCESS;
  BaseBlockCache *cache = nullptr;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(get_cache(cache))) {
    LOG_WARN("get_cache failed", K(ret));
  } else if (OB_FAIL(cache->alloc(tenant_id, key_size, value_size, kvpair, handle, inst_handle))) {
    LOG_WARN("cache put failed", K(ret));
  } else {
    const int64_t put_size = ObKVStoreMemBlock::get_align_size(key_size, value_size);
    if (OB_FAIL(add_put_size(put_size))) {
      STORAGE_LOG(WARN, "add_put_size failed", K(ret), K(put_size));
    }
  }
  return ret;
}
}//end namespace blocksstable
}//end namespace oceanbase