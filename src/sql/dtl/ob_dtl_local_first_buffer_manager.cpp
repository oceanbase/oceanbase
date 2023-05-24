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

#define USING_LOG_PREFIX SQL_DTL

#include "ob_dtl_local_first_buffer_manager.h"
#include "share/ob_errno.h"
#include "sql/dtl/ob_dtl_rpc_channel.h"
#include "share/rc/ob_tenant_base.h"
#include "share/rc/ob_context.h"
#include "sql/dtl/ob_dtl.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::sql::dtl;
using namespace oceanbase::lib;
using namespace oceanbase::share;

class PinFunction
{
public:
  void operator()(ObDtlLocalFirstBufferCache *val)
  {
    val->pin();
  }
};

int ObDtlLocalFirstBufferCache::ObDtlBufferClean::operator()(sql::dtl::ObDtlCacheBufferInfo *buffer_info)
{
  int ret = OB_SUCCESS;
  ObDtlLinkedBuffer *linked_buffer = buffer_info->buffer();
  buffer_info->set_buffer(nullptr);
  if (nullptr != linked_buffer) {
    tenant_mem_mgr_->free(linked_buffer);
  }
  buffer_info->reset();
  buffer_info_mgr_->free_buffer_info(buffer_info);
  return ret;
}

//请保证bucket_num, conrrent_cnt是2的整数倍
int ObDtlLocalFirstBufferCache::init(int64_t bucket_num, int64_t concurrent_cnt)
{
  int ret = OB_SUCCESS;
  bucket_num = next_pow2(bucket_num * 2);
  concurrent_cnt = next_pow2(concurrent_cnt * 2);
  bitset_cnt_ = bucket_num;
  if (0 >= concurrent_cnt || CONCURRENT_CNT < concurrent_cnt) {
    concurrent_cnt = CONCURRENT_CNT;
    LOG_DEBUG("unexepct concurrent_cnt", K(bucket_num), K(concurrent_cnt));
  }
  if (0 >= bucket_num || CHANNEL_HASH_BUCKET_NUM < bucket_num) {
    bucket_num = CHANNEL_HASH_BUCKET_NUM;
    LOG_DEBUG("unexepct  bucket number", K(bucket_num), K(concurrent_cnt));
  }
  if (0 >= bitset_cnt_ || MAX_BITSET_CNT < bitset_cnt_) {
    bitset_cnt_ = MAX_BITSET_CNT;
    LOG_DEBUG("unexepct bucket number", K(bucket_num), K(concurrent_cnt));
  }
  if (OB_FAIL(buffer_map_.init(bucket_num, concurrent_cnt))) {
    LOG_WARN("failed to create hash map", K(ret));
  } else if (OB_FAIL(filter_bitset_.prepare_allocate(bitset_cnt_))) {
    LOG_WARN("failed to reserve bitset", K(ret), K(bitset_cnt_));
  } else if (0 >= bitset_cnt_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("bitset cnt is invalid", K(ret), K(bitset_cnt_));
  } else {
    LOG_DEBUG("trace filter bitset", K(bitset_cnt_), K(filter_bitset_.bit_count()),
      K(filter_bitset_.bitset_word_count()), KP(this));
  }
  return ret;
}

void ObDtlLocalFirstBufferCache::destroy()
{
  registered_ = false;
  buffer_map_.reset();
  buffer_map_.destroy();
  filter_bitset_.reset();
  tenant_mem_mgr_ = nullptr;
  buffer_info_mgr_ = nullptr;
}

int ObDtlLocalFirstBufferCache::clean()
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("trace local first buffer cache unpin", K(dfo_key_), K(get_pins()));
  unpin();
  // spin until there's no reference of this channel.
  while (get_pins() > 0) {
  }
  if (get_pins() < 0) {
    LOG_ERROR("unexpect status: pins is less than 0", K(get_pins()));
  }
  ObDtlBufferClean op(tenant_mem_mgr_, buffer_info_mgr_);
  if (OB_FAIL(buffer_map_.foreach_refactored(op))) {
    LOG_WARN("failed to refactor all buffer", K(ret));
  }
  buffer_map_.reset();
  return ret;
}

int ObDtlLocalFirstBufferCache::cache_buffer(ObDtlCacheBufferInfo *&buffer)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(buffer)) {
    uint64_t chan_id = buffer->chid();
#ifdef ERRSIM
    // -17 enable failed set refactored
    ret = OB_E(EventTable::EN_DTL_ONE_ROW_ONE_BUFFER) ret;
#endif
    int64_t tmp_ret = ret;
    if (TP_ENABLE_FAILED_SET_HT == ret) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to set refactor", K(ret), K(tmp_ret));
    } else if (OB_FAIL(buffer_map_.set_refactored(chan_id, buffer))) {
      LOG_WARN("failed to insert buffer map", K(ret));
    } else {
      buffer = nullptr;
      if (TP_ENABLE_FAILED_SET_FIRST_BUFFER == tmp_ret) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to set first buffer", K(ret), K(tmp_ret));
      } else if (OB_FAIL(set_first_buffer(chan_id))) {
        LOG_WARN("failed to set first buffer", K(ret), K(chan_id), KP(chan_id));
      } else {
        inc_first_buffer_cnt();
        LOG_DEBUG("trace cache buffer", KP(chan_id), KP(this), K(dfo_key_));
      }
    }
  }
  return ret;
}

int ObDtlLocalFirstBufferCache::get_buffer(int64_t chid, ObDtlCacheBufferInfo *&buffer)
{
  int ret = OB_SUCCESS;
  buffer = nullptr;
  if (OB_FAIL(buffer_map_.erase_refactored(chid, buffer))) {
    //LOG_WARN("failed to insert buffer map", K(ret));
  } else if (OB_NOT_NULL(buffer)) {
    dec_first_buffer_cnt();
    LOG_DEBUG("trace get cached buffer", KP(buffer->chid()));
  }
  return ret;
}

void ObDtlLocalFirstBufferCache::release()
{
  // LOG_DEBUG("trace local first buffer cache unpin", K(dfo_key_), K(get_pins()));
  unpin();
}
////////////////////////////////////////////////////////////////////////////////////////////////////////////////

int ObDtlFirstBufferConcurrentCell::init(int32_t n_lock)
{
  int ret = OB_SUCCESS;
  n_lock_ = n_lock;
  if (0 >= n_lock) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status", K(n_lock));
  } else {
    char *buf = reinterpret_cast<char*>(allocator_.alloc(sizeof(ObLatch) * n_lock));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(n_lock), K(buf));
    } else {
      lock_ = reinterpret_cast<ObLatch*>(buf);
      for (int64_t i = 0; i < n_lock; ++i) {
        ObLatch *lock = new (buf) ObLatch;
        UNUSED(lock);
        buf += sizeof(ObLatch);
      }
    }
  }
  return ret;
}

void ObDtlFirstBufferConcurrentCell::reset()
{
  if (OB_NOT_NULL(lock_)) {
    allocator_.free(lock_);
    lock_ = nullptr;
  }
}

ObLatch &ObDtlFirstBufferConcurrentCell::get_lock(int32_t idx)
{
  return lock_[idx];
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////

int ObDtlBufferInfoManager::ObDtlBufferInfoAllocator::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(allocator_.init(
                lib::ObMallocAllocator::get_instance(),
                OB_MALLOC_NORMAL_BLOCK_SIZE,
                ObMemAttr(common::OB_SERVER_TENANT_ID, "DTLBufAlloc")))) {
    LOG_WARN("failed to init alocator", K(ret));
  } else {
    free_list_.reset();
  }
  return ret;
}

void ObDtlBufferInfoManager::ObDtlBufferInfoAllocator::destroy()
{
  ObDtlCacheBufferInfo *buffer_info = NULL;
  DLIST_FOREACH_REMOVESAFE_NORET(node, free_list_) {
    buffer_info = node;
    if (nullptr != buffer_info->buffer()) {
      LOG_ERROR_RET(OB_ERR_UNEXPECTED, "data buffer is not null", K(buffer_info));
      buffer_info->set_buffer(nullptr);
    }
    free_list_.remove(buffer_info);
    buffer_info->~ObDtlCacheBufferInfo();
    allocator_.free(buffer_info);
  }
}

int ObDtlBufferInfoManager::ObDtlBufferInfoAllocator::alloc_buffer_info(int64_t chid, ObDtlCacheBufferInfo *&buffer_info)
{
  int ret = OB_SUCCESS;
  buffer_info = nullptr;
  ObDtlCacheBufferInfo *res = NULL;
  {
    ObLockGuard<ObSpinLock> lock_guard(spin_lock_);
    res = free_list_.remove_last();
  }
  if (nullptr != res) {
    if (OB_NOT_NULL(res->buffer())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("linked buffer is not null", K(ret));
    } else {
      res->reset();
    }
  }
  if (OB_SUCC(ret) && nullptr == res) {
    void *buf = allocator_.alloc(sizeof(ObDtlCacheBufferInfo));
    if (OB_ISNULL(buf)) {
      ret = OB_MALLOC_NORMAL_BLOCK_SIZE;
      LOG_WARN("alloc memory failed", K(ret));
    } else {
      res = new (buf) ObDtlCacheBufferInfo();
      res->reset();
    }
  }
  if (OB_SUCC(ret) && nullptr != res) {
    // 由于会重用，本质上chid应该不可以改变，但即使改变，chid对应的hashtable也是对应的相同的hash cell
    res->chid() = chid;
    buffer_info = res;
  }
  return ret;
}

int ObDtlBufferInfoManager::ObDtlBufferInfoAllocator::free_buffer_info(ObDtlCacheBufferInfo *buffer_info)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(buffer_info)) {
    if (OB_NOT_NULL(buffer_info->buffer())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected status: link buffer is not null", K(ret), K(buffer_info->chid()));
      buffer_info->set_buffer(nullptr);
    }
    if (MAX_FREE_LIST_SIZE > free_list_.get_size()) {
      ObLockGuard<ObSpinLock> lock_guard(spin_lock_);
      free_list_.add_last(buffer_info);
      buffer_info = nullptr;
    }
    if (OB_NOT_NULL(buffer_info)) {
      buffer_info->~ObDtlCacheBufferInfo();
      allocator_.free(buffer_info);
    }
  }
  return ret;
}

int ObDtlBufferInfoManager::init()
{
  int ret = OB_SUCCESS;
  char *buf = reinterpret_cast<char*>(allocator_.alloc(ALLOCATOR_CNT * sizeof(ObDtlBufferInfoAllocator)));
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret));
  } else {
    conrrent_allocators_ = reinterpret_cast<ObDtlBufferInfoAllocator*>(buf);
    for (int64_t i = 0; i < ALLOCATOR_CNT && OB_SUCC(ret); ++i) {
      ObDtlBufferInfoAllocator *alloc = new (buf) ObDtlBufferInfoAllocator;
      if (OB_FAIL(alloc->init())) {
        LOG_WARN("failed to init allocator");
      }
      buf += sizeof(ObDtlBufferInfoAllocator);
    }
  }
  if (OB_FAIL(ret) && nullptr != buf) {
    allocator_.free(buf);
  }
  return ret;
}

void ObDtlBufferInfoManager::destroy()
{
  if (OB_NOT_NULL(conrrent_allocators_)) {
    for (int64_t i = 0; i < ALLOCATOR_CNT; ++i) {
      ObDtlBufferInfoAllocator &alloc = conrrent_allocators_[i];
      alloc.destroy();
    }
    allocator_.free(conrrent_allocators_);
    conrrent_allocators_ = nullptr;
  }
}

int ObDtlBufferInfoManager::alloc_buffer_info(int64_t chid, ObDtlCacheBufferInfo *&buffer_info)
{
  int ret = OB_SUCCESS;
  buffer_info = nullptr;
  int64_t hash_val = get_hash_value(chid);
  if (OB_FAIL(conrrent_allocators_[hash_val].alloc_buffer_info(chid, buffer_info))) {
    LOG_WARN("failed to alloc dtl buffer info");
  }
  return ret;
}

int ObDtlBufferInfoManager::free_buffer_info(ObDtlCacheBufferInfo *buffer_info)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(buffer_info)) {
    int64_t hash_val = get_hash_value(buffer_info->chid());
    if (OB_FAIL(conrrent_allocators_[hash_val].free_buffer_info(buffer_info))) {
      LOG_WARN("failed to alloc dtl buffer info");
    }
  }
  return ret;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////

ObDtlLocalFirstBufferCacheManager::ObDtlLocalFirstBufferCacheManager(uint64_t tenant_id, ObDtlTenantMemManager *tenant_mem_mgr) :
  tenant_id_(tenant_id), allocator_(tenant_id), tenant_mem_mgr_(tenant_mem_mgr),
  buffer_info_mgr_(allocator_), hash_table_(allocator_)
{}

ObDtlLocalFirstBufferCacheManager::~ObDtlLocalFirstBufferCacheManager()
{
  destroy();
}

int ObDtlLocalFirstBufferCacheManager::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(allocator_.init(
                lib::ObMallocAllocator::get_instance(),
                OB_MALLOC_NORMAL_BLOCK_SIZE,
                ObMemAttr(tenant_id_, "SqlDtl1stBuf")))) {
    LOG_WARN("failed to init allocator", K(ret));
  } else {
    if (OB_FAIL(hash_table_.init(BUCKET_NUM, CONCURRENT_CNT))) {
      LOG_WARN("failed to init hash table", K(ret));
    } else if (OB_FAIL(buffer_info_mgr_.init())) {
      LOG_WARN("failed to init buffer info manager", K(ret));
    }
  }
  return ret;
}

void ObDtlLocalFirstBufferCacheManager::destroy()
{
  hash_table_.reset();
  buffer_info_mgr_.destroy();
  allocator_.reset();
  tenant_mem_mgr_ = nullptr;
}

int ObDtlLocalFirstBufferCacheManager::get_buffer_cache(ObDtlDfoKey &key, ObDtlLocalFirstBufferCache *&buf_cache)
{
  int ret = OB_SUCCESS;
  buf_cache = nullptr;
  PinFunction pin;
  if (OB_FAIL(hash_table_.get_refactored(key, buf_cache, pin))) {
    // LOG_DEBUG("failed to get buffer cache");
  }
  return ret;
}

int ObDtlLocalFirstBufferCacheManager::erase_buffer_cache(ObDtlDfoKey &key, ObDtlLocalFirstBufferCache *&buf_cache)
{
  int ret = OB_SUCCESS;
  buf_cache = nullptr;
  if (OB_FAIL(hash_table_.erase_refactored(key, buf_cache))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

// firstly, find the buffer cache, if no buffer cache, then don't cache
// secondly, cache first buffer
int ObDtlLocalFirstBufferCacheManager::cache_buffer(int64_t chid, ObDtlLinkedBuffer *&data_buffer, bool attach)
{
  int ret = OB_SUCCESS;
  ObDtlLocalFirstBufferCache *buf_cache = nullptr;
  ObDtlDfoKey &key = data_buffer->get_dfo_key();
  if (OB_FAIL(get_buffer_cache(key, buf_cache))) {
    LOG_DEBUG("failed to get buffer cache", K(key), KP(chid));
  } else if (OB_ISNULL(buf_cache)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("buffer cache is null", KP(chid), K(attach));
  } else {
    ObDtlCacheBufferInfo *buf_info = nullptr;
    if (OB_FAIL(buffer_info_mgr_.alloc_buffer_info(chid, buf_info))) {
      LOG_WARN("failed to alloc buffer info", KP(chid), K(key));
    } else if (nullptr != buf_info) {
      if (attach) {
        buf_info->set_buffer(data_buffer);
        data_buffer = nullptr;
      } else {
        ObDtlLinkedBuffer *buffer = nullptr;
    #ifdef ERRSIM
        // -16 enable failed to alloc memory
        ret = OB_E(EventTable::EN_DTL_ONE_ROW_ONE_BUFFER) ret;
    #endif
        if (OB_SUCC(ret) || TP_ENABLE_FAILED_ALLOC_MEM != ret) {
          ret = OB_SUCCESS;
          buffer = tenant_mem_mgr_->alloc(chid, data_buffer->size());
        } else {
          ret = OB_SUCCESS;
          buffer = nullptr;
        }
        if (nullptr != buffer) {
          ObDtlLinkedBuffer::assign(*data_buffer, buffer);
          buf_info->set_buffer(buffer);
        } else {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to alloc buffer", K(ret));
        }
      }
    } else {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc buffer info", K(ret));
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(buf_cache->cache_buffer(buf_info))) {
        LOG_WARN("failed to cache buffer", KP(chid), K(ret));
      }
    }
    if (OB_FAIL(ret)) {
      int tmp_ret = OB_SUCCESS;
      if (nullptr != buf_info) {
        ObDtlLinkedBuffer *buffer = buf_info->buffer();
        buf_info->set_buffer(nullptr);
        if (nullptr != buffer) {
          tenant_mem_mgr_->free(buffer);
        }
        if (OB_SUCCESS != (tmp_ret = buffer_info_mgr_.free_buffer_info(buf_info))) {
          LOG_WARN("failed to free buffer info", K(ret), K(tmp_ret));
        }
      }
    }
    buf_cache->release();
  }
  return ret;
}

int ObDtlLocalFirstBufferCacheManager::get_cached_buffer(
  ObDtlLocalFirstBufferCache *buf_cache, int64_t chid, ObDtlLinkedBuffer *&linked_buffer)
{
  int ret = OB_SUCCESS;
  linked_buffer = nullptr;
  if (OB_NOT_NULL(buf_cache)) {
    ObDtlCacheBufferInfo *buffer_info = nullptr;
    if (OB_FAIL(buf_cache->get_buffer(chid, buffer_info))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to get buffer", KP(chid));
      }
    } else if (nullptr == buffer_info) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("buffer into is null", KP(chid), K(buffer_info));
    } else {
      linked_buffer = buffer_info->buffer();
      buffer_info->set_buffer(nullptr);
      buffer_info_mgr_.free_buffer_info(buffer_info);
      LOG_DEBUG("trace get cached buffer", KP(chid), K(linked_buffer));
    }
  }
  return ret;
}

int ObDtlLocalFirstBufferCacheManager::register_first_buffer_cache(ObDtlLocalFirstBufferCache *buf_cache)
{
  int ret = OB_SUCCESS;
  buf_cache->set_buffer_mgr(tenant_mem_mgr_, &buffer_info_mgr_);
  buf_cache->pin();
  if (OB_FAIL(hash_table_.set_refactored(buf_cache->get_first_buffer_key(), buf_cache))) {
    LOG_WARN("failed to insert hash table", K(buf_cache->get_first_buffer_key()));
  } else {
    buf_cache->set_registered();
  }
  return ret;
}

int ObDtlLocalFirstBufferCacheManager::unregister_first_buffer_cache(
  ObDtlDfoKey &key,
  ObDtlLocalFirstBufferCache *org_buf_cache)
{
  int ret = OB_SUCCESS;
  ObDtlLocalFirstBufferCache *buf_cache = nullptr;
  if (OB_ISNULL(org_buf_cache)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dtl buffer cache is null", K(ret));
  } else if (org_buf_cache->is_registered() && OB_FAIL(erase_buffer_cache(key, buf_cache))) {
    LOG_WARN("failed to get buffer cache", K(ret));
  } else {
    if (OB_ISNULL(buf_cache)) {
      LOG_DEBUG("buffer cache is null");
    } else if (org_buf_cache != buf_cache) {
      LOG_ERROR("dtl buffer cache is not match", K(buf_cache->get_first_buffer_key()),
        K(org_buf_cache->get_first_buffer_key()));
    } else {
      org_buf_cache->clean();
      LOG_DEBUG("trace destroy bufffer cache");
    }
    org_buf_cache->destroy();
  }
  return ret;
}
