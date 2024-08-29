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

#ifndef OCEANBASE_MEMTABLE_OB_ROW_HASH_HOLDER_CACHE_IPP_
#define OCEANBASE_MEMTABLE_OB_ROW_HASH_HOLDER_CACHE_IPP_

#include "lib/atomic/ob_atomic.h"
#include "lib/lock/ob_small_spin_lock.h"
#include "lib/ob_errno.h"
#include "storage/memtable/hash_holder/ob_row_hash_holder_info.h"
#ifndef OCEANBASE_MEMTABLE_OB_ROW_HASH_HOLDER_CACHE_H_IPP_
#define OCEANBASE_MEMTABLE_OB_ROW_HASH_HOLDER_CACHE_H_IPP_
#include "ob_row_hash_holder_cache.h"
#endif

namespace oceanbase
{
namespace memtable
{
template <typename T>
RowHolderObjectCache<T>::RowHolderObjectCache(T *pool, int64_t pool_size)
: pre_alloc_obj_pool_(pool),
pool_size_(pool_size),
next_cached_obj_(pool),
fetch_times_(0),
revert_times_(0) {
  for (int i = 0; i < pool_size_; ++i) {
    new (&pre_alloc_obj_pool_[i]) T();
    get_linked_pointer(pre_alloc_obj_pool_[i]) = (i + 1 == pool_size_ ? nullptr : &pre_alloc_obj_pool_[i + 1]);
  }
}

template <typename T>
RowHolderObjectCache<T>::~RowHolderObjectCache() {
  if (OB_NOT_NULL(pre_alloc_obj_pool_)) {
    for (int i = 0; i < pool_size_; ++i) {
      pre_alloc_obj_pool_[i].~T();
    }
    new (this) RowHolderObjectCache<T>(nullptr, 0);
  }
}

template <typename T>
T *RowHolderObjectCache<T>::fetch_cache_object() {
  T *head = nullptr;
  ObByteLockGuard lg(lock_);
  head = next_cached_obj_;
  if (OB_NOT_NULL(head)) {
    next_cached_obj_ = get_linked_pointer(*head);
    get_linked_pointer(*head) = nullptr;
  }
#ifdef __HASH_HOLDER_ENABLE_MONITOR__
  if (OB_NOT_NULL(head)) {
    ATOMIC_AAF(&fetch_times_, 1);
  }
#endif
  return head;
}

template <typename T>
void RowHolderObjectCache<T>::revert_cache_object(T *obj) {
  OB_ASSERT(obj >= &pre_alloc_obj_pool_[0] && obj < &pre_alloc_obj_pool_[pool_size_]);
  OB_ASSERT((int64_t((char *)obj - (char *)&pre_alloc_obj_pool_[0]) % sizeof(T)) == 0);
  obj->~T();
  T *head = nullptr;
  ObByteLockGuard lg(lock_);
  head = next_cached_obj_;
  get_linked_pointer(*obj) = head;
  next_cached_obj_ = obj;
#ifdef __HASH_HOLDER_ENABLE_MONITOR__
  ATOMIC_AAF(&revert_times_, 1);
#endif
}

template <typename T>
RowHolderObjectCacheMultiThreadDispatcher<T>::~RowHolderObjectCacheMultiThreadDispatcher() {
  if (OB_NOT_NULL(thread_cache_)) {
    for (int i = 0; i < thread_size_; ++i) {
      thread_cache_[i].~RowHolderObjectCache<T>();
    }
    HashHolderAllocator::free(thread_cache_);
    HashHolderAllocator::free(pool_begin_addr_);
    new (this) RowHolderObjectCacheMultiThreadDispatcher();
  }
}

template <typename T>
int RowHolderObjectCacheMultiThreadDispatcher<T>::init(int64_t cache_size_each_thread,
                                                       int64_t thread_cache_number) {
  int ret = OB_SUCCESS;
  // contiguous memory allocation
  // so that destroy object across threads will just according to object's allocated address
  int64_t thread_cache_array_structure_size = sizeof(RowHolderObjectCache<T>) * thread_cache_number;
  alloc_size_for_pool_in_one_cache_ = ((sizeof(T) * cache_size_each_thread) / BASE_CACHE_SIZE + 1) * BASE_CACHE_SIZE;
  int64_t total_alloc_size = (ALLOC_SIZE_FOR_ONE_OBJ_CACHE + alloc_size_for_pool_in_one_cache_) * thread_cache_number;

  if (OB_NOT_NULL(thread_cache_)) {
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(thread_cache_ = (RowHolderObjectCache<T> *)HashHolderAllocator::alloc(thread_cache_array_structure_size, "RowHashCacheT"))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_ISNULL(pool_begin_addr_ = (char *)HashHolderAllocator::alloc(total_alloc_size, "RowHashCacheT"))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    HashHolderAllocator::free(thread_cache_);
    thread_cache_ = nullptr;
  } else {
    thread_size_ = thread_cache_number;
    cache_size_each_thread_ = cache_size_each_thread;
    for (int64_t idx = 0; idx < thread_cache_number; ++idx) {
      T *pool =(T *)(pool_begin_addr_ + alloc_size_for_pool_in_one_cache_ * idx);
      new (&thread_cache_[idx]) RowHolderObjectCache<T>(pool, cache_size_each_thread_);
    }
  }
  return ret;
}

template <typename T>
template <typename ...Args>
T *RowHolderObjectCacheMultiThreadDispatcher<T>::fetch_cache_object_and_construct(uint64_t hash_val, Args &&...args) {
  T *obj = nullptr;
  int64_t thread_idx_mask = thread_size_ - 1;
  int64_t idx = ((hash_val >> 1) & thread_idx_mask);
  obj = thread_cache_[idx].fetch_cache_object();
  if (OB_NOT_NULL(obj)) {
    new (obj) T(args...);
  }
  return obj;
}

template <typename T>
void RowHolderObjectCacheMultiThreadDispatcher<T>::revert_cache_object(T *node, int64_t cache_idx) {
  return thread_cache_[cache_idx].revert_cache_object(node);
}

template <typename T>
int64_t HashHolderFactory::get_object_cached_thread_idx(T *obj) {
  int64_t idx = -1;
  typename CacheTypeRouter<T>::type &cache = CacheTypeRouter<T>::get_cache(*this);
  char *obj_addr = (char *)obj;
  if (OB_LIKELY(obj_addr >= cache.pool_begin_addr_ && obj_addr < cache.pool_begin_addr_ + cache.alloc_size_for_pool_in_one_cache_ * cache.thread_size_)) {
    idx = ((char *)obj_addr - cache.pool_begin_addr_) / cache.alloc_size_for_pool_in_one_cache_;
  }
  return idx;
}

template <typename T, typename ...Args>
int HashHolderFactory::create(T *&obj, uint64_t hash, Args &&...args) {
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(obj = CacheTypeRouter<T>::get_cache(*this).fetch_cache_object_and_construct(hash, std::forward<Args>(args)...))) {
    DETECT_LOG(DEBUG, "after create obj from cache", KP(obj));
#ifdef UNITTEST_DEBUG
  } else if (DISABLE_DYNAMIC_ALLOC) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
#endif
  } else if (OB_ISNULL(obj = (T *)HashHolderAllocator::alloc(sizeof(T), "HHObject"))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    DETECT_LOG(DEBUG, "after create obj from heap", KP(obj));
    new (obj) T(args...);
#ifdef __HASH_HOLDER_ENABLE_MONITOR__
    if (std::is_same<T, RowHolderNode>::value) {
      monitor_.add_history_dynamic_alloc_node_cnt(hash);
    } else if (std::is_same<T, RowHolderList>::value) {
      monitor_.add_history_dynamic_alloc_list_cnt(hash);
    }
#endif
  }
  if (OB_SUCC(ret)) {
#ifdef __HASH_HOLDER_ENABLE_MONITOR__
    if (std::is_same<T, RowHolderNode>::value) {
      monitor_.inc_valid_node_cnt(hash);
    } else if (std::is_same<T, RowHolderList>::value) {
      monitor_.inc_valid_list_cnt(hash);
    }
#endif
  }
  return ret;
}

template <typename T>
void HashHolderFactory::destroy(T *obj, uint64_t hash) {
  int64_t cache_idx = -1;
  if (OB_LIKELY(-1 != (cache_idx = get_object_cached_thread_idx(obj)))) {
    DETECT_LOG(DEBUG, "before destroy obj from cache", KP(obj));
    CacheTypeRouter<T>::get_cache(*this).revert_cache_object(obj, cache_idx);
  } else {
    DETECT_LOG(DEBUG, "before destroy obj from heap", KP(obj));
    obj->~T();
    HashHolderAllocator::free(obj);
#ifdef __HASH_HOLDER_ENABLE_MONITOR__
    if (std::is_same<T, RowHolderNode>::value) {
      monitor_.add_history_dynamic_free_node_cnt(hash);
    } else if (std::is_same<T, RowHolderList>::value) {
      monitor_.add_history_dynamic_free_list_cnt(hash);
    }
#endif
  }
#ifdef __HASH_HOLDER_ENABLE_MONITOR__
  if (std::is_same<T, RowHolderNode>::value) {
    monitor_.dec_valid_node_cnt(hash);
  } else if (std::is_same<T, RowHolderList>::value) {
    monitor_.dec_valid_list_cnt(hash);
  }
#endif
}

}
}
#endif