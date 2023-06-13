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

#ifndef OCEANBAE_LIB_ALLOCATER_OB_CACHED_ALLOCATOR_H_
#define OCEANBAE_LIB_ALLOCATER_OB_CACHED_ALLOCATOR_H_

#include "lib/objectpool/ob_pool.h"
#include "lib/lock/ob_spin_lock.h"

namespace oceanbase
{
namespace common
{
// @note thread-safe
template <typename T>
class ObCachedAllocator
{
public:
  ObCachedAllocator();
  virtual ~ObCachedAllocator();

  T *alloc();
  void free(T *obj, bool can_reuse = true);
  int32_t get_allocated_count() const {return allocated_count_;};
  int32_t get_cached_count() const {return cached_count_;};
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObCachedAllocator);
private:
  // data members
  ObSpinLock lock_;
  ObPool<> pool_;
  ObArray<T *> cached_objs_;
  int32_t allocated_count_;
  int32_t cached_count_;
};

template<typename T>
ObCachedAllocator<T>::ObCachedAllocator()
    : lock_(ObLatchIds::OB_CACHED_ALLOCATOR_LOCK), pool_(sizeof(T)), allocated_count_(0), cached_count_(0)
{
}

template<typename T>
ObCachedAllocator<T>::~ObCachedAllocator()
{
  //_OB_LOG(DEBUG, "free cached objs, count=%ld", cached_objs_.count());
  T *obj = NULL;
  while (OB_SUCCESS == cached_objs_.pop_back(obj)) {
    obj->~T();
    pool_.free(obj);
    obj = NULL;
    --cached_count_;
  }
  if (0 != allocated_count_ || 0 != cached_count_) {
    LIB_LOG_RET(WARN, common::OB_ERR_UNEXPECTED, "some allocated object is not freed",
              K(allocated_count_), K(cached_count_));
  }
}

template<typename T>
T *ObCachedAllocator<T>::alloc()
{
  T *ret = NULL;
  ObSpinLockGuard guard(lock_);
  if (OB_SUCCESS != cached_objs_.pop_back(ret)) {
    void *p = pool_.alloc();
    if (OB_ISNULL(p)) {
      LIB_LOG_RET(ERROR, OB_ALLOCATE_MEMORY_FAILED, "no memory");
    } else {
      ret = new(p) T();
      ++ allocated_count_;
    }
  } else {
    -- cached_count_;
    ++ allocated_count_;
  }
  return ret;
}

template<typename T>
void ObCachedAllocator<T>::free(T *obj, bool can_reuse)
{
  if (OB_LIKELY(NULL != obj)) {
    int ret = OB_SUCCESS;
    ObSpinLockGuard guard(lock_);
    if (!can_reuse) {
      // free directly
      obj->~T();
      pool_.free(obj);
    } else if (OB_FAIL(cached_objs_.push_back(obj))) {
      LIB_LOG(ERROR, "failed to push obj into array", K(ret));
      // free directly
      obj->~T();
      pool_.free(obj);
    } else {
      obj->reset();
      ++ cached_count_;
    }
    -- allocated_count_;
  }
}

} // end namespace common
} // end namespace oceanbase

#endif //OCEANBAE_LIB_ALLOCATER_OB_CACHED_ALLOCATOR_H_
