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

#ifndef __OB_CONNECTION_ALLOCATOR_H__
#define __OB_CONNECTION_ALLOCATOR_H__ 1

#include "lib/objectpool/ob_pool.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/container/ob_array.h"

namespace oceanbase {
namespace common {
// @note thread-safe
template <typename T>
class ObConnectionAllocator {
public:
  ObConnectionAllocator();
  virtual ~ObConnectionAllocator();

  T* alloc();
  void free(T* obj);
  T* get_cached();
  int put_cached(T* obj);

private:
  // disallow copy
  ObConnectionAllocator(const ObConnectionAllocator& other);
  ObConnectionAllocator& operator=(const ObConnectionAllocator& other);

private:
  // data members
  ObSpinLock lock_;
  ObPool<> pool_;
  ObArray<T*> cached_objs_;
};

template <typename T>
ObConnectionAllocator<T>::ObConnectionAllocator() : pool_(sizeof(T))
{}

template <typename T>
ObConnectionAllocator<T>::~ObConnectionAllocator()
{
  _OB_LOG(DEBUG, "free cached objs, count=%ld", cached_objs_.count());
  T* obj = NULL;
  while (OB_SUCCESS == cached_objs_.pop_back(obj)) {
    obj->~T();
    pool_.free(obj);
    obj = NULL;
  }
}

template <typename T>
T* ObConnectionAllocator<T>::alloc()
{
  T* ret = NULL;
  ObSpinLockGuard guard(lock_);
  if (OB_SUCCESS != cached_objs_.pop_back(ret)) {
    void* p = pool_.alloc();
    if (NULL == p) {
      _OB_LOG(ERROR, "no memory");
    } else {
      ret = new (p) T();
    }
  }
  return ret;
}

template <typename T>
T* ObConnectionAllocator<T>::get_cached()
{
  T* ret = NULL;
  ObSpinLockGuard guard(lock_);
  if (OB_SUCCESS != cached_objs_.pop_back(ret)) {
    ret = NULL;
  }
  return ret;
}

template <typename T>
void ObConnectionAllocator<T>::free(T* obj)
{
  if (NULL != obj) {
    ObSpinLockGuard guard(lock_);
    // free directly
    obj->~T();
    pool_.free(obj);
  }
}

template <typename T>
int ObConnectionAllocator<T>::put_cached(T* obj)
{
  int err = OB_SUCCESS;
  if (NULL != obj) {
    ObSpinLockGuard guard(lock_);
    if (OB_SUCCESS != (err = cached_objs_.push_back(obj))) {
      _OB_LOG(ERROR, "failed to push obj into array, err=%d", err);
      // free directly
      obj->~T();
      pool_.free(obj);
      err = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      obj->reset();
    }
  }
  return err;
}

}  // end namespace common
}  // end namespace oceanbase

#endif /* __OB_CONNECTION_ALLOCATOR_H__ */
