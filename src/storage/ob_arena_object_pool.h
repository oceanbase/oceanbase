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

#ifndef OCEANBASE_TRANSACTION_OB_ARENA_OBJ_POOL_
#define OCEANBASE_TRANSACTION_OB_ARENA_OBJ_POOL_

#include <stdint.h>
#include "lib/allocator/ob_allocator.h"
#include "lib/lock/ob_spin_rwlock.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace transaction
{

// #define OB_ENABLE_MEMTABLE_CTX_OBJ_CACHE_DEBUG

template <typename T, int64_t OBJ_NUM>
class ObArenaObjPool
{
public:
  ObArenaObjPool(common::ObIAllocator &allocator)
   : allocator_(allocator),
     alloc_count_(0),
     free_count_(0) {}
  ObArenaObjPool() = delete;
  void *alloc();
  void free(void *obj);
  void reset();
  TO_STRING_KV(K_(alloc_count), K_(free_count));
private:
  common::ObIAllocator &allocator_;
  int64_t alloc_count_;
  int64_t free_count_;
  // only used one time.
  char obj_pool_[sizeof(T) * OBJ_NUM];

#ifdef OB_ENABLE_MEMTABLE_CTX_OBJ_CACHE_DEBUG
public:
  void *alloc(bool &hit_cache);
#endif
};

template <typename T, int64_t OBJ_NUM>
void *ObArenaObjPool<T, OBJ_NUM>::alloc()
{
  void *ptr = nullptr;
  int64_t allocated_count = ATOMIC_FAA(&alloc_count_, 1);
  if (allocated_count < OBJ_NUM) {
    int64_t pos = sizeof(T) * allocated_count;
    ptr = &obj_pool_[pos];
  } else {
    const int64_t size = sizeof(T);
    ptr = allocator_.alloc(size);
  }

  if (OB_ISNULL(ptr)) {
    ATOMIC_DEC(&alloc_count_);
    STORAGE_LOG_RET(ERROR, common::OB_ALLOCATE_MEMORY_FAILED, "obj alloc error, no memory", K(*this), K(lbt()));
  } else {
    STORAGE_LOG(DEBUG, "obj alloc succ", K(*this), KP(ptr), K(lbt()));
  }

  return ptr;
}

template <typename T, int64_t OBJ_NUM>
void ObArenaObjPool<T, OBJ_NUM>::free(void *obj)
{
  bool need_free = true;
  if (OB_ISNULL(obj)) {
    STORAGE_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "obj is null, unexpected error", KP(obj), K(*this), K(lbt()));
  } else {
    STORAGE_LOG(DEBUG, "object free succ", KP(obj), K(*this), K(lbt()));
    ATOMIC_INC(&free_count_);
    for (int i = 0; need_free && i < OBJ_NUM; i++) {
      int64_t pos = sizeof(T) * i;
      if (obj == &obj_pool_[pos]) {
        // no need free.
        need_free = false;
      }
    }
    if (need_free) {
      allocator_.free(obj);
    }
  }
}

template <typename T, int64_t OBJ_NUM>
void ObArenaObjPool<T, OBJ_NUM>::reset()
{
  if (OB_UNLIKELY(alloc_count_ != free_count_)) {
    TABLELOCK_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "object alloc and free count not match", K(*this));
  }

  alloc_count_ = 0;
  free_count_ = 0;
}

#ifdef OB_ENABLE_MEMTABLE_CTX_OBJ_CACHE_DEBUG
template <typename T, int64_t OBJ_NUM>
void *ObArenaObjPool<T, OBJ_NUM>::alloc(bool &hit_cache)
{
  void *ptr = nullptr;
  int64_t allocated_count = ATOMIC_FAA(&alloc_count_, 1);
  if (allocated_count < OBJ_NUM) {
    int64_t pos = sizeof(T) * allocated_count;
    ptr = &obj_pool_[pos];
    hit_cache = true;
  } else {
    const int64_t size = sizeof(T);
    ptr = allocator_.alloc(size);
    hit_cache = false;
  }

  if (OB_ISNULL(ptr)) {
    ATOMIC_DEC(&alloc_count_);
    STORAGE_LOG_RET(ERROR, common::OB_ALLOCATE_MEMORY_FAILED, "obj alloc error, no memory", K(*this), K(lbt()));
  } else {
    STORAGE_LOG(DEBUG, "obj alloc succ", K(*this), KP(ptr), K(lbt()));
  }

  return ptr;
}
#endif

}  // namespace transaction
}  // namespace oceanbase

#endif
