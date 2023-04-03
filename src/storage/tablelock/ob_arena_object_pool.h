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

#ifndef OCEANBASE_STORAGE_TABLELOCK_OB_STACK_OBJ_POOL_
#define OCEANBASE_STORAGE_TABLELOCK_OB_STACK_OBJ_POOL_

#include <stdint.h>
#include "lib/lock/ob_spin_lock.h"

namespace oceanbase
{
namespace transaction
{
namespace tablelock
{

template <typename T, int64_t OBJ_NUM = 1>
class ObArenaObjPool
{
public:
  ObArenaObjPool(common::ObIAllocator &allocator)
   : allocator_(allocator),
     alloc_count_(0),
     free_count_(0),
     pos_(0) {}
  ObArenaObjPool() = delete;
  void *alloc();
  void free(void *obj);
  void reset();
  TO_STRING_KV(K_(pos), K_(alloc_count), K_(free_count));
private:
  common::ObIAllocator &allocator_;
  int64_t alloc_count_;
  int64_t free_count_;
  // only used one time.
  T obj_pool_[OBJ_NUM];
  int64_t pos_;
};

template <typename T, int64_t OBJ_NUM>
void *ObArenaObjPool<T, OBJ_NUM>::alloc()
{
  void *ptr = nullptr;
  if (pos_ < OBJ_NUM) {
    ptr = &obj_pool_[pos_++];
  } else {
    const int64_t size = sizeof(T);
    ptr = allocator_.alloc(size);
  }
  if (OB_ISNULL(ptr)) {
    TABLELOCK_LOG_RET(ERROR, common::OB_ALLOCATE_MEMORY_FAILED, "obj alloc error, no memory", K(*this), K(lbt()));
  } else {
    ATOMIC_INC(&alloc_count_);
    TABLELOCK_LOG(DEBUG, "obj alloc succ", K(*this), KP(ptr), K(lbt()));
  }
  return ptr;
}

template <typename T, int64_t OBJ_NUM>
void ObArenaObjPool<T, OBJ_NUM>::free(void *obj)
{
  bool need_free = true;
  if (OB_ISNULL(obj)) {
    TABLELOCK_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "obj is null, unexpected error", KP(obj), K(*this), K(lbt()));
  } else {
    TABLELOCK_LOG(DEBUG, "object free succ", KP(obj), K(*this), K(lbt()));
    ATOMIC_INC(&free_count_);
    for (int i = 0; need_free && i < pos_; i++) {
      if (obj == &obj_pool_[i]) {
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
  pos_ = 0;
  alloc_count_ = 0;
  free_count_ = 0;
}

} // tablelock
} // transaction
} // oceanbase

#endif
