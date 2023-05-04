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

#ifndef OCEANBASE_LIB_ALLOCATOR_OB_POOLED_ALLOCATOR_H_
#define OCEANBASE_LIB_ALLOCATOR_OB_POOLED_ALLOCATOR_H_

#include "lib/objectpool/ob_pool.h"

namespace oceanbase
{
namespace common
{
// @note thread-safe depends on LockT
template <typename T, typename BlockAllocatorT = ObMalloc, typename LockT = ObNullLock>
class ObPooledAllocator
{
public:
  ObPooledAllocator(int64_t block_size = common::OB_MALLOC_NORMAL_BLOCK_SIZE,
                    const BlockAllocatorT &alloc = BlockAllocatorT(ObModIds::OB_POOL));
  virtual ~ObPooledAllocator();
  void set_attr(const lib::ObMemAttr &attr) { the_pool_.set_attr(attr); }

  T *alloc();
  void free(T *obj);
  void reset();
  void clear() { reset(); }
  void inc_ref() {};
  void dec_ref() {};

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObPooledAllocator);
private:
  // data members
  ObPool<BlockAllocatorT, LockT> the_pool_;
};

template <typename T, typename BlockAllocatorT, typename LockT>
ObPooledAllocator<T, BlockAllocatorT, LockT>::ObPooledAllocator(int64_t block_size,
                                                                const BlockAllocatorT &alloc)
    : the_pool_(sizeof(T), block_size, alloc)
{
}

template <typename T, typename BlockAllocatorT, typename LockT>
ObPooledAllocator<T, BlockAllocatorT, LockT>::~ObPooledAllocator()
{
}

template <typename T, typename BlockAllocatorT, typename LockT>
void ObPooledAllocator<T, BlockAllocatorT, LockT>::reset()
{
  the_pool_.reset();
}

template <typename T, typename BlockAllocatorT, typename LockT>
T *ObPooledAllocator<T, BlockAllocatorT, LockT>::alloc()
{
  T *ret = NULL;
  void *p = the_pool_.alloc();
  if (OB_ISNULL(p)) {
    LIB_LOG_RET(ERROR, OB_ALLOCATE_MEMORY_FAILED, "no memory");
  } else {
    ret = new(p) T();
  }
  return ret;
}

template <typename T, typename BlockAllocatorT, typename LockT>
void ObPooledAllocator<T, BlockAllocatorT, LockT>::free(T *obj)
{
  if (OB_LIKELY(NULL != obj)) {
    obj->~T();
    the_pool_.free(obj);
    obj = NULL;
  }
}

} // end namespace common
} // end namespace oceanbase

#endif //OCEANBASE_LIB_ALLOCATOR_OB_POOLED_ALLOCATOR_H_
