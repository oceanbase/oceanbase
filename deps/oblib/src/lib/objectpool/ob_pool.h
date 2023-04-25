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

#ifndef LIB_OBJECTPOOL_OB_POOL_
#define LIB_OBJECTPOOL_OB_POOL_
#include "lib/lock/ob_spin_lock.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/ob_define.h"

namespace oceanbase
{
namespace common
{
/**
 * fixed size objects pool
 * suitable for allocating & deallocating lots of objects dynamically & frequently
 * @note not thread-safe
 * @note obj_size >= sizeof(void*)
 * @see ObLockedPool
 */
template <typename BlockAllocatorT = ObMalloc, typename LockT = ObNullLock>
class ObPool
{
public:
  ObPool(int64_t obj_size, int64_t block_size = common::OB_MALLOC_NORMAL_BLOCK_SIZE,
         const BlockAllocatorT &alloc = BlockAllocatorT(ObModIds::OB_POOL));
  virtual ~ObPool();
  void reset();
  void *alloc();
  void free(void *obj);
  void set_label(const lib::ObLabel &label) { block_allocator_.set_label(label); }
  void set_attr(const lib::ObMemAttr &attr) { block_allocator_.set_attr(attr); }

  uint64_t get_free_count() const;
  uint64_t get_in_use_count() const;
  uint64_t get_total_count() const;
  int64_t get_obj_size() const { return obj_size_; }
  int64_t get_total_obj_size() const { return (get_total_count() * get_obj_size()); }
  int mprotect_mem_pool(int prot);
private:
  void *freelist_pop();
  void freelist_push(void *obj);
  void alloc_new_block();
private:
  struct FreeNode
  {
    FreeNode *next_;
  };
  struct BlockHeader
  {
    BlockHeader *next_;
  };
private:
  // data members
  int64_t obj_size_;
  int64_t block_size_;
  volatile uint64_t in_use_count_;
  volatile uint64_t free_count_;
  volatile uint64_t total_count_;
  FreeNode *freelist_;
  BlockHeader *blocklist_;
  BlockAllocatorT block_allocator_;
  LockT lock_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObPool);
};

template <typename BlockAllocatorT, typename LockT>
inline uint64_t ObPool<BlockAllocatorT, LockT>::get_free_count() const
{
  return free_count_;
}

template <typename BlockAllocatorT, typename LockT>
inline uint64_t ObPool<BlockAllocatorT, LockT>::get_in_use_count() const
{
  return in_use_count_;
}

template <typename BlockAllocatorT, typename LockT>
inline uint64_t ObPool<BlockAllocatorT, LockT>::get_total_count() const
{
  return total_count_;
}

////////////////////////////////////////////////////////////////
/// thread-safe pool by using spin-lock
typedef ObPool<ObMalloc, ObSpinLock> ObLockedPool;

////////////////////////////////////////////////////////////////
// A small block allocator which split the block allocated by the BlockAllocator into small blocks
template <typename BlockAllocatorT = ObMalloc, typename LockT = ObNullLock>
class ObSmallBlockAllocator : public ObIAllocator
{
public:
  ObSmallBlockAllocator(int64_t small_block_size, int64_t block_size,
                        const BlockAllocatorT &alloc = BlockAllocatorT(ObModIds::OB_POOL))
      : block_pool_(small_block_size, block_size, alloc)
  {
  }
  // Caution: for hashmap
  ObSmallBlockAllocator()
      : block_pool_(sizeof(void *))
  {
  }

  virtual ~ObSmallBlockAllocator() {}

  virtual void *alloc(const int64_t sz)
  {
    void *ptr_ret = NULL;
    if (sz > block_pool_.get_obj_size()) {
      LIB_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "Wrong block size", K(sz), K(block_pool_.get_obj_size()));
    } else {
      ptr_ret = block_pool_.alloc();
    }
    return ptr_ret;
  }
  virtual void *alloc(int64_t sz, const ObMemAttr &attr)
  {
    UNUSED(attr);
    return alloc(sz);
  }
  virtual void free(void *ptr) { block_pool_.free(ptr);};
  void set_label(const lib::ObLabel &label) {block_pool_.set_label(label);};
  void set_attr(const lib::ObMemAttr &attr) { block_pool_.set_attr(attr); }
  int64_t get_total_mem_size() const { return block_pool_.get_total_obj_size(); }
  int mprotect_small_allocator(int prot) { return block_pool_.mprotect_mem_pool(prot); }
  void reset() override { block_pool_.reset(); }
private:
  // data members
  ObPool<BlockAllocatorT, LockT> block_pool_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObSmallBlockAllocator);
};

} // end namespace common
} // end namespace oceanbase

#include "ob_pool.ipp"

#endif /* LIB_OBJECTPOOL_OB_POOL_ */
