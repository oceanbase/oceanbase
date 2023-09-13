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

#ifndef  OCEANBASE_COMMON_ALLOCATOR_H_
#define  OCEANBASE_COMMON_ALLOCATOR_H_

#include "lib/ob_define.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/alloc/alloc_struct.h"
#include "lib/alloc/object_set.h"
#include "lib/alloc/ob_malloc_allocator.h"
#include "lib/alloc/alloc_interface.h"
#include "lib/allocator/ob_page_manager.h"
#ifndef ENABLE_SANITY
#include "lib/lock/ob_latch.h"
#else
#include "lib/alloc/ob_latch_v2.h"
#endif

namespace oceanbase
{
namespace lib
{
class __MemoryContext__;
}
namespace common
{
using lib::__MemoryContext__;
using lib::ObjectSet;
using lib::AObject;
using common::ObPageManager;

class ObAllocator : public ObIAllocator
{
  friend class lib::__MemoryContext__;
  friend class ObParallelAllocator;
public:
  ObAllocator(__MemoryContext__ *mem_context, const ObMemAttr &attr,
              const bool use_pm=false,
              const uint32_t ablock_size=lib::INTACT_NORMAL_AOBJECT_SIZE);
  virtual ~ObAllocator() {}
  virtual void *alloc(const int64_t size) override
  {
    return alloc(size, attr_);
  }
  virtual void *alloc(const int64_t size, const ObMemAttr &attr) override;
  virtual void free(void *ptr) override;
  int64_t hold() const;
  int64_t total() const override { return hold(); }
  int64_t used() const override;
  void *get_pm() { return pm_; }
private:
  int init();
  void lock() { locker_->lock(); }
  void unlock() { locker_->unlock(); }
  bool trylock() { return locker_->trylock(); }
  // for unittest only
  void reset() override { os_.reset(); }
private:
  __MemoryContext__ *mem_context_;
  ObMemAttr attr_;
  const bool use_pm_;
  void *pm_;
  lib::ISetLocker *locker_;
  lib::SetDoNothingLocker do_nothing_locker_;
#ifndef ENABLE_SANITY
  lib::ObMutex mutex_;
#else
  lib::ObMutexV2 mutex_;
#endif
  lib::SetLocker<decltype(mutex_)> do_locker_;
  class : public lib::IBlockMgr
  {
  public:
    virtual ABlock *alloc_block(uint64_t size, const ObMemAttr &attr) override
    {
      ABlock *block = NULL;
      auto ta =
        lib::ObMallocAllocator::get_instance()->get_tenant_ctx_allocator(tenant_id_, ctx_id_);
      if (OB_NOT_NULL(ta)) {
        block = ta->get_block_mgr().alloc_block(size, attr);
      }
      return block;
    }
    virtual void free_block(ABlock *block) override
    {
      auto ta =
        lib::ObMallocAllocator::get_instance()->get_tenant_ctx_allocator(tenant_id_, ctx_id_);
      if (OB_NOT_NULL(ta)) {
        ta->get_block_mgr().free_block(block);
      } else {
        OB_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "tenant ctx allocator is null", K(tenant_id_), K(ctx_id_));
      }
    }
    virtual int64_t sync_wash(int64_t wash_size) override
    {
      int64_t washed_size = 0;
      auto ta =
        lib::ObMallocAllocator::get_instance()->get_tenant_ctx_allocator(tenant_id_, ctx_id_);
      if (OB_NOT_NULL(ta)) {
        washed_size = ta->get_block_mgr().sync_wash(wash_size);
      }
      return washed_size;
    }
  } blk_mgr_, nblk_mgr_;
  ObjectSet os_;
  ObjectSet nos_;
  bool is_inited_;
public:
};

inline ObAllocator::ObAllocator(__MemoryContext__ *mem_context, const ObMemAttr &attr, const bool use_pm,
                                const uint32_t ablock_size)
  : mem_context_(mem_context),
    attr_(attr),
    use_pm_(use_pm),
    pm_(nullptr),
    locker_(&do_nothing_locker_),
    mutex_(common::ObLatchIds::OB_ALLOCATOR_LOCK),
    do_locker_(mutex_),
    os_(mem_context_, ablock_size),
    nos_(mem_context_, ablock_size),
    is_inited_(false)
{
}

inline int ObAllocator::init()
{
  int ret = OB_SUCCESS;
  ObPageManager *pm = nullptr;
  lib::IBlockMgr *blk_mgr = nullptr;
  lib::IBlockMgr *nblk_mgr = nullptr;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    OB_LOG(ERROR, "init twice", K(ret));
  } else if (use_pm_ &&
             (pm = ObPageManager::thread_local_instance()) != nullptr &&
             attr_.tenant_id_ == pm->get_tenant_id() &&
             attr_.ctx_id_ == pm->get_ctx_id()) {
    blk_mgr = pm;
    pm_ = pm;
  } else {
    blk_mgr_.set_tenant_id(attr_.tenant_id_);
    blk_mgr_.set_ctx_id(attr_.ctx_id_);
    blk_mgr = &blk_mgr_;
  }

  if (OB_SUCC(ret)) {
    nblk_mgr_.set_tenant_id(OB_SERVER_TENANT_ID);
    nblk_mgr_.set_ctx_id(attr_.ctx_id_);
    nblk_mgr = &nblk_mgr_;
    os_.set_block_mgr(blk_mgr);
    os_.set_locker(locker_);
    nos_.set_block_mgr(nblk_mgr);
    nos_.set_locker(locker_);
    is_inited_ = true;
  }
  return ret;
}

inline int64_t ObAllocator::hold() const
{
  return os_.get_hold_bytes();
}

OB_INLINE int64_t ObAllocator::used() const
{
  return os_.get_alloc_bytes();
}

class ObParallelAllocator : public ObIAllocator
{
  // Maximum concurrency
  static const int N = 8;
public:
  ObParallelAllocator(ObAllocator &root_allocator,
                      __MemoryContext__ *mem_context,
                      const ObMemAttr &attr,
                      const int parallel=4,
                      const uint32_t ablock_size=lib::INTACT_NORMAL_AOBJECT_SIZE);
  virtual ~ObParallelAllocator();
  void *alloc(const int64_t size) override
  {
    return alloc(size, attr_);
  }
  void *alloc(const int64_t size, const ObMemAttr &attr) override;
  void free(void *ptr) override;
  int64_t hold() const;
  int64_t total() const override { return hold(); }
  int64_t used() const override;
private:
  int init();
private:
  ObAllocator &root_allocator_;
  __MemoryContext__ *mem_context_;
  ObMemAttr attr_;
  uint32_t ablock_size_;
  // buffer of sub_allocators_
  void *buf_;
  // Static allocation takes up too much space, considering that there is less demand for parallel multiple channels, change to dynamic allocation
  ObAllocator *sub_allocators_[N];
  const int sub_cnt_;
  bool is_inited_;
  // for init
#ifndef ENABLE_SANITY
  lib::ObMutex mutex_;
#else
  lib::ObMutexV2 mutex_;
#endif
};

inline ObParallelAllocator::ObParallelAllocator(ObAllocator &root_allocator,
                                                 __MemoryContext__ *mem_context,
                                                 const ObMemAttr &attr,
                                                 const int parallel,
                                                 const uint32_t ablock_size)
  : root_allocator_(root_allocator), mem_context_(mem_context), attr_(attr),
    ablock_size_(ablock_size), buf_(nullptr), sub_cnt_(MIN(parallel, N)),
    is_inited_(false)
{
  for (int i = 0; i < sub_cnt_; i++) {
    sub_allocators_[i] = nullptr;
  }
}

inline ObParallelAllocator::~ObParallelAllocator()
{
  for (int64_t i = 0; i < sub_cnt_; i++) {
    if (sub_allocators_[i] != nullptr) {
      sub_allocators_[i]->~ObAllocator();
      sub_allocators_[i] = nullptr;
    }
  }
  // Release the memory of the multipath itself
  if (buf_ != nullptr) {
    root_allocator_.free(buf_);
    buf_ = nullptr;
  }
}

inline int ObParallelAllocator::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
  } else {
    buf_ = root_allocator_.alloc(sizeof(ObAllocator) * sub_cnt_);
    if (OB_ISNULL(buf_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    }
    for (int i = 0; OB_SUCC(ret) && i < sub_cnt_; i++) {
      sub_allocators_[i] = new ((char*)buf_ + sizeof(ObAllocator) *  i)
        ObAllocator(mem_context_, attr_, false, ablock_size_);
      sub_allocators_[i]->locker_ = &sub_allocators_[i]->do_locker_;
    }
    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
  }
  return ret;
}

inline int64_t ObParallelAllocator::hold() const
{
  int64_t hold = 0;
  if (is_inited_) {
    hold += root_allocator_.hold();
    for (int64_t i = 0; i < sub_cnt_; i++) {
      hold += sub_allocators_[i]->hold();
    }
  }
  return hold;
}

inline int64_t ObParallelAllocator::used() const
{
  int64_t used = 0;
  if (is_inited_) {
    used += root_allocator_.used();
    for (int64_t i = 0; i < sub_cnt_; i++) {
      used += sub_allocators_[i]->used();
    }
  }
  return used;
}

}
}

#endif //OCEANBASE_COMMON_ALLOCATOR_H_
