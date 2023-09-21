/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

// A Smaller Thread-Safe Arena Allocator for libobcdc

#ifndef OCEANBASE_LIBOBCDC_OB_LOG_SAFE_ARENA_
#define OCEANBASE_LIBOBCDC_OB_LOG_SAFE_ARENA_

#include "lib/allocator/ob_allocator.h"
#include "lib/allocator/page_arena.h"
#include "lib/lock/ob_small_spin_lock.h"

namespace oceanbase
{
namespace libobcdc
{

class ObCdcSafeArena: public common::ObIAllocator
{
public:
  ObCdcSafeArena(const lib::ObLabel &label = ObModIds::OB_MODULE_PAGE_ALLOCATOR,
      const int64_t page_size = OB_MALLOC_NORMAL_BLOCK_SIZE,
      int64_t tenant_id = OB_SERVER_TENANT_ID,
      int64_t ctx_id = 0) :
      arena_(label, page_size, tenant_id, ctx_id),
      lock_() {}

  ObCdcSafeArena(
    ObIAllocator &base_allocator,
    const lib::ObLabel &label = ObModIds::OB_MODULE_PAGE_ALLOCATOR,
    int64_t tenant_id = OB_SERVER_TENANT_ID,
    const int64_t page_size = OB_MALLOC_NORMAL_BLOCK_SIZE,
    int64_t ctx_id = 0) :
    arena_(base_allocator, page_size)
  {
    ObMemAttr attr(tenant_id, label, ctx_id);
    arena_.set_attr(attr);
  }

  virtual ~ObCdcSafeArena() {}
  virtual void *alloc(const int64_t size) override
  {
    ObByteLockGuard guard(lock_);
    return arena_.alloc(size);
  }
  virtual void* alloc(const int64_t size, const ObMemAttr &attr)
  {
    ObByteLockGuard guard(lock_);
    return arena_.alloc(size, attr);
  }
  virtual void free(void *ptr) override
  {
    ObByteLockGuard guard(lock_);
    arena_.free(ptr);
  }

  virtual void clear()
  {
    ObByteLockGuard guard(lock_);
    arena_.clear();
  }

  virtual int64_t total() const override
  {
    return arena_.total();
  }
  virtual int64_t used() const override
  {
    return arena_.used();
  }
  virtual void reset() override
  {
    ObByteLockGuard guard(lock_);
    arena_.reset();
  }
  virtual void reuse() override
  {
    ObByteLockGuard guard(lock_);
    arena_.reuse();
  }
  virtual void set_attr(const ObMemAttr &attr) override
  {
    ObByteLockGuard guard(lock_);
    arena_.set_attr(attr);
  }

private:
  ObArenaAllocator arena_;
  ObByteLock lock_;
};

}
}



 #endif