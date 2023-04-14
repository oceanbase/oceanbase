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

#ifndef OCEANBASE_COMMON_SAFE_ARENA_H__
#define OCEANBASE_COMMON_SAFE_ARENA_H__

#include "lib/allocator/page_arena.h"  // ModuleArena
#include "lib/lock/ob_spin_lock.h"     // ObSpinLock

namespace oceanbase
{
namespace common
{
class ObSafeArena : public ObIAllocator
{
public:
  ObSafeArena(const lib::ObLabel &label, const int64_t page_size = OB_MALLOC_NORMAL_BLOCK_SIZE,
              int64_t tenant_id = OB_SERVER_TENANT_ID)
      : arena_alloc_(label, page_size, tenant_id),
        lock_(ObLatchIds::OB_AREAN_ALLOCATOR_LOCK)
  {}

  virtual ~ObSafeArena() {}

public:
  virtual void *alloc(const int64_t sz) override
  {
    ObSpinLockGuard guard(lock_);
    return arena_alloc_.alloc(sz);
  }

  virtual void* alloc(const int64_t sz, const ObMemAttr &attr) override
  {
    ObSpinLockGuard guard(lock_);
    return arena_alloc_.alloc(sz, attr);
  }

  virtual void free(void *ptr) override
  {
    ObSpinLockGuard guard(lock_);
    arena_alloc_.free(ptr);
  }

  virtual void clear()
  {
    ObSpinLockGuard guard(lock_);
    arena_alloc_.clear();
  }

  virtual void reuse() override
  {
    ObSpinLockGuard guard(lock_);
    arena_alloc_.reuse();
  }

  virtual void reset() override
  {
    ObSpinLockGuard guard(lock_);
    arena_alloc_.reset();
  }

  int64_t used() const override
  { return arena_alloc_.used(); }

  int64_t total() const override
  { return arena_alloc_.total(); }

  ModuleArena &get_arena()
  {
    return arena_alloc_.get_arena();
  }

private:
  ObArenaAllocator arena_alloc_;
  ObSpinLock lock_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObSafeArena);
};
} // namespace common
} // namespace oceanbase
#endif /* OCEANBASE_COMMON_SAFE_ARENA_H__ */
