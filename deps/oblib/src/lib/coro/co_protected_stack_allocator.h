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

#ifndef CO_PROTECTED_STACK_ALLOCATOR
#define CO_PROTECTED_STACK_ALLOCATOR

#include <stdint.h>
#include <stddef.h>
#include "lib/lock/ob_mutex.h"

namespace oceanbase {
namespace lib {

struct ObStackHeader {
  constexpr static uint64_t MAGIC = 0xcaca12344321acac;
  bool check_magic()
  {
    return MAGIC == magic_;
  }
  ObStackHeader() : magic_(MAGIC)
  {}
  uint64_t magic_;
  uint64_t tenant_id_;
  ssize_t size_;
  // use thread instead of alloc
  uint64_t pth_;
  ObStackHeader* prev_;
  ObStackHeader* next_;
};

class CoProtectedStackAllocator {
private:
  class IMemHoldUpdater {
  public:
    virtual int update_hold(const uint64_t tenant_id, const ssize_t size, bool& updated) = 0;
  };
  class MemHoldUpdater : public IMemHoldUpdater {
  public:
    virtual int update_hold(const uint64_t tenant_id, const ssize_t size, bool& updated);
  };

public:
  void* alloc(const uint64_t tenant_id, const ssize_t size);
  void dealloc(void* ptr);
  static ObStackHeader* stack_header(void* ptr);

private:
  static ssize_t page_size();
  void* __alloc(const uint64_t tenant_id, const ssize_t size);
  static MemHoldUpdater mhu_;
  static IMemHoldUpdater* ref_mhu_;
};

class ObMemoryCutter;
class CoStackMgr {
  friend class ObMemoryCutter;

public:
  CoStackMgr()
  {
    dummy_.prev_ = dummy_.next_ = &dummy_;
  }
  void insert(ObStackHeader*);
  void erase(ObStackHeader*);

private:
  lib::ObMutex mutex_;
  ObStackHeader dummy_;
};

extern CoProtectedStackAllocator g_stack_allocer;
extern CoStackMgr g_stack_mgr;

}  // namespace lib
}  // namespace oceanbase

#endif /* CO_PROTECTED_STACK_ALLOCATOR */
