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

#ifndef PROTECTED_STACK_ALLOCATOR
#define PROTECTED_STACK_ALLOCATOR

#include <stdint.h>
#include <stddef.h>
#include "lib/lock/ob_mutex.h"

namespace oceanbase {
namespace lib {

struct ObStackHeader
{
  constexpr static uint64_t MAGIC = 0xcaca12344321acac;
  bool check_magic() { return MAGIC == magic_; }
  ObStackHeader()
    : magic_(MAGIC) {}
  uint64_t magic_;
  uint64_t tenant_id_;
  ssize_t size_;
  // use thread instead of alloc
  uint64_t pth_;
  char *base_;
  bool has_guarded_page_;
  ObStackHeader *prev_;
  ObStackHeader *next_;
};

class ProtectedStackAllocator
{
public:
  void *alloc(const uint64_t tenant_id, const ssize_t size);
  void *smart_call_alloc(const uint64_t tenant_id, const ssize_t size);
  void dealloc(void *ptr);
  static ObStackHeader *stack_header(void *ptr);
  static ssize_t page_size();
private:
  void *_alloc(const uint64_t tenant_id, const uint64_t ctx_id, const ssize_t size,
               const bool guard_page);
  void *__alloc(const uint64_t tenant_id, const uint64_t ctx_id, const ssize_t size,
                const bool guard_page);
};

class ObMemoryCutter;
class StackMgr
{
  friend class ObMemoryCutter;
public:
  class Guard
  {
  public:
    Guard(StackMgr& mgr) : mgr_(mgr), cur_(nullptr)
    {
      mgr_.mutex_.lock();
      cur_ = mgr_.dummy_.next_;
    }
    ~Guard() { mgr_.mutex_.unlock(); }
    ObStackHeader* operator*() { return (cur_ == &(mgr_.dummy_)) ? nullptr : cur_; }
    ObStackHeader* next()
    {
      cur_ = cur_->next_;
      return (cur_ == &(mgr_.dummy_)) ? nullptr : cur_;
    }
  private:
    StackMgr& mgr_;
    ObStackHeader* cur_;
  };
  StackMgr()
  {
    dummy_.prev_ = dummy_.next_ = &dummy_;
  }
  void insert(ObStackHeader *);
  void erase(ObStackHeader *);
private:
  ObStackHeader *begin() { return dummy_.next_; }
  ObStackHeader *end() { return &dummy_; }
private:
  lib::ObMutex mutex_;
  ObStackHeader dummy_;
};

class ObStackHeaderGuard
{
public:
  ObStackHeaderGuard();
  ~ObStackHeaderGuard();
private:
  ObStackHeader header_;
};

extern ProtectedStackAllocator g_stack_allocer;
extern StackMgr g_stack_mgr;

}  // lib
}  // oceanbase

#endif /* PROTECTED_STACK_ALLOCATOR */
