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
  ObStackHeader *prev_;
  ObStackHeader *next_;
};

class ProtectedStackAllocator
{
public:
  void *alloc(const uint64_t tenant_id, const ssize_t size);
  void dealloc(void *ptr);
  static ssize_t adjust_size(const ssize_t size);
  static ObStackHeader *stack_header(void *ptr);
  static ssize_t page_size();
private:
  void *__alloc(const uint64_t tenant_id, const ssize_t size);
};

class ObMemoryCutter;
class StackMgr
{
  friend class ObMemoryCutter;
public:
  StackMgr()
    : flow_print_pos_(0)
  {
    dummy_.prev_ = dummy_.next_ = &dummy_;
  }
  void insert(ObStackHeader *);
  void erase(ObStackHeader *);
  ObStackHeader *begin() { return dummy_.next_; }
  ObStackHeader *end() { return &dummy_; }
private:
  void add_flow(ObStackHeader *header, bool is_alloc);
private:
  lib::ObMutex mutex_;
  char flow_print_buf_[8192];
  int64_t flow_print_pos_;
  ObStackHeader dummy_;
};

extern ProtectedStackAllocator g_stack_allocer;
extern StackMgr g_stack_mgr;

}  // lib
}  // oceanbase

#endif /* PROTECTED_STACK_ALLOCATOR */
