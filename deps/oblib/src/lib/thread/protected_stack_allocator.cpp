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

#define USING_LOG_PREFIX LIB

#include "lib/thread/protected_stack_allocator.h"
#include <cmath>
#include <errno.h>
#include <sys/mman.h>
#include "lib/ob_errno.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/alloc/abit_set.h"

namespace oceanbase
{
using namespace common;

namespace lib
{
ProtectedStackAllocator g_stack_allocer;
StackMgr g_stack_mgr;

ssize_t ProtectedStackAllocator::page_size()
{
  return get_page_size();
}

/*
  protected_page & user_stack must be aligned with page_size.

  before
      low_addr                           <<<                         high_addr
      --------------------------------------------------------------
      | StackHeader | padding |    protected page     | user_stack |
      --------------------------------------------------------------
      |       page_size       |       page_size       | stack_size |
      --------------------------------------------------------------
       aligned with page_size-^                       ^-return

  now
      low_addr                           <<<                         high_addr
      ------------------------------------------------------------------------
      | padding | StackHeader |    protected page     | user_stack | padding |
      ------------------------------------------------------------------------
      |                       |       page_size       | stack_size |         |
      ------------------------------------------------------------------------
       aligned with page_size-^                       ^-return
*/

void *ProtectedStackAllocator::alloc(const uint64_t tenant_id,
                                     const ssize_t stack_size)
{
  return _alloc(tenant_id, ObCtxIds::CO_STACK, stack_size, true);
}

void *ProtectedStackAllocator::smart_call_alloc(const uint64_t tenant_id,
                                                const ssize_t stack_size)
{
  return _alloc(tenant_id, ObCtxIds::DEFAULT_CTX_ID, stack_size, false);
}

void *ProtectedStackAllocator::_alloc(const uint64_t tenant_id,
                                      const uint64_t ctx_id,
                                      const ssize_t stack_size,
                                      const bool guard_page)
{
  void *ptr = nullptr;

  int ret = OB_SUCCESS;
  const ssize_t ps = page_size();
  const ssize_t alloc_size = stack_size + ps * 2 + sizeof(ObStackHeader);
  if (stack_size < ps || ACHUNK_PURE_HEADER_SIZE + sizeof(ObStackHeader) > ps) {
    LOG_ERROR("invalid arg", K(stack_size), K(alloc_size));
  } else if (OB_ISNULL(ptr = __alloc(tenant_id, ctx_id, alloc_size, guard_page))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc failed", K(ret));
  } else {
    // do nothing
  }

  return ptr;
}

void *ProtectedStackAllocator::__alloc(const uint64_t tenant_id,
                                       const uint64_t ctx_id,
                                       const ssize_t size,
                                       const bool guard_page)
{
  void *ptr = nullptr;

  const ssize_t ps = page_size();
  const ObMemAttr attr(tenant_id, "CoStack", ctx_id, OB_HIGH_ALLOC);
  // page at bottom will be used as guard-page
  char *buffer = (char *)ob_malloc(size, attr);
  if (OB_ISNULL(buffer)) {
    LOG_ERROR_RET(OB_ALLOCATE_MEMORY_FAILED, "CO_STACK alloc failed", K(size));
  } else {
    uint64_t base = (uint64_t)buffer;
    ObStackHeader *header = nullptr;
    if (base + sizeof(ObStackHeader) > align_up2(base, ps)) {
      base += ps;
    }
    base = align_up2(base, ps);
    header = new ((char *)base - sizeof(ObStackHeader)) ObStackHeader;
    header->tenant_id_ = tenant_id;
    header->size_ = size;
    header->pth_ = 0;
    header->base_ = buffer;
    header->has_guarded_page_ = guard_page;
    g_stack_mgr.insert(header);

    if (guard_page && 0 != mprotect((char*)base, ps, PROT_NONE)) {
      LOG_WARN_RET(OB_ERR_SYS, "mprotect failed", K(errno), K(base), K(ps));
    }
    ptr = (char*)header + sizeof(ObStackHeader) + ps;
  }
  return ptr;
}

void ProtectedStackAllocator::dealloc(void *ptr)
{
  if (OB_ISNULL(ptr)) {
    // do nothing
  } else {
    ObStackHeader *header = stack_header(ptr);
    abort_unless(header->check_magic());
    char *base = (char *)header->base_;
    const ssize_t ps = page_size();
    if (header->has_guarded_page_
        && 0 != mprotect((char *)header + sizeof(ObStackHeader), ps, PROT_READ | PROT_WRITE)) {
      LOG_WARN_RET(OB_ERR_SYS, "mprotect failed", K(errno), K(header), K(ps));
    } else {
      const uint64_t tenant_id = header->tenant_id_;
      const ssize_t size = header->size_;
      g_stack_mgr.erase(header);
      ob_free(base);
    }
  }
}

ObStackHeader *ProtectedStackAllocator::stack_header(void *ptr)
{
  return (ObStackHeader *)((char *)ptr - page_size() - sizeof(ObStackHeader));
}

void StackMgr::insert(ObStackHeader *header)
{
  if (header != nullptr) {
    abort_unless(header->check_magic());
    rwlock_.wrlock(common::ObLatchIds::DEFAULT_SPIN_RWLOCK);
    header->prev_ = &dummy_;
    header->next_ = dummy_.next_;
    dummy_.next_->prev_ = header;
    dummy_.next_ = header;
    rwlock_.unlock();
  }
}

void StackMgr::erase(ObStackHeader *header)
{
  if (header != nullptr) {
    abort_unless(header->check_magic());
    rwlock_.wrlock(common::ObLatchIds::DEFAULT_SPIN_RWLOCK);
    header->prev_->next_ = header->next_;
    header->next_->prev_ = header->prev_;
    header->prev_ = header->next_ = header;
    rwlock_.unlock();
  }
}

ObStackHeaderGuard::ObStackHeaderGuard()
{
  header_.pth_ = (uint64_t)pthread_self();
  g_stack_mgr.insert(&header_);
}

ObStackHeaderGuard::~ObStackHeaderGuard()
{
  g_stack_mgr.erase(&header_);
}

}  // lib
}  // oceanbase
