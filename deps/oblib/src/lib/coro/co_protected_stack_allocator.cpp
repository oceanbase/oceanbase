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

#include "lib/coro/co_protected_stack_allocator.h"
#include <cmath>
#include <errno.h>
#include "lib/ob_errno.h"
#include "lib/coro/co.h"
#include "lib/resource/ob_resource_mgr.h"

namespace oceanbase {
using namespace common;

namespace lib {
CoProtectedStackAllocator g_stack_allocer;
CoStackMgr g_stack_mgr;

int CoProtectedStackAllocator::MemHoldUpdater::update_hold(const uint64_t tenant_id, const ssize_t size, bool& updated)
{
  int ret = OB_SUCCESS;
  updated = false;

  ObTenantResourceMgrHandle resource_handle;
  if (OB_FAIL(ObResourceMgr::get_instance().get_tenant_resource_mgr(tenant_id, resource_handle))) {
    LIB_LOG(ERROR, "get_tenant_resource_mgr failed", K(ret), K(tenant_id));
  } else {
    updated = resource_handle.get_memory_mgr()->coro_stack_update_hold(size);
  }

  return ret;
}

CoProtectedStackAllocator::MemHoldUpdater CoProtectedStackAllocator::mhu_;
CoProtectedStackAllocator::IMemHoldUpdater* CoProtectedStackAllocator::ref_mhu_ = &CoProtectedStackAllocator::mhu_;

ssize_t CoProtectedStackAllocator::page_size()
{
  static ssize_t ps = sysconf(_SC_PAGESIZE);
  return ps;
}

/*
      low_addr                            <<<                                high_addr
      --------------------------------------------------------------------------------
      |        header        |    protected page      |          user_stack          |
      --------------------------------------------------------------------------------
      |       page_size      |      page_size         |                              |
      --------------------------------------------------------------------------------
 */
void* CoProtectedStackAllocator::alloc(const uint64_t tenant_id, const ssize_t size)
{
  void* ptr = nullptr;

  int ret = OB_SUCCESS;
  bool updated = false;
  const ssize_t ps = page_size();
  if (size < ps) {
    LOG_WARN("invalid arg", K(size));
  } else if (OB_FAIL(ref_mhu_->update_hold(tenant_id, size + ps * 2, updated))) {
    LOG_WARN("update hold failed", K(ret), K(tenant_id), K(size));
  } else if (updated) {
    void* tmp_ptr = __alloc(tenant_id, size);
    if (nullptr == tmp_ptr) {
      LOG_WARN("alloc failed", K(ret));
      ref_mhu_->update_hold(tenant_id, -(size + ps * 2), updated);
    } else {
      ptr = (char*)tmp_ptr + ps;
      // LOG_INFO("alloc stack", "map_addr", tmp_ptr, "sp", ptr, "size", size);
    }
  }

  return ptr;
}

void* CoProtectedStackAllocator::__alloc(const uint64_t tenant_id, const ssize_t size)
{
  void* ptr = nullptr;

  const ssize_t ps = page_size();
  // page at bottom will be used as guard-page
  const ssize_t m_size = size + ps * 2;
  void* m_ptr = mmap(0, m_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  if (MAP_FAILED == m_ptr) {
    LOG_WARN("mmap failed", K(size));
  } else {
    ObStackHeader* header = new (m_ptr) ObStackHeader;
    header->tenant_id_ = tenant_id;
    header->size_ = m_size;
    header->pth_ = 0;
    if (0 != mprotect((char*)m_ptr + ps, ps, PROT_NONE)) {
      LOG_WARN("mprotect failed", K(errno));
      munmap(m_ptr, m_size);
    } else {
      ptr = (char*)m_ptr + ps;
    }
  }
  return ptr;
}

void CoProtectedStackAllocator::dealloc(void* ptr)
{
  if (ptr != nullptr) {
    const ssize_t ps = page_size();
    ObStackHeader* header = (ObStackHeader*)((char*)ptr - ps * 2);
    abort_unless(header->check_magic());
    // LOG_INFO("free stack", "map_addr", header);
    if (0 != mprotect((char*)header + ps, ps, PROT_READ)) {
      LOG_WARN("mprotect failed", K(errno));
    } else {
      const uint64_t tenant_id = header->tenant_id_;
      const ssize_t size = header->size_;
      if (0 != munmap(header, size)) {
        LOG_WARN("munmap failed", K(tenant_id), K(size), K(errno));
      } else {
        bool updated = false;
        ref_mhu_->update_hold(tenant_id, -size, updated);
      }
    }
  }
}

ObStackHeader* CoProtectedStackAllocator::stack_header(void* ptr)
{
  return (ObStackHeader*)((char*)ptr - page_size() * 2);
}

void CoStackMgr::insert(ObStackHeader* header)
{
  if (header != nullptr) {
    abort_unless(header->check_magic());
    mutex_.lock();
    header->prev_ = &dummy_;
    header->next_ = dummy_.next_;
    dummy_.next_->prev_ = header;
    dummy_.next_ = header;
    mutex_.unlock();
  }
}

void CoStackMgr::erase(ObStackHeader* header)
{
  if (header != nullptr) {
    abort_unless(header->check_magic());
    mutex_.lock();
    header->prev_->next_ = header->next_;
    header->next_->prev_ = header->prev_;
    header->prev_ = header->next_ = header;
    mutex_.unlock();
  }
}

}  // namespace lib
}  // namespace oceanbase
