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

#include "co_context.h"
#include "lib/ob_errno.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/coro/co_var_center.h"
#include "lib/coro/co_protected_stack_allocator.h"

using namespace oceanbase::lib;
using namespace oceanbase::common;

EXTERN_C_BEGIN
extern void* __libc_malloc(size_t size);
extern void __libc_free(void* ptr);
EXTERN_C_END

namespace oceanbase {
namespace lib {

int CoCtx::create(const uint64_t tenant_id, size_t ss_size, CoCtx* cc)
{
  volatile int ret = OB_SUCCESS;

  if (OB_FAIL(cc->init(tenant_id, ss_size))) {
    LOG_WARN("init stack failed", K(ret), K(tenant_id), K(ss_size));
  }

  return ret;
}

void CoCtx::destroy(CoCtx* cc)
{
  if (!OB_ISNULL(cc)) {
    if (!OB_ISNULL(cc->ss_sp_)) {
      auto* stack_header = CoProtectedStackAllocator::stack_header(cc->ss_sp_);
      g_stack_mgr.erase(stack_header);
      g_stack_allocer.dealloc(cc->ss_sp_);
      cc->ss_sp_ = nullptr;
    }
  }
}

int CoCtx::init(const uint64_t tenant_id, const size_t ss_size)
{
  int ret = OB_SUCCESS;
  const auto crls_size = coro::CoConfig::MAX_CRLS_SIZE;
  auto total_size = ss_size + crls_size;
  void* ss_buf = g_stack_allocer.alloc(tenant_id, total_size);
  if (nullptr == ss_buf) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    ss_sp_ = (char*)ss_buf;
    ss_size_ = ss_size;
    auto* stack_header = CoProtectedStackAllocator::stack_header(ss_sp_);
    abort_unless(stack_header->check_magic());
    // The assumption here is coroutine stack must be the application of this thread,
    // which can be guaranteed at present
    stack_header->pth_ = (uint64_t)pthread_self();
    g_stack_mgr.insert(stack_header);
  }

  return ret;
}

}  // namespace lib
}  // namespace oceanbase
