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
#include "lib/rc/context.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace lib
{
_RLOCAL(bool, ContextTLOptGuard::enable_tl_opt);

__MemoryContext__ &__MemoryContext__::root()
{
  static __MemoryContext__ *root = nullptr;
  if (OB_UNLIKELY(nullptr == root)) {
    static lib::ObMutex mutex;
    lib::ObMutexGuard guard(mutex);
    if (nullptr == root) {
      ContextParam param;
      param.set_properties(ADD_CHILD_THREAD_SAFE | ALLOC_THREAD_SAFE)
        .set_parallel(4)
        .set_mem_attr(OB_SERVER_TENANT_ID, ObModIds::OB_ROOT_CONTEXT, ObCtxIds::DEFAULT_CTX_ID);
      // root_context相对底层，被其他static对象依赖，而static对象之间析构顺序又是不确定的,
      // So here is modeled on ObMallocAllocator to design a non-destroy mode
      static StaticInfo static_info{__FILENAME__, __LINE__, __FUNCTION__};
      __MemoryContext__ *tmp = new (std::nothrow) __MemoryContext__(false, DynamicInfo(), nullptr, param, &static_info);
      abort_unless(tmp != nullptr);
      int ret = tmp->init();
      abort_unless(OB_SUCCESS == ret);
      root = tmp;
    }
  }
  return *root;
}
#ifdef OB_USE_ASAN
bool __MemoryContext__::enable_asan_allocator = false;
#endif
MemoryContext &MemoryContext::root()
{
  static MemoryContext root(&__MemoryContext__::root());
  return root;
}

int64_t MemoryContext::tree_mem_hold()
{
  int64_t total = 0;
  if (OB_LIKELY(ref_context_ != nullptr)) {
    total = ref_context_->tree_mem_hold();
  }
  return total;
}

} // end of namespace lib
} // end of namespace oceanbase
