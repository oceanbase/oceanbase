/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "oceanbase/ob_plugin_allocator.h"
#include "lib/allocator/ob_malloc.h"
#include "plugin/adaptor/ob_plugin_adaptor.h"
#include "share/rc/ob_tenant_base.h"

using namespace oceanbase;
using namespace oceanbase::lib;
using namespace oceanbase::common;
using namespace oceanbase::plugin;

#ifdef __cplusplus
extern "C" {
#endif

OBP_PUBLIC_API void *obp_malloc(int64_t size)
{
  return ob_malloc(size, ObMemAttr(MTL_ID(), default_plugin_memory_label));
}

OBP_PUBLIC_API void *obp_malloc_align(int64_t alignment, int64_t size)
{
  return ob_malloc_align(alignment, size, ObMemAttr(MTL_ID(), default_plugin_memory_label));
}

OBP_PUBLIC_API void obp_free(void *ptr)
{
  return ob_free(ptr);
}

OBP_PUBLIC_API void *obp_allocate(ObPluginAllocatorPtr allocator, int64_t size)
{
  void *ptr = nullptr;
  if (OB_ISNULL(allocator)) {
  } else {
    ptr = ((ObIAllocator *)allocator)->alloc(size);
  }
  return ptr;
}

OBP_PUBLIC_API void *obp_allocate_align(ObPluginAllocatorPtr allocator, int64_t alignment, int64_t size)
{
  void *ptr = nullptr;
  if (OB_ISNULL(allocator)) {
  } else {
    ptr = ((ObIAllocator *)allocator)->alloc_align(size, alignment);
  }
  return ptr;
}
OBP_PUBLIC_API void obp_deallocate(ObPluginAllocatorPtr allocator, void *ptr)
{
  if (OB_ISNULL(allocator) || OB_ISNULL(ptr)) {
  } else {
    ((ObIAllocator *)allocator)->free(ptr);
  }
}

#ifdef __cplusplus
} // extern "C"
#endif
