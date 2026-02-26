#include <stdint.h>
#include <stddef.h>
#include <stdlib.h>
#include "lib/allocator/ob_malloc.h"
#include "lib/allocator/ob_vslice_alloc.h"
#include "lib/alloc/malloc_hook.h"
#include "lib/resource/achunk_mgr.h"
using namespace oceanbase::common;

#define HOOK_MALLOC 1

oceanbase::common::ObVSliceAlloc g_alloc;

int benchmark_initialize(void)
{
  OB_TSC_TIMESTAMP.init();
  init_malloc_hook();
  oceanbase::lib::set_memory_limit(1L<<40);
#if defined(VSLICE_MALLOC)
  g_alloc.init(8L<<10, default_blk_alloc, ObMemAttr(500, "test"));
  g_alloc.set_nway(16);
#elif defined(OB_MALLOC)
#elif defined(HOOK_MALLOC)
#endif
  //enable_malloc_v2(true);
  return 0;
}

int benchmark_finalize(void) { return 0; }

int benchmark_thread_initialize(void) { return 0;}

int benchmark_thread_finalize(void) { return 0; }

void benchmark_thread_collect(void) {}

void* benchmark_malloc(size_t alignment, size_t size)
{
#if defined(VSLICE_MALLOC)
  return g_alloc.alloc(size);
#elif defined(OB_MALLOC)
  return oceanbase::common::ob_malloc(size, "test");
#elif defined(HOOK_MALLOC)
  return malloc(size);
#endif
}

void benchmark_free(void* ptr)
{
#if defined(VSLICE_MALLOC)
  return g_alloc.free(ptr);
#elif defined(OB_MALLOC)
  return oceanbase::common::ob_free(ptr);
#elif defined(HOOK_MALLOC)
  return free(ptr);
#endif
}

const char* benchmark_name(void) { return "none"; }
