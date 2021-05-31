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

#define USING_LOG_PREFIX LIB_ALLOC

#include <sys/mman.h>
#include <malloc.h>
#include "lib/signal/ob_signal_struct.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/allocator/ob_mod_define.h"
#include "lib/utility/utility.h"
#include "lib/hash_func/ob_hash_func.h"
#include "lib/allocator/ob_mem_leak_checker.h"
#include "lib/alloc/ob_malloc_allocator.h"
#include "lib/coro/co_routine.h"
#include "lib/worker.h"

using namespace oceanbase::lib;
using namespace oceanbase::common;

namespace oceanbase {
namespace common {
void print_malloc_stats(bool print_glibc_malloc_stats)
{
  if (print_glibc_malloc_stats) {
    _OB_LOG(INFO, "=== malloc_stats ===");
    malloc_stats();
    _OB_LOG(INFO, "=== main heap info ===");
    struct mallinfo info = mallinfo();
    _OB_LOG(INFO, "mmap_chunks=%d", info.hblks);
    _OB_LOG(INFO, "mmap_bytes=%d", info.hblkhd);
    _OB_LOG(INFO, "sbrk_sys_bytes=%d", info.arena);
    _OB_LOG(INFO, "sbrk_used_chunk_bytes=%d", info.uordblks);
    _OB_LOG(INFO, "sbrk_not_in_use_chunks=%d", info.ordblks);
    _OB_LOG(INFO, "sbrk_not_in_use_chunk_bytes=%d", info.fordblks);
    _OB_LOG(INFO, "sbrk_top_most_releasable_chunk_bytes=%d", info.keepcost);
    _OB_LOG(INFO, "=== detailed malloc_info ===");
    // malloc_info(0, stderr);
  }
}
};  // end namespace common
};  // end namespace oceanbase

void __attribute__((weak)) memory_limit_callback()
{
  LOG_DEBUG("common memory_limit_callback");
}

static bool is_aligned(uint64_t x, uint64_t align)
{
  return 0 == (x & (align - 1));
}

static uint64_t up2align(uint64_t x, uint64_t align)
{
  return (x + (align - 1)) & ~(align - 1);
}

#define __DM_MALLOC 1
#define __DM_MMAP 2
#define __DM_MMAP_ALIGNED 3
#define __DIRECT_MALLOC__ __DM_MMAP_ALIGNED

#if __DIRECT_MALLOC__ == __DM_MALLOC
void* direct_malloc(int64_t size)
{
  return ::malloc(size);
}
void direct_free(void* p, int64_t size)
{
  UNUSED(size);
  ::free(p);
}
#elif __DIRECT_MALLOC__ == __DM_MMAP
void* direct_malloc(int64_t size)
{
  void* p = NULL;
  if (MAP_FAILED == (p = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0))) {
    p = NULL;
  }
  return p;
}
void direct_free(void* p, int64_t size)
{
  if (NULL != p) {
    munmap(p, size);
  }
}
#elif __DIRECT_MALLOC__ == __DM_MMAP_ALIGNED
const static uint64_t MMAP_BLOCK_ALIGN = 1ULL << 21;

inline void* mmap_aligned(uint64_t size, uint64_t align)
{
  void* ret = NULL;
  if (MAP_FAILED == (ret = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0))) {
    ret = NULL;
  } else if (is_aligned((uint64_t)ret, align)) {
  } else {
    munmap(ret, size);
    if (MAP_FAILED == (ret = mmap(NULL, size + align, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0))) {
      ret = NULL;
    } else {
      uint64_t aligned_addr = up2align((uint64_t)ret, align);
      // Compute the header/trailer size to avoid using ret after munmap.
      uint64_t header_size = aligned_addr - (uint64_t)ret;
      uint64_t trailer_size = (uint64_t)ret + align - aligned_addr;

      munmap(ret, header_size);
      munmap((void*)(aligned_addr + size), trailer_size);
      ret = (void*)aligned_addr;
    }
  }
  return ret;
}

void* direct_malloc(int64_t size)
{
  return mmap_aligned(size, MMAP_BLOCK_ALIGN);
}

void direct_free(void* p, int64_t size)
{
  if (NULL != p) {
    munmap(p, size);
  }
}
#endif  // __DIRECT_MALLOC__

namespace oceanbase {
namespace common {

ObIAllocator* global_default_allocator = NULL;
extern void tsi_factory_init();
extern void tsi_factory_destroy();
int ob_init_memory_pool(int64_t block_size)
{
  UNUSED(block_size);
  return OB_SUCCESS;
}

ObMemLeakChecker& get_mem_leak_checker()
{
  return ObMemLeakChecker::get_instance();
}

void reset_mem_leak_checker_label(const char* str)
{
  get_mem_leak_checker().set_str(str);
  get_mem_leak_checker().reset();
}

void reset_mem_leak_checker_rate(int64_t rate)
{
  get_mem_leak_checker().set_rate(rate);
}

const ObCtxInfo& get_global_ctx_info()
{
  static ObCtxInfo info;
  return info;
}

void __attribute__((constructor(MALLOC_INIT_PRIORITY))) init_global_memory_pool()
{
  int ret = OB_SUCCESS;
  // coro local storage construct function
  CoRoutine::co_cb_ = [](CoRoutine& coro) {
    new (coro.get_context().get_local_store()) ObLocalStore();
    new (coro.get_rtctx()) ObRuntimeContext();
    auto cls = reinterpret_cast<common::ObLocalStore*>(coro.get_context().get_local_store());
    coro.get_context().get_stack(cls->stack_addr_, cls->stack_size_);
    return OB_SUCCESS;
  };
  ObMallocAllocator* allocator = ObMallocAllocator::get_instance();
  if (OB_ISNULL(allocator) || OB_FAIL(allocator->init())) {}
  global_default_allocator = ObMallocAllocator::get_instance();
  tsi_factory_init();
  abort_unless(OB_SUCCESS == install_ob_signal_handler());
}

void __attribute__((destructor(MALLOC_INIT_PRIORITY))) deinit_global_memory_pool()
{
  tsi_factory_destroy();
}

}  // end namespace common
}  // end namespace oceanbase
