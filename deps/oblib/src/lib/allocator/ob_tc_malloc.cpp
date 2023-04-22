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
#include "lib/list/ob_free_list.h"
#include "lib/utility/ob_tracepoint.h"
#include "lib/utility/utility.h"
#include "lib/hash_func/ob_hash_func.h"
#include "lib/allocator/ob_mem_leak_checker.h"
#include "lib/alloc/ob_malloc_allocator.h"
#include "lib/worker.h"
#include "lib/alloc/malloc_hook.h"

using namespace oceanbase::lib;
using namespace oceanbase::common;

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
void *direct_malloc(int64_t size)
{
  return ::malloc(size);
}
void direct_free(void *p, int64_t size)
{
  UNUSED(size);
  ::free(p);
}
#elif __DIRECT_MALLOC__ == __DM_MMAP
void *direct_malloc(int64_t size)
{
  void *p = NULL;
  if (MAP_FAILED == (p = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1,
                              0))) {
    p = NULL;
  }
  return p;
}
void direct_free(void *p, int64_t size)
{
  if (NULL != p) {
    munmap(p, size);
  }
}
#elif __DIRECT_MALLOC__ == __DM_MMAP_ALIGNED
const static uint64_t MMAP_BLOCK_ALIGN = 1ULL << 21;

inline void *mmap_aligned(uint64_t size, uint64_t align)
{
  void *ret = NULL;
  if (MAP_FAILED == (ret = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1,
                                0))) {
    ret = NULL;
  } else if (is_aligned((uint64_t)ret, align)) {
  } else {
    munmap(ret, size);
    if (MAP_FAILED == (ret = mmap(NULL, size + align, PROT_READ | PROT_WRITE,
                                  MAP_PRIVATE | MAP_ANONYMOUS, -1, 0))) {
      ret = NULL;
    } else {
      uint64_t aligned_addr = up2align((uint64_t)ret, align);
      // Fix Coverity issue:
      // Compute the header/trailer size to avoid using ret after munmap.
      uint64_t header_size = aligned_addr - (uint64_t) ret;
      uint64_t trailer_size = (uint64_t) ret + align - aligned_addr;

      munmap(ret, header_size);
      munmap((void *) (aligned_addr + size), trailer_size);
      ret = (void *) aligned_addr;
    }
  }
  return ret;
}

#endif // __DIRECT_MALLOC__

namespace oceanbase
{
namespace common
{

ObIAllocator *global_default_allocator = NULL;

ObMemLeakChecker &get_mem_leak_checker()
{
  return ObMemLeakChecker::get_instance();
}

void reset_mem_leak_checker_label(const char *str)
{
  get_mem_leak_checker().set_str(str);
  get_mem_leak_checker().reset();
}

void reset_mem_leak_checker_rate(int64_t rate)
{
  get_mem_leak_checker().set_rate(rate);
}

const ObCtxInfo &get_global_ctx_info()
{
  static ObCtxInfo info;
  return info;
}

void  __attribute__((constructor(MALLOC_INIT_PRIORITY))) init_global_memory_pool()
{
  auto& t = EventTable::instance();
  auto& c = get_mem_leak_checker();
  auto& a = AChunkMgr::instance();
  in_hook()= true;
  global_default_allocator = ObMallocAllocator::get_instance();
  in_hook()= false;
  #ifndef OB_USE_ASAN
  abort_unless(OB_SUCCESS == install_ob_signal_handler());
  #endif
  init_proc_map_info();
}

int64_t get_virtual_memory_used(int64_t *resident_size)
{
  static const int ps = sysconf(_SC_PAGESIZE);
  int64_t page_cnt = 0;
  int64_t res_page_cnt = 0;
  FILE *statm = fopen("/proc/self/statm", "r");
  if (OB_NOT_NULL(statm)) {
    fscanf(statm, "%ld %ld", &page_cnt, &res_page_cnt);
    fclose(statm);
    if (resident_size) *resident_size = res_page_cnt * ps;
  }
  return page_cnt * ps;
}

} // end namespace common
} // end namespace oceanbase
