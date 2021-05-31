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

#include "co_utils.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/thread_local/ob_tsi_utils.h"
#include "lib/alloc/malloc_hook.h"
#include "lib/coro/co_thread_fair_sched.h"

namespace oceanbase {
namespace lib {

uint64_t alloc_coid()
{
  static uint64_t next_coid = 0;
  return ATOMIC_FAA(&next_coid, 1);
}

// Max co-routines per thread is 256K, and each tl slot can store 64
// bits which is 64 of coidx. So number of slots should be
// 256K/64=4096.
static const int64_t MAX_CORO_PER_THREAD = 1 << 12;
static __thread uint64_t tl_slots[MAX_CORO_PER_THREAD];
CoIdx alloc_coidx()
{
  uint64_t cidx = UINT64_MAX;
  for (int i = 0; i < MAX_CORO_PER_THREAD; i++) {
    int pos = ffsl(~tl_slots[i]);
    if (pos > 0) {
      tl_slots[i] |= 1UL << (pos - 1);
      cidx = 64 * i + pos - 1;
      break;
    }
  }
  OB_ASSERT(cidx != UINT64_MAX);

  CoIdx co_idx;
  co_idx.tidx_ = common::get_itid() & 0xFFFUL;
  co_idx.cidx_ = cidx & 0xFFFFFFFFFFFFUL;
  return co_idx;
}

void free_coidx(CoIdx coidx)
{
  OB_ASSERT(common::get_itid() == coidx.tidx_);
  const uint64_t pos = coidx.cidx_ % 64;
  const uint64_t slot_idx = coidx.cidx_ / 64;
  tl_slots[slot_idx] &= ~(1UL << (pos - 1));
}

///////////////////////////////////////////////////////////////////////
// CoMainRoutine &__get_default_routine()
// {
//   static class : public CoMainRoutine {
//     void run() override {};
//   } default_routine;
//   return default_routine;
// }

// // This function is called by default when program starts.
// void __attribute__((constructor(CORO_INIT_PRIORITY))) init_coro()
// {
//   __get_default_routine().init();
//   CoSched::active_routine_ = &__get_default_routine();
//   //enable_glibc_hook = true;
// }

// // This function is called by default when program exits.
// void __attribute__((destructor(CORO_INIT_PRIORITY))) deinit_coro()
// {
//   //enable_glibc_hook = false;
//   // __get_default_routine().post_run();
// }
// ////////////////////////////////////////////////////////////////////////

}  // namespace lib
}  // namespace oceanbase
