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

#define USING_LOG_PREFIX COMMON
#include "lib/other/recursion.h"

#ifdef DETECT_RECURSION
namespace oceanbase
{
using namespace lib;
namespace common
{

thread_local void** RecursionCheckerGuard::tl_func = NULL;
thread_local int    RecursionCheckerGuard::tl_n_func = 0;
RecursionCheckerGuard::RecursionCheckerGuard()
  : func_bak_(tl_func),
    n_func_bak_(tl_n_func)
{
  tl_func = func_;
  tl_n_func = 0;
}

RecursionCheckerGuard::~RecursionCheckerGuard()
{
  tl_func = func_bak_;
  tl_n_func = n_func_bak_;
}

extern "C" void __cyg_profile_func_enter(void *this_func, void *call_func)
{
  if (!RecursionCheckerGuard::tl_func) return;

  #define MIN_START_DEPTH 32
  #define MAX_BACKTRACK_DEPTH 16
  #define MAX_REPEAT_TIMES 4
  int64_t n = RecursionCheckerGuard::tl_n_func++;
  if (n >= MIN_START_DEPTH && n < RecursionCheckerGuard::max_cap) {
    RecursionCheckerGuard::tl_func[n]= this_func;
    int repeats = 0;
    for (int i = n - 1, j = 0; i >= MIN_START_DEPTH && j < MAX_BACKTRACK_DEPTH; i--, j++) {
      if (RecursionCheckerGuard::tl_func[i] == this_func) repeats++;
    }
    if (repeats >= MAX_REPEAT_TIMES) {
      void *addr = this_func;
      oceanbase::common::addrs_to_offsets(&addr, 1);
      LOG_ERROR_RET(oceanbase::OB_ERR_SYS, "RECURSION DETECTED", KP(addr));
    }
  }
  #undef MAX_REPEAT_TIMES
  #undef MAX_BACKTRACK_DEPTH
  #undef MIN_START_DEPTH
}

extern "C" void __cyg_profile_func_exit(void *this_func, void *call_func)
{
  RecursionCheckerGuard::tl_n_func--;
}

} // end namespace common
} // end namespace oceanbase

#endif
