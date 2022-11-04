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

#ifndef OCEANBASE_COMMON_OB_TSI_UTILS_
#define OCEANBASE_COMMON_OB_TSI_UTILS_
#include <errno.h>
#include <unistd.h>
#include <sys/syscall.h>
#include <sys/time.h>
#include <linux/futex.h>
#include <sched.h>
#include "lib/atomic/ob_atomic.h"
#include "lib/ob_define.h"

namespace oceanbase
{
namespace common
{
extern uint64_t itid_slots[1024];
static constexpr int64_t INVALID_ITID = -1;
extern volatile int64_t max_itid;
extern volatile int64_t max_used_itid;

int64_t alloc_itid();
void free_itid(int64_t itid);
void defer_free_itid(int64_t &itid);
int64_t detect_max_itid();

#ifdef COMPILE_DLL_MODE
class Itid 
{
public:
  inline static int64_t get_tid()
  {
    if (OB_UNLIKELY(tid_ < 0)) {
      tid_ = alloc_itid();
      defer_free_itid(tid_);
    }
    return tid_;
  }
private:
  static TLOCAL(int64_t, tid_);
};
inline int64_t get_itid()		
{		
  return Itid::get_tid();		
}
#else
inline int64_t get_itid()
{
  // NOTE: Thread ID
  static TLOCAL(int64_t, tid) = INVALID_ITID;
  if (OB_UNLIKELY(tid < 0)) {
    tid = alloc_itid();
    defer_free_itid(tid);
  }
  return tid;
}
#endif

inline int64_t get_max_itid()
{
  // Make compatible with historical usage of this function.  It
  // returned the max itid plus one, not the max itid before. So we
  // obey the rule and return same value.
  return ATOMIC_LOAD(&max_itid) + 1;
}

inline int64_t get_max_used_itid()
{
  return ATOMIC_LOAD(&max_used_itid) + 1;
}

#ifndef HAVE_SCHED_GETCPU
inline int sched_getcpu(void) { return get_itid() & (OB_MAX_CPU_NUM - 1); }
#endif

inline int64_t icpu_id()
{
  return sched_getcpu();
}

inline int64_t get_max_icpu_id()
{
#ifndef HAVE_SCHED_GETCPU
  return 64;
#else
  return sysconf(_SC_NPROCESSORS_ONLN);
#endif
}

inline int icore_id()
{
  return sched_getcpu();
}

class SimpleItem
{
public:
  SimpleItem(): value_(0) {}
  ~SimpleItem() {}
  inline void reset() {value_ = 0;}
  inline SimpleItem &operator+=(const SimpleItem &item)
  {
    this->value_ += item.value_;
    return *this;
  }
  int64_t value_;
};
} // end namespace common
} // end namespace oceanbase

#endif // OCEANBASE_COMMON_OB_TSI_UTILS_
