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

#include "lib/thread_local/ob_tsi_utils.h"
#include "lib/utility/utility.h"

namespace oceanbase
{
namespace common
{
#ifdef COMPILE_DLL_MODE
TLOCAL(int64_t, Itid::tid_) = INVALID_ITID;
#endif
volatile int64_t max_itid = INVALID_ITID;
volatile int64_t max_used_itid = INVALID_ITID;

// This class defines a class which is used to free thread local itid
// when thread exits.
class FreeItid {
  static pthread_key_t the_key;
public:
  FreeItid() {
    int ret = pthread_key_create(&the_key, [](void *arg) {
      int64_t itid_plus_1 = reinterpret_cast<int64_t>(arg);
      if (itid_plus_1 != INVALID_ITID) {
        free_itid(itid_plus_1-1);
      }
    });
    if (ret != 0) {
      ret_ = OB_ERR_SYS;
    } else {
      ret_ = OB_SUCCESS;
    }
  }
  ~FreeItid()
  {
    if (OB_SUCCESS == ret_) {
      pthread_key_delete(the_key);
    }
  }
  int set_itid(int64_t itid) {
    int ret = ret_;
    if (OB_SUCC(ret)) {
      if (itid != INVALID_ITID) {
        pthread_setspecific(the_key, (void*)(itid+1));
      } else {
        pthread_setspecific(the_key, (void*)(INVALID_ITID));
      }
    }
    return ret;
  }
  int ret_;
};
pthread_key_t FreeItid::the_key;

static bool itid_lock = 0;

uint64_t itid_slots[1024] = {};
int64_t alloc_itid()
{
  int64_t idx = INVALID_ITID;
  int pos = 0;
  int i = 0;
  while (idx < 0 && i != 1024) {
    for (i = 0; i < 1024; i++) {
      uint64_t slot = ATOMIC_LOAD(&itid_slots[i]);
      if ((pos = ffsl(~slot)) > 0) {
        if (ATOMIC_BCAS(&itid_slots[i], slot, slot | (1UL << (pos - 1)))) {
          idx = 64 * i + pos - 1;
          break;
        } else {
          break;
        }
      }
    }
  }
  if (OB_UNLIKELY(idx < 0)) {
    ob_abort();
  }
  while (!ATOMIC_BCAS(&itid_lock, 0, 1)) {
  }
  max_itid = std::max(idx, ATOMIC_LOAD(&max_itid));
  if (idx > ATOMIC_LOAD(&max_used_itid)) {
    ATOMIC_STORE(&max_used_itid, idx);
  }
  itid_lock = 0;

  return idx;
}

void defer_free_itid(int64_t &itid)
{
  static FreeItid free;
  if (itid != INVALID_ITID) {
    int ret = OB_SUCCESS;
    if (OB_FAIL(free.set_itid(itid))) {
      free_itid(itid);
      itid = INVALID_ITID;
    }
  }
}

void free_itid(int64_t itid)
{
  while (true) {
    uint64_t slot = ATOMIC_LOAD(&itid_slots[itid/64]);
    if (ATOMIC_BCAS(&itid_slots[itid/64], slot, slot & ~(1UL << itid%64))) {
      break;
    }
  }
  while (!ATOMIC_BCAS(&itid_lock, 0, 1)) {
  }
  if (itid >= ATOMIC_LOAD(&max_itid)) {
    max_itid = detect_max_itid();
  }
  itid_lock = 0;
}

int64_t detect_max_itid()
{
  int64_t idx = INVALID_ITID;
  for (int i = 1023; i >= 0; i--) {
    uint64_t slot = ATOMIC_LOAD(&itid_slots[i]);
    if (slot != 0) {
      int pos = __builtin_clzl(slot);
      idx = 64 * i + (64 - pos - 1);
      break;
    }
  }
  return idx;
}

} // end namespace common
} // end namespace oceanbase
