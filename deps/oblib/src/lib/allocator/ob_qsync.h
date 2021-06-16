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

#ifndef OCEANBASE_LIB_OB_QSYNC_
#define OCEANBASE_LIB_OB_QSYNC_

#include "lib/ob_define.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/thread_local/ob_tsi_utils.h"
#include "lib/oblog/ob_log.h"

namespace oceanbase {
namespace common {
class ObQSync {
public:
  enum { MAX_REF_CNT = 256 };
  struct Ref {
    Ref() : ref_(0)
    {}
    ~Ref()
    {}
    int64_t ref_ CACHE_ALIGNED;
  };
  ObQSync()
  {}
  virtual ~ObQSync()
  {}
  int64_t acquire_ref()
  {
    int64_t idx = get_itid() % MAX_REF_CNT;
    const int64_t new_ref = add_ref(idx, 1);
    if (OB_UNLIKELY(0 >= new_ref)) {
      COMMON_LOG(ERROR, "unexpected ref", K(new_ref), K(idx));
    }
    return idx;
  }
  void release_ref()
  {
    release_ref(get_itid() % MAX_REF_CNT);
  }
  void release_ref(int64_t idx)
  {
    const int64_t new_ref = add_ref(idx, -1);
    if (OB_UNLIKELY(0 > new_ref)) {
      COMMON_LOG(ERROR, "unexpected ref", K(new_ref), K(idx));
    }
  }
  void sync()
  {
    for (int64_t i = 0; i < MAX_REF_CNT; i++) {
      Ref* ref = ref_array_ + i;
      if (NULL != ref) {
        while (ATOMIC_LOAD(&ref->ref_) != 0)
          ;
      }
    }
  }
  bool try_sync()
  {
    bool bool_ret = true;
    for (int64_t i = 0; i < MAX_REF_CNT; i++) {
      Ref* ref = ref_array_ + i;
      if (NULL != ref) {
        if (ATOMIC_LOAD(&ref->ref_) != 0) {
          bool_ret = false;
          break;
        }
      }
    }
    return bool_ret;
  }

private:
  int64_t add_ref(int64_t idx, int64_t x)
  {
    int64_t ret = 0;
    if (idx < MAX_REF_CNT) {
      Ref* ref = ref_array_ + idx;
      if (NULL != ref) {
        ret = ATOMIC_AAF(&ref->ref_, x);
      }
    }
    return ret;
  }

private:
  Ref ref_array_[MAX_REF_CNT];
};

struct QSyncCriticalGuard {
  explicit QSyncCriticalGuard(ObQSync& qsync) : qsync_(qsync), ref_(qsync.acquire_ref())
  {}
  ~QSyncCriticalGuard()
  {
    qsync_.release_ref(ref_);
  }
  ObQSync& qsync_;
  int64_t ref_;
};

#define CriticalGuard(qs) oceanbase::common::QSyncCriticalGuard critical_guard((qs))
#define WaitQuiescent(qs) (qs).sync()

};  // end namespace common
};  // end namespace oceanbase

#endif
