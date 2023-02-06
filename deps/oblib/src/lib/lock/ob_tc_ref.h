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

#ifndef OCEANBASE_LOCK_TC_REF_H_
#define OCEANBASE_LOCK_TC_REF_H_
#include "lib/thread_local/ob_tsi_utils.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/allocator/ob_qsync.h"

namespace oceanbase
{
namespace common
{
class TCRef
{
public:
  enum { REF_LIMIT = INT32_MAX/2, MAX_CPU_NUM = 1024 };
  TCRef(int32_t ref_count_per_thread = 8) : ref_count_per_thread_(ref_count_per_thread)
  {
    total_ref_count_ = MAX_CPU_NUM * ref_count_per_thread_;
    if (OB_ISNULL(ref_ = (int32_t **)ob_malloc_align(64, total_ref_count_ * sizeof(int32_t *), "TCREF"))) {
      COMMON_LOG_RET(ERROR, common::OB_ALLOCATE_MEMORY_FAILED, "memory alloc failed", K(ref_count_per_thread_), K(total_ref_count_));
    } else {
      memset(ref_, 0, total_ref_count_ * sizeof(int32_t *));
    }
  }
  ~TCRef()
  {
    ob_free_align(ref_);
    ref_ = nullptr;
  }
  int32_t born(int32_t* p) { return xref_(p, REF_LIMIT); }
  int32_t end(int32_t* p) { return xref_(p, -REF_LIMIT); }
  void sync() {
    int64_t end = total_ref_count_;
    for(int64_t i = 0; i < end; i++) {
      CriticalGuard(get_qs());
      int32_t** slot = ref_ + i;
      int32_t* p = NULL;
      if (NULL != ATOMIC_LOAD(slot) && OB_NOT_NULL(p = ATOMIC_TAS(slot, NULL))) {
        xref_(p, 1);
      }
    }
    WaitQuiescent(get_qs());
  }
  int32_t sync(int32_t* p) {
    int32_t cnt = 0;
    int64_t end = total_ref_count_;
    for(int64_t i = 0; i < end; i++) {
      int32_t** slot = ref_ + i;
      if (p == ATOMIC_LOAD(slot) && OB_LIKELY(ATOMIC_BCAS(slot, p, NULL))) {
        cnt++;
      }
    }
    return xref_(p, cnt);
  }
  // 不传出slot_id的只会关注自己的槽
  int32_t inc_ref(int32_t* p) {
    int32_t ref_cnt = REF_LIMIT;
    int64_t start = ref_count_per_thread_ * (get_itid() % MAX_CPU_NUM);
    int64_t i = 0;
    for(i = 0; i < ref_count_per_thread_; i++) {
      int32_t** slot = ref_ + start + i;
      if (NULL == ATOMIC_LOAD(slot) && OB_LIKELY(ATOMIC_BCAS(slot, NULL, p))) {
        break;
      }
    }
    if (OB_UNLIKELY(i == ref_count_per_thread_)) {
      ref_cnt = xref_(p, 1);
    }
    return ref_cnt;
  }
  int32_t dec_ref(int32_t* p) {
    int32_t ref_cnt = REF_LIMIT;
    int64_t start = ref_count_per_thread_ * (get_itid() % MAX_CPU_NUM);
    int64_t i = 0;
    for(i = 0; i < ref_count_per_thread_; i++) {
      int32_t** slot = ref_ + start + i;
      if (p == ATOMIC_LOAD(slot) && OB_LIKELY(ATOMIC_BCAS(slot, p, NULL))) {
        break;
      }
    }
    if (OB_UNLIKELY(i == ref_count_per_thread_)) {
      ref_cnt = xref_(p, -1);
    }
    return ref_cnt;
  }
  // 传出slot_id的会尝试所有的SLOT
  int32_t inc_ref(int32_t* p, int64_t &slot_id) {
    int32_t ref_cnt = REF_LIMIT;
    int64_t start = ref_count_per_thread_ * (get_itid() % MAX_CPU_NUM);
    int64_t i = 0;
    slot_id = -1;
    for(i = 0; i < ref_count_per_thread_; i++) {
      int32_t** slot = ref_ + start + i;
      if (NULL == ATOMIC_LOAD(slot) && OB_LIKELY(ATOMIC_BCAS(slot, NULL, p))) {
        slot_id = start + i;
        break;
      }
    }
    if (OB_UNLIKELY(slot_id < 0)) {
      ref_cnt = xref_(p, 1);
    }
    return ref_cnt;
  }
  int32_t dec_ref(int32_t* p, int64_t slot_id) {
    int32_t ref_cnt = REF_LIMIT;
    if (OB_UNLIKELY(slot_id < 0) || OB_UNLIKELY(!ATOMIC_BCAS(ref_ + slot_id, p, NULL))) {
      ref_cnt = xref_(p, -1);
    }
    return ref_cnt;
  }
private:
  int32_t xref_(int32_t* p, int32_t x) { return ATOMIC_AAF(p, x); }
  common::ObQSync& get_qs() {
    static common::ObQSync qsync;
    return qsync;
  }
private:
  int32_t ref_count_per_thread_;
  int32_t total_ref_count_;
  int32_t **ref_;
} CACHE_ALIGNED;

inline TCRef& get_global_tcref()
{
  static TCRef global_tc_ref;
  return global_tc_ref;
}


}; // end namespace common
}; // end namespace oceanbase

#endif /* OCEANBASE_LOCK_TC_REF_H_ */

