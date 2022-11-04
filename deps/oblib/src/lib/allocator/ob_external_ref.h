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

#ifndef OCEANBASE_ALLOCATOR_OB_EXTERNAL_REF_H_
#define OCEANBASE_ALLOCATOR_OB_EXTERNAL_REF_H_

#include "lib/atomic/ob_atomic.h"
#include <sched.h>

namespace oceanbase
{
namespace common
{
class ObExternalRef
{
public:
  enum { REF_LIMIT = OB_MAX_CPU_NUM * 16 };
  class IERefPtr {
  public:
    IERefPtr() {}
    virtual ~IERefPtr() {}
    virtual void on_quiescent() = 0;
  };
  ObExternalRef() { memset(ref_array_, 0, sizeof(ref_array_)); }
  ~ObExternalRef() {}
  void* acquire(void** paddr) {
    void* addr = NULL;
    while(1) {
      xref((addr = load_ptr(paddr)), 1);
      if (load_ptr(paddr) == addr) {
        break;
      }
      xref(addr, -1);
      sched_yield();
    }
    if (NULL == addr) {
      xref(addr, -1);
    }
    return addr;
  }
  void release(void* addr) {
    if (NULL != addr) {
      xref(addr, -1);
    }
  }
  void retire(IERefPtr* ptr) {
    if (NULL != ptr) {
      wait_quiescent(ptr);
      ptr->on_quiescent();
    }
  }
  void wait_quiescent(void* addr) {
    while(!is_ref_clear(addr)) {
      sched_yield();
    }
  }
private:
  void* load_ptr(void** paddr) { return (void*)(((uint64_t)ATOMIC_LOAD(paddr)) & ~1ULL); }
  void xref(void* addr, int x) { ATOMIC_FAA(locate(addr), x); }
  bool is_ref_clear(void* addr) { return get_ref(addr) == 0; }
  int64_t get_ref(void* addr) { return ATOMIC_LOAD(locate(addr)); }
  int64_t* locate(void* addr) { return ref_array_ + (hash((uint64_t)addr) % REF_LIMIT); }
  static uint64_t hash(uint64_t h) {
    h ^= h >> 33;
    h *= 0xff51afd7ed558ccd;
    h ^= h >> 33;
    h *= 0xc4ceb9fe1a85ec53;
    h ^= h >> 33;
    return h;
  }
private:
  int64_t ref_array_[REF_LIMIT];
};
}; // end namespace common
}; // end namespace oceanbase

#endif /* OCEANBASE_ALLOCATOR_OB_EXTERNAL_REF_H_ */

