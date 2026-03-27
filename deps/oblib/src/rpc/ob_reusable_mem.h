/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_RPC_OB_REUSABLE_MEM_H_
#define OCEANBASE_RPC_OB_REUSABLE_MEM_H_
#include "lib/oblog/ob_log.h"

namespace oceanbase
{
namespace rpc
{
class ObReusableMem
{
public:
  enum { N_PTR = 4 };
  struct MemPtr
  {
    void set(void* p, int64_t sz) {
      ptr_ = p;
      size_ = sz;
    }
    void* ptr_;
    int64_t size_;
  };
public:
  ObReusableMem(): nptr_(0), mask_(0) {}
  ~ObReusableMem() {}
  void reuse() { mask_ = (1U<<nptr_) - 1; }
  void* alloc(int32_t size)
  {
    void* p = NULL;
    for(int i = 0; NULL == p && i < nptr_; i++) {
      if ((mask_ & (1<<i)) && ptr_[i].size_ == size) {
        p = ptr_[i].ptr_;
        mask_ &= ~(1U<<i);
      }
    }
    return p;
  }
  void add(void* ptr, int64_t size)
  {
    if (nptr_ < N_PTR) {
      ptr_[nptr_++].set(ptr, size);
    }
  }
private:
  int32_t nptr_;
  uint32_t mask_;
  MemPtr ptr_[N_PTR];
};

}; // end namespace rpc
}; // end namespace oceanbase
#endif /* OCEANBASE_RPC_OB_REUSABLE_MEM_H_ */
