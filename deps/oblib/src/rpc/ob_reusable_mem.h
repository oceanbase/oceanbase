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
