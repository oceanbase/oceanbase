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

#ifndef OCEANBASE_MEMTABLE_OB_SAFE_REF_H_
#define OCEANBASE_MEMTABLE_OB_SAFE_REF_H_
#include "lib/objectpool/ob_concurrency_objpool.h"
#include "lib/queue/ob_link_queue.h"

namespace oceanbase {
namespace storage {
struct SafeRef : public common::ObLink {
  SafeRef() : ref_(0), seq_(0), ptr_(NULL)
  {}
  ~SafeRef()
  {}
  void xref(int64_t x)
  {
    ATOMIC_AAF(&ref_, x);
  }
  int64_t ref()
  {
    return ATOMIC_LOAD(&ref_);
  }
  int64_t seq()
  {
    return ATOMIC_LOAD(&seq_);
  }
  void inc_seq()
  {
    ATOMIC_AAF(&seq_, 1);
  }
  int64_t ref_;
  int64_t seq_;
  void* ptr_;
};

struct SafeRef2 {
  SafeRef2() : sref_(NULL), seq_(0)
  {}
  ~SafeRef2()
  {}
  SafeRef* sref_;
  int64_t seq_;
};

class SafeRefAlloc {
public:
  typedef common::ObLinkQueue FreeList;
  SafeRef* alloc()
  {
    int ret = common::OB_SUCCESS;
    SafeRef* p = NULL;
    while (NULL == p) {
      if (OB_FAIL(free_list_.pop((common::ObLink*&)p))) {
        p = op_alloc(SafeRef);
      }
      if (NULL == p) {
        usleep(10 * 1000);
      }
    }
    return p;
  }
  void free(SafeRef* p)
  {
    (void)free_list_.push(p);
  }

private:
  FreeList free_list_;
};

class ObSafeRefKeeper {
public:
  typedef SafeRefAlloc Alloc;
  ObSafeRefKeeper()
  {}
  ~ObSafeRefKeeper()
  {}
  void reg(SafeRef2& ref, void* ptr)
  {
    SafeRef* p = alloc_.alloc();
    p->ptr_ = ptr;
    ref.sref_ = p;
    ref.seq_ = p->seq_;
  }
  void unreg(SafeRef2& ref)
  {
    SafeRef* p = (SafeRef*)ref.sref_;
    if (NULL != p) {
      p->inc_seq();
      ATOMIC_STORE(&ref.sref_, NULL);
      while (p->ref() > 0) {
        PAUSE();
      }
      ATOMIC_STORE(&p->ptr_, NULL);
      alloc_.free(p);
    }
  }
  void* lock(SafeRef2& ref)
  {
    void* ret = NULL;
    SafeRef* p = (SafeRef*)ref.sref_;
    if (NULL != p) {
      p->xref(1);
      ret = p->ptr_;
      if (NULL == ret) {
        // unexpected
        p->xref(-1);
      } else if (p->seq() != ref.seq_) {
        p->xref(-1);
        ret = NULL;
      }
    }
    return ret;
  }
  void unlock(SafeRef2& ref)
  {
    SafeRef* p = (SafeRef*)ref.sref_;
    if (NULL != p) {
      p->xref(-1);
    }
  }

private:
  Alloc alloc_;
};

inline ObSafeRefKeeper& get_safe_ref_keeper()
{
  static ObSafeRefKeeper ref_keeper;
  return ref_keeper;
}
#define REF_KEEPER get_safe_ref_keeper()
};  // namespace storage
};  // end namespace oceanbase

#endif /* OCEANBASE_MEMTABLE_OB_SAFE_REF_H_ */
