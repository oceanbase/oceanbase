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

#ifndef  OCEANBASE_COMMON_ALLOCATOR_H_
#define  OCEANBASE_COMMON_ALLOCATOR_H_

#include "lib/ob_define.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/alloc/alloc_struct.h"
#include "lib/alloc/object_set.h"
#include "lib/alloc/ob_malloc_allocator.h"
#include "lib/alloc/alloc_interface.h"
#ifndef ENABLE_SANITY
#include "lib/lock/ob_latch.h"
#else
#include "lib/alloc/ob_latch_v2.h"
#endif
using namespace oceanbase::lib;
namespace oceanbase
{
namespace lib
{
class __MemoryContext__;
}
namespace common
{

class ObAllocator : public ObIAllocator
{
  friend class lib::__MemoryContext__;
public:
  struct AllocNode
  {
    AllocNode *prev_;
    AllocNode *next_;
    char data_[0];
  };
  ObAllocator(__MemoryContext__ *mem_context, const ObMemAttr &attr);
  virtual ~ObAllocator() { reset(); }
  virtual void *alloc(const int64_t size) override;
  virtual void *alloc(const int64_t size, const ObMemAttr &attr) override;
  virtual void free(void *ptr) override;
  virtual void reset() override;
  int64_t hold() const { return hold_; }
  int64_t total() const override { return hold_; }
  int64_t used() const override { return hold_; }
protected:
  virtual void push(AllocNode *node, const int64_t size)
  {
    node->next_ = &head_;
    node->prev_ = head_.prev_;
    head_.prev_->next_ = node;
    head_.prev_ = node;
    hold_ += size;
  }
  virtual void remove(AllocNode *node, const int64_t size)
  {
    node->prev_->next_ = node->next_;
    node->next_->prev_ = node->prev_;
    hold_ -= size;
  }
  virtual AllocNode *popall()
  {
    AllocNode *ret = nullptr;
    if (head_.next_ != &head_) {
      ret = head_.next_;
      head_.prev_->next_ = nullptr;
      head_.next_ = &head_;
      head_.prev_ = &head_;
    }
    hold_ = 0;
    return ret;
  }
private:
  __MemoryContext__ *mem_context_;
  ObTenantCtxAllocatorGuard ta_;
  ObMemAttr attr_;
  int64_t hold_;
  AllocNode head_;
  ObjectSet *os_;

};

class ObParallelAllocator : public ObAllocator
{
public:
  ObParallelAllocator(__MemoryContext__ *mem_context, const ObMemAttr &attr);
private:
  void push(AllocNode *node, const int64_t size) override;
  void remove(AllocNode *node, const int64_t size) override;
  AllocNode *popall() override;
  ObSimpleLock lock_;
};

}
}

#endif //OCEANBASE_COMMON_ALLOCATOR_H_
