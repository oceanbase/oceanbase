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

#ifndef OCEANBASE_COMMON_ASAN_ALLOCATOR_H_
#define OCEANBASE_COMMON_ASAN_ALLOCATOR_H_

#include "lib/allocator/ob_allocator.h"
#include "lib/alloc/alloc_interface.h"
#include "lib/thread_local/ob_tsi_utils.h"

namespace oceanbase
{
namespace common
{
using oceanbase::lib::ObMemAttr;
static const uint64_t AllocNode_MAGIC_CODE = 0x11AA22BB;
class ObAsanAllocator;

struct AllocNode
{
  AllocNode(ObAsanAllocator *allocator) :
    MAGIC_CODE_(AllocNode_MAGIC_CODE),
    prev_(this),next_(this),
    allocator_(allocator)
  {}
  bool check_magic_code() const
  {
    return MAGIC_CODE_ == AllocNode_MAGIC_CODE;
  }

  const uint32_t MAGIC_CODE_;
  uint32_t size_;
  AllocNode *prev_;
  AllocNode *next_;
  ObAsanAllocator *allocator_;
  char data_[0];
} __attribute__ ((aligned (16)));

class ObAsanAllocator : public ObIAllocator
{
  friend class ObParallelAsanAllocator;
public:
  ObAsanAllocator();
  virtual ~ObAsanAllocator();
  void reset();
  void destroy() { reset(); }
  int init();
  bool is_inited() const { return is_inited_; } 
  virtual void *alloc(const int64_t size) override;
  virtual void *alloc(const int64_t size, const ObMemAttr &) override
  {
    return alloc(size);
  }
  void *alloc_align(const int64_t size, const int64_t align)
  {
    UNUSED(align);
    return alloc(size);
  }
  virtual void *alloc_align(const int64_t size, const int64_t align, const ObMemAttr &)
  {
    return alloc_align(size, align);
  }
  virtual void free(void *ptr) override;

  void set_label(const lib::ObLabel &label) { UNUSED(label); }
  void set_attr(const ObMemAttr &) {}

  int64_t hold() const;
  int64_t total() const override { return hold(); }
  int64_t allocated() const { return hold(); }
  int64_t used() const override;
private:
  void lock() { mutex_.lock(); }
  void unlock() { mutex_.unlock(); }
  bool trylock() { return 0 == mutex_.trylock(); }
  void add_node(AllocNode *node);
  void del_node(AllocNode *node);
private:
  lib::ObMutex mutex_;
  AllocNode using_list_;
  uint64_t alloc_bytes_;
  bool is_inited_;
};

inline ObAsanAllocator::ObAsanAllocator()
  : mutex_(common::ObLatchIds::OB_ALLOCATOR_LOCK),
    using_list_(this),
    alloc_bytes_(0),
    is_inited_(false)
{}

inline ObAsanAllocator::~ObAsanAllocator()
{
  destroy();
}

inline void ObAsanAllocator::reset()
{
  while (using_list_.next_ != &using_list_) {
    void *ptr = using_list_.next_->data_;
    free(ptr);
  }
  alloc_bytes_ = 0;
  is_inited_ = false;
}

inline int ObAsanAllocator::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
  } else {
    is_inited_ = true;
  }
  return ret;
}
inline int64_t ObAsanAllocator::hold() const
{
  return alloc_bytes_;
}

OB_INLINE int64_t ObAsanAllocator::used() const
{
  return alloc_bytes_;
}

inline void *ObAsanAllocator::alloc(const int64_t size)
{
  void *ret = nullptr;
  const int64_t real_size = size + sizeof(AllocNode);
  void *buffer = ::malloc(real_size);
  if (OB_LIKELY(nullptr != buffer)) {
    AllocNode *new_node = new(buffer) AllocNode(this);
    new_node->size_ = static_cast<uint32_t>(real_size);
    ret = (void *)new_node->data_;
    add_node(new_node);
    alloc_bytes_ += real_size;
  }
  return ret;
}

inline void ObAsanAllocator::free(void *ptr)
{
  if (OB_LIKELY(nullptr != ptr)) {
    AllocNode *node = (AllocNode *)((char *)ptr - sizeof(AllocNode));
    abort_unless(node->check_magic_code());
    del_node(node);
    alloc_bytes_ -= node->size_;
    ::free((void *)node);
  }
}

inline void ObAsanAllocator::add_node(AllocNode *node)
{
  node->prev_ = &using_list_;
  node->next_ = using_list_.next_;
  using_list_.next_->prev_ = node;
  using_list_.next_ = node;
}

inline void ObAsanAllocator::del_node(AllocNode *node)
{
  node->prev_->next_ = node->next_;
  node->next_->prev_ = node->prev_;
  node->prev_ = nullptr;
  node->next_ = nullptr;
}


class ObParallelAsanAllocator : public ObIAllocator
{
  // Maximum concurrency
  static const int N = 8;
public:
  ObParallelAsanAllocator(ObAsanAllocator &root_allocator,
                          const int parallel = 4);
  virtual ~ObParallelAsanAllocator();
  void *alloc(const int64_t size, const ObMemAttr &) override
  {
    return alloc(size);
  }
  void *alloc(const int64_t size) override;
  void free(void *ptr) override;
  int64_t hold() const;
  int64_t total() const override { return hold(); }
  int64_t used() const override;
private:
  int init();
private:
  ObAsanAllocator &root_allocator_;
  // buffer of sub_allocators_
  void *buf_;
  // Static allocation takes up too much space, considering that there is less demand for parallel multiple channels, change to dynamic allocation
  ObAsanAllocator *sub_allocators_[N];
  const int sub_cnt_;
  bool is_inited_;
  // for init
  lib::ObMutex mutex_;
};

inline ObParallelAsanAllocator::ObParallelAsanAllocator(ObAsanAllocator &root_allocator,
                                                        const int parallel)
  : root_allocator_(root_allocator),
    buf_(nullptr), sub_cnt_(MIN(parallel, N)),
    is_inited_(false)
{
  for (int i = 0; i < sub_cnt_; i++) {
    sub_allocators_[i] = nullptr;
  }
}

inline ObParallelAsanAllocator::~ObParallelAsanAllocator()
{
  for (int64_t i = 0; i < sub_cnt_; i++) {
    if (sub_allocators_[i] != nullptr) {
      sub_allocators_[i]->~ObAsanAllocator();
      sub_allocators_[i] = nullptr;
    }
  }
  // Release the memory of the multipath itself
  if (buf_ != nullptr) {
    root_allocator_.free(buf_);
    buf_ = nullptr;
  }
}

inline int ObParallelAsanAllocator::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
  } else {
    buf_ = root_allocator_.alloc(sizeof(ObAsanAllocator) * sub_cnt_);
    if (OB_ISNULL(buf_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    }
    for (int i = 0; OB_SUCC(ret) && i < sub_cnt_; i++) {
      sub_allocators_[i] = new ((char*)buf_ + sizeof(ObAsanAllocator) *  i)
        ObAsanAllocator();
    }
    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
  }
  return ret;
}

inline int64_t ObParallelAsanAllocator::hold() const
{
  int64_t hold = 0;
  if (is_inited_) {
    hold += root_allocator_.hold();
    for (int64_t i = 0; i < sub_cnt_; i++) {
      hold += sub_allocators_[i]->hold();
    }
  }
  return hold;
}

inline int64_t ObParallelAsanAllocator::used() const
{
  int64_t used = 0;
  if (is_inited_) {
    used += root_allocator_.used();
    for (int64_t i = 0; i < sub_cnt_; i++) {
      used += sub_allocators_[i]->used();
    }
  }
  return used;
}

inline void *ObParallelAsanAllocator::alloc(const int64_t size)
{
  void *ptr = NULL;
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    lib::ObMutexGuard guard(mutex_);
    if (!is_inited_) {
      ret = init();
    }
  }
  if (OB_SUCC(ret)) {
    bool found = false;
    const int64_t start = common::get_itid();
    for (int64_t i = 0; nullptr == ptr && i < sub_cnt_ && !found; i++) {
      int64_t idx = (start + i) % sub_cnt_;
      if (sub_allocators_[idx]->trylock()) {
        ptr = sub_allocators_[idx]->alloc(size);
        sub_allocators_[idx]->unlock();
        found = true;
        break;
      }
    }
    if (!found && nullptr == ptr) {
      const int64_t idx = start % sub_cnt_;
      sub_allocators_[idx]->lock();
      ptr = sub_allocators_[idx]->alloc(size);
      sub_allocators_[idx]->unlock();
    }
  }
  return ptr;
}

inline void ObParallelAsanAllocator::free(void *ptr)
{
  if (OB_LIKELY(nullptr != ptr)) {
    AllocNode* node = (AllocNode*)((char*)ptr - sizeof(AllocNode));
    abort_unless(node->check_magic_code());
    ObAsanAllocator *allocator = node->allocator_;
    allocator->lock();
    allocator->free(ptr);
    allocator->unlock();
  }
}

}
}

#endif // OCEANBASE_COMMON_ASAN_ALLOCATOR_H_
