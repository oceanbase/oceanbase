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

#ifndef OCEANBASE_COMMON_RESERVE_ARENA_H__
#define OCEANBASE_COMMON_RESERVE_ARENA_H__

#include "lib/allocator/page_arena.h"  // ModuleArena

namespace oceanbase
{
namespace common
{
template<int64_t MAX_RESERVE_SIZE = 0>
class ObReserveArenaAllocator : public ObIAllocator
{
public:
  ObReserveArenaAllocator(const lib::ObMemAttr &attr,
                const int64_t page_size = OB_MALLOC_NORMAL_BLOCK_SIZE)
              : pos_(0),
                allocator_(attr, page_size)
  {
    OB_ASSERT(MAX_RESERVE_SIZE >= 0);
  }
  virtual ~ObReserveArenaAllocator()
  {
    reset();
  }
  virtual void *alloc(const int64_t size, const ObMemAttr &attr) override
  {
    UNUSED(attr);
    return alloc(size);
  }
  virtual void* alloc(const int64_t size) override
  {
    void *p = NULL;
    if (size > MAX_RESERVE_SIZE - 1 - pos_) {
      p = allocator_.alloc(size);
    } else {
      p = reinterpret_cast<void *>(buf_ + pos_);
      pos_ += size;
    }
    return p;
  }
  virtual void* realloc(const void *ptr, const int64_t size, const ObMemAttr &attr) override
  {
    // not supportted
    UNUSED(ptr);
    UNUSED(size);
    UNUSED(attr);
    return nullptr;
  }
  virtual void *realloc(void *ptr, const int64_t oldsz, const int64_t newsz) override
  {
    // not supportted
    UNUSED(ptr);
    UNUSED(oldsz);
    UNUSED(newsz);
    return nullptr;
  }
  virtual void free(void *ptr) override
  {
    UNUSED(ptr);
  }
  virtual int64_t total() const override
  {
    return pos_ + allocator_.total();
  }
  virtual int64_t used() const override
  {
    return pos_ + allocator_.used();
  }
  virtual void reset() override
  {
    if (allocator_.used() > 0) {
      allocator_.reset();
    }
    pos_ = 0;
    buf_[0] = '\0';
  }
  virtual void reuse() override
  {
	  if (allocator_.used() > 0) {
      allocator_.reuse();
    }
    pos_ = 0;
    buf_[0] = '\0';
  }

  virtual void set_attr(const ObMemAttr &attr) { UNUSED(attr); }
private:
  char buf_[MAX_RESERVE_SIZE];
  int64_t pos_;
  common::ObArenaAllocator allocator_;
};
} // namespace common
} // namespace oceanbase
#endif /* OCEANBASE_COMMON_HIGH_PERFORMANC_ARENA_H__ */
