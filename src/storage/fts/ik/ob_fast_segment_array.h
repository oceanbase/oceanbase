/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef _OCEANBASE_STORAGE_FTS_OB_FAST_SEGMENT_ARRAY_H_
#define _OCEANBASE_STORAGE_FTS_OB_FAST_SEGMENT_ARRAY_H_

#define USING_LOG_PREFIX STORAGE_FTS

#include "lib/allocator/ob_allocator.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"

namespace oceanbase
{
namespace storage
{

template <typename T, int64_t block_capacity = 256>
class ObFastSegmentArray
{
  static_assert(block_capacity > 2 && block_capacity <= (1 << 30),
      "blocks capacity must be greater than 2 and less than or equal to (1 << 30)");

public:
  static constexpr int64_t BLOCK_POINTER_ARRAY_CAPACITY = 64;
  static constexpr int64_t BLOCK_CAPACITY_POWER = 64 - __builtin_clzll(static_cast<uint64_t>(block_capacity) - 1);
  static constexpr int64_t REAL_BLOCK_CAPACITY = static_cast<int64_t>(1) << BLOCK_CAPACITY_POWER;
  static constexpr int64_t BLOCK_LOCATOR = REAL_BLOCK_CAPACITY - 1;

  ObFastSegmentArray(ObIAllocator &allocator,
                     int64_t init_block_arr_cap = BLOCK_POINTER_ARRAY_CAPACITY)
      : allocator_(allocator),
        block_arr_(nullptr),
        block_arr_cap_(0),
        block_count_(0),
        size_(0),
        init_block_arr_cap_(init_block_arr_cap > 0 ? init_block_arr_cap : BLOCK_POINTER_ARRAY_CAPACITY)
  {

  }

  ~ObFastSegmentArray() { reset(); }

  int push_back(const T &value)
  {
    int ret = OB_SUCCESS;
    const int64_t block_idx = size_ >> BLOCK_CAPACITY_POWER;
    if (OB_FAIL(ensure_block_(block_idx))) {
    } else {
      const int64_t inner_idx = size_ & BLOCK_LOCATOR;
      block_arr_[block_idx][inner_idx] = value;
      ++size_;
    }
    return ret;
  }

  int alloc(T *&ptr)
  {
    int ret = OB_SUCCESS;
    const int64_t block_idx = size_ >> BLOCK_CAPACITY_POWER;
    if (OB_FAIL(ensure_block_(block_idx))) {
    } else {
      const int64_t inner_idx = size_ & BLOCK_LOCATOR;
      ptr = &block_arr_[block_idx][inner_idx];
      ++size_;
    }
    return ret;
  }

  int free_an_obj()
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(0 == size_)) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      --size_;
    }
    return ret;
  }

  const T &at(const int64_t idx) const
  {
    const int64_t block_idx = idx >> BLOCK_CAPACITY_POWER;
    const int64_t inner_idx = idx & BLOCK_LOCATOR;
    return block_arr_[block_idx][inner_idx];
  }

  T &at(const int64_t idx)
  {
    return const_cast<T &>(static_cast<const ObFastSegmentArray &>(*this).at(idx));
  }

  int64_t count() const { return size_; }
  bool empty() const { return 0 == size_; }

  void reuse() { size_ = 0; }

  void reset()
  {
    for (int64_t i = 0; i < block_count_; ++i) {
      if (nullptr != block_arr_[i]) {
        allocator_.free(block_arr_[i]);
        block_arr_[i] = nullptr;
      }
    }
    if (nullptr != block_arr_) {
      allocator_.free(block_arr_);
    }
    block_arr_ = nullptr;
    block_arr_cap_ = 0;
    block_count_ = 0;
    size_ = 0;
  }

private:
  int ensure_block_(const int64_t block_idx)
  {
    int ret = OB_SUCCESS;
    if (block_idx >= block_count_) {
      if (block_idx >= block_arr_cap_) {
        if (OB_FAIL(expand_block_array_(block_idx + 1))) {
          LOG_WARN("Failed to expand block array", K(ret), K(block_idx));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_UNLIKELY(nullptr == block_arr_[block_idx])) {
          void *buf = allocator_.alloc(REAL_BLOCK_CAPACITY * static_cast<int64_t>(sizeof(T)));
          if (OB_UNLIKELY(nullptr == buf)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("Failed to allocate memory", K(ret), K(block_idx));
          } else {
            block_arr_[block_idx] = static_cast<T *>(buf);
            ++block_count_;
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected error", K(ret));
        }
      }
    }
    return ret;
  }

  int expand_block_array_(const int64_t need_cap)
  {
    int ret = OB_SUCCESS;
    int64_t next_block_arr_cap = block_arr_cap_ > 0 ? block_arr_cap_ : init_block_arr_cap_;
    while (next_block_arr_cap < need_cap) {
      next_block_arr_cap <<= 1;
    }
    const int64_t bytes = next_block_arr_cap * static_cast<int64_t>(sizeof(T *));
    T **next_block_arr = static_cast<T **>(allocator_.alloc(bytes));
    if (nullptr == next_block_arr) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Failed to allocate memory", K(ret), K(next_block_arr_cap));
    } else {
      MEMSET(next_block_arr, 0, bytes);
      if (nullptr != block_arr_) {
        MEMCPY(next_block_arr, block_arr_, block_count_ * static_cast<int64_t>(sizeof(T *)));
        allocator_.free(block_arr_);
        block_arr_ = nullptr;
      }
      block_arr_ = next_block_arr;
      block_arr_cap_ = next_block_arr_cap;
    }
    return ret;
  }

private:
  ObIAllocator &allocator_;
  T **block_arr_;
  int64_t block_arr_cap_;
  int64_t block_count_;
  int64_t size_;
  const int64_t init_block_arr_cap_;

  DISALLOW_COPY_AND_ASSIGN(ObFastSegmentArray);
};

} // namespace storage
} // namespace oceanbase

#endif // _OCEANBASE_STORAGE_FTS_OB_FAST_SEGMENT_ARRAY_H_