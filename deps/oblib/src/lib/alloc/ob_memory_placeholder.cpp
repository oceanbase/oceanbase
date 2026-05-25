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

#define USING_LOG_PREFIX LIB

#include "lib/alloc/ob_memory_placeholder.h"
#include "lib/alloc/alloc_assist.h"
#include <sys/mman.h>
#include <unistd.h>

namespace oceanbase
{
namespace lib
{

void ObMemoryPlaceholder::destroy()
{
  // Free all allocated memory chunks
  for (int64_t i = 0; i < chunks_.count(); ++i) {
    void *chunk = chunks_.at(i);
    if (OB_NOT_NULL(chunk) && OB_NOT_NULL(allocator_)) {
      allocator_->free(chunk);
    }
  }
  // Clear the array
  chunks_.reset();
}

void *ObMemoryPlaceholder::allocate_and_mark_dontneed(const uint64_t size)
{
  void *ptr = nullptr;
  if (OB_ISNULL(allocator_)) {
    LOG_WARN_RET(OB_NOT_INIT, "allocator is not set", K(size));
  } else if (OB_ISNULL(ptr = allocator_->alloc(size, attr_))) {
    // allocate memory failed
  } else {
    const int64_t page_size = get_page_size();
    const int64_t start_addr = reinterpret_cast<int64_t>(ptr);
    const int64_t end_addr = start_addr + size;
    const int64_t aligned_start = (start_addr + page_size - 1) & ~(page_size - 1);
    const int64_t aligned_end = end_addr & ~(page_size - 1);
    if (aligned_start < aligned_end) {
      if (0 != madvise(reinterpret_cast<void *>(aligned_start), aligned_end - aligned_start, MADV_DONTNEED)) {
        LOG_WARN_RET(OB_ERR_SYS, "madvise MADV_DONTNEED failed", K(errno), K(size));
        allocator_->free(ptr);
        ptr = nullptr;
      }
    }
  }
  return ptr;
}

int ObMemoryPlaceholder::memory_placeholder_sync(const int64_t place_hold_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(place_hold_size < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid place_hold_size", K(ret), K(place_hold_size));
  } else {
    int64_t chunk_count = 0;
    if (place_hold_size > 0) {
      chunk_count = (place_hold_size + chunk_placeholder_size_ - 1) / chunk_placeholder_size_;
    }
    const int64_t current_count = chunks_.count();

    if (chunk_count > current_count) {
      const int64_t diff = chunk_count - current_count;
      for (int64_t i = 0; OB_SUCC(ret) && i < diff; ++i) {
        void *chunk = allocate_and_mark_dontneed(CHUNK_ALLOC_SIZE);
        if (OB_ISNULL(chunk)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to allocate and mark dontneed", K(ret));
        } else if (OB_FAIL(chunks_.push_back(chunk))) {
          LOG_ERROR("fail to push chunk to array", K(ret));
          allocator_->free(chunk);
        }
      }
    } else if (chunk_count < current_count) {
      const int64_t diff = current_count - chunk_count;
      for (int64_t i = 0; OB_SUCC(ret) && i < diff; ++i) {
        void *chunk = nullptr;
        if (OB_FAIL(chunks_.pop_back(chunk))) {
          LOG_ERROR("fail to pop chunk from array", K(ret));
        } else if (OB_NOT_NULL(chunk)) {
          allocator_->free(chunk);
        }
      }
    }
  }
  return ret;
}

} // namespace lib
} // namespace oceanbase
