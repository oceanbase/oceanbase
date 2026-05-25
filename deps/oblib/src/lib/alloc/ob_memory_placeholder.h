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

#ifndef OB_PLACEHOLDER_MEMORY_H_
#define OB_PLACEHOLDER_MEMORY_H_

#include "lib/container/ob_array.h"
#include "lib/alloc/alloc_struct.h"
#include "lib/allocator/ob_allocator_v2.h"

namespace oceanbase
{
namespace lib
{

/**
 * @brief ObMemoryPlaceholder is a class that consumes from the memory limit quota but does not use physical memory
 *        Not thread safe
 */
class ObMemoryPlaceholder final
{
public:
  explicit ObMemoryPlaceholder(int64_t tenant_id)
  : attr_(tenant_id, "placeholder_mem", ObCtxIds::DEFAULT_CTX_ID),
    allocator_(nullptr)
  {
    int64_t page_size = get_page_size();
    chunk_placeholder_size_ = (CHUNK_ALLOC_SIZE / page_size - 1) * page_size;
  }
  explicit ObMemoryPlaceholder(const ObMemAttr &attr)
  : attr_(attr),
    allocator_(nullptr)
  {
    int64_t page_size = get_page_size();
    chunk_placeholder_size_ = (CHUNK_ALLOC_SIZE / page_size - 1) * page_size;
  }
  ~ObMemoryPlaceholder()
  {
    destroy();
  }
  // Bind an allocator before the first sync. Only chunk-level allocators (ObAllocator /
  // ObParallelAllocator) are accepted
  void set_allocator(common::ObAllocator *allocator) { allocator_ = allocator; }
  int memory_placeholder_sync(const int64_t place_hold_size);
  void destroy();
  int get_placeholder_size() const
  {
    return chunk_placeholder_size_ * chunks_.count();
  }

private:
  static const int64_t CHUNK_ALLOC_SIZE = ACHUNK_SIZE - AOBJECT_META_SIZE;
  ObMemAttr attr_;
  common::ObAllocator *allocator_;
  common::ObArray<void *> chunks_;
  int64_t chunk_placeholder_size_;

  // Allocate memory and mark it as MADV_DONTNEED to not occupy physical memory
  void *allocate_and_mark_dontneed(const uint64_t size);

  // Get system page size, cached in page_size_
  static int64_t get_page_size()
  {
    static int64_t page_size = sysconf(_SC_PAGESIZE);
    return page_size;
  }
};

} // namespace lib
} // namespace oceanbase

#endif // OB_PLACEHOLDER_MEMORY_H_
