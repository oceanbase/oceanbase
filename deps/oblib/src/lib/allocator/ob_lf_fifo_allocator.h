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

#ifndef OCEANBASE_ALLOCATOR_OB_LF_FIFO_ALLOCATOR_V2_H_
#define OCEANBASE_ALLOCATOR_OB_LF_FIFO_ALLOCATOR_V2_H_
#include "lib/allocator/ob_allocator.h"
#include "lib/allocator/ob_vslice_alloc.h"

namespace oceanbase {
namespace common {
class ObLfFIFOAllocator : public ObIAllocator, public ObVSliceAlloc {
public:
  typedef ObBlockAllocMgr BlockAlloc;
  static const int64_t DEFAULT_CACHE_PAGE_COUNT = 64;
  ObLfFIFOAllocator()
  {}
  virtual ~ObLfFIFOAllocator()
  {
    destroy();
  }
  int init(const int64_t page_size, const lib::ObLabel& label, const uint64_t tenant_id = OB_SERVER_TENANT_ID,
      const int64_t cache_page_count = DEFAULT_CACHE_PAGE_COUNT, const int64_t total_limit = INT64_MAX)
  {
    int ret = OB_SUCCESS;
    mattr_.label_ = label;
    mattr_.tenant_id_ = tenant_id;
    block_alloc_.set_limit(total_limit);
    if (OB_FAIL(ObVSliceAlloc::init(page_size, block_alloc_, mattr_))) {
    } else {
      ObVSliceAlloc::set_nway(cache_page_count);
    }
    return ret;
  }
  void destroy()
  {
    ObVSliceAlloc::purge_extra_cached_block(0);
  }

public:
  void* alloc(const int64_t size)
  {
    return ObVSliceAlloc::alloc(size);
  }
  void* alloc(const int64_t size, const ObMemAttr& attr)
  {
    UNUSED(attr);
    return alloc(size);
  }
  void free(void* ptr)
  {
    ObVSliceAlloc::free(ptr);
  }
  int64_t allocated()
  {
    return block_alloc_.hold();
  }
  void set_tenant_id(const uint64_t tenant_id)
  {
    mattr_.tenant_id_ = tenant_id;
  }
  void set_label(const lib::ObLabel& label)
  {
    mattr_.label_ = label;
  }
  void set_total_limit(int64_t total_limit)
  {
    block_alloc_.set_limit(total_limit);
  }
  bool is_fragment(void* ptr)
  {
    return get_block_using_ratio(ptr) < 0.8;
  }

private:
  BlockAlloc block_alloc_;
};
};  // namespace common
};  // end namespace oceanbase

#endif /* OCEANBASE_ALLOCATOR_OB_LF_FIFO_ALLOCATOR_V2_H_ */
