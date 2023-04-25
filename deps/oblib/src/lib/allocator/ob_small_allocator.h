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

#ifndef OCEANBASE_ALLOCATOR_OB_SMALL_ALLOCATOR_V2_H_
#define OCEANBASE_ALLOCATOR_OB_SMALL_ALLOCATOR_V2_H_

#include "lib/allocator/ob_slice_alloc.h"
namespace oceanbase
{
namespace common
{
class ObSmallAllocator: public ObSliceAlloc
{
public:
  typedef ObBlockAllocMgr BlockAlloc;
  static const int64_t DEFAULT_MIN_OBJ_COUNT_ON_BLOCK = 1;
  ObSmallAllocator() {}
  ~ObSmallAllocator() { destroy(); }

  int init(const int64_t obj_size,
           const lib::ObLabel &label = nullptr,
           const uint64_t tenant_id = OB_SERVER_TENANT_ID,
           const int64_t block_size = OB_MALLOC_NORMAL_BLOCK_SIZE,
           const int64_t min_obj_count_on_block = DEFAULT_MIN_OBJ_COUNT_ON_BLOCK,
           const int64_t limit_num = INT64_MAX) {
    lib::ObMemAttr attr;
    attr.label_ = label;
    attr.tenant_id_ = tenant_id;
    return init(obj_size, attr, block_size, min_obj_count_on_block, limit_num);
  }
  int init(const int64_t obj_size,
           const lib::ObMemAttr &attr,
           const int64_t block_size = OB_MALLOC_NORMAL_BLOCK_SIZE,
           const int64_t min_obj_count_on_block = DEFAULT_MIN_OBJ_COUNT_ON_BLOCK,
           const int64_t limit_num = INT64_MAX) {
    int ret = OB_SUCCESS;
    attr_ = attr;
    UNUSED(min_obj_count_on_block);
    if (limit_num < INT64_MAX) {
      block_alloc_.set_limit(limit_num * obj_size);
    }
    if (obj_size < 0 || obj_size > INT32_MAX || block_size < 0 || block_size > INT32_MAX) {
      ret = OB_INVALID_ARGUMENT;
    } else if (OB_FAIL(ObSliceAlloc::init(static_cast<int32_t>(obj_size),
        static_cast<int32_t>(block_size), block_alloc_, attr_))) {
    } else {
      ObSliceAlloc::set_nway(OB_MAX_CPU_NUM * 16);
    }
    return ret;
  }
  int destroy() { purge_extra_cached_block(0); return OB_SUCCESS; }
private:
  BlockAlloc block_alloc_;
};
}; // end namespace common
}; // end namespace oceanbase

#endif /* OCEANBASE_ALLOCATOR_OB_SMALL_ALLOCATOR_V2_H_ */
