/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "mds_table_order_flusher.h"

namespace oceanbase
{
namespace storage
{
namespace mds
{

// define in cpp file is for rewrire this function in unittest
void *MdsFlusherModulePageAllocator::alloc(const int64_t size, const ObMemAttr &attr) {
  void *ret = nullptr;
  ret = (NULL == allocator_) ? share::mtl_malloc(size, attr) : allocator_->alloc(size, attr);
  return ret;
}

}
}
}