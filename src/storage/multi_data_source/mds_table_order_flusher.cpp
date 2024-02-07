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