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

#ifndef OCEANBASE_SQL_PLAN_CACHE_OB_ID_MANAGER_ALLOCATOR_
#define OCEANBASE_SQL_PLAN_CACHE_OB_ID_MANAGER_ALLOCATOR_

#include "lib/allocator/ob_malloc.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/allocator/ob_small_allocator.h"
#include "lib/alloc/alloc_struct.h"

namespace oceanbase
{
namespace sql
{

// ObIdManagerAllocator is used for small-size memory allocation.
// Use ObSmallAllocator for size less than object_size_, else use ObMalloc.
// ALLOC_MAGIC is used to mark and check the memory allocated.
class ObIdManagerAllocator : public common::ObIAllocator
{
public:
  ObIdManagerAllocator();
  ~ObIdManagerAllocator();

  int init(const int64_t obj_size, const char *label, const uint64_t tenant_id_);

  void *alloc(const int64_t sz) override { return alloc_(sz); }
  void* alloc(const int64_t size, const ObMemAttr &attr) override
  {
    UNUSED(attr);
    return alloc(size);
  }
  void free(void *ptr) override { free_(ptr); }
  void reset();

private:
  void *alloc_(const int64_t sz);
  void free_(void *ptr);

private:
  const static int64_t ALLOC_MAGIC = 0x1A4420844B;
  const static int64_t SMALL_ALLOC_SYMBOL = 0x38;
  const static int64_t M_ALLOC_SYMBOL = 0x7;
  const static int EXTEND_SIZE = sizeof(int64_t) * 2;
  const static int64_t BLOCK_SIZE = 16 * 1024; //16K

  common::ObSmallAllocator small_alloc_;
  common::ObMalloc m_alloc_;
  lib::ObMemAttr mem_attr_;
  int64_t object_size_;
  bool inited_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObIdManagerAllocator);
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_PLAN_CACHE_OB_ID_MANAGER_ALLOCATOR_
