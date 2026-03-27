/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef  OCEANBASE_UNITTEST_MEMTABLE_MOD_ALLOCATOR_H_
#define  OCEANBASE_UNITTEST_MEMTABLE_MOD_ALLOCATOR_H_

#include "lib/allocator/page_arena.h"

namespace oceanbase
{
namespace unittest
{
using namespace oceanbase::common;

class ObModAllocator : public DefaultPageAllocator
{
public:
  void *mod_alloc(const int64_t size, const char *label)
  {
    set_label(label);
    return alloc(size);
  }
};

}
}

#endif //OCEANBASE_UNITTEST_MEMTABLE_MOD_ALLOCATOR_H_

