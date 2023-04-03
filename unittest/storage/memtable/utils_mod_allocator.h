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

