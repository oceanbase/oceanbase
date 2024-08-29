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

#ifndef OCEANBASE_MEMTABLE_OB_ROW_HASH_HOLDER_ALLOCATOR_H_
#define OCEANBASE_MEMTABLE_OB_ROW_HASH_HOLDER_ALLOCATOR_H_
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
namespace memtable
{

struct HashHolderAllocator {
#ifdef UNITTEST_DEBUG
  static int64_t ALLOC_TIMES;
  static int64_t FREE_TIMES;
  static bool NO_MEMORY;
#endif
  template <int N>
  static void *alloc(int64_t size, const char (&tag)[N]) {
    void *obj = nullptr;
#ifdef UNITTEST_DEBUG
    obj = NO_MEMORY ? nullptr : ob_malloc(size, tag);
    if (obj) {
      ++ALLOC_TIMES;
      DETECT_LOG(DEBUG, "alloc obj", KP(obj), K(ALLOC_TIMES));
    }
#else
    obj = share::mtl_malloc(size, tag);
#endif
    return obj;
  }
  static void free(void *obj) {
#ifdef UNITTEST_DEBUG
    ob_free(obj);
    ++FREE_TIMES;
    DETECT_LOG(DEBUG, "free obj", KP(obj), K(FREE_TIMES));
#else
    share::mtl_free(obj);
#endif
  }
};

}
}

#endif