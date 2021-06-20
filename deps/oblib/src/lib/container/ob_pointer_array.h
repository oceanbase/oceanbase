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

#ifndef OCEANBASE_COMMON_POINTER_ARRAY_H_
#define OCEANBASE_COMMON_POINTER_ARRAY_H_
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <pthread.h>
#include <new>
#include <typeinfo>
#include "lib/ob_define.h"
#include "lib/allocator/page_arena.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/allocator/ob_mod_define.h"

namespace oceanbase {
namespace common {
template <typename T, int64_t SIZE>
class ObPointerArray {
public:
  ObPointerArray()
  {
    memset(array_, 0, sizeof(array_));
  };
  ~ObPointerArray()
  {
    for (int64_t i = 0; i < SIZE; i++) {
      if (NULL != array_[i]) {
        array_[i]->~T();
        array_[i] = NULL;
      }
    }
  };

public:
  T* operator[](const int64_t index)
  {
    T* ret = NULL;
    if (0 <= index && SIZE > index) {
      if (NULL == (ret = array_[index])) {
        char* buffer = allocator_.alloc(sizeof(T));
        if (NULL != buffer) {
          array_[index] = new (buffer) T();
          ret = array_[index];
        }
      }
    }
    return ret;
  };

private:
  T* array_[SIZE];
  PageArena<char> allocator_;
};
}  // namespace common
}  // namespace oceanbase

#endif  // OCEANBASE_COMMON_POINTER_ARRAY_H_
