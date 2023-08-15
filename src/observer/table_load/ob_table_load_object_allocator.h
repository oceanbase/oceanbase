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

#pragma once

#include "lib/allocator/ob_small_allocator.h"

namespace oceanbase
{
namespace observer
{

template<class T>
class ObTableLoadObjectAllocator
{
public:
  static const int64_t DEFAULT_MIN_OBJ_COUNT_ON_BLOCK = 1;
  ObTableLoadObjectAllocator() {}
  ~ObTableLoadObjectAllocator() {}

  template<typename... Args>
  T *alloc(Args&&... args);
  void free(T *t);

  int init(const lib::ObLabel &label = nullptr,
           const uint64_t tenant_id = common::OB_SERVER_TENANT_ID,
           const int64_t block_size = common::OB_MALLOC_NORMAL_BLOCK_SIZE,
           const int64_t min_obj_count_on_block = DEFAULT_MIN_OBJ_COUNT_ON_BLOCK,
           const int64_t limit_num = INT64_MAX) {
    return small_allocator_.init(sizeof(T), label, tenant_id, block_size, min_obj_count_on_block, limit_num);
  }

private:
  common::ObSmallAllocator small_allocator_;
};

template<class T>
template<typename... Args>
T *ObTableLoadObjectAllocator<T>::alloc(Args&&... args)
{
  T *t = nullptr;
  void *buf = nullptr;
  if (OB_NOT_NULL(buf = small_allocator_.alloc())) {
    t = new (buf) T(args...);
  }
  return t;
}

template<class T>
void ObTableLoadObjectAllocator<T>::free(T *t)
{
  if (OB_NOT_NULL(t)) {
    t->~T();
    small_allocator_.free(t);
  }
}

}  // namespace observer
}  // namespace oceanbase
