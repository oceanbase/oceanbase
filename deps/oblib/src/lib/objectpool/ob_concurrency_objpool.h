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

#ifndef OB_LIB_CONCURRENCY_OBJPOOL_H_
#define OB_LIB_CONCURRENCY_OBJPOOL_H_

#include <typeinfo>
#include <type_traits>
#include "lib/allocator/ob_slice_alloc.h"

namespace oceanbase
{
namespace common
{
template <class T>
    using __is_default_constructible__ = std::is_default_constructible<T>;

template<class T>
class ObFixedClassAllocator
{
public:
  ObFixedClassAllocator(const int obj_size, const ObMemAttr &attr, int blk_size, int64_t nway) : allocator_(obj_size, attr, blk_size)
  {
    allocator_.set_nway(static_cast<int32_t>(nway));
  }
  virtual ~ObFixedClassAllocator() {}

  static int choose_blk_size(int obj_size)
  {
    static const int MIN_SLICE_CNT = 64;
    int blk_size = OB_MALLOC_NORMAL_BLOCK_SIZE;  // default blk size is 8KB
    if (obj_size <= 0) {
    } else if (MIN_SLICE_CNT <= (OB_MALLOC_NORMAL_BLOCK_SIZE / obj_size)) {
    } else if (MIN_SLICE_CNT <= (OB_MALLOC_MIDDLE_BLOCK_SIZE / obj_size)) {
      blk_size = OB_MALLOC_MIDDLE_BLOCK_SIZE;
    } else {
      blk_size = OB_MALLOC_BIG_BLOCK_SIZE;
    }
    return blk_size;
  }

  static ObFixedClassAllocator<T> *get(const char* label = "ConcurObjPool")
  {
    static ObFixedClassAllocator<T> instance(sizeof(T),
                                             SET_USE_500(ObMemAttr(common::OB_SERVER_TENANT_ID, label)),
                                             choose_blk_size(sizeof(T)),
                                             common::get_cpu_count());
    return &instance;
  }

  void *alloc()
  {
    return allocator_.alloc();
  }

  void free(T *ptr)
  {
    if (OB_LIKELY(NULL != ptr)) {
      ptr->~T();
      allocator_.free(ptr);
      ptr = NULL;
    }
  }

private:
  ObSliceAlloc allocator_;
};

#define op_alloc_args(type, args...) \
  ({ \
    type *ret = NULL; \
    common::ObFixedClassAllocator<type> *instance = \
      common::ObFixedClassAllocator<type>::get(#type); \
    if (OB_LIKELY(NULL != instance)) { \
      void *tmp = instance->alloc(); \
      if (OB_LIKELY(NULL != tmp)) { \
        ret = new (tmp) type(args); \
      } \
    } \
    ret; \
  })

#define op_alloc(type) \
  ({ \
    OLD_STATIC_ASSERT((std::is_default_constructible<type>::value), "type is not default constructible"); \
    type *ret = NULL; \
    common::ObFixedClassAllocator<type> *instance = \
      common::ObFixedClassAllocator<type>::get(#type); \
    if (OB_LIKELY(NULL != instance)) { \
      void *tmp = instance->alloc(); \
      if (OB_LIKELY(NULL != tmp)) { \
        ret = new (tmp) type(); \
      } \
    } \
    ret; \
  })

#define op_free(ptr) \
  ({ \
    common::ObFixedClassAllocator<__typeof__(*ptr)> *instance = \
      common::ObFixedClassAllocator<__typeof__(*ptr)>::get(); \
    if (OB_LIKELY(NULL != instance)) { \
      instance->free(ptr); \
    } \
  })

#define op_reclaim_alloc(type) op_alloc(type)
#define op_reclaim_free(ptr) op_free(ptr)

} // end of namespace common
} // end of namespace oceanbase

#endif // OB_LIB_CONCURRENCY_OBJPOOL_H_
