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

class ObClassMeta
{
  constexpr static int64_t MAGIC_CODE = 0xbebe23311332bbcc;
public:
  ObClassMeta(void *instance)
    : magic_code_(MAGIC_CODE),
      instance_(instance)
  {}
  bool check_magic_code() { return MAGIC_CODE == magic_code_; }
  void *instance() { return instance_; }
private:
  const int64_t magic_code_;
  void *instance_;
};

template<class T>
class ObFixedClassAllocator
{
public:
  using object_type = T;
public:
  ObFixedClassAllocator(const ObMemAttr &attr, int64_t nway, int32_t slice_cnt = 0)
    : allocator_(sizeof(T) + sizeof(ObClassMeta), attr,
                 0 == slice_cnt ? choose_blk_size(sizeof(T) + sizeof(ObClassMeta)) :
                                  ObBlockSlicer::get_blk_size(sizeof(T) + sizeof(ObClassMeta), slice_cnt))
  {
    allocator_.set_nway(static_cast<int32_t>(nway));
  }
  virtual ~ObFixedClassAllocator() {}

  static int choose_blk_size(int obj_size)
  {
    static const int MIN_SLICE_CNT = 32;
    const int64_t OB_MALLOC_HIGH_BLOCK_SIZE = (1LL << 18) - 128;        //256KB
    int blk_size = OB_MALLOC_NORMAL_BLOCK_SIZE;  // default blk size is 8KB
    if (obj_size <= 0) {
    } else if (MIN_SLICE_CNT <= (OB_MALLOC_NORMAL_BLOCK_SIZE / obj_size)) {
    } else if (MIN_SLICE_CNT <= (OB_MALLOC_MIDDLE_BLOCK_SIZE / obj_size)) {
      blk_size = OB_MALLOC_MIDDLE_BLOCK_SIZE;
    } else if (MIN_SLICE_CNT <= (OB_MALLOC_HIGH_BLOCK_SIZE / obj_size)) {
      blk_size = OB_MALLOC_HIGH_BLOCK_SIZE;
    } else {
      blk_size = OB_MALLOC_BIG_BLOCK_SIZE;
    }
    return blk_size;
  }

  static ObFixedClassAllocator<T> *get(const char* label = "ConcurObjPool")
  {
    static ObFixedClassAllocator<T> instance(SET_USE_500(ObMemAttr(common::OB_SERVER_TENANT_ID, label)),
                                             common::get_cpu_count());
    return &instance;
  }

  void *alloc()
  {
    return allocator_.alloc();
  }

  void free(void *ptr)
  {
    if (OB_LIKELY(NULL != ptr)) {
      allocator_.free(ptr);
      ptr = NULL;
    }
  }

private:
  ObSliceAlloc allocator_;
};

template<typename instance_type, typename object_type>
inline void type_checker(instance_type*, object_type*)
{
  OLD_STATIC_ASSERT((std::is_same<typename instance_type::object_type, object_type>::value),
                    "unmatched type");
}

template<typename T>
inline void free_helper(T *ptr)
{
  auto *meta = reinterpret_cast<common::ObClassMeta*>((char*)ptr - sizeof(common::ObClassMeta));
  abort_unless(meta->check_magic_code());
  ptr->~T();
  ((ObFixedClassAllocator<T>*)meta->instance())->free(meta);
}

#define op_instance_alloc_args(instance, type, args...)  \
  ({ \
    type *ret = NULL; \
    type_checker(instance, ret); \
    if (OB_LIKELY(NULL != instance)) { \
      void *ptr = (instance)->alloc(); \
      if (OB_LIKELY(NULL != ptr)) { \
        auto *meta = new (ptr) common::ObClassMeta(instance); \
        ret = new (meta + 1) type(args); \
      } \
    } \
    ret; \
  })

#define op_alloc_args(type, args...) \
  ({ \
    common::ObFixedClassAllocator<type> *instance = \
      common::ObFixedClassAllocator<type>::get(#type); \
    op_instance_alloc_args(instance, type, args); \
  })

#define op_alloc(type) \
  ({ \
    OLD_STATIC_ASSERT((std::is_default_constructible<type>::value), "type is not default constructible"); \
    op_alloc_args(type); \
  })

#define op_free(ptr) \
  ({ \
    if (OB_LIKELY(NULL != ptr)) { \
      free_helper(ptr); \
    } \
  })

#define op_reclaim_alloc(type) op_alloc(type)
#define op_reclaim_free(ptr) op_free(ptr)

} // end of namespace common
} // end of namespace oceanbase

#endif // OB_LIB_CONCURRENCY_OBJPOOL_H_
