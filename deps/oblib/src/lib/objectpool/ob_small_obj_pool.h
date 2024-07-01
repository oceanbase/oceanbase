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

#ifndef OCEANBASE_LIB_OBJPOOL_OB_SMALL_OBJ_POOL_H__
#define OCEANBASE_LIB_OBJPOOL_OB_SMALL_OBJ_POOL_H__

#include "lib/allocator/ob_small_allocator.h"   // ObSmallAllocator
#include "lib/queue/ob_fixed_queue.h"           // ObFixedQueue
#include "lib/list/ob_atomic_list.h"

namespace oceanbase
{
namespace common
{
template <class T>
class ObSmallObjPool
{
public:
  struct ObjItem
  {
    enum
    {
      SRC_UNKNOWN = 0,  // Fixed allocation of elements, not released
      SRC_FIXED = 1,    // Fixed allocation of elements, not released
      SRC_ALLOC = 2,    // Dynamically allocated elements, released after use
    };

    void *next_;
    int8_t src_type_;
    T obj_;


    ObjItem() : src_type_(SRC_UNKNOWN), obj_()
    {}
    ~ObjItem()
    {
      src_type_ = SRC_UNKNOWN;
    }
  };

  static const int64_t DEFAULT_FIXED_COUNT = 1024;

public:
  ObSmallObjPool();
  virtual ~ObSmallObjPool();

public:
  int alloc(T *&obj);
  int free(T* obj);
  void set_fixed_count(int64_t fixed_count) { fixed_count_ = fixed_count; }
  int64_t get_free_count() const { return free_count_; }
  int64_t get_alloc_count() const { return alloc_count_; }
  int64_t get_fixed_count() const { return fixed_count_; }
  int64_t get_cached_total_count() const { return free_count_; }
  int64_t get_cached_free_count() const { return max(0, fixed_count_ - free_count_); }

public:
  int init(const int64_t fixed_count = DEFAULT_FIXED_COUNT,
           const lib::ObLabel &label = ObModIds::OB_SMALL_OBJ_POOL,
           const uint64_t tenant_id = OB_SERVER_TENANT_ID,
           const int64_t block_size = OB_MALLOC_NORMAL_BLOCK_SIZE);
  void destroy();

private:
  int alloc_obj_(T *&obj);

private:
  bool inited_;
  int64_t fixed_count_;
  int64_t free_count_;
  int64_t alloc_count_;
  ObAtomicList free_list_;
  ObSmallAllocator allocator_;  // Obj allocator

private:
  DISALLOW_COPY_AND_ASSIGN(ObSmallObjPool);
};

template <class T>
ObSmallObjPool<T>::ObSmallObjPool() : inited_(false),
                                      fixed_count_(0),
                                      free_count_(0),
                                      alloc_count_(0),
                                      free_list_(),
                                      allocator_()
{}

template <class T>
ObSmallObjPool<T>::~ObSmallObjPool()
{
  destroy();
}

template <class T>
int ObSmallObjPool<T>::init(const int64_t fixed_count,
    const lib::ObLabel &label,
    const uint64_t tenant_id,
    const int64_t block_size)
{
  int ret = OB_SUCCESS;
  int64_t obj_size = sizeof(ObjItem);
  lib::ObMemAttr attr(tenant_id, label);
  if (OB_UNLIKELY(inited_)) {
    LIB_LOG(ERROR, "small obj pool has been initialized");
    ret = OB_INIT_TWICE;
  } else if (OB_UNLIKELY(fixed_count <= 0)) {
    LIB_LOG(ERROR, "invalid argument", K(fixed_count));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(allocator_.init(obj_size, label, tenant_id, block_size))) {
    LIB_LOG(ERROR, "init small allocator fail", K(ret), K(obj_size), K(label),
        K(tenant_id), K(block_size));
  } else if (OB_FAIL(free_list_.init("SmallObjPool", 0))) {
    LIB_LOG(ERROR, "init free list fail", K(fixed_count));
  } else {
    fixed_count_ = fixed_count;
    free_count_ = 0;
    alloc_count_ = 0;
    inited_ = true;
  }
  return ret;
}

template <class T>
void ObSmallObjPool<T>::destroy()
{
  if (inited_) {
    if (free_count_ != alloc_count_) {
      LIB_LOG(INFO, "ObSmallObjPool object info on destroy",
          K_(alloc_count), K_(free_count), K_(fixed_count));
    }

    inited_ = false;
    ObjItem *obj = NULL;

    while (OB_NOT_NULL(obj = (ObjItem*)free_list_.pop())) {
      obj->~ObjItem();
      allocator_.free(obj);
      obj = NULL;
    }

    fixed_count_ = 0;
    free_count_ = 0;
    alloc_count_ = 0;

    (void)allocator_.destroy();
  }
}

template <class T>
int ObSmallObjPool<T>::alloc(T *&obj)
{
  int ret = OB_SUCCESS;
  ObjItem *obj_item = NULL;

  if (OB_UNLIKELY(! inited_)) {
    LIB_LOG(ERROR, "small obj pool has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_NOT_NULL(obj_item = (ObjItem*)free_list_.pop())) {
    obj = &obj_item->obj_;
    (void)ATOMIC_AAF(&free_count_, -1);
  } else if (OB_FAIL(alloc_obj_(obj))) {
    LIB_LOG(ERROR, "alloc_obj fail", K(ret));
  } else {
    // succ
  }

  return ret;
}

template <class T>
int ObSmallObjPool<T>::free(T* obj)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(! inited_)) {
    LIB_LOG(ERROR, "small obj pool has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(obj)) {
    LIB_LOG(ERROR, "invalid argument", K(obj));
    ret = OB_INVALID_ARGUMENT;
  } else {
    ObjItem *obj_item = CONTAINER_OF(obj, ObjItem, obj_);

    if (ObjItem::SRC_FIXED == obj_item->src_type_) {
      free_list_.push(obj_item);
      (void)ATOMIC_AAF(&free_count_, 1);
    } else if (ObjItem::SRC_ALLOC == obj_item->src_type_) {
      obj_item->~ObjItem();
      allocator_.free(obj_item);
      obj_item = NULL;
      (void)ATOMIC_AAF(&alloc_count_, -1);
    } else {
      LIB_LOG(ERROR, "invalid object", K(obj), K(obj_item), "src_type", obj_item->src_type_);
      ret = OB_INVALID_DATA;
    }
  }
  return ret;
}

template <class T>
int ObSmallObjPool<T>::alloc_obj_(T *&obj)
{
  int ret = OB_SUCCESS;
  ObjItem *obj_item = NULL;
  void *ptr = allocator_.alloc();
  int64_t allocated_count = ATOMIC_AAF(&alloc_count_, 1);

  obj = NULL;

  if (OB_ISNULL(ptr)) {
    LIB_LOG(ERROR, "allocate memory fail", K_(allocator));
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    obj_item = new(ptr) ObjItem();
    if (allocated_count > fixed_count_) {
      obj_item->src_type_ = ObjItem::SRC_ALLOC;
    } else {
      obj_item->src_type_ = ObjItem::SRC_FIXED;
    }

    obj = &obj_item->obj_;
  }

  return ret;
}

} // namespace liboblog
} // namespace oceanbase
#endif /* OCEANBASE_LIB_OBJPOOL_OB_SMALL_OBJ_POOL_H__ */
