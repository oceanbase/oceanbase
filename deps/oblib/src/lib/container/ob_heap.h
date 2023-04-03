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

#ifndef OCEANBASE_OB_HEAP_H
#define OCEANBASE_OB_HEAP_H
#include "lib/container/ob_se_array.h"
namespace oceanbase
{
namespace common
{

template <typename T,
          typename CompareFunctor,
          int64_t LOCAL_ARRAY_SIZE = 50>
class ObBinaryHeapBase
{
public:
  ObBinaryHeapBase(CompareFunctor &cmp, common::ObIAllocator *allocator = NULL);
  virtual ~ObBinaryHeapBase() { /* do nothing */}
  int push(const T &element);
  int pop();
  const T& top() const;
  T& top();
  int top(const T* &element);
  int replace_top(const T &element);
  bool empty() {return array_.empty();}
  int64_t count() const {return array_.count();}
  void reset()
  {
    array_.reset();
    reset_root_cmp_cache();
  }
  const T &at(int64_t index) const {return array_.at(index);}
  T &at(int64_t index) {return array_.at(index);}

  // ObIArray's elements are continuous stored, use the interface to get continuous elements.
  // This interface prevent %array_ be replace with non-continuous array (e.g.: Ob2DArray)
  const ObIArray<T> &get_heap_data() const { return array_; }

  TO_STRING_KV(K_(array));
protected:
  virtual int upheap(int64_t index) = 0;
  virtual int downheap(int64_t index) = 0;
protected:
  virtual void reset_root_cmp_cache() { root_cmp_index_cache_ = INT64_MAX; }
  static inline size_t get_root() { return 0; }
  //no need to use shift operation here. gcc is smart enough, believe me, please.
  static inline size_t get_parent(size_t index) { return (index - 1) / 2; }
  static inline size_t get_left(size_t index) { return 2 * index + 1; }
  static inline size_t get_right(size_t index) { return 2 * index + 2; }
protected:
  ObSEArray<T, LOCAL_ARRAY_SIZE> array_;
  int64_t root_cmp_index_cache_;
  CompareFunctor &cmp_;
};

template<typename T, typename CompareFunctor, int64_t LOCAL_ARRAY_SIZE>
ObBinaryHeapBase<T, CompareFunctor, LOCAL_ARRAY_SIZE>::ObBinaryHeapBase(
    CompareFunctor &cmp, common::ObIAllocator *allocator)
    : array_(OB_MALLOC_NORMAL_BLOCK_SIZE,
        NULL == allocator
        ? ModulePageAllocator(ObModIds::OB_SE_ARRAY)
        : ModulePageAllocator(*allocator, ObModIds::OB_SE_ARRAY)),
    cmp_(cmp)
{
  STATIC_ASSERT(LOCAL_ARRAY_SIZE > 0, "array size invalid");
  STATIC_ASSERT(std::is_trivially_copyable<T>::value, "class is not supported");
  reset_root_cmp_cache();
}

template<typename T, typename CompareFunctor, int64_t LOCAL_ARRAY_SIZE>
int ObBinaryHeapBase<T, CompareFunctor, LOCAL_ARRAY_SIZE>::push(const T &element)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(array_.push_back(element))) {
    LIB_LOG(WARN, "push element to array failed", K(ret));
  } else {
    ret = upheap(array_.count() - 1);
  }
  return ret;
}

template<typename T, typename CompareFunctor, int64_t LOCAL_ARRAY_SIZE>
int ObBinaryHeapBase<T, CompareFunctor, LOCAL_ARRAY_SIZE>::pop()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(array_.empty())) {
    ret = OB_EMPTY_RESULT;
  } else {
    array_.at(get_root()) = array_.at(array_.count() - 1);
    array_.pop_back();
    if (!array_.empty()) {
      ret = downheap(get_root());
    } else {
      reset_root_cmp_cache();
    }
  }
  return ret;
}

template<typename T, typename CompareFunctor, int64_t LOCAL_ARRAY_SIZE>
int ObBinaryHeapBase<T, CompareFunctor, LOCAL_ARRAY_SIZE>::top(const T *&element)
{
  int ret = OB_SUCCESS;
  element = NULL;
  if (OB_UNLIKELY(array_.empty())) {
    ret = OB_EMPTY_RESULT;
    LIB_LOG(WARN, "heap is empty", K(ret));
  } else {
    element = &(array_.at(get_root()));
  }
  return ret;
}

template<typename T, typename CompareFunctor, int64_t LOCAL_ARRAY_SIZE>
const T& ObBinaryHeapBase<T, CompareFunctor, LOCAL_ARRAY_SIZE>::top() const
{
  return array_.at(get_root());
}

template<typename T, typename CompareFunctor, int64_t LOCAL_ARRAY_SIZE>
T& ObBinaryHeapBase<T, CompareFunctor, LOCAL_ARRAY_SIZE>::top()
{
  return array_.at(get_root());
}

template<typename T, typename CompareFunctor, int64_t LOCAL_ARRAY_SIZE>
int ObBinaryHeapBase<T, CompareFunctor, LOCAL_ARRAY_SIZE>::replace_top(const T &element)
{
  int ret = OB_SUCCESS;
  const int64_t cnt = array_.count();
  if (OB_UNLIKELY(0 == cnt)) {
    ret = OB_EMPTY_RESULT;
    LIB_LOG(WARN, "heap is empty", K(ret));
  } else {
    T &current_top = array_.at(get_root());
    current_top = element;
    if (cnt > 1) {
      ret = downheap(get_root());
    }
  }
  return ret;
}

template <typename T,
          typename CompareFunctor,
          int64_t LOCAL_ARRAY_SIZE = 50>
class ObBinaryHeap : public ObBinaryHeapBase<T, CompareFunctor, LOCAL_ARRAY_SIZE>
{
public:
  ObBinaryHeap(CompareFunctor &cmp, common::ObIAllocator *allocator = NULL);
  virtual ~ObBinaryHeap() {}
protected:
  virtual int upheap(int64_t index);
  virtual int downheap(int64_t index);
  virtual void record_index(int64_t index) { (void)index; } // do nothing
private:
  using ObBinaryHeapBase<T, CompareFunctor, LOCAL_ARRAY_SIZE>::array_;
  using ObBinaryHeapBase<T, CompareFunctor, LOCAL_ARRAY_SIZE>::cmp_;
  using ObBinaryHeapBase<T, CompareFunctor, LOCAL_ARRAY_SIZE>::root_cmp_index_cache_;
  using ObBinaryHeapBase<T, CompareFunctor, LOCAL_ARRAY_SIZE>::get_root;
  using ObBinaryHeapBase<T, CompareFunctor, LOCAL_ARRAY_SIZE>::get_parent;
  using ObBinaryHeapBase<T, CompareFunctor, LOCAL_ARRAY_SIZE>::get_left;
  using ObBinaryHeapBase<T, CompareFunctor, LOCAL_ARRAY_SIZE>::get_right;
  using ObBinaryHeapBase<T, CompareFunctor, LOCAL_ARRAY_SIZE>::reset_root_cmp_cache;
};

template<typename T, typename CompareFunctor, int64_t LOCAL_ARRAY_SIZE>
ObBinaryHeap<T, CompareFunctor, LOCAL_ARRAY_SIZE>::ObBinaryHeap(CompareFunctor &cmp, common::ObIAllocator *allocator)
  : ObBinaryHeapBase<T, CompareFunctor, LOCAL_ARRAY_SIZE>(cmp, allocator)
{
}

template<typename T, typename CompareFunctor, int64_t LOCAL_ARRAY_SIZE>
int ObBinaryHeap<T, CompareFunctor, LOCAL_ARRAY_SIZE>::upheap(int64_t index)
{
  int ret = OB_SUCCESS;
  T tmp = array_.at(index);
  while (index > get_root()) {
    const int64_t parent = get_parent(index);
    if (!cmp_(array_.at(parent), tmp)) {
      break;
    }
    array_.at(index) = array_.at(parent);
    record_index(index);
    index = parent;
  }

  array_.at(index) = tmp;
  if (OB_SUCC(cmp_.get_error_code())) {
    record_index(index);
    reset_root_cmp_cache();
  }
  return ret;
}

template<typename T, typename CompareFunctor, int64_t LOCAL_ARRAY_SIZE>
int ObBinaryHeap<T, CompareFunctor, LOCAL_ARRAY_SIZE>::downheap(int64_t index)
{
  int ret = OB_SUCCESS;
  const T tmp = array_.at(index);
  int64_t picked_child = INT64_MAX;
  while (true) {
    const int64_t left_child = get_left(index);
    if (get_left(index) >= array_.count()) {
      break;
    }
    const int64_t right_child = left_child + 1;
    picked_child = left_child;
    if (index == 0 && root_cmp_index_cache_ < array_.count()) {
      picked_child = root_cmp_index_cache_;
    } else if (right_child < array_.count() && cmp_(array_.at(left_child), array_.at(right_child))) {
      picked_child = right_child;
    }
    if (!cmp_(tmp, array_.at(picked_child))) {
      break;
    }
    array_.at(index) = array_.at(picked_child);
    record_index(index);
    index = picked_child;
  }

  array_.at(index) = tmp;
  if (OB_SUCC(cmp_.get_error_code())) {
    if (index == get_root()) {
      // We did not change anything in the tree except for the value
      // of the root node, left and right child did not change, we can
      // cache that `picked_child` is the smallest child
      // so next time we compare against it directly
      root_cmp_index_cache_ = picked_child;
    } else {
      // the tree changed, reset cache
      reset_root_cmp_cache();
    }
    record_index(index);
  }
  return ret;
}

template<typename T>
T &get_element_reference(T &element) { return element; }

template<typename T>
T &get_element_reference(T *element) { return *element; }

template <typename T,
          typename CompareFunctor,
          int64_t std::remove_pointer<T>::type::*member,
          int64_t LOCAL_ARRAY_SIZE = 50>
class ObRemovableHeap : public ObBinaryHeap<T, CompareFunctor, LOCAL_ARRAY_SIZE>
{
  using ObBinaryHeapBase<T, CompareFunctor, LOCAL_ARRAY_SIZE>::array_;
  using ObBinaryHeapBase<T, CompareFunctor, LOCAL_ARRAY_SIZE>::cmp_;
  using ObBinaryHeapBase<T, CompareFunctor, LOCAL_ARRAY_SIZE>::get_parent;
  using ObBinaryHeapBase<T, CompareFunctor, LOCAL_ARRAY_SIZE>::get_root;
  using ObBinaryHeapBase<T, CompareFunctor, LOCAL_ARRAY_SIZE>::reset_root_cmp_cache;
  using ObBinaryHeap<T, CompareFunctor, LOCAL_ARRAY_SIZE>::upheap;
  using ObBinaryHeap<T, CompareFunctor, LOCAL_ARRAY_SIZE>::downheap;
public:
  ObRemovableHeap(CompareFunctor &cmp, common::ObIAllocator *allocator = NULL)
    : ObBinaryHeap<T, CompareFunctor, LOCAL_ARRAY_SIZE>(cmp, allocator) {}
  virtual ~ObRemovableHeap() {}
  virtual void record_index(int64_t index) override
  {
    get_element_reference(array_.at(index)).*member = index;
  }
  int remove(const T &element)
  {
    int64_t index = get_element_reference(element).*member;
    return remove_by_index(index);
  }
  int remove_by_index(int64_t index)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(index < 0 || index >= array_.count())) {
      ret = OB_INVALID_ARGUMENT;
      LIB_LOG(WARN, "invalid index", K(ret), K(index), K(array_.count()));
    } else {
      reset_root_cmp_cache();
      array_.at(index) = array_.at(array_.count() - 1);
      array_.pop_back();
      if (index < array_.count()) {
        if (index == get_root()) {
          ret = downheap(index);
        } else {
          int64_t parent = get_parent(index);
          if (cmp_(array_.at(parent), array_.at(index))) {
            ret = upheap(index);
          } else {
            ret = downheap(index);
          }
        }
      }
    }
    return ret;
  }
};

} // end namespace common
} // end namespace oceanbase

#endif /* OCEANBASE_OB_HEAP_H */
