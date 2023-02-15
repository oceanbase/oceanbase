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

#ifndef OCEANBAFast_LIB_CONTAINER_FAST_ARRAY_
#define OCEANBAFast_LIB_CONTAINER_FAST_ARRAY_
#include "lib/allocator/page_arena.h"         // for ArenaAllocator
#include "lib/utility/utility.h"
#include "lib/utility/ob_hang_fatal_error.h"

namespace oceanbase
{
namespace common
{
namespace array
{
template <typename T, int64_t LOCAL_ARRAY_SIZE = 50>
class ObFastArrayIterator;
}


template <typename T, int64_t LOCAL_ARRAY_SIZE = 50>
class ObFastArray final
{
public:
  typedef array::ObFastArrayIterator<T, LOCAL_ARRAY_SIZE> iterator;
  ObFastArray(common::ObIAllocator *allocator = NULL);
  ObFastArray(common::ObIAllocator &allocator);
  inline void set_allocator(common::ObIAllocator *allocator)
  {
    allocator_ = allocator;
  }
  ~ObFastArray() { /* do nothing */}

  int push_back(const T &obj);

  inline int at(int64_t idx, T &obj) const
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(0 > idx || idx >= count_)) {
      ret = OB_ARRAY_OUT_OF_RANGE;
    } else {
      const T* buffer = get_buffer();
      if (OB_ISNULL(buffer)) {
        ret = OB_ERR_UNEXPECTED;
        LIB_LOG(WARN, "buffer is null", K(ret), K(stack_), K(heap_), K(capacity_));
      } else {
        if (OB_FAIL(copy_assign(obj, buffer[idx]))) {
          LIB_LOG(WARN, "failed to copy data", K(ret));
        }
      }
    }
    return ret;
  }
  inline T &at(int64_t idx)     // dangerous
  {
    T* buffer = get_buffer();
    if (OB_UNLIKELY(0 > idx || idx >= count_ || buffer == NULL)) {
      LIB_LOG_RET(ERROR, common::OB_INVALID_ARGUMENT, "invalid argument", K(idx), K_(count), K(buffer));
      common::right_to_die_or_duty_to_live();
    }
    return buffer[idx];
  }
  inline const T &at(int64_t idx) const // dangerous
  {
    const T* buffer = get_buffer();
    if (OB_UNLIKELY(0 > idx || idx >= count_ || buffer == NULL)) {
      LIB_LOG_RET(ERROR, common::OB_INVALID_ARGUMENT, "invalid argument", K(idx), K_(count), K(buffer));
      common::right_to_die_or_duty_to_live();
    }
    return buffer[idx];
  }
  inline int reserve(int64_t capacity)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(capacity > LOCAL_ARRAY_SIZE)) {
      if (OB_ISNULL(allocator_)) {
        ret = OB_NOT_INIT;
        LIB_LOG(WARN, "array is not inited", K(ret));
      } else if (OB_UNLIKELY(heap_ != NULL)) {
        ret = OB_ERR_UNEXPECTED;
        LIB_LOG(WARN, "heap should have been NULL", K(ret), K(capacity_), K(heap_));
      } else {
        heap_ = static_cast<T*>(allocator_->alloc(capacity * sizeof(T)));
        if (OB_ISNULL(heap_)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LIB_LOG(WARN, "allocate memory failed", K(ret));
        } else {
          capacity_ = capacity;
        }
      }
    }
    return ret;
  }
  inline int64_t count() const { return count_; }
  inline const T *head() const { return get_buffer(); }
  inline int64_t capacity() const { return capacity_; }
  inline void reset() { destroy(); }
  inline void reuse();
  inline bool empty() const {return 0 == count_;}
  void pop_back()
  {
    if (count_ > 0) {
      --count_;
    }
  }
  void destroy();
  int64_t to_string(char *buf, int64_t buf_len) const;
private:
  int push_back_when_full(const T &obj);
  inline T* get_buffer() {return capacity_ <= LOCAL_ARRAY_SIZE ? stack_ : heap_;}
  inline const T* get_buffer() const {return capacity_ <= LOCAL_ARRAY_SIZE ? stack_ : heap_;}

private:
  // data members
  T *stack_;
  char stack_buf_[LOCAL_ARRAY_SIZE * sizeof(T)];
  int64_t count_;
  int64_t capacity_;
  T *heap_;
  common::ObIAllocator *allocator_;
};

template<typename T, int64_t LOCAL_ARRAY_SIZE>
ObFastArray<T, LOCAL_ARRAY_SIZE>::ObFastArray(common::ObIAllocator *allocator)
    : stack_(static_cast<T *>(static_cast<void *>(stack_buf_))),
      count_(0),
      capacity_(LOCAL_ARRAY_SIZE),
      heap_(NULL),
      allocator_(allocator)
{
  STATIC_ASSERT(LOCAL_ARRAY_SIZE > 0, "array size invalid");
}

template<typename T, int64_t LOCAL_ARRAY_SIZE>
ObFastArray<T, LOCAL_ARRAY_SIZE>::ObFastArray(common::ObIAllocator &allocator)
    : stack_(static_cast<T *>(static_cast<void *>(stack_buf_))),
      count_(0),
      capacity_(LOCAL_ARRAY_SIZE),
      heap_(NULL),
      allocator_(&allocator)
{
  STATIC_ASSERT(LOCAL_ARRAY_SIZE > 0, "array size invalid");
}

template<typename T, int64_t LOCAL_ARRAY_SIZE>
int ObFastArray<T, LOCAL_ARRAY_SIZE>::push_back(const T &obj)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(count_ == capacity_)) { //full
    ret = push_back_when_full(obj);
  } else {
    T* buffer = get_buffer();
    if (OB_ISNULL(buffer)) {
      ret = OB_ERR_UNEXPECTED;
      LIB_LOG(WARN, "buffer is null", K(ret), K(capacity_), K(buffer));
    } else if (OB_FAIL(construct_assign(buffer[count_], obj))) {
      LIB_LOG(WARN, "failed to construct obj", K(ret));
    } else {
      ++count_;
    }
  }
  return ret;
}

template<typename T, int64_t LOCAL_ARRAY_SIZE>
int ObFastArray<T, LOCAL_ARRAY_SIZE>::push_back_when_full(const T &obj)
{
  int ret = OB_SUCCESS;
  T* source = get_buffer();
  if (OB_ISNULL(source)) {
    ret = OB_ERR_UNEXPECTED;
    LIB_LOG(WARN, "buffer is null", K(ret), K(capacity_), K(source));
  } else if (OB_ISNULL(allocator_)) {
    ret = OB_NOT_INIT;
    LIB_LOG(WARN, "array is not inited", K(ret));
  } else {
    T *new_buffer = static_cast<T*>(allocator_->alloc(2 * capacity_ * sizeof(T)));
    if (OB_ISNULL(new_buffer)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LIB_LOG(WARN, "allocate memory failed", K(ret), K(capacity_), K(count_), K(new_buffer));
    } else if (OB_FAIL(objects_copy(new_buffer, source, capacity_))) {
      LIB_LOG(WARN, "failed to move memory space", K(ret));
    } else if (OB_FAIL(construct_assign(new_buffer[capacity_], obj))) {
      LIB_LOG(WARN, "failed to construct obj", K(ret));
    } else {
      heap_ = new_buffer;
      capacity_ = 2 * capacity_;
      ++count_;
    }
  }
  return ret;
}

template<typename T, int64_t LOCAL_ARRAY_SIZE>
void ObFastArray<T, LOCAL_ARRAY_SIZE>::destroy()
{
  T *buffer = get_buffer();
  if (buffer != NULL) {
    for (int64_t i = 0; i < count_; ++i) {
      buffer[i].~T();
    }
  }
  count_ = 0;
  heap_ = NULL;
  capacity_ = LOCAL_ARRAY_SIZE;
  //do not bother yourself to capacity_
}

template<typename T, int64_t LOCAL_ARRAY_SIZE>
void ObFastArray<T, LOCAL_ARRAY_SIZE>::reuse()
{
  T *buffer = get_buffer();
  if (buffer != NULL) {
    for (int64_t i = 0; i < count_; ++i) {
      buffer[i].~T();
    }
  }
  count_ = 0;
//  heap_ = NULL; reuse not reset heap
  capacity_ = LOCAL_ARRAY_SIZE;
  //do not bother yourself to capacity_
}

template<typename T, int64_t LOCAL_ARRAY_SIZE>
int64_t ObFastArray<T, LOCAL_ARRAY_SIZE>::to_string(char *buf, int64_t buf_len) const
{
  int64_t need_print_count = count();
  int64_t pos = 0;
  const int64_t log_keep_size = (ObLogger::get_logger().is_in_async_logging()
                                 ? OB_ASYNC_LOG_KEEP_SIZE
                                 : OB_LOG_KEEP_SIZE);

  J_ARRAY_START();
  //delete now.If needed, must add satisfy json and run passed observer pretest
  //common::databuff_printf(buf, buf_len, pos, "%ld:", count_);
  for (int64_t index = 0; (index < need_print_count - 1) && (pos < buf_len - 1); ++index) {
    if (pos + log_keep_size >= buf_len - 1) {
      BUF_PRINTF(OB_LOG_ELLIPSIS);
      J_COMMA();
      break;
    }
    BUF_PRINTO(at(index));
    J_COMMA();
  }
  if (need_print_count > 0) {
    BUF_PRINTO(at(need_print_count - 1));
  }
  J_ARRAY_END();
  return pos;
}
} // end namespace common
} // end namespace oceanbase

#endif /* OCEANBAFast_LIB_CONTAINER_FAST_ARRAY_ */
