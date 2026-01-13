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

#ifndef OCEANBASE_SQL_OB_SQL_ARRAY_H_
#define OCEANBASE_SQL_OB_SQL_ARRAY_H_
#include <typeinfo>
#include "lib/container/ob_iarray.h"
#include "lib/utility/ob_template_utils.h"

namespace oceanbase
{
namespace sql
{
template <typename T, bool construct_with_allocator>
class ObSqlArrayIterator;

// strategy class to dispatch to different implementations which is dependent on whether T is a POD type
template <typename T, bool is_pod_type>
struct ObClassOp
{
  static int assign(const T &src, T &dst);
  static int assign_and_destruct(T &src, T &dst);
  static int construct_and_assign(const T &src, T &addr);
  static int array_remove(T* arr, int64_t &arr_size, int64_t idx);
  static T* default_construct(T &addr);
  static void default_destruct(T &addr);
  static void array_default_construct(T *addr, int64_t arr_size);
  static int array_assign(const T *src_arr, T *dst_arr, int64_t arr_size);
  static int array_expand(const T *orig_arr, T *new_arr, int64_t orig_size);
  static void array_construct(T* arr, int64_t arr_size);
};

// POD types
template <typename T>
struct ObClassOp<T, true>
{
  OB_INLINE static int assign(const T &src, T &dst)
  {
    MEMCPY(&dst, &src, sizeof(T));
    return OB_SUCCESS;
  }
  OB_INLINE static int assign_and_destruct(T &src, T &dst)
  {
    MEMCPY(&dst, &src, sizeof(T));
    return OB_SUCCESS;
  }
  OB_INLINE static int construct_and_assign(const T &src, T &addr)
  {
    MEMCPY(&addr, &src, sizeof(T));
    return OB_SUCCESS;
  }
  OB_INLINE static int array_remove(T* arr, int64_t &arr_size, int64_t idx)
  {
    memmove(&arr[idx], &arr[idx + 1], ((--arr_size) - idx) * sizeof(T));
    return OB_SUCCESS;
  }
  OB_INLINE static T* default_construct(T &addr)
  {
    return &addr;
  }
  OB_INLINE static void default_destruct(T &addr)
  {
    UNUSED(addr);
  }
  OB_INLINE static void array_default_construct(T *addr, int64_t arr_size)
  {
    MEMSET(addr, 0, sizeof(T) * arr_size);
  }
  OB_INLINE static int array_assign(const T *src_arr, T *dst_arr, int64_t arr_size)
  {
    MEMCPY(dst_arr, src_arr, sizeof(T)*arr_size);
    return OB_SUCCESS;
  }
  OB_INLINE static int array_expand(const T *orig_arr, T *new_arr, int64_t orig_size)
  {
    MEMCPY(new_arr, orig_arr, sizeof(T) * orig_size);
    return OB_SUCCESS;
  }
  OB_INLINE static void array_construct(T* arr, int64_t arr_size)
  {
    MEMSET(arr, 0, sizeof(T) * arr_size);
  }
};

// non-POD types so we need to deal with construct/destruct
template <typename T>
struct ObClassOp<T, false>
{
  OB_INLINE static int assign(const T &src, T &dst)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(copy_assign(dst, src))) {
      SQL_LOG(WARN, "failed to copy data", K(ret));
    }
    return ret;
  }
  OB_INLINE static int assign_and_destruct(T &src, T &dst)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(copy_assign(dst, src))) {
      SQL_LOG(WARN, "failed to copy data", K(ret));
    }
    src.~T();
    return ret;
  }
  template <typename U = T, typename std::enable_if<std::is_default_constructible<U>::value, int>::type = 0>
  OB_INLINE static int construct_and_assign(const T &src, T &addr)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(construct_assign(addr, src))) {
      SQL_LOG(WARN, "failed to copy data", K(ret));
    }
    return ret;
  }
  OB_INLINE static int array_remove(T* arr, int64_t &arr_size, int64_t idx)
  {
    int ret = OB_SUCCESS;
    for (int64_t i = idx; OB_SUCC(ret) && i < arr_size - 1; ++i) {
        if (OB_FAIL(copy_assign(arr[i], arr[i + 1]))) {
          SQL_LOG(WARN, "failed to copy data", K(ret));
        }
    }
    if (OB_SUCC(ret)) {
      arr[--arr_size].~T();
    }
    return ret;
  }
  template <typename U = T, typename std::enable_if<std::is_default_constructible<U>::value, int>::type = 0>
  OB_INLINE static T* default_construct(T &addr)
  {
    return new(&addr) T();
  }
  OB_INLINE static void default_destruct(T &addr)
  {
    addr.~T();
  }
  template <typename U = T, typename std::enable_if<std::is_default_constructible<U>::value, int>::type = 0>
  OB_INLINE static void array_default_construct(T *addr, int64_t arr_size)
  {
    for (int64_t i = 0; i < arr_size; ++i) {
      new(addr + i) T();
    }
  }
  template <typename U = T, typename std::enable_if<std::is_default_constructible<U>::value, int>::type = 0>
  OB_INLINE static int array_assign(const T *src_arr, T *dst_arr, int64_t arr_size)
  {
    int ret = OB_SUCCESS;
    for (int64_t i = 0; OB_SUCC(ret) && i < arr_size; ++i) {
      if (OB_FAIL(construct_assign(dst_arr[i], src_arr[i]))) {
        SQL_LOG(WARN, "failed to copy data", K(ret), K(i));
      }
    }
    return ret;
  }
  template <typename U = T, typename std::enable_if<std::is_default_constructible<U>::value, int>::type = 0>
  OB_INLINE static int array_expand(const T *orig_arr, T *new_arr, int64_t orig_size)
  {
    int ret = OB_SUCCESS;
    int64_t fail_pos = -1;
    for (int64_t i = 0; OB_SUCC(ret) && i < orig_size; i++) {
      if (OB_FAIL(construct_assign(new_arr[i], orig_arr[i]))) {
        SQL_LOG(WARN, "failed to copy new_data", K(ret), K(i));
        fail_pos = i;
      }
    }
    if (!std::is_trivially_destructible<T>::value) {
      // if construct assign fails, objects that have been copied should be destructed
      if (OB_FAIL(ret)) {
        for (int64_t i = 0; i < fail_pos; i++) {
          new_arr[i].~T();
        }
      } else {
        // if construct assign succeeds, old objects in data_ should be destructed
        for (int64_t i = 0; i < orig_size; i++) {
          orig_arr[i].~T();
        }
      }
    }
    return ret;
  }
  template <typename U = T, typename std::enable_if<std::is_default_constructible<U>::value, int>::type = 0>
  OB_INLINE static void array_construct(T* arr, int64_t arr_size)
  {
    new (arr) T[arr_size];
  }
};

// ObSqlArrayImpl is a lite array for SQL compile,
// to guarantee performance, it should be used in this way
// 1. always construct with allocator
// 2. disallow copy and assign
// 3. T should be a trivial type, otherwise the array will disable dynamic extending
template <typename T, bool construct_with_allocator = false>
class ObSqlArrayImpl : public ObIArray<T>
{
public:
  static_assert(std::is_default_constructible<T>::value ||
      (std::is_constructible<T, common::ObIAllocator &>::value && construct_with_allocator),
      "When T must be constructed with allocator, "
      "try to explicitly specify construct_with_allocator. "
      "Notice that dynamic extending array size is forbidden in this situation, "
      "you can treat the array as a fixed array");
  using ObIArray<T>::count;
  using ObIArray<T>::at;
  using MyOp = ObClassOp<T, std::is_trivial<T>::value>;
  typedef ObSqlArrayIterator<T, construct_with_allocator> iterator;
  explicit ObSqlArrayImpl(common::ObIAllocator &allocator);
  virtual ~ObSqlArrayImpl();

  // deep copy
  virtual int assign(const ObIArray<T> &other) override
  {
    return assign_(other);
  }
  template <typename U = T, typename std::enable_if<std::is_default_constructible<U>::value, int>::type = 0>
  int assign_(const ObIArray<T> &other);
  template <typename U = T, typename std::enable_if<!std::is_default_constructible<U>::value, int>::type = 0>
  int assign_(const ObIArray<T> &other);

  virtual int push_back(const T &obj) override
  {
    return push_back_(obj);
  }
  template <typename U = T, typename std::enable_if<std::is_default_constructible<U>::value, int>::type = 0>
  int push_back_(const T &obj);
  template <typename U = T, typename std::enable_if<!std::is_default_constructible<U>::value, int>::type = 0>
  int push_back_(const T &obj);
  virtual void pop_back() override;
  virtual int pop_back(T &obj) override;
  virtual int remove(int64_t idx) override;

  virtual inline int at(int64_t idx, T &obj) const override
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(0 > idx || idx >= count_)) {
      ret = OB_ARRAY_OUT_OF_RANGE;
    } else {
      ret = MyOp::assign(data_[idx], obj);
    }
    return ret;
  }
  void extra_access_check(void ) const override {}

  inline T &operator[](int64_t idx)  // dangerous
  {
    if (OB_UNLIKELY(0 > idx || idx >= count_)) {
      SQL_LOG_RET(ERROR, OB_ARRAY_OUT_OF_RANGE, "idx out of range", K(idx), K(count_));
    }
    return data_[idx];
  }
  inline const T &operator[](int64_t idx) const  // dangerous
  {
    if (OB_UNLIKELY(0 > idx || idx >= count_)) {
      SQL_LOG_RET(ERROR, OB_ARRAY_OUT_OF_RANGE, "idx out of range", K(idx), K(count_));
    }
    return data_[idx];
  }
  virtual T *alloc_place_holder() override
  {
    return alloc_place_holder_();
  }
  template <typename U = T, typename std::enable_if<std::is_default_constructible<U>::value, int>::type = 0>
  T *alloc_place_holder_();
  template <typename U = T, typename std::enable_if<!std::is_default_constructible<U>::value, int>::type = 0>
  T *alloc_place_holder_();

  virtual void reuse() override
  {
    if (is_destructor_safe()) {
    } else {
      for (int64_t i = 0; i < count_; i++) {
        data_[i].~T();
      }
    }
    count_ = 0;
  }
  virtual inline void reset() override { destroy(); }
  virtual void destroy() override;
  virtual inline int reserve(int64_t capacity) override
  {
    return reserve_(capacity, true);
  }
  template <typename U = T, typename std::enable_if<std::is_default_constructible<U>::value, int>::type = 0>
  int reserve_(int64_t capacity, bool align_to_block_size)
  {
    int ret = OB_SUCCESS;
    if (capacity > capacity_) {
      // memory size should be integer times of block_size
      int64_t new_size = capacity * sizeof(T);
      int64_t plus = new_size % BLOCK_SIZE;
      new_size += (0 == plus || !align_to_block_size) ? 0 : (BLOCK_SIZE - plus);
      T *new_data = reinterpret_cast<T*>(internal_malloc_(new_size));
      if (OB_NOT_NULL(new_data)) {
        ret = MyOp::array_expand(data_, new_data, count_);
        if (OB_SUCC(ret)) {
          if (NULL != data_) {
            internal_free_(data_);
          }
          data_ = new_data;
          capacity_ = new_size / sizeof(T);
        } else {
          // construct_assign fails, free new allocated memory, keep data_'s memory not changed
          // the result is that reserve is not called at all
          internal_free_(new_data);
        }
      } else {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SQL_LOG(ERROR, "no memory", K(ret), K(new_size), K(count_), K(capacity_));
      }
    }
    return ret;
  }
  template <typename U = T, typename std::enable_if<!std::is_default_constructible<U>::value, int>::type = 0>
  int reserve_(int64_t capacity, bool align_to_block_size)
  {
    int ret = OB_SUCCESS;
    UNUSED(align_to_block_size);
    if (capacity <= capacity_) {
      // do nothing
    } else if (OB_UNLIKELY(count_ > 0)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "array should be empty", K(ret), K(count_), K(capacity_));
    } else {
      // memory size should be integer times of block_size
      int64_t new_size = capacity * sizeof(T);
      T *new_data = reinterpret_cast<T*>(internal_malloc_(new_size));
      if (OB_NOT_NULL(new_data)) {
        if (NULL != data_) {
          internal_free_(data_);
        }
        data_ = new_data;
        capacity_ = capacity;
      } else {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SQL_LOG(ERROR, "no memory", K(ret), K(new_size), K(count_), K(capacity_));
      }
    }
    return ret;
  }
  virtual int prepare_allocate(int64_t capacity) override
  {
    return prepare_allocate_(capacity);
  }
  template <typename U = T, typename std::enable_if<std::is_default_constructible<U>::value, int>::type = 0>
  int prepare_allocate_(int64_t capacity);
  template <typename U = T, typename std::enable_if<!std::is_default_constructible<U>::value, int>::type = 0>
  int prepare_allocate_(int64_t capacity);
  template<typename ... Args>
  int prepare_allocate(int64_t capacity, Args && ... args);
  int64_t to_string(char *buffer, int64_t length) const;
  inline int64_t get_data_size() const
  {
    int64_t data_size = capacity_ * sizeof(T);
    int64_t plus = data_size % BLOCK_SIZE;
    data_size += (0 == plus) ? 0 : (BLOCK_SIZE - plus);
    return data_size;
  }
  inline int64_t get_capacity() const
  {
    return capacity_;
  }
  NEED_SERIALIZE_AND_DESERIALIZE;

  static uint32_t local_data_offset_bits() { return offsetof(ObSqlArrayImpl, data_) * 8; }
  static uint32_t count_offset_bits() { return offsetof(ObSqlArrayImpl, count_) * 8; }
  static uint32_t capacity_offset_bits() { return offsetof(ObSqlArrayImpl, capacity_) * 8; }
  static uint32_t allocator_offset_bits() { return offsetof(ObSqlArrayImpl, block_allocator_) * 8; }
public:
  iterator begin();
  iterator end();
private:
  // types and constants
  static constexpr int64_t DEFAULT_CAPACITY = 4;
  static constexpr int64_t BLOCK_SIZE = std::min(static_cast<int64_t>(DEFAULT_CAPACITY * sizeof(T)), OB_MALLOC_NORMAL_BLOCK_SIZE);
  static constexpr int64_t MAX_PRINT_COUNT = 32;
private:
  // if the object has construct but doesn't have virtual function, it cannot memcpy
  // but it doesn't need to call destructor
  inline bool is_memcpy_safe() const
  {
    // no need to call constructor
    return std::is_trivial<T>::value;
  }
  inline bool is_destructor_safe() const
  {
    // no need to call destructor
    return std::is_trivially_destructible<T>::value;
  }
  void *internal_malloc_(int64_t size)
  {
    return block_allocator_.alloc(size);
  }

  void internal_free_(void *p) {
    return block_allocator_.free(p);
  }

protected:
  using ObIArray<T>::data_;
  using ObIArray<T>::count_;
private:
  int64_t capacity_;
  common::ObIAllocator &block_allocator_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObSqlArrayImpl);
};

template<typename T, bool construct_with_allocator>
int ObSqlArrayImpl<T, construct_with_allocator>::serialize(
    char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, count()))) {
    SQL_LOG(WARN, "failed to encode ob array count", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < count(); i ++) {
    if (OB_SUCCESS != (ret = serialization::encode(buf, buf_len, pos, at(i)))) {
      SQL_LOG(WARN, "failed to encode item", K(i), K(ret));
    }
  }
  return ret;
}

template<typename T, bool construct_with_allocator>
int ObSqlArrayImpl<T, construct_with_allocator>::deserialize(
    const char *buf, int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t count = 0;
  reset();
  if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &count))) {
    SQL_LOG(WARN, "failed to decode ob array count", K(ret));
  } else if (OB_SUCCESS != (ret = prepare_allocate(count))) {
    SQL_LOG(WARN, "failed to allocate space", K(ret), K(count));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < count; i ++) {
    T &item = at(i);
    if (OB_SUCCESS != (ret = serialization::decode(buf, data_len, pos, item))) {
      SQL_LOG(WARN, "failed to decode array item", K(ret), K(i), K(count));
    }
  }
  return ret;
}

template<typename T, bool construct_with_allocator>
int64_t ObSqlArrayImpl<T, construct_with_allocator>::get_serialize_size()
const
{
  int64_t size = 0;
  size += serialization::encoded_length_vi64(count());
  for (int64_t i = 0; i < count(); i ++) {
    size += serialization::encoded_length(at(i));
  }
  return size;
}

template<typename T, bool construct_with_allocator>
ObSqlArrayImpl<T, construct_with_allocator>::ObSqlArrayImpl(
    common::ObIAllocator &allocator)
    : ObIArray<T>(),
      capacity_(0),
      block_allocator_(allocator)
{
}

template<typename T, bool construct_with_allocator>
ObSqlArrayImpl<T, construct_with_allocator>::~ObSqlArrayImpl()
{
  destroy();
}

template<typename T, bool construct_with_allocator>
template <typename U, typename std::enable_if<std::is_default_constructible<U>::value, int>::type>
int ObSqlArrayImpl<T, construct_with_allocator>::push_back_(
    const T &obj)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(count_ == capacity_)) {
    int64_t new_capacity = std::max(2 * capacity_, DEFAULT_CAPACITY);
    ret = reserve(new_capacity);
  }
  if (OB_SUCC(ret) && OB_SUCC(MyOp::construct_and_assign(obj, data_[count_]))) {
    count_++;
  }
  return ret;
}

template<typename T, bool construct_with_allocator>
template <typename U, typename std::enable_if<!std::is_default_constructible<U>::value, int>::type>
int ObSqlArrayImpl<T, construct_with_allocator>::push_back_(
    const T &obj)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(count_ >= capacity_)) {
    ret = OB_SIZE_OVERFLOW;
    SQL_LOG(WARN, "array size overflow", K(ret), K(count_), K(capacity_));
  } else {
    new(data_ + count_) T(block_allocator_);
    if (OB_FAIL(data_[count_].assign(obj))) {
      data_[count_].~T();
    } else {
      count_++;
    }
  }
  return ret;
}

template<typename T, bool construct_with_allocator>
void ObSqlArrayImpl<T, construct_with_allocator>::pop_back()
{
  if (OB_UNLIKELY(count_ <= 0)) {
  } else {
    MyOp::default_destruct(data_[--count_]);
  }
}

template<typename T, bool construct_with_allocator>
int ObSqlArrayImpl<T, construct_with_allocator>::pop_back(
    T &obj)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(count_ <= 0)) {
    ret = OB_ENTRY_NOT_EXIST;
  } else {
    ret = MyOp::assign_and_destruct(data_[--count_], obj);
  }
  return ret;
}

template<typename T, bool construct_with_allocator>
int ObSqlArrayImpl<T, construct_with_allocator>::remove(
    int64_t idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 > idx || idx >= count_)) {
    ret = OB_ARRAY_OUT_OF_RANGE;
  } else {
    ret = MyOp::array_remove(data_, count_, idx);
  }
  return ret;
}

template<typename T, bool construct_with_allocator>
template <typename U, typename std::enable_if<std::is_default_constructible<U>::value, int>::type>
inline T *ObSqlArrayImpl<T, construct_with_allocator>::alloc_place_holder_()
{
  int ret = OB_SUCCESS;
  T *ptr = NULL;
  if (OB_UNLIKELY(count_ == capacity_)) {
    int64_t new_capacity = std::max(2 * capacity_, DEFAULT_CAPACITY);
    ret = reserve(new_capacity);
  }
  if (OB_SUCC(ret)) {
    ptr = MyOp::default_construct(data_[count_++]);
  }
  return ptr;
}

template<typename T, bool construct_with_allocator>
template <typename U, typename std::enable_if<!std::is_default_constructible<U>::value, int>::type>
inline T *ObSqlArrayImpl<T, construct_with_allocator>::alloc_place_holder_()
{
  int ret = OB_SUCCESS;
  T *ptr = NULL;
  if (OB_FAIL(reserve(count_ + 1))) {
    SQL_LOG(WARN, "failed to reserve space", K(ret), K(count_), K(capacity_));
  } else {
    ptr = new(data_ + count_++) T(block_allocator_);
  }
  return ptr;
}

template<typename T, bool construct_with_allocator>
void ObSqlArrayImpl<T, construct_with_allocator>::destroy()
{
  if (is_destructor_safe()) {
  } else {
    for (int64_t i = 0; i < count_; i++) {
      data_[i].~T();
    }
  }

  if (data_ != NULL) {
    internal_free_(data_);
    data_ = NULL;
  }
  count_ = 0;
  capacity_ = 0;
}

template<typename T, bool construct_with_allocator>
template <typename U, typename std::enable_if<std::is_default_constructible<U>::value, int>::type>
inline int
ObSqlArrayImpl<T, construct_with_allocator>::prepare_allocate_(
    int64_t capacity)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(capacity_ < capacity)) {
    ret = reserve_(capacity, false);
  }
  if (OB_SUCC(ret)) {
    if (capacity > count_) {
      MyOp::array_default_construct(data_ + count_, capacity - count_);
      count_ = capacity;
    }
  }
  return ret;
}

template<typename T, bool construct_with_allocator>
template <typename U, typename std::enable_if<!std::is_default_constructible<U>::value, int>::type>
inline int
ObSqlArrayImpl<T, construct_with_allocator>::prepare_allocate_(
    int64_t capacity)
{
  return prepare_allocate(capacity, block_allocator_);
}

template<typename T, bool construct_with_allocator>
int64_t ObSqlArrayImpl<T, construct_with_allocator>::to_string(
    char *buf, int64_t buf_len) const
{
  int64_t need_print_count = (count_ > MAX_PRINT_COUNT ? MAX_PRINT_COUNT : count_);
  int64_t pos = 0;
  const int64_t log_keep_size = (ObLogger::get_logger().is_in_async_logging()
                                 ? OB_ASYNC_LOG_KEEP_SIZE
                                 : OB_LOG_KEEP_SIZE);
  J_ARRAY_START();
  for (int64_t index = 0; (index < need_print_count - 1) && (pos < buf_len - 1); ++index) {
    if (pos + log_keep_size >= buf_len - 1) {
      BUF_PRINTF(OB_LOG_ELLIPSIS);
      J_COMMA();
      break;
    }
    BUF_PRINTO(at(index));
    J_COMMA();
  }
  if (0 < need_print_count) {
    BUF_PRINTO(at(need_print_count - 1));
  }
  J_ARRAY_END();
  return pos;
}

template<typename T, bool construct_with_allocator>
template <typename U, typename std::enable_if<std::is_default_constructible<U>::value, int>::type>
int ObSqlArrayImpl<T, construct_with_allocator>::assign_(const ObIArray<T> &other)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(this != &other)) {
    this->reuse();
    int64_t N = other.count();
    ret = reserve_(N, false);
    if (OB_SUCC(ret)) {
      if (typeid(other).hash_code() == typeid(this).hash_code()) {
        ret = MyOp::array_assign(
            static_cast<const ObSqlArrayImpl<T, construct_with_allocator>*>(&other)->data_, data_, N);
      } else {
        // use construct_assign
        ret = ObClassOp<T, false>::array_assign(
            static_cast<const ObSqlArrayImpl<T, construct_with_allocator>*>(&other)->data_, data_, N);
      }
    }
    if (OB_SUCC(ret)) {
      count_ = N;
    }
  }
  return ret;
}

template<typename T, bool construct_with_allocator>
template <typename U, typename std::enable_if<!std::is_default_constructible<U>::value, int>::type>
int ObSqlArrayImpl<T, construct_with_allocator>::assign_(const ObIArray<T> &other)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(this == &other)) {
    // do nothing
  } else if (OB_UNLIKELY(count_ > 0)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_LOG(WARN, "assign to non-empty array is not allowed", K(ret), K(count_));
  } else {
    int64_t N = other.count();
    ret = prepare_allocate(N);
    for (int64_t i = 0; OB_SUCC(ret) && i < N; i++) {
      if (OB_FAIL(data_[i].assign(other.at(i)))) {
        SQL_LOG(WARN, "failed to assign obj", K(ret), K(i), K(N));
      }
    }
    if (OB_SUCC(ret)) {
      count_ = N;
    }
  }
  return ret;
}

template<typename T, bool construct_with_allocator>
template<typename ... Args>
inline int
ObSqlArrayImpl<T, construct_with_allocator>::prepare_allocate(
    int64_t capacity, Args && ... args)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(capacity_ < capacity)) {
    ret = reserve_(capacity, false);
  }
  if (OB_SUCC(ret)) {
    if (is_memcpy_safe()) {
      if (capacity > count_) {
        MEMSET((void *)(data_ + count_), 0, sizeof(T) * (capacity - count_));
      }
    } else {
      for (int64_t i = count_; i < capacity; ++i) {
        new(&data_[i]) T(args...);
      }
    }
    count_ = capacity;
  }
  return ret;
}

template <typename T, bool construct_with_allocator = false>
class ObSqlArray final : public ObSqlArrayImpl<T, construct_with_allocator>
{
public:
  using ObSqlArrayImpl<T, construct_with_allocator>::ObSqlArrayImpl;
  // make copy_assign_wrap happy to support pure nested array
  int assign(const ObIArray<T> &other)
  {
    return ObSqlArrayImpl<T, construct_with_allocator>::assign(other);
  }
  int assign(const ObSqlArray &other)
  {
    return ObSqlArrayImpl<T, construct_with_allocator>::assign(other);
  }
};

} // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_SQL_OB_SQL_ARRAY_H_ */
