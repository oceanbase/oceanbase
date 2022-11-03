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

#ifndef OCEANBASE_LIB_CONTAINER_RAW_SE_ARRAY_
#define OCEANBASE_LIB_CONTAINER_RAW_SE_ARRAY_ 1
#include <typeinfo>
#include "lib/container/ob_array.h"
#include "lib/allocator/page_arena.h"         // for ModulePageAllocator
#include "lib/container/ob_se_array.h"
#include "lib/utility/ob_template_utils.h"
namespace oceanbase
{
namespace common
{

// high performance only in below scences:
//1. rarely called remove
//2. when calling assign, most of Array is empty.
static const int64_t OB_DEFAULT_RAW_SE_ARRAY_COUNT = 64;
template <typename T,
          int64_t LOCAL_ARRAY_SIZE,
          typename BlockAllocatorT = ModulePageAllocator,
          bool auto_free  = false>
class ObRawSEArray final
{
public:
  ObRawSEArray(int64_t block_size = OB_MALLOC_NORMAL_BLOCK_SIZE,
            const BlockAllocatorT &alloc = BlockAllocatorT(ObModIds::OB_SE_ARRAY));
  ObRawSEArray(const lib::ObLabel &label, int64_t block_size);
  ~ObRawSEArray();

  // deep copy
  ObRawSEArray(const ObRawSEArray &other);
  ObRawSEArray &operator=(const ObRawSEArray &other);
  int assign(const ObIArray<T> &other);
  int assign(const ObRawSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, auto_free> &other);

  OB_INLINE int push_back(const T &obj);
  void pop_back();
  int pop_back(T &obj);
  int remove(int64_t idx);

  OB_INLINE int at(int64_t idx, T &obj) const
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(0 > idx || idx >= count_)) {
      ret = OB_ARRAY_OUT_OF_RANGE;
    } else {
      if (is_memcpy_safe()) {
        memcpy(&obj, &local_data_[idx], sizeof(T));
      } else {
        if (OB_FAIL(copy_assign(obj, local_data_[idx]))) {
          LIB_LOG(WARN, "failed to copy data", K(ret));
        }
      }
    }
    return ret;
  }
  OB_INLINE T &at(int64_t idx)     // dangerous
  {
    return local_data_[idx];
  }
  OB_INLINE const T &at(int64_t idx) const // dangerous
  {
    return local_data_[idx];
  }
  OB_INLINE const T &operator[](int64_t idx) const  // dangerous
  {
    return local_data_[idx];
  }
  T *alloc_place_holder();

  OB_INLINE int64_t count() const { return count_; }
  OB_INLINE bool empty() const { return 0 == count(); }

  inline int set_max_print_count(const int64_t max_print_count)
  {
    int ret = OB_SUCCESS;

    if (max_print_count < 0) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      max_print_count_ = max_print_count;
    }

    return ret;
  }

  void reuse()
  {
    if (is_destructor_safe()) {
    } else {
      for (int64_t i = 0; i < count_; i++) {
        local_data_[i].~T();
      }
    }
    count_ = 0;
    error_ = OB_SUCCESS;
  }
  inline void reset() { destroy(); }
  void destroy();
  inline int reserve(int64_t capacity)
  {
    int ret = OB_SUCCESS;
    if (capacity > capacity_) {
      // allocated memory should aligned block_size_
      int64_t new_size = capacity * sizeof(T);
      int64_t plus = new_size % block_size_;
      new_size += (0 == plus) ? 0 : (block_size_ - plus);
      T *new_data = reinterpret_cast<T*>(internal_malloc_(new_size));
      if (OB_NOT_NULL(new_data)) {
        if (is_memcpy_safe()) {
          memcpy(new_data, local_data_, count_ * sizeof(T));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < count_; i++) {
            if (OB_FAIL(construct_assign(new_data[i], local_data_[i]))) {
              LIB_LOG(WARN, "failed to copy new_data", K(ret));
            }
          }
          if (is_destructor_safe()) {
          } else {
            for (int64_t i = 0; OB_SUCC(ret) && i < count_; i++) {
              local_data_[i].~T();
            }
          }
        }
        if (reinterpret_cast<T *>(local_data_buf_) != local_data_) {
          internal_free_(local_data_);
        }
        local_data_ = new_data;
        capacity_ = new_size / sizeof(T);
      } else {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LIB_LOG(ERROR, "no memory", K(ret), K(new_size), K(block_size_), K(count_), K(capacity_));
      }
    }
    return ret;
  }
  //prepare allocate can avoid declaring local data
  int prepare_allocate(int64_t capacity);
  int64_t to_string(char *buffer, int64_t length) const;
  inline int64_t get_data_size() const
  {
    int64_t data_size = capacity_ * sizeof(T);
    if (capacity_ > LOCAL_ARRAY_SIZE) {
      int64_t plus = data_size % block_size_;
      data_size += (0 == plus) ? 0 : (block_size_ - plus);
    }
    return data_size;
  }
  inline bool error() const {return error_ != OB_SUCCESS;};
  inline int64_t get_capacity() const
  {
    return capacity_;
  }
  NEED_SERIALIZE_AND_DESERIALIZE;

  static uint32_t local_data_offset_bits() { return offsetof(ObRawSEArray, local_data_) * 8; }
  static uint32_t local_data_buf_offset_bits() { return offsetof(ObRawSEArray, local_data_buf_) * 8; }
  static uint32_t block_size_offset_bits() { return offsetof(ObRawSEArray, block_size_) * 8; }
  static uint32_t count_offset_bits() { return offsetof(ObRawSEArray, count_) * 8; }
  static uint32_t capacity_offset_bits() { return offsetof(ObRawSEArray, capacity_) * 8; }
  static uint32_t max_print_count_offset_bits() { return offsetof(ObRawSEArray, max_print_count_) * 8; }
  static uint32_t error_offset_bits() { return offsetof(ObRawSEArray, error_) * 8; }
  static uint32_t allocator_offset_bits() { return offsetof(ObRawSEArray, block_allocator_) * 8; }

private:
  // types and constants
  static const int64_t DEFAULT_MAX_PRINT_COUNT = 32;
private:
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
    if (OB_UNLIKELY(!has_alloc_)) {
      if (auto_free) {
        mem_context_ = CURRENT_CONTEXT;
        block_allocator_ = BlockAllocatorT(mem_context_->get_allocator());
      }
      has_alloc_ = true;
    } else {
      //if (auto_free && lib::this_worker().has_req_flag()) {
      //  OB_ASSERT(&CURRENT_CONTEXT->context() == mem_context_);
      //}
    }
    return block_allocator_.alloc(size);
  }

  void internal_free_(void *p) {
    //if (auto_free && lib::this_worker().has_req_flag()) {
    //  OB_ASSERT(&CURRENT_CONTEXT->context() == mem_context_);
    //}
    return block_allocator_.free(p);
  }
private:
  // data members
  T *local_data_;
  char local_data_buf_[LOCAL_ARRAY_SIZE * sizeof(T)];
  int64_t block_size_;
  int64_t count_;
  int64_t capacity_;
  int64_t max_print_count_;
  int error_;
  BlockAllocatorT block_allocator_;
  bool has_alloc_;
  lib::MemoryContext mem_context_;
};

template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT, bool auto_free>
int ObRawSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, auto_free>::serialize(
    char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, count()))) {
    LIB_LOG(WARN, "fail to encode ob array count", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < count(); i ++) {
    if (OB_SUCCESS != (ret = serialization::encode(buf, buf_len, pos, at(i)))) {
      LIB_LOG(WARN, "fail to encode item", K(i), K(ret));
    }
  }
  return ret;
}

template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT, bool auto_free>
int ObRawSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, auto_free>::deserialize(
    const char *buf, int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t count = 0;
  T item;
  reset();
  if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &count))) {
    LIB_LOG(WARN, "fail to decode ob array count", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < count; i ++) {
    if (OB_SUCCESS != (ret = serialization::decode(buf, data_len, pos, item))) {
      LIB_LOG(WARN, "fail to decode array item", K(ret), K(i), K(count));
    } else if (OB_SUCCESS != (ret = push_back(item))) {
      LIB_LOG(WARN, "fail to add item to array", K(ret), K(i), K(count));
    }
  }
  return ret;
}

template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT, bool auto_free>
int64_t ObRawSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, auto_free>::get_serialize_size()
const
{
  int64_t size = 0;
  size += serialization::encoded_length_vi64(count());
  for (int64_t i = 0; i < count(); i ++) {
    size += serialization::encoded_length(at(i));
  }
  return size;
}

template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT, bool auto_free>
ObRawSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, auto_free>::ObRawSEArray(
    int64_t block_size, const BlockAllocatorT &alloc)
    : local_data_(reinterpret_cast<T *>(local_data_buf_)),
      block_size_(block_size),
      count_(0),
      capacity_(LOCAL_ARRAY_SIZE),
      max_print_count_(DEFAULT_MAX_PRINT_COUNT),
      error_(OB_SUCCESS),
      block_allocator_(alloc),
      has_alloc_(false),
      mem_context_(nullptr)
{
  STATIC_ASSERT(LOCAL_ARRAY_SIZE > 0, "se local array size invalid");
  block_size_ = std::max(static_cast<int64_t>(sizeof(T)), block_size);
}

template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT, bool auto_free>
ObRawSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, auto_free>::ObRawSEArray(
    const lib::ObLabel &label, int64_t block_size)
    : local_data_(reinterpret_cast<T *>(local_data_buf_)),
      block_size_(block_size),
      count_(0),
      capacity_(LOCAL_ARRAY_SIZE),
      max_print_count_(DEFAULT_MAX_PRINT_COUNT),
      error_(OB_SUCCESS),
      block_allocator_(label),
      has_alloc_(false),
      mem_context_(nullptr)
{
  STATIC_ASSERT(LOCAL_ARRAY_SIZE > 0, "se local array size invalid");
  block_size_ = std::max(static_cast<int64_t>(sizeof(T)), block_size);
}

template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT, bool auto_free>
ObRawSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, auto_free>::~ObRawSEArray()
{
  destroy();
}


template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT, bool auto_free>
OB_INLINE int ObRawSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, auto_free>::push_back(
    const T &obj)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(count_ == capacity_)) {
    ret = reserve(2 * capacity_);
  }

  if (OB_SUCC(ret)) {
    if (is_memcpy_safe()) {
      local_data_[count_] = obj;
      count_++;
    } else {
      if (OB_FAIL(construct_assign(local_data_[count_], obj))) {
        LIB_LOG(WARN, "failed to copy data", K(ret));
      } else {
        count_++;
      }
    }
  }

  return ret;
}

template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT, bool auto_free>
void ObRawSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, auto_free>::pop_back()
{
  if (OB_UNLIKELY(count_ <= 0)) {
  } else {
    --count_;
  }
}

template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT, bool auto_free>
int ObRawSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, auto_free>::pop_back(
    T &obj)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(count_ <= 0)) {
    ret = OB_ENTRY_NOT_EXIST;
  } else {
    if (is_memcpy_safe()) {
      memcpy(&obj, &local_data_[--count_], sizeof(T));
    } else {
      if (OB_FAIL(copy_assign(obj, local_data_[count_ - 1]))) {
        LIB_LOG(WARN, "failed to copy data", K(ret));
      }
      local_data_[count_ - 1].~T();
      count_--;
    }
  }
  return ret;
}

template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT, bool auto_free>
int ObRawSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, auto_free>::remove(
    int64_t idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 > idx || idx >= count_)) {
    ret = OB_ARRAY_OUT_OF_RANGE;
  } else {
    if (is_memcpy_safe()) {
      memmove(&local_data_[idx], &local_data_[idx + 1], (count_ - idx - 1) * sizeof(T));
    } else {
      for (int64_t i = idx; OB_SUCC(ret) && i < count_ - 1; ++i) {
        if (OB_FAIL(copy_assign(local_data_[i], local_data_[i + 1]))) {
          LIB_LOG(WARN, "failed to copy data", K(ret));
        }
      }
      local_data_[count_ - 1].~T();
    }
    count_--;
  }
  return ret;
}

template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT, bool auto_free>
inline  T *ObRawSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, auto_free>::alloc_place_holder()
{
  int ret = OB_SUCCESS;
  T *ptr = NULL;

  if (OB_UNLIKELY(count_ == capacity_)) {
    ret = reserve(2 * capacity_);
  }

  if (OB_SUCC(ret)) {
    if (is_memcpy_safe()) {
      ptr = &local_data_[count_];
    } else {
      ptr = new(&local_data_[count_]) T();
    }
    count_++;
  }

  return ptr;
}

template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT, bool auto_free>
void ObRawSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, auto_free>::destroy()
{
  if (is_destructor_safe()) {
  } else {
    for (int64_t i = 0; i < count_; i++) {
      local_data_[i].~T();
    }
  }

  if (local_data_ != reinterpret_cast<T *>(local_data_buf_)) {
    internal_free_(local_data_);
    local_data_ = reinterpret_cast<T *>(local_data_buf_);
  }
  count_ = 0;
  capacity_ = LOCAL_ARRAY_SIZE;
  max_print_count_ = DEFAULT_MAX_PRINT_COUNT;
  error_ = OB_SUCCESS;
}

template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT, bool auto_free>
inline int
ObRawSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, auto_free>::prepare_allocate(
    int64_t capacity)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(capacity_ < capacity)) {
    ret = reserve(capacity);
  }
  if (OB_SUCC(ret)) {
    if (is_memcpy_safe()) {
    } else {
      for (int64_t i = count_; i < capacity; ++i) {
        new(&local_data_[i]) T();
      }
    }
    count_ = capacity;
  }
  return ret;
}

template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT, bool auto_free>
int64_t ObRawSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, auto_free>::to_string(
    char *buf, int64_t buf_len) const
{
  int64_t need_print_count = (count_ > max_print_count_ ? max_print_count_ : count_);
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
  if (0 < need_print_count) {
    BUF_PRINTO(at(need_print_count - 1));
  }
  J_ARRAY_END();
  return pos;
}

template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT, bool auto_free>
ObRawSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, auto_free>
&ObRawSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, auto_free>::operator=
(const ObRawSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, auto_free> &other)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(this != &other)) {
    this->reuse();
    int64_t N = other.count();
    ret = reserve(N);
    for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
      if (OB_FAIL(construct_assign(local_data_[i], other.local_data_[i]))) {
        LIB_LOG(WARN, "failed to copy data", K(ret));
      }
    }
    error_ = ret;
    if (OB_SUCC(ret)) {
      count_ = N;
    }
  }
  return *this;
}

template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT, bool auto_free>
int ObRawSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, auto_free>::assign(const ObIArray<T> &other)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(this != &other)) {
    this->reuse();
    int64_t N = other.count();
    ret = reserve(N);
    if (OB_SUCC(ret)) {
      if (is_memcpy_safe() && typeid(other).hash_code() == typeid(this).hash_code()) {
        memcpy(local_data_, static_cast<ObRawSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, auto_free>*>(
          const_cast<ObIArray<T>*>(&other))->local_data_, N * sizeof(T));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
          if (OB_FAIL(construct_assign(local_data_[i], other.at(i)))) {
            LIB_LOG(WARN, "failed to copy data", K(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      count_ = N;
    }
  }
  return ret;
}

template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT, bool auto_free>
int ObRawSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, auto_free>::assign(
    const ObRawSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, auto_free> &other)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(this != &other)) {
    this->reuse();
    int64_t N = other.count();
    ret = reserve(N);
    if (OB_SUCC(ret)) {
      if (is_memcpy_safe() && typeid(other).hash_code() == typeid(this).hash_code()) {
        memcpy(
            local_data_,
            const_cast<ObRawSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, auto_free>*>(&other)->local_data_,
            N * sizeof(T));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
          if (OB_FAIL(construct_assign(local_data_[i], other.at(i)))) {
            LIB_LOG(WARN, "failed to copy data", K(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      count_ = N;
    }
  }
  return ret;
}

template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT, bool auto_free>
ObRawSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, auto_free>::ObRawSEArray(
    const ObRawSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, auto_free> &other)
    : local_data_(reinterpret_cast<T *>(local_data_buf_)),
      block_size_(other.block_size_),
      count_(0),
      capacity_(LOCAL_ARRAY_SIZE),
      max_print_count_(DEFAULT_MAX_PRINT_COUNT),
      error_(OB_SUCCESS),
      block_allocator_(),
      has_alloc_(false),
      mem_context_(nullptr)
{
  *this = other;
}

template <typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT, bool auto_free>
inline int databuff_encode_element(char *buf, const int64_t buf_len, int64_t &pos,
                                   oceanbase::yson::ElementKeyType key,
                                   const ObRawSEArray<T, LOCAL_ARRAY_SIZE,
                                   BlockAllocatorT, auto_free> &array)
{
  return oceanbase::yson::databuff_encode_array_element(buf, buf_len, pos, key, array);
}

} // end namespace common
} // end namespace oceanbase

#endif /* OCEANBASE_LIB_CONTAINER_RAW_SE_ARRAY_ */
