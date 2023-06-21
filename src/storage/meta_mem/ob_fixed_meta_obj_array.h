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

#ifndef OCEANBASE_STORAGE_OB_FIXED_META_OBJ_ARRAY_H_
#define OCEANBASE_STORAGE_OB_FIXED_META_OBJ_ARRAY_H_

#include "lib/container/ob_iarray.h"
#include "lib/utility/ob_unify_serialize.h"
#include "lib/utility/utility.h"

namespace oceanbase
{

namespace storage
{
/*
  Fixed size array that support
  1. Allocate memory from inner allocator, lifetime guaranteed by container
  2. Construct array on specific memory segment, lifetime guaranteed by caller
  Deep copy and trasition from memory layouts above
  Serialization / deserialization

  type of item is prefered to be POD
*/
template <typename T>
class ObFixedMetaObjArray : public common::ObIArray<T>
{
public:
  using common::ObIArray<T>::count;
  using common::ObIArray<T>::at;

  ObFixedMetaObjArray()
    : ObIArray<T>(),
      allocator_(nullptr),
      capacity_(0),
      init_cnt_(0) {}

  ~ObFixedMetaObjArray() { reset(); }

  inline int init(const int64_t capacity, ObIAllocator &allocator);
  inline int init(const int64_t capacity, const int64_t buf_len, char *data_buf, int64_t &array_size);
  inline int init_and_assign(const common::ObIArray<T> &other, ObIAllocator &allocator);
  inline int64_t get_serialize_size() const;
  inline int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  inline int deserialize(const char *buf, int64_t data_len, int64_t &pos, ObIAllocator &allocator);
  inline int64_t get_deep_copy_size() const;
  inline int deep_copy(char *dst_buf, const int64_t buf_size, int64_t &pos, ObFixedMetaObjArray &dst_array) const;
  inline T &operator[](const int64_t idx) { return at(idx); }
  inline const T &operator[](const int64_t idx) const { return at(idx); }
  inline void reset() override;
  inline void reuse() override;
  inline int push_back(const T &obj) override;
  inline void pop_back() override;
  inline int pop_back(T &obj) override;
  inline int remove(int64_t idx) override;
  inline int at(int64_t idx, T &obj) const override;
  inline void extra_access_check(void) const override {}
  inline void destroy() override;
  inline int reserve(int64_t capacity) override;
  inline int assign(const common::ObIArray<T> &other) override;
  inline int prepare_allocate(int64_t capacity) override;
  int64_t get_count() const { return count_; }
  bool is_empty() const { return 0 == count_; }
  INHERIT_TO_STRING_KV("ObIArray", ObIArray<T>, KP_(data), K_(count),
      KP_(allocator), K_(capacity), K_(init_cnt));
private:
  DISALLOW_COPY_AND_ASSIGN(ObFixedMetaObjArray);
private:
  using common::ObIArray<T>::data_;
  using common::ObIArray<T>::count_;
  ObIAllocator *allocator_;
  int64_t capacity_;
  int64_t init_cnt_;
};

template<typename T>
int ObFixedMetaObjArray<T>::init(const int64_t capacity, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(data_) || OB_NOT_NULL(allocator_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "double initialization", K(ret), KPC(this));
  } else if (OB_UNLIKELY(capacity < 0)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "invalid argument", K(capacity));
  } else {
    allocator_ = &allocator;
    count_ = 0;
    init_cnt_ = 0;
    if (0 == capacity) {
      capacity_ = capacity;
    } else if (OB_ISNULL(data_ = static_cast<T *>(allocator_->alloc(capacity * sizeof(T))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "failed to allocate memory for array", K(ret), K(capacity), K(sizeof(T)));
    } else {
      capacity_ = capacity;
    }
  }
  return ret;
}

template<typename T>
int ObFixedMetaObjArray<T>::init(
    const int64_t capacity,
    const int64_t buf_len,
    char *data_buf,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  const int64_t inner_array_size = sizeof(T) * capacity;
  if (OB_NOT_NULL(data_) || OB_NOT_NULL(allocator_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init twice", K(ret), KPC(this));
  } else if (OB_UNLIKELY(capacity < 0 || (buf_len - pos) < inner_array_size) || OB_ISNULL(data_buf)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "invalid argument", K(capacity), K(buf_len), K(pos), K(inner_array_size), KP(data_buf));
  } else {
    allocator_ = nullptr;
    count_ = 0;
    init_cnt_ = 0;
    data_ = reinterpret_cast<T *>(data_buf + pos);
    capacity_ = capacity;
    pos += inner_array_size;
  }
  return ret;
}

template<typename T>
int ObFixedMetaObjArray<T>::init_and_assign(
    const common::ObIArray<T> &other,
    ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init(other.count(), allocator))) {
    STORAGE_LOG(WARN, "failed to init fixed array", K(ret));
  } else if (OB_FAIL(assign(other))) {
    STORAGE_LOG(WARN, "failed to assign from other array", K(ret), K(other));
  }
  return ret;
}

template<typename T>
int64_t ObFixedMetaObjArray<T>::get_serialize_size() const
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN_ARRAY(data_, count_);
  return len;
}

template<typename T>
int ObFixedMetaObjArray<T>::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE_ARRAY(data_, count_);
  return ret;
}

template<typename T>
int ObFixedMetaObjArray<T>::deserialize(
    const char *buf,
    int64_t data_len,
    int64_t &pos,
    ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  int64_t count = 0;
  if (OB_UNLIKELY(0 != count_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "deserialize a non-empty fixed array is not supported", K(ret));
  }
  OB_UNIS_DECODE(count);
  if (OB_SUCC(ret) && count > 0) {
    if (OB_FAIL(init(count, allocator))) {
      STORAGE_LOG(WARN, "fail to init array", K(ret));
    } else if (OB_FAIL(prepare_allocate(count))) {
      STORAGE_LOG(WARN, "fail to init array item", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < count; i ++) {
        OB_UNIS_DECODE(at(i));
      }
    }
  }
  return ret;
}

template<typename T>
int64_t ObFixedMetaObjArray<T>::get_deep_copy_size() const
{
  return count_ * sizeof(T);
}

template<typename T>
int ObFixedMetaObjArray<T>::deep_copy(
    char *dst_buf,
    const int64_t buf_size,
    int64_t &pos,
    ObFixedMetaObjArray &dst_array) const
{
  int ret = OB_SUCCESS;
  const int64_t memory_size = get_deep_copy_size();
  if (OB_ISNULL(dst_buf) || OB_UNLIKELY(buf_size < memory_size)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalue argument", K(ret), KP(dst_buf), K(buf_size), K(memory_size));
  } else if (OB_NOT_NULL(dst_array.data_) || OB_NOT_NULL(dst_array.allocator_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "can not copy to inited array with this deep copy interface", K(ret), K(dst_array));
  } else if (OB_FAIL(dst_array.init(count_, buf_size, dst_buf, pos))) {
    STORAGE_LOG(WARN, "fail to init dst array with copy buf", K(ret), KP(dst_buf), K_(count));
  } else if (OB_FAIL(dst_array.assign(*this))) {
    STORAGE_LOG(WARN, "fail to copy local data to dest array", K(ret));
  }
  return ret;
}

template<typename T>
void ObFixedMetaObjArray<T>::reset()
{
  if (nullptr != data_) {
    for (int64_t i = 0; i < init_cnt_; ++i) {
      data_[i].~T();
    }
    if (allocator_ != nullptr) {
      allocator_->free(data_);
      allocator_ = nullptr;
    }
    data_ = nullptr;
  }
  count_ = 0;
  capacity_ = 0;
  init_cnt_ = 0;
}

template<typename T>
void ObFixedMetaObjArray<T>::reuse()
{
  reset();
}

template<typename T>
int ObFixedMetaObjArray<T>::push_back(const T &obj)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(count_ >= capacity_)) {
    ret = OB_SIZE_OVERFLOW;
    STORAGE_LOG(WARN, "fixede array size over flow", K(ret), K_(capacity));
  } else if (OB_ISNULL(data_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "array not inited", K(ret));
  } else if (OB_LIKELY(count_ >= init_cnt_)) {
    if (OB_FAIL(common::construct_assign(data_[count_], obj))) {
      STORAGE_LOG(WARN, "failed to copy data", K(ret));
    } else {
      ++init_cnt_;
    }
  } else if (OB_FAIL(common::copy_assign(data_[count_], obj))) {
    STORAGE_LOG(WARN, "failed to copy data", K(ret));
  }

  if (OB_SUCC(ret)) {
    ++count_;
  }
  return ret;
}

template<typename T>
void ObFixedMetaObjArray<T>::pop_back()
{
  if (count_ > 0) {
    --count_;
  }
}

template<typename T>
int ObFixedMetaObjArray<T>::pop_back(T &obj)
{
  UNUSED(obj);
  return OB_NOT_IMPLEMENT;
}

template<typename T>
int ObFixedMetaObjArray<T>::remove(int64_t idx)
{
  UNUSED(idx);
  return OB_NOT_IMPLEMENT;
}

template<typename T>
int ObFixedMetaObjArray<T>::at(int64_t idx, T &obj) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(idx < 0 || idx >= count_)) {
    ret = OB_ARRAY_OUT_OF_RANGE;
  } else {
    if (OB_FAIL(common::copy_assign(obj, data_[idx]))) {
      OB_LOG(WARN, "failed to copy obj", K(ret));
    }
  }
  return ret;
}

template<typename T>
void ObFixedMetaObjArray<T>::destroy()
{
  reset();
}

template<typename T>
int ObFixedMetaObjArray<T>::reserve(int64_t capacity)
{
  UNUSED(capacity);
  return OB_NOT_IMPLEMENT;
}

template<typename T>
int ObFixedMetaObjArray<T>::assign(const common::ObIArray<T> &other)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(capacity_ < other.count())) {
    ret = OB_SIZE_OVERFLOW;
    STORAGE_LOG(WARN, "memory of current array is not enough", K(ret), K_(capacity), K(other));
  } else if (OB_UNLIKELY(0 != count_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "assign to a non-empty array is not supported", K(ret));
  } else if (0 == other.count()) {
    capacity_ = 0;
    count_ = 0;
    init_cnt_ = 0;
    data_ = nullptr;
  } else if (OB_ISNULL(data_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "assign to a non-inited fixed meta obj array is not supported", K(ret));
  } else if (this != &other) {
    for (int64_t i = 0; OB_SUCC(ret) && i < other.count(); ++i) {
      if (OB_FAIL(push_back(other.at(i)))) {
        STORAGE_LOG(WARN, "failed to push item in array", K(ret), K(i));
      }
    }
  }
  return ret;
}

template<typename T>
int ObFixedMetaObjArray<T>::prepare_allocate(int64_t capacity)
{
  // memory required to be pre-allocated for ObFixedMetaObjArray
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(capacity > capacity_)) {
    ret = OB_SIZE_OVERFLOW;
    STORAGE_LOG(WARN, "fixed array size over flow", K(ret), K(capacity), K_(capacity));
  } else if (0 == capacity) {
    count_ = 0;
    init_cnt_ = 0;
  } else if (OB_ISNULL(data_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "array not inited", K(ret));
  } else {
    for (int64_t i = init_cnt_; i < capacity; ++i) {
      new(&data_[i]) T();
    }
    count_ = capacity > count_ ? capacity : count_;
    init_cnt_ = capacity >init_cnt_ ? capacity : init_cnt_;
  }
  return ret;
}

} // namespace oceanbase
} // namespace oceanbase

#endif