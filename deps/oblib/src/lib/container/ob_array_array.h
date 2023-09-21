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

#ifndef OCEANBASE_LIB_CONTAINER_ARRAY_ARRAY_
#define OCEANBASE_LIB_CONTAINER_ARRAY_ARRAY_

#include <typeinfo>
#include "lib/utility/ob_macro_utils.h"
#include "lib/container/ob_array.h"
#include "lib/allocator/page_arena.h"         // for ModulePageAllocator
#include "lib/container/ob_iarray.h"
#include "lib/container/ob_se_array.h"
#include "lib/utility/ob_template_utils.h"
#include "lib/utility/ob_hang_fatal_error.h"
#include "lib/utility/serialization.h"

namespace oceanbase
{
namespace common
{

static const int64_t DEFAULT_LOCAL_ARRAY_SIZE = 8;
static const int64_t DEFAULT_ARRAY_ARRAY_SIZE = 8;
template <typename T,
          int64_t LOCAL_ARRAY_SIZE = DEFAULT_LOCAL_ARRAY_SIZE,
          int64_t ARRAY_ARRAY_SIZE = DEFAULT_ARRAY_ARRAY_SIZE,
          typename BlockAllocatorT = ModulePageAllocator>
class ObArrayArray
{
public:
  typedef ObSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, false> LocalArrayT ;
  explicit ObArrayArray(const int64_t block_size = OB_MALLOC_NORMAL_BLOCK_SIZE,
                        const BlockAllocatorT &alloc = BlockAllocatorT("ObArrayArray"));
  explicit ObArrayArray(const lib::ObLabel &label,
                        const int64_t block_size = OB_MALLOC_NORMAL_BLOCK_SIZE);
  virtual ~ObArrayArray();
  void reset();
  void reuse();
  int reserve(const int64_t size);
  int at(const int64_t array_idx, ObIArray<T> &array);
  int at(const int64_t array_idx, const int64_t idx, T &obj);
  int assign(const ObArrayArray<T> &other);
  NEED_SERIALIZE_AND_DESERIALIZE;

  OB_INLINE T &at(const int64_t array_idx, const int64_t idx)
  {
    if (OB_UNLIKELY(0 > array_idx || array_idx >= count_)) {
      LIB_LOG_RET(ERROR, common::OB_INVALID_ARGUMENT, "Unexpected array idx", K_(count), K_(capacity), K(array_idx));
      right_to_die_or_duty_to_live();
    } else if (OB_ISNULL(array_ptrs_) || OB_ISNULL(array_ptrs_[array_idx])) {
      LIB_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "Unexpected null array array ptr", K_(count), K_(capacity), K(array_idx),
              KP_(array_ptrs));
      right_to_die_or_duty_to_live();
    }
    return array_ptrs_[array_idx]->at(idx);
  }
  OB_INLINE ObIArray<T> &at(const int64_t array_idx)
  {
    if (OB_UNLIKELY(0 > array_idx || array_idx >= count_)) {
      LIB_LOG_RET(ERROR, common::OB_INVALID_ARGUMENT, "Unexpected array idx", K_(count), K_(capacity), K(array_idx));
      right_to_die_or_duty_to_live();
    } else if (OB_ISNULL(array_ptrs_) || OB_ISNULL(array_ptrs_[array_idx])) {
      LIB_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "Unexpected null array array ptr", K_(count), K_(capacity), K(array_idx),
              KP_(array_ptrs));
      right_to_die_or_duty_to_live();
    }
    return *array_ptrs_[array_idx];
  }
  OB_INLINE const ObIArray<T> &at(const int64_t array_idx) const
  {
    if (OB_UNLIKELY(0 > array_idx || array_idx >= count_)) {
      LIB_LOG_RET(ERROR, common::OB_INVALID_ARGUMENT, "Unexpected array idx", K_(count), K_(capacity), K(array_idx));
      right_to_die_or_duty_to_live();
    } else if (OB_ISNULL(array_ptrs_) || OB_ISNULL(array_ptrs_[array_idx])) {
      LIB_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "Unexpected null array array ptr", K_(count), K_(capacity), K(array_idx),
              KP_(array_ptrs));
      right_to_die_or_duty_to_live();
    }
    return *array_ptrs_[array_idx];
  }
  int push_back(const ObIArray<T> &obj_array);
  int push_back(const int64_t array_idx, const T &obj);
  OB_INLINE int64_t count() const { return count_; }
  OB_INLINE int64_t count(const int64_t array_idx) const
  {
    if (OB_UNLIKELY(0 > array_idx || array_idx >= count_)) {
      LIB_LOG_RET(ERROR, common::OB_INVALID_ARGUMENT, "Unexpected array idx", K_(count), K_(capacity), K(array_idx));
      right_to_die_or_duty_to_live();
    } else if (OB_ISNULL(array_ptrs_) || OB_ISNULL(array_ptrs_[array_idx])) {
      LIB_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "Unexpected null array array ptr", K_(count), K_(capacity), K(array_idx),
              KP_(array_ptrs));
      right_to_die_or_duty_to_live();
    }
    return array_ptrs_[array_idx]->count();
  }
  int64_t to_string(char *buffer, int64_t length) const;
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObArrayArray);
private:
  LocalArrayT *local_array_buf_[ARRAY_ARRAY_SIZE];
  LocalArrayT **array_ptrs_;
  int64_t count_;
  int64_t capacity_;
  int64_t ptr_capacity_;
  int64_t block_size_;
  BlockAllocatorT alloc_;
};

template<typename T, int64_t LOCAL_ARRAY_SIZE, int64_t ARRAY_ARRAY_SIZE, typename BlockAllocatorT>
ObArrayArray<T, LOCAL_ARRAY_SIZE, ARRAY_ARRAY_SIZE, BlockAllocatorT>::ObArrayArray(
    const int64_t block_size, const BlockAllocatorT &alloc)
  : local_array_buf_(),
    array_ptrs_(local_array_buf_),
    count_(0),
    capacity_(0),
    ptr_capacity_(ARRAY_ARRAY_SIZE),
    block_size_(block_size),
    alloc_(alloc)
{
  MEMSET(local_array_buf_, 0, sizeof(LocalArrayT *) * ARRAY_ARRAY_SIZE);
  block_size_ = MAX(static_cast<int64_t>(sizeof(T)), block_size);
}

template<typename T, int64_t LOCAL_ARRAY_SIZE, int64_t ARRAY_ARRAY_SIZE, typename BlockAllocatorT>
ObArrayArray<T, LOCAL_ARRAY_SIZE, ARRAY_ARRAY_SIZE, BlockAllocatorT>::ObArrayArray(
    const lib::ObLabel &label, const int64_t block_size)
  : local_array_buf_(),
    array_ptrs_(local_array_buf_),
    count_(0),
    capacity_(0),
    ptr_capacity_(ARRAY_ARRAY_SIZE),
    block_size_(block_size),
    alloc_(label)
{
  MEMSET(local_array_buf_, 0, sizeof(LocalArrayT *) * ARRAY_ARRAY_SIZE);
  block_size_ = MAX(static_cast<int64_t>(sizeof(T)), block_size);
}

template<typename T, int64_t LOCAL_ARRAY_SIZE, int64_t ARRAY_ARRAY_SIZE, typename BlockAllocatorT>
ObArrayArray<T, LOCAL_ARRAY_SIZE, ARRAY_ARRAY_SIZE, BlockAllocatorT>::~ObArrayArray()
{
  reset();
}

template<typename T, int64_t LOCAL_ARRAY_SIZE, int64_t ARRAY_ARRAY_SIZE, typename BlockAllocatorT>
void ObArrayArray<T, LOCAL_ARRAY_SIZE, ARRAY_ARRAY_SIZE, BlockAllocatorT>::reset()
{
  if (OB_ISNULL(array_ptrs_)) {
    LIB_LOG_RET(ERROR, common::OB_INVALID_ARGUMENT, "Unexpected null array array ptr", K_(count), K_(capacity), KP_(array_ptrs));
    array_ptrs_ = local_array_buf_;
    capacity_ = ARRAY_ARRAY_SIZE;
  }

  for (int64_t i = 0; i < capacity_; i++) {
    if (OB_NOT_NULL(array_ptrs_[i])) {
      array_ptrs_[i]->destroy();
      alloc_.free(array_ptrs_[i]);
      array_ptrs_[i] = nullptr;
    } else {
      LIB_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "Unexpected null array array ptr", K(i), K_(count), K_(capacity));
    }
  }
  if (array_ptrs_ != local_array_buf_) {
    alloc_.free(array_ptrs_);
    array_ptrs_ = local_array_buf_;
  }
  MEMSET(local_array_buf_, 0, sizeof(LocalArrayT *) * ARRAY_ARRAY_SIZE);
  count_ = 0;
  capacity_ = 0;
  ptr_capacity_ = ARRAY_ARRAY_SIZE;
}

template<typename T, int64_t LOCAL_ARRAY_SIZE, int64_t ARRAY_ARRAY_SIZE, typename BlockAllocatorT>
void ObArrayArray<T, LOCAL_ARRAY_SIZE, ARRAY_ARRAY_SIZE, BlockAllocatorT>::reuse()
{
  if (OB_ISNULL(array_ptrs_)) {
    LIB_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "Unexpected null array array ptr", K_(count), K_(capacity), KP_(array_ptrs));
    reset();
  }

  for (int64_t i = 0; i < capacity_; i++) {
    if (OB_NOT_NULL(array_ptrs_[i])) {
      array_ptrs_[i]->reuse();
    } else {
      LIB_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "Unexpected null array array ptr", K(i), K_(count), K_(capacity));
    }
  }
  count_ = 0;
}

template<typename T, int64_t LOCAL_ARRAY_SIZE, int64_t ARRAY_ARRAY_SIZE, typename BlockAllocatorT>
int ObArrayArray<T, LOCAL_ARRAY_SIZE, ARRAY_ARRAY_SIZE, BlockAllocatorT>::reserve(
    const int64_t capacity)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(capacity < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "Invalid argument to reserve array array", K(ret), K(capacity));
  } else if (capacity <= capacity_) {
  } else  {
    LocalArrayT **new_array_ptrs = array_ptrs_;
    int64_t new_ptr_capacity = ptr_capacity_;
    if (capacity > ptr_capacity_) {
      // need expand ptr array
      new_ptr_capacity = MAX(ptr_capacity_ * 2, capacity);
      if (OB_ISNULL(new_array_ptrs = reinterpret_cast<LocalArrayT **>
                                     (alloc_.alloc(sizeof(LocalArrayT *) * new_ptr_capacity)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LIB_LOG(ERROR, "Failed to alloc memory for obarrayarray", K(new_ptr_capacity), K_(count),
                K_(capacity));
      } else {
        MEMSET(new_array_ptrs, 0, sizeof(LocalArrayT *) * new_ptr_capacity);
        MEMCPY(new_array_ptrs, array_ptrs_, sizeof(LocalArrayT *) * capacity_);
      }
    }
    if (OB_SUCC(ret)) {
      void *ptr = nullptr;
      for (int64_t i = capacity_; OB_SUCC(ret) && i < capacity; i++) {
        if (OB_NOT_NULL(new_array_ptrs[i])) {
          ret = OB_ERR_UNEXPECTED;
          LIB_LOG(ERROR, "Unexpecte not null array array ptr", K(i), K_(count), K_(capacity), K(capacity),
                  KP(new_array_ptrs[i]));
        } else if (OB_ISNULL(ptr = alloc_.alloc(sizeof(LocalArrayT)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LIB_LOG(ERROR, "Failed to alloc memory for obarrayarray", K(ret), K(i), K_(capacity));
        } else {
          new_array_ptrs[i] = new (ptr) LocalArrayT(block_size_, alloc_);
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (new_ptr_capacity == ptr_capacity_) {
      } else if (array_ptrs_ != local_array_buf_) {
        alloc_.free(array_ptrs_);
      } else {
        MEMSET(local_array_buf_, 0, sizeof(LocalArrayT *) * ARRAY_ARRAY_SIZE);
      }
      array_ptrs_ = new_array_ptrs;
      capacity_ = capacity;
      ptr_capacity_ = new_ptr_capacity;
    } else if (OB_NOT_NULL(new_array_ptrs) && new_array_ptrs != array_ptrs_) {
      alloc_.free(new_array_ptrs);
      new_array_ptrs = nullptr;
    }
  }
  return ret;
}

template<typename T, int64_t LOCAL_ARRAY_SIZE, int64_t ARRAY_ARRAY_SIZE, typename BlockAllocatorT>
int ObArrayArray<T, LOCAL_ARRAY_SIZE, ARRAY_ARRAY_SIZE, BlockAllocatorT>::push_back(
    const ObIArray<T> &other_array)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(count_ == capacity_) && OB_FAIL(reserve(MAX(count_ + 1, count_ * 2)))) {
    LIB_LOG(WARN, "Failed to reserve array array", K(ret), K_(count), K_(capacity));
  } else if (OB_ISNULL(array_ptrs_[count_])) {
    ret = OB_ERR_UNEXPECTED;
    LIB_LOG(ERROR, "Unexpected null array array ptr", K(ret), K_(count), K_(capacity));
  } else if (OB_FAIL(array_ptrs_[count_]->assign(other_array))) {
    LIB_LOG(WARN, "Failed to assign other array", K(ret), K(other_array), K_(capacity));
  } else {
    count_++;
  }

  return ret;
}

template<typename T, int64_t LOCAL_ARRAY_SIZE, int64_t ARRAY_ARRAY_SIZE, typename BlockAllocatorT>
int ObArrayArray<T, LOCAL_ARRAY_SIZE, ARRAY_ARRAY_SIZE, BlockAllocatorT>::push_back(
    const int64_t array_idx, const T &obj)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(array_idx >= count_)) {
    ret = OB_ARRAY_OUT_OF_RANGE;
    LIB_LOG(WARN, "Invalid array index", K(ret), K_(count), K_(capacity), K(array_idx));
  } else if (OB_ISNULL(array_ptrs_) || OB_ISNULL(array_ptrs_[array_idx])) {
    ret = OB_ERR_UNEXPECTED;
    LIB_LOG(ERROR, "Unexpected null array array ptr", K(ret), K_(count), K_(capacity), K(array_idx),
            KP_(array_ptrs));
  } else if (OB_FAIL(array_ptrs_[array_idx]->push_back(obj))) {
    LIB_LOG(WARN, "Failed to assign other array", K(ret), K(obj), K(array_idx));
  }

  return ret;

}

template<typename T, int64_t LOCAL_ARRAY_SIZE, int64_t ARRAY_ARRAY_SIZE, typename BlockAllocatorT>
int ObArrayArray<T, LOCAL_ARRAY_SIZE, ARRAY_ARRAY_SIZE, BlockAllocatorT>::at(
    const int64_t array_idx, ObIArray<T> &array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 > array_idx || array_idx >= count_)) {
    ret = OB_ARRAY_OUT_OF_RANGE;
    LIB_LOG(WARN, "Invalid array index", K(ret), K_(count), K_(capacity), K(array_idx));
  } else if (OB_ISNULL(array_ptrs_) || OB_ISNULL(array_ptrs_[array_idx])) {
    ret = OB_ERR_UNEXPECTED;
    LIB_LOG(ERROR, "Unexpected null array array ptr", K(ret), K_(count), K_(capacity), K(array_idx),
            KP_(array_ptrs));
  } else {
    array = *array_ptrs_[array_idx];
  }
  return ret;
}

template<typename T, int64_t LOCAL_ARRAY_SIZE, int64_t ARRAY_ARRAY_SIZE, typename BlockAllocatorT>
int ObArrayArray<T, LOCAL_ARRAY_SIZE, ARRAY_ARRAY_SIZE, BlockAllocatorT>::at(
    const int64_t array_idx, const int64_t idx, T &obj)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 > array_idx || array_idx >= count_)) {
    ret = OB_ARRAY_OUT_OF_RANGE;
    LIB_LOG(WARN, "Invalid array index", K(ret), K_(count), K_(capacity), K(array_idx));
  } else if (OB_ISNULL(array_ptrs_) || OB_ISNULL(array_ptrs_[array_idx])) {
    ret = OB_ERR_UNEXPECTED;
    LIB_LOG(ERROR, "Unexpected null array array ptr", K(ret), K_(count), K_(capacity), K(array_idx),
            KP_(array_ptrs));
  } else {
    ret = array_ptrs_[array_idx]->at(idx, obj);
  }
  return ret;
}

template<typename T, int64_t LOCAL_ARRAY_SIZE, int64_t ARRAY_ARRAY_SIZE, typename BlockAllocatorT>
int64_t ObArrayArray<T, LOCAL_ARRAY_SIZE, ARRAY_ARRAY_SIZE, BlockAllocatorT>::to_string(
    char *buf, int64_t buf_len) const
{
  int64_t pos = 0;
  J_KV(K_(count), K_(capacity), K_(ptr_capacity), K_(block_size));
  J_COMMA();
  for (int64_t i = 0; i < count_; i++) {
    if (OB_NOT_NULL(array_ptrs_[i])) {
      J_KV("array_idx", i, KPC(array_ptrs_[i]));
      J_COMMA();
    }
  }
  return pos;
}

template<typename T, int64_t LOCAL_ARRAY_SIZE, int64_t ARRAY_ARRAY_SIZE, typename BlockAllocatorT>
int ObArrayArray<T, LOCAL_ARRAY_SIZE, ARRAY_ARRAY_SIZE, BlockAllocatorT>::assign(const ObArrayArray<T> &other)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(this != &other)) {
    this->reuse();
    int64_t N = other.count();
    ret = reserve(N);
    for (int64_t i = 0; OB_SUCC(ret) && i < N; i++) {
      if (OB_FAIL(array_ptrs_[i]->assign(other.at(i)))) {
        LIB_LOG(WARN, "fail to assign array", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      count_ = N;
    }
  }
  return ret;
}


template<typename T, int64_t LOCAL_ARRAY_SIZE, int64_t ARRAY_ARRAY_SIZE, typename BlockAllocatorT>
int ObArrayArray<T, LOCAL_ARRAY_SIZE, ARRAY_ARRAY_SIZE, BlockAllocatorT>::serialize(
    char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, count()))) {
    LIB_LOG(WARN, "fail to encode ob array count", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < count(); i++) {
    const LocalArrayT &item = *array_ptrs_[i];
    if (OB_SUCCESS != (ret = serialization::encode(buf, buf_len, pos, item))) {
      LIB_LOG(WARN, "fail to encode item", K(i), K(ret));
    }
  }
  return ret;
}

template<typename T, int64_t LOCAL_ARRAY_SIZE, int64_t ARRAY_ARRAY_SIZE, typename BlockAllocatorT>
int ObArrayArray<T, LOCAL_ARRAY_SIZE, ARRAY_ARRAY_SIZE, BlockAllocatorT>::deserialize(
    const char *buf, int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t count = 0;
  reset();
  if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &count))) {
    LIB_LOG(WARN, "fail to decode ob array count", K(ret));
  } else if (OB_SUCCESS != (ret = reserve(count))) {
    LIB_LOG(WARN, "fail to allocate space", K(ret), K(count));
  } else {
    count_ = count;
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < count; i++) {
    LocalArrayT &item = *array_ptrs_[i];
    if (OB_SUCCESS != (ret = serialization::decode(buf, data_len, pos, item))) {
      LIB_LOG(WARN, "fail to decode array item", K(ret), K(i), K(count));
    }
  }
  return ret;
}

template<typename T, int64_t LOCAL_ARRAY_SIZE, int64_t ARRAY_ARRAY_SIZE, typename BlockAllocatorT>
int64_t ObArrayArray<T, LOCAL_ARRAY_SIZE, ARRAY_ARRAY_SIZE, BlockAllocatorT>::get_serialize_size()
const
{
  int64_t size = 0;
  size += serialization::encoded_length_vi64(count());
  for (int64_t i = 0; i < count(); i++) {
    const LocalArrayT &item = *array_ptrs_[i];
    size += serialization::encoded_length(item);
  }
  return size;
}


} // common
} // oceanbase
#endif
