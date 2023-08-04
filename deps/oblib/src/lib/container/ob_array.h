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

#ifndef _OB_ARRAY_H
#define _OB_ARRAY_H 1
#include "lib/container/ob_iarray.h"
#include "lib/worker.h"
#include "lib/json/ob_yson_encode.h"

namespace oceanbase
{
namespace common
{
template<typename T>
class ObArrayDefaultCallBack
{
public:
  void operator()(T *ptr)
  {
    UNUSED(ptr);
  }
};

template<typename T>
class ObArrayExpressionCallBack
{
public:
  void operator()(T *ptr)
  {
    ptr->reset();
  }
};

////////////////////////////////////////////////////////////////
template <class T>
struct NotImplementItemEncode
{
  static int encode(char *buf, const int64_t buf_len, int64_t &pos, const T &item)
  {
    UNUSED(buf);
    UNUSED(buf_len);
    UNUSED(pos);
    UNUSED(item);
    _OB_LOG_RET(WARN, OB_NOT_IMPLEMENT, "call not implemented function.");
    return OB_NOT_IMPLEMENT;
  }

  static int decode(const char *buf, int64_t data_len, int64_t &pos, T *item)
  {
    UNUSED(buf);
    UNUSED(data_len);
    UNUSED(pos);
    UNUSED(item);
    _OB_LOG_RET(WARN, OB_NOT_IMPLEMENT, "call not implemented function.");
    return OB_NOT_IMPLEMENT;
  }

  static int64_t encoded_length_item(const T &item)
  {
    UNUSED(item);
    return 0;
  }
};

template<typename T, typename BlockAllocatorT, bool auto_free, typename CallBack, typename ItemEncode>
class ObArrayImpl;
////////////////////////////////////////////////////////////////
namespace array
{
template <class ObArrayImpl, class T>
class Iterator;
}

////////////////////////////////////////////////////////////////
template<typename T, typename BlockAllocatorT = ModulePageAllocator, bool auto_free = false, typename CallBack = ObArrayDefaultCallBack<T>, typename ItemEncode = NotImplementItemEncode<T> >
class ObArrayImpl : public ObIArray<T>
{
  typedef ObArrayImpl<T, BlockAllocatorT, auto_free, CallBack, ItemEncode> self_t;
public:
  typedef array::Iterator<self_t, T> iterator;
  typedef array::Iterator<self_t, const T> const_iterator;
  typedef T value_type;

  using ObIArray<T>::count;
  using ObIArray<T>::at;
public:
  ObArrayImpl(int64_t block_size = OB_MALLOC_NORMAL_BLOCK_SIZE,
          const BlockAllocatorT &alloc = BlockAllocatorT(ObNewModIds::OB_COMMON_ARRAY));
  virtual ~ObArrayImpl() __attribute__((noinline));
  inline void set_label(const lib::ObLabel &label) { block_allocator_.set_label(label); }
  inline void set_attr(const lib::ObMemAttr &attr) { block_allocator_.set_attr(attr); }
  inline void set_tenant_id(const uint64_t &tenant_id) { block_allocator_.set_tenant_id(tenant_id); }
  inline void set_block_size(const int64_t block_size) { block_size_ = block_size; }
  inline int64_t get_block_size() const {return block_size_; }
  inline void set_block_allocator(const BlockAllocatorT &alloc) { block_allocator_ = alloc; }
  inline const BlockAllocatorT   &get_block_allocator() const {return block_allocator_;}
  int push_back(const T &obj);
  void pop_back();
  int pop_back(T &obj);
  int remove(const int64_t idx);

  inline int at(const int64_t idx, T &obj) const
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(OB_SUCCESS != error_)) {
      ret = OB_ERROR;
      _OB_LOG(ERROR, "array in error state");
    } else if (OB_UNLIKELY(0 > idx || idx >= count_)) {
      ret = OB_ARRAY_OUT_OF_RANGE;
    } else {
      if(OB_FAIL(copy_assign(obj, data_[idx]))) {
        LIB_LOG(WARN, "failed to copy obj", K(ret));
      }
    }
    return ret;
  }

  inline void extra_access_check(void) const override { OB_ASSERT(OB_SUCCESS == error_); }

  inline T &operator[](const int64_t idx) {return at(idx);}
  inline const T &operator[](const int64_t idx) const {return at(idx);}
  T *alloc_place_holder();
  inline int64_t size() const { return count();}
  void reuse()
  {
    CallBack cb;
    if (data_size_ <= block_size_) {
      do_clear_func(cb);
    } else {
      destroy();
    }
  }
  inline void reset() {destroy();}
  void destroy();
  inline int reserve(int64_t capacity)
  {
    int ret = OB_SUCCESS;
    if (capacity > data_size_ / (int64_t)sizeof(T)) {
      int64_t new_size = capacity * sizeof(T);
      int64_t plus = new_size % block_size_;
      new_size += (0 == plus) ? 0 : (block_size_ - plus);
      ret = extend_buf(new_size);
    }
    return ret;
  }
  //prepare allocate can avoid declaring local data
  int prepare_allocate(int64_t capacity)
  {
    int ret = OB_SUCCESS;
    ret = reserve(capacity);
    if (OB_SUCC(ret)) {
      for (int64_t index = valid_count_; index < capacity; ++index) {
        new(&data_[index]) T();
      }
      count_ = (capacity > count_) ? capacity : count_;
      valid_count_ = (capacity > valid_count_) ? capacity : valid_count_;
    } else {
      OB_LOG(WARN, "Reserve capacity error", K(ret));
    }
    return ret;
  }
  int64_t to_string(char *buffer, int64_t length) const;
  inline int64_t get_data_size() const {return data_size_;}
  inline bool error() const {return error_ != OB_SUCCESS;}
  inline int get_copy_assign_ret() const { return error_; }
  const_iterator begin() const;
  const_iterator end() const;
  iterator begin();
  iterator end();
  inline const ObArrayImpl &get_const_ref()
  {
    return *this;
  }
  inline int64_t get_capacity() const { return data_size_ / static_cast<int64_t>(sizeof(T)); }
  inline int mprotect_data(int prot)
  {
    int ret = OB_SUCCESS;
    // if (NULL == data_) {
    //   // do nothing, return OB_SUCCESS
    //   LIB_LOG(WARN, "mprotect data_ while data_ is NULL", K(ret), K(lbt()));
    // } else if (OB_SUCCESS != (ret = ob_mprotect(data_, prot))) {
    //   LIB_LOG(WARN, "fail to mprotect data_", K(ret), K(data_), K(lbt()));
    // }
    UNUSED(prot);
    return ret;
  }
  // deep copy
  ObArrayImpl(const ObArrayImpl &other);
  ObArrayImpl &operator=(const ObArrayImpl &other);
  int assign(const ObIArray<T> &other);
  NEED_SERIALIZE_AND_DESERIALIZE;

  static uint32_t data_offset_bits() { return offsetof(ObArrayImpl, data_) * 8; }
  static uint32_t count_offset_bits() { return offsetof(ObArrayImpl, count_) * 8; }
  static uint32_t valid_count_offset_bits() { return offsetof(ObArrayImpl, valid_count_) * 8; }
  static uint32_t data_size_offset_bits() { return offsetof(ObArrayImpl, data_size_) * 8; }
  static uint32_t block_size_offset_bits() { return offsetof(ObArrayImpl, block_size_) * 8; }
  static uint32_t error_offset_bits() { return offsetof(ObArrayImpl, error_) * 8; }
  static uint32_t reserve_offset_bits() { return offsetof(ObArrayImpl, reserve_) * 8; }

protected:
  using ObIArray<T>::data_;
  using ObIArray<T>::count_;

private:
  inline int extend_buf()
  {
    int64_t new_size = MAX(2 * data_size_, block_size_);
    return extend_buf(new_size);
  }
  inline int extend_buf(int64_t new_size)
  {
    int ret = OB_SUCCESS;
    OB_ASSERT(new_size > data_size_);
    T *new_data = (T *)block_allocator_.alloc(new_size);
    if (NULL != new_data) {
      if (NULL != data_) {
        int64_t failed_idx = 0;
        for (int64_t i = 0; OB_SUCC(ret) && i < count_; ++i) {
          if (OB_FAIL(construct_assign(new_data[i], data_[i]))) {
            LIB_LOG(WARN, "failed to copy new_data", K(ret));
            failed_idx = i;
          }
        }
        if (OB_SUCC(ret)) {
          for (int64_t i = 0; i < valid_count_; ++i) {
            data_[i].~T();
          }
          valid_count_ = count_;
          block_allocator_.free(data_);
        } else {
          // if failed when construct_assign data, keep the array being the same as before
          for (int64_t i = 0; i < failed_idx; ++i) {
            new_data[i].~T();
          }
          block_allocator_.free(new_data);
          new_data = NULL;
        }
      }
      if (0 < data_size_) {
        _OB_LOG(DEBUG, "array extend buf, old_size=%ld new_size=%ld old_ptr=%p new_ptr=%p "
                  "count=%ld obj_size=%ld block_size=%ld",
                  data_size_, new_size, data_, new_data,
                  count_, static_cast<int64_t>(sizeof(T)), block_size_);
      }
      if (OB_SUCC(ret)) {
        data_size_ = new_size;
        data_ = new_data;
      }
    } else {
      _OB_LOG(ERROR, "no memory");
      ret = OB_ALLOCATE_MEMORY_FAILED;
    }
    return ret;
  }
  inline void do_clear_func(ObArrayDefaultCallBack<T> &cb)
  {
    UNUSED(cb);
    count_ = 0;
    error_ = OB_SUCCESS;
  }
  template <class CB>
  inline void do_clear_func(CB &cb)
  {
    for (int64_t index = 0; index < count_; ++index) {
      T *obj = &data_[index];
      cb(obj);
    }
    count_ = 0;
    error_ = OB_SUCCESS;
  }
private:
  int64_t valid_count_; // constructed item count
  int64_t data_size_;
  int64_t block_size_;
  int error_;
  int32_t reserve_;
  BlockAllocatorT block_allocator_;
};


template<typename T, typename BlockAllocatorT, bool auto_free, typename CallBack, typename ItemEncode>
ObArrayImpl<T, BlockAllocatorT, auto_free, CallBack, ItemEncode>::ObArrayImpl(int64_t block_size,
                                                           const BlockAllocatorT &alloc)
    : ObIArray<T>(), valid_count_(0),
      data_size_(0), block_size_(block_size),
      error_(OB_SUCCESS), reserve_(0),
      block_allocator_(alloc)
{
  block_size_ = std::max(static_cast<int64_t>(sizeof(T)), block_size);

  // if (lib::this_worker().has_req_flag()) {
    if (auto_free) {
      set_block_allocator(
          BlockAllocatorT(static_cast<ObIAllocator&>(
                              lib::this_worker().get_sql_arena_allocator())));
    }
  // }
}

template<typename T, typename BlockAllocatorT, bool auto_free, typename CallBack, typename ItemEncode>
ObArrayImpl<T, BlockAllocatorT, auto_free, CallBack, ItemEncode>::~ObArrayImpl()
{
  destroy();
}

template<typename T, typename BlockAllocatorT, bool auto_free, typename CallBack, typename ItemEncode>
T *ObArrayImpl<T, BlockAllocatorT, auto_free, CallBack, ItemEncode>::alloc_place_holder()
{
  T *ret = NULL;
  if (OB_UNLIKELY(count_ >= data_size_ / (int64_t)sizeof(T))) {
    (void)extend_buf();
  }
  if (count_ < data_size_ / (int64_t)sizeof(T)) {
    if (count_ < valid_count_) {
      ret = &data_[count_];
    } else {
      ret = new(&data_[count_]) T();
      valid_count_++;
    }
    count_++;
  } else {
    _OB_LOG_RET(WARN, OB_ALLOCATE_MEMORY_FAILED, "extend buf error, "
              "count_=%ld, data_size_=%ld, (int64_t)sizeof(T)=%ld, data_size_/(int64_t)sizeof(T)=%ld",
              count_, data_size_, static_cast<int64_t>(sizeof(T)), data_size_ / static_cast<int64_t>(sizeof(T)));
  }
  return ret;
}

template<typename T, typename BlockAllocatorT, bool auto_free, typename CallBack, typename ItemEncode>
int64_t ObArrayImpl<T, BlockAllocatorT, auto_free, CallBack, ItemEncode>::to_string(char *buf,
                                                                     int64_t buf_len) const
{
  int64_t pos = 0;
  J_ARRAY_START();
  for (int64_t index = 0; index < count_ - 1; ++index) {
    BUF_PRINTO(at(index));
    J_COMMA();
  }
  if (0 < count_) {
    BUF_PRINTO(at(count_ - 1));
  }
  J_ARRAY_END();
  return pos;
}

template<typename T, typename BlockAllocatorT, bool auto_free, typename CallBack, typename ItemEncode>
int ObArrayImpl<T, BlockAllocatorT, auto_free, CallBack, ItemEncode>::push_back(const T &obj)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_SUCCESS != error_)) {
    ret = OB_ERROR;
    _OB_LOG(ERROR, "array in error state");
  } else if (count_ >= data_size_ / (int64_t)sizeof(T)) {
    ret = extend_buf();
  }
  if (OB_SUCCESS == ret && (count_ < data_size_ / (int64_t)sizeof(T))) {
    if (count_ < valid_count_) {
      if (OB_FAIL(copy_assign(data_[count_], obj))) {
        LIB_LOG(WARN, "failed to copy data", K(ret));
      } else {
        ++count_;
      }
    } else {
      if (OB_FAIL(construct_assign(data_[count_], obj))) {
        LIB_LOG(WARN, "failed to copy data", K(ret));
      } else {
        ++valid_count_;
        ++count_;
      }
    }
  } else {
    if (OB_SUCC(ret)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    }
    _OB_LOG(WARN, "count_=%ld, data_size_=%ld, (int64_t)sizeof(T)=%ld, data_size_/(int64_t)sizeof(T)=%ld, ret=%d",
              count_, data_size_, static_cast<int64_t>(sizeof(T)), data_size_ / static_cast<int64_t>(sizeof(T)),
              ret);
  }
  return ret;
}

template<typename T, typename BlockAllocatorT, bool auto_free, typename CallBack, typename ItemEncode>
void ObArrayImpl<T, BlockAllocatorT, auto_free, CallBack, ItemEncode>::pop_back()
{
  if (0 < count_) {
    count_--;
  }
}

template<typename T, typename BlockAllocatorT, bool auto_free, typename CallBack, typename ItemEncode>
int ObArrayImpl<T, BlockAllocatorT, auto_free, CallBack, ItemEncode>::pop_back(T &obj)
{
  int ret = OB_ENTRY_NOT_EXIST;
  if (0 < count_) {
    if (OB_FAIL(copy_assign(obj, data_[--count_]))) {
      LIB_LOG(WARN, "failed to copy data", K(ret));
    }
  }
  return ret;
}

template<typename T, typename BlockAllocatorT, bool auto_free, typename CallBack, typename ItemEncode>
int ObArrayImpl<T, BlockAllocatorT, auto_free, CallBack, ItemEncode>::remove(int64_t idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 > idx || idx >= count_)) {
    ret = OB_ARRAY_OUT_OF_RANGE;
  } else {
    for (int64_t i = idx; OB_SUCC(ret) && i < count_ - 1; ++i) {
      if (OB_FAIL(copy_assign(data_[i], data_[i + 1]))) {
        LIB_LOG(WARN, "failed to copy data", K(ret));
      }
    }
    count_--;
  }
  return ret;
}

template<typename T, typename BlockAllocatorT, bool auto_free, typename CallBack, typename ItemEncode>
ObArrayImpl<T, BlockAllocatorT, auto_free, CallBack, ItemEncode>::ObArrayImpl(const
                                                           ObArrayImpl<T, BlockAllocatorT, auto_free, CallBack, ItemEncode> &other)
    : ObIArray<T>(), valid_count_(0),
      data_size_(0), block_size_(other.block_size_),
      error_(OB_SUCCESS), reserve_(0),
      block_allocator_(other.block_allocator_)
{
  *this = other;
}

template<typename T, typename BlockAllocatorT, bool auto_free, typename CallBack, typename ItemEncode>
ObArrayImpl<T, BlockAllocatorT, auto_free, CallBack, ItemEncode>
&ObArrayImpl<T, BlockAllocatorT, auto_free, CallBack, ItemEncode>::operator=(const
                                                              ObArrayImpl<T, BlockAllocatorT, auto_free, CallBack, ItemEncode> &other)
{
  if (this != &other) {
    this->reset();
    this->reserve(other.count());
    if (OB_UNLIKELY(static_cast<uint64_t>(data_size_) < (sizeof(T)*other.count_))) {
      error_ = OB_ALLOCATE_MEMORY_FAILED;
      _OB_LOG_RET(ERROR, error_, "no memory");
    } else {
      const int64_t assign = std::min(valid_count_, other.count_);
      for (int64_t i = 0; OB_LIKELY(OB_SUCCESS == error_) && i < assign; ++i) {
        if (OB_UNLIKELY(OB_SUCCESS != (error_ = copy_assign(data_[i], other.data_[i])))) {
          LIB_LOG_RET(WARN, error_, "failed to copy data", K(error_));
          count_ = i;
        }
      }
      for (int64_t i = assign; OB_LIKELY(OB_SUCCESS == error_) && i < other.count_; ++i) {
        if (OB_UNLIKELY(OB_SUCCESS != (error_ = construct_assign(data_[i], other.data_[i])))) {
          LIB_LOG_RET(WARN, error_, "failed to copy data", K(error_));
          count_ = i;
        }
      }
      if (OB_LIKELY(OB_SUCCESS == error_)) {
        count_ = other.count_;
      }
      if (valid_count_ < count_) {
        valid_count_ = count_;
      }
    }
  }
  return *this;
}

template<typename T, typename BlockAllocatorT, bool auto_free, typename CallBack, typename ItemEncode>
int ObArrayImpl<T, BlockAllocatorT, auto_free, CallBack, ItemEncode>::assign(const ObIArray<T> &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    this->reset();
    int64_t N = other.count();
    (void)this->reserve(other.count());
    if (OB_UNLIKELY(static_cast<uint64_t>(data_size_) < (sizeof(T)*N))) {
      _OB_LOG(WARN, "no memory");
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      const int64_t assign = std::min(valid_count_, N);
      for (int64_t i = 0; OB_SUCC(ret) && i < assign; ++i) {
        if (OB_FAIL(copy_assign(data_[i], other.at(i)))) {
          LIB_LOG(WARN, "failed to copy data", K(ret));
          count_ = i;
        }
      }
      for (int64_t i = assign; OB_SUCC(ret) && i < N; ++i) {
        if (OB_FAIL(construct_assign(data_[i], other.at(i)))) {
          LIB_LOG(WARN, "failed to copy data", K(ret));
          count_ = i;
        }
      }
      if (OB_SUCC(ret)) {
        count_ = N;
      }
      if (valid_count_ < count_) {
        valid_count_ = count_;
      }
    }
  }
  return ret;
}

template<typename T, typename BlockAllocatorT, bool auto_free, typename CallBack, typename ItemEncode>
void ObArrayImpl<T, BlockAllocatorT, auto_free, CallBack, ItemEncode>::destroy()
{
  if (NULL != data_) {
    for (int i = 0; i < valid_count_; ++i) {
      data_[i].~T();
    }
    block_allocator_.free(data_);
    data_ = NULL;
  }
  count_ = data_size_ = 0;
  valid_count_ = 0;
  error_ = OB_SUCCESS;
  reserve_ = 0;
}


template<class T, class BlockAllocatorT, bool auto_free, class CallBack, class ItemEncode>
inline int databuff_encode_element(char *buf, const int64_t buf_len, int64_t &pos,
                                   oceanbase::yson::ElementKeyType key,
                                   const ObArrayImpl<T, BlockAllocatorT, auto_free, CallBack, ItemEncode> &array)
{
  return oceanbase::yson::databuff_encode_array_element(buf, buf_len, pos, key, array);
}

template<typename T, typename BlockAllocatorT = ModulePageAllocator, bool auto_free = false, typename CallBack = ObArrayDefaultCallBack<T>, typename ItemEncode = NotImplementItemEncode<T> >
class ObArray final : public ObArrayImpl<T, BlockAllocatorT, auto_free, CallBack, ItemEncode>
{
public:
  // use ObArrayImpl constructors
  using ObArrayImpl<T, BlockAllocatorT, auto_free, CallBack, ItemEncode>::ObArrayImpl;
};

} // end namespace common
} // end namespace oceanbase

#endif /* _OB_ARRAY_H */
