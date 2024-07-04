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

#ifndef OCEANBASE_LIB_CONTAINER_SE_ARRAY_
#define OCEANBASE_LIB_CONTAINER_SE_ARRAY_ 1
#include <typeinfo>
#include "lib/container/ob_array.h"
#include "lib/allocator/page_arena.h"         // for ModulePageAllocator
#include "lib/container/ob_iarray.h"
#include "lib/utility/ob_template_utils.h"
#include "lib/json/ob_yson_encode.h"

namespace oceanbase
{
namespace common
{
namespace array
{
template <typename T, int64_t LOCAL_ARRAY_SIZE,
          typename BlockAllocatorT, bool auto_free>
class ObSEArrayIterator;
}

// ObNullAllocator cannot alloc buffer
class ObNullAllocator: public ObIAllocator
{
public:
  ObNullAllocator(const lib::ObLabel &label = ObModIds::OB_MOD_DO_NOT_USE_ME,
      int64_t tenant_id = OB_SERVER_TENANT_ID)
  {
    UNUSED(label);
    UNUSED(tenant_id);
  }
  ObNullAllocator(ObIAllocator &allocator)
  {
    UNUSED(allocator);
  }
  virtual void *alloc(const int64_t sz) override
  {
    UNUSEDx(sz);
    return nullptr;
  }
  virtual void *alloc(const int64_t sz, const ObMemAttr &attr) override
  {
    UNUSEDx(sz, attr);
    return nullptr;
  }

  virtual void free(void *p) override { UNUSED(p); }

  virtual ~ObNullAllocator() {};
};

template<typename BlockAllocatorT>
static inline void init_block_allocator(lib::MemoryContext &mem_context, BlockAllocatorT &block_allocator)
{
  block_allocator = BlockAllocatorT(mem_context->get_allocator());
}
static inline void init_block_allocator(lib::MemoryContext &mem_context, ModulePageAllocator &block_allocator)
{
  block_allocator = ModulePageAllocator(mem_context->get_allocator(), block_allocator.get_label());
}
static inline void init_block_allocator(lib::MemoryContext &mem_context, ObIAllocator &block_allocator)
{
  // this implement is invalid, just for compilation.
  // protected by static_assert.
  UNUSED(mem_context);
  UNUSED(block_allocator);
}


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
      LIB_LOG(WARN, "failed to copy data", K(ret));
    }
    return ret;
  }
  OB_INLINE static int assign_and_destruct(T &src, T &dst)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(copy_assign(dst, src))) {
      LIB_LOG(WARN, "failed to copy data", K(ret));
    }
    src.~T();
    return ret;
  }
  OB_INLINE static int construct_and_assign(const T &src, T &addr)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(construct_assign(addr, src))) {
      LIB_LOG(WARN, "failed to copy data", K(ret));
    }
    return ret;
  }
  OB_INLINE static int array_remove(T* arr, int64_t &arr_size, int64_t idx)
  {
    int ret = OB_SUCCESS;
    for (int64_t i = idx; OB_SUCC(ret) && i < arr_size - 1; ++i) {
        if (OB_FAIL(copy_assign(arr[i], arr[i + 1]))) {
          LIB_LOG(WARN, "failed to copy data", K(ret));
        }
    }
    if (OB_SUCC(ret)) {
      arr[--arr_size].~T();
    }
    return ret;
  }
  OB_INLINE static T* default_construct(T &addr)
  {
    return new(&addr) T();
  }
  OB_INLINE static void default_destruct(T &addr)
  {
    addr.~T();
  }
  OB_INLINE static void array_default_construct(T *addr, int64_t arr_size)
  {
    for (int64_t i = 0; i < arr_size; ++i) {
      new(addr + i) T();
    }
  }
  OB_INLINE static int array_assign(const T *src_arr, T *dst_arr, int64_t arr_size)
  {
    int ret = OB_SUCCESS;
    for (int64_t i = 0; OB_SUCC(ret) && i < arr_size; ++i) {
      if (OB_FAIL(construct_assign(dst_arr[i], src_arr[i]))) {
        LIB_LOG(WARN, "failed to copy data", K(ret), K(i));
      }
    }
    return ret;
  }
  OB_INLINE static int array_expand(const T *orig_arr, T *new_arr, int64_t orig_size)
  {
    int ret = OB_SUCCESS;
    int64_t fail_pos = -1;
    for (int64_t i = 0; OB_SUCC(ret) && i < orig_size; i++) {
      if (OB_FAIL(construct_assign(new_arr[i], orig_arr[i]))) {
        LIB_LOG(WARN, "failed to copy new_data", K(ret), K(i));
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
  OB_INLINE static void array_construct(T* arr, int64_t arr_size)
  {
    new (arr) T[arr_size];
  }
};

//ObSEArrayImpl is a high performant array for OceanBase developers,
//to guarantee performance, it should be used in this way
//1. remove is called unusually
//2. assign is usually called with empty array
static const int64_t OB_DEFAULT_SE_ARRAY_COUNT = 64;
template <typename T,
          int64_t LOCAL_ARRAY_SIZE,
          typename BlockAllocatorT = ModulePageAllocator,
          bool auto_free  = false>
class ObSEArrayImpl : public ObIArray<T>
{
  static_assert(std::is_constructible<BlockAllocatorT>::value || !auto_free,
                "BlockAllocatorT can not be constructed.");
public:

  using ObIArray<T>::count;
  using ObIArray<T>::at;
  using MyOp = ObClassOp<T, std::is_trivial<T>::value>;
  typedef array::ObSEArrayIterator<T,
                                   LOCAL_ARRAY_SIZE,
                                   BlockAllocatorT,
                                   auto_free> iterator;
#ifdef DISABLE_SE_ARRAY
  explicit ObSEArrayImpl(int64_t block_size = OB_MALLOC_NORMAL_BLOCK_SIZE,
            const BlockAllocatorT &alloc = BlockAllocatorT("SE_ARRAY_MEM_CK"));
#else
  explicit ObSEArrayImpl(int64_t block_size = OB_MALLOC_NORMAL_BLOCK_SIZE,
            const BlockAllocatorT &alloc = BlockAllocatorT(ObModIds::OB_SE_ARRAY));
#endif
  ObSEArrayImpl(const lib::ObLabel &label, int64_t block_size);
  virtual ~ObSEArrayImpl();

  // deep copy
  explicit ObSEArrayImpl(const ObSEArrayImpl &other);
  inline void set_attr(const lib::ObMemAttr &attr) { block_allocator_.set_attr(attr); }
  ObSEArrayImpl &operator=(const ObSEArrayImpl &other);
  int assign(const ObIArray<T> &other);

  int push_back(const T &obj);
  void pop_back();
  int pop_back(T &obj);
  int remove(int64_t idx);

  inline int at(int64_t idx, T &obj) const
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(0 > idx || idx >= count_)) {
      ret = OB_ARRAY_OUT_OF_RANGE;
    } else {
      ret = MyOp::assign(data_[idx], obj);
    }
    return ret;
  }
  void extra_access_check(void ) const override { OB_ASSERT(OB_SUCCESS == error_); }

  inline T &operator[](int64_t idx)  // dangerous
  {
    if (OB_UNLIKELY(0 > idx || idx >= count_)) {
      LIB_LOG_RET(ERROR, OB_ARRAY_OUT_OF_RANGE, "idx out of range", K(idx), K(count_));
    }
    return data_[idx];
  }
  inline const T &operator[](int64_t idx) const  // dangerous
  {
    if (OB_UNLIKELY(0 > idx || idx >= count_)) {
      LIB_LOG_RET(ERROR, OB_ARRAY_OUT_OF_RANGE, "idx out of range", K(idx), K(count_));
    }
    return data_[idx];
  }
  T *alloc_place_holder();

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
        data_[i].~T();
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
      // memory size should be integer times of block_size_
      int64_t new_size = capacity * sizeof(T);
      int64_t plus = new_size % block_size_;
      new_size += (0 == plus) ? 0 : (block_size_ - plus);
      T *new_data = reinterpret_cast<T*>(internal_malloc_(new_size));
      if (OB_NOT_NULL(new_data)) {
        ret = MyOp::array_expand(data_, new_data, count_);
        if (OB_SUCC(ret)) {
          if (reinterpret_cast<T *>(local_data_buf_) != data_) {
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
        LIB_LOG(ERROR, "no memory", K(ret), K(new_size), K(block_size_), K(count_), K(capacity_));
      }
    }
    return ret;
  }
  //prepare allocate can avoid declaring local data
  int prepare_allocate(int64_t capacity);
  template<typename ... Args>
  int prepare_allocate(int64_t capacity, Args && ... args);
  int64_t to_string(char *buffer, int64_t length) const;
  inline int64_t get_data_size() const
  {
    int64_t data_size = capacity_ * sizeof(T);
#ifdef DISABLE_SE_ARRAY
    if (typeid(BlockAllocatorT) != typeid(ObNullAllocator)) {
#else
    if (capacity_ > LOCAL_ARRAY_SIZE) {
#endif
      int64_t plus = data_size % block_size_;
      data_size += (0 == plus) ? 0 : (block_size_ - plus);
    }
    return data_size;
  }
  inline bool error() const {return error_ != OB_SUCCESS;}
  inline int get_copy_assign_ret() const { return error_;}
  inline int64_t get_capacity() const
  {
    return capacity_;
  }
  NEED_SERIALIZE_AND_DESERIALIZE;

  static uint32_t local_data_offset_bits() { return offsetof(ObSEArrayImpl, data_) * 8; }
  static uint32_t local_data_buf_offset_bits() { return offsetof(ObSEArrayImpl, local_data_buf_) * 8; }
  static uint32_t block_size_offset_bits() { return offsetof(ObSEArrayImpl, block_size_) * 8; }
  static uint32_t count_offset_bits() { return offsetof(ObSEArrayImpl, count_) * 8; }
  static uint32_t capacity_offset_bits() { return offsetof(ObSEArrayImpl, capacity_) * 8; }
  static uint32_t max_print_count_offset_bits() { return offsetof(ObSEArrayImpl, max_print_count_) * 8; }
  static uint32_t error_offset_bits() { return offsetof(ObSEArrayImpl, error_) * 8; }
  static uint32_t allocator_offset_bits() { return offsetof(ObSEArrayImpl, block_allocator_) * 8; }
  static uint32_t has_alloc_offset_bits() { return offsetof(ObSEArrayImpl, has_alloc_) * 8; }
  static uint32_t mem_context_offset_bits() { return offsetof(ObSEArrayImpl, mem_context_) * 8; }
public:
  iterator begin();
  iterator end();
private:
  // types and constants
  static const int64_t DEFAULT_MAX_PRINT_COUNT = 32;
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
    if (OB_UNLIKELY(!has_alloc_)) {
      if (auto_free) {
        mem_context_ = CURRENT_CONTEXT;
        init_block_allocator(mem_context_, block_allocator_);
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

protected:
  using ObIArray<T>::data_;
  using ObIArray<T>::count_;
private:
  char local_data_buf_[LOCAL_ARRAY_SIZE * sizeof(T)];
  int64_t block_size_;
  int64_t capacity_;
  int64_t max_print_count_; //max count that needs to be printed
  int error_;
  BlockAllocatorT block_allocator_;
  bool has_alloc_;
  lib::MemoryContext mem_context_;
};

template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT, bool auto_free>
int ObSEArrayImpl<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, auto_free>::serialize(
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
int ObSEArrayImpl<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, auto_free>::deserialize(
    const char *buf, int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t count = 0;
  reset();
  if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &count))) {
    LIB_LOG(WARN, "fail to decode ob array count", K(ret));
  } else if (OB_SUCCESS != (ret = prepare_allocate(count))) {
    LIB_LOG(WARN, "fail to allocate space", K(ret), K(count));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < count; i ++) {
    T &item = at(i);
    if (OB_SUCCESS != (ret = serialization::decode(buf, data_len, pos, item))) {
      LIB_LOG(WARN, "fail to decode array item", K(ret), K(i), K(count));
    }
  }
  return ret;
}

template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT, bool auto_free>
int64_t ObSEArrayImpl<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, auto_free>::get_serialize_size()
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
ObSEArrayImpl<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, auto_free>::ObSEArrayImpl(
    int64_t block_size, const BlockAllocatorT &alloc)
    : ObIArray<T>(reinterpret_cast<T *>(local_data_buf_), 0),
      block_size_(block_size),
      capacity_(LOCAL_ARRAY_SIZE),
      max_print_count_(DEFAULT_MAX_PRINT_COUNT),
      error_(OB_SUCCESS),
      block_allocator_(alloc),
      has_alloc_(false),
      mem_context_(nullptr)
{
#ifdef DISABLE_SE_ARRAY
  if (typeid(BlockAllocatorT) != typeid(ObNullAllocator)) {
    capacity_ = 0;
  }
#endif
  STATIC_ASSERT(LOCAL_ARRAY_SIZE > 0, "se local array size invalid");
  block_size_ = std::max(static_cast<int64_t>(sizeof(T)), block_size);
}

template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT, bool auto_free>
ObSEArrayImpl<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, auto_free>::ObSEArrayImpl(
    const lib::ObLabel &label, int64_t block_size)
    : ObIArray<T>(reinterpret_cast<T *>(local_data_buf_), 0),
      block_size_(block_size),
      capacity_(LOCAL_ARRAY_SIZE),
      max_print_count_(DEFAULT_MAX_PRINT_COUNT),
      error_(OB_SUCCESS),
      block_allocator_(label),
      has_alloc_(false),
      mem_context_(nullptr)
{
#ifdef DISABLE_SE_ARRAY
  if (typeid(BlockAllocatorT) != typeid(ObNullAllocator)) {
    capacity_ = 0;
    block_allocator_.set_label("SE_ARRAY_MEM_CK");
  }
#endif
  STATIC_ASSERT(LOCAL_ARRAY_SIZE > 0, "se local array size invalid");
  block_size_ = std::max(static_cast<int64_t>(sizeof(T)), block_size);
}

template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT, bool auto_free>
ObSEArrayImpl<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, auto_free>::~ObSEArrayImpl()
{
  destroy();
}

template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT, bool auto_free>
int ObSEArrayImpl<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, auto_free>::push_back(
    const T &obj)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(count_ == capacity_)) {
    int64_t new_capacity = 2 * capacity_;
#ifdef DISABLE_SE_ARRAY
    if (new_capacity == 0) {
      new_capacity = LOCAL_ARRAY_SIZE;
    }
#endif
    ret = reserve(new_capacity);
  }

  if (OB_SUCC(ret) && OB_SUCC(MyOp::construct_and_assign(obj, data_[count_]))) {
    count_++;
  }

  return ret;
}

template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT, bool auto_free>
void ObSEArrayImpl<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, auto_free>::pop_back()
{
  if (OB_UNLIKELY(count_ <= 0)) {
  } else {
    MyOp::default_destruct(data_[--count_]);
  }
}

template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT, bool auto_free>
int ObSEArrayImpl<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, auto_free>::pop_back(
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

template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT, bool auto_free>
int ObSEArrayImpl<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, auto_free>::remove(
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

template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT, bool auto_free>
inline  T *ObSEArrayImpl<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, auto_free>::alloc_place_holder()
{
  int ret = OB_SUCCESS;
  T *ptr = NULL;

  if (OB_UNLIKELY(count_ == capacity_)) {
    int64_t new_capacity = 2 * capacity_;
#ifdef DISABLE_SE_ARRAY
    if (new_capacity == 0) {
      new_capacity = LOCAL_ARRAY_SIZE;
    }
#endif
    ret = reserve(new_capacity);
  }

  if (OB_SUCC(ret)) {
    ptr = MyOp::default_construct(data_[count_++]);
  }

  return ptr;
}

template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT, bool auto_free>
void ObSEArrayImpl<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, auto_free>::destroy()
{
  if (is_destructor_safe()) {
  } else {
    for (int64_t i = 0; i < count_; i++) {
      data_[i].~T();
    }
  }

  if (data_ != reinterpret_cast<T *>(local_data_buf_)) {
    internal_free_(data_);
    data_ = reinterpret_cast<T *>(local_data_buf_);
  }
  count_ = 0;
#ifndef DISABLE_SE_ARRAY
  capacity_ = LOCAL_ARRAY_SIZE;
#else
  if (typeid(BlockAllocatorT) != typeid(ObNullAllocator)) {
    capacity_ = 0;
  } else {
    capacity_ = LOCAL_ARRAY_SIZE;
  }
#endif
  max_print_count_ = DEFAULT_MAX_PRINT_COUNT;
  error_ = OB_SUCCESS;
}

template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT, bool auto_free>
inline int
ObSEArrayImpl<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, auto_free>::prepare_allocate(
    int64_t capacity)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(capacity_ < capacity)) {
    ret = reserve(capacity);
  }
  if (OB_SUCC(ret)) {
    if (capacity > count_) {
      MyOp::array_default_construct(data_ + count_, capacity - count_);
      count_ = capacity;
    }
  }
  return ret;
}

template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT, bool auto_free>
int64_t ObSEArrayImpl<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, auto_free>::to_string(
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
ObSEArrayImpl<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, auto_free>
&ObSEArrayImpl<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, auto_free>::operator=
(const ObSEArrayImpl<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, auto_free> &other)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(this != &other)) {
    this->reuse();
    int64_t N = other.count();
    ret = reserve(N);
    for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
      if (OB_FAIL(construct_assign(data_[i], other.data_[i]))) {
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
int ObSEArrayImpl<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, auto_free>::assign(const ObIArray<T> &other)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(this != &other)) {
    this->reuse();
    int64_t N = other.count();
    ret = reserve(N);
    if (OB_SUCC(ret)) {
      if (typeid(other).hash_code() == typeid(this).hash_code()) {
        ret = MyOp::array_assign(
            static_cast<const ObSEArrayImpl<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, auto_free>*>(&other)->data_, data_, N);
      } else {
        // use construct_assign
        ret = ObClassOp<T, false>::array_assign(
            static_cast<const ObSEArrayImpl<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, auto_free>*>(&other)->data_, data_, N);
      }
    }
    if (OB_SUCC(ret)) {
      count_ = N;
    }
  }
  return ret;
}

template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT, bool auto_free>
ObSEArrayImpl<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, auto_free>::ObSEArrayImpl(
    const ObSEArrayImpl<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, auto_free> &other)
    : ObIArray<T>(reinterpret_cast<T *>(local_data_buf_), 0),
      block_size_(other.block_size_),
      capacity_(LOCAL_ARRAY_SIZE),
      max_print_count_(DEFAULT_MAX_PRINT_COUNT),
      error_(OB_SUCCESS),
      block_allocator_(),
      has_alloc_(false),
      mem_context_(nullptr)
{
#ifdef DISABLE_SE_ARRAY
  if (typeid(BlockAllocatorT) != typeid(ObNullAllocator)) {
    capacity_ = 0;
  }
#endif
  *this = other;
}

template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT, bool auto_free>
template<typename ... Args>
inline int
ObSEArrayImpl<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, auto_free>::prepare_allocate(
    int64_t capacity, Args && ... args)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(capacity_ < capacity)) {
    ret = reserve(capacity);
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

template <typename T,
          int64_t LOCAL_ARRAY_SIZE,
          typename BlockAllocatorT = ModulePageAllocator,
          bool auto_free  = false>
class ObSEArray final : public ObSEArrayImpl<T,
                                             LOCAL_ARRAY_SIZE,
                                             BlockAllocatorT,
                                             auto_free>
{
public:
  using ObSEArrayImpl<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, auto_free>::ObSEArrayImpl;
};

template <typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT, bool auto_free>
inline int databuff_encode_element(char *buf, const int64_t buf_len, int64_t &pos,
                                   oceanbase::yson::ElementKeyType key,
                                   const ObSEArray<T, LOCAL_ARRAY_SIZE,
                                   BlockAllocatorT, auto_free> &array)
{
  return oceanbase::yson::databuff_encode_array_element(buf, buf_len, pos, key, array);
}


} // end namespace common
} // end namespace oceanbase

#endif /* OCEANBASE_LIB_CONTAINER_SE_ARRAY_ */
