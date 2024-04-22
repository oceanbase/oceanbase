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

#ifndef OCEANBASE_COMMON_OB_MALLOC_H_
#define OCEANBASE_COMMON_OB_MALLOC_H_
#include <stdint.h>
#include "lib/allocator/ob_allocator.h"
#include "lib/allocator/ob_mod_define.h"
#include "lib/allocator/ob_tc_malloc.h"
#include "lib/time/ob_time_utility.h"
#include "lib/alloc/ob_malloc_allocator.h"

namespace oceanbase
{
namespace common
{
inline void ob_print_mod_memory_usage(bool print_to_std = false,
                                      bool print_glibc_malloc_stats = false)
{
  UNUSEDx(print_to_std, print_glibc_malloc_stats);
}

inline void *ob_malloc(const int64_t nbyte, const ObMemAttr &attr)
{
  void *ptr = NULL;
  auto allocator = lib::ObMallocAllocator::get_instance();
  if (!OB_ISNULL(allocator)) {
    ptr = allocator->alloc(nbyte, attr);
    if (OB_ISNULL(ptr)) {
      LIB_LOG_RET(WARN, OB_ALLOCATE_MEMORY_FAILED, "allocate memory fail", K(attr), K(nbyte));
    }
  }
  return ptr;
}

inline void ob_free(void *ptr)
{
  if (OB_LIKELY(lib::ObMallocAllocator::is_inited_)) {
    auto *allocator = lib::ObMallocAllocator::get_instance();
    abort_unless(!OB_ISNULL(allocator));
    allocator->free(ptr);
    ptr = NULL;
  }
}

inline void *ob_realloc(void *ptr, const int64_t nbyte, const ObMemAttr &attr)
{
  void *nptr = NULL;
  if (OB_LIKELY(lib::ObMallocAllocator::is_inited_)) {
    auto *allocator = lib::ObMallocAllocator::get_instance();
    if (!OB_ISNULL(allocator)) {
      nptr = allocator->realloc(ptr, nbyte, attr);
      if (OB_ISNULL(nptr)) {
        LIB_LOG_RET(ERROR, OB_ALLOCATE_MEMORY_FAILED, "allocate memory fail", K(attr), K(nbyte));
      }
    }
  }
  return nptr;
}

void *ob_malloc_align(
    const int64_t alignment, const int64_t nbyte,
    const ObMemAttr &attr);
void ob_free_align(void *ptr);

// Deprecated interface
inline void *ob_malloc(const int64_t nbyte, const lib::ObLabel &label)
{
  ObMemAttr attr;
  attr.label_ = label;
  return ob_malloc(nbyte, attr);
}

// Deprecated interface
void *ob_malloc_align(
    const int64_t alignment,
    const int64_t nbyte, const lib::ObLabel &label);

////////////////////////////////////////////////////////////////
class ObMalloc : public ObIAllocator
{
public:
  ObMalloc() {};
  explicit ObMalloc(const lib::ObLabel &label)
  {
    memattr_.label_ = label;
  };
  explicit ObMalloc(ObMemAttr attr)
    : memattr_(attr)
  {}
  virtual ~ObMalloc() {};
public:
  void set_label(const lib::ObLabel &label) {memattr_.label_ = label;};
  void set_attr(const ObMemAttr &attr) { memattr_ = attr; }
  void *alloc(const int64_t sz)
  {
    return ob_malloc(sz, memattr_);
  }
  void *alloc(const int64_t size, const ObMemAttr &attr)
  {
    return ob_malloc(size, attr);
  }
  void free(void *ptr) { ob_free(ptr); };
private:
  ObMemAttr memattr_;
};
typedef ObMalloc ObTCMalloc;

class ObMemBuf
{
public:
  ObMemBuf()
    : buf_ptr_(NULL),
      buf_size_(OB_MALLOC_NORMAL_BLOCK_SIZE),
      label_(ObModIds::OB_MOD_DO_NOT_USE_ME)
  {

  }

  explicit ObMemBuf(const int64_t default_size)
    : buf_ptr_(NULL),
      buf_size_(default_size),
      label_(ObModIds::OB_MOD_DO_NOT_USE_ME)
  {

  }

  virtual ~ObMemBuf()
  {
    if (NULL != buf_ptr_) {
      ob_free(buf_ptr_);
      buf_ptr_ = NULL;
    }
  }

  inline char *get_buffer()
  {
    return buf_ptr_;
  }

  int64_t get_buffer_size() const
  {
    return buf_size_;
  }

  int ensure_space(const int64_t size, const lib::ObLabel &label = nullptr);

private:
  char *buf_ptr_;
  int64_t buf_size_;
  lib::ObLabel label_;
};

class ObMemBufAllocatorWrapper : public ObIAllocator
{
public:
  ObMemBufAllocatorWrapper(ObMemBuf &mem_buf, const lib::ObLabel &label = nullptr)
      : mem_buf_(mem_buf), label_(label) {}
public:
  virtual void *alloc(int64_t sz) override
  {
    char *ptr = NULL;
    if (OB_SUCCESS == mem_buf_.ensure_space(sz, label_)) {
      ptr = mem_buf_.get_buffer();
    }
    return ptr;
  }
  virtual void *alloc(int64_t sz, const ObMemAttr &attr) override
  {
    UNUSEDx(attr);
    return alloc(sz);
  }
  virtual void free(void *ptr) override
  {
    UNUSED(ptr);
  }
private:
  ObMemBuf &mem_buf_;
  lib::ObLabel label_;
};


class ObRawBufAllocatorWrapper : public ObIAllocator
{
public:
  ObRawBufAllocatorWrapper(char *mem_buf, int64_t mem_buf_len) : mem_buf_(mem_buf),
                                                                 mem_buf_len_(mem_buf_len) {}
public:
  virtual void *alloc(int64_t sz) override
  {
    char *ptr = NULL;
    if (mem_buf_len_ >= sz) {
      ptr = mem_buf_;
    }
    return ptr;
  }
  virtual void *alloc(int64_t sz, const ObMemAttr &attr) override
  {
    UNUSEDx(attr);
    return alloc(sz);
  }
  virtual void free(void *ptr) override
  {
    UNUSED(ptr);
  }
private:
  char *mem_buf_;
  int64_t mem_buf_len_;
};

template <typename T>
void ob_delete(T *&ptr)
{
  if (NULL != ptr) {
    ptr->~T();
    ob_free(ptr);
    ptr = NULL;
  }
}

}
}

extern "C" void *ob_zalloc(const int64_t nbyte);
extern "C" void ob_zfree(void *ptr);

#define OB_NEW(T, label, ...)                  \
  ({                                            \
    T* ret = NULL;                              \
    void *buf = oceanbase::common::ob_malloc(sizeof(T), label); \
    if (OB_NOT_NULL(buf))                       \
    {                                           \
      ret = new(buf) T(__VA_ARGS__);            \
    }                                           \
    ret;                                        \
  })

#define OB_NEW_ALIGN32(T, label, ...)          \
  ({                                            \
    T* ret = NULL;                              \
    void *buf = ob_malloc_align(32, sizeof(T), label);   \
    if (OB_NOT_NULL(buf))                       \
    {                                           \
      ret = new(buf) T(__VA_ARGS__);            \
    }                                           \
    ret;                                        \
  })

#define OB_NEWx(T, pool, ...)                   \
  ({                                            \
    T* ret = NULL;                              \
    if (OB_NOT_NULL(pool)) {                    \
      void *_buf_ = (pool)->alloc(sizeof(T));   \
      if (OB_NOT_NULL(_buf_))                   \
      {                                         \
        ret = new(_buf_) T(__VA_ARGS__);        \
      }                                         \
    }                                           \
    ret;                                        \
  })

#define OB_NEW_ARRAY(T, pool, count)            \
  ({                                            \
    T* ret = NULL;                              \
    if (OB_NOT_NULL(pool) && count > 0) {       \
      int64_t _size_ = sizeof(T) * count;       \
      void *_buf_ = (pool)->alloc(_size_);      \
      if (OB_NOT_NULL(_buf_))                   \
      {                                         \
        ret = new(_buf_) T[count];              \
      }                                         \
    }                                           \
    ret;                                        \
  })

#define OB_DELETE(T, label, ptr)               \
  do{                                           \
    if (NULL != ptr)                            \
    {                                           \
      ptr->~T();                                \
      ob_free(ptr);                             \
      ptr = NULL;                               \
    }                                           \
  } while(0)

#define OB_DELETE_ALIGN32(T, label, ptr)       \
  do{                                           \
    if (NULL != ptr)                            \
    {                                           \
      ptr->~T();                                \
      ob_free_align(ptr);                       \
      ptr = NULL;                               \
    }                                           \
  } while(0)



#endif /* OCEANBASE_SRC_COMMON_OB_MALLOC_H_ */
