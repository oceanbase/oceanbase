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

#ifndef  OCEANBASE_COMMON_IALLOCATOR_H_
#define  OCEANBASE_COMMON_IALLOCATOR_H_

#include "lib/ob_define.h"
#include "lib/alloc/alloc_struct.h"

namespace oceanbase
{
namespace common
{
using lib::ObMemAttr;
class ObIAllocator
{
public:
  /************************************************************************/
  /*                     New Interface (Under construction)               */
  /************************************************************************/
  // Use attr passed in by set_attr().
  virtual ~ObIAllocator() {};
  virtual void *alloc(const int64_t size) = 0;
  virtual void* alloc(const int64_t size, const ObMemAttr &attr) = 0;
  virtual void* realloc(const void *ptr, const int64_t size, const ObMemAttr &attr)
  {
    UNUSED(ptr);
    UNUSED(size);
    UNUSED(attr);
    return nullptr;
  }
  virtual void *realloc(void *ptr, const int64_t oldsz, const int64_t newsz)
  {
    UNUSED(ptr);
    UNUSED(oldsz);
    UNUSED(newsz);
    return nullptr;
  }
  virtual void free(void *ptr) = 0;
  virtual int64_t total() const
  {
    return 0;
  }
  virtual int64_t used() const
  {
    return 0;
  }
  virtual void reset() {}
  virtual void reuse() {}

  virtual void set_attr(const ObMemAttr &attr) { UNUSED(attr); }

  virtual ObIAllocator &operator=(const ObIAllocator &that) {
    UNUSED(that);
    return *this;
  }
};

extern ObIAllocator *global_default_allocator;

class ObWrapperAllocator: public ObIAllocator
{
public:
  explicit ObWrapperAllocator(ObIAllocator *alloc): alloc_(alloc) {};
  explicit ObWrapperAllocator(const lib::ObLabel &label): alloc_(NULL) {UNUSED(label);};
  explicit ObWrapperAllocator(ObIAllocator &alloc): alloc_(&alloc) { } // for ObArray::ObArray()
  ObWrapperAllocator(): alloc_(NULL) {};
  virtual ~ObWrapperAllocator() {};
  virtual void *alloc(int64_t sz, const ObMemAttr &attr)
  {
    return NULL == alloc_ ? NULL : alloc_->alloc(sz, attr);
  }
  virtual void *alloc(const int64_t sz)
  { return NULL == alloc_ ? NULL : alloc_->alloc(sz); }
  virtual void* realloc(const void *ptr, const int64_t size, const ObMemAttr &attr)
  { return NULL == alloc_ ? NULL : alloc_->realloc(ptr, size, attr); }

  virtual void *realloc(void *ptr, const int64_t oldsz, const int64_t newsz)
  { return NULL == alloc_ ? NULL : alloc_->realloc(ptr, oldsz, newsz); }

  void free(void *ptr)
  {
    if (NULL != alloc_) {
      alloc_->free(ptr); ptr = NULL;
    }
  }
  virtual int64_t total() const { return alloc_ != nullptr ? alloc_->total() : 0; }
  virtual int64_t used() const { return alloc_ != nullptr ? alloc_->used() : 0; }
  void set_alloc(ObIAllocator *alloc) { alloc_ = alloc; }
  ObWrapperAllocator &operator=(const ObWrapperAllocator &that)
  {
    if (this != &that) {
      alloc_ = that.alloc_;
    }
    return *this;
  }
  const ObIAllocator *get_alloc() const { return alloc_;}
  ObIAllocator *get_alloc() { return alloc_;}
  static uint32_t alloc_offset_bits()
  {
DISABLE_WARNING_GCC_PUSH
DISABLE_WARNING_GCC("-Winvalid-offsetof")
    return offsetof(ObWrapperAllocator, alloc_) * 8;
DISABLE_WARNING_GCC_POP
  }
private:
  // data members
  ObIAllocator *alloc_;
};

class ObWrapperAllocatorWithAttr: public ObWrapperAllocator
{
public:
  explicit ObWrapperAllocatorWithAttr(ObIAllocator *alloc, ObMemAttr attr = ObMemAttr())
    : ObWrapperAllocator(alloc), mem_attr_(attr) {};
  explicit ObWrapperAllocatorWithAttr(const lib::ObLabel &label)
    : ObWrapperAllocator(NULL), mem_attr_() { mem_attr_.label_ = label; };
  explicit ObWrapperAllocatorWithAttr(ObIAllocator &alloc, ObMemAttr attr = ObMemAttr())
    : ObWrapperAllocator(&alloc), mem_attr_(attr) {} // for ObArray::ObArray()
  ObWrapperAllocatorWithAttr(): ObWrapperAllocator(), mem_attr_() {};
  virtual ~ObWrapperAllocatorWithAttr() {};
  virtual void *alloc(const int64_t sz) { return ObWrapperAllocator::alloc(sz, mem_attr_); };
  const ObMemAttr &get_attr() const { return mem_attr_; }
  void set_attr(const ObMemAttr &attr) { mem_attr_ = attr; }
private:
  ObMemAttr mem_attr_;
};
}
}

#endif //OCEANBASE_COMMON_IALLOCATOR_H_
