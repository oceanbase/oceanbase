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

#ifndef OCEANBASE_COMMON_IALLOCATOR_H_
#define OCEANBASE_COMMON_IALLOCATOR_H_

#include "lib/ob_define.h"
#include "lib/alloc/alloc_struct.h"

namespace oceanbase {
namespace lib {
struct ObMemAttr;
}  // end of namespace lib

namespace common {
using lib::ObMemAttr;
extern ObMemAttr default_memattr;
class ObIAllocator {
public:
  virtual ~ObIAllocator(){};

public:
  /************************************************************************/
  /*                     New Interface (Under construction)               */
  /************************************************************************/
  // Use attr passed in by set_attr().
  virtual void* alloc(const int64_t size)
  {
    UNUSED(size);
    return NULL;
  }

  virtual void* alloc(const int64_t size, const ObMemAttr& attr)
  {
    UNUSED(size);
    UNUSED(attr);
    return NULL;
  }

  virtual void* realloc(const void* ptr, const int64_t size, const ObMemAttr& attr)
  {
    UNUSED(ptr);
    UNUSED(size);
    UNUSED(attr);
    return NULL;
  }

  virtual void* realloc(void* ptr, const int64_t oldsz, const int64_t newsz)
  {
    UNUSED(ptr);
    UNUSED(oldsz);
    UNUSED(newsz);
    return NULL;
  }

  virtual void free(void* ptr)
  {
    UNUSED(ptr);
  }

  virtual int64_t total() const
  {
    return 0;
  }
  virtual int64_t used() const
  {
    return 0;
  }
  virtual void reset()
  {}
  virtual void reuse()
  {}

  virtual void set_attr(const ObMemAttr& attr)
  {
    UNUSED(attr);
  }

  virtual ObIAllocator& operator=(const ObIAllocator& that)
  {
    UNUSED(that);
    return *this;
  }
};

extern ObIAllocator* global_default_allocator;

class ObWrapperAllocator : public ObIAllocator {
public:
  explicit ObWrapperAllocator(ObIAllocator* alloc) : alloc_(alloc){};
  explicit ObWrapperAllocator(const lib::ObLabel& label) : alloc_(NULL)
  {
    UNUSED(label);
  };
  explicit ObWrapperAllocator(ObIAllocator& alloc) : alloc_(&alloc)
  {}  // for ObArray::ObArray()
  ObWrapperAllocator() : alloc_(NULL){};
  virtual ~ObWrapperAllocator(){};
  virtual void* alloc(int64_t sz, const ObMemAttr& attr)
  {
    return NULL == alloc_ ? NULL : alloc_->alloc(sz, attr);
  }
  virtual void* alloc(const int64_t sz)
  {
    return alloc(sz, default_memattr);
  };
  void free(void* ptr)
  {
    if (NULL != alloc_) {
      alloc_->free(ptr);
      ptr = NULL;
    }
  };
  void set_alloc(ObIAllocator* alloc)
  {
    alloc_ = alloc;
  };
  ObWrapperAllocator& operator=(const ObWrapperAllocator& that)
  {
    if (this != &that) {
      alloc_ = that.alloc_;
    }
    return *this;
  }
  const ObIAllocator* get_alloc() const
  {
    return alloc_;
  }
  static uint32_t alloc_offset_bits()
  {
    return offsetof(ObWrapperAllocator, alloc_) * 8;
  }

private:
  // data members
  ObIAllocator* alloc_;
};

class ObWrapperAllocatorWithAttr : public ObWrapperAllocator {
public:
  explicit ObWrapperAllocatorWithAttr(ObIAllocator* alloc, ObMemAttr attr = ObMemAttr())
      : ObWrapperAllocator(alloc), mem_attr_(attr){};
  explicit ObWrapperAllocatorWithAttr(const lib::ObLabel& label) : ObWrapperAllocator(NULL), mem_attr_()
  {
    mem_attr_.label_ = label;
  };
  explicit ObWrapperAllocatorWithAttr(ObIAllocator& alloc, ObMemAttr attr = ObMemAttr())
      : ObWrapperAllocator(&alloc), mem_attr_(attr)
  {}  // for ObArray::ObArray()
  ObWrapperAllocatorWithAttr() : ObWrapperAllocator(), mem_attr_(){};
  virtual ~ObWrapperAllocatorWithAttr(){};
  virtual void* alloc(const int64_t sz)
  {
    return ObWrapperAllocator::alloc(sz, mem_attr_);
  };

private:
  ObMemAttr mem_attr_;
};
}  // namespace common
}  // namespace oceanbase

#endif  // OCEANBASE_COMMON_IALLOCATOR_H_
