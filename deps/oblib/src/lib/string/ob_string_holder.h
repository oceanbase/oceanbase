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

#ifndef LIB_STRING_OB_STRING_HOLDER_H
#define LIB_STRING_OB_STRING_HOLDER_H
#include "lib/allocator/ob_allocator.h"
#include "lib/allocator/ob_malloc.h"
#include "ob_string.h"
#include <utility>

namespace oceanbase
{
namespace common
{
namespace value_sematic_string
{
class DefaultAllocator : public ObIAllocator
{
public:
  virtual void* alloc(const int64_t size) override
  { return ob_malloc(size, "VSStr"); }

  virtual void* alloc(const int64_t size, const ObMemAttr &attr) override
  { return ob_malloc(size, attr); }

  virtual void free(void *ptr) override
  { ob_free(ptr); }

  static DefaultAllocator &get_instance() {
    static DefaultAllocator allocator;
    return allocator;
  }
};
}

#define DEFAULT_ALLOCATOR value_sematic_string::DefaultAllocator::get_instance()
class ObStringHolder
{
public:
  ObStringHolder() : ObStringHolder(DEFAULT_ALLOCATOR) {};
  ObStringHolder(const ObStringHolder &) = delete;
  ObStringHolder(ObStringHolder &&rhs) : ObStringHolder(DEFAULT_ALLOCATOR)
  {
    std::swap(buffer_, rhs.buffer_);
    std::swap(len_, rhs.len_);
  }
  ObStringHolder &operator=(const ObStringHolder &) = delete;
  ObStringHolder &operator=(ObStringHolder &&rhs)
  {
    std::swap(buffer_, rhs.buffer_);
    std::swap(len_, rhs.len_);
    return *this;
  }
  ObStringHolder(ObIAllocator &alloc) :
  buffer_(nullptr), len_(0), allocator_(alloc) {}
  ~ObStringHolder() { reset(); }
  void reset() {
    if (OB_NOT_NULL(buffer_)) {
      allocator_.free(buffer_);
      buffer_ = nullptr;
      len_ = 0;
    }
  }
  ObString get_ob_string() const { return ObString(len_, buffer_); }
  int assign(const ObStringHolder &rhs) {
    int ret = OB_SUCCESS;
    if (!rhs.empty()) {
      reset();
      if (OB_ISNULL(buffer_ = (char *)allocator_.alloc(rhs.len_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        len_ = rhs.len_;
        memcpy(buffer_, rhs.buffer_, len_);
      }
    }
    return ret;
  }
  int assign(const ObString &str) {
    int ret = OB_SUCCESS;
    reset();
    if (OB_LIKELY(!str.empty())) {
      int64_t len = str.length();
      if (OB_ISNULL(buffer_ = (char *)allocator_.alloc(len))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        len_ = len;
        memcpy(buffer_, str.ptr(), len_);
      }
    }
    return ret;
  }
  bool empty() const {
    return OB_ISNULL(buffer_) && len_ == 0;
  }
  int64_t to_string(char *buf, const int64_t buf_len) const {
    ObString str = get_ob_string();
    return str.to_string(buf, buf_len);
  }
  int64_t get_serialize_size() const {
    ObString str = get_ob_string();
    return str.get_serialize_size();
  }
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const {
    ObString str = get_ob_string();
    return str.serialize(buf, buf_len, pos);
  }
  int deserialize(const char *buf, const int64_t data_len, int64_t &pos) {
    int ret = OB_SUCCESS;
    ObString str;
    if (OB_FAIL(str.deserialize(buf, data_len, pos))) {
      OB_LOG(WARN, "deserialize ObString failed", K(ret));
    } else if (OB_FAIL(assign(str))) {
      OB_LOG(WARN, "init ObStringHolder from ObString failed", K(ret), K(str));
    }
    return ret;
  }
private:
  char *buffer_;
  int64_t len_;
  ObIAllocator &allocator_;
};
#undef DEFAULT_ALLOCATOR

}
}
#endif