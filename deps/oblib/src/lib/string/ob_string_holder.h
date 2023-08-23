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

class ObStringHolder
{
  static constexpr int64_t TINY_STR_SIZE = 32;// no need count '\0'
public:
  ObStringHolder(const lib::ObMemAttr &attr=lib::ObMemAttr(OB_SERVER_TENANT_ID, "VSStr"))
    : buffer_(nullptr), len_(0), attr_(attr) {}
  ~ObStringHolder() { reset(); }
  void reset() {
    if (buffer_ == local_buffer_for_tiny_str_) {// tiny str
      buffer_ = nullptr;
      len_ = 0;
    } else if (OB_ISNULL(buffer_)) {// empty str
      len_ = 0;
    } else {// big str
      ob_free(buffer_);
      buffer_ = nullptr;
      len_ = 0;
    }
  }
  // move sematic
  ObStringHolder(ObStringHolder &&rhs) : ObStringHolder() { *this = std::move(rhs); }
  ObStringHolder &operator=(ObStringHolder &&rhs) {
    reset();
    if (rhs.buffer_ == rhs.local_buffer_for_tiny_str_) {// tiny str
      copy_from_tiny_ob_str_(rhs.get_ob_string());
    } else {// big str
      std::swap(buffer_, rhs.buffer_);
      std::swap(len_, rhs.len_);
    }
    return *this;
  }
  // not allow copy construction and copy assignment
  ObStringHolder(const ObStringHolder &) = delete;
  ObStringHolder &operator=(const ObStringHolder &) = delete;
  // copy from assign
  int assign(const ObStringHolder &rhs) { return assign(rhs.get_ob_string()); }
  int assign(const ObString &str) {
    int ret = OB_SUCCESS;
    if (OB_LIKELY(!str.empty())) {
      if (str.length() <= TINY_STR_SIZE) {// tiny str
        copy_from_tiny_ob_str_(str);
      } else {// big str
        int64_t len = str.length();
        char *temp_buffer = nullptr;
        if (OB_ISNULL(temp_buffer = (char *)ob_malloc(len, attr_))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else {
          reset();
          buffer_ = temp_buffer;
          len_ = len;
          memcpy(buffer_, str.ptr(), len_);
        }
      }
    } else {
      reset();
    }
    return ret;
  }
  // use ObString method to serialize and print
  ObString get_ob_string() const { return ObString(len_, buffer_); }
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
  bool operator==(const ObString &str) const {
    return str == ObString(len_, buffer_);
  }
  bool operator!=(const ObString &str) const {
    return !(*this == str);
  }
private:
  void copy_from_tiny_ob_str_(const ObString &tiny_str) {
    reset();
    OB_ASSERT(tiny_str.length() <= TINY_STR_SIZE);
    memcpy(local_buffer_for_tiny_str_, tiny_str.ptr(), tiny_str.length());
    buffer_ = local_buffer_for_tiny_str_;
    len_ = tiny_str.length();
  }
private:
  char *buffer_;
  int64_t len_;
  char local_buffer_for_tiny_str_[TINY_STR_SIZE];
  lib::ObMemAttr attr_;
};

}
}
#endif
