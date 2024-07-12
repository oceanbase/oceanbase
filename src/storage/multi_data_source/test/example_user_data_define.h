/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef UNITTEST_SHARE_MULTI_DATA_SOURCE_EXAMPLE_USER_DATA_DEFINE_H
#define UNITTEST_SHARE_MULTI_DATA_SOURCE_EXAMPLE_USER_DATA_DEFINE_H
#include "lib/ob_errno.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/ob_unify_serialize.h"
#include "lib/utility/serialization.h"
#include "meta_programming/ob_meta_serialization.h"
#include "common_define.h"

namespace oceanbase {
namespace unittest {
struct ExampleUserKey {
  OB_UNIS_VERSION(1);
public:
  ExampleUserKey() : value_(0) {}
  ExampleUserKey(const int64_t val) : value_(val) {}
  TO_STRING_KV(K_(value));
  int mds_serialize(char *buf, const int64_t buf_len, int64_t &pos) const {
    int ret = OB_SUCCESS;
    int64_t tmp = value_;
    for (int64_t idx = 0; idx < 8 && OB_SUCC(ret); ++idx) {
      if (pos >= buf_len) {
        ret = OB_BUF_NOT_ENOUGH;
      } else {
        buf[pos++] = ((tmp >> (56 - 8 * idx)) & 0x00000000000000FF);
      }
    }
    return ret;
  }
  int mds_deserialize(const char *buf, const int64_t buf_len, int64_t &pos) {
    int ret = OB_SUCCESS;
    int64_t tmp = 0;
    for (int64_t idx = 0; idx < 8 && OB_SUCC(ret); ++idx) {
      if (pos >= buf_len) {
        ret = OB_BUF_NOT_ENOUGH;
      } else {
        tmp <<= 8;
        tmp |= (0x00000000000000FF & buf[pos++]);
      }
    }
    if (OB_SUCC(ret)) {
      value_ = tmp;
    }
    return ret;
  }
  int64_t mds_get_serialize_size() const { return sizeof(value_); }
  int64_t value_;
};

OB_SERIALIZE_MEMBER_TEMP(inline, ExampleUserKey, value_);
struct ExampleUserData1 {// simple data structure
  OB_UNIS_VERSION(1);
public:
  bool operator==(const ExampleUserData1 &rhs) const { return value_ == rhs.value_; }
  ExampleUserData1() : value_(0) {}
  ExampleUserData1(const int val) : value_(val) {}
  TO_STRING_KV(K_(value));
  int value_;
};

OB_SERIALIZE_MEMBER_TEMP(inline, ExampleUserData1, value_);

struct ExampleUserData2 {// complicated data structure
public:
  ExampleUserData2() : alloc_(nullptr) {}
  ~ExampleUserData2() {
    if (OB_NOT_NULL(alloc_)) {
      if (!data_.empty()) {
        alloc_->free(data_.ptr());
      }
    }
  }
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(alloc_)) {
    } else if (OB_FAIL(serialization::encode(buf, buf_len, pos, (int64_t)data_.length()))) {
    } else {
      for (int64_t idx = 0; idx < data_.length() && OB_SUCC(ret); ++idx) {
        ret = serialization::encode(buf, buf_len, pos, data_.ptr()[idx]);
      }
    }
    return ret;
  }
  int deserialize(ObIAllocator &alloc, const char *buf, const int64_t buf_len, int64_t &pos) {
    int ret = OB_SUCCESS;
    int64_t length = 0;
    char *buffer = nullptr;
    if (OB_FAIL(serialization::decode(buf, buf_len, pos, length))) {
    } else if (OB_ISNULL(buffer = (char *)alloc.alloc(length))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      for (int64_t idx = 0; idx < length && OB_SUCC(ret); ++idx) {
        ret = serialization::decode(buf, buf_len, pos, buffer[idx]);
      }
      if (OB_SUCC(ret)) {
        data_.assign(buffer, length);
        alloc_ = &alloc;
      }
    }
    return ret;
  }
  int64_t get_serialize_size() const {
    int64_t total_size = 0;
    total_size += serialization::encoded_length((int64_t)data_.length());
    for (int64_t idx = 0; idx < data_.length(); ++idx) {
      total_size += serialization::encoded_length(data_.ptr()[idx]);
    }
    return total_size;
  }
  int assign(ObIAllocator &alloc, const char *str) {
    int ret = OB_SUCCESS;
    OB_ASSERT(OB_ISNULL(alloc_) && data_.empty());
    int64_t len = strlen(str);
    char *buffer = nullptr;
    if (OB_ISNULL(buffer = (char *)alloc.alloc(len))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      memcpy(buffer, str, len);
      alloc_ = &alloc;
      data_.assign(buffer, len);
    }
    return ret;
  }
  int assign(ObIAllocator &alloc, const ExampleUserData2 &rhs) {
    int ret = OB_SUCCESS;
    if (OB_NOT_NULL(rhs.alloc_)) {
      OB_ASSERT(OB_ISNULL(alloc_) && data_.empty());
      int64_t len = rhs.data_.length();
      char *buffer = nullptr;
      if (OB_ISNULL(buffer = (char *)alloc.alloc(len))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        memcpy(buffer, rhs.data_.ptr(), len);
        alloc_ = &alloc;
        data_.assign(buffer, len);
      }
    }
    return ret;
  }
  // support move semantic
  ExampleUserData2(ExampleUserData2 &&rhs) {
    if (OB_NOT_NULL(rhs.alloc_)) {
      data_ = rhs.data_;
      alloc_ = rhs.alloc_;
      rhs.alloc_ = nullptr;
      rhs.data_.reset();
      MDS_LOG(INFO, "call move construction", K(*this));
    }
  }
  TO_STRING_KV(KP(data_.ptr()), K(data_.length()));
  ObIAllocator *alloc_;
  ObString data_;
};

}
}
#endif