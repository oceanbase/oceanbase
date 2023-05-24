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

#define USING_LOG_PREFIX LIB
#include "ob_string_buffer.h"

namespace oceanbase {
namespace common {

ObStringBuffer::ObStringBuffer(common::ObIAllocator *allocator)
    : allocator_(allocator), data_(NULL), len_(0), cap_(0)
{}

ObStringBuffer::ObStringBuffer()
    : allocator_(NULL), data_(NULL), len_(0), cap_(0)
{}

ObStringBuffer::~ObStringBuffer()
{
  reset();
}

void ObStringBuffer::reset()
{
  if (OB_NOT_NULL(data_) && OB_NOT_NULL(allocator_)) {
    allocator_->free(data_);
    data_ = NULL;
  }

  cap_ = 0;
  len_ = 0;
}

void ObStringBuffer::reuse()
{
  len_ = 0;
}

int ObStringBuffer::append(const char *str)
{
  return append(str, NULL == str ? 0 : strlen(str));
}

int ObStringBuffer::append(const char *str, const uint64_t len)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("allocator is null.", K(ret));
  } else if (len > INT64_MAX) {
    // %str can be NULL
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(len));
  } else {
    if (NULL != str && len >= 0) {
      const uint64_t need_len = len_ + len;
      if (need_len < len_) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("size over flow", K(ret), K(need_len), K(len_));
      } else if (OB_FAIL(reserve(need_len))) {
        LOG_WARN("reserve data failed", K(ret), K(need_len), K(len_));
      } else {
        MEMCPY(data_ + len_, str, len);
        len_ += len;
        data_[len_] = '\0';
      }
    }
  }

  return ret;
}

int ObStringBuffer::append(const ObString &str)
{
  return append(str.ptr(), str.length());
}

int ObStringBuffer::reserve(const uint64_t len)
{
  INIT_SUCC(ret);
  const uint64_t need_size = len_ + len + 8; // 1 more byte for C terminating null byte ('\0')
  static const uint64_t BIT_PER_BYTE = 8;
  if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("allocator is null.", K(ret));
  } else if (cap_ < need_size) {
    uint64_t extend_to = (cap_ == 0) ? STRING_BUFFER_INIT_STRING_LEN : cap_;
    // buffer extend by double
    for (uint64_t i = 0; i < sizeof(extend_to) * BIT_PER_BYTE && extend_to < need_size; ++i) {
      extend_to = extend_to << 1;
    }
    if (extend_to < need_size) {
      ret = OB_SIZE_OVERFLOW;
      LOG_ERROR("size overflow", K(ret), K(extend_to), K(need_size));
    } else if (OB_FAIL(extend(extend_to))) {
      LOG_WARN("extend failed", K(ret), K(extend_to));
    }
  }
  return ret;
}

int ObStringBuffer::extend(const uint64_t len)
{
  INIT_SUCC(ret);
  char *new_data = NULL;
  if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("allocator is null.", K(ret));
  } else if (len > INT64_MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(len));
  } else if (NULL == (new_data = (static_cast<char *>(allocator_->alloc(len))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("allocate memory failed", K(ret), K(len));
  } else {
    if (NULL != data_) {
      MEMCPY(new_data, data_, len_ + 1);
      allocator_->free(data_);
      data_ = NULL;
    }
    data_ = new_data;
    cap_ = len;
  }
  return ret;
}

int ObStringBuffer::set_length(const uint64_t len)
{
  INIT_SUCC(ret);
  if (len < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(len));
  } else if (len > capacity()) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_ERROR("try set too long length, buffer maybe overflow",
        K(ret), "capacity", capacity(), K(len));
  } else {
    len_ = len;
    if (cap_ > 0) {
      data_[len_] = '\0';
    }
  }
  return ret;
}

const ObString ObStringBuffer::string() const
{
  return ObString(0, static_cast<int32_t>(len_), data_);
}


} // namespace common
} // namespace oceanbase