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

#ifndef __OB_BLOCK_SSTABLE_DATA_BUFFER_
#define __OB_BLOCK_SSTABLE_DATA_BUFFER_
#include <stdint.h>
#include <stdarg.h>
#include <algorithm>
#include "lib/allocator/ob_fixed_size_block_allocator.h"
#include "lib/utility/utility.h"
#include "share/ob_define.h"
#include "common/log/ob_log_constants.h"

namespace oceanbase
{
namespace blocksstable
{
class ObBufferHolder
{
public:
  inline char *data()
  {
    return data_;
  }
  inline const char *data() const
  {
    return data_;
  }

  inline bool is_valid() const
  {
    return (NULL != data_) && (capacity_ >0);
  }
  inline char *current()
  {
    return data_ + pos_;
  }
  inline const char *current() const
  {
    return data_ + pos_;
  }

  inline int64_t pos() const
  {
    return pos_;
  }

  inline int64_t length() const
  {
    return pos_;
  }

  inline int64_t upper_align_length() const
  {
    return upper_align(pos_, DIO_ALIGN_SIZE);
  }

  inline int64_t remain() const
  {
    return capacity_ - pos_;
  }
  inline int64_t capacity() const
  {
    return capacity_;
  }

  inline int advance(const int64_t length)
  {
    int ret = common::OB_SUCCESS;
    if (remain() >= length) {
      pos_ += length;
    } else {
      ret = common::OB_BUF_NOT_ENOUGH;
    }
    return ret;
  }

  inline int advance_zero(const int64_t length)
  {
    int ret = common::OB_SUCCESS;
    if (remain() >= length) {
      MEMSET(current(), 0, length);
      pos_ += length;
    } else {
      ret = common::OB_BUF_NOT_ENOUGH;
    }
    return ret;
  }

  inline int backward(const int64_t length)
  {
    int ret = common::OB_SUCCESS;
    if (pos_ >= length) {
      pos_ -= length;
    } else {
      ret = common::OB_BUF_NOT_ENOUGH;
    }
    return ret;
  }

  inline int set_pos(const int64_t pos)
  {
    int ret = common::OB_SUCCESS;
    if (pos >= 0 && pos <= capacity_) {
      pos_ = pos;
    } else {
      ret = common::OB_INVALID_ARGUMENT;
    }
    return ret;
  }

  TO_STRING_KV(KP_(data), K_(pos), K_(capacity));

public:
  ObBufferHolder()
      : data_(NULL), pos_(0), capacity_(0) {}
  virtual ~ObBufferHolder() {}
  INLINE_NEED_SERIALIZE_AND_DESERIALIZE;
protected:
  char *data_;
  int64_t pos_;
  int64_t capacity_;
};

DEFINE_SERIALIZE(ObBufferHolder)
{
  int ret = common::OB_SUCCESS;
  const int64_t serialize_size = get_serialize_size();
  //Null ObString is allowed
  if (OB_ISNULL(buf) || OB_UNLIKELY(serialize_size > buf_len - pos)) {
    ret = common::OB_SIZE_OVERFLOW;
    LIB_LOG(WARN, "size overflow", K(ret),
        KP(buf), K(serialize_size), "remain", buf_len - pos);
  } else if (OB_FAIL(common::serialization::encode_vstr(buf, buf_len, pos, data_, pos_))) {
    LIB_LOG(WARN, "string serialize failed", K(ret));
  }
  return ret;
}

DEFINE_DESERIALIZE(ObBufferHolder)
{
  int ret = common::OB_SUCCESS;
  int64_t len = 0;
  const int64_t MINIMAL_NEEDED_SIZE = 2; //at least need two bytes
  if (OB_ISNULL(buf) || OB_UNLIKELY((data_len - pos) < MINIMAL_NEEDED_SIZE)) {
    ret = common::OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid argument", K(ret), KP(buf), "remain", data_len - pos);
  } else {
    data_ = const_cast<char *>(common::serialization::decode_vstr(buf, data_len, pos, &len));
    if (OB_ISNULL(data_)) {
      ret = common::OB_ERROR;
      LIB_LOG(WARN, "decode NULL string", K(ret));
    } else {
      capacity_ = len;
      pos_ = 0;
    }
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObBufferHolder)
{
  return common::serialization::encoded_length_vstr(pos_);
}

template <bool> struct BufferWriteWrap;
template <>
struct BufferWriteWrap<true>
{
  template <typename T, typename U>
  int operator()(T &t, const U &u) { return t.write_serialize(u); }
};
template <>
struct BufferWriteWrap<false>
{
  template <typename T, typename U>
  int operator()(T &t, const U &u) { return t.write_pod(u); }
};

template <bool> struct BufferReadWrap;
template <>
struct BufferReadWrap<true>
{
  template <typename T, typename U>
  int operator()(T &t, U &u) { return t.read_serialize(u); }
};
template <>
struct BufferReadWrap<false>
{
  template <typename T, typename U>
  int operator()(T &t, U &u) { return t.read_pod(u); }
};

class ObBufferWriter : public ObBufferHolder
{
public:
  template <typename T>
  int rollback(const T &value)
  {
    UNUSED(value);
    return backward(sizeof(T));
  }

  template <typename T>
  int write_pod(const T &value)
  {
    int ret = common::OB_SUCCESS;

    if (remain() >= (int64_t)sizeof(T)) {
      do_write(value);
    } else if (common::OB_SUCCESS ==
               (ret = expand(static_cast<int64_t>(sizeof(T))))) {
      // ASSERT(remain() > sizeof(T))
      do_write(value);
    }
    return ret;
  }

  int write(const char *buf, const int64_t size)
  {
    int ret = common::OB_SUCCESS;
    if (remain() >= size) {
      do_write(buf, size);
    } else if (common::OB_SUCCESS == (ret = expand(size))) {
      // ASSERT(remain() > sizeof(T))
      do_write(buf, size);
    }
    return ret;
  }


  template <typename T>
  int write_serialize(const T &value)
  {
    int ret = common::OB_SUCCESS;
    int64_t serialize_size = value.get_serialize_size();
    if (remain() >= serialize_size) {
      ret = value.serialize(data_, capacity_, pos_);
    } else if (common::OB_SUCCESS == (ret = expand(serialize_size))) {
      // ASSERT(remain() > sizeof(T))
      ret = value.serialize(data_, capacity_, pos_);
    }
    return ret;
  }

  template <typename T>
  int write(const T &value)
  {
    typedef BufferWriteWrap<HAS_MEMBER(T, get_serialize_size)> WRAP;
    return WRAP()(*this, value);
  }

  int append_fmt(const char *fmt, ...) __attribute__((format(printf, 2, 3)))
  {
    int rc = common::OB_SUCCESS;
    va_list ap;
    va_start(ap, fmt);
    rc = vappend_fmt(fmt, ap);
    va_end(ap);
    return rc;
  }

  int vappend_fmt(const char *fmt, va_list ap)
  {
    int rc = common::OB_SUCCESS;
    va_list ap2;
    va_copy(ap2, ap);
    int64_t n = vsnprintf(data_ + pos_, remain(), fmt, ap);
    if (n++ < 0) { // include '\0' at tail
      _OB_LOG_RET(WARN, common::OB_ERR_SYS, "vsnprintf failed, errno %d", errno);
      rc = common::OB_ERROR;
    } else if (n > remain()) {
      rc = expand(n + 1);
      if (common::OB_SUCCESS == rc) {
        n = vsnprintf(data_ + pos_, remain(), fmt, ap2);
        if (n < 0) {
          _OB_LOG_RET(WARN, common::OB_ERR_SYS, "vsnprintf failed, errno %d", errno);
          rc = common::OB_ERROR;
        } else {
          pos_ += n;
        }
      }
    } else {
      pos_ += n;
    }
    va_end(ap2);
    return rc;
  }

  virtual int expand(const int64_t size)
  {
    UNUSED(size);
    return common::OB_BUF_NOT_ENOUGH;
  }

public:
  virtual ~ObBufferWriter() {}
  ObBufferWriter(char *buf, const int64_t data_len, int64_t pos = 0)
  {
    assign(buf, data_len, pos);
  }
  void assign(char *buf, const int64_t data_len, int64_t pos = 0)
  {
    data_ = const_cast<char *>(buf);
    capacity_ = data_len;
    pos_ = pos;
  }
protected:

  // write with no check;
  // caller ensure memory not out of bounds.
  template <typename T>
  void do_write(const T &value)
  {
    *((T *)(data_ + pos_)) = value;
    pos_ += sizeof(T);
  }

  void do_write(const char *buf, const int64_t size)
  {
    MEMCPY(current(), buf, size);
    pos_ += size;
  }

};

/**
 * write data buffer, auto expand memory when write overflow.
 * allocate memory for own self;
 */
class ObSelfBufferWriter : public ObBufferWriter
{
  static const int64_t BUFFER_ALIGN_SIZE = common::ObLogConstants::LOG_FILE_ALIGN_SIZE;
public:
  virtual int expand(const int64_t size)
  {
    int64_t new_size = std::max(capacity_ * 2,
                                static_cast<int64_t>(size + capacity_));
    if (is_aligned_) {
      new_size = common::upper_align(new_size, BUFFER_ALIGN_SIZE);
    }

    return ensure_space(new_size);
  }

public:
  ObSelfBufferWriter(
      const char *label,
      const int64_t size = 0,
      const bool need_align = false,
      const bool use_fixed_blk = true);
  virtual ~ObSelfBufferWriter();
  int ensure_space(const int64_t size);
  bool is_dirty() const { return OB_NOT_NULL(data_) && pos_ > 0; }
  inline void reuse() { pos_ = 0; }
  inline void reset() { free(); pos_= 0; capacity_ = 0; }
  inline void set_prop(const char *label, const bool need_align)
  {
    label_ = label;
    is_aligned_ = need_align;
  }
private:
  char *alloc(const int64_t size);
  void free();
private:
  const char *label_;
  bool is_aligned_;
  bool use_fixed_blk_;
  common::ObMacroBlockSizeMemoryContext macro_block_mem_ctx_;
};

class ObBufferReader : public ObBufferHolder
{
public:
  template <typename T>
  int read_pod(T &value)
  {
    int ret = common::OB_SUCCESS;

    if (remain() >= static_cast<int64_t>(sizeof(T))) {
      value = *(reinterpret_cast<T *>(current())) ;
      pos_ += (static_cast<int64_t>(sizeof(T)));
    } else {
      ret = common::OB_BUF_NOT_ENOUGH;
    }
    return ret;
  }

  template <typename T>
  int read_serialize(T &value)
  {
    return value.deserialize(data_, capacity_, pos_);
  }

  template <typename Allocator, typename T>
  int read(Allocator &allocate, T &value)
  {
    return value.deserialize(allocate, data_, capacity_, pos_);
  }

  int read(char *buf, const int64_t size)
  {
    int ret = common::OB_SUCCESS;
    if (remain() >= size) {
      MEMCPY(buf, current(), size);
      pos_ += size;
    } else {
      ret = common::OB_BUF_NOT_ENOUGH;
    }
    return ret;
  }

  int read_cstr(char *&buf)
  {
    int ret = common::OB_SUCCESS;
    buf = current();
    while (pos_ < capacity_ && 0 != *(data_ + pos_)) {
      ++pos_;
    }
    if (pos_ == capacity_) {
      ret = common::OB_BUF_NOT_ENOUGH;
    } else {
      ++pos_; // skip trailing '\0'
    }
    return ret;
  }

  template <typename T>
  int read(T &value)
  {
    typedef BufferReadWrap<HAS_MEMBER(T, get_serialize_size)> WRAP;
    return WRAP()(*this, value);
  }

  template <typename T>
  int get(const T *&value)
  {
    int ret = common::OB_SUCCESS;

    if (OB_UNLIKELY((capacity_ - pos_) < static_cast<int64_t>(sizeof(T)))) {
      ret = common::OB_BUF_NOT_ENOUGH;
    } else {
      value = (const T *)(data_ + pos_) ;
      pos_ += (static_cast<int64_t>(sizeof(T)));
    }
    return ret;
  }

public:
  ObBufferReader()
  {
    assign(NULL, 0, 0);
  }
  ObBufferReader(const char *buf, const int64_t data_len, int64_t pos = 0)
  {
    assign(buf, data_len, pos);
  }
  ~ObBufferReader() {}

  ObBufferReader(const ObBufferReader &other)
  {
    if (this != &other) {
      data_ = other.data_;
      capacity_ = other.capacity_;
      pos_ = other.pos_;
    }
  }

  ObBufferReader &operator = (const ObBufferReader &other)
  {
    if (this != &other) {
      data_ = other.data_;
      capacity_ = other.capacity_;
      pos_ = other.pos_;
    }
    return *this;
  }

  void assign(const char *buf, const int64_t data_len, int64_t pos = 0)
  {
    data_ = const_cast<char *>(buf);
    capacity_ = data_len;
    pos_ = pos;
  }
};
}
}


#endif //
