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

#ifndef OCEANBASE_LIB_OB_STRING_H_
#define OCEANBASE_LIB_OB_STRING_H_

#include <algorithm>
#include <ctype.h>
#include <cstring>
#include <ostream>
#include "lib/ob_define.h"
#include "lib/alloc/alloc_assist.h"
#include "lib/oblog/ob_log.h"

namespace oceanbase
{
namespace common
{
class ObDataBuffer;
extern uint64_t murmurhash(const void *data, int32_t len, uint64_t hash);
/**
 * ObString do not own the buffer's memory
 * @ptr_ : buffer pointer, allocated by user.
 * @buffer_size_ : buffer's capacity
 * @data_length_ : actual data length of %ptr_
 */
class ObString final
{
public:
  typedef int32_t obstr_size_t;
public:
  ObString()
      : buffer_size_(0), data_length_(0), ptr_(NULL)
  {
  }

  NEED_SERIALIZE_AND_DESERIALIZE;

  /*
  ObString(obstr_size_t size, char *ptr)
    : size_(size), length_(0), ptr_(ptr)
  {
    assert(length_ <= size_);
  }
  */

  /*
   * attach the buf start from ptr, capacity is size, data length is length
   */

  ObString(const obstr_size_t size, const obstr_size_t length, char *ptr)
      : buffer_size_(size), data_length_(length), ptr_(ptr)
  {
    if (OB_ISNULL(ptr_)) {
      buffer_size_ = 0;
      data_length_ = 0;
    }
  }

  ObString(const int64_t length, const char *ptr)
      : buffer_size_(0),
        data_length_(static_cast<obstr_size_t>(length)),
        ptr_(const_cast<char *>(ptr))
  {
    if (OB_ISNULL(ptr_)) {
      data_length_ = 0;
    }
  }

  ObString(const char *ptr)
      : buffer_size_(0),
        data_length_(0),
        ptr_(const_cast<char *>(ptr))
  {
    if (NULL != ptr_) {
      data_length_ = static_cast<obstr_size_t>(strlen(ptr_));
    }
  }

  // copy a char[] into buf and assign myself with it, buf may be used continuously
  int clone(const char *rv, const int32_t len, ObDataBuffer &buf, bool add_separator = true);

  ObString(const obstr_size_t size, const obstr_size_t length, const char *ptr)
      : buffer_size_(size), data_length_(length), ptr_(const_cast<char *>(ptr))
  {
    if (OB_ISNULL(ptr_)) {
      buffer_size_ = 0;
      data_length_ = 0;
    }
  }

  inline bool empty() const
  {
    return NULL == ptr_ || 0 == data_length_;
  }

  /*
   * attache myself to buf, and then copy rv's data to myself.
   * copy obstring in rv to buf, link with buf
   *
   */

  int clone(const ObString &rv, ObDataBuffer &buf);

  // reset
  void reset()
  {
    buffer_size_ = 0;
    data_length_ = 0;
    ptr_ = NULL;
  }
  // ObString 's copy constructor && assignment, use default, copy every member.
  // ObString(const ObString & obstr);
  // ObString & operator=(const ObString& obstr);

  /*
   * write a stream to my buffer,
   * return buffer
   *
   */

  inline obstr_size_t write(const char *bytes, const obstr_size_t length)
  {
    obstr_size_t writed = 0;
    if (OB_ISNULL(bytes) || OB_UNLIKELY(length <= 0)) {
      // do nothing
    } else {
      if (OB_LIKELY(data_length_ + length <= buffer_size_)) {
        MEMCPY(ptr_ + data_length_, bytes, length);
        data_length_ += length;
        writed = length;
      }
    }
    return writed;
  }

  /*
   * write a stream to my buffer at front,
   * return buffer
   *
   */

  inline obstr_size_t write_front(const char *bytes, const obstr_size_t length)
  {
    obstr_size_t writed = 0;
    if (OB_ISNULL(bytes) || OB_UNLIKELY(length <= 0)) {
      // do nothing
    } else {
      if (OB_LIKELY(data_length_ + length <= buffer_size_)) {
        if (data_length_ > 0) {
          MEMMOVE(ptr_ + length, ptr_, data_length_);
        }
        MEMCPY(ptr_, bytes, length);
        data_length_ += length;
        writed = length;
      }
    }
    return writed;
  }
  /*
   * DO NOT USE THIS ANY MORE
   */

  inline void assign(char *bytes, const int64_t length) //TODO(yongle.xh): for -Wshorten-64-to-32, delete it later 4.3
  {
    if (length > INT32_MAX) {
      LIB_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "invalid length for assign", K(length));
    }
    assign(bytes, static_cast<int32_t>(length));
  }

  /*
   * DO NOT USE THIS ANY MORE
   */

  inline void assign(char *bytes, const obstr_size_t length)
  {
    ptr_ = bytes;
    buffer_size_ = length;
    data_length_ = length;
    if (OB_ISNULL(ptr_)) {
      buffer_size_ = 0;
      data_length_ = 0;
    }
  }

  /*
   * attach myself to other's buf, so you can read through me, but not write
   */

  inline void assign_ptr(const char *bytes, const obstr_size_t length)
  {
    ptr_ = const_cast<char *>(bytes);
    buffer_size_ = 0;   //this means I do not hold the buf, just a ptr
    data_length_ = length;
    if (OB_ISNULL(ptr_)) {
      data_length_ = 0;
    }
  }

  inline void assign_ptr(const char *bytes, const int64_t length)  //TODO(yongle.xh): for -Wshorten-64-to-32, delete it later 4.3
  {
    if (length < 0 || length > INT32_MAX) {
      LIB_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "invalid length for assign ptr", K(length));
    }
    assign_ptr(bytes, static_cast<int32_t>(length));
  }

  inline void assign_ptr(const char *bytes, const uint64_t length)  //TODO(yongle.xh): for -Wshorten-64-to-32, delete it later 4.3
  {
    if (length < 0 || length > INT32_MAX) {
      LIB_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "invalid length for assign ptr", K(length));
    }
    assign_ptr(bytes, static_cast<int32_t>(length));
  }

  inline void assign_ptr(const char *bytes, const uint32_t length)  //TODO(yongle.xh): for -Wshorten-64-to-32, delete it later 4.3
  {
    if (length > INT32_MAX) {
      LIB_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "invalid length for assign ptr", K(length));
    }
    assign_ptr(bytes, static_cast<int32_t>(length));
  }
  /*
   * attach myself to a buffer, whoes capacity is size
   */

  inline void assign_buffer(char *buffer, const obstr_size_t size)
  {
    ptr_ = buffer;
    buffer_size_ = size;  //this means I hold the buffer, so you can do write
    data_length_ = 0;
    if (OB_ISNULL(ptr_)) {
      buffer_size_ = 0;
      data_length_ = 0;
    }

  }

  inline obstr_size_t set_length(const obstr_size_t length)
  {
    if (OB_LIKELY(NULL != ptr_) && OB_LIKELY(length <= buffer_size_)) {
      data_length_ = length;
    }
    return data_length_;
  }

  /*
   * the remain size of my buffer
   */

  inline obstr_size_t remain() const
  {
    return OB_LIKELY(buffer_size_ > 0) ? (buffer_size_ - data_length_) : buffer_size_;
  }

  inline obstr_size_t length_without_blank() const
  {
    obstr_size_t size = 0;
    if (NULL != ptr_) {
      char *start = ptr_;
      char *end = ptr_ + data_length_;
      while (start < end) {
        if (!isspace(*start)) {
          size++;
        }
        start++;
      }
    }
    return size;
  }

  inline obstr_size_t length() const { return data_length_; }
  inline obstr_size_t size() const { return buffer_size_; }
  inline const char *ptr() const { return ptr_; }
  inline char *ptr() { return ptr_; }

  inline uint64_t hash() const
  {
    return hash(0);
  }

  inline uint64_t hash(uint64_t seed) const
  {
    uint64_t hash_val = seed;
    if (OB_LIKELY(NULL != ptr_) && OB_LIKELY(data_length_ > 0)) {
      hash_val = murmurhash(ptr_, data_length_, hash_val);
    }
    return hash_val;
  }

  inline int hash(uint64_t &hash_val, uint64_t seed) const
  {
    hash_val = hash(seed);
    return OB_SUCCESS;
  }

  inline int case_compare(const ObString &obstr) const
  {
    int cmp = 0;
    if (NULL == ptr_) {
      if (NULL != obstr.ptr_) {
        cmp = -1;
      }
    } else if (NULL == obstr.ptr_) {
      cmp = 1;
    } else {
      cmp = strncasecmp(ptr_, obstr.ptr_, std::min(data_length_, obstr.data_length_));
      if (0 == cmp) {
        cmp = data_length_ - obstr.data_length_;
      }
    }
    return cmp;
  }

  inline int case_compare(const char *str) const
  {
    obstr_size_t len = 0;
    if (NULL != str) {
      len = static_cast<obstr_size_t>(strlen(str));
    }
    char *p = const_cast<char *>(str);
    const ObString rv(0, len, p);
    return case_compare(rv);
  }


  inline int compare(const ObString &obstr) const
  {
    int cmp = 0;
    if (ptr_ == obstr.ptr_) {
      cmp = data_length_ - obstr.data_length_;
    } else if (0 == data_length_ && 0 == obstr.data_length_) {
      cmp = 0;
    } else if (0 == (cmp = MEMCMP(ptr_, obstr.ptr_, std::min(data_length_, obstr.data_length_)))) {
      cmp = data_length_ - obstr.data_length_;
    }
    return cmp;
  }

  inline int32_t compare(const char *str) const
  {
    obstr_size_t len = 0;
    if (NULL != str) {
      len = static_cast<obstr_size_t>(strlen(str));
    }
    char *p = const_cast<char *>(str);
    const ObString rv(0, len, p);
    return compare(rv);
  }
  //return: false:not match ; true:match
  inline bool prefix_match(const ObString &obstr) const
  {
    bool match = false;
    if (ptr_ == obstr.ptr_) {
      match = data_length_ >= obstr.data_length_ ? true : false;
    } else if (data_length_ < obstr.data_length_) {
      match = false;
    } else if (0 == MEMCMP(ptr_, obstr.ptr_, obstr.data_length_)) {
      match = true;
    }
    return match;
  }

  inline bool prefix_match(const char *str) const
  {
    obstr_size_t len = 0;
    if (NULL != str) {
      len = static_cast<obstr_size_t>(strlen(str));
    }
    char *p = const_cast<char *>(str);
    const ObString rv(0, len, p);
    return prefix_match(rv);
  }

  inline bool prefix_match_ci(const ObString &obstr) const
  {
    bool match = false;
    if (ptr_ == obstr.ptr_) {
      match = data_length_ >= obstr.data_length_ ? true : false;
    } else if (data_length_ < obstr.data_length_) {
      match = false;
    } else if (0 == STRNCASECMP(ptr_, obstr.ptr_, obstr.data_length_)) {
      match = true;
    }
    return match;
  }

  inline bool prefix_match_ci(const char *str) const
  {
    obstr_size_t len = 0;
    if (NULL != str) {
      len = static_cast<obstr_size_t>(strlen(str));
    }
    char *p = const_cast<char *>(str);
    const ObString rv(0, len, p);
    return prefix_match_ci(rv);
  } 

  inline obstr_size_t shrink()
  {

    obstr_size_t rem  = remain();
    if (buffer_size_ > 0) {
      buffer_size_ = data_length_;
    }
    return rem;
  }

  inline bool operator<(const ObString &obstr) const
  {
    return compare(obstr) < 0;
  }

  inline bool operator<=(const ObString &obstr) const
  {
    return compare(obstr) <= 0;
  }

  inline bool operator>(const ObString &obstr) const
  {
    return compare(obstr) > 0;
  }

  inline bool operator>=(const ObString &obstr) const
  {
    return compare(obstr) >= 0;
  }

  inline bool operator==(const ObString &obstr) const
  {
    return compare(obstr) == 0;
  }

  inline bool operator!=(const ObString &obstr) const
  {
    return compare(obstr) != 0;
  }

  inline bool operator<(const char *str) const
  {
    return compare(str) < 0;
  }

  inline bool operator<=(const char *str) const
  {
    return compare(str) <= 0;
  }

  inline bool operator>(const char *str) const
  {
    return compare(str) > 0;
  }

  inline bool operator>=(const char *str) const
  {
    return compare(str) >= 0;
  }

  inline bool operator==(const char *str) const
  {
    return compare(str) == 0;
  }

  inline bool operator!=(const char *str) const
  {
    return compare(str) != 0;
  }

  const ObString trim()
  {
    ObString ret;
    if (NULL != ptr_) {
      char *start = ptr_;
      char *end = ptr_ + data_length_;
      while (start < end && isspace(*start)) {
        start++;
      }
      while (start < end && isspace(*(end - 1))) {
        end--;
      }
      ret.assign_ptr(start, static_cast<obstr_size_t>(end - start));
    }
    return ret;
  }

  const ObString trim_space_only()
  {
    ObString ret;
    if (NULL != ptr_) {
      char *start = ptr_;
      char *end = ptr_ + data_length_;
      while (start < end && ' ' == *start) {
        start++;
      }
      while (start < end && ' ' == *(end - 1)) {
        end--;
      }
      ret.assign_ptr(start, static_cast<obstr_size_t>(end - start));
    }
    return ret;
  }

  static ObString make_string(const char *cstr)
  {
    return NULL == cstr
        ? ObString()
        : ObString(0, static_cast<obstr_size_t>(strlen(cstr)), const_cast<char *>(cstr));
  }

  static ObString make_empty_string()
  {
    return ObString();
  }

  int64_t to_string(char *buf, const int64_t len) const
  {
    int64_t pos = 0;
    if (OB_LIKELY(NULL != buf) && OB_LIKELY(len > 0)) {
      if (NULL != ptr()) {
        pos = snprintf(buf, len, "%.*s", MIN(static_cast<int32_t>(len), length()), ptr());
        if (pos < 0) {
          pos = 0;
        } else if (pos >= len) {
          pos = len - 1;
        }
      }
    }
    return pos;
  }

  // @return The first character in the buffer.
  char operator *() const { return *ptr_; }

  // Discard the first character in the buffer.
  // @return @a this object.
  ObString &operator++()
  {
    if (OB_LIKELY(NULL != ptr_) && OB_LIKELY(data_length_ > 0)) {
      ++ptr_;
      --data_length_;
    }
    return *this;
  }

  // Discard the first @a n characters.
  // @return @a this object.
  ObString &operator +=(const int64_t n)
  {
    if (OB_LIKELY(NULL != ptr_) && OB_LIKELY(data_length_ >= n)) {
      ptr_ += n;
      data_length_ -= static_cast<obstr_size_t>(n);
    } else {
      ptr_ = NULL;
      data_length_ = 0;
    }
    return *this;
  }

  // Check for empty buffer.
  // @return @c true if the buffer has a zero pointer @b or data length.
  bool operator !() const { return !(NULL != ptr_ && data_length_ > 0); }

  // Check for non-empty buffer.
  // @return @c true if the buffer has a non-zero pointer @b and data length.
  typedef bool (ObString::*pseudo_bool)() const;
  operator pseudo_bool() const { return (NULL != ptr_ && data_length_ > 0) ? &ObString::operator! : 0; }

  // Access a character (no bounds check).
  char operator[] (const int64_t n) const { return ptr_[n]; }

  // @return @c true if @a p points at a character in @a this.
  bool contains(const char *p) const { return ptr_ <= p && p < ptr_ + data_length_; }

  // Find a character.
  // @return A pointer to the first occurrence of @a c in @a this
  // or @c NULL if @a c is not found.
  const char *find(char c) const { return static_cast<const char *>(memchr(ptr_, c, data_length_)); }

  // Reverse Find a character from backwards.
  // @return A pointer to the first occurrence of @a c in @a this
  // or @c NULL if @a c is not found.
  const char *reverse_find(char c) const { return static_cast<const char *>(memrchr(ptr_, c, data_length_)); }

  // Split the buffer on the character at @a p.
  //
  // The buffer is split in to two parts and the character at @a p
  // is discarded. @a this retains all data @b after @a p. The
  // initial part of the buffer is returned. Neither buffer will
  // contain the character at @a p.
  //
  // This is convenient when tokenizing and @a p points at the token
  // separator.
  //
  // @note If @a *p is in the buffer then @a this is not changed
  // and an empty buffer is returned. This means the caller can
  // simply pass the result of @c find and check for an empty
  // buffer returned to detect no more separators.
  //
  // @return A buffer containing data up to but not including @a p.
  ObString split_on(const char *p) {
    ObString str; // default to empty return.
    if (contains(p)) {
      const int64_t n = p - ptr_;
      str.assign(ptr_, static_cast<obstr_size_t>(n));
      ptr_ = const_cast<char *>(p + 1);
      data_length_ -= static_cast<obstr_size_t>(n + 1);
    }
    return str;
  }

  // Split the buffer on the character @a c.
  //
  // The buffer is split in to two parts and the occurrence of @a c
  // is discarded. @a this retains all data @b after @a c. The
  // initial part of the buffer is returned. Neither buffer will
  // contain the first occurrence of @a c.
  //
  // This is convenient when tokenizing and @a c is the token
  // separator.
  //
  // @note If @a c is not found then @a this is not changed and an
  // empty buffer is returned.
  //
  // @return A buffer containing data up to but not including @a p.
  ObString split_on(const char c) { return split_on(find(c)); }

  // Get a trailing segment of the buffer.
  // @return A buffer that contains all data after @a p.
  ObString after(const char *p) const
  {
    return contains(p) ? ObString((data_length_ - (p - ptr_)) - 1, p + 1) : ObString();
  }

  // Get a trailing segment of the buffer.
  //
  // @return A buffer that contains all data after the first
  // occurrence of @a c.
  ObString after(char c) const { return after(find(c)); }

  // Remove trailing segment.
  //
  // Data at @a p and beyond is removed from the buffer.
  // If @a p is not in the buffer, no change is made.
  //
  // @return @a this.
  ObString &clip(const char *p)
  {
    if (contains(p)) {
      data_length_ = static_cast<obstr_size_t>(p - ptr_);
    }
    return *this;
  }

  // @return true is all character is digital
  inline bool is_numeric() const
  {
    int ret = true;
    if (OB_ISNULL(ptr_) || data_length_ == 0) {
      ret = false;
    } else {
      int i = 0;
      if (data_length_ >= 2 && ptr_[0] == '-') {
        i += 1;
      }
      for(; i < data_length_; ++i) {
        if (!isdigit(ptr_[i])) {
          ret = false;
        }
      }
    }
    return ret;
  }

  friend std::ostream &operator<<(std::ostream &os, const ObString &str);  // for google test

  static uint32_t buffer_size_offset_bits() { return offsetof(ObString, buffer_size_) * 8; }
  static uint32_t data_length_offset_bits() { return offsetof(ObString, data_length_) * 8; }
  static uint32_t ptr_offset_bits() { return offsetof(ObString, ptr_) * 8; }

private:
  obstr_size_t buffer_size_;
  obstr_size_t data_length_;
  char *ptr_;
};

inline std::ostream &operator<<(std::ostream &os, const ObString &str)  // for google test
{
  os << "size=" << str.buffer_size_ << " len=" << str.data_length_;
  return os;
}

template <typename AllocatorT>
int ob_write_string(AllocatorT &allocator, const ObString &src, ObString &dst, bool c_style = false)
{
  int ret = OB_SUCCESS;
  const ObString::obstr_size_t src_len = src.length();
  char *ptr = NULL;
  if (OB_ISNULL(src.ptr()) || OB_UNLIKELY(0 >= src_len)) {
    dst.assign(NULL, 0);
  } else if (NULL == (ptr = static_cast<char *>(allocator.alloc(src_len + (c_style ? 1 : 0))))) {
    dst.assign(NULL, 0);
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LIB_LOG(ERROR, "allocate memory failed", K(ret), "size", src_len);
  } else {
    MEMCPY(ptr, src.ptr(), src_len);
    if (c_style) {
      ptr[src_len] = 0;
    }
    dst.assign_ptr(ptr, src_len);
  }
  return ret;
}

template <typename AllocatorT>
int ob_strip_space(AllocatorT &allocator, const ObString &src, ObString &dst)
{
  int ret = OB_SUCCESS;
  const ObString::obstr_size_t src_len_without_blank = src.length_without_blank();
  char *ptr = NULL;
  if (OB_ISNULL(src.ptr()) || OB_UNLIKELY(0 >= src_len_without_blank)) {
    dst.assign(NULL, 0);
  } else if (NULL == (ptr = static_cast<char *>(allocator.alloc(src_len_without_blank)))) {
    dst.assign(NULL, 0);
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LIB_LOG(ERROR, "allocate memory failed", K(ret), "size", src_len_without_blank);
  } else {
    const char *start = src.ptr();
    const char *end = src.ptr() + src.length();
    for (int64_t i=0; start < end; start++) {
      if (!isspace(*start)) {
        ptr[i++] = *start;
      }
    }
    dst.assign_ptr(ptr, src_len_without_blank);
  }
  return ret;
}

template <typename AllocatorT>
int ob_simple_low_to_up(AllocatorT &allocator, const ObString &src, ObString &dst)
{
  int ret = OB_SUCCESS;
  const ObString::obstr_size_t src_len = src.length();
  const char *src_ptr = src.ptr();
  char *dst_ptr = NULL;
  void *ptr = NULL;
  char letter='\0';
  if (OB_ISNULL(src_ptr) || OB_UNLIKELY(0 >= src_len)) {
    dst.assign(NULL, 0);
  } else if (NULL == (ptr = allocator.alloc(src_len))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LIB_LOG(ERROR, "allocate memory failed", K(ret), "size", src_len);
  } else {
    dst_ptr = static_cast<char *>(ptr);
    for(ObString::obstr_size_t i = 0; i < src_len; ++i) {
      letter = src_ptr[i];
      if(letter >= 'a' && letter <= 'z'){
        dst_ptr[i] = static_cast<char>(letter - 32);
      } else{
        dst_ptr[i] = letter;
      }
    }
    dst.assign_ptr(dst_ptr, src_len);
  }
  return ret;
}

template <typename AllocatorT>
int ob_sub_str(AllocatorT &allocator, const ObString &src, int32_t start_index, ObString &dst)
{
  int ret = OB_SUCCESS;
  const ObString::obstr_size_t src_len = src.length();
  const char *src_ptr = src.ptr();
  char *dst_ptr = NULL;
  void *ptr = NULL;
  if (OB_ISNULL(src_ptr) || OB_UNLIKELY(0 >= src_len)) {
    dst.assign(NULL, 0);
  } else if(start_index < 0 || start_index >= src_len) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid argument", K(ret), K(start_index), K(src_len));
  } else if (NULL == (ptr = allocator.alloc(src_len - start_index))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LIB_LOG(ERROR, "allocate memory failed", K(ret), "size", src_len - start_index);
  } else{
    dst_ptr = static_cast<char *>(ptr);
    MEMCPY(dst_ptr, src.ptr() + start_index, src_len - start_index);
    dst.assign_ptr(dst_ptr, src_len - start_index);
  }
  return ret;
}

template <typename AllocatorT>
int ob_sub_str(AllocatorT &allocator, const ObString &src, int32_t start_index, int32_t end_index, ObString &dst)
{
  int ret = OB_SUCCESS;
  const ObString::obstr_size_t src_len = src.length();
  const char *src_ptr = src.ptr();
  char *dst_ptr = NULL;
  void *ptr = NULL;
  if (OB_ISNULL(src_ptr) || OB_UNLIKELY(0 >= src_len)) {
    dst.assign(NULL, 0);
  } else if(start_index < 0 || end_index >= src_len || start_index > end_index) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid argument", K(ret), K(start_index), K(end_index), K(src_len));
  } else if (NULL == (ptr = allocator.alloc(end_index - start_index + 1))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LIB_LOG(ERROR, "allocate memory failed", K(ret), "size", end_index - start_index + 1);
  } else{
    dst_ptr = static_cast<char *>(ptr);
    MEMCPY(dst_ptr, src.ptr() + start_index, end_index - start_index + 1);
    dst.assign_ptr(dst_ptr, end_index - start_index + 1);
  }
  return ret;
}

template <typename AllocatorT>
int ob_dup_cstring(AllocatorT &allocator, const ObString &src, char* &dst)
{
  int ret = OB_SUCCESS;
  const ObString::obstr_size_t src_len = src.length();
  void *ptr = NULL;
  if (OB_ISNULL(src.ptr()) || OB_UNLIKELY(0 >= src_len)) {
    dst = NULL;
  } else if (NULL == (ptr = allocator.alloc(src_len+1))) {
    dst = NULL;
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LIB_LOG(ERROR, "allocate memory failed", K(ret), "size", src_len);
  } else {
    MEMCPY(ptr, src.ptr(), src_len);
    dst = reinterpret_cast<char *>(ptr);
    dst[src_len] = '\0';
  }
  return ret;
}
} // end namespace common
} // end namespace oceanbase
#endif // OCEANBASE_LIB_OB_STRING_H_
