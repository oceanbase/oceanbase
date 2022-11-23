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

#ifndef _BUFFER_H__
#define _BUFFER_H__

#include <stdexcept>

#include <pthread.h>
#include "lib/allocator/ob_malloc.h"
#include "lib/string/ob_string.h"
#include "common/data_buffer.h"

namespace oceanbase
{
namespace common
{
template <const char *Label = ObModIds::OB_BUFFER>
struct DfltBufferAlloc
{
  void *malloc(const int64_t sz) { return ob_malloc(sz, Label); }
  void free(void *p) { ob_free(p, Label); }
};

template <int N = 9, const char *Label = ObModIds::OB_BUFFER>
struct ObAlignedAllocator
{
  static const int64_t size_N = 1 << N;
  static const int64_t mask_N = (-1) >> N << N;
  int64_t get_suitable_size(const int64_t sz)
  {
    return ((sz + sizeof(void *)) & mask_N) + size_N + size_N;
  }
  void *get_aligned_pointer(void *pt)
  {
    void *p = static_cast<void *>(static_cast<void **>(pt) + 1);
    if (p != reinterpret_cast<void *>(reinterpret_cast<int64_t>(p) & mask_N)) {
      p = reinterpret_cast<void *>((reinterpret_cast<int64_t>(p) & mask_N) + size_N);
    }
    *(static_cast<void **>(p) - 1) = pt;
    return p;
  }
  void *malloc(const int64_t sz)
  {
    void *pt = NULL;
    if (sz > 0) {
      pt = ob_malloc(get_suitable_size(sz), Label);
      if (NULL != pt) { pt = get_aligned_pointer(pt); }
    }
    return pt;
  }
  void free(void *p)
  {
    ob_free(*(static_cast<void **>(p) - 1), Label);
  }
};

template <class Alloc = DfltBufferAlloc<> >
class base_buffer
{
public:
  static const int64_t DEFAULT_CAPACITY = BUFSIZ;
  static const bool USE_EXCEPTION = true;
  static const bool NO_EXCEPTION = false;
protected:
  int64_t capacity_;
  int64_t length_;
  int64_t base_;
  Alloc alloc_;

protected:
  int64_t min(int64_t a, int64_t b) const {return a < b ? a : b;}

  virtual int reset_size_(bool exception, int64_t size, bool retain_data) = 0;

  int alloc_size_(bool exception, int64_t size, bool retain_data)
  {
    if (size > capacity_) { return reset_size_(exception, size, retain_data); }
    else { return 0; }
  }

  int alloc_size_(bool exception, int64_t size) {return alloc_size_(exception, size, true);}

  void init_capacity_(int64_t capacity) {capacity_ = capacity;}
  int64_t unum_to_str_(uint64_t num, char *buf, int64_t buf_len)
  {
    int64_t len = 0;
    if (0 != num) {
      while (0 != num) {
        buf[len++] = num % 10 + '0';
        num /= 10;
      }
    } else {
      buf[0] = '0';
      len = 1;
    }

    return len;
  }

  int64_t num_to_str_(int64_t num, char *buf, int64_t buf_len)
  {
    int64_t len = 0;

    bool positive = true;
    if (num < 0) {
      positive = false;
      num = -num;
    }
    len = unum_to_str_(num, buf, buf_len);
    if (!positive) { buf[len++] = '-'; }

    return len;
  }

public:
  base_buffer() : capacity_(0), length_(0), base_(0) {init_capacity_(DEFAULT_CAPACITY);}
  base_buffer(int64_t capacity) : capacity_(0), length_(0), base_(0) {init_capacity_(capacity);}
  virtual ~base_buffer() {}
  void reset() {base_ = 0; length_ = 0;}

public:
  virtual char *ptre() = 0;
  virtual const char *ptre() const = 0;
  virtual char *ptr() = 0;
  virtual const char *ptr() const = 0;

  int64_t base() const {return base_;}
  void set_base(int64_t new_base) {length_ -= new_base - base_; base_ = new_base;}

  int64_t length() const {return length_;}
  int64_t &length() {return length_;}

  int64_t capacity() const {return capacity_ - base_;}
  void set_capacity(int64_t new_capacity) {reset_size_(USE_EXCEPTION, new_capacity, true);}
  int64_t set_capacity_safe(int64_t new_capacity) {return reset_size_(NO_EXCEPTION, new_capacity, true);}

  char ate(int64_t pos) const {return ptre()[pos];}
  char &ate(int64_t pos) {return ptre()[pos];}
  char at(int64_t pos) const {return ptr()[pos];}
  char &at(int64_t pos) {return ptr()[pos];}

  ObString get_obstring() {return ObString(length(), length(), ptr());}
  const ObString get_obstring() const {return ObString(length(), length(), ptr());}

  ObDataBuffer get_obdatabuffer() {return ObDataBuffer(ptr(), capacity());}
  const ObDataBuffer get_obdatabuffer() const {return ObDataBuffer(ptr(), capacity());}

public:
  int64_t compare(const char *str, int64_t len) const
  {
    int64_t r = MEMCMP(ptr(), str, min(length(), len));
    if (0 == r) {
      if (length() < len) { r = -1; }
      else if (length() > len) { r = 1; }
    }
    return r;
  }
  int64_t compare(const char *str) const {return compare(str, strlen(str));}
  int64_t compare(const base_buffer &rbuf) const {return compare(rbuf.ptr(), rbuf.length());}

public:
  int append(const char *str, int64_t len)
  {
    if (0 == str || len < 0) { return OB_ERROR; }

    int ret = alloc_size_(NO_EXCEPTION, length() + len);
    if (OB_SUCC(ret)) {
      MEMCPY(ptr() + length(), str, len);
      length() += len;
    }
    return ret;
  }
  int append_old(int64_t num)
  {
    const int64_t buf_len = sizeof(int64_t) * 5;
    char buf[buf_len];
    int64_t len = num_to_str_(num, buf, buf_len);

    int ret = alloc_size_(NO_EXCEPTION, length() + len);
    if (0 == ret) while (--len >= 0) { ptr()[length()++] = buf[len]; }

    return ret;
  }
  int append_v2(int64_t num)
  {
    ObFastFormatInt ffi(num);
    int ret = alloc_size_(NO_EXCEPTION, length() + ffi.length());
    if (0 == ret) {
      MEMCPY(ptr() + length(), ffi.ptr(), ffi.length());
    }
    return ret;
  }
  inline int append_(int64_t num) { return append_v2(num); }
//  inline int append_(int64_t num) { return append_old(num); }
  int append_old(uint64_t num)
  {
    const int64_t buf_len = sizeof(int64_t) * 5;
    char buf[buf_len];
    int64_t len = unum_to_str_(num, buf, buf_len);

    int ret = alloc_size_(NO_EXCEPTION, length() + len);
    if (0 == ret) while (--len >= 0) { ptr()[length()++] = buf[len]; }

    return ret;
  }
  int append_v2(uint64_t num)
  {
    ObFastFormatInt ffi(num);
    int ret = alloc_size_(NO_EXCEPTION, length() + ffi.length());
    if (0 == ret) {
      MEMCPY(ptr() + length(), ffi.ptr(), ffi.length());
    }
    return ret;
  }
  inline int append_(uint64_t num) { return append_v2(num); }
//  inline int append_(uint64_t num) { return append_old(num); }
  int append(bool v)
  {
    if (v) { return append("true", 4); }
    else { return append("false", 5); }
  }
  int append(const char *str)
  {
    if (0 == str) { return OB_ERROR; }
    else { return append(str, strlen(str)); }
  }
  int append(const base_buffer &rbuf) {return append(rbuf.ptr(), rbuf.length());}
  int append(int32_t num) {return append(static_cast<int64_t>(num));}
  int append(int16_t num) {return append(static_cast<int64_t>(num));}
  int append(int8_t num) {return append(static_cast<int64_t>(num));}
  int append(uint32_t num) {return append(static_cast<uint64_t>(num));}
  int append(uint16_t num) {return append(static_cast<uint64_t>(num));}
  int append(uint8_t num) {return append(static_cast<uint64_t>(num));}

  template <class AT>
  int append(AT h)
  {
    int ret = h.to_string(ptr() + length(), capacity());
    if (0 != ret) { return ret; }
    else {
      length() += strlen(ptr() + length());
      return 0;
    }
  }

  base_buffer &appende(const char *str, int64_t len)
  {
    alloc_size_(USE_EXCEPTION, length() + len);
    MEMCPY(ptr() + length(), str, len);
    length() += len;
    return *this;
  }
  base_buffer &appende(int64_t num)
  {
    const int64_t buf_len = sizeof(int64_t) * 5;
    char buf[buf_len];
    int64_t len = num_to_str_(num, buf, buf_len);

    alloc_size_(USE_EXCEPTION, length() + len);
    while (--len >= 0) { ptr()[length()++] = buf[len]; }

    return *this;
  }
  base_buffer &appende(uint64_t num)
  {
    const int64_t buf_len = sizeof(int64_t) * 5;
    char buf[buf_len];
    int64_t len = unum_to_str_(num, buf, buf_len);

    alloc_size_(USE_EXCEPTION, length() + len);
    while (--len >= 0) { ptr()[length()++] = buf[len]; }

    return *this;
  }
  base_buffer &appende(bool v)
  {
    if (v) { return appende("true", 4); }
    else { return appende("false", 5); }
  }
  base_buffer &appende(const char *str)
  {
    if (0 == str) { throw std::runtime_error("buffer points to NULL"); }
    else { return appende(str, strlen(str)); }
  }
  base_buffer &appende(const base_buffer &rbuf) {return appende(rbuf.ptr(), rbuf.length());}
  base_buffer &appende(int32_t num) {return appende(static_cast<int64_t>(num));}
  base_buffer &appende(int16_t num) {return appende(static_cast<int64_t>(num));}
  base_buffer &appende(int8_t num) {return appende(static_cast<int64_t>(num));}
  base_buffer &appende(uint32_t num) {return appende(static_cast<uint64_t>(num));}
  base_buffer &appende(uint16_t num) {return appende(static_cast<uint64_t>(num));}
  base_buffer &appende(uint8_t num) {return appende(static_cast<uint64_t>(num));}

  template <class AT>
  base_buffer &appende(AT h)
  {
    int64_t ret = h.to_string(ptre() + length(), capacity());
    if (0 != ret) { throw std::runtime_error("appende to_string error"); }
    else {
      length() += strlen(ptre() + length());
      return *this;
    }
  }

public:
  int assign(const char *str, int64_t len)
  {
    if (0 == str || len < 0) { return OB_ERROR; }
    else {
      length() = 0;
      return append(str, len);
    }
  }
  int assign(const char *str)
  {
    if (0 == str) { return OB_ERROR; }
    else { return assign(str, strlen(str)); }
  }
  int assign(const base_buffer &rbuf) {return assign(rbuf.ptr(), rbuf.length());}
  int assign(int64_t num) {length() = 0; return append(num);}
  int assign(int32_t num) {return assign(static_cast<int64_t>(num));}
  int assign(int16_t num) {return assign(static_cast<int64_t>(num));}
  int assign(int8_t num) {return assign(static_cast<int64_t>(num));}
  int assign(uint64_t num) {length() = 0; return append(num);}
  int assign(uint32_t num) {return assign(static_cast<uint64_t>(num));}
  int assign(uint16_t num) {return assign(static_cast<uint64_t>(num));}
  int assign(uint8_t num) {return assign(static_cast<uint64_t>(num));}
  int assign(bool v) {length() = 0; return append(v);}

  template <class AT>
  int assigne(AT h)
  {
    length() = 0;
    return append(h);
  }

  base_buffer &assigne(const char *str, int64_t len)
  {
    if (0 == str || len < 0) { throw std::runtime_error("buffer points to NULL"); }
    else {
      length() = 0;
      return appende(str, len);
    }
  }
  base_buffer &assigne(const char *str)
  {
    if (0 == str) { throw std::runtime_error("buffer points to NULL"); }
    else { return assigne(str, strlen(str)); }
  }
  base_buffer &assigne(const base_buffer &rbuf) {return assigne(rbuf.ptr(), rbuf.length());}
  base_buffer &assigne(int64_t num) {length() = 0; return appende(num);}
  base_buffer &assigne(int32_t num) {return assigne(static_cast<int64_t>(num));}
  base_buffer &assigne(int16_t num) {return assigne(static_cast<int64_t>(num));}
  base_buffer &assigne(int8_t num) {return assigne(static_cast<int64_t>(num));}
  base_buffer &assigne(uint64_t num) {length() = 0; return appende(num);}
  base_buffer &assigne(uint32_t num) {return assigne(static_cast<uint64_t>(num));}
  base_buffer &assigne(uint16_t num) {return assigne(static_cast<uint64_t>(num));}
  base_buffer &assigne(uint8_t num) {return assigne(static_cast<uint64_t>(num));}
  base_buffer &assigne(bool v) {length() = 0; return appende(v);}

  template <class AT>
  base_buffer &assigne(AT h)
  {
    length() = 0;
    return appende(h);
  }


public:
  template <class charT>
  class iterator_base
  {
  private:
    charT *cur_;
  public:
    explicit iterator_base(charT *p) {cur_ = p;}

    template <class charOT>
    iterator_base(const iterator_base<charOT> &ri) {cur_ = ri.ptr();}

    const charT *ptr() const {return cur_;}
    iterator_base &operator++() {cur_++; return *this;}
    iterator_base operator++(int) {return iterator_base(cur_++);}
    const charT operator*() const {return *cur_;}
    charT &operator*() {return *cur_;}
    charT *operator->() {return cur_;}
    charT *operator+(int64_t n) {return cur_ + n;}
    charT *operator-(int64_t n) {return cur_ - n;}
    charT *operator+=(int64_t n) {return cur_ = cur_ + n;}
    charT *operator-=(int64_t n) {return cur_ = cur_ - n;}
    template <class charTR>
    bool operator==(iterator_base<charTR> ri) const {return ptr() == ri.ptr();}
    template <class charTR>
    bool operator!=(iterator_base<charTR> ri) const {return ptr() != ri.ptr();}
  };

  typedef iterator_base<char> iterator;
  typedef iterator_base<const char> const_iterator;

  iterator begin() {return iterator(ptr());}
  const_iterator begin() const {return const_iterator(ptr());}
  iterator end() {return iterator(ptr() + length_);}
  const_iterator end() const {return const_iterator(ptr() + length_);}
  iterator capacity_end() {return iterator(ptr() + capacity_);}
  const_iterator capacity_end() const {return const_iterator(ptr() + capacity_);}
  int64_t pos(iterator iter) const {return iter.ptr() - ptr();}
  int64_t pos(const_iterator iter) const {return iter.ptr() - ptr();}
};

template <class Alloc = DfltBufferAlloc<> >
class base_normal_buffer : public base_buffer<Alloc>
{
protected:
  char *ptr_;

public:
  typedef base_buffer<Alloc> Parent;

protected:
  virtual int reset_size_(bool exception, int64_t size, bool retain_data)
  {
    if (0 == ptr_) {
      if (exception) {
        throw std::runtime_error("buffer points to NULL");
      } else {
        return 1;
      }
    }
    char *pptr_ = ptr_;
    ptr_ = static_cast<char *>(Parent::alloc_.malloc(size));
    if (0 != ptr_) {
      if (retain_data) { MEMCPY(ptr_, pptr_, min(Parent::length_, size)); }
      else { Parent::length_ = 0; }
      Parent::capacity_ = size;
      Parent::alloc_.free(pptr_);
      pptr_ = 0;
      return 0;
    } else {
      ptr_ = pptr_;
      if (exception) { throw std::bad_alloc(); }
      else { return 1; }
    }
  }

  void preset_()
  {
    Parent::length_ = 0;
    ptr_ = static_cast<char *>(Parent::alloc_.malloc(Parent::capacity_));
    if (0 == ptr_) {
      Parent::capacity_ = 0;
      printf("base_buffer::preset_, %ld\n", Parent::capacity_);
      throw std::bad_alloc();
    }
  }

public:
  base_normal_buffer() : base_buffer<Alloc>() {preset_();}
  base_normal_buffer(int64_t capacity) : base_buffer<Alloc>(capacity) {preset_();}
  virtual ~base_normal_buffer()
  {
    if (0 != ptr_) {
      Parent::alloc_.free(ptr_);
      ptr_ = 0;
    }
    Parent::capacity_ = 0;
    Parent::length_ = 0;
  }

public:
  virtual char *ptre() {return ptr_ + Parent::base_;}
  virtual const char *ptre() const {return ptr_ + Parent::base_;}
  virtual char *ptr() {return ptr_ + Parent::base_;}
  virtual const char *ptr() const {return ptr_ + Parent::base_;}
};

template <int N = 256, class Alloc = DfltBufferAlloc<> >
class sbuffer : public base_buffer<Alloc>
{
protected:
  char buff_[N];

public:
  typedef base_buffer<Alloc> Parent;

protected:
  virtual int reset_size_(bool exception, int64_t size, bool retain_data)
  {
    (void)size;
    (void)retain_data;
    if (exception) { throw std::runtime_error("stack buffer can not reset_size_"); }
    else { return 1; }
  }

public:
  sbuffer() : base_buffer<Alloc>(N) {}
  virtual ~sbuffer() {}

public:
  virtual char *ptre() {return buff_ + Parent::base_;}
  virtual const char *ptre() const {return buff_ + Parent::base_;}
  virtual char *ptr() {return buff_ + Parent::base_;}
  virtual const char *ptr() const {return buff_ + Parent::base_;}
};

typedef base_normal_buffer<> buffer;
typedef base_normal_buffer<ObAlignedAllocator<> > aligned_buffer;
}
}

#endif // _BUFFER_H__
