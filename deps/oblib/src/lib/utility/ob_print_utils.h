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

#ifndef LIB_UTILITY_OB_PRINT_UTILS_
#define LIB_UTILITY_OB_PRINT_UTILS_
#include "lib/ob_define.h"
#include "lib/ob_name_def.h"
#include "lib/string/ob_fixed_length_string.h"
#include "lib/hash_func/murmur_hash.h"
//#include "lib/thread_local/ob_tsi_factory.h"
#include "lib/allocator/page_arena.h"
#include "lib/allocator/ob_allocator.h"
namespace oceanbase
{
namespace common
{
class ObIAllocator;
#define COMMA_FORMAT ", "
#define WITH_COMMA(format)  (with_comma ? COMMA_FORMAT format: format)

class ObString;
char get_xdigit(const char c1);

//transform data to hext_cstr include '\0'
int to_hex_cstr(const void *in_data, const int64_t data_length,
                char *buff, const int64_t buff_size);
//transform data to hex cstr. pos include '\0'. cstr_pos is the cstr begin position.
//If you need pos not include '\0', you can use hex_print
int to_hex_cstr(const void *in_data,
                const int64_t data_length,
                char *buf,
                const int64_t buf_len,
                int64_t &pos,
                int64_t &cstr_pos);

int hex_to_cstr(const void *in_data,
                const int64_t data_lenth,
                char *buff,
                const int64_t buff_size);
int hex_to_cstr(const void *in_data,
                const int64_t data_length,
                char *buff,
                const int64_t buff_size,
                int64_t &pos);
/**
 * convert the input buffer into hex string,  not include '\0'
 *
 * @param in_buf [in] input buffer
 * @param in_len [in] input buffer length
 * @param buffer [in/out] output buffer
 * @param buf_len [in]  output buffer length
 * @param pos [in/out] in:start position to store result, out: pos of end char
 *
 * @retval SIZE_OVERFLOW if the buffer is not enough
 */
int hex_print(const void* in_buf, const int64_t in_len,
              char *buffer, int64_t buf_len, int64_t &pos);
int bit_print(uint64_t bit_val, char *buffer, int64_t buf_len, int64_t &pos);
int is_printable(const char *buf, int64_t buf_len, bool &is_printable);

int32_t str_to_hex(const void *in_data, const int32_t data_length, void *buff,
                   const int32_t buff_size);
/**
 * convert bit value to char array, they have the same bytes content
 *
 * @param value [in] bit numeric value
 * @param bit_len [in] the length of bit
 * @param buffer [in/out] output buffer
 * @param buf_len [in]  output buffer length
 * @param pos [in/out] in:start position to store result, out: pos of end char
 */
int bit_to_char_array(uint64_t value, int32_t bit_len, char *buf, int64_t buf_len, int64_t &pos);
//Use to print data to hex str
struct ObLogPrintHex
{
  ObLogPrintHex(const void *data, const int64_t data_size)
      : data_(data), data_size_(data_size)
  {}
  ~ObLogPrintHex()
  {}
  int64_t to_string(char *buf, const int64_t len) const
  {
    int64_t pos = 0;
    int ret = OB_SUCCESS;
    if (OB_FAIL(logdata_printf(buf, len, pos, "\"data_size:%ld, data:", data_size_))) {
    } else if (OB_FAIL(hex_print(data_, data_size_, buf, len, pos))) {
    } else if (OB_FAIL(logdata_printf(buf, len, pos, "\""))) {
    } else {}
    return pos;
  }
  const void *data_;
  const int64_t data_size_;
};

struct ObHexStringWrap
{
  ObHexStringWrap(const common::ObString &str)
      :str_(str)
  {}
  int64_t to_string(char *buf, const int64_t len) const;
private:
  const common::ObString &str_;
};

////////////////////////////////////////////////////////////////
// to_string stuff
////////////////////////////////////////////////////////////////
template <typename T>
int64_t to_string(const T &obj, char *buffer, const int64_t buffer_size)
{
  return obj.to_string(buffer, buffer_size);
}
template <>
int64_t to_string<int32_t>(const int32_t &obj, char *buffer, const int64_t buffer_size);
template <>
int64_t to_string<uint32_t>(const uint32_t &obj, char *buffer, const int64_t buffer_size);
template <>
int64_t to_string<int64_t>(const int64_t &obj, char *buffer, const int64_t buffer_size);
template <>
int64_t to_string<uint64_t>(const uint64_t &obj, char *buffer, const int64_t buffer_size);
template <>
int64_t to_string<double>(const double &obj, char *buffer, const int64_t buffer_size);
template <>
int64_t to_string<bool>(const bool &obj, char *buffer, const int64_t buffer_size);

class ToStringAdaptor
{
public:
  virtual ~ToStringAdaptor() {}
  virtual int64_t to_string(char *buffer, const int64_t length) const = 0;
};

////////////////////////////////////////////////////////////////
// databuff stuff
////////////////////////////////////////////////////////////////

/// @return OB_SUCCESS or OB_SIZE_OVERFLOW
int databuff_printf(char *buf, const int64_t buf_len, const char *fmt,
                    ...) __attribute__((format(printf, 3, 4)));
/// @return OB_SUCCESS or OB_SIZE_OVERFLOW
int databuff_printf(char *buf, const int64_t buf_len, int64_t &pos, const char *fmt,
                    ...) __attribute__((format(printf, 4, 5)));
/// @return OB_SUCCESS or OB_SIZE_OVERFLOW
int databuff_printf(char *&buf, int64_t &buf_len,
                    int64_t &pos, common::ObIAllocator &alloc,
                    const char *fmt, ...) __attribute__((format(printf, 5, 6)));
/// @return OB_SUCCESS or OB_SIZE_OVERFLOW
template <typename T>
int databuff_printf(char *buf, int64_t buf_len, int64_t &pos, const T &obj)
{
  int ret = OB_SUCCESS;
  if (nullptr != buf && pos >= 0 && pos < buf_len) {
    int64_t str_len = to_string(obj, buf + pos, buf_len - pos);
    if (str_len >= 0 && str_len < buf_len - pos) {
      buf[pos + str_len] = '\0';
      pos += str_len;
    } else {
      buf[buf_len -1] = '\0';
      pos = buf_len - 1;
      ret = OB_SIZE_OVERFLOW;
    }
  } else {
    ret = OB_SIZE_OVERFLOW;
  }
  return ret;
}
/// @return OB_SUCCESS or OB_SIZE_OVERFLOW
template <typename T>
int databuff_printf(char *buf, int64_t buf_len, int64_t &pos, T *const p_obj)
{
  int ret = OB_SUCCESS;
  if (nullptr == p_obj) {
    ret = databuff_printf(buf, buf_len, pos, "NULL");
  } else {
    ret = databuff_printf(buf, buf_len, pos, *p_obj);
  }
  return ret;
}

template <typename T>
int databuff_print_one_obj(char* buf, const int64_t buf_len, int64_t &pos, const T &obj)
{
  return logdata_print_obj(buf, buf_len, pos, obj);
}
int databuff_print_one_obj(char* buf, const int64_t buf_len, int64_t &pos, const char *p_obj);


template <typename ...Rest>
int databuff_print_multi_objs_helper(char* buf, const int64_t buf_len, int64_t &pos) {
  int ret = OB_SUCCESS;
  if (nullptr != buf && pos >= 0 && pos < buf_len) {
    buf[pos] = '\0';
  } else {
    ret = OB_SIZE_OVERFLOW;
  }
  return ret;
}
template <typename First, typename ...Rest>
int databuff_print_multi_objs_helper(
    char* buf,
    const int64_t buf_len,
    int64_t &pos,
    const First& first,
    const Rest &...others)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_print_one_obj(buf, buf_len, pos, first))) {
  } else if (OB_FAIL(databuff_print_multi_objs_helper(buf, buf_len, pos, others...))) {
  } else {}
  return ret;
}
/*
databuff_print_multi_objs is an enhanced version of databuff_printf.
It does not require a formatted string, and supports fundamental types and classes
that implement the to_string method. Example:
  ret = databuff_printf(buf, buf_len, pos, "{num_u64=", num_u64, ", addr=[", addr, "]}");
See test_databuff_printf.cpp for more details.
Note the following special formatting:
+--------------+------------+
| type         | format     |
+--------------+------------+
| double       | %.18le     |
| float        | %.9e       |
| bool         | True/False |
| null pointer | NULL       |
+--------------+------------+
*/
template <typename ...Rest>
int databuff_print_multi_objs(char* buf, const int64_t buf_len, int64_t &pos, const Rest &...others)
{
    return databuff_print_multi_objs_helper(buf, buf_len, pos, others...);
}

/*
ObCStringHelper: convert the object to cstring with an inner buffer
If you need a persistent cstring, you cannot use this class.
*/
class ObCStringHelper
{
  const static int64_t EXPAND_BUF_LEN = 16L << 10;
  const static int64_t HELPER_MEMORY_LIMIT = 64L << 20;
public:
  ObCStringHelper()
    : allocator_(SET_USE_500("CStringHelper"))
  {
#ifdef ENABLE_DEBUG_LOG
    force_alloc();
#else
    if (is_force_alloc()) {
      force_alloc();
    }
#endif
  }
  template<typename T>
  const char *convert(const T &obj)
  {
    char *ptr = nullptr;
    int64_t pos = 0;
    int ret = databuff_printf(buf_+ pos_, buf_len_ - pos_, pos, obj);
    // Some to_string implementations are based on databuff_printf, and even if overflowed,
    // the return value (i.e., pos in this case) will still be equal to
    // the length of buf - 1 (i.e., buf_len_ - pos_ -1 in this case). To prevent this, we require
    // that pos must be less than buf_len_ - pos_ -1.
    if (OB_SUCC(ret) && pos < buf_len_ - pos_ - 1) {
      ptr = buf_ + pos_;
      // we need a c-style string, so the '\0' can't be overwritten
      pos_ += pos + 1;
    } else if (OB_SIZE_OVERFLOW == ret || (OB_SUCC(ret) && pos >= buf_len_ - pos_ - 1)) {
      if (allocator_.total() >= HELPER_MEMORY_LIMIT) { // 64MB
        ret = OB_ALLOCATE_MEMORY_FAILED;
        ptr = nullptr;
        LIB_LOG(ERROR, "ObCStringHelper memory has reached the upper limit",
            "hold", allocator_.total(), "limit", HELPER_MEMORY_LIMIT, K(ret));
      } else {
        char *old_buf = buf_;
        int64_t old_buf_len = buf_len_;
        if (buf_ == reserve_buf_) {
          buf_len_ = EXPAND_BUF_LEN;
        } else {
          buf_len_ = buf_len_ << 1;
        }
        if (OB_ISNULL(buf_ = reinterpret_cast<char *>(allocator_.alloc(buf_len_)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LIB_LOG(WARN, "failed to allocate memory", "length want to alloc", buf_len_, K(ret));
          buf_ = old_buf;
          buf_len_ = old_buf_len;
          ptr = nullptr;
        } else {
          pos_= 0;
          ptr = const_cast<char *>(convert(obj));
        }
      }
    } else {
      ptr = nullptr;
      LIB_LOG(WARN, "failed to convert the obj to cstring", K(ret));
    }
    ob_errno_ = ret;
    return ptr;
  }
  template<typename T>
  int convert(const T &obj, const char *&str)
  {
    int ret = OB_SUCCESS;
    str = convert(obj);
    ret = get_ob_errno();
    return ret;
  }
  void reset()
  {
    allocator_.reset();
    reserve_buf_[0] = '\0';
    buf_ = reserve_buf_;
    buf_len_ = ARRAYSIZEOF(reserve_buf_);
    pos_ = 0;
    ob_errno_ = OB_SUCCESS;
  }
  int get_ob_errno() const
  {
    return ob_errno_;
  }

private:
  bool is_force_alloc();
  void force_alloc();

private:
  DISABLE_COPY_ASSIGN(ObCStringHelper);
private:
  ObArenaAllocator allocator_;
  char reserve_buf_[1024];
  char *buf_ = reserve_buf_;
  int64_t buf_len_ = ARRAYSIZEOF(reserve_buf_);
  int64_t pos_= 0;
  int ob_errno_ = OB_SUCCESS;
};

/// @return OB_SUCCESS or OB_ALLOCATE_MEMORY_FAILED
int multiple_extend_buf(char *&stmt_buf, int64_t &stmt_buf_len, common::ObIAllocator &alloc);
/// @return OB_SUCCESS or OB_SIZE_OVERFLOW
int databuff_vprintf(char *buf, const int64_t buf_len, int64_t &pos, const char *fmt,
                     va_list args);
/// @return OB_SUCCESS or OB_SIZE_OVERFLOW
int databuff_memcpy(char *buf, const int64_t buf_len, int64_t &pos, const int64_t src_len,
                    const char *src);

/// print object
template<class T>
int databuff_print_obj(char *buf, const int64_t buf_len, int64_t &pos, const T &obj)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(databuff_print_obj(buf, buf_len, pos, obj, BoolType<__is_enum(T)>()))) {
  } else {}
  return ret;
}
/// print enum object
template<class T>
int databuff_print_obj(char *buf, const int64_t buf_len, int64_t &pos, const T &obj, TrueType)
{
  return databuff_printf(buf, buf_len, pos, "%ld", static_cast<int64_t>(obj));
}
/// print object with to_string members
template<class T>
int databuff_print_obj(char *buf, const int64_t buf_len, int64_t &pos, const T &obj, FalseType)
{
  pos += obj.to_string(buf + pos, buf_len - pos);
  return OB_SUCCESS;
}
template<class T>
int databuff_print_obj(char *buf, const int64_t buf_len, int64_t &pos, T *obj)
{
  int ret = OB_SUCCESS;
  if (NULL == obj) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "null"))) {
    } else {}
  } else if (std::is_convertible<T, T *>::value) {
    // Function type can be implicit converted to function pointer,
    // stop recursion by check the implicit convert.
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%p", obj))) {
    }
  } else {
    if (OB_FAIL(databuff_print_obj(buf, buf_len, pos, *obj))) {
    } else {}
  }
  return ret;
}
template<class T>
int databuff_print_obj(char *buf, const int64_t buf_len, int64_t &pos, const T *obj)
{
  int ret = OB_SUCCESS;
  if (NULL == obj) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "null"))) {
    } else {}
  } else if (std::is_convertible<T, T *>::value) {
    // Function type can be implicit converted to function pointer,
    // stop recursion by check the implicit convert.
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%p", obj))) {
    }
  } else {
    if (OB_FAIL(databuff_print_obj(buf, buf_len, pos, *obj))) {
    } else {}
  }
  return ret;
}
inline int databuff_print_obj(char *buf, const int64_t buf_len, int64_t &pos, void *obj)
{
  int ret = OB_SUCCESS;
  if (NULL == obj) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "null"))) {
    } else {}
  } else {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%p", obj))) {
    } else {}
  }
  return ret;
}
inline int databuff_print_obj(char *buf, const int64_t buf_len, int64_t &pos, const void *obj)
{
  int ret = OB_SUCCESS;
  if (NULL == obj) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "null"))) {
    } else {}
  } else {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%p", obj))) {
    } else {}
  }
  return ret;
}
inline int databuff_print_obj(char *buf, const int64_t buf_len, int64_t &pos, const volatile int64_t &obj)
{
  return databuff_printf(buf, buf_len, pos, "%ld", obj);
}
template<>
inline int databuff_print_obj(char *buf, const int64_t buf_len, int64_t &pos, const uint64_t &obj)
{
  return databuff_printf(buf, buf_len, pos, "%lu", obj);
}
template<>
inline int databuff_print_obj(char *buf, const int64_t buf_len, int64_t &pos, const volatile uint64_t &obj)
{
  return databuff_printf(buf, buf_len, pos, "%lu", obj);
}
template<>
inline int databuff_print_obj(char *buf, const int64_t buf_len, int64_t &pos, const int64_t &obj)
{
  return databuff_printf(buf, buf_len, pos, "%ld", obj);
}
template<>
inline int databuff_print_obj(char *buf, const int64_t buf_len, int64_t &pos, const uint32_t &obj)
{
  return databuff_printf(buf, buf_len, pos, "%u", obj);
}
template<>
inline int databuff_print_obj(char *buf, const int64_t buf_len, int64_t &pos, const volatile int32_t &obj)
{
  return databuff_printf(buf, buf_len, pos, "%d", obj);
}
template<>
inline int databuff_print_obj(char *buf, const int64_t buf_len, int64_t &pos, const int32_t &obj)
{
  return databuff_printf(buf, buf_len, pos, "%d", obj);
}
template<>
inline int databuff_print_obj(char *buf, const int64_t buf_len, int64_t &pos, const uint16_t &obj)
{
  return databuff_printf(buf, buf_len, pos, "%u", obj);
}
template<>
inline int databuff_print_obj(char *buf, const int64_t buf_len, int64_t &pos, const int16_t &obj)
{
  return databuff_printf(buf, buf_len, pos, "%d", obj);
}
template<>
inline int databuff_print_obj(char *buf, const int64_t buf_len, int64_t &pos, const uint8_t &obj)
{
  return databuff_printf(buf, buf_len, pos, "%hhu", obj);
}
template<>
inline int databuff_print_obj(char *buf, const int64_t buf_len, int64_t &pos, const int8_t &obj)
{
  return databuff_printf(buf, buf_len, pos, "%hhd", obj);
}
template<>
inline int databuff_print_obj(char *buf, const int64_t buf_len, int64_t &pos, const char &obj)
{
  return databuff_printf(buf, buf_len, pos, "%c", obj);
}
template<>
inline int databuff_print_obj(char *buf, const int64_t buf_len, int64_t &pos, const float &obj)
{
  return databuff_printf(buf, buf_len, pos, "\"%.9e\"", obj);
}
template<>
inline int databuff_print_obj(char *buf, const int64_t buf_len, int64_t &pos, const double &obj)
{
  return databuff_printf(buf, buf_len, pos, "\"%.18le\"", obj);
}
template<>
inline int databuff_print_obj(char *buf, const int64_t buf_len, int64_t &pos, const volatile bool &obj)
{
  return databuff_printf(buf, buf_len, pos, "%s", obj ? N_TRUE : N_FALSE);
}
template<>
inline int databuff_print_obj(char *buf, const int64_t buf_len, int64_t &pos, const bool &obj)
{
  return databuff_printf(buf, buf_len, pos, "%s", obj ? N_TRUE : N_FALSE);
}
template<>
inline int databuff_print_obj(char *buf, const int64_t buf_len, int64_t &pos,
                              const char *const &obj)
{
  int ret = OB_SUCCESS;
  if (NULL == obj) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "null"))) {
    } else {}
  } else {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "\"%s\"", obj))) {
    } else {}
  }
  return ret;
}
template<>
inline int databuff_print_obj(char *buf, const int64_t buf_len, int64_t &pos, const char *obj)
{
  int ret = OB_SUCCESS;
  if (NULL == obj) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "null"))) {
    } else {}
  } else {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "\"%s\"", obj))) {
    } else {}
  }
  return ret;
}

template<>
int databuff_print_obj(char *buf, const int64_t buf_len, int64_t &pos, const ObString &obj);

template<typename T1, typename T2>
    inline int databuff_print_obj(char *buf, const int64_t buf_len, int64_t &pos, const std::pair<T1, T2> &obj)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf, buf_len, pos, "["))) {
  } else if (OB_FAIL(databuff_print_obj(buf, buf_len, pos, obj.first))) {
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, ", "))) {
  } else if (OB_FAIL(databuff_print_obj(buf, buf_len, pos, obj.second))) {
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "]"))) {
  } else {}
  return ret;
}

template<int64_t N>
inline int databuff_print_obj(char *buf, const int64_t buf_len,
                              int64_t &pos, const ObFixedLengthString<N> &obj)
{
  return databuff_printf(buf, buf_len, pos, "\"%s\"", obj.ptr());
}

//============
/// print object
template<class T>
int databuff_print_key_obj(char *buf, const int64_t buf_len, int64_t &pos, const char *key,
                           const bool with_comma, const T &obj)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    ret = databuff_print_key_obj(buf, buf_len, pos, key, with_comma, obj, BoolType<__is_enum(T)>());
  }
  return ret;
}
/// print enum object
template<class T>
int databuff_print_key_obj(char *buf, const int64_t buf_len, int64_t &pos, const char *key,
                           const bool with_comma, const T &obj, TrueType)
{
  return databuff_printf(buf, buf_len, pos, WITH_COMMA("%s:%ld"), key, static_cast<int64_t>(obj));
}
/// print object with to_string members
template<class T>
int databuff_print_key_obj(char *buf, const int64_t buf_len, int64_t &pos, const char *key,
                           const bool with_comma, const T &obj, FalseType)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(databuff_printf(buf, buf_len, pos, WITH_COMMA("%s:"), key))) {
    pos += obj.to_string(buf + pos, buf_len - pos);
  }
  return OB_SUCCESS;
}
template<class T>
int databuff_print_key_obj(char *buf, const int64_t buf_len, int64_t &pos, const char *key,
                           const bool with_comma, T *obj)
{
  int ret = OB_SUCCESS;
  if (NULL == obj) {
    ret = databuff_printf(buf, buf_len, pos, WITH_COMMA("%s:null"), key);
  } else if (std::is_convertible<T, T *>::value) {
    // Function type can be implicit converted to function pointer,
    // stop recursion by check the implicit convert.
    ret = databuff_printf(buf, buf_len, pos, WITH_COMMA("%s:%p"), key, obj);
  } else {
    ret = databuff_print_key_obj(buf, buf_len, pos, key, with_comma, *obj);
  }
  return ret;
}
template<class T>
int databuff_print_key_obj(char *buf, const int64_t buf_len, int64_t &pos, const char *key,
                           const bool with_comma, const T *obj)
{
  int ret = OB_SUCCESS;
  if (NULL == obj) {
    ret = databuff_printf(buf, buf_len, pos, WITH_COMMA("%s:null"), key);
  } else if (std::is_convertible<T, T *>::value) {
    // Function type can be implicit converted to function pointer,
    // stop recursion by check the implicit convert.
    ret = databuff_printf(buf, buf_len, pos, WITH_COMMA("%s:%p"), key, obj);
  } else {
    ret = databuff_print_key_obj(buf, buf_len, pos, key, with_comma, *obj);
  }
  return ret;
}
inline int databuff_print_key_obj(char *buf, const int64_t buf_len, int64_t &pos, const char *key,
                                  const bool with_comma, void *obj)
{
  int ret = OB_SUCCESS;
  if (NULL == obj) {
    ret = databuff_printf(buf, buf_len, pos, WITH_COMMA("%s:null"), key);
  } else {
    ret = databuff_printf(buf, buf_len, pos, WITH_COMMA("%s:%p"), key, obj);
  }
  return ret;
}
inline int databuff_print_key_obj(char *buf, const int64_t buf_len, int64_t &pos, const char *key,
                                  const bool with_comma, const void *obj)
{
  int ret = OB_SUCCESS;
  if (NULL == obj) {
    ret = databuff_printf(buf, buf_len, pos, WITH_COMMA("%s:null"), key);
  } else {
    ret = databuff_printf(buf, buf_len, pos, WITH_COMMA("%s:%p"), key, obj);
  }
  return ret;
}
inline int databuff_print_key_obj(char *buf, const int64_t buf_len, int64_t &pos, const char *key,
                                  const bool with_comma, const volatile int64_t &obj)
{
  return databuff_printf(buf, buf_len, pos, WITH_COMMA("%s:%ld"), key, obj);;
}
template<>
inline int databuff_print_key_obj(char *buf, const int64_t buf_len, int64_t &pos, const char *key, const bool with_comma, const uint64_t &obj)
{
  return databuff_printf(buf, buf_len, pos, WITH_COMMA("%s:%lu"), key, obj);;
}
template<>
inline int databuff_print_key_obj(char *buf, const int64_t buf_len, int64_t &pos, const char *key, const bool with_comma, const volatile uint64_t &obj)
{
  return databuff_printf(buf, buf_len, pos, WITH_COMMA("%s:%lu"), key, obj);;
}
template<>
inline int databuff_print_key_obj(char *buf, const int64_t buf_len, int64_t &pos, const char *key, const bool with_comma, const int64_t &obj)
{
  return databuff_printf(buf, buf_len, pos, WITH_COMMA("%s:%ld"), key, obj);;
}
template<>
inline int databuff_print_key_obj(char *buf, const int64_t buf_len, int64_t &pos, const char *key, const bool with_comma, const uint32_t &obj)
{
  return databuff_printf(buf, buf_len, pos, WITH_COMMA("%s:%u"), key, obj);;
}
template<>
inline int databuff_print_key_obj(char *buf, const int64_t buf_len, int64_t &pos, const char *key, const bool with_comma, const volatile int32_t &obj)
{
  return databuff_printf(buf, buf_len, pos, WITH_COMMA("%s:%d"), key, obj);;
}
template<>
inline int databuff_print_key_obj(char *buf, const int64_t buf_len, int64_t &pos, const char *key, const bool with_comma, const int32_t &obj)
{
  return databuff_printf(buf, buf_len, pos, WITH_COMMA("%s:%d"), key, obj);;
}
template<>
inline int databuff_print_key_obj(char *buf, const int64_t buf_len, int64_t &pos, const char *key, const bool with_comma, const uint16_t &obj)
{
  return databuff_printf(buf, buf_len, pos, WITH_COMMA("%s:%u"), key, obj);;
}
template<>
inline int databuff_print_key_obj(char *buf, const int64_t buf_len, int64_t &pos, const char *key, const bool with_comma, const int16_t &obj)
{
  return databuff_printf(buf, buf_len, pos, WITH_COMMA("%s:%d"), key, obj);;
}
template<>
inline int databuff_print_key_obj(char *buf, const int64_t buf_len, int64_t &pos, const char *key, const bool with_comma, const uint8_t &obj)
{
  return databuff_printf(buf, buf_len, pos, WITH_COMMA("%s:%hhu"), key, obj);;
}
template<>
inline int databuff_print_key_obj(char *buf, const int64_t buf_len, int64_t &pos, const char *key, const bool with_comma, const int8_t &obj)
{
  return databuff_printf(buf, buf_len, pos, WITH_COMMA("%s:%hhd"), key, obj);;
}
template<>
inline int databuff_print_key_obj(char *buf, const int64_t buf_len, int64_t &pos, const char *key, const bool with_comma, const char &obj)
{
  return databuff_printf(buf, buf_len, pos, WITH_COMMA("%s:%c"), key, obj);;
}
template<>
inline int databuff_print_key_obj(char *buf, const int64_t buf_len, int64_t &pos, const char *key, const bool with_comma, const float &obj)
{
  return databuff_printf(buf, buf_len, pos, WITH_COMMA("%s:\"%.9e\""), key, obj);;
}
template<>
inline int databuff_print_key_obj(char *buf, const int64_t buf_len, int64_t &pos, const char *key, const bool with_comma, const double &obj)
{
  return databuff_printf(buf, buf_len, pos, WITH_COMMA("%s:\"%.18le\""), key, obj);;
}
template<>
inline int databuff_print_key_obj(char *buf, const int64_t buf_len, int64_t &pos, const char *key, const bool with_comma, const volatile bool &obj)
{
  return databuff_printf(buf, buf_len, pos, WITH_COMMA("%s:%s"), key, obj ? N_TRUE : N_FALSE);
}
template<>
inline int databuff_print_key_obj(char *buf, const int64_t buf_len, int64_t &pos, const char *key,
                                  const bool with_comma, const bool &obj)
{
  return databuff_printf(buf, buf_len, pos, WITH_COMMA("%s:%s"), key, obj ? N_TRUE : N_FALSE);
}
template<>
inline int databuff_print_key_obj(char *buf, const int64_t buf_len, int64_t &pos,
                                  const char *key, const bool with_comma, const char *const &obj)
{
  int ret = OB_SUCCESS;
  if (NULL == obj) {
    ret = databuff_printf(buf, buf_len, pos, WITH_COMMA("%s:null"), key);
  } else {
    ret = databuff_printf(buf, buf_len, pos, WITH_COMMA("%s:\"%s\""), key, obj);
  }
  return ret;
}
template<>
inline int databuff_print_key_obj(char *buf, const int64_t buf_len, int64_t &pos, const char *key,
                                  const bool with_comma, const char *obj)
{
  int ret = OB_SUCCESS;
  if (NULL == obj) {
    ret = databuff_printf(buf, buf_len, pos, WITH_COMMA("%s:null"), key);
  } else {
    ret = databuff_printf(buf, buf_len, pos, WITH_COMMA("%s:\"%s\""), key, obj);
  }
  return ret;
}
template<>
int databuff_print_key_obj(char *buf, const int64_t buf_len, int64_t &pos, const char *key,
                                  const bool with_comma, const ObString &obj);
template<typename T1, typename T2>
    inline int databuff_print_key_obj(char *buf, const int64_t buf_len, int64_t &pos, const char *key,
                                      const bool with_comma, const std::pair<T1, T2> &obj)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf, buf_len, pos, WITH_COMMA("%s:["), key))) {
  } else if (OB_FAIL(databuff_print_obj(buf, buf_len, pos, obj.first))) {
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, ", "))) {
  } else if (OB_FAIL(databuff_print_obj(buf, buf_len, pos, obj.second))) {
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "]"))) {
  } else {}
  return ret;
}

template<int64_t N>
inline int databuff_print_key_obj(char *buf, const int64_t buf_len,
                              int64_t &pos, const char *key,
                              const bool with_comma, const ObFixedLengthString<N> &obj)
{
  return databuff_printf(buf, buf_len, pos, WITH_COMMA("%s:\"%s\""), key, obj.ptr());
}

/// print JSON-style key-value pair
template <class T>
int databuff_print_json_kv(char *buf, const int64_t buf_len, int64_t &pos, const char *key,
                           const T &obj)
{
  int ret = OB_SUCCESS;
  if (NULL == key) {
    ret = databuff_print_obj(buf, buf_len, pos, obj);
  } else {
    ret = databuff_print_key_obj(buf, buf_len, pos, key, false, obj);
  }
  return ret;
}
template <class T>
int databuff_print_json_kv_comma(char *buf, const int64_t buf_len, int64_t &pos, const char *key,
                                 const T &obj)
{
  int ret = OB_SUCCESS;
  if (NULL == key) {
    ret = databuff_print_key_obj(buf, buf_len, pos, ", ", false, obj);
  } else {
    ret = databuff_print_key_obj(buf, buf_len, pos, key, true, obj);
  }
  return ret;
}

/// print array of objects
template<class T>
int databuff_print_obj_array(char *buf, const int64_t buf_len, int64_t &pos, const T *obj,
                             const int64_t size)
{
  int ret = OB_SUCCESS;
  if (NULL == obj) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "null"))) {
    } else {}
  } else {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "["))) {
    } else {}
    for (int64_t i = 0; OB_SUCC(ret) && i < size; ++i) {
      if (OB_FAIL(databuff_print_obj(buf, buf_len, pos, obj[i]))) {
      } else if (i != size - 1) {
        if (OB_FAIL(databuff_printf(buf, buf_len, pos, ", "))) {
        } else {}
      } else {}
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "]"))) {
    } else {}
  }
  return ret;
}

} // end namespace common
} // end namespace oceanbase

#define DECLARE_TO_STRING int64_t to_string(char* buf, const int64_t buf_len) const
#define DECLARE_VIRTUAL_TO_STRING virtual int64_t to_string(char* buf, const int64_t buf_len) const
#define DECLARE_PURE_VIRTUAL_TO_STRING DECLARE_VIRTUAL_TO_STRING = 0
#define DEF_TO_STRING(CLS) int64_t CLS::to_string(char* buf, const int64_t buf_len) const
#define DEFINE_TO_STRING(body) DECLARE_TO_STRING    \
  {                                                 \
    int64_t pos = 0;                                \
    J_OBJ_START();                                  \
    body;                                           \
    J_OBJ_END();                                    \
    return pos;                                     \
  }
#define DEFINE_VIRTUAL_TO_STRING(body) DECLARE_VIRTUAL_TO_STRING    \
  {                                                 \
    int64_t pos = 0;                                \
    J_OBJ_START();                                  \
    body;                                           \
    J_OBJ_END();                                    \
    return pos;                                     \
  }

#define DEFINE_INHERIT_TO_STRING(parent_name, parent_class, body) DECLARE_TO_STRING    \
    {                                                 \
      int64_t pos = 0;                                \
      J_OBJ_START();                                  \
      J_NAME(parent_name);                            \
      J_COLON();                                      \
      pos += parent_class::to_string(buf + pos, buf_len - pos);  \
      J_COMMA();                                      \
      body;                                           \
      J_OBJ_END();                                    \
      return pos;                                     \
    }
#define DEFINE_TO_STRING_WITH_HELPER(body) DECLARE_TO_STRING    \
  {                                                 \
    int64_t pos = 0;                                \
    ObCStringHelper helper;                         \
    J_OBJ_START();                                  \
    body;                                           \
    J_OBJ_END();                                    \
    return pos;                                     \
  }

#define BUF_PRINTF(args...) ::oceanbase::common::databuff_printf(buf, buf_len, pos, ##args)
#define BUF_PRINTO(obj) (void)::oceanbase::common::databuff_print_obj(buf, buf_len, pos, obj)

// #define J_NAME(key) BUF_PRINTF("\"%s\"", (key))
// We do not quote json names to make the log message more readable.

#define J_NAME(key) BUF_PRINTF("%s", (key))
#define J_COLON() BUF_PRINTF(":")
#define J_COMMA() BUF_PRINTF(", ")
#define J_QUOTE() BUF_PRINTF("\"")
#define J_OBJ_START() BUF_PRINTF("{")
#define J_OBJ_END() BUF_PRINTF("}")
#define J_ARRAY_START() BUF_PRINTF("[")
#define J_ARRAY_END() BUF_PRINTF("]")
#define J_NULL() BUF_PRINTF("null")
#define J_NOP() BUF_PRINTF("NOP")
#define J_EMPTY_OBJ() BUF_PRINTF("{}")
#define J_NEWLINE() BUF_PRINTF("\n")

#include "lib/utility/ob_print_kv.h"
#define J_KV(args...) ::oceanbase::common::databuff_print_kv(buf, buf_len, pos, ##args)

#define TO_STRING_KV(args...) DEFINE_TO_STRING(J_KV(args))
#define TO_STRING_KV_WITH_HELPER(args...) DEFINE_TO_STRING_WITH_HELPER(J_KV(args))
#define VIRTUAL_TO_STRING_KV(args...) DEFINE_VIRTUAL_TO_STRING(J_KV(args))
#define INHERIT_TO_STRING_KV(parent_name, parent_class, args...) \
    DEFINE_INHERIT_TO_STRING(parent_name, parent_class, J_KV(args))
#define TO_STRING_EMPTY() DECLARE_TO_STRING         \
    {                                               \
      int64_t pos = 0;                              \
      J_OBJ_START();                                \
      J_OBJ_END();                                  \
      return pos;                                   \
    }

// for compatible
#define TO_STRING_KV2 TO_STRING_KV
#define TO_STRING_KV3 TO_STRING_KV
#define TO_STRING_KV4 TO_STRING_KV
#define TO_STRING_KV5 TO_STRING_KV

#define J_OBJ(obj)                     \
  J_OBJ_START();                       \
  BUF_PRINTO((obj));                   \
  J_OBJ_END()

#define J_KO(key, obj)                 \
  BUF_PRINTF("%s:", (key));        \
  J_OBJ(obj)

#define J_KW(key, body)                 \
  BUF_PRINTF("%s:", (key));        \
  J_OBJ_START();                       \
  body;                   \
  J_OBJ_END()

#define J_OW(body)                     \
  J_OBJ_START();                       \
  body;                   \
  J_OBJ_END()

#define J_KN(key)                 \
  BUF_PRINTF("%s:null", (key))

#define STR_BOOL(b) ((b) ? "true" : "false")

#define DECLARE_TO_STRING_AND_YSON                                      \
  int to_yson(char *buf, const int64_t buf_len, int64_t &pos) const;    \
  DECLARE_TO_STRING;

#define DECLARE_TO_YSON_KV int to_yson(char *buf, const int64_t buf_len, int64_t &pos) const

template <int FIRST, int SECOND, typename T>
struct ObIntegerCombine {
  STATIC_ASSERT(FIRST > 0 && SECOND > 0 && (FIRST + SECOND == sizeof(T) * 8),
      "bit count mismatch");
  explicit ObIntegerCombine(const T v) : v_(v) {}
  ObIntegerCombine(const T first, const T second)
  {
    v_ = (first << SECOND) | ObIntegerCombine(second).second();
  }

  T first() const { return v_ >> SECOND; }
  T second() const { return v_ & (~((~(static_cast<T>(0))) << SECOND)); }

  TO_STRING_KV("value", v_, "first", first(), "second", second());

  T v_;
};
#define KT(var) K(var)
#define KT_(var) K_(var)

#endif /* LIB_UTILITY_OB_PRINT_UTILS_ */
