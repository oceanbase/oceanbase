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

#ifndef OCEABASE_COMMON_OB_LOG_PRINT_KV_H_
#define OCEABASE_COMMON_OB_LOG_PRINT_KV_H_
#include <cstdarg>
#include <cerrno>
#include <type_traits>

#include "lib/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_template_utils.h"
#include "lib/alloc/alloc_assist.h"

#define LOG_N_TRUE "true"
#define LOG_N_FLASE "false"
namespace oceanbase
{
namespace common
{
#define COMMA_FORMAT ", "
#define WITH_COMMA(format)  (with_comma ? COMMA_FORMAT format: format)

#define LOG_MACRO_JOIN(x, y) LOG_MACRO_JOIN1(x, y)
#define LOG_MACRO_JOIN1(x, y) x##y

#define _LOG_MACRO_JOIN(x, y) _LOG_MACRO_JOIN1(x, y)
#define _LOG_MACRO_JOIN1(x, y) _##x##y

/// @return OB_SUCCESS or OB_BUF_NOT_ENOUGH
int logdata_printf(char *buf, const int64_t buf_len, int64_t &pos, const char *fmt, ...)
__attribute__((format(printf, 4, 5)));
int logdata_vprintf(char *buf, const int64_t buf_len, int64_t &pos, const char *fmt, va_list args);

#define LOG_STDERR(...) do {if(1 == isatty(STDERR_FILENO)) {fprintf(stderr, __VA_ARGS__); }} while(0)
#define LOG_STDOUT(...) do {if(1 == isatty(STDOUT_FILENO)) {fprintf(stdout, __VA_ARGS__); }} while(0)

/**
 * used by macro K
 * check the type of v is not one of char *, const char *, char[], char[N]
 */
template <typename T>
constexpr const T & check_char_array(const T &v)
{
  #if 0
  static_assert((false == std::is_array<T>::value
		 || false == std::is_same<char, typename std::remove_all_extents<T>::type>::value
		 )
		&&
		(false == std::is_pointer<T>::value
		  || (false == std::is_same<char *, typename std::remove_cv<T>::type>::value
		    && false == std::is_same<const char *, typename std::remove_cv<T>::type>::value)
		 ),
                "logging zero terminated string by K is not supported any more and use KCSTRING instead");
  #endif
  return v;
}

/**
 * this is a simple wrapper of C string and used to print C string (string terminated with zero, sz) by macro KCSTRING
 */
class ObSzString
{
public:
  explicit ObSzString(const char *s) : str_(s)
  {}
  ~ObSzString()
  {}

  int64_t to_string(char *buf, const int64_t len) const
  {
    int64_t pos = 0;
    if (OB_ISNULL(str_)) {
      (void)logdata_printf(buf, len, pos, "NULL");
    } else {
      (void)logdata_printf(buf, len, pos, "%s", str_);
    }
    return pos;
  }
private:
  const char *str_;
};

class ObQuoteSzString
{
public:
  explicit ObQuoteSzString(const char *s) : str_(s)
  {}
  ~ObQuoteSzString()
  {}

  int64_t to_string(char *buf, const int64_t len) const
  {
    int64_t pos = 0;
    if (OB_ISNULL(str_)) {
      (void)logdata_printf(buf, len, pos, "NULL");
    } else {
      (void)logdata_printf(buf, len, pos, "\"%s\"", str_);
    }
    return pos;
  }
private:
  const char *str_;
};

/**
 * used to print string that maybe not terminated by zero and has a max lenght
 */
class ObLenString
{
public:
  explicit ObLenString(const char *s, int len) : str_(s), len_(len)
  {}

  int64_t to_string(char *buf, const int64_t len) const
  {
    int64_t pos = 0;
    if (OB_LIKELY(NULL != buf && len > 0)) {
      int64_t max_cpy = MIN(len, len_);
      STRNCPY(buf, str_, max_cpy);
      while (buf[pos] != '\0' && pos < max_cpy)
	pos++;
    }
    return pos;
  }
private:
  const char *str_;
  int len_;
};

template <typename T>
struct ObLogPrintPointerCnt
{
  explicit ObLogPrintPointerCnt(T v) : v_(v)
  { }
  int64_t to_string(char *buf, const int64_t len) const
  {
    int64_t pos = 0;
    if (OB_LIKELY(NULL != buf) && OB_LIKELY(len > 0)) {
      if (OB_ISNULL(v_)) {
        (void)logdata_printf(buf, len, pos, "NULL");
      } else {
        pos += v_->to_string(buf, len);
      }
    }
    return pos;
  }
  T v_;
};

//FalseType not pointer
template <typename T>
int logdata_print_name(char *buf, const int64_t buf_len, int64_t &pos, const T &obj, FalseType)
{
  int ret = OB_SUCCESS;
  ret = obj.get_name(buf, buf_len, pos);
  return ret;
}

template <typename T>
int logdata_print_name(char *buf, const int64_t buf_len, int64_t &pos, const T *obj, TrueType)
{
  int ret = OB_SUCCESS;
  if (NULL == obj) {
    ret = logdata_printf(buf, buf_len, pos, "NULL");
  } else {
    ret = obj->get_name(buf, buf_len, pos);
  }
  return ret;
}

template <typename T>
struct ObLogPrintName
{
  explicit ObLogPrintName(const T &v) : v_(v)
  { }
  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    int ret = OB_SUCCESS;
    if (OB_FAIL(logdata_printf(buf, buf_len, pos, "\""))) {
    } else if (OB_FAIL(logdata_print_name(buf, buf_len, pos, v_, IsPointer<T>()))) {
    } else if (OB_FAIL(logdata_printf(buf, buf_len, pos, "\""))) {
    } else {}//do nothing
    return pos;
  }
  const T &v_;
};

//print errmsg of errno.As strerror not thread-safe, need
//to call ERRMSG, KERRMSG which use this class.
struct ObLogPrintErrMsg
{
  ObLogPrintErrMsg()
  {}
  ~ObLogPrintErrMsg()
  {}
  int64_t to_string(char *buf, const int64_t len) const
  {
    int64_t pos = 0;
    if (OB_LIKELY(NULL != buf) && OB_LIKELY(len > 0)) {
      (void)logdata_printf(buf, len, pos, "\"%m\"");
    }
    return pos;
  }
};

//print errmsg of no specified. As strerror not thread-safe, need
//to call ERRNOMSG, KERRNOMSG which use this class.
struct ObLogPrintErrNoMsg
{
  explicit ObLogPrintErrNoMsg(int no) : errno_(no)
  {}
  ~ObLogPrintErrNoMsg()
  {}
  int64_t to_string(char *buf, const int64_t len) const
  {
    int64_t pos = 0;
    int old_err = errno;
    errno = errno_;
    if (OB_LIKELY(NULL != buf) && OB_LIKELY(len > 0)) {
      (void)logdata_printf(buf, len, pos, "\"%m\"");
    }
    errno = old_err;
    return pos;
  }
  int errno_;
};

// print object with to_string() member
template<class T>
int logdata_print_obj(char *buf, const int64_t buf_len, int64_t &pos, const T &obj, FalseType)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(pos >= buf_len)) {
    ret = OB_SIZE_OVERFLOW;
  } else {
    pos += obj.to_string(buf + pos, buf_len - pos);
  }
  return ret;
}
// print enum object
template<class T>
int logdata_print_obj(char *buf, const int64_t buf_len, int64_t &pos, const T &obj, TrueType)
{
  return logdata_printf(buf, buf_len, pos, "%ld", static_cast<int64_t>(obj));
}
// print object
template<class T>
int logdata_print_obj(char *buf, const int64_t buf_len, int64_t &pos, const T &obj)
{
  int ret = OB_SUCCESS;
  if (NULL == &obj) {
    ret = logdata_printf(buf, buf_len, pos, "NULL");
  } else {
    ret = logdata_print_obj(buf, buf_len, pos, obj, BoolType<__is_enum(T)>());
  }
  return ret;
}
template<class T>
int logdata_print_point_obj(char *buf, const int64_t buf_len, int64_t &pos, const T *obj, TrueType)
{
  return logdata_print_obj(buf, buf_len, pos, *obj);
}
template<class T>
int logdata_print_point_obj(char *buf, const int64_t buf_len, int64_t &pos, const T *obj, FalseType)
{
  return logdata_printf(buf, buf_len, pos, "%p", obj);
}
template<class T>
int logdata_print_obj(char *buf, const int64_t buf_len, int64_t &pos, T *obj)
{
  int ret = OB_SUCCESS;
  if (NULL == obj) {
    ret = logdata_printf(buf, buf_len, pos, "NULL");
  } else {
    ret = logdata_print_point_obj(buf, buf_len, pos, obj, BoolType<HAS_MEMBER(T, to_string)>());
  }
  return ret;
}
template<class T>
int logdata_print_obj(char *buf, const int64_t buf_len, int64_t &pos, const T *obj)
{
  int ret = OB_SUCCESS;
  if (NULL == obj) {
    ret = logdata_printf(buf, buf_len, pos, "NULL");
  } else {
    ret = logdata_print_point_obj(buf, buf_len, pos, obj, BoolType<HAS_MEMBER(T, to_string)>());
  }
  return ret;
}
extern int logdata_print_obj(char *buf, const int64_t buf_len, int64_t &pos, const uint64_t obj);
extern int logdata_print_obj(char *buf, const int64_t buf_len, int64_t &pos, const int64_t obj);

inline int logdata_print_obj(char *buf, const int64_t buf_len, int64_t &pos, const uint32_t obj)
{
  return logdata_print_obj(buf, buf_len, pos, static_cast<uint64_t>(obj));
}
inline int logdata_print_obj(char *buf, const int64_t buf_len, int64_t &pos, const int32_t obj)
{
  return logdata_print_obj(buf, buf_len, pos, static_cast<int64_t>(obj));
}
inline int logdata_print_obj(char *buf, const int64_t buf_len, int64_t &pos, const uint16_t obj)
{
  return logdata_print_obj(buf, buf_len, pos, static_cast<uint64_t>(obj));
}
inline int logdata_print_obj(char *buf, const int64_t buf_len, int64_t &pos, const int16_t obj)
{
  return logdata_print_obj(buf, buf_len, pos, static_cast<int64_t>(obj));
}
inline int logdata_print_obj(char *buf, const int64_t buf_len, int64_t &pos, const uint8_t obj)
{
  return logdata_print_obj(buf, buf_len, pos, static_cast<uint64_t>(obj));
}
inline int logdata_print_obj(char *buf, const int64_t buf_len, int64_t &pos, const int8_t obj)
{
  return logdata_print_obj(buf, buf_len, pos, static_cast<int64_t>(obj));
}
inline int logdata_print_obj(char *buf, const int64_t buf_len, int64_t &pos, const char obj)
{
  return logdata_printf(buf, buf_len, pos, "%c", obj);
}
inline int logdata_print_obj(char *buf, const int64_t buf_len, int64_t &pos, const float obj)
{
  return logdata_printf(buf, buf_len, pos, "%.9e", obj);
//  return logdata_printf(buf, buf_len, pos, "%f", obj);
}
inline int logdata_print_obj(char *buf, const int64_t buf_len, int64_t &pos, const double obj)
{
  return logdata_printf(buf, buf_len, pos, "%.18le", obj);
//  return logdata_printf(buf, buf_len, pos, "%.12f", obj);
}
inline int logdata_print_obj(char *buf, const int64_t buf_len, int64_t &pos, const bool obj)
{
  return logdata_printf(buf, buf_len, pos, "%s", obj ? LOG_N_TRUE : LOG_N_FLASE);
}
inline int logdata_print_obj(char *buf, const int64_t buf_len, int64_t &pos, char *obj)
{
  int ret = OB_SUCCESS;
  if (NULL == obj) {
    ret = logdata_printf(buf, buf_len, pos, "NULL");
  } else {
    ret = logdata_printf(buf, buf_len, pos, "\"%s\"", obj);
  }
  return ret;
}
inline int logdata_print_obj(char *buf, const int64_t buf_len, int64_t &pos, const char *obj)
{
  int ret = OB_SUCCESS;
  if (NULL == obj) {
    ret = logdata_printf(buf, buf_len, pos, "NULL");
  } else {
    ret = logdata_printf(buf, buf_len, pos, "\"%s\"", obj);
  }
  return ret;
}
inline int logdata_print_info(char *buf, const int64_t buf_len, int64_t &pos, const char *info)
{
  int ret = OB_SUCCESS;
  if (NULL == info) {
    ret = logdata_printf(buf, buf_len, pos, "NULL");
  } else {
    ret = logdata_printf(buf, buf_len, pos, "%s", info);
  }
  return ret;
}

inline int logdata_print_info_begin(char *buf, const int64_t buf_len, int64_t &pos, const char *info)
{
  int ret = OB_SUCCESS;
  if (NULL == info) {
    ret = logdata_printf(buf, buf_len, pos, "NULL(");
  } else {
    ret = logdata_printf(buf, buf_len, pos, "%s(", info);
  }
  return ret;
}

// print object with to_string() member
template<class T>
int logdata_print_key_obj(char *buf, const int64_t buf_len, int64_t &pos, const char *key,
                          const bool with_comma, const T &obj, FalseType)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(pos >= buf_len)) {
    ret = OB_SIZE_OVERFLOW;
  } else if (OB_FAIL(logdata_printf(buf, buf_len, pos, WITH_COMMA("%s="), key))) {
    //
  } else {
    pos += obj.to_string(buf + pos, buf_len - pos);
  }
  return ret;
}
// print enum object
template<class T>
int logdata_print_key_obj(char *buf, const int64_t buf_len, int64_t &pos, const char *key,
                          const bool with_comma, const T &obj, TrueType)
{
  return logdata_printf(buf, buf_len, pos, WITH_COMMA("%s=%ld"), key, static_cast<int64_t>(obj));
}
// print object
template<class T>
int logdata_print_key_obj(char *buf, const int64_t buf_len, int64_t &pos, const char *key,
                          const bool with_comma, const T &obj)
{
  int ret = OB_SUCCESS;
  if (reinterpret_cast<const void*>(&obj) == nullptr) {
    ret = logdata_printf(buf, buf_len, pos, WITH_COMMA("%s=NULL"), key);
  } else {
    ret = logdata_print_key_obj(buf, buf_len, pos, key, with_comma, obj, BoolType<__is_enum(T)>());
  }
  return ret;
}
template<class T>
int logdata_print_key_obj(char *buf, const int64_t buf_len, int64_t &pos, const char *key,
                          const bool with_comma, T *obj)
{
  int ret = OB_SUCCESS;
  if (NULL == obj) {
    ret = logdata_printf(buf, buf_len, pos, WITH_COMMA("%s=NULL"), key);
  } else {
    ret = logdata_printf(buf, buf_len, pos, WITH_COMMA("%s=%p"), key, obj);
  }
  return ret;
}
template<class T>
int logdata_print_key_obj(char *buf, const int64_t buf_len, int64_t &pos, const char *key,
                          const bool with_comma, const T *obj)
{
  int ret = OB_SUCCESS;
  if (NULL == obj) {
    ret = logdata_printf(buf, buf_len, pos, WITH_COMMA("%s=NULL"), key);
  } else {
    ret = logdata_printf(buf, buf_len, pos, WITH_COMMA("%s=%p"), key, obj);
  }
  return ret;
}
inline int logdata_print_key_obj(char *buf, const int64_t buf_len, int64_t &pos, const char *key,
                                 const bool with_comma, const uint64_t obj)
{
  return logdata_printf(buf, buf_len, pos, WITH_COMMA("%s=%lu"), key, obj);
}
inline int logdata_print_key_obj(char *buf, const int64_t buf_len, int64_t &pos, const char *key,
                                 const bool with_comma, const int64_t obj)
{
  return logdata_printf(buf, buf_len, pos, WITH_COMMA("%s=%ld"), key, obj);
}
inline int logdata_print_key_obj(char *buf, const int64_t buf_len, int64_t &pos, const char *key,
                                 const bool with_comma, const uint32_t obj)
{
  return logdata_printf(buf, buf_len, pos, WITH_COMMA("%s=%u"), key, obj);
}
inline int logdata_print_key_obj(char *buf, const int64_t buf_len, int64_t &pos, const char *key,
                                 const bool with_comma, const int32_t obj)
{
  return logdata_printf(buf, buf_len, pos, WITH_COMMA("%s=%d"), key, obj);
}
inline int logdata_print_key_obj(char *buf, const int64_t buf_len, int64_t &pos, const char *key,
                                 const bool with_comma, const uint16_t obj)
{
  return logdata_printf(buf, buf_len, pos, WITH_COMMA("%s=%u"), key, obj);
}
inline int logdata_print_key_obj(char *buf, const int64_t buf_len, int64_t &pos, const char *key,
                                 const bool with_comma, const int16_t obj)
{
  return logdata_printf(buf, buf_len, pos, WITH_COMMA("%s=%d"), key, obj);
}
inline int logdata_print_key_obj(char *buf, const int64_t buf_len, int64_t &pos, const char *key,
                                 const bool with_comma, const uint8_t obj)
{
  return logdata_printf(buf, buf_len, pos, WITH_COMMA("%s=%u"), key, obj);
}
inline int logdata_print_key_obj(char *buf, const int64_t buf_len, int64_t &pos, const char *key,
                                 const bool with_comma, const int8_t obj)
{
  return logdata_printf(buf, buf_len, pos, WITH_COMMA("%s=%d"), key, obj);
}
inline int logdata_print_key_obj(char *buf, const int64_t buf_len, int64_t &pos, const char *key,
                                 const bool with_comma, const char obj)
{
  return logdata_printf(buf, buf_len, pos, WITH_COMMA("%s=%c"), key, obj);
}
inline int logdata_print_key_obj(char *buf, const int64_t buf_len, int64_t &pos, const char *key,
                                 const bool with_comma, const float obj)
{
  return logdata_printf(buf, buf_len, pos, WITH_COMMA("%s=%.9e"), key, obj);
}
inline int logdata_print_key_obj(char *buf, const int64_t buf_len, int64_t &pos, const char *key,
                                 const bool with_comma, const double obj)
{
  return logdata_printf(buf, buf_len, pos, WITH_COMMA("%s=%.18le"), key, obj);
}
inline int logdata_print_key_obj(char *buf, const int64_t buf_len, int64_t &pos, const char *key,
                                 const bool with_comma, const bool obj)
{
  return logdata_printf(buf, buf_len, pos, WITH_COMMA("%s=%s"), key, obj ? LOG_N_TRUE : LOG_N_FLASE);
}
inline int logdata_print_key_obj(char *buf, const int64_t buf_len, int64_t &pos, const char *key,
                                 const bool with_comma, char *obj)
{
  int ret = OB_SUCCESS;
  if (NULL == obj) {
    ret = logdata_printf(buf, buf_len, pos, WITH_COMMA("%s=NULL"), key);
  } else {
    ret = logdata_printf(buf, buf_len, pos, WITH_COMMA("%s=\"%s\""), key, obj);
  }
  return ret;
}
inline int logdata_print_key_obj(char *buf, const int64_t buf_len, int64_t &pos, const char *key,
                                 const bool with_comma, const char *obj)
{
  int ret = OB_SUCCESS;
  if (NULL == obj) {
    ret = logdata_printf(buf, buf_len, pos, WITH_COMMA("%s=NULL"), key);
  } else {
    ret = logdata_printf(buf, buf_len, pos, WITH_COMMA("%s=\"%s\""), key, obj);
  }
  return ret;
}

template <class T>
int logdata_print_kv(char *buf, const int64_t buf_len, int64_t &pos, const char *key, const T &obj)
{
  return logdata_print_key_obj(buf, buf_len, pos, key, false, obj);
}

class ObILogKV
{
public:
  virtual int print(char *buf, int64_t buf_len, int64_t &pos, const bool with_comma) const = 0;
};

template<typename T,  const bool is_rvalue>
class ObLogKV;

template<typename T>
class ObLogKV<T, false> : public ObILogKV
{
public:
  ObLogKV(const char *key, const T &value) : key_(key), value_(value) {}
  int print(char *buf, int64_t buf_len, int64_t &pos, const bool with_comma) const override
  {
    return logdata_print_key_obj(buf, buf_len, pos, key_,
                                 with_comma, value_);
  }
private:
  const char *key_;
  const T &value_;
};

template<typename T> T& unmove(T&& v) { return v; }

template<typename T>
class ObLogKV<T, true> : public ObILogKV
{
public:
  ObLogKV(const char *key, const T &&value) : key_(key), value_(unmove(value)) {}
  int print(char *buf, int64_t buf_len, int64_t &pos, const bool with_comma) const override
  {
    return logdata_print_key_obj(buf, buf_len, pos, key_,
                                 with_comma, value_);
  }
private:
  const char *key_;
  const T &value_;
};

// for dba log (alert.log)
template<class T>
inline int logdata_print_value(char *buf, const int64_t buf_len, int64_t &pos, T obj)
{
  return logdata_print_obj(buf, buf_len, pos, obj);
}
template <>
inline int logdata_print_value<char*>(char *buf, const int64_t buf_len, int64_t &pos, char *obj)
{
  int ret = OB_SUCCESS;
  if (NULL == obj) {
    ret = logdata_printf(buf, buf_len, pos, "NULL");
  } else {
    ret = logdata_printf(buf, buf_len, pos, "%s", obj);
  }
  return ret;
}
template <>
inline int logdata_print_value<const char*>(char *buf, const int64_t buf_len, int64_t &pos, const char *obj)
{
  int ret = OB_SUCCESS;
  if (NULL == obj) {
    ret = logdata_printf(buf, buf_len, pos, "NULL");
  } else {
    ret = logdata_printf(buf, buf_len, pos, "%s", obj);
  }
  return ret;
}

class ObILogValue
{
public:
  virtual int print(char *buf, int64_t buf_len, int64_t &pos) const = 0;
};

template<typename T,  const bool is_rvalue>
class ObLogValue;

template<typename T>
class ObLogValue<T, false> : public ObILogValue
{
public:
  ObLogValue(const T &value) : value_(value) {}
  int print(char *buf, int64_t buf_len, int64_t &pos) const override
  {
    return logdata_print_value(buf, buf_len, pos, value_);
  }
private:
  const T &value_;
};

template<typename T>
class ObLogValue<T, true> : public ObILogValue
{
public:
  ObLogValue(const T &&value) : value_(unmove(value)) {}
  int print(char *buf, int64_t buf_len, int64_t &pos) const override
  {
    return logdata_print_value(buf, buf_len, pos, value_);
  }
private:
  const T &value_;
};

} // common
}

#define LOG_PRINT_INFO(obj)                                                      \
  if (OB_SUCC(ret)) {                                                            \
    ret = ::oceanbase::common::logdata_print_info(data, buf_len, pos, obj);      \
  }

#define LOG_PRINT_INFO_BEGIN(obj)                                                   \
  if (OB_SUCC(ret)) {                                                               \
    ret = ::oceanbase::common::logdata_print_info_begin(data, buf_len, pos, obj);   \
  }

#define LOG_PRINT_KV(key, value)                                                      \
  if (OB_SUCC(ret)) {                                                                 \
    ret = ::oceanbase::common::logdata_print_kv(data, buf_len, pos, key, value);      \
  }

#define LOG_DATA_PRINTF(args...)                                                  \
  if (OB_SUCC(ret)) {                                                             \
    ret = ::oceanbase::common::logdata_printf(data, buf_len, pos, ##args);        \
  }

#define LOG_COMMA() LOG_DATA_PRINTF(", ")
#define LOG_KV_BEGIN() LOG_DATA_PRINTF("(")
#define LOG_KV_END()   LOG_DATA_PRINTF(")")

#define LOG_TYPENAME_TN0
#define LOG_TYPENAME_TN1 typename T1
#define LOG_TYPENAME_TN2 LOG_TYPENAME_TN1, typename T2
#define LOG_TYPENAME_TN3 LOG_TYPENAME_TN2, typename T3
#define LOG_TYPENAME_TN4 LOG_TYPENAME_TN3, typename T4
#define LOG_TYPENAME_TN5 LOG_TYPENAME_TN4, typename T5
#define LOG_TYPENAME_TN6 LOG_TYPENAME_TN5, typename T6
#define LOG_TYPENAME_TN7 LOG_TYPENAME_TN6, typename T7
#define LOG_TYPENAME_TN8 LOG_TYPENAME_TN7, typename T8
#define LOG_TYPENAME_TN9 LOG_TYPENAME_TN8, typename T9
#define LOG_TYPENAME_TN10 LOG_TYPENAME_TN9, typename T10
#define LOG_TYPENAME_TN11 LOG_TYPENAME_TN10, typename T11
#define LOG_TYPENAME_TN12 LOG_TYPENAME_TN11, typename T12
#define LOG_TYPENAME_TN13 LOG_TYPENAME_TN12, typename T13
#define LOG_TYPENAME_TN14 LOG_TYPENAME_TN13, typename T14
#define LOG_TYPENAME_TN15 LOG_TYPENAME_TN14, typename T15
#define LOG_TYPENAME_TN16 LOG_TYPENAME_TN15, typename T16
#define LOG_TYPENAME_TN17 LOG_TYPENAME_TN16, typename T17
#define LOG_TYPENAME_TN18 LOG_TYPENAME_TN17, typename T18
#define LOG_TYPENAME_TN19 LOG_TYPENAME_TN18, typename T19
#define LOG_TYPENAME_TN20 LOG_TYPENAME_TN19, typename T20
#define LOG_TYPENAME_TN21 LOG_TYPENAME_TN20, typename T21
#define LOG_TYPENAME_TN22 LOG_TYPENAME_TN21, typename T22

#define LOG_FUNC_ARG_KV(n) const char* key##n, const T##n &obj##n
#define LOG_PARAMETER_KV0
#define LOG_PARAMETER_KV1 LOG_FUNC_ARG_KV(1)
#define LOG_PARAMETER_KV2 LOG_PARAMETER_KV1, LOG_FUNC_ARG_KV(2)
#define LOG_PARAMETER_KV3 LOG_PARAMETER_KV2, LOG_FUNC_ARG_KV(3)
#define LOG_PARAMETER_KV4 LOG_PARAMETER_KV3, LOG_FUNC_ARG_KV(4)
#define LOG_PARAMETER_KV5 LOG_PARAMETER_KV4, LOG_FUNC_ARG_KV(5)
#define LOG_PARAMETER_KV6 LOG_PARAMETER_KV5, LOG_FUNC_ARG_KV(6)
#define LOG_PARAMETER_KV7 LOG_PARAMETER_KV6, LOG_FUNC_ARG_KV(7)
#define LOG_PARAMETER_KV8 LOG_PARAMETER_KV7, LOG_FUNC_ARG_KV(8)
#define LOG_PARAMETER_KV9 LOG_PARAMETER_KV8, LOG_FUNC_ARG_KV(9)
#define LOG_PARAMETER_KV10 LOG_PARAMETER_KV9, LOG_FUNC_ARG_KV(10)
#define LOG_PARAMETER_KV11 LOG_PARAMETER_KV10, LOG_FUNC_ARG_KV(11)
#define LOG_PARAMETER_KV12 LOG_PARAMETER_KV11, LOG_FUNC_ARG_KV(12)
#define LOG_PARAMETER_KV13 LOG_PARAMETER_KV12, LOG_FUNC_ARG_KV(13)
#define LOG_PARAMETER_KV14 LOG_PARAMETER_KV13, LOG_FUNC_ARG_KV(14)
#define LOG_PARAMETER_KV15 LOG_PARAMETER_KV14, LOG_FUNC_ARG_KV(15)
#define LOG_PARAMETER_KV16 LOG_PARAMETER_KV15, LOG_FUNC_ARG_KV(16)
#define LOG_PARAMETER_KV17 LOG_PARAMETER_KV16, LOG_FUNC_ARG_KV(17)
#define LOG_PARAMETER_KV18 LOG_PARAMETER_KV17, LOG_FUNC_ARG_KV(18)
#define LOG_PARAMETER_KV19 LOG_PARAMETER_KV18, LOG_FUNC_ARG_KV(19)
#define LOG_PARAMETER_KV20 LOG_PARAMETER_KV19, LOG_FUNC_ARG_KV(20)
#define LOG_PARAMETER_KV21 LOG_PARAMETER_KV20, LOG_FUNC_ARG_KV(21)
#define LOG_PARAMETER_KV22 LOG_PARAMETER_KV21, LOG_FUNC_ARG_KV(22)

#define EXPAND_ARGUMENT_0(x)
#define EXPAND_ARGUMENT_1(x) LOG_MACRO_JOIN(x, 1)
#define EXPAND_ARGUMENT_2(x) EXPAND_ARGUMENT_1(x), LOG_MACRO_JOIN(x, 2)
#define EXPAND_ARGUMENT_3(x) EXPAND_ARGUMENT_2(x), LOG_MACRO_JOIN(x, 3)
#define EXPAND_ARGUMENT_4(x) EXPAND_ARGUMENT_3(x), LOG_MACRO_JOIN(x, 4)
#define EXPAND_ARGUMENT_5(x) EXPAND_ARGUMENT_4(x), LOG_MACRO_JOIN(x, 5)
#define EXPAND_ARGUMENT_6(x) EXPAND_ARGUMENT_5(x), LOG_MACRO_JOIN(x, 6)
#define EXPAND_ARGUMENT_7(x) EXPAND_ARGUMENT_6(x), LOG_MACRO_JOIN(x, 7)
#define EXPAND_ARGUMENT_8(x) EXPAND_ARGUMENT_7(x), LOG_MACRO_JOIN(x, 8)
#define EXPAND_ARGUMENT_9(x) EXPAND_ARGUMENT_8(x), LOG_MACRO_JOIN(x, 9)
#define EXPAND_ARGUMENT_10(x) EXPAND_ARGUMENT_9(x), LOG_MACRO_JOIN(x, 10)
#define EXPAND_ARGUMENT_11(x) EXPAND_ARGUMENT_10(x), LOG_MACRO_JOIN(x, 11)
#define EXPAND_ARGUMENT_12(x) EXPAND_ARGUMENT_11(x), LOG_MACRO_JOIN(x, 12)
#define EXPAND_ARGUMENT_13(x) EXPAND_ARGUMENT_12(x), LOG_MACRO_JOIN(x, 13)
#define EXPAND_ARGUMENT_14(x) EXPAND_ARGUMENT_13(x), LOG_MACRO_JOIN(x, 14)
#define EXPAND_ARGUMENT_15(x) EXPAND_ARGUMENT_14(x), LOG_MACRO_JOIN(x, 15)
#define EXPAND_ARGUMENT_16(x) EXPAND_ARGUMENT_15(x), LOG_MACRO_JOIN(x, 16)
#define EXPAND_ARGUMENT_17(x) EXPAND_ARGUMENT_16(x), LOG_MACRO_JOIN(x, 17)
#define EXPAND_ARGUMENT_18(x) EXPAND_ARGUMENT_17(x), LOG_MACRO_JOIN(x, 18)
#define EXPAND_ARGUMENT_19(x) EXPAND_ARGUMENT_18(x), LOG_MACRO_JOIN(x, 19)
#define EXPAND_ARGUMENT_20(x) EXPAND_ARGUMENT_19(x), LOG_MACRO_JOIN(x, 20)
#define EXPAND_ARGUMENT_21(x) EXPAND_ARGUMENT_20(x), LOG_MACRO_JOIN(x, 21)
#define EXPAND_ARGUMENT_22(x) EXPAND_ARGUMENT_21(x), LOG_MACRO_JOIN(x, 22)

#define LOG_BODY(n) \
  if (OB_SUCC(ret)) { \
    if (1 == n) {\
      ret = ::oceanbase::common::logdata_print_kv(data, buf_len, pos, key##n, obj##n); \
    } else {\
      ret = ::oceanbase::common::logdata_print_key_obj(data, buf_len, pos, key##n, true, obj##n); \
    }\
  }

#define LOG_FUNC_BODY_0
#define LOG_FUNC_BODY_1 LOG_BODY(1)
#define LOG_FUNC_BODY_2 LOG_FUNC_BODY_1; LOG_BODY(2)
#define LOG_FUNC_BODY_3 LOG_FUNC_BODY_2; LOG_BODY(3)
#define LOG_FUNC_BODY_4 LOG_FUNC_BODY_3; LOG_BODY(4)
#define LOG_FUNC_BODY_5 LOG_FUNC_BODY_4; LOG_BODY(5)
#define LOG_FUNC_BODY_6 LOG_FUNC_BODY_5; LOG_BODY(6)
#define LOG_FUNC_BODY_7 LOG_FUNC_BODY_6; LOG_BODY(7)
#define LOG_FUNC_BODY_8 LOG_FUNC_BODY_7; LOG_BODY(8)
#define LOG_FUNC_BODY_9 LOG_FUNC_BODY_8; LOG_BODY(9)
#define LOG_FUNC_BODY_10 LOG_FUNC_BODY_9; LOG_BODY(10)
#define LOG_FUNC_BODY_11 LOG_FUNC_BODY_10; LOG_BODY(11)
#define LOG_FUNC_BODY_12 LOG_FUNC_BODY_11; LOG_BODY(12)
#define LOG_FUNC_BODY_13 LOG_FUNC_BODY_12; LOG_BODY(13)
#define LOG_FUNC_BODY_14 LOG_FUNC_BODY_13; LOG_BODY(14)
#define LOG_FUNC_BODY_15 LOG_FUNC_BODY_14; LOG_BODY(15)
#define LOG_FUNC_BODY_16 LOG_FUNC_BODY_15; LOG_BODY(16)
#define LOG_FUNC_BODY_17 LOG_FUNC_BODY_16; LOG_BODY(17)
#define LOG_FUNC_BODY_18 LOG_FUNC_BODY_17; LOG_BODY(18)
#define LOG_FUNC_BODY_19 LOG_FUNC_BODY_18; LOG_BODY(19)
#define LOG_FUNC_BODY_20 LOG_FUNC_BODY_19; LOG_BODY(20)
#define LOG_FUNC_BODY_21 LOG_FUNC_BODY_20; LOG_BODY(21)
#define LOG_FUNC_BODY_22 LOG_FUNC_BODY_21; LOG_BODY(22)

#define LOG_KV(key, obj) \
  (::oceanbase::common::ObILogKV&&)::oceanbase::common::ObLogKV<decltype(obj), \
  std::is_rvalue_reference<decltype((obj))&&>::value>(key, obj)
#define LOG_KVS_0()
#define LOG_KVS_1()
#define LOG_KVS_2(key, obj)           , LOG_KV(key, obj)
#define LOG_KVS_4(key, obj, args...)  , LOG_KV(key, obj) LOG_KVS_2(args)
#define LOG_KVS_6(key, obj, args...)  , LOG_KV(key, obj) LOG_KVS_4(args)
#define LOG_KVS_8(key, obj, args...)  , LOG_KV(key, obj) LOG_KVS_6(args)
#define LOG_KVS_10(key, obj, args...) , LOG_KV(key, obj) LOG_KVS_8(args)
#define LOG_KVS_12(key, obj, args...) , LOG_KV(key, obj) LOG_KVS_10(args)
#define LOG_KVS_14(key, obj, args...) , LOG_KV(key, obj) LOG_KVS_12(args)
#define LOG_KVS_16(key, obj, args...) , LOG_KV(key, obj) LOG_KVS_14(args)
#define LOG_KVS_18(key, obj, args...) , LOG_KV(key, obj) LOG_KVS_16(args)
#define LOG_KVS_20(key, obj, args...) , LOG_KV(key, obj) LOG_KVS_18(args)
#define LOG_KVS_22(key, obj, args...) , LOG_KV(key, obj) LOG_KVS_20(args)
#define LOG_KVS_24(key, obj, args...) , LOG_KV(key, obj) LOG_KVS_22(args)
#define LOG_KVS_26(key, obj, args...) , LOG_KV(key, obj) LOG_KVS_24(args)
#define LOG_KVS_28(key, obj, args...) , LOG_KV(key, obj) LOG_KVS_26(args)
#define LOG_KVS_30(key, obj, args...) , LOG_KV(key, obj) LOG_KVS_28(args)
#define LOG_KVS_32(key, obj, args...) , LOG_KV(key, obj) LOG_KVS_30(args)
#define LOG_KVS_34(key, obj, args...) , LOG_KV(key, obj) LOG_KVS_32(args)
#define LOG_KVS_36(key, obj, args...) , LOG_KV(key, obj) LOG_KVS_34(args)
#define LOG_KVS_38(key, obj, args...) , LOG_KV(key, obj) LOG_KVS_36(args)
#define LOG_KVS_40(key, obj, args...) , LOG_KV(key, obj) LOG_KVS_38(args)
#define LOG_KVS_42(key, obj, args...) , LOG_KV(key, obj) LOG_KVS_40(args)
#define LOG_KVS_44(key, obj, args...) , LOG_KV(key, obj) LOG_KVS_42(args)

#define LOG_KVS_(N, ...) CONCAT(LOG_KVS_, N)(__VA_ARGS__)
#define LOG_KVS(...) "placeholder" LOG_KVS_(ARGS_NUM(__VA_ARGS__), __VA_ARGS__)

#define LOG_VALUE(obj) \
  (::oceanbase::common::ObILogValue&&)::oceanbase::common::ObLogValue<decltype(obj), \
  std::is_rvalue_reference<decltype((obj))&&>::value>(obj)
#define LOG_VALUES_0()
#define LOG_VALUES_1(obj)           , LOG_VALUE(obj)
#define LOG_VALUES_2(obj, args...)  , LOG_VALUE(obj) LOG_VALUES_1(args)
#define LOG_VALUES_3(obj, args...)  , LOG_VALUE(obj) LOG_VALUES_2(args)
#define LOG_VALUES_4(obj, args...)  , LOG_VALUE(obj) LOG_VALUES_3(args)
#define LOG_VALUES_5(obj, args...)  , LOG_VALUE(obj) LOG_VALUES_4(args)
#define LOG_VALUES_6(obj, args...)  , LOG_VALUE(obj) LOG_VALUES_5(args)
#define LOG_VALUES_7(obj, args...)  , LOG_VALUE(obj) LOG_VALUES_6(args)
#define LOG_VALUES_8(obj, args...)  , LOG_VALUE(obj) LOG_VALUES_7(args)
#define LOG_VALUES_9(obj, args...)  , LOG_VALUE(obj) LOG_VALUES_8(args)
#define LOG_VALUES_10(obj, args...) , LOG_VALUE(obj) LOG_VALUES_9(args)
#define LOG_VALUES_11(obj, args...) , LOG_VALUE(obj) LOG_VALUES_10(args)
#define LOG_VALUES_12(obj, args...) , LOG_VALUE(obj) LOG_VALUES_11(args)
#define LOG_VALUES_13(obj, args...) , LOG_VALUE(obj) LOG_VALUES_12(args)
#define LOG_VALUES_14(obj, args...) , LOG_VALUE(obj) LOG_VALUES_13(args)
#define LOG_VALUES_15(obj, args...) , LOG_VALUE(obj) LOG_VALUES_14(args)
#define LOG_VALUES_16(obj, args...) , LOG_VALUE(obj) LOG_VALUES_15(args)
#define LOG_VALUES_17(obj, args...) , LOG_VALUE(obj) LOG_VALUES_16(args)
#define LOG_VALUES_18(obj, args...) , LOG_VALUE(obj) LOG_VALUES_17(args)
#define LOG_VALUES_19(obj, args...) , LOG_VALUE(obj) LOG_VALUES_18(args)
#define LOG_VALUES_20(obj, args...) , LOG_VALUE(obj) LOG_VALUES_19(args)
#define LOG_VALUES_21(obj, args...) , LOG_VALUE(obj) LOG_VALUES_20(args)
#define LOG_VALUES_22(obj, args...) , LOG_VALUE(obj) LOG_VALUES_21(args)
#define LOG_VALUES_23(obj, args...) , LOG_VALUE(obj) LOG_VALUES_22(args)
#define LOG_VALUES_24(obj, args...) , LOG_VALUE(obj) LOG_VALUES_23(args)
#define LOG_VALUES_25(obj, args...) , LOG_VALUE(obj) LOG_VALUES_24(args)
#define LOG_VALUES_26(obj, args...) , LOG_VALUE(obj) LOG_VALUES_25(args)
#define LOG_VALUES_27(obj, args...) , LOG_VALUE(obj) LOG_VALUES_26(args)
#define LOG_VALUES_28(obj, args...) , LOG_VALUE(obj) LOG_VALUES_27(args)
#define LOG_VALUES_29(obj, args...) , LOG_VALUE(obj) LOG_VALUES_28(args)
#define LOG_VALUES_30(obj, args...) , LOG_VALUE(obj) LOG_VALUES_29(args)

#define LOG_VALUES_(N, ...) CONCAT(LOG_VALUES_, N)(__VA_ARGS__)
#define LOG_VALUES(...) "placeholder" LOG_VALUES_(ARGS_NUM(__VA_ARGS__), __VA_ARGS__)

#endif
