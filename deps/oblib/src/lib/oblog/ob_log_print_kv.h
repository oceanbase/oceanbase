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

#include "lib/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_template_utils.h"
#include "lib/utility/ob_fast_convert.h"
#include "lib/alloc/alloc_assist.h"

#define LOG_N_TRUE "true"
#define LOG_N_FLASE "false"
namespace oceanbase {
namespace common {
#define COMMA_FORMAT ", "
#define WITH_COMMA(format) (with_comma ? COMMA_FORMAT format : format)

#define LOG_MACRO_JOIN(x, y) LOG_MACRO_JOIN1(x, y)
#define LOG_MACRO_JOIN1(x, y) x##y

#define _LOG_MACRO_JOIN(x, y) _LOG_MACRO_JOIN1(x, y)
#define _LOG_MACRO_JOIN1(x, y) _##x##y

/// @return OB_SUCCESS or OB_BUF_NOT_ENOUGH
int logdata_printf(char* buf, const int64_t buf_len, int64_t& pos, const char* fmt, ...)
    __attribute__((format(printf, 4, 5)));
int logdata_vprintf(char* buf, const int64_t buf_len, int64_t& pos, const char* fmt, va_list args);

#define LOG_STDERR(...)               \
  do {                                \
    if (1 == isatty(STDERR_FILENO)) { \
      fprintf(stderr, __VA_ARGS__);   \
    }                                 \
  } while (0)
#define LOG_STDOUT(...)               \
  do {                                \
    if (1 == isatty(STDOUT_FILENO)) { \
      fprintf(stdout, __VA_ARGS__);   \
    }                                 \
  } while (0)

template <typename T>
struct ObLogPrintPointerCnt {
  explicit ObLogPrintPointerCnt(T v) : v_(v)
  {}
  int64_t to_string(char* buf, const int64_t len) const
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

// FalseType not pointer
template <typename T>
int logdata_print_name(char* buf, const int64_t buf_len, int64_t& pos, const T& obj, FalseType)
{
  int ret = OB_SUCCESS;
  ret = obj.get_name(buf, buf_len, pos);
  return ret;
}

template <typename T>
int logdata_print_name(char* buf, const int64_t buf_len, int64_t& pos, const T* obj, TrueType)
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
struct ObLogPrintName {
  explicit ObLogPrintName(const T& v) : v_(v)
  {}
  int64_t to_string(char* buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    int ret = OB_SUCCESS;
    if (OB_FAIL(logdata_printf(buf, buf_len, pos, "\""))) {
    } else if (OB_FAIL(logdata_print_name(buf, buf_len, pos, v_, IsPointer<T>()))) {
    } else if (OB_FAIL(logdata_printf(buf, buf_len, pos, "\""))) {
    } else {
    }  // do nothing
    return pos;
  }
  const T& v_;
};

// print errmsg of errno.As strerror not thread-safe, need
// to call ERRMSG, KERRMSG which use this class.
struct ObLogPrintErrMsg {
  ObLogPrintErrMsg()
  {}
  ~ObLogPrintErrMsg()
  {}
  int64_t to_string(char* buf, const int64_t len) const
  {
    int64_t pos = 0;
    if (OB_LIKELY(NULL != buf) && OB_LIKELY(len > 0)) {
      (void)logdata_printf(buf, len, pos, "\"%m\"");
    }
    return pos;
  }
};

// print errmsg of no specified. As strerror not thread-safe, need
// to call ERRNOMSG, KERRNOMSG which use this class.
struct ObLogPrintErrNoMsg {
  explicit ObLogPrintErrNoMsg(int no) : errno_(no)
  {}
  ~ObLogPrintErrNoMsg()
  {}
  int64_t to_string(char* buf, const int64_t len) const
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
template <class T>
int logdata_print_obj(char* buf, const int64_t buf_len, int64_t& pos, const T& obj, FalseType)
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
template <class T>
int logdata_print_obj(char* buf, const int64_t buf_len, int64_t& pos, const T& obj, TrueType)
{
  return logdata_printf(buf, buf_len, pos, "%ld", static_cast<int64_t>(obj));
}
// print object
template <class T>
int logdata_print_obj(char* buf, const int64_t buf_len, int64_t& pos, const T& obj)
{
  int ret = OB_SUCCESS;
  if (NULL == &obj) {
    ret = logdata_printf(buf, buf_len, pos, "NULL");
  } else {
    ret = logdata_print_obj(buf, buf_len, pos, obj, BoolType<__is_enum(T)>());
  }
  return ret;
}
template <class T>
int logdata_print_point_obj(char* buf, const int64_t buf_len, int64_t& pos, const T* obj, TrueType)
{
  return logdata_print_obj(buf, buf_len, pos, *obj);
}
template <class T>
int logdata_print_point_obj(char* buf, const int64_t buf_len, int64_t& pos, const T* obj, FalseType)
{
  return logdata_printf(buf, buf_len, pos, "%p", obj);
}
template <class T>
int logdata_print_obj(char* buf, const int64_t buf_len, int64_t& pos, T* obj)
{
  int ret = OB_SUCCESS;
  if (NULL == obj) {
    ret = logdata_printf(buf, buf_len, pos, "NULL");
  } else {
    ret = logdata_print_point_obj(buf, buf_len, pos, obj, BoolType<HAS_MEMBER(T, to_string)>());
  }
  return ret;
}
template <class T>
int logdata_print_obj(char* buf, const int64_t buf_len, int64_t& pos, const T* obj)
{
  int ret = OB_SUCCESS;
  if (NULL == obj) {
    ret = logdata_printf(buf, buf_len, pos, "NULL");
  } else {
    ret = logdata_print_point_obj(buf, buf_len, pos, obj, BoolType<HAS_MEMBER(T, to_string)>());
  }
  return ret;
}
inline int logdata_print_obj(char* buf, const int64_t buf_len, int64_t& pos, const uint64_t obj)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(NULL != buf) && OB_LIKELY(0 <= pos) && OB_LIKELY(pos < buf_len) &&
      OB_LIKELY(buf_len - pos >= ObFastFormatInt::MAX_DIGITS10_STR_SIZE)) {
    pos += ObFastFormatInt::format_unsigned(obj, buf + pos);
  } else {
    ret = OB_SIZE_OVERFLOW;
  }
  return ret;
}
inline int logdata_print_obj(char* buf, const int64_t buf_len, int64_t& pos, const int64_t obj)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(NULL != buf) && OB_LIKELY(0 <= pos) && OB_LIKELY(pos < buf_len) &&
      OB_LIKELY(buf_len - pos >= ObFastFormatInt::MAX_DIGITS10_STR_SIZE)) {
    pos += ObFastFormatInt::format_signed(obj, buf + pos);
  } else {
    ret = OB_SIZE_OVERFLOW;
  }
  return ret;
}
inline int logdata_print_obj(char* buf, const int64_t buf_len, int64_t& pos, const uint32_t obj)
{
  return logdata_print_obj(buf, buf_len, pos, static_cast<uint64_t>(obj));
}
inline int logdata_print_obj(char* buf, const int64_t buf_len, int64_t& pos, const int32_t obj)
{
  return logdata_print_obj(buf, buf_len, pos, static_cast<int64_t>(obj));
}
inline int logdata_print_obj(char* buf, const int64_t buf_len, int64_t& pos, const uint16_t obj)
{
  return logdata_print_obj(buf, buf_len, pos, static_cast<uint64_t>(obj));
}
inline int logdata_print_obj(char* buf, const int64_t buf_len, int64_t& pos, const int16_t obj)
{
  return logdata_print_obj(buf, buf_len, pos, static_cast<int64_t>(obj));
}
inline int logdata_print_obj(char* buf, const int64_t buf_len, int64_t& pos, const uint8_t obj)
{
  return logdata_print_obj(buf, buf_len, pos, static_cast<uint64_t>(obj));
}
inline int logdata_print_obj(char* buf, const int64_t buf_len, int64_t& pos, const int8_t obj)
{
  return logdata_print_obj(buf, buf_len, pos, static_cast<int64_t>(obj));
}
inline int logdata_print_obj(char* buf, const int64_t buf_len, int64_t& pos, const char obj)
{
  return logdata_printf(buf, buf_len, pos, "%c", obj);
}
inline int logdata_print_obj(char* buf, const int64_t buf_len, int64_t& pos, const float obj)
{
  return logdata_printf(buf, buf_len, pos, "%.9e", obj);
  //  return logdata_printf(buf, buf_len, pos, "%f", obj);
}
inline int logdata_print_obj(char* buf, const int64_t buf_len, int64_t& pos, const double obj)
{
  return logdata_printf(buf, buf_len, pos, "%.18le", obj);
  //  return logdata_printf(buf, buf_len, pos, "%.12f", obj);
}
inline int logdata_print_obj(char* buf, const int64_t buf_len, int64_t& pos, const bool obj)
{
  return logdata_printf(buf, buf_len, pos, "%s", obj ? LOG_N_TRUE : LOG_N_FLASE);
}
inline int logdata_print_obj(char* buf, const int64_t buf_len, int64_t& pos, char* obj)
{
  int ret = OB_SUCCESS;
  if (NULL == obj) {
    ret = logdata_printf(buf, buf_len, pos, "NULL");
  } else {
    ret = logdata_printf(buf, buf_len, pos, "\"%s\"", obj);
  }
  return ret;
}
inline int logdata_print_obj(char* buf, const int64_t buf_len, int64_t& pos, const char* obj)
{
  int ret = OB_SUCCESS;
  if (NULL == obj) {
    ret = logdata_printf(buf, buf_len, pos, "NULL");
  } else {
    ret = logdata_printf(buf, buf_len, pos, "\"%s\"", obj);
  }
  return ret;
}
inline int logdata_print_info(char* buf, const int64_t buf_len, int64_t& pos, const char* info)
{
  int ret = OB_SUCCESS;
  if (NULL == info) {
    ret = logdata_printf(buf, buf_len, pos, "NULL");
  } else {
    ret = logdata_printf(buf, buf_len, pos, "%s", info);
  }
  return ret;
}

inline int logdata_print_info_begin(char* buf, const int64_t buf_len, int64_t& pos, const char* info)
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
template <class T>
int logdata_print_key_obj(
    char* buf, const int64_t buf_len, int64_t& pos, const char* key, const bool with_comma, const T& obj, FalseType)
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
template <class T>
int logdata_print_key_obj(
    char* buf, const int64_t buf_len, int64_t& pos, const char* key, const bool with_comma, const T& obj, TrueType)
{
  return logdata_printf(buf, buf_len, pos, WITH_COMMA("%s=%ld"), key, static_cast<int64_t>(obj));
}
// print object
template <class T>
int logdata_print_key_obj(
    char* buf, const int64_t buf_len, int64_t& pos, const char* key, const bool with_comma, const T& obj)
{
  int ret = OB_SUCCESS;
  if (reinterpret_cast<const void*>(&obj) == nullptr) {
    ret = logdata_printf(buf, buf_len, pos, WITH_COMMA("%s=NULL"), key);
  } else {
    ret = logdata_print_key_obj(buf, buf_len, pos, key, with_comma, obj, BoolType<__is_enum(T)>());
  }
  return ret;
}
template <class T>
int logdata_print_key_obj(
    char* buf, const int64_t buf_len, int64_t& pos, const char* key, const bool with_comma, T* obj)
{
  int ret = OB_SUCCESS;
  if (NULL == obj) {
    ret = logdata_printf(buf, buf_len, pos, WITH_COMMA("%s=NULL"), key);
  } else {
    ret = logdata_printf(buf, buf_len, pos, WITH_COMMA("%s=%p"), key, obj);
  }
  return ret;
}
template <class T>
int logdata_print_key_obj(
    char* buf, const int64_t buf_len, int64_t& pos, const char* key, const bool with_comma, const T* obj)
{
  int ret = OB_SUCCESS;
  if (NULL == obj) {
    ret = logdata_printf(buf, buf_len, pos, WITH_COMMA("%s=NULL"), key);
  } else {
    ret = logdata_printf(buf, buf_len, pos, WITH_COMMA("%s=%p"), key, obj);
  }
  return ret;
}
inline int logdata_print_key_obj(
    char* buf, const int64_t buf_len, int64_t& pos, const char* key, const bool with_comma, const uint64_t obj)
{
  return logdata_printf(buf, buf_len, pos, WITH_COMMA("%s=%lu"), key, obj);
}
inline int logdata_print_key_obj(
    char* buf, const int64_t buf_len, int64_t& pos, const char* key, const bool with_comma, const int64_t obj)
{
  return logdata_printf(buf, buf_len, pos, WITH_COMMA("%s=%ld"), key, obj);
}
inline int logdata_print_key_obj(
    char* buf, const int64_t buf_len, int64_t& pos, const char* key, const bool with_comma, const uint32_t obj)
{
  return logdata_printf(buf, buf_len, pos, WITH_COMMA("%s=%u"), key, obj);
}
inline int logdata_print_key_obj(
    char* buf, const int64_t buf_len, int64_t& pos, const char* key, const bool with_comma, const int32_t obj)
{
  return logdata_printf(buf, buf_len, pos, WITH_COMMA("%s=%d"), key, obj);
}
inline int logdata_print_key_obj(
    char* buf, const int64_t buf_len, int64_t& pos, const char* key, const bool with_comma, const uint16_t obj)
{
  return logdata_printf(buf, buf_len, pos, WITH_COMMA("%s=%u"), key, obj);
}
inline int logdata_print_key_obj(
    char* buf, const int64_t buf_len, int64_t& pos, const char* key, const bool with_comma, const int16_t obj)
{
  return logdata_printf(buf, buf_len, pos, WITH_COMMA("%s=%d"), key, obj);
}
inline int logdata_print_key_obj(
    char* buf, const int64_t buf_len, int64_t& pos, const char* key, const bool with_comma, const uint8_t obj)
{
  return logdata_printf(buf, buf_len, pos, WITH_COMMA("%s=%u"), key, obj);
}
inline int logdata_print_key_obj(
    char* buf, const int64_t buf_len, int64_t& pos, const char* key, const bool with_comma, const int8_t obj)
{
  return logdata_printf(buf, buf_len, pos, WITH_COMMA("%s=%d"), key, obj);
}
inline int logdata_print_key_obj(
    char* buf, const int64_t buf_len, int64_t& pos, const char* key, const bool with_comma, const char obj)
{
  return logdata_printf(buf, buf_len, pos, WITH_COMMA("%s=%c"), key, obj);
}
inline int logdata_print_key_obj(
    char* buf, const int64_t buf_len, int64_t& pos, const char* key, const bool with_comma, const float obj)
{
  return logdata_printf(buf, buf_len, pos, WITH_COMMA("%s=%.9e"), key, obj);
}
inline int logdata_print_key_obj(
    char* buf, const int64_t buf_len, int64_t& pos, const char* key, const bool with_comma, const double obj)
{
  return logdata_printf(buf, buf_len, pos, WITH_COMMA("%s=%.18le"), key, obj);
}
inline int logdata_print_key_obj(
    char* buf, const int64_t buf_len, int64_t& pos, const char* key, const bool with_comma, const bool obj)
{
  return logdata_printf(buf, buf_len, pos, WITH_COMMA("%s=%s"), key, obj ? LOG_N_TRUE : LOG_N_FLASE);
}
inline int logdata_print_key_obj(
    char* buf, const int64_t buf_len, int64_t& pos, const char* key, const bool with_comma, char* obj)
{
  int ret = OB_SUCCESS;
  if (NULL == obj) {
    ret = logdata_printf(buf, buf_len, pos, WITH_COMMA("%s=NULL"), key);
  } else {
    ret = logdata_printf(buf, buf_len, pos, WITH_COMMA("%s=\"%s\""), key, obj);
  }
  return ret;
}
inline int logdata_print_key_obj(
    char* buf, const int64_t buf_len, int64_t& pos, const char* key, const bool with_comma, const char* obj)
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
int logdata_print_kv(char* buf, const int64_t buf_len, int64_t& pos, const char* key, const T& obj)
{
  return logdata_print_key_obj(buf, buf_len, pos, key, false, obj);
}

}  // namespace common
}  // namespace oceanbase

#define LOG_PRINT_INFO(obj)                                                      \
  if (OB_SUCC(ret)) {                                                            \
    ret = ::oceanbase::common::logdata_print_info(data, MAX_LOG_SIZE, pos, obj); \
  }

#define LOG_PRINT_INFO_BEGIN(obj)                                                      \
  if (OB_SUCC(ret)) {                                                                  \
    ret = ::oceanbase::common::logdata_print_info_begin(data, MAX_LOG_SIZE, pos, obj); \
  }

#define LOG_PRINT_KV(key, value)                                                      \
  if (OB_SUCC(ret)) {                                                                 \
    ret = ::oceanbase::common::logdata_print_kv(data, MAX_LOG_SIZE, pos, key, value); \
  }

#define LOG_DATA_PRINTF(args...)                                                \
  if (OB_SUCC(ret)) {                                                           \
    ret = ::oceanbase::common::logdata_printf(data, MAX_LOG_SIZE, pos, ##args); \
  }

#define LOG_COMMA() LOG_DATA_PRINTF(", ")
#define LOG_KV_BEGIN() LOG_DATA_PRINTF("(")
#define LOG_KV_END() LOG_DATA_PRINTF(")")

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

#define LOG_FUNC_ARG_KV(n) const char *key##n, const T##n &obj##n
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

#define FILL_LOG_BUFFER_BODY(n)                                                \
  if (OB_SUCC(ret)) {                                                          \
    const bool with_comma = (1 != n);                                          \
    if (NULL == obj##n) {                                                      \
      ret = logdata_printf(data, MAX_LOG_SIZE, pos, "buffer is NULL");         \
    } else {                                                                   \
      ret = logdata_printf(data, MAX_LOG_SIZE, pos, WITH_COMMA("%s"), obj##n); \
    }                                                                          \
  }

#define FILL_LOG_BUFFER_BODY_0
#define FILL_LOG_BUFFER_BODY_1 FILL_LOG_BUFFER_BODY(1)
#define FILL_LOG_BUFFER_BODY_2 \
  FILL_LOG_BUFFER_BODY_1;      \
  FILL_LOG_BUFFER_BODY(2)
#define FILL_LOG_BUFFER_BODY_3 \
  FILL_LOG_BUFFER_BODY_2;      \
  FILL_LOG_BUFFER_BODY(3)
#define FILL_LOG_BUFFER_BODY_4 \
  FILL_LOG_BUFFER_BODY_3;      \
  FILL_LOG_BUFFER_BODY(4)
#define FILL_LOG_BUFFER_BODY_5 \
  FILL_LOG_BUFFER_BODY_4;      \
  FILL_LOG_BUFFER_BODY(5)
#define FILL_LOG_BUFFER_BODY_6 \
  FILL_LOG_BUFFER_BODY_5;      \
  FILL_LOG_BUFFER_BODY(6)
#define FILL_LOG_BUFFER_BODY_7 \
  FILL_LOG_BUFFER_BODY_6;      \
  FILL_LOG_BUFFER_BODY(7)
#define FILL_LOG_BUFFER_BODY_8 \
  FILL_LOG_BUFFER_BODY_7;      \
  FILL_LOG_BUFFER_BODY(8)
#define FILL_LOG_BUFFER_BODY_9 \
  FILL_LOG_BUFFER_BODY_8;      \
  FILL_LOG_BUFFER_BODY(9)
#define FILL_LOG_BUFFER_BODY_10 \
  FILL_LOG_BUFFER_BODY_9;       \
  FILL_LOG_BUFFER_BODY(10)
#define FILL_LOG_BUFFER_BODY_11 \
  FILL_LOG_BUFFER_BODY_10;      \
  FILL_LOG_BUFFER_BODY(11)
#define FILL_LOG_BUFFER_BODY_12 \
  FILL_LOG_BUFFER_BODY_11;      \
  FILL_LOG_BUFFER_BODY(12)
#define FILL_LOG_BUFFER_BODY_13 \
  FILL_LOG_BUFFER_BODY_12;      \
  FILL_LOG_BUFFER_BODY(13)
#define FILL_LOG_BUFFER_BODY_14 \
  FILL_LOG_BUFFER_BODY_13;      \
  FILL_LOG_BUFFER_BODY(14)
#define FILL_LOG_BUFFER_BODY_15 \
  FILL_LOG_BUFFER_BODY_14;      \
  FILL_LOG_BUFFER_BODY(15)
#define FILL_LOG_BUFFER_BODY_16 \
  FILL_LOG_BUFFER_BODY_15;      \
  FILL_LOG_BUFFER_BODY(16)
#define FILL_LOG_BUFFER_BODY_17 \
  FILL_LOG_BUFFER_BODY_16;      \
  FILL_LOG_BUFFER_BODY(17)
#define FILL_LOG_BUFFER_BODY_18 \
  FILL_LOG_BUFFER_BODY_17;      \
  FILL_LOG_BUFFER_BODY(18)
#define FILL_LOG_BUFFER_BODY_19 \
  FILL_LOG_BUFFER_BODY_18;      \
  FILL_LOG_BUFFER_BODY(19)
#define FILL_LOG_BUFFER_BODY_20 \
  FILL_LOG_BUFFER_BODY_19;      \
  FILL_LOG_BUFFER_BODY(20)
#define FILL_LOG_BUFFER_BODY_21 \
  FILL_LOG_BUFFER_BODY_20;      \
  FILL_LOG_BUFFER_BODY(21)
#define FILL_LOG_BUFFER_BODY_22 \
  FILL_LOG_BUFFER_BODY_21;      \
  FILL_LOG_BUFFER_BODY(22)

#define LOG_BODY(n)                                                                                    \
  if (OB_SUCC(ret)) {                                                                                  \
    if (1 == n) {                                                                                      \
      ret = ::oceanbase::common::logdata_print_kv(data, MAX_LOG_SIZE, pos, key##n, obj##n);            \
    } else {                                                                                           \
      ret = ::oceanbase::common::logdata_print_key_obj(data, MAX_LOG_SIZE, pos, key##n, true, obj##n); \
    }                                                                                                  \
  }

#define LOG_FUNC_BODY_0
#define LOG_FUNC_BODY_1 LOG_BODY(1)
#define LOG_FUNC_BODY_2 \
  LOG_FUNC_BODY_1;      \
  LOG_BODY(2)
#define LOG_FUNC_BODY_3 \
  LOG_FUNC_BODY_2;      \
  LOG_BODY(3)
#define LOG_FUNC_BODY_4 \
  LOG_FUNC_BODY_3;      \
  LOG_BODY(4)
#define LOG_FUNC_BODY_5 \
  LOG_FUNC_BODY_4;      \
  LOG_BODY(5)
#define LOG_FUNC_BODY_6 \
  LOG_FUNC_BODY_5;      \
  LOG_BODY(6)
#define LOG_FUNC_BODY_7 \
  LOG_FUNC_BODY_6;      \
  LOG_BODY(7)
#define LOG_FUNC_BODY_8 \
  LOG_FUNC_BODY_7;      \
  LOG_BODY(8)
#define LOG_FUNC_BODY_9 \
  LOG_FUNC_BODY_8;      \
  LOG_BODY(9)
#define LOG_FUNC_BODY_10 \
  LOG_FUNC_BODY_9;       \
  LOG_BODY(10)
#define LOG_FUNC_BODY_11 \
  LOG_FUNC_BODY_10;      \
  LOG_BODY(11)
#define LOG_FUNC_BODY_12 \
  LOG_FUNC_BODY_11;      \
  LOG_BODY(12)
#define LOG_FUNC_BODY_13 \
  LOG_FUNC_BODY_12;      \
  LOG_BODY(13)
#define LOG_FUNC_BODY_14 \
  LOG_FUNC_BODY_13;      \
  LOG_BODY(14)
#define LOG_FUNC_BODY_15 \
  LOG_FUNC_BODY_14;      \
  LOG_BODY(15)
#define LOG_FUNC_BODY_16 \
  LOG_FUNC_BODY_15;      \
  LOG_BODY(16)
#define LOG_FUNC_BODY_17 \
  LOG_FUNC_BODY_16;      \
  LOG_BODY(17)
#define LOG_FUNC_BODY_18 \
  LOG_FUNC_BODY_17;      \
  LOG_BODY(18)
#define LOG_FUNC_BODY_19 \
  LOG_FUNC_BODY_18;      \
  LOG_BODY(19)
#define LOG_FUNC_BODY_20 \
  LOG_FUNC_BODY_19;      \
  LOG_BODY(20)
#define LOG_FUNC_BODY_21 \
  LOG_FUNC_BODY_20;      \
  LOG_BODY(21)
#define LOG_FUNC_BODY_22 \
  LOG_FUNC_BODY_21;      \
  LOG_BODY(22)

#define CHECK_LOG_END_AND_ERROR_LOG(log_item)              \
  check_log_end(*log_item, pos);                           \
  if (OB_UNLIKELY(OB_SIZE_OVERFLOW == ret)) {              \
    log_item->set_size_overflow();                         \
    ret = OB_SUCCESS;                                      \
  }                                                        \
  if (OB_SUCC(ret)) {                                      \
    if (OB_FAIL(check_error_log(*log_item))) {             \
      LOG_STDERR("check_error_log error ret = %d\n", ret); \
    }                                                      \
  }

// for ObLogger
#define DEFINE_LOG_PRINT_KV(n)                                                                                   \
  __attribute__((noinline, cold)) int fill_log_buffer(char* data,                                                \
      const int64_t MAX_LOG_SIZE,                                                                                \
      int64_t& pos,                                                                                              \
      const char* info_string,                                                                                   \
      EXPAND_ARGUMENT_##n(const char* obj))                                                                      \
  {                                                                                                              \
    int ret = OB_SUCCESS;                                                                                        \
    LOG_PRINT_INFO_BEGIN(info_string);                                                                           \
    FILL_LOG_BUFFER_BODY_##n;                                                                                    \
    LOG_KV_END();                                                                                                \
    return ret;                                                                                                  \
  }                                                                                                              \
  __attribute__((noinline, cold)) void log_message_kv(const char* mod_name,                                      \
      const int32_t level,                                                                                       \
      const char* file,                                                                                          \
      const int32_t line,                                                                                        \
      const char* function,                                                                                      \
      const uint64_t location_hash_val,                                                                          \
      const char* info_string,                                                                                   \
      EXPAND_ARGUMENT_##n(const char* obj))                                                                      \
  {                                                                                                              \
    int ret = OB_SUCCESS;                                                                                        \
    LogBuffer* log_buffer = NULL;                                                                                \
    if (OB_LIKELY(level <= OB_LOG_LEVEL_DEBUG) && OB_LIKELY(level >= OB_LOG_LEVEL_ERROR) &&                      \
        OB_LIKELY(is_enable_logging()) && OB_NOT_NULL(mod_name) && OB_NOT_NULL(file) && OB_NOT_NULL(function) && \
        OB_NOT_NULL(function) && OB_NOT_NULL(info_string)) {                                                     \
      if (is_trace_mode()) {                                                                                     \
        if (OB_NOT_NULL(log_buffer = get_thread_buffer()) && OB_LIKELY(!log_buffer->is_oversize())) {            \
          set_disable_logging(true);                                                                             \
          log_head_info(mod_name, level, LogLocation(file, line, function), *log_buffer);                        \
          int64_t& pos = log_buffer->pos_;                                                                       \
          char* data = log_buffer->buffer_;                                                                      \
          ret = fill_log_buffer(data, MAX_LOG_SIZE, pos, info_string, EXPAND_ARGUMENT_##n(obj));                 \
          log_tail(level, *log_buffer);                                                                          \
        }                                                                                                        \
      } else if (!is_async_log_used()) {                                                                         \
        if (OB_NOT_NULL(log_buffer = get_thread_buffer()) && OB_LIKELY(!log_buffer->is_oversize())) {            \
          set_disable_logging(true);                                                                             \
          int64_t& pos = log_buffer->pos_;                                                                       \
          char* data = log_buffer->buffer_;                                                                      \
          ret = fill_log_buffer(data, MAX_LOG_SIZE, pos, info_string, EXPAND_ARGUMENT_##n(obj));                 \
          log_data(mod_name, level, LogLocation(file, line, function), *log_buffer);                             \
        }                                                                                                        \
      } else {                                                                                                   \
        set_disable_logging(true);                                                                               \
        auto log_data_func = [&](ObPLogItem* log_item) {                                                         \
          int ret = OB_SUCCESS;                                                                                  \
          int64_t MAX_LOG_SIZE = log_item->get_buf_size();                                                       \
          int64_t pos = log_item->get_data_len();                                                                \
          char* data = log_item->get_buf();                                                                      \
          fill_log_buffer(data, MAX_LOG_SIZE, pos, info_string, EXPAND_ARGUMENT_##n(obj));                       \
          CHECK_LOG_END_AND_ERROR_LOG(log_item);                                                                 \
          return ret;                                                                                            \
        };                                                                                                       \
        do_async_log_message(mod_name, level, file, line, function, location_hash_val, log_data_func);           \
      }                                                                                                          \
      set_disable_logging(false);                                                                                \
    }                                                                                                            \
    UNUSED(ret);                                                                                                 \
  }

// for TraceLog
#define DEFINE_FILL_LOG_KV(n)                                                                                        \
  template <LOG_TYPENAME_TN##n>                                                                                      \
  static void fill_log_kv(LogBuffer& log_buffer, const char* function, const char* info_string, LOG_PARAMETER_KV##n) \
  {                                                                                                                  \
    int ret = OB_SUCCESS;                                                                                            \
    log_buffer.check_and_lock();                                                                                     \
    int64_t MAX_LOG_SIZE = LogBuffer::LOG_BUFFER_SIZE - log_buffer.cur_pos_;                                         \
    if (MAX_LOG_SIZE > 0) {                                                                                          \
      char* data = log_buffer.buffer_ + log_buffer.cur_pos_;                                                         \
      int64_t pos = 0;                                                                                               \
      LOG_DATA_PRINTF("[%s] ", function);                                                                            \
      ret = fill_log_buffer(data, MAX_LOG_SIZE, pos, info_string, EXPAND_ARGUMENT_##n(obj));                         \
      if (pos >= 0 && pos < MAX_LOG_SIZE) {                                                                          \
        log_buffer.cur_pos_ += pos;                                                                                  \
        add_stamp(log_buffer, MAX_LOG_SIZE, pos);                                                                    \
      } else {                                                                                                       \
        if (log_buffer.cur_pos_ >= 0 && log_buffer.cur_pos_ < LogBuffer::LOG_BUFFER_SIZE) {                          \
          log_buffer.buffer_[log_buffer.cur_pos_] = '\0';                                                            \
        }                                                                                                            \
        print_log(log_buffer);                                                                                       \
        MAX_LOG_SIZE = LogBuffer::LOG_BUFFER_SIZE - log_buffer.cur_pos_;                                             \
        data = log_buffer.buffer_ + log_buffer.cur_pos_;                                                             \
        pos = 0;                                                                                                     \
        LOG_DATA_PRINTF("[%s] ", function);                                                                          \
        ret = fill_log_buffer(data, MAX_LOG_SIZE, pos, info_string, EXPAND_ARGUMENT_##n(obj));                       \
        if (pos >= 0 && pos < MAX_LOG_SIZE) {                                                                        \
          log_buffer.cur_pos_ += pos;                                                                                \
          add_stamp(log_buffer, MAX_LOG_SIZE, pos);                                                                  \
        } else {                                                                                                     \
          print_log(log_buffer);                                                                                     \
        }                                                                                                            \
      }                                                                                                              \
      if (log_buffer.cur_pos_ >= 0 && log_buffer.cur_pos_ < LogBuffer::LOG_BUFFER_SIZE) {                            \
        log_buffer.buffer_[log_buffer.cur_pos_] = '\0';                                                              \
      }                                                                                                              \
    }                                                                                                                \
    log_buffer.check_and_unlock();                                                                                   \
  }

#endif
