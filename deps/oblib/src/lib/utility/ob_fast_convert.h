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

#ifndef OCEANBASE_COMMON_FAST_FORMAT_H_
#define OCEANBASE_COMMON_FAST_FORMAT_H_

#include <stdint.h>
#include <limits>
#include <type_traits>
#include "lib/utility/ob_macro_utils.h"

namespace oceanbase
{
namespace common
{
//The search starts with a short gallop favoring small numbers,
//after which it goes into a hand-woven binary search.
inline uint32_t ob_fast_digits10(uint64_t v)
{
  static const uint64_t MAX_INTEGER[13] = {
    0,
    10,
    100,
    1000,
    10000,
    100000,
    1000000,
    10000000,
    100000000,
    1000000000,
    10000000000,
    100000000000,
    1000000000000,
  };
  uint32_t ret_int = 0;
  if (v < MAX_INTEGER[1]) {
    ret_int = 1;
  } else if (v < MAX_INTEGER[2]) {
    ret_int = 2;
  } else if (v < MAX_INTEGER[3]) {
    ret_int = 3;
  } else if (v < MAX_INTEGER[12]) {
    if (v < MAX_INTEGER[8]) {
      if (v < MAX_INTEGER[6]) {
        if (v < MAX_INTEGER[4]) {
          ret_int = 4;
        } else {
          ret_int = 5 + (v >= MAX_INTEGER[5]);
        }
      } else {
        ret_int = 7 + (v >= MAX_INTEGER[7]);
      }
    } else if (v < MAX_INTEGER[10]) {
      ret_int = 9 + (v >= MAX_INTEGER[9]);
    } else {
      ret_int = 11 + (v >= MAX_INTEGER[11]);
    }
  } else {
    ret_int = 12 + ob_fast_digits10(v / MAX_INTEGER[12]);
  }
  return ret_int;
}

/*
 * The idea comes from the talk by Alexandrescu "Three Optimization Tips for C++".
 * faster then databuff_print for 20~4 times(digits from 1~20),  20~7 times(digits from 1~9),
 *
 * usage:
 *  ObFastFormatInt ffi(num);
 *  ffi.ptr()     ----for num_str ptr
 *  ffi.length()  ----for num_str length, without terminating null character.
 *  ffi.str()     ----for num_str c_str, with terminating null character.
 *  ffi.string()  ----for num_str ObString, without terminating null character.
 *
 *  ObFastFormatInt::format_signed(num, buf);
 *  ObFastFormatInt::format_unsigned(num, buf);
 */
class ObFastFormatInt
{
public:
  explicit ObFastFormatInt(const int8_t value) { format_signed(value); }
  explicit ObFastFormatInt(const int16_t value) { format_signed(value); }
  explicit ObFastFormatInt(const int32_t value) { format_signed(value); }
  explicit ObFastFormatInt(const int64_t value) { format_signed(value); }
  explicit ObFastFormatInt(const uint8_t value)
    : ptr_(format_unsigned(value)), len_(buf_ - ptr_ + MAX_DIGITS10_STR_SIZE - 1) {}
  explicit ObFastFormatInt(const uint16_t value)
    : ptr_(format_unsigned(value)), len_(buf_ - ptr_ + MAX_DIGITS10_STR_SIZE - 1) {}
  explicit ObFastFormatInt(const uint32_t value)
    : ptr_(format_unsigned(value)), len_(buf_ - ptr_ + MAX_DIGITS10_STR_SIZE - 1) {}
  explicit ObFastFormatInt(const uint64_t value)
    : ptr_(format_unsigned(value)), len_(buf_ - ptr_ + MAX_DIGITS10_STR_SIZE - 1) {}

  explicit ObFastFormatInt(const int64_t value, const bool is_unsigned)
    : ptr_(nullptr), len_(buf_ - ptr_ + MAX_DIGITS10_STR_SIZE - 1)
  {
    if (is_unsigned) {
      ptr_ = format_unsigned(static_cast<uint64_t>(value));
      len_ = buf_ - ptr_ + MAX_DIGITS10_STR_SIZE - 1;
    } else {
      format_signed(value);
    }
  }

  //Returns a pointer to the output buffer content without terminating null character is appended.
  const char *ptr() const { return ptr_; }

  //Returns then formated length not including null character.
  int64_t length() const { return len_; }

  //Returns a pointer to the output buffer content with terminating null character appended.
  const char *str() const { buf_[MAX_DIGITS10_STR_SIZE - 1] = '\0'; return ptr_; }

  //Returns the number of tail '0' characters written to the output buffer
  inline int64_t get_tail_zero_count() const
  {
    int64_t tail_zero_count = 0;
    if (len_ > 0) {
      const char *ptr = buf_ + (MAX_DIGITS10_STR_SIZE - 2);
      while (ptr >= ptr_ && '0' == *ptr) {
        --ptr;
      }
      tail_zero_count = buf_ + (MAX_DIGITS10_STR_SIZE - 2) - ptr;
    }
    return tail_zero_count;
  }

  /*
   * Formats value into input buf with terminating null character appended, like snprintf
   *
   * input:
   *      value: need formated unsigned integer
   *      buf  : buffer will store integer, size need large then MAX_DIGITS10_STR_SIZE
   * output:
   *      Returns then formated length not including null character.
   */
  static int64_t format_unsigned(uint64_t value, char *buf);

  /*
   * Formats value into input buf with terminating null character appended, like snprintf
   *
   * input:
   *      value: need formated signed integer
   *      buf  : buffer will store integer, size need large then MAX_DIGITS10_STR_SIZE
   * output:
   *      Returns then formated length not including null character.
   */
  static int64_t format_signed(int64_t value, char *buf);

  // Buffer should be large enough to hold all digits (digits10 + 1), a sign and a null character.
  static const int64_t MAX_DIGITS10_STR_SIZE = std::numeric_limits<uint64_t>::digits10 + 3;

private:
  /*
   * Formats value in reverse and returns a pointer to the beginning
   *
   * input:
   *      value: need formated unsigned integer
   * output:
   *      Returns a pointer to the output buffer content with terminating null character appended.
   */
  char *format_unsigned(uint64_t value);

  /*
   * Formats value in reverse and set the the beginning pointer to this->ptr_,
   * set str length(without terminating null character) to this->len_
   *
   * input:
   *      value: need formated signed integer
   * output:
   *      null
   */
  void format_signed(int64_t value);

private:
  static const char DIGITS[];

  mutable char buf_[MAX_DIGITS10_STR_SIZE];
  char *ptr_;
  int64_t len_;
};

//ref: https://github.com/jsteemann/atoi
template<typename T>
class ObFastAtoi
{
public:
  // low-level worker function to convert the string value between p
  // (inclusive) and e (exclusive) into a negative number value of type T,
  // without validation of the input string - use this only for trusted input!
  //
  // the input string will always be interpreted as a base-10 number.
  // expects the input string to contain only the digits '0' to '9'.
  // there is no validation of the input string, and overflow or underflow
  // of the result value will not be detected.
  // this function will not modify errno.
  static inline T atoi_negative_unchecked(char const* p, char const* e)
  {
    T result = 0;
    while (p != e) {
      result = (result << 1) + (result << 3) - (*(p++) - '0');
    }
    return result;
  }

  // low-level worker function to convert the string value between p
  // (inclusive) and e (exclusive) into a positive number value of type T,
  // without validation of the input string - use this only for trusted input!
  //
  // the input string will always be interpreted as a base-10 number.
  // expects the input string to contain only the digits '0' to '9'.
  // there is no validation of the input string, and overflow or underflow
  // of the result value will not be detected.
  // this function will not modify errno.
  static inline T atoi_positive_unchecked(char const* p, char const* e)
  {
    T result = 0;
    while (p != e) {
      result = (result << 1) + (result << 3) + *(p++) - '0';
    }

    return result;
  }

  // function to convert the string value between p
  // (inclusive) and e (exclusive) into a number value of type T, without
  // validation of the input string - use this only for trusted input!
  //
  // the input string will always be interpreted as a base-10 number.
  // expects the input string to contain only the digits '0' to '9'. an
  // optional '+' or '-' sign is allowed too.
  // there is no validation of the input string, and overflow or underflow
  // of the result value will not be detected.
  // this function will not modify errno.
  static inline T atoi_unchecked(char const* p, char const* e)
  {
    if (OB_UNLIKELY(p == e)) {
      return T();
    }

    if (*p == '-') {
      if (!std::is_signed<T>::value) {
        return T();
      }
      return atoi_negative_unchecked(++p, e);
    }
    if (OB_UNLIKELY(*p == '+')) {
      ++p;
    }

    return atoi_positive_unchecked(p, e);
  }

  // low-level worker function to convert the string value between p
  // (inclusive) and e (exclusive) into a negative number value of type T
  //
  // the input string will always be interpreted as a base-10 number.
  // expects the input string to contain only the digits '0' to '9'.
  // if any other character is found, the output parameter "valid" will
  // be set to false. if the parsed value is less than what type T can
  // store without truncation, "valid" will also be set to false.
  // this function will not modify errno.
  static inline T atoi_negative(char const* p, char const* e, bool& valid)
  {
    if (OB_UNLIKELY(p == e)) {
      valid = false;
      return T();
    }

    constexpr T cutoff = (std::numeric_limits<T>::min)() / 10;
    constexpr char cutlim = -((std::numeric_limits<T>::min)() % 10);
    T result = 0;

    do {
      char c = *p;
      // we expect only '0' to '9'. everything else is unexpected
      if (OB_UNLIKELY(c < '0' || c > '9')) {
        valid = false;
        return result;
      }

      c -= '0';
      // we expect the bulk of values to not hit the bounds restrictions
      if (OB_UNLIKELY(result < cutoff || (result == cutoff && c > cutlim))) {
        valid = false;
        return result;
      }
      result *= 10;
      result -= c;
    } while (++p < e);

    valid = true;
    return result;
  }

  // low-level worker function to convert the string value between p
  // (inclusive) and e (exclusive) into a positive number value of type T
  //
  // the input string will always be interpreted as a base-10 number.
  // expects the input string to contain only the digits '0' to '9'.
  // if any other character is found, the output parameter "valid" will
  // be set to false. if the parsed value is greater than what type T can
  // store without truncation, "valid" will also be set to false.
  // this function will not modify errno.
  static inline T atoi_positive(char const* p, char const* e, bool& valid)
  {
    if (OB_UNLIKELY(p == e)) {
      valid = false;
      return T();
    }

    constexpr T cutoff = (std::numeric_limits<T>::max)() / 10;
    constexpr char cutlim = (std::numeric_limits<T>::max)() % 10;
    T result = 0;

    do {
      char c = *p;

      // we expect only '0' to '9'. everything else is unexpected
      if (OB_UNLIKELY(c < '0' || c > '9')) {
        valid = false;
        return result;
      }

      c -= '0';
      // we expect the bulk of values to not hit the bounds restrictions
      if (OB_UNLIKELY(result > cutoff || (result == cutoff && c > cutlim))) {
        valid = false;
        return result;
      }
      result *= 10;
      result += c;
    } while (++p < e);

    valid = true;
    return result;
  }

  // function to convert the string value between p
  // (inclusive) and e (exclusive) into a number value of type T
  //
  // the input string will always be interpreted as a base-10 number.
  // expects the input string to contain only the digits '0' to '9'. an
  // optional '+' or '-' sign is allowed too.
  // if any other character is found, the output parameter "valid" will
  // be set to false. if the parsed value is less or greater than what
  // type T can store without truncation, "valid" will also be set to
  // false.
  // this function will not modify errno.
  static inline T atoi(char const* p, char const* e, bool& valid)
  {
    if (OB_UNLIKELY(p == e)) {
      valid = false;
      return T();
    }

    if (*p == '-') {
      return atoi_negative(++p, e, valid);
    }
    if (OB_UNLIKELY(*p == '+')) {
      ++p;
    }

    return atoi_positive(p, e, valid);
  }
};

} // end namespace common
} // end namespace oceanbase


#endif //OCEANBASE_COMMON_FAST_FORMAT_H_
