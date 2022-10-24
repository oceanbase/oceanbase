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

#include <gtest/gtest.h>
#include "lib/allocator/ob_malloc.h"
#include "lib/utility/ob_fast_convert.h"
#include "lib/number/ob_number_v2.h"

using namespace oceanbase::common;

char *ltoa10_test(int64_t val,char *dst, const bool is_signed)
{
  char buffer[65];
  uint64_t uval = (uint64_t) val;

  if (is_signed)
  {
    if (val < 0)
    {
      *dst++ = '-';
      /* Avoid integer overflow in (-val) for LONGLONG_MIN*/
      uval = (uint64_t)0 - uval;
    }
  }

  char *p = &buffer[sizeof(buffer)-1];
  *p = '\0';
  int64_t new_val= (int64_t) (uval / 10);
  *--p = (char)('0'+ (uval - (uint64_t) new_val * 10));
  val = new_val;

  while (val != 0)
  {
    new_val=val/10;
    *--p = (char)('0' + (val-new_val*10));
    val= new_val;
  }
  while ((*dst++ = *p++) != 0) ;
  return dst-1;
}

void u64toa_naive(uint64_t value, char* buffer) {
    char temp[20];
    char *p = temp;
    do {
        *p++ = char(value % 10) + '0';
        value /= 10;
    } while (value > 0);

    do {
        *buffer++ = *--p;
    } while (p != temp);

    *buffer = '\0';
}

void i64toa_naive(int64_t value, char* buffer) {
    uint64_t u = static_cast<uint64_t>(value);
    if (value < 0) {
        *buffer++ = '-';
        u = ~u + 1;
    }
    u64toa_naive(u, buffer);
}


const char gDigitsLut[200] = {
    '0','0','0','1','0','2','0','3','0','4','0','5','0','6','0','7','0','8','0','9',
    '1','0','1','1','1','2','1','3','1','4','1','5','1','6','1','7','1','8','1','9',
    '2','0','2','1','2','2','2','3','2','4','2','5','2','6','2','7','2','8','2','9',
    '3','0','3','1','3','2','3','3','3','4','3','5','3','6','3','7','3','8','3','9',
    '4','0','4','1','4','2','4','3','4','4','4','5','4','6','4','7','4','8','4','9',
    '5','0','5','1','5','2','5','3','5','4','5','5','5','6','5','7','5','8','5','9',
    '6','0','6','1','6','2','6','3','6','4','6','5','6','6','6','7','6','8','6','9',
    '7','0','7','1','7','2','7','3','7','4','7','5','7','6','7','7','7','8','7','9',
    '8','0','8','1','8','2','8','3','8','4','8','5','8','6','8','7','8','8','8','9',
    '9','0','9','1','9','2','9','3','9','4','9','5','9','6','9','7','9','8','9','9'
};

void u64toa_lut(uint64_t value, char* buffer) {
    char temp[20];
    char* p = temp;

    while (value >= 100) {
        const unsigned i = static_cast<unsigned>(value % 100) << 1;
        value /= 100;
        *p++ = gDigitsLut[i + 1];
        *p++ = gDigitsLut[i];
    }

    if (value < 10)
        *p++ = char(value) + '0';
    else {
        const unsigned i = static_cast<unsigned>(value) << 1;
        *p++ = gDigitsLut[i + 1];
        *p++ = gDigitsLut[i];
    }

    do {
        *buffer++ = *--p;
    } while (p != temp);

    *buffer = '\0';
}

void i64toa_lut(int64_t value, char* buffer) {
    uint64_t u = static_cast<uint64_t>(value);
    if (value < 0) {
        *buffer++ = '-';
        u = ~u + 1;
    }
    u64toa_lut(u, buffer);
}


inline uint32_t CountDecimalDigit64(uint64_t n) {
#if defined(_MSC_VER) || defined(__GNUC__)
    static const uint64_t powers_of_10[] = {
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
        10000000000000,
        100000000000000,
        1000000000000000,
        10000000000000000,
        100000000000000000,
        1000000000000000000,
        10000000000000000000U
    };

#if __GNUC__
    uint32_t t = (64 - __builtin_clzll(n | 1)) * 1233 >> 12;
#elif _M_IX86
    unsigned long i = 0;
    uint64_t m = n | 1;
    if (_BitScanReverse(&i, m >> 32))
        i += 32;
    else
        _BitScanReverse(&i, m & 0xFFFFFFFF);
    uint32_t t = (i + 1) * 1233 >> 12;
#elif _M_X64
    unsigned long i = 0;
    _BitScanReverse64(&i, n | 1);
    uint32_t t = (i + 1) * 1233 >> 12;
#endif
    return t - (n < powers_of_10[t]) + 1;
#else
    // Simple pure C++ implementation
    if (n < 10) return 1;
    if (n < 100) return 2;
    if (n < 1000) return 3;
    if (n < 10000) return 4;
    if (n < 100000) return 5;
    if (n < 1000000) return 6;
    if (n < 10000000) return 7;
    if (n < 100000000) return 8;
    if (n < 1000000000) return 9;
    if (n < 10000000000) return 10;
    if (n < 100000000000) return 11;
    if (n < 1000000000000) return 12;
    if (n < 10000000000000) return 13;
    if (n < 100000000000000) return 14;
    if (n < 1000000000000000) return 15;
    if (n < 10000000000000000) return 16;
    if (n < 100000000000000000) return 17;
    if (n < 1000000000000000000) return 18;
    if (n < 10000000000000000000) return 19;
    return 20;
#endif
}

void u64toa_countlut(uint64_t value, char* buffer) {
    unsigned digit = CountDecimalDigit64(value);
    buffer += digit;
    *buffer = '\0';

    while (value >= 100) {
        const unsigned i = static_cast<unsigned>(value % 100) << 1;
        value /= 100;
        *--buffer = gDigitsLut[i + 1];
        *--buffer = gDigitsLut[i];
    }

    if (value < 10) {
        *--buffer = char(value) + '0';
    }
    else {
        const unsigned i = static_cast<unsigned>(value) << 1;
        *--buffer = gDigitsLut[i + 1];
        *--buffer = gDigitsLut[i];
    }
}

void i64toa_countlut(int64_t value, char* buffer) {
    uint64_t u = static_cast<uint64_t>(value);
    if (value < 0) {
        *buffer++ = '-';
        u = ~u + 1;
    }
    u64toa_countlut(u, buffer);
}

TEST(utility, format_int_cmp)
{
  const int64_t MAX_TEST_COUNT  = 10000;
  const int64_t MAX_BUF_SIZE = 256;
  char buf_v1[MAX_BUF_SIZE];
  int64_t pos_v1 = 0;
  char buf_v2[MAX_BUF_SIZE];
  int64_t pos_v2 = 0;
  int64_t get_range_beg = 0;
  int64_t get_range_cost = 0;
  int64_t begin_value = 0;
  int64_t end_value   = 0;

  for (int64_t j = 0; j < 22; ++j) {
    begin_value = pow(10, j);
    end_value = pow(10, j + 1) - 1;
    _OB_LOG(INFO, "\n\ntest numer %ld, [%ld, %ld]", j + 1, begin_value, end_value);

    for (int64_t i = begin_value, k = 0; k < MAX_TEST_COUNT; ++i, ++k) {
      int64_t value = i * (1 == i%2 ? 1 : -1);

      pos_v1 = 0;
      databuff_printf(buf_v1, MAX_BUF_SIZE, pos_v1, "%ld", value);

      pos_v2 = (int64_t)(ltoa10_test(value, buf_v2, true) - buf_v2);
      ASSERT_EQ(pos_v1, pos_v2);
      ASSERT_EQ(0, memcmp(buf_v1, buf_v2, pos_v1));


      i64toa_naive(value, buf_v2);
      pos_v2 = (int64_t)strlen(buf_v2);
      ASSERT_EQ(pos_v1, pos_v2);
      ASSERT_EQ(0, memcmp(buf_v1, buf_v2, pos_v1));

      ObFastFormatInt ffi(value);
      MEMCPY(buf_v2, ffi.ptr(), ffi.length());
      pos_v2 = ffi.length();
      ASSERT_EQ(pos_v1, pos_v2);
      ASSERT_EQ(0, memcmp(buf_v1, buf_v2, pos_v1));

      pos_v2 = ObFastFormatInt::format_signed(value, buf_v2 + 0);
      _OB_LOG(INFO, "debug jianhua, l1=%ld, l2=%ld,  v1=%.*s, v2=%.*s", pos_v1, pos_v2, (int)pos_v1, buf_v1, (int)pos_v1, buf_v2);
      ASSERT_EQ(pos_v1, pos_v2);
      ASSERT_EQ(0, memcmp(buf_v1, buf_v2, pos_v1));

      i64toa_lut(value, buf_v2);
      pos_v2 = (int64_t)strlen(buf_v2);
      ASSERT_EQ(pos_v1, pos_v2);
      ASSERT_EQ(0, memcmp(buf_v1, buf_v2, pos_v1));

      i64toa_countlut(value, buf_v2);
      pos_v2 = (int64_t)strlen(buf_v2);
      ASSERT_EQ(pos_v1, pos_v2);
      ASSERT_EQ(0, memcmp(buf_v1, buf_v2, pos_v1));

      bool is_valid = false;
      ASSERT_EQ(value, ObFastAtoi<int64_t>::atoi(buf_v1, buf_v1 + pos_v1, is_valid));
      ASSERT_EQ(true, is_valid);

      if (i > end_value) {
        i = begin_value;
      }
    }
  }
}

TEST(utility, format_int_perf)
{
  const int64_t MAX_TEST_COUNT  = 10000;
  const int64_t MAX_BUF_SIZE = 256;
  char buf[MAX_BUF_SIZE];
  int64_t pos = 0;
  int64_t get_range_beg = 0;
  int64_t get_range_cost = 0;
  int64_t begin_value = 0;
  int64_t end_value   = 0;

  for (int64_t j = 0; j < 22; ++j) {
    begin_value = pow(10, j);
    end_value = pow(10, j + 1) - 1;
    _OB_LOG(INFO, "\n\ntest numer %ld, [%ld, %ld]", j + 1, begin_value, end_value);

    get_range_beg = ObTimeUtility::current_time();
    for (int64_t i = begin_value, k = 0; k < MAX_TEST_COUNT; ++i, ++k) {
      databuff_printf(buf, MAX_BUF_SIZE, "%ld", i);
      if (i > end_value) {
        i = begin_value;
      }
    }
    get_range_cost = ObTimeUtility::current_time() - get_range_beg;
    _OB_LOG(INFO, "databuff_printf, cost time: %f us", (double)get_range_cost / MAX_TEST_COUNT);



    get_range_beg = ObTimeUtility::current_time();
    for (int64_t i = begin_value, k = 0; k < MAX_TEST_COUNT; ++i, ++k) {
      ltoa10_test(i, buf, true);
      if (i > end_value) {
        i = begin_value;
      }
    }
    get_range_cost = ObTimeUtility::current_time() - get_range_beg;
    _OB_LOG(INFO, "ltoa10, cost time: %f us", (double)get_range_cost / MAX_TEST_COUNT);


    get_range_beg = ObTimeUtility::current_time();
    for (int64_t i = begin_value, k = 0; k < MAX_TEST_COUNT; ++i, ++k) {
      i64toa_naive(i, buf);
      if (i > end_value) {
        i = begin_value;
      }
    }
    get_range_cost = ObTimeUtility::current_time() - get_range_beg;
    _OB_LOG(INFO, "i64toa_naive, cost time: %f us", (double)get_range_cost / MAX_TEST_COUNT);


    get_range_beg = ObTimeUtility::current_time();
    for (int64_t i = begin_value, k = 0; k < MAX_TEST_COUNT; ++i, ++k) {
      ObFastFormatInt ffi(i);
      MEMCPY(buf, ffi.ptr(), ffi.length());
      if (i > end_value) {
        i = begin_value;
      }
    }
    get_range_cost = ObTimeUtility::current_time() - get_range_beg;
    _OB_LOG(INFO, "ObFastFormatInt, cost time: %f us", (double)get_range_cost / MAX_TEST_COUNT);


    get_range_beg = ObTimeUtility::current_time();
    for (int64_t i = begin_value, k = 0; k < MAX_TEST_COUNT; ++i, ++k) {
      ObFastFormatInt ffi(i);
      if (i > end_value) {
        i = begin_value;
      }
    }
    get_range_cost = ObTimeUtility::current_time() - get_range_beg;
    _OB_LOG(INFO, "ObFastFormatInt 2, cost time: %f us", (double)get_range_cost / MAX_TEST_COUNT);


    get_range_beg = ObTimeUtility::current_time();
    for (int64_t i = begin_value, k = 0; k < MAX_TEST_COUNT; ++i, ++k) {
      pos = ObFastFormatInt::format_signed(i, buf + 0);
      if (i > end_value) {
        i = begin_value;
      }
    }
    get_range_cost = ObTimeUtility::current_time() - get_range_beg;
    _OB_LOG(INFO, "ObFastFormatInt 3, cost time: %f us", (double)get_range_cost / MAX_TEST_COUNT);


    get_range_beg = ObTimeUtility::current_time();
    for (int64_t i = begin_value, k = 0; k < MAX_TEST_COUNT; ++i, ++k) {
      i64toa_lut(i, buf);
      if (i > end_value) {
        i = begin_value;
      }
    }
    get_range_cost = ObTimeUtility::current_time() - get_range_beg;
    _OB_LOG(INFO, "lut, cost time: %f us", (double)get_range_cost / MAX_TEST_COUNT);


    get_range_beg = ObTimeUtility::current_time();
    for (int64_t i = begin_value, k = 0; k < MAX_TEST_COUNT; ++i, ++k) {
      i64toa_countlut(i, buf);
      if (i > end_value) {
        i = begin_value;
      }
    }
    get_range_cost = ObTimeUtility::current_time() - get_range_beg;
    _OB_LOG(INFO, "countlut, cost time: %f us", (double)get_range_cost / MAX_TEST_COUNT);


    get_range_beg = ObTimeUtility::current_time();
    for (int64_t i = begin_value, k = 0; k < MAX_TEST_COUNT; ++i, ++k) {
      if (i > end_value) {
        i = begin_value;
      }
    }
    get_range_cost = ObTimeUtility::current_time() - get_range_beg;
    _OB_LOG(INFO, "null, cost time: %f us", (double)get_range_cost / MAX_TEST_COUNT);


    get_range_beg = ObTimeUtility::current_time();
    bool is_valid = false;
    for (int64_t i = begin_value, k = 0; k < MAX_TEST_COUNT; ++i, ++k) {
      ObFastFormatInt ffi(i);
      (void)atoi(ffi.str());
      if (i > end_value) {
        i = begin_value;
      }
    }
    get_range_cost = ObTimeUtility::current_time() - get_range_beg;
    _OB_LOG(INFO, "atoi, cost time: %f us", (double)get_range_cost / MAX_TEST_COUNT);


    get_range_beg = ObTimeUtility::current_time();
    for (int64_t i = begin_value, k = 0; k < MAX_TEST_COUNT; ++i, ++k) {
      ObFastFormatInt ffi(i);
      ObFastAtoi<int64_t>::atoi(ffi.ptr(), ffi.ptr() + ffi.length(), is_valid);
      if (i > end_value) {
        i = begin_value;
      }
    }
    get_range_cost = ObTimeUtility::current_time() - get_range_beg;
    _OB_LOG(INFO, "ObFastAtoi::atoi, cost time: %f us", (double)get_range_cost / MAX_TEST_COUNT);


    get_range_beg = ObTimeUtility::current_time();
    for (int64_t i = begin_value, k = 0; k < MAX_TEST_COUNT; ++i, ++k) {
      ObFastFormatInt ffi(i);
      ObFastAtoi<int64_t>::atoi_unchecked(ffi.ptr(), ffi.ptr() + ffi.length());
      if (i > end_value) {
        i = begin_value;
      }
    }
    get_range_cost = ObTimeUtility::current_time() - get_range_beg;
    _OB_LOG(INFO, "ObFastAtoi::atoi_unchecked, cost time: %f us", (double)get_range_cost / MAX_TEST_COUNT);


  }
}


int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
