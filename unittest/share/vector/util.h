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

#include <vector>
#include <random>
#include <time.h>

#include "share/vector/type_traits.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/number/ob_number_v2.h"
#include "lib/wide_integer/ob_wide_integer.h"
#include "lib/timezone/ob_timezone_info.h"
#include "sql/engine/expr/ob_expr.h"

namespace oceanbase
{
namespace common
{

struct MockAllocator: public ObIAllocator
{
  void* alloc(const int64_t size) override
  {
    return malloc(size);
  }
  void* alloc(const int64_t num, const lib::ObMemAttr &attr) override {
    return malloc(num);
  }
  void free(void *ptr) override {
    std::free(ptr);
  }
};

struct Item
{
  char *data;
  int32_t data_len;
};

template<VecValueTypeClass>
Item rand_item()
{
  return  Item();
}

template<>
Item rand_item<VEC_TC_NUMBER>()
{
  std::default_random_engine rd(time(NULL));
  std::uniform_int_distribution<int32_t> digit_len(1, 30);
  std::uniform_int_distribution<int32_t> digits(0, 9);
  std::uniform_int_distribution<int32_t> neg(0, 10);
  std::string num_str;
  int32_t int_len = digit_len(rd), dec_len = digit_len(rd);
  if (neg(rd) <= 5) {
    num_str.push_back('-');
  }
  for (int i = 0; i < int_len; i++) {
    num_str.push_back(digits(rd) + '0');
  }
  num_str.push_back('.');
  for (int i = 0; i < dec_len; i++) {
    num_str.push_back(digits(rd) + '0');
  }

  MockAllocator alloc;
  number::ObNumber v;
  int ret = v.from(num_str.c_str(), num_str.size(), alloc);
  int32_t data_len = sizeof(ObNumberDesc) + v.get_length() * sizeof(uint32_t);
  char *data = (char *)alloc.alloc(data_len);
  *reinterpret_cast<ObNumberDesc *>(data) = v.get_desc();
  std::memcpy(data + sizeof(ObNumberDesc), v.get_digits(), sizeof(uint32_t) * v.get_length());
  return Item{data, data_len};
}

template<>
Item rand_item<VEC_TC_STRING>()
{
  static const std::string chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
  std::default_random_engine rd(time(NULL));
  std::uniform_int_distribution<int32_t> idx(0, chars.size() - 1);
  std::string str;
  std::uniform_int_distribution<int32_t> data_len(0, 1000);
  int32_t len = data_len(rd);
  for (int i = 0; i < len; i++) {
    str.push_back(chars[idx(rd)]);
  }
  void *data = std::malloc(len);
  std::memcpy(data, str.data(), len);
  return Item{(char *)data, len};
}

template<typename T>
struct RandomData
{
  static T rand()
  {
    return T();
  }
};

template<>
struct RandomData<int64_t>
{
  static int64_t rand()
  {
    std::default_random_engine random(time(NULL));
    std::uniform_int_distribution<int64_t> dist(INT64_MIN, INT64_MAX);
    return dist(random);
  }
};

template <typename T>
struct WideIntegerRandomData
{
  static T rand()
  {
    std::default_random_engine random(time(NULL));
    std::uniform_int_distribution<int64_t> dist(0, UINT64_MAX);
    T res = 0;
    for (int i = 0; i < T::ITEM_COUNT; i++) {
      res = (res << 64) + dist(random);
    }
    return res;
  }
};

template<>
struct RandomData<int128_t>: WideIntegerRandomData<int128_t> {};

template<>
struct RandomData<int256_t>: WideIntegerRandomData<int256_t> {};

template<>
struct RandomData<int512_t>: WideIntegerRandomData<int512_t> {};

template<>
struct RandomData<uint64_t>
{
  static uint64_t rand()
  {
    std::default_random_engine random(time(NULL));
    std::uniform_int_distribution<uint64_t> dist(0, UINT64_MAX);
    return dist(random);
  }
};

template<>
struct RandomData<double>
{
  static double rand()
  {
    std::default_random_engine random(time(NULL));
    std::uniform_real_distribution<double> dist(-9999999999.0, 9999999999.0);
    return dist(random);
  }
};

template<>
struct RandomData<float>
{
  static float rand()
  {
    std::default_random_engine random(time(NULL));
    std::uniform_real_distribution<float> dist(-9999999999.0, 9999999999.0);
    return dist(random);
  }
};

template<>
struct RandomData<bool>
{
  static bool rand()
  {
    std::default_random_engine random(time(NULL));
    std::uniform_real_distribution<float> dist(-10.0, 10.0);
    return dist(random) >= 0;
  }
};

template<>
struct RandomData<ObOTimestampData>
{
  static ObOTimestampData rand()
  {
    std::default_random_engine random(time(NULL));
    std::uniform_int_distribution<int64_t> us(1, 100000000);
    std::uniform_int_distribution<int16_t> tz(1, 2047);
    std::uniform_int_distribution<int16_t> tran_type(0, 31);
    std::uniform_int_distribution<int16_t> tail_nsec(0,999);
    ObOTimestampData::UnionTZCtx rand_tz_ctx;
    rand_tz_ctx.tail_nsec_ = tail_nsec(random);
    rand_tz_ctx.version_ = 0;
    rand_tz_ctx.store_tz_id_ = true;
    rand_tz_ctx.is_null_ = false;
    rand_tz_ctx.time_reserved_ = 0;

    rand_tz_ctx.tz_id_ = tz(random);
    rand_tz_ctx.tran_type_id_ = tran_type(random);

    int64_t time_us = us(random);

    return ObOTimestampData(time_us, rand_tz_ctx);
  }
};

template<>
struct RandomData<ObIntervalDSValue>
{
  static ObIntervalDSValue rand()
  {
    std::default_random_engine random(time(NULL));
    std::uniform_int_distribution<int32_t> frac_sec(INT32_MIN, INT32_MAX);
    std::uniform_int_distribution<int64_t> nsec(INT64_MIN, INT64_MAX);
    return ObIntervalDSValue(nsec(random), frac_sec(random));
  }
};


// sql::ObExpr *rand_data_expr(VecValueTypeClass data_tc, sql::ObEvalCtx)
} // end common
} // oceanbase