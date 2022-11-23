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

#include "lib/utility/utility.h"
#include "lib/number/ob_number_v2.h"
#include "lib/ob_define.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/alloc/alloc_struct.h"
#include "lib/charset/ob_dtoa.h"
#include "lib/utility/ob_fast_convert.h"
#include <gtest/gtest.h>
#include <float.h>
#include <cmath>
using namespace oceanbase::common;
using namespace oceanbase::common::number;
using namespace oceanbase::lib;

template <typename T, int64_t BUFFER_NUM = 16>
const char* format(const T &obj, int16_t scale)
{
  static const int64_t BUFFER_SIZE = 4096;
  static __thread char buffers[BUFFER_NUM][BUFFER_SIZE];
  static __thread uint64_t i = 0;
  char *buffer = buffers[i++ % BUFFER_NUM];
  memset(buffer, 0, BUFFER_SIZE);
  int64_t length = 0;
  //int16_t scale = 0;
  obj.format(buffer, BUFFER_SIZE, length, scale);
  return buffer;
}

class LimitedAllocator : public ObIAllocator
{
  public:
    LimitedAllocator() {};
    ~LimitedAllocator() {};
  public:
    void *alloc(const int64_t size)
    {
      return alloc(size, default_memattr);
    };
    void *alloc(const int64_t size, const ObMemAttr &attr)
    {
      void *ret = NULL;
      if (size <= (int64_t)sizeof(buffer_))
      {
        ret = buffer_;
      }
      return ret;
    };
  void free(void *) {}
  private:
    uint32_t buffer_[number::ObNumber::MAX_STORE_LEN + 1];
};

class MockPoly : public number::ObCalcVector
{
  public:
    MockPoly() : idx_(0) {};
  public:
    template <typename... Args>
    MockPoly(const uint64_t base, const Args&... args)
    {
      fill(base, args...);
    };
  public:
    template <typename... Args>
    void fill(const uint64_t base, const Args&... args)
    {
      set_base(base);
      idx_ = 0;
      fill_(args...);
    };
    template <typename T, typename... Args>
    void fill_(const T &head, const Args&... args)
    {
      ensure(idx_ + 1);
      set(idx_, head);
      idx_ += 1;
      fill_(args...);
    };
    void fill_(void)
    {
    };
public:
    MockPoly ref(const int64_t start, const int64_t end) const
    {
      MockPoly ret;
      ret.set_base(this->base());
      ret.idx_ = idx_;
      number::ObCalcVector *ptr = &ret;
      *ptr = number::ObCalcVector::ref(start, end);
      return ret;
    };
    int64_t to_string(char* buffer, const int64_t length) const
    {
      int64_t pos = 0;
      databuff_printf(buffer, length, pos, "[");
      for (int64_t i = 0; i < size(); i++)
      {
        databuff_printf(buffer, length, pos, "%lu,", at(i));
      }
      databuff_printf(buffer, length, pos, "]");
      return pos;
    };
  public:
    int64_t idx_;
};

//class MockPoly
//{
//  public:
//    MockPoly() {};
//    ~MockPoly() {};
//    template <typename... Args>
//    MockPoly(const uint64_t base, const Args&... args)
//    {
//      fill(base, args...);
//    };
//  public:
//    void resize(const int64_t size)
//    {
//      store_.resize(size, 0);
//    };
//    template <typename... Args>
//    void fill(const uint64_t base, const Args&... args)
//    {
//      base_ = base;
//      store_.clear();
//      fill_(args...);
//    };
//    template <typename T, typename... Args>
//    void fill_(const T &head, const Args&... args)
//    {
//      store_.push_back(head);
//      fill_(args...);
//    };
//    void fill_(void)
//    {
//    };
//    int64_t to_string(char* buffer, const int64_t length) const
//    {
//      int64_t pos = 0;
//      databuff_printf(buffer, length, pos, "[");
//      std::vector<uint64_t>::const_iterator iter;
//      for (iter = store_.begin(); iter != store_.end(); iter++)
//      {
//        databuff_printf(buffer, length, pos, "%lu,", *iter);
//      }
//      databuff_printf(buffer, length, pos, "]");
//      return pos;
//    };
//  public:
//    uint64_t at(const int64_t idx) const
//    {
//      return store_[idx];
//    };
//    uint64_t base() const
//    {
//      return base_;
//    };
//    int64_t size() const
//    {
//      return store_.size();
//    };
//    int set(const int64_t idx, const uint64_t digit)
//    {
//      int ret = OB_SUCCESS;
//      if (idx >= (int64_t)store_.size())
//      {
//        ret = OB_BUF_NOT_ENOUGH;
//      }
//      else if (digit >= base_)
//      {
//        ret = OB_DECIMAL_OVERFLOW_WARN;
//      }
//      else
//      {
//        store_[idx] = digit & 0x00000000ffffffff;
//      }
//      return ret;
//    };
//    void set_base(const uint64_t base)
//    {
//      base_ = base;
//    };
//    int ensure(const int64_t size)
//    {
//      store_.resize(size, 0);
//      return OB_SUCCESS;
//    };
//    const MockPoly ref(const int64_t start, const int64_t end) const
//    {
//      MockPoly ret;
//      ret.base_ = base_;
//      for (int64_t i = start; i <= end; i++)
//      {
//        ret.store_.push_back(store_[i]);
//      }
//      return ret;
//    };
//    int assign(const MockPoly &other, const int64_t start, const int64_t end)
//    {
//      for (int64_t i = start; i <= end; i++)
//      {
//        store_[i] = other.store_[i - start];
//      }
//      return OB_SUCCESS;
//    }
//  private:
//    uint64_t base_;
//    std::vector<uint64_t> store_;
//};

TEST(ObNumber, poly_mono_mul)
{
  MockPoly poly(10, 4);
  uint64_t multiplier = 0;
  MockPoly product(10);
  product.resize(poly.size() + 1);
  int ret = OB_SUCCESS;

  multiplier = 1;
  ret = poly_mono_mul(poly, multiplier, product);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ(to_cstring(MockPoly(10, 0, 4)), to_cstring(product));

  multiplier = 2;
  ret = poly_mono_mul(poly, multiplier, product);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ(to_cstring(MockPoly(10, 0, 8)), to_cstring(product));

  multiplier = 3;
  ret = poly_mono_mul(poly, multiplier, product);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ(to_cstring(MockPoly(10, 1, 2)), to_cstring(product));

  product.resize(poly.size());
  ret = poly_mono_mul(poly, multiplier, product);
  EXPECT_EQ(OB_INVALID_ARGUMENT, ret);
  product.resize(poly.size() + 2);
  ret = poly_mono_mul(poly, multiplier, product);
  EXPECT_EQ(OB_INVALID_ARGUMENT, ret);
}

TEST(ObNumber, poly_mono_div)
{
  MockPoly poly(10, 1, 2);
  uint64_t divisor = 0;
  MockPoly quotient(10);
  uint64_t remainder = 0;
  quotient.resize(poly.size());
  int ret = OB_SUCCESS;

  divisor = 1;
  ret = poly_mono_div(poly, divisor, quotient, remainder);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ(to_cstring(MockPoly(10, 1, 2)), to_cstring(quotient));
  EXPECT_EQ(0, remainder);

  divisor = 3;
  ret = poly_mono_div(poly, divisor, quotient, remainder);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ(to_cstring(MockPoly(10, 0, 4)), to_cstring(quotient));
  EXPECT_EQ(0, remainder);

  divisor = 5;
  ret = poly_mono_div(poly, divisor, quotient, remainder);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ(to_cstring(MockPoly(10, 0, 2)), to_cstring(quotient));
  EXPECT_EQ(2, remainder);

  poly.fill(10, 8);
  quotient.resize(poly.size());
  divisor = 9;
  ret = poly_mono_div(poly, divisor, quotient, remainder);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ(to_cstring(MockPoly(10, 0)), to_cstring(quotient));
  EXPECT_EQ(8, remainder);

  quotient.resize(poly.size() - 1);
  ret = poly_mono_div(poly, divisor, quotient, remainder);
  EXPECT_EQ(OB_INVALID_ARGUMENT, ret);
  quotient.resize(poly.size() + 1);
  ret = poly_mono_div(poly, divisor, quotient, remainder);
  EXPECT_EQ(OB_INVALID_ARGUMENT, ret);
}

TEST(ObNumber, poly_poly_add)
{
  MockPoly augend(10, 9, 9, 8);
  MockPoly addend(10, 1);
  MockPoly sum(10);
  int ret = OB_SUCCESS;

  sum.resize(std::max(augend.size(), addend.size()) + 1);
  ret = poly_poly_add(augend, addend, sum);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ(to_cstring(MockPoly(10, 0, 9, 9, 9)), to_cstring(sum));

  addend.fill(10, 2);
  sum.resize(std::max(augend.size(), addend.size()) + 1);
  ret = poly_poly_add(augend, addend, sum);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ(to_cstring(MockPoly(10, 1, 0, 0, 0)), to_cstring(sum));

  addend.fill(10, 9, 9, 9);
  sum.resize(std::max(augend.size(), addend.size()) + 1);
  ret = poly_poly_add(augend, addend, sum);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ(to_cstring(MockPoly(10, 1, 9, 9, 7)), to_cstring(sum));

  sum.resize(std::max(augend.size(), addend.size()));
  ret = poly_poly_add(augend, addend, sum);
  EXPECT_EQ(OB_INVALID_ARGUMENT, ret);
  sum.resize(std::max(augend.size(), addend.size()) + 2);
  ret = poly_poly_add(augend, addend, sum);
  EXPECT_EQ(OB_INVALID_ARGUMENT, ret);
}

TEST(ObNumber, poly_poly_sub)
{
  MockPoly minuend(10, 9, 9, 8);
  MockPoly subtrahend(10, 1);
  MockPoly remainder(10);
  bool negative = false;
  int ret = OB_SUCCESS;

  remainder.resize(std::max(minuend.size(), subtrahend.size()));
  ret = poly_poly_sub(minuend, subtrahend, remainder, negative);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ(to_cstring(MockPoly(10, 9, 9, 7)), to_cstring(remainder));
  EXPECT_FALSE(negative);

  subtrahend.fill(10, 9, 9, 9);
  remainder.resize(std::max(minuend.size(), subtrahend.size()));
  ret = poly_poly_sub(minuend, subtrahend, remainder, negative);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ(to_cstring(MockPoly(10, 0, 0, 1)), to_cstring(remainder));
  EXPECT_TRUE(negative);

  subtrahend.fill(10, 9, 9, 9, 9, 9);
  remainder.resize(std::max(minuend.size(), subtrahend.size()));
  ret = poly_poly_sub(minuend, subtrahend, remainder, negative);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ(to_cstring(MockPoly(10, 9, 9, 0, 0, 1)), to_cstring(remainder));
  EXPECT_TRUE(negative);

  remainder.resize(std::max(minuend.size(), subtrahend.size()) - 1);
  ret = poly_poly_sub(minuend, subtrahend, remainder, negative);
  EXPECT_EQ(OB_INVALID_ARGUMENT, ret);
  remainder.resize(std::max(minuend.size(), subtrahend.size()) + 1);
  ret = poly_poly_sub(minuend, subtrahend, remainder, negative);
  EXPECT_EQ(OB_INVALID_ARGUMENT, ret);
}

TEST(ObNumber, poly_poly_mul)
{
  MockPoly multiplicand(10, 9, 9, 9, 9);
  MockPoly multiplier(10, 1);
  MockPoly product(10);
  int ret = OB_SUCCESS;

  product.resize(multiplicand.size() + multiplier.size());
  ret = poly_poly_mul(multiplicand, multiplier, product);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ(to_cstring(MockPoly(10, 0, 9, 9, 9, 9)), to_cstring(product));

  multiplier.fill(10, 9, 9);
  product.resize(multiplicand.size() + multiplier.size());
  ret = poly_poly_mul(multiplicand, multiplier, product);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ(to_cstring(MockPoly(10, 9, 8, 9, 9, 0, 1)), to_cstring(product));

  multiplier.fill(10, 9, 9, 9, 9);
  product.resize(multiplicand.size() + multiplier.size());
  ret = poly_poly_mul(multiplicand, multiplier, product);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ(to_cstring(MockPoly(10, 9, 9, 9, 8, 0, 0, 0, 1)), to_cstring(product));

  multiplicand.fill(10, 1, 0, 0, 0);
  multiplier.fill(10, 1, 0, 0, 0);
  product.resize(multiplicand.size() + multiplier.size());
  ret = poly_poly_mul(multiplicand, multiplier, product);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ(to_cstring(MockPoly(10, 0, 1, 0, 0, 0, 0, 0, 0)), to_cstring(product));

  product.resize(multiplicand.size() + multiplier.size() - 1);
  ret = poly_poly_mul(multiplicand, multiplier, product);
  EXPECT_EQ(OB_INVALID_ARGUMENT, ret);
  product.resize(multiplicand.size() + multiplier.size() + 1);
  ret = poly_poly_mul(multiplicand, multiplier, product);
  EXPECT_EQ(OB_INVALID_ARGUMENT, ret);

  multiplicand.fill(0x0000000100000000, 0xffffffff, 0xffffffff, 0xffffffff, 0xffffffff);
  multiplier.fill(0x0000000100000000, 0xffffffff, 0xffffffff, 0xffffffff, 0xffffffff);
  product.fill(0x0000000100000000);
  product.resize(multiplicand.size() + multiplier.size());
  ret = poly_poly_mul(multiplicand, multiplier, product);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ(to_cstring(MockPoly(0x0000000100000000, 4294967295, 4294967295, 4294967295, 4294967294, 0, 0, 0, 1)), to_cstring(product));
}

//TEST(ObNumber, poly_poly_div)
//{
//  MockPoly dividend(10, 9, 9, 9);
//  MockPoly divisor(10, 4, 5);
//  MockPoly quotient(10);
//  MockPoly remainder(10);
//  int ret = OB_SUCCESS;
//
//  quotient.resize(dividend.size() - divisor.size() + 1);
//  remainder.resize(divisor.size());
//  ret = poly_poly_div(dividend, divisor, quotient, remainder);
//  EXPECT_EQ(OB_SUCCESS, ret);
//  EXPECT_STREQ(to_cstring(MockPoly(10, 2, 2)), to_cstring(quotient));
//  EXPECT_STREQ(to_cstring(MockPoly(10, 0, 9)), to_cstring(remainder));
//
//  dividend.fill(10, 4, 1, 0, 0);
//  divisor.fill(10, 5, 8, 8);
//  quotient.resize(dividend.size() - divisor.size() + 1);
//  remainder.resize(divisor.size());
//  ret = poly_poly_div(dividend, divisor, quotient, remainder);
//  EXPECT_EQ(OB_SUCCESS, ret);
//  EXPECT_STREQ(to_cstring(MockPoly(10, 0, 6)), to_cstring(quotient));
//  EXPECT_STREQ(to_cstring(MockPoly(10, 5, 7, 2)), to_cstring(remainder));
//
//  dividend.fill(10, 4, 1, 0, 0);
//  divisor.fill(10, 2, 0);
//  quotient.resize(dividend.size() - divisor.size() + 1);
//  remainder.resize(divisor.size());
//  ret = poly_poly_div(dividend, divisor, quotient, remainder);
//  EXPECT_EQ(OB_SUCCESS, ret);
//  EXPECT_STREQ(to_cstring(MockPoly(10, 2, 0, 5)), to_cstring(quotient));
//  EXPECT_STREQ(to_cstring(MockPoly(10, 0, 0)), to_cstring(remainder));
//
//  dividend.fill(10, 4, 1, 0, 0);
//  divisor.fill(10, 2);
//  quotient.resize(dividend.size() - divisor.size() + 1);
//  remainder.resize(divisor.size());
//  ret = poly_poly_div(dividend, divisor, quotient, remainder);
//  EXPECT_EQ(OB_SUCCESS, ret);
//  EXPECT_STREQ(to_cstring(MockPoly(10, 2, 0, 5, 0)), to_cstring(quotient));
//  EXPECT_STREQ(to_cstring(MockPoly(10, 0)), to_cstring(remainder));
//
//  dividend.fill(10, 4, 1, 0, 0);
//  divisor.fill(10, 1);
//  quotient.resize(dividend.size() - divisor.size() + 1);
//  remainder.resize(divisor.size());
//  ret = poly_poly_div(dividend, divisor, quotient, remainder);
//  EXPECT_EQ(OB_SUCCESS, ret);
//  EXPECT_STREQ(to_cstring(MockPoly(10, 4, 1, 0, 0)), to_cstring(quotient));
//  EXPECT_STREQ(to_cstring(MockPoly(10, 0)), to_cstring(remainder));
//
//  dividend.fill(10, 1, 1);
//  divisor.fill(10, 3);
//  quotient.resize(dividend.size() - divisor.size() + 1);
//  remainder.resize(divisor.size());
//  ret = poly_poly_div(dividend, divisor, quotient, remainder);
//  EXPECT_EQ(OB_SUCCESS, ret);
//  EXPECT_STREQ(to_cstring(MockPoly(10, 0, 3)), to_cstring(quotient));
//  EXPECT_STREQ(to_cstring(MockPoly(10, 2)), to_cstring(remainder));
//}

class ParseHelperTester : public ObNumberBuilder
{
  public:
    ParseHelperTester() {};
    ~ParseHelperTester() {};
  public:
    int fill(const char *str)
    {
      return number_.from(str, allocator_);
    };
    int fill_old(const char *str)
    {
      return number_.from_v1(str, strlen(str), allocator_);
    };
    int from(const uint32_t desc, ObCalcVector &vector)
    {
      return number_.from(desc, vector, allocator_);
    };
  private:
    CharArena allocator_;
};

std::string negate(const char* str)
{
  std::string ret;
  if ('-' == str[0])
  {
    ret = &str[1];
  }
  else if (0 != strcmp(str, "0"))
  {
    ret = "-";
    ret += str;
  }
  else
  {
    ret = "0";
  }
  return ret;
}

TEST(ObNumberParseHelper, from_format)
{
  uint32_t digits[number::ObNumber::MAX_STORE_LEN];
  number::ObNumber::Desc desc;
  desc.len_ = 0;
  desc.reserved_ = 0;
  desc.sign_ = number::ObNumber::POSITIVE;
  desc.exp_ = number::ObNumber::EXP_ZERO;
  ParseHelperTester nph;
  nph.number_.assign(desc.desc_, digits);
  int ret = OB_SUCCESS;

  const int64_t MAX_BUF_SIZE = 256;
  char buf_v1[MAX_BUF_SIZE];
  char buf_v2[MAX_BUF_SIZE];
  int64_t pos_v1 = 0;
  int64_t pos_v2 = 0;

  char buf_alloc[MAX_BUF_SIZE];
  int64_t pos = 0;
  ObDataBuffer allocator(buf_alloc, MAX_BUF_SIZE);

  number::ObNumber num1;
  number::ObNumber num2;
  EXPECT_EQ(OB_SUCCESS, num1.from_v1("0.1", 3, allocator));
  EXPECT_EQ(OB_SUCCESS, num2.from_v2("0.1", 3, allocator));

  OB_LOG(INFO, "debug jianhua",  K(num1), K(num2));
  ASSERT_EQ(0, num1.compare_v1(num2));
  allocator.free();

  // positive
  ret = nph.fill("1.1"); // normal
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("1.1", format(nph.number_, 1));
  EXPECT_EQ(0x40, nph.get_exp());
  EXPECT_EQ(2, nph.get_length());

  ret = nph.fill("0.1"); // normal
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("0.1", format(nph.number_, 1));
  EXPECT_EQ(0x3f, nph.get_exp());
  EXPECT_EQ(1, nph.get_length());

  ret = nph.fill("1.01"); // normal
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("1.01", format(nph.number_, 2));
  EXPECT_EQ(0x40, nph.get_exp());
  EXPECT_EQ(2, nph.get_length());

  ret = nph.fill("1.000000001"); // decimal 1digit
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("1.000000001", format(nph.number_, 9));
  EXPECT_EQ(0x40, nph.get_exp());
  EXPECT_EQ(2, nph.get_length());

  ret = nph.fill("1.000000001234"); // decimal 2digit, decimal first digit not zero
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("1.000000001234", format(nph.number_, 12));
  EXPECT_EQ(0x40, nph.get_exp());
  EXPECT_EQ(3, nph.get_length());

  ret = nph.fill("1.0000000001234"); // decimal 2digit, decimal first digit is zero
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("1.0000000001234", format(nph.number_, 13));
  EXPECT_EQ(0x40, nph.get_exp());
  EXPECT_EQ(3, nph.get_length());

  ret = nph.fill("0.000000001"); // decimal 1digit
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("0.000000001", format(nph.number_, 9));
  EXPECT_EQ(0x3f, nph.get_exp());
  EXPECT_EQ(1, nph.get_length());

  ret = nph.fill("0.000000001234"); // decimal 2digit, decimal first digit not zero
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("0.000000001234", format(nph.number_, 12));
  EXPECT_EQ(0x3f, nph.get_exp());
  EXPECT_EQ(2, nph.get_length());

  ret = nph.fill("0.0000000001234"); // decimal 2digit, decimal first digit is zero
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("0.0000000001234", format(nph.number_, 13));
  EXPECT_EQ(0x3e, nph.get_exp());
  EXPECT_EQ(1, nph.get_length());

  ret = nph.fill("00.1"); // extra zero on integer head
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("0.1", format(nph.number_, 1));
  EXPECT_EQ(0x3f, nph.get_exp());
  EXPECT_EQ(1, nph.get_length());

  ret = nph.fill("0000000000.1"); //extra zero on integer head, integer digit occupy 2digit
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("0.1", format(nph.number_, 1));
  EXPECT_EQ(0x3f, nph.get_exp());
  EXPECT_EQ(1, nph.get_length());

  ret = nph.fill("123456789.01"); // integer 1digit
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("123456789.01", format(nph.number_, 2));
  EXPECT_EQ(0x40, nph.get_exp());
  EXPECT_EQ(2, nph.get_length());

  ret = nph.fill("1234567891.01"); // integer 2digit, 1over
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("1234567891.01", format(nph.number_, 2));
  EXPECT_EQ(0x41, nph.get_exp());
  EXPECT_EQ(3, nph.get_length());

  ret = nph.fill("12345678910.01"); // integer 2digit, 2over
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("12345678910.01", format(nph.number_, 2));
  EXPECT_EQ(0x41, nph.get_exp());
  EXPECT_EQ(3, nph.get_length());

  ret = nph.fill("123456789100.01"); // integer 2digit, 3over
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("123456789100.01", format(nph.number_, 2));
  EXPECT_EQ(0x41, nph.get_exp());
  EXPECT_EQ(3, nph.get_length());

  ret = nph.fill("123456789100000000.01"); // integer 2digit, align
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("123456789100000000.01", format(nph.number_, 2));
  EXPECT_EQ(0x41, nph.get_exp());
  EXPECT_EQ(3, nph.get_length());

  ret = nph.fill("1234567891000000000.01"); // integer 3digit, 1over
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("1234567891000000000.01", format(nph.number_, 2));
  EXPECT_EQ(0x42, nph.get_exp());
  EXPECT_EQ(4, nph.get_length());

  ret = nph.fill("1234567890000000001.01"); // integer 3digit, 1over
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("1234567890000000001.01", format(nph.number_, 2));
  EXPECT_EQ(0x42, nph.get_exp());
  EXPECT_EQ(4, nph.get_length());

  ret = nph.fill("12345678910000000000.01"); // integer 3digit, 2over
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("12345678910000000000.01", format(nph.number_, 2));
  EXPECT_EQ(0x42, nph.get_exp());
  EXPECT_EQ(4, nph.get_length());

  ret = nph.fill("12345678900000000001.01"); // integer 3digit, 2over
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("12345678900000000001.01", format(nph.number_, 2));
  EXPECT_EQ(0x42, nph.get_exp());
  EXPECT_EQ(4, nph.get_length());

  ret = nph.fill("12345678900000000001"); // integer 3digit, 2over
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("12345678900000000001", format(nph.number_, 0));
  EXPECT_EQ(0x42, nph.get_exp());
  EXPECT_EQ(3, nph.get_length());

  ////////////////////////////////////////////////////////////////////////////////////////////////////

  // negative
  ret = nph.fill("-1.1"); // normal
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("-1.1", format(nph.number_, 1));
  EXPECT_EQ(0x40, nph.get_exp());
  EXPECT_EQ(2, nph.get_length());

  ret = nph.fill("-0.1"); // normal
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("-0.1", format(nph.number_, 1));
  EXPECT_EQ(0x41, nph.get_exp());
  EXPECT_EQ(1, nph.get_length());

  ret = nph.fill("-1.01"); // normal
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("-1.01", format(nph.number_, 2));
  EXPECT_EQ(0x40, nph.get_exp());
  EXPECT_EQ(2, nph.get_length());

  ret = nph.fill("-1.000000001"); // decimal 1digit
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("-1.000000001", format(nph.number_, 9));
  EXPECT_EQ(0x40, nph.get_exp());
  EXPECT_EQ(2, nph.get_length());

  ret = nph.fill("-1.000000001234"); // decimal 2digit, decimal first digit not zero
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("-1.000000001234", format(nph.number_, 12));
  EXPECT_EQ(0x40, nph.get_exp());
  EXPECT_EQ(3, nph.get_length());

  ret = nph.fill("-1.0000000001234"); // decimal 2digit, decimal first digit is zero
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("-1.0000000001234", format(nph.number_, 13));
  EXPECT_EQ(0x40, nph.get_exp());
  EXPECT_EQ(3, nph.get_length());

  ret = nph.fill("-0.000000001"); // decimal 1digit
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("-0.000000001", format(nph.number_, 9));
  EXPECT_EQ(0x41, nph.get_exp());
  EXPECT_EQ(1, nph.get_length());

  ret = nph.fill("-0.000000001234"); // decimal 2digit, decimal first digit not zero
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("-0.000000001234", format(nph.number_, 12));
  EXPECT_EQ(0x41, nph.get_exp());
  EXPECT_EQ(2, nph.get_length());

  ret = nph.fill("-0.0000000001234"); // decimal 2digit, decimal first digit is zero
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("-0.0000000001234", format(nph.number_, 13));
  EXPECT_EQ(0x42, nph.get_exp());
  EXPECT_EQ(1, nph.get_length());

  ret = nph.fill("-00.1"); // extra zero on integer head
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("-0.1", format(nph.number_, 1));
  EXPECT_EQ(0x41, nph.get_exp());
  EXPECT_EQ(1, nph.get_length());

  ret = nph.fill("-0000000000.1"); //extra zero on integer head, integer digit occupy 2digit
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("-0.1", format(nph.number_, 1));
  EXPECT_EQ(0x41, nph.get_exp());
  EXPECT_EQ(1, nph.get_length());

  ret = nph.fill("-123456789.01"); // integer 1digit
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("-123456789.01", format(nph.number_, 2));
  EXPECT_EQ(0x40, nph.get_exp());
  EXPECT_EQ(2, nph.get_length());

  ret = nph.fill("-1234567891.01"); // integer 2digit, 1over
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("-1234567891.01", format(nph.number_, 2));
  EXPECT_EQ(0x3f, nph.get_exp());
  EXPECT_EQ(3, nph.get_length());

  ret = nph.fill("-12345678910.01"); // integer 2digit, 2over
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("-12345678910.01", format(nph.number_, 2));
  EXPECT_EQ(0x3f, nph.get_exp());
  EXPECT_EQ(3, nph.get_length());

  ret = nph.fill("-123456789100.01"); // integer 2digit, 3over
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("-123456789100.01", format(nph.number_, 2));
  EXPECT_EQ(0x3f, nph.get_exp());
  EXPECT_EQ(3, nph.get_length());

  ret = nph.fill("-123456789100000000.01"); // integer 2digit, align
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("-123456789100000000.01", format(nph.number_, 2));
  EXPECT_EQ(0x3f, nph.get_exp());
  EXPECT_EQ(3, nph.get_length());

  ret = nph.fill("-1234567891000000000.01"); // integer 3digit, 1over
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("-1234567891000000000.01", format(nph.number_, 2));
  EXPECT_EQ(0x3e, nph.get_exp());
  EXPECT_EQ(4, nph.get_length());

  ret = nph.fill("-1234567890000000001.01"); // integer 3digit, 1over
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("-1234567890000000001.01", format(nph.number_, 2));
  EXPECT_EQ(0x3e, nph.get_exp());
  EXPECT_EQ(4, nph.get_length());

  ret = nph.fill("-12345678910000000000.01"); // integer 3digit, 2over
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("-12345678910000000000.01", format(nph.number_, 2));
  EXPECT_EQ(0x3e, nph.get_exp());
  EXPECT_EQ(4, nph.get_length());

  ret = nph.fill("-12345678900000000001.01"); // integer 3digit, 2over
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("-12345678900000000001.01", format(nph.number_, 2));
  EXPECT_EQ(0x3e, nph.get_exp());
  EXPECT_EQ(4, nph.get_length());

  ret = nph.fill("-12345678900000000001"); // integer 3digit, 2over
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("-12345678900000000001", format(nph.number_, 0));
  EXPECT_EQ(0x3e, nph.get_exp());
  EXPECT_EQ(3, nph.get_length());

  ////////////////////////////////////////////////////////////////////////////////////////////////////

  // abnormal
  ret = nph.fill("9999999999999999999999999999999999999999999994");
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("9999999999999999999999999999999999999999999994", format(nph.number_, 0));

  ret = nph.fill("9999999999999999999999999999999999999999999995");
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("9999999999999999999999999999999999999999999995", format(nph.number_, 0));

  ret = nph.fill("1111111112222222223333333334444444445555555556");
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("1111111112222222223333333334444444445555555556", format(nph.number_, 0));

  ret = nph.fill("111111111222222222333333333444444444555555555");
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("111111111222222222333333333444444444555555555", format(nph.number_, 0));

  ret = nph.fill("111111111222222222333333333444444444555555555666666666");
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("111111111222222222333333333444444444555555555666666666", format(nph.number_, 0));

  ret = nph.fill("1111111112222222223333333334444444445555555556666666667");
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("1111111112222222223333333334444444445555555556666666667", format(nph.number_, 0));

  ret = nph.fill("11111111122222222233333333344444444455555555.5666666/66");
  EXPECT_EQ(OB_INVALID_NUMERIC, ret);
  EXPECT_STREQ("11111111122222222233333333344444444455555555.6", format(nph.number_, 1));

  ret = nph.fill("11111111122222222233333333344444444455555555.5666666667");
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("11111111122222222233333333344444444455555555.6", format(nph.number_, 1));

  ret = nph.fill(".1111111112222222223333333334444444445555555556");
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("0.111111111222222222333333333444444444555555556", format(nph.number_, 45));

  ret = nph.fill(".111111111222222222333333333444444444555555555");
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("0.111111111222222222333333333444444444555555555", format(nph.number_, 45));

  ret = nph.fill("111111111.2222222223333333334444444445555555556");
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("111111111.222222222333333333444444444555555556", format(nph.number_, 36));

  ret = nph.fill("111111111.222222222333333333444444444555555555");
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("111111111.222222222333333333444444444555555555", format(nph.number_, 36));

  ret = nph.fill("111111111.222222222333333333444444444555555555666666666");
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("111111111.222222222333333333444444444555555556", format(nph.number_, 36));

  ret = nph.fill("111111111.2222222223333333334444444445555555556666666667");
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("111111111.222222222333333333444444444555555556", format(nph.number_, 36));

  ret = nph.fill("1111111112.2222222223333333334444444445555555546666666667");
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("1111111112.22222222233333333344444444455555555", format(nph.number_, 35));

  ret = nph.fill("1111111112.2222222223333333334444444445555555576666666667");
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("1111111112.22222222233333333344444444455555556", format(nph.number_, 35));

  ret = nph.fill("-9999999999999999999999999999999999999999999994");
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("-9999999999999999999999999999999999999999999994", format(nph.number_, 0));

  ret = nph.fill("-9999999999999999999999999999999999999999999995");
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("-9999999999999999999999999999999999999999999995", format(nph.number_, 0));

  ret = nph.fill("-1111111112222222223333333334444444445555555556");
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("-1111111112222222223333333334444444445555555556", format(nph.number_, 0));

  ret = nph.fill("-111111111222222222333333333444444444555555555");
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("-111111111222222222333333333444444444555555555", format(nph.number_, 0));

  ret = nph.fill("-111111111222222222333333333444444444555555555666666666");
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("-111111111222222222333333333444444444555555555666666666", format(nph.number_, 0));

  ret = nph.fill("-1111111112222222223333333334444444445555555556666666667");
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("-1111111112222222223333333334444444445555555556666666667", format(nph.number_, 0));

  ret = nph.fill("-11111111122222222233333333344444444455555555.566666666");
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("-11111111122222222233333333344444444455555555.6", format(nph.number_, 1));

  ret = nph.fill("-11111111122222222233333333344444444455555555.5666666667");
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("-11111111122222222233333333344444444455555555.6", format(nph.number_, 1));

  ret = nph.fill("-.1111111112222222223333333334444444445555555556");
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("-0.111111111222222222333333333444444444555555556", format(nph.number_, 45));

  ret = nph.fill("-.111111111222222222333333333444444444555555555");
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("-0.111111111222222222333333333444444444555555555", format(nph.number_, 45));

  ret = nph.fill("-111111111.2222222223333333334444444445555555556");
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("-111111111.222222222333333333444444444555555556", format(nph.number_, 36));

  ret = nph.fill("-111111111.222222222333333333444444444555555555");
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("-111111111.222222222333333333444444444555555555", format(nph.number_, 36));

  ret = nph.fill("-111111111.222222222333333333444444444555555555666666666");
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("-111111111.222222222333333333444444444555555556", format(nph.number_, 36));

  ret = nph.fill("-111111111.2222222223333333334444444445555555556666666667");
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("-111111111.222222222333333333444444444555555556", format(nph.number_, 36));

  ret = nph.fill("-1111111112.2222222223333333334444444445555555546666666667");
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("-1111111112.22222222233333333344444444455555555", format(nph.number_, 35));

  ret = nph.fill("-1111111112.2222222223333333334444444445555555576666666667");
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("-1111111112.22222222233333333344444444455555556", format(nph.number_, 35));

  ret = nph.fill("0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001");
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("0", format(nph.number_, 0));
  ret = nph.fill(".0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001");
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("0", format(nph.number_, 0));

  ret = nph.fill("888888888888888888888888888");
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("888888888888888888888888888", format(nph.number_, 0));

  //代码中限制补零的个数为60为上限，否则报4002错误，所以下面的执行format后，返回错误码为4002
  //ret = nph.fill("0.000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001");
  //EXPECT_EQ(OB_SUCCESS, ret);
  //EXPECT_STREQ("0.000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001", format(nph.number_, 135));

  //ret = nph.fill("0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000010");
  //EXPECT_EQ(OB_SUCCESS, ret);
  //EXPECT_STREQ("0.000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001", format(nph.number_, 135));

  //ret = nph.fill(".000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001");
  //EXPECT_EQ(OB_SUCCESS, ret);
  //EXPECT_STREQ("0.000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001", format(nph.number_, 135));



  ret = nph.fill("1000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000");
  EXPECT_EQ(OB_INTEGER_PRECISION_OVERFLOW, ret);
  ret = nph.fill("1000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.");
  EXPECT_EQ(OB_INTEGER_PRECISION_OVERFLOW, ret);
  ret = nph.fill("1000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.0");
  EXPECT_EQ(OB_INTEGER_PRECISION_OVERFLOW, ret);

  ret = nph.fill("999999999999999999999999999999999999999999999000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000");
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("999999999999999999999999999999999999999999999000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000", format(nph.number_, 0));
  ret = nph.fill("999999999999999999999999999999999999999999999000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.");
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("999999999999999999999999999999999999999999999000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000", format(nph.number_, 0));
  ret = nph.fill("999999999999999999999999999999999999999999999000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000.0");
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("999999999999999999999999999999999999999999999000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000", format(nph.number_, 0));

  // special
  ret = nph.fill("0");
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("0", format(nph.number_, 0));

  ret = nph.fill("0.0");
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("0", format(nph.number_, 0));

  ret = nph.fill("0.0000000000");
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("0", format(nph.number_, 0));

  ret = nph.fill("0000000000.0");
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("0", format(nph.number_, 0));

  ret = nph.fill("-0.0");
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("0", format(nph.number_, 0));

  ret = nph.fill("-0.0000000000");
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("0", format(nph.number_, 0));

  ret = nph.fill("-0000000000.0");
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("0", format(nph.number_, 0));

}

#define EQ(str1, str2) \
  EXPECT_EQ(OB_SUCCESS, nph1.fill(str1)); \
  EXPECT_EQ(OB_SUCCESS, nph2.fill(str2)); \
  EXPECT_EQ(nph1.number_, nph2.number_); \
  EXPECT_LE(nph1.number_, nph2.number_); \
  EXPECT_GE(nph1.number_, nph2.number_); \
  EXPECT_EQ(OB_SUCCESS, nph3.fill_old(str2)); \
  EXPECT_EQ(nph3.number_, nph1.number_); \
  EXPECT_LE(nph1.number_, nph3.number_); \
  EXPECT_GE(nph1.number_, nph3.number_); \
  EXPECT_EQ(OB_SUCCESS, nph3.fill_old(str1)); \
  EXPECT_EQ(nph3.number_, nph1.number_); \
  EXPECT_EQ(nph3.number_, nph2.number_); \
  EXPECT_LE(nph3.number_, nph2.number_); \
  EXPECT_GE(nph3.number_, nph2.number_);

#define NE(str1, str2) \
  EXPECT_EQ(OB_SUCCESS, nph1.fill(str1)); \
  EXPECT_EQ(OB_SUCCESS, nph2.fill(str2)); \
  EXPECT_NE(nph1, nph2); \
  EXPECT_EQ(OB_SUCCESS, nph3.fill_old(str2)); \
  EXPECT_EQ(nph3, nph2); \
  EXPECT_NE(nph1, nph3); \
  EXPECT_EQ(OB_SUCCESS, nph3.fill_old(str1)); \
  EXPECT_EQ(nph3, nph1); \
  EXPECT_NE(nph3, nph2);


#define LT(str1, str2) \
  EXPECT_EQ(OB_SUCCESS, nph1.fill(str1)); \
  EXPECT_EQ(OB_SUCCESS, nph2.fill(str2)); \
  EXPECT_TRUE(nph1.number_ < nph2.number_); \
  EXPECT_TRUE(nph1.number_ <= nph2.number_); \
  EXPECT_EQ(OB_SUCCESS, nph3.fill_old(str2)); \
  EXPECT_TRUE(nph3.number_ == nph2.number_); \
  EXPECT_TRUE(nph1.number_ < nph3.number_); \
  EXPECT_TRUE(nph1.number_ <= nph3.number_); \
  EXPECT_EQ(OB_SUCCESS, nph3.fill_old(str1)); \
  EXPECT_TRUE(nph3.number_ == nph1.number_); \
  EXPECT_TRUE(nph3.number_ < nph2.number_); \
  EXPECT_TRUE(nph3.number_ <= nph2.number_); \

#define LE(str1, str2) \
  EXPECT_EQ(OB_SUCCESS, nph1.fill(str1)); \
  EXPECT_EQ(OB_SUCCESS, nph2.fill(str2)); \
  EXPECT_TRUE(nph1.number_ <= nph2.number_); \
  EXPECT_EQ(OB_SUCCESS, nph3.fill_old(str2)); \
  EXPECT_TRUE(nph3.number_ == nph2.number_); \
  EXPECT_TRUE(nph1.number_ <= nph3.number_); \
  EXPECT_EQ(OB_SUCCESS, nph3.fill_old(str1)); \
  EXPECT_TRUE(nph3.number_ == nph1.number_); \
  EXPECT_TRUE(nph3.number_ <= nph2.number_);


#define GT(str1, str2) \
  EXPECT_EQ(OB_SUCCESS, nph1.fill(str1)); \
  EXPECT_EQ(OB_SUCCESS, nph2.fill(str2)); \
  EXPECT_TRUE(nph1.number_ > nph2.number_); \
  EXPECT_TRUE(nph1.number_ >= nph2.number_); \
  EXPECT_EQ(OB_SUCCESS, nph3.fill_old(str2)); \
  EXPECT_TRUE(nph3.number_ == nph2.number_); \
  EXPECT_TRUE(nph1.number_ > nph3.number_); \
  EXPECT_TRUE(nph1.number_ >= nph3.number_); \
  EXPECT_EQ(OB_SUCCESS, nph3.fill_old(str1)); \
  EXPECT_TRUE(nph3.number_ == nph1.number_); \
  EXPECT_TRUE(nph3.number_ > nph2.number_); \
  EXPECT_TRUE(nph2.number_ >= nph2.number_);

#define GE(str1, str2) \
  EXPECT_EQ(OB_SUCCESS, nph1.fill(str1)); \
  EXPECT_EQ(OB_SUCCESS, nph2.fill(str2)); \
  EXPECT_TRUE(nph1.number_ == nph2.number_); \
  EXPECT_EQ(OB_SUCCESS, nph3.fill(str2)); \
  EXPECT_TRUE(nph3.number_ == nph2.number_); \
  EXPECT_TRUE(nph1.number_ >= nph3.number_); \
  EXPECT_EQ(OB_SUCCESS, nph3.fill(str1)); \
  EXPECT_TRUE(nph3.number_ == nph1.number_); \
  EXPECT_TRUE(nph3.number_ >= nph2.number_); \


TEST(ObNumberParseHelper, compare)
{
  uint32_t digits1[number::ObNumber::MAX_STORE_LEN];
  uint32_t digits2[number::ObNumber::MAX_STORE_LEN];
  uint32_t digits3[number::ObNumber::MAX_STORE_LEN];
  number::ObNumber::Desc desc;
  ParseHelperTester nph1;
  ParseHelperTester nph2;
  ParseHelperTester nph3;

  desc.len_ = 0;
  desc.reserved_ = 0;
  desc.sign_ = number::ObNumber::POSITIVE;
  desc.exp_ = number::ObNumber::EXP_ZERO;
  nph1.number_.assign(desc.desc_, digits1);

  desc.len_ = 0;
  desc.reserved_ = 0;
  desc.sign_ = number::ObNumber::POSITIVE;
  desc.exp_ = number::ObNumber::EXP_ZERO;
  nph2.number_.assign(desc.desc_, digits2);

  desc.len_ = 0;
  desc.reserved_ = 0;
  desc.sign_ = number::ObNumber::POSITIVE;
  desc.exp_ = number::ObNumber::EXP_ZERO;
  nph3.number_.assign(desc.desc_, digits3);

  EQ("0.0", "0.0");
  EQ("0000000000.0", "0.0000000000");
  EQ("-0.0", "-0.0");
  EQ("-0000000000.0", "-0.0000000000");
  EQ("-0.0", "0.0");
  EQ("-0000000000.0", "0.0000000000");

  EQ("1.1", "1.1");
  EQ("0000000001.1", "1.1000000000");
  EQ("-1.1", "-1.1");
  EQ("-0000000001.1", "-1.1000000000");

  LT("0", "1.1");
  GT("1.1", "0");
  GT("0", "-1.1");
  LT("-1.1", "0");
  LT("-1.1", "-1");
  LT("1.0000000001", "1.0000000002");
  GT("-1.0000000001", "-1.0000000002");
  LT("1000000000.00000000001", "1000000000.00000000002");
  GT("-1000000000.00000000001", "-1000000000.00000000002");

  EQ("1234567890.1234567890", "1234567890.1234567890");
  EQ("-1234567890.1234567890", "-1234567890.1234567890");

  LT("-1234567890.1234567890", "1234567890.1234567890");
  GT("1234567890.1234567890", "-1234567890.1234567890");

  LT("1.234", "1.2345");
  GT("1.2345", "1.234");
  GT("-1.234", "-1.2345");
  LT("-1.2345", "-1.234");

  LT("1.234567891", "1.2345678912");
  GT("1.2345678912", "1.234567891");
  GT("-1.234567891", "-1.2345678912");
  LT("-1.2345678912", "-1.234567891");
  LT("300000000000000000000", "400000000000000000000");
  LT("3000000000000000000000000000000", "4000000000000000000000000000000");
  LT("3000000000000000000000000000000000000000", "4000000000000000000000000000000000000000");

  LT("0", "0.000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001");
  GT("0.000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001", "0");

  LT("0.000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001",
     "0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000011");
  GT("0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000011",
     "0.000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001");
}

TEST(ObNumberParseHelper, precision)
{
  uint32_t digits[number::ObNumber::MAX_STORE_LEN];
  number::ObNumber::Desc desc;
  desc.len_ = 0;
  desc.reserved_ = 0;
  desc.sign_ = number::ObNumber::POSITIVE;
  desc.exp_ = number::ObNumber::EXP_ZERO;
  ParseHelperTester nph;
  nph.number_.assign(desc.desc_, digits);
  int ret = OB_SUCCESS;

  nph.fill("123123456789.1234");
  ret = nph.number_.check_and_round(15, 3);
  EXPECT_EQ(OB_SUCCESS, ret);

  nph.fill("1234123456789.1234");
  ret = nph.number_.check_and_round(15, 3);
  EXPECT_EQ(OB_INTEGER_PRECISION_OVERFLOW, ret);

  nph.fill("0.0000000000001234");
  ret = nph.number_.check_and_round(3, 15);
  EXPECT_EQ(OB_SUCCESS, ret);

  nph.fill("1.0000000000001234");
  ret = nph.number_.check_and_round(3, 15);
  EXPECT_EQ(OB_INTEGER_PRECISION_OVERFLOW, ret);

  nph.fill("0.000000000001234");
  ret = nph.number_.check_and_round(3, 15);
  EXPECT_EQ(OB_DECIMAL_PRECISION_OVERFLOW, ret);

  nph.fill("0.0000000019");
  ret = nph.number_.check_and_round(18, 9);
  EXPECT_EQ(OB_SUCCESS, ret);
}

#define SCALE(res, str, p, s) \
do { \
  nph.fill(str); \
  ret = nph.number_.check_and_round(p, s); \
  EXPECT_EQ(OB_SUCCESS, ret); \
  ASSERT_STREQ(res, format(nph.number_, s)); \
  \
  nph.fill(negate(str).c_str()); \
  ret = nph.number_.check_and_round(p, s); \
  EXPECT_EQ(OB_SUCCESS, ret); \
  ASSERT_STREQ(negate(res).c_str(), format(nph.number_, s)); \
} while (0)

TEST(ObNumberParseHelper, scale)
{
  uint32_t digits[number::ObNumber::MAX_STORE_LEN];
  number::ObNumber::Desc desc;
  desc.len_ = 0;
  desc.reserved_ = 0;
  desc.sign_ = number::ObNumber::POSITIVE;
  desc.exp_ = number::ObNumber::EXP_ZERO;
  ParseHelperTester nph;
  nph.number_.assign(desc.desc_, digits);
  int ret = OB_SUCCESS;

  SCALE("1.123",
        "1.1234", 32, 3);
  SCALE("1.124",
        "1.1235", 32, 3);
  SCALE("1.124",
        "1.1236", 32, 3);

  SCALE("-1.123",
        "-1.1234", 32, 3);
  SCALE("-1.124",
        "-1.1235", 32, 3);
  SCALE("-1.124",
        "-1.1236", 32, 3);

  SCALE("0.123",
        "0.1234", 32, 3);
  SCALE("0.124",
        "0.1235", 32, 3);
  SCALE("0.124",
        "0.1236", 32, 3);

  SCALE("-0.123",
        "-0.1234", 32, 3);
  SCALE("-0.124",
        "-0.1235", 32, 3);
  SCALE("-0.124",
        "-0.1236", 32, 3);

  SCALE("1.000000000123",
        "1.0000000001234", 32, 12);
  SCALE("1.000000000124",
        "1.0000000001235", 32, 12);
  SCALE("1.000000000124",
        "1.0000000001236", 32, 12);

  SCALE("-1.000000000123",
        "-1.0000000001234", 32, 12);
  SCALE("-1.000000000124",
        "-1.0000000001235", 32, 12);
  SCALE("-1.000000000124",
        "-1.0000000001236", 32, 12);

  SCALE("0.000000000123",
        "0.0000000001234", 32, 12);
  SCALE("0.000000000000000000123",
        "0.0000000000000000001234", 32, 21);

  SCALE("0.000000000124",
        "0.0000000001235", 32, 12);
  SCALE("0.000000000124",
        "0.0000000001236", 32, 12);

  SCALE("-0.000000000123",
        "-0.0000000001234", 32, 12);
  SCALE("-0.000000000124",
        "-0.0000000001235", 32, 12);
  SCALE("-0.000000000124",
        "-0.0000000001236", 32, 12);

  SCALE("0.000000000000000123123",
        "0.0000000000000001231234", 32, 21);
  SCALE("0.000000000000000123124",
        "0.0000000000000001231235", 32, 21);
  SCALE("0.000000000000000123124",
        "0.0000000000000001231236", 32, 21);

  SCALE("-0.000000000000000123123",
        "-0.0000000000000001231234", 32, 21);
  SCALE("-0.000000000000000123124",
        "-0.0000000000000001231235", 32, 21);
  SCALE("-0.000000000000000123124",
        "-0.0000000000000001231236", 32, 21);

  SCALE("1.000000001123",
        "1.0000000011234", 32, 12);
  SCALE("1.000000001124",
        "1.0000000011235", 32, 12);
  SCALE("1.000000001124",
        "1.0000000011236", 32, 12);

  SCALE("0.000000001123",
        "0.0000000011234", 32, 12);
  SCALE("0.000000001124",
        "0.0000000011235", 32, 12);
  SCALE("0.000000001124",
        "0.0000000011236", 32, 12);

  SCALE("-0.000000001123",
        "-0.0000000011234", 32, 12);
  SCALE("-0.000000001124",
        "-0.0000000011235", 32, 12);
  SCALE("-0.000000001124",
        "-0.0000000011236", 32, 12);

  SCALE("1.000000009",
        "1.0000000094", 32, 9);
  SCALE("1.00000001",
        "1.0000000095", 32, 8);
  SCALE("1.00000001",
        "1.0000000096", 32, 8);

  SCALE("0.000000009",
        "0.0000000094", 32, 9);
  SCALE("0.00000001",
        "0.0000000095", 32, 8);
  SCALE("0.00000001",
        "0.0000000096", 32, 8);

  SCALE("-0.000000009",
        "-0.0000000094", 32, 9);
  SCALE("-0.00000001",
        "-0.0000000095", 32, 8);
  SCALE("-0.00000001",
        "-0.0000000096", 32, 8);

  SCALE("1.000000000000000009",
        "1.0000000000000000094", 32, 18);
  SCALE("1.00000000000000001",
        "1.0000000000000000095", 32, 17);
  SCALE("1.00000000000000001",
        "1.0000000000000000096", 32, 17);

  SCALE("0.000000000000000009",
        "0.0000000000000000094", 32, 18);
  SCALE("0.00000000000000001",
        "0.0000000000000000095", 32, 17);
  SCALE("0.00000000000000001",
        "0.0000000000000000096", 32, 17);

  SCALE("1",
        "1.000000000000000009", 32, 0);
  SCALE("1",
        "1.00000000000000001",  32, 0);
  SCALE("1",
        "1.00000000000000001",  32, 0);

  SCALE("-1",
        "-1.000000000000000009", 32, 0);
  SCALE("-1",
        "-1.00000000000000001",  32, 0);
  SCALE("-1",
        "-1.00000000000000001",  32, 0);

  SCALE("0",
        "-0.000000000000000009", 32, 0);
  SCALE("0",
        "-0.00000000000000001",  32, 0);
  SCALE("0",
        "-0.00000000000000001",  32, 0);


  SCALE("0",
        "-0.00009", 32, 0);
  SCALE("0",
        "-0.00009",  32, 0);
  SCALE("0",
        "-0.00009",  32, 0);

  SCALE("-1",
        "-0.500000001", 32, 0);
  SCALE("1",
        "0.500000001",  32, 0);
  SCALE("0",
        "-0.499999999",  32, 0);
  SCALE("0",
        "0.499999999",  32, 0);

  SCALE("1.0000000001",
        "1.00000000014234567891", 32, 10);
  SCALE("1.0000000002",
        "1.00000000015234567891", 32, 10);
  SCALE("1.0000000002",
        "1.00000000016234567891", 32, 10);

  SCALE("0.0000000001",
        "0.00000000014234567891", 32, 10);
  SCALE("0.0000000002",
        "0.00000000015234567891", 32, 10);
  SCALE("0.0000000002",
        "0.00000000016234567891", 32, 10);

  SCALE("1.000000001",
        "1.0000000014234567891", 32, 9);
  SCALE("1.000000002",
        "1.0000000015234567891", 32, 9);
  SCALE("1.000000002",
        "1.0000000016234567891", 32, 9);

  SCALE("0.000000001",
        "0.0000000014234567891", 32, 9);
  SCALE("0.000000002",
        "0.0000000015234567891", 32, 9);
  SCALE("0.000000002",
        "0.0000000016234567891", 32, 9);

  SCALE("1.00000001",
        "1.000000014234567891", 32, 8);
  SCALE("1.00000002",
        "1.000000015234567891", 32, 8);
  SCALE("1.00000002",
        "1.000000016234567891", 32, 8);

  SCALE("0.00000001",
        "0.000000014234567891", 32, 8);
  SCALE("0.00000002",
        "0.000000015234567891", 32, 8);
  SCALE("0.00000002",
        "0.000000016234567891", 32, 8);

  SCALE("1200",
        "1245.1234", 32, -2);
  SCALE("1300",
        "1256.1234", 32, -2);
  SCALE("1300",
        "1267.1234", 32, -2);

  SCALE("1200",
        "1245", 32, -2);
  SCALE("1300",
        "1256", 32, -2);
  SCALE("1300",
        "1267", 32, -2);

  SCALE("1234567891200",
        "1234567891245.1234", 32, -2);
  SCALE("1234567891300",
        "1234567891256.1234", 32, -2);
  SCALE("1234567891300",
        "1234567891267.1234", 32, -2);

  SCALE("1234567891200",
        "1234567891245", 32, -2);
  SCALE("1234567891300",
        "1234567891256", 32, -2);
  SCALE("1234567891300",
        "1234567891267", 32, -2);

  SCALE("1000000000",
        "1423456789.1234", 32, -9);
  SCALE("2000000000",
        "1523456789.1234", 32, -9);
  SCALE("2000000000",
        "1623456789.1234", 32, -9);

  SCALE("1000000000",
        "1423456789", 32, -9);
  SCALE("2000000000",
        "1523456789", 32, -9);
  SCALE("2000000000",
        "1623456789", 32, -9);

  SCALE("1000000000000000000",
        "1423456789123456789.1234", 32, -18);
  SCALE("2000000000000000000",
        "1523456789123456789.1234", 32, -18);
  SCALE("2000000000000000000",
        "1623456789123456789.1234", 32, -18);

  SCALE("-1000000000000000000",
        "-1423456789123456789.1234", 32, -18);
  SCALE("-2000000000000000000",
        "-1523456789123456789.1234", 32, -18);
  SCALE("-2000000000000000000",
        "-1623456789123456789.1234", 32, -18);

  SCALE("1000000000000000000",
        "1423456789123456789", 32, -18);
  SCALE("2000000000000000000",
        "1523456789123456789", 32, -18);
  SCALE("2000000000000000000",
        "1623456789123456789", 32, -18);

  SCALE("10000000000",
        "14234567891.1234", 32, -10);
  SCALE("20000000000",
        "15234567891.1234", 32, -10);
  SCALE("20000000000",
        "16234567891.1234", 32, -10);

  SCALE("10000000000",
        "14234567891", 32, -10);
  SCALE("20000000000",
        "15234567891", 32, -10);
  SCALE("20000000000",
        "16234567891", 32, -10);

  SCALE("-10000000000",
        "-14234567891", 32, -10);
  SCALE("-20000000000",
        "-15234567891", 32, -10);
  SCALE("-20000000000",
        "-16234567891", 32, -10);

  SCALE("1",
        "1.4", 32, 0);
  SCALE("2",
        "1.5", 32, 0);
  SCALE("2",
        "1.6", 32, 0);

  SCALE("1234567891",
        "1234567891.4", 32, 0);
  SCALE("1234567892",
        "1234567891.5", 32, 0);
  SCALE("1234567892",
        "1234567891.6", 32, 0);

  SCALE("-1234567891",
        "-1234567891.4", 32, 0);
  SCALE("-1234567892",
        "-1234567891.5", 32, 0);
  SCALE("-1234567892",
        "-1234567891.6", 32, 0);

  SCALE("0",
        "1.4", 32, -1);
  SCALE("0",
        "1.5", 32, -1);
  SCALE("0",
        "1.6", 32, -1);

  SCALE("0",
        "4234567891.4", 32, -10);
  SCALE("10000000000",
        "5234567891.5", 32, -10);
  SCALE("10000000000",
        "6234567891.6", 32, -10);

  SCALE("0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001",
        "0.00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000014", 38, 127);

  SCALE("0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000002",
        "0.00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000015", 38, 127);
  SCALE("0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000002",
        "0.00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000016", 38, 127);

  SCALE("-0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001",
        "-0.00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000014", 38, 127);
  SCALE("-0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000002",
        "-0.00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000015", 38, 127);
  SCALE("-0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000002",
        "-0.00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000016", 38, 127);


  SCALE("0",
        "0.00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000004", 38, 0);
  SCALE("0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001",
        "0.00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000005", 38, 127);
  SCALE("0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001",
        "0.00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000006", 38, 127);

  SCALE("11111111111111111111111111111111111111000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
        "11111111111111111111111111111111111111400000000000000000000000000000000000000000000000000000000000000000000000000000000000.0", 38, -84);
  SCALE("11111111111111111111111111111111111112000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
        "11111111111111111111111111111111111111500000000000000000000000000000000000000000000000000000000000000000000000000000000000.0", 38, -84);
  SCALE("11111111111111111111111111111111111112000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
        "11111111111111111111111111111111111111600000000000000000000000000000000000000000000000000000000000000000000000000000000000.0", 38, -84);

  SCALE("19999",
        "19999.4", 5, 0);
  SCALE("20000",
        "19999.5", 5, 0);
  SCALE("20000",
        "19999.6", 5, 0);

  SCALE("19999999999999",
        "19999999999999.4", 14, 0);
  SCALE("20000000000000",
        "19999999999999.5", 14, 0);
  SCALE("20000000000000",
        "19999999999999.6", 14, 0);

  nph.fill("99999.5");
  ret = nph.number_.check_and_round(5, 0);
  EXPECT_EQ(OB_INTEGER_PRECISION_OVERFLOW, ret);

  nph.fill("99999999999999.5");
  ret = nph.number_.check_and_round(14, 0);
  EXPECT_EQ(OB_INTEGER_PRECISION_OVERFLOW, ret);

  nph.fill("999999999.5");
  ret = nph.number_.check_and_round(9, 0);
  EXPECT_EQ(OB_INTEGER_PRECISION_OVERFLOW, ret);

  nph.fill("0.0000000005");
  ret = nph.number_.check_and_round(0, 9);
  EXPECT_EQ(OB_DECIMAL_PRECISION_OVERFLOW, ret);

  nph.fill("500000000");
  ret = nph.number_.check_and_round(0, -8);
  EXPECT_EQ(OB_INTEGER_PRECISION_OVERFLOW, ret);

  SCALE("1000000000",
        "999999999.5", 10, 0);
  SCALE("1000000000000000000",
        "999999999999999999.5", 19, 0);
  SCALE("1000000000000000000000000000000000000000000000",
        "999999999999999999999999999999999999999999999", 46, -1);
  SCALE("1000000000",
        "500000000", 1, -9);

  SCALE("1",
        "0.9999999995", 10, 0);
  SCALE("1",
        "0.9999999999999999995", 10, 0);
  SCALE("1",
        "0.999999999999999999999999999999999999999999999", 10, 0);
  SCALE("0.000000001",
        "0.000000000999999999999999999999999999999999999", 11, 9);
  SCALE("1",
        "0.5", 10, 0);
  SCALE("1000000000",
        "999999999.5", 10, 0);

  SCALE("0",
        "0", 1, 0);
  //SCALE("1", "1", INT64_MAX, INT64_MAX);
}

TEST(ObNumberParseHelper, vector)
{
  uint32_t digits[number::ObNumber::MAX_STORE_LEN];
  number::ObNumber::Desc desc;
  desc.len_ = 0;
  desc.reserved_ = 0;
  desc.sign_ = number::ObNumber::POSITIVE;
  desc.exp_ = number::ObNumber::EXP_ZERO;
  ParseHelperTester nph;
  nph.number_.assign(desc.desc_, digits);
  int ret = OB_SUCCESS;
  ObCalcVector cv;

  ObCalcVector cv1;
  ret = nph.fill("0.00000000031415");
  desc.desc_ = nph.number_.get_desc_value();
  desc.exp_ = (0x7f) & (desc.exp_ + 3);
  cv1.init(desc.desc_, nph.number_.get_digits());

  ObCalcVector cv2;
  ret = nph.fill("1000000000.31415");
  desc = nph.number_.get_desc();
  desc.exp_ = (0x7f) & (desc.exp_ + 3);
  cv2.init(desc.desc_, nph.number_.get_digits());

  ObCalcVector cvs;
  cvs.ensure(std::max(cv1.size(), cv2.size()) + 1);
  poly_poly_add(cv1, cv2, cvs);
  desc.len_ = (0x7f) & cvs.size();
  desc.exp_ = (0x7f) & (desc.exp_ - 3);
  ret = nph.from(desc.desc_, cvs);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("1000000000.31415000031415", format(nph.number_, 14));

  cv.ensure(6);
  cv.set(0, 999999999);
  cv.set(1, 999999999);
  cv.set(2, 999999999);
  cv.set(3, 999999999);
  cv.set(4, 999999999);
  cv.set(5, 500000000);

  desc.exp_ = number::ObNumber::EXP_ZERO + 4;
  desc.sign_ = number::ObNumber::POSITIVE;
  ret = nph.from(desc.desc_, cv);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("1000000000000000000000000000000000000000000000", format(nph.number_, 0));

  desc.exp_ = number::ObNumber::EXP_ZERO + 3;
  desc.sign_ = number::ObNumber::POSITIVE;
  ret = nph.from(desc.desc_, cv);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("1000000000000000000000000000000000000", format(nph.number_, 0));

  desc.exp_ = number::ObNumber::EXP_ZERO;
  desc.sign_ = number::ObNumber::NEGATIVE;
  ret = nph.from(desc.desc_, cv);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("-1000000000", format(nph.number_, 0));

  desc.exp_ = number::ObNumber::EXP_ZERO - 1;
  desc.sign_ = number::ObNumber::NEGATIVE;
  ret = nph.from(desc.desc_, cv);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("-1000000000000000000", format(nph.number_, 0));

  cv.ensure(6);
  cv.set(0, 999999999);
  cv.set(1, 999999999);
  cv.set(2, 999999999);
  cv.set(3, 999999999);
  cv.set(4, 999999999);
  cv.set(5, 400000000);

  desc.exp_ = number::ObNumber::EXP_ZERO + 4;
  desc.sign_ = number::ObNumber::POSITIVE;
  ret = nph.from(desc.desc_, cv);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("999999999999999999999999999999999999999999999", format(nph.number_, 0));

  desc.exp_ = number::ObNumber::EXP_ZERO + 3;
  desc.sign_ = number::ObNumber::POSITIVE;
  ret = nph.from(desc.desc_, cv);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("999999999999999999999999999999999999.999999999", format(nph.number_, 9));

  desc.exp_ = number::ObNumber::EXP_ZERO;
  desc.sign_ = number::ObNumber::NEGATIVE;
  ret = nph.from(desc.desc_, cv);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("-999999999.999999999999999999999999999999999999", format(nph.number_, 36));

  desc.exp_ = number::ObNumber::EXP_ZERO - 1;
  desc.sign_ = number::ObNumber::NEGATIVE;
  ret = nph.from(desc.desc_, cv);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_STREQ("-999999999999999999.999999999999999999999999999", format(nph.number_, 27));
}

#define ADD(n1, n2, res, scale) \
do { \
  int ret = OB_SUCCESS; \
  CharArena allocator; \
  LimitedAllocator la; \
  number::ObNumber augend; \
  number::ObNumber addend; \
  number::ObNumber sum; \
  number::ObNumber result; \
  \
  ret = augend.from(n1, allocator); \
  EXPECT_EQ(OB_SUCCESS, ret); \
  ret = addend.from(n2, allocator); \
  EXPECT_EQ(OB_SUCCESS, ret); \
  ret = result.from(res, allocator); \
  EXPECT_EQ(OB_SUCCESS, ret); \
  ret = augend.add_v3(addend, sum, la); \
  EXPECT_EQ(OB_SUCCESS, ret); \
  EXPECT_STREQ(res, format(sum, scale)); \
  if (scale > 0) {\
    EXPECT_EQ(0, result.compare_v1(sum)); \
  }\
  \
  ret = augend.from(negate(n1).c_str(), allocator); \
  EXPECT_EQ(OB_SUCCESS, ret); \
  ret = addend.from(negate(n2).c_str(), allocator); \
  EXPECT_EQ(OB_SUCCESS, ret); \
  ret = result.from(negate(res).c_str(), allocator); \
  EXPECT_EQ(OB_SUCCESS, ret); \
  ret = augend.add_v3(addend, sum, la); \
  EXPECT_EQ(OB_SUCCESS, ret); \
  EXPECT_STREQ(negate(res).c_str(), format(sum, scale)); \
  if (scale > 0) {\
    EXPECT_EQ(0, result.compare_v1(sum)); \
  }\
} while (0)

TEST(ObNumberCalc, add)
{
  ADD("1.1", "1.2", "2.3", 1);
  ADD("9.9", "9.9", "19.8", 1);
  ADD("9999999999.9999999999", "9999999999.9999999999", "19999999999.9999999998", 10);
  ADD("1", "0.0000000002", "1.0000000002", 10);
  ADD("1", "0.9999999999999999999999999999999999994", "1.9999999999999999999999999999999999994", 37);
  ADD("1", "0.9999999999999999999999999999999999995", "1.9999999999999999999999999999999999995", 37);
  ADD("1", "0.99999999999999999999999999999999999955555555", "1.99999999999999999999999999999999999955555555", 44);
  ADD("1", "0.999999999999999999999999999999999999999999995", "2", 0);
  ADD("1", "0", "1", 0);
  ADD("0", "1", "1", 0);
  ADD("999999999", "1", "1000000000", 0);
  ADD("999999999", "999999999", "1999999998", 0);

  ADD("999999999999999999999999999999999999999999999000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
      "0.000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001",
      "999999999999999999999999999999999999999999999000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000", 0);

  ADD("999999999999999999999999999999999999999999999000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
      "0.000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000999999999999999999999999999999999999999999999",
      "999999999999999999999999999999999999999999999000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000", 0);
  ADD("0.0000000000000000001234",
      "0.0000000001234",
      "0.0000000001234000001234",
      22);
  ADD("0.000000000000000000000000000000000000000000001", "0.999999999999999999999999999999999999999999999", "1", 0);

  ADD("-1.1", "1.2", "0.1", 1);
  ADD("-1", "0.0000000002", "-0.9999999998", 10);
  ADD("-1", "0.9999999999999999999999999999999999994", "-0.0000000000000000000000000000000000006", 37);
  ADD("-1", "0.999999999999999999999999999999999999999999999", "-0.000000000000000000000000000000000000000000001", 45);
  ADD("-1", "0.0000000000000000000000000000000000000000000001", "-1", 0);
  ADD("-1", "0", "-1", 0);
  ADD("0", "-1", "-1", 0);
  ADD("-999999999999999999999999999999999999999999999000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
      "0.000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001",
      "-999999999999999999999999999999999999999999999000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000", 0);

  ADD("-999999999999999999999999999999999999999999999000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
      "0.000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000999999999999999999999999999999999999999999999",
      "-999999999999999999999999999999999999999999999000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000", 0);
  ADD("-0.0000000000000000001234",
      "0.0000000001234",
      "0.0000000001233999998766", 22);
  ADD("-0.000000000000000000000000000000000000000000001", "0.999999999999999999999999999999999999999999999", "0.999999999999999999999999999999999999999999998", 45);

  ADD("999999999999999995", "5", "1000000000000000000", 0);
  ADD("0.999999999999999995", "0.000000000000000005", "1", 0);

  ADD("99999999999999999999999999999999999999999999999999999999999999999999.9999",
      "7.99365542543574543435457546878765465768765465476876465746876576576576577",
      "100000000000000000000000000000000000000000000000000000000000000000008", 0);
  ADD("99999999999999999999999999999999999999999999999999999999999999999999.9999",
      "-1",
      "99999999999999999999999999999999999999999999999999999999999999999999", 0);

  ADD("0.9902819038736083899663929849555723217376",
      "0.000000000000000000000000000000000000000000000000000000000025578569366733499189294448246024165831350",
      "1", 0);


  CharArena allocator;
  LimitedAllocator la;
  number::ObNumber augend;
  number::ObNumber addend;
  number::ObNumber sum;
  int ret = augend.from("500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000", allocator);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = addend.from("500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000", allocator);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = augend.add_v3(addend, sum, la);
  EXPECT_EQ(OB_INTEGER_PRECISION_OVERFLOW, ret);
}

#define NEGATE(v, scale) \
do { \
  int ret = OB_SUCCESS; \
  CharArena allocator; \
  number::ObNumber orig; \
  number::ObNumber res; \
  ret = orig.from(v, allocator); \
  EXPECT_EQ(OB_SUCCESS, ret); \
  \
  LimitedAllocator la1; \
  ret = orig.negate(res, la1); \
  EXPECT_EQ(OB_SUCCESS, ret); \
  EXPECT_STREQ(negate(v).c_str(), format(res, scale)); \
  \
  LimitedAllocator la2; \
  ret = res.negate(res, la2); \
  EXPECT_EQ(OB_SUCCESS, ret); \
  EXPECT_STREQ(format(orig, scale), format(res, scale)); \
} while (0)

TEST(ObNumberCalc, negate)
{
  NEGATE("0", 0);
  NEGATE("1", 0);
  NEGATE("999999999999999999999999999999999999999999999", 0);
  NEGATE("0.999999999999999999999999999999999999999999999", 45);
  NEGATE("999999999.999999999999999999999999999999999999", 36);
}

#define SUB(n1, n2, res, scale) \
do { \
  int ret = OB_SUCCESS; \
  CharArena allocator; \
  LimitedAllocator la; \
  number::ObNumber minuend; \
  number::ObNumber subtrahend; \
  number::ObNumber remainder; \
  \
  ret = minuend.from(n1, allocator); \
  EXPECT_EQ(OB_SUCCESS, ret); \
  ret = subtrahend.from(n2, allocator); \
  EXPECT_EQ(OB_SUCCESS, ret); \
  ret = minuend.sub_v3(subtrahend, remainder, la); \
  EXPECT_EQ(OB_SUCCESS, ret); \
  EXPECT_STREQ(res, format(remainder, scale)); \
  \
  ret = minuend.from(negate(n1).c_str(), allocator); \
  EXPECT_EQ(OB_SUCCESS, ret); \
  ret = subtrahend.from(negate(n2).c_str(), allocator); \
  EXPECT_EQ(OB_SUCCESS, ret); \
  ret = minuend.sub_v3(subtrahend, remainder, la); \
  EXPECT_EQ(OB_SUCCESS, ret); \
  EXPECT_STREQ(negate(res).c_str(), format(remainder, scale)); \
} while (0)

TEST(ObNumberCalc, sub)
{
  SUB("1",
      "0.999999999999999999999999999999999999940000000000000000000000000000000000000900000",
      "0.000000000000000000000000000000000000060000000", 45);

  SUB("1", "-1", "2", 0);
  SUB("1", "-1351298048", "1351298049", 0);
  SUB("3", "2", "1", 0);
  SUB("2", "3", "-1", 0);
  SUB("3", "0", "3", 0);
  SUB("0", "3", "-3", 0);
  SUB("3", "3", "0", 0);
  SUB("1", "0.999999999999999999999999999999999999999999999", "0.000000000000000000000000000000000000000000001", 45);
  SUB("0.999999999999999999999999999999999999999999999", "1", "-0.000000000000000000000000000000000000000000001", 45);
  SUB("999999999999999999999999999999999999999999999000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
      "0.000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001",
      "999999999999999999999999999999999999999999999000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000", 0);
  SUB("999999999999999999999999999999999999999999999000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
      "0.000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000999999999999999999999999999999999999999999999",
      "999999999999999999999999999999999999999999999000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000", 0);

  SUB("-3", "2", "-5", 0);
  SUB("-2", "3", "-5", 0);
  SUB("-3", "0", "-3", 0);
  SUB("0", "-3", "3", 0);
  SUB("-3", "3", "-6", 0);
  SUB("-1", "0.999999999999999999999999999999999999999999999", "-2", 0);
  SUB("-0.999999999999999999999999999999999999999999999", "1", "-2", 0);
  SUB("-999999999999999999999999999999999999999999999000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
      "0.000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001",
      "-999999999999999999999999999999999999999999999000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000", 0);
  SUB("-999999999999999999999999999999999999999999999000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
      "0.000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000999999999999999999999999999999999999999999999",
      "-999999999999999999999999999999999999999999999000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000", 0);

  SUB("1000000000000000005", "5", "1000000000000000000", 0);
  SUB("1.000000000000000005", "0.000000000000000005", "1", 0);
  SUB("99999999999999999999999999999999999999999999999999999999999999999999.9999",
      "7.99365542543574543435457546878765465768765465476876465746876576576576577",
      "99999999999999999999999999999999999999999999999999999999999999999992", 0);
  SUB("99999999999999999999999999999999999999999999999999999999999999999999.9999",
      "1",
      "99999999999999999999999999999999999999999999999999999999999999999999", 0);

}

#define MUL(n1, n2, res, scale) \
do { \
  int ret = OB_SUCCESS; \
  CharArena allocator; \
  LimitedAllocator la; \
  number::ObNumber multiplicand; \
  number::ObNumber multiplier; \
  number::ObNumber product; \
  \
  ret = multiplicand.from(n1, allocator); \
  EXPECT_EQ(OB_SUCCESS, ret); \
  ret = multiplier.from(n2, allocator); \
  EXPECT_EQ(OB_SUCCESS, ret); \
  ret = multiplicand.mul_v3(multiplier, product, la); \
  EXPECT_EQ(OB_SUCCESS, ret); \
  EXPECT_STREQ(res, format(product, scale)); \
  \
  ret = multiplicand.from(negate(n1).c_str(), allocator); \
  EXPECT_EQ(OB_SUCCESS, ret); \
  ret = multiplier.from(negate(n2).c_str(), allocator); \
  EXPECT_EQ(OB_SUCCESS, ret); \
  ret = multiplicand.mul_v3(multiplier, product, la); \
  EXPECT_EQ(OB_SUCCESS, ret); \
  EXPECT_STREQ(res, format(product, scale)); \
} while (0)

TEST(ObNumberCalc, mul)
{
  MUL("2", "3", "6", 0);
  MUL("2", "500000000", "1000000000", 0);
  MUL("999999999999999999999999999999999999999999999000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
      "0.000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001",
      "0.999999999999999999999999999999999999999999999", 45);
  MUL("0.999999999999999999999999999999999999999999999",
      "2",
      "2", 0);
  MUL("0.999999999999999999999999999999999999999999999",
      "0.000000001",
      "0.000000000999999999999999999999999999999999999999999999", 54);

  MUL("-2", "3", "-6", 0);
  MUL("-2", "500000000", "-1000000000", 0);
  MUL("-999999999999999999999999999999999999999999999000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
      "0.000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001",
      "-0.999999999999999999999999999999999999999999999", 45);
  MUL("-0.999999999999999999999999999999999999999999999",
      "2",
      "-2", 0);
  MUL("-0.999999999999999999999999999999999999999999999",
      "0.000000001",
      "-0.000000000999999999999999999999999999999999999999999999", 54);

  MUL("99999999999999999999999999999999999999999999999999999999999999999999.9999",
      "1.99365542543574543435457546878765465768765465476876465746876576576576577",
      "199365542543574543435457546878765465768765465476876465746876576576576.577000000000000000000000000000000000000000000000000000",
      54);


  CharArena allocator;
  number::ObNumber multiplicand;
  number::ObNumber multiplier;
  number::ObNumber product;
  int ret = multiplicand.from("500000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000", allocator);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = multiplier.from("2", allocator);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = multiplicand.mul(multiplier, product, allocator);
  EXPECT_EQ(OB_INTEGER_PRECISION_OVERFLOW, ret);
}

#define DIV(n1, n2, res, scale) \
do { \
  int ret = OB_SUCCESS; \
  CharArena allocator; \
  LimitedAllocator la; \
  number::ObNumber dividend; \
  number::ObNumber divisor; \
  number::ObNumber quotient; \
  \
  ret = dividend.from(n1, allocator); \
  EXPECT_EQ(OB_SUCCESS, ret); \
  ret = divisor.from(n2, allocator); \
  EXPECT_EQ(OB_SUCCESS, ret); \
  ret = dividend.div_v3(divisor, quotient, la); \
  EXPECT_EQ(OB_SUCCESS, ret); \
  ASSERT_STREQ(res, format(quotient, scale)); \
  \
  ret = dividend.from(negate(n1).c_str(), allocator); \
  EXPECT_EQ(OB_SUCCESS, ret); \
  ret = divisor.from(negate(n2).c_str(), allocator); \
  EXPECT_EQ(OB_SUCCESS, ret); \
  ret = dividend.div_v3(divisor, quotient, la); \
  EXPECT_EQ(OB_SUCCESS, ret); \
  ASSERT_STREQ(res, format(quotient, scale)); \
} while (0)

TEST(ObNumberCalc, div)
{
  DIV("1", "3", "0.333333333333333333333333333333333333333333333", 45);
  DIV("1000000000", "3", "333333333.333333333333333333333333333333333333", 36);
  DIV("1000000000000000000", "3", "333333333333333333.333333333333333333333333333", 27);
  DIV("1000000000000000000", "3000000000", "333333333.333333333333333333333333333333333333", 36);
  DIV("1", "3000000000", "0.000000000333333333333333333333333333333333333333333333", 54);
  DIV("1", "3000000000000000000", "0.000000000000000000333333333333333333333333333333333333333333333", 63);
   DIV("1", "3000000000000000000000000000", "0.000000000000000000000000000333333333333333333333333333333333333333333333", 72);

   //代码中限制补零的个数为60为上限，否则报4002错误，所以下面的执行format后，返回错误码为4002
  //DIV("1",
  //    "300000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
  //    "0.000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000003333333333333333333333333333333333333", 171);

  DIV("1", "0.000000002", "500000000", 0);
  DIV("1", "0.000000000000000002", "500000000000000000", 0);
  DIV("0.1", "0.000000000000000002", "50000000000000000", 0);
  DIV("0.0000000001", "0.000000000000000002", "50000000", 0);

  DIV("1", "0.000000003", "333333333.333333333333333333333333333333333333", 36);
  DIV("1", "0.000000000000000003", "333333333333333333.333333333333333333333333333", 27);
  DIV("0.1", "0.000000000000000003", "33333333333333333.3333333333333333333333333333", 28);
  DIV("0.0000000001", "0.000000000000000003", "33333333.3333333333333333333333333333333333333", 37);

  DIV("1", "7", "0.142857142857142857142857142857142857142857143", 45);
  DIV("-1", "7", "-0.142857142857142857142857142857142857142857143", 45);
  DIV("1", "-7", "-0.142857142857142857142857142857142857142857143", 45);

  DIV("111111111.222222222333333333444444444555555555", "333333333.444444444555555555", "0.333333333555555555703703704469135802868312755", 45);
  DIV("111111111222222222.333333333444444444555555555", "333333333.444444444555555555", "333333333.555555555703703704469135802868312755", 36);
  DIV("0.6", "2.166190378969060094174830575509116615300000000", "0.276983964948433490291384292515194359", 36);

  DIV("99999999999999999999999999999999999999999999999999999999999999999999.9999",
      "1.99365542543574543435457546878765465768765465476876465746876576576576577",
      "50159119135716942081585699790616486181246465997952174780397060972446.202400000000000000000000000000000000000000000", 45);

  DIV("1089999999", "2044381910", "0.533168481714847496376056272186442894126371916", 45);
}

#define POWER(n1, n2, res, scale)                 \
do { \
  int ret = OB_SUCCESS; \
  CharArena allocator; \
  LimitedAllocator la; \
  number::ObNumber base; \
  number::ObNumber exponent; \
  number::ObNumber value; \
  \
  ret = base.from(n1, allocator); \
  EXPECT_EQ(OB_SUCCESS, ret); \
  ret = exponent.from(n2, allocator); \
  EXPECT_EQ(OB_SUCCESS, ret); \
  ret = base.power(exponent, value, la); \
  EXPECT_EQ(OB_SUCCESS, ret); \
  ASSERT_STREQ(res, format(value, scale)); \
} while (0)

TEST(ObNumberCalc, power)
{
  POWER("2", "10", "1024", 0);
  POWER("2", "0.5", "1.414213562373095048801688724209698079", 36);
  POWER("2", "0.5", "1.414213562373095048801688724209698078569671875", 45);
  POWER("10", "-100", "0.0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001", 100);

  POWER("0.98", "50", "0.364169680087117065217373981453183629371404595", 45);
  POWER("0.98", "60", "0.297553142692120732008661162497860252541618135", 45);
  POWER("0.98", "70", "0.243122581497661847070621529790268289060887739", 45);
}

#define EXP(arg, res, scale)                    \
do { \
  int ret = OB_SUCCESS; \
  CharArena allocator; \
  LimitedAllocator la; \
  number::ObNumber exponent; \
  number::ObNumber value; \
  number::ObNumber power_with_e_value; \
  number::ObNumber ln_value; \
  number::ObNumber e; \
  ret = e.from("2.7182818284590452353602874713526624977572470936999595749669", allocator); \
  EXPECT_EQ(OB_SUCCESS, ret); \
  \
  ret = exponent.from(arg, allocator); \
  EXPECT_EQ(OB_SUCCESS, ret); \
  ret = exponent.e_power(value, la); \
  EXPECT_EQ(OB_SUCCESS, ret); \
  ASSERT_STREQ(res, format(value, scale)); \
  ret = e.power(exponent, power_with_e_value, la);       \
  EXPECT_EQ(OB_SUCCESS, ret); \
  ASSERT_STREQ(res, format(power_with_e_value, scale)); \
  ret = value.ln(ln_value, la); \
  EXPECT_EQ(OB_SUCCESS, ret); \
  ASSERT_STREQ(format(ln_value, scale), format(exponent, scale)); \
} while (0)

TEST(ObNumberCalc, exp)
{
  EXP("1", "2.7182818285", 10);
  EXP("5", "148.4131591026", 10);
  EXP("1", "2.71828182845904523536028747135266249775724709", 44);
  EXP("5", "148.413159102576603421115580040552279623487668", 42);

  EXP("-1", "0.367879441171442321595523770161460867445811131", 45);
  EXP("-5", "0.00673794699908546709663604842314842424884958503", 47);

  EXP("100", "26881171418161354484126255515800135873611118.8", 1);
  EXP("-30", "0.00000000000009357622968840174604915832223378706744958", 53);
}

#define LN(arg, res, scale)                    \
do { \
  int ret = OB_SUCCESS; \
  CharArena allocator; \
  LimitedAllocator la; \
  number::ObNumber arg_num; \
  number::ObNumber value; \
  number::ObNumber exp_value; \
  \
  ret = arg_num.from(arg, allocator); \
  EXPECT_EQ(OB_SUCCESS, ret); \
  ret = arg_num.ln(value, la); \
  EXPECT_EQ(OB_SUCCESS, ret); \
  ASSERT_STREQ(res, format(value, scale)); \
  ret = value.e_power(exp_value, la); \
  EXPECT_EQ(OB_SUCCESS, ret); \
  ASSERT_STREQ(format(exp_value, scale), format(arg_num, scale)); \
} while (0)

TEST(ObNumberCalc, ln)
{
  LN("0.52", "-0.653926467406664013148031224887056674245934764", 45);
  LN("1.00000001", "0.00000000999999995000000033333333083333335333333316667", 53);
  LN("1", "0.00000000000000000000000000000000000000000000", 44);
  LN("5", "1.60943791243410037460075933322618763952560135", 44);
  LN("5.5", "1.70474809223842523464471145650695273174620672", 44);

  // exp^ln_value get 9999999999999999999999999999999999999999.99999999999999999999999999786061
  // LN("10000000000000000000000000000000000000000", "92.1034037197618273607196581873745683040440595", 43);

  // exp^ln_value get "999999999999999999999999999999.9999999999999999999999999999999999999247660"
  // LN("1000000000000000000000000000000", "69.0775527898213705205397436405309262280330447", 43);

  LN("100000000000000000000", "46.0517018598809136803598290936872841520220298", 43);
}

#define LOG(n1, n2, res, scale)               \
do { \
  int ret = OB_SUCCESS; \
  CharArena allocator; \
  LimitedAllocator la; \
  number::ObNumber base; \
  number::ObNumber x; \
  number::ObNumber value; \
  number::ObNumber power_base_value; \
  \
  ret = base.from(n1, allocator); \
  EXPECT_EQ(OB_SUCCESS, ret); \
  ret = x.from(n2, allocator); \
  EXPECT_EQ(OB_SUCCESS, ret); \
  ret = x.log(base, value, la); \
  EXPECT_EQ(OB_SUCCESS, ret); \
  ASSERT_STREQ(res, format(value, scale)); \
  ret = base.power(value, power_base_value, la);       \
  EXPECT_EQ(OB_SUCCESS, ret); \
  ASSERT_STREQ(format(power_base_value, scale), format(x, scale)); \
} while (0)

TEST(ObNumberCalc, log)
{
  LOG("2", "1024", "10", 0);
  LOG("10", "100000", "5", 0);
  LOG("7", "0.01", "-2.366589324909876653635857123293718296330889046", 45);
  LOG("0.2", "100", "-2.861353116146786101340213137527931264139583864", 45);
  LOG("0.2", "0.0001", "5.722706232293572202680426275055862528279167728", 45);
}

#define SQRT(arg, res, scale)                     \
do { \
  int ret = OB_SUCCESS; \
  CharArena allocator; \
  LimitedAllocator la; \
  number::ObNumber arg_num; \
  number::ObNumber value; \
  number::ObNumber power_point_five_value; \
  number::ObNumber point_five; \
  ret = point_five.from("0.5", allocator);            \
  EXPECT_EQ(OB_SUCCESS, ret); \
  \
  ret = arg_num.from(arg, allocator); \
  EXPECT_EQ(OB_SUCCESS, ret); \
  ret = arg_num.sqrt(value, la); \
  EXPECT_EQ(OB_SUCCESS, ret); \
  ASSERT_STREQ(res, format(value, scale)); \
  ret = arg_num.power(point_five, power_point_five_value, la);       \
  EXPECT_EQ(OB_SUCCESS, ret); \
  ASSERT_STREQ(format(power_point_five_value, scale), format(value, scale)); \
} while (0)

TEST(ObNumberCalc, sqrt)
{
  SQRT("0.001", "0.0316227766016837933199889354443271853371955514", 46);
  SQRT("0.233332333323333", "0.483044856429848552010281859139251460607774146", 45);
  SQRT("2", "1.41421356237309504880168872420969807856967188", 44);
  SQRT("5", "2.23606797749978969640917366873127623544061836", 44);
  SQRT("23332333.23332333", "4830.35539410127979042754285628563348297762838", 41);
  SQRT("1000000000000001", "31622776.6016838091313772362862198924845880631", 37);
}

#define REM(n1, n2, res, scale) \
do { \
  int ret = OB_SUCCESS; \
  CharArena allocator; \
  number::ObNumber dividend; \
  number::ObNumber divisor; \
  number::ObNumber remainder; \
  \
  ret = dividend.from(n1, allocator); \
  EXPECT_EQ(OB_SUCCESS, ret); \
  ret = divisor.from(n2, allocator); \
  EXPECT_EQ(OB_SUCCESS, ret); \
  ret = dividend.rem(divisor, remainder, allocator); \
  EXPECT_EQ(OB_SUCCESS, ret); \
  EXPECT_STREQ(res, format(remainder, scale)); \
  \
  ret = dividend.rem(divisor.negate(), remainder, allocator); \
  EXPECT_EQ(OB_SUCCESS, ret); \
  EXPECT_STREQ(res, format(remainder, scale)); \
  \
  ret = dividend.negate().rem(divisor, remainder, allocator); \
  EXPECT_EQ(OB_SUCCESS, ret); \
  EXPECT_STREQ(res, format(remainder.negate(), scale)); \
  \
  ret = dividend.negate().rem(divisor.negate(), remainder, allocator); \
  EXPECT_EQ(OB_SUCCESS, ret); \
  EXPECT_STREQ(res, format(remainder.negate(), scale)); \
} while (0)

TEST(ObNumberCalc, rem)
{
  REM("1000000000", "3", "1", 0);
  REM("1000000000000000000", "3", "1", 0);
  REM("1000000000000000000", "3000000000", "1000000000", 0);
  REM("4000000000", "3000000000", "1000000000", 0);
  REM("4000000000000000000", "3000000000000000000", "1000000000000000000", 0);
  REM("4000000000000000000000000000",
      "3000000000000000000000000000",
      "1000000000000000000000000000", 0);
  REM("4000000000000000000000000000000",
      "3000000000000000000000000000000",
      "1000000000000000000000000000000", 0);
  REM("4000000000000000000000000000000000",
      "3000000000000000000000000000000000",
      "1000000000000000000000000000000000", 0);
  REM("400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
      "300000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
      "100000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000", 0);

  REM("1.000000001", "0.000000002", "0.000000001", 9);
  REM("1.000000000000000001", "0.000000000000000002", "0.000000000000000001", 18);
  REM("1.000000000000000000001", "0.000000000000000000002", "0.000000000000000000001", 21);
  REM("1.00000000000000000000000000000000000000000001",
      "0.00000000000000000000000000000000000000000002",
      "0.00000000000000000000000000000000000000000001", 44);

  REM("1", "0.000000003", "0.000000001", 9);
  REM("1", "0.000000000000000003", "0.000000000000000001", 18);
  REM("0.1", "0.000000000000000003", "0.000000000000000001", 18);
  REM("0.0000000001", "0.000000000000000003", "0.000000000000000001", 18);

  REM("1", "7", "1", 0);
  REM("-1", "7", "-1", 0);
  REM("1", "-7", "1", 0);
}


#define INIT_NUMBER12 \
  ObNumber num1;\
  ObNumber num2;\
  const uint32_t test_data = data[k][j];\
  for (int64_t m = 0; m < i; ++m) {\
    digits1[m] = data[k][j] == 0 ? 2 : data[k][j];\
    digits2[m] = data[k][j] <= 1 ? 1 : data[k][j] - 1;\
  }\
  ObNumberDesc desc;\
  desc.len_ = i;\
  desc.exp_ = (ObNumber::EXP_ZERO) & 0x7f;\
  desc.sign_ = ObNumber::POSITIVE;\
  num1.assign(desc.desc_, digits1);\
  num2.assign(desc.desc_, digits2);\
  tmp +=(num1.is_zero() + num2.is_zero());

TEST(ObNumber, format_number_format_cmp)
{
  const int64_t MAX_TEST_COUNT  = 10000;
  const int64_t MAX_BUF_SIZE = 256;
  char buf_v1[MAX_BUF_SIZE];
  char buf_v2[MAX_BUF_SIZE];
  int64_t pos_v1 = 0;
  int64_t pos_v2 = 0;

  char buf_alloc[MAX_BUF_SIZE];
  int64_t pos = 0;
  int64_t get_range_beg = 0;
  int64_t get_range_cost = 0;
  int64_t begin_value = 0;
  int64_t end_value   = 0;
  ObDataBuffer allocator(buf_alloc, MAX_BUF_SIZE);


  number::ObNumber tmp_number;
  int64_t tmp_value = 0;
  tmp_number.from(tmp_value, allocator);
  EXPECT_EQ(OB_INVALID_ARGUMENT, tmp_number.format_v2(buf_v1, -1, pos_v1, -1));
  for (int64_t j = 0; j < 3; ++j) {
    EXPECT_EQ(OB_SIZE_OVERFLOW, tmp_number.format_v2(buf_v1, j, pos_v1, -1));
  }
  EXPECT_EQ(OB_SUCCESS, tmp_number.format_v2(buf_v1, 3, pos_v1, -1));
  EXPECT_EQ(pos_v1, 1);
  EXPECT_EQ(0, strcmp(buf_v1, "0"));

  allocator.free();
  pos_v1 = 0;
  tmp_value = -1;
  tmp_number.from(tmp_value, allocator);
  EXPECT_EQ(OB_INVALID_ARGUMENT, tmp_number.format_v2(buf_v1, -1, pos_v1, -1));
  for (int64_t j = 0; j < 12; ++j) {
    EXPECT_EQ(OB_SIZE_OVERFLOW, tmp_number.format_v2(buf_v1, j, pos_v1, -1));
  }
  EXPECT_EQ(OB_SUCCESS, tmp_number.format_v2(buf_v1, 12, pos_v1, -1));
  EXPECT_EQ(pos_v1, 2);
  EXPECT_EQ(0, strcmp(buf_v1, "-1"));

  allocator.free();
  pos_v1 = 0;
  tmp_value = -1000000000;
  tmp_number.from(tmp_value, allocator);
  EXPECT_EQ(OB_INVALID_ARGUMENT, tmp_number.format_v2(buf_v1, -1, pos_v1, -1));
  for (int64_t j = 0; j < 21; ++j) {
    EXPECT_EQ(OB_SIZE_OVERFLOW, tmp_number.format_v2(buf_v1, j, pos_v1, -1));
  }
  EXPECT_EQ(OB_SUCCESS, tmp_number.format_v2(buf_v1, 21, pos_v1, -1));
  EXPECT_EQ(pos_v1, 11);
  EXPECT_EQ(0, strcmp(buf_v1, "-1000000000"));

  allocator.free();
  pos_v1 = 0;
  tmp_number.from("-123.456", allocator);
  EXPECT_EQ(OB_INVALID_ARGUMENT, tmp_number.format_v2(buf_v1, -1, pos_v1, -1));
  for (int64_t j = 0; j < 21; ++j) {
    EXPECT_EQ(OB_SIZE_OVERFLOW, tmp_number.format_v2(buf_v1, j, pos_v1, -1));
  }
  EXPECT_EQ(OB_SUCCESS, tmp_number.format_v2(buf_v1, 21, pos_v1, -1));
  EXPECT_EQ(pos_v1, 8);
  EXPECT_EQ(0, strcmp(buf_v1, "-123.456"));

  allocator.free();
  pos_v1 = 0;


  for (int64_t j = 0; j < 20; ++j) {
    begin_value = pow(10, j);
    end_value = pow(10, j + 1) - 1;
    _OB_LOG(INFO, "test numer %ld, [%ld, %ld]", j + 1, begin_value, end_value);

    number::ObNumber num1;
    int64_t add_step = (end_value - begin_value) / MAX_TEST_COUNT;
    if (add_step < 17) {
      add_step = 17;
    }
    for (int64_t i = begin_value, k = 0; k < MAX_TEST_COUNT && i < end_value - begin_value; i += add_step, ++k) {
      num1.from(i * (1 == i%2 ? 1 : -1), allocator);
      pos_v1 = 0;
      pos_v2 = 0;
      EXPECT_EQ(OB_SUCCESS, num1.format_v1(buf_v1, MAX_BUF_SIZE, pos_v1, -1));
      EXPECT_EQ(OB_SUCCESS, num1.format_v2(buf_v2, MAX_BUF_SIZE, pos_v2, -1));

      _OB_LOG(INFO, "debug jianhua, j=%ld, i=%ld, k=%ld, l1=%ld, l2=%ld, v1=%.*s, v2=%.*s", j ,i, k, pos_v1, pos_v2, static_cast<int>(pos_v1), buf_v1, static_cast<int>(pos_v2), buf_v2);
      EXPECT_EQ(pos_v1, pos_v2);
      EXPECT_EQ(0, memcmp(buf_v1, buf_v2, pos_v1));
      allocator.free();

      if (i > end_value) {
        i -= (end_value - begin_value);
      }
    }
  }

  for (int64_t j = 0; j < 20; ++j) {
    begin_value = pow(10, j);
    end_value = pow(10, j + 1) - 1;
    _OB_LOG(INFO, "test numer %ld, [%ld, %ld]", j + 1, begin_value, end_value);

    number::ObNumber num1;
    int64_t add_step = (end_value - begin_value) / MAX_TEST_COUNT;
    if (add_step < 17) {
      add_step = 17;
    }
    for (int64_t i = begin_value, k = 0; k < MAX_TEST_COUNT && i < end_value - begin_value; i += add_step, ++k) {
      for (int64_t m = 0; m < j; ++m) {
        ObFastFormatInt ffi(i * (1 == i%2 ? 1 : -1));
        *const_cast<char *>(ffi.ptr() + m) = '.';
        SET_OB_LOG_TRACE_MODE();    // prevent printing log
        num1.from_v1(ffi.ptr(), ffi.length(), allocator);
        CANCLE_OB_LOG_TRACE_MODE();    // prevent printing log

        pos_v1 = 0;
        pos_v2 = 0;
        EXPECT_EQ(OB_SUCCESS, num1.format_v1(buf_v1, MAX_BUF_SIZE, pos_v1, -1));
        EXPECT_EQ(OB_SUCCESS, num1.format_v2(buf_v2, MAX_BUF_SIZE, pos_v2, -1));

//        _OB_LOG(INFO, "debug jianhua decimal, j=%ld, i=%ld, k=%ld, ffi=%.*s, l1=%ld, l2=%ld,  v1=%.*s, v2=%.*s", j, i, k, ffi.length(), ffi.ptr(), pos_v1, pos_v2, pos_v1, buf_v1, pos_v2, buf_v2);
        EXPECT_EQ(pos_v1, pos_v2);
        EXPECT_EQ(0, memcmp(buf_v1, buf_v2, pos_v1));
        allocator.free();
      }

      char tmp_buf[MAX_BUF_SIZE];
      snprintf(tmp_buf, MAX_BUF_SIZE, "%.17lf", (i * (1 == i%2 ? 1 : -1)) * pow(10, 20));
      SET_OB_LOG_TRACE_MODE();    // prevent printing log
      num1.from_v1(tmp_buf, strlen(tmp_buf), allocator);
      CANCLE_OB_LOG_TRACE_MODE();    // prevent printing log

      pos_v1 = 0;
      pos_v2 = 0;
      EXPECT_EQ(OB_SUCCESS, num1.format_v1(buf_v1, MAX_BUF_SIZE, pos_v1, -1));
      EXPECT_EQ(OB_SUCCESS, num1.format_v2(buf_v2, MAX_BUF_SIZE, pos_v2, -1));

      _OB_LOG(INFO, "debug jianhua decimal, j=%ld, i=%ld, k=%ld, v1=%s", j, i, k, tmp_buf);
      EXPECT_EQ(pos_v1, pos_v2);
      EXPECT_EQ(0, memcmp(buf_v1, buf_v2, pos_v1));
      allocator.free();


      if (i > end_value) {
        i -= (end_value - begin_value);
      }
    }
  }

  //200
  char const_number_str[] = "98765432109876543210987654321098765432109876543210987654321098765432109876543210987654321098765432109876543210987654321098765432109876543210987654321098765432109876543210987654321098765432109876543210";
  ObString const_number_string(const_number_str);
  for (int64_t j = 1; j < 80; ++j) {
    number::ObNumber num1, num2, value_old,value_new;
    for (int64_t i = 0; i < 11; ++i) {
      allocator.free();
      SET_OB_LOG_TRACE_MODE();    // prevent printing log
      num1.from_v1(const_number_str + i, j, allocator);
      CANCLE_OB_LOG_TRACE_MODE();    // prevent printing log

      pos_v1 = 0;
      pos_v2 = 0;
      EXPECT_EQ(OB_SUCCESS, num1.format_v1(buf_v1, MAX_BUF_SIZE, pos_v1, -1));
      EXPECT_EQ(OB_SUCCESS, num1.format_v2(buf_v2, MAX_BUF_SIZE, pos_v2, -1));

//      _OB_LOG(INFO, "debug jianhua decimal, j=%ld, i=%ld, ffi=%.*s, l1=%ld, l2=%ld,  v1=%.*s, v2=%.*s", j, i, j, const_number_str + i, pos_v1, pos_v2, pos_v1, buf_v1, pos_v2, buf_v2);
      EXPECT_EQ(pos_v1, pos_v2);
      EXPECT_EQ(0, memcmp(buf_v1, buf_v2, pos_v1));
    }
  }
}


TEST(ObNumber, number_format_perf)
{
  const int64_t MAX_TEST_COUNT  = 10000;
  const int64_t MAX_BUF_SIZE = 256;
  char buf[MAX_BUF_SIZE];
  char buf_v2[MAX_BUF_SIZE];
  char buf_alloc[MAX_BUF_SIZE];
  int64_t pos = 0;
  int64_t get_range_beg = 0;
  int64_t get_range_cost = 0;
  int64_t base_get_range_cost = 0;
  int64_t begin_value = 0;
  int64_t end_value   = 0;
  ObDataBuffer allocator(buf_alloc, MAX_BUF_SIZE);

  for (int64_t j = 0; j < 20; ++j) {
    begin_value = pow(10, j);
    end_value = pow(10, j + 1) - 1;
    _OB_LOG(INFO, "test numer %ld, [%ld, %ld]", j + 1, begin_value, end_value);

    get_range_beg = ObTimeUtility::current_time();
    number::ObNumber num1;
    for (int64_t i = begin_value, k = 0; k < MAX_TEST_COUNT; ++i, ++k) {
      num1.from(i, allocator);
      pos = 0;
      allocator.free();
      if (i > end_value) {
        i -= (end_value - begin_value);
      }
    }
    base_get_range_cost = ObTimeUtility::current_time() - get_range_beg;


    get_range_beg = ObTimeUtility::current_time();
    for (int64_t i = begin_value, k = 0; k < MAX_TEST_COUNT; ++i, ++k) {
      num1.from(i, allocator);
      pos = 0;
      num1.format_v1(buf, MAX_BUF_SIZE, pos, -1);
      allocator.free();
      if (i > end_value) {
        i -= (end_value - begin_value);
      }
    }
    get_range_cost = ObTimeUtility::current_time() - get_range_beg - base_get_range_cost;
    _OB_LOG(INFO, "format, cost time: %f us", (double)get_range_cost / MAX_TEST_COUNT);


    get_range_beg = ObTimeUtility::current_time();
    for (int64_t i = begin_value, k = 0; k < MAX_TEST_COUNT; ++i, ++k) {
      num1.from(i, allocator);
      pos = 0;
      num1.format_v1(buf, MAX_BUF_SIZE, pos, -1);
      allocator.free();
      if (i > end_value) {
        i -= (end_value - begin_value);
      }
    }
    get_range_cost = ObTimeUtility::current_time() - get_range_beg - base_get_range_cost;
    _OB_LOG(INFO, "format_v1, cost time: %f us", (double)get_range_cost / MAX_TEST_COUNT);


    get_range_beg = ObTimeUtility::current_time();
    for (int64_t i = begin_value, k = 0; k < MAX_TEST_COUNT; ++i, ++k) {
      num1.from(i, allocator);
      pos = 0;
      num1.format_v2(buf, MAX_BUF_SIZE, pos, -1);
      allocator.free();
      if (i > end_value) {
        i -= (end_value - begin_value);
      }
    }
    get_range_cost = ObTimeUtility::current_time() - get_range_beg - base_get_range_cost;
    _OB_LOG(INFO, "format_v2, cost time: %f us", (double)get_range_cost / MAX_TEST_COUNT);
  }


  for (int64_t j = 0; j < 20; ++j) {
    begin_value = pow(10, j);
    end_value = pow(10, j + 1) - 1;

    _OB_LOG(INFO, "test numer decimal %ld, [%ld, %ld]", j + 1, begin_value, end_value);

    get_range_beg = ObTimeUtility::current_time();
    number::ObNumber num1;
    for (int64_t i = begin_value, k = 0; k < MAX_TEST_COUNT; ++i, ++k) {
      ObFastFormatInt ffi(i);
      *const_cast<char *>(ffi.ptr() + ffi.length() / 2) = '.';
      num1.from_v1(ffi.ptr(), ffi.length(), allocator);
      pos = 0;
      allocator.free();
      if (i > end_value) {
        i -= (end_value - begin_value);
      }
    }
    base_get_range_cost = ObTimeUtility::current_time() - get_range_beg;


    get_range_beg = ObTimeUtility::current_time();
    for (int64_t i = begin_value, k = 0; k < MAX_TEST_COUNT; ++i, ++k) {
      ObFastFormatInt ffi(i);
      *const_cast<char *>(ffi.ptr() + ffi.length() / 2) = '.';
      num1.from_v1(ffi.ptr(), ffi.length(), allocator);
      pos = 0;
      num1.format_v1(buf, MAX_BUF_SIZE, pos, -1);
      allocator.free();
      if (i > end_value) {
        i -= (end_value - begin_value);
      }
    }
    get_range_cost = ObTimeUtility::current_time() - get_range_beg - base_get_range_cost;
    _OB_LOG(INFO, "format, cost time: %f us", (double)get_range_cost / MAX_TEST_COUNT);

    get_range_beg = ObTimeUtility::current_time();
    for (int64_t i = begin_value, k = 0; k < MAX_TEST_COUNT; ++i, ++k) {
      ObFastFormatInt ffi(i);
      *const_cast<char *>(ffi.ptr() + ffi.length() / 2) = '.';
      num1.from_v1(ffi.ptr(), ffi.length(), allocator);
      pos = 0;
      num1.format_v2(buf, MAX_BUF_SIZE, pos, -1);
      allocator.free();
      if (i > end_value) {
        i -= (end_value - begin_value);
      }
    }
    get_range_cost = ObTimeUtility::current_time() - get_range_beg - base_get_range_cost;
    _OB_LOG(INFO, "format_v2, cost time: %f us", (double)get_range_cost / MAX_TEST_COUNT);
  }
}


TEST(ObNumber, DISABLED_number_format_from_perf_v2)
{
  const int64_t MAX_TEST_COUNT  = 10000;
  const int64_t MAX_TEST_COUNT2 = 40;
  const int64_t MAX_BUF_SIZE = 256;

  char buf_alloc[MAX_BUF_SIZE];
  char buf_alloc2[MAX_BUF_SIZE];
  int64_t tmp = 0;
  int64_t pos = 0;
  int64_t get_range_beg = 0;
  int64_t base_get_range_cost = 0;
  int64_t base_get_range_cost2 = 0;
  int64_t get_range_cost = 0;
  int64_t get_range_cost2 = 0;
  int64_t begin_value = 0;
  int64_t end_value   = 0;
  ObDataBuffer allocator(buf_alloc, MAX_BUF_SIZE);
  const int64_t MAX_DOUBLE_STRICT_PRINT_SIZE2 = 512;
  char buf[MAX_DOUBLE_STRICT_PRINT_SIZE2] = {};
  int64_t length = 0 ;

  enum TestType { TT_MIN = 0, TT_MAX, TT_RAND, TT_MAX_TYPE };
  uint32_t data[TestType::TT_MAX_TYPE][MAX_TEST_COUNT];
  for (int64_t i = 0; i < MAX_TEST_COUNT; ++i) {
    data[TestType::TT_MIN][i] = rand() % 100;
    data[TestType::TT_MAX][i] = rand() % 100 + ObNumber::BASE - 120;
    data[TestType::TT_RAND][i] = rand() % ObNumber::BASE;
  }

  uint32_t digits1[ObNumber::MAX_STORE_LEN];
  uint32_t digits2[ObNumber::MAX_STORE_LEN];
  for (int64_t k = 0; k < TestType::TT_MAX_TYPE; ++k) {
    for (int64_t i = 1; i <= ObNumber::MAX_STORE_LEN; ++i) {
      _OB_LOG(INFO, "test numer k=%ld, i=%ld", k, i);

      ObNumber value_v1;

      base_get_range_cost = 0;
      for (int64_t r = 1; r <= MAX_TEST_COUNT2; ++r) {
        get_range_beg = ObTimeUtility::current_time();
        for (int64_t j = 0; j < MAX_TEST_COUNT; ++j) {
          INIT_NUMBER12;
  //        EXPECT_EQ(OB_SUCCESS, num1.add_v1(num2, value_v1, allocator));
          allocator.free();
        }
        base_get_range_cost += (ObTimeUtility::current_time() - get_range_beg);
      }
      base_get_range_cost2 = base_get_range_cost / MAX_TEST_COUNT2;

      get_range_cost = 0;
      for (int64_t r = 1; r <= MAX_TEST_COUNT2; ++r) {
        get_range_beg = ObTimeUtility::current_time();
        for (int64_t j = 0; j < MAX_TEST_COUNT; ++j) {
          INIT_NUMBER12;
          pos = 0;
          num1.format_v1(buf, MAX_BUF_SIZE, pos, -1);
        }
        get_range_cost += (ObTimeUtility::current_time() - get_range_beg);
      }
      get_range_cost2 = get_range_cost/MAX_TEST_COUNT2 - base_get_range_cost2;
      _OB_LOG(INFO, "format_v1, cost time: %f us", (double)get_range_cost2 / MAX_TEST_COUNT);


      get_range_cost = 0;
      for (int64_t r = 1; r <= MAX_TEST_COUNT2; ++r) {
        get_range_beg = ObTimeUtility::current_time();
        for (int64_t j = 0; j < MAX_TEST_COUNT; ++j) {
          INIT_NUMBER12;
          pos = 0;
          num1.format_v2(buf, MAX_BUF_SIZE, pos, -1);
        }
        get_range_cost += (ObTimeUtility::current_time() - get_range_beg);
      }
      get_range_cost2 = get_range_cost/MAX_TEST_COUNT2 - base_get_range_cost2;
      _OB_LOG(INFO, "format_v2, cost time: %f us", (double)get_range_cost2 / MAX_TEST_COUNT);

      base_get_range_cost2 = get_range_cost2;
      get_range_cost = 0;
      for (int64_t r = 1; r <= MAX_TEST_COUNT2; ++r) {
        get_range_beg = ObTimeUtility::current_time();
        for (int64_t j = 0; j < MAX_TEST_COUNT; ++j) {
          INIT_NUMBER12;
          pos = 0;
          num1.format_v2(buf, MAX_BUF_SIZE, pos, -1);
          num1.from_v1(buf, pos, allocator);
          allocator.free();
        }
        get_range_cost += (ObTimeUtility::current_time() - get_range_beg);
      }
      get_range_cost2 = get_range_cost/MAX_TEST_COUNT2 - base_get_range_cost2;
      _OB_LOG(INFO, "from_v1, cost time: %f us", (double)get_range_cost2 / MAX_TEST_COUNT);

      get_range_cost = 0;
      for (int64_t r = 1; r <= MAX_TEST_COUNT2; ++r) {
        get_range_beg = ObTimeUtility::current_time();
        for (int64_t j = 0; j < MAX_TEST_COUNT; ++j) {
          INIT_NUMBER12;
          pos = 0;
          num1.format_v2(buf, MAX_BUF_SIZE, pos, -1);
          num1.from_v2(buf, pos, allocator);
          allocator.free();
        }
        get_range_cost += (ObTimeUtility::current_time() - get_range_beg);
      }
      get_range_cost2 = get_range_cost/MAX_TEST_COUNT2 - base_get_range_cost2;
      _OB_LOG(INFO, "from_v2, cost time: %f us", (double)get_range_cost2 / MAX_TEST_COUNT);

      get_range_cost = 0;
      for (int64_t r = 1; r <= MAX_TEST_COUNT2; ++r) {
        get_range_beg = ObTimeUtility::current_time();
        for (int64_t j = 0; j < MAX_TEST_COUNT; ++j) {
          INIT_NUMBER12;
          pos = 0;
          num1.format_v2(buf, MAX_BUF_SIZE, pos, -1);
          num1.from_v3(buf, pos, allocator);
          allocator.free();
        }
        get_range_cost += (ObTimeUtility::current_time() - get_range_beg);
      }
      get_range_cost2 = get_range_cost/MAX_TEST_COUNT2 - base_get_range_cost2;
      _OB_LOG(INFO, "from_v3, cost time: %f us", (double)get_range_cost2 / MAX_TEST_COUNT);
    }
  }
}

TEST(ObNumber, format_number_from_cmp)
{
  const int64_t MAX_TEST_COUNT  = 1000;
  const int64_t MAX_BUF_SIZE = 256;
  char buf_v1[MAX_BUF_SIZE];
  char buf_v2[MAX_BUF_SIZE];
  int64_t pos_v1 = 0;
  int64_t pos_v2 = 0;

  char buf_alloc[MAX_BUF_SIZE];
  int64_t pos = 0;
  int64_t get_range_beg = 0;
  int64_t get_range_cost = 0;
  int64_t begin_value = 0;
  int64_t end_value   = 0;
  ObDataBuffer allocator(buf_alloc, MAX_BUF_SIZE);

  for (int64_t j = 0; j < 20; ++j) {
    begin_value = pow(10, j);
    end_value = pow(10, j + 1) - 1;
    _OB_LOG(INFO, "test numer %ld, [%ld, %ld]", j + 1, begin_value, end_value);

    number::ObNumber num1;
    number::ObNumber num2;
    number::ObNumber num3;
    int64_t add_step = (end_value - begin_value) / MAX_TEST_COUNT;
    if (add_step < 17) {
      add_step = 17;
    }
    for (int64_t i = begin_value, k = 0; k < MAX_TEST_COUNT && i < end_value - begin_value; i += add_step, ++k) {
      int64_t tmp_int = (i * (1 == i%2 ? 1 : -1));
      ObFastFormatInt ffi(tmp_int);
      EXPECT_EQ(OB_SUCCESS, num1.from(tmp_int, allocator));
      EXPECT_EQ(OB_SUCCESS, num2.from_v2(ffi.ptr(), ffi.length(), allocator));
      EXPECT_EQ(OB_SUCCESS, num3.from_v3(ffi.ptr(), ffi.length(), allocator));

      OB_LOG(INFO, "debug jianhua", K(j), K(i), K(k), "str", ffi.str(), K(num1), K(num2), K(num3));
      ASSERT_EQ(0, num1.compare_v1(num2));
      ASSERT_EQ(0, num1.compare_v1(num3));
      allocator.free();

      if (i > end_value) {
        i -= (end_value - begin_value);
      }
    }
  }

  for (int64_t j = 0; j < 20; ++j) {
    begin_value = pow(10, j);
    end_value = pow(10, j + 1) - 1;
    _OB_LOG(INFO, "test numer %ld, [%ld, %ld]", j + 1, begin_value, end_value);

    number::ObNumber num1;
    number::ObNumber num2;
    int64_t add_step = (end_value - begin_value) / MAX_TEST_COUNT;
    if (add_step < 17) {
      add_step = 17;
    }
    for (int64_t i = begin_value, k = 0; k < MAX_TEST_COUNT && i < end_value - begin_value; i += add_step, ++k) {
      ObFastFormatInt ffi(i * (1 == i%2 ? 1 : -1));
      EXPECT_EQ(OB_SUCCESS, num1.from_v1(ffi.ptr(), ffi.length(), allocator));
      EXPECT_EQ(OB_SUCCESS, num2.from_v2(ffi.ptr(), ffi.length(), allocator));

      OB_LOG(INFO, "debug jianhua", K(j), K(i), K(k), "str", ffi.str(), K(num1), K(num2));
      ASSERT_EQ(0, num1.compare_v1(num2));
      allocator.free();

      if (i > end_value) {
        i -= (end_value - begin_value);
      }
    }
  }

  for (int64_t j = 0; j < 20; ++j) {
    begin_value = pow(10, j);
    end_value = pow(10, j + 1) - 1;
    _OB_LOG(INFO, "test numer %ld, [%ld, %ld]", j + 1, begin_value, end_value);

    number::ObNumber num1;
    number::ObNumber num2;
    int64_t add_step = (end_value - begin_value) / MAX_TEST_COUNT;
    if (add_step < 17) {
      add_step = 17;
    }
    for (int64_t i = begin_value, k = 0; k < MAX_TEST_COUNT && i < end_value - begin_value; i += add_step, ++k) {
      for (int64_t m = 0; m < j; ++m) {
        ObFastFormatInt ffi(i * (1 == i%2 ? 1 : -1));
        *const_cast<char *>(ffi.ptr() + m) = '.';
        EXPECT_EQ(OB_SUCCESS, num1.from_v1(ffi.ptr(), ffi.length(), allocator));
        EXPECT_EQ(OB_SUCCESS, num2.from_v2(ffi.ptr(), ffi.length(), allocator));

        OB_LOG(INFO, "debug jianhua decimal", K(j), K(i), K(k), K(m), "str", ffi.str(), K(num1), K(num2));
        ASSERT_EQ(0, num1.compare_v1(num2));
        allocator.free();
      }

      char tmp_buf[MAX_BUF_SIZE];
      snprintf(tmp_buf, MAX_BUF_SIZE, "%.17lf", (i * (1 == i%2 ? 1 : -1)) * pow(10, 20));
      ObString ffi(strlen(tmp_buf), tmp_buf);
      EXPECT_EQ(OB_SUCCESS, num1.from_v1(ffi.ptr(), ffi.length(), allocator));
      EXPECT_EQ(OB_SUCCESS, num2.from_v2(ffi.ptr(), ffi.length(), allocator));

      OB_LOG(INFO, "debug jianhua decimal", K(j), K(i), K(k), "str", tmp_buf);
      ASSERT_EQ(0, num1.compare_v1(num2));
      allocator.free();

      if (i > end_value) {
        i -= (end_value - begin_value);
      }
    }
  }

  //200
  char const_number_str[] = "98765432109876543210987654321098765432109876543210987654321098765432109876543210987654321098765432109876543210987654321098765432109876543210987654321098765432109876543210987654321098765432109876543210";
  ObString const_number_string(const_number_str);
  for (int64_t j = 1; j < 80; ++j) {
    number::ObNumber num1, num2, value_old,value_new;
    for (int64_t i = 0; i < 11; ++i) {
      _OB_LOG(INFO, "debug jianhua decimal, j=%ld, i=%ld, ffi=%.*s", j, i, static_cast<int>(j), const_number_str + i);

      allocator.free();
      num1.set_zero();
      num2.set_zero();

      ASSERT_EQ(num1.from_v1(const_number_str + i, j, allocator), num2.from_v2(const_number_str + i, j, allocator));
      ASSERT_EQ(0, num1.compare_v1(num2));
      allocator.free();
      ASSERT_EQ(num1.from_v1(const_number_str + i, j, allocator), num2.from_v3(const_number_str + i, j, allocator));
      ASSERT_EQ(0, num1.compare_v1(num2));
    }
  }
}



TEST(ObNumber, format_number_from_perf)
{
  const int64_t MAX_TEST_COUNT  = 10000;
  const int64_t MAX_BUF_SIZE = 256;
  char buf[MAX_BUF_SIZE];
  char buf_v2[MAX_BUF_SIZE];
  char buf_alloc[MAX_BUF_SIZE];
  int64_t pos = 0;
  int64_t get_range_beg = 0;
  int64_t get_range_cost = 0;
  int64_t base_get_range_cost = 0;
  int64_t begin_value = 0;
  int64_t end_value   = 0;
  ObDataBuffer allocator(buf_alloc, MAX_BUF_SIZE);

  for (int64_t j = 0; j < 21; ++j) {
    begin_value = pow(10, j);
    end_value = pow(10, j + 1) - 1;
    _OB_LOG(INFO, "test numer %ld, [%ld, %ld]", j + 1, begin_value, end_value);

    get_range_beg = ObTimeUtility::current_time();
    number::ObNumber num1;
    for (int64_t i = begin_value, k = 0; k < MAX_TEST_COUNT; ++i, ++k) {
      allocator.free();
      if (i > end_value) {
        i -= (end_value - begin_value);
      }
    }
    base_get_range_cost = ObTimeUtility::current_time() - get_range_beg;


    get_range_beg = ObTimeUtility::current_time();
    for (int64_t i = begin_value, k = 0; k < MAX_TEST_COUNT; ++i, ++k) {
      ObFastFormatInt ffi(i);
      num1.from_v1(ffi.ptr(), ffi.length(), allocator);
      allocator.free();
      if (i > end_value) {
        i -= (end_value - begin_value);
      }
    }
    get_range_cost = ObTimeUtility::current_time() - get_range_beg - base_get_range_cost;
    _OB_LOG(INFO, "from, cost time: %f us", (double)get_range_cost / MAX_TEST_COUNT);


    get_range_beg = ObTimeUtility::current_time();
    for (int64_t i = begin_value, k = 0; k < MAX_TEST_COUNT; ++i, ++k) {
      ObFastFormatInt ffi(i);
      num1.from_v2(ffi.ptr(), ffi.length(), allocator);
      allocator.free();
      if (i > end_value) {
        i -= (end_value - begin_value);
      }
    }
    get_range_cost = ObTimeUtility::current_time() - get_range_beg - base_get_range_cost;
    _OB_LOG(INFO, "from_v2, cost time: %f us", (double)get_range_cost / MAX_TEST_COUNT);

    get_range_beg = ObTimeUtility::current_time();
    for (int64_t i = begin_value, k = 0; k < MAX_TEST_COUNT; ++i, ++k) {
      ObFastFormatInt ffi(i);
      num1.from_v3(ffi.ptr(), ffi.length(), allocator);
      allocator.free();
      if (i > end_value) {
        i -= (end_value - begin_value);
      }
    }
    get_range_cost = ObTimeUtility::current_time() - get_range_beg - base_get_range_cost;
    _OB_LOG(INFO, "from_v3, cost time: %f us", (double)get_range_cost / MAX_TEST_COUNT);
  }


  for (int64_t j = 0; j < 21; ++j) {
    begin_value = pow(10, j);
    end_value = pow(10, j + 1) - 1;
    _OB_LOG(INFO, "test numer decimal %ld, [%ld, %ld]", j + 1, begin_value, end_value);

    get_range_beg = ObTimeUtility::current_time();
    number::ObNumber num1;
    for (int64_t i = begin_value, k = 0; k < MAX_TEST_COUNT; ++i, ++k) {
      allocator.free();
      if (i > end_value) {
        i -= (end_value - begin_value);
      }
    }
    base_get_range_cost = ObTimeUtility::current_time() - get_range_beg;


    get_range_beg = ObTimeUtility::current_time();
    for (int64_t i = begin_value, k = 0; k < MAX_TEST_COUNT; ++i, ++k) {
      ObFastFormatInt ffi(i);
      *const_cast<char *>(ffi.ptr() + ffi.length() / 2) = '.';
      num1.from(ffi.ptr(), ffi.length(), allocator);
      allocator.free();
      if (i > end_value) {
        i -= (end_value - begin_value);
      }
    }
    get_range_cost = ObTimeUtility::current_time() - get_range_beg - base_get_range_cost;
    _OB_LOG(INFO, "from, cost time: %f us", (double)get_range_cost / MAX_TEST_COUNT);


    get_range_beg = ObTimeUtility::current_time();
    for (int64_t i = begin_value, k = 0; k < MAX_TEST_COUNT; ++i, ++k) {
      ObFastFormatInt ffi(i);
      *const_cast<char *>(ffi.ptr() + ffi.length() / 2) = '.';
      num1.from_v2(ffi.ptr(), ffi.length(), allocator);
      allocator.free();
      if (i > end_value) {
        i -= (end_value - begin_value);
      }
    }
    get_range_cost = ObTimeUtility::current_time() - get_range_beg - base_get_range_cost;
    _OB_LOG(INFO, "from_v2, cost time: %f us", (double)get_range_cost / MAX_TEST_COUNT);
  }
}



TEST(ObNumber, format_number_round_cmp)
{
  const int64_t MAX_TEST_COUNT  = 10000;
  const int64_t MAX_BUF_SIZE = 256;

  char buf_alloc[MAX_BUF_SIZE];
  int64_t pos = 0;
  int64_t get_range_beg = 0;
  int64_t get_range_cost = 0;
  int64_t begin_value = 0;
  int64_t end_value   = 0;
  ObDataBuffer allocator(buf_alloc, MAX_BUF_SIZE);

  for (int64_t j = 0; j < 20; ++j) {
    begin_value = pow(10, j);
    end_value = pow(10, j + 1) - 1;
    _OB_LOG(INFO, "test numer %ld, [%ld, %ld]", j + 1, begin_value, end_value);

    number::ObNumber num1;
    number::ObNumber num2;
    int64_t add_step = (end_value - begin_value) / MAX_TEST_COUNT;
    if (add_step < 17) {
      add_step = 17;
    }
    for (int64_t i = begin_value, k = 0; k < MAX_TEST_COUNT && i < end_value - begin_value; i += add_step, ++k) {
      num1.from(i * (1 == i%2 ? 1 : -1), allocator);
      num2.from(i * (1 == i%2 ? 1 : -1), allocator);

      _OB_LOG(INFO, "debug jianhua, j=%ld, i=%ld, k=%ld", j ,i, k);
      for (int64_t z = 0; z < j + 2; z++) {
        EXPECT_EQ(OB_SUCCESS, num1.round_v1(z));
        EXPECT_EQ(OB_SUCCESS, num2.round_v2(z));
        EXPECT_EQ(num1, num2);

        EXPECT_EQ(OB_SUCCESS, num2.round_v3(z));
        EXPECT_EQ(num1, num2);
      }
      allocator.free();

      if (i > end_value) {
        i -= (end_value - begin_value);
      }
    }
  }

  for (int64_t j = 0; j < 20; ++j) {
    begin_value = pow(10, j);
    end_value = pow(10, j + 1) - 1;
    _OB_LOG(INFO, "test numer %ld, [%ld, %ld]", j + 1, begin_value, end_value);

    number::ObNumber num1;
    number::ObNumber num2;
    int64_t add_step = (end_value - begin_value) / MAX_TEST_COUNT;
    if (add_step < 17) {
      add_step = 17;
    }
    for (int64_t i = begin_value, k = 0; k < MAX_TEST_COUNT && i < end_value - begin_value; i += add_step, ++k) {
      for (int64_t m = 0; m < j; ++m) {
        ObFastFormatInt ffi(i * (1 == i%2 ? 1 : -1));
        *const_cast<char *>(ffi.ptr() + m) = '.';
        SET_OB_LOG_TRACE_MODE();    // prevent printing log
        num1.from_v1(ffi.ptr(), ffi.length(), allocator);
        num2.from_v1(ffi.ptr(), ffi.length(), allocator);
        CANCLE_OB_LOG_TRACE_MODE();    // prevent printing log

        _OB_LOG(INFO, "debug jianhua decimal, j=%ld, i=%ld, k=%ld, ffi=%.*s", j, i, k, static_cast<int>(ffi.length()), ffi.ptr());
        for (int64_t z = -(j+2); z < j + 2; z++) {
          EXPECT_EQ(OB_SUCCESS, num1.round_v1(z));
          EXPECT_EQ(OB_SUCCESS, num2.round_v2(z));
          ASSERT_EQ(num1, num2);

          EXPECT_EQ(OB_SUCCESS, num2.round_v3(z));
          ASSERT_EQ(num1, num2);
        }
        allocator.free();
      }

      if (i > end_value) {
        i -= (end_value - begin_value);
      }
    }
  }


  for (int64_t j = 0; j < 20; ++j) {
    begin_value = pow(10, j);
    end_value = pow(10, j + 1) - 1;
    _OB_LOG(INFO, "test numer %ld, [%ld, %ld]", j + 1, begin_value, end_value);

    number::ObNumber num1;
    number::ObNumber num2;
    int64_t add_step = (end_value - begin_value) / MAX_TEST_COUNT;
    if (add_step < 17) {
      add_step = 17;
    }
    for (int64_t i = begin_value, k = 0; k < MAX_TEST_COUNT && i < end_value - begin_value; i += add_step, ++k) {
      char tmp_buf[MAX_BUF_SIZE];
      snprintf(tmp_buf, MAX_BUF_SIZE, "%.17lf", (i * (1 == i%2 ? 1 : -1)) * pow(10, 20));
      SET_OB_LOG_TRACE_MODE();    // prevent printing log
      num1.from(tmp_buf, allocator);
      num2.from(tmp_buf, allocator);
      CANCLE_OB_LOG_TRACE_MODE();    // prevent printing log

      _OB_LOG(INFO, "debug jianhua decimal, j=%ld, i=%ld, k=%ld, str=%s", j, i, k, tmp_buf);
      for (int64_t z = -(j+2); z < j + 2; z++) {
        EXPECT_EQ(OB_SUCCESS, num1.round_v1(z));
        EXPECT_EQ(OB_SUCCESS, num2.round_v2(z));
        EXPECT_EQ(num1, num2);

        EXPECT_EQ(OB_SUCCESS, num2.round_v3(z));
        EXPECT_EQ(num1, num2);
      }
      allocator.free();

      if (i > end_value) {
        i -= (end_value - begin_value);
      }
    }
  }

  //200
  char const_number_str[] = "98765432109876543210987654321098765432109876543210987654321098765432109876543210987654321098765432109876543210987654321098765432109876543210987654321098765432109876543210987654321098765432109876543210";
  ObString const_number_string(const_number_str);
  for (int64_t j = 1; j < 80; ++j) {
    number::ObNumber num1, num2, value_old,value_new;
    for (int64_t i = 0; i < 11; ++i) {
      _OB_LOG(INFO, "debug jianhua decimal, j=%ld, i=%ld, ffi=%.*s", j, i, static_cast<int>(j), const_number_str + i);

      allocator.free();
      SET_OB_LOG_TRACE_MODE();    // prevent printing log
      num1.from_v1(const_number_str + i, j, allocator);
      num2.from_v1(const_number_str + i, j, allocator);
      CANCLE_OB_LOG_TRACE_MODE();    // prevent printing log

      for (int64_t z = -(j+2); z < j + 2; z++) {
        num1.set_zero();
        num2.set_zero();
        EXPECT_EQ(num1.round_v1(z), num2.round_v2(z));
        EXPECT_EQ(num1, num2);

        num1.set_zero();
        num2.set_zero();
        EXPECT_EQ(num1.round_v1(z), num2.round_v3(z));
        EXPECT_EQ(num1, num2);
      }
    }
  }
}


TEST(ObNumber, DISABLED_format_number_round_perf)
{
  const int64_t MAX_TEST_COUNT  = 10000;
  const int64_t MAX_BUF_SIZE = 256;
  char buf_alloc[MAX_BUF_SIZE];
  int64_t pos = 0;
  int64_t get_range_beg = 0;
  int64_t get_range_cost = 0;
  int64_t base_get_range_cost = 0;
  int64_t begin_value = 0;
  int64_t end_value   = 0;
  ObDataBuffer allocator(buf_alloc, MAX_BUF_SIZE);

  for (int64_t j = 0; j < 20; ++j) {
    begin_value = pow(10, j);
    end_value = pow(10, j + 1) - 1;
    _OB_LOG(INFO, "test numer %ld, [%ld, %ld]", j + 1, begin_value, end_value);

    get_range_beg = ObTimeUtility::current_time();
    number::ObNumber num1;
    for (int64_t i = begin_value, k = 0; k < MAX_TEST_COUNT; ++i, ++k) {
      num1.from(i, allocator);
      for (int64_t z = -(j+2); z < j + 2; z++) {
        //nothing
      }
      allocator.free();
      if (i > end_value) {
        i -= (end_value - begin_value);
      }
    }
    base_get_range_cost = ObTimeUtility::current_time() - get_range_beg;


    get_range_beg = ObTimeUtility::current_time();
    for (int64_t i = begin_value, k = 0; k < MAX_TEST_COUNT; ++i, ++k) {
      num1.from(i, allocator);
      for (int64_t z = -(j+2); z < j + 2; z++) {
        num1.round_v1(z);
      }
      allocator.free();
      if (i > end_value) {
        i -= (end_value - begin_value);
      }
    }
    get_range_cost = ObTimeUtility::current_time() - get_range_beg - base_get_range_cost;
    _OB_LOG(INFO, "round_old, cost time: %f us", (double)get_range_cost / MAX_TEST_COUNT);


    get_range_beg = ObTimeUtility::current_time();
    for (int64_t i = begin_value, k = 0; k < MAX_TEST_COUNT; ++i, ++k) {
      num1.from(i, allocator);
      pos = 0;
      for (int64_t z = -(j+2); z < j + 2; z++) {
        num1.round_v2(z);
      }
      allocator.free();
      if (i > end_value) {
        i -= (end_value - begin_value);
      }
    }
    get_range_cost = ObTimeUtility::current_time() - get_range_beg - base_get_range_cost;
    _OB_LOG(INFO, "round_v2, cost time: %f us", (double)get_range_cost / MAX_TEST_COUNT);

    get_range_beg = ObTimeUtility::current_time();
    for (int64_t i = begin_value, k = 0; k < MAX_TEST_COUNT; ++i, ++k) {
      num1.from(i, allocator);
      pos = 0;
      for (int64_t z = -(j+2); z < j + 2; z++) {
        num1.round_v3(z);
      }
      allocator.free();
      if (i > end_value) {
        i -= (end_value - begin_value);
      }
    }
    get_range_cost = ObTimeUtility::current_time() - get_range_beg - base_get_range_cost;
    _OB_LOG(INFO, "round_v3, cost time: %f us", (double)get_range_cost / MAX_TEST_COUNT);

  }

  for (int64_t j = 0; j < 20; ++j) {
    begin_value = pow(10, j);
    end_value = pow(10, j + 1) - 1;

    _OB_LOG(INFO, "test numer decimal %ld, [%ld, %ld]", j + 1, begin_value, end_value);

    get_range_beg = ObTimeUtility::current_time();
    number::ObNumber num1;
    for (int64_t i = begin_value, k = 0; k < MAX_TEST_COUNT; ++i, ++k) {
      ObFastFormatInt ffi(i);
      *const_cast<char *>(ffi.ptr() + ffi.length() / 2) = '.';
      num1.from_v1(ffi.ptr(), ffi.length(), allocator);
      for (int64_t z = -(j+2); z < j + 2; z++) {
        //
      }
      allocator.free();
      if (i > end_value) {
        i -= (end_value - begin_value);
      }
    }
    base_get_range_cost = ObTimeUtility::current_time() - get_range_beg;


    get_range_beg = ObTimeUtility::current_time();
    for (int64_t i = begin_value, k = 0; k < MAX_TEST_COUNT; ++i, ++k) {
      ObFastFormatInt ffi(i);
      *const_cast<char *>(ffi.ptr() + ffi.length() / 2) = '.';
      num1.from_v1(ffi.ptr(), ffi.length(), allocator);
      for (int64_t z = -(j+2); z < j + 2; z++) {
        num1.round_v1(z);
      }
      allocator.free();
      if (i > end_value) {
        i -= (end_value - begin_value);
      }
    }
    get_range_cost = ObTimeUtility::current_time() - get_range_beg - base_get_range_cost;
    _OB_LOG(INFO, "rould_old, cost time: %f us", (double)get_range_cost / MAX_TEST_COUNT);

    get_range_beg = ObTimeUtility::current_time();
    for (int64_t i = begin_value, k = 0; k < MAX_TEST_COUNT; ++i, ++k) {
      ObFastFormatInt ffi(i);
      *const_cast<char *>(ffi.ptr() + ffi.length() / 2) = '.';
      num1.from_v1(ffi.ptr(), ffi.length(), allocator);
      for (int64_t z = -(j+2); z < j + 2; z++) {
        num1.round_v2(z);
      }
      allocator.free();
      if (i > end_value) {
        i -= (end_value - begin_value);
      }
    }
    get_range_cost = ObTimeUtility::current_time() - get_range_beg - base_get_range_cost;
    _OB_LOG(INFO, "round_v2, cost time: %f us", (double)get_range_cost / MAX_TEST_COUNT);


    get_range_beg = ObTimeUtility::current_time();
    for (int64_t i = begin_value, k = 0; k < MAX_TEST_COUNT; ++i, ++k) {
      ObFastFormatInt ffi(i);
      *const_cast<char *>(ffi.ptr() + ffi.length() / 2) = '.';
      num1.from_v1(ffi.ptr(), ffi.length(), allocator);
      for (int64_t z = -(j+2); z < j + 2; z++) {
        num1.round_v3(z);
      }
      allocator.free();
      if (i > end_value) {
        i -= (end_value - begin_value);
      }
    }
    get_range_cost = ObTimeUtility::current_time() - get_range_beg - base_get_range_cost;
    _OB_LOG(INFO, "round_v3, cost time: %f us", (double)get_range_cost / MAX_TEST_COUNT);

  }

  for (int64_t j = 0; j < 20; ++j) {
    begin_value = pow(10, j);
    end_value = pow(10, j + 1) - 1;

    _OB_LOG(INFO, "test numer decimal2 %ld, [%ld, %ld]", j + 1, begin_value, end_value);

    get_range_beg = ObTimeUtility::current_time();
    number::ObNumber num1;
    for (int64_t i = begin_value, k = 0; k < MAX_TEST_COUNT; ++i, ++k) {
      char tmp_buf[MAX_BUF_SIZE];
      snprintf(tmp_buf, MAX_BUF_SIZE, "%.17lf", (i * (1 == i%2 ? 1 : -1)) * pow(10, 20));
      num1.from_v1(tmp_buf, strlen(tmp_buf), allocator);
      for (int64_t z = -(j+2); z < j + 2; z++) {
        //
      }
      allocator.free();
      if (i > end_value) {
        i -= (end_value - begin_value);
      }
    }
    base_get_range_cost = ObTimeUtility::current_time() - get_range_beg;


    get_range_beg = ObTimeUtility::current_time();
    for (int64_t i = begin_value, k = 0; k < MAX_TEST_COUNT; ++i, ++k) {
      char tmp_buf[MAX_BUF_SIZE];
      snprintf(tmp_buf, MAX_BUF_SIZE, "%.17lf", (i * (1 == i%2 ? 1 : -1)) * pow(10, 20));
      num1.from_v1(tmp_buf, strlen(tmp_buf), allocator);
      for (int64_t z = -(j+2); z < j + 2; z++) {
        num1.round_v1(z);
      }
      allocator.free();
      if (i > end_value) {
        i -= (end_value - begin_value);
      }
    }
    get_range_cost = ObTimeUtility::current_time() - get_range_beg - base_get_range_cost;
    _OB_LOG(INFO, "rould_old, cost time: %f us", (double)get_range_cost / MAX_TEST_COUNT);

    get_range_beg = ObTimeUtility::current_time();
    for (int64_t i = begin_value, k = 0; k < MAX_TEST_COUNT; ++i, ++k) {
      char tmp_buf[MAX_BUF_SIZE];
      snprintf(tmp_buf, MAX_BUF_SIZE, "%.17lf", (i * (1 == i%2 ? 1 : -1)) * pow(10, 20));
      num1.from_v1(tmp_buf, strlen(tmp_buf), allocator);
      for (int64_t z = -(j+2); z < j + 2; z++) {
        num1.round_v2(z);
      }
      allocator.free();
      if (i > end_value) {
        i -= (end_value - begin_value);
      }
    }
    get_range_cost = ObTimeUtility::current_time() - get_range_beg - base_get_range_cost;
    _OB_LOG(INFO, "round_v2, cost time: %f us", (double)get_range_cost / MAX_TEST_COUNT);

    get_range_beg = ObTimeUtility::current_time();
    for (int64_t i = begin_value, k = 0; k < MAX_TEST_COUNT; ++i, ++k) {
      char tmp_buf[MAX_BUF_SIZE];
      snprintf(tmp_buf, MAX_BUF_SIZE, "%.17lf", (i * (1 == i%2 ? 1 : -1)) * pow(10, 20));
      num1.from_v1(tmp_buf, strlen(tmp_buf), allocator);
      for (int64_t z = -(j+2); z < j + 2; z++) {
        num1.round_v3(z);
      }
      allocator.free();
      if (i > end_value) {
        i -= (end_value - begin_value);
      }
    }
    get_range_cost = ObTimeUtility::current_time() - get_range_beg - base_get_range_cost;
    _OB_LOG(INFO, "round_v3, cost time: %f us", (double)get_range_cost / MAX_TEST_COUNT);
  }
}


TEST(ObNumber, compare_number_cmp)
{
  const int64_t MAX_TEST_COUNT  = 100;
  const int64_t MAX_BUF_SIZE = 256;
  const int64_t MAX_TEST_LOOP  = 10;

  char buf_alloc[MAX_BUF_SIZE];
  int64_t pos = 0;
  int64_t get_range_beg = 0;
  int64_t get_range_cost = 0;
  int64_t begin_value = 0;
  int64_t end_value   = 0;
  ObDataBuffer allocator(buf_alloc, MAX_BUF_SIZE);
  const int64_t MAX_DOUBLE_STRICT_PRINT_SIZE2 = 512;
  char buf[MAX_DOUBLE_STRICT_PRINT_SIZE2] = {};
  int64_t length = 0 ;

  int64_t arr[MAX_TEST_COUNT];
  for (int i = 0; i < MAX_TEST_COUNT; i++) {
    arr[i] = rand();
  }

  number::ObNumber num1;
  number::ObNumber num2;
  for (int l = 0; l < MAX_TEST_LOOP; l++) {
    for (int i = 0; i < MAX_TEST_COUNT; i++) {
      for (int j = 0; j < MAX_TEST_COUNT; j++) {
        EXPECT_EQ(OB_SUCCESS, num1.from(arr[i] * (1 == l%2 ? 1 : -1), allocator));
        EXPECT_EQ(OB_SUCCESS, num2.from(arr[j] * (1 == l%2 ? 1 : -1), allocator));
        EXPECT_EQ(num1.compare_v1(num2), num1.compare(num2));
        allocator.free();
      }
    }
  }

  double darr[MAX_TEST_COUNT];
  for (int i = 0; i < MAX_TEST_COUNT; i++) {
    darr[i] = drand48() * 1000000000000000000;
  }

  for (int l = 0; l < MAX_TEST_LOOP; l++) {
    for (int i = 0; i < MAX_TEST_COUNT; i++) {
      for (int j = 0; j < MAX_TEST_COUNT; j++) {
        MEMSET(buf, 0, MAX_DOUBLE_STRICT_PRINT_SIZE2);
        length = ob_gcvt_strict(darr[i] * (1 == l%2 ? 1 : -1), OB_GCVT_ARG_DOUBLE, sizeof(buf) - 1, buf, NULL, TRUE, TRUE, FALSE);
        EXPECT_EQ(OB_SUCCESS, num1.from(buf, length, allocator));

        ObString str(sizeof(buf), static_cast<int32_t>(length), buf);
        OB_LOG(DEBUG, "debug jianhua 1", K(darr[i]), K(str));

        MEMSET(buf, 0, MAX_DOUBLE_STRICT_PRINT_SIZE2);
        length = ob_gcvt_strict(darr[j] * (1 == l%2 ? 1 : -1), OB_GCVT_ARG_DOUBLE, sizeof(buf) - 1, buf, NULL, TRUE, TRUE, FALSE);
        EXPECT_EQ(OB_SUCCESS, num2.from(buf, length, allocator));

        ObString str2(sizeof(buf), static_cast<int32_t>(length), buf);
        OB_LOG(DEBUG, "debug jianhua 2", K(darr[i]), K(str2));

        EXPECT_EQ(num1.compare_v1(num2), num1.compare(num2));
        allocator.free();
      }
    }
  }
}

#define ASSERT_FORMAT_STREQ(n1, n2) ASSERT_STREQ(format(n1, 45), format(n2, 45))

TEST(ObNumber, sin_cos_tan)
{
  int ret = OB_SUCCESS;
  const int64_t MAX_TEST_COUNT  = 100;
  const int64_t MAX_BUF_SIZE = 256;
  char const_buf_alloc[MAX_BUF_SIZE];
  ObDataBuffer const_allocator(const_buf_alloc, MAX_BUF_SIZE);
  ObNumber const_point_five;
  ret = const_point_five.from("0.5", const_allocator);
  EXPECT_EQ(OB_SUCCESS, ret);
  char buf_alloc[MAX_BUF_SIZE];
  char buf_alloc2[MAX_BUF_SIZE];
  int64_t tmp = 0;
  ObDataBuffer allocator(buf_alloc, MAX_BUF_SIZE);
  enum TestType { TT_MIN = 0, TT_MAX, TT_RAND, TT_MAX_TYPE };
  uint32_t data[TestType::TT_MAX_TYPE][MAX_TEST_COUNT];
  for (int64_t i = 0; i < MAX_TEST_COUNT; ++i) {
    data[TestType::TT_MIN][i] = 3;
    data[TestType::TT_MAX][i] = ObNumber::BASE - 1;
    data[TestType::TT_RAND][i] = (rand() % (ObNumber::BASE - 3)) + 3;
  }
  uint32_t digits1[ObNumber::MAX_STORE_LEN];
  uint32_t digits2[ObNumber::MAX_STORE_LEN];
  for (int64_t k = 0; k < TestType::TT_MAX_TYPE; ++k) {
    for (int64_t i = 1; i <= ObNumber::MAX_STORE_LEN; ++i) {
      _OB_LOG(INFO, "test numer k=%ld, i=%ld", k, i);
      for (int64_t j = 0; j < MAX_TEST_COUNT; ++j) {
        INIT_NUMBER12;

        ObNumber value_v1, value_v2, value_v3;
        // sin/cos == tan
        EXPECT_EQ(OB_SUCCESS, num1.sin(value_v1, allocator));
        EXPECT_EQ(OB_SUCCESS, num1.cos(value_v2, allocator));
        EXPECT_EQ(OB_SUCCESS, value_v1.div_v3(value_v2, value_v3, allocator));
        EXPECT_EQ(OB_SUCCESS, num1.tan(value_v1, allocator));
        ASSERT_FORMAT_STREQ(value_v1, value_v3);
        allocator.free();

        // sin^2 + cos^2 = 1
        EXPECT_EQ(OB_SUCCESS, num1.sin(value_v1, allocator));
        EXPECT_EQ(OB_SUCCESS, num1.cos(value_v2, allocator));
        EXPECT_EQ(OB_SUCCESS, value_v1.mul_v3(value_v1, value_v1, allocator));
        EXPECT_EQ(OB_SUCCESS, value_v2.mul_v3(value_v2, value_v2, allocator));
        EXPECT_EQ(OB_SUCCESS, value_v1.add_v3(value_v2, value_v1, allocator));
        EXPECT_EQ(OB_SUCCESS, value_v2.from(ObNumber::get_positive_one(), allocator));
        ASSERT_FORMAT_STREQ(value_v1, value_v2);
        allocator.free();

        // sin(-x) = -sin(x)
        EXPECT_EQ(OB_SUCCESS, num1.sin(value_v1, allocator));
        EXPECT_EQ(OB_SUCCESS, num1.negate(value_v2, allocator));
        EXPECT_EQ(OB_SUCCESS, value_v2.sin(value_v2, allocator));
        EXPECT_EQ(OB_SUCCESS, value_v2.negate(value_v2, allocator));
        ASSERT_FORMAT_STREQ(value_v1, value_v2);
        allocator.free();

        // cos(-x) = cos(x)
        EXPECT_EQ(OB_SUCCESS, num1.cos(value_v1, allocator));
        EXPECT_EQ(OB_SUCCESS, num1.negate(value_v2, allocator));
        EXPECT_EQ(OB_SUCCESS, value_v2.cos(value_v2, allocator));
        ASSERT_FORMAT_STREQ(value_v1, value_v2);
        allocator.free();

        // tan(-x) = tan(x)
        EXPECT_EQ(OB_SUCCESS, num1.tan(value_v1, allocator));
        EXPECT_EQ(OB_SUCCESS, num1.negate(value_v2, allocator));
        EXPECT_EQ(OB_SUCCESS, value_v2.tan(value_v2, allocator));
        EXPECT_EQ(OB_SUCCESS, value_v2.negate(value_v2, allocator));
        ASSERT_FORMAT_STREQ(value_v1, value_v2);
        allocator.free();
      }
    }
  }
}

TEST(ObNumber, number_round_remainder)
{
  const int64_t MAX_TEST_COUNT  = 10000;
  const int64_t MAX_BUF_SIZE = 256;

  char buf_alloc[MAX_BUF_SIZE];
  ObDataBuffer allocator(buf_alloc, MAX_BUF_SIZE);
  int64_t tmp = 0;
  const int64_t MAX_DOUBLE_STRICT_PRINT_SIZE2 = 512;
  char buf[MAX_DOUBLE_STRICT_PRINT_SIZE2] = {};
  int64_t length = 0 ;

  enum TestType { TT_MIN = 0, TT_MAX, TT_RAND, TT_MAX_TYPE };
  uint32_t data[TestType::TT_MAX_TYPE][MAX_TEST_COUNT];
  for (int i = 0; i < MAX_TEST_COUNT; ++i) {
    data[TestType::TT_MIN][i] = rand() % 100;
    data[TestType::TT_MAX][i] = rand() % 100 + ObNumber::BASE - 120;
    data[TestType::TT_RAND][i] = rand() % ObNumber::BASE;
  }

  uint32_t digits1[ObNumber::MAX_STORE_LEN];
  uint32_t digits2[ObNumber::MAX_STORE_LEN];
  for (int64_t k = 0; k < TestType::TT_MAX_TYPE; ++k) {
    for (int64_t i = 1; i <= ObNumber::MAX_STORE_LEN; ++i) {
      OB_LOG(INFO, "debug jianhua 1", K(k), K(i));
      for (int64_t j = 0; j < MAX_TEST_COUNT; ++j) {
        INIT_NUMBER12;
        ObNumber value;
        ASSERT_EQ(OB_SUCCESS, num1.round_remainder(num2, value, allocator));
        ASSERT_EQ(OB_SUCCESS, value.add(value, value, allocator));
        ASSERT_LE(value.abs_compare(num2),  0);
        allocator.free();
      }
    }
  }
}

TEST(ObNumber, round_even_number)
{
  const int64_t MAX_TEST_COUNT  = 10000;
  const int64_t MAX_BUF_SIZE = 256;
  const int64_t MAX_TEST_LOOP  = 10;

  char buf_alloc[MAX_BUF_SIZE];
  ObDataBuffer allocator(buf_alloc, MAX_BUF_SIZE);
  const int64_t MAX_DOUBLE_STRICT_PRINT_SIZE2 = 512;
  char buf[MAX_DOUBLE_STRICT_PRINT_SIZE2] = {};
  int64_t length = 0 ;

  number::ObNumber num1;
  number::ObNumber num2;

  double darr[MAX_TEST_COUNT];
  for (int i = 0; i < MAX_TEST_COUNT; i++) {
    darr[i] = drand48() * pow(10.0,i % 16);
  }

  for (int l = 0; l < MAX_TEST_LOOP; l++) {
    for (int i = 0; i < MAX_TEST_COUNT; i++) {
        MEMSET(buf, 0, MAX_DOUBLE_STRICT_PRINT_SIZE2);
        double tmp_darr = darr[i] * (1 == l%2 ? 1 : -1);
        length = ob_gcvt_strict(tmp_darr, OB_GCVT_ARG_DOUBLE, sizeof(buf) - 1, buf, NULL, TRUE, TRUE, FALSE);
        EXPECT_EQ(OB_SUCCESS, num1.from(buf, length, allocator));

//        int err = 0;
//        char *endptr = NULL;
        ObString str(sizeof(buf), static_cast<int32_t>(length), buf);
//        double tmp_darr = ObCharset::strntod(str.ptr(), str.length(), &endptr, &err);
        OB_LOG(DEBUG, "debug shanting 1", K(tmp_darr), K(num1), K(str), K(length));

        EXPECT_EQ(OB_SUCCESS, num1.round_even_number());
        double num = tmp_darr > 0 ? tmp_darr : - tmp_darr;
        int64_t integer = (int64_t)num;
        double rem = num - integer;
        int64_t res = (int64_t)integer;
        if (rem > 0.5 + DBL_EPSILON || (rem < 0.5 + DBL_EPSILON && rem > 0.5 - DBL_EPSILON && 1 == (integer % 2))) {
          res++;
        }
        res *= (tmp_darr > 0 ? 1 : -1);
        OB_LOG(DEBUG, "debug shanting 2", K(tmp_darr), K(res), K(num1), K(str));

        EXPECT_EQ(OB_SUCCESS, num2.from(res, allocator));
        ASSERT_EQ(0,num1.compare(num2));
        allocator.free();
    }
  }
}

TEST(ObNumber, DISABLED_compare_number_perf)
{
  const int64_t MAX_TEST_COUNT  = 200;
  const int64_t MAX_BUF_SIZE = 256;
  const int64_t MAX_TEST_LOOP  = 10;

  char buf_alloc[MAX_BUF_SIZE];
  int64_t pos = 0;
  int64_t get_range_beg = 0;
  int64_t get_range_cost = 0;
  int64_t base_get_range_cost = 0;
  int64_t begin_value = 0;
  int64_t end_value   = 0;
  int64_t sum = 0;
  ObDataBuffer allocator(buf_alloc, MAX_BUF_SIZE);
  const int64_t MAX_DOUBLE_STRICT_PRINT_SIZE2 = 512;
  char buf[MAX_DOUBLE_STRICT_PRINT_SIZE2] = {};
  int64_t length = 0;

  int64_t arr[MAX_TEST_COUNT];
  for (int i = 0; i < MAX_TEST_COUNT; i++) {
    arr[i] = rand();
  }

  get_range_beg = ObTimeUtility::current_time();
  number::ObNumber num1;
  number::ObNumber num2;
  for (int l = 0; l < MAX_TEST_LOOP; l++) {
    for (int i = 0; i < MAX_TEST_COUNT; i++) {
      for (int j = 0; j < MAX_TEST_COUNT; j++) {
        EXPECT_EQ(OB_SUCCESS, num1.from(arr[i] * (1 == l%2 ? 1 : -1), allocator));
        EXPECT_EQ(OB_SUCCESS, num2.from(arr[j] * (1 == l%2 ? 1 : -1), allocator));
        sum += j;
        allocator.free();
      }
    }
  }
  base_get_range_cost = ObTimeUtility::current_time() - get_range_beg;
  _OB_LOG(INFO, "compare base, cost time: %f us, sum=%ld", (double)base_get_range_cost / (MAX_TEST_COUNT * MAX_TEST_COUNT), sum);

  get_range_beg = ObTimeUtility::current_time();
  for (int l = 0; l < MAX_TEST_LOOP; l++) {
    for (int i = 0; i < MAX_TEST_COUNT; i++) {
      for (int j = 0; j < MAX_TEST_COUNT; j++) {
        EXPECT_EQ(OB_SUCCESS, num1.from(arr[i] * (1 == l%2 ? 1 : -1), allocator));
        EXPECT_EQ(OB_SUCCESS, num2.from(arr[j] * (1 == l%2 ? 1 : -1), allocator));
        sum += num1.compare_v1(num2);
        allocator.free();
      }
    }
  }
  get_range_cost = ObTimeUtility::current_time() - get_range_beg - base_get_range_cost;
  _OB_LOG(INFO, "compare int, cost time: %f us, sum=%ld", (double)get_range_cost / (MAX_TEST_COUNT * MAX_TEST_COUNT), sum);

  sum = 0;
  get_range_beg = ObTimeUtility::current_time();
  for (int l = 0; l < MAX_TEST_LOOP; l++) {
    for (int i = 0; i < MAX_TEST_COUNT; i++) {
      for (int j = 0; j < MAX_TEST_COUNT; j++) {
        EXPECT_EQ(OB_SUCCESS, num1.from(arr[i] * (1 == l%2 ? 1 : -1), allocator));
        EXPECT_EQ(OB_SUCCESS, num2.from(arr[j] * (1 == l%2 ? 1 : -1), allocator));
        sum += num1.compare(num2);
        allocator.free();
      }
    }
  }
  get_range_cost = ObTimeUtility::current_time() - get_range_beg - base_get_range_cost;
  _OB_LOG(INFO, "compare_v2 int, cost time: %f us, sum=%ld", (double)get_range_cost / (MAX_TEST_COUNT * MAX_TEST_COUNT), sum);


  double darr[MAX_TEST_COUNT];
  for (int i = 0; i < MAX_TEST_COUNT; i++) {
    darr[i] = drand48() * 1000000000000000000;
  }


  sum = 0;
  get_range_beg = ObTimeUtility::current_time();
  for (int l = 0; l < MAX_TEST_LOOP; l++) {
    for (int i = 0; i < MAX_TEST_COUNT; i++) {
      for (int j = 0; j < MAX_TEST_COUNT; j++) {
        MEMSET(buf, 0, MAX_DOUBLE_STRICT_PRINT_SIZE2);
        length = ob_gcvt_strict(darr[i] * (1 == l%2 ? 1 : -1), OB_GCVT_ARG_DOUBLE, sizeof(buf) - 1, buf, NULL, TRUE, TRUE, FALSE);
        EXPECT_EQ(OB_SUCCESS, num1.from(buf, length, allocator));

        MEMSET(buf, 0, MAX_DOUBLE_STRICT_PRINT_SIZE2);
        length = ob_gcvt_strict(darr[j] * (1 == l%2 ? 1 : -1), OB_GCVT_ARG_DOUBLE, sizeof(buf) - 1, buf, NULL, TRUE, TRUE, FALSE);
        EXPECT_EQ(OB_SUCCESS, num2.from(buf, length, allocator));
        sum += j;
        allocator.free();
      }
    }
  }
  base_get_range_cost = ObTimeUtility::current_time() - get_range_beg;
  _OB_LOG(INFO, "compare base, cost time: %f us, sum=%ld", (double)base_get_range_cost / (MAX_TEST_COUNT * MAX_TEST_COUNT), sum);

  sum = 0;
  get_range_beg = ObTimeUtility::current_time();
  for (int l = 0; l < MAX_TEST_LOOP; l++) {
    for (int i = 0; i < MAX_TEST_COUNT; i++) {
      for (int j = 0; j < MAX_TEST_COUNT; j++) {
        MEMSET(buf, 0, MAX_DOUBLE_STRICT_PRINT_SIZE2);
        length = ob_gcvt_strict(darr[i] * (1 == l%2 ? 1 : -1), OB_GCVT_ARG_DOUBLE, sizeof(buf) - 1, buf, NULL, TRUE, TRUE, FALSE);
        EXPECT_EQ(OB_SUCCESS, num1.from(buf, length, allocator));

        MEMSET(buf, 0, MAX_DOUBLE_STRICT_PRINT_SIZE2);
        length = ob_gcvt_strict(darr[j] * (1 == l%2 ? 1 : -1), OB_GCVT_ARG_DOUBLE, sizeof(buf) - 1, buf, NULL, TRUE, TRUE, FALSE);
        EXPECT_EQ(OB_SUCCESS, num2.from(buf, length, allocator));

        sum += num1.compare_v1(num2);
        allocator.free();
      }
    }
  }
  get_range_cost = ObTimeUtility::current_time() - get_range_beg - base_get_range_cost;
  _OB_LOG(INFO, "compare double, cost time: %f us, sum=%ld", (double)get_range_cost / (MAX_TEST_COUNT * MAX_TEST_COUNT), sum);


  sum = 0;
  get_range_beg = ObTimeUtility::current_time();
  for (int l = 0; l < MAX_TEST_LOOP; l++) {
    for (int i = 0; i < MAX_TEST_COUNT; i++) {
      for (int j = 0; j < MAX_TEST_COUNT; j++) {
        MEMSET(buf, 0, MAX_DOUBLE_STRICT_PRINT_SIZE2);
        length = ob_gcvt_strict(darr[i] * (1 == l%2 ? 1 : -1), OB_GCVT_ARG_DOUBLE, sizeof(buf) - 1, buf, NULL, TRUE, TRUE, FALSE);
        EXPECT_EQ(OB_SUCCESS, num1.from(buf, length, allocator));

        MEMSET(buf, 0, MAX_DOUBLE_STRICT_PRINT_SIZE2);
        length = ob_gcvt_strict(darr[j] * (1 == l%2 ? 1 : -1), OB_GCVT_ARG_DOUBLE, sizeof(buf) - 1, buf, NULL, TRUE, TRUE, FALSE);
        EXPECT_EQ(OB_SUCCESS, num2.from(buf, length, allocator));

        sum += num1.compare(num2);
        allocator.free();
      }
    }
  }
  get_range_cost = ObTimeUtility::current_time() - get_range_beg - base_get_range_cost;
  _OB_LOG(INFO, "compare_v2 double, cost time: %f us, sum=%ld", (double)get_range_cost / (MAX_TEST_COUNT * MAX_TEST_COUNT), sum);
}


TEST(ObNumber, encode_decode)
{
  const int64_t MAX_TEST_COUNT  = 100;
  const int64_t MAX_BUF_SIZE = 256;
  const int64_t MAX_TEST_LOOP  = 10;

  char buf_alloc[MAX_BUF_SIZE];
  int64_t pos = 0;
  int64_t get_range_beg = 0;
  int64_t get_range_cost = 0;
  int64_t begin_value = 0;
  int64_t end_value   = 0;
  ObDataBuffer allocator(buf_alloc, MAX_BUF_SIZE);
  const int64_t MAX_DOUBLE_STRICT_PRINT_SIZE2 = 512;
  char buf[MAX_DOUBLE_STRICT_PRINT_SIZE2] = {};
  int64_t length = 0 ;

  int64_t arr[MAX_TEST_COUNT];
  for (int i = 0; i < MAX_TEST_COUNT; i++) {
    arr[i] = rand();
  }

  number::ObNumber num1;
  number::ObNumber num2;
  for (int l = 0; l < MAX_TEST_LOOP; l++) {
    for (int i = 0; i < MAX_TEST_COUNT; i++) {
      for (int j = 0; j < MAX_TEST_COUNT; j++) {
        EXPECT_EQ(OB_SUCCESS, num1.from(arr[i] * (1 == l%2 ? 1 : -1), allocator));
        pos = 0 ;
        EXPECT_EQ(OB_SUCCESS, num1.encode(buf, MAX_DOUBLE_STRICT_PRINT_SIZE2, pos));
        pos = 0 ;
        EXPECT_EQ(OB_SUCCESS, num2.decode(buf, MAX_DOUBLE_STRICT_PRINT_SIZE2, pos));
        ASSERT_EQ(0, num1.compare_v1(num2));
        allocator.free();
      }
    }
  }

  double darr[MAX_TEST_COUNT];
  for (int i = 0; i < MAX_TEST_COUNT; i++) {
    darr[i] = drand48() * 1000000000000000000;
  }

  for (int l = 0; l < MAX_TEST_LOOP; l++) {
    for (int i = 0; i < MAX_TEST_COUNT; i++) {
      for (int j = 0; j < MAX_TEST_COUNT; j++) {
        MEMSET(buf, 0, MAX_DOUBLE_STRICT_PRINT_SIZE2);
        length = ob_gcvt_strict(darr[i] * (1 == l%2 ? 1 : -1), OB_GCVT_ARG_DOUBLE, sizeof(buf) - 1, buf, NULL, TRUE, TRUE, FALSE);
        EXPECT_EQ(OB_SUCCESS, num1.from(buf, length, allocator));

        pos = 0 ;
        EXPECT_EQ(OB_SUCCESS, num1.encode(buf, MAX_DOUBLE_STRICT_PRINT_SIZE2, pos));
        pos = 0 ;
        EXPECT_EQ(OB_SUCCESS, num2.decode(buf, MAX_DOUBLE_STRICT_PRINT_SIZE2, pos));
        ASSERT_EQ(0, num1.compare_v1(num2));
        allocator.free();
      }
    }
  }
}

TEST(ObNumber, int64_conversion)
{
  const int64_t MAX_BUF_SIZE = 256;
  char buf_alloc[MAX_BUF_SIZE];
  ObDataBuffer allocator(buf_alloc, MAX_BUF_SIZE);
  int64_t from_int = 0;
  int64_t to_int = 0;
  number::ObNumber num;

  // int64_t boundary test [-9,223,372,036,854,775,808, 9,223,372,036,854,775,807]
  allocator.free();
  from_int = 9223372036854775806;
  ASSERT_EQ(OB_SUCCESS, num.from("9223372036854775806", allocator));
  ASSERT_TRUE(num.is_int64());
  ASSERT_EQ(OB_SUCCESS, num.cast_to_int64(to_int));
  ASSERT_EQ(from_int, to_int);

  allocator.free();
  from_int = 9223372036854775807;
  ASSERT_EQ(OB_SUCCESS, num.from("9223372036854775807", allocator));
  ASSERT_TRUE(num.is_int64());
  ASSERT_EQ(OB_SUCCESS, num.cast_to_int64(to_int));
  ASSERT_EQ(from_int, to_int);

  allocator.free();
  ASSERT_EQ(OB_SUCCESS, num.from("9223372036854775808", allocator));
  ASSERT_FALSE(num.is_int64());
  ASSERT_EQ(OB_INTEGER_PRECISION_OVERFLOW, num.cast_to_int64(to_int));

  allocator.free();
  from_int = -9223372036854775807;
  ASSERT_EQ(OB_SUCCESS, num.from("-9223372036854775807", allocator));
  ASSERT_TRUE(num.is_int64());
  ASSERT_EQ(OB_SUCCESS, num.cast_to_int64(to_int));
  ASSERT_EQ(from_int, to_int);

  allocator.free();
  from_int = INT64_MIN;
  ASSERT_EQ(OB_SUCCESS, num.from("-9223372036854775808", allocator));
  ASSERT_TRUE(num.is_int64());
  ASSERT_EQ(OB_SUCCESS, num.cast_to_int64(to_int));
  ASSERT_EQ(from_int, to_int);

  allocator.free();
  ASSERT_EQ(OB_SUCCESS, num.from("-9223372036854775809", allocator));
  ASSERT_FALSE(num.is_int64());
  ASSERT_EQ(OB_INTEGER_PRECISION_OVERFLOW, num.cast_to_int64(to_int));

  // 0 value test
  for (from_int = -1; from_int <= 1; from_int++) {
    allocator.free();
    ASSERT_EQ(OB_SUCCESS, num.from(from_int, allocator));
    ASSERT_TRUE(num.is_int64());
    ASSERT_EQ(OB_SUCCESS, num.cast_to_int64(to_int));
    ASSERT_EQ(from_int, to_int);
  }

  // digital array boundary
  allocator.free();
  from_int = 999999999;
  ASSERT_EQ(OB_SUCCESS, num.from(from_int, allocator));
  ASSERT_TRUE(num.is_int64());
  ASSERT_EQ(OB_SUCCESS, num.cast_to_int64(to_int));
  ASSERT_EQ(from_int, to_int);

  allocator.free();
  from_int = -999999999;
  ASSERT_EQ(OB_SUCCESS, num.from(from_int, allocator));
  ASSERT_TRUE(num.is_int64());
  ASSERT_EQ(OB_SUCCESS, num.cast_to_int64(to_int));
  ASSERT_EQ(from_int, to_int);

  allocator.free();
  from_int = 1000000000;
  ASSERT_EQ(OB_SUCCESS, num.from(from_int, allocator));
  ASSERT_TRUE(num.is_int64());
  ASSERT_EQ(OB_SUCCESS, num.cast_to_int64(to_int));
  ASSERT_EQ(from_int, to_int);

  allocator.free();
  from_int = -1000000000;
  ASSERT_EQ(OB_SUCCESS, num.from(from_int, allocator));
  ASSERT_TRUE(num.is_int64());
  ASSERT_EQ(OB_SUCCESS, num.cast_to_int64(to_int));
  ASSERT_EQ(from_int, to_int);

  allocator.free();
  from_int = 999999999999999999;
  ASSERT_EQ(OB_SUCCESS, num.from(from_int, allocator));
  ASSERT_TRUE(num.is_int64());
  ASSERT_EQ(OB_SUCCESS, num.cast_to_int64(to_int));
  ASSERT_EQ(from_int, to_int);

  allocator.free();
  from_int = -999999999999999999;
  ASSERT_EQ(OB_SUCCESS, num.from(from_int, allocator));
  ASSERT_TRUE(num.is_int64());
  ASSERT_EQ(OB_SUCCESS, num.cast_to_int64(to_int));
  ASSERT_EQ(from_int, to_int);

  allocator.free();
  from_int = 1000000000000000000;
  ASSERT_EQ(OB_SUCCESS, num.from(from_int, allocator));
  ASSERT_TRUE(num.is_int64());
  ASSERT_EQ(OB_SUCCESS, num.cast_to_int64(to_int));
  ASSERT_EQ(from_int, to_int);

  allocator.free();
  from_int = -1000000000000000000;
  ASSERT_EQ(OB_SUCCESS, num.from(from_int, allocator));
  ASSERT_TRUE(num.is_int64());
  ASSERT_EQ(OB_SUCCESS, num.cast_to_int64(to_int));
  ASSERT_EQ(from_int, to_int);

  // decimal test
  allocator.free();
  ASSERT_EQ(OB_SUCCESS, num.from("0.0", allocator));
  ASSERT_TRUE(num.is_int64());
  ASSERT_EQ(OB_SUCCESS, num.cast_to_int64(to_int));
  ASSERT_EQ(0, to_int);

  allocator.free();
  ASSERT_EQ(OB_SUCCESS, num.from("0.001", allocator));
  ASSERT_FALSE(num.is_int64());
  ASSERT_EQ(OB_INTEGER_PRECISION_OVERFLOW, num.cast_to_int64(to_int));

  // overflow test
  // middle digit greater then 223372036
  allocator.free();
  ASSERT_EQ(OB_SUCCESS, num.from("-9223372037000000000", allocator));
  ASSERT_FALSE(num.is_int64());
  ASSERT_EQ(OB_INTEGER_PRECISION_OVERFLOW, num.cast_to_int64(to_int));

  // max digit greater then 9
  allocator.free();
  ASSERT_EQ(OB_SUCCESS, num.from("10223372035000000000", allocator));
  ASSERT_FALSE(num.is_int64());
  ASSERT_EQ(OB_INTEGER_PRECISION_OVERFLOW, num.cast_to_int64(to_int));
}

TEST(ObNumber, arithmetic_cmp)
{
  const int64_t MAX_TEST_COUNT  = 100;
  const int64_t MAX_TEST2_COUNT  = 1000;
  const int64_t MAX_BUF_SIZE = 256;

  char buf_alloc[MAX_BUF_SIZE];
  char buf_alloc2[MAX_BUF_SIZE];
  int64_t pos = 0;
  int64_t get_range_beg = 0;
  int64_t get_range_cost = 0;
  int64_t begin_value = 0;
  int64_t end_value   = 0;
  ObDataBuffer allocator(buf_alloc, MAX_BUF_SIZE);
  ObDataBuffer allocator2(buf_alloc2, MAX_BUF_SIZE);
  const int64_t MAX_DOUBLE_STRICT_PRINT_SIZE2 = 512;
  char buf[MAX_DOUBLE_STRICT_PRINT_SIZE2] = {};
  int64_t length = 0 ;

  int64_t arr[MAX_TEST2_COUNT];
  for (int i = 0; i < MAX_TEST2_COUNT; i++) {
    arr[i] = rand();
  }

  double darr[MAX_TEST2_COUNT];
  for (int i = 0; i < MAX_TEST2_COUNT; i++) {
    darr[i] = drand48() * 1000000000000000000;
  }

  allocator.free();
  number::ObNumber num1;
  int64_t tmp_int64 = 0;
  ASSERT_EQ(OB_SUCCESS, num1.from("9223372036854775807", allocator));
  ASSERT_EQ(OB_SUCCESS, num1.from("-9223372036854775808", allocator));
  ASSERT_EQ(OB_SUCCESS, num1.from("9223372036854775808", allocator));
  ASSERT_EQ(OB_SUCCESS, num1.from("9223372036854775808", allocator));
  allocator.free();
  ASSERT_EQ(OB_INTEGER_PRECISION_OVERFLOW, num1.cast_to_int64(tmp_int64));
  ASSERT_EQ(OB_SUCCESS, num1.from("-9223372036854775809", allocator));
  ASSERT_EQ(OB_INTEGER_PRECISION_OVERFLOW, num1.cast_to_int64(tmp_int64));
  ASSERT_EQ(OB_SUCCESS, num1.from("9999999999000000000", allocator));
  ASSERT_EQ(OB_INTEGER_PRECISION_OVERFLOW, num1.cast_to_int64(tmp_int64));
  ASSERT_EQ(OB_SUCCESS, num1.from("9999999999999999999", allocator));
  ASSERT_EQ(OB_INTEGER_PRECISION_OVERFLOW, num1.cast_to_int64(tmp_int64));
  ASSERT_EQ(OB_SUCCESS, num1.from("-9999999999000000000", allocator));
  ASSERT_EQ(OB_INTEGER_PRECISION_OVERFLOW, num1.cast_to_int64(tmp_int64));
  ASSERT_EQ(OB_SUCCESS, num1.from("-9999999999999999999", allocator));
  ASSERT_EQ(OB_INTEGER_PRECISION_OVERFLOW, num1.cast_to_int64(tmp_int64));
  ASSERT_EQ(OB_SUCCESS, num1.from("1111111111111111111111111", allocator));
  ASSERT_EQ(OB_INTEGER_PRECISION_OVERFLOW, num1.cast_to_int64(tmp_int64));
  ASSERT_EQ(OB_SUCCESS, num1.from("-1111111111111111111111111", allocator));
  ASSERT_EQ(OB_INTEGER_PRECISION_OVERFLOW, num1.cast_to_int64(tmp_int64));
  ASSERT_EQ(OB_SUCCESS, num1.from("1111111000000000000000000", allocator));
  ASSERT_EQ(OB_INTEGER_PRECISION_OVERFLOW, num1.cast_to_int64(tmp_int64));
  ASSERT_EQ(OB_SUCCESS, num1.from("-1111111000000000000000000", allocator));
  ASSERT_EQ(OB_INTEGER_PRECISION_OVERFLOW, num1.cast_to_int64(tmp_int64));

  for (int64_t j = 1; j < 20; ++j) {
    begin_value = pow(10, j);
    end_value = pow(10, j + 1) - 1;

    number::ObNumber num1;
    number::ObNumber num2;
    int64_t add_step = (end_value - begin_value) / MAX_TEST_COUNT;
    if (add_step < 17) {
      add_step = 17;
    }
    _OB_LOG(INFO, "test numer %ld, [%ld, %ld], add_step=%ld", j + 1, begin_value, end_value, add_step);

    for (int64_t i = begin_value, k = 0; k < MAX_TEST_COUNT && i < end_value - begin_value; i += add_step, ++k) {
      int64_t tmp_i = i * (1 == i%2 ? 1 : -1);
      num1.from(tmp_i, allocator);
      _OB_LOG(INFO, "debug jianhua, j=%ld, i=%ld, k=%ld", j ,i, k);

      int64_t tmp_v1 = 0;
      if (tmp_i <=  INT64_MAX && tmp_i >= INT64_MIN) {
        EXPECT_EQ(OB_SUCCESS, num1.cast_to_int64(tmp_v1));
        ASSERT_EQ(tmp_i, tmp_v1);
      }

      for (int d = 0; d < MAX_TEST2_COUNT; d++) {
        const int64_t test_rd = (arr[d] * (1 == d%2 ? 1 : -1) % begin_value) + tmp_i;
        num2.from(test_rd, allocator2);
        ObNumber value_old,value_new,value_new2;

        EXPECT_EQ(OB_SUCCESS, num1.add_v1(num2, value_old, allocator2));
        EXPECT_EQ(OB_SUCCESS, num1.add_v2(num2, value_new, allocator2));
        EXPECT_EQ(OB_SUCCESS, num1.add_v3(num2, value_new2, allocator2));
        ASSERT_EQ(0, value_new.compare(value_old));
        ASSERT_EQ(0, value_new2.compare(value_old));
        allocator2.free();

        num2.from(test_rd, allocator2);
        EXPECT_EQ(OB_SUCCESS, num1.sub_v1(num2, value_old, allocator2));
        EXPECT_EQ(OB_SUCCESS, num1.sub_v2(num2, value_new, allocator2));
        EXPECT_EQ(OB_SUCCESS, num1.sub_v3(num2, value_new2, allocator2));
        ASSERT_EQ(0, value_new.compare(value_old));
        ASSERT_EQ(0, value_new2.compare(value_old));
        allocator2.free();


        num2.from(test_rd, allocator2);
        EXPECT_EQ(OB_SUCCESS, num1.mul_v1(num2, value_old, allocator2));
        EXPECT_EQ(OB_SUCCESS, num1.mul_v2(num2, value_new, allocator2));
        EXPECT_EQ(OB_SUCCESS, num1.mul_v3(num2, value_new2, allocator2));
        ASSERT_EQ(0, value_new.compare(value_old));
        ASSERT_EQ(0, value_new2.compare(value_old));
        allocator2.free();

        num2.from(test_rd, allocator2);
        if (!num2.is_zero()) {
          //EXPECT_EQ(OB_SUCCESS, num1.div_v1(num2, value_old, allocator2));
          EXPECT_EQ(OB_SUCCESS, num1.div_v2(num2, value_new, allocator2));
          EXPECT_EQ(OB_SUCCESS, num1.div_v3(num2, value_new2, allocator2));
          //ASSERT_EQ(0, value_new.compare(value_old));
          //ASSERT_EQ(0, value_new2.compare(value_old));
          ASSERT_EQ(0, value_new2.compare(value_new));
        }
        allocator2.free();

        num2.from(test_rd, allocator2);
        if (!num2.is_zero()) {
          //EXPECT_EQ(OB_SUCCESS, num1.rem_v1(num2, value_old, allocator2));
          EXPECT_EQ(OB_SUCCESS, num1.rem_v2(num2, value_old, allocator2));
          EXPECT_EQ(OB_SUCCESS, num1.rem_v2(num2, value_new, allocator2));
          ASSERT_EQ(0, value_new.compare(value_old));
        }
        allocator2.free();
      }

      if (i > end_value) {
        i -= (end_value - begin_value);
      }
      allocator.free();
    }
  }

  for (int64_t j = 1; j < 20; ++j) {
    begin_value = pow(10, j);
    end_value = pow(10, j + 1) - 1;

    number::ObNumber num1;
    number::ObNumber num2;
    int64_t add_step = (end_value - begin_value) / MAX_TEST_COUNT;
    if (add_step < 17) {
      add_step = 17;
    }
    _OB_LOG(INFO, "test numer %ld, [%ld, %ld], add_step=%ld", j + 1, begin_value, end_value, add_step);

    for (int64_t i = begin_value, k = 0; k < MAX_TEST_COUNT && i < end_value - begin_value; i += add_step, ++k) {
      for (int64_t m = 0; m < j; ++m) {
        ObFastFormatInt ffi(i * (1 == i%2 ? 1 : -1));
        *const_cast<char *>(ffi.ptr() + m) = '.';
        SET_OB_LOG_TRACE_MODE();    // prevent printing log
        num1.from_v1(ffi.ptr(), ffi.length(), allocator);
        CANCLE_OB_LOG_TRACE_MODE();    // prevent printing log

        _OB_LOG(INFO, "debug jianhua decimal, j=%ld, i=%ld, k=%ld, ffi=%.*s", j, i, k, static_cast<int>(ffi.length()), ffi.ptr());
        for (int d = 0; d < MAX_TEST2_COUNT; d++) {
          MEMSET(buf, 0, MAX_DOUBLE_STRICT_PRINT_SIZE2);
          double test_rd = darr[d] * (1 == d%2 ? 1 : -1) / begin_value + i * (1 == i%2 ? 1 : -1);
          length = ob_gcvt_strict(test_rd, OB_GCVT_ARG_DOUBLE, sizeof(buf) - 1, buf, NULL, TRUE, TRUE, FALSE);
          ObNumber value_old,value_new,value_new2;

          EXPECT_EQ(OB_SUCCESS, num2.from(buf, length, allocator2));
          EXPECT_EQ(OB_SUCCESS, num1.add_v1(num2, value_old, allocator2));
          EXPECT_EQ(OB_SUCCESS, num1.add_v2(num2, value_new, allocator2));
          EXPECT_EQ(OB_SUCCESS, num1.add_v3(num2, value_new2, allocator2));
          ASSERT_EQ(0, value_new.compare(value_old));
          ASSERT_EQ(0, value_new2.compare(value_old));
          allocator2.free();


          EXPECT_EQ(OB_SUCCESS, num2.from(buf, length, allocator2));
          EXPECT_EQ(OB_SUCCESS, num1.sub_v1(num2, value_old, allocator2));
          EXPECT_EQ(OB_SUCCESS, num1.sub_v2(num2, value_new, allocator2));
          EXPECT_EQ(OB_SUCCESS, num1.sub_v3(num2, value_new2, allocator2));
          ASSERT_EQ(0, value_new.compare(value_old));
          ASSERT_EQ(0, value_new2.compare(value_old));
          allocator2.free();



          EXPECT_EQ(OB_SUCCESS, num2.from(buf, length, allocator2));
          EXPECT_EQ(OB_SUCCESS, num1.mul_v1(num2, value_old, allocator2));
          EXPECT_EQ(OB_SUCCESS, num1.mul_v2(num2, value_new, allocator2));
          EXPECT_EQ(OB_SUCCESS, num1.mul_v3(num2, value_new2, allocator2));
          ASSERT_EQ(0, value_new.compare(value_old));
          ASSERT_EQ(0, value_new2.compare(value_old));
          allocator2.free();

          EXPECT_EQ(OB_SUCCESS, num2.from(buf, length, allocator2));
          if (!num2.is_zero()) {
            //EXPECT_EQ(OB_SUCCESS, num1.div_v1(num2, value_old, allocator2));
            EXPECT_EQ(OB_SUCCESS, num1.div_v2(num2, value_new, allocator2));
            EXPECT_EQ(OB_SUCCESS, num1.div_v3(num2, value_new2, allocator2));
            //ASSERT_EQ(0, value_new.compare(value_old));
            //ASSERT_EQ(0, value_new2.compare(value_old));
            ASSERT_EQ(0, value_new2.compare(value_new));
          }
          allocator2.free();
        }
        allocator.free();
      }

      if (i > end_value) {
        i -= (end_value - begin_value);
      }
    }
  }

  //200
  char const_number_str[] = "98765432109876543210987654321098765432109876543210987654321098765432109876543210987654321098765432109876543210987654321098765432109876543210987654321098765432109876543210987654321098765432109876543210";
  ObString const_number_string(const_number_str);
  for (int64_t j = 1; j < 80; ++j) {
    number::ObNumber num1, num2, value_old,value_new;
    for (int64_t i = 0; i < 11; ++i) {
      _OB_LOG(INFO, "debug jianhua decimal, j=%ld, i=%ld, ffi=%.*s", j, i, static_cast<int>(j), const_number_str + i);

      for (int64_t m = 0; m < 11; ++m) {
        allocator.free();
        SET_OB_LOG_TRACE_MODE();    // prevent printing log
        num1.from_v1(const_number_str + i, j, allocator);
        num2.from_v1(const_number_str + m, j, allocator);
        CANCLE_OB_LOG_TRACE_MODE();    // prevent printing log

        value_old.set_zero();
        value_new.set_zero();
        EXPECT_EQ(num1.add_v1(num2, value_old, allocator2), num1.add_v2(num2, value_new, allocator2));
        ASSERT_EQ(0, value_new.compare(value_old));
        allocator2.free();

        value_old.set_zero();
        value_new.set_zero();
        EXPECT_EQ(num1.add_v1(num2, value_old, allocator2), num1.add_v3(num2, value_new, allocator2, false));
        ASSERT_EQ(0, value_new.compare(value_old));
        allocator2.free();

        value_old.set_zero();
        value_new.set_zero();
        EXPECT_EQ(num1.add_v1(num2, value_old, allocator2), num1.add_v3(num2, value_new, allocator2));
        ASSERT_EQ(0, value_new.compare(value_old));
        allocator2.free();


        value_old.set_zero();
        value_new.set_zero();
        EXPECT_EQ(num1.sub_v1(num2, value_old, allocator2), num1.sub_v2(num2, value_new, allocator2));
        ASSERT_EQ(0, value_new.compare(value_old));
        allocator2.free();

        value_old.set_zero();
        value_new.set_zero();
        EXPECT_EQ(num1.sub_v1(num2, value_old, allocator2), num1.sub_v3(num2, value_new, allocator2, false));
        ASSERT_EQ(0, value_new.compare(value_old));
        allocator2.free();

        value_old.set_zero();
        value_new.set_zero();
        EXPECT_EQ(num1.sub_v1(num2, value_old, allocator2), num1.sub_v3(num2, value_new, allocator2));
        ASSERT_EQ(0, value_new.compare(value_old));
        allocator2.free();

        value_old.set_zero();
        value_new.set_zero();
        EXPECT_EQ(num1.mul_v1(num2, value_old, allocator2), num1.mul_v2(num2, value_new, allocator2));
        ASSERT_EQ(0, value_new.compare(value_old));
        allocator2.free();

        value_old.set_zero();
        value_new.set_zero();
        EXPECT_EQ(num1.mul_v1(num2, value_old, allocator2), num1.mul_v3(num2, value_new, allocator2, false));
        ASSERT_EQ(0, value_new.compare(value_old));
        allocator2.free();

        value_old.set_zero();
        value_new.set_zero();
        EXPECT_EQ(num1.mul_v1(num2, value_old, allocator2), num1.mul_v3(num2, value_new, allocator2));
        ASSERT_EQ(0, value_new.compare(value_old));
        allocator2.free();

        if (!num2.is_zero()) {
          value_old.set_zero();
          value_new.set_zero();
          //EXPECT_EQ(num1.div_v1(num2, value_old, allocator2), num1.div_v2(num2, value_new, allocator2));
          EXPECT_EQ(num1.div_v3(num2, value_old, allocator2), num1.div_v2(num2, value_new, allocator2));
          ASSERT_EQ(0, value_new.compare(value_old));

         //value_old.set_zero();
         //value_new.set_zero();
         //EXPECT_EQ(num1.div_v1(num2, value_old, allocator2), num1.div_v3(num2, value_new, allocator2));
         //ASSERT_EQ(0, value_new.compare(value_old));
        }
        allocator2.free();
      }
    }
  }
}


TEST(ObNumber, arithmetic_cmp_v2)
{
  const int64_t MAX_TEST_COUNT  = 10000;
  const int64_t MAX_BUF_SIZE = 256;

  char buf_alloc[MAX_BUF_SIZE];
  char buf_alloc2[MAX_BUF_SIZE];
  int64_t tmp = 0;
  int64_t pos = 0;
  int64_t get_range_beg = 0;
  int64_t get_range_cost = 0;
  int64_t begin_value = 0;
  int64_t end_value   = 0;
  ObDataBuffer allocator(buf_alloc, MAX_BUF_SIZE);
  const int64_t MAX_DOUBLE_STRICT_PRINT_SIZE2 = 512;
  char buf[MAX_DOUBLE_STRICT_PRINT_SIZE2] = {};
  int64_t length = 0 ;

  enum TestType { TT_MIN = 0, TT_MAX, TT_RAND, TT_MAX_TYPE };
  uint32_t data[TestType::TT_MAX_TYPE][MAX_TEST_COUNT];
  for (int64_t i = 0; i < MAX_TEST_COUNT; ++i) {
    data[TestType::TT_MIN][i] = rand() % 100;
    data[TestType::TT_MAX][i] = rand() % 100 + ObNumber::BASE - 120;
    data[TestType::TT_RAND][i] = rand() % ObNumber::BASE;
  }

  uint32_t digits1[ObNumber::MAX_STORE_LEN];
  uint32_t digits2[ObNumber::MAX_STORE_LEN];
  for (int64_t k = 0; k < TestType::TT_MAX_TYPE; ++k) {
    for (int64_t i = 1; i <= ObNumber::MAX_STORE_LEN; ++i) {
      _OB_LOG(INFO, "test numer k=%ld, i=%ld", k, i);
      for (int64_t j = 0; j < MAX_TEST_COUNT; ++j) {
        INIT_NUMBER12;
//        OB_LOG(INFO, "--", K(j), K(num1), K(num2));
        ObNumber value_v1,value_v2,value_v3;

        EXPECT_EQ(OB_SUCCESS, num1.add_v1(num2, value_v1, allocator));
        EXPECT_EQ(OB_SUCCESS, num1.add_v2(num2, value_v2, allocator));
        EXPECT_EQ(OB_SUCCESS, num1.add_v3(num2, value_v3, allocator));
        ASSERT_EQ(0, value_v1.compare(value_v2));
        ASSERT_EQ(0, value_v1.compare(value_v3));
        allocator.free();

        EXPECT_EQ(OB_SUCCESS, num1.sub_v1(num2, value_v1, allocator));
        EXPECT_EQ(OB_SUCCESS, num1.sub_v2(num2, value_v2, allocator));
        EXPECT_EQ(OB_SUCCESS, num1.sub_v3(num2, value_v3, allocator));
        ASSERT_EQ(0, value_v1.compare(value_v2));
        ASSERT_EQ(0, value_v1.compare(value_v3));
        allocator.free();

        EXPECT_EQ(OB_SUCCESS, num1.mul_v1(num2, value_v1, allocator));
        EXPECT_EQ(OB_SUCCESS, num1.mul_v2(num2, value_v2, allocator));
        EXPECT_EQ(OB_SUCCESS, num1.mul_v3(num2, value_v3, allocator));
        ASSERT_EQ(0, value_v1.compare(value_v2));
        ASSERT_EQ(0, value_v1.compare(value_v3));
        allocator.free();

        //EXPECT_EQ(OB_SUCCESS, num1.div_v1(num2, value_v1, allocator));
        EXPECT_EQ(OB_SUCCESS, num1.div_v2(num2, value_v2, allocator));
        EXPECT_EQ(OB_SUCCESS, num1.div_v3(num2, value_v3, allocator));
        ASSERT_EQ(0, value_v3.compare(value_v2));
        // ASSERT_EQ(0, value_v1.compare(value_v2));
        // ASSERT_EQ(0, value_v1.compare(value_v3));
        allocator.free();
      }
    }
  }
}


TEST(ObNumber, DISABLED_arithmetic_perf_v2)
{
  const int64_t MAX_TEST_COUNT  = 10000;
  const int64_t MAX_TEST_COUNT2 = 20;
  const int64_t MAX_BUF_SIZE = 256;

  char buf_alloc[MAX_BUF_SIZE];
  char buf_alloc2[MAX_BUF_SIZE];
  int64_t tmp = 0;
  int64_t pos = 0;
  int64_t get_range_beg = 0;
  int64_t base_get_range_cost = 0;
  int64_t base_get_range_cost2 = 0;
  int64_t get_range_cost = 0;
  int64_t get_range_cost2 = 0;
  int64_t begin_value = 0;
  int64_t end_value   = 0;
  ObDataBuffer allocator(buf_alloc, MAX_BUF_SIZE);
  const int64_t MAX_DOUBLE_STRICT_PRINT_SIZE2 = 512;
  char buf[MAX_DOUBLE_STRICT_PRINT_SIZE2] = {};
  int64_t length = 0 ;

  enum TestType { TT_MIN = 0, TT_MAX, TT_RAND, TT_MAX_TYPE };
  uint32_t data[TestType::TT_MAX_TYPE][MAX_TEST_COUNT];
  for (int64_t i = 0; i < MAX_TEST_COUNT; ++i) {
    data[TestType::TT_MIN][i] = rand() % 100;
    data[TestType::TT_MAX][i] = rand() % 100 + ObNumber::BASE - 120;
    data[TestType::TT_RAND][i] = rand() % ObNumber::BASE;
  }

  uint32_t digits1[ObNumber::MAX_STORE_LEN];
  uint32_t digits2[ObNumber::MAX_STORE_LEN];
  for (int64_t k = 0; k < TestType::TT_MAX_TYPE; ++k) {
    for (int64_t i = 1; i <= ObNumber::MAX_STORE_LEN; ++i) {
      _OB_LOG(INFO, "test numer k=%ld, i=%ld", k, i);

      ObNumber value_v1;

      base_get_range_cost = 0;
      for (int64_t r = 1; r <= MAX_TEST_COUNT2; ++r) {
        get_range_beg = ObTimeUtility::current_time();
        for (int64_t j = 0; j < MAX_TEST_COUNT; ++j) {
          INIT_NUMBER12;
  //        EXPECT_EQ(OB_SUCCESS, num1.add_v1(num2, value_v1, allocator));
          allocator.free();
        }
        base_get_range_cost += (ObTimeUtility::current_time() - get_range_beg);
      }
      base_get_range_cost2 = base_get_range_cost / MAX_TEST_COUNT2;

      get_range_cost = 0;
      for (int64_t r = 1; r <= MAX_TEST_COUNT2; ++r) {
        get_range_beg = ObTimeUtility::current_time();
        for (int64_t j = 0; j < MAX_TEST_COUNT; ++j) {
          INIT_NUMBER12;
          EXPECT_EQ(OB_SUCCESS, num1.add_v1(num2, value_v1, allocator));
          allocator.free();
        }
        get_range_cost += (ObTimeUtility::current_time() - get_range_beg);
      }
      get_range_cost2 = get_range_cost/MAX_TEST_COUNT2 - base_get_range_cost2;
      _OB_LOG(INFO, "add v1, cost time: %f us", (double)get_range_cost2 / MAX_TEST_COUNT);


      get_range_cost = 0;
      for (int64_t r = 1; r <= MAX_TEST_COUNT2; ++r) {
        get_range_beg = ObTimeUtility::current_time();
        for (int64_t j = 0; j < MAX_TEST_COUNT; ++j) {
          INIT_NUMBER12;
          EXPECT_EQ(OB_SUCCESS, num1.add_v2(num2, value_v1, allocator));
          allocator.free();
        }
        get_range_cost += (ObTimeUtility::current_time() - get_range_beg);
      }
      get_range_cost2 = get_range_cost/MAX_TEST_COUNT2 - base_get_range_cost2;
      _OB_LOG(INFO, "add v2, cost time: %f us", (double)get_range_cost2 / MAX_TEST_COUNT);

      get_range_cost = 0;
      for (int64_t r = 1; r <= MAX_TEST_COUNT2; ++r) {
        get_range_beg = ObTimeUtility::current_time();
        for (int64_t j = 0; j < MAX_TEST_COUNT; ++j) {
          INIT_NUMBER12;
          EXPECT_EQ(OB_SUCCESS, num1.add_v3(num2, value_v1, allocator));
          allocator.free();
        }
        get_range_cost += (ObTimeUtility::current_time() - get_range_beg);
      }
      get_range_cost2 = get_range_cost/MAX_TEST_COUNT2 - base_get_range_cost2;
      _OB_LOG(INFO, "add v3, cost time: %f us", (double)get_range_cost2 / MAX_TEST_COUNT);

      get_range_cost = 0;
      for (int64_t r = 1; r <= MAX_TEST_COUNT2; ++r) {
        get_range_beg = ObTimeUtility::current_time();
        for (int64_t j = 0; j < MAX_TEST_COUNT; ++j) {
          INIT_NUMBER12;
          EXPECT_EQ(OB_SUCCESS, num1.sub_v1(num2, value_v1, allocator));
          allocator.free();
        }
        get_range_cost += (ObTimeUtility::current_time() - get_range_beg);
      }
      get_range_cost2 = get_range_cost/MAX_TEST_COUNT2 - base_get_range_cost2;
      _OB_LOG(INFO, "sub v1, cost time: %f us", (double)get_range_cost2 / MAX_TEST_COUNT);


      get_range_cost = 0;
      for (int64_t r = 1; r <= MAX_TEST_COUNT2; ++r) {
        get_range_beg = ObTimeUtility::current_time();
        for (int64_t j = 0; j < MAX_TEST_COUNT; ++j) {
          INIT_NUMBER12;
          EXPECT_EQ(OB_SUCCESS, num1.sub_v2(num2, value_v1, allocator));
          allocator.free();
        }
        get_range_cost += (ObTimeUtility::current_time() - get_range_beg);
      }
      get_range_cost2 = get_range_cost/MAX_TEST_COUNT2 - base_get_range_cost2;
      _OB_LOG(INFO, "sub v2, cost time: %f us", (double)get_range_cost2 / MAX_TEST_COUNT);


      get_range_cost = 0;
      for (int64_t r = 1; r <= MAX_TEST_COUNT2; ++r) {
        get_range_beg = ObTimeUtility::current_time();
        for (int64_t j = 0; j < MAX_TEST_COUNT; ++j) {
          INIT_NUMBER12;
          EXPECT_EQ(OB_SUCCESS, num1.sub_v3(num2, value_v1, allocator));
          allocator.free();
        }
        get_range_cost += (ObTimeUtility::current_time() - get_range_beg);
      }
      get_range_cost2 = get_range_cost/MAX_TEST_COUNT2 - base_get_range_cost2;
      _OB_LOG(INFO, "sub v3, cost time: %f us", (double)get_range_cost2 / MAX_TEST_COUNT);

      get_range_cost = 0;
      for (int64_t r = 1; r <= MAX_TEST_COUNT2; ++r) {
        get_range_beg = ObTimeUtility::current_time();
        for (int64_t j = 0; j < MAX_TEST_COUNT; ++j) {
          INIT_NUMBER12;
          EXPECT_EQ(OB_SUCCESS, num1.mul_v1(num2, value_v1, allocator));
          allocator.free();
        }
        get_range_cost += (ObTimeUtility::current_time() - get_range_beg);
      }
      get_range_cost2 = get_range_cost/MAX_TEST_COUNT2 - base_get_range_cost2;
      _OB_LOG(INFO, "mul v1, cost time: %f us", (double)get_range_cost2 / MAX_TEST_COUNT);

      get_range_cost = 0;
      for (int64_t r = 1; r <= MAX_TEST_COUNT2; ++r) {
        get_range_beg = ObTimeUtility::current_time();
        for (int64_t j = 0; j < MAX_TEST_COUNT; ++j) {
          INIT_NUMBER12;
          EXPECT_EQ(OB_SUCCESS, num1.mul_v2(num2, value_v1, allocator));
          allocator.free();
        }
        get_range_cost += (ObTimeUtility::current_time() - get_range_beg);
      }
      get_range_cost2 = get_range_cost/MAX_TEST_COUNT2 - base_get_range_cost2;
      _OB_LOG(INFO, "mul v2, cost time: %f us", (double)get_range_cost2 / MAX_TEST_COUNT);


      get_range_cost = 0;
      for (int64_t r = 1; r <= MAX_TEST_COUNT2; ++r) {
        get_range_beg = ObTimeUtility::current_time();
        for (int64_t j = 0; j < MAX_TEST_COUNT; ++j) {
          INIT_NUMBER12;
          EXPECT_EQ(OB_SUCCESS, num1.mul_v3(num2, value_v1, allocator));
          allocator.free();
        }
        get_range_cost += (ObTimeUtility::current_time() - get_range_beg);
      }
      get_range_cost2 = get_range_cost/MAX_TEST_COUNT2 - base_get_range_cost2;
      _OB_LOG(INFO, "mul v3, cost time: %f us", (double)get_range_cost2 / MAX_TEST_COUNT);


      //get_range_cost = 0;
      //for (int64_t r = 1; r <= MAX_TEST_COUNT2; ++r) {
      //  get_range_beg = ObTimeUtility::current_time();
      //  for (int64_t j = 0; j < MAX_TEST_COUNT; ++j) {
      //    INIT_NUMBER12;
      //    EXPECT_EQ(OB_SUCCESS, num1.div_v1(num2, value_v1, allocator));
      //    allocator.free();
      //  }
      //  get_range_cost += (ObTimeUtility::current_time() - get_range_beg);
      //}
      //get_range_cost2 = get_range_cost/MAX_TEST_COUNT2 - base_get_range_cost2;
      //_OB_LOG(INFO, "div v1, cost time: %f us", (double)get_range_cost2 / MAX_TEST_COUNT);


      get_range_cost = 0;
      for (int64_t r = 1; r <= MAX_TEST_COUNT2; ++r) {
        get_range_beg = ObTimeUtility::current_time();
        for (int64_t j = 0; j < MAX_TEST_COUNT; ++j) {
          INIT_NUMBER12;
          EXPECT_EQ(OB_SUCCESS, num1.div_v2(num2, value_v1, allocator));
          allocator.free();
        }
        get_range_cost += (ObTimeUtility::current_time() - get_range_beg);
      }
      get_range_cost2 = get_range_cost/MAX_TEST_COUNT2 - base_get_range_cost2;
      _OB_LOG(INFO, "div v2, cost time: %f us", (double)get_range_cost2 / MAX_TEST_COUNT);


      get_range_cost = 0;
      for (int64_t r = 1; r <= MAX_TEST_COUNT2; ++r) {
        get_range_beg = ObTimeUtility::current_time();
        for (int64_t j = 0; j < MAX_TEST_COUNT; ++j) {
          INIT_NUMBER12;
          EXPECT_EQ(OB_SUCCESS, num1.div_v3(num2, value_v1, allocator));
          allocator.free();
        }
        get_range_cost += (ObTimeUtility::current_time() - get_range_beg);
      }
      get_range_cost2 = get_range_cost/MAX_TEST_COUNT2 - base_get_range_cost2;
      _OB_LOG(INFO, "div v3, cost time: %f us", (double)get_range_cost2 / MAX_TEST_COUNT);


      get_range_cost = 0;
      for (int64_t r = 1; r <= MAX_TEST_COUNT2; ++r) {
        get_range_beg = ObTimeUtility::current_time();
        for (int64_t j = 0; j < MAX_TEST_COUNT; ++j) {
          INIT_NUMBER12;
          if (!num2.is_zero()) {
            // EXPECT_EQ(OB_SUCCESS, num1.rem_v1(num2, value_v1, allocator));
            EXPECT_EQ(OB_SUCCESS, num1.rem_v2(num2, value_v1, allocator));
          }
          allocator.free();
        }
        get_range_cost += (ObTimeUtility::current_time() - get_range_beg);
      }
      get_range_cost2 = get_range_cost/MAX_TEST_COUNT2 - base_get_range_cost2;
      _OB_LOG(INFO, "rem v1, cost time: %f us", (double)get_range_cost2 / MAX_TEST_COUNT);

      get_range_cost = 0;
      for (int64_t r = 1; r <= MAX_TEST_COUNT2; ++r) {
        get_range_beg = ObTimeUtility::current_time();
        for (int64_t j = 0; j < MAX_TEST_COUNT; ++j) {
          INIT_NUMBER12;
          if (!num2.is_zero()) {
            EXPECT_EQ(OB_SUCCESS, num1.rem_v2(num2, value_v1, allocator));
          }
          allocator.free();
        }
        get_range_cost += (ObTimeUtility::current_time() - get_range_beg);
      }
      get_range_cost2 = get_range_cost/MAX_TEST_COUNT2 - base_get_range_cost2;
      _OB_LOG(INFO, "rem v3, cost time: %f us", (double)get_range_cost2 / MAX_TEST_COUNT);
    }
  }
}


int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_number_v2.log", true);
  //oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
