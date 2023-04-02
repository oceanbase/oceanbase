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
#define private public
#include "lib/json_type/ob_json_tree.h"
#include "lib/json_type/ob_json_bin.h"
#include "lib/json_type/ob_json_base.h"
#include "lib/string/ob_sql_string.h"
#include "lib/timezone/ob_timezone_info.h"
#undef private

#include <sys/time.h>    
using namespace std;
namespace oceanbase {
namespace common {

class TestJsonBase : public ::testing::Test {
public:
  TestJsonBase()
  {}
  ~TestJsonBase()
  {}
  virtual void SetUp()
  {}
  virtual void TearDown()
  {}

  static void SetUpTestCase()
  {}

  static void TearDownTestCase()
  {}

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestJsonBase);
};

// json text 比较
//
// @param [in] json_text_a 比较的json text a
// @param [in] json_text_b 比较的json text b
// @return 小于返回-1，大于返回1， 相等返回0
static int json_compare_json_text(common::ObString &json_text_a, common::ObString &json_text_b)
{
  common::ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree_a = NULL;
  ObIJsonBase *j_tree_b = NULL;
  ObIJsonBase *j_bin_a = NULL;
  ObIJsonBase *j_bin_b = NULL;
  int res_tree = 0;
  int res_bin = 0;

  // 1. test json tree
  EXPECT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, json_text_a,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree_a));
  EXPECT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, json_text_b,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree_b));
  EXPECT_EQ(OB_SUCCESS, j_tree_a->compare(*j_tree_b, res_tree));

  // 2. test json bin
  EXPECT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, json_text_a,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin_a));
  EXPECT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, json_text_b,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin_b));
  EXPECT_EQ(OB_SUCCESS, j_bin_a->compare(*j_bin_b, res_bin));

  EXPECT_EQ(res_tree, res_bin);

  return res_bin;
}

// json string 比较
//
// @param [in] str_a 比较的json 字符串a
// @param [in] len_a 比较的json 字符串a的长度
// @param [in] str_b 比较的json 字符串b
// @param [in] len_a 比较的json 字符串b的长度
// @return 小于返回-1，大于返回1， 相等返回0
static int json_compare_string(const char *str_a, uint32_t len_a, const char *str_b, uint32_t len_b)
{
  common::ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree_a = NULL;
  ObIJsonBase *j_tree_b = NULL;
  ObIJsonBase *j_bin_a = NULL;
  ObIJsonBase *j_bin_b = NULL;
  int res_tree = 0;
  int res_bin = 0;

  // 1. test json tree
  ObJsonString j_str_a(str_a, len_a);
  j_tree_a = &j_str_a;
  ObJsonString j_str_b(str_b, len_b);
  j_tree_b = &j_str_b;
  EXPECT_EQ(OB_SUCCESS, j_tree_a->compare(*j_tree_b, res_tree));

  // 2. test json bin
  EXPECT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_str_a,
      ObJsonInType::JSON_BIN, j_bin_a));
  EXPECT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_str_b,
      ObJsonInType::JSON_BIN, j_bin_b));
  EXPECT_EQ(OB_SUCCESS, j_bin_a->compare(*j_bin_b, res_bin));

  EXPECT_EQ(res_tree, res_bin);

  return res_bin;
}

// json int vs int
//
// @param [in] a int64_t
// @param [in] b int64_t
// @return 小于返回-1，大于返回1， 相等返回0
static int json_compare_int_int(int64_t a, int64_t b)
{
  common::ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree_a = NULL;
  ObIJsonBase *j_tree_b = NULL;
  ObIJsonBase *j_bin_a = NULL;
  ObIJsonBase *j_bin_b = NULL;
  int res_tree = 0;
  int res_bin = 0;

  // 1. test json tree
  ObJsonInt j_int_a(a);
  j_tree_a = &j_int_a;
  ObJsonInt j_int_b(b);
  j_tree_b = &j_int_b;
  EXPECT_EQ(OB_SUCCESS, j_tree_a->compare(*j_tree_b, res_tree));

  // 2. test json bin
  EXPECT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_int_a,
      ObJsonInType::JSON_BIN, j_bin_a));
  EXPECT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_int_b,
      ObJsonInType::JSON_BIN, j_bin_b));
  EXPECT_EQ(OB_SUCCESS, j_bin_a->compare(*j_bin_b, res_bin));

  EXPECT_EQ(res_tree, res_bin);

  return res_bin;
}

// json int vs uint
//
// @param [in] a int64_t
// @param [in] b uint64_t
// @return 小于返回-1，大于返回1， 相等返回0
static int json_compare_int_uint(int64_t a, uint64_t b)
{
  common::ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree_a = NULL;
  ObIJsonBase *j_tree_b = NULL;
  ObIJsonBase *j_bin_a = NULL;
  ObIJsonBase *j_bin_b = NULL;
  int res_tree = 0;
  int res_bin = 0;

  // 1. test json tree
  ObJsonInt j_int_a(a);
  j_tree_a = &j_int_a;
  ObJsonUint j_uint_b(b);
  j_tree_b = &j_uint_b;
  EXPECT_EQ(OB_SUCCESS, j_tree_a->compare(*j_tree_b, res_tree));

  // 2. test json bin
  EXPECT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_int_a,
      ObJsonInType::JSON_BIN, j_bin_a));
  EXPECT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_uint_b,
      ObJsonInType::JSON_BIN, j_bin_b));
  EXPECT_EQ(OB_SUCCESS, j_bin_a->compare(*j_bin_b, res_bin));

  EXPECT_EQ(res_tree, res_bin);

  return res_bin;
}

// json int vs double
//
// @param [in] a int64_t
// @param [in] b double
// @return 小于返回-1，大于返回1， 相等返回0
static int json_compare_int_double(int64_t a, double b)
{
  common::ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree_a = NULL;
  ObIJsonBase *j_tree_b = NULL;
  ObIJsonBase *j_bin_a = NULL;
  ObIJsonBase *j_bin_b = NULL;
  int res_tree = 0;
  int res_bin = 0;

  // 1. test json tree
  ObJsonInt j_int_a(a);
  j_tree_a = &j_int_a;
  ObJsonDouble j_double_b(b);
  j_tree_b = &j_double_b;
  EXPECT_EQ(OB_SUCCESS, j_tree_a->compare(*j_tree_b, res_tree));

  // 2. test json bin
  EXPECT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_int_a,
      ObJsonInType::JSON_BIN, j_bin_a));
  EXPECT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_double_b,
      ObJsonInType::JSON_BIN, j_bin_b));
  EXPECT_EQ(OB_SUCCESS, j_bin_a->compare(*j_bin_b, res_bin));

  EXPECT_EQ(res_tree, res_bin);

  return res_bin;
}

// json int vs decimal
//
// @param [in] a int64_t
// @param [in] b ObNumber
// @return 小于返回-1，大于返回1， 相等返回0
static int json_compare_int_decimal(int64_t a, number::ObNumber &b)
{
  common::ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree_a = NULL;
  ObIJsonBase *j_tree_b = NULL;
  ObIJsonBase *j_bin_a = NULL;
  ObIJsonBase *j_bin_b = NULL;
  int res_tree = 0;
  int res_bin = 0;

  // 1. test json tree
  ObJsonInt j_int_a(a);
  j_tree_a = &j_int_a;
  ObJsonDecimal j_dec_b(b);
  j_tree_b = &j_dec_b;
  EXPECT_EQ(OB_SUCCESS, j_tree_a->compare(*j_tree_b, res_tree));

  // 2. test json bin
  EXPECT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_int_a,
      ObJsonInType::JSON_BIN, j_bin_a));
  EXPECT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_dec_b,
      ObJsonInType::JSON_BIN, j_bin_b));
  EXPECT_EQ(OB_SUCCESS, j_bin_a->compare(*j_bin_b, res_bin));

  EXPECT_EQ(res_tree, res_bin);

  return res_bin;
}

// json uint vs uint
//
// @param [in] a uint64_t
// @param [in] b uint64_t
// @return 小于返回-1，大于返回1， 相等返回0
static int json_compare_uint_uint(uint64_t a, uint64_t b)
{
  common::ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree_a = NULL;
  ObIJsonBase *j_tree_b = NULL;
  ObIJsonBase *j_bin_a = NULL;
  ObIJsonBase *j_bin_b = NULL;
  int res_tree = 0;
  int res_bin = 0;

  // 1. test json tree
  ObJsonUint j_uint_a(a);
  j_tree_a = &j_uint_a;
  ObJsonUint j_uint_b(b);
  j_tree_b = &j_uint_b;
  EXPECT_EQ(OB_SUCCESS, j_tree_a->compare(*j_tree_b, res_tree));

  // 2. test json bin
  EXPECT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_uint_a,
      ObJsonInType::JSON_BIN, j_bin_a));
  EXPECT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_uint_b,
      ObJsonInType::JSON_BIN, j_bin_b));
  EXPECT_EQ(OB_SUCCESS, j_bin_a->compare(*j_bin_b, res_bin));

  EXPECT_EQ(res_tree, res_bin);

  return res_bin;
}

// json uint vs int
//
// @param [in] a uint64_t
// @param [in] b int64_t
// @return 小于返回-1，大于返回1， 相等返回0
static int json_compare_uint_int(uint64_t a, int64_t b)
{
  common::ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree_a = NULL;
  ObIJsonBase *j_tree_b = NULL;
  ObIJsonBase *j_bin_a = NULL;
  ObIJsonBase *j_bin_b = NULL;
  int res_tree = 0;
  int res_bin = 0;

  // 1. test json tree
  ObJsonUint j_uint_a(a);
  j_tree_a = &j_uint_a;
  ObJsonInt j_int_b(b);
  j_tree_b = &j_int_b;
  EXPECT_EQ(OB_SUCCESS, j_tree_a->compare(*j_tree_b, res_tree));

  // 2. test json bin
  EXPECT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_uint_a,
      ObJsonInType::JSON_BIN, j_bin_a));
  EXPECT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_int_b,
      ObJsonInType::JSON_BIN, j_bin_b));
  EXPECT_EQ(OB_SUCCESS, j_bin_a->compare(*j_bin_b, res_bin));

  EXPECT_EQ(res_tree, res_bin);

  return res_bin;
}

// json uint vs double
//
// @param [in] a uint64_t
// @param [in] b double
// @return 小于返回-1，大于返回1， 相等返回0
static int json_compare_uint_double(uint64_t a, double b)
{
  common::ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree_a = NULL;
  ObIJsonBase *j_tree_b = NULL;
  ObIJsonBase *j_bin_a = NULL;
  ObIJsonBase *j_bin_b = NULL;
  int res_tree = 0;
  int res_bin = 0;

  // 1. test json tree
  ObJsonUint j_uint_a(a);
  j_tree_a = &j_uint_a;
  ObJsonDouble j_double_b(b);
  j_tree_b = &j_double_b;
  EXPECT_EQ(OB_SUCCESS, j_tree_a->compare(*j_tree_b, res_tree));

  // 2. test json bin
  EXPECT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_uint_a,
      ObJsonInType::JSON_BIN, j_bin_a));
  EXPECT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_double_b,
      ObJsonInType::JSON_BIN, j_bin_b));
  EXPECT_EQ(OB_SUCCESS, j_bin_a->compare(*j_bin_b, res_bin));

  EXPECT_EQ(res_tree, res_bin);

  return res_bin;
}

// json uint vs decimal
//
// @param [in] a uint64_t
// @param [in] b ObNumber
// @return 小于返回-1，大于返回1， 相等返回0
static int json_compare_uint_decimal(uint64_t a, number::ObNumber &b)
{
  common::ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree_a = NULL;
  ObIJsonBase *j_tree_b = NULL;
  ObIJsonBase *j_bin_a = NULL;
  ObIJsonBase *j_bin_b = NULL;
  int res_tree = 0;
  int res_bin = 0;

  // 1. test json tree
  ObJsonUint j_uint_a(a);
  j_tree_a = &j_uint_a;
  ObJsonDecimal j_dec_b(b);
  j_tree_b = &j_dec_b;
  EXPECT_EQ(OB_SUCCESS, j_tree_a->compare(*j_tree_b, res_tree));

  // 2. test json bin
  EXPECT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_uint_a,
      ObJsonInType::JSON_BIN, j_bin_a));
  EXPECT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_dec_b,
      ObJsonInType::JSON_BIN, j_bin_b));
  EXPECT_EQ(OB_SUCCESS, j_bin_a->compare(*j_bin_b, res_bin));

  EXPECT_EQ(res_tree, res_bin);

  return res_bin;
}

// json double vs int
//
// @param [in] a double
// @param [in] b int64_t
// @return 小于返回-1，大于返回1， 相等返回0
static int json_compare_doule_int(double a, int64_t b)
{
  common::ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree_a = NULL;
  ObIJsonBase *j_tree_b = NULL;
  ObIJsonBase *j_bin_a = NULL;
  ObIJsonBase *j_bin_b = NULL;
  int res_tree = 0;
  int res_bin = 0;

  // 1. test json tree
  ObJsonDouble j_double_a(a);
  j_tree_a = &j_double_a;
  ObJsonInt j_int_b(b);
  j_tree_b = &j_int_b;
  EXPECT_EQ(OB_SUCCESS, j_tree_a->compare(*j_tree_b, res_tree));

  // 2. test json bin
  EXPECT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_double_a,
      ObJsonInType::JSON_BIN, j_bin_a));
  EXPECT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_int_b,
      ObJsonInType::JSON_BIN, j_bin_b));
  EXPECT_EQ(OB_SUCCESS, j_bin_a->compare(*j_bin_b, res_bin));

  EXPECT_EQ(res_tree, res_bin);

  return res_bin;
}

// json double vs uint
//
// @param [in] a double
// @param [in] b uint64_t
// @return 小于返回-1，大于返回1， 相等返回0
static int json_compare_doule_uint(double a, uint64_t b)
{
  common::ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree_a = NULL;
  ObIJsonBase *j_tree_b = NULL;
  ObIJsonBase *j_bin_a = NULL;
  ObIJsonBase *j_bin_b = NULL;
  int res_tree = 0;
  int res_bin = 0;

  // 1. test json tree
  ObJsonDouble j_double_a(a);
  j_tree_a = &j_double_a;
  ObJsonUint j_uint_b(b);
  j_tree_b = &j_uint_b;
  EXPECT_EQ(OB_SUCCESS, j_tree_a->compare(*j_tree_b, res_tree));

  // 2. test json bin
  EXPECT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_double_a,
      ObJsonInType::JSON_BIN, j_bin_a));
  EXPECT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_uint_b,
      ObJsonInType::JSON_BIN, j_bin_b));
  EXPECT_EQ(OB_SUCCESS, j_bin_a->compare(*j_bin_b, res_bin));

  EXPECT_EQ(res_tree, res_bin);

  return res_bin;
}

// json double vs decimal
//
// @param [in] a double
// @param [in] b ObNumber
// @return 小于返回-1，大于返回1， 相等返回0
static int json_compare_double_decimal(double a, number::ObNumber &b)
{
  common::ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree_a = NULL;
  ObIJsonBase *j_tree_b = NULL;
  ObIJsonBase *j_bin_a = NULL;
  ObIJsonBase *j_bin_b = NULL;
  int res_tree = 0;
  int res_bin = 0;

  // 1. test json tree
  ObJsonDouble j_double_a(a);
  j_tree_a = &j_double_a;
  ObJsonDecimal j_dec_b(b);
  j_tree_b = &j_dec_b;
  EXPECT_EQ(OB_SUCCESS, j_tree_a->compare(*j_tree_b, res_tree));

  // 2. test json bin
  EXPECT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_double_a,
      ObJsonInType::JSON_BIN, j_bin_a));
  EXPECT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_dec_b,
      ObJsonInType::JSON_BIN, j_bin_b));
  EXPECT_EQ(OB_SUCCESS, j_bin_a->compare(*j_bin_b, res_bin));

  EXPECT_EQ(res_tree, res_bin);

  return res_bin;
}

// json double vs double
//
// @param [in] a double
// @param [in] b double
// @return 小于返回-1，大于返回1， 相等返回0
static int json_compare_double_double(double a, double b)
{
  common::ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree_a = NULL;
  ObIJsonBase *j_tree_b = NULL;
  ObIJsonBase *j_bin_a = NULL;
  ObIJsonBase *j_bin_b = NULL;
  int res_tree = 0;
  int res_bin = 0;

  // 1. test json tree
  ObJsonDouble j_double_a(a);
  j_tree_a = &j_double_a;
  ObJsonDouble j_double_b(b);
  j_tree_b = &j_double_b;
  EXPECT_EQ(OB_SUCCESS, j_tree_a->compare(*j_tree_b, res_tree));

  // 2. test json bin
  EXPECT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_double_a,
      ObJsonInType::JSON_BIN, j_bin_a));
  EXPECT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_double_b,
      ObJsonInType::JSON_BIN, j_bin_b));
  EXPECT_EQ(OB_SUCCESS, j_bin_a->compare(*j_bin_b, res_bin));

  EXPECT_EQ(res_tree, res_bin);

  return res_bin;
}

// json decimal vs decimal
//
// @param [in] a ObNumber
// @param [in] b ObNumber
// @return 小于返回-1，大于返回1， 相等返回0
static int json_compare_decimal_decimal(number::ObNumber &a, number::ObNumber &b)
{
  common::ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree_a = NULL;
  ObIJsonBase *j_tree_b = NULL;
  ObIJsonBase *j_bin_a = NULL;
  ObIJsonBase *j_bin_b = NULL;
  int res_tree = 0;
  int res_bin = 0;

  // 1. test json tree
  ObJsonDecimal j_dec_a(a);
  j_tree_a = &j_dec_a;
  ObJsonDecimal j_dec_b(b);
  j_tree_b = &j_dec_b;
  EXPECT_EQ(OB_SUCCESS, j_tree_a->compare(*j_tree_b, res_tree));

  // 2. test json bin
  EXPECT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_dec_a,
      ObJsonInType::JSON_BIN, j_bin_a));
  EXPECT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_dec_b,
      ObJsonInType::JSON_BIN, j_bin_b));
  EXPECT_EQ(OB_SUCCESS, j_bin_a->compare(*j_bin_b, res_bin));

  EXPECT_EQ(res_tree, res_bin);

  return res_bin;
}

// json decimal vs int
//
// @param [in] a ObNumber
// @param [in] b int64_t
// @return 小于返回-1，大于返回1， 相等返回0
static int json_compare_decimal_int(number::ObNumber &a, int64_t b)
{
  common::ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree_a = NULL;
  ObIJsonBase *j_tree_b = NULL;
  ObIJsonBase *j_bin_a = NULL;
  ObIJsonBase *j_bin_b = NULL;
  int res_tree = 0;
  int res_bin = 0;

  // 1. test json tree
  ObJsonDecimal j_dec_a(a);
  j_tree_a = &j_dec_a;
  ObJsonInt j_int_b(b);
  j_tree_b = &j_int_b;
  EXPECT_EQ(OB_SUCCESS, j_tree_a->compare(*j_tree_b, res_tree));

  // 2. test json bin
  EXPECT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_dec_a,
      ObJsonInType::JSON_BIN, j_bin_a));
  EXPECT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_int_b,
      ObJsonInType::JSON_BIN, j_bin_b));
  EXPECT_EQ(OB_SUCCESS, j_bin_a->compare(*j_bin_b, res_bin));

  EXPECT_EQ(res_tree, res_bin);

  return res_bin;
}

// json decimal vs uint
//
// @param [in] a ObNumber
// @param [in] b uint64_t
// @return 小于返回-1，大于返回1， 相等返回0
static int json_compare_decimal_uint(number::ObNumber &a, uint64_t b)
{
  common::ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree_a = NULL;
  ObIJsonBase *j_tree_b = NULL;
  ObIJsonBase *j_bin_a = NULL;
  ObIJsonBase *j_bin_b = NULL;
  int res_tree = 0;
  int res_bin = 0;

  // 1. test json tree
  ObJsonDecimal j_dec_a(a);
  j_tree_a = &j_dec_a;
  ObJsonUint j_uint_b(b);
  j_tree_b = &j_uint_b;
  EXPECT_EQ(OB_SUCCESS, j_tree_a->compare(*j_tree_b, res_tree));

  // 2. test json bin
  EXPECT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_dec_a,
      ObJsonInType::JSON_BIN, j_bin_a));
  EXPECT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_uint_b,
      ObJsonInType::JSON_BIN, j_bin_b));
  EXPECT_EQ(OB_SUCCESS, j_bin_a->compare(*j_bin_b, res_bin));

  EXPECT_EQ(res_tree, res_bin);

  return res_bin;
}

// json decimal vs double
//
// @param [in] a ObNumber
// @param [in] b double
// @return 小于返回-1，大于返回1， 相等返回0
static int json_compare_decimal_double(number::ObNumber &a, double b)
{
  common::ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree_a = NULL;
  ObIJsonBase *j_tree_b = NULL;
  ObIJsonBase *j_bin_a = NULL;
  ObIJsonBase *j_bin_b = NULL;
  int res_tree = 0;
  int res_bin = 0;

  // 1. test json tree
  ObJsonDecimal j_dec_a(a);
  j_tree_a = &j_dec_a;
  ObJsonDouble j_double_b(b);
  j_tree_b = &j_double_b;
  EXPECT_EQ(OB_SUCCESS, j_tree_a->compare(*j_tree_b, res_tree));

  // 2. test json bin
  EXPECT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_dec_a,
      ObJsonInType::JSON_BIN, j_bin_a));
  EXPECT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_double_b,
      ObJsonInType::JSON_BIN, j_bin_b));
  EXPECT_EQ(OB_SUCCESS, j_bin_a->compare(*j_bin_b, res_bin));

  EXPECT_EQ(res_tree, res_bin);

  return res_bin;
}

// json boolean vs boolean
//
// @param [in] a bool
// @param [in] b bool
// @return 小于返回-1，大于返回1， 相等返回0
static int json_compare_boolean_boolean(bool a, bool b)
{
  common::ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree_a = NULL;
  ObIJsonBase *j_tree_b = NULL;
  ObIJsonBase *j_bin_a = NULL;
  ObIJsonBase *j_bin_b = NULL;
  int res_tree = 0;
  int res_bin = 0;

  // 1. test json tree
  ObJsonBoolean j_bool_a(a);
  j_tree_a = &j_bool_a;
  ObJsonBoolean j_bool_b(b);
  j_tree_b = &j_bool_b;
  EXPECT_EQ(OB_SUCCESS, j_tree_a->compare(*j_tree_b, res_tree));

  // 2. test json bin
  EXPECT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_bool_a,
      ObJsonInType::JSON_BIN, j_bin_a));
  EXPECT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_bool_b,
      ObJsonInType::JSON_BIN, j_bin_b));
  EXPECT_EQ(OB_SUCCESS, j_bin_a->compare(*j_bin_b, res_bin));

  EXPECT_EQ(res_tree, res_bin);

  return res_bin;
}

// json datetime vs datetime
//
// @param [in] a ObTime
// @param [in] a 时间字段类型
// @param [in] b ObTime
// @param [in] a 时间字段类型
// @return 小于返回-1，大于返回1， 相等返回0
static int json_compare_datetime_datetime(ObTime &a, ObObjType type_a,
                                          ObTime &b, ObObjType type_b)
{
  common::ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree_a = NULL;
  ObIJsonBase *j_tree_b = NULL;
  ObIJsonBase *j_bin_a = NULL;
  ObIJsonBase *j_bin_b = NULL;
  int res_tree = 0;
  int res_bin = 0;

  // 1. test json tree
  ObJsonDatetime j_datetime_a(a, type_a);
  j_tree_a = &j_datetime_a;
  ObJsonDatetime j_datetime_b(b, type_b);
  j_tree_b = &j_datetime_b;
  EXPECT_EQ(OB_SUCCESS, j_tree_a->compare(*j_tree_b, res_tree));

  // 2. test json bin
  EXPECT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_datetime_a,
      ObJsonInType::JSON_BIN, j_bin_a));
  EXPECT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_datetime_b,
      ObJsonInType::JSON_BIN, j_bin_b));
  EXPECT_EQ(OB_SUCCESS, j_bin_a->compare(*j_bin_b, res_bin));

  EXPECT_EQ(res_tree, res_bin);

  return res_bin;
}

// json opaque vs opaque
//
// @param [in] a ObString
// @param [in] type_a ObObjType
// @param [in] b ObString
// @param [in] type_a ObObjType
// @return 小于返回-1，大于返回1， 相等返回0
static int json_compare_opaque_opaque(common::ObString a, ObObjType type_a,
                                      common::ObString b, ObObjType type_b)
{
  common::ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree_a = NULL;
  ObIJsonBase *j_tree_b = NULL;
  ObIJsonBase *j_bin_a = NULL;
  ObIJsonBase *j_bin_b = NULL;
  int res_tree = 0;
  int res_bin = 0;

  // 1. test json tree
  ObJsonOpaque j_opaque_a(a, type_a);
  j_tree_a = &j_opaque_a;
  ObJsonOpaque j_opaque_b(b, type_b);
  j_tree_b = &j_opaque_b;
  EXPECT_EQ(OB_SUCCESS, j_tree_a->compare(*j_tree_b, res_tree));

  // 2. test json bin
  EXPECT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_opaque_a,
      ObJsonInType::JSON_BIN, j_bin_a));
  EXPECT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_opaque_b,
      ObJsonInType::JSON_BIN, j_bin_b));
  EXPECT_EQ(OB_SUCCESS, j_bin_a->compare(*j_bin_b, res_bin));

  EXPECT_EQ(res_tree, res_bin);

  return res_bin;
}

static int create_object(common::ObIAllocator *allocator, const char *key_ptr, 
                         uint64_t length, ObJsonNode *value, ObJsonObject *&j_obj)
{
  int ret = OB_SUCCESS;
  void *obj_buf = allocator->alloc(sizeof(ObJsonObject));
  void *key_buf = allocator->alloc(sizeof(ObString));
  void *key_val = allocator->alloc(sizeof(char) * length);
  ObJsonObject *obj = new (obj_buf) ObJsonObject(allocator);
  MEMCPY(key_val, key_ptr, length);
  ObString *key = new (key_buf) ObString(length, (char *)key_val);

  if (value == NULL) {
    void *val_buf = allocator->alloc(sizeof(ObJsonInt));
    value = new (val_buf) ObJsonInt(1);
  }

  if (OB_FAIL(obj->add(*key, value))) {
    ret = OB_ERR_UNEXPECTED;
  } else{
    j_obj = obj;
  }

  return ret;
}

/* ========================= test for class ObJsonBaseFactory begin ============================= */
TEST_F(TestJsonBase, test_get_json_base)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  common::ObString j_text("{ \"greeting\" : \"Hello!\", \"farewell\" : \"bye-bye!\"}");
  ObIJsonBase *j_tree = NULL;
  ObIJsonBase *j_bin = NULL;

  // TREE->TREE
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree));
  ASSERT_TRUE(j_tree->is_tree());
  // TREE -> BIN
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
  ASSERT_TRUE(j_bin->is_bin());
  ASSERT_EQ(ObJsonNodeType::J_OBJECT, j_bin->json_type());

  common::ObString raw_bin;
  ASSERT_EQ(OB_SUCCESS, j_bin->get_raw_binary(raw_bin, &allocator));
  // BIN-> BIN
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, raw_bin,
      ObJsonInType::JSON_BIN, ObJsonInType::JSON_BIN, j_bin));
  ASSERT_TRUE(j_bin->is_bin());
  ASSERT_EQ(ObJsonNodeType::J_OBJECT, j_bin->json_type());
  // BIN -> TREE
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, raw_bin,
      ObJsonInType::JSON_BIN, ObJsonInType::JSON_TREE, j_tree));
  ASSERT_TRUE(j_tree->is_tree());
  ASSERT_EQ(ObJsonNodeType::J_OBJECT, j_tree->json_type());
}

TEST_F(TestJsonBase, test_transform)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  common::ObString j_text("{ \"greeting\" : \"Hello!\", \"farewell\" : \"bye-bye!\"}");
  ObIJsonBase *j_tree = NULL;

  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree));
  ASSERT_TRUE(j_tree->is_tree());

  ObIJsonBase *j_new = NULL;
  // TREE -> TREE
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree,
      ObJsonInType::JSON_TREE, j_new));
  // TREE -> BIN
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree,
      ObJsonInType::JSON_BIN, j_new));
  // BIN -> BIN
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_new,
      ObJsonInType::JSON_BIN, j_tree));
  // BIN -> TREE
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_new,
      ObJsonInType::JSON_TREE, j_new));
}
/* ========================= test for class ObJsonBaseFactory end ============================== */

/* ============================= test for class ObIJsonBase begin =============================== */
TEST_F(TestJsonBase, test_is_tree)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  common::ObString j_text("{ \"greeting\" : \"Hello!\", \"farewell\" : \"bye-bye!\"}");
  ObIJsonBase *j_tree = NULL;

  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree));
  ASSERT_TRUE(j_tree->is_tree());
}

TEST_F(TestJsonBase, test_is_bin)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  common::ObString j_text("{ \"greeting\" : \"Hello!\", \"farewell\" : \"bye-bye!\"}");
  ObIJsonBase *j_bin = NULL;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
  ASSERT_TRUE(j_bin->is_bin());
}

TEST_F(TestJsonBase, test_get_allocator)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  common::ObString j_text("{ \"greeting\" : \"Hello!\", \"farewell\" : \"bye-bye!\"}");
  ObIJsonBase *j_tree = NULL;

  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree));
  ASSERT_TRUE(j_tree->get_allocator() != NULL);
}

TEST_F(TestJsonBase, test_seek)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObIJsonBase *j_bin = NULL;
  ObJsonBaseVector hit;
  ObJsonBuffer j_buf(&allocator);

  // 1. seek_not_exist_member
  common::ObString j_text1("{\"name\": \"Safari\", \"os\": \"Mac\"}");
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text1,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree));
  common::ObString path_text1("$.b");
  ObJsonPath j_path1(path_text1, &allocator);
  ASSERT_EQ(OB_SUCCESS, j_path1.parse_path());
  // tree
  ASSERT_EQ(OB_SUCCESS, j_tree->seek(j_path1, j_path1.path_node_cnt(), false, false, hit));
  ASSERT_EQ(hit.size(), 0);
  // bin
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree,
      ObJsonInType::JSON_BIN, j_bin));
  hit.reset();
  ASSERT_EQ(OB_SUCCESS, j_bin->seek(j_path1, j_path1.path_node_cnt(), false, false, hit));
  ASSERT_EQ(hit.size(), 0);

  // 2. seek_member
  common::ObString j_text2("{\"x\": \"Safari\", \"os\": \"Mac\", \"resolution\": \
      {\"x\": \"1920\", \"y\": \"1080\"}}");
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text2,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree));
  common::ObString path_text2("$.\"resolution\".x");
  ObJsonPath j_path2(path_text2, &allocator);
  ASSERT_EQ(OB_SUCCESS, j_path2.parse_path());
  hit.reset();
  // tree
  ASSERT_EQ(OB_SUCCESS, j_tree->seek(j_path2, j_path2.path_node_cnt(), false, false, hit));
  ASSERT_EQ(hit.size(), 1);
  ASSERT_EQ(0, strncmp("1920", hit[0]->get_data(), hit[0]->get_data_length()));
  // bin
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree,
      ObJsonInType::JSON_BIN, j_bin));
  hit.reset();
  ASSERT_EQ(OB_SUCCESS, j_bin->seek(j_path2, j_path2.path_node_cnt(), false, false, hit));
  ASSERT_EQ(hit.size(), 1);
  ASSERT_EQ(0, strncmp("1920", hit[0]->get_data(), hit[0]->get_data_length()));

  // 3. seek_member_wildcard1
  common::ObString j_text3("[1, [[{\"x\": [{\"a\":{\"b\":{\"c\":42}}}]}]]]");
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text3,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree));
  common::ObString path_text3("$**.a.*");
  ObJsonPath j_path3(path_text3, &allocator);
  ASSERT_EQ(OB_SUCCESS, j_path3.parse_path());
  hit.reset();
  // tree
  ASSERT_EQ(OB_SUCCESS, j_tree->seek(j_path3, j_path3.path_node_cnt(), false, false, hit));
  ASSERT_EQ(hit.size(), 1);
  ASSERT_EQ(OB_SUCCESS, hit[0]->print(j_buf, false));
  ASSERT_EQ(0, strncmp("{\"c\": 42}", j_buf.ptr(), j_buf.length()));
  // bin
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree,
      ObJsonInType::JSON_BIN, j_bin));
  hit.reset();
  ASSERT_EQ(OB_SUCCESS, j_bin->seek(j_path3, j_path3.path_node_cnt(), false, false, hit));
  ASSERT_EQ(hit.size(), 1);
  j_buf.reset();
  ASSERT_EQ(OB_SUCCESS, hit[0]->print(j_buf, false));
  ASSERT_EQ(0, strncmp("{\"c\": 42}", j_buf.ptr(), j_buf.length()));

  // 4. seek_member_wildcard2
  common::ObString j_text4("{\"name\": \"Safari\", \"os\": \"Mac\", \"resolution\": \
      {\"x\": \"1920\", \"y\": \"1080\"}}");
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text4,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree));
  common::ObString path_text4("$.*");
  ObJsonPath j_path4(path_text4, &allocator);
  ASSERT_EQ(OB_SUCCESS, j_path4.parse_path());
  hit.reset();
  // tree
  ASSERT_EQ(OB_SUCCESS, j_tree->seek(j_path4, j_path4.path_node_cnt(), false, false, hit));
  ASSERT_EQ(hit.size(), 3);
  for (uint32_t i = 0; i < hit.size(); ++i) {
    j_buf.reset();
    ASSERT_EQ(OB_SUCCESS, hit[i]->print(j_buf, false));
    std::cout << i << ": " << j_buf.ptr() << std:: endl;
  }
  // bin
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree,
      ObJsonInType::JSON_BIN, j_bin));
  hit.reset();
  ASSERT_EQ(OB_SUCCESS, j_bin->seek(j_path4, j_path4.path_node_cnt(), false, false, hit));
  ASSERT_EQ(hit.size(), 3);
  for (uint32_t i = 0; i < hit.size(); ++i) {
    j_buf.reset();
    ASSERT_EQ(OB_SUCCESS, hit[i]->print(j_buf, false));
    std::cout << i << ": " << j_buf.ptr() << std:: endl;
  }

  // 5. seek_array_cell
  common::ObString j_text5("[1, 2, 3, 4, 5]");
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text5,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree));
  common::ObString path_text5("$[0][0 to last]");
  ObJsonPath j_path5(path_text5, &allocator);
  ASSERT_EQ(OB_SUCCESS, j_path5.parse_path());
  hit.reset();
  // tree
  ASSERT_EQ(OB_SUCCESS, j_tree->seek(j_path5, j_path5.path_node_cnt(), true, false, hit));
  ASSERT_EQ(hit.size(), 1);
  ASSERT_EQ(1, hit[0]->get_int());
  // bin
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree,
      ObJsonInType::JSON_BIN, j_bin));
  hit.reset();
  ASSERT_EQ(OB_SUCCESS, j_bin->seek(j_path5, j_path5.path_node_cnt(), true, false, hit));
  ASSERT_EQ(hit.size(), 1);
  ASSERT_EQ(1, hit[0]->get_uint());

  // 6. seek_array_range
  common::ObString j_text6("[1, 2, 3, 4, 5]");
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text6,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree));
  common::ObString path_text6("$[last-4 to last -3]");
  ObJsonPath j_path6(path_text6, &allocator);
  ASSERT_EQ(OB_SUCCESS, j_path6.parse_path());
  hit.reset();
  // tree
  ASSERT_EQ(OB_SUCCESS, j_tree->seek(j_path6, j_path6.path_node_cnt(), false, false, hit));
  ASSERT_EQ(hit.size(), 2);
  ASSERT_EQ(1, hit[0]->get_int());
  ASSERT_EQ(2, hit[1]->get_int());
  // bin
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree,
      ObJsonInType::JSON_BIN, j_bin));
  hit.reset();
  ASSERT_EQ(OB_SUCCESS, j_bin->seek(j_path6, j_path6.path_node_cnt(), false, false, hit));
  ASSERT_EQ(hit.size(), 2);
  ASSERT_EQ(1, hit[0]->get_uint());
  ASSERT_EQ(2, hit[1]->get_uint());

  // 7. seek_ellipsis
  common::ObString j_text7("{\"x\": [1, 2, 3], \"os\": [4, 5,6] }");
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text7,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree));
  common::ObString path_text7("$**[0]");
  ObJsonPath j_path7(path_text7, &allocator);
  ASSERT_EQ(OB_SUCCESS, j_path7.parse_path());
  hit.reset();
  // tree
  ASSERT_EQ(OB_SUCCESS, j_tree->seek(j_path7, j_path7.path_node_cnt(), false, false, hit));
  ASSERT_EQ(hit.size(), 2);
  ObJsonUint *val7_0 = static_cast<ObJsonUint *>(hit[0]);
  ObJsonUint *val7_1 = static_cast<ObJsonUint *>(hit[1]);
  ASSERT_EQ(1, hit[0]->get_int());
  ASSERT_EQ(4, hit[1]->get_int());
  // bin
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree,
      ObJsonInType::JSON_BIN, j_bin));
  hit.reset();
  ASSERT_EQ(OB_SUCCESS, j_bin->seek(j_path7, j_path7.path_node_cnt(), false, false, hit));
  ASSERT_EQ(hit.size(), 2);
  ASSERT_EQ(1, hit[0]->get_uint());
  ASSERT_EQ(4, hit[1]->get_uint());

  // 8. seek test1
  common::ObString j_text8("{\"width\":\"800\",\"image\":800,\"thumbnail\":{\"url\":    \
      \"http://www.example.com/image/481989943\",\"width\":100},\"ids\":[116,943,234,38793]}");
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text8,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree));
  common::ObString path_text8_0("$.ids[0]");
  ObJsonPath j_path8_0(path_text8_0, &allocator);
  ASSERT_EQ(OB_SUCCESS, j_path8_0.parse_path());
  hit.reset();
  ASSERT_EQ(OB_SUCCESS, j_tree->seek(j_path8_0, j_path8_0.path_node_cnt(), true, false, hit));
  ASSERT_EQ(hit.size(), 1);
  common::ObString path_text8_1("$[0].thumbnail.url");
  ObJsonPath j_path8_1(path_text8_1, &allocator);
  ASSERT_EQ(OB_SUCCESS, j_path8_1.parse_path());
  hit.reset();
  ASSERT_EQ(OB_SUCCESS, j_tree->seek(j_path8_1, j_path8_1.path_node_cnt(), true, false, hit));
  ASSERT_EQ(hit.size(), 1);
  common::ObString path_text8_2("$[0].width[0]");
  ObJsonPath j_path8_2(path_text8_2, &allocator);
  ASSERT_EQ(OB_SUCCESS, j_path8_2.parse_path());
  hit.reset();
  ASSERT_EQ(OB_SUCCESS, j_tree->seek(j_path8_2, j_path8_2.path_node_cnt(), true, false, hit));
  ASSERT_EQ(hit.size(), 1);
  common::ObString path_text8_3("$[0][0]");
  ObJsonPath j_path8_3(path_text8_3, &allocator);
  ASSERT_EQ(OB_SUCCESS, j_path8_3.parse_path());
  hit.reset();
  ASSERT_EQ(OB_SUCCESS, j_tree->seek(j_path8_3, j_path8_3.path_node_cnt(), true, false, hit));
  ASSERT_EQ(hit.size(), 1);

  // 9. seek test2
  hit.reset();
  common::ObString j_text9("{\"a\": 1, \"b\": {\"e\": \"foo\", \"b\": 3}}");
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text9,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree));
  common::ObString path_text9("$.*");
  ObJsonPath j_path9(path_text9, &allocator);
  ASSERT_EQ(OB_SUCCESS, j_path9.parse_path());
  ASSERT_EQ(OB_SUCCESS, j_tree->seek(j_path9, j_path9.path_node_cnt(), true, false, hit));
  ASSERT_EQ(hit.size(), 2);
  ObIJsonBase *jb_res = NULL;
  ObJsonArray j_arr_res(&allocator);
  jb_res = &j_arr_res;
  ObJsonNode *j_node = NULL;
  for (int32_t i = 0; i < hit.size(); i++) {
    j_node = static_cast<ObJsonNode *>(hit[i]);
    ASSERT_EQ(OB_SUCCESS, jb_res->array_append(j_node->clone(&allocator)));
  }
  ObString raw_str;
  ASSERT_EQ(OB_SUCCESS, jb_res->get_raw_binary(raw_str, &allocator));
  ASSERT_EQ(27, raw_str.length());
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, jb_res,
      ObJsonInType::JSON_BIN, j_bin));
  raw_str.reset();
  ASSERT_EQ(OB_SUCCESS, j_bin->get_raw_binary(raw_str, &allocator));
  ASSERT_EQ(27, raw_str.length());

  // 10. seek test3(bin->seek->print)
  common::ObString j_text10("{\"some_key\": true}");
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text10,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
  common::ObString path_text10("$.some_key");
  ObJsonPath j_path10(path_text10, &allocator);
  ASSERT_EQ(OB_SUCCESS, j_path10.parse_path());
  hit.reset();
  ASSERT_EQ(OB_SUCCESS, j_bin->seek(j_path10, j_path10.path_node_cnt(), false, false, hit));
  ASSERT_EQ(hit.size(), 1);
  raw_str.reset();
  ASSERT_EQ(OB_SUCCESS, hit[0]->get_raw_binary(raw_str, &allocator));
  ObJsonBin j_bin_tmp(raw_str.ptr(), raw_str.length());
  ObIJsonBase *j_base = &j_bin_tmp;
  j_buf.reset();
  ASSERT_EQ(OB_SUCCESS, j_bin_tmp.reset_iter());
  ASSERT_EQ(OB_SUCCESS, j_base->print(j_buf, false));
  cout << j_buf.ptr() << endl;
  ASSERT_EQ(0, strncmp("true", j_buf.ptr(), j_buf.length()));
}

TEST_F(TestJsonBase, test_oracle_seek)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObIJsonBase *j_bin = NULL;
  ObJsonBaseVector hit;
  ObJsonBuffer j_buf(&allocator);

  // 1. seek_not_exist_member
  common::ObString j_text1("{\"name\": \"Safari\", \"os\": \"Mac\"}");
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text1,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree));
  common::ObString path_text1("$.b");
  ObJsonPath j_path1(path_text1, &allocator);
  j_path1.is_mysql_ = false;
  ASSERT_EQ(OB_SUCCESS, j_path1.parse_path());
  // tree
  ASSERT_EQ(OB_SUCCESS, j_tree->seek(j_path1, j_path1.path_node_cnt(), true, false, hit));
  ASSERT_EQ(hit.size(), 0);
  // bin
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree,
      ObJsonInType::JSON_BIN, j_bin));
  hit.reset();
  ASSERT_EQ(OB_SUCCESS, j_bin->seek(j_path1, j_path1.path_node_cnt(), true, false, hit));
  ASSERT_EQ(hit.size(), 0);
  std::cout <<"seek_not_exist_member(tree & bin) success."<<std::endl;

  // 2. seek_member
  common::ObString j_text2("{\"x\": \"Safari\", \"os\": \"Mac\", \"resolution\": \
      {\"x\": \"1920\", \"y\": \"1080\"}}");
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text2,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree));
  common::ObString path_text2("$.\"resolution\".x");
  ObJsonPath j_path2(path_text2, &allocator);
  j_path2.is_mysql_ = false;
  ASSERT_EQ(OB_SUCCESS, j_path2.parse_path());
  hit.reset();
  // tree
  ASSERT_EQ(OB_SUCCESS, j_tree->seek(j_path2, j_path2.path_node_cnt(), true, false, hit));
  ASSERT_EQ(hit.size(), 1);
  ASSERT_EQ(0, strncmp("1920", hit[0]->get_data(), hit[0]->get_data_length()));
  // bin
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree,
      ObJsonInType::JSON_BIN, j_bin));
  hit.reset();
  ASSERT_EQ(OB_SUCCESS, j_bin->seek(j_path2, j_path2.path_node_cnt(), true, false, hit));
  ASSERT_EQ(hit.size(), 1);
  ASSERT_EQ(0, strncmp("1920", hit[0]->get_data(), hit[0]->get_data_length()));
  std::cout <<"seek_member(tree & bin) success."<<std::endl;

  // 3. seek_member in lax mode
  common::ObString j_text31("[1, 2, 3, 4, 5, {\"z\" : \"6\"}, 7, 8, 9]");
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text31,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree));
  common::ObString path_text31("$.z");
  ObJsonPath j_path31(path_text31, &allocator);
  j_path31.is_mysql_ = false;
  ASSERT_EQ(OB_SUCCESS, j_path31.parse_path());
  hit.reset();
  // tree
  ASSERT_EQ(OB_SUCCESS, j_tree->seek(j_path31, j_path31.path_node_cnt(), true, false, hit));
  ASSERT_EQ(hit.size(), 1);
  ASSERT_EQ(0, strncmp("6", hit[0]->get_data(), hit[0]->get_data_length()));
  // bin
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree,
      ObJsonInType::JSON_BIN, j_bin));
  hit.reset();
  ASSERT_EQ(OB_SUCCESS, j_bin->seek(j_path31, j_path31.path_node_cnt(), true, false, hit));
  ASSERT_EQ(hit.size(), 1);
  //ASSERT_EQ(6, hit[0]->get_uint());
  ASSERT_EQ(0, strncmp("6", hit[0]->get_data(), hit[0]->get_data_length()));
  std::cout <<"seek_member(tree & bin) in lax mode success."<<std::endl;


  // 3. seek_member_wildcard1
  common::ObString j_text3("{\"x\": \"Safari\", \"os\": \"Mac\", \"resolution\": \
      {\"x\": \"1920\", \"y\": \"1080\"}}");
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text3,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree));
  common::ObString path_text3("$.\"resolution\".*");
  ObJsonPath j_path3(path_text3, &allocator);
  j_path3.is_mysql_ = false;
  ASSERT_EQ(OB_SUCCESS, j_path3.parse_path());
  hit.reset();
  // tree
  ASSERT_EQ(OB_SUCCESS, j_tree->seek(j_path3, j_path3.path_node_cnt(), true, false, hit));
  ASSERT_EQ(hit.size(), 2);
  std::cout <<"seek_member_wildcard1"<<std::endl;
  for (uint32_t i = 0; i < hit.size(); ++i) {
    j_buf.reset();
    ASSERT_EQ(OB_SUCCESS, hit[i]->print(j_buf, false));
    std::cout << i << ": " << j_buf.ptr() << std:: endl;
  }
  // bin
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree,
      ObJsonInType::JSON_BIN, j_bin));
  hit.reset();
  ASSERT_EQ(OB_SUCCESS, j_bin->seek(j_path3, j_path3.path_node_cnt(), true, false, hit));
  ASSERT_EQ(hit.size(), 2);
  std::cout <<"seek_member_wildcard1 (bin)"<<std::endl;
  for (uint32_t i = 0; i < hit.size(); ++i) {
    j_buf.reset();
    ASSERT_EQ(OB_SUCCESS, hit[i]->print(j_buf, false));
    std::cout << i << ": " << j_buf.ptr() << std:: endl;
  }

  // 4. seek_member_wildcard2
  common::ObString j_text4("{\"name\": \"Safari\", \"os\": \"Mac\", \"resolution\": \
      {\"x\": \"1920\", \"y\": \"1080\"}}");
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text4,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree));
  common::ObString path_text4("$.*");
  ObJsonPath j_path4(path_text4, &allocator);
  j_path4.is_mysql_ = false;
  ASSERT_EQ(OB_SUCCESS, j_path4.parse_path());
  hit.reset();
  // tree
  ASSERT_EQ(OB_SUCCESS, j_tree->seek(j_path4, j_path4.path_node_cnt(), true, false, hit));
  ASSERT_EQ(hit.size(), 3);
  std::cout <<"seek_member_wildcard2"<<std::endl;
  for (uint32_t i = 0; i < hit.size(); ++i) {
    j_buf.reset();
    ASSERT_EQ(OB_SUCCESS, hit[i]->print(j_buf, false));
    std::cout << i << ": " << j_buf.ptr() << std:: endl;
  }
  // bin
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree,
      ObJsonInType::JSON_BIN, j_bin));
  hit.reset();
  ASSERT_EQ(OB_SUCCESS, j_bin->seek(j_path4, j_path4.path_node_cnt(), true, false, hit));
  ASSERT_EQ(hit.size(), 3);
  std::cout <<"seek_member_wildcard2 (bin)"<<std::endl;
  for (uint32_t i = 0; i < hit.size(); ++i) {
    j_buf.reset();
    ASSERT_EQ(OB_SUCCESS, hit[i]->print(j_buf, false));
    std::cout << i << ": " << j_buf.ptr() << std:: endl;
  }

  // 41. seek member_wildcard in lax_mode
  common::ObString j_text41("[1, 2, 3, 4, 5, {\"resolution\": \
      {\"x\": \"1920\", \"y\": \"1080\"}}, 7, 8, 9]");
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text41,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree));
  common::ObString path_text41("lax $.resolution.*");
  ObJsonPath j_path41(path_text41, &allocator);
  j_path41.is_mysql_ = false;
  ASSERT_EQ(OB_SUCCESS, j_path41.parse_path());
  hit.reset();
  // tree
  ASSERT_EQ(OB_SUCCESS, j_tree->seek(j_path41, j_path41.path_node_cnt(), true, false, hit));
  ASSERT_EQ(hit.size(), 2);
  std::cout <<"seek_member_wildcard in lax mode"<<std::endl;
  for (uint32_t i = 0; i < hit.size(); ++i) {
    j_buf.reset();
    ASSERT_EQ(OB_SUCCESS, hit[i]->print(j_buf, false));
    std::cout << i << ": " << j_buf.ptr() << std:: endl;
  }
  // bin
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree,
      ObJsonInType::JSON_BIN, j_bin));
  hit.reset();
  ASSERT_EQ(OB_SUCCESS, j_bin->seek(j_path41, j_path41.path_node_cnt(), true, false, hit));
  ASSERT_EQ(hit.size(), 2);
  std::cout <<"seek_member_wildcard1 in lax (bin)"<<std::endl;
  for (uint32_t i = 0; i < hit.size(); ++i) {
    j_buf.reset();
    ASSERT_EQ(OB_SUCCESS, hit[i]->print(j_buf, false));
    std::cout << i << ": " << j_buf.ptr() << std:: endl;
  }

  // 5. seek_array_cell
  common::ObString j_text5("[1, 2, 3, 4, 5]");
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text5,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree));
  common::ObString path_text5("$[0][0]");
  ObJsonPath j_path5(path_text5, &allocator);
  j_path5.is_mysql_ = false;
  ASSERT_EQ(OB_SUCCESS, j_path5.parse_path());
  hit.reset();
  // tree
  ASSERT_EQ(OB_SUCCESS, j_tree->seek(j_path5, j_path5.path_node_cnt(), true, false, hit));
  ASSERT_EQ(hit.size(), 1);
  ASSERT_EQ(1, hit[0]->get_uint());
  // bin
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree,
      ObJsonInType::JSON_BIN, j_bin));
  hit.reset();
  ASSERT_EQ(OB_SUCCESS, j_bin->seek(j_path5, j_path5.path_node_cnt(), true, false, hit));
  ASSERT_EQ(hit.size(), 1);
  ASSERT_EQ(1, hit[0]->get_uint());
  std::cout <<"seek_array_cell success"<<std::endl;


  // 6. seek_array_wildcard
  common::ObString j_text6("[1, 2]");
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text6,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree));
  common::ObString path_text6("$[*][*]");
  ObJsonPath j_path6(path_text6, &allocator);
  j_path6.is_mysql_ = false;
  ASSERT_EQ(OB_SUCCESS, j_path6.parse_path());
  hit.reset();
  // tree
  ASSERT_EQ(OB_SUCCESS, j_tree->seek(j_path6, j_path6.path_node_cnt(), true, false, hit));
  ASSERT_EQ(hit.size(), 2);
  ASSERT_EQ(1, hit[0]->get_uint());
  ASSERT_EQ(2, hit[1]->get_uint());
  std::cout <<"seek_array_wildcard(tree) success."<<std::endl;
  // bin
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree,
      ObJsonInType::JSON_BIN, j_bin));
  hit.reset();
  ASSERT_EQ(OB_SUCCESS, j_bin->seek(j_path6, j_path6.path_node_cnt(), true, false, hit));
  ASSERT_EQ(hit.size(), 2);
  ASSERT_EQ(1, hit[0]->get_uint());
  ASSERT_EQ(2, hit[1]->get_uint());
  std::cout <<"seek_array_wildcard(bin) success."<<std::endl;

  // 7. seek_ellipsis
  common::ObString j_text7("{\"name\": \"Safari\", \"x\": \"Mac\", \"resolution\": \
      {\"x\": \"1920\", \"y\": \"1080\"}}");
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text7,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree));
  common::ObString path_text7("$..x");
  ObJsonPath j_path7(path_text7, &allocator);
  j_path7.is_mysql_ = false;
  ASSERT_EQ(OB_SUCCESS, j_path7.parse_path());
  hit.reset();
  // tree
  ASSERT_EQ(OB_SUCCESS, j_tree->seek(j_path7, j_path7.path_node_cnt(), true, false, hit));
  std::cout <<"seek_ellipsis "<<std::endl;
  ASSERT_EQ(hit.size(), 2);
  for (uint32_t i = 0; i < hit.size(); ++i) {
    j_buf.reset();
    ASSERT_EQ(OB_SUCCESS, hit[i]->print(j_buf, false));
    std::cout << i << ": " << j_buf.ptr() << std:: endl;
  }
  // bin
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree,
      ObJsonInType::JSON_BIN, j_bin));
  hit.reset();
  ASSERT_EQ(OB_SUCCESS, j_bin->seek(j_path7, j_path7.path_node_cnt(), true, false, hit));
  ASSERT_EQ(hit.size(), 2);
  for (uint32_t i = 0; i < hit.size(); ++i) {
    j_buf.reset();
    ASSERT_EQ(OB_SUCCESS, hit[i]->print(j_buf, false));
    std::cout << i << "(ellipsis_bin): " << j_buf.ptr() << std:: endl;
  }

  // 71. seek_ellipsis in lax_mode
  common::ObString j_text71("[1, 2, 3, 4, 5, {\"x\": \
      {\"x\": \"1920\", \"y\": \"1080\"}}, 7, 8, 9]");
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text71,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree));
  common::ObString path_text71("lax $..x");
  ObJsonPath j_path71(path_text71, &allocator);
  j_path71.is_mysql_ = false;
  ASSERT_EQ(OB_SUCCESS, j_path71.parse_path());
  hit.reset();
  // tree
  ASSERT_EQ(OB_SUCCESS, j_tree->seek(j_path71, j_path71.path_node_cnt(), true, false, hit));
  ASSERT_EQ(hit.size(), 2);
  std::cout <<"seek_ellipsis in lax mode"<<std::endl;
  for (uint32_t i = 0; i < hit.size(); ++i) {
    j_buf.reset();
    ASSERT_EQ(OB_SUCCESS, hit[i]->print(j_buf, false));
    std::cout << i << ": " << j_buf.ptr() << std:: endl;
  }
  // bin
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree,
      ObJsonInType::JSON_BIN, j_bin));
  hit.reset();
  ASSERT_EQ(OB_SUCCESS, j_bin->seek(j_path71, j_path71.path_node_cnt(), true, false, hit));
  ASSERT_EQ(hit.size(), 2);
  for (uint32_t i = 0; i < hit.size(); ++i) {
    j_buf.reset();
    ASSERT_EQ(OB_SUCCESS, hit[i]->print(j_buf, false));
    std::cout << i << "(ellipsis_bin_lax): " << j_buf.ptr() << std:: endl;
  }

  // 8. seek_array_range
  common::ObString j_text8("[1, 2, 3, 4, 5]");
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text8,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree));
  common::ObString path_text8("$[last-4 to last -3]");
  ObJsonPath j_path8(path_text8, &allocator);
  j_path8.is_mysql_ = false;
  ASSERT_EQ(OB_SUCCESS, j_path8.parse_path());
  hit.reset();
  // tree
  ASSERT_EQ(OB_SUCCESS, j_tree->seek(j_path8, j_path8.path_node_cnt(), true, false, hit));
  ASSERT_EQ(hit.size(), 2);
  ASSERT_EQ(1, hit[0]->get_uint());
  ASSERT_EQ(2, hit[1]->get_uint());
  // bin
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree,
      ObJsonInType::JSON_BIN, j_bin));
  hit.reset();
  ASSERT_EQ(OB_SUCCESS, j_bin->seek(j_path8, j_path8.path_node_cnt(), true, false, hit));
  ASSERT_EQ(hit.size(), 2);
  ASSERT_EQ(1, hit[0]->get_uint());
  ASSERT_EQ(2, hit[1]->get_uint());

  // 9. seek_array_range2
  common::ObString j_text9("[1, 2, 3, 4, 5]");
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text9,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree));
  common::ObString path_text9("$[2, 2 to 3]");
  ObJsonPath j_path9(path_text9, &allocator);
  j_path9.is_mysql_ = false;
  ASSERT_EQ(OB_SUCCESS, j_path9.parse_path());
  hit.reset();
  // tree
  ASSERT_EQ(OB_SUCCESS, j_tree->seek(j_path9, j_path9.path_node_cnt(), true, false, hit));
  ASSERT_EQ(hit.size(), 3);
  ASSERT_EQ(3, hit[0]->get_uint());
  ASSERT_EQ(3, hit[1]->get_uint());
  ASSERT_EQ(4, hit[2]->get_uint());
  // bin
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree,
      ObJsonInType::JSON_BIN, j_bin));
  hit.reset();
  ASSERT_EQ(OB_SUCCESS, j_bin->seek(j_path9, j_path9.path_node_cnt(), true, false, hit));
  ASSERT_EQ(hit.size(), 3);
  ASSERT_EQ(3, hit[0]->get_uint());
  ASSERT_EQ(3, hit[1]->get_uint());
  ASSERT_EQ(4, hit[2]->get_uint());

  // 00. seek_special
  common::ObString j_text00("{}");
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text00,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree));
  common::ObString path_text00("$[*]");
  ObJsonPath j_path00(path_text00, &allocator);
  j_path00.is_mysql_ = false;
  ASSERT_EQ(OB_SUCCESS, j_path00.parse_path());
  hit.reset();
  // tree
  ASSERT_EQ(OB_SUCCESS, j_tree->seek(j_path00, j_path00.path_node_cnt(), true, false, hit));
  ASSERT_EQ(hit.size(), 1);
  for (uint32_t i = 0; i < hit.size(); ++i) {
    j_buf.reset();
    ASSERT_EQ(OB_SUCCESS, hit[i]->print(j_buf, false));
    std::cout << i << ": " << j_buf.ptr() << std:: endl;
  }
  // bin
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree,
      ObJsonInType::JSON_BIN, j_bin));
  hit.reset();
  ASSERT_EQ(OB_SUCCESS, j_bin->seek(j_path00, j_path00.path_node_cnt(), true, false, hit));
  ASSERT_EQ(hit.size(), 1);
  for (uint32_t i = 0; i < hit.size(); ++i) {
    j_buf.reset();
    ASSERT_EQ(OB_SUCCESS, hit[i]->print(j_buf, false));
    std::cout << i << ": " << j_buf.ptr() << std:: endl;
  }

  // 10. seek_numeric_item_method (abs, floor, ceiling)
  common::ObString j_text10("[1.1, 2, 3, 4, 5]");
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text10,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree));
  common::ObString path_text10("$[0].ceiling()");
  ObJsonPath j_path10(path_text10, &allocator);
  j_path10.is_mysql_ = false;
  ASSERT_EQ(OB_SUCCESS, j_path10.parse_path());
  hit.reset();
  // tree
  ASSERT_EQ(OB_SUCCESS, j_tree->seek(j_path10, j_path10.path_node_cnt(), true, false, hit));
  ASSERT_EQ(hit.size(), 1);
  ASSERT_EQ(2, hit[0]->get_int());
  // bin
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree,
      ObJsonInType::JSON_BIN, j_bin));
  hit.reset();
  ASSERT_EQ(OB_SUCCESS, j_bin->seek(j_path10, j_path10.path_node_cnt(), true, false, hit));
  ASSERT_EQ(hit.size(), 1);
  ASSERT_EQ(2, hit[0]->get_int());
}

TEST_F(TestJsonBase, test_seek_str_cmp)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObIJsonBase *j_bin = NULL;
  ObJsonBaseVector hit;
  ObJsonBuffer j_buf(&allocator);

  common::ObString var_name("sql");
  common::ObString j_text_var("23");
  ObIJsonBase *j_tree_var = NULL;

  // init pass_map
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text_var,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree_var));
  PassingMap pass_map;
  ASSERT_EQ(OB_SUCCESS, pass_map.create(2, "json_sql_var"));
  ASSERT_EQ(OB_SUCCESS, pass_map.set_refactored(var_name, j_tree_var));
  ASSERT_EQ(OB_SUCCESS, pass_map.set_refactored("test", nullptr));

  // 1. starts with (string)
  common::ObString j_text1("{\"a\": \"Safari\", \"b\": true, \"c\": 123}");
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text1,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree));
  common::ObString path_text1("$?( @.a starts with \"Saf\").c");
  ObJsonPath j_path1(path_text1, &allocator);
  j_path1.is_mysql_ = false;
  ASSERT_EQ(OB_SUCCESS, j_path1.parse_path());
  // tree
  ASSERT_EQ(OB_SUCCESS, j_tree->seek(j_path1, j_path1.path_node_cnt(), true, false, hit));
  ASSERT_EQ(hit.size(), 1);
  ASSERT_EQ(123, hit[0]->get_int());
  // bin
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree,
      ObJsonInType::JSON_BIN, j_bin));
  hit.reset();
  ASSERT_EQ(OB_SUCCESS, j_bin->seek(j_path1, j_path1.path_node_cnt(), true, false, hit));
  ASSERT_EQ(hit.size(), 1);
  ASSERT_EQ(123, hit[0]->get_int());

  // 2. starts with (trans to string)
  common::ObString path_text2("$?( @.b starts with \"true\").c");
  ObJsonPath j_path2(path_text2, &allocator);
  j_path2.is_mysql_ = false;
  ASSERT_EQ(OB_SUCCESS, j_path2.parse_path());
  // tree
  hit.reset();
  ASSERT_EQ(OB_SUCCESS, j_tree->seek(j_path2, j_path2.path_node_cnt(), true, false, hit));
  ASSERT_EQ(hit.size(), 1);
  ASSERT_EQ(123, hit[0]->get_int());
  // bin
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree,
      ObJsonInType::JSON_BIN, j_bin));
  hit.reset();
  ASSERT_EQ(OB_SUCCESS, j_bin->seek(j_path2, j_path2.path_node_cnt(), true, false, hit));
  ASSERT_EQ(hit.size(), 1);
  ASSERT_EQ(123, hit[0]->get_int());

  // 3. has substring (string)
  common::ObString path_text3("$?( @.a has substring \"ri\").c");
  ObJsonPath j_path3(path_text3, &allocator);
  j_path3.is_mysql_ = false;
  ASSERT_EQ(OB_SUCCESS, j_path3.parse_path());
  // tree
  hit.reset();
  ASSERT_EQ(OB_SUCCESS, j_tree->seek(j_path3, j_path3.path_node_cnt(), true, false, hit));
  ASSERT_EQ(hit.size(), 1);
  ASSERT_EQ(123, hit[0]->get_int());
  // bin
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree,
      ObJsonInType::JSON_BIN, j_bin));
  hit.reset();
  ASSERT_EQ(OB_SUCCESS, j_bin->seek(j_path3, j_path3.path_node_cnt(), true, false, hit));
  ASSERT_EQ(hit.size(), 1);
  ASSERT_EQ(123, hit[0]->get_int());

  // 4. has substring (trans to string)
  common::ObString path_text4("$?( @.c has substring $sql).c");
  ObJsonPath j_path4(path_text4, &allocator);
  j_path4.is_mysql_ = false;
  ASSERT_EQ(OB_SUCCESS, j_path4.parse_path());
  // tree
  hit.reset();
  ASSERT_EQ(OB_SUCCESS, j_tree->seek(j_path4, j_path4.path_node_cnt(), true, false, hit, &pass_map));
  ASSERT_EQ(hit.size(), 1);
  ASSERT_EQ(123, hit[0]->get_int());
  // bin
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree,
      ObJsonInType::JSON_BIN, j_bin));
  hit.reset();
  ASSERT_EQ(OB_SUCCESS, j_bin->seek(j_path4, j_path4.path_node_cnt(), true, false, hit, &pass_map));
  ASSERT_EQ(hit.size(), 1);
  ASSERT_EQ(123, hit[0]->get_int());
  std::cout <<"seek_not_exist_member(tree & bin) success."<<std::endl;
}

TEST_F(TestJsonBase, test_seek_func)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObIJsonBase *j_bin = NULL;
  ObJsonBaseVector hit;
  ObJsonBuffer j_buf(&allocator);

  // 11. seek_type
  common::ObString j_text11("[1.1, true, null, \"a string\", {\"z\": 1}, [6]]");
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text11,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree));
  common::ObString path_text11("$[*].type()");
  ObJsonPath j_path11(path_text11, &allocator);
  j_path11.is_mysql_ = false;

  ASSERT_EQ(OB_SUCCESS, j_path11.parse_path());
  hit.reset();
  // tree
  ASSERT_EQ(OB_SUCCESS, j_tree->seek(j_path11, j_path11.path_node_cnt(), true, false, hit));
  std::cout <<"seek_type (tree)"<<std::endl;
  ASSERT_EQ(hit.size(), 6);
  for (uint32_t i = 0; i < hit.size(); ++i) {
    j_buf.reset();
    ASSERT_EQ(OB_SUCCESS, hit[i]->print(j_buf, false));
    std::cout << i << ": " << j_buf.ptr() << std:: endl;
  }
  // bin
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree,
      ObJsonInType::JSON_BIN, j_bin));
  hit.reset();
  ASSERT_EQ(OB_SUCCESS, j_bin->seek(j_path11, j_path11.path_node_cnt(), true, false, hit));
  std::cout <<"seek_type (bin)"<<std::endl;
  ASSERT_EQ(hit.size(), 6);
  for (uint32_t i = 0; i < hit.size(); ++i) {
    j_buf.reset();
    ASSERT_EQ(OB_SUCCESS, hit[i]->print(j_buf, false));
    std::cout << i << ": " << j_buf.ptr() << std:: endl;
  }

  // 12. seek_length
  common::ObString j_text12("[1.1, true, null, \"a string\", {\"z\": 1}, [6]]");
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text12,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree));
  common::ObString path_text12("$[*].length()");
  ObJsonPath j_path12(path_text12, &allocator);
  j_path12.is_mysql_ = false;
  ASSERT_EQ(OB_SUCCESS, j_path12.parse_path());
  hit.reset();
  // tree
  ASSERT_EQ(OB_SUCCESS, j_tree->seek(j_path12, j_path12.path_node_cnt(), true, false, hit));
  std::cout <<"seek_length (tree)"<<std::endl;
  ASSERT_EQ(hit.size(), 6);
  for (uint32_t i = 0; i < hit.size(); ++i) {
    j_buf.reset();
    ASSERT_EQ(OB_SUCCESS, hit[i]->print(j_buf, false));
    std::cout << i << ": " << j_buf.ptr() << std:: endl;
  }
  // bin
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree,
      ObJsonInType::JSON_BIN, j_bin));
  hit.reset();
  ASSERT_EQ(OB_SUCCESS, j_bin->seek(j_path12, j_path12.path_node_cnt(), true, false, hit));
  std::cout <<"seek_length (bin)"<<std::endl;
  ASSERT_EQ(hit.size(), 6);
  for (uint32_t i = 0; i < hit.size(); ++i) {
    j_buf.reset();
    ASSERT_EQ(OB_SUCCESS, hit[i]->print(j_buf, false));
    std::cout << i << ": " << j_buf.ptr() << std:: endl;
  }

  // 13. seek_bool/boolOnly
  common::ObString j_text13("[1.1, true, false, null, \"a string\", {\"z\": 1}, [6]]");
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text13,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree));
  common::ObString path_text13("$[*].boolean()");
  ObJsonPath j_path13(path_text13, &allocator);
  j_path13.is_mysql_ = false;
  ASSERT_EQ(OB_SUCCESS, j_path13.parse_path());
  hit.reset();
  // tree
  ASSERT_EQ(OB_SUCCESS, j_tree->seek(j_path13, j_path13.path_node_cnt(), true, false, hit));
  std::cout <<"seek_bool (tree)"<<std::endl;
  ASSERT_EQ(hit.size(), 7);
  for (uint32_t i = 0; i < hit.size(); ++i) {
    j_buf.reset();
    ASSERT_EQ(OB_SUCCESS, hit[i]->print(j_buf, false));
    std::cout << i << ": " << j_buf.ptr() << std:: endl;
  }
  // bin
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree,
      ObJsonInType::JSON_BIN, j_bin));
  hit.reset();
  ASSERT_EQ(OB_SUCCESS, j_bin->seek(j_path13, j_path13.path_node_cnt(), true, false, hit));
  std::cout <<"seek_bool (bin)"<<std::endl;
  ASSERT_EQ(hit.size(), 7);
  for (uint32_t i = 0; i < hit.size(); ++i) {
    j_buf.reset();
    ASSERT_EQ(OB_SUCCESS, hit[i]->print(j_buf, false));
    std::cout << i << ": " << j_buf.ptr() << std:: endl;
  }

  // 14. seek_date & timestamp
  common::ObString j_text14("[\"2022-8-30\", \"2015-4-15 11:12:00.123456\"]");
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text14,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree));
  common::ObString path_text14("$[1].timestamp()");
  ObJsonPath j_path14(path_text14, &allocator);
  j_path14.is_mysql_ = false;
  ASSERT_EQ(OB_SUCCESS, j_path14.parse_path());
  hit.reset();
  // tree
  ASSERT_EQ(OB_SUCCESS, j_tree->seek(j_path14, j_path14.path_node_cnt(), true, false, hit));
  std::cout <<"seek_date& timestamp (tree)"<<std::endl;
  ASSERT_EQ(hit.size(), 1);
  for (uint32_t i = 0; i < hit.size(); ++i) {
    j_buf.reset();
    ASSERT_EQ(OB_SUCCESS, hit[i]->print(j_buf, false));
    std::cout << i << ": " << j_buf.ptr() << std:: endl;
  }
  // bin
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree,
      ObJsonInType::JSON_BIN, j_bin));
  hit.reset();
  ASSERT_EQ(OB_SUCCESS, j_bin->seek(j_path14, j_path14.path_node_cnt(), true, false, hit));
  std::cout <<"seek_date& timestamp (bin)"<<std::endl;
  ASSERT_EQ(hit.size(), 1);
  for (uint32_t i = 0; i < hit.size(); ++i) {
    j_buf.reset();
    ASSERT_EQ(OB_SUCCESS, hit[i]->print(j_buf, false));
    std::cout << i << ": " << j_buf.ptr() << std:: endl;
  }

  // 15. seek_double
  common::ObString j_text15("[\"1.1\"]");
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text15,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree));
  common::ObString path_text15("$[0].double()");
  ObJsonPath j_path15(path_text15, &allocator);
  j_path15.is_mysql_ = false;
  ASSERT_EQ(OB_SUCCESS, j_path15.parse_path());
  hit.reset();
  // tree
  ASSERT_EQ(OB_SUCCESS, j_tree->seek(j_path15, j_path15.path_node_cnt(), true, false, hit));
  std::cout <<"seek_double (tree)"<<std::endl;
  ASSERT_EQ(hit.size(), 1);
  ASSERT_EQ(1.1, hit[0]->get_double());
  // bin
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree,
      ObJsonInType::JSON_BIN, j_bin));
  hit.reset();
  ASSERT_EQ(OB_SUCCESS, j_bin->seek(j_path15, j_path15.path_node_cnt(), true, false, hit));
  std::cout <<"seek_double (bin)"<<std::endl;
  ASSERT_EQ(hit.size(), 1);
  ASSERT_EQ(1.1, hit[0]->get_double());

  // 16. seek_number
  common::ObString j_text16("[\"1.1\"]");
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text16,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree));
  common::ObString path_text16("$[0].number()");
  ObJsonPath j_path16(path_text16, &allocator);
  j_path16.is_mysql_ = false;
  ASSERT_EQ(OB_SUCCESS, j_path16.parse_path());
  hit.reset();
  // tree
  ASSERT_EQ(OB_SUCCESS, j_tree->seek(j_path16, j_path16.path_node_cnt(), true, false, hit));
  std::cout <<"seek_number (tree)"<<std::endl;
  ASSERT_EQ(hit.size(), 1);
  ASSERT_EQ(1.1, hit[0]->get_double());
  // bin
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree,
      ObJsonInType::JSON_BIN, j_bin));
  hit.reset();
  ASSERT_EQ(OB_SUCCESS, j_bin->seek(j_path16, j_path16.path_node_cnt(), true, false, hit));
  std::cout <<"seek_number (bin)"<<std::endl;
  ASSERT_EQ(hit.size(), 1);
  ASSERT_EQ(1.1, hit[0]->get_double());

  // 17. seek_string
  common::ObString j_text17("[0, true , \"a string\"]");
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text17,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree));
  common::ObString path_text17("$[*].string()");
  ObJsonPath j_path17(path_text17, &allocator);
  j_path17.is_mysql_ = false;
  ASSERT_EQ(OB_SUCCESS, j_path17.parse_path());
  hit.reset();
  // tree
  ASSERT_EQ(OB_SUCCESS, j_tree->seek(j_path17, j_path17.path_node_cnt(), true, false, hit));
  std::cout <<"seek_string (tree)"<<std::endl;
  ASSERT_EQ(hit.size(), 3);
  for (uint32_t i = 0; i < hit.size(); ++i) {
    j_buf.reset();
    ASSERT_EQ(OB_SUCCESS, hit[i]->print(j_buf, false));
    std::cout << i << ": " << j_buf.ptr() << std:: endl;
  }
  // bin
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree,
      ObJsonInType::JSON_BIN, j_bin));
  hit.reset();
  ASSERT_EQ(OB_SUCCESS, j_bin->seek(j_path17, j_path17.path_node_cnt(), true, false, hit));
  std::cout <<"seek_string (bin)"<<std::endl;
  ASSERT_EQ(hit.size(), 3);
  for (uint32_t i = 0; i < hit.size(); ++i) {
    j_buf.reset();
    ASSERT_EQ(OB_SUCCESS, hit[i]->print(j_buf, false));
    std::cout << i << ": " << j_buf.ptr() << std:: endl;
  }

  // 18. seek_size
  common::ObString j_text18("[1, 2, \"test\", {\"a\": 3}]");
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text18,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree));
  common::ObString path_text18("$[0].size()");
  ObJsonPath j_path18(path_text18, &allocator);
  j_path18.is_mysql_ = false;
  ASSERT_EQ(OB_SUCCESS, j_path18.parse_path());
  hit.reset();
  // tree
  ASSERT_EQ(OB_SUCCESS, j_tree->seek(j_path18, j_path18.path_node_cnt(), true, false, hit));
  std::cout <<"seek_size (tree)"<<std::endl;
  ASSERT_EQ(hit.size(), 1);
  ASSERT_EQ(1, hit[0]->get_uint());
  // bin
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree,
      ObJsonInType::JSON_BIN, j_bin));
  hit.reset();
  ASSERT_EQ(OB_SUCCESS, j_bin->seek(j_path18, j_path18.path_node_cnt(), true, false, hit));
  std::cout <<"seek_size (bin)"<<std::endl;
  ASSERT_EQ(hit.size(), 1);
  ASSERT_EQ(1, hit[0]->get_uint());

  // 19. seek_upper & lower
  common::ObString j_text19("[\"aBcDeFg\"]");
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text19,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree));
  common::ObString path_text19("$[0].lower()");
  ObJsonPath j_path19(path_text19, &allocator);
  j_path19.is_mysql_ = false;
  hit.reset();
  ASSERT_EQ(OB_SUCCESS, j_path19.parse_path());
  // tree
  ASSERT_EQ(OB_SUCCESS, j_tree->seek(j_path19, j_path19.path_node_cnt(), true, false, hit));
  std::cout <<"seek_trans (tree)"<<std::endl;
  ASSERT_EQ(hit.size(), 1);
  ObString ans1(hit[0]->get_data_length(), hit[0]->get_data());
  ASSERT_EQ(ans1, "abcdefg");
  // bin
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree,
      ObJsonInType::JSON_BIN, j_bin));
  common::ObString path_text20("$[0].upper()");
  ObJsonPath j_path20(path_text20, &allocator);
  j_path20.is_mysql_ = false;
  ASSERT_EQ(OB_SUCCESS, j_path20.parse_path());
  hit.reset();
  ASSERT_EQ(OB_SUCCESS, j_bin->seek(j_path20, j_path20.path_node_cnt(), true, false, hit));
  std::cout <<"seek_trans (bin)"<<std::endl;
  ASSERT_EQ(hit.size(), 1);
  ObString ans2(hit[0]->get_data_length(), hit[0]->get_data());
  ASSERT_EQ(ans2, "ABCDEFG");
}

TEST_F(TestJsonBase, test_seek_bad_filter)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObIJsonBase *j_bin = NULL;
  ObJsonBaseVector hit;
  ObJsonBuffer j_buf(&allocator);
  INIT_SUCC(ret);
/*
  bad filter
  [\"a\", 2, 3, {\"resolution\" : {\"x\": 1920, \"y\": \"2015-4-15 11:12:00.123456\", \"z\": true}}, [5, 6]]
    1. $?( @[3].resolution.x.number() == \"1920\")
    2. $?( @[3].resolution.x.numberOnly() == \"1920\")
    3. $?( @[3].resolution.x.length() == \"1920\")
    4. $?( @[3].resolution.x.abs() == \"1920\")
    5. $?( @[3].resolution.x.floor() == \"1920\")
    6. $?( @[3].resolution.x.ceiling() == \"1920\")
    7. $?( @[3].resolution.x.double() == \"1920\")
    8. $?( @[4].double() == \"1920\")
    9. $[4]?( @.double() == \"1920\")
    10. $?(\"1920\" ==  @[3].resolution.x.number())
    11. $?(\"1920\" ==   @[3].resolution.x.numberOnly())
    12. $?(\"1920\" ==   @[3].resolution.x.length())
    13. $?( \"1920\" ==  @[3].resolution.x.abs())
    14. $?(\"1920\" ==   @[3].resolution.x.floor())
    15. $?(\"1920\" ==   @[3].resolution.x.ceiling())
    16. $?(\"1920\" ==   @[3].resolution.x.double() )
    17. $?(\"1920\" ==   @[4].double() )
    18. $[4]?(\"1920\" ==   @.double())
    19. $?( @[3].resolution.x.string() == 1920)
    20. $?( @[3].resolution.x.stringOnly() == 1920)
    21. $?( @[3].resolution.x.lower() == 1920)
    22. $?( @[3].resolution.x.upper() == 1920)
    23. $?( @[3].resolution.x.type() == 1920)
    24. $?( @[3].resolution.x.date() == 1920)
    25. $?( @[3].resolution.x.timestamp() == 1920)
    26. $?( @[3].resolution.z.boolean() == 1920)
    27. $?( @[3].resolution.x.string() == 1920)
    28. $?( @[3].resolution.x.stringOnly() == 1920)
    29. $?( @[3].resolution.x.lower() == 1920)
    30. $?( @[3].resolution.x.upper() == 1920)
    31. $?( @[3].resolution.x.type() == 1920)
    32. $?( @[3].resolution.x.date() == 1920)
    33. $?( @[3].resolution.x.timestamp() == 1920)
    34. $?( @[3].resolution.z.boolean() == 1920)
    35. $?( @[3].resolution.z.boolean() == \" true\")
    36. $?( @[3].resolution.z.boolean() == \"true \")
    37. $?( @[3].resolution.z.boolean() == \"fals\")
    38. $?( @[3].resolution.z.boolean() == \"falsee\")
    39. $?(true == null)
    40. $?(false == 1)
    41. $?(\"1\" == 1)
    42. $?(\"1\" == true)
*/
  common::ObString j_text1("[\"a\", 2, 3, {\"resolution\" : {\"x\": 1920, \"y\": \"2015-4-15 11:12:00.123456\", \"z\": true}}, [5, 6]]");
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text1,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree));
  common::ObString path_text1("$?( @[3].resolution.z.boolean() == \"false\")");
  ObJsonPath j_path1(path_text1, &allocator);
  j_path1.is_mysql_ = false;
  ASSERT_EQ(OB_SUCCESS, j_path1.parse_path());
  // tree
  ret = j_tree->seek(j_path1, j_path1.path_node_cnt(), true, false, hit);
  ASSERT_EQ(false, OB_FAIL(ret));
  // bin
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree,
      ObJsonInType::JSON_BIN, j_bin));
  hit.reset();
  ret = OB_SUCCESS;
  ret =  j_bin->seek(j_path1, j_path1.path_node_cnt(), true, false, hit);
  ASSERT_EQ(false, OB_FAIL(ret));
}

TEST_F(TestJsonBase, test_seek_filter)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObIJsonBase *j_bin = NULL;
  ObJsonBaseVector hit;
  ObJsonBuffer j_buf(&allocator);

  /*
  1.compare (scalar，scalar) :
    (number, number)
        1. $?(1 == 1)
        2. $?(1 < 15535.6
        3. $?(1 <= 15535.6
        4.  "$?(15535.6 > 1.346647)"
        5. "$?(15535.6 >= 1.346647)"
        6.  "$?(15535.6 != 1.346647)"
    (string, string)
        7. $?(\"abc\" != \"abc\")
        8. $?(\"abc\" == \"abc\")
        9. $?(\"abc\" >= \"abc\")
        10. $?(\"abc\" <= \"abc\")
        11. $?(\"abcd\" > \"abc\")
    (bool, bool/string)
        12. $?(true == true)
        13. $?(false == false)
        14. $?(true == false)
        15. $?( true ==  \"true\")
        16. $?( true ==  \"TrUe\")
        17. $?( \"false\" == false)
        18. $?( \"fALse\" == false)
    (null, null)
        19. $?( null ==  null)
*/
  common::ObString j_text1("[\"a\", 2, 3, {\"resolution\" : {\"x\": 1920, \"y\": \"2015-4-15 11:12:00.123456\", \"z\": \"trueeee\"}}, [5, 6]]");
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text1,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree));
  common::ObString path_text1("$?( true ==  \"TrUe\")");
  ObJsonPath j_path1(path_text1, &allocator);
  j_path1.is_mysql_ = false;
  ASSERT_EQ(OB_SUCCESS, j_path1.parse_path());
  // tree
  ASSERT_EQ(OB_SUCCESS, j_tree->seek(j_path1, j_path1.path_node_cnt(), true, false, hit));
  ASSERT_EQ(hit.size(), 1);
  // bin
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree,
      ObJsonInType::JSON_BIN, j_bin));
  hit.reset();
  ASSERT_EQ(OB_SUCCESS, j_bin->seek(j_path1, j_path1.path_node_cnt(), true, false, hit));
  ASSERT_EQ(hit.size(), 1);
  std::cout << "1.compare (scalar, scalar) success"<< std::endl;

  common::ObString path_text0("$?(@[3].resolution.z.boolean() !=  \"true\")");
  ObJsonPath j_path0(path_text0, &allocator);
  j_path0.is_mysql_ = false;
  ASSERT_EQ(OB_SUCCESS, j_path0.parse_path());
  // tree
  hit.reset();
  ASSERT_EQ(OB_SUCCESS, j_tree->seek(j_path0, j_path0.path_node_cnt(), true, false, hit));
  ASSERT_EQ(hit.size(), 1);
  ASSERT_NE(false, hit[0]->get_boolean());
  // bin
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree,
      ObJsonInType::JSON_BIN, j_bin));
  hit.reset();
  ASSERT_EQ(OB_SUCCESS, j_bin->seek(j_path0, j_path0.path_node_cnt(), true, false, hit));
  ASSERT_EQ(hit.size(), 1);

/*
  2.compare (scalar，subpath) / (subpath，scalar):
    "[\"a\", 2, 3, {\"resolution\" : {\"x\": 1920, \"y\": \"2015-4-15 11:12:00.1234567 -07:00"\", \"z\": \"true\" }}, [5, 6]]"
    20. $?(2 == @[1])
    21. $?(@[1] == 2)
    22. $?(\"1930\" >= @[3].resolution.x)
    23. $?(@[3].resolution.x <=  \"1930\" )
    24. $?(@[3].resolution.x.number() != 1930)   // 如果是$?( @[3].resolution.x.number() != \"1930\") 则报错
    25. $?(1930 != @[3].resolution.x.number())
    26. $?(@[3].resolution.x.string() == \"1920\")
    27. $?(\"1920\" == @[3].resolution.x.string())
    28. $?(@[3].resolution.z.boolean() == \"true\")
    29. $?(\"true\" == @[3].resolution.z.boolean())
    30. $?(\"false\" != @[3].resolution.z.boolean())
    31. $?(@[3].resolution.z.boolean() == \"false\")  // change @.z = "false"
    32. $?( \"FaLsE\" ==@[3].resolution.z.boolean())
    33. $?(@[3].resolution.y.timestamp() == \"2015-4-15 11:12:00.123456\")
    34. $?( \"2015-4-15 11:12:00.123456\" == @[3].resolution.y.timestamp())
    35. $?(\"2015-4-15\" == @[3].resolution.y.date())
    36. $?(@[3].resolution.y.date() == \"2015-4-15\")
*/
  common::ObString path_text2("$?(@[3].resolution.x <=  \"1930\")");
  ObJsonPath j_path2(path_text2, &allocator);
  j_path2.is_mysql_ = false;
  ASSERT_EQ(OB_SUCCESS, j_path2.parse_path());
  // tree
  hit.reset();
  ASSERT_EQ(OB_SUCCESS, j_tree->seek(j_path2, j_path2.path_node_cnt(), true, false, hit));
  ASSERT_EQ(hit.size(), 1);
  // bin
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree,
      ObJsonInType::JSON_BIN, j_bin));
  hit.reset();
  ASSERT_EQ(OB_SUCCESS, j_bin->seek(j_path2, j_path2.path_node_cnt(), true, false, hit));
  ASSERT_EQ(hit.size(), 1);
  std::cout << "2.compare (scalar, subpath) / (subpath, scalar): success"<< std::endl;

/*
  3. (subpath, sql_var) / (sql_var, sub_path)
    37. $?($v1 == @[1])      // $v1 = 2
    38. $?(@[1] == $v1)
    39. $?($v1 >= @[3].resolution.x)  // $v1 = 1930
    40. $?(@[3].resolution.x <= $v1 )
    41. $?(@[3].resolution.x.number() != $v1)
    42. $?($v1 != @[3].resolution.x.number())
    43. $?(@[3].resolution.x.string() == $v1)    // $v1 = \"1920\"
    44. $?($v1 == @[3].resolution.x.string())
    45. $?(@[3].resolution.z.boolean() == $v1)  // $v1 = true
    46. $?($v1== @[3].resolution.z.boolean())
    47. $?($v1 != @[3].resolution.z.boolean())  // $v1 = false
    48. $?(@[3].resolution.z.boolean() == $v1)  // change @.z = "false"
    49. $?( $v1 ==@[3].resolution.z.boolean()) // $v1 = fAlse
    50. $?(@[3].resolution.y.timestamp() ==$v1)  // $v1 =  \"2015-4-15 11:12:00.123456\"
    51. $?($v1 == @[3].resolution.y.timestamp())
    52. $?($v1 == @[3].resolution.y.date()) / $v1 =  \"2015-4-15\"
    53. $?(@[3].resolution.y.date() == $v1")
*/
  // common::ObString path_text3("$?(@[3].resolution.z.boolean() ==$v1)");
  common::ObString path_text3("$?(exists(@[3].resolution.*?($v1 == @[0])))");
  common::ObString var_name("v1");
  common::ObString j_text_var("1920");
  ObIJsonBase *j_tree_var = NULL;

  // init pass_map
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text_var,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree_var));
  PassingMap pass_map;
  ASSERT_EQ(OB_SUCCESS, pass_map.create(2, "json_sql_var"));
  ASSERT_EQ(OB_SUCCESS, pass_map.set_refactored(var_name, j_tree_var));
  ASSERT_EQ(OB_SUCCESS, pass_map.set_refactored("test", nullptr));
  ObJsonPath j_path3(path_text3, &allocator);
  j_path3.is_mysql_ = false;
  ASSERT_EQ(OB_SUCCESS, j_path3.parse_path());
  // tree
  hit.reset();
  ASSERT_EQ(OB_SUCCESS, j_tree->seek(j_path3, j_path3.path_node_cnt(), true, false, hit, &pass_map));
  ASSERT_EQ(hit.size(), 1);
  // bin
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree,
      ObJsonInType::JSON_BIN, j_bin));
  hit.reset();
  ASSERT_EQ(OB_SUCCESS, j_bin->seek(j_path3, j_path3.path_node_cnt(), true, false, hit, &pass_map));
  ASSERT_EQ(hit.size(), 1);
  std::cout << "3. (subpath, sql_var) / (sql_var, sub_path) success"<< std::endl;

/*
  4. exists/!exists
  exists(sub_path)
    54. $?(exists(@[3].resolution.x?(@[0] == 1920)))  ——> true
    55. $?(exists(@[3].resolution.x?(@[0] <= 1920)))  ——> true
    56. $?(exists(@[3].resolution.x?(@[0] < 1920)))   ——> false
    57. $?(exists(@[3].resolution.x?(@[0].string() == \"1920\")))  ——> true
    58. $?(exists(@[3].resolution.y?(@[0].date() == \"2015-4-15\")))  ——> true
    59. $?(exists(@[3].resolution.y?(@[0].date() == \"2015-8-15\")))  ——> false
    60. $[3].resolution?(exists(@.*?(@[0].date() == \"2015-4-15\")))  ——> true
    61. $[3].resolution?(exists(@.*?(@[0].date() == \"2015-8-15\")))  ——> false
    62. $?(exists(@[3].resolution.y?(@[0].timestamp() == \"2015-4-15 11:12:00.123456\")))  ——> true
    63. $?(exists(@[3].resolution.y?(@[0].timestamp() == \"2015-8-15 11:12:00.123456\")))  ——> false
    64. $[3].resolution?(exists(@.*?(@[0].timestamp() == \"2015-4-15 11:12:00.123456\")))  ——> true
    65. $[3].resolution?(exists(@.*?(@[0].timestamp() == \"2015-8-15 11:12:00.123456\")))  ——> false
    66. $?(exists(@[5].resolution?(@.* == 1920).x.number())) ——> false
    67. $?(exists(@[3].resolution?(@.* == 1920).x.number())) ——> true
    68. $?(!exists(@[3].resolution.x?(@[0] == 1920)))  ——> false
    69. $?(!exists(@[3].resolution.x?(@[0] <= 1920)))  ——> false
    70. $?(!exists(@[3].resolution.x?(@[0] < 1920)))    ——> ture
    71. $?(!exists(@[3].resolution.x?(@[0].string() == \"1920\")))  ——> false
    72. $?(!exists(@[3].resolution.y?(@[0].date() == \"2015-4-15\")))  ——> false
    73. $?(!exists(@[3].resolution.y?(@[0].date() == \"2015-8-15\")))  ——> true
    74. $[3].resolution?(exists(@.*?(@[0].date() == \"2015-4-15\")))  ——> false
    75. $[3].resolution?(exists(@.*?(@[0].date() == \"2015-8-15\")))  ——> true
    76. $?(!exists(@[3].resolution.y?(@[0].timestamp() == \"2015-4-15 11:12:00.123456\")))  ——> false
    77. $?(!exists(@[3].resolution.y?(@[0].timestamp() == \"2015-8-15 11:12:00.123456\")))  ——> true
    78. $[3].resolution?(!exists(@.*?(@[0].timestamp() == \"2015-4-15 11:12:00.123456\")))  ——> false
    79. $[3].resolution?(!exists(@.*?(@[0].timestamp() == \"2015-8-15 11:12:00.123456\")))  ——> true
    80. $?(!exists(@[5].resolution?(@.* == 1920).x.number()))     ——> true
    81. $?(!exists(@[5].resolution?(@.* == 1920).x.number()))     ——> false
    82. $[3]?(exists(@.resolution?(@.x == 1920))).resolution.z    ——> hit.size() = 1, hit[0]->get_boolean() == true
    83. $[3]?(exists(@.resolution?(@.x == 1920))).resolution.xyz  ——> hit.size() = 0
*/
  common::ObString path_text4("$?(exists(@[3].resolution?(@.* == 1920).x.number())) ");
  ObJsonPath j_path4(path_text4, &allocator);
  j_path4.is_mysql_ = false;
  ASSERT_EQ(OB_SUCCESS, j_path4.parse_path());
  // tree
  hit.reset();
  ASSERT_EQ(OB_SUCCESS, j_tree->seek(j_path4, j_path4.path_node_cnt(), true, false, hit));
  ASSERT_EQ(hit.size(), 1);
  // bin
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree,
      ObJsonInType::JSON_BIN, j_bin));
  hit.reset();
  ASSERT_EQ(OB_SUCCESS, j_bin->seek(j_path4, j_path4.path_node_cnt(), true, false, hit));
  ASSERT_EQ(hit.size(), 1);
  std::cout << "4. exists/!exists success"<< std::endl;

  /*
  5. seek recursively
    84. $?(@[*] == 2)
    85. $?(@[*].number() > 10)  ——> false
    86. $[*]?(@[*] == 6)
    87. $[*]?(@[*].number() == 5)  ——>hit.size() = 5
    88. $?(@[*].resolution.* == 1920)
  */
  common::ObString path_text5("$?(@[*].resolution.* == 1920)");
  ObJsonPath j_path5(path_text5, &allocator);
  j_path5.is_mysql_ = false;
  ASSERT_EQ(OB_SUCCESS, j_path5.parse_path());
  // tree
  hit.reset();
  ASSERT_EQ(OB_SUCCESS, j_tree->seek(j_path5, j_path5.path_node_cnt(), true, false, hit));
  ASSERT_EQ(hit.size(), 1);
  /*
  for (int i=0; i<hit.size(); ++i) {
    if(hit[i]->get_boolean() == true) {
      std::cout<<i<<": true"<<std::endl;
    } else if (hit[i]->get_boolean() == true) {
      std::cout<<i<<": false"<<std::endl;
    } else {
        std::cout<<i<<": not bool"<<std::endl;
    }
  }
  */
  // bin
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree,
      ObJsonInType::JSON_BIN, j_bin));
  hit.reset();
  ASSERT_EQ(OB_SUCCESS, j_bin->seek(j_path5, j_path5.path_node_cnt(), true, false, hit));
  ASSERT_EQ(hit.size(), 1);
  std::cout << "5. seek recursively success"<< std::endl;

/*
6. filter_node + path_nodes
    89. $?(1930 != @[3].resolution.x.number())[1]
    90. $?(@[3].resolution.x.string() == \"1920\")[1]
    91. $?(\"1920\" == @[3].resolution.x.string())[1]
    92. $?(@[3].resolution.z.boolean() == \"true\")[1]
    93. $?($v1== @[3].resolution.z.boolean())[2].abs()
    94. $?(\"true\" == @[3].resolution.z.boolean())[3].resolution.x
    95. $?(\"false\" == @[3].resolution.z.boolean()) [3].resolution.x ——> JsonNull
    96. $?(exists(@[3].resolution.x?(@[0] < 1920))) [3].resolution.x ——> JsonNull
    97. $?(exists(@[3].resolution.x?(@[0].string() == \"1920\"))) [3].resolution.x.number()
    98. $?(exists(@[3].resolution.y?(@[0].date() == \"2015-4-15\")))[3].resolution.x.number()
    99. $?(exists(@[3].resolution.y?(@[0].date() == \"2015-8-15\"))) [3].resolution.x.number()——> JsonNull
    100. $[3].resolution?(!exists(@.*?(@[0].timestamp() == \"2015-4-15 11:12:00.123456\")))[2].number()——> JsonNull
    101. $[3].resolution?(!exists(@.*?(@[0].timestamp() == \"2015-8-15 11:12:00.123456\"))) [2].number() ——空
    102. $[3].resolution?(!exists(@.*?(@[0].timestamp() == \"2015-8-15 11:12:00.123456\"))).x.number()
*/
  common::ObString path_text6("$[3].resolution?(exists(@.*?(@[0].timestamp() == \"2015-4-15 11:12:00.123456\"))).x.number()");
  ObJsonPath j_path6(path_text6, &allocator);
  j_path6.is_mysql_ = false;
  ASSERT_EQ(OB_SUCCESS, j_path6.parse_path());
  // tree
  hit.reset();
  ASSERT_EQ(OB_SUCCESS, j_tree->seek(j_path6, j_path6.path_node_cnt(), true, false, hit));
  ASSERT_EQ(hit.size(), 1);
  ASSERT_EQ(1920, hit[0]->get_uint());
  // bin
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree,
      ObJsonInType::JSON_BIN, j_bin));
  hit.reset();
  ASSERT_EQ(OB_SUCCESS, j_bin->seek(j_path6, j_path6.path_node_cnt(), true, false, hit));
  ASSERT_EQ(hit.size(), 1);
  ASSERT_EQ(1920, hit[0]->get_uint());
  std::cout << "6. filter_node + path_nodes success"<< std::endl;

/*
7. cond
    103. $?(2 == @[1] && @[3].resolution.x >  \"1930\") ——> false
    104. $?(2 == @[1] && @[3].resolution.x <  \"1930\")
    105. $?(2 == @[1] || @[3].resolution.x >  \"1930\")
    106. $?(2 != @[1] || @[3].resolution.x >  \"1930\") ——> false
    107. $?(!(2 == @[1])) ——> false
    108. $?(!(2 != @[1]))
    109. $?(2 == @[1] && !(@[3].resolution.x >  \"1930\"))
    110. $?(!(2 == @[1]) && !(@[3].resolution.x >  \"1930\")) ——> false
    111. $?(!(2 == @[1] ) ||  @[3].resolution.x <  \"1930\")
    112. $?(!(2 == @[1] ) || !( @[3].resolution.x <  \"1930\")) ——> false
    113. $?(!(2 == @[1] ) || ( @[3].resolution.x >  \"1930\"))  ——> false
    114. $?(!(2 == @[1] ) || 2 == @[1] && @[3].resolution.x  <  \"1930\")
    115. $?((!(2 != @[1] ) || 2 != @[1] ) && @[3].resolution.x  <  \"1930\")
*/
  common::ObString path_text7("$?((!(2 != @[1] ) || 2 != @[1] ) && @[3].resolution.x  <  \"1930\") ");
  ObJsonPath j_path7(path_text7, &allocator);
  j_path7.is_mysql_ = false;
  ASSERT_EQ(OB_SUCCESS, j_path7.parse_path());
  // tree
  hit.reset();
  ASSERT_EQ(OB_SUCCESS, j_tree->seek(j_path7, j_path7.path_node_cnt(), true, false, hit));
  ASSERT_EQ(hit.size(), 1);
  // bin
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree,
      ObJsonInType::JSON_BIN, j_bin));
  hit.reset();
  ASSERT_EQ(OB_SUCCESS, j_bin->seek(j_path7, j_path7.path_node_cnt(), true, false, hit));
  ASSERT_EQ(hit.size(), 1);
  std::cout << "cond success"<< std::endl;
}

TEST_F(TestJsonBase, test_print)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonBuffer j_buf(&allocator);
  ObIJsonBase *j_tree = NULL;
  ObIJsonBase *j_bin = NULL;

  // 1. date
  // json tree
  ObTime t_date;
  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::date_to_ob_time(16540, t_date));
  ObJsonDatetime j_date(t_date, ObDateType);
  j_tree = &j_date;
  ASSERT_EQ(OB_SUCCESS, j_tree->print(j_buf, false));
  ASSERT_EQ(0, strncmp("2015-04-15", j_buf.ptr(), j_buf.length()));
  j_buf.reset();
  ASSERT_EQ(OB_SUCCESS, j_tree->print(j_buf, true));
  ASSERT_EQ(0, strncmp("\"2015-04-15\"", j_buf.ptr(), j_buf.length()));
  // json bin
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_date,
      ObJsonInType::JSON_BIN, j_bin));
  j_buf.reset();
  ASSERT_EQ(OB_SUCCESS, j_bin->print(j_buf, false));
  ASSERT_EQ(0, strncmp("2015-04-15", j_buf.ptr(), j_buf.length()));
  j_buf.reset();
  ASSERT_EQ(OB_SUCCESS, j_bin->print(j_buf, true));
  ASSERT_EQ(0, strncmp("\"2015-04-15\"", j_buf.ptr(), j_buf.length()));

  // 2. time
  // json tree
  ObTime t_time;
  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::time_to_ob_time(
      43509 * static_cast<int64_t>(USECS_PER_SEC) + 1234, t_time));
  ObJsonDatetime j_time(t_time, ObTimeType);
  j_tree = &j_time;
  j_buf.reset();
  ASSERT_EQ(OB_SUCCESS, j_tree->print(j_buf, false));
  ASSERT_EQ(0, strncmp("12:05:09.001234", j_buf.ptr(), j_buf.length()));
  j_buf.reset();
  ASSERT_EQ(OB_SUCCESS, j_tree->print(j_buf, true));
  ASSERT_EQ(0, strncmp("\"12:05:09.001234\"", j_buf.ptr(), j_buf.length()));
  // json bin
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_time,
      ObJsonInType::JSON_BIN, j_bin));
  j_buf.reset();
  ASSERT_EQ(OB_SUCCESS, j_bin->print(j_buf, false));
  ASSERT_EQ(0, strncmp("12:05:09.001234", j_buf.ptr(), j_buf.length()));
  j_buf.reset();
  ASSERT_EQ(OB_SUCCESS, j_bin->print(j_buf, true));
  ASSERT_EQ(0, strncmp("\"12:05:09.001234\"", j_buf.ptr(), j_buf.length()));

  // 3. datetime
  // json tree
  ObTime t_datetime;
  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::datetime_to_ob_time(
      1429089727 * USECS_PER_SEC, NULL, t_datetime));
  ObJsonDatetime j_datetime(t_datetime, ObDateTimeType);
  j_tree = &j_datetime;
  j_buf.reset();
  ASSERT_EQ(OB_SUCCESS, j_tree->print(j_buf, false));
  ASSERT_EQ(0, strncmp("2015-04-15 09:22:07.000000", j_buf.ptr(), j_buf.length()));
  j_buf.reset();
  ASSERT_EQ(OB_SUCCESS, j_tree->print(j_buf, true));
  ASSERT_EQ(0, strncmp("\"2015-04-15 09:22:07.000000\"", j_buf.ptr(), j_buf.length()));
  // json bin
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_datetime,
      ObJsonInType::JSON_BIN, j_bin));
  j_buf.reset();
  ASSERT_EQ(OB_SUCCESS, j_bin->print(j_buf, false));
  ASSERT_EQ(0, strncmp("2015-04-15 09:22:07.000000", j_buf.ptr(), j_buf.length()));
  j_buf.reset();
  ASSERT_EQ(OB_SUCCESS, j_bin->print(j_buf, true));
  ASSERT_EQ(0, strncmp("\"2015-04-15 09:22:07.000000\"", j_buf.ptr(), j_buf.length()));

  // 4. timestamp
  // json tree
  ObTime t_timestamp;
  const int64_t MAX_BUF_SIZE = 256;
  char buf[MAX_BUF_SIZE] = {0};
  strcpy(buf, "2015-4-15 11:12:00.1234567 -07:00");
  ObString str;
  str.assign(buf, static_cast<int32_t>(strlen(buf)));
  ObTimeZoneInfo tz_info;
  ObTimeConvertCtx cvrt_ctx(&tz_info, false);
  cvrt_ctx.oracle_nls_format_ = ObTimeConverter::COMPAT_OLD_NLS_TIMESTAMP_TZ_FORMAT;
  ObOTimestampData ot_data;
  int16_t scale = 0;
  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::str_to_otimestamp(str, cvrt_ctx,
      ObTimestampTZType, ot_data, scale));
  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::otimestamp_to_ob_time(ObTimestampNanoType,
      ot_data, NULL, t_timestamp));
  ObJsonDatetime j_timestamp(t_timestamp, ObTimestampType);
  j_tree = &j_timestamp;
  j_buf.reset();
  ASSERT_EQ(OB_SUCCESS, j_tree->print(j_buf, false));
  ASSERT_EQ(0, strncmp("2015-04-15 18:12:00.123456", j_buf.ptr(), j_buf.length()));
  j_buf.reset();
  ASSERT_EQ(OB_SUCCESS, j_tree->print(j_buf, true));
  ASSERT_EQ(0, strncmp("\"2015-04-15 18:12:00.123456\"", j_buf.ptr(), j_buf.length()));
  // json bin
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_timestamp,
      ObJsonInType::JSON_BIN, j_bin));
  j_buf.reset();
  ASSERT_EQ(OB_SUCCESS, j_bin->print(j_buf, false));
  ASSERT_EQ(0, strncmp("2015-04-15 18:12:00.123456", j_buf.ptr(), j_buf.length()));
  j_buf.reset();
  ASSERT_EQ(OB_SUCCESS, j_bin->print(j_buf, true));
  ASSERT_EQ(0, strncmp("\"2015-04-15 18:12:00.123456\"", j_buf.ptr(), j_buf.length()));

  // 5. array
  // json tree
  void *buf_ptr = NULL;
  ObJsonArray j_array(&allocator);
  ObJsonArray *j_array_head = &j_array;
  ObJsonArray *j_arr_last = j_array_head;
  for (uint32_t i = 0; i < 99; i++) { // 99个数组嵌套
    buf_ptr = allocator.alloc(sizeof(ObJsonArray));
    ASSERT_TRUE(buf_ptr != NULL);
    ObJsonArray *j_new_arr = new (buf_ptr) ObJsonArray(&allocator);
    buf_ptr = allocator.alloc(sizeof(ObJsonInt));
    ASSERT_TRUE(buf_ptr != NULL);
    ObJsonInt *j_int_val = new (buf_ptr) ObJsonInt(1);
    ASSERT_EQ(OB_SUCCESS, j_new_arr->append(j_int_val));
    ASSERT_EQ(OB_SUCCESS, j_arr_last->append(j_new_arr));
    j_arr_last = j_new_arr;
  }
  j_tree = j_array_head;
  j_buf.reset();
  ASSERT_EQ(OB_SUCCESS, j_tree->print(j_buf, false));
  // 第100个,预期报错OB_ERR_JSON_OUT_OF_DEPTH
  buf_ptr = allocator.alloc(sizeof(ObJsonArray));
  ASSERT_TRUE(buf_ptr != NULL);
  ObJsonArray *j_new_arr = new (buf_ptr) ObJsonArray(&allocator);
  buf_ptr = allocator.alloc(sizeof(ObJsonInt));
  ASSERT_TRUE(buf_ptr != NULL);
  ObJsonInt *j_int_val = new (buf_ptr) ObJsonInt(1);
  ASSERT_EQ(OB_SUCCESS, j_new_arr->append(j_int_val));
  ASSERT_EQ(OB_SUCCESS, j_arr_last->append(j_new_arr));
  j_buf.reset();
  ASSERT_EQ(OB_ERR_JSON_OUT_OF_DEPTH, j_tree->print(j_buf, false));
  // json bin
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree,
      ObJsonInType::JSON_BIN, j_bin));
  j_buf.reset();
  ASSERT_EQ(OB_ERR_JSON_OUT_OF_DEPTH, j_bin->print(j_buf, false));

  // object
  // json tree
  ObJsonObject *last_obj = NULL;
  ObJsonObject *new_obj = NULL;
  char key_buf[10] = {0};
  for (uint32_t i = 0; i < 100; i++) { // 99个对象嵌套
    sprintf(key_buf, "key%d", i);
    if (i == 0) {
      ASSERT_EQ(OB_SUCCESS, create_object(&allocator, key_buf, strlen(key_buf), NULL, new_obj));
    } else {
      ASSERT_EQ(OB_SUCCESS, create_object(&allocator, key_buf, strlen(key_buf), last_obj, new_obj));
    }
    last_obj = new_obj;
  }
  j_tree = new_obj;
  j_buf.reset();
  ASSERT_EQ(OB_SUCCESS, j_tree->print(j_buf, false));
  // 第100个,预期报错OB_ERR_JSON_OUT_OF_DEPTH
  ASSERT_EQ(OB_SUCCESS, create_object(&allocator, key_buf, strlen(key_buf), last_obj, new_obj));
  j_tree = new_obj;
  ASSERT_EQ(OB_ERR_JSON_OUT_OF_DEPTH, j_tree->print(j_buf, false));
  // json bin
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree,
      ObJsonInType::JSON_BIN, j_bin));
  j_buf.reset();
  ASSERT_EQ(OB_ERR_JSON_OUT_OF_DEPTH, j_bin->print(j_buf, false));

  // boolean
  // json tree
  ObJsonBoolean j_bool(true);
  j_tree = &j_bool;
  j_buf.reset();
  ASSERT_EQ(OB_SUCCESS, j_tree->print(j_buf, false));
  ASSERT_EQ(0, strncmp("true", j_buf.ptr(), j_buf.length()));
  j_buf.reset();
  ASSERT_EQ(OB_SUCCESS, j_tree->print(j_buf, true));
  ASSERT_EQ(0, strncmp("true", j_buf.ptr(), j_buf.length()));
  // json bin
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_bool,
      ObJsonInType::JSON_BIN, j_bin));
  j_buf.reset();
  ASSERT_EQ(OB_SUCCESS, j_bin->print(j_buf, false));
  ASSERT_EQ(0, strncmp("true", j_buf.ptr(), j_buf.length()));
  j_buf.reset();
  ASSERT_EQ(OB_SUCCESS, j_bin->print(j_buf, true));
  ASSERT_EQ(0, strncmp("true", j_buf.ptr(), j_buf.length()));

  // decimal
  // json tree
  number::ObNumber num;
  int length = sprintf(buf, "%f", 100.0002);
  ASSERT_EQ(OB_SUCCESS, num.from(buf, allocator));
  ObJsonDecimal j_dec(num, 3, 4);
  j_tree = &j_dec;
  j_buf.reset();
  ASSERT_EQ(OB_SUCCESS, j_tree->print(j_buf, false));
  ASSERT_EQ(0, strncmp("100.0002", j_buf.ptr(), j_buf.length()));
  j_buf.reset();
  ASSERT_EQ(OB_SUCCESS, j_tree->print(j_buf, true));
  ASSERT_EQ(0, strncmp("100.0002", j_buf.ptr(), j_buf.length()));
  // json bin
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_dec,
      ObJsonInType::JSON_BIN, j_bin));
  j_buf.reset();
  ASSERT_EQ(OB_SUCCESS, j_bin->print(j_buf, false));
  ASSERT_EQ(0, strncmp("100.0002", j_buf.ptr(), j_buf.length()));
  j_buf.reset();
  ASSERT_EQ(OB_SUCCESS, j_bin->print(j_buf, true));
  ASSERT_EQ(0, strncmp("100.0002", j_buf.ptr(), j_buf.length()));

  // double
  // json tree
  ObJsonDouble j_double(DBL_MAX);
  j_tree = &j_double;
  j_buf.reset();
  ASSERT_EQ(OB_SUCCESS, j_tree->print(j_buf, false));
  ASSERT_EQ(0, strncmp("1.7976931348623157e308", j_buf.ptr(), j_buf.length()));
  j_buf.reset();
  ASSERT_EQ(OB_SUCCESS, j_tree->print(j_buf, true));
  ASSERT_EQ(0, strncmp("1.7976931348623157e308", j_buf.ptr(), j_buf.length()));
  // json bin
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_double,
      ObJsonInType::JSON_BIN, j_bin));
  j_buf.reset();
  ASSERT_EQ(OB_SUCCESS, j_bin->print(j_buf, false));
  ASSERT_EQ(0, strncmp("1.7976931348623157e308", j_buf.ptr(), j_buf.length()));
  j_buf.reset();
  ASSERT_EQ(OB_SUCCESS, j_bin->print(j_buf, true));
  ASSERT_EQ(0, strncmp("1.7976931348623157e308", j_buf.ptr(), j_buf.length()));

  // null
  // json tree
  ObJsonNull j_null;
  j_tree = &j_null;
  j_buf.reset();
  ASSERT_EQ(OB_SUCCESS, j_tree->print(j_buf, false));
  ASSERT_EQ(0, strncmp("null", j_buf.ptr(), j_buf.length()));
  j_buf.reset();
  ASSERT_EQ(OB_SUCCESS, j_tree->print(j_buf, true));
  ASSERT_EQ(0, strncmp("null", j_buf.ptr(), j_buf.length()));
  // json bin
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_null,
      ObJsonInType::JSON_BIN, j_bin));
  j_buf.reset();
  ASSERT_EQ(OB_SUCCESS, j_bin->print(j_buf, false));
  ASSERT_EQ(0, strncmp("null", j_buf.ptr(), j_buf.length()));
  j_buf.reset();
  ASSERT_EQ(OB_SUCCESS, j_bin->print(j_buf, true));
  ASSERT_EQ(0, strncmp("null", j_buf.ptr(), j_buf.length()));

  // opaque
  // json tree
  ObString str_opaque("opaque");
  ObJsonOpaque j_opaque(str_opaque, ObBitType);
  j_tree = &j_opaque;
  j_buf.reset();
  ASSERT_EQ(OB_SUCCESS, j_tree->print(j_buf, false));
  ASSERT_EQ(0, strncmp("base64:type16:b3BhcXVl", j_buf.ptr(), j_buf.length()));
  j_buf.reset();
  ASSERT_EQ(OB_SUCCESS, j_tree->print(j_buf, true));
  ASSERT_EQ(0, strncmp("\"base64:type16:b3BhcXVl\"", j_buf.ptr(), j_buf.length()));
  // json bin
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_opaque,
      ObJsonInType::JSON_BIN, j_bin));
  j_buf.reset();
  ASSERT_EQ(OB_SUCCESS, j_bin->print(j_buf, false));
  ASSERT_EQ(0, strncmp("base64:type16:b3BhcXVl", j_buf.ptr(), j_buf.length()));
  j_buf.reset();
  ASSERT_EQ(OB_SUCCESS, j_bin->print(j_buf, true));
  ASSERT_EQ(0, strncmp("\"base64:type16:b3BhcXVl\"", j_buf.ptr(), j_buf.length()));
  
  // string
  // json tree
  ObString str_string("string");
  ObJsonString j_string(str_string.ptr(), str_string.length());
  j_tree = &j_string;
  j_buf.reset();
  ASSERT_EQ(OB_SUCCESS, j_tree->print(j_buf, false));
  ASSERT_EQ(0, strncmp("string", j_buf.ptr(), j_buf.length()));
  j_buf.reset();
  ASSERT_EQ(OB_SUCCESS, j_tree->print(j_buf, true));
  ASSERT_EQ(0, strncmp("\"string\"", j_buf.ptr(), j_buf.length()));
  // json bin
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_string,
      ObJsonInType::JSON_BIN, j_bin));
  j_buf.reset();
  ASSERT_EQ(OB_SUCCESS, j_bin->print(j_buf, false));
  ASSERT_EQ(0, strncmp("string", j_buf.ptr(), j_buf.length()));
  j_buf.reset();
  ASSERT_EQ(OB_SUCCESS, j_bin->print(j_buf, true));
  ASSERT_EQ(0, strncmp("\"string\"", j_buf.ptr(), j_buf.length()));

  // int
  // json tree
  ObJsonInt j_int(123);
  j_tree = &j_int;
  j_buf.reset();
  ASSERT_EQ(OB_SUCCESS, j_tree->print(j_buf, false));
  ASSERT_EQ(0, strncmp("123", j_buf.ptr(), j_buf.length()));
  j_buf.reset();
  ASSERT_EQ(OB_SUCCESS, j_tree->print(j_buf, true));
  ASSERT_EQ(0, strncmp("123", j_buf.ptr(), j_buf.length()));
  // json bin
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_int,
      ObJsonInType::JSON_BIN, j_bin));
  j_buf.reset();
  ASSERT_EQ(OB_SUCCESS, j_bin->print(j_buf, false));
  ASSERT_EQ(0, strncmp("123", j_buf.ptr(), j_buf.length()));
  j_buf.reset();
  ASSERT_EQ(OB_SUCCESS, j_bin->print(j_buf, true));
  ASSERT_EQ(0, strncmp("123", j_buf.ptr(), j_buf.length()));

  // uint
  // json tree
  ObJsonUint j_uint(123);
  j_tree = &j_uint;
  j_buf.reset();
  ASSERT_EQ(OB_SUCCESS, j_tree->print(j_buf, false));
  ASSERT_EQ(0, strncmp("123", j_buf.ptr(), j_buf.length()));
  j_buf.reset();
  ASSERT_EQ(OB_SUCCESS, j_tree->print(j_buf, true));
  ASSERT_EQ(0, strncmp("123", j_buf.ptr(), j_buf.length()));
  // json bin
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_uint,
      ObJsonInType::JSON_BIN, j_bin));
  j_buf.reset();
  ASSERT_EQ(OB_SUCCESS, j_bin->print(j_buf, false));
  ASSERT_EQ(0, strncmp("123", j_buf.ptr(), j_buf.length()));
  j_buf.reset();
  ASSERT_EQ(OB_SUCCESS, j_bin->print(j_buf, true));
  ASSERT_EQ(0, strncmp("123", j_buf.ptr(), j_buf.length()));
}

TEST_F(TestJsonBase, test_calc_json_hash_value)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObJsonBuffer j_buf(&allocator);
  ObIJsonBase *j_tree = NULL;
  uint64_t hash_value = 0;
  common::ObString j_text("{\"name\": \"tom\", \"is_male\": true, \"age\" : 25, \"friends\": \
      [{\"name\":\"jim\", \"is_male\": true, \"age\":21}, \
      {\"name\":\"jerry\", \"is_male\": true, \"age\":28}], \"company\": null, \
      \"addr\": {\"country\" : \"china\", \"province\": \"guangdong\", \"detail\" : \
      {\"district\": \"nanshan\", \"street\":\"dengliang\"}}}");
  
  // test json tree
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree));
  ASSERT_EQ(OB_SUCCESS, j_tree->calc_json_hash_value(1243323423, NULL, hash_value));
  ASSERT_EQ(12421634862223985558ULL, hash_value);

  // test json bin
  ObIJsonBase *j_bin = NULL;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree,
      ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(OB_SUCCESS, j_bin->calc_json_hash_value(1243323423, NULL, hash_value));
  ASSERT_EQ(12421634862223985558ULL, hash_value);
}

TEST_F(TestJsonBase, test_compare)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  
  // J_ARRAY vs J_ARRAY
  common::ObString json_text_a("[0, 1, 2, 3]");
  common::ObString json_text_b("[0, 1, 2, 3]");
  ASSERT_EQ(0, json_compare_json_text(json_text_a, json_text_b));

  json_text_a.reset();
  json_text_b.reset();
  json_text_a.assign_ptr("[0, 1, 2, 3]", strlen("[0, 1, 2, 3]"));
  json_text_b.assign_ptr("[0, 1, 3, 1, 1, 1, 1]", strlen("[0, 1, 3, 1, 1, 1, 1]"));
  ASSERT_EQ(-1, json_compare_json_text(json_text_a, json_text_b));

  json_text_a.reset();
  json_text_b.reset();
  json_text_a.assign_ptr("[0, 1, 2]", strlen("[0, 1, 2]"));
  json_text_b.assign_ptr("[0, 1, 2, 1, 1, 1, 1]", strlen("[0, 1, 2, 1, 1, 1, 1]"));
  ASSERT_EQ(-1, json_compare_json_text(json_text_a, json_text_b));

  // J_OBJECT vs J_OBJECT
  json_text_a.reset();
  json_text_b.reset();
  json_text_a.assign_ptr("{ \"a\" : \"1\", \"b\" : \"2\", \"c\" : \"3\" }",
                         strlen("{ \"a\" : \"1\", \"b\" : \"2\", \"c\" : \"3\" }"));
  json_text_b.assign_ptr("{ \"a\" : \"1\", \"b\" : \"2\", \"c\" : \"3\" }",
                         strlen("{ \"a\" : \"1\", \"b\" : \"2\", \"c\" : \"3\" }"));
  ASSERT_EQ(0, json_compare_json_text(json_text_a, json_text_b));

  json_text_a.reset();
  json_text_b.reset();
  json_text_a.assign_ptr("{ \"a\" : \"1\", \"b\" : \"2\" }",
                         strlen("{ \"a\" : \"1\", \"b\" : \"2\" }"));
  json_text_b.assign_ptr("{ \"a\" : \"1\", \"b\" : \"2\", \"c\" : \"3\" }",
                         strlen("{ \"a\" : \"1\", \"b\" : \"2\", \"c\" : \"3\" }"));
  ASSERT_EQ(-1, json_compare_json_text(json_text_a, json_text_b));

  json_text_a.reset();
  json_text_b.reset();
  json_text_a.assign_ptr("{ \"a\" : \"1\", \"b\" : \"2\", \"c\" : \"3\" }",
                         strlen("{ \"a\" : \"1\", \"b\" : \"2\", \"c\" : \"3\" }"));
  json_text_b.assign_ptr("{ \"a\" : \"1\", \"d\" : \"2\", \"c\" : \"3\" }",
                         strlen("{ \"a\" : \"1\", \"d\" : \"2\", \"c\" : \"3\" }"));
  ASSERT_EQ(-1, json_compare_json_text(json_text_a, json_text_b));

  json_text_a.reset();
  json_text_b.reset();
  json_text_a.assign_ptr("{ \"a\" : \"1\", \"b\" : \"2\", \"c\" : \"3\" }",
                         strlen("{ \"a\" : \"1\", \"b\" : \"2\", \"c\" : \"3\" }"));
  json_text_b.assign_ptr("{ \"a\" : \"1\", \"b\" : \"20\", \"c\" : \"3\" }",
                         strlen("{ \"a\" : \"1\", \"b\" : \"20\", \"c\" : \"3\" }"));
  ASSERT_EQ(-1, json_compare_json_text(json_text_a, json_text_b));

  // J_STRING vs J_STRING
  ASSERT_EQ(0, json_compare_string("abc", strlen("abc"), "abc", strlen("abc")));
  ASSERT_EQ(1, json_compare_string("abc", strlen("abc"), "ab", strlen("ab")));
  ASSERT_EQ(-1, json_compare_string("abc", strlen("abc"), "abcd", strlen("abcd")));

  // J_INT vs (J_INT)
  ASSERT_EQ(-1, json_compare_int_int(LLONG_MIN, LLONG_MAX));
  ASSERT_EQ(0, json_compare_int_int(LLONG_MIN, LLONG_MIN));
  ASSERT_EQ(0, json_compare_int_int(LLONG_MAX, LLONG_MAX));
  ASSERT_EQ(1, json_compare_int_int(LLONG_MAX, LLONG_MIN));

  // J_INT vs J_UINT
  ASSERT_EQ(-1, json_compare_int_uint(LLONG_MIN, ULLONG_MAX));
  ASSERT_EQ(0, json_compare_int_uint(LLONG_MAX, LLONG_MAX));
  ASSERT_EQ(1, json_compare_int_uint(LLONG_MAX, 0));

  // J_INT vs J_DOUBLE
  ASSERT_EQ(-1, json_compare_int_double(LLONG_MIN, DBL_MAX));
  ASSERT_EQ(0, json_compare_int_double(0, 0.0));
  ASSERT_EQ(1, json_compare_int_double(LLONG_MAX, DBL_MIN));

  // J_INT vs J_DECIMAL
  const int MAX_BUF_SIZE = 256;
  char buf_alloc[MAX_BUF_SIZE];
  ObDataBuffer alloc(buf_alloc, MAX_BUF_SIZE);
  number::ObNumber num;
  char buf[MAX_BUF_SIZE] = {0};
  int length = sprintf(buf, "%llu", ULLONG_MAX);
  ASSERT_EQ(OB_SUCCESS, num.from(buf, alloc));
  ASSERT_EQ(-1, json_compare_int_decimal(LLONG_MIN, num));

  alloc.free();
  MEMSET(buf, 0, sizeof(buf));
  length = sprintf(buf, "%lld", LLONG_MIN);
  ASSERT_EQ(OB_SUCCESS, num.from(buf, alloc));
  ASSERT_EQ(0, json_compare_int_decimal(LLONG_MIN, num));

  alloc.free();
  MEMSET(buf, 0, sizeof(buf));
  length = sprintf(buf, "%lld", LLONG_MIN);
  ASSERT_EQ(OB_SUCCESS, num.from(buf, alloc));
  ASSERT_EQ(1, json_compare_int_decimal(LLONG_MAX, num));

  // J_UINT vs J_UINT
  ASSERT_EQ(-1, json_compare_uint_uint(0, ULLONG_MAX));
  ASSERT_EQ(0, json_compare_uint_uint(0, 0));
  ASSERT_EQ(0, json_compare_uint_uint(ULLONG_MAX, ULLONG_MAX));
  ASSERT_EQ(1, json_compare_uint_uint(ULLONG_MAX, 0));

  // J_UINT vs J_INT
  ASSERT_EQ(-1, json_compare_uint_int(0, LLONG_MAX));
  ASSERT_EQ(0, json_compare_uint_int(0, 0));
  ASSERT_EQ(1, json_compare_uint_int(ULLONG_MAX, LLONG_MAX));

  // J_UINT vs J_DOUBLE
  ASSERT_EQ(-1, json_compare_uint_double(0, DBL_MAX));
  ASSERT_EQ(0, json_compare_uint_double(0, 0.0));
  ASSERT_EQ(1, json_compare_uint_double(ULLONG_MAX, DBL_MIN));

  // J_UINT vs J_DECIMAL
  alloc.free();
  MEMSET(buf, 0, sizeof(buf));
  length = sprintf(buf, "%llu", ULLONG_MAX);
  ASSERT_EQ(OB_SUCCESS, num.from(buf, alloc));
  ASSERT_EQ(-1, json_compare_uint_decimal(0, num));

  alloc.free();
  MEMSET(buf, 0, sizeof(buf));
  length = sprintf(buf, "%llu", ULLONG_MAX);
  ASSERT_EQ(OB_SUCCESS, num.from(buf, alloc));
  ASSERT_EQ(0, json_compare_uint_decimal(ULLONG_MAX, num));

  alloc.free();
  MEMSET(buf, 0, sizeof(buf));
  length = sprintf(buf, "%lld", LLONG_MIN);
  ASSERT_EQ(OB_SUCCESS, num.from(buf, alloc));
  ASSERT_EQ(1, json_compare_uint_decimal(LLONG_MAX, num));

  // J_DOUBLE vs J_DOUBLE
  ASSERT_EQ(-1, json_compare_double_double(DBL_MIN, DBL_MAX));
  ASSERT_EQ(0, json_compare_double_double(DBL_MIN, DBL_MIN));
  ASSERT_EQ(0, json_compare_double_double(DBL_MAX, DBL_MAX));
  ASSERT_EQ(1, json_compare_double_double(DBL_MAX, DBL_MIN));

  // J_DOUBLE vs J_INT
  ASSERT_EQ(-1, json_compare_doule_int(DBL_MIN, LLONG_MAX));
  ASSERT_EQ(0, json_compare_doule_int(0.0, 0));
  ASSERT_EQ(1, json_compare_doule_int(DBL_MAX, LLONG_MIN));

  // J_DOUBLE vs J_UINT
  ASSERT_EQ(-1, json_compare_doule_uint(DBL_MIN, ULLONG_MAX));
  ASSERT_EQ(0, json_compare_doule_uint(0, 0.0));
  ASSERT_EQ(1, json_compare_doule_uint(ULLONG_MAX, 0));

  // J_DOUBLE vs J_DECIMAL
  alloc.free();
  MEMSET(buf, 0, sizeof(buf));
  length = sprintf(buf, "%llu", ULLONG_MAX);
  ASSERT_EQ(OB_SUCCESS, num.from(buf, alloc));
  ASSERT_EQ(-1, json_compare_double_decimal(DBL_MIN, num));

  alloc.free();
  MEMSET(buf, 0, sizeof(buf));
  length = sprintf(buf, "%d", 0);
  ASSERT_EQ(OB_SUCCESS, num.from(buf, alloc));
  ASSERT_EQ(0, json_compare_double_decimal(0, num));

  alloc.free();
  MEMSET(buf, 0, sizeof(buf));
  length = sprintf(buf, "%lld", LLONG_MIN);
  ASSERT_EQ(OB_SUCCESS, num.from(buf, alloc));
  ASSERT_EQ(1, json_compare_double_decimal(DBL_MAX, num));

  // J_DECIMAL vs J_DECIMAL
  alloc.free();
  MEMSET(buf, 0, sizeof(buf));
  number::ObNumber num_a;
  length = sprintf(buf, "%lld", LLONG_MIN);
  ASSERT_EQ(OB_SUCCESS, num_a.from(buf, alloc));
  alloc.free();
  MEMSET(buf, 0, sizeof(buf));
  length = sprintf(buf, "%llu", ULLONG_MAX);
  ASSERT_EQ(OB_SUCCESS, num.from(buf, alloc));
  ASSERT_EQ(-1, json_compare_decimal_decimal(num_a, num));

  alloc.free();
  MEMSET(buf, 0, sizeof(buf));
  length = sprintf(buf, "%llu", ULLONG_MAX);
  ASSERT_EQ(OB_SUCCESS, num_a.from(buf, alloc));
  alloc.free();
  MEMSET(buf, 0, sizeof(buf));
  length = sprintf(buf, "%llu", ULLONG_MAX);
  ASSERT_EQ(OB_SUCCESS, num.from(buf, alloc));
  ASSERT_EQ(0, json_compare_decimal_decimal(num_a, num));

  alloc.free();
  MEMSET(buf, 0, sizeof(buf));
  length = sprintf(buf, "%llu", ULLONG_MAX);
  ASSERT_EQ(OB_SUCCESS, num_a.from(buf, alloc));
  alloc.free();
  MEMSET(buf, 0, sizeof(buf));
  length = sprintf(buf, "%lld", LLONG_MIN);
  ASSERT_EQ(OB_SUCCESS, num.from(buf, alloc));
  ASSERT_EQ(1, json_compare_decimal_decimal(num_a, num));

  // J_DECIMAL vs J_INT
  alloc.free();
  MEMSET(buf, 0, sizeof(buf));
  length = sprintf(buf, "%d", 0);
  ASSERT_EQ(OB_SUCCESS, num_a.from(buf, alloc));
  ASSERT_EQ(-1, json_compare_decimal_int(num_a, LLONG_MAX));

  alloc.free();
  MEMSET(buf, 0, sizeof(buf));
  length = sprintf(buf, "%lld", LLONG_MIN);
  ASSERT_EQ(OB_SUCCESS, num_a.from(buf, alloc));
  ASSERT_EQ(0, json_compare_decimal_int(num_a, LLONG_MIN));

  alloc.free();
  MEMSET(buf, 0, sizeof(buf));
  length = sprintf(buf, "%llu", ULLONG_MAX);
  ASSERT_EQ(OB_SUCCESS, num_a.from(buf, alloc));
  ASSERT_EQ(1, json_compare_decimal_int(num_a, LLONG_MIN));

  // J_DECIMAL vs J_UINT
  alloc.free();
  MEMSET(buf, 0, sizeof(buf));
  length = sprintf(buf, "%d", 0);
  ASSERT_EQ(OB_SUCCESS, num_a.from(buf, alloc));
  ASSERT_EQ(-1, json_compare_decimal_uint(num_a, ULLONG_MAX));

  alloc.free();
  MEMSET(buf, 0, sizeof(buf));
  length = sprintf(buf, "%d", 0);
  ASSERT_EQ(OB_SUCCESS, num_a.from(buf, alloc));
  ASSERT_EQ(0, json_compare_decimal_uint(num_a, 0));

  alloc.free();
  MEMSET(buf, 0, sizeof(buf));
  length = sprintf(buf, "%llu", ULLONG_MAX);
  ASSERT_EQ(OB_SUCCESS, num_a.from(buf, alloc));
  ASSERT_EQ(1, json_compare_decimal_uint(num_a, 0));

  // J_DECIMAL vs J_DOUBLE
  alloc.free();
  MEMSET(buf, 0, sizeof(buf));
  length = sprintf(buf, "%d", 0);
  ASSERT_EQ(OB_SUCCESS, num_a.from(buf, alloc));
  ASSERT_EQ(-1, json_compare_decimal_double(num_a, DBL_MAX));

  alloc.free();
  MEMSET(buf, 0, sizeof(buf));
  length = sprintf(buf, "%d", 0);
  ASSERT_EQ(OB_SUCCESS, num_a.from(buf, alloc));
  ASSERT_EQ(0, json_compare_decimal_double(num_a, 0));

  alloc.free();
  MEMSET(buf, 0, sizeof(buf));
  length = sprintf(buf, "%llu", ULLONG_MAX);
  ASSERT_EQ(OB_SUCCESS, num_a.from(buf, alloc));
  ASSERT_EQ(1, json_compare_decimal_double(num_a, DBL_MIN));

  // J_BOOLEAN vs J_BOOLEAN
  ASSERT_EQ(-1, json_compare_boolean_boolean(false, true));
  ASSERT_EQ(0, json_compare_boolean_boolean(false, false));
  ASSERT_EQ(0, json_compare_boolean_boolean(true, true));
  ASSERT_EQ(1, json_compare_boolean_boolean(true, false));

  // J_DATETIME vs J_DATETIME
  ObTime ob_datetime_a;
  ObTime ob_datetime_b;
  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::datetime_to_ob_time(1429089720 * USECS_PER_SEC,
      NULL, ob_datetime_a));
  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::datetime_to_ob_time(1429089727 * USECS_PER_SEC,
      NULL, ob_datetime_b));
  ASSERT_EQ(-1, json_compare_datetime_datetime(ob_datetime_a,
                                               ObDateTimeType,
                                               ob_datetime_b,
                                               ObDateTimeType));

  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::datetime_to_ob_time(1429089727 * USECS_PER_SEC,
      NULL, ob_datetime_a));
  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::datetime_to_ob_time(1429089727 * USECS_PER_SEC,
      NULL, ob_datetime_b));
  ASSERT_EQ(0, json_compare_datetime_datetime(ob_datetime_a,
                                              ObDateTimeType,
                                              ob_datetime_b,
                                              ObDateTimeType));

  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::datetime_to_ob_time(1429089727 * USECS_PER_SEC,
      NULL, ob_datetime_a));
  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::datetime_to_ob_time(1429089720 * USECS_PER_SEC,
      NULL, ob_datetime_b));
  ASSERT_EQ(1, json_compare_datetime_datetime(ob_datetime_a,
                                              ObDateTimeType,
                                              ob_datetime_b,
                                              ObDateTimeType));

  // J_DATETIME vs J_TIMESTAMP
  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::datetime_to_ob_time(1429089727 * USECS_PER_SEC,
      NULL, ob_datetime_a));
  MEMSET(buf, 0, sizeof(buf));
  ObTime ob_timestamp_b;
  ObString str_timestamp_b;
  ObTimeZoneInfo tz_info;
  ObTimeConvertCtx cvrt_ctx(&tz_info, false);
  cvrt_ctx.oracle_nls_format_ = ObTimeConverter::COMPAT_OLD_NLS_TIMESTAMP_TZ_FORMAT;
  ObOTimestampData ot_data;
  int16_t scale = 0;
  MEMSET(buf, 0, sizeof(buf));
  strcpy(buf, "2015-4-15 09:22:07.1234567 -07:00");
  str_timestamp_b.assign(buf, static_cast<int32_t>(strlen(buf)));
  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::str_to_otimestamp(str_timestamp_b, cvrt_ctx,
      ObTimestampTZType, ot_data, scale));
  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::otimestamp_to_ob_time(ObTimestampNanoType, ot_data,
      NULL, ob_timestamp_b));
  ASSERT_EQ(1, json_compare_datetime_datetime(ob_datetime_a,
                                               ObDateTimeType,
                                               ob_datetime_b,
                                               ObTimestampType));

  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::datetime_to_ob_time(1429089727 * USECS_PER_SEC,
      NULL, ob_datetime_a));
  MEMSET(buf, 0, sizeof(buf));
  strcpy(buf, "2015-4-15 09:22:07");
  str_timestamp_b.assign(buf, static_cast<int32_t>(strlen(buf)));
  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::str_to_otimestamp(str_timestamp_b, cvrt_ctx,
      ObTimestampTZType, ot_data, scale));
  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::otimestamp_to_ob_time(ObTimestampNanoType,
      ot_data, NULL, ob_timestamp_b));
  ASSERT_EQ(1, json_compare_datetime_datetime(ob_datetime_a,
                                              ObDateTimeType,
                                              ob_datetime_b,
                                              ObTimestampType));

  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::datetime_to_ob_time(1429089727 * USECS_PER_SEC,
      NULL, ob_datetime_a));
  MEMSET(buf, 0, sizeof(buf));
  strcpy(buf, "2015-4-15 09:22:06 -07:00");
  str_timestamp_b.assign(buf, static_cast<int32_t>(strlen(buf)));
  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::str_to_otimestamp(str_timestamp_b, cvrt_ctx,
      ObTimestampTZType, ot_data, scale));
  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::otimestamp_to_ob_time(ObTimestampNanoType,
      ot_data, NULL, ob_timestamp_b));
  ASSERT_EQ(1, json_compare_datetime_datetime(ob_datetime_a,
                                              ObDateTimeType,
                                              ob_datetime_b,
                                              ObTimestampType));

  // J_TIME vs J_TIME
  ObTime ob_time_a;
  ObTime ob_time_b;
  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::time_to_ob_time(43508
      * static_cast<int64_t>(USECS_PER_SEC) + 1234, ob_time_a));
  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::time_to_ob_time(43509
      * static_cast<int64_t>(USECS_PER_SEC) + 1234, ob_time_b));
  ASSERT_EQ(-1, json_compare_datetime_datetime(ob_time_a,
                                              ObTimeType,
                                              ob_time_b,
                                              ObTimeType));

  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::time_to_ob_time(43509
      * static_cast<int64_t>(USECS_PER_SEC) + 1234, ob_time_a));
  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::time_to_ob_time(43509
      * static_cast<int64_t>(USECS_PER_SEC) + 1234, ob_time_b));
  ASSERT_EQ(0, json_compare_datetime_datetime(ob_time_a,
                                              ObTimeType,
                                              ob_time_b,
                                              ObTimeType));

  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::time_to_ob_time(43509
      * static_cast<int64_t>(USECS_PER_SEC) + 1234, ob_time_a));
  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::time_to_ob_time(43508
      * static_cast<int64_t>(USECS_PER_SEC) + 1234, ob_time_b));
  ASSERT_EQ(1, json_compare_datetime_datetime(ob_time_a,
                                              ObTimeType,
                                              ob_time_b,
                                              ObTimeType));

  // J_DATE vs J_DATE
  ObTime ob_time_date_a;
  ObTime ob_time_date_b;
  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::date_to_ob_time(16530, ob_time_date_a));
  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::date_to_ob_time(16540, ob_time_date_b));
  ASSERT_EQ(-1, json_compare_datetime_datetime(ob_time_date_a,
                                              ObDateType,
                                              ob_time_date_b,
                                              ObDateType));

  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::date_to_ob_time(16540, ob_time_date_a));
  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::date_to_ob_time(16540, ob_time_date_b));
  ASSERT_EQ(0, json_compare_datetime_datetime(ob_time_date_a,
                                              ObDateType,
                                              ob_time_date_b,
                                              ObDateType));

  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::date_to_ob_time(16540, ob_time_date_a));
  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::date_to_ob_time(16530, ob_time_date_b));
  ASSERT_EQ(1, json_compare_datetime_datetime(ob_time_date_a,
                                              ObDateType,
                                              ob_time_date_b,
                                              ObDateType));

  // J_OPAQUE vs J_OPAQUE
  ObString str_a("abc");
  ObString str_b("abc");
  ASSERT_EQ(0, json_compare_opaque_opaque(str_a, ObVarcharType,
                                          str_b, ObVarcharType));

  ASSERT_EQ(1, json_compare_opaque_opaque(str_a, ObLongTextType,
                                          str_b, ObVarcharType));
}

TEST_F(TestJsonBase, test_get_internal_type)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObIJsonBase *j_bin = NULL;
  common::ObString j_text("[1, 2, 3, 4, 5]");

  // test json tree
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree));
  ASSERT_EQ(ObJsonInType::JSON_TREE, j_tree->get_internal_type());

  // test json bin
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(ObJsonInType::JSON_BIN, j_bin->get_internal_type());
}

TEST_F(TestJsonBase, test_element_count)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObIJsonBase *j_bin = NULL;
  ObJsonBuffer j_buf(&allocator);
  common::ObString j_arr_text("[1, 2, 3, 4, 5]");
  common::ObString j_obj_text("{\"name\": \"Safari\", \"os\": \"Mac\"}");

  // 1. test json tree
  // array
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_arr_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree));
  ASSERT_EQ(5, j_tree->element_count());
  // object
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_obj_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree));
  ASSERT_EQ(2, j_tree->element_count());
  // scalar
  ObJsonUint j_uint(1);
  j_tree = &j_uint;
  ASSERT_EQ(1, j_tree->element_count());

  // 2. test json bin
  // array
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_arr_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(5, j_bin->element_count());
  // object
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_obj_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(2, j_bin->element_count());
  // scalar -----> bug, bin only calc element count of json container
  j_tree = &j_uint;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree,
      ObJsonInType::JSON_BIN, j_bin));
  // ASSERT_EQ(1, j_bin->element_count());
}

TEST_F(TestJsonBase, test_json_type)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObIJsonBase *j_bin = NULL;
  const number::ObNumber num;
  common::ObString str;
  ObTime time;

  // 1. test json tree
  // array
  ObJsonArray j_arr(&allocator);
  j_tree = &j_arr;
  ASSERT_EQ(ObJsonNodeType::J_ARRAY, j_tree->json_type());
  // object
  ObJsonObject j_obj(&allocator);
  j_tree = &j_obj;
  ASSERT_EQ(ObJsonNodeType::J_OBJECT, j_tree->json_type());
  // null
  ObJsonNull j_null;
  j_tree = &j_null;
  ASSERT_EQ(ObJsonNodeType::J_NULL, j_tree->json_type());
  // decimal
  ObJsonDecimal j_dec(num);
  j_tree = &j_dec;
  ASSERT_EQ(ObJsonNodeType::J_DECIMAL, j_tree->json_type());
  // int
  ObJsonInt j_int(1);
  j_tree = &j_int;
  ASSERT_EQ(ObJsonNodeType::J_INT, j_tree->json_type());
  // uint
  ObJsonUint j_uint(1);
  j_tree = &j_uint;
  ASSERT_EQ(ObJsonNodeType::J_UINT, j_tree->json_type());
  // double
  ObJsonDouble j_double(0.0);
  j_tree = &j_double;
  ASSERT_EQ(ObJsonNodeType::J_DOUBLE, j_tree->json_type());
  // string
  ObJsonString j_str(str.ptr(), str.length());
  j_tree = &j_str;
  ASSERT_EQ(ObJsonNodeType::J_STRING, j_tree->json_type());
  // boolean
  ObJsonBoolean j_bool(true);
  j_tree = &j_bool;
  ASSERT_EQ(ObJsonNodeType::J_BOOLEAN, j_tree->json_type());
  // date
  ObJsonDatetime j_date(time, ObDateType);
  j_tree = &j_date;
  ASSERT_EQ(ObJsonNodeType::J_DATE, j_tree->json_type());
  // time
  ObJsonDatetime j_time(time, ObTimeType);
  j_tree = &j_time;
  ASSERT_EQ(ObJsonNodeType::J_TIME, j_tree->json_type());
  // datetime
  ObJsonDatetime j_datetime(time, ObDateTimeType);
  j_tree = &j_datetime;
  ASSERT_EQ(ObJsonNodeType::J_DATETIME, j_tree->json_type());
  // timestamp
  ObJsonDatetime j_timestamp(time, ObTimestampType);
  j_tree = &j_timestamp;
  ASSERT_EQ(ObJsonNodeType::J_TIMESTAMP, j_tree->json_type());
  // opaque
  ObJsonOpaque j_opaque(str, ObBitType);
  j_tree = &j_opaque;
  ASSERT_EQ(ObJsonNodeType::J_OPAQUE, j_tree->json_type());

  // 2. test json bin
  // array
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_arr,
      ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(ObJsonNodeType::J_ARRAY, j_bin->json_type());
  // object
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_obj,
      ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(ObJsonNodeType::J_OBJECT, j_bin->json_type());
  // null
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_null,
      ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(ObJsonNodeType::J_NULL, j_bin->json_type());
  // decimal
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_dec,
      ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(ObJsonNodeType::J_DECIMAL, j_bin->json_type());
  // int
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_int,
      ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(ObJsonNodeType::J_INT, j_bin->json_type());
  // uint
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_uint,
      ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(ObJsonNodeType::J_UINT, j_bin->json_type());
  // double
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_double,
      ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(ObJsonNodeType::J_DOUBLE, j_bin->json_type());
  // string
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_str,
      ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(ObJsonNodeType::J_STRING, j_bin->json_type());
  // boolean
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_bool,
      ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(ObJsonNodeType::J_BOOLEAN, j_bin->json_type());
  // date
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_date,
      ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(ObJsonNodeType::J_DATE, j_bin->json_type());
  // time
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_time,
      ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(ObJsonNodeType::J_TIME, j_bin->json_type());
  // datetime
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_datetime,
      ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(ObJsonNodeType::J_DATETIME, j_bin->json_type());
  // timestamp
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_timestamp,
      ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(ObJsonNodeType::J_TIMESTAMP, j_bin->json_type());
  // opaque
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_opaque,
      ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(ObJsonNodeType::J_OPAQUE, j_bin->json_type());
}

TEST_F(TestJsonBase, test_field_type)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObIJsonBase *j_bin = NULL;
  common::ObString str;
  ObTime time;

  // 1. test json tree
  // date
  ObJsonDatetime j_date(time, ObDateType);
  j_tree = &j_date;
  ASSERT_EQ(ObDateType, j_tree->field_type());
  // time
  ObJsonDatetime j_time(time, ObTimeType);
  j_tree = &j_time;
  ASSERT_EQ(ObTimeType, j_tree->field_type());
  // datetime
  ObJsonDatetime j_datetime(time, ObDateTimeType);
  j_tree = &j_datetime;
  ASSERT_EQ(ObDateTimeType, j_tree->field_type());
  // timestamp
  ObJsonDatetime j_timestamp(time, ObTimestampType);
  j_tree = &j_timestamp;
  ASSERT_EQ(ObTimestampType, j_tree->field_type());
  // opaque
  ObJsonOpaque j_opaque(str, ObBitType);
  j_tree = &j_opaque;
  ASSERT_EQ(ObBitType, j_tree->field_type());

  // 2. test json bin
  // date
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_date,
      ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(ObDateType, j_bin->field_type());
  // time
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_time,
      ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(ObTimeType, j_bin->field_type());
  // datetime
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_datetime,
      ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(ObDateTimeType, j_bin->field_type());
  // timestamp
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_timestamp,
      ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(ObTimestampType, j_bin->field_type());
  // opaque
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_opaque,
      ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(ObBitType, j_bin->field_type());
}

TEST_F(TestJsonBase, test_depth)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObIJsonBase *j_bin = NULL;
  common::ObString j_arr_text("[1, [1, 2, [1, 2, [1, 2, [1, 2]]]]]");
  common::ObString j_obj_text("{\"name\": {\"os1\": {\"os2\": {\"os3\": \"Mac\"}}}}");

  // 1. test json tree
  // array
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_arr_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree));
  ASSERT_EQ(6, j_tree->depth());
  // object
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_obj_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree));
  ASSERT_EQ(5, j_tree->depth());
  // scalar
  ObJsonUint j_uint(1);
  j_tree = &j_uint;
  ASSERT_EQ(1, j_tree->depth());

  // 2. test json bin
  // array
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_arr_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(6, j_bin->depth());
  // object
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_obj_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(5, j_bin->depth());
  // scalar
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_uint,
      ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(1, j_bin->depth());
}

TEST_F(TestJsonBase, test_get_location)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObIJsonBase *j_bin = NULL;
  ObIJsonBase *jb_ele_ptr = NULL;
  ObJsonBuffer j_buf(&allocator);
  common::ObString j_arr_text("[1, 2, 3, [1, 2, [1, 2, [1, 2, [1, 2]]]]]");
  common::ObString j_obj_text("{\"name\": {\"os1\": {\"os2\": {\"os3\": \"Mac\"}}}}");

  // 1. test json tree
  // array
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_arr_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree));
  
  ASSERT_EQ(OB_SUCCESS, j_tree->get_array_element(2, jb_ele_ptr));
  ASSERT_EQ(OB_SUCCESS, jb_ele_ptr->get_location(j_buf));
  ASSERT_EQ(0, strncmp("$[2]", j_buf.ptr(), j_buf.length()));
  // object
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_obj_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree));
  ASSERT_EQ(OB_SUCCESS, j_tree->get_object_value(0, jb_ele_ptr));
  j_buf.reset();
  ASSERT_EQ(OB_SUCCESS, jb_ele_ptr->get_location(j_buf));
  ASSERT_EQ(0, strncmp("$.name", j_buf.ptr(), j_buf.length()));

  // 2. test json bin
  // array
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_arr_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
  j_buf.reset();
  ASSERT_EQ(OB_SUCCESS, j_bin->get_location(j_buf));
  ASSERT_EQ(0, strncmp("$[2]", j_buf.ptr(), j_buf.length()));
  // object
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_obj_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
  j_buf.reset();
  ASSERT_EQ(OB_SUCCESS, j_bin->get_location(j_buf));
  ASSERT_EQ(0, strncmp("$.name", j_buf.ptr(), j_buf.length()));
}

TEST_F(TestJsonBase, test_get_raw_binary)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObIJsonBase *j_bin = NULL;
  common::ObString str;
  common::ObString j_arr_text("[1, 2, 3, [1, 2, [1, 2, [1, 2, [1, 2]]]]]");

  // 1. test json tree
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_arr_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree));
  ASSERT_EQ(OB_SUCCESS, j_tree->get_raw_binary(str, &allocator));

  // 2. test json bin
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_arr_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(OB_SUCCESS, j_bin->get_raw_binary(str));
}

TEST_F(TestJsonBase, test_get_key)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObIJsonBase *j_bin = NULL;
  common::ObString key;
  common::ObString j_obj_text("{\"name\": {\"os1\": {\"os2\": {\"os3\": \"Mac\"}}}}");

  // 1. test json tree
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_obj_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree));
  ASSERT_EQ(OB_SUCCESS, j_tree->get_key(0, key));
  ASSERT_EQ(0, strncmp(key.ptr(), "name", key.length()));

  // 2. test json bin
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_obj_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
  key.reset();
  ASSERT_EQ(OB_SUCCESS, j_bin->get_key(0, key));
  ASSERT_EQ(0, strncmp(key.ptr(), "name", key.length()));
}

TEST_F(TestJsonBase, test_get_array_element)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObIJsonBase *j_bin = NULL;
  ObIJsonBase *jb_ele_ptr = NULL;
  common::ObString j_arr_text("[1, 2, 3, [1, 2, [1, 2, [1, 2, [1, 2]]]]]");

  // 1. test json tree
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_arr_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree));
  ASSERT_EQ(OB_SUCCESS, j_tree->get_array_element(1, jb_ele_ptr));
  ASSERT_EQ(2, jb_ele_ptr->get_int());

  // 2. test json bin
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_arr_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
  ObJsonBin j_bin_tmp(&allocator);
  jb_ele_ptr = &j_bin_tmp;
  ASSERT_EQ(OB_SUCCESS, j_tree->get_array_element(1, jb_ele_ptr));
  ASSERT_EQ(2, jb_ele_ptr->get_int());
}

TEST_F(TestJsonBase, test_get_object_value)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObIJsonBase *j_bin = NULL;
  ObIJsonBase *jb_ele_ptr = NULL;
  ObJsonBuffer j_buf(&allocator);
  common::ObString key("name");
  common::ObString j_obj_text("{\"name\": {\"os1\": {\"os2\": {\"os3\": \"Mac\"}}}}");

  // 1. test json tree
  // by key
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_obj_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree));
  ASSERT_EQ(OB_SUCCESS, j_tree->get_object_value(key, jb_ele_ptr));
  ASSERT_EQ(OB_SUCCESS, jb_ele_ptr->print(j_buf, false));
  ASSERT_EQ(0, strncmp("{\"os1\": {\"os2\": {\"os3\": \"Mac\"}}}", j_buf.ptr(), j_buf.length()));
  // by index
  ASSERT_EQ(OB_SUCCESS, j_tree->get_object_value(0, jb_ele_ptr));
  j_buf.reset();
  ASSERT_EQ(OB_SUCCESS, jb_ele_ptr->print(j_buf, false));
  ASSERT_EQ(0, strncmp("{\"os1\": {\"os2\": {\"os3\": \"Mac\"}}}", j_buf.ptr(), j_buf.length()));

  // 2. test json bin
  // by key
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_obj_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
  ObJsonBin j_bin_tmp(&allocator);
  jb_ele_ptr = &j_bin_tmp;
  ASSERT_EQ(OB_SUCCESS, j_tree->get_object_value(key, jb_ele_ptr));
  j_buf.reset();
  ASSERT_EQ(OB_SUCCESS, jb_ele_ptr->print(j_buf, false));
  ASSERT_EQ(0, strncmp("{\"os1\": {\"os2\": {\"os3\": \"Mac\"}}}", j_buf.ptr(), j_buf.length()));
  // by index
  jb_ele_ptr = &j_bin_tmp;
  ASSERT_EQ(OB_SUCCESS, j_tree->get_object_value(0, jb_ele_ptr));
  j_buf.reset();
  ASSERT_EQ(OB_SUCCESS, jb_ele_ptr->print(j_buf, false));
  ASSERT_EQ(0, strncmp("{\"os1\": {\"os2\": {\"os3\": \"Mac\"}}}", j_buf.ptr(), j_buf.length()));
}

TEST_F(TestJsonBase, test_get_used_size)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObIJsonBase *j_bin = NULL;
  uint64_t use_size = 0;
  common::ObString j_obj_text("{\"name\": {\"os1\": {\"os2\": {\"os3\": \"Mac\"}}}}");

  // 1. test json tree
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_obj_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree));
  ASSERT_EQ(OB_SUCCESS, j_tree->get_used_size(use_size));
  ASSERT_EQ(50, use_size);

  // 2. test json bin
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_obj_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(OB_SUCCESS, j_bin->get_used_size(use_size));
  ASSERT_EQ(50, use_size);
}

TEST_F(TestJsonBase, test_get_free_space)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObIJsonBase *j_bin = NULL;
  uint64_t free_space = 0;
  common::ObString key("k2");
  common::ObString j_obj_text("{\"k1\": 1, \"k2\": 2, \"k3\": 3}");

  // 1. test json tree
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_obj_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree));
  ASSERT_EQ(OB_SUCCESS, j_tree->object_remove(key));
  ASSERT_EQ(OB_SUCCESS, j_tree->get_free_space(free_space));
  ASSERT_EQ(0, free_space);

  // 2. test json bin
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_obj_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(OB_SUCCESS, j_bin->object_remove(key));
  ASSERT_EQ(OB_SUCCESS, j_bin->get_free_space(free_space));
  ASSERT_EQ(4, free_space);
}

TEST_F(TestJsonBase, test_array_append)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObJsonBuffer j_buf(&allocator);
  common::ObString j_text("[1, 2, 3, 4, 5]");
  ObJsonUint j_uint0(6);
  ObJsonUint j_uint1(7);

  // test json tree
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree));
  ASSERT_TRUE(j_tree->is_tree());
  ASSERT_EQ(ObJsonNodeType::J_ARRAY, j_tree->json_type());
  ASSERT_EQ(5, j_tree->element_count());
  ASSERT_EQ(OB_SUCCESS, j_tree->array_append(&j_uint0));
  ASSERT_EQ(OB_SUCCESS, j_tree->print(j_buf, false));
  ASSERT_EQ(0, strncmp("[1, 2, 3, 4, 5, 6]", j_buf.ptr(), j_buf.length()));
  
  // test json bin
  ObIJsonBase *j_bin = NULL;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree,
      ObJsonInType::JSON_BIN, j_bin));
  ObIJsonBase *j_bin_val = NULL;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_uint1,
      ObJsonInType::JSON_BIN, j_bin_val));
  ASSERT_EQ(OB_SUCCESS, j_bin->array_append(j_bin_val));
  j_buf.reset();
  ASSERT_EQ(OB_SUCCESS, j_bin->print(j_buf, false));
  ASSERT_EQ(0, strncmp("[1, 2, 3, 4, 5, 6, 7]", j_buf.ptr(), j_buf.length()));
}

TEST_F(TestJsonBase, test_array_insert)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObJsonBuffer j_buf(&allocator);
  common::ObString j_text("[1, 2, 4, 5]");
  ObJsonUint j_uint0(3);
  ObJsonUint j_uint1(6);

  // test json tree
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree));
  ASSERT_TRUE(j_tree->is_tree());
  ASSERT_EQ(ObJsonNodeType::J_ARRAY, j_tree->json_type());
  ASSERT_EQ(4, j_tree->element_count());
  ASSERT_EQ(OB_SUCCESS, j_tree->array_insert(2, &j_uint0));
  ASSERT_EQ(OB_SUCCESS, j_tree->print(j_buf, false));
  ASSERT_EQ(0, strncmp("[1, 2, 3, 4, 5]", j_buf.ptr(), j_buf.length()));
  
  // test json bin
  ObIJsonBase *j_bin = NULL;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree,
      ObJsonInType::JSON_BIN, j_bin));
  ObIJsonBase *j_bin_val = NULL;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_uint1,
      ObJsonInType::JSON_BIN, j_bin_val));
  ASSERT_EQ(OB_SUCCESS, j_bin->array_insert(2, j_bin_val));
  j_buf.reset();
  ASSERT_EQ(OB_SUCCESS, j_bin->print(j_buf, false));
  ASSERT_EQ(0, strncmp("[1, 2, 6, 3, 4, 5]", j_buf.ptr(), j_buf.length()));
}

TEST_F(TestJsonBase, test_array_remove)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObJsonBuffer j_buf(&allocator);
  common::ObString j_text("[1, 2, 3, 4, 5]");

  // test json tree
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree));
  ASSERT_TRUE(j_tree->is_tree());
  ASSERT_EQ(ObJsonNodeType::J_ARRAY, j_tree->json_type());
  ASSERT_EQ(5, j_tree->element_count());
  ASSERT_EQ(OB_SUCCESS, j_tree->array_remove(3));
  ASSERT_EQ(OB_SUCCESS, j_tree->print(j_buf, false));
  ASSERT_EQ(0, strncmp("[1, 2, 3, 5]", j_buf.ptr(), j_buf.length()));
  
  // test json bin
  ObIJsonBase *j_bin = NULL;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree,
      ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(OB_SUCCESS, j_bin->array_remove(3));
  ASSERT_EQ(3, j_bin->element_count());
  j_buf.reset();
  ASSERT_EQ(OB_SUCCESS, j_bin->print(j_buf, false));
  ASSERT_EQ(0, strncmp("[1, 2, 3]", j_buf.ptr(), j_buf.length()));
}

TEST_F(TestJsonBase, test_object_add)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObJsonBuffer j_buf(&allocator);

  // test json tree
  common::ObString j_text1("{\"name\": \"Safari\", \"os\": \"Mac\"}");
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text1,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree));
  ASSERT_TRUE(j_tree->is_tree());
  ASSERT_EQ(ObJsonNodeType::J_OBJECT, j_tree->json_type());
  ASSERT_EQ(2, j_tree->element_count());
  common::ObString key1("key1");
  ObJsonUint j_uint1(1);
  ASSERT_EQ(OB_SUCCESS, j_tree->object_add(key1, &j_uint1));
  ASSERT_EQ(OB_SUCCESS, j_tree->print(j_buf, false));
  ASSERT_EQ(0, strncmp("{\"os\": \"Mac\", \"key1\": 1, \"name\": \"Safari\"}",
      j_buf.ptr(), j_buf.length()));

  // test json bin
  ObIJsonBase *j_bin = NULL;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree,
      ObJsonInType::JSON_BIN, j_bin));
  common::ObString key2("key2");
  ObJsonUint j_uint2(2);
  ObIJsonBase *j_bin_val2 = NULL;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_uint2,
      ObJsonInType::JSON_BIN, j_bin_val2));
  ASSERT_EQ(OB_SUCCESS, j_bin->object_add(key2, j_bin_val2));
  j_buf.reset();
  ASSERT_EQ(OB_SUCCESS, j_bin->print(j_buf, false));
  ASSERT_EQ(0, strncmp("{\"os\": \"Mac\", \"key1\": 1, \"name\": \"Safari\", \"key2\": 2}",
      j_buf.ptr(), j_buf.length()));
}

TEST_F(TestJsonBase, test_object_remove)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObJsonBuffer j_buf(&allocator);

  // test json tree
  common::ObString j_text1("{\"name\": \"Safari\", \"os\": \"Mac\"}");
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::get_json_base(&allocator, j_text1,
      ObJsonInType::JSON_TREE, ObJsonInType::JSON_TREE, j_tree));
  ASSERT_TRUE(j_tree->is_tree());
  ASSERT_EQ(ObJsonNodeType::J_OBJECT, j_tree->json_type());
  ASSERT_EQ(2, j_tree->element_count());
  common::ObString key0("name");
  ASSERT_EQ(OB_SUCCESS, j_tree->object_remove(key0));
  ASSERT_EQ(OB_SUCCESS, j_tree->print(j_buf, false));
  ASSERT_EQ(0, strncmp("{\"os\": \"Mac\"}", j_buf.ptr(), j_buf.length()));

  // test json bin
  ObIJsonBase *j_bin = NULL;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree,
      ObJsonInType::JSON_BIN, j_bin));
  common::ObString key1("os");
  ASSERT_EQ(OB_SUCCESS, j_bin->object_remove(key1));
  j_buf.reset();
  ASSERT_EQ(OB_SUCCESS, j_bin->print(j_buf, false));
  ASSERT_EQ(0, strncmp("{}", j_buf.ptr(), j_buf.length()));
}

TEST_F(TestJsonBase, test_replace)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObJsonBuffer j_buf(&allocator);

  // 1. test json tree
  // array
  ObJsonArray j_arr(&allocator);
  j_tree = &j_arr;
  ObJsonUint j_uint0(0);
  ASSERT_EQ(OB_SUCCESS, j_tree->array_append(&j_uint0));
  ObJsonUint j_uint1(1);
  ASSERT_EQ(OB_SUCCESS, j_tree->replace(&j_uint0, &j_uint1));
  ASSERT_EQ(OB_SUCCESS, j_tree->print(j_buf, false));
  ASSERT_EQ(0, strncmp("[1]", j_buf.ptr(), j_buf.length()));
  // object
  ObJsonObject j_obj(&allocator);
  j_tree = &j_obj;
  common::ObString key0("key0");
  ObJsonUint j_val0(0);
  ASSERT_EQ(OB_SUCCESS, j_tree->object_add(key0, &j_val0));
  ObJsonUint j_val1(1);
  ASSERT_EQ(OB_SUCCESS, j_tree->replace(&j_val0, &j_val1));
  j_buf.reset();
  ASSERT_EQ(OB_SUCCESS, j_tree->print(j_buf, false));
  ASSERT_EQ(0, strncmp("{\"key0\": 1}", j_buf.ptr(), j_buf.length()));

  // 2. test json bin
  // array
  ObIJsonBase *j_bin = NULL;
  ObJsonBin j_bin_tmp(&allocator);
  ObIJsonBase *jb_bin_old_ptr = &j_bin_tmp;
  j_tree = &j_arr;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree,
      ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(OB_SUCCESS, j_bin->get_array_element(0, jb_bin_old_ptr));
  ObIJsonBase *j_bin_new_ptr = NULL;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_uint0,
      ObJsonInType::JSON_BIN, j_bin_new_ptr));
  ASSERT_EQ(OB_SUCCESS, j_bin->replace(jb_bin_old_ptr, j_bin_new_ptr));
  j_buf.reset();
  ASSERT_EQ(OB_SUCCESS, j_bin->print(j_buf, false));
  ASSERT_EQ(0, strncmp("[0]", j_buf.ptr(), j_buf.length()));

  // object
  j_tree = &j_obj;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree,
      ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(OB_SUCCESS, j_bin->get_object_value(key0, jb_bin_old_ptr));
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_val0,
      ObJsonInType::JSON_BIN, j_bin_new_ptr));
  ASSERT_EQ(OB_SUCCESS, j_bin->replace(jb_bin_old_ptr, j_bin_new_ptr));
  j_buf.reset();
  ASSERT_EQ(OB_SUCCESS, j_bin->print(j_buf, false));
  ASSERT_EQ(0, strncmp("{\"key0\": 0}", j_buf.ptr(), j_buf.length()));
}

TEST_F(TestJsonBase, test_merge_tree)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_base0 = NULL;
  ObIJsonBase *j_base1 = NULL;
  ObIJsonBase *res = NULL;
  ObJsonBuffer j_buf(&allocator);

  // 1. test json tree
  // array
  ObJsonArray j_arr0(&allocator);
  j_base0 = &j_arr0;
  ObJsonUint j_uint0_0(0);
  ObJsonUint j_uint0_1(1);
  ASSERT_EQ(OB_SUCCESS, j_base0->array_append(&j_uint0_0));
  ASSERT_EQ(OB_SUCCESS, j_base0->array_append(&j_uint0_1));
  ObJsonArray j_arr1(&allocator);
  j_base1 = &j_arr1;
  ObJsonUint j_uint1_0(0);
  ObJsonUint j_uint1_1(1);
  ASSERT_EQ(OB_SUCCESS, j_base1->array_append(&j_uint1_0));
  ASSERT_EQ(OB_SUCCESS, j_base1->array_append(&j_uint1_1));
  ASSERT_EQ(OB_SUCCESS, j_base0->merge_tree(&allocator, j_base1, res));
  ASSERT_EQ(OB_SUCCESS, res->print(j_buf, false));
  ASSERT_EQ(0, strncmp("[0, 1, 0, 1]", j_buf.ptr(), j_buf.length()));
  // object
  ObJsonObject j_obj0(&allocator);
  j_base0 = &j_obj0;
  common::ObString key0_0("key0");
  ObJsonUint j_val0_0(0);
  ASSERT_EQ(OB_SUCCESS, j_base0->object_add(key0_0, &j_val0_0));
  ObJsonObject j_obj1(&allocator);
  j_base1 = &j_obj1;
  common::ObString key1_0("key0");
  ObJsonUint j_val1_0(1);
  ASSERT_EQ(OB_SUCCESS, j_base1->object_add(key1_0, &j_val1_0));
  ASSERT_EQ(OB_SUCCESS, j_base0->merge_tree(&allocator, j_base1, res));
  j_buf.reset();
  ASSERT_EQ(OB_SUCCESS, res->print(j_buf, false));
  ASSERT_EQ(0, strncmp("{\"key0\": [0, 1]}", j_buf.ptr(), j_buf.length()));

  // 2. test json bin
  // array
  j_base0 = &j_arr0;
  j_base1 = &j_arr1;
  ObIJsonBase *j_bin0 = NULL;
  ObIJsonBase *j_bin1 = NULL;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_base0,
      ObJsonInType::JSON_BIN, j_bin0));
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_base1,
      ObJsonInType::JSON_BIN, j_bin1));
  ASSERT_EQ(OB_SUCCESS, j_bin0->merge_tree(&allocator, j_bin1, res));
  j_buf.reset();
  ASSERT_EQ(OB_SUCCESS, res->print(j_buf, false));
  ASSERT_EQ(0, strncmp("[0, 1, 0, 1]", j_buf.ptr(), j_buf.length()));

  // object
  j_base0 = &j_obj0;
  j_base1 = &j_obj1;
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_base0,
      ObJsonInType::JSON_BIN, j_bin0));
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_base1,
      ObJsonInType::JSON_BIN, j_bin1));
  ASSERT_EQ(OB_SUCCESS, j_bin0->merge_tree(&allocator, j_bin1, res));
  j_buf.reset();
  ASSERT_EQ(OB_SUCCESS, res->print(j_buf, false));
  ASSERT_EQ(0, strncmp("{\"key0\": [0, 1]}", j_buf.ptr(), j_buf.length()));
}

TEST_F(TestJsonBase, test_get_boolean)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObIJsonBase *j_bin = NULL;
  ObJsonBoolean j_bool(true);

  // 1. test json tree
  j_tree = &j_bool;
  ASSERT_EQ(true, j_tree->get_boolean());
  j_bool.set_value(false);
  ASSERT_EQ(false, j_tree->get_boolean());

  // 2. test for json bin
  j_bool.set_value(true);
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree,
      ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(true, j_bin->get_boolean());
  j_bool.set_value(false);
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree,
      ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(false, j_bin->get_boolean());
}

TEST_F(TestJsonBase, test_get_double)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObIJsonBase *j_bin = NULL;
  ObJsonDouble j_double(1.0);

  // 1. test json tree
  j_tree = &j_double;
  ASSERT_EQ(1.0, j_tree->get_double());

  // 2. test for json bin
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree,
      ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(1.0, j_bin->get_double());
}

TEST_F(TestJsonBase, test_get_int)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObIJsonBase *j_bin = NULL;
  ObJsonInt j_int(1);

  // 1. test json tree
  j_tree = &j_int;
  ASSERT_EQ(1, j_tree->get_int());

  // 2. test for json bin
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree,
      ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(1, j_bin->get_int());
}

TEST_F(TestJsonBase, test_get_uint)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObIJsonBase *j_bin = NULL;
  ObJsonUint j_uint(1);

  // 1. test json tree
  j_tree = &j_uint;
  ASSERT_EQ(1, j_tree->get_uint());

  // 2. test for json bin
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree,
      ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(1, j_bin->get_uint());
}

TEST_F(TestJsonBase, test_get_data)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObIJsonBase *j_bin = NULL;
  ObJsonString j_str("abc", strlen("abc"));

  // 1. test json tree
  j_tree = &j_str;
  ASSERT_EQ(0, strncmp("abc", j_tree->get_data(), j_tree->get_data_length()));

  // 2. test for json bin
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree,
      ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(0, strncmp("abc", j_bin->get_data(), j_bin->get_data_length()));
}

TEST_F(TestJsonBase, test_get_data_length)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObIJsonBase *j_bin = NULL;
  ObJsonString j_str("abc", strlen("abc"));

  // 1. test json tree
  j_tree = &j_str;
  ASSERT_EQ(3, j_tree->get_data_length());

  // 2. test for json bin
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree,
      ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(3, j_tree->get_data_length());
}

TEST_F(TestJsonBase, test_get_decimal_data)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObIJsonBase *j_bin = NULL;
  const int64_t MAX_BUF_SIZE = 512;
  char buf[MAX_BUF_SIZE] = {0};
  number::ObNumber num;
  int length = sprintf(buf, "%f", 10.222);
  ASSERT_EQ(OB_SUCCESS, num.from(buf, allocator));
  ObJsonDecimal j_dec(num, 3, 3);
  number::ObNumber res;
  int64_t val = 0;

  // 1. test json tree
  j_tree = &j_dec;
  res = j_tree->get_decimal_data();
  ASSERT_EQ(num, res);

  // 2. test for json bin
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree,
      ObJsonInType::JSON_BIN, j_bin));
  res = j_bin->get_decimal_data();
  ASSERT_EQ(num, res);
}

TEST_F(TestJsonBase, test_get_decimal_precision)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObIJsonBase *j_bin = NULL;
  const int64_t MAX_BUF_SIZE = 512;
  char buf[MAX_BUF_SIZE] = {0};
  number::ObNumber num;
  int length = sprintf(buf, "%f", 10.222);
  ASSERT_EQ(OB_SUCCESS, num.from(buf, allocator));
  ObJsonDecimal j_dec(num, 3, 3);

  // 1. test json tree
  j_tree = &j_dec;
  ASSERT_EQ(3, j_tree->get_decimal_precision());

  // 2. test for json bin
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree,
      ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(3, j_tree->get_decimal_precision());
}

TEST_F(TestJsonBase, test_get_decimal_scale)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObIJsonBase *j_bin = NULL;
  const int64_t MAX_BUF_SIZE = 512;
  char buf[MAX_BUF_SIZE] = {0};
  number::ObNumber num;
  int length = sprintf(buf, "%f", 10.222);
  ASSERT_EQ(OB_SUCCESS, num.from(buf, allocator));
  ObJsonDecimal j_dec(num, 3, 3);

  // 1. test json tree
  j_tree = &j_dec;
  ASSERT_EQ(3, j_tree->get_decimal_scale());

  // 2. test for json bin
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree,
      ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(3, j_tree->get_decimal_scale());
}

TEST_F(TestJsonBase, test_get_obtime)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObIJsonBase *j_bin = NULL;
  ObTime t_date_time;
  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::datetime_to_ob_time(1429089727 * USECS_PER_SEC,
      NULL, t_date_time));
  ObJsonDatetime j_datetime(t_date_time, ObDateTimeType);
  ObTime res;

  // 1. test json tree
  j_tree = &j_datetime;
  ASSERT_EQ(OB_SUCCESS, j_tree->get_obtime(res));

  // 2. test for json bin
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, j_tree,
      ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(OB_SUCCESS, j_tree->get_obtime(res));
}

TEST_F(TestJsonBase, test_to_int)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObIJsonBase *j_bin = NULL;
  ObJsonInt j_int(123);
  ObJsonUint j_uint(123);
  ObJsonString j_str("123", strlen("123"));
  ObJsonBoolean j_bool(true);
  ObJsonDouble j_double(123.123);
  int64_t res = 0;

  // 1. test json tree
  // int
  j_tree = &j_int;
  ASSERT_EQ(OB_SUCCESS, j_tree->to_int(res));
  ASSERT_EQ(123, res);
  // uint
  j_tree = &j_uint;
  ASSERT_EQ(OB_SUCCESS, j_tree->to_int(res));
  ASSERT_EQ(123, res);
  // string
  j_tree = &j_str;
  ASSERT_EQ(OB_SUCCESS, j_tree->to_int(res));
  ASSERT_EQ(123, res);
  // bool
  j_tree = &j_bool;
  ASSERT_EQ(OB_SUCCESS, j_tree->to_int(res));
  ASSERT_EQ(1, res);
  // double
  j_tree = &j_double;
  ASSERT_EQ(OB_SUCCESS, j_tree->to_int(res));
  ASSERT_EQ(123, res);

  // 2. test json bin
  // int
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_int,
      ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(OB_SUCCESS, j_bin->to_int(res));
  ASSERT_EQ(123, res);
  // uint
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_uint,
      ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(OB_SUCCESS, j_bin->to_int(res));
  ASSERT_EQ(123, res);
  // string
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_str,
      ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(OB_SUCCESS, j_bin->to_int(res));
  ASSERT_EQ(123, res);
  // bool
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_bool,
      ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(OB_SUCCESS, j_bin->to_int(res));
  ASSERT_EQ(1, res);
  // double
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_double,
      ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(OB_SUCCESS, j_bin->to_int(res));
  ASSERT_EQ(123, res);
}

TEST_F(TestJsonBase, test_to_uint)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObIJsonBase *j_bin = NULL;
  ObJsonInt j_int(123);
  ObJsonUint j_uint(123);
  ObJsonString j_str("123", strlen("123"));
  ObJsonBoolean j_bool(true);
  ObJsonDouble j_double(123.123);
  uint64_t res = 0;

  // 1. test json tree
  // int
  j_tree = &j_int;
  ASSERT_EQ(OB_SUCCESS, j_tree->to_uint(res));
  ASSERT_EQ(123, res);
  // uint
  j_tree = &j_uint;
  ASSERT_EQ(OB_SUCCESS, j_tree->to_uint(res));
  ASSERT_EQ(123, res);
  // string
  j_tree = &j_str;
  ASSERT_EQ(OB_SUCCESS, j_tree->to_uint(res));
  ASSERT_EQ(123, res);
  // bool
  j_tree = &j_bool;
  ASSERT_EQ(OB_SUCCESS, j_tree->to_uint(res));
  ASSERT_EQ(1, res);
  // double
  j_tree = &j_double;
  ASSERT_EQ(OB_SUCCESS, j_tree->to_uint(res));
  ASSERT_EQ(123, res);

  // 2. test json bin
  // int
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_int,
      ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(OB_SUCCESS, j_bin->to_uint(res));
  ASSERT_EQ(123, res);
  // uint
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_uint,
      ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(OB_SUCCESS, j_bin->to_uint(res));
  ASSERT_EQ(123, res);
  // string
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_str,
      ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(OB_SUCCESS, j_bin->to_uint(res));
  ASSERT_EQ(123, res);
  // bool
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_bool,
      ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(OB_SUCCESS, j_bin->to_uint(res));
  ASSERT_EQ(1, res);
  // double
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_double,
      ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(OB_SUCCESS, j_bin->to_uint(res));
  ASSERT_EQ(123, res);
}

TEST_F(TestJsonBase, test_to_double)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObIJsonBase *j_bin = NULL;
  ObJsonInt j_int(123);
  ObJsonUint j_uint(123);
  ObJsonString j_str("123.123", strlen("123.123"));
  ObJsonBoolean j_bool(true);
  ObJsonDouble j_double(123.123);
  double res = 0;

  // 1. test json tree
  // int
  j_tree = &j_int;
  ASSERT_EQ(OB_SUCCESS, j_tree->to_double(res));
  ASSERT_EQ(123, res);
  // uint
  j_tree = &j_uint;
  ASSERT_EQ(OB_SUCCESS, j_tree->to_double(res));
  ASSERT_EQ(123, res);
  // string
  j_tree = &j_str;
  ASSERT_EQ(OB_SUCCESS, j_tree->to_double(res));
  ASSERT_EQ(123.123, res);
  // bool
  j_tree = &j_bool;
  ASSERT_EQ(OB_SUCCESS, j_tree->to_double(res));
  ASSERT_EQ(1, res);
  // double
  j_tree = &j_double;
  ASSERT_EQ(OB_SUCCESS, j_tree->to_double(res));
  ASSERT_EQ(123.123, res);

  // 2. test json bin
  // int
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_int,
      ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(OB_SUCCESS, j_bin->to_double(res));
  ASSERT_EQ(123, res);
  // uint
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_uint,
      ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(OB_SUCCESS, j_bin->to_double(res));
  ASSERT_EQ(123, res);
  // string
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_str,
      ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(OB_SUCCESS, j_bin->to_double(res));
  ASSERT_EQ(123.123, res);
  // bool
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_bool,
      ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(OB_SUCCESS, j_bin->to_double(res));
  ASSERT_EQ(1, res);
  // double
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_double,
      ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(OB_SUCCESS, j_bin->to_double(res));
  ASSERT_EQ(123.123, res);
}

TEST_F(TestJsonBase, test_to_number)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObIJsonBase *j_bin = NULL;
  ObJsonInt j_int(123);
  ObJsonUint j_uint(123);
  ObJsonString j_str("123.123", strlen("123.123"));
  ObJsonBoolean j_bool(true);
  ObJsonDouble j_double(123.123);
  number::ObNumber num;
  const int64_t MAX_BUF_SIZE = 512;
  char buf[MAX_BUF_SIZE] = {0};
  int length = sprintf(buf, "%f", 123.1234);
  ASSERT_EQ(OB_SUCCESS, num.from(buf, allocator));
  ObJsonDecimal j_dec(num);
  number::ObNumber res;
  int64_t res_int = 123;

  // 1. test json tree
  // number
  j_tree = &j_dec;
  ASSERT_EQ(OB_SUCCESS, j_tree->to_number(&allocator, res));
  ASSERT_EQ(false, res == res_int);
  // int
  j_tree = &j_int;
  ASSERT_EQ(OB_SUCCESS, j_tree->to_number(&allocator, res));
  ASSERT_EQ(true, res == res_int);
  // uint
  j_tree = &j_uint;
  ASSERT_EQ(OB_SUCCESS, j_tree->to_number(&allocator, res));
  ASSERT_EQ(true, res == res_int);
  // string
  j_tree = &j_str;
  ASSERT_EQ(OB_SUCCESS, j_tree->to_number(&allocator, res));
  ASSERT_EQ(false, res == res_int);
  // bool
  j_tree = &j_bool;
  ASSERT_EQ(OB_SUCCESS, j_tree->to_number(&allocator, res));
  res_int = 1;
  ASSERT_EQ(true, res == res_int);
  // double
  j_tree = &j_double;
  ASSERT_EQ(OB_SUCCESS, j_tree->to_number(&allocator, res));
  res_int = 123;
  ASSERT_EQ(false, res == res_int);

  // 2. test json bin
  // number
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_dec,
      ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(OB_SUCCESS, j_bin->to_number(&allocator, res));
  ASSERT_EQ(false, res == res_int);
  // int
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_int,
      ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(OB_SUCCESS, j_bin->to_number(&allocator, res));
  ASSERT_EQ(true, res == res_int);
  // uint
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_uint,
      ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(OB_SUCCESS, j_bin->to_number(&allocator, res));
  ASSERT_EQ(true, res == res_int);
  // string
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_str,
      ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(OB_SUCCESS, j_bin->to_number(&allocator, res));
  ASSERT_EQ(false, res == res_int);
  // bool
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_bool,
      ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(OB_SUCCESS, j_bin->to_number(&allocator, res));
  res_int = 1;
  ASSERT_EQ(true, res == res_int);
  // double
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_double,
      ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(OB_SUCCESS, j_bin->to_number(&allocator, res));
  res_int = 123;
  ASSERT_EQ(false, res == res_int);
}

TEST_F(TestJsonBase, test_to_datetime)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObIJsonBase *j_bin = NULL;
  int64_t res = 0;
  int32_t date = 0;

  // 1. test json tree
  // datetime
  ObTime ob_time1;
  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::datetime_to_ob_time(1429089727 * USECS_PER_SEC,
      NULL, ob_time1));
  ObJsonDatetime j_datetime(ob_time1, ObDateTimeType);
  j_tree = &j_datetime;
  ASSERT_EQ(OB_SUCCESS, j_tree->to_datetime(res));
  ASSERT_EQ(1429089727 * USECS_PER_SEC, res);
  // date
  ObTime ob_time2;
  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::date_to_ob_time(16540, ob_time2));
  ObJsonDatetime j_date(ob_time2, ObDateType);
  j_tree = &j_date;
  ASSERT_EQ(OB_SUCCESS, j_tree->to_datetime(res));
  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::datetime_to_date(res, NULL, date));
  ASSERT_EQ(16540, date);
  // timestamp
  ObTime ob_time3;
  const int64_t MAX_BUF_SIZE = 256;
  char buf[MAX_BUF_SIZE] = {0};
  strcpy(buf, "2015-4-15 11:12:00.1234567 -07:00");
  ObString str;
  str.assign(buf, static_cast<int32_t>(strlen(buf)));
  ObTimeZoneInfo tz_info;
  ObTimeConvertCtx cvrt_ctx(&tz_info, false);
  cvrt_ctx.oracle_nls_format_ = ObTimeConverter::COMPAT_OLD_NLS_TIMESTAMP_TZ_FORMAT;
  ObOTimestampData ot_data;
  int16_t scale = 0;
  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::str_to_otimestamp(str, cvrt_ctx, ObTimestampTZType,
      ot_data, scale));
  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::otimestamp_to_ob_time(ObTimestampNanoType,
      ot_data, NULL, ob_time3));
  ObJsonDatetime j_timestamp(ObJsonNodeType::J_TIMESTAMP, ob_time3);
  j_tree = &j_timestamp;
  ASSERT_EQ(OB_SUCCESS, j_tree->to_datetime(res));
  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::datetime_to_date(res, NULL, date));
  ASSERT_EQ(16540, date);
  // string
  MEMSET(buf, 0, sizeof(buf));
  strcpy(buf, "2015-4-15 11:12:00.1234567");
  ObJsonString j_str(buf, strlen(buf));
  j_tree = &j_str;
  ASSERT_EQ(OB_SUCCESS, j_tree->to_datetime(res));
  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::datetime_to_date(res, NULL, date));
  ASSERT_EQ(16540, date);

  // 2. test json bin
  // datetime
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_datetime,
      ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(OB_SUCCESS, j_bin->to_datetime(res));
  ASSERT_EQ(1429089727 * USECS_PER_SEC, res);
  // date
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_date,
      ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(OB_SUCCESS, j_bin->to_datetime(res));
  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::datetime_to_date(res, NULL, date));
  ASSERT_EQ(16540, date);
  // timestamp
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_timestamp,
      ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(OB_SUCCESS, j_bin->to_datetime(res));
  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::datetime_to_date(res, NULL, date));
  ASSERT_EQ(16540, date);
  // string
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_str,
      ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(OB_SUCCESS, j_bin->to_datetime(res));
  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::datetime_to_date(res, NULL, date));
  ASSERT_EQ(16540, date);
}

TEST_F(TestJsonBase, test_to_date)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObIJsonBase *j_bin = NULL;
  int32_t res = 0;

  // 1. test json tree
  // datetime
  ObTime ob_time1;
  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::datetime_to_ob_time(1429089727 * USECS_PER_SEC,
      NULL, ob_time1));
  ObJsonDatetime j_datetime(ob_time1, ObDateTimeType);
  j_tree = &j_datetime;
  ASSERT_EQ(OB_SUCCESS, j_tree->to_date(res));
  ASSERT_EQ(16540, res);
  // date
  ObTime ob_time2;
  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::date_to_ob_time(16540, ob_time2));
  ObJsonDatetime j_date(ob_time2, ObDateType);
  j_tree = &j_date;
  ASSERT_EQ(OB_SUCCESS, j_tree->to_date(res));
  ASSERT_EQ(16540, res);
  // timestamp
  ObTime ob_time3;
  const int64_t MAX_BUF_SIZE = 256;
  char buf[MAX_BUF_SIZE] = {0};
  strcpy(buf, "2015-4-15 11:12:00.1234567 -07:00");
  ObString str;
  str.assign(buf, static_cast<int32_t>(strlen(buf)));
  ObTimeZoneInfo tz_info;
  ObTimeConvertCtx cvrt_ctx(&tz_info, false);
  cvrt_ctx.oracle_nls_format_ = ObTimeConverter::COMPAT_OLD_NLS_TIMESTAMP_TZ_FORMAT;
  ObOTimestampData ot_data;
  int16_t scale = 0;
  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::str_to_otimestamp(str, cvrt_ctx, ObTimestampTZType,
      ot_data, scale));
  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::otimestamp_to_ob_time(ObTimestampNanoType,
      ot_data, NULL, ob_time3));
  ObJsonDatetime j_timestamp(ob_time3, ObTimestampType);
  j_tree = &j_timestamp;
  ASSERT_EQ(OB_SUCCESS, j_tree->to_date(res));
  ASSERT_EQ(16540, res);
  // string
  MEMSET(buf, 0, sizeof(buf));
  strcpy(buf, "2015-4-15 11:12:00.1234567");
  ObJsonString j_str(buf, strlen(buf));
  j_tree = &j_str;
  ASSERT_EQ(OB_SUCCESS, j_tree->to_date(res));
  ASSERT_EQ(16540, res);

  // 2. test json bin
  // datetime
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_datetime,
      ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(OB_SUCCESS, j_bin->to_date(res));
  ASSERT_EQ(16540, res);
  // date
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_date,
      ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(OB_SUCCESS, j_bin->to_date(res));
  ASSERT_EQ(16540, res);
  // timestamp
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_timestamp,
      ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(OB_SUCCESS, j_bin->to_date(res));
  ASSERT_EQ(16540, res);
  // string
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_str,
      ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(OB_SUCCESS, j_bin->to_date(res));
  ASSERT_EQ(16540, res);
}

TEST_F(TestJsonBase, test_to_time)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObIJsonBase *j_bin = NULL;
  int64_t res = 0;

  // 1. test json tree
  // time
  ObTime ob_time1;
  ASSERT_EQ(OB_SUCCESS, ObTimeConverter::time_to_ob_time(43509 *
      static_cast<int64_t>(USECS_PER_SEC) + 1234, ob_time1));
  ObJsonDatetime j_time(ob_time1, ObTimeType);
  j_tree = &j_time;
  ASSERT_EQ(OB_SUCCESS, j_tree->to_time(res));
  ASSERT_EQ(43509 * static_cast<int64_t>(USECS_PER_SEC) + 1234, res);
  // string
  const int64_t MAX_BUF_SIZE = 256;
  char buf[MAX_BUF_SIZE] = {0};
  MEMSET(buf, 0, sizeof(buf));
  strcpy(buf, "12:05:09.001234");
  ObJsonString j_str(buf, strlen(buf));
  j_tree = &j_str;
  ASSERT_EQ(OB_SUCCESS, j_tree->to_time(res));
  ASSERT_EQ(43509 * static_cast<int64_t>(USECS_PER_SEC) + 1234, res);

  // 2. test json bin
  // time
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_time,
      ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(OB_SUCCESS, j_bin->to_time(res));
  ASSERT_EQ(43509 * static_cast<int64_t>(USECS_PER_SEC) + 1234, res);
  // string
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_str,
      ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(OB_SUCCESS, j_bin->to_time(res));
  ASSERT_EQ(43509 * static_cast<int64_t>(USECS_PER_SEC) + 1234, res);
}

TEST_F(TestJsonBase, test_to_bit)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObIJsonBase *j_tree = NULL;
  ObIJsonBase *j_bin = NULL;
  ObJsonInt j_int(123);
  ObJsonUint j_uint(123);
  ObJsonString j_str("123", strlen("123"));
  ObJsonBoolean j_bool(true);
  ObJsonDouble j_double(123.123);
  number::ObNumber num;
  const int64_t MAX_BUF_SIZE = 512;
  char buf[MAX_BUF_SIZE] = {0};
  int length = sprintf(buf, "%d", 123);
  ASSERT_EQ(OB_SUCCESS, num.from(buf, allocator));
  ObJsonDecimal j_dec(num);
  uint64_t res = 0;

  // 1. test json tree
  // int
  j_tree = &j_int;
  ASSERT_EQ(OB_SUCCESS, j_tree->to_bit(res));
  ASSERT_EQ(123, res);
  // uint
  j_tree = &j_uint;
  ASSERT_EQ(OB_SUCCESS, j_tree->to_bit(res));
  ASSERT_EQ(123, res);
  // string
  j_tree = &j_str;
  ASSERT_EQ(OB_SUCCESS, j_tree->to_bit(res));
  ASSERT_EQ(3224115, res);
  // bool
  j_tree = &j_bool;
  ASSERT_EQ(OB_SUCCESS, j_tree->to_bit(res));
  ASSERT_EQ(1, res);
  // double
  j_tree = &j_double;
  ASSERT_EQ(OB_SUCCESS, j_tree->to_bit(res));
  ASSERT_EQ(123, res);
  // number
  j_tree = &j_dec;
  ASSERT_EQ(OB_SUCCESS, j_tree->to_bit(res));
  ASSERT_EQ(123, res);

  // 2. test json bin
  // int
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_int,
      ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(OB_SUCCESS, j_bin->to_bit(res));
  ASSERT_EQ(123, res);
  // uint
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_uint,
      ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(OB_SUCCESS, j_bin->to_bit(res));
  ASSERT_EQ(123, res);
  // string
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_str,
      ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(OB_SUCCESS, j_bin->to_bit(res));
  ASSERT_EQ(3224115, res);
  // bool
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_bool,
      ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(OB_SUCCESS, j_bin->to_bit(res));
  ASSERT_EQ(1, res);
  // double
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_double,
      ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(OB_SUCCESS, j_bin->to_bit(res));
  ASSERT_EQ(123, res);
  // number
  ASSERT_EQ(OB_SUCCESS, ObJsonBaseFactory::transform(&allocator, &j_dec,
      ObJsonInType::JSON_BIN, j_bin));
  ASSERT_EQ(OB_SUCCESS, j_bin->to_bit(res));
  ASSERT_EQ(123, res);
}
/* ============================= test for class ObIJsonBase end ================================= */


} // namespace common
} // namespace oceanbase

int main(int argc, char** argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  // system("rm -f test_json_base.log");
  // OB_LOGGER.set_file_name("test_json_base.log");
  // OB_LOGGER.set_log_level("INFO");
  return RUN_ALL_TESTS();
}
