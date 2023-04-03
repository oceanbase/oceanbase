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

#include "sql/engine/expr/ob_expr_equal.h"
#include <gtest/gtest.h>
#include "ob_expr_test_utils.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;
class ObExprEqualTest: public ::testing::Test
{
  public:
    ObExprEqualTest();
    virtual ~ObExprEqualTest();
    virtual void SetUp();
    virtual void TearDown();
  private:
    // disallow copy
    ObExprEqualTest(const ObExprEqualTest &other);
    ObExprEqualTest& operator=(const ObExprEqualTest &other);
  protected:
    // data members
};

ObExprEqualTest::ObExprEqualTest()
{
}

ObExprEqualTest::~ObExprEqualTest()
{
}

void ObExprEqualTest::SetUp()
{
}

void ObExprEqualTest::TearDown()
{
}

#define T(t1, v1, t2, v2, res) COMPARE_EXPECT(ObExprEqual, &buf, calc_result2, t1, v1, t2, v2, res)
#define T_BIN(t1, v1, t2, v2, res) COMPARE_EXPECT_BIN(ObExprEqual, &buf, calc_result2, t1, v1, t2, v2, res)
#define T_GEN(t1, v1, t2, v2, res) COMPARE_EXPECT_GEN(ObExprEqual, &buf, calc_result2, t1, v1, t2, v2, res)

const int64_t MIN_VALUE = ObObj::MIN_OBJECT_VALUE;
const int64_t MAX_VALUE = ObObj::MAX_OBJECT_VALUE;

TEST_F(ObExprEqualTest, null_test)
{
  number::ObNumber nmb;
  ObMalloc buf;
  ASSERT_EQ(OB_SUCCESS, nmb.from("789.012", buf));
  T(null, , null, , MY_NULL);
  T(null, , tinyint, INT8_MIN, MY_NULL);
  T(null, , smallint, INT16_MAX, MY_NULL);
  T(null, , int32, INT32_MIN, MY_NULL);
  T(null, , int, INT64_MAX, MY_NULL);
  T(null, , utinyint, UINT8_MAX, MY_NULL);
  T(null, , usmallint, UINT16_MAX, MY_NULL);
  T(null, , uint32, UINT32_MAX, MY_NULL);
  T(null, , uint64, UINT64_MAX, MY_NULL);
  T(null, , float, -123.456F, MY_NULL);
  T(null, , ufloat, 123.456F, MY_NULL);
  T(null, , double, -789.012, MY_NULL);
  T(null, , udouble, 789.012, MY_NULL);
  T(null, , number, nmb, MY_NULL);
  T(null, , unumber, nmb, MY_NULL);
  T(null, , varchar, "varchar", MY_NULL);
  T(null, , char, "char", MY_NULL);
  T(null, , varbinary, "varbinary", MY_NULL);
  T(null, , binary, "binary", MY_NULL);
  T(null, , ext, MIN_VALUE, MY_NULL);
}

TEST_F(ObExprEqualTest, int_test)
{
  number::ObNumber nmb;
  ObMalloc buf;
  ASSERT_EQ(OB_SUCCESS, nmb.from("12345", buf));
  T(int, INT64_MIN, null, , MY_NULL);
  T(tinyint, INT8_MAX, smallint, INT8_MAX, MY_TRUE);
  T(tinyint, INT8_MIN, smallint, INT16_MIN, MY_FALSE);
  T(int32, INT32_MAX, int, INT32_MAX, MY_TRUE);
  T(int32, INT32_MIN, int, INT64_MIN, MY_FALSE);
  T(tinyint, INT8_MAX, usmallint, INT8_MAX, MY_TRUE);
  T(tinyint, INT8_MIN, usmallint, UINT16_MAX, MY_FALSE);
  T(int32, INT32_MAX, uint64, INT32_MAX, MY_TRUE);
  T(int32, INT32_MIN, uint64, UINT64_MAX, MY_FALSE);
  T(int32, 12345, float, 12345.0f, MY_TRUE);
  T(int32, -12345, float, -12345.0f, MY_TRUE);
  T(int32, 12345, double, 12345.0, MY_TRUE);
  T(int32, -12345, double, -12345.0, MY_TRUE);
  T(int, 12345, number, nmb, MY_TRUE);
  T(int, -23451, varchar, "-23451", MY_TRUE);
  T(int, 34512, char, "12345", MY_FALSE);
  T(int, -45123, varbinary, "-45123", MY_TRUE);
  T(int, 51234, binary, "12345", MY_FALSE);
  T(int32, INT32_MIN, ext, MAX_VALUE, MY_FALSE);
}

TEST_F(ObExprEqualTest, uint_test)
{
  number::ObNumber nmb;
  ObMalloc buf;
  ASSERT_EQ(OB_SUCCESS, nmb.from("12345", buf));
  T(uint64, UINT64_MAX, null, , MY_NULL);
  T(utinyint, INT8_MAX, smallint, INT8_MAX, MY_TRUE);
  T(utinyint, INT8_MIN, smallint, INT16_MIN, MY_FALSE);
  T(uint32, INT32_MAX, int, INT32_MAX, MY_TRUE);
  T(uint32, INT32_MIN, int, INT64_MIN, MY_FALSE);
  T(utinyint, INT8_MAX, usmallint, INT8_MAX, MY_TRUE);
  T(utinyint, INT8_MIN, usmallint, UINT16_MAX, MY_FALSE);
  T(uint32, INT32_MAX, uint64, INT32_MAX, MY_TRUE);
  T(uint32, INT32_MIN, uint64, UINT64_MAX, MY_FALSE);
  T(uint32, 12345, float, 12345.0f, MY_TRUE);
  T(uint32, 12345, float, -12345.0f, MY_FALSE);
  T(uint32, 23451, double, 23451.0, MY_TRUE);
  T(uint32, 23451, double, -23451.0, MY_FALSE);
  T(uint64, 12345, number, nmb, MY_TRUE);
  T(uint64, 23451, varchar, "23451", MY_TRUE);
  T(uint64, 34512, char, "-34512", MY_FALSE);
  T(uint64, 45123, varbinary, "45123", MY_TRUE);
  T(uint64, 51234, binary, "-51234", MY_FALSE);
  T(uint32, UINT32_MAX, ext, MIN_VALUE, MY_FALSE);
}

TEST_F(ObExprEqualTest, float_test)
{
  number::ObNumber nmb;
  ObMalloc buf;
  ASSERT_EQ(OB_SUCCESS, nmb.from("12345", buf));
  T(float, 12345.0f, null, , MY_NULL);
  T(float, 23451.0f, int32, 23451, MY_TRUE);
  T(float, 23451.1f, int32, 23451, MY_FALSE);
  T(float, 34512.0f, int, 34512, MY_TRUE);
  T(float, 34512.1f, int, 34512, MY_FALSE);
  T(float, 45123.0f, uint32, 45123, MY_TRUE);
  T(float, 45123.1f, uint32, 45123, MY_FALSE);
  T(float, 51234.0f, uint64, 51234, MY_TRUE);
  T(float, 51234.1f, uint64, 51234, MY_FALSE);
  T(float, 12345.0f, float, 12345.0f, MY_TRUE);
  T(float, 12345.1f, float, 12345.0f, MY_FALSE);
  T(float, 23451.0f, double, 23451.0, MY_TRUE);
  T(float, 23451.1f, double, 23451.0, MY_FALSE);
  T(float, 12345.0f, number, nmb, MY_TRUE);
  T(float, 12345.1f, number, nmb, MY_FALSE);
  //T(float, 23451.0f, varchar, "23451.0", MY_TRUE);
  T(float, 34512.0f, char, "-34512.1", MY_FALSE);
  //T(float, 45123.0f, varbinary, "45123.0", MY_TRUE);
  T(float, 51234.0f, binary, "-51234.1", MY_FALSE);
  T(float, 12345.0f, ext, MIN_VALUE, MY_FALSE);
}

TEST_F(ObExprEqualTest, double_test)
{
  number::ObNumber nmb;
  ObMalloc buf;
  ASSERT_EQ(OB_SUCCESS, nmb.from("12345", buf));
  T(double, 12345.0f, null, , MY_NULL);
  T(double, 23451.0f, int32, 23451, MY_TRUE);
  T(double, 23451.1f, int32, 23451, MY_FALSE);
  T(double, 34512.0f, int, 34512, MY_TRUE);
  T(double, 34512.1f, int, 34512, MY_FALSE);
  T(double, 45123.0f, uint32, 45123, MY_TRUE);
  T(double, 45123.1f, uint32, 45123, MY_FALSE);
  T(double, 51234.0f, uint64, 51234, MY_TRUE);
  T(double, 51234.1f, uint64, 51234, MY_FALSE);
  T(double, 12345.0f, float, 12345.0f, MY_TRUE);
  T(double, 12345.1f, float, 12345.0f, MY_FALSE);
  T(double, 23451.0f, double, 23451.0f, MY_TRUE);
  T(double, 23451.1f, double, 23451.0f, MY_FALSE);
  T(double, 12345.0f, number, nmb, MY_TRUE);
  T(double, 12345.1f, number, nmb, MY_FALSE);
  //T(double, 23451.0f, varchar, "23451.0", MY_TRUE);
  T(double, 34512.0f, char, "-34512.1", MY_FALSE);
  //T(double, 45123.0f, varbinary, "45123.0", MY_TRUE);
  T(double, 51234.0f, binary, "-51234.1", MY_FALSE);
  T(double, 12345.0f, ext, MIN_VALUE, MY_FALSE);
}

TEST_F(ObExprEqualTest, number_test)
{
  number::ObNumber nmb12345_0;
  number::ObNumber nmb12345_1;
  number::ObNumber nmb23451_0;
  number::ObNumber nmb23451_1;
  number::ObNumber nmb34512_0;
  number::ObNumber nmb34512_1;
  number::ObNumber nmb45123_0;
  number::ObNumber nmb45123_1;
  number::ObNumber nmb51234_0;
  number::ObNumber nmb51234_1;
  ObMalloc buf;
  ASSERT_EQ(OB_SUCCESS, nmb12345_0.from("12345.0", buf));
  ASSERT_EQ(OB_SUCCESS, nmb12345_1.from("12345.1", buf));
  ASSERT_EQ(OB_SUCCESS, nmb23451_0.from("23451.0", buf));
  ASSERT_EQ(OB_SUCCESS, nmb23451_1.from("23451.1", buf));
  ASSERT_EQ(OB_SUCCESS, nmb34512_0.from("34512.0", buf));
  ASSERT_EQ(OB_SUCCESS, nmb34512_1.from("34512.1", buf));
  ASSERT_EQ(OB_SUCCESS, nmb45123_0.from("45123.0", buf));
  ASSERT_EQ(OB_SUCCESS, nmb45123_1.from("45123.1", buf));
  ASSERT_EQ(OB_SUCCESS, nmb51234_0.from("51234.0", buf));
  ASSERT_EQ(OB_SUCCESS, nmb51234_1.from("51234.1", buf));
  T(number, nmb12345_0, null, , MY_NULL);
  T(number, nmb23451_0, int32, 23451, MY_TRUE);
  T(number, nmb23451_1, int32, 23451, MY_FALSE);
  T(number, nmb34512_0, int, 34512, MY_TRUE);
  T(number, nmb34512_1, int, 34512, MY_FALSE);
  T(number, nmb45123_0, uint32, 45123, MY_TRUE);
  T(number, nmb45123_1, uint32, 45123, MY_FALSE);
  T(number, nmb51234_0, uint64, 51234, MY_TRUE);
  T(number, nmb51234_1, uint64, 51234, MY_FALSE);
  T(number, nmb12345_0, float, 12345.0f, MY_TRUE);
  T(number, nmb12345_1, float, 12345.0f, MY_FALSE);
  T(number, nmb23451_0, double, 23451.0f, MY_TRUE);
  T(number, nmb23451_1, double, 23451.0f, MY_FALSE);
  T(number, nmb12345_0, number, nmb12345_0, MY_TRUE);
  T(number, nmb12345_1, number, nmb12345_0, MY_FALSE);
  //T(number, nmb23451_0, varchar, "23451.0", MY_TRUE);
  T(number, nmb34512_0, char, "-34512.1", MY_FALSE);
  //T(number, nmb45123_0, varbinary, "45123.0", MY_TRUE);
  T(number, nmb51234_0, binary, "-51234.1", MY_FALSE);
  T(number, nmb12345_0, ext, MIN_VALUE, MY_FALSE);
}

TEST_F(ObExprEqualTest, string_test)
{
  number::ObNumber nmb;
  ObMalloc buf;
  ASSERT_EQ(OB_SUCCESS, nmb.from("12345", buf));
  T(varchar, "12345.0", null, , MY_NULL);
  T(char, "23451.0", int32, 23451, MY_FALSE);
  T(char, "23451.1", int32, 23451, MY_FALSE);
  //T(varbinary, "34512.0", int, 34512, MY_TRUE);
  T(varbinary, "34512.1", int, 34512, MY_FALSE);
  //T(binary, "45123.0", uint32, 45123, MY_TRUE);
  T(binary, "45123.1", uint32, 45123, MY_FALSE);
  //T(varchar, "51234.0", uint64, 51234, MY_TRUE);
  T(varchar, "51234.1", uint64, 51234, MY_FALSE);
  //T(char, "12345.0", float, 12345.0, MY_TRUE);
  T(char, "12345.1", float, 12345.0, MY_FALSE);
  //T(varbinary, "23451.0", double, 23451.0, MY_TRUE);
  T(varbinary, "23451.1", double, 23451.0, MY_FALSE);
  //T(binary, "12345.0", number, nmb, MY_TRUE);
  T(binary, "12345.1", number, nmb, MY_FALSE);
  // string vs string, see collation_test.
  T(varchar, "12345.0", ext, MIN_VALUE, MY_FALSE);
}

TEST_F(ObExprEqualTest, collation_test)
{
  ObMalloc buf;
  T_BIN(varchar, "ß", varchar, "s", MY_FALSE);
  T_BIN(varchar, "", varchar, "s", MY_FALSE);
  T_BIN(varchar, "", varchar, "", MY_TRUE);
  T_BIN(varchar, "asads;[]da", varchar, "ASads;[]da", MY_FALSE);
  T_BIN(varchar, "ßßßßß", varchar, "ßßßßß", MY_TRUE);
  T_BIN(varchar, "aaaaa", varchar, "AAAAA", MY_FALSE);
  T_BIN(varchar, "bbaa", varchar, "Bbaa", MY_FALSE);
  T_BIN(varchar, "你好world", varchar, "你好world", MY_TRUE);
  T_BIN(varchar, "this is 阿里巴巴", varchar, "this is 阿里巴巴", MY_TRUE);
  T_GEN(varchar, "ß", varchar, "s", MY_TRUE);
  T_GEN(varchar, "", varchar, "s", MY_FALSE);
  T_GEN(varchar, "", varchar, "", MY_TRUE);
  T_GEN(varchar, "asads;[]da", varchar, "ASads;[]da", MY_TRUE);
  T_GEN(varchar, "ßßßßß", varchar, "ßßßßß", MY_TRUE);
  T_GEN(varchar, "aaaaa", varchar, "AAAAA", MY_TRUE);
  T_GEN(varchar, "bbaa", varchar, "Bbaa", MY_TRUE);
  T_GEN(varchar, "你好world", varchar, "你好WORLD", MY_TRUE);
  T_GEN(varchar, "this is 阿里巴巴", varchar, "THIS IS 阿里巴巴", MY_TRUE);
}

TEST_F(ObExprEqualTest, extend_test)
{
  number::ObNumber nmb;
  ObMalloc buf;
  ASSERT_EQ(OB_SUCCESS, nmb.from("789.012", buf));
  T(ext, MIN_VALUE, null, , MY_NULL);
  T(ext, MAX_VALUE, tinyint, INT8_MIN, MY_FALSE);
  T(ext, MIN_VALUE, smallint, INT16_MAX, MY_FALSE);
  T(ext, MAX_VALUE, int32, INT32_MIN, MY_FALSE);
  T(ext, MIN_VALUE, int, INT64_MAX, MY_FALSE);
  T(ext, MAX_VALUE, utinyint, UINT8_MAX, MY_FALSE);
  T(ext, MIN_VALUE, usmallint, UINT16_MAX, MY_FALSE);
  T(ext, MAX_VALUE, uint32, UINT32_MAX, MY_FALSE);
  T(ext, MIN_VALUE, uint64, UINT64_MAX, MY_FALSE);
  T(ext, MAX_VALUE, float, -123.456F, MY_FALSE);
  T(ext, MIN_VALUE, ufloat, 123.456F, MY_FALSE);
  T(ext, MAX_VALUE, double, -789.012, MY_FALSE);
  T(ext, MIN_VALUE, udouble, 789.012, MY_FALSE);
  T(ext, MAX_VALUE, number, nmb, MY_FALSE);
  T(ext, MIN_VALUE, unumber, nmb, MY_FALSE);
  T(ext, MAX_VALUE, varchar, "varchar", MY_FALSE);
  T(ext, MIN_VALUE, char, "char", MY_FALSE);
  T(ext, MAX_VALUE, varbinary, "varbinary", MY_FALSE);
  T(ext, MIN_VALUE, binary, "binary", MY_FALSE);
  T(ext, MAX_VALUE, ext, MAX_VALUE, MY_TRUE);
  T(ext, MAX_VALUE, ext, MIN_VALUE, MY_FALSE);
}

TEST_F(ObExprEqualTest, enum_test)
{
  number::ObNumber nmb;
  ObMalloc buf;
  ASSERT_EQ(OB_SUCCESS, nmb.from("12345", buf));

  T(enum, 1, null, , MY_NULL);
  T(enum, 2, smallint, 2, MY_TRUE);
  T(enum, 1, smallint, INT16_MIN, MY_FALSE);
  T(enum, INT32_MAX, int, INT32_MAX, MY_TRUE);
  T(enum, INT32_MIN, int, INT64_MIN, MY_FALSE);
  T(enum, INT8_MAX, usmallint, INT8_MAX, MY_TRUE);
  T(enum, INT8_MIN, usmallint, UINT16_MAX, MY_FALSE);
  T(enum, INT32_MAX, uint64, INT32_MAX, MY_TRUE);
  T(enum, INT32_MIN, uint64, UINT64_MAX, MY_FALSE);
  T(enum, 12345, float, 12345.0f, MY_TRUE);
  T(enum, 12345, float, -12345.0f, MY_FALSE);
  T(enum, 23451, double, 23451.0, MY_TRUE);
  T(enum, 23451, double, -23451.0, MY_FALSE);
  T(enum, 12345, number, nmb, MY_TRUE);
  T(enum, UINT32_MAX, ext, MIN_VALUE, MY_FALSE);
}

/*
TEST_F(ObExprEqualTest, int8_test)
{
  ObMalloc buf;
  T(tinyint, INT8_MIN, tinyint, INT8_MIN, MY_TRUE);
  T(tinyint, INT8_MIN, tinyint, 0, MY_FALSE);
  T(tinyint, INT8_MIN, tinyint, INT8_MAX, MY_FALSE);
  T(tinyint, 0, tinyint, INT8_MIN, MY_FALSE);
  T(tinyint, 0, tinyint, 0, MY_TRUE);
  T(tinyint, 0, tinyint, INT8_MAX, MY_FALSE);
  T(tinyint, INT8_MAX, tinyint, INT8_MIN, MY_FALSE);
  T(tinyint, INT8_MAX, tinyint, 0, MY_FALSE);
  T(tinyint, INT8_MAX, tinyint, INT8_MAX, MY_TRUE);
  T(tinyint, INT8_MIN, utinyint, 0, MY_FALSE);
  T(tinyint, INT8_MIN, utinyint, UINT8_MAX, MY_FALSE);
  T(tinyint, 0, utinyint, 0, MY_TRUE);
  T(tinyint, 0, utinyint, UINT8_MAX, MY_FALSE);
  T(tinyint, INT8_MAX, utinyint, 0, MY_FALSE);
  T(tinyint, INT8_MAX, utinyint, UINT8_MAX, MY_FALSE);
  T(tinyint, INT8_MIN, smallint, INT16_MIN, MY_FALSE);
  T(tinyint, INT8_MIN, smallint, 0, MY_FALSE);
  T(tinyint, INT8_MIN, smallint, INT16_MAX, MY_FALSE);
  T(tinyint, 0, smallint, INT16_MIN, MY_FALSE);
  T(tinyint, 0, smallint, 0, MY_TRUE);
  T(tinyint, 0, smallint, INT16_MAX, MY_FALSE);
  T(tinyint, INT8_MAX, smallint, INT16_MIN, MY_FALSE);
  T(tinyint, INT8_MAX, smallint, 0, MY_FALSE);
  T(tinyint, INT8_MAX, smallint, INT16_MAX, MY_FALSE);
  T(tinyint, INT8_MIN, usmallint, 0, MY_FALSE);
  T(tinyint, INT8_MIN, usmallint, UINT16_MAX, MY_FALSE);
  T(tinyint, 0, usmallint, 0, MY_TRUE);
  T(tinyint, 0, usmallint, UINT16_MAX, MY_FALSE);
  T(tinyint, INT8_MAX, usmallint, 0, MY_FALSE);
  T(tinyint, INT8_MAX, usmallint, UINT16_MAX, MY_FALSE);
  T(tinyint, INT8_MIN, int32, INT32_MIN, MY_FALSE);
  T(tinyint, INT8_MIN, int32, 0, MY_FALSE);
  T(tinyint, INT8_MIN, int32, INT32_MAX, MY_FALSE);
  T(tinyint, 0, int32, INT32_MIN, MY_FALSE);
  T(tinyint, 0, int32, 0, MY_TRUE);
  T(tinyint, 0, int32, INT32_MAX, MY_FALSE);
  T(tinyint, INT8_MAX, int32, INT32_MIN, MY_FALSE);
  T(tinyint, INT8_MAX, int32, 0, MY_FALSE);
  T(tinyint, INT8_MAX, int32, INT32_MAX, MY_FALSE);
  T(tinyint, INT8_MIN, uint32, 0, MY_FALSE);
  T(tinyint, INT8_MIN, uint32, UINT32_MAX, MY_FALSE);
  T(tinyint, 0, uint32, 0, MY_TRUE);
  T(tinyint, 0, uint32, UINT32_MAX, MY_FALSE);
  T(tinyint, INT8_MAX, uint32, 0, MY_FALSE);
  T(tinyint, INT8_MAX, uint32, UINT32_MAX, MY_FALSE);
  T(tinyint, INT8_MIN, int, INT64_MIN, MY_FALSE);
  T(tinyint, INT8_MIN, int, 0, MY_FALSE);
  T(tinyint, INT8_MIN, int, INT64_MAX, MY_FALSE);
  T(tinyint, 0, int, INT64_MIN, MY_FALSE);
  T(tinyint, 0, int, 0, MY_TRUE);
  T(tinyint, 0, int, INT64_MAX, MY_FALSE);
  T(tinyint, INT8_MAX, int, INT64_MIN, MY_FALSE);
  T(tinyint, INT8_MAX, int, 0, MY_FALSE);
  T(tinyint, INT8_MAX, int, INT64_MAX, MY_FALSE);
  T(tinyint, INT8_MIN, uint64, 0, MY_FALSE);
  T(tinyint, INT8_MIN, uint64, UINT64_MAX, MY_FALSE);
  T(tinyint, 0, uint64, 0, MY_TRUE);
  T(tinyint, 0, uint64, UINT64_MAX, MY_FALSE);
  T(tinyint, INT8_MAX, uint64, 0, MY_FALSE);
  T(tinyint, INT8_MAX, uint64, UINT64_MAX, MY_FALSE);
}

TEST_F(ObExprEqualTest, uint8_test)
{
  ObMalloc buf;
  T(utinyint, 0, tinyint, INT8_MIN, MY_FALSE);
  T(utinyint, 0, tinyint, 0, MY_TRUE);
  T(utinyint, 0, tinyint, INT8_MAX, MY_FALSE);
  T(utinyint, UINT8_MAX, tinyint, INT8_MIN, MY_FALSE);
  T(utinyint, UINT8_MAX, tinyint, 0, MY_FALSE);
  T(utinyint, UINT8_MAX, tinyint, INT8_MAX, MY_FALSE);
  T(utinyint, 0, utinyint, 0, MY_TRUE);
  T(utinyint, 0, utinyint, UINT8_MAX, MY_FALSE);
  T(utinyint, UINT8_MAX, utinyint, 0, MY_FALSE);
  T(utinyint, UINT8_MAX, utinyint, UINT8_MAX, MY_TRUE);
  T(utinyint, 0, smallint, INT16_MIN, MY_FALSE);
  T(utinyint, 0, smallint, 0, MY_TRUE);
  T(utinyint, 0, smallint, INT16_MAX, MY_FALSE);
  T(utinyint, UINT8_MAX, smallint, INT16_MIN, MY_FALSE);
  T(utinyint, UINT8_MAX, smallint, 0, MY_FALSE);
  T(utinyint, UINT8_MAX, smallint, INT16_MAX, MY_FALSE);
  T(utinyint, 0, usmallint, 0, MY_TRUE);
  T(utinyint, 0, usmallint, UINT16_MAX, MY_FALSE);
  T(utinyint, UINT8_MAX, usmallint, 0, MY_FALSE);
  T(utinyint, UINT8_MAX, usmallint, UINT16_MAX, MY_FALSE);
  T(utinyint, 0, int32, INT32_MIN, MY_FALSE);
  T(utinyint, 0, int32, 0, MY_TRUE);
  T(utinyint, 0, int32, INT32_MAX, MY_FALSE);
  T(utinyint, UINT8_MAX, int32, INT32_MIN, MY_FALSE);
  T(utinyint, UINT8_MAX, int32, 0, MY_FALSE);
  T(utinyint, UINT8_MAX, int32, INT32_MAX, MY_FALSE);
  T(utinyint, 0, uint32, 0, MY_TRUE);
  T(utinyint, 0, uint32, UINT32_MAX, MY_FALSE);
  T(utinyint, UINT8_MAX, uint32, 0, MY_FALSE);
  T(utinyint, UINT8_MAX, uint32, UINT32_MAX, MY_FALSE);
  T(utinyint, 0, int, INT64_MIN, MY_FALSE);
  T(utinyint, 0, int, 0, MY_TRUE);
  T(utinyint, 0, int, INT64_MAX, MY_FALSE);
  T(utinyint, UINT8_MAX, int, INT64_MIN, MY_FALSE);
  T(utinyint, UINT8_MAX, int, 0, MY_FALSE);
  T(utinyint, UINT8_MAX, int, INT64_MAX, MY_FALSE);
  T(utinyint, 0, uint64, 0, MY_TRUE);
  T(utinyint, 0, uint64, UINT64_MAX, MY_FALSE);
  T(utinyint, UINT8_MAX, uint64, 0, MY_FALSE);
  T(utinyint, UINT8_MAX, uint64, UINT64_MAX, MY_FALSE);
}

TEST_F(ObExprEqualTest, int16_test)
{
  ObMalloc buf;
  T(smallint, INT16_MIN, tinyint, INT8_MIN, MY_FALSE);
  T(smallint, INT16_MIN, tinyint, 0, MY_FALSE);
  T(smallint, INT16_MIN, tinyint, INT8_MAX, MY_FALSE);
  T(smallint, 0, tinyint, INT8_MIN, MY_FALSE);
  T(smallint, 0, tinyint, 0, MY_TRUE);
  T(smallint, 0, tinyint, INT8_MAX, MY_FALSE);
  T(smallint, INT16_MAX, tinyint, INT8_MIN, MY_FALSE);
  T(smallint, INT16_MAX, tinyint, 0, MY_FALSE);
  T(smallint, INT16_MAX, tinyint, INT8_MAX, MY_FALSE);
  T(smallint, INT16_MIN, utinyint, 0, MY_FALSE);
  T(smallint, INT16_MIN, utinyint, UINT8_MAX, MY_FALSE);
  T(smallint, 0, utinyint, 0, MY_TRUE);
  T(smallint, 0, utinyint, UINT8_MAX, MY_FALSE);
  T(smallint, INT16_MAX, utinyint, 0, MY_FALSE);
  T(smallint, INT16_MAX, utinyint, UINT8_MAX, MY_FALSE);
  T(smallint, INT16_MIN, smallint, INT16_MIN, MY_TRUE);
  T(smallint, INT16_MIN, smallint, 0, MY_FALSE);
  T(smallint, INT16_MIN, smallint, INT16_MAX, MY_FALSE);
  T(smallint, 0, smallint, INT16_MIN, MY_FALSE);
  T(smallint, 0, smallint, 0, MY_TRUE);
  T(smallint, 0, smallint, INT16_MAX, MY_FALSE);
  T(smallint, INT16_MAX, smallint, INT16_MIN, MY_FALSE);
  T(smallint, INT16_MAX, smallint, 0, MY_FALSE);
  T(smallint, INT16_MAX, smallint, INT16_MAX, MY_TRUE);
  T(smallint, INT16_MIN, usmallint, 0, MY_FALSE);
  T(smallint, INT16_MIN, usmallint, UINT16_MAX, MY_FALSE);
  T(smallint, 0, usmallint, 0, MY_TRUE);
  T(smallint, 0, usmallint, UINT16_MAX, MY_FALSE);
  T(smallint, INT16_MAX, usmallint, 0, MY_FALSE);
  T(smallint, INT16_MAX, usmallint, UINT16_MAX, MY_FALSE);
  T(smallint, INT16_MIN, int32, INT32_MIN, MY_FALSE);
  T(smallint, INT16_MIN, int32, 0, MY_FALSE);
  T(smallint, INT16_MIN, int32, INT32_MAX, MY_FALSE);
  T(smallint, 0, int32, INT32_MIN, MY_FALSE);
  T(smallint, 0, int32, 0, MY_TRUE);
  T(smallint, 0, int32, INT32_MAX, MY_FALSE);
  T(smallint, INT16_MAX, int32, INT32_MIN, MY_FALSE);
  T(smallint, INT16_MAX, int32, 0, MY_FALSE);
  T(smallint, INT16_MAX, int32, INT32_MAX, MY_FALSE);
  T(smallint, INT16_MIN, uint32, 0, MY_FALSE);
  T(smallint, INT16_MIN, uint32, UINT32_MAX, MY_FALSE);
  T(smallint, 0, uint32, 0, MY_TRUE);
  T(smallint, 0, uint32, UINT32_MAX, MY_FALSE);
  T(smallint, INT16_MAX, uint32, 0, MY_FALSE);
  T(smallint, INT16_MAX, uint32, UINT32_MAX, MY_FALSE);
  T(smallint, INT16_MIN, int, INT64_MIN, MY_FALSE);
  T(smallint, INT16_MIN, int, 0, MY_FALSE);
  T(smallint, INT16_MIN, int, INT64_MAX, MY_FALSE);
  T(smallint, 0, int, INT64_MIN, MY_FALSE);
  T(smallint, 0, int, 0, MY_TRUE);
  T(smallint, 0, int, INT64_MAX, MY_FALSE);
  T(smallint, INT16_MAX, int, INT64_MIN, MY_FALSE);
  T(smallint, INT16_MAX, int, 0, MY_FALSE);
  T(smallint, INT16_MAX, int, INT64_MAX, MY_FALSE);
  T(smallint, INT16_MIN, uint64, 0, MY_FALSE);
  T(smallint, INT16_MIN, uint64, UINT64_MAX, MY_FALSE);
  T(smallint, 0, uint64, 0, MY_TRUE);
  T(smallint, 0, uint64, UINT64_MAX, MY_FALSE);
  T(smallint, INT16_MAX, uint64, 0, MY_FALSE);
  T(smallint, INT16_MAX, uint64, UINT64_MAX, MY_FALSE);
}

TEST_F(ObExprEqualTest, uint16_test)
{
  ObMalloc buf;
  T(usmallint, 0, tinyint, INT8_MIN, MY_FALSE);
  T(usmallint, 0, tinyint, 0, MY_TRUE);
  T(usmallint, 0, tinyint, INT8_MAX, MY_FALSE);
  T(usmallint, UINT16_MAX, tinyint, INT8_MIN, MY_FALSE);
  T(usmallint, UINT16_MAX, tinyint, 0, MY_FALSE);
  T(usmallint, UINT16_MAX, tinyint, INT8_MAX, MY_FALSE);
  T(usmallint, 0, utinyint, 0, MY_TRUE);
  T(usmallint, 0, utinyint, UINT8_MAX, MY_FALSE);
  T(usmallint, UINT16_MAX, utinyint, 0, MY_FALSE);
  T(usmallint, UINT16_MAX, utinyint, UINT8_MAX, MY_FALSE);
  T(usmallint, 0, smallint, INT16_MIN, MY_FALSE);
  T(usmallint, 0, smallint, 0, MY_TRUE);
  T(usmallint, 0, smallint, INT16_MAX, MY_FALSE);
  T(usmallint, UINT16_MAX, smallint, INT16_MIN, MY_FALSE);
  T(usmallint, UINT16_MAX, smallint, 0, MY_FALSE);
  T(usmallint, UINT16_MAX, smallint, INT16_MAX, MY_FALSE);
  T(usmallint, 0, usmallint, 0, MY_TRUE);
  T(usmallint, 0, usmallint, UINT16_MAX, MY_FALSE);
  T(usmallint, UINT16_MAX, usmallint, 0, MY_FALSE);
  T(usmallint, UINT16_MAX, usmallint, UINT16_MAX, MY_TRUE);
  T(usmallint, 0, int32, INT32_MIN, MY_FALSE);
  T(usmallint, 0, int32, 0, MY_TRUE);
  T(usmallint, 0, int32, INT32_MAX, MY_FALSE);
  T(usmallint, UINT16_MAX, int32, INT32_MIN, MY_FALSE);
  T(usmallint, UINT16_MAX, int32, 0, MY_FALSE);
  T(usmallint, UINT16_MAX, int32, INT32_MAX, MY_FALSE);
  T(usmallint, 0, uint32, 0, MY_TRUE);
  T(usmallint, 0, uint32, UINT32_MAX, MY_FALSE);
  T(usmallint, UINT16_MAX, uint32, 0, MY_FALSE);
  T(usmallint, UINT16_MAX, uint32, UINT32_MAX, MY_FALSE);
  T(usmallint, 0, int, INT64_MIN, MY_FALSE);
  T(usmallint, 0, int, 0, MY_TRUE);
  T(usmallint, 0, int, INT64_MAX, MY_FALSE);
  T(usmallint, UINT16_MAX, int, INT64_MIN, MY_FALSE);
  T(usmallint, UINT16_MAX, int, 0, MY_FALSE);
  T(usmallint, UINT16_MAX, int, INT64_MAX, MY_FALSE);
  T(usmallint, 0, uint64, 0, MY_TRUE);
  T(usmallint, 0, uint64, UINT64_MAX, MY_FALSE);
  T(usmallint, UINT16_MAX, uint64, 0, MY_FALSE);
  T(usmallint, UINT16_MAX, uint64, UINT64_MAX, MY_FALSE);
}

TEST_F(ObExprEqualTest, int32_test)
{
  ObMalloc buf;
  T(int32, INT32_MIN, tinyint, INT8_MIN, MY_FALSE);
  T(int32, INT32_MIN, tinyint, 0, MY_FALSE);
  T(int32, INT32_MIN, tinyint, INT8_MAX, MY_FALSE);
  T(int32, 0, tinyint, INT8_MIN, MY_FALSE);
  T(int32, 0, tinyint, 0, MY_TRUE);
  T(int32, 0, tinyint, INT8_MAX, MY_FALSE);
  T(int32, INT32_MAX, tinyint, INT8_MIN, MY_FALSE);
  T(int32, INT32_MAX, tinyint, 0, MY_FALSE);
  T(int32, INT32_MAX, tinyint, INT8_MAX, MY_FALSE);
  T(int32, INT32_MIN, utinyint, 0, MY_FALSE);
  T(int32, INT32_MIN, utinyint, UINT8_MAX, MY_FALSE);
  T(int32, 0, utinyint, 0, MY_TRUE);
  T(int32, 0, utinyint, UINT8_MAX, MY_FALSE);
  T(int32, INT32_MAX, utinyint, 0, MY_FALSE);
  T(int32, INT32_MAX, utinyint, UINT8_MAX, MY_FALSE);
  T(int32, INT32_MIN, smallint, INT16_MIN, MY_FALSE);
  T(int32, INT32_MIN, smallint, 0, MY_FALSE);
  T(int32, INT32_MIN, smallint, INT16_MAX, MY_FALSE);
  T(int32, 0, smallint, INT16_MIN, MY_FALSE);
  T(int32, 0, smallint, 0, MY_TRUE);
  T(int32, 0, smallint, INT16_MAX, MY_FALSE);
  T(int32, INT32_MAX, smallint, INT16_MIN, MY_FALSE);
  T(int32, INT32_MAX, smallint, 0, MY_FALSE);
  T(int32, INT32_MAX, smallint, INT16_MAX, MY_FALSE);
  T(int32, INT32_MIN, usmallint, 0, MY_FALSE);
  T(int32, INT32_MIN, usmallint, UINT16_MAX, MY_FALSE);
  T(int32, 0, usmallint, 0, MY_TRUE);
  T(int32, 0, usmallint, UINT16_MAX, MY_FALSE);
  T(int32, INT32_MAX, usmallint, 0, MY_FALSE);
  T(int32, INT32_MAX, usmallint, UINT16_MAX, MY_FALSE);
  T(int32, INT32_MIN, int32, INT32_MIN, MY_TRUE);
  T(int32, INT32_MIN, int32, 0, MY_FALSE);
  T(int32, INT32_MIN, int32, INT32_MAX, MY_FALSE);
  T(int32, 0, int32, INT32_MIN, MY_FALSE);
  T(int32, 0, int32, 0, MY_TRUE);
  T(int32, 0, int32, INT32_MAX, MY_FALSE);
  T(int32, INT32_MAX, int32, INT32_MIN, MY_FALSE);
  T(int32, INT32_MAX, int32, 0, MY_FALSE);
  T(int32, INT32_MAX, int32, INT32_MAX, MY_TRUE);
  T(int32, INT32_MIN, uint32, 0, MY_FALSE);
  T(int32, INT32_MIN, uint32, UINT32_MAX, MY_FALSE);
  T(int32, 0, uint32, 0, MY_TRUE);
  T(int32, 0, uint32, UINT32_MAX, MY_FALSE);
  T(int32, INT32_MAX, uint32, 0, MY_FALSE);
  T(int32, INT32_MAX, uint32, UINT32_MAX, MY_FALSE);
  T(int32, INT32_MIN, int, INT64_MIN, MY_FALSE);
  T(int32, INT32_MIN, int, 0, MY_FALSE);
  T(int32, INT32_MIN, int, INT64_MAX, MY_FALSE);
  T(int32, 0, int, INT64_MIN, MY_FALSE);
  T(int32, 0, int, 0, MY_TRUE);
  T(int32, 0, int, INT64_MAX, MY_FALSE);
  T(int32, INT32_MAX, int, INT64_MIN, MY_FALSE);
  T(int32, INT32_MAX, int, 0, MY_FALSE);
  T(int32, INT32_MAX, int, INT64_MAX, MY_FALSE);
  T(int32, INT32_MIN, uint64, 0, MY_FALSE);
  T(int32, INT32_MIN, uint64, UINT64_MAX, MY_FALSE);
  T(int32, 0, uint64, 0, MY_TRUE);
  T(int32, 0, uint64, UINT64_MAX, MY_FALSE);
  T(int32, INT32_MAX, uint64, 0, MY_FALSE);
  T(int32, INT32_MAX, uint64, UINT64_MAX, MY_FALSE);
}

TEST_F(ObExprEqualTest, uint32_test)
{
  ObMalloc buf;
  T(uint32, 0, tinyint, INT8_MIN, MY_FALSE);
  T(uint32, 0, tinyint, 0, MY_TRUE);
  T(uint32, 0, tinyint, INT8_MAX, MY_FALSE);
  T(uint32, UINT32_MAX, tinyint, INT8_MIN, MY_FALSE);
  T(uint32, UINT32_MAX, tinyint, 0, MY_FALSE);
  T(uint32, UINT32_MAX, tinyint, INT8_MAX, MY_FALSE);
  T(uint32, 0, utinyint, 0, MY_TRUE);
  T(uint32, 0, utinyint, UINT8_MAX, MY_FALSE);
  T(uint32, UINT32_MAX, utinyint, 0, MY_FALSE);
  T(uint32, UINT32_MAX, utinyint, UINT8_MAX, MY_FALSE);
  T(uint32, 0, smallint, INT16_MIN, MY_FALSE);
  T(uint32, 0, smallint, 0, MY_TRUE);
  T(uint32, 0, smallint, INT16_MAX, MY_FALSE);
  T(uint32, UINT32_MAX, smallint, INT16_MIN, MY_FALSE);
  T(uint32, UINT32_MAX, smallint, 0, MY_FALSE);
  T(uint32, UINT32_MAX, smallint, INT16_MAX, MY_FALSE);
  T(uint32, 0, usmallint, 0, MY_TRUE);
  T(uint32, 0, usmallint, UINT16_MAX, MY_FALSE);
  T(uint32, UINT32_MAX, usmallint, 0, MY_FALSE);
  T(uint32, UINT32_MAX, usmallint, UINT16_MAX, MY_FALSE);
  T(uint32, 0, int32, INT32_MIN, MY_FALSE);
  T(uint32, 0, int32, 0, MY_TRUE);
  T(uint32, 0, int32, INT32_MAX, MY_FALSE);
  T(uint32, UINT32_MAX, int32, INT32_MIN, MY_FALSE);
  T(uint32, UINT32_MAX, int32, 0, MY_FALSE);
  T(uint32, UINT32_MAX, int32, INT32_MAX, MY_FALSE);
  T(uint32, 0, uint32, 0, MY_TRUE);
  T(uint32, 0, uint32, UINT32_MAX, MY_FALSE);
  T(uint32, UINT32_MAX, uint32, 0, MY_FALSE);
  T(uint32, UINT32_MAX, uint32, UINT32_MAX, MY_TRUE);
  T(uint32, 0, int, INT64_MIN, MY_FALSE);
  T(uint32, 0, int, 0, MY_TRUE);
  T(uint32, 0, int, INT64_MAX, MY_FALSE);
  T(uint32, UINT32_MAX, int, INT64_MIN, MY_FALSE);
  T(uint32, UINT32_MAX, int, 0, MY_FALSE);
  T(uint32, UINT32_MAX, int, INT64_MAX, MY_FALSE);
  T(uint32, 0, uint64, 0, MY_TRUE);
  T(uint32, 0, uint64, UINT64_MAX, MY_FALSE);
  T(uint32, UINT32_MAX, uint64, 0, MY_FALSE);
  T(uint32, UINT32_MAX, uint64, UINT64_MAX, MY_FALSE);
}

TEST_F(ObExprEqualTest, int64_test)
{
  ObMalloc buf;
  T(int, INT64_MIN, tinyint, INT8_MIN, MY_FALSE);
  T(int, INT64_MIN, tinyint, 0, MY_FALSE);
  T(int, INT64_MIN, tinyint, INT8_MAX, MY_FALSE);
  T(int, 0, tinyint, INT8_MIN, MY_FALSE);
  T(int, 0, tinyint, 0, MY_TRUE);
  T(int, 0, tinyint, INT8_MAX, MY_FALSE);
  T(int, INT64_MAX, tinyint, INT8_MIN, MY_FALSE);
  T(int, INT64_MAX, tinyint, 0, MY_FALSE);
  T(int, INT64_MAX, tinyint, INT8_MAX, MY_FALSE);
  T(int, INT64_MIN, utinyint, 0, MY_FALSE);
  T(int, INT64_MIN, utinyint, UINT8_MAX, MY_FALSE);
  T(int, 0, utinyint, 0, MY_TRUE);
  T(int, 0, utinyint, UINT8_MAX, MY_FALSE);
  T(int, INT64_MAX, utinyint, 0, MY_FALSE);
  T(int, INT64_MAX, utinyint, UINT8_MAX, MY_FALSE);
  T(int, INT64_MIN, smallint, INT16_MIN, MY_FALSE);
  T(int, INT64_MIN, smallint, 0, MY_FALSE);
  T(int, INT64_MIN, smallint, INT16_MAX, MY_FALSE);
  T(int, 0, smallint, INT16_MIN, MY_FALSE);
  T(int, 0, smallint, 0, MY_TRUE);
  T(int, 0, smallint, INT16_MAX, MY_FALSE);
  T(int, INT64_MAX, smallint, INT16_MIN, MY_FALSE);
  T(int, INT64_MAX, smallint, 0, MY_FALSE);
  T(int, INT64_MAX, smallint, INT16_MAX, MY_FALSE);
  T(int, INT64_MIN, usmallint, 0, MY_FALSE);
  T(int, INT64_MIN, usmallint, UINT16_MAX, MY_FALSE);
  T(int, 0, usmallint, 0, MY_TRUE);
  T(int, 0, usmallint, UINT16_MAX, MY_FALSE);
  T(int, INT64_MAX, usmallint, 0, MY_FALSE);
  T(int, INT64_MAX, usmallint, UINT16_MAX, MY_FALSE);
  T(int, INT64_MIN, int32, INT32_MIN, MY_FALSE);
  T(int, INT64_MIN, int32, 0, MY_FALSE);
  T(int, INT64_MIN, int32, INT32_MAX, MY_FALSE);
  T(int, 0, int32, INT32_MIN, MY_FALSE);
  T(int, 0, int32, 0, MY_TRUE);
  T(int, 0, int32, INT32_MAX, MY_FALSE);
  T(int, INT64_MAX, int32, INT32_MIN, MY_FALSE);
  T(int, INT64_MAX, int32, 0, MY_FALSE);
  T(int, INT64_MAX, int32, INT32_MAX, MY_FALSE);
  T(int, INT64_MIN, uint32, 0, MY_FALSE);
  T(int, INT64_MIN, uint32, UINT32_MAX, MY_FALSE);
  T(int, 0, uint32, 0, MY_TRUE);
  T(int, 0, uint32, UINT32_MAX, MY_FALSE);
  T(int, INT64_MAX, uint32, 0, MY_FALSE);
  T(int, INT64_MAX, uint32, UINT32_MAX, MY_FALSE);
  T(int, INT64_MIN, int, INT64_MIN, MY_TRUE);
  T(int, INT64_MIN, int, 0, MY_FALSE);
  T(int, INT64_MIN, int, INT64_MAX, MY_FALSE);
  T(int, 0, int, INT64_MIN, MY_FALSE);
  T(int, 0, int, 0, MY_TRUE);
  T(int, 0, int, INT64_MAX, MY_FALSE);
  T(int, INT64_MAX, int, INT64_MIN, MY_FALSE);
  T(int, INT64_MAX, int, 0, MY_FALSE);
  T(int, INT64_MAX, int, INT64_MAX, MY_TRUE);
  T(int, INT64_MIN, uint64, 0, MY_FALSE);
  T(int, INT64_MIN, uint64, UINT64_MAX, MY_FALSE);
  T(int, 0, uint64, 0, MY_TRUE);
  T(int, 0, uint64, UINT64_MAX, MY_FALSE);
  T(int, INT64_MAX, uint64, 0, MY_FALSE);
  T(int, INT64_MAX, uint64, UINT64_MAX, MY_FALSE);
}

TEST_F(ObExprEqualTest, uint64_test)
{
  ObMalloc buf;
  T(uint64, 0, tinyint, INT8_MIN, MY_FALSE);
  T(uint64, 0, tinyint, 0, MY_TRUE);
  T(uint64, 0, tinyint, INT8_MAX, MY_FALSE);
  T(uint64, UINT64_MAX, tinyint, INT8_MIN, MY_FALSE);
  T(uint64, UINT64_MAX, tinyint, 0, MY_FALSE);
  T(uint64, UINT64_MAX, tinyint, INT8_MAX, MY_FALSE);
  T(uint64, 0, utinyint, 0, MY_TRUE);
  T(uint64, 0, utinyint, UINT8_MAX, MY_FALSE);
  T(uint64, UINT64_MAX, utinyint, 0, MY_FALSE);
  T(uint64, UINT64_MAX, utinyint, UINT8_MAX, MY_FALSE);
  T(uint64, 0, smallint, INT16_MIN, MY_FALSE);
  T(uint64, 0, smallint, 0, MY_TRUE);
  T(uint64, 0, smallint, INT16_MAX, MY_FALSE);
  T(uint64, UINT64_MAX, smallint, INT16_MIN, MY_FALSE);
  T(uint64, UINT64_MAX, smallint, 0, MY_FALSE);
  T(uint64, UINT64_MAX, smallint, INT16_MAX, MY_FALSE);
  T(uint64, 0, usmallint, 0, MY_TRUE);
  T(uint64, 0, usmallint, UINT16_MAX, MY_FALSE);
  T(uint64, UINT64_MAX, usmallint, 0, MY_FALSE);
  T(uint64, UINT64_MAX, usmallint, UINT16_MAX, MY_FALSE);
  T(uint64, 0, int32, INT32_MIN, MY_FALSE);
  T(uint64, 0, int32, 0, MY_TRUE);
  T(uint64, 0, int32, INT32_MAX, MY_FALSE);
  T(uint64, UINT64_MAX, int32, INT32_MIN, MY_FALSE);
  T(uint64, UINT64_MAX, int32, 0, MY_FALSE);
  T(uint64, UINT64_MAX, int32, INT32_MAX, MY_FALSE);
  T(uint64, 0, uint32, 0, MY_TRUE);
  T(uint64, 0, uint32, UINT32_MAX, MY_FALSE);
  T(uint64, UINT64_MAX, uint32, 0, MY_FALSE);
  T(uint64, UINT64_MAX, uint32, UINT32_MAX, MY_FALSE);
  T(uint64, 0, int, INT64_MIN, MY_FALSE);
  T(uint64, 0, int, 0, MY_TRUE);
  T(uint64, 0, int, INT64_MAX, MY_FALSE);
  T(uint64, UINT64_MAX, int, INT64_MIN, MY_FALSE);
  T(uint64, UINT64_MAX, int, 0, MY_FALSE);
  T(uint64, UINT64_MAX, int, INT64_MAX, MY_FALSE);
  T(uint64, 0, uint64, 0, MY_TRUE);
  T(uint64, 0, uint64, UINT64_MAX, MY_FALSE);
  T(uint64, UINT64_MAX, uint64, 0, MY_FALSE);
  T(uint64, UINT64_MAX, uint64, UINT64_MAX, MY_TRUE);
}

TEST_F(ObExprEqualTest, varchar_test)
{
  ObMalloc buf;
  T_BIN(varchar, "ß", varchar, "s", MY_FALSE);
  T_BIN(varchar, "", varchar, "s", MY_FALSE);
  T_BIN(varchar, "", varchar, "", MY_TRUE);
  T_BIN(varchar, "asads;[]da", varchar, "ASads;[]da", MY_FALSE);
  T_BIN(varchar, "ßßßßß", varchar, "ßßßßß", MY_TRUE);
  T_BIN(varchar, "aaaaa", varchar, "AAAAA", MY_FALSE);
  T_BIN(varchar, "bbaa", varchar, "Bbaa", MY_FALSE);
  T_BIN(varchar, "你好world", varchar, "你好world", MY_TRUE);
  T_BIN(varchar, "this is 阿里巴巴", varchar, "this is 阿里巴巴", MY_TRUE);

  T_GEN(varchar, "ß", varchar, "s", MY_TRUE);
  T_GEN(varchar, "", varchar, "s", MY_FALSE);
  T_GEN(varchar, "", varchar, "", MY_TRUE);
  T_GEN(varchar, "asads;[]da", varchar, "ASads;[]da", MY_TRUE);
  T_GEN(varchar, "ßßßßß", varchar, "ßßßßß", MY_TRUE);
  T_GEN(varchar, "aaaaa", varchar, "AAAAA", MY_TRUE);
  T_GEN(varchar, "bbaa", varchar, "Bbaa", MY_TRUE);
  T_GEN(varchar, "你好world", varchar, "你好WORLD", MY_TRUE);
  T_GEN(varchar, "this is 阿里巴巴", varchar, "THIS IS 阿里巴巴", MY_TRUE);
}

TEST_F(ObExprEqualTest, basic_test)
{
  ObExprEqual eq;
  ASSERT_EQ(2, eq.get_param_num());
  ObMalloc buf;
  // special vs special
  T(null, , null, , MY_NULL);
  T(null, , min, , MY_NULL);
  T(min, , null, , MY_NULL);
  T(null, , max, , MY_NULL);
  T(max, , null, , MY_NULL);
  T(min, , max, , MY_FALSE);
  T(min, , min, , MY_TRUE);
  T(max, , min, , MY_FALSE);
  T(max, , max, , MY_TRUE);
  // int vs int
  T(int, 1, int, 2, MY_FALSE);
  T(int, 1, int, 1, MY_TRUE);
  T(int, 2, int, 1, MY_FALSE);
  // int vs special
  T(int, 1, null, , MY_NULL);
  T(null, , int, 1, MY_NULL);
  T(int, 1, min, , MY_FALSE);
  T(min, , int, 1, MY_FALSE);
  T(int, 1, max, , MY_FALSE);
  T(max, , int, 1, MY_FALSE);
  // varchar vs varchar
  T(varchar, "", varchar, "", MY_TRUE);
  T(varchar, "", varchar, "a", MY_FALSE);
  T(varchar, "a", varchar, "", MY_FALSE);
  T(varchar, "a", varchar, "a", MY_TRUE);
  T(varchar, "a", varchar, "b", MY_FALSE);
  T(varchar, "a", varchar, "aa", MY_FALSE);
  T(varchar, "aa", varchar, "aa", MY_TRUE);
  // varchar vs special
  T(varchar, "", null, , MY_NULL);
  T(null, , varchar, "", MY_NULL);
  T(varchar, "", min, , MY_FALSE);
  T(min, , varchar, "", MY_FALSE);
  T(varchar, "", max, , MY_FALSE);
  T(max, , varchar, "", MY_FALSE);
  // float vs float
  T(float, 1.0, float, 2.0, MY_FALSE);
  T(float, 1.0, float, 1.0, MY_TRUE);
  T(float, 2.0, float, 1.0, MY_FALSE);
  // float vs special
  T(float, 1.0, null, , MY_NULL);
  T(null, , float, 1.0, MY_NULL);
  T(float, 1.0, min, , MY_FALSE);
  T(min, , float, 1.0, MY_FALSE);
  T(float, 1.0, max, , MY_FALSE);
  T(max, , float, 1.0, MY_FALSE);
  // double vs double
  T(double, 1.0, double, 2.0, MY_FALSE);
  T(double, 1.0, double, 1.0, MY_TRUE);
  T(double, 2.0, double, 1.0, MY_FALSE);
  // double vs special
  T(double, 1.0, null, , MY_NULL);
  T(null, , double, 1.0, MY_NULL);
  T(double, 1.0, min, , MY_FALSE);
  T(min, , double, 1.0, MY_FALSE);
  T(double, 1.0, max, , MY_FALSE);
  T(max, , double, 1.0, MY_FALSE);
  // precise_datetime vs precise_datetime
//  T(precise_datetime, 1, precise_datetime, 2, MY_FALSE);
//  T(precise_datetime, 1, precise_datetime, 1, MY_TRUE);
//  T(precise_datetime, 2, precise_datetime, 1, MY_FALSE);
  // precise_datetime vs special
//  T(precise_datetime, 1, null, , MY_NULL);
//  T(null, , precise_datetime, 1, MY_NULL);
//  T(precise_datetime, 1, min, , MY_FALSE);
//  T(min, , precise_datetime, 1, MY_FALSE);
//  T(precise_datetime, 1, max, , MY_FALSE);
//  T(max, , precise_datetime, 1, MY_FALSE);
  // ctime vs ctime
//  T(ctime, 1, ctime, 2, MY_FALSE);
//  T(ctime, 1, ctime, 1, MY_TRUE);
//  T(ctime, 2, ctime, 1, MY_FALSE);
  // ctime vs special
//  T(ctime, 1, null, , MY_NULL);
//  T(null, , ctime, 1, MY_NULL);
//  T(ctime, 1, min, , MY_FALSE);
//  T(min, , ctime, 1, MY_FALSE);
//  T(ctime, 1, max, , MY_FALSE);
//  T(max, , ctime, 1, MY_FALSE);
  // mtime vs mtime
//  T(mtime, 1, mtime, 2, MY_FALSE);
//  T(mtime, 1, mtime, 1, MY_TRUE);
//  T(mtime, 2, mtime, 1, MY_FALSE);
  // mtime vs special
//  T(mtime, 1, null, , MY_NULL);
//  T(null, , mtime, 1, MY_NULL);
//  T(mtime, 1, min, , MY_FALSE);
//  T(min, , mtime, 1, MY_FALSE);
//  T(mtime, 1, max, , MY_FALSE);
//  T(max, , mtime, 1, MY_FALSE);
  // bool vs bool
  T(bool, true, bool, true, MY_TRUE);
  T(bool, true, bool, false, MY_FALSE);
  T(bool, false, bool, false, MY_TRUE);
  T(bool, false, bool, true, MY_FALSE);
  // bool vs special
  T(bool, true, null, , MY_NULL);
  T(null, , bool, true, MY_NULL);
  T(bool, true, min, , MY_FALSE);
  T(min, , bool, true, MY_FALSE);
  T(bool, true, max, , MY_FALSE);
  T(max, , bool, true, MY_FALSE);
}

TEST_F(ObExprEqualTest, promotion_test)
{
  ObMalloc buf;
  // int vs float
  T(int, 1, float, 2.0, MY_FALSE);
  T(int, 1, float, 1.0, MY_TRUE);
  T(int, 1, float, 0.0, MY_FALSE);
  T(float, 1.0, int, 2, MY_FALSE);
  T(float, 1.0, int, 1, MY_TRUE);
  T(float, 1.0, int, 0, MY_FALSE);
  // int vs double
  T(int, 1, double, 2.0, MY_FALSE);
  T(int, 1, double, 1.0, MY_TRUE);
  T(int, 1, double, 0.0, MY_FALSE);
  T(double, 1.0, int, 2, MY_FALSE);
  T(double, 1.0, int, 1, MY_TRUE);
  T(double, 1.0, int, 0, MY_FALSE);
  // int vs pdatetime
//  T(int, 1, precise_datetime, 2, MY_ERROR);
//  T(int, 1, precise_datetime, 1, MY_ERROR);
//  T(int, 1, precise_datetime, 0, MY_ERROR);
//  T(precise_datetime, 1, int, 2, MY_ERROR);
//  T(precise_datetime, 1, int, 1, MY_ERROR);
//  T(precise_datetime, 1, int, 0, MY_ERROR);
  // int vs varchar
  T(int, 1, varchar, "2", MY_FALSE);
  T(int, 1, varchar, "1", MY_TRUE);
  T(int, 1, varchar, "0", MY_FALSE);
  T(varchar, "1", int, 2, MY_FALSE);
  T(varchar, "1", int, 1, MY_TRUE);
  T(varchar, "1", int, 0, MY_FALSE);
  //T(int, 1, varchar, "2ab", MY_FALSE);
  //T(int, 1, varchar, "1ab", MY_TRUE);
  //T(int, 1, varchar, "0ab", MY_FALSE);
  //T(int, 1, varchar, "ab", MY_FALSE);
  //T(int, 0, varchar, "ab", MY_TRUE);
  //T(int, -1, varchar, "ab", MY_FALSE);
  // int vs bool
  T(int, 1, bool, false, MY_ERROR);
  T(int, -1, bool, false, MY_ERROR);
  T(int, 0, bool, false, MY_ERROR);
  T(int, 1, bool, true, MY_ERROR);
  T(int, -1, bool, true, MY_ERROR);
  T(int, 0, bool, true, MY_ERROR);
  // float vs double
  T(float, 1.0, double, 2.0, MY_FALSE);
  T(float, 1.0, double, 1.0, MY_TRUE);
  T(float, 2.0, double, 1.0, MY_FALSE);
  T(double, 1.0, float, 2.0, MY_FALSE);
  T(double, 1.0, float, 1.0, MY_TRUE);
  T(double, 2.0, float, 1.0, MY_FALSE);
  // float vs pdatetime
//  T(float, 1.0, precise_datetime, 2, MY_ERROR);
//  T(float, 1.0, precise_datetime, 1, MY_ERROR);
//  T(float, 1.0, precise_datetime, 0, MY_ERROR);
//  T(precise_datetime, 1, float, 2.0, MY_ERROR);
//  T(precise_datetime, 1, float, 1.0, MY_ERROR);
//  T(precise_datetime, 1, float, 0.0, MY_ERROR);
  // float vs varchar
  T(float, 1.0, varchar, "2.0", MY_FALSE);
  T(float, 1.0, varchar, "1.0", MY_TRUE);
  T(float, 1.0, varchar, "0.0", MY_FALSE);
  T(varchar, "1.0", float, 2.0, MY_FALSE);
  T(varchar, "1.0", float, 1.0, MY_TRUE);
  T(varchar, "1.0", float, 0.0, MY_FALSE);
  //T(float, 1.0, varchar, "2.0ab", MY_FALSE);
  //T(float, 1.0, varchar, "1.0ab", MY_TRUE);
  //T(float, 1.0, varchar, "0.0ab", MY_FALSE);
  //T(float, 1.0, varchar, "ab", MY_FALSE);
  //T(float, 0.0, varchar, "ab", MY_TRUE);
  //T(float, -1.0, varchar, "ab", MY_FALSE);
  // float vs bool
  T(float, 1.0, bool, false, MY_ERROR);
  T(float, -1.0, bool, false, MY_ERROR);
  T(float, 0.0, bool, false, MY_ERROR);
  T(float, 1.0, bool, true, MY_ERROR);
  T(float, -1.0, bool, true, MY_ERROR);
  T(float, 0.0, bool, true, MY_ERROR);
  // double vs pdatetime
//  T(double, 1.0, precise_datetime, 2, MY_ERROR);
//  T(double, 1.0, precise_datetime, 1, MY_ERROR);
//  T(double, 1.0, precise_datetime, 0, MY_ERROR);
//  T(precise_datetime, 1, double, 2.0, MY_ERROR);
//  T(precise_datetime, 1, double, 1.0, MY_ERROR);
//  T(precise_datetime, 1, double, 0.0, MY_ERROR);
  // double vs varchar
  T(double, 1.0, varchar, "2.0", MY_FALSE);
  T(double, 1.0, varchar, "1.0", MY_TRUE);
  T(double, 1.0, varchar, "0.0", MY_FALSE);
  T(varchar, "1.0", double, 2.0, MY_FALSE);
  T(varchar, "1.0", double, 1.0, MY_TRUE);
  T(varchar, "1.0", double, 0.0, MY_FALSE);
  //T(double, 1.0, varchar, "2.0ab", MY_FALSE);
  //T(double, 1.0, varchar, "1.0ab", MY_TRUE);
  //T(double, 1.0, varchar, "0.0ab", MY_FALSE);
  //T(double, 1.0, varchar, "ab", MY_FALSE);
  //T(double, 0.0, varchar, "ab", MY_TRUE);
  //T(double, -1.0, varchar, "ab", MY_FALSE);
  // double vs bool
  T(double, 1.0, bool, false, MY_ERROR);
  T(double, -1.0, bool, false, MY_ERROR);
  T(double, 0.0, bool, false, MY_ERROR);
  T(double, 1.0, bool, true, MY_ERROR);
  T(double, -1.0, bool, true, MY_ERROR);
  T(double, 0.0, bool, true, MY_ERROR);
  // pdatetime vs varchar
//  T(pdatetime, "2013-12-16 19:26:30", varchar, "2013-12-16 19:26:30.1", MY_FALSE);
//  T(pdatetime, "2013-12-16 19:26:30", varchar, "2013-12-16 19:26:30", MY_TRUE);
//  T(pdatetime, "2013-12-16 19:26:30", varchar, "2013-12-16 19:26:29", MY_FALSE);
//  T(varchar, "2013-12-16 19:26:30", pdatetime, "2013-12-16 19:26:30.1", MY_FALSE);
//  T(varchar, "2013-12-16 19:26:30", pdatetime, "2013-12-16 19:26:30", MY_TRUE);
//  T(varchar, "2013-12-16 19:26:30", pdatetime, "2013-12-16 19:26:29", MY_FALSE);
  // pdatetime vs bool
//  T(precise_datetime, 1, bool, false, MY_ERROR);
//  T(precise_datetime, -1, bool, false, MY_ERROR);
//  T(precise_datetime, 0, bool, false, MY_ERROR);
//  T(precise_datetime, 1, bool, true, MY_ERROR);
//  T(precise_datetime, -1, bool, true, MY_ERROR);
//  T(precise_datetime, 0, bool, true, MY_ERROR);
  // varchar vs bool
  T(varchar, "true", bool, false, MY_FALSE);
  T(varchar, "false", bool, false, MY_TRUE);
  T(varchar, "kaka", bool, false, MY_ERROR);
  T(varchar, "true", bool, true, MY_TRUE);
  T(varchar, "false", bool, true, MY_FALSE);
  T(varchar, "kaka", bool, true, MY_ERROR);
  T(bool, false, varchar, "true", MY_FALSE);
  T(bool, false, varchar, "false", MY_TRUE);
  T(bool, false, varchar, "kaka", MY_ERROR);
  T(bool, true, varchar, "true", MY_TRUE);
  T(bool, true, varchar, "false", MY_FALSE);
  T(bool, true, varchar, "kaka", MY_ERROR);
}
*/

#define R(t1, v1, t2, v2, res) ROW1_COMPARE_EXPECT(ObExprEqual, &buf, calc, t1, v1, t2, v2, res)
/*
TEST_F(ObExprEqualTest, row1_basic_test)
{
  ObExprEqual eq;
  ASSERT_EQ(2, eq.get_param_num());
  ObMalloc buf;
  // special vs special
  R(null, , null, , MY_TRUE);
  R(null, , min, , MY_FALSE);
  R(min, , null, , MY_FALSE);
  R(null, , max, , MY_FALSE);
  R(max, , null, , MY_FALSE);
  R(min, , max, , MY_FALSE);
  R(min, , min, , MY_TRUE);
  R(max, , min, , MY_FALSE);
  R(max, , max, , MY_TRUE);
  // int vs int
  R(int, 1, int, 2, MY_FALSE);
  R(int, 1, int, 1, MY_TRUE);
  R(int, 2, int, 1, MY_FALSE);
  // int vs special
  R(int, 1, null, , MY_FALSE);
  R(null, , int, 1, MY_FALSE);
  R(int, 1, min, , MY_FALSE);
  R(min, , int, 1, MY_FALSE);
  R(int, 1, max, , MY_FALSE);
  R(max, , int, 1, MY_FALSE);
  // varchar vs varchar
  R(varchar, "", varchar, "", MY_TRUE);
  R(varchar, "", varchar, "a", MY_FALSE);
  R(varchar, "a", varchar, "", MY_FALSE);
  R(varchar, "a", varchar, "a", MY_TRUE);
  R(varchar, "a", varchar, "b", MY_FALSE);
  R(varchar, "a", varchar, "aa", MY_FALSE);
  R(varchar, "aa", varchar, "aa", MY_TRUE);
  // varchar vs special
  R(varchar, "", null, , MY_FALSE);
  R(null, , varchar, "", MY_FALSE);
  R(varchar, "", min, , MY_FALSE);
  R(min, , varchar, "", MY_FALSE);
  R(varchar, "", max, , MY_FALSE);
  R(max, , varchar, "", MY_FALSE);
  // float vs float
  R(float, 1.0, float, 2.0, MY_FALSE);
  R(float, 1.0, float, 1.0, MY_TRUE);
  R(float, 2.0, float, 1.0, MY_FALSE);
  // float vs special
  R(float, 1.0, null, , MY_FALSE);
  R(null, , float, 1.0, MY_FALSE);
  R(float, 1.0, min, , MY_FALSE);
  R(min, , float, 1.0, MY_FALSE);
  R(float, 1.0, max, , MY_FALSE);
  R(max, , float, 1.0, MY_FALSE);
  // double vs double
  R(double, 1.0, double, 2.0, MY_FALSE);
  R(double, 1.0, double, 1.0, MY_TRUE);
  R(double, 2.0, double, 1.0, MY_FALSE);
  // double vs special
  R(double, 1.0, null, , MY_FALSE);
  R(null, , double, 1.0, MY_FALSE);
  R(double, 1.0, min, , MY_FALSE);
  R(min, , double, 1.0, MY_FALSE);
  R(double, 1.0, max, , MY_FALSE);
  R(max, , double, 1.0, MY_FALSE);
  // precise_datetime vs precise_datetime
//  ROW1_TEST(precise_datetime, 1, precise_datetime, 2, MY_FALSE);
//  ROW1_TEST(precise_datetime, 1, precise_datetime, 1, MY_TRUE);
//  ROW1_TEST(precise_datetime, 2, precise_datetime, 1, MY_FALSE);
  // precise_datetime vs special
//  ROW1_TEST(precise_datetime, 1, null, , MY_FALSE);
//  ROW1_TEST(null, , precise_datetime, 1, MY_FALSE);
//  ROW1_TEST(precise_datetime, 1, min, , MY_FALSE);
//  ROW1_TEST(min, , precise_datetime, 1, MY_FALSE);
//  ROW1_TEST(precise_datetime, 1, max, , MY_FALSE);
//  ROW1_TEST(max, , precise_datetime, 1, MY_FALSE);
  // ctime vs ctime
//  ROW1_TEST(ctime, 1, ctime, 2, MY_FALSE);
//  ROW1_TEST(ctime, 1, ctime, 1, MY_TRUE);
//  ROW1_TEST(ctime, 2, ctime, 1, MY_FALSE);
  // ctime vs special
//  ROW1_TEST(ctime, 1, null, , MY_FALSE);
//  ROW1_TEST(null, , ctime, 1, MY_FALSE);
//  ROW1_TEST(ctime, 1, min, , MY_FALSE);
//  ROW1_TEST(min, , ctime, 1, MY_FALSE);
//  ROW1_TEST(ctime, 1, max, , MY_FALSE);
//  ROW1_TEST(max, , ctime, 1, MY_FALSE);
  // mtime vs mtime
//  ROW1_TEST(mtime, 1, mtime, 2, MY_FALSE);
//  ROW1_TEST(mtime, 1, mtime, 1, MY_TRUE);
//  ROW1_TEST(mtime, 2, mtime, 1, MY_FALSE);
  // mtime vs special
//  ROW1_TEST(mtime, 1, null, , MY_FALSE);
//  ROW1_TEST(null, , mtime, 1, MY_FALSE);
//  ROW1_TEST(mtime, 1, min, , MY_FALSE);
//  ROW1_TEST(min, , mtime, 1, MY_FALSE);
//  ROW1_TEST(mtime, 1, max, , MY_FALSE);
//  ROW1_TEST(max, , mtime, 1, MY_FALSE);
  // bool vs bool
  R(bool, true, bool, true, MY_TRUE);
  R(bool, true, bool, false, MY_FALSE);
  R(bool, false, bool, false, MY_TRUE);
  R(bool, false, bool, true, MY_FALSE);
  // bool vs special
  R(bool, true, null, , MY_FALSE);
  R(null, , bool, true, MY_FALSE);
  R(bool, true, min, , MY_FALSE);
  R(min, , bool, true, MY_FALSE);
  R(bool, true, max, , MY_FALSE);
  R(max, , bool, true, MY_FALSE);
}
*/

#define W(t11, v11, t12, v12, t21, v21, t22, v22, res) \
  ROW2_COMPARE_EXPECT(ObExprEqual, &buf, calc, t11, v11, t12, v12, t21, v21, t22, v22, res)
/*
TEST_F(ObExprEqualTest, row2_basic_test)
{
  ObMalloc buf;
  // (int, int) vs (int int)
  W(int, 0, int, 0, int, 0, int, 0, MY_TRUE);
  W(int, 0, int, 0, int, 0, int, 1, MY_FALSE);
  W(int, 0, int, 0, int, 1, int, 0, MY_FALSE);
  W(int, 0, int, 0, int, 1, int, 1, MY_FALSE);
  W(int, 0, int, 1, int, 0, int, 0, MY_FALSE);
  W(int, 0, int, 1, int, 0, int, 1, MY_TRUE);
  W(int, 0, int, 1, int, 1, int, 0, MY_FALSE);
  W(int, 0, int, 1, int, 1, int, 1, MY_FALSE);
  W(int, 1, int, 0, int, 0, int, 0, MY_FALSE);
  W(int, 1, int, 0, int, 0, int, 1, MY_FALSE);
  W(int, 1, int, 0, int, 1, int, 0, MY_TRUE);
  W(int, 1, int, 0, int, 1, int, 1, MY_FALSE);
  W(int, 1, int, 1, int, 0, int, 0, MY_FALSE);
  W(int, 1, int, 1, int, 0, int, 1, MY_FALSE);
  W(int, 1, int, 1, int, 1, int, 0, MY_FALSE);
  W(int, 1, int, 1, int, 1, int, 1, MY_TRUE);
  // (int, varchar) vs (varchar, double)
  W(int, 0, varchar, "0", varchar, "0", double, 0, MY_TRUE);
  W(int, 0, varchar, "0", varchar, "0", double, 1, MY_FALSE);
  W(int, 0, varchar, "0", varchar, "1", double, 0, MY_FALSE);
  W(int, 0, varchar, "0", varchar, "1", double, 1, MY_FALSE);
  W(int, 0, varchar, "1", varchar, "0", double, 0, MY_FALSE);
  W(int, 0, varchar, "1", varchar, "0", double, 1, MY_TRUE);
  W(int, 0, varchar, "1", varchar, "1", double, 0, MY_FALSE);
  W(int, 0, varchar, "1", varchar, "1", double, 1, MY_FALSE);
  W(int, 1, varchar, "0", varchar, "0", double, 0, MY_FALSE);
  W(int, 1, varchar, "0", varchar, "0", double, 1, MY_FALSE);
  W(int, 1, varchar, "0", varchar, "1", double, 0, MY_TRUE);
  W(int, 1, varchar, "0", varchar, "1", double, 1, MY_FALSE);
  W(int, 1, varchar, "1", varchar, "0", double, 0, MY_FALSE);
  W(int, 1, varchar, "1", varchar, "0", double, 1, MY_FALSE);
  W(int, 1, varchar, "1", varchar, "1", double, 0, MY_FALSE);
  W(int, 1, varchar, "1", varchar, "1", double, 1, MY_TRUE);
  // special values
  W(min, , min, , min, , min, , MY_TRUE);
  W(min, , min, , min, , null, , MY_FALSE);
  W(min, , min, , min, , int,  0, MY_FALSE);
  W(min, , min, , min, , max, , MY_FALSE);
  W(min, , min, , null, , min, , MY_FALSE);
  W(min, , min, , null, , null, , MY_FALSE);
  W(min, , min, , null, , int,  0, MY_FALSE);
  W(min, , min, , null, , max, , MY_FALSE);
  W(min, , min, , int,  0, min, , MY_FALSE);
  W(min, , min, , int,  0, null, , MY_FALSE);
  W(min, , min, , int,  0, int,  0, MY_FALSE);
  W(min, , min, , int,  0, max, , MY_FALSE);
  W(min, , min, , max, , min, , MY_FALSE);
  W(min, , min, , max, , null, , MY_FALSE);
  W(min, , min, , max, , int,  0, MY_FALSE);
  W(min, , min, , max, , max, , MY_FALSE);

  W(min, , null, , min, , min, , MY_FALSE);
  W(min, , null, , min, , null, , MY_TRUE);
  W(min, , null, , min, , int,  0, MY_FALSE);
  W(min, , null, , min, , max, , MY_FALSE);
  W(min, , null, , null, , min, , MY_FALSE);
  W(min, , null, , null, , null, , MY_FALSE);
  W(min, , null, , null, , int,  0, MY_FALSE);
  W(min, , null, , null, , max, , MY_FALSE);
  W(min, , null, , int,  0, min, , MY_FALSE);
  W(min, , null, , int,  0, null, , MY_FALSE);
  W(min, , null, , int,  0, int,  0, MY_FALSE);
  W(min, , null, , int,  0, max, , MY_FALSE);
  W(min, , null, , max, , min, , MY_FALSE);
  W(min, , null, , max, , null, , MY_FALSE);
  W(min, , null, , max, , int,  0, MY_FALSE);
  W(min, , null, , max, , max, , MY_FALSE);

  W(min, , int, 0, min, , min, , MY_FALSE);
  W(min, , int, 0, min, , null, , MY_FALSE);
  W(min, , int, 0, min, , int,  0, MY_TRUE);
  W(min, , int, 0, min, , max, , MY_FALSE);
  W(min, , int, 0, null, , min, , MY_FALSE);
  W(min, , int, 0, null, , null, , MY_FALSE);
  W(min, , int, 0, null, , int,  0, MY_FALSE);
  W(min, , int, 0, null, , max, , MY_FALSE);
  W(min, , int, 0, int,  0, min, , MY_FALSE);
  W(min, , int, 0, int,  0, null, , MY_FALSE);
  W(min, , int, 0, int,  0, int,  0, MY_FALSE);
  W(min, , int, 0, int,  0, max, , MY_FALSE);
  W(min, , int, 0, max, , min, , MY_FALSE);
  W(min, , int, 0, max, , null, , MY_FALSE);
  W(min, , int, 0, max, , int,  0, MY_FALSE);
  W(min, , int, 0, max, , max, , MY_FALSE);

  W(min, , max, , min, , min, , MY_FALSE);
  W(min, , max, , min, , null, , MY_FALSE);
  W(min, , max, , min, , int,  0, MY_FALSE);
  W(min, , max, , min, , max, , MY_TRUE);
  W(min, , max, , null, , min, , MY_FALSE);
  W(min, , max, , null, , null, , MY_FALSE);
  W(min, , max, , null, , int,  0, MY_FALSE);
  W(min, , max, , null, , max, , MY_FALSE);
  W(min, , max, , int,  0, min, , MY_FALSE);
  W(min, , max, , int,  0, null, , MY_FALSE);
  W(min, , max, , int,  0, int,  0, MY_FALSE);
  W(min, , max, , int,  0, max, , MY_FALSE);
  W(min, , max, , max, , min, , MY_FALSE);
  W(min, , max, , max, , null, , MY_FALSE);
  W(min, , max, , max, , int,  0, MY_FALSE);
  W(min, , max, , max, , max, , MY_FALSE);


  W(null, , min, , min, , min, , MY_FALSE);
  W(null, , min, , min, , null, , MY_FALSE);
  W(null, , min, , min, , int,  0, MY_FALSE);
  W(null, , min, , min, , max, , MY_FALSE);
  W(null, , min, , null, , min, , MY_TRUE);
  W(null, , min, , null, , null, , MY_FALSE);
  W(null, , min, , null, , int,  0, MY_FALSE);
  W(null, , min, , null, , max, , MY_FALSE);
  W(null, , min, , int,  0, min, , MY_FALSE);
  W(null, , min, , int,  0, null, , MY_FALSE);
  W(null, , min, , int,  0, int,  0, MY_FALSE);
  W(null, , min, , int,  0, max, , MY_FALSE);
  W(null, , min, , max, , min, , MY_FALSE);
  W(null, , min, , max, , null, , MY_FALSE);
  W(null, , min, , max, , int,  0, MY_FALSE);
  W(null, , min, , max, , max, , MY_FALSE);

  W(null, , null, , min, , min, , MY_FALSE);
  W(null, , null, , min, , null, , MY_FALSE);
  W(null, , null, , min, , int,  0, MY_FALSE);
  W(null, , null, , min, , max, , MY_FALSE);
  W(null, , null, , null, , min, , MY_FALSE);
  W(null, , null, , null, , null, , MY_TRUE);
  W(null, , null, , null, , int,  0, MY_FALSE);
  W(null, , null, , null, , max, , MY_FALSE);
  W(null, , null, , int,  0, min, , MY_FALSE);
  W(null, , null, , int,  0, null, , MY_FALSE);
  W(null, , null, , int,  0, int,  0, MY_FALSE);
  W(null, , null, , int,  0, max, , MY_FALSE);
  W(null, , null, , max, , min, , MY_FALSE);
  W(null, , null, , max, , null, , MY_FALSE);
  W(null, , null, , max, , int,  0, MY_FALSE);
  W(null, , null, , max, , max, , MY_FALSE);

  W(null, , int, 0, min, , min, , MY_FALSE);
  W(null, , int, 0, min, , null, , MY_FALSE);
  W(null, , int, 0, min, , int,  0, MY_FALSE);
  W(null, , int, 0, min, , max, , MY_FALSE);
  W(null, , int, 0, null, , min, , MY_FALSE);
  W(null, , int, 0, null, , null, , MY_FALSE);
  W(null, , int, 0, null, , int,  0, MY_TRUE);
  W(null, , int, 0, null, , max, , MY_FALSE);
  W(null, , int, 0, int,  0, min, , MY_FALSE);
  W(null, , int, 0, int,  0, null, , MY_FALSE);
  W(null, , int, 0, int,  0, int,  0, MY_FALSE);
  W(null, , int, 0, int,  0, max, , MY_FALSE);
  W(null, , int, 0, max, , min, , MY_FALSE);
  W(null, , int, 0, max, , null, , MY_FALSE);
  W(null, , int, 0, max, , int,  0, MY_FALSE);
  W(null, , int, 0, max, , max, , MY_FALSE);

  W(null, , max, , min, , min, , MY_FALSE);
  W(null, , max, , min, , null, , MY_FALSE);
  W(null, , max, , min, , int,  0, MY_FALSE);
  W(null, , max, , min, , max, , MY_FALSE);
  W(null, , max, , null, , min, , MY_FALSE);
  W(null, , max, , null, , null, , MY_FALSE);
  W(null, , max, , null, , int,  0, MY_FALSE);
  W(null, , max, , null, , max, , MY_TRUE);
  W(null, , max, , int,  0, min, , MY_FALSE);
  W(null, , max, , int,  0, null, , MY_FALSE);
  W(null, , max, , int,  0, int,  0, MY_FALSE);
  W(null, , max, , int,  0, max, , MY_FALSE);
  W(null, , max, , max, , min, , MY_FALSE);
  W(null, , max, , max, , null, , MY_FALSE);
  W(null, , max, , max, , int,  0, MY_FALSE);
  W(null, , max, , max, , max, , MY_FALSE);

  W(int, 0, min, , min, , min, , MY_FALSE);
  W(int, 0, min, , min, , null, , MY_FALSE);
  W(int, 0, min, , min, , int,  0, MY_FALSE);
  W(int, 0, min, , min, , max, , MY_FALSE);
  W(int, 0, min, , null, , min, , MY_FALSE);
  W(int, 0, min, , null, , null, , MY_FALSE);
  W(int, 0, min, , null, , int,  0, MY_FALSE);
  W(int, 0, min, , null, , max, , MY_FALSE);
  W(int, 0, min, , int,  0, min, , MY_TRUE);
  W(int, 0, min, , int,  0, null, , MY_FALSE);
  W(int, 0, min, , int,  0, int,  0, MY_FALSE);
  W(int, 0, min, , int,  0, max, , MY_FALSE);
  W(int, 0, min, , max, , min, , MY_FALSE);
  W(int, 0, min, , max, , null, , MY_FALSE);
  W(int, 0, min, , max, , int,  0, MY_FALSE);
  W(int, 0, min, , max, , max, , MY_FALSE);

  W(int, 0, null, , min, , min, , MY_FALSE);
  W(int, 0, null, , min, , null, , MY_FALSE);
  W(int, 0, null, , min, , int,  0, MY_FALSE);
  W(int, 0, null, , min, , max, , MY_FALSE);
  W(int, 0, null, , null, , min, , MY_FALSE);
  W(int, 0, null, , null, , null, , MY_FALSE);
  W(int, 0, null, , null, , int,  0, MY_FALSE);
  W(int, 0, null, , null, , max, , MY_FALSE);
  W(int, 0, null, , int,  0, min, , MY_FALSE);
  W(int, 0, null, , int,  0, null, , MY_TRUE);
  W(int, 0, null, , int,  0, int,  0, MY_FALSE);
  W(int, 0, null, , int,  0, max, , MY_FALSE);
  W(int, 0, null, , max, , min, , MY_FALSE);
  W(int, 0, null, , max, , null, , MY_FALSE);
  W(int, 0, null, , max, , int,  0, MY_FALSE);
  W(int, 0, null, , max, , max, , MY_FALSE);

  W(int, 0, int, 0, min, , min, , MY_FALSE);
  W(int, 0, int, 0, min, , null, , MY_FALSE);
  W(int, 0, int, 0, min, , int,  0, MY_FALSE);
  W(int, 0, int, 0, min, , max, , MY_FALSE);
  W(int, 0, int, 0, null, , min, , MY_FALSE);
  W(int, 0, int, 0, null, , null, , MY_FALSE);
  W(int, 0, int, 0, null, , int,  0, MY_FALSE);
  W(int, 0, int, 0, null, , max, , MY_FALSE);
  W(int, 0, int, 0, int,  0, min, , MY_FALSE);
  W(int, 0, int, 0, int,  0, null, , MY_FALSE);
  W(int, 0, int, 0, int,  0, int,  0, MY_TRUE);
  W(int, 0, int, 0, int,  0, max, , MY_FALSE);
  W(int, 0, int, 0, max, , min, , MY_FALSE);
  W(int, 0, int, 0, max, , null, , MY_FALSE);
  W(int, 0, int, 0, max, , int,  0, MY_FALSE);
  W(int, 0, int, 0, max, , max, , MY_FALSE);

  W(int, 0, max, , min, , min, , MY_FALSE);
  W(int, 0, max, , min, , null, , MY_FALSE);
  W(int, 0, max, , min, , int,  0, MY_FALSE);
  W(int, 0, max, , min, , max, , MY_FALSE);
  W(int, 0, max, , null, , min, , MY_FALSE);
  W(int, 0, max, , null, , null, , MY_FALSE);
  W(int, 0, max, , null, , int,  0, MY_FALSE);
  W(int, 0, max, , null, , max, , MY_FALSE);
  W(int, 0, max, , int,  0, min, , MY_FALSE);
  W(int, 0, max, , int,  0, null, , MY_FALSE);
  W(int, 0, max, , int,  0, int,  0, MY_FALSE);
  W(int, 0, max, , int,  0, max, , MY_TRUE);
  W(int, 0, max, , max, , min, , MY_FALSE);
  W(int, 0, max, , max, , null, , MY_FALSE);
  W(int, 0, max, , max, , int,  0, MY_FALSE);
  W(int, 0, max, , max, , max, , MY_FALSE);

  W(max, , min, , min, , min, , MY_FALSE);
  W(max, , min, , min, , null, , MY_FALSE);
  W(max, , min, , min, , int,  0, MY_FALSE);
  W(max, , min, , min, , max, , MY_FALSE);
  W(max, , min, , null, , min, , MY_FALSE);
  W(max, , min, , null, , null, , MY_FALSE);
  W(max, , min, , null, , int,  0, MY_FALSE);
  W(max, , min, , null, , max, , MY_FALSE);
  W(max, , min, , int,  0, min, , MY_FALSE);
  W(max, , min, , int,  0, null, , MY_FALSE);
  W(max, , min, , int,  0, int,  0, MY_FALSE);
  W(max, , min, , int,  0, max, , MY_FALSE);
  W(max, , min, , max, , min, , MY_TRUE);
  W(max, , min, , max, , null, , MY_FALSE);
  W(max, , min, , max, , int,  0, MY_FALSE);
  W(max, , min, , max, , max, , MY_FALSE);

  W(max, , null, , min, , min, , MY_FALSE);
  W(max, , null, , min, , null, , MY_FALSE);
  W(max, , null, , min, , int,  0, MY_FALSE);
  W(max, , null, , min, , max, , MY_FALSE);
  W(max, , null, , null, , min, , MY_FALSE);
  W(max, , null, , null, , null, , MY_FALSE);
  W(max, , null, , null, , int,  0, MY_FALSE);
  W(max, , null, , null, , max, , MY_FALSE);
  W(max, , null, , int,  0, min, , MY_FALSE);
  W(max, , null, , int,  0, null, , MY_FALSE);
  W(max, , null, , int,  0, int,  0, MY_FALSE);
  W(max, , null, , int,  0, max, , MY_FALSE);
  W(max, , null, , max, , min, , MY_FALSE);
  W(max, , null, , max, , null, , MY_TRUE);
  W(max, , null, , max, , int,  0, MY_FALSE);
  W(max, , null, , max, , max, , MY_FALSE);

  W(max, , int, 0, min, , min, , MY_FALSE);
  W(max, , int, 0, min, , null, , MY_FALSE);
  W(max, , int, 0, min, , int,  0, MY_FALSE);
  W(max, , int, 0, min, , max, , MY_FALSE);
  W(max, , int, 0, null, , min, , MY_FALSE);
  W(max, , int, 0, null, , null, , MY_FALSE);
  W(max, , int, 0, null, , int,  0, MY_FALSE);
  W(max, , int, 0, null, , max, , MY_FALSE);
  W(max, , int, 0, int,  0, min, , MY_FALSE);
  W(max, , int, 0, int,  0, null, , MY_FALSE);
  W(max, , int, 0, int,  0, int,  0, MY_FALSE);
  W(max, , int, 0, int,  0, max, , MY_FALSE);
  W(max, , int, 0, max, , min, , MY_FALSE);
  W(max, , int, 0, max, , null, , MY_FALSE);
  W(max, , int, 0, max, , int,  0, MY_TRUE);
  W(max, , int, 0, max, , max, , MY_FALSE);

  W(max, , max, , min, , min, , MY_FALSE);
  W(max, , max, , min, , null, , MY_FALSE);
  W(max, , max, , min, , int,  0, MY_FALSE);
  W(max, , max, , min, , max, , MY_FALSE);
  W(max, , max, , null, , min, , MY_FALSE);
  W(max, , max, , null, , null, , MY_FALSE);
  W(max, , max, , null, , int,  0, MY_FALSE);
  W(max, , max, , null, , max, , MY_FALSE);
  W(max, , max, , int,  0, min, , MY_FALSE);
  W(max, , max, , int,  0, null, , MY_FALSE);
  W(max, , max, , int,  0, int,  0, MY_FALSE);
  W(max, , max, , int,  0, max, , MY_FALSE);
  W(max, , max, , max, , min, , MY_FALSE);
  W(max, , max, , max, , null, , MY_FALSE);
  W(max, , max, , max, , int,  0, MY_FALSE);
  W(max, , max, , max, , max, , MY_TRUE);
}
*/

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
