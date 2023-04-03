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
#include "sql/engine/expr/ob_expr_div.h"
#include "ob_expr_test_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

class ObExprDivTest: public ::testing::Test
{
  public:
    ObExprDivTest();
    virtual ~ObExprDivTest();
    virtual void SetUp();
    virtual void TearDown();
  private:
    // disallow copy
    ObExprDivTest(const ObExprDivTest &other);
    ObExprDivTest& operator=(const ObExprDivTest &other);
  protected:
    // data members
};

ObExprDivTest::ObExprDivTest()
{
}

ObExprDivTest::~ObExprDivTest()
{
}

void ObExprDivTest::SetUp()
{
}

void ObExprDivTest::TearDown()
{
}
#define DIV_EXCEPT_NULL(cmp_op, str_buf, func, type1, v1, type2, v2) \
{                                                     \
  ObObj t1;                                       \
  ObObj t2;                                       \
  ObObj vres;                                     \
  t1.set_##type1(v1);                                 \
  t2.set_##type2(v2);                                 \
  t2.set_collation_type(CS_TYPE_UTF8MB4_BIN);         \
  ObArenaAllocator alloc;\
  cmp_op op(alloc);                                          \
  int err = op.func(vres, t1, t2, str_buf, -1);           \
  EXPECT_TRUE(OB_SUCCESS == err);                     \
  EXPECT_TRUE(vres.is_null());                        \
}while(0)

#define R(t1, v1, t2, v2, rt, rv) ARITH_EXPECT_OBJ(ObExprDiv, &buf, calc, t1, v1, t2, v2, rt, rv)
#define TN(t1, v1, t2, v2) DIV_EXCEPT_NULL(ObExprDiv, &buf, calc, t1, v1, t2, v2)

TEST_F(ObExprDivTest, int_uint_test)
{
  ObMalloc buf;
  int64_t int1 = 0;
  int64_t int2 = 0;
  uint64_t uint1 = 0;
  uint64_t uint2 = 0;
  number::ObNumber num1;
  number::ObNumber num2;
  number::ObNumber num_ret;

  // basic test
  int1 = 23432;
  int2 = 786723;
  num1.from(int1, buf);
  num2.from(int2, buf);
  ASSERT_EQ(OB_SUCCESS, num1.div(num2, num_ret, buf));
  R(int, int1, int, int2, number, num_ret);
  int1 = -3490456;
  int2 = 465;
  num1.from(int1, buf);
  num2.from(int2, buf);
  ASSERT_EQ(OB_SUCCESS, num1.div(num2, num_ret, buf));
  R(int, int1, int, int2, number, num_ret);
  int1 = 45453232;
  int2 = -3545;
  num1.from(int1, buf);
  num2.from(int2, buf);
  ASSERT_EQ(OB_SUCCESS, num1.div(num2, num_ret, buf));
  R(int, int1, int, int2, number, num_ret);
  int1 = 3453535;
  uint2 = 4545;
  num1.from(int1, buf);
  num2.from(uint2, buf);
  ASSERT_EQ(OB_SUCCESS, num1.div(num2, num_ret, buf));
  R(int, int1, uint64, uint2, number, num_ret);

  // int_uint
  int1 = INT64_MAX;
  int2 = 1;
  num1.from(int1, buf);
  num2.from(int2, buf);
  ASSERT_EQ(OB_SUCCESS, num1.div(num2, num_ret, buf));
  R(int, int1, int, int2, number, num_ret);
  int1 = INT64_MIN;
  int2 = 1;
  num1.from(int1, buf);
  num2.from(int2, buf);
  ASSERT_EQ(OB_SUCCESS, num1.div(num2, num_ret, buf));
  R(int, int1, int, int2, number, num_ret);
  int1 = 0;
  int2 = INT64_MAX;
  num1.from(int1, buf);
  num2.from(int2, buf);
  ASSERT_EQ(OB_SUCCESS, num1.div(num2, num_ret, buf));
  R(int, int1, int, int2, number, num_ret);
  int1 = 0;
  int2 = INT64_MIN;
  num1.from(int1, buf);
  num2.from(int2, buf);
  ASSERT_EQ(OB_SUCCESS, num1.div(num2, num_ret, buf));
  R(int, int1, int, int2, number, num_ret);
  int1 = 0;
  uint2 = UINT64_MAX;
  num1.from(int1, buf);
  num2.from(uint2, buf);
  ASSERT_EQ(OB_SUCCESS, num1.div(num2, num_ret, buf));
  R(int, int1, uint64, uint2, number, num_ret);
  int1 = INT64_MAX;
  uint2 = UINT64_MAX;
  num1.from(int1, buf);
  num2.from(uint2, buf);
  ASSERT_EQ(OB_SUCCESS, num1.div(num2, num_ret, buf));
  R(int, int1, uint64, uint2, number, num_ret);
  int1 = INT64_MIN;
  uint2 = UINT64_MAX;
  num1.from(int1, buf);
  num2.from(uint2, buf);
  ASSERT_EQ(OB_SUCCESS, num1.div(num2, num_ret, buf));
  R(int, int1, uint64, uint2, number, num_ret);
  uint1 = UINT64_MAX;
  int2 = INT64_MAX;
  num1.from(uint1, buf);
  num2.from(int2, buf);
  ASSERT_EQ(OB_SUCCESS, num1.div(num2, num_ret, buf));
  R(uint64, uint1, int, int2, number, num_ret);
  uint1 = UINT64_MAX;
  int2 = INT64_MIN;
  num1.from(uint1, buf);
  num2.from(int2, buf);
  ASSERT_EQ(OB_SUCCESS, num1.div(num2, num_ret, buf));
  R(uint64, uint1, int, int2, number, num_ret);
  uint1 = 0;
  uint2 = UINT64_MAX;
  num1.from(uint1, buf);
  num2.from(uint2, buf);
  ASSERT_EQ(OB_SUCCESS, num1.div(num2, num_ret, buf));
  R(uint64, uint1, uint64, uint2, number, num_ret);
}

TEST_F(ObExprDivTest, float_double_test)
{
  ObMalloc buf;
  //basic_test
  R(float, static_cast<float>(2323), float, static_cast<float>(23), double, static_cast<double>(2323/23));
  R(float, 3, float, 2, double, 3/2.0);
  R(float, 0, float, 34, double, 0.0);
  R(float, 323.0, double, 46.66, double, static_cast<double>(323.0)/46.66);
  R(float, 3, float, 2, double, 3/2.0);
  R(float, 0, float, 34, double, 0.0);
  R(double, 32323.4, double, 4656.66, double, 32323.4/4656.66);
  R(double, 3, double, 2, double, 3/2.0);
  R(double, 0, double, 34, double, 0.0);

  // float_double_int_uint
  R(float, 0, int, INT64_MAX, double, 0.0);
  R(float, static_cast<float>(INT64_MAX), int, INT64_MAX, double, 1.0);
  R(float, 3434, int, INT64_MIN, double, 3434.0/INT64_MIN);
  R(float, 3454, uint64, UINT64_MAX, double, 3454.0/static_cast<double>(UINT64_MAX));
  R(double, 0, int, INT64_MAX, double, 0.0);
  R(double, static_cast<double>(INT64_MAX), int, INT64_MAX, double, 1.0);
  R(double, 3434, int, INT64_MIN, double, 3434.0/INT64_MIN);
  R(double, 3454, uint64, UINT64_MAX, double, 3454.0/static_cast<double>(UINT64_MAX));
}

TEST_F(ObExprDivTest, number_test)
{
  ObMalloc buf;
  number::ObNumber num123_p;
  num123_p.from("123.0", buf);
  number::ObNumber num123_n;
  num123_n.from("-123.0", buf);
  number::ObNumber num_zero;
  num_zero.from("0", buf);
  number::ObNumber num1_p;
  num1_p.from("1", buf);
  number::ObNumber num1_n;
  num1_n.from("-1", buf);
  number::ObNumber num2_n;
  num2_n.from("-2", buf);
  number::ObNumber num2_p;
  num2_p.from("2", buf);
  number::ObNumber numres_n;
  numres_n.from("-61.5", buf);
  number::ObNumber numres_p;
  numres_p.from("61.5", buf);

  R(number, num123_n, number, num123_p, number, num1_n);
  R(number, num123_n, number, num123_n, number, num1_p);
  R(number, num123_p, number, num123_p, number, num1_p);
  R(number, num_zero, number, num123_p, number, num_zero);
  R(number, num_zero, number, num123_n, number, num_zero);
  R(number, num123_n, number, num2_p, number, numres_n);
  R(number, num123_p, number, num2_n, number, numres_n);
  R(number, num123_p, number, num2_p, number, numres_p);
  R(number, num123_p, number, num2_n, number, numres_n);
  R(number, num123_n, number, num2_p, number, numres_n);
}

TEST_F(ObExprDivTest, string_test)
{
  ObMalloc buf;
  // string_int_uint
  //R(varchar, "", int, INT64_MAX, double, 0);
  //R(varchar, "", int, INT64_MIN, double, 0);
  //R(varchar, "abc", int, INT64_MAX, double, 0);
  //R(varchar, "abc", uint64, UINT64_MAX, double, 0);
  //R(varchar, "-9223372036854775808abc", int, INT64_MIN, double, 1.0);
  //R(varchar, "-9223372036854775807abc", int, INT64_MAX, double, -1.0);
  //R(varchar, "18446744073709551615abd13", uint64, UINT64_MAX, double, 1.0);
  //R(varchar, "-9223372036854775808", int, 1, double, static_cast<double>(INT64_MIN));
  //R(varchar, "9223372036854775807abc", int, 1, double, static_cast<double>(INT64_MAX));

  // string_double
  //R(varchar, "123abc", int, 123, double, 1.0);
  //R(varchar, "-123abc", int, 123, double, -1.0);
  //R(varchar, "-345abc12", double, 23.45, double, static_cast<double>(-345/23.45));
  //R(varchar, "345abc456", double, 123.3, double, 345/123.3);
  //R(varchar, "-123.5abc", double, 23.5, double, -123.5/23.5);
  R(varchar, "456.45", double, 12.45, double, 456.45/12.45);
}

TEST_F(ObExprDivTest, null_test)
{
  ObMalloc buf;
  number::ObNumber num_zero;
  num_zero.from("0", buf);
  TN(int, INT64_MAX, int, 0);
  TN(int, INT64_MIN, int, 0);
  TN(double, 12, double, 0.0);
  TN(float, static_cast<float>(121.3), float, 0.0);
  //TN(int, 34, varchar, "");
  //TN(int, 45, varchar, "abc");
  //TN(double, 232.23, varchar, "abc12323");
  TN(double, 123.4, number, num_zero);

}
/*TEST_F(ObExprDivTest, type_test)
{
  //row not support arithmetic
  TE(ObNullType, ObNullType, OB_ERR_INVALID_TYPE_FOR_OP);
  TE(ObIntType, ObNullType, OB_ERR_INVALID_TYPE_FOR_OP);
  TE(ObIntType, ObNullType, OB_ERR_INVALID_TYPE_FOR_OP);
  TE(ObIntType, ObIntType, OB_ERR_INVALID_TYPE_FOR_OP);
  TE(ObIntType, ObFloatType, OB_ERR_INVALID_TYPE_FOR_OP);
  TE(ObIntType, ObDoubleType, OB_ERR_INVALID_TYPE_FOR_OP);
  TE(ObIntType, ObDateTimeType, OB_ERR_INVALID_TYPE_FOR_OP);
  TE(ObVarcharType, ObPreciseDateTimeType, OB_ERR_INVALID_TYPE_FOR_OP);
  TE(ObIntType, ObVarcharType, OB_ERR_INVALID_TYPE_FOR_OP);
  TE(ObIntType, ObUnknownType, OB_ERR_INVALID_TYPE_FOR_OP);
  TE(ObIntType, ObCreateTimeType, OB_ERR_INVALID_TYPE_FOR_OP);
  TE(ObIntType, ObModifyTimeType, OB_ERR_INVALID_TYPE_FOR_OP);
  TE(ObIntType, ObExtendType, OB_ERR_INVALID_TYPE_FOR_OP);
  TE(ObIntType, ObBoolType, OB_ERR_INVALID_TYPE_FOR_OP);
  TE(ObIntType, ObDecimalType, OB_ERR_INVALID_TYPE_FOR_OP);

  //ObNullType && XX_Type
  T(ObNullType, ObNullType, ObNullType);
  T(ObNullType, ObIntType, ObNullType);
  T(ObNullType, ObFloatType, ObNullType);
  T(ObNullType, ObDoubleType, ObNullType);
  TYE(ObNullType, ObDateTimeType);
  TYE(ObNullType, ObPreciseDateTimeType);
  T(ObNullType, ObVarcharType, ObNullType);
  T(ObNullType, ObUnknownType, ObNullType);
  TYE(ObNullType, ObCreateTimeType);
  TYE(ObNullType, ObModifyTimeType);
  TYE(ObNullType, ObExtendType);
  TYE(ObNullType, ObBoolType);
  T(ObNullType, ObDecimalType, ObNullType);

  //ObIntType && XX_Type
  T(ObIntType, ObNullType, ObNullType);
  T(ObIntType, ObIntType, ObNumberType);
  T(ObIntType, ObFloatType, ObNumberType);
  T(ObIntType, ObDoubleType, ObNumberType);
  TYE(ObIntType, ObDateTimeType);
  TYE(ObIntType, ObPreciseDateTimeType);
  T(ObIntType, ObVarcharType, ObNumberType);
  T(ObIntType, ObUnknownType, ObNumberType);
  TYE(ObIntType, ObCreateTimeType);
  TYE(ObIntType, ObModifyTimeType);
  TYE(ObIntType, ObExtendType);
  TYE(ObIntType, ObBoolType);
  //T(ObIntType, ObDecimalType, ObDecimalType);

  //ObFloatType && XX_Type
  T(ObFloatType, ObNullType, ObNullType);
  T(ObFloatType, ObIntType, ObNumberType);
  T(ObFloatType, ObFloatType, ObNumberType);
  T(ObFloatType, ObDoubleType, ObNumberType);
  TYE(ObFloatType, ObDateTimeType);
  TYE(ObFloatType, ObPreciseDateTimeType);
  T(ObFloatType, ObVarcharType, ObNumberType);
  T(ObFloatType, ObUnknownType, ObNumberType);
  TYE(ObFloatType, ObCreateTimeType);
  TYE(ObFloatType, ObModifyTimeType);
  TYE(ObFloatType, ObExtendType);
  TYE(ObFloatType, ObBoolType);
  //T(ObFloatType, ObDecimalType, ObDoubleType);

  //ObDoubleType && XX_Type
  T(ObDoubleType, ObNullType, ObNullType);
  T(ObDoubleType, ObIntType, ObNumberType);
  T(ObDoubleType, ObFloatType, ObNumberType);
  T(ObDoubleType, ObDoubleType, ObNumberType);
  TYE(ObDoubleType, ObDateTimeType);
  TYE(ObDoubleType, ObPreciseDateTimeType);
  T(ObDoubleType, ObVarcharType, ObNumberType);
  T(ObDoubleType, ObUnknownType, ObNumberType);
  TYE(ObDoubleType, ObCreateTimeType);
  TYE(ObDoubleType, ObModifyTimeType);
  TYE(ObDoubleType, ObExtendType);
  TYE(ObDoubleType, ObBoolType);
  //T(ObDoubleType, ObDecimalType, ObDoubleType);

  //ObDateTimeType && XX_Type
  TYE(ObDateTimeType, ObNullType);
  TYE(ObDateTimeType, ObIntType);
  TYE(ObDateTimeType, ObFloatType);
  TYE(ObDateTimeType, ObDoubleType);
  TYE(ObDateTimeType, ObDateTimeType);
  TYE(ObDateTimeType, ObPreciseDateTimeType);
  TYE(ObDateTimeType, ObVarcharType);
  TYE(ObDateTimeType, ObUnknownType);
  TYE(ObDateTimeType, ObCreateTimeType);
  TYE(ObDateTimeType, ObModifyTimeType);
  TYE(ObDateTimeType, ObExtendType);
  TYE(ObDateTimeType, ObBoolType);
  //T(ObDateTimeType, ObDecimalType, ObDecimalType);

  //ObPreciseDateTimeType && XX_Type
  TYE(ObPreciseDateTimeType, ObNullType);
  TYE(ObPreciseDateTimeType, ObIntType);
  TYE(ObPreciseDateTimeType, ObFloatType);
  TYE(ObPreciseDateTimeType, ObDoubleType);
  TYE(ObPreciseDateTimeType, ObDateTimeType);
  TYE(ObPreciseDateTimeType, ObPreciseDateTimeType);
  TYE(ObPreciseDateTimeType, ObVarcharType);
  TYE(ObPreciseDateTimeType, ObUnknownType);
  TYE(ObPreciseDateTimeType, ObCreateTimeType);
  TYE(ObPreciseDateTimeType, ObModifyTimeType);
  TYE(ObPreciseDateTimeType, ObExtendType);
  TYE(ObPreciseDateTimeType, ObBoolType);
  //T(ObPreciseDateTimeType, ObDecimalType, ObDecimalType);

  //ObVarcharType && XX_Type
  T(ObVarcharType, ObNullType, ObNullType);
  T(ObVarcharType, ObIntType, ObNumberType);
  T(ObVarcharType, ObFloatType, ObNumberType);
  T(ObVarcharType, ObDoubleType, ObNumberType);
  TYE(ObVarcharType, ObDateTimeType);
  TYE(ObVarcharType, ObPreciseDateTimeType);
  T(ObVarcharType, ObVarcharType, ObNumberType);
  T(ObVarcharType, ObUnknownType, ObNumberType);
  TYE(ObVarcharType, ObCreateTimeType);
  TYE(ObVarcharType, ObModifyTimeType);
  TYE(ObVarcharType, ObExtendType);
  TYE(ObVarcharType, ObBoolType);
  //T(ObVarcharType, ObDecimalType, ObDecimalType);

  //ObUnknownType && XX_Type
  T(ObUnknownType, ObNullType, ObNullType);
  T(ObUnknownType, ObIntType, ObNumberType);
  T(ObUnknownType, ObFloatType, ObNumberType);
  T(ObUnknownType, ObDoubleType, ObNumberType);
  TYE(ObUnknownType, ObDateTimeType);
  TYE(ObUnknownType, ObPreciseDateTimeType);
  T(ObUnknownType, ObVarcharType, ObNumberType);
  T(ObUnknownType, ObUnknownType, ObNumberType);
  TYE(ObUnknownType, ObCreateTimeType);
  TYE(ObUnknownType, ObModifyTimeType);
  TYE(ObUnknownType, ObExtendType);
  TYE(ObUnknownType, ObBoolType);
  //T(ObUnknownType, ObDecimalType, ObMaxType);

  //ObCreateTimeType && XX_Type
  TYE(ObCreateTimeType, ObNullType);
  TYE(ObCreateTimeType, ObIntType);
  TYE(ObCreateTimeType, ObFloatType);
  TYE(ObCreateTimeType, ObDoubleType);
  TYE(ObCreateTimeType, ObDateTimeType);
  TYE(ObCreateTimeType, ObPreciseDateTimeType);
  TYE(ObCreateTimeType, ObVarcharType);
  TYE(ObCreateTimeType, ObUnknownType);
  TYE(ObCreateTimeType, ObCreateTimeType);
  TYE(ObCreateTimeType, ObModifyTimeType);
  TYE(ObCreateTimeType, ObExtendType);
  TYE(ObCreateTimeType, ObBoolType);
  //T(ObCreateTimeType, ObDecimalType, ObDecimalType);

  //ObModifyTimeType && XX_Type
  TYE(ObModifyTimeType, ObNullType);
  TYE(ObModifyTimeType, ObIntType);
  TYE(ObModifyTimeType, ObFloatType);
  TYE(ObModifyTimeType, ObDoubleType);
  TYE(ObModifyTimeType, ObDateTimeType);
  TYE(ObModifyTimeType, ObPreciseDateTimeType);
  TYE(ObModifyTimeType, ObVarcharType);
  TYE(ObModifyTimeType, ObUnknownType);
  TYE(ObModifyTimeType, ObCreateTimeType);
  TYE(ObModifyTimeType, ObModifyTimeType);
  TYE(ObModifyTimeType, ObExtendType);
  TYE(ObModifyTimeType, ObBoolType);
  //T(ObModifyTimeType, ObDecimalType, ObDecimalType);

  //ObExtendType && XX_Type
  TYE(ObExtendType, ObNullType);
  TYE(ObExtendType, ObIntType);
  TYE(ObExtendType, ObFloatType);
  TYE(ObExtendType, ObDoubleType);
  TYE(ObExtendType, ObDateTimeType);
  TYE(ObExtendType, ObPreciseDateTimeType);
  TYE(ObExtendType, ObVarcharType);
  TYE(ObExtendType, ObUnknownType);
  TYE(ObExtendType, ObCreateTimeType);
  TYE(ObExtendType, ObModifyTimeType);
  TYE(ObExtendType, ObExtendType);
  TYE(ObExtendType, ObBoolType);
  //T(ObExtendType, ObDecimalType, ObMaxType);

  //ObBoolType && XX_Type
  TYE(ObBoolType, ObNullType);
  TYE(ObBoolType, ObIntType);
  TYE(ObBoolType, ObFloatType);
  TYE(ObBoolType, ObDoubleType);
  TYE(ObBoolType, ObDateTimeType);
  TYE(ObBoolType, ObPreciseDateTimeType);
  TYE(ObBoolType, ObVarcharType);
  TYE(ObBoolType, ObUnknownType);
  TYE(ObBoolType, ObCreateTimeType);
  TYE(ObBoolType, ObModifyTimeType);
  TYE(ObBoolType, ObExtendType);
  TYE(ObBoolType, ObBoolType);
  //T(ObBoolType, ObDecimalType, ObDecimalType);
}

TEST_F(ObExprDivTest, div_by_zero)
{
  ObExprStringBuf buf;
  // divide by zero
  R(int, 100, int, 0, ObNullType);
}

TEST_F(ObExprDivTest, result_test)
{
  //ObNullType
  ObExprStringBuf buf;
  R(null, 0, null, 1, ObNullType);
  R(null, 0, int, 2, ObNullType);
  R(null, 0, float, 3.0, ObNullType);
  R(null, 0, double, 4.0, ObNullType);
  //R(null, 0, datetime, 5, OB_ERR_INVALID_TYPE_FOR_OP);
  R(null, 0, datetime, 5, ObNullType);
  R(null, 0, precise_datetime, 6, ObNullType);
  R(null, 0, varchar, "7", ObNullType);
  R(null, 0, ctime, 9, ObNullType);
  R(null, 0, mtime, 10, ObNullType);
  R(null, 0, ext, 2, ObNullType);
  R(null, 0, bool, 1, ObNullType);
  //R(null, 0, , 2, ObNullType); decimal not supported

  //ObIntType
  R(int, 0, null, 1, ObNullType);
  R(int, 1, int, 2, 0.5);
  R(int, 2, float, 3.0, 0.666666666666666666666666666666666666666666667);
  R(int, 3, double, 4.0, 0.75);
  R(int, 4, datetime, 5, OB_ERR_INVALID_TYPE_FOR_OP);
  R(int, 5, precise_datetime, 6, OB_ERR_INVALID_TYPE_FOR_OP);
  R(int, 6, varchar, "7", 0.857142857142857142857142857142857142857142857);
  R(int, 6, varchar, "a", OB_ERR_CAST_VARCHAR_TO_NUMBER);
  R(int, 6, varchar, "07a", OB_ERR_CAST_VARCHAR_TO_NUMBER);
  R(int, 8, ctime, 9, OB_ERR_INVALID_TYPE_FOR_OP);
  R(int, 9, mtime, 10, OB_ERR_INVALID_TYPE_FOR_OP);
  E(int, 10, ext, 2, OB_ERR_INVALID_TYPE_FOR_OP);
  R(int, 11, bool, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  R(int, 11, bool, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  //R(null, 0, , 2, ObNullType); decimal not supported

  //ObFloatType
  R(float, 0.0, null, 1, ObNullType);
  R(float, 1.0, int, 2, 0.5);
  R(float, 2.0, float, 3.0, 0.666666666666666666666666666666666666666666667);
  R(float, 3.0, double, 4.0, 0.75);
  R(float, 4.0, datetime, 5, OB_ERR_INVALID_TYPE_FOR_OP);
  R(float, 5.0, precise_datetime, 6, OB_ERR_INVALID_TYPE_FOR_OP);
  R(float, 6.0, varchar, "7", 0.857142857142857142857142857142857142857142857);
  R(float, 6.0, varchar, "a", OB_ERR_CAST_VARCHAR_TO_NUMBER);
  R(float, 6.0, varchar, "07a", OB_ERR_CAST_VARCHAR_TO_NUMBER);
  R(float, 6.0, varchar, "0.7a", OB_ERR_CAST_VARCHAR_TO_NUMBER);
  R(float, 8.0, ctime, 9, OB_ERR_INVALID_TYPE_FOR_OP);
  R(float, 9.0, mtime, 10, OB_ERR_INVALID_TYPE_FOR_OP);
  E(float, 10.0, ext, 2, OB_ERR_INVALID_TYPE_FOR_OP);
  R(float, 11.0, bool, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  R(float, 11.0, bool, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  //R(null, 0, , 2, ObNullType); decimal not supported

  //ObDoubleType
  R(double, 0.0, null, 1, ObNullType);
  R(double, 1.0, int, 2, 0.5);
  R(double, 2.0, float, 3.0, 0.666666666666666666666666666666666666666666667);
  R(double, 3.0, double, 4.0, 0.75);
  R(double, 4.0, datetime, 5, OB_ERR_INVALID_TYPE_FOR_OP);
  R(double, 5.0, precise_datetime, 6, OB_ERR_INVALID_TYPE_FOR_OP);
  R(double, 6.0, varchar, "7", 0.857142857142857142857142857142857142857142857);
  R(double, 6.0, varchar, "a", OB_ERR_CAST_VARCHAR_TO_NUMBER);
  R(double, 6.0, varchar, "07a", OB_ERR_CAST_VARCHAR_TO_NUMBER);
  R(double, 6.0, varchar, "0.7a", OB_ERR_CAST_VARCHAR_TO_NUMBER);
  R(double, 8.0, ctime, 9, OB_ERR_INVALID_TYPE_FOR_OP);
  R(double, 9.0, mtime, 10, OB_ERR_INVALID_TYPE_FOR_OP);
  E(double, 10.0, ext, 2, OB_ERR_INVALID_TYPE_FOR_OP);
  R(double, 11.0, bool, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  R(double, 11.0, bool, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  //R(null, 0, , 2, ObNullType); decimal not supported

  //ObDateTimeType
  R(datetime, 0, null, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  R(datetime, 1, int, 2, OB_ERR_INVALID_TYPE_FOR_OP);
  R(datetime, 2, float, 3.0, OB_ERR_INVALID_TYPE_FOR_OP);
  R(datetime, 3, double, 4.0, OB_ERR_INVALID_TYPE_FOR_OP);
  R(datetime, 4, datetime, 5, OB_ERR_INVALID_TYPE_FOR_OP);
  R(datetime, 5, precise_datetime, 6, OB_ERR_INVALID_TYPE_FOR_OP);
  R(datetime, 6, varchar, "7", OB_ERR_INVALID_TYPE_FOR_OP);
  R(datetime, 6, varchar, "a", OB_ERR_INVALID_TYPE_FOR_OP);
  R(datetime, 6, varchar, "07a", OB_ERR_INVALID_TYPE_FOR_OP);
  R(datetime, 8, ctime, 9, OB_ERR_INVALID_TYPE_FOR_OP);
  R(datetime, 9, mtime, 10, OB_ERR_INVALID_TYPE_FOR_OP);
  E(datetime, 10, ext, 2, OB_ERR_INVALID_TYPE_FOR_OP);
  R(datetime, 11, bool, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  R(datetime, 11, bool, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  //R(null, 0, , 2, ObNullType); decimal not supported

  //ObPreciseDateTimeType
  R(precise_datetime, 0, null, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  R(precise_datetime, 1, int, 2, OB_ERR_INVALID_TYPE_FOR_OP);
  R(precise_datetime, 2, float, 3.0, OB_ERR_INVALID_TYPE_FOR_OP);
  R(precise_datetime, 3, double, 4.0, OB_ERR_INVALID_TYPE_FOR_OP);
  R(precise_datetime, 4, datetime, 5, OB_ERR_INVALID_TYPE_FOR_OP);
  R(precise_datetime, 5, precise_datetime, 6, OB_ERR_INVALID_TYPE_FOR_OP);
  R(precise_datetime, 6, varchar, "7", OB_ERR_INVALID_TYPE_FOR_OP);
  R(precise_datetime, 6, varchar, "a", OB_ERR_INVALID_TYPE_FOR_OP);
  R(precise_datetime, 6, varchar, "07a", OB_ERR_INVALID_TYPE_FOR_OP);
  R(precise_datetime, 8, ctime, 9, OB_ERR_INVALID_TYPE_FOR_OP);
  R(precise_datetime, 9, mtime, 10, OB_ERR_INVALID_TYPE_FOR_OP);
  E(precise_datetime, 10, ext, 2, OB_ERR_INVALID_TYPE_FOR_OP);
  R(precise_datetime, 11, bool, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  R(precise_datetime, 11, bool, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  //R(null, 0, , 2, ObNullType); decimal not supported

  //ObVarcharType
  R(varchar, "0", null, 1, ObNullType);
  R(varchar, "1", int, 2, 0.5);
  R(varchar, "2", float, 3.0, 0.666666666666666666666666666666666666666666667);
  R(varchar, "2.12345", float, 3.0, 0.707816666666666666666666666666666666666666667);
  R(varchar, "3", double, 4.0, 0.75);
  R(varchar, "3.12345", double, 4.0, 0.7808625);
  R(varchar, "4", datetime, 5, OB_ERR_INVALID_TYPE_FOR_OP);
  R(varchar, "5", precise_datetime, 6, OB_ERR_INVALID_TYPE_FOR_OP);
  R(varchar, "6", varchar, "7", 0.857142857142857142857142857142857142857142857);
  R(varchar, "6", varchar, "a", OB_ERR_CAST_VARCHAR_TO_NUMBER);
  R(varchar, "6", varchar, "07a", OB_ERR_CAST_VARCHAR_TO_NUMBER);
  R(varchar, "8", ctime, 9, OB_ERR_INVALID_TYPE_FOR_OP);
  R(varchar, "9", mtime, 10, OB_ERR_INVALID_TYPE_FOR_OP);
  E(varchar, "10", ext, 2, OB_ERR_INVALID_TYPE_FOR_OP);
  R(varchar, "11", bool, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  R(varchar, "11", bool, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  //R(null, 0, , 2, ObNullType); decimal not supported

  //ObCreateTimeType
  R(ctime, 0, null, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  R(ctime, 1, int, 2, OB_ERR_INVALID_TYPE_FOR_OP);
  R(ctime, 2, float, 3.0, OB_ERR_INVALID_TYPE_FOR_OP);
  R(ctime, 3, double, 4.0, OB_ERR_INVALID_TYPE_FOR_OP);
  R(ctime, 4, datetime, 5, OB_ERR_INVALID_TYPE_FOR_OP);
  R(ctime, 5, precise_datetime, 6, OB_ERR_INVALID_TYPE_FOR_OP);
  R(ctime, 6, varchar, "7", OB_ERR_INVALID_TYPE_FOR_OP);
  R(ctime, 6, varchar, "a", OB_ERR_INVALID_TYPE_FOR_OP);
  R(ctime, 6, varchar, "07a", OB_ERR_INVALID_TYPE_FOR_OP);
  R(ctime, 8, ctime, 9, OB_ERR_INVALID_TYPE_FOR_OP);
  R(ctime, 9, mtime, 10, OB_ERR_INVALID_TYPE_FOR_OP);
  E(ctime, 10, ext, 2, OB_ERR_INVALID_TYPE_FOR_OP);
  R(ctime, 11, bool, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  R(ctime, 11, bool, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  //R(null, 0, , 2, ObNullType); decimal not supported

  //ObModifyTimeType
  R(mtime, 0, null, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  R(mtime, 1, int, 2, OB_ERR_INVALID_TYPE_FOR_OP);
  R(mtime, 2, float, 3.0, OB_ERR_INVALID_TYPE_FOR_OP);
  R(mtime, 3, double, 4.0, OB_ERR_INVALID_TYPE_FOR_OP);
  R(mtime, 4, datetime, 5, OB_ERR_INVALID_TYPE_FOR_OP);
  R(mtime, 5, precise_datetime, 6, OB_ERR_INVALID_TYPE_FOR_OP);
  R(mtime, 6, varchar, "7", OB_ERR_INVALID_TYPE_FOR_OP);
  R(mtime, 6, varchar, "a", OB_ERR_INVALID_TYPE_FOR_OP);
  R(mtime, 6, varchar, "07a", OB_ERR_INVALID_TYPE_FOR_OP);
  R(mtime, 8, ctime, 9, OB_ERR_INVALID_TYPE_FOR_OP);
  R(mtime, 9, mtime, 10, OB_ERR_INVALID_TYPE_FOR_OP);
  E(mtime, 10, ext, 2, OB_ERR_INVALID_TYPE_FOR_OP);
  R(mtime, 11, bool, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  R(mtime, 11, bool, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  //R(null, 0, , 2, ObNullType); decimal not supported

  //ObBoolType
  R(bool, 0, null, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  R(bool, 1, null, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  R(bool, 0, int, 2, OB_ERR_INVALID_TYPE_FOR_OP);
  R(bool, 1, int, 2, OB_ERR_INVALID_TYPE_FOR_OP);
  R(bool, 0, float, 3.0, OB_ERR_INVALID_TYPE_FOR_OP);
  R(bool, 1, float, 3.0, OB_ERR_INVALID_TYPE_FOR_OP);
  R(bool, 0, double, 4.0, OB_ERR_INVALID_TYPE_FOR_OP);
  R(bool, -2, double, 4.0, OB_ERR_INVALID_TYPE_FOR_OP);
  R(bool, 0, datetime, 5, OB_ERR_INVALID_TYPE_FOR_OP);
  R(bool, 4, datetime, 5, OB_ERR_INVALID_TYPE_FOR_OP);
  R(bool, 0, precise_datetime, 6, OB_ERR_INVALID_TYPE_FOR_OP);
  R(bool, 5, precise_datetime, 6, OB_ERR_INVALID_TYPE_FOR_OP);
  R(bool, 0, varchar, "7", OB_ERR_INVALID_TYPE_FOR_OP);
  R(bool, 6, varchar, "7", OB_ERR_INVALID_TYPE_FOR_OP);
  R(bool, 0, varchar, "a", OB_ERR_INVALID_TYPE_FOR_OP);
  R(bool, 6, varchar, "a", OB_ERR_INVALID_TYPE_FOR_OP);
  R(bool, 0, varchar, "07a", OB_ERR_INVALID_TYPE_FOR_OP);
  R(bool, 6, varchar, "07a", OB_ERR_INVALID_TYPE_FOR_OP);
  R(bool, 0, ctime, 9, OB_ERR_INVALID_TYPE_FOR_OP);
  R(bool, 8, ctime, 9, OB_ERR_INVALID_TYPE_FOR_OP);
  R(bool, 0, mtime, 10, OB_ERR_INVALID_TYPE_FOR_OP);
  R(bool, 9, mtime, 10, OB_ERR_INVALID_TYPE_FOR_OP);
  E(bool, 10, ext, 2, OB_ERR_INVALID_TYPE_FOR_OP);
  R(bool, 11, bool, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  R(bool, 11, bool, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  //R(null, 0, , 2, ObNullType); decimal not supported

  //ObExtendType
  R(ext, 0, null, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E(ext, 0, int, 2, OB_ERR_INVALID_TYPE_FOR_OP);
  E(ext, 0, float, 3.0, OB_ERR_INVALID_TYPE_FOR_OP);
  E(ext, 0, double, 4.0, OB_ERR_INVALID_TYPE_FOR_OP);
  E(ext, 0, datetime, 5, OB_ERR_INVALID_TYPE_FOR_OP);
  E(ext, 0, precise_datetime, 6, OB_ERR_INVALID_TYPE_FOR_OP);
  E(ext, 0, varchar, "7", OB_ERR_INVALID_TYPE_FOR_OP);
  E(ext, 0, varchar, "a", OB_ERR_INVALID_TYPE_FOR_OP);
  E(ext, 0, varchar, "07a", OB_ERR_INVALID_TYPE_FOR_OP);
  E(ext, 0, ctime, 9, OB_ERR_INVALID_TYPE_FOR_OP);
  E(ext, 0, mtime, 10, OB_ERR_INVALID_TYPE_FOR_OP);
  E(ext, 0, ext, 2, OB_ERR_INVALID_TYPE_FOR_OP);
  E(ext, 0, bool, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  E(ext, 0, bool, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  //R(null, 0, , 2, ObNullType); decimal not supported

  //R(null, 0, , 2, ObNullType); decimal not supported
}*/

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
