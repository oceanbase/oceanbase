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

#define USING_LOG_PREFIX SQL_ENG
#include <gtest/gtest.h>
#include "sql/engine/expr/ob_expr_add.h"
#include "ob_expr_test_utils.h"
#include "lib/oblog/ob_log.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

class ObExprAddTest: public ::testing::Test
{
public:
  ObExprAddTest();
  virtual ~ObExprAddTest();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  ObExprAddTest(const ObExprAddTest &other);
  ObExprAddTest& operator=(const ObExprAddTest &other);
protected:
  // data members
};

ObExprAddTest::ObExprAddTest()
{
}

ObExprAddTest::~ObExprAddTest()
{
}

void ObExprAddTest::SetUp()
{
}

void ObExprAddTest::TearDown()
{
}

#define R(t1, v1, t2, v2, rt, res) ARITH_EXPECT_OBJ(ObExprAdd, &buf, calc, t1, v1, t2, v2, rt, res)
#define FE(t1, v1, t2, v2) ARITH_EXPECT_ERROR(ObExprAdd, &buf, calc, t1, v1, t2, v2)
#define D_PRESCISION 10
#define DEQ(v1, v2) EXPECT_TRUE(double_cmp_given_precision((v1), (v2), D_PRESCISION)==0)
#define DNEQ(v1, v2) EXPECT_TRUE(double_cmp_given_precision((v1), (v2), D_PRESCISION)!=0)
#define RD(t1, v1, t2, v2, res, p) ARITH_EXPECT_OBJ_DOUBLE(ObExprAdd, &buf, calc, t1, v1, t2, v2, res, p)

TEST_F(ObExprAddTest, int_uint_test)
{
  ObMalloc buf;
  // basic test
  R(int, 16413, int, 29168, int, 45581);
  R(int, 900, int, 32591, int, 33491);
  R(int, 18762, int, 1655, int, 20417);
  R(int, 17410, int, 6359, int, 23769);
  R(int, 9161, int, -18636, int, -9475);
  R(int, 22355, int, -24767, int, -2412);
  R(int, 23655, int, -15574, int, 8081);
  R(int, 4031, int, -12052, int, -8021);
  R(int, 27350, int, -1150, int, 26200);
  R(int, 16941, int, -21724, int, -4783);
  R(int, 13966, int, -3430, int, 10536);

  // int_int
  R(int, INT64_MIN, int, 0, int, INT64_MIN);
  R(int, INT64_MIN + 1, int, -1, int, INT64_MIN);
  R(int, INT64_MIN, int, INT64_MAX, int, -1);
  R(int, 0, int, 0, int, 0);
  R(int, INT64_MAX, int, 0, int, INT64_MAX);
  R(int, INT64_MAX, int, -1, int, INT64_MAX - 1);

  // int_uint
  R(int, INT64_MAX, uint64, 0, uint64, static_cast<uint64_t>INT64_MAX);
  R(int, INT64_MAX, uint64, static_cast<uint64_t>INT64_MAX + 1, uint64, UINT64_MAX);
  R(int, INT64_MIN, uint64, static_cast<uint64_t>INT64_MAX + 1, int, 0);
  R(int, 0, uint64, 0, uint64, 0);
  R(int, 0, uint64, UINT64_MAX, uint64, UINT64_MAX);

  // uint_uint
  R(uint64, 0, uint64, 0, uint64, 0);
  R(uint64, UINT64_MAX, uint64, 0, uint64, UINT64_MAX);
  R(uint64, UINT64_MAX - 1, uint64, 1, uint64, UINT64_MAX);

  //smallint--tinyint
  R(tinyint, -128, smallint, -32768, int, -32896);
  R(tinyint, -128, smallint, 32767, int, 32639);
  R(tinyint, -5, smallint, -12, int, -17);
  R(tinyint, -5, smallint, -5, int, -10);
  R(tinyint, -5, smallint, 12, int, 7);
  R(tinyint, 5, smallint, -12, int, -7);
  R(tinyint, 5, smallint, 12, int, 17);
  R(tinyint, 12, smallint, 12, int, 24);
  R(tinyint, 127, smallint, -32768, int, -32641);
  R(tinyint, 127, smallint, 32767, int, 32894);

  //uint--smallint
  R(int, 1, uint64, 1u, uint64, 2u);
  R(int, 5, uint64, 5u, uint64, 10u);
  R(int, 12, uint64, 5u, uint64, 17u);
  R(int, 12, uint64, 12u, uint64, 24u);
  R(int, 32767, uint64, 32767u, uint64, 65534u);
  R(int, 4294967295, uint64, 32767u, uint64, 4295000062u);

  //type promotion test
  //XX->int32->int
  R(int32, INT32_MAX, int32, INT32_MAX, int, 4294967294);
  R(int32, INT32_MAX, int32, 1, int, 2147483648);
  R(int32, INT32_MAX, mediumint, 1, int, 2147483648);
  R(int32, INT32_MAX, smallint, 1, int, 2147483648);
  R(int32, INT32_MAX, tinyint, 1, int, 2147483648);

  R(mediumint, 8388607, mediumint, 8388607, int32, 16777214);
  R(mediumint, 8388607, mediumint, 1, int32, 8388608);
  R(mediumint, 8388607, smallint, 1, int32, 8388608);
  R(mediumint, 8388607, tinyint, 1, int32, 8388608);

  R(smallint, INT16_MAX, smallint, INT16_MAX, int32, INT16_MAX * 2);
  R(smallint, INT16_MAX, smallint, 1, int32, INT16_MAX + 1);
  R(smallint, INT16_MAX, tinyint, 1, int32, INT16_MAX + 1);
  R(tinyint, INT8_MAX, tinyint, INT8_MAX, int32, INT8_MAX * 2);
  R(tinyint, INT8_MAX, tinyint, 1, int32, INT8_MAX + 1);

  //overflow/underflow
  FE(int, INT64_MAX, int, INT64_MAX);
  FE(int, INT64_MAX, int, 1);
  FE(int, INT64_MAX, tinyint, 1);
  FE(int, INT64_MIN, int, INT64_MIN);
  FE(int, INT64_MIN, int, -1);
  FE(int, INT64_MIN, tinyint, -1);

  FE(utinyint, 0, tinyint, -1);
  FE(usmallint, 0, tinyint, -1);
  FE(umediumint, 0, tinyint, -1);
  FE(uint32, 0, tinyint, -1);
  FE(uint64, 0, tinyint, -1);

  //null
  R(tinyint, 42, null, , null, );
  R(smallint, 42, null, , null, );
  R(mediumint, 42, null, , null, );
  R(int32, 42, null, , null, );
  R(int, 42, null, , null, );

  R(utinyint, 42, null, , null, );
  R(usmallint, 42, null, , null, );
  R(umediumint, 42, null, , null, );
  R(uint32, 42, null, , null, );
  R(uint64, 42, null, , null, );

  R(null, , tinyint, 42, null, );
  R(null, , smallint, 42, null, );
  R(null, , mediumint, 42, null, );
  R(null, , int32, 42, null, );
  R(null, , int, 42, null, );

  R(null, , utinyint, 42, null, );
  R(null, , usmallint, 42, null, );
  R(null, , umediumint, 42, null, );
  R(null, , uint32, 42, null, );
  R(null, , uint64, 42, null, );

  R(null, , null, , null, );

}

TEST_F(ObExprAddTest, float_double_test)
{
  ObMalloc buf;

  //int -- float
  RD(int, -12, float, static_cast<float>(-12.001), -24.0010004, 10);
  RD(int, -12, float, static_cast<float>(-12), -24, 10);
  RD(int, -5, float, static_cast<float>(-12), -17, 10);
  RD(int, -5, float, static_cast<float>(12), 7, 10);
  RD(int, 5, float, static_cast<float>(-12), -7, 10);
  RD(int, 5, float, static_cast<float>(5), 10, 10);
  RD(int, 5, float, static_cast<float>(5.001), 10.00099993, 10);
  RD(int, 5, float, static_cast<float>(12), 17, 10);

  //int--double
  RD(int, -12, double, static_cast<double>(-12.001), -24.001, 10);
  RD(int, -12, double, static_cast<double>(-12), -24, 10);
  RD(int, -5, double, static_cast<double>(-12), -17, 10);
  RD(int, -5, double, static_cast<double>(12), 7, 10);
  RD(int, 5, double, static_cast<double>(-12), -7, 10);
  RD(int, 5, double, static_cast<double>(5), 10, 10);
  RD(int, 5, double, static_cast<double>(5.001), 10.001, 10);
  RD(int, 5, double, static_cast<double>(12), 17, 10);

  RD(smallint, 32767, float, static_cast<float>(123.45),
      static_cast<double>(32890.4499969), 10);
  RD(smallint, 0, float, static_cast<float>(123.45),
      static_cast<double>(123.449996948), 10);
  RD(smallint, -23, float, static_cast<float>(123.45),
      static_cast<double>(100.449996948), 10);
  RD(smallint, 45, float, static_cast<float>(123.45),
      static_cast<double>(168.449996948), 10);
  RD(smallint, 32722, float, static_cast<float>(123.45),
      static_cast<double>(32845.4499969), 10);
  RD(smallint, -32745, float, static_cast<float>(123.45),
      static_cast<double>(-32621.5500031), 10);

  //basic test
  R(float, static_cast<float>(45645645.67), int, 43243, double, static_cast<double>(45688887));
  R(float, static_cast<float>(34234.34534), uint64, 323121231, double, static_cast<double>(323155465.34375));
  R(float, static_cast<float>(24343.676), double, static_cast<double>(6767975.232), double, static_cast<double>(6792318.9077812498));
  R(double, static_cast<double>(354543.5454), int, 3546567, double, static_cast<double>(3901110.5454));
  R(double, static_cast<double>(5765756.68), uint64, 5645743, double, static_cast<double>(11411499.68));

  // double_float
  R(double, -123.0, double, -123.0, double, -246.0);
  R(float, -123.0, double, -123.0, double, -246.0);
  R(float, -123.0, float, -123.0, double, -246.0);

  // float_int_uint
  R(float, static_cast<float>(INT64_MIN), int, INT64_MIN, double, static_cast<double>(INT64_MIN) + static_cast<double>(INT64_MIN));
  R(float, static_cast<float>(INT64_MIN), int, 0, double, static_cast<double>(INT64_MIN));
  R(float, static_cast<float>(INT64_MIN), int, INT64_MAX, double, static_cast<double>(INT64_MIN) + static_cast<double>(INT64_MAX));
  R(float, 0, int, INT64_MIN, double, static_cast<double>(INT64_MIN));
  R(float, 0, int, 0, double, 0);
  R(float, 0, int, INT64_MAX, double, static_cast<double>(INT64_MAX));
  R(float, static_cast<float>(INT64_MAX), int, INT64_MIN, double, static_cast<double>(INT64_MAX) + static_cast<double>(INT64_MIN));
  R(float, static_cast<float>(INT64_MAX), int, 0, double, static_cast<double>(INT64_MAX));
  R(float, static_cast<float>(INT64_MAX), int, INT64_MAX, double, static_cast<double>(INT64_MAX) + static_cast<double>(INT64_MAX));
  R(float, static_cast<float>(UINT64_MAX), int, INT64_MIN, double, static_cast<double>(UINT64_MAX) + static_cast<double>(INT64_MIN));
  R(float, static_cast<float>(UINT64_MAX), int, 0, double, static_cast<double>(UINT64_MAX));
  R(float, static_cast<float>(UINT64_MAX), int, INT64_MAX, double, static_cast<double>(UINT64_MAX) + static_cast<double>(INT64_MAX));
  R(float, static_cast<float>(UINT64_MAX), uint64, UINT64_MAX, double, static_cast<double>(UINT64_MAX) + static_cast<double>(UINT64_MAX));

  // double_int_uint
  R(double, static_cast<double>(INT64_MIN), int, INT64_MIN, double, static_cast<double>(INT64_MIN) + static_cast<double>(INT64_MIN));
  R(double, static_cast<double>(INT64_MIN), int, 0, double, static_cast<double>(INT64_MIN));
  R(double, static_cast<double>(INT64_MIN), int, INT64_MAX, double, static_cast<double>(INT64_MIN) + static_cast<double>(INT64_MAX));
  R(double, 0, int, INT64_MIN, double, static_cast<double>(INT64_MIN));
  R(double, 0, int, 0, double, 0);
  R(double, 0, int, INT64_MAX, double, static_cast<double>(INT64_MAX));
  R(double, static_cast<double>(INT64_MAX), int, INT64_MIN, double, static_cast<double>(INT64_MAX) + static_cast<double>(INT64_MIN));
  R(double, static_cast<double>(INT64_MAX), int, 0, double, static_cast<double>(INT64_MAX));
  R(double, static_cast<double>(INT64_MAX), int, INT64_MAX, double, static_cast<double>(INT64_MAX) + static_cast<double>(INT64_MAX));
  R(double, static_cast<double>(UINT64_MAX), int, INT64_MIN, double, static_cast<double>(UINT64_MAX) + static_cast<double>(INT64_MIN));
  R(double, static_cast<double>(UINT64_MAX), int, 0, double, static_cast<double>(UINT64_MAX));
  R(double, static_cast<double>(UINT64_MAX), int, INT64_MAX, double, static_cast<double>(UINT64_MAX) + static_cast<double>(INT64_MAX));
  R(double, static_cast<double>(UINT64_MAX), uint64, UINT64_MAX, double, static_cast<double>(UINT64_MAX) + static_cast<double>(UINT64_MAX));

  //overflow/underflow
  FE(double, 1.79769e+308, double, 1.79769e+308);
  FE(double, 1.79769e+308, double, 1e+303);
  RD(double, 1.79769e+308, double, 1e+302, 1.79769e+308, 6);

  FE(double, -1.79769e+308, double, -1.79769e+308);
  FE(double, -1.79769e+308, double, -1e+303);
  RD(double, -1.79769e+308, double, -1e+302, -1.79769e+308, 6);

  //null test
  R(null, , float, static_cast<float>(123.45), null, );
  R(float, static_cast<float>(123.45), null, , null, );

  R(null, , double, 123.45, null, );
  R(double, 123.45, null, , null, );
}

TEST_F(ObExprAddTest, number_test)
{
  ObMalloc buf;
  number::ObNumber num123_p;
  num123_p.from("123.0", buf);
  number::ObNumber num123_n;
  num123_n.from("-123.0", buf);
  number::ObNumber num_zero;
  num_zero.from("0", buf);
  number::ObNumber num246_p;
  num246_p.from("246", buf);
  number::ObNumber num246_n;
  num246_n.from("-246", buf);

  //basic test
  R(number, num123_n, int, -123, number, num246_n);
  R(number, num123_n, int, 123, number, num_zero);
  R(number, num123_p, int, 123, number, num246_p);
  R(number, num123_n, uint64, 123, number, num_zero);
  R(number, num123_p, uint64, 123, number, num246_p);
  R(number, num123_n, float, -123.0, number, num246_n);
  R(number, num123_n, float, 123.0, number, num_zero);
  R(number, num123_p, float, 123.0, number, num246_p);
  R(number, num123_n, double, -123.0, number, num246_n);
  R(number, num123_n, double, 123.0, number, num_zero);
  R(number, num123_p, double, 123.0, number, num246_p);
  R(number, num123_n, number, num123_n, number, num246_n);
  R(number, num123_n, number, num123_p, number, num_zero);
  R(number, num123_p, number, num123_p, number, num246_p);
  R(number, num123_n, number, num123_n, number, num246_n);

  //long decimal
  //precision overflow
  //value overflow/underflow
  number::ObNumber n1, n2, n3;
  n1.from("99999999999999999999999999999999999999999999999999999999999999999", buf);
  n2.from("3", buf);
  n3.from("100000000000000000000000000000000000000000000000000000000000000002", buf);
  R(number, n1, number, n2, number, n3);
  n1.from("9999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999", buf);
  n3.from("10000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000002", buf);
  R(number, n1, number, n2, number, n3);
  //null test
  R(number, num123_n, null, , null, );
  R(null, , number, num123_n, null, );
}

TEST_F(ObExprAddTest, flow_test)
{
  ObMalloc buf;
  // int_int
  FE(int, INT64_MAX, int, INT64_MAX);
  FE(int, INT64_MIN, int, -2);

  //int_uint
  FE(int, INT64_MAX, uint64, UINT64_MAX);
  FE(uint64, UINT64_MAX, int, 2);
  FE(int, INT64_MIN, uint64, 5);
  FE(uint64, 100, int, -200);

  // uint_uint
  FE(uint64, UINT64_MAX, uint64, UINT64_MAX);
  FE(uint64, 100, uint64, UINT64_MAX);
  FE(uint64, UINT64_MAX, uint64, 100);
}

TEST_F(ObExprAddTest, int_string_test)
{
  ObMalloc buf;

  // int_string normal
  R(int, 16413, varchar, "29168", double, 45581);
  R(int, 900, varchar, "32591", double, 33491);
  R(int, 18762, varchar, "1655", double, 20417);
  R(int, 17410, varchar, "6359", double, 23769);
  R(int, 9161, varchar, "-18636", double, -9475);
  R(int, 22355, varchar, "-24767", double, -2412);
  R(int, 23655, varchar, "-15574", double, 8081);
  R(int, 4031, varchar, "-12052", double, -8021);
  R(int, 27350, varchar, "-1150", double, 26200);
  R(int, 16941, varchar, "-21724", double, -4783);
  R(int, 13966, varchar, "-3430", double, 10536);

  // int_string alpha
  //R(int, 10, varchar, "a0.22222", double, 10);
  //R(int, 10, varchar, "0.2a2222", double, 10.2);
  //R(int, 10, varchar, "-0.2a2222", double, 9.8);
  //R(int, 10, varchar, "-3430a", double, -3420);
  //R(int, 10, varchar, "-b3430a", double, 10);
  //R(int, 10, varchar, "-.b3430a", double, 10);
  //R(int, 10, varchar, "10b.3430a", double, 20);
  //R(int, 10, varchar, "10-b.3430a", double, 20);

  // uint_string
  //R(uint64, 10, varchar, "a0.22222", double, 10);
  ////R(uint64, 10, varchar, "0.2a2222", double, 10.2);
  //R(uint64, 10, varchar, "-0.2a2222", double, 9.8);
  //R(uint64, 10, varchar, "-3430a", double, -3420);
  //R(uint64, 10, varchar, "-b3430a", double, 10);
  //R(uint64, 10, varchar, "-.b3430a", double, 10);
  //R(uint64, 10, varchar, "10b.3430a", double, 20);
  //R(uint64, 10, varchar, "10-b.3430a", double, 20);

  //null test
  R(varchar, "1234", null, , null, );
  R(null, , varchar, "1234", null, );
}

TEST_F(ObExprAddTest, double_string_test)
{
  ObMalloc buf;

  // double_string normal
  R(double, 173.125, varchar, "291.2354", double, 464.3604);
  R(double, 200.2, varchar, "-23.376", double, 176.82399999999998);
  R(double, -0.2834, varchar, "-23.376", double, -23.6594);
  R(double, 321.2834, varchar, "-23.376", double, 297.9074);

  // double_string alpha
  //R(double, 31.33, varchar, "-23a.376", double, 8.329999999999998);
  //R(double, 31.33, varchar, "a-23a.376", double, 31.33);
  //R(double, -209.33, varchar, "-6.2c02", double, -215.53);
  ///R(double, 1209.33, varchar, "-.a6.2c02", double, 1209.33);
  //R(double, 1209.33, varchar, "-0.a6.2c02", double, 1209.33);
  //R(double, 1209.33, varchar, "-01.a6.2c02", double, 1208.33);

}

TEST_F(ObExprAddTest, float_string_test)
{
  //ObMalloc buf;

  // float_string normal
  //RD(float, static_cast<float>(173.125), varchar, "291.2354", 464.3604, 8);
  //RD(float, static_cast<float>(200.2), varchar, "-23.376", 176.82399999999998, 8);
  //RD(float, static_cast<float>(-0.2834), varchar, "-23.376", -23.6594, 8);
  //RD(float, static_cast<float>(321.283), varchar, "-23.376", 297.9069895, 8);

  // float_string alpha
  //RD(float, static_cast<float>(31.33), varchar, "-23a.376", 8.329999999999998, 8);
  //RD(float, static_cast<float>(31.33), varchar, "a-23a.376", 31.33, 8);
  //RD(float, static_cast<float>(-209.33), varchar, "-6.2c02", -215.53, 8);
  //RD(float, static_cast<float>(1209.33), varchar, "-.a6.2c02", 1209.33, 8);
  //RD(float, static_cast<float>(1209.33), varchar, "-0.a6.2c02", 1209.33, 8);
  //RD(float, static_cast<float>(1209.33), varchar, "-01.a6.2c02", 1208.33, 8);

}

// datetime + int is decimal, so we cant't do this test now.
/*
TEST_F(ObExprAddTest, datetime_test)
{
  ObMalloc buf;
  //Currently only support string -> datetime promotion
  //So date series cannot pariticipate in addition with numbers.
  //MySQL support them all, so list here.
  R(datetime, 1, int, 1, int, 19700101000001);
}
*/

/*TEST_F(ObExprAddTest, calc_type)
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
 T(ObIntType, ObIntType, ObIntType);
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

 TEST_F(ObExprAddTest, calc_result)
 {
 //ObNullType
 ObExprStringBuf buf;
 R(null, 0, null, 1, ObNullType);
 R(null, 0, int, 2, ObNullType);
 R(null, 0, float, 3.0, ObNullType);
 R(null, 0, double, 4.0, ObNullType);
 R(null, 0, datetime, 5, OB_ERR_INVALID_TYPE_FOR_OP);
 R(null, 0, precise_datetime, 6, OB_ERR_INVALID_TYPE_FOR_OP);
 R(null, 0, varchar, "7", ObNullType);
 R(null, 0, ctime, 9, OB_ERR_INVALID_TYPE_FOR_OP);
 R(null, 0, mtime, 10, OB_ERR_INVALID_TYPE_FOR_OP);
 R(null, 0, ext, 2, OB_ERR_INVALID_TYPE_FOR_OP);
 R(null, 0, bool, 1, OB_ERR_INVALID_TYPE_FOR_OP);
 //R(null, 0, , 2, ObNullType); decimal not supported


 //ObIntType
 R(int, 0, null, 1, ObNullType);
 R(int, 1, int, 2, 3);
 R(int, 2, float, 3.0, 5.0);
 R(int, 3, double, 4.0, 7.0);
 R(int, 4, datetime, 5, OB_ERR_INVALID_TYPE_FOR_OP);
 R(int, 5, precise_datetime, 6, OB_ERR_INVALID_TYPE_FOR_OP);
 R(int, 6, varchar, "7", 13.0);
 R(int, 6, varchar, "a", OB_ERR_CAST_VARCHAR_TO_NUMBER);
 R(int, 6, varchar, "07a", OB_ERR_CAST_VARCHAR_TO_NUMBER);
 R(int, 8, ctime, 9, OB_ERR_INVALID_TYPE_FOR_OP);
 R(int, 9, mtime, 10, OB_ERR_INVALID_TYPE_FOR_OP);
 R(int, 10, ext, 2, OB_ERR_INVALID_TYPE_FOR_OP);
 R(int, 11, bool, 1, OB_ERR_INVALID_TYPE_FOR_OP);
 R(int, 11, bool, 0, OB_ERR_INVALID_TYPE_FOR_OP);
 R(int, 9223372036854775807, int, 2, OB_OPERATE_OVERFLOW);
 //R(null, 0, , 2, ObNullType); decimal not supported

 //ObFloatType
 R(float, 0.0, null, 1, ObNullType);
 R(float, 1.0, int, 2, 3.0);
 R(float, 2.0, float, 3.0, 5.0);
 R(float, 3.0, double, 4.0, 7.0);
 R(float, 4.0, datetime, 5, OB_ERR_INVALID_TYPE_FOR_OP);
 R(float, 5.0, precise_datetime, 6, OB_ERR_INVALID_TYPE_FOR_OP);
 R(float, 6.0, varchar, "7", 13.0);
 R(float, 6.0, varchar, "a", OB_ERR_CAST_VARCHAR_TO_NUMBER);
 R(float, 6.0, varchar, "07a", OB_ERR_CAST_VARCHAR_TO_NUMBER);
 R(float, 6.0, varchar, "0.7a", OB_ERR_CAST_VARCHAR_TO_NUMBER);
 R(float, 8.0, ctime, 9, OB_ERR_INVALID_TYPE_FOR_OP);
 R(float, 9.0, mtime, 10, OB_ERR_INVALID_TYPE_FOR_OP);
 R(float, 10.0, ext, 2, OB_ERR_INVALID_TYPE_FOR_OP);
 R(float, 11.0, bool, 1, OB_ERR_INVALID_TYPE_FOR_OP);
 R(float, 11.0, bool, 0, OB_ERR_INVALID_TYPE_FOR_OP);
 //R(null, 0, , 2, ObNullType); decimal not supported

 //ObDoubleType
 R(double, 0.0, null, 1, ObNullType);
 R(double, 1.0, int, 2, 3.0);
 R(double, 2.0, float, 3.0, 5.0);
 R(double, 3.0, double, 4.0, 7.0);
 R(double, 4.0, datetime, 5, OB_ERR_INVALID_TYPE_FOR_OP);
 R(double, 5.0, precise_datetime, 6, OB_ERR_INVALID_TYPE_FOR_OP);
 R(double, 6.0, varchar, "7", 13.0);
 R(double, 6.0, varchar, "a", OB_ERR_CAST_VARCHAR_TO_NUMBER);
 R(double, 6.0, varchar, "07a", OB_ERR_CAST_VARCHAR_TO_NUMBER);
 R(float, 6.0, varchar, "0.7a", OB_ERR_CAST_VARCHAR_TO_NUMBER);
 R(double, 8.0, ctime, 9, OB_ERR_INVALID_TYPE_FOR_OP);
 R(double, 9.0, mtime, 10, OB_ERR_INVALID_TYPE_FOR_OP);
 R(double, 10.0, ext, 2, OB_ERR_INVALID_TYPE_FOR_OP);
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
 R(datetime, 10, ext, 2, OB_ERR_INVALID_TYPE_FOR_OP);
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
 R(precise_datetime, 10, ext, 2, OB_ERR_INVALID_TYPE_FOR_OP);
 R(precise_datetime, 11, bool, 1, OB_ERR_INVALID_TYPE_FOR_OP);
 R(precise_datetime, 11, bool, 0, OB_ERR_INVALID_TYPE_FOR_OP);
 //R(null, 0, , 2, ObNullType); decimal not supported

 //ObVarcharType
 R(varchar, "0", null, 1, ObNullType);
 R(varchar, "1", int, 2, 3);
 R(varchar, "2", float, 3.0, 5.0);
 R(varchar, "2.12345", float, 3.0, 5.12345);
 R(varchar, "3", double, 4.0, 7.0);
 R(varchar, "3.12345", double, 4.0, 7.12345);
 R(varchar, "4", datetime, 5, OB_ERR_INVALID_TYPE_FOR_OP);
 R(varchar, "5", precise_datetime, 6, OB_ERR_INVALID_TYPE_FOR_OP);
 R(varchar, "6", varchar, "7", 13);
 R(varchar, "6", varchar, "a", OB_ERR_CAST_VARCHAR_TO_NUMBER);
 R(varchar, "6", varchar, "07a", OB_ERR_CAST_VARCHAR_TO_NUMBER);
 R(varchar, "8", ctime, 9, OB_ERR_INVALID_TYPE_FOR_OP);
 R(varchar, "9", mtime, 10, OB_ERR_INVALID_TYPE_FOR_OP);
 R(varchar, "10", ext, 2, OB_ERR_INVALID_TYPE_FOR_OP);
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
 R(ctime, 10, ext, 2, OB_ERR_INVALID_TYPE_FOR_OP);
 R(ctime, 11, bool, 1, OB_ERR_INVALID_TYPE_FOR_OP);
 R(ctime, 11, bool, 0, OB_ERR_INVALID_TYPE_FOR_OP);
 //R(null, 0, , 2, OB_ERR_INVALID_TYPE_FOR_OP);

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
 R(mtime, 10, ext, 2, OB_ERR_INVALID_TYPE_FOR_OP);
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
 R(bool, 10, ext, 2, OB_ERR_INVALID_TYPE_FOR_OP);
 R(bool, 11, bool, 1, OB_ERR_INVALID_TYPE_FOR_OP);
 R(bool, 11, bool, 0, OB_ERR_INVALID_TYPE_FOR_OP);
 //R(null, 0, , 2, ObNullType); decimal not supported

 //ObExtendType
 R(ext, 0, null, 1, OB_ERR_INVALID_TYPE_FOR_OP);
 R(ext, 0, int, 2, OB_ERR_INVALID_TYPE_FOR_OP);
 R(ext, 0, float, 3.0, OB_ERR_INVALID_TYPE_FOR_OP);
 R(ext, 0, double, 4.0, OB_ERR_INVALID_TYPE_FOR_OP);
 R(ext, 0, datetime, 5, OB_ERR_INVALID_TYPE_FOR_OP);
 R(ext, 0, precise_datetime, 6, OB_ERR_INVALID_TYPE_FOR_OP);
 R(ext, 0, varchar, "7", OB_ERR_INVALID_TYPE_FOR_OP);
 R(ext, 0, varchar, "a", OB_ERR_INVALID_TYPE_FOR_OP);
 R(ext, 0, varchar, "07a", OB_ERR_INVALID_TYPE_FOR_OP);
 R(ext, 0, ctime, 9, OB_ERR_INVALID_TYPE_FOR_OP);
 R(ext, 0, mtime, 10, OB_ERR_INVALID_TYPE_FOR_OP);
 R(ext, 0, ext, 2, OB_ERR_INVALID_TYPE_FOR_OP);
 R(ext, 0, bool, 1, OB_ERR_INVALID_TYPE_FOR_OP);
 R(ext, 0, bool, 0, OB_ERR_INVALID_TYPE_FOR_OP);
 //R(null, 0, , 2, ObNullType); decimal not supported

 //ObUnknownType
 //R(unknown, 0, null, 1, OB_ERR_INVALID_TYPE_FOR_OP);
 //R(unknown, 0, int, 2, OB_ERR_INVALID_TYPE_FOR_OP);
 //R(unknown, 0, float, 3.0, OB_ERR_INVALID_TYPE_FOR_OP);
 //R(unknown, 0, double, 4.0, OB_ERR_INVALID_TYPE_FOR_OP);
 //R(unknown, 0, datetime, 5, OB_ERR_INVALID_TYPE_FOR_OP);
 //R(unknown, 0, precise_datetime, 6, OB_ERR_INVALID_TYPE_FOR_OP);
 //R(unknown, 0, varchar, "7", OB_ERR_INVALID_TYPE_FOR_OP);
 //R(unknown, 0, varchar, "a", OB_ERR_INVALID_TYPE_FOR_OP);
 //R(unknown, 0, varchar, "07a", OB_ERR_INVALID_TYPE_FOR_OP);
 //R(unknown, 0, unknown, 0, OB_ERR_INVALID_TYPE_FOR_OP);
 //R(unknown, 0, ctime, 9, OB_ERR_INVALID_TYPE_FOR_OP);
 //R(unknown, 0, mtime, 10, OB_ERR_INVALID_TYPE_FOR_OP);
 //R(unknown, 0, ext, 2, OB_ERR_INVALID_TYPE_FOR_OP);
 //R(unknown, 0, bool, 1, OB_ERR_INVALID_TYPE_FOR_OP);
 //R(unknown, 0, bool, 0, OB_ERR_INVALID_TYPE_FOR_OP);
 //R(null, 0, , 2, ObNullType); decimal not supported

 }
 */

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
 // return RUN_ALL_TESTS();
}
