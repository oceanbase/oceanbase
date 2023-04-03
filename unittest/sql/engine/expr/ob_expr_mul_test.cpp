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

#include <float.h>
#include <gtest/gtest.h>
#include "ob_expr_test_utils.h"
#include "sql/engine/expr/ob_expr_mul.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

class ObExprMulTest: public ::testing::Test
{
  public:
    ObExprMulTest();
    virtual ~ObExprMulTest();
    virtual void SetUp();
    virtual void TearDown();
  private:
    // disallow copy
    ObExprMulTest(const ObExprMulTest &other);
    ObExprMulTest& operator=(const ObExprMulTest &other);
  protected:
    // data members
};

ObExprMulTest::ObExprMulTest()
{
}

ObExprMulTest::~ObExprMulTest()
{
}

void ObExprMulTest::SetUp()
{
}

void ObExprMulTest::TearDown()
{
}

#define MUL_EXPECT_OVERFLOW(cmp_op, str_buf, func, type1, v1, type2, v2) \
{                                                     \
  ObObj t1;                                       \
  ObObj t2;                                       \
  ObObj vres;                                     \
  ObObj resf;                                     \
  ObArenaAllocator alloc;\
  cmp_op op(alloc);                                          \
  t1.set_##type1(v1);                                 \
  t1.set_collation_type(CS_TYPE_UTF8MB4_BIN);         \
  t2.set_##type2(v2);                                 \
  t2.set_collation_type(CS_TYPE_UTF8MB4_BIN);         \
  int err = op.func(vres, t1, t2, str_buf, -1);          \
  SQL_LOG(INFO, "output err", K(err));                \
  EXPECT_TRUE(OB_SUCCESS != err);            \
} while(0)

#define R(t1, v1, t2, v2, rt, rv) ARITH_EXPECT_OBJ(ObExprMul, &buf, calc, t1, v1, t2, v2, rt, rv)
#define FE(t1, v1, t2, v2) MUL_EXPECT_OVERFLOW(ObExprMul, &buf, calc, t1, v1, t2, v2)
#define RD(t1, v1, t2, v2, res, p) ARITH_EXPECT_OBJ_DOUBLE(ObExprMul, &buf, calc, t1, v1, t2, v2, res, p)
#define RN(v1, v2, v3) \
do { \
  num1.from(#v1, buf); \
  num2.from(#v2, buf); \
  num3.from(#v3, buf); \
  R(number, num1, number, num2, number, num3); \
} while (0)
#define RN1(v1, t2, v2, res) \
do {   \
  num1.from(#v1, buf);  \
  num3.from(#res, buf); \
  R(number, num1, t2, v2, number, num3); \
} while(0)
#define RN2(t1, v1, v2, res) \
do {   \
  num2.from(#v2, buf);  \
  num3.from(#res, buf); \
  R(t1, v1, number, num2, number, num3); \
} while(0)


TEST_F(ObExprMulTest, int_int)
{
  ObMalloc buf;
  //small tiny medium
  R(tinyint, 10, tinyint, 20, int, 200);
  R(smallint, 30, smallint, 40, int, 1200);
  R(mediumint, 50, mediumint, 60, int, 3000);
  R(tinyint, 10, smallint, -20, int, -200);
  R(smallint, -30, mediumint, 40, int, -1200);
  R(mediumint, -50, tinyint, -60, int, 3000);
  R(int32, -70, int32, 80, int, -5600);
  R(int32, 70, tinyint, -80, int, -5600);
  //int int
  R(int, -1024, int, -1024, int, 1048576);
  R(int, -1024, int, -1, int, 1024);
  R(int, -1024, int, 0, int, 0);
  R(int, -1024, int, 1, int, -1024);
  R(int, -1024, int, 1024, int, -1048576);
  R(int, -1, int, -1024, int, 1024);
  R(int, -1, int, -1, int, 1);
  R(int, -1, int, 0, int, 0);
  R(int, -1, int, 1, int, -1);
  R(int, -1, int, 1024, int, -1024);
  R(int, 0, int, -1024, int, 0);
  R(int, 0, int, -1, int, 0);
  R(int, 0, int, 0, int, 0);
  R(int, 0, int, 1, int, 0);
  R(int, 0, int, 1024, int, 0);
  R(int, 1, int, -1024, int, -1024);
  R(int, 1, int, -1, int, -1);
  R(int, 1, int, 0, int, 0);
  R(int, 1, int, 1, int, 1);
  R(int, 1, int, 1024, int, 1024);
  R(int, 1024, int, -1024, int, -1048576);
  R(int, 1024, int, -1, int, -1024);
  R(int, 1024, int, 0, int, 0);
  R(int, 1024, int, 1, int, 1024);
  R(int, 1024, int, 1024, int, 1048576);
  //edge
  FE(int, INT64_MIN, int, INT64_MIN);
  FE(int, INT64_MIN, int, -1);
  FE(int, INT64_MIN, int, -2);
  R(int, INT64_MIN, int, 0, int, 0);
  FE(int, -1, int, INT64_MIN);
  FE(int, -2, int, INT64_MIN);
  R(int, 0, int, INT64_MIN, int, 0);
  R(int, 1, int, INT64_MIN, int, INT64_MIN);
  FE(int, 2, int, INT64_MIN);
  FE(int, INT64_MAX, int, INT64_MIN);
  R(int, INT64_MAX, int, -1, int, INT64_MIN + 1);
  FE(int, INT64_MAX, int, -2);
  R(int, INT64_MAX, int, 0, int, 0);
}


TEST_F(ObExprMulTest, int_uint)
{
  ObMalloc buf;
  //small tiny medium
  R(tinyint, 10, utinyint, 20, uint64, 200);
  R(smallint, 30, usmallint, 40, uint64, 1200);
  R(mediumint, 50, umediumint, 60, uint64, 3000);
  R(utinyint, 10, tinyint, 20, uint64, 200);
  R(usmallint, 30, smallint, 40, uint64, 1200);
  R(umediumint, 50, mediumint, 60, uint64, 3000);
  R(tinyint, 10, usmallint, 20, uint64, 200);
  R(smallint, 30, umediumint, 40, uint64, 1200);
  R(mediumint, 50, uint32, 60, uint64, 3000);
  R(int32, 70, uint64, 80, uint64, 5600);
  R(int, 90, utinyint, 100, uint64, 9000);
  R(tinyint, 90, uint64, 80, uint64, 7200);
  R(int, 70, uint32, 60, uint64, 4200);
  R(int32, 50, umediumint, 40, uint64, 2000);
  R(mediumint, 30, usmallint, 20, uint64, 600);
  R(smallint, 10, utinyint, 90, uint64, 900);
  //int uint
  R(int, -2147483648, uint64, 0, uint64, 0);
  FE(int, -2147483648, uint64, 1);
  FE(int, -2147483648, uint64, 1024);
  FE(int, -2147483648, uint64, 4294967295);
  R(int, -1024, uint64, 0, uint64, 0);
  FE(int, -1024, uint64, 1);
  FE(int, -1024, uint64, 1024);
  FE(int, -1024, uint64, 4294967295);
  R(int, -1, uint64, 0, uint64, 0);
  FE(int, -1, uint64, 1);
  FE(int, -1, uint64, 1024);
  FE(int, -1, uint64, 4294967295);
  R(int, 0, uint64, 0, uint64, 0);
  R(int, 0, uint64, 1, uint64, 0);
  R(int, 0, uint64, 1024, uint64, 0);
  R(int, 0, uint64, 4294967295, uint64, 0);
  R(int, 1, uint64, 0, uint64, 0);
  R(int, 1, uint64, 1, uint64, 1);
  R(int, 1, uint64, 1024, uint64, 1024);
  R(int, 1, uint64, 4294967295, uint64, 4294967295);
  R(int, 1024, uint64, 0, uint64, 0);
  R(int, 1024, uint64, 1, uint64, 1024);
  R(int, 1024, uint64, 1024, uint64, 1048576);
  R(int, 1024, uint64, 4294967295, uint64, 4398046510080);
  R(int, 2147483647, uint64, 0, uint64, 0);
  R(int, 2147483647, uint64, 1, uint64, 2147483647);
  R(int, 2147483647, uint64, 1024, uint64, 2199023254528);
  R(int, 2147483647, uint64, 4294967295, uint64, 9223372030412324865);
  //uint int
  R(uint64, 0, int, -2147483648, uint64, 0);
  FE(uint64, 1, int, -2147483648);
  FE(uint64, 1024, int, -2147483648);
  FE(uint64, 4294967295, int, -2147483648);
  R(uint64, 0, int, -1024, uint64, 0);
  FE(uint64, 1, int, -1024);
  FE(uint64, 1024, int, -1024);
  FE(uint64, 4294967295, int, -1024);
  R(uint64, 0, int, -1, uint64, 0);
  FE(uint64, 1, int, -1);
  FE(uint64, 1024, int, -1);
  FE(uint64, 4294967295, int, -1);
  R(uint64, 0, int, 0, uint64, 0);
  R(uint64, 1, int, 0, uint64, 0);
  R(uint64, 1024, int, 0, uint64, 0);
  R(uint64, 4294967295, int, 0, uint64, 0);
  R(uint64, 0, int, 1, uint64, 0);
  R(uint64, 1, int, 1, uint64, 1);
  R(uint64, 1024, int, 1, uint64, 1024);
  R(uint64, 4294967295, int, 1, uint64, 4294967295);
  R(uint64, 0, int, 1024, uint64, 0);
  R(uint64, 1, int, 1024, uint64, 1024);
  R(uint64, 1024, int, 1024, uint64, 1048576);
  R(uint64, 4294967295, int, 1024, uint64, 4398046510080);
  R(uint64, 0, int, 2147483647, uint64, 0);
  R(uint64, 1, int, 2147483647, uint64, 2147483647);
  R(uint64, 1024, int, 2147483647, uint64, 2199023254528);
  R(uint64, 4294967295, int, 2147483647, uint64, 9223372030412324865);
  //edge
  FE(int, 4294967296, uint64, 4294967296);
  FE(uint64, 4294967296, int, 4294967296);
  R(int, INT64_MIN, uint64, 0, uint64, 0);
  FE(int, INT64_MIN, uint64, 1);
  FE(int, INT64_MIN, uint64, 2);
  FE(int, INT64_MIN, uint64, UINT64_MAX);
  FE(int, -1, uint64, UINT64_MAX);
  FE(int, -2, uint64, UINT64_MAX);
  R(int, 0, uint64, UINT64_MAX, uint64, 0);
  R(int, 1, uint64, UINT64_MAX, uint64, UINT64_MAX);
  FE(int, 2, uint64, UINT64_MAX);
  R(int, INT64_MAX, uint64, 0, uint64, 0);
  R(int, INT64_MAX, uint64, 1, uint64, INT64_MAX);
  R(int, INT64_MAX, uint64, 2, uint64, UINT64_MAX - 1);
  FE(int, INT64_MAX, uint64, UINT64_MAX);
  R(uint64, 0, int, INT64_MIN, uint64, 0);
  FE(uint64, 1, int, INT64_MIN);
  FE(uint64, 2, int, INT64_MIN);
  FE(uint64, UINT64_MAX, int, INT64_MIN);
  FE(uint64, UINT64_MAX, int, -1);
  FE(uint64, UINT64_MAX, int, -2);
  R(uint64, UINT64_MAX, int, 0, uint64, 0);
  R(uint64, UINT64_MAX, int, 1, uint64, UINT64_MAX);
  FE(uint64, UINT64_MAX, int, 2);
  R(uint64, 0, int, INT64_MAX, uint64, 0);
  R(uint64, 1, int, INT64_MAX, uint64, INT64_MAX);
  R(uint64, 2, int, INT64_MAX, uint64, UINT64_MAX - 1);
  FE(uint64, UINT64_MAX, int, INT64_MAX);
}

TEST_F(ObExprMulTest, int_float)
{
  ObMalloc buf;
  //small tiny medium
  RD(tinyint, 10, float, 12.34f, 123.4, 5);
  RD(smallint, 20, float, -12.34f, -246.8, 5);
  RD(mediumint, -30, float, -12.34f, 370.2, 5);
  RD(int32, -40, float, 12.34f, -493.6, 5);
  RD(float, 12.34f, tinyint, 10, 123.4, 5);
  RD(float, -12.34f, smallint, 20, -246.8, 5);
  RD(float, -12.34f, mediumint, -30, 370.2, 5);
  RD(float, 12.34f, int32, -40, -493.6, 5);
  //int float
  RD(float, static_cast<float>(0), int, 0, 0, 5);
  RD(float, static_cast<float>(0), int, 123, 0, 5);
  RD(float, static_cast<float>(0), int, -123, 0, 5);
  RD(float, static_cast<float>(0), int, 2147483647, 0, 5);
  RD(float, static_cast<float>(0), int, -2147483648, 0, 5);
  RD(float, static_cast<float>(123.45), int, 0, 0, 5);
  RD(float, static_cast<float>(123.45), int, 123, 15184.35, 5);
  RD(float, static_cast<float>(123.45), int, -123, -15184.35, 5);
  RD(float, static_cast<float>(123.45), int, 2147483647, 265106856222.15, 5);
  RD(float, static_cast<float>(123.45), int, -2147483648, -265106856345.6, 5);
  RD(float, static_cast<float>(-123.45), int, 0, 0, 5);
  RD(float, static_cast<float>(-123.45), int, 123, -15184.35, 5);
  RD(float, static_cast<float>(-123.45), int, -123, 15184.35, 5);
  RD(float, static_cast<float>(-123.45), int, 2147483647, -265106856222.15, 5);
  RD(float, static_cast<float>(-123.45), int, -2147483648, 265106856345.6, 5);
  RD(float, static_cast<float>(3.4E+38), int, 0, 0, 5);
  RD(float, static_cast<float>(3.4E+38), int, 123, 4.182E+40, 5);
  RD(float, static_cast<float>(3.4E+38), int, -123, -4.182E+40, 5);
  RD(float, static_cast<float>(3.4E+38), int, 2147483647, 7.3014443998E+47, 5);
  RD(float, static_cast<float>(3.4E+38), int, -2147483648, -7.3014444032E+47, 5);
  RD(float, static_cast<float>(-3.4E+38), int, 0, 0, 5);
  RD(float, static_cast<float>(-3.4E+38), int, 123, -4.182E+40, 5);
  RD(float, static_cast<float>(-3.4E+38), int, -123, 4.182E+40, 5);
  RD(float, static_cast<float>(-3.4E+38), int, 2147483647, -7.3014443998E+47, 5);
  RD(float, static_cast<float>(-3.4E+38), int, -2147483648, 7.3014444032E+47, 5);
  //float int
  RD(int, 0, float, static_cast<float>(0), 0, 5);
  RD(int, 123, float, static_cast<float>(0), 0, 5);
  RD(int, -123, float, static_cast<float>(0), 0, 5);
  RD(int, 2147483647, float, static_cast<float>(0), 0, 5);
  RD(int, -2147483648, float, static_cast<float>(0), 0, 5);
  RD(int, 0, float, static_cast<float>(123.45), 0, 5);
  RD(int, 123, float, static_cast<float>(123.45), 15184.35, 5);
  RD(int, -123, float, static_cast<float>(123.45), -15184.35, 5);
  RD(int, 2147483647, float, static_cast<float>(123.45), 265106856222.15, 5);
  RD(int, -2147483648, float, static_cast<float>(123.45), -265106856345.6, 5);
  RD(int, 0, float, static_cast<float>(-123.45), 0, 5);
  RD(int, 123, float, static_cast<float>(-123.45), -15184.35, 5);
  RD(int, -123, float, static_cast<float>(-123.45), 15184.35, 5);
  RD(int, 2147483647, float, static_cast<float>(-123.45), -265106856222.15, 5);
  RD(int, -2147483648, float, static_cast<float>(-123.45), 265106856345.6, 5);
  RD(int, 0, float, static_cast<float>(3.4E+38), 0, 5);
  RD(int, 123, float, static_cast<float>(3.4E+38), 4.182E+40, 5);
  RD(int, -123, float, static_cast<float>(3.4E+38), -4.182E+40, 5);
  RD(int, 2147483647, float, static_cast<float>(3.4E+38), 7.3014443998E+47, 5);
  RD(int, -2147483648, float, static_cast<float>(3.4E+38), -7.3014444032E+47, 5);
  RD(int, 0, float, static_cast<float>(-3.4E+38), 0, 5);
  RD(int, 123, float, static_cast<float>(-3.4E+38), -4.182E+40, 5);
  RD(int, -123, float, static_cast<float>(-3.4E+38), 4.182E+40, 5);
  RD(int, 2147483647, float, static_cast<float>(-3.4E+38), -7.3014443998E+47, 5);
  RD(int, -2147483648, float, static_cast<float>(-3.4E+38), 7.3014444032E+47, 5);
  //edge
  RD(int, INT64_MAX, float, FLT_MAX, static_cast<double>(INT64_MAX) * FLT_MAX, 5);
  RD(int, -INT64_MAX, float, FLT_MAX, static_cast<double>(INT64_MAX) * -FLT_MAX, 5);
  RD(int, INT64_MAX, float, -FLT_MAX, static_cast<double>(INT64_MAX) * -FLT_MAX, 5);
  RD(int, -INT64_MAX, float, -FLT_MAX, static_cast<double>(INT64_MAX) * FLT_MAX, 5);
  RD(float, FLT_MAX, int, INT64_MAX, static_cast<double>(INT64_MAX) * FLT_MAX, 5);
  RD(float, FLT_MAX, int, -INT64_MAX, static_cast<double>(INT64_MAX) * -FLT_MAX, 5);
  RD(float, -FLT_MAX, int, INT64_MAX, static_cast<double>(INT64_MAX) * -FLT_MAX, 5);
  RD(float, -FLT_MAX, int, -INT64_MAX, static_cast<double>(INT64_MAX) * FLT_MAX, 5);
}


TEST_F(ObExprMulTest, int_double)
{
  ObMalloc buf;
  //small tiny medium
  RD(tinyint, 10, double, 12.34f, 123.4, 5);
  RD(smallint, 20, double, -12.34f, -246.8, 5);
  RD(mediumint, -30, double, -12.34f, 370.2, 5);
  RD(int32, -40, double, 12.34f, -493.6, 5);
  RD(double, 12.34f, tinyint, 10, 123.4, 5);
  RD(double, -12.34f, smallint, 20, -246.8, 5);
  RD(double, -12.34f, mediumint, -30, 370.2, 5);
  RD(double, 12.34f, int32, -40, -493.6, 5);
  //int double
  RD(int, 0, double, 0, 0, 5);
  RD(int, 123, double, 0, 0, 5);
  RD(int, -123, double, 0, 0, 5);
  RD(int, 2147483647, double, 0, 0, 5);
  RD(int, -2147483648, double, 0, 0, 5);
  RD(int, 0, double, 123.45, 0, 5);
  RD(int, 123, double, 123.45, 15184.35, 5);
  RD(int, -123, double, 123.45, -15184.35, 5);
  RD(int, 2147483647, double, 123.45, 265106856222.15, 5);
  RD(int, -2147483648, double, 123.45, -265106856345.6, 5);
  RD(int, 0, double, -123.45, 0, 5);
  RD(int, 123, double, -123.45, -15184.35, 5);
  RD(int, -123, double, -123.45, 15184.35, 5);
  RD(int, 2147483647, double, -123.45, -265106856222.15, 5);
  RD(int, -2147483648, double, -123.45, 265106856345.6, 5);
  RD(int, 0, double, 3.4E+38, 0, 5);
  RD(int, 123, double, 3.4E+38, 4.182E+40, 5);
  RD(int, -123, double, 3.4E+38, -4.182E+40, 5);
  RD(int, 2147483647, double, 3.4E+38, 7.3014443998E+47, 5);
  RD(int, -2147483648, double, 3.4E+38, -7.3014444032E+47, 5);
  RD(int, 0, double, -3.4E+38, 0, 5);
  RD(int, 123, double, -3.4E+38, -4.182E+40, 5);
  RD(int, -123, double, -3.4E+38, 4.182E+40, 5);
  RD(int, 2147483647, double, -3.4E+38, -7.3014443998E+47, 5);
  RD(int, -2147483648, double, -3.4E+38, 7.3014444032E+47, 5);
  //double int
  RD(double, 0, int, 0, 0, 5);
  RD(double, 0, int, 123, 0, 5);
  RD(double, 0, int, -123, 0, 5);
  RD(double, 0, int, 2147483647, 0, 5);
  RD(double, 0, int, -2147483648, 0, 5);
  RD(double, 123.45, int, 0, 0, 5);
  RD(double, 123.45, int, 123, 15184.35, 5);
  RD(double, 123.45, int, -123, -15184.35, 5);
  RD(double, 123.45, int, 2147483647, 265106856222.15, 5);
  RD(double, 123.45, int, -2147483648, -265106856345.6, 5);
  RD(double, -123.45, int, 0, 0, 5);
  RD(double, -123.45, int, 123, -15184.35, 5);
  RD(double, -123.45, int, -123, 15184.35, 5);
  RD(double, -123.45, int, 2147483647, -265106856222.15, 5);
  RD(double, -123.45, int, -2147483648, 265106856345.6, 5);
  RD(double, 3.4E+38, int, 0, 0, 5);
  RD(double, 3.4E+38, int, 123, 4.182E+40, 5);
  RD(double, 3.4E+38, int, -123, -4.182E+40, 5);
  RD(double, 3.4E+38, int, 2147483647, 7.3014443998E+47, 5);
  RD(double, 3.4E+38, int, -2147483648, -7.3014444032E+47, 5);
  RD(double, -3.4E+38, int, 0, 0, 5);
  RD(double, -3.4E+38, int, 123, -4.182E+40, 5);
  RD(double, -3.4E+38, int, -123, 4.182E+40, 5);
  RD(double, -3.4E+38, int, 2147483647, -7.3014443998E+47, 5);
  RD(double, -3.4E+38, int, -2147483648, 7.3014444032E+47, 5);
  //edge
  FE(int, 2, double, DBL_MAX);
  FE(int, -2, double, DBL_MAX);
  FE(int, 2, double, -DBL_MAX);
  FE(int, -2, double, -DBL_MAX);
  FE(double, DBL_MAX, int, 2);
  FE(double, DBL_MAX, int, -2);
  FE(double, -DBL_MAX, int, 2);
  FE(double, -DBL_MAX, int, -2);
  FE(int, INT_MAX, double, DBL_MAX);
  FE(int, INT_MIN, double, DBL_MAX);
  FE(int, INT_MAX, double, -DBL_MAX);
  FE(int, INT_MIN, double, -DBL_MAX);
  FE(double, DBL_MAX, int, INT_MAX);
  FE(double, DBL_MAX, int, INT_MIN);
  FE(double, -DBL_MAX, int, INT_MAX);
  FE(double, -DBL_MAX, int, INT_MIN);
}

TEST_F(ObExprMulTest, int_number)
{
  ObMalloc buf;
  number::ObNumber num1;
  number::ObNumber num2;
  number::ObNumber num3;
  //tiny small medium
  RN2(tinyint, 0, 0, 0);
  RN2(tinyint, 123, 0, 0);
  RN2(tinyint, -123, 0, 0);
  RN2(smallint, 0, 123.45, 0);
  RN2(smallint, 123, 123.45, 15184.35);
  RN2(smallint, -123, 123.45, -15184.35);
  RN2(mediumint, 0, -123.45, 0);
  RN2(mediumint, 123, -123.45, -15184.35);
  RN2(mediumint, -123, -123.45, 15184.35);
  RN1(0, mediumint, 0, 0);
  RN1(0, mediumint, 123, 0);
  RN1(0, mediumint, -123, 0);
  RN1(123.45, smallint, 0, 0);
  RN1(123.45, smallint, 123, 15184.35);
  RN1(123.45, smallint, -123, -15184.35);
  RN1(-123.45, tinyint, 0, 0);
  RN1(-123.45, tinyint, 123, -15184.35);
  RN1(-123.45, tinyint, -123, 15184.35);
  //int number
  RN2(int, 0, 0, 0);
  RN2(int, 123, 0, 0);
  RN2(int, -123, 0, 0);
  RN2(int, 0, 123.45, 0);
  RN2(int, 123, 123.45, 15184.35);
  RN2(int, -123, 123.45, -15184.35);
  RN2(int, 0, -123.45, 0);
  RN2(int, 123, -123.45, -15184.35);
  RN2(int, -123, -123.45, 15184.35);
  //number int
  RN1(0, int, 0, 0);
  RN1(0, int, 123, 0);
  RN1(0, int, -123, 0);
  RN1(123.45, int, 0, 0);
  RN1(123.45, int, 123, 15184.35);
  RN1(123.45, int, -123, -15184.35);
  RN1(-123.45, int, 0, 0);
  RN1(-123.45, int, 123, -15184.35);
  RN1(-123.45, int, -123, 15184.35);
  //edge
  //65 digit
  RN1(10000000000000000000000000000000000000000000000, int, INT64_MAX, 92233720368547758070000000000000000000000000000000000000000000000);
  RN2(int, INT64_MAX, 10000000000000000000000000000000000000000000000, 92233720368547758070000000000000000000000000000000000000000000000);
  //90 digit
  RN1(100000000000000000000000000000000000000000000000000000000000000000000000, int, INT64_MAX, 922337203685477580700000000000000000000000000000000000000000000000000000000000000000000000);
  RN2(int, INT64_MAX, 100000000000000000000000000000000000000000000000000000000000000000000000, 922337203685477580700000000000000000000000000000000000000000000000000000000000000000000000);
  //100 digit
  RN1(1000000000000000000000000000000000000000000000000000000000000000000000000000000000, int, INT64_MAX, 9223372036854775807000000000000000000000000000000000000000000000000000000000000000000000000000000000);
  RN2(int, INT64_MAX, 1000000000000000000000000000000000000000000000000000000000000000000000000000000000, 9223372036854775807000000000000000000000000000000000000000000000000000000000000000000000000000000000);
  //65 digit
  RN1(-10000000000000000000000000000000000000000000000, int, INT64_MAX, -92233720368547758070000000000000000000000000000000000000000000000);
  RN2(int, INT64_MAX, -10000000000000000000000000000000000000000000000, -92233720368547758070000000000000000000000000000000000000000000000);
  //90 digit
  RN1(-100000000000000000000000000000000000000000000000000000000000000000000000, int, INT64_MAX, -922337203685477580700000000000000000000000000000000000000000000000000000000000000000000000);
  RN2(int, INT64_MAX, -100000000000000000000000000000000000000000000000000000000000000000000000, -922337203685477580700000000000000000000000000000000000000000000000000000000000000000000000);
  //100 digit
  RN1(-1000000000000000000000000000000000000000000000000000000000000000000000000000000000, int, INT64_MAX, -9223372036854775807000000000000000000000000000000000000000000000000000000000000000000000000000000000);
  RN2(int, INT64_MAX, -1000000000000000000000000000000000000000000000000000000000000000000000000000000000, -9223372036854775807000000000000000000000000000000000000000000000000000000000000000000000000000000000);
}

/*
TEST_F(ObExprMulTest, int_string)
{
  ObMalloc buf;
  //small tiny medium
  RD(tinyint, 0, varchar, "10", 0, 5);
  RD(tinyint, 123, varchar, "20", 2460, 5);
  RD(smallint, -123, varchar, "30", -3690, 5);
  RD(smallint, -456, varchar, "1.5", -684, 5);
  RD(mediumint, 789, varchar, "123.456", 97406.784, 5);
  RD(mediumint, 890, varchar, "789.12", 702316.8, 5);
  RD(int32, 123, varchar, "123.45", 15184.35, 5);
  RD(int32, -123, varchar, "123.45", -15184.35, 5);
  RD(varchar, "10", tinyint, 0, 0, 5);
  RD(varchar, "20", tinyint, 123, 2460, 5);
  RD(varchar, "30", smallint, -123, -3690, 5);
  RD(varchar, "1.5", smallint, -456, -684, 5);
  RD(varchar, "123.456", mediumint, 789, 97406.784, 5);
  RD(varchar, "789.12", mediumint, 890, 702316.8, 5);
  RD(varchar, "123.45", int32, 123, 15184.35, 5);
  RD(varchar, "123.45", int32, -123, -15184.35, 5);
  //int string
  RD(int, 0, varchar, "0", 0, 5);
  RD(int, 123, varchar, "0", 0, 5);
  RD(int, -123, varchar, "0", 0, 5);
  RD(int, 2147483647, varchar, "0", 0, 5);
  RD(int, -2147483648, varchar, "0", 0, 5);
  RD(int, 0, varchar, "123.45", 0, 5);
  RD(int, 123, varchar, "123.45", 15184.35, 5);
  RD(int, -123, varchar, "123.45", -15184.35, 5);
  RD(int, 2147483647, varchar, "123.45", 265106856222.15, 5);
  RD(int, -2147483648, varchar, "123.45", -265106856345.6, 5);
  RD(int, 0, varchar, "-123.45", 0, 5);
  RD(int, 123, varchar, "-123.45", -15184.35, 5);
  RD(int, -123, varchar, "-123.45", 15184.35, 5);
  RD(int, 2147483647, varchar, "-123.45", -265106856222.15, 5);
  RD(int, -2147483648, varchar, "-123.45", 265106856345.6, 5);
  RD(int, 0, varchar, "3.4E+38", 0, 5);
  RD(int, 123, varchar, "3.4E+38", 4.182E+40, 5);
  RD(int, -123, varchar, "3.4E+38", -4.182E+40, 5);
  RD(int, 2147483647, varchar, "3.4E+38", 7.3014443998E+47, 5);
  RD(int, -2147483648, varchar, "3.4E+38", -7.3014444032E+47, 5);
  RD(int, 0, varchar, "-3.4E+38", 0, 5);
  RD(int, 123, varchar, "-3.4E+38", -4.182E+40, 5);
  RD(int, -123, varchar, "-3.4E+38", 4.182E+40, 5);
  RD(int, 2147483647, varchar, "-3.4E+38", -7.3014443998E+47, 5);
  RD(int, -2147483648, varchar, "-3.4E+38", 7.3014444032E+47, 5);
  //string int
  RD(varchar, "0", int, 0, 0, 5);
  RD(varchar, "0", int, 123, 0, 5);
  RD(varchar, "0", int, -123, 0, 5);
  RD(varchar, "0", int, 2147483647, 0, 5);
  RD(varchar, "0", int, -2147483648, 0, 5);
  RD(varchar, "123.45", int, 0, 0, 5);
  RD(varchar, "123.45", int, 123, 15184.35, 5);
  RD(varchar, "123.45", int, -123, -15184.35, 5);
  RD(varchar, "123.45", int, 2147483647, 265106856222.15, 5);
  RD(varchar, "123.45", int, -2147483648, -265106856345.6, 5);
  RD(varchar, "-123.45", int, 0, 0, 5);
  RD(varchar, "-123.45", int, 123, -15184.35, 5);
  RD(varchar, "-123.45", int, -123, 15184.35, 5);
  RD(varchar, "-123.45", int, 2147483647, -265106856222.15, 5);
  RD(varchar, "-123.45", int, -2147483648, 265106856345.6, 5);
  RD(varchar, "3.4E+38", int, 0, 0, 5);
  RD(varchar, "3.4E+38", int, 123, 4.182E+40, 5);
  RD(varchar, "3.4E+38", int, -123, -4.182E+40, 5);
  RD(varchar, "3.4E+38", int, 2147483647, 7.3014443998E+47, 5);
  RD(varchar, "3.4E+38", int, -2147483648, -7.3014444032E+47, 5);
  RD(varchar, "-3.4E+38", int, 0, 0, 5);
  RD(varchar, "-3.4E+38", int, 123, -4.182E+40, 5);
  RD(varchar, "-3.4E+38", int, -123, 4.182E+40, 5);
  RD(varchar, "-3.4E+38", int, 2147483647, -7.3014443998E+47, 5);
  RD(varchar, "-3.4E+38", int, -2147483648, 7.3014444032E+47, 5);
}
*/

TEST_F(ObExprMulTest, uint_uint)
{
  ObMalloc buf;
  //small types
  R(utinyint, 32, usmallint, 16, uint64, 512);
  R(utinyint, 32, usmallint, 32, uint64, 1024);
  R(utinyint, 32, usmallint, 64, uint64, 2048);
  R(usmallint, 16, utinyint, 127, uint64, 2032);
  R(usmallint, 32, utinyint, 127, uint64, 4064);
  R(usmallint, 64, utinyint, 127, uint64, 8128);
  R(umediumint, 127, usmallint, 32, uint64, 4064);
  R(umediumint, 127, usmallint, 64, uint64, 8128);
  R(umediumint, 255, usmallint, 0, uint64, 0);
  R(usmallint, 32, umediumint, 127, uint64, 4064);
  R(usmallint, 64, umediumint, 127, uint64, 8128);
  R(usmallint, 0, umediumint, 255, uint64, 0);
  R(umediumint, 32, uint32, 0, uint64, 0);
  R(umediumint, 32, uint32, 16, uint64, 512);
  R(umediumint, 32, uint32, 32, uint64, 1024);
  R(uint32, 0, umediumint, 32, uint64, 0);
  R(uint32, 16, umediumint, 32, uint64, 512);
  R(uint32, 32, umediumint, 32, uint64, 1024);
  R(utinyint, 32, uint32, 0, uint64, 0);
  R(utinyint, 32, uint32, 16, uint64, 512);
  R(utinyint, 32, uint32, 32, uint64, 1024);
  R(uint32, 0, utinyint, 32, uint64, 0);
  R(uint32, 16, utinyint, 32, uint64, 512);
  R(uint32, 32, utinyint, 32, uint64, 1024);
  //uint64 uint64
  R(uint64, 0, uint64, 0, uint64, 0);
  R(uint64, 0, uint64, 1, uint64, 0);
  R(uint64, 0, uint64, 2, uint64, 0);
  R(uint64, 0, uint64, 1024, uint64, 0);
  R(uint64, 0, uint64, INT64_MAX, uint64, 0);
  R(uint64, 0, uint64, UINT64_MAX, uint64, 0);
  R(uint64, 1, uint64, 0, uint64, 0);
  R(uint64, 1, uint64, 1, uint64, 1);
  R(uint64, 1, uint64, 2, uint64, 2);
  R(uint64, 1, uint64, 1024, uint64, 1024);
  R(uint64, 1, uint64, INT64_MAX, uint64, INT64_MAX);
  R(uint64, 1, uint64, UINT64_MAX, uint64, UINT64_MAX);
  R(uint64, 2, uint64, 0, uint64, 0);
  R(uint64, 2, uint64, 1, uint64, 2);
  R(uint64, 2, uint64, 2, uint64, 4);
  R(uint64, 2, uint64, 1024, uint64, 2048);
  R(uint64, 2, uint64, INT64_MAX, uint64, UINT64_MAX - 1);
  FE(uint64, 2, uint64, UINT64_MAX);
  R(uint64, 1024, uint64, 0, uint64, 0);
  R(uint64, 1024, uint64, 1, uint64, 1024);
  R(uint64, 1024, uint64, 2, uint64, 2048);
  R(uint64, 1024, uint64, 1024, uint64, 1048576);
  FE(uint64, 1024, uint64, INT64_MAX);
  FE(uint64, 1024, uint64, UINT64_MAX);
  R(uint64, INT64_MAX, uint64, 0, uint64, 0);
  R(uint64, INT64_MAX, uint64, 1, uint64, INT64_MAX);
  R(uint64, INT64_MAX, uint64, 2, uint64, UINT64_MAX - 1);
  FE(uint64, INT64_MAX, uint64, 1024);
  FE(uint64, INT64_MAX, uint64, INT64_MAX);
  FE(uint64, INT64_MAX, uint64, UINT64_MAX);
  R(uint64, UINT64_MAX, uint64, 0, uint64, 0);
  R(uint64, UINT64_MAX, uint64, 1, uint64, UINT64_MAX);
  FE(uint64, UINT64_MAX, uint64, 2);
  FE(uint64, UINT64_MAX, uint64, 1024);
  FE(uint64, UINT64_MAX, uint64, INT64_MAX);
  FE(uint64, UINT64_MAX, uint64, UINT64_MAX);
}

TEST_F(ObExprMulTest, uint_float)
{
  ObMalloc buf;
  //small types
  RD(utinyint, 40, float, static_cast<float>(123.45), 4938, 5);
  RD(utinyint, 0, float, static_cast<float>(-123.45), 0, 5);
  RD(utinyint, 10, float, static_cast<float>(-123.45), -1234.5, 5);
  RD(float, static_cast<float>(-123.45), utinyint, 0, 0, 5);
  RD(float, static_cast<float>(-123.45), utinyint, 10, -1234.5, 5);
  RD(float, static_cast<float>(-123.45), utinyint, 20, -2469, 5);
  RD(usmallint, 10, float, static_cast<float>(3.4E+38), 3.4E+39, 5);
  RD(usmallint, 20, float, static_cast<float>(3.4E+38), 6.8E+39, 5);
  RD(usmallint, 30, float, static_cast<float>(3.4E+38), 1.02E+40, 5);
  RD(float, static_cast<float>(3.4E+38), usmallint, 40, 1.36E+40, 5);
  RD(float, static_cast<float>(-3.4E+38), usmallint, 0, 0, 5);
  RD(float, static_cast<float>(-3.4E+38), usmallint, 10, -3.4E+39, 5);
  RD(umediumint, 0, float, static_cast<float>(123.45), 0, 5);
  RD(umediumint, 10, float, static_cast<float>(123.45), 1234.5, 5);
  RD(umediumint, 20, float, static_cast<float>(123.45), 2469, 5);
  RD(float, static_cast<float>(-3.4E+38), umediumint, 20, -6.8E+39, 5);
  RD(float, static_cast<float>(-3.4E+38), umediumint, 30, -1.02E+40, 5);
  RD(float, static_cast<float>(-3.4E+38), umediumint, 40, -1.36E+40, 5);
  RD(uint32, 10, float, static_cast<float>(123.45), 1234.5, 5);
  RD(uint32, 20, float, static_cast<float>(123.45), 2469, 5);
  RD(uint32, 30, float, static_cast<float>(123.45), 3703.5, 5);
  RD(float, static_cast<float>(-123.45), uint32, 30, -3703.5, 5);
  RD(float, static_cast<float>(-123.45), uint32, 40, -4938, 5);
  RD(float, static_cast<float>(3.4E+38), uint32, 0, 0, 5);
  //uint64 float
  RD(uint64, 0, float, static_cast<float>(0), 0, 5);
  RD(uint64, 1, float, static_cast<float>(0), 0, 5);
  RD(uint64, 123, float, static_cast<float>(0), 0, 5);
  RD(uint64, INT64_MAX, float, static_cast<float>(0), 0, 5);
  RD(uint64, UINT64_MAX, float, static_cast<float>(0), 0, 5);
  RD(uint64, 0, float, static_cast<float>(123.45), 0, 5);
  RD(uint64, 1, float, static_cast<float>(123.45), 123.45, 5);
  RD(uint64, 123, float, static_cast<float>(123.45), 15184.35, 5);
  RD(uint64, INT64_MAX, float, static_cast<float>(123.45), 1.13862527794972E+21, 5);
  RD(uint64, UINT64_MAX, float, static_cast<float>(123.45), 2.27725055589944E+21, 5);
  RD(uint64, 0, float, static_cast<float>(-123.45), 0, 5);
  RD(uint64, 1, float, static_cast<float>(-123.45), -123.45, 5);
  RD(uint64, 123, float, static_cast<float>(-123.45), -15184.35, 5);
  RD(uint64, INT64_MAX, float, static_cast<float>(-123.45), -1.13862527794972E+21, 5);
  RD(uint64, UINT64_MAX, float, static_cast<float>(-123.45), -2.27725055589944E+21, 5);
  RD(uint64, 0, float, static_cast<float>(3.4E+38), 0, 5);
  RD(uint64, 1, float, static_cast<float>(3.4E+38), 3.4E+38, 5);
  RD(uint64, 123, float, static_cast<float>(3.4E+38), 4.182E+40, 5);
  RD(uint64, INT64_MAX, float, static_cast<float>(3.4E+38), 3.13594649253062E+57, 5);
  RD(uint64, UINT64_MAX, float, static_cast<float>(3.4E+38), 6.27189298506123E+57, 5);
  RD(uint64, 0, float, static_cast<float>(-3.4E+38), 0, 5);
  RD(uint64, 1, float, static_cast<float>(-3.4E+38), -3.4E+38, 5);
  RD(uint64, 123, float, static_cast<float>(-3.4E+38), -4.182E+40, 5);
  RD(uint64, INT64_MAX, float, static_cast<float>(-3.4E+38), -3.13594649253062E+57, 5);
  RD(uint64, UINT64_MAX, float, static_cast<float>(-3.4E+38), -6.27189298506123E+57, 5);
  //float uint64
  RD(float, static_cast<float>(0), uint64, 0, 0, 5);
  RD(float, static_cast<float>(0), uint64, 1, 0, 5);
  RD(float, static_cast<float>(0), uint64, 123, 0, 5);
  RD(float, static_cast<float>(0), uint64, INT64_MAX, 0, 5);
  RD(float, static_cast<float>(0), uint64, UINT64_MAX, 0, 5);
  RD(float, static_cast<float>(123.45), uint64, 0, 0, 5);
  RD(float, static_cast<float>(123.45), uint64, 1, 123.45, 5);
  RD(float, static_cast<float>(123.45), uint64, 123, 15184.35, 5);
  RD(float, static_cast<float>(123.45), uint64, INT64_MAX, 1.13862527794972E+21, 5);
  RD(float, static_cast<float>(123.45), uint64, UINT64_MAX, 2.27725055589944E+21, 5);
  RD(float, static_cast<float>(-123.45), uint64, 0, 0, 5);
  RD(float, static_cast<float>(-123.45), uint64, 1, -123.45, 5);
  RD(float, static_cast<float>(-123.45), uint64, 123, -15184.35, 5);
  RD(float, static_cast<float>(-123.45), uint64, INT64_MAX, -1.13862527794972E+21, 5);
  RD(float, static_cast<float>(-123.45), uint64, UINT64_MAX, -2.27725055589944E+21, 5);
  RD(float, static_cast<float>(3.4E+38), uint64, 0, 0, 5);
  RD(float, static_cast<float>(3.4E+38), uint64, 1, 3.4E+38, 5);
  RD(float, static_cast<float>(3.4E+38), uint64, 123, 4.182E+40, 5);
  RD(float, static_cast<float>(3.4E+38), uint64, INT64_MAX, 3.13594649253062E+57, 5);
  RD(float, static_cast<float>(3.4E+38), uint64, UINT64_MAX, 6.27189298506123E+57, 5);
  RD(float, static_cast<float>(-3.4E+38), uint64, 0, 0, 5);
  RD(float, static_cast<float>(-3.4E+38), uint64, 1, -3.4E+38, 5);
  RD(float, static_cast<float>(-3.4E+38), uint64, 123, -4.182E+40, 5);
  RD(float, static_cast<float>(-3.4E+38), uint64, INT64_MAX, -3.13594649253062E+57, 5);
  RD(float, static_cast<float>(-3.4E+38), uint64, UINT64_MAX, -6.27189298506123E+57, 5);
}


TEST_F(ObExprMulTest, uint_double)
{
  ObMalloc buf;
  //small types
  RD(utinyint, 0, double, 123.45, 0, 5);
  RD(utinyint, 10, double, 123.45, 1234.5, 5);
  RD(utinyint, 20, double, 123.45, 2469, 5);
  RD(double, 123.45, utinyint, 10, 1234.5, 5);
  RD(double, 123.45, utinyint, 20, 2469, 5);
  RD(double, 123.45, utinyint, 30, 3703.5, 5);
  RD(usmallint, 30, double, 123.45, 3703.5, 5);
  RD(usmallint, 40, double, 123.45, 4938, 5);
  RD(usmallint, 0, double, -123.45, 0, 5);
  RD(double, 3.4E+38, usmallint, 10, 3.4E+39, 5);
  RD(double, 3.4E+38, usmallint, 20, 6.8E+39, 5);
  RD(double, 3.4E+38, usmallint, 30, 1.02E+40, 5);
  RD(umediumint, 20, double, -3.4E+38, -6.8E+39, 5);
  RD(umediumint, 30, double, -3.4E+38, -1.02E+40, 5);
  RD(umediumint, 40, double, -3.4E+38, -1.36E+40, 5);
  RD(double, -3.4E+38, umediumint, 20, -6.8E+39, 5);
  RD(double, -3.4E+38, umediumint, 30, -1.02E+40, 5);
  RD(double, -3.4E+38, umediumint, 40, -1.36E+40, 5);
  RD(uint32, 0, double, -123.45, 0, 5);
  RD(uint32, 10, double, -123.45, -1234.5, 5);
  RD(uint32, 20, double, -123.45, -2469, 5);
  RD(double, 3.4E+38, uint32, 10, 3.4E+39, 5);
  RD(double, 3.4E+38, uint32, 20, 6.8E+39, 5);
  RD(double, 3.4E+38, uint32, 30, 1.02E+40, 5);
  //uint double
  RD(uint64, 0, double, 0, 0, 5);
  RD(uint64, 1, double, 0, 0, 5);
  RD(uint64, 123, double, 0, 0, 5);
  RD(uint64, INT64_MAX, double, 0, 0, 5);
  RD(uint64, UINT64_MAX, double, 0, 0, 5);
  RD(uint64, 0, double, 123.45, 0, 5);
  RD(uint64, 1, double, 123.45, 123.45, 5);
  RD(uint64, 123, double, 123.45, 15184.35, 5);
  RD(uint64, INT64_MAX, double, 123.45, 1.13862527794972E+21, 5);
  RD(uint64, UINT64_MAX, double, 123.45, 2.27725055589944E+21, 5);
  RD(uint64, 0, double, -123.45, 0, 5);
  RD(uint64, 1, double, -123.45, -123.45, 5);
  RD(uint64, 123, double, -123.45, -15184.35, 5);
  RD(uint64, INT64_MAX, double, -123.45, -1.13862527794972E+21, 5);
  RD(uint64, UINT64_MAX, double, -123.45, -2.27725055589944E+21, 5);
  RD(uint64, 0, double, 3.4E+38, 0, 5);
  RD(uint64, 1, double, 3.4E+38, 3.4E+38, 5);
  RD(uint64, 123, double, 3.4E+38, 4.182E+40, 5);
  RD(uint64, INT64_MAX, double, 3.4E+38, 3.13594649253062E+57, 5);
  RD(uint64, UINT64_MAX, double, 3.4E+38, 6.27189298506123E+57, 5);
  RD(uint64, 0, double, -3.4E+38, 0, 5);
  RD(uint64, 1, double, -3.4E+38, -3.4E+38, 5);
  RD(uint64, 123, double, -3.4E+38, -4.182E+40, 5);
  RD(uint64, INT64_MAX, double, -3.4E+38, -3.13594649253062E+57, 5);
  RD(uint64, UINT64_MAX, double, -3.4E+38, -6.27189298506123E+57, 5);
  //double uint
  RD(double, 0, uint64, 0, 0, 5);
  RD(double, 0, uint64, 1, 0, 5);
  RD(double, 0, uint64, 123, 0, 5);
  RD(double, 0, uint64, INT64_MAX, 0, 5);
  RD(double, 0, uint64, UINT64_MAX, 0, 5);
  RD(double, 123.45, uint64, 0, 0, 5);
  RD(double, 123.45, uint64, 1, 123.45, 5);
  RD(double, 123.45, uint64, 123, 15184.35, 5);
  RD(double, 123.45, uint64, INT64_MAX, 1.13862527794972E+21, 5);
  RD(double, 123.45, uint64, UINT64_MAX, 2.27725055589944E+21, 5);
  RD(double, -123.45, uint64, 0, 0, 5);
  RD(double, -123.45, uint64, 1, -123.45, 5);
  RD(double, -123.45, uint64, 123, -15184.35, 5);
  RD(double, -123.45, uint64, INT64_MAX, -1.13862527794972E+21, 5);
  RD(double, -123.45, uint64, UINT64_MAX, -2.27725055589944E+21, 5);
  RD(double, 3.4E+38, uint64, 0, 0, 5);
  RD(double, 3.4E+38, uint64, 1, 3.4E+38, 5);
  RD(double, 3.4E+38, uint64, 123, 4.182E+40, 5);
  RD(double, 3.4E+38, uint64, INT64_MAX, 3.13594649253062E+57, 5);
  RD(double, 3.4E+38, uint64, UINT64_MAX, 6.27189298506123E+57, 5);
  RD(double, -3.4E+38, uint64, 0, 0, 5);
  RD(double, -3.4E+38, uint64, 1, -3.4E+38, 5);
  RD(double, -3.4E+38, uint64, 123, -4.182E+40, 5);
  RD(double, -3.4E+38, uint64, INT64_MAX, -3.13594649253062E+57, 5);
  RD(double, -3.4E+38, uint64, UINT64_MAX, -6.27189298506123E+57, 5);
  //edge
  R(uint64, 1, double, DBL_MAX, double, DBL_MAX);
  FE(uint64, 10, double, DBL_MAX);
  R(uint64, 1, double, -DBL_MAX, double, -DBL_MAX);
  FE(uint64, 10, double, -DBL_MAX);
  R(double, DBL_MAX, uint64, 1, double, DBL_MAX);
  FE(double, DBL_MAX, uint64, 10);
  R(double, -DBL_MAX, uint64, 1, double, -DBL_MAX);
  FE(double, -DBL_MAX, uint64, 10);
}

TEST_F(ObExprMulTest, uint_number)
{
  ObMalloc buf;
  number::ObNumber num1;
  number::ObNumber num2;
  number::ObNumber num3;
  //small types
  RN2(utinyint, 0, 128, 0);
  RN2(utinyint, 10, 128, 1280);
  RN2(utinyint, 20, 128, 2560);
  RN1(128, utinyint, 30, 3840);
  RN1(128, utinyint, 40, 5120);
  RN1(128, utinyint, 50, 6400);
  RN2(usmallint, 40, -1024, -40960);
  RN2(usmallint, 50, -1024, -51200);
  RN2(usmallint, 0, -256, 0);
  RN1(0, usmallint, 40, 0);
  RN1(0, usmallint, 50, 0);
  RN1(128, usmallint, 0, 0);
  RN2(umediumint, 20, -256, -5120);
  RN2(umediumint, 30, -256, -7680);
  RN2(umediumint, 40, -256, -10240);
  RN1(512, umediumint, 0, 0);
  RN1(512, umediumint, 10, 5120);
  RN1(512, umediumint, 20, 10240);
  RN2(uint32, 40, -1024, -40960);
  RN2(uint32, 50, -1024, -51200);
  RN2(uint32, 0, -256, 0);
  RN1(128, uint32, 50, 6400);
  RN1(512, uint32, 0, 0);
  RN1(512, uint32, 10, 5120);
  //uint number
  RN2(uint64, 0, -1024, 0);
  RN2(uint64, 1, -1024, -1024);
  RN2(uint64, 1024, -1024, -1048576);
  RN2(uint64, 0, -1, 0);
  RN2(uint64, 1, -1, -1);
  RN2(uint64, 1024, -1, -1024);
  RN2(uint64, 0, 0, 0);
  RN2(uint64, 1, 0, 0);
  RN2(uint64, 1024, 0, 0);
  RN2(uint64, 0, 1, 0);
  RN2(uint64, 1, 1, 1);
  RN2(uint64, 1024, 1, 1024);
  RN2(uint64, 0, 1024, 0);
  RN2(uint64, 1, 1024, 1024);
  RN2(uint64, 1024, 1024, 1048576);
  //number uint
  RN1(-1024, uint64, 0, 0);
  RN1(-1024, uint64, 1, -1024);
  RN1(-1024, uint64, 1024, -1048576);
  RN1(-1, uint64, 0, 0);
  RN1(-1, uint64, 1, -1);
  RN1(-1, uint64, 1024, -1024);
  RN1(0, uint64, 0, 0);
  RN1(0, uint64, 1, 0);
  RN1(0, uint64, 1024, 0);
  RN1(1, uint64, 0, 0);
  RN1(1, uint64, 1, 1);
  RN1(1, uint64, 1024, 1024);
  RN1(1024, uint64, 0, 0);
  RN1(1024, uint64, 1, 1024);
  RN1(1024, uint64, 1024, 1048576);
  //edge
  //65 digit
  RN1(10000000000000000000000000000000000000000000000, uint64, INT64_MAX, 92233720368547758070000000000000000000000000000000000000000000000);
  RN2(uint64, INT64_MAX, 10000000000000000000000000000000000000000000000, 92233720368547758070000000000000000000000000000000000000000000000);
  //90 digit
  RN1(100000000000000000000000000000000000000000000000000000000000000000000000, uint64, INT64_MAX, 922337203685477580700000000000000000000000000000000000000000000000000000000000000000000000);
  RN2(uint64, INT64_MAX, 100000000000000000000000000000000000000000000000000000000000000000000000, 922337203685477580700000000000000000000000000000000000000000000000000000000000000000000000);
  //100 digit
  RN1(1000000000000000000000000000000000000000000000000000000000000000000000000000000000, uint64, INT64_MAX, 9223372036854775807000000000000000000000000000000000000000000000000000000000000000000000000000000000);
  RN2(uint64, INT64_MAX, 1000000000000000000000000000000000000000000000000000000000000000000000000000000000, 9223372036854775807000000000000000000000000000000000000000000000000000000000000000000000000000000000);
  //65 digit
  RN1(-10000000000000000000000000000000000000000000000, uint64, INT64_MAX, -92233720368547758070000000000000000000000000000000000000000000000);
  RN2(uint64, INT64_MAX, -10000000000000000000000000000000000000000000000, -92233720368547758070000000000000000000000000000000000000000000000);
  //90 digit
  RN1(-100000000000000000000000000000000000000000000000000000000000000000000000, uint64, INT64_MAX, -922337203685477580700000000000000000000000000000000000000000000000000000000000000000000000);
  RN2(uint64, INT64_MAX, -100000000000000000000000000000000000000000000000000000000000000000000000, -922337203685477580700000000000000000000000000000000000000000000000000000000000000000000000);
  //100 digit
  RN1(-1000000000000000000000000000000000000000000000000000000000000000000000000000000000, uint64, INT64_MAX, -9223372036854775807000000000000000000000000000000000000000000000000000000000000000000000000000000000);
  RN2(uint64, INT64_MAX, -1000000000000000000000000000000000000000000000000000000000000000000000000000000000, -9223372036854775807000000000000000000000000000000000000000000000000000000000000000000000000000000000);
}

/*
TEST_F(ObExprMulTest, uint_string)
{
  ObMalloc buf;
  //small types
  RD(utinyint, 60, varchar, "30", 1800, 5);
  RD(utinyint, 80, varchar, "30", 2400, 5);
  RD(utinyint, 0, varchar, "30", 0, 5);
  RD(varchar, "10", utinyint, 40, 400, 5);
  RD(varchar, "10", utinyint, 60, 600, 5);
  RD(varchar, "10", utinyint, 80, 800, 5);
  RD(usmallint, 40, varchar, "10", 400, 5);
  RD(usmallint, 60, varchar, "10", 600, 5);
  RD(usmallint, 80, varchar, "10", 800, 5);
  RD(varchar, "30", usmallint, 80, 2400, 5);
  RD(varchar, "30", usmallint, 0, 0, 5);
  RD(varchar, "50", usmallint, 0, 0, 5);
  RD(umediumint, 0, varchar, "0", 0, 5);
  RD(umediumint, 20, varchar, "0", 0, 5);
  RD(umediumint, 40, varchar, "0", 0, 5);
  RD(varchar, "70", umediumint, 20, 1400, 5);
  RD(varchar, "70", umediumint, 40, 2800, 5);
  RD(varchar, "70", umediumint, 60, 4200, 5);
  RD(uint32, 0, varchar, "0", 0, 5);
  RD(uint32, 0, varchar, "10", 0, 5);
  RD(uint32, 20, varchar, "10", 200, 5);
  RD(varchar, "50", uint32, 0, 0, 5);
  RD(varchar, "70", uint32, 0, 0, 5);
  RD(varchar, "70", uint32, 20, 1400, 5);
  //uint string
  RD(uint64, 0, varchar, "0", 0, 5);
  RD(uint64, 1, varchar, "0", 0, 5);
  RD(uint64, 123, varchar, "0", 0, 5);
  RD(uint64, INT64_MAX, varchar, "0", 0, 5);
  RD(uint64, UINT64_MAX, varchar, "0", 0, 5);
  RD(uint64, 0, varchar, "123.45", 0, 5);
  RD(uint64, 1, varchar, "123.45", 123.45, 5);
  RD(uint64, 123, varchar, "123.45", 15184.35, 5);
  RD(uint64, INT64_MAX, varchar, "123.45", 1.13862527794972E+21, 5);
  RD(uint64, UINT64_MAX, varchar, "123.45", 2.27725055589944E+21, 5);
  RD(uint64, 0, varchar, "-123.45", 0, 5);
  RD(uint64, 1, varchar, "-123.45", -123.45, 5);
  RD(uint64, 123, varchar, "-123.45", -15184.35, 5);
  RD(uint64, INT64_MAX, varchar, "-123.45", -1.13862527794972E+21, 5);
  RD(uint64, UINT64_MAX, varchar, "-123.45", -2.27725055589944E+21, 5);
  RD(uint64, 0, varchar, "3.4E+39", 0, 5);
  RD(uint64, 1, varchar, "3.4E+39", 3.4E+39, 5);
  RD(uint64, 123, varchar, "3.4E+39", 4.182E+41, 5);
  RD(uint64, INT64_MAX, varchar, "3.4E+39", 3.13594649253062E+58, 5);
  RD(uint64, UINT64_MAX, varchar, "3.4E+39", 6.27189298506123E+58, 5);
  RD(uint64, 0, varchar, "-3.4E+38", 0, 5);
  RD(uint64, 1, varchar, "-3.4E+38", -3.4E+38, 5);
  RD(uint64, 123, varchar, "-3.4E+38", -4.182E+40, 5);
  RD(uint64, INT64_MAX, varchar, "-3.4E+38", -3.13594649253062E+57, 5);
  RD(uint64, UINT64_MAX, varchar, "-3.4E+38", -6.27189298506123E+57, 5);
  //string uint
  RD(varchar, "0", uint64, 0, 0, 5);
  RD(varchar, "0", uint64, 1, 0, 5);
  RD(varchar, "0", uint64, 123, 0, 5);
  RD(varchar, "0", uint64, INT64_MAX, 0, 5);
  RD(varchar, "0", uint64, UINT64_MAX, 0, 5);
  RD(varchar, "123.45", uint64, 0, 0, 5);
  RD(varchar, "123.45", uint64, 1, 123.45, 5);
  RD(varchar, "123.45", uint64, 123, 15184.35, 5);
  RD(varchar, "123.45", uint64, INT64_MAX, 1.13862527794972E+21, 5);
  RD(varchar, "123.45", uint64, UINT64_MAX, 2.27725055589944E+21, 5);
  RD(varchar, "-123.45", uint64, 0, 0, 5);
  RD(varchar, "-123.45", uint64, 1, -123.45, 5);
  RD(varchar, "-123.45", uint64, 123, -15184.35, 5);
  RD(varchar, "-123.45", uint64, INT64_MAX, -1.13862527794972E+21, 5);
  RD(varchar, "-123.45", uint64, UINT64_MAX, -2.27725055589944E+21, 5);
  RD(varchar, "3.4E+39", uint64, 0, 0, 5);
  RD(varchar, "3.4E+39", uint64, 1, 3.4E+39, 5);
  RD(varchar, "3.4E+39", uint64, 123, 4.182E+41, 5);
  RD(varchar, "3.4E+39", uint64, INT64_MAX, 3.13594649253062E+58, 5);
  RD(varchar, "3.4E+39", uint64, UINT64_MAX, 6.27189298506123E+58, 5);
  RD(varchar, "-3.4E+38", uint64, 0, 0, 5);
  RD(varchar, "-3.4E+38", uint64, 1, -3.4E+38, 5);
  RD(varchar, "-3.4E+38", uint64, 123, -4.182E+40, 5);
  RD(varchar, "-3.4E+38", uint64, INT64_MAX, -3.13594649253062E+57, 5);
  RD(varchar, "-3.4E+38", uint64, UINT64_MAX, -6.27189298506123E+57, 5);
  //edge
  FE(uint64, UINT64_MAX, varchar, "1.79e+308");
  FE(uint64, 10, varchar, "1.79e+308");
  FE(varchar, "1.79e+308", uint64, UINT64_MAX);
  FE(varchar, "1.79e+308", uint64, 10);
  FE(uint64, UINT64_MAX, varchar, "-1.79e+308");
  FE(uint64, 10, varchar, "-1.79e+308");
  FE(varchar, "-1.79e+308", uint64, UINT64_MAX);
  FE(varchar, "-1.79e+308", uint64, 10);
}
*/

TEST_F(ObExprMulTest, float_float)
{
  ObMalloc buf;
  //float float
  RD(float, static_cast<float>(0), float, static_cast<float>(0), 0, 5);
  RD(float, static_cast<float>(0), float, static_cast<float>(123.45), 0, 5);
  RD(float, static_cast<float>(0), float, static_cast<float>(-123.45), 0, 5);
  RD(float, static_cast<float>(0), float, static_cast<float>(3.4E+38), 0, 5);
  RD(float, static_cast<float>(0), float, static_cast<float>(-3.4E+38), 0, 5);
  RD(float, static_cast<float>(123.45), float, static_cast<float>(0), 0, 5);
  RD(float, static_cast<float>(123.45), float, static_cast<float>(123.45), 15239.9025, 5);
  RD(float, static_cast<float>(123.45), float, static_cast<float>(-123.45), -15239.9025, 5);
  RD(float, static_cast<float>(123.45), float, static_cast<float>(3.4E+38), 4.1973E+40, 5);
  RD(float, static_cast<float>(123.45), float, static_cast<float>(-3.4E+38), -4.1973E+40, 5);
  RD(float, static_cast<float>(-123.45), float, static_cast<float>(0), 0, 5);
  RD(float, static_cast<float>(-123.45), float, static_cast<float>(123.45), -15239.9025, 5);
  RD(float, static_cast<float>(-123.45), float, static_cast<float>(-123.45), 15239.9025, 5);
  RD(float, static_cast<float>(-123.45), float, static_cast<float>(3.4E+38), -4.1973E+40, 5);
  RD(float, static_cast<float>(-123.45), float, static_cast<float>(-3.4E+38), 4.1973E+40, 5);
  RD(float, static_cast<float>(3.4E+38), float, static_cast<float>(0), 0, 5);
  RD(float, static_cast<float>(3.4E+38), float, static_cast<float>(123.45), 4.1973E+40, 5);
  RD(float, static_cast<float>(3.4E+38), float, static_cast<float>(-123.45), -4.1973E+40, 5);
  RD(float, static_cast<float>(3.4E+38), float, static_cast<float>(3.4E+38), 1.156E+77, 5);
  RD(float, static_cast<float>(3.4E+38), float, static_cast<float>(-3.4E+38), -1.156E+77, 5);
  RD(float, static_cast<float>(-3.4E+38), float, static_cast<float>(0), 0, 5);
  RD(float, static_cast<float>(-3.4E+38), float, static_cast<float>(123.45), -4.1973E+40, 5);
  RD(float, static_cast<float>(-3.4E+38), float, static_cast<float>(-123.45), 4.1973E+40, 5);
  RD(float, static_cast<float>(-3.4E+38), float, static_cast<float>(3.4E+38), -1.156E+77, 5);
  RD(float, static_cast<float>(-3.4E+38), float, static_cast<float>(-3.4E+38), 1.156E+77, 5);
}

TEST_F(ObExprMulTest, float_double)
{
  ObMalloc buf;
  //float double
  RD(float, static_cast<float>(0), double, 0, 0, 5);
  RD(float, static_cast<float>(0), double, 123.45, 0, 5);
  RD(float, static_cast<float>(0), double, -123.45, 0, 5);
  RD(float, static_cast<float>(0), double, 3.4E+38, 0, 5);
  RD(float, static_cast<float>(0), double, -3.4E+38, 0, 5);
  RD(float, static_cast<float>(123.45), double, 0, 0, 5);
  RD(float, static_cast<float>(123.45), double, 123.45, 15239.9025, 5);
  RD(float, static_cast<float>(123.45), double, -123.45, -15239.9025, 5);
  RD(float, static_cast<float>(123.45), double, 3.4E+38, 4.1973E+40, 5);
  RD(float, static_cast<float>(123.45), double, -3.4E+38, -4.1973E+40, 5);
  RD(float, static_cast<float>(-123.45), double, 0, 0, 5);
  RD(float, static_cast<float>(-123.45), double, 123.45, -15239.9025, 5);
  RD(float, static_cast<float>(-123.45), double, -123.45, 15239.9025, 5);
  RD(float, static_cast<float>(-123.45), double, 3.4E+38, -4.1973E+40, 5);
  RD(float, static_cast<float>(-123.45), double, -3.4E+38, 4.1973E+40, 5);
  RD(float, static_cast<float>(3.4E+38), double, 0, 0, 5);
  RD(float, static_cast<float>(3.4E+38), double, 123.45, 4.1973E+40, 5);
  RD(float, static_cast<float>(3.4E+38), double, -123.45, -4.1973E+40, 5);
  RD(float, static_cast<float>(3.4E+38), double, 3.4E+38, 1.156E+77, 5);
  RD(float, static_cast<float>(3.4E+38), double, -3.4E+38, -1.156E+77, 5);
  RD(float, static_cast<float>(-3.4E+38), double, 0, 0, 5);
  RD(float, static_cast<float>(-3.4E+38), double, 123.45, -4.1973E+40, 5);
  RD(float, static_cast<float>(-3.4E+38), double, -123.45, 4.1973E+40, 5);
  RD(float, static_cast<float>(-3.4E+38), double, 3.4E+38, -1.156E+77, 5);
  RD(float, static_cast<float>(-3.4E+38), double, -3.4E+38, 1.156E+77, 5);
  //double float
  RD(double, 0, float, static_cast<float>(0), 0, 5);
  RD(double, 123.45, float, static_cast<float>(0), 0, 5);
  RD(double, -123.45, float, static_cast<float>(0), 0, 5);
  RD(double, 3.4E+38, float, static_cast<float>(0), 0, 5);
  RD(double, -3.4E+38, float, static_cast<float>(0), 0, 5);
  RD(double, 0, float, static_cast<float>(123.45), 0, 5);
  RD(double, 123.45, float, static_cast<float>(123.45), 15239.9025, 5);
  RD(double, -123.45, float, static_cast<float>(123.45), -15239.9025, 5);
  RD(double, 3.4E+38, float, static_cast<float>(123.45), 4.1973E+40, 5);
  RD(double, -3.4E+38, float, static_cast<float>(123.45), -4.1973E+40, 5);
  RD(double, 0, float, static_cast<float>(-123.45), 0, 5);
  RD(double, 123.45, float, static_cast<float>(-123.45), -15239.9025, 5);
  RD(double, -123.45, float, static_cast<float>(-123.45), 15239.9025, 5);
  RD(double, 3.4E+38, float, static_cast<float>(-123.45), -4.1973E+40, 5);
  RD(double, -3.4E+38, float, static_cast<float>(-123.45), 4.1973E+40, 5);
  RD(double, 0, float, static_cast<float>(3.4E+38), 0, 5);
  RD(double, 123.45, float, static_cast<float>(3.4E+38), 4.1973E+40, 5);
  RD(double, -123.45, float, static_cast<float>(3.4E+38), -4.1973E+40, 5);
  RD(double, 3.4E+38, float, static_cast<float>(3.4E+38), 1.156E+77, 5);
  RD(double, -3.4E+38, float, static_cast<float>(3.4E+38), -1.156E+77, 5);
  RD(double, 0, float, static_cast<float>(-3.4E+38), 0, 5);
  RD(double, 123.45, float, static_cast<float>(-3.4E+38), -4.1973E+40, 5);
  RD(double, -123.45, float, static_cast<float>(-3.4E+38), 4.1973E+40, 5);
  RD(double, 3.4E+38, float, static_cast<float>(-3.4E+38), -1.156E+77, 5);
  RD(double, -3.4E+38, float, static_cast<float>(-3.4E+38), 1.156E+77, 5);
  //edge
  RD(double, DBL_MAX, float, 1.0f, DBL_MAX, 5);
  FE(double, DBL_MAX, float, 10.0f);
  FE(double, DBL_MAX, float, -10.0f);
  RD(double, -DBL_MAX, float, 1.0f, -DBL_MAX, 5);
  FE(double, -DBL_MAX, float, 10.0f);
  FE(double, -DBL_MAX, float, -10.0f);
  RD(float, 1.0f, double, DBL_MAX, DBL_MAX, 5);
  FE(float, 10.0f, double, DBL_MAX);
  FE(float, -10.0f, double, DBL_MAX);
  RD(float, 1.0f, double, -DBL_MAX, -DBL_MAX, 5);
  FE(float, 10.0f, double, -DBL_MAX);
  FE(float, -10.0f, double, -DBL_MAX);
}


TEST_F(ObExprMulTest, float_number)
{
  ObMalloc buf;
  number::ObNumber num1;
  number::ObNumber num2;
  number::ObNumber num3;
  //float number
  RN2(float, static_cast<float>(0), -123.45, 0);
  RN2(float, static_cast<float>(1), -123.45, -123.45);
  RN2(float, static_cast<float>(1.5), -123.45, -185.175);
  RN2(float, static_cast<float>(123.5), -123.45, -15246.075);
  RN2(float, static_cast<float>(-123.5), -123.45, 15246.075);
  RN2(float, static_cast<float>(0), -1, 0);
  RN2(float, static_cast<float>(1), -1, -1);
  RN2(float, static_cast<float>(1.5), -1, -1.5);
  RN2(float, static_cast<float>(123.5), -1, -123.5);
  RN2(float, static_cast<float>(-123.5), -1, 123.5);
  RN2(float, static_cast<float>(0), 0, 0);
  RN2(float, static_cast<float>(1), 0, 0);
  RN2(float, static_cast<float>(1.5), 0, 0);
  RN2(float, static_cast<float>(123.5), 0, 0);
  RN2(float, static_cast<float>(-123.5), 0, 0);
  RN2(float, static_cast<float>(0), 1, 0);
  RN2(float, static_cast<float>(1), 1, 1);
  RN2(float, static_cast<float>(1.5), 1, 1.5);
  RN2(float, static_cast<float>(123.5), 1, 123.5);
  RN2(float, static_cast<float>(-123.5), 1, -123.5);
  RN2(float, static_cast<float>(0), 123.45, 0);
  RN2(float, static_cast<float>(1), 123.45, 123.45);
  RN2(float, static_cast<float>(1.5), 123.45, 185.175);
  RN2(float, static_cast<float>(123.5), 123.45, 15246.075);
  RN2(float, static_cast<float>(-123.5), 123.45, -15246.075);
  //number float
  RN1(-123.45, float, static_cast<float>(0), 0);
  RN1(-123.45, float, static_cast<float>(1), -123.45);
  RN1(-123.45, float, static_cast<float>(1.5), -185.175);
  RN1(-123.45, float, static_cast<float>(123.5), -15246.075);
  RN1(-123.45, float, static_cast<float>(-123.5), 15246.075);
  RN1(-1, float, static_cast<float>(0), 0);
  RN1(-1, float, static_cast<float>(1), -1);
  RN1(-1, float, static_cast<float>(1.5), -1.5);
  RN1(-1, float, static_cast<float>(123.5), -123.5);
  RN1(-1, float, static_cast<float>(-123.5), 123.5);
  RN1(0, float, static_cast<float>(0), 0);
  RN1(0, float, static_cast<float>(1), 0);
  RN1(0, float, static_cast<float>(1.5), 0);
  RN1(0, float, static_cast<float>(123.5), 0);
  RN1(0, float, static_cast<float>(-123.5), 0);
  RN1(1, float, static_cast<float>(0), 0);
  RN1(1, float, static_cast<float>(1), 1);
  RN1(1, float, static_cast<float>(1.5), 1.5);
  RN1(1, float, static_cast<float>(123.5), 123.5);
  RN1(1, float, static_cast<float>(-123.5), -123.5);
  RN1(123.45, float, static_cast<float>(0), 0);
  RN1(123.45, float, static_cast<float>(1), 123.45);
  RN1(123.45, float, static_cast<float>(1.5), 185.175);
  RN1(123.45, float, static_cast<float>(123.5), 15246.075);
  RN1(123.45, float, static_cast<float>(-123.5), -15246.075);
  //edge
  //65 digit
  RN1(10000000000000000000000000000000000000000000000, float, 123.5, 1235000000000000000000000000000000000000000000000);
  RN2(float, 123.5, 10000000000000000000000000000000000000000000000, 1235000000000000000000000000000000000000000000000);
  //90 digit
  RN1(100000000000000000000000000000000000000000000000000000000000000000000000, float, 123.5, 12350000000000000000000000000000000000000000000000000000000000000000000000);
  RN2(float, 123.5, 100000000000000000000000000000000000000000000000000000000000000000000000, 12350000000000000000000000000000000000000000000000000000000000000000000000);
  //100 digit
  RN1(1000000000000000000000000000000000000000000000000000000000000000000000000000000000, float, 123.5, 123500000000000000000000000000000000000000000000000000000000000000000000000000000000);
  RN2(float, 123.5, 1000000000000000000000000000000000000000000000000000000000000000000000000000000000, 123500000000000000000000000000000000000000000000000000000000000000000000000000000000);
  //65 digit
  RN1(-10000000000000000000000000000000000000000000000, float, 123.5, -1235000000000000000000000000000000000000000000000);
  RN2(float, 123.5, -10000000000000000000000000000000000000000000000, -1235000000000000000000000000000000000000000000000);
  //90 digit
  RN1(-100000000000000000000000000000000000000000000000000000000000000000000000, float, 123.5, -12350000000000000000000000000000000000000000000000000000000000000000000000);
  RN2(float, 123.5, -100000000000000000000000000000000000000000000000000000000000000000000000, -12350000000000000000000000000000000000000000000000000000000000000000000000);
  //100 digit
  RN1(-1000000000000000000000000000000000000000000000000000000000000000000000000000000000, float, 123.5, -123500000000000000000000000000000000000000000000000000000000000000000000000000000000);
  RN2(float, 123.5, -1000000000000000000000000000000000000000000000000000000000000000000000000000000000, -123500000000000000000000000000000000000000000000000000000000000000000000000000000000);
}

/*
TEST_F(ObExprMulTest, float_string)
{
  ObMalloc buf;
  //string float
  RD(varchar, "0", float, static_cast<float>(0), 0, 5);
  RD(varchar, "123.45", float, static_cast<float>(0), 0, 5);
  RD(varchar, "-123.45", float, static_cast<float>(0), 0, 5);
  RD(varchar, "3.4E+38", float, static_cast<float>(0), 0, 5);
  RD(varchar, "-3.4E+38", float, static_cast<float>(0), 0, 5);
  RD(varchar, "0", float, static_cast<float>(123.45), 0, 5);
  RD(varchar, "123.45", float, static_cast<float>(123.45), 15239.9025, 5);
  RD(varchar, "-123.45", float, static_cast<float>(123.45), -15239.9025, 5);
  RD(varchar, "3.4E+38", float, static_cast<float>(123.45), 4.1973E+40, 5);
  RD(varchar, "-3.4E+38", float, static_cast<float>(123.45), -4.1973E+40, 5);
  RD(varchar, "0", float, static_cast<float>(-123.45), 0, 5);
  RD(varchar, "123.45", float, static_cast<float>(-123.45), -15239.9025, 5);
  RD(varchar, "-123.45", float, static_cast<float>(-123.45), 15239.9025, 5);
  RD(varchar, "3.4E+38", float, static_cast<float>(-123.45), -4.1973E+40, 5);
  RD(varchar, "-3.4E+38", float, static_cast<float>(-123.45), 4.1973E+40, 5);
  RD(varchar, "0", float, static_cast<float>(3.4E+38), 0, 5);
  RD(varchar, "123.45", float, static_cast<float>(3.4E+38), 4.1973E+40, 5);
  RD(varchar, "-123.45", float, static_cast<float>(3.4E+38), -4.1973E+40, 5);
  RD(varchar, "3.4E+38", float, static_cast<float>(3.4E+38), 1.156E+77, 5);
  RD(varchar, "-3.4E+38", float, static_cast<float>(3.4E+38), -1.156E+77, 5);
  RD(varchar, "0", float, static_cast<float>(-3.4E+38), 0, 5);
  RD(varchar, "123.45", float, static_cast<float>(-3.4E+38), -4.1973E+40, 5);
  RD(varchar, "-123.45", float, static_cast<float>(-3.4E+38), 4.1973E+40, 5);
  RD(varchar, "3.4E+38", float, static_cast<float>(-3.4E+38), -1.156E+77, 5);
  RD(varchar, "-3.4E+38", float, static_cast<float>(-3.4E+38), 1.156E+77, 5);
  //float string
  RD(float, static_cast<float>(0), varchar, "0", 0, 5);
  RD(float, static_cast<float>(0), varchar, "123.45", 0, 5);
  RD(float, static_cast<float>(0), varchar, "-123.45", 0, 5);
  RD(float, static_cast<float>(0), varchar, "3.4E+38", 0, 5);
  RD(float, static_cast<float>(0), varchar, "-3.4E+38", 0, 5);
  RD(float, static_cast<float>(123.45), varchar, "0", 0, 5);
  RD(float, static_cast<float>(123.45), varchar, "123.45", 15239.9025, 5);
  RD(float, static_cast<float>(123.45), varchar, "-123.45", -15239.9025, 5);
  RD(float, static_cast<float>(123.45), varchar, "3.4E+38", 4.1973E+40, 5);
  RD(float, static_cast<float>(123.45), varchar, "-3.4E+38", -4.1973E+40, 5);
  RD(float, static_cast<float>(-123.45), varchar, "0", 0, 5);
  RD(float, static_cast<float>(-123.45), varchar, "123.45", -15239.9025, 5);
  RD(float, static_cast<float>(-123.45), varchar, "-123.45", 15239.9025, 5);
  RD(float, static_cast<float>(-123.45), varchar, "3.4E+38", -4.1973E+40, 5);
  RD(float, static_cast<float>(-123.45), varchar, "-3.4E+38", 4.1973E+40, 5);
  RD(float, static_cast<float>(3.4E+38), varchar, "0", 0, 5);
  RD(float, static_cast<float>(3.4E+38), varchar, "123.45", 4.1973E+40, 5);
  RD(float, static_cast<float>(3.4E+38), varchar, "-123.45", -4.1973E+40, 5);
  RD(float, static_cast<float>(3.4E+38), varchar, "3.4E+38", 1.156E+77, 5);
  RD(float, static_cast<float>(3.4E+38), varchar, "-3.4E+38", -1.156E+77, 5);
  RD(float, static_cast<float>(-3.4E+38), varchar, "0", 0, 5);
  RD(float, static_cast<float>(-3.4E+38), varchar, "123.45", -4.1973E+40, 5);
  RD(float, static_cast<float>(-3.4E+38), varchar, "-123.45", 4.1973E+40, 5);
  RD(float, static_cast<float>(-3.4E+38), varchar, "3.4E+38", -1.156E+77, 5);
  RD(float, static_cast<float>(-3.4E+38), varchar, "-3.4E+38", 1.156E+77, 5);
  //edge
  RD(varchar, "1.79e+308", float, 1.0f, 1.79e+308, 5);
  FE(varchar, "1.79e+308", float, 10.0f);
  FE(varchar, "1.79e+308", float, -10.0f);
  RD(varchar, "-1.79e+308", float, 1.0f, -1.79e+308, 5);
  FE(varchar, "-1.79e+308", float, 10.0f);
  FE(varchar, "-1.79e+308", float, -10.0f);
  RD(float, 1.0f, varchar, "1.79e+308", 1.79e+308, 5);
  FE(float, 10.0f, varchar, "1.79e+308");
  FE(float, -10.0f, varchar, "1.79e+308");
  RD(float, 1.0f, varchar, "-1.79e+308", -1.79e+308, 5);
  FE(float, 10.0f, varchar, "-1.79e+308");
  FE(float, -10.0f, varchar, "-1.79e+308");
}
*/

TEST_F(ObExprMulTest, double_double)
{
  ObMalloc buf;
  //double double
  RD(double, 0, double, 0, 0, 5);
  RD(double, 0, double, 123.45, 0, 5);
  RD(double, 0, double, -123.45, 0, 5);
  RD(double, 0, double, 3.4E+38, 0, 5);
  RD(double, 0, double, -3.4E+38, 0, 5);
  RD(double, 123.45, double, 0, 0, 5);
  RD(double, 123.45, double, 123.45, 15239.9025, 5);
  RD(double, 123.45, double, -123.45, -15239.9025, 5);
  RD(double, 123.45, double, 3.4E+38, 4.1973E+40, 5);
  RD(double, 123.45, double, -3.4E+38, -4.1973E+40, 5);
  RD(double, -123.45, double, 0, 0, 5);
  RD(double, -123.45, double, 123.45, -15239.9025, 5);
  RD(double, -123.45, double, -123.45, 15239.9025, 5);
  RD(double, -123.45, double, 3.4E+38, -4.1973E+40, 5);
  RD(double, -123.45, double, -3.4E+38, 4.1973E+40, 5);
  RD(double, 3.4E+38, double, 0, 0, 5);
  RD(double, 3.4E+38, double, 123.45, 4.1973E+40, 5);
  RD(double, 3.4E+38, double, -123.45, -4.1973E+40, 5);
  RD(double, 3.4E+38, double, 3.4E+38, 1.156E+77, 5);
  RD(double, 3.4E+38, double, -3.4E+38, -1.156E+77, 5);
  RD(double, -3.4E+38, double, 0, 0, 5);
  RD(double, -3.4E+38, double, 123.45, -4.1973E+40, 5);
  RD(double, -3.4E+38, double, -123.45, 4.1973E+40, 5);
  RD(double, -3.4E+38, double, 3.4E+38, -1.156E+77, 5);
  RD(double, -3.4E+38, double, -3.4E+38, 1.156E+77, 5);
  //edge
  RD(double, DBL_MAX, double, 1, DBL_MAX, 5);
  RD(double, 1, double, DBL_MAX, DBL_MAX, 5);
  RD(double, -DBL_MAX, double, 1, -DBL_MAX, 5);
  RD(double, 1, double, -DBL_MAX, -DBL_MAX, 5);
  FE(double, DBL_MAX, double, 10);
  FE(double, 10, double, DBL_MAX);
  FE(double, -DBL_MAX, double, 10);
  FE(double, 10, double, -DBL_MAX);
  FE(double, DBL_MAX, double, DBL_MAX);
  FE(double, DBL_MAX, double, -DBL_MAX);
  FE(double, -DBL_MAX, double, DBL_MAX);
  FE(double, -DBL_MAX, double, -DBL_MAX);
}

TEST_F(ObExprMulTest, double_number)
{
  ObMalloc buf;
  number::ObNumber num1;
  number::ObNumber num2;
  number::ObNumber num3;
  //double number
  RN2(double, 0, -123.45678, 0);
  RN2(double, 1, -123.45678, -123.45678);
  RN2(double, 1.5, -123.45678, -185.18517);
  RN2(double, 123.45678, -123.45678, -15241.5765279684);
  RN2(double, -123.45678, -123.45678, 15241.5765279684);
  RN2(double, 0, -1, 0);
  RN2(double, 1, -1, -1);
  RN2(double, 1.5, -1, -1.5);
  RN2(double, 123.45678, -1, -123.45678);
  RN2(double, -123.45678, -1, 123.45678);
  RN2(double, 0, 0, 0);
  RN2(double, 1, 0, 0);
  RN2(double, 1.5, 0, 0);
  RN2(double, 123.45678, 0, 0);
  RN2(double, -123.45678, 0, 0);
  RN2(double, 0, 1, 0);
  RN2(double, 1, 1, 1);
  RN2(double, 1.5, 1, 1.5);
  RN2(double, 123.45678, 1, 123.45678);
  RN2(double, -123.45678, 1, -123.45678);
  RN2(double, 0, 123.45678, 0);
  RN2(double, 1, 123.45678, 123.45678);
  RN2(double, 1.5, 123.45678, 185.18517);
  RN2(double, 123.45678, 123.45678, 15241.5765279684);
  RN2(double, -123.45678, 123.45678, -15241.5765279684);
  //number double
  RN1(-123.45678, double, 0, 0);
  RN1(-123.45678, double, 1, -123.45678);
  RN1(-123.45678, double, 1.5, -185.18517);
  RN1(-123.45678, double, 123.45678, -15241.5765279684);
  RN1(-123.45678, double, -123.45678, 15241.5765279684);
  RN1(-1, double, 0, 0);
  RN1(-1, double, 1, -1);
  RN1(-1, double, 1.5, -1.5);
  RN1(-1, double, 123.45678, -123.45678);
  RN1(-1, double, -123.45678, 123.45678);
  RN1(0, double, 0, 0);
  RN1(0, double, 1, 0);
  RN1(0, double, 1.5, 0);
  RN1(0, double, 123.45678, 0);
  RN1(0, double, -123.45678, 0);
  RN1(1, double, 0, 0);
  RN1(1, double, 1, 1);
  RN1(1, double, 1.5, 1.5);
  RN1(1, double, 123.45678, 123.45678);
  RN1(1, double, -123.45678, -123.45678);
  RN1(123.45678, double, 0, 0);
  RN1(123.45678, double, 1, 123.45678);
  RN1(123.45678, double, 1.5, 185.18517);
  RN1(123.45678, double, 123.45678, 15241.5765279684);
  RN1(123.45678, double, -123.45678, -15241.5765279684);
  //edge
  //65 digit
  RN1(10000000000000000000000000000000000000000000000, double, 123.5, 1235000000000000000000000000000000000000000000000);
  RN2(double, 123.5, 10000000000000000000000000000000000000000000000, 1235000000000000000000000000000000000000000000000);
  //90 digit
  RN1(100000000000000000000000000000000000000000000000000000000000000000000000, double, 123.5, 12350000000000000000000000000000000000000000000000000000000000000000000000);
  RN2(double, 123.5, 100000000000000000000000000000000000000000000000000000000000000000000000, 12350000000000000000000000000000000000000000000000000000000000000000000000);
  //100 digit
  RN1(1000000000000000000000000000000000000000000000000000000000000000000000000000000000, double, 123.5, 123500000000000000000000000000000000000000000000000000000000000000000000000000000000);
  RN2(double, 123.5, 1000000000000000000000000000000000000000000000000000000000000000000000000000000000, 123500000000000000000000000000000000000000000000000000000000000000000000000000000000);
  //65 digit
  RN1(-10000000000000000000000000000000000000000000000, double, 123.5, -1235000000000000000000000000000000000000000000000);
  RN2(double, 123.5, -10000000000000000000000000000000000000000000000, -1235000000000000000000000000000000000000000000000);
  //90 digit
  RN1(-100000000000000000000000000000000000000000000000000000000000000000000000, double, 123.5, -12350000000000000000000000000000000000000000000000000000000000000000000000);
  RN2(double, 123.5, -100000000000000000000000000000000000000000000000000000000000000000000000, -12350000000000000000000000000000000000000000000000000000000000000000000000);
  //100 digit
  RN1(-1000000000000000000000000000000000000000000000000000000000000000000000000000000000, double, 123.5, -123500000000000000000000000000000000000000000000000000000000000000000000000000000000);
  RN2(double, 123.5, -1000000000000000000000000000000000000000000000000000000000000000000000000000000000, -123500000000000000000000000000000000000000000000000000000000000000000000000000000000);
}

/*
TEST_F(ObExprMulTest, double_string)
{
  ObMalloc buf;
  RD(double, 0, varchar, "0", 0, 5);
  RD(double, 0, varchar, "123.45", 0, 5);
  RD(double, 0, varchar, "-123.45", 0, 5);
  RD(double, 0, varchar, "3.4E+38", 0, 5);
  RD(double, 0, varchar, "-3.4E+38", 0, 5);
  RD(double, 123.45, varchar, "0", 0, 5);
  RD(double, 123.45, varchar, "123.45", 15239.9025, 5);
  RD(double, 123.45, varchar, "-123.45", -15239.9025, 5);
  RD(double, 123.45, varchar, "3.4E+38", 4.1973E+40, 5);
  RD(double, 123.45, varchar, "-3.4E+38", -4.1973E+40, 5);
  RD(double, -123.45, varchar, "0", 0, 5);
  RD(double, -123.45, varchar, "123.45", -15239.9025, 5);
  RD(double, -123.45, varchar, "-123.45", 15239.9025, 5);
  RD(double, -123.45, varchar, "3.4E+38", -4.1973E+40, 5);
  RD(double, -123.45, varchar, "-3.4E+38", 4.1973E+40, 5);
  RD(double, 3.4E+38, varchar, "0", 0, 5);
  RD(double, 3.4E+38, varchar, "123.45", 4.1973E+40, 5);
  RD(double, 3.4E+38, varchar, "-123.45", -4.1973E+40, 5);
  RD(double, 3.4E+38, varchar, "3.4E+38", 1.156E+77, 5);
  RD(double, 3.4E+38, varchar, "-3.4E+38", -1.156E+77, 5);
  RD(double, -3.4E+38, varchar, "0", 0, 5);
  RD(double, -3.4E+38, varchar, "123.45", -4.1973E+40, 5);
  RD(double, -3.4E+38, varchar, "-123.45", 4.1973E+40, 5);
  RD(double, -3.4E+38, varchar, "3.4E+38", -1.156E+77, 5);
  RD(double, -3.4E+38, varchar, "-3.4E+38", 1.156E+77, 5);
  //string double
  RD(varchar, "0", double, 0, 0, 5);
  RD(varchar, "123.45", double, 0, 0, 5);
  RD(varchar, "-123.45", double, 0, 0, 5);
  RD(varchar, "3.4E+38", double, 0, 0, 5);
  RD(varchar, "-3.4E+38", double, 0, 0, 5);
  RD(varchar, "0", double, 123.45, 0, 5);
  RD(varchar, "123.45", double, 123.45, 15239.9025, 5);
  RD(varchar, "-123.45", double, 123.45, -15239.9025, 5);
  RD(varchar, "3.4E+38", double, 123.45, 4.1973E+40, 5);
  RD(varchar, "-3.4E+38", double, 123.45, -4.1973E+40, 5);
  RD(varchar, "0", double, -123.45, 0, 5);
  RD(varchar, "123.45", double, -123.45, -15239.9025, 5);
  RD(varchar, "-123.45", double, -123.45, 15239.9025, 5);
  RD(varchar, "3.4E+38", double, -123.45, -4.1973E+40, 5);
  RD(varchar, "-3.4E+38", double, -123.45, 4.1973E+40, 5);
  RD(varchar, "0", double, 3.4E+38, 0, 5);
  RD(varchar, "123.45", double, 3.4E+38, 4.1973E+40, 5);
  RD(varchar, "-123.45", double, 3.4E+38, -4.1973E+40, 5);
  RD(varchar, "3.4E+38", double, 3.4E+38, 1.156E+77, 5);
  RD(varchar, "-3.4E+38", double, 3.4E+38, -1.156E+77, 5);
  RD(varchar, "0", double, -3.4E+38, 0, 5);
  RD(varchar, "123.45", double, -3.4E+38, -4.1973E+40, 5);
  RD(varchar, "-123.45", double, -3.4E+38, 4.1973E+40, 5);
  RD(varchar, "3.4E+38", double, -3.4E+38, -1.156E+77, 5);
  RD(varchar, "-3.4E+38", double, -3.4E+38, 1.156E+77, 5);
  //edge
  RD(double, DBL_MAX, varchar, "1", DBL_MAX, 5);
  FE(double, DBL_MAX, varchar, "10");
  FE(double, DBL_MAX, varchar, "1.79e+308");
  RD(varchar, "1.79e+308", double, 1, 1.79e+308, 5);
  FE(varchar, "1.79e+308", double, 10);
  FE(varchar, "1.79e+308", double, DBL_MAX);
  RD(double, -DBL_MAX, varchar, "1", -DBL_MAX, 5);
  FE(double, -DBL_MAX, varchar, "10");
  FE(double, -DBL_MAX, varchar, "1.79e+308");
  RD(varchar, "-1.79e+308", double, 1, -1.79e+308, 5);
  FE(varchar, "-1.79e+308", double, 10);
  FE(varchar, "-1.79e+308", double, DBL_MAX);
}
*/

TEST_F(ObExprMulTest, number_number)
{
  ObMalloc buf;
  number::ObNumber num1;
  number::ObNumber num2;
  number::ObNumber num3;
  //small number
  RN(-123.45678, 0, 0);
  RN(-123.45678, 1, -123.45678);
  RN(-123.45678, 1.5, -185.18517);
  RN(-123.45678, 123.45678, -15241.5765279684);
  RN(-123.45678, -123.45678, 15241.5765279684);
  RN(-1, 0, 0);
  RN(-1, 1, -1);
  RN(-1, 1.5, -1.5);
  RN(-1, 123.45678, -123.45678);
  RN(-1, -123.45678, 123.45678);
  RN(0, 0, 0);
  RN(0, 1, 0);
  RN(0, 1.5, 0);
  RN(0, 123.45678, 0);
  RN(0, -123.45678, 0);
  RN(1, 0, 0);
  RN(1, 1, 1);
  RN(1, 1.5, 1.5);
  RN(1, 123.45678, 123.45678);
  RN(1, -123.45678, -123.45678);
  RN(123.45678, 0, 0);
  RN(123.45678, 1, 123.45678);
  RN(123.45678, 1.5, 185.18517);
  RN(123.45678, 123.45678, 15241.5765279684);
  RN(123.45678, -123.45678, -15241.5765279684);
  //precision test
  RN(1, 999999999999999999999999999999999999999999999, 999999999999999999999999999999999999999999999);
  RN(1, 623745263548167241234123412341223453846715283, 623745263548167241234123412341223453846715283);
  RN(2, 499999999999999999999999999999999999999999999, 999999999999999999999999999999999999999999998);
  RN(42345234523452345, 5674567345674567456745623456, 240290885071714030758456245814391370530204320);
  RN(11234987162398.7461928763, 908798712341.234123412, 10210341866358276366407094.4596596165094499356);
  RN(999999999999999999999999999999999999999999999, 1, 999999999999999999999999999999999999999999999);
  RN(623745263548167241234123412341223453846715283, 1, 623745263548167241234123412341223453846715283);
  RN(499999999999999999999999999999999999999999999, 2, 999999999999999999999999999999999999999999998);
  RN(5674567345674567456745623456, 42345234523452345, 240290885071714030758456245814391370530204320);
  RN(908798712341.234123412, 11234987162398.7461928763, 10210341866358276366407094.4596596165094499356);
  RN(-1, 999999999999999999999999999999999999999999999, -999999999999999999999999999999999999999999999);
  RN(-1, -623745263548167241234123412341223453846715283, 623745263548167241234123412341223453846715283);
  RN(-2, -499999999999999999999999999999999999999999999, 999999999999999999999999999999999999999999998);
  RN(-42345234523452345, 5674567345674567456745623456, -240290885071714030758456245814391370530204320);
  RN(-11234987162398.7461928763, -908798712341.234123412, 10210341866358276366407094.4596596165094499356);
  RN(-999999999999999999999999999999999999999999999, 1, -999999999999999999999999999999999999999999999);
  RN(-623745263548167241234123412341223453846715283, 1, -623745263548167241234123412341223453846715283);
  RN(499999999999999999999999999999999999999999999, -2, -999999999999999999999999999999999999999999998);
  RN(-5674567345674567456745623456, -42345234523452345, 240290885071714030758456245814391370530204320);
  RN(-908798712341.234123412, 11234987162398.7461928763, -10210341866358276366407094.4596596165094499356);
}

/*
TEST_F(ObExprMulTest, number_string)
{
  ObMalloc buf;
  number::ObNumber num1;
  number::ObNumber num2;
  number::ObNumber num3;
  //number varchar
  RN1(-123.45678, varchar, "0", 0);
  RN1(-123.45678, varchar, "1", -123.45678);
  RN1(-123.45678, varchar, "1.5", -185.18517);
  RN1(-123.45678, varchar, "123.45678", -15241.5765279684);
  RN1(-123.45678, varchar, "-123.45678", 15241.5765279684);
  RN1(-1, varchar, "0", 0);
  RN1(-1, varchar, "1", -1);
  RN1(-1, varchar, "1.5", -1.5);
  RN1(-1, varchar, "123.45678", -123.45678);
  RN1(-1, varchar, "-123.45678", 123.45678);
  RN1(0, varchar, "0", 0);
  RN1(0, varchar, "1", 0);
  RN1(0, varchar, "1.5", 0);
  RN1(0, varchar, "123.45678", 0);
  RN1(0, varchar, "-123.45678", 0);
  RN1(1, varchar, "0", 0);
  RN1(1, varchar, "1", 1);
  RN1(1, varchar, "1.5", 1.5);
  RN1(1, varchar, "123.45678", 123.45678);
  RN1(1, varchar, "-123.45678", -123.45678);
  RN1(123.45678, varchar, "0", 0);
  RN1(123.45678, varchar, "1", 123.45678);
  RN1(123.45678, varchar, "1.5", 185.18517);
  RN1(123.45678, varchar, "123.45678", 15241.5765279684);
  RN1(123.45678, varchar, "-123.45678", -15241.5765279684);
  //varchar number
  RN2(varchar, "0", -123.45678, 0);
  RN2(varchar, "1", -123.45678, -123.45678);
  RN2(varchar, "1.5", -123.45678, -185.18517);
  RN2(varchar, "123.45678", -123.45678, -15241.5765279684);
  RN2(varchar, "-123.45678", -123.45678, 15241.5765279684);
  RN2(varchar, "0", -1, 0);
  RN2(varchar, "1", -1, -1);
  RN2(varchar, "1.5", -1, -1.5);
  RN2(varchar, "123.45678", -1, -123.45678);
  RN2(varchar, "-123.45678", -1, 123.45678);
  RN2(varchar, "0", 0, 0);
  RN2(varchar, "1", 0, 0);
  RN2(varchar, "1.5", 0, 0);
  RN2(varchar, "123.45678", 0, 0);
  RN2(varchar, "-123.45678", 0, 0);
  RN2(varchar, "0", 1, 0);
  RN2(varchar, "1", 1, 1);
  RN2(varchar, "1.5", 1, 1.5);
  RN2(varchar, "123.45678", 1, 123.45678);
  RN2(varchar, "-123.45678", 1, -123.45678);
  RN2(varchar, "0", 123.45678, 0);
  RN2(varchar, "1", 123.45678, 123.45678);
  RN2(varchar, "1.5", 123.45678, 185.18517);
  RN2(varchar, "123.45678", 123.45678, 15241.5765279684);
  RN2(varchar, "-123.45678", 123.45678, -15241.5765279684);
}
*/

/*
TEST_F(ObExprMulTest, string_string)
{
  ObMalloc buf;
  //string string
  RD(varchar, "0", varchar, "0", 0, 5);
  RD(varchar, "0", varchar, "123.45", 0, 5);
  RD(varchar, "0", varchar, "-123.45", 0, 5);
  RD(varchar, "0", varchar, "3.4E+38", 0, 5);
  RD(varchar, "0", varchar, "-3.4E+38", 0, 5);
  RD(varchar, "123.45", varchar, "0", 0, 5);
  RD(varchar, "123.45", varchar, "123.45", 15239.9025, 5);
  RD(varchar, "123.45", varchar, "-123.45", -15239.9025, 5);
  RD(varchar, "123.45", varchar, "3.4E+38", 4.1973E+40, 5);
  RD(varchar, "123.45", varchar, "-3.4E+38", -4.1973E+40, 5);
  RD(varchar, "-123.45", varchar, "0", 0, 5);
  RD(varchar, "-123.45", varchar, "123.45", -15239.9025, 5);
  RD(varchar, "-123.45", varchar, "-123.45", 15239.9025, 5);
  RD(varchar, "-123.45", varchar, "3.4E+38", -4.1973E+40, 5);
  RD(varchar, "-123.45", varchar, "-3.4E+38", 4.1973E+40, 5);
  RD(varchar, "3.4E+38", varchar, "0", 0, 5);
  RD(varchar, "3.4E+38", varchar, "123.45", 4.1973E+40, 5);
  RD(varchar, "3.4E+38", varchar, "-123.45", -4.1973E+40, 5);
  RD(varchar, "3.4E+38", varchar, "3.4E+38", 1.156E+77, 5);
  RD(varchar, "3.4E+38", varchar, "-3.4E+38", -1.156E+77, 5);
  RD(varchar, "-3.4E+38", varchar, "0", 0, 5);
  RD(varchar, "-3.4E+38", varchar, "123.45", -4.1973E+40, 5);
  RD(varchar, "-3.4E+38", varchar, "-123.45", 4.1973E+40, 5);
  RD(varchar, "-3.4E+38", varchar, "3.4E+38", -1.156E+77, 5);
  RD(varchar, "-3.4E+38", varchar, "-3.4E+38", 1.156E+77, 5);
  //edge
  RD(varchar, "1.79e+308", varchar, "1", 1.79e+308, 5);
  FE(varchar, "1.79e+308", varchar, "10");
  FE(varchar, "1.79e+308", varchar, "1.79e+308");
  RD(varchar, "1.79e+308", varchar, "1", 1.79e+308, 5);
  FE(varchar, "1.79e+308", varchar, "10");
  FE(varchar, "1.79e+308", varchar, "1.79e+308");
  RD(varchar, "-1.79e+308", varchar, "1", -1.79e+308, 5);
  FE(varchar, "-1.79e+308", varchar, "10");
  FE(varchar, "-1.79e+308", varchar, "1.79e+308");
  RD(varchar, "-1.79e+308", varchar, "1", -1.79e+308, 5);
  FE(varchar, "-1.79e+308", varchar, "10");
  FE(varchar, "-1.79e+308", varchar, "1.79e+308");
}
*/

TEST_F(ObExprMulTest, null_test)
{
  ObMalloc buf;
  number::ObNumber num1;
  num1.from("123", buf);
  //null any
  R(null, , tinyint, 123, null, );
  R(null, , smallint, 123, null, );
  R(null, , mediumint, 123, null, );
  R(null, , int32, 123, null, );
  R(null, , int, 123, null, );
  R(null, , utinyint, 123, null, );
  R(null, , usmallint, 123, null, );
  R(null, , umediumint, 123, null, );
  R(null, , uint32, 123, null, );
  R(null, , uint64, 123, null, );
  R(null, , float, 123, null, );
  R(null, , double, 123, null, );
  R(null, , number, num1, null, );
  R(null, , varchar, "123", null, );
  //any null
  R(tinyint, 123, null, , null, );
  R(smallint, 123, null, , null, );
  R(mediumint, 123, null, , null, );
  R(int32, 123, null, , null, );
  R(int, 123, null, , null, );
  R(utinyint, 123, null, , null, );
  R(usmallint, 123, null, , null, );
  R(umediumint, 123, null, , null, );
  R(uint32, 123, null, , null, );
  R(uint64, 123, null, , null, );
  R(float, 123, null, , null, );
  R(double, 123, null, , null, );
  R(number, num1, null, , null, );
  R(varchar, "123", null, , null, );
}

TEST_F(ObExprMulTest, int_uint_test)
{
  ObMalloc buf;
  //int_int
  R(int, 234, int, 7867, int, 1840878);
  R(int, -3490, int, 465, int, -1622850);
  R(int, INT64_MIN, int, 1, int, INT64_MIN);
  R(int, 0, int, INT64_MAX, int, 0);
  R(int, 0, int, INT64_MIN, int, 0);

  //int_unit
  R(int, 0, uint64, UINT64_MAX, uint64, 0);
  R(int, 535, uint64, 4545, uint64, 2431575);

}


TEST_F(ObExprMulTest, float_double_test)
{
  ObMalloc buf;
  //basic_test
  R(float, static_cast<float>(2323), float, static_cast<float>(23), double, 53429);
  R(float, 3, float, 2, double, 3 * 2.0);
  R(float, 0, float, 34, double, 0.0);
  R(float, 323.0, double, 46.66, double, static_cast<double>(323.0) * 46.66);
  R(float, 3, float, 2, double, 3 * 2.0);
  R(float, 0, float, 34, double, 0.0);
  R(double, 32323.4, double, 4656.66, double, static_cast<double>(32323.4) * 4656.66);
  R(double, 3, double, 2, double, 3 * 2.0);
  R(double, 0, double, 34, double, 0.0);


  //float_double_int_uint
  R(float, 0, int, INT64_MAX, double, 0.0);
  R(double, 0, int, INT64_MAX, double, 0.0);
  R(float, 3434, int, INT64_MIN, double, 3434.0 * static_cast<double>(INT64_MIN));
  R(double, 3454.23, uint64, 7000, double, 24179610.00);
}


TEST_F(ObExprMulTest, number_test)
{
  ObMalloc buf;

  number::ObNumber num123_0;
  num123_0.from("123.0", buf);
  number::ObNumber num2_2;
  num2_2.from("2.2", buf);
  number::ObNumber num_270_6;
  num_270_6.from("270.6", buf);
  number::ObNumber num_zero;
  num_zero.from("0", buf);
  number::ObNumber numm246_0;
  numm246_0.from("-246", buf);
  number::ObNumber numm2_0;
  numm2_0.from("-2", buf);
  number::ObNumber num492_0;
  num492_0.from("492.0", buf);

  R(number, num123_0, number, num2_2, number, num_270_6);
  R(number, num_zero, number, numm246_0, number, num_zero);
  R(number, numm246_0, number, numm2_0, number, num492_0);
}

/*
TEST_F(ObExprMulTest, string_test)
{
  ObMalloc buf;
  //string_int_uint
  R(varchar, "", int, INT64_MAX, double, 0);
  R(varchar, "abc", int, INT64_MIN, double, 0);
  R(varchar, "abc", uint64, 231988239893819, double, 0);

  R(varchar, "-9223372036854775808abc", int, -1, double, static_cast<double>(INT64_MAX));
  R(varchar, "18446744073709551615abd13", uint64, 1, double, static_cast<double>(UINT64_MAX));

  //string_double
  R(varchar, "123abc", double, 1, double, 123);
  R(varchar, "-123abc", double, 123, double, -15129);
  R(varchar, "-345abc12", double, 23.45, double, static_cast<double>(-345 * 23.45));
  R(varchar, "345abc456", double, 123.3, double, 345 * 123.3);
  R(varchar, "-123.5abc", double, 23.5, double, -123.5 * 23.5);
  R(varchar, "456.45", double, 12.45, double, 456.45 * 12.45);
}
*/

TEST_F(ObExprMulTest, overflow_test)
{
  ObMalloc buf;

  // int_int
  FE(int, INT64_MAX, int, INT64_MAX);
  FE(int, INT64_MIN, int, -2);

  //int_uint
  FE(int, INT64_MAX, uint64, UINT64_MAX);
  FE(int, INT64_MIN, uint64, UINT64_MAX);
  FE(uint64, UINT64_MAX, int, 2);
  FE(int, INT64_MIN, uint64, 5);
  FE(uint64, UINT64_MAX, int, -200);
  FE(uint64, UINT64_MAX, int, 200);

  // uint_uint
  FE(uint64, UINT64_MAX, uint64, UINT64_MAX);
  FE(uint64, 100, uint64, UINT64_MAX);
  FE(uint64, UINT64_MAX, uint64, 100);

  //double
  //FE(double, 213813138982198398198392189389182938.321, double, 213813138982198398198392189389182938.321);

}



/*TEST_F(ObExprMulTest, basic_test)
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
  R(int, 1, int, 2, 2);
  R(int, 2, float, 3.0, 6.0);
  R(int, 3, double, 4.0, 12.0);
  R(int, 4, datetime, 5, OB_ERR_INVALID_TYPE_FOR_OP);
  R(int, 5, precise_datetime, 6, OB_ERR_INVALID_TYPE_FOR_OP);
  R(int, 6, varchar, "7", 42);
  R(int, 6, varchar, "a", OB_ERR_CAST_VARCHAR_TO_NUMBER);
  R(int, 6, varchar, "07a", OB_ERR_CAST_VARCHAR_TO_NUMBER);
  R(int, 8, ctime, 9, OB_ERR_INVALID_TYPE_FOR_OP);
  R(int, 9, mtime, 10, OB_ERR_INVALID_TYPE_FOR_OP);
  R(int, 10, ext, 2, OB_ERR_INVALID_TYPE_FOR_OP);
  R(int, 11, bool, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  R(int, 11, bool, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  //R(null, 0, , 2, ObNullType); decimal not supported

  //ObFloatType
  R(float, 0.0, null, 1, ObNullType);
  R(float, 1.0, int, 2, 2.0);
  R(float, 2.0, float, 3.0, 6.0);
  R(float, 3.0, double, 4.0, 12.0);
  R(float, 4.0, datetime, 5, OB_ERR_INVALID_TYPE_FOR_OP);
  R(float, 5.0, precise_datetime, 6, OB_ERR_INVALID_TYPE_FOR_OP);
  R(float, 6.0, varchar, "7", 42.0);
  R(float, 6.0, varchar, "a", OB_ERR_CAST_VARCHAR_TO_NUMBER);
  R(float, 6.0, varchar, "07a", OB_ERR_CAST_VARCHAR_TO_NUMBER);
  R(float, 6.0, varchar, "0.7a", OB_ERR_CAST_VARCHAR_TO_NUMBER);
  R(float, 8.0, ctime, 9, OB_ERR_INVALID_TYPE_FOR_OP);
  R(float, 9.0, mtime, 10, OB_ERR_INVALID_TYPE_FOR_OP);
  R(float, 10.0, ext, 2, OB_ERR_INVALID_TYPE_FOR_OP);
  R(float, 11.0, bool, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  R(float, 11.0, bool, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  //R(null, 0, , 2, OB_ERR_INVALID_TYPE_FOR_OP);

  //ObDoubleType
  R(double, 0.0, null, 1, ObNullType);
  R(double, 1.0, int, 2, 2.0);
  R(double, 2.0, float, 3.0, 6.0);
  R(double, 3.0, double, 4.0, 12.0);
  R(double, 4.0, datetime, 5, OB_ERR_INVALID_TYPE_FOR_OP);
  R(double, 5.0, precise_datetime, 6, OB_ERR_INVALID_TYPE_FOR_OP);
  R(double, 6.0, varchar, "7", 42.0);
  R(double, 6.0, varchar, "a", OB_ERR_CAST_VARCHAR_TO_NUMBER);
  R(double, 6.0, varchar, "07a", OB_ERR_CAST_VARCHAR_TO_NUMBER);
  R(double, 6.0, varchar, "0.7a", OB_ERR_CAST_VARCHAR_TO_NUMBER);
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
  R(varchar, "1", int, 2, 2);
  R(varchar, "2", float, 3.0, 6.0);
  R(varchar, "2.12345", float, 3.0, 6.37035);
  R(varchar, "3", double, 4.0, 12.0);
  R(varchar, "3.12345", double, 4.0, 12.49380);
  R(varchar, "4", datetime, 5, OB_ERR_INVALID_TYPE_FOR_OP);
  R(varchar, "5", precise_datetime, 6, OB_ERR_INVALID_TYPE_FOR_OP);
  R(varchar, "6", varchar, "7", 42);
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
  //R(null, 0, , 2, OB_ERR_INVALID_TYPE_FOR_OP);

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
  //R(null, 0, , 2, OB_ERR_INVALID_TYPE_FOR_OP);

  //R(null, 0, , 2, OB_ERR_INVALID_TYPE_FOR_OP);
}
*/

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
