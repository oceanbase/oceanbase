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
#include "sql/engine/expr/ob_expr_mod.h"
#include "ob_expr_test_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
class ObExprModTest: public ::testing::Test
{
  public:
    ObExprModTest();
    virtual ~ObExprModTest();
    virtual void SetUp();
    virtual void TearDown();
  private:
    // disallow copy
    ObExprModTest(const ObExprModTest &other);
    ObExprModTest& operator=(const ObExprModTest &other);
  protected:
    // data members
};

ObExprModTest::ObExprModTest()
{
}

ObExprModTest::~ObExprModTest()
{
}

void ObExprModTest::SetUp()
{
}

void ObExprModTest::TearDown()
{
}

/*
#define R(t1, v1, t2, v2, res) ARITH_EXPECT(ObExprMod, &buf, calc_result2, t1, v1, t2, v2, res)
#define E(t1, v1, t2, v2, res) ARITH_ERROR(ObExprMod, &buf, calc_result2, t1, v1, t2, v2, res)
#define T(t1, t2, res) ARITH_EXPECT_TYPE(ObExprMod, calc_result_type2, t1, t2, res)
#define TE(t1, t2, res) ARITH_EXPECT_TYPE_WITH_ROW(ObExprMod, calc_result_type2, t1, t2, res)
*/
#define R(t1, v1, t2, v2, rt, res) ARITH_EXPECT_OBJ(ObExprMod, &buf, calc, t1, v1, t2, v2, rt, res)

TEST_F(ObExprModTest, int_int_test)
{
	ObMalloc buf;
  R(int, 5, int, 12, int, 5);
  R(int, 12, int, 5, int, 2);
  R(int, -5, int, 12, int, -5);
  R(int, 12, int, -5, int, 2);
  R(int, 5, int, -12, int, 5);
  R(int, -12, int, 5, int, -2);
  R(int, -5, int, -12, int, -5);
  R(int, -12, int, -5, int, -2);
  R(int, INT64_MAX, int, INT64_MIN, int, INT64_MAX);
  R(int, INT64_MIN, int, INT64_MAX, int, -1);
}

TEST_F(ObExprModTest, uint_uint_test)
{
  ObMalloc buf;
  R(uint64, 5, uint64, 12, int, 5);
  R(uint64, 12, uint64, 5, int, 2);
  R(uint64, UINT64_MAX - 1, uint64, UINT64_MAX, uint64, UINT64_MAX - 1);
  R(uint64, UINT64_MAX, uint64, UINT64_MAX - 1, uint64, 1);
}

TEST_F(ObExprModTest, int_uint_test)
{
	ObMalloc buf;
  R(int, 5, uint64, 12, int, 5);
  R(uint64, 12, int, 5, int, 2);
  R(int, -5, uint64, 12, int, -5);
  R(uint64, 12, int, -5, int, 2);
  R(int, 12, uint64, 5, int, 2);
  R(uint64, 5, int, 12, int, 5);
  R(int, -12, uint64, 5, int, -2);
  R(uint64, 5, int, -12, int, 5);
  R(int, INT64_MAX, uint64, UINT64_MAX, int, INT64_MAX);
  R(uint64, UINT64_MAX, int, INT64_MAX, int, 1);
  R(int, INT64_MIN, uint64, UINT64_MAX, int, INT64_MIN);
  R(uint64, UINT64_MAX, int, INT64_MIN, int, INT64_MAX);
}

TEST_F(ObExprModTest, float_test)
{
  ObMalloc buf;
  R(float, 1.0, int, 2, double, 1.0);
  R(float, 2.0, float, 3.0, double, 2.0);
  R(float, 3.0, double, 4.0, double, 3.0);
  R(float, 4.0, double, -3.0, double, 1.0);
}

TEST_F(ObExprModTest, float_double)
{
	ObMalloc buf;
  R(float, 1.0, int, 2, double, 1.0);
  R(float, 2.0, float, 3.0, double, 2.0);
  R(float, 3.0, double, 4.0, double, 3.0);
  R(float, 4.0, double, -3.0, double, 1.0);
  R(double, 3.0, double, 4.0, double, 3.0);
  R(double, 4.0, double, -3.0, double, 1.0);
  R(double, -4.2, double, -3.0, double, -1.2);
}

TEST_F(ObExprModTest, varchar)
{
	ObMalloc buf;

	R(varchar, "1", int, 2, double, 1.0);
	R(varchar, "4.2", float, 3.0, double, 1.2);
	R(varchar, "-2.12345", float, 3.0, double, -2.12345);
	R(varchar, "30.5", double, 4.5, double, 3.5);
	R(varchar, "9.12345", double, 4.0, double, 1.12345);
}

TEST_F(ObExprModTest, mod_zero_test)
{
	ObMalloc buf;

}

/*
TEST_F(ObExprModTest, result_test)
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
  R(null, 0, unknown, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  R(null, 0, ctime, 9, OB_ERR_INVALID_TYPE_FOR_OP);
  R(null, 0, mtime, 10, OB_ERR_INVALID_TYPE_FOR_OP);
  R(null, 0, ext, 2, OB_ERR_INVALID_TYPE_FOR_OP);
  R(null, 0, bool, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  //R(null, 0, , 2, ObNullType); decimal not supported

  //ObIntType
  R(int, 0, null, 1, ObNullType);
  R(int, 1, int, 2, 1);
  R(int, 2, float, 3.0, 2.0);
  R(int, 20, float, 3.0, 2.0);
  R(int, 20, float, float(3.9), 0.5);
  R(int, 20, float, float(-3.9), 0.5);
  R(int, -20, float, float(-3.9), -0.5);
  R(int, 3, double, 4.0, 3.0);
  R(int, 4, datetime, 5, OB_ERR_INVALID_TYPE_FOR_OP);
  R(int, 5, precise_datetime, 6, OB_ERR_INVALID_TYPE_FOR_OP);
  R(int, 6, varchar, "7", 6.0);
  R(int, 6, varchar, "a", OB_ERR_CAST_VARCHAR_TO_NUMBER);
  R(int, 6, varchar, "07a", OB_ERR_CAST_VARCHAR_TO_NUMBER);
  R(int, 7, unknown, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  R(int, 18, ctime, 9, OB_ERR_INVALID_TYPE_FOR_OP);
  R(int, 19, mtime, 10, OB_ERR_INVALID_TYPE_FOR_OP);
  R(int, 10, ext, 2, OB_ERR_INVALID_TYPE_FOR_OP);
  R(int, 11, bool, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  R(int, 11, bool, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  //R(null, 0, , 2, ObNullType); decimal not supported

  //ObFloatType
  R(float, 0.0, null, 1, ObNullType);
  R(float, 1.0, int, 2, 1.0);
  R(float, 2.0, float, 3.0, 2.0);
  R(float, 3.0, double, 4.0, 3.0);
  R(float, 4.0, double, -3.0, 1.0);
  R(float, float(-4.2), double, -3.0, -1.2);
  R(float, 4.0, datetime, 5, OB_ERR_INVALID_TYPE_FOR_OP);
  R(float, 5.0, precise_datetime, 6, OB_ERR_INVALID_TYPE_FOR_OP);
  R(float, float(6.1), varchar, "7", 6.1);
  R(float, float(6.2), varchar, "a", OB_ERR_CAST_VARCHAR_TO_NUMBER);
  R(float, float(6.3), varchar, "06a", OB_ERR_CAST_VARCHAR_TO_NUMBER);
  R(float, 6.0, varchar, "-0.7a", OB_ERR_CAST_VARCHAR_TO_NUMBER);
  R(float, 7.0, unknown, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  R(float, 8.0, ctime, 9, OB_ERR_INVALID_TYPE_FOR_OP);
  R(float, 9.0, mtime, 10, OB_ERR_INVALID_TYPE_FOR_OP);
  R(float, 10.0, ext, 2, OB_ERR_INVALID_TYPE_FOR_OP);
  R(float, 11.0, bool, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  R(float, 11.0, bool, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  //R(null, 0, , 2, ObNullType); decimal not supported

  //ObDoubleType
  R(double, 0.0, null, 1, ObNullType);
  R(double, 1.0, int, 2, 1.0);
  R(double, 2.0, float, 3.0, 2.0);
  R(double, 12.4, float, float(3.6), 1.6);
  R(double, 3.0, double, 4.0, 3.0);
  R(double, 4.0, double, -3.0, 1.0);
  R(double, -4.2, double, -3.0, -1.2);
  R(double, 4.0, datetime, 5, OB_ERR_INVALID_TYPE_FOR_OP);
  R(double, 5.0, precise_datetime, 6, OB_ERR_INVALID_TYPE_FOR_OP);
  R(double, 6.1, varchar, "7", 6.1);
  R(double, 6.2, varchar, "a", OB_ERR_CAST_VARCHAR_TO_NUMBER);
  R(double, 6.3, varchar, "06a", OB_ERR_CAST_VARCHAR_TO_NUMBER);
  R(double, 6.0, varchar, "-0.7a", OB_ERR_CAST_VARCHAR_TO_NUMBER);
  R(double, 7.0, unknown, 0, OB_ERR_INVALID_TYPE_FOR_OP);
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
  R(datetime, 20, float, 3.0, OB_ERR_INVALID_TYPE_FOR_OP);
  R(datetime, 3, double, 4.0, OB_ERR_INVALID_TYPE_FOR_OP);
  R(datetime, 4, datetime, 5, OB_ERR_INVALID_TYPE_FOR_OP);
  R(datetime, 5, precise_datetime, 6, OB_ERR_INVALID_TYPE_FOR_OP);
  R(datetime, 6, varchar, "7", OB_ERR_INVALID_TYPE_FOR_OP);
  R(datetime, 6, varchar, "a", OB_ERR_INVALID_TYPE_FOR_OP);
  R(datetime, 6, varchar, "07a", OB_ERR_INVALID_TYPE_FOR_OP);
  R(datetime, 7, unknown, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  R(datetime, 18, ctime, 9, OB_ERR_INVALID_TYPE_FOR_OP);
  R(datetime, 19, mtime, 10, OB_ERR_INVALID_TYPE_FOR_OP);
  R(datetime, 10, ext, 2, OB_ERR_INVALID_TYPE_FOR_OP);
  R(datetime, 11, bool, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  R(datetime, 11, bool, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  //R(null, 0, , 2, ObNullType); decimal not supported

  //ObPreciseDateTimeType
  R(precise_datetime, 0, null, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  R(precise_datetime, 1, int, 2, OB_ERR_INVALID_TYPE_FOR_OP);
  R(precise_datetime, 2, float, 3.0, OB_ERR_INVALID_TYPE_FOR_OP);
  R(precise_datetime, 20, float, 3.0, OB_ERR_INVALID_TYPE_FOR_OP);
  R(precise_datetime, 3, double, 4.0, OB_ERR_INVALID_TYPE_FOR_OP);
  R(precise_datetime, 4, datetime, 5, OB_ERR_INVALID_TYPE_FOR_OP);
  R(precise_datetime, 5, precise_datetime, 6, OB_ERR_INVALID_TYPE_FOR_OP);
  R(precise_datetime, 6, varchar, "7", OB_ERR_INVALID_TYPE_FOR_OP);
  R(precise_datetime, 6, varchar, "a", OB_ERR_INVALID_TYPE_FOR_OP);
  R(precise_datetime, 6, varchar, "07a", OB_ERR_INVALID_TYPE_FOR_OP);
  R(precise_datetime, 7, unknown, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  R(precise_datetime, 18, ctime, 9, OB_ERR_INVALID_TYPE_FOR_OP);
  R(precise_datetime, 19, mtime, 10, OB_ERR_INVALID_TYPE_FOR_OP);
  R(precise_datetime, 10, ext, 2, OB_ERR_INVALID_TYPE_FOR_OP);
  R(precise_datetime, 11, bool, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  R(precise_datetime, 11, bool, 0, OB_ERR_INVALID_TYPE_FOR_OP);

  //ObVarcharType
  R(varchar, "0", null, 1, ObNullType);
  R(varchar, "1", int, 2, 1.0);
  R(varchar, "4.2", float, 3.0, 1.2);
  R(varchar, "-2.12345", float, 3.0, -2.12345);
  R(varchar, "30.5", double, 4.5, 3.5);
  R(varchar, "9.12345", double, 4.0, 1.12345);
  R(varchar, "hello", datetime, 5, OB_ERR_INVALID_TYPE_FOR_OP);
  R(varchar, "6hello", precise_datetime, 6, OB_ERR_INVALID_TYPE_FOR_OP);
  R(varchar, "hello6", varchar, "7", OB_ERR_CAST_VARCHAR_TO_NUMBER);
  R(varchar, "df", varchar, "a", OB_ERR_CAST_VARCHAR_TO_NUMBER);
  R(varchar, "6", varchar, "07a", OB_ERR_CAST_VARCHAR_TO_NUMBER);
  R(varchar, "-7", unknown, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  R(varchar, "-8", ctime, -129, OB_ERR_INVALID_TYPE_FOR_OP);
  R(varchar, "-1239", mtime, -34, OB_ERR_INVALID_TYPE_FOR_OP);
  R(varchar, "10", ext, 2, OB_ERR_INVALID_TYPE_FOR_OP);
  R(varchar, "11he", bool, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  R(varchar, "11", bool, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  //R(null, 0, , 2, ObNullType); decimal not supported

  //ObCreateTimeType
  R(ctime, 0, null, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  R(ctime, 1, int, 2, OB_ERR_INVALID_TYPE_FOR_OP);
  R(ctime, 2, float, 3.0, OB_ERR_INVALID_TYPE_FOR_OP);
  R(ctime, 20, float, 3.0, OB_ERR_INVALID_TYPE_FOR_OP);
  R(ctime, 3, double, 4.0, OB_ERR_INVALID_TYPE_FOR_OP);
  R(ctime, 4, datetime, 5, OB_ERR_INVALID_TYPE_FOR_OP);
  R(ctime, 5, precise_datetime, 6, OB_ERR_INVALID_TYPE_FOR_OP);
  R(ctime, 6, varchar, "7", OB_ERR_INVALID_TYPE_FOR_OP);
  R(ctime, 6, varchar, "a", OB_ERR_INVALID_TYPE_FOR_OP);
  R(ctime, 6, varchar, "07a", OB_ERR_INVALID_TYPE_FOR_OP);
  R(ctime, 7, unknown, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  R(ctime, 18, ctime, 9, OB_ERR_INVALID_TYPE_FOR_OP);
  R(ctime, 19, mtime, 10, OB_ERR_INVALID_TYPE_FOR_OP);
  R(ctime, 10, ext, 2, OB_ERR_INVALID_TYPE_FOR_OP);
  R(ctime, 11, bool, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  R(ctime, 11, bool, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  //R(null, 0, , 2, ObNullType); decimal not supported

  //ObModifyTimeType
  R(mtime, 0, null, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  R(mtime, 1, int, 2, OB_ERR_INVALID_TYPE_FOR_OP);
  R(mtime, 2, float, 3.0, OB_ERR_INVALID_TYPE_FOR_OP);
  R(mtime, 20, float, 3.0, OB_ERR_INVALID_TYPE_FOR_OP);
  R(mtime, 3, double, 4.0, OB_ERR_INVALID_TYPE_FOR_OP);
  R(mtime, 4, datetime, 5, OB_ERR_INVALID_TYPE_FOR_OP);
  R(mtime, 5, precise_datetime, 6, OB_ERR_INVALID_TYPE_FOR_OP);
  R(mtime, 6, varchar, "7", OB_ERR_INVALID_TYPE_FOR_OP);
  R(mtime, 6, varchar, "a", OB_ERR_INVALID_TYPE_FOR_OP);
  R(mtime, 6, varchar, "07a", OB_ERR_INVALID_TYPE_FOR_OP);
  R(mtime, 7, unknown, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  R(mtime, 18, ctime, 9, OB_ERR_INVALID_TYPE_FOR_OP);
  R(mtime, 19, mtime, 10, OB_ERR_INVALID_TYPE_FOR_OP);
  R(mtime, 10, ext, 2, OB_ERR_INVALID_TYPE_FOR_OP);
  R(mtime, 11, bool, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  R(mtime, 11, bool, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  //R(null, 0, , 2, ObNullType); decimal not supported

  //ObBoolType
  R(bool, 0, null, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  R(bool, 1, null, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  R(bool, 0, int, 2, OB_ERR_INVALID_TYPE_FOR_OP);
  R(bool, 1, int, 2, OB_ERR_INVALID_TYPE_FOR_OP);
  R(bool, 0, float, float(0.3), OB_ERR_INVALID_TYPE_FOR_OP);
  R(bool, 1, float, float(0.3), OB_ERR_INVALID_TYPE_FOR_OP);
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
  R(bool, 7, unknown, 0, OB_ERR_INVALID_TYPE_FOR_OP);
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
  R(ext, 0, unknown, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  R(ext, 0, ctime, 9, OB_ERR_INVALID_TYPE_FOR_OP);
  R(ext, 0, mtime, 10, OB_ERR_INVALID_TYPE_FOR_OP);
  R(ext, 0, ext, 2, OB_ERR_INVALID_TYPE_FOR_OP);
  R(ext, 0, bool, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  R(ext, 0, bool, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  //R(null, 0, , 2, OB_ERR_INVALID_TYPE_FOR_OP);

  //ObUnknownType
  R(unknown, 0, null, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  R(unknown, 0, int, 2, OB_ERR_INVALID_TYPE_FOR_OP);
  R(unknown, 0, float, 3.0, OB_ERR_INVALID_TYPE_FOR_OP);
  R(unknown, 0, double, 4.0, OB_ERR_INVALID_TYPE_FOR_OP);
  R(unknown, 0, datetime, 5, OB_ERR_INVALID_TYPE_FOR_OP);
  R(unknown, 0, precise_datetime, 6, OB_ERR_INVALID_TYPE_FOR_OP);
  R(unknown, 0, varchar, "7", OB_ERR_INVALID_TYPE_FOR_OP);
  R(unknown, 0, varchar, "a", OB_ERR_INVALID_TYPE_FOR_OP);
  R(unknown, 0, varchar, "07a", OB_ERR_INVALID_TYPE_FOR_OP);
  R(unknown, 0, unknown, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  R(unknown, 0, ctime, 9, OB_ERR_INVALID_TYPE_FOR_OP);
  R(unknown, 0, mtime, 10, OB_ERR_INVALID_TYPE_FOR_OP);
  R(unknown, 0, ext, 2, OB_ERR_INVALID_TYPE_FOR_OP);
  R(unknown, 0, bool, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  R(unknown, 0, bool, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  //R(null, 0, , 2, OB_ERR_INVALID_TYPE_FOR_OP);
}
*/


int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
