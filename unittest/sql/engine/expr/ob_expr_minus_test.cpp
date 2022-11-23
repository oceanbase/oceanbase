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

#include <math.h>
#include <float.h>
#include <gtest/gtest.h>
#include "sql/engine/expr/ob_expr_minus.h"
#include "ob_expr_test_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

class ObExprMinusTest: public ::testing::Test
{
  public:
    ObExprMinusTest();
    virtual ~ObExprMinusTest();
    virtual void SetUp();
    virtual void TearDown();
  private:
    // disallow copy
    ObExprMinusTest(const ObExprMinusTest &other);
    ObExprMinusTest& operator=(const ObExprMinusTest &other);
  protected:
    // data members
};

ObExprMinusTest::ObExprMinusTest()
{
}

ObExprMinusTest::~ObExprMinusTest()
{
}

void ObExprMinusTest::SetUp()
{
}

void ObExprMinusTest::TearDown()
{
}


#define R(t1, v1, t2, v2, rt, res) ARITH_EXPECT_OBJ(ObExprMinus, &buf, calc, t1, v1, t2, v2, rt, res)
#define FE(t1, v1, t2, v2) ARITH_EXPECT_ERROR(ObExprMinus, &buf, calc, t1, v1, t2, v2)
#define RD(t1, v1, t2, v2, res, p) ARITH_EXPECT_OBJ_DOUBLE(ObExprMinus, &buf, calc, t1, v1, t2, v2, res, p)
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


TEST_F(ObExprMinusTest, tiny_utiny)
{
  ObMalloc buf;
  //tiny vs utiny
  FE(tinyint, -128, utinyint, 0);
  FE(tinyint, -128, utinyint, 64);
  FE(tinyint, -128, utinyint, 128);
  FE(tinyint, -128, utinyint, 192);
  FE(tinyint, -128, utinyint, 255);
  FE(tinyint, -64, utinyint, 0);
  FE(tinyint, -64, utinyint, 64);
  FE(tinyint, -64, utinyint, 128);
  FE(tinyint, -64, utinyint, 192);
  FE(tinyint, -64, utinyint, 255);
  FE(tinyint, 0, utinyint, 64);
  FE(tinyint, 0, utinyint, 128);
  FE(tinyint, 0, utinyint, 192);
  FE(tinyint, 0, utinyint, 255);
  FE(tinyint, 64, utinyint, 128);
  FE(tinyint, 64, utinyint, 192);
  FE(tinyint, 64, utinyint, 255);
  FE(tinyint, 127, utinyint, 128);
  FE(tinyint, 127, utinyint, 192);
  FE(tinyint, 127, utinyint, 255);

  R(tinyint, 0, utinyint, 0, uint64, 0);
  R(tinyint, 64, utinyint, 0, uint64, 64);
  R(tinyint, 64, utinyint, 64, uint64, 0);
  R(tinyint, 127, utinyint, 0, uint64, 127);
  R(tinyint, 127, utinyint, 64, uint64, 63);
  //utiny vs tiny
  R(utinyint, 0, tinyint, -128, uint64, 128);
  R(utinyint, 64, tinyint, -128, uint64, 192);
  R(utinyint, 128, tinyint, -128, uint64, 256);
  R(utinyint, 192, tinyint, -128, uint64, 320);
  R(utinyint, 255, tinyint, -128, uint64, 383);
  R(utinyint, 0, tinyint, -64, uint64, 64);
  R(utinyint, 64, tinyint, -64, uint64, 128);
  R(utinyint, 128, tinyint, -64, uint64, 192);
  R(utinyint, 192, tinyint, -64, uint64, 256);
  R(utinyint, 255, tinyint, -64, uint64, 319);
  R(utinyint, 0, tinyint, 0, uint64, 0);
  R(utinyint, 64, tinyint, 0, uint64, 64);
  R(utinyint, 128, tinyint, 0, uint64, 128);
  R(utinyint, 192, tinyint, 0, uint64, 192);
  R(utinyint, 255, tinyint, 0, uint64, 255);
  R(utinyint, 64, tinyint, 64, uint64, 0);
  R(utinyint, 128, tinyint, 64, uint64, 64);
  R(utinyint, 192, tinyint, 64, uint64, 128);
  R(utinyint, 255, tinyint, 64, uint64, 191);
  R(utinyint, 128, tinyint, 127, uint64, 1);
  R(utinyint, 192, tinyint, 127, uint64, 65);
  R(utinyint, 255, tinyint, 127, uint64, 128);

  FE(utinyint, 0, tinyint, 64);
  FE(utinyint, 0, tinyint, 127);
  FE(utinyint, 64, tinyint, 127);
}


TEST_F(ObExprMinusTest, tiny_small)
{
  ObMalloc buf;
  //tinyint smallint
  R(tinyint, -128, smallint, -32768, int, 32640);
  R(tinyint, -128, smallint, -16384, int, 16256);
  R(tinyint, -128, smallint, 0, int, -128);
  R(tinyint, -128, smallint, 16384, int, -16512);
  R(tinyint, -128, smallint, 32767, int, -32895);
  R(tinyint, -128, smallint, -128, int, 0);
  R(tinyint, -128, smallint, -64, int, -64);
  R(tinyint, -128, smallint, 64, int, -192);
  R(tinyint, -128, smallint, 127, int, -255);
  R(tinyint, -64, smallint, -32768, int, 32704);
  R(tinyint, -64, smallint, -16384, int, 16320);
  R(tinyint, -64, smallint, 0, int, -64);
  R(tinyint, -64, smallint, 16384, int, -16448);
  R(tinyint, -64, smallint, 32767, int, -32831);
  R(tinyint, -64, smallint, -128, int, 64);
  R(tinyint, -64, smallint, -64, int, 0);
  R(tinyint, -64, smallint, 64, int, -128);
  R(tinyint, -64, smallint, 127, int, -191);
  R(tinyint, 0, smallint, -32768, int, 32768);
  R(tinyint, 0, smallint, -16384, int, 16384);
  R(tinyint, 0, smallint, 0, int, 0);
  R(tinyint, 0, smallint, 16384, int, -16384);
  R(tinyint, 0, smallint, 32767, int, -32767);
  R(tinyint, 0, smallint, -128, int, 128);
  R(tinyint, 0, smallint, -64, int, 64);
  R(tinyint, 0, smallint, 64, int, -64);
  R(tinyint, 0, smallint, 127, int, -127);
  R(tinyint, 64, smallint, -32768, int, 32832);
  R(tinyint, 64, smallint, -16384, int, 16448);
  R(tinyint, 64, smallint, 0, int, 64);
  R(tinyint, 64, smallint, 16384, int, -16320);
  R(tinyint, 64, smallint, 32767, int, -32703);
  R(tinyint, 64, smallint, -128, int, 192);
  R(tinyint, 64, smallint, -64, int, 128);
  R(tinyint, 64, smallint, 64, int, 0);
  R(tinyint, 64, smallint, 127, int, -63);
  R(tinyint, 127, smallint, -32768, int, 32895);
  R(tinyint, 127, smallint, -16384, int, 16511);
  R(tinyint, 127, smallint, 0, int, 127);
  R(tinyint, 127, smallint, 16384, int, -16257);
  R(tinyint, 127, smallint, 32767, int, -32640);
  R(tinyint, 127, smallint, -128, int, 255);
  R(tinyint, 127, smallint, -64, int, 191);
  R(tinyint, 127, smallint, 64, int, 63);
  R(tinyint, 127, smallint, 127, int, 0);
  //smallint tinyint
  R(smallint, -32768, tinyint, -128, int, -32640);
  R(smallint, -16384, tinyint, -128, int, -16256);
  R(smallint, 0, tinyint, -128, int, 128);
  R(smallint, 16384, tinyint, -128, int, 16512);
  R(smallint, 32767, tinyint, -128, int, 32895);
  R(smallint, -128, tinyint, -128, int, 0);
  R(smallint, -64, tinyint, -128, int, 64);
  R(smallint, 64, tinyint, -128, int, 192);
  R(smallint, 127, tinyint, -128, int, 255);
  R(smallint, -32768, tinyint, -64, int, -32704);
  R(smallint, -16384, tinyint, -64, int, -16320);
  R(smallint, 0, tinyint, -64, int, 64);
  R(smallint, 16384, tinyint, -64, int, 16448);
  R(smallint, 32767, tinyint, -64, int, 32831);
  R(smallint, -128, tinyint, -64, int, -64);
  R(smallint, -64, tinyint, -64, int, 0);
  R(smallint, 64, tinyint, -64, int, 128);
  R(smallint, 127, tinyint, -64, int, 191);
  R(smallint, -32768, tinyint, 0, int, -32768);
  R(smallint, -16384, tinyint, 0, int, -16384);
  R(smallint, 0, tinyint, 0, int, 0);
  R(smallint, 16384, tinyint, 0, int, 16384);
  R(smallint, 32767, tinyint, 0, int, 32767);
  R(smallint, -128, tinyint, 0, int, -128);
  R(smallint, -64, tinyint, 0, int, -64);
  R(smallint, 64, tinyint, 0, int, 64);
  R(smallint, 127, tinyint, 0, int, 127);
  R(smallint, -32768, tinyint, 64, int, -32832);
  R(smallint, -16384, tinyint, 64, int, -16448);
  R(smallint, 0, tinyint, 64, int, -64);
  R(smallint, 16384, tinyint, 64, int, 16320);
  R(smallint, 32767, tinyint, 64, int, 32703);
  R(smallint, -128, tinyint, 64, int, -192);
  R(smallint, -64, tinyint, 64, int, -128);
  R(smallint, 64, tinyint, 64, int, 0);
  R(smallint, 127, tinyint, 64, int, 63);
  R(smallint, -32768, tinyint, 127, int, -32895);
  R(smallint, -16384, tinyint, 127, int, -16511);
  R(smallint, 0, tinyint, 127, int, -127);
  R(smallint, 16384, tinyint, 127, int, 16257);
  R(smallint, 32767, tinyint, 127, int, 32640);
  R(smallint, -128, tinyint, 127, int, -255);
  R(smallint, -64, tinyint, 127, int, -191);
  R(smallint, 64, tinyint, 127, int, -63);
  R(smallint, 127, tinyint, 127, int, 0);
}


TEST_F(ObExprMinusTest, tiny_usmall)
{
  ObMalloc buf;
  //tinyint - usmallint
  FE(tinyint, -128, usmallint, 0);
  FE(tinyint, -128, usmallint, 16384);
  FE(tinyint, -128, usmallint, 32768);
  FE(tinyint, -128, usmallint, 49152);
  FE(tinyint, -128, usmallint, 65535);
  FE(tinyint, -128, usmallint, 64);
  FE(tinyint, -128, usmallint, 128);
  FE(tinyint, -128, usmallint, 192);
  FE(tinyint, -128, usmallint, 255);
  FE(tinyint, -64, usmallint, 0);
  FE(tinyint, -64, usmallint, 16384);
  FE(tinyint, -64, usmallint, 32768);
  FE(tinyint, -64, usmallint, 49152);
  FE(tinyint, -64, usmallint, 65535);
  FE(tinyint, -64, usmallint, 64);
  FE(tinyint, -64, usmallint, 128);
  FE(tinyint, -64, usmallint, 192);
  FE(tinyint, -64, usmallint, 255);
  R(tinyint, 0, usmallint, 0, uint64, 0);
  FE(tinyint, 0, usmallint, 16384);
  FE(tinyint, 0, usmallint, 32768);
  FE(tinyint, 0, usmallint, 49152);
  FE(tinyint, 0, usmallint, 65535);
  FE(tinyint, 0, usmallint, 64);
  FE(tinyint, 0, usmallint, 128);
  FE(tinyint, 0, usmallint, 192);
  FE(tinyint, 0, usmallint, 255);
  R(tinyint, 64, usmallint, 0, uint64, 64);
  FE(tinyint, 64, usmallint, 16384);
  FE(tinyint, 64, usmallint, 32768);
  FE(tinyint, 64, usmallint, 49152);
  FE(tinyint, 64, usmallint, 65535);
  R(tinyint, 64, usmallint, 64, uint64, 0);
  FE(tinyint, 64, usmallint, 128);
  FE(tinyint, 64, usmallint, 192);
  FE(tinyint, 64, usmallint, 255);
  R(tinyint, 127, usmallint, 0, uint64, 127);
  FE(tinyint, 127, usmallint, 16384);
  FE(tinyint, 127, usmallint, 32768);
  FE(tinyint, 127, usmallint, 49152);
  FE(tinyint, 127, usmallint, 65535);
  R(tinyint, 127, usmallint, 64, uint64, 63);
  FE(tinyint, 127, usmallint, 128);
  FE(tinyint, 127, usmallint, 192);
  FE(tinyint, 127, usmallint, 255);

  //usmallint  tinyint
  R(usmallint, 0, tinyint, -128, uint64, 128);
  R(usmallint, 16384, tinyint, -128, uint64, 16512);
  R(usmallint, 32768, tinyint, -128, uint64, 32896);
  R(usmallint, 49152, tinyint, -128, uint64, 49280);
  R(usmallint, 65535, tinyint, -128, uint64, 65663);
  R(usmallint, 64, tinyint, -128, uint64, 192);
  R(usmallint, 128, tinyint, -128, uint64, 256);
  R(usmallint, 192, tinyint, -128, uint64, 320);
  R(usmallint, 255, tinyint, -128, uint64, 383);
  R(usmallint, 0, tinyint, -64, uint64, 64);
  R(usmallint, 16384, tinyint, -64, uint64, 16448);
  R(usmallint, 32768, tinyint, -64, uint64, 32832);
  R(usmallint, 49152, tinyint, -64, uint64, 49216);
  R(usmallint, 65535, tinyint, -64, uint64, 65599);
  R(usmallint, 64, tinyint, -64, uint64, 128);
  R(usmallint, 128, tinyint, -64, uint64, 192);
  R(usmallint, 192, tinyint, -64, uint64, 256);
  R(usmallint, 255, tinyint, -64, uint64, 319);
  R(usmallint, 0, tinyint, 0, uint64, 0);
  R(usmallint, 16384, tinyint, 0, uint64, 16384);
  R(usmallint, 32768, tinyint, 0, uint64, 32768);
  R(usmallint, 49152, tinyint, 0, uint64, 49152);
  R(usmallint, 65535, tinyint, 0, uint64, 65535);
  R(usmallint, 64, tinyint, 0, uint64, 64);
  R(usmallint, 128, tinyint, 0, uint64, 128);
  R(usmallint, 192, tinyint, 0, uint64, 192);
  R(usmallint, 255, tinyint, 0, uint64, 255);
  FE(usmallint, 0, tinyint, 64);
  R(usmallint, 16384, tinyint, 64, uint64, 16320);
  R(usmallint, 32768, tinyint, 64, uint64, 32704);
  R(usmallint, 49152, tinyint, 64, uint64, 49088);
  R(usmallint, 65535, tinyint, 64, uint64, 65471);
  R(usmallint, 64, tinyint, 64, uint64, 0);
  R(usmallint, 128, tinyint, 64, uint64, 64);
  R(usmallint, 192, tinyint, 64, uint64, 128);
  R(usmallint, 255, tinyint, 64, uint64, 191);
  FE(usmallint, 0, tinyint, 127);
  R(usmallint, 16384, tinyint, 127, uint64, 16257);
  R(usmallint, 32768, tinyint, 127, uint64, 32641);
  R(usmallint, 49152, tinyint, 127, uint64, 49025);
  R(usmallint, 65535, tinyint, 127, uint64, 65408);
  FE(usmallint, 64, tinyint, 127);
  R(usmallint, 128, tinyint, 127, uint64, 1);
  R(usmallint, 192, tinyint, 127, uint64, 65);
  R(usmallint, 255, tinyint, 127, uint64, 128);
}

TEST_F(ObExprMinusTest, tiny_int32)
{
  ObMalloc buf;
  //tiny  int32
  R(tinyint, -128, int32, -2147483648, int, 2147483520);
  R(tinyint, -128, int32, 0, int, -128);
  R(tinyint, -128, int32, 2147483647, int, -2147483775);
  R(tinyint, -128, int32, -32768, int, 32640);
  R(tinyint, -128, int32, 32767, int, -32895);
  R(tinyint, -128, int32, -128, int, 0);
  R(tinyint, -128, int32, -64, int, -64);
  R(tinyint, -128, int32, 64, int, -192);
  R(tinyint, -128, int32, 127, int, -255);
  R(tinyint, -64, int32, -2147483648, int, 2147483584);
  R(tinyint, -64, int32, 0, int, -64);
  R(tinyint, -64, int32, 2147483647, int, -2147483711);
  R(tinyint, -64, int32, -32768, int, 32704);
  R(tinyint, -64, int32, 32767, int, -32831);
  R(tinyint, -64, int32, -128, int, 64);
  R(tinyint, -64, int32, -64, int, 0);
  R(tinyint, -64, int32, 64, int, -128);
  R(tinyint, -64, int32, 127, int, -191);
  R(tinyint, 0, int32, -2147483648, int, 2147483648);
  R(tinyint, 0, int32, 0, int, 0);
  R(tinyint, 0, int32, 2147483647, int, -2147483647);
  R(tinyint, 0, int32, -32768, int, 32768);
  R(tinyint, 0, int32, 32767, int, -32767);
  R(tinyint, 0, int32, -128, int, 128);
  R(tinyint, 0, int32, -64, int, 64);
  R(tinyint, 0, int32, 64, int, -64);
  R(tinyint, 0, int32, 127, int, -127);
  R(tinyint, 64, int32, -2147483648, int, 2147483712);
  R(tinyint, 64, int32, 0, int, 64);
  R(tinyint, 64, int32, 2147483647, int, -2147483583);
  R(tinyint, 64, int32, -32768, int, 32832);
  R(tinyint, 64, int32, 32767, int, -32703);
  R(tinyint, 64, int32, -128, int, 192);
  R(tinyint, 64, int32, -64, int, 128);
  R(tinyint, 64, int32, 64, int, 0);
  R(tinyint, 64, int32, 127, int, -63);
  R(tinyint, 127, int32, -2147483648, int, 2147483775);
  R(tinyint, 127, int32, 0, int, 127);
  R(tinyint, 127, int32, 2147483647, int, -2147483520);
  R(tinyint, 127, int32, -32768, int, 32895);
  R(tinyint, 127, int32, 32767, int, -32640);
  R(tinyint, 127, int32, -128, int, 255);
  R(tinyint, 127, int32, -64, int, 191);
  R(tinyint, 127, int32, 64, int, 63);
  R(tinyint, 127, int32, 127, int, 0);

  //int32  tiny
  R(int32, -2147483648, tinyint, -128, int, -2147483520);
  R(int32, 0, tinyint, -128, int, 128);
  R(int32, 2147483647, tinyint, -128, int, 2147483775);
  R(int32, -32768, tinyint, -128, int, -32640);
  R(int32, 32767, tinyint, -128, int, 32895);
  R(int32, -128, tinyint, -128, int, 0);
  R(int32, -64, tinyint, -128, int, 64);
  R(int32, 64, tinyint, -128, int, 192);
  R(int32, 127, tinyint, -128, int, 255);
  R(int32, -2147483648, tinyint, -64, int, -2147483584);
  R(int32, 0, tinyint, -64, int, 64);
  R(int32, 2147483647, tinyint, -64, int, 2147483711);
  R(int32, -32768, tinyint, -64, int, -32704);
  R(int32, 32767, tinyint, -64, int, 32831);
  R(int32, -128, tinyint, -64, int, -64);
  R(int32, -64, tinyint, -64, int, 0);
  R(int32, 64, tinyint, -64, int, 128);
  R(int32, 127, tinyint, -64, int, 191);
  R(int32, -2147483648, tinyint, 0, int, -2147483648);
  R(int32, 0, tinyint, 0, int, 0);
  R(int32, 2147483647, tinyint, 0, int, 2147483647);
  R(int32, -32768, tinyint, 0, int, -32768);
  R(int32, 32767, tinyint, 0, int, 32767);
  R(int32, -128, tinyint, 0, int, -128);
  R(int32, -64, tinyint, 0, int, -64);
  R(int32, 64, tinyint, 0, int, 64);
  R(int32, 127, tinyint, 0, int, 127);
  R(int32, -2147483648, tinyint, 64, int, -2147483712);
  R(int32, 0, tinyint, 64, int, -64);
  R(int32, 2147483647, tinyint, 64, int, 2147483583);
  R(int32, -32768, tinyint, 64, int, -32832);
  R(int32, 32767, tinyint, 64, int, 32703);
  R(int32, -128, tinyint, 64, int, -192);
  R(int32, -64, tinyint, 64, int, -128);
  R(int32, 64, tinyint, 64, int, 0);
  R(int32, 127, tinyint, 64, int, 63);
  R(int32, -2147483648, tinyint, 127, int, -2147483775);
  R(int32, 0, tinyint, 127, int, -127);
  R(int32, 2147483647, tinyint, 127, int, 2147483520);
  R(int32, -32768, tinyint, 127, int, -32895);
  R(int32, 32767, tinyint, 127, int, 32640);
  R(int32, -128, tinyint, 127, int, -255);
  R(int32, -64, tinyint, 127, int, -191);
  R(int32, 64, tinyint, 127, int, -63);
  R(int32, 127, tinyint, 127, int, 0);
}

TEST_F(ObExprMinusTest, tiny_uint32)
{
  ObMalloc buf;
  //tiny  uint32
  FE(tinyint, -128, uint32, 0);
  FE(tinyint, -128, uint32, 2147483648);
  FE(tinyint, -128, uint32, 4294967295);
  FE(tinyint, -128, uint32, 32768);
  FE(tinyint, -128, uint32, 64);
  FE(tinyint, -128, uint32, 255);
  FE(tinyint, -64, uint32, 0);
  FE(tinyint, -64, uint32, 2147483648);
  FE(tinyint, -64, uint32, 4294967295);
  FE(tinyint, -64, uint32, 32768);
  FE(tinyint, -64, uint32, 64);
  FE(tinyint, -64, uint32, 255);
  R(tinyint, 0, uint32, 0, uint64, 0);
  FE(tinyint, 0, uint32, 2147483648);
  FE(tinyint, 0, uint32, 4294967295);
  FE(tinyint, 0, uint32, 32768);
  FE(tinyint, 0, uint32, 64);
  FE(tinyint, 0, uint32, 255);
  R(tinyint, 64, uint32, 0, uint64, 64);
  FE(tinyint, 64, uint32, 2147483648);
  FE(tinyint, 64, uint32, 4294967295);
  FE(tinyint, 64, uint32, 32768);
  R(tinyint, 64, uint32, 64, uint64, 0);
  FE(tinyint, 64, uint32, 255);
  R(tinyint, 127, uint32, 0, uint64, 127);
  FE(tinyint, 127, uint32, 2147483648);
  FE(tinyint, 127, uint32, 4294967295);
  FE(tinyint, 127, uint32, 32768);
  R(tinyint, 127, uint32, 64, uint64, 63);
  FE(tinyint, 127, uint32, 255);


  //uint32  tiny
  R(uint32, 0, tinyint, -128, uint64, 128);
  R(uint32, 2147483648, tinyint, -128, uint64, 2147483776);
  R(uint32, 4294967295, tinyint, -128, uint64, 4294967423);
  R(uint32, 32768, tinyint, -128, uint64, 32896);
  R(uint32, 64, tinyint, -128, uint64, 192);
  R(uint32, 255, tinyint, -128, uint64, 383);
  R(uint32, 0, tinyint, -64, uint64, 64);
  R(uint32, 2147483648, tinyint, -64, uint64, 2147483712);
  R(uint32, 4294967295, tinyint, -64, uint64, 4294967359);
  R(uint32, 32768, tinyint, -64, uint64, 32832);
  R(uint32, 64, tinyint, -64, uint64, 128);
  R(uint32, 255, tinyint, -64, uint64, 319);
  R(uint32, 0, tinyint, 0, uint64, 0);
  R(uint32, 2147483648, tinyint, 0, uint64, 2147483648);
  R(uint32, 4294967295, tinyint, 0, uint64, 4294967295);
  R(uint32, 32768, tinyint, 0, uint64, 32768);
  R(uint32, 64, tinyint, 0, uint64, 64);
  R(uint32, 255, tinyint, 0, uint64, 255);
  FE(uint32, 0, tinyint, 64);
  R(uint32, 2147483648, tinyint, 64, uint64, 2147483584);
  R(uint32, 4294967295, tinyint, 64, uint64, 4294967231);
  R(uint32, 32768, tinyint, 64, uint64, 32704);
  R(uint32, 64, tinyint, 64, uint64, 0);
  R(uint32, 255, tinyint, 64, uint64, 191);
  FE(uint32, 0, tinyint, 127);
  R(uint32, 2147483648, tinyint, 127, uint64, 2147483521);
  R(uint32, 4294967295, tinyint, 127, uint64, 4294967168);
  R(uint32, 32768, tinyint, 127, uint64, 32641);
  FE(uint32, 64, tinyint, 127);
  R(uint32, 255, tinyint, 127, uint64, 128);
}

TEST_F(ObExprMinusTest, tiny_int64)
{
  ObMalloc buf;
  //tiny  int64
  R(tinyint, -128, int, -2147483648, int, 2147483520);
  R(tinyint, -128, int, 0, int, -128);
  R(tinyint, -128, int, 2147483647, int, -2147483775);
  R(tinyint, -128, int, -32768, int, 32640);
  R(tinyint, -128, int, 32767, int, -32895);
  R(tinyint, -128, int, -128, int, 0);
  R(tinyint, -128, int, 127, int, -255);
  R(tinyint, -64, int, -2147483648, int, 2147483584);
  R(tinyint, -64, int, 0, int, -64);
  R(tinyint, -64, int, 2147483647, int, -2147483711);
  R(tinyint, -64, int, -32768, int, 32704);
  R(tinyint, -64, int, 32767, int, -32831);
  R(tinyint, -64, int, -128, int, 64);
  R(tinyint, -64, int, 127, int, -191);
  R(tinyint, 0, int, -2147483648, int, 2147483648);
  R(tinyint, 0, int, 0, int, 0);
  R(tinyint, 0, int, 2147483647, int, -2147483647);
  R(tinyint, 0, int, -32768, int, 32768);
  R(tinyint, 0, int, 32767, int, -32767);
  R(tinyint, 0, int, -128, int, 128);
  R(tinyint, 0, int, 127, int, -127);
  R(tinyint, 64, int, -2147483648, int, 2147483712);
  R(tinyint, 64, int, 0, int, 64);
  R(tinyint, 64, int, 2147483647, int, -2147483583);
  R(tinyint, 64, int, -32768, int, 32832);
  R(tinyint, 64, int, 32767, int, -32703);
  R(tinyint, 64, int, -128, int, 192);
  R(tinyint, 64, int, 127, int, -63);
  R(tinyint, 127, int, -2147483648, int, 2147483775);
  R(tinyint, 127, int, 0, int, 127);
  R(tinyint, 127, int, 2147483647, int, -2147483520);
  R(tinyint, 127, int, -32768, int, 32895);
  R(tinyint, 127, int, 32767, int, -32640);
  R(tinyint, 127, int, -128, int, 255);
  R(tinyint, 127, int, 127, int, 0);

  //int64  tiny
  R(int, -2147483648, tinyint, -128, int, -2147483520);
  R(int, 0, tinyint, -128, int, 128);
  R(int, 2147483647, tinyint, -128, int, 2147483775);
  R(int, -32768, tinyint, -128, int, -32640);
  R(int, 32767, tinyint, -128, int, 32895);
  R(int, -128, tinyint, -128, int, 0);
  R(int, 127, tinyint, -128, int, 255);
  R(int, -2147483648, tinyint, -64, int, -2147483584);
  R(int, 0, tinyint, -64, int, 64);
  R(int, 2147483647, tinyint, -64, int, 2147483711);
  R(int, -32768, tinyint, -64, int, -32704);
  R(int, 32767, tinyint, -64, int, 32831);
  R(int, -128, tinyint, -64, int, -64);
  R(int, 127, tinyint, -64, int, 191);
  R(int, -2147483648, tinyint, 0, int, -2147483648);
  R(int, 0, tinyint, 0, int, 0);
  R(int, 2147483647, tinyint, 0, int, 2147483647);
  R(int, -32768, tinyint, 0, int, -32768);
  R(int, 32767, tinyint, 0, int, 32767);
  R(int, -128, tinyint, 0, int, -128);
  R(int, 127, tinyint, 0, int, 127);
  R(int, -2147483648, tinyint, 64, int, -2147483712);
  R(int, 0, tinyint, 64, int, -64);
  R(int, 2147483647, tinyint, 64, int, 2147483583);
  R(int, -32768, tinyint, 64, int, -32832);
  R(int, 32767, tinyint, 64, int, 32703);
  R(int, -128, tinyint, 64, int, -192);
  R(int, 127, tinyint, 64, int, 63);
  R(int, -2147483648, tinyint, 127, int, -2147483775);
  R(int, 0, tinyint, 127, int, -127);
  R(int, 2147483647, tinyint, 127, int, 2147483520);
  R(int, -32768, tinyint, 127, int, -32895);
  R(int, 32767, tinyint, 127, int, 32640);
  R(int, -128, tinyint, 127, int, -255);
  R(int, 127, tinyint, 127, int, 0);

  //edge
  FE(tinyint, -128, int, INT64_MAX);
  FE(int, INT64_MIN, tinyint, 127);
}

TEST_F(ObExprMinusTest, tiny_uint64)
{
  ObMalloc buf;
  //tiny uint64
  FE(tinyint, -128, uint64, 0);
  FE(tinyint, -128, uint64, 4294967295);
  FE(tinyint, -128, uint64, 32768);
  FE(tinyint, -128, uint64, 64);
  FE(tinyint, -64, uint64, 0);
  FE(tinyint, -64, uint64, 4294967295);
  FE(tinyint, -64, uint64, 32768);
  FE(tinyint, -64, uint64, 64);
  R(tinyint, 0, uint64, 0, uint64, 0);
  FE(tinyint, 0, uint64, 4294967295);
  FE(tinyint, 0, uint64, 32768);
  FE(tinyint, 0, uint64, 64);
  R(tinyint, 127, uint64, 0, uint64, 127);
  FE(tinyint, 127, uint64, 4294967295);
  FE(tinyint, 127, uint64, 32768);
  R(tinyint, 127, uint64, 64, uint64, 63);

  //uint64 tiny
  R(uint64, 0, tinyint, -128, uint64, 128);
  R(uint64, 4294967295, tinyint, -128, uint64, 4294967423);
  R(uint64, 32768, tinyint, -128, uint64, 32896);
  R(uint64, 64, tinyint, -128, uint64, 192);
  R(uint64, 0, tinyint, -64, uint64, 64);
  R(uint64, 4294967295, tinyint, -64, uint64, 4294967359);
  R(uint64, 32768, tinyint, -64, uint64, 32832);
  R(uint64, 64, tinyint, -64, uint64, 128);
  R(uint64, 0, tinyint, 0, uint64, 0);
  R(uint64, 4294967295, tinyint, 0, uint64, 4294967295);
  R(uint64, 32768, tinyint, 0, uint64, 32768);
  R(uint64, 64, tinyint, 0, uint64, 64);
  FE(uint64, 0, tinyint, 127);
  R(uint64, 4294967295, tinyint, 127, uint64, 4294967168);
  R(uint64, 32768, tinyint, 127, uint64, 32641);
  FE(uint64, 64, tinyint, 127);

  //edge
  FE(tinyint, 127, uint64, UINT64_MAX);
  FE(tinyint, -128, uint64, UINT64_MAX);
  FE(uint64, 0, tinyint, 1);
}

TEST_F(ObExprMinusTest, utiny_small)
{
  ObMalloc buf;
  //utiny small
  R(utinyint, 0, smallint, -32768, uint64, 32768);
  R(utinyint, 0, smallint, 0, uint64, 0);
  FE(utinyint, 0, smallint, 32767);
  R(utinyint, 0, smallint, -64, uint64, 64);
  FE(utinyint, 0, smallint, 64);
  R(utinyint, 64, smallint, -32768, uint64, 32832);
  R(utinyint, 64, smallint, 0, uint64, 64);
  FE(utinyint, 64, smallint, 32767);
  R(utinyint, 64, smallint, -64, uint64, 128);
  R(utinyint, 64, smallint, 64, uint64, 0);
  R(utinyint, 255, smallint, -32768, uint64, 33023);
  R(utinyint, 255, smallint, 0, uint64, 255);
  FE(utinyint, 255, smallint, 32767);
  R(utinyint, 255, smallint, -64, uint64, 319);
  R(utinyint, 255, smallint, 64, uint64, 191);

  //small utiny
  FE(smallint, -32768, utinyint, 0);
  R(smallint, 0, utinyint, 0, uint64, 0);
  R(smallint, 32767, utinyint, 0, uint64, 32767);
  FE(smallint, -64, utinyint, 0);
  R(smallint, 64, utinyint, 0, uint64, 64);
  FE(smallint, -32768, utinyint, 64);
  FE(smallint, 0, utinyint, 64);
  R(smallint, 32767, utinyint, 64, uint64, 32703);
  FE(smallint, -64, utinyint, 64);
  R(smallint, 64, utinyint, 64, uint64, 0);
  FE(smallint, -32768, utinyint, 255);
  FE(smallint, 0, utinyint, 255);
  R(smallint, 32767, utinyint, 255, uint64, 32512);
  FE(smallint, -64, utinyint, 255);
  FE(smallint, 64, utinyint, 255);
}

TEST_F(ObExprMinusTest, utiny_usmall)
{
  ObMalloc buf;
  //utiny usmall
  R(utinyint, 0, usmallint, 0, uint64, 0);
  FE(utinyint, 0, usmallint, 32768);
  FE(utinyint, 0, usmallint, 65535);
  FE(utinyint, 0, usmallint, 64);
  FE(utinyint, 0, usmallint, 255);
  R(utinyint, 64, usmallint, 0, uint64, 64);
  FE(utinyint, 64, usmallint, 32768);
  FE(utinyint, 64, usmallint, 65535);
  R(utinyint, 64, usmallint, 64, uint64, 0);
  FE(utinyint, 64, usmallint, 255);
  R(utinyint, 255, usmallint, 0, uint64, 255);
  FE(utinyint, 255, usmallint, 32768);
  FE(utinyint, 255, usmallint, 65535);
  R(utinyint, 255, usmallint, 64, uint64, 191);
  R(utinyint, 255, usmallint, 255, uint64, 0);

  //usmall utiny
  R(usmallint, 0, utinyint, 0, uint64, 0);
  R(usmallint, 32768, utinyint, 0, uint64, 32768);
  R(usmallint, 65535, utinyint, 0, uint64, 65535);
  R(usmallint, 64, utinyint, 0, uint64, 64);
  R(usmallint, 255, utinyint, 0, uint64, 255);
  FE(usmallint, 0, utinyint, 64);
  R(usmallint, 32768, utinyint, 64, uint64, 32704);
  R(usmallint, 65535, utinyint, 64, uint64, 65471);
  R(usmallint, 64, utinyint, 64, uint64, 0);
  R(usmallint, 255, utinyint, 64, uint64, 191);
  FE(usmallint, 0, utinyint, 255);
  R(usmallint, 32768, utinyint, 255, uint64, 32513);
  R(usmallint, 65535, utinyint, 255, uint64, 65280);
  FE(usmallint, 64, utinyint, 255);
  R(usmallint, 255, utinyint, 255, uint64, 0);
}

TEST_F(ObExprMinusTest, utiny_int32)
{
  ObMalloc buf;
  //utiny int32
  R(utinyint, 0, int32, -2147483648, uint64, 2147483648);
  R(utinyint, 0, int32, 0, uint64, 0);
  FE(utinyint, 0, int32, 2147483647);
  FE(utinyint, 0, int32, 255);
  R(utinyint, 0, int32, -64, uint64, 64);
  R(utinyint, 64, int32, -2147483648, uint64, 2147483712);
  R(utinyint, 64, int32, 0, uint64, 64);
  FE(utinyint, 64, int32, 2147483647);
  FE(utinyint, 64, int32, 255);
  R(utinyint, 64, int32, -64, uint64, 128);
  R(utinyint, 255, int32, -2147483648, uint64, 2147483903);
  R(utinyint, 255, int32, 0, uint64, 255);
  FE(utinyint, 255, int32, 2147483647);
  R(utinyint, 255, int32, 255, uint64, 0);
  R(utinyint, 255, int32, -64, uint64, 319);

  //int32 utiny
  FE(int32, -2147483648, utinyint, 0);
  R(int32, 0, utinyint, 0, uint64, 0);
  R(int32, 2147483647, utinyint, 0, uint64, 2147483647);
  R(int32, 255, utinyint, 0, uint64, 255);
  FE(int32, -64, utinyint, 0);
  FE(int32, -2147483648, utinyint, 64);
  FE(int32, 0, utinyint, 64);
  R(int32, 2147483647, utinyint, 64, uint64, 2147483583);
  R(int32, 255, utinyint, 64, uint64, 191);
  FE(int32, -64, utinyint, 64);
  FE(int32, -2147483648, utinyint, 255);
  FE(int32, 0, utinyint, 255);
  R(int32, 2147483647, utinyint, 255, uint64, 2147483392);
  R(int32, 255, utinyint, 255, uint64, 0);
  FE(int32, -64, utinyint, 255);
}

TEST_F(ObExprMinusTest, utiny_uint32)
{
  ObMalloc buf;
  //utiny uint32
  R(utinyint, 0, uint32, 0, uint64, 0);
  FE(utinyint, 0, uint32, 4294967295);
  FE(utinyint, 0, uint32, 65535);
  FE(utinyint, 0, uint32, 64);
  FE(utinyint, 0, uint32, 255);
  R(utinyint, 64, uint32, 0, uint64, 64);
  FE(utinyint, 64, uint32, 4294967295);
  FE(utinyint, 64, uint32, 65535);
  R(utinyint, 64, uint32, 64, uint64, 0);
  FE(utinyint, 64, uint32, 255);
  R(utinyint, 255, uint32, 0, uint64, 255);
  FE(utinyint, 255, uint32, 4294967295);
  FE(utinyint, 255, uint32, 65535);
  R(utinyint, 255, uint32, 64, uint64, 191);
  R(utinyint, 255, uint32, 255, uint64, 0);

  //uint32 utiny
  R(uint32, 0, utinyint, 0, uint64, 0);
  R(uint32, 4294967295, utinyint, 0, uint64, 4294967295);
  R(uint32, 65535, utinyint, 0, uint64, 65535);
  R(uint32, 64, utinyint, 0, uint64, 64);
  R(uint32, 255, utinyint, 0, uint64, 255);
  FE(uint32, 0, utinyint, 64);
  R(uint32, 4294967295, utinyint, 64, uint64, 4294967231);
  R(uint32, 65535, utinyint, 64, uint64, 65471);
  R(uint32, 64, utinyint, 64, uint64, 0);
  R(uint32, 255, utinyint, 64, uint64, 191);
  FE(uint32, 0, utinyint, 255);
  R(uint32, 4294967295, utinyint, 255, uint64, 4294967040);
  R(uint32, 65535, utinyint, 255, uint64, 65280);
  FE(uint32, 64, utinyint, 255);
  R(uint32, 255, utinyint, 255, uint64, 0);

}

TEST_F(ObExprMinusTest, utiny_int64)
{
  ObMalloc buf;
  //utiny int64
  R(utinyint, 0, int, -2147483648, uint64, 2147483648);
  R(utinyint, 0, int, 0, uint64, 0);
  FE(utinyint, 0, int, 2147483647);
  FE(utinyint, 0, int, 255);
  R(utinyint, 0, int, -64, uint64, 64);
  R(utinyint, 64, int, -2147483648, uint64, 2147483712);
  R(utinyint, 64, int, 0, uint64, 64);
  FE(utinyint, 64, int, 2147483647);
  FE(utinyint, 64, int, 255);
  R(utinyint, 64, int, -64, uint64, 128);
  R(utinyint, 255, int, -2147483648, uint64, 2147483903);
  R(utinyint, 255, int, 0, uint64, 255);
  FE(utinyint, 255, int, 2147483647);
  R(utinyint, 255, int, 255, uint64, 0);
  R(utinyint, 255, int, -64, uint64, 319);
  //int64 utiny
  FE(int, -2147483648, utinyint, 0);
  R(int, 0, utinyint, 0, uint64, 0);
  R(int, 2147483647, utinyint, 0, uint64, 2147483647);
  R(int, 255, utinyint, 0, uint64, 255);
  FE(int, -64, utinyint, 0);
  FE(int, -2147483648, utinyint, 64);
  FE(int, 0, utinyint, 64);
  R(int, 2147483647, utinyint, 64, uint64, 2147483583);
  R(int, 255, utinyint, 64, uint64, 191);
  FE(int, -64, utinyint, 64);
  FE(int, -2147483648, utinyint, 255);
  FE(int, 0, utinyint, 255);
  R(int, 2147483647, utinyint, 255, uint64, 2147483392);
  R(int, 255, utinyint, 255, uint64, 0);
  FE(int, -64, utinyint, 255);

  //edge
  FE(int, INT64_MIN, utinyint, 1);
  FE(utinyint, 255, int, INT64_MAX);
}

TEST_F(ObExprMinusTest, utiny_uint64)
{
  ObMalloc buf;
  //utiny uint64
  R(utinyint, 0, uint64, 0, uint64, 0);
  FE(utinyint, 0, uint64, 4294967295);
  FE(utinyint, 0, uint64, 255);
  FE(utinyint, 0, uint64, 64);
  R(utinyint, 0, uint64, 0, uint64, 0);
  R(utinyint, 64, uint64, 0, uint64, 64);
  FE(utinyint, 64, uint64, 4294967295);
  FE(utinyint, 64, uint64, 255);
  R(utinyint, 64, uint64, 64, uint64, 0);
  R(utinyint, 64, uint64, 0, uint64, 64);
  R(utinyint, 255, uint64, 0, uint64, 255);
  FE(utinyint, 255, uint64, 4294967295);
  R(utinyint, 255, uint64, 255, uint64, 0);
  R(utinyint, 255, uint64, 64, uint64, 191);
  R(utinyint, 255, uint64, 0, uint64, 255);

  //uint64 utiny
  R(uint64, 0, utinyint, 0, uint64, 0);
  R(uint64, 4294967295, utinyint, 0, uint64, 4294967295);
  R(uint64, 255, utinyint, 0, uint64, 255);
  R(uint64, 64, utinyint, 0, uint64, 64);
  R(uint64, 0, utinyint, 0, uint64, 0);
  FE(uint64, 0, utinyint, 64);
  R(uint64, 4294967295, utinyint, 64, uint64, 4294967231);
  R(uint64, 255, utinyint, 64, uint64, 191);
  R(uint64, 64, utinyint, 64, uint64, 0);
  FE(uint64, 0, utinyint, 64);
  FE(uint64, 0, utinyint, 255);
  R(uint64, 4294967295, utinyint, 255, uint64, 4294967040);
  R(uint64, 255, utinyint, 255, uint64, 0);
  FE(uint64, 64, utinyint, 255);
  FE(uint64, 0, utinyint, 255);

  //edge
  FE(utinyint, 255, uint64, UINT64_MAX);
  FE(utinyint, 0, uint64, UINT64_MAX);
}

TEST_F(ObExprMinusTest, small_small)
{
  ObMalloc buf;
  //small small
  R(smallint, -32768, smallint, -32768, int, 0);
  R(smallint, -32768, smallint, 0, int, -32768);
  R(smallint, -32768, smallint, 32767, int, -65535);
  R(smallint, -32768, smallint, -128, int, -32640);
  R(smallint, -32768, smallint, 127, int, -32895);
  R(smallint, 0, smallint, -32768, int, 32768);
  R(smallint, 0, smallint, 0, int, 0);
  R(smallint, 0, smallint, 32767, int, -32767);
  R(smallint, 0, smallint, -128, int, 128);
  R(smallint, 0, smallint, 127, int, -127);
  R(smallint, 32767, smallint, -32768, int, 65535);
  R(smallint, 32767, smallint, 0, int, 32767);
  R(smallint, 32767, smallint, 32767, int, 0);
  R(smallint, 32767, smallint, -128, int, 32895);
  R(smallint, 32767, smallint, 127, int, 32640);
  R(smallint, -128, smallint, -32768, int, 32640);
  R(smallint, -128, smallint, 0, int, -128);
  R(smallint, -128, smallint, 32767, int, -32895);
  R(smallint, -128, smallint, -128, int, 0);
  R(smallint, -128, smallint, 127, int, -255);
  R(smallint, 127, smallint, -32768, int, 32895);
  R(smallint, 127, smallint, 0, int, 127);
  R(smallint, 127, smallint, 32767, int, -32640);
  R(smallint, 127, smallint, -128, int, 255);
  R(smallint, 127, smallint, 127, int, 0);
}

TEST_F(ObExprMinusTest, small_usmall)
{
  ObMalloc buf;
  //small usmall
  FE(smallint, -32768, usmallint, 0);
  FE(smallint, -32768, usmallint, 32768);
  FE(smallint, -32768, usmallint, 65535);
  FE(smallint, -32768, usmallint, 64);
  FE(smallint, -32768, usmallint, 255);
  R(smallint, 0, usmallint, 0, uint64, 0);
  FE(smallint, 0, usmallint, 32768);
  FE(smallint, 0, usmallint, 65535);
  FE(smallint, 0, usmallint, 64);
  FE(smallint, 0, usmallint, 255);
  R(smallint, 32767, usmallint, 0, uint64, 32767);
  FE(smallint, 32767, usmallint, 32768);
  FE(smallint, 32767, usmallint, 65535);
  R(smallint, 32767, usmallint, 64, uint64, 32703);
  R(smallint, 32767, usmallint, 255, uint64, 32512);
  FE(smallint, -128, usmallint, 0);
  FE(smallint, -128, usmallint, 32768);
  FE(smallint, -128, usmallint, 65535);
  FE(smallint, -128, usmallint, 64);
  FE(smallint, -128, usmallint, 255);
  R(smallint, 127, usmallint, 0, uint64, 127);
  FE(smallint, 127, usmallint, 32768);
  FE(smallint, 127, usmallint, 65535);
  R(smallint, 127, usmallint, 64, uint64, 63);
  FE(smallint, 127, usmallint, 255);

  //usmall small
  R(usmallint, 0, smallint, -32768, uint64, 32768);
  R(usmallint, 32768, smallint, -32768, uint64, 65536);
  R(usmallint, 65535, smallint, -32768, uint64, 98303);
  R(usmallint, 64, smallint, -32768, uint64, 32832);
  R(usmallint, 255, smallint, -32768, uint64, 33023);
  R(usmallint, 0, smallint, 0, uint64, 0);
  R(usmallint, 32768, smallint, 0, uint64, 32768);
  R(usmallint, 65535, smallint, 0, uint64, 65535);
  R(usmallint, 64, smallint, 0, uint64, 64);
  R(usmallint, 255, smallint, 0, uint64, 255);
  FE(usmallint, 0, smallint, 32767);
  R(usmallint, 32768, smallint, 32767, uint64, 1);
  R(usmallint, 65535, smallint, 32767, uint64, 32768);
  FE(usmallint, 64, smallint, 32767);
  FE(usmallint, 255, smallint, 32767);
  R(usmallint, 0, smallint, -128, uint64, 128);
  R(usmallint, 32768, smallint, -128, uint64, 32896);
  R(usmallint, 65535, smallint, -128, uint64, 65663);
  R(usmallint, 64, smallint, -128, uint64, 192);
  R(usmallint, 255, smallint, -128, uint64, 383);
  FE(usmallint, 0, smallint, 127);
  R(usmallint, 32768, smallint, 127, uint64, 32641);
  R(usmallint, 65535, smallint, 127, uint64, 65408);
  FE(usmallint, 64, smallint, 127);
  R(usmallint, 255, smallint, 127, uint64, 128);
}


TEST_F(ObExprMinusTest, small_int32)
{
  ObMalloc buf;
  //small int32
  R(smallint, -32768, int32, -2147483648, int, 2147450880);
  R(smallint, -32768, int32, 0, int, -32768);
  R(smallint, -32768, int32, 2147483647, int, -2147516415);
  R(smallint, -32768, int32, 255, int, -33023);
  R(smallint, -32768, int32, -64, int, -32704);
  R(smallint, 0, int32, -2147483648, int, 2147483648);
  R(smallint, 0, int32, 0, int, 0);
  R(smallint, 0, int32, 2147483647, int, -2147483647);
  R(smallint, 0, int32, 255, int, -255);
  R(smallint, 0, int32, -64, int, 64);
  R(smallint, 32767, int32, -2147483648, int, 2147516415);
  R(smallint, 32767, int32, 0, int, 32767);
  R(smallint, 32767, int32, 2147483647, int, -2147450880);
  R(smallint, 32767, int32, 255, int, 32512);
  R(smallint, 32767, int32, -64, int, 32831);
  R(smallint, -128, int32, -2147483648, int, 2147483520);
  R(smallint, -128, int32, 0, int, -128);
  R(smallint, -128, int32, 2147483647, int, -2147483775);
  R(smallint, -128, int32, 255, int, -383);
  R(smallint, -128, int32, -64, int, -64);
  R(smallint, 127, int32, -2147483648, int, 2147483775);
  R(smallint, 127, int32, 0, int, 127);
  R(smallint, 127, int32, 2147483647, int, -2147483520);
  R(smallint, 127, int32, 255, int, -128);
  R(smallint, 127, int32, -64, int, 191);

  //int32 small
  R(int32, -2147483648, smallint, -32768, int, -2147450880);
  R(int32, 0, smallint, -32768, int, 32768);
  R(int32, 2147483647, smallint, -32768, int, 2147516415);
  R(int32, 255, smallint, -32768, int, 33023);
  R(int32, -64, smallint, -32768, int, 32704);
  R(int32, -2147483648, smallint, 0, int, -2147483648);
  R(int32, 0, smallint, 0, int, 0);
  R(int32, 2147483647, smallint, 0, int, 2147483647);
  R(int32, 255, smallint, 0, int, 255);
  R(int32, -64, smallint, 0, int, -64);
  R(int32, -2147483648, smallint, 32767, int, -2147516415);
  R(int32, 0, smallint, 32767, int, -32767);
  R(int32, 2147483647, smallint, 32767, int, 2147450880);
  R(int32, 255, smallint, 32767, int, -32512);
  R(int32, -64, smallint, 32767, int, -32831);
  R(int32, -2147483648, smallint, -128, int, -2147483520);
  R(int32, 0, smallint, -128, int, 128);
  R(int32, 2147483647, smallint, -128, int, 2147483775);
  R(int32, 255, smallint, -128, int, 383);
  R(int32, -64, smallint, -128, int, 64);
  R(int32, -2147483648, smallint, 127, int, -2147483775);
  R(int32, 0, smallint, 127, int, -127);
  R(int32, 2147483647, smallint, 127, int, 2147483520);
  R(int32, 255, smallint, 127, int, 128);
  R(int32, -64, smallint, 127, int, -191);
}


TEST_F(ObExprMinusTest, small_uint32)
{
  ObMalloc buf;
  //small int32
  //small uint32
  FE(smallint, -32768, uint32, 0);
  FE(smallint, -32768, uint32, 4294967295);
  FE(smallint, -32768, uint32, 32767);
  FE(smallint, -32768, uint32, 64);
  FE(smallint, -32768, uint32, 255);
  R(smallint, 0, uint32, 0, uint64, 0);
  FE(smallint, 0, uint32, 4294967295);
  FE(smallint, 0, uint32, 32767);
  FE(smallint, 0, uint32, 64);
  FE(smallint, 0, uint32, 255);
  R(smallint, 32767, uint32, 0, uint64, 32767);
  FE(smallint, 32767, uint32, 4294967295);
  R(smallint, 32767, uint32, 32767, uint64, 0);
  R(smallint, 32767, uint32, 64, uint64, 32703);
  R(smallint, 32767, uint32, 255, uint64, 32512);
  FE(smallint, -128, uint32, 0);
  FE(smallint, -128, uint32, 4294967295);
  FE(smallint, -128, uint32, 32767);
  FE(smallint, -128, uint32, 64);
  FE(smallint, -128, uint32, 255);
  R(smallint, 127, uint32, 0, uint64, 127);
  FE(smallint, 127, uint32, 4294967295);
  FE(smallint, 127, uint32, 32767);
  R(smallint, 127, uint32, 64, uint64, 63);
  FE(smallint, 127, uint32, 255);

  //uint32 small
  R(uint32, 0, smallint, -32768, uint64, 32768);
  R(uint32, 4294967295, smallint, -32768, uint64, 4295000063);
  R(uint32, 32767, smallint, -32768, uint64, 65535);
  R(uint32, 64, smallint, -32768, uint64, 32832);
  R(uint32, 255, smallint, -32768, uint64, 33023);
  R(uint32, 0, smallint, 0, uint64, 0);
  R(uint32, 4294967295, smallint, 0, uint64, 4294967295);
  R(uint32, 32767, smallint, 0, uint64, 32767);
  R(uint32, 64, smallint, 0, uint64, 64);
  R(uint32, 255, smallint, 0, uint64, 255);
  FE(uint32, 0, smallint, 32767);
  R(uint32, 4294967295, smallint, 32767, uint64, 4294934528);
  R(uint32, 32767, smallint, 32767, uint64, 0);
  FE(uint32, 64, smallint, 32767);
  FE(uint32, 255, smallint, 32767);
  R(uint32, 0, smallint, -128, uint64, 128);
  R(uint32, 4294967295, smallint, -128, uint64, 4294967423);
  R(uint32, 32767, smallint, -128, uint64, 32895);
  R(uint32, 64, smallint, -128, uint64, 192);
  R(uint32, 255, smallint, -128, uint64, 383);
  FE(uint32, 0, smallint, 127);
  R(uint32, 4294967295, smallint, 127, uint64, 4294967168);
  R(uint32, 32767, smallint, 127, uint64, 32640);
  FE(uint32, 64, smallint, 127);
  R(uint32, 255, smallint, 127, uint64, 128);

}



TEST_F(ObExprMinusTest, small_int64)
{
  ObMalloc buf;
  //small int64
  R(smallint, -32768, int, -2147483648, int, 2147450880);
  R(smallint, -32768, int, 0, int, -32768);
  R(smallint, -32768, int, 2147483647, int, -2147516415);
  R(smallint, -32768, int, 255, int, -33023);
  R(smallint, -32768, int, -64, int, -32704);
  R(smallint, 0, int, -2147483648, int, 2147483648);
  R(smallint, 0, int, 0, int, 0);
  R(smallint, 0, int, 2147483647, int, -2147483647);
  R(smallint, 0, int, 255, int, -255);
  R(smallint, 0, int, -64, int, 64);
  R(smallint, 32767, int, -2147483648, int, 2147516415);
  R(smallint, 32767, int, 0, int, 32767);
  R(smallint, 32767, int, 2147483647, int, -2147450880);
  R(smallint, 32767, int, 255, int, 32512);
  R(smallint, 32767, int, -64, int, 32831);
  R(smallint, -128, int, -2147483648, int, 2147483520);
  R(smallint, -128, int, 0, int, -128);
  R(smallint, -128, int, 2147483647, int, -2147483775);
  R(smallint, -128, int, 255, int, -383);
  R(smallint, -128, int, -64, int, -64);
  R(smallint, 127, int, -2147483648, int, 2147483775);
  R(smallint, 127, int, 0, int, 127);
  R(smallint, 127, int, 2147483647, int, -2147483520);
  R(smallint, 127, int, 255, int, -128);
  R(smallint, 127, int, -64, int, 191);

  //int64 small
  R(int, -2147483648, smallint, -32768, int, -2147450880);
  R(int, 0, smallint, -32768, int, 32768);
  R(int, 2147483647, smallint, -32768, int, 2147516415);
  R(int, 255, smallint, -32768, int, 33023);
  R(int, -64, smallint, -32768, int, 32704);
  R(int, -2147483648, smallint, 0, int, -2147483648);
  R(int, 0, smallint, 0, int, 0);
  R(int, 2147483647, smallint, 0, int, 2147483647);
  R(int, 255, smallint, 0, int, 255);
  R(int, -64, smallint, 0, int, -64);
  R(int, -2147483648, smallint, 32767, int, -2147516415);
  R(int, 0, smallint, 32767, int, -32767);
  R(int, 2147483647, smallint, 32767, int, 2147450880);
  R(int, 255, smallint, 32767, int, -32512);
  R(int, -64, smallint, 32767, int, -32831);
  R(int, -2147483648, smallint, -128, int, -2147483520);
  R(int, 0, smallint, -128, int, 128);
  R(int, 2147483647, smallint, -128, int, 2147483775);
  R(int, 255, smallint, -128, int, 383);
  R(int, -64, smallint, -128, int, 64);
  R(int, -2147483648, smallint, 127, int, -2147483775);
  R(int, 0, smallint, 127, int, -127);
  R(int, 2147483647, smallint, 127, int, 2147483520);
  R(int, 255, smallint, 127, int, 128);
  R(int, -64, smallint, 127, int, -191);

  //edge
  FE(smallint, -32768, int, INT64_MAX);
  FE(smallint, -2, int, INT64_MAX);
  FE(int, INT64_MIN, smallint, 1);
  FE(int, INT64_MIN, smallint, 32767);
  FE(int, INT64_MAX, smallint, -1);
  FE(int, INT64_MAX, smallint, -32768);
  FE(smallint, 32767, int, INT64_MIN);
}

TEST_F(ObExprMinusTest, small_uint64)
{
  ObMalloc buf;
  //small uint64
  FE(smallint, -32768, uint64, 0);
  FE(smallint, -32768, uint64, 4294967295);
  FE(smallint, -32768, uint64, 32768);
  FE(smallint, -32768, uint64, 128);
  FE(smallint, -32768, uint64, 255);
  R(smallint, 0, uint64, 0, uint64, 0);
  FE(smallint, 0, uint64, 4294967295);
  FE(smallint, 0, uint64, 32768);
  FE(smallint, 0, uint64, 128);
  FE(smallint, 0, uint64, 255);
  R(smallint, 32767, uint64, 0, uint64, 32767);
  FE(smallint, 32767, uint64, 4294967295);
  FE(smallint, 32767, uint64, 32768);
  R(smallint, 32767, uint64, 128, uint64, 32639);
  R(smallint, 32767, uint64, 255, uint64, 32512);
  FE(smallint, -128, uint64, 0);
  FE(smallint, -128, uint64, 4294967295);
  FE(smallint, -128, uint64, 32768);
  FE(smallint, -128, uint64, 128);
  FE(smallint, -128, uint64, 255);
  R(smallint, 127, uint64, 0, uint64, 127);
  FE(smallint, 127, uint64, 4294967295);
  FE(smallint, 127, uint64, 32768);
  FE(smallint, 127, uint64, 128);
  FE(smallint, 127, uint64, 255);


  //uint64 small
  R(uint64, 0, smallint, -32768, uint64, 32768);
  R(uint64, 4294967295, smallint, -32768, uint64, 4295000063);
  R(uint64, 32768, smallint, -32768, uint64, 65536);
  R(uint64, 128, smallint, -32768, uint64, 32896);
  R(uint64, 255, smallint, -32768, uint64, 33023);
  R(uint64, 0, smallint, 0, uint64, 0);
  R(uint64, 4294967295, smallint, 0, uint64, 4294967295);
  R(uint64, 32768, smallint, 0, uint64, 32768);
  R(uint64, 128, smallint, 0, uint64, 128);
  R(uint64, 255, smallint, 0, uint64, 255);
  FE(uint64, 0, smallint, 32767);
  R(uint64, 4294967295, smallint, 32767, uint64, 4294934528);
  R(uint64, 32768, smallint, 32767, uint64, 1);
  FE(uint64, 128, smallint, 32767);
  FE(uint64, 255, smallint, 32767);
  R(uint64, 0, smallint, -128, uint64, 128);
  R(uint64, 4294967295, smallint, -128, uint64, 4294967423);
  R(uint64, 32768, smallint, -128, uint64, 32896);
  R(uint64, 128, smallint, -128, uint64, 256);
  R(uint64, 255, smallint, -128, uint64, 383);
  FE(uint64, 0, smallint, 127);
  R(uint64, 4294967295, smallint, 127, uint64, 4294967168);
  R(uint64, 32768, smallint, 127, uint64, 32641);
  R(uint64, 128, smallint, 127, uint64, 1);
  R(uint64, 255, smallint, 127, uint64, 128);

  //edge
  FE(smallint, -32768, uint64, UINT64_MAX);
  FE(smallint, 32767, uint64, UINT64_MAX);
}


TEST_F(ObExprMinusTest, usmall_usmall)
{
  ObMalloc buf;
  //usmall usmall
  R(usmallint, 0, usmallint, 0, uint64, 0);
  FE(usmallint, 0, usmallint, 32768);
  FE(usmallint, 0, usmallint, 65535);
  FE(usmallint, 0, usmallint, 64);
  FE(usmallint, 0, usmallint, 255);
  R(usmallint, 32768, usmallint, 0, uint64, 32768);
  R(usmallint, 32768, usmallint, 32768, uint64, 0);
  FE(usmallint, 32768, usmallint, 65535);
  R(usmallint, 32768, usmallint, 64, uint64, 32704);
  R(usmallint, 32768, usmallint, 255, uint64, 32513);
  R(usmallint, 65535, usmallint, 0, uint64, 65535);
  R(usmallint, 65535, usmallint, 32768, uint64, 32767);
  R(usmallint, 65535, usmallint, 65535, uint64, 0);
  R(usmallint, 65535, usmallint, 64, uint64, 65471);
  R(usmallint, 65535, usmallint, 255, uint64, 65280);
  R(usmallint, 64, usmallint, 0, uint64, 64);
  FE(usmallint, 64, usmallint, 32768);
  FE(usmallint, 64, usmallint, 65535);
  R(usmallint, 64, usmallint, 64, uint64, 0);
  FE(usmallint, 64, usmallint, 255);
  R(usmallint, 255, usmallint, 0, uint64, 255);
  FE(usmallint, 255, usmallint, 32768);
  FE(usmallint, 255, usmallint, 65535);
  R(usmallint, 255, usmallint, 64, uint64, 191);
  R(usmallint, 255, usmallint, 255, uint64, 0);
}

TEST_F(ObExprMinusTest, usmall_int32)
{
  ObMalloc buf;
  //usmall int32
  R(usmallint, 0, int32, -2147483648, uint64, 2147483648);
  R(usmallint, 0, int32, 0, uint64, 0);
  FE(usmallint, 0, int32, 2147483647);
  FE(usmallint, 0, int32, 255);
  R(usmallint, 0, int32, -64, uint64, 64);
  R(usmallint, 32768, int32, -2147483648, uint64, 2147516416);
  R(usmallint, 32768, int32, 0, uint64, 32768);
  FE(usmallint, 32768, int32, 2147483647);
  R(usmallint, 32768, int32, 255, uint64, 32513);
  R(usmallint, 32768, int32, -64, uint64, 32832);
  R(usmallint, 65535, int32, -2147483648, uint64, 2147549183);
  R(usmallint, 65535, int32, 0, uint64, 65535);
  FE(usmallint, 65535, int32, 2147483647);
  R(usmallint, 65535, int32, 255, uint64, 65280);
  R(usmallint, 65535, int32, -64, uint64, 65599);
  R(usmallint, 64, int32, -2147483648, uint64, 2147483712);
  R(usmallint, 64, int32, 0, uint64, 64);
  FE(usmallint, 64, int32, 2147483647);
  FE(usmallint, 64, int32, 255);
  R(usmallint, 64, int32, -64, uint64, 128);
  R(usmallint, 255, int32, -2147483648, uint64, 2147483903);
  R(usmallint, 255, int32, 0, uint64, 255);
  FE(usmallint, 255, int32, 2147483647);
  R(usmallint, 255, int32, 255, uint64, 0);
  R(usmallint, 255, int32, -64, uint64, 319);

  //int32 usmall
  FE(int32, -2147483648, usmallint, 0);
  R(int32, 0, usmallint, 0, uint64, 0);
  R(int32, 2147483647, usmallint, 0, uint64, 2147483647);
  R(int32, 255, usmallint, 0, uint64, 255);
  FE(int32, -64, usmallint, 0);
  FE(int32, -2147483648, usmallint, 32768);
  FE(int32, 0, usmallint, 32768);
  R(int32, 2147483647, usmallint, 32768, uint64, 2147450879);
  FE(int32, 255, usmallint, 32768);
  FE(int32, -64, usmallint, 32768);
  FE(int32, -2147483648, usmallint, 65535);
  FE(int32, 0, usmallint, 65535);
  R(int32, 2147483647, usmallint, 65535, uint64, 2147418112);
  FE(int32, 255, usmallint, 65535);
  FE(int32, -64, usmallint, 65535);
  FE(int32, -2147483648, usmallint, 64);
  FE(int32, 0, usmallint, 64);
  R(int32, 2147483647, usmallint, 64, uint64, 2147483583);
  R(int32, 255, usmallint, 64, uint64, 191);
  FE(int32, -64, usmallint, 64);
  FE(int32, -2147483648, usmallint, 255);
  FE(int32, 0, usmallint, 255);
  R(int32, 2147483647, usmallint, 255, uint64, 2147483392);
  R(int32, 255, usmallint, 255, uint64, 0);
  FE(int32, -64, usmallint, 255);

}

TEST_F(ObExprMinusTest, usmall_uint32)
{
  ObMalloc buf;
  //usmall uint32
  R(usmallint, 0, uint32, 0, uint64, 0);
  FE(usmallint, 0, uint32, 4294967295);
  FE(usmallint, 0, uint32, 32767);
  FE(usmallint, 0, uint32, 64);
  FE(usmallint, 0, uint32, 255);
  R(usmallint, 32768, uint32, 0, uint64, 32768);
  FE(usmallint, 32768, uint32, 4294967295);
  R(usmallint, 32768, uint32, 32767, uint64, 1);
  R(usmallint, 32768, uint32, 64, uint64, 32704);
  R(usmallint, 32768, uint32, 255, uint64, 32513);
  R(usmallint, 65535, uint32, 0, uint64, 65535);
  FE(usmallint, 65535, uint32, 4294967295);
  R(usmallint, 65535, uint32, 32767, uint64, 32768);
  R(usmallint, 65535, uint32, 64, uint64, 65471);
  R(usmallint, 65535, uint32, 255, uint64, 65280);
  R(usmallint, 64, uint32, 0, uint64, 64);
  FE(usmallint, 64, uint32, 4294967295);
  FE(usmallint, 64, uint32, 32767);
  R(usmallint, 64, uint32, 64, uint64, 0);
  FE(usmallint, 64, uint32, 255);
  R(usmallint, 255, uint32, 0, uint64, 255);
  FE(usmallint, 255, uint32, 4294967295);
  FE(usmallint, 255, uint32, 32767);
  R(usmallint, 255, uint32, 64, uint64, 191);
  R(usmallint, 255, uint32, 255, uint64, 0);

  //uint32 usmall
  R(uint32, 0, usmallint, 0, uint64, 0);
  R(uint32, 4294967295, usmallint, 0, uint64, 4294967295);
  R(uint32, 32767, usmallint, 0, uint64, 32767);
  R(uint32, 64, usmallint, 0, uint64, 64);
  R(uint32, 255, usmallint, 0, uint64, 255);
  FE(uint32, 0, usmallint, 32768);
  R(uint32, 4294967295, usmallint, 32768, uint64, 4294934527);
  FE(uint32, 32767, usmallint, 32768);
  FE(uint32, 64, usmallint, 32768);
  FE(uint32, 255, usmallint, 32768);
  FE(uint32, 0, usmallint, 65535);
  R(uint32, 4294967295, usmallint, 65535, uint64, 4294901760);
  FE(uint32, 32767, usmallint, 65535);
  FE(uint32, 64, usmallint, 65535);
  FE(uint32, 255, usmallint, 65535);
  FE(uint32, 0, usmallint, 64);
  R(uint32, 4294967295, usmallint, 64, uint64, 4294967231);
  R(uint32, 32767, usmallint, 64, uint64, 32703);
  R(uint32, 64, usmallint, 64, uint64, 0);
  R(uint32, 255, usmallint, 64, uint64, 191);
  FE(uint32, 0, usmallint, 255);
  R(uint32, 4294967295, usmallint, 255, uint64, 4294967040);
  R(uint32, 32767, usmallint, 255, uint64, 32512);
  FE(uint32, 64, usmallint, 255);
  R(uint32, 255, usmallint, 255, uint64, 0);

}

TEST_F(ObExprMinusTest, usmall_int64)
{
  ObMalloc buf;
  //usmall int64
  R(usmallint, 0, int, -2147483648, uint64, 2147483648);
  R(usmallint, 0, int, 0, uint64, 0);
  FE(usmallint, 0, int, 2147483647);
  FE(usmallint, 0, int, 255);
  R(usmallint, 0, int, -64, uint64, 64);
  R(usmallint, 32768, int, -2147483648, uint64, 2147516416);
  R(usmallint, 32768, int, 0, uint64, 32768);
  FE(usmallint, 32768, int, 2147483647);
  R(usmallint, 32768, int, 255, uint64, 32513);
  R(usmallint, 32768, int, -64, uint64, 32832);
  R(usmallint, 65535, int, -2147483648, uint64, 2147549183);
  R(usmallint, 65535, int, 0, uint64, 65535);
  FE(usmallint, 65535, int, 2147483647);
  R(usmallint, 65535, int, 255, uint64, 65280);
  R(usmallint, 65535, int, -64, uint64, 65599);
  R(usmallint, 64, int, -2147483648, uint64, 2147483712);
  R(usmallint, 64, int, 0, uint64, 64);
  FE(usmallint, 64, int, 2147483647);
  FE(usmallint, 64, int, 255);
  R(usmallint, 64, int, -64, uint64, 128);
  R(usmallint, 255, int, -2147483648, uint64, 2147483903);
  R(usmallint, 255, int, 0, uint64, 255);
  FE(usmallint, 255, int, 2147483647);
  R(usmallint, 255, int, 255, uint64, 0);
  R(usmallint, 255, int, -64, uint64, 319);


  //int64 usmall
  FE(int, -2147483648, usmallint, 0);
  R(int, 0, usmallint, 0, uint64, 0);
  R(int, 2147483647, usmallint, 0, uint64, 2147483647);
  R(int, 255, usmallint, 0, uint64, 255);
  FE(int, -64, usmallint, 0);
  FE(int, -2147483648, usmallint, 32768);
  FE(int, 0, usmallint, 32768);
  R(int, 2147483647, usmallint, 32768, uint64, 2147450879);
  FE(int, 255, usmallint, 32768);
  FE(int, -64, usmallint, 32768);
  FE(int, -2147483648, usmallint, 65535);
  FE(int, 0, usmallint, 65535);
  R(int, 2147483647, usmallint, 65535, uint64, 2147418112);
  FE(int, 255, usmallint, 65535);
  FE(int, -64, usmallint, 65535);
  FE(int, -2147483648, usmallint, 64);
  FE(int, 0, usmallint, 64);
  R(int, 2147483647, usmallint, 64, uint64, 2147483583);
  R(int, 255, usmallint, 64, uint64, 191);
  FE(int, -64, usmallint, 64);
  FE(int, -2147483648, usmallint, 255);
  FE(int, 0, usmallint, 255);
  R(int, 2147483647, usmallint, 255, uint64, 2147483392);
  R(int, 255, usmallint, 255, uint64, 0);
  FE(int, -64, usmallint, 255);

  //edge
  FE(usmallint, 32767, int, INT64_MAX);
  FE(int, INT64_MIN, usmallint, 0);
  R(int, INT64_MAX, usmallint, 1, uint64, INT64_MAX-1);
  R(int, INT64_MAX, usmallint, 32767, uint64, (INT64_MAX-32767));
}

TEST_F(ObExprMinusTest, usmall_uint64)
{
  ObMalloc buf;
  //usmallint uint64
  R(usmallint, 0, uint64, 0, uint64, 0);
  FE(usmallint, 0, uint64, 4294967295);
  FE(usmallint, 0, uint64, 32768);
  FE(usmallint, 0, uint64, 128);
  FE(usmallint, 0, uint64, 255);
  R(usmallint, 32768, uint64, 0, uint64, 32768);
  FE(usmallint, 32768, uint64, 4294967295);
  R(usmallint, 32768, uint64, 32768, uint64, 0);
  R(usmallint, 32768, uint64, 128, uint64, 32640);
  R(usmallint, 32768, uint64, 255, uint64, 32513);
  R(usmallint, 65535, uint64, 0, uint64, 65535);
  FE(usmallint, 65535, uint64, 4294967295);
  R(usmallint, 65535, uint64, 32768, uint64, 32767);
  R(usmallint, 65535, uint64, 128, uint64, 65407);
  R(usmallint, 65535, uint64, 255, uint64, 65280);
  R(usmallint, 64, uint64, 0, uint64, 64);
  FE(usmallint, 64, uint64, 4294967295);
  FE(usmallint, 64, uint64, 32768);
  FE(usmallint, 64, uint64, 128);
  FE(usmallint, 64, uint64, 255);
  R(usmallint, 255, uint64, 0, uint64, 255);
  FE(usmallint, 255, uint64, 4294967295);
  FE(usmallint, 255, uint64, 32768);
  R(usmallint, 255, uint64, 128, uint64, 127);
  R(usmallint, 255, uint64, 255, uint64, 0);


  //uint64 usmallint
  R(uint64, 0, usmallint, 0, uint64, 0);
  R(uint64, 4294967295, usmallint, 0, uint64, 4294967295);
  R(uint64, 32768, usmallint, 0, uint64, 32768);
  R(uint64, 128, usmallint, 0, uint64, 128);
  R(uint64, 255, usmallint, 0, uint64, 255);
  FE(uint64, 0, usmallint, 32768);
  R(uint64, 4294967295, usmallint, 32768, uint64, 4294934527);
  R(uint64, 32768, usmallint, 32768, uint64, 0);
  FE(uint64, 128, usmallint, 32768);
  FE(uint64, 255, usmallint, 32768);
  FE(uint64, 0, usmallint, 65535);
  R(uint64, 4294967295, usmallint, 65535, uint64, 4294901760);
  FE(uint64, 32768, usmallint, 65535);
  FE(uint64, 128, usmallint, 65535);
  FE(uint64, 255, usmallint, 65535);
  FE(uint64, 0, usmallint, 64);
  R(uint64, 4294967295, usmallint, 64, uint64, 4294967231);
  R(uint64, 32768, usmallint, 64, uint64, 32704);
  R(uint64, 128, usmallint, 64, uint64, 64);
  R(uint64, 255, usmallint, 64, uint64, 191);
  FE(uint64, 0, usmallint, 255);
  R(uint64, 4294967295, usmallint, 255, uint64, 4294967040);
  R(uint64, 32768, usmallint, 255, uint64, 32513);
  FE(uint64, 128, usmallint, 255);
  R(uint64, 255, usmallint, 255, uint64, 0);

  //edge
  FE(usmallint, 0, uint64, UINT64_MAX);
  FE(usmallint, 32767, uint64, UINT64_MAX);
  R(uint64, UINT64_MAX, usmallint, 1, uint64, UINT64_MAX-1);
}


TEST_F(ObExprMinusTest, int32_int32)
{
  ObMalloc buf;
  //int32 int32
  R(int32, -2147483648, int32, -2147483648, int, 0);
  R(int32, -2147483648, int32, 0, int, -2147483648);
  R(int32, -2147483648, int32, 2147483647, int, -4294967295);
  R(int32, -2147483648, int32, -32768, int, -2147450880);
  R(int32, -2147483648, int32, 32767, int, -2147516415);
  R(int32, 0, int32, -2147483648, int, 2147483648);
  R(int32, 0, int32, 0, int, 0);
  R(int32, 0, int32, 2147483647, int, -2147483647);
  R(int32, 0, int32, -32768, int, 32768);
  R(int32, 0, int32, 32767, int, -32767);
  R(int32, 2147483647, int32, -2147483648, int, 4294967295);
  R(int32, 2147483647, int32, 0, int, 2147483647);
  R(int32, 2147483647, int32, 2147483647, int, 0);
  R(int32, 2147483647, int32, -32768, int, 2147516415);
  R(int32, 2147483647, int32, 32767, int, 2147450880);
  R(int32, -32768, int32, -2147483648, int, 2147450880);
  R(int32, -32768, int32, 0, int, -32768);
  R(int32, -32768, int32, 2147483647, int, -2147516415);
  R(int32, -32768, int32, -32768, int, 0);
  R(int32, -32768, int32, 32767, int, -65535);
  R(int32, 32767, int32, -2147483648, int, 2147516415);
  R(int32, 32767, int32, 0, int, 32767);
  R(int32, 32767, int32, 2147483647, int, -2147450880);
  R(int32, 32767, int32, -32768, int, 65535);
  R(int32, 32767, int32, 32767, int, 0);

}

TEST_F(ObExprMinusTest, int32_uint32)
{
  ObMalloc buf;
  //int32 uint32
  FE(int32, -2147483648, uint32, 0);
  FE(int32, -2147483648, uint32, 4294967295);
  FE(int32, -2147483648, uint32, 32767);
  FE(int32, -2147483648, uint32, 2147483647);
  FE(int32, -2147483648, uint32, 255);
  R(int32, 0, uint32, 0, uint64, 0);
  FE(int32, 0, uint32, 4294967295);
  FE(int32, 0, uint32, 32767);
  FE(int32, 0, uint32, 2147483647);
  FE(int32, 0, uint32, 255);
  R(int32, 2147483647, uint32, 0, uint64, 2147483647);
  FE(int32, 2147483647, uint32, 4294967295);
  R(int32, 2147483647, uint32, 32767, uint64, 2147450880);
  R(int32, 2147483647, uint32, 2147483647, uint64, 0);
  R(int32, 2147483647, uint32, 255, uint64, 2147483392);
  FE(int32, -32768, uint32, 0);
  FE(int32, -32768, uint32, 4294967295);
  FE(int32, -32768, uint32, 32767);
  FE(int32, -32768, uint32, 2147483647);
  FE(int32, -32768, uint32, 255);
  R(int32, 32767, uint32, 0, uint64, 32767);
  FE(int32, 32767, uint32, 4294967295);
  R(int32, 32767, uint32, 32767, uint64, 0);
  FE(int32, 32767, uint32, 2147483647);
  R(int32, 32767, uint32, 255, uint64, 32512);

  //uint32 int32
  R(uint32, 0, int32, -2147483648, uint64, 2147483648);
  R(uint32, 4294967295, int32, -2147483648, uint64, 6442450943);
  R(uint32, 32767, int32, -2147483648, uint64, 2147516415);
  R(uint32, 2147483647, int32, -2147483648, uint64, 4294967295);
  R(uint32, 255, int32, -2147483648, uint64, 2147483903);
  R(uint32, 0, int32, 0, uint64, 0);
  R(uint32, 4294967295, int32, 0, uint64, 4294967295);
  R(uint32, 32767, int32, 0, uint64, 32767);
  R(uint32, 2147483647, int32, 0, uint64, 2147483647);
  R(uint32, 255, int32, 0, uint64, 255);
  FE(uint32, 0, int32, 2147483647);
  R(uint32, 4294967295, int32, 2147483647, uint64, 2147483648);
  FE(uint32, 32767, int32, 2147483647);
  R(uint32, 2147483647, int32, 2147483647, uint64, 0);
  FE(uint32, 255, int32, 2147483647);
  R(uint32, 0, int32, -32768, uint64, 32768);
  R(uint32, 4294967295, int32, -32768, uint64, 4295000063);
  R(uint32, 32767, int32, -32768, uint64, 65535);
  R(uint32, 2147483647, int32, -32768, uint64, 2147516415);
  R(uint32, 255, int32, -32768, uint64, 33023);
  FE(uint32, 0, int32, 32767);
  R(uint32, 4294967295, int32, 32767, uint64, 4294934528);
  R(uint32, 32767, int32, 32767, uint64, 0);
  R(uint32, 2147483647, int32, 32767, uint64, 2147450880);
  FE(uint32, 255, int32, 32767);
}

TEST_F(ObExprMinusTest, int32_int64)
{
  ObMalloc buf;
  //int32 int64
  R(int32, -2147483648, int, -2147483648, int, 0);
  R(int32, -2147483648, int, 0, int, -2147483648);
  R(int32, -2147483648, int, 2147483647, int, -4294967295);
  R(int32, -2147483648, int, -32768, int, -2147450880);
  R(int32, -2147483648, int, 32767, int, -2147516415);
  R(int32, 0, int, -2147483648, int, 2147483648);
  R(int32, 0, int, 0, int, 0);
  R(int32, 0, int, 2147483647, int, -2147483647);
  R(int32, 0, int, -32768, int, 32768);
  R(int32, 0, int, 32767, int, -32767);
  R(int32, 2147483647, int, -2147483648, int, 4294967295);
  R(int32, 2147483647, int, 0, int, 2147483647);
  R(int32, 2147483647, int, 2147483647, int, 0);
  R(int32, 2147483647, int, -32768, int, 2147516415);
  R(int32, 2147483647, int, 32767, int, 2147450880);
  R(int32, -32768, int, -2147483648, int, 2147450880);
  R(int32, -32768, int, 0, int, -32768);
  R(int32, -32768, int, 2147483647, int, -2147516415);
  R(int32, -32768, int, -32768, int, 0);
  R(int32, -32768, int, 32767, int, -65535);
  R(int32, 32767, int, -2147483648, int, 2147516415);
  R(int32, 32767, int, 0, int, 32767);
  R(int32, 32767, int, 2147483647, int, -2147450880);
  R(int32, 32767, int, -32768, int, 65535);
  R(int32, 32767, int, 32767, int, 0);

  //int64 int32
  R(int, -2147483648, int32, -2147483648, int, 0);
  R(int, 0, int32, -2147483648, int, 2147483648);
  R(int, 2147483647, int32, -2147483648, int, 4294967295);
  R(int, -32768, int32, -2147483648, int, 2147450880);
  R(int, 32767, int32, -2147483648, int, 2147516415);
  R(int, -2147483648, int32, 0, int, -2147483648);
  R(int, 0, int32, 0, int, 0);
  R(int, 2147483647, int32, 0, int, 2147483647);
  R(int, -32768, int32, 0, int, -32768);
  R(int, 32767, int32, 0, int, 32767);
  R(int, -2147483648, int32, 2147483647, int, -4294967295);
  R(int, 0, int32, 2147483647, int, -2147483647);
  R(int, 2147483647, int32, 2147483647, int, 0);
  R(int, -32768, int32, 2147483647, int, -2147516415);
  R(int, 32767, int32, 2147483647, int, -2147450880);
  R(int, -2147483648, int32, -32768, int, -2147450880);
  R(int, 0, int32, -32768, int, 32768);
  R(int, 2147483647, int32, -32768, int, 2147516415);
  R(int, -32768, int32, -32768, int, 0);
  R(int, 32767, int32, -32768, int, 65535);
  R(int, -2147483648, int32, 32767, int, -2147516415);
  R(int, 0, int32, 32767, int, -32767);
  R(int, 2147483647, int32, 32767, int, 2147450880);
  R(int, -32768, int32, 32767, int, -65535);
  R(int, 32767, int32, 32767, int, 0);

  //edge
  FE(int, INT64_MAX, int32, -1);
  FE(int, INT64_MIN, int32, 1);
  FE(int32, 32767, int, INT64_MIN);
  FE(int32, -2, int, INT64_MAX);
}

TEST_F(ObExprMinusTest, int32_uint64)
{
  ObMalloc buf;
  //int32 uint64
  FE(int32, -2147483648, uint64, 0);
  FE(int32, -2147483648, uint64, 4294967295);
  FE(int32, -2147483648, uint64, 32768);
  FE(int32, -2147483648, uint64, 128);
  FE(int32, -2147483648, uint64, 255);
  R(int32, 0, uint64, 0, uint64, 0);
  FE(int32, 0, uint64, 4294967295);
  FE(int32, 0, uint64, 32768);
  FE(int32, 0, uint64, 128);
  FE(int32, 0, uint64, 255);
  R(int32, 2147483647, uint64, 0, uint64, 2147483647);
  FE(int32, 2147483647, uint64, 4294967295);
  R(int32, 2147483647, uint64, 32768, uint64, 2147450879);
  R(int32, 2147483647, uint64, 128, uint64, 2147483519);
  R(int32, 2147483647, uint64, 255, uint64, 2147483392);
  FE(int32, -32768, uint64, 0);
  FE(int32, -32768, uint64, 4294967295);
  FE(int32, -32768, uint64, 32768);
  FE(int32, -32768, uint64, 128);
  FE(int32, -32768, uint64, 255);
  R(int32, 32767, uint64, 0, uint64, 32767);
  FE(int32, 32767, uint64, 4294967295);
  FE(int32, 32767, uint64, 32768);
  R(int32, 32767, uint64, 128, uint64, 32639);
  R(int32, 32767, uint64, 255, uint64, 32512);

  //uint64 int32
  R(uint64, 0, int32, -2147483648, uint64, 2147483648);
  R(uint64, 4294967295, int32, -2147483648, uint64, 6442450943);
  R(uint64, 32768, int32, -2147483648, uint64, 2147516416);
  R(uint64, 128, int32, -2147483648, uint64, 2147483776);
  R(uint64, 255, int32, -2147483648, uint64, 2147483903);
  R(uint64, 0, int32, 0, uint64, 0);
  R(uint64, 4294967295, int32, 0, uint64, 4294967295);
  R(uint64, 32768, int32, 0, uint64, 32768);
  R(uint64, 128, int32, 0, uint64, 128);
  R(uint64, 255, int32, 0, uint64, 255);
  FE(uint64, 0, int32, 2147483647);
  R(uint64, 4294967295, int32, 2147483647, uint64, 2147483648);
  FE(uint64, 32768, int32, 2147483647);
  FE(uint64, 128, int32, 2147483647);
  FE(uint64, 255, int32, 2147483647);
  R(uint64, 0, int32, -32768, uint64, 32768);
  R(uint64, 4294967295, int32, -32768, uint64, 4295000063);
  R(uint64, 32768, int32, -32768, uint64, 65536);
  R(uint64, 128, int32, -32768, uint64, 32896);
  R(uint64, 255, int32, -32768, uint64, 33023);
  FE(uint64, 0, int32, 32767);
  R(uint64, 4294967295, int32, 32767, uint64, 4294934528);
  R(uint64, 32768, int32, 32767, uint64, 1);
  FE(uint64, 128, int32, 32767);
  FE(uint64, 255, int32, 32767);

  //edge
  FE(uint64, UINT64_MAX, int32, -1);
  FE(int32, -2, uint64, UINT64_MAX);
}

TEST_F(ObExprMinusTest, uint32_uint32)
{
  ObMalloc buf;
  //uint32 uint32
  R(uint32, 0, uint32, 0, uint64, 0);
  FE(uint32, 0, uint32, 4294967295);
  FE(uint32, 0, uint32, 2147483647);
  FE(uint32, 0, uint32, 255);
  R(uint32, 4294967295, uint32, 0, uint64, 4294967295);
  R(uint32, 4294967295, uint32, 4294967295, uint64, 0);
  R(uint32, 4294967295, uint32, 2147483647, uint64, 2147483648);
  R(uint32, 4294967295, uint32, 255, uint64, 4294967040);
  R(uint32, 32767, uint32, 0, uint64, 32767);
  FE(uint32, 32767, uint32, 4294967295);
  FE(uint32, 32767, uint32, 2147483647);
  R(uint32, 32767, uint32, 255, uint64, 32512);
  R(uint32, 2147483647, uint32, 0, uint64, 2147483647);
  FE(uint32, 2147483647, uint32, 4294967295);
  R(uint32, 2147483647, uint32, 2147483647, uint64, 0);
  R(uint32, 2147483647, uint32, 255, uint64, 2147483392);
}

TEST_F(ObExprMinusTest, uint32_int64)
{
  ObMalloc buf;
  //uint32 int64
  R(uint32, 0, int, -2147483648, uint64, 2147483648);
  R(uint32, 0, int, 0, uint64, 0);
  FE(uint32, 0, int, 2147483647);
  R(uint32, 0, int, -32768, uint64, 32768);
  FE(uint32, 0, int, 32767);
  R(uint32, 4294967295, int, -2147483648, uint64, 6442450943);
  R(uint32, 4294967295, int, 0, uint64, 4294967295);
  R(uint32, 4294967295, int, 2147483647, uint64, 2147483648);
  R(uint32, 4294967295, int, -32768, uint64, 4295000063);
  R(uint32, 4294967295, int, 32767, uint64, 4294934528);
  R(uint32, 32767, int, -2147483648, uint64, 2147516415);
  R(uint32, 32767, int, 0, uint64, 32767);
  FE(uint32, 32767, int, 2147483647);
  R(uint32, 32767, int, -32768, uint64, 65535);
  R(uint32, 32767, int, 32767, uint64, 0);
  R(uint32, 2147483647, int, -2147483648, uint64, 4294967295);
  R(uint32, 2147483647, int, 0, uint64, 2147483647);
  R(uint32, 2147483647, int, 2147483647, uint64, 0);
  R(uint32, 2147483647, int, -32768, uint64, 2147516415);
  R(uint32, 2147483647, int, 32767, uint64, 2147450880);

  //int64 uint32
  FE(int, -2147483648, uint32, 0);
  R(int, 0, uint32, 0, uint64, 0);
  R(int, 2147483647, uint32, 0, uint64, 2147483647);
  FE(int, -32768, uint32, 0);
  R(int, 32767, uint32, 0, uint64, 32767);
  FE(int, -2147483648, uint32, 4294967295);
  FE(int, 0, uint32, 4294967295);
  FE(int, 2147483647, uint32, 4294967295);
  FE(int, -32768, uint32, 4294967295);
  FE(int, 32767, uint32, 4294967295);
  FE(int, -2147483648, uint32, 32767);
  FE(int, 0, uint32, 32767);
  R(int, 2147483647, uint32, 32767, uint64, 2147450880);
  FE(int, -32768, uint32, 32767);
  R(int, 32767, uint32, 32767, uint64, 0);
  FE(int, -2147483648, uint32, 2147483647);
  FE(int, 0, uint32, 2147483647);
  R(int, 2147483647, uint32, 2147483647, uint64, 0);
  FE(int, -32768, uint32, 2147483647);
  FE(int, 32767, uint32, 2147483647);

  //edge
  R(int, INT64_MAX, uint32, 1, uint64, INT64_MAX-1);
  FE(int, INT64_MIN, int32, 1);
  FE(uint32, UINT32_MAX, int, INT64_MAX);
  R(uint32, UINT32_MAX, int, INT64_MIN, uint64, static_cast<uint64_t>(UINT32_MAX)- INT64_MIN);

}

TEST_F(ObExprMinusTest, uint32_uint64)
{
  ObMalloc buf;
  //uint32 uint64
  R(uint32, 0, uint64, 0, uint64, 0);
  FE(uint32, 0, uint64, 4294967295);
  FE(uint32, 0, uint64, 2147483647);
  FE(uint32, 0, uint64, 255);
  R(uint32, 4294967295, uint64, 0, uint64, 4294967295);
  R(uint32, 4294967295, uint64, 4294967295, uint64, 0);
  R(uint32, 4294967295, uint64, 2147483647, uint64, 2147483648);
  R(uint32, 4294967295, uint64, 255, uint64, 4294967040);
  R(uint32, 32767, uint64, 0, uint64, 32767);
  FE(uint32, 32767, uint64, 4294967295);
  FE(uint32, 32767, uint64, 2147483647);
  R(uint32, 32767, uint64, 255, uint64, 32512);
  R(uint32, 2147483647, uint64, 0, uint64, 2147483647);
  FE(uint32, 2147483647, uint64, 4294967295);
  R(uint32, 2147483647, uint64, 2147483647, uint64, 0);
  R(uint32, 2147483647, uint64, 255, uint64, 2147483392);
  R(uint32, 0, uint64, 0, uint64, 0);
  FE(uint32, 0, uint64, 4294967295);
  FE(uint32, 0, uint64, 2147483647);
  FE(uint32, 0, uint64, 255);
  R(uint32, 0, uint64, 0, uint64, 0);
  FE(uint32, 0, uint64, 4294967295);
  FE(uint32, 0, uint64, 2147483647);
  FE(uint32, 0, uint64, 255);
  R(uint32, 0, uint64, 0, uint64, 0);

  //uint64 uint32
  R(uint64, 0, uint32, 0, uint64, 0);
  R(uint64, 4294967295, uint32, 0, uint64, 4294967295);
  R(uint64, 2147483647, uint32, 0, uint64, 2147483647);
  R(uint64, 255, uint32, 0, uint64, 255);
  FE(uint64, 0, uint32, 4294967295);
  R(uint64, 4294967295, uint32, 4294967295, uint64, 0);
  FE(uint64, 2147483647, uint32, 4294967295);
  FE(uint64, 255, uint32, 4294967295);
  FE(uint64, 0, uint32, 32767);
  R(uint64, 4294967295, uint32, 32767, uint64, 4294934528);
  R(uint64, 2147483647, uint32, 32767, uint64, 2147450880);
  FE(uint64, 255, uint32, 32767);
  FE(uint64, 0, uint32, 2147483647);
  R(uint64, 4294967295, uint32, 2147483647, uint64, 2147483648);
  R(uint64, 2147483647, uint32, 2147483647, uint64, 0);
  FE(uint64, 255, uint32, 2147483647);
  R(uint64, 0, uint32, 0, uint64, 0);
  R(uint64, 4294967295, uint32, 0, uint64, 4294967295);
  R(uint64, 2147483647, uint32, 0, uint64, 2147483647);
  R(uint64, 255, uint32, 0, uint64, 255);
  R(uint64, 0, uint32, 0, uint64, 0);
  R(uint64, 4294967295, uint32, 0, uint64, 4294967295);
  R(uint64, 2147483647, uint32, 0, uint64, 2147483647);
  R(uint64, 255, uint32, 0, uint64, 255);
  R(uint64, 0, uint32, 0, uint64, 0);

  //edge
  R(uint64, UINT64_MAX, uint32, 1, uint64, UINT64_MAX-1);
  FE(uint32, 1, uint64, UINT64_MAX);


}

TEST_F(ObExprMinusTest, int64_int64)
{
  ObMalloc buf;
  //int64 int64
  R(int, -2147483648, int, -2147483648, int, 0);
  R(int, -2147483648, int, 0, int, -2147483648);
  R(int, -2147483648, int, 2147483647, int, -4294967295);
  R(int, -2147483648, int, -32768, int, -2147450880);
  R(int, -2147483648, int, 32767, int, -2147516415);
  R(int, 0, int, -2147483648, int, 2147483648);
  R(int, 0, int, 0, int, 0);
  R(int, 0, int, 2147483647, int, -2147483647);
  R(int, 0, int, -32768, int, 32768);
  R(int, 0, int, 32767, int, -32767);
  R(int, 2147483647, int, -2147483648, int, 4294967295);
  R(int, 2147483647, int, 0, int, 2147483647);
  R(int, 2147483647, int, 2147483647, int, 0);
  R(int, 2147483647, int, -32768, int, 2147516415);
  R(int, 2147483647, int, 32767, int, 2147450880);
  R(int, -32768, int, -2147483648, int, 2147450880);
  R(int, -32768, int, 0, int, -32768);
  R(int, -32768, int, 2147483647, int, -2147516415);
  R(int, -32768, int, -32768, int, 0);
  R(int, -32768, int, 32767, int, -65535);
  R(int, 32767, int, -2147483648, int, 2147516415);
  R(int, 32767, int, 0, int, 32767);
  R(int, 32767, int, 2147483647, int, -2147450880);
  R(int, 32767, int, -32768, int, 65535);
  R(int, 32767, int, 32767, int, 0);

  //edge
  R(int, INT64_MAX, int, INT64_MAX, int, 0);
  R(int, INT64_MIN, int, INT64_MIN, int, 0);
  FE(int, INT64_MAX, int, INT64_MIN);
  FE(int, INT64_MIN, int, INT64_MAX);
}

TEST_F(ObExprMinusTest, int64_uint64)
{
  ObMalloc buf;
  //int64 uint64
  FE(int, -2147483648, uint64, 0);
  FE(int, -2147483648, uint64, 4294967295);
  FE(int, -2147483648, uint64, 2147483647);
  FE(int, -2147483648, uint64, 255);
  R(int, 0, uint64, 0, uint64, 0);
  FE(int, 0, uint64, 4294967295);
  FE(int, 0, uint64, 2147483647);
  FE(int, 0, uint64, 255);
  R(int, 2147483647, uint64, 0, uint64, 2147483647);
  FE(int, 2147483647, uint64, 4294967295);
  R(int, 2147483647, uint64, 2147483647, uint64, 0);
  R(int, 2147483647, uint64, 255, uint64, 2147483392);
  FE(int, -32768, uint64, 0);
  FE(int, -32768, uint64, 4294967295);
  FE(int, -32768, uint64, 2147483647);
  FE(int, -32768, uint64, 255);
  R(int, 32767, uint64, 0, uint64, 32767);
  FE(int, 32767, uint64, 4294967295);
  FE(int, 32767, uint64, 2147483647);
  R(int, 32767, uint64, 255, uint64, 32512);

  //uint64 int64
  R(uint64, 0, int, -2147483648, uint64, 2147483648);
  R(uint64, 4294967295, int, -2147483648, uint64, 6442450943);
  R(uint64, 2147483647, int, -2147483648, uint64, 4294967295);
  R(uint64, 255, int, -2147483648, uint64, 2147483903);
  R(uint64, 0, int, 0, uint64, 0);
  R(uint64, 4294967295, int, 0, uint64, 4294967295);
  R(uint64, 2147483647, int, 0, uint64, 2147483647);
  R(uint64, 255, int, 0, uint64, 255);
  FE(uint64, 0, int, 2147483647);
  R(uint64, 4294967295, int, 2147483647, uint64, 2147483648);
  R(uint64, 2147483647, int, 2147483647, uint64, 0);
  FE(uint64, 255, int, 2147483647);
  R(uint64, 0, int, -32768, uint64, 32768);
  R(uint64, 4294967295, int, -32768, uint64, 4295000063);
  R(uint64, 2147483647, int, -32768, uint64, 2147516415);
  R(uint64, 255, int, -32768, uint64, 33023);
  FE(uint64, 0, int, 32767);
  R(uint64, 4294967295, int, 32767, uint64, 4294934528);
  R(uint64, 2147483647, int, 32767, uint64, 2147450880);
  FE(uint64, 255, int, 32767);

  //edge
  FE(int, INT64_MAX, uint64, UINT64_MAX);
  FE(uint64, UINT64_MAX, int, -1);
}


TEST_F(ObExprMinusTest, uint64_uint64)
{
  ObMalloc buf;
  //uint64 uint64
  R(uint64, 0, uint64, 0, uint64, 0);
  FE(uint64, 0, uint64, 4294967295);
  FE(uint64, 0, uint64, 2147483647);
  FE(uint64, 0, uint64, 255);
  R(uint64, 4294967295, uint64, 0, uint64, 4294967295);
  R(uint64, 4294967295, uint64, 4294967295, uint64, 0);
  R(uint64, 4294967295, uint64, 2147483647, uint64, 2147483648);
  R(uint64, 4294967295, uint64, 255, uint64, 4294967040);
  R(uint64, 2147483647, uint64, 0, uint64, 2147483647);
  FE(uint64, 2147483647, uint64, 4294967295);
  R(uint64, 2147483647, uint64, 2147483647, uint64, 0);
  R(uint64, 2147483647, uint64, 255, uint64, 2147483392);
  R(uint64, 255, uint64, 0, uint64, 255);
  FE(uint64, 255, uint64, 4294967295);
  FE(uint64, 255, uint64, 2147483647);
  R(uint64, 255, uint64, 255, uint64, 0);

  //edge
  FE(uint64, 0, uint64, UINT64_MAX);
  R(uint64, UINT64_MAX, uint64, UINT64_MAX, uint64, 0);
}


TEST_F(ObExprMinusTest, float_float)
{
  ObMalloc buf;
  RD(float, static_cast<float>(0), float, static_cast<float>(0), 0, 5);
  RD(float, static_cast<float>(0), float, static_cast<float>(1), -1, 5);
  RD(float, static_cast<float>(0), float, static_cast<float>(-1), 1, 5);
  RD(float, static_cast<float>(1), float, static_cast<float>(0), 1, 5);
  RD(float, static_cast<float>(1), float, static_cast<float>(1), 0, 5);
  RD(float, static_cast<float>(1), float, static_cast<float>(-1), 2, 5);
  RD(float, static_cast<float>(1), float, static_cast<float>(-123.4), 124.4, 5);
  RD(float, static_cast<float>(0.125), float, static_cast<float>(0.0625), 0.0625, 5);
  RD(float, static_cast<float>(-1), float, static_cast<float>(0), -1, 5);
  RD(float, static_cast<float>(-1), float, static_cast<float>(1), -2, 5);
  RD(float, static_cast<float>(123.4), float, static_cast<float>(123.4), 0, 5);
  RD(float, static_cast<float>(123.4), float, static_cast<float>(-123.4), 246.8, 5);
  RD(float, static_cast<float>(123.4), float, static_cast<float>(1), 122.4, 5);
  //edge
  RD(float, static_cast<float>(123.4), float, static_cast<float>(-FLT_MAX), FLT_MAX, 5);
  RD(float, static_cast<float>(123.4), float, static_cast<float>(FLT_MAX), -FLT_MAX, 5);
  RD(float, static_cast<float>(FLT_MAX), float, static_cast<float>(123.4), FLT_MAX, 5);
  RD(float, static_cast<float>(FLT_MAX), float, static_cast<float>(-FLT_MAX), 2*static_cast<double>(FLT_MAX), 5);

  RD(float, static_cast<float>(-FLT_MAX), float, static_cast<float>(123.4), -FLT_MAX, 5);
  RD(float, static_cast<float>(-FLT_MAX), float, static_cast<float>(-FLT_MAX), 0, 5);
}


TEST_F(ObExprMinusTest, float_double)
{
  ObMalloc buf;
  //float double
  RD(float, static_cast<float>(0), double, 0, 0, 5);
  RD(float, static_cast<float>(0), double, 1, -1, 5);
  RD(float, static_cast<float>(0), double, -1, 1, 5);
  RD(float, static_cast<float>(1), double, 0, 1, 5);
  RD(float, static_cast<float>(1), double, 1, 0, 5);
  RD(float, static_cast<float>(1), double, -1, 2, 5);
  RD(float, static_cast<float>(1), double, -123.4, 124.4, 5);
  RD(float, static_cast<float>(-1), double, 0, -1, 5);
  RD(float, static_cast<float>(-1), double, 1, -2, 5);
  RD(float, static_cast<float>(123.4), double, 123.4, 0, 5);
  RD(float, static_cast<float>(123.4), double, -123.4, 246.8, 5);
  RD(float, static_cast<float>(123.4), double, 1, 122.4, 5);
  //double float
  RD(double, 0, float, static_cast<float>(0), 0, 5);
  RD(double, 0, float, static_cast<float>(1), -1, 5);
  RD(double, 0, float, static_cast<float>(-1), 1, 5);
  RD(double, 1, float, static_cast<float>(0), 1, 5);
  RD(double, 1, float, static_cast<float>(1), 0, 5);
  RD(double, 1, float, static_cast<float>(-1), 2, 5);
  RD(double, 1, float, static_cast<float>(-123.4), 124.4, 5);
  RD(double, -1, float, static_cast<float>(0), -1, 5);
  RD(double, -1, float, static_cast<float>(1), -2, 5);
  RD(double, 123.4, float, static_cast<float>(123.4), 0, 5);
  RD(double, 123.4, float, static_cast<float>(-123.4), 246.8, 5);
  RD(double, 123.4, float, static_cast<float>(1), 122.4, 5);

  //edge
  RD(float, static_cast<float>(123.4), double, -FLT_MAX, FLT_MAX, 5);
  RD(float, static_cast<float>(123.4), double, FLT_MAX, -FLT_MAX, 5);
  RD(float, static_cast<float>(FLT_MAX), double, 123.4, FLT_MAX, 5);
  RD(float, static_cast<float>(FLT_MAX), double, -FLT_MAX, 2*static_cast<double>(FLT_MAX), 5);

  RD(float, static_cast<float>(123.4), double, -DBL_MAX, DBL_MAX, 5);
  RD(float, static_cast<float>(123.4), double, DBL_MAX, -DBL_MAX, 5);
  RD(float, static_cast<float>(FLT_MAX), double, -DBL_MAX, DBL_MAX, 5);
  RD(float, static_cast<float>(FLT_MAX), double, DBL_MAX, -DBL_MAX, 5);
  RD(float, static_cast<float>(-FLT_MAX), double, -DBL_MAX, DBL_MAX, 5);
  RD(float, static_cast<float>(-FLT_MAX), double, DBL_MAX, -DBL_MAX, 5);
}


TEST_F(ObExprMinusTest, double_double)
{
  ObMalloc buf;
  //float double
  RD(double, 0, double, 0, 0, 5);
  RD(double, 0, double, 1, -1, 5);
  RD(double, 0, double, -1, 1, 5);
  RD(double, 1, double, 0, 1, 5);
  RD(double, 1, double, 1, 0, 5);
  RD(double, 1, double, -1, 2, 5);
  RD(double, 1, double, -123.4, 124.4, 5);
  RD(double, -1, double, 0, -1, 5);
  RD(double, -1, double, 1, -2, 5);
  RD(double, 123.4, double, 123.4, 0, 5);
  RD(double, 123.4, double, -123.4, 246.8, 5);
  RD(double, 123.4, double, 1, 122.4, 5);
  //edge

  RD(double, DBL_MAX, double, 0, DBL_MAX, 5);
  RD(double, DBL_MAX, double, 123.45, DBL_MAX, 5);
  RD(double, -DBL_MAX, double, 0, -DBL_MAX, 5);
  RD(double, -DBL_MAX, double, 123.45, -DBL_MAX, 5);
  RD(double, DBL_MAX, double, DBL_MAX, 0, 5);
  RD(double, -DBL_MAX, double, -DBL_MAX, 0, 5);
  RD(double, 0, double, DBL_MAX, -DBL_MAX, 5);
  RD(double, 0, double, -DBL_MAX, DBL_MAX, 5);
  FE(double, DBL_MAX, double, -DBL_MAX);
  FE(double, -DBL_MAX, double, DBL_MAX);
}

TEST_F(ObExprMinusTest, double_int)
{
  ObMalloc buf;
  //tiny small med
  RD(double, 0, tinyint, 0, 0, 5);
  RD(double, 0, tinyint, -5, 5, 5);
  RD(double, 125, tinyint, 5, 120, 5);
  RD(tinyint, 127, double, 500, -373, 5);
  RD(tinyint, -128, double, 500, -628, 5);
  RD(double, -345, smallint, -5, -340, 5);
  RD(double, -567, smallint, 10, -577, 5);
  RD(smallint, 100, double, 50.5, 49.5, 8);
  RD(smallint, -100, double, 50.5, -150.5, 8);
  RD(double, -345, mediumint, -5, -340, 5);
  RD(double, -567, mediumint, 10, -577, 5);
  RD(mediumint, 100, double, 50.5, 49.5, 8);
  RD(mediumint, -100, double, 50.5, -150.5, 8);
  //double int
  RD(double, 0, int, 0, 0, 8);
  RD(double, 0, int, 123, -123, 8);
  RD(double, 0, int, -123, 123, 8);
  RD(double, 0, int, 2147483647, -2147483647, 8);
  RD(double, 0, int, -2147483648, 2147483648, 8);
  RD(double, 123, int, 0, 123, 8);
  RD(double, 123, int, 123, 0, 8);
  RD(double, 123, int, -123, 246, 8);
  RD(double, 123, int, 2147483647, -2147483524, 8);
  RD(double, 123, int, -2147483648, 2147483771, 8);
  RD(double, -123, int, 0, -123, 8);
  RD(double, -123, int, 123, -246, 8);
  RD(double, -123, int, -123, 0, 8);
  RD(double, -123, int, 2147483647, -2147483770, 8);
  RD(double, -123, int, -2147483648, 2147483525, 8);
  RD(double, 3.4E+38, int, 0, 3.4E+38, 8);
  RD(double, 3.4E+38, int, 123, 3.4E+38, 8);
  RD(double, 3.4E+38, int, -123, 3.4E+38, 8);
  RD(double, 3.4E+38, int, 2147483647, 3.4E+38, 8);
  RD(double, 3.4E+38, int, -2147483648, 3.4E+38, 8);
  RD(double, -3.4E+38, int, 0, -3.4E+38, 8);
  RD(double, -3.4E+38, int, 123, -3.4E+38, 8);
  RD(double, -3.4E+38, int, -123, -3.4E+38, 8);
  RD(double, -3.4E+38, int, 2147483647, -3.4E+38, 8);
  RD(double, -3.4E+38, int, -2147483648, -3.4E+38, 8);
  //int double
  RD(int, 0, double, 0, 0, 8);
  RD(int, 123, double, 0, 123, 8);
  RD(int, -123, double, 0, -123, 8);
  RD(int, 2147483647, double, 0, 2147483647, 8);
  RD(int, -2147483648, double, 0, -2147483648, 8);
  RD(int, 0, double, 123, -123, 8);
  RD(int, 123, double, 123, 0, 8);
  RD(int, -123, double, 123, -246, 8);
  RD(int, 2147483647, double, 123, 2147483524, 8);
  RD(int, -2147483648, double, 123, -2147483771, 8);
  RD(int, 0, double, -123, 123, 8);
  RD(int, 123, double, -123, 246, 8);
  RD(int, -123, double, -123, 0, 8);
  RD(int, 2147483647, double, -123, 2147483770, 8);
  RD(int, -2147483648, double, -123, -2147483525, 8);
  RD(int, 0, double, 3.4E+38, -3.4E+38, 8);
  RD(int, 123, double, 3.4E+38, -3.4E+38, 8);
  RD(int, -123, double, 3.4E+38, -3.4E+38, 8);
  RD(int, 2147483647, double, 3.4E+38, -3.4E+38, 8);
  RD(int, -2147483648, double, 3.4E+38, -3.4E+38, 8);
  RD(int, 0, double, -3.4E+38, 3.4E+38, 8);
  RD(int, 123, double, -3.4E+38, 3.4E+38, 8);
  RD(int, -123, double, -3.4E+38, 3.4E+38, 8);
  RD(int, 2147483647, double, -3.4E+38, 3.4E+38, 8);
  RD(int, -2147483648, double, -3.4E+38, 3.4E+38, 8);
  //edge
  RD(double, DBL_MAX, int, INT64_MIN, DBL_MAX, 8);
  RD(double, DBL_MAX, int, INT64_MAX, DBL_MAX, 8);
  RD(double, -DBL_MAX, int, INT64_MIN, -DBL_MAX, 8);
  RD(double, -DBL_MAX, int, INT64_MAX, -DBL_MAX, 8);

  RD(int, INT64_MAX, double, DBL_MAX, -DBL_MAX, 8);
  RD(int, INT64_MAX, double, -DBL_MAX, DBL_MAX, 8);
  RD(int, INT64_MIN, double, DBL_MAX, -DBL_MAX, 8);
  RD(int, INT64_MIN, double, -DBL_MAX, DBL_MAX, 8);

}

TEST_F(ObExprMinusTest, double_uint)
{
  ObMalloc buf;
  //tiny small med
  RD(double, 0, utinyint, 0, 0, 5);
  RD(double, 125, utinyint, 5, 120, 5);
  RD(utinyint, 127, double, 500, -373, 5);
  RD(utinyint, 0, double, 500, -500, 5);
  RD(double, -345, usmallint, 5, -350, 5);
  RD(double, -567, usmallint, 65535, -66102, 5);
  RD(usmallint, 100, double, 50.5, 49.5, 8);
  RD(usmallint, 65535, double, 50.5, 65484.5, 8);
  RD(double, -345, umediumint, 50, -395, 5);
  RD(double, -567, umediumint, 16777215, -16777782, 5);
  RD(umediumint, 100, double, 50.5, 49.5, 8);
  RD(umediumint, 16777215, double, 50.5, 16777164.5, 8);
  //double uint
  RD(double, 0, uint64, 0, 0, 8);
  RD(double, 0, uint64, 123, -123, 8);
  RD(double, 0, uint64, 4294967295, -4294967295, 8);
  RD(double, 123, uint64, 0, 123, 8);
  RD(double, 123, uint64, 123, 0, 8);
  RD(double, 123, uint64, 4294967295, -4294967172, 8);
  RD(double, -123, uint64, 0, -123, 8);
  RD(double, -123, uint64, 123, -246, 8);
  RD(double, -123, uint64, 4294967295, -4294967418, 8);
  RD(double, 3.4E+38, uint64, 0, 3.4E+38, 8);
  RD(double, 3.4E+38, uint64, 123, 3.4E+38, 8);
  RD(double, 3.4E+38, uint64, 4294967295, 3.4E+38, 8);
  RD(double, -3.4E+38, uint64, 0, -3.4E+38, 8);
  RD(double, -3.4E+38, uint64, 123, -3.4E+38, 8);
  RD(double, -3.4E+38, uint64, 4294967295, -3.4E+38, 8);
  //uint double
  RD(uint64, 0, double, 0, 0, 8);
  RD(uint64, 123, double, 0, 123, 8);
  RD(uint64, 4294967295, double, 0, 4294967295, 8);
  RD(uint64, 0, double, 123, -123, 8);
  RD(uint64, 123, double, 123, 0, 8);
  RD(uint64, 4294967295, double, 123, 4294967172, 8);
  RD(uint64, 0, double, -123, 123, 8);
  RD(uint64, 123, double, -123, 246, 8);
  RD(uint64, 4294967295, double, -123, 4294967418, 8);
  RD(uint64, 0, double, 3.4E+38, -3.4E+38, 8);
  RD(uint64, 123, double, 3.4E+38, -3.4E+38, 8);
  RD(uint64, 4294967295, double, 3.4E+38, -3.4E+38, 8);
  RD(uint64, 0, double, -3.4E+38, 3.4E+38, 8);
  RD(uint64, 123, double, -3.4E+38, 3.4E+38, 8);
  RD(uint64, 4294967295, double, -3.4E+38, 3.4E+38, 8);
  //edge
  RD(double, DBL_MAX, uint64, 0, DBL_MAX, 8);
  RD(double, DBL_MAX, uint64, UINT64_MAX, DBL_MAX, 8);
  RD(double, -DBL_MAX, uint64, 0, -DBL_MAX, 8);
  RD(double, -DBL_MAX, uint64, INT64_MAX, -DBL_MAX, 8);

  RD(uint64, UINT64_MAX, double, DBL_MAX, -DBL_MAX, 8);
  RD(uint64, UINT64_MAX, double, -DBL_MAX, DBL_MAX, 8);
  RD(uint64, 0, double, DBL_MAX, -DBL_MAX, 8);
  RD(uint64, 0, double, -DBL_MAX, DBL_MAX, 8);
}


TEST_F(ObExprMinusTest, float_int)
{
  ObMalloc buf;
  //tiny small med
  RD(float, 0.0f, tinyint, 0, 0, 5);
  RD(float, 0.0f, tinyint, -5, 5, 5);
  RD(float, 125.0f, tinyint, 5, 120, 5);
  RD(tinyint, 127, float, 500.0f, -373, 5);
  RD(tinyint, -128, float, 500.0f, -628, 5);
  RD(float, -345.0f, smallint, -5, -340, 5);
  RD(float, -567.0f, smallint, 10, -577, 5);
  RD(smallint, 100, float, 50.5f, 49.5, 8);
  RD(smallint, -100, float, 50.5f, -150.5, 8);
  RD(float, -345.0f, mediumint, -5, -340, 5);
  RD(float, -567.0f, mediumint, 10, -577, 5);
  RD(mediumint, 100, float, 50.5f, 49.5, 8);
  RD(mediumint, -100, float, 50.5f, -150.5, 8);
  //float int
  RD(float, static_cast<float>(0), int, 0, 0, 8);
  RD(float, static_cast<float>(0), int, 123, -123, 8);
  RD(float, static_cast<float>(0), int, -123, 123, 8);
  RD(float, static_cast<float>(0), int, 2147483647, -2147483647, 8);
  RD(float, static_cast<float>(0), int, -2147483648, 2147483648, 8);
  RD(float, static_cast<float>(123), int, 0, 123, 8);
  RD(float, static_cast<float>(123), int, 123, 0, 8);
  RD(float, static_cast<float>(123), int, -123, 246, 8);
  RD(float, static_cast<float>(123), int, 2147483647, -2147483524, 8);
  RD(float, static_cast<float>(123), int, -2147483648, 2147483771, 8);
  RD(float, static_cast<float>(-123), int, 0, -123, 8);
  RD(float, static_cast<float>(-123), int, 123, -246, 8);
  RD(float, static_cast<float>(-123), int, -123, 0, 8);
  RD(float, static_cast<float>(-123), int, 2147483647, -2147483770, 8);
  RD(float, static_cast<float>(-123), int, -2147483648, 2147483525, 8);
  RD(float, static_cast<float>(3.4E+38), int, 0, 3.4E+38, 8);
  RD(float, static_cast<float>(3.4E+38), int, 123, 3.4E+38, 8);
  RD(float, static_cast<float>(3.4E+38), int, -123, 3.4E+38, 8);
  RD(float, static_cast<float>(3.4E+38), int, 2147483647, 3.4E+38, 8);
  RD(float, static_cast<float>(3.4E+38), int, -2147483648, 3.4E+38, 8);
  RD(float, static_cast<float>(-3.4E+38), int, 0, -3.4E+38, 8);
  RD(float, static_cast<float>(-3.4E+38), int, 123, -3.4E+38, 8);
  RD(float, static_cast<float>(-3.4E+38), int, -123, -3.4E+38, 8);
  RD(float, static_cast<float>(-3.4E+38), int, 2147483647, -3.4E+38, 8);
  RD(float, static_cast<float>(-3.4E+38), int, -2147483648, -3.4E+38, 8);
  //int float
  RD(int, 0, float, static_cast<float>(0), 0, 8);
  RD(int, 123, float, static_cast<float>(0), 123, 8);
  RD(int, -123, float, static_cast<float>(0), -123, 8);
  RD(int, 2147483647, float, static_cast<float>(0), 2147483647, 8);
  RD(int, -2147483648, float, static_cast<float>(0), -2147483648, 8);
  RD(int, 0, float, static_cast<float>(123), -123, 8);
  RD(int, 123, float, static_cast<float>(123), 0, 8);
  RD(int, -123, float, static_cast<float>(123), -246, 8);
  RD(int, 2147483647, float, static_cast<float>(123), 2147483524, 8);
  RD(int, -2147483648, float, static_cast<float>(123), -2147483771, 8);
  RD(int, 0, float, static_cast<float>(-123), 123, 8);
  RD(int, 123, float, static_cast<float>(-123), 246, 8);
  RD(int, -123, float, static_cast<float>(-123), 0, 8);
  RD(int, 2147483647, float, static_cast<float>(-123), 2147483770, 8);
  RD(int, -2147483648, float, static_cast<float>(-123), -2147483525, 8);
  RD(int, 0, float, static_cast<float>(3.4E+38), -3.4E+38, 8);
  RD(int, 123, float, static_cast<float>(3.4E+38), -3.4E+38, 8);
  RD(int, -123, float, static_cast<float>(3.4E+38), -3.4E+38, 8);
  RD(int, 2147483647, float, static_cast<float>(3.4E+38), -3.4E+38, 8);
  RD(int, -2147483648, float, static_cast<float>(3.4E+38), -3.4E+38, 8);
  RD(int, 0, float, static_cast<float>(-3.4E+38), 3.4E+38, 8);
  RD(int, 123, float, static_cast<float>(-3.4E+38), 3.4E+38, 8);
  RD(int, -123, float, static_cast<float>(-3.4E+38), 3.4E+38, 8);
  RD(int, 2147483647, float, static_cast<float>(-3.4E+38), 3.4E+38, 8);
  RD(int, -2147483648, float, static_cast<float>(-3.4E+38), 3.4E+38, 8);
  //edge
  RD(float, static_cast<float>(-FLT_MAX), int, INT64_MIN, -FLT_MAX, 8);
  RD(float, static_cast<float>(-FLT_MAX), int, INT64_MAX, -FLT_MAX, 8);
  RD(float, static_cast<float>(FLT_MAX), int, INT64_MIN, FLT_MAX, 8);
  RD(float, static_cast<float>(FLT_MAX), int, INT64_MAX, FLT_MAX, 8);
  RD(int, INT64_MAX, float, static_cast<float>(-FLT_MAX), FLT_MAX, 8);
  RD(int, INT64_MAX, float, static_cast<float>(FLT_MAX), -FLT_MAX, 8);
  RD(int, INT64_MIN, float, static_cast<float>(-FLT_MAX), FLT_MAX, 8);
  RD(int, INT64_MIN, float, static_cast<float>(FLT_MAX), -FLT_MAX, 8);
}


TEST_F(ObExprMinusTest, float_uint)
{
  ObMalloc buf;
  //tiny small med
  RD(float, 0.0f, utinyint, 0, 0, 5);
  RD(float, 125.0f, utinyint, 5, 120, 5);
  RD(utinyint, 127, float, 500.0f, -373, 5);
  RD(utinyint, 0, float, 500.0f, -500, 5);
  RD(float, -345.0f, usmallint, 5, -350, 5);
  RD(float, -567.0f, usmallint, 65535, -66102, 5);
  RD(usmallint, 100, float, 50.5, 49.5, 8);
  RD(usmallint, 65535, float, 50.5, 65484.5, 8);
  RD(float, -345.0f, umediumint, 50, -395, 5);
  RD(float, -567.0f, umediumint, 16777215, -16777782, 5);
  RD(umediumint, 100, float, 50.5, 49.5, 8);
  RD(umediumint, 16777215, float, 50.5, 16777164.5, 8);
  //float uint
  RD(float, static_cast<float>(0), uint64, 0, 0, 8);
  RD(float, static_cast<float>(0), uint64, 123, -123, 8);
  RD(float, static_cast<float>(0), uint64, 4294967295, -4294967295, 8);
  RD(float, static_cast<float>(123), uint64, 0, 123, 8);
  RD(float, static_cast<float>(123), uint64, 123, 0, 8);
  RD(float, static_cast<float>(123), uint64, 4294967295, -4294967172, 8);
  RD(float, static_cast<float>(-123), uint64, 0, -123, 8);
  RD(float, static_cast<float>(-123), uint64, 123, -246, 8);
  RD(float, static_cast<float>(-123), uint64, 4294967295, -4294967418, 8);
  RD(float, static_cast<float>(3.4E+38), uint64, 0, 3.4E+38, 8);
  RD(float, static_cast<float>(3.4E+38), uint64, 123, 3.4E+38, 8);
  RD(float, static_cast<float>(3.4E+38), uint64, 4294967295, 3.4E+38, 8);
  RD(float, static_cast<float>(-3.4E+38), uint64, 0, -3.4E+38, 8);
  RD(float, static_cast<float>(-3.4E+38), uint64, 123, -3.4E+38, 8);
  RD(float, static_cast<float>(-3.4E+38), uint64, 4294967295, -3.4E+38, 8);
  //uint float
  RD(uint64, 0, float, static_cast<float>(0), 0, 8);
  RD(uint64, 123, float, static_cast<float>(0), 123, 8);
  RD(uint64, 4294967295, float, static_cast<float>(0), 4294967295, 8);
  RD(uint64, 0, float, static_cast<float>(123), -123, 8);
  RD(uint64, 123, float, static_cast<float>(123), 0, 8);
  RD(uint64, 4294967295, float, static_cast<float>(123), 4294967172, 8);
  RD(uint64, 0, float, static_cast<float>(-123), 123, 8);
  RD(uint64, 123, float, static_cast<float>(-123), 246, 8);
  RD(uint64, 4294967295, float, static_cast<float>(-123), 4294967418, 8);
  RD(uint64, 0, float, static_cast<float>(3.4E+38), -3.4E+38, 8);
  RD(uint64, 123, float, static_cast<float>(3.4E+38), -3.4E+38, 8);
  RD(uint64, 4294967295, float, static_cast<float>(3.4E+38), -3.4E+38, 8);
  RD(uint64, 0, float, static_cast<float>(-3.4E+38), 3.4E+38, 8);
  RD(uint64, 123, float, static_cast<float>(-3.4E+38), 3.4E+38, 8);
  RD(uint64, 4294967295, float, static_cast<float>(-3.4E+38), 3.4E+38, 8);
  //edge
  RD(float, FLT_MAX, uint64, 0, FLT_MAX, 8);
  RD(float, FLT_MAX, uint64, UINT64_MAX, FLT_MAX, 8);
  RD(float, -FLT_MAX, uint64, 0, -FLT_MAX, 8);
  RD(float, -FLT_MAX, uint64, INT64_MAX, -FLT_MAX, 8);

  RD(uint64, UINT64_MAX, float, FLT_MAX, -FLT_MAX, 8);
  RD(uint64, UINT64_MAX, float, -FLT_MAX, FLT_MAX, 8);
  RD(uint64, 0, float, FLT_MAX, -FLT_MAX, 8);
  RD(uint64, 0, float, -FLT_MAX, FLT_MAX, 8);
}


TEST_F(ObExprMinusTest, double_number)
{
  ObMalloc buf;
  number::ObNumber num1;
  number::ObNumber num2;
  number::ObNumber num3;
  //number double
  RN1(0, double, 0, 0);
  RN1(0, double, 123.45, -123.45);
  RN1(0, double, -123.45, 123.45);
  RN1(0, double, 98765.4321, -98765.4321);
  RN1(0, double, -98765.4321, 98765.4321);
  RN1(123.45, double, 0, 123.45);
  RN1(123.45, double, 123.45, 0);
  RN1(123.45, double, -123.45, 246.9);
  RN1(123.45, double, 98765.4321, -98641.9821);
  RN1(123.45, double, -98765.4321, 98888.8821);
  RN1(-123.45, double, 0, -123.45);
  RN1(-123.45, double, 123.45, -246.9);
  RN1(-123.45, double, -123.45, 0);
  RN1(-123.45, double, 98765.4321, -98888.8821);
  RN1(-123.45, double, -98765.4321, 98641.9821);
  //double number
  RN2(double, 0, 0, 0);
  RN2(double, 123.45, 0, 123.45);
  RN2(double, -123.45, 0, -123.45);
  RN2(double, 98765.4321, 0, 98765.4321);
  RN2(double, -98765.4321, 0, -98765.4321);
  RN2(double, 0, 123.45, -123.45);
  RN2(double, 123.45, 123.45, 0);
  RN2(double, -123.45, 123.45, -246.9);
  RN2(double, 98765.4321, 123.45, 98641.9821);
  RN2(double, -98765.4321, 123.45, -98888.8821);
  RN2(double, 0, -123.45, 123.45);
  RN2(double, 123.45, -123.45, 246.9);
  RN2(double, -123.45, -123.45, 0);
  RN2(double, 98765.4321, -123.45, 98888.8821);
  RN2(double, -98765.4321, -123.45, -98641.9821);
}


TEST_F(ObExprMinusTest, float_number)
{
  ObMalloc buf;
  number::ObNumber num1;
  number::ObNumber num2;
  number::ObNumber num3;
  //number float
  RN1(0, float, static_cast<float>(0), 0);
  RN1(0, float, static_cast<float>(123), -123);
  RN1(0, float, static_cast<float>(-123), 123);
  RN1(0, float, static_cast<float>(456), -456);
  RN1(0, float, static_cast<float>(-87), 87);
  RN1(123.45, float, static_cast<float>(0), 123.45);
  RN1(123.45, float, static_cast<float>(123), 0.45);
  RN1(123.45, float, static_cast<float>(-123), 246.45);
  RN1(123.45, float, static_cast<float>(456), -332.55);
  RN1(123.45, float, static_cast<float>(-87), 210.45);
  RN1(-123.45, float, static_cast<float>(0), -123.45);
  RN1(-123.45, float, static_cast<float>(123), -246.45);
  RN1(-123.45, float, static_cast<float>(-123), -0.45);
  RN1(-123.45, float, static_cast<float>(456), -579.45);
  RN1(-123.45, float, static_cast<float>(-87), -36.45);
  //float number
  RN2(float, static_cast<float>(0), 0, 0);
  RN2(float, static_cast<float>(123), 0, 123);
  RN2(float, static_cast<float>(-123), 0, -123);
  RN2(float, static_cast<float>(456), 0, 456);
  RN2(float, static_cast<float>(-87), 0, -87);
  RN2(float, static_cast<float>(0), 123.45, -123.45);
  RN2(float, static_cast<float>(123), 123.45, -0.45);
  RN2(float, static_cast<float>(-123), 123.45, -246.45);
  RN2(float, static_cast<float>(456), 123.45, 332.55);
  RN2(float, static_cast<float>(-87), 123.45, -210.45);
  RN2(float, static_cast<float>(0), -123.45, 123.45);
  RN2(float, static_cast<float>(123), -123.45, 246.45);
  RN2(float, static_cast<float>(-123), -123.45, 0.45);
  RN2(float, static_cast<float>(456), -123.45, 579.45);
  RN2(float, static_cast<float>(-87), -123.45, 36.45);
}


TEST_F(ObExprMinusTest, number_int)
{
  ObMalloc buf;
  number::ObNumber num1;
  number::ObNumber num2;
  number::ObNumber num3;
  //tiny small medium
  RN1(0, tinyint, 0, 0);
  RN1(0, tinyint, 42, -42);
  RN2(tinyint, 56, 0, 56);
  RN2(tinyint, -56, 78, -134);
  RN1(0, smallint, 0, 0);
  RN1(0, smallint, 42, -42);
  RN2(smallint, 56, 0, 56);
  RN2(smallint, -56, 78, -134);
  RN1(0, mediumint, 0, 0);
  RN1(0, mediumint, 42, -42);
  RN2(mediumint, 56, 0, 56);
  RN2(mediumint, -56, 78, -134);
  //number int
  RN1(0, int, 0, 0);
  RN1(0, int, 42, -42);
  RN1(0, int, -42, 42);
  RN1(0, int, 256, -256);
  RN1(123.45, int, 0, 123.45);
  RN1(123.45, int, 42, 81.45);
  RN1(123.45, int, -42, 165.45);
  RN1(123.45, int, 256, -132.55);
  RN1(-123.45, int, 0, -123.45);
  RN1(-123.45, int, 42, -165.45);
  RN1(-123.45, int, -42, -81.45);
  RN1(-123.45, int, 256, -379.45);
  //int number
  RN2(int, 0, 0, 0);
  RN2(int, 42, 0, 42);
  RN2(int, -42, 0, -42);
  RN2(int, 256, 0, 256);
  RN2(int, 0, 123.45, -123.45);
  RN2(int, 42, 123.45, -81.45);
  RN2(int, -42, 123.45, -165.45);
  RN2(int, 256, 123.45, 132.55);
  RN2(int, 0, -123.45, 123.45);
  RN2(int, 42, -123.45, 165.45);
  RN2(int, -42, -123.45, 81.45);
  RN2(int, 256, -123.45, 379.45);
  //edge
  RN1(-1000000000000, int, 2147483647, -1002147483647);
  RN1(-100000000000000000000000000000000000000000000000000000000000000000,
      int, 2147483647, -100000000000000000000000000000000000000000000000000000002147483647);
  RN1(-10000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000,
      int, 2147483647, -10000000000000000000000000000000000000000000000000000000000000000000000000000000000000000002147483647);
}


TEST_F(ObExprMinusTest, number_uint)
{
  ObMalloc buf;
  number::ObNumber num1;
  number::ObNumber num2;
  number::ObNumber num3;
  //tiny small medium
  RN1(0, utinyint, 0, 0);
  RN1(0, utinyint, 42, -42);
  RN2(utinyint, 56, 0, 56);
  RN2(utinyint, 56, 78, -22);
  RN1(0, usmallint, 0, 0);
  RN1(0, usmallint, 42, -42);
  RN2(usmallint, 56, 0, 56);
  RN2(usmallint, 56, 78, -22);
  RN1(0, umediumint, 0, 0);
  RN1(0, umediumint, 42, -42);
  RN2(umediumint, 56, 0, 56);
  RN2(umediumint, 56, 78, -22);
  //number uint64
  RN1(0, uint64, 0, 0);
  RN1(0, uint64, 42, -42);
  RN1(0, uint64, 4294967295, -4294967295);
  RN1(123.45, uint64, 0, 123.45);
  RN1(123.45, uint64, 42, 81.45);
  RN1(123.45, uint64, 4294967295, -4294967171.55);
  RN1(-123.45, uint64, 0, -123.45);
  RN1(-123.45, uint64, 42, -165.45);
  RN1(-123.45, uint64, 4294967295, -4294967418.45);
  RN1(1000000000000000, uint64, 0, 1000000000000000);
  RN1(1000000000000000, uint64, 42, 999999999999958);
  RN1(1000000000000000, uint64, 4294967295, 999995705032705);
  //uint64 number
  RN2(uint64, 0, 0, 0);
  RN2(uint64, 42, 0, 42);
  RN2(uint64, 4294967295, 0, 4294967295);
  RN2(uint64, 0, 123.45, -123.45);
  RN2(uint64, 42, 123.45, -81.45);
  RN2(uint64, 4294967295, 123.45, 4294967171.55);
  RN2(uint64, 0, -123.45, 123.45);
  RN2(uint64, 42, -123.45, 165.45);
  RN2(uint64, 4294967295, -123.45, 4294967418.45);
  RN2(uint64, 0, 1000000000000000, -1000000000000000);
  RN2(uint64, 42, 1000000000000000, -999999999999958);
  RN2(uint64, 4294967295, 1000000000000000, -999995705032705);
  //edge
  RN1(-1000000000000, uint64, 2147483647, -1002147483647);
  RN1(-100000000000000000000000000000000000000000000000000000000000000000, uint64,
      2147483647, -100000000000000000000000000000000000000000000000000000002147483647);
  RN1(-10000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000,
      uint64, 2147483647, -10000000000000000000000000000000000000000000000000000000000000000000000000000000000000000002147483647);
}


TEST_F(ObExprMinusTest, number_string)
{
  ObMalloc buf;
  number::ObNumber num1;
  number::ObNumber num2;
  number::ObNumber num3;
  //number int
  RN1(0, varchar, "0", 0);
  RN1(0, varchar, "42", -42);
  RN1(0, varchar, "-42", 42);
  RN1(0, varchar, "256", -256);
  RN1(123.45, varchar, "0", 123.45);
  RN1(123.45, varchar, "42", 81.45);
  RN1(123.45, varchar, "-42", 165.45);
  RN1(123.45, varchar, "256", -132.55);
  RN1(-123.45, varchar, "0", -123.45);
  RN1(-123.45, varchar, "42", -165.45);
  RN1(-123.45, varchar, "-42", -81.45);
  RN1(-123.45, varchar, "256", -379.45);
  //int number
  RN2(varchar, "0", 0, 0);
  RN2(varchar, "42", 0, 42);
  RN2(varchar, "-42", 0, -42);
  RN2(varchar, "256", 0, 256);
  RN2(varchar, "0", 123.45, -123.45);
  RN2(varchar, "42", 123.45, -81.45);
  RN2(varchar, "-42", 123.45, -165.45);
  RN2(varchar, "256", 123.45, 132.55);
  RN2(varchar, "0", -123.45, 123.45);
  RN2(varchar, "42", -123.45, 165.45);
  RN2(varchar, "-42", -123.45, 81.45);
  RN2(varchar, "256", -123.45, 379.45);
}


TEST_F(ObExprMinusTest, double_string)
{
  ObMalloc buf;
  //double string
  RD(double, 1, varchar, "-123.4", 124.4, 5);
  RD(double, -1, varchar, "0", -1, 5);
  RD(double, -1, varchar, "1", -2, 5);
  RD(double, 123.4, varchar, "123.4", 0, 5);
  RD(double, 123.4, varchar, "-123.4", 246.8, 5);
  RD(double, 123.4, varchar, "1", 122.4, 5);
  //string double
  RD(varchar, "1", double, -123.4, 124.4, 5);
  RD(varchar, "-1", double, 0, -1, 5);
  RD(varchar, "-1", double, 1, -2, 5);
  RD(varchar, "123.4", double, 123.4, 0, 5);
  RD(varchar, "123.4", double, -123.4, 246.8, 5);
  RD(varchar, "123.4", double, 1, 122.4, 5);
}

TEST_F(ObExprMinusTest, float_string)
{
  ObMalloc buf;
  //float string
  RD(float, static_cast<float>(1), varchar, "-123.4", 124.4, 5);
  RD(float, static_cast<float>(-1), varchar, "0", -1, 5);
  RD(float, static_cast<float>(-1), varchar, "1", -2, 5);
  RD(float, static_cast<float>(123.4), varchar, "123.4", 0, 5);
  RD(float, static_cast<float>(123.4), varchar, "-123.4", 246.8, 5);
  RD(float, static_cast<float>(123.4), varchar, "1", 122.4, 5);
  //string float
  RD(varchar, "1", float, static_cast<float>(-123.4), 124.4, 5);
  RD(varchar, "-1", float, static_cast<float>(0), -1, 5);
  RD(varchar, "-1", float, static_cast<float>(1), -2, 5);
  RD(varchar, "123.4", float, static_cast<float>(123.4), 0, 5);
  RD(varchar, "123.4", float, static_cast<float>(-123.4), 246.8, 5);
  RD(varchar, "123.4", float, static_cast<float>(1), 122.4, 5);
}


TEST_F(ObExprMinusTest, string_int)
{
  ObMalloc buf;
  //tiny small medium
  RD(varchar, "0", tinyint, 10, -10, 5);
  RD(varchar, "123.456", tinyint, 20, 103.456, 5);
  RD(varchar, "-123.456", smallint, 30, -153.456, 5);
  RD(varchar, "0", smallint, -40, 40, 5);
  RD(varchar, "123.456", mediumint, -50, 173.456, 5);
  RD(varchar, "-123.456", mediumint, -60, -63.456, 5);
  RD(tinyint, 10, varchar, "0", 10, 5);
  RD(tinyint, 20, varchar, "123.456", -103.456, 5);
  RD(smallint, 30, varchar, "-123.456", 153.456, 5);
  RD(smallint, -40, varchar, "0", -40, 5);
  RD(mediumint, -50, varchar, "123.456", -173.456, 5);
  RD(mediumint, -60, varchar, "-123.456", 63.456, 5);

  //string int
  RD(varchar, "0", int, 100, -100, 5);
  RD(varchar, "123.456", int, 100, 23.456, 5);
  RD(varchar, "-123.456", int, 100, -223.456, 5);
  RD(varchar, "0", int, -100, 100, 5);
  RD(varchar, "123.456", int, -100, 223.456, 5);
  RD(varchar, "-123.456", int, -100, -23.456, 5);
  //int string
  RD(int, 100, varchar, "0", 100, 5);
  RD(int, 100, varchar, "123.456", -23.456, 5);
  RD(int, 100, varchar, "-123.456", 223.456, 5);
  RD(int, -100, varchar, "0", -100, 5);
  RD(int, -100, varchar, "123.456", -223.456, 5);
  RD(int, -100, varchar, "-123.456", 23.456, 5);
}

TEST_F(ObExprMinusTest, string_uint)
{
  ObMalloc buf;
  //utiny usmall umedium
  RD(varchar, "0", utinyint, 0, 0, 5);
  RD(varchar, "123.456", utinyint, 10, 113.456, 5);
  RD(varchar, "-123.456", usmallint, 20, -143.456, 5);
  RD(varchar, "0", usmallint, 30, -30, 5);
  RD(varchar, "123.456", umediumint, 40, 83.456, 5);
  RD(varchar, "-123.456", umediumint, 50, -173.456, 5);
  RD(utinyint, 0, varchar, "0", 0, 5);
  RD(utinyint, 10, varchar, "123.456", -113.456, 5);
  RD(usmallint, 20, varchar, "-123.456", 143.456, 5);
  RD(usmallint, 30, varchar, "0", 30, 5);
  RD(umediumint, 40, varchar, "123.456", -83.456, 5);
  RD(umediumint, 50, varchar, "-123.456", 173.456, 5);
  //string uint
  RD(varchar, "0", uint64, 100, -100, 5);
  RD(varchar, "123.456", uint64, 100, 23.456, 5);
  RD(varchar, "-123.456", uint64, 100, -223.456, 5);
  //uint string
  RD(uint64, 100, varchar, "0", 100, 5);
  RD(uint64, 100, varchar, "123.456", -23.456, 5);
  RD(uint64, 100, varchar, "-123.456", 223.456, 5);
}


TEST_F(ObExprMinusTest, float_double_test)
{
  ObMalloc buf;

  // double_float
  R(double, -123.0, double, -123.0, double, 0);
  R(float, -123.0, double, -123.0, double, 0);
  R(float, -123.0, float, -123.0, double, 0);

  // float_int_uint
  R(float, static_cast<float>(INT64_MIN), int, INT64_MIN, double, static_cast<double>(INT64_MIN) - static_cast<double>(INT64_MIN));
  R(float, static_cast<float>(INT64_MIN), int, 0, double, static_cast<double>(INT64_MIN));
  R(float, static_cast<float>(INT64_MIN), int, INT64_MAX, double, static_cast<double>(INT64_MIN) - static_cast<double>(INT64_MAX));
  R(float, 0, int, INT64_MIN, double, -static_cast<double>(INT64_MIN));
  R(float, 0, int, 0, float, 0);
  R(float, 0, int, INT64_MAX, double, -static_cast<double>(INT64_MAX));
  R(float, static_cast<float>(INT64_MAX), int, INT64_MIN, double, static_cast<double>(INT64_MAX) - static_cast<double>(INT64_MIN));
  R(float, static_cast<float>(INT64_MAX), int, 0, double, static_cast<double>(INT64_MAX));
  R(float, static_cast<float>(INT64_MAX), int, INT64_MAX, double, static_cast<double>(INT64_MAX) - static_cast<double>(INT64_MAX));
  R(float, static_cast<float>(UINT64_MAX), int, INT64_MIN, double, static_cast<double>(UINT64_MAX) - static_cast<double>(INT64_MIN));
  R(float, static_cast<float>(UINT64_MAX), int, 0, double, static_cast<double>(UINT64_MAX));
  R(float, static_cast<float>(UINT64_MAX), int, INT64_MAX, double, static_cast<double>(UINT64_MAX) - static_cast<double>(INT64_MAX));
  R(float, static_cast<float>(UINT64_MAX), uint64, UINT64_MAX, double, static_cast<double>(UINT64_MAX) - static_cast<double>(UINT64_MAX));

  // double_int_uint
  R(double, static_cast<double>(INT64_MIN), int, INT64_MIN, double, static_cast<double>(INT64_MIN) - static_cast<double>(INT64_MIN));
  R(double, static_cast<double>(INT64_MIN), int, 0, double, static_cast<double>(INT64_MIN));
  R(double, static_cast<double>(INT64_MIN), int, INT64_MAX, double, static_cast<double>(INT64_MIN) - static_cast<double>(INT64_MAX));
  R(double, 0, int, INT64_MIN, double, -static_cast<double>(INT64_MIN));
  R(double, 0, int, 0, double, 0);
  R(double, 0, int, INT64_MAX, double, -static_cast<double>(INT64_MAX));
  R(double, static_cast<double>(INT64_MAX), int, INT64_MIN, double, static_cast<double>(INT64_MAX) - static_cast<double>(INT64_MIN));
  R(double, static_cast<double>(INT64_MAX), int, 0, double, static_cast<double>(INT64_MAX));
  R(double, static_cast<double>(INT64_MAX), int, INT64_MAX, double, static_cast<double>(INT64_MAX) - static_cast<double>(INT64_MAX));
  R(double, static_cast<double>(UINT64_MAX), int, INT64_MIN, double, static_cast<double>(UINT64_MAX) - static_cast<double>(INT64_MIN));
  R(double, static_cast<double>(UINT64_MAX), int, 0, double, static_cast<double>(UINT64_MAX));
  R(double, static_cast<double>(UINT64_MAX), int, INT64_MAX, double, static_cast<double>(UINT64_MAX) - static_cast<double>(INT64_MAX));
  R(double, static_cast<double>(UINT64_MAX), uint64, UINT64_MAX, double, static_cast<double>(UINT64_MAX) - static_cast<double>(UINT64_MAX));
}

TEST_F(ObExprMinusTest, number_test)
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

  R(number, num123_n, int, -123, number, num_zero);
  R(number, num123_n, int,  123, number, num246_n);
  R(number, num123_p, int,  123, number, num_zero);
  R(number, num123_n, uint64, 123, number, num246_n);
  R(number, num123_p, uint64, 123, number, num_zero);
  R(number, num123_n, float, -123.0, number, num_zero);
  R(number, num123_n, float,  123.0, number, num246_n);
  R(number, num123_p, float,  -123.0, number, num246_p);
  R(number, num123_n, double, -123.0, number, num_zero);
  R(number, num123_n, double,  123.0, number, num246_n);
  R(number, num123_p, double,  -123.0, number, num246_p);
  R(number, num123_n, number, num123_n, number, num_zero);
  R(number, num123_n, number, num123_p, number, num246_n);
  R(number, num123_p, number, num123_p, number, num_zero);
  R(number, num123_p, number, num123_n, number, num246_p);
}

TEST_F(ObExprMinusTest, flow_test)
{
  ObMalloc buf;
  // int_int
  FE(int, INT64_MAX, int, INT64_MIN);
  FE(int, INT64_MIN, int, 100);

  //int_uint
  FE(int, 100, uint64, 200);
  FE(int, 100, uint64, UINT64_MAX);
  FE(int, INT64_MIN, uint64, 100);
  FE(int, INT64_MIN, uint64, UINT64_MAX);
  FE(uint64, UINT64_MAX, int, -2);
  FE(uint64, 100, int, 500);

  // uint_uint
  FE(uint64, 100, uint64, UINT64_MAX);
  FE(uint64, 100, uint64, 200);
  FE(uint64, UINT64_MAX-2, uint64, UINT64_MAX);
}

TEST_F(ObExprMinusTest, int_string_test)
{
  ObMalloc buf;
  // int_string normal
  R(int, 16413, varchar, "29168", double, -12755);
  R(int, -900, varchar, "32591", double, -33491);
  R(int, 18762, varchar, "-1655", double, 20417);
  R(int, 17410, varchar, "6359", double, 11051);
  //R(int, 9161, varchar, "+18636", double, -9475);


  // int_string alpha
  //R(int, 10, varchar, "a0.22222", double, 10);
  //R(int, 10, varchar, "0.2a2222", double, 9.8);
  //R(int, 10, varchar, "-0.2a2222", double, 10.2);
  //R(int, 10, varchar, "-3430a", double, 3440);
  //R(int, 10, varchar, "-b3430a", double, 10);
  //R(int, 10, varchar, "-.b3430a", double, 10);
  //R(int, 10, varchar, "10b.3430a", double, 0);
  //R(int, 10, varchar, "+10-b.3430a", double, 0);


  // uint_string
  //R(uint64, 10, varchar, "a0.22222", double, 10);
  //R(uint64, 10, varchar, "0.2a2222", double, 9.8);
  //R(uint64, 10, varchar, "-0.2a2222", double, 10.2);
  //R(uint64, 10, varchar, "-3430a", double, 3440);
  //R(uint64, 10, varchar, "-b3430a", double, 10);
  //R(uint64, 10, varchar, "-.b3430a", double, 10);
  ///R(uint64, 10, varchar, "10b.3430a", double, 0);
  //R(uint64, 10, varchar, "+10-b.3430a", double, 0);
}

/*TEST_F(ObExprMinusTest, basic_test)
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
  R(int, 1, int, 2, -1);
  R(int, 2, float, 3.0, -1.0);
  R(int, 3, double, 4.0, -1.0);
  R(int, 4, datetime, 5, OB_ERR_INVALID_TYPE_FOR_OP);
  R(int, 5, precise_datetime, 6, OB_ERR_INVALID_TYPE_FOR_OP);
  R(int, 6, varchar, "7", -1);
  R(int, 6, varchar, "a", OB_ERR_CAST_VARCHAR_TO_NUMBER);
  R(int, 6, varchar, "07a", OB_ERR_CAST_VARCHAR_TO_NUMBER);
  R(int, 8, ctime, 9, OB_ERR_INVALID_TYPE_FOR_OP);
  R(int, 9, mtime, 10, OB_ERR_INVALID_TYPE_FOR_OP);
  R(int, 10, ext, 2, OB_ERR_INVALID_TYPE_FOR_OP);
  R(int, 11, bool, 1, OB_ERR_INVALID_TYPE_FOR_OP);
  R(int, 11, bool, 0, OB_ERR_INVALID_TYPE_FOR_OP);
  R(int, -9223372036854775807, int, 2, OB_OPERATE_OVERFLOW);
  //R(null, 0, , 2, ObNullType); decimal not supported

  //ObFloatType
  R(float, 0.0, null, 1, ObNullType);
  R(float, 1.0, int, 2, -1.0);
  R(float, 2.0, float, 3.0, -1.0);
  R(float, 3.0, double, 4.0, -1.0);
  R(float, 4.0, datetime, 5, OB_ERR_INVALID_TYPE_FOR_OP);
  R(float, 5.0, precise_datetime, 6, OB_ERR_INVALID_TYPE_FOR_OP);
  R(float, 6.0, varchar, "7", -1.0);
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
  R(double, 1.0, int, 2, -1.0);
  R(double, 2.0, float, 3.0, -1.0);
  R(double, 3.0, double, 4.0, -1.0);
  R(double, 4.0, datetime, 5, OB_ERR_INVALID_TYPE_FOR_OP);
  R(double, 5.0, precise_datetime, 6, OB_ERR_INVALID_TYPE_FOR_OP);
  R(double, 6.0, varchar, "7", -1.0);
  R(double, 6.0, varchar, "-0.7", 6.7);
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
  R(varchar, "1", int, 2, -1);
  R(varchar, "2", float, 3.0, -1.0);
  R(varchar, "2.12345", float, 3.0, -0.87655);
  R(varchar, "3", double, 4.0, -1.0);
  R(varchar, "3.12345", double, 4.0, -0.87655);
  R(varchar, "4", datetime, 5, OB_ERR_INVALID_TYPE_FOR_OP);
  R(varchar, "5", precise_datetime, 6, OB_ERR_INVALID_TYPE_FOR_OP);
  R(varchar, "6", varchar, "7", -1);
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

  //R(null, 0, , 2, OB_ERR_INVALID_TYPE_FOR_OP);
}
*/


int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
