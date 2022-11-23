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
//#include "sql/engine/expr/ob_expr_mod.h"
#include "sql/engine/expr/ob_expr_int_div.h"
#include "ob_expr_test_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
class ObExprIntDivTest: public ::testing::Test
{
  public:
    ObExprIntDivTest();
    virtual ~ObExprIntDivTest();
    virtual void SetUp();
    virtual void TearDown();
  private:
    // disallow copy
    ObExprIntDivTest(const ObExprIntDivTest &other);
    ObExprIntDivTest& operator=(const ObExprIntDivTest &other);
  protected:
    // data members
};

ObExprIntDivTest::ObExprIntDivTest()
{
}

ObExprIntDivTest::~ObExprIntDivTest()
{
}

void ObExprIntDivTest::SetUp()
{
}

void ObExprIntDivTest::TearDown()
{
}

#define R(t1, v1, t2, v2, res) ARITH_EXPECT(ObExprIntDiv, &buf, calc, t1, v1, t2, v2, res)
#define E(t1, v1, t2, v2, res) ARITH_ERROR(ObExprIntDiv, &buf, calc, t1, v1, t2, v2, res)
#define T(t1, t2, res) ARITH_EXPECT_TYPE(ObExprIntDiv, calc_result_type2, t1, t2, res)
#define TE(t1, t2, res) ARITH_EXPECT_TYPE_WITH_ROW(ObExprIntDiv, calc_result_type2, t1, t2, res)


TEST_F(ObExprIntDivTest, result_test)
{

  ObMalloc  buf;
   
  //ObIntType
  R(int, 6, int, 5, 1);	// int
  R(int, 6, int, -6, -1);
  R(int, 6, int, 7, 0);
  R(int, 0, int, 5, 0);
  R(int, 6, uint64, 5, 1);	// uint
  R(int, 6, uint64, 7, 0);
  R(int, 0, uint64, 5, 0);
  R(int, -6, uint64, 7, 0);
  R(int, 6, float, float(1.2), 5); // float
  R(int, 6, float, float(1.4), 4);
  R(int, 6, float, float(-1.3), -4);
  R(int, 0, float, float(1.2), 0);
  R(int, 6, double, double(1.2), 5); // double
  R(int, 6, double, double(1.4), 4);
  R(int, 6, double, double(-1.3), -4);
  R(int, 0, double, double(1.2), 0);
  R(int, 6, varchar, "5", 1);  // varchar
  R(int, 6, varchar, "7", 0);
  R(int, 6, varchar, "-2", -3);

  //ObUIntType
  R(uint64, 6, int, 5, 1);  // uint64
  R(uint64, 6, int, 6, 1);  
  R(uint64, 6, int, 7, 0);  
  R(uint64, 6, int, -7, 0);
  R(uint64, 6, uint64, 5, 1);	// uint
  R(uint64, 6, uint64, 7, 0);
  R(uint64, 0, uint64, 5, 0);
  R(uint64, 6, float, float(1.2), 5); // float
  R(uint64, 6, float, float(1.4), 4);
  R(uint64, 0, float, float(1.2), 0);
  R(uint64, 6, double, double(1.2), 5); // double
  R(uint64, 6, double, double(1.4), 4);
  // uint div double is uint, so we can't do this test now.
//  R(uint64, 6, double, double(-1.3), -4);
  R(uint64, 0, double, double(1.2), 0);
  R(uint64, 6, varchar, "5", 1);  // var
  R(uint64, 6, varchar, "7", 0);
  // uint div varchar is uint, so we can't do this test now.
//  R(uint64, 6, varchar, "-2", -3);

  //ObFloatType
  R(float, 6.0, int, 5, 1);  // int
  R(float, 6.0, int, 7, 0);
  R(float, 6.0, int, -6, -1);
  R(float, 6.0, uint64, 5, 1);  // uint64
  R(float, 6.0, uint64, 7, 0);
  // float div uint is uint, so we can't do this test now.
//  R(float, -6.0, uint64, 6, -1);
  R(float, 6.0, float, 5.0, 1);  // float
  R(float, 6.0, float, -6.0, -1);
  R(float, 6.0, float, 7.0, 0);
  R(float, 6.0, double, 5.0, 1);  // double
  R(float, 6.0, double, -6.0, -1);
  R(float, 6.0, double, 7.0, 0);
  R(float, 6.0, bool, 1, 6);  // bool
  R(float, 6.0, varchar, "5", 1);  // varchar
  R(float, 6.0, varchar, "7", 0);
  R(float, 6.0, varchar, "-2", -3);
  
  //ObDoubleType
  R(double, 6.0, int, 5, 1);  // int
  R(double, -6.0, int, 6, -1);
  R(double, 6.0, int, 7, 0);
  R(double, 6.0, uint64, 5, 1);  // uint64
  // double div uint is uint, so we can't do this test now.
//  R(double, -6.0, uint64, 6, -1);
  R(double, 6.0, uint64, 7, 0);
  R(double, 6.0, float, 5.0, 1);  // float
  R(double, -6.0, float, 6.0, -1);
  R(double, 6.0, float, 7.0, 0);
  R(double, 6.0, double, 5.0, 1);  // double
  R(double, -6.0, double, 6.0, -1);
  R(double, 6.0, double, 7.0, 0);
  R(double, 6.0, varchar, "5", 1);  // varchar
  R(double, 6.0, varchar, "7", 0);
  R(double, 6.0, varchar, "-2", -3);

  //ObVarcharType
  R(varchar, "6", int, 5, 1);  // int
  R(varchar, "-6.0", int, 6, -1);
  R(varchar, "6.0", int, 7, 0);
  R(varchar, "6", uint64, 5, 1);  // uint64
  // varchar div uint is uint, so we can't do this test now.
//  R(varchar, "-6.0", uint64, 6, -1);
  R(varchar, "6.0", uint64, 7, 0);
  R(varchar, "6", float, 5.0, 1);  // float
  R(varchar, "-6.0", float, 6.0, -1);
  R(varchar, "6.0", float, 7.0, 0);  
  R(varchar, "6", double, 5.0, 1);  // double
  R(varchar, "-6.0", double, 6.0, -1);
  R(varchar, "6.0", double, 7.0, 0);
  R(varchar, "6", varchar, "5", 1);  // varchar
  R(varchar, "-6.0", varchar, "6", -1);
  R(varchar, "6.0", varchar, "7", 0);
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
