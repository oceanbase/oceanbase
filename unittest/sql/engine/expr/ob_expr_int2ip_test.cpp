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
#include "sql/engine/expr/ob_expr_int2ip.h"
#include "ob_expr_test_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

class ObExprInt2ipTest : public ::testing::Test
{
public:
  ObExprInt2ipTest();
  virtual ~ObExprInt2ipTest();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  ObExprInt2ipTest(const ObExprInt2ipTest &other);
  ObExprInt2ipTest& operator=(const ObExprInt2ipTest &other);
private:
  // data members
};
ObExprInt2ipTest::ObExprInt2ipTest()
{
}

ObExprInt2ipTest::~ObExprInt2ipTest()
{
}

void ObExprInt2ipTest::SetUp()
{
}

void ObExprInt2ipTest::TearDown()
{
}

#define T(obj, t1, v1, ref_type, ref_value) EXPECT_RESULT1(obj, &buf, calc_result1, t1, v1, ref_type, ref_value)
#define F(obj, t1, v1, ref_type, ref_value) EXPECT_FAIL_RESULT1(obj, &buf, calc_result1, t1, v1, ref_type, ref_value)

TEST_F(ObExprInt2ipTest, basic_test)
{
  ObExprInt2ip int2ip;
  ObExprStringBuf buf;
  ASSERT_EQ(1, int2ip.get_param_num());

  // null
  T(int2ip, null, 0, null, 0);
  T(int2ip, int, 10, varchar, "0.0.0.10");
  T(int2ip, int, 0, varchar, "0.0.0.0");
  T(int2ip, int, 167772160, varchar, "10.0.0.0");
  T(int2ip, int, 4294967295, varchar, "255.255.255.255");
  T(int2ip, int, 4294967296, null, 0);
  T(int2ip, int, 14294967266, null, 0);
}

TEST_F(ObExprInt2ipTest, fail_tst)
{
  ObExprInt2ip int2ip;
  ObExprStringBuf buf;
  ASSERT_EQ(1, int2ip.get_param_num());

  F(int2ip, double, 10.2, null, 0);
  F(int2ip, bool, true, null, 0);
  F(int2ip, varchar, "ABC", null, 0);
  F(int2ip, max, 0, null, 0);
  F(int2ip, min, 0, null, 0);
}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

