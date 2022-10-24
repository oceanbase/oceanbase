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
#include "sql/engine/expr/ob_expr_ip2int.h"
#include "ob_expr_test_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

class ObExprIp2intTest : public ::testing::Test
{
public:
  ObExprIp2intTest();
  virtual ~ObExprIp2intTest();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  ObExprIp2intTest(const ObExprIp2intTest &other);
  ObExprIp2intTest& operator=(const ObExprIp2intTest &other);
private:
  // data members
};
ObExprIp2intTest::ObExprIp2intTest()
{
}

ObExprIp2intTest::~ObExprIp2intTest()
{
}

void ObExprIp2intTest::SetUp()
{
}

void ObExprIp2intTest::TearDown()
{
}

#define T(obj, t1, v1, ref_type, ref_value) EXPECT_RESULT1(obj, &buf, calc_result1, t1, v1, ref_type, ref_value)
#define F(obj, t1, v1, ref_type, ref_value) EXPECT_FAIL_RESULT1(obj, &buf, calc_result1, t1, v1, ref_type, ref_value)

TEST_F(ObExprIp2intTest, basic_test)
{
  ObExprIp2int ip2int;
  ObExprStringBuf buf;
  ASSERT_EQ(1, ip2int.get_param_num());

  // null, extend
  T(ip2int, null, 0, null, 0);
  // normal
  T(ip2int, varchar, "10.23", null, 0);
  T(ip2int, varchar, "10.10.10.10", int, 168430090);
  T(ip2int, varchar, "255.255.255.255", int, 4294967295);
  T(ip2int, varchar, "0.0.0.0", int, 0);
  T(ip2int, varchar, "1.0.0.0", int, 1<<24);
  T(ip2int, varchar, "0.0.0.1", int, 1);

  // take care!
  T(ip2int, varchar, "ABC", null, 0);
  T(ip2int, varchar, "10.lll.ooo.jjj", null, 0);
  T(ip2int, varchar, "999.999.999.999", null, 0);
  T(ip2int, varchar, "10000.0.0.0", null, 0);
}

TEST_F(ObExprIp2intTest, fail_tst)
{
  ObExprIp2int ip2int;
  ObExprStringBuf buf;
  ASSERT_EQ(1, ip2int.get_param_num());

  F(ip2int, max, 0, null, 0);
  F(ip2int, min, 0, null, 0);
  F(ip2int, double, 10.2, null, 0);
  F(ip2int, bool, true, null, 0);
  F(ip2int, int, 10, null, 0);
  F(ip2int, int, 0, null, 0);
}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

