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
#include "sql/engine/expr/ob_expr_length.h"
#include "ob_expr_test_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

class ObExprLengthTest : public ::testing::Test {
public:
  ObExprLengthTest();
  virtual ~ObExprLengthTest();
  virtual void SetUp();
  virtual void TearDown();

private:
  // disallow copy
  ObExprLengthTest(const ObExprLengthTest& other);
  ObExprLengthTest& operator=(const ObExprLengthTest& other);

private:
  // data members
};
ObExprLengthTest::ObExprLengthTest()
{}

ObExprLengthTest::~ObExprLengthTest()
{}

void ObExprLengthTest::SetUp()
{}

void ObExprLengthTest::TearDown()
{}

#define T(obj, t1, v1, ref_type, ref_value) EXPECT_RESULT1(obj, &buf, calc_result1, t1, v1, ref_type, ref_value)
#define F(obj, t1, v1) EXPECT_FAIL_RESULT1(obj, &buf, calc_result1, t1, v1)
#define F0(obj, t1) EXPECT_FAIL_RESULT0(obj, &buf, calc_result1, t1)
#define T0(obj, t1, ref_type, ref_value) EXPECT_RESULT0(obj, &buf, calc_result1, t1, ref_type, ref_value)

TEST_F(ObExprLengthTest, basic_test)
{
  ObArenaAllocator buf(ObModIds::OB_SQL_SESSION);
  ObExprLength length(buf);
  ASSERT_EQ(1, length.get_param_num());

  T0(length, null, null, 0);
  T(length, varchar, "helo", int, 4);
  T(length, varchar, "", int, 0);
  T(length, int, 1, int, 1);
  T(length, int, 12, int, 2);
  T(length, varchar, "好", int, 3);
}

TEST_F(ObExprLengthTest, fail_test)
{
  ObArenaAllocator buf(ObModIds::OB_SQL_SESSION);
  ObExprLength length(buf);
  ASSERT_EQ(1, length.get_param_num());
}

int main(int argc, char** argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
