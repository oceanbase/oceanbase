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
#include "sql/engine/expr/ob_expr_unhex.h"
#include "ob_expr_test_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

class ObExprUnhexTest : public ::testing::Test
{
public:
  ObExprUnhexTest();
  virtual ~ObExprUnhexTest();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  ObExprUnhexTest(const ObExprUnhexTest &other);
  ObExprUnhexTest& operator=(const ObExprUnhexTest &other);
private:
  // data members
};
ObExprUnhexTest::ObExprUnhexTest()
{
}

ObExprUnhexTest::~ObExprUnhexTest()
{
}

void ObExprUnhexTest::SetUp()
{
}

void ObExprUnhexTest::TearDown()
{
}

#define T(obj, t1, v1, ref_type, ref_value) EXPECT_RESULT1(obj, &buf, calc_result1, t1, v1, ref_type, ref_value)
#define F(obj, t1, v1, ref_type, ref_value) EXPECT_FAIL_RESULT1(obj, &buf, calc_result1, t1, v1, ref_type, ref_value)

TEST_F(ObExprUnhexTest, basic_test)
{
  ObExprUnhex unhex;
  ObExprStringBuf buf;
  ASSERT_EQ(1, unhex.get_param_num());

  // input not varchar
  T(unhex, null, 0, null, 0);
  // ok
  T(unhex, varchar, "41424344", varchar, "ABCD");
  T(unhex, varchar, "61626364", varchar, "abcd");
  T(unhex, varchar, "4E4F", varchar, "NO");
  T(unhex, varchar, "4e4F", varchar, "NO");
  T(unhex, varchar, "4e4f", varchar, "NO");
}

TEST_F(ObExprUnhexTest, fail_test)
{
  ObExprUnhex unhex;
  ObExprStringBuf buf;
  ASSERT_EQ(1, unhex.get_param_num());

  F(unhex, int, 10, null, 0);
  F(unhex, int, 0, null, 0);
  F(unhex, double, 10, null, 0);
  F(unhex, datetime, 10, null, 0);
  F(unhex, precise_datetime, 10212, null, 0);
  F(unhex, bool, true, null, 0);
  F(unhex, bool, false, null, 0);


  F(unhex, max, 0, null, 0);
  F(unhex, min, 0, null, 0);

  T(unhex, varchar, "4x4f", null, 0);
  T(unhex, varchar, "4X4f", null, 0);
  T(unhex, varchar, "4e4", null, 0);
  T(unhex, varchar, "@@", null, 0);
  T(unhex, varchar, "4e4", null, 0);
}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

