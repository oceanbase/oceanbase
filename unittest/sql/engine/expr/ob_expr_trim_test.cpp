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
#include "sql/engine/expr/ob_expr_trim.h"
#include "ob_expr_test_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

class ObExprTrimTest : public ::testing::Test
{
public:
  ObExprTrimTest();
  virtual ~ObExprTrimTest();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  ObExprTrimTest(const ObExprTrimTest &other);
  ObExprTrimTest& operator=(const ObExprTrimTest &other);
private:
  // data members
};
ObExprTrimTest::ObExprTrimTest()
{
}

ObExprTrimTest::~ObExprTrimTest()
{
}

void ObExprTrimTest::SetUp()
{
}

void ObExprTrimTest::TearDown()
{
}

#define LRTRIM 0
#define LTRIM 1
#define RTRIM 2
#define UNKNOWN_TRIM 3

#define T(obj, t1, v1, t2, v2, t3, v3, ref_type, ref_value) EXPECT_RESULT3(obj, &buf, calc_result3, t1, v1, t2, v2, t3, v3, ref_type, ref_value)
#define F(obj, t1, v1, t2, v2, t3, v3, ref_type, ref_value) EXPECT_FAIL_RESULT3(obj, &buf, calc_result3, t1, v1, t2, v2, t3, v3, ref_type, ref_value)

TEST_F(ObExprTrimTest, basic_test)
{
  ObExprTrim trim;
  ObExprStringBuf buf;

  ASSERT_EQ(3, trim.get_param_num());

  // null
  T(trim, null, 0, null, 0, null, 0, null, 0);
  T(trim, int, LRTRIM, null, 0, null, 0, null, 0);
  T(trim, int, LRTRIM, varchar, "hi", null, 0, null, 0);
  T(trim, int, LRTRIM, null, 0, varchar, "hi", null, 0);
  T(trim, int, LRTRIM, min, 0, null, 0, null, 0);
  T(trim, int, LRTRIM, max, 0, null, 0, null, 0);
  T(trim, int, LRTRIM, null, 0, max, 0, null, 0);
  T(trim, int, LRTRIM, null, 0, min, 0, null, 0);

  // normal
  T(trim, int, LRTRIM, varchar, "a", varchar, "aaabbbaaa", varchar, "bbb");
  T(trim, int, LRTRIM, varchar, "a", varchar, "abdada", varchar, "bdad");
  T(trim, int, LRTRIM, varchar, "ab", varchar, "abdada", varchar, "dada");
  T(trim, int, LRTRIM, varchar, "da", varchar, "abdada", varchar, "ab");
  T(trim, int, LRTRIM, varchar, "abdadaddddd", varchar, "abdada", varchar, "abdada");
  T(trim, int, LRTRIM, varchar, "xxxxx", varchar, "abdada", varchar, "abdada");
  // special case: pattern length = 0
  T(trim, int, LRTRIM, varchar, "", varchar, "abdada", varchar, "abdada");
}

TEST_F(ObExprTrimTest, fail_test)
{
  ObExprTrim trim;
  ObExprStringBuf buf;

  ASSERT_EQ(3, trim.get_param_num());

  // promotion
  F(trim, int, LRTRIM, int, 101010, int, 101010, varchar, "");
  F(trim, int, LRTRIM, int, 10, varchar, "101010", varchar, "");
  F(trim, int, LRTRIM, int, 101010, varchar, "101010", varchar, "");
  F(trim, int, LRTRIM, int, 1, varchar, "101010", varchar, "01010");
  F(trim, int, LRTRIM, int, 1010100, varchar, "101010", varchar, "101010");
  F(trim, int, LRTRIM, float, 10.0, varchar, "10.00000000", varchar, "00"); // float have six digit trailing


  F(trim, int, LRTRIM, min, 0, min, 0, varchar, "");
  F(trim, int, LRTRIM, min, 0, max, 0, max, 0);
  F(trim, int, LRTRIM, max, 0, min, 0, min, 0);
  F(trim, int, LRTRIM, max, 0, varchar, "hi", varchar, "hi");
  F(trim, int, LRTRIM, min, 0, varchar, "hi", varchar, "hi");
  F(trim, int, LRTRIM, max, 0, int, 12, varchar, "12");
  F(trim, int, LRTRIM, min, 0, int, 12, varchar, "12");
  F(trim, int, LRTRIM, int, 10, min, 0, min, 0);
  F(trim, int, LRTRIM, int, 10, max, 0, max, 0);

}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

