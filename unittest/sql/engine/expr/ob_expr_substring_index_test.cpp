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
#include "sql/engine/expr/ob_expr_substring_index.h"
#include "ob_expr_test_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

class ObExprSubstringIndexTest : public ::testing::Test
{
public:
  ObExprSubstringIndexTest();
  virtual ~ObExprSubstringIndexTest();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  ObExprSubstringIndexTest(const ObExprSubstringIndexTest &other);
  ObExprSubstringIndexTest& operator=(const ObExprSubstringIndexTest &other);
private:
  // data members
};
ObExprSubstringIndexTest::ObExprSubstringIndexTest()
{
}

ObExprSubstringIndexTest::~ObExprSubstringIndexTest()
{
}

void ObExprSubstringIndexTest::SetUp()
{
}

void ObExprSubstringIndexTest::TearDown()
{
}

#define T(obj, t1, v1, t2, v2, t3, v3, ref_type, ref_value) EXPECT_RESULT3(obj, &buf, calc_result3, t1, v1, t2, v2, t3, v3, ref_type, ref_value)
#define F(obj, t1, v1, t2, v2, t3, v3, ref_type, ref_value) EXPECT_FAIL_RESULT3(obj, &buf, calc_result3, t1, v1, t2, v2, t3, v3, ref_type, ref_value)

TEST_F(ObExprSubstringIndexTest, basic_test)
{
  ObExprSubstringIndex substr_index;
  ObExprStringBuf buf;

  ASSERT_EQ(3, substr_index.get_param_num());

  // null
  T(substr_index, null, 0, null, 0, null, 0, null, 0);

  T(substr_index, null, 0, null, 0, int, 0, null, 0);
  T(substr_index, null, 0, null, 0, int, 1, null, 0);
  T(substr_index, null, 0, null, 0, int, 2, null, 0);
  T(substr_index, null, 0, null, 0, int, 3, null, 0);
  T(substr_index, null, 0, null, 0, int, -1, null, 0);
  T(substr_index, null, 0, null, 0, int, -2, null, 0);
  T(substr_index, null, 0, null, 0, int, -3, null, 0);

  T(substr_index, varchar, "abcdabc", null, 0, null, 0, null, 0);
  T(substr_index, varchar, "abcdabc", null, 0, int, 0, null, 0);
  T(substr_index, varchar, "abcdabc", null, 0, int, 1, null, 0);
  T(substr_index, varchar, "abcdabc", null, 0, int, 2, null, 0);
  T(substr_index, varchar, "abcdabc", null, 0, int, 3, null, 0);
  T(substr_index, varchar, "abcdabc", null, 0, int, -1, null, 0);
  T(substr_index, varchar, "abcdabc", null, 0, int, -2, null, 0);
  T(substr_index, varchar, "abcdabc", null, 0, int, -3, null, 0);

  T(substr_index, null, 0, varchar, "abc", null, 0, null, 0);
  T(substr_index, null, 0, varchar, "abc", int, 0, null, 0);
  T(substr_index, null, 0, varchar, "abc", int, 1, null, 0);
  T(substr_index, null, 0, varchar, "abc", int, 2, null, 0);
  T(substr_index, null, 0, varchar, "abc", int, 3, null, 0);
  T(substr_index, null, 0, varchar, "abc", int, -1, null, 0);
  T(substr_index, null, 0, varchar, "abc", int, -2, null, 0);
  T(substr_index, null, 0, varchar, "abc", int, -3, null, 0);

  // empty
  T(substr_index, varchar, "", varchar, "", int, 0, varchar, "");
  T(substr_index, varchar, "", varchar, "", int, 1, varchar, "");
  T(substr_index, varchar, "", varchar, "", int, 2, varchar, "");
  T(substr_index, varchar, "", varchar, "", int, 3, varchar, "");
  T(substr_index, varchar, "", varchar, "", int, -1, varchar, "");
  T(substr_index, varchar, "", varchar, "", int, -2, varchar, "");
  T(substr_index, varchar, "", varchar, "", int, -3, varchar, "");

  T(substr_index, varchar, "abcdabc", varchar, "", int, 0, varchar, "");
  T(substr_index, varchar, "abcdabc", varchar, "", int, 1, varchar, "");
  T(substr_index, varchar, "abcdabc", varchar, "", int, 2, varchar, "");
  T(substr_index, varchar, "abcdabc", varchar, "", int, 3, varchar, "");
  T(substr_index, varchar, "abcdabc", varchar, "", int, -1, varchar, "");
  T(substr_index, varchar, "abcdabc", varchar, "", int, -2, varchar, "");
  T(substr_index, varchar, "abcdabc", varchar, "", int, -3, varchar, "");

  T(substr_index, varchar, "", varchar, "abc", int, 0, varchar, "");
  T(substr_index, varchar, "", varchar, "abc", int, 1, varchar, "");
  T(substr_index, varchar, "", varchar, "abc", int, 2, varchar, "");
  T(substr_index, varchar, "", varchar, "abc", int, 3, varchar, "");
  T(substr_index, varchar, "", varchar, "abc", int, -1, varchar, "");
  T(substr_index, varchar, "", varchar, "abc", int, -2, varchar, "");
  T(substr_index, varchar, "", varchar, "abc", int, -3, varchar, "");

  // normal
  T(substr_index, varchar, "abcdabc", varchar, "abc", int, 0, varchar, "");
  T(substr_index, varchar, "abcdabc", varchar, "abc", int, 1, varchar, "");
  T(substr_index, varchar, "abcdabc", varchar, "abc", int, 2, varchar, "abcd");
  T(substr_index, varchar, "abcdabc", varchar, "abc", int, 3, varchar, "abcdabc");
  T(substr_index, varchar, "abcdabc", varchar, "abc", int, -1, varchar, "");
  T(substr_index, varchar, "abcdabc", varchar, "abc", int, -2, varchar, "dabc");
  T(substr_index, varchar, "abcdabc", varchar, "abc", int, -3, varchar, "abcdabc");

  // convert
  T(substr_index, varchar, "abcdabc", varchar, "abc", varchar, "0", varchar, "");
  T(substr_index, varchar, "abcdabc", varchar, "abc", varchar, "1", varchar, "");
  T(substr_index, varchar, "abcdabc", varchar, "abc", varchar, "2", varchar, "abcd");
  T(substr_index, varchar, "abcdabc", varchar, "abc", varchar, "3", varchar, "abcdabc");
  T(substr_index, varchar, "abcdabc", varchar, "abc", varchar, "-1", varchar, "");
  T(substr_index, varchar, "abcdabc", varchar, "abc", varchar, "-2", varchar, "dabc");
  T(substr_index, varchar, "abcdabc", varchar, "abc", varchar, "-3", varchar, "abcdabc");
}

TEST_F(ObExprSubstringIndexTest, fail_test)
{
  ObExprSubstringIndex substr_index;
  ObExprStringBuf buf;

  ASSERT_EQ(3, substr_index.get_param_num());
  F(substr_index, varchar, "abcdabc", varchar, "abc", max, 0, varchar, "");
  F(substr_index, varchar, "abcdabc", varchar, "abc", min, 0, varchar, "");

  F(substr_index, varchar, "abcdabc", max, 0, int, 1, varchar, "");
  F(substr_index, varchar, "abcdabc", max, 0, max, 0, varchar, "");
  F(substr_index, varchar, "abcdabc", max, 0, min, 0, varchar, "");
  F(substr_index, varchar, "abcdabc", min, 0, int, 1, varchar, "");
  F(substr_index, varchar, "abcdabc", min, 0, max, 0, varchar, "");
  F(substr_index, varchar, "abcdabc", min, 0, min, 0, varchar, "");

  F(substr_index, max, 0, varchar, "abc", int, 1, varchar, "");
  F(substr_index, max, 0, varchar, "abc", max, 0, varchar, "");
  F(substr_index, max, 0, varchar, "abc", min, 0, varchar, "");
  F(substr_index, min, 0, varchar, "abc", int, 1, varchar, "");
  F(substr_index, min, 0, varchar, "abc", max, 0, varchar, "");
  F(substr_index, min, 0, varchar, "abc", min, 0, varchar, "");
}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
