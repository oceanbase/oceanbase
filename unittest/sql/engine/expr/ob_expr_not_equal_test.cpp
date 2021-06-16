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

#include "sql/engine/expr/ob_expr_not_equal.h"
#include <gtest/gtest.h>
#include "ob_expr_test_utils.h"
using namespace oceanbase::common;
using namespace oceanbase::sql;
class ObExprNotEqualTest : public ::testing::Test {
public:
  ObExprNotEqualTest();
  virtual ~ObExprNotEqualTest();
  virtual void SetUp();
  virtual void TearDown();

private:
  // disallow copy
  ObExprNotEqualTest(const ObExprNotEqualTest& other);
  ObExprNotEqualTest& operator=(const ObExprNotEqualTest& other);

protected:
  // data members
};

ObExprNotEqualTest::ObExprNotEqualTest()
{}

ObExprNotEqualTest::~ObExprNotEqualTest()
{}

void ObExprNotEqualTest::SetUp()
{}

void ObExprNotEqualTest::TearDown()
{}

#define T(t1, v1, t2, v2, res) COMPARE_EXPECT(ObExprNotEqual, &buf, calc_result2, t1, v1, t2, v2, res)
#define T_BIN(t1, v1, t2, v2, res) COMPARE_EXPECT_BIN(ObExprNotEqual, &buf, calc_result2, t1, v1, t2, v2, res)
#define T_GEN(t1, v1, t2, v2, res) COMPARE_EXPECT_GEN(ObExprNotEqual, &buf, calc_result2, t1, v1, t2, v2, res)

TEST_F(ObExprNotEqualTest, collation_test)
{
  ObMalloc buf;
  T_BIN(varchar, "ß", varchar, "s", MY_TRUE);
  T_GEN(varchar, "ß", varchar, "s", MY_FALSE);
}

int main(int argc, char** argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
