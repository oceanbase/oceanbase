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
#include "sql/engine/expr/ob_expr_substr.h"
#include "ob_expr_test_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

class ObExprSubstrTest : public ::testing::Test
{
public:
  ObExprSubstrTest();
  virtual ~ObExprSubstrTest();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  ObExprSubstrTest(const ObExprSubstrTest &other);
  ObExprSubstrTest& operator=(const ObExprSubstrTest &other);
private:
  // data members
};
ObExprSubstrTest::ObExprSubstrTest()
{
}

ObExprSubstrTest::~ObExprSubstrTest()
{
}

void ObExprSubstrTest::SetUp()
{
}

void ObExprSubstrTest::TearDown()
{
}

#define T(obj, t1, v1, t2, v2, t3, v3, ref_type, ref_value) EXPECT_RESULT3(obj, &buf, calc_result3, t1, v1, t2, v2, t3, v3, ref_type, ref_value)
#define F(obj, t1, v1, t2, v2, t3, v3, ref_type, ref_value) EXPECT_FAIL_RESULT3(obj, &buf, calc_result3, t1, v1, t2, v2, t3, v3, ref_type, ref_value)

TEST_F(ObExprSubstrTest, basic_test)
{
  ObArenaAllocator alloc;
  ObExprSubstr substr(alloc);
  ObExprStringBuf buf;

  ASSERT_EQ(ObExprOperator::TWO_OR_THREE, substr.get_param_num());

  // null
  T(substr, null,  , null,  , null,  , null,  );
  T(substr, int,1, null,  , null,  , null,  );
  T(substr, int,1, varchar, "hi", null,  , null,  );
  T(substr, int,1, null,  , varchar, "hi", null,  );
  //T(substr, int,1, min, 0, null,  , null,  );
  //T(substr, int,1, max, 0, null,  , null,  );
  //T(substr, int,1, null,  , max, 0, null,  );
  //T(substr, int,1, null,  , min, 0, null,  );
  //T(substr, null,  , null,  , min, 0, null,  );

  // normal
  T(substr, varchar, "hello", int,0, int,100, varchar, "");
  T(substr, varchar, "hello", int,1, int,100, varchar, "hello");
  T(substr, varchar, "hello", int,2, int,100, varchar, "ello");
  T(substr, varchar, "hello", int,3, int,100, varchar, "llo");
  T(substr, varchar, "hello", int,4, int,100, varchar, "lo");
  T(substr, varchar, "hello", int,5, int,100, varchar, "o");
  T(substr, varchar, "hello", int,6, int,100, varchar, "");
  T(substr, varchar, "hello", int,7, int,100, varchar, "");

  T(substr, varchar, "hello", int,-1, int,100, varchar, "o");
  T(substr, varchar, "hello", int,-2, int,100, varchar, "lo");
  T(substr, varchar, "hello", int,-3, int,100, varchar, "llo");
  T(substr, varchar, "hello", int,-4, int,100, varchar, "ello");
  T(substr, varchar, "hello", int,-5, int,100, varchar, "hello");
  T(substr, varchar, "hello", int,-6, int,100, varchar, "");
  T(substr, varchar, "hello", int,-7, int,100, varchar, "");
  T(substr, varchar, "hello", int,-6, int,1, varchar, "");
  T(substr, varchar, "hello", int,-6, int,2, varchar, "");
  T(substr, varchar, "hello", int,-6, int,3, varchar, "");
  T(substr, varchar, "hello", int,-6, int,4, varchar, "");
  T(substr, varchar, "hello", int,-6, int,5, varchar, "");
  T(substr, varchar, "hello", int,-6, int,6, varchar, "");
  T(substr, varchar, "hello", int,-6, int,7, varchar, "");
  T(substr, varchar, "hello", int,-1, int,2, varchar, "o");
  T(substr, varchar, "hello", int,-2, int,2, varchar, "lo");
  T(substr, varchar, "hello", int,-3, int,2, varchar, "ll");
  T(substr, varchar, "hello", int,-4, int,2, varchar, "el");
  T(substr, varchar, "hello", int,-5, int,2, varchar, "he");

  T(substr, varchar, "hello", int,0, int,-1, varchar, "");
  T(substr, varchar, "hello", int,1, int,-1, varchar, "");
  T(substr, varchar, "hello", int,2, int,-1, varchar, "");
  T(substr, varchar, "hello", int,3, int,-1, varchar, "");
  T(substr, varchar, "hello", int,4, int,-1, varchar, "");
  T(substr, varchar, "hello", int,5, int,-1, varchar, "");
  T(substr, varchar, "hello", int,6, int,-1, varchar, "");

  T(substr, varchar, "hello", int,0, int,0, varchar, "");
  T(substr, varchar, "hello", int,1, int,0, varchar, "");
  T(substr, varchar, "hello", int,2, int,0, varchar, "");
  T(substr, varchar, "hello", int,3, int,0, varchar, "");
  T(substr, varchar, "hello", int,4, int,0, varchar, "");
  T(substr, varchar, "hello", int,5, int,0, varchar, "");
  T(substr, varchar, "hello", int,6, int,0, varchar, "");


}

TEST_F(ObExprSubstrTest, fail_test)
{
  ObArenaAllocator alloc;
  ObExprSubstr substr(alloc);
  ObExprStringBuf buf;

  ASSERT_EQ(ObExprOperator::TWO_OR_THREE, substr.get_param_num());
  F(substr, varchar, "hi", int,1, max_value, , varchar, "hi");
  F(substr, varchar, "hi", int,1, min_value,  , varchar, "hi");
  F(substr, varchar, "hi", min_value,  , int,1, varchar, "hi");
  F(substr, varchar, "hi", max_value,  , int,1, varchar, "hi");
  F(substr, int,1, min_value,  , min_value,  , varchar, "1");
  F(substr, double, 1.3, min_value,  , min_value,  , varchar, "1.300000");

  // convert
  //F(substr, varchar, "hello", varchar, "3", varchar, "100", varchar, "llo");
  //F(substr, varchar, "hello", varchar, "4", varchar, "100", varchar, "lo");
  //F(substr, varchar, "hello", varchar, "5", varchar, "100", varchar, "o");
  //F(substr, varchar, "hello", varchar, "6", varchar, "100", varchar, "");
  //F(substr, varchar, "hello", varchar, "7", varchar, "100", varchar, "");
  //F(substr, varchar, "hello", varchar, "-1", varchar, "100", varchar, "o");
  //F(substr, varchar, "hello", varchar, "-2", varchar, "100", varchar, "lo");
  //F(substr, varchar, "hello", varchar, "-3", varchar, "100", varchar, "llo");
  //F(substr, varchar, "hello", varchar, "-4", varchar, "100", varchar, "ello");

}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

