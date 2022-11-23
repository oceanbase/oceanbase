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
#include "sql/engine/expr/ob_expr_upper.h"
#include "ob_expr_test_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

class ObExprUpperTest: public ::testing::Test
{
public:
  ObExprUpperTest();
  virtual ~ObExprUpperTest();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  ObExprUpperTest(const ObExprUpperTest &other);
  ObExprUpperTest& operator=(const ObExprUpperTest &other);
private:
  // data members
};
ObExprUpperTest::ObExprUpperTest() {}

ObExprUpperTest::~ObExprUpperTest() {}

void ObExprUpperTest::SetUp() {}

void ObExprUpperTest::TearDown() {}

class TestAllocator: public ObIAllocator
{
public:
  TestAllocator() :
      label_(ObModIds::TEST) {}
  virtual ~TestAllocator() {}
  void *alloc(const int64_t sz) {
    UNUSED(sz);
    return NULL;
  }
  void free(void *p) {
    UNUSED(p);
  }
  void freed(const int64_t sz) {
    UNUSED(sz);
  }
  void set_label(const char *label) {
    label_ = label;
  }
  ;
private:
  const char *label_;
};

#define T(obj, t1, v1, ref_type, ref_value) EXPECT_RESULT1(obj, &buf, calc_result1, t1, v1, ref_type, ref_value)
#define F(obj, t1, v1) EXPECT_FAIL_RESULT1(obj, &buf, calc_result1, t1, v1)
#define TN(obj) EXPECT_NULL1(obj, &buf, calc_result1)
#define TNI(obj, t1, v1) EXPECT_BUF_NOT_INIT1(obj, &buf, calc_result1, t1, v1)
#define TAMF(obj, t1, v1) EXPECT_ALLOCATE_MEMORY_FAILED1(obj, &str_buf, calc_result1, t1, v1)
TEST_F(ObExprUpperTest, basic_test)
{
  ObExprUpper upper;
  DefaultPageAllocator buf;
  ASSERT_EQ(1, upper.get_param_num());

  // null
  TN(upper);
  // test
  T(upper, varchar, "ABC", varchar, "ABC");
  T(upper, varchar, "123", varchar, "123");
  T(upper, varchar, "", varchar, "");
  T(upper, varchar, "abssd", varchar, "ABSSD");
  T(upper, varchar, " aBcD 09 %d#$", varchar, " ABCD 09 %D#$");
}

TEST_F(ObExprUpperTest, fail_test)
{
  ObExprUpper upper;
  DefaultPageAllocator buf;
  TestAllocator str_buf;
  ASSERT_EQ(1, upper.get_param_num());

  F(upper, int, 10);
  F(upper, int, 0);
  F(upper, double, 10.2);
  F(upper, bool, true);
  F(upper, bool, false);
  F(upper, int, 123);
  //OB_NOT_INIT
  TNI(upper, int, 123);
  //OB_ALLOCATE_MEMORY_FAILED
  TAMF(upper, varchar, "123");
}
int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

