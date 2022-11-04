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
#include "sql/engine/expr/ob_expr_sign.h"
#include "ob_expr_test_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

class ObExprSignTest: public ::testing::Test
{
public:
  ObExprSignTest();
  virtual ~ObExprSignTest();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  ObExprSignTest(const ObExprSignTest &other);
  ObExprSignTest& operator=(const ObExprSignTest &other);
private:
  // data members
};
ObExprSignTest::ObExprSignTest() {}

ObExprSignTest::~ObExprSignTest() {}

void ObExprSignTest::SetUp() {}

void ObExprSignTest::TearDown() {}

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

TEST_F(ObExprSignTest, basic_test)
{
  ObExprSign sign;
  DefaultPageAllocator buf;
  TestAllocator str_buf;
  ASSERT_EQ(1, sign.get_param_num());

  //null
  TN(sign);
  //test
//  T(sign, varchar, "123", int32, 1);
//  T(sign, varchar, "-123", int32, -1);
//  T(sign, varchar, "0", int32, 0);
//  T(sign, int, 123, int32, 1);
//  T(sign, int, -123, int32, -1);
//  T(sign, int, 0, int32, 0);
  T(sign, bool, true, int32, 1);
  T(sign, bool , false, int32, 0);
  T(sign, double, 123.3, int32, 1);
  T(sign, double, -123.3, int32, -1);
  T(sign, double, 0, int32, 0);
  T(sign, varchar, "999999999999999999999999999", int32, 1);
  T(sign, varchar, "-999999999999999999999999999", int32, -1);
}

TEST_F(ObExprSignTest, fail_test)
{
  ObExprSign sign;
  DefaultPageAllocator buf;
  TestAllocator str_buf;
  ASSERT_EQ(1, sign.get_param_num());

  //OB_NOT_INIT
  TNI(sign, int, 123);
}
int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
