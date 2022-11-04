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
#include "sql/engine/expr/ob_expr_extract.h"
#include "ob_expr_test_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

class ObExprExtractTest : public ::testing::Test
{
public:
  ObExprExtractTest();
  virtual ~ObExprExtractTest();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  ObExprExtractTest(const ObExprExtractTest &other);
  ObExprExtractTest& operator=(const ObExprExtractTest &other);
private:
  // data members
};

ObExprExtractTest::ObExprExtractTest()
{
}

ObExprExtractTest::~ObExprExtractTest()
{
}

void ObExprExtractTest::SetUp()
{
}

void ObExprExtractTest::TearDown()
{
}

#define T(obj, t1, v1, t2, v2, ref_type, ref_value) EXPECT_RESULT2(obj, &buf, calc_result2, t1, v1, t2, v2, ref_type, ref_value)
#define F(obj, t1, v1, t2, v2) EXPECT_FAIL_RESULT2(obj, &buf, calc_result2, t1, v1, t2, v2, int, 0)

TEST_F(ObExprExtractTest, base_test)
{
  ObMalloc buf;
  ObExprExtract extract(buf);

  ASSERT_EQ(2, extract.get_param_num());

 // T(extract, int, DATE_UNIT_MICROSECOND, int, 1, int, 0);
  T(extract, int, DATE_UNIT_MICROSECOND, varchar, "2014-01-04 11:58:58.999", int, 999000);
  T(extract, int, DATE_UNIT_SECOND, varchar, "2014-01-04 11:58:58.999", int, 58);
  T(extract, int, DATE_UNIT_MINUTE, varchar, "2014-01-04 11:58:58.999", int, 58);
  T(extract, int, DATE_UNIT_HOUR, varchar, "2014-01-04 11:58:58.999", int, 11);
  T(extract, int, DATE_UNIT_DAY, varchar, "2014-01-04 11:58:58.999", int, 4);
  T(extract, int, DATE_UNIT_MONTH, varchar, "2014-01-04 11:58:58.999", int, 1);
  T(extract, int, DATE_UNIT_WEEK, varchar, "2014-01-04 11:58:58.999", int, 0);
  T(extract, int, DATE_UNIT_WEEK, varchar, "2014-02-04 11:58:58.999", int, 5);
  T(extract, int, DATE_UNIT_WEEK, varchar, "2014-03-04 11:58:58.999", int, 9);
  T(extract, int, DATE_UNIT_WEEK, varchar, "2014-04-04 11:58:58.999", int, 13);
  T(extract, int, DATE_UNIT_WEEK, varchar, "2014-12-04 11:58:58.999", int, 48);
  T(extract, int, DATE_UNIT_WEEK, varchar, "2014-12-31 11:58:58.999", int, 52);
  T(extract, int, DATE_UNIT_WEEK, varchar, "2000-12-31 11:58:58.999", int, 53);
  T(extract, int, DATE_UNIT_QUARTER, varchar, "2014-01-04 11:58:58.999", int, 1);
  T(extract, int, DATE_UNIT_YEAR, varchar, "2014-01-04 11:58:58.999", int, 2014);
  T(extract, int, DATE_UNIT_MINUTE_SECOND, varchar, "2014-01-04 11:58:58.999", int, 5858);
  T(extract, int, DATE_UNIT_MINUTE_MICROSECOND, varchar, "2014-01-04 11:58:58.999", int, 5858999000);
  T(extract, int, DATE_UNIT_DAY_HOUR, varchar, "2014-01-04 11:58:58.999", int, 411);
}

//TEST_F(ObExprExtractTest, fail_test)
//{
//  ObExprExtract extract;
//  //ObExprStringBuf buf;
//
//  ASSERT_EQ(2, extract.get_param_num());
//
//  /*F(extract, int, DATE_UNIT_MICROSECOND, int, 1);
//  F(extract, varchar, "ddd", int, 1);
//  F(extract, int, 1, double, 2.2);
//  F(extract, double, 1.1111, int, 2);*/
//}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
