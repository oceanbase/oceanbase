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
#include "lib/time/ob_time_utility.h"
#include "sql/engine/expr/ob_expr_str_to_date.h"
#include "ob_expr_test_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

class ObExprStrtodateTest : public ::testing::Test
{
public:
  ObExprStrtodateTest();
  virtual ~ObExprStrtodateTest();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  ObExprStrtodateTest(const ObExprStrtodateTest &other);
  ObExprStrtodateTest& operator=(const ObExprStrtodateTest &other);
private:
  // data members
};
ObExprStrtodateTest::ObExprStrtodateTest()
{
}

ObExprStrtodateTest::~ObExprStrtodateTest()
{
}

void ObExprStrtodateTest::SetUp()
{
}

void ObExprStrtodateTest::TearDown()
{
}

int64_t get_usec(int year, int month, int day, int hour, int minute, int sec, int64_t usec)
{
  int64_t ret = 0;
  struct tm t;
  memset(&t, 0, sizeof(struct tm));
  t.tm_year = year - 1900;
  t.tm_mon = month - 1;
  t.tm_mday = day;
  t.tm_hour = hour;
  t.tm_min = minute;
  t.tm_sec = sec;
  ObTimeUtility::timestamp_to_usec(t, usec, ret);
  return ret;
}
#define T(obj, t1, v1, t2, v2, ref_type, ref_value) EXPECT_RESULT2(obj, &buf, calc_result2, t1, v1, t2, v2, ref_type, ref_value)
#define F(obj, t1, v1, t2, v2, ref_type, ref_value) EXPECT_FAIL_RESULT2(obj, &buf, calc_result2, t1, v1, t2, v2, ref_type, ref_value)

TEST_F(ObExprStrtodateTest, base_test)
{
  ObExprStrToDate strtodate;
  ObExprStringBuf buf;

  ASSERT_EQ(2, strtodate.get_param_num());

  T(strtodate, varchar, "2013-01-02", varchar, "%Y-%m-%d", precise_datetime, get_usec(2013, 01, 02, 0, 0, 0, 0));
  T(strtodate, varchar, "2013-Jan-3rd", varchar, "%Y-%b-%D", precise_datetime, get_usec(2013, 01, 03, 0, 0, 0, 0));
  T(strtodate, varchar, "2013-01-02 12:01:01 AM", varchar, "%Y-%m-%d %r", precise_datetime, get_usec(2013, 01, 02, 0, 1, 1, 0));
  T(strtodate, varchar, "2013-01-02 23:01:01", varchar, "%Y-%m-%d %T", precise_datetime, get_usec(2013, 1, 2, 23, 1, 1, 0));
  T(strtodate, varchar, "2013-12-31 23:59:59.999999", varchar, "%Y-%m-%d %H:%i:%S.%f", precise_datetime, get_usec(2013, 12, 31, 23, 59, 59, 999999));
  T(strtodate, varchar, "2013-12-31 11:59:59.999999 PM", varchar, "%Y-%m-%d %h:%i:%S.%f %p", precise_datetime, get_usec(2013, 12, 31, 23, 59, 59, 999999));
  T(strtodate, varchar, "January/2014/23rd", varchar, "%M/%Y/%D", precise_datetime, get_usec(2014, 1, 23, 0, 0, 0, 0));
}

TEST_F(ObExprStrtodateTest, fail_test)
{
  ObExprStrToDate strtodate;
  ObExprStringBuf buf;

  ASSERT_EQ(2, strtodate.get_param_num());
  F(strtodate, int, 1000, int, 1000, precise_datetime, 0);
  //F(strtodate, null, 0, null, 0, precise_datetime, 0);
  F(strtodate, double, 0, varchar, "%Y-%m-%d", precise_datetime, 0);
  F(strtodate, varchar, "2013-01-02", varchar, "%Y/%m/%d", precise_datetime, 0);
  F(strtodate, varchar, "-9999-01-02", varchar, "%Y-%m-%d", precise_datetime, 0);
  F(strtodate, varchar, "2013-13-02", varchar, "%Y-%m-%d", precise_datetime, 0);
  F(strtodate, varchar, "2013-01-32", varchar, "%Y-%m-%d", precise_datetime, 0);
  F(strtodate, varchar, "2013-02-29", varchar, "%Y-%m-%d", precise_datetime, 0);
  F(strtodate, varchar, "2013-12-31 24:59:59.999999", varchar, "%Y-%m-%d %H:%i:%S.%f", precise_datetime, 0);
  F(strtodate, varchar, "2013-12-31 23:61:59.999999", varchar, "%Y-%m-%d %H:%i:%S.%f", precise_datetime, 0);
  F(strtodate, varchar, "2013-12-31 23:59:61.999999", varchar, "%Y-%m-%d %H:%i:%S.%f", precise_datetime, 0);
  F(strtodate, varchar, "2013-12-31 23:59:59.999999", int, 100, precise_datetime, 0);
}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
