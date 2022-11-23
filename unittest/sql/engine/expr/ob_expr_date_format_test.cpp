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
#include "sql/engine/expr/ob_expr_date_format.h"
#include "ob_expr_test_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

class ObExprDateFormatTest : public ::testing::Test
{
public:
  ObExprDateFormatTest();
  virtual ~ObExprDateFormatTest();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  ObExprDateFormatTest(const ObExprDateFormatTest &other);
  ObExprDateFormatTest& operator=(const ObExprDateFormatTest &other);
private:
  // data members
};
ObExprDateFormatTest::ObExprDateFormatTest()
{
}

ObExprDateFormatTest::~ObExprDateFormatTest()
{
}

void ObExprDateFormatTest::SetUp()
{
}

void ObExprDateFormatTest::TearDown()
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

TEST_F(ObExprDateFormatTest, base_test)
{
  ObExprDateFormat dateformat;
  ObExprStringBuf buf;

  ASSERT_EQ(2, dateformat.get_param_num());

  T(dateformat, precise_datetime, get_usec(2013, 9, 10, 0, 0, 0, 0), varchar, "%Y-%m-%d", varchar, "2013-09-10");
  T(dateformat, varchar, "2013-09-10 23:59:59.9999", varchar, "%Y", varchar, "2013");
  T(dateformat, varchar, "2013-09-10 23:59:59.9999", varchar, "%a", varchar, "Tue");
  T(dateformat, varchar, "2013-09-10 23:59:59.9999", varchar, "%b", varchar, "Sep");
  T(dateformat, varchar, "2013-09-10 23:59:59.9999", varchar, "%c", varchar, "9");
  T(dateformat, varchar, "2013-09-10 23:59:59.9999", varchar, "%D", varchar, "10th");
  T(dateformat, varchar, "2013-09-10 23:59:59.9999", varchar, "%d", varchar, "10");
  T(dateformat, varchar, "2013-09-10 23:59:59.9999", varchar, "%e", varchar, "10");
  T(dateformat, varchar, "2013-09-10 23:59:59.9999", varchar, "%f", varchar, "999900");
  T(dateformat, varchar, "2013-09-10 23:59:59.9999", varchar, "%H", varchar, "23");
  T(dateformat, varchar, "2013-09-10 23:59:59.9999", varchar, "%h", varchar, "11");
  T(dateformat, varchar, "2013-09-10 23:59:59.9999", varchar, "%i", varchar, "59");
  T(dateformat, varchar, "2013-09-10 23:59:59.9999", varchar, "%j", varchar, "253");
  T(dateformat, varchar, "2013-09-10 23:59:59.9999", varchar, "%k", varchar, "23");
  T(dateformat, varchar, "2013-09-10 23:59:59.9999", varchar, "%l", varchar, "11");
  T(dateformat, varchar, "2013-09-10 23:59:59.9999", varchar, "%M", varchar, "September");
  T(dateformat, varchar, "2013-09-10 23:59:59.9999", varchar, "%p", varchar, "PM");
  T(dateformat, varchar, "2013-09-10 12:59:59.9999", varchar, "%p", varchar, "PM");
  T(dateformat, varchar, "2013-09-10 00:59:59.9999", varchar, "%p", varchar, "AM");
  T(dateformat, varchar, "2013-09-10 23:59:59.9999", varchar, "%r", varchar, "11:59:59 PM");
  T(dateformat, varchar, "2013-09-10 23:59:59.9999", varchar, "%S", varchar, "59");
  T(dateformat, varchar, "2013-09-10 23:59:59.9999", varchar, "%T", varchar, "23:59:59");
  T(dateformat, varchar, "2013-09-10 23:59:59.9999", varchar, "%W", varchar, "Tuesday");
  T(dateformat, varchar, "2013-09-10 23:59:59.9999", varchar, "%w", varchar, "2");
  T(dateformat, varchar, "2013-09-10 23:59:59.9999", varchar, "%y", varchar, "13");
}

TEST_F(ObExprDateFormatTest, fail_test)
{
  ObExprDateFormat dateformat;
  ObExprStringBuf buf;

  ASSERT_EQ(2, dateformat.get_param_num());

  F(dateformat, int, 1, int, 0, varchar, "");
  //F(dateformat, int, 1, null, 0, varchar, "");
  F(dateformat, int, get_usec(2013, 9, 10, 0, 0, 0, 0), varchar, "%Y-%m-%d", varchar, "");
  F(dateformat, double, 1.1, varchar, "%Y-%m-%d", varchar, "");
  F(dateformat, float, 1.1f, varchar, "%Y-%m-%d", varchar, "");
  F(dateformat, varchar, "2013-09-10 23:59:59.9999", double, 0.0, varchar, "");
  F(dateformat, varchar, "2013-09-10 23:59:59.9999", varchar, "%qqq", varchar, "");
  F(dateformat, varchar, "2013-09-10 23:59:59.9999", varchar, "", varchar, "");
}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
