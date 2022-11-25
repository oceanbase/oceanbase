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
#include <stdint.h>
#include "lib/ob_define.h"
#include "lib/time/ob_time_utility.h"

using namespace oceanbase;
using namespace oceanbase::common;

class ObTimeUtilityTest: public ::testing::Test
{
public:
  ObTimeUtilityTest();
  virtual ~ObTimeUtilityTest();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  ObTimeUtilityTest(const ObTimeUtilityTest &other);
  ObTimeUtilityTest& operator=(const ObTimeUtilityTest &other);
protected:
  // data members
};

ObTimeUtilityTest::ObTimeUtilityTest()
{
}

ObTimeUtilityTest::~ObTimeUtilityTest()
{
}

void ObTimeUtilityTest::SetUp()
{
}

void ObTimeUtilityTest::TearDown()
{
}

TEST(ObTimeUtilityTest, str_to_date_test)
{
  struct tm t;
  int64_t usec = 0;
  const char *date_ptr = "1970-02-03 07:08:09.12";
  const char *format_ptr = "%Y-%m-%d %k:%i:%S.%f";
  ASSERT_EQ(OB_SUCCESS, ObTimeUtility::str_format_to_timestamp(ObString::make_string(date_ptr),
    ObString::make_string(format_ptr), t, usec));
  printf("date: %04d-%02d-%02d %02d:%02d:%02d.%06ld\n",
    t.tm_year + 1900, t.tm_mon + 1, t.tm_mday, t.tm_hour, t.tm_min, t.tm_sec, usec);
  date_ptr = "13/02/17";
  format_ptr = "%y/%m/%d";
  ASSERT_EQ(OB_NOT_SUPPORTED, ObTimeUtility::str_format_to_timestamp(ObString::make_string(date_ptr),
    ObString::make_string(format_ptr), t, usec));
//   ASSERT_EQ(2013, t.tm_year + 1900);
//   ASSERT_EQ(2, t.tm_mon + 1);
//   ASSERT_EQ(17, t.tm_mday);

  date_ptr = "1970-01-01 08:00:01";
  format_ptr = "%Y-%m-%d %k:%i:%S";
  ASSERT_EQ(OB_SUCCESS, ObTimeUtility::str_format_to_usec(ObString::make_string(date_ptr),
    ObString::make_string(format_ptr), usec));
  ASSERT_EQ(1000L * 1000L, usec);
}

TEST(ObTimeUtilityTest, str_to_timestamp_test)
{
  struct tm t;
  int64_t usec = 0;
  const char *date_ptr = "1970-02-03 07:08:09.12";
  ASSERT_EQ(OB_SUCCESS, ObTimeUtility::str_to_timestamp(ObString::make_string(date_ptr), t, usec));
  printf("date: %04d-%02d-%02d %02d:%02d:%02d.%06ld\n",
    t.tm_year + 1900, t.tm_mon + 1, t.tm_mday, t.tm_hour, t.tm_min, t.tm_sec, usec);
  date_ptr = "1970^^02***03&&&07:08:09->12";
  ASSERT_EQ(OB_SUCCESS, ObTimeUtility::str_to_timestamp(ObString::make_string(date_ptr), t, usec));
  printf("date: %04d-%02d-%02d %02d:%02d:%02d.%06ld\n",
    t.tm_year + 1900, t.tm_mon + 1, t.tm_mday, t.tm_hour, t.tm_min, t.tm_sec, usec);
}

TEST(ObTimeUtilityTest, str_dateunit_to_timestamp_test)
{
  int64_t usec = 0;
  const char *str = "12";
  struct tm t;

  ASSERT_EQ(OB_SUCCESS, ObTimeUtility::str_dateunit_to_timestamp(ObString::make_string(str),
    DATE_UNIT_MICROSECOND, t, usec));
  ASSERT_EQ(12, usec);

  str = "1";
  ASSERT_EQ(OB_SUCCESS, ObTimeUtility::str_dateunit_to_timestamp(ObString::make_string(str),
    DATE_UNIT_DAY, t, usec));
  ASSERT_EQ(1 * 1000L * 1000L * 60L * 60L * 24L, usec);

  str = "21-25";
  ASSERT_EQ(OB_SUCCESS, ObTimeUtility::str_dateunit_to_timestamp(ObString::make_string(str),
    DATE_UNIT_YEAR_MONTH, t, usec));
  ASSERT_EQ(21, t.tm_year);
  ASSERT_EQ(25, t.tm_mon);
}

TEST(ObTimeUtilityTest, date_format_test)
{
  const char *date = "2013-01-01 22:23:00";
  const char *format = "%U %V %X";
  char buf[1024] = {'\0'};
  int64_t pos = 0;
  int64_t usec = 0;

  ASSERT_EQ(OB_SUCCESS, ObTimeUtility::str_to_usec(ObString::make_string(date), usec));
  ASSERT_EQ(OB_SUCCESS, ObTimeUtility::usec_format_to_str(usec, ObString::make_string(format), buf, 1024, pos));
  printf("buf: %s\n", buf);
  ASSERT_STREQ("00 53 2012", buf);
  format = "%c %d %e %f %H %h %I %i %j %k %l %m %p %r %S %s %T %U %u %V %v %w %X %x %Y %y %%";
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, ObTimeUtility::usec_format_to_str(usec, ObString::make_string(format), buf, 1024, pos));
  buf[pos] = '\0';
  printf("date: %s, buf: %s\n", date, buf);
  ASSERT_STREQ("1 01 1 000000 22 10 10 23 001 22 10 01 PM 10:23:00 PM 00 00 22:23:00 00 01 53 01 2 2012 2013 2013 13 %", buf);
}

#define T_WEEK(today, wday, flag_mask, result) \
  { \
    struct tm t; \
    t.tm_yday = today - 1; \
    t.tm_wday = wday; \
    ASSERT_EQ(result, ObTimeUtility::get_weeks_of_year(t, flag_mask)); \
  }

#define T_WEEK_CHECK_YEAR(today, wday, year, flag_mask, week_count, r_year) \
  { \
    struct tm t; \
    t.tm_year = year - 1900; \
    t.tm_yday = today - 1; \
    t.tm_wday = wday; \
    ASSERT_EQ(week_count, ObTimeUtility::get_weeks_of_year(t, flag_mask)); \
    ASSERT_EQ(r_year, t.tm_year + 1900); \
  }

TEST(ObTimeUtilityTest, get_weeks_of_year_test)
{
  //'%U'
  T_WEEK(1, 0, START_WITH_SUNDAY | WEEK_FIRST_WEEKDAY, 1);
  T_WEEK(1, 3, START_WITH_SUNDAY | WEEK_FIRST_WEEKDAY, 0);
  T_WEEK(8, 4, START_WITH_SUNDAY | WEEK_FIRST_WEEKDAY, 1);
  T_WEEK(32, 6, START_WITH_SUNDAY | WEEK_FIRST_WEEKDAY, 4);
  //'%u'
  T_WEEK(1, 0, 0, 0);
  T_WEEK(1, 1, 0, 1);
  T_WEEK(8, 4, 0, 2);
  T_WEEK(32, 6, 0, 5);
  T_WEEK(1, 3, 0, 1);
  //'%V'
  T_WEEK_CHECK_YEAR(1, 0, 2014, START_WITH_SUNDAY | WEEK_FIRST_WEEKDAY | INCLUDE_CRITICAL_WEEK, 1, 2014);
  T_WEEK_CHECK_YEAR(1, 3, 2014, START_WITH_SUNDAY | WEEK_FIRST_WEEKDAY | INCLUDE_CRITICAL_WEEK, 52, 2013);
  T_WEEK_CHECK_YEAR(32, 6, 2014, START_WITH_SUNDAY | WEEK_FIRST_WEEKDAY | INCLUDE_CRITICAL_WEEK, 4, 2014);
  T_WEEK_CHECK_YEAR(365, 2, 2013, START_WITH_SUNDAY | WEEK_FIRST_WEEKDAY | INCLUDE_CRITICAL_WEEK, 52, 2013);
  //'%v'
  T_WEEK_CHECK_YEAR(1, 3, 2014, INCLUDE_CRITICAL_WEEK, 1, 2014);
  T_WEEK_CHECK_YEAR(365, 2, 2013, INCLUDE_CRITICAL_WEEK, 1, 2014);
  T_WEEK_CHECK_YEAR(1, 0, 2012, INCLUDE_CRITICAL_WEEK, 52, 2011);
  T_WEEK_CHECK_YEAR(32, 6, 2014, INCLUDE_CRITICAL_WEEK, 5, 2014);
  T_WEEK_CHECK_YEAR(365, 0, 2023, INCLUDE_CRITICAL_WEEK, 52, 2023);
}

TEST(ObTimeUtilityTest, extract_usec_test1)
{
  int64_t usec = 0;
  const char *str = "12";

  ASSERT_EQ(OB_SUCCESS, ObTimeUtility::extract_usec(ObString::make_string(str), usec));
  ASSERT_EQ(120000, usec);
}

TEST(ObTimeUtilityTest, extract_usec_test2)
{
  int64_t pos = 0;
  int64_t usec = 0;
  ASSERT_EQ(OB_SUCCESS, ObTimeUtility::extract_usec(ObString::make_string("123"), pos, usec, ObTimeUtility::DIGTS_INSENSITIVE));
  ASSERT_EQ(123000, usec);
}

TEST(ObTimeUtilityTest, extract_usec_reverse_test)
{
  int64_t pos = strlen("123") - 1;
  int64_t usec = 0;
  ASSERT_EQ(OB_SUCCESS, ObTimeUtility::extract_usec_reverse(ObString::make_string("123"), pos, usec));
  ASSERT_EQ(123000, usec);
}

TEST(ObTimeUtilityTest, extract_date_test)
{
  int64_t pos = 0;
  int64_t date = 0;
  ASSERT_EQ(OB_SUCCESS, ObTimeUtility::extract_date(ObString::make_string("123"), 0, pos, date));
  ASSERT_EQ(123, date);
}

TEST(ObTimeUtilityTest, extract_date_reverse_test)
{
  int64_t pos = strlen("123") - 1;
  int64_t date = 0;
  ASSERT_EQ(OB_SUCCESS, ObTimeUtility::extract_date_reverse(ObString::make_string("123"), 0, pos, date));
  ASSERT_EQ(123, date);
}

TEST(ObTimeUtilityTest, is_valid_date_test)
{
  ASSERT_TRUE(ObTimeUtility::is_valid_date(2013, 12, 23));
  ASSERT_FALSE(ObTimeUtility::is_valid_date(2013, 2, 29));
  ASSERT_TRUE(ObTimeUtility::is_valid_date(2012, 2, 29));
  ASSERT_FALSE(ObTimeUtility::is_valid_date(1900, 2, 29));
  ASSERT_FALSE(ObTimeUtility::is_valid_date(2013, 1, 32));
  ASSERT_TRUE(ObTimeUtility::is_valid_date(2013, 3, 31));
  ASSERT_TRUE(ObTimeUtility::is_valid_date(2013, 5, 31));
  ASSERT_TRUE(ObTimeUtility::is_valid_date(2013, 7, 31));
  ASSERT_TRUE(ObTimeUtility::is_valid_date(2013, 8, 31));
  ASSERT_TRUE(ObTimeUtility::is_valid_date(2013, 10, 31));
  ASSERT_TRUE(ObTimeUtility::is_valid_date(2013, 12, 31));
  ASSERT_FALSE(ObTimeUtility::is_valid_date(2013, 4, 31));
  ASSERT_FALSE(ObTimeUtility::is_valid_date(2013, 6, 31));
  ASSERT_FALSE(ObTimeUtility::is_valid_date(2013, 9, 31));
  ASSERT_FALSE(ObTimeUtility::is_valid_date(2013, 11, 31));
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
