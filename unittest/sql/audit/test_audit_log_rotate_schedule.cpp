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
#include <ctime>
#include "sql/audit/ob_audit_log_rotate_schedule.h"

namespace oceanbase
{
using namespace common;
using namespace sql;
namespace unittest
{

class TestAuditLogRotateSchedule : public ::testing::Test
{
protected:
  int64_t local_time(int year, int month, int day, int hour, int minute, int second = 0)
  {
    struct tm date = {};
    date.tm_year = year - 1900;
    date.tm_mon = month - 1;
    date.tm_mday = day;
    date.tm_hour = hour;
    date.tm_min = minute;
    date.tm_sec = second;
    date.tm_isdst = -1;
    return mktime(&date) * USECS_PER_SEC;
  }

  void expect_next(const char *rule, int64_t reference, int64_t expected)
  {
    ObAuditLogRotateSchedule schedule;
    ASSERT_EQ(OB_SUCCESS, schedule.parse(ObString(rule))) << rule;
    int64_t next = -1;
    ASSERT_EQ(OB_SUCCESS, schedule.calc_next_time(reference, next)) << rule;
    EXPECT_EQ(expected, next) << rule;
  }
};

TEST_F(TestAuditLogRotateSchedule, disabled_and_reset)
{
  ObAuditLogRotateSchedule schedule;
  int64_t next = -1;
  ASSERT_FALSE(schedule.is_enable());
  ASSERT_EQ(OB_SUCCESS, schedule.calc_next_time(local_time(2026, 10, 9, 0, 0), next));
  EXPECT_EQ(0, next);
  ASSERT_EQ(OB_SUCCESS, schedule.parse(ObString(" \t\r\n")));
  EXPECT_FALSE(schedule.is_enable());
  ASSERT_EQ(OB_SUCCESS, schedule.parse(ObString("\v\f 06:00 \r\n")));
  EXPECT_TRUE(schedule.is_enable());
  ASSERT_EQ(OB_SUCCESS, schedule.parse(ObString("06:00")));
  EXPECT_TRUE(schedule.is_enable());
  schedule.reset();
  EXPECT_FALSE(schedule.is_enable());
}

TEST_F(TestAuditLogRotateSchedule, parsed_rule_equality)
{
  const struct {
    const char *left_;
    const char *right_;
    bool equal_;
  } pairs[] = {
    {"12:30,00:00,12:30", " 00:00 , 12:30 ", true},
    {"00:00/60m", "00:00/1H", true},
    {"00:00/1h", "01:00/1h", false},
    {"06:00/24h", "06:00", false},
    {"01:00/7h", "22:00,01:00,15:00,08:00", false},
    {"06:00/5h", "02:00,06:00,11:00,16:00,21:00", false},
    {"00:00/0008m", "00:00/8M", true},
    {"00:00,12:00", "00:00,13:00", false}
  };
  for (const auto &pair : pairs) {
    ObAuditLogRotateSchedule left;
    ObAuditLogRotateSchedule right;
    ASSERT_EQ(OB_SUCCESS, left.parse(ObString(pair.left_)));
    ASSERT_EQ(OB_SUCCESS, right.parse(ObString(pair.right_)));
    EXPECT_EQ(pair.equal_, left.equal(right)) << pair.left_ << " vs " << pair.right_;
    EXPECT_EQ(pair.equal_, right.equal(left));
  }
  ObAuditLogRotateSchedule left;
  ObAuditLogRotateSchedule right;
  ASSERT_EQ(OB_SUCCESS, left.parse(ObString("01:00/7h")));
  ASSERT_EQ(OB_SUCCESS, right.parse(ObString("01:00/6h")));
  EXPECT_FALSE(left.equal(right));
}

TEST_F(TestAuditLogRotateSchedule, invalid_input)
{
  const char *invalid[] = {
    "24:00", "12:60", "1:00", "01:1", "01:00:00", "01:00x", "01: 00",
    ",00:00", "00:00,", "00:00,,12:00", "00:00, ,12:00",
    "00:00/0m", "00:00/25h", "00:00/1441m", "00:00/1s", "00:00/+1h",
    "00:00/-1h", "00:00/1.5h", "00:00/1 h", "00:00 /1h", "00:00/ 1h",
    "00:00/5m,12:00", "00:00/5m,03:00/5m", "00:00/1h/1m",
    "00:00/999999999999999999999999999m"
  };
  ObAuditLogRotateSchedule schedule;
  ASSERT_EQ(OB_SUCCESS, schedule.parse(ObString("06:00")));
  for (const char *text : invalid) {
    EXPECT_EQ(OB_INVALID_ARGUMENT, schedule.parse(ObString(text))) << text;
  }
  const char embedded_null[] = {'0', '1', ':', '0', '0', '\0', 'x'};
  EXPECT_EQ(OB_INVALID_ARGUMENT, schedule.parse(ObString(sizeof(embedded_null), embedded_null)));
}

TEST_F(TestAuditLogRotateSchedule, strictly_future_points)
{
  const int64_t noon = local_time(2026, 10, 9, 12, 30);
  expect_next("12:30,00:00", noon - 1, noon);
  expect_next("12:30,00:00", noon, local_time(2026, 10, 10, 0, 0));
  expect_next("12:30,00:00", noon + 1, local_time(2026, 10, 10, 0, 0));
  expect_next("00:00/1m", local_time(2026, 10, 9, 12, 30, 59),
              local_time(2026, 10, 9, 12, 31));
  expect_next("00:00,12:30", local_time(2026, 10, 9, 3, 0),
              local_time(2026, 10, 9, 12, 30));
}

TEST_F(TestAuditLogRotateSchedule, daily_interval_reset)
{
  expect_next("01:00/7h", local_time(2026, 10, 9, 21, 59), local_time(2026, 10, 9, 22, 0));
  expect_next("01:00/7h", local_time(2026, 10, 9, 22, 0), local_time(2026, 10, 10, 1, 0));
  expect_next("06:00/5h", local_time(2026, 10, 9, 21, 0), local_time(2026, 10, 10, 2, 0));
  expect_next("06:00/5h", local_time(2026, 10, 9, 1, 0), local_time(2026, 10, 9, 2, 0));
  expect_next("23:30/1h", local_time(2026, 10, 9, 23, 30), local_time(2026, 10, 10, 0, 30));
  expect_next("00:00/7m", local_time(2026, 10, 9, 23, 55), local_time(2026, 10, 10, 0, 0));
  expect_next("06:00/1440m", local_time(2026, 10, 9, 6, 0), local_time(2026, 10, 10, 6, 0));
}

TEST_F(TestAuditLogRotateSchedule, sorted_points_and_copy)
{
  ObAuditLogRotateSchedule points;
  ObAuditLogRotateSchedule expected;
  ObAuditLogRotateSchedule copy;
  ASSERT_EQ(OB_SUCCESS, points.parse(ObString(
      "22:00,20:00,18:00,16:00,14:00,12:00,10:00,08:00,06:00,04:00,02:00,00:00,04:00")));
  ASSERT_EQ(OB_SUCCESS, expected.parse(ObString(
      "00:00,02:00,04:00,06:00,08:00,10:00,12:00,14:00,16:00,18:00,20:00,22:00")));
  EXPECT_TRUE(points.equal(expected));
  ASSERT_EQ(OB_SUCCESS, copy.assign(points));
  EXPECT_TRUE(copy.equal(expected));
  ASSERT_EQ(OB_SUCCESS, points.parse(ObString("06:00/5h")));
  EXPECT_TRUE(copy.equal(expected));
  int64_t next = 0;
  ASSERT_EQ(OB_SUCCESS, copy.calc_next_time(local_time(2026, 10, 10, 10, 0), next));
  EXPECT_EQ(local_time(2026, 10, 10, 12, 0), next);
}

TEST_F(TestAuditLogRotateSchedule, interval_candidates_across_daily_anchor)
{
  expect_next("06:00/7m", local_time(2026, 10, 10, 0, 0), local_time(2026, 10, 10, 0, 5));
  expect_next("06:00/7m", local_time(2026, 10, 10, 5, 55), local_time(2026, 10, 10, 6, 0));
  expect_next("06:00/7m", local_time(2026, 10, 10, 23, 58), local_time(2026, 10, 11, 0, 5));
  expect_next("23:30/1000m", local_time(2026, 10, 10, 0, 0), local_time(2026, 10, 10, 16, 10));
  expect_next("23:30/1000m", local_time(2026, 10, 10, 16, 10), local_time(2026, 10, 10, 23, 30));
  expect_next("23:59/1439m", local_time(2026, 10, 10, 23, 58), local_time(2026, 10, 10, 23, 59));
  expect_next("23:59/1439m", local_time(2026, 10, 10, 23, 59), local_time(2026, 10, 11, 23, 58));
}

TEST_F(TestAuditLogRotateSchedule, calendar_boundaries)
{
  expect_next("00:00", local_time(2026, 12, 31, 23, 59), local_time(2027, 1, 1, 0, 0));
  expect_next("00:00", local_time(2028, 2, 28, 23, 59), local_time(2028, 2, 29, 0, 0));
  expect_next("00:00", local_time(2028, 2, 29, 23, 59), local_time(2028, 3, 1, 0, 0));
}

TEST_F(TestAuditLogRotateSchedule, timestamp_overflow_preserves_output)
{
  ObAuditLogRotateSchedule schedule;
  ASSERT_EQ(OB_SUCCESS, schedule.parse(ObString("00:00")));
  int64_t next = 123;
  EXPECT_EQ(OB_SIZE_OVERFLOW, schedule.calc_next_time(INT64_MAX, next));
  EXPECT_EQ(123, next);
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
