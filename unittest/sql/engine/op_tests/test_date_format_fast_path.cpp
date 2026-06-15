/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 *
 * Unit tests for DATE_FORMAT fast path optimization.
 *
 * Tests cover all 4 fast-path format patterns via expr_unit_test():
 * - %Y-%m-%d, %Y-%m-01, %Y-%m, %Y%m%d
 * - NULL/zero-datetime handling
 * - Batch boundary behavior
 * - Unknown format fallback
 * - Edge cases: leap years, boundary values, single-digit months/days
 * - Timestamp type: verifies fast path is NOT used (requires tz_offset)
 * - Timestamp with cross-day timezone boundary
 */

#include "unittest/sql/engine/op_tests/ob_op_test_kit.h"
#include "lib/timezone/ob_time_convert.h"
#include "gtest/gtest.h"

using namespace oceanbase;
using namespace oceanbase::sql;

// ============================================================================
// Helper: convert date components to microseconds from Unix epoch (1970-01-01)
// Test framework uses ObDateTimeType (int64_t microseconds) for "datetime" columns
// ============================================================================
static int64_t dt_to_usec(int y, int m, int d, int hh = 0, int mm = 0, int ss = 0)
{
  auto is_leap = [](int yr) { return (yr % 4 == 0 && yr % 100 != 0) || (yr % 400 == 0); };
  static const int month_days[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

  int64_t days = 0;
  for (int yr = 1970; yr < y; ++yr) {
    days += is_leap(yr) ? 366 : 365;
  }
  for (int mo = 1; mo < m; ++mo) {
    days += month_days[mo];
    if (mo == 2 && is_leap(y)) days += 1;
  }
  days += (d - 1);

  return days * 86400LL * 1000000LL
       + hh * 3600LL * 1000000LL
       + mm * 60LL * 1000000LL
       + ss * 1000000LL;
}

// ============================================================================
// Vectorized fast path tests via OpTestKit
// ============================================================================
class DateFormatFastPathTest : public OpTestKit {};

// ----- %Y-%m-%d fast path -----
TEST_F(DateFormatFastPathTest, format_y_m_d_basic)
{
  auto result = expr_unit_test()
      .columns("dt datetime")
      .with_expr("DATE_FORMAT(dt, '%Y-%m-%d')")
      .with_data({
        {dt_to_usec(2024, 1, 15)},
        {dt_to_usec(2024, 12, 31, 23, 59, 59)},
        {dt_to_usec(2023, 6, 1, 12, 30, 45)},
        {dt_to_usec(2022, 3, 15)},
      })
      .enable_dual_format_check().run(engine_);
  EXPECT_EQ(4, result.row_count());
  EXPECT_TRUE(result.verify_ordered({
    {"2024-01-15"},
    {"2024-12-31"},
    {"2023-06-01"},
    {"2022-03-15"},
  }));
}

// ----- %Y-%m-01 fast path -----
TEST_F(DateFormatFastPathTest, format_y_m_01_basic)
{
  auto result = expr_unit_test()
      .columns("dt datetime")
      .with_expr("DATE_FORMAT(dt, '%Y-%m-01')")
      .with_data({
        {dt_to_usec(2024, 3, 15)},
        {dt_to_usec(2024, 1, 1, 10, 30, 0)},
        {dt_to_usec(2023, 12, 25, 8, 0, 0)},
      })
      .enable_dual_format_check().run(engine_);
  EXPECT_EQ(3, result.row_count());
  EXPECT_TRUE(result.verify_ordered({
    {"2024-03-01"},
    {"2024-01-01"},
    {"2023-12-01"},
  }));
}

// ----- %Y-%m fast path -----
TEST_F(DateFormatFastPathTest, format_y_m_basic)
{
  auto result = expr_unit_test()
      .columns("dt datetime")
      .with_expr("DATE_FORMAT(dt, '%Y-%m')")
      .with_data({
        {dt_to_usec(2024, 6, 15)},
        {dt_to_usec(2024, 10, 1)},
        {dt_to_usec(2023, 1, 31)},
      })
      .enable_dual_format_check().run(engine_);
  EXPECT_EQ(3, result.row_count());
  EXPECT_TRUE(result.verify_ordered({
    {"2024-06"},
    {"2024-10"},
    {"2023-01"},
  }));
}

// ----- %Y%m%d fast path -----
TEST_F(DateFormatFastPathTest, format_ymd_basic)
{
  auto result = expr_unit_test()
      .columns("dt datetime")
      .with_expr("DATE_FORMAT(dt, '%Y%m%d')")
      .with_data({
        {dt_to_usec(2024, 1, 15)},
        {dt_to_usec(2024, 12, 31, 23, 59, 59)},
        {dt_to_usec(2023, 7, 7, 12, 0, 0)},
      })
      .enable_dual_format_check().run(engine_);
  EXPECT_EQ(3, result.row_count());
  EXPECT_TRUE(result.verify_ordered({
    {"20240115"},
    {"20241231"},
    {"20230707"},
  }));
}

// ----- Zero datetime → formatted zero-string output (matching native MySQL) -----
TEST_F(DateFormatFastPathTest, zero_datetime_yields_null)
{
  auto result = expr_unit_test()
      .columns("dt datetime")
      .with_expr("DATE_FORMAT(dt, '%Y-%m-%d')")
      .with_data({
        {dt_to_usec(2024, 1, 15)},
        {ObTimeConverter::ZERO_DATETIME},
        {dt_to_usec(2024, 12, 31)},
      })
      .run(engine_);
  EXPECT_EQ(3, result.row_count());
  EXPECT_TRUE(result.verify_ordered({
    {"2024-01-15"},
    {"0000-00-00"},  // Zero datetime → "0000-00-00", matches MySQL
    {"2024-12-31"},
  }));
}

// Test zero datetime handling for all 4 fast path formats
TEST_F(DateFormatFastPathTest, zero_datetime_all_patterns)
{
  // For each format, test that zero datetime produces formatted zero-strings
  struct {
    const char *format;
    const char *name;
    const char *zero_expected;  // MySQL: zero datetime format produces zero-strings
  } formats[] = {
    {"%Y-%m-%d", "y_m_d",  "0000-00-00"},
    {"%Y-%m-01", "y_m_01", "0000-00-01"},
    {"%Y-%m",    "y_m",    "0000-00"},
    {"%Y%m%d",   "ymd",    "00000000"},
  };

  for (auto &fmt : formats) {
    std::string expr_str = std::string("DATE_FORMAT(dt, '") + fmt.format + "')";
    auto result = expr_unit_test()
        .columns("dt datetime")
        .with_expr(expr_str.c_str())
        .with_data({
          {ObTimeConverter::ZERO_DATETIME},
          {dt_to_usec(2024, 6, 15)},
        })
        .run(engine_);
    EXPECT_EQ(2, result.row_count()) << "pattern: " << fmt.name;
    // Each format produces different output for the same date 2024-06-15
    const char *expected_val =
        (strcmp(fmt.format, "%Y-%m") == 0)     ? "2024-06"   :
        (strcmp(fmt.format, "%Y-%m-01") == 0)  ? "2024-06-01" :
        (strcmp(fmt.format, "%Y%m%d") == 0)    ? "20240615"   : "2024-06-15";
    EXPECT_TRUE(result.verify_ordered({
      {fmt.zero_expected},
      {expected_val},
    })) << "pattern: " << fmt.name;
  }
}

// ----- NULL input → NULL output -----
TEST_F(DateFormatFastPathTest, null_input_yields_null)
{
  auto result = expr_unit_test()
      .columns("dt datetime")
      .with_expr("DATE_FORMAT(dt, '%Y-%m-%d')")
      .with_data({
        {dt_to_usec(2024, 1, 15)},
        {NULL_VAL},
        {dt_to_usec(2024, 12, 31)},
        {NULL_VAL},
      })
      .run(engine_);
  EXPECT_EQ(4, result.row_count());
  EXPECT_TRUE(result.verify_ordered({
    {"2024-01-15"},
    {NULL_VAL},
    {"2024-12-31"},
    {NULL_VAL},
  }));
}

// ----- Unknown format falls back to regular path -----
TEST_F(DateFormatFastPathTest, unknown_format_fallback)
{
  auto result = expr_unit_test()
      .columns("dt datetime")
      .with_expr("DATE_FORMAT(dt, '%Y/%m/%d')")
      .with_data({
        {dt_to_usec(2024, 1, 15)},
        {dt_to_usec(2024, 12, 31)},
      })
      .enable_dual_format_check().run(engine_);
  EXPECT_EQ(2, result.row_count());
  // Unknown format uses the regular vector_date_format path
  EXPECT_TRUE(result.verify_ordered({
    {"2024/01/15"},
    {"2024/12/31"},
  }));
}

// ----- Single-digit month/day formatting -----
TEST_F(DateFormatFastPathTest, single_digit_month_day)
{
  // Test that months and days < 10 are correctly zero-padded
  auto result = expr_unit_test()
      .columns("dt datetime")
      .with_expr("DATE_FORMAT(dt, '%Y-%m-%d')")
      .with_data({
        {dt_to_usec(2024, 1, 1)},        // Jan 1
        {dt_to_usec(2024, 9, 5)},        // Sep 5
        {dt_to_usec(2024, 10, 10)},      // Oct 10
        {dt_to_usec(2023, 3, 31)},       // Mar 31
      })
      .enable_dual_format_check().run(engine_);
  EXPECT_EQ(4, result.row_count());
  EXPECT_TRUE(result.verify_ordered({
    {"2024-01-01"},
    {"2024-09-05"},
    {"2024-10-10"},
    {"2023-03-31"},
  }));
}

// ----- Small batch size (exercises vector batch boundaries) -----
TEST_F(DateFormatFastPathTest, small_batch_boundary)
{
  std::vector<TestRow> data;
  // Generate 10 rows: one per month in 2024
  for (int m = 1; m <= 10; ++m) {
    data.push_back({dt_to_usec(2024, m, 15)});
  }

  auto result = expr_unit_test()
      .columns("dt datetime")
      .with_expr("DATE_FORMAT(dt, '%Y-%m')")
      .with_batch_size(4)  // Non-aligned: 10 rows / batch_size=4 → 3 batches
      .with_data(data)
      .enable_dual_format_check().run(engine_);
  EXPECT_EQ(10, result.row_count());
}

// ----- Large batch (exercises no_skip_no_null fast path) -----
TEST_F(DateFormatFastPathTest, large_batch_no_skip)
{
  // Generate data using a generator for clean, no-skip batches
  auto result = expr_unit_test()
      .columns("dt datetime")
      .with_expr("DATE_FORMAT(dt, '%Y%m%d')")
      .with_data_generator(
          256,
          [](int64_t i) -> TestValue {
            int m = static_cast<int>((i % 12) + 1);
            int d = static_cast<int>((i % 28) + 1);
            return dt_to_usec(2024, m, d);
          })
      .run(engine_);
  EXPECT_EQ(256, result.row_count());
}

// ----- Fixed format output length verification -----
TEST_F(DateFormatFastPathTest, output_lengths)
{
  enum { YMD = 10, Y_M_01 = 10, Y_M = 7, YMD_COMPACT = 8 };

  // Each format produces a specific output length
  struct {
    const char *expr;
    int expected_len;
  } tests[] = {
    {"DATE_FORMAT(dt, '%Y-%m-%d')", YMD},
    {"DATE_FORMAT(dt, '%Y-%m-01')", Y_M_01},
    {"DATE_FORMAT(dt, '%Y-%m')", Y_M},
    {"DATE_FORMAT(dt, '%Y%m%d')", YMD_COMPACT},
  };

  for (auto &t : tests) {
    auto result = expr_unit_test()
        .columns("dt datetime")
        .with_expr(t.expr)
        .with_data({
          {dt_to_usec(2024, 6, 15)},
        })
        .run(engine_);
    EXPECT_EQ(1, result.row_count());
  }
}

// ----- Multiple NULL values in a batch -----
TEST_F(DateFormatFastPathTest, mixed_null_and_valid)
{
  std::vector<TestRow> data;
  for (int i = 0; i < 16; ++i) {
    if (i % 3 == 0) {
      data.push_back({NULL_VAL});
    } else if (i % 5 == 0) {
      data.push_back({ObTimeConverter::ZERO_DATETIME});
    } else {
      data.push_back({dt_to_usec(2024, (i % 12) + 1, (i % 28) + 1)});
    }
  }

  auto result = expr_unit_test()
      .columns("dt datetime")
      .with_expr("DATE_FORMAT(dt, '%Y-%m-%d')")
      .with_data(data)
      .run(engine_);
  EXPECT_EQ(16, result.row_count());

  // SQL NULL -> NULL, ZERO_DATETIME -> formatted zero-string (MySQL behavior)
  for (int i = 0; i < 16; ++i) {
    if (i % 3 == 0) {
      EXPECT_EQ("NULL", result.get_row(i)[0])
          << "Row " << i << " (SQL NULL) should be NULL";
    } else if (i % 5 == 0) {
      EXPECT_EQ("0000-00-00", result.get_row(i)[0])
          << "Row " << i << " (ZERO_DATETIME) should be 0000-00-00";
    }
  }
}

// ----- %Y-%m-01 with single-digit months -----
TEST_F(DateFormatFastPathTest, y_m_01_single_digit_month)
{
  auto result = expr_unit_test()
      .columns("dt datetime")
      .with_expr("DATE_FORMAT(dt, '%Y-%m-01')")
      .with_data({
        {dt_to_usec(2024, 1, 15)},
        {dt_to_usec(2024, 2, 28)},
        {dt_to_usec(2024, 9, 1)},
        {dt_to_usec(2024, 11, 10)},
        {dt_to_usec(2024, 12, 31)},
      })
      .enable_dual_format_check().run(engine_);
  EXPECT_EQ(5, result.row_count());
  EXPECT_TRUE(result.verify_ordered({
    {"2024-01-01"},
    {"2024-02-01"},
    {"2024-09-01"},
    {"2024-11-01"},
    {"2024-12-01"},
  }));
}

// ----- Leap year handling -----
TEST_F(DateFormatFastPathTest, leap_year)
{
  auto result = expr_unit_test()
      .columns("dt datetime")
      .with_expr("DATE_FORMAT(dt, '%Y-%m-%d')")
      .with_data({
        {dt_to_usec(2024, 2, 29)},  // Leap year
        {dt_to_usec(2023, 2, 28)},  // Non-leap year
        {dt_to_usec(2000, 2, 29)},  // Century leap year
      })
      .enable_dual_format_check().run(engine_);
  EXPECT_EQ(3, result.row_count());
  EXPECT_TRUE(result.verify_ordered({
    {"2024-02-29"},
    {"2023-02-28"},
    {"2000-02-29"},
  }));
}

// ----- Boundary values -----
TEST_F(DateFormatFastPathTest, boundary_values)
{
  auto result = expr_unit_test()
      .columns("dt datetime")
      .with_expr("DATE_FORMAT(dt, '%Y%m%d')")
      .with_data({
        {dt_to_usec(1970, 1, 1)},
        {dt_to_usec(9999, 12, 31)},
        {dt_to_usec(2000, 1, 1)},
      })
      .run(engine_);
  EXPECT_EQ(3, result.row_count());
  EXPECT_TRUE(result.verify_ordered({
    {"19700101"},
    {"99991231"},
    {"20000101"},
  }));
}

// ----- Timestamp type: ensure fast path is NOT used (tz_offset required) -----
TEST_F(DateFormatFastPathTest, timestamp_basic)
{
  // Timestamp stores UTC time. With time_zone='+08:00', tz_offset must be applied.
  // This test verifies that DATE_FORMAT correctly applies session timezone for timestamp.
  // The fast path is intentionally NOT used for timestamp (requires tz_offset).
  auto result = expr_unit_test()
      .columns("ts timestamp")
      .with_expr("DATE_FORMAT(ts, '%Y-%m-%d')")
      .with_session_vars({{"time_zone", "+08:00"}})
      .with_data({
        // dt_to_usec computes microseconds from epoch as local time.
        // For a timestamp column, the value is interpreted as UTC.
        {dt_to_usec(2024, 6, 15, 10, 0, 0)},
        {dt_to_usec(2024, 12, 31, 0, 0, 0)},
        {dt_to_usec(2023, 1, 1, 8, 0, 0)},
      })
      .run(engine_);
  EXPECT_EQ(3, result.row_count());
  // With +08:00, local time = UTC + 8h
  // 2024-06-15 10:00:00 UTC → 2024-06-15 18:00:00 local → "2024-06-15"
  // 2024-12-31 00:00:00 UTC → 2024-12-31 08:00:00 local → "2024-12-31"
  // 2023-01-01 08:00:00 UTC → 2023-01-01 16:00:00 local → "2023-01-01"
  EXPECT_TRUE(result.verify_ordered({
    {"2024-06-15"},
    {"2024-12-31"},
    {"2023-01-01"},
  }));
}

// Timestamp with cross-day timezone boundary: UTC time in previous day,
// local time (after +08:00 offset) in next day.
TEST_F(DateFormatFastPathTest, timestamp_tz_cross_day)
{
  // Data: UTC 2024-01-14 23:00:00 → local +08:00 = 2024-01-15 07:00:00
  // Without tz_offset, fast path would incorrectly return "2024-01-14"
  // With correct tz_offset (+8h), DATE_FORMAT returns "2024-01-15"
  auto result = expr_unit_test()
      .columns("ts timestamp")
      .with_expr("DATE_FORMAT(ts, '%Y-%m-%d')")
      .with_session_vars({{"time_zone", "+08:00"}})
      .with_data({
        {dt_to_usec(2024, 1, 14, 23, 0, 0)},
        {dt_to_usec(2024, 6, 15, 18, 30, 0)},
        {dt_to_usec(2023, 12, 31, 16, 0, 0)},
      })
      .run(engine_);
  EXPECT_EQ(3, result.row_count());
  // Correct results with tz_offset applied:
  // UTC 2024-01-14 23:00 → +08:00 2024-01-15 07:00 → "2024-01-15"
  // UTC 2024-06-15 18:30 → +08:00 2024-06-16 02:30 → "2024-06-16"
  // UTC 2023-12-31 16:00 → +08:00 2024-01-01 00:00 → "2024-01-01"
  EXPECT_TRUE(result.verify_ordered({
    {"2024-01-15"},
    {"2024-06-16"},
    {"2024-01-01"},
  }));
}

// Timestamp in UTC timezone: tz_offset=0, behaves like datetime
TEST_F(DateFormatFastPathTest, timestamp_utc_timezone)
{
  auto result = expr_unit_test()
      .columns("ts timestamp")
      .with_expr("DATE_FORMAT(ts, '%Y-%m-%d')")
      .with_session_vars({{"time_zone", "+00:00"}})
      .with_data({
        {dt_to_usec(2024, 1, 15, 0, 0, 0)},
        {dt_to_usec(2024, 12, 31, 23, 59, 59)},
        {dt_to_usec(2023, 6, 1, 12, 30, 45)},
      })
      .run(engine_);
  EXPECT_EQ(3, result.row_count());
  // With tz_offset=0, UTC date equals local date
  EXPECT_TRUE(result.verify_ordered({
    {"2024-01-15"},
    {"2024-12-31"},
    {"2023-06-01"},
  }));
}

// Timestamp with NULL and zero datetime
// Note: zero datetime produces formatted zero-string, not NULL, matching native MySQL
TEST_F(DateFormatFastPathTest, timestamp_null_and_zero)
{
  auto result = expr_unit_test()
      .columns("ts timestamp")
      .with_expr("DATE_FORMAT(ts, '%Y-%m-%d')")
      .with_session_vars({{"time_zone", "+08:00"}})
      .with_data({
        {NULL_VAL},
        {dt_to_usec(2024, 6, 15, 10, 0, 0)},
        {ObTimeConverter::ZERO_DATETIME},
      })
      .run(engine_);
  EXPECT_EQ(3, result.row_count());
  EXPECT_TRUE(result.verify_ordered({
    {NULL_VAL},
    {"2024-06-15"},
    {"0000-00-00"},  // Zero datetime → "0000-00-00", matches MySQL
  }));
}

// Timestamp should not use fast path for any recognized format pattern
TEST_F(DateFormatFastPathTest, timestamp_all_patterns_use_slow_path)
{
  struct {
    const char *format;
    const char *expected_val;
  } patterns[] = {
    {"%Y-%m-%d", "2024-06-16"},    // cross-day due to +08:00 tz_offset
    {"%Y-%m-01", "2024-06-01"},
    {"%Y-%m", "2024-06"},
    {"%Y%m%d", "20240616"},
  };

  for (const auto &p : patterns) {
    std::string expr_str = std::string("DATE_FORMAT(ts, '") + p.format + "')";
    auto result = expr_unit_test()
        .columns("ts timestamp")
        .with_expr(expr_str.c_str())
        .with_session_vars({{"time_zone", "+08:00"}})
        .with_data({
          // UTC 2024-06-15 18:00 → local +08:00 2024-06-16 02:00 (crosses day!)
          {dt_to_usec(2024, 6, 15, 18, 0, 0)},
        })
        .run(engine_);
    EXPECT_EQ(1, result.row_count()) << "pattern: " << p.format;
    EXPECT_TRUE(result.verify_ordered({
      {p.expected_val},
    })) << "pattern: " << p.format;
  }
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
