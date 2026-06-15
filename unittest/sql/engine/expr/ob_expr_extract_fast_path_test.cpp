/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 *
 * Unit tests for the vectorized fast-path functions added in ob_expr_extract.cpp:
 *   - process_vector_mysql_datetime_fast   (VEC_TC_DATETIME, ObDateTimeType)
 *   - process_vector_mysql_date_fast       (VEC_TC_DATE)
 *   - process_vector_mysql_mysqldatetime_fast (VEC_TC_MYSQL_DATETIME)
 *   - process_vector_mysql_mysqldate_fast  (VEC_TC_MYSQL_DATE)
 *
 * The functions are file-scoped (static), so we include the .cpp directly.
 * This pattern is established in OceanBase's own test suite.
 */

#include <gtest/gtest.h>
#define private public
#define protected public
#include "sql/engine/expr/ob_expr_extract.cpp"
#include "share/vector/ob_fixed_length_vector.h"
#undef private
#undef protected

using namespace oceanbase::common;
using namespace oceanbase::sql;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

static const int64_t ROW_CNT = 8;

/** Allocate and zero-initialise a bit-vector large enough for ROW_CNT rows. */
static ObBitVector *alloc_bitvec(void *buf, int64_t row_cnt = ROW_CNT)
{
  int64_t sz = ObBitVector::memory_size(row_cnt);
  MEMSET(buf, 0, sz);
  return reinterpret_cast<ObBitVector *>(buf);
}

// ---------------------------------------------------------------------------
// Typed vector wrapper: owns data + nulls buffers, concrete instantiation.
// ---------------------------------------------------------------------------
template <VecValueTypeClass VTC>
struct TestVec {
  using ValueType = RTCType<VTC>;
  using VecType   = ObFixedLengthVector<ValueType, VectorBasicOp<VTC>>;

  static const int64_t MAX_ROWS = ROW_CNT;
  char data_buf[MAX_ROWS * sizeof(ValueType)];
  char nulls_buf[1024]; // large enough for ROW_CNT bits
  ObBitVector *nulls;
  VecType vec;

  TestVec() : nulls(alloc_bitvec(nulls_buf)), vec(data_buf, nulls)
  {
    MEMSET(data_buf, 0, sizeof(data_buf));
  }

  // Set raw integer (cast to ValueType).
  void set_val(int64_t i, int64_t v)
  {
    ValueType typed = static_cast<ValueType>(v);
    vec.set_payload(i, &typed, sizeof(typed));
    vec.unset_null(i);
  }
  void set_null(int64_t i) { vec.set_null(i); }

  int64_t get_val(int64_t i) { return static_cast<int64_t>(vec.get_int(i)); }
  bool    is_null(int64_t i) { return vec.is_null(i); }

  // Reset result buffer between sub-tests.
  void reset()
  {
    MEMSET(data_buf, 0, sizeof(data_buf));
    MEMSET(nulls_buf, 0, sizeof(nulls_buf));
    nulls = alloc_bitvec(nulls_buf);
    new (&vec) VecType(data_buf, nulls);
  }
};

// Convenience aliases
using DtVec    = TestVec<VEC_TC_DATETIME>;
using DateVec  = TestVec<VEC_TC_DATE>;
using MdtVec   = TestVec<VEC_TC_MYSQL_DATETIME>;
using MdVec    = TestVec<VEC_TC_MYSQL_DATE>;
using ResVec   = TestVec<VEC_TC_INTEGER>;

// ---------------------------------------------------------------------------
// Fixture
// ---------------------------------------------------------------------------
class ObExprExtractFastPathTest : public ::testing::Test
{
protected:
  char skip_buf[1024];
  char eval_flags_buf[1024];
  ObBitVector *skip;
  ObBitVector *eval_flags;
  EvalBound bound;

  void SetUp() override
  {
    skip       = alloc_bitvec(skip_buf);
    eval_flags = alloc_bitvec(eval_flags_buf);
    bound      = EvalBound(static_cast<uint16_t>(ROW_CNT), true /*all_active*/);
  }

  // Construct a DATETIME usec value from date+time components.
  static int64_t make_datetime(int32_t year, int32_t mon, int32_t day,
                               int32_t hour = 0, int32_t min = 0, int32_t sec = 0,
                               int32_t usec = 0)
  {
    int32_t days = ObTimeConverter::calc_date(year, mon, day);
    return static_cast<int64_t>(days) * USECS_PER_DAY
           + static_cast<int64_t>(hour) * MINS_PER_HOUR * SECS_PER_MIN * USECS_PER_SEC
           + static_cast<int64_t>(min)  * SECS_PER_MIN  * USECS_PER_SEC
           + static_cast<int64_t>(sec)  * USECS_PER_SEC
           + usec;
  }

  // Construct an ObMySQLDateTime packed int64_t.
  static int64_t make_mysql_datetime(int32_t year, int32_t mon, int32_t day,
                                     int32_t hour = 0, int32_t min = 0, int32_t sec = 0,
                                     int32_t usec = 0)
  {
    ObMySQLDateTime mdt;
    mdt.year_month_   = ObMySQLDateTime::year_month(year, mon);
    mdt.day_          = day;
    mdt.hour_         = hour;
    mdt.minute_       = min;
    mdt.second_       = sec;
    mdt.microseconds_ = usec;
    mdt.sign_         = 0;
    return mdt.datetime_;
  }

  // Construct an ObMySQLDate packed int32_t.
  static int32_t make_mysql_date(int32_t year, int32_t mon, int32_t day)
  {
    ObMySQLDate md;
    md.year_  = year;
    md.month_ = mon;
    md.day_   = day;
    return md.date_;
  }
};

// ===========================================================================
// 1.  process_vector_mysql_datetime_fast  (need_tz = false, ObDateTimeType)
// ===========================================================================

static int call_dt_fast(DtVec &arg, ResVec &res,
                        const EvalBound &bound,
                        ObBitVector &skip, ObBitVector &eval_flags,
                        ObDateUnitType field,
                        ObObjType date_type = ObDateTimeType,
                        const ObTimeZoneInfo *tz_info = nullptr)
{
  return process_vector_mysql_datetime_fast(
      arg.vec, date_type, bound, skip, eval_flags, tz_info, field, res.vec);
}

TEST_F(ObExprExtractFastPathTest, datetime_extract_year)
{
  DtVec arg; ResVec res;
  arg.set_val(0, make_datetime(2014,  1,  4));
  arg.set_val(1, make_datetime(2000, 12, 31));
  arg.set_val(2, ObTimeConverter::ZERO_DATETIME);
  arg.set_null(3);

  ASSERT_EQ(OB_SUCCESS, call_dt_fast(arg, res, bound, *skip, *eval_flags, DATE_UNIT_YEAR));
  EXPECT_EQ(2014, res.get_val(0));
  EXPECT_EQ(2000, res.get_val(1));
  EXPECT_EQ(0,    res.get_val(2));
  EXPECT_TRUE(res.is_null(3));
}

TEST_F(ObExprExtractFastPathTest, datetime_extract_month)
{
  DtVec arg; ResVec res;
  arg.set_val(0, make_datetime(2014,  1,  4));
  arg.set_val(1, make_datetime(2014,  7, 15));
  arg.set_val(2, make_datetime(2014, 12, 31));
  arg.set_val(3, ObTimeConverter::ZERO_DATETIME);
  arg.set_null(4);

  ASSERT_EQ(OB_SUCCESS, call_dt_fast(arg, res, bound, *skip, *eval_flags, DATE_UNIT_MONTH));
  EXPECT_EQ( 1, res.get_val(0));
  EXPECT_EQ( 7, res.get_val(1));
  EXPECT_EQ(12, res.get_val(2));
  EXPECT_EQ( 0, res.get_val(3));
  EXPECT_TRUE(res.is_null(4));
}

TEST_F(ObExprExtractFastPathTest, datetime_extract_day)
{
  DtVec arg; ResVec res;
  arg.set_val(0, make_datetime(2014,  1,  4));
  arg.set_val(1, make_datetime(2014,  1, 31));
  arg.set_val(2, ObTimeConverter::ZERO_DATETIME);
  arg.set_null(3);

  ASSERT_EQ(OB_SUCCESS, call_dt_fast(arg, res, bound, *skip, *eval_flags, DATE_UNIT_DAY));
  EXPECT_EQ( 4, res.get_val(0));
  EXPECT_EQ(31, res.get_val(1));
  EXPECT_EQ( 0, res.get_val(2));
  EXPECT_TRUE(res.is_null(3));
}

TEST_F(ObExprExtractFastPathTest, datetime_extract_quarter)
{
  DtVec arg; ResVec res;
  arg.set_val(0, make_datetime(2014,  1, 4));   // Q1
  arg.set_val(1, make_datetime(2014,  4, 1));   // Q2
  arg.set_val(2, make_datetime(2014,  7, 1));   // Q3
  arg.set_val(3, make_datetime(2014, 10, 1));   // Q4
  arg.set_val(4, ObTimeConverter::ZERO_DATETIME);
  arg.set_null(5);

  ASSERT_EQ(OB_SUCCESS, call_dt_fast(arg, res, bound, *skip, *eval_flags, DATE_UNIT_QUARTER));
  EXPECT_EQ(1, res.get_val(0));
  EXPECT_EQ(2, res.get_val(1));
  EXPECT_EQ(3, res.get_val(2));
  EXPECT_EQ(4, res.get_val(3));
  EXPECT_EQ(0, res.get_val(4));
  EXPECT_TRUE(res.is_null(5));
}

TEST_F(ObExprExtractFastPathTest, datetime_extract_hour)
{
  DtVec arg; ResVec res;
  arg.set_val(0, make_datetime(2014, 1, 4, 11, 58, 58, 999000));
  arg.set_val(1, make_datetime(2014, 1, 4,  0,  0,  0,      0));
  arg.set_val(2, make_datetime(2014, 1, 4, 23, 59, 59, 999999));
  arg.set_val(3, ObTimeConverter::ZERO_DATETIME);
  arg.set_null(4);

  ASSERT_EQ(OB_SUCCESS, call_dt_fast(arg, res, bound, *skip, *eval_flags, DATE_UNIT_HOUR));
  EXPECT_EQ(11, res.get_val(0));
  EXPECT_EQ( 0, res.get_val(1));
  EXPECT_EQ(23, res.get_val(2));
  EXPECT_EQ( 0, res.get_val(3));
  EXPECT_TRUE(res.is_null(4));
}

TEST_F(ObExprExtractFastPathTest, datetime_extract_minute)
{
  DtVec arg; ResVec res;
  arg.set_val(0, make_datetime(2014, 1, 4, 11, 58, 58, 999000));
  arg.set_val(1, make_datetime(2014, 1, 4,  0,  0,  0,      0));
  arg.set_val(2, make_datetime(2014, 1, 4, 23, 59, 59, 999999));
  arg.set_val(3, ObTimeConverter::ZERO_DATETIME);
  arg.set_null(4);

  ASSERT_EQ(OB_SUCCESS, call_dt_fast(arg, res, bound, *skip, *eval_flags, DATE_UNIT_MINUTE));
  EXPECT_EQ(58, res.get_val(0));
  EXPECT_EQ( 0, res.get_val(1));
  EXPECT_EQ(59, res.get_val(2));
  EXPECT_EQ( 0, res.get_val(3));
  EXPECT_TRUE(res.is_null(4));
}

TEST_F(ObExprExtractFastPathTest, datetime_extract_second)
{
  DtVec arg; ResVec res;
  arg.set_val(0, make_datetime(2014, 1, 4, 11, 58, 58, 999000));
  arg.set_val(1, make_datetime(2014, 1, 4,  0,  0,  0,      0));
  arg.set_val(2, make_datetime(2014, 1, 4, 23, 59,  1, 500000));
  arg.set_val(3, ObTimeConverter::ZERO_DATETIME);
  arg.set_null(4);

  ASSERT_EQ(OB_SUCCESS, call_dt_fast(arg, res, bound, *skip, *eval_flags, DATE_UNIT_SECOND));
  EXPECT_EQ(58, res.get_val(0));
  EXPECT_EQ( 0, res.get_val(1));
  EXPECT_EQ( 1, res.get_val(2));
  EXPECT_EQ( 0, res.get_val(3));
  EXPECT_TRUE(res.is_null(4));
}

TEST_F(ObExprExtractFastPathTest, datetime_extract_microsecond)
{
  DtVec arg; ResVec res;
  arg.set_val(0, make_datetime(2014, 1, 4, 11, 58, 58, 999000));
  arg.set_val(1, make_datetime(2014, 1, 4,  0,  0,  0,      0));
  arg.set_val(2, make_datetime(2014, 1, 4, 23, 59, 59, 123456));
  arg.set_val(3, ObTimeConverter::ZERO_DATETIME);
  arg.set_null(4);

  ASSERT_EQ(OB_SUCCESS, call_dt_fast(arg, res, bound, *skip, *eval_flags, DATE_UNIT_MICROSECOND));
  EXPECT_EQ(999000, res.get_val(0));
  EXPECT_EQ(     0, res.get_val(1));
  EXPECT_EQ(123456, res.get_val(2));
  EXPECT_EQ(     0, res.get_val(3));
  EXPECT_TRUE(res.is_null(4));
}

// Compound/unsupported fields must not pass can_fast_extract().
TEST_F(ObExprExtractFastPathTest, datetime_fast_unsupported_fields)
{
  EXPECT_FALSE(can_fast_extract(DATE_UNIT_MINUTE_SECOND));
  EXPECT_FALSE(can_fast_extract(DATE_UNIT_DAY_HOUR));
  EXPECT_FALSE(can_fast_extract(DATE_UNIT_WEEK));
  EXPECT_FALSE(can_fast_extract(DATE_UNIT_YEAR_MONTH));
  EXPECT_FALSE(can_fast_extract(DATE_UNIT_DAY_MINUTE));
  EXPECT_FALSE(can_fast_extract(DATE_UNIT_DAY_SECOND));
  EXPECT_FALSE(can_fast_extract(DATE_UNIT_HOUR_MINUTE));
  EXPECT_FALSE(can_fast_extract(DATE_UNIT_HOUR_SECOND));
  EXPECT_FALSE(can_fast_extract(DATE_UNIT_DAY_MICROSECOND));
  EXPECT_FALSE(can_fast_extract(DATE_UNIT_HOUR_MICROSECOND));
  EXPECT_FALSE(can_fast_extract(DATE_UNIT_MINUTE_MICROSECOND));
  EXPECT_FALSE(can_fast_extract(DATE_UNIT_SECOND_MICROSECOND));
  EXPECT_FALSE(can_fast_extract(DATE_UNIT_TIMEZONE_HOUR));
  EXPECT_FALSE(can_fast_extract(DATE_UNIT_TIMEZONE_MINUTE));
  EXPECT_FALSE(can_fast_extract(DATE_UNIT_TIMEZONE_REGION));
  EXPECT_FALSE(can_fast_extract(DATE_UNIT_TIMEZONE_ABBR));
}

// Compound/unsupported fields must not pass can_fast_extract_date().
TEST_F(ObExprExtractFastPathTest, date_fast_unsupported_fields)
{
  EXPECT_FALSE(can_fast_extract_date(DATE_UNIT_HOUR));
  EXPECT_FALSE(can_fast_extract_date(DATE_UNIT_MINUTE));
  EXPECT_FALSE(can_fast_extract_date(DATE_UNIT_SECOND));
  EXPECT_FALSE(can_fast_extract_date(DATE_UNIT_MICROSECOND));
  EXPECT_FALSE(can_fast_extract_date(DATE_UNIT_WEEK));
  EXPECT_FALSE(can_fast_extract_date(DATE_UNIT_MINUTE_SECOND));
  EXPECT_FALSE(can_fast_extract_date(DATE_UNIT_DAY_HOUR));
}

// skip bit set: row must be left untouched.
TEST_F(ObExprExtractFastPathTest, datetime_fast_skip_respected)
{
  DtVec arg; ResVec res;
  arg.set_val(0, make_datetime(2014, 1, 4));
  arg.set_val(1, make_datetime(2022, 6, 15));
  skip->set(0);

  ASSERT_EQ(OB_SUCCESS, call_dt_fast(arg, res, bound, *skip, *eval_flags, DATE_UNIT_YEAR));
  EXPECT_EQ(   0, res.get_val(0));  // untouched
  EXPECT_EQ(2022, res.get_val(1));
}

// eval_flags already set: row already evaluated, must not be re-written.
TEST_F(ObExprExtractFastPathTest, datetime_fast_eval_flags_respected)
{
  DtVec arg; ResVec res;
  arg.set_val(0, make_datetime(2014, 1, 4));
  arg.set_val(1, make_datetime(2022, 6, 15));
  eval_flags->set(0);

  ASSERT_EQ(OB_SUCCESS, call_dt_fast(arg, res, bound, *skip, *eval_flags, DATE_UNIT_YEAR));
  EXPECT_EQ(   0, res.get_val(0));  // untouched
  EXPECT_EQ(2022, res.get_val(1));
}

// ===========================================================================
// 2.  process_vector_mysql_date_fast  (VEC_TC_DATE, int32_t days)
// ===========================================================================

static int call_date_fast(DateVec &arg, ResVec &res,
                          const EvalBound &bound,
                          ObBitVector &skip, ObBitVector &eval_flags,
                          ObDateUnitType field)
{
  return process_vector_mysql_date_fast(arg.vec, bound, skip, eval_flags, field, res.vec);
}

TEST_F(ObExprExtractFastPathTest, date_extract_year)
{
  DateVec arg; ResVec res;
  arg.set_val(0, ObTimeConverter::calc_date(2014,  1,  4));
  arg.set_val(1, ObTimeConverter::calc_date(2000, 12, 31));
  arg.set_val(2, ObTimeConverter::ZERO_DATE);
  arg.set_null(3);

  ASSERT_EQ(OB_SUCCESS, call_date_fast(arg, res, bound, *skip, *eval_flags, DATE_UNIT_YEAR));
  EXPECT_EQ(2014, res.get_val(0));
  EXPECT_EQ(2000, res.get_val(1));
  EXPECT_EQ(   0, res.get_val(2));
  EXPECT_TRUE(res.is_null(3));
}

TEST_F(ObExprExtractFastPathTest, date_extract_month)
{
  DateVec arg; ResVec res;
  arg.set_val(0, ObTimeConverter::calc_date(2014,  1,  4));
  arg.set_val(1, ObTimeConverter::calc_date(2014, 12, 31));
  arg.set_val(2, ObTimeConverter::ZERO_DATE);
  arg.set_null(3);

  ASSERT_EQ(OB_SUCCESS, call_date_fast(arg, res, bound, *skip, *eval_flags, DATE_UNIT_MONTH));
  EXPECT_EQ( 1, res.get_val(0));
  EXPECT_EQ(12, res.get_val(1));
  EXPECT_EQ( 0, res.get_val(2));
  EXPECT_TRUE(res.is_null(3));
}

TEST_F(ObExprExtractFastPathTest, date_extract_day)
{
  DateVec arg; ResVec res;
  arg.set_val(0, ObTimeConverter::calc_date(2014,  1,  4));
  arg.set_val(1, ObTimeConverter::calc_date(2014,  1, 31));
  arg.set_val(2, ObTimeConverter::ZERO_DATE);
  arg.set_null(3);

  ASSERT_EQ(OB_SUCCESS, call_date_fast(arg, res, bound, *skip, *eval_flags, DATE_UNIT_DAY));
  EXPECT_EQ( 4, res.get_val(0));
  EXPECT_EQ(31, res.get_val(1));
  EXPECT_EQ( 0, res.get_val(2));
  EXPECT_TRUE(res.is_null(3));
}

TEST_F(ObExprExtractFastPathTest, date_extract_quarter)
{
  DateVec arg; ResVec res;
  arg.set_val(0, ObTimeConverter::calc_date(2014,  1, 4));  // Q1
  arg.set_val(1, ObTimeConverter::calc_date(2014,  4, 1));  // Q2
  arg.set_val(2, ObTimeConverter::calc_date(2014,  7, 1));  // Q3
  arg.set_val(3, ObTimeConverter::calc_date(2014, 10, 1));  // Q4
  arg.set_val(4, ObTimeConverter::ZERO_DATE);
  arg.set_null(5);

  ASSERT_EQ(OB_SUCCESS, call_date_fast(arg, res, bound, *skip, *eval_flags, DATE_UNIT_QUARTER));
  EXPECT_EQ(1, res.get_val(0));
  EXPECT_EQ(2, res.get_val(1));
  EXPECT_EQ(3, res.get_val(2));
  EXPECT_EQ(4, res.get_val(3));
  EXPECT_EQ(0, res.get_val(4));
  EXPECT_TRUE(res.is_null(5));
}

TEST_F(ObExprExtractFastPathTest, date_fast_supported_only_date_fields)
{
  // can_fast_extract_date() only accepts date components.
  EXPECT_TRUE(can_fast_extract_date(DATE_UNIT_YEAR));
  EXPECT_TRUE(can_fast_extract_date(DATE_UNIT_MONTH));
  EXPECT_TRUE(can_fast_extract_date(DATE_UNIT_DAY));
  EXPECT_TRUE(can_fast_extract_date(DATE_UNIT_QUARTER));

  // Time fields, compound fields, week not supported for date-only types.
  EXPECT_FALSE(can_fast_extract_date(DATE_UNIT_HOUR));
  EXPECT_FALSE(can_fast_extract_date(DATE_UNIT_MINUTE));
  EXPECT_FALSE(can_fast_extract_date(DATE_UNIT_SECOND));
  EXPECT_FALSE(can_fast_extract_date(DATE_UNIT_MICROSECOND));
  EXPECT_FALSE(can_fast_extract_date(DATE_UNIT_WEEK));
}

// ===========================================================================
// 3.  process_vector_mysql_mysqldatetime_fast  (VEC_TC_MYSQL_DATETIME)
// ===========================================================================

static int call_mdt_fast(MdtVec &arg, ResVec &res,
                         const EvalBound &bound,
                         ObBitVector &skip, ObBitVector &eval_flags,
                         ObDateUnitType field)
{
  return process_vector_mysql_mysqldatetime_fast(
      arg.vec, bound, skip, eval_flags, field, res.vec);
}

TEST_F(ObExprExtractFastPathTest, mysqldatetime_extract_all_fields)
{
  // 2014-01-04 11:58:58.999000
  const int64_t v = make_mysql_datetime(2014, 1, 4, 11, 58, 58, 999000);

  struct { ObDateUnitType field; int64_t expected; } cases[] = {
    { DATE_UNIT_YEAR,         2014 },
    { DATE_UNIT_MONTH,           1 },
    { DATE_UNIT_DAY,             4 },
    { DATE_UNIT_HOUR,           11 },
    { DATE_UNIT_MINUTE,         58 },
    { DATE_UNIT_SECOND,         58 },
    { DATE_UNIT_MICROSECOND, 999000 },
    { DATE_UNIT_QUARTER,         1 },
  };

  for (auto &c : cases) {
    MdtVec arg; ResVec res;
    arg.set_val(0, v);
    arg.set_val(1, ObTimeConverter::MYSQL_ZERO_DATETIME);
    arg.set_null(2);

    SCOPED_TRACE(c.field);
    ASSERT_EQ(OB_SUCCESS, call_mdt_fast(arg, res, bound, *skip, *eval_flags, c.field));
    EXPECT_EQ(c.expected, res.get_val(0)) << "field=" << c.field;
    EXPECT_EQ(0,           res.get_val(1)) << "ZERO for field=" << c.field;
    EXPECT_TRUE(res.is_null(2))            << "NULL for field=" << c.field;
  }
}

TEST_F(ObExprExtractFastPathTest, mysqldatetime_extract_quarter_boundaries)
{
  MdtVec arg; ResVec res;
  arg.set_val(0, make_mysql_datetime(2024,  3, 31));  // Q1
  arg.set_val(1, make_mysql_datetime(2024,  6, 30));  // Q2
  arg.set_val(2, make_mysql_datetime(2024,  9, 30));  // Q3
  arg.set_val(3, make_mysql_datetime(2024, 12, 31));  // Q4

  ASSERT_EQ(OB_SUCCESS, call_mdt_fast(arg, res, bound, *skip, *eval_flags, DATE_UNIT_QUARTER));
  EXPECT_EQ(1, res.get_val(0));
  EXPECT_EQ(2, res.get_val(1));
  EXPECT_EQ(3, res.get_val(2));
  EXPECT_EQ(4, res.get_val(3));
}

TEST_F(ObExprExtractFastPathTest, mysqldatetime_fast_all_simple_fields_supported)
{
  // can_fast_extract() accepts all simple fields for datetime types.
  EXPECT_TRUE(can_fast_extract(DATE_UNIT_YEAR));
  EXPECT_TRUE(can_fast_extract(DATE_UNIT_MONTH));
  EXPECT_TRUE(can_fast_extract(DATE_UNIT_DAY));
  EXPECT_TRUE(can_fast_extract(DATE_UNIT_QUARTER));
  EXPECT_TRUE(can_fast_extract(DATE_UNIT_HOUR));
  EXPECT_TRUE(can_fast_extract(DATE_UNIT_MINUTE));
  EXPECT_TRUE(can_fast_extract(DATE_UNIT_SECOND));
  EXPECT_TRUE(can_fast_extract(DATE_UNIT_MICROSECOND));
}

// ===========================================================================
// 4.  process_vector_mysql_mysqldate_fast  (VEC_TC_MYSQL_DATE)
// ===========================================================================

static int call_md_fast(MdVec &arg, ResVec &res,
                        const EvalBound &bound,
                        ObBitVector &skip, ObBitVector &eval_flags,
                        ObDateUnitType field)
{
  return process_vector_mysql_mysqldate_fast(
      arg.vec, bound, skip, eval_flags, field, res.vec);
}

TEST_F(ObExprExtractFastPathTest, mysqldate_extract_year)
{
  MdVec arg; ResVec res;
  arg.set_val(0, make_mysql_date(2014,  1,  4));
  arg.set_val(1, make_mysql_date(2000, 12, 31));
  arg.set_val(2, ObTimeConverter::MYSQL_ZERO_DATE);
  arg.set_null(3);

  ASSERT_EQ(OB_SUCCESS, call_md_fast(arg, res, bound, *skip, *eval_flags, DATE_UNIT_YEAR));
  EXPECT_EQ(2014, res.get_val(0));
  EXPECT_EQ(2000, res.get_val(1));
  EXPECT_EQ(   0, res.get_val(2));
  EXPECT_TRUE(res.is_null(3));
}

TEST_F(ObExprExtractFastPathTest, mysqldate_extract_month)
{
  MdVec arg; ResVec res;
  arg.set_val(0, make_mysql_date(2014,  1,  4));
  arg.set_val(1, make_mysql_date(2014, 12, 31));
  arg.set_val(2, ObTimeConverter::MYSQL_ZERO_DATE);
  arg.set_null(3);

  ASSERT_EQ(OB_SUCCESS, call_md_fast(arg, res, bound, *skip, *eval_flags, DATE_UNIT_MONTH));
  EXPECT_EQ( 1, res.get_val(0));
  EXPECT_EQ(12, res.get_val(1));
  EXPECT_EQ( 0, res.get_val(2));
  EXPECT_TRUE(res.is_null(3));
}

TEST_F(ObExprExtractFastPathTest, mysqldate_extract_day)
{
  MdVec arg; ResVec res;
  arg.set_val(0, make_mysql_date(2014,  1,  4));
  arg.set_val(1, make_mysql_date(2014,  1, 31));
  arg.set_val(2, ObTimeConverter::MYSQL_ZERO_DATE);
  arg.set_null(3);

  ASSERT_EQ(OB_SUCCESS, call_md_fast(arg, res, bound, *skip, *eval_flags, DATE_UNIT_DAY));
  EXPECT_EQ( 4, res.get_val(0));
  EXPECT_EQ(31, res.get_val(1));
  EXPECT_EQ( 0, res.get_val(2));
  EXPECT_TRUE(res.is_null(3));
}

TEST_F(ObExprExtractFastPathTest, mysqldate_extract_quarter)
{
  MdVec arg; ResVec res;
  arg.set_val(0, make_mysql_date(2024,  1,  1));  // Q1
  arg.set_val(1, make_mysql_date(2024,  4, 30));  // Q2
  arg.set_val(2, make_mysql_date(2024,  9,  1));  // Q3
  arg.set_val(3, make_mysql_date(2024, 11, 15));  // Q4
  arg.set_val(4, ObTimeConverter::MYSQL_ZERO_DATE);
  arg.set_null(5);

  ASSERT_EQ(OB_SUCCESS, call_md_fast(arg, res, bound, *skip, *eval_flags, DATE_UNIT_QUARTER));
  EXPECT_EQ(1, res.get_val(0));
  EXPECT_EQ(2, res.get_val(1));
  EXPECT_EQ(3, res.get_val(2));
  EXPECT_EQ(4, res.get_val(3));
  EXPECT_EQ(0, res.get_val(4));
  EXPECT_TRUE(res.is_null(5));
}

TEST_F(ObExprExtractFastPathTest, mysqldate_fast_supported_only_date_fields)
{
  // can_fast_extract_date() accepts only date components.
  EXPECT_TRUE(can_fast_extract_date(DATE_UNIT_YEAR));
  EXPECT_TRUE(can_fast_extract_date(DATE_UNIT_MONTH));
  EXPECT_TRUE(can_fast_extract_date(DATE_UNIT_DAY));
  EXPECT_TRUE(can_fast_extract_date(DATE_UNIT_QUARTER));

  // Time fields, compound fields, week not supported for date-only types.
  EXPECT_FALSE(can_fast_extract_date(DATE_UNIT_HOUR));
  EXPECT_FALSE(can_fast_extract_date(DATE_UNIT_MINUTE));
  EXPECT_FALSE(can_fast_extract_date(DATE_UNIT_SECOND));
  EXPECT_FALSE(can_fast_extract_date(DATE_UNIT_MICROSECOND));
  EXPECT_FALSE(can_fast_extract_date(DATE_UNIT_WEEK));
}

// ===========================================================================
// main
// ===========================================================================

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
