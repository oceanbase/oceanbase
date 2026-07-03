/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 *
 * GoogleTest coverage for ObDASMinShouldMatchSpec / ObDASMSMRuntimeUtil
 * (src/sql/das/search/ob_das_msm_runtime_util.{h,cpp}).
 */

// put top to use macro tricks
#include "mtlenv/mock_tenant_module_env.h"
// put top to use macro tricks

#include <climits>
#include <cstring>
#include <gtest/gtest.h>

#include "lib/allocator/page_arena.h"
#include "lib/ob_errno.h"
#include "share/datum/ob_datum.h"
#include "sql/das/search/ob_das_msm_runtime_util.h"

#define USING_LOG_PREFIX SQL_DAS

namespace oceanbase
{
namespace sql
{

using namespace oceanbase::common;

class TestDASMSMRuntimeUtil : public ::testing::Test
{
protected:
  static void SetUpTestCase()
  {
    ASSERT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
  }
  static void TearDownTestCase()
  {
    MockTenantModuleEnv::get_instance().destroy();
  }

  void SetUp() override
  {
    arena_.reset();
  }

  void TearDown() override
  {
    arena_.reset();
  }

  static void set_datum_cstr(ObArenaAllocator &alloc, ObDatum &d, const char *cstr)
  {
    const int64_t len = static_cast<int64_t>(strlen(cstr));
    char *buf = static_cast<char *>(alloc.alloc(len));
    ASSERT_NE(nullptr, buf);
    memcpy(buf, cstr, static_cast<size_t>(len));
    d.set_string(buf, static_cast<uint32_t>(len));
  }

  // ObDatum::set_int writes through ptr_/int_; default-constructed datum has null ptr — bind storage first.
  static void set_datum_int(ObArenaAllocator &alloc, ObDatum &d, const int64_t v)
  {
    void *buf = alloc.alloc(sizeof(int64_t));
    ASSERT_NE(nullptr, buf);
    d.ptr_ = static_cast<const char *>(buf);
    d.set_int(v);
  }

  ObArenaAllocator arena_{ObModIds::TEST};
};

// ---------- ObDASMinShouldMatchSpec::int_calc ----------

TEST_F(TestDASMSMRuntimeUtil, int_calc_fixed_positive)
{
  int64_t out = 0;
  ASSERT_EQ(OB_SUCCESS, ObDASMinShouldMatchSpec::int_calc(5, 2, out));
  EXPECT_EQ(2, out);
}

TEST_F(TestDASMSMRuntimeUtil, int_calc_negative_es_semantics)
{
  int64_t out = 0;
  ASSERT_EQ(OB_SUCCESS, ObDASMinShouldMatchSpec::int_calc(5, -1, out));
  EXPECT_EQ(4, out);

  ASSERT_EQ(OB_SUCCESS, ObDASMinShouldMatchSpec::int_calc(5, -2, out));
  EXPECT_EQ(3, out);
}

TEST_F(TestDASMSMRuntimeUtil, int_calc_clamp_non_positive)
{
  int64_t out = -1;
  ASSERT_EQ(OB_SUCCESS, ObDASMinShouldMatchSpec::int_calc(3, -5, out));
  EXPECT_EQ(0, out);
  ASSERT_EQ(OB_SUCCESS, ObDASMinShouldMatchSpec::int_calc(3, -4, out));
  EXPECT_EQ(0, out);
}

TEST_F(TestDASMSMRuntimeUtil, int_calc_invalid_optional_count)
{
  int64_t out = 0;
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObDASMinShouldMatchSpec::int_calc(-1, 2, out));
}

TEST_F(TestDASMSMRuntimeUtil, int_calc_raw_out_of_int32_range)
{
  int64_t out = 0;
  const int64_t too_large = static_cast<int64_t>(INT32_MAX) + 1;
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObDASMinShouldMatchSpec::int_calc(5, too_large, out));
}

// ---------- ObDASMinShouldMatchSpec::string_calc ----------

TEST_F(TestDASMSMRuntimeUtil, string_calc_plain_integer_string)
{
  int64_t out = 0;
  ASSERT_EQ(OB_SUCCESS, ObDASMinShouldMatchSpec::string_calc(arena_, 5, ObString::make_string("2"), out));
  EXPECT_EQ(2, out);
}

TEST_F(TestDASMSMRuntimeUtil, string_calc_negative_integer_string)
{
  int64_t out = 0;
  ASSERT_EQ(OB_SUCCESS, ObDASMinShouldMatchSpec::string_calc(arena_, 5, ObString::make_string("-2"), out));
  EXPECT_EQ(3, out);
}

TEST_F(TestDASMSMRuntimeUtil, string_calc_percent)
{
  int64_t out = 0;
  ASSERT_EQ(OB_SUCCESS, ObDASMinShouldMatchSpec::string_calc(arena_, 4, ObString::make_string("75%"), out));
  EXPECT_EQ(3, out);
}

TEST_F(TestDASMSMRuntimeUtil, string_calc_empty_spec_fails)
{
  int64_t out = 0;
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObDASMinShouldMatchSpec::string_calc(arena_, 5, ObString::make_string("   "), out));
}

// ES combination: N>3 -> apply rhs "90%" on fixed count (implementation uses inner string_calc).
TEST_F(TestDASMSMRuntimeUtil, string_calc_combination_3_lt_90_percent)
{
  int64_t out = 0;
  ASSERT_EQ(OB_SUCCESS,
      ObDASMinShouldMatchSpec::string_calc(arena_, 5, ObString::make_string("3<90%"), out));
  EXPECT_EQ(4, out);
}

// Negative percentage: ratio < 0 -> optional_count + floor(ratio).
TEST_F(TestDASMSMRuntimeUtil, string_calc_negative_percent)
{
  int64_t out = 0;
  ASSERT_EQ(OB_SUCCESS,
      ObDASMinShouldMatchSpec::string_calc(arena_, 5, ObString::make_string("-25%"), out));
  EXPECT_EQ(4, out);
}

// Multi-segment combination (space-separated), ES-style doc example.
TEST_F(TestDASMSMRuntimeUtil, string_calc_combination_multi_segment)
{
  int64_t out = 0;
  ASSERT_EQ(OB_SUCCESS, ObDASMinShouldMatchSpec::string_calc(
                arena_, 10, ObString::make_string("2<-25% 9<-3"), out));
  EXPECT_EQ(7, out);
}

TEST_F(TestDASMSMRuntimeUtil, string_calc_invalid_spec_not_integer)
{
  int64_t out = 0;
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      ObDASMinShouldMatchSpec::string_calc(arena_, 5, ObString::make_string("ABC"), out));
}

// ---------- ObDASMSMRuntimeUtil::calc_bool_should_msm ----------

TEST_F(TestDASMSMRuntimeUtil, calc_bool_null_datum)
{
  int64_t out = 0;
  ASSERT_EQ(OB_ERR_UNEXPECTED, ObDASMSMRuntimeUtil::calc_bool_should_msm(arena_, 3, false, nullptr, out));
}

TEST_F(TestDASMSMRuntimeUtil, calc_bool_invalid_unresolved_spec_propagates)
{
  ObDatum d;
  set_datum_cstr(arena_, d, "ABC");
  int64_t out = 0;
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      ObDASMSMRuntimeUtil::calc_bool_should_msm(arena_, 5, true, &d, out));
}

TEST_F(TestDASMSMRuntimeUtil, calc_bool_int_negative)
{
  ObDatum d;
  set_datum_int(arena_, d, -1);
  int64_t out = 0;
  ASSERT_EQ(OB_SUCCESS, ObDASMSMRuntimeUtil::calc_bool_should_msm(arena_, 5, false, &d, out));
  EXPECT_EQ(4, out);
}

TEST_F(TestDASMSMRuntimeUtil, calc_bool_string_spec)
{
  ObDatum d;
  set_datum_cstr(arena_, d, "75%");
  int64_t out = 0;
  ASSERT_EQ(OB_SUCCESS, ObDASMSMRuntimeUtil::calc_bool_should_msm(arena_, 4, true, &d, out));
  EXPECT_EQ(3, out);
}

TEST_F(TestDASMSMRuntimeUtil, calc_bool_string_extreme_percent_clamped_to_0_n)
{
  ObDatum d;
  int64_t out = 0;

  set_datum_cstr(arena_, d, "200%");
  ASSERT_EQ(OB_SUCCESS, ObDASMSMRuntimeUtil::calc_bool_should_msm(arena_, 5, true, &d, out));
  EXPECT_EQ(5, out);

  set_datum_cstr(arena_, d, "-150%");
  ASSERT_EQ(OB_SUCCESS, ObDASMSMRuntimeUtil::calc_bool_should_msm(arena_, 5, true, &d, out));
  EXPECT_EQ(0, out);
}

// ---------- ObDASMSMRuntimeUtil::calc_match_msm ----------

TEST_F(TestDASMSMRuntimeUtil, calc_match_null_datum)
{
  int64_t out = 0;
  ASSERT_EQ(OB_ERR_UNEXPECTED,
      ObDASMSMRuntimeUtil::calc_match_msm(arena_, 5, false, nullptr, out));
}

TEST_F(TestDASMSMRuntimeUtil, calc_match_invalid_unresolved_spec_propagates)
{
  ObDatum d;
  set_datum_cstr(arena_, d, "ABC");
  int64_t out = 0;
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      ObDASMSMRuntimeUtil::calc_match_msm(arena_, 5, true, &d, out));
}

TEST_F(TestDASMSMRuntimeUtil, calc_match_int_clamped_by_token_n)
{
  ObDatum d;
  set_datum_int(arena_, d, 10);
  int64_t out = 0;
  ASSERT_EQ(OB_SUCCESS, ObDASMSMRuntimeUtil::calc_match_msm(arena_, 5, false, &d, out));
  EXPECT_EQ(5, out);
}

TEST_F(TestDASMSMRuntimeUtil, calc_match_negative_then_cap)
{
  ObDatum d;
  set_datum_int(arena_, d, -1);
  int64_t out = 0;
  ASSERT_EQ(OB_SUCCESS, ObDASMSMRuntimeUtil::calc_match_msm(arena_, 5, false, &d, out));
  EXPECT_EQ(4, out);
}

TEST_F(TestDASMSMRuntimeUtil, calc_match_int_zero_treated_as_one)
{
  ObDatum d;
  set_datum_int(arena_, d, 0);
  int64_t out = 0;
  ASSERT_EQ(OB_SUCCESS, ObDASMSMRuntimeUtil::calc_match_msm(arena_, 5, false, &d, out));
  EXPECT_EQ(1, out);
}

TEST_F(TestDASMSMRuntimeUtil, calc_match_negative_overflow_treated_as_one)
{
  ObDatum d;
  set_datum_int(arena_, d, -4);
  int64_t out = 0;
  ASSERT_EQ(OB_SUCCESS, ObDASMSMRuntimeUtil::calc_match_msm(arena_, 3, false, &d, out));
  EXPECT_EQ(1, out);
}

TEST_F(TestDASMSMRuntimeUtil, calc_match_string_spec)
{
  ObDatum d;
  set_datum_cstr(arena_, d, "2");
  int64_t out = 0;
  ASSERT_EQ(OB_SUCCESS, ObDASMSMRuntimeUtil::calc_match_msm(arena_, 5, true, &d, out));
  EXPECT_EQ(2, out);
}

TEST_F(TestDASMSMRuntimeUtil, calc_match_string_extreme_percent_clamped_to_0_n)
{
  ObDatum d;
  int64_t out = 0;

  set_datum_cstr(arena_, d, "200%");
  ASSERT_EQ(OB_SUCCESS, ObDASMSMRuntimeUtil::calc_match_msm(arena_, 5, true, &d, out));
  EXPECT_EQ(5, out);

  set_datum_cstr(arena_, d, "-150%");
  ASSERT_EQ(OB_SUCCESS, ObDASMSMRuntimeUtil::calc_match_msm(arena_, 5, true, &d, out));
  EXPECT_EQ(1, out);
}

// ---------- ObDASMSMRuntimeUtil::calc_multi_or_query_string_msm ----------

TEST_F(TestDASMSMRuntimeUtil, calc_multi_null_datum)
{
  int64_t out = 0;
  ASSERT_EQ(OB_ERR_UNEXPECTED,
      ObDASMSMRuntimeUtil::calc_multi_or_query_string_msm(arena_, 5, false, nullptr, out));
}

TEST_F(TestDASMSMRuntimeUtil, calc_multi_invalid_unresolved_spec_propagates)
{
  ObDatum d;
  set_datum_cstr(arena_, d, "ABC");
  int64_t out = 0;
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      ObDASMSMRuntimeUtil::calc_multi_or_query_string_msm(arena_, 5, true, &d, out));
}

TEST_F(TestDASMSMRuntimeUtil, calc_multi_resolved_int_and_cap)
{
  ObDatum d;
  set_datum_int(arena_, d, 3);
  int64_t out = 0;
  ASSERT_EQ(OB_SUCCESS, ObDASMSMRuntimeUtil::calc_multi_or_query_string_msm(arena_, 10, false, &d, out));
  EXPECT_EQ(3, out);
}

TEST_F(TestDASMSMRuntimeUtil, calc_multi_unresolved_string_clamp_to_1_n)
{
  ObDatum d;
  set_datum_cstr(arena_, d, "75%");
  int64_t out = 0;
  ASSERT_EQ(OB_SUCCESS, ObDASMSMRuntimeUtil::calc_multi_or_query_string_msm(arena_, 4, true, &d, out));
  EXPECT_EQ(3, out);
}

TEST_F(TestDASMSMRuntimeUtil, calc_multi_unresolved_string_extreme_percent_clamped_to_1_n)
{
  ObDatum d;
  int64_t out = 0;

  set_datum_cstr(arena_, d, "200%");
  ASSERT_EQ(OB_SUCCESS, ObDASMSMRuntimeUtil::calc_multi_or_query_string_msm(arena_, 5, true, &d, out));
  EXPECT_EQ(5, out);

  set_datum_cstr(arena_, d, "-150%");
  ASSERT_EQ(OB_SUCCESS, ObDASMSMRuntimeUtil::calc_multi_or_query_string_msm(arena_, 5, true, &d, out));
  EXPECT_EQ(1, out);
}

TEST_F(TestDASMSMRuntimeUtil, calc_multi_resolved_negative_int)
{
  ObDatum d;
  set_datum_int(arena_, d, -2);
  int64_t out = 0;
  ASSERT_EQ(OB_SUCCESS, ObDASMSMRuntimeUtil::calc_multi_or_query_string_msm(arena_, 5, false, &d, out));
  EXPECT_EQ(3, out);
}

TEST_F(TestDASMSMRuntimeUtil, calc_multi_resolved_int_zero_treated_as_one)
{
  ObDatum d;
  set_datum_int(arena_, d, 0);
  int64_t out = 0;
  ASSERT_EQ(OB_SUCCESS, ObDASMSMRuntimeUtil::calc_multi_or_query_string_msm(arena_, 5, false, &d, out));
  EXPECT_EQ(1, out);
}

TEST_F(TestDASMSMRuntimeUtil, calc_multi_negative_overflow_treated_as_one)
{
  ObDatum d;
  set_datum_int(arena_, d, -4);
  int64_t out = 0;
  ASSERT_EQ(OB_SUCCESS, ObDASMSMRuntimeUtil::calc_multi_or_query_string_msm(arena_, 3, false, &d, out));
  EXPECT_EQ(1, out);
}

} // namespace sql
} // namespace oceanbase

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
