/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL

// Correctness + performance tests for the SUM(decint32) -> int128 SIMD fast path.
//
//   Part A (kernel level): drive neon_sum_i32_to_i128 directly on raw arrays,
//     cross-checked against an independent native __int128 golden. This is both
//     the rigorous correctness oracle and the clean perf A/B -- the "before"
//     baseline is the per-row int128 add that util.h add_values performs today.
//
//   Part B (end to end): ScalarAggTestSpec with enable_dual_format_check(),
//     which runs the legacy scalar operator (does NOT go through
//     share/aggregate) against the vec operator (hits NeonSumI32ToI128 on ARM)
//     and asserts identical results.

#include <gtest/gtest.h>
#include <vector>
#include <random>
#include <cstdio>
// op_test_kit MUST be included before any sql engine / code-generator header:
// ob_op_test_base.h enables `#define private public` to reach
// ObStaticEngineExprCG::get_rt_expr, so the cg header has to be parsed under it.
#include "unittest/sql/engine/op_tests/ob_op_test_kit.h"
#include "share/aggregate/sum_simd.h"
#include "lib/wide_integer/ob_wide_integer_str_funcs.h"
#include "lib/time/ob_time_utility.h"

namespace oceanbase
{
using namespace common;
using namespace share::aggregate;

namespace unittest
{

// ===================== Part A: kernel correctness =====================

class SumI128KernelTest : public ::testing::Test {};

#if defined(__ARM_NEON)
// Build an int128_t from a native __int128 golden (items_[0]=low, items_[1]=high).
static int128_t make_i128(__int128 g)
{
  unsigned __int128 u = static_cast<unsigned __int128>(g);
  uint64_t lo = static_cast<uint64_t>(u & 0xFFFFFFFFFFFFFFFFULL);
  uint64_t hi = static_cast<uint64_t>(u >> 64);
  return int128_t{lo, hi};
}

static std::string to_dec(const int128_t &v)
{
  char buf[64] = {0};
  int64_t pos = 0;
  if (OB_SUCCESS != wide::to_string(v, buf, sizeof(buf), pos)) {
    return std::string("<to_string failed>");
  }
  return std::string(buf, pos);
}
#endif // __ARM_NEON

#if defined(__ARM_NEON)
// int32 -> int128 (NEON widening)
TEST_F(SumI128KernelTest, NeonI32_Correctness)
{
  std::mt19937_64 rng(20260529);
  std::uniform_int_distribution<int32_t> dist(INT32_MIN, INT32_MAX);
  // boundary sizes hit the 16-wide main loop + scalar tail
  const std::vector<int64_t> sizes = {0, 1, 2, 3, 5, 15, 16, 17, 31, 33, 64, 65, 255, 256, 1000};
  for (int64_t n : sizes) {
    std::vector<int32_t> data(n);
    __int128 golden = 0;
    for (int64_t i = 0; i < n; ++i) {
      data[i] = dist(rng);
      golden += data[i];
    }
    // fresh accumulator
    int128_t acc = 0;
    neon_sum_i32_to_i128(n > 0 ? data.data() : nullptr, n, acc);
    int128_t exp = make_i128(golden);
    ASSERT_TRUE(exp == acc) << "n=" << n << " exp=" << to_dec(exp) << " got=" << to_dec(acc);

    // non-zero starting accumulator (simulates multi-batch accumulation)
    __int128 seed = (__int128)1234567890123LL;
    int128_t acc2 = make_i128(seed);
    neon_sum_i32_to_i128(n > 0 ? data.data() : nullptr, n, acc2);
    int128_t exp2 = make_i128(seed + golden);
    ASSERT_TRUE(exp2 == acc2) << "seeded n=" << n;
  }
}

// int32 all-extreme values: ensure no int32+int32 overflow (widening keeps it exact)
TEST_F(SumI128KernelTest, NeonI32_ExtremeNoOverflow)
{
  const int64_t n = 70000;  // > batch, all INT32_MIN
  std::vector<int32_t> data(n, INT32_MIN);
  __int128 golden = (__int128)n * (__int128)INT32_MIN;
  int128_t acc = 0;
  neon_sum_i32_to_i128(data.data(), n, acc);
  ASSERT_TRUE(make_i128(golden) == acc) << "exp=" << to_dec(make_i128(golden)) << " got=" << to_dec(acc);
}
#endif // __ARM_NEON

// ===================== Part A: performance A/B =====================

#if defined(__ARM_NEON)
// Reference baseline: per-row int128 accumulation, exactly what util.h
// add_values does today (acc += (int128)v per row). NOT auto-vectorizable
// because operator+= is a multi-limb wide-integer op.
template <typename T>
static int128_t ref_scalar_sum(const T *data, int64_t n)
{
  int128_t acc = 0;
  for (int64_t i = 0; i < n; ++i) { acc += data[i]; }
  return acc;
}
#endif // __ARM_NEON

TEST_F(SumI128KernelTest, Perf)
{
#if defined(__ARM_NEON)
  const int64_t N = 1 << 20;   // 1M rows
  const int     K = 200;       // repetitions
  std::mt19937_64 rng(99);

  // ---- int32 -> int128 (NEON widening) ----
  {
    std::uniform_int_distribution<int32_t> dist(INT32_MIN, INT32_MAX);
    std::vector<int32_t> data(N);
    for (int64_t i = 0; i < N; ++i) { data[i] = dist(rng); }

    int128_t sink = 0;
    for (int w = 0; w < 3; ++w) { sink += ref_scalar_sum(data.data(), N); }   // warmup
    int64_t t0 = ObTimeUtility::current_time();
    for (int k = 0; k < K; ++k) { sink += ref_scalar_sum(data.data(), N); }
    int64_t t1 = ObTimeUtility::current_time();
    for (int k = 0; k < K; ++k) { int128_t a = 0; neon_sum_i32_to_i128(data.data(), N, a); sink += a; }
    int64_t t2 = ObTimeUtility::current_time();

    double base_ns = (double)(t1 - t0) * 1000.0 / (double)N / K;
    double opt_ns  = (double)(t2 - t1) * 1000.0 / (double)N / K;
    fprintf(stderr, "[PERF][i32->i128] baseline(int128+=)=%.3f ns/row  neon=%.3f ns/row  speedup=%.2fx  (sink=%s)\n",
            base_ns, opt_ns, base_ns / opt_ns, to_dec(sink).c_str());
    EXPECT_GT(t1 - t0, 0);
    EXPECT_GT(t2 - t1, 0);
    EXPECT_LT(t2 - t1, t1 - t0) << "NEON i32->i128 should beat scalar int128 accumulation";
  }
#endif // __ARM_NEON
}

}  // namespace unittest

// ===================== Part B: end-to-end via op_tests =====================
namespace sql
{

// Format a native __int128 as a base-10 string (the form a decimal(p,0) SUM
// result prints as), used to build a golden for large-volume cases. Negation
// is done in the unsigned domain to stay defined for INT128_MIN.
static std::string i128_to_dec(__int128 v)
{
  if (v == 0) { return std::string("0"); }
  const bool neg = v < 0;
  unsigned __int128 u = neg ? (~static_cast<unsigned __int128>(v) + 1)
                            : static_cast<unsigned __int128>(v);
  char tmp[48];
  int pos = 0;
  while (u > 0) { tmp[pos++] = static_cast<char>('0' + static_cast<int>(u % 10)); u /= 10; }
  std::string s;
  if (neg) { s.push_back('-'); }
  for (int i = pos - 1; i >= 0; --i) { s.push_back(tmp[i]); }
  return s;
}

class SumI128OpTest : public OpTestKit
{
protected:
  static constexpr int64_t DEC9_MAX = 999999999LL;  // max |value| for decimal(9,0)

  std::vector<TestRow> gen_i64(const std::vector<int64_t> &vals)
  {
    std::vector<TestRow> rows;
    for (int64_t v : vals) { rows.push_back({v}); }
    return rows;
  }
};

// decimal(9,0) -> stored as decint32, SUM widens to int128 path
TEST_F(SumI128OpTest, SumDecimal9_DualFormat)
{
  OpTestResult r = ScalarAggTestSpec()
      .table("t", "a decimal(9,0)")
      .select("SUM(a)")
      .with_data(gen_i64({1, 2, 3, 4, 5, -10, 999999999, -999999999}))
      .enable_dual_format_check().run(engine_);
  EXPECT_EQ(1, r.row_count());
  // 1+2+3+4+5-10 = 5 ; +999999999-999999999 = 5
  EXPECT_TRUE(r.verify_ordered({{"5"}}));
}

// NULL present -> fast path bypassed, falls back to base; result still correct
TEST_F(SumI128OpTest, SumDecimal9_WithNull)
{
  OpTestResult r = ScalarAggTestSpec()
      .table("t", "a decimal(9,0)")
      .select("SUM(a)")
      .with_data({{int64_t(10)}, {TestValue::null()}, {int64_t(20)},
                  {TestValue::null()}, {int64_t(30)}})
      .enable_dual_format_check().run(engine_);
  EXPECT_EQ(1, r.row_count());
  EXPECT_TRUE(r.verify_ordered({{"60"}}));
}

// ---- large-volume cases (decimal(9,0) -> decint32 -> int128) --------------
// These exercise the NEON 16-wide main loop many times over, cross-batch
// int128 accumulation, sign extension, and non-16-aligned per-batch tails.
// enable_dual_format_check() cross-checks legacy-scalar vs vec on every arch;
// the host __int128 golden is the absolute oracle.

// 131072 = 2*65536 rows, all at decimal(9,0) max -> well past int64 main-loop
// lane range, forcing the full NEON 16-wide path repeatedly.
TEST_F(SumI128OpTest, SumDecimal9_LargeVolume_AllMax)
{
  const int64_t n = 131072;
  std::vector<int64_t> vals(n, DEC9_MAX);
  const __int128 golden = static_cast<__int128>(n) * static_cast<__int128>(DEC9_MAX);
  OpTestResult r = ScalarAggTestSpec()
      .table("t", "a decimal(9,0)")
      .select("SUM(a)")
      .with_data(gen_i64(vals))
      .enable_dual_format_check().run(engine_);
  EXPECT_EQ(1, r.row_count());
  EXPECT_TRUE(r.verify_ordered({{i128_to_dec(golden)}}));
}

// Alternating +max/-max with an odd row count: stresses sign extension in the
// widening add and leaves a scalar tail (n % 16 != 0).
TEST_F(SumI128OpTest, SumDecimal9_LargeVolume_MixedSign)
{
  const int64_t n = 100003;  // prime-ish, not a multiple of 16
  std::vector<int64_t> vals;
  vals.reserve(n);
  __int128 golden = 0;
  for (int64_t i = 0; i < n; ++i) {
    int64_t v = (i % 2 == 0) ? DEC9_MAX : -DEC9_MAX;
    vals.push_back(v);
    golden += v;
  }
  OpTestResult r = ScalarAggTestSpec()
      .table("t", "a decimal(9,0)")
      .select("SUM(a)")
      .with_data(gen_i64(vals))
      .enable_dual_format_check().run(engine_);
  EXPECT_EQ(1, r.row_count());
  EXPECT_TRUE(r.verify_ordered({{i128_to_dec(golden)}}));  // == DEC9_MAX (one unmatched +max)
}

// Large random fill across the full decimal(9,0) value range; host __int128 is
// the golden. Strong randomized oracle at volume.
TEST_F(SumI128OpTest, SumDecimal9_LargeVolume_Random)
{
  const int64_t n = 131072;
  std::mt19937_64 rng(20260602);
  std::uniform_int_distribution<int64_t> dist(-DEC9_MAX, DEC9_MAX);
  std::vector<int64_t> vals;
  vals.reserve(n);
  __int128 golden = 0;
  for (int64_t i = 0; i < n; ++i) {
    int64_t v = dist(rng);
    vals.push_back(v);
    golden += v;
  }
  OpTestResult r = ScalarAggTestSpec()
      .table("t", "a decimal(9,0)")
      .select("SUM(a)")
      .with_data(gen_i64(vals))
      .enable_dual_format_check().run(engine_);
  EXPECT_EQ(1, r.row_count());
  EXPECT_TRUE(r.verify_ordered({{i128_to_dec(golden)}}));
}

// Same large random data but a small, non-power-of-two batch size: forces many
// cross-batch flushes into int128 and a per-batch scalar tail every batch.
TEST_F(SumI128OpTest, SumDecimal9_LargeVolume_SmallBatch)
{
  const int64_t n = 50000;
  std::mt19937_64 rng(0xBADC0DE);
  std::uniform_int_distribution<int64_t> dist(-DEC9_MAX, DEC9_MAX);
  std::vector<int64_t> vals;
  vals.reserve(n);
  __int128 golden = 0;
  for (int64_t i = 0; i < n; ++i) {
    int64_t v = dist(rng);
    vals.push_back(v);
    golden += v;
  }
  OpTestResult r = ScalarAggTestSpec()
      .table("t", "a decimal(9,0)")
      .select("SUM(a)")
      .with_batch_size(7)
      .with_data(gen_i64(vals))
      .enable_dual_format_check().run(engine_);
  EXPECT_EQ(1, r.row_count());
  EXPECT_TRUE(r.verify_ordered({{i128_to_dec(golden)}}));
}

}  // namespace sql
}  // namespace oceanbase

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
