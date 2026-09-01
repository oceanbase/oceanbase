/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL

#include <gtest/gtest.h>
#include <cstdlib>
#include <cstring>
#include <cstdio>
#include <chrono>
#include <iostream>
#include <iomanip>
#define private public
#define protected public

#include "sql/engine/expr/ob_expr_trim.h"
#include "common/ob_target_specific.h"
#include "lib/oblog/ob_log_module.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sql;

// Trim type constants
static const int64_t TRIM_LTRIM = ObExprLtrim::TYPE_LTRIM;
static const int64_t TRIM_RTRIM = ObExprLtrim::TYPE_RTRIM;
static const int64_t TRIM_LRTRIM = ObExprLtrim::TYPE_LRTRIM;

// Helper for timing
class Timer {
public:
  Timer() : start_(std::chrono::high_resolution_clock::now()) {}

  double elapsed_ms() const {
    auto end = std::chrono::high_resolution_clock::now();
    return std::chrono::duration<double, std::milli>(end - start_).count();
  }

  void reset() {
    start_ = std::chrono::high_resolution_clock::now();
  }

private:
  std::chrono::high_resolution_clock::time_point start_;
};

// Generate test string
static std::string gen_test_string(int64_t leading_spaces, int64_t content_len, int64_t trailing_spaces) {
  std::string s;
  s.append(leading_spaces, ' ');
  s.append(content_len, 'a');
  s.append(trailing_spaces, ' ');
  return s;
}

// Reference implementation (pure C++ trim)
static std::string ref_trim(const std::string &src, char pattern, bool trim_leading, bool trim_trailing) {
  int32_t start = 0;
  int32_t end = src.length();

  if (trim_leading) {
    while (start < end && src[start] == pattern) {
      start++;
    }
  }
  if (trim_trailing) {
    while (end > start && src[end - 1] == pattern) {
      end--;
    }
  }
  return src.substr(start, end - start);
}

class TestTrimPerf : public ::testing::Test
{
public:
  TestTrimPerf() : allocator_(ObModIds::TEST) {}
  virtual void SetUp() {
    // Check if NEON is supported at runtime
    init_arches();
    std::cout << "\n=== Runtime Check ===" << std::endl;
    std::cout << "NEON supported: " << is_arch_supported(ObTargetArch::NEON) << std::endl;
  }
  virtual void TearDown() { allocator_.reset(); }

protected:
  ObArenaAllocator allocator_;
};

#if defined(__ARM_NEON)

// Performance test for ltrim
TEST_F(TestTrimPerf, ltrim_performance) {
  const int64_t iterations = 100000;

  std::cout << "\n=== LTRIM Performance Test ===" << std::endl;
  std::cout << std::setw(15) << "String Size"
            << std::setw(15) << "Scalar(ms)"
            << std::setw(15) << "NEON(ms)"
            << std::setw(15) << "Speedup" << std::endl;
  std::cout << std::string(60, '-') << std::endl;

  // Test different string sizes
  std::vector<int64_t> sizes = {64, 256, 1024, 4096, 16384};

  for (int64_t size : sizes) {
    // Generate string with 25% leading spaces, 50% content, 25% trailing spaces
    int64_t leading = size / 4;
    int64_t content = size / 2;
    int64_t trailing = size / 4;
    std::string s = gen_test_string(leading, content, trailing);
    ObString src(s.length(), s.c_str());
    ObString pattern(1, " ");

    // Warm up
    ObString result;
    for (int i = 0; i < 1000; i++) {
      ObExprTrim::trim(result, TRIM_LTRIM, pattern, src);
      allocator_.reuse();
    }

    // NEON benchmark (uses optimized path via ObExprTrim::trim)
    Timer timer;
    for (int64_t i = 0; i < iterations; i++) {
      ObExprTrim::trim(result, TRIM_LTRIM, pattern, src);
      allocator_.reuse();
    }
    double neon_ms = timer.elapsed_ms();

    // Scalar reference benchmark
    timer.reset();
    for (int64_t i = 0; i < iterations; i++) {
      std::string r = ref_trim(s, ' ', true, false);
      (void)r;
    }
    double scalar_ms = timer.elapsed_ms();

    double speedup = scalar_ms / neon_ms;

    std::cout << std::setw(15) << size
              << std::setw(15) << std::fixed << std::setprecision(2) << scalar_ms
              << std::setw(15) << neon_ms
              << std::setw(14) << speedup << "x" << std::endl;
  }
}

// Performance test for rtrim
TEST_F(TestTrimPerf, rtrim_performance) {
  const int64_t iterations = 100000;

  std::cout << "\n=== RTRIM Performance Test ===" << std::endl;
  std::cout << std::setw(15) << "String Size"
            << std::setw(15) << "Scalar(ms)"
            << std::setw(15) << "NEON(ms)"
            << std::setw(15) << "Speedup" << std::endl;
  std::cout << std::string(60, '-') << std::endl;

  std::vector<int64_t> sizes = {64, 256, 1024, 4096, 16384};

  for (int64_t size : sizes) {
    int64_t leading = size / 4;
    int64_t content = size / 2;
    int64_t trailing = size / 4;
    std::string s = gen_test_string(leading, content, trailing);
    ObString src(s.length(), s.c_str());
    ObString pattern(1, " ");

    // Warm up
    ObString result;
    for (int i = 0; i < 1000; i++) {
      ObExprTrim::trim(result, TRIM_RTRIM, pattern, src);
      allocator_.reuse();
    }

    // NEON benchmark
    Timer timer;
    for (int64_t i = 0; i < iterations; i++) {
      ObExprTrim::trim(result, TRIM_RTRIM, pattern, src);
      allocator_.reuse();
    }
    double neon_ms = timer.elapsed_ms();

    // Scalar reference benchmark
    timer.reset();
    for (int64_t i = 0; i < iterations; i++) {
      std::string r = ref_trim(s, ' ', false, true);
      (void)r;
    }
    double scalar_ms = timer.elapsed_ms();

    double speedup = scalar_ms / neon_ms;

    std::cout << std::setw(15) << size
              << std::setw(15) << std::fixed << std::setprecision(2) << scalar_ms
              << std::setw(15) << neon_ms
              << std::setw(14) << speedup << "x" << std::endl;
  }
}

// Performance test for lrtrim (both leading and trailing)
TEST_F(TestTrimPerf, lrtrim_performance) {
  const int64_t iterations = 100000;

  std::cout << "\n=== LRTRIM Performance Test ===" << std::endl;
  std::cout << std::setw(15) << "String Size"
            << std::setw(15) << "Scalar(ms)"
            << std::setw(15) << "NEON(ms)"
            << std::setw(15) << "Speedup" << std::endl;
  std::cout << std::string(60, '-') << std::endl;

  std::vector<int64_t> sizes = {64, 256, 1024, 4096, 16384};

  for (int64_t size : sizes) {
    int64_t leading = size / 4;
    int64_t content = size / 2;
    int64_t trailing = size / 4;
    std::string s = gen_test_string(leading, content, trailing);
    ObString src(s.length(), s.c_str());
    ObString pattern(1, " ");

    // Warm up
    ObString result;
    for (int i = 0; i < 1000; i++) {
      ObExprTrim::trim(result, TRIM_LRTRIM, pattern, src);
      allocator_.reuse();
    }

    // NEON benchmark
    Timer timer;
    for (int64_t i = 0; i < iterations; i++) {
      ObExprTrim::trim(result, TRIM_LRTRIM, pattern, src);
      allocator_.reuse();
    }
    double neon_ms = timer.elapsed_ms();

    // Scalar reference benchmark
    timer.reset();
    for (int64_t i = 0; i < iterations; i++) {
      std::string r = ref_trim(s, ' ', true, true);
      (void)r;
    }
    double scalar_ms = timer.elapsed_ms();

    double speedup = scalar_ms / neon_ms;

    std::cout << std::setw(15) << size
              << std::setw(15) << std::fixed << std::setprecision(2) << scalar_ms
              << std::setw(15) << neon_ms
              << std::setw(14) << speedup << "x" << std::endl;
  }
}

// Performance test with different space ratios
TEST_F(TestTrimPerf, ltrim_different_ratios) {
  const int64_t iterations = 100000;
  const int64_t size = 1024;

  std::cout << "\n=== LTRIM Performance with Different Space Ratios (size=" << size << ") ===" << std::endl;
  std::cout << std::setw(15) << "Space Ratio"
            << std::setw(15) << "Scalar(ms)"
            << std::setw(15) << "NEON(ms)"
            << std::setw(15) << "Speedup" << std::endl;
  std::cout << std::string(60, '-') << std::endl;

  std::vector<double> ratios = {0.0, 0.1, 0.25, 0.5, 0.75, 1.0};

  for (double ratio : ratios) {
    int64_t leading = static_cast<int64_t>(size * ratio);
    int64_t content = size - leading;
    std::string s = gen_test_string(leading, content, 0);
    ObString src(s.length(), s.c_str());
    ObString pattern(1, " ");

    // Warm up
    ObString result;
    for (int i = 0; i < 1000; i++) {
      ObExprTrim::trim(result, TRIM_LTRIM, pattern, src);
      allocator_.reuse();
    }

    // NEON benchmark
    Timer timer;
    for (int64_t i = 0; i < iterations; i++) {
      ObExprTrim::trim(result, TRIM_LTRIM, pattern, src);
      allocator_.reuse();
    }
    double neon_ms = timer.elapsed_ms();

    // Scalar reference benchmark
    timer.reset();
    for (int64_t i = 0; i < iterations; i++) {
      std::string r = ref_trim(s, ' ', true, false);
      (void)r;
    }
    double scalar_ms = timer.elapsed_ms();

    double speedup = scalar_ms / neon_ms;

    std::cout << std::setw(14) << (ratio * 100) << "%"
              << std::setw(15) << std::fixed << std::setprecision(2) << scalar_ms
              << std::setw(15) << neon_ms
              << std::setw(14) << speedup << "x" << std::endl;
  }
}

#else
// Non-NEON platform: placeholder test
TEST_F(TestTrimPerf, neon_not_available) {
  GTEST_SKIP() << "NEON not available on this platform";
}
#endif // __ARM_NEON

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}