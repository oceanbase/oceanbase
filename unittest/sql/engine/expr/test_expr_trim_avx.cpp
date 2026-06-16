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

// Helper function to generate string with leading/trailing spaces
static std::string gen_trim_test_string(int64_t leading_spaces, int64_t content_len, int64_t trailing_spaces) {
  std::string s;
  s.append(leading_spaces, ' ');
  s.append(content_len, 'a');
  s.append(trailing_spaces, ' ');
  return s;
}

// Reference implementation (pure C++)
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

// Test fixture
class TestTrimAvx : public ::testing::Test
{
public:
  TestTrimAvx() : allocator_(ObModIds::TEST) {}
  virtual void SetUp() {}
  virtual void TearDown() { allocator_.reset(); }

protected:
  ObArenaAllocator allocator_;
};

#if OB_USE_MULTITARGET_CODE

// Test ltrim with various lengths
TEST_F(TestTrimAvx, ltrim_correctness) {
  // Test empty string
  {
    ObString src(0, "");
    ObString pattern(1, " ");
    ObString result;
    EXPECT_EQ(OB_SUCCESS, ObExprTrim::trim(result, TRIM_LTRIM, pattern, src));
    EXPECT_EQ(0, result.length());
  }

  // Test single character - no trim
  {
    ObString src(1, "a");
    ObString pattern(1, " ");
    ObString result;
    EXPECT_EQ(OB_SUCCESS, ObExprTrim::trim(result, TRIM_LTRIM, pattern, src));
    EXPECT_EQ(1, result.length());
    EXPECT_EQ('a', result[0]);
  }

  // Test single character - trim all
  {
    ObString src(1, " ");
    ObString pattern(1, " ");
    ObString result;
    EXPECT_EQ(OB_SUCCESS, ObExprTrim::trim(result, TRIM_LTRIM, pattern, src));
    EXPECT_EQ(0, result.length());
  }

  // Test string < 16 bytes
  {
    std::string s = "   hello";
    ObString src(s.length(), s.c_str());
    ObString pattern(1, " ");
    ObString result;
    EXPECT_EQ(OB_SUCCESS, ObExprTrim::trim(result, TRIM_LTRIM, pattern, src));
    EXPECT_EQ(5, result.length());
    EXPECT_EQ(0, strncmp(result.ptr(), "hello", 5));
  }

  // Test string == 16 bytes (all spaces)
  {
    std::string s(16, ' ');
    ObString src(s.length(), s.c_str());
    ObString pattern(1, " ");
    ObString result;
    EXPECT_EQ(OB_SUCCESS, ObExprTrim::trim(result, TRIM_LTRIM, pattern, src));
    EXPECT_EQ(0, result.length());
  }

  // Test string == 16 bytes (mixed)
  {
    std::string s = "               a";  // 15 spaces + 'a'
    ObString src(s.length(), s.c_str());
    ObString pattern(1, " ");
    ObString result;
    EXPECT_EQ(OB_SUCCESS, ObExprTrim::trim(result, TRIM_LTRIM, pattern, src));
    EXPECT_EQ(1, result.length());
    EXPECT_EQ('a', result[0]);
  }

  // Test string > 16 bytes
  {
    std::string s = "                hello";  // 16 spaces + "hello"
    ObString src(s.length(), s.c_str());
    ObString pattern(1, " ");
    ObString result;
    EXPECT_EQ(OB_SUCCESS, ObExprTrim::trim(result, TRIM_LTRIM, pattern, src));
    EXPECT_EQ(5, result.length());
    EXPECT_EQ(0, strncmp(result.ptr(), "hello", 5));
  }

  // Test string with trim in middle of SIMD block
  {
    std::string s = "      hello world";  // 6 spaces + content
    ObString src(s.length(), s.c_str());
    ObString pattern(1, " ");
    ObString result;
    EXPECT_EQ(OB_SUCCESS, ObExprTrim::trim(result, TRIM_LTRIM, pattern, src));
    EXPECT_EQ(11, result.length());
    EXPECT_EQ(0, strncmp(result.ptr(), "hello world", 11));
  }

  // Test with different pattern character
  {
    std::string s = "xxxxabcxxx";
    ObString src(s.length(), s.c_str());
    ObString pattern(1, "x");
    ObString result;
    EXPECT_EQ(OB_SUCCESS, ObExprTrim::trim(result, TRIM_LTRIM, pattern, src));
    EXPECT_EQ(6, result.length());
    EXPECT_EQ(0, strncmp(result.ptr(), "abcxxx", 6));
  }
}

// Test rtrim with various lengths
TEST_F(TestTrimAvx, rtrim_correctness) {
  // Test empty string
  {
    ObString src(0, "");
    ObString pattern(1, " ");
    ObString result;
    EXPECT_EQ(OB_SUCCESS, ObExprTrim::trim(result, TRIM_RTRIM, pattern, src));
    EXPECT_EQ(0, result.length());
  }

  // Test single character - no trim
  {
    ObString src(1, "a");
    ObString pattern(1, " ");
    ObString result;
    EXPECT_EQ(OB_SUCCESS, ObExprTrim::trim(result, TRIM_RTRIM, pattern, src));
    EXPECT_EQ(1, result.length());
    EXPECT_EQ('a', result[0]);
  }

  // Test string < 16 bytes
  {
    std::string s = "hello   ";
    ObString src(s.length(), s.c_str());
    ObString pattern(1, " ");
    ObString result;
    EXPECT_EQ(OB_SUCCESS, ObExprTrim::trim(result, TRIM_RTRIM, pattern, src));
    EXPECT_EQ(5, result.length());
    EXPECT_EQ(0, strncmp(result.ptr(), "hello", 5));
  }

  // Test string == 16 bytes (all spaces)
  {
    std::string s(16, ' ');
    ObString src(s.length(), s.c_str());
    ObString pattern(1, " ");
    ObString result;
    EXPECT_EQ(OB_SUCCESS, ObExprTrim::trim(result, TRIM_RTRIM, pattern, src));
    EXPECT_EQ(0, result.length());
  }

  // Test string == 16 bytes (mixed)
  {
    std::string s = "a               ";  // 'a' + 15 spaces
    ObString src(s.length(), s.c_str());
    ObString pattern(1, " ");
    ObString result;
    EXPECT_EQ(OB_SUCCESS, ObExprTrim::trim(result, TRIM_RTRIM, pattern, src));
    EXPECT_EQ(1, result.length());
    EXPECT_EQ('a', result[0]);
  }

  // Test string > 16 bytes
  {
    std::string s = "hello                ";  // "hello" + 16 spaces
    ObString src(s.length(), s.c_str());
    ObString pattern(1, " ");
    ObString result;
    EXPECT_EQ(OB_SUCCESS, ObExprTrim::trim(result, TRIM_RTRIM, pattern, src));
    EXPECT_EQ(5, result.length());
    EXPECT_EQ(0, strncmp(result.ptr(), "hello", 5));
  }

  // Test with different pattern character
  {
    std::string s = "abcxxxxxxx";
    ObString src(s.length(), s.c_str());
    ObString pattern(1, "x");
    ObString result;
    EXPECT_EQ(OB_SUCCESS, ObExprTrim::trim(result, TRIM_RTRIM, pattern, src));
    EXPECT_EQ(3, result.length());
    EXPECT_EQ(0, strncmp(result.ptr(), "abc", 3));
  }
}

// Test lrtrim combination
TEST_F(TestTrimAvx, lrtrim_correctness) {
  // Test string with both leading and trailing spaces
  {
    std::string s = "   hello world   ";
    ObString src(s.length(), s.c_str());
    ObString pattern(1, " ");
    ObString result;
    EXPECT_EQ(OB_SUCCESS, ObExprTrim::trim(result, TRIM_LRTRIM, pattern, src));
    EXPECT_EQ(11, result.length());
    EXPECT_EQ(0, strncmp(result.ptr(), "hello world", 11));
  }

  // Test all spaces
  {
    std::string s(32, ' ');
    ObString src(s.length(), s.c_str());
    ObString pattern(1, " ");
    ObString result;
    EXPECT_EQ(OB_SUCCESS, ObExprTrim::trim(result, TRIM_LRTRIM, pattern, src));
    EXPECT_EQ(0, result.length());
  }

  // Test no trim needed
  {
    std::string s = "hello";
    ObString src(s.length(), s.c_str());
    ObString pattern(1, " ");
    ObString result;
    EXPECT_EQ(OB_SUCCESS, ObExprTrim::trim(result, TRIM_LRTRIM, pattern, src));
    EXPECT_EQ(5, result.length());
    EXPECT_EQ(0, strncmp(result.ptr(), "hello", 5));
  }
}

// Compare with reference implementation for random strings
TEST_F(TestTrimAvx, compare_with_reference) {
  const int NUM_TESTS = 1000;
  const int MAX_LEN = 256;

  srand(12345);  // Fixed seed for reproducibility

  for (int t = 0; t < NUM_TESTS; t++) {
    int len = rand() % MAX_LEN;
    int leading = rand() % (len + 1);
    int remaining = len - leading;
    int trailing = remaining > 0 ? rand() % remaining : 0;

    std::string s = gen_trim_test_string(leading, remaining - trailing, trailing);
    ObString src(s.length(), s.c_str());
    ObString pattern(1, " ");

    // Test ltrim
    ObString avx_result;
    EXPECT_EQ(OB_SUCCESS, ObExprTrim::trim(avx_result, TRIM_LTRIM, pattern, src));
    std::string ref_result = ref_trim(s, ' ', true, false);
    EXPECT_EQ(ref_result.length(), avx_result.length())
        << "ltrim length mismatch for string: '" << s << "'";
    EXPECT_EQ(0, strncmp(avx_result.ptr(), ref_result.c_str(), avx_result.length()))
        << "ltrim content mismatch for string: '" << s << "'";

    allocator_.reuse();

    // Test rtrim
    EXPECT_EQ(OB_SUCCESS, ObExprTrim::trim(avx_result, TRIM_RTRIM, pattern, src));
    ref_result = ref_trim(s, ' ', false, true);
    EXPECT_EQ(ref_result.length(), avx_result.length())
        << "rtrim length mismatch for string: '" << s << "'";
    EXPECT_EQ(0, strncmp(avx_result.ptr(), ref_result.c_str(), avx_result.length()))
        << "rtrim content mismatch for string: '" << s << "'";

    allocator_.reuse();

    // Test lrtrim
    EXPECT_EQ(OB_SUCCESS, ObExprTrim::trim(avx_result, TRIM_LRTRIM, pattern, src));
    ref_result = ref_trim(s, ' ', true, true);
    EXPECT_EQ(ref_result.length(), avx_result.length())
        << "lrtrim length mismatch for string: '" << s << "'";
    EXPECT_EQ(0, strncmp(avx_result.ptr(), ref_result.c_str(), avx_result.length()))
        << "lrtrim content mismatch for string: '" << s << "'";

    allocator_.reuse();
  }
}

// Edge cases
TEST_F(TestTrimAvx, edge_cases) {
  // Test very long string
  {
    std::string s(10000, ' ');
    s += "content";
    s += std::string(10000, ' ');
    ObString src(s.length(), s.c_str());
    ObString pattern(1, " ");
    ObString result;
    EXPECT_EQ(OB_SUCCESS, ObExprTrim::trim(result, TRIM_LRTRIM, pattern, src));
    EXPECT_EQ(7, result.length());
    EXPECT_EQ(0, strncmp(result.ptr(), "content", 7));
  }

  // Test string exactly 32 bytes (2 SIMD blocks)
  {
    std::string s(32, ' ');
    ObString src(s.length(), s.c_str());
    ObString pattern(1, " ");
    ObString result;
    EXPECT_EQ(OB_SUCCESS, ObExprTrim::trim(result, TRIM_LTRIM, pattern, src));
    EXPECT_EQ(0, result.length());
  }

  // Test string with content spanning multiple SIMD blocks
  {
    std::string spaces(48, ' ');  // 3 SIMD blocks of spaces
    std::string s = spaces + "content" + spaces;
    ObString src(s.length(), s.c_str());
    ObString pattern(1, " ");
    ObString result;
    EXPECT_EQ(OB_SUCCESS, ObExprTrim::trim(result, TRIM_LRTRIM, pattern, src));
    EXPECT_EQ(7, result.length());
    EXPECT_EQ(0, strncmp(result.ptr(), "content", 7));
  }
}

// Test boundary conditions around SIMD block sizes
TEST_F(TestTrimAvx, simd_boundary_tests) {
  // Test strings with length just under, at, and over 16 bytes
  for (int len = 14; len <= 18; ++len) {
    // All spaces
    {
      std::string s(len, ' ');
      ObString src(s.length(), s.c_str());
      ObString pattern(1, " ");
      ObString result;
      EXPECT_EQ(OB_SUCCESS, ObExprTrim::trim(result, TRIM_LTRIM, pattern, src));
      EXPECT_EQ(0, result.length()) << "Failed for length " << len;
    }

    // One non-space at start
    {
      std::string s(len, ' ');
      s[0] = 'x';
      ObString src(s.length(), s.c_str());
      ObString pattern(1, " ");
      ObString result;
      EXPECT_EQ(OB_SUCCESS, ObExprTrim::trim(result, TRIM_LTRIM, pattern, src));
      EXPECT_EQ(len, result.length()) << "Failed for length " << len;
    }

    // One non-space at end
    {
      std::string s(len, ' ');
      s[len - 1] = 'x';
      ObString src(s.length(), s.c_str());
      ObString pattern(1, " ");
      ObString result;
      EXPECT_EQ(OB_SUCCESS, ObExprTrim::trim(result, TRIM_LTRIM, pattern, src));
      EXPECT_EQ(1, result.length()) << "Failed for length " << len;
      EXPECT_EQ('x', result[0]);
    }

    allocator_.reuse();
  }
}

// Test with non-space pattern
TEST_F(TestTrimAvx, custom_pattern_tests) {
  // Pattern 'x'
  {
    std::string s = "xxxxxhelloxxxxx";
    ObString src(s.length(), s.c_str());
    ObString pattern(1, "x");
    ObString result;
    EXPECT_EQ(OB_SUCCESS, ObExprTrim::trim(result, TRIM_LRTRIM, pattern, src));
    EXPECT_EQ(5, result.length());
    EXPECT_EQ(0, strncmp(result.ptr(), "hello", 5));
  }

  // Pattern '0' (digit)
  {
    std::string s = "0000012345678900000";
    ObString src(s.length(), s.c_str());
    ObString pattern(1, "0");
    ObString result;
    EXPECT_EQ(OB_SUCCESS, ObExprTrim::trim(result, TRIM_LRTRIM, pattern, src));
    EXPECT_EQ(9, result.length());
    EXPECT_EQ(0, strncmp(result.ptr(), "123456789", 9));
  }
}

#else
#endif // OB_USE_MULTITARGET_CODE

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
