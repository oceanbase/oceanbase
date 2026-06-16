/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include "sql/engine/expr/ob_expr_string_searcher.h"
#include "lib/oblog/ob_log.h"

using namespace oceanbase::common;

// Test fixture for StringSearcher tests
class StringSearcherTest : public ::testing::Test
{
protected:
  virtual void SetUp() {}
  virtual void TearDown() {}
};

#if defined(__x86_64__) && defined(OB_USE_MULTITARGET_CODE)
// AVX2 implementation tests
TEST_F(StringSearcherTest, AVX2_Init)
{
  specific::avx2::ObStringSearcher searcher;
  const char *pattern = "test";
  int ret = searcher.init(pattern, 4);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(4u, searcher.get_pattern_length());
}

TEST_F(StringSearcherTest, AVX2_IsSubstring_Basic)
{
  specific::avx2::ObStringSearcher searcher;
  const char *pattern = "test";
  searcher.init(pattern, 4);

  const char *text = "this is a test string";
  bool res = false;
  int ret = searcher.is_substring(text, text + strlen(text), res);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_TRUE(res);
}

TEST_F(StringSearcherTest, AVX2_IsSubstring_NotFound)
{
  specific::avx2::ObStringSearcher searcher;
  const char *pattern = "xyz";
  searcher.init(pattern, 3);

  const char *text = "this is a test string";
  bool res = false;
  int ret = searcher.is_substring(text, text + strlen(text), res);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_FALSE(res);
}

TEST_F(StringSearcherTest, AVX2_StartWith)
{
  specific::avx2::ObStringSearcher searcher;
  const char *pattern = "hello";
  searcher.init(pattern, 5);

  const char *text = "hello world";
  bool res = false;
  int ret = searcher.start_with(text, text + strlen(text), res);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_TRUE(res);
}

TEST_F(StringSearcherTest, AVX2_EndWith)
{
  specific::avx2::ObStringSearcher searcher;
  const char *pattern = "world";
  searcher.init(pattern, 5);

  const char *text = "hello world";
  bool res = false;
  int ret = searcher.end_with(text, text + strlen(text), res);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_TRUE(res);
}

TEST_F(StringSearcherTest, AVX2_Equal)
{
  specific::avx2::ObStringSearcher searcher;
  const char *pattern = "hello";
  searcher.init(pattern, 5);

  const char *text = "hello";
  bool res = false;
  int ret = searcher.equal(text, text + strlen(text), res);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_TRUE(res);
}

TEST_F(StringSearcherTest, AVX2_SingleChar)
{
  specific::avx2::ObStringSearcher searcher;
  const char *pattern = "a";
  searcher.init(pattern, 1);

  const char *text = "banana";
  bool res = false;
  int ret = searcher.is_substring(text, text + strlen(text), res);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_TRUE(res);
}

TEST_F(StringSearcherTest, AVX2_Instr)
{
  specific::avx2::ObStringSearcher searcher;
  const char *pattern = "test";
  searcher.init(pattern, 4);

  const char *text = "this is a test string";
  int64_t pos = -1;
  bool find = false;
  int ret = searcher.instr(text, text + strlen(text), pos, find);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_TRUE(find);
  EXPECT_EQ(10, pos);
}
#endif

#if defined(__aarch64__) && defined(__ARM_NEON)
// NEON implementation tests
TEST_F(StringSearcherTest, NEON_Init)
{
  specific::neon::ObStringSearcher searcher;
  const char *pattern = "test";
  int ret = searcher.init(pattern, 4);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(4u, searcher.get_pattern_length());
}

TEST_F(StringSearcherTest, NEON_IsSubstring_Basic)
{
  specific::neon::ObStringSearcher searcher;
  const char *pattern = "test";
  searcher.init(pattern, 4);

  const char *text = "this is a test string";
  bool res = false;
  int ret = searcher.is_substring(text, text + strlen(text), res);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_TRUE(res);
}

TEST_F(StringSearcherTest, NEON_IsSubstring_NotFound)
{
  specific::neon::ObStringSearcher searcher;
  const char *pattern = "xyz";
  searcher.init(pattern, 3);

  const char *text = "this is a test string";
  bool res = false;
  int ret = searcher.is_substring(text, text + strlen(text), res);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_FALSE(res);
}

TEST_F(StringSearcherTest, NEON_StartWith)
{
  specific::neon::ObStringSearcher searcher;
  const char *pattern = "hello";
  searcher.init(pattern, 5);

  const char *text = "hello world";
  bool res = false;
  int ret = searcher.start_with(text, text + strlen(text), res);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_TRUE(res);
}

TEST_F(StringSearcherTest, NEON_EndWith)
{
  specific::neon::ObStringSearcher searcher;
  const char *pattern = "world";
  searcher.init(pattern, 5);

  const char *text = "hello world";
  bool res = false;
  int ret = searcher.end_with(text, text + strlen(text), res);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_TRUE(res);
}

TEST_F(StringSearcherTest, NEON_Equal)
{
  specific::neon::ObStringSearcher searcher;
  const char *pattern = "hello";
  searcher.init(pattern, 5);

  const char *text = "hello";
  bool res = false;
  int ret = searcher.equal(text, text + strlen(text), res);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_TRUE(res);
}

TEST_F(StringSearcherTest, NEON_SingleChar)
{
  specific::neon::ObStringSearcher searcher;
  const char *pattern = "a";
  searcher.init(pattern, 1);

  const char *text = "banana";
  bool res = false;
  int ret = searcher.is_substring(text, text + strlen(text), res);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_TRUE(res);
}

TEST_F(StringSearcherTest, NEON_Instr)
{
  specific::neon::ObStringSearcher searcher;
  const char *pattern = "test";
  searcher.init(pattern, 4);

  const char *text = "this is a test string";
  int64_t pos = -1;
  bool find = false;
  int ret = searcher.instr(text, text + strlen(text), pos, find);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_TRUE(find);
  EXPECT_EQ(10, pos);
}

TEST_F(StringSearcherTest, NEON_LongText)
{
  specific::neon::ObStringSearcher searcher;
  const char *pattern = "pattern";
  searcher.init(pattern, 7);

  // Create a long text to test NEON vectorized path
  char text[256];
  memset(text, 'a', sizeof(text) - 1);
  text[sizeof(text) - 1] = '\0';
  // Place pattern at position 100
  memcpy(text + 100, pattern, 7);

  bool res = false;
  int ret = searcher.is_substring(text, text + strlen(text), res);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_TRUE(res);
}
#endif

// UTF8 utility function tests
TEST_F(StringSearcherTest, UTF8_CharLength)
{
  // ASCII
  EXPECT_EQ(1u, utf8_char_length("a"));
  // 2-byte UTF-8
  EXPECT_EQ(2u, utf8_char_length("\xC3\xA9"));  // é
  // 3-byte UTF-8
  EXPECT_EQ(3u, utf8_char_length("\xE4\xB8\xAD"));  // 中
  // 4-byte UTF-8
  EXPECT_EQ(4u, utf8_char_length("\xF0\x9F\x98\x80"));  // 😀
}

TEST_F(StringSearcherTest, UTF8_LocConversion)
{
  const char *str = "hello";
  size_t len = strlen(str);

  int64_t locb;
  int ret = utf8_loc_to_locb(str, len, 3, locb);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(2, locb);

  uint32_t loc;
  ret = utf8_locb_to_loc(str, len, locb, loc);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(3u, loc);
}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}