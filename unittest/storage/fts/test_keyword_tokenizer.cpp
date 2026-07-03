/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

// put top to use macro tricks
#include "mtlenv/mock_tenant_module_env.h"
// put top to use macro tricks

#include "storage/fts/analyzer/tokenizer/ob_keyword_tokenizer.h"
#include "unittest/storage/fts/test_analyzer_helper.h"

#define USING_LOG_PREFIX STORAGE_FTS

namespace oceanbase
{
namespace storage
{

class FTKeywordTokenizerTest : public ::testing::Test
{
protected:
  static void SetUpTestCase()
  {
    LOG_INFO("SetUpTestCase");
    EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
  }

  static void TearDownTestCase()
  {
    LOG_INFO("TearDownTestCase");
    MockTenantModuleEnv::get_instance().destroy();
  }
};

TEST_F(FTKeywordTokenizerTest, basic_cases)
{
  common::ObArenaAllocator allocator(ObModIds::TEST);
  ObKeywordTokenizerSpec spec;

  ObKeywordTokenizer tokenizer;
  ASSERT_EQ(OB_SUCCESS, tokenizer.init(spec, allocator));

  FTAnalyzerTestHelper::assert_tokenizer_output(
      "ascii text",
      tokenizer,
      u8"OceanBase keyword tokenizer",
      {u8"OceanBase keyword tokenizer"});

  FTAnalyzerTestHelper::assert_tokenizer_output(
      "mixed punctuation and whitespace",
      tokenizer,
      u8"  hello, world! 123  ",
      {u8"  hello, world! 123  "});

  FTAnalyzerTestHelper::assert_tokenizer_output(
      "cjk and emoji",
      tokenizer,
      u8"中文检索😊日本語한국어",
      {u8"中文检索😊日本語한국어"});

  FTAnalyzerTestHelper::assert_tokenizer_output(
      "decomposed utf8 preserved",
      tokenizer,
      u8"café Ångström",
      {u8"café Ångström"});
}

TEST_F(FTKeywordTokenizerTest, error_cases)
{
  common::ObArenaAllocator allocator(ObModIds::TEST);
  ObTokenAttr token;

  {
    // init twice
    ObKeywordTokenizerSpec spec;
    ObKeywordTokenizer tokenizer;
    ASSERT_EQ(OB_SUCCESS, tokenizer.init(spec, allocator));
    EXPECT_EQ(OB_INIT_TWICE, tokenizer.init(spec, allocator));
  }

  {
    // use before init
    ObKeywordTokenizer tokenizer;
    EXPECT_EQ(OB_NOT_INIT, tokenizer.set_input(u8"hello", 5, CS_TYPE_UTF8MB4_BIN));
    EXPECT_EQ(OB_NOT_INIT, tokenizer.get_next_token(token));
  }

  {
    // invalid input arguments
    ObKeywordTokenizerSpec spec;
    ObKeywordTokenizer tokenizer;
    ASSERT_EQ(OB_SUCCESS, tokenizer.init(spec, allocator));

    EXPECT_EQ(OB_INVALID_ARGUMENT, tokenizer.set_input(nullptr, 0, CS_TYPE_UTF8MB4_BIN));
    EXPECT_EQ(OB_INVALID_ARGUMENT, tokenizer.set_input(u8"hello", -1, CS_TYPE_UTF8MB4_BIN));
    EXPECT_EQ(OB_INVALID_ARGUMENT, tokenizer.set_input(u8"hello", 5, CS_TYPE_UTF8MB4_GENERAL_CI));
  }

  {
    // invalid utf8
    const char invalid_utf8[] = {
        'a',
        static_cast<char>(0xF0),
        static_cast<char>(0x28),
        static_cast<char>(0x8C),
        static_cast<char>(0x28),
        'b'};
    ObKeywordTokenizerSpec spec;
    ObKeywordTokenizer tokenizer;
    ASSERT_EQ(OB_SUCCESS, tokenizer.init(spec, allocator));
    EXPECT_NE(OB_SUCCESS,
              tokenizer.set_input(invalid_utf8, sizeof(invalid_utf8), CS_TYPE_UTF8MB4_BIN));
  }

  {
    // use after reset
    ObKeywordTokenizerSpec spec;
    ObKeywordTokenizer tokenizer;
    ASSERT_EQ(OB_SUCCESS, tokenizer.init(spec, allocator));
    ASSERT_EQ(OB_SUCCESS, tokenizer.set_input(u8"hello", 5, CS_TYPE_UTF8MB4_BIN));
    ASSERT_EQ(OB_SUCCESS, tokenizer.get_next_token(token));

    tokenizer.reset();
    EXPECT_EQ(OB_NOT_INIT, tokenizer.get_next_token(token));
  }
}

TEST_F(FTKeywordTokenizerTest, edge_cases_should_succeed)
{
  common::ObArenaAllocator allocator(ObModIds::TEST);
  ObKeywordTokenizerSpec spec;
  ObTokenAttr token;

  {
    // empty input
    ObKeywordTokenizer tokenizer;
    ASSERT_EQ(OB_SUCCESS, tokenizer.init(spec, allocator));
    ASSERT_EQ(OB_SUCCESS, tokenizer.set_input(u8"", 0, CS_TYPE_UTF8MB4_BIN));
    EXPECT_EQ(OB_ITER_END, tokenizer.get_next_token(token));
    EXPECT_EQ(OB_ITER_END, tokenizer.get_next_token(token));
  }

  {
    // exhaust stream
    ObKeywordTokenizer tokenizer;
    ASSERT_EQ(OB_SUCCESS, tokenizer.init(spec, allocator));
    ASSERT_EQ(OB_SUCCESS, tokenizer.set_input(u8"single token", 12, CS_TYPE_UTF8MB4_BIN));

    ASSERT_EQ(OB_SUCCESS, tokenizer.get_next_token(token));
    EXPECT_EQ(std::string(u8"single token"), std::string(token.token_ptr_, token.token_len_));
    EXPECT_EQ(1, token.pos_inc_);
    EXPECT_FALSE(token.is_keyword_);

    EXPECT_EQ(OB_ITER_END, tokenizer.get_next_token(token));
    EXPECT_EQ(OB_ITER_END, tokenizer.get_next_token(token));
  }

  {
    // overwrite input before emission
    ObKeywordTokenizer tokenizer;
    ASSERT_EQ(OB_SUCCESS, tokenizer.init(spec, allocator));
    ASSERT_EQ(OB_SUCCESS, tokenizer.set_input(u8"first", 5, CS_TYPE_UTF8MB4_BIN));
    ASSERT_EQ(OB_SUCCESS, tokenizer.set_input(u8"second", 6, CS_TYPE_UTF8MB4_BIN));

    ASSERT_EQ(OB_SUCCESS, tokenizer.get_next_token(token));
    EXPECT_EQ(std::string(u8"second"), std::string(token.token_ptr_, token.token_len_));
    EXPECT_EQ(OB_ITER_END, tokenizer.get_next_token(token));
  }

  {
    // reentrant reset
    ObKeywordTokenizer tokenizer;
    tokenizer.reset();
    tokenizer.reset();

    ASSERT_EQ(OB_SUCCESS, tokenizer.init(spec, allocator));
    ASSERT_EQ(OB_SUCCESS, tokenizer.set_input(u8"reset ok", 8, CS_TYPE_UTF8MB4_BIN));
    ASSERT_EQ(OB_SUCCESS, tokenizer.get_next_token(token));
    EXPECT_EQ(std::string(u8"reset ok"), std::string(token.token_ptr_, token.token_len_));

    tokenizer.reset();
    tokenizer.reset();
  }
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
