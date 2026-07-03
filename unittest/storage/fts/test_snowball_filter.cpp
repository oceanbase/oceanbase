/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

// put top to use macro tricks
#include "mtlenv/mock_tenant_module_env.h"
// put top to use macro tricks

#include "lib/allocator/page_arena.h"
#include "lib/ob_errno.h"
#include "storage/fts/analyzer/filter/ob_snowball_filter.h"
#include "unittest/storage/fts/test_analyzer_helper.h"

#include <gtest/gtest.h>

#define USING_LOG_PREFIX STORAGE_FTS

namespace oceanbase
{
namespace storage
{

class FTSnowballFilterTest : public ::testing::Test
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

TEST_F(FTSnowballFilterTest, basic_cases)
{
  common::ObArenaAllocator allocator(ObModIds::TEST);
  ObSnowballFilterSpec spec(ObSnowballFilterSpec::Algorithm::ENGLISH);
  ObSnowballFilter filter;
  ASSERT_EQ(OB_SUCCESS, filter.init(spec, allocator));

  FTAnalyzerTestHelper::assert_token_filter_output(
      "english conjugation and declension",
      filter,
      {"running", "jumps", "tested", "knits", "refinery", "observatories", "presidents'", "president's"},
      {"run", "jump", "test", "knit", "refineri", "observatori", "presid", "presid"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "english verb-derived nouns",
      filter,
      {"development", "information", "appearance", "actor", "engineering", "refusal", "pressure", "failure", "simplification"},
      {"develop", "inform", "appear", "actor", "engin", "refus", "pressur", "failur", "simplif"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "english adjective-derived nouns",
      filter,
      {"happiness", "possibility", "safety", "width", "accuracy", "independence"},
      {"happi", "possibl", "safeti", "width", "accuraci", "independ"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "english noun-derived adjectives and adverbs",
      filter,
      {"hopeful", "rainy", "actively", "faithfully", "dangerous", "comfortable", "flexible", "musical", "economical", "otherwise"},
      {"hope", "raini", "activ", "faith", "danger", "comfort", "flexibl", "music", "econom", "otherwis"});

  FTAnalyzerTestHelper::assert_token_filter_output(
      "english noun- and adjective-derived verbs",
      filter,
      {"modernize", "modernise", "simplify", "sharpen", "activate", "formulate"},
      {"modern", "modernis", "simplifi", "sharpen", "activ", "formul"});
}

TEST_F(FTSnowballFilterTest, multilingual_cases)
{
  common::ObArenaAllocator allocator(ObModIds::TEST);

  {
    ObSnowballFilterSpec spec(ObSnowballFilterSpec::Algorithm::FRENCH);
    ObSnowballFilter filter;
    ASSERT_EQ(OB_SUCCESS, filter.init(spec, allocator));

    FTAnalyzerTestHelper::assert_token_filter_output(
        "french",
        filter,
        {"étudiants", "mangera", "dangereuses"},
        {"étudi", "mang", "danger"});
  }

  {
    ObSnowballFilterSpec spec(ObSnowballFilterSpec::Algorithm::GERMAN);
    ObSnowballFilter filter;
    ASSERT_EQ(OB_SUCCESS, filter.init(spec, allocator));

    FTAnalyzerTestHelper::assert_token_filter_output(
        "german",
        filter,
        {"hauser", "kätzchen", "kategorisch"},
        {"haus", "katzch", "kategor"});
  }

  {
    ObSnowballFilterSpec spec(ObSnowballFilterSpec::Algorithm::RUSSIAN);
    ObSnowballFilter filter;
    ASSERT_EQ(OB_SUCCESS, filter.init(spec, allocator));

    FTAnalyzerTestHelper::assert_token_filter_output(
        "russian",
        filter,
        {"важному", "пагубная", "пакостей"},
        {"важн", "пагубн", "пакост"});
  }

  {
    ObSnowballFilterSpec spec(ObSnowballFilterSpec::Algorithm::INDONESIAN);
    ObSnowballFilter filter;
    ASSERT_EQ(OB_SUCCESS, filter.init(spec, allocator));

    FTAnalyzerTestHelper::assert_token_filter_output(
        "indonesian",
        filter,
        {"ekonominya", "perambatannya", "perakit"},
        {"ekonom", "ambat", "akit"});
  }

  {
    ObSnowballFilterSpec spec(ObSnowballFilterSpec::Algorithm::PORTER);
    ObSnowballFilter filter;
    ASSERT_EQ(OB_SUCCESS, filter.init(spec, allocator));

    FTAnalyzerTestHelper::assert_token_filter_output(
        "porter",
        filter,
        {"running", "relational", "happiness"},
        {"run", "relat", "happi"});
  }

  {
    for (int64_t i = 1; i < static_cast<int>(ObSnowballFilterSpec::Algorithm::MAX); ++i) {
      ObSnowballFilterSpec::Algorithm algo = static_cast<ObSnowballFilterSpec::Algorithm>(i);
      ObSnowballFilterSpec spec(algo);
      ObSnowballFilter filter;
      ASSERT_EQ(OB_SUCCESS, filter.init(spec, allocator)) << "algo=" << i;
    }
  }
}

TEST_F(FTSnowballFilterTest, error_cases)
{
  common::ObArenaAllocator allocator(ObModIds::TEST);
  ObTokenAttr token;

  {
    // invalid algorithm
    ObSnowballFilterSpec invalid_spec;
    ObSnowballFilter filter;
    EXPECT_EQ(OB_INVALID_ARGUMENT, filter.init(invalid_spec, allocator));
  }

  {
    // wrong spec type
    ObTokenFilterSpec bad_spec;
    ObSnowballFilter filter;
    bad_spec.type_ = ObTokenFilterType::TOKEN_FILTER_TYPE_LOWERCASE;
    EXPECT_EQ(OB_INVALID_ARGUMENT, filter.init(bad_spec, allocator));
  }

  {
    // init twice
    ObSnowballFilterSpec spec(ObSnowballFilterSpec::Algorithm::ENGLISH);
    ObSnowballFilter filter;
    ASSERT_EQ(OB_SUCCESS, filter.init(spec, allocator));
    EXPECT_EQ(OB_INIT_TWICE, filter.init(spec, allocator));
  }

  {
    // use before init
    MockTokenStream input({"running"});
    ObSnowballFilter filter;
    EXPECT_EQ(OB_NOT_INIT, filter.get_next_token(token));
    filter.set_input(&input);
    EXPECT_EQ(OB_NOT_INIT, filter.get_next_token(token));
  }

  {
    // use after reset
    ObSnowballFilterSpec spec(ObSnowballFilterSpec::Algorithm::ENGLISH);
    ObSnowballFilter filter;
    ASSERT_EQ(OB_SUCCESS, filter.init(spec, allocator));
    MockTokenStream input({"running", "jumps"});
    filter.set_input(&input);
    ASSERT_EQ(OB_SUCCESS, filter.get_next_token(token));
    EXPECT_EQ(std::string("run"), std::string(token.token_ptr_, token.token_len_));

    filter.reset();
    EXPECT_EQ(OB_NOT_INIT, filter.get_next_token(token));
  }

  {
    // get before set input
    ObSnowballFilterSpec spec(ObSnowballFilterSpec::Algorithm::ENGLISH);
    ObSnowballFilter filter;
    ASSERT_EQ(OB_SUCCESS, filter.init(spec, allocator));
    EXPECT_EQ(OB_ERR_UNEXPECTED, filter.get_next_token(token));
  }
}

TEST_F(FTSnowballFilterTest, edge_cases_should_succeed)
{
  common::ObArenaAllocator allocator(ObModIds::TEST);
  ObSnowballFilterSpec spec(ObSnowballFilterSpec::Algorithm::ENGLISH);
  ObTokenAttr token;

  {
    // empty input stream
    ObSnowballFilter filter;
    MockTokenStream input;
    ASSERT_EQ(OB_SUCCESS, filter.init(spec, allocator));
    filter.set_input(&input);
    EXPECT_EQ(OB_ITER_END, filter.get_next_token(token));
  }

  {
    // exhaust stream
    ObSnowballFilter filter;
    MockTokenStream input({"running", "jumps"});
    ASSERT_EQ(OB_SUCCESS, filter.init(spec, allocator));
    filter.set_input(&input);

    ASSERT_EQ(OB_SUCCESS, filter.get_next_token(token));
    EXPECT_EQ(std::string("run"), std::string(token.token_ptr_, token.token_len_));
    EXPECT_EQ(1, token.pos_inc_);
    EXPECT_FALSE(token.is_keyword_);

    ASSERT_EQ(OB_SUCCESS, filter.get_next_token(token));
    EXPECT_EQ(std::string("jump"), std::string(token.token_ptr_, token.token_len_));
    EXPECT_EQ(1, token.pos_inc_);
    EXPECT_FALSE(token.is_keyword_);

    EXPECT_EQ(OB_ITER_END, filter.get_next_token(token));
    EXPECT_EQ(OB_ITER_END, filter.get_next_token(token));
  }

  {
    // reentrant reset
    ObSnowballFilter filter;
    filter.reset();
    filter.reset();

    ASSERT_EQ(OB_SUCCESS, filter.init(spec, allocator));
    MockTokenStream input({"running"});
    filter.set_input(&input);

    ASSERT_EQ(OB_SUCCESS, filter.get_next_token(token));
    EXPECT_EQ(std::string("run"), std::string(token.token_ptr_, token.token_len_));
    EXPECT_EQ(OB_ITER_END, filter.get_next_token(token));

    filter.reset();
    filter.reset();
  }

  {
    // keyword token passes through unchanged
    ObSnowballFilter filter;
    MockTokenStream input;
    ASSERT_EQ(OB_SUCCESS, filter.init(spec, allocator));
    input.push_token("running", 1, true);
    filter.set_input(&input);

    ASSERT_EQ(OB_SUCCESS, filter.get_next_token(token));
    EXPECT_EQ(std::string("running"), std::string(token.token_ptr_, token.token_len_));
    EXPECT_EQ(1, token.pos_inc_);
    EXPECT_TRUE(token.is_keyword_);
    EXPECT_EQ(OB_ITER_END, filter.get_next_token(token));
  }

  {
    // two filters with different configs
    ObSnowballFilterSpec english_spec(ObSnowballFilterSpec::Algorithm::ENGLISH);
    ObSnowballFilter english_filter;
    MockTokenStream english_input({"running"});

    ObSnowballFilterSpec french_spec(ObSnowballFilterSpec::Algorithm::FRENCH);
    ObSnowballFilter french_filter;
    MockTokenStream french_input({"courir"});

    ASSERT_EQ(OB_SUCCESS, english_filter.init(english_spec, allocator));
    ASSERT_EQ(OB_SUCCESS, french_filter.init(french_spec, allocator));

    english_filter.set_input(&english_input);
    french_filter.set_input(&french_input);

    ASSERT_EQ(OB_SUCCESS, english_filter.get_next_token(token));
    EXPECT_EQ(std::string("run"), std::string(token.token_ptr_, token.token_len_));
    EXPECT_EQ(OB_ITER_END, english_filter.get_next_token(token));

    english_filter.reset();

    ASSERT_EQ(OB_SUCCESS, french_filter.get_next_token(token));
    EXPECT_EQ(std::string("cour"), std::string(token.token_ptr_, token.token_len_));
    EXPECT_EQ(OB_ITER_END, french_filter.get_next_token(token));
  }

  {
    // two filters with same config
    ObSnowballFilter filter1;
    ObSnowballFilter filter2;
    MockTokenStream input1({"running"});
    MockTokenStream input2({"jumps"});
    ASSERT_EQ(OB_SUCCESS, filter1.init(spec, allocator));
    ASSERT_EQ(OB_SUCCESS, filter2.init(spec, allocator));

    filter1.set_input(&input1);
    filter2.set_input(&input2);

    ASSERT_EQ(OB_SUCCESS, filter1.get_next_token(token));
    EXPECT_EQ(std::string("run"), std::string(token.token_ptr_, token.token_len_));
    EXPECT_EQ(OB_ITER_END, filter1.get_next_token(token));

    filter1.reset();

    ASSERT_EQ(OB_SUCCESS, filter2.get_next_token(token));
    EXPECT_EQ(std::string("jump"), std::string(token.token_ptr_, token.token_len_));
    EXPECT_EQ(OB_ITER_END, filter2.get_next_token(token));
  }
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
