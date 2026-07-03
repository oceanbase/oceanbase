/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 *
 * Test suite for FTS analyzer components.
 *
 * [TestAnalysisJsonResolverSpec]
 *   builtin_analyzer              — standard analyzer creation, case-insensitive, extra fields
 *   builtin_analyzer_support      — english/vietnamese/indonesian/unknown/empty
 *   custom_analyzer_object        — minimal custom analyzer JSON object, package-gated
 *   thai_malay_analyzer_support   — thai/malay JSON spec shape
 *   thai_malay_analyzer_tokenize* — ICU word break + stop filters for thai/malay
 *   invalid_input                 — empty/invalid JSON, missing key, bad value types
 *   destroy_spec                  — destroy null/partial/full spec boundary
 *   ddl_resolver                  — DDL resolve_analysis_json valid/empty/invalid
 *
 * [TestUtf8mb4BinCharFilter]
 *   init                          — success, double init, invalid type, invalid collation
 *   filter                        — not inited, no conversion, empty/null input, charset convert
 *   reset_and_reuse               — reset then re-init with different collation
 *
 * [TestFTSAnalyzer]
 *   standard_analyzer_tokenization       — basic/short/punct/empty/chinese/mixed/numbers/lowercase
 *   standard_analyzer_position_and_phrase — token position tracking, phrase match index type
 *   space_parser                         — basic/empty/single/multi-space/fulltext/min-max
 *   ngram_parser                         — basic/chinese/uppercase/corner cases
 *   beng_parser                          — basic english fulltext
 *   ik_smart                             — hello/numbers/date/scientific/compound/address/url/cjk/news/comprehensive
 *   ik_max_word                          — numbers, url+email decomposition
 *   helper_segment_legacy                — space/ngram segment() integration
 *   helper_segment_negative              — not-inited/init-twice/null/zero-len/neg-len/bad-collation
 *   helper_segment_analyzer              — analyzer segment() basic/repeated/invalid-collation
 *   check_is_the_same                    — same/different json/index-type, analyzer-vs-legacy, bare name
 *   helper_reset_reinit                  — analyzer->analyzer, analyzer->legacy, legacy->analyzer
 *   create_analyzer_negative             — not-inited/empty-json/invalid-json/null-alloc/empty-name/init-twice
 */

// put top to use macro tricks
#include "mtlenv/mock_tenant_module_env.h"
// put top to use macro tricks

#include <gtest/gtest.h>

#define USING_LOG_PREFIX STORAGE_FTS

#include "lib/allocator/page_arena.h"
#include "lib/container/ob_se_array.h"
#include "lib/charset/ob_charset.h"
#include "lib/ob_errno.h"
#include "lib/string/ob_string.h"
#include "plugin/sys/ob_plugin_mgr.h"
#include "share/ob_server_struct.h"
#include "share/rc/ob_tenant_base.h"
#include "sql/resolver/ddl/ob_fts_parser_resolver.h"
#include "storage/fts/ob_fts_plugin_helper.h"
#include "storage/fts/ob_fts_literal.h"
#include "storage/fts/analyzer/ob_token_stream.h"
#include "storage/fts/analyzer/ob_token_stream_factory.h"
#include "storage/fts/analyzer/char_filter/ob_legacy_char_filter.h"
#include "storage/fts/analyzer/filter/ob_snowball_filter.h"
#include "storage/fts/analyzer/filter/ob_stop_word_filter.h"
#include "storage/fts/dict/ob_ft_cache.h"
#include "storage/fts/dict/ob_ft_dict_def.h"
#include "storage/fts/dict/ob_ft_dict_mgr.h"
#define private protected
#include "storage/fts/dict/ob_ft_dict_cache_loader.h"
#undef private
#include "storage/fts/dict/ob_ft_range_dict.h"
#include "storage/fts/dict/ob_ik_dic.h"

using namespace oceanbase::common;
using namespace oceanbase::plugin;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace storage
{

class TestFTDictCacheLoaderExec : public ObFTDictCacheLoaderBase
{
public:
  int load_cache(const ObFTDictDesc &desc, ObFTCacheRangeContainer &range_container) override
  {
    int ret = OB_SUCCESS;
    const int64_t snapshot_version = 1;
    ObIKDictLoader::RawDict dict_text;

    if (share::OB_FT_DICT_IK_UTF8_TID == desc.table_id_) {
      dict_text = ObIKDictLoader::dict_text();
    } else if (share::OB_FT_STOPWORD_IK_UTF8_TID == desc.table_id_) {
      dict_text = ObIKDictLoader::dict_stop();
    } else if (share::OB_FT_QUANTIFIER_IK_UTF8_TID == desc.table_id_) {
      dict_text = ObIKDictLoader::dict_quen_text();
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("Test loader only supports builtin IK dicts", K(ret), K(desc));
    }

    if (OB_SUCC(ret)) {
      ObIKDictIterator iter(dict_text);
      if (OB_FAIL(iter.init())) {
        LOG_WARN("Failed to init iterator", K(ret));
      } else if (OB_FAIL(build_ranges(desc, iter, range_container, snapshot_version, nullptr))) {
        LOG_WARN("Failed to build ranges from builtin IK dict", K(ret), K(desc));
      }
    }
    return ret;
  }
};

typedef ObSEArray<ObString, 512> ObFtTokArr;

static inline int ft_expect_push(ObFtTokArr &arr, const char *literal_utf8)
{
  return arr.push_back(ObString::make_string(literal_utf8));
}

#define FTS_EXPECT_STR_EQ(arr, idx, lit)                                                       \
  EXPECT_EQ(0, (arr).at(static_cast<int64_t>(idx)).compare(ObString::make_string(lit))) << "idx=" << (idx)

static inline bool is_array_equal(const ObIArray<ObString> &a, const ObIArray<ObString> &b)
{
  if (a.count() != b.count()) {
    return false;
  }
  for (int64_t i = 0; i < a.count(); ++i) {
    if (0 != a.at(i).compare(b.at(i))) {
      return false;
    }
  }
  return true;
}

class TestFTSAnalyzer : public ::testing::Test
{
protected:
  TestFTSAnalyzer() : tenant_id_(1), tenant_base_(tenant_id_), dict_mgr_() {}
  virtual ~TestFTSAnalyzer() {}

  static void SetUpTestCase()
  {
    ASSERT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
    ASSERT_EQ(OB_SUCCESS, ObDictCache::get_instance().init("dict_cache"));
    if (OB_NOT_NULL(GCTX.plugin_mgr_)) {
      GCTX.plugin_mgr_->load_builtin_plugins();
    } else {
      GCTX.plugin_mgr_ = OB_NEW(ObPluginMgr, ObMemAttr(OB_SYS_TENANT_ID, "TestAnalyzer"));
      ASSERT_NE(nullptr, GCTX.plugin_mgr_);
      ASSERT_EQ(OB_SUCCESS, GCTX.plugin_mgr_->init(ObString("")));
      ASSERT_EQ(OB_SUCCESS, GCTX.plugin_mgr_->load_builtin_plugins());
    }
  }
  static void TearDownTestCase()
  {
    ObDictCache::get_instance().destroy();
    if (OB_NOT_NULL(GCTX.plugin_mgr_)) {
      GCTX.plugin_mgr_->destroy();
      OB_DELETE(ObPluginMgr, "TestAnalyzer", GCTX.plugin_mgr_);
      GCTX.plugin_mgr_ = nullptr;
    }
    MockTenantModuleEnv::get_instance().destroy();
  }
  virtual void SetUp()
  {
    allocator_.reset();
    helper_.reset();
    ASSERT_EQ(OB_SUCCESS, dict_mgr_.init());
    tenant_base_.set(&dict_mgr_);
    ObTenantEnv::set_tenant(&tenant_base_);
    ASSERT_EQ(OB_SUCCESS, tenant_base_.init());
  }
  virtual void TearDown()
  {
    helper_.reset();
    ObTenantEnv::set_tenant(nullptr);
    tenant_base_.destroy();
    dict_mgr_.destroy();
  }

  static constexpr const char *DEFAULT_PROPERTIES =
      "{\"min_token_size\":3,\"max_token_size\":84,"
      "\"stopword_table\":\"default\",\"dict_table\":\"none\","
      "\"quanitfier_table\":\"none\",\"ngram_token_size\":2}";

  static constexpr const char *IK_SMART_PROPERTIES =
      "{\"min_token_size\":3,\"max_token_size\":84,"
      "\"stopword_table\":\"default\",\"dict_table\":\"none\","
      "\"quanitfier_table\":\"none\",\"ngram_token_size\":2,"
      "\"ik_mode\":\"smart\"}";

  static constexpr const char *IK_MAX_WORD_PROPERTIES =
      "{\"min_token_size\":3,\"max_token_size\":84,"
      "\"stopword_table\":\"default\",\"dict_table\":\"none\","
      "\"quanitfier_table\":\"none\",\"ngram_token_size\":2,"
      "\"ik_mode\":\"max_word\"}";

  int init_helper(const char *parser_name_str, const char *properties = DEFAULT_PROPERTIES)
  {
    int ret = OB_SUCCESS;
    ObString parser_name = ObString::make_string(parser_name_str);
    ObString parser_props = ObString::make_string(properties);
    if (OB_FAIL(helper_.init(&allocator_, parser_name, parser_props,
                             share::schema::OB_FTS_INDEX_TYPE_MATCH))) {
      LOG_WARN("fail to init parser helper", K(ret));
    } else if (ObTokenizerType::TOKENIZER_TYPE_IK == helper_.get_parser_name().to_tokenizer_type()
               && OB_FAIL(prepare_ik_dict_cache())) {
      LOG_WARN("fail to prepare IK dict cache", K(ret));
    }
    return ret;
  }

  int prepare_ik_dict_cache()
  {
    int ret = OB_SUCCESS;
    TestFTDictCacheLoaderExec cache_loader;
    ObFTCacheRangeContainer range_container(allocator_);
    ObFTDictDesc main_desc(ObCharsetType::CHARSET_UTF8MB4,
                           ObCollationType::CS_TYPE_UTF8MB4_BIN,
                           share::OB_FT_DICT_IK_UTF8_TID,
                           ObString(ObFTSLiteral::FT_DEFAULT_IK_DICT_UTF8_TABLE));
    ObFTDictDesc stopword_desc(ObCharsetType::CHARSET_UTF8MB4,
                               ObCollationType::CS_TYPE_UTF8MB4_BIN,
                               share::OB_FT_STOPWORD_IK_UTF8_TID,
                               ObString(ObFTSLiteral::FT_DEFAULT_IK_STOPWORD_UTF8_TABLE));
    ObFTDictDesc quan_desc(ObCharsetType::CHARSET_UTF8MB4,
                           ObCollationType::CS_TYPE_UTF8MB4_BIN,
                           share::OB_FT_QUANTIFIER_IK_UTF8_TID,
                           ObString(ObFTSLiteral::FT_DEFAULT_IK_QUANTIFIER_UTF8_TABLE));

    if (OB_FAIL(cache_loader.load_cache(main_desc, range_container))) {
      LOG_WARN("fail to load main IK dict cache", K(ret));
    } else if (FALSE_IT(range_container.reset())) {
    } else if (OB_FAIL(cache_loader.load_cache(stopword_desc, range_container))) {
      LOG_WARN("fail to load stopword IK dict cache", K(ret));
    } else if (FALSE_IT(range_container.reset())) {
    } else if (OB_FAIL(cache_loader.load_cache(quan_desc, range_container))) {
      LOG_WARN("fail to load quantifier IK dict cache", K(ret));
    }
    return ret;
  }

  int segment_and_collect(const char *text, ObIArray<ObString> &tokens,
                          ObCollationType coll_type = CS_TYPE_UTF8MB4_GENERAL_CI)
  {
    int ret = OB_SUCCESS;
    ObObjMeta meta;
    meta.set_varchar();
    meta.set_collation_type(coll_type);
    meta.set_collation_level(CS_LEVEL_IMPLICIT);

    ObFTAnalyzerParam param;
    param.legacy_tokenizer_type_ = helper_.get_parser_name().to_tokenizer_type();
    param.parser_property_ = &helper_.get_parser_property();
    param.fts_index_type_ = share::schema::OB_FTS_INDEX_TYPE_MATCH;
    param.process_token_flag_ = helper_.get_process_token_flags();
    param.meta_ = meta;
    param.alloc_ = &allocator_;

    ObFTSAnalyzer *analyzer = nullptr;
    ObITokenStream *stream = nullptr;
    tokens.reuse();

    if (OB_FAIL(helper_.create_analyzer(param, analyzer))) {
      LOG_WARN("fail to create analyzer", K(ret));
    } else if (OB_FAIL(analyzer->analyze(text, strlen(text),
                                          allocator_, stream))) {
      LOG_WARN("fail to analyze", K(ret));
    } else if (OB_ISNULL(stream)) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      ObTokenAttr token;
      while (OB_SUCC(ret)) {
        if (OB_FAIL(stream->get_next_token(token))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
          }
          break;
        } else {
          ObString piece(token.token_len_, token.token_ptr_);
          ObString stored;
          if (OB_FAIL(ob_write_string(allocator_, piece, stored))) {
            LOG_WARN("fail to copy token string", K(ret));
          } else if (OB_FAIL(tokens.push_back(stored))) {
            LOG_WARN("fail to push token", K(ret));
          }
        }
      }
    }

    if (OB_NOT_NULL(analyzer)) {
      analyzer->~ObFTSAnalyzer();
      allocator_.free(analyzer);
    }
    return ret;
  }

protected:
  const uint64_t tenant_id_;
  ObTenantBase tenant_base_;
  ObFTDictMgr dict_mgr_;
  ObArenaAllocator allocator_{ObMemAttr(OB_SYS_TENANT_ID, "TestAnalyzer")};
  ObFTParseHelper helper_;
};

class TestAnalysisJsonResolverSpec : public ::testing::Test
{
protected:
  void SetUp() override {}
  void TearDown() override
  {
    ObAnalyzerSpecFactory::destroy_analyzer_spec(alloc_, spec_);
    alloc_.reset();
  }

  int create_spec(const ObString &json)
  {
    return ObAnalyzerSpecFactory::create_analyzer_spec(json, alloc_, spec_);
  }

  void reset_spec()
  {
    ObAnalyzerSpecFactory::destroy_analyzer_spec(alloc_, spec_);
    spec_ = nullptr;
  }

  ObArenaAllocator alloc_;
  ObAnalyzerSpec *spec_ = nullptr;
};

// ============================================================
// Analysis JSON spec resolver tests
// ============================================================

TEST_F(TestAnalysisJsonResolverSpec, builtin_analyzer)
{
  {
    SCOPED_TRACE("standard");
    ObString json("{\"analyzer\": \"standard\"}");
    ASSERT_EQ(OB_SUCCESS, create_spec(json));
    const ObAnalyzerSpec *spec = spec_;
    ASSERT_TRUE(OB_NOT_NULL(spec));
    ASSERT_EQ(ObAnalyzerType::ANALYZER_TYPE_STANDARD, spec->analyzer_type_);
    ASSERT_TRUE(spec->is_valid());
    ASSERT_EQ(1, spec->char_filter_specs_.count());
    ASSERT_EQ(ObCharFilterType::CHAR_FILTER_TYPE_UTF8MB4_BIN,
              spec->char_filter_specs_.at(0)->type_);
    ASSERT_EQ(2, spec->token_filter_specs_.count());
    ASSERT_EQ(ObTokenFilterType::TOKEN_FILTER_TYPE_LOWERCASE,
              spec->token_filter_specs_.at(0)->type_);
    ASSERT_EQ(ObTokenFilterType::TOKEN_FILTER_TYPE_CHARSET_CONVERT,
              spec->token_filter_specs_.at(1)->type_);
    reset_spec();
  }
  {
    SCOPED_TRACE("case_insensitive");
    ObString json("{\"analyzer\": \"STANDARD\"}");
    ASSERT_EQ(OB_SUCCESS, create_spec(json));
    const ObAnalyzerSpec *spec = spec_;
    ASSERT_TRUE(OB_NOT_NULL(spec));
    ASSERT_EQ(ObAnalyzerType::ANALYZER_TYPE_STANDARD, spec->analyzer_type_);
    reset_spec();
  }
  {
    SCOPED_TRACE("extra_fields_ignored");
    ObString json("{\"analyzer\": \"standard\", \"foo\": \"bar\"}");
    ASSERT_EQ(OB_SUCCESS, create_spec(json));
    ASSERT_TRUE(OB_NOT_NULL(spec_));
    ASSERT_EQ(ObAnalyzerType::ANALYZER_TYPE_STANDARD, spec_->analyzer_type_);
  }
}

TEST_F(TestAnalysisJsonResolverSpec, builtin_analyzer_support)
{
  struct Case {
    const char *json;
    int expect_ret;
    ObAnalyzerType expected_type;
    ObTokenizerType expected_tokenizer_type;
    int64_t expected_token_filter_count;
    ObTokenFilterType expected_token_filter_types[5];
    const char *desc;
  };
  Case cases[] = {
    {"{\"analyzer\": \"standard\"}",
     OB_SUCCESS,
     ObAnalyzerType::ANALYZER_TYPE_STANDARD,
     ObTokenizerType::TOKENIZER_TYPE_STANDARD,
     2,
     {ObTokenFilterType::TOKEN_FILTER_TYPE_LOWERCASE,
      ObTokenFilterType::TOKEN_FILTER_TYPE_CHARSET_CONVERT},
     "standard"},
    {"{\"analyzer\": \"english\"}",
     OB_SUCCESS,
     ObAnalyzerType::ANALYZER_TYPE_ENGLISH,
     ObTokenizerType::TOKENIZER_TYPE_STANDARD,
     5,
     {ObTokenFilterType::TOKEN_FILTER_TYPE_POSSESSIVE_ENGLISH,
      ObTokenFilterType::TOKEN_FILTER_TYPE_LOWERCASE,
      ObTokenFilterType::TOKEN_FILTER_TYPE_STOP,
      ObTokenFilterType::TOKEN_FILTER_TYPE_SNOWBALL,
      ObTokenFilterType::TOKEN_FILTER_TYPE_CHARSET_CONVERT},
     "english"},
    {"{\"analyzer\": \"thai\"}",
     OB_SUCCESS,
     ObAnalyzerType::ANALYZER_TYPE_THAI,
     ObTokenizerType::TOKENIZER_TYPE_STANDARD,
     4,
     {ObTokenFilterType::TOKEN_FILTER_TYPE_LOWERCASE,
      ObTokenFilterType::TOKEN_FILTER_TYPE_DECIMAL_DIGIT,
      ObTokenFilterType::TOKEN_FILTER_TYPE_STOP,
      ObTokenFilterType::TOKEN_FILTER_TYPE_CHARSET_CONVERT},
     "thai"},
    {"{\"analyzer\": \"vietnamese\"}",
     OB_SUCCESS,
     ObAnalyzerType::ANALYZER_TYPE_VIETNAMESE,
     ObTokenizerType::TOKENIZER_TYPE_STANDARD,
     2,
     {ObTokenFilterType::TOKEN_FILTER_TYPE_ICU_FOLDING,
      ObTokenFilterType::TOKEN_FILTER_TYPE_CHARSET_CONVERT},
     "vietnamese"},
    {"{\"analyzer\": \"indonesian\"}",
     OB_SUCCESS,
     ObAnalyzerType::ANALYZER_TYPE_INDONESIAN,
     ObTokenizerType::TOKENIZER_TYPE_STANDARD,
     4,
     {ObTokenFilterType::TOKEN_FILTER_TYPE_LOWERCASE,
      ObTokenFilterType::TOKEN_FILTER_TYPE_STOP,
      ObTokenFilterType::TOKEN_FILTER_TYPE_SNOWBALL,
      ObTokenFilterType::TOKEN_FILTER_TYPE_CHARSET_CONVERT},
     "indonesian"},
    {"{\"analyzer\": \"malay\"}",
     OB_SUCCESS,
     ObAnalyzerType::ANALYZER_TYPE_MALAY,
     ObTokenizerType::TOKENIZER_TYPE_STANDARD,
     2,
     {ObTokenFilterType::TOKEN_FILTER_TYPE_LOWERCASE,
      ObTokenFilterType::TOKEN_FILTER_TYPE_CHARSET_CONVERT},
     "malay"},
    {"{\"analyzer\": \"unknown_lang\"}",
     OB_NOT_SUPPORTED,
     ObAnalyzerType::ANALYZER_TYPE_INVALID,
     ObTokenizerType::TOKENIZER_TYPE_INVALID,
     0,
     {ObTokenFilterType::TOKEN_FILTER_TYPE_INVALID},
     "unknown_type"},
    {"{\"analyzer\": \"\"}",
     OB_NOT_SUPPORTED,
     ObAnalyzerType::ANALYZER_TYPE_INVALID,
     ObTokenizerType::TOKENIZER_TYPE_INVALID,
     0,
     {ObTokenFilterType::TOKEN_FILTER_TYPE_INVALID},
     "empty_string"},
    {"{\"analyzer\": {}}",
     OB_INVALID_ARGUMENT,
     ObAnalyzerType::ANALYZER_TYPE_INVALID,
     ObTokenizerType::TOKENIZER_TYPE_INVALID,
     0,
     {ObTokenFilterType::TOKEN_FILTER_TYPE_INVALID},
     "empty_object"},
  };
  for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); ++i) {
    SCOPED_TRACE(cases[i].desc);
    ObString json(cases[i].json);
    ASSERT_EQ(cases[i].expect_ret, create_spec(json));
    if (OB_SUCCESS == cases[i].expect_ret) {
      ASSERT_TRUE(OB_NOT_NULL(spec_));
      ASSERT_EQ(cases[i].expected_type, spec_->analyzer_type_);
      ASSERT_TRUE(OB_NOT_NULL(spec_->tokenizer_spec_));
      ASSERT_EQ(cases[i].expected_tokenizer_type, spec_->tokenizer_spec_->type_);
      ASSERT_EQ(1, spec_->char_filter_specs_.count());
      ASSERT_EQ(ObCharFilterType::CHAR_FILTER_TYPE_UTF8MB4_BIN,
                spec_->char_filter_specs_.at(0)->type_);
      ASSERT_EQ(cases[i].expected_token_filter_count, spec_->token_filter_specs_.count());
      for (int64_t j = 0; j < cases[i].expected_token_filter_count; ++j) {
        ASSERT_EQ(cases[i].expected_token_filter_types[j], spec_->token_filter_specs_.at(j)->type_);
      }
    }
    reset_spec();
  }
}

TEST_F(TestAnalysisJsonResolverSpec, custom_analyzer_object)
{
  ObString json("{\"analyzer\": {\"my_custom\": {\"type\": \"custom\", \"tokenizer\": \"standard\"}}}");
#ifdef OB_BUILD_PACKAGE
  ASSERT_EQ(OB_NOT_SUPPORTED, create_spec(json));
#else
  ASSERT_EQ(OB_SUCCESS, create_spec(json));
  ASSERT_TRUE(OB_NOT_NULL(spec_));
  ASSERT_EQ(ObAnalyzerType::ANALYZER_TYPE_CUSTOM, spec_->analyzer_type_);
  ASSERT_TRUE(OB_NOT_NULL(spec_->tokenizer_spec_));
  ASSERT_EQ(ObTokenizerType::TOKENIZER_TYPE_STANDARD, spec_->tokenizer_spec_->type_);
  ASSERT_EQ(1, spec_->char_filter_specs_.count());
  ASSERT_EQ(ObCharFilterType::CHAR_FILTER_TYPE_UTF8MB4_BIN,
            spec_->char_filter_specs_.at(0)->type_);
  ASSERT_EQ(1, spec_->token_filter_specs_.count());
  ASSERT_EQ(ObTokenFilterType::TOKEN_FILTER_TYPE_CHARSET_CONVERT,
            spec_->token_filter_specs_.at(0)->type_);
#endif
}

TEST_F(TestAnalysisJsonResolverSpec, stop_filter_default_stopwords)
{
  ObString json("{\"analyzer\":{\"a\":{\"type\":\"custom\",\"tokenizer\":\"standard\","
                "\"filter\":[\"lowercase\",\"stop\"]}}}");
#ifdef OB_BUILD_PACKAGE
  ASSERT_EQ(OB_NOT_SUPPORTED, create_spec(json));
#else
  ASSERT_EQ(OB_SUCCESS, create_spec(json));
  ASSERT_TRUE(OB_NOT_NULL(spec_));
  ASSERT_EQ(3, spec_->token_filter_specs_.count());
  ASSERT_EQ(ObTokenFilterType::TOKEN_FILTER_TYPE_LOWERCASE, spec_->token_filter_specs_.at(0)->type_);
  ASSERT_EQ(ObTokenFilterType::TOKEN_FILTER_TYPE_STOP, spec_->token_filter_specs_.at(1)->type_);
  ASSERT_EQ(ObTokenFilterType::TOKEN_FILTER_TYPE_CHARSET_CONVERT,
            spec_->token_filter_specs_.at(2)->type_);
  ObStopWordFilterSpec *stop_spec =
      static_cast<ObStopWordFilterSpec *>(spec_->token_filter_specs_.at(1));
  ASSERT_EQ(ObStopWordLanguageKind::LANGUAGE_ENGLISH, stop_spec->language_);
#endif
}

TEST_F(TestAnalysisJsonResolverSpec, snowball_default_language)
{
  // Aligned with Elasticsearch: when "language" is omitted, snowball defaults to English.
  ObString json("{\"analyzer\":{\"a\":{\"type\":\"custom\",\"tokenizer\":\"standard\","
                "\"filter\":[\"my_sb\"]}},\"filter\":{\"my_sb\":{\"type\":\"snowball\"}}}");
#ifdef OB_BUILD_PACKAGE
  ASSERT_EQ(OB_NOT_SUPPORTED, create_spec(json));
#else
  ASSERT_EQ(OB_SUCCESS, create_spec(json));
  ASSERT_TRUE(OB_NOT_NULL(spec_));
  ASSERT_EQ(2, spec_->token_filter_specs_.count());
  ASSERT_EQ(ObTokenFilterType::TOKEN_FILTER_TYPE_SNOWBALL,
            spec_->token_filter_specs_.at(0)->type_);
  ASSERT_EQ(ObTokenFilterType::TOKEN_FILTER_TYPE_CHARSET_CONVERT,
            spec_->token_filter_specs_.at(1)->type_);
  ObSnowballFilterSpec *snowball_spec =
      static_cast<ObSnowballFilterSpec *>(spec_->token_filter_specs_.at(0));
  ASSERT_EQ(ObSnowballFilterSpec::Algorithm::ENGLISH, snowball_spec->algo_);
#endif
}

TEST_F(TestAnalysisJsonResolverSpec, thai_malay_analyzer_support)
{
  struct Case {
    const char *json;
    ObAnalyzerType expected_type;
    int64_t expected_token_filter_count;
    ObTokenFilterType expected_token_filter_types[4];
    const char *desc;
  };
  Case cases[] = {
    {"{\"analyzer\": \"thai\"}",
     ObAnalyzerType::ANALYZER_TYPE_THAI,
     4,
     {ObTokenFilterType::TOKEN_FILTER_TYPE_LOWERCASE,
      ObTokenFilterType::TOKEN_FILTER_TYPE_DECIMAL_DIGIT,
      ObTokenFilterType::TOKEN_FILTER_TYPE_STOP,
      ObTokenFilterType::TOKEN_FILTER_TYPE_CHARSET_CONVERT},
     "thai"},
    {"{\"analyzer\": \"malay\"}",
     ObAnalyzerType::ANALYZER_TYPE_MALAY,
     2,
     {ObTokenFilterType::TOKEN_FILTER_TYPE_LOWERCASE,
      ObTokenFilterType::TOKEN_FILTER_TYPE_CHARSET_CONVERT},
     "malay"},
  };

  for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); ++i) {
    SCOPED_TRACE(cases[i].desc);
    ObString json(cases[i].json);
    ASSERT_EQ(OB_SUCCESS, create_spec(json));
    ASSERT_TRUE(OB_NOT_NULL(spec_));
    ASSERT_EQ(cases[i].expected_type, spec_->analyzer_type_);
    ASSERT_EQ(cases[i].expected_token_filter_count, spec_->token_filter_specs_.count());
    for (int64_t j = 0; j < cases[i].expected_token_filter_count; ++j) {
      ASSERT_EQ(cases[i].expected_token_filter_types[j],
                spec_->token_filter_specs_.at(j)->type_);
    }
    reset_spec();
  }
}

TEST_F(TestFTSAnalyzer, thai_malay_analyzer_tokenize)
{
  ObFtTokArr tokens;
  const char *text_thai = u8"สวัสดี เทคโนโลยี ไทย สำหรับ ผู้ค้นหา";
  const char *text_malay = u8"Selamat datang ke dunia pencarian cerdas";

  ASSERT_EQ(OB_SUCCESS, init_helper("analyzer.1", "{\"analyzer\": \"thai\"}"));
  ASSERT_EQ(OB_SUCCESS, segment_and_collect(text_thai, tokens));
  ObFtTokArr expected_thai;
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected_thai, u8"สวัสดี"));
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected_thai, u8"เทคโนโลยี"));
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected_thai, u8"ไทย"));
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected_thai, u8"สำหรับ"));
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected_thai, u8"ผู้"));
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected_thai, u8"ค้นหา"));
  ASSERT_TRUE(is_array_equal(expected_thai, tokens));

  helper_.reset();

  ASSERT_EQ(OB_SUCCESS, init_helper("analyzer.2", "{\"analyzer\": \"malay\"}"));
  ASSERT_EQ(OB_SUCCESS, segment_and_collect(text_malay, tokens));
  ObFtTokArr expected_malay;
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected_malay, "selamat"));
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected_malay, "datang"));
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected_malay, "ke"));
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected_malay, "dunia"));
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected_malay, "pencarian"));
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected_malay, "cerdas"));
  ASSERT_TRUE(is_array_equal(expected_malay, tokens));
  helper_.reset();
}

TEST_F(TestFTSAnalyzer, thai_malay_analyzer_tokenize_richer_samples)
{
  ObFtTokArr tokens;
  ObFtTokArr expected;

  const char *thai_text_1 = u8"เทคโนโลยี ภาษาไทย และ นวัตกรรม";
  const char *thai_text_2 = u8"ไทย เทคโนโลยี AI 2026";
  const char *thai_text_3 = u8"ไทย และ ของ เทคโนโลยี ภาษา";
  const char *malay_text_1 = u8"Pencarian cepat, data besar, dan pangkalan";
  const char *malay_text_2 = u8"ANALYZER MALAY mendukung bahasa Melayu modern";
  const char *malay_text_3 = u8"Data, pencarian!!! CERDAS?? 2026";

  ASSERT_EQ(OB_SUCCESS, init_helper("analyzer.3", "{\"analyzer\": \"thai\"}"));
  ASSERT_EQ(OB_SUCCESS, segment_and_collect(thai_text_1, tokens));
  expected.reuse();
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, u8"เทคโนโลยี"));
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, u8"ภาษา"));
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, u8"ไทย"));
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, u8"นวัตกรรม"));
  ASSERT_TRUE(is_array_equal(expected, tokens));

  helper_.reset();
  ASSERT_EQ(OB_SUCCESS, init_helper("analyzer.4", "{\"analyzer\": \"thai\"}"));
  ASSERT_EQ(OB_SUCCESS, segment_and_collect(thai_text_2, tokens));
  expected.reuse();
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, u8"ไทย"));
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, u8"เทคโนโลยี"));
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, "ai"));
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, "2026"));
  ASSERT_TRUE(is_array_equal(expected, tokens));

  helper_.reset();
  ASSERT_EQ(OB_SUCCESS, init_helper("analyzer.7", "{\"analyzer\": \"thai\"}"));
  ASSERT_EQ(OB_SUCCESS, segment_and_collect(thai_text_3, tokens));
  expected.reuse();
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, u8"ไทย"));
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, u8"เทคโนโลยี"));
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, u8"ภาษา"));
  ASSERT_TRUE(is_array_equal(expected, tokens));

  helper_.reset();
  ASSERT_EQ(OB_SUCCESS, init_helper("analyzer.5", "{\"analyzer\": \"malay\"}"));
  ASSERT_EQ(OB_SUCCESS, segment_and_collect(malay_text_1, tokens));
  expected.reuse();
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, "pencarian"));
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, "cepat"));
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, "data"));
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, "besar"));
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, "dan"));
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, "pangkalan"));
  ASSERT_TRUE(is_array_equal(expected, tokens));

  helper_.reset();
  ASSERT_EQ(OB_SUCCESS, init_helper("analyzer.6", "{\"analyzer\": \"malay\"}"));
  ASSERT_EQ(OB_SUCCESS, segment_and_collect(malay_text_2, tokens));
  expected.reuse();
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, "analyzer"));
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, "malay"));
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, "mendukung"));
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, "bahasa"));
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, "melayu"));
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, "modern"));
  ASSERT_TRUE(is_array_equal(expected, tokens));

  helper_.reset();
  ASSERT_EQ(OB_SUCCESS, init_helper("analyzer.8", "{\"analyzer\": \"malay\"}"));
  ASSERT_EQ(OB_SUCCESS, segment_and_collect(malay_text_3, tokens));
  expected.reuse();
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, "data"));
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, "pencarian"));
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, "cerdas"));
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, "2026"));
  ASSERT_TRUE(is_array_equal(expected, tokens));
}

TEST_F(TestFTSAnalyzer, thai_malay_analyzer_tokenize_boundary_samples)
{
  ObFtTokArr tokens;
  ObFtTokArr expected;

  const char *thai_text_1 = u8"สวัสดี  \nไทย\tAI!!!";
  const char *thai_text_2 = u8"ผู้ค้นหา และ การค้นหา ภาษา";
  const char *malay_text_1 = u8"Data  PENCARIAN,,\tcerdas 2024!!!";
  const char *malay_text_2 = u8"Bahan-Baku DAN cerdas - MALAY";
  const char *thai_text_3 = u8"สวัสดี\tไทย\r\nAI 2024";
  const char *malay_text_3 = u8"  Data, PENCARIAN 9999!! DAN MALAY";
  const char *malay_text_4 = u8"Bahan---Baku;; ANALYZER??\ncerdas";
  const char *thai_text_4 = u8"เทคโนโลยี และ เทคโนโลยี และ เทคโนโลยี";
  const char *malay_text_5 = u8"Pencarian cepat cepat dan data data 2024 2024";
  const char *thai_text_6 = u8"สวัสดี!! AI --- สวัสดี";
  const char *malay_text_6 = u8"Cerdas-data BAHAN***CERDAS???";
  const char *thai_text_7 = u8"ภาษา ภาษา และ ภาษา!!!";
  const char *malay_text_7 = u8"Data\npencarian,\tBahan--MALAY 2026";

  ASSERT_EQ(OB_SUCCESS, init_helper("analyzer.9", "{\"analyzer\": \"thai\"}"));
  ASSERT_EQ(OB_SUCCESS, segment_and_collect(thai_text_1, tokens));
  expected.reuse();
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, u8"สวัสดี"));
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, u8"ไทย"));
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, "ai"));
  ASSERT_TRUE(is_array_equal(expected, tokens));

  helper_.reset();
  ASSERT_EQ(OB_SUCCESS, init_helper("analyzer.10", "{\"analyzer\": \"thai\"}"));
  ASSERT_EQ(OB_SUCCESS, segment_and_collect(thai_text_2, tokens));
  expected.reuse();
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, u8"ผู้"));
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, u8"ค้นหา"));
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, u8"ค้นหา"));
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, u8"ภาษา"));
  ASSERT_TRUE(is_array_equal(expected, tokens));

  helper_.reset();
  ASSERT_EQ(OB_SUCCESS, init_helper("analyzer.11", "{\"analyzer\": \"malay\"}"));
  ASSERT_EQ(OB_SUCCESS, segment_and_collect(malay_text_1, tokens));
  expected.reuse();
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, "data"));
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, "pencarian"));
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, "cerdas"));
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, "2024"));
  ASSERT_TRUE(is_array_equal(expected, tokens));

  helper_.reset();
  ASSERT_EQ(OB_SUCCESS, init_helper("analyzer.12", "{\"analyzer\": \"malay\"}"));
  ASSERT_EQ(OB_SUCCESS, segment_and_collect(malay_text_2, tokens));
  expected.reuse();
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, "bahan"));
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, "baku"));
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, "dan"));
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, "cerdas"));
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, "malay"));
  ASSERT_TRUE(is_array_equal(expected, tokens));

  helper_.reset();
  ASSERT_EQ(OB_SUCCESS, init_helper("analyzer.13", "{\"analyzer\": \"thai\"}"));
  ASSERT_EQ(OB_SUCCESS, segment_and_collect(thai_text_3, tokens));
  expected.reuse();
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, u8"สวัสดี"));
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, u8"ไทย"));
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, "ai"));
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, "2024"));
  ASSERT_TRUE(is_array_equal(expected, tokens));

  helper_.reset();
  ASSERT_EQ(OB_SUCCESS, init_helper("analyzer.14", "{\"analyzer\": \"malay\"}"));
  ASSERT_EQ(OB_SUCCESS, segment_and_collect(malay_text_3, tokens));
  expected.reuse();
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, "data"));
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, "pencarian"));
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, "9999"));
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, "dan"));
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, "malay"));
  ASSERT_TRUE(is_array_equal(expected, tokens));

  helper_.reset();
  ASSERT_EQ(OB_SUCCESS, init_helper("analyzer.15", "{\"analyzer\": \"malay\"}"));
  ASSERT_EQ(OB_SUCCESS, segment_and_collect(malay_text_4, tokens));
  expected.reuse();
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, "bahan"));
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, "baku"));
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, "analyzer"));
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, "cerdas"));
  ASSERT_TRUE(is_array_equal(expected, tokens));

  helper_.reset();
  ASSERT_EQ(OB_SUCCESS, init_helper("analyzer.16", "{\"analyzer\": \"thai\"}"));
  ASSERT_EQ(OB_SUCCESS, segment_and_collect(thai_text_4, tokens));
  expected.reuse();
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, u8"เทคโนโลยี"));
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, u8"เทคโนโลยี"));
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, u8"เทคโนโลยี"));
  ASSERT_TRUE(is_array_equal(expected, tokens));

  helper_.reset();
  ASSERT_EQ(OB_SUCCESS, init_helper("analyzer.17", "{\"analyzer\": \"malay\"}"));
  ASSERT_EQ(OB_SUCCESS, segment_and_collect(malay_text_5, tokens));
  expected.reuse();
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, "pencarian"));
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, "cepat"));
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, "cepat"));
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, "dan"));
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, "data"));
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, "data"));
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, "2024"));
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, "2024"));
  ASSERT_TRUE(is_array_equal(expected, tokens));

  helper_.reset();
  ASSERT_EQ(OB_SUCCESS, init_helper("analyzer.18", "{\"analyzer\": \"thai\"}"));
  ASSERT_EQ(OB_SUCCESS, segment_and_collect(thai_text_6, tokens));
  expected.reuse();
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, u8"สวัสดี"));
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, "ai"));
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, u8"สวัสดี"));
  ASSERT_TRUE(is_array_equal(expected, tokens));

  helper_.reset();
  ASSERT_EQ(OB_SUCCESS, init_helper("analyzer.19", "{\"analyzer\": \"malay\"}"));
  ASSERT_EQ(OB_SUCCESS, segment_and_collect(malay_text_6, tokens));
  expected.reuse();
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, "cerdas"));
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, "data"));
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, "bahan"));
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, "cerdas"));
  ASSERT_TRUE(is_array_equal(expected, tokens));

  helper_.reset();
  ASSERT_EQ(OB_SUCCESS, init_helper("analyzer.20", "{\"analyzer\": \"thai\"}"));
  ASSERT_EQ(OB_SUCCESS, segment_and_collect(thai_text_7, tokens));
  expected.reuse();
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, u8"ภาษา"));
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, u8"ภาษา"));
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, u8"ภาษา"));
  ASSERT_TRUE(is_array_equal(expected, tokens));

  helper_.reset();
  ASSERT_EQ(OB_SUCCESS, init_helper("analyzer.21", "{\"analyzer\": \"malay\"}"));
  ASSERT_EQ(OB_SUCCESS, segment_and_collect(malay_text_7, tokens));
  expected.reuse();
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, "data"));
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, "pencarian"));
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, "bahan"));
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, "malay"));
  ASSERT_EQ(OB_SUCCESS, ft_expect_push(expected, "2026"));
  ASSERT_TRUE(is_array_equal(expected, tokens));
}

TEST_F(TestAnalysisJsonResolverSpec, invalid_input)
{
  {
    SCOPED_TRACE("empty_json");
    ObString json;
    ASSERT_EQ(OB_INVALID_ARGUMENT, create_spec(json));
    reset_spec();
  }
  {
    SCOPED_TRACE("invalid_json");
    ObString json("not a json");
    ASSERT_NE(OB_SUCCESS, create_spec(json));
    reset_spec();
  }
  {
    SCOPED_TRACE("missing_analyzer_key");
    ObString json("{\"tokenizer\": \"standard\"}");
    ASSERT_EQ(OB_SEARCH_NOT_FOUND, create_spec(json));
    reset_spec();
  }
  {
    SCOPED_TRACE("empty_json_object");
    ObString json("{}");
    ASSERT_NE(OB_SUCCESS, create_spec(json));
    reset_spec();
  }
  {
    SCOPED_TRACE("numeric_value");
    ObString json("{\"analyzer\": 42}");
    ASSERT_EQ(OB_INVALID_ARGUMENT, create_spec(json));
    reset_spec();
  }
  {
    SCOPED_TRACE("null_value");
    ObString json("{\"analyzer\": null}");
    ASSERT_NE(OB_SUCCESS, create_spec(json));
    reset_spec();
  }
  {
    SCOPED_TRACE("array_value");
    ObString json("{\"analyzer\": [\"standard\"]}");
    ASSERT_EQ(OB_INVALID_ARGUMENT, create_spec(json));
    reset_spec();
  }
  {
    SCOPED_TRACE("boolean_value");
    ObString json("{\"analyzer\": true}");
    ASSERT_EQ(OB_INVALID_ARGUMENT, create_spec(json));
  }
}

TEST_F(TestAnalysisJsonResolverSpec, destroy_spec)
{
  {
    SCOPED_TRACE("null_noop");
    ObAnalyzerSpec *spec = nullptr;
    ObAnalyzerSpecFactory::destroy_analyzer_spec(alloc_, spec);
    EXPECT_TRUE(spec == nullptr);
  }
  {
    SCOPED_TRACE("partial_init");
    void *buf = alloc_.alloc(sizeof(ObAnalyzerSpec));
    ASSERT_TRUE(buf != nullptr);
    ObAnalyzerSpec *spec = new (buf) ObAnalyzerSpec(alloc_);
    spec->analyzer_type_ = ObAnalyzerType::ANALYZER_TYPE_STANDARD;
    ObAnalyzerSpecFactory::destroy_analyzer_spec(alloc_, spec);
  }
  {
    SCOPED_TRACE("after_create");
    ObString json("{\"analyzer\": \"standard\"}");
    ASSERT_EQ(OB_SUCCESS, create_spec(json));
    const ObAnalyzerSpec *spec = spec_;
    ASSERT_TRUE(OB_NOT_NULL(spec));
    ASSERT_TRUE(spec->is_valid());
    ObAnalyzerSpecFactory::destroy_analyzer_spec(alloc_, spec_);
  }
}

TEST_F(TestAnalysisJsonResolverSpec, ddl_resolver)
{
  {
    SCOPED_TRACE("valid_json");
    ObString json("{\"analyzer\": \"standard\"}");
    ObString validated;
    ASSERT_EQ(OB_SUCCESS, ObFTParserResolverHelper::resolve_analysis_json(json, alloc_, validated));
    ASSERT_FALSE(validated.empty());
    ASSERT_EQ(0, validated.compare(json));
  }
  {
    SCOPED_TRACE("empty_json");
    ObString json;
    ObString validated;
    ASSERT_EQ(OB_INVALID_ARGUMENT,
        ObFTParserResolverHelper::resolve_analysis_json(json, alloc_, validated));
  }
  {
    SCOPED_TRACE("invalid_json");
    ObString json("not json");
    ObString validated;
    ASSERT_NE(OB_SUCCESS,
        ObFTParserResolverHelper::resolve_analysis_json(json, alloc_, validated));
  }
}

// ============================================================
// ObUtf8mb4BinCharFilter unit tests
// ============================================================

class TestUtf8mb4BinCharFilter : public ::testing::Test
{
protected:
  void SetUp() override
  {
    alloc_.reset();
  }
  void TearDown() override
  {
    reset_filter();
  }

  void reset_filter()
  {
    if (OB_NOT_NULL(filter_)) {
      filter_->reset();
      filter_->~ObUtf8mb4BinCharFilter();
      alloc_.free(filter_);
      filter_ = nullptr;
    }
  }

  int create_filter(ObCollationType src_collation = CS_TYPE_UTF8MB4_GENERAL_CI)
  {
    int ret = OB_SUCCESS;
    void *buf = nullptr;
    if (OB_ISNULL(buf = alloc_.alloc(sizeof(ObUtf8mb4BinCharFilter)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      filter_ = new (buf) ObUtf8mb4BinCharFilter();
      ObUtf8mb4BinCharFilterSpec spec;
      spec.src_collation_ = src_collation;
      if (OB_FAIL(filter_->init(spec, alloc_))) {
        filter_->~ObUtf8mb4BinCharFilter();
        alloc_.free(filter_);
        filter_ = nullptr;
      }
    }
    return ret;
  }

  ObArenaAllocator alloc_{ObMemAttr(OB_SYS_TENANT_ID, "TestUtf8mb4")};
  ObUtf8mb4BinCharFilter *filter_ = nullptr;
};

TEST_F(TestUtf8mb4BinCharFilter, init)
{
  {
    SCOPED_TRACE("success_utf8mb4");
    ASSERT_EQ(OB_SUCCESS, create_filter(CS_TYPE_UTF8MB4_GENERAL_CI));
    ASSERT_TRUE(filter_ != nullptr);
  }
  {
    SCOPED_TRACE("double_init");
    ObUtf8mb4BinCharFilterSpec spec;
    spec.src_collation_ = CS_TYPE_UTF8MB4_GENERAL_CI;
    ASSERT_EQ(OB_INIT_TWICE, filter_->init(spec, alloc_));
  }
  reset_filter();
  {
    SCOPED_TRACE("invalid_type");
    void *buf = alloc_.alloc(sizeof(ObUtf8mb4BinCharFilter));
    ASSERT_TRUE(buf != nullptr);
    filter_ = new (buf) ObUtf8mb4BinCharFilter();
    ObCharFilterSpec invalid_spec;
    invalid_spec.type_ = ObCharFilterType::CHAR_FILTER_TYPE_LOWERCASE_LEGACY;
    ASSERT_EQ(OB_INVALID_ARGUMENT, filter_->init(invalid_spec, alloc_));
  }
  reset_filter();
  {
    SCOPED_TRACE("invalid_collation");
    void *buf = alloc_.alloc(sizeof(ObUtf8mb4BinCharFilter));
    ASSERT_TRUE(buf != nullptr);
    filter_ = new (buf) ObUtf8mb4BinCharFilter();
    ObUtf8mb4BinCharFilterSpec spec;
    spec.src_collation_ = CS_TYPE_INVALID;
    ASSERT_EQ(OB_INVALID_ARGUMENT, filter_->init(spec, alloc_));
  }
}

TEST_F(TestUtf8mb4BinCharFilter, filter)
{
  {
    SCOPED_TRACE("not_inited");
    void *buf = alloc_.alloc(sizeof(ObUtf8mb4BinCharFilter));
    ASSERT_TRUE(buf != nullptr);
    filter_ = new (buf) ObUtf8mb4BinCharFilter();
    const char *output = nullptr;
    int64_t output_len = 0;
    ASSERT_EQ(OB_NOT_INIT, filter_->filter("test", 4, output, output_len));
  }
  reset_filter();
  {
    SCOPED_TRACE("utf8mb4_no_conversion");
    ASSERT_EQ(OB_SUCCESS, create_filter(CS_TYPE_UTF8MB4_GENERAL_CI));
    const char *input = "hello world";
    const char *output = nullptr;
    int64_t output_len = 0;
    ASSERT_EQ(OB_SUCCESS, filter_->filter(input, strlen(input), output, output_len));
    EXPECT_EQ(input, output);
    EXPECT_EQ(static_cast<int64_t>(strlen(input)), output_len);
  }
  {
    SCOPED_TRACE("empty_input");
    const char *input = "";
    const char *output = nullptr;
    int64_t output_len = 0;
    ASSERT_EQ(OB_SUCCESS, filter_->filter(input, 0, output, output_len));
  }
  {
    SCOPED_TRACE("null_input");
    const char *output = nullptr;
    int64_t output_len = 0;
    ASSERT_EQ(OB_SUCCESS, filter_->filter(nullptr, 0, output, output_len));
  }
  reset_filter();
  {
    SCOPED_TRACE("non_utf8mb4_converts");
    ASSERT_EQ(OB_SUCCESS, create_filter(CS_TYPE_GBK_CHINESE_CI));
    const char *input = "OceanBase";
    const char *output = nullptr;
    int64_t output_len = 0;
    ASSERT_EQ(OB_SUCCESS, filter_->filter(input, strlen(input), output, output_len));
    EXPECT_TRUE(output != nullptr);
    EXPECT_GT(output_len, 0);
    EXPECT_EQ(static_cast<int64_t>(strlen(input)), output_len);
  }
}

TEST_F(TestUtf8mb4BinCharFilter, reset_and_reuse)
{
  ASSERT_EQ(OB_SUCCESS, create_filter(CS_TYPE_UTF8MB4_GENERAL_CI));
  ASSERT_TRUE(filter_ != nullptr);

  filter_->reset();
  ObUtf8mb4BinCharFilterSpec spec;
  spec.src_collation_ = CS_TYPE_GBK_CHINESE_CI;
  ASSERT_EQ(OB_SUCCESS, filter_->init(spec, alloc_));

  const char *input = "Test";
  const char *output = nullptr;
  int64_t output_len = 0;
  ASSERT_EQ(OB_SUCCESS, filter_->filter(input, strlen(input), output, output_len));
  EXPECT_GT(output_len, 0);
}

// ============================================================
// Standard analyzer end-to-end tokenization tests
// ============================================================

TEST_F(TestFTSAnalyzer, standard_analyzer_tokenization)
{
  ASSERT_EQ(OB_SUCCESS, init_helper("analyzer.1",
      "{\"analyzer\": \"standard\"}"));
  ObFtTokArr tokens;

  {
    SCOPED_TRACE("basic");
    ASSERT_EQ(OB_SUCCESS, segment_and_collect("The Quick Brown Fox", tokens));
    ASSERT_EQ(4, tokens.count());
    FTS_EXPECT_STR_EQ(tokens, 0, "the");
    FTS_EXPECT_STR_EQ(tokens, 1, "quick");
    FTS_EXPECT_STR_EQ(tokens, 2, "brown");
    FTS_EXPECT_STR_EQ(tokens, 3, "fox");
  }
  {
    SCOPED_TRACE("short_tokens");
    ASSERT_EQ(OB_SUCCESS, segment_and_collect("a bc def", tokens));
    ASSERT_EQ(3, tokens.count());
    FTS_EXPECT_STR_EQ(tokens, 0, "a");
    FTS_EXPECT_STR_EQ(tokens, 1, "bc");
    FTS_EXPECT_STR_EQ(tokens, 2, "def");
  }
  {
    SCOPED_TRACE("punctuation");
    ASSERT_EQ(OB_SUCCESS, segment_and_collect("Hello, world! How are you?", tokens));
    ASSERT_EQ(5, tokens.count());
    FTS_EXPECT_STR_EQ(tokens, 0, "hello");
    FTS_EXPECT_STR_EQ(tokens, 1, "world");
    FTS_EXPECT_STR_EQ(tokens, 2, "how");
    FTS_EXPECT_STR_EQ(tokens, 3, "are");
    FTS_EXPECT_STR_EQ(tokens, 4, "you");
  }
  {
    SCOPED_TRACE("empty_input");
    int ret = segment_and_collect("", tokens);
    ASSERT_TRUE(OB_SUCCESS != ret || 0 == tokens.count());
  }
  {
    SCOPED_TRACE("chinese");
    ASSERT_EQ(OB_SUCCESS, segment_and_collect(u8"你好世界", tokens));
    ASSERT_GE(tokens.count(), 1);
  }
  {
    SCOPED_TRACE("mixed_cjk_english");
    ASSERT_EQ(OB_SUCCESS, segment_and_collect(u8"OceanBase数据库 is great", tokens));
    ASSERT_GE(tokens.count(), 3);
    bool found_oceanbase = false;
    for (int64_t i = 0; i < tokens.count(); ++i) {
      if (0 == tokens.at(i).compare(ObString::make_string("oceanbase"))) {
        found_oceanbase = true;
      }
    }
    EXPECT_TRUE(found_oceanbase);
  }
  {
    SCOPED_TRACE("numbers");
    ASSERT_EQ(OB_SUCCESS, segment_and_collect("version 4.6.0 released 2025", tokens));
    ASSERT_GE(tokens.count(), 3);
  }
  {
    SCOPED_TRACE("lowercase_mixed_case");
    ASSERT_EQ(OB_SUCCESS, segment_and_collect("OceanBase HELLO world", tokens));
    ASSERT_EQ(3, tokens.count());
    FTS_EXPECT_STR_EQ(tokens, 0, "oceanbase");
    FTS_EXPECT_STR_EQ(tokens, 1, "hello");
    FTS_EXPECT_STR_EQ(tokens, 2, "world");
  }
}

// ============================================================
// Standard analyzer: position tracking and phrase match
// ============================================================

TEST_F(TestFTSAnalyzer, standard_analyzer_position_and_phrase)
{
  {
    SCOPED_TRACE("position_starts_at_zero");
    ASSERT_EQ(OB_SUCCESS, init_helper("analyzer.1",
        "{\"analyzer\": \"standard\"}"));

    ObObjMeta meta;
    meta.set_varchar();
    meta.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    meta.set_collation_level(CS_LEVEL_IMPLICIT);

    ObFTAnalyzerParam param;
    param.legacy_tokenizer_type_ = helper_.get_parser_name().to_tokenizer_type();
    param.parser_property_ = &helper_.get_parser_property();
    param.fts_index_type_ = share::schema::OB_FTS_INDEX_TYPE_MATCH;
    param.process_token_flag_ = helper_.get_process_token_flags();
    param.meta_ = meta;
    param.alloc_ = &allocator_;

    ObFTSAnalyzer *analyzer = nullptr;
    ObITokenStream *stream = nullptr;
    ASSERT_EQ(OB_SUCCESS, helper_.create_analyzer(param, analyzer));
    ASSERT_TRUE(OB_NOT_NULL(analyzer));

    const char *text = "Alpha Beta Gamma";
    ASSERT_EQ(OB_SUCCESS, analyzer->analyze(text, strlen(text), allocator_, stream));
    ASSERT_TRUE(OB_NOT_NULL(stream));

    ObTokenAttr token;
    int64_t position = -1;
    ObSEArray<int64_t, 16> positions;
    while (OB_SUCCESS == stream->get_next_token(token)) {
      position += token.pos_inc_;
      ASSERT_EQ(OB_SUCCESS, positions.push_back(position));
    }

    ASSERT_EQ(3, positions.count());
    EXPECT_EQ(0, positions.at(0));
    EXPECT_EQ(1, positions.at(1));
    EXPECT_EQ(2, positions.at(2));

    if (OB_NOT_NULL(analyzer)) {
      analyzer->~ObFTSAnalyzer();
      allocator_.free(analyzer);
    }
  }
  helper_.reset();
  {
    SCOPED_TRACE("phrase_match");
    ObString parser_name = ObString::make_string("analyzer.1");
    ObString parser_props = ObString::make_string("{\"analyzer\": \"standard\"}");
    ASSERT_EQ(OB_SUCCESS, helper_.init(&allocator_, parser_name, parser_props,
        share::schema::OB_FTS_INDEX_TYPE_PHRASE_MATCH));

    ObObjMeta meta;
    meta.set_varchar();
    meta.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    meta.set_collation_level(CS_LEVEL_IMPLICIT);

    int64_t doc_length = 0;
    ObFTTokenMap ft_token_map;
    ASSERT_EQ(OB_SUCCESS, ft_token_map.create(64, ObMemAttr(OB_SYS_TENANT_ID, "TestMap")));

    const char *text = "Hello World Hello";
    ASSERT_EQ(OB_SUCCESS, helper_.segment(meta, text, strlen(text), doc_length, ft_token_map));

    EXPECT_EQ(3, doc_length);
    EXPECT_EQ(2, ft_token_map.size());
  }
}

// ============================================================
// Space parser tests
// ============================================================

TEST_F(TestFTSAnalyzer, space_parser)
{
  ASSERT_EQ(OB_SUCCESS, init_helper("space.1"));
  ObFtTokArr tokens;

  {
    SCOPED_TRACE("basic");
    ASSERT_EQ(OB_SUCCESS, segment_and_collect("Hello World Test", tokens));
    ASSERT_EQ(3, tokens.count());
    FTS_EXPECT_STR_EQ(tokens, 0, "hello");
    FTS_EXPECT_STR_EQ(tokens, 1, "world");
    FTS_EXPECT_STR_EQ(tokens, 2, "test");
  }
  {
    SCOPED_TRACE("empty");
    int ret = segment_and_collect("", tokens);
    ASSERT_TRUE(OB_SUCCESS != ret || 0 == tokens.count());
  }
  {
    SCOPED_TRACE("single_word");
    ASSERT_EQ(OB_SUCCESS, segment_and_collect("HELLO", tokens));
    ASSERT_EQ(1, tokens.count());
    FTS_EXPECT_STR_EQ(tokens, 0, "hello");
  }
  {
    SCOPED_TRACE("multiple_spaces");
    ASSERT_EQ(OB_SUCCESS, segment_and_collect("  foo   bar  ", tokens));
    ASSERT_EQ(2, tokens.count());
    FTS_EXPECT_STR_EQ(tokens, 0, "foo");
    FTS_EXPECT_STR_EQ(tokens, 1, "bar");
  }
  {
    SCOPED_TRACE("fulltext");
    ASSERT_EQ(OB_SUCCESS, segment_and_collect(
        "OceanBase fulltext search is No.1 in the world.", tokens));
    ASSERT_EQ(4, tokens.count());
    FTS_EXPECT_STR_EQ(tokens, 0, "oceanbase");
    FTS_EXPECT_STR_EQ(tokens, 1, "fulltext");
    FTS_EXPECT_STR_EQ(tokens, 2, "search");
    FTS_EXPECT_STR_EQ(tokens, 3, "world");
  }
  {
    SCOPED_TRACE("min_max_word_len");
    ASSERT_EQ(OB_SUCCESS, segment_and_collect("ab", tokens));
    ASSERT_EQ(0, tokens.count());

    ASSERT_EQ(OB_SUCCESS, segment_and_collect("abc", tokens));
    ASSERT_EQ(1, tokens.count());
    FTS_EXPECT_STR_EQ(tokens, 0, "abc");

    ASSERT_EQ(OB_SUCCESS, segment_and_collect("abcd", tokens));
    ASSERT_EQ(1, tokens.count());
    FTS_EXPECT_STR_EQ(tokens, 0, "abcd");

    const char *word_84 = "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz123456";
    ASSERT_EQ(OB_SUCCESS, segment_and_collect(word_84, tokens));
    ASSERT_EQ(1, tokens.count());

    const char *word_85 = "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz1234567";
    ASSERT_EQ(OB_SUCCESS, segment_and_collect(word_85, tokens));
    ASSERT_EQ(0, tokens.count());
  }
}

// ============================================================
// Ngram parser tests
// ============================================================

TEST_F(TestFTSAnalyzer, ngram_parser)
{
  ASSERT_EQ(OB_SUCCESS, init_helper("ngram.1"));
  ObFtTokArr tokens;

  {
    SCOPED_TRACE("basic");
    ASSERT_EQ(OB_SUCCESS, segment_and_collect("abcde", tokens));
    ASSERT_EQ(4, tokens.count());
    FTS_EXPECT_STR_EQ(tokens, 0, "ab");
    FTS_EXPECT_STR_EQ(tokens, 1, "bc");
    FTS_EXPECT_STR_EQ(tokens, 2, "cd");
    FTS_EXPECT_STR_EQ(tokens, 3, "de");
  }
  {
    SCOPED_TRACE("chinese");
    ASSERT_EQ(OB_SUCCESS, segment_and_collect(u8"你好世界", tokens));
    ASSERT_EQ(3, tokens.count());
    FTS_EXPECT_STR_EQ(tokens, 0, u8"你好");
    FTS_EXPECT_STR_EQ(tokens, 1, u8"好世");
    FTS_EXPECT_STR_EQ(tokens, 2, u8"世界");
  }
  {
    SCOPED_TRACE("uppercase");
    ASSERT_EQ(OB_SUCCESS, segment_and_collect("ABCD", tokens));
    ASSERT_EQ(3, tokens.count());
    FTS_EXPECT_STR_EQ(tokens, 0, "ab");
    FTS_EXPECT_STR_EQ(tokens, 1, "bc");
    FTS_EXPECT_STR_EQ(tokens, 2, "cd");
  }
  {
    SCOPED_TRACE("corner_cases");
    ASSERT_EQ(OB_SUCCESS, segment_and_collect("f", tokens));
    ASSERT_EQ(0, tokens.count());

    ASSERT_EQ(OB_SUCCESS, segment_and_collect(" f", tokens));
    ASSERT_EQ(0, tokens.count());

    ASSERT_EQ(OB_SUCCESS, segment_and_collect(" f ", tokens));
    ASSERT_EQ(0, tokens.count());

    ASSERT_EQ(OB_SUCCESS, segment_and_collect("192.168.2.3", tokens));
    ASSERT_EQ(4, tokens.count());
  }
}

// ============================================================
// Beng (Basic English) parser test
// ============================================================

TEST_F(TestFTSAnalyzer, beng_parser)
{
  ASSERT_EQ(OB_SUCCESS, init_helper("beng.1"));
  ObFtTokArr tokens;
  ASSERT_EQ(OB_SUCCESS, segment_and_collect(
      "OceanBase fulltext search is No.1 in the world.", tokens));

  ASSERT_EQ(4, tokens.count());
  FTS_EXPECT_STR_EQ(tokens, 0, "oceanbase");
  FTS_EXPECT_STR_EQ(tokens, 1, "fulltext");
  FTS_EXPECT_STR_EQ(tokens, 2, "search");
  FTS_EXPECT_STR_EQ(tokens, 3, "world");
}

// ============================================================
// IK parser tests — SMART mode
// ============================================================

#define SKIP_IF_IK_UNAVAILABLE(parser, properties)                             \
  do {                                                                         \
    int _ret = init_helper(parser, properties);                                \
    if (OB_SUCCESS != _ret) {                                                  \
      LOG_INFO("IK unittest skipped: dict unavailable", K(_ret));              \
      return;                                                                  \
    }                                                                          \
    ObSEArray<ObString, 8> _probe;                                           \
    _ret = segment_and_collect("test", _probe);                                \
    if (OB_SUCCESS != _ret) {                                                  \
      LOG_INFO("IK unittest skipped: segment probe failed", K(_ret));          \
      return;                                                                  \
    }                                                                          \
  } while (0)

TEST_F(TestFTSAnalyzer, ik_smart)
{
  SKIP_IF_IK_UNAVAILABLE("ik.1", IK_SMART_PROPERTIES);
  ObFtTokArr tokens;

  {
    SCOPED_TRACE("hello");
    ASSERT_EQ(OB_SUCCESS, segment_and_collect("hello world", tokens));
    ASSERT_EQ(2, tokens.count());
    FTS_EXPECT_STR_EQ(tokens, 0, "hello");
    FTS_EXPECT_STR_EQ(tokens, 1, "world");
  }
  {
    SCOPED_TRACE("numbers");
    ASSERT_EQ(OB_SUCCESS, segment_and_collect(u8"20多人 20几万", tokens));
    ASSERT_EQ(4, tokens.count());
    FTS_EXPECT_STR_EQ(tokens, 0, u8"20");
    FTS_EXPECT_STR_EQ(tokens, 1, u8"多人");
    FTS_EXPECT_STR_EQ(tokens, 2, u8"20");
    FTS_EXPECT_STR_EQ(tokens, 3, u8"几万");
  }
  {
    SCOPED_TRACE("date");
    ASSERT_EQ(OB_SUCCESS, segment_and_collect(u8"一九九五年12月31日", tokens));
    ASSERT_EQ(4, tokens.count());
    FTS_EXPECT_STR_EQ(tokens, 0, u8"一九九五");
    FTS_EXPECT_STR_EQ(tokens, 1, u8"年");
    FTS_EXPECT_STR_EQ(tokens, 2, u8"12月");
    FTS_EXPECT_STR_EQ(tokens, 3, u8"31日");
  }
  {
    SCOPED_TRACE("scientific");
    ASSERT_EQ(OB_SUCCESS, segment_and_collect("-2e-12 xxxx1E++300/++", tokens));
    ASSERT_EQ(2, tokens.count());
    FTS_EXPECT_STR_EQ(tokens, 0, "2e-12");
    FTS_EXPECT_STR_EQ(tokens, 1, "xxxx1e++300");
  }
  {
    SCOPED_TRACE("chinese_compound");
    ASSERT_EQ(OB_SUCCESS, segment_and_collect(u8"中华人民共和国人民大会堂", tokens));
    ASSERT_EQ(2, tokens.count());
    FTS_EXPECT_STR_EQ(tokens, 0, u8"中华人民共和国");
    FTS_EXPECT_STR_EQ(tokens, 1, u8"人民大会堂");
  }
  {
    SCOPED_TRACE("address");
    ASSERT_EQ(OB_SUCCESS, segment_and_collect(u8"古田县城关六一四路四百零五号", tokens));
    ASSERT_EQ(5, tokens.count());
    FTS_EXPECT_STR_EQ(tokens, 0, u8"古田县");
    FTS_EXPECT_STR_EQ(tokens, 1, u8"城关");
    FTS_EXPECT_STR_EQ(tokens, 2, u8"六一四");
    FTS_EXPECT_STR_EQ(tokens, 3, u8"路");
    FTS_EXPECT_STR_EQ(tokens, 4, u8"四百零五号");
  }
  {
    SCOPED_TRACE("url_email");
    ASSERT_EQ(OB_SUCCESS, segment_and_collect(
        u8"作者博客：www.baidu.com  电子邮件地址：squarious@gmail.com", tokens));
    ASSERT_EQ(6, tokens.count());
    FTS_EXPECT_STR_EQ(tokens, 0, u8"作者");
    FTS_EXPECT_STR_EQ(tokens, 1, u8"博客");
    FTS_EXPECT_STR_EQ(tokens, 2, "www.baidu.com");
    FTS_EXPECT_STR_EQ(tokens, 3, u8"电子");
    FTS_EXPECT_STR_EQ(tokens, 4, u8"邮件地址");
    FTS_EXPECT_STR_EQ(tokens, 5, "squarious@gmail.com");
  }
  {
    SCOPED_TRACE("mixed_cjk");
    ASSERT_EQ(OB_SUCCESS, segment_and_collect(
        u8"神话电视连续剧  20092008년을 마무리 할까 합니다  "
        u8"右のテキストエリアに訳文が にちほん ", tokens));

    ObFtTokArr expect;
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"神话"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"电视连续剧"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"20092008"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"년"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"을"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"마"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"무"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"리"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"할"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"까"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"합"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"니"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"다"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"右"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"の"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"テ"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"キ"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"ス"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"ト"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"エ"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"リ"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"ア"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"に"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"訳"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"文"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"が"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"に"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"ち"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"ほ"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"ん"));
    ASSERT_TRUE(is_array_equal(expect, tokens));
  }
  {
    SCOPED_TRACE("news");
    ASSERT_EQ(OB_SUCCESS, segment_and_collect(
        u8"据路透社报道，印度尼西亚社会事务部一官员星期二(29日)"
        u8"表示，日惹市附近当地时间27日晨5时53分发生的里氏6."
        u8"2级地震已经造成至少5427人死亡，20000余人受伤，近20万人无家可归。", tokens));

    ObFtTokArr expect;
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"据"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"路透社"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"报道"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"印度尼西亚"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"社会"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"事务部"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"一"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"官员"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"星期二"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"29日"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"表示"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"日"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"惹"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"市"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"附近"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"当地时间"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"27日"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"晨"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"5时"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"53分"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"发生"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"的"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"里氏"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"6.2级"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"地震"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"已经"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"造成"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"至少"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"5427人"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"死亡"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"20000"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"余人"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"受伤"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"近"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"20"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"万人"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"无家可归"));
    ASSERT_TRUE(is_array_equal(expect, tokens));
  }
  {
    SCOPED_TRACE("comprehensive");
    ASSERT_EQ(OB_SUCCESS, segment_and_collect(
        u8"中华人民共和国人民大会堂有16人在唱《hello world》，"
        u8"十里相送 一百二十个人 1.2立方米", tokens));

    ObFtTokArr expect;
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"中华人民共和国"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"人民大会堂"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"有"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"16人"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"在唱"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"hello"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"world"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"十里"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"相送"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"一百二十"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"个人"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"1.2立方米"));
    ASSERT_TRUE(is_array_equal(expect, tokens));
  }
}

// ============================================================
// IK parser tests — MAX_WORD mode
// ============================================================

TEST_F(TestFTSAnalyzer, ik_max_word)
{
  SKIP_IF_IK_UNAVAILABLE("ik.1", IK_MAX_WORD_PROPERTIES);
  ObFtTokArr tokens;

  {
    SCOPED_TRACE("numbers");
    ASSERT_EQ(OB_SUCCESS, segment_and_collect(u8"20多人 20几万", tokens));
    ASSERT_EQ(5, tokens.count());
    FTS_EXPECT_STR_EQ(tokens, 0, u8"20");
    FTS_EXPECT_STR_EQ(tokens, 1, u8"多人");
    FTS_EXPECT_STR_EQ(tokens, 2, u8"20");
    FTS_EXPECT_STR_EQ(tokens, 3, u8"几万");
    FTS_EXPECT_STR_EQ(tokens, 4, u8"万");
  }
  {
    SCOPED_TRACE("url_email");
    ASSERT_EQ(OB_SUCCESS, segment_and_collect(
        u8"作者博客：www.baidu.com  电子邮件地址：squarious@gmail.com", tokens));

    ObFtTokArr expect;
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"作者"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"博客"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"www.baidu.com"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"www"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"baidu"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"com"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"电子邮件"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"电子"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"邮件地址"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"邮件"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"地址"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"squarious@gmail.com"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"squarious"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"gmail"));
    ASSERT_EQ(OB_SUCCESS, ft_expect_push(expect, u8"com"));
    ASSERT_TRUE(is_array_equal(expect, tokens));
  }
}

// ============================================================
// ObFTParseHelper::segment() — legacy parser integration tests
// ============================================================

TEST_F(TestFTSAnalyzer, helper_segment_legacy)
{
  ObObjMeta meta;
  meta.set_varchar();
  meta.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  meta.set_collation_level(CS_LEVEL_IMPLICIT);

  {
    SCOPED_TRACE("space");
    ASSERT_EQ(OB_SUCCESS, init_helper("space.1"));

    int64_t doc_length = 0;
    ObFTTokenMap ft_token_map;
    ASSERT_EQ(OB_SUCCESS, ft_token_map.create(64, ObMemAttr(OB_SYS_TENANT_ID, "TestMap")));

    const char *text = "The Quick Brown Fox";
    ASSERT_EQ(OB_SUCCESS, helper_.segment(meta, text, strlen(text), doc_length, ft_token_map));
    EXPECT_GT(doc_length, 0);
    EXPECT_GT(ft_token_map.size(), 0);
  }
  helper_.reset();
  {
    SCOPED_TRACE("space_fulltext");
    ASSERT_EQ(OB_SUCCESS, init_helper("space.1"));

    int64_t doc_length = 0;
    ObFTTokenMap ft_token_map;
    ASSERT_EQ(OB_SUCCESS, ft_token_map.create(64, ObMemAttr(OB_SYS_TENANT_ID, "TestMap")));

    const char *text = "OceanBase fulltext search is No.1 in the world.";
    ASSERT_EQ(OB_SUCCESS, helper_.segment(meta, text, strlen(text), doc_length, ft_token_map));
    EXPECT_EQ(4, doc_length);
    EXPECT_EQ(4, ft_token_map.size());
  }
  helper_.reset();
  {
    SCOPED_TRACE("ngram");
    ASSERT_EQ(OB_SUCCESS, init_helper("ngram.1"));

    int64_t doc_length = 0;
    ObFTTokenMap ft_token_map;
    ASSERT_EQ(OB_SUCCESS, ft_token_map.create(64, ObMemAttr(OB_SYS_TENANT_ID, "TestMap")));

    const char *text = "abcde";
    ASSERT_EQ(OB_SUCCESS, helper_.segment(meta, text, strlen(text), doc_length, ft_token_map));
    EXPECT_EQ(4, doc_length);
    EXPECT_EQ(4, ft_token_map.size());
  }
  helper_.reset();
  {
    SCOPED_TRACE("ngram_fulltext");
    ASSERT_EQ(OB_SUCCESS, init_helper("ngram.1"));

    int64_t doc_length = 0;
    ObFTTokenMap ft_token_map;
    ASSERT_EQ(OB_SUCCESS, ft_token_map.create(64, ObMemAttr(OB_SYS_TENANT_ID, "TestMap")));

    const char *text = "OceanBase fulltext search is No.1 in the world.";
    ASSERT_EQ(OB_SUCCESS, helper_.segment(meta, text, strlen(text), doc_length, ft_token_map));
    EXPECT_EQ(27, ft_token_map.size());
  }
}

// ============================================================
// ObFTParseHelper::segment() / init() negative cases
// ============================================================

int segment_with_meta(ObFTParseHelper &helper, ObFTTokenMap &ft_token_map, const ObObjMeta &meta,
    const char *text, const int64_t text_len, int64_t &doc_length)
{
  return helper.segment(meta, text, text_len, doc_length, ft_token_map);
}

TEST_F(TestFTSAnalyzer, helper_segment_negative)
{
  ObObjMeta meta;
  meta.set_varchar();
  meta.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  meta.set_collation_level(CS_LEVEL_IMPLICIT);

  {
    SCOPED_TRACE("not_inited");
    int64_t doc_length = 0;
    ObFTTokenMap ft_token_map;
    ASSERT_EQ(OB_SUCCESS, ft_token_map.create(64, ObMemAttr(OB_SYS_TENANT_ID, "TestMapNeg")));

    const char *text = "hello";
    ASSERT_EQ(OB_NOT_INIT,
        segment_with_meta(helper_, ft_token_map, meta, text, static_cast<int64_t>(strlen(text)), doc_length));
  }

  ASSERT_EQ(OB_SUCCESS, init_helper("space.1"));

  {
    SCOPED_TRACE("init_twice");
    ASSERT_EQ(OB_INIT_TWICE, init_helper("space.1"));
  }
  {
    SCOPED_TRACE("fulltext_null");
    int64_t doc_length = 0;
    ObFTTokenMap ft_token_map;
    ASSERT_EQ(OB_SUCCESS, ft_token_map.create(64, ObMemAttr(OB_SYS_TENANT_ID, "TestMapNeg")));
    ASSERT_EQ(OB_INVALID_ARGUMENT,
        segment_with_meta(helper_, ft_token_map, meta, nullptr, 10, doc_length));
  }
  {
    SCOPED_TRACE("fulltext_len_zero");
    int64_t doc_length = 0;
    ObFTTokenMap ft_token_map;
    ASSERT_EQ(OB_SUCCESS, ft_token_map.create(64, ObMemAttr(OB_SYS_TENANT_ID, "TestMapNeg")));
    const char empty[] = "";
    ASSERT_EQ(OB_INVALID_ARGUMENT,
        segment_with_meta(helper_, ft_token_map, meta, empty, 0, doc_length));
  }
  {
    SCOPED_TRACE("fulltext_len_negative");
    int64_t doc_length = 0;
    ObFTTokenMap ft_token_map;
    ASSERT_EQ(OB_SUCCESS, ft_token_map.create(64, ObMemAttr(OB_SYS_TENANT_ID, "TestMapNeg")));
    const char *text = "abc";
    ASSERT_EQ(OB_INVALID_ARGUMENT,
        segment_with_meta(helper_, ft_token_map, meta, text, -1, doc_length));
  }
  {
    SCOPED_TRACE("collation_invalid");
    int64_t doc_length = 0;
    ObFTTokenMap ft_token_map;
    ASSERT_EQ(OB_SUCCESS, ft_token_map.create(64, ObMemAttr(OB_SYS_TENANT_ID, "TestMapNeg")));

    ObObjMeta bad_meta;
    bad_meta.set_varchar();
    bad_meta.set_collation_type(CS_TYPE_INVALID);
    bad_meta.set_collation_level(CS_LEVEL_IMPLICIT);

    const char *text = "hello";
    ASSERT_EQ(OB_INVALID_ARGUMENT, helper_.segment(bad_meta, text, static_cast<int64_t>(strlen(text)),
                doc_length, ft_token_map));
  }
  {
    SCOPED_TRACE("collation_pinyin_mark");
    int64_t doc_length = 0;
    ObFTTokenMap ft_token_map;
    ASSERT_EQ(OB_SUCCESS, ft_token_map.create(64, ObMemAttr(OB_SYS_TENANT_ID, "TestMapNeg")));

    ObObjMeta bad_meta;
    bad_meta.set_varchar();
    bad_meta.set_collation_type(CS_TYPE_PINYIN_BEGIN_MARK);
    bad_meta.set_collation_level(CS_LEVEL_IMPLICIT);

    const char *text = "hello";
    ASSERT_EQ(OB_INVALID_ARGUMENT, helper_.segment(bad_meta, text, static_cast<int64_t>(strlen(text)),
                doc_length, ft_token_map));
  }
}

// ============================================================
// ObFTParseHelper::segment() — analyzer new path
// ============================================================

TEST_F(TestFTSAnalyzer, helper_segment_analyzer)
{
  ASSERT_EQ(OB_SUCCESS, init_helper("analyzer.1",
      "{\"analyzer\": \"standard\"}"));

  ObObjMeta meta;
  meta.set_varchar();
  meta.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  meta.set_collation_level(CS_LEVEL_IMPLICIT);

  {
    SCOPED_TRACE("basic");
    int64_t doc_length = 0;
    ObFTTokenMap ft_token_map;
    ASSERT_EQ(OB_SUCCESS, ft_token_map.create(64, ObMemAttr(OB_SYS_TENANT_ID, "TestMap")));

    const char *text = "The Quick Brown Fox";
    ASSERT_EQ(OB_SUCCESS, helper_.segment(meta, text, strlen(text), doc_length, ft_token_map));
    EXPECT_EQ(4, doc_length);
    EXPECT_EQ(4, ft_token_map.size());
  }
  {
    SCOPED_TRACE("repeated");
    for (int i = 0; i < 5; ++i) {
      int64_t doc_length = 0;
      ObFTTokenMap ft_token_map;
      ASSERT_EQ(OB_SUCCESS, ft_token_map.create(64, ObMemAttr(OB_SYS_TENANT_ID, "TestMap")));

      const char *text = "Hello World";
      ASSERT_EQ(OB_SUCCESS, helper_.segment(meta, text, strlen(text), doc_length, ft_token_map));
      EXPECT_EQ(2, doc_length) << "iteration " << i;
      EXPECT_EQ(2, ft_token_map.size()) << "iteration " << i;
    }
  }
  {
    SCOPED_TRACE("invalid_collation");
    ObObjMeta bad_meta;
    bad_meta.set_varchar();
    bad_meta.set_collation_type(CS_TYPE_INVALID);
    bad_meta.set_collation_level(CS_LEVEL_IMPLICIT);

    int64_t doc_length = 0;
    ObFTTokenMap ft_token_map;
    ASSERT_EQ(OB_SUCCESS, ft_token_map.create(64, ObMemAttr(OB_SYS_TENANT_ID, "TestMap")));

    const char *text = "hello";
    ASSERT_EQ(OB_INVALID_ARGUMENT,
        helper_.segment(bad_meta, text, strlen(text), doc_length, ft_token_map));
  }
}

// ============================================================
// ObFTParseHelper::check_is_the_same() — analyzer path
// ============================================================

TEST_F(TestFTSAnalyzer, check_is_the_same)
{
  ASSERT_EQ(OB_SUCCESS, init_helper("analyzer.1",
      "{\"analyzer\": \"standard\"}"));
  bool is_same = false;

  {
    SCOPED_TRACE("same_json");
    ASSERT_EQ(OB_SUCCESS, helper_.check_is_the_same(
        ObString::make_string("analyzer.1"),
        ObString::make_string("{\"analyzer\": \"standard\"}"),
        share::schema::OB_FTS_INDEX_TYPE_MATCH,
        is_same));
    EXPECT_TRUE(is_same);
  }
  {
    SCOPED_TRACE("different_json");
    ASSERT_EQ(OB_SUCCESS, helper_.check_is_the_same(
        ObString::make_string("analyzer.1"),
        ObString::make_string("{\"analyzer\": \"english\"}"),
        share::schema::OB_FTS_INDEX_TYPE_MATCH,
        is_same));
    EXPECT_FALSE(is_same);
  }
  {
    SCOPED_TRACE("different_index_type");
    ASSERT_EQ(OB_SUCCESS, helper_.check_is_the_same(
        ObString::make_string("analyzer.1"),
        ObString::make_string("{\"analyzer\": \"standard\"}"),
        share::schema::OB_FTS_INDEX_TYPE_PHRASE_MATCH,
        is_same));
    EXPECT_FALSE(is_same);
  }
  {
    SCOPED_TRACE("analyzer_vs_legacy");
    ASSERT_EQ(OB_SUCCESS, helper_.check_is_the_same(
        ObString::make_string("space.1"),
        ObString::make_string(DEFAULT_PROPERTIES),
        share::schema::OB_FTS_INDEX_TYPE_MATCH,
        is_same));
    EXPECT_FALSE(is_same);
  }
  {
    SCOPED_TRACE("bare_name");
    ASSERT_EQ(OB_SUCCESS, helper_.check_is_the_same(
        ObString::make_string("analyzer"),
        ObString::make_string("{\"analyzer\": \"standard\"}"),
        share::schema::OB_FTS_INDEX_TYPE_MATCH,
        is_same));
    EXPECT_TRUE(is_same);
  }
  helper_.reset();
  {
    SCOPED_TRACE("legacy_vs_analyzer");
    ASSERT_EQ(OB_SUCCESS, init_helper("space.1"));
    ASSERT_EQ(OB_SUCCESS, helper_.check_is_the_same(
        ObString::make_string("analyzer.1"),
        ObString::make_string("{\"analyzer\": \"standard\"}"),
        share::schema::OB_FTS_INDEX_TYPE_MATCH,
        is_same));
    EXPECT_FALSE(is_same);
  }
}

// ============================================================
// reset() then re-init — spec released and reusable
// ============================================================

TEST_F(TestFTSAnalyzer, helper_reset_reinit)
{
  ObFtTokArr tokens;

  {
    SCOPED_TRACE("analyzer_to_analyzer");
    ASSERT_EQ(OB_SUCCESS, init_helper("analyzer.1",
        "{\"analyzer\": \"standard\"}"));
    ASSERT_TRUE(OB_NOT_NULL(helper_.get_analyzer_spec()));

    ASSERT_EQ(OB_SUCCESS, segment_and_collect("Hello World", tokens));
    ASSERT_EQ(2, tokens.count());

    helper_.reset();
    ASSERT_TRUE(OB_ISNULL(helper_.get_analyzer_spec()));

    ASSERT_EQ(OB_SUCCESS, init_helper("analyzer.1",
        "{\"analyzer\": \"standard\"}"));
    ASSERT_TRUE(OB_NOT_NULL(helper_.get_analyzer_spec()));

    ASSERT_EQ(OB_SUCCESS, segment_and_collect("Foo Bar Baz", tokens));
    ASSERT_EQ(3, tokens.count());
    FTS_EXPECT_STR_EQ(tokens, 0, "foo");
    FTS_EXPECT_STR_EQ(tokens, 1, "bar");
    FTS_EXPECT_STR_EQ(tokens, 2, "baz");
  }
  helper_.reset();
  {
    SCOPED_TRACE("analyzer_to_legacy");
    ASSERT_EQ(OB_SUCCESS, init_helper("analyzer.1",
        "{\"analyzer\": \"standard\"}"));
    ASSERT_TRUE(helper_.is_analyzer_parser());

    helper_.reset();

    ASSERT_EQ(OB_SUCCESS, init_helper("space.1"));
    ASSERT_TRUE(helper_.is_legacy_parser());
    ASSERT_TRUE(OB_ISNULL(helper_.get_analyzer_spec()));

    ASSERT_EQ(OB_SUCCESS, segment_and_collect("Hello World Test", tokens));
    ASSERT_EQ(3, tokens.count());
  }
  helper_.reset();
  {
    SCOPED_TRACE("legacy_to_analyzer");
    ASSERT_EQ(OB_SUCCESS, init_helper("space.1"));
    ASSERT_TRUE(helper_.is_legacy_parser());

    helper_.reset();

    ASSERT_EQ(OB_SUCCESS, init_helper("analyzer.1",
        "{\"analyzer\": \"standard\"}"));
    ASSERT_TRUE(helper_.is_analyzer_parser());

    ASSERT_EQ(OB_SUCCESS, segment_and_collect("Hello World", tokens));
    ASSERT_EQ(2, tokens.count());
    FTS_EXPECT_STR_EQ(tokens, 0, "hello");
    FTS_EXPECT_STR_EQ(tokens, 1, "world");
  }
}

// ============================================================
// create_analyzer() / init() negative cases
// ============================================================

TEST_F(TestFTSAnalyzer, create_analyzer_negative)
{
  {
    SCOPED_TRACE("not_inited");
    ObObjMeta meta;
    meta.set_varchar();
    meta.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    meta.set_collation_level(CS_LEVEL_IMPLICIT);

    ObFTAnalyzerParam param;
    param.meta_ = meta;
    param.alloc_ = &allocator_;
    param.parser_property_ = &helper_.get_parser_property();
    param.fts_index_type_ = share::schema::OB_FTS_INDEX_TYPE_MATCH;

    ObFTSAnalyzer *analyzer = nullptr;
    ASSERT_EQ(OB_NOT_INIT, helper_.create_analyzer(param, analyzer));
    ASSERT_TRUE(OB_ISNULL(analyzer));
  }
  {
    SCOPED_TRACE("empty_analysis_json");
    ObString parser_name = ObString::make_string("analyzer.1");
    ObString empty_props = ObString::make_string("");
    ASSERT_EQ(OB_INVALID_ARGUMENT,
        helper_.init(&allocator_, parser_name, empty_props,
                     share::schema::OB_FTS_INDEX_TYPE_MATCH));
  }
  helper_.reset();
  {
    SCOPED_TRACE("invalid_json");
    ObString parser_name = ObString::make_string("analyzer.1");
    ObString bad_props = ObString::make_string("not json at all");
    ASSERT_NE(OB_SUCCESS,
        helper_.init(&allocator_, parser_name, bad_props,
                     share::schema::OB_FTS_INDEX_TYPE_MATCH));
  }
  helper_.reset();
  {
    SCOPED_TRACE("null_allocator");
    ObString parser_name = ObString::make_string("analyzer.1");
    ObString props = ObString::make_string("{\"analyzer\": \"standard\"}");
    ASSERT_EQ(OB_INVALID_ARGUMENT,
        helper_.init(nullptr, parser_name, props,
                     share::schema::OB_FTS_INDEX_TYPE_MATCH));
  }
  helper_.reset();
  {
    SCOPED_TRACE("empty_parser_name");
    ObString empty_name;
    ObString props = ObString::make_string("{\"analyzer\": \"standard\"}");
    ASSERT_EQ(OB_INVALID_ARGUMENT,
        helper_.init(&allocator_, empty_name, props,
                     share::schema::OB_FTS_INDEX_TYPE_MATCH));
  }
  helper_.reset();
  {
    SCOPED_TRACE("init_twice");
    ASSERT_EQ(OB_SUCCESS, init_helper("analyzer.1",
        "{\"analyzer\": \"standard\"}"));
    ASSERT_EQ(OB_INIT_TWICE, init_helper("analyzer.1",
        "{\"analyzer\": \"standard\"}"));
  }
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
