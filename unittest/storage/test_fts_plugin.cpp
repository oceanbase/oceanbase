/**
 * Copyright (c) 2023 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include <gtest/gtest.h>

#define USING_LOG_PREFIX STORAGE

#define protected public
#define private   public

#include "lib/ob_plugin.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/fts/ob_fts_plugin_helper.h"
#include "storage/fts/ob_fts_plugin_mgr.h"
#include "storage/fts/ob_whitespace_ft_parser.h"
#include "storage/fts/ob_fts_stop_word.h"
#include "sql/das/ob_das_utils.h"

namespace oceanbase
{

static storage::ObTenantFTPluginMgr ft_plugin_mgr(OB_SYS_TENANT_ID);

namespace storage
{

ObTenantFTPluginMgr &ObTenantFTPluginMgr::get_ft_plugin_mgr()
{
  return ft_plugin_mgr;
}

typedef common::hash::ObHashMap<ObFTWord, int64_t> ObFTWordMap;

int segment_and_calc_word_count(
    common::ObIAllocator &allocator,
    storage::ObFTParseHelper *helper,
    const common::ObCollationType &type,
    const ObString &fulltext,
    ObFTWordMap &words_count)
{
  int ret = OB_SUCCESS;
  int64_t doc_length = 0;
  common::ObSEArray<ObFTWord, 256> words;
  if (OB_ISNULL(helper)
      || OB_UNLIKELY(ObCollationType::CS_TYPE_INVALID == type
                  || ObCollationType::CS_TYPE_EXTENDED_MARK < type)
      || OB_UNLIKELY(!words_count.created())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KPC(helper), K(type), K(words_count.created()));
  } else if (OB_FAIL(helper->segment(type, fulltext.ptr(), fulltext.length(), doc_length, words))) {
    LOG_WARN("fail to segment", K(ret), KPC(helper), K(type), K(fulltext));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < words.count(); ++i) {
      const ObFTWord &ft_word = words.at(i);
      int64_t word_count = 0;
      if (OB_FAIL(words_count.get_refactored(ft_word, word_count)) && OB_HASH_NOT_EXIST != ret) {
        LOG_WARN("fail to get ft word", K(ret), K(ft_word));
      } else {
        word_count = OB_HASH_NOT_EXIST == ret ? 1 : ++word_count;
        if (OB_FAIL(words_count.set_refactored(ft_word, word_count, 1/*overwrite*/))) {
          LOG_WARN("fail to set ft word and count", K(ret), K(ft_word));
        }
      }
    }
  }
  return ret;
}

class ObTestAddWord final : public lib::ObFTParserParam::ObIAddWord
{
public:
  static const char *TEST_FULLTEXT;
  static const int64_t TEST_WORD_COUNT = 5;
  static const int64_t TEST_WORD_COUNT_WITHOUT_STOPWORD = 4;
public:
  ObTestAddWord();
  virtual ~ObTestAddWord() = default;
  virtual int operator()(
      lib::ObFTParserParam *param,
      const char *word,
      const int64_t word_len) override;
  virtual int64_t get_add_word_count() const override { return ith_word_; }
  VIRTUAL_TO_STRING_KV(K_(ith_word));
private:
  const char *words_[TEST_WORD_COUNT];
  const char *words_without_stopword_[TEST_WORD_COUNT_WITHOUT_STOPWORD];
  int64_t ith_word_;
};

const char *ObTestAddWord::TEST_FULLTEXT = "OceanBase fulltext search is No.1 in the world.";

ObTestAddWord::ObTestAddWord()
  : words_{"oceanbase", "fulltext", "search", "the", "world"},
    words_without_stopword_{"oceanbase", "fulltext", "search", "world"},
    ith_word_(0)
{
}

int ObTestAddWord::operator()(
      lib::ObFTParserParam *param,
      const char *word,
      const int64_t word_len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(param) || OB_ISNULL(word) || OB_UNLIKELY(0 >= word_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(word), KP(param), K(word_len));
  } else if (OB_UNLIKELY(0 != strncmp(words_[ith_word_], word, word_len))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the ith word isn't default word", K(ret), K(ith_word_), KCSTRING(words_[ith_word_]),
        KCSTRING(word), K(word_len));
  } else {
    ++ith_word_;
  }
  return ret;
}

class TestDefaultFTParser : public ::testing::Test
{
public:
  TestDefaultFTParser();
  virtual ~TestDefaultFTParser() = default;

  virtual void SetUp() override;
  virtual void TearDown() override;

private:
  lib::ObPluginParam plugin_param_;
  lib::ObFTParserParam ft_parser_param_;
  ObTestAddWord add_word_;
  ObWhiteSpaceFTParserDesc desc_;
  common::ObArenaAllocator allocator_;
};

TestDefaultFTParser::TestDefaultFTParser()
  : plugin_param_(),
    ft_parser_param_(),
    add_word_(),
    desc_(),
    allocator_()
{
  plugin_param_.desc_ = &desc_;
}

void TestDefaultFTParser::SetUp()
{
  ASSERT_EQ(OB_SUCCESS, desc_.init(&plugin_param_));

  ft_parser_param_.allocator_ = &allocator_;
  ft_parser_param_.add_word_ = &add_word_;
  ft_parser_param_.cs_ = common::ObCharset::get_charset(ObCollationType::CS_TYPE_UTF8MB4_BIN);
  ft_parser_param_.parser_version_ = 0x00001;
  ASSERT_TRUE(nullptr != ft_parser_param_.cs_);
}

void TestDefaultFTParser::TearDown()
{
  ft_parser_param_.reset();

  ASSERT_EQ(OB_SUCCESS, desc_.deinit(&plugin_param_));
}

TEST_F(TestDefaultFTParser, test_space_ft_parser_segment)
{
  const char *fulltext = ObTestAddWord::TEST_FULLTEXT;
  const int64_t ft_len = strlen(fulltext);

  ASSERT_EQ(OB_INVALID_ARGUMENT, ObSpaceFTParser::segment(nullptr, nullptr, 0));
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObSpaceFTParser::segment(&ft_parser_param_, nullptr, 0));
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObSpaceFTParser::segment(&ft_parser_param_, fulltext, 0));
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObSpaceFTParser::segment(&ft_parser_param_, fulltext, -1));

  ft_parser_param_.fulltext_ = fulltext;
  ft_parser_param_.ft_length_ = ft_len;

  LOG_INFO("before space segment", KCSTRING(fulltext), K(ft_len), K(ft_parser_param_));
  ASSERT_EQ(OB_SUCCESS, ObSpaceFTParser::segment(&ft_parser_param_, fulltext, ft_len));
  LOG_INFO("after space segment", KCSTRING(fulltext), K(ft_len), K(ft_parser_param_));
}

TEST_F(TestDefaultFTParser, test_space_ft_parser_segment_bug_56324268)
{
  common::ObArray<ObFTWord> words;
  ObNoStopWordAddWord add_word(ObCollationType::CS_TYPE_LATIN1_SWEDISH_CI, allocator_, words);
  const char *fulltext = "\201 想 将 数据 添加 到 数据库\f\026 ";
  const int64_t ft_len = strlen(fulltext);

  ft_parser_param_.fulltext_ = fulltext;
  ft_parser_param_.ft_length_ = ft_len;
  ft_parser_param_.add_word_ = &add_word;
  ft_parser_param_.cs_ = common::ObCharset::get_charset(ObCollationType::CS_TYPE_LATIN1_SWEDISH_CI);

  LOG_INFO("before space segment", KCSTRING(fulltext), K(ft_len), K(ft_parser_param_));
  ASSERT_EQ(OB_SUCCESS, ObSpaceFTParser::segment(&ft_parser_param_, fulltext, ft_len));
  LOG_INFO("after space segment", KCSTRING(fulltext), K(words), K(ft_len), K(ft_parser_param_));
}

TEST_F(TestDefaultFTParser, test_default_ft_parser_desc)
{
  ASSERT_EQ(OB_INVALID_ARGUMENT, desc_.segment(&ft_parser_param_));

  ft_parser_param_.fulltext_ = ObTestAddWord::TEST_FULLTEXT;
  ft_parser_param_.ft_length_ = strlen(ft_parser_param_.fulltext_);

  ASSERT_EQ(OB_SUCCESS, desc_.segment(&ft_parser_param_));

  ASSERT_EQ(OB_SUCCESS, desc_.deinit(&plugin_param_));
  ASSERT_EQ(OB_NOT_INIT, desc_.segment(&ft_parser_param_));

  ASSERT_EQ(OB_SUCCESS, desc_.init(&plugin_param_));
  ASSERT_EQ(OB_INVALID_ARGUMENT, desc_.segment(nullptr));
}

class ObTestFTPluginHelper : public ::testing::Test
{
public:
  static const char *TEST_FULLTEXT;
  static const char *file_name;
public:
  ObTestFTPluginHelper();
  virtual ~ObTestFTPluginHelper() = default;

  virtual void SetUp() override;
  virtual void TearDown() override;

private:
  share::ObPluginSoHandler handler_;
  const char *plugin_name_;
  const ObCharsetInfo *cs_;
  common::ObArenaAllocator allocator_;
};

const char *ObTestFTPluginHelper::TEST_FULLTEXT = "Test fulltext plugin.";
const char *ObTestFTPluginHelper::file_name = "libmock_ft_parser.so";

ObTestFTPluginHelper::ObTestFTPluginHelper()
  : handler_(),
    plugin_name_("mock_ft_parser"),
    cs_(nullptr),
    allocator_()
{
}

void ObTestFTPluginHelper::SetUp()
{
  ASSERT_EQ(OB_SUCCESS, handler_.open(plugin_name_, file_name));

  cs_ = common::ObCharset::get_charset(ObCollationType::CS_TYPE_UTF8MB4_BIN);
  ASSERT_TRUE(nullptr != cs_);
}

void ObTestFTPluginHelper::TearDown()
{
  cs_ = nullptr;
  ASSERT_EQ(OB_SUCCESS, handler_.close());
}

//TEST_F(ObTestFTPluginHelper, test_fts_plugin)
//{
//  int64_t version = -1;
//  ASSERT_EQ(OB_SUCCESS, handler_.get_plugin_version(version));
//  ASSERT_EQ(OB_PLUGIN_INTERFACE_VERSION, version);
//
//  int64_t size = -1;
//  ASSERT_EQ(OB_SUCCESS, handler_.get_plugin_size(size));
//  ASSERT_EQ(sizeof(lib::ObPlugin), size);
//
//  lib::ObPlugin *plugin = nullptr;
//  ASSERT_EQ(OB_SUCCESS, handler_.get_plugin(plugin));
//  ASSERT_TRUE(nullptr != plugin);
//  ASSERT_TRUE(plugin->is_valid());
//  ASSERT_EQ(lib::ObPluginType::OB_FT_PARSER_PLUGIN, plugin->type_);
//  LOG_INFO("jinzhu debug", KCSTRING(plugin->name_), KCSTRING(plugin->author_), KCSTRING(plugin->spec_));
//  ASSERT_TRUE(0 == std::strncmp("mock_ft_parser", plugin->name_, std::strlen("mock_ft_parser")));
//  ASSERT_TRUE(0 == std::strncmp(OB_PLUGIN_AUTHOR_OCEANBASE, plugin->author_, std::strlen(OB_PLUGIN_AUTHOR_OCEANBASE)));
//  ASSERT_TRUE(0 == std::strncmp("This is mock fulltext parser plugin.", plugin->spec_, std::strlen("This is mock fulltext parser plugin.")));
//  ASSERT_EQ(0x00001, plugin->version_);
//  ASSERT_EQ(lib::ObPluginLicenseType::OB_MULAN_V2_LICENSE, plugin->license_);
//  ASSERT_TRUE(nullptr != plugin->desc_);
//
//  lib::ObIFTParserDesc *desc = nullptr;
//  ASSERT_EQ(OB_SUCCESS, ObFTParseHelper::get_fulltext_parser_desc(handler_, desc));
//  ASSERT_TRUE(nullptr != desc);
//
//  ObTestAddWord test_add_word;
//  ASSERT_EQ(OB_SUCCESS, ObFTParseHelper::segment(1/*plugin_vserion*/, desc, cs_, TEST_FULLTEXT,
//        strlen(TEST_FULLTEXT), allocator_, test_add_word));
//}
//
//TEST_F(ObTestFTPluginHelper, test_main_program_for_plugin)
//{
//  ASSERT_EQ(OB_SUCCESS, handler_.close());
//  ASSERT_EQ(OB_SUCCESS, handler_.open(plugin_name_, nullptr/*use main program*/));
//
//  int64_t version = -1;
//  ASSERT_EQ(OB_SUCCESS, handler_.get_plugin_version(version));
//  ASSERT_EQ(OB_PLUGIN_INTERFACE_VERSION, version);
//
//  int64_t size = -1;
//  ASSERT_EQ(OB_SUCCESS, handler_.get_plugin_size(size));
//  ASSERT_EQ(sizeof(lib::ObPlugin), size);
//
//  lib::ObPlugin *plugin = nullptr;
//  ASSERT_EQ(OB_SUCCESS, handler_.get_plugin(plugin));
//  ASSERT_TRUE(nullptr != plugin);
//  ASSERT_TRUE(plugin->is_valid());
//  ASSERT_EQ(lib::ObPluginType::OB_FT_PARSER_PLUGIN, plugin->type_);
//  LOG_INFO("jinzhu debug", KCSTRING(plugin->name_), KCSTRING(plugin->author_), KCSTRING(plugin->spec_));
//  ASSERT_TRUE(0 == std::strncmp("mock_ft_parser", plugin->name_, std::strlen("mock_ft_parser")));
//  ASSERT_TRUE(0 == std::strncmp(OB_PLUGIN_AUTHOR_OCEANBASE, plugin->author_, std::strlen(OB_PLUGIN_AUTHOR_OCEANBASE)));
//  ASSERT_TRUE(0 == std::strncmp("This is mock fulltext parser plugin.", plugin->spec_, std::strlen("This is mock fulltext parser plugin.")));
//  ASSERT_EQ(0x00001, plugin->version_);
//  ASSERT_EQ(lib::ObPluginLicenseType::OB_MULAN_V2_LICENSE, plugin->license_);
//  ASSERT_TRUE(nullptr != plugin->desc_);
//
//  lib::ObIFTParserDesc *desc = nullptr;
//  ASSERT_EQ(OB_SUCCESS, ObFTParseHelper::get_fulltext_parser_desc(handler_, desc));
//  ASSERT_TRUE(nullptr != desc);
//
//  ObTestAddWord test_add_word;
//  ASSERT_EQ(OB_SUCCESS, ObFTParseHelper::segment(1/*plugin_vserion*/, desc, cs_, TEST_FULLTEXT,
//        strlen(TEST_FULLTEXT), allocator_, test_add_word));
//
//  ASSERT_EQ(0, ObCharset::strcmp(ObCollationType::CS_TYPE_UTF8MB4_GENERAL_CI, "OceanBase", "Oceanbase"));
//}
//
//TEST_F(ObTestFTPluginHelper, test_no_exist_symbol)
//{
//  void *sym_ptr = nullptr;
//  ASSERT_EQ(OB_SEARCH_NOT_FOUND, handler_.get_symbol_ptr("test_no_exist_symbol", sym_ptr));
//  ASSERT_EQ(OB_INVALID_ARGUMENT, handler_.get_symbol_ptr(nullptr, sym_ptr));
//
//  ASSERT_EQ(OB_SUCCESS, handler_.close());
//  ASSERT_EQ(OB_FILE_NOT_OPENED, handler_.get_symbol_ptr("test_no_exist_symbol", sym_ptr));
//
//  ASSERT_EQ(OB_ERR_SYS, handler_.open(plugin_name_, "./test_no_exist_file.so"));
//  ASSERT_EQ(OB_INVALID_ARGUMENT, handler_.open(nullptr/*plugin name*/, nullptr/*file_name*/));
//
//  ASSERT_EQ(OB_SUCCESS, handler_.open(plugin_name_, nullptr/*use main program*/));
//  ASSERT_EQ(OB_INIT_TWICE, handler_.open(plugin_name_, nullptr/*use main program*/));
//}

class ObTestFTParseHelper : public ::testing::Test
{
public:
  static const char *name_;
  typedef common::hash::ObHashMap<ObFTWord, int64_t> ObFTWordMap;
public:
  ObTestFTParseHelper();
  virtual ~ObTestFTParseHelper() = default;

  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp() override;
  virtual void TearDown() override;

private:
  const common::ObString plugin_name_;
  const common::ObCollationType cs_type_;
  common::ObArenaAllocator allocator_;
  ObFTParseHelper parse_helper_;
};

const char *ObTestFTParseHelper::name_ = "space.1";

ObTestFTParseHelper::ObTestFTParseHelper()
  : plugin_name_(STRLEN(name_), name_),
    cs_type_(ObCollationType::CS_TYPE_UTF8MB4_BIN),
    allocator_()
{
}

void ObTestFTParseHelper::SetUp()
{
  ASSERT_EQ(OB_SUCCESS, parse_helper_.init(&allocator_, plugin_name_));
}

void ObTestFTParseHelper::TearDown()
{
  parse_helper_.reset();
}

void ObTestFTParseHelper::SetUpTestCase()
{
  ASSERT_EQ(common::OB_SUCCESS, ObTenantFTPluginMgr::register_plugins());
  ASSERT_EQ(common::OB_SUCCESS, ft_plugin_mgr.init());
}

void ObTestFTParseHelper::TearDownTestCase()
{
  ft_plugin_mgr.destroy();
  ObTenantFTPluginMgr::unregister_plugins();
}

TEST_F(ObTestFTParseHelper, test_parse_fulltext)
{
  common::ObSEArray<ObFTWord, 16> words;
  int64_t doc_length = 0;
  ASSERT_EQ(OB_SUCCESS, parse_helper_.segment(cs_type_, ObTestAddWord::TEST_FULLTEXT,
        std::strlen(ObTestAddWord::TEST_FULLTEXT), doc_length, words));

  ObTestAddWord test_add_word;
  for (int64_t i = 0; i < words.count(); ++i) {
    ASSERT_TRUE(0 == strncmp(test_add_word.words_without_stopword_[i], words[i].word_.ptr(), words[i].word_.length()));
  }

  ObFTWordMap ft_word_map;
  ASSERT_EQ(OB_SUCCESS, ft_word_map.create(words.count(), "TestParse"));
  ASSERT_EQ(OB_SUCCESS, segment_and_calc_word_count(allocator_, &parse_helper_,
        cs_type_, ObTestAddWord::TEST_FULLTEXT, ft_word_map));
  ASSERT_EQ(words.count(), ft_word_map.size());

  ASSERT_EQ(OB_INVALID_ARGUMENT, parse_helper_.segment(cs_type_, nullptr, std::strlen(ObTestAddWord::TEST_FULLTEXT), doc_length, words));
  ASSERT_EQ(OB_INVALID_ARGUMENT, parse_helper_.segment(cs_type_, ObTestAddWord::TEST_FULLTEXT, 0, doc_length, words));
  ASSERT_EQ(OB_INVALID_ARGUMENT, parse_helper_.segment(cs_type_, ObTestAddWord::TEST_FULLTEXT, -1, doc_length, words));

  parse_helper_.reset();
  ASSERT_EQ(OB_NOT_INIT, parse_helper_.segment(cs_type_, ObTestAddWord::TEST_FULLTEXT,
        std::strlen(ObTestAddWord::TEST_FULLTEXT), doc_length, words));

  ASSERT_EQ(OB_INVALID_ARGUMENT, parse_helper_.init(nullptr, plugin_name_));
  ASSERT_EQ(OB_INVALID_ARGUMENT, parse_helper_.init(&allocator_, ObString()));

  ASSERT_EQ(OB_SUCCESS, parse_helper_.init(&allocator_, plugin_name_));

  ASSERT_EQ(OB_INVALID_ARGUMENT, parse_helper_.segment(CS_TYPE_INVALID, ObTestAddWord::TEST_FULLTEXT,
        std::strlen(ObTestAddWord::TEST_FULLTEXT), doc_length, words));
  ASSERT_EQ(OB_INVALID_ARGUMENT, parse_helper_.segment(CS_TYPE_EXTENDED_MARK, ObTestAddWord::TEST_FULLTEXT,
        std::strlen(ObTestAddWord::TEST_FULLTEXT), doc_length, words));

  ASSERT_EQ(OB_INIT_TWICE, parse_helper_.init(&allocator_, plugin_name_));

  parse_helper_.reset();
  ASSERT_EQ(OB_SUCCESS, parse_helper_.init(&allocator_, plugin_name_));

  parse_helper_.reset();
  ASSERT_EQ(OB_SUCCESS, parse_helper_.init(&allocator_, plugin_name_));
  ASSERT_EQ(OB_SUCCESS, parse_helper_.segment(cs_type_, ObTestAddWord::TEST_FULLTEXT,
        std::strlen(ObTestAddWord::TEST_FULLTEXT), doc_length, words));
  for (int64_t i = 0; i < words.count(); ++i) {
    ASSERT_TRUE(0 == strncmp(test_add_word.words_without_stopword_[i], words[i].word_.ptr(), words[i].word_.length()));
  }
}

TEST_F(ObTestFTParseHelper, test_min_and_max_word_len)
{
  common::ObSEArray<ObFTWord, 16> words;
  int64_t doc_length = 0;

  // word len = 2;
  const char *word_len_2 = "ab";
  ASSERT_EQ(OB_SUCCESS, parse_helper_.segment(cs_type_, word_len_2, std::strlen(word_len_2), doc_length, words));
  ASSERT_EQ(0, words.count());

  // word len = 3;
  const char *word_len_3 = "abc";
  ASSERT_EQ(OB_SUCCESS, parse_helper_.segment(cs_type_, word_len_3, std::strlen(word_len_3), doc_length, words));
  ASSERT_EQ(1, words.count());

  // word len = 4;
  const char *word_len_4 = "abcd";
  ASSERT_EQ(OB_SUCCESS, parse_helper_.segment(cs_type_, word_len_4, std::strlen(word_len_4), doc_length, words));
  ASSERT_EQ(1, words.count());

  // word len = 76;
  const char *word_len_76 = "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz";
  ASSERT_EQ(OB_SUCCESS, parse_helper_.segment(cs_type_, word_len_76, std::strlen(word_len_76), doc_length, words));
  ASSERT_EQ(1, words.count());

  // word len = 84;
  const char *word_len_84 = "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz123456";
  ASSERT_EQ(OB_SUCCESS, parse_helper_.segment(cs_type_, word_len_84, std::strlen(word_len_84), doc_length, words));
  ASSERT_EQ(1, words.count());

  // word len = 85;
  const char *word_len_85 = "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz1234567";
  ASSERT_EQ(OB_SUCCESS, parse_helper_.segment(cs_type_, word_len_85, std::strlen(word_len_85), doc_length, words));
  ASSERT_EQ(0, words.count());
}

class ObTestNgramFTParseHelper : public ::testing::Test
{
public:
  static const char *name_;
  static const int64_t TEST_WORD_COUNT = 29;
  typedef common::hash::ObHashMap<ObFTWord, int64_t> ObFTWordMap;
public:
  ObTestNgramFTParseHelper();
  virtual ~ObTestNgramFTParseHelper() = default;

  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp() override;
  virtual void TearDown() override;

private:
  const common::ObString plugin_name_;
  const char *ngram_words_[TEST_WORD_COUNT];
  const common::ObCollationType cs_type_;
  common::ObArenaAllocator allocator_;
  ObFTParseHelper parse_helper_;
};

const char *ObTestNgramFTParseHelper::name_ = "ngram.1";

ObTestNgramFTParseHelper::ObTestNgramFTParseHelper()
  : plugin_name_(STRLEN(name_), name_),
    ngram_words_{"Oc", "ce", "ea", "an", "nB", "Ba", "as", "se", "fu", "ul", "ll", "lt", "te", "ex", "xt", "se", "ea", "ar", "rc", "ch", "is", "No", "in", "th", "he", "wo", "or", "rl", "ld"},
    cs_type_(ObCollationType::CS_TYPE_UTF8MB4_BIN),
    allocator_()
{
}

void ObTestNgramFTParseHelper::SetUp()
{
  ASSERT_EQ(OB_SUCCESS, parse_helper_.init(&allocator_, plugin_name_));
}

void ObTestNgramFTParseHelper::TearDown()
{
  parse_helper_.reset();
}

void ObTestNgramFTParseHelper::SetUpTestCase()
{
  ASSERT_EQ(common::OB_SUCCESS, ObTenantFTPluginMgr::register_plugins());
  ASSERT_EQ(common::OB_SUCCESS, ft_plugin_mgr.init());
}

void ObTestNgramFTParseHelper::TearDownTestCase()
{
  ft_plugin_mgr.destroy();
  ObTenantFTPluginMgr::unregister_plugins();
}

TEST_F(ObTestNgramFTParseHelper, test_parse_fulltext)
{
  int64_t doc_length = 0;
  common::ObSEArray<ObFTWord, 16> words;
  ASSERT_EQ(OB_SUCCESS, parse_helper_.segment(cs_type_, ObTestAddWord::TEST_FULLTEXT,
        std::strlen(ObTestAddWord::TEST_FULLTEXT), doc_length, words));

  for (int64_t i = 0; i < words.count(); ++i) {
    ASSERT_TRUE(0 == strncmp(ngram_words_[i], words[i].word_.ptr(), words[i].word_.length()));
  }

  ObFTWordMap ft_word_map;
  ASSERT_EQ(OB_SUCCESS, ft_word_map.create(words.count(), "TestParse"));
  ASSERT_EQ(OB_SUCCESS, segment_and_calc_word_count(allocator_, &parse_helper_,
        cs_type_, ObTestAddWord::TEST_FULLTEXT, ft_word_map));
  ASSERT_EQ(words.count(), ft_word_map.size() + 2);

  ASSERT_EQ(OB_INVALID_ARGUMENT, parse_helper_.segment(cs_type_, nullptr, std::strlen(ObTestAddWord::TEST_FULLTEXT), doc_length, words));
  ASSERT_EQ(OB_INVALID_ARGUMENT, parse_helper_.segment(cs_type_, ObTestAddWord::TEST_FULLTEXT, 0, doc_length, words));
  ASSERT_EQ(OB_INVALID_ARGUMENT, parse_helper_.segment(cs_type_, ObTestAddWord::TEST_FULLTEXT, -1, doc_length, words));

  parse_helper_.reset();
  ASSERT_EQ(OB_NOT_INIT, parse_helper_.segment(cs_type_, ObTestAddWord::TEST_FULLTEXT,
        std::strlen(ObTestAddWord::TEST_FULLTEXT), doc_length, words));

  ASSERT_EQ(OB_INVALID_ARGUMENT, parse_helper_.init(nullptr, plugin_name_));
  ASSERT_EQ(OB_INVALID_ARGUMENT, parse_helper_.init(&allocator_, ObString()));

  const char *plugin_name = "space.1";
  ASSERT_EQ(OB_SUCCESS, parse_helper_.init(&allocator_, common::ObString(STRLEN(plugin_name), plugin_name)));

  ASSERT_EQ(OB_INVALID_ARGUMENT, parse_helper_.segment(CS_TYPE_INVALID, ObTestAddWord::TEST_FULLTEXT,
        std::strlen(ObTestAddWord::TEST_FULLTEXT), doc_length, words));
  ASSERT_EQ(OB_INVALID_ARGUMENT, parse_helper_.segment(CS_TYPE_EXTENDED_MARK, ObTestAddWord::TEST_FULLTEXT,
        std::strlen(ObTestAddWord::TEST_FULLTEXT), doc_length, words));

  ASSERT_EQ(OB_INIT_TWICE, parse_helper_.init(&allocator_, plugin_name_));

  parse_helper_.reset();
  ASSERT_EQ(OB_SUCCESS, parse_helper_.init(&allocator_, plugin_name_));

  parse_helper_.reset();
  ASSERT_EQ(OB_SUCCESS, parse_helper_.init(&allocator_, plugin_name_));
  ASSERT_EQ(OB_SUCCESS, parse_helper_.segment(cs_type_, ObTestAddWord::TEST_FULLTEXT,
        std::strlen(ObTestAddWord::TEST_FULLTEXT), doc_length, words));
  for (int64_t i = 0; i < words.count(); ++i) {
    ASSERT_TRUE(0 == strncmp(ngram_words_[i], words[i].word_.ptr(), words[i].word_.length()));
  }
}

} // end namespace storage
} // end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_fts_plugin.log");
  OB_LOGGER.set_file_name("test_fts_plugin.log", true);
  OB_LOGGER.set_log_level("INFO");
  oceanbase::storage::ObTestFTPluginHelper::file_name = argv[0];
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
