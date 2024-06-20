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
  if (OB_ISNULL(helper)
      || OB_UNLIKELY(ObCollationType::CS_TYPE_INVALID == type
                  || ObCollationType::CS_TYPE_EXTENDED_MARK < type)
      || OB_UNLIKELY(!words_count.created())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KPC(helper), K(type), K(words_count.created()));
  } else if (OB_FAIL(helper->segment(type, fulltext.ptr(), fulltext.length(), doc_length, words_count))) {
    LOG_WARN("fail to segment", K(ret), KPC(helper), K(type), K(fulltext));
  }
  return ret;
}

class ObTestAddWord final
{
public:
  static const char *TEST_FULLTEXT;
  static const int64_t TEST_WORD_COUNT = 5;
  static const int64_t TEST_WORD_COUNT_WITHOUT_STOPWORD = 4;
  static const int64_t FT_MIN_WORD_LEN = 3;
  static const int64_t FT_MAX_WORD_LEN = 84;
public:
  ObTestAddWord(const ObCollationType &type, common::ObIAllocator &allocator);
  ~ObTestAddWord() = default;
  int check_words(lib::ObITokenIterator *iter);
  int64_t get_add_word_count() const { return ith_word_; }
  static int64_t get_word_cnt_without_stopword() { return TEST_WORD_COUNT_WITHOUT_STOPWORD; }
  VIRTUAL_TO_STRING_KV(K_(ith_word));
private:
  int check_ith_word(
      const char *word,
      const int64_t word_len,
      const int64_t char_cnt);
private:
  bool is_min_max_word(const int64_t c_len) const;
  int casedown_word(const ObFTWord &src, ObFTWord &dst);
  ObCollationType collation_type_;
  common::ObIAllocator &allocator_;
  const char *words_[TEST_WORD_COUNT];
  const char *words_without_stopword_[TEST_WORD_COUNT_WITHOUT_STOPWORD];
  int64_t ith_word_;
};

const char *ObTestAddWord::TEST_FULLTEXT = "OceanBase fulltext search is No.1 in the world.";

ObTestAddWord::ObTestAddWord(const ObCollationType &type, common::ObIAllocator &allocator)
  : collation_type_(type),
    allocator_(allocator),
    words_{"oceanbase", "fulltext", "search", "the", "world"},
    words_without_stopword_{"oceanbase", "fulltext", "search", "world"},
    ith_word_(0)
{
}

bool ObTestAddWord::is_min_max_word(const int64_t c_len) const
{
  return c_len < FT_MIN_WORD_LEN || c_len > FT_MAX_WORD_LEN;
}

int ObTestAddWord::casedown_word(const ObFTWord &src, ObFTWord &dst)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(src.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid src ft word", K(ret), K(src));
  } else {
    ObString dst_str;
    if (OB_FAIL(ObCharset::tolower(collation_type_, src.get_word(), dst_str, allocator_))) {
      LOG_WARN("fail to tolower", K(ret), K(src), K(collation_type_));
    } else {
      ObFTWord tmp(dst_str.length(), dst_str.ptr(), collation_type_);
      dst = tmp;
    }
  }
  return ret;
}

int ObTestAddWord::check_words(lib::ObITokenIterator *iter)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(iter)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(iter));
  } else {
    const char *word = nullptr;
    int64_t word_len = 0;
    int64_t char_len = 0;
    int64_t word_freq = 0;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(iter->get_next_token(word, word_len, char_len, word_freq))) {
        LOG_WARN("fail to get next token", K(ret), KPC(iter));
      } else if (OB_FAIL(check_ith_word(word, word_len, char_len))) {
        LOG_WARN("fail to check ith word", K(ret), KP(word), K(word_len), K(char_len));
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObTestAddWord::check_ith_word(
      const char *word,
      const int64_t word_len,
      const int64_t char_cnt)
{
  int ret = OB_SUCCESS;
  ObFTWord src_word(word_len, word, collation_type_);
  ObFTWord dst_word;
  if (OB_ISNULL(word) || OB_UNLIKELY(0 >= word_len || 0 >= char_cnt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(word), K(word_len), K(char_cnt));
  } else if (is_min_max_word(char_cnt)) {
    // skip min/max word
  } else if (OB_FAIL(casedown_word(src_word, dst_word))) {
    LOG_WARN("fail to casedown word", K(ret), K(src_word));
  } else if (OB_UNLIKELY(0 != strncmp(words_[ith_word_], dst_word.get_word().ptr(), dst_word.get_word().length()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the ith word isn't default word", K(ret), K(ith_word_), KCSTRING(words_[ith_word_]), K(dst_word));
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
  ObWhiteSpaceFTParserDesc desc_;
  common::ObArenaAllocator allocator_;
  ObTestAddWord add_word_;
};

TestDefaultFTParser::TestDefaultFTParser()
  : plugin_param_(),
    ft_parser_param_(),
    desc_(),
    allocator_(),
    add_word_(ObCollationType::CS_TYPE_UTF8MB4_BIN, allocator_)
{
  plugin_param_.desc_ = &desc_;
}

void TestDefaultFTParser::SetUp()
{
  ASSERT_EQ(OB_SUCCESS, desc_.init(&plugin_param_));

  ft_parser_param_.allocator_ = &allocator_;
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
  ObSpaceFTParser parser;
  const char *fulltext = ObTestAddWord::TEST_FULLTEXT;
  const int64_t ft_len = strlen(fulltext);

  ASSERT_EQ(OB_INVALID_ARGUMENT, parser.init(nullptr));

  ft_parser_param_.fulltext_ = nullptr;
  ft_parser_param_.ft_length_ = 0;
  ASSERT_EQ(OB_INVALID_ARGUMENT, parser.init(&ft_parser_param_));

  ft_parser_param_.fulltext_ = fulltext;
  ASSERT_EQ(OB_INVALID_ARGUMENT, parser.init(&ft_parser_param_));

  ft_parser_param_.ft_length_ = -1;
  ASSERT_EQ(OB_INVALID_ARGUMENT, parser.init(&ft_parser_param_));

  ft_parser_param_.fulltext_ = fulltext;
  ft_parser_param_.ft_length_ = ft_len;

  LOG_INFO("before space segment", KCSTRING(fulltext), K(ft_len), K(ft_parser_param_));
  ASSERT_EQ(OB_SUCCESS, parser.init(&ft_parser_param_));
  ASSERT_EQ(OB_SUCCESS, add_word_.check_words(&parser));
  LOG_INFO("after space segment", KCSTRING(fulltext), K(ft_len), K(ft_parser_param_));
}

TEST_F(TestDefaultFTParser, test_space_ft_parser_segment_bug_56324268)
{
  ObSpaceFTParser parser;
  const char *fulltext = "\201 想 将 数据 添加 到 数据库\f\026 ";
  const int64_t ft_len = strlen(fulltext);

  ft_parser_param_.fulltext_ = fulltext;
  ft_parser_param_.ft_length_ = ft_len;
  ft_parser_param_.cs_ = common::ObCharset::get_charset(ObCollationType::CS_TYPE_LATIN1_SWEDISH_CI);

  LOG_INFO("before space segment", KCSTRING(fulltext), K(ft_len), K(ft_parser_param_));
  ASSERT_EQ(OB_SUCCESS, parser.init(&ft_parser_param_));
  const char *word = nullptr;
  int64_t word_len = 0;
  int64_t char_len = 0;
  int64_t word_freq = 0;
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(parser.get_next_token(word, word_len, char_len, word_freq))) {
      LOG_WARN("fail to get next token", K(ret), K(parser));
    } else {
      LOG_INFO("succeed to get next token", K(ret), K(ObString(word_len, word)), K(char_len));
    }
  }
  LOG_INFO("after space segment", KCSTRING(fulltext), K(ft_len), K(ft_parser_param_));
}

TEST_F(TestDefaultFTParser, test_default_ft_parser_desc)
{
  ObITokenIterator *iter = nullptr;
  ASSERT_EQ(OB_INVALID_ARGUMENT, desc_.segment(&ft_parser_param_, iter));

  ft_parser_param_.fulltext_ = ObTestAddWord::TEST_FULLTEXT;
  ft_parser_param_.ft_length_ = strlen(ft_parser_param_.fulltext_);

  ASSERT_EQ(OB_SUCCESS, desc_.segment(&ft_parser_param_, iter));
  ASSERT_EQ(OB_SUCCESS, add_word_.check_words(iter));

  ASSERT_EQ(OB_SUCCESS, desc_.deinit(&plugin_param_));
  ASSERT_EQ(OB_NOT_INIT, desc_.segment(&ft_parser_param_, iter));

  ASSERT_EQ(OB_SUCCESS, desc_.init(&plugin_param_));
  ASSERT_EQ(OB_INVALID_ARGUMENT, desc_.segment(nullptr, iter));
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
//  ObTestAddWord test_add_word(ObCollationType::CS_TYPE_UTF8MB4_BIN, allocator_);
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
//  ObTestAddWord test_add_word(ObCollationType::CS_TYPE_UTF8MB4_BIN, allocator_);
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
  ObFTWordMap ft_word_map;
  ASSERT_EQ(OB_SUCCESS, ft_word_map.create(10, "TestParse"));
  int64_t doc_length = 0;
  ASSERT_EQ(OB_SUCCESS, parse_helper_.segment(cs_type_, ObTestAddWord::TEST_FULLTEXT,
        std::strlen(ObTestAddWord::TEST_FULLTEXT), doc_length, ft_word_map));

  ObTestAddWord test_add_word(cs_type_, allocator_);
  ASSERT_EQ(ObTestAddWord::get_word_cnt_without_stopword(), ft_word_map.size());
  for (int64_t i = 0; i < ft_word_map.size(); ++i) {
    int64_t word_cnt = 0;
    ObFTWord word(strlen(test_add_word.words_without_stopword_[i]), test_add_word.words_without_stopword_[i], cs_type_);
    ASSERT_EQ(OB_SUCCESS, ft_word_map.get_refactored(word, word_cnt));
    ASSERT_TRUE(word_cnt >= 1);
  }

  ft_word_map.clear();
  ASSERT_EQ(OB_SUCCESS, segment_and_calc_word_count(allocator_, &parse_helper_,
        cs_type_, ObTestAddWord::TEST_FULLTEXT, ft_word_map));
  ASSERT_EQ(ObTestAddWord::get_word_cnt_without_stopword(), ft_word_map.size());

  ft_word_map.clear();
  ASSERT_EQ(OB_INVALID_ARGUMENT, parse_helper_.segment(cs_type_, nullptr, std::strlen(ObTestAddWord::TEST_FULLTEXT), doc_length, ft_word_map));
  ASSERT_EQ(OB_INVALID_ARGUMENT, parse_helper_.segment(cs_type_, ObTestAddWord::TEST_FULLTEXT, 0, doc_length, ft_word_map));
  ASSERT_EQ(OB_INVALID_ARGUMENT, parse_helper_.segment(cs_type_, ObTestAddWord::TEST_FULLTEXT, -1, doc_length, ft_word_map));

  parse_helper_.reset();
  ft_word_map.clear();
  ASSERT_EQ(OB_NOT_INIT, parse_helper_.segment(cs_type_, ObTestAddWord::TEST_FULLTEXT,
        std::strlen(ObTestAddWord::TEST_FULLTEXT), doc_length, ft_word_map));

  ASSERT_EQ(OB_INVALID_ARGUMENT, parse_helper_.init(nullptr, plugin_name_));
  ASSERT_EQ(OB_INVALID_ARGUMENT, parse_helper_.init(&allocator_, ObString()));

  ASSERT_EQ(OB_SUCCESS, parse_helper_.init(&allocator_, plugin_name_));

  ASSERT_EQ(OB_INVALID_ARGUMENT, parse_helper_.segment(CS_TYPE_INVALID, ObTestAddWord::TEST_FULLTEXT,
        std::strlen(ObTestAddWord::TEST_FULLTEXT), doc_length, ft_word_map));
  ASSERT_EQ(OB_INVALID_ARGUMENT, parse_helper_.segment(CS_TYPE_EXTENDED_MARK, ObTestAddWord::TEST_FULLTEXT,
        std::strlen(ObTestAddWord::TEST_FULLTEXT), doc_length, ft_word_map));

  ASSERT_EQ(OB_INIT_TWICE, parse_helper_.init(&allocator_, plugin_name_));

  parse_helper_.reset();
  ASSERT_EQ(OB_SUCCESS, parse_helper_.init(&allocator_, plugin_name_));

  parse_helper_.reset();
  ASSERT_EQ(OB_SUCCESS, parse_helper_.init(&allocator_, plugin_name_));
  ASSERT_EQ(OB_SUCCESS, parse_helper_.segment(cs_type_, ObTestAddWord::TEST_FULLTEXT,
        std::strlen(ObTestAddWord::TEST_FULLTEXT), doc_length, ft_word_map));
  ASSERT_EQ(ObTestAddWord::get_word_cnt_without_stopword(), ft_word_map.size());
  for (int64_t i = 0; i < ft_word_map.size(); ++i) {
    int64_t word_cnt = 0;
    ObFTWord word(strlen(test_add_word.words_without_stopword_[i]), test_add_word.words_without_stopword_[i], cs_type_);
    ASSERT_EQ(OB_SUCCESS, ft_word_map.get_refactored(word, word_cnt));
    ASSERT_TRUE(word_cnt >= 1);
  }
  parse_helper_.reset();
  ft_word_map.clear();
  ASSERT_EQ(OB_SUCCESS, parse_helper_.init(&allocator_, "beng.1"));
  ASSERT_EQ(OB_SUCCESS, parse_helper_.segment(cs_type_, ObTestAddWord::TEST_FULLTEXT,
        std::strlen(ObTestAddWord::TEST_FULLTEXT), doc_length, ft_word_map));
  ASSERT_EQ(ObTestAddWord::get_word_cnt_without_stopword(), ft_word_map.size());
  for (int64_t i = 0; i < ft_word_map.size(); ++i) {
    int64_t word_cnt = 0;
    ObFTWord word(strlen(test_add_word.words_without_stopword_[i]), test_add_word.words_without_stopword_[i], cs_type_);
    ASSERT_EQ(OB_SUCCESS, ft_word_map.get_refactored(word, word_cnt));
    ASSERT_TRUE(word_cnt >= 1);
  }
}

TEST_F(ObTestFTParseHelper, test_min_and_max_word_len)
{
  ObFTWordMap words;
  ASSERT_EQ(OB_SUCCESS, words.create(10, "TestParse"));
  int64_t doc_length = 0;

  // word len = 2;
  const char *word_len_2 = "ab";
  ASSERT_EQ(OB_SUCCESS, parse_helper_.segment(cs_type_, word_len_2, std::strlen(word_len_2), doc_length, words));
  ASSERT_EQ(0, words.size());

  // word len = 3;
  const char *word_len_3 = "abc";
  words.clear();
  ASSERT_EQ(OB_SUCCESS, parse_helper_.segment(cs_type_, word_len_3, std::strlen(word_len_3), doc_length, words));
  ASSERT_EQ(1, words.size());

  // word len = 4;
  const char *word_len_4 = "abcd";
  words.clear();
  ASSERT_EQ(OB_SUCCESS, parse_helper_.segment(cs_type_, word_len_4, std::strlen(word_len_4), doc_length, words));
  ASSERT_EQ(1, words.size());

  // word len = 76;
  const char *word_len_76 = "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz";
  words.clear();
  ASSERT_EQ(OB_SUCCESS, parse_helper_.segment(cs_type_, word_len_76, std::strlen(word_len_76), doc_length, words));
  ASSERT_EQ(1, words.size());

  // word len = 84;
  const char *word_len_84 = "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz123456";
  words.clear();
  ASSERT_EQ(OB_SUCCESS, parse_helper_.segment(cs_type_, word_len_84, std::strlen(word_len_84), doc_length, words));
  ASSERT_EQ(1, words.size());

  // word len = 85;
  const char *word_len_85 = "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz1234567";
  words.clear();
  ASSERT_EQ(OB_SUCCESS, parse_helper_.segment(cs_type_, word_len_85, std::strlen(word_len_85), doc_length, words));
  ASSERT_EQ(0, words.size());
}

class ObTestNgramFTParseHelper : public ::testing::Test
{
public:
  static const char *name_;
  static const int64_t TEST_WORD_COUNT = 27;
  typedef common::hash::ObHashMap<ObFTWord, int64_t> ObFTWordMap;
public:
  ObTestNgramFTParseHelper();
  virtual ~ObTestNgramFTParseHelper() = default;
  static int64_t get_word_count() { return TEST_WORD_COUNT; }

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
    ngram_words_{"oc", "ce", "ea", "an", "nb", "ba", "as", "se", "fu", "ul", "ll", "lt", "te", "ex", "xt", "ar", "rc", "ch", "is", "no", "in", "th", "he", "wo", "or", "rl", "ld"},
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
  ObFTWordMap words;
  ASSERT_EQ(OB_SUCCESS, words.create(10, "TestParse"));
  int64_t doc_length = 0;
  ASSERT_EQ(OB_SUCCESS, parse_helper_.segment(cs_type_, ObTestAddWord::TEST_FULLTEXT,
        std::strlen(ObTestAddWord::TEST_FULLTEXT), doc_length, words));

  ASSERT_EQ(get_word_count(), words.size());
  for (int64_t i = 0; i < words.size(); ++i) {
    int64_t word_cnt = 0;
    ObFTWord word(strlen(ngram_words_[i]), ngram_words_[i], cs_type_);
    ASSERT_EQ(OB_SUCCESS, words.get_refactored(word, word_cnt));
    ASSERT_TRUE(word_cnt >= 1);
  }

  ObFTWordMap ft_word_map;
  ASSERT_EQ(OB_SUCCESS, ft_word_map.create(10, "TestParse"));
  ASSERT_EQ(OB_SUCCESS, segment_and_calc_word_count(allocator_, &parse_helper_,
        cs_type_, ObTestAddWord::TEST_FULLTEXT, ft_word_map));
  ASSERT_EQ(words.size(), ft_word_map.size());

  words.clear();
  ASSERT_EQ(OB_INVALID_ARGUMENT, parse_helper_.segment(cs_type_, nullptr, std::strlen(ObTestAddWord::TEST_FULLTEXT), doc_length, words));
  ASSERT_EQ(OB_INVALID_ARGUMENT, parse_helper_.segment(cs_type_, ObTestAddWord::TEST_FULLTEXT, 0, doc_length, words));
  ASSERT_EQ(OB_INVALID_ARGUMENT, parse_helper_.segment(cs_type_, ObTestAddWord::TEST_FULLTEXT, -1, doc_length, words));

  parse_helper_.reset();
  words.clear();
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
  words.clear();
  ASSERT_EQ(OB_SUCCESS, parse_helper_.init(&allocator_, plugin_name_));

  parse_helper_.reset();
  ASSERT_EQ(OB_SUCCESS, parse_helper_.init(&allocator_, plugin_name_));
  ASSERT_EQ(OB_SUCCESS, parse_helper_.segment(cs_type_, ObTestAddWord::TEST_FULLTEXT,
        std::strlen(ObTestAddWord::TEST_FULLTEXT), doc_length, words));
  ASSERT_EQ(get_word_count(), words.size());
  for (int64_t i = 0; i < words.size(); ++i) {
    int64_t word_cnt = 0;
    ObFTWord word(strlen(ngram_words_[i]), ngram_words_[i], cs_type_);
    ASSERT_EQ(OB_SUCCESS, words.get_refactored(word, word_cnt));
    ASSERT_TRUE(word_cnt >= 1);
  }
}

} // end namespace storage
} // end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_fts_plugin.log");
  OB_LOGGER.set_file_name("test_fts_plugin.log", true);
  OB_LOGGER.set_log_level("DEBUG");
  oceanbase::storage::ObTestFTPluginHelper::file_name = argv[0];
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
