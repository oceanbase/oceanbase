/**
 * Copyright (c) 2024 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "lib/json_type/ob_json_base.h"
#include "lib/json_type/ob_json_parse.h"
#include "lib/json_type/ob_json_tree.h"
#include "lib/ob_errno.h"
#include "lib/string/ob_string.h"
#include "storage/fts/ob_fts_parser_property.h"
#include "storage/fts/ob_fts_plugin_helper.h"

#include <cstdint>
#include <gtest/gtest.h>

#define USING_LOG_PREFIX STORAGE_FTS

#define protected public
#define private public

namespace oceanbase
{
namespace storage
{
class TestFTParserJsonProperty : public ::testing::Test
{
public:
  TestFTParserJsonProperty() = default;
  ~TestFTParserJsonProperty() = default;
};

class TestFTParserProperty : public ::testing::Test
{
public:
  TestFTParserProperty();
  ~TestFTParserProperty() = default;

  static void SetUpTestCase();
  static void TearDownTestCase();

  virtual void SetUp() override;
  virtual void TearDown() override;

private:
  static const int64_t FT_MIN_TOKEN_SIZE;
  static const int64_t FT_MAX_TOKEN_SIZE;
  static const int64_t FT_NGRAM_TOKEN_SIZE;
  static const char *SPACE_PARSER_NAME;
  static const char *NGRAM_PARSER_NAME;
  static const char *BENG_PARSER_NAME;
  static const char *IK_PARSER_NAME;
  static const char *SPACE_PARSER_STR;
  static const char *NGRAM_PARSER_STR;
  static const char *BENG_PARSER_STR;
  static const char *IK_PARSER_STR;
  static const char *NON_BUILTIN_PARSER_STR;
  static ObFTParser space_parser_;
  static ObFTParser ngram_parser_;
  static ObFTParser beng_parser_;
  static ObFTParser ik_parser_;
};

TestFTParserProperty::TestFTParserProperty()
{
}

const int64_t TestFTParserProperty::FT_MIN_TOKEN_SIZE = 3;
const int64_t TestFTParserProperty::FT_MAX_TOKEN_SIZE = 84;
const int64_t TestFTParserProperty::FT_NGRAM_TOKEN_SIZE = 2;

const char *TestFTParserProperty::SPACE_PARSER_NAME = "space";
const char *TestFTParserProperty::NGRAM_PARSER_NAME = "ngram";
const char *TestFTParserProperty::BENG_PARSER_NAME = "beng";
const char *TestFTParserProperty::IK_PARSER_NAME = "ik";

const char *TestFTParserProperty::SPACE_PARSER_STR = "space.1";
const char *TestFTParserProperty::NGRAM_PARSER_STR = "ngram.1";
const char *TestFTParserProperty::BENG_PARSER_STR = "beng.1";
const char *TestFTParserProperty::IK_PARSER_STR = "ik.1";
const char *TestFTParserProperty::NON_BUILTIN_PARSER_STR = "jieba.1";

ObFTParser TestFTParserProperty::space_parser_;
ObFTParser TestFTParserProperty::ngram_parser_;
ObFTParser TestFTParserProperty::beng_parser_;
ObFTParser TestFTParserProperty::ik_parser_;

void TestFTParserProperty::SetUpTestCase()
{
  space_parser_.set_name_and_version(share::ObPluginName(SPACE_PARSER_NAME), 1);
  ngram_parser_.set_name_and_version(share::ObPluginName(NGRAM_PARSER_NAME), 1);
  beng_parser_.set_name_and_version(share::ObPluginName(BENG_PARSER_NAME), 1);
  ik_parser_.set_name_and_version(share::ObPluginName(IK_PARSER_NAME), 1);
}

void TestFTParserProperty::TearDownTestCase() {}

void TestFTParserProperty::SetUp() {}

void TestFTParserProperty::TearDown() {}

TEST_F(TestFTParserJsonProperty, happy_path_test)
{
  int ret = OB_SUCCESS;
  common::ObString result;
  ObFTParserJsonProps json_props;

  static const uint64_t K_TEST_MIN_TOKEN_SIZE = 2;
  static const uint64_t K_TEST_MAX_TOKEN_SIZE = 81;
  static const uint64_t K_TEST_NGRAM_TOKEN_SIZE = 5;
  static const ObString K_TEST_DICT_TABLE = "a_dict_table_name";
  static const ObString K_TEST_STOPWORD_TABLE = "a_stopword_table_name";
  static const ObString K_TEST_QUANTIFIER_TABLE = "a_quantifier_table_name";

  ret = json_props.init();
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = json_props.config_set_min_token_size(K_TEST_MIN_TOKEN_SIZE);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = json_props.config_set_max_token_size(K_TEST_MAX_TOKEN_SIZE);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = json_props.config_set_ngram_token_size(K_TEST_NGRAM_TOKEN_SIZE);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = json_props.config_set_dict_table(K_TEST_DICT_TABLE);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = json_props.config_set_stopword_table(K_TEST_STOPWORD_TABLE);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = json_props.config_set_quantifier_table(K_TEST_QUANTIFIER_TABLE);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObArenaAllocator allocator;
  ObString str;
  ret = json_props.to_format_json(allocator, str);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObFTParserJsonProps json_props2;
  ret = json_props2.init();
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = json_props2.parse_from_valid_str(str);
  ASSERT_EQ(OB_SUCCESS, ret);

  // min
  int64_t min_token_size = 0;
  ret = json_props.config_get_min_token_size(min_token_size);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(min_token_size, K_TEST_MIN_TOKEN_SIZE);

  // max
  int64_t max_token_size = 0;
  ret = json_props.config_get_max_token_size(max_token_size);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(max_token_size, K_TEST_MAX_TOKEN_SIZE);

  // ngram
  int64_t ngram_token_size = 0;
  ret = json_props.config_get_ngram_token_size(ngram_token_size);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(ngram_token_size, K_TEST_NGRAM_TOKEN_SIZE);

  // dict
  ObString dict_table;
  ret = json_props.config_get_dict_table(dict_table);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(dict_table, K_TEST_DICT_TABLE);

  // stopword
  ObString stopword_table;
  ret = json_props.config_get_stopword_table(stopword_table);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(stopword_table, K_TEST_STOPWORD_TABLE);

  // quantifier
  ObString quantifier_table;
  ret = json_props.config_get_quantifier_table(quantifier_table);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(quantifier_table, K_TEST_QUANTIFIER_TABLE);
}

TEST_F(TestFTParserJsonProperty, test_parse_from_string)
{
  int ret = OB_SUCCESS;

  common::ObString result;

  // okay to parse, but not valid for parser.
  const ObString prebuild_result
      = R"({"min_token_size": 3, "max_token_size": 84, "stopword_table": "a_stopword_table_name", "ngram_token_size": 2, "quanitfier_table": "a_quantifier_table_name"})";

  ObFTParserJsonProps json_props;
  ret = json_props.init();
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = json_props.parse_from_valid_str(prebuild_result);
  ASSERT_EQ(ret, OB_SUCCESS);

  int64_t min_token_size = 0;
  int64_t max_token_size = 0;
  int64_t ngram_token_size = 0;

  ret = json_props.config_get_min_token_size(min_token_size);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(min_token_size, 3);

  ret = json_props.config_get_max_token_size(max_token_size);
  ASSERT_EQ(max_token_size, 84);

  ret = json_props.config_get_ngram_token_size(ngram_token_size);
  ASSERT_EQ(ngram_token_size, 2);

  ObString dict_table;
  ret = json_props.config_get_dict_table(dict_table);
  ASSERT_EQ(ret, OB_SEARCH_NOT_FOUND);

  ObString stopword_table;
  ret = json_props.config_get_stopword_table(stopword_table);
  ASSERT_EQ(stopword_table, ObString("a_stopword_table_name"));

  ObString quanitfier_table;
  ret = json_props.config_get_quantifier_table(quanitfier_table);
  ASSERT_EQ(quanitfier_table, ObString("a_quantifier_table_name"));

  ObString parse_string;
  ObArenaAllocator allocator;
  ret = json_props.to_format_json(allocator, parse_string);
  ASSERT_EQ(ret, OB_SUCCESS);
}

TEST_F(TestFTParserJsonProperty, test_parse_from_tokenize_array)
{
  const ObString prebuild_result
      = R"({"additional_args": [{"min_token_size": 5}, {"max_token_size": 11}]})";
  ObArenaAllocator allocator;

  ObJsonNode *root = nullptr;
  int ret = ObJsonParser::get_tree(&allocator, prebuild_result, root);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObIJsonBase *array_value;

  ret = root->get_object_value(0, array_value);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(array_value->json_type(), ObJsonNodeType::J_ARRAY);

  {
    ObArenaAllocator alloc;
    ObString str;
    ret = ObFTParserJsonProps::tokenize_array_to_props_json(alloc, array_value, str);
    ASSERT_EQ(OB_SUCCESS, ret);

    ObFTParserJsonProps json_props;
    ret = json_props.init();
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = json_props.parse_from_valid_str(str);
    ASSERT_EQ(OB_SUCCESS, ret);

    int64_t min_token_size = 0;
    ret = json_props.config_get_min_token_size(min_token_size);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(min_token_size, 5);

    int64_t max_token_size = 0;
    ret = json_props.config_get_max_token_size(max_token_size);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(max_token_size, 11);
  }
}

TEST_F(TestFTParserProperty, test_parse_for_ddl)
{
  // space happy path
  {
    int ret = OB_SUCCESS;
    common::ObString result;
    ObFTParserJsonProps json_props;

    ret = json_props.init();
    ASSERT_EQ(OB_SUCCESS, ret);

    // test space parser
    const common::ObString space_result = R"({"min_token_size": 3, "max_token_size": 14})";
    ret = json_props.parse_from_valid_str(space_result);
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = json_props.rebuild_props_for_ddl(SPACE_PARSER_STR,
                                           ObCollationType::CS_TYPE_UTF8MB4_BIN,
                                           false);
    ASSERT_EQ(OB_SUCCESS, ret);
    ObString str;
    ret = json_props.config_get_dict_table(str);
    ASSERT_EQ(OB_SEARCH_NOT_FOUND, ret);

    ret = json_props.config_get_stopword_table(str);
    ASSERT_EQ(OB_SEARCH_NOT_FOUND, ret);

    ret = json_props.config_get_quantifier_table(str);
    ASSERT_EQ(OB_SEARCH_NOT_FOUND, ret);

    int64_t min_token_size = 0;
    ret = json_props.config_get_min_token_size(min_token_size);
    ASSERT_EQ(OB_SUCCESS, ret);

    int64_t max_token_size = 0;
    ret = json_props.config_get_max_token_size(max_token_size);

    ASSERT_EQ(3, min_token_size);
    ASSERT_EQ(14, max_token_size);

    int64_t ngram_token_size = 0;
    ret = json_props.config_get_ngram_token_size(ngram_token_size);
    ASSERT_EQ(OB_SEARCH_NOT_FOUND, ret);

    ObArenaAllocator tmp_alloc;
    ObString new_json;
    ret = json_props.to_format_json(tmp_alloc, new_json);

    char output[] = R"(PARSER_PROPERTIES=(min_token_size=3,max_token_size=14) )";
    char output_buf[128];
    int64_t pos = 0;
    ret = json_props.show_parser_properties(json_props, output_buf, 128, pos);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObString(output), ObString(output_buf));
  }

  // test space not supported property
  {
    int ret = OB_SUCCESS;
    common::ObString result;
    ObFTParserJsonProps json_props;

    ret = json_props.init();
    ASSERT_EQ(OB_SUCCESS, ret);

    // test space parser
    const common::ObString space_result = R"({"ngram_token_size": 3,"max_token_size": 14})";

    ret = json_props.parse_from_valid_str(space_result);
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = json_props.rebuild_props_for_ddl(SPACE_PARSER_STR,
                                           ObCollationType::CS_TYPE_UTF8MB4_BIN,
                                           false);
    ASSERT_EQ(OB_NOT_SUPPORTED, ret);
  }

  // invalid json
  {
    int ret = OB_SUCCESS;
    common::ObString result;
    ObFTParserJsonProps json_props;

    ret = json_props.init();
    ASSERT_EQ(OB_SUCCESS, ret);

    // test space parser
    const common::ObString space_result = R"({ngram_token_size":3,"max_token_size":14)";

    ret = json_props.parse_from_valid_str(space_result);
    ASSERT_EQ(OB_ERR_INVALID_JSON_TEXT, ret);
  }

  // invalid config name
  {
    int ret = OB_SUCCESS;
    common::ObString result;
    ObFTParserJsonProps json_props;

    ret = json_props.init();
    ASSERT_EQ(OB_SUCCESS, ret);

    // test space parser
    const common::ObString space_result = R"({"not_exist_name": 3,"max_token_size": 14})";

    ret = json_props.parse_from_valid_str(space_result);
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = json_props.rebuild_props_for_ddl(SPACE_PARSER_STR,
                                           ObCollationType::CS_TYPE_UTF8MB4_BIN,
                                           false);
    ASSERT_EQ(OB_NOT_SUPPORTED, ret);
  }

  // invalid config value
  {
    int ret = OB_SUCCESS;
    common::ObString result;
    ObFTParserJsonProps json_props;

    ret = json_props.init();
    ASSERT_EQ(OB_SUCCESS, ret);

    // test space parser
    const common::ObString space_result = R"({"max_token_size": 9})";

    ret = json_props.parse_from_valid_str(space_result);
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = json_props.rebuild_props_for_ddl(SPACE_PARSER_STR,
                                           ObCollationType::CS_TYPE_UTF8MB4_BIN,
                                           false);
    ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  }

  // test ngram parser
  {
    int ret = OB_SUCCESS;
    common::ObString result;
    ObFTParserJsonProps json_props;

    ret = json_props.init();
    ASSERT_EQ(OB_SUCCESS, ret);

    const common::ObString ngram_str = R"({"ngram_token_size": 4})";
    ret = json_props.parse_from_valid_str(ngram_str);
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = json_props.rebuild_props_for_ddl(NGRAM_PARSER_STR,
                                           ObCollationType::CS_TYPE_UTF8MB4_BIN,
                                           false);
    ASSERT_EQ(OB_SUCCESS, ret);
    ObString str;
    ret = json_props.config_get_dict_table(str);
    ASSERT_EQ(OB_SEARCH_NOT_FOUND, ret);

    ret = json_props.config_get_stopword_table(str);
    ASSERT_EQ(OB_SEARCH_NOT_FOUND, ret);

    ret = json_props.config_get_quantifier_table(str);
    ASSERT_EQ(OB_SEARCH_NOT_FOUND, ret);

    int64_t min_token_size = 0;
    ret = json_props.config_get_min_token_size(min_token_size);
    ASSERT_EQ(OB_SEARCH_NOT_FOUND, ret);

    int64_t max_token_size = 0;
    ret = json_props.config_get_max_token_size(max_token_size);
    ASSERT_EQ(OB_SEARCH_NOT_FOUND, ret);

    int64_t ngram_token_size = 0;
    ret = json_props.config_get_ngram_token_size(ngram_token_size);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(4, ngram_token_size);

    ObArenaAllocator tmp_alloc;
    ObString new_json;
    ret = json_props.to_format_json(tmp_alloc, new_json);

    char output[] = R"(PARSER_PROPERTIES=(ngram_token_size=4) )";
    char output_buf[128];
    int64_t pos = 0;
    ret = json_props.show_parser_properties(json_props, output_buf, 128, pos);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObString(output), ObString(output_buf));
  }

  // beng path
  {
    int ret = OB_SUCCESS;
    common::ObString result;
    ObFTParserJsonProps json_props;

    ret = json_props.init();
    ASSERT_EQ(OB_SUCCESS, ret);

    // test space parser
    const common::ObString space_result = R"({"min_token_size": 3, "max_token_size": 14})";
    ret = json_props.parse_from_valid_str(space_result);
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = json_props.rebuild_props_for_ddl(BENG_PARSER_STR,
                                           ObCollationType::CS_TYPE_UTF8MB4_BIN,
                                           false);
    ASSERT_EQ(OB_SUCCESS, ret);
    ObString str;
    ret = json_props.config_get_dict_table(str);
    ASSERT_EQ(OB_SEARCH_NOT_FOUND, ret);

    ret = json_props.config_get_stopword_table(str);
    ASSERT_EQ(OB_SEARCH_NOT_FOUND, ret);

    ret = json_props.config_get_quantifier_table(str);
    ASSERT_EQ(OB_SEARCH_NOT_FOUND, ret);

    int64_t min_token_size = 0;
    ret = json_props.config_get_min_token_size(min_token_size);
    ASSERT_EQ(OB_SUCCESS, ret);

    int64_t max_token_size = 0;
    ret = json_props.config_get_max_token_size(max_token_size);

    ASSERT_EQ(3, min_token_size);
    ASSERT_EQ(14, max_token_size);

    int64_t ngram_token_size = 0;
    ret = json_props.config_get_ngram_token_size(ngram_token_size);
    ASSERT_EQ(OB_SEARCH_NOT_FOUND, ret);

    ObArenaAllocator tmp_alloc;
    ObString new_json;
    ret = json_props.to_format_json(tmp_alloc, new_json);

    char output[] = R"(PARSER_PROPERTIES=(min_token_size=3,max_token_size=14) )";
    char output_buf[128];
    int64_t pos = 0;
    ret = json_props.show_parser_properties(json_props, output_buf, 128, pos);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObString(output), ObString(output_buf));
  }

  // ik path
  {
    int ret = OB_SUCCESS;
    common::ObString result;
    ObFTParserJsonProps json_props;

    ret = json_props.init();
    ASSERT_EQ(OB_SUCCESS, ret);

    const common::ObString ik_str
        = R"({"stopword_table": "a_stopword_table_name", "quanitfier_table": "a_quantifier_table_name"})";
    ret = json_props.parse_from_valid_str(ik_str);
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = json_props.rebuild_props_for_ddl(IK_PARSER_STR,
                                           ObCollationType::CS_TYPE_UTF8MB4_BIN,
                                           false);
    ASSERT_EQ(OB_SUCCESS, ret);
    ObString str;
    ret = json_props.config_get_dict_table(str);
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = json_props.config_get_stopword_table(str);
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = json_props.config_get_quantifier_table(str);
    ASSERT_EQ(OB_SUCCESS, ret);

    int64_t min_token_size = 0;
    ret = json_props.config_get_min_token_size(min_token_size);
    ASSERT_EQ(OB_SEARCH_NOT_FOUND, ret);

    int64_t max_token_size = 0;
    ret = json_props.config_get_max_token_size(max_token_size);
    ASSERT_EQ(OB_SEARCH_NOT_FOUND, ret);

    int64_t ngram_token_size = 0;
    ret = json_props.config_get_ngram_token_size(ngram_token_size);
    ASSERT_EQ(OB_SEARCH_NOT_FOUND, ret);

    ObArenaAllocator tmp_alloc;
    ObString new_json;
    ret = json_props.to_format_json(tmp_alloc, new_json);

    char output[] = R"(PARSER_PROPERTIES=(ik_mode="smart") )";
    char output_buf[128];
    int64_t pos = 0;
    ret = json_props.show_parser_properties(json_props, output_buf, 128, pos);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(ObString(output), ObString(output_buf));
  }
  {
    int ret = OB_SUCCESS;
    common::ObString result;
    ObFTParserJsonProps json_props;

    ret = json_props.init();
    ASSERT_EQ(OB_SUCCESS, ret);

    const common::ObString ik_str = ObString(); // empty
    ret = json_props.parse_from_valid_str(ik_str);
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = json_props.rebuild_props_for_ddl(NON_BUILTIN_PARSER_STR,
                                           ObCollationType::CS_TYPE_UTF8MB4_BIN,
                                           false);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
}

} // namespace storage
} // end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_fts_property.log");
  OB_LOGGER.set_file_name("test_fts_property.log", true);
  OB_LOGGER.set_log_level("DEBUG");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
