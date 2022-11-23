/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "sql/parser/ob_parser.h"
#include <gtest/gtest.h>
#include "lib/utility/ob_test_util.h"
#include "../test_sql_utils.h"
#include "lib/allocator/page_arena.h"
#include "lib/json/ob_json_print_utils.h"  // for SJ
#include "sql/plan_cache/ob_sql_parameterization.h"
#include <fstream>
#include <iterator>
using namespace oceanbase::common;
using namespace oceanbase::sql;
namespace test
{
class TestParser: public TestSqlUtils, public ::testing::Test
{
public:
  TestParser();
  virtual ~TestParser();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestParser);
protected:
  // function members
  void do_parse(const char *query_str, std::ofstream &of_result, int64_t expect_error = OB_SUCCESS);
  void print_parse_tree(const char *query_str, std::ofstream &of_result, int64_t expect_error = OB_SUCCESS);
  void print_parse_outline(const char *query_str, std::ofstream &of_result, int64_t expect_error = OB_SUCCESS);
  void do_filter_hint(const char *query_str, std::ofstream &of_result, int64_t expect_error = OB_SUCCESS);
  bool pretreat_cmd(std::string line, int64_t &expect_error);
protected:
  // data members
  ObArenaAllocator allocator_;
};

TestParser::TestParser() : allocator_(ObModIds::TEST)
{
}

TestParser::~TestParser()
{
}

void TestParser::SetUp()
{
  init();
}

void TestParser::TearDown()
{
}

TEST_F(TestParser, basic_test)
{
  const char* test_file = "./test_parser.test";
  const char* result_file = "./test_parser.result";
  const char* tmp_file = "./test_parser.tmp";
  // run tests
  std::ifstream if_tests(test_file);
  ASSERT_TRUE(if_tests.is_open());
  std::ofstream of_result(tmp_file);
  ASSERT_TRUE(of_result.is_open());
  std::string line;
  int64_t case_id = 0;
  int64_t expect_error = 0;
  while (std::getline(if_tests, line)) {
    if (pretreat_cmd(line, expect_error)) {
      continue;
    }
    of_result << "**************   Case "<< ++case_id << "   ***************" << std::endl;
    of_result << line << std::endl;
    ASSERT_NO_FATAL_FAILURE(do_parse(line.c_str(), of_result, expect_error));
    if (expect_error != 0) {
      expect_error = 0;
    }
  }
  of_result.close();
  // verify results
  is_equal_content(tmp_file,result_file);
}

TEST_F(TestParser, filter_hint)
{
  const char* test_file = "./test_parser_outline.test";
  const char* result_file = "./test_filter_hint.result";
  const char* tmp_file = "./test_filter_hint.tmp";
  // run tests
  std::ifstream if_tests(test_file);
  ASSERT_TRUE(if_tests.is_open());
  std::ofstream of_result(tmp_file);
  ASSERT_TRUE(of_result.is_open());
  std::string line;
  int64_t case_id = 0;
  int64_t expect_error = 0;
  while (std::getline(if_tests, line)) {
    if (pretreat_cmd(line, expect_error)) {
      continue;
    }
    of_result << "**************   Case "<< ++case_id << "   ***************" << std::endl;
    of_result << line << std::endl;
    do_filter_hint(line.c_str(), of_result, expect_error);
    if (expect_error != 0) {
      expect_error = 0;
    }
  }
  of_result.close();
  // verify results
  is_equal_content(tmp_file,result_file);
}

TEST_F(TestParser, print_parser_tree)
{
  const char* test_file = "./test_parser.test";
  const char* result_file = "./print_parser_tree.result";
  const char* tmp_file = "./print_parser_tree.tmp";
  // run tests
  std::ifstream if_tests(test_file);
  ASSERT_TRUE(if_tests.is_open());
  std::ofstream of_result(tmp_file);
  ASSERT_TRUE(of_result.is_open());
  std::string line;
  int64_t case_id = 0;
  int64_t expect_error = 0;
  int64_t line_number = 0;
  while (std::getline(if_tests, line)) {
    ++line_number;
    if (pretreat_cmd(line, expect_error)) {
      continue;
    }
    of_result << "**************   Case "<< ++case_id << "   ***************" << std::endl;
    of_result << line << std::endl;
    _OB_LOG(INFO, "line content:%ld, %s", line_number, line.c_str());
    ASSERT_NO_FATAL_FAILURE(print_parse_tree(line.c_str(), of_result, expect_error));
    if (expect_error != 0) {
      expect_error = 0;
    }
  }
  of_result.close();
  // verify results
  is_equal_content(tmp_file,result_file);
}
TEST_F(TestParser, pre_parse)
{
  constexpr int STR_NUM = 4;
  std::string str[] = {
    "\t/*\ttrace_id=ABC\t*/rpc_id=xxx",
    "\n/*\ntrace_id=ABC\nrpc_id=xxx*/",
    "\f/*\ftrace_id=ABC\frpc_id=xxx*/",
    "\r/*\rtrace_id=ABC\rrpc_id=xxx*/"
  };
  const char* test_file = "./test_pre_parse.test";
  const char* result_file = "./test_pre_parse.result";
  const char* tmp_file = "./test_pre_parse.tmp";
  // run tests
  std::ifstream if_tests(test_file);
  ASSERT_TRUE(if_tests.is_open());
  std::ofstream of_result(tmp_file);
  ASSERT_TRUE(of_result.is_open());
  std::string line;
  int64_t case_id = 0;
  char *w;
  char *p;
  for (int i = 0; i < STR_NUM; i++) {
    of_result << "**************   Case "<< ++case_id << "   ***************" << std::endl;
    of_result << str[i] << std::endl;
    PreParseResult res;
    ASSERT_EQ(OB_SUCCESS, ObParser::pre_parse(ObString(str[i].length(), str[i].c_str()), res));
    of_result << "trace_id: " << std::string(res.trace_id_.ptr(), res.trace_id_.length()) << std::endl;
  }
  while (std::getline(if_tests, line)) {
    if (line.size() <= 0) continue;
    if (line.at(0) == '#') continue;
    if (strncmp(line.c_str(), "--error", strlen("--error")) == 0) {
      p = const_cast<char*>(line.c_str());
      w = strsep(&p, " ");
      continue;
    }
    of_result << "**************   Case "<< ++case_id << "   ***************" << std::endl;
    of_result << line << std::endl;
    PreParseResult res;
    ASSERT_EQ(OB_SUCCESS, ObParser::pre_parse(ObString(line.length(), line.c_str()), res));
    of_result << "trace_id: " << std::string(res.trace_id_.ptr(), res.trace_id_.length()) << std::endl;
  }
  UNUSED(w);
  of_result.close();
   //verify results
  is_equal_content(tmp_file,result_file);
}

void TestParser::print_parse_tree(const char *query_str, std::ofstream &of_result, int64_t expect_error) {
  ObSQLMode mode = test::clp.sql_mode;
  ObParser parser(allocator_, mode);
  ParseResult parse_result;
  ObString query = ObString::make_string(query_str);
  int ret = OB_SUCCESS;
  ret = parser.parse(query, parse_result);
  if(expect_error != -ret) {
    fprintf(stderr, "QUERY:%s failed\n", query_str);
  }
  ASSERT_EQ(expect_error, -ret);
  if (NULL != parse_result.result_tree_) {
    _OB_LOG(INFO, "%s", CSJ(ObParserResultTreePrintWrapper(*parse_result.result_tree_)));
    of_result << "question_mask_size: " << parse_result.result_tree_->children_[0]->value_ << std::endl;
    of_result << CSJ(ObParserResultTreePrintWrapper(*parse_result.result_tree_)) << std::endl;
  }
  parser.free_result(parse_result);
}

void TestParser::print_parse_outline(const char *query_str, std::ofstream &of_result, int64_t expect_error) {
  ObSQLMode mode = test::clp.sql_mode;
  ObParser parser(allocator_, mode);
  ParseResult parse_result;
  ObString query = ObString::make_string(query_str);
  int ret = OB_SUCCESS;
  ret = parser.parse(query, parse_result, FP_PARAMERIZE_AND_FILTER_HINT_MODE);
  if(expect_error != -ret) {
    fprintf(stderr, "QUERY:%s failed\n", query_str);
  }
  ASSERT_EQ(expect_error, -ret);
  of_result << parse_result.no_param_sql_<<std::endl<<std::endl;
  parser.free_result(parse_result);
}

TEST_F(TestParser, test_parser_outline)
{
  const char* test_file = "./test_parser_outline.test";
  const char* result_file = "./test_parser_outline.result";
  const char* tmp_file = "./test_parser_outline.tmp";
  // run tests
  std::ifstream if_tests(test_file);
  ASSERT_TRUE(if_tests.is_open());
  std::ofstream of_result(tmp_file);
  ASSERT_TRUE(of_result.is_open());
  std::string line;
  int64_t case_id = 0;
  int64_t expect_error = 0;
  while (std::getline(if_tests, line)) {
    if (pretreat_cmd(line, expect_error)) {
      continue;
    }
    of_result << "**************   Case "<< ++case_id << "   ***************" << std::endl;
    of_result << line << std::endl;
    ASSERT_NO_FATAL_FAILURE(print_parse_outline(line.c_str(), of_result, expect_error));
    if (expect_error != 0) {
      expect_error = 0;
    }
  }
  of_result.close();
  // verify results
  is_equal_content(tmp_file,result_file);
}

void TestParser::do_parse(const char *query_str, std::ofstream &of_result, int64_t expect_error) {
  ObSQLMode mode = test::clp.sql_mode;
  ObParser parser(allocator_, mode);
  ParseResult parse_result;
  ObString query = ObString::make_string(query_str);
  int ret = OB_SUCCESS;
  _OB_LOG(INFO, "QUERY: %s", query_str);
  ret = parser.parse(query, parse_result);
  if(expect_error != -ret) {
    fprintf(stderr, "QUERY:%s failed\n", query_str);
  }
  ASSERT_EQ(expect_error, -ret);
  if (NULL != parse_result.result_tree_){
    _OB_LOG(INFO, "%s", CSJ(ObParserResultPrintWrapper(*parse_result.result_tree_)));
    of_result << "question_mask_size: " << parse_result.result_tree_->children_[0]->value_ << std::endl;
    of_result << CSJ(ObParserResultPrintWrapper(*parse_result.result_tree_)) << std::endl;
  }
  parser.free_result(parse_result);
}

void TestParser::do_filter_hint(const char *query_str, std::ofstream &of_result, int64_t expect_error)
{
  ObString real_query = ObString::make_string(query_str).trim();
  if (real_query.length() > 0 && *real_query.ptr() != '#') {
    //ignore empty query and comment
    _OB_LOG(INFO, "query_str: %s", query_str);
    int ret = OB_SUCCESS;
    ObString result_query;
    if (OB_FAIL(ObSQLUtils::filter_hint_in_query_sql(allocator_,
                                                     session_info_,
                                                     real_query,
                                                     result_query))) {
      _OB_LOG(INFO, "query_str: %s", query_str);
      if(expect_error != -ret) {
        fprintf(stderr, "QUERY:%s failed\n", query_str);
      }
      ASSERT_EQ(expect_error, -ret);
    }
    if (expect_error == 0) {
      char buffer[OB_MAX_SQL_LENGTH];
      memset(buffer, '\0', OB_MAX_SQL_LENGTH);
      memcpy(buffer, result_query.ptr(), result_query.length());
      of_result << buffer << std::endl;
    }
  }
}

bool TestParser::pretreat_cmd(std::string line, int64_t &expect_error)
{
  bool skip_cmd = false;
  char *w = NULL;
  char *p = NULL;
  if (line.size() <= 0) skip_cmd = true;
  else if (line.at(0) == '#') skip_cmd = true;
  else if (strncmp(line.c_str(), "--error", strlen("--error")) == 0) {
    p = const_cast<char*>(line.c_str());
    w = strsep(&p, " ");
    expect_error = atol(p);
    skip_cmd = true;
  } else if (strncmp(line.c_str(), "--sql_mode", strlen("--sql_mode")) == 0) {
    p = const_cast<char*>(line.c_str());
    w = strsep(&p, " ");
    if (strncmp(p, "oracle", strlen("oracle")) == 0) {
      OB_LOG(INFO, "switch parser sql_mode to oracle");
      test::clp.sql_mode = DEFAULT_ORACLE_MODE | SMO_ORACLE;
    } else if (strncmp(p, "mysql", strlen("mysql")) == 0) {
      OB_LOG(INFO, "switch parser sql_mode to mysql");
      test::clp.sql_mode = DEFAULT_MYSQL_MODE;
    }
    skip_cmd = true;
    UNUSED(w);
  }
  return skip_cmd;
}
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc,argv);
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_parser.log", true);
  test::parse_cmd_line_param(argc, argv, test::clp);
  return RUN_ALL_TESTS();
}
