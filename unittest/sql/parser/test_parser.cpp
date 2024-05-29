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
  void parse_keyword(std::ifstream &if_tests, std::ofstream &of_result, bool is_sql_keyword);
  void do_parse_keyword(const char *keyword, std::ofstream &of_result, bool is_reserved, bool is_sql_keyword);
  bool non_reserved_keyword_can_not_be_name(const char *keyword, bool is_sql_keyword);
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

#ifdef OB_BUILD_ORACLE_PARSER
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
#endif

#ifdef OB_BUILD_ORACLE_PARSER
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
#endif

#ifdef OB_BUILD_ORACLE_PARSER
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
#endif

#ifdef OB_BUILD_ORACLE_PARSER
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
#endif

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

#ifdef OB_BUILD_ORACLE_PARSER
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
#endif

#ifdef OB_BUILD_ORACLE_PARSER
TEST_F(TestParser, test_mysql_sql_keyword)
{
  const char* test_file = "../../../../src/sql/parser/sql_parser_mysql_mode.y";
  const char* result_file = "./test_mysql_sql_keyword.result";
  const char* tmp_file = "./test_mysql_sql_keyword.tmp";
  // run tests
  std::ifstream if_tests(test_file);
  ASSERT_TRUE(if_tests.is_open());
  std::ofstream of_result(tmp_file);
  ASSERT_TRUE(of_result.is_open());
  test::clp.sql_mode = DEFAULT_MYSQL_MODE;
  set_compat_mode(oceanbase::lib::Worker::CompatMode::MYSQL);
  ASSERT_NO_FATAL_FAILURE(parse_keyword(if_tests, of_result, true));
  of_result.close();
  // verify results
  is_equal_content(tmp_file,result_file);
}
#endif

#ifdef OB_BUILD_ORACLE_PARSER
TEST_F(TestParser, test_mysql_pl_keyword)
{
  const char* test_file = "../../../../src/pl/parser/pl_parser_mysql_mode.y";
  const char* result_file = "./test_mysql_pl_keyword.result";
  const char* tmp_file = "./test_mysql_pl_keyword.tmp";
  // run tests
  std::ifstream if_tests(test_file);
  ASSERT_TRUE(if_tests.is_open());
  std::ofstream of_result(tmp_file);
  ASSERT_TRUE(of_result.is_open());
  test::clp.sql_mode = DEFAULT_MYSQL_MODE;
  set_compat_mode(oceanbase::lib::Worker::CompatMode::MYSQL);
  ASSERT_NO_FATAL_FAILURE(parse_keyword(if_tests, of_result, false));
  of_result.close();
  // verify results
  is_equal_content(tmp_file,result_file);
}
#endif

#ifdef OB_BUILD_ORACLE_PARSER
TEST_F(TestParser, test_oracle_sql_keyword)
{
  const char* test_file = "../../../../close_modules/oracle_parser/sql/parser/sql_parser_oracle_mode.y";
  const char* result_file = "./test_oracle_sql_keyword.result";
  const char* tmp_file = "./test_oracle_sql_keyword.tmp";
  // run tests
  std::ifstream if_tests(test_file);
  ASSERT_TRUE(if_tests.is_open());
  std::ofstream of_result(tmp_file);
  ASSERT_TRUE(of_result.is_open());
  test::clp.sql_mode = DEFAULT_ORACLE_MODE | SMO_ORACLE;
  set_compat_mode(oceanbase::lib::Worker::CompatMode::ORACLE);
  ASSERT_NO_FATAL_FAILURE(parse_keyword(if_tests, of_result, true));
  of_result.close();
  // verify results
  is_equal_content(tmp_file,result_file);
}
#endif

#ifdef OB_BUILD_ORACLE_PARSER
TEST_F(TestParser, test_oracle_pl_keyword)
{
  const char* test_file = "../../../../close_modules/oracle_pl/pl/parser/pl_parser_oracle_mode.y";
  const char* result_file = "./test_oracle_pl_keyword.result";
  const char* tmp_file = "./test_oracle_pl_keyword.tmp";
  // run tests
  std::ifstream if_tests(test_file);
  ASSERT_TRUE(if_tests.is_open());
  std::ofstream of_result(tmp_file);
  ASSERT_TRUE(of_result.is_open());
  test::clp.sql_mode = DEFAULT_ORACLE_MODE | SMO_ORACLE;
  set_compat_mode(oceanbase::lib::Worker::CompatMode::ORACLE);
  ASSERT_NO_FATAL_FAILURE(parse_keyword(if_tests, of_result, false));
  of_result.close();
  // verify results
  is_equal_content(tmp_file,result_file);
}
#endif

void TestParser::parse_keyword(std::ifstream &if_tests, std::ofstream &of_result, bool is_sql_keyword)
{
  std::string line;
  const char* reserved_keyword_begin = "//-----------------------------reserved keyword begin-----------------------------------------------";
  const char* reserved_keyword_end = "//-----------------------------reserved keyword end-------------------------------------------------";
  const char* non_reserved_keyword_begin = "//-----------------------------non_reserved keyword begin-------------------------------------------";
  const char* non_keyword_end = "//-----------------------------non_reserved keyword end---------------------------------------------";
  bool test_reserved_keyword = false;
  bool test_non_reserved_keyword = false;
  int64_t num_reserved_keyword = 0;
  while (std::getline(if_tests, line)) {
    if (strncmp(line.c_str(), reserved_keyword_begin, strlen(reserved_keyword_begin)) == 0) {
      of_result << "**************   Begin Test Reserved Keyword   ***************" << std::endl;
      test_reserved_keyword = true;
      continue;
    } else if (strncmp(line.c_str(), reserved_keyword_end, strlen(reserved_keyword_end)) == 0) {
      of_result << "************** Total Count of Reserved Keyword:" << num_reserved_keyword <<"   ***************" << std::endl;
      of_result << "**************   End Test Reserved Keyword   ***************" << std::endl;
      test_reserved_keyword = false;
      continue;
    } else if (strncmp(line.c_str(), non_reserved_keyword_begin, strlen(non_reserved_keyword_begin)) == 0) {
      of_result << "**************   Begin Test Non-Reserved Keyword  ***************" << std::endl;
      test_non_reserved_keyword = true;
      continue;
    } else if (strncmp(line.c_str(), non_keyword_end, strlen(non_keyword_end)) == 0) {
      of_result << "**************   End Test Non-Reserved Keyword  ***************" << std::endl;
      break;
    } else if (!test_reserved_keyword && !test_non_reserved_keyword) {
      continue;
    }
    int64_t line_size = strlen(line.c_str());
    char *tmp_buf = NULL;
    if (NULL == (tmp_buf = static_cast<char*>(allocator_.alloc(line_size + 1)))) {
      fprintf(stderr, "failed to alloc memory for: %s\n", line.c_str());
      break;
    } else {
      MEMSET(tmp_buf, '\0', line_size + 1);
      int64_t valid_buf_len = 0;
      for (int64_t i = 0; i < line_size; ++i) {
        if (isspace(line.c_str()[i])) {
          if (valid_buf_len > 0 && valid_buf_len <= line_size) {
            tmp_buf[valid_buf_len] = '\0';
            if ((is_sql_keyword && (test::clp.sql_mode & SMO_ORACLE) && strcmp(tmp_buf, "FILE_KEY") == 0) ||
                (!is_sql_keyword && (test::clp.sql_mode & SMO_ORACLE) && strcmp(tmp_buf, "BEGIN_KEY") == 0) ||
                (!is_sql_keyword && (test::clp.sql_mode & SMO_ORACLE) && strcmp(tmp_buf, "END_KEY") == 0)) {
              valid_buf_len -= 4;//FILE_KEY ==> FILE、BEGIN_KEY==> DEGIN、END_KEY==> END
              tmp_buf[valid_buf_len] = '\0';
            } else if ((is_sql_keyword && (test::clp.sql_mode & SMO_ORACLE) && strcmp(tmp_buf, "INITIAL_") == 0) ||
                       (!is_sql_keyword && (test::clp.sql_mode & SMO_ORACLE) && strcmp(tmp_buf, "NULLX") == 0)) {
              valid_buf_len -= 1;//INITIAL_ ==> INITIAL、NULLX==>NULL
              tmp_buf[valid_buf_len] = '\0';
            }
            ASSERT_NO_FATAL_FAILURE(do_parse_keyword(tmp_buf, of_result, test_reserved_keyword, is_sql_keyword));
            valid_buf_len = 0;
            if (test_reserved_keyword) {
              ++ num_reserved_keyword;
            }
          }
        } else if (line.c_str()[i] == '/' && i < line_size - 1 && line.c_str()[i + 1] == '*') {
          i = i + 2;
          while (i < line_size) {
            if (line.c_str()[i] == '*' && i < line_size - 1 && line.c_str()[i + 1] == '/') {
              i = i + 2;
              break;
            }
            ++ i;
          }
        } else if (valid_buf_len < line_size) {
          tmp_buf[valid_buf_len++] = line.c_str()[i];
        }
      }
      if (valid_buf_len > 0 && valid_buf_len <= line_size) {
        tmp_buf[valid_buf_len] = '\0';
        if ((is_sql_keyword && (test::clp.sql_mode & SMO_ORACLE) && strcmp(tmp_buf, "FILE_KEY") == 0) ||
            (!is_sql_keyword && (test::clp.sql_mode & SMO_ORACLE) && strcmp(tmp_buf, "BEGIN_KEY") == 0) ||
            (!is_sql_keyword && (test::clp.sql_mode & SMO_ORACLE) && strcmp(tmp_buf, "END_KEY") == 0)) {
          valid_buf_len -= 4;//FILE_KEY ==> FILE、BEGIN_KEY==> DEGIN、END_KEY==> END
          tmp_buf[valid_buf_len] = '\0';
        } else if ((is_sql_keyword && (test::clp.sql_mode & SMO_ORACLE) && strcmp(tmp_buf, "INITIAL_") == 0) ||
                    (!is_sql_keyword && (test::clp.sql_mode & SMO_ORACLE) && strcmp(tmp_buf, "NULLX") == 0)) {
          valid_buf_len -= 1;//INITIAL_ ==> INITIAL、NULLX==>NULL
          tmp_buf[valid_buf_len] = '\0';
        }
        ASSERT_NO_FATAL_FAILURE(do_parse_keyword(tmp_buf, of_result, test_reserved_keyword, is_sql_keyword));
        valid_buf_len = 0;
        if (test_reserved_keyword) {
          ++ num_reserved_keyword;
        }
      }
    }
  }
}
//reserved keyword test: create table table_name(column_name int);
//non_reserved keyword test: create table table_name(column_name int)
//                           select 1 alias_name from t1 alias_name;
void TestParser::do_parse_keyword(const char *keyword, std::ofstream &of_result, bool is_reserved, bool is_sql_keyword)
{
  char *buf = NULL;
  int64_t buf_len = strlen(keyword) * 2 + 1024;
  if (NULL == (buf = static_cast<char*>(allocator_.alloc(buf_len)))) {
    fprintf(stderr, "failed to alloc memory for: %s\n", keyword);
  } else if (is_sql_keyword) {
    sprintf(buf, "create table %s(%s int);", keyword, keyword);
    if (is_reserved) {
      of_result << "Reserved Keyword: "<< keyword << std::endl;
      of_result << "Test Table Name and Column Name Sql:" << buf << std::endl;
    }
  } else if (test::clp.sql_mode & SMO_ORACLE) {
    sprintf(buf, "create type %s as table of varchar2(1);", keyword);
    if (is_reserved) {
      of_result << "Reserved Keyword: "<< keyword << std::endl;
      of_result << "Test PL type name Sql:" << buf << std::endl;
    }
  } else {
    sprintf(buf, "create procedure %s () select 1;", keyword);
    if (is_reserved) {
      of_result << "Reserved Keyword: "<< keyword << std::endl;
      of_result << "Test PL procedure name Sql:" << buf << std::endl;
    }
  }
  ObString query = ObString::make_string(buf);
  int ret = OB_SUCCESS;
  _OB_LOG(INFO, "test keyword to be name: %s", buf);
  ObSQLMode mode = test::clp.sql_mode;
  ObParser parser(allocator_, mode);
  ParseResult parse_result;
  ret = parser.parse(query, parse_result);
  if ((is_reserved && ret != 0) ||
      (!is_reserved && ret == 0)) {
    ret = OB_SUCCESS;
  } else if (!is_reserved && is_sql_keyword && (test::clp.sql_mode & SMO_ORACLE) && strcmp(keyword, "CONSTRAINT") == 0) {
    ret = OB_SUCCESS;
  } else if ((is_reserved && !is_sql_keyword && (test::clp.sql_mode & SMO_ORACLE) && strcmp(keyword, "EXISTS") == 0) ||
              (is_reserved && !is_sql_keyword && (test::clp.sql_mode & SMO_ORACLE) && strcmp(keyword, "SET") == 0)) {
    ret = OB_SUCCESS;
  } else if (!is_reserved && !is_sql_keyword && non_reserved_keyword_can_not_be_name(keyword, is_sql_keyword)) {
    ret = OB_SUCCESS;
  } else {
    fprintf(stderr, "test keyword to be name failed:%s\n", buf);
  }
  ASSERT_EQ(OB_SUCCESS, -ret);
  if (!is_reserved && is_sql_keyword) {
    sprintf(buf, "select 1 %s from t1 %s;", keyword, keyword);
    query = ObString::make_string(buf);
    _OB_LOG(INFO, "test keyword to alias name: %s", buf);
    ret = parser.parse(query, parse_result);
    if (ret != 0) {
      if (non_reserved_keyword_can_not_be_name(keyword, is_sql_keyword)) {
        ret = 0;
      } else {
        fprintf(stderr, "test keyword to alias name failed:%s\n", buf);
      }
    }
  }
  ASSERT_EQ(OB_SUCCESS, -ret);
  parser.free_result(parse_result);
}

bool TestParser::non_reserved_keyword_can_not_be_name(const char *keyword, bool is_sql_keyword)
{
  bool ret = false;
  if (is_sql_keyword) {
    if (test::clp.sql_mode & SMO_ORACLE) {
      if (strcmp(keyword, "USING") == 0 ||
          strcmp(keyword, "BULK") == 0 ||
          strcmp(keyword, "COLLATE") == 0 ||
          strcmp(keyword, "CROSS") == 0 ||
          strcmp(keyword, "FULL") == 0 ||
          strcmp(keyword, "INNER") == 0 ||
          strcmp(keyword, "JOIN") == 0 ||
          strcmp(keyword, "LOG") == 0 ||
          strcmp(keyword, "LEFT") == 0 ||
          strcmp(keyword, "NATURAL") == 0 ||
          strcmp(keyword, "RETURN") == 0 ||
          strcmp(keyword, "RIGHT") == 0 ||
          strcmp(keyword, "RETURNING") == 0) {
        ret = true;
      }
    } else if (strcmp(keyword, "EXCEPT") == 0 ||
               strcmp(keyword, "INTERSECT") == 0 ||
               strcmp(keyword, "MINUS") == 0 ||
               strcmp(keyword, "MEMBER") == 0 ||
               strcmp(keyword, "SOUNDS") == 0 ||
               strcmp(keyword, "WINDOW") == 0) {
      ret = true;
    }
  } else {
    if (test::clp.sql_mode & SMO_ORACLE) {
      if (strcmp(keyword, "CHARACTER") == 0 ||
          strcmp(keyword, "CONSTANT") == 0 ||
          strcmp(keyword, "DATE") == 0 ||
          strcmp(keyword, "FLOAT") == 0 ||
          strcmp(keyword, "LOOP") == 0 ||
          strcmp(keyword, "NUMBER") == 0 ||
          strcmp(keyword, "RAW") == 0 ||
          strcmp(keyword, "REAL") == 0 ||
          strcmp(keyword, "VARCHAR") == 0 ||
          strcmp(keyword, "VARCHAR2") == 0) {
        ret = true;
      }
    }
  }
  return ret;
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
      set_compat_mode(oceanbase::lib::Worker::CompatMode::ORACLE);
    } else if (strncmp(p, "mysql", strlen("mysql")) == 0) {
      OB_LOG(INFO, "switch parser sql_mode to mysql");
      test::clp.sql_mode = DEFAULT_MYSQL_MODE;
      set_compat_mode(oceanbase::lib::Worker::CompatMode::MYSQL);
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
