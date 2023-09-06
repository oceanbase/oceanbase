/**
 * Copyright (c) 2022 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "observer/table/ob_htable_filter_parser.h"
#include "observer/table/ob_htable_filters.h"
#include <gtest/gtest.h>
#include "lib/utility/ob_test_util.h"
#include "lib/json/ob_json_print_utils.h"  // for SJ
#include <fstream>
using namespace oceanbase::common;
using namespace oceanbase::table;

class TestHFilterParser: public ::testing::Test
{
public:
  TestHFilterParser();
  virtual ~TestHFilterParser();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestHFilterParser);
protected:
  // function members
  void do_parse(const char *filter_cstr, std::ofstream &of_result, int64_t expect_error);
  bool is_prefix_equal(const char* tmp_file, const char* result_file);
  void is_equal_content(const char* tmp_file, const char* result_file);
};

TestHFilterParser::TestHFilterParser()
{
}

TestHFilterParser::~TestHFilterParser()
{
}

void TestHFilterParser::SetUp()
{
}

void TestHFilterParser::TearDown()
{
}

bool TestHFilterParser::is_prefix_equal(const char* tmp_file, const char* result_file)
{
  std::ifstream if_test(tmp_file);
  if_test.is_open();
  EXPECT_EQ(true, if_test.is_open());
  std::istream_iterator<std::string> it_test(if_test);
  std::ifstream if_expected(result_file);
  if_expected.is_open();
  EXPECT_EQ(true, if_expected.is_open());
  std::istream_iterator<std::string> it_expected(if_expected);
  return std::equal(it_test, std::istream_iterator<std::string>(), it_expected);
}

void TestHFilterParser::is_equal_content(const char* tmp_file, const char* result_file)
{
  bool is_equal = is_prefix_equal(tmp_file, result_file) & is_prefix_equal(result_file, tmp_file);
  _OB_LOG_RET(WARN, OB_SUCCESS, "result file is %s, expect file is %s, is_equal:%d", tmp_file, result_file, is_equal);
  if (is_equal) {
    std::remove(tmp_file);
  } else {
    fprintf(stdout, "The result files mismatched, you can choose to\n");
    fprintf(stdout, "diff -u %s %s\n", tmp_file, result_file);
  }
  EXPECT_EQ(true, is_equal);
}

void TestHFilterParser::do_parse(const char *filter_cstr, std::ofstream &of_result, int64_t expect_error)
{
  ObArenaAllocator allocator;
  ObHTableFilterParser parser;
  ASSERT_EQ(OB_SUCCESS, parser.init(&allocator));
  ObString filter_string = ObString::make_string(filter_cstr);
  int ret = OB_SUCCESS;
  _OB_LOG(INFO, "FILTER: >>>%.*s<<<", filter_string.length(), filter_string.ptr());
  hfilter::Filter *filter = NULL;
  ret = parser.parse_filter(filter_string, filter);
  if (OB_FAIL(ret)) {
    _OB_LOG(WARN, "failed to parse filter. msg=%s [%d.%d-%d.%d]", parser.error_msg_,
            parser.first_line_, parser.first_column_, parser.last_line_, parser.last_column_);
  }
  ASSERT_EQ(expect_error, -ret);
  if (NULL != filter){
    of_result << SJ(*filter) << std::endl;
  }
  parser.destroy();
}

TEST_F(TestHFilterParser, basic_test)
{
  const char* test_file = "./table/hfilter_parser.test";
  const char* result_file = "./table/hfilter_parser.result";
  const char* tmp_file = "./table/hfilter_parser.tmp";
  // run tests
  std::ifstream if_tests(test_file);
  ASSERT_TRUE(if_tests.is_open());
  std::ofstream of_result(tmp_file);
  ASSERT_TRUE(of_result.is_open());
  std::string line;
  int64_t case_id = 0;
  int64_t expect_error = 0;
  char *w = NULL;
  char *p = NULL;
  UNUSED(w);
  while (std::getline(if_tests, line)) {
    if (line.size() <= 0) continue;
    if (line.at(0) == '#') continue;
    if (strncmp(line.c_str(), "--error", strlen("--error")) == 0) {
      p = const_cast<char*>(line.c_str());
      w = strsep(&p, " ");
      expect_error = atol(p);
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
  is_equal_content(tmp_file, result_file);
}

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_hfilter_parser.log", true);
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
