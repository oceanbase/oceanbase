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

#define UNUSED(param) (void)param
#include <cstdint>
#include <cstdlib>
#include <cstdio>
#include <cassert>
#include <cstring>
#include <fstream>
#include <iterator>
#include <iostream>

#include <gtest/gtest.h>

#include "sql/parser/ob_sql_parser.h"
#include "sql/parser/parse_malloc.h"
#include "sql/parser/parser_proxy_func.h"

extern "C"
{
  extern void right_to_die_or_duty_to_live_c()
  {
    while (1) {
      sleep(120);
    }
  }
}

using namespace oceanbase::common;
using std::cout;
using std::endl;

// 必须实现以下4个函数
void *parser_alloc_buffer(void *malloc_pool, const int64_t buff_size)
{
  UNUSED(malloc_pool);
  return std::malloc(buff_size);
}

void parser_free_buffer(void *malloc_pool, void *buffer)
{
  UNUSED(malloc_pool);
  std::free(buffer);
}

extern "C"
{
int check_stack_overflow_in_c(int *check_overflow)
{
  *check_overflow = 0;
  return 0;
}

bool check_stack_overflow_c()
{
  return 0;
}
}

using namespace oceanbase::sql;
using namespace oceanbase::common;

void is_equal_content(const char* tmp_file, const char* result_file, bool is_record = false)
{
  std::ifstream if_test(tmp_file);
  if_test.is_open();
  EXPECT_EQ(true, if_test.is_open());
  std::istream_iterator<std::string> it_test(if_test);
  std::ifstream if_expected(result_file);
  if_expected.is_open();
  EXPECT_EQ(true, if_expected.is_open());
  std::istream_iterator<std::string> it_expected(if_expected);
  bool is_equal = std::equal(it_test, std::istream_iterator<std::string>(), it_expected);
  if (is_equal) {
    std::remove(tmp_file);
  } else if (is_record) {
    fprintf(stdout, "The result files mismatched, you can choose to\n");
    fprintf(stdout, "emacs -q %s %s\n", result_file, tmp_file);
    fprintf(stdout, "diff -u %s %s\n", result_file, tmp_file);
    fprintf(stdout, "mv %s %s\n", tmp_file, result_file);
    std::rename(tmp_file,result_file);
  } else {
    fprintf(stdout, "The result files mismatched, you can choose to\n");
    fprintf(stdout, "diff -u %s %s\n", tmp_file, result_file);
  }
  EXPECT_EQ(true, is_equal);
}

void print_result_tree(ParseNode *root, const int level, std::ostream &of)
{
  for (int i = 0 ; i < level *2; i++) {
    of << "-";
  }
  of << get_type_name(root->type_);
  if (NULL == root->str_value_) {
    of << " (null)";
  } else {
    of << " (" << root->str_value_ << ")";
  }
  of << " off: " << root->token_off_;
  of << " len: " << root->token_len_ << endl;
  for (int i = 0; i < root->num_child_; i++) {
    if (NULL == root->children_[i]) {
      // do nothing
    } else {
      print_result_tree(root->children_[i], level + 1, of);
    }
  }
}

void print_comment_list(ParseResult *p, std::ostream &of)
{
  for (int i = 0; i < p->comment_cnt_; ++i) {
    of << "i: " << i;
    of << ", off: " << p->comment_list_[i].token_off_;
    of << ", len: " << p->comment_list_[i].token_len_;
    of << endl;
  }
}

class TestFastSqlParser: public ::testing::Test
{
  virtual void SetUp() {}
  virtual void TearDown() {}
};

void test_fast_parser()
{
  ParseResult parse_result;
  int tmp_ptr = 1;
  memset(&parse_result, 0, sizeof(parse_result));
  parse_result.is_fp_ = true;
  parse_result.is_multi_query_ = false;
  parse_result.malloc_pool_ = &tmp_ptr; // 为了parse_malloc内部的检查，malloc_pool在正常情况下绝对不能为空
  parse_result.is_ignore_hint_ = false;
  parse_result.need_parameterize_ = true;
  parse_result.pl_parse_info_.is_pl_parse_ = false;
  parse_result.minus_ctx_.has_minus_ = false;
  parse_result.minus_ctx_.pos_ = -1;
  parse_result.minus_ctx_.raw_sql_offset_ = -1;
  parse_result.is_for_trigger_ = false;
  parse_result.is_dynamic_sql_ = false;
  parse_result.is_batched_multi_enabled_split_ = false;

  char input_sql[1024];

  const int64_t test_sql_cnt = 10;
  const char *test_sqls[test_sql_cnt] = {
      "select 1",
      "select 2.2345;",
      "select a from t where a = 'hello' and b = 3;",
      "select a from t where a = 1 and b = 'hello';",
      "select t1.a from t1 join t2 where b = udf_func(3) and c = 'hello';",
      "select t2.a from t1 join t2 where b = udf_func(3) and c = 'hello';",
      "select * from t;",
      "select  * from t;",
      "select t1.a from t1 join t2 where b = udf_func(3) and c = 'hello';",
      "select t1.a from t1 join t2 where b = udf_func(3.1231232) and c = "
      "'hello';"};
  const char* expected_sql_ids[test_sql_cnt] = {
    "1FE1379FE2A31B8D16219655761820A2",
    "99B8023929C1482A458CB071ADE822AC",
    "ADFDC18C98D0A001EF337A9EAB18CAD8",
    "ADFDC18C98D0A001EF337A9EAB18CAD8",
    "27B601C30CABBDF94C0117EF53236D45",
    "6C3F77365F581EC95803ED167CD82046",
    "99C43C457B0C8E5AA6666E89C80DC5CE",
    "0FD2573F48AA6C8DF7C3C71028CAFC81",
    "27B601C30CABBDF94C0117EF53236D45",
    "27B601C30CABBDF94C0117EF53236D45",
  };
  for (int i = 0; i < test_sql_cnt; i++) {
    const char *input_sql = test_sqls[i];

    int64_t new_length = std::strlen(input_sql) + 1;
    char *buf = (char *)parse_malloc(new_length, parse_result.malloc_pool_);

    parse_result.param_nodes_ = NULL;
    parse_result.tail_param_node_ = NULL;
    parse_result.no_param_sql_ = buf;
    parse_result.no_param_sql_buf_len_ = new_length;

    const int64_t SQL_ID_LENGTH = 32;
    ObSQLParser sql_parser(*(ObIAllocator *)(parse_result.malloc_pool_),
                           FP_MODE);
    char sql_id[SQL_ID_LENGTH + 1];
    int ret = sql_parser.parse_and_gen_sqlid(&tmp_ptr, input_sql,
                                             std::strlen(input_sql),
                                             sizeof(sql_id), sql_id);
    sql_id[SQL_ID_LENGTH] = '\0';
    fprintf(stdout, "compare sql id, index:%d, sql_id:%s, expected_sql_id:%s\n", i, sql_id, expected_sql_ids[i]);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(0, std::strncmp(sql_id, expected_sql_ids[i], strlen(expected_sql_ids[i])));
  }
}

void test_sql_parser()
{
  ParseResult parse_result;
  int tmp_ptr = 1;
  memset(&parse_result, 0, sizeof(parse_result));
  parse_result.sql_mode_ = DEFAULT_MYSQL_MODE;
  parse_result.is_fp_ = false;
  parse_result.is_multi_query_ = false;
  parse_result.malloc_pool_ = &tmp_ptr;  // 为了parse_malloc内部的检查，malloc_pool在正常情况下绝对不能为空
  parse_result.is_ignore_hint_ = false;
  parse_result.need_parameterize_ = true;
  parse_result.pl_parse_info_.is_pl_parse_ = false;
  parse_result.minus_ctx_.has_minus_ = false;
  parse_result.minus_ctx_.pos_ = -1;
  parse_result.minus_ctx_.raw_sql_offset_ = -1;
  parse_result.is_for_trigger_ = false;
  parse_result.is_dynamic_sql_ = false;
  parse_result.is_batched_multi_enabled_split_ = false;

  const int64_t test_sql_cnt = 10;
  const char *test_sqls[test_sql_cnt] = {
      "select 1",
      "select 2.2345;",
      "select a from t where a = 'hello' and b = 3;",
      "select a from t where a = 1 and b = 'hello';",
      "select t1.a from t1 join t2 where b = udf_func(3) and c = 'hello';",
      "select t2.a from t1 join t2 where b = udf_func(3) and c = 'hello';",
      "select * from t;",
      "select  * from t;",
      "select t1.a from t1 join t2 where b = udf_func(3) and c = 'hello';",
      "select t1.a from t1 join t2 where b = udf_func(3.1231232) and c = "
      "'hello';"};

  for (int i = 0; i < test_sql_cnt; i++) {
    const char *input_sql = test_sqls[i];

    int64_t new_length = std::strlen(input_sql) + 1;
    char *buf = (char *)parse_malloc(new_length, parse_result.malloc_pool_);

    parse_result.param_nodes_ = NULL;
    parse_result.tail_param_node_ = NULL;
    parse_result.no_param_sql_ = buf;
    parse_result.no_param_sql_buf_len_ = new_length;

    ObSQLParser sql_parser(*(ObIAllocator *)(parse_result.malloc_pool_),
                           parse_result.sql_mode_);
    int ret = sql_parser.parse(input_sql, std::strlen(input_sql), parse_result);
    ASSERT_TRUE(NULL != parse_result.result_tree_);
  }
}

void setup_parse_result(ParseResult &parse_result, int &tmp_ptr)
{
  memset(&parse_result, 0, sizeof(parse_result));
  parse_result.sql_mode_ = DEFAULT_MYSQL_MODE;
  parse_result.is_fp_ = false;
  parse_result.is_multi_query_ = false;
  parse_result.malloc_pool_ = &tmp_ptr;  // 为了parse_malloc内部的检查，malloc_pool在正常情况下绝对不能为空
  parse_result.is_ignore_hint_ = false;
  parse_result.need_parameterize_ = true;
  parse_result.pl_parse_info_.is_pl_parse_ = false;
  parse_result.minus_ctx_.has_minus_ = false;
  parse_result.minus_ctx_.pos_ = -1;
  parse_result.minus_ctx_.raw_sql_offset_ = -1;
  parse_result.is_for_trigger_ = false;
  parse_result.is_dynamic_sql_ = false;
  parse_result.is_batched_multi_enabled_split_ = false;

  parse_result.realloc_cnt_ = 10;
}

void start_test_token_offset(const char *test_sqls[],
                             int test_sql_cnt,
                             uint64_t sql_mode,
                             std::ostream &of)
{
  for (int64_t i = 0; i < test_sql_cnt; ++i) {
    ParseResult parse_result;
    int tmp_ptr = 1;
    setup_parse_result(parse_result, tmp_ptr);
    parse_result.sql_mode_ = sql_mode;
    const char *input_sql = test_sqls[i];

    ObSQLParser sql_parser(*(ObIAllocator *)(parse_result.malloc_pool_),
                           parse_result.sql_mode_);
    int ret = sql_parser.parse(input_sql, std::strlen(input_sql), parse_result);

    of << input_sql << endl;
    ASSERT_TRUE(NULL != parse_result.result_tree_);
    print_result_tree(parse_result.result_tree_, 0, of);
    print_comment_list(&parse_result, of);
  }
}

void test_token_pos()
{
  const char *test_sqls_mysql[] = {
    // test hint and comment
    "/*+ c1 */ /* c2 */ select /* ignored */ /*+ no_rewrite, index(t1 primary)    */ /* ignored  */ c1 from t1;",
    // test sys fun / add minus... / order by
    "select * from t1 where c1 = c2+1*2/3-4 order by c1 + 2 limit 1;",
    "select * from t1 where c1 = c2+1*2/3-4 order by c1 + 2 limit 1 offset 1;",
    "select /*+ index(t1.c1 primary) */* from t1 where `name` = 'abc';",
    "select /*+ index(t1.name primary) */* from t1 where `c1` = 'abc';",
  };

  const char *test_sqls_oracle[] = {
    // test hint and comment
    "/*+ c1 */ /* c2 */ select /* ignored */ /*+ no_rewrite, index(t1 primary)    */ /* ignored  */ c1 from t1;",
    // test sys fun / add minus... / order by
    "select * from t1 where c1 = c2+1*2/3-4 and rownum < 1 order by c1 + 2;",
    "select * from t1 where c1 = c2+1*2/3-4 and rownum < 1+1 order by c1 + 2;",
    "select /*+ index(t1.c1 primary) */* from t1 where \"name\" = 'abc';",
    "select /*+ index(t1.name primary) */* from t1 where \"c1\" = 'abc';",
  };

  const char* res_file = "./test_sql_fast_parser.result";
  const char* tmp_file = "./test_sql_fast_parser.tmp";
  bool generate_res_file = false;
  if (generate_res_file) {
    std::ofstream res_of(res_file);
    std::ofstream tmp_of(tmp_file);
    ASSERT_TRUE(res_of.is_open());
    ASSERT_TRUE(tmp_of.is_open());

    tmp_of << "MySQL mode: \n";
    start_test_token_offset(test_sqls_mysql,
                            sizeof(test_sqls_mysql) / sizeof(char*),
                            DEFAULT_MYSQL_MODE,
                            tmp_of);
    tmp_of << "Oracle mode: \n";
    start_test_token_offset(test_sqls_oracle,
                            sizeof(test_sqls_oracle) / sizeof(char*),
                            DEFAULT_ORACLE_MODE | SMO_ORACLE,
                            tmp_of);

    res_of.close();
    tmp_of.close();
    is_equal_content(tmp_file, res_file, generate_res_file);
  } else {
    std::ofstream tmp_of(tmp_file);
    ASSERT_TRUE(tmp_of.is_open());

    tmp_of << "MySQL mode: \n";
    start_test_token_offset(test_sqls_mysql,
                            sizeof(test_sqls_mysql) / sizeof(char*),
                            DEFAULT_MYSQL_MODE,
                            tmp_of);
    tmp_of << "Oracle mode: \n";
    start_test_token_offset(test_sqls_oracle,
                            sizeof(test_sqls_oracle) / sizeof(char*),
                            DEFAULT_ORACLE_MODE | SMO_ORACLE ,
                            tmp_of);

    tmp_of.close();
    is_equal_content(tmp_file, res_file, generate_res_file);
  }
}

TEST_F(TestFastSqlParser, linker_test)
{
  test_sql_parser();
  test_fast_parser();
  test_token_pos();
}
int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
