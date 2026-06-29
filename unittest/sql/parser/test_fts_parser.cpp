/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL

#include "sql/parser/fts_parse.h"
#include "sql/das/iter/ob_das_text_retrieval_eval_node.h"
#include "lib/string/ob_string.h"
#include <gtest/gtest.h>
#include "lib/utility/ob_test_util.h"
#include "../test_sql_utils.h"
#include "lib/allocator/page_arena.h"
#include <fstream>
#include <iterator>
using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace test
{


class TestFtsParser: public ::testing::Test
{
public:
  TestFtsParser();
  virtual ~TestFtsParser();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestFtsParser);
protected:
  // data members
  ObArenaAllocator allocator_;
};

TestFtsParser::TestFtsParser() : allocator_(ObModIds::TEST)
{
}

TestFtsParser::~TestFtsParser()
{
}

void TestFtsParser::SetUp()
{
}

void TestFtsParser::TearDown()
{
}

static int transfer_ret_code(int ret)
{
    switch (ret) {
    case FTS_OK:
      return OB_SUCCESS;
    case FTS_ERROR_MEMORY:
      return OB_ALLOCATE_MEMORY_FAILED;
    case FTS_ERROR_SYNTAX:
      return OB_ERR_PARSER_SYNTAX;
    case FTS_ERROR_OTHER:
      return OB_ERR_UNEXPECTED;
    case FTS_ERROR_INVALID_ARGUMENT:
      return OB_NOT_SUPPORTED;
    }
    return OB_SUCCESS;
}

TEST_F(TestFtsParser, input_error_test)
{
  int ret = OB_SUCCESS;
  const char *query_str = nullptr;
  FtsParserResult ss;
  query_str = "you ++me";
  fts_parse_docment(query_str, strlen(query_str), &allocator_, &ss);
  ASSERT_EQ(OB_ERR_PARSER_SYNTAX, transfer_ret_code(ss.ret_));
  query_str = "you +-me2";
  fts_parse_docment(query_str, strlen(query_str), &allocator_, &ss);
  ASSERT_EQ(OB_ERR_PARSER_SYNTAX, transfer_ret_code(ss.ret_));
  query_str = "you --me2";
  fts_parse_docment(query_str, strlen(query_str), &allocator_, &ss);
  ASSERT_EQ(OB_ERR_PARSER_SYNTAX, transfer_ret_code(ss.ret_));
  query_str = "you (+-me2)";
  fts_parse_docment(query_str, strlen(query_str), &allocator_, &ss);
  ASSERT_EQ(OB_ERR_PARSER_SYNTAX, transfer_ret_code(ss.ret_));
  query_str = "you (+-)me2";
  fts_parse_docment(query_str, strlen(query_str), &allocator_, &ss);
  ASSERT_EQ(OB_ERR_PARSER_SYNTAX, transfer_ret_code(ss.ret_));
  query_str = "you (me2";
  fts_parse_docment(query_str, strlen(query_str), &allocator_, &ss);
  ASSERT_EQ(OB_ERR_PARSER_SYNTAX, transfer_ret_code(ss.ret_));
  query_str = "you me2)";
  fts_parse_docment(query_str, strlen(query_str), &allocator_, &ss);
  ASSERT_EQ(OB_ERR_PARSER_SYNTAX, transfer_ret_code(ss.ret_));
  query_str = "+you +(-me2 +(A C) -)";
  fts_parse_docment(query_str, strlen(query_str), &allocator_, &ss);
  ASSERT_EQ(OB_ERR_PARSER_SYNTAX, transfer_ret_code(ss.ret_));
  query_str = ">";
  fts_parse_docment(query_str, strlen(query_str), &allocator_, &ss);
  ASSERT_EQ(OB_ERR_PARSER_SYNTAX, transfer_ret_code(ss.ret_));
  query_str = "<";
  fts_parse_docment(query_str, strlen(query_str), &allocator_, &ss);
  ASSERT_EQ(OB_ERR_PARSER_SYNTAX, transfer_ret_code(ss.ret_));
  query_str = "~";
  fts_parse_docment(query_str, strlen(query_str), &allocator_, &ss);
  ASSERT_EQ(OB_ERR_PARSER_SYNTAX, transfer_ret_code(ss.ret_));
  query_str = "@";
  fts_parse_docment(query_str, strlen(query_str), &allocator_, &ss);
  ASSERT_EQ(OB_ERR_PARSER_SYNTAX, transfer_ret_code(ss.ret_));
  query_str = "*";
  fts_parse_docment(query_str, strlen(query_str), &allocator_, &ss);
  ASSERT_EQ(OB_ERR_PARSER_SYNTAX, transfer_ret_code(ss.ret_));
}

TEST_F(TestFtsParser, input_ok_test)
{
  int ret = OB_SUCCESS;
  const char *query_str = "you me";
  FtsParserResult ss;
  fts_parse_docment(query_str, strlen(query_str), &allocator_, &ss);
  ASSERT_EQ(OB_SUCCESS, transfer_ret_code(ss.ret_));
  query_str = "you +(-me2)";
  fts_parse_docment(query_str, strlen(query_str), &allocator_, &ss);
  ASSERT_EQ(OB_SUCCESS, transfer_ret_code(ss.ret_));
  query_str = "+you +(-me2 +(A +C))";
  fts_parse_docment(query_str, strlen(query_str), &allocator_, &ss);
  ASSERT_EQ(OB_SUCCESS, transfer_ret_code(ss.ret_));
}

TEST_F(TestFtsParser, nested_depth_limit_ok)
{
  const char *term = "alpha";
  const int64_t term_len = strlen(term);
  char query[FTS_MAX_BOOLEAN_NESTING_DEPTH * 3 + 6] = {0};
  int64_t pos = 0;
  FtsParserResult ss;
  for (int64_t i = 0; i < FTS_MAX_BOOLEAN_NESTING_DEPTH; ++i) {
    query[pos++] = '+';
    query[pos++] = '(';
  }
  MEMCPY(query + pos, term, term_len);
  pos += term_len;
  for (int64_t i = 0; i < FTS_MAX_BOOLEAN_NESTING_DEPTH; ++i) {
    query[pos++] = ')';
  }
  query[pos] = '\0';
  fts_parse_docment(query, static_cast<int>(pos), &allocator_, &ss);
  ASSERT_EQ(FTS_OK, ss.ret_);
}

TEST_F(TestFtsParser, nested_depth_limit_exceeded)
{
  const int64_t nesting_depth = FTS_MAX_BOOLEAN_NESTING_DEPTH + 1;
  const char *term = "alpha";
  const int64_t term_len = strlen(term);
  char query[(FTS_MAX_BOOLEAN_NESTING_DEPTH + 1) * 3 + 6] = {0};
  int64_t pos = 0;
  FtsParserResult ss;
  for (int64_t i = 0; i < nesting_depth; ++i) {
    query[pos++] = '+';
    query[pos++] = '(';
  }
  MEMCPY(query + pos, term, term_len);
  pos += term_len;
  for (int64_t i = 0; i < nesting_depth; ++i) {
    query[pos++] = ')';
  }
  query[pos] = '\0';
  fts_parse_docment(query, static_cast<int>(pos), &allocator_, &ss);
  ASSERT_EQ(FTS_ERROR_INVALID_ARGUMENT, ss.ret_);
  ASSERT_NE(nullptr, ss.err_info_.str_);
  ASSERT_NE(nullptr, strstr(ss.err_info_.str_, "fulltext boolean query with nesting depth exceeding the maximum of 256"));
}

TEST_F(TestFtsParser, unmatched_parentheses_still_syntax_error)
{
  const char *query_str = "alpha)";
  FtsParserResult ss;
  fts_parse_docment(query_str, strlen(query_str), &allocator_, &ss);
  ASSERT_EQ(FTS_ERROR_SYNTAX, ss.ret_);
  query_str = "(alpha";
  fts_parse_docment(query_str, strlen(query_str), &allocator_, &ss);
  ASSERT_EQ(FTS_ERROR_SYNTAX, ss.ret_);
}


TEST_F(TestFtsParser, create_node_test)
{
  int ret = OB_SUCCESS;
  const char *query_str = "";
  query_str = "+you -me and (let +and)";
  FtsParserResult ss;
  fts_parse_docment(query_str, strlen(query_str), &allocator_, &ss);
  ASSERT_EQ(OB_SUCCESS, transfer_ret_code(ss.ret_));
  FtsNode *node = ss.root_;
  ObFtsEvalNode *parant_node =nullptr;
  hash::ObHashMap<ObString, int32_t> tokens_map;
  ObArray<ObString> query_tokens;
  ObArray<oceanbase::sql::ObFtsEvalNode::FtsComputeFlag> child_flags_;
  const int64_t ft_word_bkt_cnt = MAX(strlen(query_str) / 10, 2);
  ret = tokens_map.create(ft_word_bkt_cnt, common::ObMemAttr(MTL_ID(), "FTWordMapTest"));
  ASSERT_EQ(OB_SUCCESS, ret);
  bool dummy_has_duplicate_tokens = false;
  ObFtsEvalNode::fts_boolean_node_create(parant_node, node, ObCollationType::CS_TYPE_UTF8MB4_GENERAL_CI, allocator_, query_tokens, tokens_map, dummy_has_duplicate_tokens);
  ASSERT_FALSE(parant_node->leaf_node_);

  ASSERT_TRUE(parant_node->child_nodes_.at(0)->leaf_node_);
  ASSERT_EQ(0, parant_node->child_nodes_.at(0)->postion_);
  ASSERT_EQ(oceanbase::sql::ObFtsEvalNode::AND, parant_node->child_flags_.at(0));

  ASSERT_TRUE(parant_node->child_nodes_.at(1)->leaf_node_);
  ASSERT_EQ(1, parant_node->child_nodes_.at(1)->postion_);
  ASSERT_EQ(oceanbase::sql::ObFtsEvalNode::NOT, parant_node->child_flags_.at(1));

  ASSERT_TRUE(parant_node->child_nodes_.at(2)->leaf_node_);
  ASSERT_EQ(2, parant_node->child_nodes_.at(2)->postion_);
  ASSERT_EQ(oceanbase::sql::ObFtsEvalNode::NO_OPERATOR, parant_node->child_flags_.at(2));

  ASSERT_FALSE(parant_node->child_nodes_.at(3)->child_nodes_.at(0)->leaf_node_);
  ASSERT_EQ(oceanbase::sql::ObFtsEvalNode::OR, parant_node->child_nodes_.at(3)->child_flags_.at(0));

  ASSERT_TRUE(parant_node->child_nodes_.at(3)->child_nodes_.at(0)->child_nodes_.at(0)->leaf_node_);
  ASSERT_EQ(3, parant_node->child_nodes_.at(3)->child_nodes_.at(0)->child_nodes_.at(0)->postion_);
  ASSERT_EQ(oceanbase::sql::ObFtsEvalNode::NO_OPERATOR, parant_node->child_nodes_.at(3)->child_nodes_.at(0)->child_flags_.at(0));

  ASSERT_TRUE(parant_node->child_nodes_.at(3)->child_nodes_.at(0)->child_nodes_.at(1)->leaf_node_);
  ASSERT_EQ(2, parant_node->child_nodes_.at(3)->child_nodes_.at(0)->child_nodes_.at(1)->postion_);
  ASSERT_EQ(oceanbase::sql::ObFtsEvalNode::AND, parant_node->child_nodes_.at(3)->child_nodes_.at(0)->child_flags_.at(1));

  ObFixedArray<double, ObIAllocator> relevences;
  relevences.set_allocator(&allocator_);
  ret = relevences.init(query_tokens.count());
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = relevences.prepare_allocate(query_tokens.count());
  ASSERT_EQ(OB_SUCCESS, ret);
  // +you -me and (let +and)
  relevences[0] = 1;
  relevences[1] = 0;
  relevences[2] = 0;
  relevences[3] = 1.1;
  double result = 0;
  ret = ObFtsEvalNode::fts_boolean_eval(parant_node, relevences, result);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, result);
  // +you -me and (let +and)
  relevences[0] = 1;
  relevences[1] = 0;
  relevences[2] = 2.2;
  relevences[3] = 1.1;
  result = 0;
  ret = ObFtsEvalNode::fts_boolean_eval(parant_node, relevences, result);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(6.5, result);
  // +you -me and (let +and)
  relevences[0] = 1;
  relevences[1] = 1.4;
  relevences[2] = 0;
  relevences[3] = 1;
  result = 0;
  ret = ObFtsEvalNode::fts_boolean_eval(parant_node, relevences, result);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(-1, result);
  // +you -me and (let +and)
  relevences[0] = 1;
  relevences[1] = 1.4;
  relevences[2] = 2.2;
  relevences[3] = 0;
  result = 0;
  ret = ObFtsEvalNode::fts_boolean_eval(parant_node, relevences, result);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(5.4, result);
}


}


int main(int argc, char **argv)
{
  system("rm -rf test_fts_parser.log");
  OB_LOGGER.set_file_name("test_fts_parser.log", true);
  OB_LOGGER.set_log_level("DEBUG");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
