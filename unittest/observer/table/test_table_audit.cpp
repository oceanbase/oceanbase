/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include <gtest/gtest.h>
#define private public  // 获取private成员
#define protected public  // 获取protect成员
#include "observer/table/ob_table_filter.h"

using namespace oceanbase::common;
using namespace oceanbase::table;
using namespace oceanbase::sql;
using namespace oceanbase::share;
using namespace oceanbase::observer;
using namespace oceanbase::table::hfilter;
using namespace oceanbase::sql::stmt;

class TestTableAudit: public ::testing::Test
{
public:
  TestTableAudit();
  virtual ~TestTableAudit() {}
  virtual void SetUp();
  virtual void TearDown();
public:
  template<typename... Args>
  void construct_prop_names(const char *col, Args... args);
  void construct_prop_names(){}
  template<typename... Args>
  void construct_select_names(const char *col, Args... args);
  void construct_select_names(){}
  template<typename... Args>
  void construct_range_names(const char *col, Args... args);
  void construct_range_names(){}
private:
  ObTableEntity entity_;
  ObTableQuery query_;
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestTableAudit);
};

TestTableAudit::TestTableAudit()
{
}

void TestTableAudit::SetUp()
{
}

void TestTableAudit::TearDown()
{
  entity_.reset();
  query_.reset();
}

template<typename... Args>
void TestTableAudit::construct_prop_names(const char *col, Args... args) {
  entity_.properties_names_.push_back(ObString::make_string(col));
  construct_prop_names(args...);
}

template<typename... Args>
void TestTableAudit::construct_select_names(const char *col, Args... args) {
  query_.select_columns_.push_back(ObString::make_string(col));
  construct_select_names(args...);
}

template<typename... Args>
void TestTableAudit::construct_range_names(const char *col, Args... args) {
  query_.scan_range_columns_.push_back(ObString::make_string(col));
  construct_range_names(args...);
}

// 测试：表名为空
TEST_F(TestTableAudit, singleOpStmt1)
{
  const int64_t buf_len = 128;
  char buf[buf_len] = {0};
  int64_t pos = 0;
  ObString table_name = ObString::make_empty_string();
  ObTableOperation op;
  op.entity_ = &entity_;
  op.operation_type_ = ObTableOperationType::INSERT;
  construct_prop_names("c1", "c2", "c3");
  ASSERT_EQ(OB_SUCCESS, op.generate_stmt(table_name, buf, buf_len, pos));
  const char *result = "insert (null) \"c1\", \"c2\", \"c3\"";
  ASSERT_STRCASEEQ(result, buf);
  ASSERT_EQ(strlen(result), pos);
  ASSERT_EQ(op.get_stmt_length(table_name), pos);
}

// 测试：列名为空
TEST_F(TestTableAudit, singleOpStmt2)
{
  const int64_t buf_len = 128;
  char buf[buf_len] = {0};
  int64_t pos = 0;
  ObString table_name = ObString::make_string("test");
  ObTableOperation op;
  op.entity_ = &entity_;
  op.operation_type_ = ObTableOperationType::INSERT;
  ASSERT_EQ(OB_SUCCESS, op.generate_stmt(table_name, buf, buf_len, pos));
  const char *result = "insert test ";
  ASSERT_STRCASEEQ(result, buf);
  ASSERT_EQ(strlen(result), pos);
  ASSERT_EQ(op.get_stmt_length(table_name), pos);
}

// 测试：buf为空
TEST_F(TestTableAudit, singleOpStmt3)
{
  const int64_t buf_len = 128;
  char *buf = nullptr;
  int64_t pos = 0;
  ObString table_name = ObString::make_string("test");
  ObTableOperation op;
  op.entity_ = &entity_;
  op.operation_type_ = ObTableOperationType::INSERT;
  ASSERT_EQ(OB_INVALID_ARGUMENT, op.generate_stmt(table_name, buf, buf_len, pos));
  ASSERT_EQ(0, pos);
}

// 测试：buf长度不足
TEST_F(TestTableAudit, singleOpStmt4)
{
  const int64_t buf_len = 1;
  char buf[buf_len] = {0};
  int64_t pos = 0;
  ObString table_name = ObString::make_string("test");
  ObTableOperation op;
  op.entity_ = &entity_;
  op.operation_type_ = ObTableOperationType::INSERT;
  ASSERT_EQ(OB_BUF_NOT_ENOUGH, op.generate_stmt(table_name, buf, buf_len, pos));
  ASSERT_EQ(0, pos);

  const int64_t buf_len2 = 10;
  char buf2[buf_len2] = {0};
  pos = 0;
  ASSERT_EQ(OB_BUF_NOT_ENOUGH, op.generate_stmt(table_name, buf2, buf_len2, pos));
}

// 测试：操作类型
TEST_F(TestTableAudit, singleOpStmt5)
{
  const int64_t buf_len = 128;
  char buf[buf_len] = {0};
  int64_t pos = 0;
  ObString table_name = ObString::make_string("test");
  ObTableOperation op;
  op.entity_ = &entity_;

  // get
  op.operation_type_ = ObTableOperationType::GET;
  construct_prop_names("c1");
  ASSERT_EQ(OB_SUCCESS, op.generate_stmt(table_name, buf, buf_len, pos));
  const char *result = "get test \"c1\"";
  ASSERT_STRCASEEQ(result, buf);
  ASSERT_EQ(strlen(result), pos);
  ASSERT_EQ(op.get_stmt_length(table_name), pos);

  // insert
  MEMSET(buf, 0, buf_len);
  pos = 0;
  op.operation_type_ = ObTableOperationType::INSERT;
  ASSERT_EQ(OB_SUCCESS, op.generate_stmt(table_name, buf, buf_len, pos));
  result = "insert test \"c1\"";
  ASSERT_STRCASEEQ(result, buf);
  ASSERT_EQ(strlen(result), pos);
  ASSERT_EQ(op.get_stmt_length(table_name), pos);

  // delete
  MEMSET(buf, 0, buf_len);
  pos = 0;
  op.operation_type_ = ObTableOperationType::DEL;
  ASSERT_EQ(OB_SUCCESS, op.generate_stmt(table_name, buf, buf_len, pos));
  result = "delete test \"c1\"";
  ASSERT_STRCASEEQ(result, buf);
  ASSERT_EQ(strlen(result), pos);
  ASSERT_EQ(op.get_stmt_length(table_name), pos);

  // update
  MEMSET(buf, 0, buf_len);
  pos = 0;
  op.operation_type_ = ObTableOperationType::UPDATE;
  ASSERT_EQ(OB_SUCCESS, op.generate_stmt(table_name, buf, buf_len, pos));
  result = "update test \"c1\"";
  ASSERT_STRCASEEQ(result, buf);
  ASSERT_EQ(strlen(result), pos);
  ASSERT_EQ(op.get_stmt_length(table_name), pos);

  // insertOrUpdate
  MEMSET(buf, 0, buf_len);
  pos = 0;
  op.operation_type_ = ObTableOperationType::INSERT_OR_UPDATE;
  ASSERT_EQ(OB_SUCCESS, op.generate_stmt(table_name, buf, buf_len, pos));
  result = "insertOrUpdate test \"c1\"";
  ASSERT_STRCASEEQ(result, buf);
  ASSERT_EQ(strlen(result), pos);
  ASSERT_EQ(op.get_stmt_length(table_name), pos);

  // replace
  MEMSET(buf, 0, buf_len);
  pos = 0;
  op.operation_type_ = ObTableOperationType::REPLACE;
  ASSERT_EQ(OB_SUCCESS, op.generate_stmt(table_name, buf, buf_len, pos));
  result = "replace test \"c1\"";
  ASSERT_STRCASEEQ(result, buf);
  ASSERT_EQ(strlen(result), pos);
  ASSERT_EQ(op.get_stmt_length(table_name), pos);

  // increment
  MEMSET(buf, 0, buf_len);
  pos = 0;
  op.operation_type_ = ObTableOperationType::INCREMENT;
  ASSERT_EQ(OB_SUCCESS, op.generate_stmt(table_name, buf, buf_len, pos));
  result = "increment test \"c1\"";
  ASSERT_STRCASEEQ(result, buf);
  ASSERT_EQ(strlen(result), pos);
  ASSERT_EQ(op.get_stmt_length(table_name), pos);

  // append
  MEMSET(buf, 0, buf_len);
  pos = 0;
  op.operation_type_ = ObTableOperationType::APPEND;
  ASSERT_EQ(OB_SUCCESS, op.generate_stmt(table_name, buf, buf_len, pos));
  result = "append test \"c1\"";
  ASSERT_STRCASEEQ(result, buf);
  ASSERT_EQ(strlen(result), pos);
  ASSERT_EQ(op.get_stmt_length(table_name), pos);

  // checkAndInsertUp
  MEMSET(buf, 0, buf_len);
  pos = 0;
  op.operation_type_ = ObTableOperationType::CHECK_AND_INSERT_UP;
  ASSERT_EQ(OB_SUCCESS, op.generate_stmt(table_name, buf, buf_len, pos));
  result = "checkAndInsertUp test \"c1\"";
  ASSERT_STRCASEEQ(result, buf);
  ASSERT_EQ(strlen(result), pos);
  ASSERT_EQ(op.get_stmt_length(table_name), pos);

  // put
  MEMSET(buf, 0, buf_len);
  pos = 0;
  op.operation_type_ = ObTableOperationType::PUT;
  ASSERT_EQ(OB_SUCCESS, op.generate_stmt(table_name, buf, buf_len, pos));
  result = "put test \"c1\"";
  ASSERT_STRCASEEQ(result, buf);
  ASSERT_EQ(strlen(result), pos);
  ASSERT_EQ(op.get_stmt_length(table_name), pos);

  // unknown
  MEMSET(buf, 0, buf_len);
  pos = 0;
  op.operation_type_ = ObTableOperationType::TTL;
  ASSERT_EQ(OB_SUCCESS, op.generate_stmt(table_name, buf, buf_len, pos));
  result = "unknown test \"c1\"";
  ASSERT_STRCASEEQ(result, buf);
  ASSERT_EQ(strlen(result), pos);
  ASSERT_EQ(op.get_stmt_length(table_name), pos);

  // unknown
  MEMSET(buf, 0, buf_len);
  pos = 0;
  op.operation_type_ = ObTableOperationType::SCAN;
  ASSERT_EQ(OB_SUCCESS, op.generate_stmt(table_name, buf, buf_len, pos));
  result = "unknown test \"c1\"";
  ASSERT_STRCASEEQ(result, buf);
  ASSERT_EQ(strlen(result), pos);
  ASSERT_EQ(op.get_stmt_length(table_name), pos);

  // unknown
  MEMSET(buf, 0, buf_len);
  pos = 0;
  op.operation_type_ = ObTableOperationType::TRIGGER;
  ASSERT_EQ(OB_SUCCESS, op.generate_stmt(table_name, buf, buf_len, pos));
  result = "unknown test \"c1\"";
  ASSERT_STRCASEEQ(result, buf);
  ASSERT_EQ(strlen(result), pos);
  ASSERT_EQ(op.get_stmt_length(table_name), pos);

  // unknown
  MEMSET(buf, 0, buf_len);
  pos = 0;
  op.operation_type_ = ObTableOperationType::INVALID;
  ASSERT_EQ(OB_SUCCESS, op.generate_stmt(table_name, buf, buf_len, pos));
  result = "unknown test \"c1\"";
  ASSERT_STRCASEEQ(result, buf);
  ASSERT_EQ(strlen(result), pos);
  ASSERT_EQ(op.get_stmt_length(table_name), pos);
}

// 测试：表名为空
TEST_F(TestTableAudit, queryStmt1)
{
  const int64_t buf_len = 128;
  char buf[buf_len] = {0};
  int64_t pos = 0;
  ObString table_name = ObString::make_empty_string();
  construct_select_names("c1", "c2", "c3");
  construct_range_names("c1", "c2", "c3");
  query_.index_name_ = ObString::make_string("idx");
  ASSERT_EQ(OB_SUCCESS, query_.generate_stmt(table_name, buf, buf_len, pos));
  const char *result = "query (null) \"c1\", \"c2\", \"c3\" range:\"c1\", \"c2\", \"c3\" index:idx";
  ASSERT_STRCASEEQ(result, buf);
  ASSERT_EQ(strlen(result), pos);
  ASSERT_EQ(query_.get_stmt_length(table_name), pos);
}

// 测试：select column为空
TEST_F(TestTableAudit, queryStmt2)
{
  const int64_t buf_len = 128;
  char buf[buf_len] = {0};
  int64_t pos = 0;
  ObString table_name = ObString::make_string("test");
  query_.index_name_ = ObString::make_string("idx");
  construct_range_names("c1", "c2", "c3");
  ASSERT_EQ(OB_SUCCESS, query_.generate_stmt(table_name, buf, buf_len, pos));
  const char *result = "query test  range:\"c1\", \"c2\", \"c3\" index:idx";
  ASSERT_STRCASEEQ(result, buf);
  ASSERT_EQ(strlen(result), pos);
  ASSERT_EQ(query_.get_stmt_length(table_name), pos);
}

// 测试：scan column为空
TEST_F(TestTableAudit, queryStmt3)
{
  const int64_t buf_len = 128;
  char buf[buf_len] = {0};
  int64_t pos = 0;
  ObString table_name = ObString::make_string("test");
  query_.index_name_ = ObString::make_string("idx");
  construct_select_names("c1", "c2", "c3");
  ASSERT_EQ(OB_SUCCESS, query_.generate_stmt(table_name, buf, buf_len, pos));
  const char *result = "query test \"c1\", \"c2\", \"c3\" range: index:idx";
  ASSERT_STRCASEEQ(result, buf);
  ASSERT_EQ(strlen(result), pos);
  ASSERT_EQ(query_.get_stmt_length(table_name), pos);
}

// 测试：scan column为空
TEST_F(TestTableAudit, queryStmt4)
{
  const int64_t buf_len = 128;
  char buf[buf_len] = {0};
  int64_t pos = 0;
  ObString table_name = ObString::make_string("test");
  query_.index_name_ = ObString::make_string("idx");
  construct_select_names("c1", "c2", "c3");
  ASSERT_EQ(OB_SUCCESS, query_.generate_stmt(table_name, buf, buf_len, pos));
  const char *result = "query test \"c1\", \"c2\", \"c3\" range: index:idx";
  ASSERT_STRCASEEQ(result, buf);
  ASSERT_EQ(strlen(result), pos);
  ASSERT_EQ(query_.get_stmt_length(table_name), pos);
}

// 测试：index name为空
TEST_F(TestTableAudit, queryStmt5)
{
  const int64_t buf_len = 128;
  char buf[buf_len] = {0};
  int64_t pos = 0;
  ObString table_name = ObString::make_string("test");
  construct_select_names("c1", "c2", "c3");
  construct_range_names("c1", "c2", "c3");
  ASSERT_EQ(OB_SUCCESS, query_.generate_stmt(table_name, buf, buf_len, pos));
  const char *result = "query test \"c1\", \"c2\", \"c3\" range:\"c1\", \"c2\", \"c3\" index:(null)";
  ASSERT_STRCASEEQ(result, buf);
  ASSERT_EQ(strlen(result), pos);
  ASSERT_EQ(query_.get_stmt_length(table_name), pos);
}

// 测试：buf为空
TEST_F(TestTableAudit, queryStmt6)
{
  const int64_t buf_len = 128;
  char *buf = nullptr;
  int64_t pos = 0;
  ObString table_name = ObString::make_string("test");
  query_.index_name_ = ObString::make_string("idx");
  construct_select_names("c1", "c2", "c3");
  construct_range_names("c1", "c2", "c3");
  ASSERT_EQ(OB_INVALID_ARGUMENT, query_.generate_stmt(table_name, buf, buf_len, pos));
  ASSERT_EQ(0, pos);
}

// 测试：buf长度不足
TEST_F(TestTableAudit, queryStmt7)
{
  const int64_t buf_len = 1;
  char buf[buf_len] = {0};
  int64_t pos = 0;
  ObString table_name = ObString::make_string("test");
  query_.index_name_ = ObString::make_string("idx");
  construct_select_names("c1", "c2", "c3");
  construct_range_names("c1", "c2", "c3");
  ASSERT_EQ(OB_BUF_NOT_ENOUGH, query_.generate_stmt(table_name, buf, buf_len, pos));
  ASSERT_EQ(0, pos);

  const int64_t buf_len2 = 10;
  char buf2[buf_len2] = {0};
  pos = 0;
  ASSERT_EQ(OB_BUF_NOT_ENOUGH, query_.generate_stmt(table_name, buf2, buf_len2, pos));

  const int64_t buf_len3 = 30;
  char buf3[buf_len3] = {0};
  pos = 0;
  ASSERT_EQ(OB_BUF_NOT_ENOUGH, query_.generate_stmt(table_name, buf3, buf_len3, pos));

  const int64_t buf_len4 = 30;
  char buf4[buf_len4] = {0};
  pos = 0;
  ASSERT_EQ(OB_BUF_NOT_ENOUGH, query_.generate_stmt(table_name, buf4, buf_len4, pos));
}

// 测试：get_compare_op_name
TEST_F(TestTableAudit, get_compare_op_name)
{
  ASSERT_STRCASEEQ("EQUAL", FilterUtils::get_compare_op_name(CompareOperator::EQUAL));
  ASSERT_STRCASEEQ("GREATER", FilterUtils::get_compare_op_name(CompareOperator::GREATER));
  ASSERT_STRCASEEQ("GREATER_OR_EQUAL", FilterUtils::get_compare_op_name(CompareOperator::GREATER_OR_EQUAL));
  ASSERT_STRCASEEQ("LESS", FilterUtils::get_compare_op_name(CompareOperator::LESS));
  ASSERT_STRCASEEQ("LESS_OR_EQUAL", FilterUtils::get_compare_op_name(CompareOperator::LESS_OR_EQUAL));
  ASSERT_STRCASEEQ("NO_OP", FilterUtils::get_compare_op_name(CompareOperator::NO_OP));
  ASSERT_STRCASEEQ("NOT_EQUAL", FilterUtils::get_compare_op_name(CompareOperator::NOT_EQUAL));
  ASSERT_STRCASEEQ("IS", FilterUtils::get_compare_op_name(CompareOperator::IS));
  ASSERT_STRCASEEQ("IS_NOT", FilterUtils::get_compare_op_name(CompareOperator::IS_NOT));
}

// 测试：RowFilter
TEST_F(TestTableAudit, RowFilter)
{
  Comparable cmp(ObString::make_string("hello"));
  RowFilter filter(CompareOperator::EQUAL, &cmp);
  const int64_t buf_len = 128;
  char buf[buf_len] = {0};
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, filter.get_format_filter_string(buf, buf_len, pos));
  const char *result = "RowFilter EQUAL";
  ASSERT_STRCASEEQ(result, buf);
  ASSERT_EQ(strlen(result), pos);
  ASSERT_EQ(filter.get_format_filter_string_length(), pos);

  ASSERT_EQ(OB_INVALID_ARGUMENT, filter.get_format_filter_string(nullptr, buf_len, pos));
  ASSERT_EQ(OB_BUF_NOT_ENOUGH, filter.get_format_filter_string(buf, 0, pos));
}

// 测试：QualifierFilter
TEST_F(TestTableAudit, QualifierFilter)
{
  Comparable cmp(ObString::make_string("hello"));
  QualifierFilter filter(CompareOperator::EQUAL, &cmp);
  const int64_t buf_len = 128;
  char buf[buf_len] = {0};
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, filter.get_format_filter_string(buf, buf_len, pos));
  const char *result = "QualifierFilter EQUAL";
  ASSERT_STRCASEEQ(result, buf);
  ASSERT_EQ(strlen(result), pos);
  ASSERT_EQ(filter.get_format_filter_string_length(), pos);

  ASSERT_EQ(OB_INVALID_ARGUMENT, filter.get_format_filter_string(nullptr, buf_len, pos));
  ASSERT_EQ(OB_BUF_NOT_ENOUGH, filter.get_format_filter_string(buf, 0, pos));
}

// 测试：ValueFilter
TEST_F(TestTableAudit, ValueFilter)
{
  Comparable cmp(ObString::make_string("hello"));
  ValueFilter filter(CompareOperator::EQUAL, &cmp);
  const int64_t buf_len = 128;
  char buf[buf_len] = {0};
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, filter.get_format_filter_string(buf, buf_len, pos));
  const char *result = "ValueFilter EQUAL";
  ASSERT_STRCASEEQ(result, buf);
  ASSERT_EQ(strlen(result), pos);
  ASSERT_EQ(filter.get_format_filter_string_length(), pos);

  ASSERT_EQ(OB_INVALID_ARGUMENT, filter.get_format_filter_string(nullptr, buf_len, pos));
  ASSERT_EQ(OB_BUF_NOT_ENOUGH, filter.get_format_filter_string(buf, 0, pos));
}

// 测试：FilterList
TEST_F(TestTableAudit, FilterList)
{
  Comparable cmp(ObString::make_string("hello"));
  ValueFilter vfilter1(CompareOperator::EQUAL, &cmp);
  ValueFilter vfilter2(CompareOperator::GREATER, &cmp);
  // FilterListAND
  FilterListAND and_filter(FilterListBase::Operator::MUST_PASS_ALL);
  const int64_t buf_len = 128;
  char buf[buf_len] = {0};
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, and_filter.filters_.push_back(&vfilter1));
  ASSERT_EQ(OB_SUCCESS, and_filter.filters_.push_back(&vfilter2));
  ASSERT_EQ(OB_SUCCESS, and_filter.get_format_filter_string(buf, buf_len, pos));
  const char *result = "FilterListAND AND ValueFilter EQUAL, ValueFilter GREATER";
  ASSERT_STRCASEEQ(result, buf);
  ASSERT_EQ(strlen(result), pos);
  ASSERT_EQ(and_filter.get_format_filter_string_length(), pos);

  // FilterListOR
  FilterListOR or_filter(FilterListBase::Operator::MUST_PASS_ALL);
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, or_filter.filters_.push_back(&vfilter1));
  ASSERT_EQ(OB_SUCCESS, or_filter.filters_.push_back(&vfilter2));
  ASSERT_EQ(OB_SUCCESS, or_filter.get_format_filter_string(buf, buf_len, pos));
  result = "FilterListOR AND ValueFilter EQUAL, ValueFilter GREATER";
  ASSERT_STRCASEEQ(result, buf);
  ASSERT_EQ(strlen(result), pos);
  ASSERT_EQ(or_filter.get_format_filter_string_length(), pos);

  // ObTableFilterListAnd
  ObTableFilterListAnd tb_and_filter(FilterListBase::Operator::MUST_PASS_ALL);
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, tb_and_filter.filters_.push_back(&vfilter1));
  ASSERT_EQ(OB_SUCCESS, tb_and_filter.filters_.push_back(&vfilter2));
  ASSERT_EQ(OB_SUCCESS, tb_and_filter.get_format_filter_string(buf, buf_len, pos));
  result = "ObTableFilterListAnd AND ValueFilter EQUAL, ValueFilter GREATER";
  ASSERT_STRCASEEQ(result, buf);
  ASSERT_EQ(strlen(result), pos);
  ASSERT_EQ(tb_and_filter.get_format_filter_string_length(), pos);

  // ObTableFilterListOr
  ObTableFilterListOr tb_or_filter(FilterListBase::Operator::MUST_PASS_ALL);
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, tb_or_filter.filters_.push_back(&vfilter1));
  ASSERT_EQ(OB_SUCCESS, tb_or_filter.filters_.push_back(&vfilter2));
  ASSERT_EQ(OB_SUCCESS, tb_or_filter.get_format_filter_string(buf, buf_len, pos));
  result = "ObTableFilterListOr AND ValueFilter EQUAL, ValueFilter GREATER";
  ASSERT_STRCASEEQ(result, buf);
  ASSERT_EQ(strlen(result), pos);
  ASSERT_EQ(tb_or_filter.get_format_filter_string_length(), pos);

  ASSERT_EQ(OB_INVALID_ARGUMENT, tb_or_filter.get_format_filter_string(nullptr, buf_len, pos));
  ASSERT_EQ(OB_BUF_NOT_ENOUGH, tb_or_filter.get_format_filter_string(buf, 0, pos));
}

// 测试：SkipFilter
TEST_F(TestTableAudit, SkipFilter)
{
  SkipFilter filter(nullptr);
  const int64_t buf_len = 128;
  char buf[buf_len] = {0};
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, filter.get_format_filter_string(buf, buf_len, pos));
  const char *result = "SkipFilter ";
  ASSERT_STRCASEEQ(result, buf);
  ASSERT_EQ(strlen(result), pos);
  ASSERT_EQ(filter.get_format_filter_string_length(), pos);

  MEMSET(buf, 0, buf_len);
  pos = 0;
  Comparable cmp(ObString::make_string("hello"));
  ValueFilter vfilter(CompareOperator::EQUAL, &cmp);
  filter.filter_ = &vfilter;
  ASSERT_EQ(OB_SUCCESS, filter.get_format_filter_string(buf, buf_len, pos));
  result = "SkipFilter ValueFilter EQUAL";
  ASSERT_STRCASEEQ(result, buf);
  ASSERT_EQ(strlen(result), pos);
  ASSERT_EQ(filter.get_format_filter_string_length(), pos);

  ASSERT_EQ(OB_INVALID_ARGUMENT, filter.get_format_filter_string(nullptr, buf_len, pos));
  ASSERT_EQ(OB_BUF_NOT_ENOUGH, filter.get_format_filter_string(buf, 0, pos));
}

// 测试：WhileMatchFilter
TEST_F(TestTableAudit, WhileMatchFilter)
{
  WhileMatchFilter filter(nullptr);
  const int64_t buf_len = 128;
  char buf[buf_len] = {0};
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, filter.get_format_filter_string(buf, buf_len, pos));
  const char *result = "WhileMatchFilter ";
  ASSERT_STRCASEEQ(result, buf);
  ASSERT_EQ(strlen(result), pos);
  ASSERT_EQ(filter.get_format_filter_string_length(), pos);

  MEMSET(buf, 0, buf_len);
  pos = 0;
  Comparable cmp(ObString::make_string("hello"));
  ValueFilter vfilter(CompareOperator::EQUAL, &cmp);
  filter.filter_ = &vfilter;
  ASSERT_EQ(OB_SUCCESS, filter.get_format_filter_string(buf, buf_len, pos));
  result = "WhileMatchFilter ValueFilter EQUAL";
  ASSERT_STRCASEEQ(result, buf);
  ASSERT_EQ(strlen(result), pos);
  ASSERT_EQ(filter.get_format_filter_string_length(), pos);

  ASSERT_EQ(OB_INVALID_ARGUMENT, filter.get_format_filter_string(nullptr, buf_len, pos));
  ASSERT_EQ(OB_BUF_NOT_ENOUGH, filter.get_format_filter_string(buf, 0, pos));
}

// 测试：SingleColumnValueFilter
TEST_F(TestTableAudit, SingleColumnValueFilter)
{
  ObString family = ObString::make_string("family1");
  ObString qualifier = ObString::make_string("qualifier1");
  Comparable cmp(ObString::make_string("hello"));
  SingleColumnValueFilter filter(family, qualifier, CompareOperator::EQUAL, &cmp);
  const int64_t buf_len = 128;
  char buf[buf_len] = {0};
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, filter.get_format_filter_string(buf, buf_len, pos));
  const char *result = "SingleColumnValueFilter family1 qualifier1 EQUAL";
  ASSERT_STRCASEEQ(result, buf);
  ASSERT_EQ(strlen(result), pos);
  ASSERT_EQ(filter.get_format_filter_string_length(), pos);

  ASSERT_EQ(OB_INVALID_ARGUMENT, filter.get_format_filter_string(nullptr, buf_len, pos));
  ASSERT_EQ(OB_BUF_NOT_ENOUGH, filter.get_format_filter_string(buf, 0, pos));
}

// 测试：PageFilter
TEST_F(TestTableAudit, PageFilter)
{
  PageFilter filter(1);
  const int64_t buf_len = 128;
  char buf[buf_len] = {0};
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, filter.get_format_filter_string(buf, buf_len, pos));
  const char *result = "PageFilter";
  ASSERT_STRCASEEQ(result, buf);
  ASSERT_EQ(strlen(result), pos);
  ASSERT_EQ(filter.get_format_filter_string_length(), pos);

  ASSERT_EQ(OB_INVALID_ARGUMENT, filter.get_format_filter_string(nullptr, buf_len, pos));
  ASSERT_EQ(OB_BUF_NOT_ENOUGH, filter.get_format_filter_string(buf, 0, pos));
}

// 测试：ColumnCountGetFilter
TEST_F(TestTableAudit, ColumnCountGetFilter)
{
  ColumnCountGetFilter filter(1);
  const int64_t buf_len = 128;
  char buf[buf_len] = {0};
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, filter.get_format_filter_string(buf, buf_len, pos));
  const char *result = "ColumnCountGetFilter";
  ASSERT_STRCASEEQ(result, buf);
  ASSERT_EQ(strlen(result), pos);
  ASSERT_EQ(filter.get_format_filter_string_length(), pos);

  ASSERT_EQ(OB_INVALID_ARGUMENT, filter.get_format_filter_string(nullptr, buf_len, pos));
  ASSERT_EQ(OB_BUF_NOT_ENOUGH, filter.get_format_filter_string(buf, 0, pos));
}

// 测试：CheckAndMutateFilter
TEST_F(TestTableAudit, CheckAndMutateFilter)
{
  ObString family = ObString::make_string("family1");
  ObString qualifier = ObString::make_string("qualifier1");
  Comparable cmp(ObString::make_string("hello"));
  CheckAndMutateFilter filter(family, qualifier, CompareOperator::EQUAL, &cmp, false);
  const int64_t buf_len = 128;
  char buf[buf_len] = {0};
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, filter.get_format_filter_string(buf, buf_len, pos));
  const char *result = "CheckAndMutateFilter family1 qualifier1 EQUAL";
  ASSERT_STRCASEEQ(result, buf);
  ASSERT_EQ(strlen(result), pos);
  ASSERT_EQ(filter.get_format_filter_string_length(), pos);

  ASSERT_EQ(OB_INVALID_ARGUMENT, filter.get_format_filter_string(nullptr, buf_len, pos));
  ASSERT_EQ(OB_BUF_NOT_ENOUGH, filter.get_format_filter_string(buf, 0, pos));
}

// 测试：ObTableCompareFilter
TEST_F(TestTableAudit, ObTableCompareFilter)
{
  Comparable cmp(ObString::make_string("hello"));
  ObTableCompareFilter filter(CompareOperator::EQUAL, &cmp);
  ObTableComparator comparator(ObString::make_string("c1"), ObString::make_string("hello"));
  filter.comparator_ = &comparator;
  const int64_t buf_len = 128;
  char buf[buf_len] = {0};
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, filter.get_format_filter_string(buf, buf_len, pos));
  const char *result = "ObTableCompareFilter c1 EQUAL";
  ASSERT_STRCASEEQ(result, buf);
  ASSERT_EQ(strlen(result), pos);
  ASSERT_EQ(filter.get_format_filter_string_length(), pos);

  ASSERT_EQ(OB_INVALID_ARGUMENT, filter.get_format_filter_string(nullptr, buf_len, pos));
  ASSERT_EQ(OB_BUF_NOT_ENOUGH, filter.get_format_filter_string(buf, 0, pos));
}

// 测试: ObTableAuditUtils
TEST_F(TestTableAudit, ObTableAuditUtils)
{
  ASSERT_EQ(StmtType::T_KV_GET, ObTableAuditUtils::get_stmt_type(ObTableOperationType::GET));
  ASSERT_EQ(StmtType::T_KV_INSERT, ObTableAuditUtils::get_stmt_type(ObTableOperationType::INSERT));
  ASSERT_EQ(StmtType::T_KV_DELETE, ObTableAuditUtils::get_stmt_type(ObTableOperationType::DEL));
  ASSERT_EQ(StmtType::T_KV_UPDATE, ObTableAuditUtils::get_stmt_type(ObTableOperationType::UPDATE));
  ASSERT_EQ(StmtType::T_KV_INSERT_OR_UPDATE, ObTableAuditUtils::get_stmt_type(ObTableOperationType::INSERT_OR_UPDATE));
  ASSERT_EQ(StmtType::T_KV_REPLACE, ObTableAuditUtils::get_stmt_type(ObTableOperationType::REPLACE));
  ASSERT_EQ(StmtType::T_KV_INCREMENT, ObTableAuditUtils::get_stmt_type(ObTableOperationType::INCREMENT));
  ASSERT_EQ(StmtType::T_KV_APPEND, ObTableAuditUtils::get_stmt_type(ObTableOperationType::APPEND));
  ASSERT_EQ(StmtType::T_KV_PUT, ObTableAuditUtils::get_stmt_type(ObTableOperationType::PUT));
  ASSERT_EQ(StmtType::T_MAX, ObTableAuditUtils::get_stmt_type((ObTableOperationType::Type)-1)); // Test for invalid input
}

// 测试: ObTableAuditMultiOp
TEST_F(TestTableAudit, ObTableAuditMultiOp)
{
  const int64_t buf_len = 128;
  char buf[buf_len] = {0};
  int64_t pos = 0;
  ObTableOperationType::Type op_type = ObTableOperationType::Type::INSERT;
  ObSEArray<ObTableOperation, 8> ops;
  ObTableAuditMultiOp multi_op(op_type, ops);
  // test1: empty table name
  ObString table_name = ObString::make_empty_string();
  ASSERT_EQ(OB_SUCCESS, multi_op.generate_stmt(table_name, buf, buf_len, pos));
  const char *result = "multi insert (null) ";
  ASSERT_STRCASEEQ(result, buf);
  ASSERT_EQ(strlen(result), pos);
  ASSERT_EQ(multi_op.get_stmt_length(table_name), pos);
  // test2: unknow operation type
  MEMSET(buf, 0, buf_len);
  pos = 0;
  table_name = ObString::make_string("test");
  multi_op.op_type_ = (ObTableOperationType::Type) - 1;
  table_name = ObString::make_empty_string();
  ASSERT_EQ(OB_SUCCESS, multi_op.generate_stmt(table_name, buf, buf_len, pos));
  result = "multi unknown (null) ";
  ASSERT_STRCASEEQ(result, buf);
  ASSERT_EQ(strlen(result), pos);
  ASSERT_EQ(multi_op.get_stmt_length(table_name), pos);
  // test3: empty prop name
  MEMSET(buf, 0, buf_len);
  pos = 0;
  ObTableOperation op;
  op.set_type(op_type);
  op.set_entity(entity_);
  ASSERT_EQ(OB_SUCCESS, ops.push_back(op));
  table_name = ObString::make_string("test");
  multi_op.op_type_ = ObTableOperationType::Type::INSERT;
  ASSERT_EQ(OB_SUCCESS, multi_op.generate_stmt(table_name, buf, buf_len, pos));
  result = "multi insert test ";
  ASSERT_STRCASEEQ(result, buf);
  ASSERT_EQ(strlen(result), pos);
  ASSERT_EQ(multi_op.get_stmt_length(table_name), pos);
  // test4: one prop name
  MEMSET(buf, 0, buf_len);
  pos = 0;
  ops.reset();
  construct_prop_names("c1");
  ASSERT_EQ(OB_SUCCESS, ops.push_back(op));
  table_name = ObString::make_string("test");
  multi_op.op_type_ = ObTableOperationType::Type::INSERT;
  ASSERT_EQ(OB_SUCCESS, multi_op.generate_stmt(table_name, buf, buf_len, pos));
  result = "multi insert test \"c1\"";
  ASSERT_STRCASEEQ(result, buf);
  ASSERT_EQ(strlen(result), pos);
  ASSERT_EQ(multi_op.get_stmt_length(table_name), pos);
  // test5: three prop name
  MEMSET(buf, 0, buf_len);
  pos = 0;
  ops.reset();
  entity_.reset();
  construct_prop_names("c1", "c2", "c3");
  ASSERT_EQ(OB_SUCCESS, ops.push_back(op));
  table_name = ObString::make_string("test");
  multi_op.op_type_ = ObTableOperationType::Type::INSERT;
  ASSERT_EQ(OB_SUCCESS, multi_op.generate_stmt(table_name, buf, buf_len, pos));
  result = "multi insert test \"c1\", \"c2\", \"c3\"";
  ASSERT_STRCASEEQ(result, buf);
  ASSERT_EQ(strlen(result), pos);
  ASSERT_EQ(multi_op.get_stmt_length(table_name), pos);
  // test6: buf not enough
  ASSERT_EQ(OB_BUF_NOT_ENOUGH, multi_op.generate_stmt(table_name, buf, 0, pos));
  // test7: buf is null
  ASSERT_EQ(OB_INVALID_ARGUMENT, multi_op.generate_stmt(table_name, nullptr, buf_len, pos));
  // test8: bug length too short
  ASSERT_EQ(OB_BUF_NOT_ENOUGH, multi_op.generate_stmt(table_name, buf, 7, pos));
}

// 测试: ObTableAuditRedisOp
TEST_F(TestTableAudit, ObTableAuditRedisOp)
{
  const int64_t buf_len = 128;
  char buf[buf_len] = {0};
  int64_t pos = 0;
  ObString table_name = ObString::make_string("test");
  ObString cmd_name = ObString::make_string("set");
  ObTableAuditRedisOp redis_op(cmd_name);
  // test1: empty table name
  table_name = ObString::make_empty_string();
  ASSERT_EQ(OB_SUCCESS, redis_op.generate_stmt(table_name, buf, buf_len, pos));
  const char *result = "set";
  ASSERT_STRCASEEQ(result, buf);
  ASSERT_EQ(strlen(result), pos);
  ASSERT_EQ(redis_op.get_stmt_length(table_name), pos);
  // test2: empty cmd name
  MEMSET(buf, 0, buf_len);
  pos = 0;
  cmd_name = ObString::make_empty_string();
  ObTableAuditRedisOp redis_op1(cmd_name);
  ASSERT_EQ(OB_SUCCESS, redis_op.generate_stmt(table_name, buf, buf_len, pos));
  result = "(null)";
  ASSERT_STRCASEEQ(result, buf);
  ASSERT_EQ(strlen(result), pos);
  ASSERT_EQ(redis_op.get_stmt_length(table_name), pos);
  // test3: buf not enough
  ASSERT_EQ(OB_BUF_NOT_ENOUGH, redis_op.generate_stmt(table_name, buf, 0, pos));
  // test4: buf is null
  ASSERT_EQ(OB_INVALID_ARGUMENT, redis_op.generate_stmt(table_name, nullptr, buf_len, pos));
  // test5: bug length too short
  ASSERT_EQ(OB_BUF_NOT_ENOUGH, redis_op.generate_stmt(table_name, buf, 7, pos));
}

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_table_audit.log", true);
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
