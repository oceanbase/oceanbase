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

#include "sql/engine/sort/ob_in_memory_topn_sort.h"
#include "lib/utility/ob_tracepoint.h"
#include "lib/container/ob_se_array.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_physical_plan.h"
#include <gtest/gtest.h>
#include "ob_fake_table.h"
#include "sql/engine/ob_exec_context.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;
using oceanbase::sql::test::ObFakeTable;

class InMemoryTopnSortTest: public ::testing::Test
{
public:
  InMemoryTopnSortTest();
  virtual ~InMemoryTopnSortTest();
private:
  // disallow copy
  InMemoryTopnSortTest(const InMemoryTopnSortTest &other);
  InMemoryTopnSortTest& operator=(const InMemoryTopnSortTest &other);
protected:
  typedef ObSArray<ObSortColumn> ObSortColumns;
  void sort_test(int64_t row_count,
                 int64_t sort_col1, ObCollationType cs_type1,
                 int64_t sort_col2, ObCollationType cs_type2,
                 int64_t topn);
  void sort_exception_test(int expect_ret);
private:
  int init(ObInMemoryTopnSort &in_mem_topn_sort,
           ObFakeTable &input_table,
           int64_t row_count,
           int64_t sort_col1, ObCollationType cs_type1,
           int64_t sort_col2, ObCollationType cs_type2,
           int64_t topn);
  void cons_sort_columns(int64_t col1, ObCollationType cs_type1,
                         int64_t col2, ObCollationType cs_type2,
                         ObSortColumns &sort_columns);
  void cons_op_schema_objs(const ObIArray<ObSortColumn> &sort_columns,
                           const ObNewRow *row,
                           ObIArray<ObOpSchemaObj> &op_schema_objs);
  void copy_cell_varchar(ObObj &cell, char *buf, int64_t buf_size);
  void cons_new_row(ObNewRow &row, int64_t column_count);
};

InMemoryTopnSortTest::InMemoryTopnSortTest()
{
}

InMemoryTopnSortTest::~InMemoryTopnSortTest()
{
}

void InMemoryTopnSortTest::sort_test(int64_t row_count,
                                     int64_t sort_col1, ObCollationType cs_type1,
                                     int64_t sort_col2, ObCollationType cs_type2,
                                     int64_t topn)
{
  ObArenaAllocator alloc;
  ObInMemoryTopnSort in_mem_topn_sort;
  ObFakeTable input_table;

  ASSERT_EQ(OB_SUCCESS, init(in_mem_topn_sort, input_table, row_count,
                             sort_col1, cs_type1, sort_col2, cs_type2, topn));
  // read and check
  const ObObj *cell1 = NULL;
  const ObObj *cell2 = NULL;
  ObObj last_cell1;
  ObObj last_cell2;
  char varchar_buf[1024];
  ObMalloc allocator;
  ObNewRow row;
  cons_new_row(row, input_table.get_column_count());
  if (0 != topn) {
    ASSERT_EQ(OB_SUCCESS, in_mem_topn_sort.get_next_row(row));
  }
  last_cell1 = row.cells_[sort_col1];
  last_cell2 = row.cells_[sort_col2];
  for (int64_t i = 1; i < min(row_count, topn); ++i) {
    ASSERT_EQ(OB_SUCCESS, in_mem_topn_sort.get_next_row(row));
    cell1 = &row.cells_[sort_col1];
    cell2 = &row.cells_[sort_col2];
    // check order, cell1: descending, cell2: ascending.
    if (0 == last_cell1.compare(*cell1, cs_type1)) {
      ASSERT_TRUE(last_cell2.compare(*cell2, cs_type2) <= 0);
    } else {
      ASSERT_TRUE(last_cell1.compare(*cell1, cs_type1) > 0);
    }
    last_cell1 = *cell1;
    last_cell2 = *cell2;
    if (ObVarcharType == last_cell1.get_type()) {
      copy_cell_varchar(last_cell1, varchar_buf, 1024);
    } else if (ObVarcharType == last_cell2.get_type()) {
      copy_cell_varchar(last_cell2, varchar_buf, 1024);
    }
  } // end for
  ASSERT_EQ(OB_ITER_END, in_mem_topn_sort.get_next_row(row));
}

void InMemoryTopnSortTest::sort_exception_test(int expect_ret)
{
  int ret = OB_SUCCESS;
  ObInMemoryTopnSort in_mem_sort;
  ObFakeTable input_table;
  int64_t row_count = 1024;
  int64_t sort_col1 = 0;
  int64_t sort_col2 = 1;
  ObCollationType cs_type1 = CS_TYPE_UTF8MB4_BIN;
  ObCollationType cs_type2 = CS_TYPE_UTF8MB4_BIN;
  ObNewRow row;
  int64_t topn = 100;

  if (OB_FAIL(init(in_mem_sort, input_table, row_count,
      sort_col1, cs_type1, sort_col2, cs_type2, topn))) {
  } else {
    cons_new_row(row, input_table.get_column_count());
    for (int64_t i = 0; OB_SUCC(ret) && i < min(topn, row_count); ++i) {
      ret = in_mem_sort.get_next_row(row);
    }
    if (OB_SUCC(ret)) {
      ASSERT_EQ(OB_ITER_END, in_mem_sort.get_next_row(row));
    }
  }
  in_mem_sort.reuse();
  if (OB_FAIL(ret)) {
    ASSERT_EQ(expect_ret, ret);
  }
}

int InMemoryTopnSortTest::init(ObInMemoryTopnSort &in_mem_topn_sort,
                               ObFakeTable &input_table,
                               int64_t row_count,
                               int64_t sort_col1, ObCollationType cs_type1,
                               int64_t sort_col2, ObCollationType cs_type2,
                               int64_t topn)
{
  int ret = OB_SUCCESS;
  ObSortColumns sort_columns;
  ObExecContext exec_ctx;
  ObPhysicalPlan physical_plan;
  const ObNewRow *row = NULL;
  ObSQLSessionInfo my_session;
  my_session.test_init(0,0,0,NULL);
  exec_ctx.set_my_session(&my_session);

  cons_sort_columns(sort_col1, cs_type1, sort_col2, cs_type2, sort_columns);
  input_table.set_id(0);
  input_table.set_row_count(row_count);
  input_table.set_phy_plan(&physical_plan);
  if (OB_FAIL(exec_ctx.init_phy_op(1))) {}
  else if (OB_FAIL(exec_ctx.create_physical_plan_ctx())) {}
  else {
    exec_ctx.get_physical_plan_ctx()->set_phy_plan(&physical_plan);
  }
  in_mem_topn_sort.set_topn(topn);
  if (OB_FAIL(ret)) {}
  else if (OB_FAIL(in_mem_topn_sort.set_sort_columns(sort_columns))) {}
  else if (OB_FAIL(input_table.open(exec_ctx))) {}
  else {
    ObSEArray<ObOpSchemaObj, 8> op_schema_objs;
    for (int i = 0; OB_SUCC(ret) && i < row_count; ++i) {
      if (OB_FAIL(input_table.get_next_row(exec_ctx, row))) {}
      else if (OB_FAIL(in_mem_topn_sort.add_row(*row))) {}
    }
    cons_op_schema_objs(sort_columns, row, op_schema_objs)
    if (OB_FAIL(ret)) {}
    else if (OB_FAIL(in_mem_topn_sort.sort_rows(op_schema_objs))) {}
    else if (OB_FAIL(input_table.close(exec_ctx))) {}
  }

  return ret;
}

void InMemoryTopnSortTest::cons_sort_columns(int64_t col1, ObCollationType cs_type1,
                                           int64_t col2, ObCollationType cs_type2,
                                           ObSortColumns &sort_columns)
{
  ObSortColumn sort_column;
  sort_column.index_ = col1;
  sort_column.cs_type_ = cs_type1;
  sort_column.set_is_ascending(false);
  ASSERT_EQ(OB_SUCCESS, sort_columns.push_back(sort_column));
  sort_column.index_ = col2;
  sort_column.cs_type_ = cs_type2;
  sort_column.set_is_ascending(true);
  ASSERT_EQ(OB_SUCCESS, sort_columns.push_back(sort_column));
  return;
}

void InMemoryTopnSortTest::cons_op_schema_objs(const ObIArray<ObSortColumn> &sort_columns,
                                               const ObNewRow *row,
                                               ObIArray<ObOpSchemaObj> &op_schema_objs)
{
  if (NULL == row) {
    // do nothing
  } else {
    for (int64_t i =0; i < sort_columns.count(); i++) {
      ObOpSchemaObj op_schema_obj(row->get_cell(sort_columns.at(i).index_).get_type());
      op_schema_objs.push_back(op_schema_obj);
    }
  }
  return;
}

void InMemoryTopnSortTest::copy_cell_varchar(ObObj &cell, char *buf, int64_t buf_size)
{
  ObString str;
  ASSERT_EQ(OB_SUCCESS, cell.get_varchar(str));
  ASSERT_TRUE(str.length() < buf_size);
  memcpy(buf, str.ptr(), str.length());
  str.assign_ptr(buf, str.length());
  cell.set_varchar(str);
  return;
}

void InMemoryTopnSortTest::cons_new_row(ObNewRow &row, int64_t column_count)
{
  row.cells_ = static_cast<ObObj *>(malloc(column_count * sizeof(ObObj)));
  row.count_ = column_count;
}

TEST_F(InMemoryTopnSortTest, varchar_int_item)
{
  int64_t sort_col1 = 0;
  int64_t sort_col2 = 1;
  ObCollationType cs_type1 = CS_TYPE_UTF8MB4_BIN;
  ObCollationType cs_type2 = CS_TYPE_UTF8MB4_BIN;
  for (int64_t i = 1 ; i < 2048 ; i += 100) {
    sort_test(      10, sort_col1, cs_type1, sort_col2, cs_type2, i);
    sort_test( 16 * 10, sort_col1, cs_type1, sort_col2, cs_type2, i);
    sort_test(256 * 10, sort_col1, cs_type1, sort_col2, cs_type2, i);
    cs_type1 = CS_TYPE_UTF8MB4_GENERAL_CI;
    sort_test(      10, sort_col1, cs_type1, sort_col2, cs_type2, i);
    sort_test( 16 * 10, sort_col1, cs_type1, sort_col2, cs_type2, i);
    sort_test(256 * 10, sort_col1, cs_type1, sort_col2, cs_type2, i);
  }
}

TEST_F(InMemoryTopnSortTest, int_int_equal_item)
{
  int64_t sort_col1 = 2;
  int64_t sort_col2 = 3;
  ObCollationType cs_type1 = CS_TYPE_UTF8MB4_BIN;
  ObCollationType cs_type2 = CS_TYPE_UTF8MB4_BIN;
  for (int64_t i = 1 ; i < 2048 ; i += 100) {
    sort_test(      10, sort_col1, cs_type1, sort_col2, cs_type2, i);
    sort_test( 16 * 10, sort_col1, cs_type1, sort_col2, cs_type2, i);
    sort_test(256 * 10, sort_col1, cs_type1, sort_col2, cs_type2, i);
  }
}

TEST_F(InMemoryTopnSortTest, int_int_item)
{
  int64_t sort_col1 = 11;
  int64_t sort_col2 = 12;
  ObCollationType cs_type1 = CS_TYPE_UTF8MB4_BIN;
  ObCollationType cs_type2 = CS_TYPE_UTF8MB4_BIN;
  for (int64_t i = 1 ; i < 2048 ; i += 100) {
    sort_test(      10, sort_col1, cs_type1, sort_col2, cs_type2, i);
    sort_test( 16 * 10, sort_col1, cs_type1, sort_col2, cs_type2, i);
    sort_test(256 * 10, sort_col1, cs_type1, sort_col2, cs_type2, i);
  }
}

#define SORT_TOPN_EXCEPTION_TEST(file, func, key, err, expect_ret) \
  do { \
    TP_SET_ERROR("engine/sort/" file, func, key, err); \
    sort_exception_test(expect_ret); \
    TP_SET_ERROR("engine/sort/" file, func, key, NULL); \
  } while (0)

TEST_F(InMemoryTopnSortTest, sort_exception)
{
  SORT_TOPN_EXCEPTION_TEST("ob_in_memory_sort.cpp", "set_sort_columns", "t1", OB_ERROR, OB_ERROR);
  SORT_TOPN_EXCEPTION_TEST("ob_in_memory_sort.cpp", "add_row", "t1", OB_ERROR, OB_ERROR);
  SORT_TOPN_EXCEPTION_TEST("ob_in_memory_sort.cpp", "add_row", "t3", OB_ERROR, OB_ERROR);
  SORT_TOPN_EXCEPTION_TEST("ob_in_memory_sort.cpp", "get_next_row", "t1", OB_ERROR, OB_ERROR);
}


int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc,argv);
  oceanbase::common::ObLogger::get_logger().set_log_level("WARN");
  return RUN_ALL_TESTS();
}
