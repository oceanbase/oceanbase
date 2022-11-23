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

#define protected public
#include "sql/engine/sort/ob_base_sort.h"
#undef protected
#include "sql/engine/sort/ob_specific_columns_sort.h"
#include "lib/utility/ob_tracepoint.h"
#include "lib/container/ob_se_array.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_physical_plan.h"
#include <gtest/gtest.h>
#include "ob_fake_table.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/ob_sql_init.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;
using oceanbase::sql::test::ObFakeTable;

class TestInMemorySpecificColumnsSortTest: public ::testing::Test
{
public:
  struct ColumnOpt
  {
    int64_t col;
    ObCollationType cs_type;
    TO_STRING_KV("col", col);
  };
  TestInMemorySpecificColumnsSortTest();
  virtual ~TestInMemorySpecificColumnsSortTest();
private:
  // disallow copy
  TestInMemorySpecificColumnsSortTest(const TestInMemorySpecificColumnsSortTest &other);
  TestInMemorySpecificColumnsSortTest& operator=(const TestInMemorySpecificColumnsSortTest &other);
protected:
  typedef ObSArray<ObSortColumn> ObSortColumns;
  void sort_test(int64_t row_count,
                 ObArray<const ColumnOpt *> &columns,
                 ObBaseSort *base_sort,
                 int64_t column_keys_pos = -1);
  void prefix_sort_test(int64_t row_count,
                        ObArray<const ColumnOpt *> &columns,
                        ObArray<const ColumnOpt *> &prefix_columns,
                        int64_t column_keys_pos);
  void serialize_test(int expect_ret);
  void sort_exception_test(int expect_ret);
private:
  int init(ObBaseSort *base_sort,
           ObFakeTable &input_table,
           int64_t row_count,
           ObArray<const ColumnOpt *> &columns,
           ObSortColumns &sort_columns);
  void cons_sort_columns(ObArray<const ColumnOpt *> &columns,
                         ObSortColumns &sort_columns);
  void copy_cell_varchar(ObObj &cell, char *buf, int64_t buf_size);
  void cons_new_row(ObNewRow &row, int64_t column_count);
};

TestInMemorySpecificColumnsSortTest::TestInMemorySpecificColumnsSortTest()
{
}

TestInMemorySpecificColumnsSortTest::~TestInMemorySpecificColumnsSortTest()
{
}

void TestInMemorySpecificColumnsSortTest::sort_test(int64_t row_count,
                                     ObArray<const ColumnOpt *> &columns,
                                     ObBaseSort *base_sort,
                                     int64_t column_keys_pos)
{
  ObArenaAllocator alloc;
  ObFakeTable input_table;
  ObSortColumns sort_columns;
  UNUSED(column_keys_pos);
  ASSERT_EQ(OB_SUCCESS, init(base_sort, input_table, row_count, columns, sort_columns));
  // read and check
  ObObj *cell[64];
  ObObj last_cell[64];
  for (int i = 0 ;i < 10 ; i++) {
    cell[i] = NULL;
  }

  char varchar_buf[1024];
  ObMalloc allocator;
  ObNewRow row;
  cons_new_row(row, input_table.get_column_count());
  ASSERT_EQ(OB_SUCCESS, base_sort->get_next_row(row));

  const ColumnOpt *opt = NULL;
  for (int64_t j = 0 ; j < columns.count(); ++j) {
    opt = columns.at(j);
    ASSERT_TRUE(opt != NULL);
    last_cell[j] = row.cells_[opt->col];
  }
  ASSERT_EQ(columns.count(), base_sort->get_sort_columns()->count());
  ObSortColumn cn;
  for (int64_t i = 1; i < row_count; ++i) {
    ASSERT_EQ(OB_SUCCESS, base_sort->get_next_row(row));
    //SQL_ENG_LOG(WARN, "yeti_test", K(row));
    // check order
    int64_t cmp = 0;
    bool cmp_next = true;
    for (int64_t j = 0 ; cmp_next && j < columns.count(); ++j) {
      opt = columns.at(j);
      cn = base_sort->get_sort_columns()->at(j);
      ASSERT_TRUE(opt != NULL);
      last_cell[j] = row.cells_[opt->col];
      cell[j] = &row.cells_[opt->col];
      cmp = last_cell[j].compare(*cell[j], opt->cs_type);
      if (cn.is_ascending()) {
        ASSERT_TRUE(cmp <= 0);
      } else {
        ASSERT_TRUE(cmp >= 0);
      }
      if (0 != cmp) {
        cmp_next = false;
      }
    }
    //set last_cell
    for (int64_t j = 0 ; j < columns.count(); ++j) {
      opt = columns.at(j);
      last_cell[j] = row.cells_[opt->col];
      if (ObVarcharType == last_cell[j].get_type()) {
        copy_cell_varchar(last_cell[j], varchar_buf, 1024);
      } else if (ObVarcharType == last_cell[j].get_type()) {
        copy_cell_varchar(last_cell[j], varchar_buf, 1024);
      }
    }
  } // end for
  ASSERT_EQ(OB_ITER_END, base_sort->get_next_row(row));
}

void TestInMemorySpecificColumnsSortTest::serialize_test(int expect_ret)
{
  int ret = OB_SUCCESS;
  ObArray<const ColumnOpt *> columns;
  ColumnOpt cpt1,cpt2;
  ObSortColumns sort_columns_1;
  ObSortColumns sort_columns_2;
  cpt1.col = 0;
  cpt2.col = 1;
  cpt1.cs_type = CS_TYPE_UTF8MB4_BIN;
  cpt2.cs_type = CS_TYPE_UTF8MB4_BIN;
  ASSERT_EQ(OB_SUCCESS, columns.push_back(&cpt1));
  ASSERT_EQ(OB_SUCCESS, columns.push_back(&cpt2));

  const int64_t MAX_SERIALIZE_BUF_LEN = 1024;
  char buf[MAX_SERIALIZE_BUF_LEN] = {'\0'};
  int64_t pos = 0;

  cons_sort_columns(columns, sort_columns_1);
  if (OB_FAIL(sort_columns_1.serialize(buf, MAX_SERIALIZE_BUF_LEN, pos))) {}
  else {
    ASSERT_EQ(pos, sort_columns_1.get_serialize_size());
    int64_t data_len = pos;
    pos = 0;
    if (OB_FAIL(sort_columns_2.deserialize(buf, data_len, pos))){}
    else {
      ASSERT_EQ(0, strcmp(to_cstring(sort_columns_1), to_cstring(sort_columns_2)));
    }
  }
  if (OB_FAIL(ret)) {
    ASSERT_EQ(expect_ret, ret);
  }
}

void TestInMemorySpecificColumnsSortTest::sort_exception_test(int expect_ret)
{
  int ret = OB_SUCCESS;
  ObSpecificColumnsSort in_mem_sort;
  ObSortColumns sort_columns;
  ObFakeTable input_table;
  int64_t row_count = 1024;
  ObArray<const ColumnOpt *> columns;
  ColumnOpt cpt1,cpt2;
  ObSortColumns sort_columns_1;
  ObSortColumns sort_columns_2;
  cpt1.col = 0;
  cpt2.col = 1;
  cpt1.cs_type = CS_TYPE_UTF8MB4_BIN;
  cpt2.cs_type = CS_TYPE_UTF8MB4_BIN;
  ASSERT_EQ(OB_SUCCESS, columns.push_back(&cpt1));
  ASSERT_EQ(OB_SUCCESS, columns.push_back(&cpt2));
  ObNewRow row;

  if (OB_FAIL(init(&in_mem_sort, input_table, row_count, columns, sort_columns))) {

  } else {
    cons_new_row(row, input_table.get_column_count());
    for (int64_t i = 0; OB_SUCC(ret) && i < row_count; ++i) {
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

int TestInMemorySpecificColumnsSortTest::init(ObBaseSort *base_sort,
                               ObFakeTable &input_table,
                               int64_t row_count,
                               ObArray<const ColumnOpt *> &columns,
                               ObSortColumns &sort_columns)
{
  int ret = OB_SUCCESS;
  ObExecContext exec_ctx;
  ObPhysicalPlan physical_plan;
  const ObNewRow *row = NULL;
  ObSQLSessionInfo my_session;
  exec_ctx.set_my_session(&my_session);

  my_session.test_init(0,0,0,NULL);
  cons_sort_columns(columns, sort_columns);
  input_table.set_id(0);
  input_table.set_row_count(row_count);
  input_table.set_phy_plan(&physical_plan);
  if (OB_FAIL(exec_ctx.init_phy_op(1))) {}
  else if (OB_FAIL(exec_ctx.create_physical_plan_ctx())) {}
  if (OB_FAIL(ret)) {}
  else if (OB_FAIL(base_sort->set_sort_columns(sort_columns, 0))) 
  {
	  SQL_ENG_LOG(WARN, "set sort columns error", K(row));
  }
  else if (OB_FAIL(input_table.open(exec_ctx))) {}
  else {
    ObSEArray<ObOpSchemaObj, 8> op_schema_objs;
    bool need_sort = false;
    for (int i = 0; OB_SUCC(ret) && i < row_count; ++i) {
      if (OB_FAIL(input_table.get_next_row(exec_ctx, row))) {}
      else if (OB_FAIL(base_sort->add_row(*row, need_sort))) {}
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else {
    }
    if (OB_FAIL(ret)) {}
    else if (OB_FAIL(base_sort->sort_rows()))
    {

    }
    else if (OB_FAIL(input_table.close(exec_ctx))) 
    {

    }
    //int64_t tmp = base_sort->get_sort_columns()->count();
    //SQL_ENG_LOG(WARN, "test mem", K(tmp));
  }

  return ret;
}

void TestInMemorySpecificColumnsSortTest::cons_sort_columns(ObArray<const ColumnOpt *> &columns,
                                           ObSortColumns &sort_columns)
{
  ObSortColumn sort_column;
  const ColumnOpt *cpt = NULL;
  for (int64_t i = 0 ; i < columns.count(); ++i) {
    cpt = columns.at(i);
    ASSERT_TRUE(cpt != NULL);
    sort_column.index_ = cpt->col;
    sort_column.cs_type_ = cpt->cs_type;
    sort_column.set_is_ascending((i%2) ? true : false);
    ASSERT_EQ(OB_SUCCESS, sort_columns.push_back(sort_column));
  }
  return;
}

void TestInMemorySpecificColumnsSortTest::copy_cell_varchar(ObObj &cell, char *buf, int64_t buf_size)
{
  ObString str;
  ASSERT_EQ(OB_SUCCESS, cell.get_varchar(str));
  ASSERT_TRUE(str.length() < buf_size);
  memcpy(buf, str.ptr(), str.length());
  str.assign_ptr(buf, str.length());
  cell.set_varchar(str);
  return;
}

void TestInMemorySpecificColumnsSortTest::cons_new_row(ObNewRow &row, int64_t column_count)
{
  row.cells_ = static_cast<ObObj *>(malloc(column_count * sizeof(ObObj)));
  row.count_ = column_count;
}

TEST_F(TestInMemorySpecificColumnsSortTest, 1M_spe_varchar_int_item)
{
  ObArray<const ColumnOpt *> columns;
  ColumnOpt cpt1;
  ColumnOpt cpt2;
  //ColumnOpt cpt3;
  cpt1.col = 11;
  cpt2.col = 12;
  //cpt1.cs_type = CS_TYPE_UTF8MB4_GENERAL_CI;
  cpt1.cs_type = CS_TYPE_UTF8MB4_BIN;
  cpt2.cs_type = CS_TYPE_UTF8MB4_BIN;
  ASSERT_EQ(OB_SUCCESS, columns.push_back(&cpt1));
  ASSERT_EQ(OB_SUCCESS, columns.push_back(&cpt2));
  //ASSERT_EQ(OB_SUCCESS, columns.push_back(&cpt3));
  ObSpecificColumnsSort in_mem_sort;
  sort_test(      1005000, columns, &in_mem_sort);
}

TEST_F(TestInMemorySpecificColumnsSortTest, 1M_base_varchar_int_item)
{
  ObArray<const ColumnOpt *> columns;
  ColumnOpt cpt1;
  ColumnOpt cpt2;
  //ColumnOpt cpt3;
  cpt1.col = 11;
  cpt2.col = 12;
  //cpt3.col = 2;
  cpt1.cs_type = CS_TYPE_UTF8MB4_BIN;
  cpt2.cs_type = CS_TYPE_UTF8MB4_BIN;
  //cpt3.cs_type = CS_TYPE_UTF8MB4_BIN;
  ASSERT_EQ(OB_SUCCESS, columns.push_back(&cpt1));
  ASSERT_EQ(OB_SUCCESS, columns.push_back(&cpt2));
  //ASSERT_EQ(OB_SUCCESS, columns.push_back(&cpt3));
  ObBaseSort in_mem_sort;
  sort_test(      1005000, columns, &in_mem_sort);
}

TEST_F(TestInMemorySpecificColumnsSortTest, spe_varchar_int_item)
{
  ObArray<const ColumnOpt *> columns;
  ColumnOpt cpt1;
  ColumnOpt cpt2;
  cpt1.col = 0;
  cpt2.col = 1;
  cpt1.cs_type = CS_TYPE_UTF8MB4_BIN;
  cpt2.cs_type = CS_TYPE_UTF8MB4_BIN;
  ASSERT_EQ(OB_SUCCESS, columns.push_back(&cpt1));
  ASSERT_EQ(OB_SUCCESS, columns.push_back(&cpt2));
  ObSpecificColumnsSort in_mem_sort;
  sort_test(      1024, columns, &in_mem_sort);
  ObSpecificColumnsSort in_mem_sort1;
  sort_test( 16 * 1024, columns, &in_mem_sort1);
  ObSpecificColumnsSort in_mem_sort2;
  sort_test(256 * 1024, columns, &in_mem_sort2);
  ObSpecificColumnsSort in_mem_sort3;
  cpt1.cs_type = CS_TYPE_UTF8MB4_GENERAL_CI;
  sort_test(      1024, columns, &in_mem_sort3);
  ObSpecificColumnsSort in_mem_sort4;
  sort_test( 16 * 1024, columns, &in_mem_sort4);
  ObSpecificColumnsSort in_mem_sort5;
  sort_test(256 * 1024, columns, &in_mem_sort5);

  //topn sort
  for (int64_t i = 1 ; i < 2048 ; i += 100) {
    ObSpecificColumnsSort mem_topn_sort;
    sort_test(      10, columns, &mem_topn_sort, i);
    mem_topn_sort.reset();
    sort_test( 16 * 10, columns, &mem_topn_sort, i);
    mem_topn_sort.reset();
    sort_test(256 * 10, columns, &mem_topn_sort, i);
    mem_topn_sort.reset();
    cpt1.cs_type = CS_TYPE_UTF8MB4_GENERAL_CI;
    sort_test(      10, columns, &mem_topn_sort, i);
    mem_topn_sort.reset();
    sort_test( 16 * 10, columns, &mem_topn_sort, i);
    mem_topn_sort.reset();
    sort_test(256 * 10, columns, &mem_topn_sort, i);
  }
}

TEST_F(TestInMemorySpecificColumnsSortTest, base_varchar_int_item)
{
  ObArray<const ColumnOpt *> columns;
  ColumnOpt cpt1;
  ColumnOpt cpt2;
  cpt1.col = 0;
  cpt2.col = 1;
  cpt1.cs_type = CS_TYPE_UTF8MB4_BIN;
  cpt2.cs_type = CS_TYPE_UTF8MB4_BIN;
  ASSERT_EQ(OB_SUCCESS, columns.push_back(&cpt1));
  ASSERT_EQ(OB_SUCCESS, columns.push_back(&cpt2));
  ObBaseSort in_mem_sort;
  sort_test(      1024, columns, &in_mem_sort);
  ObBaseSort in_mem_sort1;
  sort_test( 16 * 1024, columns, &in_mem_sort1);
  ObBaseSort in_mem_sort2;
  sort_test(256 * 1024, columns, &in_mem_sort2);
  ObBaseSort in_mem_sort3;
  cpt1.cs_type = CS_TYPE_UTF8MB4_GENERAL_CI;
  sort_test(      1024, columns, &in_mem_sort3);
  ObBaseSort in_mem_sort4;
  sort_test( 16 * 1024, columns, &in_mem_sort4);
  ObBaseSort in_mem_sort5;
  sort_test(256 * 1024, columns, &in_mem_sort5);

  //topn sort
  for (int64_t i = 1 ; i < 2048 ; i += 100) {
    ObBaseSort mem_topn_sort;
    sort_test(      10, columns, &mem_topn_sort, i);
    mem_topn_sort.reset();
    sort_test( 16 * 10, columns, &mem_topn_sort, i);
    mem_topn_sort.reset();
    sort_test(256 * 10, columns, &mem_topn_sort, i);
    mem_topn_sort.reset();
    cpt1.cs_type = CS_TYPE_UTF8MB4_GENERAL_CI;
    sort_test(      10, columns, &mem_topn_sort, i);
    mem_topn_sort.reset();
    sort_test( 16 * 10, columns, &mem_topn_sort, i);
    mem_topn_sort.reset();
    sort_test(256 * 10, columns, &mem_topn_sort, i);
  }
}

TEST_F(TestInMemorySpecificColumnsSortTest, spe_int_int_equal_item)
{
  ObArray<const ColumnOpt *> columns;
  ColumnOpt cpt1;
  ColumnOpt cpt2;
  cpt1.col = 2;
  cpt2.col = 3;
  cpt1.cs_type = CS_TYPE_UTF8MB4_BIN;
  cpt2.cs_type = CS_TYPE_UTF8MB4_BIN;
  columns.push_back(&cpt1);
  columns.push_back(&cpt2);
  ObSpecificColumnsSort in_mem_sort;
  sort_test(      1024, columns, &in_mem_sort);
  ObSpecificColumnsSort in_mem_sort1;
  sort_test( 16 * 1024, columns, &in_mem_sort1);
  ObSpecificColumnsSort in_mem_sort2;
  sort_test(256 * 1024, columns, &in_mem_sort2);

  //topn sort
  for (int64_t i = 1 ; i < 2048 ; i += 100) {
    ObSpecificColumnsSort mem_topn_sort;
    sort_test(      10, columns, &mem_topn_sort, i);
    mem_topn_sort.reset();
    sort_test( 16 * 10, columns, &mem_topn_sort, i);
    mem_topn_sort.reset();
    sort_test(256 * 10, columns, &mem_topn_sort, i);
  }
}

TEST_F(TestInMemorySpecificColumnsSortTest, base_int_int_equal_item)
{
  ObArray<const ColumnOpt *> columns;
  ColumnOpt cpt1;
  ColumnOpt cpt2;
  cpt1.col = 2;
  cpt2.col = 3;
  cpt1.cs_type = CS_TYPE_UTF8MB4_BIN;
  cpt2.cs_type = CS_TYPE_UTF8MB4_BIN;
  columns.push_back(&cpt1);
  columns.push_back(&cpt2);
  ObBaseSort in_mem_sort;
  sort_test(      1024, columns, &in_mem_sort);
  ObBaseSort in_mem_sort1;
  sort_test( 16 * 1024, columns, &in_mem_sort1);
  ObBaseSort in_mem_sort2;
  sort_test(256 * 1024, columns, &in_mem_sort2);

  // topn sort
  for (int64_t i = 1 ; i < 2048 ; i += 100) {
    ObBaseSort mem_topn_sort;
    sort_test(      10, columns, &mem_topn_sort, i);
    mem_topn_sort.reset();
    sort_test( 16 * 10, columns, &mem_topn_sort, i);
    mem_topn_sort.reset();
    sort_test(256 * 10, columns, &mem_topn_sort, i);
  }
}

TEST_F(TestInMemorySpecificColumnsSortTest, spe_int_int_item)
{
  ObArray<const ColumnOpt *> columns;
  ColumnOpt cpt1;
  ColumnOpt cpt2;
  cpt1.col = 11;
  cpt2.col = 12;
  cpt1.cs_type = CS_TYPE_UTF8MB4_BIN;
  cpt2.cs_type = CS_TYPE_UTF8MB4_BIN;
  columns.push_back(&cpt1);
  columns.push_back(&cpt2);
  ObSpecificColumnsSort in_mem_sort;
  sort_test(      1024, columns, &in_mem_sort);
  ObSpecificColumnsSort in_mem_sort1;
  sort_test( 16 * 1024, columns, &in_mem_sort1);
  ObSpecificColumnsSort in_mem_sort2;
  sort_test(256 * 1024, columns, &in_mem_sort2);

  //topn sort
  for (int64_t i = 1 ; i < 2048 ; i += 100) {
    ObSpecificColumnsSort mem_topn_sort;
    sort_test(      10, columns, &mem_topn_sort, i);
    mem_topn_sort.reset();
    sort_test( 16 * 10, columns, &mem_topn_sort, i);
    mem_topn_sort.reset();
    sort_test(256 * 10, columns, &mem_topn_sort, i);
  }
}

TEST_F(TestInMemorySpecificColumnsSortTest, base_int_int_item)
{
  ObArray<const ColumnOpt *> columns;
  ColumnOpt cpt1;
  ColumnOpt cpt2;
  cpt1.col = 11;
  cpt2.col = 12;
  cpt1.cs_type = CS_TYPE_UTF8MB4_BIN;
  cpt2.cs_type = CS_TYPE_UTF8MB4_BIN;
  columns.push_back(&cpt1);
  columns.push_back(&cpt2);
  ObBaseSort in_mem_sort;
  sort_test(      1024, columns, &in_mem_sort);
  ObBaseSort in_mem_sort1;
  sort_test( 16 * 1024, columns, &in_mem_sort1);
  ObBaseSort in_mem_sort2;
  sort_test(256 * 1024, columns, &in_mem_sort2);

  //topn sort
  for (int64_t i = 1 ; i < 2048 ; i += 100) {
    ObBaseSort mem_topn_sort;
    sort_test(      10, columns, &mem_topn_sort, i);
    mem_topn_sort.reset();
    sort_test( 16 * 10, columns, &mem_topn_sort, i);
    mem_topn_sort.reset();
    sort_test(256 * 10, columns, &mem_topn_sort, i);
  }
}


TEST_F(TestInMemorySpecificColumnsSortTest, spe_4_32_mixed_item)
{
  ObArray<const ColumnOpt *> columns;
	ColumnOpt cpt1;
	ColumnOpt cpt2;
	ColumnOpt cpt3;
	ColumnOpt cpt4;
	cpt1.col = 0;
	cpt2.col = 1;
	cpt3.col = 2;
	cpt4.col = 3;
	cpt1.cs_type = CS_TYPE_UTF8MB4_GENERAL_CI;
	cpt2.cs_type = CS_TYPE_UTF8MB4_BIN;
	cpt3.cs_type = CS_TYPE_UTF8MB4_BIN;
	cpt4.cs_type = CS_TYPE_UTF8MB4_BIN;
	columns.push_back(&cpt1);
	columns.push_back(&cpt2);
	columns.push_back(&cpt3);
	columns.push_back(&cpt4);
	ObSpecificColumnsSort in_mem_sort;
	sort_test(1024, columns, &in_mem_sort);
	ObSpecificColumnsSort in_mem_sort1;
	sort_test(16 * 1024, columns, &in_mem_sort1);
	ObSpecificColumnsSort in_mem_sort2;
	sort_test(256 * 1024, columns, &in_mem_sort2);

}

TEST_F(TestInMemorySpecificColumnsSortTest, base_4_32_mixed_item) {
	ObArray<const ColumnOpt *> columns;
	ColumnOpt cpt1;
	ColumnOpt cpt2;
	ColumnOpt cpt3;
	ColumnOpt cpt4;
	cpt1.col = 0;
	cpt2.col = 1;
	cpt3.col = 2;
	cpt4.col = 3;
	cpt1.cs_type = CS_TYPE_UTF8MB4_GENERAL_CI;
	cpt2.cs_type = CS_TYPE_UTF8MB4_BIN;
	cpt3.cs_type = CS_TYPE_UTF8MB4_BIN;
	cpt4.cs_type = CS_TYPE_UTF8MB4_BIN;
	columns.push_back(&cpt1);
	columns.push_back(&cpt2);
	columns.push_back(&cpt3);
	columns.push_back(&cpt4);
  ObBaseSort in_mem_sort;
  sort_test(      1024, columns, &in_mem_sort);
  ObBaseSort in_mem_sort1;
  sort_test( 16 * 1024, columns, &in_mem_sort1);
  ObBaseSort in_mem_sort2;
  sort_test(256 * 1024, columns, &in_mem_sort2);

}


TEST_F(TestInMemorySpecificColumnsSortTest, spe_8_32_mixed_item)
{
  ObArray<const ColumnOpt *> columns;
	ColumnOpt cpt1;
	ColumnOpt cpt2;
	ColumnOpt cpt3;
	ColumnOpt cpt4;
	ColumnOpt cpt5;
	ColumnOpt cpt6;
	ColumnOpt cpt7;
	ColumnOpt cpt8;
	cpt1.col = 0;
	cpt2.col = 1;
	cpt3.col = 2;
	cpt4.col = 3;
	cpt5.col = 4;
	cpt6.col = 5;
	cpt7.col = 6;
	cpt8.col = 7;
	cpt1.cs_type = CS_TYPE_UTF8MB4_GENERAL_CI;
	cpt2.cs_type = CS_TYPE_UTF8MB4_BIN;
	cpt3.cs_type = CS_TYPE_UTF8MB4_BIN;
	cpt4.cs_type = CS_TYPE_UTF8MB4_BIN;
	cpt5.cs_type = CS_TYPE_UTF8MB4_BIN;
	cpt6.cs_type = CS_TYPE_UTF8MB4_BIN;
	cpt7.cs_type = CS_TYPE_UTF8MB4_BIN;
	cpt8.cs_type = CS_TYPE_UTF8MB4_BIN;
	columns.push_back(&cpt1);
	columns.push_back(&cpt2);
	columns.push_back(&cpt3);
	columns.push_back(&cpt4);
	columns.push_back(&cpt5);
	columns.push_back(&cpt6);
	columns.push_back(&cpt7);
	columns.push_back(&cpt8);
	ObSpecificColumnsSort in_mem_sort;
	sort_test(1024, columns, &in_mem_sort);
	ObSpecificColumnsSort in_mem_sort1;
	sort_test(16 * 1024, columns, &in_mem_sort1);
	ObSpecificColumnsSort in_mem_sort2;
	sort_test(256 * 1024, columns, &in_mem_sort2);

}

TEST_F(TestInMemorySpecificColumnsSortTest, base_8_32_mixed_item) {
	ObArray<const ColumnOpt *> columns;
	ColumnOpt cpt1;
	ColumnOpt cpt2;
	ColumnOpt cpt3;
	ColumnOpt cpt4;
	ColumnOpt cpt5;
	ColumnOpt cpt6;
	ColumnOpt cpt7;
	ColumnOpt cpt8;
	cpt1.col = 0;
	cpt2.col = 1;
	cpt3.col = 2;
	cpt4.col = 3;
	cpt5.col = 4;
	cpt6.col = 5;
	cpt7.col = 6;
	cpt8.col = 7;
	cpt1.cs_type = CS_TYPE_UTF8MB4_GENERAL_CI;
	cpt2.cs_type = CS_TYPE_UTF8MB4_BIN;
	cpt3.cs_type = CS_TYPE_UTF8MB4_BIN;
	cpt4.cs_type = CS_TYPE_UTF8MB4_BIN;
	cpt5.cs_type = CS_TYPE_UTF8MB4_BIN;
	cpt6.cs_type = CS_TYPE_UTF8MB4_BIN;
	cpt7.cs_type = CS_TYPE_UTF8MB4_BIN;
	cpt8.cs_type = CS_TYPE_UTF8MB4_BIN;
	columns.push_back(&cpt1);
	columns.push_back(&cpt2);
	columns.push_back(&cpt3);
	columns.push_back(&cpt4);
	columns.push_back(&cpt5);
	columns.push_back(&cpt6);
	columns.push_back(&cpt7);
	columns.push_back(&cpt8);
  ObBaseSort in_mem_sort;
  sort_test(      1024, columns, &in_mem_sort);
  ObBaseSort in_mem_sort1;
  sort_test( 16 * 1024, columns, &in_mem_sort1);
  ObBaseSort in_mem_sort2;
  sort_test(256 * 1024, columns, &in_mem_sort2);

}


TEST_F(TestInMemorySpecificColumnsSortTest, spe_16_32_mixed_item)
{
  ObArray<const ColumnOpt *> columns;
	ColumnOpt cpt1;
	ColumnOpt cpt2;
	ColumnOpt cpt3;
	ColumnOpt cpt4;
	ColumnOpt cpt5;
	ColumnOpt cpt6;
	ColumnOpt cpt7;
	ColumnOpt cpt8;
	ColumnOpt cpt9;
	ColumnOpt cpt10;
	ColumnOpt cpt11;
	ColumnOpt cpt12;
	ColumnOpt cpt13;
	ColumnOpt cpt14;
	ColumnOpt cpt15;
	ColumnOpt cpt16;
	cpt1.col = 0;
	cpt2.col = 1;
	cpt3.col = 2;
	cpt4.col = 3;
	cpt5.col = 4;
	cpt6.col = 5;
	cpt7.col = 6;
	cpt8.col = 7;
	cpt9.col = 8;
	cpt10.col = 9;
	cpt11.col = 10;
	cpt12.col = 11;
	cpt13.col = 12;
	cpt14.col = 13;
	cpt15.col = 14;
	cpt16.col = 15;
	cpt1.cs_type = CS_TYPE_UTF8MB4_GENERAL_CI;
	cpt2.cs_type = CS_TYPE_UTF8MB4_BIN;
	cpt3.cs_type = CS_TYPE_UTF8MB4_BIN;
	cpt4.cs_type = CS_TYPE_UTF8MB4_BIN;
	cpt5.cs_type = CS_TYPE_UTF8MB4_BIN;
	cpt6.cs_type = CS_TYPE_UTF8MB4_BIN;
	cpt7.cs_type = CS_TYPE_UTF8MB4_BIN;
	cpt8.cs_type = CS_TYPE_UTF8MB4_BIN;
	cpt9.cs_type = CS_TYPE_UTF8MB4_BIN;
	cpt10.cs_type = CS_TYPE_UTF8MB4_BIN;
	cpt11.cs_type = CS_TYPE_UTF8MB4_BIN;
	cpt12.cs_type = CS_TYPE_UTF8MB4_BIN;
	cpt13.cs_type = CS_TYPE_UTF8MB4_BIN;
	cpt14.cs_type = CS_TYPE_UTF8MB4_BIN;
	cpt15.cs_type = CS_TYPE_UTF8MB4_BIN;
	cpt16.cs_type = CS_TYPE_UTF8MB4_BIN;
	columns.push_back(&cpt1);
	columns.push_back(&cpt2);
	columns.push_back(&cpt3);
	columns.push_back(&cpt4);
	columns.push_back(&cpt5);
	columns.push_back(&cpt6);
	columns.push_back(&cpt7);
	columns.push_back(&cpt8);
	columns.push_back(&cpt9);
	columns.push_back(&cpt10);
	columns.push_back(&cpt11);
	columns.push_back(&cpt12);
	columns.push_back(&cpt13);
	columns.push_back(&cpt14);
	columns.push_back(&cpt15);
	columns.push_back(&cpt16);
	ObSpecificColumnsSort in_mem_sort;
	sort_test(1024, columns, &in_mem_sort);
	ObSpecificColumnsSort in_mem_sort1;
	sort_test(16 * 1024, columns, &in_mem_sort1);
	ObSpecificColumnsSort in_mem_sort2;
	sort_test(256 * 1024, columns, &in_mem_sort2);

}

TEST_F(TestInMemorySpecificColumnsSortTest, base_16_32_mixed_item) {
	ObArray<const ColumnOpt *> columns;
	ColumnOpt cpt1;
	ColumnOpt cpt2;
	ColumnOpt cpt3;
	ColumnOpt cpt4;
	ColumnOpt cpt5;
	ColumnOpt cpt6;
	ColumnOpt cpt7;
	ColumnOpt cpt8;
	ColumnOpt cpt9;
	ColumnOpt cpt10;
	ColumnOpt cpt11;
	ColumnOpt cpt12;
	ColumnOpt cpt13;
	ColumnOpt cpt14;
	ColumnOpt cpt15;
	ColumnOpt cpt16;
	cpt1.col = 0;
	cpt2.col = 1;
	cpt3.col = 2;
	cpt4.col = 3;
	cpt5.col = 4;
	cpt6.col = 5;
	cpt7.col = 6;
	cpt8.col = 7;
	cpt9.col = 8;
	cpt10.col = 9;
	cpt11.col = 10;
	cpt12.col = 11;
	cpt13.col = 12;
	cpt14.col = 13;
	cpt15.col = 14;
	cpt16.col = 15;
	cpt1.cs_type = CS_TYPE_UTF8MB4_GENERAL_CI;
	cpt2.cs_type = CS_TYPE_UTF8MB4_BIN;
	cpt3.cs_type = CS_TYPE_UTF8MB4_BIN;
	cpt4.cs_type = CS_TYPE_UTF8MB4_BIN;
	cpt5.cs_type = CS_TYPE_UTF8MB4_BIN;
	cpt6.cs_type = CS_TYPE_UTF8MB4_BIN;
	cpt7.cs_type = CS_TYPE_UTF8MB4_BIN;
	cpt8.cs_type = CS_TYPE_UTF8MB4_BIN;
	cpt9.cs_type = CS_TYPE_UTF8MB4_BIN;
	cpt10.cs_type = CS_TYPE_UTF8MB4_BIN;
	cpt11.cs_type = CS_TYPE_UTF8MB4_BIN;
	cpt12.cs_type = CS_TYPE_UTF8MB4_BIN;
	cpt13.cs_type = CS_TYPE_UTF8MB4_BIN;
	cpt14.cs_type = CS_TYPE_UTF8MB4_BIN;
	cpt15.cs_type = CS_TYPE_UTF8MB4_BIN;
	cpt16.cs_type = CS_TYPE_UTF8MB4_BIN;
	columns.push_back(&cpt1);
	columns.push_back(&cpt2);
	columns.push_back(&cpt3);
	columns.push_back(&cpt4);
	columns.push_back(&cpt5);
	columns.push_back(&cpt6);
	columns.push_back(&cpt7);
	columns.push_back(&cpt8);
	columns.push_back(&cpt9);
	columns.push_back(&cpt10);
	columns.push_back(&cpt11);
	columns.push_back(&cpt12);
	columns.push_back(&cpt13);
	columns.push_back(&cpt14);
	columns.push_back(&cpt15);
	columns.push_back(&cpt16);
  ObBaseSort in_mem_sort;
  sort_test(      1024, columns, &in_mem_sort);
  ObBaseSort in_mem_sort1;
  sort_test( 16 * 1024, columns, &in_mem_sort1);
  ObBaseSort in_mem_sort2;
  sort_test(256 * 1024, columns, &in_mem_sort2);

}


TEST_F(TestInMemorySpecificColumnsSortTest, spe_32_32_mixed_item)
{
  ObArray<const ColumnOpt *> columns;
  ColumnOpt cpt1;
  	ColumnOpt cpt2;
  	ColumnOpt cpt3;
  	ColumnOpt cpt4;
  	ColumnOpt cpt5;
  	ColumnOpt cpt6;
  	ColumnOpt cpt7;
  	ColumnOpt cpt8;
  	ColumnOpt cpt9;
  	ColumnOpt cpt10;
  	ColumnOpt cpt11;
  	ColumnOpt cpt12;
  	ColumnOpt cpt13;
  	ColumnOpt cpt14;
  	ColumnOpt cpt15;
  	ColumnOpt cpt16;
  	ColumnOpt cpt17;
  	ColumnOpt cpt18;
  	ColumnOpt cpt19;
  	ColumnOpt cpt20;
  	ColumnOpt cpt21;
  	ColumnOpt cpt22;
  	ColumnOpt cpt23;
  	ColumnOpt cpt24;
  	ColumnOpt cpt25;
  	ColumnOpt cpt26;
  	ColumnOpt cpt27;
  	ColumnOpt cpt28;
  	ColumnOpt cpt29;
  	ColumnOpt cpt30;
  	ColumnOpt cpt31;
  	ColumnOpt cpt32;
  	cpt1.col = 0;
  	cpt2.col = 1;
  	cpt3.col = 2;
  	cpt4.col = 3;
  	cpt5.col = 4;
  	cpt6.col = 5;
  	cpt7.col = 6;
  	cpt8.col = 7;
  	cpt9.col = 8;
  	cpt10.col = 9;
  	cpt11.col = 10;
  	cpt12.col = 11;
  	cpt13.col = 12;
  	cpt14.col = 13;
  	cpt15.col = 14;
  	cpt16.col = 15;
  	cpt17.col = 16;
  	cpt18.col = 17;
  	cpt19.col = 18;
  	cpt20.col = 19;
  	cpt21.col = 20;
  	cpt22.col = 21;
  	cpt23.col = 22;
  	cpt24.col = 23;
  	cpt25.col = 24;
  	cpt26.col = 25;
  	cpt27.col = 26;
  	cpt28.col = 27;
  	cpt29.col = 28;
  	cpt30.col = 29;
  	cpt31.col = 30;
  	cpt32.col = 31;
  	cpt1.cs_type = CS_TYPE_UTF8MB4_GENERAL_CI;
  	cpt2.cs_type = CS_TYPE_UTF8MB4_BIN;
  	cpt3.cs_type = CS_TYPE_UTF8MB4_BIN;
  	cpt4.cs_type = CS_TYPE_UTF8MB4_BIN;
  	cpt5.cs_type = CS_TYPE_UTF8MB4_BIN;
  	cpt6.cs_type = CS_TYPE_UTF8MB4_BIN;
  	cpt7.cs_type = CS_TYPE_UTF8MB4_BIN;
  	cpt8.cs_type = CS_TYPE_UTF8MB4_BIN;
  	cpt9.cs_type = CS_TYPE_UTF8MB4_BIN;
  	cpt10.cs_type = CS_TYPE_UTF8MB4_BIN;
  	cpt11.cs_type = CS_TYPE_UTF8MB4_BIN;
  	cpt12.cs_type = CS_TYPE_UTF8MB4_BIN;
  	cpt13.cs_type = CS_TYPE_UTF8MB4_BIN;
  	cpt14.cs_type = CS_TYPE_UTF8MB4_BIN;
  	cpt15.cs_type = CS_TYPE_UTF8MB4_BIN;
  	cpt16.cs_type = CS_TYPE_UTF8MB4_BIN;
  	cpt17.cs_type = CS_TYPE_UTF8MB4_BIN;
  	cpt18.cs_type = CS_TYPE_UTF8MB4_BIN;
  	cpt18.cs_type = CS_TYPE_UTF8MB4_BIN;
  	cpt19.cs_type = CS_TYPE_UTF8MB4_BIN;
  	cpt20.cs_type = CS_TYPE_UTF8MB4_BIN;
  	cpt21.cs_type = CS_TYPE_UTF8MB4_BIN;
  	cpt22.cs_type = CS_TYPE_UTF8MB4_BIN;
  	cpt23.cs_type = CS_TYPE_UTF8MB4_BIN;
  	cpt24.cs_type = CS_TYPE_UTF8MB4_BIN;
  	cpt25.cs_type = CS_TYPE_UTF8MB4_BIN;
  	cpt26.cs_type = CS_TYPE_UTF8MB4_BIN;
  	cpt27.cs_type = CS_TYPE_UTF8MB4_BIN;
  	cpt28.cs_type = CS_TYPE_UTF8MB4_BIN;
  	cpt29.cs_type = CS_TYPE_UTF8MB4_BIN;
  	cpt30.cs_type = CS_TYPE_UTF8MB4_BIN;
  	cpt31.cs_type = CS_TYPE_UTF8MB4_BIN;
  	cpt32.cs_type = CS_TYPE_UTF8MB4_BIN;
  	columns.push_back(&cpt1);
  	columns.push_back(&cpt2);
  	columns.push_back(&cpt3);
  	columns.push_back(&cpt4);
  	columns.push_back(&cpt5);
  	columns.push_back(&cpt6);
  	columns.push_back(&cpt7);
  	columns.push_back(&cpt8);
  	columns.push_back(&cpt9);
  	columns.push_back(&cpt10);
  	columns.push_back(&cpt11);
  	columns.push_back(&cpt12);
  	columns.push_back(&cpt13);
  	columns.push_back(&cpt14);
  	columns.push_back(&cpt15);
  	columns.push_back(&cpt16);
  	columns.push_back(&cpt17);
  	columns.push_back(&cpt18);
  	columns.push_back(&cpt19);
  	columns.push_back(&cpt20);
  	columns.push_back(&cpt21);
  	columns.push_back(&cpt22);
  	columns.push_back(&cpt23);
  	columns.push_back(&cpt24);
  	columns.push_back(&cpt25);
  	columns.push_back(&cpt26);
  	columns.push_back(&cpt27);
  	columns.push_back(&cpt28);
  	columns.push_back(&cpt29);
  	columns.push_back(&cpt30);
  	columns.push_back(&cpt31);
  	columns.push_back(&cpt32);
	ObSpecificColumnsSort in_mem_sort;
	sort_test(1024, columns, &in_mem_sort);
	ObSpecificColumnsSort in_mem_sort1;
	sort_test(16 * 1024, columns, &in_mem_sort1);
	ObSpecificColumnsSort in_mem_sort2;
	sort_test(256 * 1024, columns, &in_mem_sort2);

}

TEST_F(TestInMemorySpecificColumnsSortTest, base_32_32_mixed_item) {
	ObArray<const ColumnOpt *> columns;
	ColumnOpt cpt1;
	ColumnOpt cpt2;
	ColumnOpt cpt3;
	ColumnOpt cpt4;
	ColumnOpt cpt5;
	ColumnOpt cpt6;
	ColumnOpt cpt7;
	ColumnOpt cpt8;
	ColumnOpt cpt9;
	ColumnOpt cpt10;
	ColumnOpt cpt11;
	ColumnOpt cpt12;
	ColumnOpt cpt13;
	ColumnOpt cpt14;
	ColumnOpt cpt15;
	ColumnOpt cpt16;
	ColumnOpt cpt17;
	ColumnOpt cpt18;
	ColumnOpt cpt19;
	ColumnOpt cpt20;
	ColumnOpt cpt21;
	ColumnOpt cpt22;
	ColumnOpt cpt23;
	ColumnOpt cpt24;
	ColumnOpt cpt25;
	ColumnOpt cpt26;
	ColumnOpt cpt27;
	ColumnOpt cpt28;
	ColumnOpt cpt29;
	ColumnOpt cpt30;
	ColumnOpt cpt31;
	ColumnOpt cpt32;
	cpt1.col = 0;
	cpt2.col = 1;
	cpt3.col = 2;
	cpt4.col = 3;
	cpt5.col = 4;
	cpt6.col = 5;
	cpt7.col = 6;
	cpt8.col = 7;
	cpt9.col = 8;
	cpt10.col = 9;
	cpt11.col = 10;
	cpt12.col = 11;
	cpt13.col = 12;
	cpt14.col = 13;
	cpt15.col = 14;
	cpt16.col = 15;
	cpt17.col = 16;
	cpt18.col = 17;
	cpt19.col = 18;
	cpt20.col = 19;
	cpt21.col = 20;
	cpt22.col = 21;
	cpt23.col = 22;
	cpt24.col = 23;
	cpt25.col = 24;
	cpt26.col = 25;
	cpt27.col = 26;
	cpt28.col = 27;
	cpt29.col = 28;
	cpt30.col = 29;
	cpt31.col = 30;
	cpt32.col = 31;
	cpt1.cs_type = CS_TYPE_UTF8MB4_GENERAL_CI;
	cpt2.cs_type = CS_TYPE_UTF8MB4_BIN;
	cpt3.cs_type = CS_TYPE_UTF8MB4_BIN;
	cpt4.cs_type = CS_TYPE_UTF8MB4_BIN;
	cpt5.cs_type = CS_TYPE_UTF8MB4_BIN;
	cpt6.cs_type = CS_TYPE_UTF8MB4_BIN;
	cpt7.cs_type = CS_TYPE_UTF8MB4_BIN;
	cpt8.cs_type = CS_TYPE_UTF8MB4_BIN;
	cpt9.cs_type = CS_TYPE_UTF8MB4_BIN;
	cpt10.cs_type = CS_TYPE_UTF8MB4_BIN;
	cpt11.cs_type = CS_TYPE_UTF8MB4_BIN;
	cpt12.cs_type = CS_TYPE_UTF8MB4_BIN;
	cpt13.cs_type = CS_TYPE_UTF8MB4_BIN;
	cpt14.cs_type = CS_TYPE_UTF8MB4_BIN;
	cpt15.cs_type = CS_TYPE_UTF8MB4_BIN;
	cpt16.cs_type = CS_TYPE_UTF8MB4_BIN;
	cpt17.cs_type = CS_TYPE_UTF8MB4_BIN;
	cpt18.cs_type = CS_TYPE_UTF8MB4_BIN;
	cpt18.cs_type = CS_TYPE_UTF8MB4_BIN;
	cpt19.cs_type = CS_TYPE_UTF8MB4_BIN;
	cpt20.cs_type = CS_TYPE_UTF8MB4_BIN;
	cpt21.cs_type = CS_TYPE_UTF8MB4_BIN;
	cpt22.cs_type = CS_TYPE_UTF8MB4_BIN;
	cpt23.cs_type = CS_TYPE_UTF8MB4_BIN;
	cpt24.cs_type = CS_TYPE_UTF8MB4_BIN;
	cpt25.cs_type = CS_TYPE_UTF8MB4_BIN;
	cpt26.cs_type = CS_TYPE_UTF8MB4_BIN;
	cpt27.cs_type = CS_TYPE_UTF8MB4_BIN;
	cpt28.cs_type = CS_TYPE_UTF8MB4_BIN;
	cpt29.cs_type = CS_TYPE_UTF8MB4_BIN;
	cpt30.cs_type = CS_TYPE_UTF8MB4_BIN;
	cpt31.cs_type = CS_TYPE_UTF8MB4_BIN;
	cpt32.cs_type = CS_TYPE_UTF8MB4_BIN;
	columns.push_back(&cpt1);
	columns.push_back(&cpt2);
	columns.push_back(&cpt3);
	columns.push_back(&cpt4);
	columns.push_back(&cpt5);
	columns.push_back(&cpt6);
	columns.push_back(&cpt7);
	columns.push_back(&cpt8);
	columns.push_back(&cpt9);
	columns.push_back(&cpt10);
	columns.push_back(&cpt11);
	columns.push_back(&cpt12);
	columns.push_back(&cpt13);
	columns.push_back(&cpt14);
	columns.push_back(&cpt15);
	columns.push_back(&cpt16);
	columns.push_back(&cpt17);
	columns.push_back(&cpt18);
	columns.push_back(&cpt19);
	columns.push_back(&cpt20);
	columns.push_back(&cpt21);
	columns.push_back(&cpt22);
	columns.push_back(&cpt23);
	columns.push_back(&cpt24);
	columns.push_back(&cpt25);
	columns.push_back(&cpt26);
	columns.push_back(&cpt27);
	columns.push_back(&cpt28);
	columns.push_back(&cpt29);
	columns.push_back(&cpt30);
	columns.push_back(&cpt31);
	columns.push_back(&cpt32);
  ObBaseSort in_mem_sort;
  sort_test(      1024, columns, &in_mem_sort);
  ObBaseSort in_mem_sort1;
  sort_test( 16 * 1024, columns, &in_mem_sort1);
  ObBaseSort in_mem_sort2;
  sort_test(256 * 1024, columns, &in_mem_sort2);

}


/*
TEST_F(TestInMemorySpecificColumnsSortTest, prefix_int_test)
{
  ObArray<const ColumnOpt *> columns;
  ObArray<const ColumnOpt *> prefix_columns;
  ColumnOpt cpt1;
  ColumnOpt cpt2;
  ColumnOpt cpt3;
  ColumnOpt cpt4;
  cpt1.col = 9;
  cpt2.col = 10;
  cpt3.col = 11;
  cpt4.col = 12;

  cpt1.cs_type = CS_TYPE_UTF8MB4_BIN;
  cpt2.cs_type = CS_TYPE_UTF8MB4_BIN;
  cpt3.cs_type = CS_TYPE_UTF8MB4_BIN;
  cpt4.cs_type = CS_TYPE_UTF8MB4_BIN;

  columns.push_back(&cpt1);
  columns.push_back(&cpt2);

  prefix_columns.push_back(&cpt1);
  prefix_columns.push_back(&cpt2);
  prefix_columns.push_back(&cpt3);
  prefix_columns.push_back(&cpt4);

  ObSpecificColumnsSort in_mem_sort;
  prefix_sort_test(      1024, columns, prefix_columns, 2);
  ObSpecificColumnsSort in_mem_sort1;
  prefix_sort_test( 16 * 1024, columns, prefix_columns, 2);
  ObSpecificColumnsSort in_mem_sort2;
  prefix_sort_test(256 * 1024, columns, prefix_columns, 2);
}

TEST_F(TestInMemorySpecificColumnsSortTest, prefix_var_int_test)
{
  ObArray<const ColumnOpt *> columns;
  ObArray<const ColumnOpt *> prefix_columns;
  ColumnOpt cpt1;
  ColumnOpt cpt2;
  ColumnOpt cpt3;
  cpt1.col = 9;
  cpt2.col = 10;
  cpt3.col = 0;

  cpt1.cs_type = CS_TYPE_UTF8MB4_BIN;
  cpt2.cs_type = CS_TYPE_UTF8MB4_BIN;
  cpt3.cs_type = CS_TYPE_UTF8MB4_BIN;

  columns.push_back(&cpt1);
  columns.push_back(&cpt2);

  prefix_columns.push_back(&cpt1);
  prefix_columns.push_back(&cpt2);
  prefix_columns.push_back(&cpt3);

  ObSpecificColumnsSort in_mem_sort;
  prefix_sort_test(      1024, columns, prefix_columns, 2);
  ObSpecificColumnsSort in_mem_sort1;
  prefix_sort_test( 16 * 1024, columns, prefix_columns, 2);
  ObSpecificColumnsSort in_mem_sort2;
  prefix_sort_test(256 * 1024, columns, prefix_columns, 2);
}*/

TEST_F(TestInMemorySpecificColumnsSortTest, serialize)
{
  serialize_test(OB_SUCCESS);
}

#define SORT_EXCEPTION_TEST(file, func, key, err, expect_ret) \
  do { \
    TP_SET_ERROR("engine/sort/" file, func, key, err); \
    sort_exception_test(expect_ret); \
    TP_SET_ERROR("engine/sort/" file, func, key, NULL); \
  } while (0)

TEST_F(TestInMemorySpecificColumnsSortTest, sort_exception)
{
  SORT_EXCEPTION_TEST("ob_base_sort.cpp", "set_sort_columns", "t1", OB_ERROR, OB_ERROR);
  SORT_EXCEPTION_TEST("ob_base_sort.cpp", "add_row", "t1", OB_ERROR, OB_ERROR);
  SORT_EXCEPTION_TEST("ob_base_sort.cpp", "add_row", "t3", OB_ERROR, OB_ERROR);
  SORT_EXCEPTION_TEST("ob_base_sort.cpp", "get_next_row", "t1", OB_ERROR, OB_ERROR);
}

#define SERIALIZE_EXCEPTION_TEST(file, func, key, err, expect_ret) \
  do { \
    TP_SET_ERROR("engine/sort/" file, func, key, err); \
    serialize_test(expect_ret); \
    TP_SET_ERROR("engine/sort/" file, func, key, NULL); \
  } while (0)

TEST_F(TestInMemorySpecificColumnsSortTest, serialize_exception)
{
  SERIALIZE_EXCEPTION_TEST("ob_base_sort.cpp", "serialize", "t1", OB_ERROR, OB_ERROR);
  SERIALIZE_EXCEPTION_TEST("ob_base_sort.cpp", "serialize", "t3", OB_ERROR, OB_ERROR);
  SERIALIZE_EXCEPTION_TEST("ob_base_sort.cpp", "deserialize", "t1", OB_ERROR, OB_ERROR);
  SERIALIZE_EXCEPTION_TEST("ob_base_sort.cpp", "deserialize", "t3", OB_ERROR, OB_ERROR);
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc,argv);
  oceanbase::common::ObLogger::get_logger().set_log_level("WARN");
  return RUN_ALL_TESTS();
}
