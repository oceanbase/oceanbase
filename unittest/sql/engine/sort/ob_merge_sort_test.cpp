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

#include <gtest/gtest.h>

#include "lib/allocator/ob_malloc.h"
#include "lib/utility/ob_tracepoint.h"
#include "lib/container/ob_se_array.h"
#include "share/ob_tenant_mgr.h"
#include "share/ob_srv_rpc_proxy.h"
#include "storage/blocksstable/ob_data_file_prepare.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/ob_sql_init.h"
#include "sql/engine/sort/ob_merge_sort.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/session/ob_sql_session_info.h"
#include "ob_fake_table.h"
#include "share/ob_simple_mem_limit_getter.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::storage;
using namespace oceanbase::blocksstable;
using oceanbase::sql::test::ObFakeTable;

static ObSimpleMemLimitGetter getter;
class TestMergeSort : public ObMergeSort
{
public:
  TestMergeSort(ObIAllocator &alloc) : ObMergeSort(alloc) {}
  ~TestMergeSort() {}
};

class TestMergeSortTest: public oceanbase::blocksstable::TestDataFilePrepare
{
  static const int64_t SORT_BUF_SIZE = 2 * 1024 * 1024;
  static const int64_t EXPIRE_TIMESTAMP = 0;
  static const int64_t MACRO_BLOCK_SIZE = 2 * 1024 * 1024;
  static const int64_t MACRO_BLOCK_COUNT = 50 * 1024;
  static const uint64_t TENATN_ID = 1;
public:
  TestMergeSortTest();
  virtual ~TestMergeSortTest();
  virtual void SetUp();
  virtual void TearDown();
  int init_tenant_mgr();
  void destroy_tenant_mgr();
private:
  // disallow copy
  TestMergeSortTest(const TestMergeSortTest &other);
  TestMergeSortTest& operator=(const TestMergeSortTest &other);
protected:
  typedef ObSArray<ObSortColumn> ObSortColumns;
  void sort_test(int64_t run_count, int64_t row_count,
                 int64_t sort_col1, ObCollationType cs_type1,
                 int64_t sort_col2, ObCollationType cs_type2);
  void sort_exception_test(int expect_ret);
private:
  int init_op(TestMergeSort &merge_sort,
              ObFakeTable &input_table,
              ObSortColumns &sort_cols,
              int64_t row_count,
              int64_t sort_col1, ObCollationType cs_type1,
              int64_t sort_col2, ObCollationType cs_type2);
  int init_data(TestMergeSort &merge_sort,
                ObBaseSort &in_mem_sort,
                ObFakeTable &input_table,
                ObSortColumns &sort_cols,
                int64_t run_count,
                int64_t row_count);
  void cons_sort_cols(int64_t col1, ObCollationType cs_type1,
                      int64_t col2, ObCollationType cs_type2,
                      ObSortColumns &sort_cols);
  void copy_cell_varchar(ObObj &cell, char *buf, int64_t buf_size);
  void cons_new_row(ObNewRow &row, int64_t column_count);
private:
  ObPhysicalPlan physical_plan_;
  ObArenaAllocator alloc_;
};

TestMergeSortTest::TestMergeSortTest()
  : TestDataFilePrepare(&getter, "TestMergeSort", MACRO_BLOCK_SIZE, MACRO_BLOCK_COUNT)
{
}

TestMergeSortTest::~TestMergeSortTest()
{
}

void TestMergeSortTest::SetUp()
{
  TestDataFilePrepare::SetUp();
  FILE_MANAGER_INSTANCE_V2.init();
  ASSERT_EQ(OB_SUCCESS, init_tenant_mgr());
}

void TestMergeSortTest::TearDown()
{
  alloc_.reset();
  FILE_MANAGER_INSTANCE_V2.destroy();
  TestDataFilePrepare::TearDown();
  destroy_tenant_mgr();
}

int TestMergeSortTest::init_tenant_mgr()
{
  int ret = OB_SUCCESS;
  ObAddr self;
  oceanbase::rpc::frame::ObReqTransport req_transport(NULL, NULL);
  oceanbase::obrpc::ObSrvRpcProxy rpc_proxy;
  oceanbase::obrpc::ObCommonRpcProxy rs_rpc_proxy;
  oceanbase::share::ObRsMgr rs_mgr;
  int64_t tenant_id = 1;
  self.set_ip_addr("127.0.0.1", 8086);
  ret = getter.add_tenant(tenant_id,
                          2L * 1024L * 1024L * 1024L, 4L * 1024L * 1024L * 1024L);
  EXPECT_EQ(OB_SUCCESS, ret);
  const int64_t ulmt = 128LL << 30;
  const int64_t llmt = 128LL << 30;
  ret = getter.add_tenant(OB_SERVER_TENANT_ID, ulmt, llmt);
  EXPECT_EQ(OB_SUCCESS, ret);
  oceanbase::lib::set_memory_limit(128LL << 32);
  return ret;
}

void TestMergeSortTest::destroy_tenant_mgr()
{
}

void TestMergeSortTest::sort_test(int64_t run_count, int64_t row_count,
                                int64_t sort_col1, ObCollationType cs_type1,
                                int64_t sort_col2, ObCollationType cs_type2)
{
  ObArenaAllocator alloc;
  TestMergeSort merge_sort(alloc_);
  ObBaseSort in_mem_sort;
  ObFakeTable input_table;
  ObSortColumns sort_cols;
  const ObObj *cell1 = NULL;
  const ObObj *cell2 = NULL;
  ObObj last_cell1;
  ObObj last_cell2;
  char varchar_buf[1024];
  ObNewRow row;
  // ASSERT_EQ(OB_SUCCESS, SLOGGER.begin(OB_LOG_CS_DAILY_MERGE));
  ASSERT_EQ(OB_SUCCESS, init_op(merge_sort, input_table, sort_cols, row_count,
                                sort_col1, cs_type1, sort_col2, cs_type2));
  ASSERT_EQ(OB_SUCCESS, init_data(merge_sort, in_mem_sort, input_table, sort_cols, run_count, row_count));
  ASSERT_EQ(OB_SUCCESS, merge_sort.do_merge_sort(input_table.get_column_count()));
  cons_new_row(row, input_table.get_column_count());
  ASSERT_EQ(OB_SUCCESS, merge_sort.get_next_row(row));
  last_cell1 = row.cells_[sort_col1];
  last_cell2 = row.cells_[sort_col2];
  int64_t total_row_count = run_count * row_count;
  //int ret = OB_SUCCESS;
  for (int64_t i = 1; i < total_row_count; ++i) {
    merge_sort.get_next_row(row);
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
  }
  ASSERT_EQ(OB_ITER_END, merge_sort.get_next_row(row));
  // ASSERT_EQ(OB_SUCCESS, SLOGGER.abort());
  return;
}

void TestMergeSortTest::sort_exception_test(int expect_ret)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator alloc;
  TestMergeSort merge_sort(alloc);
  ObBaseSort in_mem_sort;
  ObFakeTable input_table;
  ObSortColumns sort_cols;
  ObMalloc allocator;
  int64_t run_count = 4;
  int64_t row_count = 4096;
  int64_t sort_col1 = 0;
  int64_t sort_col2 = 1;
  ObCollationType cs_type1 = CS_TYPE_UTF8MB4_BIN;
  ObCollationType cs_type2 = CS_TYPE_UTF8MB4_BIN;

  if (OB_FAIL(init_op(merge_sort, input_table, sort_cols, row_count,
                      sort_col1, cs_type1, sort_col2, cs_type2))) {}
  else if (OB_FAIL(init_data(merge_sort, in_mem_sort, input_table, sort_cols, run_count, row_count))) {}
  else if (OB_FAIL(merge_sort.do_merge_sort(input_table.get_column_count()))) {}
  else {
    ObNewRow row;
    cons_new_row(row, input_table.get_column_count());
    int64_t total_row_count = run_count * row_count;
    for (int64_t i = 0; OB_SUCC(ret) && i < total_row_count; ++i) {
      ret = merge_sort.get_next_row(row);
    }
    if (OB_SUCC(ret)) {
      ASSERT_EQ(OB_ITER_END, merge_sort.get_next_row(row));
    }
  }
  merge_sort.reuse();
  if (OB_FAIL(ret)) {
    ASSERT_EQ(expect_ret, ret);
  }
}

int TestMergeSortTest::init_op(TestMergeSort &merge_sort,
                             ObFakeTable &input_table,
                             ObSortColumns &sort_cols,
                             int64_t row_count,
                             int64_t sort_col1, ObCollationType cs_type1,
                             int64_t sort_col2, ObCollationType cs_type2)
{
  int ret = OB_SUCCESS;
  ObString filename;
  cons_sort_cols(sort_col1, cs_type1, sort_col2, cs_type2, sort_cols);
  ObNewRow row;
  cons_new_row(row, input_table.get_column_count());
  if (OB_FAIL(merge_sort.init(sort_cols, row, TENATN_ID))) {
  } else {
    merge_sort.set_merge_run_count(2);
    input_table.set_id(0);
    input_table.set_row_count(row_count);
    input_table.set_phy_plan(&physical_plan_);
  }
  return ret;
}

int TestMergeSortTest::init_data(TestMergeSort &merge_sort,
                                 ObBaseSort &in_mem_sort,
                                 ObFakeTable &input_table,
                                 ObSortColumns &sort_cols,
                                 int64_t run_count,
                                 int64_t row_count)
{
  int ret = OB_SUCCESS;
  const ObNewRow *row = NULL;
  bool need_false = false;
  // loop run_count to generate dumped runs.
  for (int i = 0; OB_SUCC(ret) && i < run_count; ++i) {
    ObExecContext exec_ctx;
    ObSQLSessionInfo my_session;
    my_session.test_init(0,0,0,NULL);
    exec_ctx.set_my_session(&my_session);
    my_session.test_init(0,0,0,NULL);
    in_mem_sort.reuse();
    if (OB_FAIL(exec_ctx.init_phy_op(1))) {}
    else if (OB_FAIL(exec_ctx.create_physical_plan_ctx())) {}
    else if (OB_FAIL(in_mem_sort.set_sort_columns(sort_cols, 0))) {}
    else if (OB_FAIL(input_table.open(exec_ctx))) {}
    else {
      for (int j = 0; OB_SUCC(ret) && j < row_count; ++j) {
        if (OB_FAIL(input_table.get_next_row(exec_ctx, row))) {}
        else if (OB_FAIL(in_mem_sort.add_row(*row, need_false))) {}
      }
    }
    ObSEArray<ObOpSchemaObj, 8> op_schema_objs;
    if (OB_FAIL(ret)) {
      // do nothing
    } else {
    }
    if (OB_FAIL(ret)) {}
    else if (OB_FAIL(in_mem_sort.sort_rows())) {}
    else if (OB_FAIL(input_table.close(exec_ctx))) {}
    else if (OB_FAIL(merge_sort.dump_base_run(in_mem_sort))) {}
  } // end for i
  return ret;
}

void TestMergeSortTest::cons_sort_cols(int64_t col1, ObCollationType cs_type1,
                                     int64_t col2, ObCollationType cs_type2,
                                     ObSortColumns &sort_cols)
{
  ObSortColumn sort_col;
  sort_col.index_ = col1;
  sort_col.cs_type_ = cs_type1;
  sort_col.set_is_ascending(false);
  ASSERT_EQ(OB_SUCCESS, sort_cols.push_back(sort_col));
  sort_col.index_ = col2;
  sort_col.cs_type_ = cs_type2;
  sort_col.set_is_ascending(true);
  ASSERT_EQ(OB_SUCCESS, sort_cols.push_back(sort_col));
  return;
}

void TestMergeSortTest::copy_cell_varchar(ObObj &cell, char *buf, int64_t buf_size)
{
  ObString str;
  ASSERT_EQ(OB_SUCCESS, cell.get_varchar(str));
  ASSERT_TRUE(str.length() < buf_size);
  memcpy(buf, str.ptr(), str.length());
  str.assign_ptr(buf, str.length());
  cell.set_varchar(str);
  return;
}

void TestMergeSortTest::cons_new_row(ObNewRow &row, int64_t column_count)
{
  row.cells_ = static_cast<ObObj *>(malloc(column_count * sizeof(ObObj)));
  row.count_ = column_count;
}

TEST_F(TestMergeSortTest, sorted_int_int_test)
{
  int64_t sort_col1 = 1;
  int64_t sort_col2 = 2;
  ObCollationType cs_type1 = CS_TYPE_UTF8MB4_BIN;
  ObCollationType cs_type2 = CS_TYPE_UTF8MB4_BIN;
  sort_test(4, 4,    sort_col1, cs_type1, sort_col2, cs_type2);
  sort_test(4, 256,   sort_col1, cs_type1, sort_col2, cs_type2);
  sort_test(4, 4096,  sort_col1, cs_type1, sort_col2, cs_type2);
  sort_test(4, 65536, sort_col1, cs_type1, sort_col2, cs_type2);

  sort_test(16,  4, sort_col1, cs_type1, sort_col2, cs_type2);
  sort_test(256, 4, sort_col1, cs_type1, sort_col2, cs_type2);
}

TEST_F(TestMergeSortTest, random_int_int_test)
{
  int64_t sort_col1 = 12;
  int64_t sort_col2 = 13;
  ObCollationType cs_type1 = CS_TYPE_UTF8MB4_BIN;
  ObCollationType cs_type2 = CS_TYPE_UTF8MB4_BIN;
  sort_test(4, 16,    sort_col1, cs_type1, sort_col2, cs_type2);
  sort_test(4, 256,   sort_col1, cs_type1, sort_col2, cs_type2);
  sort_test(4, 4096,  sort_col1, cs_type1, sort_col2, cs_type2);
  sort_test(4, 65536, sort_col1, cs_type1, sort_col2, cs_type2);
  sort_test(16,  4, sort_col1, cs_type1, sort_col2, cs_type2);
  sort_test(256, 4, sort_col1, cs_type1, sort_col2, cs_type2);
}

TEST_F(TestMergeSortTest, random_varchar_int_test)
{
  int64_t sort_col1 = 0;
  int64_t sort_col2 = 1;
  ObCollationType cs_type1 = CS_TYPE_UTF8MB4_BIN;
  ObCollationType cs_type2 = CS_TYPE_UTF8MB4_BIN;
  sort_test(4, 16,    sort_col1, cs_type1, sort_col2, cs_type2);
  sort_test(4, 256,   sort_col1, cs_type1, sort_col2, cs_type2);
  sort_test(4, 4096,  sort_col1, cs_type1, sort_col2, cs_type2);

  sort_test(16,  4, sort_col1, cs_type1, sort_col2, cs_type2);
  sort_test(256, 4, sort_col1, cs_type1, sort_col2, cs_type2);
  sort_test(4, 65536, sort_col1, cs_type1, sort_col2, cs_type2);
  cs_type1 = CS_TYPE_UTF8MB4_GENERAL_CI;
  sort_test(4, 16,    sort_col1, cs_type1, sort_col2, cs_type2);
  sort_test(4, 256,   sort_col1, cs_type1, sort_col2, cs_type2);
  sort_test(4, 4096,  sort_col1, cs_type1, sort_col2, cs_type2);
  sort_test(16,  4, sort_col1, cs_type1, sort_col2, cs_type2);
  sort_test(256, 4, sort_col1, cs_type1, sort_col2, cs_type2);
  sort_test(4, 65536, sort_col1, cs_type1, sort_col2, cs_type2);
}

#define SORT_EXCEPTION_TEST(file, func, key, err, expect_ret) \
  do { \
    TP_SET_ERROR("engine/sort/" file, func, key, err); \
    sort_exception_test(expect_ret); \
    TP_SET_ERROR("engine/sort/" file, func, key, NULL); \
  } while (0)

TEST_F(TestMergeSortTest, sort_exception)
{
  SORT_EXCEPTION_TEST("ob_merge_sort.cpp", "init", "t1", OB_ERROR, OB_ERROR);
  SORT_EXCEPTION_TEST("ob_merge_sort.cpp", "dump_base_run", "t2", OB_ERROR, OB_ERROR);
  SORT_EXCEPTION_TEST("ob_merge_sort.cpp", "dump_base_run", "t3", OB_ERROR, OB_ERROR);
  SORT_EXCEPTION_TEST("ob_merge_sort.cpp", "build_merge_heap", "t3", OB_ERROR, OB_ERROR);
  SORT_EXCEPTION_TEST("ob_merge_sort.cpp", "build_merge_heap", "t4", OB_ERROR, OB_ERROR);
  SORT_EXCEPTION_TEST("ob_merge_sort.cpp", "dump_merge_run", "t4", OB_ERROR, OB_ERROR);
  SORT_EXCEPTION_TEST("ob_merge_sort.cpp", "dump_merge_run", "t5", OB_ERROR, OB_ERROR);
  SORT_EXCEPTION_TEST("ob_merge_sort.cpp", "do_one_round", "t5", OB_ERROR, OB_ERROR);
  SORT_EXCEPTION_TEST("ob_merge_sort.cpp", "do_one_round", "t6", OB_ERROR, OB_ERROR);
  SORT_EXCEPTION_TEST("ob_merge_sort.cpp", "do_merge_sort", "t6", OB_ERROR, OB_ERROR);
  SORT_EXCEPTION_TEST("ob_merge_sort.cpp", "get_next_row", "t7", OB_ERROR, OB_ERROR);
}

int main(int argc, char **argv)
{
  system("rm -f test_merge_sort.log*");
  OB_LOGGER.set_file_name("test_merge_sort.log", true, true);
  //OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc,argv);
  oceanbase::common::ObLogger::get_logger().set_log_level("WARN");
  return RUN_ALL_TESTS();
}
