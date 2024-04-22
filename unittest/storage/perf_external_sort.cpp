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
#define private public

#include "storage/blocksstable/ob_sstable.h"
#include "storage/ob_partition_scheduler.h"
#include "storage/blocksstable/ob_macro_block_writer.h"
#include "storage/ob_store_row_comparer.h"
#include "storage/ob_parallel_external_sort.h"
#include "mock_ob_partition_report.h"
#include "mockcontainer/fake_partition_utils.h"
#include "blocksstable/ob_row_generate.h"
#include "share/ob_tenant_mgr.h"
#include "lib/container/ob_array_iterator.h"
#include "lib/stat/ob_diagnose_info.h"
#include "lib/sort/ob_async_external_sorter.h"
#include "lib/alloc/alloc_func.h"
#include "./blocksstable/ob_data_file_prepare.h"

namespace oceanbase
{
using namespace blocksstable;
using namespace common;
using namespace storage;
using namespace share::schema;

namespace unittest
{

static const int64_t TEST_ROWKEY_COLUMN_CNT = 8;
static const int64_t TEST_COLUMN_CNT = ObExtendType - 1;
static const int64_t TEST_MULTI_GET_CNT = 2000;

class TestParallelSortMergeThread;

class TestParallelSortThread : public share::ObThreadPool
{
public:
  TestParallelSortThread();
  virtual ~TestParallelSortThread();
  int init(const int64_t task_id, const int64_t task_cnt, std::vector<int64_t> *total_items,
      ObIArray<int64_t> *sort_column_indexes,
      ObRowGenerate *row_generate, TestParallelSortMergeThread *merge_task, ObExternalSort<ObStoreRow, ObStoreRowComparer> *sorter, ObArenaAllocator *allocator);
  virtual void run1();
private:
  int shuffle_items(const int64_t task_id, const int64_t task_cnt,
      std::vector<int64_t> &total_items, std::vector<int64_t> &task_items);
private:
  int64_t task_id_;
  int64_t task_cnt_;
  std::vector<int64_t> *total_items_;
  ObIArray<int64_t> *sort_column_indexes_;
  ObRowGenerate *row_generate_;
  TestParallelSortMergeThread *merge_task_;
  ObExternalSort<ObStoreRow, ObStoreRowComparer> *opt_sorter_;
  ObArenaAllocator *allocator_;
};

class TestParallelSortMergeThread : public share::ObThreadPool
{
public:
  TestParallelSortMergeThread();
  virtual ~TestParallelSortMergeThread();
  int init(const int64_t task_cnt, ObIArray<int64_t> *sort_column_indexes, ObRowGenerate *row_generate);
  virtual void run1();
  int add_local_sort(ObExternalSort<ObStoreRow, ObStoreRowComparer> *local_sort);
private:
  ObArray<ObExternalSort<ObStoreRow, ObStoreRowComparer> *> sorters_;
  ObThreadCond cond_;
  int64_t task_cnt_;
  ObIArray<int64_t> *sort_column_indexes_;
  ObRowGenerate *row_generate_;
};

TestParallelSortThread::TestParallelSortThread()
  : task_id_(-1), task_cnt_(0), total_items_(NULL)
{
}

TestParallelSortThread::~TestParallelSortThread()
{
}

int TestParallelSortThread::init(const int64_t task_id, const int64_t task_cnt, std::vector<int64_t> *total_items,
    ObIArray<int64_t> *sort_column_indexes,
    ObRowGenerate *row_generate, TestParallelSortMergeThread *merge_task, ObExternalSort<ObStoreRow, ObStoreRowComparer> *sorter, ObArenaAllocator *allocator)
{
  int ret = OB_SUCCESS;
  if (task_id < 0 || task_cnt < 0 || NULL == total_items) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(task_id), K(task_cnt), KP(total_items));
  } else {
    task_id_ = task_id;
    task_cnt_ = task_cnt;
    total_items_ = total_items;
    sort_column_indexes_ = sort_column_indexes;
    row_generate_ = row_generate;
    merge_task_ = merge_task;
    opt_sorter_ = sorter;
    allocator_ = allocator;
  }
  return ret;
}

int TestParallelSortThread::shuffle_items(const int64_t task_id, const int64_t task_cnt,
  std::vector<int64_t> &total_items, std::vector<int64_t> &task_items)
{
  int ret = OB_SUCCESS;
  const int64_t total_item_cnt = total_items.size();
  int64_t start_idx = std::max(0L, total_item_cnt * task_id / task_cnt);
  int64_t end_idx = std::min(total_item_cnt - 1, total_item_cnt * (task_id + 1) / task_cnt - 1);
  for (int64_t i = start_idx; i <= end_idx; ++i) {
    task_items.push_back(total_items.at(i));
  }
  return ret;
}

void TestParallelSortThread::run(obsys::CThread *thread, void *arg)
{

  UNUSED(arg);
  int ret = OB_SUCCESS;
  std::vector<int64_t> task_items;
  ObStoreRow row;
  ObObj cells[TEST_COLUMN_CNT];
  ObSortTempMacroBlockWriter writer;
  ObSortTempMacroBlockReader reader;
  const int64_t mem_limit = 128L * 1024L * 1024L;
  const int64_t file_buf_size = 2 * 1024 * 1024L;
  const int64_t expire_timestamp = 0;
  int comp_ret = OB_SUCCESS;
  ObVersion v1(1, 0);
  ObVersion v2(2, 0);
  ObLocalIndexBuildCancel index_build_cancel(v1, v2);
  ObStoreRowComparer comparer(comp_ret, *sort_column_indexes_, &index_build_cancel);
  row.row_val_.assign(cells, TEST_COLUMN_CNT);
  ret = SLOGGER.begin(OB_LOG_CS_DAILY_MERGE);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = opt_sorter_->init(mem_limit, file_buf_size, expire_timestamp, OB_SYS_TENANT_ID, &comparer);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = shuffle_items(task_id_, task_cnt_, *total_items_, task_items);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = 0; OB_SUCC(ret) && i < task_items.size(); ++i) {
    ret = row_generate_->get_next_row(task_items.at(i), row);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = opt_sorter_->add_item(row);
    ASSERT_EQ(OB_SUCCESS, ret);
    if (i % 1000000 == 0) {
       allocator_->reuse();
    }
  }
  ret = opt_sorter_->do_sort(false);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = merge_task_->add_local_sort(opt_sorter_);
  ASSERT_EQ(OB_SUCCESS, ret);
  STORAGE_LOG(INFO, "add fragment iter", K(task_id_));
  SLOGGER.abort();
}

TestParallelSortMergeThread::TestParallelSortMergeThread()
  : sorters_(), cond_(), task_cnt_(0)
{
}

TestParallelSortMergeThread::~TestParallelSortMergeThread()
{
}

int TestParallelSortMergeThread::init(const int64_t task_cnt, ObIArray<int64_t> *sort_column_indexes, ObRowGenerate *row_generate)
{
  int ret = OB_SUCCESS;
  task_cnt_ = task_cnt;
  sort_column_indexes_ = sort_column_indexes;
  row_generate_ = row_generate;
  if (OB_FAIL(cond_.init(ObWaitEventIds::DEFAULT_COND_WAIT))) {
    STORAGE_LOG(WARN, "fail to init condition", K(ret));
  }
  return ret;
}

int TestParallelSortMergeThread::add_local_sort(
  ObExternalSort<ObStoreRow, ObStoreRowComparer> *opt_sorter_)
{
  int ret = OB_SUCCESS;
  cond_.lock();
  STORAGE_LOG(INFO, "push a task", K(sorters_.count()));
  if (OB_FAIL(sorters_.push_back(opt_sorter_))) {
    STORAGE_LOG(WARN, "fail to push back iterator", K(ret));
  } else if (sorters_.count() == task_cnt_) {
    cond_.signal();
  }
  cond_.unlock();
  return ret;
}

void TestParallelSortMergeThread::run(obsys::CThread *thread, void *arg)
{

  UNUSED(arg);
  int ret = OB_SUCCESS;
  STORAGE_LOG(INFO, "merge thread start");
  cond_.lock();
  cond_.wait();
  cond_.unlock();
  STORAGE_LOG(INFO, "merge thread working");
  ObExternalSort<ObStoreRow, ObStoreRowComparer> merge_sorter;
  ObSortTempMacroBlockWriter writer;
  ObSortTempMacroBlockReader reader;
  const int64_t mem_limit = 128L * 1024L * 1024L;
  const int64_t file_buf_size = 2 * 1024 * 1024L;
  const int64_t expire_timestamp = 0;
  int comp_ret = OB_SUCCESS;
  ObVersion v1(1, 0);
  ObVersion v2(2, 0);
  ObLocalIndexBuildCancel index_build_cancel(v1, v2);
  ObStoreRowComparer comparer(comp_ret, *sort_column_indexes_, &index_build_cancel);
  ObStoreRow row;
  const ObStoreRow *item = NULL;
  ObObj cells[TEST_COLUMN_CNT];
  row.row_val_.assign(cells, TEST_COLUMN_CNT);
  ret = SLOGGER.begin(OB_LOG_CS_DAILY_MERGE);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = merge_sorter.init(2 * mem_limit, file_buf_size, expire_timestamp, OB_SYS_TENANT_ID, &comparer);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = 0; OB_SUCC(ret) && i < sorters_.count(); ++i) {
    ret = sorters_.at(i)->transfer_final_sorted_fragment_iter(merge_sorter);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  ret = merge_sorter.do_sort(true);
  ASSERT_EQ(OB_SUCCESS, ret);
  STORAGE_LOG(INFO, "sort complete");
  while(OB_SUCC(ret)) {
    ret = merge_sorter.get_next_item(item);
    if (OB_SUCC(ret)) {
    } else {
      break;
    }
  }
  ASSERT_EQ(OB_ITER_END, ret);
  SLOGGER.abort();
  if (OB_FAIL(ObTenantManager::get_instance().print_tenant_usage())) {
    STORAGE_LOG(WARN, "fail to print tenant useage");
  }
}

class TestExternalSort : public TestDataFilePrepare
{
public:
  TestExternalSort ();
  virtual ~TestExternalSort();
  virtual void SetUp();
  virtual void TearDown();
  int generate_seed(const int64_t seed_count, std::vector<int64_t> &seeds);
  void test_parallel_sort(const int64_t task_cnt, std::vector<int64_t> &total_items);
protected:
  static const int64_t MACRO_BLOCK_SIZE = 2 * 1024 * 1024LL;
  static const int64_t MACRO_BLOCK_COUNT = 15 * 1024;
  void prepare_schema();
  void prepare_data();
  int init_tenant_mgr();
  int64_t row_cnt_;
  ObTableSchema table_schema_;
  ObRowGenerate row_generate_;
  ObArenaAllocator allocator_;
  ObArray<int64_t> sort_column_indexes_;
};

TestExternalSort::TestExternalSort()
  : TestDataFilePrepare("ExternalSortPerf", MACRO_BLOCK_SIZE, MACRO_BLOCK_COUNT),
    row_cnt_(0),
    table_schema_(),
    row_generate_(),
    allocator_(ObModIds::TEST)
{
}

TestExternalSort::~TestExternalSort()
{
}

void TestExternalSort::SetUp()
{
  system("rm -f sort_tmp*");
  TestDataFilePrepare::SetUp();
  prepare_schema();
  prepare_data();
  ASSERT_EQ(OB_SUCCESS, init_tenant_mgr());
  srand(static_cast<uint32_t>(time(NULL)));
}

void TestExternalSort::TearDown()
{
  ObTenantManager::get_instance().destroy();
  table_schema_.reset();
  row_generate_.reset();
  allocator_.reuse();
  TestDataFilePrepare::TearDown();
}

int TestExternalSort::generate_seed(const int64_t count, std::vector<int64_t> &seeds)
{
  int ret = OB_SUCCESS;
  seeds.reserve(static_cast<int32_t>(count));
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      seeds.push_back(i);
    }
    if (OB_SUCC(ret)) {
      std::random_shuffle(seeds.begin(), seeds.end());
    }
  return ret;
}

int TestExternalSort::init_tenant_mgr()
{
  int ret = OB_SUCCESS;
  ObTenantManager &tm = ObTenantManager::get_instance();
  ObAddr self;
  obrpc::ObSrvRpcProxy rpc_proxy;
  self.set_ip_addr("127.0.0.1", 8086);
  rpc::frame::ObReqTransport req_transport(NULL, NULL);
  ret = tm.init(self,
                rpc_proxy,
                &req_transport,
                &ObServerConfig::get_instance());
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = tm.add_tenant(OB_SYS_TENANT_ID);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = tm.add_tenant(OB_SERVER_TENANT_ID);
  EXPECT_EQ(OB_SUCCESS, ret);
  const int64_t ulmt = 128LL << 30;
  const int64_t llmt = 128LL << 30;
  ret = tm.set_tenant_mem_limit(OB_SYS_TENANT_ID, ulmt, llmt);
  EXPECT_EQ(OB_SUCCESS, ret);
  lib::set_memory_limit(128LL << 32);
  return ret;
}

void TestExternalSort::prepare_schema()
{
  int64_t table_id = combine_id(1, 3001);
  ObColumnSchemaV2 column;
  //init table schema
  table_schema_.reset();
  ASSERT_EQ(OB_SUCCESS, table_schema_.set_table_name("test_sstable"));
  table_schema_.set_tenant_id(1);
  table_schema_.set_tablegroup_id(1);
  table_schema_.set_database_id(1);
  table_schema_.set_table_id(table_id);
  table_schema_.set_rowkey_column_num(TEST_ROWKEY_COLUMN_CNT);
  table_schema_.set_max_used_column_id(TEST_COLUMN_CNT);
  table_schema_.set_block_size(4 * 1024);
  table_schema_.set_compress_func_name("none");
  //init column
  char name[OB_MAX_FILE_NAME_LENGTH];
  memset(name, 0, sizeof(name));
  ObObjMeta meta_type;
  for(int64_t i = 0; i < TEST_COLUMN_CNT; ++i){
    ObObjType obj_type = static_cast<ObObjType>(i + 1);
    column.reset();
    column.set_table_id(table_id);
    column.set_column_id(i + OB_APP_MIN_COLUMN_ID);
    column.set_data_length(1);
    sprintf(name, "test%020ld", i);
    ASSERT_EQ(OB_SUCCESS, column.set_column_name(name));
    meta_type.set_type(obj_type);
    column.set_meta_type(meta_type);
    if (ob_is_string_type(obj_type) && obj_type != ObHexStringType) {
      meta_type.set_collation_level(CS_LEVEL_IMPLICIT);
      meta_type.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
      column.set_meta_type(meta_type);
      ASSERT_EQ(OB_SUCCESS, sort_column_indexes_.push_back(i));
    }
    if(obj_type == common::ObVarcharType){
      column.set_rowkey_position(8);
    } else if (obj_type == common::ObCharType){
      column.set_rowkey_position(7);
    } else if (obj_type == common::ObDoubleType){
      column.set_rowkey_position(6);
    } else if (obj_type == common::ObNumberType){
      column.set_rowkey_position(5);
    } else if (obj_type == common::ObUNumberType){
      column.set_rowkey_position(4);
    } else if (obj_type == common::ObIntType){
      column.set_rowkey_position(3);
    } else if (obj_type == common::ObHexStringType){
      column.set_rowkey_position(2);
    } else if (obj_type == common::ObUInt64Type){
      column.set_rowkey_position(1);
    } else {
      column.set_rowkey_position(0);
    }
    ASSERT_EQ(OB_SUCCESS, table_schema_.add_column(column));
  }
}

void TestExternalSort::prepare_data()
{
  int ret = OB_SUCCESS;
  ObStoreRow row;
  ObObj cells[TEST_COLUMN_CNT];
  row.row_val_.assign(cells, TEST_COLUMN_CNT);

  ret = row_generate_.init(table_schema_, &allocator_);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestExternalSort, perf_external_sort)
{
  int ret = OB_SUCCESS;
  std::vector<int64_t> total_items;
  std::vector<int64_t> task_items;
  char sort_file_name[OB_MAX_FILE_NAME_LENGTH] = "sort_tmp";
  int64_t data_size = 0;
  ObStoreRow row;
  const ObStoreRow *item = NULL;
  ObObj cells[TEST_COLUMN_CNT];
  ObAsyncExternalSorter<ObStoreRow, ObStoreRowComparer> origin_sorter;
  ObSortTempMacroBlockWriter writer;
  ObSortTempMacroBlockReader reader;
  const int64_t mem_limit = 128L * 1024L * 1024L;
  const int64_t file_buf_size = 2 * 1024 * 1024L;
  const int64_t expire_timestamp = 0;
  int comp_ret = OB_SUCCESS;
  ObVersion v1(1, 0);
  ObVersion v2(2, 0);
  ObStoreRowComparer comparer(comp_ret, sort_column_indexes_);
  row.row_val_.assign(cells, TEST_COLUMN_CNT);
  ret = SLOGGER.begin(OB_LOG_CS_DAILY_MERGE);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, ObMacroFileManager::get_instance().init());
  ret = origin_sorter.init(ObString::make_string(sort_file_name),
      mem_limit,
      file_buf_size,
      expire_timestamp,
      &comparer);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = generate_seed(1000000, total_items);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = 0; OB_SUCC(ret) && i < total_items.size(); ++i) {
    ret = row_generate_.get_next_row(total_items.at(i), row);
    ASSERT_EQ(OB_SUCCESS, ret);
    data_size += row.get_serialize_size();
    ret = origin_sorter.add_item(row);
    ASSERT_EQ(OB_SUCCESS, ret);
    if (i % 1000000 == 0) {
       allocator_.reuse();
    }
  }
  ret = origin_sorter.do_sort();
  ASSERT_EQ(OB_SUCCESS, ret);
  while(OB_SUCC(ret)) {
    ret = origin_sorter.get_next_item(item);
    if (OB_SUCC(ret)) {
    } else {
      break;
    }
  }
  ASSERT_EQ(OB_ITER_END, ret);
  STORAGE_LOG(INFO, "data size", K(data_size));
  SLOGGER.abort();
  ObMacroFileManager::get_instance().destroy();
}

TEST_F(TestExternalSort, perf_parallel_sort)
{
  int ret = OB_SUCCESS;
  const int64_t task_cnt = 16;
  std::vector<int64_t> total_items;
  ObRowGenerate row_generates[task_cnt];
  ObArenaAllocator allocators[task_cnt];
  TestParallelSortThread sort_threads[task_cnt];
  TestParallelSortMergeThread merge_thread;
  ObExternalSort<ObStoreRow, ObStoreRowComparer> sorters[task_cnt];
  ret = generate_seed(10000000, total_items);
  ASSERT_EQ(OB_SUCCESS, ret);
  merge_thread.set_thread_count(1);
  ASSERT_EQ(OB_SUCCESS, ObMacroFileManager::get_instance().init());
  ret = merge_thread.init(task_cnt, &sort_column_indexes_, &row_generate_);
  ASSERT_EQ(OB_SUCCESS, ret);
  merge_thread.start();
  for (int64_t i = 0; OB_SUCC(ret) && i < task_cnt; ++i) {
    ret = row_generates[i].init(table_schema_, &allocators[i]);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = sort_threads[i].init(i, task_cnt, &total_items, &sort_column_indexes_, &row_generates[i], &merge_thread, &sorters[i], &allocators[i]);
    ASSERT_EQ(OB_SUCCESS, ret);
    sort_threads[i].set_thread_count(1);
    sort_threads[i].start();
  }
  merge_thread.wait();
  ObMacroFileManager::get_instance().destroy();
}

}  // end namespace unittest
}  // end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_external_sort_perf.log*");
  OB_LOGGER.set_file_name("test_external_sort_perf.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  CLOG_LOG(INFO, "begin unittest: test_external_sort_perf");
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
