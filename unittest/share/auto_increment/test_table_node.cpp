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

#define USING_LOG_PREFIX SHARE
#include <gtest/gtest.h>
#include "share/ob_define.h"
#include "lib/net/ob_addr.h"
#include "lib/time/ob_time_utility.h"
#include "lib/hash/ob_hashset.h"
#include "share/ob_autoincrement_service.h"

using namespace oceanbase::share;
using namespace oceanbase::common;
using namespace oceanbase::common::hash;

namespace test
{
static const int64_t MAX_THREAD_COUNT = 10;
static const int64_t BUCKET_NUM = 1 << 10;
static const int64_t TEST_COLUMN_ID = 1;
static const int64_t TABLE_ID_START = 3001;
static const int64_t TABLE_COUNT = 20;

static ObSmallAllocator param_allocator;
static ObAutoincrementService &service = ObAutoincrementService::get_instance();;
static ObHashSet<int64_t> value_set; // for one table
static ObHashSet<int64_t> value_sets[TABLE_COUNT]; // for multi table
static ObHashSet<int64_t> pset; // for allocator test
static AutoincParam param;
static pthread_barrier_t barrier;

void init_table_node(TableNode *&table_node)
{
  table_node = static_cast<TableNode *>(service.node_allocator_.alloc());
  table_node = new (table_node) TableNode;
  table_node->next_value_ = 1;
  table_node->max_value_ = UINT64_MAX;
  table_node->local_sync_ = 0;
  table_node->last_refresh_ts_ = ObTimeUtility::current_time();
  table_node->curr_node_.cache_start_ = 1;
  table_node->curr_node_.cache_end_ = 1000000;
}

void init()
{
  param_allocator.init(sizeof(CacheHandle), ObModIds::TEST);

  ASSERT_TRUE(OB_SUCCESS == value_set.create(BUCKET_NUM));
  ASSERT_TRUE(OB_SUCCESS == pset.create(BUCKET_NUM));

  for (int64_t i = 0; i < TABLE_COUNT; ++i) {
    //void *ptr = service.allocator_.alloc(sizeof(TableNode));
    //value_sets[i] = new (ptr) ObHashSet<int64_t>;
    ASSERT_TRUE(OB_SUCCESS == value_sets[i].create(BUCKET_NUM));
  }

  ObAddr addr(ObAddr::IPV4, "1.1.1.1", 8888);
  service.init(addr, NULL, NULL, NULL, NULL);
  AutoincKey key;
  key.tenant_id_ = OB_SYS_TENANT_ID;
  key.column_id_ = TEST_COLUMN_ID;

  TableNode *table_node = NULL;

  for (int64_t i = 0; i < TABLE_COUNT; ++i) {
    key.table_id_ = TABLE_ID_START + i;

    init_table_node(table_node);
    table_node->table_id_ = TABLE_ID_START + i;
    ASSERT_EQ(OB_HASH_INSERT_SUCC, service.node_map_.set(key, table_node));
  }

  param.tenant_id_ = OB_SYS_TENANT_ID;
  param.autoinc_col_id_ = TEST_COLUMN_ID;
  param.autoinc_col_type_ = ObUInt64Type;
  param.autoinc_table_part_num_ = 1;
  param.autoinc_increment_ = 1;
  param.autoinc_offset_ = 1;
  param.total_value_count_ = 1;
  param.autoinc_desired_count_ = 0;
  param.curr_value_count_ = 0;
  param.sync_flag_ = false;
  param.value_to_sync_ = 0;
}

class TestTableNode: public ::testing::Test
{
public:
  TestTableNode() {}
  virtual ~TestTableNode() {}
  virtual void SetUp();
  virtual void TearDown() {}
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestTableNode);
protected:
  // function members
protected:
  // data members
};

void TestTableNode::SetUp()
{
}

class TestRunner : public share::ObThreadPool
{
public:
  void run1()
  {

    UNUSED(arg);
    pthread_barrier_wait(&barrier);
    void *pointer = service.handle_allocator_.alloc();
    //LOG_INFO("alloc pt", K(pointer));
    ASSERT_EQ(OB_HASH_INSERT_SUCC, pset.set((int64_t)(pointer)));
  }
};

TEST_F(TestTableNode, allocator)
{
  pthread_barrier_init(&barrier, NULL, MAX_THREAD_COUNT + 1);

  TestRunner runner;
  obsys::CThread threads[MAX_THREAD_COUNT];
  for (int64_t i = 0; i < MAX_THREAD_COUNT; ++i) {
    threads[i].start(&runner, NULL);
  }

  pthread_barrier_wait(&barrier);
  pthread_barrier_destroy(&barrier);

  for (int i = 0; i < MAX_THREAD_COUNT; ++i) {
    threads[i].join();
  }
}

void task(void *arg) {
  AutoincParam *param_t = static_cast<AutoincParam *>(arg);
  CacheHandle *handle = NULL;

  pthread_barrier_wait(&barrier);
  int ret = service.get_handle(*param_t, handle);
  bool result = OB_SUCCESS == ret || OB_INVALID_ARGUMENT == ret;
  ASSERT_TRUE(result);

  if (OB_SUCC(ret)) {
    uint64_t next_value = 0;
    while (OB_SUCCESS == handle->next_value(next_value)) {
      int64_t index = param_t->autoinc_table_id_ - TABLE_ID_START;
      ASSERT_TRUE(OB_HASH_INSERT_SUCC == value_sets[index].set(next_value));
    }
  }
}

void* run_task(void *arg)
{
  task(arg);
  return NULL;
}

class TaskRunner : public share::ObThreadPool
{
public:
  void run1()
  {
    LOG_INFO("start thread", K(thread));
    run_task(arg);
  }
};

TEST_F(TestTableNode, concurrent_same_table)
{
  value_sets[0].clear();

  pthread_barrier_init(&barrier, NULL, MAX_THREAD_COUNT + 1);
  TaskRunner runner;
  obsys::CThread threads[MAX_THREAD_COUNT];
  for (int i = 0; i < MAX_THREAD_COUNT; ++i) {
    AutoincParam *param_t = static_cast<AutoincParam *>(param_allocator.alloc());
    *param_t = param;
    param_t->autoinc_table_id_ = TABLE_ID_START;
    param_t->total_value_count_ = i;
    threads[i].start(&runner, param_t);
  }
  pthread_barrier_wait(&barrier);
  pthread_barrier_destroy(&barrier);

  for (int i = 0; i < MAX_THREAD_COUNT; ++i) {
    threads[i].join();
  }
}

TEST_F(TestTableNode, concurrent_diff_table)
{
  pthread_barrier_init(&barrier, NULL, MAX_THREAD_COUNT * TABLE_COUNT + 1);
  pthread_t threads[TABLE_COUNT * MAX_THREAD_COUNT];
  for (int64_t i = 0; i < TABLE_COUNT * MAX_THREAD_COUNT; ++i) {
    int64_t index = i % TABLE_COUNT;
    AutoincParam *param_t = static_cast<AutoincParam *>(param_allocator.alloc());
    *param_t = param;
    param_t->autoinc_table_id_ = TABLE_ID_START + index;
    param_t->total_value_count_ = i;
    pthread_create(&threads[i], NULL, run_task, param_t);
  }

  pthread_barrier_wait(&barrier);
  pthread_barrier_destroy(&barrier);

  for (int64_t i = 0; i < TABLE_COUNT * MAX_THREAD_COUNT; ++i) {
    pthread_join(threads[i], NULL);
  }
}

}

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("ERROR");
  ::test::init();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
