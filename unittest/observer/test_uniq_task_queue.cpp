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

#define USING_LOG_PREFIX SERVER

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include "observer/ob_uniq_task_queue.h"
#include "rpc/mock_ob_common_rpc_proxy.h"
#include "lib/ob_define.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/config/ob_server_config.h"
#include "lib/oblog/ob_log_module.h"
namespace oceanbase
{
namespace observer
{
using namespace common;
using namespace share;
using testing::_;
using ::testing::Return;
using ::testing::AtLeast;
using ::testing::Invoke;

class TestUniqTaskQueue : public ::testing::Test
{
public:
  virtual void SetUp() {}
  virtual void TearDown() {}
  TestUniqTaskQueue() {}
  ~TestUniqTaskQueue() {}
};

class MockTask : public common::ObDLinkBase<MockTask>
{
public:
  friend class MockTaskProcesser;

  MockTask()
   : group_id_(common::OB_INVALID_ID),
     task_id_(common::OB_INVALID_ID) {}

  MockTask(const uint64_t group_id,
           const uint64_t task_id)
   : group_id_(group_id), task_id_(task_id) {}

  virtual ~MockTask() {}

  void init(const uint64_t group_id,
            const uint64_t task_id)
  {
    group_id_ = group_id;
    task_id_ = task_id;
  }

  int assign(const MockTask &other) {
    int ret = OB_SUCCESS;
    group_id_ = other.group_id_;
    task_id_ = other.task_id_;
    return ret;
  }

  bool is_valid() const { return true; }

  virtual int64_t hash() const
  {
    uint64_t hash_val = 0;
    hash_val = murmurhash(&group_id_, sizeof(group_id_), hash_val);
    hash_val = murmurhash(&task_id_, sizeof(task_id_), hash_val);
    return static_cast<int64_t>(hash_val);
  }

  inline int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; }

  uint64_t get_group_id() const { return group_id_; }
  bool is_barrier() const { return false; }
  bool need_process_alone() const { return true; }

  virtual bool operator==(const MockTask &other) const
  {
    return group_id_ == other.group_id_ && task_id_ == other.task_id_;
  }

  virtual bool compare_without_version(const MockTask &other) const
  {
    return *this == other;
  }

  bool need_assign_when_equal() const { return false; }
  int assign_when_equal(const MockTask &other) { UNUSED(other); return OB_NOT_SUPPORTED; }

  uint64_t get_task_id() const { return task_id_; }
  TO_STRING_KV(K_(group_id), K_(task_id));
private:
  uint64_t group_id_;
  uint64_t task_id_;
};

class MockTaskProcesser;
typedef ObUniqTaskQueue<MockTask, MockTaskProcesser> MockTaskQueue;
class MockTaskProcesser
{
public:
  MockTaskProcesser(MockTaskQueue &queue) : results_(), queue_(queue) {}
  virtual int batch_process_tasks(const common::ObIArray<MockTask> &tasks, bool &stopped)
  {
    UNUSED(stopped);
    int ret = OB_SUCCESS;
    if (1 != tasks.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("task count is invalid", KR(ret), K(tasks.count()));
    } else if (OB_FAIL(results_.push_back(tasks.at(0)))) {
      LOG_WARN("fail to push back task", KR(ret), K(tasks.count()));
    } else if (tasks.at(0).get_group_id() == 1
               && tasks.at(0).get_task_id() == 0) {
      MockTask task(1 /*group_id*/, 1 /*task_id*/);
      if (OB_FAIL(queue_.add(task))) {
        LOG_WARN("fail to add task", KR(ret), K(task));
      }
    }
    return ret;
  }
  int process_barrier(const MockTask &task, bool &stopped)
  {
    UNUSEDx(task, stopped);
    return OB_NOT_SUPPORTED;
  }
  ObArray<MockTask >&get_results() { return results_; }
private:
  ObArray<MockTask> results_;
  MockTaskQueue &queue_;
};

/*
class MockUpdate : public ObPartitionTableUpdater
{
public:
  MockUpdate() : batch_count_(0) {}
  ~MockUpdate() {}
  int64_t batch_count() {return batch_count_; }
  virtual int batch_process_tasks(const common::ObIArray<ObPTUpdateTask> &tasks, bool &stopped)
  {
    UNUSED(tasks);
    LOG_INFO("batch process task", K(tasks.count()), K(stopped), K(batch_count_));
    if (batch_count_ < 10) {
      sleep(2);
    }
    batch_count_ ++;
    return OB_SUCCESS;
  }
  int process_barrier(const ObPTUpdateTask &task, bool &stopped)
  {
    LOG_INFO("barrier process task", K(task), K(stopped));
    return OB_SUCCESS;
  }
private:
  int64_t batch_count_;
};
TEST_F(TestUniqTaskQueue, test_concurrency_execute)
{
  MockUpdate updater;
  ObUniqTaskQueue<ObPTUpdateTask, MockUpdate> queue;
  queue.init(&updater, 1, 100000);
  int64_t core_table_id = combine_id(OB_SYS_TENANT_ID, OB_ALL_CORE_TABLE_TID);
  int64_t sys_table_id = combine_id(OB_SYS_TENANT_ID, OB_ALL_DDL_OPERATION_TID);
  int64_t user_table_id = combine_id(500001, 100001);
  int64_t partition_cnt = 1;
  int64_t partition_id = 0;
  ObPartitionKey core_key(core_table_id, partition_id, partition_cnt);
  ObPartitionKey sys_key(sys_table_id, partition_id, partition_cnt);
  ObPartitionKey user_key(user_table_id, partition_id, partition_cnt);
  ObPTUpdateTask task;
  //同一个partition不同的key，作为不同的batch进行处理
  for (int64_t i = 1; i <= 3000; i++) {
    EXPECT_EQ(OB_SUCCESS, task.set_update_task(core_key, i));
    EXPECT_EQ(OB_SUCCESS, queue.add(task));
  }
  while (queue.task_count() > 0) {
    sleep(3);
  }
  EXPECT_EQ(updater.batch_count(), 3000);
  //不同的partition， 可以作为一个batch 处理
  int64_t data_version = 4;
  for (int64_t i = 0; i < 3000; i++) {
    ObPartitionKey sys_key(sys_table_id, i, 3000);
    EXPECT_EQ(OB_SUCCESS, task.set_update_task(sys_key, data_version));
    EXPECT_EQ(OB_SUCCESS, queue.add(task));
  }
  while (queue.task_count() > 0) {
    sleep(3);
  }
  EXPECT_LT(updater.batch_count(), 6000);
}
*/

// bugfix: workitem/49006474
TEST_F(TestUniqTaskQueue, test_queue_starvation)
{
  obrpc::MockObCommonRpcProxy rpc;
  GDS.set_rpc_proxy(&rpc);

  ObMalloc allocator;
  ObDSSessionActions sa;
  ASSERT_EQ(OB_SUCCESS, sa.init(1024, allocator));

  GCONF.debug_sync_timeout.set_value("1000s");
  ASSERT_TRUE(GCONF.is_debug_sync_enabled());

  const bool L = false; // local
  ASSERT_EQ(OB_SUCCESS, GDS.add_debug_sync("BEFORE_UNIQ_TASK_RUN wait_for signal", L, sa));

  MockTaskQueue queue;
  MockTaskProcesser processor(queue);
  ASSERT_EQ(OB_SUCCESS, queue.init(&processor, 1 /*thread_num*/, 1024 /*queue_size*/));

  MockTask task(0 /*group_id*/, 0 /*task_id*/);
  ASSERT_EQ(OB_SUCCESS, queue.add(task));

  (void) task.init(0 /*group_id*/, 1 /*task_id*/);
  ASSERT_EQ(OB_SUCCESS, queue.add(task));

  (void) task.init(1 /*group_id*/, 0 /*task_id*/);
  ASSERT_EQ(OB_SUCCESS, queue.add(task));

  /*
   * tasks in queue are as follows:
   *
   *       |-----------|         |-----------|
   *       |--group 0--|         |--group 1--|
   * ...   |-----------| <-----> |-----------| <-----> tail
   *       |--task 0---|         |--task 0---|
   *       |--task 1---|
   *       |-----------|
   */

  ASSERT_EQ(OB_SUCCESS, GDS.add_debug_sync("now signal signal", L, sa));

  /*
   * before task(1, 0) runs, tasks in queue are as follows:
   *
   *       |-----------|         |-----------|
   *       |--group 0--|         |--group 1--|
   * ...   |-----------| <-----> |-----------| <-----> tail
   *       |--task 1---|         |--task 0---|
   *       |-----------|
   *         cur_group              group
   *
   * when task(1, 0) runs, task(1, 1) will be generated and tasks in queue are as follows:
   *
   *       |-----------|         |-----------|
   *       |--group 0--|         |--group 1--|
   * ...   |-----------| <-----> |-----------| <-----> tail
   *       |--task 1---|         |--task 1---|
   *       |-----------|
   */

  int64_t max_retry_times = 10;
  int64_t retry_times = 0;
  while (retry_times < max_retry_times && processor.get_results().count() < 4) {
    retry_times++;
    usleep(1 * 1000 * 1000L); //1s
  }

  ASSERT_EQ(4, processor.get_results().count());

  // Eventaully, results should be (0, 0)->(1, 0)->(0, 1)->(1, 1) if group rotates.
  //
  // When group doesn't rotate, results will be (0, 0)->(1, 0)->(1, 1)->(0, 1).
  (void) task.init(0 /*group_id*/, 0 /*task_id*/);
  ASSERT_EQ(task, processor.get_results().at(0));

  (void) task.init(1 /*group_id*/, 0 /*task_id*/);
  ASSERT_EQ(task, processor.get_results().at(1));

  (void) task.init(0 /*group_id*/, 1 /*task_id*/);
  ASSERT_EQ(task, processor.get_results().at(2));

  (void) task.init(1 /*group_id*/, 1 /*task_id*/);
  ASSERT_EQ(task, processor.get_results().at(3));

  GCONF.debug_sync_timeout.set_value("0");
  ASSERT_FALSE(GCONF.is_debug_sync_enabled());
}
} //namespace observer
} //namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_uniq_task_queue.log");
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_file_name("test_uniq_task_queue.log", true);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
