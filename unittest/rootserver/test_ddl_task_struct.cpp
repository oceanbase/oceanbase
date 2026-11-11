/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include <atomic>
#include <memory>
#include <string>
#include <thread>
#define USING_LOG_PREFIX SERVER
#define protected public
#define private public

#include "rootserver/ddl_task/ob_ddl_task.h"
#include "rootserver/ddl_task/ob_ddl_scheduler.h"
#include "share/scheduler/ob_sys_task_stat.h"
#include "share/ob_debug_sync.h"
#include "share/ob_ddl_sim_point.h"
#include "share/config/ob_server_config.h"

#define LOG_FILE_PATH "./test_ddl_task_struct.log"

namespace oceanbase
{
namespace unittest
{

using namespace oceanbase::common;
using namespace oceanbase::storage;
using namespace oceanbase::rootserver;

#define ASSERT_SUCC(expr) ASSERT_EQ(OB_SUCCESS, (expr))

class QueueOwnershipTask : public ObDDLTask
{
public:
  QueueOwnershipTask(int &destroy_count, const uint64_t object_id, const int64_t task_id,
                     const int64_t parent_task_id = 1)
    : destroy_count_(destroy_count)
  {
    dst_tenant_id_ = 1001;
    target_object_id_ = object_id;
    dst_schema_version_ = 1;
    task_id_ = task_id;
    parent_task_id_ = parent_task_id;
  }
  ~QueueOwnershipTask() override { ++destroy_count_; }
private:
  int &destroy_count_;
};

TEST(test_ddl_task_struct, queue_push_preserves_scheduling_readiness)
{
  int destroy_count = 0;
  std::unique_ptr<QueueOwnershipTask> task(new QueueOwnershipTask(destroy_count, 100, 1));
  rootserver::ObDDLTaskQueue queue;
  ASSERT_SUCC(queue.init(16));
  queue.set_stop(false);
  ASSERT_SUCC(queue.push_task(task.get()));
  ObDDLTask *next = nullptr;
  ASSERT_SUCC(queue.get_next_task(next));
  ASSERT_EQ(task.get(), next);
  EXPECT_EQ(0, next->next_schedule_ts_);
  EXPECT_TRUE(next->get_sys_task_id().is_invalid());
  ObDDLTask *mapped = nullptr;
  ASSERT_SUCC(queue.task_map_.get_refactored(next->get_task_key(), mapped));
  EXPECT_EQ(next, mapped);
  ASSERT_SUCC(queue.task_id_map_.get_refactored(next->get_ddl_task_id(), mapped));
  EXPECT_EQ(next, mapped);
  ASSERT_SUCC(queue.remove_task(next));
  EXPECT_EQ(0, queue.get_task_cnt());
  EXPECT_EQ(0, queue.task_map_.size());
  EXPECT_EQ(0, queue.task_id_map_.size());
  task.reset();
  EXPECT_EQ(1, destroy_count);
}

TEST(test_ddl_task_struct, queue_duplicate_keeps_caller_ownership)
{
  int destroy_count = 0;
  std::unique_ptr<QueueOwnershipTask> original(new QueueOwnershipTask(destroy_count, 100, 1));
  std::unique_ptr<QueueOwnershipTask> duplicate_key(new QueueOwnershipTask(destroy_count, 100, 2));
  std::unique_ptr<QueueOwnershipTask> duplicate_id(new QueueOwnershipTask(destroy_count, 101, 1));
  rootserver::ObDDLTaskQueue queue;
  ASSERT_SUCC(queue.init(16));
  queue.set_stop(false);
  ASSERT_SUCC(queue.push_task(original.get()));
  ASSERT_EQ(OB_ENTRY_EXIST, queue.push_task(duplicate_key.get()));
  ASSERT_EQ(OB_ENTRY_EXIST, queue.push_task(duplicate_id.get()));
  ASSERT_EQ(0, duplicate_key->next_schedule_ts_);
  ASSERT_EQ(0, duplicate_id->next_schedule_ts_);
  ASSERT_EQ(0, original->next_schedule_ts_);
  ASSERT_EQ(1, queue.get_task_cnt());
  ASSERT_EQ(1, queue.task_map_.size());
  ASSERT_EQ(1, queue.task_id_map_.size());
  ObDDLTask *mapped = nullptr;
  ASSERT_SUCC(queue.task_map_.get_refactored(original->get_task_key(), mapped));
  ASSERT_EQ(original.get(), mapped);
  mapped = nullptr;
  ASSERT_SUCC(queue.task_id_map_.get_refactored(original->get_ddl_task_id(), mapped));
  ASSERT_EQ(original.get(), mapped);
  ASSERT_EQ(OB_HASH_NOT_EXIST,
            queue.task_map_.get_refactored(duplicate_id->get_task_key(), mapped));
  ASSERT_EQ(OB_HASH_NOT_EXIST,
            queue.task_id_map_.get_refactored(duplicate_key->get_ddl_task_id(), mapped));
  ASSERT_EQ(0, destroy_count);
  duplicate_key.reset();
  duplicate_id.reset();
  ASSERT_EQ(2, destroy_count);
  ObDDLTask *next = nullptr;
  ASSERT_SUCC(queue.get_next_task(next));
  ASSERT_EQ(original.get(), next);
  ASSERT_SUCC(queue.remove_task(next));
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, queue.get_next_task(next));
  ASSERT_EQ(0, queue.task_map_.size());
  ASSERT_EQ(0, queue.task_id_map_.size());
  original.reset();
  ASSERT_EQ(3, destroy_count);
}

TEST(test_ddl_task_struct, queue_stop_rejects_before_publish)
{
  int destroy_count = 0;
  std::unique_ptr<QueueOwnershipTask> task(new QueueOwnershipTask(destroy_count, 100, 1));
  rootserver::ObDDLTaskQueue queue;
  ASSERT_SUCC(queue.init(16));
  ASSERT_TRUE(queue.has_set_stop());
  ASSERT_EQ(OB_STATE_NOT_MATCH, queue.push_task(task.get()));
  ASSERT_EQ(0, task->next_schedule_ts_);
  ASSERT_EQ(0, queue.get_task_cnt());
  ASSERT_EQ(0, destroy_count);
  queue.set_stop(false);
  ASSERT_SUCC(queue.push_task(task.get()));
  ASSERT_EQ(0, task->next_schedule_ts_);
  ASSERT_EQ(1, queue.get_task_cnt());
  ASSERT_EQ(0, destroy_count);
  int rejected_destroy_count = 0;
  std::unique_ptr<QueueOwnershipTask> rejected(
      new QueueOwnershipTask(rejected_destroy_count, 101, 2));
  queue.set_stop(true);
  ASSERT_EQ(OB_STATE_NOT_MATCH, queue.push_task(rejected.get()));
  ASSERT_EQ(0, rejected->next_schedule_ts_);
  ASSERT_EQ(1, queue.get_task_cnt());
  ASSERT_EQ(1, queue.task_map_.size());
  ASSERT_EQ(1, queue.task_id_map_.size());
  ObDDLTask *mapped = nullptr;
  ASSERT_SUCC(queue.task_map_.get_refactored(task->get_task_key(), mapped));
  ASSERT_EQ(task.get(), mapped);
  ASSERT_SUCC(queue.task_id_map_.get_refactored(task->get_ddl_task_id(), mapped));
  ASSERT_EQ(task.get(), mapped);
  ASSERT_EQ(OB_HASH_NOT_EXIST, queue.task_map_.get_refactored(rejected->get_task_key(), mapped));
  ASSERT_EQ(OB_HASH_NOT_EXIST, queue.task_id_map_.get_refactored(rejected->get_ddl_task_id(), mapped));
  ASSERT_EQ(0, rejected_destroy_count);
  rejected.reset();
  ASSERT_EQ(1, rejected_destroy_count);
  ASSERT_EQ(0, destroy_count);
  ObDDLTask *next = nullptr;
  ASSERT_SUCC(queue.get_next_task(next));
  ASSERT_EQ(task.get(), next);
  ASSERT_SUCC(queue.remove_task(next));
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, queue.get_next_task(next));
  ASSERT_EQ(0, queue.get_task_cnt());
  ASSERT_EQ(0, queue.task_map_.size());
  ASSERT_EQ(0, queue.task_id_map_.size());
  task.reset();
  ASSERT_EQ(1, destroy_count);
}

TEST(test_ddl_task_struct, queue_sys_task_registration_failure_does_not_reject_task)
{
  class SysTaskAddressGuard
  {
  public:
    SysTaskAddressGuard() : saved_addr_(SYS_TASK_STATUS_MGR.self_addr_)
    {
      SYS_TASK_STATUS_MGR.self_addr_.reset();
    }
    ~SysTaskAddressGuard() { SYS_TASK_STATUS_MGR.self_addr_ = saved_addr_; }
  private:
    ObAddr saved_addr_;
  } address_guard;
  const int64_t registered_before = SYS_TASK_STATUS_MGR.task_array_.count();
  int destroy_count = 0;
  std::unique_ptr<QueueOwnershipTask> task(new QueueOwnershipTask(destroy_count, 100, 1, 0));
  std::unique_ptr<QueueOwnershipTask> original(new QueueOwnershipTask(destroy_count, 101, 2));
  rootserver::ObDDLTaskQueue queue;
  ASSERT_SUCC(queue.init(16));
  queue.set_stop(false);
  ASSERT_SUCC(queue.push_task(original.get()));
  ASSERT_SUCC(queue.push_task(task.get()));
  ASSERT_EQ(0, destroy_count);
  ASSERT_EQ(0, task->next_schedule_ts_);
  ASSERT_TRUE(task->get_sys_task_id().is_invalid());
  ASSERT_EQ(registered_before, SYS_TASK_STATUS_MGR.task_array_.count());
  ASSERT_EQ(2, queue.get_task_cnt());
  ASSERT_EQ(2, queue.task_map_.size());
  ASSERT_EQ(2, queue.task_id_map_.size());
  ObDDLTask *mapped = nullptr;
  ASSERT_SUCC(queue.task_map_.get_refactored(task->get_task_key(), mapped));
  ASSERT_EQ(task.get(), mapped);
  ASSERT_SUCC(queue.task_id_map_.get_refactored(task->get_ddl_task_id(), mapped));
  ASSERT_EQ(task.get(), mapped);
  ObDDLTask *next = nullptr;
  ASSERT_SUCC(queue.task_map_.get_refactored(original->get_task_key(), mapped));
  ASSERT_EQ(original.get(), mapped);
  ASSERT_SUCC(queue.task_id_map_.get_refactored(original->get_ddl_task_id(), mapped));
  ASSERT_EQ(original.get(), mapped);
  ASSERT_SUCC(queue.get_next_task(next));
  ASSERT_EQ(original.get(), next);
  ASSERT_SUCC(queue.remove_task(next));
  ASSERT_SUCC(queue.get_next_task(next));
  ASSERT_EQ(task.get(), next);
  ASSERT_SUCC(queue.remove_task(next));
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, queue.get_next_task(next));
  ASSERT_EQ(0, queue.get_task_cnt());
  ASSERT_EQ(0, queue.task_map_.size());
  ASSERT_EQ(0, queue.task_id_map_.size());
  ASSERT_EQ(registered_before, SYS_TASK_STATUS_MGR.task_array_.count());
  original.reset();
  task.reset();
  ASSERT_EQ(2, destroy_count);
}

TEST(test_ddl_task_struct, scheduler_consumer_frees_before_submitter_resumes)
{
  class ScheduleEnvironmentGuard
  {
  public:
    ScheduleEnvironmentGuard()
      : saved_addr_(SYS_TASK_STATUS_MGR.self_addr_),
        saved_timeout_(GCONF.debug_sync_timeout.str()) {}
    ~ScheduleEnvironmentGuard()
    {
      ObDebugSyncAction action;
      action.sync_point_ = AFTER_DDL_TASK_QUEUE_PUSH;
      GDS.set_global_action(false, true, action);
      GCONF.debug_sync_timeout.set_value(saved_timeout_.c_str());
      SYS_TASK_STATUS_MGR.self_addr_ = saved_addr_;
    }
  private:
    ObAddr saved_addr_;
    std::string saved_timeout_;
  } environment;
  ObAddr self;
  ASSERT_TRUE(self.set_ip_addr("127.0.0.1", 2882));
  ASSERT_SUCC(SYS_TASK_STATUS_MGR.set_self_addr(self));
  ASSERT_TRUE(GCONF.debug_sync_timeout.set_value("10s"));
  ObDebugSyncAction action;
  action.sync_point_ = AFTER_DDL_TASK_QUEUE_PUSH;
  action.timeout_ = 10 * 1000 * 1000;
  action.execute_ = 1;
  ASSERT_SUCC(action.signal_.assign("ddl_schedule_arrived"));
  ASSERT_SUCC(action.wait_.assign("ddl_schedule_resume"));
  ASSERT_SUCC(GDS.set_global_action(false, false, action));
  ASSERT_TRUE(GDS.global_actions_.is_active(AFTER_DDL_TASK_QUEUE_PUSH));
  const int64_t registered_before = SYS_TASK_STATUS_MGR.task_array_.count();
  int destroy_count = 0;
  std::unique_ptr<QueueOwnershipTask> task(new QueueOwnershipTask(destroy_count, 100, 1, 0));
  ObDDLTask *const submitted = task.get();
  ObDDLTaskRecord record;
  record.gmt_create_ = 123;
  ObDDLScheduler scheduler;
  ASSERT_SUCC(scheduler.task_queue_.init(16));
  scheduler.set_stop(false);
  scheduler.is_stop_ = false;
  scheduler.task_queue_.set_stop(false);
  scheduler.rs_epoch_ = 42;
  int schedule_ret = OB_ERR_UNEXPECTED;
  std::atomic<bool> completed(false);
  std::thread submitter([&]() {
    schedule_ret = scheduler.inner_schedule_ddl_task(submitted, record);
    completed.store(true);
  });
  const int arrived_ret = GDS.event_control_.wait(action.signal_, action.timeout_, false);
  bool arrived = false;
  {
    ObThreadCondGuard guard(GDS.event_control_.cond_);
    DLIST_FOREACH_NORET(event, GDS.event_control_.used_) {
      if (event->name_ == action.signal_ && event->signal_cnt_ > 0) {
        arrived = true;
      }
    }
  }
  if (arrived) {
    GDS.event_control_.wait(action.signal_, action.timeout_, true);
  }
  int pop_ret = OB_ERR_UNEXPECTED;
  int unregister_ret = OB_ERR_UNEXPECTED;
  int remove_ret = OB_ERR_UNEXPECTED;
  bool same_task = false;
  bool enabled_before_resume = false;
  bool registered_before_resume = false;
  int64_t rs_epoch = 0;
  uint64_t gmt_create = 0;
  if (OB_SUCCESS == arrived_ret && arrived) {
    ObDDLTask *next = nullptr;
    pop_ret = scheduler.task_queue_.get_next_task(next);
    same_task = (next == submitted);
    if (OB_SUCCESS == pop_ret && same_task) {
      enabled_before_resume = (0 == next->next_schedule_ts_);
      registered_before_resume = !next->get_sys_task_id().is_invalid();
      rs_epoch = next->get_rs_epoch_snapshot();
      gmt_create = next->get_gmt_create();
      unregister_ret = scheduler.remove_sys_task(next);
      remove_ret = scheduler.task_queue_.remove_task(next);
      if (OB_SUCCESS == remove_ret) {
        task.reset();
      }
    }
  }
  const int destroyed_before_resume = destroy_count;
  const bool completed_before_resume = completed.load();
  const int signal_ret = GDS.event_control_.signal(action.wait_);
  submitter.join();
  GDS.event_control_.wait(action.signal_, 1, true);
  GDS.event_control_.wait(action.wait_, 1, true);
  if (task) {
    ObDDLTask *remaining = nullptr;
    if (OB_SUCCESS == scheduler.task_queue_.get_next_task(remaining)) {
      scheduler.remove_sys_task(remaining);
      scheduler.task_queue_.remove_task(remaining);
    }
  }
  EXPECT_EQ(OB_SUCCESS, arrived_ret);
  EXPECT_TRUE(arrived);
  EXPECT_FALSE(completed_before_resume);
  EXPECT_EQ(OB_SUCCESS, pop_ret);
  EXPECT_TRUE(same_task);
  EXPECT_TRUE(enabled_before_resume);
  EXPECT_TRUE(registered_before_resume);
  EXPECT_EQ(42, rs_epoch);
  EXPECT_EQ(123, gmt_create);
  EXPECT_EQ(OB_SUCCESS, unregister_ret);
  EXPECT_EQ(OB_SUCCESS, remove_ret);
  EXPECT_EQ(1, destroyed_before_resume);
  EXPECT_EQ(OB_SUCCESS, signal_ret);
  EXPECT_EQ(OB_SUCCESS, schedule_ret);
  EXPECT_EQ(1, destroy_count);
  EXPECT_EQ(0, scheduler.task_queue_.get_task_cnt());
  EXPECT_EQ(0, scheduler.task_queue_.task_map_.size());
  EXPECT_EQ(0, scheduler.task_queue_.task_id_map_.size());
  EXPECT_EQ(registered_before, SYS_TASK_STATUS_MGR.task_array_.count());
}

TEST(test_ddl_task_struct, ddl_slice_info)
{
  // 1. default construct
  ObDDLSliceInfo slice_info;
  ASSERT_FALSE(slice_info.is_valid());

  // 2. push back an tablet range
  sql::ObPxTabletRange tablet_range;
  tablet_range.tablet_id_ = 0;
  ASSERT_SUCC(slice_info.part_ranges_.push_back(tablet_range));
  ASSERT_TRUE(slice_info.is_valid());

  // 3. test assign
  ObDDLSliceInfo assign_slice_info;
  tablet_range.tablet_id_ = 1;
  sql::ObPxTabletRange::DatumKey endkey;
  char test_string[32] = { 0 };
  const char *hello_str = "hello oceanbase";
  memcpy(test_string, hello_str, strlen(hello_str));
  ASSERT_SUCC(endkey.prepare_allocate(2));
  char datum_buffer[16] = { 0 };
  endkey.at(0).ptr_ = datum_buffer;
  endkey.at(0).set_int(100);
  endkey.at(1).ptr_ = datum_buffer + 8;
  endkey.at(1).set_string(test_string, sizeof(test_string));
  ASSERT_SUCC(tablet_range.range_cut_.push_back(endkey));
  ASSERT_SUCC(slice_info.part_ranges_.push_back(tablet_range));
  ASSERT_SUCC(assign_slice_info.assign(slice_info));
  ASSERT_TRUE(assign_slice_info.part_ranges_.count() == 2);

  // 4. test deep copy
  ObDDLSliceInfo deep_copied_slice_info;
  ObArenaAllocator arena;
  ASSERT_SUCC(deep_copied_slice_info.deep_copy(slice_info, arena));
  ASSERT_TRUE(assign_slice_info.part_ranges_.count() == 2);
  memset(test_string, 0, sizeof(test_string));
  LOG_INFO("after deep copy", K(slice_info), K(deep_copied_slice_info), K(assign_slice_info));
  ASSERT_EQ(0, strcmp(deep_copied_slice_info.part_ranges_.at(1).range_cut_.at(0).at(1).get_string().ptr(), hello_str));
  ASSERT_NE(0, strcmp(     assign_slice_info.part_ranges_.at(1).range_cut_.at(0).at(1).get_string().ptr(), hello_str));

  // 5. reset
  slice_info.reset();
  ASSERT_FALSE(slice_info.is_valid());
}

}
}

int main(int argc, char **argv)
{
  system("rm -rf " LOG_FILE_PATH "*");
  oceanbase::common::ObLogger::get_logger().set_log_level("WDIAG");
  oceanbase::common::ObLogger::get_logger().set_file_name(LOG_FILE_PATH, true);
  testing::InitGoogleTest(&argc, argv);
#ifdef ERRSIM
  if (oceanbase::common::OB_SUCCESS != oceanbase::share::ObDDLSimPointMgr::get_instance().init()) {
    return 1;
  }
#endif
  return RUN_ALL_TESTS();
}
