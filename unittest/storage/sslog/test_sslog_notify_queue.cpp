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
#define UNITTEST_DEBUG
#include <gtest/gtest.h>
#include "lib/ob_errno.h"
#include "share/ob_errno.h"
#include <exception>
#define private public
#define protected public
#include "lib/allocator/ob_malloc.h"
#include <thread>
#include <iostream>
#include <vector>
#include <chrono>
#include "close_modules/shared_storage/storage/incremental/sslog/notify/ob_sslog_notify_service.h"
#include "close_modules/shared_storage/storage/incremental/sslog/notify/ob_sslog_notify_task.h"
#include "close_modules/shared_storage/storage/incremental/sslog/notify/ob_sslog_notify_task_queue.h"

namespace oceanbase {
namespace sslog {
void ObSSLogNotifyTaskQueue::append_rolling(ObSSLogNotifyTask *task) {
  ObByteLockGuard lg(lock_);
  append_(task);
  while (count_ * sizeof(ObSSLogNotifyTask) > sizeof(ObSSLogNotifyTask)) {
    ObSSLogNotifyTask *head = sentinel_node_.next_;
    head->remove_self();
    count_--;
    SSLogNotifyAllocator::get_instance().free(head);
  }
}
}
using namespace sslog;
namespace unittest {

using namespace common;
using namespace std;
using namespace storage;
using namespace mds;

class TestSSLogNotifyQueue: public ::testing::Test
{
public:
  TestSSLogNotifyQueue() {};
  virtual ~TestSSLogNotifyQueue() {};
  virtual void SetUp() {
  };
  virtual void TearDown() {
  };
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestSSLogNotifyQueue);
};

TEST_F(TestSSLogNotifyQueue, serialize) {
  ObSSLogNotifyTaskQueue queue;
  ObSSLogNotifyTask *task1 = (ObSSLogNotifyTask *)SSLogNotifyAllocator::get_instance().alloc(sizeof(ObSSLogNotifyTask));
  new (task1) ObSSLogNotifyTask();
  task1->meta_key_.set_meta_key(ObSSLogNotifyLSKey(1, 1));
  queue.append(task1);
  ObSSLogNotifyTask *task2 = (ObSSLogNotifyTask *)SSLogNotifyAllocator::get_instance().alloc(sizeof(ObSSLogNotifyTask));
  new (task2) ObSSLogNotifyTask();
  task2->meta_key_.set_meta_key(ObSSLogNotifyLSKey(2, 2));
  queue.append(task2);
  ObSSLogNotifyTask *task3 = (ObSSLogNotifyTask *)SSLogNotifyAllocator::get_instance().alloc(sizeof(ObSSLogNotifyTask));
  new (task3) ObSSLogNotifyTask();
  task3->meta_key_.set_meta_key(ObSSLogNotifyLSKey(3, 3));
  queue.append(task3);
  int64_t len = queue.get_serialize_size();
  char buffer[queue.get_serialize_size()];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, queue.serialize(buffer, len, pos));
  ObSSLogNotifyTaskQueueView queue_view;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, queue_view.deserialize(buffer, len, pos));
  ObSSLogNotifyLSKey ls_key;
  ASSERT_EQ(OB_SUCCESS, queue_view.p_queue_->sentinel_node_.next_->meta_key_.get_meta_key(ls_key));
  ASSERT_EQ(ObSSLogNotifyLSKey(1, 1), ls_key);
  ASSERT_EQ(OB_SUCCESS, queue_view.p_queue_->sentinel_node_.next_->next_->meta_key_.get_meta_key(ls_key));
  ASSERT_EQ(ObSSLogNotifyLSKey(2, 2), ls_key);
  ASSERT_EQ(OB_SUCCESS, queue_view.p_queue_->sentinel_node_.next_->next_->next_->meta_key_.get_meta_key(ls_key));
  ASSERT_EQ(ObSSLogNotifyLSKey(3, 3), ls_key);

  queue_view.move_to(queue);
  ASSERT_EQ(OB_SUCCESS, queue.sentinel_node_.next_->meta_key_.get_meta_key(ls_key));
  ASSERT_EQ(ObSSLogNotifyLSKey(1, 1), ls_key);
  ASSERT_EQ(OB_SUCCESS, queue.sentinel_node_.next_->next_->meta_key_.get_meta_key(ls_key));
  ASSERT_EQ(ObSSLogNotifyLSKey(2, 2), ls_key);
  ASSERT_EQ(OB_SUCCESS, queue.sentinel_node_.next_->next_->next_->meta_key_.get_meta_key(ls_key));
  ASSERT_EQ(ObSSLogNotifyLSKey(3, 3), ls_key);
}

TEST_F(TestSSLogNotifyQueue, rolling) {
  ObSSLogNotifyTaskQueue queue;
  ObSSLogNotifyTask *task1 = (ObSSLogNotifyTask *)SSLogNotifyAllocator::get_instance().alloc(sizeof(ObSSLogNotifyTask));
  new (task1) ObSSLogNotifyTask();
  task1->meta_key_.set_meta_key(ObSSLogNotifyLSKey(1, 1));
  queue.append_rolling(task1);
  ObSSLogNotifyTask *task2 = (ObSSLogNotifyTask *)SSLogNotifyAllocator::get_instance().alloc(sizeof(ObSSLogNotifyTask));
  new (task2) ObSSLogNotifyTask();
  task2->meta_key_.set_meta_key(ObSSLogNotifyLSKey(1, 1));
  queue.append_rolling(task2);
  ASSERT_EQ(queue.count_, 1);
}

TEST_F(TestSSLogNotifyQueue, multi_destroy) {
  ObSSLogNotifyTaskQueue queue;
  queue.~ObSSLogNotifyTaskQueue();
  queue.~ObSSLogNotifyTaskQueue();
}

}
}

int main(int argc, char **argv)
{
  system("rm -rf test_sslog_notify_queue.log");
  oceanbase::common::ObLogger &logger = oceanbase::common::ObLogger::get_logger();
  logger.set_file_name("test_sslog_notify_queue.log", false);
  logger.set_log_level(OB_LOG_LEVEL_DEBUG);
  testing::InitGoogleTest(&argc, argv);
  int ret = RUN_ALL_TESTS();
  return ret;
}