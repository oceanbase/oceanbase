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

#include "gtest/gtest.h"
#include "lib/thread/ob_async_task_queue.h"

namespace oceanbase
{
using namespace common;
namespace share
{

class ObFakeTask : public ObAsyncTask
{
public:
  ObFakeTask(const bool copy_success, const bool process_success)
    : copy_success_(copy_success), process_success_(process_success) {}
  virtual int process() { return process_success_ ? OB_SUCCESS : OB_ERROR; }
  int64_t get_deep_copy_size() const { return sizeof(ObFakeTask); }
  ObAsyncTask *deep_copy(char *buf, const int64_t buf_size) const;
protected:
  bool copy_success_;
  bool process_success_;
};

ObAsyncTask *ObFakeTask::deep_copy(char *buf, const int64_t buf_size) const
{
  UNUSED(buf_size);
  ObAsyncTask *task = NULL;
  if (copy_success_) {
    task = new (buf)ObFakeTask(copy_success_, process_success_);
    memcpy(task, this, sizeof(*this));
  }
  return task;
}

TEST(TestAsyncTaskQueue, init)
{
  ObAsyncTaskQueue queue;
  ASSERT_EQ(OB_INVALID_ARGUMENT, queue.init(0, 0));
  ASSERT_EQ(OB_INVALID_ARGUMENT, queue.init(1, 0));
  ASSERT_EQ(OB_INVALID_ARGUMENT, queue.init(1, 7));
  ASSERT_EQ(OB_SUCCESS, queue.init(1, 1024));

  ASSERT_EQ(OB_INIT_TWICE, queue.init(2, 1024));
  queue.destroy();
}

TEST(TestAsyncTaskQueue, push)
{
  ObAsyncTaskQueue queue;
  ASSERT_EQ(OB_SUCCESS, queue.init(2, 1024));
  ObFakeTask fail_task(false, true);
  ASSERT_EQ(OB_ERR_UNEXPECTED, queue.push(fail_task));
  ObFakeTask success_task(true, true);
  ASSERT_EQ(OB_SUCCESS, queue.push(success_task));
  queue.destroy();
}

TEST(TestAsyncTaskQueue, run)
{
  ObAsyncTaskQueue queue;
  ASSERT_EQ(OB_SUCCESS, queue.init(2, 1024));
  ObFakeTask task(true, true);
  for (int i = 0; i < 10; ++i) {
    ASSERT_EQ(OB_SUCCESS, queue.push(task));
  }

  ObFakeTask fail_task(true, false);
  fail_task.set_retry_times(3);
  ASSERT_EQ(OB_SUCCESS, queue.push(fail_task));

  usleep(6 * static_cast<int32_t>(fail_task.get_retry_interval()));
  queue.destroy();
}

}//end namespace share
}//end namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

