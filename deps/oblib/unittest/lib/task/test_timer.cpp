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
#include "lib/task/ob_timer.h"
#undef private

using namespace oceanbase::lib;
namespace oceanbase
{
namespace common
{

class TestTimerTask : public ObTimerTask
{
public:
  TestTimerTask() : running_(false), task_run_count_(0) {}

  void runTimerTask()
  {
    has_run_ = true;
    running_ = true;
    ++task_run_count_;
    ::usleep(exec_time_);
    running_ = false;
  }

  volatile bool running_;
  int64_t task_run_count_;
  int64_t exec_time_ = 50000; // 50ms
  bool has_run_ = false;
};

TEST(TestTimer, timer_task)
{
  TestTimerTask task[32 + 1];
  ObTimer timer;
  ASSERT_EQ(OB_SUCCESS, timer.init());
  ASSERT_EQ(OB_SUCCESS, timer.start());
  const bool is_repeat = true;
  ASSERT_EQ(OB_SUCCESS, timer.schedule(task[0], 100, is_repeat));
  for(int i=1; i<32; ++i)
  {
    ASSERT_EQ(OB_SUCCESS,timer.schedule(task[i], 5000000000, is_repeat));//5000s
  }
  ::usleep(5000);//5ms
  ASSERT_EQ(OB_ERR_UNEXPECTED, timer.schedule(task[32], 50000000, is_repeat));
  ::usleep(1000000);//1s
  ASSERT_GT(task[0].task_run_count_, 1);
}

TEST(TestTimer, task_cancel)
{
  TestTimerTask task;
  ObTimer timer;
  ASSERT_FALSE(timer.inited());
  ASSERT_EQ(OB_SUCCESS, timer.init());
  ASSERT_TRUE(timer.inited());
  ASSERT_EQ(OB_SUCCESS, timer.start());
  ASSERT_EQ(OB_SUCCESS, timer.schedule(task, 50000, true));
  ASSERT_TRUE(timer.task_exist(task));
  timer.dump();
  timer.cancel(task);
  ASSERT_FALSE(timer.task_exist(task));

  ASSERT_EQ(OB_SUCCESS, timer.schedule(task, 1000, false));
  ASSERT_TRUE(timer.task_exist(task));
  ::usleep(1000000);
  ASSERT_FALSE(timer.task_exist(task));
}

TEST(TestTimer, scheduled_immediately_task)
{
  TestTimerTask task[2];
  ObTimer timer;
  ASSERT_EQ(OB_SUCCESS, timer.init());
  ASSERT_EQ(OB_SUCCESS, timer.start());
  ASSERT_EQ(OB_SUCCESS, timer.schedule(task[0], 500000, true));
  ::usleep(60000);
  ASSERT_EQ(task[0].task_run_count_, 0);

  ASSERT_EQ(OB_SUCCESS, timer.schedule_repeate_task_immediately(task[1], 600000));
  ::usleep(500000);
  ASSERT_EQ(task[1].task_run_count_, 1);
  ::usleep(600000);
  ASSERT_GT(task[1].task_run_count_, 1);
}


TEST(TestTimer, start_stop)
{
  TestTimerTask task;
  ObTimer timer;
  ASSERT_EQ(OB_SUCCESS, timer.init());
  ASSERT_EQ(OB_SUCCESS, timer.start());
  ASSERT_EQ(OB_SUCCESS, timer.schedule(task, 0, true));
  timer.stop();
  ASSERT_NE(OB_SUCCESS, timer.schedule(task, 0, true));
  timer.wait();

  timer.cancel_all();

  timer.start();
  ASSERT_EQ(OB_SUCCESS, timer.schedule(task, 0, true));

  while (!task.running_) {}

  timer.stop();
  timer.wait();

  timer.start();
  ASSERT_EQ(OB_SUCCESS, timer.schedule(task, 0, true));
  timer.destroy();
}

TEST(TestTimer, task_cancel_wait)
{
  TestTimerTask task;
  ObTimer timer;
  ASSERT_EQ(OB_SUCCESS, timer.init());
  ASSERT_EQ(OB_SUCCESS, timer.start());
  // cancel from non-running
  {
    ASSERT_EQ(OB_SUCCESS, timer.schedule(task, 1000000, true));
    ASSERT_EQ(1, timer.get_tasks_num());
    ASSERT_EQ(OB_SUCCESS, timer.cancel_task(task));
    ASSERT_FALSE(timer.task_exist(task));
    // repeat cancel
    ASSERT_EQ(OB_SUCCESS, timer.cancel_task(task));
    ASSERT_FALSE(timer.task_exist(task));
  }
  // cancel from running
  {
    task.exec_time_ = 1000000;
    int64_t cur_time = ObTimeUtility::current_time();
    ASSERT_EQ(OB_SUCCESS, timer.schedule(task, 0, true));
    usleep(10000);
    ASSERT_EQ(OB_SUCCESS, timer.cancel_task(task));
    // repeat cancel
    ASSERT_EQ(OB_SUCCESS, timer.cancel_task(task));
    ASSERT_LT(ObTimeUtility::current_time() - cur_time, 1000000);
    ASSERT_EQ(OB_SUCCESS, timer.wait_task(task));
    ASSERT_GT(ObTimeUtility::current_time() - cur_time, 1000000);
    ASSERT_FALSE(timer.task_exist(task));
    // wait non-exist task
    ASSERT_EQ(OB_SUCCESS, timer.wait_task(task));
    ASSERT_TRUE(NULL == timer.running_task_);
    ASSERT_TRUE(NULL == timer.uncanceled_task_);
  }
  // cancel all
  {
    TestTimerTask task2;
    TestTimerTask task3;
    task2.exec_time_ = 1000000;
    ASSERT_EQ(OB_SUCCESS, timer.schedule(task2, 0, true));
    ASSERT_EQ(OB_SUCCESS, timer.schedule(task3, 1000, true));
    usleep(10000);
    timer.cancel_all();
    ASSERT_TRUE(task2.has_run_);
    ASSERT_FALSE(task3.has_run_);
  }
  timer.stop();
  timer.wait();
  timer.destroy();
}

} // end namespace common
} // end namespace oceanbase

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
