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
#include <iostream>
#include <chrono>
#define private public
#include "lib/task/ob_timer.h"
#undef private

using namespace oceanbase::lib;
namespace oceanbase
{
namespace common
{

class TestTimer : public testing::Test
{
protected:
  static void SetUpTestCase()
  {
    ASSERT_EQ(OB_SUCCESS, ObTimerService::get_instance().start());
  }

  static void TearDownTestCase()
  {
    ObTimerService::get_instance().stop();
    ObTimerService::get_instance().wait();
    ObTimerService::get_instance().destroy();
  }
};

class TestTimerTask : public ObTimerTask
{
public:
  TestTimerTask() : running_(false), task_run_count_(0) {}
  TestTimerTask(const TestTimerTask &) = delete;
  ~TestTimerTask() { abort_unless(false == running_); }
  const TestTimerTask &operator=(const TestTimerTask &) = delete;

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

TEST_F(TestTimer, timer_task)
{
  TestTimerTask task[32 + 1];
  ObTimer timer;
  ASSERT_EQ(OB_SUCCESS, timer.init());
  ASSERT_EQ(OB_SUCCESS, timer.start());
  const bool is_repeat = true;
  ASSERT_EQ(OB_SUCCESS, timer.schedule(task[0], 100, is_repeat)); // 0.1ms
  for(int i=1; i<32; ++i)
  {
    ASSERT_EQ(OB_SUCCESS,timer.schedule(task[i], 5000000000, is_repeat)); // 5000s
  }
  ::usleep(5000); //5ms
  ASSERT_EQ(OB_SUCCESS, timer.schedule(task[32], 50000000, is_repeat));
  ::usleep(1000000); // 1s
  ASSERT_GT(task[0].task_run_count_, 1);
  timer.stop();
  timer.destroy();
}

TEST_F(TestTimer, task_cancel)
{
  TestTimerTask task;
  ObTimer timer;
  ASSERT_FALSE(timer.inited());
  ASSERT_EQ(OB_SUCCESS, timer.init());
  ASSERT_TRUE(timer.inited());
  ASSERT_EQ(OB_SUCCESS, timer.start());
  ASSERT_EQ(OB_SUCCESS, timer.schedule(task, 50000, true));
  ASSERT_TRUE(timer.task_exist(task));
  // timer.dump();
  timer.cancel(task);
  ASSERT_FALSE(timer.task_exist(task));

  ASSERT_EQ(OB_SUCCESS, timer.schedule(task, 1000, false));
  ASSERT_TRUE(timer.task_exist(task));
  ::usleep(100000); // 100ms
  ASSERT_FALSE(timer.task_exist(task));
  timer.stop();
  timer.destroy();
}

TEST_F(TestTimer, scheduled_immediately_task)
{
  TestTimerTask task[2];
  ObTimer timer;
  ASSERT_EQ(OB_SUCCESS, timer.init());
  ASSERT_EQ(OB_SUCCESS, timer.start());
  ASSERT_EQ(OB_SUCCESS, timer.schedule(task[0], 200000, true));  // delay=200ms repeat=true
  ::usleep(100000); // 100ms
  ASSERT_EQ(task[0].task_run_count_, 0);

  ASSERT_EQ(OB_SUCCESS, timer.schedule_repeate_task_immediately(task[1], 200000));  // delay=200ms repeat=true
  ::usleep(200000); // 200ms
  ASSERT_EQ(task[0].task_run_count_, 1);
  ASSERT_EQ(task[1].task_run_count_, 1);
  ::usleep(200000); // 200ms
  ASSERT_EQ(task[1].task_run_count_, 2);

  timer.stop();
  timer.destroy();
}

TEST_F(TestTimer, start_stop)
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

TEST_F(TestTimer, task_cancel_wait)
{
  TestTimerTask task;
  ObTimer timer;
  ASSERT_EQ(OB_SUCCESS, timer.init());
  ASSERT_EQ(OB_SUCCESS, timer.start());

  // cancel from non-running
  ASSERT_EQ(OB_SUCCESS, timer.schedule(task, 1000000, true));
  ASSERT_EQ(OB_SUCCESS, timer.cancel_task(task));
  ASSERT_FALSE(timer.task_exist(task));
  // repeat cancel
  ASSERT_EQ(OB_SUCCESS, timer.cancel_task(task));
  ASSERT_FALSE(timer.task_exist(task));

  // cancel from running
  task.exec_time_ = 1000000;
  int64_t cur_time = ObTimeUtility::current_time();
  ASSERT_EQ(OB_SUCCESS, timer.schedule(task, 0, true));
  // it must sleep for enough time to ensure that the task has started running
  usleep(50000);
  ASSERT_EQ(OB_SUCCESS, timer.cancel_task(task));
  // repeat cancel
  ASSERT_EQ(OB_SUCCESS, timer.cancel_task(task));
  ASSERT_LT(ObTimeUtility::current_time() - cur_time, 1000000);
  ASSERT_EQ(OB_SUCCESS, timer.wait_task(task));
  ASSERT_GT(ObTimeUtility::current_time() - cur_time, 1000000);
  ASSERT_FALSE(timer.task_exist(task));
  // wait non-exist task
  ASSERT_EQ(OB_SUCCESS, timer.wait_task(task));

  // cancel all
  TestTimerTask task2;
  TestTimerTask task3;
  task2.exec_time_ = 200000; // 200ms
  ASSERT_EQ(OB_SUCCESS, timer.schedule(task2, 0, true));
  ASSERT_EQ(OB_SUCCESS, timer.schedule(task3, 1000, true));
  usleep(10000); // 10ms
  timer.cancel_all();
  ASSERT_TRUE(task2.has_run_);
  ASSERT_FALSE(task3.has_run_);
  int64_t t2_count = task2.task_run_count_;
  int64_t t3_count = task3.task_run_count_;
  usleep(500000); // 500ms
  ASSERT_EQ(t2_count, task2.task_run_count_);
  ASSERT_EQ(t3_count, task3.task_run_count_);

  timer.stop();
  timer.wait();
  timer.destroy();
}

TEST_F(TestTimer, task_service_stop)
{
  TestTimerTask task1;
  task1.exec_time_ = 200000; // 200ms
  TestTimerTask task2;
  ObTimer timer1;
  ObTimer timer2;
  ASSERT_EQ(OB_SUCCESS, timer1.init());
  ASSERT_EQ(OB_SUCCESS, timer1.start());
  ASSERT_EQ(OB_SUCCESS, timer1.schedule(task1, 50000, true, true)); // delay=50ms, repeat=true, immediate=true
  ASSERT_EQ(OB_SUCCESS, timer2.init());
  ASSERT_EQ(OB_SUCCESS, timer2.start());
  ASSERT_EQ(OB_SUCCESS, timer2.schedule(task2, 500000, false, false)); // delay=500ms
  usleep(100000); // 100ms
  ASSERT_EQ(1, task1.task_run_count_);
  ASSERT_EQ(0, task2.task_run_count_);
  timer1.stop();
  timer1.wait();
  timer1.destroy();
  timer2.stop();
  timer2.wait();
  timer2.destroy();
}

TEST_F(TestTimer, task_run1_wait)
{
  TestTimerTask task1;
  TestTimerTask task2;
  TestTimerTask task3;
  task1.exec_time_ = 1000000; // 1s
  task2.exec_time_ = 10000;   // 10ms
  task3.exec_time_ = 10000;   // 10ms
  ObTimer timer1;
  ObTimer timer2;
  ASSERT_EQ(OB_SUCCESS, timer1.init());
  ASSERT_EQ(OB_SUCCESS, timer1.start());
  ASSERT_EQ(OB_SUCCESS, timer2.init());
  ASSERT_EQ(OB_SUCCESS, timer2.start());
  ASSERT_EQ(OB_SUCCESS, timer1.schedule(task1, 0, false, false));
  ASSERT_EQ(OB_SUCCESS, timer1.schedule(task2, 50000, false, false));
  ASSERT_EQ(OB_SUCCESS, timer2.schedule(task3, 200000, false, false)); // delay 200ms
  usleep(400000); // 400ms
  ASSERT_EQ(1, task3.task_run_count_); // ensure that task2 is not delayed by task1
  timer1.cancel_all();
  timer1.stop();
  timer1.wait();
  timer1.destroy();
  timer2.stop();
  timer2.wait();
  timer2.destroy();
}

TEST_F(TestTimer, schedule_after_stop)
{
  ObTimer timer;
  TestTimerTask task;
  task.exec_time_ = 10000; // 10ms
  ASSERT_EQ(OB_SUCCESS, timer.init());
  ASSERT_EQ(OB_SUCCESS, timer.start());
  ASSERT_EQ(OB_SUCCESS, timer.schedule(task, 0, false, false));
  usleep(100000); // 100ms
  ASSERT_EQ(1, task.task_run_count_);
  timer.stop();
  ASSERT_EQ(OB_CANCELED, timer.schedule(task, 0, false, false));
  timer.wait();
  timer.destroy();
}

TEST_F(TestTimer, without_start)
{
  // ensure that the timer still works without calling start
  TestTimerTask task;
  task.exec_time_ = 10000; // 10ms
  ObTimer timer;
  ASSERT_EQ(OB_SUCCESS, timer.init());
  ASSERT_EQ(OB_SUCCESS, timer.schedule(task, 0, false, false));
  usleep(50000); // 50ms
  ASSERT_EQ(1, task.task_run_count_);
  timer.stop();
  timer.wait();
  timer.destroy();
  // ensure that the timer still works after destroy and re-init
  ASSERT_EQ(OB_SUCCESS, timer.init());
  ASSERT_EQ(OB_SUCCESS, timer.schedule(task, 0, false, false));
  usleep(50000); // 50ms
  ASSERT_EQ(2, task.task_run_count_);
  timer.stop();
  timer.wait();
  timer.destroy();
}

} // end namespace common
} // end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_timer.log");
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_timer.log", true);

  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
