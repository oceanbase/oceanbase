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

class TaskCommon : public ObTimerTask
{
public:
  TaskCommon()
    : task_run_count_(0)
  {}
  void runTimerTask()
  {
    ++task_run_count_;
    ::usleep(exec_time_);
  }

  int64_t task_run_count_;
  int64_t exec_time_ = 100000; // 100ms
};

class TaskCancelSelf : public ObTimerTask
{
public:
  TaskCancelSelf(ObTimer &t)
    : task_run_count_(0), timer_(t)
  {}
  void runTimerTask()
  {
    int ret = OB_SUCCESS;
    ::usleep(20000); // 20ms
    if (OB_FAIL(timer_.cancel_task(*this))) {
      fprintf(stderr, "[%s: %d] call cancel_task failed, ret=%d\n", __FUNCTION__, __LINE__, ret);
      return;
    }
    ++task_run_count_; // call cancel first, then self-increment
    ::usleep(20000); // 20ms
  }

  int64_t task_run_count_;
  ObTimer &timer_;
};

class TaskRescheduleAndCancel : public ObTimerTask
{
public:
  TaskRescheduleAndCancel(ObTimer &t)
    : task_run_count_(0), timer_(t)
  {}
  void runTimerTask()
  {
    int ret = OB_SUCCESS;
    ++task_run_count_;
    ::usleep(20000); // 20ms
    if (OB_FAIL(timer_.schedule(*this, 0, false))) {  // repeate = false
      fprintf(stderr, "[%s: %d] call schedule failed, ret=%d\n", __FUNCTION__, __LINE__, ret);
    } else if (OB_FAIL(timer_.cancel_task(*this))) { // both cancel the running one
                                                     // and the re-scheduled one
      fprintf(stderr, "[%s: %d] call cancel_task failed, ret=%d\n", __FUNCTION__, __LINE__, ret);
    }
    ::usleep(20000); // 20ms
  }

  int64_t task_run_count_;
  ObTimer &timer_;
};

// case1: cancel the task immediately
TEST(TestCancelTask, cancel_immediately)
{
  ObTimer timer;
  ASSERT_EQ(OB_SUCCESS, timer.init());
  ASSERT_EQ(OB_SUCCESS, timer.start());
  TaskCommon task;
  TaskCommon task_another;
  task_another.exec_time_ = 20000; // 20ms
  ASSERT_EQ(OB_SUCCESS, timer.schedule(task, 50000, true));  // delay = 50ms
  ASSERT_EQ(OB_SUCCESS, timer.schedule(task_another, 50000, false));
  ASSERT_EQ(2, timer.get_tasks_num());
  ASSERT_EQ(OB_SUCCESS, timer.cancel(task)); // cancel it
  ASSERT_EQ(1, timer.get_tasks_num()); // cancel task, do not affect task_another
  ASSERT_EQ(OB_SUCCESS, timer.cancel(task)); // test duplicate cancel
  timer.wait_task(task);
  timer.wait_task(task_another);
  ASSERT_EQ(0, task.task_run_count_);
  ASSERT_EQ(1, task_another.task_run_count_);
  timer.destroy();
}

// case2: cancel the running task
TEST(TestCancelTask, cancel_running)
{
  ObTimer timer;
  ASSERT_EQ(OB_SUCCESS, timer.init());
  ASSERT_EQ(OB_SUCCESS, timer.start());
  TaskCommon task;
  task.exec_time_ = 300000; // 300ms
  ASSERT_EQ(OB_SUCCESS, timer.schedule(task, 50000, true)); // repeate = true , delay = 50ms
  ASSERT_EQ(1, timer.get_tasks_num());
  ::usleep(150000);
  ASSERT_EQ(1, task.task_run_count_); // task is running
  // the running task has been removed from the task array
  ASSERT_EQ(0, timer.get_tasks_num());
  ASSERT_EQ(OB_SUCCESS, timer.cancel(task)); // cancel it
  timer.wait_task(task);
  ASSERT_EQ(1, task.task_run_count_); // the repeat task has been canceled.
  timer.destroy();
}

// case3: cancel the non-running task
TEST(TestCancelTask, cancel_non_running)
{
  ObTimer timer;
  ASSERT_EQ(OB_SUCCESS, timer.init());
  ASSERT_EQ(OB_SUCCESS, timer.start());
  TaskCommon task1;
  task1.exec_time_ = 300000; // 300ms
  TaskCommon task2;
  task2.exec_time_ = 50000; //50ms
  ASSERT_EQ(OB_SUCCESS, timer.schedule(task1, 50000, false));  // t1
  ASSERT_EQ(OB_SUCCESS, timer.schedule(task2, 400000, true));  // t2
  ASSERT_EQ(OB_SUCCESS, timer.schedule(task2, 500000, true));  // t3
  ASSERT_EQ(OB_SUCCESS, timer.schedule(task2, 600000, true));  // t4
  ASSERT_EQ(4, timer.get_tasks_num()); // 4 tasks were scheduled
  ::usleep(150000);
  ASSERT_EQ(1, task1.task_run_count_); // t1 is running
  // t1 (i.e., the running task) has been removed from the task array
  ASSERT_EQ(3, timer.get_tasks_num());
  ASSERT_EQ(OB_SUCCESS, timer.cancel(task2)); // cancel task2 (i.e., t2, t3, t4)
  ASSERT_EQ(0, timer.get_tasks_num()); // no tasks in task array
  // task2 did not run once, because all scheduling for it has been canceled
  ASSERT_EQ(0, task2.task_run_count_);
  ASSERT_EQ(OB_SUCCESS, timer.cancel(task2)); // test duplicate cancel
  timer.wait_task(task1);
  timer.wait_task(task2);
  timer.destroy();
}

// case4: cancel self
TEST(TestCancelTask, cancel_self)
{
  ObTimer timer;
  ASSERT_EQ(OB_SUCCESS, timer.init());
  ASSERT_TRUE(timer.inited());
  TaskCancelSelf task(timer);
  ASSERT_EQ(OB_SUCCESS, timer.start());
  ASSERT_EQ(OB_SUCCESS, timer.schedule(task, 10000, true)); // repeate = true
  timer.wait_task(task);
  // when canceling a running task, it can't be stopped immediately,
  // but has to wait until the end of the current round.
  ASSERT_EQ(1, task.task_run_count_);
  timer.destroy();
}

// case5: re-schedule slef, then cancel
TEST(TestCancelTask, reschedule_self_and_cancel)
{
  ObTimer timer;
  ASSERT_EQ(OB_SUCCESS, timer.init());
  ASSERT_TRUE(timer.inited());
  TaskRescheduleAndCancel task(timer);
  ASSERT_EQ(OB_SUCCESS, timer.start());
  ASSERT_EQ(OB_SUCCESS, timer.schedule(task, 0));
  timer.wait_task(task);
  ASSERT_EQ(task.task_run_count_, 1);
  timer.destroy();
}

// case6: cancel-->wait_task
TEST(TestCancelTask, cancel_and_wait_task)
{
  ObTimer timer;
  ASSERT_EQ(OB_SUCCESS, timer.init());
  ASSERT_TRUE(timer.inited());
  ASSERT_EQ(OB_SUCCESS, timer.start());

  TaskCommon task;
  task.exec_time_ = 10000; // 10ms
  ASSERT_EQ(OB_SUCCESS, timer.schedule(task, 500000, true)); // 500ms true
  ::usleep(800000); // 800ms
  ASSERT_EQ(1, task.task_run_count_);
  timer.cancel_task(task);
  timer.wait_task(task);
  ::usleep(1000000); // 1s
  ASSERT_EQ(1, task.task_run_count_);
  timer.destroy();
}

} // end namespace common
} // end namespace oceanbase

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
