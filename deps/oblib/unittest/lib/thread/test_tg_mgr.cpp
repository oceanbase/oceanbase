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
#include "lib/thread/thread_mgr.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::lib;

class TestTimerTask : public ObTimerTask
{
public:
  TestTimerTask() : running_(false), task_run_count_(0) {}

  void runTimerTask()
  {
    running_ = true;
    ++task_run_count_;
    ::usleep(500000);
    running_ = false;
  }

  bool running_;
  int64_t task_run_count_;
};

TEST(TG, timer)
{
  int tg_id = TGDefIDs::TEST1;
  TestTimerTask task;
  // start
  ASSERT_EQ(OB_SUCCESS, TG_START(tg_id));
  ASSERT_EQ(OB_SUCCESS, TG_SCHEDULE(tg_id, task, 0, true));
  ::usleep(250000);
  ASSERT_TRUE(task.running_);
  ::usleep(750000);
  ASSERT_EQ(1, task.task_run_count_);
  ASSERT_EQ(OB_SUCCESS, TG_STOP_R(tg_id));
  TG_WAIT_ONLY(tg_id);
  ASSERT_EQ(OB_SUCCESS, TG_CANCEL_R(tg_id, task));
  ASSERT_EQ(OB_SUCCESS, TG_WAIT_R(tg_id));
  // TG_WAIT = wait + destroy
  ASSERT_EQ(OB_ERR_UNEXPECTED, TG_CANCEL_R(tg_id, task));

  // restart
  ASSERT_EQ(OB_SUCCESS, TG_START(tg_id));
  ASSERT_EQ(OB_SUCCESS, TG_SCHEDULE(tg_id, task, 0, true));
  ::usleep(250000);
  ASSERT_TRUE(task.running_);
  ::usleep(750000);
  ASSERT_EQ(2, task.task_run_count_);
  ASSERT_EQ(OB_SUCCESS, TG_STOP_R(tg_id));
  ASSERT_EQ(OB_SUCCESS, TG_WAIT_R(tg_id));

  ASSERT_TRUE(TG_EXIST(tg_id));
  TG_DESTROY(tg_id);
  ASSERT_FALSE(TG_EXIST(tg_id));
}

class Handler : public TGTaskHandler
{
public:
  void handle(void *task) override
  {
    UNUSED(task);
    ++handle_count_;
    ::usleep(50000);
  }

  int64_t handle_count_=0;
};

TEST(TG, queue_thread)
{
  int tg_id = TGDefIDs::TEST2;
  Handler handler;
  // start
  ASSERT_EQ(OB_SUCCESS, TG_SET_HANDLER(tg_id, handler));
  ASSERT_EQ(OB_SUCCESS, TG_START(tg_id));
  ASSERT_EQ(OB_SUCCESS, TG_PUSH_TASK(tg_id, &tg_id));
  ::usleep(50000);
  ASSERT_EQ(OB_SUCCESS, TG_STOP_R(tg_id));
  ASSERT_EQ(OB_SUCCESS, TG_WAIT_R(tg_id));
  ASSERT_EQ(1, handler.handle_count_);

  // restart
  ASSERT_EQ(OB_SUCCESS, TG_SET_HANDLER(tg_id, handler));
  ASSERT_EQ(OB_SUCCESS, TG_START(tg_id));
  ASSERT_EQ(OB_SUCCESS, TG_PUSH_TASK(tg_id, &tg_id));
  ::usleep(50000);
  ASSERT_EQ(OB_SUCCESS, TG_STOP_R(tg_id));
  ASSERT_EQ(OB_SUCCESS, TG_WAIT_R(tg_id));
  ASSERT_EQ(2, handler.handle_count_);

  ASSERT_TRUE(TG_EXIST(tg_id));
  TG_DESTROY(tg_id);
  ASSERT_FALSE(TG_EXIST(tg_id));
}

class MyDTask: public common::IObDedupTask
{
public:
  MyDTask() : common::IObDedupTask(common::T_BLOOMFILTER) {}
  virtual int64_t hash() const { return reinterpret_cast<int64_t>(this); }
  virtual bool operator ==(const IObDedupTask &task) const { return this == &task; }
  virtual int64_t get_deep_copy_size() const
  {
    return sizeof(*this);
  }
  virtual IObDedupTask *deep_copy(char *buffer, const int64_t buf_size) const
  {
    UNUSED(buf_size);
    return new (buffer) MyDTask;
  }
  virtual int64_t get_abs_expired_time() const {  return 0;  }
  virtual int process()
  {
    handle_count_++;
    return OB_SUCCESS;
  }
  TO_STRING_KV(K(""));
  static int64_t handle_count_;
};
int64_t MyDTask::handle_count_ = 0;

TEST(TG, dedup_queue)
{
  int tg_id = TGDefIDs::TEST3;
  MyDTask task;
  // start
  ASSERT_EQ(OB_SUCCESS, TG_START(tg_id));
  ASSERT_EQ(OB_SUCCESS, TG_PUSH_TASK(tg_id, task));
  ::usleep(50000);
  ASSERT_EQ(OB_SUCCESS, TG_STOP_R(tg_id));
  ASSERT_EQ(OB_SUCCESS, TG_WAIT_R(tg_id));
  ASSERT_EQ(1, task.handle_count_);

  // restart
  ASSERT_EQ(OB_SUCCESS, TG_START(tg_id));
  ASSERT_EQ(OB_SUCCESS, TG_PUSH_TASK(tg_id, task));
  ::usleep(50000);
  ASSERT_EQ(OB_SUCCESS, TG_STOP_R(tg_id));
  ASSERT_EQ(OB_SUCCESS, TG_WAIT_R(tg_id));
  ASSERT_EQ(2, task.handle_count_);

  ASSERT_TRUE(TG_EXIST(tg_id));
  TG_DESTROY(tg_id);
  ASSERT_FALSE(TG_EXIST(tg_id));
}

class MyRunnable : public TGRunnable
{
public:
  void run1() override
  {

    run_count_++;
    while (!has_set_stop()) {
      ::usleep(50000);
    }
  }
  int64_t run_count_=0;
};

TEST(TG, thread_pool)
{
  int tg_id = TGDefIDs::TEST4;
  MyRunnable runnable;
  // start
  ASSERT_EQ(OB_SUCCESS, TG_SET_RUNNABLE(tg_id, runnable));
  ASSERT_EQ(OB_SUCCESS, TG_START(tg_id));
  ::usleep(50000);
  ASSERT_EQ(OB_SUCCESS, TG_STOP_R(tg_id));
  ASSERT_EQ(OB_SUCCESS, TG_WAIT_R(tg_id));
  ASSERT_EQ(1, runnable.run_count_);

  // restart
  ASSERT_EQ(OB_SUCCESS, TG_SET_RUNNABLE(tg_id, runnable));
  ASSERT_EQ(OB_SUCCESS, TG_START(tg_id));
  ::usleep(50000);
  ASSERT_EQ(OB_SUCCESS, TG_STOP_R(tg_id));
  ASSERT_EQ(OB_SUCCESS, TG_WAIT_R(tg_id));
  ASSERT_EQ(2, runnable.run_count_);

  ASSERT_TRUE(TG_EXIST(tg_id));
  TG_DESTROY(tg_id);
  ASSERT_FALSE(TG_EXIST(tg_id));
}

TEST(TG, reentrant_thread_pool)
{
  int tg_id = TGDefIDs::TEST8;
  MyRunnable runnable;
  // start
  ASSERT_EQ(OB_SUCCESS, TG_SET_RUNNABLE(tg_id, runnable));
  ASSERT_EQ(OB_SUCCESS, TG_START(tg_id));
  ::usleep(50000);
  ASSERT_EQ(OB_SUCCESS, TG_REENTRANT_LOGICAL_START(tg_id));
  ::usleep(50000);
  TG_REENTRANT_LOGICAL_STOP(tg_id);
  ::usleep(50000);
  TG_REENTRANT_LOGICAL_WAIT(tg_id);
  ASSERT_EQ(1, runnable.run_count_);
  ASSERT_EQ(OB_SUCCESS, TG_REENTRANT_LOGICAL_START(tg_id));
  ::usleep(50000);
  ASSERT_EQ(OB_SUCCESS, TG_STOP_R(tg_id));
  ASSERT_EQ(OB_SUCCESS, TG_WAIT_R(tg_id));
  ASSERT_EQ(2, runnable.run_count_);
  // restart
  ASSERT_EQ(OB_SUCCESS, TG_SET_RUNNABLE(tg_id, runnable));
  ASSERT_EQ(OB_SUCCESS, TG_START(tg_id));
  ::usleep(50000);
  ASSERT_EQ(OB_SUCCESS, TG_STOP_R(tg_id));
  ASSERT_EQ(OB_SUCCESS, TG_WAIT_R(tg_id));
  ASSERT_EQ(2, runnable.run_count_);

  ASSERT_TRUE(TG_EXIST(tg_id));
  TG_DESTROY(tg_id);
  ASSERT_FALSE(TG_EXIST(tg_id));
}
class MyTask : public share::ObAsyncTask
{
public:
  virtual int process() override
  {
    handle_count_++;
    ::usleep(50000);
    return OB_SUCCESS;
  }

  virtual int64_t get_deep_copy_size() const override
  { return sizeof(*this); }

  virtual ObAsyncTask *deep_copy(char *buf, const int64_t buf_size) const override
  {
    UNUSED(buf_size);
    return new (buf) MyTask();
  }
  static int64_t handle_count_;
};
int64_t MyTask::handle_count_ = 0;

TEST(TG, async_task_queue)
{
  int tg_id = TGDefIDs::TEST5;
  MyTask task;
  // start
  ASSERT_EQ(OB_SUCCESS, TG_START(tg_id));
  ASSERT_EQ(OB_SUCCESS, TG_PUSH_TASK(tg_id, task));
  ::usleep(50000);
  ASSERT_EQ(OB_SUCCESS, TG_STOP_R(tg_id));
  ASSERT_EQ(OB_SUCCESS, TG_WAIT_R(tg_id));
  ASSERT_EQ(1, task.handle_count_);

  // restart
  ASSERT_EQ(OB_SUCCESS, TG_START(tg_id));
  ASSERT_EQ(OB_SUCCESS, TG_PUSH_TASK(tg_id, task));
  ::usleep(50000);
  ASSERT_EQ(OB_SUCCESS, TG_STOP_R(tg_id));
  ASSERT_EQ(OB_SUCCESS, TG_WAIT_R(tg_id));
  ASSERT_EQ(2, task.handle_count_);

  ASSERT_TRUE(TG_EXIST(tg_id));
  TG_DESTROY(tg_id);
  ASSERT_FALSE(TG_EXIST(tg_id));
}

class MapQueueThreadHandler : public TGTaskHandler
{
public:
  void handle(void *task) override
  {}
  void handle(void *task, volatile bool &is_stopped) override
  {
    UNUSED(task);
    UNUSED(is_stopped);
    ++handle_count_;
    ::usleep(50000);
  }

  int64_t handle_count_ = 0;
};

TEST(TG, map_queue_thread)
{
  int tg_id = TGDefIDs::TEST6;
  MapQueueThreadHandler handler;
  // start
  ASSERT_EQ(OB_SUCCESS, TG_SET_HANDLER(tg_id, handler));
  ASSERT_EQ(OB_SUCCESS, TG_START(tg_id));
  ASSERT_EQ(OB_SUCCESS, TG_PUSH_TASK(tg_id, &tg_id, 0));
  ::usleep(50000);
  ASSERT_EQ(OB_SUCCESS, TG_STOP_R(tg_id));
  ASSERT_EQ(OB_SUCCESS, TG_WAIT_R(tg_id));
  ASSERT_EQ(1, handler.handle_count_);

  // restart
  ASSERT_EQ(OB_SUCCESS, TG_SET_HANDLER(tg_id, handler));
  ASSERT_EQ(OB_SUCCESS, TG_START(tg_id));
  ASSERT_EQ(OB_SUCCESS, TG_PUSH_TASK(tg_id, &tg_id, 1));
  ::usleep(50000);
  ASSERT_EQ(OB_SUCCESS, TG_STOP_R(tg_id));
  ASSERT_EQ(OB_SUCCESS, TG_WAIT_R(tg_id));
  ASSERT_EQ(2, handler.handle_count_);

  ASSERT_TRUE(TG_EXIST(tg_id));
  TG_DESTROY(tg_id);
  ASSERT_FALSE(TG_EXIST(tg_id));
}

int main(int argc, char *argv[])
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_tg_mgr.log", true);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
