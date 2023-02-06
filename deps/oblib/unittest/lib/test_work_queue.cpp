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

#include "lib/thread/ob_work_queue.h"
#include <gtest/gtest.h>
#include "lib/utility/ob_test_util.h"
using namespace oceanbase::common;
class TestWorkQueue: public ::testing::Test
{
public:
  TestWorkQueue();
  virtual ~TestWorkQueue();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestWorkQueue);
protected:
  // function members
protected:
  // data members
};

TestWorkQueue::TestWorkQueue()
{
}

TestWorkQueue::~TestWorkQueue()
{
}

void TestWorkQueue::SetUp()
{
}

void TestWorkQueue::TearDown()
{
}

TEST_F(TestWorkQueue, init)
{
  ObWorkQueue wqueue;
  ASSERT_EQ(OB_INVALID_ARGUMENT, wqueue.init(4, 1000));
  ASSERT_EQ(OB_SUCCESS, wqueue.init(4, 1024));
  ASSERT_EQ(OB_INIT_TWICE, wqueue.init(4, 1024));
  ASSERT_EQ(OB_SUCCESS, wqueue.start());
  ASSERT_EQ(OB_SUCCESS, wqueue.stop());
  ASSERT_EQ(OB_SUCCESS, wqueue.wait());
}

class ATimerTask: public ObAsyncTimerTask
{
public:
  ATimerTask(ObWorkQueue &queue, bool fail_it = false)
      :ObAsyncTimerTask(queue),
       fail_it_(fail_it)
  {
    set_retry_interval(0);
    set_retry_times(1);
  }
  virtual ~ATimerTask()
  {}
  virtual int process() override
  {
    int ret = OB_SUCCESS;
    ATOMIC_INC(&process_count_);
    if (fail_it_) {
      OB_LOG(INFO, "test fail and retry", K_(process_count));
      ret = OB_INVALID_ARGUMENT;
      fail_it_ = false;
    } else {
      OB_LOG(INFO, "process", K_(process_count));
    }
    return ret;
  }
  virtual int64_t get_deep_copy_size() const override
  {
    return sizeof(*this);
  }
  virtual ObAsyncTask *deep_copy(char *buf, const int64_t buf_size) const override
  {
    int ret = 0;
    ObAsyncTask *task = NULL;
    if (buf == NULL || buf_size < sizeof(*this)) {
      OB_LOG(ERROR, "invalid argument");
    } else {
      task = new(buf) ATimerTask(work_queue_, fail_it_);
    }
    return task;
  }
  static int64_t get_process_count() { return process_count_; }
  static void clear_process_count() { process_count_ = 0; }
private:
  // types and constants
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ATimerTask);
  // function members
private:
  // data members
  static int64_t process_count_;
  bool fail_it_;
};
int64_t ATimerTask::process_count_ = 0;

TEST_F(TestWorkQueue, async_task)
{
  ObWorkQueue wqueue;
  ASSERT_EQ(OB_INVALID_ARGUMENT, wqueue.init(4, 1000));
  ASSERT_EQ(OB_SUCCESS, wqueue.init(4, 1024));
  ASSERT_EQ(OB_INIT_TWICE, wqueue.init(4, 1024));
  ASSERT_EQ(OB_SUCCESS, wqueue.start());
  ATimerTask task1(wqueue);
  ATimerTask::clear_process_count();
  for (int64_t i = 0; i < 16; ++i)
  {
    ASSERT_EQ(OB_SUCCESS, wqueue.add_async_task(task1));
  }
  sleep(3);
  ASSERT_EQ(16, task1.get_process_count());

  ASSERT_EQ(OB_SUCCESS, wqueue.stop());
  ASSERT_EQ(OB_SUCCESS, wqueue.wait());
}

TEST_F(TestWorkQueue, on_shoot_timer_task)
{
  ObWorkQueue wqueue;
  ASSERT_EQ(OB_INVALID_ARGUMENT, wqueue.init(4, 1000));
  ASSERT_EQ(OB_SUCCESS, wqueue.init(4, 1024));
  ASSERT_EQ(OB_INIT_TWICE, wqueue.init(4, 1024));
  ASSERT_EQ(OB_SUCCESS, wqueue.start());
  ATimerTask task1(wqueue);
  ATimerTask::clear_process_count();
  for (int64_t i = 0; i < 16; ++i)
  {
    ASSERT_EQ(OB_SUCCESS, wqueue.add_timer_task(task1, 2*1000*1000, false));
  }
  OB_LOG(INFO, "before sleep 1");
  sleep(1);
  OB_LOG(INFO, "after sleep 1");
  ASSERT_EQ(0, task1.get_process_count());
  OB_LOG(INFO, "before sleep 2");
  sleep(2);
  OB_LOG(INFO, "after sleep 2");
  ASSERT_EQ(16, task1.get_process_count());

  ASSERT_EQ(OB_SUCCESS, wqueue.stop());
  ASSERT_EQ(OB_SUCCESS, wqueue.wait());
}

TEST_F(TestWorkQueue, repeat_timer_task)
{
  ObWorkQueue wqueue;
  ASSERT_EQ(OB_INVALID_ARGUMENT, wqueue.init(4, 1000));
  ASSERT_EQ(OB_SUCCESS, wqueue.init(4, 1024));
  ASSERT_EQ(OB_INIT_TWICE, wqueue.init(4, 1024));
  ASSERT_EQ(OB_SUCCESS, wqueue.start());
  ATimerTask task1(wqueue);
  ATimerTask::clear_process_count();
  for (int64_t i = 0; i < 16; ++i)
  {
    ASSERT_EQ(OB_SUCCESS, wqueue.add_timer_task(task1, 2*1000*1000, true));
  }
  OB_LOG(INFO, "before sleep 1");
  sleep(1);
  OB_LOG(INFO, "after sleep 1");
  ASSERT_EQ(0, task1.get_process_count());
  OB_LOG(INFO, "before sleep 2");
  sleep(2);
  OB_LOG(INFO, "after sleep 2");
  ASSERT_EQ(16, task1.get_process_count());
  sleep(2);
  OB_LOG(INFO, "sleep 2");
  ASSERT_EQ(32, task1.get_process_count());

  ASSERT_EQ(OB_SUCCESS, wqueue.stop());
  ASSERT_EQ(OB_SUCCESS, wqueue.wait());
}

TEST_F(TestWorkQueue, retry_task)
{
  ObWorkQueue wqueue;
  ASSERT_EQ(OB_INVALID_ARGUMENT, wqueue.init(4, 1000));
  ASSERT_EQ(OB_SUCCESS, wqueue.init(4, 1024));
  ASSERT_EQ(OB_INIT_TWICE, wqueue.init(4, 1024));
  ASSERT_EQ(OB_SUCCESS, wqueue.start());
  ATimerTask task1(wqueue, true);
  ATimerTask::clear_process_count();
  for (int64_t i = 0; i < 16; ++i)
  {
    ASSERT_EQ(OB_SUCCESS, wqueue.add_timer_task(task1, 2*1000*1000, false));
  }
  OB_LOG(INFO, "before sleep 1");
  sleep(1);
  OB_LOG(INFO, "after sleep 1");
  ASSERT_EQ(0, task1.get_process_count());
  OB_LOG(INFO, "before sleep 2");
  sleep(2);
  OB_LOG(INFO, "after sleep 2");
  ASSERT_EQ(32, task1.get_process_count());

  ASSERT_EQ(OB_SUCCESS, wqueue.stop());
  ASSERT_EQ(OB_SUCCESS, wqueue.wait());
}

TEST_F(TestWorkQueue, immediate_task)
{
  ObWorkQueue wqueue;
  ASSERT_EQ(OB_INVALID_ARGUMENT, wqueue.init(4, 1000));
  ASSERT_EQ(OB_SUCCESS, wqueue.init(4, 1024));
  ASSERT_EQ(OB_INIT_TWICE, wqueue.init(4, 1024));
  ASSERT_EQ(OB_SUCCESS, wqueue.start());
  ATimerTask task1(wqueue);
  ATimerTask::clear_process_count();
  for (int64_t i = 0; i < 16; ++i)
  {
    ASSERT_EQ(OB_SUCCESS, wqueue.add_repeat_timer_task_schedule_immediately(task1, 2*1000*1000));
  }
  OB_LOG(INFO, "before sleep 1");
  sleep(1);
  OB_LOG(INFO, "after sleep 1");
  ASSERT_EQ(16, task1.get_process_count());
  OB_LOG(INFO, "before sleep 2");
  sleep(2);
  OB_LOG(INFO, "after sleep 2");
  ASSERT_EQ(32, task1.get_process_count());

  ASSERT_EQ(OB_SUCCESS, wqueue.stop());
  ASSERT_EQ(OB_SUCCESS, wqueue.wait());
}

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
