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
#include "ob_concurrent_seq_queue.h"

#include "share/ob_define.h"
#include "lib/time/ob_time_utility.h"
#include "lib/atomic/ob_atomic.h"

namespace oceanbase
{
namespace common
{
class TestConSeqQueue : public ::testing::Test
{
public:
  static const int64_t RUN_TIME = 1L * 60L * 60L * 1000L * 1000L;
  static const int64_t THREAD_NUM = 20;
  static const int64_t STAT_INTERVAL = 5 * 1000 * 1000;
public:
  TestConSeqQueue() {}
  ~TestConSeqQueue() {}

  virtual void SetUp()
  {
    ASSERT_EQ(0, queue_.init(1024, ObMemAttr(OB_SERVER_TENANT_ID, ObNewModIds::TEST)));
    produce_seq_ = 0;
    consume_seq_ = 0;
    consume_thread_counter_ = 0;
    consume_task_count_ = 0;
    last_stat_time_ = 0;
    last_consume_task_count_ = 0;
    stop_flag_ = false;
  }
  virtual void TearDown()
  {
    queue_.destroy();
  }
  static void *produce_thread_func(void *args);
  static void *consume_thread_func(void *args);
  void run_produce();
  void run_consume();

public:
  pthread_t produce_threads_[THREAD_NUM];
  pthread_t consume_threads_[THREAD_NUM];
  int64_t consume_thread_counter_;
  ObConcurrentSeqQueue queue_;
  int64_t produce_seq_ CACHE_ALIGNED;
  int64_t consume_seq_ CACHE_ALIGNED;
  int64_t consume_task_count_ CACHE_ALIGNED;
  int64_t last_consume_task_count_ CACHE_ALIGNED;
  int64_t last_stat_time_ CACHE_ALIGNED;

  volatile bool stop_flag_ CACHE_ALIGNED;
};

TEST_F(TestConSeqQueue, basic)
{
  ObConcurrentSeqQueue queue;
  void *data = 0;

  EXPECT_EQ(0, queue.init(1024, ObMemAttr(OB_SERVER_TENANT_ID, ObNewModIds::TEST)));

  EXPECT_EQ(0, queue.push((void*)0, 0, 0));
  EXPECT_EQ(0, queue.push((void*)1, 1, 0));
  EXPECT_EQ(0, queue.push((void*)2, 2, 0));

  EXPECT_EQ(0, queue.pop(data, 0, 0));
  EXPECT_EQ(0, (int64_t)data);
  EXPECT_EQ(0, queue.pop(data, 1, 0));
  EXPECT_EQ(1, (int64_t)data);
  EXPECT_EQ(0, queue.pop(data, 2, 0));
  EXPECT_EQ(2, (int64_t)data);

  // Failed to push and pop elements with the same serial number again
  EXPECT_NE(0, queue.push((void*)0, 0, 0));
  EXPECT_NE(0, queue.push((void*)1, 1, 0));
  EXPECT_NE(0, queue.push((void*)2, 2, 0));
  EXPECT_NE(0, queue.pop(data, 0, 0));
  EXPECT_NE(0, queue.pop(data, 1, 0));
  EXPECT_NE(0, queue.pop(data, 2, 0));
}

void *TestConSeqQueue::produce_thread_func(void *args)
{
  if (NULL != args) {
    ((TestConSeqQueue *)args)->run_produce();
  }

  return NULL;
}

void TestConSeqQueue::run_produce()
{
  int ret = OB_SUCCESS;
  int64_t batch_count = 1000;

  while (OB_SUCCESS == ret && ! stop_flag_) {
    for (int64_t index = 0; OB_SUCCESS == ret && index < batch_count; index++) {
      int64_t seq = ATOMIC_FAA(&produce_seq_, 1);
      while (! stop_flag_ && OB_TIMEOUT == (ret = queue_.push((void*)seq, seq, 1 * 1000 * 1000)));
      if (! stop_flag_) {
        EXPECT_EQ(OB_SUCCESS, ret);
      }
    }
  }
}

void *TestConSeqQueue::consume_thread_func(void *args)
{
  if (NULL != args) {
    ((TestConSeqQueue *)args)->run_consume();
  }

  return NULL;
}

void TestConSeqQueue::run_consume()
{
  int ret = OB_SUCCESS;
  int64_t batch_count = 1000;
  int64_t end_time = ObTimeUtility::current_time();

  int64_t thread_index = ATOMIC_FAA(&consume_thread_counter_, 0);

  while (OB_SUCCESS == ret && !stop_flag_) {
    for (int64_t index = 0; OB_SUCCESS == ret && index < batch_count; index++) {
      int64_t seq = ATOMIC_FAA(&consume_seq_, 1);
      void *data = NULL;
      while (! stop_flag_ && OB_TIMEOUT == (ret = queue_.pop(data, seq, 1 * 1000 * 1000)));
      if (! stop_flag_) {
        EXPECT_EQ(OB_SUCCESS, ret);
        EXPECT_EQ(seq, (int64_t)data);
        ATOMIC_INC(&consume_task_count_);
      }
    }

    int64_t cur_time = ObTimeUtility::current_time();
    if (OB_UNLIKELY(0 == thread_index) && cur_time - last_stat_time_ > STAT_INTERVAL) {
      int64_t task_count = ATOMIC_LOAD(&consume_task_count_);
      int64_t consume_seq = ATOMIC_LOAD(&consume_seq_);
      int64_t produce_seq = ATOMIC_LOAD(&produce_seq_);
      if (0 != last_stat_time_) {
        int64_t delta_task_count = task_count - last_consume_task_count_;
        int64_t delta_time_sec = (cur_time - last_stat_time_)/1000000;
        LIB_LOG(INFO, "STAT", "POP_TPS", delta_task_count/delta_time_sec, K(delta_task_count),
            K(consume_seq), K(produce_seq), K(INT32_MAX));
      }

      last_stat_time_ = cur_time;
      last_consume_task_count_ = task_count;
    }

    if (end_time - cur_time <= 0) {
      stop_flag_ = true;
    }
  }
}

TEST_F(TestConSeqQueue, thread)
{
  for (int64_t index = 0; index < THREAD_NUM; index++) {
    ASSERT_EQ(0, pthread_create(produce_threads_ + index, NULL, produce_thread_func, this));
  }
  for (int64_t index = 0; index < THREAD_NUM; index++) {
    ASSERT_EQ(0, pthread_create(consume_threads_ + index, NULL, consume_thread_func, this));
  }
  for (int64_t index = 0; index < THREAD_NUM; index++) {
    pthread_join(produce_threads_[index], NULL);
    produce_threads_[index] = 0;
  }
  for (int64_t index = 0; index < THREAD_NUM; index++) {
    pthread_join(consume_threads_[index], NULL);
    consume_threads_[index] = 0;
  }
}

}
}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
