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
#include "common/ob_partition_key.h"    // ObPartitionKey
#include "lib/queue/ob_ext_ms_queue.h"  // ObExtMsQueue

#define DEFAULT_LOG_LEVEL "INFO"
#define DEFAULT_LOG_FILE "test_ext_ms_queue.log"

using namespace oceanbase::common;

class ExtMsQueueTest : public ::testing::Test {
public:
  static const int64_t KEY_COUNT = 10;
  static const int64_t MAX_CACHED_MS_QUEUE_ITEM_COUNT = KEY_COUNT;
  static const int64_t MSQ_QUEUE_COUNT = 4;
  static const int64_t MSQ_QUEUE_LEN = 1024;
  static const int64_t PRODUCER_NUM = 4;
  static const int64_t CONSUMER_NUM = 4;
  static const int64_t RUN_TIME_SEC = 5;
  static const int64_t OP_TIMEOUT = 100 * 1000;
  static const int64_t WAIT_TIME = 100;

public:
  typedef ObPartitionKey KeyType;
  typedef ObLink TaskType;

  struct KeyValue {
    KeyType key_;
    int64_t seq_;

    KeyValue() : key_(), seq_(0)
    {}
    ~KeyValue()
    {}

    void reset()
    {
      key_.reset();
      seq_ = 0;
    }
  };

  static void* produce_func(void* args);
  static void* consume_func(void* args);

  int start_consumers_();
  int start_producers_();
  void wait_producers_();
  void wait_consumers_();
  int terminate_all_ms_queue_();

public:
  ExtMsQueueTest();
  virtual ~ExtMsQueueTest();
  virtual void SetUp();
  virtual void TearDown();

public:
  int64_t end_time_;
  KeyValue kv_[KEY_COUNT];
  TaskType task_[MSQ_QUEUE_COUNT];  // every Queue in MsQueue corresponds to a Task
  pthread_t producers_[PRODUCER_NUM];
  pthread_t consumers_[CONSUMER_NUM];
  int64_t consumer_thread_index_counter_;
  int64_t producer_thread_index_counter_;
  ObExtMsQueue<KeyType> ext_ms_queue_;

private:
  DISALLOW_COPY_AND_ASSIGN(ExtMsQueueTest);
};

ExtMsQueueTest::ExtMsQueueTest()
{}

ExtMsQueueTest::~ExtMsQueueTest()
{}

void ExtMsQueueTest::SetUp()
{
  char* run_time_sec_str = getenv("run_time_sec");
  int64_t run_time_sec = NULL == run_time_sec_str ? 0 : strtoll(run_time_sec_str, NULL, 10);
  run_time_sec = run_time_sec <= 0 ? RUN_TIME_SEC : run_time_sec;

  _LIB_LOG(INFO, "run_time_sec = %ld sec", run_time_sec);

  (void)memset(producers_, 0, sizeof(producers_));
  (void)memset(consumers_, 0, sizeof(consumers_));

  srandom((unsigned int)ObTimeUtility::current_time());

  end_time_ = ObTimeUtility::current_time() + run_time_sec * 1000 * 1000;

  consumer_thread_index_counter_ = 0;
  producer_thread_index_counter_ = 0;
}

void ExtMsQueueTest::TearDown()
{
  (void)memset(producers_, 0, sizeof(producers_));
  (void)memset(consumers_, 0, sizeof(consumers_));
}

void* ExtMsQueueTest::produce_func(void* args)
{
  int ret = OB_SUCCESS;
  ExtMsQueueTest* test = (ExtMsQueueTest*)args;

  if (NULL != test) {
    int64_t end_time = test->end_time_;
    int64_t thread_index = ATOMIC_FAA(&test->producer_thread_index_counter_, 1);
    ObExtMsQueue<KeyType>& queue = test->ext_ms_queue_;
    LIB_LOG(INFO, "producer thread started", K(thread_index));

    while (OB_SUCC(ret)) {
      int key_index = (int)(random() % KEY_COUNT);
      // every transaction push atmost one task to every Queue in MsQueue because of share Task global
      int64_t stmt_count = (random() % MSQ_QUEUE_COUNT) + 1;
      int64_t seq = ATOMIC_FAA(&(test->kv_[key_index].seq_), 1);
      KeyType& key = test->kv_[key_index].key_;

      for (int64_t index = 0; OB_SUCC(ret) && index < stmt_count; index++) {
        TaskType* task = test->task_ + index;  // push Task

        while (true) {
          ret = queue.push(key, task, seq, index, OP_TIMEOUT);

          if (OB_TIMEOUT != ret) {
            if (OB_FAIL(ret)) {
              LIB_LOG(ERROR, "producer push queue fail", K(ret), K(key), K(seq), K(index));
            }
            break;
          }
        }

        EXPECT_EQ(OB_SUCCESS, ret);
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(queue.end_batch(key, seq, stmt_count))) {
          LIB_LOG(ERROR, "producer end_batch fail", K(ret), K(key), K(seq));
        }

        EXPECT_EQ(OB_SUCCESS, ret);
      }

      this_routine::usleep(WAIT_TIME);

      if (ObTimeUtility::current_time() >= end_time) {
        break;
      }
    }

    LIB_LOG(INFO, "producer thread stoped", K(thread_index));
  }

  return NULL;
}

void* ExtMsQueueTest::consume_func(void* args)
{
  int ret = OB_SUCCESS;
  ExtMsQueueTest* test = (ExtMsQueueTest*)args;

  if (NULL != test) {
    int64_t thread_index = ATOMIC_FAA(&test->consumer_thread_index_counter_, 1);
    ObExtMsQueue<KeyType>& queue = test->ext_ms_queue_;
    LIB_LOG(INFO, "consumer thread started", K(thread_index), "ms_queue_count", queue.get_ms_queue_count());

    TaskType* task = NULL;
    void* ctx = NULL;
    while (OB_SUCCESS == ret && queue.get_ms_queue_count() > 0) {
      while (true) {
        ret = queue.get(task, ctx, OP_TIMEOUT);
        if (OB_TIMEOUT != ret) {
          if (OB_FAIL(ret)) {
            LIB_LOG(ERROR, "get task from queue fail", K(ret));
          }
          break;
        }

        if (queue.get_ms_queue_count() <= 0) {
          ret = OB_SUCCESS;
          break;
        }
      }

      EXPECT_EQ(OB_SUCCESS, ret);
    }

    LIB_LOG(INFO, "consumer thread stoped", K(thread_index), "ms_queue_count", queue.get_ms_queue_count());
  }
  return NULL;
}

void ExtMsQueueTest::wait_producers_()
{
  LIB_LOG(INFO, "wait producers");

  for (int64_t index = 0; index < PRODUCER_NUM; index++) {
    if (0 != producers_[index]) {
      pthread_join(producers_[index], NULL);
    }

    producers_[index] = 0;
  }
}

void ExtMsQueueTest::wait_consumers_()
{
  LIB_LOG(INFO, "wait consumers");

  for (int64_t index = 0; index < CONSUMER_NUM; index++) {
    if (0 != consumers_[index]) {
      pthread_join(consumers_[index], NULL);
    }

    consumers_[index] = 0;
  }
}

int ExtMsQueueTest::start_consumers_()
{
  int ret = OB_SUCCESS;
  for (int64_t index = 0; index < CONSUMER_NUM; index++) {
    pthread_create(consumers_ + index, NULL, consume_func, this);
  }
  return ret;
}

int ExtMsQueueTest::start_producers_()
{
  int ret = OB_SUCCESS;
  for (int64_t index = 0; index < PRODUCER_NUM; index++) {
    pthread_create(producers_ + index, NULL, produce_func, this);
  }
  return ret;
}

int ExtMsQueueTest::terminate_all_ms_queue_()
{
  int ret = OB_SUCCESS;
  LIB_LOG(INFO, "terminate_all_ms_queue");
  for (int64_t index = 0; OB_SUCC(ret) && index < KEY_COUNT; index++) {
    KeyType& key = kv_[index].key_;
    int64_t end_seq = kv_[index].seq_;
    LIB_LOG(DEBUG, "terminate_ms_queue", K(key), K(end_seq));

    while (true) {
      ret = ext_ms_queue_.terminate_ms_queue(key, end_seq, OP_TIMEOUT);

      if (OB_TIMEOUT != ret) {
        if (OB_FAIL(ret)) {
          LIB_LOG(ERROR, "terminate_ms_queue fail", K(ret), K(key), K(end_seq));
        }
        break;
      }
    }
  }

  return ret;
}

TEST_F(ExtMsQueueTest, basic_test)
{
  // init ExtMsQueue
  EXPECT_EQ(OB_SUCCESS, ext_ms_queue_.init(MAX_CACHED_MS_QUEUE_ITEM_COUNT, MSQ_QUEUE_COUNT, MSQ_QUEUE_LEN));

  // add MSQueue
  for (int64_t index = 0; index < KEY_COUNT; index++) {
    kv_[index].reset();
    ASSERT_EQ(OB_SUCCESS, kv_[index].key_.init(index, 0, 1));

    EXPECT_EQ(OB_SUCCESS, ext_ms_queue_.add_ms_queue(kv_[index].key_));
  }

  // add duplicated key
  EXPECT_EQ(OB_ENTRY_EXIST, ext_ms_queue_.add_ms_queue(kv_[0].key_));

  // start consumers
  EXPECT_EQ(OB_SUCCESS, start_consumers_());

  // start produces
  EXPECT_EQ(OB_SUCCESS, start_producers_());

  // wait all producers exit
  wait_producers_();

  // stop all MsQueue
  EXPECT_EQ(OB_SUCCESS, terminate_all_ms_queue_());

  // wait all consumers exit
  wait_consumers_();

  ext_ms_queue_.destroy();
}

int main(int argc, char** argv)
{
  const char* log_level = getenv("log_level");
  const char* log_file = getenv("log_file");
  log_level = (NULL == log_level ? DEFAULT_LOG_LEVEL : log_level);
  log_file = (NULL == log_file) ? DEFAULT_LOG_FILE : log_file;

  OB_LOGGER.set_log_level(log_level);
  OB_LOGGER.set_file_name(log_file, true);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
