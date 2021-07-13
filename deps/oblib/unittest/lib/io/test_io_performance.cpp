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
#include "test_io_performance.h"

using namespace oceanbase::common;
using namespace oceanbase::lib;

namespace oceanbase {
namespace unittest {

class TestIOPerformance : public ::testing::Test {
public:
  virtual void SetUp()
  {
    COMMON_LOG(INFO, "start set up");
    ObIOManager& io_manager = ObIOManager::get_instance();
    // io_manager.destroy();
    io_manager.init();
  }

  virtual void TearDown()
  {
    ObIOManager& io_manager = ObIOManager::get_instance();
    io_manager.destroy();
  }
};

TEST_F(TestIOPerformance, single)
{
  IOStress stress;
  ASSERT_EQ(OB_SUCCESS, stress.init("stress.config"));
  ASSERT_EQ(OB_SUCCESS, stress.run());
}

class MPSC_ThreadPool : public lib::ThreadPool {
public:
  MPSC_ThreadPool() : inited_(false), is_lighty_(true)
  {}
  virtual ~MPSC_ThreadPool()
  {}
  int init(const int64_t queue_length);
  virtual void run1();
  static const int64_t PRODUCER_COUNT = 16;
  static const int64_t CONSUMER_COUNT = 1;
  static const int64_t PRODUCE_INTERVAL_US = 100;

private:
  int produce();
  int consume();

private:
  bool inited_;
  ObLightyQueue lighty_queue_;
  ObFixedQueue<int64_t> fixed_queue_;
  ObThreadCond cond_;
  bool is_lighty_;
};

int MPSC_ThreadPool::init(const int64_t queue_length)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    COMMON_LOG(WARN, "init twice");
  } else if (OB_FAIL(lighty_queue_.init(queue_length))) {
    COMMON_LOG(WARN, "fail to init lighty queue", K(ret));
  } else if (OB_FAIL(fixed_queue_.init(queue_length))) {
    COMMON_LOG(WARN, "fail to init lighty queue", K(ret));
  } else if (OB_FAIL(cond_.init(ObWaitEventIds::IO_QUEUE_LOCK_WAIT))) {
    COMMON_LOG(WARN, "fail to init lighty queue", K(ret));
  } else if (OB_FAIL(set_thread_count(PRODUCER_COUNT + CONSUMER_COUNT))) {
    COMMON_LOG(WARN, "fail to set thread count", K(ret));
  } else {
    inited_ = true;
    if (OB_FAIL(start())) {
      inited_ = false;
      COMMON_LOG(WARN, "fail to start thread pool", K(ret));
    }
  }
  return ret;
}

int MPSC_ThreadPool::produce()
{
  int ret = OB_SUCCESS;
  static int64_t val = 0;
  ATOMIC_INC(&val);

  if (is_lighty_) {  // push lighty queue
    if (OB_FAIL(lighty_queue_.push(reinterpret_cast<void*>(val)))) {
      COMMON_LOG(WARN, "fail to push to lighty queue", K(ret), K(val));
    }
  } else {  // push fixed queue
    ObThreadCondGuard cond_guard(cond_);
    if (OB_FAIL(cond_guard.get_ret())) {
      COMMON_LOG(ERROR, "Fail to guard queue condition", K(ret));
    } else if (OB_FAIL(fixed_queue_.push(reinterpret_cast<int64_t*>(val)))) {
      COMMON_LOG(WARN, "Fail to push to fixed queue", K(ret), K(val));
    } else if (OB_FAIL(cond_.signal())) {
      COMMON_LOG(ERROR, "Fail to signal queue condition", K(ret));
    }
  }

  if (REACH_TIME_INTERVAL(1000 * 1000L)) {
    static int64_t last_val = 0;
    COMMON_LOG(INFO, "wenqu debug: produced", K(val), "qps", val - last_val);
    last_val = val;
  }
  return ret;
}

int MPSC_ThreadPool::consume()
{
  int ret = OB_SUCCESS;
  static const int64_t timeout_us = 1000L;
  int64_t val = 0;

  if (is_lighty_) {  // consume lighty queue
    if (OB_FAIL(lighty_queue_.pop(reinterpret_cast<void*&>(val), timeout_us))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        COMMON_LOG(WARN, "fail to pop from lighty queue", K(ret), K(val));
      } else {
        ret = OB_SUCCESS;
      }
    } else if (val <= 0) {
      COMMON_LOG(WARN, "invalid value", K(val));
    }
  } else {  // consume fixed queue
    if (fixed_queue_.get_curr_total() <= 0) {
      ObThreadCondGuard cond_guard(cond_);
      if (OB_FAIL(cond_guard.get_ret())) {
        COMMON_LOG(ERROR, "Fail to guard queue condition", K(ret));
      } else if (OB_FAIL(cond_.wait_us(timeout_us))) {
        if (OB_TIMEOUT == ret) {
          ret = OB_SUCCESS;
        } else {
          COMMON_LOG(ERROR, "fail to wait queue condition", K(ret));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(fixed_queue_.pop(reinterpret_cast<int64_t*&>(val)))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        COMMON_LOG(WARN, "fail to pop from fixed queue", K(ret), K(val));
      } else {
        ret = OB_SUCCESS;
      }
    } else if (val <= 0) {
      COMMON_LOG(WARN, "invalid value", K(val));
    }
  }

  if (REACH_TIME_INTERVAL(1000 * 1000L)) {
    static int64_t last_val = 0;
    COMMON_LOG(INFO, "wenqu debug: consumed", K(val), "qps", val - last_val);
    last_val = val;
  }
  return ret;
}

void MPSC_ThreadPool::run1()
{
  int ret = OB_SUCCESS;
  const int64_t thread_id = get_thread_idx();
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "The thread pool has not been inited, ", K(ret));
  } else {
    if (thread_id < PRODUCER_COUNT) {  // submit thread pool
      while (!has_set_stop()) {
        produce();
        if (PRODUCE_INTERVAL_US > 0) {
          usleep(PRODUCE_INTERVAL_US);
        }
      }
      COMMON_LOG(INFO, "produce thread stopped", K(thread_id));
    } else if (thread_id < PRODUCER_COUNT + CONSUMER_COUNT) {  // getevent thread pool
      while (!has_set_stop()) {
        consume();
      }
      COMMON_LOG(INFO, "consumer thread stopped", K(thread_id));
    } else {
      ret = OB_ERR_UNEXPECTED;
      if (REACH_TIME_INTERVAL(60 * 1000 * 1000)) {
        COMMON_LOG(ERROR, "unexpected thread", K(ret), K(thread_id));
      }
      sleep(1);
    }
  }
}

TEST(TestObLightyQueue, DISABLED_mpsc)
{
  MPSC_ThreadPool tp;
  ASSERT_EQ(OB_SUCCESS, tp.init(10 * 10000L));
  sleep(1000);
}

}  // end namespace unittest
}  // end namespace oceanbase

int main(int argc, char** argv)
{
  OB_LOGGER.set_max_file_size(256 * 1024 * 1024);
  system("rm -f test_io_performance.log*");
  OB_LOGGER.set_file_name("test_io_performance.log");
  OB_LOGGER.set_log_level("INFO");
  set_memory_limit(40L * 1024L * 1024L * 1024L);  // 40GB
  ObIOManager::get_instance().init();
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
