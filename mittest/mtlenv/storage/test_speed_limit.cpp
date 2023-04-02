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
#include <thread>

#define USING_LOG_PREFIX STORAGE

#define protected public
#define private public
#include "lib/lock/ob_spin_rwlock.h"           // SpinRWLock
#include "lib/time/ob_time_utility.h"
#include "lib/random/ob_random.h"
#include "lib/utility/utility.h"
#include "lib/task/ob_timer.h"


using namespace oceanbase::common;
using namespace std;

namespace oceanbase
{

namespace storage
{

class TestSpeedLimit : public ::testing::Test
{
public:
  TestSpeedLimit()
    : timer_(),
      task_(*this)
      {}
  virtual ~TestSpeedLimit() = default;

  virtual void SetUp() override
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(timer_.init("Flush"))) {
      LOG_INFO("fail to init timer", K(ret));
    } else if (OB_FAIL(timer_.schedule(task_, FLUSH_INTERVAL, true))) {
      LOG_INFO("fail to schedule checkpoint task", K(ret));
    }
  }

  virtual void TearDown() override
  {
    timer_.wait();
    timer_.stop();
  }

  static void SetUpTestCase() {}
  static void TearDownTestCase() {}

  static const int64_t MEM_SLICE_SIZE = 2 * 1024 * 1024; //Bytes per usecond
  static const int64_t ADVANCE_CLOCK_INTERVAL = 50;//50us
  static const int64_t MIN_INTERVAL = 20000;
  static const int64_t SLEEP_INTERVAL_PER_TIME = 20 * 1000;
  static const int64_t SPEED_LIMIT_MAX_SLEEP_TIME = 20 * 1000 * 1000;
  static const int64_t FLUSH_INTERVAL = 5 * 1000 * 1000;
  static const int64_t INSERT_THREADS = 1000;
  static const int64_t WRITE_DURATION = 1000L; //adjust when test

  void write(int64_t size);

  void release(int64_t size);

  void set_param(int64_t trigger_percentage, int64_t lastest_memstore_threshold, int64_t writing_throttling_maximum_duration)
  {
    trigger_percentage_ = trigger_percentage;
    lastest_memstore_threshold_ = lastest_memstore_threshold;
    writing_throttling_maximum_duration_ = writing_throttling_maximum_duration;
  }

  bool need_do_writing_throttle() const
  {
    int64_t trigger_mem_limit = lastest_memstore_threshold_ * trigger_percentage_ / 100;
    int64_t cur_mem_hold = ATOMIC_LOAD(&hold_);
    bool need_do_writing_throttle = cur_mem_hold > trigger_mem_limit;
    return need_do_writing_throttle;
  }

  int64_t calc_mem_limit(int64_t cur_mem_hold, int64_t trigger_mem_limit, int64_t dt) const
  {
    double cur_chunk_seq = static_cast<double>(((cur_mem_hold - trigger_mem_limit) + MEM_SLICE_SIZE - 1)/ (MEM_SLICE_SIZE));
    int64_t mem_can_be_assigned = 0;

    int64_t allocate_size_in_the_page = MEM_SLICE_SIZE - (cur_mem_hold - trigger_mem_limit) % MEM_SLICE_SIZE;
    int64_t accumulate_interval = 0;
    int64_t the_page_interval = 0;
    while (accumulate_interval < dt) {
      the_page_interval = static_cast<int64_t>(decay_factor_ * cur_chunk_seq * cur_chunk_seq * cur_chunk_seq) * allocate_size_in_the_page / MEM_SLICE_SIZE;
      accumulate_interval += the_page_interval;

      mem_can_be_assigned += (accumulate_interval > dt ?
                              allocate_size_in_the_page - (accumulate_interval - dt) * allocate_size_in_the_page / the_page_interval :
                              allocate_size_in_the_page);
      allocate_size_in_the_page = MEM_SLICE_SIZE;
      cur_chunk_seq += double(1);
    }

    return mem_can_be_assigned;
  }

  int64_t expected_wait_time(int64_t seq) const
  {
    int64_t expected_wait_time = 0;
    if (clock_ < seq) {
      int64_t trigger_mem_limit = lastest_memstore_threshold_ * trigger_percentage_ / 100;
      int64_t can_assign_in_next_period = calc_mem_limit(hold_, trigger_mem_limit, ADVANCE_CLOCK_INTERVAL);
      expected_wait_time = (seq - clock_) * ADVANCE_CLOCK_INTERVAL / can_assign_in_next_period;
    }

    return expected_wait_time;
  }

  void speed_limit(int64_t cur_mem_hold, int64_t alloc_size)
  {
    int ret = OB_SUCCESS;
    int64_t trigger_mem_limit = 0;
    bool need_speed_limit = false;
    int64_t seq = 0;
    int64_t throttling_interval = 0;
    if (trigger_percentage_ < 100) {
      if (OB_UNLIKELY(cur_mem_hold < 0 || alloc_size <= 0 || lastest_memstore_threshold_ <= 0 || trigger_percentage_ <= 0)) {
        COMMON_LOG(ERROR, "invalid arguments", K(cur_mem_hold), K(alloc_size), K(lastest_memstore_threshold_), K(trigger_percentage_));
      } else if (cur_mem_hold > (trigger_mem_limit = lastest_memstore_threshold_ * trigger_percentage_ / 100)) {
        need_speed_limit = true;
        if (OB_FAIL(check_and_calc_decay_factor())) {
          COMMON_LOG(WARN, "failed to check_and_calc_decay_factor", K(cur_mem_hold), K(alloc_size));
        }
      }
      COMMON_LOG(INFO, "CCTT 2");
      advance_clock(need_speed_limit);
      seq = ATOMIC_AAF(&max_seq_, alloc_size);
      get_seq() = seq;
      tl_need_speed_limit() = need_speed_limit;

      if (need_speed_limit && REACH_TIME_INTERVAL(1 * 1000 * 1000L)) {
        COMMON_LOG(INFO, "report write throttle info", K(alloc_size), K(throttling_interval),
                    "max_seq_", ATOMIC_LOAD(&max_seq_), K(clock_),
                    K(cur_mem_hold), K(seq));
      }
    }
  }

  int check_and_calc_decay_factor()
  {
    int ret = OB_SUCCESS;
    int64_t available_mem = (100 - trigger_percentage_) * lastest_memstore_threshold_ / 100;
    double N =  static_cast<double>(available_mem) / static_cast<double>(MEM_SLICE_SIZE);
    decay_factor_ = (static_cast<double>(writing_throttling_maximum_duration_) - N * static_cast<double>(MIN_INTERVAL))/ static_cast<double>((((N*(N+1)*N*(N+1)))/4));
    decay_factor_ = decay_factor_ < 0 ? 0 : decay_factor_;
    COMMON_LOG(INFO, "recalculate decay factor", K(trigger_percentage_),
              K(decay_factor_), K(writing_throttling_maximum_duration_), K(available_mem), K(N));

    return ret;
  }

  bool check_clock_over_seq(int64_t req)
  {
    advance_clock(true);
    RLockGuard guard(rwlock_);
    if (clock_ < req) {
      COMMON_LOG(INFO, "CCTT 1", K(clock_), K(req));
    }

    return req <= clock_;
  }

  void advance_clock(bool need_speed_limit)
  {
    int64_t cur_ts = ObTimeUtility::current_time();
    int64_t old_ts = last_update_ts_;
    if ((cur_ts - last_update_ts_ > ADVANCE_CLOCK_INTERVAL) &&
        old_ts == ATOMIC_CAS(&last_update_ts_, old_ts, cur_ts)) {
      WLockGuard guard(rwlock_);
      int64_t trigger_mem_limit = lastest_memstore_threshold_ * trigger_percentage_ / 100;
      int64_t mem_limit = (need_speed_limit ? calc_mem_limit(hold_, trigger_mem_limit, cur_ts - old_ts) : trigger_mem_limit - hold_);
      clock_ = std::min(max_seq_, clock_ + mem_limit);
      COMMON_LOG(INFO, "current clock is ", K(clock_), K(max_seq_), K(mem_limit), K(hold_), K(cur_ts - old_ts));
    }
  }

  double decay_factor_;
  int64_t trigger_percentage_;
  int64_t lastest_memstore_threshold_;
  int64_t hold_;
  int64_t writing_throttling_maximum_duration_;

  typedef common::SpinRWLock RWLock;
  typedef common::SpinRLockGuard  RLockGuard;
  typedef common::SpinWLockGuard  WLockGuard;
  RWLock rwlock_;
  int64_t max_seq_;
  int64_t clock_;
  int64_t last_update_ts_;

  class ObTraversalFlushTask : public common::ObTimerTask
  {
  public:
    ObTraversalFlushTask(TestSpeedLimit &test_speed_limit)
      : test_speed_limit_(test_speed_limit) {}
    virtual ~ObTraversalFlushTask() {}
    virtual void runTimerTask()
    {
      test_speed_limit_.release(3000 * 1000 * 1000L);
    }
  private:
    TestSpeedLimit &test_speed_limit_;
  };
  common::ObTimer timer_;
  ObTraversalFlushTask task_;
};

void TestSpeedLimit::write(int64_t size)
{
  speed_limit(hold_, size);

  (void)ATOMIC_AAF(&hold_, size);
  // mock write latency
  int64_t sleep_time = ObRandom::rand(30, 1000);
  ob_usleep(sleep_time);

  bool &need_speed_limit = tl_need_speed_limit();
  int64_t &seq = get_seq();
  int64_t left_interval = SPEED_LIMIT_MAX_SLEEP_TIME;
  bool has_sleep = false;
  int64_t has_sleep_time = 0;
  int time = 0;
  bool need_sleep = need_speed_limit;

  while (need_sleep &&
         !check_clock_over_seq(seq) &&
        (left_interval > 0)) {
    int64_t wait_time = expected_wait_time(seq);
    //LOG_INFO("CCTT 3", K(need_sleep), K(left_interval), K(wait_time), K(seq), K(clock_), K(max_seq_));
    if (wait_time == 0) {
      break;
    }
    uint32_t sleep_interval =
      static_cast<uint32_t>(min(min(left_interval, SLEEP_INTERVAL_PER_TIME), wait_time));
    ob_usleep<common::ObWaitEventIds::STORAGE_WRITING_THROTTLE_SLEEP>(sleep_interval);
    has_sleep_time += sleep_interval;
    time++;
    left_interval -= sleep_interval;
    has_sleep = true;
    need_sleep = need_do_writing_throttle();
  }

  if (need_speed_limit && TC_REACH_TIME_INTERVAL(100L * 1000L)) {
    LOG_INFO("throttle situation", K(has_sleep_time), K(time), K(seq));
  }
}

void TestSpeedLimit::release(int64_t size)
{
  if (hold_ > (lastest_memstore_threshold_ * (trigger_percentage_ + 30) / 100)) {
    (void)ATOMIC_AAF(&hold_, -size);
  }
}

TEST_F(TestSpeedLimit, test_speed_limit)
{
  set_param(60, 9663676400, 3600000000);
  std::vector<std::thread> insert_threads;

  int insert_thread_num = 5;
  for (int i = 0; i < insert_thread_num; ++i) {
    insert_threads.push_back(std::thread([this]() {
      const int64_t start_time = ObTimeUtility::current_time();
      while (ObTimeUtility::current_time() - start_time <= WRITE_DURATION) {
        write(1600);
      }}));
  }

  for (int i = 0; i < insert_thread_num; ++i) {
    insert_threads[i].join();
  }
}

}
}

int main(int argc, char **argv)
{
  system("rm -f test_speed_limit.log*");
  OB_LOGGER.set_file_name("test_speed_limit.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}