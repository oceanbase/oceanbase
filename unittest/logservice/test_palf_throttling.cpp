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

#include "common/ob_clock_generator.h"

#define private public
#include "logservice/palf/palf_env_impl.h"
#undef private

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace palf;

namespace unittest
{

class TestPalfThrottleEnv : public PalfEnvImpl
{
public:
  TestPalfThrottleEnv()
    : PalfEnvImpl(),
      use_injected_options_(false),
      get_options_ret_(OB_SUCCESS),
      options_()
  {}
  virtual ~TestPalfThrottleEnv() {}

  virtual int get_throttling_options(PalfThrottleOptions &options) override
  {
    int ret = get_options_ret_;
    if (OB_SUCC(ret)) {
      if (use_injected_options_) {
        options = options_;
      } else {
        disk_options_wrapper_.get_throttling_options(options);
      }
    }
    return ret;
  }

public:
  bool use_injected_options_;
  int get_options_ret_;
  PalfThrottleOptions options_;
};

class TestPalfThrottling : public ::testing::Test
{
public:
  TestPalfThrottling();
  virtual ~TestPalfThrottling();
  virtual void SetUp();
  virtual void TearDown();
protected:
  void expire_throttle_update(LogWritingThrottle &throttle);
  void configure_active_throttle(LogWritingThrottle &throttle,
                                 TestPalfThrottleEnv &palf_env);
  void assert_zero_throttle_counters(const LogWritingThrottle &throttle);

  bool g_need_purging_throttling;
  NeedPurgingThrottlingFunc g_need_purging_throttling_func;
};

TestPalfThrottling::TestPalfThrottling() : g_need_purging_throttling(false) {
  g_need_purging_throttling_func = [this](){ return g_need_purging_throttling; };
}

TestPalfThrottling::~TestPalfThrottling()
{
}

void TestPalfThrottling::SetUp()
{
  ASSERT_EQ(OB_SUCCESS, ObClockGenerator::init());
  //ObMallocAllocator::get_instance()->create_and_add_tenant_allocator(1001);
  // init MTL
  //ObTenantBase tbase(1001);
  //ObTenantEnv::set_tenant(&tbase);
}

void TestPalfThrottling::TearDown()
{
  PALF_LOG(INFO, "TestPalfThrottling has TearDown");
  ObClockGenerator::destroy();
  //ObMallocAllocator::get_instance()->recycle_tenant_allocator(1001);
}

void TestPalfThrottling::expire_throttle_update(LogWritingThrottle &throttle)
{
  throttle.last_update_ts_ = ObClockGenerator::getClock()
      - LogWritingThrottle::UPDATE_INTERVAL_US - 1;
}

void TestPalfThrottling::configure_active_throttle(LogWritingThrottle &throttle,
                                                   TestPalfThrottleEnv &palf_env)
{
  static const int64_t TOTAL_DISK_SIZE = 1024L * 1024L * 1024L;
  palf_env.use_injected_options_ = true;
  palf_env.options_.total_disk_space_ = TOTAL_DISK_SIZE;
  palf_env.options_.trigger_percentage_ = 60;
  palf_env.options_.stopping_writing_percentage_ = 95;
  palf_env.options_.maximum_duration_ = 7200L * 1000L * 1000L;
  palf_env.options_.unrecyclable_disk_space_ = TOTAL_DISK_SIZE * 70 / 100;
  throttle.notify_need_writing_throttling(true);
}

void TestPalfThrottling::assert_zero_throttle_counters(const LogWritingThrottle &throttle)
{
  ASSERT_EQ(0, throttle.stat_.total_skipped_size_);
  ASSERT_EQ(0, throttle.stat_.total_skipped_task_cnt_);
  ASSERT_EQ(0, throttle.stat_.total_throttling_size_);
  ASSERT_EQ(0, throttle.stat_.total_throttling_task_cnt_);
  ASSERT_EQ(0, throttle.stat_.total_throttling_interval_);
  ASSERT_EQ(0, throttle.stat_.max_throttling_interval_);
}


TEST_F(TestPalfThrottling, test_palf_options)
{
  char buf[64]  = {0};
  memset(buf, 0, 64);
  int64_t checksum = common::ob_crc64(buf, 64);
  PALF_LOG(INFO, "checksum", K(checksum));
  //test PalfDiskOptionsWrapper
  PalfDiskOptionsWrapper wrapper;
  ASSERT_EQ(false, wrapper.need_throttling());
  int64_t total_disk_size = 1024 * 1024 * 1024L;
  int64_t utilization_limit_threshold = 95;
  int64_t throttling_percentage = 60;
  wrapper.disk_opts_for_stopping_writing_.log_disk_throttling_percentage_ = throttling_percentage;
  wrapper.disk_opts_for_stopping_writing_.log_disk_throttling_maximum_duration_ = 7200 * 1000 * 1000L;
  wrapper.disk_opts_for_stopping_writing_.log_disk_usage_limit_size_ = total_disk_size;
  wrapper.disk_opts_for_stopping_writing_.log_disk_utilization_threshold_ = 80;
  wrapper.disk_opts_for_stopping_writing_.log_disk_utilization_limit_threshold_ = utilization_limit_threshold;
  wrapper.disk_opts_for_stopping_writing_.log_writer_parallelism_ = 1;
  int64_t unrecyclable_size = 0;
  wrapper.set_cur_unrecyclable_log_disk_size(unrecyclable_size);
  ASSERT_EQ(false, wrapper.need_throttling());
  unrecyclable_size = total_disk_size * 70 /100;
  wrapper.set_cur_unrecyclable_log_disk_size(unrecyclable_size);
  PALF_LOG(INFO, "test_palf_options trace", K(wrapper));
  ASSERT_EQ(true, wrapper.need_throttling());
  //test PalfThrottleOptions
  PalfThrottleOptions throttling_options;
  ASSERT_EQ(false, throttling_options.is_valid());
  ASSERT_EQ(false, throttling_options.need_throttling());
  ASSERT_EQ(0, throttling_options.get_available_size_after_limit());
  wrapper.get_throttling_options(throttling_options);
  ASSERT_EQ(true, throttling_options.is_valid());
  ASSERT_EQ(throttling_options.total_disk_space_, total_disk_size);
  ASSERT_EQ(throttling_options.stopping_writing_percentage_, utilization_limit_threshold);
  ASSERT_EQ(throttling_options.trigger_percentage_, throttling_percentage);
  ASSERT_EQ(throttling_options.unrecyclable_disk_space_, unrecyclable_size);
  ASSERT_EQ(true, throttling_options.need_throttling());
  ASSERT_EQ(total_disk_size * (utilization_limit_threshold - throttling_percentage)/100, throttling_options.get_available_size_after_limit());
}

TEST_F(TestPalfThrottling, test_throttling_stat)
{
  LogThrottlingStat stat;
  ASSERT_EQ(false, stat.has_ever_throttled());
  stat.start_throttling();
  ASSERT_EQ(true, stat.has_ever_throttled());
  stat.after_throttling(0, 1024);
  stat.after_throttling(100, 1024);
  stat.after_throttling(200, 1024);
  stat.after_throttling(0, 1024);
  stat.stop_throttling();
  ASSERT_EQ(true, OB_INVALID_TIMESTAMP != stat.start_ts_);
  ASSERT_EQ(true, OB_INVALID_TIMESTAMP != stat.stop_ts_);
  ASSERT_EQ(2048, stat.total_skipped_size_);
  ASSERT_EQ(2, stat.total_skipped_task_cnt_);
  ASSERT_EQ(2048, stat.total_throttling_size_);
  ASSERT_EQ(2, stat.total_throttling_task_cnt_);
  ASSERT_EQ(300, stat.total_throttling_interval_);
  ASSERT_EQ(200, stat.max_throttling_interval_);
  stat.start_throttling();
  ASSERT_EQ(0, stat.total_skipped_size_);
  ASSERT_EQ(0, stat.total_skipped_task_cnt_);
  ASSERT_EQ(0, stat.total_throttling_size_);
  ASSERT_EQ(0, stat.total_throttling_task_cnt_);
  ASSERT_EQ(0, stat.total_throttling_interval_);
  ASSERT_EQ(0, stat.max_throttling_interval_);
}

TEST_F(TestPalfThrottling, test_async_admission_invalid_inputs_fail_closed)
{
  static const int64_t LOGICAL_BYTES = 1024L * 1024L;
  LogWritingThrottle throttle;
  TestPalfThrottleEnv palf_env;
  NeedPurgingThrottlingFunc invalid_purge_func;
  bool can_admit = true;
  int64_t delay_us = -1;

  ASSERT_EQ(OB_INVALID_ARGUMENT,
            throttle.try_admit_async(LOGICAL_BYTES, invalid_purge_func,
                                     &palf_env, can_admit, delay_us));
  ASSERT_FALSE(can_admit);
  ASSERT_EQ(0, delay_us);
  assert_zero_throttle_counters(throttle);

  can_admit = true;
  delay_us = -1;
  ASSERT_EQ(OB_INVALID_ARGUMENT,
            throttle.probe_admit_async(LOGICAL_BYTES, invalid_purge_func,
                                       &palf_env, can_admit, delay_us));
  ASSERT_FALSE(can_admit);
  ASSERT_EQ(0, delay_us);
  assert_zero_throttle_counters(throttle);

  can_admit = true;
  delay_us = -1;
  ASSERT_EQ(OB_INVALID_ARGUMENT,
            throttle.try_admit_async(LOGICAL_BYTES, g_need_purging_throttling_func,
                                     NULL, can_admit, delay_us));
  ASSERT_FALSE(can_admit);
  ASSERT_EQ(0, delay_us);
  assert_zero_throttle_counters(throttle);

  can_admit = true;
  delay_us = -1;
  ASSERT_EQ(OB_INVALID_ARGUMENT,
            throttle.probe_admit_async(LOGICAL_BYTES, g_need_purging_throttling_func,
                                       NULL, can_admit, delay_us));
  ASSERT_FALSE(can_admit);
  ASSERT_EQ(0, delay_us);
  assert_zero_throttle_counters(throttle);

  can_admit = true;
  delay_us = -1;
  ASSERT_EQ(OB_INVALID_ARGUMENT,
            throttle.try_admit_async(-1, g_need_purging_throttling_func,
                                     &palf_env, can_admit, delay_us));
  ASSERT_FALSE(can_admit);
  ASSERT_EQ(0, delay_us);
  assert_zero_throttle_counters(throttle);

  can_admit = true;
  delay_us = -1;
  ASSERT_EQ(OB_INVALID_ARGUMENT,
            throttle.probe_admit_async(-1, g_need_purging_throttling_func,
                                       &palf_env, can_admit, delay_us));
  ASSERT_FALSE(can_admit);
  ASSERT_EQ(0, delay_us);
  assert_zero_throttle_counters(throttle);

  can_admit = false;
  delay_us = -1;
  ASSERT_EQ(OB_SUCCESS,
            throttle.try_admit_async(0, g_need_purging_throttling_func,
                                     &palf_env, can_admit, delay_us));
  ASSERT_TRUE(can_admit);
  ASSERT_EQ(0, delay_us);
  assert_zero_throttle_counters(throttle);

  can_admit = false;
  delay_us = -1;
  ASSERT_EQ(OB_SUCCESS,
            throttle.probe_admit_async(0, g_need_purging_throttling_func,
                                       &palf_env, can_admit, delay_us));
  ASSERT_TRUE(can_admit);
  ASSERT_EQ(0, delay_us);
  assert_zero_throttle_counters(throttle);
}

TEST_F(TestPalfThrottling, test_async_admission_decision_and_stats)
{
  static const int64_t LOGICAL_BYTES = 1024L * 1024L;
  LogWritingThrottle throttle;
  TestPalfThrottleEnv palf_env;
  bool can_admit = false;
  int64_t delay_us = -1;

  configure_active_throttle(throttle, palf_env);
  throttle.notify_need_writing_throttling(false);
  ASSERT_EQ(OB_SUCCESS,
            throttle.try_admit_async(LOGICAL_BYTES, g_need_purging_throttling_func,
                                     &palf_env, can_admit, delay_us));
  ASSERT_TRUE(can_admit);
  ASSERT_EQ(0, delay_us);
  assert_zero_throttle_counters(throttle);

  throttle.notify_need_writing_throttling(true);
  expire_throttle_update(throttle);
  ASSERT_EQ(OB_SUCCESS,
            throttle.probe_admit_async(LOGICAL_BYTES, g_need_purging_throttling_func,
                                       &palf_env, can_admit, delay_us));
  ASSERT_FALSE(can_admit);
  ASSERT_GT(delay_us, 0);
  const int64_t probe_delay_us = delay_us;
  assert_zero_throttle_counters(throttle);

  ASSERT_EQ(OB_SUCCESS,
            throttle.try_admit_async(LOGICAL_BYTES, g_need_purging_throttling_func,
                                     &palf_env, can_admit, delay_us));
  ASSERT_FALSE(can_admit);
  ASSERT_EQ(probe_delay_us, delay_us);
  ASSERT_EQ(1, throttle.stat_.total_skipped_task_cnt_);
  ASSERT_EQ(LOGICAL_BYTES, throttle.stat_.total_skipped_size_);
  ASSERT_EQ(0, throttle.stat_.total_throttling_task_cnt_);
  ASSERT_EQ(0, throttle.stat_.total_throttling_size_);
  ASSERT_EQ(0, throttle.stat_.total_throttling_interval_);

  ASSERT_EQ(OB_SUCCESS,
            throttle.probe_admit_async(LOGICAL_BYTES, g_need_purging_throttling_func,
                                       &palf_env, can_admit, delay_us));
  ASSERT_FALSE(can_admit);
  ASSERT_GT(delay_us, 0);
  ASSERT_EQ(1, throttle.stat_.total_skipped_task_cnt_);
  ASSERT_EQ(LOGICAL_BYTES, throttle.stat_.total_skipped_size_);
}

TEST_F(TestPalfThrottling, test_async_admission_purge_bypass)
{
  static const int64_t LOGICAL_BYTES = 1024L * 1024L;
  LogWritingThrottle throttle;
  TestPalfThrottleEnv palf_env;
  bool can_admit = false;
  int64_t delay_us = -1;

  configure_active_throttle(throttle, palf_env);
  g_need_purging_throttling = true;
  ASSERT_EQ(OB_SUCCESS,
            throttle.probe_admit_async(LOGICAL_BYTES, g_need_purging_throttling_func,
                                       &palf_env, can_admit, delay_us));
  ASSERT_TRUE(can_admit);
  ASSERT_EQ(0, delay_us);
  assert_zero_throttle_counters(throttle);

  ASSERT_EQ(OB_SUCCESS,
            throttle.try_admit_async(LOGICAL_BYTES, g_need_purging_throttling_func,
                                     &palf_env, can_admit, delay_us));
  ASSERT_TRUE(can_admit);
  ASSERT_EQ(0, delay_us);
  ASSERT_EQ(1, throttle.stat_.total_skipped_task_cnt_);
  ASSERT_EQ(LOGICAL_BYTES, throttle.stat_.total_skipped_size_);
  ASSERT_EQ(0, throttle.stat_.total_throttling_task_cnt_);

  g_need_purging_throttling = false;
  ASSERT_EQ(OB_SUCCESS,
            throttle.probe_admit_async(LOGICAL_BYTES, g_need_purging_throttling_func,
                                       &palf_env, can_admit, delay_us));
  ASSERT_FALSE(can_admit);
  ASSERT_GT(delay_us, 0);
  ASSERT_EQ(1, throttle.stat_.total_skipped_task_cnt_);
  ASSERT_EQ(LOGICAL_BYTES, throttle.stat_.total_skipped_size_);
}

TEST_F(TestPalfThrottling, test_async_admission_decision_error_fails_closed)
{
  static const int64_t LOGICAL_BYTES = 1024L * 1024L;
  LogWritingThrottle throttle;
  TestPalfThrottleEnv palf_env;
  bool can_admit = true;
  int64_t delay_us = -1;

  configure_active_throttle(throttle, palf_env);
  palf_env.get_options_ret_ = OB_NOT_INIT;
  ASSERT_EQ(OB_NOT_INIT,
            throttle.try_admit_async(LOGICAL_BYTES, g_need_purging_throttling_func,
                                     &palf_env, can_admit, delay_us));
  ASSERT_FALSE(can_admit);
  ASSERT_EQ(0, delay_us);
  assert_zero_throttle_counters(throttle);

  can_admit = true;
  delay_us = -1;
  ASSERT_EQ(OB_NOT_INIT,
            throttle.probe_admit_async(LOGICAL_BYTES, g_need_purging_throttling_func,
                                       &palf_env, can_admit, delay_us));
  ASSERT_FALSE(can_admit);
  ASSERT_EQ(0, delay_us);
  assert_zero_throttle_counters(throttle);
}

TEST_F(TestPalfThrottling, test_log_write_throttle)
{
  int64_t total_disk_size = 1024 * 1024 * 1024L;
  int64_t utilization_limit_threshold = 95;
  int64_t throttling_percentage = 60;
  TestPalfThrottleEnv palf_env_impl;
  palf_env_impl.disk_options_wrapper_.disk_opts_for_stopping_writing_.log_disk_throttling_percentage_ = throttling_percentage;
  palf_env_impl.disk_options_wrapper_.disk_opts_for_stopping_writing_.log_disk_throttling_maximum_duration_ = 7200 * 1000 * 1000L;
  palf_env_impl.disk_options_wrapper_.disk_opts_for_stopping_writing_.log_disk_usage_limit_size_ = total_disk_size;
  palf_env_impl.disk_options_wrapper_.disk_opts_for_stopping_writing_.log_disk_utilization_threshold_ = 80;
  palf_env_impl.disk_options_wrapper_.disk_opts_for_stopping_writing_.log_disk_utilization_limit_threshold_ = utilization_limit_threshold;
  int64_t unrecyclable_size = 0;
  palf_env_impl.disk_options_wrapper_.set_cur_unrecyclable_log_disk_size(unrecyclable_size);
  PalfThrottleOptions throttle_options;
  palf_env_impl.disk_options_wrapper_.get_throttling_options(throttle_options);

  LogWritingThrottle throttle;
  ASSERT_EQ(false, throttle.need_throttling_not_guarded_by_lock_(g_need_purging_throttling_func));
  ASSERT_EQ(false, throttle.need_writing_throttling_notified());
  throttle.notify_need_writing_throttling(true);
  ASSERT_EQ(true, throttle.need_writing_throttling_notified());

  throttle.notify_need_writing_throttling(false);
  ASSERT_EQ(false, throttle.need_writing_throttling_notified());

  ASSERT_EQ(OB_INVALID_ARGUMENT, throttle.after_append_log(-1));

  //test throttling only after notified
  PALF_LOG(INFO, "case 1: test no need throttling while notify_need_writing_throttling is false");
  throttle.update_throttling_options(&palf_env_impl);
  throttle.throttling(1024, g_need_purging_throttling_func, &palf_env_impl);
  PalfThrottleOptions invalid_throttle_options;
  ASSERT_EQ(invalid_throttle_options, throttle.throttling_options_);
  ASSERT_EQ(true, OB_INVALID_TIMESTAMP != throttle.last_update_ts_);
  ASSERT_EQ(false, throttle.need_writing_throttling_notified_);

  // test update interval 500ms
  PALF_LOG(INFO, "case 2: test update interval");
  throttle.throttling(1024, g_need_purging_throttling_func, &palf_env_impl);
  ASSERT_EQ(invalid_throttle_options, throttle.throttling_options_);
  ASSERT_EQ(true, OB_INVALID_TIMESTAMP != throttle.last_update_ts_);
  ASSERT_EQ(false, throttle.need_writing_throttling_notified_);

  //test no need throttling after update
  PALF_LOG(INFO, "case 3: test no need throttling while unrecyclable_log_disk_size is no more than trigger_size");
  expire_throttle_update(throttle);
  throttle.update_throttling_options(&palf_env_impl);
  throttle.notify_need_writing_throttling(true);
  throttle.throttling(1024, g_need_purging_throttling_func, &palf_env_impl);
  ASSERT_EQ(false, throttle.need_throttling_not_guarded_by_lock_(g_need_purging_throttling_func));
  ASSERT_EQ(false, throttle.stat_.has_ever_throttled());


  PALF_LOG(INFO, "case 4: test no need throttling while trigger percentage is 100", K(throttle));
  unrecyclable_size = total_disk_size * 70 / 100;
  palf_env_impl.disk_options_wrapper_.set_cur_unrecyclable_log_disk_size(unrecyclable_size);
  palf_env_impl.disk_options_wrapper_.disk_opts_for_stopping_writing_.log_disk_throttling_percentage_ = 100;
  expire_throttle_update(throttle);
  throttle.update_throttling_options(&palf_env_impl);
  throttle.throttling(1024, g_need_purging_throttling_func, &palf_env_impl);
  palf_env_impl.disk_options_wrapper_.get_throttling_options(throttle_options);
  ASSERT_EQ(false, throttle.need_throttling_not_guarded_by_lock_(g_need_purging_throttling_func));
  ASSERT_EQ(false, throttle.stat_.has_ever_throttled());

  //test need throttling after update
  PALF_LOG(INFO, "case 4: test need throttling", K(throttle));
  palf_env_impl.disk_options_wrapper_.disk_opts_for_stopping_writing_.log_disk_throttling_percentage_ = throttling_percentage;
  unrecyclable_size = total_disk_size * 70 / 100;
  palf_env_impl.disk_options_wrapper_.set_cur_unrecyclable_log_disk_size(unrecyclable_size);
  expire_throttle_update(throttle);
  throttle.update_throttling_options(&palf_env_impl);
  throttle.throttling(1024, g_need_purging_throttling_func, &palf_env_impl);
  palf_env_impl.disk_options_wrapper_.get_throttling_options(throttle_options);
  PALF_LOG(INFO, "case 4: YYY test need throttling", K(throttle_options), K(throttle.throttling_options_));
  ASSERT_EQ(throttle_options, throttle.throttling_options_);
  ASSERT_EQ(true, throttle.need_throttling_not_guarded_by_lock_(g_need_purging_throttling_func));
  ASSERT_EQ(true, throttle.stat_.has_ever_throttled());
  ASSERT_EQ(1024, throttle.stat_.total_throttling_size_);
  ASSERT_EQ(1, throttle.stat_.total_throttling_task_cnt_);
  ASSERT_EQ(0, throttle.stat_.total_skipped_task_cnt_);
  ASSERT_EQ(0, throttle.stat_.total_skipped_size_);
  throttle.after_append_log(1024);
  ASSERT_EQ(1024, throttle.appended_log_size_cur_round_);

  //test no need throttling with flush meta task in queue
  //.1 flush log task
  g_need_purging_throttling = true;
  PALF_LOG(INFO, "case 5: test no need throttling while flush meta task ", K(throttle));
  throttle.throttling(1024, g_need_purging_throttling_func, &palf_env_impl);
  ASSERT_EQ(throttle_options, throttle.throttling_options_);
  // meta task need purging throttling
  ASSERT_EQ(false, throttle.need_throttling_not_guarded_by_lock_(g_need_purging_throttling_func));
  ASSERT_EQ(1024, throttle.stat_.total_throttling_size_);
  ASSERT_EQ(1, throttle.stat_.total_throttling_task_cnt_);
  ASSERT_EQ(1, throttle.stat_.total_skipped_task_cnt_);
  ASSERT_EQ(1024, throttle.stat_.total_skipped_size_);
  throttle.after_append_log(1024);
  ASSERT_EQ(2048, throttle.appended_log_size_cur_round_);

//.2 flush meta task
  PALF_LOG(INFO, "case 6: test no need throttling and flush meta task ", K(throttle));
  throttle.throttling(1024, g_need_purging_throttling_func, &palf_env_impl);
  ASSERT_EQ(throttle_options, throttle.throttling_options_);
  ASSERT_EQ(true, throttle.need_throttling_with_options_not_guarded_by_lock_());
  ASSERT_EQ(false, throttle.need_throttling_not_guarded_by_lock_(g_need_purging_throttling_func));
  ASSERT_EQ(1024, throttle.stat_.total_throttling_size_);
  ASSERT_EQ(1, throttle.stat_.total_throttling_task_cnt_);
  ASSERT_EQ(2, throttle.stat_.total_skipped_task_cnt_);
  ASSERT_EQ(2048, throttle.stat_.total_skipped_size_);
  throttle.after_append_log(1024);
  ASSERT_EQ(3072, throttle.appended_log_size_cur_round_);

  throttle.throttling(1024, g_need_purging_throttling_func, &palf_env_impl);
  ASSERT_EQ(throttle_options, throttle.throttling_options_);
  ASSERT_EQ(true, throttle.need_throttling_with_options_not_guarded_by_lock_());
  ASSERT_EQ(false, throttle.need_throttling_not_guarded_by_lock_(g_need_purging_throttling_func));
  ASSERT_EQ(1024, throttle.stat_.total_throttling_size_);
  ASSERT_EQ(1, throttle.stat_.total_throttling_task_cnt_);
  ASSERT_EQ(3, throttle.stat_.total_skipped_task_cnt_);
  ASSERT_EQ(3072, throttle.stat_.total_skipped_size_);
  throttle.after_append_log(1024);
  ASSERT_EQ(4096, throttle.appended_log_size_cur_round_);

  PALF_LOG(INFO, "case 7: need throttling after all flush meta task handled", K(throttle));
  g_need_purging_throttling = false;
  throttle.throttling(1024, g_need_purging_throttling_func, &palf_env_impl);
  ASSERT_EQ(throttle_options, throttle.throttling_options_);
  ASSERT_EQ(true, throttle.decay_factor_ > 0.0);
  ASSERT_EQ(true, throttle.need_throttling_not_guarded_by_lock_(g_need_purging_throttling_func));
  ASSERT_EQ(2048, throttle.stat_.total_throttling_size_);
  ASSERT_EQ(2, throttle.stat_.total_throttling_task_cnt_);
  ASSERT_EQ(3, throttle.stat_.total_skipped_task_cnt_);
  ASSERT_EQ(3072, throttle.stat_.total_skipped_size_);
  throttle.after_append_log(1024);
  ASSERT_EQ(5120, throttle.appended_log_size_cur_round_);

  //test  notify_need_writing_throttling(false) changed
  PALF_LOG(INFO, "case 8: no need to throttle after notify_need_throttling(false)", K(throttle));
  expire_throttle_update(throttle);
  throttle.notify_need_writing_throttling(false);
  throttle.update_throttling_options(&palf_env_impl);
  throttle.throttling(1024, g_need_purging_throttling_func, &palf_env_impl);
  ASSERT_EQ(invalid_throttle_options, throttle.throttling_options_);
  ASSERT_EQ(false, throttle.need_throttling_with_options_not_guarded_by_lock_());
  ASSERT_EQ(false, throttle.need_throttling_not_guarded_by_lock_(g_need_purging_throttling_func));
  ASSERT_EQ(2048, throttle.stat_.total_throttling_size_);
  ASSERT_EQ(2, throttle.stat_.total_throttling_task_cnt_);
  ASSERT_EQ(3, throttle.stat_.total_skipped_task_cnt_);
  ASSERT_EQ(3072, throttle.stat_.total_skipped_size_);
  ASSERT_EQ(true, OB_INVALID_TIMESTAMP != throttle.stat_.stop_ts_);
  throttle.after_append_log(1024);
  ASSERT_EQ(1024, throttle.appended_log_size_cur_round_);
  ASSERT_EQ(0, throttle.decay_factor_);

  //test need write throttling again
  PALF_LOG(INFO, "case 9: need to throttle after notify_need_throttling(true)", K(throttle));
  expire_throttle_update(throttle);
  throttle.notify_need_writing_throttling(true);
  throttle.update_throttling_options(&palf_env_impl);
  throttle.throttling(1024, g_need_purging_throttling_func, &palf_env_impl);
  ASSERT_EQ(true, throttle.need_throttling_with_options_not_guarded_by_lock_());
  ASSERT_EQ(true, throttle.need_throttling_not_guarded_by_lock_(g_need_purging_throttling_func));
  ASSERT_EQ(throttle_options, throttle.throttling_options_);
  ASSERT_EQ(true, throttle.decay_factor_ > 0.0);
  ASSERT_EQ(1024, throttle.stat_.total_throttling_size_);
  ASSERT_EQ(1, throttle.stat_.total_throttling_task_cnt_);
  ASSERT_EQ(0, throttle.stat_.total_skipped_task_cnt_);
  ASSERT_EQ(0, throttle.stat_.total_skipped_size_);
  ASSERT_EQ(true, throttle.stat_.total_throttling_interval_ > 0);
  ASSERT_EQ(true, OB_INVALID_TIMESTAMP == throttle.stat_.stop_ts_);
  throttle.after_append_log(1024);
  ASSERT_EQ(1024, throttle.appended_log_size_cur_round_);

  double old_decay_factor = throttle.decay_factor_;
  //
  PALF_LOG(INFO, "case 10: test recalculate decay_factor", K(throttle));
  expire_throttle_update(throttle);
  palf_env_impl.disk_options_wrapper_.disk_opts_for_stopping_writing_.log_disk_throttling_percentage_ = 55;
  palf_env_impl.get_throttling_options(throttle_options);
  throttle.update_throttling_options(&palf_env_impl);
  throttle.throttling(1024, g_need_purging_throttling_func, &palf_env_impl);
  ASSERT_EQ(true, throttle.need_throttling_with_options_not_guarded_by_lock_());
  ASSERT_EQ(true, throttle.need_throttling_not_guarded_by_lock_(g_need_purging_throttling_func));
  ASSERT_EQ(throttle_options, throttle.throttling_options_);
  ASSERT_EQ(true, throttle.decay_factor_ > 0.0);
  ASSERT_EQ(true, throttle.decay_factor_ != old_decay_factor);
  ASSERT_EQ(2048, throttle.stat_.total_throttling_size_);
  ASSERT_EQ(2, throttle.stat_.total_throttling_task_cnt_);
  ASSERT_EQ(0, throttle.stat_.total_skipped_task_cnt_);
  ASSERT_EQ(0, throttle.stat_.total_skipped_size_);
  ASSERT_EQ(true, OB_INVALID_TIMESTAMP == throttle.stat_.stop_ts_);
  throttle.after_append_log(1024);
  ASSERT_EQ(2048, throttle.appended_log_size_cur_round_);

  //test reset appended_log_size_cur_round_
  PALF_LOG(INFO, "case 11: test reset appended_log_size_cur_round_ after unrecyclable_size changes", K(throttle));
  old_decay_factor = throttle.decay_factor_;
  expire_throttle_update(throttle);
  unrecyclable_size = total_disk_size * 65/100;
  palf_env_impl.disk_options_wrapper_.set_cur_unrecyclable_log_disk_size(unrecyclable_size);
  throttle.update_throttling_options(&palf_env_impl);
  throttle.throttling(1024, g_need_purging_throttling_func, &palf_env_impl);
  ASSERT_EQ(true, throttle.need_throttling_with_options_not_guarded_by_lock_());
  ASSERT_EQ(true, throttle.need_throttling_not_guarded_by_lock_(g_need_purging_throttling_func));
  palf_env_impl.get_throttling_options(throttle_options);
  ASSERT_EQ(throttle_options, throttle.throttling_options_);
  ASSERT_EQ(true, throttle.decay_factor_ > 0.0);
  ASSERT_EQ(true, throttle.decay_factor_ == old_decay_factor);
  ASSERT_EQ(3072, throttle.stat_.total_throttling_size_);
  ASSERT_EQ(3, throttle.stat_.total_throttling_task_cnt_);
  ASSERT_EQ(0, throttle.stat_.total_skipped_task_cnt_);
  ASSERT_EQ(0, throttle.stat_.total_skipped_size_);
  ASSERT_EQ(true, OB_INVALID_TIMESTAMP != throttle.stat_.start_ts_);
  ASSERT_EQ(true, OB_INVALID_TIMESTAMP == throttle.stat_.stop_ts_);
  throttle.after_append_log(1024);
  ASSERT_EQ(1024, throttle.appended_log_size_cur_round_);
  ASSERT_EQ(OB_SUCCESS, throttle.after_append_log(0));

//test stop write throttling when trigger percentage changed
  PALF_LOG(INFO, "case 12: test stop write throttling when trigger percentage changed", K(throttle));
  expire_throttle_update(throttle);
  palf_env_impl.disk_options_wrapper_.disk_opts_for_stopping_writing_.log_disk_throttling_percentage_ = 80;
  palf_env_impl.get_throttling_options(throttle_options);
  throttle.update_throttling_options(&palf_env_impl);
  throttle.throttling(1024, g_need_purging_throttling_func, &palf_env_impl);
  ASSERT_EQ(false, throttle.need_throttling_with_options_not_guarded_by_lock_());
  ASSERT_EQ(false, throttle.need_throttling_not_guarded_by_lock_(g_need_purging_throttling_func));
  ASSERT_EQ(invalid_throttle_options, throttle.throttling_options_);
  ASSERT_EQ(0, throttle.decay_factor_);
  ASSERT_EQ(3072, throttle.stat_.total_throttling_size_);
  ASSERT_EQ(3, throttle.stat_.total_throttling_task_cnt_);
  ASSERT_EQ(0, throttle.stat_.total_skipped_task_cnt_);
  ASSERT_EQ(0, throttle.stat_.total_skipped_size_);
  ASSERT_EQ(true, OB_INVALID_TIMESTAMP != throttle.stat_.stop_ts_);
  ASSERT_EQ(0, throttle.appended_log_size_cur_round_);

  PALF_LOG(INFO, "case 12: test stop writing throttling when unrecyclable size fallbacks", K(throttle));
  expire_throttle_update(throttle);
  palf_env_impl.disk_options_wrapper_.disk_opts_for_stopping_writing_.log_disk_throttling_percentage_ = 60;
  throttle.notify_need_writing_throttling(true);
  throttle.update_throttling_options(&palf_env_impl);
  throttle.throttling(1024, g_need_purging_throttling_func, &palf_env_impl);
  ASSERT_EQ(true, throttle.need_throttling_with_options_not_guarded_by_lock_());
  ASSERT_EQ(true, throttle.need_throttling_not_guarded_by_lock_(g_need_purging_throttling_func));
  palf_env_impl.get_throttling_options(throttle_options);
  ASSERT_EQ(throttle_options, throttle.throttling_options_);
  ASSERT_EQ(true, throttle.decay_factor_ > 0.0);
  ASSERT_EQ(1024, throttle.stat_.total_throttling_size_);
  ASSERT_EQ(1, throttle.stat_.total_throttling_task_cnt_);
  ASSERT_EQ(0, throttle.stat_.total_skipped_task_cnt_);
  ASSERT_EQ(0, throttle.stat_.total_skipped_size_);
  ASSERT_EQ(true, throttle.stat_.total_throttling_interval_ > 0);
  ASSERT_EQ(true, OB_INVALID_TIMESTAMP == throttle.stat_.stop_ts_);
  throttle.after_append_log(1024);
  ASSERT_EQ(1024, throttle.appended_log_size_cur_round_);

  expire_throttle_update(throttle);
  unrecyclable_size = total_disk_size * 45/100;
  palf_env_impl.disk_options_wrapper_.set_cur_unrecyclable_log_disk_size(unrecyclable_size);
  throttle.update_throttling_options(&palf_env_impl);
  throttle.throttling(1024, g_need_purging_throttling_func, &palf_env_impl);
  ASSERT_EQ(false, throttle.need_throttling_with_options_not_guarded_by_lock_());

}

// P1-THROTTLE: non-blocking async admission gate. try_admit_async reuses the
// legacy disk-state refresh + need_throttling predicate + decay model, returns
// a delay_us to the async worker, and NEVER sleeps.
TEST_F(TestPalfThrottling, test_try_admit_async)
{
  int64_t total_disk_size = 1024 * 1024 * 1024L;
  int64_t utilization_limit_threshold = 95;
  int64_t throttling_percentage = 60;
  TestPalfThrottleEnv palf_env_impl;
  palf_env_impl.disk_options_wrapper_.disk_opts_for_stopping_writing_.log_disk_throttling_percentage_ = throttling_percentage;
  palf_env_impl.disk_options_wrapper_.disk_opts_for_stopping_writing_.log_disk_throttling_maximum_duration_ = 7200 * 1000 * 1000L;
  palf_env_impl.disk_options_wrapper_.disk_opts_for_stopping_writing_.log_disk_usage_limit_size_ = total_disk_size;
  palf_env_impl.disk_options_wrapper_.disk_opts_for_stopping_writing_.log_disk_utilization_threshold_ = 80;
  palf_env_impl.disk_options_wrapper_.disk_opts_for_stopping_writing_.log_disk_utilization_limit_threshold_ = utilization_limit_threshold;
  int64_t unrecyclable_size = 0;
  palf_env_impl.disk_options_wrapper_.set_cur_unrecyclable_log_disk_size(unrecyclable_size);

  LogWritingThrottle throttle;
  bool can_admit = false;
  int64_t delay_us = 0;

  // case 0: invalid args -> OB_INVALID_ARGUMENT and fail closed.
  NeedPurgingThrottlingFunc invalid_func; // default-constructed -> !is_valid()
  ASSERT_EQ(OB_INVALID_ARGUMENT,
            throttle.try_admit_async(1024, invalid_func, &palf_env_impl, can_admit, delay_us));
  ASSERT_EQ(OB_INVALID_ARGUMENT,
            throttle.try_admit_async(1024, g_need_purging_throttling_func, NULL, can_admit, delay_us));
  ASSERT_EQ(OB_INVALID_ARGUMENT,
            throttle.try_admit_async(-1, g_need_purging_throttling_func, &palf_env_impl, can_admit, delay_us));
  // logical_bytes == 0 -> always admit, no error.
  can_admit = false;
  ASSERT_EQ(OB_SUCCESS,
            throttle.try_admit_async(0, g_need_purging_throttling_func, &palf_env_impl, can_admit, delay_us));
  ASSERT_TRUE(can_admit);
  ASSERT_EQ(0, delay_us);

  // case 1: not notified -> admit regardless of logical_bytes.
  ASSERT_EQ(false, throttle.need_writing_throttling_notified());
  can_admit = false;
  ASSERT_EQ(OB_SUCCESS,
            throttle.try_admit_async(1 << 20, g_need_purging_throttling_func, &palf_env_impl, can_admit, delay_us));
  ASSERT_TRUE(can_admit);
  ASSERT_EQ(0, delay_us);

  // case 2: notified but options below trigger (unrecyclable == 0) -> admit.
  throttle.notify_need_writing_throttling(true);
  expire_throttle_update(throttle);
  can_admit = false;
  ASSERT_EQ(OB_SUCCESS,
            throttle.try_admit_async(1 << 20, g_need_purging_throttling_func, &palf_env_impl, can_admit, delay_us));
  ASSERT_TRUE(can_admit);
  ASSERT_EQ(0, delay_us);
  ASSERT_EQ(false, throttle.need_throttling_not_guarded_by_lock_(g_need_purging_throttling_func));

  // case 3: notified AND over trigger -> async admission is gated and the
  // caller receives the computed delay_us instead of sleeping in place.
  unrecyclable_size = total_disk_size * 70 / 100;
  palf_env_impl.disk_options_wrapper_.set_cur_unrecyclable_log_disk_size(unrecyclable_size);
  expire_throttle_update(throttle);
  can_admit = false;
  ASSERT_EQ(OB_SUCCESS,
            throttle.try_admit_async(1 << 20, g_need_purging_throttling_func, &palf_env_impl, can_admit, delay_us));
  ASSERT_EQ(true, throttle.need_throttling_not_guarded_by_lock_(g_need_purging_throttling_func));
  ASSERT_FALSE(can_admit);
  ASSERT_GT(delay_us, 0);
  ASSERT_EQ(1 << 20, throttle.stat_.total_skipped_size_);
  ASSERT_EQ(1, throttle.stat_.total_skipped_task_cnt_);

  // case 4: in a purge window -> admit even when over trigger.
  g_need_purging_throttling = true;
  delay_us = -1;
  can_admit = false;
  ASSERT_EQ(OB_SUCCESS,
            throttle.try_admit_async(1 << 20, g_need_purging_throttling_func, &palf_env_impl, can_admit, delay_us));
  ASSERT_TRUE(can_admit);
  ASSERT_EQ(0, delay_us);
  g_need_purging_throttling = false;

  // case 5: STOP after space freed. Drop unrecyclable below trigger, wait the
  // refresh interval, and verify need_throttling flips off -> admit.
  unrecyclable_size = total_disk_size * 40 / 100;
  palf_env_impl.disk_options_wrapper_.set_cur_unrecyclable_log_disk_size(unrecyclable_size);
  expire_throttle_update(throttle);
  can_admit = false;
  ASSERT_EQ(OB_SUCCESS,
            throttle.try_admit_async(1 << 20, g_need_purging_throttling_func, &palf_env_impl, can_admit, delay_us));
  ASSERT_TRUE(can_admit);
  ASSERT_EQ(0, delay_us);
  ASSERT_EQ(false, throttle.need_throttling_with_options_not_guarded_by_lock_());
}

} // END of unittest
} // end of oceanbase

int main(int argc, char **argv)
{
  system("rm -rf ./test_palf_throttling.log*");
  OB_LOGGER.set_file_name("test_palf_throttling.log", true);
  OB_LOGGER.set_log_level("INFO");
  PALF_LOG(INFO, "begin unittest::test_palf_throttling");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
