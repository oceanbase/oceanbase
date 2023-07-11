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
#include <random>
#include <string>
#include <pthread.h>

#define private public
#include "logservice/palf/palf_env_impl.h"
#include "logservice/palf/log_io_worker.h"
#undef private
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace palf;

namespace unittest
{

class TestPalfThrottling : public ::testing::Test
{
public:
  TestPalfThrottling();
  virtual ~TestPalfThrottling();
  virtual void SetUp();
  virtual void TearDown();
protected:
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
  //ObMallocAllocator::get_instance()->create_and_add_tenant_allocator(1001);
  // init MTL
  //ObTenantBase tbase(1001);
  //ObTenantEnv::set_tenant(&tbase);
}

void TestPalfThrottling::TearDown()
{
  PALF_LOG(INFO, "TestPalfThrottling has TearDown");
  //ObMallocAllocator::get_instance()->recycle_tenant_allocator(1001);
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

TEST_F(TestPalfThrottling, test_log_write_throttle)
{
  int64_t total_disk_size = 1024 * 1024 * 1024L;
  int64_t utilization_limit_threshold = 95;
  int64_t throttling_percentage = 60;
  PalfEnvImpl palf_env_impl;
  palf_env_impl.is_inited_ = true;
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
  usleep(LogWritingThrottle::UPDATE_INTERVAL_US);
  throttle.update_throttling_options(&palf_env_impl);
  throttle.notify_need_writing_throttling(true);
  throttle.throttling(1024, g_need_purging_throttling_func, &palf_env_impl);
  ASSERT_EQ(false, throttle.need_throttling_not_guarded_by_lock_(g_need_purging_throttling_func));
  ASSERT_EQ(false, throttle.stat_.has_ever_throttled());


  PALF_LOG(INFO, "case 4: test no need throttling while trigger percentage is 100", K(throttle));
  unrecyclable_size = total_disk_size * 70 / 100;
  palf_env_impl.disk_options_wrapper_.set_cur_unrecyclable_log_disk_size(unrecyclable_size);
  palf_env_impl.disk_options_wrapper_.disk_opts_for_stopping_writing_.log_disk_throttling_percentage_ = 100;
  usleep(LogWritingThrottle::UPDATE_INTERVAL_US);
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
  usleep(LogWritingThrottle::UPDATE_INTERVAL_US);
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
  usleep(LogWritingThrottle::UPDATE_INTERVAL_US);
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
  usleep(LogWritingThrottle::UPDATE_INTERVAL_US);
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
  usleep(LogWritingThrottle::UPDATE_INTERVAL_US);
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
  usleep(LogWritingThrottle::UPDATE_INTERVAL_US);
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
  usleep(LogWritingThrottle::UPDATE_INTERVAL_US);
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
  usleep(LogWritingThrottle::UPDATE_INTERVAL_US);
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

  usleep(LogWritingThrottle::UPDATE_INTERVAL_US);
  unrecyclable_size = total_disk_size * 45/100;
  palf_env_impl.disk_options_wrapper_.set_cur_unrecyclable_log_disk_size(unrecyclable_size);
  throttle.update_throttling_options(&palf_env_impl);
  throttle.throttling(1024, g_need_purging_throttling_func, &palf_env_impl);
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
