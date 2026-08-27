/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#define USING_LOG_PREFIX SERVER
#define private public
#define protected public
#include "observer/table/ttl/ob_tenant_ttl_manager.h"
#undef protected
#undef private

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::table;

namespace
{
const uint64_t TEST_TENANT_ID = 1001;
const int64_t TWO_DAYS_US = 2L * 24L * 60L * 60L * 1000L * 1000L;
const int64_t JAN_2_2024_NOON_UTC_US = 1704196800000000L;
const int64_t JAN_2_2025_NOON_UTC_US = 1735819200000000L;

class MockTTLTaskScheduler : public ObTTLTaskScheduler
{
public:
  MockTTLTaskScheduler()
    : fetch_ret_(OB_SUCCESS), insert_ret_(OB_SUCCESS), fetch_count_(0), insert_count_(0), next_task_id_(1)
  {}

  virtual bool enable_scheduler() override
  {
    return true;
  }

  virtual int fetch_ttl_task_id(uint64_t tenant_id, int64_t &new_task_id) override
  {
    UNUSED(tenant_id);
    ++fetch_count_;
    if (OB_SUCCESS == fetch_ret_) {
      new_task_id = next_task_id_++;
    }
    return fetch_ret_;
  }

  virtual int insert_tenant_task(ObTTLStatus &ttl_task) override
  {
    UNUSED(ttl_task);
    ++insert_count_;
    return insert_ret_;
  }

  int fetch_ret_;
  int insert_ret_;
  int64_t fetch_count_;
  int64_t insert_count_;
  int64_t next_task_id_;
};

class TestTTLTaskScheduler : public ::testing::Test
{
public:
  TestTTLTaskScheduler() : tenant_base_(OB_SYS_TENANT_ID), scheduler_(nullptr) {}

  virtual void SetUp() override
  {
    ObTenantEnv::set_tenant(&tenant_base_);
    scheduler_ = new MockTTLTaskScheduler();
    scheduler_->is_inited_ = true;
    scheduler_->tenant_id_ = TEST_TENANT_ID;
  }

  virtual void TearDown() override
  {
    delete scheduler_;
    scheduler_ = nullptr;
    ObTenantEnv::set_tenant(nullptr);
  }

protected:
  ObTenantBase tenant_base_;
  MockTTLTaskScheduler *scheduler_;
};

TEST_F(TestTTLTaskScheduler, running_task_does_not_consume_periodic_opportunity)
{
  const int64_t current_ts = ObTimeUtility::current_time();
  scheduler_->tenant_task_.is_finished_ = false;
  scheduler_->tenant_task_.ttl_status_.trigger_type_ =
      static_cast<int64_t>(TRIGGER_TYPE::USER_TRIGGER);

  ASSERT_EQ(OB_SUCCESS, scheduler_->try_add_periodic_task_in_active_time(current_ts));
  ASSERT_EQ(0, scheduler_->insert_count_);
  ASSERT_FALSE(scheduler_->periodic_launched_);
  ASSERT_EQ(OB_INVALID_TIMESTAMP, scheduler_->last_launched_ts_);

  scheduler_->tenant_task_.is_finished_ = true;
  ASSERT_EQ(OB_SUCCESS, scheduler_->try_add_periodic_task_in_active_time(current_ts));
  ASSERT_EQ(1, scheduler_->insert_count_);
  ASSERT_TRUE(scheduler_->periodic_launched_);
  ASSERT_EQ(scheduler_->tenant_task_.ttl_status_.task_start_time_, scheduler_->last_launched_ts_);
  ASSERT_EQ(static_cast<int64_t>(TRIGGER_TYPE::PERIODIC_TRIGGER),
            scheduler_->tenant_task_.ttl_status_.trigger_type_);
}

TEST_F(TestTTLTaskScheduler, reloaded_periodic_task_restores_launch_watermark)
{
  const int64_t current_ts = ObTimeUtility::current_time();
  scheduler_->tenant_task_.is_finished_ = false;
  scheduler_->tenant_task_.ttl_status_.trigger_type_ =
      static_cast<int64_t>(TRIGGER_TYPE::PERIODIC_TRIGGER);
  scheduler_->tenant_task_.ttl_status_.task_start_time_ = current_ts;

  ASSERT_EQ(OB_SUCCESS, scheduler_->try_add_periodic_task_in_active_time(current_ts));
  ASSERT_EQ(0, scheduler_->insert_count_);
  ASSERT_TRUE(scheduler_->periodic_launched_);
  ASSERT_EQ(current_ts, scheduler_->last_launched_ts_);

  scheduler_->tenant_task_.is_finished_ = true;
  ASSERT_EQ(OB_SUCCESS, scheduler_->try_add_periodic_task_in_active_time(current_ts));
  ASSERT_EQ(0, scheduler_->insert_count_);
}

TEST_F(TestTTLTaskScheduler, failed_periodic_task_creation_is_retryable)
{
  const int64_t current_ts = ObTimeUtility::current_time();
  scheduler_->insert_ret_ = OB_ERR_UNEXPECTED;

  ASSERT_EQ(OB_ERR_UNEXPECTED, scheduler_->try_add_periodic_task_in_active_time(current_ts));
  ASSERT_EQ(1, scheduler_->insert_count_);
  ASSERT_FALSE(scheduler_->periodic_launched_);
  ASSERT_EQ(OB_INVALID_TIMESTAMP, scheduler_->last_launched_ts_);

  scheduler_->insert_ret_ = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS, scheduler_->try_add_periodic_task_in_active_time(current_ts));
  ASSERT_EQ(2, scheduler_->insert_count_);
  ASSERT_TRUE(scheduler_->periodic_launched_);
  ASSERT_EQ(scheduler_->tenant_task_.ttl_status_.task_start_time_, scheduler_->last_launched_ts_);
}

TEST_F(TestTTLTaskScheduler, successful_launch_is_not_repeated_on_same_day)
{
  const int64_t current_ts = ObTimeUtility::current_time();

  ASSERT_EQ(OB_SUCCESS, scheduler_->try_add_periodic_task_in_active_time(current_ts));
  ASSERT_EQ(1, scheduler_->insert_count_);

  const int64_t launch_ts = scheduler_->last_launched_ts_;
  scheduler_->tenant_task_.is_finished_ = true;
  ASSERT_EQ(OB_SUCCESS, scheduler_->try_add_periodic_task_in_active_time(launch_ts));
  ASSERT_EQ(1, scheduler_->insert_count_);
}

TEST_F(TestTTLTaskScheduler, same_day_of_year_in_different_year_is_a_new_day)
{
  scheduler_->periodic_launched_ = true;
  scheduler_->last_launched_ts_ = JAN_2_2024_NOON_UTC_US;

  ASSERT_FALSE(scheduler_->is_last_launched_same_day(JAN_2_2025_NOON_UTC_US));
  ASSERT_EQ(OB_SUCCESS,
            scheduler_->try_add_periodic_task_in_active_time(JAN_2_2025_NOON_UTC_US));
  ASSERT_EQ(1, scheduler_->insert_count_);
  ASSERT_EQ(scheduler_->tenant_task_.ttl_status_.task_start_time_, scheduler_->last_launched_ts_);
  ASSERT_NE(JAN_2_2024_NOON_UTC_US, scheduler_->last_launched_ts_);
}

TEST_F(TestTTLTaskScheduler, all_day_window_launches_again_on_a_new_day)
{
  const int64_t current_ts = ObTimeUtility::current_time();
  const int64_t previous_launch_ts = current_ts - TWO_DAYS_US;
  scheduler_->periodic_launched_ = true;
  scheduler_->last_launched_ts_ = previous_launch_ts;

  ASSERT_EQ(OB_SUCCESS, scheduler_->try_add_periodic_task_in_active_time(current_ts));
  ASSERT_EQ(1, scheduler_->insert_count_);
  ASSERT_EQ(scheduler_->tenant_task_.ttl_status_.task_start_time_, scheduler_->last_launched_ts_);
  ASSERT_NE(previous_launch_ts, scheduler_->last_launched_ts_);
}

TEST_F(TestTTLTaskScheduler, running_task_does_not_consume_new_day_opportunity)
{
  const int64_t current_ts = ObTimeUtility::current_time();
  const int64_t previous_launch_ts = current_ts - TWO_DAYS_US;
  scheduler_->periodic_launched_ = true;
  scheduler_->last_launched_ts_ = previous_launch_ts;
  scheduler_->tenant_task_.is_finished_ = false;
  scheduler_->tenant_task_.ttl_status_.trigger_type_ =
      static_cast<int64_t>(TRIGGER_TYPE::USER_TRIGGER);

  ASSERT_EQ(OB_SUCCESS, scheduler_->try_add_periodic_task_in_active_time(current_ts));
  ASSERT_EQ(0, scheduler_->insert_count_);
  ASSERT_EQ(previous_launch_ts, scheduler_->last_launched_ts_);

  scheduler_->tenant_task_.is_finished_ = true;
  ASSERT_EQ(OB_SUCCESS, scheduler_->try_add_periodic_task_in_active_time(current_ts));
  ASSERT_EQ(1, scheduler_->insert_count_);
  ASSERT_EQ(scheduler_->tenant_task_.ttl_status_.task_start_time_, scheduler_->last_launched_ts_);
  ASSERT_NE(previous_launch_ts, scheduler_->last_launched_ts_);
}
} // namespace

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_ttl_task_scheduler.log", true);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
