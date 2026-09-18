/**
 * Copyright (c) 2026 OceanBase
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
#include <chrono>
#include <future>
#include "lib/thread/thread_mgr.h"
#include "logservice/ob_append_callback.h"
#define private public
#include "logservice/applyservice/ob_log_apply_service.h"
#include "logservice/palf/palf_handle.h"
#include "logservice/transportservice/ob_log_transport_service.h"
#undef private
#include "mock_palf_handle_impl_for_async.h"

namespace oceanbase
{
namespace unittest
{
using namespace common;
using namespace logservice;

class ApplyModePalfImpl : public MockAsyncPalfHandleImpl
{
public:
  int get_sync_mode(palf::SyncMode &mode) const override
  {
    mode = mode_;
    return query_ret_;
  }
  palf::SyncMode mode_ = palf::SyncMode::SYNC;
  int query_ret_ = OB_SUCCESS;
};

class CompletionCb : public AppendCb
{
public:
  int on_success() override { completion_.set_value(OB_SUCCESS); return OB_SUCCESS; }
  int on_failure() override { completion_.set_value(OB_NOT_MASTER); return OB_SUCCESS; }
  const char *get_cb_name() const override { return "TestModeDowngradeCb"; }
  std::promise<int> completion_;
};

class TestTransportSyncMode : public ::testing::Test
{
protected:
  void SetUp() override
  {
    palf_handle_.palf_handle_impl_ = &palf_impl_;
    transport_.is_inited_ = true;
    transport_.is_enabled_ = true;
    transport_.palf_handle_ = &palf_handle_;
    transport_.standby_committed_end_lsn_ = palf::LSN(100);
  }

  ApplyModePalfImpl palf_impl_;
  palf::PalfHandle palf_handle_;
  LogTransportStatus transport_;
};

class TestApplySyncMode : public TestTransportSyncMode
{
protected:
  void SetUp() override
  {
    TestTransportSyncMode::SetUp();
    // Exercise real Apply workers; only the PALF mode query is mocked.
    status_.palf_handle_ = &palf_handle_;
    ASSERT_EQ(OB_SUCCESS, TG_CREATE(lib::TGDefIDs::ApplyService, service_.tg_id_));
    service_.is_inited_ = true;
    ASSERT_EQ(OB_SUCCESS, service_.start());
    status_.is_inited_ = true;
    status_.is_in_stop_state_ = false;
    status_.role_ = LEADER;
    status_.proposal_id_ = 10;
    status_.is_sync_mode_ = true;
    status_.palf_committed_end_lsn_ = palf::LSN(200);
    status_.min_committed_end_lsn_ = palf::LSN(100);
    status_.ap_sv_ = &service_;
    status_.inc_ref(); // keep the fixture alive after workers release their refs
    ASSERT_EQ(OB_SUCCESS, status_.submit_task_.init(&status_));
    for (int64_t i = 0; i < APPLY_TASK_QUEUE_SIZE; ++i) {
      ASSERT_EQ(OB_SUCCESS, status_.cb_queues_[i].init(&status_, i));
    }
    cb_.__set_lsn(palf::LSN(150));
    ASSERT_EQ(OB_SUCCESS, status_.cb_queues_[0].push(AppendCb::__get_member_address(&cb_)));
    status_.cb_queues_[0].inc_total_submit_cb_cnt();
  }

  void TearDown() override
  {
    if (service_.tg_id_ >= 0) {
      TG_STOP(service_.tg_id_);
      TG_WAIT(service_.tg_id_);
      TG_DESTROY(service_.tg_id_);
      service_.tg_id_ = -1;
    }
    // Also clean up the pending callback when the old implementation fails.
    for (int64_t i = 0; i < APPLY_TASK_QUEUE_SIZE; ++i) {
      while (nullptr != status_.cb_queues_[i].top()) {
        EXPECT_EQ(OB_SUCCESS, status_.cb_queues_[i].pop());
      }
    }
  }

  void check_downgrade_wakes_callback(const palf::SyncMode mode)
  {
    auto completed = cb_.completion_.get_future();
    bool timeslice_run_out = false;
    ASSERT_EQ(OB_SUCCESS, status_.try_handle_cb_queue(&status_.cb_queues_[0], timeslice_run_out));
    ASSERT_EQ(std::future_status::timeout, completed.wait_for(std::chrono::seconds(0)));
    ASSERT_EQ(OB_SUCCESS, status_.switch_sync_mode(11, mode));
    // No new append, local commit update, or standby acknowledgement follows.
    ASSERT_EQ(std::future_status::ready, completed.wait_for(std::chrono::seconds(1)));
    EXPECT_EQ(OB_SUCCESS, completed.get());
  }

  CompletionCb cb_;
  ObLogApplyService service_;
  ObApplyStatus status_;
};

TEST_F(TestApplySyncMode, pre_async_wakes_committed_callback_without_new_logs)
{
  check_downgrade_wakes_callback(palf::SyncMode::PRE_ASYNC);
}

TEST_F(TestApplySyncMode, async_wakes_committed_callback_without_new_logs)
{
  check_downgrade_wakes_callback(palf::SyncMode::ASYNC);
}

TEST_F(TestApplySyncMode, downgrade_does_not_acknowledge_uncommitted_log)
{
  cb_.__set_lsn(palf::LSN(250));
  auto completed = cb_.completion_.get_future();
  ASSERT_EQ(OB_SUCCESS, status_.switch_sync_mode(11, palf::SyncMode::ASYNC));
  // Drain the submitted work before checking that no callback was acknowledged.
  const int64_t deadline = ObTimeUtility::current_time() + 1000 * 1000;
  while (ATOMIC_LOAD(&status_.ref_cnt_) > 1 && ObTimeUtility::current_time() < deadline) {
    ob_usleep(1000);
  }
  ASSERT_EQ(1, ATOMIC_LOAD(&status_.ref_cnt_));
  EXPECT_EQ(std::future_status::timeout, completed.wait_for(std::chrono::seconds(0)));
}

TEST_F(TestApplySyncMode, transport_downgrade_allows_apply_follower_wakeup)
{
  auto completed = cb_.completion_.get_future();
  bool exhausted = false;
  ASSERT_EQ(OB_SUCCESS, status_.try_handle_cb_queue(&status_.cb_queues_[0], exhausted));
  palf_impl_.mode_ = palf::SyncMode::ASYNC;
  bool done = false;
  ASSERT_EQ(OB_SUCCESS, transport_.is_standby_sync_done(palf::LSN(200), done));
  ASSERT_TRUE(done);
  EXPECT_TRUE(status_.is_sync_mode_);
  EXPECT_EQ(std::future_status::timeout, completed.wait_for(std::chrono::seconds(0)));
  // Model RC switching Apply to follower after the Transport wait completes.
  ASSERT_EQ(OB_SUCCESS, status_.switch_to_follower());
  ASSERT_EQ(std::future_status::ready, completed.wait_for(std::chrono::seconds(1)));
  EXPECT_EQ(OB_SUCCESS, completed.get());
  EXPECT_FALSE(status_.is_sync_mode_);
}

TEST_F(TestTransportSyncMode, transport_observes_actual_mode_each_wait)
{
  bool done = false;
  ASSERT_EQ(OB_SUCCESS, transport_.is_standby_sync_done(palf::LSN(200), done));
  EXPECT_FALSE(done);
  palf_impl_.mode_ = palf::SyncMode::PRE_ASYNC;
  ASSERT_EQ(OB_SUCCESS, transport_.is_standby_sync_done(palf::LSN(200), done));
  EXPECT_TRUE(done);
  palf_impl_.mode_ = palf::SyncMode::SYNC;
  ASSERT_EQ(OB_SUCCESS, transport_.is_standby_sync_done(palf::LSN(200), done));
  EXPECT_FALSE(done);
  palf_impl_.mode_ = palf::SyncMode::ASYNC;
  ASSERT_EQ(OB_SUCCESS, transport_.is_standby_sync_done(palf::LSN(200), done));
  EXPECT_TRUE(done);
}

TEST_F(TestTransportSyncMode, transport_disabled_cache_does_not_bypass_sync_wait)
{
  ASSERT_EQ(OB_SUCCESS, transport_.disable_status(11));
  bool done = true;
  ASSERT_EQ(OB_SUCCESS, transport_.is_standby_sync_done(palf::LSN(200), done));
  EXPECT_FALSE(done);
  ASSERT_EQ(OB_SUCCESS, transport_.is_standby_sync_done(palf::LSN(100), done));
  EXPECT_TRUE(done);
}

TEST_F(TestTransportSyncMode, transport_disabled_cache_does_not_bypass_unknown_mode)
{
  ASSERT_EQ(OB_SUCCESS, transport_.disable_status(11));
  palf_impl_.mode_ = palf::SyncMode::ASYNC;
  palf_impl_.query_ret_ = OB_EAGAIN;
  bool done = true;
  ASSERT_EQ(OB_SUCCESS, transport_.is_standby_sync_done(palf::LSN(200), done));
  EXPECT_FALSE(done);
  palf_impl_.query_ret_ = OB_SUCCESS;
  palf_impl_.mode_ = palf::SyncMode::INVALID_SYNC_MODE;
  ASSERT_EQ(OB_SUCCESS, transport_.is_standby_sync_done(palf::LSN(200), done));
  EXPECT_FALSE(done);
  palf_impl_.mode_ = palf::SyncMode::PRE_ASYNC;
  ASSERT_EQ(OB_SUCCESS, transport_.is_standby_sync_done(palf::LSN(200), done));
  EXPECT_TRUE(done);
  palf_impl_.mode_ = palf::SyncMode::ASYNC;
  ASSERT_EQ(OB_SUCCESS, transport_.is_standby_sync_done(palf::LSN(200), done));
  EXPECT_TRUE(done);
}

TEST_F(TestTransportSyncMode, transport_confirmed_lsn_completes_on_mode_query_error)
{
  bool done = false;
  palf_impl_.query_ret_ = OB_EAGAIN;
  ASSERT_EQ(OB_SUCCESS, transport_.is_standby_sync_done(palf::LSN(100), done));
  EXPECT_TRUE(done);
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_apply_sync_mode.log", true);
  OB_LOGGER.set_log_level("WARN");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
