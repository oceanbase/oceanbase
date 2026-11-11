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
#include "share/ob_cluster_version.h"
#define private public
#include "mock_logservice_container/mock_log_engine.h"
#include "mock_logservice_container/mock_election.h"
#include "mock_logservice_container/mock_log_sliding_window.h"
#include "mock_logservice_container/mock_log_reconfirm.h"
#include "mock_logservice_container/mock_log_config_mgr.h"
#include "mock_logservice_container/mock_log_mode_mgr.h"
#include "mock_logservice_container/mock_palf_role_change_cb_wrapper.h"
#undef private

namespace oceanbase
{
using namespace common;
using namespace palf;
using namespace share;

namespace unittest
{

class RecordingFetchWindow : public MockLogSlidingWindow
{
public:
  RecordingFetchWindow() : fetch_count_(0), ack_count_(0), fetch_ret_(OB_SUCCESS),
      last_trigger_(FetchTriggerType::SLIDING_CB) {}
  int try_fetch_log(const FetchTriggerType &trigger, const LSN prev_lsn = LSN(),
                    const LSN start_lsn = LSN(), const int64_t start_id = OB_INVALID_LOG_ID) override
  {
    UNUSEDx(prev_lsn, start_lsn, start_id);
    ++fetch_count_;
    last_trigger_ = trigger;
    return fetch_ret_;
  }
  int submit_push_log_resp(const ObAddr &server) override
  {
    UNUSED(server);
    ++ack_count_;
    return OB_SUCCESS;
  }
  int64_t fetch_count_;
  int64_t ack_count_;
  int fetch_ret_;
  FetchTriggerType last_trigger_;
};

class TestLogStateMgr: public ::testing::Test
{
public:
  TestLogStateMgr();
  virtual ~TestLogStateMgr();
public:
  virtual void SetUp();
  virtual void TearDown();
  void init_mode_fetch(RecordingFetchWindow &sw);
public:
  int64_t palf_id_;
  common::ObAddr self_;
  LogPrepareMeta log_prepare_meta_;
  LogReplicaPropertyMeta log_replica_property_meta_;
  ::oceanbase::palf::mockelection::MockElection mock_election_;
  MockLogSlidingWindow mock_sw_;
  MockLogReconfirm mock_reconfirm_;
  MockLogEngine mock_log_engine_;
  MockLogConfigMgr mock_config_mgr_;
  MockLogModeMgr mock_mode_mgr_;
  MockPalfRoleChangeCbWrapper mock_role_change_cb_;
  LogPlugins plugins_;
  LogStateMgr state_mgr_;
};

TestLogStateMgr::TestLogStateMgr() {}
TestLogStateMgr::~TestLogStateMgr() {}

void TestLogStateMgr::SetUp()
{
  palf_id_ = 1001;
  self_.set_ip_addr("127.0.0.1", 12345);
  LogVotedFor voted_for;
  log_prepare_meta_.generate(voted_for, 1);
  log_replica_property_meta_.generate(
      true, LogReplicaType::NORMAL_REPLICA, LogIOMode::SYNC);
}

void TestLogStateMgr::TearDown()
{}

void TestLogStateMgr::init_mode_fetch(RecordingFetchWindow &sw)
{
  ASSERT_EQ(OB_SUCCESS, state_mgr_.init(palf_id_, self_, log_prepare_meta_, log_replica_property_meta_,
      &mock_election_, &sw, &mock_reconfirm_, &mock_log_engine_, &mock_config_mgr_,
      &mock_mode_mgr_, &mock_role_change_cb_, &plugins_));
  state_mgr_.prepare_meta_.log_proposal_id_ = 2;
  state_mgr_.role_ = FOLLOWER;
  state_mgr_.state_ = ACTIVE;
  state_mgr_.leader_.set_ip_addr("127.0.0.1", 12346);
  mock_mode_mgr_.is_inited_ = true;
  ASSERT_EQ(OB_SUCCESS, mock_mode_mgr_.applied_mode_meta_.generate(
      1, 1, AccessMode::APPEND, SyncMode::ASYNC, SCN::min_scn()));
  ASSERT_EQ(OB_SUCCESS, mock_mode_mgr_.mock_accepted_mode_meta_.generate(
      2, 2, AccessMode::APPEND, SyncMode::SYNC, SCN::min_scn()));
  state_mgr_.last_check_start_id_ = sw.get_start_id();
  state_mgr_.last_check_start_id_time_us_ = ObTimeUtility::current_time();
}

TEST_F(TestLogStateMgr, pending_mode_uses_short_stagnation_interval)
{
  RecordingFetchWindow sw;
  init_mode_fetch(sw);
  state_mgr_.last_check_start_id_time_us_ -= 300 * 1000;
  EXPECT_EQ(OB_SUCCESS, state_mgr_.check_and_try_fetch_log_());
  EXPECT_EQ(1, sw.fetch_count_);
  EXPECT_EQ(1, sw.ack_count_);
  EXPECT_EQ(FetchTriggerType::LOG_LOOP_TH, sw.last_trigger_);
  EXPECT_EQ(OB_SUCCESS, state_mgr_.check_and_try_fetch_log_());
  EXPECT_EQ(1, sw.fetch_count_);
}

TEST_F(TestLogStateMgr, pending_mode_failure_keeps_previous_retry_behavior)
{
  RecordingFetchWindow sw;
  init_mode_fetch(sw);
  sw.fetch_ret_ = OB_EAGAIN;
  const int64_t expired_time = ObTimeUtility::current_time() - 300 * 1000;
  for (const int64_t check_time : {OB_INVALID_TIMESTAMP, expired_time}) {
    state_mgr_.last_check_start_id_ = OB_INVALID_TIMESTAMP == check_time ?
        OB_INVALID_LOG_ID : sw.get_start_id();
    state_mgr_.last_check_start_id_time_us_ = check_time;
    const int64_t start_id = state_mgr_.last_check_start_id_;
    const int64_t previous_count = sw.fetch_count_;
    for (int i = 0; i < 100; ++i) {
      if (OB_EAGAIN != state_mgr_.check_and_try_fetch_log_()) {
        ADD_FAILURE() << "A failed fetch must not defer the next ordinary check";
        break;
      }
    }
    EXPECT_EQ(previous_count + 100, sw.fetch_count_);
    EXPECT_EQ(start_id, state_mgr_.last_check_start_id_);
    EXPECT_EQ(check_time, state_mgr_.last_check_start_id_time_us_);
  }
  EXPECT_EQ(0, sw.ack_count_);

  // A successful check still consumes the short interval, including a no-op fetch.
  sw.fetch_ret_ = OB_SUCCESS;
  EXPECT_EQ(OB_SUCCESS, state_mgr_.check_and_try_fetch_log_());
  const int64_t successful_count = sw.fetch_count_;
  EXPECT_NE(OB_INVALID_TIMESTAMP, state_mgr_.last_check_start_id_time_us_);
  EXPECT_EQ(sw.get_start_id(), state_mgr_.last_check_start_id_);
  EXPECT_EQ(OB_SUCCESS, state_mgr_.check_and_try_fetch_log_());
  EXPECT_EQ(successful_count, sw.fetch_count_);
}

TEST_F(TestLogStateMgr, mode_meta_flush_arms_next_check_without_fetching)
{
  RecordingFetchWindow sw;
  init_mode_fetch(sw);
  state_mgr_.on_mode_meta_flushed();
  EXPECT_EQ(OB_INVALID_TIMESTAMP, state_mgr_.last_check_start_id_time_us_);
  EXPECT_EQ(0, sw.fetch_count_);
  EXPECT_EQ(OB_SUCCESS, state_mgr_.check_and_try_fetch_log_());
  EXPECT_EQ(1, sw.fetch_count_);
  EXPECT_EQ(1, sw.ack_count_);
  EXPECT_NE(OB_INVALID_TIMESTAMP, state_mgr_.last_check_start_id_time_us_);
}

TEST_F(TestLogStateMgr, sliding_progress_defers_short_interval_fetch)
{
  RecordingFetchWindow sw;
  init_mode_fetch(sw);
  state_mgr_.last_check_start_id_time_us_ -= 300 * 1000;
  ++sw.mock_start_id_;
  EXPECT_EQ(OB_SUCCESS, state_mgr_.check_and_try_fetch_log_());
  EXPECT_EQ(0, sw.fetch_count_);
  EXPECT_EQ(sw.get_start_id(), state_mgr_.last_check_start_id_);
  const int64_t progress_time = state_mgr_.last_check_start_id_time_us_;
  EXPECT_EQ(OB_SUCCESS, state_mgr_.check_and_try_fetch_log_());
  EXPECT_EQ(0, sw.fetch_count_);
  EXPECT_EQ(progress_time, state_mgr_.last_check_start_id_time_us_);
}

TEST_F(TestLogStateMgr, applied_mode_restores_two_second_interval)
{
  RecordingFetchWindow sw;
  init_mode_fetch(sw);
  mock_mode_mgr_.applied_mode_meta_ = mock_mode_mgr_.mock_accepted_mode_meta_;
  state_mgr_.last_check_start_id_time_us_ -= 500 * 1000;
  const int64_t check_time = state_mgr_.last_check_start_id_time_us_;
  state_mgr_.on_mode_meta_flushed();
  EXPECT_EQ(check_time, state_mgr_.last_check_start_id_time_us_);
  EXPECT_EQ(OB_SUCCESS, state_mgr_.check_and_try_fetch_log_());
  EXPECT_EQ(0, sw.fetch_count_);
  state_mgr_.last_check_start_id_time_us_ -= 1600 * 1000;
  EXPECT_EQ(OB_SUCCESS, state_mgr_.check_and_try_fetch_log_());
  EXPECT_EQ(1, sw.fetch_count_);
  EXPECT_EQ(0, sw.ack_count_);
}

TEST_F(TestLogStateMgr, reconfirm_and_stale_meta_keep_ordinary_interval)
{
  RecordingFetchWindow sw;
  init_mode_fetch(sw);
  // Reconfirm retains the previous mode_version even when accepted proposal advances.
  mock_mode_mgr_.mock_accepted_mode_meta_.mode_version_ = 1;
  state_mgr_.last_check_start_id_time_us_ -= 500 * 1000;
  const int64_t check_time = state_mgr_.last_check_start_id_time_us_;
  state_mgr_.on_mode_meta_flushed();
  EXPECT_EQ(check_time, state_mgr_.last_check_start_id_time_us_);
  EXPECT_EQ(OB_SUCCESS, state_mgr_.check_and_try_fetch_log_());
  EXPECT_EQ(0, sw.fetch_count_);
  mock_mode_mgr_.mock_accepted_mode_meta_.mode_version_ = 2;
  state_mgr_.prepare_meta_.log_proposal_id_ = 3;
  state_mgr_.on_mode_meta_flushed();
  EXPECT_EQ(check_time, state_mgr_.last_check_start_id_time_us_);
  EXPECT_EQ(OB_SUCCESS, state_mgr_.check_and_try_fetch_log_());
  EXPECT_EQ(0, sw.fetch_count_);
}

TEST_F(TestLogStateMgr, disabled_sync_and_leader_do_not_arm_mode_fetch)
{
  RecordingFetchWindow sw;
  init_mode_fetch(sw);
  const int64_t check_time = state_mgr_.last_check_start_id_time_us_;
  state_mgr_.is_sync_enabled_ = false;
  state_mgr_.on_mode_meta_flushed();
  EXPECT_EQ(check_time, state_mgr_.last_check_start_id_time_us_);
  EXPECT_EQ(OB_SUCCESS, state_mgr_.check_and_try_fetch_log_());
  EXPECT_EQ(0, sw.fetch_count_);
  state_mgr_.is_sync_enabled_ = true;
  state_mgr_.role_ = LEADER;
  state_mgr_.on_mode_meta_flushed();
  EXPECT_EQ(check_time, state_mgr_.last_check_start_id_time_us_);
}

TEST_F(TestLogStateMgr, ordinary_fetch_failure_keeps_previous_retry_behavior)
{
  RecordingFetchWindow sw;
  init_mode_fetch(sw);
  mock_mode_mgr_.applied_mode_meta_ = mock_mode_mgr_.mock_accepted_mode_meta_;
  sw.fetch_ret_ = OB_EAGAIN;
  state_mgr_.last_check_start_id_time_us_ = OB_INVALID_TIMESTAMP;
  EXPECT_EQ(OB_EAGAIN, state_mgr_.check_and_try_fetch_log_());
  EXPECT_EQ(OB_EAGAIN, state_mgr_.check_and_try_fetch_log_());
  EXPECT_EQ(2, sw.fetch_count_);
  EXPECT_EQ(OB_INVALID_TIMESTAMP, state_mgr_.last_check_start_id_time_us_);
}

TEST_F(TestLogStateMgr, test_init)
{
  EXPECT_EQ(OB_INVALID_ARGUMENT, state_mgr_.init(palf_id_, self_, log_prepare_meta_, log_replica_property_meta_, NULL, &mock_sw_,
        &mock_reconfirm_, &mock_log_engine_, &mock_config_mgr_, &mock_mode_mgr_, &mock_role_change_cb_, &plugins_));
  EXPECT_EQ(OB_INVALID_ARGUMENT, state_mgr_.init(palf_id_, self_, log_prepare_meta_, log_replica_property_meta_, &mock_election_, NULL,
        &mock_reconfirm_, &mock_log_engine_, &mock_config_mgr_, &mock_mode_mgr_, &mock_role_change_cb_, &plugins_));
  EXPECT_EQ(OB_SUCCESS, state_mgr_.init(palf_id_, self_, log_prepare_meta_, log_replica_property_meta_, &mock_election_, &mock_sw_,
        &mock_reconfirm_, &mock_log_engine_, &mock_config_mgr_, &mock_mode_mgr_, &mock_role_change_cb_, &plugins_));
  EXPECT_EQ(OB_INIT_TWICE, state_mgr_.init(palf_id_, self_, log_prepare_meta_, log_replica_property_meta_, &mock_election_, &mock_sw_,
        &mock_reconfirm_, &mock_log_engine_, &mock_config_mgr_, &mock_mode_mgr_, &mock_role_change_cb_, &plugins_));
}

TEST_F(TestLogStateMgr, logonly_replica_state_caps)
{
  ASSERT_EQ(OB_SUCCESS,
            log_replica_property_meta_.generate(
                true /* allow_vote */, LogReplicaType::LOGONLY_REPLICA, LogIOMode::SYNC));
  EXPECT_EQ(OB_SUCCESS, state_mgr_.init(palf_id_, self_, log_prepare_meta_, log_replica_property_meta_, &mock_election_, &mock_sw_,
        &mock_reconfirm_, &mock_log_engine_, &mock_config_mgr_, &mock_mode_mgr_, &mock_role_change_cb_, &plugins_));
  EXPECT_TRUE(state_mgr_.is_logonly_replica());
  EXPECT_FALSE(state_mgr_.is_arb_replica());
  EXPECT_FALSE(state_mgr_.can_be_active_leader());
  EXPECT_TRUE(state_mgr_.is_sync_enabled());
}

TEST_F(TestLogStateMgr, reject_log_and_ack_from_old_proposal)
{
  ASSERT_EQ(OB_SUCCESS, state_mgr_.init(palf_id_, self_, log_prepare_meta_, log_replica_property_meta_,
      &mock_election_, &mock_sw_, &mock_reconfirm_, &mock_log_engine_, &mock_config_mgr_,
      &mock_mode_mgr_, &mock_role_change_cb_, &plugins_));
  state_mgr_.prepare_meta_.log_proposal_id_ = 2;

  state_mgr_.role_ = FOLLOWER;
  state_mgr_.state_ = ACTIVE;
  EXPECT_FALSE(state_mgr_.can_receive_log(1));
  EXPECT_TRUE(state_mgr_.can_receive_log(2));

  state_mgr_.role_ = LEADER;
  EXPECT_FALSE(state_mgr_.can_receive_log_ack(1));
  EXPECT_TRUE(state_mgr_.can_receive_log_ack(2));
}

TEST_F(TestLogStateMgr, replay_to_leader_active)
{
  EXPECT_EQ(OB_SUCCESS, state_mgr_.init(palf_id_, self_, log_prepare_meta_, log_replica_property_meta_, &mock_election_, &mock_sw_,
        &mock_reconfirm_, &mock_log_engine_, &mock_config_mgr_, &mock_mode_mgr_, &mock_role_change_cb_, &plugins_));
  // set default config meta
  ObMemberList default_mlist;
  default_mlist.add_server(self_);
  GlobalLearnerList learners;
  LogConfigMeta config_meta;
  LogConfigInfoV2 init_config_info;
  LogConfigVersion init_config_version;
  EXPECT_EQ(OB_SUCCESS, init_config_version.generate(1, 0));
  EXPECT_EQ(OB_SUCCESS, init_config_info.generate(default_mlist, 1, learners, init_config_version));
  config_meta.curr_ = init_config_info;
  mock_config_mgr_.log_ms_meta_ = config_meta;

  EXPECT_EQ(OB_SUCCESS, state_mgr_.set_scan_disk_log_finished());
  EXPECT_EQ(OB_SUCCESS, state_mgr_.switch_state());  // replay to follower active
  EXPECT_EQ(true, state_mgr_.is_follower_active());
  mock_config_mgr_.self_ = self_;
  mock_election_.self_ = self_;

  // test follower to leader
  mock_election_.leader_ = self_;
  mock_election_.leader_epoch_ = 10;
  EXPECT_EQ(OB_SUCCESS, state_mgr_.switch_state());
  EXPECT_EQ(true, state_mgr_.is_leader_active());

  // test leader to follower
  mock_election_.leader_.reset();
  mock_election_.leader_epoch_ = 0;
  mock_sw_.pending_end_lsn_.val_ = 100;
  mock_sw_.last_slide_end_lsn_.val_ = 90;
  EXPECT_EQ(OB_SUCCESS, state_mgr_.switch_state());
  EXPECT_EQ(true, state_mgr_.is_follower_pending());
  mock_sw_.last_slide_end_lsn_.val_ = 100;
  EXPECT_EQ(OB_SUCCESS, state_mgr_.switch_state());
  EXPECT_EQ(true, state_mgr_.is_follower_active());

  // test follower pending to reconfirm to leader active
  mock_election_.leader_ = self_;
  mock_election_.leader_epoch_ = 11;
  EXPECT_EQ(OB_SUCCESS, state_mgr_.switch_state());
  EXPECT_EQ(true, state_mgr_.is_leader_active());
  mock_election_.leader_.reset();
  mock_election_.leader_epoch_ = 0;
  mock_sw_.pending_end_lsn_.val_ = 200;
  mock_sw_.last_slide_end_lsn_.val_ = 190;
  EXPECT_EQ(OB_SUCCESS, state_mgr_.switch_state());
  EXPECT_EQ(true, state_mgr_.is_follower_pending());
  // pending to reconfirm
  mock_election_.leader_ = self_;
  mock_election_.leader_epoch_ = 12;
  mock_reconfirm_.mock_ret_ = OB_EAGAIN;
  EXPECT_EQ(OB_SUCCESS, state_mgr_.switch_state());
  EXPECT_EQ(true, state_mgr_.is_leader_reconfirm());
  // reconfirm to pending
  mock_election_.leader_.reset();
  mock_election_.leader_epoch_ = 0;
  mock_sw_.pending_end_lsn_.val_ = 300;
  mock_sw_.last_slide_end_lsn_.val_ = 290;
  EXPECT_EQ(OB_SUCCESS, state_mgr_.switch_state());
  EXPECT_EQ(true, state_mgr_.is_follower_pending());
  // pending to reconfirm
  mock_election_.leader_ = self_;
  mock_election_.leader_epoch_ = 13;
  EXPECT_EQ(OB_SUCCESS, state_mgr_.switch_state());
  EXPECT_EQ(true, state_mgr_.is_leader_reconfirm());
  // reconfirm to pending
  // reconfirm to leader active
  mock_reconfirm_.mock_ret_ = OB_SUCCESS;
  EXPECT_EQ(OB_SUCCESS, state_mgr_.switch_state());
  EXPECT_EQ(true, state_mgr_.is_leader_active());
}

} // END of unittest
} // end of oceanbase

int main(int argc, char **argv)
{
  system("rm -f ./test_log_state_mgr.log");
  OB_LOGGER.set_file_name("test_log_state_mgr.log", true);
  OB_LOGGER.set_log_level("TRACE");
  PALF_LOG(INFO, "begin unittest::test_log_state_mgr");
  ::testing::InitGoogleTest(&argc, argv);
  oceanbase::ObClusterVersion::get_instance().update_data_version(DATA_CURRENT_VERSION);
  oceanbase::ObClusterVersion::get_instance().update_cluster_version(CLUSTER_CURRENT_VERSION);
  return RUN_ALL_TESTS();
}
