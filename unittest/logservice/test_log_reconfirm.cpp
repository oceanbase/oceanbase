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
#define private public
#include "logservice/palf/log_reconfirm.h"
#include "mock_logservice_container/mock_log_config_mgr.h"
#include "mock_logservice_container/mock_log_engine.h"
#include "mock_logservice_container/mock_log_sliding_window.h"
#include "mock_logservice_container/mock_log_state_mgr.h"
#include "mock_logservice_container/mock_log_mode_mgr.h"
#undef private

namespace oceanbase
{
using namespace common;
using namespace palf;
using namespace share;

namespace unittest
{

class TestLogReconfirm : public ::testing::Test
{
public:
  TestLogReconfirm();
  virtual ~TestLogReconfirm();
public:
  virtual void SetUp();
  virtual void TearDown();
public:
  class MockLogReconfirm : public LogReconfirm
  {
    public:
      MockLogReconfirm() {}
      ~MockLogReconfirm() {}
  };
  void set_default_config_meta();
public:
  int64_t palf_id_;
  common::ObAddr self_;
  MockLogSlidingWindow mock_sw_;
  MockLogStateMgr mock_state_mgr_;
  MockLogConfigMgr mock_mm_;
  MockLogModeMgr mock_mode_mgr_;
  MockLogEngine mock_log_engine_;
  MockLogReconfirm log_reconfirm_;
};

TestLogReconfirm::TestLogReconfirm() {}
TestLogReconfirm::~TestLogReconfirm() {}

void TestLogReconfirm::SetUp()
{
  palf_id_ = 1001;
  self_.set_ip_addr("127.0.0.1", 12345);
}

void TestLogReconfirm::TearDown()
{}

TEST_F(TestLogReconfirm, test_init)
{
  EXPECT_EQ(OB_INVALID_ARGUMENT, log_reconfirm_.init(palf_id_, self_, NULL,
        &mock_state_mgr_, &mock_mm_, &mock_mode_mgr_, &mock_log_engine_));
  EXPECT_EQ(OB_INVALID_ARGUMENT, log_reconfirm_.init(palf_id_, self_, &mock_sw_,
        NULL, &mock_mm_, &mock_mode_mgr_, &mock_log_engine_));
  EXPECT_EQ(OB_INVALID_ARGUMENT, log_reconfirm_.init(palf_id_, self_, &mock_sw_,
        &mock_state_mgr_, NULL, &mock_mode_mgr_, &mock_log_engine_));
  EXPECT_EQ(OB_INVALID_ARGUMENT, log_reconfirm_.init(palf_id_, self_, &mock_sw_,
        &mock_state_mgr_, &mock_mm_, NULL, &mock_log_engine_));
  EXPECT_EQ(OB_INVALID_ARGUMENT, log_reconfirm_.init(palf_id_, self_, &mock_sw_,
        &mock_state_mgr_, &mock_mm_, &mock_mode_mgr_, NULL));
  EXPECT_EQ(OB_SUCCESS, log_reconfirm_.init(palf_id_, self_, &mock_sw_,
        &mock_state_mgr_, &mock_mm_, &mock_mode_mgr_, &mock_log_engine_));
  EXPECT_EQ(OB_INIT_TWICE, log_reconfirm_.init(palf_id_, self_, &mock_sw_,
        &mock_state_mgr_, &mock_mm_, &mock_mode_mgr_, &mock_log_engine_));
}

TEST_F(TestLogReconfirm, test_reset_state)
{
  EXPECT_EQ(OB_SUCCESS, log_reconfirm_.init(palf_id_, self_, &mock_sw_,
        &mock_state_mgr_, &mock_mm_, &mock_mode_mgr_, &mock_log_engine_));
  log_reconfirm_.reset_state();
  log_reconfirm_.state_ = MockLogReconfirm::WAITING_LOG_FLUSHED;
  log_reconfirm_.reset_state();
}

void TestLogReconfirm::set_default_config_meta()
{
  int64_t curr_proposal_id = 1;
  ObMemberList default_mlist;
  default_mlist.add_server(self_);
  GlobalLearnerList learners;
  LogConfigMeta config_meta;
  LogConfigInfoV2 init_config_info;
  LogConfigVersion init_config_version;
  init_config_version.generate(curr_proposal_id, 0);
  init_config_info.config_.config_version_ = init_config_version;
  EXPECT_EQ(OB_SUCCESS, init_config_info.generate(default_mlist, 1, learners, init_config_version));
  config_meta.curr_ = init_config_info;
  mock_mm_.log_ms_meta_ = config_meta;
}

TEST_F(TestLogReconfirm, test_init_reconfirm)
{
  EXPECT_EQ(OB_SUCCESS, log_reconfirm_.init(palf_id_, self_, &mock_sw_,
        &mock_state_mgr_, &mock_mm_, &mock_mode_mgr_, &mock_log_engine_));
  EXPECT_EQ(OB_NOT_INIT, log_reconfirm_.init_reconfirm_());
  set_default_config_meta();
  EXPECT_EQ(OB_SUCCESS, log_reconfirm_.init_reconfirm_());
}

TEST_F(TestLogReconfirm, test_handle_prepare_response)
{
  int64_t src_proposal_id = 2;
  int64_t src_accept_proposal_id = 1;
  ObAddr src_server;
  LSN last_lsn;
  LSN committed_end_lsn;
  EXPECT_EQ(OB_NOT_INIT, log_reconfirm_.handle_prepare_response(src_server, src_proposal_id,
        src_accept_proposal_id, last_lsn, committed_end_lsn));
  EXPECT_EQ(OB_SUCCESS, log_reconfirm_.init(palf_id_, self_, &mock_sw_,
        &mock_state_mgr_, &mock_mm_, &mock_mode_mgr_, &mock_log_engine_));
  EXPECT_EQ(OB_INVALID_ARGUMENT, log_reconfirm_.handle_prepare_response(src_server, src_proposal_id,
        src_accept_proposal_id, last_lsn, committed_end_lsn));
  src_server = self_;
  log_reconfirm_.curr_paxos_follower_list_.add_server(self_);
  last_lsn.val_ = 0;
  committed_end_lsn.val_ = 0;
  log_reconfirm_.new_proposal_id_ = 2;
  EXPECT_EQ(OB_STATE_NOT_MATCH, log_reconfirm_.handle_prepare_response(src_server, src_proposal_id,
        src_accept_proposal_id, last_lsn, committed_end_lsn));
  // src_proposal_id < majority_max_accept_pid_, not receive
  log_reconfirm_.majority_max_accept_pid_ = 2;
  log_reconfirm_.majority_max_lsn_.val_ = 100;
  last_lsn.val_ = 50;
  log_reconfirm_.state_ = MockLogReconfirm::FETCH_MAX_LOG_LSN;
  EXPECT_EQ(OB_SUCCESS, log_reconfirm_.handle_prepare_response(src_server, src_proposal_id,
        src_accept_proposal_id, last_lsn, committed_end_lsn));
  // src_proposal_id == majority_max_accept_pid_, receive
  src_accept_proposal_id = 2;
  last_lsn.val_ = 120;
  committed_end_lsn.val_ = 100;
  EXPECT_EQ(0, log_reconfirm_.follower_end_lsn_list_.count());
  EXPECT_EQ(OB_SUCCESS, log_reconfirm_.handle_prepare_response(src_server, src_proposal_id,
        src_accept_proposal_id, last_lsn, committed_end_lsn));
  EXPECT_EQ(1, log_reconfirm_.follower_end_lsn_list_.count());
  EXPECT_EQ(committed_end_lsn, log_reconfirm_.follower_end_lsn_list_[0].last_flushed_end_lsn_);
  EXPECT_EQ(last_lsn, log_reconfirm_.majority_max_lsn_);
  log_reconfirm_.majority_max_accept_pid_ = 1;
  EXPECT_EQ(OB_SUCCESS, log_reconfirm_.handle_prepare_response(src_server, src_proposal_id,
        src_accept_proposal_id, last_lsn, committed_end_lsn));
}

TEST_F(TestLogReconfirm, test_try_fetch_log)
{
  EXPECT_EQ(OB_SUCCESS, log_reconfirm_.init(palf_id_, self_, &mock_sw_,
        &mock_state_mgr_, &mock_mm_, &mock_mode_mgr_, &mock_log_engine_));
  mock_sw_.max_flushed_end_lsn_.val_ = 100;
  log_reconfirm_.majority_max_lsn_.val_ = 100;
  EXPECT_EQ(true, log_reconfirm_.is_fetch_log_finished_());
  mock_sw_.mock_start_id_ = 1;
  EXPECT_EQ(OB_SUCCESS, log_reconfirm_.try_fetch_log_());
  EXPECT_EQ(OB_SUCCESS, log_reconfirm_.try_fetch_log_());
  mock_sw_.mock_start_id_ = 5;
  EXPECT_EQ(OB_SUCCESS, log_reconfirm_.try_fetch_log_());
  EXPECT_EQ(OB_SUCCESS, log_reconfirm_.try_fetch_log_());
}

} // END of unittest
} // end of oceanbase

int main(int argc, char **argv)
{
  system("rm -f ./test_log_reconfirm.log");
  OB_LOGGER.set_file_name("test_log_reconfirm.log", true);
  OB_LOGGER.set_log_level("TRACE");
  PALF_LOG(INFO, "begin unittest::test_log_reconfirm");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
