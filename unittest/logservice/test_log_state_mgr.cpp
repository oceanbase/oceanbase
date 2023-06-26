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
#include "mock_logservice_container/mock_log_engine.h"
#include "mock_logservice_container/mock_election.h"
#include "mock_logservice_container/mock_log_sliding_window.h"
#include "mock_logservice_container/mock_log_reconfirm.h"
#include "mock_logservice_container/mock_log_config_mgr.h"
#include "mock_logservice_container/mock_log_mode_mgr.h"
#include "mock_logservice_container/mock_palf_role_change_cb_wrapper.h"
#include "logservice/palf/log_state_mgr.h"
#undef private

namespace oceanbase
{
using namespace common;
using namespace palf;
using namespace share;

namespace unittest
{

class TestLogStateMgr: public ::testing::Test
{
public:
  TestLogStateMgr();
  virtual ~TestLogStateMgr();
public:
  virtual void SetUp();
  virtual void TearDown();
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
  log_replica_property_meta_.generate(true, LogReplicaType::NORMAL_REPLICA);
}

void TestLogStateMgr::TearDown()
{}

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
  return RUN_ALL_TESTS();
}
