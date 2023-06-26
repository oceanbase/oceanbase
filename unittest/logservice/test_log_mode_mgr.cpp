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
#include "lib/ob_define.h"
#include "share/rc/ob_tenant_base.h"
#include "share/rc/ob_tenant_module_init_ctx.h"
#define private public
#include "logservice/palf/log_mode_mgr.h"
#include "mock_logservice_container/mock_election.h"
#include "mock_logservice_container/mock_log_state_mgr.h"
#include "mock_logservice_container/mock_log_sliding_window.h"
#include "mock_logservice_container/mock_log_engine.h"
#include "mock_logservice_container/mock_log_config_mgr.h"
#undef private
namespace oceanbase
{
namespace unittest
{
using namespace common;
using namespace share;
using namespace palf;

const ObAddr addr1(ObAddr::IPV4, "127.0.0.1", 1000);
const ObAddr addr2(ObAddr::IPV4, "127.0.0.2", 1000);
const ObAddr addr3(ObAddr::IPV4, "127.0.0.3", 1000);
const ObAddr addr4(ObAddr::IPV4, "127.0.0.4", 1000);
const ObAddr addr5(ObAddr::IPV4, "127.0.0.5", 1000);
const ObAddr addr6(ObAddr::IPV4, "127.0.0.6", 1000);
const ObAddr addr7(ObAddr::IPV4, "127.0.0.7", 1000);
const ObAddr addr8(ObAddr::IPV4, "127.0.0.8", 1000);
const ObAddr addr9(ObAddr::IPV4, "127.0.0.9", 1000);
ObRegion region1("BEIJING");
ObRegion region2("SHANGHAI");
ObRegion default_region(DEFAULT_REGION_NAME);

class TestLogModeMgr : public ::testing::Test
{
public:
  TestLogModeMgr()
  {
    mock_election_ = OB_NEW(mockelection::MockElection, "TestLog");
    mock_state_mgr_ = OB_NEW(MockLogStateMgr, "TestLog");
    mock_sw_ = OB_NEW(MockLogSlidingWindow, "TestLog");
    mock_log_engine_ = OB_NEW(MockLogEngine, "TestLog");
    mock_config_mgr_ = OB_NEW(MockLogConfigMgr, "TestLog");
  }
  ~TestLogModeMgr()
  {
    OB_DELETE(MockElection, "TestLog", mock_election_);
    OB_DELETE(MockLogStateMgr, "TestLog", mock_state_mgr_);
    OB_DELETE(MockLogSlidingWindow, "TestLog", mock_sw_);
    OB_DELETE(MockLogEngine, "TestLog", mock_log_engine_);
    OB_DELETE(MockLogConfigMgr, "TestLog", mock_config_mgr_);
  }
  void init_test_mode_mgr_env(const common::ObAddr &self,
                              const LogModeMeta &mode_meta,
                              LogModeMgr &mode_mgr,
                              common::ObRole role = LEADER,
                              ObReplicaState state = ACTIVE)
  {
    mode_mgr.destroy();
    mock_state_mgr_->role_ = role;
    mock_state_mgr_->state_ = state;
    mock_state_mgr_->leader_ = (LEADER == role)? self: ObAddr();
    const int64_t init_pid = 1;
    LogConfigInfo config_info;
    LogConfigMeta config_meta;
    LogConfigInfoV2 default_config_info;
    ObMemberList init_member_list;
    GlobalLearnerList learner_list;
    LogConfigVersion init_config_version;
    init_config_version.generate(init_pid, 1);
    init_member_list.add_server(addr1);
    init_member_list.add_server(addr2);
    init_member_list.add_server(addr3);
    EXPECT_EQ(OB_SUCCESS, default_config_info.generate(init_member_list, 3, learner_list, init_config_version));
    EXPECT_EQ(OB_SUCCESS, config_meta.generate(init_pid, default_config_info, default_config_info, init_pid, LSN(0), init_pid));
    mock_config_mgr_->destroy();
    mock_config_mgr_->log_ms_meta_ = config_meta;
    EXPECT_TRUE(mode_meta.is_valid());
    PALF_LOG(INFO, "init_test_mode_mgr_env", K(role), K(state), K(mock_state_mgr_->leader_), K(mock_state_mgr_->role_));
    mock_log_engine_->reset_register_parent_resp_ret();
    EXPECT_EQ(OB_SUCCESS, mode_mgr.init(1, self, mode_meta, mock_state_mgr_, mock_log_engine_, mock_config_mgr_, mock_sw_));
  }
public:
  mockelection::MockElection *mock_election_;
  palf::MockLogStateMgr *mock_state_mgr_;
  palf::MockLogSlidingWindow *mock_sw_;
  palf::MockLogEngine *mock_log_engine_;
  palf::MockLogConfigMgr *mock_config_mgr_;
};

TEST_F(TestLogModeMgr, test_init)
{
  PALF_LOG(INFO, "test_init case");
  LogModeMgr mode_mgr;
  LogModeMeta valid_meta, invalid_meta;
  ObAddr invalid_addr;
  EXPECT_EQ(OB_SUCCESS, valid_meta.generate(1, 1, AccessMode::APPEND, share::SCN::base_scn()));
  EXPECT_EQ(OB_INVALID_ARGUMENT, mode_mgr.init(-1, addr1, valid_meta,
      mock_state_mgr_, mock_log_engine_, mock_config_mgr_, mock_sw_));
  EXPECT_EQ(OB_INVALID_ARGUMENT, mode_mgr.init(1, invalid_addr, valid_meta,
      mock_state_mgr_, mock_log_engine_, mock_config_mgr_, mock_sw_));
  EXPECT_EQ(OB_INVALID_ARGUMENT, mode_mgr.init(1, addr1, invalid_meta,
      mock_state_mgr_, mock_log_engine_, mock_config_mgr_, mock_sw_));
  EXPECT_EQ(OB_INVALID_ARGUMENT, mode_mgr.init(1, addr1, valid_meta,
      NULL, mock_log_engine_, mock_config_mgr_, mock_sw_));
  EXPECT_EQ(OB_INVALID_ARGUMENT, mode_mgr.init(1, addr1, valid_meta,
      mock_state_mgr_, NULL, mock_config_mgr_, mock_sw_));
  EXPECT_EQ(OB_INVALID_ARGUMENT, mode_mgr.init(1, addr1, valid_meta,
      mock_state_mgr_, mock_log_engine_, NULL, mock_sw_));
  EXPECT_EQ(OB_INVALID_ARGUMENT, mode_mgr.init(1, addr1, valid_meta,
      mock_state_mgr_, mock_log_engine_, mock_config_mgr_, NULL));
  EXPECT_EQ(OB_SUCCESS, mode_mgr.init(1, addr1, valid_meta,
      mock_state_mgr_, mock_log_engine_, mock_config_mgr_, mock_sw_));
  EXPECT_EQ(OB_INIT_TWICE, mode_mgr.init(1, addr1, valid_meta,
      mock_state_mgr_, mock_log_engine_, mock_config_mgr_, mock_sw_));
  PALF_LOG(INFO, "test_init case");
}

TEST_F(TestLogModeMgr, test_can_interface)
{
  PALF_LOG(INFO, "test_can_interface case");
  std::vector<AccessMode> access_mode_list;
  std::vector<ModeChangeState> state_list;
  std::vector<bool> can_append_list;
  std::vector<bool> can_raw_list;
  
  access_mode_list.push_back(AccessMode::APPEND);
  state_list.push_back(ModeChangeState::MODE_INIT);
  can_append_list.push_back(true);
  can_raw_list.push_back(false);

  access_mode_list.push_back(AccessMode::APPEND);
  state_list.push_back(ModeChangeState::MODE_PREPARE);
  can_append_list.push_back(false);
  can_raw_list.push_back(false);

  access_mode_list.push_back(AccessMode::APPEND);
  state_list.push_back(ModeChangeState::MODE_ACCEPT);
  can_append_list.push_back(true);
  can_raw_list.push_back(false);
  
  access_mode_list.push_back(AccessMode::RAW_WRITE);
  state_list.push_back(ModeChangeState::MODE_INIT);
  can_append_list.push_back(false);
  can_raw_list.push_back(true);

  access_mode_list.push_back(AccessMode::RAW_WRITE);
  state_list.push_back(ModeChangeState::MODE_PREPARE);
  can_append_list.push_back(false);
  can_raw_list.push_back(false);

  access_mode_list.push_back(AccessMode::RAW_WRITE);
  state_list.push_back(ModeChangeState::MODE_ACCEPT);
  can_append_list.push_back(false);
  can_raw_list.push_back(true);
  
  access_mode_list.push_back(AccessMode::FLASHBACK);
  state_list.push_back(ModeChangeState::MODE_INIT);
  can_append_list.push_back(false);
  can_raw_list.push_back(false);

  access_mode_list.push_back(AccessMode::FLASHBACK);
  state_list.push_back(ModeChangeState::MODE_PREPARE);
  can_append_list.push_back(false);
  can_raw_list.push_back(false);

  access_mode_list.push_back(AccessMode::FLASHBACK);
  state_list.push_back(ModeChangeState::MODE_ACCEPT);
  can_append_list.push_back(false);
  can_raw_list.push_back(false);
  for (int i = 0; i < access_mode_list.size(); i++) {
    LogModeMgr mode_mgr;
    mode_mgr.applied_mode_meta_.access_mode_ = access_mode_list[i];
    mode_mgr.state_ = state_list[i];
    EXPECT_EQ(can_append_list[i], mode_mgr.can_append());
    EXPECT_EQ(can_raw_list[i], mode_mgr.can_raw_write());
  }
  PALF_LOG(INFO, "test_can_interface case");
}

TEST_F(TestLogModeMgr, test_receive_mode_meta)
{
  PALF_LOG(INFO, "test_receive_mode_meta case");
  int64_t pid = 1;
  LogModeMeta valid_meta, invalid_meta;
  EXPECT_EQ(OB_SUCCESS, valid_meta.generate(1, 1, AccessMode::APPEND, share::SCN::base_scn()));
  bool has_accepted = false;

  {
    // not init, invalid arg
    LogModeMgr mode_mgr;
    EXPECT_FALSE(mode_mgr.can_receive_mode_meta(pid, valid_meta, has_accepted));

    EXPECT_EQ(OB_SUCCESS, mode_mgr.init(1, addr1, valid_meta,
        mock_state_mgr_, mock_log_engine_, mock_config_mgr_, mock_sw_));

    EXPECT_FALSE(mode_mgr.can_receive_mode_meta(INVALID_PROPOSAL_ID, valid_meta, has_accepted));
    EXPECT_FALSE(mode_mgr.can_receive_mode_meta(pid, invalid_meta, has_accepted));
  }
  {
    // check role
    LogModeMgr mode_mgr;
    init_test_mode_mgr_env(addr1, valid_meta, mode_mgr, LEADER, ACTIVE);
    EXPECT_FALSE(mode_mgr.can_receive_mode_meta(pid, valid_meta, has_accepted));
  }
  {
    // has accepted
    LogModeMgr mode_mgr;
    init_test_mode_mgr_env(addr1, valid_meta, mode_mgr, FOLLOWER, ACTIVE);
    EXPECT_TRUE(mode_mgr.can_receive_mode_meta(pid, valid_meta, has_accepted));
    EXPECT_TRUE(has_accepted);
  }
  {
    // can accpet
    LogModeMgr mode_mgr;
    init_test_mode_mgr_env(addr1, valid_meta, mode_mgr, FOLLOWER, ACTIVE);
    EXPECT_EQ(OB_SUCCESS, valid_meta.generate(2, 2, AccessMode::APPEND, share::SCN::base_scn()));
    EXPECT_TRUE(mode_mgr.can_receive_mode_meta(pid, valid_meta, has_accepted));
    EXPECT_FALSE(has_accepted);
    EXPECT_EQ(OB_SUCCESS, mode_mgr.receive_mode_meta(addr2, 2, false, valid_meta));
    EXPECT_EQ(valid_meta.mode_version_, mode_mgr.last_submit_mode_meta_.mode_version_);
    EXPECT_GT(valid_meta.mode_version_, mode_mgr.accepted_mode_meta_.mode_version_);
    EXPECT_GT(valid_meta.mode_version_, mode_mgr.applied_mode_meta_.mode_version_);
    EXPECT_EQ(OB_SUCCESS, mode_mgr.after_flush_mode_meta(false, valid_meta));
    EXPECT_EQ(valid_meta.mode_version_, mode_mgr.accepted_mode_meta_.mode_version_);
    // receive applied mode_meta
    EXPECT_EQ(OB_SUCCESS, mode_mgr.after_flush_mode_meta(true, valid_meta));
    EXPECT_EQ(valid_meta.mode_version_, mode_mgr.applied_mode_meta_.mode_version_);
  }
}

TEST_F(TestLogModeMgr, test_can_change_access_mode)
{
  PALF_LOG(INFO, "test_can_change_access_mode case");
  const int64_t mode_version = 1;
  const AccessMode access_mode = AccessMode::APPEND;
  const share::SCN ref_scn = share::SCN::base_scn();
  LogModeMeta valid_meta;
  EXPECT_EQ(OB_SUCCESS, valid_meta.generate(1, mode_version, AccessMode::APPEND, share::SCN::base_scn()));
  {
    LogModeMgr mode_mgr;
    // role check
    init_test_mode_mgr_env(addr1, valid_meta, mode_mgr, FOLLOWER, ACTIVE);
    EXPECT_EQ(OB_NOT_MASTER, mode_mgr.can_change_access_mode_(mode_version));
    init_test_mode_mgr_env(addr1, valid_meta, mode_mgr, FOLLOWER, PENDING);
    EXPECT_EQ(OB_NOT_MASTER, mode_mgr.can_change_access_mode_(mode_version));
    init_test_mode_mgr_env(addr1, valid_meta, mode_mgr, LEADER, RECONFIRM);
    EXPECT_EQ(OB_NOT_MASTER, mode_mgr.change_access_mode(mode_version, access_mode, ref_scn));
    // mode_version check
    init_test_mode_mgr_env(addr1, valid_meta, mode_mgr, LEADER, ACTIVE);
    EXPECT_EQ(OB_STATE_NOT_MATCH, mode_mgr.can_change_access_mode_(2));
    // normal
    init_test_mode_mgr_env(addr1, valid_meta, mode_mgr, LEADER, ACTIVE);
    EXPECT_EQ(OB_SUCCESS, mode_mgr.can_change_access_mode_(mode_version));
  }
  {
    // test see newer log
    LogModeMgr mode_mgr;
    init_test_mode_mgr_env(addr1, valid_meta, mode_mgr, LEADER, ACTIVE);
    // switch to prepare state
    EXPECT_EQ(OB_EAGAIN, mode_mgr.change_access_mode(mode_version, AccessMode::RAW_WRITE, share::SCN::base_scn()));
    EXPECT_EQ(MODE_PREPARE, mode_mgr.state_);
    // accept_log_propopsal_id is 2
    EXPECT_EQ(OB_SUCCESS, mode_mgr.handle_prepare_response(addr2, 2, 2, LSN(0), valid_meta));
    EXPECT_EQ(OB_NOT_MASTER, mode_mgr.change_access_mode(mode_version, AccessMode::RAW_WRITE, share::SCN::base_scn()));
    EXPECT_EQ(MODE_PREPARE, mode_mgr.state_);
  }
  PALF_LOG(INFO, "test_can_change_access_mode case");
}

TEST_F(TestLogModeMgr, test_can_switch_access_mode)
{
  PALF_LOG(INFO, "test_can_switch_access_mode case");
  std::vector<AccessMode> src_access_mode_list;
  std::vector<AccessMode> dst_access_mode_list;
  std::vector<bool> ret_list;

  src_access_mode_list.push_back(AccessMode::APPEND);
  dst_access_mode_list.push_back(AccessMode::APPEND);
  ret_list.push_back(false);

  src_access_mode_list.push_back(AccessMode::APPEND);
  dst_access_mode_list.push_back(AccessMode::RAW_WRITE);
  ret_list.push_back(true);

  src_access_mode_list.push_back(AccessMode::APPEND);
  dst_access_mode_list.push_back(AccessMode::FLASHBACK);
  ret_list.push_back(false);

  src_access_mode_list.push_back(AccessMode::RAW_WRITE);
  dst_access_mode_list.push_back(AccessMode::RAW_WRITE);
  ret_list.push_back(false);

  src_access_mode_list.push_back(AccessMode::RAW_WRITE);
  dst_access_mode_list.push_back(AccessMode::APPEND);
  ret_list.push_back(true);

  src_access_mode_list.push_back(AccessMode::RAW_WRITE);
  dst_access_mode_list.push_back(AccessMode::FLASHBACK);
  ret_list.push_back(true);

  src_access_mode_list.push_back(AccessMode::FLASHBACK);
  dst_access_mode_list.push_back(AccessMode::FLASHBACK);
  ret_list.push_back(false);

  src_access_mode_list.push_back(AccessMode::FLASHBACK);
  dst_access_mode_list.push_back(AccessMode::APPEND);
  ret_list.push_back(true);

  src_access_mode_list.push_back(AccessMode::FLASHBACK);
  dst_access_mode_list.push_back(AccessMode::RAW_WRITE);
  ret_list.push_back(false);

  for (int i = 0; i < src_access_mode_list.size(); i++) {
    const bool expected = (ret_list[i] == can_switch_access_mode_(src_access_mode_list[i], dst_access_mode_list[i]));
    EXPECT_TRUE(expected);
  }
  PALF_LOG(INFO, "test_can_switch_access_mode case");
}

TEST_F(TestLogModeMgr, test_change_access_mode)
{
  PALF_LOG(INFO, "test_change_access_mode case");
  const int64_t mode_version = 1;
  const AccessMode access_mode = AccessMode::APPEND;
  const share::SCN ref_scn = share::SCN::base_scn();
  LogModeMeta valid_meta;
  EXPECT_EQ(OB_SUCCESS, valid_meta.generate(1, mode_version, AccessMode::APPEND, share::SCN::base_scn()));
  {
    // not init, invalid args
    LogModeMgr mode_mgr;
    share::SCN invalid_scn;
    EXPECT_EQ(OB_NOT_INIT, mode_mgr.change_access_mode(mode_version, access_mode, ref_scn));
    init_test_mode_mgr_env(addr1, valid_meta, mode_mgr, LEADER, ACTIVE);
    EXPECT_EQ(OB_INVALID_ARGUMENT, mode_mgr.change_access_mode(-1, access_mode, ref_scn));
    EXPECT_EQ(OB_INVALID_ARGUMENT, mode_mgr.change_access_mode(mode_version, AccessMode::INVALID_ACCESS_MODE, ref_scn));
    EXPECT_EQ(OB_INVALID_ARGUMENT, mode_mgr.change_access_mode(mode_version, access_mode, invalid_scn));
  }
  {
    LogModeMgr mode_mgr;
    init_test_mode_mgr_env(addr1, valid_meta, mode_mgr, LEADER, ACTIVE);
    // switch to prepare
    EXPECT_EQ(OB_EAGAIN, mode_mgr.change_access_mode(mode_version, AccessMode::RAW_WRITE, share::SCN::base_scn()));
    EXPECT_EQ(MODE_PREPARE, mode_mgr.state_);
    // handle prepare resp
    EXPECT_EQ(OB_SUCCESS, mode_mgr.handle_prepare_response(addr2, 2, 1, LSN(0), valid_meta));
    // switch to accept
    EXPECT_EQ(OB_EAGAIN, mode_mgr.change_access_mode(mode_version, AccessMode::RAW_WRITE, share::SCN::base_scn()));
    EXPECT_EQ(MODE_ACCEPT, mode_mgr.state_);
    // switch to accept
    EXPECT_EQ(OB_SUCCESS, mode_mgr.ack_mode_meta(addr1, 2));
    EXPECT_EQ(OB_SUCCESS, mode_mgr.ack_mode_meta(addr2, 2));
    // should not reach majority before leader's AccessMode is flushed
    EXPECT_EQ(OB_EAGAIN, mode_mgr.change_access_mode(mode_version, AccessMode::RAW_WRITE, share::SCN::base_scn()));
    // self is flushed
    EXPECT_EQ(OB_SUCCESS, mode_mgr.after_flush_mode_meta(false, mode_mgr.last_submit_mode_meta_));
    EXPECT_EQ(OB_SUCCESS, mode_mgr.change_access_mode(mode_version, AccessMode::RAW_WRITE, share::SCN::base_scn()));
    EXPECT_EQ(MODE_INIT, mode_mgr.state_);
    EXPECT_EQ(2, mode_mgr.applied_mode_meta_.mode_version_);
    EXPECT_EQ(AccessMode::RAW_WRITE, mode_mgr.applied_mode_meta_.access_mode_);
  }
  PALF_LOG(INFO, "test_change_access_mode case");
}

} // end namespace unittest
} // end namespace oceanbase

int main(int argc, char **argv)
{
  const std::string rm_base_dir_cmd = "rm -f test_log_mode_mgr.log";
  system(rm_base_dir_cmd.c_str());
  OB_LOGGER.set_file_name("test_log_mode_mgr.log", true);
  OB_LOGGER.set_log_level("INFO");
  PALF_LOG(INFO, "begin unittest::test_log_config_mgr");
  ::testing::InitGoogleTest(&argc, argv);
  oceanbase::ObClusterVersion::get_instance().update_data_version(DATA_CURRENT_VERSION);
  return RUN_ALL_TESTS();
}
