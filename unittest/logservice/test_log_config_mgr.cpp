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
#include "logservice/palf/log_config_mgr.h"
#include "mock_logservice_container/mock_election.h"
#include "mock_logservice_container/mock_log_state_mgr.h"
#include "mock_logservice_container/mock_log_sliding_window.h"
#include "mock_logservice_container/mock_log_engine.h"
#include "mock_logservice_container/mock_log_mode_mgr.h"
#include "mock_logservice_container/mock_log_reconfirm.h"
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
const int64_t INIT_ELE_EPOCH = 1;
const int64_t INIT_PROPOSAL_ID = 1;

class TestLogConfigMgr : public ::testing::Test
{
public:
  TestLogConfigMgr()
  {
    mock_election_ = OB_NEW(mockelection::MockElection, "TestLog");
    mock_state_mgr_ = OB_NEW(MockLogStateMgr, "TestLog");
    mock_sw_ = OB_NEW(MockLogSlidingWindow, "TestLog");
    mock_log_engine_ = OB_NEW(MockLogEngine, "TestLog");
    mock_mode_mgr_ = OB_NEW(MockLogModeMgr, "TestLog");
    mock_reconfirm_ = OB_NEW(MockLogReconfirm, "TestLog");
  }
  ~TestLogConfigMgr()
  {
    OB_DELETE(MockElection, "TestLog", mock_election_);
    OB_DELETE(MockLogStateMgr, "TestLog", mock_state_mgr_);
    OB_DELETE(MockLogSlidingWindow, "TestLog", mock_sw_);
    OB_DELETE(MockLogEngine, "TestLog", mock_log_engine_);
    OB_DELETE(MockLogModeMgr, "TestLog", mock_mode_mgr_);
    OB_DELETE(MockLogReconfirm, "TestLog", mock_reconfirm_);
  }
  void init_test_log_config_env(const common::ObAddr &self,
                                const LogConfigInfo &config_info,
                                LogConfigMgr &cm,
                                common::ObRole role = LEADER,
                                ObReplicaState state = ACTIVE)
  {
    mock_state_mgr_->role_ = role;
    mock_state_mgr_->state_ = state;
    mock_state_mgr_->leader_ = (LEADER == role)? self: ObAddr();
    mock_election_->role_ = role;
    mock_election_->leader_epoch_ = INIT_ELE_EPOCH;
    mock_sw_->mock_last_submit_lsn_ = LSN(PALF_INITIAL_LSN_VAL);
    mock_sw_->mock_last_submit_pid_ = INIT_PROPOSAL_ID;
    EXPECT_TRUE(config_info.is_valid());
    PALF_LOG(INFO, "init_test_log_config_env", K(role), K(state), K(mock_state_mgr_->leader_), K(mock_state_mgr_->role_));
    mock_log_engine_->reset_register_parent_resp_ret();
    common::GlobalLearnerList learner_list;
    const int64_t init_pid = INIT_PROPOSAL_ID;
    LogConfigMeta config_meta;
    EXPECT_EQ(OB_SUCCESS, config_meta.generate(init_pid, config_info, config_info, 1, LSN(0), 1));
    EXPECT_EQ(OB_SUCCESS, cm.init(1, self, config_meta, mock_log_engine_, mock_sw_, mock_state_mgr_, mock_election_,
        mock_mode_mgr_, mock_reconfirm_));
    LogMemberRegionMap region_map;
    EXPECT_EQ(OB_SUCCESS, region_map.init("localmap", OB_MAX_MEMBER_NUMBER));
    for (int i = 0; i < cm.alive_paxos_memberlist_.get_member_number(); ++i) {
      ObAddr server;
      cm.alive_paxos_memberlist_.get_server_by_index(i, server);
      region_map.insert(server, default_region);
    }
    EXPECT_EQ(OB_SUCCESS, cm.set_paxos_member_region_map(region_map));
  }
public:
  mockelection::MockElection *mock_election_;
  palf::MockLogStateMgr *mock_state_mgr_;
  palf::MockLogSlidingWindow *mock_sw_;
  palf::MockLogEngine *mock_log_engine_;
  palf::MockLogModeMgr *mock_mode_mgr_;
  palf::MockLogReconfirm *mock_reconfirm_;
};

TEST_F(TestLogConfigMgr, test_remove_child_is_not_learner)
{
  LogConfigMgr cm;
  LogLearnerList removed_children;
  LogLearnerList retire_children;
  LogLearnerList target_children;
  EXPECT_EQ(OB_SUCCESS, cm.all_learnerlist_.add_learner(ObMember(addr1, -1)));
  EXPECT_EQ(OB_SUCCESS, cm.all_learnerlist_.add_learner(ObMember(addr4, -1)));
  EXPECT_EQ(OB_SUCCESS, cm.children_.add_learner(LogLearner(addr1, 1)));
  EXPECT_EQ(OB_SUCCESS, cm.children_.add_learner(LogLearner(addr2, 1)));
  EXPECT_EQ(OB_SUCCESS, cm.children_.add_learner(LogLearner(addr3, 1)));
  EXPECT_EQ(OB_SUCCESS, cm.children_.add_learner(LogLearner(addr4, 1)));
  EXPECT_EQ(OB_SUCCESS, target_children.add_learner(LogLearner(addr1, 1)));
  EXPECT_EQ(OB_SUCCESS, target_children.add_learner(LogLearner(addr4, 1)));
  EXPECT_EQ(OB_SUCCESS, retire_children.add_learner(LogLearner(addr2, 1)));
  EXPECT_EQ(OB_SUCCESS, retire_children.add_learner(LogLearner(addr3, 1)));
  EXPECT_EQ(OB_SUCCESS, cm.remove_child_is_not_learner_(removed_children));
  EXPECT_TRUE(cm.children_.learner_addr_equal(target_children));
  EXPECT_TRUE(retire_children.learner_addr_equal(removed_children));
  PALF_LOG(INFO, "children", K(cm.children_));
}

TEST_F(TestLogConfigMgr, test_remove_diff_region_child)
{
  LogConfigMgr cm;
  LogLearnerList diff_region_children;
  LogLearnerList removed_children;
  LogLearnerList target_children;
  ObTenantBase tenant_base(1);
  ObTenantEnv::set_tenant(&tenant_base);
  cm.region_ = region1;
  EXPECT_EQ(OB_SUCCESS, target_children.add_learner(LogLearner(addr1, region1, 1)));
  EXPECT_EQ(OB_SUCCESS, target_children.add_learner(LogLearner(addr2, region1, 1)));
  EXPECT_EQ(OB_SUCCESS, removed_children.add_learner(LogLearner(addr3, 1)));
  EXPECT_EQ(OB_SUCCESS, removed_children.add_learner(LogLearner(addr4, 1)));
  EXPECT_EQ(OB_SUCCESS, cm.children_.add_learner(LogLearner(addr1, region1, 1)));
  EXPECT_EQ(OB_SUCCESS, cm.children_.add_learner(LogLearner(addr2, region1, 1)));
  EXPECT_EQ(OB_SUCCESS, cm.children_.add_learner(LogLearner(addr3, 1)));
  EXPECT_EQ(OB_SUCCESS, cm.children_.add_learner(LogLearner(addr4, 1)));
  EXPECT_EQ(OB_SUCCESS, cm.remove_diff_region_child_(diff_region_children));
  EXPECT_TRUE(target_children.learner_addr_equal(cm.children_));
  EXPECT_TRUE(removed_children.learner_addr_equal(diff_region_children));
}

TEST_F(TestLogConfigMgr, test_remove_duplicate_region_child)
{
  LogConfigMgr cm;
  LogLearnerList dup_region_children;
  LogLearnerList removed_children;
  LogLearnerList target_children;
  ObTenantBase tenant_base(1);
  ObTenantEnv::set_tenant(&tenant_base);
  EXPECT_EQ(OB_SUCCESS, target_children.add_learner(LogLearner(addr1, region1, 1)));
  EXPECT_EQ(OB_SUCCESS, target_children.add_learner(LogLearner(addr3, region2, 1)));
  EXPECT_EQ(OB_SUCCESS, removed_children.add_learner(LogLearner(addr2, region1, 1)));
  EXPECT_EQ(OB_SUCCESS, removed_children.add_learner(LogLearner(addr4, region2, 1)));
  EXPECT_EQ(OB_SUCCESS, cm.children_.add_learner(LogLearner(addr1, region1, 1)));
  EXPECT_EQ(OB_SUCCESS, cm.children_.add_learner(LogLearner(addr2, region1, 1)));
  EXPECT_EQ(OB_SUCCESS, cm.children_.add_learner(LogLearner(addr3, region2, 1)));
  EXPECT_EQ(OB_SUCCESS, cm.children_.add_learner(LogLearner(addr4, region2, 1)));
  EXPECT_EQ(OB_SUCCESS, cm.remove_duplicate_region_child_(dup_region_children));
  EXPECT_TRUE(target_children.learner_addr_equal(cm.children_));
  EXPECT_TRUE(removed_children.learner_addr_equal(dup_region_children));
}

TEST_F(TestLogConfigMgr, test_handle_learner_keepalive)
{
  LogConfigMgr cm;
  cm.is_inited_ = true;
  LogLearner child(addr1, 1);
  EXPECT_EQ(OB_SUCCESS, cm.children_.add_learner(child));
  EXPECT_EQ(OB_SUCCESS, cm.handle_learner_keepalive_resp(child));
  LogLearner child_in_list;
  EXPECT_EQ(OB_SUCCESS, cm.children_.get_learner_by_addr(addr1, child_in_list));
  EXPECT_GT(child_in_list.keepalive_ts_, 0);
}

TEST_F(TestLogConfigMgr, test_set_paxos_member_region_map)
{
  LogConfigMgr cm;
  cm.is_inited_ = true;
  EXPECT_EQ(OB_SUCCESS, cm.paxos_member_region_map_.init("testmap", OB_MAX_MEMBER_NUMBER));
  cm.alive_paxos_memberlist_.add_server(addr1);
  cm.alive_paxos_memberlist_.add_server(addr2);
  cm.alive_paxos_memberlist_.add_server(addr3);
  LogMemberRegionMap region_map;
  EXPECT_EQ(OB_SUCCESS, region_map.init("localmap", OB_MAX_MEMBER_NUMBER));
  region_map.insert(addr1, region1);
  region_map.insert(addr2, region2);
  cm.set_paxos_member_region_map(region_map);
  ObRegion tmp_region;
  EXPECT_EQ(OB_SUCCESS, cm.paxos_member_region_map_.get(addr1, tmp_region));
  EXPECT_EQ(region1, tmp_region);
  EXPECT_EQ(OB_SUCCESS, cm.paxos_member_region_map_.get(addr2, tmp_region));
  EXPECT_EQ(region2, tmp_region);
  EXPECT_EQ(OB_SUCCESS, cm.paxos_member_region_map_.get(addr3, tmp_region));
  EXPECT_EQ(default_region, tmp_region);
}

TEST_F(TestLogConfigMgr, test_apply_config_meta)
{
  ObMemberList init_member_list, one_f_member_list,
      two_f_member_list, four_f_member_list, five_f_member_list;
  init_member_list.add_server(addr1);
  init_member_list.add_server(addr2);
  init_member_list.add_server(addr3);
  one_f_member_list.add_server(addr1);
  two_f_member_list.add_server(addr1);
  two_f_member_list.add_server(addr2);
  four_f_member_list.add_server(addr1);
  four_f_member_list.add_server(addr2);
  four_f_member_list.add_server(addr3);
  four_f_member_list.add_server(addr4);
  five_f_member_list.add_server(addr1);
  five_f_member_list.add_server(addr2);
  five_f_member_list.add_server(addr3);
  five_f_member_list.add_server(addr4);
  five_f_member_list.add_server(addr5);
  LogConfigVersion init_config_version;
  init_config_version.generate(1, 1);
  GlobalLearnerList learner_list;
  LogConfigInfo default_config_info, one_f_one_a_config_info,
      two_f_one_a_config_info, four_f_one_a_config_info, five_f_config_info,
      four_f_config_info, two_f_config_info;
  EXPECT_EQ(OB_SUCCESS, default_config_info.generate(init_member_list, 3, learner_list, init_config_version));

  EXPECT_EQ(OB_SUCCESS, one_f_one_a_config_info.generate(one_f_member_list, 1, learner_list, init_config_version));
  one_f_one_a_config_info.arbitration_member_ = ObMember(addr2, -1);
  EXPECT_EQ(OB_SUCCESS, two_f_one_a_config_info.generate(two_f_member_list, 2, learner_list, init_config_version));
  two_f_one_a_config_info.arbitration_member_ = ObMember(addr3, -1);
  EXPECT_EQ(OB_SUCCESS, two_f_config_info.generate(two_f_member_list, 2, learner_list, init_config_version));
  EXPECT_EQ(OB_SUCCESS, four_f_one_a_config_info.generate(four_f_member_list, 4, learner_list, init_config_version));
  four_f_one_a_config_info.arbitration_member_ = ObMember(addr5, -1);
  EXPECT_EQ(OB_SUCCESS, four_f_config_info.generate(four_f_member_list, 4, learner_list, init_config_version));
  EXPECT_EQ(OB_SUCCESS, five_f_config_info.generate(five_f_member_list, 5, learner_list, init_config_version));

  std::vector<LogConfigInfo> config_info_list;
  std::vector<LogConfigChangeArgs> arg_list;
  std::vector<ObMemberList> expect_member_list;
  std::vector<int64_t> expect_num_list;
  std::vector<LogLearnerList> expext_learner_list;
  std::vector<int> expect_ret_list;
  std::vector<bool> expect_finished_list;
  // 1. INVALID_LOG_CONFIG_CHANGE_TYPE
  config_info_list.push_back(default_config_info);
  arg_list.push_back(LogConfigChangeArgs());
  expect_ret_list.push_back(OB_INVALID_ARGUMENT);
  expect_finished_list.push_back(false);
  expect_member_list.push_back(init_member_list);
  // 2. self is added member
  config_info_list.push_back(default_config_info);
  arg_list.push_back(LogConfigChangeArgs(ObMember(addr1, -1), 3, ADD_MEMBER));
  expect_ret_list.push_back(OB_SUCCESS);
  expect_finished_list.push_back(true);
  expect_member_list.push_back(init_member_list);
  // 3. normal add member
  config_info_list.push_back(default_config_info);
  arg_list.push_back(LogConfigChangeArgs(ObMember(addr4, -1), 4, ADD_MEMBER));
  expect_ret_list.push_back(OB_SUCCESS);
  expect_finished_list.push_back(false);
  expect_member_list.push_back(init_member_list);
  expect_member_list.back().add_server(addr4);
  // 4. already_in
  config_info_list.push_back(default_config_info);
  arg_list.push_back(LogConfigChangeArgs(ObMember(addr3, -1), 3, ADD_MEMBER));
  expect_ret_list.push_back(OB_SUCCESS);
  expect_finished_list.push_back(true);
  expect_member_list.push_back(init_member_list);
  // 5. can not reach majority
  config_info_list.push_back(default_config_info);
  arg_list.push_back(LogConfigChangeArgs(ObMember(addr4, -1), 3, ADD_MEMBER));
  expect_ret_list.push_back(OB_INVALID_ARGUMENT);
  expect_finished_list.push_back(false);
  expect_member_list.push_back(init_member_list);
  // 6. can not reach majority
  config_info_list.push_back(default_config_info);
  arg_list.push_back(LogConfigChangeArgs(ObMember(addr3, -1), 4, REMOVE_MEMBER));
  expect_ret_list.push_back(OB_INVALID_ARGUMENT);
  expect_finished_list.push_back(false);
  expect_member_list.push_back(init_member_list);
  // 7. normal remove member
  config_info_list.push_back(default_config_info);
  arg_list.push_back(LogConfigChangeArgs(ObMember(addr3, -1), 2, REMOVE_MEMBER));
  expect_ret_list.push_back(OB_SUCCESS);
  expect_finished_list.push_back(false);
  expect_member_list.push_back(init_member_list);
  expect_member_list.back().remove_server(addr3);
  // 8. already removed
  config_info_list.push_back(default_config_info);
  arg_list.push_back(LogConfigChangeArgs(ObMember(addr4, -1), 3, REMOVE_MEMBER));
  expect_ret_list.push_back(OB_SUCCESS);
  expect_finished_list.push_back(true);
  expect_member_list.push_back(init_member_list);
  // 9. add and num
  config_info_list.push_back(default_config_info);
  arg_list.push_back(LogConfigChangeArgs(ObMember(addr4, -1), 0, ADD_MEMBER_AND_NUM));
  expect_ret_list.push_back(OB_SUCCESS);
  expect_finished_list.push_back(false);
  expect_member_list.push_back(init_member_list);
  expect_member_list.back().add_server(addr4);
  // 10. remove and num
  config_info_list.push_back(default_config_info);
  arg_list.push_back(LogConfigChangeArgs(ObMember(addr3, -1), 0, REMOVE_MEMBER_AND_NUM));
  expect_ret_list.push_back(OB_SUCCESS);
  expect_finished_list.push_back(false);
  expect_member_list.push_back(init_member_list);
  expect_member_list.back().remove_server(addr3);
  // 11. add learner
  config_info_list.push_back(default_config_info);
  arg_list.push_back(LogConfigChangeArgs(ObMember(addr4, -1), 0, ADD_LEARNER));
  expect_ret_list.push_back(OB_SUCCESS);
  expect_finished_list.push_back(false);
  expect_member_list.push_back(init_member_list);
  // 12. add learenr which already in member_list
  config_info_list.push_back(default_config_info);
  arg_list.push_back(LogConfigChangeArgs(ObMember(addr3, -1), 0, ADD_LEARNER));
  expect_ret_list.push_back(OB_INVALID_ARGUMENT);
  expect_finished_list.push_back(false);
  expect_member_list.push_back(init_member_list);
  // 13. remove learner in member_list
  config_info_list.push_back(default_config_info);
  arg_list.push_back(LogConfigChangeArgs(ObMember(addr3, -1), 0, REMOVE_LEARNER));
  expect_ret_list.push_back(OB_INVALID_ARGUMENT);
  expect_finished_list.push_back(false);
  expect_member_list.push_back(init_member_list);
  // 14. acceptor -> learner
  config_info_list.push_back(default_config_info);
  arg_list.push_back(LogConfigChangeArgs(ObMember(addr3, -1), 0, SWITCH_ACCEPTOR_TO_LEARNER));
  expect_ret_list.push_back(OB_SUCCESS);
  expect_finished_list.push_back(false);
  expect_member_list.push_back(init_member_list);
  expect_member_list.back().remove_server(addr3);
  // 15. 3F, add_arb_member, replica_num 3
  config_info_list.push_back(default_config_info);
  arg_list.push_back(LogConfigChangeArgs(ObMember(addr4, -1), 3, palf::ADD_ARB_MEMBER));
  expect_ret_list.push_back(OB_SUCCESS);
  expect_finished_list.push_back(false);
  expect_member_list.push_back(init_member_list);
  expect_member_list.back().add_member(ObMember(addr4, -1));
  // 16. 3F, add_arb_member, replica_num 4
  config_info_list.push_back(default_config_info);
  arg_list.push_back(LogConfigChangeArgs(ObMember(addr4, -1), 4, palf::ADD_ARB_MEMBER));
  expect_ret_list.push_back(OB_SUCCESS);
  expect_finished_list.push_back(false);
  expect_member_list.push_back(init_member_list);
  expect_member_list.back().add_member(ObMember(addr4, -1));
  // 17. 2F1A, add_arb_member again
  config_info_list.push_back(two_f_one_a_config_info);
  arg_list.push_back(LogConfigChangeArgs(ObMember(addr4, -1), 4, palf::ADD_ARB_MEMBER));
  expect_ret_list.push_back(OB_INVALID_ARGUMENT);
  expect_finished_list.push_back(false);
  expect_member_list.push_back(two_f_member_list);
  expect_member_list.back().add_member(ObMember(addr3, -1));
  // 18. 2F1A, remove arb member
  config_info_list.push_back(two_f_one_a_config_info);
  arg_list.push_back(LogConfigChangeArgs(ObMember(addr3, -1), 0, palf::REMOVE_ARB_MEMBER));
  expect_ret_list.push_back(OB_SUCCESS);
  expect_finished_list.push_back(false);
  expect_member_list.push_back(two_f_member_list);
  // 19. 3F, degrade
  config_info_list.push_back(default_config_info);
  arg_list.push_back(LogConfigChangeArgs(ObMember(addr3, -1), 0, palf::DEGRADE_ACCEPTOR_TO_LEARNER));
  expect_ret_list.push_back(OB_INVALID_ARGUMENT);
  expect_finished_list.push_back(false);
  expect_member_list.push_back(init_member_list);
  // 20. 2F1A, degrade
  config_info_list.push_back(two_f_one_a_config_info);
  arg_list.push_back(LogConfigChangeArgs(ObMember(addr2, -1), 0, palf::DEGRADE_ACCEPTOR_TO_LEARNER));
  expect_ret_list.push_back(OB_SUCCESS);
  expect_finished_list.push_back(false);
  expect_member_list.push_back(init_member_list);
  expect_member_list.back().remove_server(addr2);
  // in degrade scenario, new member_list will not take effect after calling append_config_meta_,
  // so the expect_member_list is init_member_list
  // 21. 3F, upgrade a normal learner
  config_info_list.push_back(default_config_info);
  config_info_list.back().learnerlist_.add_learner(ObMember(addr4, -1));
  arg_list.push_back(LogConfigChangeArgs(ObMember(addr4, -1), 0, palf::UPGRADE_LEARNER_TO_ACCEPTOR));
  expect_ret_list.push_back(OB_INVALID_ARGUMENT);
  expect_finished_list.push_back(false);
  expect_member_list.push_back(init_member_list);
  // 22. 3F, upgrade an acceptor, already finish
  config_info_list.push_back(default_config_info);
  arg_list.push_back(LogConfigChangeArgs(ObMember(addr3, -1), 0, palf::UPGRADE_LEARNER_TO_ACCEPTOR));
  expect_ret_list.push_back(OB_SUCCESS);
  expect_finished_list.push_back(true);
  expect_member_list.push_back(init_member_list);
  // 23. 3F, upgrade a degraded learner
  config_info_list.push_back(default_config_info);
  config_info_list.back().degraded_learnerlist_.add_learner(ObMember(addr4, -1));
  arg_list.push_back(LogConfigChangeArgs(ObMember(addr4, -1), 0, palf::UPGRADE_LEARNER_TO_ACCEPTOR));
  expect_ret_list.push_back(OB_SUCCESS);
  expect_finished_list.push_back(false);
  expect_member_list.push_back(init_member_list);
  expect_member_list.back().add_server(addr4);
  // 24. 2F, add_member(C, 5)
  config_info_list.push_back(two_f_one_a_config_info);
  config_info_list.back().arbitration_member_.reset();
  arg_list.push_back(LogConfigChangeArgs(ObMember(addr3, -1), 5, palf::ADD_MEMBER));
  expect_ret_list.push_back(OB_SUCCESS);
  expect_finished_list.push_back(false);
  expect_member_list.push_back(two_f_member_list);
  expect_member_list.back().add_member(ObMember(addr3, -1));
  // 25. 1F1A, add_member(C, 5)
  config_info_list.push_back(one_f_one_a_config_info);
  arg_list.push_back(LogConfigChangeArgs(ObMember(addr3, -1), 5, palf::ADD_MEMBER));
  expect_ret_list.push_back(OB_INVALID_ARGUMENT);
  expect_finished_list.push_back(false);
  expect_member_list.push_back(one_f_member_list);
  expect_member_list.back().add_member(ObMember(addr2, -1));
  // 26. 2F1A, add_learner
  config_info_list.push_back(two_f_one_a_config_info);
  arg_list.push_back(LogConfigChangeArgs(ObMember(addr4, -1), 0, palf::ADD_LEARNER));
  expect_ret_list.push_back(OB_SUCCESS);
  expect_finished_list.push_back(false);
  expect_member_list.push_back(init_member_list);
  // 27. 2F1A, add_member
  config_info_list.push_back(two_f_one_a_config_info);
  arg_list.push_back(LogConfigChangeArgs(ObMember(addr4, -1), 4, palf::ADD_MEMBER));
  expect_ret_list.push_back(OB_SUCCESS);
  expect_finished_list.push_back(false);
  expect_member_list.push_back(init_member_list);
  expect_member_list.back().add_member(ObMember(addr4, -1));
  // 28. 2F1A, add_member_num
  config_info_list.push_back(two_f_one_a_config_info);
  arg_list.push_back(LogConfigChangeArgs(ObMember(addr4, -1), 0, palf::ADD_MEMBER_AND_NUM));
  expect_ret_list.push_back(OB_SUCCESS);
  expect_finished_list.push_back(false);
  expect_member_list.push_back(init_member_list);
  expect_member_list.back().add_member(ObMember(addr4, -1));
  // 29. 3F, change_replica_num, invalid
  config_info_list.push_back(default_config_info);
  arg_list.push_back(LogConfigChangeArgs(
      default_config_info.log_sync_memberlist_,
      default_config_info.log_sync_replica_num_,
      2, palf::CHANGE_REPLICA_NUM));
  expect_ret_list.push_back(OB_INVALID_ARGUMENT);
  expect_finished_list.push_back(false);
  expect_member_list.push_back(init_member_list);
  // 30. 3F, change_replica_num, 3->5
  config_info_list.push_back(default_config_info);
  arg_list.push_back(LogConfigChangeArgs(
      default_config_info.log_sync_memberlist_,
      default_config_info.log_sync_replica_num_,
      5, palf::CHANGE_REPLICA_NUM));
  expect_ret_list.push_back(OB_SUCCESS);
  expect_finished_list.push_back(false);
  expect_member_list.push_back(init_member_list);
  // 31. 3F replica_num 5, change_replica_num, 5->3
  LogConfigInfo three_f_num5_config_info;
  EXPECT_EQ(OB_SUCCESS, three_f_num5_config_info.generate(init_member_list, 5, learner_list, init_config_version));
  config_info_list.push_back(three_f_num5_config_info);
  arg_list.push_back(LogConfigChangeArgs(
      three_f_num5_config_info.log_sync_memberlist_,
      three_f_num5_config_info.log_sync_replica_num_,
      3, palf::CHANGE_REPLICA_NUM));
  expect_ret_list.push_back(OB_SUCCESS);
  expect_finished_list.push_back(false);
  expect_member_list.push_back(init_member_list);
  // 32. 4F1A, change_replica_num, 5->3
  config_info_list.push_back(four_f_one_a_config_info);
  arg_list.push_back(LogConfigChangeArgs(
      five_f_member_list,
      5, 3, palf::CHANGE_REPLICA_NUM));
  expect_ret_list.push_back(OB_INVALID_ARGUMENT);
  expect_finished_list.push_back(false);
  expect_member_list.push_back(five_f_member_list);
  // 33. 3F1A replica_num 5, change_replica_num, 4->3
  EXPECT_EQ(OB_SUCCESS, three_f_num5_config_info.generate(init_member_list, 4, learner_list, init_config_version));
  three_f_num5_config_info.arbitration_member_ = ObMember(addr4, -1);
  config_info_list.push_back(three_f_num5_config_info);
  arg_list.push_back(LogConfigChangeArgs(
      init_member_list,
      4, 3, palf::CHANGE_REPLICA_NUM));
  expect_ret_list.push_back(OB_SUCCESS);
  expect_finished_list.push_back(false);
  expect_member_list.push_back(init_member_list);
  expect_member_list.back().add_member(ObMember(addr4, -1));
  // 34. 3F1A replica_num 5, change_replica_num, 5->3
  EXPECT_EQ(OB_SUCCESS, three_f_num5_config_info.generate(init_member_list, 4, learner_list, init_config_version));
  three_f_num5_config_info.arbitration_member_ = ObMember(addr4, -1);
  config_info_list.push_back(three_f_num5_config_info);
  arg_list.push_back(LogConfigChangeArgs(
      four_f_member_list,
      5, 3, palf::CHANGE_REPLICA_NUM));
  expect_ret_list.push_back(OB_INVALID_ARGUMENT);
  expect_finished_list.push_back(false);
  expect_member_list.push_back(four_f_member_list);
  // 35. self is added member, but replica num don't match
  config_info_list.push_back(default_config_info);
  arg_list.push_back(LogConfigChangeArgs(ObMember(addr1, -1), 4, ADD_MEMBER));
  expect_ret_list.push_back(OB_INVALID_ARGUMENT);
  expect_finished_list.push_back(false);
  expect_member_list.push_back(init_member_list);
  // 36. add_arb_member based on 2F
  config_info_list.push_back(two_f_config_info);
  arg_list.push_back(LogConfigChangeArgs(ObMember(addr3, -1), 2, palf::ADD_ARB_MEMBER));
  expect_ret_list.push_back(OB_SUCCESS);
  expect_finished_list.push_back(false);
  expect_member_list.push_back(two_f_member_list);
  expect_member_list.back().add_member(ObMember(addr3, -1));
  // 37. add_arb_member based on 4F
  config_info_list.push_back(four_f_config_info);
  arg_list.push_back(LogConfigChangeArgs(ObMember(addr5, -1), 4, palf::ADD_ARB_MEMBER));
  expect_ret_list.push_back(OB_SUCCESS);
  expect_finished_list.push_back(false);
  expect_member_list.push_back(four_f_member_list);
  expect_member_list.back().add_member(ObMember(addr5, -1));
  // 38. 2F1A - abc, degrade b, migrate b to d: add d.
  config_info_list.push_back(one_f_one_a_config_info);
  config_info_list.back().degraded_learnerlist_.add_learner(ObMember(addr3, -1));
  arg_list.push_back(LogConfigChangeArgs(ObMember(addr4, -1), 0, palf::ADD_MEMBER_AND_NUM));
  expect_ret_list.push_back(OB_SUCCESS);
  expect_finished_list.push_back(false);
  expect_member_list.push_back(init_member_list);
  expect_member_list.back().remove_server(addr3);
  expect_member_list.back().add_server(addr4);
  // 39. 2F1A - abc, degrade b, migrate b to d: add d, remove degraded b.
  config_info_list.push_back(two_f_one_a_config_info);
  config_info_list.back().degraded_learnerlist_.add_learner(ObMember(addr4, -1));
  arg_list.push_back(LogConfigChangeArgs(ObMember(addr4, -1), 0, palf::REMOVE_MEMBER_AND_NUM));
  expect_ret_list.push_back(OB_SUCCESS);
  expect_finished_list.push_back(false);
  expect_member_list.push_back(init_member_list);
  // 40. 2F1A - abc, degrade b, RS wants to remove b when permanent offline occurs.
  config_info_list.push_back(one_f_one_a_config_info);
  config_info_list.back().degraded_learnerlist_.add_learner(ObMember(addr3, -1));
  arg_list.push_back(LogConfigChangeArgs(ObMember(addr3, -1), 2, palf::REMOVE_MEMBER));
  expect_ret_list.push_back(OB_SUCCESS);
  expect_finished_list.push_back(false);
  expect_member_list.push_back(init_member_list);
  expect_member_list.back().remove_server(addr3);
  // 41. 2F1A - abc, degrade b, RS wants to switch b to acceptor
  config_info_list.push_back(one_f_one_a_config_info);
  config_info_list.back().degraded_learnerlist_.add_learner(ObMember(addr3, -1));
  arg_list.push_back(LogConfigChangeArgs(ObMember(addr3, -1), 3, palf::SWITCH_LEARNER_TO_ACCEPTOR));
  expect_ret_list.push_back(OB_INVALID_ARGUMENT);
  expect_finished_list.push_back(false);
  expect_member_list.push_back(init_member_list);
  expect_member_list.back().remove_server(addr3);
  // 42. 2F1A - abc, degrade b, RS wants to switch b to acceptor
  config_info_list.push_back(one_f_one_a_config_info);
  config_info_list.back().degraded_learnerlist_.add_learner(ObMember(addr3, -1));
  arg_list.push_back(LogConfigChangeArgs(ObMember(addr3, -1), 2, palf::SWITCH_ACCEPTOR_TO_LEARNER));
  expect_ret_list.push_back(OB_INVALID_ARGUMENT);
  expect_finished_list.push_back(false);
  expect_member_list.push_back(init_member_list);
  expect_member_list.back().remove_server(addr3);
  // 43. leader remove itself
  config_info_list.push_back(default_config_info);
  arg_list.push_back(LogConfigChangeArgs(ObMember(addr1, -1), 3, REMOVE_MEMBER));
  expect_ret_list.push_back(OB_NOT_ALLOW_REMOVING_LEADER);
  expect_finished_list.push_back(false);
  expect_member_list.push_back(init_member_list);
  for (int i = 0; i < arg_list.size(); ++i) {
    PALF_LOG(INFO, "test_check_config_change_args begin case", K(i));
    LogConfigMgr cm;
    LogConfigVersion config_version, expect_config_version;
    init_test_log_config_env(addr1, config_info_list[i], cm);
    init_config_version.generate(cm.log_ms_meta_.proposal_id_, 1);
    expect_config_version = init_config_version;
    expect_config_version.inc_update_version(cm.log_ms_meta_.proposal_id_);
    bool already_finished = false;
    int tmp_ret = OB_SUCCESS;
    LSN prev_lsn;
    prev_lsn.val_ = PALF_INITIAL_LSN_VAL;
    tmp_ret = cm.append_config_meta_(1, arg_list[i], already_finished);
    config_version = cm.log_ms_meta_.curr_.config_version_;
    EXPECT_EQ(tmp_ret, expect_ret_list[i]) << "ret failed case: " << (i+1);
    EXPECT_EQ(already_finished, expect_finished_list[i]) << "finished failed case:" << (i+1);
    // memberlist will not be applied right now when there is arb member, so use alive_paxos_memberlist_
    bool member_equal = (cm.alive_paxos_memberlist_.member_addr_equal(expect_member_list[i]));
    EXPECT_TRUE(member_equal) << "ret failed case: " << (i+1);
    if (tmp_ret == OB_SUCCESS) {
      if (already_finished) {
        EXPECT_EQ(config_version, init_config_version) << i;
      } else {
        EXPECT_EQ(config_version, expect_config_version) << i;
      }
    }
    PALF_LOG(INFO, "test_check_config_change_args end case", K(i));
  }
  {
    // 26. 4F1A, remove(D, 5), remove(D, 5),
    PALF_LOG(INFO, "test_check_config_change_args begin case 26");
    LogConfigMgr cm;
    init_test_log_config_env(addr1, four_f_one_a_config_info, cm);
    bool already_finished = false;
    int tmp_ret = OB_SUCCESS;
    LSN prev_lsn;
    prev_lsn.val_ = PALF_INITIAL_LSN_VAL;
    // remove(D, 5)
    LogConfigChangeArgs remove_d_arg(ObMember(addr4, -1), 5, palf::REMOVE_MEMBER);
    tmp_ret = cm.append_config_meta_(1, remove_d_arg, already_finished);
    EXPECT_EQ(tmp_ret, OB_SUCCESS) << "remove(D, 5)";
    EXPECT_EQ(already_finished, false) << "remove(D, 5)";
    ObMemberList expect_member_list = four_f_member_list;
    expect_member_list.remove_server(addr4);
    expect_member_list.add_server(addr5);
    // memberlist will not be applied right now when there is arb member, so use alive_paxos_memberlist_
    bool member_equal = (cm.alive_paxos_memberlist_.member_addr_equal(expect_member_list));
    EXPECT_TRUE(member_equal);
    // remove(C, 5)
    cm.reset_status();
    LogConfigChangeArgs remove_c_arg(ObMember(addr3, -1), 5, palf::REMOVE_MEMBER);
    tmp_ret = cm.append_config_meta_(1, remove_c_arg, already_finished);
    EXPECT_EQ(tmp_ret, OB_INVALID_ARGUMENT) << "remove(C, 5)";
    PALF_LOG(INFO, "test_check_config_change_args end case 26");
  }
}

TEST_F(TestLogConfigMgr, test_submit_start_working_log)
{
  PALF_LOG(INFO, "test_submit_start_working_log begin case");
  ObMemberList init_member_list;
  init_member_list.add_server(addr1);
  init_member_list.add_server(addr2);
  init_member_list.add_server(addr3);
  GlobalLearnerList learner_list;
  LogConfigVersion init_config_version;
  init_config_version.generate(1, 1);
  {
    LogConfigMgr cm;
    LogConfigInfo config_info;
    LogConfigVersion sw_config_version;
    EXPECT_EQ(OB_SUCCESS, config_info.generate(init_member_list, 3, learner_list, init_config_version));
    init_test_log_config_env(addr1, config_info, cm, FOLLOWER);
    PALF_LOG(INFO, "curr_leader", "leader", cm.state_mgr_->get_leader());
    EXPECT_EQ(OB_NOT_MASTER, cm.confirm_start_working_log(INIT_PROPOSAL_ID, INIT_ELE_EPOCH, sw_config_version));
  }
  {
    LogConfigMgr cm;
    LogConfigInfo config_info;
    LogConfigVersion sw_config_version;
    EXPECT_EQ(OB_SUCCESS, config_info.generate(init_member_list, 3, learner_list, init_config_version));
    init_test_log_config_env(addr1, config_info, cm, LEADER, RECONFIRM);
    EXPECT_EQ(OB_SUCCESS, cm.log_ms_meta_.curr_.learnerlist_.add_learner(ObMember(addr4, -1)));
    LSN prev_lsn;
    prev_lsn.val_ = PALF_INITIAL_LSN_VAL;
    int64_t prev_log_proposal_id = INVALID_PROPOSAL_ID;
    int64_t prev_mode_pid = 1;
    mock_sw_->mock_last_submit_lsn_ = prev_lsn;
    mock_sw_->mock_last_submit_pid_ = prev_log_proposal_id;
    mock_mode_mgr_->mock_last_submit_mode_meta_.proposal_id_ = prev_mode_pid;
    mock_sw_->mock_max_flushed_lsn_ = prev_lsn;
    mock_sw_->mock_max_flushed_log_pid_ = prev_log_proposal_id;
    mock_mode_mgr_->mock_accepted_mode_meta_.proposal_id_ = prev_mode_pid;
    LogConfigVersion config_version, expect_config_version;
    EXPECT_EQ(OB_SUCCESS, expect_config_version.generate(1, 2));
    EXPECT_EQ(OB_EAGAIN, cm.confirm_start_working_log(INIT_PROPOSAL_ID, INIT_ELE_EPOCH, sw_config_version));
    config_version = cm.log_ms_meta_.curr_.config_version_;
    EXPECT_EQ(config_version, expect_config_version);
    // invoke resend ms log
    EXPECT_EQ(OB_EAGAIN, cm.confirm_start_working_log(INIT_PROPOSAL_ID, INIT_ELE_EPOCH, sw_config_version));
    EXPECT_GT(cm.last_submit_config_log_time_us_, 0);
    // ack defensive code
    EXPECT_EQ(OB_STATE_NOT_MATCH, cm.ack_config_log(addr2, 2, expect_config_version));
    EXPECT_EQ(OB_STATE_NOT_MATCH, cm.ack_config_log(addr2, 1, LogConfigVersion()));
    // receive ack from learner
    EXPECT_EQ(OB_SUCCESS, cm.ack_config_log(addr4, 1, expect_config_version));
    EXPECT_EQ(0, cm.ms_ack_list_.get_count());
    EXPECT_EQ(2, cm.resend_log_list_.get_member_number());
    EXPECT_EQ(OB_EAGAIN, cm.confirm_start_working_log(INIT_PROPOSAL_ID, INIT_ELE_EPOCH, sw_config_version));
    // receive ack from member
    EXPECT_EQ(OB_SUCCESS, cm.ack_config_log(addr1, 1, expect_config_version));
    EXPECT_EQ(1, cm.ms_ack_list_.get_count());
    EXPECT_EQ(2, cm.resend_log_list_.get_member_number());
    EXPECT_EQ(OB_EAGAIN, cm.confirm_start_working_log(INIT_PROPOSAL_ID, INIT_ELE_EPOCH, sw_config_version));
    EXPECT_EQ(OB_SUCCESS, cm.ack_config_log(addr2, 1, expect_config_version));
    EXPECT_EQ(2, cm.ms_ack_list_.get_count());
    EXPECT_EQ(1, cm.resend_log_list_.get_member_number());
    // check if config log is committed
    EXPECT_EQ(OB_SUCCESS, cm.confirm_start_working_log(INIT_PROPOSAL_ID, INIT_ELE_EPOCH, sw_config_version));
    EXPECT_EQ(0, cm.state_);
    EXPECT_EQ(0, cm.ms_ack_list_.get_count());
    EXPECT_EQ(expect_config_version, cm.resend_config_version_);
    EXPECT_EQ(1, cm.resend_log_list_.get_member_number());
    // resend config log
    cm.last_submit_config_log_time_us_ = 0;
    EXPECT_EQ(OB_SUCCESS, cm.try_resend_config_log_(1));
    // receive ack from last member
    EXPECT_EQ(OB_SUCCESS, cm.ack_config_log(addr3, 1, expect_config_version));
    EXPECT_EQ(0, cm.ms_ack_list_.get_count());
    EXPECT_EQ(0, cm.resend_log_list_.get_member_number());
  }
  PALF_LOG(INFO, "test_submit_start_working_log end case");
}

TEST_F(TestLogConfigMgr, test_submit_config_log)
{
  PALF_LOG(INFO, "test_submit_config_log begin case");
  ObMemberList init_member_list;
  init_member_list.add_server(addr1);
  init_member_list.add_server(addr2);
  init_member_list.add_server(addr3);
  LogConfigInfo config_info;
  GlobalLearnerList learner_list;
  LogConfigVersion init_config_version;
  init_config_version.generate(1, 1);
  EXPECT_EQ(OB_SUCCESS, config_info.generate(init_member_list, 3, learner_list, init_config_version));
  {
    LogConfigMgr cm;
    init_test_log_config_env(addr1, config_info, cm, LEADER);
    int64_t proposal_id = INVALID_PROPOSAL_ID;
    EXPECT_EQ(OB_INVALID_ARGUMENT, cm.submit_config_log_(cm.log_ms_meta_.curr_.log_sync_memberlist_, proposal_id, 1, LSN(0), 1, cm.log_ms_meta_));
    EXPECT_EQ(OB_INVALID_ARGUMENT, cm.submit_config_log_(cm.log_ms_meta_.curr_.log_sync_memberlist_, 1, 1, LSN(), 1, cm.log_ms_meta_));
    EXPECT_EQ(OB_INVALID_ARGUMENT, cm.submit_config_log_(cm.log_ms_meta_.curr_.log_sync_memberlist_, 1, 1, LSN(0), INVALID_PROPOSAL_ID, cm.log_ms_meta_));
    EXPECT_EQ(OB_INVALID_ARGUMENT, cm.submit_config_log_(cm.log_ms_meta_.curr_.log_sync_memberlist_, 1, 1, LSN(0), 1, LogConfigMeta()));
    proposal_id = 1;
    mock_sw_->mock_max_flushed_lsn_.val_ = PALF_INITIAL_LSN_VAL;
    mock_sw_->mock_max_flushed_log_pid_ = 0;
    mock_mode_mgr_->mock_accepted_mode_meta_.proposal_id_ = 1;
    EXPECT_EQ(OB_SUCCESS, cm.submit_config_log_(cm.log_ms_meta_.curr_.log_sync_memberlist_, proposal_id, INVALID_PROPOSAL_ID, LSN(0), 1, cm.log_ms_meta_));
    EXPECT_GT(cm.last_submit_config_log_time_us_, 0);
  }
  {
    // submit config log when no mode has been flushed
    LogConfigMgr cm;
    init_test_log_config_env(addr1, config_info, cm);
    int64_t proposal_id = 1;
    LogConfigVersion config_version;
    LSN prev_lsn;
    prev_lsn.val_ = PALF_INITIAL_LSN_VAL;
    const int64_t prev_pid = 0;
    LogConfigChangeArgs args;
    args.type_ = palf::ADD_MEMBER_AND_NUM;
    args.server_ = common::ObMember(addr4, -1);
    bool is_already_finished = false;
    mock_sw_->mock_last_submit_lsn_ = prev_lsn;
    mock_sw_->mock_last_submit_pid_ = INVALID_PROPOSAL_ID;
    mock_mode_mgr_->mock_last_submit_mode_meta_.proposal_id_ = prev_pid;
    mock_sw_->mock_max_flushed_lsn_.val_ = PALF_INITIAL_LSN_VAL;
    mock_sw_->mock_max_flushed_log_pid_ = 0;
    mock_mode_mgr_->mock_accepted_mode_meta_.proposal_id_ = INVALID_PROPOSAL_ID;
    EXPECT_EQ(OB_EAGAIN, cm.change_config_(args, proposal_id, INIT_ELE_EPOCH, config_version));
    EXPECT_EQ(OB_EAGAIN, cm.submit_config_log_(cm.log_ms_meta_.curr_.log_sync_memberlist_, proposal_id, 1, prev_lsn, 2, cm.log_ms_meta_));
    EXPECT_EQ(OB_EAGAIN, cm.submit_config_log_(cm.log_ms_meta_.curr_.log_sync_memberlist_, proposal_id, 0, prev_lsn, 2, cm.log_ms_meta_));
    EXPECT_EQ(cm.state_, 1);
    EXPECT_EQ(cm.last_submit_config_log_time_us_, OB_INVALID_TIMESTAMP);
    mock_sw_->mock_max_flushed_log_pid_ = 1;
    mock_mode_mgr_->mock_accepted_mode_meta_.proposal_id_ = 2;
    EXPECT_EQ(OB_SUCCESS, cm.submit_config_log_(cm.log_ms_meta_.curr_.log_sync_memberlist_, proposal_id, 1, prev_lsn, 2, cm.log_ms_meta_));
    EXPECT_GT(cm.last_submit_config_log_time_us_, 0);
  }
  {
    // submit config log when no logs
    LogConfigMgr cm;
    init_test_log_config_env(addr1, config_info, cm);
    int64_t proposal_id = 1;
    LogConfigVersion config_version;
    LSN prev_lsn;
    prev_lsn.val_ = PALF_INITIAL_LSN_VAL;
    const int64_t prev_pid = 0;
    LogConfigChangeArgs args;
    args.type_ = palf::ADD_MEMBER_AND_NUM;
    args.server_ = common::ObMember(addr4, -1);
    bool is_already_finished = false;
    mock_sw_->mock_last_submit_lsn_ = prev_lsn;
    mock_sw_->mock_last_submit_pid_ = INVALID_PROPOSAL_ID;
    mock_mode_mgr_->mock_last_submit_mode_meta_.proposal_id_ = prev_pid;
    mock_sw_->mock_max_flushed_lsn_.val_ = PALF_INITIAL_LSN_VAL;
    mock_sw_->mock_max_flushed_log_pid_ = 0;
    mock_mode_mgr_->mock_accepted_mode_meta_.proposal_id_ = 0;
    EXPECT_EQ(OB_EAGAIN, cm.change_config_(args, proposal_id, INIT_ELE_EPOCH, config_version));
    EXPECT_EQ(OB_SUCCESS, cm.submit_config_log_(cm.log_ms_meta_.curr_.log_sync_memberlist_, proposal_id, prev_pid, prev_lsn, prev_pid, cm.log_ms_meta_));
    EXPECT_GT(cm.last_submit_config_log_time_us_, 0);
    EXPECT_EQ(cm.state_, 1);
  }
  {
    // submit config log when logs exist in disk
    LogConfigMgr cm;
    init_test_log_config_env(addr1, config_info, cm);
    int64_t proposal_id = 1;
    mock_state_mgr_->mock_proposal_id_ = proposal_id;
    LogConfigVersion config_version;
    LSN prev_lsn;
    prev_lsn.val_ = 10000;
    const int64_t prev_pid = 1;
    LogConfigChangeArgs args;
    args.type_ = palf::ADD_MEMBER_AND_NUM;
    args.server_ = common::ObMember(addr4, -1);
    bool is_already_finished = false;
    mock_sw_->mock_last_submit_lsn_ = prev_lsn;
    mock_sw_->mock_last_submit_pid_ = prev_pid;
    mock_mode_mgr_->mock_last_submit_mode_meta_.proposal_id_ = prev_pid;
    mock_sw_->mock_max_flushed_lsn_.val_ = 1000;
    mock_sw_->mock_max_flushed_log_pid_ = 1;
    mock_mode_mgr_->mock_accepted_mode_meta_.proposal_id_ = 3;
    EXPECT_EQ(OB_EAGAIN, cm.change_config_(args, proposal_id, INIT_ELE_EPOCH, config_version));
    EXPECT_EQ(OB_EAGAIN, cm.submit_config_log_(cm.log_ms_meta_.curr_.log_sync_memberlist_, proposal_id, prev_pid, prev_lsn, prev_pid, cm.log_ms_meta_));
    EXPECT_EQ(cm.last_submit_config_log_time_us_, OB_INVALID_TIMESTAMP);
    EXPECT_EQ(cm.state_, 1);
    mock_sw_->mock_max_flushed_lsn_.val_ = 20000;
    EXPECT_EQ(OB_SUCCESS, cm.submit_config_log_(cm.log_ms_meta_.curr_.log_sync_memberlist_, proposal_id, prev_pid, prev_lsn, prev_pid, cm.log_ms_meta_));
    EXPECT_GT(cm.last_submit_config_log_time_us_, 0);
    EXPECT_EQ(cm.state_, 1);
  }
  {
    // submit config log when logs exist in disk
    // proposal_id of prev log is smaller than current proposal_id
    LogConfigMgr cm;
    init_test_log_config_env(addr1, config_info, cm);
    int64_t proposal_id = 2;
    mock_state_mgr_->mock_proposal_id_ = proposal_id;
    LogConfigVersion config_version;
    LSN prev_lsn;
    prev_lsn.val_ = 10000;
    const int64_t prev_pid = 1;
    LogConfigChangeArgs args;
    args.type_ = palf::ADD_MEMBER_AND_NUM;
    args.server_ = common::ObMember(addr4, -1);
    bool is_already_finished = false;
    mock_sw_->mock_last_submit_lsn_ = prev_lsn;
    mock_sw_->mock_last_submit_pid_ = prev_pid;
    mock_mode_mgr_->mock_last_submit_mode_meta_.proposal_id_ = prev_pid;
    mock_sw_->mock_max_flushed_lsn_.val_ = 11000;
    mock_sw_->mock_max_flushed_log_pid_ = 2;
    mock_mode_mgr_->mock_accepted_mode_meta_.proposal_id_ = 3;
    EXPECT_EQ(OB_EAGAIN, cm.change_config_(args, proposal_id, INIT_ELE_EPOCH, config_version));
    EXPECT_EQ(OB_SUCCESS, cm.submit_config_log_(cm.log_ms_meta_.curr_.log_sync_memberlist_, proposal_id, prev_pid, prev_lsn, prev_pid, cm.log_ms_meta_));
    EXPECT_GT(cm.last_submit_config_log_time_us_, 0);
    EXPECT_EQ(cm.state_, 1);
  }
  PALF_LOG(INFO, "test_submit_config_log end case");
}


TEST_F(TestLogConfigMgr, test_after_flush_config_log)
{
  PALF_LOG(INFO, "test_after_flush_config_log begin case");
  ObMemberList init_member_list;
  init_member_list.add_server(addr1);
  init_member_list.add_server(addr2);
  init_member_list.add_server(addr3);
  GlobalLearnerList learner_list;
  LogConfigVersion init_config_version;
  init_config_version.generate(1, 1);
  // self is paxos member, retire parent
  // child is not learner, retire child
  LogConfigMgr cm;
  LogConfigInfo config_info;
  EXPECT_EQ(OB_SUCCESS, config_info.generate(init_member_list, 3, learner_list, init_config_version));
  init_test_log_config_env(addr1, config_info, cm);
  cm.parent_ = addr2;
  cm.register_time_us_ = 555;
  EXPECT_EQ(OB_SUCCESS, cm.children_.add_learner(LogLearner(addr3, 1)));
  EXPECT_EQ(OB_SUCCESS, cm.after_flush_config_log(cm.log_ms_meta_.curr_.config_version_));
  EXPECT_EQ(OB_INVALID_TIMESTAMP, cm.register_time_us_);
  EXPECT_EQ(OB_INVALID_TIMESTAMP, cm.parent_keepalive_time_us_);
  EXPECT_EQ(OB_INVALID_TIMESTAMP, cm.last_submit_register_req_time_us_);
  EXPECT_FALSE(cm.parent_.is_valid());
  EXPECT_EQ(0, cm.children_.get_member_number());
  PALF_LOG(INFO, "test_after_flush_config_log end case");
}

TEST_F(TestLogConfigMgr, test_degrade__upgrade_scenario)
{
  PALF_LOG(INFO, "test_degrade_upgrade_scenario begin case");
  ObMemberList init_member_list;
  init_member_list.add_server(addr1);
  init_member_list.add_server(addr2);
  GlobalLearnerList learner_list;
  LogConfigVersion init_config_version;
  init_config_version.generate(1, 1);
  {
    LogConfigMgr cm;
    LogConfigInfo config_info;
    EXPECT_EQ(OB_SUCCESS, config_info.generate(init_member_list, 2, learner_list, init_config_version));
    config_info.arbitration_member_ = ObMember(addr3, 1);
    init_test_log_config_env(addr1, config_info, cm, LEADER, ACTIVE);
    LSN prev_lsn(PALF_INITIAL_LSN_VAL);
    int64_t prev_log_proposal_id = INVALID_PROPOSAL_ID;
    int64_t prev_mode_pid = 1;
    mock_sw_->mock_last_submit_lsn_ = prev_lsn;
    mock_sw_->mock_last_submit_pid_ = prev_log_proposal_id;
    mock_mode_mgr_->mock_last_submit_mode_meta_.proposal_id_ = prev_mode_pid;
    mock_sw_->mock_max_flushed_lsn_ = prev_lsn;
    mock_sw_->mock_max_flushed_log_pid_ = prev_log_proposal_id;
    mock_mode_mgr_->mock_accepted_mode_meta_.proposal_id_ = prev_mode_pid;

    // degrade
    LogConfigChangeArgs args(ObMember(addr2, OB_INVALID_TIMESTAMP), 0, DEGRADE_ACCEPTOR_TO_LEARNER);
    LogConfigVersion de_config_version;
    EXPECT_EQ(OB_EAGAIN, cm.change_config(args, INIT_PROPOSAL_ID, INIT_ELE_EPOCH, de_config_version));
    EXPECT_EQ(1, cm.state_);
    // member_list will not take effect after append_config_meta_
    EXPECT_FALSE(cm.log_ms_meta_.curr_.log_sync_memberlist_.contains(addr2));
    EXPECT_TRUE(cm.config_meta_.curr_.log_sync_memberlist_.contains(addr2));
    // self ack config log
    EXPECT_EQ(OB_SUCCESS, cm.ack_config_log(addr1, cm.log_ms_meta_.proposal_id_, cm.log_ms_meta_.curr_.config_version_));
    // reach majority - 1
    EXPECT_EQ(OB_EAGAIN, cm.change_config(args, INIT_PROPOSAL_ID, INIT_ELE_EPOCH, de_config_version));
    EXPECT_EQ(1, cm.state_);
    EXPECT_FALSE(cm.log_ms_meta_.curr_.log_sync_memberlist_.contains(addr2));
    EXPECT_TRUE(cm.config_meta_.curr_.log_sync_memberlist_.contains(addr2));
    // ack config log
    EXPECT_EQ(OB_SUCCESS, cm.ack_config_log(addr3, cm.log_ms_meta_.proposal_id_, cm.log_ms_meta_.curr_.config_version_));
    // degrade success, switch to INIT state
    EXPECT_EQ(OB_SUCCESS, cm.change_config(args, INIT_PROPOSAL_ID, INIT_ELE_EPOCH, de_config_version));
    EXPECT_EQ(0, cm.state_);
    EXPECT_FALSE(cm.log_ms_meta_.curr_.log_sync_memberlist_.contains(addr2));
    EXPECT_FALSE(cm.config_meta_.curr_.log_sync_memberlist_.contains(addr2));

    // upgrade
    LogConfigChangeArgs up_args(ObMember(addr2, OB_INVALID_TIMESTAMP), 0, UPGRADE_LEARNER_TO_ACCEPTOR);
    LogConfigVersion up_config_version;
    EXPECT_EQ(OB_EAGAIN, cm.change_config(up_args, INIT_PROPOSAL_ID, INIT_ELE_EPOCH, up_config_version));
    EXPECT_EQ(1, cm.state_);
    // member_list will not take effect after append_config_meta_
    EXPECT_TRUE(cm.log_ms_meta_.curr_.log_sync_memberlist_.contains(addr2));
    EXPECT_FALSE(cm.config_meta_.curr_.log_sync_memberlist_.contains(addr2));
    // self ack config log
    EXPECT_EQ(OB_SUCCESS, cm.ack_config_log(addr1, cm.log_ms_meta_.proposal_id_, cm.log_ms_meta_.curr_.config_version_));
    // reach majority - 1
    EXPECT_EQ(OB_EAGAIN, cm.change_config(up_args, INIT_PROPOSAL_ID, INIT_ELE_EPOCH, up_config_version));
    EXPECT_TRUE(cm.log_ms_meta_.curr_.log_sync_memberlist_.contains(addr2));
    EXPECT_FALSE(cm.config_meta_.curr_.log_sync_memberlist_.contains(addr2));
    EXPECT_EQ(1, cm.state_);
    // ack config log
    EXPECT_EQ(OB_SUCCESS, cm.ack_config_log(addr3, cm.log_ms_meta_.proposal_id_, cm.log_ms_meta_.curr_.config_version_));
    // degrade success, switch to INIT state
    EXPECT_EQ(OB_SUCCESS, cm.change_config(args, INIT_PROPOSAL_ID, INIT_ELE_EPOCH, up_config_version));
    EXPECT_EQ(0, cm.state_);
    EXPECT_TRUE(cm.log_ms_meta_.curr_.log_sync_memberlist_.contains(addr2));
    EXPECT_TRUE(cm.config_meta_.curr_.log_sync_memberlist_.contains(addr2));
  }
}

TEST_F(TestLogConfigMgr, test_handle_register_parent_req)
{
  PALF_LOG(INFO, "test_handle_register_parent_req begin case");
  ObMemberList init_member_list;
  init_member_list.add_server(addr1);
  init_member_list.add_server(addr2);
  init_member_list.add_server(addr3);
  GlobalLearnerList learner_list;
  LogConfigVersion init_config_version;
  init_config_version.generate(1, 1);
  {
    // paxos member register, ignore
    LogConfigMgr cm;
    LogConfigInfo config_info;
    EXPECT_EQ(OB_SUCCESS, config_info.generate(init_member_list, 3, learner_list, init_config_version));
    init_test_log_config_env(addr1, config_info, cm);
    EXPECT_EQ(OB_SUCCESS, cm.handle_register_parent_req(LogLearner(addr3, 1), true));
    EXPECT_EQ(0, cm.children_.get_member_number());
    EXPECT_EQ(RegisterReturn::INVALID_REG_RET, mock_log_engine_->reg_ret_);
  }
  {
    // duplicate register req
    LogConfigMgr cm;
    LogConfigInfo config_info;
    EXPECT_EQ(OB_SUCCESS, config_info.generate(init_member_list, 3, learner_list, init_config_version));
    init_test_log_config_env(addr1, config_info, cm);
    LogLearner child(addr4, 1);
    EXPECT_EQ(OB_SUCCESS, cm.children_.add_learner(child));
    cm.all_learnerlist_.add_learner(ObMember(addr4, -1));
    child.register_time_us_ = 1;
    EXPECT_EQ(OB_SUCCESS, cm.handle_register_parent_req(child, true));
    EXPECT_EQ(RegisterReturn::REGISTER_DONE, mock_log_engine_->reg_ret_);
  }
  {
    // register to leader
    LogConfigMgr cm;
    LogConfigInfo config_info;
    EXPECT_EQ(OB_SUCCESS, config_info.generate(init_member_list, 3, learner_list, init_config_version));
    init_test_log_config_env(addr1, config_info, cm);
    LogLearner child1(addr4, 1);
    LogLearner exist_child(addr5, region1, 1);
    EXPECT_EQ(OB_SUCCESS, cm.children_.add_learner(child1));
    EXPECT_EQ(OB_SUCCESS, cm.children_.add_learner(exist_child));
    cm.all_learnerlist_.add_learner(ObMember(child1.get_server(), -1));
    cm.all_learnerlist_.add_learner(ObMember(exist_child.get_server(), -1));
    child1.register_time_us_ = 1;
    child1.region_ = region1;
    cm.paxos_member_region_map_.update(addr2, region1);
    // child1 register
    // duplicate register req, region has changed
    EXPECT_EQ(OB_SUCCESS, cm.handle_register_parent_req(child1, true));
    EXPECT_EQ(RegisterReturn::REGISTER_CONTINUE, mock_log_engine_->reg_ret_);
    EXPECT_EQ(2, mock_log_engine_->candidate_list_.get_member_number());
    EXPECT_TRUE(mock_log_engine_->candidate_list_.contains(addr2));
    EXPECT_TRUE(mock_log_engine_->candidate_list_.contains(addr5));
    mock_log_engine_->reset_register_parent_resp_ret();
    // child2 register, child2' region is in a new region
    LogLearner child2(addr6, region2, 1);
    child2.register_time_us_ = 1;
    cm.all_learnerlist_.add_learner(ObMember(child2.get_server(), -1));
    EXPECT_EQ(OB_SUCCESS, cm.handle_register_parent_req(child2, true));
    EXPECT_EQ(RegisterReturn::REGISTER_DONE, mock_log_engine_->reg_ret_);
    EXPECT_TRUE(cm.children_.contains(child2));
    EXPECT_EQ(cm.self_, mock_log_engine_->parent_itself_.server_);
    EXPECT_EQ(child2.register_time_us_, mock_log_engine_->parent_itself_.register_time_us_);
  }
  {
    // register to follower
    LogConfigMgr cm;
    LogConfigInfo config_info;
    EXPECT_EQ(OB_SUCCESS, config_info.generate(init_member_list, 3, learner_list, init_config_version));
    init_test_log_config_env(addr1, config_info, cm);
    // diff region
    LogLearner child1(addr4, region1, 1);
    child1.register_time_us_ = 1;
    EXPECT_EQ(OB_SUCCESS, cm.handle_register_parent_req(child1, false));
    EXPECT_EQ(REGISTER_DIFF_REGION, mock_log_engine_->reg_ret_);
    mock_log_engine_->reset_register_parent_resp_ret();
    // register success
    LogLearner child2(addr4, 1);
    child2.register_time_us_ = 1;
    EXPECT_EQ(OB_SUCCESS, cm.handle_register_parent_req(child2, false));
    EXPECT_EQ(REGISTER_DONE, mock_log_engine_->reg_ret_);
    EXPECT_TRUE(cm.children_.contains(addr4));
    mock_log_engine_->reset_register_parent_resp_ret();
    // children_ is full, depend on OB_MAX_CHILD_MEMBER_NUMBER_IN_FOLLOWER = 5
    cm.children_.add_learner(LogLearner(addr5, 1));
    cm.children_.add_learner(LogLearner(addr6, 1));
    cm.children_.add_learner(LogLearner(addr7, 1));
    cm.children_.add_learner(LogLearner(addr8, 1));
    LogLearner child9(addr9, 1);
    child9.register_time_us_ = 1;
    EXPECT_EQ(OB_SUCCESS, cm.handle_register_parent_req(child9, false));
    EXPECT_EQ(REGISTER_CONTINUE, mock_log_engine_->reg_ret_);
    PALF_LOG(INFO, "trace", K(cm.children_), K(mock_log_engine_->candidate_list_));
  }
  PALF_LOG(INFO, "test_handle_register_parent_req end case");
}

TEST_F(TestLogConfigMgr, test_handle_register_parent_resp)
{
  PALF_LOG(INFO, "test_handle_register_parent_resp begin case");
  ObMemberList init_member_list;
  init_member_list.add_server(addr1);
  init_member_list.add_server(addr2);
  init_member_list.add_server(addr3);
  GlobalLearnerList learner_list;
  LogConfigVersion init_config_version;
  init_config_version.generate(1, 1);
  {
    // state not match
    LogConfigMgr cm;
    LogConfigInfo config_info;
    EXPECT_EQ(OB_SUCCESS, config_info.generate(init_member_list, 3, learner_list, init_config_version));
    init_test_log_config_env(addr1, config_info, cm, FOLLOWER);
    EXPECT_EQ(OB_STATE_NOT_MATCH, cm.handle_register_parent_resp(LogLearner(addr4, 1), LogCandidateList(), REGISTER_DONE));
    // register done
    cm.register_time_us_ = 1;
    cm.last_submit_register_req_time_us_ = 1;
    LogLearner parent(addr4, 1);
    parent.register_time_us_ = 1;
    EXPECT_EQ(OB_SUCCESS, cm.handle_register_parent_resp(parent, LogCandidateList(), REGISTER_DONE));
    EXPECT_EQ(OB_INVALID_TIMESTAMP, cm.last_submit_register_req_time_us_);
  }
  {
    // register continue
    LogConfigMgr cm;
    LogConfigInfo config_info;
    EXPECT_EQ(OB_SUCCESS, config_info.generate(init_member_list, 3, learner_list, init_config_version));
    init_test_log_config_env(addr1, config_info, cm, FOLLOWER);
    cm.register_time_us_ = 1;
    cm.last_submit_register_req_time_us_ = 1;
    LogLearner parent(addr4, 1);
    parent.register_time_us_ = 1;
    // cnadidate list is empty
    EXPECT_EQ(OB_ERR_UNEXPECTED, cm.handle_register_parent_resp(parent, LogCandidateList(), REGISTER_CONTINUE));
    LogCandidateList candidate_list;
    candidate_list.add_learner(common::ObMember(addr2, -1));
    // register continue
    EXPECT_EQ(OB_SUCCESS, cm.handle_register_parent_resp(parent, candidate_list, REGISTER_CONTINUE));
    EXPECT_GT(cm.last_submit_register_req_time_us_, 1);
    EXPECT_EQ(1, cm.register_time_us_);
  }
  {
    // register not master
    LogConfigMgr cm;
    LogConfigInfo config_info;
    EXPECT_EQ(OB_SUCCESS, config_info.generate(init_member_list, 3, learner_list, init_config_version));
    init_test_log_config_env(addr4, config_info, cm, FOLLOWER);
    mock_state_mgr_->leader_ = addr1;
    cm.register_time_us_ = 1;
    cm.last_submit_register_req_time_us_ = 1;
    LogLearner parent(addr4, 1);
    parent.register_time_us_ = 1;
    LogCandidateList candidate_list;
    candidate_list.add_learner(common::ObMember(addr2, -1));
    // register continue
    EXPECT_EQ(OB_SUCCESS, cm.handle_register_parent_resp(parent, candidate_list, REGISTER_NOT_MASTER));
    EXPECT_EQ(cm.last_submit_register_req_time_us_, 1);
    EXPECT_EQ(cm.register_time_us_, 1);
    EXPECT_EQ(OB_SUCCESS, cm.handle_register_parent_resp(parent, candidate_list, REGISTER_DIFF_REGION));
    EXPECT_GT(cm.last_submit_register_req_time_us_, 1);
    EXPECT_GT(cm.register_time_us_, 1);
  }
  PALF_LOG(INFO, "test_handle_register_parent_resp end case");
}

TEST_F(TestLogConfigMgr, test_region_changed)
{
  PALF_LOG(INFO, "test_region_changed begin case");
  ObMemberList init_member_list;
  init_member_list.add_server(addr1);
  init_member_list.add_server(addr2);
  init_member_list.add_server(addr3);
  GlobalLearnerList learner_list;
  LogConfigVersion init_config_version;
  init_config_version.generate(1, 1);
  LogConfigMgr cm;
  LogConfigInfo config_info;
  EXPECT_EQ(OB_SUCCESS, config_info.generate(init_member_list, 3, learner_list, init_config_version));
  init_test_log_config_env(addr5, config_info, cm, FOLLOWER);
  cm.parent_ = addr4;
  cm.register_time_us_ = 1;
  cm.parent_keepalive_time_us_ = 5;
  mock_state_mgr_->leader_ = addr1;
  EXPECT_EQ(OB_SUCCESS, cm.set_region(region1));
  EXPECT_EQ(region1, cm.region_);
  EXPECT_FALSE(cm.parent_.is_valid());
  EXPECT_GT(cm.last_submit_register_req_time_us_, 1);
  EXPECT_GT(cm.register_time_us_, 1);
  EXPECT_EQ(cm.parent_keepalive_time_us_, OB_INVALID_TIMESTAMP);
  PALF_LOG(INFO, "test_region_changed end case");
}

TEST_F(TestLogConfigMgr, test_check_children_health)
{
  PALF_LOG(INFO, "test_check_children_health begin case");
  ObMemberList init_member_list;
  init_member_list.add_server(addr1);
  init_member_list.add_server(addr2);
  init_member_list.add_server(addr3);
  GlobalLearnerList learner_list;
  LogConfigVersion init_config_version;
  init_config_version.generate(1, 1);
  {
    // active leader
    LogConfigMgr cm;
    LogConfigInfo config_info;
    EXPECT_EQ(OB_SUCCESS, config_info.generate(init_member_list, 3, learner_list, init_config_version));
    init_test_log_config_env(addr1, config_info, cm);
    LogLearner timeout_child(addr4, 1);
    LogLearner dup_region_child(addr5, 1);
    LogLearner normal_child(addr6, region1, 1);
    dup_region_child.keepalive_ts_ = ObTimeUtility::current_time_ns();
    normal_child.keepalive_ts_ = ObTimeUtility::current_time_ns();
    cm.children_.add_learner(timeout_child);
    cm.children_.add_learner(dup_region_child);
    cm.children_.add_learner(normal_child);
    EXPECT_EQ(OB_SUCCESS, cm.check_children_health());
    EXPECT_EQ(1, cm.children_.get_member_number());
    EXPECT_TRUE(cm.children_.contains(normal_child.server_));
  }
  {
    // active follower
    LogConfigMgr cm;
    LogConfigInfo config_info;
    EXPECT_EQ(OB_SUCCESS, config_info.generate(init_member_list, 3, learner_list, init_config_version));
    init_test_log_config_env(addr2, config_info, cm, FOLLOWER);
    LogLearner timeout_child(addr4, 1);
    LogLearner diff_region_child(addr5, region1, 1);
    LogLearner normal_child(addr6, 1);
    diff_region_child.keepalive_ts_ = ObTimeUtility::current_time_ns();
    normal_child.keepalive_ts_ = ObTimeUtility::current_time_ns();
    cm.children_.add_learner(timeout_child);
    cm.children_.add_learner(diff_region_child);
    cm.children_.add_learner(normal_child);
    EXPECT_EQ(OB_SUCCESS, cm.check_children_health());
    LogLearnerList children;
    EXPECT_EQ(OB_SUCCESS, cm.get_children_list(children));
    EXPECT_EQ(1, children.get_member_number());
    EXPECT_TRUE(children.contains(normal_child.server_));
  }
  PALF_LOG(INFO, "test_check_children_health end case");
}

TEST_F(TestLogConfigMgr, test_check_parent_health)
{
  PALF_LOG(INFO, "test_check_parent_health begin case");
  ObMemberList init_member_list;
  init_member_list.add_server(addr1);
  init_member_list.add_server(addr2);
  init_member_list.add_server(addr3);
  GlobalLearnerList learner_list;
  LogConfigVersion init_config_version;
  init_config_version.generate(1, 1);
  {
    // registering timeout
    LogConfigMgr cm;
    LogConfigInfo config_info;
    EXPECT_EQ(OB_SUCCESS, config_info.generate(init_member_list, 3, learner_list, init_config_version));
    init_test_log_config_env(addr2, config_info, cm, FOLLOWER);
    cm.last_submit_register_req_time_us_ = 1;
    mock_state_mgr_->leader_ = addr1;
    EXPECT_EQ(OB_SUCCESS, cm.check_parent_health());
    EXPECT_GT(cm.register_time_us_, 1);
    EXPECT_GT(cm.last_submit_register_req_time_us_, 1);
  }
  {
    // first registration
    LogConfigMgr cm;
    LogConfigInfo config_info;
    EXPECT_EQ(OB_SUCCESS, config_info.generate(init_member_list, 3, learner_list, init_config_version));
    init_test_log_config_env(addr2, config_info, cm, FOLLOWER);
    mock_state_mgr_->leader_ = addr1;
    EXPECT_EQ(OB_SUCCESS, cm.check_parent_health());
    EXPECT_GT(cm.register_time_us_, 1);
    EXPECT_GT(cm.last_submit_register_req_time_us_, 1);
  }
  {
    // parent timeout
    LogConfigMgr cm;
    LogConfigInfo config_info;
    EXPECT_EQ(OB_SUCCESS, config_info.generate(init_member_list, 3, learner_list, init_config_version));
    init_test_log_config_env(addr2, config_info, cm, FOLLOWER);
    mock_state_mgr_->leader_ = addr1;
    cm.parent_ = addr4;
    EXPECT_EQ(OB_SUCCESS, cm.check_parent_health());
    EXPECT_GT(cm.register_time_us_, 1);
    EXPECT_GT(cm.last_submit_register_req_time_us_, 1);
    EXPECT_FALSE(cm.parent_.is_valid());
  }
  PALF_LOG(INFO, "test_check_parent_health end case");
}

TEST_F(TestLogConfigMgr, test_handle_retire_msg)
{
  PALF_LOG(INFO, "test_handle_retire_msg end case");
  ObMemberList init_member_list;
  init_member_list.add_server(addr1);
  init_member_list.add_server(addr2);
  init_member_list.add_server(addr3);
  LogConfigInfo config_info;
  GlobalLearnerList learner_list;
  LogConfigVersion init_config_version;
  init_config_version.generate(1, 1);
  EXPECT_EQ(OB_SUCCESS, config_info.generate(init_member_list, 3, learner_list, init_config_version));
  {
    LogConfigMgr cm;
    init_test_log_config_env(addr4, config_info, cm, FOLLOWER);
    mock_state_mgr_->leader_ = addr1;
    cm.parent_ = addr2;
    cm.register_time_us_ = 100;
    LogLearner parent(addr2, 100);
    EXPECT_EQ(OB_SUCCESS, cm.handle_retire_child(parent));
    EXPECT_GT(cm.last_submit_register_req_time_us_, 0);
    EXPECT_FALSE(cm.parent_.is_valid());
  }
  {
    LogConfigMgr cm;
    init_test_log_config_env(addr2, config_info, cm, FOLLOWER);
    const LogLearner child = LogLearner(addr4, 100);
    cm.children_.add_learner(child);
    EXPECT_EQ(OB_SUCCESS, cm.handle_retire_parent(child));
    EXPECT_EQ(0, cm.children_.get_member_number());
  }
  {
    // different register_time_us
    LogConfigMgr cm;
    init_test_log_config_env(addr2, config_info, cm, FOLLOWER);
    LogLearner child = LogLearner(addr4, 100);
    cm.children_.add_learner(child);
    child.register_time_us_ = 200;
    EXPECT_EQ(OB_SUCCESS, cm.handle_learner_keepalive_resp(child));
    EXPECT_EQ(child.keepalive_ts_, OB_INVALID_TIMESTAMP);
  }
  {
    LogConfigMgr cm;
    init_test_log_config_env(addr2, config_info, cm, FOLLOWER);
    const LogLearner child = LogLearner(addr4, 100);
    cm.children_.add_learner(child);
    EXPECT_EQ(OB_INVALID_TIMESTAMP, child.keepalive_ts_);
    EXPECT_EQ(OB_SUCCESS, cm.handle_learner_keepalive_resp(child));
    EXPECT_GT(cm.children_.get_learner(0).keepalive_ts_, 0);
  }
  PALF_LOG(INFO, "test_handle_retire_msg end case");
}


} // end namespace unittest
} // end namespace oceanbase

int main(int argc, char **argv)
{
  const std::string rm_base_dir_cmd = "rm -f test_log_config_mgr.log";
  system(rm_base_dir_cmd.c_str());
  OB_LOGGER.set_file_name("test_log_config_mgr.log", true);
  OB_LOGGER.set_log_level("INFO");
  PALF_LOG(INFO, "begin unittest::test_log_config_mgr");
  ::testing::InitGoogleTest(&argc, argv);
  ObClusterVersion::get_instance().update_data_version(DATA_CURRENT_VERSION);
  return RUN_ALL_TESTS();
}
