// Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#include "lib/file/file_directory_utils.h"
#include "lib/utility/ob_macro_utils.h"
#include "logservice/palf/log_define.h"
#include <cstdio>
#include <gtest/gtest.h>
#include <signal.h>
#include "lib/utility/ob_defer.h"
#define private public
#include "env/ob_simple_log_cluster_env.h"

#undef private

const std::string TEST_NAME = "flashback_arb";
using namespace oceanbase::common;
using namespace oceanbase;
namespace oceanbase
{
using namespace logservice;

namespace logservice
{
int LogRequestHandler::change_access_mode_(const LogChangeAccessModeCmd &req)
{
  int ret = common::OB_SUCCESS;
  if (false == req.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "Invalid argument!!!", K(ret), K(req));
  } else {
    palf::PalfHandleGuard palf_handle_guard;
    const int64_t palf_id = req.ls_id_;
    const common::ObAddr &server = req.src_;
    int64_t proposal_id = palf::INVALID_PROPOSAL_ID;
    common::ObRole role = ObRole::FOLLOWER;
    if (OB_FAIL(get_palf_handle_guard_(palf_id, palf_handle_guard))) {
      CLOG_LOG(WARN, "get_palf_handle_guard_ failed", K(ret), K(palf_id));
    } else if (OB_FAIL(palf_handle_guard.get_role(role, proposal_id))) {
    } else if (OB_FAIL(palf_handle_guard.change_access_mode(proposal_id, req.mode_version_,
        req.access_mode_, req.ref_scn_))) {
      CLOG_LOG(WARN, "change_access_mode failed", K(ret), K(palf_id), K(server));
    } else {
      CLOG_LOG(INFO, "change_access_mode success", K(ret), K(req));
    }
  }
  return ret;
}

int ObLogFlashbackService::get_ls_list_(const uint64_t tenant_id,
                                        share::ObLSStatusInfoArray &ls_array)
{
  int ret = OB_SUCCESS;
  common::ObFunction<int(const palf::PalfHandle&)> get_palf_info =
  [&](const palf::PalfHandle &palf_handle)
  {
    int ret = OB_SUCCESS;
    share::ObLSStatusInfo ls_status;
    int64_t palf_id = -1;
    palf_handle.get_palf_id(palf_id);
    share::ObLSFlag flag(share::ObLSFlag::NORMAL_FLAG);
    if (OB_FAIL(ls_status.init(tenant_id, share::ObLSID(palf_id), 1, share::ObLSStatus::OB_LS_NORMAL, 1, "z1", flag))) {
      CLOG_LOG(WARN, "ls_status init failed", K(ret), K(palf_id));
    } else if (OB_FAIL(ls_array.push_back(ls_status))) {
      CLOG_LOG(WARN, "ls_array push_back failed", K(ret), K(palf_id));
    }
    return ret;
  };
  logservice::ObLogService *log_service = NULL;
  log_service = MTL(logservice::ObLogService*);
  if (false == get_palf_info.is_valid()) {
    CLOG_LOG(ERROR, "invalid ObFunction", K(ret));
  } else if (OB_FAIL(log_service->iterate_palf(get_palf_info))) {
    CLOG_LOG(ERROR, "iterate_palf failed", K(ret));
  }
  return ret;
}

int ObLogFlashbackService::BaseLSOperator::update_leader_()
{
  int ret = OB_SUCCESS;
  leader_.reset();
  logservice::ObLogService *log_service = NULL;
  log_service = MTL(logservice::ObLogService*);
  palf::PalfHandleGuard palf_handle;
  if (OB_FAIL(log_service->open_palf(ls_id_, palf_handle))) {
    CLOG_LOG(ERROR, "open_palf failed", K(ret), K_(ls_id));
  } else {
    palf::PalfHandleImpl *palf_handle_impl = dynamic_cast<palf::PalfHandleImpl*>(palf_handle.palf_handle_.palf_handle_impl_);
    leader_ = palf_handle_impl->state_mgr_.get_leader();
  }
  return ret;
}

int LogRequestHandler::get_self_addr_(common::ObAddr &self) const
{
  int ret = OB_SUCCESS;
  logservice::ObLogService *log_service = NULL;
  log_service = MTL(logservice::ObLogService*);
  self = log_service->self_;
  return ret;
}

}

namespace unittest
{
class TestObSimpleLogClusterFlashbackArb : public ObSimpleLogClusterTestEnv
{
public:
  TestObSimpleLogClusterFlashbackArb() {}
};

int64_t ObSimpleLogClusterTestBase::member_cnt_ = 3;
int64_t ObSimpleLogClusterTestBase::node_cnt_ = 3;
std::string ObSimpleLogClusterTestBase::test_name_ = TEST_NAME;
bool ObSimpleLogClusterTestBase::need_add_arb_server_ = true;

// 2F1A
// 1. A reconfiguration (upgrade B) has been executed successfully with log_barrier 100
// 2. the palf group is flashed back to 50, but reconfig_barrier in LogConfigMgr is still 100
// 3. change to APPEND mode
// 4. block_pcode PUSH_LOG_RESP, so the leader can not commit logs by itself
// 5. append logs
// 6. logs in (50, 100) must not be committed by prev_member_list(A)
TEST_F(TestObSimpleLogClusterFlashbackArb, test_flashback_after_upgrading)
{
  int ret = OB_SUCCESS;
  const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  const int64_t CONFIG_CHANGE_TIMEOUT = 10 * 1000 * 1000L;
  OB_LOGGER.set_log_level("TRACE");
  SET_CASE_LOG_FILE(TEST_NAME, "test_flashback_after_upgrading");
  PALF_LOG(INFO, "begin test test_flashback_after_upgrading", K(id));
  {
    int64_t leader_idx = 0;
    int64_t arb_replica_idx = 0;
    PalfHandleImplGuard leader;
    EXPECT_EQ(OB_SUCCESS, create_paxos_group_with_arb_mock_election(id, arb_replica_idx, leader_idx, leader));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 200, id));

    // dynamic_cast<ObSimpleLogServer*>(get_cluster()[leader_idx])->log_service_.get_arbitration_service()->stop();
    const int64_t b_idx = (leader_idx + 1) % 3;
    const int64_t c_idx = (leader_idx + 2) % 3;
    const common::ObAddr a_addr = get_cluster()[leader_idx]->get_addr();
    const common::ObAddr b_addr = get_cluster()[b_idx]->get_addr();
    const common::ObAddr c_addr = get_cluster()[c_idx]->get_addr();
    const SCN flashback_scn = leader.palf_handle_impl_->sw_.get_max_scn();
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 200, id));
    const LSN before_flashback_max_lsn = leader.palf_handle_impl_->sw_.get_max_lsn();

    // 1. block_net and degrade
    block_net(leader_idx, b_idx);
    is_degraded(leader, b_idx);

    // 2. unblock_net and upgrade
    unblock_net(leader_idx, b_idx);
    is_upgraded(leader, id);

    // 3. flashback
    const LSN barrier_end_lsn = leader.palf_handle_impl_->config_mgr_.reconfig_barrier_.prev_end_lsn_;
    const int64_t barrier_mode_pid = leader.palf_handle_impl_->config_mgr_.reconfig_barrier_.prev_mode_pid_;
    int64_t mode_version = 0;
    switch_append_to_raw_write(leader, mode_version);
    ObLogFlashbackService *flashback_srv = get_cluster()[0]->get_flashback_service();
    ObTenantEnv::set_tenant(get_cluster()[leader_idx]->get_tenant_base());
    EXPECT_EQ(OB_SUCCESS, flashback_srv->flashback(MTL_ID(), flashback_scn, CONFIG_CHANGE_TIMEOUT));
    switch_flashback_to_append(leader, mode_version);
    EXPECT_UNTIL_EQ(leader.palf_handle_impl_->sw_.get_max_lsn(), leader.palf_handle_impl_->sw_.committed_end_lsn_);
    const LSN after_flashback_max_lsn = leader.palf_handle_impl_->sw_.get_max_lsn();
    const LSN after_flashback_end_lsn = leader.palf_handle_impl_->sw_.committed_end_lsn_;
    EXPECT_GT(before_flashback_max_lsn, after_flashback_max_lsn);
    EXPECT_GT(barrier_end_lsn, after_flashback_end_lsn);

    // 4. submit logs, the leader must not commit logs by itself
    block_pcode(leader_idx, ObRpcPacketCode::OB_LOG_PUSH_RESP);
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 50, id));
    const LSN curr_max_lsn = leader.palf_handle_impl_->sw_.get_max_lsn();
    sleep(5);
    EXPECT_GT(curr_max_lsn, leader.palf_handle_impl_->sw_.committed_end_lsn_);
    EXPECT_EQ(after_flashback_end_lsn.val_, leader.palf_handle_impl_->sw_.committed_end_lsn_.val_) \
        << after_flashback_end_lsn.val_ << ", " << leader.palf_handle_impl_->sw_.committed_end_lsn_.val_;

    // 5. clear env
    unblock_pcode(leader_idx, ObRpcPacketCode::OB_LOG_PUSH_RESP);
  }
  delete_paxos_group(id);
  PALF_LOG(INFO, "end test test_flashback_after_upgrading", K(id));
}

} // end unittest
} // end oceanbase

// Notes: How to write a new module integrate test case in logservice?
// 1. cp test_ob_simple_log_basic_func.cpp test_ob_simple_log_xxx.cpp
// 2. modify const string TEST_NAME, class name and log file name in test_ob_simple_log_xxx.cpp
// 3. add ob_unittest_clog() item and set label for test_ob_simple_log_xxx in unittest/cluster/CMakeFiles.txt
// 4. write new TEST_F

int main(int argc, char **argv)
{
  RUN_SIMPLE_LOG_CLUSTER_TEST(TEST_NAME);
}
