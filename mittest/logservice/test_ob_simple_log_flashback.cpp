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

const std::string TEST_NAME = "flashback";
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
class TestObSimpleLogClusterFlashback : public ObSimpleLogClusterTestEnv
{
public:
  TestObSimpleLogClusterFlashback() {}
};

int64_t ObSimpleLogClusterTestBase::member_cnt_ = 1;
int64_t ObSimpleLogClusterTestBase::node_cnt_ = 5;
std::string ObSimpleLogClusterTestBase::test_name_ = TEST_NAME;
bool ObSimpleLogClusterTestBase::need_add_arb_server_  = false;


// test cases:
// 1. basic multiple replica flashback
// 2. some replicas has been flashbacked and reconfirm
TEST_F(TestObSimpleLogClusterFlashback, flashback_basic_func)
{
  SET_CASE_LOG_FILE(TEST_NAME, "flashback_basic_func");
  OB_LOGGER.set_log_level("INFO");
  // 2 log streams
  // log stream 1's end_ts_ns is higher than flashback_ts
  // log stream 2's end_ts_ns is less than flashback_ts

  const int64_t id1 = ATOMIC_AAF(&palf_id_, 1);
  const int64_t id2 = ATOMIC_AAF(&palf_id_, 1);
  const int64_t CONFIG_CHANGE_TIMEOUT_US = 10 * 1000L * 1000L;
  int64_t leader_idx1 = 0, leader_idx2 = 0;
  int64_t mode_version1 = INVALID_PROPOSAL_ID, mode_version2 = INVALID_PROPOSAL_ID;
  ObLogFlashbackService *flashback_srv = NULL;
  // 1. create 2 palfs, each palf with 3 replicas
  unittest::PalfHandleImplGuard leader1, leader2;
  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id1, leader_idx1, leader1));
  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id2, leader_idx2, leader2));
  LogConfigVersion config_version;
  ASSERT_EQ(OB_SUCCESS, leader1.palf_handle_impl_->get_config_version(config_version));
  ASSERT_EQ(OB_SUCCESS, leader1.palf_handle_impl_->add_member(ObMember(get_cluster()[1]->get_addr(), 1), 2, config_version, CONFIG_CHANGE_TIMEOUT_US));
  ASSERT_EQ(OB_SUCCESS, leader1.palf_handle_impl_->get_config_version(config_version));
  EXPECT_EQ(OB_SUCCESS, leader1.palf_handle_impl_->add_member(ObMember(get_cluster()[2]->get_addr(), 1), 3, config_version, CONFIG_CHANGE_TIMEOUT_US));
  EXPECT_EQ(OB_SUCCESS, leader1.palf_handle_impl_->add_learner(ObMember(get_cluster()[3]->get_addr(), 1), CONFIG_CHANGE_TIMEOUT_US));
  EXPECT_EQ(OB_SUCCESS, leader1.palf_handle_impl_->add_learner(ObMember(get_cluster()[4]->get_addr(), 1), CONFIG_CHANGE_TIMEOUT_US));
  ASSERT_EQ(OB_SUCCESS, leader2.palf_handle_impl_->get_config_version(config_version));
  EXPECT_EQ(OB_SUCCESS, leader2.palf_handle_impl_->add_member(ObMember(get_cluster()[1]->get_addr(), 1), 2, config_version, CONFIG_CHANGE_TIMEOUT_US));
  ASSERT_EQ(OB_SUCCESS, leader2.palf_handle_impl_->get_config_version(config_version));
  EXPECT_EQ(OB_SUCCESS, leader2.palf_handle_impl_->add_member(ObMember(get_cluster()[2]->get_addr(), 1), 3, config_version, CONFIG_CHANGE_TIMEOUT_US));
  EXPECT_EQ(OB_SUCCESS, leader2.palf_handle_impl_->add_learner(ObMember(get_cluster()[3]->get_addr(), 1), CONFIG_CHANGE_TIMEOUT_US));
  EXPECT_EQ(OB_SUCCESS, leader2.palf_handle_impl_->add_learner(ObMember(get_cluster()[4]->get_addr(), 1), CONFIG_CHANGE_TIMEOUT_US));
  EXPECT_EQ(OB_SUCCESS, submit_log(leader1, 500, leader_idx1));
  EXPECT_EQ(OB_SUCCESS, submit_log(leader2, 500, leader_idx2));
  SCN flashback_scn;
  flashback_scn.convert_from_ts(common::ObTimeUtility::current_time());
  EXPECT_EQ(OB_SUCCESS, submit_log(leader1, 500, leader_idx1));
  wait_until_has_committed(leader1, leader1.palf_handle_impl_->get_max_lsn());
  wait_until_has_committed(leader2, leader2.palf_handle_impl_->get_max_lsn());
  EXPECT_GT(flashback_scn, leader2.palf_handle_impl_->get_max_scn());
  EXPECT_LT(flashback_scn, leader1.palf_handle_impl_->get_max_scn());

  // 2. change to RAW_WRITE mode, not allow APPEND->FLASHBACK for now
  switch_append_to_raw_write(leader1, mode_version1);
  switch_append_to_raw_write(leader2, mode_version2);

  // 3. do flashback
  flashback_srv = get_cluster()[0]->get_flashback_service();
  const int64_t TIMEOUT_US = 10 * 1000 * 1000;

  // 4. test a follower blocknet
  block_net(leader_idx1, (leader_idx1+1) % 3);
  flashback_srv = get_cluster()[0]->get_flashback_service();
  ASSERT_EQ(OB_OP_NOT_ALLOW, flashback_srv->flashback(MTL_ID(), flashback_scn, 5 * 1000 * 1000));
  unblock_net(leader_idx1, (leader_idx1+1) % 3);

  // test a learner blocknet
  block_net(leader_idx1, 3);
  flashback_srv = get_cluster()[0]->get_flashback_service();
  ASSERT_EQ(OB_OP_NOT_ALLOW, flashback_srv->flashback(MTL_ID(), flashback_scn, 5 * 1000 * 1000));
  unblock_net(leader_idx1, 3);

  // 5. test basic flashback
  EXPECT_EQ(OB_SUCCESS, flashback_srv->flashback(MTL_ID(), flashback_scn, TIMEOUT_US));
  LSN new_log_tail1 = leader1.palf_handle_impl_->log_engine_.log_storage_.log_tail_;
  EXPECT_EQ(new_log_tail1, leader1.palf_handle_impl_->sw_.committed_end_lsn_);
  EXPECT_GE(flashback_scn, leader1.palf_handle_impl_->sw_.last_slide_scn_);
  EXPECT_EQ(OB_ITER_END, read_log(leader1));
  wait_until_has_committed(leader1, leader1.palf_handle_impl_->sw_.get_max_lsn());
  EXPECT_EQ(OB_ITER_END, read_log(leader1));

  LSN new_log_tail2 = leader2.palf_handle_impl_->log_engine_.log_storage_.log_tail_;
  EXPECT_EQ(new_log_tail2, leader2.palf_handle_impl_->sw_.committed_end_lsn_);
  EXPECT_GE(flashback_scn, leader2.palf_handle_impl_->sw_.last_slide_scn_);
  EXPECT_EQ(OB_ITER_END, read_log(leader2));
  wait_until_has_committed(leader2, leader2.palf_handle_impl_->sw_.get_max_lsn());
  EXPECT_EQ(OB_ITER_END, read_log(leader2));

  std::vector<PalfHandleImplGuard*> palf_list1;
  EXPECT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id1, palf_list1));
  EXPECT_GE(flashback_scn, palf_list1[3]->palf_handle_impl_->sw_.last_slide_scn_);
  EXPECT_GE(flashback_scn, palf_list1[4]->palf_handle_impl_->sw_.last_slide_scn_);
  revert_cluster_palf_handle_guard(palf_list1);
  std::vector<PalfHandleImplGuard*> palf_list2;
  EXPECT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id1, palf_list2));
  EXPECT_GE(flashback_scn, palf_list2[3]->palf_handle_impl_->sw_.last_slide_scn_);
  EXPECT_GE(flashback_scn, palf_list2[4]->palf_handle_impl_->sw_.last_slide_scn_);
  revert_cluster_palf_handle_guard(palf_list2);
  // 4. delete paxos group
  leader1.reset();
  leader2.reset();
  delete_paxos_group(id1);
  delete_paxos_group(id2);
}

TEST_F(TestObSimpleLogClusterFlashback, flashback_with_reconfirm1)
{
  SET_CASE_LOG_FILE(TEST_NAME, "flashback_reconfirm");
  OB_LOGGER.set_log_level("TRACE");
  const int64_t id1 = ATOMIC_AAF(&palf_id_, 1);
  const int64_t CONFIG_CHANGE_TIMEOUT_US = 10 * 1000L * 1000L;
  const int64_t TIMEOUT_US = 10 * 1000 * 1000;
  int64_t leader_idx1 = 0;
  int64_t mode_version1 = INVALID_PROPOSAL_ID, mode_version2 = INVALID_PROPOSAL_ID;
  ObLogFlashbackService *flashback_srv = NULL;
  // 1. create 2 palfs, each palf with 3 replicas
  PalfHandleImplGuard leader1;
  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id1, leader_idx1, leader1));
  if (leader_idx1 != 0) {
    EXPECT_EQ(OB_SUCCESS, switch_leader(id1, 0, leader1));
  }
  LogConfigVersion config_version;
  ASSERT_EQ(OB_SUCCESS, leader1.palf_handle_impl_->get_config_version(config_version));
  EXPECT_EQ(OB_SUCCESS, leader1.palf_handle_impl_->add_member(ObMember(get_cluster()[1]->get_addr(), 1), 2, config_version, CONFIG_CHANGE_TIMEOUT_US));
  ASSERT_EQ(OB_SUCCESS, leader1.palf_handle_impl_->get_config_version(config_version));
  EXPECT_EQ(OB_SUCCESS, leader1.palf_handle_impl_->add_member(ObMember(get_cluster()[2]->get_addr(), 1), 3, config_version, CONFIG_CHANGE_TIMEOUT_US));
  EXPECT_EQ(OB_SUCCESS, leader1.palf_handle_impl_->add_learner(ObMember(get_cluster()[3]->get_addr(), 1), CONFIG_CHANGE_TIMEOUT_US));
  EXPECT_EQ(OB_SUCCESS, leader1.palf_handle_impl_->add_learner(ObMember(get_cluster()[4]->get_addr(), 1), CONFIG_CHANGE_TIMEOUT_US));
  EXPECT_EQ(OB_SUCCESS, submit_log(leader1, 1000, leader_idx1));
  wait_until_has_committed(leader1, leader1.palf_handle_impl_->get_max_lsn());
  LogEntryHeader header_origin;
  SCN flashback_scn;
  EXPECT_EQ(OB_SUCCESS, get_middle_scn(602, leader1, flashback_scn, header_origin));

  // 2. change to RAW_WRITE mode, not allow APPEND->FLASHBACK for now
  switch_append_to_raw_write(leader1, mode_version1);

  // 3. change to FLASHBACK mode
  flashback_srv = get_cluster()[0]->get_flashback_service();
  sleep(2);
  share::ObLSStatusInfoArray ls_array;
  const uint64_t tenant_id = MTL_ID();
  ObLogFlashbackService::ChangeModeOpArray mode_op_array;
  EXPECT_EQ(OB_SUCCESS, flashback_srv->get_ls_list_(tenant_id, ls_array));
  EXPECT_EQ(OB_SUCCESS, flashback_srv->wait_all_ls_replicas_log_sync_(tenant_id, flashback_scn, ls_array, TIMEOUT_US));
  EXPECT_EQ(OB_SUCCESS, flashback_srv->get_and_change_access_mode_(tenant_id, flashback_scn, palf::AccessMode::FLASHBACK, ls_array, TIMEOUT_US, mode_op_array));
  // 4. leader do flashback
  AccessMode unused_access_mode;
  EXPECT_EQ(OB_SUCCESS, leader1.palf_handle_impl_->get_access_mode(mode_version1, unused_access_mode));
  EXPECT_EQ(OB_SUCCESS, leader1.palf_handle_impl_->flashback(mode_version1, flashback_scn,  1000 * 1000));
  LogEntryHeader header_new;
  SCN scn_new;
  EXPECT_EQ(OB_SUCCESS, get_middle_scn(602, leader1, scn_new, header_new));
  EXPECT_EQ(OB_ITER_END, get_middle_scn(603, leader1, scn_new, header_new));
  PALF_LOG(INFO, "runlin trace get_middle_scn2");
  EXPECT_EQ(scn_new, flashback_scn);
  EXPECT_EQ(header_origin.data_checksum_, header_new.data_checksum_);
  leader1.reset();

  // 5. restart paxos group, server with smaller ip will be elected to leader
  restart_paxos_groups();

  // 6. get_leader and check values
  std::vector<PalfHandleImplGuard*> palf_list;
  EXPECT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id1, palf_list));
  EXPECT_EQ(OB_SUCCESS, get_leader(id1, leader1, leader_idx1));
  if (leader_idx1 != 0) {
    EXPECT_EQ(OB_SUCCESS, switch_leader(id1, 0, leader1));
  }
  EXPECT_EQ(OB_SUCCESS, leader1.palf_handle_impl_->get_access_mode(mode_version1, unused_access_mode));
  EXPECT_EQ(AccessMode::FLASHBACK, unused_access_mode);
  sleep(2);
  EXPECT_EQ(OB_NOT_MASTER, submit_log(leader1, 1, leader_idx1));
  EXPECT_GT(palf_list[1]->palf_handle_impl_->get_end_lsn(), leader1.palf_handle_impl_->get_end_lsn());
  EXPECT_GE(flashback_scn, leader1.palf_handle_impl_->sw_.last_slide_scn_);

  // 7. do flashback
  EXPECT_EQ(OB_SUCCESS, flashback_srv->flashback(tenant_id, flashback_scn, TIMEOUT_US));
  LSN new_log_tail1 = leader1.palf_handle_impl_->log_engine_.log_storage_.log_tail_;
  // in FLASHBACK mode, committed_end_lsn has been advanced to max_flshed_end_lsn by reconfirm
  EXPECT_EQ(new_log_tail1.val_, leader1.palf_handle_impl_->sw_.committed_end_lsn_.val_);
  EXPECT_GE(flashback_scn, leader1.palf_handle_impl_->sw_.last_slide_scn_);

  // restart cluster
  // we restart cluster after doing flashback operation, to
  // validate if the operation of change_access_mode to APPEND can be done.
  revert_cluster_palf_handle_guard(palf_list);
  palf_list.clear();
  leader1.reset();
  restart_paxos_groups();

  // 8. change to APPEND mode and submit_log
  EXPECT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id1, palf_list));
  EXPECT_EQ(OB_SUCCESS, get_leader(id1, leader1, leader_idx1));
  if (leader_idx1 != 0) {
    EXPECT_EQ(OB_SUCCESS, switch_leader(id1, 0, leader1));
  }
  switch_flashback_to_append(leader1, mode_version1);
  EXPECT_EQ(OB_SUCCESS, submit_log(leader1, 400, leader_idx1));
  wait_until_has_committed(leader1, leader1.palf_handle_impl_->get_max_lsn());

  flashback_scn.reset();
  EXPECT_EQ(OB_SUCCESS, get_middle_scn(702, leader1, flashback_scn, header_origin));

  // 9. change to RAW_WRITE mode, not allow APPEND->FLASHBACK for now
  switch_append_to_raw_write(leader1, mode_version1);

  // 10. change to FLASHBACK mode
  flashback_srv = get_cluster()[0]->get_flashback_service();
  sleep(2);
  ls_array.reset();
  mode_op_array.reset();
  EXPECT_EQ(OB_SUCCESS, flashback_srv->get_ls_list_(tenant_id, ls_array));
  EXPECT_EQ(OB_SUCCESS, flashback_srv->get_and_change_access_mode_(tenant_id, flashback_scn, palf::AccessMode::PREPARE_FLASHBACK, ls_array, TIMEOUT_US, mode_op_array));
  EXPECT_EQ(OB_NOT_MASTER, submit_log(leader1, 1, leader_idx1));
  EXPECT_EQ(OB_SUCCESS, flashback_srv->wait_all_ls_replicas_log_sync_(tenant_id, flashback_scn, ls_array, TIMEOUT_US));
  EXPECT_EQ(OB_SUCCESS, flashback_srv->get_and_change_access_mode_(tenant_id, flashback_scn, palf::AccessMode::FLASHBACK, ls_array, TIMEOUT_US, mode_op_array));

  // 11. two followers do flashback
  do {
    sleep(1);
    palf_list[1]->palf_handle_impl_->get_access_mode(mode_version1, unused_access_mode);
  } while (unused_access_mode != AccessMode::FLASHBACK);
  do {
    sleep(1);
    palf_list[2]->palf_handle_impl_->get_access_mode(mode_version2, unused_access_mode);
  } while (unused_access_mode != AccessMode::FLASHBACK);
  EXPECT_EQ(OB_SUCCESS, palf_list[1]->palf_handle_impl_->flashback(mode_version1, flashback_scn, TIMEOUT_US));
  EXPECT_EQ(OB_SUCCESS, palf_list[2]->palf_handle_impl_->flashback(mode_version2, flashback_scn, TIMEOUT_US));

  // 11. restart paxos group, server with smaller ip will be elected to leader
  revert_cluster_palf_handle_guard(palf_list);
  palf_list.clear();
  leader1.reset();
  restart_paxos_groups();

  // 12. get_leader and check values
  EXPECT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id1, palf_list));
  EXPECT_EQ(OB_SUCCESS, get_leader(id1, leader1, leader_idx1));
  if (leader_idx1 != 0) {
    EXPECT_EQ(OB_SUCCESS, switch_leader(id1, 0, leader1));
  }
  EXPECT_EQ(OB_SUCCESS, leader1.palf_handle_impl_->get_access_mode(mode_version1, unused_access_mode));
  EXPECT_EQ(AccessMode::FLASHBACK, unused_access_mode);
  sleep(2);
  EXPECT_EQ(OB_NOT_MASTER, submit_log(leader1, 1, leader_idx1));
  EXPECT_GT(leader1.palf_handle_impl_->get_end_lsn(), palf_list[1]->palf_handle_impl_->get_end_lsn());
  EXPECT_GT(leader1.palf_handle_impl_->get_end_lsn(), palf_list[2]->palf_handle_impl_->get_end_lsn());

  // 13. do flashback
  CLOG_LOG(INFO, "runlin trace begin last flashback", K(flashback_scn), K(leader1), K(leader_idx1),
      "max_lsn:", leader1.palf_handle_impl_->sw_.last_submit_lsn_,
      "max_scn:", leader1.palf_handle_impl_->sw_.get_max_scn(),
      "last_slide_scn:", leader1.palf_handle_impl_->sw_.last_slide_scn_,
      "committed_end_lsn:", leader1.palf_handle_impl_->sw_.committed_end_lsn_,
      "log_storage_tail:", leader1.palf_handle_impl_->log_engine_.log_storage_.log_tail_);

  EXPECT_EQ(OB_SUCCESS, flashback_srv->flashback(tenant_id, flashback_scn, TIMEOUT_US));

  CLOG_LOG(INFO, "runlin trace after last flashback", K(flashback_scn), K(leader1), K(leader_idx1),
      "max_lsn:", leader1.palf_handle_impl_->sw_.last_submit_lsn_,
      "max_scn:", leader1.palf_handle_impl_->sw_.get_max_scn(),
      "last_slide_scn:", leader1.palf_handle_impl_->sw_.last_slide_scn_,
      "committed_end_lsn:", leader1.palf_handle_impl_->sw_.committed_end_lsn_,
      "log_storage_tail:", leader1.palf_handle_impl_->log_engine_.log_storage_.log_tail_);

  EXPECT_GE(flashback_scn, leader1.palf_handle_impl_->sw_.last_slide_scn_);
  new_log_tail1 = leader1.palf_handle_impl_->log_engine_.log_storage_.log_tail_;
  EXPECT_EQ(new_log_tail1, leader1.palf_handle_impl_->sw_.committed_end_lsn_);
  leader1.reset();
  revert_cluster_palf_handle_guard(palf_list);
  delete_paxos_group(id1);
}

// this case test flashback_scn is in (end_ts_ns, max_ts_ns)
TEST_F(TestObSimpleLogClusterFlashback, flashback_after_restart)
{
  SET_CASE_LOG_FILE(TEST_NAME, "flashback_after_restart");
  OB_LOGGER.set_log_level("INFO");
  const int64_t id1 = ATOMIC_AAF(&palf_id_, 1);
  const int64_t CONFIG_CHANGE_TIMEOUT_US = 10 * 1000L * 1000L;
  const int64_t TIMEOUT_US = 10 * 1000 * 1000;
  int64_t leader_idx1 = 0;
  int64_t mode_version1 = INVALID_PROPOSAL_ID, mode_version2 = INVALID_PROPOSAL_ID;
  ObLogFlashbackService *flashback_srv = NULL;
  SCN flashback_scn = SCN::min_scn();
  // 1. create 2 palfs, each palf with 3 replicas
  PalfHandleImplGuard leader1;
  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id1, leader_idx1, leader1));
  if (leader_idx1 != 0) {
    EXPECT_EQ(OB_SUCCESS, switch_leader(id1, 0, leader1));
  }
  LogConfigVersion config_version;
  const uint64_t tenant_id = MTL_ID();
  ASSERT_EQ(OB_SUCCESS, leader1.palf_handle_impl_->get_config_version(config_version));
  EXPECT_EQ(OB_SUCCESS, leader1.palf_handle_impl_->add_member(ObMember(get_cluster()[1]->get_addr(), 1), 2, config_version, CONFIG_CHANGE_TIMEOUT_US));
  ASSERT_EQ(OB_SUCCESS, leader1.palf_handle_impl_->get_config_version(config_version));
  EXPECT_EQ(OB_SUCCESS, leader1.palf_handle_impl_->add_member(ObMember(get_cluster()[2]->get_addr(), 1), 3, config_version, CONFIG_CHANGE_TIMEOUT_US));
  EXPECT_EQ(OB_SUCCESS, leader1.palf_handle_impl_->add_learner(ObMember(get_cluster()[3]->get_addr(), 1), CONFIG_CHANGE_TIMEOUT_US));
  EXPECT_EQ(OB_SUCCESS, leader1.palf_handle_impl_->add_learner(ObMember(get_cluster()[4]->get_addr(), 1), CONFIG_CHANGE_TIMEOUT_US));
  EXPECT_EQ(OB_SUCCESS, submit_log(leader1, 1000, leader_idx1));
  wait_until_has_committed(leader1, leader1.palf_handle_impl_->get_max_lsn());
  switch_append_to_raw_write(leader1, mode_version1);

  // change to flasback mode
  ObLogFlashbackService::ChangeModeOpArray mode_op_array;
  share::ObLSStatusInfoArray ls_array;
  flashback_srv = get_cluster()[0]->get_flashback_service();
  EXPECT_EQ(OB_SUCCESS, flashback_srv->get_ls_list_(tenant_id, ls_array));
  EXPECT_EQ(OB_SUCCESS, flashback_srv->wait_all_ls_replicas_log_sync_(tenant_id, flashback_scn, ls_array, TIMEOUT_US));
  EXPECT_EQ(OB_SUCCESS, flashback_srv->get_and_change_access_mode_(tenant_id, flashback_scn, palf::AccessMode::FLASHBACK, ls_array, TIMEOUT_US, mode_op_array));

  // after restarting servers in FLASHBACK MODE, committed_end_lsn will smaller than max_lsn
  leader1.reset();
  restart_paxos_groups();

  EXPECT_EQ(OB_SUCCESS, get_leader(id1, leader1, leader_idx1));
  if (leader_idx1 != 0) {
    EXPECT_EQ(OB_SUCCESS, switch_leader(id1, 0, leader1));
  }
  // in FLASHBACK mode, committed_end_lsn has been advanced to max_flshed_end_lsn by reconfirm
  EXPECT_EQ(leader1.palf_handle_impl_->get_max_lsn(), leader1.palf_handle_impl_->get_end_lsn());
  EXPECT_EQ(leader1.palf_handle_impl_->get_max_scn(), leader1.palf_handle_impl_->get_end_scn());

  // choose a flashback_scn, which is in (end_ts_ns, max_ts_ns)
  flashback_scn.convert_for_tx((leader1.palf_handle_impl_->get_max_scn().get_val_for_inner_table_field() + leader1.palf_handle_impl_->get_end_scn().get_val_for_inner_table_field()) / 2);
  EXPECT_EQ(OB_SUCCESS, flashback_srv->flashback(tenant_id, flashback_scn, TIMEOUT_US));
  EXPECT_GE(flashback_scn, leader1.palf_handle_impl_->sw_.last_slide_scn_);
  LSN new_log_tail1 = leader1.palf_handle_impl_->log_engine_.log_storage_.log_tail_;
  EXPECT_EQ(new_log_tail1, leader1.palf_handle_impl_->sw_.committed_end_lsn_);
}

TEST_F(TestObSimpleLogClusterFlashback, flashback_log_cache)
{
  disable_hot_cache_ = true;
  SET_CASE_LOG_FILE(TEST_NAME, "flashback_log_cache");
  OB_LOGGER.set_log_level("TRACE");
  int server_idx = 0;
  int64_t leader_idx = 0;
  int64_t id = ATOMIC_AAF(&palf_id_, 1);

  PalfHandleImplGuard leader;
  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));

  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id, MAX_LOG_BODY_SIZE));
  const LSN max_lsn = leader.get_palf_handle_impl()->get_max_lsn();
  EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(leader, max_lsn));

  SCN flashback_scn;
  flashback_scn.convert_from_ts(common::ObTimeUtility::current_time());
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));
  EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(leader, leader.get_palf_handle_impl()->get_max_lsn()));
  EXPECT_LT(flashback_scn, leader.get_palf_handle_impl()->get_max_scn());

  PALF_LOG(INFO, "start to hit cache");
  EXPECT_EQ(OB_ITER_END, read_log(leader));

  int64_t miss_cnt = leader.get_palf_handle_impl()->log_cache_.cold_cache_.log_cache_stat_.miss_cnt_;
  EXPECT_NE(0, miss_cnt);
  PALF_LOG(INFO, "miss cnt", K(miss_cnt));
  EXPECT_EQ(OB_ITER_END, read_log(leader));
  EXPECT_LE(miss_cnt + 1, leader.get_palf_handle_impl()->log_cache_.cold_cache_.log_cache_stat_.miss_cnt_);

  // switch mode from APPEND to RAW_WRITE
  int64_t mode_version = INVALID_PROPOSAL_ID;
  switch_append_to_raw_write(leader, mode_version);

  // execute flashback
  PALF_LOG(INFO, "start to flashback", K(get_cluster().size()));
  ObLogFlashbackService *flashback_srv = get_cluster()[0]->get_flashback_service();
  const int64_t timeout_us = 10 * 1000 * 1000;
  int64_t flashabck_version = leader.palf_handle_impl_->log_engine_.log_storage_.flashback_version_;
  EXPECT_EQ(OB_SUCCESS, flashback_srv->flashback(MTL_ID(), flashback_scn, timeout_us));

  LSN new_log_tail1 = leader.palf_handle_impl_->log_engine_.log_storage_.log_tail_;
  EXPECT_EQ(new_log_tail1, leader.palf_handle_impl_->sw_.committed_end_lsn_);
  EXPECT_GE(flashback_scn, leader.palf_handle_impl_->sw_.last_slide_scn_);
  EXPECT_EQ(flashabck_version + 1, leader.palf_handle_impl_->log_engine_.log_storage_.flashback_version_);
  PALF_LOG(INFO, "miss cnt", K(leader.get_palf_handle_impl()->log_cache_.cold_cache_.log_cache_stat_.miss_cnt_));
  EXPECT_EQ(OB_ITER_END, read_log(leader));
  int64_t new_miss_cnt = leader.get_palf_handle_impl()->log_cache_.cold_cache_.log_cache_stat_.miss_cnt_;
  if (new_miss_cnt < miss_cnt) {
    EXPECT_NE(0, new_miss_cnt);
  } else {
    EXPECT_LT(miss_cnt + 1, new_miss_cnt);
  }
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
