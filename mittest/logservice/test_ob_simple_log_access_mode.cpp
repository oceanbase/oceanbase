// owner: tengqi.tq
// owner group: log

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

#define private public
#include "env/ob_simple_log_cluster_env.h"
#undef private

const std::string TEST_NAME = "access_mode";

using namespace oceanbase::common;
using namespace oceanbase;
namespace oceanbase
{
using namespace logservice;
using namespace palf;
namespace unittest
{

class TestObSimpleLogClusterAccessMode : public ObSimpleLogClusterTestEnv
{
public:
  TestObSimpleLogClusterAccessMode() :  ObSimpleLogClusterTestEnv()
  {}
  void check_mode_change_tail_recovery(const bool drop_ack, const bool drop_fetch = false,
                                       const bool change_sync = false, const bool drop_mode_meta = false);
};

int64_t ObSimpleLogClusterTestBase::member_cnt_ = 3;
int64_t ObSimpleLogClusterTestBase::node_cnt_ = 4;
std::string ObSimpleLogClusterTestBase::test_name_ = TEST_NAME;
bool ObSimpleLogClusterTestBase::need_add_arb_server_  = false;
bool ObSimpleLogClusterTestBase::need_shared_storage_ = false;

// 1. basic change_access_mode
// 2. change_access_mode(block a follower), then switch this follower to leader
// 3. change_access_mode when leader's net is blocked
// 4. change_access_mode, then add_member
// 5. prev logs have slid, and pid of prev logs is different from current pid

void TestObSimpleLogClusterAccessMode::check_mode_change_tail_recovery(
    const bool drop_ack, const bool drop_fetch, const bool change_sync, const bool drop_mode_meta)
{
  const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  int64_t leader_idx = 0;
  PalfHandleImplGuard leader;
  ASSERT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
  std::vector<PalfHandleImplGuard*> replicas;
  ASSERT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id, replicas));
  const int64_t first_follower = (leader_idx + 1) % 3;
  const int64_t second_follower = (leader_idx + 2) % 3;
  // Recovery must use mode meta, even if every legacy notify-fetch packet is dropped.
  block_pcode(first_follower, obrpc::OB_LOG_NOTIFY_FETCH_LOG);
  block_pcode(second_follower, obrpc::OB_LOG_NOTIFY_FETCH_LOG);
  if (drop_mode_meta) {
    block_pcode(first_follower, obrpc::OB_LOG_CHANGE_MODE_META_REQ);
    block_pcode(second_follower, obrpc::OB_LOG_CHANGE_MODE_META_REQ);
  }
  if (drop_fetch) {
    block_pcode(leader_idx, obrpc::OB_LOG_FETCH_REQ);
  }
  if (drop_ack) {
    block_pcode(leader_idx, obrpc::OB_LOG_PUSH_RESP);
    block_pcode(leader_idx, obrpc::OB_BATCH);
  } else {
    block_pcode(first_follower, obrpc::OB_LOG_PUSH_REQ);
    block_pcode(second_follower, obrpc::OB_LOG_PUSH_REQ);
    block_pcode(first_follower, obrpc::OB_BATCH);
    block_pcode(second_follower, obrpc::OB_BATCH);
  }
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 1, id, 4096));
  const LSN tail_end = leader.palf_handle_impl_->sw_.get_max_lsn();
  LSN first_flushed;
  LSN second_flushed;
  const int64_t flush_deadline = ObTimeUtility::current_time() + 1000 * 1000;
  do {
    replicas[first_follower]->palf_handle_impl_->sw_.get_max_flushed_end_lsn(first_flushed);
    replicas[second_follower]->palf_handle_impl_->sw_.get_max_flushed_end_lsn(second_flushed);
    if (!drop_ack || (first_flushed >= tail_end && second_flushed >= tail_end)) {
      break;
    }
    // Poll actual flush progress; this is not a fixed wait for asynchronous completion.
    ob_usleep(1000);
  } while (ObTimeUtility::current_time() < flush_deadline);
  EXPECT_EQ(drop_ack, first_flushed >= tail_end);
  EXPECT_EQ(drop_ack, second_flushed >= tail_end);

  AccessMode mode;
  int64_t mode_version = 0;
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_access_mode(mode_version, mode));
  int64_t new_proposal_id = INVALID_PROPOSAL_ID;
  auto change_mode = [&]() {
    LogModeMgr &mgr = leader.palf_handle_impl_->mode_mgr_;
    return change_sync ? mgr.change_sync_mode(mode_version, SyncMode::SYNC, new_proposal_id) :
        mgr.change_access_mode(mode_version, AccessMode::RAW_WRITE, share::SCN::min_scn());
  };
  const int64_t mode_start = ObTimeUtility::current_time();
  for (int64_t follower : {first_follower, second_follower}) {
    PalfHandleImpl *impl = replicas[follower]->palf_handle_impl_;
    PalfHandleImpl::WLockGuard guard(impl->lock_);
    impl->state_mgr_.last_check_start_id_ = impl->sw_.get_start_id();
    impl->state_mgr_.last_check_start_id_time_us_ = mode_start;
  }
  int ret = OB_EAGAIN;
  bool accepted = false;
  int64_t first_accept_time = OB_INVALID_TIMESTAMP;
  bool meta_unblocked = !drop_mode_meta;
  const int64_t prepare_deadline = ObTimeUtility::current_time() +
      (drop_mode_meta ? 6 : 1) * 1000 * 1000;
  while (OB_EAGAIN == ret && !accepted && ObTimeUtility::current_time() < prepare_deadline) {
    bool accept_retried = false;
    {
      PalfHandleImpl::WLockGuard guard(leader.palf_handle_impl_->lock_);
      ret = change_mode();
      const LogModeMgr &mgr = leader.palf_handle_impl_->mode_mgr_;
      accepted = MODE_ACCEPT == mgr.state_ && mgr.is_reach_quorum_() &&
          mgr.accepted_mode_meta_.proposal_id_ == leader.palf_handle_impl_->state_mgr_.get_proposal_id();
      if (!meta_unblocked && MODE_ACCEPT == mgr.state_ &&
          mgr.last_submit_mode_meta_.proposal_id_ == leader.palf_handle_impl_->state_mgr_.get_proposal_id()) {
        if (OB_INVALID_TIMESTAMP == first_accept_time) {
          first_accept_time = mgr.last_submit_req_ts_;
        } else if (mgr.last_submit_req_ts_ > first_accept_time) {
          EXPECT_GE(mgr.last_submit_req_ts_ - first_accept_time, 2 * 1000 * 1000);
          PALF_LOG(INFO, "mode-meta original accept retry", K(id),
              "retry_us", mgr.last_submit_req_ts_ - first_accept_time);
          accept_retried = true;
        }
      }
    }
    if (accept_retried) {
      unblock_pcode(first_follower, obrpc::OB_LOG_CHANGE_MODE_META_REQ);
      unblock_pcode(second_follower, obrpc::OB_LOG_CHANGE_MODE_META_REQ);
      meta_unblocked = true;
    }
    if (OB_EAGAIN == ret && !accepted) {
      ob_usleep(1000);
    }
  }
  EXPECT_TRUE(meta_unblocked);
  EXPECT_TRUE(accepted);
  EXPECT_EQ(OB_EAGAIN, ret);
  // The first fetch must be armed by the mode-meta flush, before any successful fetch response.
  for (int64_t follower : {first_follower, second_follower}) {
    PalfHandleImpl *impl = replicas[follower]->palf_handle_impl_;
    bool fetched = false;
    const int64_t deadline = ObTimeUtility::current_time() + 1000 * 1000;
    while (!fetched && ObTimeUtility::current_time() < deadline) {
      {
        PalfHandleImpl::RLockGuard guard(impl->lock_);
        fetched = impl->mode_mgr_.get_accepted_mode_meta().mode_version_ ==
            impl->state_mgr_.get_proposal_id() && impl->sw_.last_fetch_req_time_ >= mode_start;
      }
      if (!fetched) {
        ob_usleep(1000);
      }
    }
    EXPECT_TRUE(fetched);
    PALF_LOG(INFO, "mode-change first LogLoop fetch", K(id), K(change_sync), K(follower),
        "elapsed_us", ObTimeUtility::current_time() - mode_start);
  }
  if (drop_fetch) {
    for (int64_t follower : {first_follower, second_follower}) {
      PalfHandleImpl *impl = replicas[follower]->palf_handle_impl_;
      int64_t first_attempt = 0;
      int64_t first_check_time = 0;
      {
        PalfHandleImpl::WLockGuard guard(impl->lock_);
        first_attempt = impl->sw_.last_fetch_req_time_;
        first_check_time = impl->state_mgr_.last_check_start_id_time_us_;
      }
      int64_t next_attempt = first_attempt;
      int64_t next_check_time = first_check_time;
      const int64_t retry_deadline = ObTimeUtility::current_time() + 1000 * 1000;
      while (next_attempt == first_attempt && ObTimeUtility::current_time() < retry_deadline) {
        {
          PalfHandleImpl::WLockGuard guard(impl->lock_);
          next_attempt = impl->sw_.last_fetch_req_time_;
          next_check_time = impl->state_mgr_.last_check_start_id_time_us_;
        }
        if (next_attempt == first_attempt) {
          ob_usleep(1000);
        }
      }
      EXPECT_GT(next_attempt, first_attempt);
      // The interval starts before submission; variable submission cost can make
      // the spacing between successful RPC submission timestamps slightly shorter.
      EXPECT_GT(next_check_time - first_check_time, 200 * 1000);
    }
  }
  // Keep the ordinary 2 s fallback enabled, with a fresh baseline at packet recovery.
  for (int64_t follower : {first_follower, second_follower}) {
    PalfHandleImpl *impl = replicas[follower]->palf_handle_impl_;
    PalfHandleImpl::WLockGuard guard(impl->lock_);
    impl->state_mgr_.last_check_start_id_ = impl->sw_.get_start_id();
    impl->state_mgr_.last_check_start_id_time_us_ = ObTimeUtility::current_time();
  }
  if (drop_ack) {
    unblock_pcode(leader_idx, obrpc::OB_LOG_PUSH_RESP);
    unblock_pcode(leader_idx, obrpc::OB_BATCH);
  } else {
    unblock_pcode(first_follower, obrpc::OB_LOG_PUSH_REQ);
    unblock_pcode(second_follower, obrpc::OB_LOG_PUSH_REQ);
    unblock_pcode(first_follower, obrpc::OB_BATCH);
    unblock_pcode(second_follower, obrpc::OB_BATCH);
  }
  if (drop_fetch) {
    unblock_pcode(leader_idx, obrpc::OB_LOG_FETCH_REQ);
  }
  const int64_t recovery_start = ObTimeUtility::current_time();
  while (OB_EAGAIN == ret && ObTimeUtility::current_time() - recovery_start < 5 * 1000 * 1000) {
    {
      PalfHandleImpl::WLockGuard guard(leader.palf_handle_impl_->lock_);
      ret = change_mode();
    }
    if (OB_EAGAIN == ret) {
      ob_usleep(1000);
    }
  }
  EXPECT_EQ(OB_SUCCESS, ret);
  PALF_LOG(INFO, "mode-change tail recovery finished", K(id), K(change_sync), K(drop_ack), K(drop_fetch),
      "recovery_us", ObTimeUtility::current_time() - recovery_start);
  // Complete before the ordinary 2 s fallback fetch interval.
  EXPECT_LT(ObTimeUtility::current_time() - recovery_start, 1800 * 1000);
  LSN committed_end;
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->sw_.get_committed_end_lsn(committed_end));
  EXPECT_GE(committed_end, tail_end);
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_access_mode(mode_version, mode));
  EXPECT_EQ(change_sync ? AccessMode::APPEND : AccessMode::RAW_WRITE, mode);
  if (change_sync) {
    SyncMode sync_mode;
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_sync_mode(sync_mode));
    EXPECT_EQ(SyncMode::SYNC, sync_mode);
  }
  unblock_pcode(first_follower, obrpc::OB_LOG_NOTIFY_FETCH_LOG);
  unblock_pcode(second_follower, obrpc::OB_LOG_NOTIFY_FETCH_LOG);
  revert_cluster_palf_handle_guard(replicas);
  leader.reset();
  EXPECT_EQ(OB_SUCCESS, delete_paxos_group(id));
}

TEST_F(TestObSimpleLogClusterAccessMode, mode_change_recovers_missing_tail)
{
  SET_CASE_LOG_FILE(TEST_NAME, "mode_change_recovers_missing_tail");
  check_mode_change_tail_recovery(false);
}

TEST_F(TestObSimpleLogClusterAccessMode, mode_change_recovers_old_tail_ack)
{
  SET_CASE_LOG_FILE(TEST_NAME, "mode_change_recovers_old_tail_ack");
  check_mode_change_tail_recovery(true);
}

TEST_F(TestObSimpleLogClusterAccessMode, mode_change_retries_lost_fetch_request)
{
  SET_CASE_LOG_FILE(TEST_NAME, "mode_change_retries_lost_fetch_request");
  check_mode_change_tail_recovery(false, true);
}

TEST_F(TestObSimpleLogClusterAccessMode, sync_mode_retries_lost_fetch_request)
{
  SET_CASE_LOG_FILE(TEST_NAME, "sync_mode_retries_lost_fetch_request");
  check_mode_change_tail_recovery(false, true, true);
}

TEST_F(TestObSimpleLogClusterAccessMode, sync_mode_recovers_missing_tail)
{
  SET_CASE_LOG_FILE(TEST_NAME, "sync_mode_recovers_missing_tail");
  check_mode_change_tail_recovery(false, false, true);
}

TEST_F(TestObSimpleLogClusterAccessMode, sync_mode_retries_lost_initial_mode_meta)
{
  SET_CASE_LOG_FILE(TEST_NAME, "sync_mode_retries_lost_initial_mode_meta");
  check_mode_change_tail_recovery(false, false, true, true);
}

TEST_F(TestObSimpleLogClusterAccessMode, mode_meta_flush_arms_only_first_pending_mode)
{
  SET_CASE_LOG_FILE(TEST_NAME, "mode_meta_flush_arms_only_first_pending_mode");
  const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  int64_t leader_idx = 0;
  PalfHandleImplGuard leader;
  ASSERT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
  std::vector<PalfHandleImplGuard*> replicas;
  ASSERT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id, replicas));
  PalfHandleImpl *impl = replicas[(leader_idx + 1) % 3]->palf_handle_impl_;
  {
    // Hold the write lock so LogLoop cannot consume the armed check during the assertions.
    PalfHandleImpl::WLockGuard guard(impl->lock_);
    const LogModeMeta old_accepted = impl->mode_mgr_.accepted_mode_meta_;
    const LogModeMeta old_applied = impl->mode_mgr_.applied_mode_meta_;
    const int64_t old_pid = impl->state_mgr_.get_proposal_id();
    const int64_t old_start_id = impl->state_mgr_.last_check_start_id_;
    const int64_t old_check_time = impl->state_mgr_.last_check_start_id_time_us_;
    const FetchTriggerType trigger = impl->sw_.last_fetch_trigger_type_;
    const int64_t fetch_time = impl->sw_.last_fetch_req_time_;
    const LSN fetch_end = impl->sw_.last_fetch_end_lsn_;
    const LSN fetch_committed = impl->sw_.last_fetch_committed_end_lsn_;
    const int64_t fetch_max_id = impl->sw_.last_fetch_max_log_id_;
    const int64_t baseline = ObTimeUtility::current_time();
    impl->state_mgr_.last_check_start_id_time_us_ = baseline;
    // Initial reconfirm meta does not request an early check.
    EXPECT_NE(old_pid, old_accepted.mode_version_);
    impl->state_mgr_.on_mode_meta_flushed();
    EXPECT_EQ(baseline, impl->state_mgr_.last_check_start_id_time_us_);

    const int64_t pid = old_pid + 1;
    impl->state_mgr_.prepare_meta_.log_proposal_id_ = pid;
    LogModeMeta meta;
    EXPECT_EQ(OB_SUCCESS, meta.generate(pid, pid, AccessMode::APPEND, SyncMode::SYNC, share::SCN::min_scn()));
    EXPECT_EQ(OB_SUCCESS, impl->after_flush_mode_meta_(pid, false, meta));
    EXPECT_EQ(OB_INVALID_TIMESTAMP, impl->state_mgr_.last_check_start_id_time_us_);
    EXPECT_EQ(trigger, impl->sw_.last_fetch_trigger_type_);
    EXPECT_EQ(fetch_time, impl->sw_.last_fetch_req_time_);
    EXPECT_EQ(fetch_end, impl->sw_.last_fetch_end_lsn_);
    EXPECT_EQ(fetch_committed, impl->sw_.last_fetch_committed_end_lsn_);
    EXPECT_EQ(fetch_max_id, impl->sw_.last_fetch_max_log_id_);
    // A second queued flush of the same meta must not re-arm a consumed check.
    impl->state_mgr_.last_check_start_id_time_us_ = baseline;
    EXPECT_EQ(OB_SUCCESS, impl->after_flush_mode_meta_(pid, false, meta));
    EXPECT_EQ(baseline, impl->state_mgr_.last_check_start_id_time_us_);
    EXPECT_EQ(OB_SUCCESS, impl->after_flush_mode_meta_(pid, true, meta));
    EXPECT_FALSE(impl->state_mgr_.is_mode_change_pending_());
    impl->after_flush_mode_meta_(old_pid, false, old_accepted);
    EXPECT_EQ(baseline, impl->state_mgr_.last_check_start_id_time_us_);
    EXPECT_EQ(fetch_time, impl->sw_.last_fetch_req_time_);

    impl->mode_mgr_.accepted_mode_meta_ = old_accepted;
    impl->mode_mgr_.applied_mode_meta_ = old_applied;
    impl->state_mgr_.prepare_meta_.log_proposal_id_ = old_pid;
    impl->state_mgr_.last_check_start_id_ = old_start_id;
    impl->state_mgr_.last_check_start_id_time_us_ = old_check_time;
  }
  revert_cluster_palf_handle_guard(replicas);
  leader.reset();
  EXPECT_EQ(OB_SUCCESS, delete_paxos_group(id));
}

TEST_F(TestObSimpleLogClusterAccessMode, basic_change_access_mode)
{
  SET_CASE_LOG_FILE(TEST_NAME, "basic_change_access_mode");
  int ret = OB_SUCCESS;
	const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  PALF_LOG(INFO, "begin test basic_change_access_mode", K(id));
	int64_t leader_idx = 0;
  int64_t ref_ts_ns = common::ObTimeUtility::current_time_ns() + 1800 * 1000L * 1000L * 1000L;
  share::SCN ref_scn;
  ref_scn.convert_for_logservice(ref_ts_ns);
  PalfHandleImplGuard leader;
  const int64_t CONFIG_CHANGE_TIMEOUT = 10 * 1000 * 1000 * 1000L; // 10s
  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 200, id));
  AccessMode curr_access_mode;
  int64_t mode_version, curr_proposal_id;
  ObRole unused_role;
  bool state;
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_access_mode(mode_version, curr_access_mode));
  EXPECT_EQ(AccessMode::APPEND, curr_access_mode);
  // test can not submit log in MODE_PREPARE
  EXPECT_EQ(OB_EAGAIN, leader.palf_handle_impl_->mode_mgr_.change_access_mode(mode_version, palf::AccessMode::RAW_WRITE, share::SCN::min_scn()));
  // cannot submit_log in prepare state
  EXPECT_EQ(OB_NOT_MASTER, submit_log(leader, 1, id));
  int tmp_ret = OB_EAGAIN;
  while(OB_EAGAIN == tmp_ret) {
    tmp_ret = leader.palf_handle_impl_->mode_mgr_.change_access_mode(mode_version, palf::AccessMode::RAW_WRITE, share::SCN::min_scn());
    usleep(10 * 1000);
  }
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_access_mode(mode_version, curr_access_mode));
  EXPECT_EQ(AccessMode::RAW_WRITE, curr_access_mode);

  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_role(unused_role, curr_proposal_id, state));
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->change_access_mode(curr_proposal_id, mode_version, palf::AccessMode::FLASHBACK, share::SCN::min_scn()));
  EXPECT_UNTIL_EQ(false, leader.palf_handle_impl_->mode_mgr_.resend_mode_meta_list_.is_valid());
  EXPECT_EQ(OB_NOT_MASTER, submit_log(leader, 1, id));
  // base_ts: 0.5 hour later
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_access_mode(mode_version, curr_access_mode));
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_role(unused_role, curr_proposal_id, state));
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->change_access_mode(curr_proposal_id, mode_version, AccessMode::APPEND, ref_scn));
  EXPECT_EQ(leader.palf_handle_impl_->sw_.get_max_scn(), ref_scn);
  EXPECT_UNTIL_EQ(false, leader.palf_handle_impl_->mode_mgr_.resend_mode_meta_list_.is_valid());
  // check all member's applied access_mode
  sleep(1);
  std::vector<PalfHandleImplGuard*> palf_list;
  EXPECT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id, palf_list));
  EXPECT_EQ(palf::AccessMode::APPEND, palf_list[0]->palf_handle_impl_->mode_mgr_.applied_mode_meta_.access_mode_);
  EXPECT_EQ(palf::AccessMode::APPEND, palf_list[1]->palf_handle_impl_->mode_mgr_.applied_mode_meta_.access_mode_);
  EXPECT_EQ(palf::AccessMode::APPEND, palf_list[2]->palf_handle_impl_->mode_mgr_.applied_mode_meta_.access_mode_);
  revert_cluster_palf_handle_guard(palf_list);

  std::vector<LSN> lsn_array;
  std::vector<SCN> scn_arrary;
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 50, id, lsn_array, scn_arrary));
  for (auto scn : scn_arrary)
  {
    EXPECT_GT(scn, ref_scn);
  }
  // 40 minutes later
  ref_ts_ns = common::ObTimeUtility::current_time_ns() + 2400L * 1000L * 1000L * 1000L;
  ref_scn.convert_for_logservice(ref_ts_ns);
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_access_mode(mode_version, curr_access_mode));
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_role(unused_role, curr_proposal_id, state));
  // APPEND -> APPEND
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->change_access_mode(curr_proposal_id, mode_version, AccessMode::APPEND, ref_scn));
  // can not APPEND -> FLASHBACK
  EXPECT_EQ(OB_STATE_NOT_MATCH, leader.palf_handle_impl_->change_access_mode(curr_proposal_id, mode_version, AccessMode::FLASHBACK, ref_scn));
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->change_access_mode(curr_proposal_id, mode_version, AccessMode::RAW_WRITE, ref_scn));
  EXPECT_UNTIL_EQ(false, leader.palf_handle_impl_->mode_mgr_.resend_mode_meta_list_.is_valid());
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_access_mode(mode_version, curr_access_mode));
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_role(unused_role, curr_proposal_id, state));
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->change_access_mode(curr_proposal_id, mode_version, AccessMode::APPEND, ref_scn));
  EXPECT_EQ(leader.palf_handle_impl_->sw_.get_max_scn(), ref_scn);
  EXPECT_UNTIL_EQ(false, leader.palf_handle_impl_->mode_mgr_.resend_mode_meta_list_.is_valid());
  lsn_array.clear();
  scn_arrary.clear();
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 50, id, lsn_array, scn_arrary));
  for (auto scn : scn_arrary)
  {
    EXPECT_GT(scn, ref_scn);
  }
  PALF_LOG(INFO, "end test basic_change_access_mode", K(id));
}

TEST_F(TestObSimpleLogClusterAccessMode, switch_leader1)
{
  SET_CASE_LOG_FILE(TEST_NAME, "switch_leader1");
  int ret = OB_SUCCESS;
	const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  PALF_LOG(INFO, "begin test switch_leader1", K(id));
	int64_t leader_idx = 0;
  PalfHandleImplGuard leader;
  bool state;
  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 200, id));
  AccessMode curr_access_mode;
  int64_t mode_version, curr_proposal_id;
  ObRole unused_role;
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_access_mode(mode_version, curr_access_mode));
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_role(unused_role, curr_proposal_id, state));
  EXPECT_EQ(AccessMode::APPEND, curr_access_mode);
  const int64_t follower1_idx = (leader_idx+1)%3;
  const int64_t follower2_idx = (leader_idx+2)%3;
  block_net(leader_idx, follower1_idx);

  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->change_access_mode(curr_proposal_id, mode_version, palf::AccessMode::RAW_WRITE, share::SCN::min_scn()));
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_role(unused_role, curr_proposal_id, state));
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_access_mode(mode_version, curr_access_mode));
  EXPECT_EQ(curr_proposal_id, mode_version);
  block_net(leader_idx, follower2_idx);
  sleep(10);
  // a follower will be leader
  while (true) {
    PalfHandleImplGuard new_leader;
    int64_t new_leader_idx;
    int64_t mode_version_after_reconfirm = -1;
    EXPECT_EQ(OB_SUCCESS, get_leader(id, new_leader, new_leader_idx));
    if (new_leader_idx != leader_idx) {
      EXPECT_EQ(OB_SUCCESS, new_leader.palf_handle_impl_->get_access_mode(mode_version_after_reconfirm, curr_access_mode));
      EXPECT_EQ(AccessMode::RAW_WRITE, curr_access_mode);
      // mode_version won't be changed after reconfirming
      EXPECT_EQ(mode_version_after_reconfirm, mode_version);
      EXPECT_EQ(OB_SUCCESS, new_leader.palf_handle_impl_->get_role(unused_role, curr_proposal_id, state));
      EXPECT_GT(curr_proposal_id, mode_version_after_reconfirm);
      break;
    } else {
      sleep(1);
    }
  }
  unblock_net(leader_idx, follower1_idx);
  unblock_net(leader_idx, follower2_idx);
  PALF_LOG(INFO, "end test switch_leader1", K(id));
}

TEST_F(TestObSimpleLogClusterAccessMode, add_member)
{
  SET_CASE_LOG_FILE(TEST_NAME, "add_member");
  OB_LOGGER.set_log_level("INFO");
  MockLocCB loc_cb;
  int ret = OB_SUCCESS;
	const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  PALF_LOG(INFO, "begin test add_member", K(id));
  {
	  int64_t leader_idx = 0;
    PalfHandleImplGuard leader;
    std::vector<PalfHandleImplGuard*> palf_list;
    const int64_t CONFIG_CHANGE_TIMEOUT = 10 * 1000 * 1000 * 1000L; // 10s
    EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
    EXPECT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id, palf_list));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 200, id));
    // set leader's region is different from follower, learner will
    // register itself to leader
    std::vector<ObRegion> region_list;
    LogMemberRegionMap region_map;
    common::ObRegion default_region(DEFAULT_REGION_NAME);
    get_cluster()[0]->get_locality_manager()->set_server_region(get_cluster()[0]->get_addr(), default_region);
    get_cluster()[0]->get_locality_manager()->set_server_region(get_cluster()[1]->get_addr(), ObRegion("SHANGHAI"));
    get_cluster()[0]->get_locality_manager()->set_server_region(get_cluster()[2]->get_addr(), ObRegion("TIANJIN"));
    for (auto palf_handle: palf_list) { palf_handle->palf_handle_impl_->update_self_region_(); }
    AccessMode curr_access_mode;
    int64_t mode_version, curr_proposal_id;
    ObRole unused_role;
    bool unused_is_pending_state = false;
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_access_mode(mode_version, curr_access_mode));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_role(unused_role, curr_proposal_id, unused_is_pending_state));
    EXPECT_EQ(AccessMode::APPEND, curr_access_mode);
    const int64_t follower1_idx = (leader_idx+1)%3;
    const int64_t follower2_idx = (leader_idx+2)%3;
    block_net(leader_idx, follower1_idx);
    // election membership version reaches majority
    sleep(1);

    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->change_access_mode(curr_proposal_id, mode_version, palf::AccessMode::RAW_WRITE, share::SCN::min_scn()));
    unblock_net(leader_idx, follower1_idx);
    block_net(leader_idx, follower2_idx);

    // new_leader需要在delete_paxos_group()之前析构，否则palf_handle引用计数无法清零
    PalfHandleImplGuard new_leader;
    int64_t new_leader_idx;
    EXPECT_EQ(OB_SUCCESS, get_leader(id, new_leader, new_leader_idx));
    loc_cb.leader_ = get_cluster()[new_leader_idx]->get_addr();
    PALF_LOG(INFO, "set leader for loc_cb", "leader", get_cluster()[new_leader_idx]->get_addr());
    EXPECT_EQ(OB_SUCCESS, palf_list[3]->palf_handle_impl_->set_location_cache_cb(&loc_cb));

    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_learner(ObMember(get_cluster()[3]->get_addr(), 1), CONFIG_CHANGE_TIMEOUT));
    sleep(2);
    LogConfigVersion config_version;
    ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->switch_learner_to_acceptor(ObMember(get_cluster()[3]->get_addr(), 1), 4, config_version, CONFIG_CHANGE_TIMEOUT));
    unblock_net(leader_idx, follower2_idx);
    revert_cluster_palf_handle_guard(palf_list);
  }
  delete_paxos_group(id);
  PALF_LOG(INFO, "end test add_member", K(id));
}

TEST_F(TestObSimpleLogClusterAccessMode, prev_log_slide)
{
  SET_CASE_LOG_FILE(TEST_NAME, "prev_log_slide");
  int ret = OB_SUCCESS;
	const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  PALF_LOG(INFO, "begin test prev_log_slide", K(id));
	int64_t leader_idx = 0;
  PalfHandleImplGuard leader;
  const int64_t CONFIG_CHANGE_TIMEOUT = 10 * 1000 * 1000 * 1000L; // 10s
  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 200, id));

  // inc current proposal_id
  int64_t curr_pid = 0;
  ObRole role;
  bool state;
  int64_t unused_out_proposal_id = 0;
  std::vector<PalfHandleImplGuard*> palf_list;
  EXPECT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id, palf_list));
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_role(role, curr_pid, state));
  EXPECT_EQ(OB_EAGAIN, leader.palf_handle_impl_->mode_mgr_.switch_state_(AccessMode::RAW_WRITE, SyncMode::ASYNC, share::SCN::min_scn(), false, unused_out_proposal_id));
  // can not submit_log, config_change in prepare state
  EXPECT_FALSE(leader.palf_handle_impl_->mode_mgr_.can_append());
  LogConfigChangeArgs args(ObMember(get_cluster()[3]->get_addr(), 1), 0, ADD_LEARNER);
  EXPECT_EQ(OB_NOT_MASTER, submit_log(leader, 1, id));
  const LogConfigMeta config_meta = leader.palf_handle_impl_->config_mgr_.log_ms_meta_;
  LogConfigVersion config_version;
  const int64_t proposal_id = leader.palf_handle_impl_->state_mgr_.get_proposal_id();
  const int64_t leader_epoch = leader.palf_handle_impl_->state_mgr_.get_leader_epoch();
  EXPECT_EQ(OB_ERR_UNEXPECTED, leader.palf_handle_impl_->config_mgr_.change_config(args, proposal_id, leader_epoch, config_version));
  const LogConfigMeta new_config_meta = leader.palf_handle_impl_->config_mgr_.log_ms_meta_;
  EXPECT_EQ(config_meta.curr_.config_.config_version_, new_config_meta.curr_.config_.config_version_);
  // wait prepare req reaches majority
  sleep(1);
  // switch to accept state
  EXPECT_EQ(OB_EAGAIN, leader.palf_handle_impl_->mode_mgr_.switch_state_(AccessMode::RAW_WRITE, SyncMode::ASYNC, share::SCN::min_scn(), false, unused_out_proposal_id));
  EXPECT_TRUE(leader.palf_handle_impl_->mode_mgr_.can_append());
  int64_t new_pid = 0;
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_role(role, new_pid, state));
  EXPECT_EQ(curr_pid + 1, new_pid);
  EXPECT_EQ(curr_pid + 1, palf_list[1]->palf_handle_impl_->state_mgr_.get_proposal_id());
  EXPECT_EQ(curr_pid + 1, palf_list[2]->palf_handle_impl_->state_mgr_.get_proposal_id());
  //  apply config_change barrier with older log proposal_id
  EXPECT_EQ(OB_EAGAIN, leader.palf_handle_impl_->config_mgr_.change_config(args, proposal_id, leader_epoch, config_version));

  // submit log with new proposal_id and wait for sliding out in follower
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));
  while (true) {
    if (palf_list[1]->palf_handle_impl_->sw_.last_slide_log_pid_ == new_pid &&
        palf_list[2]->palf_handle_impl_->sw_.last_slide_log_pid_ == new_pid) {
      break;
    } else {
      EXPECT_EQ(OB_SUCCESS, submit_log(leader, 1, id));
      usleep(500 * 1000);
    }
  }
  sleep(1);
  revert_cluster_palf_handle_guard(palf_list);
  // continue add_learner, prev log must have slided
  while(true) {
    if (OB_SUCC(leader.palf_handle_impl_->config_mgr_.change_config(args, proposal_id, leader_epoch, config_version))) {
      break;
    } else {
      usleep(100 * 1000);
    }
  }
  PALF_LOG(INFO, "end test prev_log_slide", K(id));
}

TEST_F(TestObSimpleLogClusterAccessMode, single_replica_change_access_mode)
{
  SET_CASE_LOG_FILE(TEST_NAME, "single_replica_change_access_mode");
  int ret = OB_SUCCESS;
	const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  PALF_LOG(INFO, "begin test single_replica_change_access_mode", K(id));
	int64_t leader_idx = 0;
  int64_t ref_ts_ns = common::ObTimeUtility::current_time_ns() + 1800 * 1000L * 1000L * 1000L;
  share::SCN ref_scn;
  ref_scn.convert_for_logservice(ref_ts_ns);
  PalfHandleImplGuard leader;
  const int64_t CONFIG_CHANGE_TIMEOUT = 10 * 1000 * 1000 * 1000L; // 10s
  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 200, id));
  const int64_t b_idx = (leader_idx + 1) % 3;
  const int64_t c_idx = (leader_idx + 2) % 3;
  const common::ObAddr b_addr = get_cluster()[b_idx]->get_addr();
  const common::ObAddr c_addr = get_cluster()[c_idx]->get_addr();
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->remove_member(common::ObMember(b_addr, 1), 2, CONFIG_CHANGE_TIMEOUT));
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->remove_member(common::ObMember(c_addr, 1), 1, CONFIG_CHANGE_TIMEOUT));

  AccessMode curr_access_mode;
  int64_t mode_version, curr_proposal_id;
  ObRole unused_role;
  bool state;
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_access_mode(mode_version, curr_access_mode));
  EXPECT_EQ(AccessMode::APPEND, curr_access_mode);
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_role(unused_role, curr_proposal_id, state));
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->change_access_mode(curr_proposal_id, mode_version, palf::AccessMode::RAW_WRITE, share::SCN::min_scn()));
  EXPECT_EQ(OB_NOT_MASTER, submit_log(leader, 1, id));
  // base_ts: 0.5 hour later
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_access_mode(mode_version, curr_access_mode));
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_role(unused_role, curr_proposal_id, state));
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->change_access_mode(curr_proposal_id, mode_version, AccessMode::APPEND, ref_scn));
  // check all member's applied access_mode
  EXPECT_EQ(leader.palf_handle_impl_->sw_.get_max_scn(), ref_scn);
  sleep(1);
  std::vector<PalfHandleImplGuard*> palf_list;
  EXPECT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id, palf_list));
  EXPECT_EQ(palf::AccessMode::APPEND, palf_list[0]->palf_handle_impl_->mode_mgr_.applied_mode_meta_.access_mode_);
  EXPECT_EQ(palf::AccessMode::APPEND, palf_list[1]->palf_handle_impl_->mode_mgr_.applied_mode_meta_.access_mode_);
  EXPECT_EQ(palf::AccessMode::APPEND, palf_list[2]->palf_handle_impl_->mode_mgr_.applied_mode_meta_.access_mode_);

  revert_cluster_palf_handle_guard(palf_list);
  PALF_LOG(INFO, "end test single_replica_change_access_mode", K(id));
}

} // end unittest
} // end oceanbase

int main(int argc, char **argv)
{
  RUN_SIMPLE_LOG_CLUSTER_TEST(TEST_NAME);
}
