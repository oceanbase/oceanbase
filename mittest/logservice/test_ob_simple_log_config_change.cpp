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

#include <gtest/gtest.h>
#include <thread>
#define private public
#define protected public
#include "env/ob_simple_log_cluster_env.h"
#include "logservice/leader_coordinator/election_priority_impl/election_priority_impl.h"
#include "logservice/ob_ls_retention_gc_handler.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#undef protected
#undef private

const std::string TEST_NAME = "config_change";

using namespace oceanbase::common;
using namespace oceanbase;
namespace oceanbase
{
using namespace logservice;
namespace unittest
{
// Keep one fixture so the shared log cluster is destroyed only after all cases.
class TestObSimpleLogClusterConfigChange : public ObSimpleLogClusterTestEnv
{
public:
  TestObSimpleLogClusterConfigChange()
    : ObSimpleLogClusterTestEnv(), id_(-1), leader_idx_(0), leader_(), ls_{},
      saved_copies_(0), config_saved_(false), saved_self_(), saved_cluster_id_(0) {}
  void init_retention_gc()
  {
    saved_self_ = GCTX.self_addr();
    saved_cluster_id_ = GCONF.cluster_id;
    GCONF.cluster_id = server(0)->cluster_id_;
    id_ = ATOMIC_AAF(&palf_id_, 1);
    ASSERT_EQ(OB_SUCCESS, create_paxos_group(id_, leader_idx_, leader_));
    GCTX.self_addr_seq_.set_addr(get_cluster()[leader_idx_]->get_addr());
    const int ret = omt::ObTenantConfigMgr::get_instance().add_tenant_config(1002);
    ASSERT_TRUE(OB_SUCCESS == ret || OB_ENTRY_EXIST == ret);
    {
      omt::ObTenantConfigGuard config(
          omt::ObTenantConfigMgr::get_instance().get_tenant_config_with_lock(1002, 0, 0));
      ASSERT_TRUE(config.is_valid());
      saved_copies_ = config->log_retention_replica_count;
      config_saved_ = true;
      config->log_retention_replica_count = 4;
    }
    for (int64_t i = 0; i < 7; ++i) {
      bind_gc(i);
      ASSERT_FALSE(HasFatalFailure());
    }
    ASSERT_EQ(OB_SUCCESS, submit_log(leader_, 8, id_, 512));
    wait_committed(leader_, leader_.palf_handle_impl_->get_max_lsn());
    ASSERT_FALSE(HasFatalFailure());
    std::vector<PalfHandleImplGuard*> replicas;
    ASSERT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id_, replicas));
    DEFER(revert_cluster_palf_handle_guard(replicas));
    // Majority commit does not guarantee that every Paxos replica has committed the tail.
    for (int64_t i = 0; i < 3; ++i) {
      sync_committed_info(i, *replicas[i]);
      ASSERT_FALSE(HasFatalFailure());
    }
    for (int64_t i = 3; i <= 4; ++i) {
      ASSERT_EQ(OB_SUCCESS, leader_.palf_handle_impl_->add_learner(
          ObMember(get_cluster()[i]->get_addr(), 1), 30 * 1000 * 1000L));
      ASSERT_EQ(OB_SUCCESS, check_replica_sync(id_, &leader_, replicas[i], 30 * 1000 * 1000L));
      sync_committed_info(i, *replicas[i]);
      ASSERT_FALSE(HasFatalFailure());
      ASSERT_EQ(OB_SUCCESS, leader_.palf_handle_impl_->remove_learner(
          ObMember(get_cluster()[i]->get_addr(), 1), 30 * 1000 * 1000L));
    }
  }
  void wait_committed(PalfHandleImplGuard &replica, const LSN &end)
  {
    const int64_t deadline = ObTimeUtility::current_monotonic_time() + 30 * 1000 * 1000L;
    while (replica.palf_handle_impl_->get_end_lsn() < end
        && ObTimeUtility::current_monotonic_time() < deadline) {
      ob_usleep(1000);
    }
    ASSERT_GE(replica.palf_handle_impl_->get_end_lsn(), end);
  }
  void sync_committed_info(const int64_t i, PalfHandleImplGuard &replica)
  {
    PalfHandleImpl *leader = leader_.palf_handle_impl_;
    const LSN committed = leader->get_end_lsn();
    int64_t log_id = OB_INVALID_LOG_ID;
    int64_t log_proposal_id = INVALID_PROPOSAL_ID;
    int64_t checksum = 0;
    SCN scn;
    LSN lsn;
    LSN end;
    const int64_t deadline = ObTimeUtility::current_monotonic_time() + 30 * 1000 * 1000L;
    do {
      leader->sw_.get_last_slide_log_info_(log_id, scn, lsn, end, log_proposal_id, checksum);
      if (end < committed) { ob_usleep(1000); }
    } while (end < committed && ObTimeUtility::current_monotonic_time() < deadline);
    ASSERT_EQ(committed, end);
    // A replica may miss the final commit broadcast; learners are not its destinations.
    // Retry because a replica ignores committed info until its logs are flushed and submitted.
    while (replica.palf_handle_impl_->get_end_lsn() < committed
        && ObTimeUtility::current_monotonic_time() < deadline) {
      ASSERT_EQ(OB_SUCCESS, leader->log_engine_.submit_committed_info_req(
          get_cluster()[i]->get_addr(), leader->state_mgr_.get_proposal_id(),
          log_id, log_proposal_id, committed));
      ob_usleep(10 * 1000);
    }
    ASSERT_GE(replica.palf_handle_impl_->get_end_lsn(), committed);
    while (replica.palf_handle_impl_->get_end_scn() < scn
        && ObTimeUtility::current_monotonic_time() < deadline) {
      ob_usleep(1000);
    }
    ASSERT_GE(replica.palf_handle_impl_->get_end_scn(), scn);
  }
  void TearDown() override
  {
    if (0 <= id_) {
      leader_.reset();
      for (int64_t i = 0; i < 7; ++i) {
        if (NULL != ls_[i]) {
          ls_[i]->get_gc_handler()->reset();
          ls_[i] = NULL;
        }
        server(i)->get_tenant_base()->set(static_cast<storage::ObLSService *>(NULL));
      }
    }
    ObSimpleLogClusterTestEnv::TearDown();
    if (config_saved_) {
      omt::ObTenantConfigGuard config(
          omt::ObTenantConfigMgr::get_instance().get_tenant_config_with_lock(1002, 0, 0));
      if (config.is_valid()) {
        config->log_retention_replica_count = saved_copies_;
      }
    }
    if (0 <= id_) {
      GCTX.self_addr_seq_.set_addr(saved_self_);
      GCONF.cluster_id = saved_cluster_id_;
    }
  }
  ObSimpleLogServer *server(const int64_t i)
  {
    return static_cast<ObSimpleLogServer *>(get_cluster()[i]);
  }
  void bind_gc(const int64_t i)
  {
    ObSimpleLogServer *svr = server(i);
    ObTenantEnv::set_tenant(svr->get_tenant_base());
    ASSERT_EQ(OB_SUCCESS, svr->init_ls_service_for_role_change_());
    ASSERT_EQ(OB_SUCCESS, svr->add_empty_ls_to_ls_map_(id_, ls_[i]));
    svr->get_tenant_base()->set(&svr->ls_service_);
    ASSERT_EQ(OB_SUCCESS, ls_[i]->get_gc_handler()->init(ls_[i]));
  }
  ObLSRetentionGCHandler::Leader &gc_leader(const int64_t i)
  {
    return ls_[i]->get_gc_handler()->retention_gc_handler_.leader_;
  }
  void start_request(const int64_t i, ObLSGCRequest &request)
  {
    ObTenantEnv::set_tenant(get_cluster()[i]->get_tenant_base());
    PalfHandleGuard guard;
    PalfStat stat;
    ASSERT_EQ(OB_SUCCESS, server(i)->log_service_.open_palf(ObLSID(id_), guard));
    ASSERT_EQ(OB_SUCCESS, guard.stat(stat));
    ASSERT_LT(stat.begin_scn_, stat.end_scn_);
    ASSERT_EQ(OB_SUCCESS, request.init(get_cluster()[i]->get_addr(), ObLSID(id_),
        stat.begin_scn_, stat.end_scn_, LogConfigVersion(), stat.mode_version_,
        stat.begin_lsn_, stat.end_lsn_, INVALID_GC_TOKEN));
  }
  int send(const ObLSGCRequest &request, ObLSGCResponse &response, const int64_t destination,
           const int64_t timeout_us)
  {
    response.reset();
    return server(5)->log_service_.get_rpc_proxy()->to(get_cluster()[destination]->get_addr())
        .by(1002).timeout(timeout_us).group_id(share::OBCG_STORAGE).request_ls_gc(request, response);
  }
  void advance(ObLSGCRequest &request, const ObLSGCResponse &response)
  {
    ASSERT_TRUE(response.is_valid());
    ASSERT_EQ(OB_SUCCESS, request.init(request.src_, request.get_ls_id(), request.get_cutoff_scn(),
        request.get_end_scn(), response.get_config_version(), request.get_mode_version(),
        request.get_begin_lsn(), request.get_end_lsn(), response.get_gc_token()));
  }
  void handshake(ObLSGCRequest &request)
  {
    ObLSGCResponse response;
    ASSERT_EQ(INVALID_GC_TOKEN, request.get_gc_token());
    ASSERT_EQ(OB_SUCCESS, send(request, response, leader_idx_, 5 * 1000 * 1000L));
    ASSERT_FALSE(response.is_gc_allowed());
    ASSERT_GT(response.get_gc_token(), INVALID_GC_TOKEN);
    advance(request, response);
  }
  void hold(ObLSGCRequest &request)
  {
    ObLSGCResponse response;
    ASSERT_EQ(OB_SUCCESS, send(request, response, leader_idx_, 5 * 1000 * 1000L));
    ASSERT_FALSE(response.is_gc_allowed());
    ASSERT_GT(response.get_gc_token(), request.get_gc_token());
    advance(request, response);
  }
  void refresh()
  {
    ObTenantEnv::set_tenant(get_cluster()[leader_idx_]->get_tenant_base());
    ASSERT_EQ(OB_SUCCESS, gc_leader(leader_idx_).refresh_cache());
    ASSERT_EQ(3, gc_leader(leader_idx_).member_replica_caches_.count());
    for (int64_t i = 0; i < 3; ++i) {
      ASSERT_TRUE(gc_leader(leader_idx_).member_replica_caches_.at(i).is_valid());
    }
  }
  void approve(ObLSGCRequest &request)
  {
    ObLSGCResponse response;
    ASSERT_EQ(OB_SUCCESS, send(request, response, leader_idx_, 5 * 1000 * 1000L));
    ASSERT_TRUE(response.is_gc_allowed());
    ASSERT_EQ(request.get_gc_token(), response.get_gc_token());
    ASSERT_TRUE(gc_leader(leader_idx_).is_gc_approved_(request.src_));
  }
  void reject(const ObLSGCRequest &request)
  {
    ObLSGCResponse response;
    ASSERT_EQ(OB_SUCCESS, send(request, response, leader_idx_, 5 * 1000 * 1000L));
    ASSERT_FALSE(response.is_gc_allowed());
    ASSERT_EQ(INVALID_GC_TOKEN, response.get_gc_token());
  }
  int64_t id_;
  int64_t leader_idx_;
  PalfHandleImplGuard leader_;
  storage::ObLS *ls_[7];
  int64_t saved_copies_;
  bool config_saved_;
  ObAddr saved_self_;
  int64_t saved_cluster_id_;
};

int64_t ObSimpleLogClusterTestBase::member_cnt_ = 3;
int64_t ObSimpleLogClusterTestBase::node_cnt_ = 7;
std::string ObSimpleLogClusterTestBase::test_name_ = TEST_NAME;
bool ObSimpleLogClusterTestBase::need_add_arb_server_  = false;
bool ObSimpleLogClusterTestBase::need_remote_log_store_ = false;

bool check_children_valid(const std::vector<PalfHandleImplGuard*> &palf_list, const LogLearnerList &all_learner)
{
  int ret = OB_SUCCESS;
  LogLearnerList all_children;
  for (auto palf : palf_list) {
    const LogLearnerList &self_children = palf->palf_handle_impl_->config_mgr_.children_;
    int64_t children_cnt = self_children.get_member_number();
    for (int i = 0; i < children_cnt; ++i) {
      LogLearner tmp_server;
      if (OB_FAIL(self_children.get_learner(i, tmp_server))) {
      } else if (OB_FAIL(all_children.add_learner(tmp_server))) {
      }
    }
  }
  bool bool_ret = all_children.learner_addr_equal(all_learner);
  PALF_LOG(INFO, "check_children", K(ret), K(all_children), K(all_learner));
  return bool_ret;
}

bool check_parent(const std::vector<PalfHandleImplGuard*> &palf_list, const LogLearnerList &all_learner, const ObAddr &parent)
{
  bool bool_ret = true;
  for (auto palf : palf_list) {
    const ObAddr &self = palf->palf_handle_impl_->self_;
    if (all_learner.contains(self)) {
      const ObAddr &my_parent = palf->palf_handle_impl_->config_mgr_.parent_;
      if (my_parent.is_valid() && (my_parent == parent)) {
        continue;
      } else {
        bool_ret = false;
        break;
      }
    }
  }
  return bool_ret;
}

int check_log_sync(const std::vector<PalfHandleImplGuard*> &palf_list,
                   const common::ObMemberList &member_list,
                   const LogLearnerList &learner_list,
                   PalfHandleImplGuard &leader)
{
  int ret = OB_SUCCESS;
  const int64_t max_flushed_proposal_id = leader.palf_handle_impl_->sw_.max_flushed_log_pid_;
  const LSN max_flushed_end_lsn = leader.palf_handle_impl_->sw_.max_flushed_end_lsn_;
  PALF_LOG(INFO, "before check_log_sync", K(max_flushed_proposal_id), K(max_flushed_end_lsn));
  for (auto palf : palf_list) {
    const common::ObAddr parent_addr = palf->palf_handle_impl_->config_mgr_.parent_;
    // for paxos member, log must sync with leader
    // for learner, sync with parent's committed_end_lsn
    const int64_t this_max_flushed_proposal_id = palf->palf_handle_impl_->sw_.max_flushed_log_pid_;
    const LSN this_max_flushed_end_lsn = palf->palf_handle_impl_->sw_.max_flushed_end_lsn_;
    const common::ObAddr self = palf->palf_handle_impl_->self_;
    if (member_list.contains(self)) {
      PALF_LOG(INFO, "before check", K(max_flushed_proposal_id), K(max_flushed_end_lsn), K(this_max_flushed_proposal_id),
          K(this_max_flushed_end_lsn), K(self));
      EXPECT_EQ(max_flushed_proposal_id, this_max_flushed_proposal_id);
      EXPECT_EQ(max_flushed_end_lsn, this_max_flushed_end_lsn) \
          << max_flushed_end_lsn.val_ << "," << this_max_flushed_end_lsn.val_;
    } else if (!learner_list.contains(self)) {
    } else if (!parent_addr.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      break;
    } else {
      for (auto parent: palf_list) {
        if (parent->palf_handle_impl_->self_ == parent_addr) {
          const LSN parent_committed_end_lsn = parent->palf_handle_impl_->sw_.committed_end_lsn_;
          EXPECT_EQ(parent_committed_end_lsn, this_max_flushed_end_lsn) \
              << parent_committed_end_lsn.val_ << "," << this_max_flushed_end_lsn.val_;
          if (parent_committed_end_lsn != this_max_flushed_end_lsn) {
            PALF_LOG(ERROR, "log not sync", K(parent_addr), "self", palf->palf_handle_impl_->self_,
                K(parent_committed_end_lsn), K(this_max_flushed_end_lsn));
          }
        }
      }
    }
  }
  return ret;
}

MockLocCB loc_cb;

bool wait_election_leader(PalfHandleImplGuard *palf, const int64_t timeout_us)
{
  const int64_t end_ts = ObTimeUtility::current_time() + timeout_us;
  bool bool_ret = false;
  while (!bool_ret && ObTimeUtility::current_time() < end_ts) {
    ObRole election_role = INVALID_ROLE;
    int64_t election_epoch = OB_INVALID_TIMESTAMP;
    if (OB_SUCCESS == palf->palf_handle_impl_->state_mgr_.get_election_role(election_role, election_epoch) &&
        LEADER == election_role) {
      bool_ret = true;
    } else {
      ob_usleep(200 * 1000);
    }
  }
  return bool_ret;
}

bool wait_election_leader_in_replicas(const std::vector<PalfHandleImplGuard*> &palf_list,
                                      const int64_t idx1,
                                      const int64_t idx2,
                                      const int64_t timeout_us,
                                      int64_t &leader_idx)
{
  const int64_t end_ts = ObTimeUtility::current_time() + timeout_us;
  bool bool_ret = false;
  leader_idx = OB_INVALID_INDEX;
  while (!bool_ret && ObTimeUtility::current_time() < end_ts) {
    ObRole election_role = INVALID_ROLE;
    int64_t election_epoch = OB_INVALID_TIMESTAMP;
    if (OB_SUCCESS == palf_list[idx1]->palf_handle_impl_->state_mgr_.get_election_role(
        election_role, election_epoch) && LEADER == election_role) {
      leader_idx = idx1;
      bool_ret = true;
    } else if (OB_SUCCESS == palf_list[idx2]->palf_handle_impl_->state_mgr_.get_election_role(
        election_role, election_epoch) && LEADER == election_role) {
      leader_idx = idx2;
      bool_ret = true;
    } else {
      ob_usleep(200 * 1000);
    }
  }
  return bool_ret;
}

bool wait_config_version_ge(PalfHandleImplGuard *palf,
                            const LogConfigVersion &target_config_version,
                            const int64_t timeout_us)
{
  const int64_t end_ts = ObTimeUtility::current_time() + timeout_us;
  bool bool_ret = false;
  while (!bool_ret && ObTimeUtility::current_time() < end_ts) {
    LogConfigVersion config_version;
    if (OB_SUCCESS == palf->palf_handle_impl_->get_config_version(config_version) &&
        config_version >= target_config_version) {
      bool_ret = true;
    } else {
      ob_usleep(200 * 1000);
    }
  }
  return bool_ret;
}

TEST_F(TestObSimpleLogClusterConfigChange, logonly_election_leader_is_blocked_after_config_change)
{
  SET_CASE_LOG_FILE(TEST_NAME, "logonly_election_leader_is_blocked_after_config_change");
  const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  const int64_t member_cnt = 4;
  const int64_t logonly_idx = 3;
  const int64_t WAIT_TIMEOUT_US = 90 * 1000 * 1000L;
  int64_t leader_idx = -1;
  int64_t other_f_idx = OB_INVALID_INDEX;
  int64_t peer_f_idx = OB_INVALID_INDEX;
  ObMemberList member_list;
  PalfHandleImplGuard leader;
  std::vector<PalfHandleImplGuard*> palf_list;

  PALF_LOG(INFO, "begin logonly_election_leader_is_blocked_after_config_change", K(id));
  for (int64_t i = 0; i < member_cnt; ++i) {
    ASSERT_EQ(OB_SUCCESS, member_list.add_member(ObMember(get_cluster()[i]->get_addr(), 1)));
  }
  ASSERT_EQ(OB_SUCCESS, create_paxos_group_with_logonly(id, member_list, member_cnt,
      logonly_idx, leader_idx, leader));
  ASSERT_NE(logonly_idx, leader_idx);
  ASSERT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id, palf_list));
  ASSERT_EQ(LogReplicaType::LOGONLY_REPLICA,
      palf_list[logonly_idx]->palf_handle_impl_->state_mgr_.get_replica_type());
  for (int64_t i = 0; i < member_cnt; ++i) {
    if (i != leader_idx && i != logonly_idx) {
      if (OB_INVALID_INDEX == other_f_idx) {
        other_f_idx = i;
      } else {
        peer_f_idx = i;
        break;
      }
    }
  }
  ASSERT_NE(OB_INVALID_INDEX, other_f_idx);
  ASSERT_NE(OB_INVALID_INDEX, peer_f_idx);
  DEFER({
    unblock_all_net(leader_idx);
    unblock_pcode(other_f_idx, ObRpcPacketCode::OB_LOG_CHANGE_CONFIG_META_REQ);
    unblock_pcode(peer_f_idx, ObRpcPacketCode::OB_LOG_CHANGE_CONFIG_META_REQ);
    revert_cluster_palf_handle_guard(palf_list);
    leader.reset();
    delete_paxos_group(id);
  });

  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 20, id));
  ASSERT_EQ(OB_SUCCESS, wait_until_has_committed(leader, leader.palf_handle_impl_->get_max_lsn()));
  for (int64_t i = 0; i < member_cnt; ++i) {
    if (i != leader_idx && i != logonly_idx) {
      ASSERT_EQ(OB_SUCCESS, check_replica_sync(id, &leader, palf_list[i], WAIT_TIMEOUT_US));
    }
  }

  block_pcode(other_f_idx, ObRpcPacketCode::OB_LOG_CHANGE_CONFIG_META_REQ);
  block_pcode(peer_f_idx, ObRpcPacketCode::OB_LOG_CHANGE_CONFIG_META_REQ);

  LogConfigVersion leader_config_version;
  LogConfigVersion logonly_config_version;
  LogConfigVersion other_f_config_version;
  LogConfigVersion peer_f_config_version;
  ASSERT_EQ(OB_SUCCESS, palf_list[other_f_idx]->palf_handle_impl_->get_config_version(other_f_config_version));
  ASSERT_EQ(OB_SUCCESS, palf_list[peer_f_idx]->palf_handle_impl_->get_config_version(peer_f_config_version));
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->remove_member(
      ObMember(palf_list[peer_f_idx]->palf_handle_impl_->self_, 1), member_cnt - 1, WAIT_TIMEOUT_US));
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(leader_config_version));
  ASSERT_TRUE(wait_config_version_ge(palf_list[logonly_idx], leader_config_version, WAIT_TIMEOUT_US));
  ASSERT_EQ(OB_SUCCESS, palf_list[logonly_idx]->palf_handle_impl_->get_config_version(logonly_config_version));
  ASSERT_EQ(OB_SUCCESS, palf_list[other_f_idx]->palf_handle_impl_->get_config_version(other_f_config_version));
  ASSERT_EQ(OB_SUCCESS, palf_list[peer_f_idx]->palf_handle_impl_->get_config_version(peer_f_config_version));
  ASSERT_EQ(leader_config_version, logonly_config_version);
  ASSERT_GT(logonly_config_version, other_f_config_version);
  ASSERT_GT(logonly_config_version, peer_f_config_version);

  block_all_net(leader_idx);
  ASSERT_TRUE(wait_election_leader(palf_list[logonly_idx], WAIT_TIMEOUT_US));
  ASSERT_FALSE(palf_list[logonly_idx]->palf_handle_impl_->state_mgr_.is_leader_active());
  unblock_pcode(other_f_idx, ObRpcPacketCode::OB_LOG_CHANGE_CONFIG_META_REQ);
  ASSERT_TRUE(wait_config_version_ge(palf_list[other_f_idx], logonly_config_version,
      WAIT_TIMEOUT_US));
  ASSERT_TRUE(wait_election_leader(palf_list[other_f_idx], WAIT_TIMEOUT_US));
  ASSERT_EQ(LogReplicaType::NORMAL_REPLICA,
      palf_list[other_f_idx]->palf_handle_impl_->state_mgr_.get_replica_type());
  PALF_LOG(INFO, "end logonly_election_leader_is_blocked_after_config_change", K(id));
}

TEST_F(TestObSimpleLogClusterConfigChange, logonly_is_not_elected_when_all_f_have_clog_full)
{
  SET_CASE_LOG_FILE(TEST_NAME, "logonly_is_not_elected_when_all_f_have_clog_full");
  const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  const int64_t member_cnt = 4;
  const int64_t logonly_idx = 3;
  const int64_t WAIT_TIMEOUT_US = 90 * 1000 * 1000L;
  int64_t leader_idx = -1;
  int64_t other_f_idx = OB_INVALID_INDEX;
  int64_t peer_f_idx = OB_INVALID_INDEX;
  int64_t new_f_leader_idx = OB_INVALID_INDEX;
  ObMemberList member_list;
  PalfHandleImplGuard leader;
  std::vector<PalfHandleImplGuard*> palf_list;
  logservice::coordinator::ElectionPriorityImpl election_priority_list[member_cnt];

  PALF_LOG(INFO, "begin logonly_is_not_elected_when_all_f_have_clog_full", K(id));
  for (int64_t i = 0; i < member_cnt; ++i) {
    ASSERT_EQ(OB_SUCCESS, member_list.add_member(ObMember(get_cluster()[i]->get_addr(), 1)));
  }
  ASSERT_EQ(OB_SUCCESS, create_paxos_group_with_logonly(id, member_list, member_cnt,
      logonly_idx, leader_idx, leader));
  ASSERT_NE(logonly_idx, leader_idx);
  ASSERT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id, palf_list));
  ASSERT_EQ(LogReplicaType::LOGONLY_REPLICA,
      palf_list[logonly_idx]->palf_handle_impl_->state_mgr_.get_replica_type());
  for (int64_t i = 0; i < member_cnt; ++i) {
    election_priority_list[i].set_ls_id(share::ObLSID(id));
    ASSERT_EQ(OB_SUCCESS,
        palf_list[i]->palf_handle_impl_->set_election_priority(&election_priority_list[i]));
  }
  for (int64_t i = 0; i < member_cnt; ++i) {
    if (i != leader_idx && i != logonly_idx) {
      if (OB_INVALID_INDEX == other_f_idx) {
        other_f_idx = i;
      } else {
        peer_f_idx = i;
        break;
      }
    }
  }
  ASSERT_NE(OB_INVALID_INDEX, other_f_idx);
  ASSERT_NE(OB_INVALID_INDEX, peer_f_idx);
  DEFER({
    unblock_all_net(leader_idx);
    for (int64_t i = 0; i < member_cnt; ++i) {
      if (i < static_cast<int64_t>(palf_list.size())) {
        (void)palf_list[i]->palf_handle_impl_->reset_election_priority();
      }
    }
    revert_cluster_palf_handle_guard(palf_list);
    leader.reset();
    delete_paxos_group(id);
  });

  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 20, id));
  ASSERT_EQ(OB_SUCCESS, wait_until_has_committed(leader, leader.palf_handle_impl_->get_max_lsn()));
  for (int64_t i = 0; i < member_cnt; ++i) {
    palf::election::ElectionPriority *priority =
        palf_list[i]->palf_handle_impl_->election_.priority_;
    ASSERT_TRUE(NULL != priority);
    logservice::coordinator::PriorityV1 &priority_v1 =
        election_priority_list[i].priority_tuple_.element<1>();
    priority_v1.fatal_failures_.reset();
    if (i != logonly_idx) {
      ASSERT_EQ(OB_SUCCESS, priority_v1.fatal_failures_.push_back(
          logservice::coordinator::FailureEvent(
              logservice::coordinator::FailureType::RESOURCE_NOT_ENOUGH,
              logservice::coordinator::FailureModule::LOG,
              logservice::coordinator::FailureLevel::FATAL)));
    }
    priority_v1.is_valid_ = true;
    ASSERT_EQ(i != logonly_idx, priority->has_fatal_failure());
  }
  for (int64_t i = 0; i < member_cnt; ++i) {
    if (i != logonly_idx) {
      int compare_result = 0;
      ObStringHolder reason;
      ASSERT_EQ(OB_SUCCESS, election_priority_list[i].compare_with(
          election_priority_list[logonly_idx], CLUSTER_VERSION_4_0_0_0,
          true /* decentralized_voting */, compare_result, reason));
      ASSERT_LT(compare_result, 0);
    }
  }

  block_all_net(leader_idx);
  ASSERT_TRUE(wait_election_leader_in_replicas(palf_list, other_f_idx, peer_f_idx,
      WAIT_TIMEOUT_US, new_f_leader_idx));
  ASSERT_EQ(LogReplicaType::NORMAL_REPLICA,
      palf_list[new_f_leader_idx]->palf_handle_impl_->state_mgr_.get_replica_type());
  ASSERT_FALSE(wait_election_leader(palf_list[logonly_idx], 3 * 1000 * 1000L));
  PALF_LOG(INFO, "end logonly_is_not_elected_when_all_f_have_clog_full",
      K(id), K(new_f_leader_idx));
}

TEST_F(TestObSimpleLogClusterConfigChange, split_brain)
{
  SET_CASE_LOG_FILE(TEST_NAME, "split_brain");
  std::vector<PalfHandleImplGuard*> palf_list;
  int ret = OB_SUCCESS;
	const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  PALF_LOG(INFO, "begin test split_brain", K(id));
	int64_t leader_idx = 0;
  // 1. A, B, C
  // 2. B block_net
  // 3. A, C, D
  // 4. A, D. delete C
  // 5. unblock_net B, create new C
  // 6. may be split-brain {B, C} {A, D}
  {
    PalfHandleImplGuard leader;
    const int64_t CONFIG_CHANGE_TIMEOUT = 10 * 1000 * 1000L; // 10s
    EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, &loc_cb, leader_idx, leader));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 200, id));
    PALF_LOG(INFO, "after submit_log");
    const int64_t follower_B_idx = (leader_idx + 1);
    const int64_t follower_C_idx = (leader_idx + 2);
    const int64_t follower_D_idx = (leader_idx + 3);
    // step 2
    EXPECT_EQ(OB_SUCCESS, get_cluster()[follower_B_idx]->simple_close(false));
    // step 3
    const ObAddr follower_b_addr = get_cluster()[follower_B_idx]->get_addr();
    PALF_LOG(INFO, "before remove member");
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->remove_member(ObMember(follower_b_addr, 1), 3, CONFIG_CHANGE_TIMEOUT));
    PALF_LOG(INFO, "after remove member");

    PalfHandleImplGuard new_leader;
    int64_t new_leader_idx;
    EXPECT_EQ(OB_SUCCESS, get_leader(id, new_leader, new_leader_idx));
    loc_cb.leader_ = get_cluster()[new_leader_idx]->get_addr();
    PALF_LOG(INFO, "set leader for loc_cb", "leader", loc_cb.leader_);

    LogConfigVersion config_version;
    ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_member(ObMember(get_cluster()[follower_D_idx]->get_addr(), 1), 3, config_version, CONFIG_CHANGE_TIMEOUT));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));
    // step 4
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->remove_member(ObMember(get_cluster()[follower_C_idx]->get_addr(), 1), 3, CONFIG_CHANGE_TIMEOUT));
    EXPECT_EQ(OB_SUCCESS, get_cluster()[follower_C_idx]->get_palf_env()->remove_palf_handle_impl(id));
    // step 5
    const int64_t node_id = follower_B_idx * 2 + get_node_idx_base();
    EXPECT_EQ(OB_SUCCESS, get_cluster()[follower_B_idx]->simple_init(get_test_name(), follower_b_addr, node_id, tio_manager_, NULL, false));
    EXPECT_EQ(OB_SUCCESS, get_cluster()[follower_B_idx]->simple_start(false));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));
    PalfBaseInfo palf_base_info;
    IPalfHandleImpl* follower_C_handle = NULL;
    palf_base_info.generate_by_default();
    //PalfHandleImplGuard leader;
    palf_base_info.prev_log_info_.scn_ = share::SCN::min_scn();
    EXPECT_EQ(OB_SUCCESS, get_cluster()[follower_C_idx]->get_palf_env()->create_palf_handle_impl(
        id, palf::AccessMode::APPEND, palf_base_info, palf::LogReplicaType::NORMAL_REPLICA, follower_C_handle));
    get_cluster()[follower_C_idx]->get_palf_env()->revert_palf_handle_impl(follower_C_handle);
    sleep(5);
    // check if split-brain happens
    int64_t leader_cnt = 0;
    EXPECT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id, palf_list));
    for (auto palf: palf_list) {
      ObRole role;
      palf::ObReplicaState unused_state;
      palf->palf_handle_impl_->state_mgr_.get_role_and_state(role, unused_state);
      if (role == LEADER) {
        leader_cnt += 1;
      }
    }
    EXPECT_EQ(leader_cnt, 1);
    revert_cluster_palf_handle_guard(palf_list);
  }
  delete_paxos_group(id);
  PALF_LOG(INFO, "end test split_brain", K(id));
}

TEST_F(TestObSimpleLogClusterConfigChange, test_config_change_defensive)
{
  SET_CASE_LOG_FILE(TEST_NAME, "config_change_defensive");
  int ret = OB_SUCCESS;
  PALF_LOG(INFO, "begin test_config_change_defensive");
  const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  {
    int64_t leader_idx = 0;
    PalfHandleImplGuard leader;
    std::vector<PalfHandleImplGuard*> palf_list;
    const int64_t CONFIG_CHANGE_TIMEOUT = 10 * 1000 * 1000L; // 10s
    common::ObMember dummy_member;
    const LogConfigChangeArgs args(dummy_member, 1, STARTWORKING);
	EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, &loc_cb, leader_idx, leader));
    EXPECT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id, palf_list));
    const int64_t lag_follower_idx = (leader_idx+1)%3;
    const int64_t remove_follower_idx = (leader_idx+2)%3;
    block_net(leader_idx, lag_follower_idx);
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));
    sleep(1);
    // remove member who is in majority
    EXPECT_EQ(OB_TIMEOUT, leader.palf_handle_impl_->remove_member(ObMember(palf_list[remove_follower_idx]->palf_handle_impl_->self_, 1), 2, CONFIG_CHANGE_TIMEOUT / 2));
    unblock_net(leader_idx, lag_follower_idx);
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->remove_member(ObMember(palf_list[remove_follower_idx]->palf_handle_impl_->self_, 1), 2, CONFIG_CHANGE_TIMEOUT));

    int64_t new_leader_idx;
    PalfHandleImplGuard new_leader;
    EXPECT_EQ(OB_SUCCESS, get_leader(id, new_leader, new_leader_idx));
    loc_cb.leader_ = get_cluster()[new_leader_idx]->get_addr();
    PALF_LOG(INFO, "set leader for loc_cb", "leader", get_cluster()[new_leader_idx]->get_addr());

    LogConfigVersion config_version;
    ASSERT_EQ(OB_SUCCESS, new_leader.palf_handle_impl_->get_config_version(config_version));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_member(ObMember(palf_list[remove_follower_idx]->palf_handle_impl_->self_, 1), 3, config_version, CONFIG_CHANGE_TIMEOUT));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 10, id));
    // majority of members should be normal
    palf_list[lag_follower_idx]->palf_handle_impl_->disable_vote(false);
    EXPECT_EQ(OB_TIMEOUT, leader.palf_handle_impl_->remove_member(ObMember(palf_list[remove_follower_idx]->palf_handle_impl_->self_, 1), 2, CONFIG_CHANGE_TIMEOUT / 2));
    palf_list[lag_follower_idx]->palf_handle_impl_->enable_vote();

    block_net(leader_idx, lag_follower_idx);

    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));
    sleep(3);

    // add member without any logs
    // we will send config log to 3 before add_member(3, 4), so 3 can know who is leader and fetch log from it.
    // so just comment this line, after leader forbit fetch log req that comes from node who is not follower or
    // children of leader, we will uncomment this line
    // EXPECT_EQ(OB_TIMEOUT, leader.add_member(ObMember(palf_list[3]->palf_handle_impl_->self_, 1), 4, CONFIG_CHANGE_TIMEOUT / 2));
    unblock_net(leader_idx, lag_follower_idx);
    ASSERT_EQ(OB_SUCCESS, new_leader.palf_handle_impl_->get_config_version(config_version));
    EXPECT_EQ(OB_SUCCESS, new_leader.palf_handle_impl_->add_member(ObMember(palf_list[3]->palf_handle_impl_->self_, 1), 4, config_version, CONFIG_CHANGE_TIMEOUT));
    //test add_member_with_check
    EXPECT_EQ(OB_SUCCESS, new_leader.palf_handle_impl_->remove_member(ObMember(palf_list[3]->palf_handle_impl_->self_, 1), 3, CONFIG_CHANGE_TIMEOUT));
    LogConfigVersion invalid_config_version;
    int64_t follower_idx = (new_leader_idx +1) % 3;
    EXPECT_EQ(OB_STATE_NOT_MATCH, new_leader.palf_handle_impl_->add_member(ObMember(palf_list[3]->palf_handle_impl_->self_, 1), 4, invalid_config_version, CONFIG_CHANGE_TIMEOUT));
    EXPECT_EQ(OB_SUCCESS, new_leader.palf_handle_impl_->get_config_version(config_version));
    EXPECT_EQ(OB_NOT_MASTER, palf_list[follower_idx]->palf_handle_impl_->add_member(ObMember(palf_list[3]->palf_handle_impl_->self_, 1), 4, config_version, CONFIG_CHANGE_TIMEOUT));
    EXPECT_EQ(true, config_version.is_valid());
    EXPECT_EQ(OB_SUCCESS, new_leader.palf_handle_impl_->add_member(ObMember(palf_list[3]->palf_handle_impl_->self_, 1), 4, config_version, CONFIG_CHANGE_TIMEOUT));

    revert_cluster_palf_handle_guard(palf_list);
  }
  delete_paxos_group(id);
  PALF_LOG(INFO, "end test_config_change_defensive", K(id));
}

TEST_F(TestObSimpleLogClusterConfigChange, test_change_replica_num)
{
  SET_CASE_LOG_FILE(TEST_NAME, "change_replica_num");
  int ret = OB_SUCCESS;
	const int64_t id = ATOMIC_FAA(&palf_id_, 1);
  PALF_LOG(INFO, "begin test_change_replica_num", K(id));
  {
	  int64_t leader_idx = 0;
    PalfHandleImplGuard leader;
    const int64_t CONFIG_CHANGE_TIMEOUT = 10 * 1000 * 1000L; // 10s
    EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, &loc_cb, leader_idx, leader));
    PalfHandleImplGuard new_leader;
    int64_t new_leader_idx;
    EXPECT_EQ(OB_SUCCESS, get_leader(id, new_leader, new_leader_idx));
    loc_cb.leader_ = get_cluster()[new_leader_idx]->get_addr();
    PALF_LOG(INFO, "set leader for loc_cb", "leader", get_cluster()[new_leader_idx]->get_addr());

    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));
    // 3->5
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->change_replica_num(get_member_list(), 3, 5, CONFIG_CHANGE_TIMEOUT));
    LogConfigVersion config_version;
    ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_member(ObMember(get_cluster()[4]->get_addr(), 1), 5, config_version, CONFIG_CHANGE_TIMEOUT));
    ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_member(ObMember(get_cluster()[5]->get_addr(), 1), 5, config_version, CONFIG_CHANGE_TIMEOUT));
    common::ObMemberList curr_member_list;
    int64_t curr_replica_num;
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->remove_member(ObMember(get_cluster()[4]->get_addr(), 1), 5, CONFIG_CHANGE_TIMEOUT));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->remove_member(ObMember(get_cluster()[5]->get_addr(), 1), 5, CONFIG_CHANGE_TIMEOUT));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_paxos_member_list(curr_member_list, curr_replica_num));
    // 5->3
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->change_replica_num(curr_member_list, curr_replica_num, 3, CONFIG_CHANGE_TIMEOUT));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 200, id));
    // 3->4
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_paxos_member_list(curr_member_list, curr_replica_num));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->change_replica_num(curr_member_list, curr_replica_num, 4, CONFIG_CHANGE_TIMEOUT));
  }
  delete_paxos_group(id);
  PALF_LOG(INFO, "end test_change_replica_num", K(id));
}

TEST_F(TestObSimpleLogClusterConfigChange, test_basic_config_change)
{
  SET_CASE_LOG_FILE(TEST_NAME, "config_change");
  int ret = OB_SUCCESS;
	const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  PALF_LOG(INFO, "begin test config change", K(id));
  {
	  int64_t leader_idx = 0;
    std::vector<PalfHandleImplGuard*> palf_list;
    const int64_t CONFIG_CHANGE_TIMEOUT = 10 * 1000 * 1000L; // 10s
    PalfHandleImplGuard leader;
    EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, &loc_cb, leader_idx, leader));
    PalfHandleImplGuard new_leader;
    int64_t new_leader_idx;
    EXPECT_EQ(OB_SUCCESS, get_leader(id, new_leader, new_leader_idx));
    loc_cb.leader_ = get_cluster()[new_leader_idx]->get_addr();
    PALF_LOG(INFO, "set leader for loc_cb", "leader", get_cluster()[new_leader_idx]->get_addr());
    EXPECT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id, palf_list));
    // add member when no log
    LogConfigVersion config_version;
    ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_member(ObMember(palf_list[3]->palf_handle_impl_->self_, 1), 4, config_version, CONFIG_CHANGE_TIMEOUT));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->remove_member(ObMember(palf_list[3]->palf_handle_impl_->self_, 1), 3, CONFIG_CHANGE_TIMEOUT));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->remove_member(ObMember(palf_list[(leader_idx+1)%3]->palf_handle_impl_->self_, 1), 2, CONFIG_CHANGE_TIMEOUT));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->remove_member(ObMember(palf_list[(leader_idx+2)%3]->palf_handle_impl_->self_, 1), 1, CONFIG_CHANGE_TIMEOUT));
    ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
    EXPECT_EQ(OB_INVALID_ARGUMENT, leader.palf_handle_impl_->add_member(ObMember(palf_list[(leader_idx+1)%3]->palf_handle_impl_->self_, 1), 4, config_version, CONFIG_CHANGE_TIMEOUT));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_member(ObMember(palf_list[(leader_idx+1)%3]->palf_handle_impl_->self_, 1), 2, config_version, CONFIG_CHANGE_TIMEOUT));
    ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_member(ObMember(palf_list[(leader_idx+2)%3]->palf_handle_impl_->self_, 1), 3, config_version, CONFIG_CHANGE_TIMEOUT));
    ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_member(ObMember(palf_list[3]->palf_handle_impl_->self_, 1), 4, config_version, CONFIG_CHANGE_TIMEOUT));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));

    // add member when contains log
    ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_member(ObMember(palf_list[4]->palf_handle_impl_->self_, 1), 5, config_version, CONFIG_CHANGE_TIMEOUT));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));

    // remove member
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->remove_member(ObMember(palf_list[4]->palf_handle_impl_->self_, 1), 4, CONFIG_CHANGE_TIMEOUT));

    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));


    EXPECT_EQ(OB_STATE_NOT_MATCH, leader.palf_handle_impl_->replace_member(ObMember(palf_list[5]->palf_handle_impl_->self_, 1),
                                                                           ObMember(palf_list[3]->palf_handle_impl_->self_, 1),
                                                                           config_version,
                                                                           CONFIG_CHANGE_TIMEOUT));
    ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
    // replace member
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->replace_member(ObMember(palf_list[5]->palf_handle_impl_->self_, 1),
                                                                   ObMember(palf_list[3]->palf_handle_impl_->self_, 1),
                                                                   config_version,
                                                                   CONFIG_CHANGE_TIMEOUT));

    ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
    // switch acceptor to learner
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->switch_acceptor_to_learner(ObMember(palf_list[5]->palf_handle_impl_->self_, 1), 3, CONFIG_CHANGE_TIMEOUT));
    // add learner
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_learner(ObMember(palf_list[3]->palf_handle_impl_->self_, 1), CONFIG_CHANGE_TIMEOUT));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_learner(ObMember(palf_list[4]->palf_handle_impl_->self_, 1), CONFIG_CHANGE_TIMEOUT));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_learner(ObMember(palf_list[6]->palf_handle_impl_->self_, 1), CONFIG_CHANGE_TIMEOUT));

    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));
    revert_cluster_palf_handle_guard(palf_list);
  }
  delete_paxos_group(id);
  PALF_LOG(INFO, "end test config change", K(id));
}

TEST_F(TestObSimpleLogClusterConfigChange, test_basic_config_change_for_migration)
{
  SET_CASE_LOG_FILE(TEST_NAME, "config_change_for_migration");
  int ret = OB_SUCCESS;
	const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  PALF_LOG(INFO, "begin test config change", K(id));
  {
	  int64_t leader_idx = 0;
    std::vector<PalfHandleImplGuard*> palf_list;
    const int64_t CONFIG_CHANGE_TIMEOUT = 10 * 1000 * 1000L; // 10s
    PalfHandleImplGuard leader;
    EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, &loc_cb, leader_idx, leader));
    PalfHandleImplGuard new_leader;
    int64_t new_leader_idx;
    EXPECT_EQ(OB_SUCCESS, get_leader(id, new_leader, new_leader_idx));
    loc_cb.leader_ = get_cluster()[new_leader_idx]->get_addr();
    PALF_LOG(INFO, "set leader for loc_cb", "leader", get_cluster()[new_leader_idx]->get_addr());
    EXPECT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id, palf_list));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));
    const common::ObAddr &addr2 = get_cluster()[(leader_idx+2)%3]->get_addr();
    const common::ObAddr &addr3 = get_cluster()[3]->get_addr();
    const common::ObAddr &addr4 = get_cluster()[4]->get_addr();
    const common::ObAddr &addr5 = get_cluster()[5]->get_addr();
    // 1. replicate an FULL replica
    {
      PALF_LOG(INFO, "CASE1: replicate an FULL replica", K(id));
      common::ObMember added_member = ObMember(addr3, 1);
      added_member.set_migrating();
      LogLearnerList learners;
      learners.add_learner(LogLearner(added_member.get_server(), 1));
      EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_learner(added_member, CONFIG_CHANGE_TIMEOUT));
      EXPECT_TRUE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.learnerlist_.contains(added_member));
      EXPECT_EQ(3, leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.log_sync_replica_num_);
      EXPECT_UNTIL_EQ(true, check_children_valid(palf_list, learners));

      // clean
      EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->remove_learner(added_member, CONFIG_CHANGE_TIMEOUT));
      EXPECT_FALSE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.learnerlist_.contains(added_member));
      // add again
      EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_learner(added_member, CONFIG_CHANGE_TIMEOUT));
      EXPECT_TRUE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.learnerlist_.contains(added_member));

      LogConfigVersion config_version;
      ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
      EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->switch_learner_to_acceptor(added_member, 4, config_version, CONFIG_CHANGE_TIMEOUT));
      EXPECT_FALSE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.learnerlist_.contains(added_member));
      // member with flag do not exist
      EXPECT_FALSE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.log_sync_memberlist_.contains(added_member));
      EXPECT_TRUE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.log_sync_memberlist_.contains(added_member.get_server()));
      EXPECT_EQ(4, leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.log_sync_replica_num_);
      // reentrant, do not get config_version again
      EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->switch_learner_to_acceptor(added_member, 4, config_version, CONFIG_CHANGE_TIMEOUT));
      // reset environment
      added_member.reset_migrating();
      EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->remove_member(added_member, 3, CONFIG_CHANGE_TIMEOUT));
    }

    // 2. migrate an FULL replica (addr2 -> addr3)
    {
      PALF_LOG(INFO, "CASE2: migrate an FULL replica", K(id));
      common::ObMember added_member = ObMember(addr3, 1);
      added_member.set_migrating();
      common::ObMember replaced_member = ObMember(addr2, 1);
      EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_learner(added_member, CONFIG_CHANGE_TIMEOUT));
      EXPECT_TRUE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.learnerlist_.contains(added_member));
      EXPECT_EQ(3, leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.log_sync_replica_num_);
      LogConfigVersion config_version;
      ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
      LogConfigChangeArgs args(added_member, 0, config_version, SWITCH_LEARNER_TO_ACCEPTOR_AND_NUM);
      EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->one_stage_config_change_(args, CONFIG_CHANGE_TIMEOUT));
      EXPECT_FALSE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.learnerlist_.contains(added_member));
      EXPECT_FALSE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.log_sync_memberlist_.contains(added_member));
      EXPECT_TRUE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.log_sync_memberlist_.contains(added_member.get_server()));
      EXPECT_EQ(4, leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.log_sync_replica_num_);

      ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
      EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->replace_member_with_learner(added_member, replaced_member, config_version, CONFIG_CHANGE_TIMEOUT));
      EXPECT_FALSE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.learnerlist_.contains(added_member));
      // member with flag do not exist
      EXPECT_FALSE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.log_sync_memberlist_.contains(added_member));
      EXPECT_TRUE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.log_sync_memberlist_.contains(added_member.get_server()));
      EXPECT_EQ(3, leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.log_sync_replica_num_);
      EXPECT_FALSE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.log_sync_memberlist_.contains(replaced_member.get_server()));
      // reentrant and check
      EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->replace_member_with_learner(added_member, replaced_member, config_version, CONFIG_CHANGE_TIMEOUT));
      EXPECT_FALSE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.learnerlist_.contains(added_member));
      // member with flag do not exist
      EXPECT_FALSE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.log_sync_memberlist_.contains(added_member));
      EXPECT_TRUE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.log_sync_memberlist_.contains(added_member.get_server()));
      EXPECT_EQ(3, leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.log_sync_replica_num_);
      EXPECT_FALSE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.log_sync_memberlist_.contains(replaced_member.get_server()));
      // reset environment
      added_member.reset_migrating();
      ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
      EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->replace_member(replaced_member, added_member, config_version, CONFIG_CHANGE_TIMEOUT));
      EXPECT_FALSE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.log_sync_memberlist_.contains(added_member.get_server()));
      EXPECT_EQ(3, leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.log_sync_replica_num_);
      EXPECT_TRUE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.log_sync_memberlist_.contains(replaced_member.get_server()));
    }

    // 3. replicate an READONLY replica
    {
      PALF_LOG(INFO, "CASE3: replicate an READONLY replica", K(id));
      // learner's addr must be different from members'
      common::ObMember migrating_member = ObMember(addr2, 1);
      EXPECT_EQ(OB_INVALID_ARGUMENT, leader.palf_handle_impl_->add_learner(migrating_member, CONFIG_CHANGE_TIMEOUT));

      common::ObMember added_migrating_learner = ObMember(addr3, 1);
      common::ObMember added_learner = ObMember(addr3, 1);
      added_migrating_learner.set_migrating();
      EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_learner(added_migrating_learner, CONFIG_CHANGE_TIMEOUT));
      EXPECT_TRUE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.learnerlist_.contains(added_migrating_learner));
      EXPECT_EQ(3, leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.log_sync_replica_num_);
      ObMemberList added_learners, removed_learners;
      EXPECT_EQ(OB_SUCCESS, added_learners.add_member(added_learner));
      EXPECT_EQ(OB_SUCCESS, removed_learners.add_member(added_migrating_learner));
      EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->replace_learners(added_learners, removed_learners, CONFIG_CHANGE_TIMEOUT));
      EXPECT_TRUE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.learnerlist_.contains(added_learner));
      EXPECT_FALSE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.learnerlist_.contains(added_migrating_learner));
      // reentrant and check
      EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->replace_learners(added_learners, removed_learners, CONFIG_CHANGE_TIMEOUT));
      EXPECT_TRUE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.learnerlist_.contains(added_learner));
      EXPECT_FALSE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.learnerlist_.contains(added_migrating_learner));
      // reset environment
      EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->remove_learner(added_learner, CONFIG_CHANGE_TIMEOUT));
    }

    // 4. migrate an READONLY replica, addr4 -> addr3
    {
      PALF_LOG(INFO, "CASE4: migrate an READONLY replica", K(id));
      common::ObMember removed_learner = ObMember(addr4, 1);
      EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_learner(removed_learner, CONFIG_CHANGE_TIMEOUT));
      EXPECT_TRUE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.learnerlist_.contains(removed_learner));

      common::ObMember added_migrating_learner = ObMember(addr3, 1);
      common::ObMember added_learner = ObMember(addr3, 1);
      added_migrating_learner.set_migrating();
      EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_learner(added_migrating_learner, CONFIG_CHANGE_TIMEOUT));
      EXPECT_TRUE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.learnerlist_.contains(added_migrating_learner));
      EXPECT_EQ(3, leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.log_sync_replica_num_);
      ObMemberList added_learners, removed_learners;
      EXPECT_EQ(OB_SUCCESS, added_learners.add_member(added_learner));
      EXPECT_EQ(OB_SUCCESS, removed_learners.add_member(added_migrating_learner));
      EXPECT_EQ(OB_SUCCESS, removed_learners.add_member(removed_learner));
      EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->replace_learners(added_learners, removed_learners, CONFIG_CHANGE_TIMEOUT));
      EXPECT_TRUE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.learnerlist_.contains(added_learner));
      EXPECT_FALSE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.learnerlist_.contains(added_migrating_learner));
      EXPECT_FALSE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.learnerlist_.contains(removed_learner));
      // reentrant and check
      EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->replace_learners(added_learners, removed_learners, CONFIG_CHANGE_TIMEOUT));
      EXPECT_TRUE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.learnerlist_.contains(added_learner));
      EXPECT_FALSE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.learnerlist_.contains(added_migrating_learner));
      EXPECT_FALSE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.learnerlist_.contains(removed_learner));
      // reset environment
      EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->remove_learner(added_learner, CONFIG_CHANGE_TIMEOUT));
    }
    // 5. replace_learners (addr3, addr4) -> (addr3, addr5)
    {
      PALF_LOG(INFO, "CASE5: replace_learners", K(id));
      const common::ObMember member2 = ObMember(addr2, 1);
      const common::ObMember member3 = ObMember(addr3, 1);
      const common::ObMember member4 = ObMember(addr4, 1);
      const common::ObMember member5 = ObMember(addr5, 1);
      EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_learner(member3, CONFIG_CHANGE_TIMEOUT));
      EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_learner(member4, CONFIG_CHANGE_TIMEOUT));
      EXPECT_TRUE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.learnerlist_.contains(member3));
      EXPECT_TRUE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.learnerlist_.contains(member4));

      ObMemberList added_learners, removed_learners;
      EXPECT_EQ(OB_SUCCESS, added_learners.add_member(member3));
      EXPECT_EQ(OB_SUCCESS, added_learners.add_member(member5));
      EXPECT_EQ(OB_SUCCESS, removed_learners.add_member(member2));
      EXPECT_EQ(OB_SUCCESS, removed_learners.add_member(member4));
      EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->replace_learners(added_learners, removed_learners, CONFIG_CHANGE_TIMEOUT));
      EXPECT_TRUE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.learnerlist_.contains(member3));
      EXPECT_TRUE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.learnerlist_.contains(member5));
      EXPECT_FALSE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.learnerlist_.contains(member2));
      EXPECT_FALSE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.learnerlist_.contains(member4));
      // reentrant and check
      EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->replace_learners(added_learners, removed_learners, CONFIG_CHANGE_TIMEOUT));
      EXPECT_TRUE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.learnerlist_.contains(member3));
      EXPECT_TRUE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.learnerlist_.contains(member5));
      EXPECT_FALSE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.learnerlist_.contains(member2));
      EXPECT_FALSE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.learnerlist_.contains(member4));
      // reset environment
      EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->remove_learner(member3, CONFIG_CHANGE_TIMEOUT));
      EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->remove_learner(member5, CONFIG_CHANGE_TIMEOUT));
      EXPECT_FALSE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.learnerlist_.contains(member3));
      EXPECT_FALSE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.learnerlist_.contains(member5));
    }
    // 6. defensive
    {
      PALF_LOG(INFO, "CASE6: defensive", K(id));
      const common::ObMember member2 = ObMember(addr2, 1);
      const common::ObMember member3 = ObMember(addr3, 1);
      const common::ObMember member4 = ObMember(addr4, 1);
      const common::ObMember member5 = ObMember(addr5, 1);
      common::ObMember migrating_member3 = member3;
      migrating_member3.set_migrating();

      EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_learner(migrating_member3, CONFIG_CHANGE_TIMEOUT));
      EXPECT_EQ(OB_INVALID_ARGUMENT, leader.palf_handle_impl_->add_learner(member3, CONFIG_CHANGE_TIMEOUT));
      LogConfigVersion config_version;
      ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
      EXPECT_EQ(OB_INVALID_ARGUMENT, leader.palf_handle_impl_->add_member(member3, 4, config_version, CONFIG_CHANGE_TIMEOUT));
      EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->remove_learner(member3, CONFIG_CHANGE_TIMEOUT));
      EXPECT_TRUE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.learnerlist_.contains(migrating_member3));
      EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->remove_learner(migrating_member3, CONFIG_CHANGE_TIMEOUT));
      EXPECT_FALSE(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.learnerlist_.contains(migrating_member3));

      EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_learner(migrating_member3, CONFIG_CHANGE_TIMEOUT));
      ObMemberList added_learners, removed_learners;
      EXPECT_EQ(OB_SUCCESS, added_learners.add_member(member3));
      EXPECT_EQ(OB_SUCCESS, removed_learners.add_member(member4));
      EXPECT_EQ(OB_INVALID_ARGUMENT, leader.palf_handle_impl_->replace_learners(added_learners, removed_learners, CONFIG_CHANGE_TIMEOUT));

      common::ObMember migrating_member2 = member2;
      migrating_member2.set_migrating();
      EXPECT_EQ(OB_INVALID_ARGUMENT, leader.palf_handle_impl_->remove_member(migrating_member2, 2, CONFIG_CHANGE_TIMEOUT));
    }
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));
    revert_cluster_palf_handle_guard(palf_list);
  }
  delete_paxos_group(id);
  PALF_LOG(INFO, "end test config change", K(id));
}

TEST_F(TestObSimpleLogClusterConfigChange, test_replace_member)
{
  SET_CASE_LOG_FILE(TEST_NAME, "replace_member");
  int ret = OB_SUCCESS;
	const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  PALF_LOG(INFO, "begin test replace_member", K(id));
  {
    std::vector<PalfHandleImplGuard*> palf_list;
	  int64_t leader_idx = 0;
    PalfHandleImplGuard leader;
    const int64_t CONFIG_CHANGE_TIMEOUT = 10 * 1000 * 1000L; // 10s
	  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, &loc_cb, leader_idx, leader));
    PalfHandleImplGuard new_leader;
    int64_t new_leader_idx;
    EXPECT_EQ(OB_SUCCESS, get_leader(id, new_leader, new_leader_idx));
    loc_cb.leader_ = get_cluster()[new_leader_idx]->get_addr();
    PALF_LOG(INFO, "set leader for loc_cb", "leader", get_cluster()[new_leader_idx]->get_addr());
    EXPECT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id, palf_list));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));
    // replace member when no log
    LogConfigVersion config_version;
    ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_member(ObMember(palf_list[3]->palf_handle_impl_->self_, 1), 4, config_version, CONFIG_CHANGE_TIMEOUT));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->remove_member(ObMember(palf_list[3]->palf_handle_impl_->self_, 1), 3, CONFIG_CHANGE_TIMEOUT));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->remove_member(ObMember(palf_list[(leader_idx+1)%3]->palf_handle_impl_->self_, 1), 2, CONFIG_CHANGE_TIMEOUT));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->remove_member(ObMember(palf_list[(leader_idx+2)%3]->palf_handle_impl_->self_, 1), 1, CONFIG_CHANGE_TIMEOUT));
    ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
    EXPECT_EQ(OB_INVALID_ARGUMENT, leader.palf_handle_impl_->add_member(ObMember(palf_list[(leader_idx+1)%3]->palf_handle_impl_->self_, 1), 4, config_version, CONFIG_CHANGE_TIMEOUT));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_member(ObMember(palf_list[(leader_idx+1)%3]->palf_handle_impl_->self_, 1), 2, config_version, CONFIG_CHANGE_TIMEOUT));
    ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_member(ObMember(palf_list[(leader_idx+2)%3]->palf_handle_impl_->self_, 1), 3, config_version, CONFIG_CHANGE_TIMEOUT));
    ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_member(ObMember(palf_list[3]->palf_handle_impl_->self_, 1), 4, config_version, CONFIG_CHANGE_TIMEOUT));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));

    // add member when contains log
    ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_member(ObMember(palf_list[4]->palf_handle_impl_->self_, 1), 5, config_version, CONFIG_CHANGE_TIMEOUT));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));

    // remove member
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->remove_member(ObMember(palf_list[4]->palf_handle_impl_->self_, 1), 4, CONFIG_CHANGE_TIMEOUT));

    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));

    // replace member
    ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->replace_member(ObMember(palf_list[5]->palf_handle_impl_->self_, 1),
                                               ObMember(palf_list[3]->palf_handle_impl_->self_, 1),
                                               config_version,
                                               CONFIG_CHANGE_TIMEOUT));

    // switch acceptor to learner
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->switch_acceptor_to_learner(ObMember(palf_list[5]->palf_handle_impl_->self_, 1), 3, CONFIG_CHANGE_TIMEOUT));
    // add learner
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_learner(ObMember(palf_list[3]->palf_handle_impl_->self_, 1), CONFIG_CHANGE_TIMEOUT));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_learner(ObMember(palf_list[4]->palf_handle_impl_->self_, 1), CONFIG_CHANGE_TIMEOUT));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_learner(ObMember(palf_list[6]->palf_handle_impl_->self_, 1), CONFIG_CHANGE_TIMEOUT));

    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));
    revert_cluster_palf_handle_guard(palf_list);
  }
  delete_paxos_group(id);
  PALF_LOG(INFO, "end test replace_member", K(id));
}

TEST_F(TestObSimpleLogClusterConfigChange, learner)
{
  SET_CASE_LOG_FILE(TEST_NAME, "learner");
  int ret = OB_SUCCESS;
	const int64_t id = ATOMIC_AAF(&palf_id_, 1);
	int64_t leader_idx = 0;
  int64_t log_ts = 1;
  PalfHandleImplGuard leader;
  LogLearnerList all_learner;
  std::vector<PalfHandleImplGuard*> palf_list;
  std::vector<ObRegion> region_list;
  common::ObRegion default_region(DEFAULT_REGION_NAME);
  region_list.push_back(ObRegion("BEIJING"));
  region_list.push_back(ObRegion("SHANGHAI"));
  region_list.push_back(ObRegion("TIANJIN"));
  region_list.push_back(ObRegion("SHENZHEN"));
  region_list.push_back(ObRegion("GUANGZHOU"));
  const ObMemberList &node_list = get_node_list();
  const int64_t CONFIG_CHANGE_TIMEOUT = 10 * 1000 * 1000L; // 10s
	EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, &loc_cb, leader_idx, leader));
  EXPECT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id, palf_list));
  loc_cb.leader_ = get_cluster()[leader_idx]->get_addr();
  PALF_LOG(INFO, "set leader for loc_cb", "leader", get_cluster()[leader_idx]->get_addr());

  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));
  // case 1: set region and switch_acceptor_to_learner
  // add_learner
  for (int64_t i = 3; i < ObSimpleLogClusterTestBase::node_cnt_; ++i) {
    PalfHandleImplGuard tmp_handle;
    common::ObMember added_learner;
    EXPECT_EQ(OB_SUCCESS, node_list.get_member_by_index(i, added_learner));
    LogLearner learner(added_learner.get_server(), 1);
    EXPECT_EQ(OB_SUCCESS, all_learner.add_learner(learner));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_learner(added_learner, CONFIG_CHANGE_TIMEOUT));
  }
  // check children_cnt
  EXPECT_UNTIL_EQ(true, check_children_valid(palf_list, all_learner));
  // change region of one follower
  bool has_change_region = false;
  int64_t diff_region_follower_idx = -1;
  int64_t another_follower_idx = -1;
  for (int i = 0; i < ObSimpleLogClusterTestBase::member_cnt_; i++) {
    const common::ObAddr addr = palf_list[i]->palf_handle_impl_->self_;
    const bool not_leader = (addr != leader.palf_handle_impl_->self_);
    if (!has_change_region && not_leader) {
      get_cluster()[0]->get_locality_manager()->set_server_region(addr, region_list[0]);
      has_change_region = true;
      diff_region_follower_idx = i;
    } else {
      if (not_leader) {
        another_follower_idx = i;
      }
      get_cluster()[0]->get_locality_manager()->set_server_region(addr, default_region);
    }
  }
  for (auto palf_handle: palf_list) { palf_handle->palf_handle_impl_->update_self_region_(); }
  // check children_cnt again
  EXPECT_UNTIL_EQ(true, check_children_valid(palf_list, all_learner));
  // after setting region of a follower, parents of all learners should be another follower
  EXPECT_GE(another_follower_idx, 0);
  EXPECT_LE(another_follower_idx, 2);
  ObAddr curr_parent = palf_list[another_follower_idx]->palf_handle_impl_->self_;
  EXPECT_UNTIL_EQ(true, check_parent(palf_list, all_learner, curr_parent));
  // continue submitting log
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 20, id));
  PALF_LOG(INFO, "all_learner", K(all_learner));
  // EXPECT_EQ(OB_SUCCESS, check_log_sync(palf_list, get_member_list(), all_learner, leader));

  // switch current unique parent to learner
  EXPECT_EQ(OB_SUCCESS, all_learner.add_learner(LogLearner(curr_parent, 1)));
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->switch_acceptor_to_learner(ObMember(curr_parent, 1), 2, CONFIG_CHANGE_TIMEOUT));
  // after switch follower 1 to learner, a learner will be registered to leader, and other learners will
  // be registerd to this learner
  EXPECT_UNTIL_EQ(true, check_children_valid(palf_list, all_learner));
  // check learner topology
  ObAddr leaderschild;
  PalfHandleImplGuard leaderschild_handle;
  LogLearnerList expect_children;
  EXPECT_EQ(1, leader.palf_handle_impl_->config_mgr_.children_.get_member_number());
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->config_mgr_.children_.get_server_by_index(0, leaderschild));
  EXPECT_EQ(OB_SUCCESS, get_palf_handle_guard(palf_list, leaderschild, leaderschild_handle));
  expect_children = all_learner;
  EXPECT_EQ(OB_SUCCESS, expect_children.remove_learner(leaderschild));
  EXPECT_TRUE(expect_children.learner_addr_equal(leaderschild_handle.palf_handle_impl_->config_mgr_.children_));
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 20, id));
  // EXPECT_EQ(OB_SUCCESS, check_log_sync(palf_list, get_member_list(), all_learner, leader));
  // learners' regions are different from paxos member, so parent of all learners is leader
  // set regions
  for (int64_t i = 3; i < ObSimpleLogClusterTestBase::node_cnt_; ++i) {
    PalfHandleImplGuard tmp_handle;
    common::ObMember learner;
    EXPECT_EQ(OB_SUCCESS, node_list.get_member_by_index(i, learner));
    get_cluster()[0]->get_locality_manager()->set_server_region(learner.get_server(), region_list[i-2]);
  }
  for (auto palf_handle: palf_list) { palf_handle->palf_handle_impl_->update_self_region_(); }
  sleep(1);
  // check children_cnt
  EXPECT_UNTIL_EQ(true, check_children_valid(palf_list, all_learner));
  EXPECT_UNTIL_EQ(true, check_parent(palf_list, all_learner, leader.palf_handle_impl_->self_));

  // switch leader, after switching leader, the parent of all learners is the new leader
  const int64_t new_leader_idx = diff_region_follower_idx;
  PalfHandleImplGuard new_leader;
  EXPECT_EQ(OB_SUCCESS, switch_leader(id, 0, new_leader));

  EXPECT_UNTIL_EQ(true, check_children_valid(palf_list, all_learner));
  EXPECT_UNTIL_EQ(true, check_parent(palf_list, all_learner, new_leader.palf_handle_impl_->self_));

  revert_cluster_palf_handle_guard(palf_list);
  PALF_LOG(INFO, "end test learner", K(id));
}

TEST_F(TestObSimpleLogClusterConfigChange, test_config_change_lock)
{
  SET_CASE_LOG_FILE(TEST_NAME, "config_change_lock");
  int ret = OB_SUCCESS;
	const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  PALF_LOG(INFO, "begin test config change", K(id));
	int64_t leader_idx = 0;
  int64_t log_ts = 1;
  PalfHandleImplGuard leader;
  std::vector<PalfHandleImplGuard*> palf_list;
  const int64_t CONFIG_CHANGE_TIMEOUT = 10 * 1000 * 1000L; // 10s
	ASSERT_EQ(OB_SUCCESS, create_paxos_group(id, &loc_cb, leader_idx, leader));
  ASSERT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id, palf_list));

  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 100, id));
  loc_cb.leader_ = get_cluster()[leader_idx]->get_addr();
  PALF_LOG(INFO, "set leader for loc_cb", "leader", loc_cb.leader_);
  int64_t lock_owner_out = -1;
  bool lock_stat = false;
  //ASSERT_EQ(OB_NOT_SUPPORTED, leader.palf_handle_impl_->get_config_change_lock_stat(lock_owner_out, lock_stat));
  //ASSERT_EQ(OB_NOT_SUPPORTED, leader.palf_handle_impl_->try_lock_config_change(1, CONFIG_CHANGE_TIMEOUT));
  //ASSERT_EQ(OB_NOT_SUPPORTED, leader.palf_handle_impl_->unlock_config_change(1, CONFIG_CHANGE_TIMEOUT));
  //oceanbase::common::ObClusterVersion::get_instance().cluster_version_ = CLUSTER_VERSION_4_2_0_0;
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_change_lock_stat(lock_owner_out, lock_stat));
  ASSERT_EQ(OB_INVALID_CONFIG_CHANGE_LOCK_OWNER, lock_owner_out);
  ASSERT_EQ(false, lock_stat);

  //invalid arguments
  ASSERT_EQ(OB_INVALID_ARGUMENT, leader.palf_handle_impl_->try_lock_config_change(-1, CONFIG_CHANGE_TIMEOUT));
  ASSERT_EQ(OB_INVALID_ARGUMENT, leader.palf_handle_impl_->try_lock_config_change(1, 0));
  ASSERT_EQ(OB_INVALID_ARGUMENT, leader.palf_handle_impl_->unlock_config_change(-1, CONFIG_CHANGE_TIMEOUT));
  ASSERT_EQ(OB_INVALID_ARGUMENT, leader.palf_handle_impl_->unlock_config_change(1, 0));

  //test try lock
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->try_lock_config_change(1, CONFIG_CHANGE_TIMEOUT));
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->try_lock_config_change(1, CONFIG_CHANGE_TIMEOUT));
  ASSERT_EQ(OB_TRY_LOCK_CONFIG_CHANGE_CONFLICT, leader.palf_handle_impl_->try_lock_config_change(2, CONFIG_CHANGE_TIMEOUT));

  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 100, id));

  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_change_lock_stat(lock_owner_out, lock_stat));
  ASSERT_EQ(1, lock_owner_out);
  ASSERT_EQ(true, lock_stat);

  //test unlock
  ASSERT_EQ(OB_STATE_NOT_MATCH, leader.palf_handle_impl_->unlock_config_change(2, CONFIG_CHANGE_TIMEOUT));
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->unlock_config_change(1, CONFIG_CHANGE_TIMEOUT));
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->unlock_config_change(1, CONFIG_CHANGE_TIMEOUT));
  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 100, id));

  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_change_lock_stat(lock_owner_out, lock_stat));
  ASSERT_EQ(1, lock_owner_out);
  ASSERT_EQ(false, lock_stat);

  //changing stat, test locking stat
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->try_lock_config_change(3, CONFIG_CHANGE_TIMEOUT));
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_change_lock_stat(lock_owner_out, lock_stat));
  ASSERT_EQ(3, lock_owner_out);
  ASSERT_EQ(true, lock_stat);

  //block add_member

  LogConfigVersion config_version;
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));

  //ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_learner(ObMember(palf_list[3]->palf_handle_impl_->self_, 1), CONFIG_CHANGE_TIMEOUT));
  ASSERT_EQ(OB_EAGAIN, leader.palf_handle_impl_->add_member(ObMember(palf_list[3]->palf_handle_impl_->self_, 1), 4, config_version, CONFIG_CHANGE_TIMEOUT));
  ASSERT_EQ(OB_EAGAIN, leader.palf_handle_impl_->remove_member(ObMember(palf_list[3]->palf_handle_impl_->self_, 1), 3, CONFIG_CHANGE_TIMEOUT));
  ASSERT_EQ(OB_EAGAIN, leader.palf_handle_impl_->remove_member(ObMember(palf_list[2]->palf_handle_impl_->self_, 1), 3, CONFIG_CHANGE_TIMEOUT));
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
  ASSERT_EQ(OB_EAGAIN, leader.palf_handle_impl_->add_member(ObMember(palf_list[2]->palf_handle_impl_->self_, 1), 3, config_version, CONFIG_CHANGE_TIMEOUT));
  ASSERT_EQ(OB_EAGAIN, leader.palf_handle_impl_->switch_acceptor_to_learner(ObMember(palf_list[2]->palf_handle_impl_->self_, 1), 2, CONFIG_CHANGE_TIMEOUT));
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
  ASSERT_EQ(OB_EAGAIN, leader.palf_handle_impl_->switch_learner_to_acceptor(ObMember(palf_list[2]->palf_handle_impl_->self_, 1), 3, config_version, CONFIG_CHANGE_TIMEOUT));
  ASSERT_EQ(OB_EAGAIN, leader.palf_handle_impl_->change_replica_num(get_member_list(), 3, 4, CONFIG_CHANGE_TIMEOUT));


  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->unlock_config_change(3, CONFIG_CHANGE_TIMEOUT));
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_change_lock_stat(lock_owner_out, lock_stat));
  ASSERT_EQ(3, lock_owner_out);
  ASSERT_EQ(false, lock_stat);

  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_member(ObMember(palf_list[3]->palf_handle_impl_->self_, 1), 4, config_version, CONFIG_CHANGE_TIMEOUT));
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->remove_member(ObMember(palf_list[3]->palf_handle_impl_->self_, 1), 3, CONFIG_CHANGE_TIMEOUT));
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->remove_member(ObMember(palf_list[2]->palf_handle_impl_->self_, 1), 3, CONFIG_CHANGE_TIMEOUT));
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_member(ObMember(palf_list[2]->palf_handle_impl_->self_, 1), 3, config_version, CONFIG_CHANGE_TIMEOUT));
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->switch_acceptor_to_learner(ObMember(palf_list[2]->palf_handle_impl_->self_, 1), 2, CONFIG_CHANGE_TIMEOUT));
  ASSERT_EQ(OB_STATE_NOT_MATCH, leader.palf_handle_impl_->switch_learner_to_acceptor(ObMember(palf_list[2]->palf_handle_impl_->self_, 1), 3, config_version, CONFIG_CHANGE_TIMEOUT));
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->switch_learner_to_acceptor(ObMember(palf_list[2]->palf_handle_impl_->self_, 1), 3, config_version, CONFIG_CHANGE_TIMEOUT));
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->change_replica_num(get_member_list(), 3, 4, CONFIG_CHANGE_TIMEOUT));
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->change_replica_num(get_member_list(), 4, 3, CONFIG_CHANGE_TIMEOUT));

  //switch leader

  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 100, id));
  revert_cluster_palf_handle_guard(palf_list);
  PALF_LOG(INFO, "end test config change lock", K(id));
}

TEST_F(TestObSimpleLogClusterConfigChange, test_switch_leader)
{
  SET_CASE_LOG_FILE(TEST_NAME, "switch_leader");
  int ret = OB_SUCCESS;
	const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  PALF_LOG(INFO, "begin test switch_leader", K(id));
	int64_t leader_idx = 0;
  PalfHandleImplGuard leader;
  std::vector<PalfHandleImplGuard*> palf_list;
  oceanbase::common::ObClusterVersion::get_instance().cluster_version_ = CLUSTER_VERSION_4_2_0_0;
  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
  ASSERT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id, palf_list));
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 200, id));
  // try lock config change
  //
  const int64_t follower_idx = (leader_idx+1)%3;
  const int64_t CONFIG_CHANGE_TIMEOUT = 10 * 1000 * 1000L; // 10s

  int64_t lock_owner_out = -1;
  bool lock_stat = false;
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->try_lock_config_change(1, CONFIG_CHANGE_TIMEOUT));
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_change_lock_stat(lock_owner_out, lock_stat));
  ASSERT_EQ(1, lock_owner_out);
  ASSERT_EQ(true, lock_stat);

  PalfHandleImplGuard new_leader;
  const int64_t new_leader_idx = (leader_idx+1)%3;
  PalfHandleImplGuard &follower = *palf_list[new_leader_idx];
  ASSERT_EQ(OB_NOT_MASTER, follower.palf_handle_impl_->get_config_change_lock_stat(lock_owner_out, lock_stat));
  EXPECT_EQ(OB_SUCCESS, switch_leader(id, new_leader_idx, new_leader));
  sleep(5);
  ObRole role;
  int64_t curr_proposal_id = 0;
  bool is_pending_stat = false;
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_role(role, curr_proposal_id, is_pending_stat));
  EXPECT_EQ(ObRole::FOLLOWER, role);

  lock_owner_out = -1;
  lock_stat = false;

  ASSERT_EQ(OB_SUCCESS, new_leader.palf_handle_impl_->get_config_change_lock_stat(lock_owner_out, lock_stat));
  ASSERT_EQ(1, lock_owner_out);
  ASSERT_EQ(true, lock_stat);
  EXPECT_EQ(OB_SUCCESS, submit_log(new_leader, 200, id));


  ASSERT_EQ(OB_SUCCESS, new_leader.palf_handle_impl_->unlock_config_change(1, CONFIG_CHANGE_TIMEOUT));
  sleep(3);
  //switch leader during lock_config_change

  LogConfigChangeArgs args(2, ConfigChangeLockType::LOCK_PAXOS_MEMBER_CHANGE, TRY_LOCK_CONFIG_CHANGE);
  LogConfigVersion config_version;
  const int64_t proposal_id = new_leader.palf_handle_impl_->state_mgr_.get_proposal_id();
  const int64_t leader_epoch = new_leader.palf_handle_impl_->state_mgr_.get_leader_epoch();
  ASSERT_EQ(OB_EAGAIN, new_leader.palf_handle_impl_->config_mgr_.change_config(args, proposal_id, leader_epoch, config_version));
  ASSERT_EQ(LogConfigMgr::ConfigChangeState::CHANGING, new_leader.palf_handle_impl_->config_mgr_.state_);
  ASSERT_EQ(OB_EAGAIN, new_leader.palf_handle_impl_->get_config_change_lock_stat(lock_owner_out, lock_stat));
  //block_net
  const int64_t follower_idx1 = (new_leader_idx+1)%3;
  const int64_t follower_idx2 = (new_leader_idx+2)%3;
  block_net(follower_idx1, new_leader_idx, true);
  block_net(follower_idx2, new_leader_idx, true);
  //send info to follower
  ASSERT_EQ(OB_EAGAIN, new_leader.palf_handle_impl_->config_mgr_.change_config(args, proposal_id, leader_epoch, config_version));
  sleep(1);

  unblock_net(follower_idx1, new_leader_idx);
  unblock_net(follower_idx2, new_leader_idx);
  PalfHandleImplGuard old_leader;
  EXPECT_EQ(OB_SUCCESS, switch_leader(id, leader_idx, old_leader));

  EXPECT_EQ(OB_SUCCESS, old_leader.palf_handle_impl_->get_role(role, curr_proposal_id, is_pending_stat));
  EXPECT_EQ(ObRole::LEADER, role);

  //check config change stat
  sleep(1);
  ASSERT_EQ(OB_SUCCESS, old_leader.palf_handle_impl_->get_config_change_lock_stat(lock_owner_out, lock_stat));
  ASSERT_EQ(2, lock_owner_out);
  ASSERT_EQ(true, lock_stat);


  revert_cluster_palf_handle_guard(palf_list);
  PALF_LOG(INFO, "end test switch_leader", K(id));
}

// 1. 3F(beijing), 4R(shanghai)
// 2. the client submits logs to F replicas, but R replicas can not receive logs
// 3. switch a R to F, the R replica must be one of the children of another R.
//    Due to step 2, the R will not receive the reconfiguration log
// 4. enable the remaining R re-register parents
// 5. check loop between R replicas
TEST_F(TestObSimpleLogClusterConfigChange, learner_loop)
{
  SET_CASE_LOG_FILE(TEST_NAME, "learner_loop");
  int ret = OB_SUCCESS;
  const int64_t CONFIG_CHANGE_TIMEOUT = 10 * 1000 * 1000L; // 10s
	const int64_t id = ATOMIC_AAF(&palf_id_, 1);
	int64_t leader_idx = 0;
  PalfHandleImplGuard leader;
  LogLearnerList all_learner;
  const ObMemberList &node_list = get_node_list();
  std::vector<PalfHandleImplGuard*> palf_list;
  common::ObRegion beijing_region("BEIJING");
  common::ObRegion shanghai_region("SHANGHAI");

	EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, &loc_cb, leader_idx, leader));
  EXPECT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id, palf_list));
  loc_cb.leader_ = get_cluster()[leader_idx]->get_addr();

  // 1. init
  for (int64_t i = 3; i < ObSimpleLogClusterTestBase::node_cnt_; ++i) {
    common::ObMember added_learner;
    EXPECT_EQ(OB_SUCCESS, node_list.get_member_by_index(i, added_learner));
    LogLearner learner(added_learner.get_server(), 1);
    EXPECT_EQ(OB_SUCCESS, all_learner.add_learner(learner));
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_learner(added_learner, CONFIG_CHANGE_TIMEOUT));
  }

  // set region
  for (int i = 0; i < ObSimpleLogClusterTestBase::node_cnt_; i++) {
    const common::ObAddr addr = palf_list[i]->palf_handle_impl_->self_;
    if (leader.palf_handle_impl_->config_mgr_.alive_paxos_memberlist_.contains(addr)) {
      get_cluster()[0]->get_locality_manager()->set_server_region(addr, beijing_region);
    } else {
      get_cluster()[0]->get_locality_manager()->set_server_region(addr, shanghai_region);
    }
  }
  for (auto palf_handle: palf_list) { palf_handle->palf_handle_impl_->update_self_region_(); }
  // check topo
  EXPECT_UNTIL_EQ(true, check_children_valid(palf_list, all_learner));
  EXPECT_UNTIL_EQ(1, leader.palf_handle_impl_->config_mgr_.children_.get_member_number());
  EXPECT_UNTIL_EQ(0, palf_list[1]->palf_handle_impl_->config_mgr_.children_.get_member_number());
  EXPECT_UNTIL_EQ(0, palf_list[2]->palf_handle_impl_->config_mgr_.children_.get_member_number());
  ObAddr same_parent, any_child;
  int64_t any_child_idx = -1;
  for (int i = 0; i < ObSimpleLogClusterTestBase::node_cnt_; i++)
  {
    const common::ObAddr addr = palf_list[i]->palf_handle_impl_->self_;
    const ObAddr &this_parent = palf_list[i]->palf_handle_impl_->config_mgr_.parent_;
    if (all_learner.contains(addr) && leader.palf_handle_impl_->self_ == this_parent) {
      same_parent = addr;
      PALF_LOG(INFO, "SAME_PARENT", K(id), K(addr), K(same_parent));
      break;
    }
  }
  EXPECT_TRUE(same_parent.is_valid());
  for (int i = 0; i < ObSimpleLogClusterTestBase::node_cnt_; i++)
  {
    const common::ObAddr addr = palf_list[i]->palf_handle_impl_->self_;
    if (all_learner.contains(addr) && addr != same_parent) {
      EXPECT_UNTIL_EQ(same_parent, palf_list[i]->palf_handle_impl_->config_mgr_.parent_);
      any_child = addr;
      any_child_idx = i;
      PALF_LOG(INFO, "CHECK_PARENT", K(id), K(addr), K(same_parent));
    }
  }
  // 2. replicating logs to all F replicas
  EXPECT_NE(-1, any_child_idx);
  EXPECT_UNTIL_EQ(leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.config_version_,
      palf_list[any_child_idx]->palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.config_version_);
  EXPECT_UNTIL_EQ(leader.palf_handle_impl_->get_max_lsn().val_, leader.palf_handle_impl_->get_end_lsn().val_);
  for (int i = 0; i < ObSimpleLogClusterTestBase::node_cnt_; i++) {
    const common::ObAddr &addr = palf_list[i]->palf_handle_impl_->self_;
    if (true == all_learner.contains(addr)) {
      block_pcode(i, ObRpcPacketCode::OB_LOG_PUSH_REQ);
    }
  }
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));
  EXPECT_UNTIL_EQ(leader.palf_handle_impl_->get_max_lsn().val_, leader.palf_handle_impl_->get_end_lsn().val_);

  // 3. switch a R replica to F
  LogConfigVersion config_version;
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->switch_learner_to_acceptor(ObMember(any_child, -1), 4, config_version, CONFIG_CHANGE_TIMEOUT));

  // 4. enable the remaining R re-register parents
  leader.palf_handle_impl_->config_mgr_.children_.reset();
  for (auto palf_handle: palf_list) {
    const common::ObAddr addr = palf_handle->palf_handle_impl_->self_;
    if (true == all_learner.contains(addr) && addr != any_child) {
      palf_handle->palf_handle_impl_->config_mgr_.retire_parent_(LogConfigMgr::RetireParentReason::SELF_REGION_CHANGED);
      palf_handle->palf_handle_impl_->config_mgr_.register_parent_(LogConfigMgr::RegisterParentReason::FIRST_REGISTER);
    }
  }

  // 5. check loop
  for (int i = 0; i < ObSimpleLogClusterTestBase::node_cnt_; i++) {
    const common::ObAddr &addr = palf_list[i]->palf_handle_impl_->self_;
    if (true == all_learner.contains(addr)) {
      block_pcode(i, ObRpcPacketCode::OB_LOG_PUSH_REQ);
    }
  }
  sleep(2);

  EXPECT_EQ(OB_SUCCESS, all_learner.remove_learner(any_child));
  for (auto palf_handle: palf_list) {
    const common::ObAddr addr = palf_handle->palf_handle_impl_->self_;
    if (true == all_learner.contains(addr)) {
      EXPECT_UNTIL_EQ(true, palf_handle->palf_handle_impl_->config_mgr_.parent_.is_valid());
      EXPECT_UNTIL_EQ(false, palf_handle->palf_handle_impl_->config_mgr_.children_.contains(palf_handle->palf_handle_impl_->config_mgr_.parent_));
    }
  }

  for (int i = 0; i < ObSimpleLogClusterTestBase::node_cnt_; i++) {
    unblock_pcode(i, ObRpcPacketCode::OB_LOG_PUSH_REQ);
  }
  revert_cluster_palf_handle_guard(palf_list);
  PALF_LOG(INFO, "end test learner_loop", K(id));
}

// 1. 3 paxos member (A, B ,C), A is the leader. D is a learner, C is the parent of D.
// 2. block_net A <-> C, submit logs, logs of C and D are behind from A
// 3. remove C, or the step 4 cannot be executed succcessfully
// 4. switch D from learner to acceptor, D can not accept the reconfig log because it miss some logs
// 5. remove B
TEST_F(TestObSimpleLogClusterConfigChange, switch_lagged_learner_to_acceptor)
{
  SET_CASE_LOG_FILE(TEST_NAME, "switch_lagged_learner_to_acceptor");
  int ret = OB_SUCCESS;
  const int64_t CONFIG_CHANGE_TIMEOUT = 10 * 1000 * 1000L; // 10s
	const int64_t id = ATOMIC_AAF(&palf_id_, 1);
	int64_t leader_idx = 0;
  PalfHandleImplGuard leader;
  LogLearnerList all_learner;
  const ObMemberList &node_list = get_node_list();
  std::vector<PalfHandleImplGuard*> palf_list;
  common::ObRegion beijing_region("BEIJING");
  common::ObRegion shanghai_region("SHANGHAI");

	EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, &loc_cb, leader_idx, leader));
  EXPECT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id, palf_list));

  // 1. add a learner, idx = 3
  const int64_t learner_idx = 3;
  const int64_t followerc_idx = (leader_idx + 1) % 3;
  const int64_t followerb_idx = (leader_idx + 2) % 3;
  const ObAddr followerc_addr = get_cluster()[followerc_idx]->get_addr();
  const ObAddr followerb_addr = get_cluster()[followerb_idx]->get_addr();

  // set region, beijing(leader, follower b), shanghai(follower c, learner)
  for (int i = 0; i < ObSimpleLogClusterTestBase::node_cnt_; i++) {
    const common::ObAddr addr = palf_list[i]->palf_handle_impl_->self_;
    if (leader.palf_handle_impl_->config_mgr_.alive_paxos_memberlist_.contains(addr) &&
        i != followerc_idx) {
      get_cluster()[0]->get_locality_manager()->set_server_region(addr, beijing_region);
    } else {
      get_cluster()[0]->get_locality_manager()->set_server_region(addr, shanghai_region);
    }
  }
  for (auto palf_handle: palf_list) { palf_handle->palf_handle_impl_->update_self_region_(); }
  // add D to learner_list
  common::ObMember learner;
  EXPECT_EQ(OB_SUCCESS, node_list.get_member_by_index(learner_idx, learner));
  EXPECT_EQ(OB_SUCCESS, all_learner.add_learner(LogLearner(learner.get_server(), 1)));
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_learner(learner, CONFIG_CHANGE_TIMEOUT));

  // check topo
  EXPECT_UNTIL_EQ(true, check_children_valid(palf_list, all_learner));
  EXPECT_UNTIL_EQ(0, leader.palf_handle_impl_->config_mgr_.children_.get_member_number());
  EXPECT_UNTIL_EQ(0, palf_list[followerb_idx]->palf_handle_impl_->config_mgr_.children_.get_member_number());
  EXPECT_UNTIL_EQ(1, palf_list[followerc_idx]->palf_handle_impl_->config_mgr_.children_.get_member_number());
  EXPECT_UNTIL_EQ(followerc_addr, palf_list[learner_idx]->palf_handle_impl_->config_mgr_.parent_);
  loc_cb.leader_ = get_cluster()[leader_idx]->get_addr();

  // 2. remove C, or the step 4 cannot be executed succcessfully
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->remove_member(ObMember(followerc_addr, 1), 2, CONFIG_CHANGE_TIMEOUT));
  LogConfigVersion leader_config_version;
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(leader_config_version));
  // ensure the learner can be added successfully
  EXPECT_EQ(leader_config_version, palf_list[learner_idx]->palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.config_version_);

  // 3. block_net between leader and followerc, submit logs
  block_net(leader_idx, followerc_idx);
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));
  EXPECT_UNTIL_EQ(leader.palf_handle_impl_->get_max_lsn().val_, leader.palf_handle_impl_->get_end_lsn().val_);

  // 4. switch D from learner to acceptor, D can not accept the reconfig log because it miss some logs
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(leader_config_version));
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->switch_learner_to_acceptor(learner, 3, leader_config_version, CONFIG_CHANGE_TIMEOUT));

  // 5. remove B
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->remove_member(ObMember(followerb_addr, 1), 2, CONFIG_CHANGE_TIMEOUT));

  unblock_net(leader_idx, followerc_idx);
  leader.reset();
  revert_cluster_palf_handle_guard(palf_list);
  PALF_LOG(INFO, "end switch_lagged_learner_to_acceptor", K(id));
}


TEST_F(TestObSimpleLogClusterConfigChange, requester_restart_rejects_delayed_rpc_and_preserves_other_copy)
{
  SET_CASE_LOG_FILE(TEST_NAME, "retention_gc_requester_restart");
  init_retention_gc();
  ASSERT_FALSE(HasFatalFailure());
  ObLSGCRequest old;
  start_request(3, old);
  handshake(old);
  hold(old);
  ASSERT_FALSE(HasFatalFailure());
  ASSERT_EQ(OB_SUCCESS, restart_server(3));
  {
    // Restore the leader's committed boundary before sampling the recovered range.
    std::vector<PalfHandleImplGuard*> replicas;
    ASSERT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id_, replicas));
    DEFER(revert_cluster_palf_handle_guard(replicas));
    sync_committed_info(3, *replicas[3]);
    ASSERT_FALSE(HasFatalFailure());
  }
  ObLSGCRequest recovered;
  start_request(3, recovered);
  handshake(recovered);
  ObLSGCRequest other;
  start_request(4, other);
  handshake(other);
  hold(other);
  refresh();
  approve(recovered);
  ASSERT_FALSE(HasFatalFailure());
  ObTenantEnv::set_tenant(server(3)->get_tenant_base());
  ASSERT_EQ(OB_SUCCESS, server(3)->remove_ls(id_));
  PalfHandleGuard removed;
  ASSERT_EQ(OB_LS_NOT_EXIST, server(3)->log_service_.open_palf(ObLSID(id_), removed));
  for (int64_t i = 0; i < 8; ++i) {
    reject(old);
    ASSERT_FALSE(gc_leader(leader_idx_).is_gc_hold_(old.src_));
  }
  hold(other);
  ASSERT_TRUE(gc_leader(leader_idx_).is_gc_hold_(other.src_));
  ASSERT_TRUE(gc_leader(leader_idx_).is_gc_approved_(recovered.src_));
  ObTenantEnv::set_tenant(get_cluster()[leader_idx_]->get_tenant_base());
  LogGetFetchSourcesResp sources;
  ASSERT_EQ(OB_SUCCESS, gc_leader(leader_idx_).get_log_fetch_sources(sources));
  for (int64_t i = 0; i < sources.sources_.count(); ++i) {
    ASSERT_NE(recovered.src_, sources.sources_.at(i).server_);
  }
}

TEST_F(TestObSimpleLogClusterConfigChange, leader_aba_and_palf_restart_fence_old_rpc_tokens)
{
  SET_CASE_LOG_FILE(TEST_NAME, "retention_gc_leader_aba");
  init_retention_gc();
  ASSERT_FALSE(HasFatalFailure());
  const int64_t original_leader = leader_idx_;
  ObLSGCRequest first;
  start_request(3, first);
  handshake(first);
  const ObLSGCRequest first_formal = first;
  hold(first);
  ObLSGCRequest other;
  start_request(4, other);
  handshake(other);
  hold(other);
  refresh();
  approve(first);
  ASSERT_FALSE(HasFatalFailure());
  leader_.reset();
  leader_idx_ = (original_leader + 1) % 3;
  ASSERT_EQ(OB_SUCCESS, switch_leader(id_, leader_idx_, leader_));
  ObLSGCResponse response;
  ASSERT_EQ(OB_NOT_MASTER, send(first, response, original_leader, 5 * 1000 * 1000L));
  reject(first);
  ASSERT_TRUE(gc_leader(leader_idx_).retained_replica_caches_.empty());
  leader_.reset();
  leader_idx_ = original_leader;
  ASSERT_EQ(OB_SUCCESS, switch_leader(id_, leader_idx_, leader_));
  reject(first);
  ASSERT_TRUE(gc_leader(leader_idx_).gc_requests_.empty());
  // Restart PALF from files and reconstruct the GC handler as a fresh process would.
  leader_.reset();
  ASSERT_EQ(OB_SUCCESS, restart_server(original_leader));
  ObGCHandler *handler = ls_[original_leader]->get_gc_handler();
  handler->ObGCHandler::~ObGCHandler();
  new (handler) ObGCHandler();
  ASSERT_EQ(OB_SUCCESS, handler->init(ls_[original_leader]));
  ASSERT_EQ(OB_SUCCESS, switch_leader(id_, original_leader, leader_));
  ObLSGCRequest recovered;
  start_request(3, recovered);
  handshake(recovered);
  ASSERT_FALSE(HasFatalFailure());
  ASSERT_EQ(first_formal.get_gc_token(), recovered.get_gc_token());
  ASSERT_NE(first_formal.get_config_version(), recovered.get_config_version());
  reject(first_formal);
  hold(recovered);
  ASSERT_TRUE(gc_leader(leader_idx_).is_gc_hold_(recovered.src_));
}

TEST_F(TestObSimpleLogClusterConfigChange, timed_out_rpc_waiting_for_lock_cannot_change_gc_state)
{
  SET_CASE_LOG_FILE(TEST_NAME, "retention_gc_rpc_timeout");
  init_retention_gc();
  ASSERT_FALSE(HasFatalFailure());
  ObLSGCRequest old;
  start_request(3, old);
  handshake(old);
  ASSERT_FALSE(HasFatalFailure());
  ObLSGCResponse delayed_response;
  int rpc_ret = OB_ERR_UNEXPECTED;
  const int64_t timeout_us = 200 * 1000L;
  auto &state = gc_leader(leader_idx_);
  auto &deliver = server(leader_idx_)->deliver_;
  auto wait_count = [](int64_t &counter, const int64_t expected) {
    const int64_t deadline = ObTimeUtility::current_time() + 5 * 1000 * 1000L;
    while (ATOMIC_LOAD(&counter) < expected && ObTimeUtility::current_time() < deadline) {
      ob_usleep(1000);
    }
    return expected == ATOMIC_LOAD(&counter);
  };
  const int64_t started = ATOMIC_LOAD(&deliver.gc_rpc_started_);
  ASSERT_TRUE(wait_count(deliver.gc_rpc_finished_, started));
  state.lock_.lock();
  const int64_t next_token = state.next_gc_token_;
  std::thread delayed([&]() {
    ObTenantEnv::set_tenant(server(5)->get_tenant_base());
    rpc_ret = send(old, delayed_response, leader_idx_, timeout_us);
    ObTenantEnv::set_tenant(NULL);
  });
  // Observe process() after its entry timeout check while the decision lock is held.
  const bool entered = wait_count(deliver.gc_rpc_started_, started + 1);
  // The receiver's deadline starts at receipt and may outlast the caller's deadline.
  // Hold the lock for a full timeout after observing receiver entry.
  const int64_t expired_at = ObTimeUtility::current_time() + timeout_us;
  while (ObTimeUtility::current_time() <= expired_at) {
    ob_usleep(1000);
  }
  delayed.join();
  state.lock_.unlock();
  ASSERT_TRUE(wait_count(deliver.gc_rpc_finished_, started + 1));
  ASSERT_TRUE(entered);
  ASSERT_EQ(OB_TIMEOUT, rpc_ret);
  ASSERT_FALSE(delayed_response.is_gc_allowed());
  {
    lib::ObMutexGuard guard(state.lock_);
    ASSERT_EQ(1, state.gc_requests_.count());
    ASSERT_EQ(old.src_, state.gc_requests_.at(0).server_);
    ASSERT_EQ(old.get_gc_token(), state.gc_requests_.at(0).token_);
    ASSERT_EQ(ObLSRetentionGCHandler::Leader::GCRequestState::PREPARE,
              state.gc_requests_.at(0).phase_);
    ASSERT_EQ(next_token, state.next_gc_token_);
    ASSERT_TRUE(state.retained_replica_caches_.empty());
  }
  ObLSGCRequest current;
  start_request(3, current);
  handshake(current);
  ObLSGCRequest other;
  start_request(4, other);
  handshake(other);
  hold(other);
  refresh();
  approve(current);
  ASSERT_FALSE(HasFatalFailure());
  reject(old);
  hold(other);
  ASSERT_TRUE(state.is_gc_approved_(current.src_));
  ASSERT_TRUE(state.is_gc_hold_(other.src_));
}


} // end unittest
} // end oceanbase

int main(int argc, char **argv)
{
  RUN_SIMPLE_LOG_CLUSTER_TEST(TEST_NAME);
}
