// Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#include <atomic>
#include <cstdio>
#include <functional>
#include <gtest/gtest.h>
#include <mutex>
#include <signal.h>
#include <string>
#include <thread>
#include <vector>
#define private public
#include "env/ob_simple_log_cluster_env.h"
#undef private
#include "logservice/ob_log_base_header.h"
#include "logservice/palf/log_reader_utils.h"

const std::string TEST_NAME = "arb_service";

using namespace oceanbase::common;
using namespace oceanbase;
namespace oceanbase
{
using namespace logservice;

int64_t ARB_TIMEOUT_ARG = 2 * 1000 * 1000L;
bool fisrt_f_connected_with_rs = true;
bool second_f_connected_with_rs = true;
common::ObAddr first_f_addr;
common::ObAddr second_f_addr;
ArbDegradationPolicy degradation_policy = LS_POLICY;

namespace logservice
{

void ObArbitrationService::update_arb_timeout_()
{
  arb_timeout_us_ = ARB_TIMEOUT_ARG;
  if (REACH_TIME_INTERVAL(2 * 1000 * 1000)) {
    CLOG_LOG_RET(WARN, OB_ERR_UNEXPECTED, "update_arb_timeout_", K_(self), K_(arb_timeout_us));
  }
}

bool ObArbitrationService::is_in_cluster_policy_() const
{
  return CLUSTER_POLICY == degradation_policy;
}

int ObArbitrationService::try_probe_rs_(bool &connected)
{
  if (self_ == first_f_addr) {
    connected = fisrt_f_connected_with_rs;
  } else if (self_ == second_f_addr) {
    connected = second_f_connected_with_rs;
  }
  return OB_SUCCESS;
}

}

namespace unittest
{

class TestObSimpleLogClusterArbService : public ObSimpleLogClusterTestEnv
{
public:
  TestObSimpleLogClusterArbService() :  ObSimpleLogClusterTestEnv()
  {}
};

int64_t ObSimpleLogClusterTestBase::member_cnt_ = 3;
int64_t ObSimpleLogClusterTestBase::node_cnt_ = 5;
bool ObSimpleLogClusterTestBase::need_add_arb_server_ = true;
std::string ObSimpleLogClusterTestBase::test_name_ = TEST_NAME;

// Queue real election messages. The test chooses their delivery order, while
// PALF/configuration RPCs, disk IO and reconfirm still use the real cluster.
class DegradeVoteElectionTransport : public palf::election::ElectionMsgSender
{
public:
  int broadcast(const palf::election::ElectionPrepareRequestMsg &msg,
                const ObIArray<ObAddr> &) const override
  {
    std::lock_guard<std::mutex> guard(lock_);
    prepare_requests_.push_back(msg);
    return OB_SUCCESS;
  }
  int broadcast(const palf::election::ElectionAcceptRequestMsg &msg,
                const ObIArray<ObAddr> &) const override
  {
    std::lock_guard<std::mutex> guard(lock_);
    accept_requests_.push_back(msg);
    return OB_SUCCESS;
  }
  int send(const palf::election::ElectionPrepareResponseMsg &msg) const override
  {
    std::lock_guard<std::mutex> guard(lock_);
    prepare_responses_.push_back(msg);
    return OB_SUCCESS;
  }
  int send(const palf::election::ElectionAcceptResponseMsg &msg) const override
  {
    std::lock_guard<std::mutex> guard(lock_);
    accept_responses_.push_back(msg);
    return OB_SUCCESS;
  }
  int send(const palf::election::ElectionChangeLeaderMsg &) const override
  { return OB_SUCCESS; }

  template <typename T>
  std::vector<T> take(std::vector<T> &queue)
  {
    std::lock_guard<std::mutex> guard(lock_);
    std::vector<T> result;
    result.swap(queue);
    return result;
  }

  mutable std::mutex lock_;
  mutable std::vector<palf::election::ElectionPrepareRequestMsg> prepare_requests_;
  mutable std::vector<palf::election::ElectionAcceptRequestMsg> accept_requests_;
  mutable std::vector<palf::election::ElectionPrepareResponseMsg> prepare_responses_;
  mutable std::vector<palf::election::ElectionAcceptResponseMsg> accept_responses_;
};

// Observe actual rebuild notifications without repairing the divergent history.
class DegradeVoteRebuildObserver : public palf::PalfRebuildCb
{
public:
  int on_rebuild(const int64_t id, const palf::LSN &lsn) override
  {
    last_id_.store(id);
    last_lsn_.store(lsn.val_);
    count_.fetch_add(1);
    return OB_SUCCESS;
  }

  std::atomic<int64_t> count_{0};
  std::atomic<int64_t> last_id_{-1};
  std::atomic<uint64_t> last_lsn_{0};
};

static int defer_degrade_vote_election_tasks(palf::election::ElectionImpl &election)
{
  using namespace palf::election;
  int ret = OB_SUCCESS;
  for (auto *handle : {&election.proposer_.renew_lease_task_handle_,
                       &election.proposer_.devote_task_handle_,
                       &election.acceptor_.time_window_task_handle_}) {
    if (OB_SUCCESS != (ret = handle->reschedule_after(3600_s))) {
      break;
    } else if (handle->task_->future_.is_valid() &&
               OB_SUCCESS != (ret = handle->task_->future_.wait())) {
      break;
    }
  }
  return ret;
}

static int change_degrade_vote_config(PalfHandleImpl &handle,
                                     const LogConfigChangeArgs &args,
                                     const int64_t timeout_us)
{
  // 配置变更可能暂时忙，仅对 OB_EAGAIN 做有界重试。
  const int64_t deadline = ObTimeUtility::current_time() + timeout_us;
  int ret = OB_EAGAIN;
  while (OB_EAGAIN == ret) {
    const int64_t remaining_us = deadline - ObTimeUtility::current_time();
    if (remaining_us <= 0) {
      ret = OB_TIMEOUT;
    } else {
      ret = handle.one_stage_config_change_(args, remaining_us);
      if (OB_EAGAIN == ret) {
        ob_usleep(10 * 1000);
      }
    }
  }
  return ret;
}

static int count_degrade_vote_marker(PalfHandleImpl &handle,
                                    const std::string &marker,
                                    int64_t &count)
{
  count = 0;
  PalfBufferIterator iterator;
  int ret = handle.alloc_palf_buffer_iterator(LSN(PALF_INITIAL_LSN_VAL), iterator);
  while (OB_SUCCESS == ret && OB_SUCCESS == (ret = iterator.next())) {
    LogEntry entry;
    LSN lsn;
    if (OB_SUCCESS != (ret = iterator.get_entry(entry, lsn))) {
      break;
    } else if (entry.get_data_len() == static_cast<int64_t>(marker.size()) &&
               0 == MEMCMP(entry.get_data_buf(), marker.data(), marker.size())) {
      ++count;
    }
  }
  return OB_ITER_END == ret ? OB_SUCCESS : ret;
}

static int make_degrade_vote_marker(const std::string &tag,
                                   const char fill,
                                   std::string &marker)
{
  const ObLogBaseHeader header(TRANS_SERVICE_LOG_BASE_TYPE,
                               ObReplayBarrierType::NO_NEED_BARRIER, 0);
  marker.assign(header.get_serialize_size(), '\0');
  int64_t pos = 0;
  const int ret = header.serialize(&marker[0], marker.size(), pos);
  if (OB_SUCCESS == ret) {
    marker.append(tag).append(4096, fill);
  }
  return ret;
}

TEST_F(TestObSimpleLogClusterArbService, test_2f1a_stale_vote_committed_log_divergence)
{
  using namespace palf::election;
  using palf::election::LockGuard;
  SET_CASE_LOG_FILE(TEST_NAME, "test_2f1a_stale_vote_committed_log_divergence");
  const int64_t saved_arb_timeout = ARB_TIMEOUT_ARG;
  const uint64_t saved_cluster_version = ObClusterVersion::get_instance().cluster_version_;
  ARB_TIMEOUT_ARG = 60 * 1000 * 1000L;
  ObClusterVersion::get_instance().cluster_version_ = CLUSTER_VERSION_4_2_0_0;
  DEFER(ARB_TIMEOUT_ARG = saved_arb_timeout;
        ObClusterVersion::get_instance().cluster_version_ = saved_cluster_version);

  const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  const int64_t wait_timeout = 20 * 1000 * 1000L;
  int64_t a_idx = -1;
  int64_t r_idx = -1;
  PalfHandleImplGuard leader;
  PalfHandleLiteGuard arb;
  std::vector<PalfHandleImplGuard *> handles;
  std::vector<ObSimpleLogServer *> paused_arb_servers;
  // Restore the transports before these objects and the PALF handles go away.
  DegradeVoteElectionTransport a_transport;
  DegradeVoteElectionTransport b_transport;
  DegradeVoteElectionTransport r_transport;
  DEFER(revert_cluster_palf_handle_guard(handles);
        arb.reset(); leader.reset(); delete_paxos_group(id);
        for (auto *server : paused_arb_servers) {
          ObTenantEnv::set_tenant(server->get_tenant_base());
          EXPECT_EQ(OB_SUCCESS, server->log_service_.get_arbitration_service()->start());
        });
  // 建组前暂停 A、B 的后台升降级，避免与用例手动变更竞争。
  for (auto *server : get_cluster()) {
    if (!server->is_arb_server() && get_member_list().contains(server->get_addr())) {
      auto *log_server = dynamic_cast<ObSimpleLogServer *>(server);
      ASSERT_NE(nullptr, log_server);
      auto *arb_service = log_server->log_service_.get_arbitration_service();
      ASSERT_NE(nullptr, arb_service);
      ASSERT_FALSE(arb_service->has_set_stop());
      arb_service->lib::Threads::stop();
      arb_service->lib::Threads::wait();
      // 只释放工作线程资源，保留探活；退出时重新启动。
      arb_service->lib::Threads::destroy();
      paused_arb_servers.push_back(log_server);
    }
  }
  ASSERT_EQ(2, paused_arb_servers.size());
  ASSERT_EQ(OB_SUCCESS, create_paxos_group_with_arb(id, r_idx, a_idx, leader));
  ASSERT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id, handles));
  ASSERT_EQ(OB_SUCCESS, get_arb_member_guard(id, arb));
  ASSERT_TRUE(a_idx == 0 || a_idx == 1);
  const int64_t b_idx = 1 - a_idx;
  PalfHandleImpl &a = *handles[a_idx]->palf_handle_impl_;
  PalfHandleImpl &b = *handles[b_idx]->palf_handle_impl_;
  auto &r = *arb.palf_handle_lite_;
  ElectionImpl &a_election = a.election_;
  ElectionImpl &b_election = b.election_;
  ElectionImpl &r_election = r.election_;
  const ObAddr a_addr = a.self_;
  const ObAddr b_addr = b.self_;
  MockLocCB loc_cb;
  loc_cb.leader_ = a_addr;
  ASSERT_EQ(OB_SUCCESS, a.set_location_cache_cb(&loc_cb));
  ASSERT_EQ(OB_SUCCESS, b.set_location_cache_cb(&loc_cb));
  DEFER(a.reset_location_cache_cb(); b.reset_location_cache_cb());

  // Every wait in the reproduction is bounded. Once B is elected, keep renewing
  // its lease with actual Accept requests/responses while PALF runs reconfirm.
  std::function<int()> renew_b = []() { return OB_SUCCESS; };
  auto wait_for = [&](const std::function<bool()> &condition,
                      const bool keep_b_lease = false) -> int {
    const int64_t deadline = ObTimeUtility::current_time() + wait_timeout;
    while (!condition()) {
      if (ObTimeUtility::current_time() >= deadline) {
        return OB_TIMEOUT;
      }
      if (keep_b_lease) {
        const int ret = renew_b();
        if (OB_SUCCESS != ret) {
          return ret;
        }
      }
      ob_usleep(10 * 1000);
    }
    return OB_SUCCESS;
  };

  // 1. Establish a genuinely committed common prefix on A and B.
  ASSERT_EQ(OB_SUCCESS, submit_log(leader, 10, id, 4096));
  ASSERT_EQ(OB_SUCCESS, wait_for([&]() { return a.get_end_lsn() == a.get_max_lsn(); }));
  const LSN common_end = a.get_end_lsn();
  ASSERT_EQ(OB_SUCCESS, wait_for([&]() {
    return b.get_end_lsn() == common_end && b.get_max_lsn() == common_end;
  }));
  LogConfigVersion old_version;
  ASSERT_EQ(OB_SUCCESS, a.get_config_version(old_version));
  ASSERT_EQ(old_version, b_election.get_membership_version_());
  ASSERT_EQ(OB_SUCCESS, defer_degrade_vote_election_tasks(a_election));
  ASSERT_EQ(OB_SUCCESS, defer_degrade_vote_election_tasks(b_election));
  ASSERT_EQ(OB_SUCCESS, defer_degrade_vote_election_tasks(r_election));
  auto *a_sender = a_election.msg_handler_;
  auto *b_sender = b_election.msg_handler_;
  auto *r_sender = r_election.msg_handler_;
  {
    LockGuard guard(a_election.lock_);
    a_election.msg_handler_ = &a_transport;
  }
  {
    LockGuard guard(b_election.lock_);
    b_election.msg_handler_ = &b_transport;
  }
  {
    LockGuard guard(r_election.lock_);
    r_election.msg_handler_ = &r_transport;
  }
  DEFER(a_election.stop(); b_election.stop(); r_election.stop();
        a_election.msg_handler_ = a_sender;
        b_election.msg_handler_ = b_sender;
        r_election.msg_handler_ = r_sender);

  auto deliver_accept = [&](const ElectionAcceptRequestMsg &request,
                            ElectionImpl &receiver) -> int {
    ElectionAcceptRequestMsg delivered(request);
    delivered.set_receiver(receiver.get_self_addr());
    const int ret = receiver.handle_message(delivered);
    return OB_SUCCESS == ret ? defer_degrade_vote_election_tasks(receiver) : ret;
  };
  // Refresh A's real lease and membership acknowledgements before the fault.
  {
    LockGuard guard(a_election.lock_);
    a_election.proposer_.propose();
  }
  auto renew_requests = a_transport.take(a_transport.accept_requests_);
  ASSERT_FALSE(renew_requests.empty());
  for (const auto &request : renew_requests) {
    ASSERT_EQ(OB_SUCCESS, deliver_accept(request, a_election));
    ASSERT_EQ(OB_SUCCESS, deliver_accept(request, b_election));
    ASSERT_EQ(OB_SUCCESS, deliver_accept(request, r_election));
  }
  for (auto *transport : {&a_transport, &b_transport, &r_transport}) {
    for (const auto &response : transport->take(transport->accept_responses_)) {
      if (response.get_receiver() == a_addr) {
        ASSERT_EQ(OB_SUCCESS, a_election.handle_message(response));
      }
    }
    transport->take(transport->prepare_responses_);
  }
  ASSERT_TRUE(a_election.proposer_.check_leader());
  // Hold only time during setup, then expire it explicitly at step 4. No ballot,
  // configuration, log position, role or committed state is manufactured.
  const int64_t setup_lease_end = get_monotonic_ts() + 60_s;
  {
    LockGuard guard(a_election.lock_);
    int64_t lease_end = 0;
    int64_t epoch = 0;
    a_election.proposer_.leader_lease_and_epoch_.get(lease_end, epoch);
    a_election.proposer_.leader_lease_and_epoch_.
        set_lease_and_epoch_if_lease_expired_or_just_set_lease(setup_lease_end, epoch);
  }
  for (auto *election : {&a_election, &b_election, &r_election}) {
    LockGuard guard(election->lock_);
    ASSERT_EQ(a_addr, election->acceptor_.lease_.get_owner());
    election->acceptor_.lease_.lease_end_ts_ = setup_lease_end;
  }
  auto *b_vote = b_election.acceptor_.time_window_task_handle_.task_->func_shared_ptr_.get_ptr();
  auto *r_vote = r_election.acceptor_.time_window_task_handle_.task_->func_shared_ptr_.get_ptr();
  ASSERT_NE(nullptr, b_vote);
  ASSERT_NE(nullptr, r_vote);
  EXPECT_FALSE((*b_vote)());
  EXPECT_FALSE((*r_vote)());

  // 2. B generates a Prepare in the OLD configuration. R caches it before A
  // degrades B. A's resulting leader Prepare is intentionally not delivered,
  // reproducing its blocked RPC and the prepare_success_ballot mismatch.
  block_net(a_idx, b_idx);
  DEFER(unblock_net(a_idx, b_idx));
  {
    LockGuard guard(b_election.lock_);
    b_election.proposer_.last_do_prepare_ts_ =
        ObClockGenerator::getClock() - CALCULATE_MAX_ELECT_COST_TIME();
    b_election.proposer_.prepare(FOLLOWER);
  }
  auto prepares = b_transport.take(b_transport.prepare_requests_);
  ASSERT_EQ(1, prepares.size());
  ASSERT_EQ(old_version, prepares.front().get_membership_version());
  for (auto *receiver : {&a_election, &b_election, &r_election}) {
    ElectionPrepareRequestMsg request(prepares.front());
    request.set_receiver(receiver->get_self_addr());
    ASSERT_EQ(OB_SUCCESS, receiver->handle_message(request));
    ASSERT_EQ(OB_SUCCESS, defer_degrade_vote_election_tasks(*receiver));
  }
  ASSERT_TRUE(r_election.acceptor_.is_time_window_opened_);
  ASSERT_EQ(b_addr, r_election.acceptor_.highest_priority_prepare_req_.get_sender());
  ASSERT_NE(a_election.proposer_.prepare_success_ballot_, a_election.proposer_.ballot_number_);

  // 3. Persist the actual degradation on A and R, then commit an identifiable
  // tail using A's new one-full-replica quorum. B retains the old short prefix.
  ObTenantEnv::set_tenant(get_cluster()[a_idx]->get_tenant_base());
  const LogConfigChangeArgs degrade_b(ObMember(b_addr, 1), 0, DEGRADE_ACCEPTOR_TO_LEARNER);
  ASSERT_EQ(OB_SUCCESS, change_degrade_vote_config(a, degrade_b, 10 * 1000 * 1000L));
  LogConfigVersion degraded_version;
  ASSERT_EQ(OB_SUCCESS, a.get_config_version(degraded_version));
  ASSERT_TRUE(old_version < degraded_version);
  ASSERT_EQ(1, a.config_mgr_.log_ms_meta_.curr_.config_.log_sync_replica_num_);
  ASSERT_EQ(OB_SUCCESS, wait_for([&]() {
    LockGuard guard(r_election.lock_);
    return r_election.get_membership_version_() == degraded_version;
  }));
  ASSERT_EQ(old_version, b_election.get_membership_version_());
  const auto &r_members = r_election.proposer_.memberlist_with_states_.get_member_list().get_addr_list();
  ASSERT_EQ(2, r_members.count());
  for (const auto &member : r_members) {
    ASSERT_NE(b_addr, member);
  }

  auto append_marker = [&](PalfHandleImpl &handle, const std::string &marker,
                           LSN &lsn, SCN &scn, const bool keep_b_lease) -> int {
    PalfAppendOptions options;
    ObRole role = FOLLOWER;
    bool pending = true;
    int ret = handle.get_role(role, options.proposal_id, pending);
    if (OB_SUCCESS != ret || LEADER != role || pending) {
      return OB_SUCCESS == ret ? OB_NOT_MASTER : ret;
    }
    SCN ref_scn;
    ref_scn.convert_for_logservice(ObTimeUtility::current_time_ns());
    const int64_t deadline = ObTimeUtility::current_time() + wait_timeout;
    do {
      ret = handle.submit_log(options, marker.data(), marker.size(), ref_scn, lsn, scn);
      if (OB_EAGAIN != ret && OB_ERR_OUT_OF_UPPER_BOUND != ret) {
        break;
      } else if (ObTimeUtility::current_time() >= deadline) {
        ret = OB_TIMEOUT;
        break;
      } else if (keep_b_lease && OB_SUCCESS != (ret = renew_b())) {
        break;
      }
      ob_usleep(1000);
    } while (true);
    return ret;
  };
  std::string a_marker;
  ASSERT_EQ(OB_SUCCESS, make_degrade_vote_marker("COMMITTED_ON_A_AFTER_DEGRADE:", 'A', a_marker));
  const int64_t a_tail_count = 8;
  LSN last_a_lsn;
  SCN last_a_scn;
  for (int64_t i = 0; i < a_tail_count; ++i) {
    ASSERT_EQ(OB_SUCCESS, append_marker(a, a_marker, last_a_lsn, last_a_scn, false));
    if (0 == i) {
      // Commit the first entry separately so A's final predecessor is beyond
      // the common prefix even when the remaining entries are grouped together.
      ASSERT_EQ(OB_SUCCESS, wait_for([&]() {
        return a.get_end_lsn() > last_a_lsn + a_marker.size() && a.get_end_lsn() == a.get_max_lsn();
      }));
    }
  }
  ASSERT_EQ(OB_SUCCESS, wait_for([&]() {
    return a.get_end_lsn() > last_a_lsn + a_marker.size() && a.get_end_lsn() == a.get_max_lsn();
  }));
  const LSN a_committed_end = a.get_end_lsn();
  ASSERT_GT(a_committed_end, common_end);
  ASSERT_EQ(common_end, b.get_max_lsn());
  ASSERT_EQ(common_end, b.get_end_lsn());
  int64_t a_marker_count = 0;
  ASSERT_EQ(OB_SUCCESS, count_degrade_vote_marker(a, a_marker, a_marker_count));
  ASSERT_EQ(a_tail_count, a_marker_count);
  PALF_LOG(INFO, "REPRO step 3: A tail is committed, B is still short",
           K(id), K(common_end), K(a_committed_end), K(old_version), K(degraded_version));

  // 4. Expire A's lease and run the production timers. The original bug gives
  // B a real Prepare OK from R despite R's newer configuration. EXPECT keeps
  // going so the test also diagnoses the subsequent committed-log violation.
  {
    LockGuard guard(a_election.lock_);
    int64_t lease_end = 0;
    int64_t epoch = 0;
    a_election.proposer_.leader_lease_and_epoch_.get(lease_end, epoch);
    a_election.proposer_.leader_lease_and_epoch_.
        set_lease_and_epoch_if_lease_expired_or_just_set_lease(get_monotonic_ts() - 1, epoch);
    a_election.acceptor_.lease_.lease_end_ts_ = get_monotonic_ts() - 1;
  }
  auto *a_renew = a_election.proposer_.renew_lease_task_handle_.task_->func_shared_ptr_.get_ptr();
  ASSERT_NE(nullptr, a_renew);
  EXPECT_FALSE((*a_renew)());
  ASSERT_FALSE(a_election.proposer_.check_leader());
  for (auto *election : {&b_election, &r_election}) {
    LockGuard guard(election->lock_);
    election->acceptor_.lease_.lease_end_ts_ = get_monotonic_ts() - 1;
  }
  b_transport.take(b_transport.prepare_responses_);
  r_transport.take(r_transport.prepare_responses_);
  EXPECT_FALSE((*b_vote)());
  EXPECT_FALSE((*r_vote)());
  auto b_votes = b_transport.take(b_transport.prepare_responses_);
  auto r_votes = r_transport.take(r_transport.prepare_responses_);
  int64_t stale_vote_count = 0;
  for (const auto &vote : r_votes) {
    if (vote.get_receiver() == b_addr && vote.is_accepted()) {
      ++stale_vote_count;
    }
  }
  EXPECT_EQ(0, stale_vote_count) << "R voted for B with a stale configuration";
  if (0 == stale_vote_count) {
    // A fixed implementation stops the bad chain here. It must not fabricate
    // a vote or force B to lead merely to execute the remaining reproduction.
    EXPECT_FALSE(b_election.proposer_.check_leader());
    // Positive control formerly covered by the small election unittest: R
    // must still vote for a legitimate candidate in its current configuration.
    a_transport.take(a_transport.prepare_requests_);
    {
      LockGuard guard(a_election.lock_);
      a_election.proposer_.last_do_prepare_ts_ =
          ObClockGenerator::getClock() - CALCULATE_MAX_ELECT_COST_TIME();
      a_election.proposer_.prepare(FOLLOWER);
    }
    auto valid_prepares = a_transport.take(a_transport.prepare_requests_);
    ASSERT_EQ(1, valid_prepares.size());
    ASSERT_EQ(degraded_version, valid_prepares.front().get_membership_version());
    ElectionPrepareRequestMsg valid_request(valid_prepares.front());
    valid_request.set_receiver(r_election.get_self_addr());
    ASSERT_EQ(OB_SUCCESS, r_election.handle_message(valid_request));
    ASSERT_EQ(OB_SUCCESS, defer_degrade_vote_election_tasks(r_election));
    {
      LockGuard guard(r_election.lock_);
      r_election.acceptor_.last_time_window_open_ts_ =
          ObClockGenerator::getClock() - CALCULATE_TIME_WINDOW_SPAN_TS() - 1;
    }
    EXPECT_FALSE((*r_vote)());
    int64_t valid_vote_count = 0;
    for (const auto &vote : r_transport.take(r_transport.prepare_responses_)) {
      if (vote.get_receiver() == a_addr && vote.is_accepted()) {
        ++valid_vote_count;
      }
    }
    EXPECT_EQ(1, valid_vote_count) << "R must still vote for a valid candidate after rejecting stale B";
    return;
  }
  ASSERT_EQ(1, stale_vote_count);
  PALF_LOG(INFO, "REPRO step 4: R voted for B after the membership changed",
           K(id), K(stale_vote_count), K(old_version), K(degraded_version));
  for (const auto &votes : {b_votes, r_votes}) {
    for (const auto &vote : votes) {
      if (vote.get_receiver() == b_addr && vote.is_accepted()) {
        ASSERT_EQ(OB_SUCCESS, b_election.handle_message(vote));
      }
    }
  }
  // Only renew after the genuine Prepare replies have formed a majority.
  // Calling propose() without this check could accidentally force the chain.
  ASSERT_EQ(b_election.proposer_.ballot_number_, b_election.proposer_.prepare_success_ballot_);
  renew_b = [&]() -> int {
    {
      LockGuard guard(b_election.lock_);
      if (b_election.proposer_.prepare_success_ballot_ != b_election.proposer_.ballot_number_) {
        return OB_STATE_NOT_MATCH;
      }
      b_election.proposer_.propose();
    }
    for (const auto &request : b_transport.take(b_transport.accept_requests_)) {
      int ret = deliver_accept(request, b_election);
      if (OB_SUCCESS != ret || OB_SUCCESS != (ret = deliver_accept(request, r_election))) {
        return ret;
      }
    }
    for (auto *transport : {&b_transport, &r_transport}) {
      for (const auto &response : transport->take(transport->accept_responses_)) {
        if (response.get_receiver() == b_addr) {
          const int ret = b_election.handle_message(response);
          if (OB_SUCCESS != ret) {
            return ret;
          }
        }
      }
    }
    return OB_SUCCESS;
  };
  ASSERT_EQ(OB_SUCCESS, renew_b());
  ASSERT_TRUE(b_election.proposer_.check_leader());

  // 5. B + R perform real PALF prepare/reconfirm. A is isolated, so R supplies
  // no full-replica tail and B chooses its own shorter prefix. Degrade A in
  // reconfirm through the actual configuration-change/persistence path.
  loc_cb.leader_ = b_addr;
  block_net(a_idx, r_idx);
  DEFER(unblock_net(a_idx, r_idx));
  ASSERT_EQ(OB_SUCCESS, wait_for([&]() { return b.reconfirm_.can_do_degrade(); }, true));
  // can_do_degrade() becomes true after reconfirm's log-selection/fetch phase.
  // A is isolated and R has no full log, so B must still have only the common
  // prefix here. Check the observable tail instead of renamed private members
  // (majority_max_* in 4.2/4.3.5, prepare_quorum_max_accepted_* in master/4.4).
  const LSN b_reconfirmed_max = b.get_max_lsn();
  ASSERT_EQ(common_end, b_reconfirmed_max);
  ASSERT_LT(b_reconfirmed_max, a_committed_end);
  std::atomic<bool> change_done(false);
  std::atomic<int> change_ret(OB_EAGAIN);
  std::thread change_thread([&]() {
    ObTenantEnv::set_tenant(get_cluster()[b_idx]->get_tenant_base());
    const LogConfigChangeArgs degrade_a(ObMember(a_addr, 1), 0, DEGRADE_ACCEPTOR_TO_LEARNER);
    change_ret.store(change_degrade_vote_config(b, degrade_a, 10 * 1000 * 1000L));
    change_done.store(true);
  });
  const int change_wait_ret = wait_for([&]() { return change_done.load(); }, true);
  change_thread.join();
  ASSERT_EQ(OB_SUCCESS, change_wait_ret);
  ASSERT_EQ(OB_SUCCESS, change_ret.load());
  ASSERT_EQ(OB_SUCCESS, wait_for([&]() {
    ObRole role = FOLLOWER;
    int64_t proposal_id = 0;
    bool pending = true;
    return OB_SUCCESS == b.get_role(role, proposal_id, pending) && LEADER == role && !pending;
  }, true));
  ASSERT_EQ(1, b.config_mgr_.log_ms_meta_.curr_.config_.log_sync_replica_num_);
  PALF_LOG(INFO, "REPRO step 5: B completed reconfirm using its short tail",
           K(id), K(common_end), K(a_committed_end), "B_committed_end", b.get_end_lsn());

  // 6. B commits distinct data across A's already-committed LSN range. A stays
  // isolated so its original committed entries remain available as evidence.
  ObTenantEnv::set_tenant(get_cluster()[b_idx]->get_tenant_base());
  std::string b_marker;
  ASSERT_EQ(OB_SUCCESS, make_degrade_vote_marker("COMMITTED_ON_B_AFTER_RECONFIRM:", 'B', b_marker));
  // Make A's old end fall inside a fully persisted B group, regardless of how
  // A's eight entries were batched. This reproduces the partial group read in
  // get_prev_log_info_for_fetch_, without corrupting or patching either log.
  b_marker.resize(static_cast<size_t>(a_committed_end - common_end) + 4096, 'B');
  ASSERT_LT(b_marker.size(), static_cast<size_t>(MAX_LOG_BODY_SIZE));
  LSN last_b_lsn;
  SCN last_b_scn;
  const int64_t write_deadline = ObTimeUtility::current_time() + wait_timeout;
  do {
    ASSERT_LT(ObTimeUtility::current_time(), write_deadline);
    ASSERT_EQ(OB_SUCCESS, renew_b());
    ASSERT_EQ(OB_SUCCESS, append_marker(b, b_marker, last_b_lsn, last_b_scn, true));
    ASSERT_EQ(OB_SUCCESS, wait_for([&]() {
      return b.get_end_lsn() > last_b_lsn + b_marker.size();
    }, true));
  } while (b.get_end_lsn() <= a_committed_end);
  ASSERT_EQ(OB_SUCCESS, count_degrade_vote_marker(a, a_marker, a_marker_count));
  ASSERT_EQ(a_tail_count, a_marker_count);
  int64_t preserved_on_b = 0;
  int64_t committed_on_b = 0;
  ASSERT_EQ(OB_SUCCESS, count_degrade_vote_marker(b, a_marker, preserved_on_b));
  ASSERT_EQ(OB_SUCCESS, count_degrade_vote_marker(b, b_marker, committed_on_b));
  ASSERT_GT(committed_on_b, 0);
  EXPECT_EQ(a_tail_count, preserved_on_b)
      << "B completed reconfirm and committed past A's committed tail, but lost A's committed entries";
  ReadBufGuard a_bytes("MitTest", 4096);
  ReadBufGuard b_bytes("MitTest", 4096);
  int64_t a_read_size = 0;
  int64_t b_read_size = 0;
  ASSERT_EQ(OB_SUCCESS, a.log_engine_.read_log(common_end, 4096, a_bytes.read_buf_, a_read_size));
  ASSERT_EQ(OB_SUCCESS, b.log_engine_.read_log(common_end, 4096, b_bytes.read_buf_, b_read_size));
  ASSERT_EQ(4096, a_read_size);
  ASSERT_EQ(a_read_size, b_read_size);
  EXPECT_EQ(0, MEMCMP(a_bytes.read_buf_.buf_, b_bytes.read_buf_.buf_, a_read_size))
      << "A and B contain different committed bytes at the same LSN after stale voting and reconfirm";
  PALF_LOG(INFO, "REPRO step 6: compare committed history at the same LSN",
           K(id), K(common_end), K(a_committed_end), "B_committed_end", b.get_end_lsn(),
           K(a_marker_count), K(preserved_on_b), K(committed_on_b));

  // 7. Restore the network and keep sync enabled. Obtain A's fetch positions
  // from its real sliding window, then exercise B's production storage path.
  // The request ends inside B's different group and must fail before any logs
  // are sent. Also observe incoming fetch traffic, so an unreachable leader
  // or a disabled follower cannot masquerade as the incident's stalled fetch.
  DegradeVoteRebuildObserver rebuild_observer;
  PalfRebuildCbNode rebuild_node(&rebuild_observer);
  ASSERT_EQ(OB_SUCCESS, a.register_rebuild_cb(&rebuild_node));
  DEFER(a.unregister_rebuild_cb(&rebuild_node));
  LSN initial_rebuild_lsn;
  ASSERT_EQ(OB_SUCCESS, a.get_last_rebuild_lsn(initial_rebuild_lsn));
  ASSERT_FALSE(initial_rebuild_lsn.is_valid());
  ASSERT_TRUE(a.is_sync_enabled());
  LSN a_prev_lsn;
  LSN a_fetch_start;
  int64_t a_prev_log_id = OB_INVALID_LOG_ID;
  ASSERT_EQ(OB_SUCCESS, wait_for([&]() {
    return a.sw_.is_all_committed_log_slided_out(a_prev_lsn, a_prev_log_id, a_fetch_start);
  }, true));
  ASSERT_TRUE(a_prev_lsn.is_valid());
  ASSERT_GT(a_prev_lsn, common_end);
  ASSERT_LT(a_prev_lsn, a_fetch_start);
  ASSERT_EQ(a_committed_end, a_fetch_start);
  LogInfo prev_log_info;
  ASSERT_EQ(OB_NEED_RETRY, b.get_prev_log_info_for_fetch_(a_prev_lsn, a_fetch_start, prev_log_info));
  auto check_failed_storage_fetch = [&]() -> int {
    FetchLogStat fetch_stat;
    const int ret = b.fetch_log_from_storage_(a_addr, FETCH_LOG_FOLLOWER,
        b.state_mgr_.get_proposal_id(), a_prev_lsn, a_fetch_start,
        1024 * 1024, PALF_SLIDING_WINDOW_SIZE, SCN::max_scn(), fetch_stat);
    EXPECT_EQ(0, fetch_stat.total_size_);
    EXPECT_EQ(0, fetch_stat.group_log_cnt_);
    return ret;
  };
  ASSERT_EQ(OB_NEED_RETRY, check_failed_storage_fetch());
  unblock_net(a_idx, b_idx);
  unblock_net(a_idx, r_idx);
  ObTenantEnv::set_tenant(get_cluster()[a_idx]->get_tenant_base());
  ASSERT_EQ(OB_SUCCESS, wait_for([&]() {
    ObRole role = LEADER;
    int64_t proposal_id = 0;
    bool pending = true;
    return OB_SUCCESS == a.get_role(role, proposal_id, pending) && FOLLOWER == role;
  }, true));
  ASSERT_EQ(OB_SUCCESS, wait_for([&]() {
    ObAddr fetch_dst;
    common::RWLock::RLockGuard guard(a.lock_);
    return OB_SUCCESS == a.sw_.get_fetch_log_dst_(fetch_dst) && b_addr == fetch_dst;
  }, true));
  auto request_and_observe_fetch = [&]() -> int {
    const int64_t previous_received_size = ATOMIC_LOAD(&b.accum_fetch_log_size_);
    int ret = OB_SUCCESS;
    {
      common::RWLock::RLockGuard guard(a.lock_);
      ret = a.sw_.try_fetch_log(FetchTriggerType::LOG_LOOP_TH);
    }
    if (OB_SUCCESS == ret) {
      // This production counter advances when B handles fetch RPCs. Its
      // periodic reset is allowed; a positive changed value proves receipt.
      ret = wait_for([&]() {
        const int64_t received_size = ATOMIC_LOAD(&b.accum_fetch_log_size_);
        return received_size > 0 && received_size != previous_received_size;
      }, true);
    }
    return ret;
  };
  ASSERT_EQ(OB_SUCCESS, request_and_observe_fetch());
  ASSERT_EQ(a_committed_end, a.get_end_lsn());
  ASSERT_EQ(a_committed_end, a.get_max_lsn());
  ASSERT_EQ(0, rebuild_observer.count_.load());
  PALF_LOG(INFO, "REPRO step 7: network restored, B receives fetches but A cannot advance",
           K(id), K(a_prev_lsn), K(a_fetch_start), "A_max_lsn", a.get_max_lsn(),
           "B_received_fetch_size", ATOMIC_LOAD(&b.accum_fetch_log_size_));

  // 8. Advance B's checkpoint through the public API after real log commits.
  // set_base_lsn rounds down to a block boundary, so write just over one block.
  // Retain the physical first block: base_lsn alone is not a lower-bound read
  // error. Repeat real fetch RPCs and verify the incident's no-rebuild state.
  ASSERT_LT(a_committed_end, LSN(PALF_BLOCK_SIZE));
  const LSN checkpoint_base(PALF_BLOCK_SIZE);
  const LSN b_end_before_checkpoint = b.get_end_lsn();
  std::string checkpoint_marker;
  ASSERT_EQ(OB_SUCCESS, make_degrade_vote_marker("B_CHECKPOINT_PROGRESS:", 'C', checkpoint_marker));
  checkpoint_marker.resize(1024 * 1024, 'C');
  ObTenantEnv::set_tenant(get_cluster()[b_idx]->get_tenant_base());
  const int64_t checkpoint_deadline = ObTimeUtility::current_time() + 60 * 1000 * 1000L;
  while (b.get_end_lsn() <= checkpoint_base) {
    ASSERT_LT(ObTimeUtility::current_time(), checkpoint_deadline);
    ASSERT_EQ(OB_SUCCESS, renew_b());
    ASSERT_EQ(OB_SUCCESS, append_marker(b, checkpoint_marker, last_b_lsn, last_b_scn, true));
    ASSERT_EQ(OB_SUCCESS, wait_for([&]() {
      return b.get_end_lsn() > last_b_lsn + checkpoint_marker.size();
    }, true));
  }
  ASSERT_GT(b.get_end_lsn(), b_end_before_checkpoint);
  ASSERT_EQ(OB_SUCCESS, b.set_base_lsn(checkpoint_base));
  LSN b_base_lsn;
  ASSERT_EQ(OB_SUCCESS, wait_for([&]() {
    return OB_SUCCESS == b.get_base_lsn(b_base_lsn) && b_base_lsn == checkpoint_base;
  }, true));
  LSN b_begin_lsn;
  ASSERT_EQ(OB_SUCCESS, b.get_begin_lsn(b_begin_lsn));
  ASSERT_LT(b_begin_lsn, a_committed_end);
  ASSERT_LT(a_committed_end, b_base_lsn);
  ObTenantEnv::set_tenant(get_cluster()[a_idx]->get_tenant_base());
  for (int64_t round = 0; round < 3; ++round) {
    ASSERT_EQ(OB_SUCCESS, request_and_observe_fetch());
    // Leave the real log-loop fetch timer running for a full retry interval;
    // three immediate calls would only demonstrate a momentary lack of progress.
    const int64_t observation_end = ObTimeUtility::current_time() + PALF_FETCH_LOG_INTERVAL_US;
    ASSERT_EQ(OB_SUCCESS, wait_for([&]() {
      return ObTimeUtility::current_time() >= observation_end;
    }, true));
    ASSERT_EQ(OB_NEED_RETRY, check_failed_storage_fetch());
    ASSERT_TRUE(a.is_sync_enabled());
    ASSERT_EQ(a_committed_end, a.get_end_lsn());
    ASSERT_EQ(a_committed_end, a.get_max_lsn());
    ASSERT_EQ(0, rebuild_observer.count_.load());
    LSN last_rebuild_lsn;
    ASSERT_EQ(OB_SUCCESS, a.get_last_rebuild_lsn(last_rebuild_lsn));
    ASSERT_EQ(initial_rebuild_lsn, last_rebuild_lsn);
    PalfStat a_stat;
    ASSERT_EQ(OB_SUCCESS, a.stat(a_stat));
    ASSERT_EQ(FOLLOWER, a_stat.role_);
    ASSERT_FALSE(a_stat.is_need_rebuild_);
  }
  PalfStat b_stat;
  ASSERT_EQ(OB_SUCCESS, b.stat(b_stat));
  ASSERT_EQ(LEADER, b_stat.role_);
  ASSERT_TRUE(b_stat.degraded_list_.contains(a_addr));
  ASSERT_EQ(OB_SUCCESS, b.get_begin_lsn(b_begin_lsn));
  ASSERT_LT(b_begin_lsn, a.get_max_lsn());
  ASSERT_LT(a.get_max_lsn(), b_base_lsn);
  PALF_LOG(INFO, "REPRO step 8: B.begin < A.max < B.base, fetch retries never notify rebuild",
           K(id), K(b_begin_lsn), "A_max_lsn", a.get_max_lsn(), K(b_base_lsn),
           "B_committed_end", b.get_end_lsn(), "rebuild_count", rebuild_observer.count_.load());

  // Positive control, only after recording the automatic no-rebuild failure:
  // send an explicit real notification and verify the registered callback can
  // fire. This separates a missing automatic notification from a broken test
  // observer or a follower that would reject a valid rebuild notification.
  ObTenantEnv::set_tenant(get_cluster()[b_idx]->get_tenant_base());
  PalfBaseInfo checkpoint_info;
  ASSERT_EQ(OB_SUCCESS, b.get_base_info(checkpoint_base, checkpoint_info));
  ASSERT_EQ(OB_SUCCESS, b.log_engine_.submit_notify_rebuild_req(
      a_addr, checkpoint_base, checkpoint_info.prev_log_info_));
  ASSERT_EQ(OB_SUCCESS, wait_for([&]() { return rebuild_observer.count_.load() > 0; }, true));
  EXPECT_EQ(id, rebuild_observer.last_id_.load());
  EXPECT_EQ(checkpoint_base.val_, rebuild_observer.last_lsn_.load());
  PALF_LOG(INFO, "REPRO positive control: explicit notification reaches A's rebuild callback",
           K(id), K(checkpoint_base), "rebuild_count", rebuild_observer.count_.load());
}

TEST_F(TestObSimpleLogClusterArbService, test_2f1a_degrade_upgrade)
{
  oceanbase::common::ObClusterVersion::get_instance().cluster_version_ = CLUSTER_VERSION_4_1_0_0;
  SET_CASE_LOG_FILE(TEST_NAME, "arb_2f1a_degrade_upgrade");
  OB_LOGGER.set_log_level("TRACE");
  MockLocCB loc_cb;
  int ret = OB_SUCCESS;
  PALF_LOG(INFO, "begin test_2f1a_degrade_upgrade");
	int64_t leader_idx = 0;
  int64_t arb_replica_idx = -1;
  PalfHandleImplGuard leader;
  std::vector<PalfHandleImplGuard*> palf_list;
  const int64_t CONFIG_CHANGE_TIMEOUT = 10 * 1000 * 1000L; // 10s
	const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  common::ObMember dummy_member;
	EXPECT_EQ(OB_SUCCESS, create_paxos_group_with_arb(id, arb_replica_idx, leader_idx, leader));
  EXPECT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id, palf_list));
  const int64_t another_f_idx = (leader_idx+1)%3;
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));
  // 为备副本设置location cb，用于备副本找leader
  palf_list[another_f_idx]->get_palf_handle_impl()->set_location_cache_cb(&loc_cb);
  block_net(leader_idx, another_f_idx);
  // do not check OB_SUCCESS, may return OB_NOT_MASTER during degrading member
  submit_log(leader, 100, id);

  PALF_LOG(INFO, "CASE[1] degrade caused by block_net ");
  EXPECT_TRUE(is_degraded(leader, another_f_idx));

  loc_cb.leader_ = leader.palf_handle_impl_->self_;
  unblock_net(leader_idx, another_f_idx);
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 10, id));

  EXPECT_TRUE(is_upgraded(leader, id));
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 10, id));

  // set clog disk error
  ObTenantEnv::set_tenant(get_cluster()[another_f_idx]->get_tenant_base());
  logservice::coordinator::ObFailureDetector *detector = MTL(logservice::coordinator::ObFailureDetector *);
  if (NULL != detector) {
    PALF_LOG(INFO, "set clog full event");
    detector->has_add_clog_full_event_ = true;
  }

  PALF_LOG(INFO, "CASE[2] degrade caused by clog disk error");
  EXPECT_TRUE(is_degraded(leader, another_f_idx));

  if (NULL != detector) {
    detector->has_add_clog_full_event_ = false;
  }

  EXPECT_TRUE(is_upgraded(leader, id));
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 1, id));

  // test disable sync
  PALF_LOG(INFO, "CASE[3] degrade caused by disable_sync");
  palf_list[another_f_idx]->palf_handle_impl_->disable_sync();
  EXPECT_TRUE(is_degraded(leader, another_f_idx));
  palf_list[another_f_idx]->palf_handle_impl_->enable_sync();
  EXPECT_TRUE(is_upgraded(leader, id));

  PALF_LOG(INFO, "CASE[4] degrade caused by disable_vote");
  // test disbale vote
  palf_list[another_f_idx]->palf_handle_impl_->disable_vote(false/*no need check log missing*/);
  EXPECT_TRUE(is_degraded(leader, another_f_idx));
  palf_list[another_f_idx]->palf_handle_impl_->enable_vote();
  EXPECT_TRUE(is_upgraded(leader, id));

  // test revoking the leader when arb service is degrading
  block_all_net(another_f_idx);
  const common::ObAddr follower_addr = get_cluster()[another_f_idx]->get_addr();
  LogConfigChangeArgs args(common::ObMember(follower_addr, 1), 0, DEGRADE_ACCEPTOR_TO_LEARNER);
  int64_t ele_epoch;
  common::ObRole ele_role;
  int64_t proposal_id = leader.palf_handle_impl_->state_mgr_.get_proposal_id();
  leader.palf_handle_impl_->election_.get_role(ele_role, ele_epoch);
  LogConfigVersion config_version;
  EXPECT_EQ(OB_EAGAIN, leader.palf_handle_impl_->config_mgr_.change_config_(args, proposal_id, ele_epoch, config_version));
  EXPECT_FALSE(leader.palf_handle_impl_->config_mgr_.alive_paxos_memberlist_.contains(follower_addr));
  EXPECT_EQ(leader.palf_handle_impl_->config_mgr_.state_, 1);

  // reset status supposing the lease is expried
  block_net(leader_idx, another_f_idx);
  leader.palf_handle_impl_->config_mgr_.reset_status();
  EXPECT_TRUE(is_degraded(leader, another_f_idx));
  unblock_net(leader_idx, another_f_idx);
  unblock_all_net(another_f_idx);

  revert_cluster_palf_handle_guard(palf_list);
  leader.reset();
  delete_paxos_group(id);
  PALF_LOG(INFO, "end test_2f1a_degrade_upgrade", K(id));
}

TEST_F(TestObSimpleLogClusterArbService, test_4f1a_degrade_upgrade)
{
  SET_CASE_LOG_FILE(TEST_NAME, "arb_4f1a_degrade_upgrade");
  OB_LOGGER.set_log_level("TRACE");
  MockLocCB loc_cb;
  int ret = OB_SUCCESS;
  PALF_LOG(INFO, "begin test_4f1a_degrade_upgrade");
	int64_t leader_idx = 0;
  int64_t arb_replica_idx = -1;
  PalfHandleImplGuard leader;
  const int64_t CONFIG_CHANGE_TIMEOUT = 10 * 1000 * 1000L; // 10s
	const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  common::ObMember dummy_member;
  std::vector<PalfHandleImplGuard*> palf_list;
	EXPECT_EQ(OB_SUCCESS, create_paxos_group_with_arb(id, arb_replica_idx, leader_idx, leader));
  LogConfigVersion config_version;
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_member(ObMember(get_cluster()[3]->get_addr(), 1), 3, config_version, CONFIG_CHANGE_TIMEOUT));
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_member(ObMember(get_cluster()[4]->get_addr(), 1), 4, config_version, CONFIG_CHANGE_TIMEOUT));
  EXPECT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id, palf_list));

  const int64_t another_f1_idx = (leader_idx+3)%5;
  const int64_t another_f2_idx = (leader_idx+4)%5;
  palf_list[another_f1_idx]->palf_handle_impl_->set_location_cache_cb(&loc_cb);
  palf_list[another_f2_idx]->palf_handle_impl_->set_location_cache_cb(&loc_cb);
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));
  sleep(2);
  block_all_net(another_f1_idx);
  block_all_net(another_f2_idx);


  EXPECT_TRUE(is_degraded(leader, another_f1_idx));
  EXPECT_TRUE(is_degraded(leader, another_f2_idx));

  unblock_all_net(another_f1_idx);
  unblock_all_net(another_f2_idx);
  loc_cb.leader_ = leader.palf_handle_impl_->self_;
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 10, id));

  EXPECT_TRUE(is_upgraded(leader, id));

  revert_cluster_palf_handle_guard(palf_list);
  leader.reset();
  delete_paxos_group(id);
  PALF_LOG(INFO, "end test_4f1a_degrade_upgrade", K(id));
}

TEST_F(TestObSimpleLogClusterArbService, test_2f1a_reconfirm_degrade_upgrade)
{
  SET_CASE_LOG_FILE(TEST_NAME, "arb_2f1a_reconfirm_test");
  OB_LOGGER.set_log_level("TRACE");
  int ret = OB_SUCCESS;
  PALF_LOG(INFO, "begin test_2f1a_reconfirm_degrade_upgrade");
  MockLocCB loc_cb;
	int64_t leader_idx = 0;
  int64_t arb_replica_idx = -1;
  PalfHandleImplGuard leader;
  std::vector<PalfHandleImplGuard*> palf_list;
  const int64_t CONFIG_CHANGE_TIMEOUT = 10 * 1000 * 1000L; // 10s
	const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  common::ObMember dummy_member;
	EXPECT_EQ(OB_SUCCESS, create_paxos_group_with_arb(id, arb_replica_idx, leader_idx, leader));
  EXPECT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id, palf_list));
  const int64_t another_f_idx = (leader_idx+1)%3;
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));
  sleep(2);
  palf_list[leader_idx]->palf_handle_impl_->set_location_cache_cb(&loc_cb);
  palf_list[another_f_idx]->palf_handle_impl_->set_location_cache_cb(&loc_cb);
  // block net of old leader, new leader will be elected
  // and degrade in RECONFIRM state
  ARB_TIMEOUT_ARG = 15 * 1000 * 1000;
  block_net(leader_idx, another_f_idx);
  block_net(leader_idx, arb_replica_idx);
  // block_net后会理解进行降级操作，导致旧主上有些单副本写成功的日志被committed
  submit_log(leader, 20, id);
  // submit some logs which will be truncated

  EXPECT_TRUE(is_degraded(*palf_list[another_f_idx], leader_idx));

  int64_t new_leader_idx = -1;
  PalfHandleImplGuard new_leader;
  EXPECT_EQ(OB_SUCCESS, get_leader(id, new_leader, new_leader_idx));
  loc_cb.leader_ = new_leader.palf_handle_impl_->self_;
  unblock_net(leader_idx, another_f_idx);
  unblock_net(leader_idx, arb_replica_idx);
  EXPECT_EQ(OB_SUCCESS, submit_log(new_leader, 100, id));

  EXPECT_TRUE(is_upgraded(new_leader, id));
  revert_cluster_palf_handle_guard(palf_list);
  leader.reset();
  new_leader.reset();
  delete_paxos_group(id);
  ARB_TIMEOUT_ARG = 2 * 1000 * 1000;
  PALF_LOG(INFO, "end test_2f1a_reconfirm_degrade_upgrade", K(id));
}

TEST_F(TestObSimpleLogClusterArbService, test_4f1a_reconfirm_degrade_upgrade)
{
  SET_CASE_LOG_FILE(TEST_NAME, "arb_4f1a_reconfirm_test");
  OB_LOGGER.set_log_level("TRACE");
  MockLocCB loc_cb;
  int ret = OB_SUCCESS;
  PALF_LOG(INFO, "begin test_4f1a_reconfirm_degrade_upgrade");
	int64_t leader_idx = 0;
  int64_t arb_replica_idx = -1;
  auto cluster = get_cluster();
  PalfHandleImplGuard leader;
  const int64_t CONFIG_CHANGE_TIMEOUT = 10 * 1000 * 1000L; // 10s
	const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  common::ObMember dummy_member;
  std::vector<PalfHandleImplGuard*> palf_list;

	EXPECT_EQ(OB_SUCCESS, create_paxos_group_with_arb(id, arb_replica_idx, leader_idx, leader));
  LogConfigVersion config_version;
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_member(ObMember(get_cluster()[3]->get_addr(), 1), 3, config_version, CONFIG_CHANGE_TIMEOUT));
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_member(ObMember(get_cluster()[4]->get_addr(), 1), 4, config_version, CONFIG_CHANGE_TIMEOUT));

  EXPECT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id, palf_list));

  const int64_t another_f1_idx = 3;
  const int64_t another_f2_idx = 4;
  palf_list[leader_idx]->palf_handle_impl_->set_location_cache_cb(&loc_cb);
  palf_list[another_f1_idx]->palf_handle_impl_->set_location_cache_cb(&loc_cb);
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));
  sleep(2);
  // stop leader and a follower
  block_all_net(leader_idx);
  block_all_net(another_f1_idx);

  //EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));
  // wait for new leader is elected
  int64_t new_leader_idx = leader_idx;
  PalfHandleImplGuard new_leader;
  while (leader_idx == new_leader_idx) {
    new_leader.reset();
    EXPECT_EQ(OB_SUCCESS, get_leader(id, new_leader, new_leader_idx));
  }

  EXPECT_TRUE(is_degraded(new_leader, another_f1_idx));
  EXPECT_TRUE(is_degraded(new_leader, leader_idx));

  loc_cb.leader_ = new_leader.palf_handle_impl_->self_;
  // restart two servers
  unblock_all_net(leader_idx);
  unblock_all_net(another_f1_idx);

  EXPECT_EQ(OB_SUCCESS, submit_log(new_leader, 100, id));

  EXPECT_TRUE(is_upgraded(new_leader, id));
  leader.reset();
  new_leader.reset();
  revert_cluster_palf_handle_guard(palf_list);
  delete_paxos_group(id);
  PALF_LOG(INFO, "end test_4f1a_reconfirm_degrade_upgrade", K(id));
}

TEST_F(TestObSimpleLogClusterArbService, test_2f1a_config_change)
{
  SET_CASE_LOG_FILE(TEST_NAME, "arb_2f1a_config_change");
  OB_LOGGER.set_log_level("DEBUG");
  MockLocCB loc_cb;
  int ret = OB_SUCCESS;
  PALF_LOG(INFO, "begin arb_2f1a_config_change");
	int64_t leader_idx = 0;
  int64_t arb_replica_idx = -1;
  PalfHandleImplGuard leader;
  std::vector<PalfHandleImplGuard*> palf_list;
  const int64_t CONFIG_CHANGE_TIMEOUT = 10 * 1000 * 1000L; // 10s
	const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  common::ObMember dummy_member;
	EXPECT_EQ(OB_SUCCESS, create_paxos_group_with_arb(id, arb_replica_idx, leader_idx, leader));
  EXPECT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id, palf_list));
  // 为备副本设置location cb，用于备副本找leader
  const int64_t another_f_idx = (leader_idx+1)%3;
  loc_cb.leader_ = leader.palf_handle_impl_->self_;
  palf_list[another_f_idx]->get_palf_handle_impl()->set_location_cache_cb(&loc_cb);
  palf_list[3]->get_palf_handle_impl()->set_location_cache_cb(&loc_cb);
  palf_list[4]->get_palf_handle_impl()->set_location_cache_cb(&loc_cb);
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));
  sleep(2);

  LogConfigVersion config_version;
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
  // replace member
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->replace_member(
      ObMember(palf_list[3]->palf_handle_impl_->self_, 1),
      ObMember(palf_list[another_f_idx]->palf_handle_impl_->self_, 1),
      config_version,
      CONFIG_CHANGE_TIMEOUT));

  // add learner
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_learner(
      ObMember(palf_list[4]->palf_handle_impl_->self_, 1),
      CONFIG_CHANGE_TIMEOUT));

  // switch learner
  EXPECT_EQ(OB_INVALID_ARGUMENT, leader.palf_handle_impl_->switch_learner_to_acceptor(
      ObMember(palf_list[4]->palf_handle_impl_->self_, 1),
      2,
      config_version,
      CONFIG_CHANGE_TIMEOUT));
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->switch_learner_to_acceptor(
      ObMember(palf_list[4]->palf_handle_impl_->self_, 1),
      3,
      config_version,
      CONFIG_CHANGE_TIMEOUT));
  revert_cluster_palf_handle_guard(palf_list);
  leader.reset();
  delete_paxos_group(id);
  PALF_LOG(INFO, "end arb_2f1a_config_change", K(id));
}

TEST_F(TestObSimpleLogClusterArbService, test_2f1a_arb_with_highest_version)
{
  oceanbase::common::ObClusterVersion::get_instance().cluster_version_ = CLUSTER_VERSION_4_1_0_0;
  SET_CASE_LOG_FILE(TEST_NAME, "test_2f1a_arb_with_highest_version");
  OB_LOGGER.set_log_level("DEBUG");
  MockLocCB loc_cb;
  int ret = OB_SUCCESS;
  PALF_LOG(INFO, "begin test_2f1a_arb_with_highest_version");
	int64_t leader_idx = 0;
  int64_t arb_replica_idx = -1;
  PalfHandleImplGuard leader;
  std::vector<PalfHandleImplGuard*> palf_list;
  const int64_t CONFIG_CHANGE_TIMEOUT = 10 * 1000 * 1000L; // 10s
	const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  common::ObMember dummy_member;
	EXPECT_EQ(OB_SUCCESS, create_paxos_group_with_arb(id, arb_replica_idx, leader_idx, leader));
  EXPECT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id, palf_list));
  // 为备副本设置location cb，用于备副本找leader
  const int64_t another_f_idx = (leader_idx+1)%3;
  loc_cb.leader_ = leader.palf_handle_impl_->self_;
  palf_list[another_f_idx]->get_palf_handle_impl()->set_location_cache_cb(&loc_cb);
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 500, id));
  sleep(2);

  LogConfigChangeArgs args(ObMember(palf_list[3]->palf_handle_impl_->self_, 1), 0, ADD_LEARNER);
  int64_t proposal_id = 0;
  int64_t election_epoch = 0;
  LogConfigVersion config_version;
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->config_mgr_.start_change_config(proposal_id, election_epoch, args.type_));
  EXPECT_EQ(OB_EAGAIN, leader.palf_handle_impl_->config_mgr_.change_config(args, proposal_id, election_epoch, config_version));
  // learner list and state_ has been changed
  EXPECT_TRUE(config_version.is_valid());
  EXPECT_EQ(1, leader.palf_handle_impl_->config_mgr_.state_);
  // only send config log to arb member
  ObMemberList member_list;
  member_list.add_server(get_cluster()[2]->get_addr());
  const int64_t prev_log_proposal_id = leader.palf_handle_impl_->config_mgr_.reconfig_barrier_.prev_log_proposal_id_;
  const LSN prev_lsn = leader.palf_handle_impl_->config_mgr_.reconfig_barrier_.prev_lsn_;
  const int64_t prev_mode_pid = leader.palf_handle_impl_->config_mgr_.reconfig_barrier_.prev_mode_pid_;
  const LogConfigMeta config_meta = leader.palf_handle_impl_->config_mgr_.log_ms_meta_;
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->log_engine_.submit_change_config_meta_req( \
    member_list, proposal_id, prev_log_proposal_id, prev_lsn, prev_mode_pid, config_meta));
  sleep(1);
  // check if arb member has received and persisted the config log
  while (true) {
    PalfHandleLiteGuard arb_member;
    if (OB_FAIL(get_arb_member_guard(id, arb_member))) {
    } else if (arb_member.palf_handle_lite_->config_mgr_.persistent_config_version_ == config_version) {
      break;
    } else {
      EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->log_engine_.submit_change_config_meta_req( \
        member_list, proposal_id, prev_log_proposal_id, prev_lsn, prev_mode_pid, config_meta));
    }
    ::ob_usleep(10 * 1000);
  }
  EXPECT_GT(config_version, leader.palf_handle_impl_->config_mgr_.persistent_config_version_);
  EXPECT_GT(config_version, palf_list[1]->palf_handle_impl_->config_mgr_.persistent_config_version_);

  // restart cluster, close a follower, restart leader
  revert_cluster_palf_handle_guard(palf_list);
  leader.reset();
  // block_net, so two F cann't reach majority
  block_net(another_f_idx, leader_idx);
  EXPECT_EQ(OB_SUCCESS, restart_paxos_groups());

  const int64_t restart_finish_time_us_ = common::ObTimeUtility::current_time();
  PalfHandleImplGuard new_leader;
  int64_t new_leader_idx;
  get_leader(id, new_leader, new_leader_idx);
  EXPECT_EQ(OB_SUCCESS, submit_log(new_leader, 500, id));
  PALF_LOG(ERROR, "RTO", "RTO", common::ObTimeUtility::current_time() - restart_finish_time_us_);

  new_leader.reset();
  // must delete paxos group in here, otherwise memory of
  // MockLocCB will be relcaimed and core dump will occur
  // blacklist will not be deleted after reboot, clean it manually
  unblock_net(another_f_idx, leader_idx);
  delete_paxos_group(id);
  PALF_LOG(INFO, "end test_2f1a_arb_with_highest_version", K(id));
}

TEST_F(TestObSimpleLogClusterArbService, test_2f1a_defensive)
{
  SET_CASE_LOG_FILE(TEST_NAME, "test_2f1a_defensive");
  OB_LOGGER.set_log_level("DEBUG");
  MockLocCB loc_cb;
  int ret = OB_SUCCESS;
  PALF_LOG(INFO, "begin test_2f1a_defensive");
	int64_t leader_idx = 0;
  int64_t arb_replica_idx = -1;
  PalfHandleImplGuard leader;
  std::vector<PalfHandleImplGuard*> palf_list;
  const int64_t CONFIG_CHANGE_TIMEOUT = 10 * 1000 * 1000L; // 10s
	const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  common::ObMember dummy_member;
	EXPECT_EQ(OB_SUCCESS, create_paxos_group_with_arb(id, arb_replica_idx, leader_idx, leader));
  EXPECT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id, palf_list));
  // 为备副本设置location cb，用于备副本找leader
  const int64_t another_f_idx = (leader_idx+1)%3;
  loc_cb.leader_ = leader.palf_handle_impl_->self_;
  palf_list[another_f_idx]->get_palf_handle_impl()->set_location_cache_cb(&loc_cb);
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));
  sleep(2);
  const int64_t added_member_idx = 3;
  const common::ObMember added_member = ObMember(palf_list[added_member_idx]->palf_handle_impl_->self_, 1);

  // add a member, do not allow to append logs until config log reaches majority
  LogConfigVersion cur_config_version;
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(cur_config_version));
  LogConfigChangeArgs args(added_member, 3, cur_config_version, ADD_MEMBER);
  int64_t proposal_id = 0;
  int64_t election_epoch = 0;
  LogConfigVersion config_version;
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->config_mgr_.start_change_config(proposal_id, election_epoch, args.type_));
  EXPECT_EQ(OB_EAGAIN, leader.palf_handle_impl_->config_mgr_.change_config(args, proposal_id, election_epoch, config_version));
  // do not allow to append log when changing config with arb
  // EXPECT_TRUE(leader.palf_handle_impl_->state_mgr_.is_changing_config_with_arb());
  while (true) {
    if (OB_SUCC(leader.palf_handle_impl_->config_mgr_.change_config(args, proposal_id, election_epoch, config_version))) {
      break;
    } else {
      (void) leader.palf_handle_impl_->config_mgr_.pre_sync_config_log_and_mode_meta(args.server_, proposal_id);
      ::ob_usleep(10 * 1000);
    }
  }

  // flashback one follower
  LogEntryHeader header_origin;
  SCN base_scn;
  base_scn.set_base();
  SCN flashback_scn;
  palf::AccessMode unused_access_mode;
  int64_t mode_version;
  EXPECT_EQ(OB_SUCCESS, get_middle_scn(50, leader, flashback_scn, header_origin));
  switch_append_to_flashback(leader, mode_version);
  sleep(1);
  EXPECT_EQ(OB_SUCCESS, palf_list[another_f_idx]->palf_handle_impl_->flashback(mode_version, flashback_scn, CONFIG_CHANGE_TIMEOUT));

  // remove another follower
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->remove_member(added_member, 2, CONFIG_CHANGE_TIMEOUT));

  revert_cluster_palf_handle_guard(palf_list);
  leader.reset();
  delete_paxos_group(id);
  PALF_LOG(INFO, "end test_2f1a_defensive", K(id));
}

int get_palf_handle_lite(const int64_t tenant_id,
                         const int64_t palf_id,
                         ObSimpleArbServer *server,
                         IPalfHandleImplGuard &handle_guard)
{
  int ret = OB_SUCCESS;
  PalfEnvLiteGuard env_guard;
  if (NULL == server) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(server->get_palf_env_lite(tenant_id, env_guard))) {
    PALF_LOG(ERROR, "get_palf_env_lite failed", K(tenant_id), K(palf_id));
  } else if (OB_FAIL(env_guard.palf_env_lite_->get_palf_handle_impl(palf_id, handle_guard))) {
    PALF_LOG(ERROR, "get_palf_handle_impl failed", K(tenant_id), K(palf_id));
  } else {
  }
  return ret;
}

using namespace palflite;

TEST_F(TestObSimpleLogClusterArbService, test_multi_meta_block)
{
  SET_CASE_LOG_FILE(TEST_NAME, "test_mutli_meta_block");
  OB_LOGGER.set_log_level("INFO");
  MockLocCB loc_cb;
  int ret = OB_SUCCESS;
  PALF_LOG(INFO, "begin test_multi_meta_block");
	int64_t leader_idx = 0;
  int64_t arb_replica_idx = -1;
  PalfHandleImplGuard leader;
  std::vector<PalfHandleImplGuard*> palf_list;
  const int64_t CONFIG_CHANGE_TIMEOUT = 10 * 1000 * 1000L; // 10s
	const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  common::ObMember dummy_member;
	EXPECT_EQ(OB_SUCCESS, create_paxos_group_with_arb(id, arb_replica_idx, leader_idx, leader));
  EXPECT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id, palf_list));
  // 为备副本设置location cb，用于备副本找leader
  const int64_t another_f_idx = (leader_idx+1)%3;
  loc_cb.leader_ = leader.palf_handle_impl_->self_;
  palf_list[another_f_idx]->get_palf_handle_impl()->set_location_cache_cb(&loc_cb);
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));
  sleep(2);
  ObSimpleArbServer *arb_server = dynamic_cast<ObSimpleArbServer*>(get_cluster()[arb_replica_idx]);
  IPalfHandleImplGuard arb_guard;
  ASSERT_EQ(OB_SUCCESS, get_palf_handle_lite(OB_SERVER_TENANT_ID, id, arb_server, arb_guard));
  PalfHandleLite *arb_palf = dynamic_cast<PalfHandleLite *>(arb_guard.palf_handle_impl_);
  LogEngine *log_engine = &arb_palf->log_engine_;
  LSN meta_tail = log_engine->log_meta_storage_.log_tail_;
  LogStorage *meta_storage = &log_engine->log_meta_storage_;
  {
    while (1) {
      if (meta_storage->log_tail_ < LSN(meta_storage->logical_block_size_)) {
        EXPECT_EQ(OB_SUCCESS, log_engine->append_log_meta_(log_engine->log_meta_));
      } else {
        break;
      }
    }
  }
  meta_tail = log_engine->log_meta_storage_.log_tail_;
  ASSERT_EQ(meta_tail, LSN(log_engine->log_meta_storage_.logical_block_size_));
  revert_cluster_palf_handle_guard(palf_list);
  arb_guard.reset();
  leader.reset();
  EXPECT_EQ(OB_SUCCESS, restart_paxos_groups());
  {
    ObSimpleArbServer *arb_server = dynamic_cast<ObSimpleArbServer*>(get_cluster()[arb_replica_idx]);
    PalfHandleImplGuard leader;
    EXPECT_EQ(OB_SUCCESS, get_leader(id, leader, leader_idx));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));
    IPalfHandleImplGuard arb_guard;
    ASSERT_EQ(OB_SUCCESS, get_palf_handle_lite(OB_SERVER_TENANT_ID, id, arb_server, arb_guard));
    PalfHandleLite *arb_palf = dynamic_cast<PalfHandleLite *>(arb_guard.palf_handle_impl_);
    LogEngine *log_engine = &arb_palf->log_engine_;
    LogStorage *meta_storage = &log_engine->log_meta_storage_;
    EXPECT_EQ(OB_SUCCESS, log_engine->append_log_meta_(log_engine->log_meta_));
    LSN meta_tail = log_engine->log_meta_storage_.log_tail_;
    ASSERT_NE(meta_tail, LSN(log_engine->log_meta_storage_.logical_block_size_));
  }
  EXPECT_EQ(OB_SUCCESS, restart_paxos_groups());
  {
    ObSimpleArbServer *arb_server = dynamic_cast<ObSimpleArbServer*>(get_cluster()[arb_replica_idx]);
    PalfHandleImplGuard leader;
    EXPECT_EQ(OB_SUCCESS, get_leader(id, leader, leader_idx));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));
    IPalfHandleImplGuard arb_guard;
    ASSERT_EQ(OB_SUCCESS, get_palf_handle_lite(OB_SERVER_TENANT_ID, id, arb_server, arb_guard));
    PalfHandleLite *arb_palf = dynamic_cast<PalfHandleLite *>(arb_guard.palf_handle_impl_);
    LogEngine *log_engine = &arb_palf->log_engine_;
    LSN meta_tail = log_engine->log_meta_storage_.log_tail_;
    LogStorage *meta_storage = &log_engine->log_meta_storage_;
    while (1) {
      if (meta_storage->log_tail_ < LSN(32 * meta_storage->logical_block_size_)) {
        EXPECT_EQ(OB_SUCCESS, log_engine->append_log_meta_(log_engine->log_meta_));
      } else {
        break;
      }
    }
  }
  EXPECT_EQ(OB_SUCCESS, restart_paxos_groups());
  {
    ObSimpleArbServer *arb_server = dynamic_cast<ObSimpleArbServer*>(get_cluster()[arb_replica_idx]);
    PalfHandleImplGuard leader;
    EXPECT_EQ(OB_SUCCESS, get_leader(id, leader, leader_idx));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 4000, id));
    IPalfHandleImplGuard arb_guard;
    ASSERT_EQ(OB_SUCCESS, get_palf_handle_lite(OB_SERVER_TENANT_ID, id, arb_server, arb_guard));
    PalfHandleLite *arb_palf = dynamic_cast<PalfHandleLite *>(arb_guard.palf_handle_impl_);
    LogEngine *log_engine = &arb_palf->log_engine_;
    LSN meta_tail = log_engine->log_meta_storage_.log_tail_;
    LogStorage *meta_storage = &log_engine->log_meta_storage_;
    while (1) {
      if (meta_storage->log_tail_ < LSN(34 * meta_storage->logical_block_size_ + 4*4*1024)) {
        EXPECT_EQ(OB_SUCCESS, log_engine->append_log_meta_(log_engine->log_meta_));
      } else {
        break;
      }
    }
  }
  delete_paxos_group(id);
  PALF_LOG(INFO, "end test_mutli_meta_block", K(id));
}

// 1. 2F1A, the leader starts to degrade another F
// 2. after the config log has been accepted by another F, the leader revoked
// 3. the previous leader has been elected as the new leader
// 4. reconfirm may fail because leader's config_version is not same to that of the follower
TEST_F(TestObSimpleLogClusterArbService, test_2f1a_degrade_when_no_leader)
{
  SET_CASE_LOG_FILE(TEST_NAME, "test_2f1a_degrade_when_no_leader");
  MockLocCB loc_cb;
  int ret = OB_SUCCESS;
  PALF_LOG(INFO, "begin test_2f1a_degrade_when_no_leader");
	int64_t leader_idx = 0;
  int64_t arb_replica_idx = -1;
  PalfHandleImplGuard leader;
  std::vector<PalfHandleImplGuard*> palf_list;
	const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  common::ObMember dummy_member;
	EXPECT_EQ(OB_SUCCESS, create_paxos_group_with_arb(id, arb_replica_idx, leader_idx, leader));
  EXPECT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id, palf_list));
  // 为备副本设置location cb，用于备副本找leader
  const int64_t another_f_idx = (leader_idx+1)%3;
  loc_cb.leader_ = leader.palf_handle_impl_->self_;
  palf_list[another_f_idx]->get_palf_handle_impl()->set_location_cache_cb(&loc_cb);
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));
  sleep(2);

  const common::ObAddr b_addr = palf_list[another_f_idx]->palf_handle_impl_->self_;
  LogConfigChangeArgs args(ObMember(b_addr, 1), 0, DEGRADE_ACCEPTOR_TO_LEARNER);
  int64_t proposal_id = 0;
  int64_t election_epoch = 0;
  LogConfigVersion config_version;
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->config_mgr_.start_change_config(proposal_id, election_epoch, args.type_));
  EXPECT_EQ(OB_EAGAIN, leader.palf_handle_impl_->config_mgr_.change_config(args, proposal_id, election_epoch, config_version));

  // leader appended config meta
  EXPECT_FALSE(palf_list[leader_idx]->get_palf_handle_impl()->config_mgr_.log_ms_meta_.curr_.config_.log_sync_memberlist_.contains(b_addr));
  EXPECT_TRUE(palf_list[another_f_idx]->get_palf_handle_impl()->config_mgr_.log_ms_meta_.curr_.config_.log_sync_memberlist_.contains(b_addr));

  // block all networks of arb member, and the network from the follower to the leader
  block_net(arb_replica_idx, another_f_idx, true);
  block_net(arb_replica_idx, leader_idx, true);
  block_net(another_f_idx, leader_idx, true);

  // waiting for leader revoke
  while (leader.palf_handle_impl_->state_mgr_.role_ == common::ObRole::LEADER) {
    sleep(1);
  }

  // unblock_net
  unblock_net(another_f_idx, leader_idx);
  unblock_net(arb_replica_idx, leader_idx);

  common::ObMemberList leader_member_list;
  int64_t leader_replica_num = 0;
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->config_mgr_.get_log_sync_member_list( \
      leader_member_list, leader_replica_num));
  EXPECT_EQ(1, leader_member_list.get_member_number());
  EXPECT_EQ(1, leader_replica_num);

  int64_t new_leader_idx = 0;
  PalfHandleImplGuard new_leader;
  EXPECT_EQ(OB_SUCCESS, get_leader(id, new_leader, new_leader_idx));

  EXPECT_EQ(leader.palf_handle_impl_->self_, new_leader.palf_handle_impl_->self_);

  // waiting for upgrading
  is_upgraded(leader, id);
  EXPECT_EQ(OB_SUCCESS, new_leader.palf_handle_impl_->config_mgr_.get_log_sync_member_list( \
      leader_member_list, leader_replica_num));
  EXPECT_EQ(2, leader_member_list.get_member_number());
  EXPECT_EQ(2, leader_replica_num);

  revert_cluster_palf_handle_guard(palf_list);
  leader.reset();
  new_leader.reset();
  delete_paxos_group(id);
  PALF_LOG(INFO, "end test_2f1a_degrade_when_no_leader", K(id));
}

TEST_F(TestObSimpleLogClusterArbService, test_2f1a_upgrade_when_no_leader)
{
  SET_CASE_LOG_FILE(TEST_NAME, "test_2f1a_upgrade_when_no_leader");
//  OB_LOGGER.set_log_level("TRACE");
  MockLocCB loc_cb;
  int ret = OB_SUCCESS;
  PALF_LOG(INFO, "begin test_2f1a_upgrade_when_no_leader");
	int64_t leader_idx = 0;
  int64_t arb_replica_idx = -1;
  PalfHandleImplGuard leader;
  std::vector<PalfHandleImplGuard*> palf_list;
	const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  common::ObMember dummy_member;
	EXPECT_EQ(OB_SUCCESS, create_paxos_group_with_arb(id, arb_replica_idx, leader_idx, leader));
  EXPECT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id, palf_list));
  // 为备副本设置location cb，用于备副本找leader
  const int64_t another_f_idx = (leader_idx+1)%3;
  loc_cb.leader_ = leader.palf_handle_impl_->self_;
  palf_list[another_f_idx]->get_palf_handle_impl()->set_location_cache_cb(&loc_cb);
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));
  sleep(2);

  // block the network from the follower to the leader
  block_net(another_f_idx, leader_idx, true);
  is_degraded(leader, another_f_idx);

  // upgrade follower manually
  int64_t proposal_id;
  int64_t election_epoch;
  LogConfigVersion config_version;
  LogConfigChangeArgs args(common::ObMember(get_cluster()[another_f_idx]->get_addr(), 1), 0, UPGRADE_LEARNER_TO_ACCEPTOR);
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->config_mgr_.start_change_config(proposal_id, election_epoch, args.type_));

  block_net(arb_replica_idx, leader_idx, true);
  EXPECT_EQ(OB_EAGAIN, leader.palf_handle_impl_->config_mgr_.change_config(args, proposal_id, election_epoch, config_version));
  EXPECT_EQ(1, leader.palf_handle_impl_->config_mgr_.state_);
  EXPECT_EQ(OB_EAGAIN, leader.palf_handle_impl_->config_mgr_.change_config(args, proposal_id, election_epoch, config_version));
  EXPECT_EQ(1, leader.palf_handle_impl_->config_mgr_.state_);

  // waiting for leader revoke
  while (leader.palf_handle_impl_->state_mgr_.role_ == LEADER) {
    sleep(1);
  }

  // avoid the follower is elected to be leader
  block_net(arb_replica_idx, another_f_idx, true);
  unblock_all_net(leader_idx);

  // waiting for leader takeover
  while (!leader.palf_handle_impl_->state_mgr_.is_leader_active()) {
    sleep(1);
  }
  // waiting for upgrading
  is_upgraded(leader, id);

  revert_cluster_palf_handle_guard(palf_list);
  leader.reset();
  delete_paxos_group(id);
  PALF_LOG(INFO, "end test_2f1a_upgrade_when_no_leader", K(id));
}

TEST_F(TestObSimpleLogClusterArbService, test_1f1a_create_palf_group)
{
  oceanbase::common::ObClusterVersion::get_instance().cluster_version_ = CLUSTER_VERSION_4_1_0_0;
  SET_CASE_LOG_FILE(TEST_NAME, "test_1f1a_create_palf_group");
  PALF_LOG(INFO, "begin test_1f1a_create_palf_group");
  OB_LOGGER.set_log_level("TRACE");
  const int64_t CONFIG_CHANGE_TIMEOUT = 10 * 1000 * 1000L; // 10s
  MockLocCB loc_cb;
  int ret = OB_SUCCESS;
  int64_t leader_idx = 0;
  int64_t arb_replica_idx = -1;
  PalfHandleImplGuard leader;
  const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  common::ObMemberList member_list = get_member_list();
  member_list.remove_server(get_cluster()[1]->get_addr());
  const int64_t member_cnt = 2;
  const common::ObMember &arb_member = get_arb_member();
  EXPECT_EQ(OB_SUCCESS, create_paxos_group_with_arb(id, &loc_cb, member_list, member_cnt, arb_member, arb_replica_idx, leader_idx, false, leader));
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));

  LogConfigVersion config_version;
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_member(ObMember(get_cluster()[1]->get_addr(), 1), 2, config_version, CONFIG_CHANGE_TIMEOUT));

  EXPECT_EQ(2, leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.log_sync_replica_num_);
  EXPECT_EQ(2, leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.log_sync_memberlist_.get_member_number());

  leader.reset();
  delete_paxos_group(id);
  PALF_LOG(INFO, "end test_2f1a_degrade_upgrade", K(id));
}

// 1. 2F1A
// 2. lock_memberlist(just renew barrier)
// 3. submit and commit logs
// 4. kill leader
// 5. check committed_end_lsn
TEST_F(TestObSimpleLogClusterArbService, test_lock_memberlist_opt)
{
  SET_CASE_LOG_FILE(TEST_NAME, "test_lock_memberlist_opt");
  int ret = OB_SUCCESS;
	const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  PALF_LOG(INFO, "begin test_repeat_lock_memberlist", K(id));
	int64_t leader_idx = 0, arb_replica_idx = 0;
  PalfHandleImplGuard leader;
  oceanbase::common::ObClusterVersion::get_instance().cluster_version_ = CLUSTER_VERSION_4_2_0_0;
	EXPECT_EQ(OB_SUCCESS, create_paxos_group_with_arb(id, arb_replica_idx, leader_idx, leader));
  const int64_t CONFIG_CHANGE_TIMEOUT = 10 * 1000 * 1000L; // 10s
  const int64_t another_f_idx = (leader_idx+1)%3;

  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 10, id));
  EXPECT_UNTIL_EQ(leader.palf_handle_impl_->get_max_lsn(), leader.palf_handle_impl_->get_end_lsn());

  // 2. renew_barrier
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->config_mgr_.renew_config_change_barrier());

  // 3. submit and commit logs
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 10, id));
  const LSN max_lsn = leader.palf_handle_impl_->get_max_lsn();
  EXPECT_UNTIL_EQ(leader.palf_handle_impl_->get_max_lsn(), leader.palf_handle_impl_->get_end_lsn());

  // 4. kill leader
  block_all_net(leader_idx);

  // 5. check committed_end_lsn
  int64_t new_leader_idx = -1;
  PalfHandleImplGuard new_leader;
  EXPECT_EQ(OB_SUCCESS, get_leader(id, new_leader, new_leader_idx));
  EXPECT_UNTIL_EQ(leader.palf_handle_impl_->get_max_lsn(), leader.palf_handle_impl_->get_end_lsn());
  EXPECT_EQ(max_lsn, leader.palf_handle_impl_->get_end_lsn());
  unblock_all_net(leader_idx);

  leader.reset();
  new_leader.reset();
  delete_paxos_group(id);
  PALF_LOG(INFO, "end test_lock_memberlist_opt", K(id));
}

TEST_F(TestObSimpleLogClusterArbService, test_4f1a_force_set_member_list)
{
  SET_CASE_LOG_FILE(TEST_NAME, "test_4f1a_force_set_member_list");
  OB_LOGGER.set_log_level("INFO");
  MockLocCB loc_cb;
  int ret = OB_SUCCESS;
  PALF_LOG(INFO, "begin test_4f1a_force_set_member_list");
  int64_t leader_idx = 0;
  int64_t arb_replica_idx = -1;
  PalfHandleImplGuard leader;
  const int64_t CONFIG_CHANGE_TIMEOUT = 10 * 1000 * 1000L;
  const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  std::vector<PalfHandleImplGuard*> palf_list;
	EXPECT_EQ(OB_SUCCESS, create_paxos_group_with_arb(id, arb_replica_idx, leader_idx, leader));
  LogConfigVersion config_version;
  // change config to 4F1A
  PALF_LOG(INFO, "begin change config to 4F1A");
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_member(ObMember(get_cluster()[3]->get_addr(), 1), 3, config_version, CONFIG_CHANGE_TIMEOUT));
  ASSERT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_config_version(config_version));
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_member(ObMember(get_cluster()[4]->get_addr(), 1), 4, config_version, CONFIG_CHANGE_TIMEOUT));
  EXPECT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id, palf_list));
  PALF_LOG(INFO, "change config to 4F1A completes");
  const int64_t removed_follower_idx = (leader_idx+1)%5;
  const int64_t another_f1_idx = (leader_idx+3)%5;
  const int64_t another_f2_idx = (leader_idx+4)%5;
  palf_list[another_f1_idx]->palf_handle_impl_->set_location_cache_cb(&loc_cb);
  palf_list[another_f2_idx]->palf_handle_impl_->set_location_cache_cb(&loc_cb);
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id));
  EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(leader, leader.palf_handle_impl_->get_max_lsn()));
  // make 2F1A down
  block_all_net(arb_replica_idx);
  block_all_net(removed_follower_idx);
  block_all_net(leader_idx);
  PALF_LOG(INFO, "remove 2F1A successfully");

  int64_t new_leader_idx = leader_idx;
  PalfHandleImplGuard new_leader;
  // waiting for leader revoke
  while (leader.palf_handle_impl_->state_mgr_.role_ == common::ObRole::LEADER) {
    sleep(1);
  }

  common::ObMemberList new_member_list;
  new_member_list.add_member(ObMember(get_cluster()[another_f1_idx]->get_addr(), 1));
  new_member_list.add_member(ObMember(get_cluster()[another_f2_idx]->get_addr(), 1));
  int64_t new_replica_num = 2;
  EXPECT_EQ(OB_INVALID_ARGUMENT, palf_list[another_f1_idx]->palf_handle_impl_->force_set_member_list(new_member_list, new_replica_num + 1));
  EXPECT_EQ(OB_SUCCESS, palf_list[another_f1_idx]->palf_handle_impl_->force_set_member_list(new_member_list, new_replica_num));
  EXPECT_EQ(OB_SUCCESS, palf_list[another_f2_idx]->palf_handle_impl_->force_set_member_list(new_member_list, new_replica_num));
  PALF_LOG(INFO, "force_set_member_list completes");
  // waitr for new leader
  new_leader_idx = -1;
  while (-1 == new_leader_idx) {
    new_leader.reset();
    EXPECT_EQ(OB_SUCCESS, get_leader(id, new_leader, new_leader_idx));
  }

  common::ObMemberList curr_member_list;
  int64_t curr_replica_num = 0;
  EXPECT_EQ(OB_SUCCESS, palf_list[new_leader_idx]->palf_handle_impl_->get_paxos_member_list(curr_member_list, curr_replica_num));
  EXPECT_EQ(new_replica_num, curr_replica_num);
  EXPECT_EQ(OB_SUCCESS, submit_log(new_leader, 100, id));
  EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(new_leader, new_leader.palf_handle_impl_->get_max_lsn()));

  {
    // test restart after setting member list forcely
    revert_cluster_palf_handle_guard(palf_list);
    leader.reset();
    new_leader.reset();
    EXPECT_EQ(OB_SUCCESS, restart_paxos_groups());
    new_leader_idx = -1;
    while (-1 == new_leader_idx) {
      new_leader.reset();
      EXPECT_EQ(OB_SUCCESS, get_leader(id, new_leader, new_leader_idx));
    }
    EXPECT_EQ(OB_SUCCESS, new_leader.palf_handle_impl_->get_paxos_member_list(curr_member_list, curr_replica_num));
    EXPECT_EQ(new_replica_num, curr_replica_num);

    EXPECT_EQ(OB_SUCCESS, submit_log(new_leader, 100, id));
    EXPECT_EQ(OB_SUCCESS, wait_until_has_committed(new_leader, new_leader.palf_handle_impl_->get_max_lsn()));
  }

  unblock_all_net(arb_replica_idx);
  unblock_all_net(removed_follower_idx);
  unblock_all_net(leader_idx);

  new_leader.reset();
  delete_paxos_group(id);
  PALF_LOG(INFO, "end 4f1a_force_set_member_list", K(id));
}

TEST_F(TestObSimpleLogClusterArbService, test_degradation_policy)
{
  SET_CASE_LOG_FILE(TEST_NAME, "test_degradation_policy");
  OB_LOGGER.set_log_level("INFO");
  MockLocCB loc_cb;
  int ret = OB_SUCCESS;
  PALF_LOG(INFO, "begin test_degradation_policy");
  oceanbase::common::ObClusterVersion::get_instance().cluster_version_ = CLUSTER_VERSION_4_2_1_8;
  int64_t leader_idx = 0;
  int64_t arb_replica_idx = -1;
  int64_t another_f_idx = 1;
  PalfHandleImplGuard leader;
  const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  share::ObLSID ls_id(id);
  std::vector<PalfHandleImplGuard*> palf_list;
	EXPECT_EQ(OB_SUCCESS, create_paxos_group_with_arb(id, arb_replica_idx, leader_idx, leader));
  EXPECT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id, palf_list));
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 10, id));
  palf_list[leader_idx]->get_palf_handle_impl()->set_location_cache_cb(&loc_cb);
  palf_list[another_f_idx]->get_palf_handle_impl()->set_location_cache_cb(&loc_cb);
  first_f_addr = palf_list[leader_idx]->get_palf_handle_impl()->self_;
  second_f_addr = palf_list[another_f_idx]->get_palf_handle_impl()->self_;
  degradation_policy = CLUSTER_POLICY;
  GCONF.arbitration_degradation_policy = "CLUSTER_POLICY";
  {
    // case 1: test election silent
    int64_t first_leader_idx = leader_idx;
    PALF_LOG(INFO, "case 1: make election in current leader replica silent, the leader should change to another replica", K(first_leader_idx));
    // test election silent
    fisrt_f_connected_with_rs = false;
    EXPECT_EQ(OB_SUCCESS, palf_list[leader_idx]->palf_handle_impl_->set_election_silent_flag(true));
    // wait for leader revoke
    while (leader.palf_handle_impl_->state_mgr_.role_ == common::ObRole::LEADER) {
      sleep(1);
    }
    // wait for new leader
    leader_idx = -1;
    while (-1 == leader_idx) {
      leader.reset();
      EXPECT_EQ(OB_SUCCESS, get_leader(id, leader, leader_idx));
      sleep(1);
    }
    EXPECT_EQ(another_f_idx, leader_idx);
    // disable another replica prepare/propose
    second_f_connected_with_rs = false;
    EXPECT_EQ(OB_SUCCESS, palf_list[another_f_idx]->palf_handle_impl_->set_election_silent_flag(true));
    // wait for leader revoke
    while (leader.palf_handle_impl_->state_mgr_.role_ == common::ObRole::LEADER) {
      sleep(1);
    }
    // no leader for now
    // enable prepare/propose
    fisrt_f_connected_with_rs = true;
    EXPECT_EQ(OB_SUCCESS, palf_list[first_leader_idx]->palf_handle_impl_->set_election_silent_flag(false));
    // wait for new leader
    EXPECT_EQ(OB_SUCCESS, get_leader(id, leader, leader_idx));
    EXPECT_EQ(first_leader_idx, leader_idx);
    second_f_connected_with_rs = true;
    EXPECT_EQ(OB_SUCCESS, palf_list[another_f_idx]->palf_handle_impl_->set_election_silent_flag(false));
  }

  {
    // case 2: test network broken
    sleep(5);
    int64_t first_leader_idx = leader_idx;
    PALF_LOG(INFO, "case 2: test degrade for 2F1A");
    block_net(first_leader_idx, another_f_idx);
    fisrt_f_connected_with_rs = false;
    // should not be degraded
    sleep(5);
    common::GlobalLearnerList degraded_learner_list;
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->config_mgr_.get_degraded_learner_list(degraded_learner_list));
    EXPECT_EQ(0, degraded_learner_list.get_member_number());
    EXPECT_EQ(true, palf_list[first_leader_idx]->palf_handle_impl_->is_election_silent());

    // wait for leader revoke
    while (leader.palf_handle_impl_->state_mgr_.role_ == common::ObRole::LEADER) {
      sleep(1);
    }

    // wait for new leader
    leader_idx = -1;
    while (-1 == leader_idx) {
      leader.reset();
      EXPECT_EQ(OB_SUCCESS, get_leader(id, leader, leader_idx));
      sleep(1);
    }
    EXPECT_EQ(another_f_idx, leader_idx);
    degraded_learner_list.reset();
    // fisrt replica should be degraded
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->config_mgr_.get_degraded_learner_list(degraded_learner_list));
    EXPECT_EQ(1, degraded_learner_list.get_member_number());
    common::ObAddr degraded_addr;
    EXPECT_EQ(OB_SUCCESS, degraded_learner_list.get_server_by_index(0, degraded_addr));
    EXPECT_EQ(first_f_addr, degraded_addr);
    // network recover
    unblock_net(first_leader_idx, another_f_idx);
    fisrt_f_connected_with_rs = true;
    // wait for upgrade
    EXPECT_TRUE(is_upgraded(leader, id));
    EXPECT_EQ(false, palf_list[first_leader_idx]->palf_handle_impl_->is_election_silent());
  }

  revert_cluster_palf_handle_guard(palf_list);
  leader.reset();
  delete_paxos_group(id);
  PALF_LOG(INFO, "end test_degradation_policy", K(id));
}

} // end unittest
} // end oceanbase

int main(int argc, char **argv)
{
  RUN_SIMPLE_LOG_CLUSTER_TEST(TEST_NAME);
}
