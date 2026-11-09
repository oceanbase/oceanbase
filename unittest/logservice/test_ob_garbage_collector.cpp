/**
 * Copyright (c) 2026 OceanBase
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
#include <pthread.h>
#include "lib/worker.h"
#include "common/ob_clock_generator.h"
#include "lib/time/ob_time_utility.h"
#include "lib/utility/utility.h"
#include "lib/utility/ob_tracepoint.h"
#include "logservice/logrpc/ob_log_rpc_req.h"
#include "lib/atomic/ob_atomic.h"
#include "observer/ob_server_struct.h"
#include "share/config/ob_server_config.h"
#include "share/ob_cluster_version.h"
#define private public
#include "logservice/ob_ls_retention_gc_handler.h"
#include "logservice/logrouteservice/ob_log_route_struct.h"
#include "storage/ls/ob_ls.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#undef private
#include "logservice/logrpc/ob_log_request_handler.h"

namespace oceanbase
{
using namespace common;
using namespace logservice;
using namespace palf;
using namespace share;
using storage::ObLS;

namespace unittest
{
using RetentionGCLeader = ObLSRetentionGCHandler::Leader;
using RetentionGCReplica = ObLSRetentionGCHandler::GCReplica;

class MockRetentionGCLeader : public RetentionGCLeader
{
public:
  MockRetentionGCLeader()
    : result_(OB_SUCCESS), batch_result_(OB_SUCCESS),
      begin_(SCN::min_scn()), end_(SCN::plus(SCN::min_scn(), 200)), calls_(0),
      last_batch_size_(0), result_codes_(), unlocked_(false), leader_stat_(), on_refresh_(NULL),
      check_stat_locked_(false), reply_mode_version_(INVALID_PROPOSAL_ID), on_stat_(NULL)
  {
    leader_stat_.role_ = LEADER;
    leader_stat_.log_proposal_id_ = 10;
    leader_stat_.mode_version_ = 10;
    leader_stat_.config_version_ = version(10, 1);
  }
  static LogConfigVersion version(const int64_t proposal_id, const int64_t config_seq)
  {
    LogConfigVersion version;
    if (INVALID_PROPOSAL_ID != proposal_id) {
      EXPECT_EQ(OB_SUCCESS, version.generate(proposal_id, config_seq));
    }
    return version;
  }
  virtual ~MockRetentionGCLeader() {}
  // Seed cache fixtures independently of the RPC state-machine tests below.
  int seed_hold(const ObLSGCRequest &request, const int64_t now)
  {
    int ret = OB_SUCCESS;
    ObLSGCRequest report;
    int64_t token = INVALID_GC_TOKEN;
    int64_t index = find_gc_request_(request.src_);
    if (is_gc_approved_(request.src_) || request.get_config_version() != config_version_
        || request.get_mode_version() != mode_version_) {
      ret = OB_STATE_NOT_MATCH;
    } else if (OB_FAIL(alloc_gc_token_(token))) {
    } else if (OB_FAIL(report.init(request.src_, request.get_ls_id(), request.get_cutoff_scn(),
        request.get_end_scn(), request.get_config_version(), request.get_mode_version(),
        request.get_begin_lsn(), request.get_end_lsn(), token))) {
    } else {
      GCRequestState state(request.src_, token, request.get_mode_version());
      if (index < 0) {
        ret = gc_requests_.push_back(state);
        index = gc_requests_.count() - 1;
      } else {
        gc_requests_.at(index) = state;
      }
      if (OB_SUCC(ret) && OB_FAIL(register_retained_replica_(report, now))) {
      } else if (OB_SUCC(ret)) {
        gc_requests_.at(index).phase_ = GCRequestState::HOLD;
      }
    }
    return ret;
  }
  int mark_approved(const ObAddr &server)
  {
    int ret = OB_SUCCESS;
    int64_t token = INVALID_GC_TOKEN;
    int64_t index = find_gc_request_(server);
    if (index < 0 && OB_FAIL(alloc_gc_token_(token))) {
    } else if (index < 0 && OB_FAIL(gc_requests_.push_back(GCRequestState(server, token, mode_version_)))) {
    } else {
      index = find_gc_request_(server);
      gc_requests_.at(index).phase_ = GCRequestState::APPROVED;
    }
    return ret;
  }
  int64_t approved_count() const
  {
    int64_t count = 0;
    for (int64_t i = 0; i < gc_requests_.count(); ++i) {
      count += GCRequestState::APPROVED == gc_requests_.at(i).phase_ ? 1 : 0;
    }
    return count;
  }
  ObAddr approved_server() const
  {
    ObAddr server;
    for (int64_t i = 0; i < gc_requests_.count() && !server.is_valid(); ++i) {
      if (GCRequestState::APPROVED == gc_requests_.at(i).phase_) {
        server = gc_requests_.at(i).server_;
      }
    }
    return server;
  }
  int result_;
  int batch_result_;
  SCN begin_;
  SCN end_;
  mutable int64_t calls_;
  mutable int64_t last_batch_size_;
  ObArray<int> result_codes_;
  mutable bool unlocked_;
  PalfStat leader_stat_;
  void (*on_refresh_)(MockRetentionGCLeader *);
  bool check_stat_locked_;
  int64_t reply_mode_version_;
  void (*on_stat_)(MockRetentionGCLeader *);
  int get_leader_stat_(PalfStat &stat) const override
  {
    stat = leader_stat_;
    MockRetentionGCLeader *self = const_cast<MockRetentionGCLeader *>(this);
    if (check_stat_locked_) {
      const int lock_ret = self->lock_.trylock();
      EXPECT_NE(OB_SUCCESS, lock_ret);
      if (OB_SUCCESS == lock_ret) {
        self->lock_.unlock();
      }
    }
    if (NULL != on_stat_) {
      on_stat_(self);
    }
    return LEADER == stat.role_ ? OB_SUCCESS : OB_NOT_MASTER;
  }
  int get_replica_palf_stats_(const ObIArray<ObAddr> &servers,
                              ObIArray<PalfStat> &palf_stats) const override
  {
    int ret = batch_result_;
    const int64_t sampled_mode = leader_stat_.mode_version_;
    MockRetentionGCLeader *self = const_cast<MockRetentionGCLeader *>(this);
    unlocked_ = OB_SUCCESS == self->lock_.trylock();
    if (unlocked_) {
      self->lock_.unlock();
    } else {
      ret = OB_EAGAIN;
    }
    if (unlocked_ && NULL != on_refresh_) {
      on_refresh_(self);
    }
    PalfStat stat;
    ++calls_;
    last_batch_size_ = servers.count();
    palf_stats.reuse();
    stat.mode_version_ = INVALID_PROPOSAL_ID == reply_mode_version_ ? sampled_mode : reply_mode_version_;
    stat.begin_scn_ = begin_;
    stat.end_scn_ = end_;
    stat.begin_lsn_ = LSN(100);
    stat.end_lsn_ = LSN(200);
    if (OB_SUCC(ret) && !result_codes_.empty() && result_codes_.count() != servers.count()) {
      ret = OB_INVALID_ARGUMENT;
    }
    // Return successful replies in reverse order to exercise self-based matching.
    for (int64_t i = servers.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
      stat.self_ = servers.at(i);
      if (OB_SUCCESS == (result_codes_.empty() ? result_ : result_codes_.at(i))) {
        ret = palf_stats.push_back(stat);
      }
    }
    return ret;
  }
};

class MockRetentionGCReplica : public RetentionGCReplica
{
public:
  MockRetentionGCReplica()
    : begin_(SCN::min_scn()), end_(SCN::plus(SCN::min_scn(), 200)),
      result_(OB_SUCCESS), rpc_result_(OB_NOT_INIT),
      calls_(0), response_(), leader_(NULL),
      end_scn_(), request_(), drop_allow_response_(false), mode_version_(10), mode_result_(OB_SUCCESS), report_range_(true) {}
  virtual ~MockRetentionGCReplica() {}
  SCN begin_;
  SCN end_;
  int result_;
  int rpc_result_;
  int64_t calls_;
  ObLSGCResponse response_;
  MockRetentionGCLeader *leader_;
  SCN end_scn_;
  ObLSGCRequest request_;
  bool drop_allow_response_;
  int64_t mode_version_;
  int mode_result_;
  bool report_range_;
private:
  int get_local_log_range_(PalfStat &stat) override
  {
    stat.reset();
    stat.mode_version_ = mode_version_;
    if (report_range_) {
      stat.begin_scn_ = begin_;
      stat.end_scn_ = end_;
      stat.begin_lsn_ = LSN(100);
      stat.end_lsn_ = LSN(200);
    }
    return mode_result_;
  }
  int request_permission_(const ObLSGCRequest &request, ObLSGCResponse &response) override
  {
    ++calls_;
    request_ = request;
    response = response_;
    int ret = NULL == leader_ ? rpc_result_ : leader_->handle_request(request, response);
    if (OB_SUCCESS == ret && drop_allow_response_ && response.is_gc_allowed()) {
      // The leader has committed approval, but the caller never receives the reply.
      response.reset();
      ret = OB_TIMEOUT;
    }
    return ret;
  }
  int check_log_retention_(const SCN &end_scn,
                           bool &allow_gc, SCN &cutoff_scn) override
  {
    end_scn_ = end_scn;
    allow_gc = false;
    cutoff_scn.reset();
    if (OB_SUCCESS == result_) {
      cutoff_scn = begin_;
      allow_gc = cutoff_scn == end_scn;
    }
    return result_;
  }
};

TEST(TestObGarbageCollector, candidate_servers)
{
  MockRetentionGCLeader service;
  PalfStat stat;
  ObLSGCRequest request;
  ObArray<ObAddr> servers;
  const ObAddr requester(ObAddr::IPV4, "127.0.0.1", 2882);
  const ObAddr member(ObAddr::IPV4, "127.0.0.1", 2883);
  const ObAddr learner(ObAddr::IPV4, "127.0.0.1", 2884);
  const ObAddr retained(ObAddr::IPV4, "127.0.0.1", 2885);
  stat.log_proposal_id_ = 10;
  stat.mode_version_ = 10;
  stat.config_version_ = MockRetentionGCLeader::version(10, 1);
  stat.role_ = LEADER;
  ASSERT_EQ(OB_SUCCESS, stat.paxos_member_list_.add_server(requester));
  ASSERT_EQ(OB_SUCCESS, stat.paxos_member_list_.add_server(member));
  ASSERT_EQ(OB_SUCCESS, stat.learner_list_.add_server(learner));
  ASSERT_FALSE(service.config_version_.is_valid());
  ASSERT_EQ(OB_SUCCESS, service.update_leader_state_(stat));
  ASSERT_EQ(OB_SUCCESS, request.init(
      retained, ObLSID(1001), SCN::plus(SCN::min_scn(), 20), SCN::plus(SCN::min_scn(), 100),
      stat.config_version_, 10, LSN(100), LSN(200), INVALID_GC_TOKEN));
  ASSERT_EQ(OB_SUCCESS, service.seed_hold(request, 1000));
  ASSERT_EQ(OB_SUCCESS, servers.push_back(retained));
  ASSERT_EQ(OB_SUCCESS, service.build_candidate_servers_(stat, servers));
  ASSERT_EQ(3, servers.count());
  ASSERT_EQ(requester, servers.at(0));
  ASSERT_EQ(member, servers.at(1));
  ASSERT_EQ(learner, servers.at(2));
  ASSERT_EQ(OB_SUCCESS, service.prepare_refresh_(stat, 1000, servers));
  ASSERT_EQ(3, service.member_replica_caches_.count());
  ASSERT_EQ(1, service.retained_replica_caches_.count());
  ASSERT_EQ(3, servers.count());
  for (int64_t i = 0; i < service.member_replica_caches_.count(); ++i) {
    const ObLSGCReplicaCache &cache = service.member_replica_caches_.at(i);
    ASSERT_EQ(cache.server_ != retained,
        stat.paxos_member_list_.contains(cache.server_) || stat.learner_list_.contains(cache.server_));
    ASSERT_EQ(0, cache.last_refresh_time_);
  }

  // Registering retained replicas does not expand the member query batch.
  service.member_replica_caches_.at(0).begin_scn_ = SCN::plus(SCN::min_scn(), 20);
  service.member_replica_caches_.at(0).end_scn_ = SCN::plus(SCN::min_scn(), 100);
  service.member_replica_caches_.at(0).last_refresh_time_ = 1000;
  const ObAddr other_retained(ObAddr::IPV4, "127.0.0.1", 2886);
  ASSERT_EQ(OB_SUCCESS, request.init(
      other_retained, ObLSID(1001), SCN::plus(SCN::min_scn(), 20), SCN::plus(SCN::min_scn(), 100),
      stat.config_version_, 10, LSN(100), LSN(200), INVALID_GC_TOKEN));
  ASSERT_EQ(OB_SUCCESS, service.seed_hold(request, 1050));
  ASSERT_EQ(OB_SUCCESS, service.prepare_refresh_(stat, 1050, servers));
  ASSERT_EQ(3, service.member_replica_caches_.count());
  ASSERT_EQ(2, service.retained_replica_caches_.count());
  ASSERT_EQ(3, servers.count());
  ASSERT_EQ(requester, servers.at(0));
  ASSERT_EQ(member, servers.at(1));
  ASSERT_EQ(learner, servers.at(2));
  ASSERT_EQ(SCN::plus(SCN::min_scn(), 20), service.member_replica_caches_.at(0).begin_scn_);
  ASSERT_EQ(SCN::plus(SCN::min_scn(), 100), service.member_replica_caches_.at(0).end_scn_);
  ASSERT_EQ(1000, service.member_replica_caches_.at(0).last_refresh_time_);

  // HOLD joins as a learner while an ordinary member leaves in the same proposal.
  ASSERT_EQ(OB_SUCCESS, stat.learner_list_.add_server(retained));
  ASSERT_EQ(OB_SUCCESS, stat.paxos_member_list_.remove_server(requester));
  ASSERT_EQ(OB_SUCCESS, stat.config_version_.inc_update_version(10));
  ASSERT_EQ(OB_SUCCESS, service.prepare_refresh_(stat, 1100, servers));
  ASSERT_EQ(3, service.member_replica_caches_.count());
  ASSERT_TRUE(service.retained_replica_caches_.empty());
  ASSERT_EQ(3, servers.count());
  ASSERT_EQ(0, service.last_request_time_);
  ASSERT_EQ(stat.config_version_, service.config_version_);

  // The new request reactivates refresh; all member caches are newly created.
  ASSERT_EQ(OB_SUCCESS, request.init(
      requester, ObLSID(1001), SCN::plus(SCN::min_scn(), 20), SCN::plus(SCN::min_scn(), 100),
      stat.config_version_, 10, LSN(100), LSN(200), INVALID_GC_TOKEN));
  ASSERT_EQ(OB_SUCCESS, service.seed_hold(request, 1200));
  ASSERT_EQ(OB_SUCCESS, service.prepare_refresh_(stat, 1200, servers));
  ASSERT_EQ(3, service.member_replica_caches_.count());
  ASSERT_EQ(1, service.retained_replica_caches_.count());
  ASSERT_EQ(3, servers.count());
  for (int64_t i = 0; i < service.member_replica_caches_.count(); ++i) {
    const ObLSGCReplicaCache &cache = service.member_replica_caches_.at(i);
    ASSERT_EQ(cache.server_ != requester,
        stat.paxos_member_list_.contains(cache.server_) || stat.learner_list_.contains(cache.server_));
    ASSERT_EQ(0, cache.last_refresh_time_);
  }
}

TEST(TestObGarbageCollector, approval_excludes_previously_approved_replica)
{
  MockRetentionGCLeader service;
  service.check_stat_locked_ = true;
  ASSERT_EQ(OB_SUCCESS, service.update_leader_state_(service.leader_stat_));
  const ObAddr a(ObAddr::IPV4, "127.0.0.1", 2882);
  const ObAddr b(ObAddr::IPV4, "127.0.0.1", 2883);
  const int64_t now = ObTimeUtility::current_monotonic_time();
  ObLSGCRequest request;
  bool allow_gc = false;
  ASSERT_EQ(OB_SUCCESS, request.init(a, ObLSID(1001), SCN::plus(SCN::min_scn(), 20), SCN::plus(SCN::min_scn(), 100),
      MockRetentionGCLeader::version(10, 1), 10, LSN(100), LSN(200), INVALID_GC_TOKEN));
  ASSERT_EQ(OB_SUCCESS, service.seed_hold(request, now));
  ObLSGCReplicaCache cache(b);
  cache.begin_scn_ = SCN::min_scn();
  cache.end_scn_ = SCN::plus(SCN::min_scn(), 200);
  cache.last_refresh_time_ = now;
  ASSERT_EQ(OB_SUCCESS, service.retained_replica_caches_.push_back(cache));
  ASSERT_EQ(OB_SUCCESS, service.mark_approved(b));
  service.erase_replica_cache_(b);
  {
    lib::ObMutexGuard guard(service.lock_);
    ASSERT_EQ(OB_SUCCESS, service.check_gc_coverage_(request, 1, 10, allow_gc));
  }
  ASSERT_FALSE(allow_gc);
  ASSERT_EQ(1, service.approved_count());
  ASSERT_EQ(b, service.approved_server());
  ASSERT_EQ(1, service.retained_replica_caches_.count());
  ASSERT_EQ(a, service.retained_replica_caches_.at(0).server_);

  // An older snapshot must leave newer caches and approvals untouched.
  PalfStat old_stat = service.leader_stat_;
  ASSERT_EQ(OB_SUCCESS, service.leader_stat_.config_version_.inc_update_version(10));
  ASSERT_EQ(OB_SUCCESS, service.update_leader_state_(service.leader_stat_));
  ASSERT_EQ(OB_SUCCESS, request.init(a, ObLSID(1001), SCN::plus(SCN::min_scn(), 20), SCN::plus(SCN::min_scn(), 100),
      service.leader_stat_.config_version_, 10, LSN(100), LSN(200), INVALID_GC_TOKEN));
  ASSERT_EQ(OB_SUCCESS, service.seed_hold(request, now));
  ASSERT_EQ(OB_SUCCESS, service.mark_approved(b));
  ASSERT_EQ(OB_EAGAIN, service.update_leader_state_(old_stat));
  ASSERT_EQ(service.leader_stat_.config_version_, service.config_version_);
  ASSERT_EQ(1, service.retained_replica_caches_.count());
  ASSERT_EQ(1, service.approved_count());
  old_stat = service.leader_stat_;
  --old_stat.log_proposal_id_;
  old_stat.config_version_ = MockRetentionGCLeader::version(old_stat.log_proposal_id_, 2);
  ASSERT_EQ(OB_EAGAIN, service.update_leader_state_(old_stat));
  ASSERT_EQ(service.leader_stat_.config_version_, service.config_version_);
  service.reset();
  ASSERT_FALSE(service.config_version_.is_valid());
  ASSERT_EQ(OB_SUCCESS, service.update_leader_state_(service.leader_stat_));
  ASSERT_EQ(service.leader_stat_.config_version_, service.config_version_);
}

TEST(TestObGarbageCollector, log_fetch_sources_cache_and_expiry)
{
  MockRetentionGCLeader service;
  service.check_stat_locked_ = true;
  LogGetFetchSourcesResp response;
  const ObAddr member(ObAddr::IPV4, "127.0.0.1", 2882);
  const ObAddr retained(ObAddr::IPV4, "127.0.0.1", 2883);
  const int64_t now = ObTimeUtility::current_monotonic_time();
  ObArray<ObAddr> servers;
  ObArray<PalfStat> stats;
  PalfStat stat;
  ObLSGCRequest request;
  stat.mode_version_ = 10;
  service.config_version_ = service.leader_stat_.config_version_;
  service.mode_version_ = 10;
  ASSERT_EQ(OB_SUCCESS, service.member_replica_caches_.push_back(ObLSGCReplicaCache(member)));
  ASSERT_EQ(OB_SUCCESS, servers.push_back(member));
  ASSERT_EQ(OB_SUCCESS, servers.push_back(retained));
  stat.self_ = member;
  stat.begin_scn_ = SCN::min_scn();
  stat.end_scn_ = SCN::plus(SCN::min_scn(), 100);
  stat.begin_lsn_ = LSN(600);
  stat.end_lsn_ = LSN(1000);
  stat.max_lsn_ = LSN(1200);
  ASSERT_EQ(OB_SUCCESS, stats.push_back(stat));
  stat.self_ = retained;
  stat.begin_lsn_ = LSN(100);
  ASSERT_EQ(OB_SUCCESS, stats.push_back(stat));
  ASSERT_EQ(OB_SUCCESS, service.apply_refresh_(service.config_version_, 10, servers, stats, now));
  ASSERT_EQ(OB_SUCCESS, request.init(retained, ObLSID(1001), SCN::min_scn(),
      stat.end_scn_, service.config_version_, 10, stat.begin_lsn_, stat.end_lsn_, INVALID_GC_TOKEN));
  ASSERT_EQ(OB_SUCCESS, service.seed_hold(request, now));
  ASSERT_EQ(OB_SUCCESS, service.get_log_fetch_sources(response));
  ASSERT_EQ(2, response.sources_.count());
  ASSERT_TRUE(response.is_valid());
  ASSERT_EQ(retained, response.sources_.at(1).server_);
  ASSERT_EQ(LSN(100), response.sources_.at(1).begin_lsn_);
  ASSERT_EQ(LSN(1000), response.sources_.at(1).end_lsn_);
  ASSERT_TRUE(response.sources_.at(1).is_valid());
  ASSERT_EQ(0, service.calls_);
  ASSERT_EQ(now, service.last_request_time_);
  ASSERT_EQ(now, service.retained_replica_caches_.at(0).last_refresh_time_);

  // Failed sampling and subsequent queries must not renew the cached stat.
  stats.reuse();
  ASSERT_EQ(OB_SUCCESS, service.apply_refresh_(service.config_version_, 10, servers, stats, now + 1));
  ASSERT_EQ(now, service.retained_replica_caches_.at(0).last_refresh_time_);
  service.retained_replica_caches_.at(0).last_refresh_time_ = now - RetentionGCLeader::CACHE_TTL;
  ASSERT_EQ(OB_SUCCESS, service.get_log_fetch_sources(response));
  ASSERT_EQ(1, response.sources_.count());
  ASSERT_EQ(member, response.sources_.at(0).server_);
  service.member_replica_caches_.at(0).last_refresh_time_ = now - RetentionGCLeader::CACHE_TTL;
  ASSERT_EQ(OB_EAGAIN, service.get_log_fetch_sources(response));
  ASSERT_TRUE(response.sources_.empty());

  // A successful new sample restores the retired source, until GC is approved.
  ASSERT_EQ(OB_SUCCESS, stats.push_back(stat));
  ASSERT_EQ(OB_SUCCESS, service.seed_hold(request, now));
  ASSERT_EQ(OB_SUCCESS, service.get_log_fetch_sources(response));
  ASSERT_EQ(1, response.sources_.count());
  ASSERT_EQ(OB_SUCCESS, service.mark_approved(retained));
  ASSERT_EQ(OB_EAGAIN, service.get_log_fetch_sources(response));
  ASSERT_TRUE(response.sources_.empty());
  service.gc_requests_.reuse();
  service.leader_stat_.config_version_ = MockRetentionGCLeader::version(11, 1);
  ASSERT_EQ(OB_EAGAIN, service.get_log_fetch_sources(response));
  ASSERT_EQ(OB_SUCCESS, service.update_leader_state_(service.leader_stat_));
  ASSERT_TRUE(service.retained_replica_caches_.empty());
  service.leader_stat_.role_ = FOLLOWER;
  ASSERT_EQ(OB_NOT_MASTER, service.get_log_fetch_sources(response));
  ASSERT_TRUE(response.sources_.empty());
}

TEST(TestObGarbageCollector, mode_change_invalidates_coverage_and_fetch_sources)
{
  MockRetentionGCLeader leader;
  const ObAddr member(ObAddr::IPV4, "127.0.0.1", 2883);
  const ObAddr retired(ObAddr::IPV4, "127.0.0.1", 2884);
  const ObAddr approved(ObAddr::IPV4, "127.0.0.1", 2885);
  const ObAddr requester(ObAddr::IPV4, "127.0.0.1", 2886);
  leader.leader_stat_.log_proposal_id_ = 11;
  leader.leader_stat_.config_version_ = MockRetentionGCLeader::version(11, 1);
  ASSERT_EQ(OB_SUCCESS, leader.leader_stat_.paxos_member_list_.add_server(member));
  ASSERT_EQ(OB_SUCCESS, leader.update_leader_state_(leader.leader_stat_));
  const PalfStat old_stat = leader.leader_stat_;
  ObLSGCRequest report;
  ObLSGCRequest request;
  ObArray<ObAddr> servers;
  ObArray<PalfStat> stats;
  LogGetFetchSourcesResp response;
  const int64_t now = ObTimeUtility::current_monotonic_time();
  ASSERT_EQ(OB_SUCCESS, report.init(retired, ObLSID(1001), leader.begin_, leader.end_,
      leader.config_version_, 10, LSN(100), LSN(200), INVALID_GC_TOKEN));
  ASSERT_EQ(OB_SUCCESS, leader.seed_hold(report, now));
  ASSERT_EQ(OB_SUCCESS, leader.prepare_refresh_(leader.leader_stat_, now, servers));
  ASSERT_EQ(OB_SUCCESS, leader.get_replica_palf_stats_(servers, stats));
  ASSERT_EQ(OB_SUCCESS, leader.apply_refresh_(leader.config_version_, 10, servers, stats, now));
  ASSERT_EQ(OB_SUCCESS, leader.mark_approved(approved));
  ASSERT_EQ(OB_SUCCESS, request.init(requester, ObLSID(1001), leader.begin_, leader.end_,
      leader.config_version_, 10, LSN(100), LSN(200), INVALID_GC_TOKEN));
  bool covered = false;
  ASSERT_EQ(OB_SUCCESS, leader.check_gc_coverage_(request, 2, 10, covered));
  ASSERT_TRUE(covered);
  ASSERT_EQ(OB_SUCCESS, leader.get_log_fetch_sources(response));
  ASSERT_EQ(2, response.sources_.count());

  // Applied mode advances while both the configuration and the proposal id stay fixed.
  leader.leader_stat_.mode_version_ = 11;
  ASSERT_EQ(old_stat.config_version_, leader.leader_stat_.config_version_);
  ASSERT_EQ(old_stat.log_proposal_id_, leader.leader_stat_.log_proposal_id_);
  ASSERT_EQ(OB_EAGAIN, leader.check_gc_coverage_(request, 2, 11, covered));
  ASSERT_FALSE(covered);
  ASSERT_EQ(OB_EAGAIN, leader.get_log_fetch_sources(response));
  ASSERT_TRUE(response.sources_.empty());
  ASSERT_EQ(OB_SUCCESS, leader.update_leader_state_(leader.leader_stat_));
  ASSERT_TRUE(leader.member_replica_caches_.empty());
  ASSERT_TRUE(leader.retained_replica_caches_.empty());
  ASSERT_TRUE(leader.approved_count() == 0);
  ASSERT_EQ(OB_STATE_NOT_MATCH, leader.seed_hold(report, now + 1));
  ASSERT_TRUE(leader.retained_replica_caches_.empty());
  ASSERT_EQ(OB_EAGAIN, leader.update_leader_state_(old_stat));
  ASSERT_EQ(11, leader.mode_version_);
  ASSERT_EQ(OB_EAGAIN, leader.apply_refresh_(leader.config_version_, 10, servers, stats, now + 1));
  ASSERT_EQ(OB_SUCCESS, leader.prepare_refresh_(leader.leader_stat_, now + 1, servers));
  ASSERT_EQ(OB_SUCCESS, leader.apply_refresh_(leader.config_version_, 11, servers, stats, now + 1));
  ASSERT_FALSE(leader.member_replica_caches_.at(0).is_valid());
  ASSERT_EQ(OB_EAGAIN, leader.get_log_fetch_sources(response));
}

TEST(TestObGarbageCollector, fetch_sources_rechecks_mode_after_collecting_ranges)
{
  MockRetentionGCLeader leader;
  ObLSGCRequest report;
  LogGetFetchSourcesResp response;
  ASSERT_EQ(OB_SUCCESS, leader.update_leader_state_(leader.leader_stat_));
  ASSERT_EQ(OB_SUCCESS, report.init(GCTX.self_addr(), ObLSID(1001), leader.begin_, leader.end_,
      leader.config_version_, 10, LSN(100), LSN(200), INVALID_GC_TOKEN));
  ASSERT_EQ(OB_SUCCESS, leader.seed_hold(report, ObTimeUtility::current_monotonic_time()));
  leader.on_stat_ = [](MockRetentionGCLeader *self) {
    self->on_stat_ = NULL;
    ++self->leader_stat_.mode_version_;
  };
  ASSERT_EQ(OB_EAGAIN, leader.get_log_fetch_sources(response));
  ASSERT_TRUE(response.sources_.empty());
  ASSERT_EQ(10, leader.mode_version_);
  ASSERT_EQ(11, leader.leader_stat_.mode_version_);
}

TEST(TestObGarbageCollector, mode_change_discards_inflight_member_reply)
{
  MockRetentionGCLeader leader;
  ObLS *ls = OB_NEW(ObLS, "TestGC");
  ASSERT_NE(nullptr, ls);
  DEFER(OB_DELETE(ObLS, "TestGC", ls));
  ASSERT_EQ(OB_SUCCESS, leader.init(ls));
  const ObAddr member(ObAddr::IPV4, "127.0.0.1", 2883);
  ASSERT_EQ(OB_SUCCESS, leader.leader_stat_.paxos_member_list_.add_server(member));
  leader.leader_stat_.log_proposal_id_ = 11;
  leader.leader_stat_.config_version_ = MockRetentionGCLeader::version(11, 1);
  ASSERT_EQ(OB_SUCCESS, leader.update_leader_state_(leader.leader_stat_));
  ObLSGCRequest report;
  ASSERT_EQ(OB_SUCCESS, report.init(GCTX.self_addr(), ObLSID(1001), leader.begin_, leader.end_,
      leader.config_version_, 10, LSN(100), LSN(200), INVALID_GC_TOKEN));
  ASSERT_EQ(OB_SUCCESS, leader.seed_hold(report, ObTimeUtility::current_monotonic_time()));
  leader.on_refresh_ = [](MockRetentionGCLeader *self) {
    lib::ObMutexGuard guard(self->lock_);
    self->leader_stat_.mode_version_ = 11;
    ASSERT_EQ(OB_SUCCESS, self->update_leader_state_(self->leader_stat_));
    ObLSGCRequest report;
    ASSERT_EQ(OB_SUCCESS, report.init(GCTX.self_addr(), ObLSID(1001), self->begin_, self->end_,
        self->config_version_, 11, LSN(150), LSN(200), INVALID_GC_TOKEN));
    ASSERT_EQ(OB_SUCCESS, self->seed_hold(report, ObTimeUtility::current_monotonic_time()));
  };
  ASSERT_EQ(OB_EAGAIN, leader.refresh_cache());
  ASSERT_TRUE(leader.unlocked_);
  ASSERT_EQ(11, leader.mode_version_);
  ASSERT_EQ(MockRetentionGCLeader::version(11, 1), leader.config_version_);
  ASSERT_TRUE(leader.member_replica_caches_.empty());
  ASSERT_EQ(1, leader.retained_replica_caches_.count());
  ASSERT_EQ(LSN(150), leader.retained_replica_caches_.at(0).get_begin_lsn());
  leader.on_refresh_ = NULL;
  ASSERT_EQ(OB_SUCCESS, leader.refresh_cache());
  ASSERT_TRUE(leader.member_replica_caches_.at(0).is_valid());
  ASSERT_EQ(LSN(150), leader.retained_replica_caches_.at(0).get_begin_lsn());
}

TEST(TestObGarbageCollector, log_fetch_source_rpc_payload)
{
  LogGetFetchSourcesReq request;
  LogGetFetchSourcesReq decoded_request;
  LogGetFetchSourcesResp response;
  LogGetFetchSourcesResp decoded_response;
  LogFetchSource source;
  char buffer[1024];
  int64_t pos = 0;
  ASSERT_FALSE(request.is_valid());
  request.tenant_id_ = 1002;
  request.ls_id_ = ObLSID(1001);
  ASSERT_FALSE(request.is_valid());
  request.src_ = ObAddr(ObAddr::IPV4, "127.0.0.1", 2881);
  ASSERT_TRUE(request.is_valid());
  ASSERT_EQ(OB_SUCCESS, request.serialize(buffer, sizeof(buffer), pos));
  const int64_t request_len = pos;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded_request.deserialize(buffer, request_len, pos));
  ASSERT_EQ(request.src_, decoded_request.src_);
  ASSERT_EQ(request.tenant_id_, decoded_request.tenant_id_);
  ASSERT_EQ(request.ls_id_, decoded_request.ls_id_);
  source.server_ = ObAddr(ObAddr::IPV4, "127.0.0.1", 2882);
  source.begin_lsn_ = LSN(100);
  source.end_lsn_ = LSN(100);
  ASSERT_FALSE(source.is_valid());
  source.end_lsn_ = LSN(1000);
  ASSERT_TRUE(source.is_valid());
  ASSERT_EQ(OB_SUCCESS, response.sources_.push_back(source));
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, response.serialize(buffer, sizeof(buffer), pos));
  const int64_t response_len = pos;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded_response.deserialize(buffer, response_len, pos));
  ASSERT_TRUE(decoded_response.is_valid());
  ASSERT_EQ(1, decoded_response.sources_.count());
  ASSERT_EQ(source.server_, decoded_response.sources_.at(0).server_);
  ASSERT_EQ(source.begin_lsn_, decoded_response.sources_.at(0).begin_lsn_);
  ASSERT_EQ(source.end_lsn_, decoded_response.sources_.at(0).end_lsn_);

  LogRequestHandler handler;
  ObTenantBase tenant(1002);
  ObTenantEnv::set_tenant(&tenant);
  DEFER(ObTenantEnv::set_tenant(NULL));
  request.tenant_id_ = 1003;
  ASSERT_EQ(OB_INVALID_ARGUMENT, handler.handle_sync_request(request, response));
  ASSERT_TRUE(response.sources_.empty());
  ASSERT_FALSE(response.is_valid());
  request.tenant_id_ = 1002;
  request.src_.reset();
  ASSERT_EQ(OB_INVALID_ARGUMENT, handler.handle_sync_request(request, response));
}

TEST(TestObGarbageCollector, log_source_route_add_if_absent)
{
  LSSvrList sql_sources;
  ObLSRouterValue route;
  BlackList blacklist;
  ObArray<ObAddr> locate_servers;
  const ObAddr retained(ObAddr::IPV4, "127.0.0.1", 2882);
  const ObAddr member(ObAddr::IPV4, "127.0.0.1", 2883);
  ASSERT_EQ(OB_SUCCESS, sql_sources.add_server_or_update(member, LSN(600), LSN(1000), 1, true));
  ASSERT_EQ(OB_SUCCESS, sql_sources.add_server_if_absent(member, LSN(100), LSN(800), 0, true));
  ASSERT_EQ(OB_SUCCESS, sql_sources.add_server_if_absent(retained, LSN(100), LSN(1200), 0, false));
  ASSERT_EQ(2, sql_sources.count());
  ASSERT_EQ(LSN(600), sql_sources.svr_items_.at(0).log_ranges_.start_lsn_);
  ASSERT_EQ(LSN(1000), sql_sources.svr_items_.at(0).log_ranges_.end_lsn_);
  route.refresh_ls_svr_list(sql_sources);
  ASSERT_EQ(2, route.get_server_count());
  ObAddr selected;
  ASSERT_EQ(OB_SUCCESS, route.ls_svr_list_.next_server(LSN(1100), blacklist, selected));
  ASSERT_EQ(retained, selected);

  // A new snapshot must not restore RPC candidates omitted by the leader.
  sql_sources.reset();
  ASSERT_EQ(OB_SUCCESS, sql_sources.add_server_or_update(member, LSN(600), LSN(1000), 1, true));
  route.refresh_ls_svr_list(sql_sources);
  ASSERT_EQ(1, route.get_server_count());
  ASSERT_EQ(OB_SUCCESS, route.get_server_array_for_locate_start_lsn(locate_servers));
  ASSERT_EQ(1, locate_servers.count());
  ASSERT_EQ(member, locate_servers.at(0));
  ASSERT_FALSE(route.ls_svr_list_.need_switch_server(ObLSRouterKey(), LSN(300), blacklist, member));
  ASSERT_EQ(OB_SUCCESS, route.ls_svr_list_.next_server(LSN(300), blacklist, selected));
  ASSERT_EQ(member, selected);
}

TEST(TestObGarbageCollector, hold_range_cache_ttl)
{
  MockRetentionGCLeader service;
  const ObAddr server(ObAddr::IPV4, "127.0.0.1", 2882);
  const ObAddr requester(ObAddr::IPV4, "127.0.0.1", 2883);
  ObLSGCRequest request;
  ObArray<ObAddr> servers;
  ObArray<ObLSGCReplicaCache> ranges;
  ASSERT_EQ(OB_SUCCESS, service.update_leader_state_(service.leader_stat_));
  ASSERT_EQ(OB_SUCCESS, request.init(server, ObLSID(1001), SCN::min_scn(),
      SCN::plus(SCN::min_scn(), 100), service.config_version_, 10, LSN(100), LSN(200), INVALID_GC_TOKEN));
  ASSERT_EQ(OB_SUCCESS, service.seed_hold(request, 1000));
  ASSERT_EQ(OB_SUCCESS, service.collect_replica_ranges_(requester, 1000, ranges));
  ASSERT_EQ(1, ranges.count());
  ASSERT_EQ(OB_SUCCESS, service.prepare_refresh_(service.leader_stat_, 1000, servers));
  ASSERT_TRUE(servers.empty());
  ASSERT_EQ(OB_SUCCESS, request.init(request.src_, request.get_ls_id(), request.get_cutoff_scn(),
      request.get_end_scn(), request.get_config_version(), request.get_mode_version(), LSN(100), LSN(200), INVALID_GC_TOKEN));
  ASSERT_EQ(OB_SUCCESS, service.seed_hold(request, 1100));
  ASSERT_EQ(OB_SUCCESS, service.collect_replica_ranges_(requester, 1100, ranges));
  ASSERT_EQ(1, ranges.count());
  ASSERT_EQ(1100, ranges.at(0).get_last_refresh_time());
  ASSERT_EQ(SCN::plus(SCN::min_scn(), 100), ranges.at(0).get_end_scn());
  bool covered = false;
  ASSERT_EQ(OB_SUCCESS, service.check_log_coverage(ranges, SCN::min_scn(),
      SCN::plus(SCN::min_scn(), 150), 1, covered));
  ASSERT_FALSE(covered);
  ASSERT_EQ(OB_SUCCESS, service.collect_replica_ranges_(server, 1100, ranges));
  ASSERT_TRUE(ranges.empty());

  // A restarted replica can report a new sampled boundary.
  ASSERT_EQ(OB_SUCCESS, request.init(server, ObLSID(1001), SCN::plus(SCN::min_scn(), 50),
      SCN::plus(SCN::min_scn(), 300), service.config_version_, 10, LSN(150), LSN(400), INVALID_GC_TOKEN));
  ASSERT_EQ(OB_SUCCESS, service.seed_hold(request, 1200));
  ASSERT_EQ(OB_SUCCESS, service.collect_replica_ranges_(requester, 1200, ranges));
  ASSERT_EQ(1200, ranges.at(0).get_last_refresh_time());
  ASSERT_EQ(SCN::plus(SCN::min_scn(), 50), ranges.at(0).get_begin_scn());
  ASSERT_EQ(SCN::plus(SCN::min_scn(), 300), ranges.at(0).get_end_scn());
  ASSERT_EQ(LSN(400), ranges.at(0).get_end_lsn());
  ASSERT_EQ(OB_SUCCESS, service.collect_replica_ranges_(
      requester, 1200 + RetentionGCLeader::CACHE_TTL - 1, ranges));
  ASSERT_EQ(1, ranges.count());
  ASSERT_EQ(OB_SUCCESS, service.collect_replica_ranges_(
      requester, 1200 + RetentionGCLeader::CACHE_TTL, ranges));
  ASSERT_TRUE(ranges.empty());

  // Missing reports do not renew the last sample.
  ASSERT_EQ(1200, service.retained_replica_caches_.at(0).get_last_refresh_time());
  ASSERT_EQ(OB_SUCCESS, service.leader_stat_.config_version_.inc_update_version(10));
  ASSERT_EQ(OB_SUCCESS, service.update_leader_state_(service.leader_stat_));
  ASSERT_EQ(OB_SUCCESS, request.init(request.src_, request.get_ls_id(), request.get_cutoff_scn(),
      request.get_end_scn(), request.get_config_version(), request.get_mode_version(), LSN(100), LSN(200), INVALID_GC_TOKEN));
  ASSERT_EQ(OB_STATE_NOT_MATCH, service.seed_hold(request, 1400));
  ASSERT_TRUE(service.retained_replica_caches_.empty());
  ASSERT_EQ(0, service.calls_);
}

TEST(TestObGarbageCollector, retained_end_bound_across_migrations)
{
  MockRetentionGCLeader leader;
  const ObAddr retained(ObAddr::IPV4, "127.0.0.1", 2882);
  const ObAddr requester(ObAddr::IPV4, "127.0.0.1", 2883);
  const ObAddr member(ObAddr::IPV4, "127.0.0.1", 2884);
  const SCN retained_end = SCN::plus(SCN::min_scn(), 100);
  const SCN requested_end = SCN::plus(SCN::min_scn(), 150);
  ObLSGCRequest request;
  ObArray<ObAddr> servers;
  ObArray<PalfStat> stats;
  ObArray<ObLSGCReplicaCache> ranges;
  bool covered = false;
  for (int64_t round = 0; round < 2; ++round) {
    leader.leader_stat_.config_version_ = MockRetentionGCLeader::version(10, round + 1);
    ASSERT_EQ(OB_SUCCESS, leader.update_leader_state_(leader.leader_stat_));
    ASSERT_EQ(OB_SUCCESS, request.init(retained, ObLSID(1001), SCN::min_scn(),
        retained_end, leader.leader_stat_.config_version_, 10, LSN(100), LSN(200 + round * 40), INVALID_GC_TOKEN));
    ASSERT_EQ(OB_SUCCESS, leader.seed_hold(request, 1050 + round * 100));
    ASSERT_EQ(OB_SUCCESS, leader.collect_replica_ranges_(requester, 1050 + round * 100, ranges));
    ASSERT_EQ(1, ranges.count());
    ASSERT_EQ(retained_end, ranges.at(0).get_end_scn());
    ASSERT_EQ(LSN(200 + round * 40), ranges.at(0).get_end_lsn());
    ASSERT_EQ(OB_SUCCESS, leader.check_log_coverage(
        ranges, SCN::plus(SCN::min_scn(), 50), SCN::plus(SCN::min_scn(), 80), 1, covered));
    ASSERT_TRUE(covered);
    ASSERT_EQ(OB_SUCCESS, leader.check_log_coverage(
        ranges, SCN::plus(SCN::min_scn(), 120), requested_end, 1, covered));
    ASSERT_FALSE(covered);
  }

  // Another replica can cover this segment for K=1, but not for K=2.
  PalfStat member_stat;
  member_stat.self_ = member;
  member_stat.begin_scn_ = SCN::plus(SCN::min_scn(), 120);
  member_stat.end_scn_ = SCN::plus(SCN::min_scn(), 180);
  member_stat.begin_lsn_ = LSN(100);
  member_stat.end_lsn_ = LSN(200);
  ObLSGCReplicaCache member_cache(member);
  ASSERT_EQ(OB_SUCCESS, member_cache.init(member_stat.self_, member_stat.begin_scn_, member_stat.end_scn_,
      member_stat.begin_lsn_, member_stat.end_lsn_, 1200));
  ASSERT_EQ(OB_SUCCESS, ranges.push_back(member_cache));
  ASSERT_EQ(OB_SUCCESS, leader.check_log_coverage(
      ranges, SCN::plus(SCN::min_scn(), 120), requested_end, 1, covered));
  ASSERT_TRUE(covered);
  ASSERT_EQ(OB_SUCCESS, leader.check_log_coverage(
      ranges, SCN::plus(SCN::min_scn(), 120), requested_end, 2, covered));
  ASSERT_FALSE(covered);
}

TEST(TestObGarbageCollector, batch_range_refresh)
{
  MockRetentionGCLeader service;
  ObLS *ls = OB_NEW(ObLS, "TestGC");
  ASSERT_NE(nullptr, ls);
  DEFER(OB_DELETE(ObLS, "TestGC", ls));
  ASSERT_EQ(OB_SUCCESS, service.init(ls));
  ASSERT_EQ(OB_SUCCESS, service.update_leader_state_(service.leader_stat_));
  ObAddr server;
  ObArray<ObAddr> servers;
  for (int64_t i = 0; i < 3; ++i) {
    ASSERT_TRUE(server.set_ip_addr("127.0.0.1", 2882 + i));
    ASSERT_EQ(OB_SUCCESS, servers.push_back(server));
    ASSERT_EQ(OB_SUCCESS, i < 2 ? service.leader_stat_.paxos_member_list_.add_server(server)
        : service.leader_stat_.learner_list_.add_server(server));
  }
  const ObAddr retained(ObAddr::IPV4, "127.0.0.1", 2885);
  ObLSGCRequest request;
  const int64_t now = ObTimeUtility::current_monotonic_time();
  ASSERT_EQ(OB_SUCCESS, request.init(retained, ObLSID(1001), SCN::min_scn(), service.end_,
      service.config_version_, 10, LSN(100), LSN(200), INVALID_GC_TOKEN));
  ASSERT_EQ(OB_SUCCESS, service.seed_hold(request, now));
  ASSERT_EQ(OB_SUCCESS, service.result_codes_.push_back(OB_SUCCESS));
  ASSERT_EQ(OB_SUCCESS, service.result_codes_.push_back(OB_TIMEOUT));
  ASSERT_EQ(OB_SUCCESS, service.result_codes_.push_back(OB_LS_NOT_EXIST));
  ASSERT_EQ(OB_SUCCESS, service.refresh_cache());
  ASSERT_TRUE(service.unlocked_);
  ASSERT_EQ(3, service.last_batch_size_);
  ASSERT_EQ(3, service.member_replica_caches_.count());
  ASSERT_GE(service.member_replica_caches_.at(0).last_refresh_time_, now);
  ASSERT_EQ(0, service.member_replica_caches_.at(1).last_refresh_time_);
  ASSERT_EQ(0, service.member_replica_caches_.at(2).last_refresh_time_);
  ASSERT_EQ(now, service.retained_replica_caches_.at(0).last_refresh_time_);

  ObArray<PalfStat> stats;
  PalfStat stat;
  stat.mode_version_ = 10;
  stat.begin_scn_ = SCN::plus(SCN::min_scn(), 50);
  stat.end_scn_ = SCN::plus(SCN::min_scn(), 80);
  stat.begin_lsn_ = LSN(150);
  stat.end_lsn_ = LSN(180);
  stat.self_ = servers.at(2);
  ASSERT_EQ(OB_SUCCESS, stats.push_back(stat));
  stat.self_ = servers.at(0);
  ASSERT_EQ(OB_SUCCESS, stats.push_back(stat));
  // Even an unsolicited successful query cannot overwrite a retained self-report.
  stat.self_ = retained;
  ASSERT_EQ(OB_SUCCESS, servers.push_back(retained));
  ASSERT_EQ(OB_SUCCESS, stats.push_back(stat));
  ASSERT_EQ(OB_SUCCESS, service.apply_refresh_(service.config_version_, 10, servers, stats, now + 1));
  ASSERT_EQ(stat.begin_scn_, service.member_replica_caches_.at(2).begin_scn_);
  ASSERT_EQ(0, service.member_replica_caches_.at(1).last_refresh_time_);
  ASSERT_EQ(service.begin_, service.retained_replica_caches_.at(0).begin_scn_);
  ASSERT_EQ(now, service.retained_replica_caches_.at(0).last_refresh_time_);
  service.batch_result_ = OB_ALLOCATE_MEMORY_FAILED;
  ASSERT_EQ(OB_ALLOCATE_MEMORY_FAILED, service.refresh_cache());
  ASSERT_EQ(now + 1, service.member_replica_caches_.at(0).last_refresh_time_);
  service.batch_result_ = OB_SUCCESS;
  service.reply_mode_version_ = 9;
  ASSERT_EQ(OB_SUCCESS, service.refresh_cache());
  ASSERT_FALSE(service.member_replica_caches_.at(0).is_valid());
  ASSERT_EQ(0, service.member_replica_caches_.at(0).last_refresh_time_);
  ASSERT_EQ(now, service.retained_replica_caches_.at(0).last_refresh_time_);
}

TEST(TestObGarbageCollector, refresh_activity_window)
{
  MockRetentionGCLeader service;
  ObLSGCRequest request;
  const ObAddr server(ObAddr::IPV4, "127.0.0.1", 2882);
  ObArray<ObAddr> servers;
  ObArray<ObLSGCReplicaCache> ranges;
  const int64_t start = 1000;
  service.config_version_ = MockRetentionGCLeader::version(10, 1);
  service.mode_version_ = 10;
  ASSERT_EQ(OB_SUCCESS, service.leader_stat_.paxos_member_list_.add_server(
      ObAddr(ObAddr::IPV4, "127.0.0.1", 2883)));
  ASSERT_EQ(OB_SUCCESS, request.init(
      server, ObLSID(1001), SCN::plus(SCN::min_scn(), 20), SCN::plus(SCN::min_scn(), 100),
      MockRetentionGCLeader::version(10, 1), 10, LSN(100), LSN(200), INVALID_GC_TOKEN));
  ASSERT_EQ(OB_SUCCESS, service.seed_hold(request, start));
  ASSERT_EQ(start, service.last_request_time_);
  ASSERT_EQ(OB_SUCCESS, service.prepare_refresh_(service.leader_stat_, start, servers));
  ASSERT_EQ(1, servers.count());
  ASSERT_EQ(OB_SUCCESS, service.seed_hold(request, start + 1000000));
  ASSERT_EQ(start + 1000000, service.last_request_time_);
  ASSERT_EQ(OB_SUCCESS, service.prepare_refresh_(
      service.leader_stat_, start + 1000000, servers));
  ASSERT_EQ(1, servers.count());

  const int64_t last_request = start + RetentionGCLeader::ACTIVE_WINDOW - 1;
  ASSERT_EQ(OB_SUCCESS, service.seed_hold(request, last_request));
  ASSERT_EQ(last_request, service.last_request_time_);
  ASSERT_TRUE(service.is_gc_request_within_active_window_(last_request + RetentionGCLeader::ACTIVE_WINDOW - 1));
  const int64_t restart = last_request + RetentionGCLeader::ACTIVE_WINDOW;
  ASSERT_FALSE(service.is_gc_request_within_active_window_(restart));
  ASSERT_EQ(OB_SUCCESS, service.seed_hold(request, restart));
  ASSERT_EQ(restart, service.last_request_time_);
  ASSERT_EQ(restart, service.retained_replica_caches_.at(0).last_refresh_time_);
  ASSERT_EQ(OB_SUCCESS, service.collect_replica_ranges_(ObAddr(), restart, ranges));
  ASSERT_EQ(1, ranges.count());
  ASSERT_EQ(OB_SUCCESS, service.prepare_refresh_(service.leader_stat_, restart, servers));
  ASSERT_EQ(1, servers.count());
  ASSERT_EQ(OB_SUCCESS, service.seed_hold(request, restart + 1));
  ASSERT_EQ(restart + 1, service.last_request_time_);
  ASSERT_EQ(OB_SUCCESS, service.prepare_refresh_(service.leader_stat_, restart + 1, servers));
  ASSERT_EQ(1, servers.count());
  ASSERT_EQ(0, service.calls_);
}

TEST(TestObGarbageCollector, refresh_preserves_new_epoch_requests)
{
  MockRetentionGCLeader service;
  storage::ObLS *ls = OB_NEW(storage::ObLS, "TestGC");
  ASSERT_NE(nullptr, ls);
  DEFER(OB_DELETE(ObLS, "TestGC", ls));
  ASSERT_EQ(OB_SUCCESS, service.init(ls));
  service.config_version_ = MockRetentionGCLeader::version(10, 1);
  service.mode_version_ = 10;
  ASSERT_EQ(OB_SUCCESS, service.leader_stat_.paxos_member_list_.add_server(
      ObAddr(ObAddr::IPV4, "127.0.0.1", 2884)));
  ObLSGCRequest request;
  const ObAddr server(ObAddr::IPV4, "127.0.0.1", 2882);
  ASSERT_EQ(OB_SUCCESS, request.init(
      server, ObLSID(1001), SCN::plus(SCN::min_scn(), 20), SCN::plus(SCN::min_scn(), 100),
      MockRetentionGCLeader::version(10, 1), 10, LSN(100), LSN(200), INVALID_GC_TOKEN));
  ASSERT_EQ(OB_SUCCESS, service.seed_hold(
      request, ObTimeUtility::current_monotonic_time()));
  service.on_refresh_ = [](MockRetentionGCLeader *self) {
    lib::ObMutexGuard guard(self->lock_);
    self->reset_replica_caches_();
    self->leader_stat_.log_proposal_id_ = 11;
    self->leader_stat_.config_version_ = MockRetentionGCLeader::version(11, 1);
    self->update_leader_state_(self->leader_stat_);
    ObLSGCRequest new_request;
    const ObAddr server(ObAddr::IPV4, "127.0.0.1", 2882);
    ASSERT_EQ(OB_SUCCESS, new_request.init(
        server, ObLSID(1001), SCN::plus(SCN::min_scn(), 50), SCN::plus(SCN::min_scn(), 80),
        self->leader_stat_.config_version_, 10, LSN(150), LSN(180), INVALID_GC_TOKEN));
    ASSERT_EQ(OB_SUCCESS, self->seed_hold(
        new_request, ObTimeUtility::current_monotonic_time()));
  };
  ASSERT_EQ(OB_EAGAIN, service.refresh_cache());
  ASSERT_TRUE(service.unlocked_);
  ASSERT_EQ(MockRetentionGCLeader::version(11, 1), service.config_version_);
  ASSERT_EQ(1, service.retained_replica_caches_.count());
  ASSERT_GT(service.last_request_time_, 0);
  ASSERT_GT(service.retained_replica_caches_.at(0).last_refresh_time_, 0);
  service.on_refresh_ = NULL;
  ASSERT_EQ(OB_SUCCESS, service.refresh_cache());
  ASSERT_EQ(2, service.calls_);
  ASSERT_GT(service.retained_replica_caches_.at(0).last_refresh_time_, 0);
  // A same-view self-report stays independent of in-flight member responses.
  service.begin_ = SCN::plus(SCN::min_scn(), 20);
  service.end_ = SCN::plus(SCN::min_scn(), 100);
  service.on_refresh_ = [](MockRetentionGCLeader *self) {
    lib::ObMutexGuard guard(self->lock_);
    self->reset_replica_caches_();
    ObLSGCRequest request;
    ASSERT_EQ(OB_SUCCESS, request.init(ObAddr(ObAddr::IPV4, "127.0.0.1", 2882),
        ObLSID(1001), SCN::plus(SCN::min_scn(), 50), SCN::plus(SCN::min_scn(), 80),
        self->leader_stat_.config_version_, 10, LSN(150), LSN(180), INVALID_GC_TOKEN));
    ASSERT_EQ(OB_SUCCESS, self->seed_hold(
        request, ObTimeUtility::current_monotonic_time()));
  };
  ASSERT_EQ(OB_SUCCESS, service.refresh_cache());
  ASSERT_GT(service.retained_replica_caches_.at(0).last_refresh_time_, 0);
  ASSERT_EQ(SCN::plus(SCN::min_scn(), 50), service.retained_replica_caches_.at(0).begin_scn_);
  ASSERT_EQ(SCN::plus(SCN::min_scn(), 80), service.retained_replica_caches_.at(0).end_scn_);
  // Reactivating requests in the same view does not discard the refreshed range.
  service.begin_ = SCN::plus(SCN::min_scn(), 50);
  service.end_ = SCN::plus(SCN::min_scn(), 80);
  service.on_refresh_ = [](MockRetentionGCLeader *self) {
    lib::ObMutexGuard guard(self->lock_);
    self->last_request_time_ -= RetentionGCLeader::ACTIVE_WINDOW;
    ObLSGCRequest request;
    ASSERT_EQ(OB_SUCCESS, request.init(ObAddr(ObAddr::IPV4, "127.0.0.1", 2882),
        ObLSID(1001), SCN::plus(SCN::min_scn(), 50), SCN::plus(SCN::min_scn(), 80),
        self->leader_stat_.config_version_, 10, LSN(150), LSN(180), INVALID_GC_TOKEN));
    ASSERT_EQ(OB_SUCCESS, self->seed_hold(
        request, ObTimeUtility::current_monotonic_time()));
  };
  ASSERT_EQ(OB_SUCCESS, service.refresh_cache());
  ASSERT_EQ(SCN::plus(SCN::min_scn(), 50), service.retained_replica_caches_.at(0).begin_scn_);
  ASSERT_EQ(SCN::plus(SCN::min_scn(), 80), service.retained_replica_caches_.at(0).end_scn_);
}

TEST(TestObGarbageCollector, failed_replica_bypasses_retention)
{
  storage::ObLS *ls = OB_NEW(storage::ObLS, "TestGC");
  ASSERT_NE(nullptr, ls);
  DEFER(OB_DELETE(ObLS, "TestGC", ls));
  ObLSRetentionGCHandler service;
  ASSERT_EQ(OB_INVALID_ARGUMENT, service.init(NULL));
  ASSERT_EQ(OB_SUCCESS, service.init(ls));
  ASSERT_EQ(ls, service.leader_.ls_);
  ASSERT_EQ(ls, service.gc_replica_.ls_);
  ASSERT_EQ(OB_INIT_TWICE, service.init(ls));
  ObGCHandler gc_handler;
  ASSERT_EQ(OB_SUCCESS, gc_handler.init(ls));
  bool allow_gc = false;
  ASSERT_EQ(OB_SUCCESS, service.check_permission(allow_gc));
  ASSERT_TRUE(allow_gc);
  ASSERT_EQ(OB_SUCCESS, gc_handler.check_retention_gc_permission(allow_gc));
  ASSERT_TRUE(allow_gc);
  const storage::ObMigrationStatus failures[] = {
    storage::ObMigrationStatus::OB_MIGRATION_STATUS_ADD_FAIL,
    storage::ObMigrationStatus::OB_MIGRATION_STATUS_MIGRATE_FAIL,
    storage::ObMigrationStatus::OB_MIGRATION_STATUS_REBUILD_FAIL
  };
  for (const auto status : failures) {
    ASSERT_EQ(OB_SUCCESS, ls->ls_meta_.init(1001, ObLSID(1001), status,
        ObLSRestoreStatus(ObLSRestoreStatus::RESTORE_NONE), SCN::min_scn(), REPLICA_TYPE_FULL));
    ls->ls_meta_.set_ls_create_status(storage::ObInnerLSStatus::COMMITTED);
    service.reset();
    gc_handler.reset();
    ASSERT_EQ(OB_SUCCESS, service.init(ls));
    ASSERT_EQ(OB_SUCCESS, gc_handler.init(ls));
    allow_gc = false;
    ASSERT_EQ(OB_SUCCESS, service.check_permission(allow_gc));
    ASSERT_TRUE(allow_gc);
    ASSERT_EQ(OB_SUCCESS, gc_handler.check_retention_gc_permission(allow_gc));
    ASSERT_TRUE(allow_gc);
  }
}

TEST(TestObGarbageCollector, system_tenant_retention_bypass_injection)
{
  const uint64_t tenant_id = OB_SYS_TENANT_ID;
  ObMallocAllocator::get_instance()->create_and_add_tenant_allocator(tenant_id);
  ASSERT_EQ(OB_SUCCESS, omt::ObTenantConfigMgr::get_instance().add_tenant_config(tenant_id));
  omt::ObTenantConfigGuard config(
      omt::ObTenantConfigMgr::get_instance().get_tenant_config_with_lock(tenant_id, 0, 0));
  ASSERT_TRUE(config.is_valid());
  config->log_retention_replica_count = 0;
  ObTenantBase tenant(tenant_id);
  ObTenantEnv::set_tenant(&tenant);
  DEFER(ObTenantEnv::set_tenant(NULL));
  ObLS *ls = OB_NEW(ObLS, "TestGC");
  ASSERT_TRUE(NULL != ls);
  DEFER(OB_DELETE(ObLS, "TestGC", ls));
  ASSERT_EQ(OB_SUCCESS, ls->ls_meta_.init(tenant_id, ObLSID(1001),
      storage::ObMigrationStatus::OB_MIGRATION_STATUS_GC,
      ObLSRestoreStatus(ObLSRestoreStatus::RESTORE_NONE), SCN::min_scn(), REPLICA_TYPE_FULL));
  ls->ls_meta_.set_ls_create_status(storage::ObInnerLSStatus::COMMITTED);
  RetentionGCReplica replica;
  ASSERT_EQ(OB_SUCCESS, replica.init(ls));
  bool allow_gc = true;
  TP_SWITCH_GUARD(false);
  DEFER(EventTable::set_event("EN_BYPASS_SYS_TENANT_RETAINED_LS_GC", EventItem()));

  ASSERT_EQ(OB_SUCCESS, replica.check_gc_bypass(allow_gc));
  ASSERT_TRUE(allow_gc);
  replica.reset();
  ASSERT_EQ(OB_SUCCESS, replica.init(ls));
  config->log_retention_replica_count = 1;
  ASSERT_EQ(OB_SUCCESS, replica.check_gc_bypass(allow_gc));
  ASSERT_FALSE(allow_gc);
  EventItem bypass;
  bypass.error_code_ = OB_EAGAIN;
  bypass.trigger_freq_ = 1;
  ASSERT_EQ(OB_SUCCESS,
      EventTable::set_event("EN_BYPASS_SYS_TENANT_RETAINED_LS_GC", bypass));
  ASSERT_EQ(OB_SUCCESS, replica.check_gc_bypass(allow_gc));
  ASSERT_TRUE(allow_gc);
}

TEST(TestObGarbageCollector, retention_gc_injection_preserves_permission)
{
  ObLS *ls = OB_NEW(ObLS, "TestGC");
  ASSERT_NE(nullptr, ls);
  DEFER(OB_DELETE(ObLS, "TestGC", ls));
  ASSERT_EQ(OB_SUCCESS, ls->ls_meta_.init(1001, ObLSID(1001),
      storage::ObMigrationStatus::OB_MIGRATION_STATUS_ADD_FAIL,
      ObLSRestoreStatus(ObLSRestoreStatus::RESTORE_NONE), SCN::min_scn(), REPLICA_TYPE_FULL));
  ObGCHandler handler;
  bool allow_gc = true;
  ASSERT_EQ(OB_NOT_INIT, handler.check_retention_gc_permission(allow_gc));
  ASSERT_FALSE(allow_gc);
  ASSERT_EQ(OB_SUCCESS, handler.init(ls));
  EventItem pause;
  pause.error_code_ = OB_EAGAIN;
  pause.trigger_freq_ = 1;
  pause.cond_ = 1001;
  TP_SWITCH_GUARD(false);
  DEFER({
    EventTable::set_event("EN_RETAINED_LS_GC_BEFORE_PERMISSION", EventItem());
    EventTable::set_event("EN_RETAINED_LS_GC_AFTER_PERMISSION", EventItem());
  });
  ASSERT_EQ(OB_SUCCESS, EventTable::set_event("EN_RETAINED_LS_GC_BEFORE_PERMISSION", pause));
  ASSERT_EQ(OB_SUCCESS, handler.check_retention_gc_permission(allow_gc));
  ASSERT_FALSE(allow_gc);
  ASSERT_FALSE(ATOMIC_LOAD(&handler.retention_gc_handler_.gc_replica_.gc_started_));

  // A different LS does not match; the AFTER point retains the adopted bypass permit.
  pause.cond_ = 1002;
  ASSERT_EQ(OB_SUCCESS, EventTable::set_event("EN_RETAINED_LS_GC_BEFORE_PERMISSION", pause));
  pause.cond_ = 1001;
  ASSERT_EQ(OB_SUCCESS, EventTable::set_event("EN_RETAINED_LS_GC_AFTER_PERMISSION", pause));
  for (int64_t i = 0; i < 2; ++i) {
    ASSERT_EQ(OB_SUCCESS, handler.check_retention_gc_permission(allow_gc));
    ASSERT_FALSE(allow_gc);
    ASSERT_TRUE(ATOMIC_LOAD(&handler.retention_gc_handler_.gc_replica_.gc_started_));
  }
  ASSERT_EQ(OB_SUCCESS, EventTable::set_event("EN_RETAINED_LS_GC_AFTER_PERMISSION", EventItem()));
  ASSERT_EQ(OB_SUCCESS, EventTable::set_event("EN_RETAINED_LS_GC_BEFORE_PERMISSION", pause));
  ASSERT_EQ(OB_SUCCESS, handler.check_retention_gc_permission(allow_gc));
  ASSERT_FALSE(allow_gc);
  ASSERT_TRUE(ATOMIC_LOAD(&handler.retention_gc_handler_.gc_replica_.gc_started_));
  ASSERT_EQ(OB_SUCCESS, EventTable::set_event("EN_RETAINED_LS_GC_BEFORE_PERMISSION", EventItem()));
  ASSERT_EQ(OB_SUCCESS, handler.check_retention_gc_permission(allow_gc));
  ASSERT_TRUE(allow_gc);
}

TEST(TestObGarbageCollector, expired_range_bypasses_rpc)
{
  const uint64_t tenant_id = 1008;
  ASSERT_TRUE(is_user_tenant(tenant_id));
  ObMallocAllocator::get_instance()->create_and_add_tenant_allocator(tenant_id);
  ASSERT_EQ(OB_SUCCESS, omt::ObTenantConfigMgr::get_instance().add_tenant_config(tenant_id));
  omt::ObTenantConfigGuard config(
      omt::ObTenantConfigMgr::get_instance().get_tenant_config_with_lock(tenant_id, 0, 0));
  ASSERT_TRUE(config.is_valid());
  config->log_retention_replica_count = 8;
  ASSERT_TRUE(config->log_retention_replica_count.check());
  config->log_retention_replica_count = INT64_MAX;
  ASSERT_TRUE(config->log_retention_replica_count.check());
  config->log_retention_replica_count = -1;
  ASSERT_FALSE(config->log_retention_replica_count.check());
  config->log_retention_replica_count = 3;
  ObTenantBase tenant(tenant_id);
  ObTenantEnv::set_tenant(&tenant);
  DEFER(ObTenantEnv::set_tenant(nullptr));
  storage::ObLS *ls = OB_NEW(storage::ObLS, "TestGC");
  ASSERT_NE(nullptr, ls);
  DEFER(OB_DELETE(ObLS, "TestGC", ls));
  ASSERT_EQ(OB_SUCCESS, ls->ls_meta_.init(tenant_id, ObLSID(1001),
      storage::ObMigrationStatus::OB_MIGRATION_STATUS_GC,
      ObLSRestoreStatus(ObLSRestoreStatus::RESTORE_NONE), SCN::min_scn(), REPLICA_TYPE_FULL));
  ls->ls_meta_.set_ls_create_status(storage::ObInnerLSStatus::COMMITTED);
  {
    // Use the real retention check without PALF/RPC services: only expiry can allow GC.
    RetentionGCReplica replica;
    ASSERT_EQ(OB_SUCCESS, replica.init(ls));
    SCN end_scn;
    const int64_t retention_us = config->log_retention_time;
    ASSERT_EQ(2 * 60 * 60 * 1000 * 1000L, retention_us);
    ASSERT_EQ(OB_SUCCESS, end_scn.convert_for_tx(
        (ObClockGenerator::getClock() - retention_us - 1000000) * 1000L));
    bool expired = false;
    ASSERT_EQ(OB_SUCCESS, replica.check_permission_with_end_scn_(end_scn, expired));
    ASSERT_TRUE(expired);
    ASSERT_TRUE(ATOMIC_LOAD(&replica.gc_started_));
    replica.reset();
    ASSERT_EQ(OB_SUCCESS, replica.init(ls));
    ASSERT_EQ(OB_NOT_INIT, replica.check_permission_with_end_scn_(SCN::max_scn(), expired));
    ASSERT_FALSE(expired);
    ASSERT_FALSE(ATOMIC_LOAD(&replica.gc_started_));
    ASSERT_EQ(OB_INVALID_ARGUMENT, replica.check_permission_with_end_scn_(SCN(), expired));
    ASSERT_FALSE(expired);
    ASSERT_FALSE(ATOMIC_LOAD(&replica.gc_started_));
  }
  MockRetentionGCReplica service;
  ASSERT_EQ(OB_SUCCESS, service.init(ls));
  ObGCHandler gc_handler;
  ASSERT_EQ(OB_SUCCESS, gc_handler.init(ls));
  bool allow_gc = true;
  SCN cutoff_scn = SCN::plus(SCN::min_scn(), 100);
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      service.RetentionGCReplica::check_log_retention_(SCN(), allow_gc, cutoff_scn));
  ASSERT_FALSE(allow_gc);
  ASSERT_FALSE(cutoff_scn.is_valid());
  // No PALF service: the end SCN cannot be read, so permission is denied.
  ASSERT_EQ(OB_NOT_INIT, gc_handler.check_retention_gc_permission(allow_gc));
  ASSERT_FALSE(allow_gc);
  ASSERT_EQ(OB_NOT_INIT, gc_handler.check_retention_gc_permission(allow_gc));
  ASSERT_FALSE(allow_gc);
  ASSERT_EQ(OB_INVALID_ARGUMENT, service.check_permission_with_end_scn_(SCN(), allow_gc));
  ASSERT_FALSE(allow_gc);
  service.begin_ = service.end_;
  ASSERT_EQ(OB_SUCCESS, service.check_permission_with_end_scn_(SCN::plus(SCN::min_scn(), 200), allow_gc));
  ASSERT_TRUE(allow_gc);
  ASSERT_TRUE(ATOMIC_LOAD(&service.gc_started_));
  ASSERT_EQ(0, service.calls_);
  service.result_ = OB_EAGAIN;
  // A handed-out permit survives failed removal; only a new LS lifetime resets it.
  ASSERT_EQ(OB_SUCCESS, service.check_permission(allow_gc));
  ASSERT_TRUE(allow_gc);
  service.reset();
  ASSERT_EQ(OB_SUCCESS, service.init(ls));
  ASSERT_EQ(OB_EAGAIN, service.check_permission_with_end_scn_(SCN::plus(SCN::min_scn(), 200), allow_gc));
  ASSERT_FALSE(allow_gc);
  ASSERT_FALSE(ATOMIC_LOAD(&service.gc_started_));
  ASSERT_EQ(0, service.calls_);
  // Propagate a retention check error without an RPC or local permit.
  service.result_ = OB_ENTRY_NOT_EXIST;
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, service.check_permission_with_end_scn_(SCN::plus(SCN::min_scn(), 200), allow_gc));
  ASSERT_FALSE(allow_gc);
  ASSERT_FALSE(ATOMIC_LOAD(&service.gc_started_));
  ASSERT_EQ(0, service.calls_);
  service.result_ = OB_SUCCESS;
  service.begin_ = SCN::plus(SCN::min_scn(), 20);
  service.end_ = SCN::plus(SCN::min_scn(), 300);
  // Bypass permission survives configuration changes during removal retries.
  config->log_retention_replica_count = 0;
  ASSERT_EQ(OB_SUCCESS, service.check_gc_bypass(allow_gc));
  ASSERT_TRUE(allow_gc);
  ASSERT_TRUE(ATOMIC_LOAD(&service.gc_started_));
  config->log_retention_replica_count = 3;
  ASSERT_EQ(OB_SUCCESS, service.check_gc_bypass(allow_gc));
  ASSERT_TRUE(allow_gc);
  service.reset();
  ASSERT_EQ(OB_SUCCESS, service.init(ls));
  {
    const ObAddr self = GCTX.self_addr();
    DEFER(GCTX.self_addr_seq_.set_addr(self));
    GCTX.self_addr_seq_.set_addr(ObAddr());
    ASSERT_EQ(OB_INVALID_ARGUMENT, service.check_permission_with_end_scn_(SCN::plus(SCN::min_scn(), 200), allow_gc));
    ASSERT_FALSE(allow_gc);
    ASSERT_FALSE(ATOMIC_LOAD(&service.gc_started_));
    ASSERT_EQ(0, service.calls_);
  }
  ASSERT_EQ(OB_NOT_INIT, service.check_permission_with_end_scn_(SCN::plus(SCN::min_scn(), 200), allow_gc));
  ASSERT_FALSE(allow_gc);
  ASSERT_EQ(SCN::plus(SCN::min_scn(), 200), service.end_scn_);
  ASSERT_EQ(1, service.calls_);
  ASSERT_TRUE(service.request_.is_valid());
  ASSERT_EQ(GCTX.self_addr(), service.request_.src_);
  ASSERT_EQ(ls->get_ls_id(), service.request_.get_ls_id());
  ASSERT_EQ(SCN::plus(SCN::min_scn(), 20), service.request_.get_cutoff_scn());
  ASSERT_EQ(SCN::plus(SCN::min_scn(), 200), service.request_.get_end_scn());
  ASSERT_EQ(OB_NOT_INIT, service.check_permission_with_end_scn_(SCN::plus(SCN::min_scn(), 200), allow_gc));
  ASSERT_EQ(SCN::plus(SCN::min_scn(), 200), service.end_scn_);
  config->log_retention_replica_count = 0;
  ASSERT_TRUE(config->log_retention_replica_count.check());
  ASSERT_EQ(OB_SUCCESS, service.check_permission(allow_gc));
  ASSERT_TRUE(allow_gc);
  ASSERT_EQ(OB_SUCCESS, gc_handler.check_retention_gc_permission(allow_gc));
  ASSERT_TRUE(allow_gc);
  config->log_retention_replica_count = 3;
  ASSERT_EQ(OB_SUCCESS, gc_handler.check_retention_gc_permission(allow_gc));
  ASSERT_TRUE(allow_gc);
}

TEST(TestObGarbageCollector, meta_tenant_bypasses_retention)
{
  const uint64_t tenant_ids[] = {1007};
  for (const uint64_t tenant_id : tenant_ids) {
    ASSERT_TRUE(is_sys_tenant(tenant_id) || is_meta_tenant(tenant_id));
    ObMallocAllocator::get_instance()->create_and_add_tenant_allocator(tenant_id);
    ASSERT_EQ(OB_SUCCESS, omt::ObTenantConfigMgr::get_instance().add_tenant_config(tenant_id));
    omt::ObTenantConfigGuard config(
        omt::ObTenantConfigMgr::get_instance().get_tenant_config_with_lock(tenant_id, 0, 0));
    ASSERT_TRUE(config.is_valid());
    const int64_t old_count = config->log_retention_replica_count;
    DEFER(config->log_retention_replica_count = old_count);
    config->log_retention_replica_count = 8;
    ObTenantBase tenant(tenant_id);
    ObTenantEnv::set_tenant(&tenant);
    DEFER(ObTenantEnv::set_tenant(NULL));
    ObLS *ls = OB_NEW(ObLS, "TestGC");
    ASSERT_NE(nullptr, ls);
    DEFER(OB_DELETE(ObLS, "TestGC", ls));
    ASSERT_EQ(OB_SUCCESS, ls->ls_meta_.init(tenant_id, ObLSID(1001),
        storage::ObMigrationStatus::OB_MIGRATION_STATUS_GC,
        ObLSRestoreStatus(ObLSRestoreStatus::RESTORE_NONE), SCN::min_scn(), REPLICA_TYPE_FULL));
    ls->ls_meta_.set_ls_create_status(storage::ObInnerLSStatus::COMMITTED);
    ASSERT_FALSE(ls->is_need_gc());
    MockRetentionGCReplica replica;
    ASSERT_EQ(OB_SUCCESS, replica.init(ls));
    replica.result_ = OB_ERR_UNEXPECTED;
    bool allow_gc = false;
    ASSERT_EQ(OB_SUCCESS, replica.check_gc_bypass(allow_gc));
    ASSERT_TRUE(allow_gc);
    ASSERT_TRUE(ATOMIC_LOAD(&replica.gc_started_));
    ASSERT_EQ(0, replica.calls_);
    ObGCHandler gc_handler;
    ASSERT_EQ(OB_SUCCESS, gc_handler.init(ls));
    ASSERT_EQ(OB_SUCCESS, gc_handler.check_retention_gc_permission(allow_gc));
    ASSERT_TRUE(allow_gc);
  }
}

TEST(TestObGarbageCollector, upgrade_uses_retention_config)
{
  const uint64_t tenant_id = 1014;
  ObMallocAllocator::get_instance()->create_and_add_tenant_allocator(tenant_id);
  ASSERT_EQ(OB_SUCCESS, omt::ObTenantConfigMgr::get_instance().add_tenant_config(tenant_id));
  omt::ObTenantConfigGuard config(
      omt::ObTenantConfigMgr::get_instance().get_tenant_config_with_lock(tenant_id, 0, 0));
  ASSERT_TRUE(config.is_valid());
  config->log_retention_replica_count = 2;
  ObTenantBase tenant(tenant_id);
  ObTenantEnv::set_tenant(&tenant);
  DEFER(ObTenantEnv::set_tenant(NULL));
  ObLS *ls = OB_NEW(ObLS, "TestGC");
  ASSERT_TRUE(NULL != ls);
  DEFER(OB_DELETE(ObLS, "TestGC", ls));
  ASSERT_EQ(OB_SUCCESS, ls->ls_meta_.init(tenant_id, ObLSID(1001),
      storage::ObMigrationStatus::OB_MIGRATION_STATUS_GC,
      ObLSRestoreStatus(ObLSRestoreStatus::RESTORE_NONE), SCN::min_scn(), REPLICA_TYPE_FULL));
  ls->ls_meta_.set_ls_create_status(storage::ObInnerLSStatus::COMMITTED);
  MockRetentionGCReplica replica;
  ASSERT_EQ(OB_SUCCESS, replica.init(ls));
  const bool old_enable_upgrade_mode = GCONF.enable_upgrade_mode;
  GCONF.enable_upgrade_mode = true;
  DEFER(GCONF.enable_upgrade_mode = old_enable_upgrade_mode);
  bool allow_gc = false;
  ASSERT_EQ(OB_SUCCESS, replica.check_gc_bypass(allow_gc));
  ASSERT_FALSE(allow_gc);
  ASSERT_FALSE(ATOMIC_LOAD(&replica.gc_started_));
  ASSERT_EQ(0, replica.calls_);
}

TEST(TestObGarbageCollector, old_config_refresh_cannot_restore_hold)
{
  MockRetentionGCLeader leader;
  ObLS *ls = OB_NEW(ObLS, "TestGC");
  ASSERT_NE(nullptr, ls);
  DEFER(OB_DELETE(ObLS, "TestGC", ls));
  ASSERT_EQ(OB_SUCCESS, leader.init(ls));
  leader.update_leader_state_(leader.leader_stat_);
  ASSERT_EQ(OB_SUCCESS, leader.leader_stat_.paxos_member_list_.add_server(
      ObAddr(ObAddr::IPV4, "127.0.0.1", 2884)));
  ObLSGCRequest request;
  const ObAddr a(ObAddr::IPV4, "127.0.0.1", 2882);
  const ObAddr b(ObAddr::IPV4, "127.0.0.1", 2883);
  ASSERT_EQ(OB_SUCCESS, request.init(
      a, ObLSID(1001), SCN::min_scn(), SCN::plus(SCN::min_scn(), 200),
      MockRetentionGCLeader::version(10, 1), 10, LSN(100), LSN(200), INVALID_GC_TOKEN));
  ASSERT_EQ(OB_SUCCESS, leader.seed_hold(request,
      ObTimeUtility::current_monotonic_time()));
  ASSERT_EQ(OB_SUCCESS, request.init(
      b, ObLSID(1001), SCN::min_scn(), SCN::plus(SCN::min_scn(), 200),
      MockRetentionGCLeader::version(10, 1), 10, LSN(100), LSN(200), INVALID_GC_TOKEN));
  ASSERT_EQ(OB_SUCCESS, leader.seed_hold(request,
      ObTimeUtility::current_monotonic_time()));
  leader.on_refresh_ = [](MockRetentionGCLeader *self) {
    lib::ObMutexGuard guard(self->lock_);
    ASSERT_EQ(OB_SUCCESS, self->leader_stat_.config_version_.inc_update_version(10));
    self->update_leader_state_(self->leader_stat_);
    ObLSGCRequest request;
    const ObAddr a(ObAddr::IPV4, "127.0.0.1", 2882);
    const ObAddr b(ObAddr::IPV4, "127.0.0.1", 2883);
    ASSERT_EQ(OB_SUCCESS, request.init(
        a, ObLSID(1001), SCN::min_scn(), SCN::plus(SCN::min_scn(), 200),
        self->leader_stat_.config_version_, 10, LSN(100), LSN(200), INVALID_GC_TOKEN));
    ASSERT_EQ(OB_SUCCESS, self->seed_hold(request,
        ObTimeUtility::current_monotonic_time()));
    ASSERT_EQ(OB_SUCCESS, self->mark_approved(b));
  };
  ASSERT_EQ(OB_EAGAIN, leader.refresh_cache());
  ASSERT_EQ(MockRetentionGCLeader::version(10, 2), leader.config_version_);
  ASSERT_EQ(1, leader.retained_replica_caches_.count());
  ASSERT_EQ(a, leader.retained_replica_caches_.at(0).server_);
  ASSERT_GT(leader.retained_replica_caches_.at(0).last_refresh_time_, 0);
  ASSERT_EQ(-1, leader.find_replica_cache_(leader.retained_replica_caches_, b));
  ASSERT_EQ(1, leader.approved_count());
  leader.on_refresh_ = NULL;
  ASSERT_EQ(OB_SUCCESS, leader.refresh_cache());
  ASSERT_GT(leader.retained_replica_caches_.at(0).last_refresh_time_, 0);
  ASSERT_EQ(-1, leader.find_replica_cache_(leader.retained_replica_caches_, b));
  // STARTWORKING advances config_version before the next active leader is visible.
  ObArray<ObAddr> replicas;
  ObArray<PalfStat> stats;
  for (int64_t i = 0; i < leader.retained_replica_caches_.count(); ++i) {
    ASSERT_EQ(OB_SUCCESS, replicas.push_back(leader.retained_replica_caches_.at(i).server_));
  }
  leader.leader_stat_.log_proposal_id_ = 11;
  ASSERT_EQ(OB_SUCCESS, leader.leader_stat_.config_version_.inc_update_version(11));
  ASSERT_EQ(OB_SUCCESS, leader.update_leader_state_(leader.leader_stat_));
  ASSERT_EQ(MockRetentionGCLeader::version(11, 3), leader.config_version_);
  ASSERT_TRUE(leader.member_replica_caches_.empty());
  ASSERT_TRUE(leader.retained_replica_caches_.empty());
  ASSERT_TRUE(leader.approved_count() == 0);
  ASSERT_EQ(OB_EAGAIN, leader.apply_refresh_(MockRetentionGCLeader::version(10, 2), 10,
      replicas, stats, ObTimeUtility::current_monotonic_time()));
}

TEST(TestObGarbageCollector, adopted_gc_permit_blocks_new_hold)
{
  const uint64_t tenant_id = 1010;
  ASSERT_TRUE(is_user_tenant(tenant_id));
  ObMallocAllocator::get_instance()->create_and_add_tenant_allocator(tenant_id);
  ASSERT_EQ(OB_SUCCESS, omt::ObTenantConfigMgr::get_instance().add_tenant_config(tenant_id));
  omt::ObTenantConfigGuard config(
      omt::ObTenantConfigMgr::get_instance().get_tenant_config_with_lock(tenant_id, 0, 0));
  ASSERT_TRUE(config.is_valid());
  config->log_retention_replica_count = 2;
  ObTenantBase tenant(tenant_id);
  ObTenantEnv::set_tenant(&tenant);
  DEFER(ObTenantEnv::set_tenant(NULL));
  ObLS *ls = OB_NEW(ObLS, "TestGC");
  ASSERT_NE(nullptr, ls);
  DEFER(OB_DELETE(ObLS, "TestGC", ls));
  ASSERT_EQ(OB_SUCCESS, ls->ls_meta_.init(tenant_id, ObLSID(1001),
      storage::ObMigrationStatus::OB_MIGRATION_STATUS_GC,
      ObLSRestoreStatus(ObLSRestoreStatus::RESTORE_NONE), SCN::min_scn(), REPLICA_TYPE_FULL));
  ls->ls_meta_.set_ls_create_status(storage::ObInnerLSStatus::COMMITTED);
  MockRetentionGCReplica replica;
  ASSERT_EQ(OB_SUCCESS, replica.init(ls));
  replica.rpc_result_ = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS, replica.response_.init(
      false/*allow_gc*/, MockRetentionGCLeader::version(10, 1), 1/*gc_token*/));
  bool allow_gc = false;
  ASSERT_EQ(OB_SUCCESS, replica.check_permission_with_end_scn_(SCN::plus(SCN::min_scn(), 200), allow_gc));
  ASSERT_FALSE(allow_gc);
  ASSERT_FALSE(replica.request_.get_config_version().is_valid());
  ASSERT_EQ(MockRetentionGCLeader::version(10, 1), replica.config_version_);
  replica.response_.reset();
  ASSERT_EQ(OB_ERR_UNEXPECTED, replica.check_permission_with_end_scn_(SCN::plus(SCN::min_scn(), 200), allow_gc));
  ASSERT_FALSE(allow_gc);
  ASSERT_EQ(MockRetentionGCLeader::version(10, 1), replica.request_.get_config_version());
  ASSERT_FALSE(ATOMIC_LOAD(&replica.gc_started_));
  replica.rpc_result_ = OB_TIMEOUT;
  ASSERT_EQ(OB_TIMEOUT, replica.check_permission_with_end_scn_(SCN::plus(SCN::min_scn(), 200), allow_gc));
  ASSERT_EQ(MockRetentionGCLeader::version(10, 1), replica.config_version_);
  ASSERT_FALSE(ATOMIC_LOAD(&replica.gc_started_));
  replica.rpc_result_ = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS, replica.response_.init(
      true/*allow_gc*/, MockRetentionGCLeader::version(11, 1), 1/*gc_token*/));
  ASSERT_EQ(OB_SUCCESS, replica.check_permission_with_end_scn_(SCN::plus(SCN::min_scn(), 200), allow_gc));
  ASSERT_TRUE(allow_gc);
  ASSERT_EQ(MockRetentionGCLeader::version(10, 1), replica.request_.get_config_version());
  ASSERT_EQ(MockRetentionGCLeader::version(11, 1), replica.config_version_);
  ASSERT_TRUE(ATOMIC_LOAD(&replica.gc_started_));
  const int64_t calls = replica.calls_;
  // Permission has escaped: pending, in-flight and failed remove_ls all keep it.
  ASSERT_EQ(OB_SUCCESS, replica.response_.init(
      false/*allow_gc*/, MockRetentionGCLeader::version(12, 1), 1/*gc_token*/));
  for (int64_t i = 0; i < 3; ++i) {
    ASSERT_EQ(OB_SUCCESS, replica.check_permission(allow_gc));
    ASSERT_TRUE(allow_gc);
    ASSERT_EQ(calls, replica.calls_);
  }
  // A new object models restart before a durable remove slog: no old call survives.
  MockRetentionGCReplica recovered;
  ASSERT_EQ(OB_SUCCESS, recovered.init(ls));
  recovered.rpc_result_ = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS, recovered.response_.init(
      false/*allow_gc*/, MockRetentionGCLeader::version(10, 1), 1/*gc_token*/));
  ASSERT_EQ(OB_SUCCESS, recovered.check_permission_with_end_scn_(SCN::plus(SCN::min_scn(), 200), allow_gc));
  ASSERT_FALSE(allow_gc);
  ASSERT_EQ(1, recovered.calls_);
  ASSERT_FALSE(ATOMIC_LOAD(&recovered.gc_started_));
  // Replayed remove slog marks REMOVED before normal GC starts; no retention RPC is needed.
  recovered.reset();
  ls->set_create_state(storage::ObInnerLSStatus::REMOVED);
  ASSERT_EQ(OB_SUCCESS, recovered.init(ls));
  recovered.result_ = OB_ERR_UNEXPECTED;
  ASSERT_EQ(OB_SUCCESS, recovered.check_permission(allow_gc));
  ASSERT_TRUE(allow_gc);
  ASSERT_EQ(1, recovered.calls_);
}

TEST(TestObGarbageCollector, log_coverage)
{
  ObArray<ObLSGCReplicaCache> ranges;
  ObAddr server;
  bool covered = false;
  const ObAddr valid_server(ObAddr::IPV4, "127.0.0.1", 2882);
  PalfStat stat;
  stat.self_ = valid_server;
  stat.begin_scn_ = SCN::min_scn();
  stat.end_scn_ = SCN::plus(SCN::min_scn(), 100);
  stat.begin_lsn_ = LSN(100);
  stat.end_lsn_ = LSN(200);
  ObLSGCReplicaCache initialized_cache;
  ASSERT_EQ(OB_SUCCESS, initialized_cache.init(stat.self_, stat.begin_scn_, stat.end_scn_,
      stat.begin_lsn_, stat.end_lsn_, 1000));
  ASSERT_EQ(valid_server, initialized_cache.get_server());
  ASSERT_EQ(stat.begin_scn_, initialized_cache.get_begin_scn());
  ASSERT_EQ(stat.end_scn_, initialized_cache.get_end_scn());
  ASSERT_EQ(stat.begin_lsn_, initialized_cache.get_begin_lsn());
  ASSERT_EQ(stat.end_lsn_, initialized_cache.get_end_lsn());
  ASSERT_EQ(1000, initialized_cache.get_last_refresh_time());
  ASSERT_EQ(OB_INVALID_ARGUMENT, initialized_cache.init(stat.self_, stat.begin_scn_, stat.end_scn_,
      stat.begin_lsn_, stat.end_lsn_, 0));
  ASSERT_EQ(1000, initialized_cache.get_last_refresh_time());
  stat.end_lsn_ = LSN();
  ASSERT_EQ(OB_INVALID_ARGUMENT, initialized_cache.init(stat.self_, stat.begin_scn_, stat.end_scn_,
      stat.begin_lsn_, stat.end_lsn_, 1001));
  ASSERT_EQ(1000, initialized_cache.get_last_refresh_time());
  stat.end_lsn_ = LSN(200);
  ASSERT_EQ(OB_SUCCESS, initialized_cache.init(stat.self_, stat.begin_scn_, stat.end_scn_,
      stat.begin_lsn_, stat.end_lsn_, 1002));
  ASSERT_EQ(stat.end_scn_, initialized_cache.get_end_scn());
  const auto make_replica_cache = [](const ObAddr &server, const SCN &begin_scn, const SCN &end_scn) {
    ObLSGCReplicaCache cache(server);
    cache.begin_scn_ = begin_scn;
    cache.end_scn_ = end_scn;
    cache.begin_lsn_ = LSN(100);
    cache.end_lsn_ = LSN(200);
    return cache;
  };
  ObLSGCReplicaCache invalid_lsn_cache(valid_server);
  invalid_lsn_cache.begin_scn_ = SCN::min_scn();
  invalid_lsn_cache.end_scn_ = SCN::plus(SCN::min_scn(), 100);
  ASSERT_FALSE(invalid_lsn_cache.is_valid());
  ASSERT_FALSE(ObLSGCReplicaCache().is_valid());
  ASSERT_FALSE(make_replica_cache(server, SCN(), SCN::plus(SCN::min_scn(), 100)).is_valid());
  ASSERT_FALSE(make_replica_cache(server, SCN::min_scn(), SCN()).is_valid());
  ASSERT_FALSE(make_replica_cache(server, SCN::plus(SCN::min_scn(), 100),
      SCN::plus(SCN::min_scn(), 100)).is_valid());
  ASSERT_FALSE(make_replica_cache(server, SCN::plus(SCN::min_scn(), 200),
      SCN::plus(SCN::min_scn(), 100)).is_valid());
  ASSERT_TRUE(make_replica_cache(server, SCN::min_scn(),
      SCN::plus(SCN::min_scn(), 100)).is_valid());
  ASSERT_EQ(OB_INVALID_ARGUMENT, RetentionGCLeader::check_log_coverage(
      ranges, SCN::min_scn(), SCN::plus(SCN::min_scn(), 200), -1, covered));
  ASSERT_FALSE(covered);
  ASSERT_EQ(OB_INVALID_ARGUMENT, RetentionGCLeader::check_log_coverage(
      ranges, SCN(), SCN::plus(SCN::min_scn(), 200), 0, covered));
  ASSERT_FALSE(covered);
  ASSERT_EQ(OB_INVALID_ARGUMENT, RetentionGCLeader::check_log_coverage(
      ranges, SCN::min_scn(), SCN(), 0, covered));
  ASSERT_FALSE(covered);
  ASSERT_EQ(OB_INVALID_ARGUMENT, RetentionGCLeader::check_log_coverage(
      ranges, SCN::plus(SCN::min_scn(), 200), SCN::min_scn(), 0, covered));
  ASSERT_FALSE(covered);
  ASSERT_EQ(OB_SUCCESS, RetentionGCLeader::check_log_coverage(
      ranges, SCN::min_scn(), SCN::plus(SCN::min_scn(), 200), 0, covered));
  ASSERT_TRUE(covered);
  ASSERT_EQ(OB_SUCCESS, RetentionGCLeader::check_log_coverage(
      ranges, SCN::min_scn(), SCN::plus(SCN::min_scn(), 200), 1, covered));
  ASSERT_FALSE(covered);

  ASSERT_EQ(OB_SUCCESS, ranges.push_back(make_replica_cache(
      server, SCN::min_scn(), SCN::plus(SCN::min_scn(), 100))));
  ASSERT_EQ(OB_SUCCESS, ranges.push_back(make_replica_cache(
      server, SCN::plus(SCN::min_scn(), 100), SCN::plus(SCN::min_scn(), 200))));
  ASSERT_EQ(OB_SUCCESS, RetentionGCLeader::check_log_coverage(
      ranges, SCN::min_scn(), SCN::plus(SCN::min_scn(), 200), 1, covered));
  ASSERT_TRUE(covered);

  // Matching end SCNs cover the half-open range; one extra SCN is not covered.
  ASSERT_EQ(OB_SUCCESS, RetentionGCLeader::check_log_coverage(
      ranges, SCN::min_scn(), SCN::plus(SCN::min_scn(), 201), 1, covered));
  ASSERT_FALSE(covered);

  ranges.reuse();
  ASSERT_EQ(OB_SUCCESS, ranges.push_back(make_replica_cache(
      server, SCN::min_scn(), SCN::plus(SCN::min_scn(), 200))));
  ASSERT_EQ(OB_SUCCESS, ranges.push_back(make_replica_cache(
      server, SCN::min_scn(), SCN::plus(SCN::min_scn(), 100))));
  ASSERT_EQ(OB_SUCCESS, ranges.push_back(make_replica_cache(
      server, SCN::plus(SCN::min_scn(), 100), SCN::plus(SCN::min_scn(), 200))));
  ASSERT_EQ(OB_SUCCESS, RetentionGCLeader::check_log_coverage(
      ranges, SCN::min_scn(), SCN::plus(SCN::min_scn(), 200), 2, covered));
  ASSERT_TRUE(covered);
  ASSERT_EQ(OB_SUCCESS, RetentionGCLeader::check_log_coverage(
      ranges, SCN::min_scn(), SCN::plus(SCN::min_scn(), 250), 2, covered));
  ASSERT_FALSE(covered);

  ranges.at(0).begin_scn_ = SCN::plus(SCN::min_scn(), 50);
  ASSERT_EQ(OB_SUCCESS, RetentionGCLeader::check_log_coverage(
      ranges, SCN::min_scn(), SCN::plus(SCN::min_scn(), 200), 2, covered));
  ASSERT_FALSE(covered);

  ranges.reuse();
  ASSERT_EQ(OB_SUCCESS, ranges.push_back(make_replica_cache(
      server, SCN::min_scn(), SCN::plus(SCN::min_scn(), 90))));
  ASSERT_EQ(OB_SUCCESS, ranges.push_back(make_replica_cache(
      server, SCN::plus(SCN::min_scn(), 100), SCN::plus(SCN::min_scn(), 250))));
  ASSERT_EQ(OB_SUCCESS, RetentionGCLeader::check_log_coverage(
      ranges, SCN::plus(SCN::min_scn(), 20), SCN::plus(SCN::min_scn(), 200), 1, covered));
  ASSERT_FALSE(covered);

  ranges.reuse();
  ASSERT_EQ(OB_SUCCESS, RetentionGCLeader::check_log_coverage(
      ranges, SCN::plus(SCN::min_scn(), 200), SCN::plus(SCN::min_scn(), 200), INT64_MAX, covered));
  ASSERT_TRUE(covered);
  for (int64_t i = 0; i < 8; ++i) {
    ASSERT_TRUE(server.set_ip_addr("127.0.0.1", 2882 + i));
    ASSERT_EQ(OB_SUCCESS, ranges.push_back(make_replica_cache(
        server, SCN::min_scn(), SCN::plus(SCN::min_scn(), 200))));
  }
  ASSERT_EQ(OB_SUCCESS, RetentionGCLeader::check_log_coverage(
      ranges, SCN::min_scn(), SCN::plus(SCN::min_scn(), 200), 8, covered));
  ASSERT_TRUE(covered);
  ASSERT_EQ(OB_SUCCESS, RetentionGCLeader::check_log_coverage(
      ranges, SCN::min_scn(), SCN::plus(SCN::min_scn(), 200), 9, covered));
  ASSERT_FALSE(covered);
  ASSERT_EQ(OB_SUCCESS, RetentionGCLeader::check_log_coverage(
      ranges, SCN::min_scn(), SCN::plus(SCN::min_scn(), 200), INT64_MAX, covered));
  ASSERT_FALSE(covered);
}

TEST(TestObGarbageCollector, gc_rpc_legacy_payload_is_invalid)
{
  ObLSGCRequest request;
  ObLSGCRequest decoded;
  ObLSGCRequest decoded_without_range;
  ObLSGCRequest decoded_without_mode;
  char buffer[1024];
  ASSERT_EQ(OB_SUCCESS, request.init(GCTX.self_addr(), ObLSID(1001), SCN::min_scn(),
      SCN::plus(SCN::min_scn(), 100), MockRetentionGCLeader::version(10, 1), 10, LSN(100), LSN(200), INVALID_GC_TOKEN));
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, request.serialize(buffer, sizeof(buffer), pos));
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded.deserialize(buffer, sizeof(buffer), pos));
  ASSERT_TRUE(decoded.is_valid());
  int64_t version = 0;
  int64_t size_pos = 0;
  ASSERT_EQ(OB_SUCCESS, serialization::decode(buffer, sizeof(buffer), size_pos, version));
  int64_t old_body_size = serialization::encoded_length(request.src_)
      + serialization::encoded_length(request.get_ls_id())
      + serialization::encoded_length(request.get_cutoff_scn())
      + serialization::encoded_length(request.get_end_scn())
      + serialization::encoded_length(request.get_config_version())
      + serialization::encoded_length(request.get_mode_version());
  int64_t body_size_pos = size_pos;
  ASSERT_EQ(OB_SUCCESS, serialization::encode_fixed_bytes_i64(buffer, sizeof(buffer), body_size_pos, old_body_size));
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded_without_range.deserialize(buffer, body_size_pos + old_body_size, pos));
  ASSERT_FALSE(decoded_without_range.is_valid());
  ASSERT_EQ(10, decoded_without_range.get_mode_version());
  ASSERT_FALSE(decoded_without_range.get_begin_lsn().is_valid());
  ASSERT_FALSE(decoded_without_range.get_end_lsn().is_valid());
  old_body_size -= serialization::encoded_length(request.get_mode_version());
  body_size_pos = size_pos;
  ASSERT_EQ(OB_SUCCESS, serialization::encode_fixed_bytes_i64(buffer, sizeof(buffer), body_size_pos, old_body_size));
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded_without_mode.deserialize(buffer, body_size_pos + old_body_size, pos));
  ASSERT_FALSE(decoded_without_mode.is_valid());
  ASSERT_EQ(INVALID_PROPOSAL_ID, decoded_without_mode.get_mode_version());
}

TEST(TestObGarbageCollector, gc_rpc_serialize)
{
  ObAddr src;
  ASSERT_TRUE(src.set_ip_addr("127.0.0.1", 2882));
  ObLSGCRequest request;
  ObLSGCRequest decoded_request;
  ObLSGCResponse response;
  ObLSGCResponse decoded_response;
  char buf[1024];
  int64_t pos = 0;
  ASSERT_EQ(OB_INVALID_ARGUMENT, request.init(
      src, ObLSID(1001), SCN::plus(SCN::min_scn(), 20), SCN::plus(SCN::min_scn(), 30),
      MockRetentionGCLeader::version(10, 1), 10, LSN(), LSN(), INVALID_GC_TOKEN));
  ASSERT_FALSE(request.is_valid());
  ASSERT_EQ(OB_SUCCESS, request.init(
      src, ObLSID(1001), SCN::plus(SCN::min_scn(), 20), SCN::plus(SCN::min_scn(), 30),
      MockRetentionGCLeader::version(10, 1), 10, LSN(100), LSN(200), 17/*gc_token*/));
  ASSERT_EQ(OB_SUCCESS, response.init(
      true/*allow_gc*/, MockRetentionGCLeader::version(10, 1), 1/*gc_token*/));
  ASSERT_TRUE(request.is_valid());
  ASSERT_TRUE(response.is_valid());
  ASSERT_EQ(OB_SUCCESS, request.serialize(buf, sizeof(buf), pos));
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded_request.deserialize(buf, sizeof(buf), pos));
  ASSERT_EQ(request.src_, decoded_request.src_);
  ASSERT_EQ(request.get_ls_id(), decoded_request.get_ls_id());
  ASSERT_EQ(request.get_cutoff_scn(), decoded_request.get_cutoff_scn());
  ASSERT_EQ(request.get_end_scn(), decoded_request.get_end_scn());
  ASSERT_EQ(request.get_config_version(), decoded_request.get_config_version());
  ASSERT_EQ(request.get_mode_version(), decoded_request.get_mode_version());
  ASSERT_EQ(request.get_begin_lsn(), decoded_request.get_begin_lsn());
  ASSERT_EQ(request.get_end_lsn(), decoded_request.get_end_lsn());
  ASSERT_EQ(request.get_gc_token(), decoded_request.get_gc_token());
  ASSERT_TRUE(decoded_request.is_valid());

  ASSERT_EQ(request.get_serialize_size(), pos);
  int64_t header_pos = 0;
  int64_t version = 0;
  int64_t body_size = 0;
  ASSERT_EQ(OB_SUCCESS, serialization::decode(buf, sizeof(buf), header_pos, version));
  ASSERT_EQ(OB_SUCCESS, serialization::decode(buf, sizeof(buf), header_pos, body_size));
  ASSERT_EQ(1, version);
  ASSERT_EQ(serialization::encoded_length(request.src_)
      + serialization::encoded_length(request.get_ls_id())
      + serialization::encoded_length(request.get_cutoff_scn())
      + serialization::encoded_length(request.get_end_scn())
      + serialization::encoded_length(request.get_config_version())
      + serialization::encoded_length(request.get_mode_version())
      + serialization::encoded_length(request.get_begin_lsn())
      + serialization::encoded_length(request.get_end_lsn())
      + serialization::encoded_length(request.get_gc_token()), body_size);

  pos = 0;
  ASSERT_EQ(OB_SUCCESS, response.serialize(buf, sizeof(buf), pos));
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded_response.deserialize(buf, sizeof(buf), pos));
  ASSERT_EQ(response.is_gc_allowed(), decoded_response.is_gc_allowed());
  ASSERT_TRUE(decoded_response.is_valid());
  ASSERT_EQ(response.get_config_version(), decoded_response.get_config_version());
  ASSERT_EQ(response.get_gc_token(), decoded_response.get_gc_token());
  ASSERT_EQ(response.get_serialize_size(), pos);
  header_pos = 0;
  ASSERT_EQ(OB_SUCCESS, serialization::decode(buf, sizeof(buf), header_pos, version));
  ASSERT_EQ(OB_SUCCESS, serialization::decode(buf, sizeof(buf), header_pos, body_size));
  ASSERT_EQ(1, version);
  ASSERT_EQ(serialization::encoded_length(response.is_gc_allowed())
      + serialization::encoded_length(response.get_config_version())
      + serialization::encoded_length(response.get_gc_token()), body_size);
  response.reset();
  ASSERT_FALSE(response.is_gc_allowed());
  ASSERT_FALSE(response.is_valid());
  ASSERT_FALSE(response.get_config_version().is_valid());
  ASSERT_EQ(OB_INVALID_ARGUMENT, response.init(true/*allow_gc*/, LogConfigVersion(), 1/*gc_token*/));
  ASSERT_FALSE(response.is_valid());
  ASSERT_FALSE(response.is_gc_allowed());
  request.reset();
  ASSERT_FALSE(request.is_valid());
  ASSERT_FALSE(request.get_cutoff_scn().is_valid());
  ASSERT_FALSE(request.get_end_scn().is_valid());
  ASSERT_FALSE(request.get_config_version().is_valid());
  ASSERT_EQ(OB_INVALID_ARGUMENT, request.init(
      src, ObLSID(1001), SCN::plus(SCN::min_scn(), 30), SCN::plus(SCN::min_scn(), 20),
      MockRetentionGCLeader::version(10, 1), 10, LSN(100), LSN(200), 17/*gc_token*/));
  ASSERT_EQ(OB_INVALID_ARGUMENT, request.init(src, ObLSID(1001), SCN(), SCN::plus(SCN::min_scn(), 30),
      MockRetentionGCLeader::version(10, 1), 10, LSN(100), LSN(200), 17/*gc_token*/));
  ASSERT_EQ(OB_INVALID_ARGUMENT, request.init(
      src, ObLSID(1001), SCN::plus(SCN::min_scn(), 20), SCN::plus(SCN::min_scn(), 30),
      MockRetentionGCLeader::version(10, 1), 10, LSN(), LSN(200), INVALID_GC_TOKEN));
  ASSERT_EQ(OB_INVALID_ARGUMENT, request.init(
      src, ObLSID(1001), SCN::plus(SCN::min_scn(), 20), SCN::plus(SCN::min_scn(), 30),
      MockRetentionGCLeader::version(10, 1), 10, LSN(200), LSN(100), INVALID_GC_TOKEN));
  ASSERT_EQ(OB_INVALID_ARGUMENT, request.init(
      src, ObLSID(1001), SCN::plus(SCN::min_scn(), 20), SCN::plus(SCN::min_scn(), 30),
      MockRetentionGCLeader::version(10, 1), 10, LSN(100), LSN(), INVALID_GC_TOKEN));
  ASSERT_EQ(OB_INVALID_ARGUMENT, request.init(
      src, ObLSID(1001), SCN::plus(SCN::min_scn(), 20), SCN::plus(SCN::min_scn(), 30),
      MockRetentionGCLeader::version(10, 1), 10, LSN(100), LSN(100), INVALID_GC_TOKEN));
  ASSERT_EQ(OB_SUCCESS, request.init(
      src, ObLSID(1001), SCN::plus(SCN::min_scn(), 20), SCN::plus(SCN::min_scn(), 20),
      LogConfigVersion(), 10, LSN(100), LSN(200), 17/*gc_token*/));
  ASSERT_TRUE(request.is_valid());
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, request.serialize(buf, sizeof(buf), pos));
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded_request.deserialize(buf, sizeof(buf), pos));
  ASSERT_TRUE(decoded_request.is_valid());
  ASSERT_FALSE(decoded_request.get_config_version().is_valid());
}

class TestRetentionGCTokens : public ::testing::Test
{
public:
  using Phase = RetentionGCLeader::GCRequestState;
  TestRetentionGCTokens() : tenant_(1020), ls_(NULL), leader_() {}
  void SetUp() override
  {
    ObMallocAllocator::get_instance()->create_and_add_tenant_allocator(1020);
    const int ret = omt::ObTenantConfigMgr::get_instance().add_tenant_config(1020);
    ASSERT_TRUE(OB_SUCCESS == ret || OB_ENTRY_EXIST == ret);
    set_copies(2);
    ObTenantEnv::set_tenant(&tenant_);
    ls_ = OB_NEW(ObLS, "TestGC");
    ASSERT_NE(nullptr, ls_);
    ASSERT_EQ(OB_SUCCESS, ls_->ls_meta_.init(1020, ObLSID(1001),
        storage::ObMigrationStatus::OB_MIGRATION_STATUS_GC,
        ObLSRestoreStatus(ObLSRestoreStatus::RESTORE_NONE), SCN::min_scn(), REPLICA_TYPE_FULL));
    ls_->ls_meta_.set_ls_create_status(storage::ObInnerLSStatus::COMMITTED);
    ASSERT_EQ(OB_SUCCESS, leader_.init(ls_));
    ASSERT_EQ(OB_SUCCESS, leader_.leader_stat_.paxos_member_list_.add_server(addr(4)));
    leader_.check_stat_locked_ = true;
  }
  void TearDown() override
  {
    leader_.reset();
    if (NULL != ls_) {
      OB_DELETE(ObLS, "TestGC", ls_);
      ls_ = NULL;
    }
    ObTenantEnv::set_tenant(NULL);
  }
  static ObAddr addr(const int64_t id) { return ObAddr(ObAddr::IPV4, "127.0.0.1", 2880 + id); }
  static SCN scn(const int64_t n) { return SCN::plus(SCN::min_scn(), n); }
  void set_copies(const int64_t copies)
  {
    omt::ObTenantConfigGuard config(
        omt::ObTenantConfigMgr::get_instance().get_tenant_config_with_lock(1020, 0, 0));
    ASSERT_TRUE(config.is_valid());
    config->log_retention_replica_count = copies;
  }
  ObLSGCRequest request(const ObAddr &server, const int64_t begin, const int64_t end,
                        const int64_t mode)
  {
    ObLSGCRequest request;
    EXPECT_EQ(OB_SUCCESS, request.init(server, ObLSID(1001), scn(begin), scn(end),
        LogConfigVersion(), mode, LSN(begin + 100), LSN(end + 200), INVALID_GC_TOKEN));
    return request;
  }
  void adopt(ObLSGCRequest &request, const ObLSGCResponse &response)
  {
    ASSERT_TRUE(response.is_valid());
    ASSERT_EQ(OB_SUCCESS, request.init(request.src_, request.get_ls_id(),
        request.get_cutoff_scn(), request.get_end_scn(), response.get_config_version(),
        request.get_mode_version(), request.get_begin_lsn(), request.get_end_lsn(), response.get_gc_token()));
  }
  void handshake(ObLSGCRequest &request)
  {
    ObLSGCResponse response;
    ASSERT_EQ(INVALID_GC_TOKEN, request.get_gc_token());
    ASSERT_EQ(OB_SUCCESS, leader_.handle_request(request, response));
    ASSERT_FALSE(response.is_gc_allowed());
    ASSERT_GT(response.get_gc_token(), INVALID_GC_TOKEN);
    ASSERT_FALSE(leader_.is_gc_hold_(request.src_));
    ASSERT_FALSE(leader_.is_gc_approved_(request.src_));
    adopt(request, response);
  }
  void hold(ObLSGCRequest &request)
  {
    ObLSGCResponse response;
    const int64_t token = request.get_gc_token();
    ASSERT_EQ(OB_SUCCESS, leader_.handle_request(request, response));
    ASSERT_FALSE(response.is_gc_allowed());
    ASSERT_GT(response.get_gc_token(), token);
    ASSERT_TRUE(leader_.is_gc_hold_(request.src_));
    adopt(request, response);
  }
  void approve(ObLSGCRequest &request)
  {
    ObLSGCResponse response;
    ASSERT_EQ(OB_SUCCESS, leader_.handle_request(request, response));
    ASSERT_TRUE(response.is_gc_allowed());
    ASSERT_EQ(request.get_gc_token(), response.get_gc_token());
    ASSERT_TRUE(leader_.is_gc_approved_(request.src_));
    ASSERT_EQ(-1, leader_.find_replica_cache_(leader_.retained_replica_caches_, request.src_));
  }
  void reject_stale(const ObLSGCRequest &request)
  {
    ObLSGCResponse response;
    const int64_t tokens = leader_.next_gc_token_;
    ASSERT_EQ(OB_SUCCESS, leader_.handle_request(request, response));
    ASSERT_FALSE(response.is_gc_allowed());
    ASSERT_EQ(INVALID_GC_TOKEN, response.get_gc_token());
    ASSERT_EQ(tokens, leader_.next_gc_token_);
  }
  void prepare_coverage(MockRetentionGCReplica &replica)
  {
    ASSERT_EQ(OB_SUCCESS, replica.init(ls_));
    replica.leader_ = &leader_;
    bool allow_gc = false;
    ASSERT_EQ(OB_SUCCESS, replica.check_permission_with_end_scn_(replica.end_, allow_gc));
    ASSERT_FALSE(allow_gc);
    ASSERT_EQ(OB_SUCCESS, replica.check_permission_with_end_scn_(replica.end_, allow_gc));
    ASSERT_FALSE(allow_gc);
    ObLSGCRequest other = request(addr(3), 0, 200, 10);
    handshake(other);
    hold(other);
    ASSERT_EQ(OB_SUCCESS, leader_.refresh_cache());
  }
  ObTenantBase tenant_;
  ObLS *ls_;
  MockRetentionGCLeader leader_;
};

TEST_F(TestRetentionGCTokens, handshake_never_allows_even_with_zero_copies)
{
  set_copies(0);
  ObLSGCRequest current = request(addr(2), 20, 100, 10);
  ObLSGCRequest obsolete = request(addr(3), 20, 100, 9);
  handshake(current);
  handshake(obsolete);
  ASSERT_TRUE(leader_.retained_replica_caches_.empty());
  ASSERT_EQ(2, leader_.gc_requests_.count());
  ASSERT_NE(current.get_gc_token(), obsolete.get_gc_token());
  approve(current);
  approve(obsolete);
}

TEST_F(TestRetentionGCTokens, rejected_formal_request_consumes_token_and_reports_range)
{
  ObLSGCRequest current = request(addr(2), 20, 100, 10);
  handshake(current);
  const ObLSGCRequest old = current;
  hold(current);
  ASSERT_EQ(1, leader_.retained_replica_caches_.count());
  const ObLSGCReplicaCache cached = leader_.retained_replica_caches_.at(0);
  reject_stale(old);
  ASSERT_EQ(cached.get_begin_scn(), leader_.retained_replica_caches_.at(0).get_begin_scn());
  ASSERT_EQ(cached.get_last_refresh_time(), leader_.retained_replica_caches_.at(0).get_last_refresh_time());
  hold(current);
  ASSERT_EQ(1, leader_.gc_requests_.count());
}

TEST_F(TestRetentionGCTokens, many_rounds_of_delayed_requests_cannot_restore_approved_hold)
{
  ObArray<ObLSGCRequest> delayed;
  ObLSGCRequest current = request(addr(2), 20, 100, 10);
  handshake(current);
  for (int64_t i = 0; i < 64; ++i) {
    ASSERT_EQ(OB_SUCCESS, delayed.push_back(current));
    hold(current);
  }
  ASSERT_EQ(1, leader_.gc_requests_.count());
  set_copies(0);
  approve(current);
  for (int64_t i = delayed.count() - 1; i >= 0; --i) {
    reject_stale(delayed.at(i));
    ASSERT_TRUE(leader_.is_gc_approved_(addr(2)));
    ASSERT_TRUE(leader_.retained_replica_caches_.empty());
  }
  approve(current);
}

TEST_F(TestRetentionGCTokens, requester_restart_then_old_request_cannot_allow_both_replicas)
{
  set_copies(1);
  ObLSGCRequest old = request(addr(2), 20, 100, 10);
  handshake(old);
  hold(old);
  // A new requester has no token; its handshake withdraws the old process's range.
  ObLSGCRequest restarted = request(addr(2), 30, 100, 10);
  handshake(restarted);
  ObLSGCRequest other = request(addr(3), 30, 100, 10);
  handshake(other);
  hold(other);
  leader_.begin_ = scn(80);
  leader_.end_ = scn(100);
  ASSERT_EQ(OB_SUCCESS, leader_.refresh_cache());
  approve(restarted);
  reject_stale(old);
  ASSERT_FALSE(leader_.is_gc_hold_(addr(2)));
  // Member coverage starts at 80: B must retain [30,80) after A's approval.
  hold(other);
  ASSERT_TRUE(leader_.is_gc_hold_(addr(3)));
  ASSERT_TRUE(leader_.is_gc_approved_(addr(2)));
}

TEST_F(TestRetentionGCTokens, delayed_handshake_withdraws_hold_without_granting_permission)
{
  ObLSGCRequest delayed_handshake = request(addr(2), 20, 100, 10);
  ObLSGCRequest current = delayed_handshake;
  handshake(current);
  hold(current);
  const ObLSGCRequest delayed_formal = current;
  handshake(delayed_handshake);
  ASSERT_TRUE(leader_.retained_replica_caches_.empty());
  ASSERT_GT(delayed_handshake.get_gc_token(), current.get_gc_token());
  reject_stale(delayed_formal);
  ObLSGCRequest recovered = request(addr(2), 30, 100, 10);
  handshake(recovered);
  hold(recovered);
  ASSERT_EQ(scn(30), leader_.retained_replica_caches_.at(0).get_begin_scn());
}

TEST_F(TestRetentionGCTokens, lost_handshake_and_deny_responses_require_new_tokens)
{
  ObLSGCRequest initial = request(addr(2), 20, 100, 10);
  ObLSGCRequest first = initial;
  handshake(first);
  ObLSGCRequest retry = initial;
  handshake(retry);
  reject_stale(first);
  ObLSGCResponse lost;
  ASSERT_EQ(OB_SUCCESS, leader_.handle_request(retry, lost));
  ASSERT_FALSE(lost.is_gc_allowed());
  ASSERT_TRUE(leader_.is_gc_hold_(addr(2)));
  ASSERT_GT(lost.get_gc_token(), retry.get_gc_token());
  // The caller kept its old token because the response was lost.
  reject_stale(retry);
  ObLSGCRequest next = initial;
  handshake(next);
  ASSERT_FALSE(leader_.is_gc_hold_(addr(2)));
  hold(next);
}

TEST_F(TestRetentionGCTokens, lost_allow_response_retries_idempotently_and_removal_is_sticky)
{
  MockRetentionGCReplica replica;
  prepare_coverage(replica);
  ASSERT_FALSE(HasFatalFailure());
  replica.drop_allow_response_ = true;
  bool allow_gc = false;
  const int64_t token = replica.gc_token_;
  ASSERT_EQ(OB_TIMEOUT, replica.check_permission_with_end_scn_(replica.end_, allow_gc));
  ASSERT_FALSE(allow_gc);
  ASSERT_FALSE(ATOMIC_LOAD(&replica.gc_started_));
  ASSERT_TRUE(leader_.is_gc_approved_(GCTX.self_addr()));
  ASSERT_EQ(token, replica.gc_token_);
  const ObLSGCRequest delayed = replica.request_;
  leader_.reuse();
  replica.drop_allow_response_ = false;
  ASSERT_EQ(OB_SUCCESS, replica.check_permission_with_end_scn_(replica.end_, allow_gc));
  ASSERT_TRUE(allow_gc);
  ASSERT_TRUE(ATOMIC_LOAD(&replica.gc_started_));
  const int64_t calls = replica.calls_;
  for (int64_t i = 0; i < 3; ++i) {
    ASSERT_EQ(OB_SUCCESS, replica.check_permission(allow_gc));
    ASSERT_TRUE(allow_gc);
    ASSERT_EQ(calls, replica.calls_);
  }
  ObLSGCResponse response;
  ASSERT_EQ(OB_SUCCESS, leader_.handle_request(delayed, response));
  ASSERT_TRUE(response.is_gc_allowed());
  ASSERT_TRUE(leader_.retained_replica_caches_.empty());
}

TEST_F(TestRetentionGCTokens, requester_restart_after_approval_withdraws_old_permission)
{
  MockRetentionGCReplica replica;
  prepare_coverage(replica);
  ASSERT_FALSE(HasFatalFailure());
  bool allow_gc = false;
  ASSERT_EQ(OB_SUCCESS, replica.check_permission_with_end_scn_(replica.end_, allow_gc));
  ASSERT_TRUE(allow_gc);
  const ObLSGCRequest old = replica.request_;
  MockRetentionGCReplica restarted;
  ASSERT_EQ(OB_SUCCESS, restarted.init(ls_));
  restarted.leader_ = &leader_;
  ASSERT_EQ(INVALID_GC_TOKEN, restarted.gc_token_);
  ASSERT_EQ(OB_SUCCESS, restarted.check_permission_with_end_scn_(restarted.end_, allow_gc));
  ASSERT_FALSE(allow_gc);
  ASSERT_FALSE(leader_.is_gc_approved_(GCTX.self_addr()));
  ASSERT_FALSE(leader_.is_gc_hold_(GCTX.self_addr()));
  reject_stale(old);
  ASSERT_EQ(OB_SUCCESS, restarted.check_permission_with_end_scn_(restarted.end_, allow_gc));
  ASSERT_TRUE(allow_gc);
  ASSERT_GT(restarted.gc_token_, old.get_gc_token());
}

TEST_F(TestRetentionGCTokens, leader_restart_rejects_old_config_even_when_token_numbers_match)
{
  ObLSGCRequest old = request(addr(2), 20, 100, 10);
  handshake(old);
  MockRetentionGCLeader restarted;
  ASSERT_EQ(OB_SUCCESS, restarted.init(ls_));
  restarted.leader_stat_ = leader_.leader_stat_;
  ASSERT_EQ(OB_SUCCESS, restarted.leader_stat_.config_version_.inc_update_version(11));
  ObLSGCRequest fresh = request(addr(2), 20, 100, 10);
  ObLSGCResponse response;
  ASSERT_EQ(OB_SUCCESS, restarted.handle_request(fresh, response));
  ASSERT_FALSE(response.is_gc_allowed());
  ASSERT_EQ(old.get_gc_token(), response.get_gc_token());
  adopt(fresh, response);
  ASSERT_NE(old.get_config_version(), fresh.get_config_version());
  ASSERT_EQ(OB_SUCCESS, restarted.handle_request(old, response));
  ASSERT_FALSE(response.is_gc_allowed());
  ASSERT_EQ(INVALID_GC_TOKEN, response.get_gc_token());
  ASSERT_TRUE(restarted.retained_replica_caches_.empty());
  ASSERT_EQ(OB_SUCCESS, restarted.handle_request(fresh, response));
  ASSERT_FALSE(response.is_gc_allowed());
  ASSERT_GT(response.get_gc_token(), fresh.get_gc_token());
  ASSERT_TRUE(restarted.is_gc_hold_(addr(2)));
}

TEST_F(TestRetentionGCTokens, leader_aba_discards_old_hold_and_approval)
{
  ObLSGCRequest old = request(addr(2), 20, 100, 10);
  handshake(old);
  hold(old);
  set_copies(0);
  approve(old);
  ObLSGCResponse response;
  leader_.leader_stat_.role_ = FOLLOWER;
  ASSERT_EQ(OB_NOT_MASTER, leader_.handle_request(old, response));
  ASSERT_FALSE(response.is_gc_allowed());
  ASSERT_TRUE(leader_.is_gc_approved_(addr(2)));
  leader_.reuse();
  ASSERT_EQ(OB_SUCCESS, leader_.leader_stat_.config_version_.inc_update_version(11));
  leader_.leader_stat_.role_ = LEADER;
  reject_stale(old);
  ASSERT_TRUE(leader_.gc_requests_.empty());
  ObLSGCRequest fresh = request(addr(2), 20, 100, 10);
  handshake(fresh);
  ASSERT_GT(fresh.get_gc_token(), old.get_gc_token());
  approve(fresh);
}

TEST_F(TestRetentionGCTokens, same_config_mode_change_never_reuses_token)
{
  ObLSGCRequest old = request(addr(2), 20, 100, 10);
  handshake(old);
  hold(old);
  const int64_t token = leader_.next_gc_token_;
  leader_.leader_stat_.mode_version_ = 11;
  reject_stale(old);
  ASSERT_TRUE(leader_.gc_requests_.empty());
  ASSERT_TRUE(leader_.retained_replica_caches_.empty());
  ASSERT_EQ(token, leader_.next_gc_token_);
  ObLSGCRequest fresh = request(addr(2), 20, 100, 11);
  handshake(fresh);
  ASSERT_GT(fresh.get_gc_token(), token);
  reject_stale(old);
  hold(fresh);
}

TEST_F(TestRetentionGCTokens, mismatched_mode_token_or_config_does_not_touch_current_hold)
{
  ObLSGCRequest current = request(addr(2), 20, 100, 10);
  handshake(current);
  hold(current);
  ObLSGCRequest stale;
  const LogConfigVersion wrong = MockRetentionGCLeader::version(9, 1);
  for (int64_t i = 0; i < 3; ++i) {
    ASSERT_EQ(OB_SUCCESS, stale.init(current.src_, current.get_ls_id(), scn(0), scn(200),
        0 == i ? wrong : current.get_config_version(), 1 == i ? 9 : 10,
        LSN(100), LSN(500), current.get_gc_token() + (2 == i ? 100 : 0)));
    reject_stale(stale);
    ASSERT_EQ(scn(20), leader_.retained_replica_caches_.at(0).get_begin_scn());
    ASSERT_EQ(scn(100), leader_.retained_replica_caches_.at(0).get_end_scn());
    ASSERT_EQ(current.get_gc_token(), leader_.gc_requests_.at(0).token_);
  }
}

TEST_F(TestRetentionGCTokens, obsolete_mode_requires_handshake_and_protects_members)
{
  ObLSGCRequest old = request(addr(2), 20, 100, 9);
  handshake(old);
  approve(old);
  ObLSGCResponse response;
  ASSERT_EQ(OB_SUCCESS, leader_.leader_stat_.paxos_member_list_.add_server(addr(2)));
  ASSERT_EQ(OB_STATE_NOT_MATCH, leader_.handle_request(old, response));
  ASSERT_FALSE(response.is_gc_allowed());
  ObLSGCRequest learner = request(addr(3), 20, 100, 9);
  ASSERT_EQ(OB_SUCCESS, leader_.leader_stat_.learner_list_.add_server(addr(3)));
  ASSERT_EQ(OB_STATE_NOT_MATCH, leader_.handle_request(learner, response));
  ASSERT_FALSE(response.is_gc_allowed());
}

TEST_F(TestRetentionGCTokens, final_leader_check_rejects_config_mode_and_role_changes)
{
  ObLSGCRequest current = request(addr(2), 20, 100, 10);
  handshake(current);
  hold(current);
  set_copies(0);
  ObLSGCResponse response;
  leader_.on_stat_ = [](MockRetentionGCLeader *self) {
    self->on_stat_ = NULL;
    self->leader_stat_.mode_version_ = 11;
  };
  ASSERT_EQ(OB_EAGAIN, leader_.handle_request(current, response));
  ASSERT_FALSE(response.is_gc_allowed());
  ASSERT_FALSE(leader_.is_gc_approved_(addr(2)));
  leader_.leader_stat_.mode_version_ = 10;
  leader_.on_stat_ = [](MockRetentionGCLeader *self) {
    self->on_stat_ = NULL;
    EXPECT_EQ(OB_SUCCESS, self->leader_stat_.config_version_.inc_update_version(11));
  };
  ASSERT_EQ(OB_EAGAIN, leader_.handle_request(current, response));
  ASSERT_FALSE(response.is_gc_allowed());
  leader_.leader_stat_.config_version_ = current.get_config_version();
  leader_.on_stat_ = [](MockRetentionGCLeader *self) {
    self->on_stat_ = NULL;
    self->leader_stat_.role_ = FOLLOWER;
  };
  ASSERT_EQ(OB_NOT_MASTER, leader_.handle_request(current, response));
  ASSERT_FALSE(response.is_gc_allowed());
}

TEST_F(TestRetentionGCTokens, final_leader_check_rejects_a_changed_handshake_view)
{
  ObLSGCRequest initial = request(addr(2), 20, 100, 10);
  ObLSGCResponse response;
  leader_.on_stat_ = [](MockRetentionGCLeader *self) {
    self->on_stat_ = NULL;
    ++self->leader_stat_.mode_version_;
  };
  ASSERT_EQ(OB_EAGAIN, leader_.handle_request(initial, response));
  ASSERT_FALSE(response.is_gc_allowed());
  ASSERT_EQ(INVALID_GC_TOKEN, response.get_gc_token());
  ASSERT_TRUE(leader_.gc_requests_.empty());
  ASSERT_EQ(INVALID_GC_TOKEN, leader_.next_gc_token_);
}

TEST_F(TestRetentionGCTokens, cache_reuse_ttl_and_mode_resets_preserve_allocator_progress)
{
  ObLSGCRequest current = request(addr(2), 20, 100, 10);
  handshake(current);
  hold(current);
  const int64_t token = current.get_gc_token();
  const int64_t now = leader_.retained_replica_caches_.at(0).get_last_refresh_time();
  ObArray<ObLSGCReplicaCache> ranges;
  ASSERT_EQ(OB_SUCCESS, leader_.collect_replica_ranges_(addr(3), now + RetentionGCLeader::CACHE_TTL, ranges));
  ASSERT_TRUE(ranges.empty());
  ASSERT_EQ(token, leader_.gc_requests_.at(0).token_);
  leader_.reuse();
  ASSERT_EQ(token, leader_.next_gc_token_);
  ASSERT_TRUE(leader_.retained_replica_caches_.empty());
  hold(current);
  ASSERT_GT(current.get_gc_token(), token);
  set_copies(0);
  approve(current);
  leader_.reuse();
  ASSERT_TRUE(leader_.is_gc_approved_(addr(2)));
  approve(current);
  const int64_t last_token = leader_.next_gc_token_;
  leader_.reset();
  ASSERT_TRUE(leader_.gc_requests_.empty());
  ASSERT_EQ(last_token, leader_.next_gc_token_);
  ASSERT_EQ(OB_SUCCESS, leader_.init(ls_));
  ObLSGCRequest fresh = request(addr(2), 20, 100, 10);
  handshake(fresh);
  ASSERT_GT(fresh.get_gc_token(), last_token);
}

TEST_F(TestRetentionGCTokens, token_exhaustion_keeps_hold_and_approved_retries_work)
{
  ObLSGCRequest current = request(addr(2), 20, 100, 10);
  handshake(current);
  hold(current);
  const int64_t old_token = current.get_gc_token();
  leader_.next_gc_token_ = INT64_MAX;
  ObLSGCResponse response;
  ASSERT_EQ(OB_SIZE_OVERFLOW, leader_.handle_request(current, response));
  ASSERT_FALSE(response.is_gc_allowed());
  ASSERT_EQ(old_token, leader_.gc_requests_.at(0).token_);
  ASSERT_TRUE(leader_.is_gc_hold_(addr(2)));
  ObLSGCRequest handshake_request = request(addr(2), 30, 100, 10);
  ASSERT_EQ(OB_SIZE_OVERFLOW, leader_.handle_request(handshake_request, response));
  ASSERT_TRUE(leader_.is_gc_hold_(addr(2)));
  ASSERT_EQ(scn(20), leader_.retained_replica_caches_.at(0).get_begin_scn());
  set_copies(0);
  approve(current);
  approve(current);
  ASSERT_EQ(INT64_MAX, leader_.next_gc_token_);
}

TEST_F(TestRetentionGCTokens, expired_request_cannot_change_tokens_or_coverage)
{
  ObLSGCRequest current = request(addr(2), 20, 100, 10);
  handshake(current);
  hold(current);
  const int64_t saved_timeout = THIS_WORKER.get_timeout_ts();
  DEFER(THIS_WORKER.set_timeout_ts(saved_timeout));
  THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() - 1000000);
  ObLSGCResponse response;
  ASSERT_EQ(OB_TIMEOUT, leader_.handle_request(current, response));
  ASSERT_FALSE(response.is_gc_allowed());
  ASSERT_EQ(current.get_gc_token(), leader_.gc_requests_.at(0).token_);
  ASSERT_TRUE(leader_.is_gc_hold_(addr(2)));
  THIS_WORKER.set_timeout_ts(saved_timeout);
  leader_.on_stat_ = [](MockRetentionGCLeader *self) {
    self->on_stat_ = NULL;
    THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() - 1000000);
  };
  ASSERT_EQ(OB_TIMEOUT, leader_.handle_request(current, response));
  ASSERT_EQ(current.get_gc_token(), leader_.gc_requests_.at(0).token_);
  ASSERT_FALSE(response.is_gc_allowed());
}

TEST_F(TestRetentionGCTokens, requester_mode_and_read_failures_preserve_last_token)
{
  MockRetentionGCReplica replica;
  ASSERT_EQ(OB_SUCCESS, replica.init(ls_));
  replica.leader_ = &leader_;
  bool allow_gc = false;
  ASSERT_EQ(OB_SUCCESS, replica.check_permission_with_end_scn_(replica.end_, allow_gc));
  ASSERT_EQ(OB_SUCCESS, replica.check_permission_with_end_scn_(replica.end_, allow_gc));
  const int64_t token = replica.gc_token_;
  const int64_t calls = replica.calls_;
  const int64_t timestamp = leader_.retained_replica_caches_.at(0).get_last_refresh_time();
  replica.report_range_ = false;
  ASSERT_EQ(OB_EAGAIN, replica.check_permission_with_end_scn_(replica.end_, allow_gc));
  ASSERT_EQ(calls, replica.calls_);
  ASSERT_EQ(token, replica.gc_token_);
  ASSERT_EQ(timestamp, leader_.retained_replica_caches_.at(0).get_last_refresh_time());
  replica.mode_result_ = OB_IO_ERROR;
  ASSERT_EQ(OB_IO_ERROR, replica.check_permission_with_end_scn_(replica.end_, allow_gc));
  ASSERT_EQ(calls, replica.calls_);
  replica.report_range_ = true;
  replica.mode_result_ = OB_SUCCESS;
  replica.mode_version_ = 9;
  ASSERT_EQ(OB_SUCCESS, replica.check_permission_with_end_scn_(replica.end_, allow_gc));
  ASSERT_FALSE(allow_gc);
  ASSERT_EQ(INVALID_GC_TOKEN, replica.gc_token_);
  ASSERT_EQ(9, replica.request_.get_mode_version());
  ASSERT_EQ(OB_SUCCESS, replica.check_permission_with_end_scn_(replica.end_, allow_gc));
  ASSERT_FALSE(allow_gc);
  ASSERT_EQ(OB_SUCCESS, replica.check_permission_with_end_scn_(replica.end_, allow_gc));
  ASSERT_TRUE(allow_gc);
}

TEST_F(TestRetentionGCTokens, concurrent_approvals_leave_one_retained_replica)
{
  ObLSGCRequest a = request(addr(2), 0, 200, 10);
  ObLSGCRequest b = request(addr(3), 0, 200, 10);
  handshake(a);
  hold(a);
  handshake(b);
  hold(b);
  ASSERT_EQ(OB_SUCCESS, leader_.refresh_cache());
  struct Context
  {
    MockRetentionGCLeader *leader_;
    ObTenantBase *tenant_;
    ObLSGCRequest request_;
    ObLSGCResponse response_;
    int ret_;
  };
  Context first = {&leader_, &tenant_, a, ObLSGCResponse(), OB_ERR_UNEXPECTED};
  Context second = {&leader_, &tenant_, b, ObLSGCResponse(), OB_ERR_UNEXPECTED};
  auto run = [](void *arg) -> void * {
    Context *ctx = static_cast<Context *>(arg);
    ObTenantEnv::set_tenant(ctx->tenant_);
    ctx->ret_ = ctx->leader_->handle_request(ctx->request_, ctx->response_);
    ObTenantEnv::set_tenant(NULL);
    return NULL;
  };
  pthread_t threads[2];
  ASSERT_EQ(0, pthread_create(&threads[0], NULL, run, &first));
  ASSERT_EQ(0, pthread_create(&threads[1], NULL, run, &second));
  ASSERT_EQ(0, pthread_join(threads[0], NULL));
  ASSERT_EQ(0, pthread_join(threads[1], NULL));
  ASSERT_EQ(OB_SUCCESS, first.ret_);
  ASSERT_EQ(OB_SUCCESS, second.ret_);
  ASSERT_NE(first.response_.is_gc_allowed(), second.response_.is_gc_allowed());
  ASSERT_EQ(1, leader_.approved_count());
  ASSERT_EQ(1, leader_.retained_replica_caches_.count());
}

TEST_F(TestRetentionGCTokens, wrong_ls_and_invalid_mode_never_create_tokens)
{
  ObLSGCRequest current = request(addr(2), 20, 100, 10);
  ObLSGCRequest invalid;
  ObLSGCResponse response;
  ASSERT_EQ(OB_SUCCESS, invalid.init(addr(2), ObLSID(1002), scn(20), scn(100),
      LogConfigVersion(), 10, LSN(100), LSN(200), 0));
  ASSERT_EQ(OB_INVALID_ARGUMENT, leader_.handle_request(invalid, response));
  ASSERT_EQ(OB_SUCCESS, invalid.init(addr(2), ObLSID(1001), scn(20), scn(100),
      LogConfigVersion(), INVALID_PROPOSAL_ID, LSN(100), LSN(200), 0));
  ASSERT_EQ(OB_EAGAIN, leader_.handle_request(invalid, response));
  invalid = request(addr(2), 20, 100, 11);
  ASSERT_EQ(OB_EAGAIN, leader_.handle_request(invalid, response));
  ASSERT_TRUE(leader_.gc_requests_.empty());
  ASSERT_EQ(INVALID_GC_TOKEN, leader_.next_gc_token_);
}


TEST_F(TestRetentionGCTokens, allocation_failure_does_not_publish_a_token_or_hold)
{
  class FailingAllocator : public ObIAllocator
  {
  public:
    void *alloc(const int64_t size) override { UNUSED(size); return NULL; }
    void *alloc(const int64_t size, const ObMemAttr &attr) override
    { UNUSED(size); UNUSED(attr); return NULL; }
    void free(void *ptr) override { EXPECT_EQ(nullptr, ptr); }
  } allocator;
  const ModulePageAllocator saved_tokens = leader_.gc_requests_.get_block_allocator();
  const ModulePageAllocator saved_ranges = leader_.retained_replica_caches_.get_block_allocator();
  DEFER({
    leader_.gc_requests_.set_block_allocator(saved_tokens);
    leader_.retained_replica_caches_.set_block_allocator(saved_ranges);
  });
  ObLSGCRequest current = request(addr(2), 20, 100, 10);
  ObLSGCResponse response;
  leader_.gc_requests_.set_block_allocator(ModulePageAllocator(allocator));
  ASSERT_EQ(OB_ALLOCATE_MEMORY_FAILED, leader_.handle_request(current, response));
  ASSERT_FALSE(response.is_gc_allowed());
  ASSERT_EQ(INVALID_GC_TOKEN, response.get_gc_token());
  ASSERT_TRUE(leader_.gc_requests_.empty());
  ASSERT_EQ(1, leader_.next_gc_token_);
  leader_.gc_requests_.set_block_allocator(saved_tokens);
  handshake(current);
  ASSERT_EQ(2, current.get_gc_token());
  leader_.retained_replica_caches_.set_block_allocator(ModulePageAllocator(allocator));
  ASSERT_EQ(OB_ALLOCATE_MEMORY_FAILED, leader_.handle_request(current, response));
  ASSERT_FALSE(response.is_gc_allowed());
  ASSERT_EQ(INVALID_GC_TOKEN, response.get_gc_token());
  ASSERT_TRUE(leader_.retained_replica_caches_.empty());
  ASSERT_EQ(2, leader_.gc_requests_.at(0).token_);
  ASSERT_EQ(Phase::PREPARE, leader_.gc_requests_.at(0).phase_);
  leader_.retained_replica_caches_.set_block_allocator(saved_ranges);
  const ObLSGCRequest delayed = current;
  hold(current);
  ASSERT_GT(current.get_gc_token(), 3);
  reject_stale(delayed);
}

TEST_F(TestRetentionGCTokens, prepare_and_approved_records_never_supply_fetch_sources)
{
  ObLSGCRequest current = request(addr(2), 20, 100, 10);
  handshake(current);
  hold(current);
  ASSERT_EQ(OB_SUCCESS, leader_.refresh_cache());
  const ObLSGCReplicaCache old_range = leader_.retained_replica_caches_.at(0);
  LogGetFetchSourcesResp sources;
  ASSERT_EQ(OB_SUCCESS, leader_.get_log_fetch_sources(sources));
  ASSERT_EQ(2, sources.sources_.count());
  current = request(addr(2), 20, 100, 10);
  handshake(current);
  // Even a leftover range cannot bypass the token phase check.
  ASSERT_EQ(OB_SUCCESS, leader_.retained_replica_caches_.push_back(old_range));
  ASSERT_EQ(OB_SUCCESS, leader_.get_log_fetch_sources(sources));
  ASSERT_EQ(1, sources.sources_.count());
  ASSERT_EQ(addr(4), sources.sources_.at(0).server_);
  set_copies(0);
  approve(current);
  ASSERT_EQ(OB_SUCCESS, leader_.retained_replica_caches_.push_back(old_range));
  ObArray<ObLSGCReplicaCache> ranges;
  ASSERT_EQ(OB_SUCCESS, leader_.collect_replica_ranges_(
      addr(3), ObTimeUtility::current_monotonic_time(), ranges));
  ASSERT_EQ(1, ranges.count());
  ASSERT_EQ(addr(4), ranges.at(0).get_server());
  ASSERT_EQ(OB_SUCCESS, leader_.get_log_fetch_sources(sources));
  ASSERT_EQ(1, sources.sources_.count());
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObClockGenerator::init();
  oceanbase::common::ObClusterVersion::get_instance().update_cluster_version(CLUSTER_CURRENT_VERSION);
  oceanbase::common::ObClusterVersion::get_instance().update_data_version(DATA_CURRENT_VERSION);
  GCTX.self_addr_seq_.set_addr(
      oceanbase::common::ObAddr(oceanbase::common::ObAddr::IPV4, "127.0.0.1", 2882));
  OB_LOGGER.set_file_name("test_ob_garbage_collector.log", true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
