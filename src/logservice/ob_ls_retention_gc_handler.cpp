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

#include "ob_ls_retention_gc_handler.h"

#include <algorithm>
#include "lib/atomic/ob_atomic.h"
#include "lib/utility/ob_sort.h"
#include "lib/utility/ob_tracepoint.h"
#include "lib/utility/utility.h"
#include "lib/time/ob_time_utility.h"
#include "lib/worker.h"
#include "ob_log_service.h"
#include "ob_server_log_block_mgr.h"
#include "logrpc/ob_log_rpc_req.h"
#include "palf_handle_guard.h"
#include "logrpc/ob_log_rpc_proxy.h"
#include "common/ob_clock_generator.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "share/config/ob_server_config.h"
#include "share/location_cache/ob_location_service.h"
#include "share/rc/ob_tenant_base.h"
#include "share/rpc/ob_async_rpc_proxy.h"
#include "storage/ls/ob_ls.h"

ERRSIM_POINT_DEF(EN_RETAINED_LS_GC_BEFORE_PERMISSION);
ERRSIM_POINT_DEF(EN_RETAINED_LS_GC_AFTER_PERMISSION);
ERRSIM_POINT_DEF(EN_BYPASS_SYS_TENANT_RETAINED_LS_GC);

namespace oceanbase
{
using namespace common;
using namespace palf;
using namespace share;
using namespace storage;
namespace logservice
{

const int64_t ObLSRetentionGCHandler::Leader::CACHE_TTL;
const int64_t ObLSRetentionGCHandler::Leader::ACTIVE_WINDOW;

ObLSGCReplicaCache::ObLSGCReplicaCache()
  : server_(), begin_scn_(), end_scn_(), begin_lsn_(), end_lsn_(), last_refresh_time_(0)
{
}

ObLSGCReplicaCache::ObLSGCReplicaCache(const common::ObAddr &server)
  : server_(server), begin_scn_(), end_scn_(), begin_lsn_(), end_lsn_(), last_refresh_time_(0)
{
}

int ObLSGCReplicaCache::init(const common::ObAddr &server,
    const share::SCN &begin_scn, const share::SCN &end_scn,
    const palf::LSN &begin_lsn, const palf::LSN &end_lsn, const int64_t last_refresh_time)
{
  int ret = OB_SUCCESS;
  if (!server.is_valid() || !begin_scn.is_valid() || !end_scn.is_valid()
      || begin_scn > end_scn || !begin_lsn.is_valid() || !end_lsn.is_valid()
      || begin_lsn > end_lsn || last_refresh_time <= 0) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid replica cache argument", K(ret), K(server), K(begin_scn), K(end_scn),
        K(begin_lsn), K(end_lsn), K(last_refresh_time));
  } else {
    server_ = server;
    begin_scn_ = begin_scn;
    end_scn_ = end_scn;
    begin_lsn_ = begin_lsn;
    end_lsn_ = end_lsn;
    last_refresh_time_ = last_refresh_time;
  }
  return ret;
}

ObLSRetentionGCHandler::ObLSRetentionGCHandler()
  : leader_(), gc_replica_()
{
}

ObLSRetentionGCHandler::~ObLSRetentionGCHandler()
{
}

int ObLSRetentionGCHandler::init(storage::ObLS *ls)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(leader_.init(ls))) {
    CLOG_LOG(WARN, "init retention gc leader failed", K(ret), KP(ls));
  } else if (OB_FAIL(gc_replica_.init(ls))) {
    CLOG_LOG(WARN, "init retention gc replica failed", K(ret), KP(ls));
    leader_.reset();
  }
  return ret;
}

void ObLSRetentionGCHandler::reset()
{
  leader_.reset();
  gc_replica_.reset();
}

void ObLSRetentionGCHandler::reset_leader_state()
{
  leader_.reuse();
}

int ObLSRetentionGCHandler::refresh_cache()
{
  return leader_.refresh_cache();
}

int ObLSRetentionGCHandler::check_permission(bool &allow_gc)
{
  return gc_replica_.check_permission(allow_gc);
}

int ObLSRetentionGCHandler::handle_request(
    const ObLSGCRequest &request, ObLSGCResponse &response)
{
  return leader_.handle_request(request, response);
}

int ObLSRetentionGCHandler::get_log_fetch_sources(LogGetFetchSourcesResp &response)
{
  return leader_.get_log_fetch_sources(response);
}

ObLSRetentionGCHandler::Leader::Leader()
  : ls_(NULL),
    lock_(),
    member_replica_caches_(),
    retained_replica_caches_(),
    gc_requests_(),
    next_gc_token_(INVALID_GC_TOKEN),
    config_version_(),
    mode_version_(INVALID_PROPOSAL_ID),
    last_request_time_(0)
{
}

ObLSRetentionGCHandler::Leader::~Leader()
{
  reset();
}

int ObLSRetentionGCHandler::Leader::init(storage::ObLS *ls)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(ls_)) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(WARN, "retention gc leader has already been inited", K(ret), KP(ls_), KP(ls));
  } else if (OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), KP(ls));
  } else {
    ls_ = ls;
  }
  return ret;
}

void ObLSRetentionGCHandler::Leader::reset()
{
  lib::ObMutexGuard guard(lock_);
  ls_ = NULL;
  reset_replica_caches_();
  member_replica_caches_.reset();
  retained_replica_caches_.reset();
  gc_requests_.reset();
  config_version_.reset();
  mode_version_ = INVALID_PROPOSAL_ID;
}

void ObLSRetentionGCHandler::Leader::reuse()
{
  lib::ObMutexGuard guard(lock_);
  reset_replica_caches_();
}

void ObLSRetentionGCHandler::Leader::reset_replica_caches_()
{
  member_replica_caches_.reuse();
  retained_replica_caches_.reuse();
  last_request_time_ = 0;
  // Keep approvals through cache resets in the same configuration and access mode.
}

int ObLSRetentionGCHandler::Leader::update_leader_state_(const palf::PalfStat &stat)
{
  int ret = OB_SUCCESS;
  if (INVALID_PROPOSAL_ID == stat.mode_version_ || stat.mode_version_ < 0
      || stat.config_version_ < config_version_
      || (INVALID_PROPOSAL_ID != mode_version_ && stat.mode_version_ < mode_version_)) {
    // Never replace newer cached state with an older PALF snapshot.
    ret = OB_EAGAIN;
    CLOG_LOG(WARN, "leader stat is older than retention cache state", K(ret), K(stat),
        K_(config_version), K_(mode_version));
  } else if (config_version_ != stat.config_version_ || mode_version_ != stat.mode_version_) {
    reset_replica_caches_();
    gc_requests_.reuse();
    config_version_ = stat.config_version_;
    mode_version_ = stat.mode_version_;
  }
  return ret;
}

bool ObLSRetentionGCHandler::Leader::is_gc_request_within_active_window_(const int64_t now) const
{
  return last_request_time_ > 0 && now >= last_request_time_
      && now - last_request_time_ < ACTIVE_WINDOW;
}

int ObLSRetentionGCHandler::Leader::check_log_coverage(
    const common::ObIArray<ObLSGCReplicaCache> &replicas,
    const share::SCN &cutoff_scn,
    const share::SCN &end_scn,
    const int64_t required_log_copies,
    bool &is_covered)
{
  // Clip each replica range to [cutoff_scn, end_scn), then sweep start/end
  // events in SCN order. Every nonempty segment must retain the required copies.
  struct CoverageEvent {
    CoverageEvent() : scn_(), delta_(0) {}
    CoverageEvent(const SCN &scn, const int64_t delta) : scn_(scn), delta_(delta) {}
    bool operator<(const CoverageEvent &other) const
    {
      return scn_ < other.scn_ || (scn_ == other.scn_ && delta_ > other.delta_);
    }
    SCN scn_;
    int64_t delta_;
    TO_STRING_KV(K_(scn), K_(delta));
  };
  int ret = OB_SUCCESS;
  ObSEArray<CoverageEvent, 32> events;
  is_covered = false;
  if (!cutoff_scn.is_valid() || !end_scn.is_valid()
      || cutoff_scn > end_scn || required_log_copies < 0) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid log coverage argument", K(ret), K(cutoff_scn), K(end_scn),
        K(required_log_copies));
  } else if (0 == required_log_copies || cutoff_scn == end_scn) {
    is_covered = true;
  } else {
    SCN left;
    SCN right;
    for (int64_t i = 0; OB_SUCC(ret) && i < replicas.count(); ++i) {
      const ObLSGCReplicaCache &replica = replicas.at(i);
      if (replica.is_valid()) {
        left = std::max(replica.get_begin_scn(), cutoff_scn);
        right = std::min(replica.get_end_scn(), end_scn);
        if (left < right) {
          if (OB_FAIL(events.push_back(CoverageEvent(left, 1)))) {
            CLOG_LOG(WARN, "push coverage start event failed", K(ret), K(replica), K(left));
          } else if (OB_FAIL(events.push_back(CoverageEvent(right, -1)))) {
            CLOG_LOG(WARN, "push coverage end event failed", K(ret), K(replica), K(right));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      lib::ob_sort(events.begin(), events.end());
      int64_t coverage = 0;
      is_covered = !events.empty() && events.at(0).scn_ == cutoff_scn;
      for (int64_t i = 0; is_covered && i < events.count(); ++i) {
        const CoverageEvent &event = events.at(i);
        coverage += event.delta_;
        // Check after all events at this SCN, including starts at cutoff_scn.
        if (event.scn_ < end_scn
            && (i + 1 == events.count() || event.scn_ < events.at(i + 1).scn_)) {
          is_covered = coverage >= required_log_copies;
        }
      }
    }
  }
  return ret;
}

int ObLSRetentionGCHandler::Leader::get_leader_stat_(palf::PalfStat &palf_stat) const
{
  // PALF reports LEADER only after STARTWORKING advances the config version.
  int ret = OB_SUCCESS;
  ObLogService *log_service = MTL(ObLogService*);
  PalfHandleGuard palf_handle_guard;
  if (OB_ISNULL(log_service) || OB_ISNULL(ls_)) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "retention gc dependency is not ready", K(ret), KP(log_service), KP(ls_));
  } else if (OB_FAIL(log_service->open_palf(ls_->get_ls_id(), palf_handle_guard))) {
    CLOG_LOG(WARN, "open palf failed", K(ret), K(ls_->get_ls_id()));
  } else if (OB_FAIL(palf_handle_guard.stat(palf_stat))) {
    CLOG_LOG(WARN, "get leader palf stat failed", K(ret), K(ls_->get_ls_id()));
  } else if (!palf_stat.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "leader palf stat is invalid", K(ret), K(ls_->get_ls_id()), K(palf_stat));
  } else if (LEADER != palf_stat.role_) {
    ret = OB_NOT_MASTER;
    CLOG_LOG(WARN, "local palf is not leader", K(ret), K(ls_->get_ls_id()), K(palf_stat));
  } else if (!palf_stat.config_version_.is_valid()) {
    ret = OB_EAGAIN;
    CLOG_LOG(WARN, "local palf config version is invalid", K(ret), K(palf_stat));
  }
  return ret;
}

int ObLSRetentionGCHandler::Leader::get_replica_palf_stats_(
    const common::ObIArray<common::ObAddr> &servers,
    common::ObIArray<palf::PalfStat> &palf_stats) const
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLogService *log_service = MTL(ObLogService*);
  const int64_t timeout_us = GCONF.rpc_timeout;
  palf_stats.reuse();
  if (OB_ISNULL(ls_) || OB_ISNULL(log_service) || OB_ISNULL(log_service->get_rpc_proxy())) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "retention gc dependency is not ready", K(ret), KP(log_service), KP(ls_),
        "has_rpc_proxy", NULL != log_service && NULL != log_service->get_rpc_proxy());
  } else {
    using RpcProxy = obrpc::ObLogServiceAsyncRpcProxy;
    using StatProxy = obrpc::ObAsyncRpcProxy<obrpc::OB_LOG_GET_PALF_STAT,
        LogGetPalfStatReq, LogGetPalfStatResp, decltype(&RpcProxy::get_palf_stat_async), RpcProxy>;
    RpcProxy rpc_proxy(*log_service->get_rpc_proxy());
    StatProxy proxy(rpc_proxy, &RpcProxy::get_palf_stat_async);
    ObArray<int> return_codes;
    LogGetPalfStatReq request(GCTX.self_addr(), ls_->get_ls_id().id(), false/*is_to_leader*/);
    for (int64_t i = 0; OB_SUCC(ret) && i < servers.count(); ++i) {
      if (OB_TMP_FAIL(proxy.call(servers.at(i), timeout_us, GCONF.cluster_id,
          MTL_ID(), share::OBCG_STORAGE, request))) {
        CLOG_LOG(WARN, "send replica palf stat request failed", K(tmp_ret), K(i), "server", servers.at(i),
            K(timeout_us), K(static_cast<int64_t>(GCONF.cluster_id)), K(MTL_ID()), K(request));
      }
    }
    // Drain all posted requests even after a send failure.
    if (OB_FAIL(proxy.wait_all(return_codes))) {
      CLOG_LOG(WARN, "wait for replica palf stats failed", K(ret), K(servers), K(return_codes),
          K(request), K(timeout_us));
    } else if (OB_FAIL(proxy.check_return_cnt(return_codes.count()))) {
      CLOG_LOG(WARN, "replica stat reply count does not match", K(ret), K(request),
          "return_count", return_codes.count(), "dest_count", proxy.get_dests().count(),
          "result_count", proxy.get_results().count());
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < return_codes.count(); ++i) {
        const LogGetPalfStatResp *response = proxy.get_results().at(i);
        const ObAddr &server = proxy.get_dests().at(i);
        if (OB_SUCCESS != return_codes.at(i) || OB_ISNULL(response)
            || !response->palf_stat_.is_valid() || response->palf_stat_.self_ != server
            || response->palf_stat_.palf_id_ != ls_->get_ls_id().id()
            || !response->palf_stat_.begin_scn_.is_valid()
            || !response->palf_stat_.end_scn_.is_valid()
            || response->palf_stat_.begin_scn_ > response->palf_stat_.end_scn_) {
          CLOG_LOG(WARN, "invalid replica palf stat response", K(i), K(server), K(ls_->get_ls_id()),
              "return_code", return_codes.at(i), KPC(response));
        } else if (OB_FAIL(palf_stats.push_back(response->palf_stat_))) {
          CLOG_LOG(WARN, "append replica palf stat failed", K(ret), K(server), KPC(response));
        }
      }
    }
  }
  return ret;
}

int64_t ObLSRetentionGCHandler::Leader::find_replica_cache_(
    const common::ObIArray<ObLSGCReplicaCache> &caches, const common::ObAddr &server)
{
  int64_t index = -1;
  for (int64_t i = 0; i < caches.count() && index < 0; ++i) {
    if (caches.at(i).get_server() == server) {
      index = i;
    }
  }
  return index;
}

int ObLSRetentionGCHandler::Leader::register_retained_replica_(
    const ObLSGCRequest &request, const int64_t now)
{
  int ret = OB_SUCCESS;
  const int64_t index = find_replica_cache_(retained_replica_caches_, request.src_);
  const bool is_reliable = config_version_.is_valid()
      && request.get_config_version() == config_version_ && request.get_mode_version() == mode_version_;
  const int64_t request_index = find_gc_request_(request.src_);
  ObLSGCReplicaCache replica;
  if (!request.is_valid() || now <= 0) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid retained ls gc request", K(ret), K(request), K(now));
  } else if (!is_reliable || request_index < 0
      || gc_requests_.at(request_index).token_ != request.get_gc_token()
      || gc_requests_.at(request_index).mode_version_ != request.get_mode_version()
      || GCRequestState::APPROVED == gc_requests_.at(request_index).phase_) {
    ret = OB_STATE_NOT_MATCH;
    CLOG_LOG(WARN, "retained replica gc token or view does not match", K(ret), K(request), K_(gc_requests));
  } else if (OB_FAIL(replica.init(request.src_, request.get_cutoff_scn(), request.get_end_scn(),
      request.get_begin_lsn(), request.get_end_lsn(), now))) {
    CLOG_LOG(WARN, "cache reported retained range failed", K(ret), K(request), K(now));
  } else if (index >= 0) {
    retained_replica_caches_.at(index) = replica;
  } else if (OB_FAIL(retained_replica_caches_.push_back(replica))) {
    CLOG_LOG(WARN, "register retained replica failed", K(ret), K(request), K(replica));
  }
  if (OB_SUCC(ret)) {
    last_request_time_ = now;
  }
  return ret;
}

int ObLSRetentionGCHandler::Leader::erase_replica_cache_(const common::ObAddr &server)
{
  int ret = OB_SUCCESS;
  const int64_t index = find_replica_cache_(retained_replica_caches_, server);
  if (index >= 0 && OB_FAIL(retained_replica_caches_.remove(index))) {
    CLOG_LOG(WARN, "remove replica cache failed", K(ret), K(server), K(index),
        "cache_count", retained_replica_caches_.count());
  }
  return ret;
}

int ObLSRetentionGCHandler::Leader::build_candidate_servers_(
    const palf::PalfStat &leader_stat, common::ObIArray<common::ObAddr> &servers)
{
  int ret = OB_SUCCESS;
  servers.reuse();
  if (member_replica_caches_.empty()) {
    const int64_t paxos_member_count = leader_stat.paxos_member_list_.get_member_number();
    const int64_t member_count = paxos_member_count + leader_stat.learner_list_.get_member_number();
    ObAddr server;
    for (int64_t i = 0; OB_SUCC(ret) && i < member_count; ++i) {
      if (OB_FAIL(i < paxos_member_count
          ? leader_stat.paxos_member_list_.get_server_by_index(i, server)
          : leader_stat.learner_list_.get_server_by_index(i - paxos_member_count, server))) {
        CLOG_LOG(WARN, "get replica member failed", K(ret), K(i), K(paxos_member_count), K(leader_stat));
      } else if (OB_FAIL(member_replica_caches_.push_back(ObLSGCReplicaCache(server)))) {
        CLOG_LOG(WARN, "initialize member replica cache failed", K(ret), K(server));
      }
    }
    if (OB_FAIL(ret)) {
      // An empty array retries initialization; never leave a partially built member set.
      member_replica_caches_.reuse();
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < member_replica_caches_.count(); ++i) {
    const ObLSGCReplicaCache &cache = member_replica_caches_.at(i);
    if (!is_gc_approved_(cache.get_server())
        && OB_FAIL(servers.push_back(cache.get_server()))) {
      CLOG_LOG(WARN, "append refresh member server failed", K(ret), K(cache), K(i));
    }
  }
  return ret;
}

int ObLSRetentionGCHandler::Leader::prepare_refresh_(
    const palf::PalfStat &leader_stat, const int64_t now,
    common::ObIArray<common::ObAddr> &servers)
{
  int ret = OB_SUCCESS;
  servers.reuse();
  if (LEADER != leader_stat.role_ || !leader_stat.config_version_.is_valid()) {
    reset_replica_caches_();
    ret = OB_NOT_MASTER;
    CLOG_LOG(WARN, "cannot prepare retention refresh without valid leader", K(ret), K(leader_stat));
  } else if (OB_FAIL(update_leader_state_(leader_stat))) {
    CLOG_LOG(WARN, "discard stale leader stat before refresh", K(ret), K(leader_stat));
  } else if (OB_FAIL(build_candidate_servers_(leader_stat, servers))) {
    CLOG_LOG(WARN, "build refresh replica list failed", K(ret), K(leader_stat),
        K(now), K_(last_request_time), K(servers));
  }
  return ret;
}

int ObLSRetentionGCHandler::Leader::apply_refresh_(
    const palf::LogConfigVersion &config_version, const int64_t mode_version,
    const common::ObIArray<common::ObAddr> &servers,
    const common::ObIArray<palf::PalfStat> &palf_stats, const int64_t refresh_time)
{
  int ret = OB_SUCCESS;
  if (config_version != config_version_ || mode_version != mode_version_) {
    ret = OB_EAGAIN;
    CLOG_LOG(WARN, "retention refresh state changed", K(ret), K(config_version), K_(config_version),
        K(mode_version), K_(mode_version));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < servers.count(); ++i) {
      const ObAddr &server = servers.at(i);
      const int64_t index = find_replica_cache_(member_replica_caches_, server);
      // Only current members are queried. Retained replicas report local ranges themselves.
      if (index >= 0 && !is_gc_approved_(server)) {
        bool found = false;
        for (int64_t j = 0; OB_SUCC(ret) && !found && j < palf_stats.count(); ++j) {
          const PalfStat &stat = palf_stats.at(j);
          if (stat.self_ == server) {
            found = true;
            if (stat.mode_version_ != mode_version_) {
              // A known mode mismatch must not leave a previously usable range behind.
              member_replica_caches_.at(index) = ObLSGCReplicaCache(server);
            } else if (OB_FAIL(member_replica_caches_.at(index).init(stat.self_,
                stat.begin_scn_, stat.end_scn_, stat.begin_lsn_, stat.end_lsn_, refresh_time))) {
              CLOG_LOG(WARN, "init member replica cache failed", K(ret), K(server), K(stat),
                  K(refresh_time), K(index));
            }
          }
        }
        if (OB_SUCC(ret) && !found) {
          CLOG_LOG(WARN, "member palf stat not found during refresh", K(server),
              K(config_version), K(mode_version), K(refresh_time), K(index));
        }
      }
    }
  }
  return ret;
}

bool ObLSRetentionGCHandler::Leader::is_cache_usable_(
    const ObLSGCReplicaCache &cache, const common::ObAddr &requester, const int64_t now) const
{
  // Only valid reports renew retained samples; failed local reads let them expire.
  return cache.get_server() != requester
      && !is_gc_approved_(cache.get_server())
      && cache.get_last_refresh_time() > 0
      && now >= cache.get_last_refresh_time()
      && now - cache.get_last_refresh_time() < CACHE_TTL;
}

int ObLSRetentionGCHandler::Leader::get_log_fetch_sources(LogGetFetchSourcesResp &response)
{
  int ret = OB_SUCCESS;
  PalfStat leader_stat;
  PalfStat final_stat;
  LogFetchSource source;
  lib::ObMutexGuard guard(lock_);
  const int64_t now = ObTimeUtility::current_monotonic_time();
  response.reset();
  if (OB_FAIL(get_leader_stat_(leader_stat))) {
    CLOG_LOG(WARN, "get leader stat for log sources failed", K(ret));
  } else if (leader_stat.config_version_ != config_version_ || leader_stat.mode_version_ != mode_version_) {
    ret = OB_EAGAIN;
    CLOG_LOG(WARN, "retention fetch source state changed", K(ret),
        K(leader_stat.config_version_), K_(config_version), K(leader_stat.mode_version_), K_(mode_version));
  } else {
    const int64_t member_count = member_replica_caches_.count();
    const int64_t replica_count = member_count + retained_replica_caches_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < replica_count; ++i) {
      const ObLSGCReplicaCache &cache = i < member_count
          ? member_replica_caches_.at(i) : retained_replica_caches_.at(i - member_count);
      if ((i < member_count || is_gc_hold_(cache.get_server()))
          && is_cache_usable_(cache, ObAddr(), now)) {
        source.server_ = cache.get_server();
        source.begin_lsn_ = cache.get_begin_lsn();
        source.end_lsn_ = cache.get_end_lsn();
        if (source.is_valid() && OB_FAIL(response.sources_.push_back(source))) {
          CLOG_LOG(WARN, "append log source failed", K(ret), K(source));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(get_leader_stat_(final_stat))) {
        CLOG_LOG(WARN, "recheck leader for log sources failed", K(ret));
      } else if (leader_stat.config_version_ != final_stat.config_version_
          || leader_stat.mode_version_ != final_stat.mode_version_ || response.sources_.empty()) {
        // An empty cache is not evidence that no retained replica exists.
        ret = OB_EAGAIN;
        CLOG_LOG(WARN, "retention fetch sources are stale or empty", K(ret),
            K(leader_stat.config_version_), K(final_stat.config_version_),
            K(leader_stat.mode_version_), K(final_stat.mode_version_), "source_count", response.sources_.count());
      }
    }
  }
  if (OB_FAIL(ret)) {
    response.reset();
  }
  return ret;
}

int ObLSRetentionGCHandler::Leader::collect_replica_ranges_(
    const common::ObAddr &requester, const int64_t now,
    common::ObIArray<ObLSGCReplicaCache> &ranges) const
{
  int ret = OB_SUCCESS;
  ranges.reuse();
  for (int64_t i = 0; OB_SUCC(ret) && i < member_replica_caches_.count(); ++i) {
    const ObLSGCReplicaCache &cache = member_replica_caches_.at(i);
    if (is_cache_usable_(cache, requester, now) && OB_FAIL(ranges.push_back(cache))) {
      CLOG_LOG(WARN, "push cached member replica range failed", K(ret), K(cache), K(requester), K(now));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < retained_replica_caches_.count(); ++i) {
    const ObLSGCReplicaCache &cache = retained_replica_caches_.at(i);
    if (is_gc_hold_(cache.get_server()) && is_cache_usable_(cache, requester, now)
        && OB_FAIL(ranges.push_back(cache))) {
      CLOG_LOG(WARN, "push cached retained replica range failed", K(ret), K(cache), K(requester), K(now));
    }
  }
  return ret;
}

int ObLSRetentionGCHandler::Leader::refresh_cache()
{
  int ret = OB_SUCCESS;
  PalfStat leader_stat;
  PalfStat final_stat;
  ObArray<ObAddr> servers;
  ObArray<PalfStat> palf_stats;
  {
    lib::ObMutexGuard guard(lock_);
    if (OB_ISNULL(ls_)) {
      ret = OB_NOT_INIT;
      CLOG_LOG(WARN, "retention gc leader is not inited", K(ret), KP(ls_));
    } else if (!is_gc_request_within_active_window_(ObTimeUtility::current_monotonic_time())) {
      // No recent GC request requires a cache refresh.
    } else if (OB_FAIL(get_leader_stat_(leader_stat))) {
      if (OB_NOT_MASTER == ret) {
        reset_replica_caches_();
      }
    } else if (OB_FAIL(prepare_refresh_(leader_stat,
        ObTimeUtility::current_monotonic_time(), servers))) {
      CLOG_LOG(WARN, "prepare retention cache refresh failed", K(ret), K(leader_stat), K(servers));
    }
  }
  // Only the existing GC background thread queries replicas, without the approval lock.
  // Use the query start so slow replies do not extend the sampled stat's lifetime.
  const int64_t refresh_time = ObTimeUtility::current_monotonic_time();
  if (OB_SUCC(ret) && !servers.empty()
      && OB_FAIL(get_replica_palf_stats_(servers, palf_stats))) {
    CLOG_LOG(WARN, "get replica palf stats failed", K(ret), K(servers), K(palf_stats));
  }
  {
    lib::ObMutexGuard guard(lock_);
    if (OB_FAIL(ret)) {
      CLOG_LOG(WARN, "skip retention refresh after query preparation or execution failed", K(ret));
    } else if (servers.empty()) {
      // No replica query was issued.
    } else if (OB_FAIL(get_leader_stat_(final_stat))) {
      CLOG_LOG(WARN, "get final leader stat for retention refresh failed", K(ret));
    } else if (OB_FAIL(update_leader_state_(final_stat))) {
      CLOG_LOG(WARN, "discard stale final leader stat", K(ret), K(final_stat));
    } else if (leader_stat.config_version_ != final_stat.config_version_
        || leader_stat.mode_version_ != final_stat.mode_version_) {
      ret = OB_EAGAIN;
      CLOG_LOG(WARN, "leader state changed during refresh", K(ret), K(leader_stat), K(final_stat));
    } else if (!is_gc_request_within_active_window_(ObTimeUtility::current_monotonic_time())) {
      // Activity expired while the queries were in flight.
    } else if (OB_FAIL(apply_refresh_(leader_stat.config_version_,
        leader_stat.mode_version_, servers, palf_stats, refresh_time))) {
      CLOG_LOG(WARN, "discard stale retention refresh", K(ret), K(leader_stat),
          K(servers), K(palf_stats), K(refresh_time));
    } else {
      CLOG_LOG(INFO, "retention gc leader cache refreshed", K(MTL_ID()), K(ls_->get_ls_id()),
          K_(config_version), K_(member_replica_caches), K_(retained_replica_caches),
          K_(gc_requests), K_(last_request_time), K(refresh_time));
    }
    if (OB_NOT_MASTER == ret) {
      reset_replica_caches_();
    }
  }
  return ret;
}

int ObLSRetentionGCHandler::get_retention_config_(
    int64_t &required_log_copies, int64_t &retention_us)
{
  int ret = OB_SUCCESS;
  required_log_copies = 0;
  retention_us = 0;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  if (!tenant_config.is_valid()) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "retention config is unavailable", K(ret), K(MTL_ID()));
  } else {
    required_log_copies = tenant_config->log_retention_replica_count;
    retention_us = tenant_config->log_retention_time;
  }
  return ret;
}

int64_t ObLSRetentionGCHandler::Leader::find_gc_request_(const ObAddr &server) const
{
  int64_t index = -1;
  for (int64_t i = 0; i < gc_requests_.count() && index < 0; ++i) {
    if (gc_requests_.at(i).server_ == server) {
      index = i;
    }
  }
  return index;
}

bool ObLSRetentionGCHandler::Leader::is_gc_approved_(const ObAddr &server) const
{
  const int64_t index = find_gc_request_(server);
  return index >= 0 && GCRequestState::APPROVED == gc_requests_.at(index).phase_;
}

bool ObLSRetentionGCHandler::Leader::is_gc_hold_(const ObAddr &server) const
{
  const int64_t index = find_gc_request_(server);
  return index >= 0 && GCRequestState::HOLD == gc_requests_.at(index).phase_;
}

int ObLSRetentionGCHandler::Leader::alloc_gc_token_(int64_t &token)
{
  int ret = OB_SUCCESS;
  token = INVALID_GC_TOKEN;
  if (INT64_MAX == next_gc_token_) {
    ret = OB_SIZE_OVERFLOW;
    CLOG_LOG(WARN, "retention gc token exhausted", K(ret), K_(config_version), K_(next_gc_token));
  } else {
    token = ++next_gc_token_;
  }
  return ret;
}

int ObLSRetentionGCHandler::Leader::check_leader_state_(const PalfStat &leader_stat)
{
  int ret = OB_SUCCESS;
  PalfStat final_stat;
  if (OB_FAIL(get_leader_stat_(final_stat))) {
    if (OB_NOT_MASTER == ret) {
      reset_replica_caches_();
    }
  } else if (THIS_WORKER.is_timeout()) {
    ret = OB_TIMEOUT;
    CLOG_LOG(WARN, "retention gc request expired while checking leader", K(ret));
  } else if (leader_stat.config_version_ != final_stat.config_version_
      || leader_stat.mode_version_ != final_stat.mode_version_) {
    ret = OB_EAGAIN;
    CLOG_LOG(WARN, "retention gc leader changed before state transition", K(ret), K(leader_stat), K(final_stat));
  }
  return ret;
}

int ObLSRetentionGCHandler::Leader::prepare_gc_request_(
    const ObLSGCRequest &request, ObLSGCResponse &response)
{
  int ret = OB_SUCCESS;
  int64_t token = INVALID_GC_TOKEN;
  const int64_t index = find_gc_request_(request.src_);
  if (OB_FAIL(alloc_gc_token_(token))) {
    CLOG_LOG(WARN, "allocate retention gc handshake token failed", K(ret), K(request));
  } else {
    const GCRequestState state(request.src_, token, request.get_mode_version());
    if (index >= 0) {
      gc_requests_.at(index) = state;
    } else if (OB_FAIL(gc_requests_.push_back(state))) {
      CLOG_LOG(WARN, "record retention gc handshake failed", K(ret), K(request), K(state));
    }
    if (OB_SUCC(ret)) {
      // A handshake withdraws the previous HOLD, but never declares a new range.
      if (OB_FAIL(erase_replica_cache_(request.src_))) {
        CLOG_LOG(WARN, "withdraw previous retention gc range failed", K(ret), K(request));
      } else if (OB_FAIL(response.init(false/*allow_gc*/, config_version_, token))) {
        CLOG_LOG(WARN, "init retention gc handshake response failed", K(ret), K(request), K(token));
      } else {
        last_request_time_ = ObTimeUtility::current_monotonic_time();
      }
    }
  }
  return ret;
}

int ObLSRetentionGCHandler::Leader::check_gc_coverage_(
    const ObLSGCRequest &request, const int64_t required_log_copies,
    const int64_t leader_mode_version, bool &allow_gc) const
{
  int ret = OB_SUCCESS;
  const int64_t now = ObTimeUtility::current_monotonic_time();
  ObArray<ObLSGCReplicaCache> ranges;
  allow_gc = false;
  if (leader_mode_version != mode_version_) {
    ret = OB_EAGAIN;
    CLOG_LOG(WARN, "retention coverage mode changed", K(ret), K(leader_mode_version), K_(mode_version));
  } else if (request.get_mode_version() < leader_mode_version
      || is_gc_approved_(request.src_)) {
    // Membership and both mode versions were validated before checking permission.
    allow_gc = true;
  } else if (OB_FAIL(collect_replica_ranges_(request.src_, now, ranges))
      || OB_FAIL(check_log_coverage(ranges, request.get_cutoff_scn(),
          request.get_end_scn(), required_log_copies, allow_gc))) {
    CLOG_LOG(WARN, "check retention gc coverage failed", K(ret), K(request),
        K(required_log_copies), K(ranges), K(now), K(allow_gc));
  }
  return ret;
}

int ObLSRetentionGCHandler::Leader::check_gc_permission_(
    const ObLSGCRequest &request, const PalfStat &leader_stat,
    const int64_t required_log_copies, bool &allow_gc)
{
  int ret = OB_SUCCESS;
  allow_gc = false;
  if (OB_FAIL(check_leader_state_(leader_stat))) {
    CLOG_LOG(WARN, "leader changed before retention gc decision", K(ret), K(request));
  } else if (OB_FAIL(check_gc_coverage_(request, required_log_copies,
      leader_stat.mode_version_, allow_gc))) {
    // Check coverage after PALF stat, which may wait for its lock.
    CLOG_LOG(WARN, "check retained ls gc coverage failed", K(ret), K(request), K(required_log_copies));
  }
  return ret;
}

int ObLSRetentionGCHandler::Leader::approve_gc_request_(
    const ObLSGCRequest &request, const int64_t index, ObLSGCResponse &response)
{
  int ret = OB_SUCCESS;
  // Publish exclusion before allowing deletion, including idempotent replies.
  gc_requests_.at(index).phase_ = GCRequestState::APPROVED;
  if (OB_FAIL(erase_replica_cache_(request.src_))) {
    CLOG_LOG(WARN, "remove approved retention gc range failed", K(ret), K(request));
  } else if (OB_FAIL(response.init(true/*allow_gc*/, config_version_, request.get_gc_token()))) {
    CLOG_LOG(WARN, "init retention gc allow response failed", K(ret), K(request));
  }
  return ret;
}

int ObLSRetentionGCHandler::Leader::hold_gc_request_(
    const ObLSGCRequest &request, const int64_t index, ObLSGCResponse &response)
{
  int ret = OB_SUCCESS;
  int64_t next_token = INVALID_GC_TOKEN;
  if (OB_FAIL(alloc_gc_token_(next_token))) {
    CLOG_LOG(WARN, "allocate next retention gc token failed", K(ret), K(request));
  } else if (OB_FAIL(register_retained_replica_(request, ObTimeUtility::current_monotonic_time()))) {
    CLOG_LOG(WARN, "record retention gc hold failed", K(ret), K(request));
  } else {
    // Consuming the token prevents a delayed duplicate from replacing this range.
    gc_requests_.at(index).phase_ = GCRequestState::HOLD;
    gc_requests_.at(index).token_ = next_token;
    if (OB_FAIL(response.init(false/*allow_gc*/, config_version_, next_token))) {
      CLOG_LOG(WARN, "init retention gc hold response failed", K(ret), K(request), K(next_token));
    }
  }
  return ret;
}

int ObLSRetentionGCHandler::Leader::process_gc_request_(
    const ObLSGCRequest &request, const PalfStat &leader_stat,
    const int64_t required_log_copies, ObLSGCResponse &response)
{
  int ret = OB_SUCCESS;
  bool allow_gc = false;
  const int64_t index = find_gc_request_(request.src_);
  if (INVALID_GC_TOKEN == request.get_gc_token()) {
    if (OB_FAIL(check_leader_state_(leader_stat))) {
      CLOG_LOG(WARN, "leader changed before retention gc handshake", K(ret), K(request));
    } else if (OB_FAIL(prepare_gc_request_(request, response))) {
      CLOG_LOG(WARN, "prepare retention gc handshake failed", K(ret), K(request));
    }
  } else if (request.get_config_version() != config_version_ || index < 0
      || gc_requests_.at(index).token_ != request.get_gc_token()
      || gc_requests_.at(index).mode_version_ != request.get_mode_version()) {
    // Return an invalid token without changing the current HOLD or approval.
  } else if (OB_FAIL(check_gc_permission_(request, leader_stat, required_log_copies, allow_gc))) {
    CLOG_LOG(WARN, "check retention gc permission failed", K(ret), K(request));
  } else if (allow_gc) {
    if (OB_FAIL(approve_gc_request_(request, index, response))) {
      CLOG_LOG(WARN, "approve retention gc request failed", K(ret), K(request));
    }
  } else if (OB_FAIL(hold_gc_request_(request, index, response))) {
    CLOG_LOG(WARN, "hold retention gc request failed", K(ret), K(request));
  }
  return ret;
}

int ObLSRetentionGCHandler::Leader::handle_request(
    const ObLSGCRequest &request, ObLSGCResponse &response)
{
  int ret = OB_SUCCESS;
  PalfStat leader_stat;
  int64_t required_log_copies = 0;
  int64_t retention_us = 0;
  const int config_ret = get_retention_config_(required_log_copies, retention_us);
  lib::ObMutexGuard guard(lock_);
  response.reset();
  if (!request.is_valid() || OB_ISNULL(ls_) || request.get_ls_id() != ls_->get_ls_id()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid retained ls gc request", K(ret), K(request), KPC(ls_));
  } else if (THIS_WORKER.is_timeout()) {
    ret = OB_TIMEOUT;
    CLOG_LOG(WARN, "retention gc request expired before handling", K(ret), K(request));
  } else if (OB_FAIL(config_ret)) {
    CLOG_LOG(WARN, "tenant config is invalid", K(ret), K(MTL_ID()), K(request));
  } else if (OB_FAIL(get_leader_stat_(leader_stat))) {
    if (OB_NOT_MASTER == ret) {
      reset_replica_caches_();
    }
  } else if (OB_FAIL(response.init(false/*allow_gc*/, leader_stat.config_version_, INVALID_GC_TOKEN))) {
    CLOG_LOG(WARN, "init retention gc deny response failed", K(ret), K(request), K(leader_stat));
  } else if (OB_FAIL(update_leader_state_(leader_stat))) {
    CLOG_LOG(WARN, "update retention gc leader state failed", K(ret), K(request), K(leader_stat));
  } else if (INVALID_PROPOSAL_ID == request.get_mode_version() || request.get_mode_version() < 0
      || INVALID_PROPOSAL_ID == leader_stat.mode_version_ || leader_stat.mode_version_ < 0
      || request.get_mode_version() > leader_stat.mode_version_) {
    ret = OB_EAGAIN;
    CLOG_LOG(WARN, "retained gc mode is unavailable or requester is newer", K(ret), K(request), K(leader_stat));
  } else if (leader_stat.paxos_member_list_.contains(request.src_)
      || leader_stat.learner_list_.contains(request.src_)) {
    ret = OB_STATE_NOT_MATCH;
    CLOG_LOG(WARN, "retained gc requester is still in current configuration", K(ret), K(request), K(leader_stat));
  } else if (OB_FAIL(process_gc_request_(request, leader_stat, required_log_copies, response))) {
    CLOG_LOG(WARN, "process retention gc request failed", K(ret), K(request), K(leader_stat));
  }
  CLOG_LOG(INFO, "retained ls gc decision", K(ret), K(MTL_ID()), K(request), K(response),
      K(leader_stat.config_version_), K(required_log_copies));
  return ret;
}

ObLSRetentionGCHandler::GCReplica::GCReplica()
  : ls_(NULL),
    gc_started_(false),
    sampled_end_scn_(),
    config_version_(),
    gc_token_(INVALID_GC_TOKEN)
{
}

ObLSRetentionGCHandler::GCReplica::~GCReplica()
{
  reset();
}

int ObLSRetentionGCHandler::GCReplica::init(storage::ObLS *ls)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(ls_)) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(WARN, "retention gc replica has already been inited", K(ret), KP(ls_), KP(ls));
  } else if (OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), KP(ls));
  } else {
    ls_ = ls;
  }
  return ret;
}

void ObLSRetentionGCHandler::GCReplica::reset()
{
  ls_ = NULL;
  ATOMIC_STORE(&gc_started_, false);
  sampled_end_scn_.reset();
  config_version_.reset();
  gc_token_ = INVALID_GC_TOKEN;
}

int ObLSRetentionGCHandler::GCReplica::check_log_retention_(
    const SCN &end_scn, bool &allow_gc, SCN &cutoff_scn)
{
  // Retention coverage uses [cutoff_scn, end_scn); the end boundary is excluded.
  // Clamp the time cutoff to the local retained prefix without locating a log position.
  int ret = OB_SUCCESS;
  PalfStat palf_stat;
  ObLogService *log_service = MTL(ObLogService*);
  PalfHandleGuard palf_handle_guard;
  int64_t required_log_copies = 0;
  int64_t retention_us = 0;
  const int64_t now_us = ObClockGenerator::getClock();
  allow_gc = false;
  cutoff_scn.reset();
  if (!end_scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid retained ls gc boundary", K(ret), K(end_scn));
  } else if (OB_FAIL(get_retention_config_(required_log_copies, retention_us))) {
    CLOG_LOG(WARN, "get retention config failed", K(ret), K(MTL_ID()));
  } else if (OB_FAIL(cutoff_scn.convert_for_tx(
      (retention_us < now_us ? now_us - retention_us : 1) * 1000L))) {
    CLOG_LOG(WARN, "convert retention cutoff scn failed", K(ret), K(now_us), K(retention_us));
  } else if (end_scn <= cutoff_scn) {
    // The half-open range is empty; no PALF stat lookup or coverage RPC is needed.
    cutoff_scn = end_scn;
    allow_gc = true;
  } else if (OB_ISNULL(ls_) || OB_ISNULL(log_service)) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "retention gc dependency is not ready", K(ret), KP(ls_), KP(log_service));
  } else if (OB_FAIL(log_service->open_palf(ls_->get_ls_id(), palf_handle_guard))) {
    CLOG_LOG(WARN, "open palf failed", K(ret), K(ls_->get_ls_id()));
  } else if (OB_FAIL(palf_handle_guard.stat(palf_stat))) {
    CLOG_LOG(WARN, "get local palf stat failed", K(ret), KPC(ls_));
  } else if (!palf_stat.is_valid() || !palf_stat.begin_scn_.is_valid()
      || !palf_stat.end_scn_.is_valid() || palf_stat.begin_scn_ > palf_stat.end_scn_
      || palf_stat.end_scn_ < end_scn) {
    // Invalid stats or a tail below the sampled end SCN must not be treated as expiry.
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "invalid local palf stat for retained ls gc",
        K(ret), K(ls_->get_ls_id()), K(end_scn), K(palf_stat));
  } else {
    // Prefix recycling may have passed the deletion boundary, leaving an empty range.
    cutoff_scn = std::min(end_scn, std::max(palf_stat.begin_scn_, cutoff_scn));
    allow_gc = cutoff_scn == end_scn;
  }
  return ret;
}

int ObLSRetentionGCHandler::GCReplica::check_permission(bool &allow_gc)
{
  int ret = OB_SUCCESS;
  allow_gc = false;
  if (OB_ISNULL(ls_)) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "retention gc replica is not inited", K(ret), KP(ls_));
  } else if (0 != EVENT_CALL(EN_RETAINED_LS_GC_BEFORE_PERMISSION, ls_->get_ls_id().id())) {
    CLOG_LOG(INFO, "retained ls gc paused before permission", K(MTL_ID()),
        "id", ls_->get_ls_id(), K(GCTX.self_addr()));
  } else if (OB_FAIL(check_gc_bypass(allow_gc))) {
    CLOG_LOG(WARN, "check retention gc bypass failed", K(ret), KPC(ls_), K(allow_gc));
  } else if (allow_gc) {
    // Bypass permission is already recorded for removal retries.
  } else if (OB_FAIL(sample_retention_end_scn_())) {
    CLOG_LOG(WARN, "sample retention end scn failed", K(ret), KPC(ls_));
  } else if (OB_FAIL(check_permission_with_end_scn_(sampled_end_scn_, allow_gc))) {
    CLOG_LOG(WARN, "check retained ls gc permission failed", K(ret), K_(sampled_end_scn), KPC(ls_), K(allow_gc));
  }
  if (OB_SUCC(ret) && allow_gc
      && 0 != EVENT_CALL(EN_RETAINED_LS_GC_AFTER_PERMISSION, ls_->get_ls_id().id())) {
    // Pause removal without revoking the sticky permit.
    allow_gc = false;
    CLOG_LOG(INFO, "retained ls gc paused after permission", K(MTL_ID()),
        "id", ls_->get_ls_id(), K(GCTX.self_addr()));
  }
  return ret;
}

int ObLSRetentionGCHandler::GCReplica::sample_retention_end_scn_()
{
  int ret = OB_SUCCESS;
  ObLogService *log_service = MTL(ObLogService*);
  PalfHandleGuard palf_guard;
  SCN end_scn;
  if (sampled_end_scn_.is_valid()) {
    // Reuse the sampled retention boundary even if synchronization advances the tail.
  } else if (OB_ISNULL(log_service)) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "log service is null before gc", K(ret), KPC(ls_), KP(log_service));
  } else if (OB_FAIL(log_service->open_palf(ls_->get_ls_id(), palf_guard))) {
    CLOG_LOG(WARN, "open palf before gc failed", K(ret), KPC(ls_));
  } else if (OB_FAIL(palf_guard.get_palf_handle()->get_end_scn(end_scn))) {
    CLOG_LOG(WARN, "get log end scn before gc failed", K(ret), KPC(ls_));
  } else {
    sampled_end_scn_ = end_scn;
    SERVER_EVENT_ADD("gc", "sample_retention_end_scn",
        "tenant_id", MTL_ID(),
        "ls_id", ls_->get_ls_id().id(),
        "end_scn", sampled_end_scn_,
        "server", GCTX.self_addr());
  }
  return ret;
}

int ObLSRetentionGCHandler::GCReplica::check_gc_bypass(bool &allow_gc)
{
  int ret = OB_SUCCESS;
  allow_gc = false;
  int64_t required_log_copies = 0;
  int64_t retention_us = 0;
  ILogBlockReclaimCoordinator *reclaim_coordinator = NULL;
  allow_gc = ATOMIC_LOAD(&gc_started_);
  if (allow_gc) {
    // The single GC thread retries removal without declaring HOLD again.
  } else if (OB_ISNULL(ls_)) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "retention gc replica is not inited", K(ret), KP(ls_));
  } else if (is_meta_tenant(MTL_ID())) {
    // Meta tenants do not retain replicas for log retention coverage.
    allow_gc = true;
  } else if (is_sys_tenant(MTL_ID())
      && 0 != EVENT_CALL(EN_BYPASS_SYS_TENANT_RETAINED_LS_GC)) {
    CLOG_LOG(WARN, "bypass system tenant retained ls gc coverage by injection",
        K(MTL_ID()), K(ls_->get_ls_id()));
    allow_gc = true;
  } else if (ls_->is_need_gc()) {
    // Failed or uncommitted replicas keep the original cleanup path.
    allow_gc = true;
  } else if (OB_NOT_NULL(GCTX.log_block_mgr_)
      && OB_NOT_NULL(reclaim_coordinator = GCTX.log_block_mgr_->get_reclaim_coordinator())
      && !reclaim_coordinator->is_shared_log_enabled()) {
    // Resume ordinary replica removal only after shared reclaim finishes draining.
    allow_gc = true;
  } else if (OB_FAIL(get_retention_config_(required_log_copies, retention_us))) {
    CLOG_LOG(WARN, "retained ls gc dependency is not ready", K(ret), K(MTL_ID()),
        KP(ls_));
  } else if (0 == required_log_copies) {
    allow_gc = true;
  }
  if (OB_SUCC(ret) && allow_gc) {
    // Keep bypass permission across failed removal retries without reopening HOLD.
    ATOMIC_STORE(&gc_started_, true);
  }
  return ret;
}

int ObLSRetentionGCHandler::GCReplica::check_permission_with_end_scn_(
    const SCN &end_scn, bool &allow_gc)
{
  int ret = OB_SUCCESS;
  ObLSGCRequest request;
  ObLSGCResponse response;
  SCN cutoff_scn;
  PalfStat stat;
  allow_gc = false;
  if (!end_scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "collector did not provide a gc boundary", K(ret), K(end_scn));
  } else if (OB_FAIL(check_log_retention_(end_scn, allow_gc, cutoff_scn))) {
    CLOG_LOG(WARN, "check log retention failed", K(ret), K(ls_->get_ls_id()),
        K(end_scn), K(cutoff_scn), K(allow_gc));
  } else if (allow_gc) {
    CLOG_LOG(INFO, "retained logs have expired", K(MTL_ID()), K(ls_->get_ls_id()),
        K(cutoff_scn), K(end_scn));
  } else if (OB_FAIL(get_local_log_range_(stat))) {
    CLOG_LOG(WARN, "get local range for retained gc failed", K(ret), KPC(ls_));
  } else if (INVALID_PROPOSAL_ID == stat.mode_version_ || stat.mode_version_ < 0
      || !stat.begin_scn_.is_valid() || !stat.end_scn_.is_valid()
      || stat.begin_scn_ > cutoff_scn || end_scn > stat.end_scn_
      || !stat.begin_lsn_.is_valid() || !stat.end_lsn_.is_valid() || stat.begin_lsn_ >= stat.end_lsn_) {
    ret = OB_EAGAIN;
    CLOG_LOG(WARN, "local mode or log range is not ready for retained gc", K(ret), K(stat),
        K(cutoff_scn), K(end_scn), KPC(ls_));
  } else if (OB_FAIL(request.init(GCTX.self_addr(), ls_->get_ls_id(), cutoff_scn, end_scn,
      config_version_, stat.mode_version_, stat.begin_lsn_, stat.end_lsn_, gc_token_))) {
    CLOG_LOG(WARN, "init retained ls gc request failed", K(ret), K(GCTX.self_addr()),
        K(ls_->get_ls_id()), K(cutoff_scn), K(end_scn), K_(config_version));
  } else if (OB_FAIL(request_permission_(request, response))) {
    CLOG_LOG(WARN, "request retention gc permission failed", K(ret), K(request), K(response));
  } else if (!response.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "invalid retained ls gc response", K(ret), K(request), K(response));
  } else {
    config_version_ = response.get_config_version();
    gc_token_ = response.get_gc_token();
    allow_gc = response.is_gc_allowed();
    if (!allow_gc) {
      CLOG_LOG(INFO, "retained ls gc permission not granted", K(MTL_ID()),
          K(request), K(response));
    }
  }
  if (OB_SUCC(ret) && allow_gc) {
    // Publish before returning permission to remove_ls.
    // A failed removal keeps this permit; membership refresh must not reopen HOLD.
    ATOMIC_STORE(&gc_started_, true);
  }
  return ret;
}

int ObLSRetentionGCHandler::GCReplica::get_local_log_range_(PalfStat &stat)
{
  int ret = OB_SUCCESS;
  ObLogService *log_service = MTL(ObLogService*);
  PalfHandleGuard palf_guard;
  stat.reset();
  // Assume mode meta stops changing once the replica leaves both member and learner lists.
  // TODO: Handle the known rare case of receiving a higher mode version after removal;
  // mode metadata alone may then misrepresent the retained log history.
  if (OB_ISNULL(ls_) || OB_ISNULL(log_service)) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(log_service->open_palf(ls_->get_ls_id(), palf_guard))) {
    CLOG_LOG(WARN, "open palf for retained gc range failed", K(ret), KPC(ls_));
  } else if (OB_FAIL(palf_guard.stat(stat))) {
    CLOG_LOG(WARN, "get retained replica log range failed", K(ret), KPC(ls_));
  }
  return ret;
}

int ObLSRetentionGCHandler::GCReplica::request_permission_(
    const ObLSGCRequest &request, ObLSGCResponse &response)
{
  int ret = OB_SUCCESS;
  ObLogService *log_service = MTL(ObLogService*);
  ObAddr leader;
  const int64_t timeout_us = GCONF.rpc_timeout;
  if (OB_ISNULL(log_service) || OB_ISNULL(log_service->get_rpc_proxy())
      || OB_ISNULL(GCTX.location_service_)) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "retention gc dependency is not ready", K(ret), K(ls_->get_ls_id()),
        KP(log_service), KP(GCTX.location_service_),
        "has_rpc_proxy", NULL != log_service && NULL != log_service->get_rpc_proxy());
  } else if (OB_FAIL(GCTX.location_service_->nonblock_get_leader(
      GCONF.cluster_id, MTL_ID(), ls_->get_ls_id(), leader))) {
    CLOG_LOG(WARN, "get ls leader failed", K(ret), K(static_cast<int64_t>(GCONF.cluster_id)), K(MTL_ID()), K(ls_->get_ls_id()));
  } else if (!leader.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "ls leader is invalid", K(ret), K(ls_->get_ls_id()), K(leader));
  } else if (OB_FAIL(log_service->get_rpc_proxy()->to(leader)
      .by(MTL_ID()).timeout(timeout_us)
      .group_id(share::OBCG_STORAGE).request_ls_gc(request, response))) {
    CLOG_LOG(WARN, "request retained ls gc failed", K(ret), K(leader), K(request),
        K(response), K(MTL_ID()), K(timeout_us));
    const int tmp_ret = GCTX.location_service_->nonblock_renew(
        GCONF.cluster_id, MTL_ID(), ls_->get_ls_id());
    if (OB_SUCCESS != tmp_ret) {
      CLOG_LOG(WARN, "renew ls leader failed", K(tmp_ret), K(static_cast<int64_t>(GCONF.cluster_id)), K(MTL_ID()), K(ls_->get_ls_id()));
    }
  }
  return ret;
}

} // namespace logservice
} // namespace oceanbase
