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

#ifndef OCEANBASE_LOGSERVICE_OB_LS_RETENTION_GC_HANDLER_
#define OCEANBASE_LOGSERVICE_OB_LS_RETENTION_GC_HANDLER_

#include "lib/container/ob_array.h"
#include "lib/lock/ob_mutex.h"
#include "lib/net/ob_addr.h"
#include "logservice/palf/palf_handle_impl.h"
#include "logservice/logrpc/ob_log_rpc_req.h"
#include "share/scn.h"

namespace oceanbase
{
namespace palf
{
class PalfHandle;
}
namespace storage
{
class ObLS;
}
namespace logservice
{
class ObLSGCRequest;
class ObLSGCResponse;
struct LogGetFetchSourcesResp;

class ObLSGCReplicaCache
{
public:
  ObLSGCReplicaCache();
  explicit ObLSGCReplicaCache(const common::ObAddr &server);
  int init(const common::ObAddr &server, const share::SCN &begin_scn, const share::SCN &end_scn,
           const palf::LSN &begin_lsn, const palf::LSN &end_lsn, const int64_t last_refresh_time);
  bool is_valid() const
  {
    return begin_scn_.is_valid() && end_scn_.is_valid() && begin_scn_ < end_scn_
        && begin_lsn_.is_valid() && end_lsn_.is_valid() && begin_lsn_ < end_lsn_;
  }
  const common::ObAddr &get_server() const { return server_; }
  const share::SCN &get_begin_scn() const { return begin_scn_; }
  const share::SCN &get_end_scn() const { return end_scn_; }
  const palf::LSN &get_begin_lsn() const { return begin_lsn_; }
  const palf::LSN &get_end_lsn() const { return end_lsn_; }
  int64_t get_last_refresh_time() const { return last_refresh_time_; }
  TO_STRING_KV(K_(server), K_(begin_scn), K_(end_scn), K_(begin_lsn), K_(end_lsn),
      K_(last_refresh_time));

private:
  common::ObAddr server_;
  share::SCN begin_scn_;
  share::SCN end_scn_;
  palf::LSN begin_lsn_;
  palf::LSN end_lsn_;
  // Monotonic query-start or local-report arrival time; zero means no sample.
  int64_t last_refresh_time_;
};

// Retention GC protocol (Leader <-> GCReplica):
// The requester is a retiring replica outside the current member/learner lists,
// not an ordinary Paxos follower. One GC thread issues synchronous RPCs and echoes
// the config/token from the last successful, valid response. RPC errors never grant GC.
//
// 1. Handshake: INVALID_GC_TOKEN asks the Leader to allocate a fresh token bound to
//    the source address and request mode. The Leader enters PREPARE, withdraws any
//    previous HOLD for that address, and returns deny + current config + new token.
//    PREPARE reports no retained coverage and never permits deletion.
// 2. Formal request: config, token and bound request mode must match. A mismatch
//    returns deny + current config + INVALID_GC_TOKEN without modifying the current
//    HOLD/approval, so the requester starts a new handshake on its next round.
// 3. Decision: under the Leader mutex, recheck Leader/config/mode and the RPC deadline,
//    then evaluate fresh coverage excluding the requester. A request from an older
//    mode, or an already APPROVED request, may take the existing permission fast path.
//    Otherwise, the required copies must cover the entire requested SCN interval.
//    Denial registers the range as HOLD and consumes the token by returning a new one.
//    Approval publishes APPROVED and removes the retained range before returning
//    allow + the same token. PREPARE/APPROVED never count as retained coverage or sources.
// 4. The requester makes a received permission sticky before deletion. Failed deletion
//    retries do not report HOLD again. Leader cache resets do not revoke this local
//    permission; LS teardown/requester restart clears the in-memory requester state.
//
// Restart and Leader changes:
// - Requester restart (including reuse of the same address) loses config/token and
//   the sticky permit, resamples its range, and starts with a handshake. The new token
//   invalidates pre-restart formal requests; the handshake also withdraws the old HOLD
//   or approval. No permit is inherited merely because the address is unchanged.
// - Leader config or access-mode changes clear request records and coverage caches.
//   Cache-only reuse preserves request records in the same view. Token allocation
//   never rewinds during a Leader object's lifetime, including reset/reuse/mode changes.
// - After Leader restart or a change of Leader (including A -> B -> A), PALF must
//   advance the config through STARTWORKING before reporting an active Leader. This
//   isolates old permits even when a newly constructed allocator reuses token numbers.
//   Old-view formal requests are rejected and must handshake again; an already received
//   sticky deletion permit on a live requester remains usable.
//
// Delayed, duplicated or lost packets:
// - A consumed/stale formal token cannot overwrite a newer HOLD or restore coverage
//   after approval. A duplicate of the current APPROVED token can return allow again
//   in the same view, allowing recovery from a lost approval response.
// - If a denial response is lost, retrying the consumed token obtains INVALID_GC_TOKEN
//   and restarts the handshake. A delayed handshake can replace the current token and
//   withdraw its HOLD/approval, but never grants deletion or introduces coverage.
// - Timed-out requests are checked after acquiring the Leader mutex and again after
//   PALF stat, before transitions. Non-expired delayed requests still obey token/view
//   checks. The requester accepts responses only through its synchronous RPC call;
//   transport failures leave its last adopted config/token unchanged.
// - Replica-stat refresh runs outside the Leader mutex. Applying results revalidates
//   the Leader view and current HOLD state; refresh cannot restore an APPROVED range.
//
// Scope: token/config checks isolate request rounds, not the physical log history.
// The current stat-based range check assumes a retired replica does not acquire newer
// mode metadata for unchanged old history; late-mode/FLASHBACK history validation is
// not provided by this protocol (see get_local_log_range_()).
class ObLSRetentionGCHandler
{
public:
  ObLSRetentionGCHandler();
  ~ObLSRetentionGCHandler();
  int init(storage::ObLS *ls);
  // LS teardown only: release both roles and clear the sticky deletion permit.
  void reset();
  // Clear caches; keep same-view approvals and GC permission. In-flight stats from
  // the same leader view may still refresh registered replicas.
  void reset_leader_state();
  // Single GC background thread; query replicas outside the leader lock. A changed
  // leader view returns OB_EAGAIN for a later round.
  int refresh_cache();
  // Single GC thread checks bypasses, then samples the current end SCN once when
  // retention checking is needed. Log synchronization may continue after sampling.
  // Success may deny GC; an allow result publishes the same sticky permit as bypass.
  // Errors must not be treated as permission. Leader-state resets never revoke it.
  int check_permission(bool &allow_gc);
  // RPC entry: establish a token, then accept formal requests in that token's view.
  // Coverage and token transitions are serialized. Success may still deny GC.
  int handle_request(const ObLSGCRequest &request, ObLSGCResponse &response);
  // Return fresh samples from the current leader view without renewing the cache.
  int get_log_fetch_sources(LogGetFetchSourcesResp &response);
private:
  // Call without retention locks; the tenant config guard never escapes this helper.
  static int get_retention_config_(int64_t &required_log_copies, int64_t &retention_us);
  // Leader-side coverage checks and replica cache.
  class Leader
  {
  public:
    Leader();
    virtual ~Leader();
    int init(storage::ObLS *ls);
    void reset();
    void reuse();
    int refresh_cache();
    int handle_request(const ObLSGCRequest &request, ObLSGCResponse &response);
    int get_log_fetch_sources(LogGetFetchSourcesResp &response);
    // Check at least required_log_copies over all of [cutoff_scn, end_scn).
    // Input ranges exclude the requester; success with is_covered=false denies coverage.
    static int check_log_coverage(const common::ObIArray<ObLSGCReplicaCache> &replicas,
                                  const share::SCN &cutoff_scn,
                                  const share::SCN &end_scn,
                                  const int64_t required_log_copies,
                                  bool &is_covered);
  private:
    struct GCRequestState
    {
      enum Phase { PREPARE, HOLD, APPROVED };
      GCRequestState() : server_(), token_(INVALID_GC_TOKEN), mode_version_(palf::INVALID_PROPOSAL_ID), phase_(PREPARE) {}
      GCRequestState(const common::ObAddr &server, const int64_t token, const int64_t mode_version)
        : server_(server), token_(token), mode_version_(mode_version), phase_(PREPARE) {}
      common::ObAddr server_;
      int64_t token_;
      int64_t mode_version_;
      Phase phase_;
      TO_STRING_KV(K_(server), K_(token), K_(mode_version), K_(phase));
    };
    int64_t find_gc_request_(const common::ObAddr &server) const;
    bool is_gc_approved_(const common::ObAddr &server) const;
    bool is_gc_hold_(const common::ObAddr &server) const;
    int alloc_gc_token_(int64_t &token);
    int check_leader_state_(const palf::PalfStat &leader_stat);
    int prepare_gc_request_(const ObLSGCRequest &request, ObLSGCResponse &response);
    int process_gc_request_(const ObLSGCRequest &request, const palf::PalfStat &leader_stat,
                            const int64_t required_log_copies, ObLSGCResponse &response);
    int check_gc_permission_(const ObLSGCRequest &request, const palf::PalfStat &leader_stat,
                             const int64_t required_log_copies, bool &allow_gc);
    int approve_gc_request_(const ObLSGCRequest &request, const int64_t index, ObLSGCResponse &response);
    int hold_gc_request_(const ObLSGCRequest &request, const int64_t index, ObLSGCResponse &response);
    int check_gc_coverage_(const ObLSGCRequest &request, const int64_t required_log_copies,
                           const int64_t leader_mode_version, bool &allow_gc) const;
    virtual int get_leader_stat_(palf::PalfStat &palf_stat) const;
    virtual int get_replica_palf_stats_(const common::ObIArray<common::ObAddr> &servers,
                                       common::ObIArray<palf::PalfStat> &palf_stats) const;
    int build_candidate_servers_(const palf::PalfStat &leader_stat,
                                 common::ObIArray<common::ObAddr> &servers);
    int register_retained_replica_(const ObLSGCRequest &request, const int64_t now);
    int prepare_refresh_(const palf::PalfStat &leader_stat, const int64_t now,
                         common::ObIArray<common::ObAddr> &servers);
    int apply_refresh_(const palf::LogConfigVersion &config_version, const int64_t mode_version,
                       const common::ObIArray<common::ObAddr> &servers,
                       const common::ObIArray<palf::PalfStat> &palf_stats,
                       const int64_t refresh_time);
    int collect_replica_ranges_(const common::ObAddr &requester, const int64_t now,
                                common::ObIArray<ObLSGCReplicaCache> &ranges) const;
    bool is_cache_usable_(const ObLSGCReplicaCache &cache, const common::ObAddr &requester,
                          const int64_t now) const;
    bool is_gc_request_within_active_window_(const int64_t now) const;
    void reset_replica_caches_();
    int update_leader_state_(const palf::PalfStat &stat);
    static int64_t find_replica_cache_(const common::ObIArray<ObLSGCReplicaCache> &caches,
                                       const common::ObAddr &server);
    int erase_replica_cache_(const common::ObAddr &server);
  private:
    // Durations in microseconds, compared against the monotonic clock.
    static const int64_t CACHE_TTL = 60 * 1000 * 1000;
    static const int64_t ACTIVE_WINDOW = 60 * 1000 * 1000;
    storage::ObLS *ls_;
    // Protect all leader cache, approval, view and refresh state below. Term/config
    // changes clear caches and approvals; cache-only reuse preserves the current view.
    lib::ObMutex lock_;
    // Current Paxos members and learners; cleared on configuration, term or mode changes.
    common::ObArray<ObLSGCReplicaCache> member_replica_caches_;
    // Non-members registered in the current leader view and not approved for GC.
    common::ObArray<ObLSGCReplicaCache> retained_replica_caches_;
    // One current token per server until the leader config or mode changes.
    // PREPARE and APPROVED never contribute retained coverage.
    common::ObArray<GCRequestState> gc_requests_;
    // Never rewind within this object's lifetime, including mode changes and reuse.
    // A newly constructed leader must pass STARTWORKING with a new config version.
    int64_t next_gc_token_;
    // STARTWORKING advances this version before PALF reports an active leader.
    palf::LogConfigVersion config_version_;
    // All cached ranges belong to this applied access mode, including in-flight replies.
    int64_t mode_version_;
    // Monotonic microseconds; zero means no active GC request.
    int64_t last_request_time_;
  private:
    DISALLOW_COPY_AND_ASSIGN(Leader);
  };

  // Retiring-replica requests and sticky deletion permission.
  class GCReplica
  {
  public:
    GCReplica();
    virtual ~GCReplica();
    int init(storage::ObLS *ls);
    void reset();
    int check_permission(bool &allow_gc);
    int check_gc_bypass(bool &allow_gc);
  private:
    int sample_retention_end_scn_();
    int check_permission_with_end_scn_(const share::SCN &end_scn, bool &allow_gc);
    // False requires coverage approval; it is not a final GC denial.
    virtual int check_log_retention_(const share::SCN &end_scn,
                                     bool &allow_gc,
                                     share::SCN &cutoff_scn);
    virtual int get_local_log_range_(palf::PalfStat &stat);
    virtual int request_permission_(const ObLSGCRequest &request, ObLSGCResponse &response);
  private:
    storage::ObLS *ls_;
    // Sticky until LS destruction: remove_ls may retry after this permit escapes.
    bool gc_started_;
    // In-memory end SCN sampled on the first retention check after bypasses.
    // Reused until reset; it is not persisted and is sampled again after restart.
    share::SCN sampled_end_scn_;
    // The single GC thread echoes the last successful response's config version.
    palf::LogConfigVersion config_version_;
    int64_t gc_token_;
  private:
    DISALLOW_COPY_AND_ASSIGN(GCReplica);
  };

private:
  Leader leader_;
  GCReplica gc_replica_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObLSRetentionGCHandler);
};

} // namespace logservice
} // namespace oceanbase

#endif // OCEANBASE_LOGSERVICE_OB_LS_RETENTION_GC_HANDLER_
