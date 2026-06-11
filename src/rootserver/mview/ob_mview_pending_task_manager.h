/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#pragma once

#include "lib/allocator/ob_allocator.h"
#include "lib/allocator/ob_fifo_allocator.h"
#include "lib/container/ob_iarray.h"
#include "lib/container/ob_se_array.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/lock/ob_thread_cond.h"
#include "lib/net/ob_addr.h"
#include "lib/queue/ob_link_queue.h"
#include "rootserver/mview/ob_mview_pending_task_inspection_task.h"
#include "rootserver/mview/ob_mview_pending_task_queue.h"
#include "rootserver/mview/ob_mview_pending_task_table_operator.h"
#include "rootserver/mview/ob_mview_pending_task_scheduler.h"

namespace oceanbase
{
namespace obrpc
{
struct ObScheduleMViewRefreshArg;
struct ObScheduleMViewRefreshResult;
struct ObKillMViewRefreshArg;
}
namespace share
{
class SCN;
namespace schema
{
class ObSchemaGetterGuard;
}
}
namespace rootserver
{

typedef common::hash::ObHashMap<uint64_t, ObMViewContext *, common::hash::SpinReadWriteDefendMode> MViewContextMap;
class ObMViewPendingTaskManager
{
public:
  struct TaskResultEntry : public common::ObLink
  {
    uint64_t tenant_id_;
    int64_t  refresh_id_;
    uint64_t mview_id_;
    int      task_ret_;
    char     err_msg_buf_[common::OB_MAX_ERROR_MSG_LEN] = {0};
    void set_err_msg(const char *msg);
    common::ObString err_msg() const { return common::ObString(err_msg_buf_); }
    TO_STRING_KV(K_(tenant_id), K_(refresh_id), K_(mview_id), K_(task_ret), K_(err_msg_buf));
  };

  enum ReloadState : int64_t
  {
    RS_NOT_READY = 0,
    RS_RELOADING,
    RS_READY,
  };



public:
  ObMViewPendingTaskManager();
  ~ObMViewPendingTaskManager();
  DISALLOW_COPY_AND_ASSIGN(ObMViewPendingTaskManager);

  int init();
  int start();
  void stop();
  void wait();
  void destroy();
  void wakeup();
  int reload_tasks();

  int peek_task(ObMViewPendingTask &task);
  int schedule_task(const obrpc::ObScheduleMViewRefreshArg &arg,
                    int64_t &refresh_id,
                    bool is_force_owner = false);
  int schedule_mview_refresh(const obrpc::ObScheduleMViewRefreshArg &arg,
                             obrpc::ObScheduleMViewRefreshResult &result);
  int schedule_mview_refresh_local(const obrpc::ObScheduleMViewRefreshArg &arg,
                                   obrpc::ObScheduleMViewRefreshResult &result);
  int get_refresh_status(uint64_t tenant_id, int64_t refresh_id, ObMViewTaskStatus &status);
  int get_mview_leader_addr(uint64_t tenant_id,
                            uint64_t mview_id,
                            common::ObAddr &leader_addr);
  int mark_task_running(uint64_t tenant_id,
                        int64_t refresh_id,
                        uint64_t mview_id,
                        const common::ObAddr &leader_addr);
  int mark_task_success(uint64_t tenant_id,
                        int64_t refresh_id,
                        uint64_t mview_id);
  int mark_task_retry_wait(uint64_t tenant_id,
                           int64_t refresh_id,
                           uint64_t mview_id,
                           int task_ret = OB_SUCCESS,
                           const common::ObString &err_msg = common::ObString());
  int mark_task_pending(uint64_t tenant_id,
                        int64_t refresh_id,
                        uint64_t mview_id);
  int mark_task_failed(uint64_t tenant_id,
                       int64_t refresh_id,
                       uint64_t mview_id,
                       int task_ret = OB_SUCCESS);
  int finalize_task(uint64_t tenant_id,
                    int64_t refresh_id,
                    uint64_t mview_id,
                    int task_ret,
                    const common::ObString &err_msg = common::ObString());

  // Resync in-memory status from disk when mark_task_running hits OB_EAGAIN. Disk is the
  // source of truth; this path only aligns memory (no disk writes), then fires the matching
  // side effect (register for recovery / retry / recycle).
  int resync_task_from_disk(uint64_t tenant_id,
                            int64_t refresh_id,
                            uint64_t mview_id);
  int recycle_refresh(uint64_t tenant_id,
                      int64_t refresh_id);
  int on_schedule_task_failed(uint64_t tenant_id,
                              int64_t refresh_id,
                              bool need_rollback,
                              const int task_ret,
                              const common::ObString &err_msg);
  // Called by ObMviewPendingTaskScheduler::run1 when reload_tasks succeeds.
  // Flips reload_state_ from RELOADING to READY and broadcasts reload_cond_.
  void on_reload_done();
  int run_task(uint64_t tenant_id,
               int64_t refresh_id,
               uint64_t mview_id,
               uint64_t target_data_sync_scn,
               const share::schema::ObMVRefreshMethod refresh_method,
               const int64_t refresh_parallel,
               const int64_t retry_count,
               const bool is_consistent_refresh,
               const int64_t expire_ts,
               const common::ObAddr &leader_addr);
  int push_task_result(uint64_t tenant_id,
                       int64_t refresh_id,
                       uint64_t mview_id,
                       uint64_t target_data_sync_scn,
                       int task_ret,
                       const char *err_msg = nullptr);
  int register_running_task_for_recovery(uint64_t tenant_id, int64_t refresh_id,
                                          uint64_t mview_id,
                                          uint64_t target_data_sync_scn);
  int process_task_results(int64_t max_cnt);
  bool has_pending_results() const;
  int64_t get_total_running_cnt() const { return queue_.get_total_running_cnt(); }
  int get_min_pending_task_snapshot(share::SCN &scn);
  int check_reload_tasks_reachable(const common::ObIArray<ObMViewPendingTask *> &group,
                                   bool &is_valid) const;
  template <typename Fn>
  int foreach_running_job(Fn &fn)
  {
    int ret = common::OB_SUCCESS;
    common::ObSEArray<ObMViewPendingRunningJobInfo, 32> infos;
    if (OB_UNLIKELY(!is_inited_)) {
      ret = common::OB_NOT_INIT;
      RS_LOG(WARN, "manager not init", KR(ret));
    } else if (OB_FAIL(queue_.collect_running_jobs(infos))) {
      RS_LOG(WARN, "collect running jobs failed", KR(ret));
    } else if (OB_FAIL(load_missing_session_ids_for_running(infos))) {
      RS_LOG(WARN, "load missing session_ids failed", KR(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < infos.count(); ++i) {
        if (OB_FAIL(fn(infos.at(i)))) {
          RS_LOG(WARN, "callback for running job info failed",
                 KR(ret), K(infos.at(i)));
        }
      }
    }
    return ret;
  }
  int get_task_status(uint64_t tenant_id, int64_t refresh_id, uint64_t mview_id, ObMViewTaskStatus &status)
  {
    int ret = OB_SUCCESS;
    int64_t status_val = 0;
    if (OB_FAIL(queue_.get_task_status(tenant_id, refresh_id, mview_id, status_val))) {
    } else {
      status = static_cast<ObMViewTaskStatus>(status_val);
    }
    return ret;
  }

  /*
   * Max number of distinct (target_data_sync_scn, refresh_id) groups pulled per
   * load_tasks_batch SQL. The SQL builds an inner subquery with DISTINCT + LIMIT
   * over the group key, then INNER JOINs tasks and LEFT JOINs dependencies; the
   * final row count is roughly batch_size * tasks_per_refresh * deps_per_task.
   * We deliberately keep this small (500) so a single SQL, executed under a
   * 10s query_timeout, cannot materialize a runaway result set and blow memory
   * even when refresh groups have many tasks / many dependencies. Trade-off:
   * more reload iterations / more RPCs vs. bounded memory peak per batch.
   */
  static const int64_t RELOAD_PENDING_BATCH_SIZE = 500;
  static const int64_t MAX_PROCESS_RPC_RESULT_CNT = 10;
  /*
   * Upper bound on per-task retry attempts. A task whose retry_count has
   * reached MAX_RETRY_COUNT is considered to have exhausted its retry
   * budget and will be transitioned to MV_TASK_FAILED instead of going
   * through another RETRY_WAIT -> PENDING cycle.
   */
  static const int64_t MAX_RETRY_COUNT = 3;
  // Upper bound on how many refreshes a single mview (as the ROOT mview) may
  // have queued in the shared pending queue at once. A schedule reserves one
  // slot before enqueueing; once a mview holds this many queued refreshes, a
  // further (non-force) schedule for it is rejected with OB_SIZE_OVERFLOW so a
  // mview repeatedly scheduling in a hot loop cannot flood the shared queue
  // (MAX_PENDING_TASK_CNT) and starve other mviews. This caps the number of
  // *refreshes*, not the underlying per-refresh task count.
  static const int64_t MAX_PENDING_REFRESH_CNT_PER_MVIEW = 100;
  // Max time a write-side entry blocks waiting for RELOADING → READY before
  // returning OB_TIMEOUT. Bounded so a stuck reload cannot hang RPC threads.
  static const int64_t RELOAD_WAIT_TIMEOUT_MS = 10 * 1000;
  // A forced schedule may only hold the FORCE block while no other DROP/FORCE
  // marker exists, so concurrent forced refreshes on the same mview serialize:
  // a contender spins on acquire_mview_block until the current owner releases
  // (release_mview_block) or the wait deadline elapses. Poll interval is
  // small so the hand-off is prompt; the overall wait is bounded by the worker
  // timeout (and ACQUIRE_FORCE_WAIT_TIMEOUT_US as a fallback when the worker has
  // no valid timeout_ts) so a wedged owner cannot pin the RPC thread forever.
  static const int64_t ACQUIRE_FORCE_RETRY_INTERVAL_US = 10 * 1000;
  static const int64_t ACQUIRE_FORCE_WAIT_TIMEOUT_US = 5 * 1000 * 1000;

  // Routes to SYS_LS leader: if local leader (and !force_rpc) dispatches to the
  // matching _local handler based on arg.is_kill_by_mview_id_; otherwise sends
  // one RPC to the leader. Retries OB_NOT_MASTER up to RETRY_CNT_LIMIT.
  //
  // arg.is_kill_by_mview_id_ = false: kill the single refresh identified by
  //   arg.refresh_id_.
  // arg.is_kill_by_mview_id_ = true: best-effort kill of every active refresh
  //   targeting arg.mview_id_. Used by DDL (drop materialized view / drop
  //   database) so that any in-flight refresh transaction (which holds the
  //   OBJ_TYPE_MATERIALIZED_VIEW EXCLUSIVE in-trans lock) is killed and rolled
  //   back before DDL tries to take that lock. Returns OB_SUCCESS when no
  //   active refresh exists.
  int kill_refresh(const obrpc::ObKillMViewRefreshArg &arg, bool force_rpc = false);
  // Acquire a per-mview block marker (DROP/FORCE). Public so the kill RPC
  // processor can plant the DROP block on the drop path before enumerating.
  int acquire_mview_block(uint64_t mview_id, int64_t flag);
  int kill_refresh_local(uint64_t tenant_id, int64_t refresh_id);
  // Pure enumerate-and-kill. Assumes any required block (DROP or FORCE) has
  // already been acquired by the caller: kill_refresh takes the DROP block on the
  // drop path, while the FORCE schedule path already holds its own FORCE marker.
  int kill_refreshes_by_mview_local(uint64_t tenant_id, uint64_t mview_id);
  int mark_all_tasks_canceled(uint64_t tenant_id, int64_t refresh_id);
  int cleanup_stale_drop_blocks();

private:
  int generate_refresh_id(int64_t &refresh_id);
  int write_run_start(const obrpc::ObScheduleMViewRefreshArg &arg,
                      const int64_t refresh_id,
                      const int64_t start_time,
                      const share::SCN &target_data_sync_scn);
  int check_mview_schedule_block(const uint64_t mview_id,
                                 const bool is_force_owner,
                                 const bool check_limit,
                                 const int64_t limit);
  // Check the schedule block for every mview in pending_tasks (root + all nested
  // dependencies), so a concurrent drop/force on any dependency rejects the whole
  // schedule. Only root_mview_id may be the force owner; dependencies are checked
  // as non-owners.
  int check_pending_tasks_schedule_block(
      const common::ObIArray<ObMViewPendingTask *> &pending_tasks,
      const uint64_t root_mview_id,
      const bool is_force_owner,
      const bool is_check_limit,
      int64_t &inc_count);
  int release_mview_block(uint64_t mview_id, int64_t flag);
  int try_clear_drop_block_by_lock(uint64_t tenant_id, uint64_t mview_id,
                                   share::schema::ObSchemaGetterGuard &schema_guard,
                                   bool &lock_succ);
  // Insert a fresh ObMViewContext for mview_id if absent (insert-once). A losing
  // racer that already allocated frees its object and reuses the existing node;
  // the pointer in the map is never replaced.
  int ensure_mview_ctx(uint64_t mview_id);
  // Erase the context node for mview_id under the bucket write lock, then free
  // the popped pointer. Used by drop cleanup and the full reset path.
  int erase_and_free_ctx(uint64_t mview_id);
  // Drop every context (follower transition / destroy): the leader-only block
  // markers are meaningless on a follower, and the queue is being cleared.
  int reset_mview_contexts();

  int build_pending_tasks(common::ObIAllocator &alloc,
                          const obrpc::ObScheduleMViewRefreshArg &arg,
                          const int64_t refresh_id,
                          const int64_t start_time,
                          const share::SCN &target_data_sync_scn,
                          common::ObIArray<ObMViewPendingTask *> &pending_tasks);
  int enqueue_reload_task(const common::ObIArray<ObMViewPendingTask *> &group,
                          bool &queue_full);
  // Iterate one loaded batch, cut it into (refresh_id, scn) groups, and enqueue
  // each group via enqueue_reload_task. Batches from load_tasks_batch always
  // contain complete groups, so the trailing group is flushed before returning.
  // Updates cursor (last_scn, last_rid) and counters; sets has_more = true iff
  // this batch may not be the last one.
  int process_reload_batch(const common::ObIArray<ObMViewPendingTask *> &batch,
                           uint64_t &last_scn,
                           int64_t &last_rid,
                           int64_t &total_loaded,
                           bool &queue_full,
                           bool &has_more);
  static int get_mview_tablet_id(const share::schema::ObTableSchema &table_schema,
                                   common::ObTabletID &tablet_id);
  int server_random_pick_for_tenant(uint64_t tenant_id, common::ObAddr &server);
  int process_single_task_result(const TaskResultEntry &entry);
  int drain_result_queue_and_clear_pending_queue();
  // Block the caller until reload_state_ leaves RELOADING. Returns OB_SUCCESS
  // when state is READY, OB_NOT_MASTER when state ended up NOT_READY (i.e.
  // switched to follower during the wait), or OB_TIMEOUT when the deadline
  // elapsed without transition. Protected entries should bail out on non-success.
  int wait_reload_ready(int64_t timeout_ms);
  int publish_reload_state(const ReloadState new_state);
  static bool need_delay_before_retry(int task_ret);
  // recycle_refresh body assuming the caller already waited for READY. All
  // in-manager chains (mark_task_success/failed/cancelled, resync_task_from_disk)
  // use this to avoid re-entering reload_cond_'s mutex.
  int inner_recycle_refresh(uint64_t tenant_id, int64_t refresh_id);
  int dec_tasks_queue_cnt(const common::ObIArray<ObMViewPendingTask *> &tasks,
                          const int64_t dec_count);

  int query_running_session_infos(uint64_t tenant_id,
                                  int64_t refresh_id,
                                  common::ObIArray<uint32_t> &out_session_ids,
                                  common::ObIArray<common::ObAddr> &out_addrs,
                                  bool &need_retry);
  // Helper for foreach_running_job (template body). Scans `infos` for
  // entries with session_id_ == 0, batch-fetches them from the inner table,
  // patches both `infos` and the in-memory queue. Returns the SQL error code
  // unchanged on failure; the caller handles best-effort degradation.
  int load_missing_session_ids_for_running(
      common::ObIArray<ObMViewPendingRunningJobInfo> &infos);
  static int get_rpc_timeout_us(int64_t expire_ts, int64_t &rpc_timeout_us);

private:
  ObMViewPendingTaskQueue queue_;
  ObMViewPendingTaskTableOperator table_operator_;
  ObMviewPendingTaskScheduler scheduler_;
  ObMViewPendingTaskInspectionTask inspection_task_;
  common::ObLinkQueue result_queue_;
  common::ObFIFOAllocator result_alloc_;
  // Protects reload_state_ for the slow-path condition wait. Fast-path readers
  // use ATOMIC_LOAD on reload_state_ directly and skip this mutex.
  mutable common::ObThreadCond reload_cond_;
  // Holds a ReloadState value. Plain int64_t because __atomic_load_n /
  // __atomic_store_n require a pointer to integer. All writes go through
  // ATOMIC_STORE under reload_cond_'s mutex + broadcast; all reads use
  // ATOMIC_LOAD (cond-mutex not required for the fast-path check).
  int64_t reload_state_;
  // Per-mview context map (mview_id -> ObMViewContext*). Currently carries the
  // schedule block markers; designed to hold further per-mview state later.
  // Best-effort guard for the kill-enumerate vs schedule-insert race:
  // schedule_task checks the block flags before inserting, kill / force set them.
  // Thread-safe via the hashmap's own bucket defend mode plus atomic field access;
  // no extra lock is taken and it is never held across SQL.
  MViewContextMap mview_context_map_;
  // Backs the ObMViewContext nodes; internally thread-safe (own spinlock).
  common::ObFIFOAllocator mview_ctx_alloc_;
  bool is_inited_;
};

} // namespace rootserver
} // namespace oceanbase
