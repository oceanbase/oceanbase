/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan
 * PubL v2. You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
 * KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
 * NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE. See the
 * Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_TRANSACTION_OB_TX_HOTSPOT_DEFINE_PUBLIC_HEADER
#define OCEANBASE_TRANSACTION_OB_TX_HOTSPOT_DEFINE_PUBLIC_HEADER

#include "lib/lock/ob_spin_rwlock.h"
#include "lib/atomic/ob_atomic.h"
#include "storage/tx/ob_trans_define.h"
#include "storage/tx/ob_trans_submit_log_cb.h"
#include "storage/tx/ob_tx_log.h"
#include "share/ob_errno.h"

namespace oceanbase
{
namespace transaction
{

typedef ObSEArray<ObTransID, 10> ObAggregatedTxIDArray;

enum class TxRedoFlushStatus : int64_t
{
  NORMAL_START = 10,
  NORMAL_FLUSHED = 80,

  PRIMARY_PREPARING = 110,
  PRIMARY_COLLECTING = 120,
  PRIMARY_AGGR_SUCCEEDED = 150,
  PRIMARY_AGGR_FAILED = 160,
  PRIMARY_COMPLETED = 180,

  SECONDARY_PREPARING = 510,
  SECONDARY_MIGRATING = 520,
  // All member redo submissions and ownership registration are complete; sync is pending.
  SECONDARY_MIGRATE_SUBMITTED = 525,
  SECONDARY_MIGRATE_SYNCED = 530,
  SECONDARY_MIGRATE_FAILED = 540,
  SECONDARY_MIGRATE_SUCCEEDED = 550,
};

enum class TxSecondaryRespStatus : int64_t
{
  RESP_NOT_SENT = 0,
  RESP_COMMITTED = 1,
  RESP_ABORTED_BY_SESSION = 2,
  RESP_SKIPPED_BY_PRIMARY = 3,
  // The local scheduler no longer needs this response; not a transaction result.
  RESP_NO_RESPONSE_NEEDED = 4,
};

const char *to_cstr(const TxRedoFlushStatus &status);
const char *to_cstr(const TxSecondaryRespStatus &status);

class ObPartTransCtx;
class ObTxHotspotRedoCache;

#ifdef OB_HOTSPOT_GROUP_COMMIT
// One stack-local operation shared by all rollback members. Never retain callback pointers
// here: redo sync/fast-commit may retire them between management-lock acquisitions.
struct ObHotspotRollbackWork
{
  static const int64_t DEFAULT_BUDGET_US = 1000 * 1000;
  static const int64_t PRIMARY_POLL_US = 1000;
  static const int64_t CHECKPOINT_BATCH_COUNT = 1024;
  enum class Phase { VALIDATE, LOCK, REDO_SUBMIT, WAIT_SYNC, CHECKPOINT, LOG_UNDO, TABLE_LOCK, FINISHED };
  enum class WaitReason { NONE, CTX_LOCK, LOG_CB, REDO, SYNC, CHECKPOINT, SECONDARY_REDO };
  enum class TimeoutSource { NONE, LOCAL, TX, LEGACY_CTX };

  explicit ObHotspotRollbackWork(const int64_t budget_us = DEFAULT_BUDGET_US,
                                const int64_t stmt_expire_wall_us = INT64_MAX);
  void begin_member(const ObTransID &tx_id, const bool primary);
  int check_deadline(int64_t &remaining_us);
  // Explicit clock samples allow arithmetic tests without a production clock seam.
  int check_deadline_at(const int64_t now_mono_us, const int64_t now_wall_us,
                        int64_t &remaining_us);
  void remember_cause(const int ret);
  void finish(const int ret, const share::ObLSID &ls_id, const ObTransID &primary_tx_id);
  const char *phase_name() const;
  const char *wait_reason_name() const;
  const char *timeout_source_name() const;
  int64_t to_string(char *buf, const int64_t buf_len) const;

  const int64_t start_mono_us_;
  const int64_t start_wall_us_;
  const int64_t budget_us_;
  const int64_t local_deadline_mono_us_;
  const int64_t stmt_expire_wall_us_;
  int64_t tx_expire_wall_us_;
  Phase phase_;
  WaitReason wait_reason_;
  TimeoutSource timeout_source_;
  int cause_ret_;
  int cleanup_ret_;
  int64_t elapsed_us_;
  int64_t budget_overrun_us_;
  int64_t redo_submit_us_;
  int64_t redo_sync_wait_us_;
  int64_t checkpoint_us_;
  int64_t lock_wait_us_;
  int64_t cb_wait_us_;
  int64_t log_prepare_submit_us_;
  int64_t table_lock_us_;
  int64_t redo_out_block_cnt_;
  int64_t checkpoint_batch_cnt_;
  int64_t removed_cb_cnt_;
  int64_t rollback_log_out_cnt_;
  int64_t retry_cnt_;
  int64_t member_cnt_;
  int64_t finished_member_cnt_;
  int64_t member_index_;
  ObTransID member_tx_id_;
  bool primary_;
  bool snapshot_valid_;
  bool range_checked_;
  bool range_valid_;
  bool done_;
  int64_t epoch_;
  int64_t last_op_sn_;
  ObTxSEQ last_seq_;
  int64_t redo_state_;
  int64_t pending_write_;
  int64_t busy_cb_count_;
  // appended_ sampled under CtxLockGuard::MODE::ALL each rollback round.
  // redo_flushed_upto compares monotonic progress (logged_ + unlog_removed_)
  // against it; ctx-lock-free table-lock appends land above the bound and
  // converge via the checkpoint recheck and next round's resample.
  int64_t flush_bound_;
  ObTxSEQ from_seq_;
  ObTxSEQ to_seq_;
  share::SCN last_redo_scn_;
};
#endif

class ObHotspotSchedulerResponseTask : public ObTransTask
{
public:
  ObHotspotSchedulerResponseTask()
      : ObTransTask(ObTransRetryTaskType::SECONDARY_TX_RESP_SCHEDULER),
        primary_tx_ctx_(nullptr),
        tx_result_(OB_INVALID_ARGUMENT),
        commit_version_(),
        retry_cnt_(0),
        in_queue_(0)
  {}
  ~ObHotspotSchedulerResponseTask();

  void reset();
  void destroy();
  int init(ObPartTransCtx *tx_ctx);
  int enable(const int ret_code,
             const share::SCN commit_version,
             const bool replay_supplement = false);
  int handle();
  // Called when the worker is done. The owning ctx and this embedded task may
  // be destroyed before finish() returns.
  void finish();
  // Caller must exclude concurrent task mutation; the handle uses hotspot_lock_.
  // Returns the held ctx reference without reverting it. Revert outside locks.
  ObPartTransCtx *detach();
  bool is_in_queue() const;
  int64_t get_retry_cnt() const { return ATOMIC_LOAD(&retry_cnt_); }

  bool is_valid() const { return OB_NOT_NULL(primary_tx_ctx_); }
  bool is_enabled() const { return tx_result_ != OB_INVALID_ARGUMENT; }

  int set_in_queue(const bool in_queue_status);

  TO_STRING_KV(KP(this), K(tx_result_), K(commit_version_), "retry_cnt", get_retry_cnt(),
               KP(primary_tx_ctx_),
               K_(in_queue))
public:
  ObPartTransCtx *primary_tx_ctx_;
  int tx_result_;
  share::SCN commit_version_;
  int64_t retry_cnt_;
private:
  int64_t in_queue_;
};

class ObTxHotspotRedoCacheHandle
{
  DISALLOW_COPY_AND_ASSIGN(ObTxHotspotRedoCacheHandle);
public:
  struct HotspotLogCbView
  {
    int64_t free_cb_count;
    int64_t busy_cb_count;
    int64_t idle_cb_count;
    bool all_redo_synced;
    bool empty() const { return free_cb_count == 0 && busy_cb_count == 0 && idle_cb_count == 0; }
  };

  explicit ObTxHotspotRedoCacheHandle(TransModulePageAllocator &allocator)
      : hotspot_lock_(),
        allocator_(allocator),
        cache_(nullptr),
        active_ref_cnt_(0),
        resp_task_(),
        primary_last_seq_no_(ObTxSEQ(1, 0))
  {}
  ~ObTxHotspotRedoCacheHandle() { reset(); }

  int init(const ObTransID primary_id, const int64_t count, ObPartTransCtx *ctx);
  int insert_into(ObPartTransCtx *other_ctx);
  int append_hotspot_idle_cb(ObTxLogCb *log_cb, const int64_t target_cb_cnt);
  bool need_rearrange() const;
  int rearrange_hotspot_task();
  int extract_hotspot_redo(const int64_t cache_index);
  int assign_remapped_seq_ranges(const ObTxSEQ first_scn, const ObTxSEQ primary_last_seq_no);
  int get_secondary_tx_redo_range(const int64_t cache_index,
                                  ObSecondaryTxRedoRange &range,
                                  int64_t &fill_redo_data_size);
  int try_flush_hotspot_redo(const int64_t cache_index,
                             ObTxRedoLog &redo_log,
                             ObTxSEQ &redo_seq);
  void need_increase_logging_concurrency(int64_t &target_cnt) const;
  ObTxSEQ get_max_sub_tx_seq_no() const;
  int64_t get_hotspot_cache_count() const;
  int64_t get_free_cb_count() const;
  int64_t get_busy_cb_count() const;
  int64_t get_idle_cb_count() const;
  // Try-lock versions for to_string_() diagnostic output only.
  // Return -1 when lock is unavailable (e.g., caller holds write lock).
  int64_t try_get_free_cb_count() const;
  int64_t try_get_busy_cb_count() const;
  int64_t try_get_idle_cb_count() const;
  void get_cb_list_count(int64_t &free_cb_cnt, int64_t &busy_cb_cnt, int64_t &idle_cb_cnt) const;
  HotspotLogCbView get_cb_list_snapshot() const;
  share::SCN get_min_busy_log_ts() const;
  share::SCN get_max_busy_log_ts() const;
  palf::LSN get_max_busy_lsn() const;
  void get_max_busy_log_ts_and_lsn(share::SCN &log_ts, palf::LSN &lsn) const;
  int get_free_cb(ObTxLogCb *&log_cb);
  int add_busy_cb(ObTxLogCb *log_cb);
  int return_hotspot_cb(ObTxLogCb *log_cb, const bool is_idle, const bool continued_use = false);
  void compare_hotspot_cb(ObTxLogCb *&normal_log_cb,
                          ObTxLogCb *&hotspot_log_cb,
                          bool &is_hotspot_larger);
  int check_status(const int64_t index, bool &need_fill_redo_buf, bool &need_submit_log);
  int set_no_redo_secondaries_synced();
  int after_flush_hotspot_redo(const int64_t cache_index,
                               const share::SCN log_scn,
                               const int submit_ret_code);
  int after_sync_hotspot_redo(const int64_t cache_index,
                              const bool sync_res,
                              const share::SCN log_scn);
  int remove_synced_hotspot_redo(const int64_t cache_index, int64_t &need_remove_count);
  int push_response_task(const int ret_code,
                         const share::SCN commit_version,
                         const bool replay_supplement = false);
  // only_if_not_queued preserves a task still owned by a queued worker.
  // Detaching and reverting the task reference may destroy this handle.
  void finish_response_task(const bool only_if_not_queued = false);
  bool all_redo_flushed() const;
  bool all_redo_synced() const;
  bool all_redo_frozen_flushed() const;
  void clear_all_last_submit_ret_code();
  void record_all_secondary_finished();
  void inc_dispatch_msg_sent();
  int64_t get_pending_dispatch_msg_cnt() const;
  int64_t get_pending_submit_other_redo_msg_cnt() const;
  void inc_pending_dispatch_msg_cnt();
  void dec_pending_dispatch_msg_cnt();
  void inc_pending_submit_other_redo_msg_cnt();
  void dec_pending_submit_other_redo_msg_cnt();
  int get_rollback_range(const ObTransID *secondary_tx_id, ObTxSEQ &from_seq, ObTxSEQ &to_seq) const;
  // True if this secondary still has a filled-but-unsubmitted buf, or a submit in progress.
  // Already-submitted redo (including unsynced) is not counted.
  int has_unsubmitted_secondary_redo(const ObTransID &secondary_tx_id, bool &has_unsubmitted) const;
  // data_rolled_back_ on this cache slot: RollbackToLog for this secondary has been submitted.
  int is_secondary_data_rolled_back(const int64_t cache_index, bool &rolled_back) const;
  // Discard a filled-but-unsubmitted buf and zero unsynced_node_cnt_ so all_redo_flushed can finish.
  // Does not touch already-submitted redo.
  int discard_unsubmitted_secondary_redo(const int64_t cache_index);
  int rollback_secondary_memtable_and_mark(const ObTransID &secondary_tx_id);
  int try_release_idle_log_cb(ObPartTransCtx *primary_tx_ctx, const bool force_release);
  bool need_rollback_primary_tx() const;
  int abort_secondary_txs(const int reason);
  int cleanup_hotspot_cache_and_revert_secondary_ctxs();
  int before_prepare_secondaries(const share::SCN &version);
  // Caller keeps the owning ctx alive and holds no ctx lock. Descriptor checks
  // run outside hotspot_lock_ and never retain a descriptor pin across phases.
  int response_scheduler(const int ret_code,
                         const share::SCN commit_version,
                         const bool replay_supplement = false);
  void record_on_success_ctx_lock_wait(const int64_t wait_us);
  void record_aggr_start_ts();
  void record_dispatch_start_ts();
  void record_aggr_end_ts();
  int64_t get_aggr_end_ts() const;
  void set_terminal_ret(const int ret_code);
  int get_terminal_ret() const;
  void set_terminal_ret_if_success(const int ret_code);
  void inc_freeze_accel();
  void inc_dispatch_msg_skipped();
  void inc_submit_block_frozen();
  void inc_cb_alloc_fail();
  int reuse();
  void reset();
  void set_primary_last_seq_no(const ObTxSEQ &seq);
  inline bool has_active_cache() const { return nullptr != ATOMIC_LOAD(&cache_); }
  ObTxSEQ get_primary_last_seq_no() const;

  TO_STRING_KV(KP(cache_),
               K(active_ref_cnt_),
               K(primary_last_seq_no_),
               K(resp_task_));

private:
  class ActiveRefGuard
  {
    DISALLOW_COPY_AND_ASSIGN(ActiveRefGuard);
  public:
    explicit ActiveRefGuard(ObTxHotspotRedoCacheHandle &handle);
    ~ActiveRefGuard();
  private:
    ObTxHotspotRedoCacheHandle &handle_;
  };

  static int64_t align_up_(const int64_t value, const int64_t align);
  static int64_t calc_array_offset_();
  int check_no_active_cache_ref_locked_(const char *op) const;
  void force_destroy_cache_core_locked_();

  // Lock contention stats for clog callback hot paths.
  // Lives on Handle (not Cache) so updates are lock-free — no cache_ pointer
  // dereference needed. Handle is a direct member of ObPartTransCtx, always
  // alive during on_success/after_sync callbacks.
  struct CallbackLockWaitStats
  {
    int64_t lock_attempt_cnt_ = 0;
    int64_t max_wait_us_ = 0;

    void atomic_reset()
    {
      ATOMIC_STORE(&lock_attempt_cnt_, 0);
      ATOMIC_STORE(&max_wait_us_, 0);
    }

    // Merge locally aggregated samples with one shared counter update.
    void atomic_merge(const int64_t lock_attempt_cnt, const int64_t max_wait_us)
    {
      if (lock_attempt_cnt > 0) {
        ATOMIC_FAA(&lock_attempt_cnt_, lock_attempt_cnt);
      }
      int64_t old = ATOMIC_LOAD(&max_wait_us_);
      while (max_wait_us > old && !ATOMIC_BCAS(&max_wait_us_, old, max_wait_us)) {
        old = ATOMIC_LOAD(&max_wait_us_);
      }
    }

    void atomic_snapshot_and_reset(int64_t &lock_attempt_cnt, int64_t &max_wait_us)
    {
      lock_attempt_cnt = ATOMIC_TAS(&lock_attempt_cnt_, 0);
      max_wait_us = ATOMIC_TAS(&max_wait_us_, 0);
    }

    int64_t load_lock_attempt_cnt() const { return ATOMIC_LOAD(&lock_attempt_cnt_); }
    int64_t load_max_wait_us() const { return ATOMIC_LOAD(&max_wait_us_); }
  };

  void reset_lock_stats_()
  {
    on_success_ctx_lock_wait_stats_.atomic_reset();
    after_sync_lock_wait_stats_.atomic_reset();
  }

  // mutable: allow const methods (e.g. try_get_*_cb_count) to acquire read lock
  mutable common::SpinRWLock hotspot_lock_;
  TransModulePageAllocator &allocator_;
  ObTxHotspotRedoCache *cache_;
  int64_t active_ref_cnt_;
  ObHotspotSchedulerResponseTask resp_task_;
  ObTxSEQ primary_last_seq_no_;

  // Lock contention stats (moved from Cache for lock-free updates).
  // on_success_ctx_lock_wait: primary ctx lock_ wait during on_success callback.
  // after_sync_lock_wait: hotspot_lock_ wait during after_sync callback.
  CallbackLockWaitStats on_success_ctx_lock_wait_stats_;
  CallbackLockWaitStats after_sync_lock_wait_stats_;
};

} // namespace transaction
} // namespace oceanbase

// Keep the public facade above independent of the closed core. Non-CE builds
// include the close module here to complete the hotspot core definitions.
#ifdef OB_HOTSPOT_GROUP_COMMIT
#include "close_modules/hotspot_group_commit/storage/tx/ob_tx_hotspot_core.h"
#endif

#endif // OCEANBASE_TRANSACTION_OB_TX_HOTSPOT_DEFINE_PUBLIC_HEADER
