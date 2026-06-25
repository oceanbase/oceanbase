/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan
 * PubL v2. You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
 * KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
 * NON-INFRINGEMENT, MERCHANTABILITY OR FITNESS FOR A PARTICULAR PURPOSE. See the
 * Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_TRANSACTION_OB_TX_HOTSPOT_DEFINE_PUBLIC_HEADER
#define OCEANBASE_TRANSACTION_OB_TX_HOTSPOT_DEFINE_PUBLIC_HEADER

#include "lib/lock/ob_spin_rwlock.h"
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
};

const char *to_cstr(const TxRedoFlushStatus &status);
const char *to_cstr(const TxSecondaryRespStatus &status);

class ObPartTransCtx;
class ObTxHotspotRedoCache;

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
  int enable(const int ret_code, const share::SCN commit_version);
  int handle();
  bool is_in_queue() const;

  bool is_valid() const { return OB_NOT_NULL(primary_tx_ctx_); }
  bool is_enabled() const { return tx_result_ != OB_INVALID_ARGUMENT; }

  int set_in_queue(const bool in_queue_status);

  TO_STRING_KV(KP(this), K(tx_result_), K(commit_version_), K(retry_cnt_), KP(primary_tx_ctx_),
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
  int after_flush_hotspot_redo(const int64_t cache_index,
                               const share::SCN log_scn,
                               const int submit_ret_code);
  int after_sync_hotspot_redo(const int64_t cache_index,
                              const bool sync_res,
                              const share::SCN log_scn);
  int remove_synced_hotspot_redo(const int64_t cache_index, int64_t &need_remove_count);
  void push_response_task(const int ret_code, const share::SCN commit_version);
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
  int rollback_secondary_memtable_and_mark(const ObTransID &secondary_tx_id);
  int try_release_idle_log_cb(ObPartTransCtx *primary_tx_ctx);
  bool need_rollback_primary_tx() const;
  int abort_secondary_txs(const int reason);
  int cleanup_hotspot_cache_and_revert_secondary_ctxs();
  int response_scheduler(const int ret_code, const share::SCN commit_version);
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

  common::SpinRWLock hotspot_lock_;
  TransModulePageAllocator &allocator_;
  ObTxHotspotRedoCache *cache_;
  int64_t active_ref_cnt_;
  ObHotspotSchedulerResponseTask resp_task_;
  ObTxSEQ primary_last_seq_no_;
};

} // namespace transaction
} // namespace oceanbase

// Keep the public facade above independent of the closed core. Non-CE builds
// include the close module here to complete the hotspot core definitions.
#ifdef OB_HOTSPOT_GROUP_COMMIT
#include "close_modules/hotspot_group_commit/storage/tx/ob_tx_hotspot_core.h"
#endif

#endif // OCEANBASE_TRANSACTION_OB_TX_HOTSPOT_DEFINE_PUBLIC_HEADER
