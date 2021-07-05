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

#ifndef OCEANBASE_TRANSACTION_OB_TRANS_CTX_MGR_
#define OCEANBASE_TRANSACTION_OB_TRANS_CTX_MGR_
#include "lib/lock/ob_spin_rwlock.h"
#include "lib/lock/ob_tc_rwlock.h"
#include "lib/lock/ob_bucket_lock.h"
#include "lib/hash/ob_linear_hash_map.h"
#include "lib/hash/ob_link_hashmap.h"
#include "lib/container/ob_se_array.h"
#include "common/ob_simple_iterator.h"
#include "storage/memtable/ob_memtable_context.h"
#include "storage/ob_partition_loop_worker.h"
#include "ob_trans_ctx.h"
#include "ob_trans_stat.h"
#include "ob_trans_partition_stat.h"

namespace oceanbase {

namespace common {
class ObPartitionKey;
class SpinRWLock;
}  // namespace common

namespace storage {
class LeaderActiveArg;
}

namespace blocksstable {
class ObMacroBlockWriter;
}

namespace memtable {
class ObIMemtable;
class ObMemtable;
class ObIMemtableCtx;
}  // namespace memtable

namespace transaction {
class ObTransCtx;
class ObITsMgr;
class ObITransResultInfoMgr;
}  // namespace transaction

namespace transaction {
//  ObPartitionIterator is used to traverse all partitions maintained on the observer
typedef common::ObSimpleIterator<common::ObPartitionKey, ObModIds::OB_TRANS_VIRTUAL_TABLE_PARTITION, 16>
    ObPartitionIterator;
//  ObTransStatIterator is used to travserse all trans contexts in a specified partition
typedef common::ObSimpleIterator<ObTransStat, ObModIds::OB_TRANS_VIRTUAL_TABLE_TRANS_STAT, 16> ObTransStatIterator;
//  ObTransPartitionStatIterator is used to traverse manager status information
//  in a specified partition
typedef common::ObSimpleIterator<ObTransPartitionStat, ObModIds::OB_TRANS_VIRTUAL_TABLE_PARTITION_STAT, 16>
    ObTransPartitionMgrStatIterator;
// ObTransLockStatIterator is used to traverse trans
// and mapping information of the row locked by the transaction
typedef common::ObSimpleIterator<ObTransLockStat, ObModIds::OB_TRANS_VIRTUAL_TABLE_TRANS_STAT, 16>
    ObTransLockStatIterator;
typedef common::ObSimpleIterator<ObTransResultInfoStat, ObModIds::OB_TRANS_VIRTUAL_TABLE_TRANS_STAT, 16>
    ObTransResultInfoStatIterator;
typedef common::ObSimpleIterator<ObDuplicatePartitionStat, ObModIds::OB_TRANS_VIRTUAL_TABLE_TRANS_STAT, 16>
    ObDuplicatePartitionStatIterator;

static const int64_t SHRINK_THRESHOLD = 128;
typedef common::LinkHashNode<common::ObPartitionKey> PartitionTransCtxMgrHashNode;
typedef common::LinkHashValue<common::ObPartitionKey> PartitionTransCtxMgrHashValue;
typedef common::ObLinkHashMap<ObTransKey, ObTransCtx, TransCtxAlloc, common::RefHandle, SHRINK_THRESHOLD> CtxMap;

struct ObELRStatSummary {
  ObELRStatSummary()
  {
    reset();
  }

  void reset()
  {
    with_dependency_trx_count_ = 0;
    without_dependency_trx_count_ = 0;
    end_trans_by_prev_count_ = 0;
    end_trans_by_checkpoint_count_ = 0;
    end_trans_by_self_count_ = 0;
  }

  TO_STRING_KV(K_(with_dependency_trx_count), K_(without_dependency_trx_count), K_(end_trans_by_prev_count),
      K_(end_trans_by_checkpoint_count), K_(end_trans_by_self_count));

  uint64_t with_dependency_trx_count_;
  uint64_t without_dependency_trx_count_;
  uint64_t end_trans_by_prev_count_;
  uint64_t end_trans_by_checkpoint_count_;
  uint64_t end_trans_by_self_count_;
};

class ObPartitionTransCtxMgr : public PartitionTransCtxMgrHashValue {
  friend class LeaderRevokeFunctor;
  friend class KillTransCtxFunctor;
  friend class EraseTransCtxStatusFunctor;
  friend class IteratePartitionMgrStatFunctor;
  friend class PrintFunctor;
  friend class ReleaseAllTransCtxFunctor;
  friend class ObTransCtx;
  friend class ObDistTransCtx;
  friend class ObPartTransCtx;
  friend class ObScheTransCtx;
  friend class ObCoordTransCtx;
  friend class ObSlaveTransCtx;
  friend class ObTransTimer;

public:
  typedef common::RWLock RWLock;
  typedef RWLock::RLockGuard RLockGuard;
  typedef RWLock::WLockGuard WLockGuard;
  ObPartitionTransCtxMgr()
      : rwlock_(ObLatchIds::DEFAULT_SPIN_RWLOCK), minor_merge_lock_(ObLatchIds::DEFAULT_SPIN_RWLOCK)
  {
    reset();
  }
  virtual ~ObPartitionTransCtxMgr()
  {
    destroy();
  }
  int init(const common::ObPartitionKey& partition, const int64_t ctx_type, ObITsMgr* ts_mgr,
      storage::ObPartitionService* partition_service, CtxMap* ctx_map);
  void destroy();
  void reset();

public:
  // alloc transaction context if alloc is set true when transaction context not exist
  int get_trans_ctx(const ObTransID& trans_id, const bool for_replay, const bool is_readonly,
      const bool is_bounded_staleness_read, const bool need_completed_dirty_txn, bool& alloc, ObTransCtx*& ctx);
  int revert_trans_ctx(ObTransCtx* ctx);
  int acquire_ctx_ref(const ObTransID& trans_id)
  {
    return get_trans_ctx_(trans_id);
  }
  int release_ctx_ref(ObTransCtx* ctx)
  {
    return revert_trans_ctx_(ctx);
  }
  int erase_trans_ctx(const ObTransID& trans_id)
  {
    return erase_trans_ctx_(trans_id);
  }

  // get the min prepare version of trans module in specified partition of current observer
  int get_min_uncommit_prepare_version(int64_t& min_prepare_version);
  int get_min_uncommit_log(uint64_t& min_uncommit_log_id, int64_t& min_uncommit_log_ts);
  int get_min_prepare_version(const int64_t log_ts, int64_t& min_prepare_version);
  int gc_trans_result_info(const int64_t checkpoint_ts);

  // check the partition status
  // if stopped, the new trans ctx is not created; otherwise, the ctx is created normally
  bool is_stopped() const
  {
    return is_stopped_();
  }
  int stop(const bool graceful);
  int kill_all_trans(const KillTransArg& arg, bool& is_all_trans_clear);
  int wait_1pc_trx_end(bool& all_1pc_trx_end);
  int check_all_trans_in_trans_table_state();
  int submit_log_for_split(bool& log_finished);
  int copy_trans_table(ObTransService* txs, const ObIArray<ObPartitionKey>& dest_array);
  int calculate_trans_cost(const ObTransID& tid, uint64_t& cost);
  int block(bool& is_all_trans_clear);
  int active(const ObPartitionKey& partition);

  int64_t get_ctx_count() const
  {
    return get_ctx_count_();
  }
  int leader_revoke(const bool first_check);
  int leader_takeover(const int64_t checkpoint);
  int leader_active(const storage::LeaderActiveArg& arg);
  int check_schema_version_elapsed(const int64_t schema_version, const int64_t refreshed_schema_ts);
  int check_ctx_create_timestamp_elapsed(const int64_t ts);
  const common::ObPartitionKey& get_partition() const
  {
    return partition_;
  }
  bool is_trans_partition_leader() const
  {
    const int64_t state = ATOMIC_LOAD(&state_);
    return (state == State::L_WORKING || state == State::L_BLOCKED);
  }
  int recover_pg_guard();

public:
  void inc_total_ctx_count()
  {
    (void)ATOMIC_AAF(&total_ctx_count_, 1);
  }
  void inc_read_only_count()
  {
    (void)ATOMIC_AAF(&read_only_count_, 1);
  }
  void inc_active_read_write_count()
  {
    (void)ATOMIC_AAF(&active_read_write_count_, 1);
  }
  int64_t desc_total_ctx_count()
  {
    return ATOMIC_AAF(&total_ctx_count_, -1);
  }
  int64_t desc_read_only_count()
  {
    return ATOMIC_AAF(&read_only_count_, -1);
  }
  int64_t desc_active_read_write_count()
  {
    return ATOMIC_AAF(&active_read_write_count_, -1);
  }
  int64_t get_active_read_write_count() const
  {
    return ATOMIC_LOAD(&active_read_write_count_);
  }
  int64_t get_read_only_count() const
  {
    return ATOMIC_LOAD(&read_only_count_);
  }
  void inc_with_dependency_trx_count()
  {
    (void)ATOMIC_AAF(&with_dependency_trx_count_, 1);
  }
  void inc_without_dependency_trx_count()
  {
    (void)ATOMIC_AAF(&without_dependency_trx_count_, 1);
  }
  void inc_end_trans_by_prev_count()
  {
    (void)ATOMIC_AAF(&end_trans_by_prev_count_, 1);
  }
  void inc_end_trans_by_checkpoint_count()
  {
    (void)ATOMIC_AAF(&end_trans_by_checkpoint_count_, 1);
  }
  void inc_end_trans_by_self_count()
  {
    (void)ATOMIC_AAF(&end_trans_by_self_count_, 1);
  }
  uint64_t get_with_dependency_trx_count()
  {
    return ATOMIC_LOAD(&with_dependency_trx_count_);
  }
  uint64_t get_without_dependency_trx_count()
  {
    return ATOMIC_LOAD(&without_dependency_trx_count_);
  }
  uint64_t get_end_trans_by_prev_count()
  {
    return ATOMIC_LOAD(&end_trans_by_prev_count_);
  }
  uint64_t get_end_trans_by_checkpoint_count()
  {
    return ATOMIC_LOAD(&end_trans_by_checkpoint_count_);
  }
  uint64_t get_end_trans_by_self_count()
  {
    return ATOMIC_LOAD(&end_trans_by_self_count_);
  }
  void reset_elr_statistic();
  int iterate_trans_stat(ObTransStatIterator& trans_stat_iter);
  int iterate_trans_lock_stat(ObTransLockStatIterator& trans_lock_stat_iter);
  int iterate_trans_result_info_in_TRIM(ObTransResultInfoStatIterator& iter);
  int iterate_trans_table(const uint64_t end_log_id, blocksstable::ObMacroBlockWriter& writer);
  void print_all_ctx(const int64_t max_print, const bool verbose);
  int inactive_tenant(const uint64_t tenant_id);
  int checkpoint(const int64_t checkpoint_base_ts, storage::ObPartitionLoopWorker* lp_worker);
  int update_max_trans_version(const int64_t trans_version);
  int get_max_trans_version(int64_t& max_trans_version) const;
  int relocate_data(memtable::ObIMemtable* memtable);
  int prepare_changing_leader(const ObAddr& proposal_leader, const int64_t round, const int64_t cnt);
  int update_max_replay_batch_commit_version(const int64_t batch_commit_version);
  int check_scheduler_status();
  int64_t get_partition_type() const
  {
    return ctx_type_;
  }
  // TRIM is short for Trans Result Info manager
  int insert_in_TRIM(ObTransResultInfo* trans_result_info, bool& registered);
  int get_state_in_TRIM(const ObTransID& trans_id, int& state);
  int replay_start_working_log(const int64_t timestamp, const uint64_t log_id);
  int audit_partition(ObPartitionAuditInfoCache& cache, bool commit);
  int get_partition_audit_info(ObPartitionAuditInfo& info);
  int set_partition_audit_base_row_count(const int64_t count);
  ObDupTablePartitionMgr* get_dup_table_partition_mgr() const
  {
    return dup_table_partition_mgr_;
  }
  ObDupTablePartitionInfo* get_dup_table_partition_info() const
  {
    return dup_table_partition_info_;
  }
  int print_dup_table_partition_mgr();
  int is_dup_table_partition_serving(const bool is_master, bool& is_serving);
  int iterate_duplicate_partition_stat(ObDuplicatePartitionStatIterator& iter);
  int check_and_inc_read_only_trx_count();
  int check_and_dec_read_only_trx_count();
  int remove_callback_for_uncommited_txn(memtable::ObMemtable* mt);
  int remove_mem_ctx_for_trans_ctx(memtable::ObMemtable* mt);

  int mark_dirty_trans(const memtable::ObMemtable* const frozen_memtable,
      const memtable::ObMemtable* const active_memtable, int64_t& cb_cnt, int64_t& applied_log_ts);

  int get_applied_log_ts(int64_t& applied_log_ts);

  virtual int lock_for_read(
      const ObLockForReadArg& lock_for_read_arg, bool& can_read, int64_t& trans_version, bool& is_determined_state);
  virtual int get_transaction_status_with_log_ts(const transaction::ObTransID& data_trans_id, const int64_t log_ts,
      ObTransTableStatusType& status, int64_t& trans_version);
  virtual int check_sql_sequence_can_read(
      const transaction::ObTransID& data_trans_id, const int64_t sql_sequence, bool& can_read);
  int check_row_locked(const ObStoreRowkey& key, memtable::ObIMvccCtx& ctx, const transaction::ObTransID& read_trans_id,
      const transaction::ObTransID& data_trans_id, const int32_t sql_sequence,
      storage::ObStoreRowLockState& lock_state);
  int get_max_trans_version_before_given_log_ts(
      const int64_t log_ts, int64_t& max_trans_version, bool& is_all_rollback_trans);
  int clear_unused_trans_status(const int64_t max_cleanout_log_ts);
  int has_terminated_trx_in_given_log_ts_range(
      const int64_t start_log_ts, const int64_t end_log_ts, bool& has_terminated_trx);
  bool is_clog_aggregation_enabled();
  int64_t get_restore_snapshot_version() const
  {
    return ATOMIC_LOAD(&restore_snapshot_version_);
  }
  uint64_t get_last_restore_log_id() const
  {
    return ATOMIC_LOAD(&last_restore_log_id_);
  }
  int set_last_restore_log_id(const uint64_t last_restore_log_id);
  int set_restore_snapshot_version(const int64_t restore_snapshot_version);
  int update_restore_replay_info(const int64_t restore_snapshot_version, const uint64_t last_restore_log_id);
  ObTransLogBufferAggreContainer& get_trans_log_buffer_aggre_container()
  {
    return aggre_log_container_;
  }
  storage::ObIPartitionGroupGuard& get_pg_guard()
  {
    return pg_guard_;
  }
  int init_dup_table_mgr();
  int xa_scheduler_hb_req();
  int lock_minor_merge_lock()
  {
    return minor_merge_lock_.rdlock();
  }
  int unlock_minor_merge_lock()
  {
    return minor_merge_lock_.rdunlock();
  };
  bool is_partition_stopped()
  {
    return State::STOPPED == ATOMIC_LOAD(&state_);
  }

  TO_STRING_KV(KP(this), K_(partition), K_(state), K_(ctx_type), K_(read_only_count), K_(active_read_write_count),
      K_(total_ctx_count), K_(restore_snapshot_version), K_(last_restore_log_id), "uref",
      ((ObTransCtxType::SCHEDULER == ctx_type_ || !is_inited_) ? -1 : get_uref()));

private:
  bool is_participant_() const
  {
    return ObTransCtxType::PARTICIPANT == ctx_type_ || ObTransCtxType::SLAVE_PARTICIPANT == ctx_type_;
  }
  int get_partition_state_();

private:
  DISALLOW_COPY_AND_ASSIGN(ObPartitionTransCtxMgr);

private:
  static const int64_t OB_TRANS_STATISTICS_INTERVAL = 60 * 1000 * 1000;
  static const int64_t OB_PARTITION_AUDIT_LOCAL_STORAGE_COUNT = 4;

private:
  // erase transaction context from hashmap
  int erase_trans_ctx_(const ObTransID& trans_id);
  int remove_ctx_from_arr_(ObTransCtx* ctx);
  int get_trans_ctx_(const ObTransID& trans_id);
  int get_trans_ctx_(const ObTransID& trans_id, ObTransCtx*& ctx);
  int revert_trans_ctx_(ObTransCtx* ctx);
  int release_all_trans_ctx_();
  int process_callback_(const ObEndTransCallbackArray& cb_array) const;
  void print_all_ctx_(const int64_t max_print, const bool verbose);
  int64_t get_ctx_count_() const
  {
    return ATOMIC_LOAD(&total_ctx_count_);
  }
  int get_trans_ctx_(const ObTransID& trans_id, const bool for_replay, const bool is_readonly,
      const bool is_bounded_staleness_read, const bool need_completed_dirty_txn, bool& alloc, ObTransCtx*& ctx);
  bool has_valid_compact_mode_();
  int set_compact_mode_(const int compact_mode);
  int get_compact_mode_() const
  {
    return compact_mode_;
  }
  int init_trans_result_info_mgr_();
  int lock_for_read_(
      const ObLockForReadArg& lock_for_read_arg, bool& can_read, int64_t& trans_version, bool& is_determined_state);
  int get_transaction_status_with_log_ts_(const transaction::ObTransID& data_trans_id, const int64_t log_ts,
      ObTransTableStatusType& status, int64_t& trans_version);
  int check_sql_sequence_can_read_(
      const transaction::ObTransID& data_trans_id, const int64_t sql_sequence, bool& can_read);
  int check_row_locked_(const ObStoreRowkey& key, memtable::ObIMvccCtx& ctx,
      const transaction::ObTransID& read_trans_id, const transaction::ObTransID& data_trans_id,
      const int32_t sql_sequence, storage::ObStoreRowLockState& lock_state);

public:
  static const int64_t MAX_HASH_ITEM_PRINT = 16;

private:
  class CtxMapMgr {
  public:
    CtxMapMgr() : ctx_map_(nullptr)
    {
      reset();
    }
    ~CtxMapMgr()
    {
      destroy();
    }
    int init(const ObPartitionKey& partition, CtxMap* ctx_map);
    void destroy();
    void reset();
    // it is used to filter partition
    template <class Fn>
    class ObPartitionForEachFilterFunctor {
    public:
      explicit ObPartitionForEachFilterFunctor(const ObPartitionKey& partition, Fn& fn) : partition_(partition), fn_(fn)
      {}
      ~ObPartitionForEachFilterFunctor()
      {}
      bool operator()(const ObTransKey& trans_key, ObTransCtx* ctx_base);

    private:
      ObPartitionKey partition_;
      Fn& fn_;
    };
    template <class Fn>
    class ObPartitionRemoveIfFilterFunctor {
    public:
      explicit ObPartitionRemoveIfFilterFunctor(const ObPartitionKey& partition, Fn& fn)
          : partition_(partition), fn_(fn)
      {}
      ~ObPartitionRemoveIfFilterFunctor()
      {}
      bool operator()(const ObTransKey& trans_key, ObTransCtx* ctx_base);

    private:
      ObPartitionKey partition_;
      Fn& fn_;
    };

    int get(const ObTransID& trans_id, ObTransCtx*& ctx);
    int insert_and_get(const ObTransID& trans_id, ObTransCtx* ctx);
    void revert(ObTransCtx* ctx);
    int del(const ObTransID& trans_id);
    int64_t estimate_count()
    {
      return ctx_map_->count();
    }
    // if true is returned, continue to iterate; otherwise, stop the iteration
    template <typename Fn>
    int foreach_ctx(Fn& fn)
    {
      int ret = OB_SUCCESS;
      if (OB_NOT_NULL(ctx_map_)) {
        ObPartitionForEachFilterFunctor<Fn> filter_fn(partition_, fn);
        ret = ctx_map_->for_each(filter_fn);
      } else {
        ret = OB_NOT_INIT;
      }
      return ret;
    }
    // if true is returned, it indicates that it should be removed;
    // otherwise, it should not be removed
    template <typename Fn>
    int remove_if(Fn& fn)
    {
      int ret = OB_SUCCESS;
      if (OB_NOT_NULL(ctx_map_)) {
        ObPartitionRemoveIfFilterFunctor<Fn> filter_fn(partition_, fn);
        ret = ctx_map_->remove_if(filter_fn);
      } else {
        ret = OB_NOT_INIT;
      }
      return ret;
    }

  private:
    ObPartitionKey partition_;
    CtxMap* ctx_map_;
  };
  class State {
  public:
    static const int64_t INVALID = -1;
    static const int64_t INIT = 0;
    static const int64_t F_WORKING = 1;
    static const int64_t L_WORKING = 2;
    static const int64_t F_BLOCKED = 3;
    static const int64_t L_BLOCKED = 4;
    static const int64_t STOPPED = 5;
    static const int64_t WAIT = 6;
    static const int64_t END = 7;
    static const int64_t MAX = 8;

  public:
    static bool is_valid(const int64_t state)
    {
      return state > INVALID && state < MAX;
    }
  };
  class Ops {
  public:
    static const int64_t INVALID = -1;
    static const int64_t START = 0;
    static const int64_t LEADER_REVOKE = 1;
    static const int64_t LEADER_ACTIVE = 2;
    static const int64_t BLOCK = 3;
    static const int64_t STOP = 4;
    static const int64_t WAIT = 5;
    static const int64_t REMOVE = 6;
    static const int64_t MAX = 7;

  public:
    static bool is_valid(const int64_t op)
    {
      return op > INVALID && op < MAX;
    }
  };
  class StateHelper {
  public:
    explicit StateHelper(int64_t& state) : state_(state), last_state_(State::INVALID), is_switching_(false)
    {}
    ~StateHelper()
    {}
    int switch_state(const int64_t op);
    void restore_state();

  private:
    int64_t& state_;
    int64_t last_state_;
    bool is_switching_;
  };

private:
  bool is_master_() const
  {
    return State::L_WORKING == state_ || State::L_BLOCKED == state_;
  }
  bool is_blocked_() const
  {
    return State::F_BLOCKED == state_ || State::L_BLOCKED == state_;
  }
  bool is_stopped_() const
  {
    return State::STOPPED == ATOMIC_LOAD(&state_);
  }

private:
  bool is_inited_;
  int64_t ctx_type_;
  int64_t state_;
  CtxMapMgr ctx_map_mgr_;
  common::ObPartitionKey partition_;
  // lock for frozen_
  mutable RWLock rwlock_;
  // lock for concurrency between minor merge and remove partition
  // ATTENTION: the order between locks should be:
  //                     rwlock_ -> minor_merge_lock_
  RWLock minor_merge_lock_;
  // number of active read-only trans (no exiting)
  int64_t read_only_count_;
  // number of active read-write trans (no exiting)
  int64_t active_read_write_count_;
  // number of contexts whose memory are not released in current observer
  int64_t total_ctx_count_;
  int64_t min_uncommit_prepare_version_;
  int64_t max_trans_version_;
  // it is used to record the time point of leader takeover
  // gts must be refreshed to the newest before the leader provides services
  MonotonicTs leader_takeover_ts_;
  // record the max commit version in the replayed prepare logs of the one phase trans
  int64_t max_replay_batch_commit_version_;
  bool is_leader_serving_;
  // time source
  ObITsMgr* ts_mgr_;
  storage::ObIPartitionGroupGuard pg_guard_;
  ObITransResultInfoMgr* trans_result_info_mgr_;
  ObCoreLocalPartitionAuditInfo* core_local_partition_audit_info_;
  // compact mode: INVALID, MYSQL, ORACLE
  int compact_mode_;
  ObDupTablePartitionInfo* dup_table_partition_info_;
  ObDupTablePartitionMgr* dup_table_partition_mgr_;
  // the corresponding status for physical backup and recovery
  // it is used to record the position point for physical backup and recovery
  int64_t restore_snapshot_version_;
  // it is used to record the last restore log id pulling by physical backup and recovery
  uint64_t last_restore_log_id_;
  // the statistics for elr
  // number of trans with prev
  uint64_t with_dependency_trx_count_;
  // number of trans without prev
  uint64_t without_dependency_trx_count_;
  // number of trans ended by prev trans
  uint64_t end_trans_by_prev_count_;
  // number of trans ended by checkpoint
  uint64_t end_trans_by_checkpoint_count_;
  // number of trans ended by self replaying
  uint64_t end_trans_by_self_count_;
  int64_t clog_aggregation_buffer_amount_;
  int64_t last_refresh_tenant_config_ts_;
  ObTransLogBufferAggreContainer aggre_log_container_;
  bool is_dup_table_;
};

class PartitionTransCtxMgrAlloc {
public:
  static ObPartitionTransCtxMgr* alloc_value()
  {
    return NULL;
  }
  static void free_value(ObPartitionTransCtxMgr* p)
  {
    if (NULL != p) {
      TRANS_LOG(INFO, "ObPartitionTransCtxMgr release", K(*p));
      ObPartitionTransCtxMgrFactory::release(p);
    }
  }
  static PartitionTransCtxMgrHashNode* alloc_node(ObPartitionTransCtxMgr* p)
  {
    UNUSED(p);
    return op_alloc(PartitionTransCtxMgrHashNode);
  }
  static void free_node(PartitionTransCtxMgrHashNode* node)
  {
    if (NULL != node) {
      op_free(node);
      node = NULL;
    }
  }
};
typedef common::ObLinkHashMap<common::ObPartitionKey, ObPartitionTransCtxMgr, PartitionTransCtxMgrAlloc,
    common::RefHandle>
    PartitionCtxMap;

class ObITransCtxMgr {
public:
  ObITransCtxMgr()
  {}
  virtual ~ObITransCtxMgr()
  {}

public:
  // hold lock before revert or release trans_ctx
  virtual int get_trans_ctx(const common::ObPartitionKey& partition, const ObTransID& trans_id, const bool for_replay,
      const bool is_readonly, bool& alloc, ObTransCtx*& ctx) = 0;
  virtual int revert_trans_ctx(ObTransCtx* ctx) = 0;
};

class ObScheTransCtxMgr : public ObITransCtxMgr {
public:
  friend class ObTransCtx;
  ObScheTransCtxMgr()
  {
    reset();
  }
  ~ObScheTransCtxMgr()
  {
    destroy();
  }
  int init(ObITsMgr* ts_mgr, storage::ObPartitionService* partition_service);
  int start();
  int stop();
  int wait();
  void destroy();
  void reset();

public:
  // hold lock before revert or release trans_ctx
  int get_trans_ctx(const common::ObPartitionKey& partition, const ObTransID& trans_id, const bool for_replay,
      const bool is_readonly, bool& alloc, ObTransCtx*& ctx);
  // unlock transaction context
  int revert_trans_ctx(ObTransCtx* ctx);
  int inactive_tenant(const uint64_t tenant_id);
  void print_all_ctx(const int64_t count);
  int xa_scheduler_hb_req();

private:
  ObPartitionTransCtxMgr* get_partition_trans_ctx_mgr_(const common::ObPartitionKey& partition)
  {
    UNUSED(partition);
    return &partition_trans_ctx_mgr_;
  }
  DISALLOW_COPY_AND_ASSIGN(ObScheTransCtxMgr);

private:
  bool is_inited_;
  bool is_running_;
  CtxMap ctx_map_;
  ObPartitionTransCtxMgr partition_trans_ctx_mgr_;
  mutable common::DRWLock rwlock_;
  ObITsMgr* ts_mgr_;
};

template <typename T, int64_t CACHE_NUM>
class ObPointerCache {
public:
  ObPointerCache()
  {
    reset();
  }
  ~ObPointerCache()
  {
    destroy();
  }
  int init()
  {
    reset();
    return common::OB_SUCCESS;
  }
  void reset()
  {
    for (int64_t i = 0; i < CACHE_NUM; i++) {
      cache_[i] = NULL;
    }
  }
  void destroy()
  {
    reset();
  }
  T* get(const uint64_t hv)
  {
    return ATOMIC_LOAD(&(cache_[hv % CACHE_NUM]));
  }
  int set(const uint64_t hv, T* p)
  {
    ATOMIC_SET(&(cache_[hv % CACHE_NUM]), p);
    return common::OB_SUCCESS;
  }
  int remove(const uint64_t hv)
  {
    ATOMIC_SET(&(cache_[hv % CACHE_NUM]), NULL);
    return common::OB_SUCCESS;
  }

private:
  T* cache_[CACHE_NUM];
};

class ObTransCtxMgrImpl {
  enum { CACHE_NUM = 17313, CONTEXT_MAP_COUNT = 1 << 6 };

public:
  ObTransCtxMgrImpl() : ctx_map_(nullptr), ts_mgr_(nullptr)
  {
    reset();
  }
  ~ObTransCtxMgrImpl()
  {
    destroy();
  }
  int init(const int64_t ctx_type, ObITsMgr* ts_mgr, storage::ObPartitionService* partition_service);
  void reset();
  void destroy();

protected:
  int add_partition(const common::ObPartitionKey& partition);
  int block_partition(const common::ObPartitionKey& partition, bool& is_all_trans_clear);
  int stop_partition(const common::ObPartitionKey& partition, const bool graceful);
  int wait_partition(const common::ObPartitionKey& partition);
  int remove_partition(const common::ObPartitionKey& partition);
  int stop_all_partition();
  int wait_all_partition();
  int remove_all_partition();
  int kill_all_partition();
  int clear_all_ctx(const common::ObPartitionKey& partition);
  int kill_all_trans(const common::ObPartitionKey& partition, const KillTransArg& arg, bool& is_all_trans_clear);
  int calculate_trans_cost(const ObTransID& tid, uint64_t& cost);
  int wait_all_trans_clear(const common::ObPartitionKey& partition);
  int check_all_trans_in_trans_table_state(const common::ObPartitionKey& partition);
  int wait_1pc_trx_end(const common::ObPartitionKey& partition);

  int print_all_partition_ctx();
  int leader_revoke(const common::ObPartitionKey& partition, const bool first_check);
  int leader_takeover(const common::ObPartitionKey& partition, const int64_t checkpoint);
  int leader_active(const common::ObPartitionKey& partition, const storage::LeaderActiveArg& arg);

public:
  template <typename Fn>
  int foreach_partition(Fn& fn)
  {
    return partition_ctx_map_.for_each(fn);
  }
  template <typename Fn>
  int remove_if(Fn& fn)
  {
    return partition_ctx_map_.remove_if(fn);
  }
  ObPartitionTransCtxMgr* get_partition_trans_ctx_mgr(const common::ObPartitionKey& partition);
  int get_partition_trans_ctx_mgr_with_ref(const ObPartitionKey& partition, ObTransStateTableGuard& guard);
  int revert_partition_trans_ctx_mgr_with_ref(ObPartitionTransCtxMgr* mgr);

protected:
  int64_t ctx_type_;
  PartitionCtxMap partition_ctx_map_;
  ObPointerCache<ObPartitionTransCtxMgr, CACHE_NUM> mgr_cache_;
  CtxMap* ctx_map_;
  int64_t access_count_;
  int64_t hit_count_;
  ObITsMgr* ts_mgr_;
  int64_t partition_alloc_cnt_;
  int64_t partition_del_cnt_;

protected:
  storage::ObPartitionService* partition_service_;
};

class ObCoordTransCtxMgr : public ObITransCtxMgr, public ObTransCtxMgrImpl {
public:
  ObCoordTransCtxMgr()
  {
    reset();
  }
  ~ObCoordTransCtxMgr()
  {
    destroy();
  }
  int init(ObITsMgr* ts_mgr, storage::ObPartitionService* partition_service);
  int start();
  int stop();
  int wait();
  void destroy();
  void reset();

public:
  int get_trans_ctx(const common::ObPartitionKey& partition, const ObTransID& trans_id, const bool for_replay,
      const bool is_readonly, bool& alloc, ObTransCtx*& ctx);
  int add_partition(const common::ObPartitionKey& partition);
  int block_partition(const common::ObPartitionKey& partition, bool& is_all_trans_clear);
  int remove_partition(const common::ObPartitionKey& partition, const bool graceful);

  int revert_trans_ctx(ObTransCtx* ctx);
  int leader_revoke(const common::ObPartitionKey& partition);
  int leader_takeover(const common::ObPartitionKey& partition, const int64_t checkpoint);
  int leader_active(const common::ObPartitionKey& partition, const storage::LeaderActiveArg& arg);
  int clear_all_ctx(const common::ObPartitionKey& partition);
  int kill_all_trans(const common::ObPartitionKey& partition, const KillTransArg& arg, bool& is_all_trans_clear);
  int wait_all_trans_clear(const common::ObPartitionKey& partition);
  int wait_1pc_trx_end(const common::ObPartitionKey& partition);

private:
  DISALLOW_COPY_AND_ASSIGN(ObCoordTransCtxMgr);

private:
  bool is_inited_;
  bool is_running_;
  mutable common::DRWLock rwlock_;
};

class ObPartTransCtxMgr : public ObITransCtxMgr, public ObTransCtxMgrImpl {
public:
  ObPartTransCtxMgr()
  {
    reset();
  }
  ~ObPartTransCtxMgr()
  {
    destroy();
  }
  int init(const int ctx_type, ObITsMgr* ts_mgr, storage::ObPartitionService* partition_service);
  int start();
  int stop();
  void destroy();
  int wait();
  void reset();

public:
  int get_trans_ctx(const common::ObPartitionKey& partition, const ObTransID& trans_id, const bool for_replay,
      const bool is_readonly, bool& alloc, ObTransCtx*& ctx);
  int get_trans_ctx(const common::ObPartitionKey& partition, const ObTransID& trans_id, const bool for_replay,
      const bool is_readonly, const bool is_bounded_staleness_read, const bool need_completed_dirty_txn, bool& alloc,
      ObTransCtx*& ctx);

  int add_partition(const common::ObPartitionKey& partition);
  int block_partition(const common::ObPartitionKey& partition, bool& is_all_trans_clear);
  int remove_partition(const common::ObPartitionKey& partition, const bool graceful);

  int revert_trans_ctx(ObTransCtx* ctx);
  int leader_revoke(const common::ObPartitionKey& partition);
  int leader_takeover(const common::ObPartitionKey& partition, const int64_t checkpoint);
  int leader_active(const common::ObPartitionKey& partition, const storage::LeaderActiveArg& arg);
  int clear_all_ctx(const common::ObPartitionKey& partition);
  int kill_all_trans(const common::ObPartitionKey& partition, const KillTransArg& arg, bool& is_all_trans_clear);
  int calculate_trans_cost(const ObTransID& tid, uint64_t& cost);
  int wait_all_trans_clear(const common::ObPartitionKey& partition);
  int check_all_trans_in_trans_table_state(const common::ObPartitionKey& partition);
  int wait_1pc_trx_end(const common::ObPartitionKey& partition);
  int check_schema_version_elapsed(
      const ObPartitionKey& partition, const int64_t schema_version, const int64_t refreshed_schema_ts);
  int check_ctx_create_timestamp_elapsed(const ObPartitionKey& partition, const int64_t ts);
  int check_trans_partition_leader_unsafe(const ObPartitionKey& partition, bool& is_leader);
  int get_cached_pg_guard(const ObPartitionKey& partition, storage::ObIPartitionGroupGuard*& pg_guard);

  int get_memtable_ctx(const common::ObPartitionKey& partition, const ObTransID& trans_id,
      const bool is_bounded_staleness_read, memtable::ObMemtableCtx*& mt_ctx);
  int revert_memtable_ctx(memtable::ObIMemtableCtx*& mt_ctx);
  // get the min prepare version of trans module in the specified partitiohn of current observer
  int get_min_uncommit_prepare_version(const ObPartitionKey& partition, int64_t& min_prepare_version);
  // get the min log ID of transactions in the specified partition
  int get_min_uncommit_log(const ObPartitionKey& pkey, uint64_t& min_uncommit_log_id, int64_t& min_uncommit_log_ts);
  int get_min_prepare_version(const ObPartitionKey& partition, const int64_t log_ts, int64_t& min_prepare_version);
  int gc_trans_result_info(const ObPartitionKey& pkey, const int64_t checkpoint_ts);
  int checkpoint(
      const ObPartitionKey& pkey, const int64_t checkpoint_base_ts, storage::ObPartitionLoopWorker* lp_worker);

  // get partition iterator
  int iterate_partition(ObPartitionIterator& partition_iter);
  int iterate_partition(ObELRStatSummary& elr_stat);
  int iterate_partition_mgr_stat(ObTransPartitionMgrStatIterator& partition_mgr_stat_iter, const ObAddr& addr);
  // get transaction stat iterator by partition
  int iterate_trans_stat(const common::ObPartitionKey& partition, ObTransStatIterator& trans_stat_iter);
  int print_all_trans_ctx(const common::ObPartitionKey& partition);
  // get transaction lock stat iterator by partition
  int iterate_trans_lock_stat(const common::ObPartitionKey& partition, ObTransLockStatIterator& trans_lock_stat_iter);
  int iterate_trans_result_info_in_TRIM(const common::ObPartitionKey& partition, ObTransResultInfoStatIterator& iter);
  int update_max_trans_version(const common::ObPartitionKey& partition, const int64_t trans_version);
  int get_max_trans_version(const common::ObPartitionKey& partition, int64_t& max_trans_version);
  int relocate_data(const ObPartitionKey& partition, memtable::ObIMemtable* memtable);
  int prepare_changing_leader(
      const ObPartitionKey& partition, const ObAddr& proposal_leader, const int64_t round, const int64_t cnt);
  int check_scheduler_status(const ObPartitionKey& partition);
  int get_state_in_TRIM(const common::ObPartitionKey& partition, const ObTransID& trans_id, int& state);
  int replay_start_working_log(const ObPartitionKey& pkey, const int64_t timestamp, const uint64_t log_id);
  int get_partition_audit_info(const ObPartitionKey& pkey, ObPartitionAuditInfo& info);
  int set_partition_audit_base_row_count(const ObPartitionKey& pkey, const int64_t count);
  int handle_dup_lease_request(const ObPartitionKey& pkey, const ObDupTableLeaseRequestMsg& request);
  int handle_dup_redo_log_sync_response(const ObPartitionKey& pkey, const ObRedoLogSyncResponseMsg& msg);
  int handle_dup_lease_response(const ObPartitionKey& pkey, const ObDupTableLeaseResponseMsg& msg);
  int handle_dup_redo_log_sync_request(
      const ObPartitionKey& pkey, const ObRedoLogSyncRequestMsg& msg, storage::ObPartitionService* partition_service);
  int send_dup_table_lease_request_msg(const ObPartitionKey& pkey, const uint64_t min_replay_log_id,
      bool& need_renew_lease, bool& need_refresh_location);
  int print_dup_table_partition_mgr(const ObPartitionKey& pkey);
  int is_dup_table_partition_serving(const ObPartitionKey& pkey, const bool is_master, bool& is_serving);
  int iterate_duplicate_partition_stat(const common::ObPartitionKey& partition, ObDuplicatePartitionStatIterator& iter);
  int check_and_inc_read_only_trx_count(const common::ObPartitionKey& pkey);
  int check_and_dec_read_only_trx_count(const common::ObPartitionKey& pkey);
  int remove_callback_for_uncommited_txn(const ObPartitionKey& partition, memtable::ObMemtable* mt);
  int remove_mem_ctx_for_trans_ctx(const ObPartitionKey& partition, memtable::ObMemtable* mt);

  // Transaction status table implementation, mark the corresponding dirty transactiohns of dump memtable
  int mark_dirty_trans(const common::ObPartitionKey& pkey, const memtable::ObMemtable* const frozen_memtable,
      const memtable::ObMemtable* const active_memtable, int64_t& cb_cnt, int64_t& applied_log_ts);

  int get_applied_log_ts(const common::ObPartitionKey& pkey, int64_t& applied_log_ts);

  int get_max_trans_version_before_given_log_ts(
      const ObPartitionKey& pkey, const int64_t log_ts, int64_t& max_trans_version, bool& is_all_rollback_trans);
  int clear_unused_trans_status(const ObPartitionKey& pkey, const int64_t max_cleanout_log_id);
  int has_terminated_trx_in_given_log_ts_range(
      const ObPartitionKey& pkey, const int64_t start_log_ts, const int64_t end_log_ts, bool& has_terminated_trx);
  int set_last_restore_log_id(const common::ObPartitionKey& pkey, const uint64_t last_restore_log_id);
  int set_restore_snapshot_version(const common::ObPartitionKey& pkey, const int64_t restore_snapshot_version);
  int update_restore_replay_info(
      const ObPartitionKey& partition, const int64_t restore_snapshot_version, const uint64_t last_restore_log_id);
  int submit_log_for_split(const common::ObPartitionKey& pkey, bool& log_finished);
  int copy_trans_table(
      ObTransService* txs, const common::ObPartitionKey& pkey, const ObIArray<ObPartitionKey>& dest_array);
  int get_active_read_write_count(const ObPartitionKey& partition, int64_t& count);
  int init_dup_table_mgr(const ObPartitionKey& partition);

private:
  DISALLOW_COPY_AND_ASSIGN(ObPartTransCtxMgr);

private:
  bool is_inited_;
  bool is_running_;
  mutable common::DRWLock rwlock_;
};

// scheduler need not attach to partition. we assign a magic partition to scheduler
// just for ObTransCtxMgr interface implement
#define SCHE_PARTITION_ID common::ObPartitionKey(1000, 1000, 1000)

}  // namespace transaction
}  // namespace oceanbase

#endif  // OCEANBASE_TRANSACTION_OB_TRANS_CTX_MGR_
