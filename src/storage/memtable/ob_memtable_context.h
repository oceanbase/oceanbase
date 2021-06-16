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

#ifndef OCEANBASE_MEMTABLE_OB_MEMTABLE_CONTEXT_
#define OCEANBASE_MEMTABLE_OB_MEMTABLE_CONTEXT_

#include "lib/allocator/ob_fifo_allocator.h"
#include "lib/checksum/ob_crc64.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/lock/ob_small_spin_lock.h"
#include "share/ob_define.h"
#include "storage/memtable/ob_memtable_interface.h"
#include "storage/memtable/ob_memtable_mutator.h"
#include "storage/memtable/ob_redo_log_generator.h"
#include "storage/memtable/mvcc/ob_mvcc_trans_ctx.h"
#include "storage/memtable/mvcc/ob_crtp_util.h"
#include "storage/memtable/ob_memtable_array.h"

#include "storage/transaction/ob_trans_define.h"

namespace oceanbase {
namespace transaction {
class ObThreadLocalTransCtx;
}
namespace memtable {

class MemtableCtxStat {
public:
  MemtableCtxStat() : wlock_retry_(0), tsc_retry_(0)
  {}
  ~MemtableCtxStat()
  {}
  void reset()
  {
    wlock_retry_ = 0;
    tsc_retry_ = 0;
  }
  void on_wlock_retry()
  {
    (void)ATOMIC_FAA(&wlock_retry_, 1);
  }
  void on_tsc_retry()
  {
    (void)ATOMIC_FAA(&tsc_retry_, 1);
  }
  int32_t get_wlock_retry_count()
  {
    return ATOMIC_LOAD(&wlock_retry_);
  }
  int32_t get_tsc_retry_count()
  {
    return ATOMIC_LOAD(&tsc_retry_);
  }

private:
  int32_t wlock_retry_;
  int32_t tsc_retry_;
};

// 1. When fill redo log, if there is a big row, the meta info should record
// the relative position of the current log in the big row, that is: BIG_ROW_START,
// BIG_ROW_MID OR BIG_ROW_END
// 2. For a normal row, the flag is NORMAL_ROW
class ObTransRowFlag {
public:
  static const uint8_t NORMAL_ROW = 0;
  static const uint8_t BIG_ROW_START = 1;
  static const uint8_t BIG_ROW_MID = 2;
  static const uint8_t BIG_ROW_END = 3;
  static const uint8_t MAX = 4;
  static const uint8_t ENCRYPT = (1 << 3);

public:
  static bool is_valid_row_flag(const uint8_t row_flag)
  {
    const uint8_t real_flag = row_flag & (~ENCRYPT);
    return real_flag < MAX;
  }
  static bool is_row_start(const uint8_t row_flag)
  {
    const uint8_t real_flag = row_flag & (~ENCRYPT);
    return NORMAL_ROW == real_flag || BIG_ROW_START == real_flag;
  }
  static bool is_normal_row(const uint8_t row_flag)
  {
    const uint8_t real_flag = row_flag & (~ENCRYPT);
    return real_flag == NORMAL_ROW;
  }
  static bool is_big_row_start(const uint8_t row_flag)
  {
    const uint8_t real_flag = row_flag & (~ENCRYPT);
    return real_flag == BIG_ROW_START;
  }
  static bool is_big_row_mid(const uint8_t row_flag)
  {
    const uint8_t real_flag = row_flag & (~ENCRYPT);
    return real_flag == BIG_ROW_MID;
  }
  static bool is_big_row_end(const uint8_t row_flag)
  {
    const uint8_t real_flag = row_flag & (~ENCRYPT);
    return real_flag == BIG_ROW_END;
  }
  static bool is_encrypted(const uint8_t row_flag)
  {
    return row_flag & ENCRYPT;
  }
  static void add_encrypt_flag(uint8_t& row_flag)
  {
    row_flag |= ENCRYPT;
  }
  static void remove_encrypt_flag(uint8_t& row_flag)
  {
    row_flag &= (~ENCRYPT);
  }
};

class ObQueryAllocator : public common::ObIAllocator {
public:
  explicit ObQueryAllocator() : alloc_count_(0), free_count_(0), alloc_size_(0), is_inited_(false)
  {}
  ~ObQueryAllocator()
  {
    if (OB_UNLIKELY(ATOMIC_LOAD(&free_count_) != ATOMIC_LOAD(&alloc_count_))) {
      TRANS_LOG(ERROR, "query allocator leak found", K(alloc_count_), K(free_count_), K(alloc_size_));
    }
    ATOMIC_STORE(&is_inited_, false);
  }
  int init(common::ObIAllocator* allocator, const uint64_t tenant_id)
  {
    int ret = OB_SUCCESS;
    ObMemAttr attr(tenant_id, ObModIds::OB_QUERY_ALLOCATOR);
    if (OB_UNLIKELY(free_count_ != alloc_count_)) {
      TRANS_LOG(ERROR, "query allocator leak found", K(alloc_count_), K(free_count_), K(alloc_size_));
    }
    if (IS_NOT_INIT) {
      if (OB_FAIL(allocator_.init(
              allocator, common::OB_MALLOC_NORMAL_BLOCK_SIZE, attr, 0, common::OB_MALLOC_NORMAL_BLOCK_SIZE))) {
        TRANS_LOG(ERROR, "query allocator init failed", K(ret), K(lbt()), K(tenant_id));
      } else {
        ATOMIC_STORE(&is_inited_, true);
      }
    }
    if (OB_SUCC(ret)) {
      allocator_.set_attr(attr);
    }
    ATOMIC_STORE(&alloc_count_, 0);
    ATOMIC_STORE(&free_count_, 0);
    ATOMIC_STORE(&alloc_size_, 0);
    return ret;
  }
  void reset()
  {
    if (OB_UNLIKELY(ATOMIC_LOAD(&free_count_) != ATOMIC_LOAD(&alloc_count_))) {
      TRANS_LOG(ERROR, "query allocator leak found", K(alloc_count_), K(free_count_), K(alloc_size_));
    }
    allocator_.reset();
    ATOMIC_STORE(&alloc_count_, 0);
    ATOMIC_STORE(&free_count_, 0);
    ATOMIC_STORE(&alloc_size_, 0);
  }
  void* alloc(const int64_t size)
  {
    void* ret = nullptr;
    if (OB_ISNULL(ret = allocator_.alloc(size))) {
      TRANS_LOG(ERROR, "query alloc failed", K(alloc_count_), K(free_count_), K(alloc_size_), K(size));
    } else {
      ATOMIC_INC(&alloc_count_);
      ATOMIC_FAA(&alloc_size_, size);
    }
    return ret;
  }
  void free(void* ptr)
  {
    if (OB_ISNULL(ptr)) {
      // do nothing
    } else {
      ATOMIC_INC(&free_count_);
      allocator_.free(ptr);
    }
  }

private:
  ObFIFOAllocator allocator_;
  int64_t alloc_count_;
  int64_t free_count_;
  int64_t alloc_size_;
  bool is_inited_;
};

// The speciaal allocator for ObMemtableCtx, used to allocate callback.
// The page size is 8K, support concurrency, but at a poor performance.
class ObMemtableCtxCbAllocator : public common::ObIAllocator {
public:
  explicit ObMemtableCtxCbAllocator() : alloc_count_(0), free_count_(0), alloc_size_(0), is_inited_(false)
  {}
  ~ObMemtableCtxCbAllocator()
  {
    if (OB_UNLIKELY(ATOMIC_LOAD(&free_count_) != ATOMIC_LOAD(&alloc_count_))) {
      TRANS_LOG(ERROR, "callback memory leak found", K(alloc_count_), K(free_count_), K(alloc_size_));
    }
    ATOMIC_STORE(&is_inited_, false);
  }
  // FIFOAllocator doesn't support double init, even after reset, so is_inited_ is handled specially here.
  int init(common::ObIAllocator* allocator, const uint64_t tenant_id)
  {
    int ret = OB_SUCCESS;
    ObMemAttr attr(tenant_id, ObModIds::OB_MEMTABLE_CALLBACK);
    if (OB_UNLIKELY(free_count_ != alloc_count_)) {
      TRANS_LOG(ERROR, "callback memory leak found", K(alloc_count_), K(free_count_), K(alloc_size_));
    }
    if (IS_NOT_INIT) {
      if (OB_FAIL(allocator_.init(
              allocator, common::OB_MALLOC_NORMAL_BLOCK_SIZE, attr, 0, common::OB_MALLOC_NORMAL_BLOCK_SIZE))) {
        TRANS_LOG(ERROR, "callback allocator init failed", K(ret), K(lbt()), K(tenant_id));
      } else {
        ATOMIC_STORE(&is_inited_, true);
      }
    }
    if (OB_SUCC(ret)) {
      allocator_.set_attr(attr);
    }
    ATOMIC_STORE(&alloc_count_, 0);
    ATOMIC_STORE(&free_count_, 0);
    ATOMIC_STORE(&alloc_size_, 0);
    return ret;
  }
  void reset()
  {
    if (OB_UNLIKELY(free_count_ != alloc_count_)) {
      TRANS_LOG(ERROR, "callback memory leak found", K(alloc_count_), K(free_count_), K(alloc_size_));
    }
    allocator_.reset();
    ATOMIC_STORE(&alloc_count_, 0);
    ATOMIC_STORE(&free_count_, 0);
    ATOMIC_STORE(&alloc_size_, 0);
  }
  void* alloc(const int64_t size)
  {
    void* ret = nullptr;
    if (OB_ISNULL(ret = allocator_.alloc(size))) {
      TRANS_LOG(ERROR, "callback memory failed", K(alloc_count_), K(free_count_), K(alloc_size_), K(size));
    } else {
      ATOMIC_INC(&alloc_count_);
      ATOMIC_FAA(&alloc_size_, size);
    }
    return ret;
  }
  void free(void* ptr)
  {
    if (OB_ISNULL(ptr)) {
      // do nothing
    } else {
      ATOMIC_INC(&free_count_);
      allocator_.free(ptr);
    }
  }

private:
  ObFIFOAllocator allocator_;
  int64_t alloc_count_;
  int64_t free_count_;
  int64_t alloc_size_;
  // used to record the init condition of FIFO allocator
  bool is_inited_;
};

class ObMemtable;
typedef ObMemtableCtxFactory::IDMap MemtableIDMap;
class ObMemtableCtx : public ObIMemtableCtx {
  static const int64_t SLOW_QUERY_THRESHOULD = 500 * 1000;
  static const int64_t LOG_CONFLICT_INTERVAL = 3 * 1000 * 1000;

public:
  static uint32_t UID_FOR_PURGE;

public:
  ObMemtableCtx();
  ObMemtableCtx(MemtableIDMap& ctx_map, common::ObIAllocator& allocator);
  virtual ~ObMemtableCtx();

  virtual void reset() override;

public:
  int init(const uint64_t tenant_id, MemtableIDMap& ctx_map, common::ObIAllocator& malloc_allocator);
  virtual int add_crc(const void* key, ObMvccRow* value, ObMvccTransNode* tnode, const int64_t data_size) override;
  virtual int add_crc2(ObMvccRow* value, ObMvccTransNode* tnode, int64_t data_size) override;
  virtual int add_crc4(ObMvccRow* value, ObMvccTransNode* tnode, int64_t data_size, const int64_t log_ts) override;
  virtual void* callback_alloc(const int64_t size) override;
  virtual void callback_free(void* cb) override;
  virtual void* arena_alloc(const int64_t size) override;
  virtual common::ObIAllocator& get_query_allocator() override;
  virtual int wait_trans_version(const uint32_t descriptor, const int64_t version) override;
  virtual int wait_trans_version_v2(
      const uint32_t descriptor, const int64_t version, const ObMemtableKey* key, const ObMvccRow& row) override;
  virtual int try_elr_when_lock_for_write(
      const uint32_t descriptor, const int64_t version, const ObMemtableKey* key, const ObMvccRow& row) override;
  virtual void inc_lock_for_read_retry_count() override;
  virtual int row_compact(ObMvccRow* value, const int64_t snapshot_version) override;
  virtual const char* log_conflict_ctx(const uint32_t descriptor) override;
  virtual int read_lock_yield() override;
  virtual int write_lock_yield() override;

  virtual int after_link_trans_node(const void* key, ObMvccRow* value) override;
  virtual bool has_redo_log() const override;
  virtual void update_max_durable_sql_no(const int64_t sql_no) override;

public:
  int set_replay_host(ObMemtable* host, const bool for_replay);
  int set_leader_host(ObMemtable* host, const bool for_replay);
  int check_memstore_count(int64_t& count);
  virtual void set_read_only() override;
  virtual void inc_ref() override;
  virtual void dec_ref() override;
  virtual int write_auth() override;
  virtual int write_done() override;
  virtual int trans_begin() override;
  virtual int sub_trans_begin(const int64_t snapshot, const int64_t abs_expired_time,
      const bool is_safe_snapshot = false, const int64_t trx_lock_timeout = -1) override;
  virtual int sub_trans_end(const bool commit) override;
  virtual int sub_stmt_begin() override;
  virtual int sub_stmt_end(const bool commit) override;
  virtual int fast_select_trans_begin(
      const int64_t snapshot, const int64_t abs_expired_time, const int64_t trx_lock_timeout) override;
  virtual uint64_t calc_checksum(const int64_t trans_version) override;
  virtual uint64_t calc_checksum2(const int64_t trans_version) override;
  virtual uint64_t calc_checksum3();
  virtual uint64_t calc_checksum4();
  virtual int trans_end(const bool commit, const int64_t trans_version) override;
  virtual int trans_clear() override;
  virtual int elr_trans_preparing() override;
  virtual int trans_kill() override;
  // fake kill the trans table even when the transaction may already be committed
  // only used to set the already committed transaction to be aborted
  virtual int fake_kill();
  virtual int trans_publish() override;
  virtual int trans_replay_begin() override;
  virtual int trans_replay_end(const bool commit, const int64_t trans_version, const uint64_t checksum = 0) override;
  // method called when leader takeover
  virtual int replay_to_commit() override;
  // method called when leader revoke
  virtual int commit_to_replay() override;
  virtual int trans_link();
  virtual int get_trans_status() const override;
  virtual int fill_redo_log(char* buf, const int64_t buf_len, int64_t& buf_pos) override;
  virtual int undo_fill_redo_log() override;  // undo the function fill_redo_log(..)
  // the function apply the side effect of dirty txn and return whether
  // remaining pending callbacks.
  // NB: the fact whether there remains pending callbacks currently is only used
  // for continuing logging when minor freeze
  void sync_log_succ(const int64_t log_id, const int64_t log_ts);
  void sync_log_succ(const int64_t log_id, const int64_t log_ts, bool& has_pending_cb);
  bool is_slow_query() const;

  void set_handle_start_time(const int64_t start_time)
  {
    handle_start_time_ = start_time;
  }
  int64_t get_handle_start_time()
  {
    return handle_start_time_;
  }
  virtual void set_trans_ctx(transaction::ObTransCtx* ctx) override;
  virtual transaction::ObTransCtx* get_trans_ctx() override
  {
    return ctx_;
  }
  virtual bool is_multi_version_range_valid() const override
  {
    return multi_version_range_.is_valid();
  }
  virtual const ObVersionRange& get_multi_version_range() const override
  {
    return multi_version_range_;
  }
  virtual int stmt_data_relocate(const int relocate_data_type, ObMemtable* memtable, bool& relocated) override;
  virtual void inc_relocate_cnt() override
  {
    relocate_cnt_++;
  }
  virtual void inc_truncate_cnt() override
  {
    truncate_cnt_++;
  }
  virtual int64_t get_relocate_cnt() override
  {
    return relocate_cnt_;
  }
  virtual int audit_partition(const enum transaction::ObPartitionAuditOperator op, const int64_t count) override;
  int get_memtable_key_arr(transaction::ObMemtableKeyArray& memtable_key_arr);
  uint64_t get_lock_for_read_retry_count() const
  {
    return lock_for_read_retry_count_;
  }
  virtual void add_trans_mem_total_size(const int64_t size) override;
  virtual void set_contain_hotspot_row() override;
  MemtableIDMap* get_ctx_map()
  {
    return ctx_map_;
  }
  void set_need_print_trace_log();
  int64_t get_ref() const
  {
    return ATOMIC_LOAD(&ref_);
  }
  uint64_t get_tenant_id() const override;
  bool is_can_elr() const override;
  bool is_elr_prepared() const override;
  ObMemtableMutatorIterator* get_memtable_mutator_iter()
  {
    return mutator_iter_;
  }
  ObMemtableMutatorIterator* alloc_memtable_mutator_iter();
  int set_memtable_for_cur_log(ObIMemtable* memtable) override;
  ObIMemtable* get_memtable_for_cur_log() override
  {
    return memtable_for_cur_log_;
  }
  void clear_memtable_for_cur_log() override
  {
    if (NULL != memtable_for_cur_log_) {
      memtable_for_cur_log_->dec_pending_lob_count();
      memtable_for_cur_log_ = NULL;
      lob_start_log_ts_ = 0;
    }
  }
  int set_memtable_for_batch_commit(ObMemtable* memtable);
  int set_memtable_for_elr(ObMemtable* memtable);
  bool has_read_elr_data() const override
  {
    return read_elr_data_;
  }
  int remove_callback_for_uncommited_txn(memtable::ObMemtable* mt, int64_t& cnt);
  int rollback_to(const int32_t sql_no, const bool is_replay, const bool need_write_log,
      const int64_t max_durable_log_ts, bool& has_calc_checksum, uint64_t& checksum, int64_t& checksum_log_ts);
  int truncate_to(const int32_t sql_no);
  bool is_all_redo_submitted();
  bool is_for_replay() const
  {
    return trans_mgr_.is_for_replay();
  }
  void half_stmt_commit();

  int64_t get_trans_mem_total_size() const
  {
    return trans_mem_total_size_;
  }
  void add_lock_for_read_elapse(const int64_t elapse) override
  {
    lock_for_read_elapse_ += elapse;
  }
  int64_t get_lock_for_read_elapse() const override
  {
    return lock_for_read_elapse_;
  }
  // mark the corresponding callback of the dirty data
  int mark_frozen_data(
      const ObMemtable* const frozen_memtable, const ObMemtable* const active_memtable, bool& marked, int64_t& cb_cnt);
  int remove_mem_ctx_for_trans_ctx(ObMemtable* mt);
  int get_cur_max_sql_no(int64_t& sql_no);
  int update_and_get_trans_table_with_minor_freeze(const int64_t log_ts, uint64_t& checksum);
  int update_batch_checksum(const uint64_t checksum, const int64_t log_ts);
  int64_t get_checksum_log_ts() const
  {
    return checksum_log_ts_;
  };
  int64_t get_pending_log_size()
  {
    return trans_mgr_.get_pending_log_size();
  }
  int64_t get_flushed_log_size()
  {
    return trans_mgr_.get_flushed_log_size();
  }
  bool pending_log_size_too_large();
  void dec_pending_batch_commit_count();
  bool is_sql_need_be_submit(const int64_t sql_no);
  uint64_t get_callback_count() const
  {
    return trans_mgr_.count();
  }
  void dec_pending_elr_count();

public:
  void on_tsc_retry(const ObMemtableKey& key) override;
  void on_wlock_retry(const ObMemtableKey& key, const char* conflict_ctx) override;
  int insert_prev_trans(const uint32_t ctx_id) override;
  virtual int64_t to_string(char* buf, const int64_t buf_len) const override;
  void set_thread_local_ctx(transaction::ObThreadLocalTransCtx* thread_local_ctx)
  {
    thread_local_ctx_ = thread_local_ctx;
  }
  transaction::ObThreadLocalTransCtx* get_thread_local_ctx() const
  {
    return thread_local_ctx_;
  }
  transaction::ObTransTableStatusType get_trans_table_status() const
  {
    return trans_table_status_;
  }
  void set_trans_table_status(transaction::ObTransTableStatusType trans_table_status)
  {
    trans_table_status_ = trans_table_status;
  }

  void set_self_alloc_ctx(const bool is_self_alloc_ctx)
  {
    is_self_alloc_ctx_ = is_self_alloc_ctx;
  }
  bool is_self_alloc_ctx() const
  {
    return is_self_alloc_ctx_;
  }
  transaction::ObTransStateTableGuard* get_trans_table_guard() override
  {
    return &trans_table_guard_;
  }
  // mainly used by revert ref
  void reset_trans_table_guard();

private:
  int set_host_(ObMemtable* host, const bool for_replay);
  int do_trans_end(const bool commit, const int64_t trans_version, const int end_code);
  int trans_pending();
  static int64_t get_us()
  {
    return ::oceanbase::common::ObTimeUtility::current_time();
  }
  ObMemtable* get_active_mt() const
  {
    return memtable_arr_wrap_.get_active_mt();
  }
  int check_trans_elr_prepared_(transaction::ObTransCtx* trans_ctx, bool& elr_prepared, int64_t& elr_commit_version);
  int insert_prev_trans_(const uint32_t ctx_id, transaction::ObTransCtx* prev_trans_ctx);
  int audit_partition_cache_(const enum transaction::ObPartitionAuditOperator op, const int32_t count);
  int flush_audit_partition_cache_(bool commit);
  void trans_mgr_sub_trans_end_(bool commit);
  void trans_mgr_sub_trans_begin_();
  void set_read_elr_data(const bool read_elr_data)
  {
    read_elr_data_ = read_elr_data;
  }
  int reset_log_generator_();
  void inc_pending_log_size(const int64_t size)
  {
    trans_mgr_.inc_pending_log_size(size);
  }
  void inc_flushed_log_size(const int64_t size)
  {
    trans_mgr_.inc_flushed_log_size(size);
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObMemtableCtx);
  common::ObByteLock lock_;
  common::ObByteLock stmt_relocate_lock_;
  int end_code_;
  int64_t ref_;
  MemtableIDMap* ctx_map_;
  // allocate memory for callback when query executing
  ObQueryAllocator query_allocator_;
  ObMemtableCtxCbAllocator ctx_cb_allocator_;
  ObRedoLogGenerator log_gen_;
  /*
   * transaction checksum calculation
   * A checksum would be calculated when a transaction commits to ensure data consistency.
   * There are 3 conditions:
   * 1. When commits a transaction, iterate all the callback and calculate the checksum;
   * 2. When memtable dump occurs, the callbacks before last_replay_log_id would be traversed
   *    to calculate checksum;
   * 3. When rollback, the callbacks before max_durable_log_ts should be calculated, to ensure
   *    the checksum calculated due to memtable dump on leader also exists on follower.
   *
   * checksum_log_ts_ is the log timestamp that is currently synchronizing, bach_checksum_
   * indicates the data which hasn't been calculated, checksum_ is is the currently
   * calculated checksum(checksum supports increment calculation)
   *
   * Notice, apart from 1, the other two calculation should increment
   * checksum_log_ts_ by 1 to avoid duplicate calculation.
   */
  common::ObBatchChecksum batch_checksum_;
  int64_t checksum_log_ts_;
  uint64_t checksum_;
  int64_t handle_start_time_;
  MemtableCtxStat mtstat_;
  ObTimeInterval log_conflict_interval_;
  transaction::ObTransCtx* ctx_;
  ObMemtableMutatorIterator* mutator_iter_;
  ModuleArena arena_;
  // Indicates memtable where the first redo log of a big row has replayed,
  // the following redo log of the same big row need to be replayed at this memtable
  ObIMemtable* memtable_for_cur_log_;
  ObMemtable* memtable_for_batch_commit_;
  ObMemtable* memtable_for_elr_;
  transaction::ObPartitionAuditInfoCache partition_audit_info_cache_;
  int64_t relocate_cnt_;
  int64_t truncate_cnt_;
  // the retry count of lock for read
  uint64_t lock_for_read_retry_count_;
  // Time cost of lock for read
  int64_t lock_for_read_elapse_;
  ObMemtableArrWrap memtable_arr_wrap_;
  int64_t trans_mem_total_size_;
  // Thread local variable which points to trans ctx
  transaction::ObThreadLocalTransCtx* thread_local_ctx_;
  int64_t callback_mem_used_;
  int64_t callback_alloc_count_;
  int64_t callback_free_count_;
  transaction::ObTransTableStatusType trans_table_status_;
  bool is_read_only_;
  bool is_master_;
  bool data_relocated_;
  // Used to indicate whether elr data is read. When a statement executes,
  // if one row involves elr data, set it to true, and the row can't be purged
  bool read_elr_data_;
  bool is_self_alloc_ctx_;
  transaction::ObTransStateTableGuard trans_table_guard_;
  bool is_inited_;
};

}  // namespace memtable
}  // namespace oceanbase

#endif  // OCEANBASE_MEMTABLE_OB_MEMTABLE_CONTEXT_
