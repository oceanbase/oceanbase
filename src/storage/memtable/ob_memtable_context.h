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
#include "lib/list/ob_dlist.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/lock/ob_small_spin_lock.h"
#include "lib/utility/ob_macro_utils.h"
#include "ob_clock_generator.h"
#include "share/ob_define.h"
#include "storage/ob_memtable_ctx_obj_pool.h"
#include "storage/memtable/ob_memtable_interface.h"
#include "storage/memtable/ob_memtable_mutator.h"
#include "storage/memtable/ob_redo_log_generator.h"
#include "storage/memtable/mvcc/ob_mvcc_trans_ctx.h"
#include "storage/memtable/mvcc/ob_crtp_util.h"
#include "storage/tx/ob_trans_define.h"
#include "storage/tablelock/ob_mem_ctx_table_lock.h"
#include "storage/tx_table/ob_tx_table.h"

namespace oceanbase
{
namespace transaction
{
class ObThreadLocalTransCtx;
class ObDefensiveCheckMgr;
namespace tablelock
{
struct ObTableLockInfo;
}
}

namespace memtable
{
class ObTxCallbackListStat;
struct RetryInfo
{
  RetryInfo() : retry_cnt_(0), last_retry_ts_(0) {}
  int64_t to_string(char *buf, const int64_t buf_len) const {
    int64_t pos = 0;
    int64_t retry_cnt = ATOMIC_LOAD(&retry_cnt_);
    (void) databuff_printf(buf, buf_len, pos, "retry_cnt:%ld, last_retry_ts:%s",
                           retry_cnt, ObTime2Str::ob_timestamp_str(last_retry_ts_));
    return pos;
  }
  void reset() { retry_cnt_ = 0; last_retry_ts_ = 0; }
  void on_conflict() {
    ATOMIC_AAF(&retry_cnt_, 1);
    last_retry_ts_ = ObClockGenerator::getClock();
  }
  bool need_print() const {
    bool ret = false;
    int64_t ts = ObClockGenerator::getClock();
    if (ATOMIC_LOAD(&retry_cnt_) % 10 == 0 ||// retry cnt more than specified times
        ts - last_retry_ts_ >= 1_s ||// retry interval more than specified interval seconds
        last_retry_ts_ == 0) {// retry ts is invalid
      ret = true;
    }
    return ret;
  }
  int64_t retry_cnt_;
  int64_t last_retry_ts_;
};

// 1. When fill redo log, if there is a big row, the meta info should record
// the relative position of the current log in the big row, that is: BIG_ROW_START,
// BIG_ROW_MID OR BIG_ROW_END
// 2. For a normal row, the flag is NORMAL_ROW
class ObTransRowFlag
{
public:
  static const uint8_t NORMAL_ROW = 0;
  static const uint8_t BIG_ROW_NEW = 1;
  static const uint8_t BIG_ROW_OLD = 2;
  static const uint8_t MAX = 3;
  static const uint8_t ENCRYPT = (1 << 3);
public:
  static bool is_valid_row_flag(const uint8_t row_flag)
  {
    const uint8_t real_flag = row_flag & (~ENCRYPT);
    return real_flag < MAX;
  }
  // 是否是行首
  static bool is_row_start(const uint8_t row_flag)
  {
    const uint8_t real_flag = row_flag & (~ENCRYPT);
    return NORMAL_ROW == real_flag;
  }
  static bool is_normal_row(const uint8_t row_flag)
  {
    const uint8_t real_flag = row_flag & (~ENCRYPT);
    return real_flag == NORMAL_ROW;
  }
  static bool is_big_row(const uint8_t row_flag)
  {
    const uint8_t real_flag = row_flag & (~ENCRYPT);
    return BIG_ROW_NEW == real_flag || BIG_ROW_OLD == real_flag;
  }
  static bool is_big_row_new(const uint8_t row_flag)
  {
    const uint8_t real_flag = row_flag & (~ENCRYPT);
    return BIG_ROW_NEW == real_flag;
  }
  static bool is_big_row_start(const uint8_t row_flag)
  {
    UNUSED(row_flag);
    return false;
  }
  static bool is_big_row_mid(const uint8_t row_flag)
  {
    UNUSED(row_flag);
    return false;
  }
  static bool is_big_row_end(const uint8_t row_flag)
  {
    UNUSED(row_flag);
    return false;
  }
  static bool is_encrypted(const uint8_t row_flag)
  {
    return row_flag & ENCRYPT;
  }
  static void add_encrypt_flag(uint8_t &row_flag)
  {
    row_flag |= ENCRYPT;
  }
  static void remove_encrypt_flag(uint8_t &row_flag)
  {
    row_flag &= (~ENCRYPT);
  }
};

class ObQueryAllocator final : public common::ObIAllocator
{
public:
  explicit ObQueryAllocator()
    : alloc_count_(0),
      free_count_(0),
      alloc_size_(0),
      is_inited_(false) {}
  ~ObQueryAllocator()
  {
    if (OB_UNLIKELY(ATOMIC_LOAD(&free_count_) != ATOMIC_LOAD(&alloc_count_))) {
      TRANS_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "query allocator leak found", K(alloc_count_), K(free_count_), K(alloc_size_));
    }
    ATOMIC_STORE(&is_inited_, false);
  }
  int init(const uint64_t tenant_id)
  {
    int ret = OB_SUCCESS;
    ObMemAttr attr(tenant_id, ObModIds::OB_QUERY_ALLOCATOR);
    if (OB_UNLIKELY(free_count_ != alloc_count_)) {
      TRANS_LOG(ERROR, "query allocator leak found", K(alloc_count_), K(free_count_), K(alloc_size_));
    }
    if (IS_NOT_INIT) {
      if (OB_FAIL(allocator_.init(NULL, //use default allocator in fifo_allocator
                                  common::OB_MALLOC_NORMAL_BLOCK_SIZE,
                                  attr))) {
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
  void reset(bool only_check = false)
  {
    if (OB_UNLIKELY(ATOMIC_LOAD(&free_count_) != ATOMIC_LOAD(&alloc_count_))) {
      TRANS_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "query allocator leak found", K(alloc_count_), K(free_count_), K(alloc_size_));
      OB_SAFE_ABORT();
    }
    if (!only_check) {
      allocator_.reset();
      ATOMIC_STORE(&alloc_count_, 0);
      ATOMIC_STORE(&free_count_, 0);
      ATOMIC_STORE(&alloc_size_, 0);
      ATOMIC_STORE(&is_inited_, false);
    }
  }
  void *alloc(const int64_t size) override
  {
    void *ret = nullptr;
    if (OB_ISNULL(ret = allocator_.alloc(size))) {
      TRANS_LOG_RET(ERROR, common::OB_ALLOCATE_MEMORY_FAILED, "query alloc failed",
        K(alloc_count_), K(free_count_), K(alloc_size_), K(size));
    } else {
      ATOMIC_INC(&alloc_count_);
      ATOMIC_FAA(&alloc_size_, size);
    }
    return ret;
  }
  void* alloc(const int64_t size, const ObMemAttr &attr) override
  {
    UNUSED(attr);
    return alloc(size);
  }
  void free(void *ptr) override
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
class ObMemtableCtxCbAllocator final : public common::ObIAllocator
{
public:
  explicit ObMemtableCtxCbAllocator()
    : alloc_count_(0),
      free_count_(0),
      alloc_size_(0),
      is_inited_(false) {}
  ~ObMemtableCtxCbAllocator()
  {
    if (OB_UNLIKELY(ATOMIC_LOAD(&free_count_) != ATOMIC_LOAD(&alloc_count_))) {
      TRANS_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "callback memory leak found", K(alloc_count_), K(free_count_), K(alloc_size_));
    }
    ATOMIC_STORE(&is_inited_, false);
  }
  // FIFOAllocator doesn't support double init, even after reset, so is_inited_ is handled specially here.
  int init(const uint64_t tenant_id)
  {
    int ret = OB_SUCCESS;
    ObMemAttr attr(tenant_id, ObModIds::OB_MEMTABLE_CALLBACK, ObCtxIds::TX_CALLBACK_CTX_ID);
    if (OB_UNLIKELY(free_count_ != alloc_count_)) {
      TRANS_LOG(ERROR, "callback memory leak found", K(alloc_count_), K(free_count_), K(alloc_size_));
    }
    if (IS_NOT_INIT) {
      if (OB_FAIL(allocator_.init(NULL,
                                  common::OB_MALLOC_NORMAL_BLOCK_SIZE,
                                  attr))) {
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
  void reset(bool only_check = false)
  {
    if (OB_UNLIKELY(free_count_ != alloc_count_)) {
      TRANS_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "callback memory leak found", K(alloc_count_), K(free_count_), K(alloc_size_));
      OB_SAFE_ABORT();
    }
    if (!only_check) {
      allocator_.reset();
      ATOMIC_STORE(&alloc_count_, 0);
      ATOMIC_STORE(&free_count_, 0);
      ATOMIC_STORE(&alloc_size_, 0);
      ATOMIC_STORE(&is_inited_, false);
    }
  }
  void *alloc(const int64_t size) override
  {
    void *ret = nullptr;
    if (OB_ISNULL(ret = allocator_.alloc(size))) {
      TRANS_LOG_RET(ERROR, OB_ALLOCATE_MEMORY_FAILED, "callback memory failed",
        K(alloc_count_), K(free_count_), K(alloc_size_), K(size));
    } else {
      ATOMIC_INC(&alloc_count_);
      ATOMIC_FAA(&alloc_size_, size);
    }
    return ret;
  }
  void* alloc(const int64_t size, const ObMemAttr &attr) override
  {
    UNUSED(attr);
    return alloc(size);
  }
  void free(void *ptr) override
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
typedef common::ObIDMap<ObIMemtableCtx, uint32_t> MemtableIDMap;
class ObMemtableCtx : public ObIMemtableCtx
{
  using RWLock = common::SpinRWLock;
  using WRLockGuard = common::SpinWLockGuard;
  using RDLockGuard = common::SpinRLockGuard;
  static const int64_t SLOW_QUERY_THRESHOULD = 500 * 1000;
  static const int64_t LOG_CONFLICT_INTERVAL = 3 * 1000 * 1000;
  static const int64_t MAX_RESERVED_CONFLICT_TX_NUM = 30;
public:
  ObMemtableCtx();
  virtual ~ObMemtableCtx();
  virtual void reset();
public:
  int init(const uint64_t tenant_id);
  virtual void *old_row_alloc(const int64_t size) override;
  virtual void old_row_free(void *row) override;
  virtual common::ObIAllocator &get_query_allocator();
  virtual void inc_lock_for_read_retry_count();
  // When row lock conflict occurs in a remote execution, record the trans id in
  // transaction context, and carries it back after execution, for dead lock detect use
  virtual int add_conflict_trans_id(const transaction::ObTransID conflict_trans_id);
  void reset_conflict_trans_ids();
  int get_conflict_trans_ids(common::ObIArray<transaction::ObTransIDAndAddr> &array);
  virtual int read_lock_yield()
  {
    return ATOMIC_LOAD(&end_code_);
  }
  virtual int write_lock_yield();
public:
  virtual void set_read_only();
  virtual void inc_ref();
  virtual void dec_ref();
  void wait_pending_write();
  void wait_write_end();
  virtual int write_auth(const bool exclusive);
  virtual int write_done();
  virtual int trans_begin();
  virtual int replay_begin(const bool parallel_replay, const share::SCN scn);
  virtual int replay_end(const bool is_replay_succ,
                         const int16_t callback_list_idx,
                         const share::SCN scn);
  int rollback_redo_callbacks(const int16_t callback_list_idx, const share::SCN scn);
  virtual int calc_checksum_all(ObIArray<uint64_t> &checksum);
  static void convert_checksum_for_commit_log(const ObIArray<uint64_t> &arr,
                                              uint64_t &checksum,
                                              ObIArray<uint8_t> &sig);
  virtual void print_callbacks();
  virtual int trans_end(const bool commit,
                        const share::SCN trans_version,
                        const share::SCN final_scn);
  virtual int trans_clear();
  virtual int elr_trans_preparing();
  virtual void elr_trans_revoke();
  virtual int trans_kill();
  virtual int trans_publish();
  virtual int trans_replay_begin();
  virtual int trans_replay_end(const bool commit,
                               const share::SCN trans_version,
                               const share::SCN final_scn,
                               const uint64_t log_cluster_version = 0,
                               const uint64_t checksum = 0);
  //method called when leader takeover
  virtual int replay_to_commit(const bool is_resume);
  //method called when leader revoke
  virtual int commit_to_replay();
  virtual int fill_redo_log(ObTxFillRedoCtx &ctx);
  bool is_logging_blocked(bool &has_pending_log) const {
    return trans_mgr_.is_logging_blocked(has_pending_log);
  }
  void check_all_redo_flushed();
  int get_log_guard(const transaction::ObTxSEQ &write_seq,
                    ObCallbackListLogGuard &log_guard,
                    int &cb_list_idx);
  int calc_checksum_before_scn(const share::SCN scn,
                               ObIArray<uint64_t> &checksum,
                               ObIArray<share::SCN> &checksum_scn);
  int update_checksum(const ObIArray<uint64_t> &checksum,
                      const ObIArray<share::SCN> &checksum_scn);
  int log_submitted(const ObRedoLogSubmitHelper &helper);
  int sync_log_succ(const share::SCN scn, const ObCallbackScopeArray &callbacks);
  void sync_log_fail(const ObCallbackScopeArray &callbacks, const share::SCN &max_applied_scn);
  bool is_slow_query() const;
  virtual void set_trans_ctx(transaction::ObPartTransCtx *ctx);
  virtual transaction::ObPartTransCtx *get_trans_ctx() const { return ctx_; }
  virtual void inc_truncate_cnt() override { truncate_cnt_++; }
  int get_memtable_key_arr(transaction::ObMemtableKeyArray &memtable_key_arr);
  uint64_t get_lock_for_read_retry_count() const { return lock_for_read_retry_count_; }
  virtual void add_trans_mem_total_size(const int64_t size);
  int64_t get_ref() const { return ATOMIC_LOAD(&ref_); }
  uint64_t get_tenant_id() const;
  inline bool has_row_updated() const { return has_row_updated_; }
  inline void set_row_updated() { has_row_updated_ = true; }
  int remove_callbacks_for_fast_commit(const ObCallbackScopeArray &callbacks);
  int remove_callbacks_for_fast_commit(const int16_t callback_list_idx, const share::SCN stop_scn);
  int remove_callback_for_uncommited_txn(const memtable::ObMemtableSet *memtable_set);
  int rollback(const transaction::ObTxSEQ seq_no, const transaction::ObTxSEQ from_seq_no,
               const share::SCN replay_scn);
  void set_parallel_logging(const share::SCN serial_final_scn,
                            const transaction::ObTxSEQ serial_final_seq_no) {
    trans_mgr_.set_parallel_logging(serial_final_scn, serial_final_seq_no);
  }
  void set_skip_checksum_calc() {
    trans_mgr_.set_skip_checksum_calc();
  }
  int64_t get_trans_mem_total_size() const { return trans_mem_total_size_; }
  void add_lock_for_read_elapse(const int64_t elapse) { lock_for_read_elapse_ += elapse; }
  int64_t get_lock_for_read_elapse() const { return lock_for_read_elapse_; }
  bool pending_log_size_too_large(const transaction::ObTxSEQ &write_seq_no);
  void reset_pdml_stat();
  int clean_unlog_callbacks();
  int check_tx_mem_size_overflow(bool &is_overflow);
public:
  void on_key_duplication_retry(const ObMemtableKey& key);
  void on_tsc_retry(const ObMemtableKey& key,
                    const share::SCN snapshot_version,
                    const share::SCN max_trans_version,
                    const transaction::ObTransID &conflict_tx_id);
  void on_wlock_retry(const ObMemtableKey& key, const transaction::ObTransID &conflict_tx_id);
  virtual int64_t to_string(char *buf, const int64_t buf_len) const;
  virtual transaction::ObTransID get_tx_id() const override;
  virtual share::SCN get_tx_end_scn() const override;

  // statics maintainness for txn logging
  virtual void inc_unsubmitted_cnt() override;
  virtual void dec_unsubmitted_cnt() override;

public: // callback
  virtual void *alloc_mvcc_row_callback() override;
  virtual void free_mvcc_row_callback(ObITransCallback *cb) override;
  virtual storage::ObExtInfoCallback *alloc_ext_info_callback() override;
  virtual void free_ext_info_callback(ObITransCallback *cb) override;
  void *alloc_lock_link_node() { return mem_ctx_obj_pool_.alloc<transaction::tablelock::ObMemCtxLockOpLinkNode>(); }
  void free_lock_link_node(void *ptr) { mem_ctx_obj_pool_.free<transaction::tablelock::ObMemCtxLockOpLinkNode>(ptr); }
  void *alloc_table_lock_callback() { return mem_ctx_obj_pool_.alloc<transaction::tablelock::ObOBJLockCallback>(); }
  virtual void free_table_lock_callback(ObITransCallback *cb) override
  {
    mem_ctx_obj_pool_.free<transaction::tablelock::ObOBJLockCallback>(cb);
  }
  virtual ObOBJLockCallback *create_table_lock_callback(ObIMvccCtx &ctx, ObLockMemtable *memtable) override
  {
    return lock_mem_ctx_.create_table_lock_callback(ctx, memtable);
  }

  bool is_for_replay() const { return trans_mgr_.is_for_replay(); }
  int append_callback(ObITransCallback *cb) { return trans_mgr_.append(cb); }
  int64_t get_pending_log_size() { return trans_mgr_.get_pending_log_size(); }
  int64_t get_flushed_log_size() { return trans_mgr_.get_flushed_log_size(); }
  int acquire_callback_list(const bool new_epoch)
  { return trans_mgr_.acquire_callback_list(new_epoch); }
  void revert_callback_list() { trans_mgr_.revert_callback_list(); }
  void set_for_replay(const bool for_replay) { trans_mgr_.set_for_replay(for_replay); }
  void inc_pending_log_size(const int64_t size) { trans_mgr_.inc_pending_log_size(size); }
  void inc_flushed_log_size(const int64_t size) { trans_mgr_.inc_flushed_log_size(size); }
  int64_t get_write_epoch() const { return trans_mgr_.get_write_epoch(); }
public:
  // tx_status
  enum ObTxStatus {
    PARTIAL_ROLLBACKED = -1,
    NORMAL = 0,
    ROLLBACKED = 1,
  };
  virtual int64_t get_tx_status() const { return ATOMIC_LOAD(&tx_status_); }
  bool is_tx_rollbacked() const { return get_tx_status() != ObTxStatus::NORMAL; }
  inline void set_partial_rollbacked() { ATOMIC_STORE(&tx_status_, ObTxStatus::PARTIAL_ROLLBACKED); }
  inline void set_tx_rollbacked() { ATOMIC_STORE(&tx_status_, ObTxStatus::ROLLBACKED); }
public:
  // table lock.
  int enable_lock_table(transaction::ObLSTxCtxMgr *ls_tx_ctx_mgr);
  // for mintest
  int enable_lock_table(storage::ObTableHandleV2 &handle);
  transaction::tablelock::ObLockMemCtx &get_lock_mem_ctx() { return lock_mem_ctx_; }
  int get_tx_seq_replay_idx(const transaction::ObTxSEQ seq) const
  { return trans_mgr_.get_tx_seq_replay_idx(seq); }
  int check_lock_exist(const ObLockID &lock_id,
                       const ObTableLockOwnerID &owner_id,
                       const ObTableLockMode mode,
                       const ObTableLockOpType op_type,
                       bool &is_exist,
                       uint64_t lock_mode_cnt_in_same_trans[]) const;
  int check_modify_schema_elapsed(const common::ObTabletID &tablet_id,
                                  const int64_t schema_version);
  int check_modify_time_elapsed(const common::ObTabletID &tablet_id,
                                const int64_t timestamp);
  int iterate_tx_obj_lock_op(ObLockOpIterator &iter) const;
  int check_lock_need_replay(const share::SCN &scn,
                             const transaction::tablelock::ObTableLockOp &lock_op,
                             bool &need_replay);
  int add_lock_record(const transaction::tablelock::ObTableLockOp &lock_op);
  int replay_add_lock_record(const transaction::tablelock::ObTableLockOp &lock_op,
                             const share::SCN &scn);
  void remove_lock_record(ObMemCtxLockOpLinkNode *lock_op);
  // replay lock to lock map and trans part ctx.
  // used by the replay process of multi data source.
  int replay_lock(const transaction::tablelock::ObTableLockOp &lock_op,
                  const share::SCN &scn);
  int recover_from_table_lock_durable_info(const ObTableLockInfo &table_lock_info,
                                           const bool transfer_merge = false);
  int get_table_lock_store_info(ObTableLockInfo &table_lock_info);
  int get_table_lock_for_transfer(ObTableLockInfo &table_lock_info, const ObIArray<common::ObTabletID> &tablet_list);
  // for deadlock detect.
  void set_table_lock_killed() { lock_mem_ctx_.set_killed(); }
  bool is_table_lock_killed() const { return lock_mem_ctx_.is_killed(); }
  // The SQL can be rollbacked, and the callback of it will be removed, too.
  // In this case, the remove count of callbacks is larger than 0, but the callbacks
  // may be all decided. So we can't exactly know whether they're decided.
  bool maybe_has_undecided_callback() const {
      return trans_mgr_.get_callback_remove_for_fast_commit_count() > 0 ||
             trans_mgr_.get_callback_remove_for_remove_memtable_count() > 0;
  }
  void print_first_mvcc_callback();
  int get_callback_list_stat(ObIArray<ObTxCallbackListStat> &stats);
  int get_lock_memtable(ObLockMemtable *&memtable)
  {
    return lock_mem_ctx_.get_lock_memtable(memtable);
  }

private:
  int do_trans_end(
      const bool commit,
      const share::SCN trans_version,
      const share::SCN final_scn,
      const int end_code);
  int clear_table_lock_(const bool is_commit,
                        const share::SCN &commit_version,
                        const share::SCN &commit_scn);
  int rollback_table_lock_(const transaction::ObTxSEQ to_seq_no,
                           const transaction::ObTxSEQ from_seq_no);
  int register_multi_source_data_if_need_(
      const transaction::tablelock::ObTableLockOp &lock_op);
  static int64_t get_us() { return ::oceanbase::common::ObTimeUtility::current_time(); }
  int reset_log_generator_();
  int reuse_log_generator_();

public:
  inline ObRedoLogGenerator &get_redo_generator() { return log_gen_; }
private:
  DISALLOW_COPY_AND_ASSIGN(ObMemtableCtx);

  static const int8_t ELR_STATE_INIT = 0;
  static const int8_t ELR_STATE_DONE = 1;
  static const int8_t ELR_STATE_REVOKED = 2;

  RWLock rwlock_;
  common::ObByteLock lock_;
  int end_code_;
  int64_t tx_status_;
  int8_t elr_state_;
  int64_t ref_;
  // allocate memory for callback when query executing
  ObQueryAllocator query_allocator_;
  ObMemtableCtxCbAllocator ctx_cb_allocator_;
  ObRedoLogGenerator log_gen_;
  RetryInfo retry_info_;
  transaction::ObPartTransCtx *ctx_;
  int64_t truncate_cnt_;
  // the retry count of lock for read
  uint64_t lock_for_read_retry_count_;
  // Time cost of lock for read
  int64_t lock_for_read_elapse_;
  int64_t trans_mem_total_size_;
  // statistics for txn logging
  int64_t unsubmitted_cnt_;
  int64_t callback_mem_used_;
  int64_t callback_alloc_count_;
  int64_t callback_free_count_;
  bool is_read_only_;
  bool is_master_;
  // Used to indicate whether mvcc row is updated or not.
  // When a statement is update or select for update, the value can be set ture;
  bool has_row_updated_;
  // For deaklock detection
  // The trans id of the holder of the conflict row lock
  // TODO(Handora), for non-local execution, if no-occupy-thread wait is implemented,
  // it should be carried back the same way as local execution
  common::ObArray<transaction::ObTransID> conflict_trans_ids_;
  transaction::ObMemtableCtxObjPool mem_ctx_obj_pool_;
  // table lock mem ctx.
  transaction::tablelock::ObLockMemCtx lock_mem_ctx_;
  // trans callback mgr
  ObTransCallbackMgr trans_mgr_;
  bool is_inited_;
};

}
}

#endif //OCEANBASE_MEMTABLE_OB_MEMTABLE_CONTEXT_
