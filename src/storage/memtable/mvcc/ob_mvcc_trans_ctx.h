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

#ifndef OCEANBASE_MVCC_OB_MVCC_TRANS_CTX_
#define OCEANBASE_MVCC_OB_MVCC_TRANS_CTX_
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/utility.h"
#include "common/ob_tablet_id.h"
#include "ob_row_data.h"
#include "ob_mvcc_row.h"
#include "ob_mvcc.h"
#include "storage/memtable/ob_memtable_key.h"
#include "storage/tx/ob_trans_define.h"
#include "storage/memtable/mvcc/ob_tx_callback_list.h"
#include "storage/tablelock/ob_table_lock_common.h"
#include "storage/memtable/ob_memtable_util.h"

namespace oceanbase
{
namespace transaction
{
class ObMemtableCtxObjPool;
}
namespace common
{
class ObTabletID;
};
namespace storage
{
class ObIMemtable;
};
namespace memtable
{
class ObMemtableCtxCbAllocator;
class ObMemtable;
class ObCallbackScope;
typedef ObIArray<ObCallbackScope> ObCallbackScopeArray;
enum class MutatorType;
class ObTxFillRedoCtx;
class RedoLogEpoch;
class ObCallbackListLogGuard;
class ObTxCallbackListStat;
class ObITransCallback;
struct RedoDataNode
{
  void set(const ObMemtableKey *key,
           const ObRowData &old_row,
           const ObRowData &new_row,
           const blocksstable::ObDmlFlag dml_flag,
           const uint32_t modify_count,
           const uint32_t acc_checksum,
           const int64_t version,
           const int32_t flag,
           const transaction::ObTxSEQ seq_no,
           const common::ObTabletID &tablet_id,
           const int64_t column_cnt);
  void set_callback(ObITransCallback *callback) { callback_ = callback; }
  ObMemtableKey key_;
  ObRowData old_row_;
  ObRowData new_row_;
  blocksstable::ObDmlFlag dml_flag_;
  uint32_t modify_count_;
  uint32_t acc_checksum_;
  int64_t version_;
  int32_t flag_; // currently, unused
  transaction::ObTxSEQ seq_no_;
  ObITransCallback *callback_;
  common::ObTabletID tablet_id_;
  int64_t column_cnt_;
};

struct TableLockRedoDataNode
{
  int set(const ObMemtableKey *key,
          const transaction::tablelock::ObTableLockOp &lock_op,
          const common::ObTabletID &tablet_id,
          ObITransCallback *callback);
  ObMemtableKey key_;
  transaction::ObTxSEQ seq_no_;
  ObITransCallback *callback_;
  common::ObTabletID tablet_id_;

  transaction::tablelock::ObLockID lock_id_;
  transaction::tablelock::ObTableLockOwnerID owner_id_;
  transaction::tablelock::ObTableLockMode lock_mode_;
  transaction::tablelock::ObTableLockOpType lock_op_type_;
  int64_t create_timestamp_;
  int64_t create_schema_version_;
};

class ObMemtableCtx;
class ObTxCallbackList;

class ObITransCallbackIterator
{
public:
  ObITransCallbackIterator(): cur_(nullptr) {}
  ObITransCallbackIterator(ObITransCallback *cur): cur_(cur) {}
  void reset() { cur_ = nullptr; }
  ObITransCallbackIterator& operator=(const ObITransCallbackIterator &that)
  {
    cur_ = that.cur_;
    return *this;
  }
  ObITransCallback* operator*() { return cur_; }
  ObITransCallback* operator*() const { return cur_; }
  bool operator==(const ObITransCallbackIterator &that) const { return cur_ == that.cur_; }
  bool operator!=(const ObITransCallbackIterator &that) const { return cur_ != that.cur_; }
  ObITransCallbackIterator operator+(int i)
  {
    ObITransCallbackIterator ret(cur_);
    while (i != 0) {
      if (i > 0) {
        ++ret;
        --i;
      } else {
        --ret;
        ++i;
      }
    }
    return ret;
  }
  ObITransCallbackIterator operator-(int i)
  {
    return (*this) + (-i);
  }
  ObITransCallbackIterator& operator++() // ++iter
  {
    cur_ = cur_->get_next();
    return *this;
  }
  ObITransCallbackIterator& operator--() // --iter
  {
    cur_ = cur_->get_prev();
    return *this;
  }
  ObITransCallbackIterator operator++(int) // iter++
  {
    ObITransCallback *cur_save = cur_;
    cur_ = cur_->get_next();
    return ObITransCallbackIterator(cur_save);
  }
  ObITransCallbackIterator operator--(int) // iter--
  {
    ObITransCallback *cur_save = cur_;
    cur_ = cur_->get_prev();
    return ObITransCallbackIterator(cur_save);
  }
private:
  ObITransCallback *cur_;
};

// 事务commit/abort的callback不允许出错，也没法返回错误，就算返回错误调用者也没法处理，所以callback都返回void
class ObTransCallbackMgr
{
public:
  class WRLockGuard
  {
  public:
    explicit WRLockGuard(const common::SpinRWLock &rwlock);
    ~WRLockGuard() {}
  private:
#ifdef ENABLE_DEBUG_LOG
    common::ObSimpleTimeGuard time_guard_; // print log and lbt, if the lock is held too much time.
#endif
    common::SpinWLockGuard lock_guard_;
  };
  class RDLockGuard
  {
  public:
    explicit RDLockGuard(const common::SpinRWLock &rwlock);
    ~RDLockGuard() {}
  private:
#ifdef ENABLE_DEBUG_LOG
    common::ObSimpleTimeGuard time_guard_; // print log and lbt, if the lock is held too much time.
#endif
    common::SpinRLockGuard lock_guard_;
  };

  friend class ObITransCallbackIterator;
  enum { MAX_CALLBACK_LIST_COUNT = transaction::MAX_CALLBACK_LIST_COUNT };
  enum { MAX_CB_ALLOCATOR_COUNT = OB_MAX_CPU_NUM };
  enum {
    PARALLEL_STMT = -1
  };
public:
  ObTransCallbackMgr(ObIMvccCtx &host,
                     ObMemtableCtxCbAllocator &cb_allocator,
                     transaction::ObMemtableCtxObjPool &mem_ctx_obj_pool)
    : host_(host),
      skip_checksum_(false),
      mem_ctx_obj_pool_(mem_ctx_obj_pool),
      callback_list_(*this, 0),
      callback_lists_(NULL),
      rwlock_(ObLatchIds::MEMTABLE_CALLBACK_LIST_MGR_LOCK),
      parallel_stat_(0),
      write_epoch_(0),
      write_epoch_start_tid_(0),
      for_replay_(false),
      has_branch_replayed_into_first_list_(false),
      serial_final_scn_(share::SCN::max_scn()),
      serial_final_seq_no_(),
      serial_sync_scn_(share::SCN::min_scn()),
      callback_main_list_append_count_(0),
      callback_remove_for_trans_end_count_(0),
      callback_remove_for_remove_memtable_count_(0),
      callback_remove_for_fast_commit_count_(0),
      callback_remove_for_rollback_to_count_(0),
      callback_ext_info_log_count_(0),
      pending_log_size_(0),
      flushed_log_size_(0),
      cb_allocator_(cb_allocator),
      cb_allocators_(NULL)
  {
  }
  ~ObTransCallbackMgr() {}
  void reset();
  ObIMvccCtx &get_ctx() { return host_; }
  void *alloc_mvcc_row_callback();
  void free_mvcc_row_callback(ObITransCallback *cb);
  int append(ObITransCallback *node);
  void before_append(ObITransCallback *node);
  void after_append(ObITransCallback *node, const int ret_code);
  void trans_start();
  int calc_checksum_all(ObIArray<uint64_t> &checksum);
  void print_callbacks();
  int get_callback_list_stat(ObIArray<ObTxCallbackListStat> &stats);
  void elr_trans_preparing();
  int trans_end(const bool commit);
  void replay_begin(const bool parallel_replay, share::SCN ccn);
public:
  int replay_fail(const int16_t callback_list_idx, const share::SCN scn);
  int replay_succ(const int16_t callback_list_idx, const share::SCN scn);
  int rollback_to(const transaction::ObTxSEQ seq_no,
                  const transaction::ObTxSEQ from_seq_no,
                  const share::SCN replay_scn,
                  int64_t &remove_cnt);
  void set_for_replay(const bool for_replay);
  bool is_for_replay() const { return ATOMIC_LOAD(&for_replay_); }
  int remove_callbacks_for_fast_commit(const int16_t callback_list_idx, const share::SCN stop_scn);
  int remove_callbacks_for_fast_commit(const ObCallbackScopeArray &callbacks_arr);
  int remove_callback_for_uncommited_txn(const memtable::ObMemtableSet *memtable_set);
  int get_memtable_key_arr(transaction::ObMemtableKeyArray &memtable_key_arr);
  int acquire_callback_list(const bool new_epoch);
  void revert_callback_list();
  int get_tx_seq_replay_idx(const transaction::ObTxSEQ seq) const;
  common::SpinRWLock& get_rwlock() { return rwlock_; }
private:
  void wakeup_waiting_txns_();
  int extend_callback_lists_(const int16_t cnt);
public:
  bool is_logging_blocked(bool &has_pending_log) const;
  int fill_log(ObTxFillRedoCtx &ctx, ObITxFillRedoFunctor &func);
  int log_submitted(const ObCallbackScopeArray &callbacks, share::SCN scn, int &submitted);
  int log_sync_succ(const ObCallbackScopeArray &callbacks, const share::SCN scn, int64_t &sync_cnt);
  int log_sync_fail(const ObCallbackScopeArray &callbacks, const share::SCN scn, int64_t &removed_cnt);
  void check_all_redo_flushed();
  int calc_checksum_before_scn(const share::SCN scn,
                               ObIArray<uint64_t> &checksum,
                               ObIArray<share::SCN> &checksum_scn);
  int update_checksum(const ObIArray<uint64_t> &checksum,
                      const ObIArray<share::SCN> &checksum_scn);
  int clean_unlog_callbacks(int64_t &removed_cnt, common::ObFunction<void()> &before_remove);
  // when not inc, return -1
  int64_t inc_pending_log_size(const int64_t size);
  void inc_flushed_log_size(const int64_t size);
  void clear_pending_log_size() { ATOMIC_STORE(&pending_log_size_, 0); }
  int64_t get_pending_log_size() const;
  bool pending_log_size_too_large(const transaction::ObTxSEQ &write_seq_no, const int64_t limit);
  int64_t get_flushed_log_size() const;
  int get_log_guard(const transaction::ObTxSEQ &write_seq,
                    ObCallbackListLogGuard &log_guard,
                    int &cb_list_idx);
  void set_parallel_logging(const share::SCN serial_final_scn,
                            const transaction::ObTxSEQ serial_final_seq_no);
  void set_skip_checksum_calc();
  bool skip_checksum_calc() const { return ATOMIC_LOAD(&skip_checksum_); }
  void reset_pdml_stat();
  bool find(ObITxCallbackFinder &func);
  uint64_t get_main_list_length() const
  { return callback_list_.get_length(); }
  int64_t get_callback_main_list_append_count() const
  { return callback_main_list_append_count_; }
  int64_t get_callback_remove_for_trans_end_count() const
  { return callback_remove_for_trans_end_count_; }
  int64_t get_callback_remove_for_remove_memtable_count() const
  { return callback_remove_for_remove_memtable_count_; }
  int64_t get_callback_remove_for_fast_commit_count() const
  { return callback_remove_for_fast_commit_count_; }
  int64_t get_callback_remove_for_rollback_to_count() const
  { return callback_remove_for_rollback_to_count_; }
  int64_t get_callback_ext_info_log_count() const
  { return callback_ext_info_log_count_; }
  void add_main_list_append_cnt(int64_t cnt = 1)
  { ATOMIC_AAF(&callback_main_list_append_count_, cnt); }
  void add_tx_end_callback_remove_cnt(int64_t cnt = 1)
  { ATOMIC_AAF(&callback_remove_for_trans_end_count_, cnt); }
  void add_release_memtable_callback_remove_cnt(int64_t cnt = 1)
  { ATOMIC_AAF(&callback_remove_for_remove_memtable_count_, cnt); }
  void add_fast_commit_callback_remove_cnt(int64_t cnt= 1)
  { ATOMIC_AAF(&callback_remove_for_fast_commit_count_, cnt); }
  void add_rollback_to_callback_remove_cnt(int64_t cnt = 1)
  { ATOMIC_AAF(&callback_remove_for_rollback_to_count_, cnt); }
  void add_callback_ext_info_log_count(int64_t cnt = 1)
  { ATOMIC_AAF(&callback_ext_info_log_count_, cnt); }
  int get_callback_list_count() const
  { return  callback_lists_ ? MAX_CALLBACK_LIST_COUNT : 1; }
  int get_logging_list_count() const;
  ObTxCallbackList *get_callback_list_(const int16_t index, const bool nullable);
  bool is_serial_final() const { return is_serial_final_(); }
  bool is_callback_list_append_only(const int idx) const
  {
    return (idx == 0) || !for_replay_ || is_serial_final_();
  }
  void print_statistics(char *buf, const int64_t buf_len, int64_t &pos) const;
  transaction::ObPartTransCtx *get_trans_ctx() const;
  TO_STRING_KV(KP(this),
               K_(serial_final_scn),
               K_(serial_final_seq_no),
               K_(serial_sync_scn),
               KP_(callback_lists),
               K_(pending_log_size),
               K_(flushed_log_size),
               K_(for_replay),
               K_(parallel_stat));
private:
  void update_serial_sync_scn_(const share::SCN scn);
  bool is_serial_final_() const
  {
    return serial_final_scn_ == serial_sync_scn_;
  }
  bool is_parallel_logging_() const
  {
    return !serial_final_scn_.is_max();
  }
  int fill_from_one_list(ObTxFillRedoCtx &ctx, const int list_idx, ObITxFillRedoFunctor &func);
  int fill_from_all_list(ObTxFillRedoCtx &ctx, ObITxFillRedoFunctor &func);
  bool check_list_has_min_epoch_(const int my_idx,
                                 const int64_t my_epoch,
                                 const bool require_min,
                                 int64_t &min_epoch,
                                 int &min_idx);
  void calc_list_fill_log_epoch_(const int list_idx, int64_t &epoch_from, int64_t &epoch_to);
  void calc_next_to_fill_log_info_(const ObIArray<RedoLogEpoch> &arr,
                                   int &index,
                                   int64_t &epoch_from,
                                   int64_t &epoch_to);
  int prep_and_fill_from_list_(ObTxFillRedoCtx &ctx,
                               ObITxFillRedoFunctor &func,
                               int16 &callback_scope_idx,
                               const int index,
                               int64_t epoch_from,
                               int64_t epoch_to);
private:
  ObIMvccCtx &host_;
  // for incomplete replay, checksum is not need
  // for tx is aborted, checksum is not need
  bool skip_checksum_;
  transaction::ObMemtableCtxObjPool &mem_ctx_obj_pool_;
  ObTxCallbackList callback_list_;   // default
  ObTxCallbackList *callback_lists_; // extends for parallel write
  common::SpinRWLock rwlock_;
  union {
    struct {
      int32_t ref_cnt_;
      int32_t tid_;
    };
    int64_t parallel_stat_;
  };
  // multi writes at the same time is in a epoch
  // used to serialize callbacks between multiple callback-list when fill redo
  int64_t write_epoch_;
  // remember the tid of first thread in current epoch
  // the first thread is always assigned to first callback-list
  int64_t write_epoch_start_tid_;
  RLOCAL_STATIC(bool, parallel_replay_);
  bool for_replay_;
  // used to mark that some branch callback replayed in first callback list
  // actually, by default they were replayed into its own callback list by
  // hash on branch id.
  // this can happened when txn recovery from a point after serial final log
  // and branch callback before (or equals to) serial final scn will be put
  // into the first list for easy to handle (ensure each callback list will
  // be `appended only` after serial final state).
  bool has_branch_replayed_into_first_list_;
  // the last serial log's scn
  share::SCN serial_final_scn_;
  transaction::ObTxSEQ serial_final_seq_no_;
  // currently synced serial log's scn
  // when serial_sync_scn_ == serial_final_scn_
  // it means the serial logging or serial replay has been finished
  share::SCN serial_sync_scn_;
  // statistics for callback remove
  int64_t callback_main_list_append_count_;
  int64_t callback_remove_for_trans_end_count_;
  int64_t callback_remove_for_remove_memtable_count_;
  int64_t callback_remove_for_fast_commit_count_;
  int64_t callback_remove_for_rollback_to_count_;
  int64_t callback_ext_info_log_count_;
  // current log size in leader participant
  int64_t pending_log_size_;
  // current flushed log size in leader participant
  int64_t flushed_log_size_;
  ObMemtableCtxCbAllocator &cb_allocator_;
  ObMemtableCtxCbAllocator *cb_allocators_;
};

//class ObIMvccCtx;
class ObMvccRowCallback final : public ObITransCallback
{
public:
  ObMvccRowCallback(ObIMvccCtx &ctx, ObMvccRow& value, ObMemtable *memtable) :
      ObITransCallback(),
      ctx_(ctx),
      value_(value),
      tnode_(NULL),
      data_size_(-1),
      memtable_(memtable),
      is_link_(false),
      not_calc_checksum_(false),
      is_non_unique_local_index_cb_(false),
      seq_no_(),
      column_cnt_(0)
  {}
  ObMvccRowCallback(ObMvccRowCallback &cb, ObMemtable *memtable) :
      ObITransCallback(cb.need_submit_log_),
      ctx_(cb.ctx_),
      value_(cb.value_),
      tnode_(cb.tnode_),
      data_size_(cb.data_size_),
      memtable_(memtable),
      is_link_(cb.is_link_),
      not_calc_checksum_(cb.not_calc_checksum_),
      is_non_unique_local_index_cb_(cb.is_non_unique_local_index_cb_),
      seq_no_(cb.seq_no_),
      column_cnt_(cb.column_cnt_)
  {
    (void)key_.encode(cb.key_.get_rowkey());
  }
  virtual ~ObMvccRowCallback() {}
  int link_trans_node();
  void unlink_trans_node();
  void set_is_link() { is_link_ = true; }
  void unset_is_link() { is_link_ = false; }
  void set(const ObMemtableKey *key,
           ObMvccTransNode *node,
           const int64_t data_size,
           const ObRowData *old_row,
           const bool is_replay,
           const transaction::ObTxSEQ seq_no,
           const int64_t column_cnt,
           const bool is_non_unique_local_index_cb)
  {
    UNUSED(is_replay);

    if (NULL != key) {
      key_.encode(*key);
    }

    tnode_ = node;
    data_size_ = data_size;
    if (NULL != old_row) {
      old_row_ = *old_row;
      if (old_row_.size_ == 0 && old_row_.data_ != NULL) {
        ob_abort();
      }
    } else {
      old_row_.reset();
    }
    seq_no_ = seq_no;
    if (tnode_) {
      tnode_->set_seq_no(seq_no_);
    }
    column_cnt_ = column_cnt;
    is_non_unique_local_index_cb_ = is_non_unique_local_index_cb;
  }
  bool on_memtable(const storage::ObIMemtable * const memtable) override;
  storage::ObIMemtable *get_memtable() const override;
  bool is_non_unique_local_index_cb() const { return is_non_unique_local_index_cb_;}
  virtual MutatorType get_mutator_type() const override;
  int get_redo(RedoDataNode &node);
  ObIMvccCtx &get_ctx() const { return ctx_; }
  const ObRowData &get_old_row() const { return old_row_; }
  const ObMvccRow &get_mvcc_row() const { return value_; }
  ObMvccTransNode *get_trans_node() { return tnode_; }
  const ObMvccTransNode *get_trans_node() const { return tnode_; }
  const ObMemtableKey *get_key() { return &key_; }
  int get_memtable_key(uint64_t &table_id, common::ObStoreRowkey &rowkey) const;
  bool is_logging_blocked() const override;
  uint32_t get_freeze_clock() const override;
  transaction::ObTxSEQ get_seq_no() const { return seq_no_; }
  int get_trans_id(transaction::ObTransID &trans_id) const;
  int get_cluster_version(uint64_t &cluster_version) const override;
  transaction::ObTransCtx *get_trans_ctx() const;
  int64_t to_string(char *buf, const int64_t buf_len) const;
  virtual int before_append(const bool is_replay) override;
  virtual void after_append(const bool is_replay) override;
  virtual int log_submitted(const share::SCN scn, storage::ObIMemtable *&last_mt) override;
  int64_t get_data_size()
  {
    return data_size_;
  }
  virtual int clean();
  virtual int del();
  virtual int checkpoint_callback();
  virtual int log_sync_fail(const share::SCN max_applied_scn) override;
  virtual int print_callback() override;
  virtual blocksstable::ObDmlFlag get_dml_flag() const override;
  virtual void set_not_calc_checksum(const bool not_calc_checksum) override
  {
    not_calc_checksum_ = not_calc_checksum;
  }
  const common::ObTabletID &get_tablet_id() const;
  int merge_memtable_key(transaction::ObMemtableKeyArray &memtable_key_arr);
private:
  virtual int trans_commit() override;
  virtual int trans_abort() override;
  virtual int rollback_callback() override;
  virtual int calc_checksum(const share::SCN checksum_scn,
                            TxChecksum *checksumer) override;
  virtual int elr_trans_preparing() override;
private:
  int link_and_get_next_node(ObMvccTransNode *&next);
  int row_delete();
  int merge_memtable_key(transaction::ObMemtableKeyArray &memtable_key_arr,
      ObMemtableKey &memtable_key, const common::ObTabletID &tablet_id);
  int clean_unlog_cb();
  void inc_unsubmitted_cnt_();
  int dec_unsubmitted_cnt_();
  int wakeup_row_waiter_if_need_();
private:
  ObIMvccCtx &ctx_;
  ObMemtableKey key_;
  ObMvccRow &value_;
  ObMvccTransNode *tnode_;
  int64_t data_size_;
  ObRowData old_row_;
  ObMemtable *memtable_;
  struct {
    bool is_link_ : 1;
    bool not_calc_checksum_ : 1;
    // this flag is currently only used to skip reset_hash_holder of ObLockWaitMgr,
    // but it is not set correctly in the replay path which will be fixed later.
    bool is_non_unique_local_index_cb_ : 1;
  };
  transaction::ObTxSEQ seq_no_;
  int64_t column_cnt_;
};

}; // end namespace memtable
}; // end namespace oceanbase

#endif /* OCEANBASE_MVCC_OB_MVCC_TRANS_CTX_ */

