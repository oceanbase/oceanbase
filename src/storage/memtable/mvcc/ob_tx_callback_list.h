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

#ifndef OCEANBASE_STORAGE_MEMTABLE_MVCC_OB_TX_CALLBACK_LIST
#define OCEANBASE_STORAGE_MEMTABLE_MVCC_OB_TX_CALLBACK_LIST

#include "storage/memtable/mvcc/ob_tx_callback_functor.h"
#include "storage/memtable/ob_memtable_util.h"
namespace oceanbase
{
namespace memtable
{

class ObTransCallbackMgr;
class ObCallbackScope;
class ObTxCallbackListStat;

class ObTxCallbackList
{
public:
  ObTxCallbackList(ObTransCallbackMgr &callback_mgr, const int16_t id);
  ~ObTxCallbackList();
  void reset();

  // append_callback will append your callback into the callback list
  int append_callback(ObITransCallback *callback,
                      const bool for_replay,
                      const bool parallel_replay = false,
                      const bool serial_final = false);

  // concat_callbacks will append all callbacks in other into itself and reset
  // other. And it will return the concat number during concat_callbacks.
  int64_t concat_callbacks(ObTxCallbackList &other);

  // remove_callbacks_for_fast_commit will remove all callbacks according to the
  // parameter _fast_commit_callback_count. It will only remove callbacks
  // without removing data by calling checkpoint_callback. So user need
  // implement lazy callback for the correctness. What's more, it will calculate
  // checksum when removing.
  int remove_callbacks_for_fast_commit(const share::SCN stop_scn = share::SCN::invalid_scn());

  // remove_callbacks_for_remove_memtable will remove all callbacks that is
  // belonged to the specified memtable sets. It will only remove callbacks
  // without removing data by calling checkpoint_callback. So user need to
  // implement lazy callback for the correctness. And user need guarantee all
  // callbacks belonged to the memtable sets must be synced before removing.
  // What's more, it will calculate checksum when removing.
  int remove_callbacks_for_remove_memtable(
    const memtable::ObMemtableSet *memtable_set,
    const share::SCN stop_scn = share::SCN::invalid_scn());

  // remove_callbacks_for_rollback_to will remove callbacks from back to front
  // until callbacks smaller or equal than the seq_no. It will remove both
  // callbacks and data by calling rollback_callback. For synced callback we need
  // calculate checksum and for unsynced one we need remove them.
  int remove_callbacks_for_rollback_to(const transaction::ObTxSEQ to_seq,
                                       const transaction::ObTxSEQ from_seq,
                                       const share::SCN replay_scn);

  // reverse_search_callback_by_seq_no will search callback from back to front
  // until callbacks smaller or equal than the seq_no
  int reverse_search_callback_by_seq_no(const transaction::ObTxSEQ seq_no, ObITransCallback *search_res);

  // get_memtable_key_arr_w_timeout get all memtable key until timeout
  int get_memtable_key_arr_w_timeout(transaction::ObMemtableKeyArray &memtable_key_arr);

  // clean_unlog_callbacks will remove all unlogged callbacks. Which is called
  // when switch to follower forcely.
  int clean_unlog_callbacks(int64_t &removed_cnt, common::ObFunction<void()> &before_remove);
  int fill_log(ObITransCallback* log_cursor, ObTxFillRedoCtx &ctx, ObITxFillRedoFunctor &functor);
  int submit_log_succ(const ObCallbackScope &callbacks);
  int sync_log_succ(const share::SCN scn, int64_t sync_cnt);
  // sync_log_fail will remove all callbacks that not sync successfully. Which
  // is called when callback is on failure.
  int sync_log_fail(const ObCallbackScope &callbacks, const share::SCN scn, int64_t &removed_cnt);

  // tx_calc_checksum_before_scn will calculate checksum during execution. It will
  // remember the intermediate results for final result.
  int tx_calc_checksum_before_scn(const share::SCN scn);

  // tx_calc_checksum_all will calculate checksum when tx end. Finally it will set
  // checksum_scn to INT64_MAX and never allow more checksum calculation.
  int tx_calc_checksum_all();

  // tx_commit will commit all callbacks. And it will let the data know it has
  // been durable. For example, fulfill the version and state into tnode for txn
  // row callback.
  int tx_commit();

  // tx_commit will abort all callbacks. And it will clean the data on it. For
  // example, we remove the tnode for txn row callback.
  int tx_abort();

  // tx_elr_preparing will elr prepare all callbacks. And it will release the
  // lock after proposing the commit log and even before the commit log
  // successfully synced for single ls txn.
  int tx_elr_preparing();

  // tx_print_callback will simply print all calbacks.
  int tx_print_callback();

  // dump stat info to buffer for display
  int get_stat_for_display(ObTxCallbackListStat &stat) const;

  // when replay_succ, advance sync_scn, allow fast commit and calc checksum
  int replay_succ(const share::SCN scn);

  // replay_fail will rollback all redo in a single log according to
  // scn
  int replay_fail(const share::SCN scn, const bool serial_replay);

  // traversal to find and break
  bool find(ObITxCallbackFinder &func);

  // is logging blocked: test current list can fill log
  bool is_logging_blocked() const;
private:
  union LockState {
    LockState() : v_(0) {}
    uint8_t v_;
    struct {
      bool APPEND_LOCKED_: 1;
      bool ITERATE_LOCKED_: 1;
    };
    bool is_locked() const { return v_ != 0; }
  };
  enum class LOCK_MODE {
    LOCK_ITERATE = 1,
    LOCK_APPEND = 2,
    LOCK_ALL = 3,
    TRY_LOCK_ITERATE = 4,
    TRY_LOCK_APPEND = 5,
  };
  struct LockGuard {
    LockGuard(const ObTxCallbackList &host, const LOCK_MODE m, ObTimeGuard *tg = NULL);
    ~LockGuard();
    bool is_locked() const { return state_.is_locked(); }
    union LockState state_;
    const ObTxCallbackList &host_;
  private:
    void lock_append_(const bool try_lock);
    void lock_iterate_(const bool try_lock);
  };
  friend class LockGuard;
  bool is_append_only_() const;
private:
  int callback_(ObITxCallbackFunctor &func,
                const LockState lock_state);
  int callback_(ObITxCallbackFunctor &functor,
                const ObCallbackScope &callbacks,
                const LockState lock_state);
  int callback_(ObITxCallbackFunctor &func,
                ObITransCallback *start,
                ObITransCallback *end,
                const LockState lock_state);
  int64_t calc_need_remove_count_for_fast_commit_();
  void ensure_checksum_(const share::SCN scn);
  bool is_skip_checksum_() const;
public:
  ObITransCallback *get_guard() { return &head_; }
  ObITransCallback *get_tail() { return head_.get_prev(); }
  ObITransCallback *get_log_cursor() const { return log_cursor_; }
  int64_t get_log_epoch() const;
  share::SCN get_sync_scn() const { return sync_scn_; }
  bool check_all_redo_flushed(const bool quite = true) const;
  common::ObByteLock *try_lock_log()
  {
    return log_latch_.try_lock() ? &log_latch_ : NULL;
  }
  bool empty() const { return head_.get_next() == &head_; }
  int64_t get_appended() const { return appended_; }
  int64_t get_length() const { return length_; }
  int64_t get_logged() const { return logged_; }
  int64_t get_synced() const { return synced_; }
  int64_t get_removed() const { return removed_; }
  int64_t get_unlog_removed() const { return unlog_removed_; }
  int64_t get_branch_removed() const { return branch_removed_; }
  uint64_t get_checksum() const { return checksum_; }
  int64_t get_tmp_checksum() const { return tmp_checksum_; }
  share::SCN get_checksum_scn() const { return checksum_scn_; }
  void get_checksum_and_scn(uint64_t &checksum, share::SCN &checksum_scn);
  void update_checksum(const uint64_t checksum, const share::SCN checksum_scn);
  void inc_update_sync_scn(const share::SCN scn);
  transaction::ObPartTransCtx *get_trans_ctx() const;
  bool pending_log_too_large(const int64_t limit) const
  {
    return ATOMIC_LOAD(&data_size_) - ATOMIC_LOAD(&logged_data_size_) > limit;
  }
  // *NOTICE* this _only_ account MvccRowCallback on memtable
  bool has_pending_log() const {
    return ATOMIC_LOAD(&data_size_) - ATOMIC_LOAD(&logged_data_size_) > 0;
  }
  int64_t get_pending_log_size() const {
    return data_size_ - logged_data_size_;
  }
  int64_t get_logged_data_size() const {
    return logged_data_size_;
  }
  DECLARE_TO_STRING;
private:
  const int16_t id_;
  // callback list sentinel
  ObITransCallback head_;
  ObITransCallback *log_cursor_;
  ObITransCallback *parallel_start_pos_;
  int64_t length_;
  // stats
  int64_t appended_;
  int64_t logged_;
  int64_t synced_;
  int64_t removed_;
  int64_t unlog_removed_;
  int64_t branch_removed_;
  int64_t data_size_;
  int64_t logged_data_size_;
  // the max scn of synced callback
  // fast commit remove callback should not cross this
  share::SCN sync_scn_;
  /*
   * transaction checksum calculation
   * A checksum would be calculated when a transaction commits to ensure data consistency.
   * There are 3 conditions:
   * 1. When commits a transaction, iterate all the callback and calculate the checksum;
   * 2. When memtable dump occurs, the callbacks before last_replay_log_id would be traversed
   *    to calculate checksum;
   * 3. When rollback, the callbacks before max_durable_scn should be calculated, to ensure
   *    the checksum calculated due to memtable dump on leader also exists on follower.
   *
   * checksum_scn_ is the log timestamp that is currently synchronizing, bach_checksum_
   * indicates the data which hasn't been calculated, checksum_ is is the currently
   * calculated checksum(checksum supports increment calculation)
   *
   * Notice, apart from 1, the other two calculation should increment
   * checksum_scn_ by 1 to avoid duplicate calculation.
   */
  TxChecksum batch_checksum_;
  share::SCN checksum_scn_;
  uint64_t checksum_;
  uint64_t tmp_checksum_;
  ObTransCallbackMgr &callback_mgr_;
  // used to serialize append callback to list tail
  mutable common::ObByteLock append_latch_;
  // used to serialize fill and flush log of this list
  mutable common::ObByteLock log_latch_;
  // used to serialize operates on synced callbacks
  mutable common::ObByteLock iter_synced_latch_;
  DISALLOW_COPY_AND_ASSIGN(ObTxCallbackList);
};

} // memtable
} // oceanbase


#endif // OCEANBASE_STORAGE_MEMTABLE_MVCC_OB_TX_CALLBACK_LIST
