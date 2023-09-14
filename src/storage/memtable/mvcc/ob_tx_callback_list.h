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

class ObTxCallbackList
{
public:
  ObTxCallbackList(ObTransCallbackMgr &callback_mgr);
  ~ObTxCallbackList();
  void reset();

  // append_callback will append your callback into the callback list
  int append_callback(ObITransCallback *callback, const bool for_replay);

  // concat_callbacks will append all callbacks in other into itself and reset
  // other. And it will return the concat number during concat_callbacks.
  int64_t concat_callbacks(ObTxCallbackList &other);

  // remove_callbacks_for_fast_commit will remove all callbacks according to the
  // parameter _fast_commit_callback_count. It will only remove callbacks
  // without removing data by calling checkpoint_callback. So user need
  // implement lazy callback for the correctness. What's more, it will calculate
  // checksum when removing. Finally it returns meet_generate_cursor if you remove
  // the callbacks that generate_cursor is pointing to.
  int remove_callbacks_for_fast_commit(const ObITransCallback *generate_cursor,
                                       bool &meet_generate_cursor);

  // remove_callbacks_for_remove_memtable will remove all callbacks that is
  // belonged to the specified memtable sets. It will only remove callbacks
  // without removing data by calling checkpoint_callback. So user need to
  // implement lazy callback for the correctness. And user need guarantee all
  // callbacks belonged to the memtable sets must be synced before removing.
  // What's more, it will calculate checksum when removing.
  int remove_callbacks_for_remove_memtable(
    const memtable::ObMemtableSet *memtable_set,
    const share::SCN max_applied_scn);

  // remove_callbacks_for_rollback_to will remove callbacks from back to front
  // until callbacks smaller or equal than the seq_no. It will remove both
  // callbacks and data by calling rollback_callback. For synced callback we need
  // calculate checksum and for unsynced one we need remove them.
  int remove_callbacks_for_rollback_to(const transaction::ObTxSEQ to_seq_no);

  // reverse_search_callback_by_seq_no will search callback from back to front
  // until callbacks smaller or equal than the seq_no
  int reverse_search_callback_by_seq_no(const transaction::ObTxSEQ seq_no, ObITransCallback *search_res);

  // get_memtable_key_arr_w_timeout get all memtable key until timeout
  int get_memtable_key_arr_w_timeout(transaction::ObMemtableKeyArray &memtable_key_arr);

  // clean_unlog_callbacks will remove all unlogged callbacks. Which is called
  // when switch to follower forcely.
  int clean_unlog_callbacks(int64_t &removed_cnt);

  // sync_log_fail will remove all callbacks that not sync successfully. Which
  // is called when callback is on failure.
  int sync_log_fail(const ObCallbackScope &callbacks, int64_t &removed_cnt);

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

  // replay_fail will rollback all redo in a single log according to
  // scn
  int replay_fail(const share::SCN scn);

private:
  int callback_(ObITxCallbackFunctor &func);
  int callback_(ObITxCallbackFunctor &functor,
                const ObCallbackScope &callbacks);
  int callback_(ObITxCallbackFunctor &func,
                ObITransCallback *start,
                ObITransCallback *end);
  int64_t calc_need_remove_count_for_fast_commit_();
  void ensure_checksum_(const share::SCN scn);
public:
  ObITransCallback *get_guard() { return &head_; }
  ObITransCallback *get_tail() { return head_.get_prev(); }
  bool empty() const { return head_.get_next() == &head_; }
  int64_t get_length() const { return length_; }
  int64_t get_checksum() const { return checksum_; }
  int64_t get_tmp_checksum() const { return tmp_checksum_; }
  share::SCN get_checksum_scn() const { return checksum_scn_; }
  void get_checksum_and_scn(uint64_t &checksum, share::SCN &checksum_scn);
  void update_checksum(const uint64_t checksum, const share::SCN checksum_scn);
  transaction::ObPartTransCtx *get_trans_ctx() const;

  DECLARE_TO_STRING;

private:
  // callback list sentinel
  ObITransCallback head_;
  int64_t length_;
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
  common::ObBatchChecksum batch_checksum_;
  share::SCN checksum_scn_;
  uint64_t checksum_;
  uint64_t tmp_checksum_;

  ObTransCallbackMgr &callback_mgr_;
  common::ObByteLock latch_;
  DISALLOW_COPY_AND_ASSIGN(ObTxCallbackList);
};

} // memtable
} // oceanbase


#endif // OCEANBASE_STORAGE_MEMTABLE_MVCC_OB_TX_CALLBACK_LIST
