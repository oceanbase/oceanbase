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

#ifndef OCEANBASE_MEMTABLE_MVCC_OB_MVCC_
#define OCEANBASE_MEMTABLE_MVCC_OB_MVCC_

#include "storage/blocksstable/ob_datum_row.h"

namespace oceanbase
{
namespace storage {
class ObIMemtable;
};
namespace memtable
{
class ObTransCallbackList;
class ObITransCallbackIterator;
enum class MutatorType;
struct TxChecksum : public common::ObBatchChecksum {
  TxChecksum(): cnt_(0), scn_() {}
  void reset() {
    common::ObBatchChecksum::reset();
    cnt_ = 0;
    scn_.reset();
  }
  int cnt_;
  share::SCN scn_;
  TO_STRING_KV(K_(cnt), K_(scn), "checksum", const_cast<TxChecksum*>(this)->calc());
};
class ObITransCallback
{
  friend class ObTransCallbackList;
  friend class ObITransCallbackIterator;
public:
  ObITransCallback() :
    need_submit_log_(true),
    scn_(share::SCN::max_scn()),
    epoch_(0),
    prev_(NULL),
    next_(NULL) {}
  ObITransCallback(const bool need_submit_log) :
    need_submit_log_(need_submit_log),
    scn_(share::SCN::max_scn()),
    epoch_(0),
    prev_(NULL),
    next_(NULL) {}
  virtual ~ObITransCallback() {}

  virtual bool is_table_lock_callback() const { return false; }
  virtual int merge_memtable_key(transaction::ObMemtableKeyArray &memtable_key_arr)
  { UNUSED(memtable_key_arr); return common::OB_SUCCESS; }
  virtual bool on_memtable(const storage::ObIMemtable * const memtable)
  { UNUSED(memtable); return false; }
  virtual storage::ObIMemtable* get_memtable() const { return nullptr; }
  virtual uint32_t get_freeze_clock() const { return 0; }
  virtual transaction::ObTxSEQ get_seq_no() const { return transaction::ObTxSEQ::INVL(); }
  virtual int del() { return remove(); }
  virtual bool is_need_free() const { return true; }
  void set_scn(const share::SCN scn);
  share::SCN get_scn() const;
  bool is_log_submitted() const { return !scn_.is_max(); }
  void set_epoch(int64_t epoch) { epoch_ = epoch; }
  int64_t get_epoch() const { return epoch_; }
  int before_append_cb(const bool is_replay);
  void after_append_cb(const bool is_replay);
  bool need_submit_log() const { return need_submit_log_; }
  virtual bool is_logging_blocked() const { return false; }
  virtual bool on_frozen_memtable(storage::ObIMemtable *&last_frozen_mt) const { return true; }
  int log_submitted_cb(const share::SCN scn, storage::ObIMemtable *&last_mt);
  int log_sync_fail_cb(const share::SCN scn);
  // interface should be implement by subclasses
  virtual int before_append(const bool is_replay) { return common::OB_SUCCESS; }
  virtual void after_append(const bool is_replay) {}
  virtual int log_submitted(const share::SCN scn, storage::ObIMemtable *&last_mt)
  { UNUSED(scn); return common::OB_SUCCESS; }
  virtual int log_sync_fail(const share::SCN max_committed_scn)
  { return common::OB_SUCCESS; }
  virtual int64_t get_data_size() { return 0; }
  virtual MutatorType get_mutator_type() const; 
  virtual int get_cluster_version(uint64_t &cluster_version) const
  {
    UNUSED(cluster_version);
    return common::OB_SUCCESS;
  }
  virtual blocksstable::ObDmlFlag get_dml_flag() const { return blocksstable::ObDmlFlag::DF_NOT_EXIST; }
  virtual void set_not_calc_checksum(const bool not_calc_checksum) { UNUSED(not_calc_checksum); }
  ObITransCallback *get_next() const { return ATOMIC_LOAD(&next_); }
  ObITransCallback *get_prev() const { return ATOMIC_LOAD(&prev_); }
  void set_next(ObITransCallback *node) { ATOMIC_STORE(&next_, node); }
  void set_prev(ObITransCallback *node) { ATOMIC_STORE(&prev_, node); }
  void append(ObITransCallback *node);

public:
  // trans_commit is called when txn commit. And you need to let the data know
  // it has been durable. For example, we fulfill the version and state into tnode for
  // txn row callback.
  virtual int trans_commit() { return OB_SUCCESS; }

  // trans_abort is called when txn abort. And you need to clean the data on it.
  // For example, we remove the tnode for txn row callback.
  virtual int trans_abort() { return OB_SUCCESS; }

  // calc_checksum is used for checksum verification. If you want to adapt to
  // the checksum system, you need be care of checksum_scn, you should only
  // execution the checksum if checksum_scn is smaller or equal than your
  // scn.
  virtual int calc_checksum(const share::SCN checksum_scn,
                            TxChecksum *checksumer)
  {
    UNUSED(checksum_scn);
    UNUSED(checksumer);
    return OB_SUCCESS;
  }

  // elr_trans_preparing is used for early lock release, if you want to release
  // the lock after proposing the commit log and even before the commit log
  // successfully synced for single ls txn.
  virtual int elr_trans_preparing() { return OB_SUCCESS; }
  // elr_trans_revoke is used to revoke elr after commit failed
  virtual void elr_trans_revoke() { }

  // print_callback is used for debug only, and it is implemented to display
  // your callback.
  virtual int print_callback() { return common::OB_SUCCESS; }

  // checkpoint_callback means remove callbacks without removing the data on it.
  // For example, the lock of table lock or the tx node of the txn should not be
  // removed during the function call. So all you need to do is let the data
  // know the callback has been removed and prepare for the remove of the
  // callback itself.
  //
  // NB: You need notice the data should be checkpointed so all information
  // should be stored somewhere to satisfy the STEAL policy. So the checksum and
  // data should all be saved. What's more, the log itself must be stored. In
  // our implementation, the callbacks be checkpointed must be paxos committed
  // and applied successfully.
  virtual int checkpoint_callback() { return OB_SUCCESS; }

  // rollback_callback means remove callbacks and the data on it. For example,
  // the lock of table lock or the tx node of the txn should all be removed
  // during the function call. So all you need to do is prepare for the remove
  // of both of the callback and data.
  //
  // NB: You need notice the data should be rollbacked so all information should
  // never be readable according to the ATOMICITY policy. While the data may
  // already be saved and the log has been paxos committed, so we need calculate
  // the checksum for these and push them into UNDO_STATUS for our correctness
  // and visibility. In our implementation, the callbacks be rollbacked can be
  // both paxos committed and or not.
  virtual int rollback_callback() { return OB_SUCCESS; }


  VIRTUAL_TO_STRING_KV(KP(this), KP_(prev), KP_(next));
protected:
  int before_append(ObITransCallback *node);
  int remove();
  struct {
    // Identifies whether log has been submitted, and may not been synced
    bool need_submit_log_ : 1;
  };
  share::SCN scn_;
  int64_t epoch_;
public:
  int64_t owner_;
private:
  ObITransCallback *prev_;
  ObITransCallback *next_;
};

}; // namespace memtable
}; // namespace oceanbase

#endif //OCEANBASE_MEMTABLE_MVCC_OB_MVCC_

