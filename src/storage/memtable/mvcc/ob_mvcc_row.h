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

#ifndef OCEANBASE_MEMTABLE_MVCC_OB_MVCC_ROW_
#define OCEANBASE_MEMTABLE_MVCC_OB_MVCC_ROW_


#include "share/ob_define.h"
#include "lib/checksum/ob_crc64.h"
#include "lib/queue/ob_link.h"
#include "lib/lock/ob_latch.h"
#include "storage/ob_i_store.h"
#include "ob_row_latch.h"
#include "storage/memtable/ob_memtable_data.h"
#include "storage/ob_i_store.h"
#include "storage/memtable/mvcc/ob_mvcc_define.h"

namespace oceanbase
{
namespace memtable
{

static const uint8_t NDT_NORMAL = 0x0;
static const uint8_t NDT_COMPACT = 0x1;
class ObIMvccCtx;
class ObMemtable;
class ObIMemtableCtx;
class ObMemtableKey;
class ObMvccRowCallback;

// ObMvccTransNode is the multi-version data used for mvcc and stored on
// memtable. It only saves updated columns for write and write by aggregating tx
// nodes to data contains all columns.
struct ObMvccTransNode
{
public:
  ObMvccTransNode()
  : tx_id_(),
    trans_version_(0),
    log_timestamp_(INT64_MAX),
    seq_no_(0),
    tx_end_log_ts_(INT64_MAX),
    prev_(NULL),
    next_(NULL),
    modify_count_(0),
    acc_checksum_(0),
    version_(0),
    snapshot_version_barrier_(0),
    type_(NDT_NORMAL),
    flag_(0) {}

  ~ObMvccTransNode() {}

  transaction::ObTransID tx_id_;
  int64_t trans_version_;
  int64_t log_timestamp_;
  int64_t seq_no_;
  int64_t tx_end_log_ts_;
  ObMvccTransNode *prev_;
  ObMvccTransNode *next_;
  uint32_t modify_count_;
  uint32_t acc_checksum_;
  int64_t version_;
  int64_t snapshot_version_barrier_;
  uint8_t type_;
  uint8_t flag_;
  char buf_[0];

  // ===================== ObMvccTransNode Operation Interface =====================
  // checksum the tx node into bc
  void checksum(common::ObBatchChecksum &bc) const;

  // calc/verify the tx node checksum
  uint32_t m_cal_acc_checksum(const uint32_t last_acc_checksum) const;
  void cal_acc_checksum(const uint32_t last_acc_checksum);
  int verify_acc_checksum(const uint32_t last_acc_checksum) const;

  // trans_commit/abort commit/abort the tx node
  // fill in the version and set committed flag
  void trans_commit(const int64_t commit_version, const int64_t tx_end_log_ts);
  // set aborted flag
  void trans_abort(const int64_t tx_end_log_ts);

  // remove the callback
  void remove_callback();

  // ===================== ObMvccTransNode Tx Node Meta =====================
  // ObMvccRow records safe_read_barrier and snapshot_version_barrier to detect
  // unexpected behaviors. The safe_read_barrier means the type of the last read
  // operation performed on the row. And the snapshot_version_barrier means the
  // version of the read operation,
  void set_safe_read_barrier(const bool is_weak_consistent_read);
  void clear_safe_read_barrier();
  bool is_safe_read_barrier() const;
  void set_snapshot_version_barrier(const int64_t version);

  // ===================== ObMvccTransNode Flag Interface =====================
  void set_committed();
  bool is_committed() const { return ATOMIC_LOAD(&flag_) & F_COMMITTED; }
  void set_elr();
  bool is_elr() const { return ATOMIC_LOAD(&flag_) & F_ELR; }
  void set_aborted();
  void clear_aborted();
  bool is_aborted() const { return ATOMIC_LOAD(&flag_) & F_ABORTED; }
  void set_delayed_cleanout(const bool delayed_cleanout);
  bool is_delayed_cleanout() const;

  // ===================== ObMvccTransNode Setter/Getter =====================
  blocksstable::ObDmlFlag get_dml_flag() const;
  int fill_trans_version(const int64_t version);
  int fill_log_timestamp(const int64_t log_timestamp);
  void get_trans_id_and_seq_no(transaction::ObTransID &trans_id, int64_t &seq_no);
  int64_t get_seq_no() const { return seq_no_; }
  transaction::ObTransID get_tx_id() const { return tx_id_; }
  void set_seq_no(const int64_t seq_no) { seq_no_ = seq_no; }
  int is_lock_node(bool &is_lock) const;
  int64_t to_string(char *buf, const int64_t buf_len) const;
  void set_tx_end_log_ts(const int64_t tx_end_log_ts)
  {
    if (INT64_MAX != tx_end_log_ts) {
      ATOMIC_STORE(&tx_end_log_ts_, tx_end_log_ts);
    }
  }
  int64_t get_tx_end_log_ts() { return ATOMIC_LOAD(&tx_end_log_ts_); }

private:
  static const uint8_t F_INIT;
  static const uint8_t F_WEAK_CONSISTENT_READ_BARRIER;
  static const uint8_t F_STRONG_CONSISTENT_READ_BARRIER;
  static const uint8_t F_COMMITTED;
  static const uint8_t F_ELR;
  static const uint8_t F_ABORTED;
  static const uint8_t F_DELAYED_CLEANOUT;
  static const uint8_t F_MUTEX;
};

////////////////////////////////////////////////////////////////////////////////////////////////////

// ObMvccRow is the row contains all multi-version tx node for the specified
// key, and all tx node is bidirectional linked and ordered with newest to
// oldest.
struct ObMvccRow
{
  struct ObMvccRowIndex
  {
  public:
    ObMvccRowIndex() : is_empty_(true)
    {
      MEMSET(&replay_locations_, 0, sizeof(replay_locations_));
    }
    ~ObMvccRowIndex() {reset();}
    void reset();
    static bool is_valid_queue_index(const int64_t index);
    ObMvccTransNode *get_index_node(const int64_t index) const;
    void set_index_node(const int64_t index, ObMvccTransNode *node);
  public:
    bool is_empty_;
    ObMvccTransNode *replay_locations_[common::REPLAY_TASK_QUEUE_SIZE];
  };
  static const uint8_t F_INIT = 0x0;
  static const uint8_t F_HASH_INDEX = 0x1;
  static const uint8_t F_BTREE_INDEX = 0x2;
  static const uint8_t F_BTREE_TAG_DEL = 0x4;
  static const uint8_t F_LOWER_LOCK_SCANED = 0x8;
  static const uint8_t F_LOCK_DELAYED_CLEANOUT = 0x10;

  static const int64_t NODE_SIZE_UNIT = 1024;
  static const int64_t WARN_WAIT_LOCK_TIME = 1 *1000 * 1000;
  static const int64_t WARN_TIME_US = 10 * 1000 * 1000;
  static const int64_t LOG_INTERVAL = 1 * 1000 * 1000;

  //when the number of nodes visited before finding the right insert position exceeds INDEX_TRIGGER_LENGTH,
  //index will be constructed and used
  static const int64_t INDEX_TRIGGER_COUNT = 500;

  // Spin lock that protects row data.
  ObRowLatch latch_;
  // Update count since last row compact.
  int32_t update_since_compact_;
  uint8_t flag_;
  blocksstable::ObDmlFlag first_dml_flag_;
  blocksstable::ObDmlFlag last_dml_flag_;
  ObMvccTransNode *list_head_;
  transaction::ObTransID max_trans_id_;
  int64_t max_trans_version_;
  transaction::ObTransID max_elr_trans_id_;
  int64_t max_elr_trans_version_;
  ObMvccTransNode *latest_compact_node_;
  // using for optimizing inserting trans node when replaying
  ObMvccRowIndex *index_;
  int64_t total_trans_node_cnt_;
  int64_t latest_compact_ts_;
  int64_t last_compact_cnt_;
  // TODO(handora.qc): remove it after link all nodes
  uint32_t max_modify_count_;
  uint32_t min_modify_count_;

  ObMvccRow() { reset(); }
  void reset();

  // ===================== ObMvccRow Operation Interface =====================
  // mvcc_write resolves the write write conflict on the key
  // ctx is the write txn's context, whose tx_id, snapshot_version and sql_sequence is necessary for the concurrency control
  // node is the new data for write operation
  // has_insert returns whether node is inserted into the ObMvccRow
  // is_new_locked returns whether node represents the first lock for the operation
  // conflict_tx_id if write failed this field indicate the txn-id which hold the lock of current row
  int mvcc_write(ObIMemtableCtx &ctx,
                 const int64_t snapshot_version,
                 ObMvccTransNode &node,
                 ObMvccWriteResult &res);

  // mvcc_replay replay the tx node into the row
  // ctx is the write txn's context, whose log_ts is necessary for locating the node
  // node is the new data for replay operation
  /* int mvcc_replay(ObIMemtableCtx &ctx, */
  /*                 ObMvccTransNode &node); */

  // mvcc_replay undo the newest write operation when encountering errors
  void mvcc_undo();

  // check_row_locked check whether row is locked and returns the corresponding information
  // key is the row key for lock
  // ctx is the write txn's context, currently the tx_table is the only required field
  // lock_state is the check's result
  int check_row_locked(ObMvccAccessCtx &ctx, storage::ObStoreRowLockState &lock_state);

  // insert_trans_node insert the tx node for replay
  // ctx is the write txn's context
  // node is the node needed for insert
  // allocator is used for alloc row index to accelerate row replay
  // next_node is the next node for inserted node
  int insert_trans_node(ObIMvccCtx &ctx,
                        ObMvccTransNode &node,
                        common::ObIAllocator &allocator,
                        ObMvccTransNode *&next_node);

  // unlink the tx node from ObMvccRow
  // node is the node needed for unlink
  int unlink_trans_node(const ObMvccTransNode &node);

  // compact the row with tx nodes whose commit version is decided and lower than snapshot_version
  // memtable is used to get tx_table when compact
  // snapshot_version is the version for row compact
  // node_alloc is the allocator for compact node allocation
  int row_compact(ObMemtable *memtable,
                  const bool for_replay,
                  const int64_t snapshot_version,
                  common::ObIAllocator *node_alloc);

  int elr(const transaction::ObTransID &tx_id,
          const int64_t elr_commit_version,
          const ObTabletID &tablet_id,
          const ObMemtableKey* key);

  // commit the tx node and update the row meta.
  // the meta neccessary for update is
  // - max_trans_version
  // - max_elr_trans_version
  // - first_dml
  // - last_dml
  int trans_commit(const int64_t commit_version,
                   ObMvccTransNode &node);

  // remove_callback remove the tx node in the row
  int remove_callback(ObMvccRowCallback &cb);
  // wakeup those blocking to acquire ownership of this row to write
  int wakeup_waiter(const ObTabletID &tablet_id, const ObMemtableKey &key);

  // ===================== ObMvccRow Checker Interface =====================
  // is_partial checks whether mvcc row whose version below than version is completed
  // TODO(handora.qc): handle it properly
  bool is_partial(const int64_t version) const;
  // is_del checks whether mvcc row whose version below than version is completed
  // TODO(handora.qc): handle it properly
  bool is_del(const int64_t version) const;
  // is_transaction_set_violation check the tsc problem for the row
  bool is_transaction_set_violation(const int64_t snapshot_version);

  // ===================== ObMvccRow Getter Interface =====================
  // need_compact checks whether the compaction is necessary
  bool need_compact(const bool for_read, const bool for_replay);
  // is_empty checks whether ObMvccRow has no tx node(while the row may be deleted)
  bool is_empty() const { return (NULL == ATOMIC_LOAD(&list_head_)); }
  // get_list_head gets the head tx node
  ObMvccTransNode *get_list_head() const { return ATOMIC_LOAD(&list_head_); }
  // is_valid_replay_queue_index returns whether the index is a valid replay queue
  bool is_valid_replay_queue_index(const int64_t index) const;

  // ======================== ObMvccRow Row Metas ========================
  // first dml and last dml is the importatnt statistics for row estimation
  void update_dml_flag_(blocksstable::ObDmlFlag flag, uint32_t modify_count);
  blocksstable::ObDmlFlag get_first_dml_flag() const { return first_dml_flag_; }

  // max_trans_version/max_elr_trans_version is the max (elr) version on the row
  int64_t get_max_trans_version() const;
  int64_t get_max_trans_id() const { return max_trans_id_; }
  void update_max_trans_version(const int64_t max_trans_version,
                                const transaction::ObTransID &tx_id);
  void update_max_elr_trans_version(const int64_t max_trans_version,
                                    const transaction::ObTransID &tx_id);
  int64_t get_total_trans_node_cnt() const { return total_trans_node_cnt_; }
  int64_t get_last_compact_cnt() const { return last_compact_cnt_; }
  // ===================== ObMvccRow Event Statistic =====================
  void lock_begin(ObIMemtableCtx &ctx) const;
  void mvcc_write_end(ObIMemtableCtx &ctx, int64_t ret) const;

  // ===================== ObMvccRow Flag Interface =====================
  bool is_btree_indexed() { return flag_ & F_BTREE_INDEX; }
  void set_btree_indexed() { flag_ |= F_BTREE_INDEX; }
  void clear_btree_indexed() { flag_ &= static_cast<uint8_t>(~F_BTREE_INDEX); }
  bool is_btree_tag_del() { return flag_ & F_BTREE_TAG_DEL; }
  void set_btree_tag_del() { flag_ |= F_BTREE_TAG_DEL; }
  void clear_btree_tag_del() { flag_ &= static_cast<uint8_t>(~F_BTREE_TAG_DEL); }
  void set_hash_indexed() { flag_ |= F_HASH_INDEX; }
  bool is_lower_lock_scaned() const { return flag_ & F_LOWER_LOCK_SCANED; }
  void set_lower_lock_scaned() { flag_ |= F_LOWER_LOCK_SCANED; }

  // ===================== ObMvccRow Helper Function =====================
  int64_t to_string(char *buf, const int64_t buf_len) const;
  int64_t to_string(char *buf, const int64_t buf_len, const bool verbose) const;
  void print_row();

  // ===================== ObMvccRow Private Function =====================
  int mvcc_write_(ObIMemtableCtx &ctx,
                  ObMvccTransNode &node,
                  const int64_t snapshot_version,
                  ObMvccWriteResult &res);

  // ===================== ObMvccRow Protection Code =====================
  // check double insert
  int check_double_insert_(const int64_t snapshot_version,
                           ObMvccTransNode &node,
                           ObMvccTransNode *prev);
};

}
}

#endif //OCEANBASE_MEMTABLE_MVCC_OB_MVCC_ROW_
