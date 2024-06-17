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
#include "storage/memtable/mvcc/ob_mvcc_define.h"

namespace oceanbase
{
namespace storage
{
class ObRowState;
}
namespace memtable
{

static const uint8_t NDT_NORMAL = 0x0;
static const uint8_t NDT_COMPACT = 0x1;
class ObIMvccCtx;
class ObMemtable;
class ObIMemtableCtx;
class ObMemtableKey;
class ObMvccRowCallback;

#define ATOMIC_ADD_TAG(tag)                           \
  while (true) {                                      \
    const uint8_t old = ATOMIC_LOAD(&(flag_));        \
    const uint8_t tmp = (old | (tag));                \
    if (ATOMIC_BCAS(&(flag_), old, tmp)) {            \
      break;                                          \
    }                                                 \
  }

#define ATOMIC_SUB_TAG(tag)                     \
  while (true) {                                \
    const uint8_t old = ATOMIC_LOAD(&(flag_));  \
    const uint8_t tmp = (old & (~(tag)));       \
    if (ATOMIC_BCAS(&(flag_), old, tmp)) {      \
      break;                                    \
    }                                           \
  }

// ObMvccTransNode is the multi-version data used for mvcc and stored on
// memtable. It only saves updated columns for write and write by aggregating tx
// nodes to data contains all columns.
struct ObMvccTransNode
{
public:
  ObMvccTransNode()
  : tx_id_(),
    trans_version_(share::SCN::min_scn()),
    scn_(share::SCN::max_scn()),
    seq_no_(),
    tx_end_scn_(share::SCN::max_scn()),
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
  share::SCN trans_version_;
  share::SCN scn_;
  transaction::ObTxSEQ seq_no_;
  share::SCN tx_end_scn_;
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
  void trans_commit(const share::SCN commit_version, const share::SCN tx_end_scn);
  // set aborted flag
  void trans_abort(const share::SCN tx_end_scn);

  // remove the callback
  void remove_callback();

  // ===================== ObMvccTransNode Tx Node Meta =====================
  // ObMvccRow records snapshot_version_barrier to detect unexpected concurrency
  // control behaviors. The snapshot_version_barrier means the snapshot of the
  // latest read operation, and if a commit version appears after the read
  // operation with the commit version smaller than the snapshot version, we
  // should report the unexpected bahavior.
  void set_safe_read_barrier(const bool is_weak_consistent_read);
  void clear_safe_read_barrier();
  bool is_safe_read_barrier() const;
  void set_snapshot_version_barrier(const share::SCN version,
                                    const int64_t flag);
  void get_snapshot_version_barrier(int64_t &version, int64_t &flag);

  // ===================== ObMvccTransNode Flag Interface =====================
  OB_INLINE void set_committed()
  {
    ATOMIC_ADD_TAG(F_COMMITTED);
  }
  OB_INLINE bool is_committed() const
  {
    return ATOMIC_LOAD(&flag_) & F_COMMITTED;
  }
  OB_INLINE void set_elr()
  {
    ATOMIC_ADD_TAG(F_ELR);
  }
  OB_INLINE bool is_elr() const
  {
    return ATOMIC_LOAD(&flag_) & F_ELR;
  }
  OB_INLINE void set_aborted()
  {
    ATOMIC_ADD_TAG(F_ABORTED);
  }
  OB_INLINE void clear_aborted()
  {
    ATOMIC_SUB_TAG(F_ABORTED);
  }
  OB_INLINE bool is_aborted() const
  {
    return ATOMIC_LOAD(&flag_) & F_ABORTED;
  }
  OB_INLINE void set_delayed_cleanout(const bool delayed_cleanout)
  {
    if (OB_LIKELY(delayed_cleanout)) {
      ATOMIC_ADD_TAG(F_DELAYED_CLEANOUT);
    } else {
      ATOMIC_SUB_TAG(F_DELAYED_CLEANOUT);
    }
  }
  OB_INLINE bool is_delayed_cleanout() const
  {
    return ATOMIC_LOAD(&flag_) & F_DELAYED_CLEANOUT;
  }

  // ===================== ObMvccTransNode Setter/Getter =====================
  blocksstable::ObDmlFlag get_dml_flag() const;
  int64_t get_data_size() const;
  int fill_trans_version(const share::SCN version);
  int fill_scn(const share::SCN scn);
  void get_trans_id_and_seq_no(transaction::ObTransID &trans_id, transaction::ObTxSEQ &seq_no);
  transaction::ObTxSEQ get_seq_no() const { return seq_no_; }
  transaction::ObTransID get_tx_id() const { return tx_id_; }
  void set_seq_no(const transaction::ObTxSEQ seq_no) { seq_no_ = seq_no; }
  int is_lock_node(bool &is_lock) const;
  int64_t to_string(char *buf, const int64_t buf_len) const;
  void set_tx_end_scn(const share::SCN tx_end_scn)
  {
    if (share::SCN::max_scn() != tx_end_scn) {
      tx_end_scn_.atomic_store(tx_end_scn);
    }
  }
  share::SCN get_tx_end_scn() const { return tx_end_scn_.atomic_load(); }
  share::SCN get_tx_version() const { return trans_version_.atomic_load(); }
  share::SCN get_scn() const { return scn_.atomic_load(); }

private:
  // the row flag of the mvcc tx node
  static const uint8_t F_INIT;
  static const uint8_t F_WEAK_CONSISTENT_READ_BARRIER;
  static const uint8_t F_STRONG_CONSISTENT_READ_BARRIER;
  static const uint8_t F_COMMITTED;
  static const uint8_t F_ELR;
  static const uint8_t F_ABORTED;
  static const uint8_t F_DELAYED_CLEANOUT;
  static const uint8_t F_MUTEX;

public:
  // the snapshot flag of the snapshot version barrier
  static const int64_t NORMAL_READ_BIT =              0x0L;
  static const int64_t WEAK_READ_BIT =                0x1L << 62;
  static const int64_t COMPACT_READ_BIT =             0x2L << 62;
  static const int64_t SNAPSHOT_VERSION_BARRIER_BIT = 0x3L << 62;
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
  static const uint8_t F_LOWER_LOCK_SCANED = 0x8;

  static const int64_t NODE_SIZE_UNIT = 1024;
  static const int64_t WARN_WAIT_LOCK_TIME = 1 *1000 * 1000;
  static const int64_t WARN_TIME_US = 10 * 1000 * 1000;
  static const int64_t LOG_INTERVAL = 1 * 1000 * 1000;

  //when the number of nodes visited before finding the right insert position exceeds INDEX_TRIGGER_LENGTH,
  //index will be constructed and used
  static const int64_t INDEX_TRIGGER_COUNT = 500;

  // Spin lock that protects row data.
  ObRowLatch latch_;
  uint8_t flag_;
  blocksstable::ObDmlFlag first_dml_flag_;
  blocksstable::ObDmlFlag last_dml_flag_;
  int32_t update_since_compact_;

  int64_t total_trans_node_cnt_;
  int64_t latest_compact_ts_;
  int64_t last_compact_cnt_;
  share::SCN max_trans_version_;
  share::SCN max_elr_trans_version_;
  share::SCN max_modify_scn_;
  share::SCN min_modify_scn_;
  transaction::ObTransID max_trans_id_;
  transaction::ObTransID max_elr_trans_id_;
  ObMvccTransNode *list_head_;
  ObMvccTransNode *latest_compact_node_;
  ObMvccRowIndex *index_;

  ObMvccRow()
  {
    STATIC_ASSERT(sizeof(ObMvccRow) <= 120, "Size of ObMvccRow Overflow.");
    reset();
  }
  void reset();

  // ===================== ObMvccRow Operation Interface =====================
  // mvcc_write resolves the write write conflict on the key
  // ctx is the write txn's context, whose tx_id, snapshot_version and sql_sequence is necessary for the concurrency control
  // node is the new data for write operation
  // has_insert returns whether node is inserted into the ObMvccRow
  // is_new_locked returns whether node represents the first lock for the operation
  // conflict_tx_id if write failed this field indicate the txn-id which hold the lock of current row
  int mvcc_write(storage::ObStoreCtx &ctx,
                 const transaction::ObTxSnapshot &snapshot,
                 ObMvccTransNode &node,
                 ObMvccWriteResult &res);

  // mvcc_replay replay the tx node into the row
  // ctx is the write txn's context, whose scn is necessary for locating the node
  // node is the new data for replay operation
  /* int mvcc_replay(ObIMemtableCtx &ctx, */
  /*                 ObMvccTransNode &node); */

  // mvcc_replay undo the newest write operation when encountering errors
  void mvcc_undo();

  // check_row_locked check whether row is locked and returns the corresponding information
  // key is the row key for lock
  // ctx is the write txn's context, currently the tx_table is the only required field
  // lock_state is the check's result
  int check_row_locked(ObMvccAccessCtx &ctx,
                       storage::ObStoreRowLockState &lock_state,
                       storage::ObRowState &row_state);

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
                  const share::SCN snapshot_version,
                  common::ObIAllocator *node_alloc);

  int elr(const transaction::ObTransID &tx_id,
          const share::SCN elr_commit_version,
          const ObTabletID &tablet_id,
          const ObMemtableKey* key);

  // commit the tx node and update the row meta.
  // the meta neccessary for update is
  // - max_trans_version
  // - max_elr_trans_version
  // - first_dml
  // - last_dml
  int trans_commit(const share::SCN commit_version,
                   ObMvccTransNode &node);

  // remove_callback remove the tx node in the row
  int remove_callback(ObMvccRowCallback &cb);
  // wakeup those blocking to acquire ownership of this row to write
  int wakeup_waiter(const ObTabletID &tablet_id, const ObMemtableKey &key);

  // ===================== ObMvccRow Checker Interface =====================
  // is_transaction_set_violation check the tsc problem for the row
  bool is_transaction_set_violation(const share::SCN snapshot_version);

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
  void update_dml_flag_(const blocksstable::ObDmlFlag flag, const share::SCN modify_count);
  blocksstable::ObDmlFlag get_first_dml_flag() const { return first_dml_flag_; }

  // max_trans_version/max_elr_trans_version is the max (elr) version on the row
  share::SCN get_max_trans_version() const;
  int64_t get_max_trans_id() const { return max_trans_id_; }
  void update_max_trans_version(const share::SCN max_trans_version,
                                const transaction::ObTransID &tx_id);
  void update_max_elr_trans_version(const share::SCN max_trans_version,
                                    const transaction::ObTransID &tx_id);
  int64_t get_total_trans_node_cnt() const { return total_trans_node_cnt_; }
  int64_t get_last_compact_cnt() const { return last_compact_cnt_; }
  // ===================== ObMvccRow Flag Interface =====================
  OB_INLINE bool is_btree_indexed() const
  {
    return ATOMIC_LOAD(&flag_) & F_BTREE_INDEX;
  }
  OB_INLINE void set_btree_indexed()
  {
    ATOMIC_ADD_TAG(F_BTREE_INDEX);
  }
  OB_INLINE void clear_btree_indexed()
  {
    ATOMIC_SUB_TAG(F_BTREE_INDEX);
  }
  OB_INLINE void set_hash_indexed()
  {
    ATOMIC_ADD_TAG(F_HASH_INDEX);
  }
  OB_INLINE bool is_lower_lock_scaned() const
  {
    return ATOMIC_LOAD(&flag_) & F_LOWER_LOCK_SCANED;
  }
  OB_INLINE void set_lower_lock_scaned()
  {
    ATOMIC_ADD_TAG(F_LOWER_LOCK_SCANED);
  }
  // ===================== ObMvccRow Helper Function =====================
  int64_t to_string(char *buf, const int64_t buf_len) const;
  int64_t to_string(char *buf, const int64_t buf_len, const bool verbose) const;
  void print_row();

  // ===================== ObMvccRow Private Function =====================
  int mvcc_write_(storage::ObStoreCtx &ctx,
                  ObMvccTransNode &node,
                  const transaction::ObTxSnapshot &snapshot,
                  ObMvccWriteResult &res);

  // ===================== ObMvccRow Protection Code =====================
  // check double insert
  int check_double_insert_(const share::SCN snapshot_version,
                           ObMvccTransNode &node,
                           ObMvccTransNode *prev);
};

}
}

#endif //OCEANBASE_MEMTABLE_MVCC_OB_MVCC_ROW_
