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

#ifndef OCEANBASE_STORAGE_MEMTABLE_MVCC_OB_MVCC_DEFINE
#define OCEANBASE_STORAGE_MEMTABLE_MVCC_OB_MVCC_DEFINE
#include "share/ob_define.h"
#include "storage/ob_i_store.h"
#include "storage/memtable/ob_memtable_key.h"
#include "storage/memtable/mvcc/ob_row_data.h"
#include "storage/memtable/mvcc/ob_mvcc_row.h"

namespace oceanbase
{
namespace memtable
{

class ObMemtableData;
class ObMvccRowCallback;

// Arguments for building tx node
struct ObTxNodeArg
{
  // trans id
  transaction::ObTransID tx_id_;
  // data_ is the new row of the modifiction
  const ObMemtableData *data_;
  // old_row_ is the old row of the modificattion
  // NB: It is only used for liboblog
  ObRowData old_row_;
  // modify_count_ is used for txn checksum now
  // NB: It is began with 0
  uint32_t modify_count_;
  // acc_checksum_ is usd for row checksum now
  uint32_t acc_checksum_;
  // memstore_version_ is the memstore timestamp
  // of the memtable. It is used for debug.
  int64_t memstore_version_;
  // seq_no_ is the sequence no of the executing sql
  transaction::ObTxSEQ seq_no_;
  // parallel write epoch no
  int64_t write_epoch_;
  // scn_ is thee log ts of the redo log
  share::SCN scn_;
  // colume_cnt_ is the column count of the insert row
  int64_t column_cnt_;

  TO_STRING_KV(K_(tx_id),
               KP_(data),
               K_(old_row),
               K_(modify_count),
               K_(acc_checksum),
               K_(memstore_version),
               K_(seq_no),
               K_(write_epoch),
               K_(scn),
               K_(column_cnt));

  ObTxNodeArg()
    : tx_id_(),
    data_(nullptr),
    old_row_(),
    modify_count_(UINT32_MAX),
    acc_checksum_(0),
    memstore_version_(0),
    seq_no_(),
    scn_(share::SCN::max_scn()),
    column_cnt_(0) {}

  // Constructor for leader
  ObTxNodeArg(const transaction::ObTransID tx_id,
              const ObMemtableData *data,
              const ObRowData &old_row,
              const int64_t memstore_version,
              const transaction::ObTxSEQ seq_no,
              const int64_t write_epoch,
              const int64_t column_cnt)
    : tx_id_(tx_id),
    data_(data),
    old_row_(old_row),
    modify_count_(UINT32_MAX),
    acc_checksum_(0),
    memstore_version_(memstore_version),
    seq_no_(seq_no),
    write_epoch_(write_epoch),
    scn_(share::SCN::max_scn()),
    column_cnt_(column_cnt) {}

  // Constructor for follower
  ObTxNodeArg(const transaction::ObTransID tx_id,
              const ObMemtableData *data,
              const int64_t memstore_version,
              const transaction::ObTxSEQ seq_no,
              const uint32_t modify_count,
              const uint32_t acc_checksum,
              const share::SCN scn,
              const int64_t column_cnt)
    : tx_id_(tx_id),
    data_(data),
    old_row_(),
    modify_count_(modify_count),
    acc_checksum_(acc_checksum),
    memstore_version_(memstore_version),
    seq_no_(seq_no),
    write_epoch_(0),
    scn_(scn),
    column_cnt_(column_cnt) {}

  // Setter for leader
  void set(const transaction::ObTransID tx_id,
              const ObMemtableData *data,
              const ObRowData &old_row,
              const int64_t memstore_version,
              const transaction::ObTxSEQ seq_no,
              const int64_t write_epoch,
              const int64_t column_cnt)
  {
    tx_id_ = tx_id;
    data_ = data;
    old_row_ = old_row;
    modify_count_ = UINT32_MAX;
    acc_checksum_ = 0;
    memstore_version_ = memstore_version;
    seq_no_ = seq_no;
    write_epoch_ = write_epoch;
    scn_ = share::SCN::max_scn();
    column_cnt_ = column_cnt;
  }

  // Setter for follower
  void set(const transaction::ObTransID tx_id,
           const ObMemtableData *data,
           const int64_t memstore_version,
           const transaction::ObTxSEQ seq_no,
           const uint32_t modify_count,
           const uint32_t acc_checksum,
           const share::SCN scn,
           const int64_t column_cnt)
  {
    tx_id_ = tx_id;
    data_ = data;
    old_row_.reset();
    modify_count_ = modify_count;
    acc_checksum_ = acc_checksum;
    memstore_version_ = memstore_version;
    seq_no_ = seq_no;
    scn_ = scn;
    column_cnt_ = column_cnt;
  }

  void reset() {
    tx_id_.reset();
    data_ = NULL;
    old_row_.reset();
    modify_count_ = 0;
    acc_checksum_ = 0;
    memstore_version_ = 0;
    seq_no_.reset();
    write_epoch_ = 0;
    scn_ = share::SCN::min_scn();
    column_cnt_ = 0;
  }
};

// mvcc write result for mvcc write
struct ObMvccWriteResult {
  // can_insert_ indicates whether the insert is allowed. (It may be disallowed
  // becasue you are encountering the write-write conflict)
  bool can_insert_;
  // need_insert_ indicates whether the insert is necessary. (It may be
  // unnecessary because the row has already locked when you are locking the
  // row)
  bool need_insert_;
  // is_new_locked_ indicates whether you are locking the row for the first
  // time for your txn(mainly used for deadlock detector and detecting errors)
  bool is_new_locked_;
  // is_mvcc_undo_ indicates whether the tx_node_ is inserted
  bool is_mvcc_undo_;
  // lock_state_ is used for deadlock detector and lock wait mgr
  storage::ObStoreRowLockState lock_state_;
  // tx_callback is one-to-one correspondence with tx_node
  ObMvccRowCallback *tx_callback_;
  // tx_node_ is the node used for insert, whether it is inserted is decided by
  // has_insert()
  ObMvccTransNode *tx_node_;
  // is_checked_ is used to tell lock_rows_on_forzen_stores whether
  // sequence_set_violation has finished its check
  bool is_checked_;
  // value_ is the mvcc row used for insert, whether it is inserted is decided
  // by has_insert(). You need distinguish the mvcc_row_ in lock_state(used for
  // double check in post_lock) and value_ in mvcc_result(used for the follow-up
  // work of mvcc_write)
  ObMvccRow *value_;
  // mtk_ is used for callback registration. We really need an untemporary memtable
  // key reference for callbacks
  ObMemtableKey mtk_;

  TO_STRING_KV(K_(can_insert),
               K_(need_insert),
               K_(is_new_locked),
               K_(is_mvcc_undo),
               K_(lock_state),
               K_(is_checked),
               KP_(tx_callback),
               KPC_(tx_node),
               KPC_(value),
               K_(mtk));

  ObMvccWriteResult()
    : can_insert_(false),
    need_insert_(false),
    is_new_locked_(false),
    is_mvcc_undo_(false),
    lock_state_(),
    tx_callback_(nullptr),
    tx_node_(nullptr),
    is_checked_(false),
    value_(nullptr),
    mtk_() {}

  ObMvccWriteResult(const ObMvccWriteResult& other) {
    can_insert_ = other.can_insert_;
    need_insert_ = other.need_insert_;
    is_new_locked_ = other.is_new_locked_;
    is_mvcc_undo_ = other.is_mvcc_undo_;
    lock_state_ = other.lock_state_;
    is_checked_ = other.is_checked_;
    tx_callback_ = other.tx_callback_;
    tx_node_ = other.tx_node_;
    value_ = other.value_;
    mtk_.encode(other.mtk_);
  }
  void operator=(const ObMvccWriteResult& other) {
    can_insert_ = other.can_insert_;
    need_insert_ = other.need_insert_;
    is_new_locked_ = other.is_new_locked_;
    is_mvcc_undo_ = other.is_mvcc_undo_;
    lock_state_ = other.lock_state_;
    is_checked_ = other.is_checked_;
    tx_callback_ = other.tx_callback_;
    tx_node_ = other.tx_node_;
    value_ = other.value_;
    mtk_.encode(other.mtk_);
  }

  // has_insert indicates whether the insert is succeed
  // It is decided by both can_insert_ and need_insert_
  bool has_insert() const { return can_insert_ && need_insert_ && !is_mvcc_undo_; }

  void reset()
  {
    can_insert_ = false;
    need_insert_ = false;
    is_new_locked_ = false;
    is_mvcc_undo_ = false;
    lock_state_.reset();
    tx_callback_ = NULL;
    tx_node_ = NULL;
    is_checked_ = false;
    value_ = NULL;
    mtk_.reset();
  }
};

// mvcc replay result for mvcc replay
struct ObMvccReplayResult {
public:
  // tx_node_ is the node used for replay
  ObMvccTransNode *tx_node_;

  TO_STRING_KV(KPC_(tx_node));

  ObMvccReplayResult()
  : tx_node_(NULL) {}

  void reset()
  {
    tx_node_ = NULL;
  }
};

// used only for debug during mvcc_write
struct ObMvccWriteDebugInfo {
  ObMvccWriteDebugInfo()
    : memtable_iterate_cnt_(0),
    sstable_iterate_cnt_(0) {}

  int64_t memtable_iterate_cnt_;
  int64_t sstable_iterate_cnt_;
  // TODO(handora.qc): add more debug info if necessary
  // int64_t exist_table_bitmap_;
};

struct ObStoredKV
{
  ObMemtableKey key_;
  ObMvccRow *value_;

  ObStoredKV()
    : key_(),
    value_(nullptr) {}

  void reset() {
    key_.reset();
    value_ = nullptr;
  }

  bool empty() {
    return nullptr == value_;
  }

  void operator=(const ObStoredKV& other) {
    key_.encode(other.key_);
    value_ = other.value_;
  }

  ObStoredKV(const ObStoredKV& other) {
    key_.encode(other.key_);
    value_ = other.value_;
  }

  TO_STRING_KV(K_(key), KPC_(value));
};

static bool is_mvcc_write_related_error_(const int ret_code)
{
  return OB_TRY_LOCK_ROW_CONFLICT == ret_code ||
    OB_TRANSACTION_SET_VIOLATION == ret_code ||
    OB_ERR_PRIMARY_KEY_DUPLICATE == ret_code;
}

static bool is_mvcc_lock_related_error_(const int ret_code)
{
  return OB_TRY_LOCK_ROW_CONFLICT == ret_code ||
    OB_TRANSACTION_SET_VIOLATION == ret_code ||
    OB_ERR_SHARED_LOCK_CONFLICT == ret_code;
}

void memtable_set_injection_sleep();

int memtable_set_injection_error();

} // namespace memtable
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_MEMTABLE_MVCC_OB_MVCC_DEFINE
