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

namespace oceanbase
{
namespace memtable
{

class ObMvccTransNode;
class ObMemtableData;
class ObRowData;

// Arguments for building tx node
struct ObTxNodeArg
{
  // trans id
  transaction::ObTransID tx_id_;
  // data_ is the new row of the modifiction
  const ObMemtableData *data_;
  // old_row_ is the old row of the modificattion
  // NB: It is only used for liboblog
  const ObRowData *old_row_;
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
  // scn_ is thee log ts of the redo log
  share::SCN scn_;
  int64_t column_cnt_;

  TO_STRING_KV(K_(tx_id),
               KP_(data),
               KP_(old_row),
               K_(modify_count),
               K_(acc_checksum),
               K_(memstore_version),
               K_(seq_no),
               K_(scn),
               K_(column_cnt));

  // Constructor for leader
  ObTxNodeArg(const transaction::ObTransID tx_id,
              const ObMemtableData *data,
              const ObRowData *old_row,
              const int64_t memstore_version,
              const transaction::ObTxSEQ seq_no,
              const int64_t column_cnt)
    : tx_id_(tx_id),
    data_(data),
    old_row_(old_row),
    modify_count_(UINT32_MAX),
    acc_checksum_(0),
    memstore_version_(memstore_version),
    seq_no_(seq_no),
    scn_(share::SCN::max_scn()),
    column_cnt_(column_cnt) {}

  // Constructor for follower
  ObTxNodeArg(const transaction::ObTransID tx_id,
              const ObMemtableData *data,
              const ObRowData *old_row,
              const int64_t memstore_version,
              const transaction::ObTxSEQ seq_no,
              const uint32_t modify_count,
              const uint32_t acc_checksum,
              const share::SCN scn,
              const int64_t column_cnt)
    : tx_id_(tx_id),
    data_(data),
    old_row_(old_row),
    modify_count_(modify_count),
    acc_checksum_(acc_checksum),
    memstore_version_(memstore_version),
    seq_no_(seq_no),
    scn_(scn),
    column_cnt_(column_cnt) {}

  void reset() {
    tx_id_.reset();
    data_ = NULL;
    old_row_ = NULL;
    modify_count_ = 0;
    acc_checksum_ = 0;
    memstore_version_ = 0;
    seq_no_.reset();
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
  // tx_node_ is the node used for insert, whether it is inserted is decided by
  // has_insert()
  ObMvccTransNode *tx_node_;
  // is_checked_ is used to tell lock_rows_on_forzen_stores whether sequence_set_violation has finished its check
  bool is_checked_;

  TO_STRING_KV(K_(can_insert),
               K_(need_insert),
               K_(is_new_locked),
               K_(is_mvcc_undo),
               K_(lock_state),
               K_(is_checked),
               KPC_(tx_node));

  ObMvccWriteResult()
    : can_insert_(false),
    need_insert_(false),
    is_new_locked_(false),
    is_mvcc_undo_(false),
    lock_state_(),
    tx_node_(NULL),
    is_checked_(false) {}

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
    tx_node_ = NULL;
    is_checked_ = false;
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


} // namespace memtable
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_MEMTABLE_MVCC_OB_MVCC_DEFINE
