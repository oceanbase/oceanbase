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

#ifndef OCEANBASE_STORAGE_OB_TX_DATA_FUNCTOR
#define OCEANBASE_STORAGE_OB_TX_DATA_FUNCTOR

#include "lib/function/ob_function.h"
#include "storage/tx_table/ob_tx_table_define.h"

namespace oceanbase
{

namespace memtable
{
struct ObMvccTransNode;
struct ObMvccRow;
};
namespace observer
{
struct VirtualTxDataRow;
}

namespace storage
{

class ObReCheckOp
{
public:
  virtual bool operator()() = 0;
  int64_t to_string(char* buf, const int64_t buf_len) const { return 0; }
};

// ReCheck whether tx node is valid.
// It may be the case that the tx hasn't written any log and exists. And before
// the tx exists, the check_with_tx_state catches the tnode and does not find
// any ctx in tx_ctx_table and tx_data_table. So we need recheck the state to
// prevent the incorrect error reporting
class ObReCheckTxNodeForLockForReadOperation : public ObReCheckOp
{
public:
  ObReCheckTxNodeForLockForReadOperation(memtable::ObMvccTransNode &tnode,
                                         bool &can_read,
                                         share::SCN &trans_version)
    : tnode_(tnode),
    can_read_(can_read),
    trans_version_(trans_version) {}
  virtual bool operator()() override;
  DECLARE_TO_STRING;
private:
  memtable::ObMvccTransNode &tnode_;
  bool &can_read_;
  share::SCN &trans_version_;
};

class ObReCheckNothingOperation : public ObReCheckOp
{
public:
  ObReCheckNothingOperation() {}
  virtual bool operator()() override;
  TO_STRING_KV("ReCheckOperation", "ReCheckNothing");
};

class ObCleanoutOp
{
public:
  virtual int operator()(const ObTxDataCheckData &tx_data) = 0;
  virtual bool need_cleanout() const { return false; }
  int64_t to_string(char* buf, const int64_t buf_len) const { return 0; }
};

class ObCleanoutTxNodeOperation : public ObCleanoutOp
{
public:
  ObCleanoutTxNodeOperation(memtable::ObMvccRow &value,
                            memtable::ObMvccTransNode &tnode,
                            const bool need_row_latch)
    : value_(value),
    tnode_(tnode),
    need_row_latch_(need_row_latch) {}
  virtual int operator()(const ObTxDataCheckData &tx_data) override;
  virtual bool need_cleanout() const override;
  DECLARE_TO_STRING;
private:
  memtable::ObMvccRow &value_;
  memtable::ObMvccTransNode &tnode_;
  bool need_row_latch_;
};

class ObCleanoutNothingOperation : public ObCleanoutOp
{
public:
  ObCleanoutNothingOperation() {}
  virtual int operator()(const ObTxDataCheckData &tx_data) override;
  TO_STRING_KV("CleanoutOperation", "CleanoutNothing");
};

// NB: The functor is only used during minor merge check
// whether txn DATA_TRANS_ID with SQL_SEQUENCE is needed during minor merge
// (SQL_SEQUENCE may be unreadable for txn or stmt rollback) return whether it
// is needed during minor merge
class CheckSqlSequenceCanReadFunctor : public ObITxDataCheckFunctor
{
public:
  CheckSqlSequenceCanReadFunctor(const transaction::ObTxSEQ &sql_sequence, bool &can_read)
    : sql_sequence_(sql_sequence), can_read_(can_read) {}
  virtual int operator()(const ObTxData &tx_data, ObTxCCCtx *tx_cc_ctx = nullptr) override;
  INHERIT_TO_STRING_KV("ObITxDataCheckFunctor", ObITxDataCheckFunctor,
                       K(sql_sequence_), K(can_read_));
public:
  const transaction::ObTxSEQ &sql_sequence_;
  bool &can_read_;
};

// check whether the row key is locked by txn DATA_TRANS_ID
// return the existence of the rowkey and the commit version if committed
//
// return OB_SUCCESS if there is no lock or locked by myself.
// return OB_ERR_EXCLUSIVE_LOCK_CONFLICT or OB_TRY_LOCK_ROW_CONFLICT if locked by others.
// is_locked, is the status of lock. true means locked by myself or others
// lock_trans_id, is the trans id of who locked the row.
class CheckRowLockedFunctor : public ObITxDataCheckFunctor
{
public:
  CheckRowLockedFunctor(const transaction::ObTransID &read_tx_id,
                        const transaction::ObTransID &data_tx_id,
                        const transaction::ObTxSEQ &sql_sequence,
                        ObStoreRowLockState &lock_state)
    : read_tx_id_(read_tx_id),
      data_tx_id_(data_tx_id),
      sql_sequence_(sql_sequence),
      lock_state_(lock_state) {}
  virtual int operator()(const ObTxData &tx_data, ObTxCCCtx *tx_cc_ctx = nullptr) override;
  INHERIT_TO_STRING_KV("ObITxDataCheckFunctor", ObITxDataCheckFunctor,
                       K(read_tx_id_), K(data_tx_id_), K(sql_sequence_),
                       K(lock_state_));
public:
  const transaction::ObTransID &read_tx_id_;
  const transaction::ObTransID &data_tx_id_;
  const transaction::ObTxSEQ &sql_sequence_;
  ObStoreRowLockState &lock_state_;
};

// fetch the state of txn DATA_TRANS_ID when replaying to SCN
// the requirement can be seen from
// return the txn state and commit version if committed, INT64_MAX if running
// and 0 if rollbacked when replaying to LOG_ID
class GetTxStateWithSCNFunctor : public ObITxDataCheckFunctor
{
public:
  GetTxStateWithSCNFunctor(const share::SCN scn,
                           int64_t &state,
                           share::SCN &trans_version)
    : scn_(scn), state_(state), trans_version_(trans_version) {}
  virtual ~GetTxStateWithSCNFunctor() {}
  virtual int operator()(const ObTxData &tx_data, ObTxCCCtx *tx_cc_ctx = nullptr) override;
  INHERIT_TO_STRING_KV("ObITxDataCheckFunctor", ObITxDataCheckFunctor, K(scn_),
                       K(state_), K(trans_version_));
public:
  const share::SCN scn_;
  int64_t &state_;
  share::SCN &trans_version_;
};

// the txn READ_TRANS_ID use SNAPSHOT_VERSION to read the data,
// and check whether the data is locked, readable or unreadable
// by txn DATA_TRANS_ID.
// READ_LATEST is used to check whether read the data belong to
// the same txn
// return whether the data is readable, and corresponding state and version
class LockForReadFunctor : public ObITxDataCheckFunctor
{
public:
  LockForReadFunctor(const transaction::ObLockForReadArg &lock_for_read_arg,
                     bool &can_read,
                     share::SCN &trans_version,
                     const share::ObLSID ls_id,
                     ObCleanoutOp &cleanout_op,
                     ObReCheckOp &recheck_op)
    : lock_for_read_arg_(lock_for_read_arg),
      can_read_(can_read),
      trans_version_(trans_version),
      ls_id_(ls_id),
      cleanout_op_(cleanout_op),
      recheck_op_(recheck_op) {}
  virtual ~LockForReadFunctor() {}
  virtual int operator()(const ObTxData &tx_data, ObTxCCCtx *tx_cc_ctx = nullptr) override;
  virtual bool recheck() override;
  int check_for_standby(const transaction::ObTransID &tx_id);
  INHERIT_TO_STRING_KV("ObITxDataCheckFunctor", ObITxDataCheckFunctor, K(lock_for_read_arg_),
                       K(can_read_), K(trans_version_), K(ls_id_));
private:
  int inner_lock_for_read(const ObTxData &tx_data, ObTxCCCtx *tx_cc_ctx);
  int check_clog_disk_full_();
  int check_gc_handler_();
public:
  const transaction::ObLockForReadArg &lock_for_read_arg_;
  bool &can_read_;
  share::SCN &trans_version_;
  share::ObLSID ls_id_;
  // Cleanout the tx node if necessary
  ObCleanoutOp &cleanout_op_;
  // ReCheck whether tx node is valid.
  // It may be the case that the tx hasn't written any log and exists. And before
  // the tx exists, the check_with_tx_state catches the tnode and does not find
  // any ctx in tx_ctx_table and tx_data_table. So we need recheck the state to
  // prevent the incorrect error reporting
  ObReCheckOp &recheck_op_;
};

class CleanoutTxStateFunctor : public ObITxDataCheckFunctor
{
public:
  CleanoutTxStateFunctor(const transaction::ObTxSEQ seq_no,
                         ObCleanoutOp &op)
    : seq_no_(seq_no), operation_(op) {}
  virtual int operator()(const ObTxData &tx_data, ObTxCCCtx *tx_cc_ctx = nullptr) override;
  INHERIT_TO_STRING_KV("ObITxDataCheckFunctor", ObITxDataCheckFunctor, K_(operation), K_(seq_no));
public:
  transaction::ObTxSEQ seq_no_;
  ObCleanoutOp &operation_;
};

class GenerateVirtualTxDataRowFunctor : public ObITxDataCheckFunctor
{
public:

  GenerateVirtualTxDataRowFunctor(observer::VirtualTxDataRow &row_data) : row_data_(row_data) {}

  virtual int operator()(const ObTxData &tx_data, ObTxCCCtx *tx_cc_ctx = nullptr) override;

public:
  observer::VirtualTxDataRow &row_data_;
};

class LoadTxOpFunctor : public ObITxDataCheckFunctor
{
public:
  LoadTxOpFunctor(ObTxData &tx_data) : tx_data_(tx_data) {}
  virtual int operator()(const ObTxData &tx_data, ObTxCCCtx *tx_cc_ctx = nullptr) override;
public:
  ObTxData &tx_data_;
};

}  // namespace storage
}  // namespace oceanbase

#endif  // OCEANBASE_STORAGE_OB_TX_DATA_FUNCTOR_
