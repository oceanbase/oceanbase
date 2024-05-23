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

#ifndef OCEANBASE_TRANSACTION_OB_CTX_TX_DATA_
#define OCEANBASE_TRANSACTION_OB_CTX_TX_DATA_

#include "lib/lock/ob_spin_rwlock.h"
#include "storage/tx/ob_tx_data_define.h"

namespace oceanbase
{

namespace storage
{
class ObTxTable;
}

namespace transaction
{

class ObCtxTxData
{
public:
  ObCtxTxData() { reset(); }
  void reset();
  void destroy();

  int init(const int64_t abs_expire_time, ObLSTxCtxMgr *ctx_mgr, int64_t tx_id);

  bool is_read_only() const { return read_only_; }
  bool has_recovered_from_tx_table() const { return recovered_from_tx_table_; }
  bool is_decided() const;
  share::SCN get_max_replayed_rollback_scn() const { return max_replayed_rollback_scn_; }
  void set_max_replayed_rollback_scn(const share::SCN &scn) { max_replayed_rollback_scn_ = scn; }
  int insert_into_tx_table();
  int recover_tx_data(storage::ObTxData *tmp_tx_data);
  int deep_copy_tx_data_out(storage::ObTxDataGuard &tmp_tx_data_guard);
  // int alloc_tmp_tx_data(storage::ObTxDataGuard &tmp_tx_data);
  int free_tmp_tx_data(storage::ObTxData *&tmp_tx_data);
  int insert_tmp_tx_data(storage::ObTxData *tmp_tx_data);

  void get_tx_table(storage::ObTxTable *&tx_table);

  int set_state(int32_t state);
  int add_abort_op(share::SCN op_scn);
  int set_commit_version(const share::SCN &commit_version);
  int set_start_log_ts(const share::SCN &start_ts);
  int set_end_log_ts(const share::SCN &end_ts);
  int reserve_tx_op_space(int64_t count);

  int32_t get_state() const;
  const share::SCN get_commit_version() const;
  const share::SCN get_start_log_ts() const;
  const share::SCN get_end_log_ts() const;

  ObTransID get_tx_id() const;

  int get_tx_data(storage::ObTxDataGuard &tx_data_guard);

  // ATTENTION : use get_tx_data_ptr only if you can make sure the life cycle of ctx_tx_data is longer than your usage
  int get_tx_data_ptr(storage::ObTxData *&tx_data_ptr);

  TO_STRING_KV(KP(ctx_mgr_), K(tx_data_guard_), K(read_only_));

public:
  //only for unittest
  void test_init(storage::ObTxData &tx_data, ObLSTxCtxMgr *ctx_mgr)
  {
    ctx_mgr_ = ctx_mgr;
    tx_data_guard_.init(&tx_data);
    read_only_ = false;
  }
  void test_tx_data_reset()
  {
    if (OB_NOT_NULL(tx_data_guard_.tx_data())) {
      tx_data_guard_.reset();
    }
  }
  void test_set_tx_id(int64_t tx_id)
  {
    if (OB_NOT_NULL(tx_data_guard_.tx_data())) {
      tx_data_guard_.tx_data()->tx_id_ = tx_id;
    }
  }

private:
  int check_tx_data_writable_();
  int insert_tx_data_(storage::ObTxTable *tx_table, storage::ObTxData *tx_data);
  int deep_copy_tx_data_(storage::ObTxTable *tx_table, storage::ObTxDataGuard &tx_data);
  int revert_tx_data_(storage::ObTxData *&tx_data);

private:
  typedef common::SpinRWLock RWLock;
  typedef common::SpinRLockGuard RLockGuard;
  typedef common::SpinWLockGuard WLockGuard;
private:
  ObLSTxCtxMgr *ctx_mgr_;
  storage::ObTxDataGuard tx_data_guard_;
  bool read_only_;
  bool recovered_from_tx_table_;
  // record the max replayed rollback to end_scn
  // used in replay of RollbackToLog
  // when replay multiple RollbackToLog in parallell, tx-data inserted into
  // tx-data-table with end_scn out of order, in order to ensure the invariant
  // of tx-data with larger end_scn contains the tx-data with smaller end_scn
  // we rewrite the tx-data by delete and insert the tx-data with same end_scn
  //
  // this is a temporary solution for the problem, in the comming refine names as
  // `shared contents of tx-data`, which can ensure the tx-data has been inserted
  // into tx-data memtable was refresh with the latest content replayed out.
  share::SCN max_replayed_rollback_scn_;
  // lock for tx_data_ pointer
  RWLock lock_;
};

} // namespace transaction
} // namespace oceanbase

#endif
