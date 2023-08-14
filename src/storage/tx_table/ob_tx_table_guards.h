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

#ifndef OCEANBASE_STORAGE_OB_TRANS_TABLE_GUARDS_
#define OCEANBASE_STORAGE_OB_TRANS_TABLE_GUARDS_

#include "lib/ob_define.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/function/ob_function.h"
#include "share/scn.h"
#include "storage/tx_table/ob_tx_table_interface.h"

namespace oceanbase
{
namespace transaction {
class ObLockForReadArg;
class ObTransID;
}
namespace memtable
{
class ObMvccRow;
class ObMvccTransNode;
}
namespace storage
{
class ObStoreRowLockState;
class ObTxData;
class ObTxCCCtx;
class ObCleanoutNothingOperation;
class ObReCheckNothingOperation;
class ObCleanoutOp;
class ObReCheckOp;
class ObTxTableGuards
{
public:
  ObTxTableGuards()
   : tx_table_guard_(),
     src_tx_table_guard_(),
     transfer_start_scn_(share::SCN::invalid_scn()) {}
  ~ObTxTableGuards() { reset(); }
  void reset()
  {
    tx_table_guard_.reset();
    src_tx_table_guard_.reset();
    transfer_start_scn_.reset();
  }
  void reuse()
  {
    tx_table_guard_.reuse();
    src_tx_table_guard_.reuse();
  }
  bool is_valid() const { return tx_table_guard_.is_valid() && (src_tx_table_guard_.is_valid() == transfer_start_scn_.is_valid()); }
  /**
   * @brief check whether the row key is locked by tx id
   *
   * @param[in] read_trans_id
   * @param[in] data_trans_id
   * @param[in] sql_sequence
   * @param[out] lock_state
   */
  int check_row_locked(
      const transaction::ObTransID &read_tx_id,
      const transaction::ObTransID &data_tx_id,
      const transaction::ObTxSEQ &sql_sequence,
      const share::SCN &scn,
      storage::ObStoreRowLockState &lock_state);
  /**
   * @brief check whether transaction data_tx_id with sql_sequence is readable. (sql_sequence may be unreadable for txn or stmt rollback)
   *
   * @param[in] data_tx_id
   * @param[in] sql_sequence
   * @param[in] scn
   * @param[out] can_read
   */
  int check_sql_sequence_can_read(
      const transaction::ObTransID &data_tx_id,
      const transaction::ObTxSEQ &sql_sequence,
      const share::SCN &scn,
      bool &can_read);
  /**
   * @brief fetch the state of txn DATA_TRANS_ID when replaying to LOG_TS the requirement can be seen from
   *
   *
   * @param[in] data_trans_id
   * @param[in] scn
   * @param[out] state
   * @param[out] trans_version
   */
  int get_tx_state_with_scn(
      const transaction::ObTransID &data_trans_id,
      const share::SCN scn,
      int64_t &state,
      share::SCN &trans_version);
  /**
   * @brief the txn READ_TRANS_ID use SNAPSHOT_VERSION to read the data, and check whether the data is locked, readable or unreadable by txn DATA_TRANS_ID. READ_LATEST is used to check whether read the data belong to the same txn
   *
   * @param[in] lock_for_read_arg
   * @param[out] can_read
   * @param[out] trans_version
   * @param[out] is_determined_state
   * @param[in] op
   */
  int lock_for_read(
      const transaction::ObLockForReadArg &lock_for_read_arg,
      bool &can_read,
      share::SCN &trans_version,
      bool &is_determined_state,
      ObCleanoutOp &cleanout_op,
      ObReCheckOp &recheck_op);

  int lock_for_read(
      const transaction::ObLockForReadArg &lock_for_read_arg,
      bool &can_read,
      share::SCN &trans_version,
      bool &is_determined_state);
  /**
   * @brief cleanout the tx state when encountering the uncommitted node. The node will be cleaned out if the state of
   * the txn is decided or prepared. You neeed notice that txn commit or abort is pereformed both on mvcc row and mvcc
   * txn node. And need row latch is used for lock_for_read to shorten critical path.
   *
   * @param[in] tx_id
   * @param[in] value
   * @param[in] tnode
   * @param[in] need_row_latch
   */
  int cleanout_tx_node(
      const transaction::ObTransID &tx_id,
      memtable::ObMvccRow &value,
      memtable::ObMvccTransNode &tnode,
      const bool need_row_latch);
  bool check_ls_offline();
  bool is_need_read_src(const share::SCN scn) const;
  bool during_transfer() const;
  TO_STRING_KV(K_(tx_table_guard), K_(src_tx_table_guard), K_(transfer_start_scn));
public:
  storage::ObTxTableGuard tx_table_guard_;
  // transfer_start_scn_ and src_tx_table_guard_ need to be valid at the same time.
  // dml executed during transfer, src_tx_table_guard_ will be valid.
  storage::ObTxTableGuard src_tx_table_guard_;
  share::SCN transfer_start_scn_; // Use transfer_start_scn to judge whether you need to read src_tx_table_guard
};

}  // namespace storage
}  // namespace oceanbase

#endif
