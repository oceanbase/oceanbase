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

#include "storage/tx_table/ob_tx_table_interface.h"

#include "storage/tx/ob_tx_data_functor.h"
#include "storage/tx_table/ob_tx_table.h"
#include "lib/oblog/ob_log_module.h"

namespace oceanbase {
namespace storage {

int ObTxTableGuard::init(ObTxTable *tx_table)
{
  int ret = OB_SUCCESS;
  reset();

  if (OB_ISNULL(tx_table)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "tx_data_table is nullptr.");
  } else {
    epoch_ = tx_table->get_epoch();
    tx_table_ = tx_table;
  }
  return ret;
}

int ObTxTableGuard::check_row_locked(const transaction::ObTransID &read_tx_id,
                                     const transaction::ObTransID data_tx_id,
                                     const transaction::ObTxSEQ &sql_sequence,
                                     storage::ObStoreRowLockState &lock_state)
{
  if (OB_NOT_NULL(tx_table_)) {
    ObReadTxDataArg arg(data_tx_id, epoch_, mini_cache_);
    return tx_table_->check_row_locked(arg, read_tx_id, sql_sequence, lock_state);
  } else {
    return OB_NOT_INIT;
  }
}

int ObTxTableGuard::check_sql_sequence_can_read(const transaction::ObTransID tx_id,
                                                const transaction::ObTxSEQ &sql_sequence,
                                                bool &can_read)
{
  if (OB_NOT_NULL(tx_table_)) {
    ObReadTxDataArg arg(tx_id, epoch_, mini_cache_);
    return tx_table_->check_sql_sequence_can_read(arg, sql_sequence, can_read);
  } else {
    return OB_NOT_INIT;
  }
}

int ObTxTableGuard::get_tx_state_with_scn(const transaction::ObTransID tx_id,
                                          const share::SCN scn,
                                          int64_t &state,
                                          share::SCN &trans_version)
{
  if (OB_NOT_NULL(tx_table_)) {
    ObReadTxDataArg arg(tx_id, epoch_, mini_cache_);
    return tx_table_->get_tx_state_with_scn(arg, scn, state, trans_version);
  } else {
    return OB_NOT_INIT;
  }
}

int ObTxTableGuard::try_get_tx_state(const transaction::ObTransID tx_id,
                                     int64_t &state,
                                     share::SCN &trans_version,
                                     share::SCN &recycled_scn)
{
  if (OB_NOT_NULL(tx_table_)) {
    ObReadTxDataArg arg(tx_id, epoch_, mini_cache_);
    return tx_table_->try_get_tx_state(arg, state, trans_version, recycled_scn);
  } else {
    return OB_NOT_INIT;
  }
}

int ObTxTableGuard::lock_for_read(const transaction::ObLockForReadArg &lock_for_read_arg,
                                  bool &can_read,
                                  share::SCN &trans_version,
                                  bool &is_determined_state)
{
  ObCleanoutNothingOperation clean_nothing_op;
  ObReCheckNothingOperation recheck_nothing_op;
  return lock_for_read(
      lock_for_read_arg, can_read, trans_version, is_determined_state, clean_nothing_op, recheck_nothing_op);
}

int ObTxTableGuard::lock_for_read(const transaction::ObLockForReadArg &lock_for_read_arg,
                                  bool &can_read,
                                  share::SCN &trans_version,
                                  bool &is_determined_state,
                                  ObCleanoutOp &cleanout_op,
                                  ObReCheckOp &recheck_op)
{
  if (OB_NOT_NULL(tx_table_)) {
    ObReadTxDataArg arg(lock_for_read_arg.data_trans_id_, epoch_, mini_cache_);
    return tx_table_->lock_for_read(
        arg, lock_for_read_arg, can_read, trans_version, is_determined_state, cleanout_op, recheck_op);
  } else {
    return OB_NOT_INIT;
  }
}

int ObTxTableGuard::cleanout_tx_node(const transaction::ObTransID tx_id,
                                     memtable::ObMvccRow &value,
                                     memtable::ObMvccTransNode &tnode,
                                     const bool need_row_latch)
{
  if (OB_NOT_NULL(tx_table_)) {
    ObReadTxDataArg arg(tx_id, epoch_, mini_cache_);
    return tx_table_->cleanout_tx_node(arg, value, tnode, need_row_latch);
  } else {
    return OB_NOT_INIT;
  }
}

int ObTxTableGuard::get_recycle_scn(share::SCN &recycle_scn) { return tx_table_->get_recycle_scn(recycle_scn); }

int ObTxTableGuard::self_freeze_task()
{
  if (OB_NOT_NULL(tx_table_)) {
    return tx_table_->self_freeze_task();
  } else {
    return OB_NOT_INIT;
  }
}

bool ObTxTableGuard::check_ls_offline()
{
  bool discover_ls_offline = false;
  int ret = OB_SUCCESS;

  if (OB_ISNULL(tx_table_)) {
    ret = OB_NOT_INIT;
    discover_ls_offline = false;
    STORAGE_LOG(WARN, "tx table is nullptr", K(ret), K(discover_ls_offline));
  } else {
    int64_t cur_epoch = tx_table_->get_epoch();
    ObTxTable::TxTableState tx_table_state = tx_table_->get_state();
    if (cur_epoch != epoch_ || tx_table_state == ObTxTable::TxTableState::PREPARE_OFFLINE
        || tx_table_state == ObTxTable::TxTableState::OFFLINE) {
      discover_ls_offline = true;

      STORAGE_LOG(INFO, "discover ls offline", K(discover_ls_offline), K(cur_epoch), K(epoch_),
                  K(tx_table_state));
    }
  }

  return discover_ls_offline;
}

}  // namespace storage
}  // end namespace oceanbase
