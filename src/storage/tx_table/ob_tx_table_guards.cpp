/**
 * Copyright (c) 2022 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#define USING_LOG_PREFIX STORAGE
#include "storage/tx_table/ob_tx_table_guards.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/oblog/ob_log_module.h"
#include "storage/tx_table/ob_tx_table_interface.h"
#include "storage/tx_table/ob_tx_table.h"

namespace oceanbase
{
namespace storage
{
#define PRINT_ERROR_LOG(tx_id, ret, this) \
{ \
  if (OB_TRANS_CTX_NOT_EXIST == ret) { \
    LOG_ERROR("trans ctx not exit", KR(ret), K(tx_id), K(*this));\
  }\
}

// There are two log streams(ls_id=1001 and ls_id=1002). After the transaction is started, there may be scenarios
// ***********************************************
// |scene    |  tx_id  | ls_id=1001 | ls_id=1002 |
// ***********************************************
// |scene 1  |    1    |     Y      |      Y     |
// |scene 2  |    2    |     N      |      Y     |
// |scene 3  |    3    |     Y      |      N     |
// |scene 4  |    4    |     N      |      N     |
// ***********************************************

// Only in the transfer scenario, src_tx_table_guard may be effective.
// In the transfer scenario, suppose ls_id=1001 is src_ls, ls_id=1002 is dest_ls,
// and then obtain the the output parameters of each interface in different scenarios.
// tx_table_guard_ belongs to 1002, src_tx_table_guard belongs to 1001
// ****************************************************************************
// |             api             |  scene 1     | scene 2 | scene 3 | scene 4 |
// ****************************************************************************
// |check_row_locked             | 1002 + 1001  | 1002    | 1001    | ERROR   |
// |check_sql_sequence_can_read  | 1002 + 1001  | 1002    | 1001    | ERROR   |
// |lock_for_read                | 1002 + 1001  | 1002    | 1001    | ERROR   |
// |get_tx_state_with_log_ts     | 1002         | 1002    | 1001    | ERROR   |
// |cleanout_tx_node             | 1002 + 1001  | 1002    | 1001    | ERROR   |
// ****************************************************************************

int ObTxTableGuards::check_row_locked(
    const transaction::ObTransID &read_tx_id,
    const transaction::ObTransID &data_tx_id,
    const transaction::ObTxSEQ &sql_sequence,
    const share::SCN &scn,
    storage::ObStoreRowLockState &lock_state)
{
  int ret = OB_SUCCESS;
  bool dest_succ = false;
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tx table guard is invalid", K(ret), K(data_tx_id), K(tx_table_guard_));
  } else if (OB_SUCC(tx_table_guard_.check_row_locked(read_tx_id, data_tx_id, sql_sequence, lock_state))) {
    dest_succ = true;
  } else if (OB_TRANS_CTX_NOT_EXIST != ret || !is_need_read_src(scn)) {
    LOG_WARN("failed to check row locked", K(ret), K(data_tx_id), K(*this));
  } else {
    ret = OB_SUCCESS;
  }

  if (OB_FAIL(ret)) {
  } else if (lock_state.is_locked_ || !is_need_read_src(scn)) {
    // do nothing
  } else {
    storage::ObStoreRowLockState src_lock_state;
    if (OB_FAIL(src_tx_table_guard_.check_row_locked(read_tx_id, data_tx_id, sql_sequence, src_lock_state))) {
      if (dest_succ && OB_TRANS_CTX_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        LOG_INFO("trans ctx is not exist", K(data_tx_id));
      } else {
        LOG_WARN("failed to check row locked", K(ret), K(data_tx_id), K(*this));
      }
    } else {
      lock_state = src_lock_state;
    }
    PRINT_ERROR_LOG(data_tx_id, ret, this);
  }

  return ret;
}

// dest_succ is true or false, indicating whether the transaction status information is queried in tx_table_guard
// can_read is true or false, indicating whether the queried transaction is readable, and it is the output parameter of the interface;
// case 1: dest_succ = true, can_read = false;  The transaction is unreadable, in this case, only dest tx_table_guard can be read;
// case 2: dest_succ = true, can_read = true;   There is a readable transaction, in this case, both src tx_table_guard and dest tx_table_guard are read;
// case 3: dest_succ = false; The transaction does not exist on dest, in this case, both src tx_table_guard and dest tx_table_guard are read;
// If the conditions of case 2 and case 3 are met, but scn > transfer_start_scn, only read dest tx_table_guard;
// The interface lock_for_read also applies this logic.
int ObTxTableGuards::check_sql_sequence_can_read(
    const transaction::ObTransID &data_tx_id,
    const transaction::ObTxSEQ &sql_sequence,
    const share::SCN &scn,
    bool &can_read)
{
  int ret = OB_SUCCESS;
  bool src_can_read = false;
  bool dest_succ = false;
  can_read = false;
  if (!is_valid() || !scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tx table guard is invalid", K(ret), KPC(this), K(data_tx_id), K(scn));
  } else if (OB_SUCC(tx_table_guard_.check_sql_sequence_can_read(data_tx_id, sql_sequence, can_read))) {
    dest_succ = true;
  } else if (OB_TRANS_CTX_NOT_EXIST != ret || !is_need_read_src(scn)) {
    LOG_WARN("failed to check sql sepuence can read", K(ret), K(data_tx_id), K(*this));
  } else {
    ret = OB_SUCCESS;
  }
  // Both tx_table_guard need to be checked
  if (OB_FAIL(ret)) {
  } else if ((dest_succ && !can_read) || !is_need_read_src(scn)) {
    // do nothing
  } else {
    if (OB_FAIL(src_tx_table_guard_.check_sql_sequence_can_read(data_tx_id, sql_sequence, src_can_read))) {
      if (dest_succ && OB_TRANS_CTX_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        LOG_INFO("trans ctx is not exist", K(data_tx_id));
      } else {
        LOG_WARN("failed to check sql sepuence can read from source tx table", K(ret), K(data_tx_id), K(*this));
      }
    } else {
      can_read = src_can_read;
    }
    PRINT_ERROR_LOG(data_tx_id, ret, this);
  }

  return ret;
}

int ObTxTableGuards::get_tx_state_with_scn(
    const transaction::ObTransID &data_trans_id,
    const share::SCN scn,
    int64_t &state,
    share::SCN &trans_version)
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tx table guard is invalid", K(ret), KPC(this), K(data_trans_id));
  } else if (OB_SUCC(tx_table_guard_.get_tx_state_with_scn(data_trans_id, scn, state, trans_version))) {
  } else if (OB_TRANS_CTX_NOT_EXIST != ret || !is_need_read_src(scn)) {
    LOG_WARN("failed to get tx state with log ts", K(ret), K(data_trans_id), K(*this));
  } else {
    if (OB_FAIL(src_tx_table_guard_.get_tx_state_with_scn(data_trans_id, scn, state, trans_version))) {
      LOG_WARN("failed to get tx state with log ts from source tx table", K(ret), K(data_trans_id), K(*this));
    }
    PRINT_ERROR_LOG(data_trans_id, ret, this);
  }
  return ret;
}

int ObTxTableGuards::lock_for_read(
    const transaction::ObLockForReadArg &lock_for_read_arg,
    bool &can_read,
    share::SCN &trans_version,
    bool &is_determined_state,
    ObCleanoutOp &cleanout_op,
    ObReCheckOp &recheck_op)
{
  int ret = OB_SUCCESS;
  bool dest_succ = false;
  can_read = false;
  if (!is_valid() || !lock_for_read_arg.scn_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tx table guard is invalid", K(ret), KPC(this), K(lock_for_read_arg));
  } else if (OB_SUCC(tx_table_guard_.lock_for_read(lock_for_read_arg,
      can_read, trans_version, is_determined_state, cleanout_op, recheck_op))) {
    dest_succ = true;
  } else if (OB_TRANS_CTX_NOT_EXIST != ret || !is_need_read_src(lock_for_read_arg.scn_)) {
    LOG_WARN("failed to lock for read", K(ret), "tx_id", lock_for_read_arg.data_trans_id_, K(*this));
  } else {
    ret = OB_SUCCESS;
  }

  if (OB_FAIL(ret)) {
  } else if ((dest_succ && !can_read) || !is_need_read_src(lock_for_read_arg.scn_)) {
    // do nothing
  } else {
    // Both tx_table_guard need to be checked
    bool src_can_read = false;
    share::SCN src_trans_version = share::SCN::invalid_scn();
    bool src_is_determined_state = false;
    if (OB_FAIL(src_tx_table_guard_.lock_for_read(lock_for_read_arg,
        src_can_read, src_trans_version, src_is_determined_state, cleanout_op, recheck_op))) {
      if (dest_succ && OB_TRANS_CTX_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        LOG_INFO("trans ctx is not exist", K(lock_for_read_arg));
      } else {
        LOG_WARN("failed to lock for read from source tx table", K(ret), "tx_id", lock_for_read_arg.data_trans_id_, K(*this));
      }
    } else {
      can_read = src_can_read;
      trans_version = src_trans_version;
      is_determined_state = src_is_determined_state;
    }
    PRINT_ERROR_LOG(lock_for_read_arg.data_trans_id_, ret, this);
  }

  return ret;
}

int ObTxTableGuards::lock_for_read(
    const transaction::ObLockForReadArg &lock_for_read_arg,
    bool &can_read,
    share::SCN &trans_version,
    bool &is_determined_state)
{
  int ret = OB_SUCCESS;
  bool dest_succ = false;
  if (!tx_table_guard_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tx table guard is invalid", K(ret), K(tx_table_guard_));
  } else if (OB_SUCC(tx_table_guard_.lock_for_read(lock_for_read_arg,
      can_read, trans_version, is_determined_state))) {
    dest_succ = true;
  } else if (OB_TRANS_CTX_NOT_EXIST != ret || !is_need_read_src(lock_for_read_arg.scn_)) {
    LOG_WARN("failed to lock for read", K(ret), "tx_id", lock_for_read_arg.data_trans_id_, K(*this));
  } else {
    ret = OB_SUCCESS;
  }
  if (OB_FAIL(ret)) {
  } else if ((dest_succ && !can_read) || !is_need_read_src(lock_for_read_arg.scn_)) {
  } else {
    bool src_can_read = false;
    share::SCN src_trans_version = share::SCN::invalid_scn();
    bool src_is_determined_state = false;
    if (OB_FAIL(src_tx_table_guard_.lock_for_read(lock_for_read_arg,
        src_can_read, src_trans_version, src_is_determined_state))) {
      if (dest_succ && OB_TRANS_CTX_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        LOG_INFO("trans ctx is not exist", K(lock_for_read_arg));
      } else {
        LOG_WARN("failed to lock for read from source tx table", K(ret), "tx_id", lock_for_read_arg.data_trans_id_, K(*this));
      }
    } else {
      can_read = src_can_read;
      trans_version = src_trans_version;
      is_determined_state = src_is_determined_state;
    }
    PRINT_ERROR_LOG(lock_for_read_arg.data_trans_id_, ret, this);
  }
  return ret;
}

int ObTxTableGuards::cleanout_tx_node(
    const transaction::ObTransID &tx_id,
    memtable::ObMvccRow &value,
    memtable::ObMvccTransNode &tnode,
    const bool need_row_latch)
{
  int ret = OB_SUCCESS;
  bool dest_succ = false;
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tx table guard is invalid", K(ret), KPC(this), K(tx_id));
  } else if (OB_SUCC(tx_table_guard_.cleanout_tx_node(tx_id, value, tnode, need_row_latch))) {
    dest_succ = true;
  } else if (OB_TRANS_CTX_NOT_EXIST != ret || !is_need_read_src(tnode.get_scn())) {
    LOG_WARN("failed to cleanout tx node", K(ret), K(tx_id), K(*this));
  } else {
    ret = OB_SUCCESS;
  }
  if (OB_FAIL(ret)) {
  } else if (!is_need_read_src(tnode.get_scn())) {
    // do nothing
  } else {
    if (OB_FAIL(src_tx_table_guard_.cleanout_tx_node(tx_id, value, tnode, need_row_latch))) {
      if (dest_succ && OB_TRANS_CTX_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        LOG_INFO("trans ctx is not exist", K(tx_id));
      } else {
        LOG_WARN("failed to cleanout tx nod from source tx table", K(ret), K(tx_id), K(*this));
      }
    }
    PRINT_ERROR_LOG(tx_id, ret, this);
  }
  return ret;
}

bool ObTxTableGuards::check_ls_offline()
{
  bool discover_ls_offline = false;
  if (!src_tx_table_guard_.is_valid()) {
    discover_ls_offline = tx_table_guard_.check_ls_offline();
  } else {
    discover_ls_offline = tx_table_guard_.check_ls_offline() && src_tx_table_guard_.check_ls_offline();
  }
  return discover_ls_offline;
}

// scn: sstable is end_scn, memtable is ObMvccTransNode scn
// src_tx_table_guard_ and transfer_start_scn_ are valid, indicating that it is an operation during the transfer process.
// By comparing the size of scn and transfer_start_scn_, you can indirectly determine which log stream the data corresponding to this transaction belongs to.
// scn <= transfer_start_scn_ : the data is on the src ls of the transfer, you need to read src_tx_table_guard_.
// scn > transfer_start_scn_ : the data is on the dest ls of the transfer, you need to check tx_table_guard_.
bool ObTxTableGuards::is_need_read_src(const share::SCN scn) const
{
  bool is_need = false;
  if (src_tx_table_guard_.is_valid()
      && transfer_start_scn_.is_valid()
      && scn.is_valid()
      && !scn.is_max()
      && scn <= transfer_start_scn_) {
    is_need = true;
    LOG_INFO("need read src", K(scn), KPC(this), K(is_need));
  }
  return is_need;
}

bool ObTxTableGuards::during_transfer() const
{
  return src_tx_table_guard_.is_valid()
    && transfer_start_scn_.is_valid();
}

} // end namespace oceanbase
}
