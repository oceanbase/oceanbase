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

#define PRINT_ERROR_LOG(tx_id, ret, this)                           \
{                                                                 \
  if (OB_TRANS_CTX_NOT_EXIST == ret) {                            \
    LOG_ERROR("trans ctx not exit", KR(ret), K(tx_id), K(*this)); \
  }                                                               \
}

namespace oceanbase
{
namespace storage
{

int ObTxTableGuards::check_row_locked(const transaction::ObTransID &read_tx_id,
                                      const transaction::ObTransID &data_tx_id,
                                      const transaction::ObTxSEQ &sql_sequence,
                                      const share::SCN &scn,
                                      storage::ObStoreRowLockState &lock_state)
{
  int ret = OB_SUCCESS;

  CheckRowLockedFunctor fn(read_tx_id,
                           data_tx_id,
                           sql_sequence,
                           lock_state);

  if (!src_tx_table_guard_.is_valid()) {
    ret = check_with_tx_data(data_tx_id, fn);
  } else {
    bool use_dest = false;
    storage::ObStoreRowLockState src_lock_state;
    CheckRowLockedFunctor src_fn(read_tx_id,
                                 data_tx_id,
                                 sql_sequence,
                                 src_lock_state);

    ret = check_with_tx_data(data_tx_id,
                             fn,
                             src_fn,
                             use_dest);

    if (!use_dest) {
      lock_state = src_lock_state;
    }
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

  CheckSqlSequenceCanReadFunctor fn(sql_sequence,
                                    can_read);

  if (!src_tx_table_guard_.is_valid()) {
    ret = check_with_tx_data(data_tx_id, fn);
  } else {
    bool use_dest = false;
    bool src_can_read = false;
    CheckSqlSequenceCanReadFunctor src_fn(sql_sequence,
                                          src_can_read);

    ret = check_with_tx_data(data_tx_id,
                             fn,
                             src_fn,
                             use_dest);

    if (!use_dest) {
      can_read = src_can_read;
    }
  }

  return ret;
}

int ObTxTableGuards::get_tx_state_with_scn(
    const transaction::ObTransID &data_tx_id,
    const share::SCN scn,
    int64_t &state,
    share::SCN &trans_version)
{
  int ret = OB_SUCCESS;

  GetTxStateWithSCNFunctor fn(scn,
                              state,
                              trans_version);

  if (!src_tx_table_guard_.is_valid()) {
    ret = check_with_tx_data(data_tx_id, fn);
  } else {
    bool use_dest = false;
    int64_t src_state = 0;
    share::SCN src_trans_version;
    GetTxStateWithSCNFunctor src_fn(scn,
                                    src_state,
                                    src_trans_version);


    ret = check_with_tx_data(data_tx_id,
                             fn,
                             src_fn,
                             use_dest);

    if (!use_dest) {
      state = src_state;
      trans_version = src_trans_version;
    }
  }

  return ret;
}

int ObTxTableGuards::lock_for_read(
    const transaction::ObLockForReadArg &lock_for_read_arg,
    bool &can_read,
    share::SCN &trans_version,
    ObCleanoutOp &cleanout_op,
    ObReCheckOp &recheck_op)
{
  int ret = OB_SUCCESS;

  LockForReadFunctor fn(lock_for_read_arg,
                        can_read,
                        trans_version,
                        tx_table_guard_.get_ls_id(),
                        cleanout_op,
                        recheck_op);

  if (!src_tx_table_guard_.is_valid()) {
    ret = check_with_tx_data(lock_for_read_arg.data_trans_id_, fn);

    if (OB_SUCC(ret)) {
      if (cleanout_op.need_cleanout()) {
        cleanout_op(fn.get_tx_data_check_data());
      }
    }
  } else {
    bool use_dest = false;
    bool src_can_read = false;
    share::SCN src_trans_version;

    LockForReadFunctor src_fn(lock_for_read_arg,
                              src_can_read,
                              src_trans_version,
                              src_tx_table_guard_.get_ls_id(),
                              cleanout_op,
                              recheck_op);

    ret = check_with_tx_data(lock_for_read_arg.data_trans_id_,
                             fn,
                             src_fn,
                             use_dest);

    if (OB_SUCC(ret)) {
      if (!use_dest) {
        can_read = src_can_read;
        trans_version = src_trans_version;
      }

      if (cleanout_op.need_cleanout()) {
        if (use_dest) {
          cleanout_op(fn.get_tx_data_check_data());
        } else {
          cleanout_op(src_fn.get_tx_data_check_data());
        }
      }
    }
  }

  return ret;
}

int ObTxTableGuards::lock_for_read(
    const transaction::ObLockForReadArg &lock_for_read_arg,
    bool &can_read,
    share::SCN &trans_version)
{
  int ret = OB_SUCCESS;
  ObCleanoutNothingOperation clean_nothing_op;
  ObReCheckNothingOperation recheck_nothing_op;

  LockForReadFunctor fn(lock_for_read_arg,
                        can_read,
                        trans_version,
                        tx_table_guard_.get_ls_id(),
                        clean_nothing_op,
                        recheck_nothing_op);

  if (!src_tx_table_guard_.is_valid()) {
    ret = check_with_tx_data(lock_for_read_arg.data_trans_id_, fn);
  } else {
    bool use_dest = false;
    bool src_can_read = false;
    share::SCN src_trans_version;

    LockForReadFunctor src_fn(lock_for_read_arg,
                              src_can_read,
                              src_trans_version,
                              src_tx_table_guard_.get_ls_id(),
                              clean_nothing_op,
                              recheck_nothing_op);

    ret = check_with_tx_data(lock_for_read_arg.data_trans_id_,
                             fn,
                             src_fn,
                             use_dest);

    if (!use_dest) {
      can_read = src_can_read;
      trans_version = src_trans_version;
    }
  }

  return ret;
}

int ObTxTableGuards::cleanout_tx_node(
    const transaction::ObTransID &data_tx_id,
    memtable::ObMvccRow &value,
    memtable::ObMvccTransNode &tnode,
    const bool need_row_latch)
{
  int ret = OB_SUCCESS;

  ObCleanoutTxNodeOperation op(value,
                               tnode,
                               need_row_latch);
  CleanoutTxStateFunctor fn(tnode.seq_no_, op);

  if (!src_tx_table_guard_.is_valid()) {
    ret = check_with_tx_data(data_tx_id, fn);

    if (OB_SUCC(ret)) {
      if (op.need_cleanout()) {
        op(fn.get_tx_data_check_data());
      }
    }
  } else {
    bool use_dest = false;
    CleanoutTxStateFunctor src_fn(tnode.seq_no_, op);

    ret = check_with_tx_data(data_tx_id,
                             fn,
                             src_fn,
                             use_dest);

    if (OB_SUCC(ret)) {
      if (op.need_cleanout()) {
        if (use_dest) {
          op(fn.get_tx_data_check_data());
        } else {
          op(src_fn.get_tx_data_check_data());
        }
      }
    }
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

int ObTxTableGuards::check_with_tx_data(
  const transaction::ObTransID &data_tx_id,
  ObITxDataCheckFunctor &functor,
  ObITxDataCheckFunctor &src_functor,
  bool &use_dst)
{
  int ret = OB_SUCCESS;
  bool need_src = false;
  bool has_dest = false;

  ObReadTxDataArg arg(data_tx_id,
                      tx_table_guard_.get_epoch(),
                      tx_table_guard_.get_mini_cache());

  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tx table guards is invalid", K(ret), KPC(this), K(arg));
  } else if (OB_FAIL(tx_table_guard_.check_with_tx_data(arg, functor))) {
    if (OB_TRANS_CTX_NOT_EXIST == ret
        && src_tx_table_guard_.is_valid()) {
      // Case1: tx ctx not exists on dst, we need use src's result
      ret = OB_SUCCESS;
      need_src = true;
      has_dest = false;
    } else {
      LOG_WARN("check with dst tx data failed", K(ret), KPC(this), K(arg));
    }
  } else {
    need_src = !functor.is_decided()
      && src_tx_table_guard_.is_valid();
    has_dest = true;
  }

  if (OB_FAIL(ret)) {
    // pass
  } else if (need_src) {
    ObReadTxDataArg src_arg(data_tx_id,
                            src_tx_table_guard_.get_epoch(),
                            src_tx_table_guard_.get_mini_cache());

    if (OB_FAIL(src_tx_table_guard_.check_with_tx_data(src_arg,
                                                       src_functor))) {
      if (OB_TRANS_CTX_NOT_EXIST == ret && has_dest) {
        use_dst = true;
        ret = OB_SUCCESS;
        LOG_DEBUG("use dest tx table guard as src has no ctx", KPC(this), K(src_arg));
      } else {
        LOG_WARN("check with src tx data failed", K(ret), KPC(this), K(src_arg));
      }
    } else if (!has_dest || src_functor.is_decided()) {
      use_dst = false;
    } else {
      use_dst = true;
    }

    LOG_INFO("need read src", KPC(this), K(use_dst), K(functor), K(src_functor));
  } else {
    use_dst = true;
  }

  PRINT_ERROR_LOG(data_tx_id, ret, this);

  return ret;
}

int ObTxTableGuards::check_with_tx_data(
  const transaction::ObTransID &data_tx_id,
  ObITxDataCheckFunctor &functor)
{
  int ret = OB_SUCCESS;
  ObReadTxDataArg arg(data_tx_id,
                      tx_table_guard_.get_epoch(),
                      tx_table_guard_.get_mini_cache());

  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tx table guards is invalid", K(ret), KPC(this), K(arg));
  } else if (OB_FAIL(tx_table_guard_.check_with_tx_data(arg,
                                                        functor))) {
    if (OB_TRANS_CTX_NOT_EXIST != ret) {
      LOG_WARN("check with dst tx data failed", K(ret), KPC(this), K(arg));
    }
  }

  return ret;
}

} // end namespace storage
} // end namespace oceanbase
