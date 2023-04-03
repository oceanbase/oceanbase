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

#include "storage/tx/ob_ctx_tx_data.h"
#include "storage/tx/ob_trans_ctx_mgr.h"
#include "storage/tx/ob_tx_data_define.h"
#include "storage/tx/ob_trans_define.h"

namespace oceanbase
{

using namespace storage;
using namespace share;

namespace transaction
{

#define GET_TX_TABLE_(tx_table)                                                       \
  ObTxTableGuard table_guard;                                                         \
  if (OB_ISNULL(ctx_mgr_)) {                                                          \
    ret = OB_ERR_UNEXPECTED;                                                          \
    TRANS_LOG(WARN, "ctx_mgr is null when get_tx_table", K(ret));                     \
  } else if (OB_FAIL(ctx_mgr_->get_tx_table_guard(table_guard))) {                    \
    TRANS_LOG(WARN, "get tx table guard without check failed", KR(ret), K(*this));    \
  } else if (OB_ISNULL(tx_table = table_guard.get_tx_table())) {                      \
    ret = OB_ERR_UNEXPECTED;                                                          \
    TRANS_LOG(WARN, "tx table is null", KR(ret), K(ctx_mgr_->get_ls_id()), K(*this)); \
  }

int ObCtxTxData::init(ObLSTxCtxMgr *ctx_mgr, int64_t tx_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx_mgr) || tx_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), KP(ctx_mgr), K(tx_id));
  } else {
    ctx_mgr_ = ctx_mgr;
    ObTxTable *tx_table = nullptr;
    if (OB_ISNULL(ctx_mgr_)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "ctx_mgr is null when get_tx_table", K(ret));
    } else if (OB_ISNULL(tx_table = ctx_mgr_->get_tx_table())) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "tx table is null", KR(ret), K(ctx_mgr_->get_ls_id()), K(*this));
    } else if (OB_FAIL(tx_table->alloc_tx_data(tx_data_guard_))) {
      TRANS_LOG(WARN, "get tx data failed", KR(ret), K(ctx_mgr_->get_ls_id()));
    } else if (OB_ISNULL(tx_data_guard_.tx_data())) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "tx data is unexpected null", KR(ret), K(ctx_mgr_->get_ls_id()));
    } else {
      tx_data_guard_.tx_data()->tx_id_ = tx_id;
    }
  }
  return ret;
}

void ObCtxTxData::reset()
{
  ctx_mgr_ = nullptr;
  tx_data_guard_.reset();
  read_only_ = false;
}

void ObCtxTxData::destroy()
{
  reset();
}

int ObCtxTxData::insert_into_tx_table()
{
  int ret = OB_SUCCESS;
  WLockGuard guard(lock_);

  if (OB_FAIL(check_tx_data_writable_())) {
    TRANS_LOG(WARN, "tx data is not writeable", K(ret));
  } else {
    ObTxTable *tx_table = nullptr;
    GET_TX_TABLE_(tx_table)
    if (OB_FAIL(ret)) {
    } else {
      if (OB_FAIL(insert_tx_data_(tx_table, tx_data_guard_.tx_data()))) {
        TRANS_LOG(WARN, "insert tx data failed", K(ret), K(*this));
      } else {
        read_only_ = true;
      }
    }
  }

  return ret;
}

int ObCtxTxData::recover_tx_data(ObTxDataGuard &rhs)
{
  int ret = OB_SUCCESS;
  WLockGuard guard(lock_);
  ObTxTable *tx_table = nullptr;
  GET_TX_TABLE_(tx_table);

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(check_tx_data_writable_())) {
    TRANS_LOG(WARN, "tx data is not writeable", K(ret), KPC(this));
  } else if (OB_ISNULL(rhs.tx_data())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "input tx data guard is unexpected nullptr", K(ret), KPC(this));
  } else if (OB_FAIL(tx_data_guard_.init(rhs.tx_data()))) {
    TRANS_LOG(WARN, "init tx data guard failed", K(ret), KPC(this));
  }

  return ret;
}

int ObCtxTxData::replace_tx_data(ObTxData *tmp_tx_data)
{
  int ret = OB_SUCCESS;
  WLockGuard guard(lock_);

  ObTxTable *tx_table = nullptr;
  GET_TX_TABLE_(tx_table);

  if (OB_FAIL(check_tx_data_writable_())) {
    TRANS_LOG(WARN, "tx data is not writeable", K(ret), K(*this));
  } else if (OB_ISNULL(tmp_tx_data)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), KP(tmp_tx_data));
  } else {
    tx_data_guard_.reset();
    if (OB_FAIL(tx_data_guard_.init(tmp_tx_data))) {
      TRANS_LOG(WARN, "init tx data guard failed", KR(ret), KPC(tmp_tx_data));
    }
  }
  return ret;
}

int ObCtxTxData::deep_copy_tx_data_out(ObTxDataGuard &tmp_tx_data_guard)
{
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);

  if (OB_FAIL(check_tx_data_writable_())) {
    TRANS_LOG(WARN, "tx data is not writeable", K(ret), K(*this));
  } else {
    ObTxTable *tx_table = nullptr;
    GET_TX_TABLE_(tx_table)
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(deep_copy_tx_data_(tx_table, tmp_tx_data_guard))) {
      TRANS_LOG(WARN, "deep copy tx data failed", K(ret), K(tmp_tx_data_guard), K(*this));
    } else if (OB_ISNULL(tmp_tx_data_guard.tx_data())) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "copied tmp tx data is null", KR(ret), K(*this));
    }
  }

  return ret;
}

int ObCtxTxData::alloc_tmp_tx_data(storage::ObTxDataGuard &tmp_tx_data_guard)
{
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);

  if (OB_FAIL(check_tx_data_writable_())) {
    TRANS_LOG(WARN, "tx data is not writeable", K(ret), K(*this));
  } else {
    ObTxTable *tx_table = nullptr;
    GET_TX_TABLE_(tx_table)
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(tx_table->alloc_tx_data(tmp_tx_data_guard))) {
      TRANS_LOG(WARN, "alloc tx data failed", K(ret));
    }
  }

  return ret;
}

int ObCtxTxData::free_tmp_tx_data(ObTxData *&tmp_tx_data)
{
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);

  if (OB_FAIL(check_tx_data_writable_())) {
    TRANS_LOG(WARN, "tx data is not writeable", K(ret), K(*this));
  } else if (OB_FAIL(revert_tx_data_(tmp_tx_data))) {
    TRANS_LOG(WARN, "free tx data failed", K(ret), KPC(tmp_tx_data), K(*this));
  }

  return ret;
}

int ObCtxTxData::insert_tmp_tx_data(ObTxData *tmp_tx_data)
{
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);

  if (OB_FAIL(check_tx_data_writable_())) {
    TRANS_LOG(WARN, "tx data is not writeable", K(ret), K(*this));
  } else {
    ObTxTable *tx_table = nullptr;
    GET_TX_TABLE_(tx_table)
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(insert_tx_data_(tx_table, tmp_tx_data))) {
      TRANS_LOG(WARN, "insert tx data failed", K(ret), KPC(tmp_tx_data), K(*this));
    }
  }

  return ret;
}

void ObCtxTxData::get_tx_table(storage::ObTxTable *&tx_table)
{
  int ret = OB_SUCCESS;

  GET_TX_TABLE_(tx_table);

  if (OB_FAIL(ret)) {
    tx_table = nullptr;
  }
}

int ObCtxTxData::set_state(int32_t state)
{
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);

  if (OB_FAIL(check_tx_data_writable_())) {
    TRANS_LOG(WARN, "tx data is not writeable", K(ret), K(*this));
  } else {
    ATOMIC_STORE(&(tx_data_guard_.tx_data()->state_), state);
  }

  return ret;
}

int ObCtxTxData::set_commit_version(const SCN &commit_version)
{
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);

  if (OB_FAIL(check_tx_data_writable_())) {
    TRANS_LOG(WARN, "tx data is not writeable", K(ret), K(*this));
  } else {
    tx_data_guard_.tx_data()->commit_version_.atomic_store(commit_version);
  }

  return ret;
}

int ObCtxTxData::set_start_log_ts(const SCN &start_ts)
{
  int ret = OB_SUCCESS;
  // const SCN tmp_start_ts = (start_ts.is_valid() ? start_ts : SCN::max_scn());
  RLockGuard guard(lock_);

  if (OB_FAIL(check_tx_data_writable_())) {
    TRANS_LOG(WARN, "tx data is not writeable", K(ret), K(*this));
  } else {
    tx_data_guard_.tx_data()->start_scn_.atomic_store(start_ts);
  }

  return ret;
}

int ObCtxTxData::set_end_log_ts(const SCN &end_scn)
{
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);

  if (OB_FAIL(check_tx_data_writable_())) {
    TRANS_LOG(WARN, "tx data is not writeable", K(ret), K(*this));
  } else {
    tx_data_guard_.tx_data()->end_scn_.atomic_store(end_scn);
  }

  return ret;
}

int32_t ObCtxTxData::get_state() const
{
  RLockGuard guard(lock_);
  const ObTxData *tx_data = tx_data_guard_.tx_data();
  if (OB_ISNULL(tx_data)) {
    TRANS_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "tx data is unexpected nullptr", KPC(this));
    return 0;
  } else {
    return ATOMIC_LOAD(&tx_data->state_);
  }
}

const SCN ObCtxTxData::get_commit_version() const
{
  RLockGuard guard(lock_);
  const ObTxData *tx_data = tx_data_guard_.tx_data();
  if (OB_ISNULL(tx_data)) {
    TRANS_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "tx data is unexpected nullptr", KPC(this));
    return SCN::invalid_scn();
  } else {
    return tx_data->commit_version_.atomic_load();
  }
}

const SCN ObCtxTxData::get_start_log_ts() const
{
  RLockGuard guard(lock_);
  const ObTxData *tx_data = tx_data_guard_.tx_data();
  if (OB_ISNULL(tx_data)) {
    TRANS_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "tx data is unexpected nullptr", KPC(this));
    return SCN::invalid_scn();
  } else {
    return tx_data->start_scn_.atomic_load();
  }
}

const SCN ObCtxTxData::get_end_log_ts() const
{
  RLockGuard guard(lock_);
  const ObTxData *tx_data = tx_data_guard_.tx_data();
  if (OB_ISNULL(tx_data)) {
    TRANS_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "tx data is unexpected nullptr", KPC(this));
    return SCN::invalid_scn();
  } else {
    return tx_data->end_scn_.atomic_load();
  }
}

ObTransID ObCtxTxData::get_tx_id() const
{
  RLockGuard guard(lock_);
  const ObTxData *tx_data = tx_data_guard_.tx_data();
  if (OB_ISNULL(tx_data)) {
    TRANS_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "tx data is unexpected nullptr", KPC(this));
    return ObTransID(0);
  } else {
    return tx_data->tx_id_;
  }
}

int ObCtxTxData::get_tx_data(storage::ObTxDataGuard &tx_data_guard)
{
  int ret = OB_SUCCESS;
  ObTxData *tx_data = tx_data_guard_.tx_data();
  if (OB_ISNULL(tx_data)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "tx data is unexpected nullptr", KR(ret), KPC(this));
  } else {
    ret = tx_data_guard.init(tx_data);
  }
  return ret;
}

int ObCtxTxData::get_tx_data_ptr(storage::ObTxData *&tx_data_ptr)
{
  int ret = OB_SUCCESS;
  ObTxData *tx_data = tx_data_guard_.tx_data();
  if (OB_ISNULL(tx_data)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "tx data is unexpected nullptr", KR(ret), KPC(this));
  } else {
    tx_data_ptr = tx_data;
  }
  return ret;
}

int ObCtxTxData::prepare_add_undo_action(ObUndoAction &undo_action,
                                         storage::ObTxDataGuard &tmp_tx_data_guard,
                                         storage::ObUndoStatusNode *&tmp_undo_status)
{
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);
  /*
   * alloc undo_status_node used on commit stage
   * alloc tx_data and add undo_action to it, which will be inserted
   *       into tx_data_table after RollbackSavepoint log sync success
   */
  if (OB_FAIL(check_tx_data_writable_())) {
    TRANS_LOG(WARN, "tx data is not writeable", K(ret), K(*this));
  } else {
    ObTxTable *tx_table = nullptr;
    GET_TX_TABLE_(tx_table);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(tx_table->get_tx_data_table()->alloc_undo_status_node(tmp_undo_status))) {
      TRANS_LOG(WARN, "alloc undo status fail", K(ret), KPC(this));
    } else if (OB_ISNULL(tmp_undo_status)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "undo status is null", KR(ret), KPC(this));
    } else if (OB_FAIL(tx_table->deep_copy_tx_data(tx_data_guard_, tmp_tx_data_guard))) {
      TRANS_LOG(WARN, "copy tx data fail", K(ret), KPC(this));
    } else if (OB_ISNULL(tmp_tx_data_guard.tx_data())) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "copied tx_data is null", KR(ret), KPC(this));
    } else if (OB_FAIL(tmp_tx_data_guard.tx_data()->add_undo_action(tx_table, undo_action))) {
      TRANS_LOG(WARN, "add undo action fail", K(ret), KPC(this));
    }

    if (OB_FAIL(ret) && OB_NOT_NULL(tmp_undo_status)) {
      tx_table->get_tx_data_table()->free_undo_status_node(tmp_undo_status);
    }
  }
  return ret;
}

int ObCtxTxData::cancel_add_undo_action(storage::ObUndoStatusNode *tmp_undo_status)
{
  int ret = OB_SUCCESS;
  ObTxTable *tx_table = nullptr;
  GET_TX_TABLE_(tx_table);
  if (OB_SUCC(ret)) {
    ret = tx_table->get_tx_data_table()->free_undo_status_node(tmp_undo_status);
  }
  return ret;
}

int ObCtxTxData::commit_add_undo_action(ObUndoAction &undo_action, storage::ObUndoStatusNode *tmp_undo_status)
{
  return add_undo_action(undo_action, tmp_undo_status);
}

int ObCtxTxData::add_undo_action(ObUndoAction &undo_action, storage::ObUndoStatusNode *tmp_undo_status)
{
  int ret = OB_SUCCESS;
  RLockGuard guard(lock_);

  if (OB_FAIL(check_tx_data_writable_())) {
    TRANS_LOG(WARN, "tx data is not writeable", K(ret), K(*this));
  } else {
    ObTxTable *tx_table = nullptr;
    GET_TX_TABLE_(tx_table);
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(tx_data_guard_.tx_data()->add_undo_action(tx_table, undo_action, tmp_undo_status))) {
      TRANS_LOG(WARN, "add undo action failed", K(ret), K(undo_action), KP(tmp_undo_status), K(*this));
    };
  }

  return ret;
}

int ObCtxTxData::check_tx_data_writable_()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(tx_data_guard_.tx_data())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "tx_data_ is not valid", K(this));
  } else if (read_only_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "try to write a read-only tx_data", K(ret), K(this));
  }
  return ret;
}

int ObCtxTxData::insert_tx_data_(ObTxTable *tx_table, ObTxData *tx_data)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(tx_table)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), KP(tx_table));
  } else if (OB_ISNULL(tx_data)) {
    TRANS_LOG(INFO, "tx_data is nullptr, no need to insert", KP(tx_data), K(*this));
    // no need to insert, do nothing
  } else if (OB_FAIL(tx_table->insert(tx_data))) {
    TRANS_LOG(WARN, "insert into tx_table failed", K(ret), KPC(tx_data));
  }

  return ret;
}

int ObCtxTxData::revert_tx_data_(ObTxData *&tx_data)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(tx_data)) {
    TRANS_LOG(INFO, "tx_data is nullptr, no need to free", KP(tx_data), K(*this));
    // no need to free, do nothing
  } else {
    tx_data = nullptr;
  }
  return ret;
}

int ObCtxTxData::deep_copy_tx_data_(ObTxTable *tx_table, storage::ObTxDataGuard &out_tx_data_guard)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(tx_table)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), KP(tx_table));
  } else if (OB_ISNULL(tx_data_guard_.tx_data())) {
    TRANS_LOG(INFO, "tx_data_ is nullptr, no need to deep copy tx data", K(*this));
    // no need to free, do nothing
  } else if (OB_FAIL(tx_table->deep_copy_tx_data(tx_data_guard_, out_tx_data_guard))) {
    TRANS_LOG(WARN, "deep copy tx data failed", K(ret), K(tx_data_guard_), K(out_tx_data_guard), K(*this));
  }

  return ret;
}


} // namespace transaction

} // namespace oceanbase
