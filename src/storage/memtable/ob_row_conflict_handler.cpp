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
#define USING_LOG_PREFIX TRANS
#include "ob_row_conflict_handler.h"
#include "storage/memtable/ob_memtable.h"
#include "storage/blocksstable/ob_sstable.h"
#include "storage/memtable/mvcc/ob_mvcc_iterator.h"
#include "storage/memtable/ob_lock_wait_mgr.h"
#include "storage/tx_table/ob_tx_table_guards.h"
#include "storage/access/ob_rows_info.h"
#include "storage/ddl/ob_tablet_ddl_kv.h"

namespace oceanbase {
using namespace common;
using namespace memtable;
using namespace transaction;
namespace storage {
int ObRowConflictHandler::check_row_locked(const storage::ObTableIterParam &param,
                                           storage::ObTableAccessContext &context,
                                           const blocksstable::ObDatumRowkey &rowkey,
                                           const bool by_myself,
                                           const bool post_lock)
{
  int ret = OB_SUCCESS;
  ObStoreRowLockState lock_state;
  ObMvccAccessCtx &acc_ctx = context.store_ctx_->mvcc_acc_ctx_;
  share::SCN max_trans_version = share::SCN::min_scn();
  const ObTransID my_tx_id = acc_ctx.get_tx_id();
  const share::SCN snapshot_version = acc_ctx.get_snapshot_version();

  if (OB_FAIL(check_row_locked(param, context, rowkey, lock_state, max_trans_version))) {
    LOG_WARN("check row locked failed", K(ret), K(context), K(rowkey));
  } else {
    if (lock_state.is_locked_) {
      if ((by_myself && lock_state.lock_trans_id_ == my_tx_id)
          || (!by_myself && lock_state.lock_trans_id_ != my_tx_id)) {
        ret = OB_TRY_LOCK_ROW_CONFLICT;
        if (post_lock) {
          post_row_read_conflict(acc_ctx,
                                 rowkey.get_store_rowkey(),
                                 lock_state,
                                 context.tablet_id_,
                                 context.ls_id_,
                                 0,
                                 0, /* these two params get from mvcc_row, and for statistics, so we ignore them */
                                 lock_state.trans_scn_);
        }
      }
    } else if (max_trans_version > snapshot_version) {
      ret = OB_TRANSACTION_SET_VIOLATION;
    }
  }
  return ret;
}

int ObRowConflictHandler::check_row_locked(const storage::ObTableIterParam &param,
                                           storage::ObTableAccessContext &context,
                                           const blocksstable::ObDatumRowkey &rowkey,
                                           ObStoreRowLockState &lock_state,
                                           share::SCN &max_trans_version)
{
  int ret = OB_SUCCESS;
  ObMemtableKey mtk;
  ObMvccWriteGuard guard;
  ObStoreCtx *ctx = context.store_ctx_;

  if (!ctx->mvcc_acc_ctx_.is_write() || !rowkey.is_memtable_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid param", K(ret), KP(ctx), K(rowkey));
  } else if (OB_ISNULL(ctx->table_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "tables handle or iterator in context is null", K(ret), K(ctx));
  } else if (OB_FAIL(guard.write_auth(*ctx))) {
    TRANS_LOG(WARN, "not allow to write", KP(ctx));
  } else if (OB_FAIL(mtk.encode(param.get_read_info()->get_columns_desc(), &rowkey.get_store_rowkey()))) {
    TRANS_LOG(WARN, "mtk encode fail", "ret", ret);
  } else {
    const ObIArray<ObITable *> *stores = nullptr;
    common::ObSEArray<ObITable *, 4> iter_tables;
    ctx->table_iter_->resume();
    while (OB_SUCC(ret)) {
      ObITable *table_ptr = nullptr;
      if (OB_FAIL(ctx->table_iter_->get_next(table_ptr))) {
        if (OB_ITER_END != ret) {
          TRANS_LOG(WARN, "failed to get next tables", K(ret));
        }
      } else if (OB_ISNULL(table_ptr)) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "table must not be null", K(ret), KPC(ctx->table_iter_));
      } else if (OB_FAIL(iter_tables.push_back(table_ptr))) {
        TRANS_LOG(WARN, "rowkey_exists check::", K(ret), KPC(table_ptr));
      }
    } // end while
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
    if (OB_SUCC(ret)) {
      ObRowState row_state;
      share::SCN snapshot_version = ctx->mvcc_acc_ctx_.get_snapshot_version();
      stores = &iter_tables;
      for (int64_t i = stores->count() - 1; OB_SUCC(ret) && i >= 0; i--) {
        lock_state.reset();
        if (NULL == stores->at(i)) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(WARN, "ObIStore is null", K(ret), K(i));
        } else if (stores->at(i)->is_data_memtable()) {
          ObMemtable *memtable = static_cast<ObMemtable *>(stores->at(i));
          if (OB_FAIL(memtable->get_mvcc_engine().check_row_locked(ctx->mvcc_acc_ctx_,
                                                                   &mtk,
                                                                   lock_state,
                                                                   row_state))) {
            TRANS_LOG(WARN, "mvcc engine check row lock fail", K(ret), K(mtk));
          } else if (lock_state.is_locked_) {
            break;
          } else if (max_trans_version < lock_state.trans_version_) {
            max_trans_version = lock_state.trans_version_;
          }
        } else if (stores->at(i)->is_direct_load_memtable()) {
          ObDDLKV *ddl_kv = static_cast<ObDDLKV *>(stores->at(i));
          if (OB_FAIL(ddl_kv->check_row_locked(param, rowkey, context, lock_state, row_state))) {
            TRANS_LOG(WARN, "sstable check row lock fail", K(ret), K(rowkey));
          } else if (lock_state.is_locked_) {
            break;
          } else if (max_trans_version < row_state.max_trans_version_) {
            max_trans_version = row_state.max_trans_version_;
          }
          TRANS_LOG(DEBUG, "check_row_locked meet direct load memtable", K(ret), K(rowkey), K(row_state), K(*ddl_kv));
        } else if (stores->at(i)->is_sstable()) {
          blocksstable::ObSSTable *sstable = static_cast<blocksstable::ObSSTable *>(stores->at(i));
          if (OB_FAIL(sstable->check_row_locked(param, rowkey, context, lock_state, row_state))) {
            TRANS_LOG(WARN, "sstable check row lock fail", K(ret), K(rowkey));
          } else if (lock_state.is_locked_) {
            break;
          } else {
            if (max_trans_version < lock_state.trans_version_) {
              max_trans_version = lock_state.trans_version_;
            }
            if (max_trans_version < row_state.max_trans_version_) {
              max_trans_version = row_state.max_trans_version_;
            }
          }
          TRANS_LOG(DEBUG, "check_row_locked meet sstable", K(ret), K(rowkey), K(lock_state), K(row_state), K(*sstable));
        } else {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(ERROR, "unknown store type", K(ret));
        }
      }
    }
  }

  return ret;
}

int ObRowConflictHandler::check_foreign_key_constraint(const storage::ObTableIterParam &param,
                                                       storage::ObTableAccessContext &context,
                                                       const common::ObStoreRowkey &rowkey)
{
  int ret = OB_SUCCESS;
  ObMvccAccessCtx &acc_ctx = context.store_ctx_->mvcc_acc_ctx_;
  blocksstable::ObDatumRowkeyHelper rowkey_converter;
  blocksstable::ObDatumRowkey datum_rowkey;
  if (OB_FAIL(rowkey_converter.convert_datum_rowkey(rowkey.get_rowkey(), datum_rowkey))) {
    STORAGE_LOG(WARN, "Failed to convert datum rowkey", K(ret), K(rowkey));
  } else if (OB_FAIL(check_row_locked(param, context, datum_rowkey, false /* by_myself */, true /* post_lock */))) {
    if (OB_TRY_LOCK_ROW_CONFLICT == ret) {
      if (REACH_TIME_INTERVAL(1000 * 1000)) {
        TRANS_LOG(
          WARN, "meet lock conflict during check foreign key constraint", K(ret), K(acc_ctx.get_tx_id()), K(rowkey));
      }
    } else if (OB_TRANSACTION_SET_VIOLATION == ret) {
      if (REACH_TIME_INTERVAL(1000 * 1000)) {
        TRANS_LOG(WARN,
                  "meet tsc during check foreign key constraint",
                  K(ret),
                  K(acc_ctx.get_tx_id()),
                  K(acc_ctx.get_snapshot_version()));
      }
    } else {
      TRANS_LOG(WARN, "check row locked failed", K(param), K(context), K(rowkey));
    }
  }
  return ret;
}

int ObRowConflictHandler::check_foreign_key_constraint_for_memtable(ObMvccAccessCtx &ctx,
                                                                    ObMvccRow *value,
                                                                    ObStoreRowLockState &lock_state)
{
  int ret = OB_SUCCESS;
  storage::ObRowState row_state;
  if (OB_ISNULL(value)) {
    ret = OB_BAD_NULL_ERROR;
    TRANS_LOG(ERROR, "the ObMvccValueIterator is null", K(ret));
  } else if (OB_FAIL(value->check_row_locked(ctx, lock_state, row_state))) {
    TRANS_LOG(WARN, "check row locked fail", K(ret), K(lock_state));
  } else {
    const ObTransID my_tx_id = ctx.get_tx_id();
    const share::SCN snapshot_version = ctx.get_snapshot_version();
    if (lock_state.is_locked_ && my_tx_id != lock_state.lock_trans_id_) {
      ret = OB_TRY_LOCK_ROW_CONFLICT;
      if (REACH_TIME_INTERVAL(1000 * 1000)) {
        TRANS_LOG(WARN, "meet lock conflict on memtable", K(ret), K(lock_state.lock_trans_id_), K(my_tx_id));
      }
    } else if (!lock_state.is_locked_ && lock_state.trans_version_ > snapshot_version) {
      ret = OB_TRANSACTION_SET_VIOLATION;
      if (REACH_TIME_INTERVAL(1000 * 1000)) {
        TRANS_LOG(WARN, "meet tsc on memtable", K(ret), K(lock_state.trans_version_), K(snapshot_version));
      }
    }
  }
  return ret;
}

int ObRowConflictHandler::check_foreign_key_constraint_for_sstable(ObTxTableGuards &tx_table_guards,
                                                                   const ObTransID &read_trans_id,
                                                                   const ObTransID &data_trans_id,
                                                                   const ObTxSEQ &sql_sequence,
                                                                   const int64_t trans_version,
                                                                   const int64_t snapshot_version,
                                                                   const share::SCN &end_scn,
                                                                   ObStoreRowLockState &lock_state) {
  int ret = OB_SUCCESS;
  // If a transaction is committed, the trans_id of it is 0, which is invalid.
  // So we can not use check_row_locked interface to get the trans_version.
  if (!data_trans_id.is_valid()) {
    if (trans_version > snapshot_version) {
      ret = OB_TRANSACTION_SET_VIOLATION;
      TRANS_LOG(WARN, "meet tsc on sstable", K(ret), K(trans_version), K(snapshot_version));
    }
  } else {
    ObTxTable *tx_table = nullptr;
    if (!tx_table_guards.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "tx table guard is invalid", KR(ret));
    } else if (OB_FAIL(tx_table_guards.check_row_locked(
        read_trans_id, data_trans_id, sql_sequence, end_scn, lock_state))){
      TRANS_LOG(WARN, "check row locked fail", K(ret), K(read_trans_id), K(data_trans_id), K(sql_sequence), K(lock_state));
    }
    if (lock_state.is_locked_ && read_trans_id != lock_state.lock_trans_id_) {
      ret = OB_TRY_LOCK_ROW_CONFLICT;
      if (REACH_TIME_INTERVAL(1000 * 1000)) {
        TRANS_LOG(WARN, "meet lock conflict on sstable", K(ret), K(lock_state.lock_trans_id_), K(read_trans_id));
      }
    } else if (!lock_state.is_locked_ && lock_state.trans_version_.get_val_for_tx() > snapshot_version) {
      ret = OB_TRANSACTION_SET_VIOLATION;
      if (REACH_TIME_INTERVAL(1000 * 1000)) {
        TRANS_LOG(WARN, "meet tsc on sstable", K(ret), K(lock_state.trans_version_), K(snapshot_version));
      }
    }
  }
  return ret;
}

int ObRowConflictHandler::post_row_read_conflict(ObMvccAccessCtx &acc_ctx,
                                                 const ObStoreRowkey &row_key,
                                                 ObStoreRowLockState &lock_state,
                                                 const ObTabletID tablet_id,
                                                 const share::ObLSID ls_id,
                                                 const int64_t last_compact_cnt,
                                                 const int64_t total_trans_node_cnt,
                                                 const share::SCN &trans_scn)
{
  int ret = OB_TRY_LOCK_ROW_CONFLICT;
  ObLockWaitMgr *lock_wait_mgr = NULL;
  ObTransID conflict_tx_id = lock_state.lock_trans_id_;
  ObTxDesc *tx_desc = acc_ctx.get_tx_desc();
  int64_t current_ts = common::ObClockGenerator::getClock();
  int64_t lock_wait_start_ts = acc_ctx.get_lock_wait_start_ts() > 0
    ? acc_ctx.get_lock_wait_start_ts()
    : current_ts;
  int64_t lock_wait_expire_ts = acc_ctx.eval_lock_expire_ts(lock_wait_start_ts);
  if (current_ts >= lock_wait_expire_ts) {
    ret = OB_ERR_EXCLUSIVE_LOCK_CONFLICT;
    TRANS_LOG(WARN, "exclusive lock conflict", K(ret), K(row_key),
              K(conflict_tx_id), K(acc_ctx), K(lock_wait_expire_ts));
  } else if (OB_ISNULL(lock_wait_mgr = MTL_WITH_CHECK_TENANT(ObLockWaitMgr*,
                                                  tx_desc->get_tenant_id()))) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "can not get tenant lock_wait_mgr MTL", K(tx_desc->get_tenant_id()));
  } else {
    int tmp_ret = OB_SUCCESS;
    ObTransID tx_id = acc_ctx.get_tx_id();
    ObAddr conflict_scheduler_addr;
    // register to deadlock detector
    if (OB_FAIL(ObTransDeadlockDetectorAdapter::
                get_trans_scheduler_info_on_participant(conflict_tx_id, ls_id, conflict_scheduler_addr))) {
      TRANS_LOG(WARN, "get transaction scheduler info fail", K(ret), K(conflict_tx_id), K(tx_id), K(ls_id));
    } else {
      ObTransIDAndAddr conflict_tx(conflict_tx_id, conflict_scheduler_addr);
      tx_desc->add_conflict_tx(conflict_tx);
    }
    // The addr in tx_desc is the scheduler_addr of current trans,
    // and GCTX.self_addr() will return the addr where the row is stored
    // (i.e. where the trans is executing)
    bool remote_tx = tx_desc->get_addr() != GCTX.self_addr();
    ObFunction<int(bool&, bool&)> recheck_func([&](bool &locked, bool &wait_on_row) -> int {
      int ret = OB_SUCCESS;
      lock_state.is_locked_ = false;
      if (lock_state.is_delayed_cleanout_) {
        ObTxSEQ lock_data_sequence = lock_state.lock_data_sequence_;
        ObTxTableGuards &tx_table_guards = acc_ctx.get_tx_table_guards();
        if (OB_FAIL(tx_table_guards.check_row_locked(
                tx_id, conflict_tx_id, lock_data_sequence, trans_scn, lock_state))) {
          TRANS_LOG(WARN, "re-check row locked via tx_table fail", K(ret), K(tx_id), K(lock_state));
        }
      } else {
        storage::ObRowState row_state;
        if (OB_FAIL(lock_state.mvcc_row_->check_row_locked(acc_ctx, lock_state, row_state))) {
          TRANS_LOG(WARN, "re-check row locked via mvcc_row fail", K(ret), K(tx_id), K(lock_state));
        }
      }
      if (OB_SUCC(ret)) {
        locked = lock_state.is_locked_ && lock_state.lock_trans_id_ != tx_id;
        wait_on_row = !lock_state.is_delayed_cleanout_;
      }
      return ret;
    });
    tmp_ret = lock_wait_mgr->post_lock(OB_TRY_LOCK_ROW_CONFLICT,
                                       tablet_id,
                                       row_key,
                                       lock_wait_expire_ts,
                                       remote_tx,
                                       last_compact_cnt,
                                       total_trans_node_cnt,
                                       tx_desc->get_assoc_session_id(),
                                       tx_id,
                                       conflict_tx_id,
                                       ls_id,
                                       recheck_func);
    if (OB_SUCCESS != tmp_ret) {
      TRANS_LOG(WARN, "post_lock after tx conflict failed",
                K(tmp_ret), K(tx_id), K(conflict_tx_id));
    } else if (acc_ctx.get_lock_wait_start_ts() <= 0) {
      acc_ctx.set_lock_wait_start_ts(lock_wait_start_ts);
    }
  }
  return ret;
}
}  // namespace storage
}  // namespace oceanbase
