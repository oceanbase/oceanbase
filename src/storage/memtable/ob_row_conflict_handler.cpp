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
#include "storage/memtable/mvcc/ob_mvcc_iterator.h"
#include "storage/memtable/ob_lock_wait_mgr.h"

namespace oceanbase {
using namespace common;
using namespace memtable;
using namespace transaction;
namespace storage {
int ObRowConflictHandler::check_foreign_key_constraint_for_memtable(ObMvccValueIterator *value_iter,
                                                                    ObStoreRowLockState &lock_state)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(value_iter)) {
    ret = OB_BAD_NULL_ERROR;
    TRANS_LOG(ERROR, "the ObMvccValueIterator is null", K(ret));
  } else if (OB_FAIL(value_iter->check_row_locked(lock_state))) {
    TRANS_LOG(WARN, "check row locked fail", K(ret), K(lock_state));
  } else {
    const ObMvccAccessCtx *ctx = value_iter->get_mvcc_acc_ctx();
    const ObTransID my_tx_id = ctx->get_tx_id();
    const share::SCN snapshot_version = ctx->get_snapshot_version();
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

int ObRowConflictHandler::check_foreign_key_constraint_for_sstable(const ObTxTableGuard &tx_table_guard,
                                                                   const ObTransID &read_trans_id,
                                                                   const ObTransID &data_trans_id,
                                                                   const int64_t sql_sequence,
                                                                   const int64_t trans_version,
                                                                   const int64_t snapshot_version,
                                                                   ObStoreRowLockState &lock_state) {
  int ret = OB_SUCCESS;
  // If a transaction is committed, the trans_id of it is 0, which is invalid.
  // So we can not use check_row_locekd interface to get the trans_version.
  if (!data_trans_id.is_valid()) {
    if (trans_version > snapshot_version) {
      ret = OB_TRANSACTION_SET_VIOLATION;
      TRANS_LOG(WARN, "meet tsc on sstable", K(ret), K(lock_state.trans_version_), K(snapshot_version));
    }
  } else {
    ObTxTable *tx_table = nullptr;
    int64_t read_epoch = ObTxTable::INVALID_READ_EPOCH;
    if (!tx_table_guard.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "tx table guard is invalid", KR(ret));
    } else if (FALSE_IT(tx_table = tx_table_guard.get_tx_table())) {
    } else if (FALSE_IT(read_epoch = tx_table_guard.epoch())) {
    } else if (OB_FAIL(tx_table->check_row_locked(
                        read_trans_id, data_trans_id, sql_sequence, read_epoch, lock_state))){
      TRANS_LOG(WARN, "check row locked fail", K(ret), K(read_trans_id), K(data_trans_id), K(sql_sequence), K(read_epoch), K(lock_state));
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
                                                 const int64_t total_trans_node_cnt)
{
  int ret = OB_TRY_LOCK_ROW_CONFLICT;
  ObLockWaitMgr *lock_wait_mgr = NULL;
  ObTransID conflict_tx_id = lock_state.lock_trans_id_;
  // auto mem_ctx = acc_ctx.get_mem_ctx();
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
    TRANS_LOG(WARN, "can not get tenant lock_wait_mgr MTL", K(tx_desc->get_tenant_id()));
  } else {
    int tmp_ret = OB_SUCCESS;
    ObTransID tx_id = acc_ctx.get_tx_id();
    ObAddr conflict_scheduler_addr;
    if (OB_FAIL(ObTransDeadlockDetectorAdapter::
                get_trans_scheduler_info_on_participant(conflict_tx_id, ls_id, conflict_scheduler_addr))) {
      TRANS_LOG(WARN, "get transaction scheduler info fail", K(ret), K(conflict_tx_id), K(tx_id), K(ls_id));
    }
    ObTransIDAndAddr conflict_tx(conflict_tx_id, conflict_scheduler_addr);
    tx_desc->add_conflict_tx(conflict_tx);
    // The addr in tx_desc is the scheduler_addr of current trans,
    // and GCTX.self_addr() will retrun the addr where the row is stored
    // (i.e. where the trans is executing)
    bool remote_tx = tx_desc->get_addr() != GCTX.self_addr();
    ObFunction<int(bool&, bool&)> recheck_func([&](bool &locked, bool &wait_on_row) -> int {
      int ret = OB_SUCCESS;
      lock_state.is_locked_ = false;
      if (lock_state.is_delayed_cleanout_) {
        auto lock_data_sequence = lock_state.lock_data_sequence_;
        auto &tx_table_guard = acc_ctx.get_tx_table_guard();
        int64_t read_epoch = tx_table_guard.epoch();
        if (OB_FAIL(tx_table_guard.get_tx_table()->check_row_locked(
                tx_id, conflict_tx_id, lock_data_sequence, read_epoch, lock_state))) {
          TRANS_LOG(WARN, "re-check row locked via tx_table fail", K(ret), K(tx_id), K(lock_state));
        }
      } else {
        if (OB_FAIL(lock_state.mvcc_row_->check_row_locked(acc_ctx, lock_state))) {
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
                                       tx_id,
                                       conflict_tx_id,
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

}
}
