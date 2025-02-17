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

#include "storage/memtable/mvcc/ob_mvcc_ctx.h"
#include "share/deadlock/ob_deadlock_detector_mgr.h"
#include "storage/memtable/ob_memtable_key.h"
#include "storage/memtable/ob_memtable_data.h"
#include "storage/memtable/ob_memtable.h"
#include "common/storage/ob_sequence.h"
#include "storage/tx/ob_trans_part_ctx.h"
#include "storage/memtable/ob_memtable_util.h"
#include "storage/tablelock/ob_table_lock_callback.h"
#include "storage/lob/ob_ext_info_callback.h"
#include "storage/ls/ob_freezer.h"
#include "storage/lock_wait_mgr/ob_lock_wait_mgr.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace storage;
using namespace transaction::tablelock;
using namespace blocksstable;
using namespace lockwaitmgr;
namespace memtable
{

void ObIMvccCtx::before_prepare(const SCN version)
{
#ifdef TRANS_ERROR
  const int random = (int)ObRandom::rand(1, 1000);
  ob_usleep(random);
#endif
  set_trans_version(version);
}

int ObIMvccCtx::register_row_commit_cb(const storage::ObTableIterParam &param,
                                       ObTxNodeArg &arg,
                                       ObMvccWriteResult &res,
                                       ObMemtable *memtable)
{
  int ret = OB_SUCCESS;
  const bool is_replay = false;
  const bool is_non_unique_local_index = param.is_non_unique_local_index_;

  if (!res.has_insert()) {
  } else {
    ObMvccRowCallback *cb = NULL;
    ObMvccRow *value = res.value_;
    ObMvccTransNode *node = res.tx_node_;

    // store_key's lifecycle needs to be the same as the lifecycle of the callback
    const ObMemtableKey *stored_key = &res.mtk_;
    const int64_t data_size = node->get_data_size();
    const ObRowData &old_row = arg.old_row_;
    const transaction::ObTxSEQ seq_no = arg.seq_no_;
    const int64_t column_cnt = arg.column_cnt_;

    if (OB_ISNULL(stored_key)
        || OB_ISNULL(value)
        || OB_ISNULL(node)
        || data_size <= 0
        || OB_ISNULL(memtable)
        || column_cnt <= 0) {
      ret = OB_INVALID_ARGUMENT;
      TRANS_LOG(WARN, "invalid argument", K(stored_key), K(value), K(node),
                K(data_size), K(memtable), K(column_cnt));
    } else if (OB_ISNULL(cb = alloc_row_callback(*this, *value, memtable))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TRANS_LOG(WARN, "alloc row callback failed", K(ret));
    } else {
      // not used now, is hotspot in pdml, so comment out
      // add_trans_mem_total_size(data_size);

      cb->set(stored_key,
              node,
              data_size,
              old_row,
              is_replay,
              seq_no,
              column_cnt,
              is_non_unique_local_index);
      cb->set_is_link();

      if (OB_FAIL(append_callback(cb))) {
        TRANS_LOG(ERROR, "register callback failed", K(*this), K(ret));
      }

      if (OB_FAIL(ret)) {
        free_mvcc_row_callback(cb);
        TRANS_LOG(WARN, "append callback failed", K(ret));
      } else {
        if (OB_LIKELY(ObDeadLockDetectorMgr::is_deadlock_enabled()) && !is_non_unique_local_index) {
          ACTIVE_SESSION_FLAG_SETTER_GUARD(in_deadlock_row_register);
          MTL(ObLockWaitMgr*)->insert_or_replace_hash_holder(
            LockHashHelper::hash_rowkey(memtable->get_tablet_id(), *stored_key),
            node->tx_id_,
            node->seq_no_,
            share::SCN());
        }
      }
    }
  }
  return ret;
}

int ObIMvccCtx::register_row_commit_cb(const storage::ObTableIterParam &param,
                                       ObTxNodeArgs &tx_node_args,
                                       ObMvccWriteResults &mvcc_results,
                                       ObMemtable *memtable)
{
  int ret = OB_SUCCESS;
  const bool is_replay = false;
  const bool is_non_unique_local_index = param.is_non_unique_local_index_;
  ObMvccRowCallback *head = NULL;
  ObMvccRowCallback *tail = NULL;
  int64_t length = 0;

  for (int64_t i = 0; OB_SUCC(ret) && i < mvcc_results.count(); ++i) {
    ObTxNodeArg &tx_node_arg = tx_node_args[i];
    ObMvccWriteResult &res = mvcc_results[i];

    if (res.has_insert()) {
      ObMvccRowCallback *cb = NULL;
      ObMvccRow *value = res.value_;
      ObMvccTransNode *node = res.tx_node_;

      // it needs to be the same as the lifecycle of the callback
      const ObMemtableKey *stored_key = &res.mtk_;
      const int64_t data_size = node->get_data_size();
      const ObRowData &old_row = tx_node_arg.old_row_;
      const transaction::ObTxSEQ seq_no = tx_node_arg.seq_no_;
      const int64_t column_cnt = tx_node_arg.column_cnt_;

      if (OB_ISNULL(stored_key)
          || OB_ISNULL(value)
          || OB_ISNULL(node)
          || data_size <= 0
          || OB_ISNULL(memtable)
          || column_cnt <= 0) {
        ret = OB_INVALID_ARGUMENT;
        TRANS_LOG(WARN, "invalid argument", K(tx_node_arg), K(res), K(ret));
      } else if (OB_ISNULL(cb = alloc_row_callback(*this, *value, memtable))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TRANS_LOG(WARN, "alloc row callback failed", K(ret));
      } else {
        // not used now, is hotspot in pdml, so comment out
        // add_trans_mem_total_size(data_size);

        cb->set(stored_key,
                node,
                data_size,
                old_row,
                is_replay,
                seq_no,
                column_cnt,
                is_non_unique_local_index);
        cb->set_is_link();

        if (nullptr == head) {
          head = cb;
          tail = cb;
        } else {
          cb->set_prev(tail);
          tail->set_next(cb);
          tail = cb;
        }
        length++;
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(append_callback(head, tail, length))) {
      TRANS_LOG(ERROR, "register callback failed", K(*this), K(ret));
    } else {
      TRANS_LOG(DEBUG, "register callback succeed", K(*this), K(ret),
                KPC(head), KPC(tail), K(length), K(mvcc_results));
    }
  }

  if (OB_FAIL(ret)) {
    ObITransCallback *remove_cb = head;
    while (nullptr != remove_cb) {
      ObITransCallback *next = remove_cb->get_next();
      free_mvcc_row_callback(remove_cb);
      remove_cb = next;
      TRANS_LOG(WARN, "free failed mvcc row callback succeed", K(ret), KPC(remove_cb));
    }
    head = nullptr;
    tail = nullptr;
  } else {
    if (OB_LIKELY(ObDeadLockDetectorMgr::is_deadlock_enabled()) && !is_non_unique_local_index) {
      ACTIVE_SESSION_FLAG_SETTER_GUARD(in_deadlock_row_register);
      for (int64_t i = 0; i < mvcc_results.count(); ++i) {
        ObMvccWriteResult &res = mvcc_results[i];
        if (res.has_insert()) {
          const ObMemtableKey *stored_key = &res.mtk_;
          ObMvccTransNode *node = res.tx_node_;
          MTL(ObLockWaitMgr*)->insert_or_replace_hash_holder(
            LockHashHelper::hash_rowkey(memtable->get_tablet_id(), *stored_key),
            node->tx_id_,
            node->seq_no_,
            share::SCN());
        }
      }
    }
  }

  return ret;
}

int ObIMvccCtx::register_row_replay_cb(
    const ObMemtableKey *key,
    ObMvccRow *value,
    ObMvccTransNode *node,
    const int64_t data_size,
    ObMemtable *memtable,
    const transaction::ObTxSEQ seq_no,
    const SCN scn,
    const int64_t column_cnt)
{
  int ret = OB_SUCCESS;
  const bool is_replay = true;
  ObMvccRowCallback *cb = NULL;
  if (OB_ISNULL(key) || OB_ISNULL(value) || OB_ISNULL(node)
      || data_size <= 0 || OB_ISNULL(memtable)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(key), K(value), K(node), K(data_size), K(memtable));
  } else if (OB_ISNULL(cb = alloc_row_callback(*this, *value, memtable))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TRANS_LOG(WARN, "alloc row callback failed", K(ret));
  } else {
    ObRowData empty_old_row;
    cb->set(key,
            node,
            data_size,
            empty_old_row,
            is_replay,
            seq_no,
            column_cnt,
            false/*is_non_unique_local_index_cb, not setted correctly now, fix later*/);
    {
      ObRowLatchGuard guard(value->latch_);
      cb->link_trans_node();
    }

    cb->set_scn(scn);
    if (OB_FAIL(append_callback(cb))) {
      {
        ObRowLatchGuard guard(value->latch_);
        cb->unlink_trans_node();
      }
      TRANS_LOG(WARN, "append callback failed", K(ret));
    }

    if (OB_FAIL(ret)) {
      free_mvcc_row_callback(cb);
      TRANS_LOG(WARN, "append callback failed", K(ret));
    } else {
      if (OB_LIKELY(ObDeadLockDetectorMgr::is_deadlock_enabled()) && OB_NOT_NULL(MTL(ObLockWaitMgr*))) {
        MTL(ObLockWaitMgr*)->insert_or_replace_hash_holder(
          LockHashHelper::hash_rowkey(memtable->get_tablet_id(), *key),
          node->tx_id_,
          node->seq_no_,
          node->scn_);
      }
    }
  }
  return ret;
}

int ObIMvccCtx::register_table_lock_cb_(
    ObLockMemtable *memtable,
    ObMemCtxLockOpLinkNode *lock_op,
    ObOBJLockCallback *&cb,
    const share::SCN replay_scn)
{
  int ret = OB_SUCCESS;
  static ObFakeStoreRowKey tablelock_fake_rowkey("tbl", 3);
  const ObStoreRowkey &rowkey = tablelock_fake_rowkey.get_rowkey();
  ObMemtableKey mt_key;
  cb = nullptr;
  if (OB_ISNULL(cb = create_table_lock_callback(*this, memtable))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TRANS_LOG(WARN, "alloc row callback failed", K(ret));
  } else if (OB_FAIL(mt_key.encode(&rowkey))) {
    TRANS_LOG(WARN, "encode memtable key failed", K(ret));
  } else {
    cb->set(mt_key, lock_op);
    if (replay_scn.is_valid()) {
      cb->set_scn(replay_scn);
    }
    if (OB_FAIL(append_callback(cb))) {
      TRANS_LOG(WARN, "append table lock callback failed", K(ret), K(*cb));
    } else {
      TRANS_LOG(DEBUG, "append table lock callback", K(*cb));
    }
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(cb)) {
    free_table_lock_callback(cb);
  }
  return ret;
}

int ObIMvccCtx::register_table_lock_cb(
    ObLockMemtable *memtable,
    ObMemCtxLockOpLinkNode *lock_op)
{
  int ret = OB_SUCCESS;
  ObOBJLockCallback *cb = nullptr;
  if (OB_ISNULL(memtable) ||
      OB_ISNULL(lock_op)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(memtable), K(lock_op));
  } else if (OB_FAIL(register_table_lock_cb_(memtable,
                                             lock_op,
                                             cb))) {
    TRANS_LOG(WARN, "register tablelock callback failed", K(ret), KPC(lock_op));
  } else {
    // do nothing
  }
  return ret;
}

int ObIMvccCtx::register_table_lock_replay_cb(
    ObLockMemtable *memtable,
    ObMemCtxLockOpLinkNode *lock_op,
    const SCN scn)
{
  int ret = OB_SUCCESS;
  ObOBJLockCallback *cb = nullptr;
  if (OB_ISNULL(memtable) ||
      OB_ISNULL(lock_op)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(memtable), K(lock_op));
  } else if (OB_FAIL(register_table_lock_cb_(memtable,
                                             lock_op,
                                             cb,
                                             scn))) {
    TRANS_LOG(WARN, "register tablelock callback failed", K(ret), KPC(lock_op));
  } else {
    TRANS_LOG(DEBUG, "replay register table lock callback", K(*cb));
  }
  return ret;
}

ObMvccRowCallback *ObIMvccCtx::alloc_row_callback(ObIMvccCtx &ctx, ObMvccRow &value, ObMemtable *memtable)
{
  int ret = OB_SUCCESS;
  void *cb_buffer = NULL;
  ObMvccRowCallback *cb = NULL;
  if (NULL == (cb_buffer = alloc_mvcc_row_callback())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TRANS_LOG(WARN, "alloc ObRowCB cb_buffer fail", K(ret));
  } else if (NULL == (cb = new(cb_buffer) ObMvccRowCallback(ctx, value, memtable))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TRANS_LOG(WARN, "construct ObRowCB object fail", K(ret), "cb_buffer", cb_buffer);
  }
  return cb;
}

void ObIMvccCtx::check_row_callback_registration_between_stmt_()
{
  ObIMemtableCtx *i_mem_ctx = (ObIMemtableCtx *)(this);
  transaction::ObPartTransCtx *trans_ctx =
    (transaction::ObPartTransCtx *)(i_mem_ctx->get_trans_ctx());
  if (NULL != trans_ctx && !trans_ctx->has_pending_write()) {
    TRANS_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "register commit not match expection", K(*trans_ctx));
  }
}

int ObIMvccCtx::register_ext_info_commit_cb(
    storage::ObStoreCtx &store_ctx,
    const int64_t timeout,
    const blocksstable::ObDmlFlag dml_flag,
    ObObj &index_data,
    ObObj &ext_info_data,
    const transaction::ObTxReadSnapshot &snapshot,
    const ObExtInfoLogHeader &header,
    const ObTabletID &tabelt_id)
{
  int ret = OB_SUCCESS;
  storage::ObExtInfoCbRegister cb_register;
  if (OB_FAIL(cb_register.register_cb(this, store_ctx, timeout, dml_flag, index_data, ext_info_data, snapshot, header, tabelt_id))) {
    TRANS_LOG(WARN, "register ext info callback failed", K(ret), K(cb_register), K(*this));
  }
  return ret;
}


}
}

#include "storage/tx/ob_trans_service.h"
#include "storage/tx/ob_trans_part_ctx.h"
namespace oceanbase {
namespace memtable {
ObMvccWriteGuard::~ObMvccWriteGuard()
{
  if (NULL != ctx_) {
    int ret = OB_SUCCESS;
    transaction::ObPartTransCtx *tx_ctx = ctx_->get_trans_ctx();
    ctx_->write_done();
    if (write_ret_ && OB_SUCCESS == *write_ret_
        && OB_NOT_NULL(memtable_)
        && try_flush_redo_) {
      bool is_freeze = memtable_->is_frozen_memtable();
      ret = tx_ctx->submit_redo_after_write(is_freeze/*force*/, write_seq_no_);
      if (OB_FAIL(ret)) {
        if (REACH_TIME_INTERVAL(100 * 1000)) {
          TRANS_LOG(WARN, "failed to submit log if neccesary", K(ret), K(is_freeze), KPC(tx_ctx));
        }
        if (is_freeze && OB_BLOCK_FROZEN != ret) {
          memtable_->get_freezer()->set_need_resubmit_log(true);
        }
      }
    } else if (is_lob_ext_info_log_) {
      if (OB_FAIL(tx_ctx->submit_redo_after_write(false/*force*/, write_seq_no_))) {
        if (REACH_TIME_INTERVAL(100 * 1000)) {
          TRANS_LOG(WARN, "failed to submit ext info log if neccesary", K(ret), KPC(tx_ctx));
        }
      }
    }
  }
}

int ObMvccWriteGuard::write_auth(storage::ObStoreCtx &store_ctx)
{
  int ret = common::OB_SUCCESS;
  ObMemtableCtx *mem_ctx = store_ctx.mvcc_acc_ctx_.mem_ctx_;
  if (!store_ctx.mvcc_acc_ctx_.is_write()) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "store_ctx was not prepared for write", K(ret), K(store_ctx));
  } else if (OB_FAIL(mem_ctx->write_auth(exclusive_))) {
    TRANS_LOG(WARN, "tx ctx write auth fail", K(ret),
              K(exclusive_), K(store_ctx), KPC(mem_ctx));
  } else {
    ctx_ = mem_ctx;
    try_flush_redo_ = !(store_ctx.mvcc_acc_ctx_.write_flag_.is_skip_flush_redo()
                        // for lob column write, delay flush redo to its main tablet's write
                        || store_ctx.mvcc_acc_ctx_.write_flag_.is_lob_aux());
    write_seq_no_ = store_ctx.mvcc_acc_ctx_.tx_scn_;
  }
  return ret;
}

void ObMvccAccessCtx::warn_tx_ctx_leaky_()
{
  int ret = OB_ERR_UNEXPECTED;
  TRANS_LOG(ERROR, "tx_ctx_ not null, may be leaky reference", K(ret), KP_(tx_ctx), KPC(this));
}
} // memtable
} // oceanbase
