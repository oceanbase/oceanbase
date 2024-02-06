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
#include "storage/memtable/ob_memtable_key.h"
#include "storage/memtable/ob_memtable_data.h"
#include "storage/memtable/ob_memtable.h"
#include "common/storage/ob_sequence.h"
#include "storage/tx/ob_trans_part_ctx.h"
#include "storage/memtable/ob_memtable_util.h"
#include "storage/tablelock/ob_table_lock_callback.h"
#include "storage/ls/ob_freezer.h"
namespace oceanbase
{
using namespace common;
using namespace share;
using namespace storage;
using namespace transaction::tablelock;
using namespace blocksstable;
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

bool ObIMvccCtx::is_prepared() const
{
  const SCN prepare_version = trans_version_.atomic_get();
  return (prepare_version >= SCN::min_scn() && SCN::max_scn() != prepare_version);
}

int ObIMvccCtx::inc_pending_log_size(const int64_t size)
{
  return trans_mgr_.inc_pending_log_size(size);
}

int ObIMvccCtx::register_row_commit_cb(
    const ObMemtableKey *key,
    ObMvccRow *value,
    ObMvccTransNode *node,
    const int64_t data_size,
    const ObRowData *old_row,
    ObMemtable *memtable,
    const transaction::ObTxSEQ seq_no,
    const int64_t column_cnt)
{
  int ret = OB_SUCCESS;
  const bool is_replay = false;
  ObMvccRowCallback *cb = NULL;

  if (OB_ISNULL(key)
      || OB_ISNULL(value)
      || OB_ISNULL(node)
      || data_size <= 0
      || OB_ISNULL(memtable)
      || column_cnt <= 0) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(key), K(value), K(node), K(data_size), K(memtable), K(column_cnt));
  } else if (OB_ISNULL(cb = alloc_row_callback(*this, *value, memtable))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TRANS_LOG(WARN, "alloc row callback failed", K(ret));
  } else {
    //统计当前trans_node占用的内存大小
    add_trans_mem_total_size(data_size);
    cb->set(key,
            node,
            data_size,
            old_row,
            is_replay,
            seq_no,
            column_cnt);
    cb->set_is_link();

    if (OB_FAIL(append_callback(cb))) {
      TRANS_LOG(ERROR, "register callback failed", K(*this), K(ret));
    }

    if (OB_FAIL(ret)) {
      callback_free(cb);
      TRANS_LOG(WARN, "append callback failed", K(ret));
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
  common::ObTimeGuard timeguard("ObIMvccCtx::register_row_replay_cb", 5 * 1000);
  if (OB_ISNULL(key) || OB_ISNULL(value) || OB_ISNULL(node)
      || data_size <= 0 || OB_ISNULL(memtable)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(key), K(value), K(node), K(data_size), K(memtable));
  } else if (OB_ISNULL(cb = alloc_row_callback(*this, *value, memtable))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TRANS_LOG(WARN, "alloc row callback failed", K(ret));
  } else if (FALSE_IT(timeguard.click("alloc_row_callback"))) {
  } else {
    cb->set(key,
            node,
            data_size,
            NULL,
            is_replay,
            seq_no,
            column_cnt);
    {
      ObRowLatchGuard guard(value->latch_);
      cb->link_trans_node();
    }
    timeguard.click("link_trans_node");

    cb->set_scn(scn);
    if (OB_FAIL(append_callback(cb))) {
      {
        ObRowLatchGuard guard(value->latch_);
        cb->unlink_trans_node();
      }
      TRANS_LOG(WARN, "append callback failed", K(ret));
    }
    timeguard.click("append_callback");

    if (OB_FAIL(ret)) {
      callback_free(cb);
      timeguard.click("callback_free");
      TRANS_LOG(WARN, "append callback failed", K(ret));
    }
  }
  return ret;
}

int ObIMvccCtx::register_table_lock_cb_(
    ObLockMemtable *memtable,
    ObMemCtxLockOpLinkNode *lock_op,
    ObOBJLockCallback *&cb)
{
  int ret = OB_SUCCESS;
  static ObFakeStoreRowKey tablelock_fake_rowkey("tbl", 3);
  const ObStoreRowkey &rowkey = tablelock_fake_rowkey.get_rowkey();
  ObMemtableKey mt_key;
  cb = nullptr;
  if (OB_ISNULL(cb = alloc_table_lock_callback(*this, memtable))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TRANS_LOG(WARN, "alloc row callback failed", K(ret));
  } else if (OB_FAIL(mt_key.encode(&rowkey))) {
    TRANS_LOG(WARN, "encode memtable key failed", K(ret));
  } else {
    cb->set(mt_key, lock_op);
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
                                             cb))) {
    TRANS_LOG(WARN, "register tablelock callback failed", K(ret), KPC(lock_op));
  } else {
    cb->set_scn(scn);
    update_max_submitted_seq_no(cb->get_seq_no());
    TRANS_LOG(DEBUG, "replay register table lock callback", K(*cb));
  }
  return ret;
}

ObMvccRowCallback *ObIMvccCtx::alloc_row_callback(ObIMvccCtx &ctx, ObMvccRow &value, ObMemtable *memtable)
{
  int ret = OB_SUCCESS;
  void *cb_buffer = NULL;
  ObMvccRowCallback *cb = NULL;
  if (NULL == (cb_buffer = callback_alloc(sizeof(*cb)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TRANS_LOG(WARN, "alloc ObRowCB cb_buffer fail", K(ret));
  } else if (NULL == (cb = new(cb_buffer) ObMvccRowCallback(ctx, value, memtable))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TRANS_LOG(WARN, "construct ObRowCB object fail", K(ret), "cb_buffer", cb_buffer);
  }
  return cb;
}

ObMvccRowCallback *ObIMvccCtx::alloc_row_callback(ObMvccRowCallback &cb, ObMemtable *memtable)
{
  int ret = OB_SUCCESS;
  void *cb_buffer = NULL;
  ObMvccRowCallback *new_cb = NULL;
  if (NULL == (cb_buffer = callback_alloc(sizeof(*new_cb)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TRANS_LOG(WARN, "alloc ObRowCB cb_buffer fail", K(ret));
  } else if (NULL == (new_cb = new(cb_buffer) ObMvccRowCallback(cb, memtable))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TRANS_LOG(WARN, "construct ObRowCB object fail", K(ret), "cb_buffer", cb_buffer);
  }
  return new_cb;
}

int ObIMvccCtx::append_callback(ObITransCallback *cb)
{
  return trans_mgr_.append(cb);
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
    auto tx_ctx = ctx_->get_trans_ctx();
    ctx_->write_done();
    if (OB_NOT_NULL(memtable_)) {
      bool is_freeze = memtable_->is_frozen_memtable();
      if (OB_FAIL(tx_ctx->submit_redo_log(is_freeze))) {
        if (REACH_TIME_INTERVAL(100 * 1000)) {
          TRANS_LOG(WARN, "failed to submit log if neccesary", K(ret), K(is_freeze), KPC(tx_ctx));
        }
        if (is_freeze) {
          memtable_->get_freezer()->set_need_resubmit_log(true);
        }
      }
    }
  }
}

int ObMvccWriteGuard::write_auth(storage::ObStoreCtx &store_ctx)
{
  int ret = common::OB_SUCCESS;
  auto mem_ctx = store_ctx.mvcc_acc_ctx_.mem_ctx_;
  if (!store_ctx.mvcc_acc_ctx_.is_write()) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "store_ctx was not prepared for write", K(ret), K(store_ctx));
  } else if (OB_FAIL(mem_ctx->write_auth(exclusive_))) {
    TRANS_LOG(WARN, "tx ctx write auth fail", K(ret),
              K(exclusive_), K(store_ctx), KPC(mem_ctx));
  } else {
    ctx_ = mem_ctx;
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
