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

#define USING_LOG_PREFIX TABLELOCK

#include "storage/memtable/ob_memtable_context.h"
#include "storage/tx/ob_trans_ctx.h"
#include "storage/tablelock/ob_table_lock_callback.h"
#include "storage/tablelock/ob_lock_memtable.h"
#include "storage/tablelock/ob_mem_ctx_table_lock.h"
#include "storage/tx/ob_trans_part_ctx.h"

namespace oceanbase
{
using namespace memtable;
using namespace share;
using namespace storage;
namespace transaction
{

namespace tablelock
{

bool ObOBJLockCallback::on_memtable(const memtable::ObIMemtable * const memtable)
{
  return memtable == memtable_;
}

memtable::ObIMemtable* ObOBJLockCallback::get_memtable() const
{
  return memtable_;
}

int ObOBJLockCallback::log_sync(const SCN scn)
{
  int ret = OB_SUCCESS;
  ObMemtableCtx *mem_ctx = static_cast<ObMemtableCtx*>(ctx_);
  if (OB_UNLIKELY(SCN::max_scn() == scn)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("log ts should not be invalid", K(ret), K(scn), K(*this));
  } else if (OB_ISNULL(mem_ctx) || OB_ISNULL(lock_op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected error", K(ret), K(mem_ctx), K_(lock_op));
  } else {
    // only version after 3.3 has table lock.
    mem_ctx->update_max_submitted_seq_no(lock_op_->lock_op_.lock_seq_no_);
    // TODO: yanyuan.cxf maybe need removed.
    mem_ctx->set_log_synced(lock_op_, scn);
    scn_ = scn;
  }
  return ret;
}

int ObOBJLockCallback::print_callback()
{
  LOG_INFO("print callback", K(*this));
  return OB_SUCCESS;
}

int ObOBJLockCallback::trans_commit()
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("ObOBJLockCallback::trans_commit", K(*this));
  ObMemtableCtx *mem_ctx = static_cast<ObMemtableCtx*>(ctx_);
  switch (lock_op_->lock_op_.op_type_) {
  case IN_TRANS_DML_LOCK:
  case IN_TRANS_COMMON_LOCK: {
    // TODO: yanyuan.cxf can we use ObLockMemCtx?
    // 1. delete the lock in ObOBJLockMgr.
    // 2. delete the lock in memtable_ctx.
    if (OB_ISNULL(memtable_) || OB_ISNULL(lock_op_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid callback.", K(ret), K_(memtable), K_(lock_op));
    } else {
      memtable_->remove_lock_record(lock_op_->lock_op_);
      // TODO: yanyuan.cxf maybe we need only use mem_ctx toremove.
      mem_ctx->remove_lock_record(lock_op_);
      lock_op_ = NULL;
    }
    break;
  }
  default: {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("table lock callback failed.", K(ret), K(lock_op_->lock_op_));
  }
  }
  return ret;
}

int ObOBJLockCallback::trans_abort()
{
  return lock_abort_();
}

int ObOBJLockCallback::rollback_callback()
{
  return lock_abort_();
}

int ObOBJLockCallback::lock_abort_()
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("ObOBJLockCallback::lock_abort", K(*this));
  switch (lock_op_->lock_op_.op_type_) {
  case IN_TRANS_DML_LOCK:
  case IN_TRANS_COMMON_LOCK: {
    ObMemtableCtx *mem_ctx = static_cast<ObMemtableCtx*>(ctx_);
    // 1. delete the lock in ObLockMemtable.
    // 2. delete the lock in memtable_ctx.
    if (OB_ISNULL(memtable_) || OB_ISNULL(lock_op_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid callback.", K(ret), K_(memtable), K_(lock_op));
    } else {
      // TODO: yanyuan.cxf use only mem_ctx.
      memtable_->remove_lock_record(lock_op_->lock_op_);
      mem_ctx->remove_lock_record(lock_op_);
      lock_op_ = NULL;
    }
    break;
  }
  default: {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("table lock callback failed.", K(ret), K(lock_op_->lock_op_));
  }
  }
  return ret;
}

int ObOBJLockCallback::del()
{
  int ret = OB_SUCCESS;

  //delete from callback list
  ret = remove();
  return ret;
}

const common::ObTabletID ObOBJLockCallback::get_tablet_id()
{
  return memtable_->get_key().get_tablet_id();
}

int ObOBJLockCallback::get_redo(
    memtable::TableLockRedoDataNode &redo_node)
{
  int ret = OB_SUCCESS;
  if (NULL == key_.get_rowkey()) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("get redo failed", K(ret), K_(key));
  } else if (OB_FAIL(redo_node.set(&key_,
                                   lock_op_->lock_op_,
                                   memtable_->get_key().get_tablet_id(),
                                   this))) {
    LOG_WARN("get redo failed", K(ret), KP(this), KP_(lock_op));
  }
  LOG_DEBUG("ObOBJLockCallback::get_redo", K(ret), K(*this), K(lock_op_->lock_op_));
  return ret;
}

int ObOBJLockCallback::get_trans_id(ObTransID &trans_id) const
{
  int ret = OB_SUCCESS;
  ObMemtableCtx *mem_ctx = static_cast<ObMemtableCtx*>(ctx_);
  ObTransCtx *trans_ctx = NULL;

  if (OB_ISNULL(mem_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected mem ctx", K(ret));
  } else if (OB_ISNULL(trans_ctx = mem_ctx->get_trans_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected trans ctx", K(ret), K(ctx_));
  } else {
    trans_id = trans_ctx->get_trans_id();
  }

  return ret;
}

transaction::ObTxSEQ ObOBJLockCallback::get_seq_no() const
{
  return lock_op_->lock_op_.lock_seq_no_;
}

bool ObOBJLockCallback::must_log() const
{
  return (lock_op_->lock_op_.op_type_ != IN_TRANS_DML_LOCK &&
          lock_op_->lock_op_.op_type_ != IN_TRANS_COMMON_LOCK);
}

}

}

}
