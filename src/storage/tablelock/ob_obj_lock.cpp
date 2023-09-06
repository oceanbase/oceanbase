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

#include "share/ob_define.h"
#include "share/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "storage/memtable/ob_lock_wait_mgr.h"
#include "storage/tx/ob_trans_deadlock_adapter.h"
#include "storage/tx/ob_trans_define.h"
#include "storage/tx/ob_trans_part_ctx.h"
#include "storage/tablelock/ob_obj_lock.h"
#include "storage/tablelock/ob_table_lock_common.h"
#include "storage/tablelock/ob_mem_ctx_table_lock.h"
#include "storage/tablelock/ob_table_lock_iterator.h"
#include "storage/tablelock/ob_table_lock_rpc_struct.h"

namespace oceanbase
{

using namespace common;
using namespace storage;
using namespace memtable;
using namespace share;
using namespace palf;

namespace transaction
{

namespace tablelock
{
int64_t ObOBJLockFactory::alloc_count_ = 0;
int64_t ObOBJLockFactory::release_count_ = 0;
static const int64_t MAX_LOCK_CNT_IN_BUCKET = 10;
static const char *OB_TABLE_LOCK_NODE = "TableLockNode";
static const char *OB_TABLE_LOCK_MAP_ELEMENT = "TableLockMapEle";
static const char *OB_TABLE_LOCK_MAP = "TableLockMap";

bool ObTableLockOpLinkNode::is_complete_outtrans_lock() const
{
  return (lock_op_.op_type_ == OUT_TRANS_LOCK &&
          lock_op_.lock_op_status_ == LOCK_OP_COMPLETE);
}

bool ObTableLockOpLinkNode::is_complete_outtrans_unlock() const
{
  return (lock_op_.op_type_ == OUT_TRANS_UNLOCK &&
          lock_op_.lock_op_status_ == LOCK_OP_COMPLETE);
}

int ObTableLockOpLinkNode::init(const ObTableLockOp &op_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!op_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument ", K(ret), K(op_info));
  } else {
    lock_op_ = op_info;
  }
  return ret;
}

int ObTableLockOpLinkNode::assign(const ObTableLockOpLinkNode &other)
{
  int ret = OB_SUCCESS;
  if (!other.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument ", K(ret), K(other));
  } else {
    lock_op_ = other.lock_op_;
  }
  return ret;
}

int ObTableLockOpLinkNode::get_table_lock_store_info(ObTableLockOp &info)
{
  int ret = OB_SUCCESS;
  // store all the element at lock op
  info = lock_op_;
  return ret;
}

ObOBJLock::ObOBJLock(const ObLockID &lock_id) : lock_id_(lock_id)
{
  is_deleted_ = false;
  row_share_ = 0;
  row_exclusive_ = 0;
  memset(map_, 0, sizeof(ObTableLockOpList *) * TABLE_LOCK_MODE_COUNT);
}

int ObOBJLock::get_index_by_lock_mode(ObTableLockMode mode)
{
  int index = 0;
  switch(mode) {
  case ROW_SHARE: { index = 0; break; }
  case ROW_EXCLUSIVE: { index = 1; break; }
  case SHARE: { index = 2; break; }
  case SHARE_ROW_EXCLUSIVE: { index = 3; break; }
  case EXCLUSIVE: { index = 4; break; }
  default: { index = -1; }
  }
  return index;
}

int ObOBJLock::recover_(
    const ObTableLockOp &lock_op,
    ObMalloc &allocator)
{
  int ret = OB_SUCCESS;
  void *ptr = NULL;
  ObTableLockOpList *op_list = NULL;
  ObTableLockOpLinkNode *lock_op_node = NULL;
  bool need_recover = true;
  uint64_t tenant_id = MTL_ID();
  ObMemAttr attr(tenant_id, "ObTableLockOp");
  // 1. record lock op.
  if (OB_LIKELY(!lock_op.need_record_lock_op())) {
    // only have lock op, should not have unlock op.
    if (lock_op.lock_mode_ == ROW_EXCLUSIVE) {
      lock_row_exclusive_();
    } else if (lock_op.lock_mode_ == ROW_SHARE) {
      lock_row_share_();
    }
  } else if (OB_FAIL(get_or_create_op_list(lock_op.lock_mode_,
                                           tenant_id,
                                           allocator,
                                           op_list))) {
    LOG_WARN("get or create owner map failed.", K(ret));
  } else if (FALSE_IT(check_need_recover_(lock_op,
                                          op_list,
                                          need_recover))) {
    LOG_WARN("check need recover table lock failed.", K(ret));
  } else if (!need_recover) {
    // the same lock op exist. do nothing.
  } else if (OB_ISNULL(ptr = allocator.alloc(sizeof(ObTableLockOpLinkNode), attr))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alllocate ObTableLockOpLinkNode ", K(ret));
  } else if (FALSE_IT(lock_op_node = new(ptr) ObTableLockOpLinkNode())) {
  } else if (OB_FAIL(lock_op_node->init(lock_op))) {
    LOG_WARN("init lock owner failed.", K(ret), K(lock_op));
  } else if (!op_list->add_last(lock_op_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("add lock failed.", K(ret), K(lock_op));
  } else {
    LOG_DEBUG("succeed create lock ", K(lock_op));
  }
  if (OB_FAIL(ret) && NULL != lock_op_node) {
    lock_op_node->~ObTableLockOpLinkNode();
    allocator.free(lock_op_node);
    lock_op_node = NULL;
    // drop list, should never fail.
    drop_op_list_if_empty_(lock_op.lock_mode_,
                           op_list, allocator);
  }
  return ret;
}

int ObOBJLock::slow_lock(
    const ObLockParam &param,
    const ObTableLockOp &lock_op,
    const ObTableLockMode &lock_mode_in_same_trans,
    ObMalloc &allocator,
    ObTxIDSet &conflict_tx_set)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  void *ptr = NULL;
  ObTableLockOpList *op_list = NULL;
  ObTableLockOpLinkNode *lock_op_node = NULL;
  uint64_t tenant_id = MTL_ID();
  bool conflict_with_dml_lock = false;
  ObMemAttr attr(tenant_id, "ObTableLockOp");
  // 1. check lock conflict.
  // 2. record lock op.
  WRLockGuard guard(rwlock_);
  if (is_deleted_) {
    ret = OB_EAGAIN;
  } else if (OB_FAIL(check_allow_lock_(lock_op,
                                       lock_mode_in_same_trans,
                                       conflict_tx_set,
                                       conflict_with_dml_lock))) {
    if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
      LOG_WARN("check allow lock failed", K(ret), K(lock_op));
    }
  } else if (OB_FAIL(get_or_create_op_list(lock_op.lock_mode_,
                                           tenant_id,
                                           allocator,
                                           op_list))) {
    LOG_WARN("get or create owner map failed.", K(ret));
  } else if (OB_ISNULL(ptr = allocator.alloc(sizeof(ObTableLockOpLinkNode), attr))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alllocate ObTableLockOpLinkNode ", K(ret));
  } else if (FALSE_IT(lock_op_node = new(ptr) ObTableLockOpLinkNode())) {
    // do nothing
  } else if (OB_FAIL(lock_op_node->init(lock_op))) {
    LOG_WARN("init lock owner failed.", K(ret), K(lock_op));
  } else if (!op_list->add_last(lock_op_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("add lock failed.", K(ret), K(lock_op));
  } else {
    LOG_DEBUG("succeed create lock ", K(lock_op));
  }
  if (OB_FAIL(ret) && NULL != lock_op_node) {
    lock_op_node->~ObTableLockOpLinkNode();
    allocator.free(lock_op_node);
    lock_op_node = NULL;
    // drop list, should never fail.
    drop_op_list_if_empty_(lock_op.lock_mode_,
                           op_list, allocator);
  }
  // 1. need retry basic conditions
  // lock conflict and it is not try lock.
  // 2. need retry second conditions
  // out trans lock or in trans lock table lock should not retry if
  // it is conflict with dml lock.
  if (ret == OB_TRY_LOCK_ROW_CONFLICT && !param.is_try_lock_) {
    if (!lock_op.is_dml_lock_op() &&
        conflict_with_dml_lock &&
        param.is_deadlock_avoid_enabled_) {
      ret = OB_TRANS_KILLED;
    }
  }

  return ret;
}

int ObOBJLock::unlock_(
    const ObTableLockOp &unlock_op,
    ObMalloc &allocator)
{
  int ret = OB_SUCCESS;
  void *ptr = NULL;
  ObTableLockOpList *op_list = NULL;
  ObTableLockOpLinkNode *lock_op = NULL;
  uint64_t tenant_id = MTL_ID();
  ObMemAttr attr(tenant_id, "ObTableLockOp");
  // 1. check unlock op conflict.
  // 2. record lock op.
  if (OB_FAIL(check_allow_unlock_(unlock_op))) {
    LOG_WARN("check allow unlock failed", K(ret), K(unlock_op));
  } else if (OB_FAIL(get_op_list(unlock_op.lock_mode_,
                                 op_list))) {
    LOG_WARN("get op list failed.", K(ret));
  } else if (OB_UNLIKELY(OB_ISNULL(op_list))) {
    ret = OB_OBJ_LOCK_NOT_EXIST;
    LOG_WARN("there is no lock op, no need unlock.", K(ret), K(unlock_op));
  } else if (OB_ISNULL(ptr = allocator.alloc(sizeof(ObTableLockOpLinkNode), attr))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alllocate ObTableLockOpLinkNode ", K(ret));
  } else if (FALSE_IT(lock_op = new(ptr) ObTableLockOpLinkNode())) {
    // do nothing
  } else if (OB_FAIL(lock_op->init(unlock_op))) {
    LOG_WARN("init lock owner failed.", K(ret), K(unlock_op));
  } else if (!op_list->add_last(lock_op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("add lock failed.", K(ret), K(unlock_op));
  } else {
    LOG_DEBUG("succeed create unlock op ", K(unlock_op));
  }
  if (OB_FAIL(ret) && NULL != lock_op) {
    lock_op->~ObTableLockOpLinkNode();
    allocator.free(lock_op);
    lock_op = NULL;
  }

  return ret;
}

int ObOBJLock::recover_lock(
    const ObTableLockOp &lock_op,
    ObMalloc &allocator)
{
  int ret = OB_SUCCESS;
  common::ObTimeGuard timeguard("recover_lock", 10 * 1000);
  if (OB_UNLIKELY(!lock_op.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument.", K(ret), K(lock_op));
  } else if (FALSE_IT(timeguard.click("start"))) {
  } else if (OB_LIKELY(!lock_op.need_record_lock_op())) {
    RDLockGuard guard(rwlock_);
    timeguard.click("rlock");
    if (is_deleted_) {
      // need retry from upper layer.
      ret = OB_EAGAIN;
    } else if (OB_FAIL(recover_(lock_op, allocator))) {
      LOG_WARN("recover lock failed.", K(ret), K(lock_op));
    }
  } else {
    WRLockGuard guard(rwlock_);
    timeguard.click("wlock");
    if (is_deleted_) {
      // need retry from upper layer.
      ret = OB_EAGAIN;
    } else if (OB_FAIL(recover_(lock_op, allocator))) {
      LOG_WARN("recover lock failed.", K(ret), K(lock_op));
    }
  }
  LOG_DEBUG("recover table lock", K(ret), K(lock_op));
  return ret;
}

int ObOBJLock::update_lock_status_(
    const ObTableLockOp &lock_op,
    const SCN &commit_version,
    const SCN &commit_scn,
    const ObTableLockOpStatus status,
    ObTableLockOpList *op_list)
{
  int ret = OB_SUCCESS;
  bool find = false;
  // check all the conditions.
  DLIST_FOREACH_NORET(curr, *op_list) {
    if (curr->lock_op_.owner_id_ == lock_op.owner_id_ &&
        curr->lock_op_.create_trans_id_ == lock_op.create_trans_id_ &&
        curr->lock_op_.op_type_ == lock_op.op_type_) {
      find = true;
      curr->lock_op_.lock_op_status_ = status;
      curr->lock_op_.commit_version_ = commit_version;
      curr->lock_op_.commit_scn_ = commit_scn;
      LOG_DEBUG("update_lock_status_", K(curr->lock_op_));
      break;
    }
  }
  if (!find) {
    ret = OB_OBJ_LOCK_NOT_EXIST;
  }
  return ret;
}

int ObOBJLock::update_lock_status(const ObTableLockOp &lock_op,
                                  const SCN commit_version,
                                  const SCN commit_scn,
                                  const ObTableLockOpStatus status,
                                  ObMalloc &allocator)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool unused_is_compacted = true;
  ObTableLockOpList *op_list = NULL;
  if (OB_UNLIKELY(!lock_op.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument.", K(ret), K(lock_op));
  } else if (OB_LIKELY(!lock_op.need_record_lock_op())) {
    // do nothing
  } else {
    {
      // update the lock status to complete
      RDLockGuard guard(rwlock_);
      if (is_deleted_) {
        // the op is deleted, no need update its status.
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("the lock should not be deleted while update lock status", K(ret), K(lock_op));
      } else if (OB_FAIL(get_op_list(lock_op.lock_mode_,
                                     op_list))) {
        LOG_WARN("get lock list failed", K(ret), K(lock_op));
      } else if (OB_UNLIKELY(OB_ISNULL(op_list))) {
        ret = OB_OBJ_LOCK_NOT_EXIST;
        LOG_WARN("there is no lock op, no need update status.", K(ret),
                 K(lock_op), K(status));
      } else {
        ret = update_lock_status_(lock_op,
                                  commit_version,
                                  commit_scn,
                                  status,
                                  op_list);
      }
    }
    // compact the lock op
    if (OB_SUCC(ret) &&
        lock_op.op_type_ == OUT_TRANS_UNLOCK &&
        status == LOCK_OP_COMPLETE) {
      WRLockGuard guard(rwlock_);
      if (is_deleted_) {
        // the op is deleted, no need update its status.
        LOG_WARN("the lock is deleted, no need do compact", K(lock_op));
      } else if (OB_TMP_FAIL(get_op_list(lock_op.lock_mode_,
                                         op_list))) {
        LOG_WARN("get lock list failed, no need do compact", K(tmp_ret), K(lock_op));
      } else if (OB_TMP_FAIL(compact_tablelock_(lock_op,
                                                op_list,
                                                allocator,
                                                unused_is_compacted))) {
        LOG_WARN("compact tablelock failed", K(tmp_ret), K(lock_op));
      } else {
        drop_op_list_if_empty_(lock_op.lock_mode_, op_list, allocator);
      }
    }
    if (OB_SUCC(ret) &&
        lock_op.op_type_ == OUT_TRANS_UNLOCK &&
        status == LOCK_OP_COMPLETE) {
      wakeup_waiters_(lock_op);
    }
  }
  LOG_DEBUG("update lock status", K(ret), K(lock_op), K(commit_version), K(status));
  return ret;
}

int ObOBJLock::try_fast_lock_(
    const ObTableLockOp &lock_op,
    const ObTableLockMode &lock_mode_in_same_trans,
    ObTxIDSet &conflict_tx_set)
{
  int ret = OB_SUCCESS;
  bool unused_conflict_with_dml_lock = false;
  if (is_deleted_) {
    ret = OB_EAGAIN;
  } else if (OB_UNLIKELY(lock_op.need_record_lock_op())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("this lock op should not do fast lock", KR(ret), K(lock_op));
  } else if (OB_FAIL(check_allow_lock_(lock_op,
                                       lock_mode_in_same_trans,
                                       conflict_tx_set,
                                       unused_conflict_with_dml_lock))) {
    if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
      LOG_WARN("check allow lock failed", K(ret), K(lock_op));
    }
  } else {
    if (lock_op.lock_mode_ == ROW_EXCLUSIVE) {
      lock_row_exclusive_();
    } else if (lock_op.lock_mode_ == ROW_SHARE) {
      lock_row_share_();
    }
    LOG_DEBUG("succeed create lock ", K(lock_op));
  }
  return ret;
}

int ObOBJLock::fast_lock(
    const ObLockParam &param,
    const ObTableLockOp &lock_op,
    const ObTableLockMode &lock_mode_in_same_trans,
    ObMalloc &allocator,
    ObTxIDSet &conflict_tx_set)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  {
    // lock first time
    RDLockGuard guard(rwlock_);
    if (OB_FAIL(try_fast_lock_(lock_op,
                               lock_mode_in_same_trans,
                               conflict_tx_set))) {
      if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_EAGAIN != ret) {
        LOG_WARN("try fast lock failed", KR(ret), K(lock_op));
      }
    } else {
      LOG_DEBUG("succeed create lock ", K(lock_op));
    }
  }
  return ret;
}

int ObOBJLock::lock(
    const ObLockParam &param,
    ObStoreCtx &ctx,
    const ObTableLockOp &lock_op,
    const ObTableLockMode &lock_mode_in_same_trans,
    ObMalloc &allocator,
    ObTxIDSet &conflict_tx_set)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t USLEEP_TIME = 100; // 0.1 ms
  // 1. lock myself.
  // 2. try to lock.
  LOG_DEBUG("ObOBJLock::lock ", K(param), K(lock_op));
  if (OB_UNLIKELY(!lock_op.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument.", K(ret), K(lock_op));
  } else {
    if (OB_LIKELY(!lock_op.need_record_lock_op())) {
      if (OB_FAIL(fast_lock(param,
                            lock_op,
                            lock_mode_in_same_trans,
                            allocator,
                            conflict_tx_set))) {
        if (ret != OB_TRY_LOCK_ROW_CONFLICT &&
            ret != OB_EAGAIN) {
          LOG_WARN("lock failed.", K(ret), K(lock_op));
        }
      }
    } else if (OB_FAIL(slow_lock(param,
                                 lock_op,
                                 lock_mode_in_same_trans,
                                 allocator,
                                 conflict_tx_set))) {
      if (ret != OB_TRY_LOCK_ROW_CONFLICT &&
          ret != OB_EAGAIN) {
        LOG_WARN("lock failed.", K(ret), K(lock_op));
      }
    }

    if (OB_FAIL(ret) && REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
      LOG_WARN("ObOBJLock::lock ", K(ret), K(param), K(lock_op));
      print();
    }
  }
  LOG_DEBUG("ObOBJLock::lock finish", K(ret), K(conflict_tx_set));
  return ret;
}

int ObOBJLock::unlock(
    const ObTableLockOp &unlock_op,
    const bool is_try_lock,
    const int64_t expired_time,
    ObMalloc &allocator)
{
  int ret = OB_SUCCESS;
  int64_t USLEEP_TIME = 100; // 0.1 ms
  // 1. lock myself.
  // 2. try to unlock.
  LOG_DEBUG("ObOBJLock::unlock ", K(is_try_lock), K(expired_time), K(unlock_op));
  if (OB_UNLIKELY(!unlock_op.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument.", K(ret), K(unlock_op));
  } else if (OB_UNLIKELY(!unlock_op.need_record_lock_op())) {
    // should be only slow lock.
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("should only slow lock op", K(ret), K(unlock_op));
  } else {
    {
      WRLockGuard guard(rwlock_);
      if (!is_try_lock && OB_UNLIKELY(ObClockGenerator::getClock() >= expired_time)) {
        ret = (ret == OB_SUCCESS ? OB_TIMEOUT : ret);
        LOG_WARN("unlock is timeout", K(ret), K(unlock_op));
      } else if (is_deleted_) {
        // need retry from upper layer.
        ret = OB_EAGAIN;
      } else if (OB_FAIL(unlock_(unlock_op, allocator))) {
        if (!is_need_retry_unlock_error(ret)) {
          LOG_WARN("unlock failed.", K(ret), K(unlock_op));
        }
      }
    }
    if (OB_FAIL(ret) && REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
      LOG_WARN("ObOBJLock::unlock ", K(ret), K(is_try_lock),
               K(expired_time), K(unlock_op));
      print();
    }
  }
  LOG_DEBUG("ObOBJLock::unlock finish.", K(ret));

  return ret;
}
void ObOBJLock::remove_lock_op(
    const ObTableLockOp &lock_op,
    ObMalloc &allocator)
{
  int ret = OB_SUCCESS;
  ObTableLockOpList *op_list = NULL;
  int map_index = 0;

  LOG_DEBUG("ObOBJLock::remove_lock_op ", K(lock_op));
  if (OB_UNLIKELY(!lock_op.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument.", K(ret), K(lock_op));
  } else if (OB_LIKELY(!lock_op.need_record_lock_op())) {
    if (lock_op.lock_mode_ == ROW_EXCLUSIVE) {
      unlock_row_exclusive_();
    } else if (lock_op.lock_mode_ == ROW_SHARE) {
      unlock_row_share_();
    }
  } else if (FALSE_IT(map_index = get_index_by_lock_mode(lock_op.lock_mode_))) {
  } else if (OB_UNLIKELY(map_index < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid lock mode", K(ret), K(lock_op), K(map_index));
  } else {
    WRLockGuard guard(rwlock_);
    op_list = map_[map_index];
    delete_lock_op_from_list_(lock_op,
                              op_list,
                              allocator);
    drop_op_list_if_empty_(lock_op.lock_mode_,
                           op_list,
                           allocator);
    wakeup_waiters_(lock_op);
  }
  LOG_DEBUG("ObOBJLock::remove_lock_op finish.");
}

void ObOBJLock::wakeup_waiters_(const ObTableLockOp &lock_op)
{
  // dml in trans lock does not need do this.
  if (OB_LIKELY(!lock_op.need_wakeup_waiter())) {
    // do nothing
  } else if (OB_ISNULL(MTL(ObLockWaitMgr*))) {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "MTL(ObLockWaitMgr*) is null");
  } else {
    MTL(ObLockWaitMgr*)->wakeup(lock_op.lock_id_);
    LOG_DEBUG("ObOBJLock::wakeup_waiters_ ", K(lock_op));
  }
}

SCN ObOBJLock::get_min_ddl_lock_committed_scn(const SCN &flushed_scn) const
{
  int ret = OB_SUCCESS;
  SCN min_rec_scn = SCN::max_scn();
  RDLockGuard guard(rwlock_);
  for (int i = 0; i < TABLE_LOCK_MODE_COUNT; i++) {
    ObTableLockOpList *op_list = map_[i];
    if (op_list != NULL) {
      DLIST_FOREACH(curr, *op_list) {
        if (curr->lock_op_.op_type_ ==  OUT_TRANS_LOCK
            && curr->lock_op_.lock_op_status_ == LOCK_OP_COMPLETE
            && curr->lock_op_.commit_scn_ > flushed_scn
            && curr->lock_op_.commit_scn_ < min_rec_scn) {
          min_rec_scn = curr->lock_op_.commit_scn_;
        }
      }
    }
  }
  return min_rec_scn;
}

int ObOBJLock::get_table_lock_store_info(
    ObIArray<ObTableLockOp> &store_arr,
    const SCN &freeze_scn)
{
  int ret = OB_SUCCESS;
  RDLockGuard guard(rwlock_);
  for (int i = 0; i < TABLE_LOCK_MODE_COUNT; i++) {
    ObTableLockOpList *op_list = map_[i];
    if (op_list != NULL) {
      DLIST_FOREACH(curr, *op_list) {
        if (curr->lock_op_.commit_scn_ <= freeze_scn &&
            (curr->is_complete_outtrans_lock() || curr->is_complete_outtrans_unlock())) {
          ObTableLockOp store_info;
          if(OB_FAIL(curr->get_table_lock_store_info(store_info))) {
            LOG_WARN("get_table_lock_store_info failed", K(ret));
          } else if (OB_FAIL(store_arr.push_back(store_info))) {
            LOG_WARN("failed to push back table lock info arr", K(ret));
          }
        }

        if (ret != OB_SUCCESS) {
          break;
        }
      }
    }
    if (ret != OB_SUCCESS) {
      break;
    }
  }
  return ret;
}

int ObOBJLock::compact_tablelock(ObMalloc &allocator,
                                 bool &is_compacted,
                                 const bool is_force) {
  int ret = OB_SUCCESS;
  WRLockGuard guard(rwlock_);
  if (OB_FAIL(compact_tablelock_(allocator, is_compacted, is_force))) {
    LOG_WARN("compact table lock failed", K(ret), K(is_compacted), K(is_force));
  }
  return ret;
}

bool ObOBJLockMap::GetTableLockStoreInfoFunctor::operator() (
    ObOBJLock *obj_lock)
{
  int ret = common::OB_SUCCESS;
  bool bool_ret = false;

  if (OB_ISNULL(obj_lock)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "map", OB_P(obj_lock));
  } else if (OB_FAIL(obj_lock->get_table_lock_store_info(store_arr_, freeze_scn_))) {
    LOG_WARN("get table lock store info failed", K(ret));
  }

  if (OB_SUCCESS == ret) {
    bool_ret = true;
  }
  return bool_ret;
}

int ObOBJLockMap::get_table_lock_store_info(ObIArray<ObTableLockOp> &store_arr, SCN freeze_scn)
{
  int ret = OB_SUCCESS;
  GetTableLockStoreInfoFunctor fn(store_arr, freeze_scn);
  if (OB_FAIL(lock_map_.for_each(fn))) {
    LOG_WARN("for each get_table_lock_store_info failed", KR(ret));
  }
  return ret;
}

int ObOBJLockMap::get_lock_id_iter(ObLockIDIterator &iter)
{
  int ret = OB_SUCCESS;
  LockIDIterFunctor fn(iter);
  if (OB_FAIL(lock_map_.for_each(fn))) {
    TABLELOCK_LOG(WARN, "get lock id iterator failed", K(ret), K(fn.get_ret_code()));
    ret = fn.get_ret_code();
  } else if (OB_FAIL(iter.set_ready())) {
    TABLELOCK_LOG(WARN, "iterator set ready failed", K(ret));
  } else {
    // do nothing
  }
  return ret;
}

int ObOBJLockMap::get_lock_op_iter(const ObLockID &lock_id,
                                   ObLockOpIterator &iter)
{
  int ret = OB_SUCCESS;
  ObOBJLock *obj_lock = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TABLELOCK_LOG(WARN, "ObOBJLockMap is not inited", K(ret));
  } else if (OB_UNLIKELY(!lock_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TABLELOCK_LOG(WARN, "invalid argument.", K(ret), K(lock_id));
  } else if (OB_FAIL(get_obj_lock_with_ref_(lock_id, obj_lock))) {
    if (ret != OB_OBJ_LOCK_NOT_EXIST) {
      TABLELOCK_LOG(WARN, "get owner map failed.", K(ret), K(lock_id));
      // obj lock is deleted just now. continue iterator next obj lock.
    } else if (OB_FAIL(iter.set_ready())) {
      TABLELOCK_LOG(WARN, "iterator set ready failed", K(ret));
    } else {
      // do nothing.
    }
  } else if (OB_ISNULL(obj_lock)) {
    ret = OB_ERR_UNEXPECTED;
    TABLELOCK_LOG(WARN, "op list map should not be NULL.", K(lock_id));
  } else if (OB_FAIL(obj_lock->get_lock_op_iter(lock_id,
                                                iter))) {
    TABLELOCK_LOG(WARN, "obj_lock get lock op iter failed", K(ret), K(lock_id));
  } else {
    TABLELOCK_LOG(DEBUG, "succeed get lock op iter.", K(lock_id));
  }
  if (OB_NOT_NULL(obj_lock)) {
    lock_map_.revert(obj_lock);
  }
  return ret;
}

int ObOBJLockMap::check_and_clear_obj_lock(const bool force_compact)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLockIDIterator lock_id_iter;
  ObLockID lock_id;
  ObOBJLock *obj_lock = nullptr;
  bool is_compacted = false;
  if (OB_FAIL(get_lock_id_iter(lock_id_iter))) {
    TABLELOCK_LOG(WARN, "get lock id iterator failed", K(ret));
  } else {
    do {
      if (OB_FAIL(lock_id_iter.get_next(lock_id))) {
        if (OB_ITER_END != ret) {
          TABLELOCK_LOG(WARN, "fail to get next obj lock", K(ret));
        }
      } else if (OB_FAIL(get_obj_lock_with_ref_(lock_id, obj_lock))) {
        if (ret != OB_OBJ_LOCK_NOT_EXIST) {
          TABLELOCK_LOG(WARN, "get obj lock failed", K(ret), K(lock_id));
        } else {
          // Concurrent deletion may occur here. If it is found
          // that the obj lock cannot be gotten, it will continue
          // to iterate the remaining obj lock.
          TABLELOCK_LOG(WARN, "obj lock has been deleted", K(ret), K(lock_id));
          ret = OB_SUCCESS;
          continue;
        }
      } else {
        if (OB_TMP_FAIL(
                obj_lock->compact_tablelock(allocator_, is_compacted, force_compact))) {
          TABLELOCK_LOG(WARN, "compact table lock failed", K(ret), K(tmp_ret),
                        K(lock_id));
        }
        drop_obj_lock_if_empty_(lock_id, obj_lock);
        if (OB_NOT_NULL(obj_lock)) {
          lock_map_.revert(obj_lock);
        }
      }
    } while (OB_SUCC(ret));
  }
  ret = OB_ITER_END == ret ? OB_SUCCESS : ret;
  return ret;
}

bool ObOBJLockMap::GetMinCommittedDDLLogtsFunctor::operator() (
    ObOBJLock *obj_lock)
{
  int ret = common::OB_SUCCESS;
  bool bool_ret = false;

  if (OB_ISNULL(obj_lock)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "map", OB_P(obj_lock));
  } else {
    min_committed_scn_ = std::min(min_committed_scn_,
                                      obj_lock->get_min_ddl_lock_committed_scn(flushed_scn_));
  }

  if (OB_SUCCESS == ret) {
    bool_ret = true;
  }
  return bool_ret;
}

int ObOBJLock::check_op_allow_lock_(const ObTableLockOp &lock_op)
{
  int ret = OB_SUCCESS;
  // deal with lock twice.
  // 0. if there is no lock, return OB_SUCCESS;
  // 1. OUT_TRANS lock:
  // 1) if the lock status is LOCK_OP_DOING, return OB_TRY_LOCK_ROW_CONFLICT
  // 2) if the lock status is LOCK_OP_COMPLETE, return OB_OBJ_LOCK_EXIST if
  // there is no unlock op, else return OB_TRY_LOCK_ROW_CONFLICT.
  // 2. IN_TRANS lock:
  // 1) if the lock status is LOCK_OP_DOING, return OB_OBJ_LOCK_EXIST to prevent
  // lock twice.
  int map_index = 0;
  ObTableLockOpList *op_list = NULL;

  if (OB_UNLIKELY(!lock_op.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument.", K(ret), K(lock_op));
  } else if (FALSE_IT(map_index = get_index_by_lock_mode(
      lock_op.lock_mode_))) {
  } else if (OB_UNLIKELY(map_index < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid lock mode", K(ret), K(lock_op), K(map_index));
  } else if (OB_ISNULL(op_list = map_[map_index])) {
  } else if (OB_FAIL(check_op_allow_lock_from_list_(lock_op,
                                                    op_list))) {
    if (ret != OB_TRY_LOCK_ROW_CONFLICT &&
        ret != OB_OBJ_LOCK_EXIST) {
      LOG_WARN("check allow lock failed.", K(ret), K(lock_op));
    }
  } else {
    LOG_DEBUG("check allow lock finished. ", K(lock_op));
  }
  return ret;
}

int ObOBJLock::check_allow_unlock_(
    const ObTableLockOp &unlock_op)
{
  int ret = OB_SUCCESS;
  // only for OUT_TRANS unlock:
  // 1) if the lock status is LOCK_OP_DOING, return OB_OBJ_LOCK_NOT_COMPLETED
  // for lock op, and return OB_OBJ_UNLOCK_CONFLICT for unlock op
  // 2) if the lock status is LOCK_OP_COMPLETE, return OB_SUCCESS for locl op,
  // but return OB_OBJ_LOCK_NOT_EXIST for unlock op to avoid unlocking repeatedly
  // 3) if there is no lock, return OB_OBJ_LOCK_NOT_EXIST
  int map_index = 0;
  ObTableLockOpList *op_list = NULL;

  if (OB_UNLIKELY(!is_lock_mode_valid(unlock_op.lock_mode_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("lock mode is invalid.", K(ret), K(unlock_op));
  } else if (FALSE_IT(map_index = get_index_by_lock_mode(unlock_op.lock_mode_))) {
  } else if (OB_UNLIKELY(map_index < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid lock mode", K(ret), K(unlock_op), K(map_index));
  } else if (OB_ISNULL(op_list = map_[map_index])) {
    ret = OB_OBJ_LOCK_NOT_EXIST;
    LOG_WARN("the lock want to unlock does not exist", K(ret), K(unlock_op));
  } else if (OB_FAIL(check_op_allow_unlock_from_list_(unlock_op,
                                                      op_list))) {
    if (!is_need_retry_unlock_error(ret)) {
      LOG_WARN("check allow unlock failed.", K(ret), K(unlock_op));
    }
  } else {
    LOG_DEBUG("check allow unlock finished. ", K(unlock_op));
  }
  return ret;
}

int ObOBJLock::check_allow_lock_(
    const ObTableLockOp &lock_op,
    const ObTableLockMode &lock_mode_in_same_trans,
    ObTxIDSet &conflict_tx_set,
    bool &conflict_with_dml_lock,
    const bool include_finish_tx,
    const bool only_check_dml_lock)
{
  int ret = OB_SUCCESS;
  int64_t conflict_modes = 0;
  ObTableLockMode curr_lock_mode = NO_LOCK;
  conflict_tx_set.reset();
  if (lock_op.need_record_lock_op() &&
      OB_FAIL(check_op_allow_lock_(lock_op))) {
    if (ret != OB_TRY_LOCK_ROW_CONFLICT &&
        ret != OB_OBJ_LOCK_EXIST) {
      LOG_WARN("check_op_allow_lock failed.", K(ret), K(lock_op));
    }
  } else if (FALSE_IT(get_exist_lock_mode_without_cur_trans(lock_mode_in_same_trans,
                                                            curr_lock_mode))) {
  } else if (!request_lock(curr_lock_mode,
                           lock_op.lock_mode_,
                           conflict_modes)) {
    // TODO:
    // return OB_ERR_EXCLUSIVE_LOCK_CONFLICT_NOWAIT for ORA-00054 in oracle mode
    ret = OB_TRY_LOCK_ROW_CONFLICT;
  }
  if (OB_TRY_LOCK_ROW_CONFLICT == ret && conflict_modes != 0) {
    // get all the conflict tx id that lock mode conflict with me
    // but not myself
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = get_tx_id_set_(lock_op.create_trans_id_,
                                                conflict_modes,
                                                include_finish_tx,
                                                conflict_tx_set))) {
      LOG_WARN("get conflict tx failed", K(tmp_ret), K(lock_op));
    }
  }
  // for pre check
  if (OB_TRY_LOCK_ROW_CONFLICT == ret && only_check_dml_lock) {
    ret = ((conflict_modes & ROW_SHARE && row_share_) ||
           (conflict_modes & ROW_EXCLUSIVE && row_exclusive_)) ? OB_TRY_LOCK_ROW_CONFLICT : OB_SUCCESS;
  }
  // for no dml lock avoid deadlock with dml lock.
  if (OB_TRY_LOCK_ROW_CONFLICT == ret) {
    conflict_with_dml_lock = ((conflict_modes & ROW_SHARE && row_share_) ||
                              (conflict_modes & ROW_EXCLUSIVE && row_exclusive_));
  }
  LOG_DEBUG("check_allow_lock", K(ret), K(curr_lock_mode), K(lock_mode_in_same_trans),
            K(lock_op.lock_mode_), K(lock_op), K(conflict_modes), K(conflict_tx_set));
  return ret;
}

int ObOBJLock::check_allow_lock(
    const ObTableLockOp &lock_op,
    const ObTableLockMode &lock_mode_in_same_trans,
    ObTxIDSet &conflict_tx_set,
    bool &conflict_with_dml_lock,
    ObMalloc &allocator,
    const bool include_finish_tx,
    const bool only_check_dml_lock)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!lock_op.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument.", K(ret), K(lock_op));
  } else {
    RDLockGuard guard(rwlock_);
    ret = check_allow_lock_(lock_op,
                            lock_mode_in_same_trans,
                            conflict_tx_set,
                            conflict_with_dml_lock,
                            include_finish_tx,
                            only_check_dml_lock);
  }
  return ret;
}

void ObOBJLock::get_exist_lock_mode_without_cur_trans(
    const ObTableLockMode &lock_mode_in_same_trans,
    ObTableLockMode &curr_mode)
{
  curr_mode = 0x0;

  // get RS
  int row_share_nums = (map_[0] == NULL ? 0 : map_[0]->get_size()) + row_share_;
  int row_share = (lock_mode_in_same_trans & ROW_SHARE ?
                  row_share_nums - 1 :
                  row_share_nums);
  curr_mode |= (row_share == 0 ? 0 : ROW_SHARE);

  // get RX, S, SRX
  int row_exclusive_nums = (map_[1] == NULL ? 0 : map_[1]->get_size()) + row_exclusive_;
  int share_nums = (map_[2] == NULL ? 0 : map_[2]->get_size());
  int share_row_exclusive_nums = (map_[3] == NULL ? 0 : map_[3]->get_size());
  if ((lock_mode_in_same_trans & ROW_EXCLUSIVE) &&
       (lock_mode_in_same_trans & SHARE)) {
    // other trans should not have RX, S or SRX
    if (row_exclusive_nums > 1 ||
        share_nums > 1 ||
        share_row_exclusive_nums > 1) {
      LOG_ERROR_RET(OB_ERR_UNEXPECTED, "unexpected error",
                K(row_exclusive_nums), K(share_nums), K(share_row_exclusive_nums));
    }
  } else if (lock_mode_in_same_trans & ROW_EXCLUSIVE) {
    // other trans in the obj should not have S or SRX
    if (share_nums > 1 ||
        share_row_exclusive_nums > 1) {
      LOG_ERROR_RET(OB_ERR_UNEXPECTED, "unexpected error",
                K(row_exclusive_nums), K(share_nums), K(share_row_exclusive_nums));
    }
    curr_mode |= (row_exclusive_nums > 1 ? ROW_EXCLUSIVE : 0);
  } else if (lock_mode_in_same_trans & SHARE) {
    // other trans in the obj should not have RX or SRX
    if (row_exclusive_nums > 1 ||
        share_row_exclusive_nums > 1) {
      LOG_ERROR_RET(OB_ERR_UNEXPECTED, "unexpected error",
                K(row_exclusive_nums), K(share_nums), K(share_row_exclusive_nums));
    }
    curr_mode |= (share_nums > 1 ? SHARE : 0);
  } else {
    curr_mode |= (row_exclusive_nums > 0 ? ROW_EXCLUSIVE : 0);
    curr_mode |= (share_nums > 0 ? SHARE : 0);
    curr_mode |= (share_row_exclusive_nums > 0 ? SHARE_ROW_EXCLUSIVE : 0);
  }

  // get X
  int exclusive_nums = (map_[4] == NULL ? 0 : map_[4]->get_size());
  if (lock_mode_in_same_trans & EXCLUSIVE) {
    // other trans should not have RS, RX, S, SRX, X
    if (row_share_nums > 1 ||
        row_exclusive_nums > 1 ||
        share_nums > 1 ||
        share_row_exclusive_nums > 1 ||
        exclusive_nums > 1) {
      LOG_ERROR_RET(OB_ERR_UNEXPECTED, "unexpected error", K(row_share_nums), K(row_exclusive_nums),
                K(share_nums), K(share_row_exclusive_nums), K(exclusive_nums));
    }
  } else {
    curr_mode |= (exclusive_nums > 0 ? EXCLUSIVE : 0);
  }
}

void ObOBJLock::reset_(ObMalloc &allocator)
{
  ObTableLockOpList *op_list = NULL;
  for (int i = 0; i < TABLE_LOCK_MODE_COUNT; i++) {
    op_list = map_[i];
    if (NULL != op_list) {
      free_op_list(op_list, allocator);
      op_list->~ObTableLockOpList();
      allocator.free(op_list);
    }
  }
  row_share_ = 0;
  row_exclusive_ = 0;
  memset(map_, 0, sizeof(ObTableLockOpList *) * TABLE_LOCK_MODE_COUNT);
}
void ObOBJLock::reset(ObMalloc &allocator)
{
  WRLockGuard guard(rwlock_);
  reset_(allocator);
}

void ObOBJLock::reset_without_lock(ObMalloc &allocator)
{
  reset_(allocator);
}

void ObOBJLock::print() const
{
  RDLockGuard guard(rwlock_);
  print_();
}

void ObOBJLock::print_without_lock() const
{
  print_();
}

void ObOBJLock::print_() const
{
  ObTableLockOpList *op_list = NULL;
  LOG_INFO("ObOBJLock: ");
  for (int i = 0; i < TABLE_LOCK_MODE_COUNT; i++) {
    op_list = map_[i];
    if (NULL != op_list) {
      LOG_INFO("ObOBJLock: mode:", K(LOCK_MODE_ARRAY[i]));
      print_op_list(op_list);
    }
  }
  LOG_INFO("ObOBJLock: ", K_(row_share), K_(row_exclusive));
}

int ObOBJLock::get_lock_op_iter(const ObLockID &lock_id,
                                ObLockOpIterator &iter) const
{
  int ret = OB_SUCCESS;
  RDLockGuard guard(rwlock_);
  ObTableLockOpList *op_list = NULL;
  for (int i = 0; OB_SUCC(ret) && i < TABLE_LOCK_MODE_COUNT; i++) {
    op_list = map_[i];
    if (NULL != op_list) {
      if (OB_FAIL(get_lock_op_list_iter_(op_list,
                                         iter))) {
        TABLELOCK_LOG(WARN, "get lock op list iter failed", K(ret), K(i));
      }
    }
  }
  // add a mock lock op for row share lock count.
  if (OB_SUCC(ret) && 0 != row_share_) {
    ObTableLockOp tmp_op;
    tmp_op.lock_id_ = lock_id;
    tmp_op.lock_mode_ = ROW_SHARE;
    tmp_op.op_type_ = IN_TRANS_DML_LOCK;
    tmp_op.lock_op_status_ = LOCK_OP_DOING;
    // we use this one for the count.
    tmp_op.lock_seq_no_ = ObTxSEQ(row_share_, 0);
    if (OB_FAIL(iter.push(tmp_op))) {
      TABLELOCK_LOG(WARN, "push tmp lock op into iterator failed", K(ret), K(tmp_op));
    }
  }
  // add a mock lock op for row exclusive lock count.
  if (OB_SUCC(ret) && 0 != row_exclusive_) {
    ObTableLockOp tmp_op;
    tmp_op.lock_id_ = lock_id;
    tmp_op.lock_mode_ = ROW_EXCLUSIVE;
    tmp_op.op_type_ = IN_TRANS_DML_LOCK;
    tmp_op.lock_op_status_ = LOCK_OP_DOING;
    // we use this one for the count.
    tmp_op.lock_seq_no_ = ObTxSEQ(row_exclusive_, 0);
    if (OB_FAIL(iter.push(tmp_op))) {
      TABLELOCK_LOG(WARN, "push tmp lock op into iterator failed", K(ret), K(tmp_op));
    }
  }
  if (OB_SUCC(ret) &&
      OB_FAIL(iter.set_ready())) {
    TABLELOCK_LOG(WARN, "iterator set ready failed", K(ret));
  }
  return ret;
}

int ObOBJLock::size_without_lock() const
{
  int map_size = 0;
  for (int i = 0; i < TABLE_LOCK_MODE_COUNT; i++) {
    if (NULL != map_[i]) {
      ++map_size;
    }
  }
  if (row_share_ != 0) {
    ++map_size;
  }
  if (row_exclusive_ != 0) {
    ++map_size;
  }
  return map_size;
}

void ObOBJLock::check_need_recover_(
    const ObTableLockOp &lock_op,
    const ObTableLockOpList *op_list,
    bool &need_recover)
{
  need_recover = true;
  DLIST_FOREACH_NORET(curr, *op_list) {
    if (!(need_recover = curr->lock_op_.need_replay_or_recover(lock_op))) {
      break;
    }
  }
}

int ObOBJLock::check_op_allow_unlock_from_list_(
    const ObTableLockOp &lock_op,
    const ObTableLockOpList *op_list)
{
  int ret = OB_SUCCESS;
  bool lock_exist = false;
  if (OUT_TRANS_UNLOCK != lock_op.op_type_) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    DLIST_FOREACH(curr, *op_list) {
      if (curr->lock_op_.owner_id_ == lock_op.owner_id_) {
        lock_exist = true;
        if (curr->lock_op_.lock_op_status_ == LOCK_OP_DOING) {
          if (curr->lock_op_.op_type_ == OUT_TRANS_LOCK) {
            ret = OB_OBJ_LOCK_NOT_COMPLETED;
          } else if (curr->lock_op_.op_type_ == OUT_TRANS_UNLOCK) {
            ret = OB_OBJ_UNLOCK_CONFLICT;
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("unexpected lock op type", K(ret), K(curr->lock_op_));
          }
        } else if (curr->lock_op_.lock_op_status_ == LOCK_OP_COMPLETE) {
          // This status will occur in transaction disorder replaying,
          // i.e. there's an unlcok op replay and commit before the
          // lock op. So we return this error code to avoid continuing
          // the unlocking operation.
          if (curr->lock_op_.op_type_ == OUT_TRANS_UNLOCK) {
            ret = OB_OBJ_LOCK_NOT_EXIST;
          } else if (curr->lock_op_.op_type_ == OUT_TRANS_LOCK) {
            // do nothing
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("unexpected lock op type", K(ret), K(curr->lock_op_));
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("unexpected lock op status", K(ret), K(curr->lock_op_));
        }
      }
    } // DLIST_FOREACH
  }
  if (OB_SUCC(ret) && !lock_exist) {
    ret = OB_OBJ_LOCK_NOT_EXIST;
  }
  return ret;
}

int ObOBJLock::check_op_allow_lock_from_list_(
    const ObTableLockOp &lock_op,
    const ObTableLockOpList *op_list)
{
  int ret = OB_SUCCESS;
  bool has_unlock_op = false;
  bool need_break = false;
  DLIST_FOREACH_NORET(curr, *op_list) {
    switch (lock_op.op_type_) {
    case IN_TRANS_DML_LOCK:
    case IN_TRANS_COMMON_LOCK: {
      if (curr->lock_op_.create_trans_id_ == lock_op.create_trans_id_ &&
          (curr->lock_op_.op_type_ == IN_TRANS_DML_LOCK ||
           curr->lock_op_.op_type_ == IN_TRANS_COMMON_LOCK)) {
        if (curr->lock_op_.lock_id_.obj_type_ == ObLockOBJType::OBJ_TYPE_DBMS_LOCK) {
          ret = OB_OBJ_LOCK_EXIST;
        } else if (curr->lock_op_.lock_op_status_ != LOCK_OP_DOING) {
          // should never be here.
          ret = OB_ERR_UNEXPECTED;
          need_break = true;
          LOG_ERROR("unexpected lock op status.", K(ret), K(curr->lock_op_));
        }
      }
      break;
    }
    case OUT_TRANS_LOCK: {
      if (curr->lock_op_.owner_id_ == lock_op.owner_id_) {
        if (curr->lock_op_.op_type_ == OUT_TRANS_LOCK) {
          if (curr->lock_op_.lock_op_status_ == LOCK_OP_DOING) {
            // out trans lock conflict with itself.
            // can not lock with the same lock mode twice.
            ret = OB_TRY_LOCK_ROW_CONFLICT;
            need_break = true;
          } else if (curr->lock_op_.lock_op_status_ == LOCK_OP_COMPLETE) {
            // need continue to check unlock op
            ret = OB_OBJ_LOCK_EXIST;
          } else {
            ret = OB_ERR_UNEXPECTED;
            need_break = true;
            LOG_ERROR("unexpected lock op status.", K(ret), K(curr->lock_op_));
          }
        } else if (curr->lock_op_.op_type_ == OUT_TRANS_UNLOCK) {
          // you are unlocking, cannot lock again now.
          ret = OB_TRY_LOCK_ROW_CONFLICT;
          has_unlock_op = true;
          need_break = true;
        } else if (curr->lock_op_.op_type_ == IN_TRANS_COMMON_LOCK &&
                   curr->lock_op_.create_trans_id_ == lock_op.create_trans_id_) {
          // continue
        } else {
          ret = OB_ERR_UNEXPECTED;
          need_break = true;
          LOG_ERROR("unknown lock op type.", K(ret), K(curr->lock_op_));
        }
      }
      break;
    }
    case OUT_TRANS_UNLOCK:
    default: {
      ret = OB_ERR_UNEXPECTED;
      need_break = true;
      LOG_ERROR("unexpected lock op type.", K(ret), K(lock_op));
      break;
    } // default
    } // switch
    if (need_break) {
      break;
    }
  }
  return ret;
}

void ObOBJLock::delete_lock_op_from_list_(
    const ObTableLockOp &lock_op,
    ObTableLockOpList *&op_list,
    ObMalloc &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!lock_op.is_valid()) ||
      OB_UNLIKELY(OB_ISNULL(op_list))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(lock_op), K(op_list));
  } else {
    bool need_delete = false;
    DLIST_FOREACH_REMOVESAFE_NORET(curr, *op_list) {
      need_delete = false;
      if (curr->lock_op_.create_trans_id_ == lock_op.create_trans_id_ &&
          curr->lock_op_.owner_id_ == lock_op.owner_id_ &&
          curr->lock_op_.op_type_ == lock_op.op_type_) {
        need_delete = true;
      }

      if (need_delete) {
        (void)op_list->remove(curr);
        curr->~ObTableLockOpLinkNode();
        allocator.free(curr);
      }
    }
  }
}

void ObOBJLock::free_op_list(ObTableLockOpList *op_list, ObMalloc &allocator)
{
  DLIST_FOREACH_REMOVESAFE_NORET(curr, *op_list) {
    if (NULL != curr) {
      curr->~ObTableLockOpLinkNode();
      allocator.free(curr);
    }
  }
}

void ObOBJLock::print_op_list(const ObTableLockOpList *op_list) const
{
  DLIST_FOREACH_NORET(curr, *op_list) {
    if (NULL != curr) {
      LOG_INFO("ObTableLockOp: ", K(curr->lock_op_));
    }
  }
}

int ObOBJLock::get_lock_op_list_iter_(const ObTableLockOpList *op_list,
                                      ObLockOpIterator &iter) const
{
  int ret = OB_SUCCESS;
  DLIST_FOREACH_X(curr, *op_list, OB_SUCC(ret)) {
    if (NULL != curr &&
        OB_FAIL(iter.push(curr->lock_op_))) {
      TABLELOCK_LOG(WARN, "push lock op into iterator failed", K(ret), K(curr->lock_op_));
    }
  }
  return ret;
}

// get all the conflict tx id.
// NOTE: we can not get the DML table lock tx id.
int ObOBJLock::get_tx_id_set_(const ObTransID &myself_tx,
                              const int64_t lock_modes,
                              const bool include_finish_tx,
                              ObTxIDSet &tx_id_set)
{
  int ret = OB_SUCCESS;
  ObTableLockOpList *op_list = NULL;
  ObTableLockMode curr_mode = 0x0;
  bool need_check_curr_list = false;
  for (int i = 0; i < TABLE_LOCK_MODE_COUNT && OB_SUCC(ret); i++) {
    op_list = map_[i];
    curr_mode = LOCK_MODE_ARRAY[i];
    need_check_curr_list = ((curr_mode & lock_modes) == curr_mode);
    if (NULL != op_list && need_check_curr_list) {
      if (OB_FAIL(get_tx_id_set_(myself_tx,
                                 op_list,
                                 include_finish_tx,
                                 tx_id_set))) {
        LOG_WARN("get tx id from op list failed", K(ret), K(curr_mode));
      }
    }
  }
  return ret;
}

int ObOBJLock::get_tx_id_set_(const ObTransID &myself_tx,
                              const ObTableLockOpList *op_list,
                              const bool include_finish_tx,
                              ObTxIDSet &tx_id_set)
{
  int ret = OB_SUCCESS;
  DLIST_FOREACH_X(curr, *op_list, OB_SUCC(ret)) {
    if (NULL != curr &&
        (include_finish_tx || (!include_finish_tx && curr->lock_op_.lock_op_status_ == LOCK_OP_DOING)) &&
        (myself_tx != curr->lock_op_.create_trans_id_) &&
        OB_FAIL(tx_id_set.set_refactored(curr->lock_op_.create_trans_id_))) {
      // the trans id may be exist now.
      if (OB_HASH_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("push tx id failed", K(ret), K(curr->lock_op_));
      }
    }
  }
  return ret;
}

// find the first complete unlock op if it exists.
// else return OB_OBJ_LOCK_NOT_EXIST
int ObOBJLock::get_first_complete_unlock_op_(const ObTableLockOpList *op_list,
                                             ObTableLockOp &unlock_op) const
{
  int ret = OB_SUCCESS;
  bool is_find = false;
  if (OB_ISNULL(op_list)) {
    // there is no complete unlock op
    is_find = false;
  } else {
    DLIST_FOREACH_NORET(curr, *op_list) {
      if (curr->is_complete_outtrans_unlock()) {
        is_find = true;
        unlock_op = curr->lock_op_;
        break;
      }
    }
  }
  if (!is_find) {
    ret = OB_OBJ_LOCK_NOT_EXIST;
  }
  return ret;
}

// compact one lock op
int ObOBJLock::compact_tablelock_(const ObTableLockOp &unlock_op,
                                  ObTableLockOpList *&op_list,
                                  ObMalloc &allocator,
                                  bool &is_compact,
                                  const bool is_force)
{
  int ret = OB_SUCCESS;
  ObTableLockOpLinkNode *lock_op_ptr = nullptr;
  ObTableLockOpLinkNode *unlock_op_ptr = nullptr;
  if (OB_UNLIKELY(!unlock_op.is_valid()) ||
      OB_UNLIKELY(OB_ISNULL(op_list))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(unlock_op), K(op_list));
  } else {
    is_compact = false;
    // find the lock op and unlock op for compact
    DLIST_FOREACH_REMOVESAFE_NORET(curr, *op_list) {
      if (curr->lock_op_.owner_id_ == unlock_op.owner_id_ &&
          curr->lock_op_.lock_mode_ == unlock_op.lock_mode_) {
        if (curr->is_complete_outtrans_lock()) {
          lock_op_ptr = curr;
        } else if (curr->is_complete_outtrans_unlock()) {
          unlock_op_ptr = curr;
        }
        if (OB_NOT_NULL(lock_op_ptr) && OB_NOT_NULL(unlock_op_ptr)) {
          // iter finish, both the lock and unlock find.
          break;
        }
      }
    }
    // remove lock op
    if (OB_NOT_NULL(lock_op_ptr)) {
      is_compact = true;
      (void)op_list->remove(lock_op_ptr);
      lock_op_ptr->~ObTableLockOpLinkNode();
      allocator.free(lock_op_ptr);
    }
    if ((is_compact || is_force) &&
        OB_NOT_NULL(unlock_op_ptr)) {
      if (is_force && !is_compact) {
        LOG_WARN("an unlock op force compact", K(unlock_op_ptr));
      }
      is_compact = true;
      (void)op_list->remove(unlock_op_ptr);
      unlock_op_ptr->~ObTableLockOpLinkNode();
      allocator.free(unlock_op_ptr);
    }
  }
  LOG_DEBUG("compact finish", K(ret), KP(lock_op_ptr), KP(unlock_op_ptr), K(unlock_op));
  return ret;
}

// compact one lock op list
int ObOBJLock::compact_tablelock_(ObTableLockOpList *&op_list,
                                  ObMalloc &allocator,
                                  bool &is_compact,
                                  const bool is_force)
{
  int ret = OB_SUCCESS;
  is_compact = false;
  if (OB_ISNULL(op_list)) {
    // there is no lock op at the list, do nothing
  } else {
    ObTableLockOp unlock_op;
    bool tmp_is_compact = true;
    while (OB_SUCC(ret) &&
           tmp_is_compact &&
           OB_SUCC(get_first_complete_unlock_op_(op_list,
                                                 unlock_op))) {
      if (OB_FAIL(compact_tablelock_(unlock_op,
                                     op_list,
                                     allocator,
                                     tmp_is_compact,
                                     is_force))) {
        LOG_WARN("compact tablelock failed", K(ret), K(unlock_op), KP(op_list));
      } else if (tmp_is_compact) {
        is_compact = tmp_is_compact;
      } else {
        // do nothing
      }
    }
    // maybe we can not get any complete unlock_op,
    // so we should judge whether it's valid.
    if (unlock_op.is_valid()) {
      drop_op_list_if_empty_(unlock_op.lock_mode_, op_list, allocator);
    }
    if (OB_OBJ_LOCK_NOT_EXIST == ret) {
      // compact finished succeed
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

// compact one obj lock
int ObOBJLock::compact_tablelock_(ObMalloc &allocator,
                                  bool &is_compact,
                                  const bool is_force)
{
  int ret = OB_SUCCESS;
  ObTableLockOpList *op_list = NULL;
  bool tmp_is_compact = true;

  is_compact = false;
  for (int i = 0; OB_SUCC(ret) && i < TABLE_LOCK_MODE_COUNT; i++) {
    op_list = map_[i];
    if (OB_FAIL(compact_tablelock_(op_list, allocator, tmp_is_compact, is_force))) {
      LOG_WARN("compact table lock failed", K(ret), KP(op_list));
    } else if (tmp_is_compact) {
      is_compact = tmp_is_compact;
    } else {
      // do nothing
    }
  }
  return ret;
}

int ObOBJLock::get_or_create_op_list(const ObTableLockMode mode,
                                     const uint64_t tenant_id,
                                     ObMalloc &allocator,
                                     ObTableLockOpList *&op_list)
{
  int ret = OB_SUCCESS;
  void *ptr = NULL;
  int map_index = 0;
  op_list = NULL;
  ObMemAttr attr(tenant_id, "ObTableLockOpL");
  if (OB_UNLIKELY(!is_lock_mode_valid(mode))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("lock mode is invalid.", K(ret), K(mode));
  } else if (FALSE_IT(map_index = get_index_by_lock_mode(mode))) {
  } else if (OB_UNLIKELY(map_index < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid lock mode", K(ret), K(mode), K(map_index));
  } else if (OB_ISNULL(op_list = map_[map_index])) {
    if (OB_ISNULL(ptr = allocator.alloc(sizeof(ObTableLockOpList), attr))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alllocate ObTableLockOpList ", K(ret));
    } else if (FALSE_IT(op_list = new(ptr) ObTableLockOpList())) {
    } else {
      map_[map_index] = op_list;
    }
  }
  return ret;
}

int ObOBJLock::get_op_list(const ObTableLockMode mode,
                           ObTableLockOpList *&op_list)
{
  int ret = OB_SUCCESS;
  int map_index = 0;
  op_list = NULL;
  if (OB_UNLIKELY(!is_lock_mode_valid(mode))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("lock mode is invalid.", K(ret), K(mode));
  } else if (FALSE_IT(map_index = get_index_by_lock_mode(mode))) {
  } else if (OB_UNLIKELY(map_index < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid lock mode", K(ret), K(mode), K(map_index));
  } else {
    op_list = map_[map_index];
  }
  return ret;
}

void ObOBJLock::drop_op_list_if_empty_(
    const ObTableLockMode mode,
    ObTableLockOpList *&op_list,
    ObMalloc &allocator)
{
  int ret = OB_SUCCESS;
  int map_index = 0;
  if (OB_ISNULL(op_list) || OB_UNLIKELY(!is_lock_mode_valid(mode))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(op_list), K(mode));
  } else if (FALSE_IT(map_index = get_index_by_lock_mode(mode))) {
  } else if (OB_UNLIKELY(map_index < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid lock mode", K(ret), K(mode), K(map_index));
  } else if (op_list->get_size() == 0) {
    op_list->~ObTableLockOpList();
    allocator.free(op_list);
    map_[map_index] = NULL;
    // We have to set op_list to NULL to avoid
    // visit the op_list by the pointer again
    op_list = NULL;
  }
}

ObOBJLock *ObOBJLockFactory::alloc(const uint64_t tenant_id, const ObLockID &lock_id)
{
  int ret = OB_SUCCESS;
  void *ptr = NULL;
  ObOBJLock* obj_lock = NULL;
  ObMemAttr attr(tenant_id, OB_TABLE_LOCK_NODE);
  if (!is_valid_tenant_id(tenant_id) || !lock_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(ret), K(tenant_id), K(lock_id));
  } else if (NULL != (ptr = ob_malloc(sizeof(ObOBJLock), attr))) {
    obj_lock = new(ptr) ObOBJLock(lock_id);
    (void)ATOMIC_FAA(&alloc_count_, 1);
  }
  LOG_DEBUG( "alloc allock_count", K(alloc_count_), K(ptr));
  return obj_lock;
}

void ObOBJLockFactory::release(ObOBJLock *obj_lock)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(obj_lock)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("ObOBJLock pointer is null when released",
              K(ret), KP(obj_lock));
  } else {
    bool is_empty = false;
    is_empty = (obj_lock->size_without_lock() == 0);
    if (!is_empty) {
      LOG_ERROR("obj lock release but there some lock not released", KP(obj_lock));
      obj_lock->print_without_lock();
    }
    obj_lock->~ObOBJLock();
    ob_free(obj_lock);
    obj_lock = NULL;
    (void)ATOMIC_FAA(&release_count_, 1);
  }
  LOG_DEBUG( "release release_count", K(release_count_), K(obj_lock));
}

int ObOBJLockMap::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObOBJLockMap has been inited already", K(ret));
  } else if (OB_FAIL(lock_map_.init(lib::ObMemAttr(MTL_ID(), "ObOBJLockMap")))) {
    LOG_WARN("ObOBJLockMap create lock map failed", K(ret));
  } else {
    is_inited_ = true;
  }
  if (OB_FAIL(ret)) {
    lock_map_.destroy();
  }
  return ret;
}

void ObOBJLockMap::reset()
{
  if (IS_INIT) {
    ResetLockFunctor fn(allocator_);
    (void)lock_map_.for_each(fn);
    lock_map_.destroy();
    is_inited_ = false;
  }
}

void ObOBJLockMap::print()
{
  LOG_INFO("ObOBJLockMap locks: ");
  PrintLockFunctor fn;
  lock_map_.for_each(fn);
}

SCN ObOBJLockMap::get_min_ddl_committed_scn(SCN &flushed_scn)
{
  int ret = OB_SUCCESS;
  SCN min_ddl_committed_scn = SCN::max_scn();
  GetMinCommittedDDLLogtsFunctor fn(flushed_scn);
  if (OB_FAIL(lock_map_.for_each(fn))) {
    LOG_WARN("for each link_hash_map_ get_min_ddl_committed_scn error",
                                            KR(ret), K(flushed_scn));
  } else {
    min_ddl_committed_scn = fn.get_min_committed_scn();
  }

  return min_ddl_committed_scn;
}

int ObOBJLockMap::get_or_create_obj_lock_with_ref_(
    const ObLockID &lock_id,
    ObOBJLock *&obj_lock)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = MTL_ID();
  obj_lock = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObOBJLockMap is not inited", K(ret));
  } else {
    do {
      if (OB_FAIL(lock_map_.get(lock_id, obj_lock))) {
        if (ret == OB_ENTRY_NOT_EXIST) {
          if (OB_ISNULL(obj_lock = ObOBJLockFactory::alloc(tenant_id, lock_id))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to alllocate ObOBJLock ", K(ret));
          } else if (OB_FAIL(lock_map_.insert_and_get(obj_lock->get_lock_id(),
                                                      obj_lock,
                                                      NULL))) {
            ObOBJLockFactory::release(obj_lock);
            obj_lock = nullptr;
            if (ret != OB_ENTRY_EXIST) {
              LOG_WARN("failed to add ObOBJLock to obj_lock_map_ ",
                       K(ret), K(lock_id));
            }
          } else {
            LOG_DEBUG("succeed add ObOBJLock to obj_lock_map_ ",
                     K(lock_id), K(lock_id), K(obj_lock));
          }
        } else {
          LOG_WARN("failed to get lock from partition lock map ", K(ret),
                   K(lock_id));
        }
      }
    } while (ret == OB_ENTRY_EXIST);
  }
  return ret;
}

int ObOBJLockMap::get_obj_lock_with_ref_(
    const ObLockID &lock_id,
    ObOBJLock *&obj_lock)
{
  int ret = OB_SUCCESS;
  obj_lock = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObOBJLockMap is not inited", K(ret));
  } else if (OB_FAIL(lock_map_.get(lock_id, obj_lock))) {
    if (ret != OB_ENTRY_NOT_EXIST) {
      LOG_WARN("get lock map failed.", K(ret), K(lock_id));
    } else {
      ret = OB_OBJ_LOCK_NOT_EXIST;
    }
  }
  return ret;
}

int ObOBJLockMap::lock(
    const ObLockParam &param,
    ObStoreCtx &ctx,
    const ObTableLockOp &lock_op,
    const ObTableLockMode &lock_mode_in_same_trans,
    ObTxIDSet &conflict_tx_set)
{
  int ret = OB_SUCCESS;
  ObOBJLock *obj_lock = NULL;
  LOG_DEBUG("ObOBJLockMap::lock ", K(param), K(lock_op));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObOBJLockMap is not inited", K(ret));
  } else if (OB_UNLIKELY(!lock_op.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument.", K(ret), K(lock_op));
  } else {
    do {
      obj_lock = NULL;
      if (OB_FAIL(get_or_create_obj_lock_with_ref_(lock_op.lock_id_,
                                                   obj_lock))) {
        LOG_WARN("get or create owner map failed.", K(ret), K(lock_op));
      } else if (OB_FAIL(obj_lock->lock(param,
                                        ctx,
                                        lock_op,
                                        lock_mode_in_same_trans,
                                        allocator_,
                                        conflict_tx_set))) {
        if (ret != OB_EAGAIN &&
            ret != OB_TRY_LOCK_ROW_CONFLICT &&
            ret != OB_OBJ_LOCK_EXIST) {
          LOG_WARN("create lock failed.", K(ret), K(lock_op));
        }
      } else {
        LOG_DEBUG("succeed create lock ", K(lock_op));
      }
      if (OB_NOT_NULL(obj_lock)) {
        lock_map_.revert(obj_lock);
      }
      if (OB_FAIL(ret) && REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
        LOG_WARN("ObOBJLockMap::lock ", K(ret), K(param), K(lock_op));
      }
      // retry if the table lock list map is delete right now by others.
    } while (ret == OB_EAGAIN);
  }
  LOG_DEBUG("ObOBJLockMap::lock finish.", K(ret));

  return ret;
}

int ObOBJLockMap::unlock(
    const ObTableLockOp &lock_op,
    const bool is_try_lock,
    const int64_t expired_time)
{
  int ret = OB_SUCCESS;
  ObOBJLock *obj_lock = NULL;
  LOG_DEBUG("ObOBJLockMap::unlock ", K(is_try_lock), K(expired_time), K(lock_op));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObOBJLockMap is not inited", K(ret));
  } else if (OB_UNLIKELY(!lock_op.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument.", K(ret), K(lock_op));
  } else {
    do {
      obj_lock = NULL;
      if (OB_FAIL(get_obj_lock_with_ref_(lock_op.lock_id_,
                                         obj_lock))) {
        LOG_WARN("get lock op list map failed.", K(ret), K(lock_op));
      } else if (OB_FAIL(obj_lock->unlock(lock_op,
                                          is_try_lock,
                                          expired_time,
                                          allocator_))) {
        if (ret != OB_EAGAIN) {
          LOG_WARN("create unlock op failed.", K(ret), K(lock_op));
        }
      } else {
        LOG_DEBUG("succeed create unlock op ", K(lock_op));
      }
      if (OB_NOT_NULL(obj_lock)) {
        lock_map_.revert(obj_lock);
      }
      if (OB_FAIL(ret) && REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
        LOG_WARN("ObOBJLockMap::unlock ", K(ret), K(is_try_lock),
                 K(expired_time), K(lock_op));
      }
    } while (ret == OB_EAGAIN);
  }
  LOG_DEBUG("ObOBJLockMap::unlock finish.", K(ret));

  return ret;
}

void ObOBJLockMap::remove_lock_record(const ObTableLockOp &lock_op)
{
  int ret = OB_SUCCESS;
  ObOBJLock *obj_lock = NULL;
  ObTableLockOpList *op_list = NULL;
  LOG_DEBUG("ObOBJLockMap::remove_lock_record ", K(lock_op));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObOBJLockMap is not inited", K(ret));
  } else if (OB_UNLIKELY(!lock_op.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument.", K(ret), K(lock_op));
  } else if (OB_FAIL(lock_map_.get(lock_op.lock_id_, obj_lock))) {
    if (ret == OB_ENTRY_NOT_EXIST) {
      ret = OB_SUCCESS;
    } else {
      // should only have not exist err code.
      LOG_ERROR("get lock op list map failed. ", K(ret),
                K(lock_op.lock_id_));
    }
  } else {
    obj_lock->remove_lock_op(lock_op, allocator_);
    lock_map_.revert(obj_lock);
  }
  LOG_DEBUG("ObOBJLockMap::remove_lock_record finish.", K(ret));
}

int ObOBJLockMap::remove_lock(const ObLockID &lock_id)
{
  int ret = OB_SUCCESS;
  ObOBJLock *obj_lock = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObOBJLockMap is not inited", K(ret));
  } else if (!lock_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(lock_id));
  } else if (OB_FAIL(get_obj_lock_with_ref_(lock_id, obj_lock))) {
    if (ret != OB_OBJ_LOCK_NOT_EXIST) {
      LOG_WARN("get lock map failed.", K(ret), K(lock_id));
    }
  } else {
    WRLockGuard guard(obj_lock->rwlock_);
    obj_lock->set_deleted();
    obj_lock->reset_without_lock(allocator_);
    if (OB_FAIL(lock_map_.del(lock_id, obj_lock))) {
      if (ret != OB_ENTRY_NOT_EXIST) {
        LOG_WARN("remove lock owner list map failed. ", K(ret), K(lock_id));
      } else {
        ret = OB_OBJ_LOCK_NOT_EXIST;
      }
    }
  }
  if (OB_NOT_NULL(obj_lock)) {
    lock_map_.revert(obj_lock);
  }
  if (ret == OB_OBJ_LOCK_NOT_EXIST) {
    ret = OB_SUCCESS;
  }
  LOG_DEBUG("remove lock", K(ret), K(lock_id));
  return ret;
}

int ObOBJLockMap::recover_obj_lock(const ObTableLockOp &lock_op)
{
  int ret = OB_SUCCESS;
  ObOBJLock *obj_lock = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObOBJLockMap is not inited", K(ret), K(lock_op));
  } else if (OB_UNLIKELY(!lock_op.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument.", K(ret), K(lock_op));
  } else {
    do {
      obj_lock = NULL;
      if (OB_FAIL(get_or_create_obj_lock_with_ref_(lock_op.lock_id_,
                                                   obj_lock))) {
        LOG_WARN("get or create owner map failed.", K(ret), K(lock_op));
      } else if (OB_FAIL(obj_lock->recover_lock(lock_op, allocator_))) {
        if (ret != OB_EAGAIN) {
          LOG_WARN("create lock failed.", K(ret), K(lock_op));
        }
      } else {
        LOG_DEBUG("succeed create lock ", K(lock_op));
      }
      if (OB_NOT_NULL(obj_lock)) {
        lock_map_.revert(obj_lock);
      }
      if (OB_FAIL(ret) && REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
        LOG_WARN("ObOBJLockMap::lock ", K(ret), K(lock_op));
      }
    } while (ret == OB_EAGAIN);
  }
  return ret;
}

int ObOBJLockMap::update_lock_status(const ObTableLockOp &lock_op,
                                     const SCN commit_version,
                                     const SCN commit_scn,
                                     const ObTableLockOpStatus status)
{
  int ret = OB_SUCCESS;
  ObOBJLock *obj_lock = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObOBJLockMap is not inited", K(ret), K(lock_op));
  } else if (OB_UNLIKELY(!lock_op.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument.", K(ret), K(lock_op));
  } else {
    obj_lock = NULL;
    if (OB_FAIL(get_obj_lock_with_ref_(lock_op.lock_id_, obj_lock))) {
      LOG_WARN("the lock dose not exist, failed to update status.", K(ret), K(lock_op));
    } else if (OB_ISNULL(obj_lock)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("op list map should not be NULL.", K(lock_op));
    } else if (OB_FAIL(obj_lock->update_lock_status(lock_op, commit_version, commit_scn, status, allocator_))) {
      LOG_WARN("update lock status failed.", K(ret), K(lock_op));
    } else {
      LOG_DEBUG("succeed update lock status.", K(lock_op), K(status));
    }
    if (OB_NOT_NULL(obj_lock)) {
      lock_map_.revert(obj_lock);
    }
  }
  return ret;
}

int ObOBJLockMap::check_allow_lock(
    const ObTableLockOp &lock_op,
    const ObTableLockMode &lock_mode_in_same_trans,
    ObTxIDSet &conflict_tx_set,
    const bool include_finish_tx,
    const bool only_check_dml_lock)
{
  int ret = OB_SUCCESS;
  ObOBJLock *obj_lock = NULL;
  bool conflict_with_dml_lock = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObOBJLockMap is not inited", K(ret));
  } else if (OB_UNLIKELY(!lock_op.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument.", K(ret), K(lock_op));
  } else if (OB_FAIL(get_obj_lock_with_ref_(lock_op.lock_id_, obj_lock))) {
    if (ret != OB_OBJ_LOCK_NOT_EXIST) {
      LOG_WARN("get owner map failed.", K(ret), K(lock_op));
    } else {
      ret = OB_SUCCESS;
      // the whole lock dose not exist, allow lock
      LOG_DEBUG("the lock dose not exist, allow lock.", K(lock_op));
    }
  } else if (OB_ISNULL(obj_lock)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("op list map should not be NULL.", K(lock_op));
  } else if (OB_FAIL(obj_lock->check_allow_lock(lock_op,
                                                lock_mode_in_same_trans,
                                                conflict_tx_set,
                                                conflict_with_dml_lock,
                                                allocator_,
                                                include_finish_tx,
                                                only_check_dml_lock))) {
    if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
      LOG_WARN("obj_lock check_allow_lock failed",
               K(ret), K(lock_op), K(lock_mode_in_same_trans));
    }
  } else {
    LOG_DEBUG("succeed check allow lock.", K(lock_op));
  }
  if (OB_NOT_NULL(obj_lock)) {
    lock_map_.revert(obj_lock);
  }
  return ret;
}

void ObOBJLockMap::drop_obj_lock_if_empty_(
    const ObLockID &lock_id,
    ObOBJLock *obj_lock)
{
  int ret = OB_SUCCESS;
  bool is_empty = false;
  ObOBJLock *recheck_ptr = nullptr;
  if (OB_ISNULL(obj_lock) ||
      OB_UNLIKELY(!lock_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(obj_lock), K(lock_id));
  } else {
    {
      RDLockGuard guard(obj_lock->rwlock_);
      is_empty = (obj_lock->size_without_lock() == 0);
    }
    if (is_empty) {
      WRLockGuard guard(obj_lock->rwlock_);
      // lock and delete flag make sure no one insert a new op.
      // but maybe have deleted by another concurrent thread.
      if (obj_lock->size_without_lock() == 0 && !obj_lock->is_deleted()) {
        obj_lock->set_deleted();
        if (OB_FAIL(get_obj_lock_with_ref_(lock_id, recheck_ptr))) {
          if (ret != OB_OBJ_LOCK_NOT_EXIST) {
            LOG_WARN("remove obj lock failed", K(ret), K(lock_id));
          }
        } else if (obj_lock != recheck_ptr) {
          LOG_WARN("the obj lock at map is not me, do nothing", K(lock_id), KP(obj_lock),
                   KP(recheck_ptr));
        } else if (OB_FAIL(lock_map_.del(lock_id, obj_lock))) {
          LOG_WARN("remove obj lock from map failed. ", K(ret), K(lock_id));
        } else {
          LOG_DEBUG("remove obj lock successfully", K(ret), K(lock_id), KPC(obj_lock));
        }
      }
    }
    if (OB_NOT_NULL(recheck_ptr)) {
      lock_map_.revert(recheck_ptr);
    }
  }
  LOG_DEBUG("try remove lock owner list map. ", K(ret), K(is_empty), K(lock_id),
            K(obj_lock));
}

}
}
}
