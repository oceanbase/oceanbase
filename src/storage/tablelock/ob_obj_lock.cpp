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

#include "ob_obj_lock.h"
#include "storage/memtable/ob_lock_wait_mgr.h"
#include "storage/tx/ob_trans_service.h"
#include "storage/tablelock/ob_lock_memtable.h"
#include "storage/tablelock/ob_table_lock_deadlock.h"
#include "storage/tx_storage/ob_ls_service.h"

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
static const int64_t DEFAULT_RWLOCK_TIMEOUT_US = 24L * 3600L * 1000L * 1000L;  // 1 day

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
  max_split_epoch_ = SCN::invalid_scn();
  memset(map_, 0, sizeof(ObTableLockOpList *) * TABLE_LOCK_MODE_COUNT);
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
    const uint64_t lock_mode_cnt_in_same_trans[],
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
  // WRLockGuard guard(rwlock_);

  int64_t abs_timeout_us = ObTimeUtility::current_time() + DEFAULT_RWLOCK_TIMEOUT_US;
  if (param.expired_time_ != 0) {
    abs_timeout_us = std::min(param.expired_time_, abs_timeout_us);
  }
  WRLockGuardWithTimeout guard(rwlock_, abs_timeout_us, ret);

  if (OB_FAIL(ret)) {
    if (OB_TIMEOUT == ret) {
      ret = OB_EAGAIN;
    }
    LOG_WARN("try get write lock of obj failed", K(ret), KPC(this), K(param), K(lock_op), K(abs_timeout_us));
  } else if (is_deleted_) {
    ret = OB_EAGAIN;
  } else if (OB_FAIL(check_allow_lock_(lock_op,
                                       lock_mode_cnt_in_same_trans,
                                       conflict_tx_set,
                                       conflict_with_dml_lock,
                                       true,  /* include_finish_tx */
                                       false, /* only_check_dml_lock */
                                       param.is_for_replace_))) {
    if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
      LOG_WARN("check allow lock failed", K(ret), K(lock_op));
    }
  } else if (OB_FAIL(get_or_create_op_list(lock_op.lock_mode_, tenant_id, allocator, op_list))) {
    LOG_WARN("get or create owner map failed.", K(ret));
  } else if (OB_ISNULL(ptr = allocator.alloc(sizeof(ObTableLockOpLinkNode), attr))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alllocate ObTableLockOpLinkNode ", K(ret));
  } else if (FALSE_IT(lock_op_node = new (ptr) ObTableLockOpLinkNode())) {
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
  int64_t abs_timeout_us = ObTimeUtility::current_time() + DEFAULT_RWLOCK_TIMEOUT_US;
  if (lock_op.op_type_ == TABLET_SPLIT) {
    // WRLockGuard guard(rwlock_);
    WRLockGuardWithTimeout guard(rwlock_, abs_timeout_us, ret);
    if (OB_FAIL(ret)) {
      if (OB_TIMEOUT == ret) {
        ret = OB_EAGAIN;
      }
      LOG_WARN("try get write lock of obj failed", K(ret), KPC(this), K(lock_op), K(abs_timeout_us));
    } else {
      timeguard.click("wlock");
      LOG_INFO("recover tablet_split lock_op", K(lock_op));
      if (is_deleted_) {
        // need retry from upper layer.
        ret = OB_EAGAIN;
      } else {
        max_split_epoch_ = lock_op.commit_scn_;
      }
    }
  } else {
    if (OB_UNLIKELY(!lock_op.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument.", K(ret), K(lock_op));
    } else if (FALSE_IT(timeguard.click("start"))) {
    } else if (OB_LIKELY(!lock_op.need_record_lock_op())) {
      // RDLockGuard guard(rwlock_);
      RDLockGuardWithTimeout guard(rwlock_, abs_timeout_us, ret);
      if (OB_FAIL(ret)) {
        if (OB_TIMEOUT == ret) {
          ret = OB_EAGAIN;
        }
        LOG_WARN("try get read lock of obj failed", K(ret), KPC(this), K(lock_op), K(abs_timeout_us));
      } else {
        timeguard.click("rlock");
        if (is_deleted_) {
          // need retry from upper layer.
          ret = OB_EAGAIN;
        } else if (OB_FAIL(recover_(lock_op, allocator))) {
          LOG_WARN("recover lock failed.", K(ret), K(lock_op));
        }
      }
    } else {
      // WRLockGuard guard(rwlock_);
      WRLockGuardWithTimeout guard(rwlock_, abs_timeout_us, ret);
      if (OB_FAIL(ret)) {
        if (OB_TIMEOUT == ret) {
          ret = OB_EAGAIN;
        }
        LOG_WARN("try get write lock of obj failed", K(ret), KPC(this), K(lock_op), K(abs_timeout_us));
      } else {
        timeguard.click("wlock");
        if (is_deleted_) {
          // need retry from upper layer.
          ret = OB_EAGAIN;
        } else if (OB_FAIL(recover_(lock_op, allocator))) {
          LOG_WARN("recover lock failed.", K(ret), K(lock_op));
        }
      }
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
      // RDLockGuard guard(rwlock_);
      int64_t abs_timeout_us = ObTimeUtility::current_time() + DEFAULT_RWLOCK_TIMEOUT_US;
      RDLockGuardWithTimeout guard(rwlock_, abs_timeout_us, ret);
      if (OB_FAIL(ret)) {
        if (OB_TIMEOUT == ret) {
          ret = OB_EAGAIN;
        }
        LOG_WARN("try get read lock of obj failed", K(ret), KPC(this), K(lock_op), K(abs_timeout_us));
      } else if (is_deleted_) {
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
      // WRLockGuard guard(rwlock_);
      int64_t abs_timeout_us = ObTimeUtility::current_time() + DEFAULT_RWLOCK_TIMEOUT_US;
      WRLockGuardWithTimeout guard(rwlock_, abs_timeout_us, ret);
      if (OB_FAIL(ret)) {
        if (OB_TIMEOUT == ret) {
          ret = OB_EAGAIN;
        }
        LOG_WARN("try get write lock of obj failed", K(ret), KPC(this), K(lock_op), K(abs_timeout_us));
      } else if (is_deleted_) {
        // the op is deleted, no need update its status.
        LOG_WARN("the lock is deleted, no need do compact", K(lock_op));
      } else if (OB_TMP_FAIL(get_op_list(lock_op.lock_mode_, op_list))) {
        LOG_WARN("get lock list failed, no need do compact", K(tmp_ret), K(lock_op));
      } else if (OB_TMP_FAIL(compact_tablelock_(lock_op, op_list, allocator, unused_is_compacted))) {
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
    const uint64_t lock_mode_cnt_in_same_trans[],
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
                                       lock_mode_cnt_in_same_trans,
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
    const uint64_t lock_mode_cnt_in_same_trans[],
    ObMalloc &allocator,
    ObTxIDSet &conflict_tx_set)
{
  int ret = OB_SUCCESS;
  int64_t abs_timeout_us = ObTimeUtility::current_time() + DEFAULT_RWLOCK_TIMEOUT_US;
  if (param.expired_time_ != 0) {
    abs_timeout_us = std::min(param.expired_time_, abs_timeout_us);
  }
  {
    // lock first time
    // RDLockGuard guard(rwlock_);
    RDLockGuardWithTimeout guard(rwlock_, abs_timeout_us, ret);
    if (OB_FAIL(ret)) {
      if (OB_TIMEOUT == ret) {
        ret = OB_EAGAIN;
      }
      LOG_WARN("try get read lock of obj failed", K(ret), KPC(this), K(param), K(lock_op), K(abs_timeout_us));
    } else if (OB_FAIL(try_fast_lock_(lock_op, lock_mode_cnt_in_same_trans, conflict_tx_set))) {
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
    const uint64_t lock_mode_cnt_in_same_trans[],
    ObMalloc &allocator,
    ObTxIDSet &conflict_tx_set)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t USLEEP_TIME = 100; // 0.1 ms
  // 1. lock myself.
  // 2. try to lock.
  LOG_DEBUG("ObOBJLock::lock ", K(param), K(lock_op));
  if (OB_UNLIKELY(has_splitted())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("this obj has been splitted as src_tablet, should not get lock request", K(ret), KPC(this));
  } else if (OB_UNLIKELY(!lock_op.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument.", K(ret), K(lock_op));
  } else {
    if (OB_LIKELY(!lock_op.need_record_lock_op())) {
      if (OB_FAIL(fast_lock(param,
                            lock_op,
                            lock_mode_cnt_in_same_trans,
                            allocator,
                            conflict_tx_set))) {
        if (ret != OB_TRY_LOCK_ROW_CONFLICT &&
            ret != OB_EAGAIN) {
          LOG_WARN("lock failed.", K(ret), K(lock_op));
        }
      }
    } else if (OB_FAIL(slow_lock(param,
                                 lock_op,
                                 lock_mode_cnt_in_same_trans,
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
      // WRLockGuard guard(rwlock_);
      int64_t abs_timeout_us = ObTimeUtility::current_time() + DEFAULT_RWLOCK_TIMEOUT_US;
      if (expired_time != 0) {
        abs_timeout_us = std::min(expired_time, abs_timeout_us);
      }
      WRLockGuardWithTimeout guard(rwlock_, abs_timeout_us, ret);
      if (OB_FAIL(ret)) {
        if (OB_TIMEOUT == ret) {
          ret = OB_EAGAIN;
        }
        LOG_WARN("try get write lock of obj failed", K(ret), KPC(this), K(unlock_op), K(abs_timeout_us));
      } else if (!is_try_lock && OB_UNLIKELY(ObClockGenerator::getClock() >= expired_time)) {
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
    // WRLockGuard guard(rwlock_);
    int64_t abs_timeout_us = ObTimeUtility::current_time() + DEFAULT_RWLOCK_TIMEOUT_US;
    WRLockGuardWithTimeout guard(rwlock_, abs_timeout_us, ret);
    if (OB_FAIL(ret)) {
      if (OB_TIMEOUT == ret) {
        ret = OB_EAGAIN;
      }
      LOG_WARN("try get write lock of obj failed", K(ret), KPC(this), K(lock_op), K(abs_timeout_us));
    } else {
      op_list = map_[map_index];
      delete_lock_op_from_list_(lock_op, op_list, allocator);
      drop_op_list_if_empty_(lock_op.lock_mode_,
                            op_list,
                            allocator);
      wakeup_waiters_(lock_op);
    }
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
  int64_t abs_timeout_us = ObTimeUtility::current_time() + DEFAULT_RWLOCK_TIMEOUT_US;
  // RDLockGuard guard(rwlock_);
  RDLockGuardWithTimeout guard(rwlock_, abs_timeout_us, ret);
  if (OB_FAIL(ret)) {
    if (OB_TIMEOUT == ret) {
      ret = OB_EAGAIN;
    }
    LOG_WARN("try get read lock of obj failed", K(ret), KPC(this), K(abs_timeout_us));
  } else {
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
  }
  return min_rec_scn;
}

int ObOBJLock::get_table_lock_store_info(
    ObIArray<ObTableLockOp> &store_arr,
    const SCN &freeze_scn)
{
  int ret = OB_SUCCESS;
  // RDLockGuard guard(rwlock_);
  int64_t abs_timeout_us = ObTimeUtility::current_time() + DEFAULT_RWLOCK_TIMEOUT_US;
  RDLockGuardWithTimeout guard(rwlock_, abs_timeout_us, ret);
  if (OB_FAIL(ret)) {
    if (OB_TIMEOUT == ret) {
      ret = OB_EAGAIN;
    }
    LOG_WARN("try get read lock of obj failed", K(ret), KPC(this), K(abs_timeout_us));
  } else {
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
  }

  if (OB_SUCC(ret)) {
    // store split info
    SCN max_split_epoch = max_split_epoch_;
    if (max_split_epoch.is_valid() && !max_split_epoch.is_min()) {
      ObTableLockOp split_store_info;
      split_store_info.op_type_ = TABLET_SPLIT;
      split_store_info.commit_scn_ = max_split_epoch;
      split_store_info.lock_id_ = lock_id_;
      if (OB_FAIL(store_arr.push_back(split_store_info))) {
        LOG_WARN("failed to push back table lock info arr", K(ret));
      }
    }
  }

  return ret;
}

int ObOBJLock::compact_tablelock(ObMalloc &allocator,
                                 bool &is_compacted,
                                 const bool is_force) {
  int ret = OB_SUCCESS;
  int64_t abs_timeout_us = ObTimeUtility::current_time() + DEFAULT_RWLOCK_TIMEOUT_US;
  // WRLockGuard guard(rwlock_);
  WRLockGuardWithTimeout guard(rwlock_, abs_timeout_us, ret);
  if (OB_FAIL(ret)) {
    if (OB_TIMEOUT == ret) {
      ret = OB_EAGAIN;
    }
    LOG_WARN("try get write lock of obj failed", K(ret), KPC(this), K(abs_timeout_us));
  } else if (OB_FAIL(compact_tablelock_(allocator, is_compacted, is_force))) {
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
  // lock twice (except obj_type is DBMS_LOCK).
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

int ObOBJLock::eliminate_conflict_caused_by_split_if_need_(
    const ObTableLockOp &lock_op,
    storage::ObStoreCtx &ctx)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  // WRLockGuard guard(rwlock_);
  int64_t abs_timeout_us = ObTimeUtility::current_time() + DEFAULT_RWLOCK_TIMEOUT_US;
  WRLockGuardWithTimeout guard(rwlock_, abs_timeout_us, ret);
  if (OB_FAIL(ret)) {
    if (OB_TIMEOUT == ret) {
      ret = OB_EAGAIN;
    }
    LOG_WARN("try get write lock of obj failed", K(ret), K(lock_op), KPC(this), K(abs_timeout_us));
  } else if (has_splitted()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("this obj has been splitted as src_tablet, should not get lock request", K(ret), KPC(this));
  } else if (max_split_epoch_.is_valid()) {
    ObTableLockMode curr_lock_mode = ROW_EXCLUSIVE;
    int64_t conflict_modes = 0;
    if (!request_lock(curr_lock_mode, lock_op.lock_mode_, conflict_modes)) {
      bool conflict = true;
      ObTransService *txs = NULL;
      ObLSTxCtxMgr *ls_tx_ctx_mgr = NULL;
      SCN min_scn;
      if (OB_ISNULL(txs = MTL(ObTransService*))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("trans_service should not empty");
      } else if (OB_FAIL(txs->get_tx_ctx_mgr().get_ls_tx_ctx_mgr(ctx.ls_id_,
                                                                ls_tx_ctx_mgr))) {
        LOG_WARN("get_ls_tx_ctx_mgr failed", K(ret));
      } else if (OB_FAIL(ls_tx_ctx_mgr->get_min_start_scn(min_scn))) {
        LOG_WARN("get_min_start_scn failed", K(ret));
      } else if (min_scn >= max_split_epoch_) {  //ensure active trans over before split
        LOG_INFO("reset max_split_epoch_ when active trans over before split",
                K(min_scn), K(max_split_epoch_));
        max_split_epoch_ = SCN::invalid_scn();
        conflict = false;
      }
      if (OB_SUCC(ret) && conflict) {
        ret = OB_TRY_LOCK_ROW_CONFLICT;
      }
      if (OB_NOT_NULL(ls_tx_ctx_mgr)) {
        if (OB_TMP_FAIL(txs->get_tx_ctx_mgr().revert_ls_tx_ctx_mgr(ls_tx_ctx_mgr))) {
          ret = OB_SUCCESS == ret ? tmp_ret : ret;
          LOG_WARN("revert ls tx ctx mgr with ref failed", K(ret), K(tmp_ret));
        }
      }
    }
  }
  return ret;
}

int ObOBJLock::check_allow_lock_(
    const ObTableLockOp &lock_op,
    const uint64_t lock_mode_cnt_in_same_trans[],
    ObTxIDSet &conflict_tx_set,
    bool &conflict_with_dml_lock,
    const bool include_finish_tx,
    const bool only_check_dml_lock,
    const bool check_for_replace)
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
  } else if (OB_FAIL(get_exist_lock_mode_without_curr_trans(lock_mode_cnt_in_same_trans,
                                                            curr_lock_mode,
                                                            lock_op.create_trans_id_,
                                                            check_for_replace))) {
    LOG_WARN("meet unexpected error during get lock_mode without current trans", K(ret));
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
    if (OB_TMP_FAIL(get_tx_id_set_(lock_op.create_trans_id_,
                                   conflict_modes,
                                   include_finish_tx,
                                   conflict_tx_set))) {
      LOG_WARN("get conflict tx failed", K(tmp_ret), K(lock_op));
    }
    if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
      LOG_WARN("obj_lock conflict with others", K(ret), KNN(curr_lock, lock_op), KNN(conflict_tx, conflict_tx_set),
               K(conflict_modes));
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
  LOG_DEBUG("check_allow_lock", K(ret), K(curr_lock_mode),
            K(lock_op.lock_mode_), K(lock_op), K(conflict_modes), K(conflict_tx_set));
  return ret;
}

int ObOBJLock::check_allow_lock(
    const ObTableLockOp &lock_op,
    const uint64_t lock_mode_cnt_in_same_trans[],
    ObTxIDSet &conflict_tx_set,
    bool &conflict_with_dml_lock,
    const int64_t expired_time,
    ObMalloc &allocator,
    const bool include_finish_tx,
    const bool only_check_dml_lock)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!lock_op.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument.", K(ret), K(lock_op));
  } else {
    // RDLockGuard guard(rwlock_);
    int64_t abs_timeout_us = ObTimeUtility::current_time() + DEFAULT_RWLOCK_TIMEOUT_US;
    if (expired_time != 0) {
      abs_timeout_us = std::min(expired_time, abs_timeout_us);
    }
    RDLockGuardWithTimeout guard(rwlock_, abs_timeout_us, ret);
    if (OB_FAIL(ret)) {
      if (OB_TIMEOUT == ret) {
        ret = OB_EAGAIN;
      }
      LOG_WARN("try get read lock of obj failed", K(ret), KPC(this), K(lock_op), K(abs_timeout_us));
    } else {
      ret = check_allow_lock_(lock_op,
                              lock_mode_cnt_in_same_trans,
                              conflict_tx_set,
                              conflict_with_dml_lock,
                              include_finish_tx,
                              only_check_dml_lock);
    }
  }
  return ret;
}

int ObOBJLock::get_exist_lock_mode_without_curr_trans(const uint64_t lock_mode_cnt_in_same_trans[],
                                                      ObTableLockMode &lock_mode_without_curr_trans,
                                                      const ObTransID &trans_id,
                                                      const bool is_for_replace)
{
  int ret = OB_SUCCESS;
  ObTableLockMode lock_mode = NO_LOCK;
  int64_t lock_mode_cnt[TABLE_LOCK_MODE_COUNT] = {0};
  for (int64_t i = 0; i < TABLE_LOCK_MODE_COUNT; i++) {
    // 1. count the number of lock_ops for each lock_mode
    lock_mode = get_lock_mode_by_index(i);
    lock_mode_cnt[i] = OB_ISNULL(map_[i]) ? 0 : map_[i]->get_size();
    if (ROW_SHARE == lock_mode) {
      lock_mode_cnt[i] += row_share_;
    } else if (ROW_EXCLUSIVE == lock_mode) {
      lock_mode_cnt[i] += row_exclusive_;
    }
    // 2. remove lock_modes which is held by current transaction
    lock_mode_cnt[i] -= lock_mode_cnt_in_same_trans[i];
    // 2.1 recheck for replace
    if (is_for_replace) {
      LOG_DEBUG("recheck for replace begin", K(lock_mode), K(lock_mode_cnt[i]), K(lock_mode_cnt_in_same_trans[i]));
      // Only one case should be recehcked: lock_mode of this obj is exsited,
      // but there's no this lock_mode actually becuase current transaction
      // has unlocked it in the replace progress.
      // It happens because there may be 2 lock_ops with the same lock_mode
      // in the replace situation, 1 is lock_op which is completed, and the
      // other is unlock_op which is still doing. It means this lock_mode has
      // no locks on it now.
      // NOTICE: we can only replace OUT_TRANS_LOCK, so the lock_op which is completed and the
      // unlock_op which is doing should be both in the map. If there're less than 2 lock_ops
      // in the map, we don't need to check allow replace, too.
      if (lock_mode_cnt[i] >= 2 && OB_NOT_NULL(map_[i]) && map_[i]->get_size() >= 2) {
        bool allow_replace = false;
        if (OB_FAIL(check_allow_replace_from_list_(map_[i], trans_id, allow_replace))) {
          LOG_WARN("check allow replace failed", K(i));
        } else if (allow_replace) {
          lock_mode_cnt[i] = 0;
        }
      }
      LOG_DEBUG("recheck for replace end", K(lock_mode), K(lock_mode_cnt[i]), K(lock_mode_cnt_in_same_trans[i]));
    }
    // 2.2 check valid for the lock op of current transaction
    check_curr_trans_lock_is_valid_(lock_mode_cnt_in_same_trans, lock_mode_cnt);

    // 3. set the lock_mode without current transaction
    lock_mode_without_curr_trans |= (lock_mode_cnt[i] > 0 ? lock_mode : 0);
  }

  LOG_DEBUG("get_exist_lock_mode_without_cur_trans",
            K(lock_mode_without_curr_trans),
            K(is_for_replace));
  return ret;
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
  int ret = OB_SUCCESS;
  // WRLockGuard guard(rwlock_);
  int64_t abs_timeout_us = ObTimeUtility::current_time() + DEFAULT_RWLOCK_TIMEOUT_US;
  WRLockGuardWithTimeout guard(rwlock_, abs_timeout_us, ret);
  if (OB_FAIL(ret)) {
    if (OB_TIMEOUT == ret) {
      ret = OB_EAGAIN;
    }
    LOG_WARN("try get write lock of obj failed", K(ret), KPC(this), K(abs_timeout_us));
  } else {
    reset_(allocator);
  }
}

void ObOBJLock::reset_without_lock(ObMalloc &allocator)
{
  reset_(allocator);
}

void ObOBJLock::print() const
{
  // RDLockGuard guard(rwlock_);
  int ret = OB_SUCCESS;
  int64_t abs_timeout_us = ObTimeUtility::current_time() + DEFAULT_RWLOCK_TIMEOUT_US;
  RDLockGuardWithTimeout guard(rwlock_, abs_timeout_us, ret);
  if (OB_FAIL(ret)) {
    if (OB_TIMEOUT == ret) {
      ret = OB_EAGAIN;
    }
    LOG_WARN("try get read lock of obj failed", K(ret), KPC(this), K(abs_timeout_us));
  } else {
    print_();
  }
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
      LOG_INFO("ObOBJLock: mode:", K(get_lock_mode_by_index(i)));
      print_op_list(op_list);
    }
  }
  LOG_INFO("ObOBJLock: ", K_(row_share), K_(row_exclusive));
}

int ObOBJLock::get_lock_op_iter(const ObLockID &lock_id,
                                ObLockOpIterator &iter) const
{
  int ret = OB_SUCCESS;
  // RDLockGuard guard(rwlock_);
  int64_t abs_timeout_us = ObTimeUtility::current_time() + DEFAULT_RWLOCK_TIMEOUT_US;
  RDLockGuardWithTimeout guard(rwlock_, abs_timeout_us, ret);
  if (OB_FAIL(ret)) {
    if (OB_TIMEOUT == ret) {
      ret = OB_EAGAIN;
    }
    LOG_WARN("try get read lock of obj failed", K(ret), KPC(this), K(abs_timeout_us));
  } else {
    ObTableLockOpList *op_list = NULL;
    for (int i = 0; OB_SUCC(ret) && i < TABLE_LOCK_MODE_COUNT; i++) {
      op_list = map_[i];
      if (NULL != op_list) {
        if (OB_FAIL(get_lock_op_list_iter_(op_list, iter))) {
          TABLELOCK_LOG(WARN, "get lock op list iter failed", K(ret), K(i));
        }
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
    tmp_op.lock_seq_no_ = ObTxSEQ::cast_from_int(row_exclusive_);
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

int ObOBJLock::reset_split_epoch()
{
  int ret = OB_SUCCESS;
  // WRLockGuard guard(rwlock_);
  int64_t abs_timeout_us = ObTimeUtility::current_time() + DEFAULT_RWLOCK_TIMEOUT_US;
  WRLockGuardWithTimeout guard(rwlock_, abs_timeout_us, ret);
  if (OB_FAIL(ret)) {
    if (OB_TIMEOUT == ret) {
      ret = OB_EAGAIN;
    }
    LOG_WARN("try get write lock of obj failed", K(ret), KPC(this), K(abs_timeout_us));
  } else if (OB_FAIL(reset_split_epoch_())) {
    LOG_WARN("reset split epoch failed", KPC(this));
  }
  return ret;
}

int ObOBJLockMap::add_split_epoch(const ObLockID &lock_id,
                                  const share::SCN &split_epoch,
                                  const bool for_replay)
{
  int ret = OB_SUCCESS;
  ObOBJLock *obj_lock = NULL;
  do {
    if (OB_FAIL(get_or_create_obj_lock_with_ref_(lock_id, obj_lock))) {
      LOG_WARN("get_or_create_obj_lock_with_ref failed", K(ret), K(lock_id));
    } else if (OB_FAIL(obj_lock->set_split_epoch(split_epoch, for_replay))) {
      LOG_WARN("set_split_epoch failed", K(lock_id), K(split_epoch), K(ret), K(for_replay));
    } else {
      LOG_INFO("add_split_epoch successfully", K(split_epoch), K(lock_id));
    }

    if (OB_NOT_NULL(obj_lock)) {
      lock_map_.revert(obj_lock);
    }
  } while (OB_EAGAIN == ret);
  return ret;
}

int ObOBJLockMap::add_split_epoch(const ObSArray<ObLockID> &lock_ids,
                                  const share::SCN &split_epoch,
                                  const bool for_replay)
{
  int ret = OB_SUCCESS;
  ObOBJLock *obj_lock = NULL;
  ObSArray<ObOBJLock *> added_obj_locks;
  for (int64_t i = 0; OB_SUCC(ret) && i < lock_ids.count(); i++) {
    do {
      if (OB_FAIL(get_or_create_obj_lock_with_ref_(lock_ids[i], obj_lock))) {
        LOG_WARN("get_or_create_obj_lock_with_ref failed", K(ret), K(lock_ids[i]));
      } else if (OB_ISNULL(obj_lock)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_ERROR("obj_lock is null after get_or_create_obj_lock_with_ref_ for tablet", K(ret), K(lock_ids[i]));
      } else if (OB_FAIL(obj_lock->set_split_epoch(split_epoch, for_replay))) {
        LOG_WARN("the obj_lock is deleted", K(obj_lock), K(split_epoch), K(ret));
      } else if (OB_FAIL(added_obj_locks.push_back(obj_lock))) {
        LOG_WARN("add obj_lock into added_obj_locks failed", K(obj_lock), K(added_obj_locks.count()));
        if (OB_FAIL(obj_lock->reset_split_epoch())) {
          ret = OB_ERR_UNEXPECTED;  // the obj_lock with valid max_split_epoch_ should not be deleted
          LOG_ERROR("meet fails when push obj_lock to added_obj_locks, and reset it fails", K(ret), K(obj_lock));
        }
      } else {
        LOG_INFO("add_split_epoch successfully", K(split_epoch), K(lock_ids[i]));
      }
      // If meet fails, the last obj_lock is not in the added_obj_locks,
      // so we shuold revert it individually.
      if (OB_FAIL(ret) && OB_NOT_NULL(obj_lock)) {
        lock_map_.revert(obj_lock);
      }
    } while (OB_EAGAIN == ret);
  }

  // If meet fails, reset split epoch for all destination
  // table locks which has been set before.
  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    for (int64_t i = 0; i < added_obj_locks.count(); i++) {
      if (OB_ISNULL(obj_lock = added_obj_locks[i])) {
        tmp_ret = OB_INVALID_ARGUMENT;
        LOG_ERROR("obj_lock is null in added_obj_locks", K(ret), K(tmp_ret), K(i));
      } else if (OB_TMP_FAIL(obj_lock->reset_split_epoch())) {
        ret = OB_ERR_UNEXPECTED;  // the obj_lock with valid max_split_epoch_ should not be deleted
        LOG_ERROR("reset_split_epoch failed", K(ret), K(tmp_ret), KPC(obj_lock), K(split_epoch));
      }
      if (OB_NOT_NULL(obj_lock)) {
        lock_map_.revert(obj_lock);
      }
    }
  }

  return ret;
}

int ObOBJLockMap::get_split_epoch(const ObLockID &lock_id,
                                  share::SCN &split_epoch)
{
  int ret = OB_SUCCESS;
  ObOBJLock *obj_lock = NULL;
  do {
    if (OB_FAIL(get_obj_lock_with_ref_(lock_id, obj_lock))) {
      LOG_WARN("get_or_create_obj_lock_with_ref failed", K(ret), K(lock_id));
    } else if (OB_FAIL(obj_lock->get_split_epoch(split_epoch))) {
      LOG_WARN("the obj_lock is deleted", K(lock_id), K(split_epoch), K(ret));
    }
    if (OB_NOT_NULL(obj_lock)) {
      lock_map_.revert(obj_lock);
    }
  } while (OB_EAGAIN == ret);
  return ret;
}

int ObOBJLock::table_lock_split(const ObTabletID &src_tablet_id,
                                const ObSArray<common::ObTabletID> &dst_tablet_ids,
                                const transaction::ObTransID &trans_id,
                                ObLockTableSplitLogCb &callback)
{
  int ret = OB_SUCCESS;
  // WRLockGuard guard(rwlock_);
  int64_t abs_timeout_us = ObTimeUtility::current_time() + DEFAULT_RWLOCK_TIMEOUT_US;
  WRLockGuardWithTimeout guard(rwlock_, abs_timeout_us, ret);

  if (OB_FAIL(ret)) {
    if (callback.is_logging_) {
      ret = OB_TABLE_LOCK_IS_SPLITTING;
      LOG_WARN("table lock of this tablet is splitting",
               K(ret),
               KPC(this),
               K(src_tablet_id),
               K(dst_tablet_ids),
               K(trans_id),
               K(abs_timeout_us));
    } else if (OB_TIMEOUT == ret) {
      ret = OB_EAGAIN;
    }
    LOG_WARN("try get write lock of obj failed",
             K(ret),
             KPC(this),
             K(src_tablet_id),
             K(dst_tablet_ids),
             K(trans_id),
             K(abs_timeout_us));
  } else {
    bool exist_cannot_split_lock = exist_cannot_split_lock_(trans_id);
    bool need_split = need_split_();

    if (!exist_cannot_split_lock && need_split) {
      if (OB_FAIL(submit_log_(callback, src_tablet_id, dst_tablet_ids))) {
        LOG_WARN("submit log for splitting table lock failed", K(src_tablet_id), K(dst_tablet_ids), K(trans_id));
      } else {
        // To ensure the safety of the ObLockTableSplitLogCb in the lock_memtable,
        // we should hold the handle of the lock memtable during the callback is logging
        // (include submit log and wait for callback on_success or on_failure)
        LOG_WARN("finish log, wait for callback");
        while (callback.is_logging_) {
          if (REACH_TIME_INTERVAL(3 * 1000 * 1000)) {
            LOG_INFO("wait for the table lock split callback", K(ret), K(src_tablet_id), K(dst_tablet_ids));
          }
          ob_usleep(1000 * 1000);
        }
        // table lock split sucessfully on src_tablet, can not be rollbacked later
        if (callback.cb_success()) {
          set_split_epoch_(share::SCN::max_scn());
        }
      }
    }
    if (exist_cannot_split_lock) {
      ret = OB_NOT_SUPPORTED;
      print();
      LOG_WARN("exist can not split lock", K(ret), K(lock_id_), K(src_tablet_id));
    }
    if (!need_split) {
      if (has_splitted()) {
        ret = OB_TABLE_LOCK_SPLIT_TWICE;
        LOG_WARN("table lock has splitted", K(ret), KPC(this));
      }
      LOG_WARN("no need to split table lock", K(ret), KPC(this));
    }
  }
  return ret;
}

int ObOBJLock::set_split_epoch(const share::SCN &scn, const bool for_replay)
{
  int ret = OB_SUCCESS;
  // WRLockGuard guard(rwlock_);
  int64_t abs_timeout_us = ObTimeUtility::current_time() + DEFAULT_RWLOCK_TIMEOUT_US;
  WRLockGuardWithTimeout guard(rwlock_, abs_timeout_us, ret);
  if (OB_FAIL(ret)) {
    if (OB_TIMEOUT == ret) {
      ret = OB_EAGAIN;
    }
    LOG_WARN("try get write lock of obj failed", K(ret), KPC(this), K(abs_timeout_us));
  } else if (OB_FAIL(set_split_epoch_(scn, for_replay))) {
    LOG_WARN("set split epoch failed", KPC(this), K(scn), K(for_replay));
  }
  return ret;
}

int ObOBJLock::get_split_epoch(share::SCN &scn)
{
  int ret = OB_SUCCESS;
  // RDLockGuard guard(rwlock_);
  int64_t abs_timeout_us = ObTimeUtility::current_time() + DEFAULT_RWLOCK_TIMEOUT_US;
  RDLockGuardWithTimeout guard(rwlock_, abs_timeout_us, ret);
  if (OB_FAIL(ret)) {
    if (OB_TIMEOUT == ret) {
      ret = OB_EAGAIN;
    }
    LOG_WARN("try get read lock of obj failed", K(ret), KPC(this), K(abs_timeout_us));
  } else if (OB_FAIL(get_split_epoch_(scn))) {
    LOG_WARN("get split epoch failed", KPC(this));
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
            if (OB_TRY_LOCK_ROW_CONFLICT == ret) {
              if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
                LOG_WARN("obj_lock conflict with itself", K(ret), KNN(curr_lock, lock_op),
                         KNN(older_lock, curr->lock_op_));
              }
            }
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
          if (OB_TRY_LOCK_ROW_CONFLICT == ret) {
            if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
              LOG_WARN("obj_lock conflict with itself", K(ret), KNN(curr_lock, lock_op),
                       KNN(older_lock, curr->lock_op_));
            }
          }
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

int ObOBJLock::check_allow_replace_from_list_(ObTableLockOpList *op_list, const ObTransID &trans_id, bool &allow_replace)
{
  int ret = OB_SUCCESS;
  hash::ObHashMap<int64_t, ObTableLockOp> lock_op_map;
  int32_t lock_op_cnt = op_list->get_size();

  allow_replace = false;
  ObTableLockOwnerID owner_id;
  ObTableLockOp *lock_op = nullptr;

  LOG_DEBUG("start check_allow_replace_from_list_", K(lock_op_cnt));
  if (OB_FAIL(lock_op_map.create(10, lib::ObMemAttr(MTL_ID(), "TableLockOpMap")))) {
    LOG_WARN("create lock_map for replace check failed", K(ret));
  } else {
    DLIST_FOREACH_NORET(curr, *op_list)
    {
      int64_t owner_id = curr->get_owner_id().id();
      if (FALSE_IT(lock_op = lock_op_map.get(owner_id))) {
      } else {
        if (OB_ISNULL(lock_op)) {
          LOG_DEBUG("lock_op not in the map, will set it", K(curr->lock_op_));
          if (OB_FAIL(lock_op_map.set_refactored(owner_id, curr->lock_op_))) {
            LOG_WARN("set lock_op into map failed", K(ret), K(curr->lock_op_));
          }
        } else if ((OUT_TRANS_LOCK == lock_op->op_type_ && OUT_TRANS_UNLOCK == curr->lock_op_.op_type_
                    && LOCK_OP_COMPLETE == lock_op->lock_op_status_ && LOCK_OP_DOING == curr->lock_op_.lock_op_status_
                    && trans_id == curr->lock_op_.create_trans_id_)
                   || (OUT_TRANS_UNLOCK == lock_op->op_type_ && OUT_TRANS_LOCK == curr->lock_op_.op_type_
                       && LOCK_OP_DOING == lock_op->lock_op_status_
                       && LOCK_OP_COMPLETE == curr->lock_op_.lock_op_status_
                       && trans_id == lock_op->create_trans_id_)) {
          LOG_DEBUG("find matched lock_op, will remove them", K(lock_op_cnt), KPC(lock_op), K(curr->lock_op_));
          lock_op_cnt -= 2;
        } else {
          allow_replace = false;
          break;
        }
      }
    }
  }
  LOG_DEBUG("finsih check_allow_replace_from_list_", K(lock_op_cnt));
  if (OB_SUCC(ret) && 0 == lock_op_cnt) {
    allow_replace = true;
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
    curr_mode = get_lock_mode_by_index(i);
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

int ObOBJLock::register_into_deadlock_detector_(const ObStoreCtx &ctx,
                                                const ObTableLockOp &lock_op)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObTransLockPartID tx_lock_part_id;
  ObAddr parent_addr;
  const ObLSID &ls_id = ctx.ls_id_;
  const int64_t priority = ~(ctx.mvcc_acc_ctx_.tx_desc_->get_active_ts());
  tx_lock_part_id.lock_id_ = lock_op.lock_id_;
  tx_lock_part_id.trans_id_ = lock_op.create_trans_id_;
  if (OB_FAIL(ObTableLockDeadlockDetectorHelper::register_trans_lock_part(
      tx_lock_part_id, ls_id, priority))) {
    LOG_WARN("register trans lock part failed", K(ret), K(tx_lock_part_id),
             K(ls_id));
  } else if (OB_FAIL(ObTransDeadlockDetectorAdapter::get_trans_scheduler_info_on_participant(
      tx_lock_part_id.trans_id_, ls_id, parent_addr))) {
    LOG_WARN("get scheduler address failed", K(tx_lock_part_id), K(ls_id));
  } else if (OB_FAIL(ObTableLockDeadlockDetectorHelper::add_parent(
      tx_lock_part_id, parent_addr, lock_op.create_trans_id_))) {
    LOG_WARN("add parent failed", K(ret), K(tx_lock_part_id));
  } else if (OB_FAIL(ObTableLockDeadlockDetectorHelper::block(tx_lock_part_id,
                                                              ls_id,
                                                              lock_op))) {
    LOG_WARN("add dependency failed", K(ret), K(tx_lock_part_id));
  } else {
    LOG_DEBUG("succeed register to the dead lock detector");
  }
  if (OB_FAIL(ret)) {
    if (OB_SUCCESS != (tmp_ret = ObTableLockDeadlockDetectorHelper::
                       unregister_trans_lock_part(tx_lock_part_id))) {
      if (tmp_ret != OB_ENTRY_NOT_EXIST) {
        LOG_WARN("unregister from deadlock detector failed", K(ret),
                 K(tx_lock_part_id));
      }
    }
  }
  return ret;
}

int ObOBJLock::unregister_from_deadlock_detector_(const ObTableLockOp &lock_op)
{
  int ret = OB_SUCCESS;
  ObTransLockPartID tx_lock_part_id;
  tx_lock_part_id.lock_id_ = lock_op.lock_id_;
  tx_lock_part_id.trans_id_ = lock_op.create_trans_id_;
  if (OB_FAIL(ObTableLockDeadlockDetectorHelper::unregister_trans_lock_part(
      tx_lock_part_id))) {
    LOG_WARN("unregister trans lock part failed", K(ret), K(tx_lock_part_id));
  } else {
    // do nothing
  }
  return ret;
}

bool ObOBJLock::exist_cannot_split_lock_(const ObTransID &split_start_trans_id) const
{
  int ret = OB_SUCCESS;
  bool exist_cannot_split_lock = false;
  ObTableLockOpList *op_list = NULL;
  for (int i = 0; i < TABLE_LOCK_MODE_COUNT; i++) {
    op_list = map_[i];
    if (NULL != op_list) {
      DLIST_FOREACH_NORET(curr, *op_list) {
        if (NULL != curr &&
            curr->lock_op_.create_trans_id_ != split_start_trans_id) {
          exist_cannot_split_lock = true;
          break;
        }
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("op_list of this obj_lock is null!", K(lock_id_));
    }
  }
  return exist_cannot_split_lock;
}

bool ObOBJLock::need_split_() const
{
  // The split transaction will lock src_tablet with RX OUT_TRANS_LOCK, so there're
  // no share and exclusive locks on the src_tablet.
  // So we can only consider about splitting RS and RX locks on the src_tablet.
  // In addition, if the max_split_epoch_ is valid but not min_scn, it means that
  // we have gotten into splitting process before but it didn't finish, so it still
  // needs split. (It may be a continuous splitting scenario.)
  // And if the max_split_epoch_ is max_scn, it means that it has been splitted,
  // so it doesn't need split.
  return (row_exclusive_ != 0) || (row_share_ != 0)
         || (max_split_epoch_.is_valid() && !max_split_epoch_.is_min() && !max_split_epoch_.is_max());
}

int ObOBJLock::submit_log_(ObLockTableSplitLogCb &callback,
                           common::ObTabletID src_tablet_id,
                           const ObSArray<common::ObTabletID> &dst_tablet_ids)
{
  int ret = OB_SUCCESS;
  ObLSID ls_id;

  if (!callback.is_logging_) {
    callback.is_logging_ = true;
    ObLockTableSplitLog split_log;
    if (OB_FAIL(split_log.init(src_tablet_id, dst_tablet_ids))) {
      LOG_WARN("init split_log failed", K(ret), K(src_tablet_id), K(dst_tablet_ids));
    } else if (OB_FAIL(callback.set(src_tablet_id, dst_tablet_ids))) {
      LOG_WARN("set split_log_cb failed", K(ret), K(src_tablet_id), K(dst_tablet_ids));
    } else {
      char *buffer = nullptr;
      int64_t pos = 0;
      int64_t buffer_size = split_log.get_serialize_size();

      logservice::ObLogBaseHeader base_header(logservice::ObLogBaseType::TABLE_LOCK_LOG_BASE_TYPE,
                                              logservice::ObReplayBarrierType::NO_NEED_BARRIER);
      buffer_size += base_header.get_serialize_size();
      ObLSService *ls_service = nullptr;
      ObLSHandle ls_handle;
      ObLS *ls = nullptr;
      const bool need_nonblock = false;
      palf::LSN lsn;
      SCN scn;
      ObMemAttr attr(MTL_ID(), "SplitLog");
      if (OB_ISNULL(buffer = static_cast<char *>(mtl_malloc(buffer_size, attr)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc buffer", K(ret));
      } else if (OB_FAIL(base_header.serialize(buffer, buffer_size, pos))) {
        LOG_WARN("failed to serialize split log header", K(ret), K(buffer_size), K(pos));
      } else if (OB_FAIL(split_log.serialize(buffer, buffer_size, pos))) {
        LOG_WARN("failed to serialize split log", K(ret), K(buffer_size), K(pos));
      } else if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("mtl ObLSService should not be null", K(ret));
      } else if (OB_FAIL(ls_service->get_ls(callback.ls_id_, ls_handle, ObLSGetMod::OBSERVER_MOD))) {
        LOG_WARN("failed to get ls", K(ret));
      } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ls should not be NULL", K(ret));
      } else if (OB_FAIL(ls->append(buffer, buffer_size, SCN::min_scn(), need_nonblock, false/*allow_compression*/, &callback, lsn, scn))) {
        LOG_WARN("failed to submit log", K(ret), K(buffer_size), K(pos));
      } else {
        // These params should be gotten after append log to LS,
        // so we need to use lock to avoid LS can read callback
        // before these params have been set into it.
        callback.last_submit_scn_ = scn;
        callback.last_submit_log_ts_ = ObTimeUtility::current_time();
        LOG_INFO("submit split log success", K(scn), K(callback), K(src_tablet_id), K(dst_tablet_ids));
      }
      if (nullptr != buffer) {
        mtl_free(buffer);
        buffer = nullptr;
      }
    }
    if (OB_FAIL(ret)) {
      callback.is_logging_ = false;
    }
  } else if (callback.last_submit_log_ts_ == OB_INVALID_TIMESTAMP) {
    LOG_WARN("callback is logging, but hasn't submitted log", K(ret), K(callback));
  } else if (ObTimeUtility::current_time() - callback.last_submit_log_ts_ >
    SUBMIT_LOG_ALARM_INTERVAL && REACH_TIME_INTERVAL(1000L * 1000L)) {
    LOG_WARN("maybe submit log callback use too mush time", K(ret), K(callback));
  }

  return ret;
}

int ObOBJLock::set_split_epoch_(const share::SCN &scn, const bool for_replay)
{
  int ret = OB_SUCCESS;
  if (is_deleted_) {
    ret = OB_EAGAIN;
    LOG_WARN("the obj_lock is_deleted, and need retry", K(ret));
  } else if (!for_replay && max_split_epoch_ > scn) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN(
      "the input scn is smaller than max_split_epoch_, so we ignore this set_split_epoch", K(max_split_epoch_), K(scn));
  } else {
    max_split_epoch_.inc_update(scn);
  }
  return ret;
}

int ObOBJLock::get_split_epoch_(share::SCN &scn)
{
  int ret = OB_SUCCESS;
  if (is_deleted_) {
    ret = OB_EAGAIN;
    LOG_WARN("the obj_lock is_deleted, and need retry", K(ret));
  } else {
    scn = max_split_epoch_;
  }
  return ret;
}

int ObOBJLock::reset_split_epoch_()
{
  int ret = OB_SUCCESS;
  if (is_deleted_) {
    ret = OB_EAGAIN;
    LOG_WARN("the obj_lock is_deleted, and need retry", K(ret), K(max_split_epoch_));
  } else {
    max_split_epoch_ = SCN::invalid_scn();
  }
  return ret;
}

void ObOBJLock::check_curr_trans_lock_is_valid_(const uint64_t lock_mode_cnt_in_same_trans[], const int64_t *lock_mode_cnt)
{
  int err_code = 0;
  int64_t exclusive_cnt = lock_mode_cnt[get_index_by_lock_mode(EXCLUSIVE)];
  int64_t share_row_exclusive_cnt = lock_mode_cnt[get_index_by_lock_mode(SHARE_ROW_EXCLUSIVE)];
  int64_t share_cnt = lock_mode_cnt[get_index_by_lock_mode(SHARE)];
  int64_t row_exclusive_cnt = lock_mode_cnt[get_index_by_lock_mode(ROW_EXCLUSIVE)];
  int64_t row_share_cnt = lock_mode_cnt[get_index_by_lock_mode(ROW_SHARE)];

  if (lock_mode_cnt_in_same_trans[get_index_by_lock_mode(EXCLUSIVE)] > 0) {
    if (row_share_cnt > 1 || row_exclusive_cnt > 1 ||
      share_cnt > 1 || share_row_exclusive_cnt > 1 || exclusive_cnt > 1) {
      err_code = 1;
    }
  } else if (lock_mode_cnt_in_same_trans[get_index_by_lock_mode(SHARE_ROW_EXCLUSIVE)] > 0) {
    if (row_exclusive_cnt > 1 || share_cnt > 1 || share_row_exclusive_cnt > 1) {
      err_code = 2;
    }
  } else if (lock_mode_cnt_in_same_trans[get_index_by_lock_mode(SHARE)] > 0) {
    if (row_exclusive_cnt > 1 || share_row_exclusive_cnt > 1) {
      err_code = 3;
    }
  } else if (lock_mode_cnt_in_same_trans[get_index_by_lock_mode(ROW_EXCLUSIVE)] > 0) {
    if (share_cnt > 1 || share_row_exclusive_cnt > 1) {
      err_code = 4;
    }
  }
  if (err_code) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED,
                  "unexpected error",
                  K(err_code),
                  K(row_share_cnt),
                  K(row_exclusive_cnt),
                  K(share_cnt),
                  K(share_row_exclusive_cnt),
                  K(exclusive_cnt));
  }
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

int ObOBJLock::get_exist_lock_mode_cnt_without_curr_trans_(const ObTableLockMode &lock_mode,
                                                           const uint64_t lock_mode_cnt_in_same_trans[])
{
  int lock_mode_cnt_without_curr_trans = 0;
  if (OB_LIKELY(is_lock_mode_valid(lock_mode))) {
    int index = get_index_by_lock_mode(lock_mode);
    int total_lock_mode_cnt = OB_ISNULL(map_[index]) ? 0 : map_[index]->get_size();
    lock_mode_cnt_without_curr_trans = total_lock_mode_cnt - lock_mode_cnt_in_same_trans[index];
    switch (lock_mode) {
    case ROW_SHARE: {
      lock_mode_cnt_without_curr_trans += row_share_;
      break;
    }
    case ROW_EXCLUSIVE: {
      lock_mode_cnt_without_curr_trans += row_exclusive_;
      break;
    }
    case SHARE:
    case SHARE_ROW_EXCLUSIVE:
    case EXCLUSIVE: {
      // do nothing
      break;
    }
    default: {
      lock_mode_cnt_without_curr_trans = -1;
    }
    }
  } else {
    lock_mode_cnt_without_curr_trans = -1;
  }

  return lock_mode_cnt_without_curr_trans;
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
    const uint64_t lock_mode_cnt_in_same_trans[],
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
                                        lock_mode_cnt_in_same_trans,
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
        LOG_WARN("ObOBJLockMap::lock ", K(ret), K(param), K(lock_op), K(conflict_tx_set));
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
      } else if (OB_FAIL(obj_lock->unlock(lock_op, is_try_lock, expired_time, allocator_))) {
        LOG_WARN("get lock op list map failed.", K(ret), K(lock_op));
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
    // WRLockGuard guard(obj_lock->rwlock_);
    int abs_timeout_us = ObTimeUtility::current_time() + DEFAULT_RWLOCK_TIMEOUT_US;
    WRLockGuardWithTimeout guard(obj_lock->rwlock_, abs_timeout_us, ret);
    if (OB_FAIL(ret)) {
      if (OB_TIMEOUT == ret) {
        ret = OB_EAGAIN;
      }
      LOG_WARN("try get write lock of obj failed", K(ret), KPC(obj_lock), K(abs_timeout_us));
    } else {
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
    const uint64_t lock_mode_cnt_in_same_trans[],
    ObTxIDSet &conflict_tx_set,
    const int64_t expired_time,
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
                                                lock_mode_cnt_in_same_trans,
                                                conflict_tx_set,
                                                conflict_with_dml_lock,
                                                expired_time,
                                                allocator_,
                                                include_finish_tx,
                                                only_check_dml_lock))) {
    if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
      LOG_WARN("obj_lock check_allow_lock failed",
               K(ret), K(lock_op));
    }
  } else {
    LOG_DEBUG("succeed check allow lock.", K(lock_op));
  }
  if (OB_NOT_NULL(obj_lock)) {
    lock_map_.revert(obj_lock);
  }
  return ret;
}

int ObOBJLockMap::table_lock_split(const ObTabletID &src_tablet_id,
                                   const ObSArray<common::ObTabletID> &dst_tablet_ids,
                                   const transaction::ObTransID &trans_id,
                                   ObLockTableSplitLogCb &callback)
{
  int ret = OB_SUCCESS;
  ObLockID src_lock_id;
  ObOBJLock *src_obj_lock = nullptr;
  if (OB_FAIL(get_lock_id(src_tablet_id, src_lock_id))) {
    LOG_WARN("get_lock_id for src_tablet failed", K(ret), K(src_tablet_id));
  } else if (OB_FAIL(get_obj_lock_with_ref_(src_lock_id, src_obj_lock))) {
    LOG_WARN("get_obj_lock_with_ref_ for src_tablet failed", K(ret), K(src_lock_id), K(src_tablet_id));
  } else if (src_obj_lock->has_splitted()) {
    ret = OB_TABLE_LOCK_SPLIT_TWICE;
    LOG_WARN("src_tablet has spliitted", K(ret), K(src_lock_id), K(src_tablet_id));
  } else {
    ObSArray<ObLockID> dst_lock_ids;
    // create obj_lock for dst_tablets if it's not existed, to avoid no memory error during callback
    if (OB_FAIL(get_lock_id(dst_tablet_ids, dst_lock_ids))) {
      LOG_WARN("get_lock_id for dst_tablets failed", K(ret), K(dst_tablet_ids));
    } else if (OB_FAIL(add_split_epoch(dst_lock_ids, share::SCN::min_scn()))) {
      LOG_WARN("add split epoch for dst_lock_ids failed", K(ret), K(dst_lock_ids));
    } else if (OB_FAIL(src_obj_lock->table_lock_split(src_tablet_id, dst_tablet_ids, trans_id, callback))) {
      LOG_WARN("submit table lock split log failed", K(ret), K(src_tablet_id), K(dst_tablet_ids), K(trans_id));
      // submit table lock split failed, so we should reset split epoch for all dst_tablets,
      // to make sure that GC thread can collect them later.
      for (int64_t i = 0; i < dst_lock_ids.count(); i++) {
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(reset_split_epoch(dst_lock_ids[i]))) {
          LOG_ERROR("reset_split_epoch for dst_tablet failed", K(ret), K(tmp_ret), K(dst_lock_ids[i]));
        }
      }
    }
  }
  if (OB_NOT_NULL(src_obj_lock)) {
    lock_map_.revert(src_obj_lock);
  }
  return ret;
}

int ObOBJLockMap::reset_split_epoch(const ObLockID &lock_id)
{
  int ret = OB_SUCCESS;
  ObOBJLock *obj_lock = NULL;
  do {
    if (OB_FAIL(get_obj_lock_with_ref_(lock_id, obj_lock))) {
      LOG_WARN("get_or_create_obj_lock_with_ref failed", K(ret), K(lock_id));
    } else if (OB_FAIL(obj_lock->reset_split_epoch())) {
      LOG_WARN("reset_split_epoch failed", K(lock_id));
    }
    if (OB_NOT_NULL(obj_lock)) {
      lock_map_.revert(obj_lock);
    }
  } while (OB_EAGAIN == ret);

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
      // RDLockGuard guard(obj_lock->rwlock_);
      int64_t abs_timeout_us = ObTimeUtility::current_time() + DEFAULT_RWLOCK_TIMEOUT_US;
      RDLockGuardWithTimeout guard(obj_lock->rwlock_, abs_timeout_us, ret);
      if (OB_FAIL(ret)) {
        if (OB_TIMEOUT == ret) {
          ret = OB_EAGAIN;
        }
        LOG_WARN("try get read lock of obj failed", K(ret), KPC(obj_lock), K(abs_timeout_us));
      } else {
        is_empty = (obj_lock->size_without_lock() == 0
                    && (!obj_lock->max_split_epoch_.is_valid() || obj_lock->max_split_epoch_.is_max()));
      }
    }
    if (is_empty) {
      // WRLockGuard guard(obj_lock->rwlock_);
      int64_t abs_timeout_us = ObTimeUtility::current_time() + DEFAULT_RWLOCK_TIMEOUT_US;
      WRLockGuardWithTimeout guard(obj_lock->rwlock_, abs_timeout_us, ret);
      if (OB_FAIL(ret)) {
        if (OB_TIMEOUT == ret) {
          ret = OB_EAGAIN;
        }
        LOG_WARN("try get write lock of obj failed", K(ret), KPC(obj_lock), K(abs_timeout_us));
      } else if (obj_lock->size_without_lock() == 0
                 && (!obj_lock->max_split_epoch_.is_valid() || obj_lock->max_split_epoch_.is_max())
                 && !obj_lock->is_deleted()) {
        // lock and delete flag make sure no one insert a new op.
        // but maybe have deleted by another concurrent thread.
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
