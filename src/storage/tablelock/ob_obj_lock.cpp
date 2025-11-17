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
#include "storage/lock_wait_mgr/ob_lock_wait_mgr.h"
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
using namespace lockwaitmgr;

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

ObOBJLock::ObOBJLock(const ObLockID &lock_id) : lock_id_(lock_id), priority_queue_()
{
  is_deleted_ = false;
  row_share_ = 0;
  row_exclusive_ = 0;
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
  const bool is_two_phase_lock = param.is_two_phase_lock_;
  const int64_t trans_id_value = lock_op.create_trans_id_;
  bool enable_lock_priority = true;
  const ObTableLockPriority priority = param.lock_priority_;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  // if (!tenant_config.is_valid()) {
  //   ret = OB_ERR_UNEXPECTED;
  //   LOG_WARN("tenant config is invalid", K(ret), K(lock_op));
  // } else {
  //   enable_lock_priority = tenant_config->enable_lock_priority;
  // }
  // case 1, if it is two phase lock, must check first
  // case 2, if enable_lock_priority is true, must check first (for dml)
  // NOTE that we set enable_lock_priority to false to avoid unexpected cases
  // e.g., no lock operations but priority is not empty
  const bool need_check_first = is_two_phase_lock || enable_lock_priority;
  ObMemAttr attr(tenant_id, "ObTableLockOp");
  // 1. check lock conflict.
  // 2. record lock op.
  WRLockGuard guard(rwlock_);
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (is_deleted_) {
    ret = OB_EAGAIN;
  } else if (is_two_phase_lock) {
    ObObjLockPriorityTask *task = NULL;
    if (!priority_queue_.is_exist(ObObjLockPriorityTaskID(trans_id_value, lock_op.owner_id_),
          priority, task)) {
      ret = OB_TRANS_NEED_ROLLBACK;
      LOG_WARN("priority task not exist", KR(ret), K(lock_op));
    }
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (need_check_first
      && OB_FAIL(priority_queue_.check_first(
          priority, ObObjLockPriorityTaskID(trans_id_value, lock_op.owner_id_)))) {
    if (OB_EAGAIN == ret) {
      if (exist_self_(lock_op)) {
        ret = OB_SUCCESS;
        LOG_INFO("lock by self but not first in priority queue", KR(ret), K(lock_op));
      } else {
        ret = OB_TRY_LOCK_ROW_CONFLICT;
        if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
          LOG_WARN("not first in priority queue", KR(ret), K(lock_op), K_(priority_queue));
        }
      }
    } else {
      LOG_WARN("check first for priority failed", KR(ret), K(lock_op));
    }
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(check_allow_lock_(lock_op,
                                       lock_mode_cnt_in_same_trans,
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
  if (is_two_phase_lock && OB_SUCC(ret)) {
    // if acquire lock, try to remove task from priority queue
    const ObTableLockPrioArg arg(param.lock_priority_);
    if (OB_TMP_FAIL(remove_priority_task_(arg, lock_op, allocator))) {
      LOG_WARN("remove priority task failed", K(tmp_ret), K(lock_op));
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

int ObOBJLock::check_enable_lock_priority_(bool &enable_lock_priority)
{
  int ret = OB_SUCCESS;
  const static int64_t CACHE_REFRESH_INTERVAL = 1_s;
  RLOCAL_INIT(int64_t, last_check_timestamp, 0);
  RLOCAL_INIT(bool, last_result, 0);
  int64_t current_time = ObClockGenerator::getClock();
  bool tmp_enable_lock_priority = false;
  if (current_time - last_check_timestamp < CACHE_REFRESH_INTERVAL) {
  } else {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
    if (!tenant_config.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant config is invalid", K(ret));
    } else {
      tmp_enable_lock_priority = tenant_config->enable_lock_priority;
    }
    last_result = tmp_enable_lock_priority;
    last_check_timestamp = current_time;
  }
  enable_lock_priority = last_result;
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
  int tmp_ret = OB_SUCCESS;
  const bool is_two_phase_lock = param.is_two_phase_lock_;
  const int64_t trans_id_value = lock_op.create_trans_id_;
  const ObTableLockPriority priority = param.lock_priority_;
  bool enable_lock_priority = true;
  // if (OB_FAIL(check_enable_lock_priority_(enable_lock_priority))) {
  //   LOG_WARN("check enable lock priority failed", K(ret), K(lock_op));
  // }
  // case 1, if it is two phase lock, must check first
  // case 2, if enable_lock_priority is true, must check first (for dml)
  // NOTE that we set enable_lock_priority to false to avoid unexpected cases
  // e.g., no lock operations but priority is not empty
  const bool need_check_first = is_two_phase_lock || enable_lock_priority;
  {
    // lock first time
    RDLockGuard guard(rwlock_);
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (is_two_phase_lock) {
      ObObjLockPriorityTask *task = NULL;
      if (!priority_queue_.is_exist(ObObjLockPriorityTaskID(trans_id_value, lock_op.owner_id_),
            priority, task)) {
        ret = OB_TRANS_NEED_ROLLBACK;
        LOG_WARN("priority task not exist", KR(ret), K(lock_op));
      }
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (need_check_first
        && OB_FAIL(priority_queue_.check_first(
            priority, ObObjLockPriorityTaskID(trans_id_value, lock_op.owner_id_)))) {
      if (OB_EAGAIN == ret) {
        if (exist_self_(lock_op)) {
          ret = OB_SUCCESS;
          LOG_INFO("lock by self but not first in priority queue", KR(ret), K(lock_op),
              K_(priority_queue));
        } else {
          ret = OB_TRY_LOCK_ROW_CONFLICT;
          if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
            LOG_WARN("not first in priority queue", KR(ret), K(lock_op), K_(priority_queue));
          }
        }
      } else {
        LOG_WARN("check first for priority failed", KR(ret), K(lock_op));
      }
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(try_fast_lock_(lock_op,
                                      lock_mode_cnt_in_same_trans,
                                      conflict_tx_set))) {
      if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_EAGAIN != ret) {
        LOG_WARN("try fast lock failed", KR(ret), K(lock_op));
      }
    } else {
      LOG_DEBUG("succeed create lock ", K(lock_op));
    }
  }
  if (is_two_phase_lock && OB_SUCC(ret)) {
    // if acquire lock, try to remove task from priority queue
    // since update priority queue, need write lock guard
    const ObTableLockPrioArg arg(param.lock_priority_);
    WRLockGuard guard(rwlock_);
    if (OB_TMP_FAIL(remove_priority_task_(arg, lock_op, allocator))) {
      LOG_WARN("remove priority task failed", K(tmp_ret), K(lock_op));
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
  if (OB_UNLIKELY(!lock_op.is_valid())) {
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
  if (!priority_queue_.is_empty()) {
    priority_queue_.print();
  }
  return ret;
}

int ObOBJLock::add_priority_task(
    const ObLockParam &param,
    storage::ObStoreCtx &ctx,
    ObTableLockOp &lock_op,
    ObMalloc &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!lock_op.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(lock_op));
  } else {
    WRLockGuard guard(rwlock_);
    int64_t trans_id_value = lock_op.create_trans_id_.get_id();
    const ObTableLockPriority priority = param.lock_priority_;
    int64_t create_ts = 0;
    if (is_deleted_) {
      // if marked by delete, return eagain
      ret = OB_EAGAIN;
    } else if (OB_FAIL(priority_queue_.push(
            ObObjLockPriorityTaskID(trans_id_value, lock_op.owner_id_),
            priority, create_ts, allocator))) {
      LOG_WARN("push priority task failed", K(ret), K(lock_op));
    } else if (0 >= create_ts) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected create ts", K(ret), K(create_ts), K(lock_op));
    } else {
      lock_op.create_timestamp_ = create_ts;
      LOG_INFO("push priority task success", K(ret), K(lock_op));
      // if push success, generate first
      if (OB_FAIL(priority_queue_.generate_first())) {
        LOG_ERROR("generate first failed", K(ret), K(lock_op));
      }
    }
  }
  LOG_DEBUG("ObOBJLock::add_priority_task finish", K(ret), K(param), K(lock_op));
  return ret;
}

int ObOBJLock::prepare_priority_task(
    const ObTableLockPrioArg &arg,
    const ObTableLockOp &lock_op,
    ObMalloc &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!lock_op.is_valid())
      || OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg), K(lock_op));
  } else {
    WRLockGuard guard(rwlock_);
    // step 1, if priority queue uncertain or for leader,
    // clear priority queue and switch service for follower
    const bool need_switch = priority_queue_.need_switch_service_for_follower();
    if (need_switch) {
      priority_queue_.reset(allocator);
      priority_queue_.generate_dummy_first_for_follower();
    }
    // step 2, add priority task into specified position
    int64_t trans_id_value = lock_op.create_trans_id_.get_id();
    const int64_t create_ts = lock_op.create_timestamp_;
    if (is_deleted_) {
      // if marked by delete, return eagain
      ret = OB_EAGAIN;
    } else if (OB_FAIL(priority_queue_.add_with_create_ts(
            ObObjLockPriorityTaskID(trans_id_value, lock_op.owner_id_),
            arg.priority_,
            create_ts,
            allocator))) {
      LOG_WARN("add with create ts failed", K(ret), K(lock_op));
      if (OB_ENTRY_EXIST == ret) {
        ret = OB_SUCCESS;
      }
    } else {
      LOG_INFO("prepare priority task success", K(ret), K(lock_op), K_(priority_queue));
      // NOTE that do not generate first here
      // need generate first in leader switch phase
      // if (OB_FAIL(priority_queue_.generate_first())) {
      //   LOG_ERROR("generate first failed", K(ret), K(lock_op));
      // }
    }
  }
  LOG_DEBUG("ObOBJLock::prepare_priority_task finish", K(ret), K(arg), K(lock_op));
  return ret;
}

int ObOBJLock::remove_priority_task(
    const ObTableLockPrioArg &arg,
    const ObTableLockOp &lock_op,
    ObMalloc &allocator)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  LOG_DEBUG("ObOBJLock::remove_priority_task", K(lock_op));
  if (OB_UNLIKELY(!lock_op.is_valid())
      || OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg), K(lock_op));
  } else {
    WRLockGuard guard(rwlock_);
    if (OB_FAIL(remove_priority_task_(arg, lock_op, allocator))) {
      LOG_WARN("remove priority task failed", K(ret), K(lock_op));
    }
  }
  LOG_DEBUG("ObOBJLock::remove_priority_task finish", K(ret), K(arg), K(lock_op));
  return ret;
}

int ObOBJLock::remove_priority_task_(
    const ObTableLockPrioArg &arg,
    const ObTableLockOp &lock_op,
    ObMalloc &allocator)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t trans_id_value = lock_op.create_trans_id_.get_id();
  if (OB_FAIL(priority_queue_.remove(ObObjLockPriorityTaskID(trans_id_value, lock_op.owner_id_),
          arg.priority_, allocator))) {
    LOG_WARN("remove priority task failed", K(ret), K(lock_op));
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

bool ObOBJLock::exist_self_(const ObTableLockOp &self)
{
  bool exist = false;
  const ObTableLockMode self_lock_mode = self.lock_mode_;
  ObTableLockOpList *op_list = NULL;
  ObTableLockMode curr_mode = 0x0;
  for (int i = 0; i < TABLE_LOCK_MODE_COUNT && (!exist); i++) {
    op_list = map_[i];
    curr_mode = get_lock_mode_by_index(i);
    if (is_equal_or_greater_lock_mode(curr_mode, self_lock_mode)) {
      if (OB_ISNULL(op_list)) {
      } else {
        DLIST_FOREACH_NORET(curr, *op_list) {
          if (NULL == curr) {
          } else if (self.create_trans_id_ == curr->lock_op_.create_trans_id_) {
            // within the same trans
            exist = true;
          } else if (!self.owner_id_.is_default()
                     && self.owner_id_ == curr->lock_op_.owner_id_) {
            // with the same owner
            exist = true;
          } else {
          }
        } // end of DLIST_FOREACH_NORET
      }
    }
  }
  return exist;
}

bool ObOBJLock::exist_others_(
    const ObTableLockOp &myself,
    const ObTableLockOpList *op_list)
{
  bool exist = false;
  DLIST_FOREACH_NORET(curr, *op_list) {
    if (NULL == curr) {
    } else if (myself.create_trans_id_ == curr->lock_op_.create_trans_id_) {
      // within the same trans
    } else if (!myself.owner_id_.is_default()
               && myself.owner_id_ == curr->lock_op_.owner_id_) {
      // with the same owner
    } else {
      // there is a trans that is not me conflict with me
      exist = true;
    }
  }
  return exist;
}

bool ObOBJLock::exist_others_(
    const ObTableLockOp &myself,
    const int64_t lock_modes)
{
  bool exist = false;
  if (ROW_SHARE & lock_modes
      && row_share_ != 0) {
    // row share with no name
    exist = true;
  } else if (ROW_EXCLUSIVE & lock_modes
             && row_exclusive_ != 0) {
    // row exclusive with no name
    exist = true;
  } else {
    ObTableLockOpList *op_list = NULL;
    ObTableLockMode curr_mode = 0x0;
    for (int i = 0; i < TABLE_LOCK_MODE_COUNT && (!exist); i++) {
      op_list = map_[i];
      curr_mode = get_lock_mode_by_index(i);
      if (!(curr_mode & lock_modes)) {
      } else if (OB_ISNULL(op_list)) {
      } else if (exist_others_(myself, op_list)) {
        exist = true;
      }
    }
  }
  return exist;
}

int ObOBJLock::check_allow_lock_(
    const ObTableLockOp &lock_op,
    const uint64_t lock_mode_cnt_in_same_trans[],
    ObTxIDSet &conflict_tx_set,
    bool &conflict_with_dml_lock,
    const bool include_finish_tx,
    const bool only_check_dml_lock)
{
  int ret = OB_SUCCESS;
  int64_t conflict_modes = 0;
  ObTableLockMode curr_lock_mode = NO_LOCK;
  conflict_tx_set.reset();
  // 1. check if there is conflict trans.
  // 2. check if all the conflict trans with the same owner id with me.
  if (lock_op.need_record_lock_op() &&
      OB_FAIL(check_op_allow_lock_(lock_op))) {
    if (ret != OB_TRY_LOCK_ROW_CONFLICT &&
        ret != OB_OBJ_LOCK_EXIST) {
      LOG_WARN("check_op_allow_lock failed.", K(ret), K(lock_op));
    }
  } else if (OB_FAIL(get_other_trans_lock_mode_(lock_mode_cnt_in_same_trans,
                                                curr_lock_mode))) {
    LOG_WARN("meet unexpected error during get lock_mode without current trans", K(ret));
  } else if (!request_lock(curr_lock_mode,
                           lock_op.lock_mode_,
                           conflict_modes)
             && exist_others_(lock_op, conflict_modes)) {
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
                            lock_mode_cnt_in_same_trans,
                            conflict_tx_set,
                            conflict_with_dml_lock,
                            include_finish_tx,
                            only_check_dml_lock);
  }
  return ret;
}

int ObOBJLock::get_other_trans_lock_mode_(
    const uint64_t lock_mode_cnt_in_same_trans[],
    ObTableLockMode &lock_mode_without_curr_trans)
{
  int ret = OB_SUCCESS;
  lock_mode_without_curr_trans = 0x0;

  int row_share_not_in_curr_trans = get_other_trans_lock_mode_(ROW_SHARE, lock_mode_cnt_in_same_trans);
  int row_exclusive_not_in_curr_trans =
    get_other_trans_lock_mode_(ROW_EXCLUSIVE, lock_mode_cnt_in_same_trans);
  int share_not_in_curr_trans = get_other_trans_lock_mode_(SHARE, lock_mode_cnt_in_same_trans);
  int share_row_exclusive_not_in_curr_trans =
    get_other_trans_lock_mode_(SHARE_ROW_EXCLUSIVE, lock_mode_cnt_in_same_trans);
  int exclusive_not_in_curr_trans = get_other_trans_lock_mode_(EXCLUSIVE, lock_mode_cnt_in_same_trans);

  if (OB_UNLIKELY(row_share_not_in_curr_trans < 0 || row_exclusive_not_in_curr_trans < 0 || share_not_in_curr_trans < 0
                  || share_row_exclusive_not_in_curr_trans < 0 || exclusive_not_in_curr_trans < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("lock_mode count should not be negative",
              K(row_share_not_in_curr_trans),
              K(row_exclusive_not_in_curr_trans),
              K(share_not_in_curr_trans),
              K(share_row_exclusive_not_in_curr_trans),
              K(exclusive_not_in_curr_trans));
  } else if (OB_UNLIKELY(lock_mode_cnt_in_same_trans[get_index_by_lock_mode(ROW_SHARE)] > 0
                         && exclusive_not_in_curr_trans > 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("current trans has rs lock, others should not have x lock",
              K(lock_mode_cnt_in_same_trans),
              K(exclusive_not_in_curr_trans));
  } else if (OB_UNLIKELY(lock_mode_cnt_in_same_trans[get_index_by_lock_mode(ROW_EXCLUSIVE)] > 0
                         && (share_not_in_curr_trans > 0 || share_row_exclusive_not_in_curr_trans > 0
                             || exclusive_not_in_curr_trans))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("current trans has rx lock, others should not have s/srx/x lock",
              K(share_not_in_curr_trans),
              K(share_row_exclusive_not_in_curr_trans),
              K(exclusive_not_in_curr_trans));
  } else if (OB_UNLIKELY(lock_mode_cnt_in_same_trans[get_index_by_lock_mode(SHARE)] > 0
                         && (row_exclusive_not_in_curr_trans > 0 || share_row_exclusive_not_in_curr_trans > 0
                             || exclusive_not_in_curr_trans > 0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("current trans has s lock, others should not have rx/srx/x lock",
              K(row_exclusive_not_in_curr_trans),
              K(share_row_exclusive_not_in_curr_trans),
              K(exclusive_not_in_curr_trans));
  } else if (OB_UNLIKELY(lock_mode_cnt_in_same_trans[get_index_by_lock_mode(SHARE_ROW_EXCLUSIVE)] > 0
                         && (row_exclusive_not_in_curr_trans > 0 || share_not_in_curr_trans > 0
                             || share_row_exclusive_not_in_curr_trans > 0 || exclusive_not_in_curr_trans > 0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("current trans has srx lock, others should not have rx/s/srx/x lock",
              K(row_exclusive_not_in_curr_trans),
              K(share_not_in_curr_trans),
              K(share_row_exclusive_not_in_curr_trans),
              K(exclusive_not_in_curr_trans));
  } else if (OB_UNLIKELY(lock_mode_cnt_in_same_trans[get_index_by_lock_mode(EXCLUSIVE)] > 0
                         && (row_share_not_in_curr_trans > 0 || row_exclusive_not_in_curr_trans > 0
                             || share_not_in_curr_trans > 0 || share_row_exclusive_not_in_curr_trans > 0
                             || exclusive_not_in_curr_trans > 0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("current trans has x lock, others should not have any locks",
              K(row_share_not_in_curr_trans),
              K(row_exclusive_not_in_curr_trans),
              K(share_not_in_curr_trans),
              K(share_row_exclusive_not_in_curr_trans),
              K(exclusive_not_in_curr_trans));
  } else {
    lock_mode_without_curr_trans |= (row_share_not_in_curr_trans == 0 ? 0 : ROW_SHARE);
    lock_mode_without_curr_trans |= (row_exclusive_not_in_curr_trans == 0 ? 0 : ROW_EXCLUSIVE);
    lock_mode_without_curr_trans |= (share_not_in_curr_trans == 0 ? 0 : SHARE);
    lock_mode_without_curr_trans |= (share_row_exclusive_not_in_curr_trans == 0 ? 0 : SHARE_ROW_EXCLUSIVE);
    lock_mode_without_curr_trans |= (exclusive_not_in_curr_trans == 0 ? 0 : EXCLUSIVE);
  }

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
  priority_queue_.reset(allocator);
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
  priority_queue_.print();
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
  if (!priority_queue_.is_empty()) {
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
          if (curr->lock_op_.op_type_ == OUT_TRANS_LOCK ||
              curr->lock_op_.op_type_ == IN_TRANS_COMMON_LOCK) {
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
      if (curr->lock_op_.create_trans_id_ == lock_op.create_trans_id_
          && (curr->lock_op_.op_type_ == IN_TRANS_DML_LOCK
              || curr->lock_op_.op_type_ == IN_TRANS_COMMON_LOCK)) {
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
        } else if (curr->lock_op_.op_type_ == IN_TRANS_COMMON_LOCK) {
          if (curr->lock_op_.create_trans_id_ == lock_op.create_trans_id_) {
            // continue
          } else {
            ret = OB_TRY_LOCK_ROW_CONFLICT;
            need_break = true;
          }
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

int ObOBJLock::get_other_trans_lock_mode_(
    const ObTableLockMode &lock_mode,
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

int ObOBJLock::switch_to_leader(ObMalloc &allocator)
{
  int ret = OB_SUCCESS;
  WRLockGuard guard(rwlock_);
  if (!priority_queue_.is_empty()) {
    // step 1, check and clear
    // if not empty, check priority queue
    // if not from follower, clear priority queue
    const bool need_clear = priority_queue_.need_clear_for_leader();
    if (need_clear) {
      LOG_INFO("clear priority queue for new leader", K(ret), K_(priority_queue));
      priority_queue_.reset(allocator);
    }
    // step 2, generate first
    if (OB_FAIL(priority_queue_.generate_first())) {
      LOG_ERROR("generate first failed", K(ret), K_(priority_queue));
    } else {
      LOG_INFO("generate first success", K(ret), K_(priority_queue));
    }
  }
  return ret;
}

int ObOBJLock::switch_to_follower(ObMalloc &allocator)
{
  int ret = OB_SUCCESS;
  WRLockGuard guard(rwlock_);
  const int64_t priority_queue_size = priority_queue_.get_size();
  if (0 != priority_queue_size) {
    LOG_INFO("switch to follower", K_(priority_queue));
    priority_queue_.reset(allocator);
  }
  priority_queue_.generate_dummy_first_for_follower();
  return ret;
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
    const uint64_t lock_mode_cnt_in_same_trans[],
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
                                                lock_mode_cnt_in_same_trans,
                                                conflict_tx_set,
                                                conflict_with_dml_lock,
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

int ObOBJLockMap::add_priority_task(
    const ObLockParam &param,
    storage::ObStoreCtx &ctx,
    ObTableLockOp &lock_op)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("ObOBJLockMap::add_priority_task", K(param), K(lock_op));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObOBJLockMap is not inited", K(ret));
  } else if (OB_UNLIKELY(!lock_op.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument.", K(ret), K(param), K(lock_op));
  } else {
    do {
      ObOBJLock *obj_lock = NULL;
      if (OB_FAIL(get_or_create_obj_lock_with_ref_(lock_op.lock_id_,
                                                   obj_lock))) {
        LOG_WARN("get or create owner map failed", K(ret), K(lock_op));
      } else if (NULL == obj_lock) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("unexpected obj_lock", K(ret), K(param), K(lock_op));
      } else if (OB_FAIL(obj_lock->add_priority_task(param,
                                                     ctx,
                                                     lock_op,
                                                     allocator_))) {
        if (ret != OB_EAGAIN) {
          LOG_WARN("add priority task failed", K(ret), K(lock_op));
        }
      } else {
        LOG_DEBUG("add priority task success", K(lock_op));
      }
      if (OB_NOT_NULL(obj_lock)) {
        lock_map_.revert(obj_lock);
      }
      if (OB_FAIL(ret) && TC_REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
        LOG_WARN("ObOBJLockMap::add_priority_task", K(ret), K(param), K(lock_op));
      }
      // retry if the table lock list map is delete right now by others.
    } while (ret == OB_EAGAIN);
  }
  LOG_DEBUG("ObOBJLockMap::add_priority_task finish", K(ret), K(param), K(lock_op));
  return ret;
}

int ObOBJLockMap::prepare_priority_task(
    const ObTableLockPrioArg &arg,
    const ObTableLockOp &lock_op)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("ObOBJLockMap::prepare_priority_task", K(lock_op));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObOBJLockMap is not inited", K(ret));
  } else if (OB_UNLIKELY(!lock_op.is_valid())
      || OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument.", K(ret), K(arg), K(lock_op));
  } else {
    do {
      ObOBJLock *obj_lock = NULL;
      if (OB_FAIL(get_or_create_obj_lock_with_ref_(lock_op.lock_id_,
                                                   obj_lock))) {
        LOG_WARN("get or create owner map failed", K(ret), K(lock_op));
      } else if (NULL == obj_lock) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("unexpected obj_lock", K(ret), K(lock_op));
      } else if (OB_FAIL(obj_lock->prepare_priority_task(arg,
                                                         lock_op,
                                                         allocator_))) {
        if (ret != OB_EAGAIN) {
          LOG_WARN("prepare priority task failed", K(ret), K(lock_op));
        }
      } else {
        LOG_DEBUG("prepare priority task success", K(lock_op));
      }
      if (OB_NOT_NULL(obj_lock)) {
        lock_map_.revert(obj_lock);
      }
      if (OB_FAIL(ret) && TC_REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
        LOG_WARN("ObOBJLockMap::prepare_priority_task", K(ret), K(lock_op));
      }
      // retry if the table lock list map is delete right now by others.
    } while (ret == OB_EAGAIN);
  }
  LOG_DEBUG("ObOBJLockMap::prepare_priority_task finish", K(ret), K(lock_op));
  return ret;
}

int ObOBJLockMap::remove_priority_task(
    const ObTableLockPrioArg &arg,
    const ObTableLockOp &lock_op)
{
  int ret = OB_SUCCESS;
  ObOBJLock *obj_lock = NULL;
  ObTableLockOpList *op_list = NULL;
  LOG_DEBUG("ObOBJLockMap::remove_priority_task", K(lock_op));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObOBJLockMap is not inited", K(ret));
  } else if (OB_UNLIKELY(!lock_op.is_valid())
      || OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument.", K(ret), K(arg), K(lock_op));
  } else if (OB_FAIL(lock_map_.get(lock_op.lock_id_, obj_lock))) {
    if (ret == OB_ENTRY_NOT_EXIST) {
      ret = OB_SUCCESS;
    } else {
      // should only have not exist err code.
      LOG_ERROR("get lock op list map failed", K(ret),
                K(lock_op.lock_id_));
    }
  } else if (NULL == obj_lock) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected obj_lock", K(ret), K(lock_op));
  } else {
    if (OB_FAIL(obj_lock->remove_priority_task(arg, lock_op, allocator_))) {
      LOG_WARN("remove priority task failed", K(ret), K(lock_op));
    }
  }
  if (NULL != obj_lock) {
    lock_map_.revert(obj_lock);
  }
  LOG_DEBUG("ObOBJLockMap::remove_priority_task finish", K(ret));
  return ret;
}

int ObOBJLockMap::switch_to_leader()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLockIDIterator lock_id_iter;
  ObLockID lock_id;
  ObOBJLock *obj_lock = nullptr;
  if (OB_FAIL(get_lock_id_iter(lock_id_iter))) {
    LOG_WARN("get lock id iterator failed", K(ret));
  } else {
    do {
      if (OB_FAIL(lock_id_iter.get_next(lock_id))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next obj lock", K(ret));
        }
      } else if (OB_FAIL(get_obj_lock_with_ref_(lock_id, obj_lock))) {
        if (ret != OB_OBJ_LOCK_NOT_EXIST) {
          LOG_WARN("get obj lock failed", K(ret), K(lock_id));
        } else {
          LOG_WARN("obj lock has been deleted", K(ret), K(lock_id));
          ret = OB_SUCCESS;
        }
      } else {
        if (OB_FAIL(obj_lock->switch_to_leader(allocator_))) {
          LOG_ERROR("switch to leader failed", K(ret), K(tmp_ret), K(lock_id));
        }
        drop_obj_lock_if_empty_(lock_id, obj_lock);
        if (OB_NOT_NULL(obj_lock)) {
          lock_map_.revert(obj_lock);
        }
      }
    } while (OB_SUCC(ret));
  }
  ret = OB_ITER_END == ret ? OB_SUCCESS : ret;
  LOG_INFO("ObOBJLockMap::switch_to_leader", K(ret));
  return ret;
}

int ObOBJLockMap::switch_to_follower()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLockIDIterator lock_id_iter;
  ObLockID lock_id;
  ObOBJLock *obj_lock = nullptr;
  if (OB_FAIL(get_lock_id_iter(lock_id_iter))) {
    LOG_WARN("get lock id iterator failed", K(ret));
  } else {
    do {
      if (OB_FAIL(lock_id_iter.get_next(lock_id))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next obj lock", K(ret));
        }
      } else if (OB_FAIL(get_obj_lock_with_ref_(lock_id, obj_lock))) {
        if (ret != OB_OBJ_LOCK_NOT_EXIST) {
          LOG_WARN("get obj lock failed", K(ret), K(lock_id));
        } else {
          LOG_WARN("obj lock has been deleted", K(ret), K(lock_id));
          ret = OB_SUCCESS;
        }
      } else {
        if (OB_TMP_FAIL(obj_lock->switch_to_follower(allocator_))) {
          LOG_WARN("compact table lock failed", K(ret), K(tmp_ret), K(lock_id));
        }
        drop_obj_lock_if_empty_(lock_id, obj_lock);
        if (OB_NOT_NULL(obj_lock)) {
          lock_map_.revert(obj_lock);
        }
      }
    } while (OB_SUCC(ret));
  }
  ret = OB_ITER_END == ret ? OB_SUCCESS : ret;
  LOG_INFO("ObOBJLockMap::switch_to_follower", K(ret));
  return ret;
}

bool ObObjLockPriorityTask::is_obsolete(const int64_t timeout_us) const
{
  bool ret_bool = false;
  if (timeout_us < ObClockGenerator::getClock() - create_ts_) {
    ret_bool = true;
  }
  return ret_bool;
}

bool ObObjLockPriorityTask::is_target(
    const ObObjLockPriorityTaskID &id) const
{
  bool ret_bool = false;
  if (id.trans_id_value_ == id_) {
    // within the same trans
    ret_bool = true;
  } else if (!id.owner_id_.is_default()
             && id.owner_id_ == owner_id_) {
    // with the same owner
    ret_bool = true;
  }
  return ret_bool;
}

void ObObjLockPriorityQueue::reset(ObMalloc &allocator)
{
  DLIST_FOREACH_REMOVESAFE_NORET(task, high1_list_) {
    if (NULL != task) {
      (void)high1_list_.remove(task);
      task->~ObObjLockPriorityTask();
      allocator.free(task);
    }
  }
  DLIST_FOREACH_REMOVESAFE_NORET(task, normal_list_) {
    if (NULL != task) {
      (void)normal_list_.remove(task);
      task->~ObObjLockPriorityTask();
      allocator.free(task);
    }
  }
  high1_list_.~ObObjLockPriorityTaskList();
  normal_list_.~ObObjLockPriorityTaskList();
  current_id_ = -1;
  current_owner_id_.reset();
  current_priority_ = ObTableLockPriority::INVALID;
  total_size_ = 0;
  last_create_ts_ = -1;
}

// if exist, return entry_exist
int ObObjLockPriorityQueue::push(
    const ObObjLockPriorityTaskID &id,
    const ObTableLockPriority priority,
    int64_t &create_ts,
    ObMalloc &allocator)
{
  int ret = OB_SUCCESS;
  void *ptr = NULL;
  ObObjLockPriorityTask *task = NULL;
  uint64_t tenant_id = MTL_ID();
  ObMemAttr attr(tenant_id, "ObObjLockPrioT");
  // step 1, check duplicate
  ObObjLockPriorityTask *exist_task = NULL;
  if (is_exist(id, priority, exist_task)) {
    // if exist, return entry_exist
    ret = OB_ENTRY_EXIST;
    LOG_INFO("task has existed", K(ret), K(id));
  }
  // step 2, add into corresponding list
  if (OB_ENTRY_EXIST == ret) {
    // do nothing
  } else if (ObObjLockPriorityQueue::MAX_QUEUE_SIZE <= total_size_) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("priority queue is full", K(ret), K(*this));
  } else {
    int64_t current_ts = ObTimeUtility::current_time();
    if (current_ts > last_create_ts_) {
      last_create_ts_ = current_ts;
    } else {
      current_ts = ++last_create_ts_;
    }
    if (OB_ISNULL(ptr = allocator.alloc(sizeof(ObObjLockPriorityTask), attr))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alllocate ObObjLockPriorityTask failed", K(ret), K(*this));
    } else if (FALSE_IT(task = new(ptr) ObObjLockPriorityTask(id.trans_id_value_,
            id.owner_id_, current_ts, priority))) {
      // do nothing
    } else {
      switch (priority) {
      case ObTableLockPriority::HIGH2:
      case ObTableLockPriority::HIGH1: {
        if (false == high1_list_.add_last(task)) {
          // since add_last return false, set ret to unexpected
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("push task failed", K(ret), K(id), K(*this));
        } else {
          ATOMIC_INC(&total_size_);
          LOG_INFO("push task into high priority queue success", K(ret), K(id),
              K(current_ts), K(*this));
        }
        break;
      }
      case ObTableLockPriority::NORMAL: {
        if (false == normal_list_.add_last(task)) {
          // since add_last return false, set ret to unexpected
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("push task failed", K(ret), K(id), K(*this));
        } else {
          ATOMIC_INC(&total_size_);
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected priority", K(ret), K(id), K(*this));
      }
      }
    }
    if (OB_SUCC(ret)) {
      create_ts = current_ts;
    }
  }
  if (OB_FAIL(ret) && NULL != task) {
    task->~ObObjLockPriorityTask();
    allocator.free(task);
    task = NULL;
  }
  LOG_DEBUG("ObObjLockPriorityQueue::push", K(ret), K(id), K(*this));
  return ret;
}

int ObObjLockPriorityQueue::add_with_create_ts(
    const ObObjLockPriorityTaskID &id,
    const ObTableLockPriority priority,
    const int64_t create_ts,
    ObMalloc &allocator)
{
  int ret = OB_SUCCESS;
  // bool is_exist = false;
  void *ptr = NULL;
  ObObjLockPriorityTask *task = NULL;
  ObObjLockPriorityTask *position_task = NULL;
  uint64_t tenant_id = MTL_ID();
  ObMemAttr attr(tenant_id, "ObObjLockPrioT");
  // step 1, check duplicate

  ObObjLockPriorityTaskList *task_list = NULL;
  switch (priority) {
  case ObTableLockPriority::HIGH2:
  case ObTableLockPriority::HIGH1: {
    task_list = &high1_list_;
    break;
  }
  case ObTableLockPriority::NORMAL: {
    task_list = &normal_list_;
    break;
  }
  default: {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected priority", K(ret), K(*this));
  }
  }
  if (NULL != task_list) {
    DLIST_FOREACH_X(i_task, *task_list, OB_SUCC(ret)) {
      if (i_task->is_target(id)) {
        ret = OB_ENTRY_EXIST;
        // is_exist = true;
        position_task = i_task;
        break;
      }
      if (create_ts < i_task->get_create_ts()) {
        position_task = i_task;
        break;
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (ObObjLockPriorityQueue::MAX_QUEUE_SIZE <= total_size_) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("priority queue is full", K(ret), K(*this));
    } else if (OB_ISNULL(ptr = allocator.alloc(sizeof(ObObjLockPriorityTask), attr))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alllocate ObObjLockPriorityTask failed", K(ret), K(*this));
    } else if (FALSE_IT(task = new(ptr) ObObjLockPriorityTask(id.trans_id_value_,
            id.owner_id_, create_ts, priority))) {
      // do nothing
    } else {
      if (NULL == position_task) {
        // add last
        if (false == task_list->add_last(task)) {
          // since add_last return false, set ret to unexpected
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("push task failed", K(ret), K(id), K(*this));
        } else {
          ATOMIC_INC(&total_size_);
        }
      } else {
        // add before position
        if (false == task_list->add_before(position_task, task)) {
          // since add_last return false, set ret to unexpected
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("push task failed", K(ret), K(id), K(*this));
        } else {
          ATOMIC_INC(&total_size_);
        }
      }
      if (OB_SUCC(ret)) {
        if (create_ts > last_create_ts_) {
          last_create_ts_ = create_ts;
        }
      }
    }
    if (OB_FAIL(ret) && NULL != task) {
      task->~ObObjLockPriorityTask();
      allocator.free(task);
      task = NULL;
    }
  }
  LOG_DEBUG("ObObjLockPriorityQueue::add_with_create_ts", K(ret), K(id), K(*this));
  return ret;
}

// if first, return success
// otherwise, return eagain
int ObObjLockPriorityQueue::wait_for_first(
    const ObTableLockPriority priority,
    const ObObjLockPriorityTaskID &id,
    const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  bool need_retry = true;
  static const int64_t WAIT_US = 10000;  // 10ms
  int64_t start_ts = ObClockGenerator::getClock();
  do {
    // step 1, check first
    if (is_first_(priority, id)) {
      // in this case, return success
      need_retry = false;
    } else if (0 >= timeout_us) {
      need_retry = false;
      ret = OB_EAGAIN;
      // reutrn eagain
    }
    // step 2, sleep for retry
    if (need_retry) {
      // in this case, return success
      ob_usleep(WAIT_US);
    }
    // step 3, check timeout
    if (need_retry) {
      if (ObClockGenerator::getClock() - start_ts > timeout_us) {
        need_retry = false;
        ret = OB_EAGAIN;
        LOG_WARN("wait for first timeout", K(ret), K(id), K(*this));
      }
    }
  } while (need_retry);
  LOG_DEBUG("ObObjLockPriorityQueue::wait_for_first", K(ret), K(id), K(*this));
  return ret;
}

// only serve for leader
// if queue is empty, set current id to 0 and current prio to invalid
// CASE ONE, current task from highest priority queue
// CASE TWO, current task from non-highest priority queue
int ObObjLockPriorityQueue::generate_first()
{
  int ret = OB_SUCCESS;
  ObObjLockPriorityTask *task = NULL;
  ObTableLockPriority priority = ObTableLockPriority::INVALID;
  if (!high1_list_.is_empty()) {
    task = high1_list_.get_first();
    if (NULL == task) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get first node failed", K(ret), K(*this));
    } else {
      priority = ObTableLockPriority::HIGH1;
    }
  } else if (!normal_list_.is_empty()) {
    task = normal_list_.get_first();
    if (NULL == task) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get first node failed", K(ret), K(*this));
    } else {
      priority = ObTableLockPriority::NORMAL;
    }
  } else {
    // if empty, set current id to 0
    current_id_ = 0;
    current_priority_ = ObTableLockPriority::INVALID;
    current_owner_id_.reset();
    LOG_INFO("generate first for empty queue", K(*this));
  }
  if (OB_FAIL(ret)) {
  } else if (NULL == task) {
    // do nothing
  } else {
    current_priority_ = priority;
    current_id_ = task->get_trans_id();
    current_owner_id_ = task->get_owner_id();
    LOG_INFO("generate first success", K(ret), K(*task), K(*this));
  }
  return ret;
}

bool ObObjLockPriorityQueue::is_exist(
    const ObObjLockPriorityTaskID &id,
    const ObTableLockPriority priority,
    ObObjLockPriorityTask *&target_task)
{
  int ret = OB_SUCCESS;
  bool ret_bool = false;
  ObObjLockPriorityTaskList *task_list = NULL;
  switch (priority) {
  case ObTableLockPriority::HIGH2:
  case ObTableLockPriority::HIGH1: {
    task_list = &high1_list_;
    break;
  }
  case ObTableLockPriority::NORMAL: {
    task_list = &normal_list_;
    break;
  }
  default: {
  }
  }
  if (NULL != task_list) {
    DLIST_FOREACH_X(task, *task_list, OB_SUCC(ret)) {
      if (task->is_target(id)) {
        ret = OB_ENTRY_EXIST;
        ret_bool = true;
        target_task = task;
      }
    }
  }
  LOG_DEBUG("ObObjLockPriorityQueue::is_exist", K(ret), K(id), K(priority), K(*this));
  return ret_bool;
}

// if not exist, return success
int ObObjLockPriorityQueue::remove(
    const ObObjLockPriorityTaskID &id,
    const ObTableLockPriority priority,
    ObMalloc &allocator)
{
  int ret = OB_SUCCESS;
  ObObjLockPriorityTaskList *task_list = NULL;
  switch (priority) {
  case ObTableLockPriority::HIGH2:
  case ObTableLockPriority::HIGH1: {
    task_list = &high1_list_;
    break;
  }
  case ObTableLockPriority::NORMAL: {
    task_list = &normal_list_;
    break;
  }
  default: {
  }
  }
  // step 1, remove from list
  if (NULL != task_list) {
    DLIST_FOREACH_REMOVESAFE_X(task, *task_list, OB_SUCC(ret)) {
      if (task->is_target(id)) {
        ret = OB_ENTRY_EXIST;
        LOG_INFO("task exist, need remove", K(ret), K(*task), K(*this));
      }
      if (OB_ENTRY_EXIST == ret) {
        (void)task_list->remove(task);
        task->~ObObjLockPriorityTask();
        allocator.free(task);
      }
    }
  }
  // step 2, rewrite ret
  if (OB_ENTRY_EXIST == ret) {
    ATOMIC_DEC(&total_size_);
    ret = OB_SUCCESS;
  }
  // step 3, reset current info if first
  if (is_first_(priority, id)) {
    // if first, need generate first
    // NOTE that serving for leader in this case
    generate_first();
  }
  LOG_DEBUG("ObObjLockPriorityQueue::remove", K(ret), K(id), K(priority), K(*this));
  return ret;
}

// if empty, return success
int ObObjLockPriorityQueue::check_first(
    const ObTableLockPriority priority,
    const ObObjLockPriorityTaskID &id)
{
  int ret = OB_SUCCESS;
  if (is_empty()) {
    // return success
  } else {
    const int64_t timeout_us = 0;
    if (OB_FAIL(wait_for_first(priority, id, timeout_us))) {
      if (OB_EAGAIN != ret) {
        LOG_WARN("wait for first failed", K(ret), K(*this));
      }
    }
  }
  LOG_DEBUG("ObObjLockPriorityQueue::check_first", K(ret), K(id), K(*this));
  return ret;
}

bool ObObjLockPriorityQueue::need_clear_for_leader() const
{
  bool ret_bool = true;
  if (INT64_MAX == current_id_ && ObTableLockPriority::LOW == current_priority_) {
    ret_bool = false;
  }
  return ret_bool;
}

bool ObObjLockPriorityQueue::need_switch_service_for_follower() const
{
  bool ret_bool = true;
  if (INT64_MAX == current_id_ && ObTableLockPriority::LOW == current_priority_) {
    ret_bool = false;
  }
  return ret_bool;
}

void ObObjLockPriorityQueue::generate_dummy_first_for_follower()
{
  current_id_ = INT64_MAX;
  current_priority_ = ObTableLockPriority::LOW;
  current_owner_id_.reset();
}

void ObObjLockPriorityQueue::print() const
{
  LOG_INFO("obj lock priority queue", K(*this));
}

bool ObObjLockPriorityQueue::is_first_(
    const ObTableLockPriority priority,
    const ObObjLockPriorityTaskID &id) const
{
  bool ret_bool = false;
  if (priority == current_priority_) {
    if (id.trans_id_value_ == current_id_) {
      // within the same trans
      ret_bool = true;
    } else if (!id.owner_id_.is_default()
               && id.owner_id_ == current_owner_id_) {
      // with the same owner
      ret_bool = true;
    }
  }
  return ret_bool;
}

}
}
}
