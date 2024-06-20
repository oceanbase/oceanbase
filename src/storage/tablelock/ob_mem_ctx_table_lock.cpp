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
#include "storage/tablelock/ob_mem_ctx_table_lock.h"
#include "storage/tablelock/ob_lock_memtable.h"
#include "storage/memtable/ob_memtable_context.h"
#include "storage/tx/ob_trans_ctx_mgr_v4.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace memtable;
using namespace storage;
namespace transaction
{
namespace tablelock
{

int ObMemCtxLockOpLinkNode::init(const ObTableLockOp &op_info)
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

int ObLockMemCtx::init(ObLSTxCtxMgr *ls_tx_ctx_mgr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ls_tx_ctx_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KP(ls_tx_ctx_mgr));
  } else if (OB_FAIL(ls_tx_ctx_mgr->get_lock_memtable(memtable_handle_))) {
    TRANS_LOG(WARN, "get lock_memtable fail", KR(ret));
  } else {
    // do nothing
  }
  return ret;
}

// for mintest
int ObLockMemCtx::init(ObTableHandleV2 &handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(handle));
  } else {
    memtable_handle_ = handle;
  }
  return ret;
}

int ObLockMemCtx::get_lock_memtable(ObLockMemtable *&memtable)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!memtable_handle_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("memtable handle is empty", K(memtable_handle_));
  } else if (OB_FAIL(memtable_handle_.get_lock_memtable(memtable))) {
    LOG_ERROR("get lock memtable failed", K(ret));
  }
  return ret;
}

void ObLockMemCtx::reset()
{
  DLIST_FOREACH_REMOVESAFE_NORET(curr, lock_list_) {
    lock_list_.remove(curr);
    curr->~ObMemCtxLockOpLinkNode();
    free_lock_link_node_(curr);
  }
  is_killed_ = false;
  max_durable_scn_.reset();
  memtable_handle_.reset();
}

int ObLockMemCtx::rollback_table_lock_(const ObTxSEQ to_seq_no, const ObTxSEQ from_seq_no)
{
  int ret = OB_SUCCESS;
  ObLockMemtable *memtable = nullptr;
  if (OB_FAIL(memtable_handle_.get_lock_memtable(memtable))) {
    LOG_ERROR("get lock memtable failed", K(ret));
  } else {
    DLIST_FOREACH_REMOVESAFE_NORET(curr, lock_list_) {
      if (curr->lock_op_.lock_seq_no_ <= to_seq_no ||
          curr->lock_op_.lock_seq_no_ > from_seq_no) {
        // out of scope, do nothing
      } else if (to_seq_no.get_branch() !=0 &&
                 curr->lock_op_.lock_seq_no_.get_branch() != to_seq_no.get_branch()) {
        // branch missmatch
      } else {
        memtable->remove_lock_record(curr->lock_op_);
        (void)lock_list_.remove(curr);
        curr->~ObMemCtxLockOpLinkNode();
        free_lock_link_node_(curr);
      }
    }
  }
  return ret;
}

void ObLockMemCtx::abort_table_lock_()
{
  int ret = OB_SUCCESS;
  ObLockMemtable *memtable = nullptr;
  if (OB_FAIL(memtable_handle_.get_lock_memtable(memtable))) {
    LOG_ERROR("get lock memtable failed", K(ret));
  } else {
    DLIST_FOREACH_REMOVESAFE_NORET(curr, lock_list_) {
      memtable->remove_lock_record(curr->lock_op_);
      (void)lock_list_.remove(curr);
      curr->~ObMemCtxLockOpLinkNode();
      free_lock_link_node_(curr);
    }
  }
}

int ObLockMemCtx::commit_table_lock_(const SCN &commit_version, const SCN &commit_scn)
{
  int ret = OB_SUCCESS;
  ObLockMemtable *memtable = nullptr;
  if (OB_FAIL(memtable_handle_.get_lock_memtable(memtable))) {
    LOG_ERROR("get lock memtable failed", K(ret));
  } else {
    DLIST_FOREACH_REMOVESAFE(curr, lock_list_) {
      switch (curr->lock_op_.op_type_) {
      case IN_TRANS_DML_LOCK:
      case IN_TRANS_COMMON_LOCK: {
        // remove the lock op.
        memtable->remove_lock_record(curr->lock_op_);
        break;
      }
      case OUT_TRANS_LOCK:
      case OUT_TRANS_UNLOCK: {
        // update lock op status to LOCK_OP_COMPLETE
        if (OB_FAIL(memtable->
                    update_lock_status(curr->lock_op_,
                                       commit_version,
                                       commit_scn,
                                       LOCK_OP_COMPLETE))) {
          LOG_WARN("update lock record status failed.", K(ret),
                   K(curr->lock_op_));
        }
        break;
      }
      default: {
      } // default
      } // switch
      (void)lock_list_.remove(curr);
      curr->~ObMemCtxLockOpLinkNode();
      free_lock_link_node_(curr);
    }
  }
  return ret;
}

int ObLockMemCtx::rollback_table_lock(const ObTxSEQ to_seq_no, const ObTxSEQ from_seq_no)
{
  int ret = OB_SUCCESS;
  if (lock_list_.is_empty()) {
    // there is no table lock left, do nothing
  } else if (OB_UNLIKELY(!memtable_handle_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("memtable should not be null", K(ret), K(memtable_handle_));
  } else {
    WRLockGuard guard(list_rwlock_);
    if (OB_FAIL(rollback_table_lock_(to_seq_no, from_seq_no))) {
      LOG_WARN("rollback table lock failed", K(ret), K(to_seq_no), K(from_seq_no));
    }
  }
  LOG_DEBUG("ObLockMemCtx::rollback_table_lock ", K(ret), K(to_seq_no), K(from_seq_no));
  return ret;
}

int ObLockMemCtx::sync_log_succ(const share::SCN &scn)
{
  int ret = OB_SUCCESS;
  // NOTE: the callback of sync log succ is ensured in asc order of SCN by TxCtx
  if (max_durable_scn_ > scn) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("sync log succ is backoff", K(max_durable_scn_), K(scn));
  } else {
    max_durable_scn_ = scn;
  }
  return ret;
}

int ObLockMemCtx::get_table_lock_store_info(ObTableLockInfo &table_lock_info)
{
  int ret = OB_SUCCESS;
  RDLockGuard guard(list_rwlock_);
  DLIST_FOREACH(curr, lock_list_) {
    if (OB_UNLIKELY(!curr->is_valid())) {
      // no need dump to avoid been restored even if rollback
      LOG_WARN("the table lock op no should not dump", K(curr->lock_op_));
    } else if (OB_FAIL(table_lock_info.table_lock_ops_.push_back(curr->lock_op_))) {
      LOG_WARN("fail to push back table_lock store info", K(ret));
      break;
    }
  }
  table_lock_info.max_durable_scn_ = max_durable_scn_;
  return ret;
}

int ObLockMemCtx::get_table_lock_for_transfer(ObTableLockInfo &table_lock_info, const ObIArray<ObTabletID> &tablet_list)
{
  int ret = OB_SUCCESS;
  RDLockGuard guard(list_rwlock_);
  DLIST_FOREACH(curr, lock_list_) {
    if (OB_UNLIKELY(!curr->is_valid())) {
      // no need dump to avoid been restored even if rollback
      LOG_WARN("the table lock op no should not dump", K(curr->lock_op_));
    } else {
      bool is_hit = false;
      for (int64_t idx = 0; OB_SUCC(ret) && idx < tablet_list.count(); idx++) {
        if (curr->lock_op_.is_tablet_lock(tablet_list.at(idx))) {
          is_hit = true;
          break;
        }
      }
      if (OB_FAIL(ret)) {
      } else if (!is_hit) {
      } else if (OB_FAIL(table_lock_info.table_lock_ops_.push_back(curr->lock_op_))) {
        LOG_WARN("fail to push back table_lock store info", K(ret));
        break;
      }
    }
  }
  table_lock_info.max_durable_scn_ = max_durable_scn_;
  return ret;
}

int ObLockMemCtx::clear_table_lock(
    const bool is_committed,
    const SCN &commit_version,
    const SCN &commit_scn)
{
  int ret = OB_SUCCESS;
  if (lock_list_.is_empty()) {
    // there is no table lock left, do nothing
  } else if (OB_UNLIKELY(!memtable_handle_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("memtable should not be null", K(ret), K(memtable_handle_));
  } else if (is_committed) {
    WRLockGuard guard(list_rwlock_);
    if (OB_FAIL(commit_table_lock_(commit_version, commit_scn))) {
      LOG_WARN("commit table lock failed.", K(ret));
    }
  } else {
    WRLockGuard guard(list_rwlock_);
    abort_table_lock_();
  }
  LOG_DEBUG("ObLockMemCtx::clear_table_lock ", K(ret), K(is_committed), K(commit_scn));
  return ret;
}

int ObLockMemCtx::add_lock_record(
    const ObTableLockOp &lock_op,
    ObMemCtxLockOpLinkNode *&lock_op_node)
{
  int ret = OB_SUCCESS;
  void *ptr = NULL;
  lock_op_node = NULL;
  if (OB_UNLIKELY(!lock_op.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument.", K(ret), K(lock_op));
  } else if (OB_ISNULL(ptr = alloc_lock_link_node_())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alllocate ObTableLockOp ", K(ret));
  } else if (FALSE_IT(lock_op_node = new(ptr) ObMemCtxLockOpLinkNode())) {
    // do nothing
  } else if (OB_FAIL(lock_op_node->init(lock_op))) {
    LOG_WARN("set lock op info failed.", K(ret), K(lock_op));
  } else {
    WRLockGuard guard(list_rwlock_);
    if (!lock_list_.add_last(lock_op_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("add lock op info failed.", K(ret), K(lock_op));
    }
  }
  if (OB_FAIL(ret) && NULL != lock_op_node) {
    lock_op_node->~ObMemCtxLockOpLinkNode();
    free_lock_link_node_(lock_op_node);
    lock_op_node = NULL;
  }
  LOG_DEBUG("ObLockMemCtx::add_lock_record", K(ret), K(lock_op));
  return ret;
}

void ObLockMemCtx::remove_lock_record(
    ObMemCtxLockOpLinkNode *lock_op)
{
  if (OB_ISNULL(lock_op)) {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "invalid argument.", K(lock_op));
  } else {
    {
      WRLockGuard guard(list_rwlock_);
      (void)lock_list_.remove(lock_op);
    }
    lock_op->~ObMemCtxLockOpLinkNode();
    free_lock_link_node_(lock_op);
    lock_op = NULL;
  }
}

void ObLockMemCtx::remove_lock_record(
    const ObTableLockOp &lock_op)
{
  if (OB_UNLIKELY(!lock_op.is_valid())) {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "invalid argument.", K(lock_op));
  } else {
    WRLockGuard guard(list_rwlock_);
    DLIST_FOREACH_REMOVESAFE_NORET(curr, lock_list_) {
      if (curr->lock_op_.lock_id_ == lock_op.lock_id_ &&
          curr->lock_op_.lock_mode_ == lock_op.lock_mode_ &&
          curr->lock_op_.op_type_ == lock_op.op_type_) {
        (void)lock_list_.remove(curr);
        curr->~ObMemCtxLockOpLinkNode();
        free_lock_link_node_(curr);
      }
    }
  }
  LOG_DEBUG("ObLockMemCtx::remove_lock_record ", K(lock_op));
}

int ObLockMemCtx::check_lock_exist( //TODO(lihongqin):check it
    const ObLockID &lock_id,
    const ObTableLockOwnerID &owner_id,
    const ObTableLockMode mode,
    const ObTableLockOpType op_type,
    bool &is_exist,
    uint64_t lock_mode_cnt_in_same_trans[]) const
{
  int ret = OB_SUCCESS;
  is_exist = false;

  if (OB_UNLIKELY(!lock_id.is_valid()) ||
      OB_UNLIKELY(!is_lock_mode_valid(mode))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument.", K(ret), K(lock_id), K(owner_id), K(mode));
  } else {
    RDLockGuard guard(list_rwlock_);
    DLIST_FOREACH(curr, lock_list_) {
      if (curr->lock_op_.lock_id_ == lock_id) {
        // BE CAREFUL: get all the lock mode curr trans has got.
        lock_mode_cnt_in_same_trans[get_index_by_lock_mode(curr->lock_op_.lock_mode_)]++;
        // check exist.
        if (curr->lock_op_.owner_id_ == owner_id &&
            curr->lock_op_.op_type_ == op_type && /* different op type may lock twice */
            curr->lock_op_.lock_op_status_ == LOCK_OP_DOING) {
          // dbms_lock can only have one obj lock, no matter what lock_mode
          is_exist = lock_id.obj_type_ == ObLockOBJType::OBJ_TYPE_DBMS_LOCK ? true : curr->lock_op_.lock_mode_ == mode;
          if (is_exist) break;
        }
      }
    }
  }
  return ret;
}

int ObLockMemCtx::check_contain_tablet(ObTabletID tablet_id, bool &contain)
{
  int ret = OB_SUCCESS;
  contain = false;
  RDLockGuard guard(list_rwlock_);
  DLIST_FOREACH(curr, lock_list_) {
    if (curr->lock_op_.is_tablet_lock(tablet_id)) {
      contain = true;
      break;
    }
  }
  return ret;
}

int ObLockMemCtx::check_modify_schema_elapsed(
    const ObLockID &lock_id,
    const int64_t schema_version)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!lock_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument.", K(ret), K(lock_id));
  } else {
    RDLockGuard guard(list_rwlock_);
    DLIST_FOREACH(curr, lock_list_) {
      if (curr->lock_op_.lock_id_ == lock_id &&
          curr->lock_op_.create_schema_version_ < schema_version) {
        // there is some trans that modify the tablet before schema version
        // running.
        ret = OB_EAGAIN;
        if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
          LOG_WARN("there is some trans with smaller modify schema version not finished",
                   K(ret), K(curr->lock_op_));
        }
      }
    }
  }
  return ret;
}

int ObLockMemCtx::check_modify_time_elapsed(
    const ObLockID &lock_id,
    const int64_t timestamp)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!lock_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument.", K(ret), K(lock_id));
  } else {
    RDLockGuard guard(list_rwlock_);
    DLIST_FOREACH(curr, lock_list_) {
      if (curr->lock_op_.lock_id_ == lock_id &&
          curr->lock_op_.create_timestamp_ < timestamp) {
        // there is some trans that modify the tablet before timestamp
        // running.
        ret = OB_EAGAIN;
        if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
          LOG_WARN("there is some trans with smaller modify time not finished",
                   K(ret), K(curr->lock_op_));
        }
      }
    }
  }
  return ret;
}

int ObLockMemCtx::iterate_tx_obj_lock_op(ObLockOpIterator &iter) const
{
  int ret = OB_SUCCESS;
  RDLockGuard guard(list_rwlock_);
  DLIST_FOREACH_X(curr, lock_list_, OB_SUCC(ret)) {
    if (NULL != curr &&
        OB_FAIL(iter.push(curr->lock_op_))) {
      LOG_WARN("push lock op into iterator failed", K(ret), K(curr->lock_op_));
    }
  }
  return ret;
}

int ObLockMemCtx::check_lock_need_replay(
    const SCN &scn,
    const ObTableLockOp &lock_op,
    bool &need_replay)
{
  int ret = OB_SUCCESS;
  need_replay = true;

  if (scn < max_durable_scn_) {
    LOG_INFO("no need replay at tx ctx", K(max_durable_scn_), K(scn), K(lock_op));
    need_replay = false;
  } else if (OB_UNLIKELY(!lock_op.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument.", K(ret), K(lock_op));
  } else {
    RDLockGuard guard(list_rwlock_);
    DLIST_FOREACH(curr, lock_list_) {
      if (!(need_replay = curr->lock_op_.need_replay_or_recover(lock_op))) {
        break;
      }
    }
  }
  return ret;
}

void ObLockMemCtx::print() const
{
  RDLockGuard guard(list_rwlock_);
  LOG_INFO("ObLockMemCtx::print");
  DLIST_FOREACH_NORET(curr, lock_list_) {
    LOG_INFO("LockNode:", K(*curr));
  }
}

ObOBJLockCallback *ObLockMemCtx::create_table_lock_callback(ObIMvccCtx &ctx, ObLockMemtable *memtable)
{
  int ret = OB_SUCCESS;
  void *cb_buffer = NULL;
  ObOBJLockCallback *cb = NULL;
  if (NULL == (cb_buffer = alloc_table_lock_callback_())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TRANS_LOG(WARN, "alloc ObOBJLockCallback cb_buffer fail", K(ret));
  }
  if (NULL != cb_buffer) {
    if (NULL == (cb = new(cb_buffer) ObOBJLockCallback(ctx, memtable))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TRANS_LOG(WARN, "construct ObOBJLockCallback object fail", K(ret), "cb_buffer", cb_buffer);
    }
  }
  return cb;
}

OB_INLINE void *ObLockMemCtx::alloc_table_lock_callback_() { return host_.alloc_table_lock_callback(); }
OB_INLINE void ObLockMemCtx::free_table_lock_callback_(memtable::ObITransCallback *cb) { host_.free_table_lock_callback(cb); }
OB_INLINE void *ObLockMemCtx::alloc_lock_link_node_() { return host_.alloc_lock_link_node(); }
OB_INLINE void ObLockMemCtx::free_lock_link_node_(void *ptr) { host_.free_lock_link_node(ptr); }

}  // namespace tablelock
}  // namespace transaction
}  // namespace oceanbase
