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

#include "ob_mvcc_trans_ctx.h"
#include "ob_mvcc_ctx.h"
#include "ob_mvcc_row.h"
#include "share/deadlock/ob_deadlock_detector_mgr.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/memtable/ob_memtable.h"
#include "storage/memtable/ob_memtable_context.h"
#include "storage/memtable/ob_memtable_data.h"
#include "storage/memtable/ob_memtable_util.h"
#include "storage/memtable/ob_memtable_mutator.h"
#include "lib/atomic/atomic128.h"
#include "storage/lock_wait_mgr/ob_lock_wait_mgr.h"
#include "storage/tx/ob_trans_ctx.h"
#include "storage/tx/ob_trans_part_ctx.h"
#include "storage/tx/ob_tx_stat.h"
#include "ob_mvcc_ctx.h"
#include "storage/memtable/ob_memtable_interface.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace transaction;
using namespace lockwaitmgr;
namespace memtable
{

void CorrectHashHolderOp::operator()() {
  row_holder_.erase_hash_holder_record(hash_, node_.tx_id_, node_.seq_no_);
}

void RedoDataNode::set(const ObMemtableKey *key,
                       const ObRowData &old_row,
                       const ObRowData &new_row,
                       const blocksstable::ObDmlFlag dml_flag,
                       const uint32_t modify_count,
                       const uint32_t acc_checksum,
                       const int64_t version,
                       const int32_t flag,
                       const transaction::ObTxSEQ seq_no,
                       const common::ObTabletID &tablet_id,
                       const int64_t column_cnt)
{
  key_.encode(*key);
  old_row_ = old_row;
  new_row_ = new_row;
  dml_flag_ = dml_flag;
  modify_count_ = modify_count;
  acc_checksum_ = acc_checksum;
  version_ = version;
  flag_ = flag;
  seq_no_ = seq_no;
  callback_ = NULL;
  tablet_id_ = tablet_id;
  column_cnt_ = column_cnt;
}

int TableLockRedoDataNode::set(
    const ObMemtableKey *key,
    const oceanbase::transaction::tablelock::ObTableLockOp &lock_op,
    const common::ObTabletID &tablet_id,
    ObITransCallback *callback)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(key) || !lock_op.is_valid() || !tablet_id.is_valid() || OB_ISNULL(callback)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(ERROR, "invalid argument", K(ret), KP(key), K(lock_op), K(tablet_id), KP(callback));
  } else {
    key_.encode(*key);
    lock_id_ = lock_op.lock_id_;
    owner_id_ = lock_op.owner_id_;
    lock_mode_ = lock_op.lock_mode_;
    lock_op_type_ = lock_op.op_type_;
    seq_no_ = lock_op.lock_seq_no_;
    callback_ = callback;
    tablet_id_ = tablet_id;
    create_timestamp_ = lock_op.create_timestamp_;
    create_schema_version_ = lock_op.create_schema_version_;
  }
  return ret;
}

void ObITransCallback::set_scn(const SCN scn)
{
  if (SCN::max_scn() == scn_) {
    scn_ = scn;
  }
}

SCN ObITransCallback::get_scn() const
{
  return scn_;
}

int ObITransCallback::before_append_cb(const bool is_replay)
{
  int ret = OB_SUCCESS;
  if (is_replay && !scn_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "scn is invalid for replay", K(ret), KPC(this));
  } else if (OB_FAIL(before_append(is_replay))) {
  } else if (is_replay) {
    need_fill_redo_ = false;
    need_submit_log_ = false;
  }
  return ret;
}

void ObITransCallback::after_append_fail_cb(const bool is_replay)
{
  if (need_fill_redo_ == !is_replay && need_submit_log_ == !is_replay) {
    (void)after_append_fail(is_replay);
  }
}

int ObITransCallback::log_submitted_cb()
{
  int ret = OB_SUCCESS;
  if (need_submit_log_) {
    if (OB_SUCC(log_submitted())) {
      need_submit_log_ = false;
    }
  }
  return ret;
}

int ObITransCallback::log_sync_cb(const SCN scn, ObIMemtable *&last_mt)
{
  int ret = OB_SUCCESS;
  if (!need_fill_redo_) {
  } else if (OB_UNLIKELY(SCN::max_scn() == scn)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "log ts should not be invalid", K(ret), K(scn), K(*this));
  } else if (FALSE_IT(set_scn(scn))) {
  } else if (OB_SUCC(log_sync(scn, last_mt))) {
    need_fill_redo_ = false;
  }
  return ret;
}

int ObITransCallback::log_sync_fail_cb()
{
  int ret = OB_SUCCESS;
  if (need_fill_redo_) {
    if (OB_SUCC(log_sync_fail())) {
      need_fill_redo_ = false;
    }
  }
  return ret;
}

// All safety check is in before append
void ObITransCallback::append(ObITransCallback *node)
{
  node->set_prev(this);
  node->set_next(this->get_next());
  this->get_next()->set_prev(node);
  this->set_next(node);
}

void ObITransCallback::append(ObITransCallback *head,
                              ObITransCallback *tail)
{
  head->set_prev(this);
  tail->set_next(this->get_next());
  this->get_next()->set_prev(tail);
  this->set_next(head);
}

int ObITransCallback::remove()
{
  int ret = OB_SUCCESS;
  ObITransCallback *prev = this->get_prev();
  ObITransCallback *next = this->get_next();
  if (OB_ISNULL(prev) || OB_ISNULL(next)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    prev->set_next(next);
    next->set_prev(prev);
  }
  return ret;
}

ObTransCallbackMgr::WRLockGuard::WRLockGuard(const SpinRWLock &rwlock)
#ifdef ENABLE_DEBUG_LOG
  : time_guard_(5 * 1000 * 1000), // 5 second
    lock_guard_(rwlock)
{
  time_guard_.click();
}
#else
  : lock_guard_(rwlock)
{
}
#endif

ObTransCallbackMgr::RDLockGuard::RDLockGuard(const SpinRWLock &rwlock)
#ifdef ENABLE_DEBUG_LOG
  : time_guard_(5 * 1000 * 1000), // 5 second
    lock_guard_(rwlock)
{
  time_guard_.click();
}
#else
  : lock_guard_(rwlock)
{
}
#endif

#define CALLBACK_LISTS_FOREACH_(idx, list, CONST)                       \
  CONST ObTxCallbackList *list = &callback_list_;                             \
  const int list_cnt = (!need_merge_ && callback_lists_) ? MAX_CALLBACK_LIST_COUNT : 1; \
  for (int idx = 0; OB_SUCC(ret) && idx < list_cnt;                     \
       list = (list_cnt > 1 ? callback_lists_ + idx : NULL), ++idx)

#define CALLBACK_LISTS_FOREACH_CONST(idx, list) CALLBACK_LISTS_FOREACH_(idx, list, const)
#define CALLBACK_LISTS_FOREACH(idx, list) CALLBACK_LISTS_FOREACH_(idx, list,)

void ObTransCallbackMgr::reset()
{
  int64_t stat = ATOMIC_LOAD(&parallel_stat_);
  skip_checksum_ = false;
  callback_list_.reset();
  if (callback_lists_) {
    int cnt = need_merge_ ? MAX_CALLBACK_LIST_COUNT : MAX_CALLBACK_LIST_COUNT - 1;
    for (int i = 0; i < cnt; ++i) {
      if (!callback_lists_[i].empty()) {
        TRANS_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "txn callback list is broken", K(stat), K(i), K(this));
#ifdef ENABLE_DEBUG_LOG
        ob_abort();
#endif
      }
    }
  }
  if (NULL != cb_allocators_) {
    for (int i = 0; i < MAX_CB_ALLOCATOR_COUNT; ++i) {
      cb_allocators_[i].reset();
    }
  }
  if (OB_NOT_NULL(callback_lists_)) {
    cb_allocator_.free(callback_lists_);
    callback_lists_ = NULL;
  }
  if (OB_NOT_NULL(cb_allocators_)) {
    cb_allocator_.free(cb_allocators_);
    cb_allocators_ = NULL;
  }
  parallel_stat_ = 0;
  need_merge_ = false;
  for_replay_ = false;
  has_branch_replayed_into_first_list_ = false;
  serial_final_scn_.set_max();
  serial_final_seq_no_.reset();
  serial_sync_scn_.set_min();
  callback_main_list_append_count_ = 0;
  callback_slave_list_append_count_ = 0;
  callback_slave_list_merge_count_ = 0;
  callback_remove_for_trans_end_count_ = 0;
  callback_remove_for_remove_memtable_count_ = 0;
  callback_remove_for_fast_commit_count_ = 0;
  callback_remove_for_rollback_to_count_ = 0;
  callback_ext_info_log_count_ = 0;
  pending_log_size_ = 0;
  flushed_log_size_ = 0;
}

void ObTransCallbackMgr::free_mvcc_row_callback(ObITransCallback *cb)
{
  int64_t owner = cb->owner_;
  if (-1 == owner) {
    TRANS_LOG_RET(WARN, OB_ERR_UNEXPECTED, "callback free failed", KPC(cb));
  } else if (0 == owner) {
    mem_ctx_obj_pool_.free<ObMvccRowCallback>(cb);
  } else if (0 < owner && MAX_CB_ALLOCATOR_COUNT >= owner && OB_NOT_NULL(cb_allocators_)) {
    cb_allocators_[owner - 1].free(cb);
  } else {
    TRANS_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "unexpected cb", KPC(cb));
#ifdef ENABLE_DEBUG_LOG
    ob_abort();
#endif
  }
}

void *ObTransCallbackMgr::alloc_mvcc_row_callback()
{
  int ret = OB_SUCCESS;
  ObITransCallback *callback = nullptr;
  const int64_t tid = get_itid() + 1;
  const int64_t slot = tid % MAX_CB_ALLOCATOR_COUNT;
  int64_t stat = ATOMIC_LOAD(&parallel_stat_);

  if (PARALLEL_STMT == stat || (for_replay_ && parallel_replay_)) {
    if (NULL == cb_allocators_) {
      WRLockGuard guard(rwlock_);
      if (NULL == cb_allocators_) {
        ObMemtableCtxCbAllocator *tmp_cb_allocators = nullptr;
        if (NULL == (tmp_cb_allocators = (ObMemtableCtxCbAllocator *)cb_allocator_.alloc(
                       sizeof(ObMemtableCtxCbAllocator) * MAX_CB_ALLOCATOR_COUNT))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          TRANS_LOG(WARN, "alloc cb allocator fail", K(ret));
        } else {
          for (int i = 0; OB_SUCC(ret) && i < MAX_CB_ALLOCATOR_COUNT; ++i) {
            UNUSED(new(tmp_cb_allocators + i) ObMemtableCtxCbAllocator());
            if (OB_FAIL(tmp_cb_allocators[i].init(MTL_ID()))) {
              TRANS_LOG(ERROR, "cb_allocator_ init error", K(ret));
            }
          }
          if (OB_SUCC(ret)) {
            cb_allocators_ = tmp_cb_allocators;
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (NULL == cb_allocators_) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "cb allocators is not inited", K(ret));
      } else {
        callback = (ObITransCallback *)(cb_allocators_[slot].alloc(sizeof(ObMvccRowCallback)));
        if (nullptr != callback) {
          callback->owner_ = slot + 1;
        }
      }
    }
  } else {
    callback = (ObITransCallback *)(mem_ctx_obj_pool_.alloc<ObMvccRowCallback>());
    if (nullptr != callback) {
      callback->owner_ = 0;
    }
  }

  if (OB_FAIL(ret)) {
    callback = nullptr;
  }

  return callback;
}

inline
int ObTransCallbackMgr::extend_callback_lists_(const int16_t cnt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == callback_lists_)) {
    WRLockGuard guard(rwlock_);
    if (NULL == callback_lists_) {
      int size = sizeof(ObTxCallbackList) * cnt;
      ObTxCallbackList *tmp_callback_lists = (ObTxCallbackList *)cb_allocator_.alloc(size);
      if (OB_ISNULL(tmp_callback_lists)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TRANS_LOG(WARN, "alloc cb lists fail", K(ret));
      } else {
        for (int16_t i = 0; i < cnt; ++i) {
          UNUSED(new(tmp_callback_lists + i) ObTxCallbackList(*this, i+1));
        }
        ATOMIC_STORE(&callback_lists_, tmp_callback_lists);
      }
    }
  }
  return ret;
}

int ObTransCallbackMgr::get_tx_seq_replay_idx(const transaction::ObTxSEQ seq) const
{
  return seq.get_branch() % MAX_CALLBACK_LIST_COUNT;
}

_RLOCAL(bool, ObTransCallbackMgr::parallel_replay_);

// called by write and replay
int ObTransCallbackMgr::append(ObITransCallback *node)
{
  int ret = OB_SUCCESS;
  (void)before_append(node);
  if (!for_replay_) {
    node->set_epoch(write_epoch_);
  }
  const transaction::ObTxSEQ seq_no = node->get_seq_no();
  if (OB_LIKELY(seq_no.support_branch())) {
    // NEW since version 4.2.4, select by branch
    int slot = seq_no.get_branch() % MAX_CALLBACK_LIST_COUNT;
    if (OB_UNLIKELY(slot > 0
                    && for_replay_
                    && is_serial_final_()
                    && node->get_scn() <= serial_final_scn_)) {
      // _NOTE_
      // for log with scn before serial final and replayed after txn recovery from point after serial final
      // it's replayed into first callback-list to keep the scn is in asc order for all callback list
      // for example:
      // serial final log scn = 100
      // recovery point scn = 200
      // log replaying with scn = 80
      //
      // Checksum calculation:
      // this log has been accumulated, it will not be required in all calback-list
      if (parallel_replay_) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "parallel replay an serial log", KR(ret), KPC(this));
#ifdef ENABLE_DEBUG_LOG
        ob_abort();
#endif
      }
      if (OB_SUCC(ret) && OB_UNLIKELY(!has_branch_replayed_into_first_list_)) {
        // sanity check: the serial_final_seq_no must be set
        // which will be used in replay `rollback branch savepoint` log
        if (OB_UNLIKELY(!serial_final_seq_no_.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(ERROR, "serial_final_seq_no is invalid", KR(ret), KPC(this));
#ifdef ENABLE_DEBUG_LOG
          ob_abort();
#endif
        } else {
          // need to push up the callback_list.0's checksum_scn
          // to avoid it calculate checksum includes those callbacks
          callback_list_.inc_update_checksum_scn(SCN::scn_inc(serial_final_scn_));
          ATOMIC_STORE(&has_branch_replayed_into_first_list_, true);
          TRANS_LOG(INFO, "replay log before serial final when reach serial final",
                    KPC(this), KPC(get_trans_ctx()), KPC(node));
        }
      }
      slot = 0;
    }
    if (OB_FAIL(ret)) {
    } else if (OB_LIKELY(slot == 0)) {
      // no parallel and no branch requirement
      ret = callback_list_.append_callback(node, for_replay_, parallel_replay_, is_serial_final_());
      // try to extend callback_lists_ if required
    } else if (!callback_lists_ && OB_FAIL(extend_callback_lists_(MAX_CALLBACK_LIST_COUNT - 1))) {
      TRANS_LOG(WARN, "extend callback lists failed", K(ret));
    } else {
      ret = callback_lists_[slot - 1].append_callback(node, for_replay_, parallel_replay_, is_serial_final_());
    }
  } else {
    // OLD before version 4.2.4
    // if has parallel select from callback_lists_
    // don't select main, and merge into main finally
    const int64_t tid = get_itid() + 1;
    int slot = tid % MAX_CALLBACK_LIST_COUNT;
    int64_t stat = ATOMIC_LOAD(&parallel_stat_);
    if (PARALLEL_STMT == stat) {
      if (OB_FAIL(!callback_lists_ && extend_callback_lists_(MAX_CALLBACK_LIST_COUNT))) {
        TRANS_LOG(WARN, "extend callback lists failed", K(ret));
      } else {
        ret = callback_lists_[slot].append_callback(node, for_replay_, parallel_replay_, true);
        add_slave_list_append_cnt();
      }
    } else {
      ret = callback_list_.append_callback(node, for_replay_, parallel_replay_, true);
      add_main_list_append_cnt();
    }
  }
#ifdef ENABLE_DEBUG_LOG
  memtable_set_injection_sleep();
#endif
  after_append(node, ret);
  return ret;
}

// called only by write now
int ObTransCallbackMgr::append(ObITransCallback *head,
                               ObITransCallback *tail,
                               const int64_t length)
{
  int ret = OB_SUCCESS;

  if (nullptr != head && nullptr != tail) {
    if (for_replay_) {
      // We donot support batch append for replay now
      OB_ASSERT(1 == length);
    }

    // Step1: prepare the callback append(epoch for pdml and statistic)
    for (ObITransCallback *cb = head; cb != nullptr; cb = cb->get_next()) {
      (void)before_append(cb);
      if (!for_replay_) {
        cb->set_epoch(write_epoch_);
      }
    }

    // Step2: find the slot for register or replay
    const transaction::ObTxSEQ seq_no = head->get_seq_no();
    if (OB_LIKELY(seq_no.support_branch())) {
      // NEW since version 4.2.4, select by branch
      int slot = seq_no.get_branch() % MAX_CALLBACK_LIST_COUNT;
      if (OB_UNLIKELY(slot > 0
                      && for_replay_
                      && is_serial_final_()
                      && head->get_scn() <= serial_final_scn_)) {
        // _NOTE_
        // for log with scn before serial final and replayed after txn recovery from point after serial final
        // it's replayed into first callback-list to keep the scn is in asc order for all callback list
        // for example:
        // serial final log scn = 100
        // recovery point scn = 200
        // log replaying with scn = 80
        //
        // Checksum calculation:
        // this log has been accumulated, it will not be required in all calback-list
        if (parallel_replay_) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(ERROR, "parallel replay an serial log", KR(ret), KPC(this));
#ifdef ENABLE_DEBUG_LOG
          ob_abort();
#endif
        }
        if (OB_SUCC(ret) && OB_UNLIKELY(!has_branch_replayed_into_first_list_)) {
          // sanity check: the serial_final_seq_no must be set
          // which will be used in replay `rollback branch savepoint` log
          if (OB_UNLIKELY(!serial_final_seq_no_.is_valid())) {
            ret = OB_ERR_UNEXPECTED;
            TRANS_LOG(ERROR, "serial_final_seq_no is invalid", KR(ret), KPC(this));
#ifdef ENABLE_DEBUG_LOG
            ob_abort();
#endif
          } else {
            ATOMIC_STORE(&has_branch_replayed_into_first_list_, true);
            TRANS_LOG(INFO, "replay log before serial final when reach serial final",
                      KPC(this), KPC(get_trans_ctx()), KPC(head));
          }
        }
        slot = 0;
      }

      // Step2: register or replay
      if (OB_FAIL(ret)) {
      } else if (OB_LIKELY(slot == 0)) {
        // no parallel and no branch requirement
        ret = callback_list_.append_callback(head,
                                             tail,
                                             length,
                                             for_replay_,
                                             parallel_replay_,
                                             is_serial_final_());
        // try to extend callback_lists_ if required
      } else if (!callback_lists_ && OB_FAIL(extend_callback_lists_(MAX_CALLBACK_LIST_COUNT - 1))) {
        TRANS_LOG(WARN, "extend callback lists failed", K(ret));
      } else {
        ret = callback_lists_[slot - 1].append_callback(head,
                                                        tail,
                                                        length,
                                                        for_replay_,
                                                        parallel_replay_,
                                                        is_serial_final_());
      }
    } else {
      // OLD before version 4.2.4
      // if has parallel select from callback_lists_
      // don't select main, and merge into main finally
      const int64_t tid = get_itid() + 1;
      int slot = tid % MAX_CALLBACK_LIST_COUNT;
      int64_t stat = ATOMIC_LOAD(&parallel_stat_);
      if (PARALLEL_STMT == stat) {
        if (OB_FAIL(!callback_lists_ && extend_callback_lists_(MAX_CALLBACK_LIST_COUNT))) {
          TRANS_LOG(WARN, "extend callback lists failed", K(ret));
        } else {
          ret = callback_lists_[slot].append_callback(head,
                                                      tail,
                                                      length,
                                                      for_replay_,
                                                      parallel_replay_,
                                                      true /*is_serial_final*/);
          add_slave_list_append_cnt();
        }
      } else {
        ret = callback_list_.append_callback(head,
                                             tail,
                                             length,
                                             for_replay_,
                                             parallel_replay_,
                                             true /*is_serial_final*/);
        add_main_list_append_cnt();
      }
    }

    // Step3: revert the side effect if the append failed
    if (OB_FAIL(ret)) {
      for (ObITransCallback *cb = head;
           nullptr != cb && tail != cb->get_prev();
           cb = cb->get_next()) {
        after_append(cb, ret);
      }
    }
  }

  return ret;
}

void ObTransCallbackMgr::before_append(ObITransCallback *node)
{
  int64_t size = node->get_data_size();
  int64_t new_size = 0;
  if (for_replay_) {
    inc_flushed_log_size(size);
  } else {
    new_size = inc_pending_log_size(size);
  }
  if (OB_UNLIKELY(need_merge_)) {
    try_merge_multi_callback_lists(new_size, size, node->is_logging_blocked());
  }
}

void ObTransCallbackMgr::after_append(ObITransCallback *node, const int ret_code)
{
  if (OB_SUCCESS != ret_code) {
    int64_t size = node->get_data_size();
    if (for_replay_) {
      inc_flushed_log_size(-1 * size);
    } else {
      inc_pending_log_size(-1 * size);
    }
  }
}

int ObTransCallbackMgr::rollback_to(const ObTxSEQ to_seq_no,
                                    const ObTxSEQ from_seq_no,
                                    const share::SCN replay_scn,
                                    int64_t &remove_cnt)
{
  int ret = OB_SUCCESS;
  remove_cnt = callback_remove_for_rollback_to_count_;
  if (OB_LIKELY(to_seq_no.support_branch())) { // since 4.2.4
    // it is a global savepoint, rollback on all list
    if (to_seq_no.get_branch() == 0) {
      CALLBACK_LISTS_FOREACH(idx, list) {
        ret = list->remove_callbacks_for_rollback_to(to_seq_no, from_seq_no, replay_scn);
      }
    } else {
      // it is a branch level savepoint not supported
      ret = OB_NOT_SUPPORTED;
      TRANS_LOG(ERROR, "branch level savepoint not supported", KR(ret),
                  KPC(this), KPC(get_trans_ctx()), K(replay_scn), K(to_seq_no), K(from_seq_no));
    }
  } else { // before 4.2.4
    ret = callback_list_.remove_callbacks_for_rollback_to(to_seq_no, from_seq_no, replay_scn);
  }
  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "rollback to fail", K(ret), K(replay_scn), K(from_seq_no), K(to_seq_no));
  }
  remove_cnt = callback_remove_for_rollback_to_count_ - remove_cnt;
  return ret;
}

// merge `callback_lists_` into `callback_list_`
void ObTransCallbackMgr::merge_multi_callback_lists()
{
  int64_t stat = ATOMIC_LOAD(&parallel_stat_);
  int64_t cnt = 0;
  if (OB_UNLIKELY(ATOMIC_LOAD(&need_merge_)) && PARALLEL_STMT == stat) {
    WRLockGuard guard(rwlock_);
    if (OB_NOT_NULL(callback_lists_)) {
      for (int64_t i = 0; i < MAX_CALLBACK_LIST_COUNT; ++i) {
        cnt = callback_list_.concat_callbacks(callback_lists_[i]);
        add_slave_list_merge_cnt(cnt);
      }
    }
#ifndef NDEBUG
    TRANS_LOG(INFO, "merge callback lists to callback list", K(stat), K(host_.get_tx_id()));
#endif
  }
}

void ObTransCallbackMgr::force_merge_multi_callback_lists()
{
  int64_t cnt = 0;
  if (OB_UNLIKELY(ATOMIC_LOAD(&need_merge_))) {
    WRLockGuard guard(rwlock_);
    if (OB_NOT_NULL(callback_lists_)) {
      for (int64_t i = 0; i < MAX_CALLBACK_LIST_COUNT; ++i) {
        cnt = callback_list_.concat_callbacks(callback_lists_[i]);
        add_slave_list_merge_cnt(cnt);
      }
    }
  }
  TRANS_LOG(DEBUG, "force merge callback lists to callback list", K(host_.get_tx_id()));
}

transaction::ObPartTransCtx *ObTransCallbackMgr::get_trans_ctx() const
{
  return host_.get_trans_ctx();
}

void ObTransCallbackMgr::reset_pdml_stat()
{
  bool need_retry = true;
  while (need_retry) {
    WRLockGuard guard(rwlock_);
    int64_t stat = ATOMIC_LOAD(&parallel_stat_);
    if (!ATOMIC_BCAS(&parallel_stat_, stat, 0)) {
      TRANS_LOG_RET(WARN, OB_ERR_UNEXPECTED, "reset parallel stat when leader revoke encounter parallel",
                K(stat), K(parallel_stat_));
    } else {
      need_retry = false;
    }
  }

  force_merge_multi_callback_lists();
}

// only for replay
// @callback_list_idx: the current replay thread replayed queue
//            for serial replay, the queue maybe replay logs belongs to other callback-list
// @stop_scn: should stop at this scn, it equals to the scn of current replaying log minus 1
//            because current replaying log maybe replay failed and rolled back
int ObTransCallbackMgr::remove_callbacks_for_fast_commit(const int16_t callback_list_idx,
                                                         const share::SCN stop_scn)
{
  int ret = OB_SUCCESS;
  RDLockGuard guard(rwlock_);
  // NOTE:
  // this can handle both NEW(since 4.2.4) and compatible with OLD version:
  // because before 4.2.4, replay only append to main (the `callback_list_`)
  if (OB_UNLIKELY(callback_list_idx != 0 || is_serial_final_())) {
    ObTxCallbackList *list = get_callback_list_(callback_list_idx, true);
    if (OB_ISNULL(list)) {
      // the callback list may not extended by replay redo if row is skipped
    } else {
      // if need checksum and serial replay not final
      // for fast-commit after parallel replayed, stop at serial replayed position
      share::SCN real_stop_scn = stop_scn;
      if (!skip_checksum_ && !is_serial_final_()) {
        real_stop_scn = serial_sync_scn_;
      }
      if (OB_FAIL(list->remove_callbacks_for_fast_commit(real_stop_scn))) {
        TRANS_LOG(WARN, "remove callbacks for fast commit fail", K(ret),
                  K(real_stop_scn), K(stop_scn), K(callback_list_idx), KPC(list));
      }
    }
  } else { // for serial replayed log, and not reach serial final, handle all list
    if (OB_LIKELY(NULL == callback_lists_)) {
      ret = callback_list_.remove_callbacks_for_fast_commit(stop_scn);
    } else {
      CALLBACK_LISTS_FOREACH(idx, list) {
        if (OB_FAIL(list->remove_callbacks_for_fast_commit(stop_scn))) {
          TRANS_LOG(WARN, "remove callbacks for fast commit fail", K(ret), K(idx), KPC(list));
        }
      }
    }
  }
  return ret;
}

// for leader
// called after log apply thread has callbacked the log_cb
// @scopes: the log's callback-list scopes
int ObTransCallbackMgr::remove_callbacks_for_fast_commit(const ObCallbackScopeArray &scopes)
{
  // this can handle both NEW (since 4.2.4) and OLD (before 4.2.4):
  // before 4.2.4: scopes must be single and came from the main list
  const share::SCN stop_scn = is_serial_final_() ? share::SCN::invalid_scn() : serial_sync_scn_;
  int ret = OB_SUCCESS;
  ARRAY_FOREACH(scopes, i) {
    if (OB_FAIL(scopes.at(i).host_->remove_callbacks_for_fast_commit(stop_scn))) {
      TRANS_LOG(WARN, "remove callbacks for fast commit fail", K(ret), K(i), KPC(scopes.at(i).host_));
    }
  }
  return ret;
}

// memtable will be released, remove callbacks refer to it
// these callbacks has been logged and the writes were in checkpoint
int ObTransCallbackMgr::remove_callback_for_uncommited_txn(const memtable::ObMemtableSet *memtable_set)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(memtable_set)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "memtable is null", K(ret));
  } else if (!memtable_set->empty()) {
    share::SCN stop_scn = share::SCN::min_scn();
    for (common::hash::ObHashSet<uint64_t>::const_iterator iter = memtable_set->begin(); iter != memtable_set->end(); ++iter) {
      stop_scn = share::SCN::max(((const memtable::ObMemtable *)iter->first)->get_max_end_scn(), stop_scn);
    }
    if (need_merge_) { // OLD (before 4.2.4)
      if (OB_FAIL(callback_list_.remove_callbacks_for_remove_memtable(memtable_set, stop_scn))) {
        TRANS_LOG(WARN, "fifo remove callback fail", K(ret), KPC(memtable_set));
      }
    } else {  // NEW (since 4.2.4)
      CALLBACK_LISTS_FOREACH(idx, list) {
        if (OB_FAIL(list->remove_callbacks_for_remove_memtable(memtable_set, stop_scn))) {
          TRANS_LOG(WARN, "fifo remove callback fail", K(ret), K(idx), KPC(memtable_set));
        }
      }
    }
  }

  return ret;
}

// when leader revoked, writes has not been logged must be discarded
// otherwise freeze memtable checkpoint will be blocked on waiting these.
int ObTransCallbackMgr::clean_unlog_callbacks(int64_t &removed_cnt)
{
  int ret = OB_SUCCESS;
  if (need_merge_) { // OLD (before 4.2.4)
    if (OB_FAIL(callback_list_.clean_unlog_callbacks(removed_cnt))) {
      TRANS_LOG(WARN, "clean unlog callbacks failed", K(ret));
    }
  } else { // NEW (since 4.2.4)
    CALLBACK_LISTS_FOREACH(idx, list) {
      int64_t rm_cnt = 0;
      if (OB_FAIL(list->clean_unlog_callbacks(rm_cnt))) {
        TRANS_LOG(WARN, "clean unlog callbacks failed", K(ret), K(idx));
      } else {
        removed_cnt += rm_cnt;
      }
    }
  }
  TRANS_LOG(TRACE, "clean callbacks", K(ret), K(removed_cnt));
  return ret;
}

int ObTransCallbackMgr::calc_checksum_before_scn(const SCN scn,
                                                 ObIArray<uint64_t> &checksum,
                                                 ObIArray<share::SCN> &checksum_scn)
{
  int ret = OB_SUCCESS;
  const share::SCN stop_scn = is_serial_final_() ? share::SCN::max_scn() : serial_sync_scn_;
  const bool is_single_callback_list = ATOMIC_LOAD(&callback_lists_) == NULL;
  if (need_merge_ || is_single_callback_list) { // OLD (before 4.2.4) or only single callback_list
    if (OB_FAIL(callback_list_.tx_calc_checksum_before_scn(stop_scn))) {
      TRANS_LOG(WARN, "calc checksum fail", K(ret));
    } else {
      callback_list_.get_checksum_and_scn(checksum.at(0), checksum_scn.at(0));
    }
  } else { // new (since 4.2.4) and multiple callback_list
    // reserve space
    if (checksum.count() < MAX_CALLBACK_LIST_COUNT) {
      if (OB_FAIL(checksum.reserve(MAX_CALLBACK_LIST_COUNT))) {
        TRANS_LOG(WARN, "reserve fail", K(ret));
      } else if (OB_FAIL(checksum_scn.reserve(MAX_CALLBACK_LIST_COUNT))) {
        TRANS_LOG(WARN, "reserve fail", K(ret));
      } else {
        for (int i = checksum.count(); i < MAX_CALLBACK_LIST_COUNT; i++) {
          checksum.push_back(0);
          checksum_scn.push_back(share::SCN::min_scn());
        }
      }
    }
    if (OB_SUCC(ret)) {
      CALLBACK_LISTS_FOREACH(idx, list) {
        if (OB_FAIL(list->tx_calc_checksum_before_scn(stop_scn))) {
          TRANS_LOG(WARN, "calc checksum with minor freeze failed", K(ret), K(stop_scn), K(idx));
        } else {
          list->get_checksum_and_scn(checksum.at(idx), checksum_scn.at(idx));
        }
      }
    }
  }
  return ret;
}

#define FILL_LOG_TRACE_LEVEL DEBUG
#define FILL_LOG_TRACE(fmt, ...) TRANS_LOG(FILL_LOG_TRACE_LEVEL, "[FILL_LOG]" fmt, K(ret), KPC(this), ##__VA_ARGS__)
void ObTransCallbackMgr::calc_list_fill_log_epoch_(const int list_idx, int64_t &epoch_from, int64_t &epoch_to)
{
  epoch_to = epoch_from = 0;
  int64_t this_epoch = get_callback_list_(list_idx, false)->get_log_epoch();
  int64_t other_min = INT64_MAX;
  int list_cnt = get_logging_list_count();
  bool no_result = false;
  for (int i=0; i< list_cnt; i++) {
    if (i != list_idx) {
      ObTxCallbackList *list = get_callback_list_(i, false);
      int64_t epoch_i = list->get_log_epoch();
      if (epoch_i < this_epoch) {
        no_result = true;
        TRANS_LOG(TRACE, "no_result", K(epoch_i), K(i), K(this_epoch));
        break;
      }
      if (epoch_i < other_min) {
        other_min = epoch_i;
      }
    }
  }
  if (!no_result) {
    epoch_from = this_epoch;
    epoch_to = other_min;
  }
}

void ObTransCallbackMgr::calc_next_to_fill_log_info_(const ObIArray<RedoLogEpoch> &arr,
                                                     int &index,
                                                     int64_t &epoch_from,
                                                     int64_t &epoch_to)
{
  index = -1;
  epoch_to = epoch_from = INT64_MAX;
  for (int i =0; i< arr.count(); i++) {
    if (arr.at(i) < epoch_from) {
      index = i;
      epoch_to = epoch_from;
      epoch_from = arr.at(i);
    } else if (arr.at(i) < epoch_to) {
      epoch_to = arr.at(i);
    }
  }
}

inline
int ObTransCallbackMgr::prep_and_fill_from_list_(ObTxFillRedoCtx &ctx,
                                                 ObITxFillRedoFunctor &func,
                                                 int16 &callback_scope_idx,
                                                 const int index,
                                                 int64_t epoch_from,
                                                 int64_t epoch_to)
{
  int ret = OB_SUCCESS;
  // alloc callback scope for this list if not exist
  if (callback_scope_idx == -1) {
    ObCallbackScope scope;
    if (OB_FAIL(ctx.callback_scopes_.push_back(scope))) {
      TRANS_LOG(WARN, "prepare callbackscope fail", K(ret));
    } else {
      callback_scope_idx = ctx.callback_scopes_.count() - 1;
    }
    FILL_LOG_TRACE("choose callback scope idx", K(index), K(callback_scope_idx));
  }
  // prepare fill ctx and do fill
  if (OB_SUCC(ret)) {
    ctx.list_idx_ = index;
    ObTxCallbackList *callback_list = get_callback_list_(index, false);
    ObCallbackScope &callback_scope = ctx.callback_scopes_[callback_scope_idx];
    callback_scope.host_ = callback_list;
    ObITransCallback *log_cursor = NULL;
    if (callback_scope.cnt_ == 0) {
      // first round to fill this list
      log_cursor = callback_list->get_log_cursor();
      callback_scope.start_ = log_cursor;
    } else {
      log_cursor = *(callback_scope.end_ + 1);
    }
    int32_t cnt = 0;
    int64_t data_size = 0;
    ObITransCallback *last_filled = NULL;
    ret = func(log_cursor, // start
               callback_list->get_guard(), // end
               epoch_from,
               epoch_to,
               last_filled,
               cnt,
               data_size,
               ctx.max_seq_no_);
    if (last_filled) {
      callback_scope.end_ = last_filled;
    }
    callback_scope.cnt_ += cnt;
    callback_scope.data_size_ += data_size;
    ctx.data_size_ += data_size;
    // after fill, if none is filled, reset the callback-scope for this list
    if (OB_UNLIKELY(callback_scope.cnt_ == 0)) {
      ctx.callback_scopes_.pop_back();
      callback_scope_idx = -1;
      FILL_LOG_TRACE("fill from list result is empty, revert");
    }
  }
  return ret;
}

bool ObTransCallbackMgr::check_list_has_min_epoch_(const int my_idx,
                                                   const int64_t my_epoch,
                                                   const bool require_min,
                                                   int64_t &min_epoch,
                                                   int &min_idx)
{
  bool ret = true;
  int list_cnt = get_logging_list_count();
  for (int i=0; i< list_cnt; i++) {
    if (i != my_idx) {
      ObTxCallbackList *list = get_callback_list_(i, false);
      int64_t epoch_i = list->get_log_epoch();
      if (epoch_i < my_epoch) {
        ret = false;
        if (require_min) {
          if (min_epoch == 0 || epoch_i < min_epoch) {
            min_epoch = epoch_i;
            min_idx = i;
          }
        } else {
          min_epoch = epoch_i;
          min_idx = i;
          break;
        }
      }
    }
  }
  return ret;
}

// retval:
// - OB_EAGAIN: other list has small log_epoch
// - OB_ENTRY_NOT_EXIST: no need log
// - OB_NEED_RETRY: lock hold by other thread
// - OB_BLOCK_FROZEN: next to logging callback's memtable was logging blocked
int ObTransCallbackMgr::get_log_guard(const transaction::ObTxSEQ &write_seq,
                                      ObCallbackListLogGuard &lock_guard,
                                      int &ret_list_idx)
{
  int ret = OB_SUCCESS;
  RDLockGuard guard(rwlock_);
  const int list_idx = (write_seq.get_branch() % MAX_CALLBACK_LIST_COUNT);
  ObTxCallbackList *list = get_callback_list_(list_idx, true);
  if (OB_ISNULL(list)) {
    ret = OB_ENTRY_NOT_EXIST;
  } else {
    int64_t my_epoch = list->get_log_epoch();
    int64_t min_epoch = 0;
    int min_epoch_idx =-1;
    bool flush_min_epoch_list = false;
    common::ObByteLock *log_lock = NULL;
    if (my_epoch == INT64_MAX) {
      ret = OB_ENTRY_NOT_EXIST;
    } else if (OB_UNLIKELY(list->is_logging_blocked())) {
      ret = OB_BLOCK_FROZEN;
    } else if (OB_ISNULL(log_lock = list->try_lock_log())) {
      ret = OB_NEED_RETRY;
      // if current list pending size too large, try to submit the min_epoch list
    } else {
      if (OB_UNLIKELY(list->pending_log_too_large(GCONF._private_buffer_size * 10))) {
        flush_min_epoch_list = true;
      }
      if (!check_list_has_min_epoch_(list_idx, my_epoch, flush_min_epoch_list, min_epoch, min_epoch_idx)) {
        ret = OB_EAGAIN;
        ObIMemtable *to_log_memtable = list->get_log_cursor()->get_memtable();
        if (TC_REACH_TIME_INTERVAL(1_s)) {
          TRANS_LOG(WARN, "has smaller epoch unlogged", KPC(this),
                    K(list_idx), K(write_seq), K(my_epoch), K(min_epoch), K(min_epoch_idx), KP(to_log_memtable));
        }
      } else {
        ret_list_idx = list_idx;
        lock_guard.set(log_lock);
      }
      if (OB_FAIL(ret) && log_lock) {
        log_lock->unlock();
      }
    }
    if (OB_UNLIKELY(flush_min_epoch_list) && OB_EAGAIN == ret) {
      ObTxCallbackList *min_epoch_list = get_callback_list_(min_epoch_idx, false);
      if (OB_ISNULL(log_lock = min_epoch_list->try_lock_log())) {
        // lock conflict, acquired by others
      } else {
        if (REACH_TIME_INTERVAL(1_s)) {
          TRANS_LOG(INFO, "decide to flush callback list with min_epoch", KPC(this), K(min_epoch), K(min_epoch_idx));
        }
        ret_list_idx = min_epoch_idx;
        lock_guard.set(log_lock);
      }
    }
  }
  return ret;
}

int ObTransCallbackMgr::fill_log(ObTxFillRedoCtx &ctx, ObITxFillRedoFunctor &func)
{
  int ret = OB_SUCCESS;
  const bool is_single_list = OB_UNLIKELY(need_merge_) || ATOMIC_LOAD(&callback_lists_) == NULL;
  if (OB_LIKELY(is_single_list)) {
    if (OB_UNLIKELY(ctx.list_idx_ > 0)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "list_idx is unexpected", K(ret), K(ctx));
    } else {
      ObITransCallback *start = callback_list_.get_log_cursor();
      ObITransCallback *end = callback_list_.get_guard();
      if (start != end) {
        ctx.callback_scopes_.prepare_allocate(1);
        ctx.list_idx_ = 0;
        ObCallbackScope &callback_scope = ctx.callback_scopes_[0];
        callback_scope.host_ = &callback_list_;
        ObITransCallback *last = NULL;
        ret = func(start,
                   end,
                   start->get_epoch(),
                   INT64_MAX,
                   last,
                   callback_scope.cnt_,
                   callback_scope.data_size_,
                   ctx.max_seq_no_);
        callback_scope.start_ = start;
        callback_scope.end_ = last;
        ctx.data_size_ += callback_scope.data_size_;
        // after fill, if none is filled, reset the callback-scope for this list
        if (OB_UNLIKELY(callback_scope.cnt_ == 0)) {
          ctx.callback_scopes_.pop_back();
        }
      }
      if (OB_SUCC(ret)) {
        ctx.is_all_filled_ = true;
      }
    }
  } else if (ctx.list_idx_ >= 0) {
    ret = fill_from_one_list(ctx, ctx.list_idx_, func);
  } else {
    ret = fill_from_all_list(ctx, func);
  }
  return ret;
}

//
// fill RedoLog from single callback-list
//
// return value:
// - OB_SUCCESS: no remains, all callback is filled
// - OB_ITER_END: stopped due to has smaller write_epoch log hasn't submitted
// - OB_BLOCK_FROZEN: stopped due to memtable of cur callback is waiting previous frozen logged
// - OB_BUF_NOT_ENOUGH: buf can not hold cur callback
int ObTransCallbackMgr::fill_from_one_list(ObTxFillRedoCtx &ctx,
                                           int list_idx,
                                           ObITxFillRedoFunctor &func)
{
  int ret = OB_SUCCESS;
  FILL_LOG_TRACE("from one list", K(ctx), K(need_merge_));
  RDLockGuard guard(rwlock_);
  int64_t epoch_from = 0, epoch_to = 0;
  calc_list_fill_log_epoch_(list_idx, epoch_from, epoch_to);
  FILL_LOG_TRACE("start fill list", K(list_idx), K(epoch_from), K(epoch_to), K(ctx));
  if (!need_merge_ && epoch_from == 0) {
    ret = OB_ITER_END; // can not fill any callback, because of other list has min write epoch
  } else if (epoch_from == INT64_MAX) {
    ret = OB_SUCCESS; // no callback to fill
  } else {
    int16_t idx = -1;
    ret = prep_and_fill_from_list_(ctx, func, idx, list_idx, epoch_from, epoch_to);
  }
  if (OB_SUCC(ret)) {
    ctx.is_all_filled_ = true;
  }
  FILL_LOG_TRACE("fill from one done", K(ctx));
  return ret;
}

//
// fill redo log from all callback-list
//
// return value:
// - OB_SUCCESS: all callbacks from all callback-list filled
// - OB_EAGAIN: due to parallel logging, must return to flush this list and retry others
// - OB_BLOCK_FROZEN: stopped due to can not logging waiting memtable frozen
// - OB_ITER_END: stopped due to has smaller write_epoch whose log hasn't submitted
// - OB_BUF_NOT_ENOUGH: stopped due to buffer can not hold current node
// return policy:
// - if parallel_logging, return if need switch to next list and has
//   filled some callback from current list
// - otherwise, either buffer is full or some blocked reason can not fill any more
int ObTransCallbackMgr::fill_from_all_list(ObTxFillRedoCtx &ctx, ObITxFillRedoFunctor &func)
{
  int ret = OB_SUCCESS;
  FILL_LOG_TRACE("from all list entry", K(ctx));
  RDLockGuard guard(rwlock_);
  const int list_cnt = get_logging_list_count();
  // record each list's next to fill write_epoch
  ObIArray<RedoLogEpoch> &next_log_epoch_arr = ctx.list_log_epoch_arr_;
  if (list_cnt > next_log_epoch_arr.count()) {
    if (OB_FAIL(next_log_epoch_arr.reserve(list_cnt))) {
      TRANS_LOG(WARN, "reserve space for log epoch fail", K(ret), K(list_cnt));
    }
    for (int i=0; i < list_cnt; i++) {
      ObTxCallbackList *list = get_callback_list_(i, false);
      next_log_epoch_arr.push_back(list->get_log_epoch());
    }
  }
  // record each list's callback-scope object index in ctx.callbacks_ array
  int16_t callback_scope_idx_arr[list_cnt];
  for (int i =0; i < list_cnt; i++) {
    callback_scope_idx_arr[i] = -1;
  }

  FILL_LOG_TRACE("start from all list", K(list_cnt), K(ctx));

  int cur_index = -1;
  bool do_return = false;
  while (OB_SUCC(ret) && !do_return) {
    int index = 0;
    int64_t epoch_from = 0, epoch_to = 0;
    calc_next_to_fill_log_info_(next_log_epoch_arr, index, epoch_from, epoch_to);
    if (index == -1) {
      ctx.is_all_filled_ = true;
      FILL_LOG_TRACE("all list fill done", K(ctx));
      do_return = true; // all list is totally filled
    } else {
      int fill_ret = prep_and_fill_from_list_(ctx,
                                              func,
                                              callback_scope_idx_arr[index],
                                              index,
                                              epoch_from,
                                              epoch_to);
      FILL_LOG_TRACE("one fill round 1/2", K(fill_ret), K(index), K(epoch_from), K(epoch_to), K(ctx));
      bool try_other_lists = false;
      if (OB_SUCCESS == fill_ret) {
        // cur list is all filled
        next_log_epoch_arr.at(index) = INT64_MAX;
        // update epoch to point next, thus can consume from other list
        if (epoch_to == INT64_MAX) {
          // all filled
          ctx.is_all_filled_ = true;
          ret = fill_ret;
          do_return = true;
        } else {
          ctx.cur_epoch_ = epoch_to;
          try_other_lists = true;
        }
      } else if (OB_BUF_NOT_ENOUGH == fill_ret) {
        // buffer is full, must return to flush
        next_log_epoch_arr.at(index) = ctx.cur_epoch_;
        ret = fill_ret;
        do_return = true;
      } else if (OB_BLOCK_FROZEN == fill_ret) {
        // blocked, maybe has fill some data, maybe none
        next_log_epoch_arr.at(index) = ctx.cur_epoch_;
        try_other_lists = true;
      } else if (OB_ITER_END == fill_ret) {
        // this list has remains, but epoch larger than min epoch of other lists
        next_log_epoch_arr.at(index) = ctx.next_epoch_;
        // update epoch to point next, thus can consume from other list
        ctx.cur_epoch_ = epoch_to;
        try_other_lists = true;
      } else {
        TRANS_LOG(WARN, "fill redo from list fail", K(fill_ret), K(index));
        ret = fill_ret;
      }
      // when parallel logging, seperate log-entry for each callback-list
      if (!do_return && is_parallel_logging_() && ctx.not_empty()) {
        try_other_lists = false;
        ret = fill_ret;
        if (OB_SUCCESS == fill_ret) {
          ret = OB_EAGAIN;
        }
        do_return = true;
      }

      int choosen_list_fill_ret = fill_ret;
      // fill from other lists, this can be in two situations:
      // 1. parallel logging, but the first list can not fill any data
      // 2. serial logging, and buf is not full, need fill from others
      FILL_LOG_TRACE("one fill round 2/2", K(fill_ret), K(try_other_lists), K(ctx));
      if (try_other_lists && (list_cnt == 1 || ctx.cur_epoch_ != epoch_to)) {
        ret = fill_ret;
        ctx.is_all_filled_ = (list_cnt == 1) && (OB_SUCCESS == fill_ret);
        do_return = true;
      } else if (try_other_lists) {
        const int save_fill_count = ctx.fill_count_;
        // can only consume this epoch
        int64_t fill_epoch = ctx.cur_epoch_;
        bool all_others_reach_tail = true;
        int last_fail = OB_SUCCESS;
        for (int i = 0; i < list_cnt && !do_return; i++) {
          if (i == index) {
          } else if (next_log_epoch_arr.at(i) == INT64_MAX) {
            // nothing to fill, skip it
          } else if (next_log_epoch_arr.at(i) == fill_epoch) {
            FILL_LOG_TRACE("start fill others >>", K(i), K(fill_epoch), K(ctx));
            fill_ret = prep_and_fill_from_list_(ctx,
                                                func,
                                                callback_scope_idx_arr[i],
                                                i,
                                                fill_epoch,
                                                fill_epoch);
            FILL_LOG_TRACE("fill others done <<", K(fill_ret), K(ctx));
            if (OB_SUCCESS == fill_ret) {
              // this list is fully filled, continue to fill from others
              next_log_epoch_arr.at(i) = INT64_MAX;
            } else if (FALSE_IT(all_others_reach_tail = false)) {
              // not reach tail
            } else if (OB_ITER_END == fill_ret) {
              // this list is filled with this epoch, but its has remains
              next_log_epoch_arr.at(i) = ctx.next_epoch_;
            } else if (FALSE_IT(last_fail = fill_ret)) {
              // failure occurs, either retryable or fatal
            } else if (OB_BUF_NOT_ENOUGH == fill_ret) {
              next_log_epoch_arr.at(i) = ctx.cur_epoch_;
              ret = fill_ret;
              do_return = true;
            } else if (OB_BLOCK_FROZEN == fill_ret) {
              // blocked, try others
              next_log_epoch_arr.at(i) = ctx.cur_epoch_;
            } else {
              // other error, give up
              TRANS_LOG(WARN, "fill redo from callback-list fail", K(ret), K(i));
              next_log_epoch_arr.at(i) = ctx.cur_epoch_;
              ret = fill_ret;
              do_return = true;
            }
            if (!do_return && is_parallel_logging_() && ctx.not_empty()) {
              ret = fill_ret;
              // when parallel logging, seprate log-entry for each callback-list
              if (OB_SUCCESS == fill_ret) {
                ret = OB_EAGAIN; // return OB_EAGAIN indicate other list has remains
              }
              do_return = true;
            }
          } else {
            // this list is skipped
            all_others_reach_tail = false;
          }
        }
        if (!do_return) {
          if (OB_SUCC(ret) && (OB_SUCCESS == choosen_list_fill_ret) && all_others_reach_tail) {
            // all list reach tail, no need next round
            ctx.is_all_filled_ = true;
            do_return = true;
            FILL_LOG_TRACE("all list filled, remians 0, return now", K(ctx));
          } else if (ctx.fill_count_ - save_fill_count == 0) {
            // no extra filled from other list, no need next round
            ob_assert(last_fail != OB_SUCCESS);
            // if first list is filled without error, should return last fill error
            if (choosen_list_fill_ret == OB_SUCCESS || choosen_list_fill_ret == OB_ITER_END) {
              ret = last_fail;
            } else {
              ret = choosen_list_fill_ret;
            }
            do_return = true;
          } else {
            // go ahead, next round
          }
        }
      }
      FILL_LOG_TRACE("one round is done", K(do_return), K(ctx));
    }
  }
  if (!ctx.is_all_filled_) {
    ob_assert(ret != OB_SUCCESS);
  }
  FILL_LOG_TRACE("done fill from all list", K(list_cnt), K(ctx));
  return ret;
}

inline bool check_dup_tablet_(ObITransCallback *callback_ptr)
{
  bool is_dup_tablet = false;
  int64_t tmp_ret = OB_SUCCESS;

  // If id is a dup table tablet => true
  // If id is not a dup table tablet => false
  if (MutatorType::MUTATOR_ROW == callback_ptr->get_mutator_type()) {
    const ObMvccRowCallback *row_iter = static_cast<const ObMvccRowCallback *>(callback_ptr);
    const ObTabletID &target_tablet = row_iter->get_tablet_id();
    // if (OB_TMP_FAIL(mem_ctx_->get_trans_ctx()->merge_tablet_modify_record_(target_tablet))) {
    //   TRANS_LOG_RET(WARN, tmp_ret, "merge tablet modify record failed", K(tmp_ret),
    //                 K(target_tablet), KPC(row_iter));
    // }
    // check dup table
  }

  return is_dup_tablet;
}

int ObTransCallbackMgr::log_submitted(const ObCallbackScopeArray &callbacks, int &submitted)
{
  int ret = OB_SUCCESS;
  ARRAY_FOREACH(callbacks, i) {
    const ObCallbackScope &scope = callbacks.at(i);
    if (!scope.is_empty()) {
      int cnt = 0;
      ObITransCallbackIterator cursor = scope.start_;
      do {
        ObITransCallback *iter = *cursor;
        if (!iter->need_submit_log()) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(ERROR, "callback no need submit log", K(ret), KPC(iter), KPC(this));
#ifdef ENABLE_DEBUG_LOG
          ob_abort();
#endif
        } else if (OB_FAIL(iter->log_submitted_cb())) {
          TRANS_LOG(ERROR, "fail to log_submitted cb", K(ret), KPC(iter));
#ifdef ENABLE_DEBUG_LOG
          ob_abort();
#endif
        } // check dup table tx
        else if(check_dup_tablet_(iter)) {
          // mem_ctx_->get_trans_ctx()->set_dup_table_tx_();
        }
        ++cnt;
        ++submitted;
      } while (OB_SUCC(ret) && cursor++ != scope.end_);
      if (cnt != scope.cnt_) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "callback scope incorrect", K(ret), "iter_count", cnt, K(scope.cnt_), K(scope), KPC(this));
#ifdef ENABLE_DEBUG_LOG
        ob_abort();
#endif
      }
      if (OB_SUCC(ret)) {
        // update log cursor
        ret = scope.host_->submit_log_succ(callbacks.at(i));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "empty callback scope", K(ret), K(scope), KPC(this));
#ifdef ENABLE_DEBUG_LOG
      ob_abort();
#endif
    }
  }
  return ret;
}

int ObTransCallbackMgr::log_sync_succ(const ObCallbackScopeArray &callbacks,
                                      const share::SCN scn,
                                      int &sync_cnt)
{
  int ret = OB_SUCCESS;
  sync_cnt = 0;
  const bool serial_final = is_serial_final_();
  if (!serial_final) {
    update_serial_sync_scn_(scn);
  }
  ObIMemtable *last_mt = NULL;
  ARRAY_FOREACH(callbacks, i) {
    const ObCallbackScope &scope = callbacks.at(i);
    if (!scope.is_empty()) {
      int cnt = 0;
      ObITransCallbackIterator cursor = scope.start_;
      do {
        ObITransCallback *iter = *cursor;
        //ob_assert(iter->need_fill_redo());
        if (OB_UNLIKELY(iter->need_submit_log())) {
          TRANS_LOG(WARN, "NOTICE: callback has not marked submitted", KPC(iter));
        }
        if (OB_FAIL(iter->log_sync_cb(scn, last_mt))) {
          TRANS_LOG(ERROR, "fail to log_sync cb", K(ret), KPC(iter));
          ob_abort();
        } // check dup table tx
        else if(check_dup_tablet_(iter)) {
          // mem_ctx_->get_trans_ctx()->set_dup_table_tx_();
        }
        //ob_assert(!iter->need_fill_redo());
        ++cnt;
      } while (OB_SUCC(ret) && cursor++ != scope.end_);
      //ob_assert(cnt == scope.cnt_);
      if (OB_SUCC(ret)) {
        if (OB_FAIL(scope.host_->sync_log_succ(scn, scope.cnt_))) {
          TRANS_LOG(ERROR, "sync succ fail", K(ret));
        } else {
          sync_cnt += scope.cnt_;
        }
      }
    } else {
      ob_abort();
    }
  }
  return ret;
}

int ObTransCallbackMgr::log_sync_fail(const ObCallbackScopeArray &callbacks, int &removed_cnt)
{
  int ret = OB_SUCCESS;
  removed_cnt = 0;
  ARRAY_FOREACH(callbacks, i) {
    const ObCallbackScope &scope = callbacks.at(i);
    int64_t rm_cnt = 0;
    if (!scope.is_empty()) {
      if (OB_FAIL(scope.host_->sync_log_fail(scope, rm_cnt))) {
        TRANS_LOG(ERROR, "calblack fail", K(ret));
      } else {
        ob_assert(rm_cnt == scope.cnt_);
        removed_cnt += rm_cnt;
      }
    }
  }
  return ret;
}

// when recover from checkpoint, update checksum info for CallbackList
int ObTransCallbackMgr::update_checksum(const ObIArray<uint64_t> &checksum,
                                        const ObIArray<SCN> &checksum_scn)
{
  int ret = OB_SUCCESS;
  // extend callback list if need, this can only happened since 4.2.4
  if (checksum.count() > 1) {
    OB_ASSERT(checksum.count() == MAX_CALLBACK_LIST_COUNT);
    if (OB_ISNULL(callback_lists_) &&
        OB_FAIL(extend_callback_lists_(MAX_CALLBACK_LIST_COUNT - 1))) {
      TRANS_LOG(WARN, "expand calblack_lists failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    CALLBACK_LISTS_FOREACH(idx, list) {
      list->update_checksum(checksum.at(idx), checksum_scn.at(idx));
    }
  }
  return ret;
}

int64_t ObTransCallbackMgr::inc_pending_log_size(const int64_t size)
{
  int64_t new_size = 0;
  if (!for_replay_ && !is_parallel_logging_()) {
    new_size = ATOMIC_AAF(&pending_log_size_, size);
    int64_t old_size = new_size - size;
    if (OB_UNLIKELY(old_size < 0 || new_size < 0)) {
      TRANS_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "increase remaining data size less than 0!",
                    KPC(this), K(size), K(old_size), K(new_size), KPC(get_trans_ctx()));
    }
  }
  return new_size;
}

void ObTransCallbackMgr::try_merge_multi_callback_lists(const int64_t new_size, const int64_t size, const bool is_logging_blocked)
{
  if (OB_UNLIKELY(need_merge_) && !for_replay_) {
    int64_t old_size = new_size - size;
    if (size < 0 || new_size < 0 || old_size < 0) {
    } else if ((0 != GCONF._private_buffer_size
                && old_size < GCONF._private_buffer_size
                && new_size >= GCONF._private_buffer_size)
               || is_logging_blocked) {
      // merge the multi callback lists once the immediate logging is satisfied.
      merge_multi_callback_lists();
    }
  }
}

void ObTransCallbackMgr::inc_flushed_log_size(const int64_t size) {
  if (!is_parallel_logging_()) {
    ATOMIC_FAA(&flushed_log_size_, size);
  }
}

int ObTransCallbackMgr::get_memtable_key_arr(ObMemtableKeyArray &memtable_key_arr)
{
  int ret = OB_SUCCESS;
  int fail_at = 0;
  if (need_merge_) { // OLD (before 4.2.4)
    ret = callback_list_.get_memtable_key_arr_w_timeout(memtable_key_arr);
    if (OB_ITER_STOP == ret) { ret = OB_SUCCESS; }
  } else { // NEW (since 4.2.4)
    CALLBACK_LISTS_FOREACH(idx, list) {
      fail_at = idx;
      ret = list->get_memtable_key_arr_w_timeout(memtable_key_arr);
      if (OB_ITER_STOP == ret) { ret = OB_SUCCESS; }
    }
  }
  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "get memtablekey fail", K(ret), K(fail_at), K(memtable_key_arr));
  }
  return ret;
}

int ObTransCallbackMgr::acquire_callback_list(const bool new_epoch, const bool need_merge)
{
  int64_t stat = ATOMIC_LOAD(&parallel_stat_);
  int64_t tid = get_itid() + 1;
  if (0 == stat) { // first thread, no parallel
    if (!ATOMIC_BCAS(&parallel_stat_, 0, tid << 32)) {
      ATOMIC_STORE(&parallel_stat_, PARALLEL_STMT);
    }
  } else if (tid == (stat >> 32)) { // same thread nested, no parallel
    ATOMIC_BCAS(&parallel_stat_, stat, stat + 1);
  } else { // has parallel
    //
    ATOMIC_STORE(&parallel_stat_, PARALLEL_STMT);
  }
  // mark callback_list need merge into main
  // this is compatible with version before 4.2.4
  if (ATOMIC_LOAD(&need_merge_) != need_merge) {
    ATOMIC_STORE(&need_merge_, need_merge);
  }
  int slot = 0;
  // inc write_epoch
  // for each write epoch the first thread always stay in slot 0
  // other thread will stay in slot by its offset with first thread
  if (new_epoch) {
    ++write_epoch_;
    write_epoch_start_tid_ = tid;
    slot = 0;
  } else if (tid == write_epoch_start_tid_) {
    slot = 0;
  } else {
    // to ensure slot is positive: (m + (a - b) % m) % m
    slot =  (MAX_CALLBACK_LIST_COUNT + ((tid - write_epoch_start_tid_) % MAX_CALLBACK_LIST_COUNT)) % MAX_CALLBACK_LIST_COUNT;
  }
  return slot;
}

void ObTransCallbackMgr::revert_callback_list()
{
  int64_t stat = ATOMIC_LOAD(&parallel_stat_);
  bool need_merge = ATOMIC_LOAD(&need_merge_);
  const int64_t tid = get_itid() + 1;
  const int slot = tid % MAX_CALLBACK_LIST_COUNT;
  // if no parallel til now, all callbacks in main list, no need merge
  if (tid == (stat >> 32)) {
    if (0 == ref_cnt_) {
      UNUSED(ATOMIC_BCAS(&parallel_stat_, stat, 0));
    } else {
      UNUSED(ATOMIC_BCAS(&parallel_stat_, stat, stat - 1));
    }
    need_merge = false;
  }
  // compatible with before version 4.2.4, merge to main list
  if (need_merge) {
    WRLockGuard guard(rwlock_);
    if (OB_NOT_NULL(callback_lists_)) {
      int64_t cnt = callback_list_.concat_callbacks(callback_lists_[slot]);
      add_slave_list_merge_cnt(cnt);
    }
  }
}

void ObTransCallbackMgr::wakeup_waiting_txns_()
{
  if (OB_ISNULL(MTL(ObLockWaitMgr*))) {
    TRANS_LOG_RET(WARN, OB_ERR_UNEXPECTED, "MTL(ObLockWaitMgr*) is null");
  } else {
    ObMemtableCtx &mem_ctx = static_cast<ObMemtableCtx&>(host_);
    MTL(ObLockWaitMgr*)->wakeup(mem_ctx.get_trans_ctx()->get_trans_id());
  }
}

void ObTransCallbackMgr::set_for_replay(const bool for_replay)
{
  ATOMIC_STORE(&for_replay_, for_replay);
  if (for_replay) {
    reset_pdml_stat();
  }
}

void ObTransCallbackMgr::replay_begin(const bool parallel_replay, share::SCN scn)
{
  UNUSED(scn);
  parallel_replay_ = parallel_replay;
}

int ObTransCallbackMgr::replay_fail(const int16_t callback_list_idx, const SCN scn)
{
  int ret = OB_SUCCESS;
  // if not reach serial replay final, replay maybe in multiple list
  // must try do rollback on all list
  if (callback_list_idx == 0 && !is_serial_final_()) {
    CALLBACK_LISTS_FOREACH(idx, list) {
      ret = list->replay_fail(scn, true/*is serial replayed scn*/);
    }
  } else {
    ObTxCallbackList *list = get_callback_list_(callback_list_idx, true);
    if (OB_ISNULL(list)) {
      // callback_lists is not extended due to replay row is skipped
    } else {
      ret = list->replay_fail(scn, false/*is serial replay scn*/);
    }
  }
  return ret;
}

int ObTransCallbackMgr::replay_succ(const int16_t callback_list_idx, const SCN scn)
{
  // when replay succ, update sync_scn
  int ret = OB_SUCCESS;
  if (callback_list_idx == 0 && !is_serial_final_()) {
    // it's replaying log in tx-log queue, involve multiple callback-list maybe
    update_serial_sync_scn_(scn);
  } else {
    ObTxCallbackList *list = get_callback_list_(callback_list_idx, true);
    if (OB_ISNULL(list)) {
      // callback_lists is not extended due to replay row is skipped
    } else {
      ret = list->replay_succ(scn);
    }
  }
  return ret;
}

int ObTransCallbackMgr::trans_end(const bool commit)
{
  int ret = common::OB_SUCCESS;
  // abort transaction, skip the checksum cacluation
  // which also skip remove callback order contraint checks
  if (!commit) {
    set_skip_checksum_calc();
  }
  if (OB_UNLIKELY(ATOMIC_LOAD(&need_merge_))) { // OLD (before 4.2.4)
    // If the txn ends abnormally, there may still be tasks in execution. Our
    // solution is that before the txn resets, all callback_lists need be
    // cleaned up after blocking new writes (through end_code). So if PDML
    // exists and some data is cached in callback_lists, we need merge them into
    // main callback_list
    merge_multi_callback_lists();
    ret = commit ? callback_list_.tx_commit() : callback_list_.tx_abort();
  } else { // New (since 4.2.4)
    if (OB_LIKELY(ATOMIC_LOAD(&callback_lists_) == NULL)) {
      ret = commit ? callback_list_.tx_commit() : callback_list_.tx_abort();
    } else {
      CALLBACK_LISTS_FOREACH(idx, list) {
        ret = commit ? list->tx_commit() : list->tx_abort();
      }
    }
  }
  if (OB_SUCC(ret)) {
    wakeup_waiting_txns_();
  }
  return ret;
}

int ObTransCallbackMgr::calc_checksum_all(ObSEArray<uint64_t, 1> &checksum)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ATOMIC_LOAD(&need_merge_)) || OB_LIKELY(ATOMIC_LOAD(&callback_lists_) == NULL)) {
    callback_list_.tx_calc_checksum_all();
    ret = checksum.push_back(callback_list_.get_checksum());
  } else {
    CALLBACK_LISTS_FOREACH(idx, list) {
      list->tx_calc_checksum_all();
      ret = checksum.push_back(list->get_checksum());
    };
  }
  return ret;
}

void ObTransCallbackMgr::print_callbacks()
{
  RDLockGuard guard(rwlock_);
  if (need_merge_) {
    callback_list_.tx_print_callback();
  } else {
    int ret = OB_SUCCESS;
    CALLBACK_LISTS_FOREACH(idx, list) {
      _TRANS_LOG(INFO, "print callback at CallbackList[%d]:", idx);
      list->tx_print_callback();
    }
  }
}

int ObTransCallbackMgr::get_callback_list_stat(ObIArray<ObTxCallbackListStat> &stats)
{
  RDLockGuard guard(rwlock_);
  int ret = OB_SUCCESS;
  if (rwlock_.try_rdlock()) {
    if (need_merge_) {
      if (OB_SUCC(stats.prepare_allocate(1))) {
        ret = callback_list_.get_stat_for_display(stats.at(0));
      }
    } else if (OB_SUCC(stats.prepare_allocate(get_callback_list_count()))) {
      CALLBACK_LISTS_FOREACH(idx, list) {
        if (list->get_appended() > 0) {
          ret = list->get_stat_for_display(stats.at(idx));
        } else {
          stats.at(idx).id_ = -1; // mark as invalid
        }
      }
    }
    rwlock_.unlock();
  }
  return ret;
}

void ObTransCallbackMgr::elr_trans_preparing()
{
  if (ATOMIC_LOAD(&need_merge_)) {
    callback_list_.tx_elr_preparing();
  } else {
    int ret = OB_SUCCESS;
    CALLBACK_LISTS_FOREACH(idx, list) {
      list->tx_elr_preparing();
    }
  }
}

void ObTransCallbackMgr::elr_trans_revoke()
{
  if (ATOMIC_LOAD(&need_merge_)) {
    callback_list_.tx_elr_revoke();
  } else {
    int ret = OB_SUCCESS;
    CALLBACK_LISTS_FOREACH(idx, list) {
      list->tx_elr_revoke();
    }
  }
}


void ObTransCallbackMgr::trans_start()
{
  reset();
}

int ObMvccRowCallback::before_append(const bool is_replay)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(memtable_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "memtable is NULL", K(ret));
  } else if (!is_replay) {
    inc_unsubmitted_cnt_();
    inc_unsynced_cnt_();
  }

  return ret;
}

int ObMvccRowCallback::log_submitted()
{
  int ret = OB_SUCCESS;

  if (OB_NOT_NULL(memtable_)) {

    if (OB_FAIL(dec_unsubmitted_cnt_())) {
      TRANS_LOG(ERROR, "dec unsubmitted cnt failed", K(ret), K(*this));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "memtable is NULL", K(ret));
  }
  return ret;
}

bool ObMvccRowCallback::is_logging_blocked() const
{
  const bool is_blocked = memtable_->get_logging_blocked();
  if (is_blocked) {
    int ret = OB_SUCCESS;
    ObTransID trans_id;
    if (OB_FAIL(get_trans_id(trans_id))) {
      TRANS_LOG(WARN, "fail to get trans_id", K(ret));
    } else if (REACH_TIME_INTERVAL(1000000)) {
      TRANS_LOG(WARN, "block logging", K_(epoch), K(is_blocked), KP(memtable_),
                K(memtable_->get_key().get_tablet_id()), K(trans_id));
    }
  }
  return is_blocked;
}

uint32_t ObMvccRowCallback::get_freeze_clock() const
{
  if (OB_ISNULL(memtable_)) {
    TRANS_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "mvcc row memtable is NULL", KPC(this));
    return 0;
  } else {
    return memtable_->get_freeze_clock();
  }
}

int ObMvccRowCallback::clean()
{
  unlink_trans_node();
  return OB_SUCCESS;
}

int ObMvccRowCallback::del()
{
  int ret = OB_SUCCESS;

  if (NULL != old_row_.data_) {
    ctx_.old_row_free((void *)(old_row_.data_));
    old_row_.data_ = NULL;
  }

  if (need_submit_log_ && need_fill_redo_) {
    log_submitted();
  }
  if (need_fill_redo_) {
    dec_unsynced_cnt_();
  }

  ret = remove();
  return ret;
}

int ObMvccRowCallback::get_memtable_key(uint64_t &table_id, ObStoreRowkey &rowkey) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(key_.decode(rowkey))) {
    TRANS_LOG(WARN, "memtable key decode failed", K(ret));
  }
  return ret;
}

const common::ObTabletID &ObMvccRowCallback::get_tablet_id() const
{
  return memtable_->get_key().get_tablet_id();
}

bool ObMvccRowCallback::on_memtable(const ObIMemtable * const memtable)
{
  return memtable == memtable_;
}

ObIMemtable *ObMvccRowCallback::get_memtable() const
{
  return memtable_;
};

int ObMvccRowCallback::print_callback()
{
  ObRowLatchGuard guard(value_.latch_);

  TRANS_LOG(INFO, "print callback", K(*this));
  return OB_SUCCESS;
}

int ObMvccRowCallback::merge_memtable_key(ObMemtableKeyArray &memtable_key_arr,
    ObMemtableKey &memtable_key, const common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;

  int64_t count = memtable_key_arr.count();
  int64_t i = 0;
  for (; i < count; i++) {
    // XXX maybe
    if (memtable_key_arr.at(i).get_tablet_id() == tablet_id &&
        memtable_key_arr.at(i).get_hash_val() == memtable_key.hash()) {
      break;
    }
  }
  if (i == count) {
    ObMemtableKeyInfo memtable_key_info;
    if (OB_FAIL(memtable_key_info.init(memtable_key.hash()))) {
      TRANS_LOG(WARN, "memtable key info init fail", K(ret));
    } else {
      memtable_key_info.set_tablet_id(tablet_id);
      memtable_key.to_string(memtable_key_info.get_buf(), ObMemtableKeyInfo::MEMTABLE_KEY_INFO_BUF_SIZE);
      if (OB_FAIL(memtable_key_arr.push_back(memtable_key_info))) {
        TRANS_LOG(WARN, "memtable_key_arr push item fail", K(ret), K(memtable_key_arr), K(memtable_key_info));
      }
    }
  }

  return ret;
}

int ObMvccRowCallback::merge_memtable_key(ObMemtableKeyArray &memtable_key_arr)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(key_.get_rowkey())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "static_cast key to ObMemtableKey* error", K(ret), "context", *this);
  } else if (OB_ISNULL(memtable_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "ObMvccRowCallback's memtable_ ptr is NULL", K(ret), "context", *this);
  } else if (OB_FAIL(merge_memtable_key(memtable_key_arr, key_, memtable_->get_key().get_tablet_id()))) {
    TRANS_LOG(WARN, "memtable_key_arr push item fail", K(ret), K(key_));
  } else {
    //do nothing
  }

  return ret;
}

int ObMvccRowCallback::elr_trans_preparing()
{
  ObRowLatchGuard guard(value_.latch_);

  ObMemtableCtx *mem_ctx = static_cast<ObMemtableCtx*>(&ctx_);
  if (NULL != tnode_) {
    value_.elr(mem_ctx->get_trans_ctx()->get_trans_id(),
               ctx_.get_commit_version(),
               get_tablet_id(),
               (ObMemtableKey*)&key_,
               is_non_unique_local_index_cb_);
  }
  return OB_SUCCESS;
}

void ObMvccRowCallback::elr_trans_revoke()
{
  if (OB_NOT_NULL(tnode_)) {
    tnode_->clear_elr();
  }
}

int ObMvccRowCallback::get_trans_id(ObTransID &trans_id) const
{
  int ret = OB_SUCCESS;
  ObMemtableCtx *mem_ctx = static_cast<ObMemtableCtx*>(&ctx_);
  ObTransCtx *trans_ctx = NULL;

  if (OB_ISNULL(mem_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "unexpected mem ctx", K(ret));
  } else if (OB_ISNULL(trans_ctx = mem_ctx->get_trans_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "unexpected trans ctx", K(ret), K(ctx_));
  } else {
    trans_id = trans_ctx->get_trans_id();
  }

  return ret;
}

int ObMvccRowCallback::get_cluster_version(uint64_t &cluster_version) const
{
  int ret = OB_SUCCESS;
  ObMemtableCtx *mem_ctx = static_cast<ObMemtableCtx*>(&ctx_);
  ObTransCtx *trans_ctx = NULL;
  if (OB_ISNULL(mem_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "unexpected mem ctx", K(ret));
  } else if (OB_ISNULL(trans_ctx = mem_ctx->get_trans_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "unexpected trans ctx", K(ret), K(ctx_));
  } else {
    cluster_version = trans_ctx->get_cluster_version();
  }
  return ret;
}

ObPartTransCtx *ObMvccRowCallback::get_trans_ctx() const
{
  int ret = OB_SUCCESS;
  ObMemtableCtx *mem_ctx = static_cast<ObMemtableCtx*>(&ctx_);
  ObPartTransCtx *trans_ctx = NULL;

  if (OB_ISNULL(mem_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "unexpected mem ctx", K(ret));
  } else if (OB_ISNULL(trans_ctx = mem_ctx->get_trans_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "unexpected trans ctx", K(ret), K(ctx_));
  } else {
    // do nothing
  }

  return trans_ctx;
}

int ObMvccRowCallback::calc_checksum(const SCN checksum_scn,
                                     TxChecksum *checksumer)
{
  ObRowLatchGuard guard(value_.latch_);

  if (NULL != tnode_) {
    if (not_calc_checksum_) {
      // verification
      if (blocksstable::ObDmlFlag::DF_LOCK != get_dml_flag()) {
        TRANS_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "only LOCK node can not calc checksum",
                  K(*this), K(get_dml_flag()));
      }
    } else if (checksum_scn <= scn_) {
      tnode_->checksum(*checksumer);
      ((ObMemtableDataHeader *)tnode_->buf_)->checksum(*checksumer);
      checksumer->cnt_++;
      checksumer->scn_ = scn_;
    }
  }

  return OB_SUCCESS;
}

struct BeforeSetDelayCleanOutOp {
  BeforeSetDelayCleanOutOp(ObTabletID tablet_id,
                           const memtable::ObMemtableKey &memtable_key,
                           memtable::ObMvccTransNode &trans_node,
                           ObPartTransCtx *trans_ctx,
                           bool is_non_unique_local_index_cb)
  : tablet_id_(tablet_id),
  memtable_key_(memtable_key),
  trans_node_(trans_node),
  trans_ctx_(trans_ctx),
  is_non_unique_local_index_cb_(is_non_unique_local_index_cb) {}
  void operator()() {
    ObAddr tx_scheduler;
    int ret = OB_SUCCESS;
    if (OB_ISNULL(trans_ctx_)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "trans ctx is null", KR(ret), KPC(this));
    } else {
      tx_scheduler = trans_ctx_->get_scheduler();
      (void) MTL(ObLockWaitMgr*)->transform_row_lock_to_tx_lock(tablet_id_,
                                                                memtable_key_,
                                                                trans_node_.tx_id_,
                                                                tx_scheduler);
      if (OB_LIKELY(ObDeadLockDetectorMgr::is_deadlock_enabled() && !is_non_unique_local_index_cb_)) {
        CorrectHashHolderOp correct_hash_hodler_op(LockHashHelper::hash_rowkey(tablet_id_, memtable_key_),
                                                   trans_node_,
                                                   MTL(ObLockWaitMgr*)->get_row_holder());
        correct_hash_hodler_op();
      }
    }
  }
  TO_STRING_KV(K_(tablet_id), K_(memtable_key), K_(trans_node), KPC_(trans_ctx));
private:
  ObTabletID tablet_id_;
  const memtable::ObMemtableKey &memtable_key_;
  memtable::ObMvccTransNode &trans_node_;
  ObPartTransCtx *trans_ctx_;
  bool is_non_unique_local_index_cb_;
};
int ObMvccRowCallback::checkpoint_callback()
{
  int ret = OB_SUCCESS;

  ObRowLatchGuard guard(value_.latch_);

  if (need_submit_log_ || need_fill_redo_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "checkpoint never called on submitted callback", KPC(this));
  } else if (OB_NOT_NULL(tnode_)) {
    BeforeSetDelayCleanOutOp op(get_tablet_id(),
                                *get_key(),
                                *get_trans_node(),
                                get_trans_ctx(),
                                is_non_unique_local_index_cb_);
    tnode_->set_delayed_cleanout(op);
    (void)value_.update_dml_flag_(get_dml_flag(), tnode_->get_scn());
  }

  return ret;
}

void ObMvccRowCallback::after_append_fail(const bool is_replay)
{
  if (!is_replay) {
    dec_unsubmitted_cnt_();
    dec_unsynced_cnt_();
  }
}

static blocksstable::ObDmlFlag get_dml_flag(ObMvccTransNode *node)
{
  return NULL == node ? blocksstable::ObDmlFlag::DF_NOT_EXIST : reinterpret_cast<ObMemtableDataHeader *>(node->buf_)->dml_flag_;
}

blocksstable::ObDmlFlag ObMvccRowCallback::get_dml_flag() const
{
  return memtable::get_dml_flag(tnode_);
}

void ObMvccRowCallback::commit_trans_node_() {
  if (OB_LIKELY(ObDeadLockDetectorMgr::is_deadlock_enabled() && !is_non_unique_local_index_cb_)) {
    CorrectHashHolderOp op(LockHashHelper::hash_rowkey(get_tablet_id(), key_),
                           *tnode_,
                           MTL(ObLockWaitMgr*)->get_row_holder());
    tnode_->trans_commit(ctx_.get_commit_version(), ctx_.get_tx_end_scn(), op);
  } else {
    DummyHashHolderOp op;
    tnode_->trans_commit(ctx_.get_commit_version(), ctx_.get_tx_end_scn(), op);
  }
}
int ObMvccRowCallback::trans_commit()
{
  int ret = OB_SUCCESS;
  ObMvccTransNode *prev = NULL;
  ObMvccTransNode *next = NULL;
  const bool for_read = false;

  ObRowLatchGuard guard(value_.latch_);

  if (NULL != tnode_) {
    if (OB_FAIL(link_and_get_next_node(next))) {
      TRANS_LOG(WARN, "link trans node failed", K(ret));
    } else {
      // if (ctx_.is_for_replay()) {
      //   // verify current node checksum by previous node
      //   prev = tnode_->prev_;
      //   if (not_calc_checksum_) {
      //     // to fix the case of replay self written log
      //     // do nothing
      //   } else if (NULL == prev) {
      //     // do nothing
      //   } else if (prev->is_committed() &&
      //       prev->version_ == tnode_->version_ &&
      //       prev->modify_count_ + 1 == tnode_->modify_count_) {
      //     if (OB_FAIL(tnode_->verify_acc_checksum(prev->acc_checksum_))) {
      //       TRANS_LOG(ERROR, "current row checksum error", K(ret), K(value_), K(*prev), K(*tnode_));
      //       if (ObServerConfig::get_instance().ignore_replay_checksum_error) {
      //         // rewrite ret
      //         ret = OB_SUCCESS;
      //       }
      //     }
      //   } else {
      //     // do nothing
      //   }
      //   if (OB_SUCC(ret)) {
      //     // verify next node checksum by current node
      //     if (not_calc_checksum_) {
      //       // to fix the case of replay self log
      //       // do thing
      //     } else if (NULL == next) {
      //       // do nothing
      //     } else if (next->is_committed() &&
      //         tnode_->version_ == next->version_ &&
      //         tnode_->modify_count_ + 1 == next->modify_count_) {
      //       if (OB_FAIL(next->verify_acc_checksum(tnode_->acc_checksum_))) {
      //         TRANS_LOG(ERROR, "next row checksum error", K(ret), K(value_), K(*tnode_), K(*next));
      //         if (ObServerConfig::get_instance().ignore_replay_checksum_error) {
      //           // rewrite ret
      //           ret = OB_SUCCESS;
      //         }
      //       }
      //     } else {
      //       // do nothing
      //     }
      //   }
      // }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(value_.trans_commit(ctx_.get_commit_version(), *tnode_))) {
          TRANS_LOG(WARN, "mvcc trans ctx trans commit error", K(ret), K_(ctx), K_(value));
        } else if (FALSE_IT(commit_trans_node_())) {
        } else if (FALSE_IT(wakeup_row_waiter_if_need_())) {
        } else if (blocksstable::ObDmlFlag::DF_LOCK == get_dml_flag()) {
          unlink_trans_node();
        } else {
          const int64_t MAX_TRANS_NODE_CNT = 2 * GCONF._ob_elr_fast_freeze_threshold;
          if (value_.total_trans_node_cnt_ > MAX_TRANS_NODE_CNT
              && NULL != memtable_
              && !memtable_->has_hotspot_row()) {
            memtable_->set_contain_hotspot_row();
            TRANS_LOG(INFO, "[FF] trans commit and set hotspot row success", K_(*memtable), K_(value), K_(ctx), K(*this));
          }
          (void)ATOMIC_FAA(&value_.update_since_compact_, 1);
          if (value_.need_compact(for_read, ctx_.is_for_replay())) {
            if (ctx_.is_for_replay()) {
              if (ctx_.get_replay_compact_version().is_valid_and_not_min()
                  && SCN::max_scn() != ctx_.get_replay_compact_version()) {
                memtable_->row_compact(&value_,
                                       ctx_.get_replay_compact_version(),
                                       ObMvccTransNode::WEAK_READ_BIT
                                       | ObMvccTransNode::COMPACT_READ_BIT);
              }
            } else {
              SCN snapshot_version_for_compact = SCN::minus(SCN::max_scn(), 100);
              memtable_->row_compact(&value_,
                                     snapshot_version_for_compact,
                                     ObMvccTransNode::NORMAL_READ_BIT);
            }
          }
        }
      }
    }
  }
  return ret;
}

/*
 * wakeup_row_waiter_if_need_ - wakeup txn waiting to acquire row ownership
 *
 * The 'Row-Lock' is imply by active txn's dirty write
 * if current active txn aborted, committed or rollback to savepoint, its dirty
 * write maybe discard or be invalid, in these situations we should wakeup
 * any waiters who wait on the row
 *
 * to verify the current active txn on current row 'released ownership' actually,
 * we use these conditions:
 * 1) TxNode is in a determinated state: COMMITTED or ABORTED
 * 2) precedure of this TxNode is not owned by current txn
 *
 * however, this may cause false positive (which means the lock was not release
 * by this txn actually). but it is better to accept such ratio in order to
 * keep simple and fast
 */
int ObMvccRowCallback::wakeup_row_waiter_if_need_()
{
  int ret = OB_SUCCESS;
  if (NULL != tnode_
    && (tnode_->is_committed() || tnode_->is_aborted()) // tnode trans end
    && !tnode_->is_elr() // no need for elr trx
    && tnode_->next_ == NULL // latest trans node
    && (tnode_->prev_ == NULL || (tnode_->prev_->is_committed() // pre node status should be decided
                               || tnode_->prev_->is_aborted()
                               || tnode_->prev_->is_elr()))) {
    // wake up lock waiter, handled by the latest trans node
    //   case 1: for normal transaction commit or abort
    //   case 2: for rollback to savepoint, to avoid extra wake-up,
    //   latest trans node's previous trans node should be committed/aborted/elr/NULL
    // no need to wake up:
    //   case 1: elr transaction
    ret = value_.wakeup_waiter(get_tablet_id(), key_);
  }
  return ret;
}

int ObMvccRowCallback::trans_abort()
{
  ObRowLatchGuard guard(value_.latch_);

  if (NULL != tnode_) {
    if (!(tnode_->is_committed() || tnode_->is_aborted())) {
      if (OB_LIKELY(ObDeadLockDetectorMgr::is_deadlock_enabled() && !is_non_unique_local_index_cb_)) {
        CorrectHashHolderOp op(LockHashHelper::hash_rowkey(get_tablet_id(), key_),
                              *tnode_,
                              MTL(ObLockWaitMgr*)->get_row_holder());
        tnode_->trans_abort(ctx_.get_tx_end_scn(), op);
      } else {
        DummyHashHolderOp op;
        tnode_->trans_abort(ctx_.get_tx_end_scn(), op);
      }
      wakeup_row_waiter_if_need_();
      unlink_trans_node();
    } else if (tnode_->is_committed()) {
      TRANS_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "abort on a committed node", K(*this));
    }
  }
  return OB_SUCCESS;
}

int ObMvccRowCallback::rollback_callback()
{
  ObRowLatchGuard guard(value_.latch_);

  if (NULL != tnode_) {
    if (OB_LIKELY(ObDeadLockDetectorMgr::is_deadlock_enabled() && !is_non_unique_local_index_cb_)) {
      CorrectHashHolderOp op(LockHashHelper::hash_rowkey(get_tablet_id(), key_),
                             *tnode_,
                             MTL(ObLockWaitMgr*)->get_row_holder());
      tnode_->trans_rollback(op);
    } else {
      DummyHashHolderOp op;
      tnode_->trans_rollback(op);
    }
    wakeup_row_waiter_if_need_();
    unlink_trans_node();
  }

  if (need_submit_log_
      && need_fill_redo_
      && SCN::max_scn() == scn_) {
    ctx_.inc_pending_log_size(-1 * data_size_);
  }

  return OB_SUCCESS;
}

MutatorType ObMvccRowCallback::get_mutator_type() const
{
  return MutatorType::MUTATOR_ROW;
}

int ObMvccRowCallback::get_redo(RedoDataNode &redo_node)
{
  int ret = OB_SUCCESS;

  ObRowLatchGuard guard(value_.latch_);

  if (NULL == key_.get_rowkey() || NULL == tnode_) {
    ret = OB_ENTRY_NOT_EXIST;
  } else if (!is_link_) {
    ret = OB_STATE_NOT_MATCH;
    TRANS_LOG(ERROR, "get_redo: trans_nod not link", K(ret), K(*this));
  } else {
    uint32_t last_acc_checksum = 0;
    if (NULL != tnode_->prev_) {
      last_acc_checksum = tnode_->prev_->acc_checksum_;
    } else {
      last_acc_checksum = 0;
    }
    tnode_->cal_acc_checksum(last_acc_checksum);
    const ObMemtableDataHeader *mtd = reinterpret_cast<const ObMemtableDataHeader *>(tnode_->buf_);
    ObRowData new_row;
    new_row.set(mtd->buf_, (int32_t)(data_size_ - sizeof(*mtd)));
    redo_node.set(&key_,
                  old_row_,
                  new_row,
                  mtd->dml_flag_,
                  tnode_->modify_count_,
                  tnode_->acc_checksum_,
                  tnode_->version_,
                  0,
                  seq_no_,
                  this->get_tablet_id(),
                  column_cnt_);
    redo_node.set_callback(this);
  }
  return ret;
}

int ObMvccRowCallback::link_and_get_next_node(ObMvccTransNode *&next)
{
  int ret = OB_SUCCESS;
  if (NULL == tnode_) {
    // pass
  } else if (is_link_) {
    // pass
  } else {
    if (OB_ISNULL(memtable_)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "memtable_ is NULL", K(ret), K_(ctx));
    } else if (OB_FAIL(value_.insert_trans_node(ctx_, *tnode_, memtable_->get_allocator(), next))) {
      TRANS_LOG(ERROR, "insert trans node failed", K(ret), K_(ctx));
    } else {
      is_link_ = true;
    }
  }
  return ret;
}

int ObMvccRowCallback::link_trans_node()
{
  bool ret = OB_SUCCESS;
  ObMvccTransNode *unused = NULL;
  ret = link_and_get_next_node(unused);
  return ret;
}

void ObMvccRowCallback::unlink_trans_node()
{
  int ret = OB_SUCCESS;
  if (is_link_) {
    if (NULL == tnode_) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "trans node is NULL", K(ret), K_(ctx), K_(value));
      // trans node of LOCK type is allowed to be unlinked even after committed
    } else if (tnode_->is_committed() && blocksstable::ObDmlFlag::DF_LOCK != get_dml_flag()) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "unlink committed trans node", K(ret), K_(ctx), K_(value), K(*tnode_));
    } else if (OB_FAIL(value_.unlink_trans_node(*tnode_))) {
      // TODO(handora.qc): temproary remove it
      // TRANS_LOG(ERROR, "unlink trans node failed", K(ret), K_(ctx), K_(value), K(*tnode_));
    } else {
      is_link_ = false;
    }
  }
}

int ObMvccRowCallback::row_delete()
{
  return del();
}

int64_t ObMvccRowCallback::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "[this=%p, ctx=", this);
  databuff_printf(buf, buf_len, pos, ctx_);
  databuff_printf(buf, buf_len, pos, ", is_link=%d, need_submit_log=%d, need_fill_redo=%d, value=",
      is_link_, need_submit_log_, need_fill_redo_);
  databuff_printf(buf, buf_len, pos, value_);
  databuff_printf(buf, buf_len, pos, ", tnode=(");
  databuff_printf(buf, buf_len, pos, tnode_);
  databuff_printf(buf, buf_len, pos, "), seq_no=%ld, memtable=%p, scn=",
      seq_no_.cast_to_int(), memtable_);
  databuff_printf(buf, buf_len, pos, scn_);
  return pos;
}

int ObMvccRowCallback::log_sync(const SCN scn, ObIMemtable *&last_mt)
{
  int ret = OB_SUCCESS;

  if (last_mt != memtable_) {
    memtable_->set_rec_scn(scn);
    memtable_->set_max_end_scn(scn);
    last_mt = memtable_;
  }
  (void)tnode_->fill_scn(scn);
  if (OB_FAIL(dec_unsynced_cnt_())) {
    TRANS_LOG(ERROR, "memtable dec unsynced cnt error", K(ret), K(scn),
              K(memtable_->get_unsynced_cnt()));
  } else {
    // do nothing
  }

  return ret;
}

int ObMvccRowCallback::log_sync_fail()
{
  int ret = OB_SUCCESS;

  ObRowLatchGuard guard(value_.latch_);

  if (OB_FAIL(dec_unsynced_cnt_())) {
    TRANS_LOG(ERROR, "memtable dec unsynced cnt error", K(ret),
              K(memtable_->get_unsynced_cnt()));
  } else {
    unlink_trans_node();
  }

  return ret;
}

void ObMvccRowCallback::inc_unsubmitted_cnt_()
{
  if (OB_NOT_NULL(memtable_)) {
    memtable_->inc_unsubmitted_cnt();
    ctx_.inc_unsubmitted_cnt();
  }
}

void ObMvccRowCallback::inc_unsynced_cnt_()
{
  if (OB_NOT_NULL(memtable_)) {
    memtable_->inc_unsynced_cnt();
    ctx_.inc_unsynced_cnt();
  }
}

int ObMvccRowCallback::dec_unsubmitted_cnt_()
{
  int ret = OB_SUCCESS;

  if (OB_NOT_NULL(memtable_)) {
    ret = memtable_->dec_unsubmitted_cnt();
    ctx_.dec_unsubmitted_cnt();
  }

  return ret;
}

int ObMvccRowCallback::dec_unsynced_cnt_()
{
  int ret = OB_SUCCESS;

  if (OB_NOT_NULL(memtable_)) {
    ret = memtable_->dec_unsynced_cnt();
    ctx_.dec_unsynced_cnt();
  }

  return ret;
}

MutatorType ObITransCallback::get_mutator_type() const
{
  return MutatorType::MUTATOR_ROW;
}

void ObTransCallbackMgr::print_statistics(char *buf, const int64_t buf_len, int64_t &pos) const
{
  common::databuff_printf(buf, buf_len, pos,
                          "callback_list:{"
                          "cnt=%d need_merge=%d "
                          "stat:[",
                          get_callback_list_count(),
                          need_merge_);
  if (need_merge_) {
    common::databuff_printf(buf, buf_len, pos,
                            "main=%ld, slave=%ld, merge=%ld, ",
                            get_callback_main_list_append_count(),
                            get_callback_slave_list_append_count(),
                            get_callback_slave_list_merge_count());
  }
  common::databuff_printf(buf, buf_len, pos,
                          "tx_end=%ld, rollback_to=%ld, "
                          "fast_commit=%ld, remove_memtable=%ld, ext_info_log_cb=%ld]",
                          get_callback_remove_for_trans_end_count(),
                          get_callback_remove_for_rollback_to_count(),
                          get_callback_remove_for_fast_commit_count(),
                          get_callback_remove_for_remove_memtable_count(),
                          get_callback_ext_info_log_count());
  if (!need_merge_) {
    common::databuff_printf(buf, buf_len, pos,
    " detail:[(log_epoch,length,logged,synced,appended,removed,unlog_removed,branch_removed)|");
    int ret = OB_SUCCESS;
    CALLBACK_LISTS_FOREACH_CONST(idx, list) {
      int64_t a = list->get_length(),
        b = list->get_logged(),
        c = list->get_synced(),
        d = list->get_appended(),
        e = list->get_removed(),
        f = list->get_unlog_removed(),
        g = list->get_branch_removed();
      if (a || b || c || d || e || f || g) {
        int64_t log_epoch = list->get_log_epoch();
        log_epoch = log_epoch == INT64_MAX ? -1 : log_epoch;
        common::databuff_printf(buf, buf_len, pos, "%d:(%ld,%ld,%ld,%ld,%ld,%ld,%ld,%ld)|", idx, log_epoch, a, b, c, d, e, f, g);
      }
    }
    common::databuff_printf(buf, buf_len, pos, "]}");
  }
}

bool ObTransCallbackMgr::find(ObITxCallbackFinder &func)
{
  bool found = false;
  int ret = OB_SUCCESS;
  CALLBACK_LISTS_FOREACH(idx, list) {
    if (list->find(func)) {
      found = true;
      ret = OB_ITER_END;
    }
  }
  return found;
}

inline
ObTxCallbackList *ObTransCallbackMgr::get_callback_list_(const int16_t index, const bool nullable)
{
  if (index == 0) {
    return &callback_list_;
  }
  if (callback_lists_) {
    OB_ASSERT(index < MAX_CALLBACK_LIST_COUNT);
    return &callback_lists_[index - 1];
  } else if (!nullable) {
    TRANS_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "callback list is null", K(index));
#ifdef ENABLE_DEBUG_LOG
    ob_abort();
#endif
  }
  return NULL;
}

__attribute__((noinline))
int ObTransCallbackMgr::get_logging_list_count() const
{
  return (!need_merge_ && callback_lists_) ? MAX_CALLBACK_LIST_COUNT : 1;
}

bool ObTransCallbackMgr::pending_log_size_too_large(const transaction::ObTxSEQ &write_seq_no,
                                                    const int64_t limit)
{
  if (OB_UNLIKELY(is_parallel_logging_())) {
    ObTxCallbackList *list = get_callback_list_(write_seq_no.get_branch() % MAX_CALLBACK_LIST_COUNT, true);
    return list ? list->pending_log_too_large(limit) : 0;
  } else {
    return ATOMIC_LOAD(&pending_log_size_) > limit;
  }
}

void ObTransCallbackMgr::set_parallel_logging(const share::SCN serial_final_scn,
                                              const transaction::ObTxSEQ serial_final_seq_no)
{
  serial_final_scn_.atomic_set(serial_final_scn);
  serial_final_seq_no_.atomic_store(serial_final_seq_no);
}

void ObTransCallbackMgr::update_serial_sync_scn_(const share::SCN scn)
{
  // push all callback list's sync scn up to at least serial final scn
  // transform to append only replay mode
  serial_sync_scn_.atomic_store(scn);
  if (serial_sync_scn_ == serial_final_scn_) {
    RDLockGuard guard(rwlock_);
    int ret = OB_SUCCESS;
    CALLBACK_LISTS_FOREACH(idx, list) {
      list->inc_update_sync_scn(scn);
    }
  }
}

void ObTransCallbackMgr::set_skip_checksum_calc()
{
  ATOMIC_STORE(&skip_checksum_, true);
}

bool ObTransCallbackMgr::is_logging_blocked(bool &has_pending_log) const
{
  int ret = OB_SUCCESS;
  bool all_blocked = false;
  RDLockGuard guard(rwlock_);
  if (!for_replay_) {
    CALLBACK_LISTS_FOREACH_CONST(idx, list) {
      if (list->has_pending_log()) {
        has_pending_log = true;
        if (list->is_logging_blocked()) {
          all_blocked = true;
        } else {
          all_blocked = false;
          break;
        }
      }
    }
  }
  return all_blocked;
}

void ObTransCallbackMgr::check_all_redo_flushed()
{
  bool ok = true;
  int ret = OB_SUCCESS;
  CALLBACK_LISTS_FOREACH(idx, list) {
    ok &= list->check_all_redo_flushed(false/*quite*/);
  }
  if (!ok) {
    TRANS_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "has redo not flushed", KPC(this));
#ifdef ENABLE_DEBUG_LOG
    ob_abort();
#endif
  }
}

int64_t ObTransCallbackMgr::get_pending_log_size() const
{
  if (!is_parallel_logging_()) {
    return ATOMIC_LOAD(&pending_log_size_);
  } else {
    int64_t size = 0;
    int ret = OB_SUCCESS;
    CALLBACK_LISTS_FOREACH_CONST(idx, list) {
      size += list->get_pending_log_size();
    }
    return size;
  }
}

int64_t ObTransCallbackMgr::get_flushed_log_size() const
{
  if (!is_parallel_logging_()) {
    return ATOMIC_LOAD(&flushed_log_size_);
  } else {
    int64_t size = 0;
    int ret = OB_SUCCESS;
    CALLBACK_LISTS_FOREACH_CONST(idx, list) {
      size += list->get_logged_data_size();
    }
    return size;
  }
}

}; // end namespace mvcc
}; // end namespace oceanbase

