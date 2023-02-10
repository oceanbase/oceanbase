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

#include "storage/tablelock/ob_lock_memtable.h"
#include "share/ob_define.h"
#include "share/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "storage/compaction/ob_tablet_merge_task.h" // ObTabletMergeDagParam
#include "storage/tablelock/ob_table_lock_iterator.h"
#include "storage/ls/ob_ls.h"                        // ObLS
#include "storage/ls/ob_freezer.h"                   // ObFreezer
#include "storage/ob_i_store.h"                      // ObStoreCtx
#include "storage/memtable/ob_lock_wait_mgr.h"
#include "storage/memtable/ob_memtable_context.h"    // ObMemtableCtx
#include "storage/tablelock/ob_mem_ctx_table_lock.h"
#include "storage/tablelock/ob_table_lock_common.h"
#include "storage/tablelock/ob_table_lock_rpc_struct.h"
#include "storage/tx/ob_trans_define.h"
#include "storage/tx/ob_trans_part_ctx.h"
#include "storage/compaction/ob_schedule_dag_func.h"

namespace oceanbase
{
using namespace share;
using namespace storage;
using namespace memtable;
using namespace common;

namespace transaction
{
namespace tablelock
{

ObLockMemtable::ObLockMemtable()
  : ObIMemtable(),
    is_inited_(false),
    ls_id_(),
    freeze_log_ts_(0),
    flushed_log_ts_(0),
    rec_log_ts_(INT64_MAX),
    max_committed_log_ts_(OB_INVALID_TIMESTAMP),
    is_frozen_(false),
    freezer_(nullptr)
{
}

ObLockMemtable::~ObLockMemtable()
{
  reset();
}

int ObLockMemtable::init(
    const ObITable::TableKey &table_key,
    const ObLSID &ls_id,
    ObFreezer *freezer)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init ObLockMemtable twice", KR(ret), K(ls_id));
  } else if (OB_ISNULL(freezer)
            || !ls_id.is_valid()
            || !table_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id), K(freezer), K(table_key));
  } else {
    if (OB_FAIL(ObITable::init(table_key))) {
      LOG_WARN("ObITable::init fail", K(ret), K(table_key));
    } else if (OB_FAIL(obj_lock_map_.init())) {
      LOG_WARN("lock map mgr init failed.", K(ret));
    } else {
      ls_id_ = ls_id;
      freezer_ = freezer;
      is_inited_ = true;
      LOG_INFO("ObLockMemtable init successfully", K(ls_id), K(table_key), K(this));
    }
    if (OB_FAIL(ret)) {
      reset();
    }
  }
  return ret;
}

void ObLockMemtable::reset()
{
  rec_log_ts_ = INT64_MAX;
  max_committed_log_ts_ = OB_INVALID_TIMESTAMP;
  ls_id_.reset();
  ObITable::reset();
  obj_lock_map_.reset();
  freeze_log_ts_ = 0;
  flushed_log_ts_ = 0;
  is_frozen_ = false;
  freezer_ = nullptr;
  is_inited_ = false;
}

// RX + S = RX + SRX  | S + RX = S + SRX
OB_INLINE static void lock_upgrade(const ObTableLockMode &lock_mode_in_same_trans,
                                   ObTableLockOp &lock_op)
{
  if (((lock_mode_in_same_trans & ROW_EXCLUSIVE) &&
       !(lock_mode_in_same_trans & SHARE) &&
       lock_op.lock_mode_ == SHARE)
    || ((lock_mode_in_same_trans & SHARE) &&
       !(lock_mode_in_same_trans & ROW_EXCLUSIVE) &&
       lock_op.lock_mode_ == ROW_EXCLUSIVE)) {
    lock_op.lock_mode_ = SHARE_ROW_EXCLUSIVE;
  }
}

int ObLockMemtable::lock_(
    const ObLockParam &param,
    ObStoreCtx &ctx,
    ObTableLockOp &lock_op)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObMemtableCtx *mem_ctx = NULL;
  bool lock_exist = false;
  ObTableLockMode lock_mode_in_same_trans = 0x0;
  ObLockStep succ_step = STEP_BEGIN;
  ObTxIDSet conflict_tx_set;
  ObMvccWriteGuard guard(true);

  // 1. record lock myself(check conflict).
  // 2. record lock at memtable ctx.
  // 3. create lock callback and list it on the callback list of memtable ctx.
  if (OB_FAIL(guard.write_auth(ctx))) {
    LOG_WARN("not allow lock table.", K(ret), K(ctx));
  } else if (FALSE_IT(mem_ctx = static_cast<ObMemtableCtx *>(ctx.mvcc_acc_ctx_.mem_ctx_))) {
  } else if (OB_FAIL(mem_ctx->check_lock_exist(lock_op.lock_id_,
                                               lock_op.owner_id_,
                                               lock_op.lock_mode_,
                                               lock_exist,
                                               lock_mode_in_same_trans))) {
    LOG_WARN("failed to check lock exist ", K(ret), K(lock_op));
  } else if (lock_exist) {
    // do nothing
  } else if (FALSE_IT(lock_upgrade(lock_mode_in_same_trans, lock_op))) {
  } else if (OB_FAIL(obj_lock_map_.lock(param,
                                        ctx,
                                        lock_op,
                                        lock_mode_in_same_trans,
                                        conflict_tx_set))) {
    if (ret != OB_TRY_LOCK_ROW_CONFLICT &&
        ret != OB_OBJ_LOCK_EXIST) {
      LOG_WARN("record lock at lock map mgr failed.", K(ret), K(lock_op));
    }
  } else if (FALSE_IT(succ_step = STEP_IN_LOCK_MGR)) {
  } else if (OB_FAIL(mem_ctx->add_lock_record(lock_op))) {
    LOG_WARN("record lock at mem_ctx failed.", K(ret), K(lock_op));
  }
  if (OB_FAIL(ret) && succ_step == STEP_IN_LOCK_MGR) {
    obj_lock_map_.remove_lock_record(lock_op);
  }
  // return success if lock twice.
  if (ret == OB_OBJ_LOCK_EXIST) {
    ret = OB_SUCCESS;
  }
  if (OB_TRY_LOCK_ROW_CONFLICT == ret &&
      lock_op.is_dml_lock_op() &&   // only in trans dml lock will wait at lock wait mgr.
      conflict_tx_set.count() != 0) {
    // TODO: yanyuan.cxf only wait at the first conflict trans now, but we need
    // wait all the conflict trans to do deadlock detect.
    ObFunction<int(bool &need_wait)> recheck_f([this,
                                                &lock_op,
                                                &lock_mode_in_same_trans](bool &need_wait) -> int {
      int ret = OB_SUCCESS;
      ObTxIDSet conflict_tx_set;
      if (OB_FAIL(this->obj_lock_map_.check_allow_lock(lock_op,
                                                       lock_mode_in_same_trans,
                                                       conflict_tx_set)) &&
          OB_TRY_LOCK_ROW_CONFLICT != ret) {
        LOG_WARN("check allow lock failed", K(ret));
      } else if (OB_TRY_LOCK_ROW_CONFLICT == ret) {
        ret = OB_SUCCESS;
      } else {
        // do nothing
      }
      need_wait = (conflict_tx_set.count() != 0);
      return ret;
    });
    if (!recheck_f.is_valid()) {
      tmp_ret = OB_ERR_UNEXPECTED;
      LOG_WARN("recheck function construct failed", K(tmp_ret));
    } else if (OB_SUCCESS != (tmp_ret = post_obj_lock_conflict_(ctx.mvcc_acc_ctx_,
                                                                lock_op.lock_id_,
                                                                *(conflict_tx_set.begin()),
                                                                recheck_f))) {
      LOG_WARN("post obj lock conflict failed", K(tmp_ret));
    } else {
      // do nothing
    }
  }
  return ret;
}

int ObLockMemtable::unlock_(
    ObStoreCtx &ctx,
    const ObTableLockOp &unlock_op,
    const bool is_try_lock,
    const int64_t expired_time)
{
  int ret = OB_SUCCESS;
  ObMemtableCtx *mem_ctx = NULL;
  ObLockStep succ_step = STEP_BEGIN;
  ObMvccWriteGuard guard(true);

  // 1. record unlock op myself(check conflict).
  // 2. record unlock op at memtable ctx.
  // 3. create unlock callback and list it on the callback list of memtable ctx.
  if (OB_FAIL(guard.write_auth(ctx))) {
    LOG_WARN("not allow unlock table.", K(ret), K(ctx));
  } else if (FALSE_IT(mem_ctx = static_cast<ObMemtableCtx *>(ctx.mvcc_acc_ctx_.mem_ctx_))) {
  } else if (OB_FAIL(obj_lock_map_.unlock(unlock_op,
                                          is_try_lock,
                                          expired_time))) {
    LOG_WARN("record lock at lock map mgr failed.", K(ret), K(unlock_op));
  } else if (FALSE_IT(succ_step = STEP_IN_LOCK_MGR)) {
  } else if (OB_FAIL(mem_ctx->add_lock_record(unlock_op))) {
    LOG_WARN("record lock at mem_ctx failed.", K(ret), K(unlock_op));
  }
  if (OB_FAIL(ret) && succ_step == STEP_IN_LOCK_MGR) {
    obj_lock_map_.remove_lock_record(unlock_op);
  }
  return ret;
}

int ObLockMemtable::check_lock_need_replay_(
    ObMemtableCtx *mem_ctx,
    const ObTableLockOp &lock_op,
    const int64_t log_ts,
    bool &need_replay)
{
  // 1. filter the lock/unlock op that has been dumped at lock memtable.
  // 2. filter the lock/unlock op that has been dumped at tx ctx.
  // 3. filter the lock/unlock op that has been replayed.
  int ret = OB_SUCCESS;
  need_replay = true;
  if (lock_op.is_out_trans_lock_op() && log_ts <= flushed_log_ts_) {
    need_replay = false;
    LOG_INFO("skip replay because of lock memtable dumped", K(flushed_log_ts_), K(log_ts), K(lock_op));
  } else if (OB_FAIL(mem_ctx->check_lock_need_replay(log_ts,
                                                     lock_op,
                                                     need_replay))) {
    LOG_WARN("check need replay failed.", K(ret), K(lock_op));
  } else {
    // do nothing
  }
  return ret;
}

int ObLockMemtable::replay_lock_(
    ObMemtableCtx *mem_ctx,
    const ObTableLockOp &lock_op,
    const int64_t log_ts)
{
  int ret = OB_SUCCESS;
  bool need_replay = true;
  ObLockStep succ_step = STEP_BEGIN;

  // 1. record lock myself(check conflict).
  // 2. record lock at memtable ctx.
  // 3. create lock callback and list it on the callback list of memtable ctx.
  LOG_DEBUG("ObLockMemtable::replay_lock_ ", K(lock_op));
  if (OB_FAIL(check_lock_need_replay_(mem_ctx,
                                      lock_op,
                                      log_ts,
                                      need_replay))) {
    LOG_WARN("check need replay failed.", K(ret), K(lock_op));
  } else if (!need_replay) {
    // do nothing
  } else if (OB_FAIL(obj_lock_map_.recover_obj_lock(lock_op))) {
    LOG_WARN("replay lock at lock map mgr failed.", K(ret), K(lock_op));
  } else if (FALSE_IT(succ_step = STEP_IN_LOCK_MGR)) {
  } else if (OB_FAIL(mem_ctx->replay_add_lock_record(lock_op,
                                                     log_ts))) {
    LOG_WARN("record lock at mem_ctx failed.", K(ret), K(lock_op));
  } else {
    // do nothing
  }
  if (OB_FAIL(ret) && succ_step == STEP_IN_LOCK_MGR) {
    obj_lock_map_.remove_lock_record(lock_op);
  }
  LOG_DEBUG("ObLockMemtable::replay_lock_ finish.", K(ret), K(lock_op));
  return ret;
}

// wait lock at lock wait mgr.
int ObLockMemtable::post_obj_lock_conflict_(ObMvccAccessCtx &acc_ctx,
                                            const ObLockID &lock_id,
                                            const ObTransID &conflict_tx_id,
                                            ObFunction<int(bool &need_wait)> &recheck_f)
{
  int ret = OB_TRY_LOCK_ROW_CONFLICT;
  ObLockWaitMgr *lock_wait_mgr = NULL;
  auto mem_ctx = acc_ctx.get_mem_ctx();
  int64_t current_ts = common::ObClockGenerator::getClock();
  int64_t lock_wait_start_ts = mem_ctx->get_lock_wait_start_ts() > 0
    ? mem_ctx->get_lock_wait_start_ts()
    : current_ts;
  int64_t lock_wait_expire_ts = acc_ctx.eval_lock_expire_ts(lock_wait_start_ts);
  if (current_ts >= lock_wait_expire_ts) {
    ret = OB_ERR_EXCLUSIVE_LOCK_CONFLICT;
    LOG_WARN("exclusive lock conflict", K(ret), K(lock_id),
             K(conflict_tx_id), K(acc_ctx), K(lock_wait_expire_ts));
  } else if (OB_ISNULL(lock_wait_mgr = MTL_WITH_CHECK_TENANT(ObLockWaitMgr*,
                                                             mem_ctx->get_tenant_id()))) {
    LOG_WARN("can not get tenant lock_wait_mgr MTL", K(mem_ctx->get_tenant_id()));
  } else {
    int tmp_ret = OB_SUCCESS;
    auto tx_ctx = acc_ctx.tx_ctx_;
    auto tx_id = acc_ctx.get_tx_id();
    bool remote_tx = tx_ctx->get_scheduler() != tx_ctx->get_addr();
    // TODO: one thread only can wait at one lock now.
    // this may be not enough.
    tmp_ret = lock_wait_mgr->post_lock(OB_TRY_LOCK_ROW_CONFLICT,
                                       LS_LOCK_TABLET,
                                       lock_id,
                                       lock_wait_expire_ts,
                                       remote_tx,
                                       mem_ctx->is_can_elr(),
                                       -1,
                                       -1, // total_trans_node_cnt
                                       tx_id,
                                       conflict_tx_id,
                                       recheck_f);
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("post_lock after tx conflict failed",
               K(tmp_ret), K(tx_id), K(conflict_tx_id));
    } else if (mem_ctx->get_lock_wait_start_ts() <= 0) {
      mem_ctx->set_lock_wait_start_ts(lock_wait_start_ts);
    }
  }
  LOG_DEBUG("ObLockMemtable::post_obj_lock_conflict_", K(ret), K(lock_id), K(conflict_tx_id));
  return ret;
}

int ObLockMemtable::check_lock_conflict(
    const ObMemtableCtx *mem_ctx,
    const ObTableLockOp &lock_op,
    ObTxIDSet &conflict_tx_set,
    const bool include_finish_tx,
    const bool only_check_dml_lock)
{
  int ret = OB_SUCCESS;
  bool lock_exist = false;
  ObTableLockMode lock_mode_in_same_trans = 0x0;

  LOG_DEBUG("ObLockMemtable::check_lock_conflict ", K(lock_op));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLockMemtable not inited.", K(ret));
  } else if (OB_ISNULL(mem_ctx) ||
             OB_UNLIKELY(!lock_op.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(mem_ctx), K(lock_op));
  } else if (OB_FAIL(mem_ctx->check_lock_exist(lock_op.lock_id_,
                                               lock_op.owner_id_,
                                               lock_op.lock_mode_,
                                               lock_exist,
                                               lock_mode_in_same_trans))) {
    LOG_WARN("failed to check lock exist ", K(ret), K(lock_op));
  } else if (lock_exist) {
    LOG_INFO("lock is exist", K(ret), K(lock_op));
  } else if (OB_FAIL(obj_lock_map_.check_allow_lock(lock_op,
                                                    lock_mode_in_same_trans,
                                                    conflict_tx_set,
                                                    include_finish_tx,
                                                    only_check_dml_lock))) {
    // if the lock exist, just return success.
    if (OB_OBJ_LOCK_EXIST == ret) {
      ret = OB_SUCCESS;
    } else if (OB_TRY_LOCK_ROW_CONFLICT == ret) {
    } else {
      LOG_WARN("failed to check_allow_lock", K(ret), K(lock_op));
    }
  }

  return ret;
}

int ObLockMemtable::lock(
    const ObLockParam &param,
    ObStoreCtx &ctx,
    ObTableLockOp &lock_op)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("ObLockMemtable::lock ", K(lock_op));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLockMemtable not inited.", K(ret));
  } else if (OB_UNLIKELY(!ctx.is_valid())
             || OB_UNLIKELY(!ctx.is_write())
             || OB_UNLIKELY(!lock_op.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(lock_op), K(ctx));
  } else if (OB_FAIL(lock_(param, ctx, lock_op))) {
    if (ret != OB_TRY_LOCK_ROW_CONFLICT) {
      LOG_WARN("lock failed.", K(ret), K(param), K(lock_op));
    }
  }
  LOG_DEBUG("ObLockMemtable::lock finish.", K(ret), K(param), K(lock_op));
  return ret;
}

int ObLockMemtable::unlock(
    ObStoreCtx &ctx,
    const ObTableLockOp &unlock_op,
    const bool is_try_lock,
    const int64_t expired_time)
{
  // only has OUT_TRANS_UNLOCK
  int ret = OB_SUCCESS;
  LOG_DEBUG("ObLockMemtable::unlock ", K(unlock_op));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLockMemtable not inited.", K(ret));
  } else if (OB_UNLIKELY(!ctx.is_write())
             || OB_UNLIKELY(!ctx.is_write())
             || OB_UNLIKELY(!unlock_op.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(unlock_op), K(ctx));
  } else if (OB_FAIL(unlock_(ctx, unlock_op, is_try_lock, expired_time))) {
    if (ret != OB_TRY_LOCK_ROW_CONFLICT) {
      LOG_WARN("unlock failed.", K(ret), K(unlock_op));
    }
  }
  LOG_DEBUG("ObLockMemtable::unlock finish.", K(ret), K(is_try_lock),
            K(expired_time), K(unlock_op));
  return ret;
}

int ObLockMemtable::remove_tablet_lock(const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  ObLockID lock_id;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLockMemtable not inited.");
  } else if (OB_UNLIKELY(!tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tablet_id));
  } else if (OB_FAIL(get_lock_id(tablet_id, lock_id))) {
    LOG_WARN("get table lock id failed.", K(tablet_id));
  } else if (OB_FAIL(obj_lock_map_.remove_lock(lock_id))) {
    LOG_WARN("remove lock failed.", K(tablet_id));
  }
  LOG_DEBUG("ObLockMemtable::remove_tablet_lock", K(ret), K(tablet_id));
  return ret;
}

void ObLockMemtable::remove_lock_record(const ObTableLockOp &lock_op)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLockMemtable not inited.");
  } else if (OB_UNLIKELY(!lock_op.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(lock_op));
  } else {
    obj_lock_map_.remove_lock_record(lock_op);
  }
  LOG_DEBUG("ObLockMemtable::remove_lock_record", K(lock_op));
}

int ObLockMemtable::update_lock_status(
    const ObTableLockOp &op_info,
    const int64_t commit_version,
    const int64_t commit_log_ts,
    const ObTableLockOpStatus status)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLockMemtable not inited.");
  } else if (OB_UNLIKELY(!op_info.is_valid()) ||
             OB_UNLIKELY(!is_op_status_valid(status))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(op_info), K(status));
  } else if (op_info.is_out_trans_lock_op() && commit_log_ts <= flushed_log_ts_) {
    LOG_INFO("commit skip because of lock memtable dumped", K(flushed_log_ts_),
             K(commit_log_ts), K(op_info));
  } else if (OB_FAIL(obj_lock_map_.update_lock_status(op_info,
                                                      commit_version,
                                                      commit_log_ts,
                                                      status))) {
    LOG_WARN("update lock status failed.", K(op_info), K(status));
  } else if ((OUT_TRANS_LOCK == op_info.op_type_ || OUT_TRANS_UNLOCK == op_info.op_type_)
             && LOCK_OP_COMPLETE == status) {
    RLockGuard guard(flush_lock_);
    dec_update(&rec_log_ts_, commit_log_ts);
    inc_update(&max_committed_log_ts_, commit_log_ts);
  }
  LOG_DEBUG("ObLockMemtable::update_lock_status", K(ret), K(op_info), K(commit_log_ts), K(status));
  return ret;
}

int ObLockMemtable::recover_obj_lock(const ObTableLockOp &op_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLockMemtable not inited.");
  } else if (OB_UNLIKELY(!op_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(op_info));
  } else if (OB_FAIL(obj_lock_map_.recover_obj_lock(op_info))) {
    LOG_WARN("recover_obj_lock failed", K(ret), K(op_info));
  }
  LOG_DEBUG("LockMemtable::recover_obj_lock success", K(op_info), K(ret));
  return ret;
}

int ObLockMemtable::get_table_lock_store_info(ObIArray<ObTableLockOp> &store_arr)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLockMemtable not inited.", K(ret));
  } else if (OB_FAIL(obj_lock_map_.get_table_lock_store_info(store_arr, freeze_log_ts_))) {
    LOG_WARN("get_table_lock_store_info failed", K(ret));
  }
  return ret;
}

int ObLockMemtable::get_lock_id_iter(ObLockIDIterator &iter)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TABLELOCK_LOG(WARN, "ObLockMemtable not inited.", K(ret));
  } else if (OB_FAIL(obj_lock_map_.get_lock_id_iter(iter))) {
    TABLELOCK_LOG(WARN, "get_lock_id_iter failed", K(ret));
  }
  return ret;
}

int ObLockMemtable::get_lock_op_iter(const ObLockID &lock_id,
                                     ObLockOpIterator &iter)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TABLELOCK_LOG(WARN, "ObLockMemtable not inited.", K(ret));
  } else if (OB_FAIL(obj_lock_map_.get_lock_op_iter(lock_id,
                                                    iter))) {
    TABLELOCK_LOG(WARN, "get_lock_op_iter failed", K(ret), K(lock_id));
  }
  return ret;
}

int ObLockMemtable::scan(
    const ObTableIterParam &param,
    ObTableAccessContext &context,
    const blocksstable::ObDatumRange &key_range,
    ObStoreRowIterator *&row_iter)
{
  int ret = OB_SUCCESS;
  ObTableLockScanIterator *scan_iter_ptr = nullptr;
  void *scan_iter_buff = nullptr;
  UNUSED(key_range);
  UNUSED(context);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ob table lock memtable is not inited.", KR(ret), K(*this));
  } else if (OB_UNLIKELY(!param.is_valid() || !context.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid param", KR(ret), K(param), K(context));
  } else if (OB_UNLIKELY(!param.is_multi_version_minor_merge_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "ObTableLockMemtable only support scan for minor merge", KR(ret), K(param));
  } else if (OB_ISNULL(scan_iter_buff
                       = context.stmt_allocator_->alloc(sizeof(ObTableLockScanIterator)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "construct ObTableLockScanIterator fail", "scan_iter_buffer",
                scan_iter_buff, "scan_iter_ptr", scan_iter_ptr, KR(ret));
  } else if (FALSE_IT(scan_iter_ptr = new (scan_iter_buff) ObTableLockScanIterator())) {
  } else if (OB_FAIL(scan_iter_ptr->init(this))) {
    STORAGE_LOG(WARN, "init scan_iter_ptr fail.", KR(ret), K(context));
  } else {
    // table lock memtable scan iterator init success
    row_iter = scan_iter_ptr;
    STORAGE_LOG(INFO, "ob table lock memtable scan successfully", K(*this));
  }

  return ret;
}

int ObLockMemtable::get(
    const storage::ObTableIterParam &param,
    storage::ObTableAccessContext &context,
    const blocksstable::ObDatumRowkey &rowkey,
    blocksstable::ObDatumRow &row)
{
  UNUSED(param);
  UNUSED(context);
  UNUSED(rowkey);
  UNUSED(row);
  return OB_NOT_SUPPORTED;
}


int ObLockMemtable::set(
    storage::ObStoreCtx &ctx,
    const uint64_t table_id,
    const storage::ObTableReadInfo &read_info,
    const ObIArray<share::schema::ObColDesc> &columns,
    const storage::ObStoreRow &row)
{
  UNUSED(ctx);
  UNUSED(table_id);
  UNUSED(read_info);
  UNUSED(columns);
  UNUSED(row);
  return OB_NOT_SUPPORTED;
}

int ObLockMemtable::lock(
    storage::ObStoreCtx &ctx,
    const uint64_t table_id,
    const storage::ObTableReadInfo &read_info,
    ObNewRowIterator &row_iter)
{
  UNUSED(ctx);
  UNUSED(table_id);
  UNUSED(read_info);
  UNUSED(row_iter);
  return OB_NOT_SUPPORTED;
}

int ObLockMemtable::lock(
    storage::ObStoreCtx &ctx,
    const uint64_t table_id,
    const storage::ObTableReadInfo &read_info,
    const ObNewRow &row)
{
  UNUSED(ctx);
  UNUSED(table_id);
  UNUSED(read_info);
  UNUSED(row);
  return OB_NOT_SUPPORTED;
}

int ObLockMemtable::lock(
    storage::ObStoreCtx &ctx,
    const uint64_t table_id,
    const storage::ObTableReadInfo &read_info,
    const blocksstable::ObDatumRowkey &rowkey)
{
  UNUSED(ctx);
  UNUSED(table_id);
  UNUSED(read_info);
  UNUSED(rowkey);
  return OB_NOT_SUPPORTED;
}

int ObLockMemtable::get(
    const storage::ObTableIterParam &param,
    storage::ObTableAccessContext &context,
    const blocksstable::ObDatumRowkey &rowkey,
    ObStoreRowIterator *&row_iter)
{
  UNUSED(param);
  UNUSED(context);
  UNUSED(rowkey);
  UNUSED(row_iter);
  return OB_NOT_SUPPORTED;
}

int ObLockMemtable::multi_get(
    const ObTableIterParam &param,
    ObTableAccessContext &context,
    const common::ObIArray<blocksstable::ObDatumRowkey> &rowkeys,
    ObStoreRowIterator *&row_iter)
{
  UNUSED(param);
  UNUSED(context);
  UNUSED(rowkeys);
  UNUSED(row_iter);
  return OB_NOT_SUPPORTED;
}

int ObLockMemtable::multi_scan(
    const ObTableIterParam &param,
    ObTableAccessContext &context,
    const common::ObIArray<blocksstable::ObDatumRange> &ranges,
    ObStoreRowIterator *&row_iter)
{
  UNUSED(param);
  UNUSED(context);
  UNUSED(ranges);
  UNUSED(row_iter);
  return OB_NOT_SUPPORTED;
}

int ObLockMemtable::get_frozen_schema_version(int64_t &schema_version) const
{
  UNUSED(schema_version);
  return OB_NOT_SUPPORTED;
}

int64_t ObLockMemtable::get_rec_log_ts()
{
  // no need lock because rec_log_ts_ aesc except INT64_MAX
  LOG_INFO("rec_log_ts of ObLockMemtable is ", K(rec_log_ts_), K(flushed_log_ts_),
           K(freeze_log_ts_), K(max_committed_log_ts_), K(is_frozen_), K(ls_id_));
  return rec_log_ts_;
}

ObTabletID ObLockMemtable::get_tablet_id() const
{
  return LS_LOCK_TABLET;
}

bool ObLockMemtable::is_flushing() const
{
  return ATOMIC_LOAD(&is_frozen_);
}

int ObLockMemtable::on_memtable_flushed()
{
  int ret = OB_SUCCESS;
  WLockGuard guard(flush_lock_);
  if (max_committed_log_ts_ > freeze_log_ts_) {
    // have no_flushed commit_log_ts
    rec_log_ts_ = freeze_log_ts_;
  } else {
    // all commit_log_ts flushed
    rec_log_ts_ = INT64_MAX;
    max_committed_log_ts_ = OB_INVALID_TIMESTAMP;
  }
  if (freeze_log_ts_ > flushed_log_ts_) {
    flushed_log_ts_ = freeze_log_ts_;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("freeze_log_ts_ should not smaller than flushed_log_ts_", K(ret),
              K_(flushed_log_ts), K_(freeze_log_ts), K_(ls_id));
  }
  ATOMIC_STORE(&is_frozen_, false);
  LOG_INFO("lock memtable on_memtable_flushed success", K(ret), K(flushed_log_ts_), K(ls_id_));
  return ret;
}

bool ObLockMemtable::can_be_minor_merged()
{
  bool bool_ret = is_frozen_memtable();
  LOG_INFO("ObLockMemtable::can_be_minor_merged", K(bool_ret), K(ls_id_));
  return bool_ret;
}

bool ObLockMemtable::is_frozen_memtable() const
{
  return ATOMIC_LOAD(&is_frozen_);
}

bool ObLockMemtable::is_active_memtable() const
{
  return !ATOMIC_LOAD(&is_frozen_);
}

int ObLockMemtable::flush(int64_t recycle_log_ts, bool need_freeze)
{
  int ret = OB_SUCCESS;
  UNUSED(need_freeze);
  {
    WLockGuard guard(flush_lock_);
    int64_t rec_log_ts = get_rec_log_ts();
    if (rec_log_ts >= recycle_log_ts) {
      LOG_INFO("lock memtable no need to flush", K(rec_log_ts), K(recycle_log_ts),
              K(is_frozen_), K(ls_id_));
    } else if (is_active_memtable()) {
      if (OB_FAIL(freezer_->get_max_consequent_callbacked_log_ts(freeze_log_ts_))) {
        LOG_WARN("get_max_consequent_callbacked_log_ts failed", K(ret), K(ls_id_));
      } else if (flushed_log_ts_ >= freeze_log_ts_) {
        LOG_INFO("skip freeze because of flushed", K_(ls_id), K_(flushed_log_ts), K_(freeze_log_ts));
      } else {
        ObLogTsRange log_ts_range;
        log_ts_range.start_log_ts_ = 1;
        log_ts_range.end_log_ts_ = freeze_log_ts_;
        set_log_ts_range(log_ts_range);
        set_snapshot_version(freeze_log_ts_);
        ATOMIC_STORE(&is_frozen_, true);
      }
    }
  }

  if (is_frozen_memtable()) {
    // dependent to judging is_active_memtable() in dag
    // otherwise maybe merge active memtable
    compaction::ObTabletMergeDagParam param;
    param.ls_id_ = ls_id_;
    param.tablet_id_ = LS_LOCK_TABLET;
    param.merge_type_ = MINI_MERGE;
    param.merge_version_ = ObVersion::MIN_VERSION;
    if (OB_FAIL(compaction::ObScheduleDagFunc::schedule_tx_table_merge_dag(param))) {
      if (OB_EAGAIN != ret && OB_SIZE_OVERFLOW != ret) {
        LOG_WARN("failed to schedule lock_memtable merge dag", K(ret), K(this));
      }
    } else {
      LOG_INFO("schedule lock_memtable merge_dag successfully", K(ls_id_), K(freeze_log_ts_));
    }
  }

  return ret;
}

int ObLockMemtable::replay_row(
    storage::ObStoreCtx &ctx,
    ObMemtableMutatorIterator *mmi)
{
  int ret = OB_SUCCESS;

  ObLockID lock_id;
  ObTableLockOwnerID owner_id = 0;
  ObTableLockMode lock_mode = NO_LOCK;
  ObTableLockOpType lock_op_type = ObTableLockOpType::UNKNOWN_TYPE;
  int64_t seq_no = 0;
  int64_t create_timestamp = 0;
  int64_t create_schema_vesion = -1;
  ObMemtableCtx *mem_ctx = nullptr;
  const int64_t curr_timestamp = ObTimeUtility::current_time();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLockMemtable not inited.", K(ret));
  } else if (OB_UNLIKELY(!ctx.is_replay()) || OB_ISNULL(mmi)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ctx), KP(mmi));
  } else if (OB_FAIL(mmi->get_table_lock_row().copy(lock_id,
                                                    owner_id,
                                                    lock_mode,
                                                    lock_op_type,
                                                    seq_no,
                                                    create_timestamp,
                                                    create_schema_vesion))) {
    LOG_WARN("get lock op info error", K(ret));
  } else if (OB_ISNULL(mem_ctx = ctx.mvcc_acc_ctx_.mem_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("memtable ctx should not null", K(ctx));
  } else {
    ObTableLockOp lock_op(lock_id,
                          lock_mode,
                          owner_id,
                          ctx.mvcc_acc_ctx_.tx_id_,
                          lock_op_type,
                          LOCK_OP_DOING,
                          seq_no,
                          OB_MIN(create_timestamp, curr_timestamp),
                          create_schema_vesion);
    if (OB_UNLIKELY(!lock_op.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("lock op is not valid", K(ret), K(lock_op));
    } else if (OB_FAIL(replay_lock_(mem_ctx, lock_op, ctx.log_ts_))) {
      LOG_WARN("replay lock failed", K(ret), K(lock_op));
    }
  }
  LOG_DEBUG("ObMemtable::replay_row finish.", K(ret), K(lock_id), K(ls_id_));
  return ret;
}

int ObLockMemtable::replay_lock(
    ObMemtableCtx *mem_ctx,
    const ObTableLockOp &lock_op,
    const int64_t log_ts)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLockMemtable not inited.", K(ret));
  } else if (OB_ISNULL(mem_ctx) || OB_UNLIKELY(!lock_op.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(mem_ctx), K(lock_op));
  } else if (OB_FAIL(replay_lock_(mem_ctx, lock_op, log_ts))) {
    LOG_WARN("replay lock failed", K(ret), K(lock_op));
  } else {
    // do nothing
  }
  LOG_DEBUG("ObMemtable::replay_lock finish.", K(ret), K(lock_op), K(ls_id_));
  return ret;
}

} // tablelock
} // transaction
} // oceanbase
