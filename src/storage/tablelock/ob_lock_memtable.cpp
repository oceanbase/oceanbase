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
#include "storage/tablelock/ob_table_lock_deadlock.h"
#include "storage/tablelock/ob_table_lock_rpc_struct.h"
#include "storage/tx/ob_trans_define.h"
#include "storage/tx/ob_trans_part_ctx.h"
#include "storage/compaction/ob_schedule_dag_func.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tablet/ob_tablet.h"

namespace oceanbase
{
using namespace share;
using namespace storage;
using namespace memtable;
using namespace common;
using namespace oceanbase::lib;

namespace transaction
{
namespace tablelock
{

ObLockMemtable::ObLockMemtable()
  : ObIMemtable(),
    is_inited_(false),
    freeze_scn_(SCN::min_scn()),
    flushed_scn_(SCN::min_scn()),
    rec_scn_(SCN::max_scn()),
    pre_rec_scn_(SCN::max_scn()),
    max_committed_scn_(),
    is_frozen_(false),
    need_check_tablet_status_(false),
    freezer_(nullptr),
    flush_lock_(common::ObLatchIds::CLOG_CKPT_LOCK)
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
  rec_scn_.set_max();
  pre_rec_scn_.set_max();
  max_committed_scn_.reset();
  ObITable::reset();
  obj_lock_map_.reset();
  freeze_scn_.reset();
  flushed_scn_.reset();
  is_frozen_ = false;
  need_check_tablet_status_ = false;
  freezer_ = nullptr;
  is_inited_ = false;
  reset_trace_id();
}

int ObLockMemtable::lock_(
    const ObLockParam &param,
    ObStoreCtx &ctx,
    ObTableLockOp &lock_op)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t USLEEP_TIME = 100 * 1000; // 100 ms
  ObMemtableCtx *mem_ctx = NULL;
  bool need_retry = false;
  uint64_t lock_mode_cnt_in_same_trans[TABLE_LOCK_MODE_COUNT] = {0, 0, 0, 0, 0};
  bool lock_exist = false;
  ObLockStep succ_step = STEP_BEGIN;
  bool register_to_deadlock = false;
  ObTxIDSet conflict_tx_set;

  // 1. record lock myself(check conflict).
  // 2. record lock at memtable ctx.
  // 3. create lock callback and list it on the callback list of memtable ctx.

  do {
    // retry if there is lock conflict at part trans ctx.
    need_retry = false;
    {
      succ_step = STEP_BEGIN;
      lock_exist = false;
      memset(lock_mode_cnt_in_same_trans, 0, sizeof(lock_mode_cnt_in_same_trans));
      ObMvccWriteGuard guard;
      if (OB_FAIL(guard.write_auth(ctx))) {
        LOG_WARN("not allow lock table.", K(ret), K(ctx));
      } else if (OB_FAIL(check_tablet_write_allow_(lock_op))) {
        LOG_WARN("check tablet write allow failed", K(ret), K(lock_op));
      } else {
        mem_ctx = static_cast<ObMemtableCtx *>(ctx.mvcc_acc_ctx_.mem_ctx_);
        ObLockMemCtx::AddLockGuard guard(mem_ctx->get_lock_mem_ctx());
        if (OB_FAIL(guard.ret())) {
          LOG_WARN("failed to acquire lock on lock_mem_ctx", K(ret), K(ctx));
        } else if (OB_FAIL(mem_ctx->check_lock_exist(lock_op.lock_id_,
                                                     lock_op.owner_id_,
                                                     lock_op.lock_mode_,
                                                     lock_op.op_type_,
                                                     lock_exist,
                                                     lock_mode_cnt_in_same_trans))) {
          LOG_WARN("failed to check lock exist ", K(ret), K(lock_op));
        } else if (lock_exist) {
          // if the lock is DBMS_LOCK, we should return error code
          // to notify PL to return the actual execution result.
          if (lock_op.lock_id_.obj_type_ == ObLockOBJType::OBJ_TYPE_DBMS_LOCK) {
            ret = OB_OBJ_LOCK_EXIST;
          }
          LOG_DEBUG("lock is exist", K(ret), K(lock_op));
        } else if (OB_FAIL(obj_lock_map_.lock(param, ctx, lock_op, lock_mode_cnt_in_same_trans, conflict_tx_set))) {
          if (ret != OB_TRY_LOCK_ROW_CONFLICT &&
              ret != OB_OBJ_LOCK_EXIST) {
            LOG_WARN("record lock at lock map mgr failed.", K(ret), K(lock_op));
          }
        } else if (FALSE_IT(succ_step = STEP_IN_LOCK_MGR)) {
        } else if (OB_FAIL(mem_ctx->add_lock_record(lock_op))) {
          if (OB_EAGAIN == ret) {
            need_retry = true;
          }
          LOG_WARN("record lock at mem_ctx failed.", K(ret), K(lock_op));
        }
      }
      if (OB_FAIL(ret) && succ_step == STEP_IN_LOCK_MGR) {
        obj_lock_map_.remove_lock_record(lock_op);
      }
      if (OB_TRY_LOCK_ROW_CONFLICT == ret) {
        if (OB_TMP_FAIL(check_and_set_tx_lock_timeout_(ctx.mvcc_acc_ctx_))) {
          ret = tmp_ret;
          LOG_WARN("tx lock timeout", K(ret));
          break;
        } else if (!need_retry) {
          if (param.is_try_lock_) {
            ret = OB_ERR_EXCLUSIVE_LOCK_CONFLICT;
          } else if (ctx.mvcc_acc_ctx_.tx_ctx_->is_table_lock_killed()) {
            // trans is killed by deadlock detect or abort because of
            // something else.
            ret = OB_TRANS_KILLED;
          } else if (lock_op.is_dml_lock_op() /* only dml lock will wait at lock wait mgr */) {
            // wait at lock wait mgr but not retry at here.
          } else {
            // register to deadlock detector.
            need_retry = true;
            if (!lock_op.is_dml_lock_op() && !register_to_deadlock) {
              if (OB_TMP_FAIL(register_into_deadlock_detector_(ctx, lock_op))) {
                LOG_WARN("register to deadlock detector failed", K(ret), K(lock_op));
              } else {
                register_to_deadlock = true;
              }
            }
          }
        }
      } else if (OB_SUCCESS == ret) {
        // lock successfully, reset lock_wait_start_ts
        ctx.mvcc_acc_ctx_.set_lock_wait_start_ts(0);
      }

      if (ObClockGenerator::getClock() >= param.expired_time_) {
        ret = (ret == OB_TRY_LOCK_ROW_CONFLICT ? OB_ERR_EXCLUSIVE_LOCK_CONFLICT : OB_TIMEOUT);
        LOG_WARN("lock timeout", K(ret), K(lock_op), K(param));
        break;
      }
    }
    if (need_retry) {
      conflict_tx_set.reset();
      ob_usleep(USLEEP_TIME);
    }
  } while (need_retry);
  if (OB_UNLIKELY(register_to_deadlock)) {
    if (OB_TMP_FAIL(unregister_from_deadlock_detector_(lock_op))) {
      LOG_WARN("unregister from deadlock detector failed", K(tmp_ret), K(lock_op));
    }
  }
  // return success if lock twice, except for DBMS_LOCK.
  if (ret == OB_OBJ_LOCK_EXIST && lock_op.lock_id_.obj_type_ != ObLockOBJType::OBJ_TYPE_DBMS_LOCK) {
    ret = OB_SUCCESS;
  }
  if (OB_TRY_LOCK_ROW_CONFLICT == ret &&
      lock_op.is_dml_lock_op() &&   // only in trans dml lock will wait at lock wait mgr.
      conflict_tx_set.count() != 0) {
    // TODO: yanyuan.cxf only wait at the first conflict trans now, but we need
    // wait all the conflict trans to do deadlock detect.
    ObFunction<int(bool &need_wait)> recheck_f([this,
                                                &lock_op,
                                                &lock_mode_cnt_in_same_trans](bool &need_wait) -> int {
      int ret = OB_SUCCESS;
      ObTxIDSet conflict_tx_set;
      if (OB_FAIL(this->obj_lock_map_.check_allow_lock(lock_op,
                                                       lock_mode_cnt_in_same_trans,
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
    } else if (OB_TMP_FAIL(post_obj_lock_conflict_(ctx.mvcc_acc_ctx_,
                                                   lock_op.lock_id_,
                                                   lock_op.lock_mode_,
                                                   *(conflict_tx_set.begin()),
                                                   recheck_f))) {
      if (OB_ERR_EXCLUSIVE_LOCK_CONFLICT == tmp_ret) {
        ret = tmp_ret;
      } else {
        LOG_WARN("post obj lock conflict failed", K(tmp_ret), K(lock_op));
      }
    } else {
      // do nothing
    }
  }
  return ret;
}

int ObLockMemtable::check_tablet_write_allow_(const ObTableLockOp &lock_op)
{
  int ret = OB_SUCCESS;
  ObTabletID tablet_id;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  ObTabletHandle tablet_handle;
  ObTabletStatus::Status tablet_status = ObTabletStatus::MAX;
  ObTabletCreateDeleteMdsUserData data;
  bool is_commited = false;
  if (!need_check_tablet_status_) {
  } else if (!lock_op.lock_id_.is_tablet_lock()) {
  } else if (OB_FAIL(lock_op.lock_id_.convert_to(tablet_id))) {
    LOG_WARN("convert lock id to tablet_id failed", K(ret), K(lock_op));
  } else if (OB_FAIL(MTL(ObLSService*)->get_ls(ls_id_, ls_handle, ObLSGetMod::TABLELOCK_MOD))) {
    LOG_WARN("failed to get ls", K(ret), K(ls_id_));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), KP(ls));
  } else if (OB_FAIL(ls->get_tablet(tablet_id,
                                    tablet_handle,
                                    0,
                                    ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
    LOG_WARN("get tablet with timeout failed", K(ret), K(ls->get_ls_id()), K(tablet_id));
  } else if (OB_FAIL(tablet_handle.get_obj()->ObITabletMdsInterface::get_latest_tablet_status(
      data, is_commited))) {
    LOG_WARN("failed to get CreateDeleteMdsUserData", KR(ret));
  } else if (FALSE_IT(tablet_status = data.get_tablet_status())) {
  } else if (is_commited && (ObTabletStatus::NORMAL == tablet_status
                             || ObTabletStatus::TRANSFER_IN == tablet_status)) {
    // allow
  } else {
    ret = OB_TABLET_NOT_EXIST;
    LOG_INFO("tablet status not allow", KR(ret), K(tablet_id), K(is_commited), K(data));
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
  int64_t USLEEP_TIME = 100 * 1000; // 100 ms
  ObMemtableCtx *mem_ctx = NULL;
  bool lock_op_exist = false;
  bool need_retry = false;
  uint64_t unused_lock_mode_cnt_in_same_trans[TABLE_LOCK_MODE_COUNT] = {0, 0, 0, 0, 0};
  ObLockStep succ_step = STEP_BEGIN;

  // 1. record unlock op myself(check conflict).
  // 2. record unlock op at memtable ctx.
  // 3. create unlock callback and list it on the callback list of memtable ctx.
  Thread::WaitGuard guard(Thread::WAIT);
  do {
    // retry if there is lock conflict at part trans ctx.
    need_retry = false;
    {
      succ_step = STEP_BEGIN;
      lock_op_exist = false;
      memset(unused_lock_mode_cnt_in_same_trans, 0, sizeof(unused_lock_mode_cnt_in_same_trans));
      ObMvccWriteGuard guard(true);
      if (ObClockGenerator::getClock() >= expired_time) {
        ret = OB_TIMEOUT;
        LOG_WARN("unlock timeout", K(ret), K(unlock_op), K(expired_time));
      } else if (OB_FAIL(guard.write_auth(ctx))) {
        LOG_WARN("not allow unlock table.", K(ret), K(ctx));
      } else if (OB_FAIL(check_tablet_write_allow_(unlock_op))) {
        LOG_WARN("check tablet write allow failed", K(ret), K(unlock_op));
      } else if (FALSE_IT(mem_ctx = static_cast<ObMemtableCtx *>(ctx.mvcc_acc_ctx_.mem_ctx_))) {
        // check whether the unlock op exist already
      } else if (OB_FAIL(mem_ctx->check_lock_exist(unlock_op.lock_id_,
                                                   unlock_op.owner_id_,
                                                   unlock_op.lock_mode_,
                                                   unlock_op.op_type_,
                                                   lock_op_exist,
                                                   unused_lock_mode_cnt_in_same_trans))) {
        LOG_WARN("failed to check lock exist ", K(ret), K(unlock_op));
      } else if (lock_op_exist) {
        // do nothing
      } else if (OB_FAIL(obj_lock_map_.unlock(unlock_op,
                                              is_try_lock,
                                              expired_time))) {
        LOG_WARN("record lock at lock map mgr failed.", K(ret), K(unlock_op));
      } else if (FALSE_IT(succ_step = STEP_IN_LOCK_MGR)) {
      } else if (OB_FAIL(mem_ctx->add_lock_record(unlock_op))) {
        if (OB_EAGAIN == ret) {
          need_retry = true;
        }
        LOG_WARN("record lock at mem_ctx failed.", K(ret), K(unlock_op));
      }
      if (OB_FAIL(ret) && succ_step == STEP_IN_LOCK_MGR) {
        obj_lock_map_.remove_lock_record(unlock_op);
      }
      if (!need_retry &&
          is_need_retry_unlock_error(ret) &&
          !is_try_lock) {
        need_retry = true;
      }
    }
    if (need_retry) {
      ob_usleep(USLEEP_TIME);
    }
  } while (need_retry);
  return ret;
}

int ObLockMemtable::check_lock_need_replay_(
    ObMemtableCtx *mem_ctx,
    const ObTableLockOp &lock_op,
    const SCN &scn,
    bool &need_replay)
{
  // 1. filter the lock/unlock op that has been dumped at lock memtable.
  // 2. filter the lock/unlock op that has been dumped at tx ctx.
  // 3. filter the lock/unlock op that has been replayed.
  int ret = OB_SUCCESS;
  need_replay = true;
  if (lock_op.is_out_trans_lock_op() && scn <= flushed_scn_) {
    need_replay = false;
    LOG_INFO("skip replay because of lock memtable dumped", K(flushed_scn_), K(scn), K(lock_op));
  } else if (OB_FAIL(mem_ctx->check_lock_need_replay(scn,
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
    const SCN &scn)
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
                                      scn,
                                      need_replay))) {
    LOG_WARN("check need replay failed.", K(ret), K(lock_op));
  } else if (!need_replay) {
    // do nothing
  } else if (OB_FAIL(obj_lock_map_.recover_obj_lock(lock_op))) {
    LOG_WARN("replay lock at lock map mgr failed.", K(ret), K(lock_op));
  } else if (FALSE_IT(succ_step = STEP_IN_LOCK_MGR)) {
  } else if (OB_FAIL(mem_ctx->replay_add_lock_record(lock_op,
                                                     scn))) {
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
                                            const ObTableLockMode &lock_mode,
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
  if (OB_ISNULL(lock_wait_mgr = MTL_WITH_CHECK_TENANT(ObLockWaitMgr *, mem_ctx->get_tenant_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("can not get tenant lock_wait_mgr MTL", K(mem_ctx->get_tenant_id()));
  } else {
    int tmp_ret = OB_SUCCESS;
    auto tx_ctx = acc_ctx.tx_ctx_;
    auto tx_id = acc_ctx.get_tx_id();
    bool remote_tx = tx_ctx->get_scheduler() != tx_ctx->get_addr();
    // TODO: one thread only can wait at one lock now.
    // this may be not enough.
    if (OB_TMP_FAIL(lock_wait_mgr->post_lock(OB_TRY_LOCK_ROW_CONFLICT,
                                             LS_LOCK_TABLET,
                                             lock_id,
                                             lock_wait_expire_ts,
                                             remote_tx,
                                             -1,
                                             -1,  // total_trans_node_cnt
                                             acc_ctx.tx_desc_->get_assoc_session_id(),
                                             tx_id,
                                             conflict_tx_id,
                                             lock_mode,
                                             ls_id_,
                                             recheck_f))) {
      LOG_WARN("post_lock after tx conflict failed",
               K(tmp_ret), K(tx_id), K(conflict_tx_id));
    }
  }
  LOG_DEBUG("ObLockMemtable::post_obj_lock_conflict_",
            K(ret),
            K(lock_id),
            K(conflict_tx_id));
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
  uint64_t lock_mode_cnt_in_same_trans[TABLE_LOCK_MODE_COUNT] = {0, 0, 0, 0, 0};

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
                                               lock_op.op_type_,
                                               lock_exist,
                                               lock_mode_cnt_in_same_trans))) {
    LOG_WARN("failed to check lock exist ", K(ret), K(lock_op));
  } else if (lock_exist) {
    // if the lock is DBMS_LOCK, we should return error code
    // to notify PL to return the actual execution result.
    if (lock_op.lock_id_.obj_type_ == ObLockOBJType::OBJ_TYPE_DBMS_LOCK) {
      ret = OB_OBJ_LOCK_EXIST;
    }
    LOG_DEBUG("lock is exist", K(ret), K(lock_op));
  } else if (OB_FAIL(obj_lock_map_.check_allow_lock(lock_op,
                                                    lock_mode_cnt_in_same_trans,
                                                    conflict_tx_set,
                                                    include_finish_tx,
                                                    only_check_dml_lock))) {
    // return success if lock twice, except for DBMS_LOCK.
    if (OB_OBJ_LOCK_EXIST == ret && lock_op.lock_id_.obj_type_ != ObLockOBJType::OBJ_TYPE_DBMS_LOCK) {
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
  Thread::WaitGuard guard(Thread::WAIT);
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
  Thread::WaitGuard guard(Thread::WAIT);
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
    const SCN &commit_version,
    const SCN &commit_scn,
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
  } else if (op_info.is_out_trans_lock_op() && commit_scn <= flushed_scn_) {
    LOG_INFO("commit skip because of lock memtable dumped", K(flushed_scn_),
             K(commit_scn), K(op_info));
  } else if (OB_FAIL(obj_lock_map_.update_lock_status(op_info,
                                                      commit_version,
                                                      commit_scn,
                                                      status))) {
    LOG_WARN("update lock status failed.", K(op_info), K(status));
  } else if ((OUT_TRANS_LOCK == op_info.op_type_ || OUT_TRANS_UNLOCK == op_info.op_type_)
             && LOCK_OP_COMPLETE == status) {
    RLockGuard guard(flush_lock_);
    if (commit_scn <= freeze_scn_) {
      LOG_INFO("meet disordered replay, will dec_update pre_rec_scn_", K(ret),
               K(op_info), K(commit_scn), K(rec_scn_), K(pre_rec_scn_),
               K(freeze_scn_), K(ls_id_));
      pre_rec_scn_.dec_update(commit_scn);
    } else {
      rec_scn_.dec_update(commit_scn);
    }
    max_committed_scn_.inc_update(commit_scn);
    LOG_INFO("out_trans update_lock_status", K(ret), K(op_info), K(commit_scn), K(status), K(rec_scn_), K(ls_id_));
  }
  LOG_DEBUG("ObLockMemtable::update_lock_status", K(ret), K(op_info), K(commit_scn), K(status));
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
  } else if (OB_FAIL(obj_lock_map_.get_table_lock_store_info(
      store_arr,
      freeze_scn_))) {
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

int ObLockMemtable::check_and_clear_obj_lock(const bool force_compact)
{
  int ret = OB_SUCCESS;
  Thread::WaitGuard guard(Thread::WAIT);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TABLELOCK_LOG(WARN, "ObLockMemtable not inited.", K(ret));
  } else if (OB_FAIL(obj_lock_map_.check_and_clear_obj_lock(force_compact))) {
    TABLELOCK_LOG(WARN, "check and clear obj lock failed", K(ret));
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

SCN ObLockMemtable::get_rec_scn()
{
  // no need lock because rec_scn_ aesc except INT64_MAX
  LOG_INFO("rec_scn of ObLockMemtable is ",
           K(rec_scn_), K(flushed_scn_), K(pre_rec_scn_),
           K(freeze_scn_), K(max_committed_scn_), K(is_frozen_), K(ls_id_));
  // If pre_rec_scn_ is max, it means that previous memtable
  // has already been flushed. In ohter words, it means that
  // rec_scn_ is ready to work, so we can return rec_scn_.
  // You can regard max_scn as an invalid value for pre_rec_scn_ here.
  // (As a matter of fact, max_scn means pre_rec_scn_ or rec_scn_
  // will not block checkpoint advancing.)
  //
  // Specifically, if there's a commit_scn which is smaller
  // than the freeze_scn_, the pre_rec_scn_ will be set to
  // an valid value (i.e. not max_scn) again, it's a special
  // case in disordered replay.
  // You can see details about this case in update_lock_status.
  if (pre_rec_scn_.is_max()) {
    return rec_scn_;
  } else {
    if (pre_rec_scn_ > rec_scn_) {
      LOG_INFO("prec_rec_scn_ is larger than rec_scn_!", K(pre_rec_scn_),
               K(rec_scn_), K(flushed_scn_), K(freeze_scn_),
               K(max_committed_scn_), K(is_frozen_), K(ls_id_));
    }
    return share::SCN::min(pre_rec_scn_, rec_scn_);
  }
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
  pre_rec_scn_.set_max();
  if (freeze_scn_ > flushed_scn_) {
    flushed_scn_ = freeze_scn_;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("freeze_scn_ should not smaller than flushed_scn_", K(ret),
              K_(flushed_scn), K_(freeze_scn), K_(ls_id));
  }
  ATOMIC_STORE(&is_frozen_, false);
  LOG_INFO("lock memtable on_memtable_flushed success", K(ret), K(flushed_scn_), K(ls_id_));
  return ret;
}

bool ObLockMemtable::can_be_minor_merged()
{
  bool bool_ret = is_frozen_memtable();
  LOG_INFO("ObLockMemtable::can_be_minor_merged", K(bool_ret), K(ls_id_));
  return bool_ret;
}

bool ObLockMemtable::is_frozen_memtable()
{
  return ATOMIC_LOAD(&is_frozen_);
}

bool ObLockMemtable::is_active_memtable()
{
  return !ATOMIC_LOAD(&is_frozen_);
}

int ObLockMemtable::flush(SCN recycle_scn,
                          int64_t trace_id,
                          bool need_freeze)
{
  int ret = OB_SUCCESS;
  if (need_freeze) {
    WLockGuard guard(flush_lock_);
    SCN rec_scn = get_rec_scn();
    if (rec_scn >= recycle_scn) {
      LOG_INFO("lock memtable no need to flush", K(rec_scn), K(recycle_scn),
               K(is_frozen_), K(ls_id_));
    } else if (is_active_memtable()) {
      freeze_scn_.inc_update(max_committed_scn_);
      if (flushed_scn_ >= freeze_scn_) {
        LOG_INFO("skip freeze because of flushed", K_(ls_id), K_(flushed_scn), K_(freeze_scn));
      } else {
        pre_rec_scn_ = rec_scn_;
        rec_scn_.set_max();
        max_committed_scn_.reset();

        ObScnRange scn_range;
        scn_range.start_scn_.set_base();
        scn_range.end_scn_ = freeze_scn_;
        set_scn_range(scn_range);
        set_snapshot_version(freeze_scn_);
        ATOMIC_STORE(&is_frozen_, true);
      }
    }
  }

  if (is_frozen_memtable()) {
    SCN max_consequent_callbacked_scn = SCN::min_scn();
    if (OB_FAIL(freezer_->get_max_consequent_callbacked_scn(max_consequent_callbacked_scn))) {
      LOG_WARN("get_max_consequent_callbacked_scn failed", K(ret), K(ls_id_));
    } else if (max_consequent_callbacked_scn < freeze_scn_) {
      LOG_INFO("lock memtable not ready for flush",
               K(max_consequent_callbacked_scn),
               K(freeze_scn_));
    } else {
      // dependent to judging is_active_memtable() in dag
      // otherwise maybe merge active memtable
      compaction::ObTabletMergeDagParam param;
      param.ls_id_ = ls_id_;
      param.tablet_id_ = LS_LOCK_TABLET;
      param.merge_type_ = compaction::MINI_MERGE;
      param.merge_version_ = ObVersion::MIN_VERSION;
      set_trace_id(trace_id);
      if (OB_FAIL(compaction::ObScheduleDagFunc::schedule_tx_table_merge_dag(param))) {
        if (OB_EAGAIN != ret && OB_SIZE_OVERFLOW != ret) {
          LOG_WARN("failed to schedule lock_memtable merge dag", K(ret), K(this));
        }
      } else {
        REPORT_CHECKPOINT_DIAGNOSE_INFO(update_schedule_dag_info, this, get_rec_scn(),
            get_start_scn(), get_end_scn());
        LOG_INFO("schedule lock_memtable merge_dag successfully", K(ls_id_), K(freeze_scn_));
      }
    }
  }

  return ret;
}

int ObLockMemtable::replay_row(
    storage::ObStoreCtx &ctx,
    const share::SCN &scn,
    ObMemtableMutatorIterator *mmi)
{
  int ret = OB_SUCCESS;

  ObLockID lock_id;
  ObTableLockOwnerID owner_id;
  ObTableLockMode lock_mode = NO_LOCK;
  ObTableLockOpType lock_op_type = ObTableLockOpType::UNKNOWN_TYPE;
  transaction::ObTxSEQ seq_no;
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
    } else if (OB_FAIL(replay_lock_(mem_ctx, lock_op, scn))) {
      LOG_WARN("replay lock failed", K(ret), K(lock_op));
    }
  }
  LOG_DEBUG("ObMemtable::replay_row finish.", K(ret), K(lock_id), K(ls_id_));
  return ret;
}

int ObLockMemtable::replay_lock(
    ObMemtableCtx *mem_ctx,
    const ObTableLockOp &lock_op,
    const SCN &scn)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLockMemtable not inited.", K(ret));
  } else if (OB_ISNULL(mem_ctx) || OB_UNLIKELY(!lock_op.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(mem_ctx), K(lock_op));
  } else if (OB_FAIL(replay_lock_(mem_ctx, lock_op, scn))) {
    LOG_WARN("replay lock failed", K(ret), K(lock_op));
  } else {
    // do nothing
  }
  LOG_DEBUG("ObMemtable::replay_lock finish.", K(ret), K(lock_op), K(ls_id_));
  return ret;
}

int ObLockMemtable::register_into_deadlock_detector_(
    const ObStoreCtx &ctx,
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

int ObLockMemtable::unregister_from_deadlock_detector_(const ObTableLockOp &lock_op)
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

int ObLockMemtable::check_and_set_tx_lock_timeout_(const ObMvccAccessCtx &acc_ctx)
{
  int ret = OB_SUCCESS;
  ObMemtableCtx *mem_ctx = acc_ctx.get_mem_ctx();
  int64_t current_ts = common::ObClockGenerator::getClock();
  if (mem_ctx->get_lock_wait_start_ts() <= 0) {
    mem_ctx->set_lock_wait_start_ts(current_ts);
  } else {
    int64_t lock_wait_start_ts = mem_ctx->get_lock_wait_start_ts();
    int64_t lock_wait_expire_ts = acc_ctx.eval_lock_expire_ts(lock_wait_start_ts);
    if (current_ts >= lock_wait_expire_ts) {
      ret = OB_ERR_EXCLUSIVE_LOCK_CONFLICT;
      LOG_WARN(
        "exclusive lock conflict", K(ret), K(acc_ctx), K(lock_wait_start_ts), K(lock_wait_expire_ts), K(current_ts));
    }
  }
  return ret;
}

} // tablelock
} // transaction
} // oceanbase
