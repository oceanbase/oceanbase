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

#include "share/deadlock/ob_deadlock_detector_mgr.h"
#include "storage/memtable/ob_memtable_context.h"    // ObMemtableCtx
#include "storage/tablelock/ob_table_lock_deadlock.h"
#include "storage/tx/ob_trans_deadlock_adapter.h"
#include "storage/tx_storage/ob_ls_handle.h" 
#include "storage/tx/ob_trans_part_ctx.h"
#include "storage/tx_storage/ob_ls_map.h"
#include "storage/tx_storage/ob_ls_service.h"

namespace oceanbase
{
using namespace share;
using namespace share::detector;
using namespace memtable;

namespace transaction
{
namespace tablelock
{
OB_SERIALIZE_MEMBER(ObTransLockPartID,
                    trans_id_,
                    lock_id_);
OB_SERIALIZE_MEMBER(ObTransLockPartBlockCallBack,
                    ls_id_,
                    lock_op_);

int ObTxLockPartOnDetectOp::operator() (
    const common::ObIArray<share::detector::ObDetectorInnerReportInfo> &info,
    const int64_t self_idx)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLSHandle handle;
  ObLS *ls = nullptr;
  ObPartTransCtx *ctx = nullptr;
  if (OB_UNLIKELY(!lock_part_id_.is_valid()) ||
      OB_UNLIKELY(!ls_id_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid member", K(ret), K(lock_part_id_), K(ls_id_));
  } else if (OB_FAIL(MTL(ObLSService*)->get_ls(ls_id_,
                                               handle,
                                               ObLSGetMod::TABLELOCK_MOD))) {
    LOG_WARN("get ls failed", K(ret), K(ls_id_));
  } else if (OB_ISNULL(ls = handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be null", K(ret));
  } else if (OB_FAIL(ls->get_tx_ctx(lock_part_id_.trans_id_,
                                    true, /* does not check leader*/
                                    ctx))) {
    LOG_WARN("get tx ctx failed", K(ret), K(lock_part_id_));
  } else {
    // tell the tx lock part it has been killed.
    ctx->set_table_lock_killed();
    if (OB_SUCCESS != (tmp_ret = ls->revert_tx_ctx(ctx))) {
      LOG_ERROR("revert tx ctx failed, there may be ref leak", K(tmp_ret), KPC(ctx));
    }
  }
  LOG_INFO("tx lock part killed.", K(ret), K(lock_part_id_));
  return ret;
}

int ObTxLockPartOnDetectOp::set(
    const ObTransLockPartID &lock_part_id,
    const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!lock_part_id.is_valid()) ||
      OB_UNLIKELY(!ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(lock_part_id), K(ls_id));
  } else {
    lock_part_id_ = lock_part_id;
    ls_id_ = ls_id;
  }
  return ret;
}

void ObTxLockPartOnDetectOp::reset()
{
  lock_part_id_.reset();
  ls_id_.reset();
}

static void release_buffers(char *&buffer, char *&buffer2)
{
  if (OB_NOT_NULL(buffer)) {
    ob_free(buffer);
  }
  if (OB_NOT_NULL(buffer2)) {
    ob_free(buffer2);
  }
  buffer = nullptr;
  buffer2 = nullptr;
}

static int alloc_buffers(char *&buffer, char *&buffer2)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == (buffer =
      (char*)ob_malloc(DEFAULT_LOCK_DEADLOCK_BUFFER_LENGTH, "deadlockCB")))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate buffer memory failed", K(ret));
  } else if (OB_UNLIKELY(nullptr == (buffer2 =
      (char*)ob_malloc(DEFAULT_LOCK_DEADLOCK_BUFFER_LENGTH, "deadlockCB")))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate buffer2 memory failed", K(ret));
  } else {
    // do nothing
  }

  if (OB_FAIL(ret)) {
    release_buffers(buffer, buffer2);
  }
  return ret;
}

int ObTxLockPartCollectCallBack::operator()(ObDetectorUserReportInfo &info)
{
  int ret = OB_SUCCESS;
  char * buffer = nullptr;
  char * resource = nullptr;
  if (OB_FAIL(alloc_buffers(buffer, resource))) {
    LOG_WARN("alloc buffers failed", K(ret));
  } else {
    ObSharedGuard<char> temp_uniqe_guard;
    int step = 0;
    (void) tx_lock_part_id_.trans_id_.to_string(buffer,
                                                DEFAULT_LOCK_DEADLOCK_BUFFER_LENGTH);
    (void) tx_lock_part_id_.lock_id_.to_string(resource,
                                               DEFAULT_LOCK_DEADLOCK_BUFFER_LENGTH);
    if (++step && OB_FAIL(temp_uniqe_guard.assign((char*)"tablelock", [](char*){}))) {
    } else if (++step && OB_FAIL(info.set_module_name(temp_uniqe_guard))) {
    } else if (++step && OB_FAIL(temp_uniqe_guard.assign(buffer,
                                                         [](char* buffer)
                                                         { ob_free(buffer);}))) {
    } else if (FALSE_IT(buffer = nullptr)) {
      // make sure the buffer will be released if failed before assigned to a unique guard.
    } else if (++step && OB_FAIL(info.set_visitor(temp_uniqe_guard))) {
    } else if (++step && OB_FAIL(temp_uniqe_guard.assign(resource,
                                                         [](char* buffer)
                                                         { ob_free(buffer);}))) {
    } else if (FALSE_IT(resource = nullptr)) {
    } else if (++step && OB_FAIL(info.set_resource(temp_uniqe_guard))) {
    } else if (++step && OB_FAIL(info.set_extra_info("", ""))) {
    } else {}
    if (OB_FAIL(ret)) {
      release_buffers(buffer, resource);
      LOG_WARN("get string failed in deadlock", KR(ret), K(step));
    }
  }
  return ret;
}

int ObTransLockPartBlockCallBack::operator()(
    common::ObIArray<ObDependencyResource> &depend_res,
    bool &need_remove)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  ObLSHandle handle;
  ObLS *ls = nullptr;
  ObPartTransCtx *ctx = nullptr;
  ObMemtableCtx *mem_ctx = nullptr;
  ObTxIDSet conflict_tx_set;

  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("block callback is not valid", K(ret), KPC(this));
  } else if (OB_FAIL(MTL(ObLSService*)->get_ls(ls_id_,
                                               handle,
                                               ObLSGetMod::TABLELOCK_MOD))) {
    if (OB_NOT_RUNNING == ret ||
        OB_LS_NOT_EXIST == ret) {
      // ls is removing or removed. should never be here.
      LOG_ERROR("the ls need clean all the trans before it is removed",
                K(ret), K_(ls_id));
    } else {
      LOG_WARN("get ls failed", K(ret), K(ls_id_));
    }
  } else if (OB_ISNULL(ls = handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be null", K(ret));
  } else if (OB_FAIL(ls->get_tx_ctx(lock_op_.create_trans_id_,
                                    true, /* does not check leader*/
                                    ctx))) {
    LOG_WARN("get tx ctx failed", K(ret), K(lock_op_));
    if (OB_TRANS_CTX_NOT_EXIST == ret) {
      // the tx may be killed, do not check conflict again, and remove it from
      // deadlock detector.
      need_remove = true;
      ret = OB_SUCCESS;
    }
  } else {
    if (OB_ISNULL(mem_ctx = ctx->get_memtable_ctx())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("mem ctx should not be null", K(ret));
    } else if (OB_FAIL(ls->check_lock_conflict(mem_ctx,
                                               lock_op_,
                                               conflict_tx_set,
                                               false /* does not include finished tx */))) {
      if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
        LOG_WARN("check lock conflict failed", K(ret), K_(lock_op));
      }
    }
    if (OB_SUCC(ret)) {
      // we should not remove the dependency because there may has new dependency now.
      // need_remove = true;
    } else if (OB_TRY_LOCK_ROW_CONFLICT == ret) {
      ret = OB_SUCCESS;
      // NOTE: out trans lock and in trans lock table lock will not conflict with
      // DML lock, because a trans will kill itself if it conflict with DML lock.
      // we should not remove the dependency because there may has new dependency now.
      // need_remove = (conflict_tx_set.count() == 0) ? true : false;
      ObTxIDSet::const_iterator_t it = conflict_tx_set.begin();
      ObAddr block_trans_addr;
      UserBinaryKey binary_key;
      for(;OB_SUCC(ret) && it != conflict_tx_set.end(); ++it)
      {
        // Note: yanyuan.cxf how to deal with the completed trans?
        //       we should not see the finished trans.
        //       if a trans is finished now. the get dependency process will fail, and
        //       we need get dependency next time.
        const ObTransID &block_trans_id = *it;
        binary_key.set_user_key(block_trans_id);
        if (OB_FAIL(ObTransDeadlockDetectorAdapter::get_trans_scheduler_info_on_participant(
            block_trans_id, ls_id_, block_trans_addr))) {
          LOG_WARN("get block trans scheduler address failed", K(ret), K_(ls_id), K(block_trans_id));
        } else {
          ObDependencyResource resource(block_trans_addr,
                                        binary_key);
          if (OB_FAIL(depend_res.push_back(resource))) {
            LOG_WARN("push into array failed.", K(ret), K(resource));
          }
        }
        LOG_DEBUG("ObTransLockPartBlockCallBack get dependency", K(ret), K(lock_op_),
                  K(block_trans_id), K(block_trans_addr));
      }
    }
    if (OB_SUCCESS != (tmp_ret = ls->revert_tx_ctx(ctx))) {
      LOG_ERROR("revert tx ctx failed, there may be ref leak", K(tmp_ret),
                KPC(ctx));
    }
  }
  LOG_DEBUG("ObTransLockPartBlockCallBack", K(ret));
  return ret;
}

int ObTransLockPartBlockCallBack::init(
    const share::ObLSID &ls_id,
    const ObTableLockOp &lock_op)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_valid())) {
    ret = OB_INIT_TWICE;
    LOG_WARN("call back has been inited", K(ret), KPC(this));
  } else if (OB_UNLIKELY(!ls_id.is_valid()) || OB_UNLIKELY(!lock_op.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id), K(lock_op));
  } else {
    ls_id_ = ls_id;
    lock_op_ = lock_op;
  }
  return ret;
}

int ObTableLockDeadlockDetectorHelper::register_trans_lock_part(
    const ObTransLockPartID &tx_lock_part_id,
    const ObLSID &ls_id,
    const ObDetectorPriority priority)
{
  int ret = OB_SUCCESS;
  if (ObDeadLockDetectorMgr::is_deadlock_enabled()) {
    ObTxLockPartOnDetectOp on_detect_op;
    ObTxLockPartCollectCallBack callback(tx_lock_part_id);

    CollectCallBack on_collect_callback(callback);

    if (OB_UNLIKELY(!tx_lock_part_id.is_valid()) ||
        OB_UNLIKELY(!ls_id.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(tx_lock_part_id), K(ls_id));
    } else if (OB_FAIL(on_detect_op.set(tx_lock_part_id,
                                        ls_id))) {
      LOG_WARN("set deadlock detect op failed", K(ret), K(tx_lock_part_id));
    } else if (OB_FAIL(MTL(ObDeadLockDetectorMgr*) ->register_key(tx_lock_part_id,
                                                                  on_detect_op,
                                                                  on_collect_callback,
                                                                  priority))) {
      LOG_WARN("register to deadlock detector failed.",
              K(ret), K(tx_lock_part_id), K(priority));
    }
    LOG_DEBUG("ObTableLockDeadlockDetectorHelper::register_trans_lock_part", K(ret), K(tx_lock_part_id), K(ls_id));
  }
  return ret;
}

int ObTableLockDeadlockDetectorHelper::unregister_trans_lock_part(
    const ObTransLockPartID &tx_lock_part_id)
{
  int ret = OB_SUCCESS;
  if (ObDeadLockDetectorMgr::is_deadlock_enabled()) {
    if (OB_UNLIKELY(!tx_lock_part_id.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(tx_lock_part_id));
    } else if (OB_FAIL(MTL(ObDeadLockDetectorMgr*)->unregister_key(tx_lock_part_id))) {
      LOG_WARN("unregister from deadlock detector failed", K(tx_lock_part_id));
    }
    LOG_DEBUG("ObTableLockDeadlockDetectorHelper::unregister_trans_lock_part", K(ret), K(tx_lock_part_id));
  }
  return ret;
}

int ObTableLockDeadlockDetectorHelper::add_parent(
    const ObTransLockPartID &tx_lock_part_id,
    const common::ObAddr &parent_addr,
    const ObTransID &parent_trans_id)
{
  int ret = OB_SUCCESS;
  if (ObDeadLockDetectorMgr::is_deadlock_enabled()) {
    if (OB_UNLIKELY(!tx_lock_part_id.is_valid()) ||
        OB_UNLIKELY(!parent_addr.is_valid()) ||
        OB_UNLIKELY(!parent_trans_id.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(tx_lock_part_id), K(parent_addr),
              K(parent_trans_id));
    } else if (OB_FAIL(MTL(ObDeadLockDetectorMgr*)->add_parent(tx_lock_part_id,
                                                              parent_addr,
                                                              parent_trans_id))) {
      LOG_WARN("add lock parent trans failed", K(ret), K(tx_lock_part_id),
              K(parent_trans_id));
    }
    LOG_DEBUG("ObTableLockDeadlockDetectorHelper::add_parent", K(ret), K(tx_lock_part_id),
              K(parent_addr), K(parent_trans_id));
  }
  return ret;
}

int ObTableLockDeadlockDetectorHelper::block(
    const ObTransLockPartID &tx_lock_part_id,
    const share::ObLSID &ls_id,
    const ObTableLockOp &lock_op)
{
  int ret = OB_SUCCESS;
  if (ObDeadLockDetectorMgr::is_deadlock_enabled()) {
    ObTransLockPartBlockCallBack block_cb;
    if (OB_UNLIKELY(!tx_lock_part_id.is_valid()) ||
        OB_UNLIKELY(!ls_id.is_valid()) ||
        OB_UNLIKELY(!lock_op.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(tx_lock_part_id), K(ls_id), K(lock_op));
    } else if (OB_FAIL(block_cb.init(ls_id,
                                    lock_op))) {
      LOG_WARN("block callback init failed", K(ret), K(ls_id), K(lock_op));
    } else {
      // WARNING: be care for the BlockCallBack fn, it may use the wrong block interface.
      detector::BlockCallBack fn = block_cb;
      if (!fn.is_valid()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("block callback invalid", K(ret), K(tx_lock_part_id), K(ls_id), K(lock_op));
      } else if (OB_FAIL(MTL(ObDeadLockDetectorMgr*)->block(tx_lock_part_id,
                                                            fn))) {
        LOG_WARN("add block failed", K(ret), K(tx_lock_part_id), K(lock_op));
      } else {
        // do nothing
      }
      LOG_DEBUG("ObTableLockDeadlockDetectorHelper::block", K(ret), K(tx_lock_part_id),
                K(ls_id), K(lock_op));
    }
  }
  return ret;
}

}
}
}
