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

#include "lib/container/ob_se_array.h"
#include "storage/ls/ob_ls.h"
#include "storage/tx/ob_trans_part_ctx.h"
#include "storage/tx/ob_trans_service.h"
#include "storage/tx/ob_tx_retain_ctx_mgr.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "storage/tx_storage/ob_ls_service.h"

namespace oceanbase
{
using namespace share;

namespace transaction
{

ObAdvanceLSCkptTask::ObAdvanceLSCkptTask(share::ObLSID ls_id, SCN ckpt_ts)
{
  ;
  task_type_ = ObTransRetryTaskType::ADVANCE_LS_CKPT_TASK;
  ls_id_ = ls_id;
  target_ckpt_ts_ = ckpt_ts;
}

void ObAdvanceLSCkptTask::reset()
{
  ls_id_ = share::ObLSID::INVALID_LS_ID;
  target_ckpt_ts_.reset();
}

int ObAdvanceLSCkptTask::try_advance_ls_ckpt_ts()
{
  int ret = OB_SUCCESS;

  storage::ObLSHandle ls_handle;

  if (OB_ISNULL(MTL(ObLSService *))
      || OB_FAIL(MTL(ObLSService *)->get_ls(ls_id_, ls_handle, storage::ObLSGetMod::TRANS_MOD))
      || !ls_handle.is_valid()) {
    if (OB_SUCCESS == ret) {
      ret = OB_INVALID_ARGUMENT;
    }
    TRANS_LOG(WARN, "get ls faild", K(ret), K(MTL(ObLSService *)));
  } else if (OB_FAIL(ls_handle.get_ls()->advance_checkpoint_by_flush(target_ckpt_ts_))) {
    TRANS_LOG(WARN, "advance checkpoint ts failed", K(ret), K(ls_id_), K(target_ckpt_ts_));
  }

  if (OB_SUCC(ret)) {
    TRANS_LOG(INFO, "[RetainCtxMgr] advance ls checkpoint ts success", K(ret), K(ls_id_),
              K(target_ckpt_ts_));
  } else {
    TRANS_LOG(WARN, "[RetainCtxMgr] advance ls checkpoint ts failed", K(ret), K(ls_id_),
              K(target_ckpt_ts_));
  }

  return ret;
}

ObIRetainCtxCheckFunctor::ObIRetainCtxCheckFunctor()
{
  cause_ = RetainCause::UNKOWN;
  tx_ctx_ = nullptr;
}

ObIRetainCtxCheckFunctor::~ObIRetainCtxCheckFunctor()
{
  if (OB_NOT_NULL(tx_ctx_)) {
  }
}

int ObIRetainCtxCheckFunctor::init(ObPartTransCtx *ctx, RetainCause cause)
{
  int ret = OB_SUCCESS;
  if (ctx == nullptr || cause == RetainCause::UNKOWN) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KPC(ctx), K(cause));
    // } else if (ctx->)
  } else {
    cause_ = cause;
    tx_ctx_ = ctx;
    tx_ctx_->set_retain_cause(cause);
    tx_id_ = tx_ctx_->get_trans_id();
    ls_id_ = tx_ctx_->get_ls_id();
    // TRANS_LOG( INFO , "before inc retain ctx ref", K(ret),KPC(this),K(*tx_ctx_));
    // tx_ctx_->acquire_ctx_ref();
    // TRANS_LOG( INFO , "after inc retain ctx ref", K(ret),KPC(this),K(*tx_ctx_));
  }
  return ret;
}

int ObIRetainCtxCheckFunctor::del_retain_ctx()
{
  int ret = OB_SUCCESS;

  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), KPC(this));
  } else if (OB_FAIL(tx_ctx_->del_retain_ctx())) {
    TRANS_LOG(WARN, "del retain ctx failed", K(ret));
  } else {
    // TRANS_LOG( INFO , "before dec retain ctx ref", K(ret),KPC(this),KPC(tx_ctx_));
    // tx_ctx_->release_ctx_ref_();
    // TRANS_LOG( INFO , "after dec retain ctx ref", K(ret),KPC(this),KPC(tx_ctx_));
  }

  return ret;
}

int ObMDSRetainCtxFunctor::init(ObPartTransCtx *ctx,
                                RetainCause cause,
                                const SCN &final_log_ts)
{
  int ret = OB_SUCCESS;

  if (!final_log_ts.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(final_log_ts));
  } else if (OB_FAIL(ObIRetainCtxCheckFunctor::init(ctx, cause))) {
    TRANS_LOG(WARN, "init retain ctx check functor failed", K(ret));
  } else {
    final_log_ts_ = final_log_ts;
  }

  return ret;
}

int ObMDSRetainCtxFunctor::operator()(ObLS *ls, ObTxRetainCtxMgr *retain_mgr)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(ls) || OB_ISNULL(retain_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), KPC(ls), KPC(retain_mgr));
  } else if (tx_ctx_->get_retain_cause() == RetainCause::UNKOWN) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "find a tx ctx without retain_cause", K(ret), KPC(tx_ctx_), KPC(this));
  } else if (OB_FALSE_IT(retain_mgr->set_max_ckpt_ts(final_log_ts_))) {
  } else if (final_log_ts_ < ls->get_clog_checkpoint_scn()) {
    // only compare with ls_ckpt_ts because of flush success.
    // The downstream filters the redundant replay itself.
    ret = OB_SUCCESS;
    // can be gc
  } else {
    ret = OB_EAGAIN;
  }

  return ret;
}

bool ObMDSRetainCtxFunctor::is_valid()
{
  return ObIRetainCtxCheckFunctor::is_valid() && final_log_ts_.is_valid();
}

void ObTxRetainCtxMgr::reset()
{
  if (!retain_ctx_list_.is_empty()) {
    TRANS_LOG(INFO, "some retain ctx has not been deleted", KPC(this),
              K(retain_ctx_list_.get_first()));
  }
  max_wait_ckpt_ts_.reset();
  last_push_gc_task_ts_ = ObTimeUtility::current_time();
  retain_ctx_list_.reset();
  skip_remove_cnt_ = 0;
  reserve_allocator_.reset();
}

void *ObTxRetainCtxMgr::alloc_object(const int64_t size)
{
  return mtl_malloc(size, "RetainCtxFunc");
}

void ObTxRetainCtxMgr::free_object(void *ptr) { mtl_free(ptr); }

int ObTxRetainCtxMgr::push_retain_ctx(ObIRetainCtxCheckFunctor *retain_func, int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  bool lock_succ = false;

  ObTimeGuard tg(__func__, 1 * 1000 * 1000);
  // SpinWLockGuard guard(retain_ctx_lock_);
  if (OB_FAIL(retain_ctx_lock_.wrlock(timeout_us))) {
    if (ret == OB_TIMEOUT) {
      ret = OB_EAGAIN;
    }
    TRANS_LOG(WARN, "[RetainCtxMgr] lock retain_ctx_mgr failed", K(ret), K(timeout_us));
  } else {
    lock_succ = true;
  }
  tg.click();

  if (OB_FAIL(ret)) {
  } else if (!retain_func->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "[RetainCtxMgr] invalid argument", K(ret), KPC(retain_func));
  } else if (false == (retain_ctx_list_.add_last(retain_func))) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "[RetainCtxMgr] push back retain func failed", K(ret));
  }

  if (lock_succ) {
    int64_t tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(retain_ctx_lock_.unlock())) {
      TRANS_LOG(WARN, "[RetainCtxMgr] unlock retain_ctx_mgr failed", K(tmp_ret));
    }
  }

  return ret;
}

int ObTxRetainCtxMgr::try_gc_retain_ctx(storage::ObLS *ls)
{
  int ret = OB_SUCCESS;
  static const int64_t MAX_RUN_US = 500 * 1000;
  ObTimeGuard tg(__func__, 1 * 1000 * 1000);
  SpinWLockGuard guard(retain_ctx_lock_);
  tg.click();

  if (OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "[RetainCtxMgr] invalid argument", K(ret), KPC(ls));
  } else if (OB_FAIL(for_each_remove_(&ObTxRetainCtxMgr::try_gc_, ls, MAX_RUN_US))) {
    TRANS_LOG(WARN, "[RetainCtxMgr] try gc all retain ctx failed", K(ret), KPC(ls));
  }
  return ret;
}

int ObTxRetainCtxMgr::force_gc_retain_ctx()
{
  int ret = OB_SUCCESS;

  ObTimeGuard tg(__func__, 1 * 1000 * 1000);
  SpinWLockGuard guard(retain_ctx_lock_);
  tg.click();

  int64_t retry_force_gc_times = 0;
  while (retain_ctx_list_.get_size() > 0) {
    const int64_t before_remove_count = retain_ctx_list_.get_size();
    if (OB_FAIL(for_each_remove_(&ObTxRetainCtxMgr::force_gc_, nullptr, INT64_MAX))) {
      TRANS_LOG(WARN, "[RetainCtxMgr] force gc all retain ctx faild", K(ret));
    }
    TRANS_LOG(INFO, "[RetainCtxMgr] try to force gc all retain ctx", K(ret),
              K(retry_force_gc_times), K(before_remove_count), KPC(this));
    retry_force_gc_times++;
  }

  return ret;
}

int ObTxRetainCtxMgr::print_retain_ctx_info(share::ObLSID ls_id)
{
  int ret = OB_SUCCESS;

  ObTimeGuard tg(__func__, 1 * 1000 * 1000);
  SpinRLockGuard guard(retain_ctx_lock_);
  tg.click();
  if (!retain_ctx_list_.is_empty()) {
    TRANS_LOG(INFO, "[RetainCtxMgr] print retain ctx", K(ls_id), KPC(this),
              KPC(retain_ctx_list_.get_first()), KPC(retain_ctx_list_.get_last()));
  }
  return ret;
}

void ObTxRetainCtxMgr::try_advance_retain_ctx_gc(share::ObLSID ls_id)
{
  int ret = OB_SUCCESS;

  const int64_t tenant_id = MTL_ID();
  int64_t CUR_LS_CNT = MTL(ObLSService *)->get_ls_map()->get_ls_count();
  if (CUR_LS_CNT == 0) {
    CUR_LS_CNT = 1;
  }
  const int64_t IDLE_GC_INTERVAL = 30 * 60 * 1000 * 1000; // 30 min
  // const int64_t MIN_RETAIN_CTX_GC_THRESHOLD = 1000;
  const int64_t MIN_RETAIN_CTX_GC_THRESHOLD = ::oceanbase::lib::get_tenant_memory_limit(tenant_id)
                                              / sizeof(ObPartTransCtx) / CUR_LS_CNT / 100;

  ObTimeGuard tg(__func__, 1 * 1000 * 1000);
  SpinRLockGuard guard(retain_ctx_lock_);
  tg.click();

  const int64_t cur_time = ObTimeUtility::current_time();

  ObAdvanceLSCkptTask *task = nullptr;
  if (retain_ctx_list_.get_size() <= 0) {
    // do nothing
  } else if (retain_ctx_list_.get_size()
                 <= std::min(MAX_PART_CTX_COUNT / 10 / CUR_LS_CNT, MIN_RETAIN_CTX_GC_THRESHOLD)
             && (OB_INVALID_TIMESTAMP == last_push_gc_task_ts_
                 || (OB_INVALID_TIMESTAMP != last_push_gc_task_ts_
                     && cur_time - last_push_gc_task_ts_ <= IDLE_GC_INTERVAL))) {
    // do nothing
  } else if (OB_ISNULL(task = static_cast<ObAdvanceLSCkptTask *>(
                           share::mtl_malloc(sizeof(ObAdvanceLSCkptTask), "ad_ckpt_task")))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TRANS_LOG(WARN, "alloc ObAdvanceLSCkptTask failed", K(ret));
  } else if (OB_FALSE_IT(new (task) ObAdvanceLSCkptTask(ls_id, max_wait_ckpt_ts_))) {
  } else if (MTL(ObTransService *)->push(task)) {
    TRANS_LOG(INFO, "[RetainCtxMgr] push ObAdvanceLSCkptTask failed", K(ret),
              K(retain_ctx_list_.get_size()), K(last_push_gc_task_ts_), K(ls_id),
              K(MIN_RETAIN_CTX_GC_THRESHOLD), KPC(this));
  } else {
    TRANS_LOG(INFO, "[RetainCtxMgr] push ObAdvanceLSCkptTask success", K(ret),
              K(retain_ctx_list_.get_size()), K(last_push_gc_task_ts_), K(ls_id),
              K(MIN_RETAIN_CTX_GC_THRESHOLD), KPC(this));
    last_push_gc_task_ts_ = cur_time;
  }
  UNUSED(ret);
}

int ObTxRetainCtxMgr::remove_ctx_func_(ObIRetainCtxCheckFunctor *remove_iter)
{
  int ret = OB_SUCCESS;

  // TRANS_LOG(INFO, "before remove ctx func", K(ret), KPC(*remove_iter));
  if (remove_iter == nullptr || !(remove_iter->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "[RetainCtxMgr] invalid argument", K(ret), KPC(this));
  } else if (OB_FAIL(remove_iter->del_retain_ctx())) {
    TRANS_LOG(WARN, "[RetainCtxMgr] del retain ctx failed", K(ret));
  } else {
    // *remove_iter is equal to node_->data_ !!!
    // earse node_!!!
    if (nullptr == (retain_ctx_list_.remove(remove_iter))) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "[RetainCtxMgr] retain ctx remove failed", K(ret));
    }
    if (OB_SUCC(ret)) {
      free_object(remove_iter);
    }
  }
  return ret;
}

int ObTxRetainCtxMgr::for_each_remove_(RetainFuncHandler remove_handler,
                                       storage::ObLS *ls,
                                       const int64_t max_run_us)
{
  int ret = OB_SUCCESS;
  int64_t iter_count = 0;
  int64_t remove_count = 0;
  const int64_t origin_count = retain_ctx_list_.get_size();
  bool need_remove = false;
  const int64_t start_ts = ObTimeUtility::current_time();
  // RetainCtxList::iterator iter = retain_ctx_list_.end();
  // RetainCtxList::iterator last_remove_iter = retain_ctx_list_.end();
  // ObIRetainCtxCheckFunctor *iter = nullptr;

  DLIST_FOREACH_REMOVESAFE(iter, retain_ctx_list_)
  {
    if (iter == nullptr) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "[RetainCtxMgr] empty retain ctx functor", K(ret), KPC(this));
    } else if (iter_count < skip_remove_cnt_) {
      // do nothing
    } else if (OB_FAIL((this->*remove_handler)(iter, need_remove, ls))) {
      TRANS_LOG(WARN, "[RetainCtxMgr] execute remove_handler failed", K(ret), KPC(iter), KPC(this));
    } else if (need_remove) {
      if (OB_FAIL(remove_ctx_func_(iter))) {
        TRANS_LOG(WARN, "[RetainCtxMgr] remove from retain_ctx_list_ failed", K(ret), KPC(iter),
                  KPC(this));
      } else {
        remove_count++;
      }
    }
    iter_count++;

    const int64_t use_ts = ObTimeUtility::current_time() - start_ts;
    if (use_ts > max_run_us) {
      TRANS_LOG_RET(WARN, OB_ERR_TOO_MUCH_TIME,
                    "[RetainCtxMgr] remove retain ctx use too much time", K(use_ts),
                    K(remove_count));
      break;
    }
  }

  if (OB_FAIL(ret) || origin_count <= iter_count) {
    skip_remove_cnt_ = 0;
  } else {
    skip_remove_cnt_ = iter_count - remove_count;
  }

  return ret;
}

int ObTxRetainCtxMgr::try_gc_(ObIRetainCtxCheckFunctor *func_ptr,
                              bool &need_remove,
                              storage::ObLS *ls)
{
  int ret = OB_SUCCESS;
  need_remove = false;

  if (OB_ISNULL(func_ptr)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "[RetainCtxMgr] invalid argument", K(ret), KPC(func_ptr));
  } else if (OB_SUCC((*func_ptr)(ls, this))) {
    need_remove = true;
  } else if (OB_EAGAIN == ret) {
    // do nothing
    TRANS_LOG(TRACE, "[RetainCtxMgr] we can not remove retain ctx at this time", K(ret),
              KPC(func_ptr));
    ret = OB_SUCCESS;
  } else {
    TRANS_LOG(WARN, "[RetainCtxMgr] execute retain_ctx_check_functor failed ", K(ret),
              KPC(func_ptr));
  }

  return ret;
}

int ObTxRetainCtxMgr::force_gc_(ObIRetainCtxCheckFunctor *func_ptr,
                                bool &need_remove,
                                storage::ObLS *ls)
{
  int ret = OB_SUCCESS;

  UNUSED(ls);
  need_remove = true;

  return ret;
}

} // namespace transaction

} // namespace oceanbase
