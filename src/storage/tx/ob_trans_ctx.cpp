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

#define USING_LOG_PREFIX TRANS
#include "ob_trans_ctx.h"
#include "ob_trans_ctx_mgr.h"
#include "ob_trans_service.h"
#include "storage/memtable/ob_memtable_interface.h"
#include "share/ob_cluster_version.h"
#include "share/rc/ob_context.h"

namespace oceanbase
{
using namespace common;
using namespace memtable;
using namespace share;

namespace transaction
{

void ObTransCtx::get_ctx_guard(CtxLockGuard &guard, uint8_t mode)
{
  guard.set(lock_, mode);
}

void ObTransCtx::print_trace_log()
{
  print_trace_log_();
}

void ObTransCtx::print_trace_log_()
{
  FORCE_PRINT_TRACE(tlog_, "[force print]");
}

void ObTransCtx::before_unlock(CtxLockArg &arg)
{
  arg.trans_id_ = trans_id_;
  arg.ls_id_ = ls_id_;
  opid_++;
  if (has_pending_callback_) {
    arg.commit_cb_ = commit_cb_;
    arg.has_pending_callback_ = has_pending_callback_;
    has_pending_callback_ = false;
  }

  if (need_retry_redo_sync_by_task_) {
    arg.need_retry_redo_sync_ = true;
    need_retry_redo_sync_by_task_ = false;
  }

  if (elr_handler_.is_elr_prepared()
      && NULL != (arg.p_mt_ctx_ = elr_handler_.get_memtable_ctx())) {
    elr_handler_.set_memtable_ctx(NULL);
  }
}

void ObTransCtx::after_unlock(CtxLockArg &arg)
{
  int ret = OB_SUCCESS;
  if (NULL != arg.p_mt_ctx_) {
    arg.p_mt_ctx_->elr_trans_preparing();
    // Subtract ref to avoid the core dump caused by memory collection
    // in the ending transaction context in the subsequent process after unlocking
    (void)ls_tx_ctx_mgr_->revert_tx_ctx_without_lock(this);
  }
  if (arg.need_retry_redo_sync_) {
    (void)MTL(ObTransService *)->retry_redo_sync_by_task(arg.trans_id_, arg.ls_id_);
  }

  if (arg.has_pending_callback_) {
    int64_t remaining_wait_interval_us = get_remaining_wait_interval_us_();
    if (0 == remaining_wait_interval_us) {
      if (OB_FAIL(arg.commit_cb_.callback())) {
        TRANS_LOG(WARN, "end transaction callback failed", KR(ret), "context", *this);
      }
      REC_TRANS_TRACE_EXT2(tlog_, end_trans_cb, OB_Y(ret),
                           OB_ID(arg1), arg.commit_cb_.ret_,
                           OB_ID(arg2), arg.commit_cb_.commit_version_,
                           OB_ID(async), false);
    } else {
      // register asynchronous callback task
      ObTxCommitCallbackTask *task = NULL;
      if (OB_ISNULL(task = ObTxCommitCallbackTaskFactory::alloc())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TRANS_LOG(WARN, "alloc memory failed", KR(ret), K(*this));
      } else if (OB_FAIL(task->make(ObTransRetryTaskType::END_TRANS_CB_TASK,
                                    arg.commit_cb_,
                                    trans_need_wait_wrap_.get_receive_gts_ts(),
                                    trans_need_wait_wrap_.get_need_wait_interval_us()))) {
        TRANS_LOG(WARN, "make ObEndTransCallbackTask error", KR(ret), K(*task), K(*this));
      } else if (OB_ISNULL(trans_service_)) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "trans service is NULL", K(ret));
      } else if (OB_FAIL(trans_service_->push(task))) {
        TRANS_LOG(WARN, "push task error", KR(ret), K(*task), K(*this));
      } else {
        // do nothing
      }
      if (OB_FAIL(ret)) {
        if (NULL != task) {
          ObTxCommitCallbackTaskFactory::release(task);
        }
        // if fail to register asynchronous callback, continue to use synchronous callback
        while (get_remaining_wait_interval_us_() > 0) {
          ob_usleep(get_remaining_wait_interval_us_());
        }
        if (OB_FAIL(arg.commit_cb_.callback())) {
          TRANS_LOG(WARN, "end transaction callback failed", KR(ret), "context", *this);
        }
      }
      REC_TRANS_TRACE_EXT2(tlog_, end_trans_cb, OB_Y(ret),
                           OB_ID(arg1), arg.commit_cb_.ret_,
                           OB_ID(arg2), arg.commit_cb_.commit_version_,
                           OB_ID(async), true);
    }
  }
}

void ObTransCtx::print_trace_log_if_necessary_()
{
  // freectx
  if (!is_exiting_) {
    TRANS_LOG_RET(WARN, OB_ERROR, "ObPartTransCtx not exiting", "context", *this, K(lbt()));
    FORCE_PRINT_TRACE(tlog_, "[trans debug] ");
  }

  if (is_slow_query_()) {
    static ObMiniStat::ObStatItem item("long trans statistics", 60 * 1000 * 1000);
    ObMiniStat::stat(item);
    FORCE_PRINT_TRACE(tlog_, "[long trans] ");
  } else if (OB_UNLIKELY(trans_id_ % 128 == 1)) {
    FORCE_PRINT_TRACE(tlog_, "[trans sampling] ");
  } else {
    PRINT_TRACE(tlog_);
  }
}

void ObTransCtx::set_exiting_()
{
  int tmp_ret = OB_SUCCESS;

  if (!is_exiting_) {
    is_exiting_ = true;
    print_trace_log_if_necessary_();
    ls_tx_ctx_mgr_->dec_active_tx_count();

    const int64_t ctx_ref = get_ref();
    if (NULL == ls_tx_ctx_mgr_) {
      TRANS_LOG_RET(ERROR, tmp_ret, "ls_tx_ctx_mgr_ is null, unexpected error", KP(ls_tx_ctx_mgr_), "context", *this);
    } else {
      ls_tx_ctx_mgr_->del_tx_ctx(this);
      TRANS_LOG(DEBUG, "transaction exiting", "context", *this, K(lbt()));
      REC_TRANS_TRACE_EXT2(tlog_, exiting, OB_ID(ref), ctx_ref, OB_ID(arg1), session_id_);
    }
  }
}

bool ObTransCtx::is_slow_query_() const
{
  return ObClockGenerator::getClock() >=
      (ctx_create_time_ + ObServerConfig::get_instance().trace_log_slow_query_watermark);
}

void ObTransCtx::set_stc_(const MonotonicTs stc)
{
  if (0 == stc_.mts_) {
    stc_ = stc;
    REC_TRANS_TRACE_EXT2(tlog_, set_stc, OB_ID(stc), stc_.mts_);
  }
}

void ObTransCtx::set_stc_by_now_()
{
  if (0 == stc_.mts_) {
    stc_ = MonotonicTs::current_time();
    REC_TRANS_TRACE_EXT2(tlog_, set_stc, OB_ID(stc), stc_.mts_);
  }
}

MonotonicTs ObTransCtx::get_stc_()
{
  if (0 == stc_.mts_) {
    stc_ = MonotonicTs::current_time();
    REC_TRANS_TRACE_EXT2(tlog_, set_stc_by_get, OB_ID(stc), stc_.mts_);
  }
  return stc_;
}

ObITsMgr *ObTransCtx::get_ts_mgr_()
{
  return trans_service_->get_ts_mgr();
}


bool ObTransCtx::has_callback_scheduler_()
{
  return !commit_cb_.is_enabled() // callback scheduler has accomplished by others
    || commit_cb_.is_inited();    // callback has been defered
}

// callback scheduler commit result
int ObTransCtx::defer_callback_scheduler_(const int retcode, const SCN &commit_version)
{
  int ret = OB_SUCCESS;
  if (!commit_cb_.is_enabled()) {
    TRANS_LOG(WARN, "commit_cb disabled, skip callback", K(ret), K_(commit_cb));
  } else if (commit_cb_.is_inited()) {
    ret = OB_INIT_TWICE;
    TRANS_LOG(WARN, "inited twice", K(ret), K_(commit_cb));
  } else {
    commit_cb_.init(trans_service_, trans_id_, retcode, commit_version);
    has_pending_callback_ = true;
  }
  return ret;
}

int ObTransCtx::prepare_commit_cb_for_role_change_(const int cb_ret, ObTxCommitCallback *&cb_list)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(commit_cb_.init(trans_service_, trans_id_, cb_ret, share::SCN()))) {
    TRANS_LOG(WARN, "init commit cb fail", K(ret), KPC(this));
  } else if (OB_FAIL(commit_cb_.link(this, cb_list))) {
    TRANS_LOG(WARN, "link commit cb fail", K(ret), KPC(this));
  } else {
    cb_list = &commit_cb_;
  }
  return ret;
}

void ObTransCtx::generate_request_id_()
{
  const int64_t request_id = ObClockGenerator::getClock();
  if (OB_INVALID_TIMESTAMP == request_id_ || request_id > request_id_) {
    request_id_ = request_id;
  } else {
    ++request_id_;
  }
}

int ObTransCtx::register_timeout_task_(const int64_t interval_us)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(interval_us < 0)) {
    TRANS_LOG(WARN, "invalid argument", K(interval_us));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(timer_)) {
    TRANS_LOG(ERROR, "transaction timer is null", K_(trans_id));
    ret = OB_ERR_UNEXPECTED;
    // add ref first, and then register the time task
  } else if (OB_ISNULL(ls_tx_ctx_mgr_)) {
    TRANS_LOG(ERROR, "ls_tx_ctx_mgr_ is null, unexpected error", KP(ls_tx_ctx_mgr_), K_(trans_id));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(acquire_ctx_ref_())) {
    TRANS_LOG(WARN, "get transaction ctx for inc ref error", KR(ret), K_(trans_id));
  } else {
    if (OB_FAIL(timer_->register_timeout_task(timeout_task_, interval_us))) {
      TRANS_LOG(WARN, "register timeout task error", KR(ret), K(interval_us), K_(trans_id));
      // in case of registration failure, you need to cancel ref
      (void)ls_tx_ctx_mgr_->revert_tx_ctx_without_lock(this);
    }
  }
  if (OB_FAIL(ret)) {
    REC_TRANS_TRACE_EXT2(tlog_, register_timeout_task, OB_ID(ret), ret,
                         OB_ID(ref), get_ref());
  }
  return ret;
}

int ObTransCtx::unregister_timeout_task_()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(timer_)) {
    TRANS_LOG(ERROR, "transaction timer is null", K_(trans_id));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(timer_->unregister_timeout_task(timeout_task_))) {
    // rewrite ret
    if (OB_TIMER_TASK_HAS_NOT_SCHEDULED == ret) {
      ret = OB_SUCCESS;
    }
  } else {
    // just dec ctx ref
    if (NULL == ls_tx_ctx_mgr_) {
      TRANS_LOG(ERROR, "ls_tx_ctx_mgr_ is null, unexpected error", KP(ls_tx_ctx_mgr_), K_(trans_id));
      ret = OB_ERR_UNEXPECTED;
    } else {
      (void)ls_tx_ctx_mgr_->revert_tx_ctx_without_lock(this);
    }
  }
  if (OB_FAIL(ret)) {
    REC_TRANS_TRACE_EXT2(tlog_, unregister_timeout_task, OB_ID(ret), ret,
                         OB_ID(ref), get_ref());
  }
  return ret;
}

void ObTransCtx::update_trans_2pc_timeout_()
{
  const int64_t timeout_new = 2 * trans_2pc_timeout_;

  if (MAX_TRANS_2PC_TIMEOUT_US > timeout_new) {
    trans_2pc_timeout_ = timeout_new;
  } else {
    trans_2pc_timeout_ = MAX_TRANS_2PC_TIMEOUT_US;
  }
}

int ObTransCtx::set_app_trace_info_(const ObString &app_trace_info)
{
  int ret = OB_SUCCESS;
  const int64_t len = app_trace_info.length();

  if (OB_UNLIKELY(len < 0) || OB_UNLIKELY(len > OB_MAX_TRACE_ID_BUFFER_SIZE)) {
    TRANS_LOG(WARN, "invalid argument", "context", *this);
    ret = OB_INVALID_ARGUMENT;
  } else if (0 == trace_info_.get_app_trace_info().length()) {
    // set for the first time
    if (OB_FAIL(trace_info_.set_app_trace_info(app_trace_info))) {
      TRANS_LOG(WARN, "set app trace info error", K(ret), K(app_trace_info), K(*this));
    }
  } else if (trace_info_.get_app_trace_info().length() != app_trace_info.length()) {
    // in big trans case, leader may change if redo log is not persisted successfully
    TRANS_LOG(WARN, "different app trace info", K(ret), K(app_trace_info), "context", *this, K(lbt()));
  } else {
    // do nothing
  }
  return ret;
} 

int ObTransCtx::set_app_trace_id_(const ObString &app_trace_id)
{
  int ret = OB_SUCCESS;
  const int64_t len = app_trace_id.length();

  if (OB_UNLIKELY(len < 0) || OB_UNLIKELY(len > OB_MAX_TRACE_ID_BUFFER_SIZE)) {
    TRANS_LOG(WARN, "invalid argument", K(app_trace_id), "context", *this);
    ret = OB_INVALID_ARGUMENT;
  } else if (0 == trace_info_.get_app_trace_id().length()) {
    // set for the first time
    if (OB_FAIL(trace_info_.set_app_trace_id(app_trace_id))) {
      TRANS_LOG(WARN, "set app trace id error", K(ret), K(app_trace_id), K(*this));
    }
  } else if (trace_info_.get_app_trace_id().length() != app_trace_id.length()) {
    // in big trans case, leader may change if redo log is not persisted successfully
    TRANS_LOG(WARN, "different app trace id", K(ret), K(app_trace_id), "context", *this);
  } else {
    // do nothing
  }
  return ret;
}

void ObTransCtx::release_ctx_ref()
{
  ls_tx_ctx_mgr_->revert_tx_ctx_without_lock(this);
}

} // transaction
} // oceanbase
