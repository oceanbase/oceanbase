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

#include "ob_trans_timer.h"
#include "ob_trans_ctx.h"
#include "ob_trans_ctx_mgr.h"
#include "ob_trans_service.h"

namespace oceanbase
{
namespace transaction
{

using namespace common;

int ObTransTimeoutTask::init(ObTransCtx *ctx)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    TRANS_LOG(WARN, "ObTransTimeoutTask inited twice");
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(ctx)) {
    TRANS_LOG(WARN, "invalid argument", KP(ctx));
    ret = OB_INVALID_ARGUMENT;
  } else {
    is_inited_ = true;
    ctx_ = ctx;
  }

  return ret;
}

void ObTransTimeoutTask::destroy()
{
  ObITransTimer *timer = NULL;
  ObTransService *trans_service = NULL;
  if (is_inited_) {
    is_inited_ = false;
    if (NULL != ctx_ && (NULL != (trans_service = ctx_->get_trans_service()))) {
      if (NULL != (timer = &(trans_service->get_trans_timer()))) {
        (void)timer->unregister_timeout_task(*this);
      }
    }
  }
}

void ObTransTimeoutTask::reset()
{
  ObTimeWheelTask::reset();
  ObITimeoutTask::reset();
  is_inited_ = false;
  ctx_ = NULL;
}

uint64_t ObTransTimeoutTask::hash() const
{
  uint64_t hv = 0;
  if (NULL == ctx_) {
    TRANS_LOG_RET(ERROR, OB_INVALID_ARGUMENT, "ctx is null, unexpected error", KP_(ctx));
  } else {
    const ObTransID &trans_id = ctx_->get_trans_id();
    if (!trans_id.is_valid()) {
      TRANS_LOG_RET(WARN, OB_INVALID_ARGUMENT, "ObTransID is invalid", K(trans_id));
    } else {
      hv = trans_id.hash();
    }
  }
  return hv;
}

void ObTransTimeoutTask::runTimerTask()
{
  int tmp_ret = OB_SUCCESS;
  ObLSTxCtxMgr *ls_tx_ctx_mgr = NULL;

  if (!is_inited_) {
    TRANS_LOG_RET(WARN, OB_NOT_INIT, "ObTransTimeoutTask not inited");
  } else if (NULL == ctx_) {
    TRANS_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "ctx is null, unexpected error", KP_(ctx));
    tmp_ret = OB_ERR_UNEXPECTED;
  } else {
    if (OB_SUCCESS != (tmp_ret = ctx_->handle_timeout(delay_))) {
      TRANS_LOG_RET(WARN, tmp_ret, "handle timeout error", "ret", tmp_ret, "context", *ctx_);
    }
    if (NULL == (ls_tx_ctx_mgr = ctx_->get_ls_tx_ctx_mgr())) {
      TRANS_LOG_RET(WARN, OB_ERR_UNEXPECTED, "get partition mgr error", "context", *ctx_);
    } else {
      (void)ls_tx_ctx_mgr->revert_tx_ctx(ctx_);
    }
  }
}

int ObTxTimeoutTask::init(ObTxDesc *tx_desc, ObTransService* txs)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    TRANS_LOG(WARN, "ObTransTimeoutTask inited twice");
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(tx_desc) || OB_ISNULL(txs)) {
    TRANS_LOG(WARN, "invalid argument", KP(tx_desc));
    ret = OB_INVALID_ARGUMENT;
  } else {
    is_inited_ = true;
    tx_desc_ = tx_desc;
    txs_ = txs;
  }

  return ret;
}

void ObTxTimeoutTask::reset()
{
  ObTimeWheelTask::reset();
  ObITimeoutTask::reset();
  is_inited_ = false;
  tx_desc_ = NULL;
  txs_ = NULL;
}

uint64_t ObTxTimeoutTask::hash() const
{
  uint64_t hv = 0;
  if (NULL == tx_desc_) {
    TRANS_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "tx_desc is null, unexpected error", KP_(tx_desc));
  } else {
    const ObTransID &trans_id = tx_desc_->tid();
    if (!trans_id.is_valid()) {
      TRANS_LOG_RET(WARN, OB_INVALID_ARGUMENT, "ObTransID is invalid", K(trans_id));
    } else {
      hv = trans_id.hash();
    }
  }
  return hv;
}

void ObTxTimeoutTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    TRANS_LOG(WARN, "ObTxTimeoutTask not inited", KPC(this));
  } else if (OB_ISNULL(tx_desc_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "ctx is null, unexpected error", KPC(this));
  } else if (OB_ISNULL(txs_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "txs_ is null, unexpected error", KPC(this));
  } else {
    // NOTE: save tx_desc_/txs_ is required
    // because the handle_tx_commit_timeout may cause tx terminate
    // its execution and release all resource include current object
    // it is unsafe to use any member field after call handle func.
    ObTransService *txs = txs_;
    ObTxDesc *tx_desc = tx_desc_;
    DEFER({ txs->release_tx_ref(*tx_desc); });
    if (tx_desc_->is_xa_trans() && tx_desc_->is_sub2pc()) {
      if (OB_FAIL(txs_->handle_timeout_for_xa(*tx_desc_, delay_))) {
        TRANS_LOG(WARN, "fail to handle timeout", K(ret), KPC_(tx_desc));
      }
    } else {
      if (OB_FAIL(txs_->handle_tx_commit_timeout(*tx_desc_, delay_))) {
        TRANS_LOG(WARN, "handle timeout fail", K(ret), KPC_(tx_desc));
      }
    }
  }
}

int ObTransTimer::init(const char *timer_name)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(timer_name)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret));
  } else if (is_inited_) {
    TRANS_LOG(WARN, "ObTransTimer inited twice");
    ret = OB_INIT_TWICE;
  } else if (OB_FAIL(tw_.init(TRANS_TIMEOUT_TASK_PRECISION_US, get_thread_num_(), timer_name))) {
    TRANS_LOG(ERROR, "transaction timer init error", KR(ret));
  } else {
    TRANS_LOG(INFO, "transaction timer inited success");
    is_inited_ = true;
  }

  return ret;
}

int ObTransTimer::start()
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    TRANS_LOG(WARN, "ObTransTimer is not inited");
    ret = OB_NOT_INIT;
  } else if (is_running_) {
    TRANS_LOG(WARN, "ObTransTimer is already running");
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(tw_.start())) {
    TRANS_LOG(WARN, "ObTimeWheel start error", KR(ret));
  } else {
    is_running_ = true;
    TRANS_LOG(INFO, "ObTransTimer start success");
  }

  return ret;
}

int ObTransTimer::stop()
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    TRANS_LOG(WARN, "ObTransTimer is not inited");
    ret = OB_NOT_INIT;
  } else if (!is_running_) {
    TRANS_LOG(WARN, "ObTransTimer already has stopped");
    ret = OB_NOT_RUNNING;
  } else if (OB_FAIL(tw_.stop())) {
    TRANS_LOG(WARN, "ObTimeWheel stop error", KR(ret));
  } else {
    is_running_ = false;
    TRANS_LOG(INFO, "ObTransTimer stop success");
  }

  return ret;
}

int ObTransTimer::wait()
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    TRANS_LOG(WARN, "ObTransTimer is not inited");
    ret = OB_NOT_INIT;
  } else if (is_running_) {
    TRANS_LOG(WARN, "ObTransTimer is already running");
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(tw_.wait())) {
    TRANS_LOG(WARN, "ObTimeWheel wait error", KR(ret));
  } else {
    TRANS_LOG(INFO, "ObTransTimer wait success");
  }

  return ret;
}

void ObTransTimer::destroy()
{
  int tmp_ret = OB_SUCCESS;

  if (is_inited_) {
    if (is_running_) {
      if (OB_SUCCESS != (tmp_ret = stop())) {
        TRANS_LOG_RET(WARN, tmp_ret, "ObTransTimer stop error", K(tmp_ret));
      } else if (OB_SUCCESS != (tmp_ret = wait())) {
        TRANS_LOG_RET(WARN, tmp_ret, "ObTransTimer wait error", K(tmp_ret));
      } else {
        // do nothing
      }
    }
    is_inited_ = false;
    tw_.destroy();
    TRANS_LOG(INFO, "ObTransTimer destroyed");
  }
}

// task registered: is scheduled to TimeWheel
int ObTransTimer::register_timeout_task(ObITimeoutTask &task,
    const int64_t delay)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    TRANS_LOG(WARN, "ObTransTimer not been inited");
    ret = OB_NOT_INIT;
  } else if (!is_running_) {
    TRANS_LOG(WARN, "ObTransTimer is not running");
    ret = OB_NOT_RUNNING;
  } else if (delay < 0) {
    TRANS_LOG(WARN, "invalid argument", K(delay));
    ret = OB_INVALID_ARGUMENT;
  } else if (task.is_registered()) {
    ret = OB_TIMER_TASK_HAS_SCHEDULED;
  } else if (OB_FAIL(tw_.schedule(&task, delay))) {
    TRANS_LOG(WARN, "register timeout task error", KR(ret), K(task));
  } else {
    task.set_registered(true);
    task.set_delay(delay);
  }

  return ret;
}

int ObTransTimer::unregister_timeout_task(ObITimeoutTask &task)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    TRANS_LOG(WARN, "ObTransTimer not been inited");
    ret = OB_NOT_INIT;
  // do not check transaction timer is running or not
  // we can unregister timeout task successful always
  } else if (!task.is_registered()) {
    ret = OB_TIMER_TASK_HAS_NOT_SCHEDULED;
  } else if (OB_FAIL(tw_.cancel(&task))) {
    TRANS_LOG(DEBUG, "timewheel cancel task error", KR(ret), K(task));
    if (OB_TIMER_TASK_HAS_NOT_SCHEDULED == ret) {
      // task has picked out from timeWheel and begin to run or has ran completed
      task.set_registered(false);
    }
  } else {
    task.set_registered(false);
  }

  return ret;
}

int ObDupTableLeaseTimer::init()
{
  int ret = OB_SUCCESS;
  const char *timer_name = "DupTbLease";

  if (is_inited_) {
    TRANS_LOG(WARN, "ObDupTableLeaseTimer inited twice");
    ret = OB_INIT_TWICE;
  } else if (OB_FAIL(tw_.init(DUP_TABLE_TIMEOUT_TASK_PRECISION_US, 1, timer_name))) {
    TRANS_LOG(ERROR, "dup table lease timer init error", K(ret));
  } else {
    TRANS_LOG(INFO, "dup table lease timer inited success");
    is_inited_ = true;
  }

  return ret;
}

} // transaction
} // oceanbase
