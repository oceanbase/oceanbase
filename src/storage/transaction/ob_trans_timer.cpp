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

namespace oceanbase {
namespace transaction {

using namespace common;

int ObTransTimeoutTask::init(ObTransCtx* ctx)
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
  ObITransTimer* timer = NULL;
  ObTransService* trans_service = NULL;
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
    TRANS_LOG(ERROR, "ctx is null, unexpected error", KP_(ctx));
  } else {
    const ObTransID& trans_id = ctx_->get_trans_id();
    if (!trans_id.is_valid()) {
      TRANS_LOG(WARN, "ObTransID is invalid", K(trans_id));
    } else {
      hv = trans_id.hash();
    }
  }
  return hv;
}

void ObTransTimeoutTask::runTimerTask()
{
  int tmp_ret = OB_SUCCESS;
  ObPartitionTransCtxMgr* partition_mgr = NULL;

  if (!is_inited_) {
    TRANS_LOG(WARN, "ObTransTimeoutTask not inited");
  } else if (NULL == ctx_) {
    TRANS_LOG(ERROR, "ctx is null, unexpected error", KP_(ctx));
    tmp_ret = OB_ERR_UNEXPECTED;
  } else {
    if (OB_SUCCESS != (tmp_ret = ctx_->handle_timeout(delay_))) {
      TRANS_LOG(WARN, "handle timeout error", "ret", tmp_ret, "context", *ctx_);
    }
    if (NULL == (partition_mgr = ctx_->get_partition_mgr())) {
      TRANS_LOG(WARN, "get partition mgr error", "context", *ctx_);
    } else {
      (void)partition_mgr->revert_trans_ctx(ctx_);
    }
  }
}

int ObPrepareChangingLeaderTask::init(const int64_t expected_ts, ObTransService* txs, const ObPartitionKey& pkey,
    const ObAddr& proposal_leader, const int64_t round, const int64_t cnt)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
  } else if (0 >= expected_ts || NULL == txs || !pkey.is_valid() || !proposal_leader.is_valid() || round < 1 ||
             cnt < 0) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(
        WARN, "invalid argument", KR(ret), K(expected_ts), KP(txs), K(pkey), K(proposal_leader), K(round), K(cnt));
  } else {
    expected_ts_ = expected_ts;
    txs_ = txs;
    pkey_ = pkey;
    proposal_leader_ = proposal_leader;
    is_inited_ = true;
    round_ = round;
    cnt_ = cnt;
  }

  return ret;
}

void ObPrepareChangingLeaderTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "prepare changing leader task not inited", KR(ret));
  } else if (NULL == txs_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "txs is NULL", KR(ret));
  } else {
    ObTransMigrateWorker* trans_migrate_worker = txs_->get_trans_migrate_worker();
    if (NULL == trans_migrate_worker) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "trans migrate worker is NULL", KR(ret));
    } else if (OB_FAIL(trans_migrate_worker->push(this))) {
      TRANS_LOG(WARN, "push trans migrate task failed", KR(ret));
    } else {
      TRANS_LOG(DEBUG, "push trans migrate task success");
    }
    if (OB_FAIL(ret)) {
      op_reclaim_free(this);
    }
  }
}

int ObPrepareChangingLeaderTask::run()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "prepare changing leader task not inited", KR(ret));
  } else if (NULL == txs_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "txs is NULL", KR(ret));
  } else {
    const int64_t delay = ObTimeUtility::current_time() - expected_ts_;
    if (OB_FAIL(txs_->prepare_changing_leader(pkey_, proposal_leader_, round_, cnt_))) {
      TRANS_LOG(
          WARN, "prepare changing leader failed", KR(ret), K_(pkey), K_(proposal_leader), K(delay), K(round_), K(cnt_));
    } else if (delay >= 50000) {
      TRANS_LOG(INFO, "prepare changing leader success", K_(pkey), K_(proposal_leader), K(delay), K(round_), K(cnt_));
    } else {
      TRANS_LOG(DEBUG, "prepare changing leader success", K_(pkey), K_(proposal_leader), K(delay), K(round_), K(cnt_));
    }
  }
  op_reclaim_free(this);
  return ret;
}

uint64_t ObPrepareChangingLeaderTask::hash() const
{
  return pkey_.hash();
}

int ObTransTimer::init()
{
  int ret = OB_SUCCESS;
  const char* timer_name = "TransTimeWheel";

  if (is_inited_) {
    TRANS_LOG(WARN, "ObTransTimer inited twice");
    ret = OB_INIT_TWICE;
  } else if (OB_FAIL(tw_.init(TRANS_TIMEOUT_TASK_PRECISION_US, THREAD_NUM, timer_name))) {
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
        TRANS_LOG(WARN, "ObTransTimer stop error", K(tmp_ret));
      } else if (OB_SUCCESS != (tmp_ret = wait())) {
        TRANS_LOG(WARN, "ObTransTimer wait error", K(tmp_ret));
      } else {
        // do nothing
      }
    }
    is_inited_ = false;
    tw_.destroy();
    TRANS_LOG(INFO, "ObTransTimer destroyed");
  }
}

int ObTransTimer::register_timeout_task(ObITimeoutTask& task, const int64_t delay)
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

int ObTransTimer::unregister_timeout_task(ObITimeoutTask& task)
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
  const char* timer_name = "dup_table_lease";

  if (is_inited_) {
    TRANS_LOG(WARN, "ObDupTableLeaseTimer inited twice");
    ret = OB_INIT_TWICE;
  } else if (OB_FAIL(tw_.init(TRANS_TIMEOUT_TASK_PRECISION_US, 1, timer_name))) {
    TRANS_LOG(ERROR, "dup table lease timer init error", K(ret));
  } else {
    TRANS_LOG(INFO, "dup table lease timer inited success");
    is_inited_ = true;
  }

  return ret;
}

}  // namespace transaction
}  // namespace oceanbase
