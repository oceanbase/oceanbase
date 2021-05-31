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

#include "ob_trans_task_worker.h"
#include "lib/utility/utility.h"
#include "ob_trans_part_ctx.h"

namespace oceanbase {

using namespace storage;
using namespace clog;

namespace transaction {

void BigTransCallbackTask::reset()
{
  ObTransTask::reset();
  partition_.reset();
  log_type_ = storage::ObStorageLogType::OB_LOG_UNKNOWN;
  log_id_ = 0;
  log_timestamp_ = OB_INVALID_TIMESTAMP;
}

int BigTransCallbackTask::make(const common::ObPartitionKey& partition, const int64_t log_type, const int64_t log_id,
    const int64_t log_timestamp, ObTransCtx* ctx)
{
  int ret = OB_SUCCESS;

  if (!partition.is_valid() || !storage::ObStorageLogTypeChecker::is_trans_log(log_type) || log_id <= 0 ||
      log_timestamp <= 0 || OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(partition), K(log_type), K(log_id), K(log_timestamp), KP(ctx));
  } else if (OB_FAIL(ObTransTask::make(ObTransRetryTaskType::BIG_TRANS_CALLBACK_TASK))) {
    TRANS_LOG(WARN, "make big trans callback task fail", KR(ret));
  } else {
    partition_ = partition;
    log_type_ = log_type;
    log_id_ = log_id;
    log_timestamp_ = log_timestamp;
    ctx_ = ctx;
  }

  return ret;
}

bool BigTransCallbackTask::is_valid() const
{
  return partition_.is_valid() &&
         (log_type_ & OB_LOG_SP_TRANS_COMMIT || log_type_ & OB_LOG_SP_TRANS_ABORT || log_type_ & OB_LOG_TRANS_COMMIT ||
             log_type_ & OB_LOG_TRANS_ABORT) &&
         log_id_ > 0 && log_timestamp_ > 0 && NULL != ctx_;
}

int ObTransTaskWorker::init()
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    TRANS_LOG(WARN, "ObTransTaskWorker inited twice");
    ret = OB_INIT_TWICE;
  } else if (OB_FAIL(TG_SET_HANDLER_AND_START(lib::TGDefIDs::TransTaskWork, *this))) {
    TRANS_LOG(WARN, "thread pool init error", K(ret));
  } else {
    is_inited_ = true;
    TRANS_LOG(INFO, "ObTransTaskWorker inited success");
  }

  return ret;
}

int ObTransTaskWorker::start()
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    TRANS_LOG(WARN, "ObTransTaskWorker is not inited");
    ret = OB_NOT_INIT;
  } else if (is_running_) {
    TRANS_LOG(WARN, "ObTransTaskWorker is already running");
    ret = OB_ERR_UNEXPECTED;
  } else {
    is_running_ = true;
    TRANS_LOG(INFO, "ObTransTaskWorker start success");
  }

  return ret;
}

void ObTransTaskWorker::stop()
{
  if (!is_inited_) {
    TRANS_LOG(WARN, "ObTransTaskWorker is not inited");
  } else if (!is_running_) {
    TRANS_LOG(WARN, "ObTransTaskWorker already has been stopped");
  } else {
    TG_STOP(lib::TGDefIDs::TransTaskWork);
    is_running_ = false;
    TRANS_LOG(INFO, "ObTransTaskWorker stop success");
  }
}

void ObTransTaskWorker::wait()
{
  if (!is_inited_) {
    TRANS_LOG(WARN, "ObTransTaskWorker is not inited");
  } else if (is_running_) {
    TRANS_LOG(WARN, "ObTransTaskWorker is running");
  } else {
    TRANS_LOG(INFO, "ObTransTaskWorker wait success");
  }
}

void ObTransTaskWorker::destroy()
{
  if (is_inited_) {
    if (is_running_) {
      stop();
      wait();
    }
    is_inited_ = false;
    TRANS_LOG(INFO, "ObTransTaskWorker destroyed");
  }
}

void ObTransTaskWorker::handle(void* task)
{
  int ret = OB_SUCCESS;

  if (NULL == task) {
    TRANS_LOG(ERROR, "task is null", KP(task));
  } else {
    ObTransTask* tmp_task = static_cast<ObTransTask*>(task);
    if (tmp_task->get_task_type() == ObTransRetryTaskType::BIG_TRANS_CALLBACK_TASK) {
      BigTransCallbackTask* big_trans_cb_task = static_cast<BigTransCallbackTask*>(task);
      ObPartTransCtx* part_ctx = static_cast<ObPartTransCtx*>(big_trans_cb_task->get_trans_ctx());
      // part_ctx may be released already, it's unsafe to access the content of this pointer
      const ObTransID trans_id = part_ctx->get_trans_id();
      if (NULL == part_ctx) {
        TRANS_LOG(WARN, "part ctx is null, unexpected error", KP(part_ctx));
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(part_ctx->callback_big_trans(big_trans_cb_task->get_partition(),
                     big_trans_cb_task->get_log_type(),
                     big_trans_cb_task->get_log_id(),
                     big_trans_cb_task->get_log_timestamp()))) {
        if (EXECUTE_COUNT_PER_SEC(16)) {
          TRANS_LOG(
              WARN, "callback big trans error", KR(ret), "log_type", big_trans_cb_task->get_log_type(), K(trans_id));
        }
        BigTransCallbackTaskFactory::release(big_trans_cb_task);
        big_trans_cb_task = NULL;
      } else {
        TRANS_LOG(INFO,
            "handle callback big trans task",
            KP(task),
            "log_type",
            big_trans_cb_task->get_log_type(),
            K(trans_id));
        BigTransCallbackTaskFactory::release(big_trans_cb_task);
        big_trans_cb_task = NULL;
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "ObTransRetryTaskType invalid", KR(ret), K(tmp_task));
    }
  }
}

int ObTransTaskWorker::submit_big_trans_callback_task(const ObPartitionKey& partition, const int64_t log_type,
    const uint64_t log_id, const int64_t log_timestamp, ObTransCtx* ctx)
{
  int ret = OB_SUCCESS;

  BigTransCallbackTask* task = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObClogAdapter not inited", KR(ret));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
  } else if (OB_ISNULL(task = BigTransCallbackTaskFactory::alloc())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TRANS_LOG(
        WARN, "alloc big trans callback task is null", KR(ret), K(partition), K(log_type), K(log_id), K(log_timestamp));
  } else if (OB_FAIL(task->make(partition, log_type, log_id, log_timestamp, ctx))) {
    TRANS_LOG(WARN, "make BackfillNopLogTask fail", KR(ret), K(partition), K(log_type), K(log_id), K(log_timestamp));
  } else if (!task->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid task", KR(ret), "big_trans_callback_task", *(task));
  } else if (OB_FAIL(TG_PUSH_TASK(lib::TGDefIDs::TransTaskWork, task))) {
    TRANS_LOG(WARN, "push big trans callback task fail", K(ret), "big_trans_callback_task", *(task));
  } else {
    TRANS_LOG(INFO, "push big trans callback task succ", KR(ret), "big_trans_callback_task", *(task));
  }
  if (OB_FAIL(ret) && (NULL != task)) {
    BigTransCallbackTaskFactory::release(task);
    task = NULL;
  }

  return ret;
}

}  // namespace transaction
}  // namespace oceanbase
