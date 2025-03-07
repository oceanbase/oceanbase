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

#include "log_shared_queue_thread.h"
#include "log_shared_task.h"
#include "palf_env_impl.h"                    // PalfEnvImpl

namespace oceanbase
{
namespace palf
{
LogSharedQueueTh::LogSharedQueueTh()
    : submit_log_tg_id_(-1),
      shared_tg_id_(-1),
      palf_env_impl_(NULL),
      is_inited_(false)
{}

LogSharedQueueTh::~LogSharedQueueTh()
{
  destroy();
}

int LogSharedQueueTh::init(IPalfEnvImpl *palf_env_impl)
{
  int ret = OB_SUCCESS;
  const int submit_log_tg_id = lib::TGDefIDs::LogSubmitLogQueueTh;
  const int shared_tg_id = lib::TGDefIDs::LogSharedQueueTh;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    PALF_LOG(ERROR, "LogSharedQueueTh has inited", K(ret));
  } else if (NULL == palf_env_impl) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "Invalid argument", K(ret), KP(palf_env_impl));
  } else if (OB_FAIL(TG_CREATE_TENANT(submit_log_tg_id, submit_log_tg_id_, MAX_LOG_HANDLE_TASK_NUM))) {
    PALF_LOG(WARN, "LogSharedQueueTh TG_CREATE failed", K(ret));
  } else if (OB_FAIL(TG_CREATE_TENANT(shared_tg_id, shared_tg_id_, MAX_LOG_HANDLE_TASK_NUM))) {
    PALF_LOG(WARN, "LogSharedQueueTh TG_CREATE failed", K(ret));
  } else {
    palf_env_impl_ = palf_env_impl;
    is_inited_ = true;
    PALF_LOG(INFO, "LogSharedQueueTh init success", K(ret), K(submit_log_tg_id_), KP(palf_env_impl));
  }
  if (OB_FAIL(ret) && OB_INIT_TWICE != ret) {
    destroy();
  }
  return ret;
}

int LogSharedQueueTh::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "LogSharedQueueTh not inited", K(ret));
  } else if (OB_FAIL(TG_SET_HANDLER_AND_START(submit_log_tg_id_, *this))) {
    PALF_LOG(ERROR, "start LogSharedQueueTh failed", K(ret));
  } else if (OB_FAIL(TG_SET_HANDLER_AND_START(shared_tg_id_, *this))) {
    PALF_LOG(ERROR, "start LogSharedQueueTh failed", K(ret));
  } else {
    PALF_LOG(INFO, "start LogSharedQueueTh success", K(ret),
        K(submit_log_tg_id_));
  }
  return ret;
}

int LogSharedQueueTh::stop()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "LogSharedQueueTh not inited", K(ret));
  } else {
    TG_STOP(submit_log_tg_id_);
    TG_STOP(shared_tg_id_);
    PALF_LOG(INFO, "stop LogSharedQueueTh success", K(submit_log_tg_id_));
  }
  return ret;
}

int LogSharedQueueTh::wait()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "LogSharedQueueTh not inited", K(ret));
  } else {
    TG_WAIT(submit_log_tg_id_);
    TG_WAIT(shared_tg_id_);
    PALF_LOG(INFO, "wait LogSharedQueueTh success", K(submit_log_tg_id_));
  }
  return ret;
}

void LogSharedQueueTh::destroy()
{
  stop();
  wait();
  is_inited_ = false;
  if (-1 != submit_log_tg_id_) {
    TG_DESTROY(submit_log_tg_id_);
    PALF_LOG(INFO, "destroy LogSharedQueueTh success", K(submit_log_tg_id_));
  }
  if (-1 != shared_tg_id_) {
    TG_DESTROY(shared_tg_id_);
    PALF_LOG(INFO, "destroy LogSharedQueueTh success", K(shared_tg_id_));
  }
  shared_tg_id_ = -1;
  submit_log_tg_id_ = -1;
}

int LogSharedQueueTh::push_submit_log_task(LogHandleSubmitTask *task)
{
  int ret = OB_SUCCESS;
  if (NULL == task) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    int64_t print_log_interval = OB_INVALID_TIMESTAMP;
    while (OB_FAIL(TG_PUSH_TASK(submit_log_tg_id_, task))) {
      if (OB_IN_STOP_STATE == ret) {
        PALF_LOG(WARN, "thread_pool has been stopped, skip task", K(ret), K_(submit_log_tg_id), KPC(task));
        break;
      } else if (palf_reach_time_interval(5 * 1000 * 1000, print_log_interval)) {
        PALF_LOG(ERROR, "push task failed", K(ret), K_(submit_log_tg_id), KPC(task));
      }
      ob_usleep(1000);
    }
  }
  return ret;
}

int LogSharedQueueTh::push_task(LogSharedTask *task)
{
  int ret = OB_SUCCESS;
  if (NULL == task) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    int64_t print_log_interval = OB_INVALID_TIMESTAMP;
    if (OB_FAIL(TG_PUSH_TASK(shared_tg_id_, task))) {
      if (OB_IN_STOP_STATE == ret) {
        PALF_LOG(WARN, "thread_pool has been stopped, skip task", K(ret), K_(shared_tg_id), KPC(task));
      }
    }
  }
  return ret;
}

void LogSharedQueueTh::handle(void *task)
{
  int ret = OB_SUCCESS;
  LogSharedTask *log_shared_task = reinterpret_cast<LogSharedTask*>(task);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "LogSharedQueueTh not inited", K(ret));
  } else if (OB_ISNULL(log_shared_task)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "Invalid argument", K(ret), K(log_shared_task));
  } else if (OB_FAIL(log_shared_task->do_task(palf_env_impl_))) {
    PALF_LOG(WARN, "LogSharedTask handle_task failed", K(ret), KPC(log_shared_task));
  } else {
    PALF_LOG(TRACE, "LogSharedQueueTh handle success", KPC(log_shared_task));
  }
  if (OB_NOT_NULL(log_shared_task)) {
    log_shared_task->free_this(palf_env_impl_);
  }
}

int LogSharedQueueTh::get_tg_id() const
{
  return submit_log_tg_id_;
}

} // end namespace palf
} // end namespace oceanbase
