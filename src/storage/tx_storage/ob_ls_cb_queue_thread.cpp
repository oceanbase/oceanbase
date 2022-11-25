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

#define USING_LOG_PREFIX STORAGE

#include "storage/tx_storage/ob_ls_cb_queue_thread.h"
#include "share/ob_force_print_log.h"
#include "lib/oblog/ob_log_module.h"
#include "share/ob_thread_mgr.h"
#include "storage/ls/ob_ls.h"


namespace oceanbase
{
namespace storage
{
int64_t ObRoleChangeCbQueueThread::QUEUE_THREAD_NUM = max(4, get_cpu_count() / 5);

ObRoleChangeCbQueueThread::ObRoleChangeCbQueueThread()
  : inited_(false), tenant_id_(OB_INVALID_ID), free_queue_(),
    tg_id_(-1), free_task_num_(0)
{
}

ObRoleChangeCbQueueThread::~ObRoleChangeCbQueueThread()
{
  destroy();
}

void ObRoleChangeCbQueueThread::destroy()
{
  inited_ = false;
  tenant_id_ = OB_INVALID_ID;
  ObRoleChangeCbTask *task = NULL;
  free_task_num_ = 0;
  while (OB_EAGAIN != free_queue_.pop((common::ObLink*&)task)) {
    ob_free(task);
  }
  LOG_INFO("ObRoleChangeCbQueueThread destroy");
}

int ObRoleChangeCbQueueThread::alloc_task(ObRoleChangeCbTask *&task)
{
  int ret = OB_SUCCESS;
  int64_t size = sizeof(ObRoleChangeCbTask);
  ObMemAttr attr(tenant_id_, ObModIds::OB_CALLBACK_TASK);
  if (NULL == (task = (ObRoleChangeCbTask *)ob_malloc(size, attr))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
      LOG_ERROR("no memory, failed to alloc call back task", K(ret), K(size));
    }
  }
  return ret;
}
int ObRoleChangeCbQueueThread::init(uint64_t tenant_id, int tg_id)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObRoleChangeCbQueueThread has already been inited", K(ret));
  } else if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(TG_CREATE_TENANT(tg_id, tg_id_))) {
    LOG_WARN("ObSimpleThreadPool create tg", K(ret));
  } else if (OB_FAIL(TG_SET_HANDLER_AND_START(tg_id_, *this))) {
    LOG_WARN("ObSimpleThreadPool inited error.", K(ret));
  } else {
    tenant_id_ = tenant_id;
    inited_ = true;
  }
  if (OB_SUCCESS != ret && !inited_) {
    destroy();
  }
  return ret;
}

int ObRoleChangeCbQueueThread::get_task(ObRoleChangeCbTask *&task)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRoleChangeCbQueueThread not init", K(ret));
  } else {
    while (true) {
      // will always get one
      if (OB_SUCC(free_queue_.pop((common::ObLink*&)task))) {
        ATOMIC_DEC(&free_task_num_);
        break;
      } else if (OB_SUCC(alloc_task(task))) {
        break;
      } else {
        PAUSE();
      }
    }
  }
  return ret;
}

void ObRoleChangeCbQueueThread::free_task(ObRoleChangeCbTask *task)
{
  int tmp_ret = OB_SUCCESS;
  if (!inited_) {
    tmp_ret = OB_NOT_INIT;
    LOG_ERROR("ObRoleChangeCbQueueThread not init", K(tmp_ret));
  } else if (NULL == task) {
    tmp_ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("ObRoleChangeCbQueueThread invalid argument", K(tmp_ret), K(task));
  } else {
    task->~ObRoleChangeCbTask();
    int old_val = 0;
    int new_val = 0;
    while (true) {
      old_val = ATOMIC_LOAD(&free_task_num_);
      new_val = old_val + 1;
      if (old_val >= MAX_FREE_TASK_NUM) {
        ob_free(task);
        break;
      } else if (ATOMIC_BCAS(&free_task_num_, old_val, new_val)) {
        // will always push success.
        free_queue_.push(task);
        break;
      } else {
        // do nothing
      }
    }
  }
}

int ObRoleChangeCbQueueThread::push(const ObRoleChangeCbTask *task)
{
  int ret = OB_SUCCESS;
  ObRoleChangeCbTask *saved_task = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRoleChangeCbQueueThread is not initialized", K(ret));
  } else if (NULL == task) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(task));
  } else if (OB_SUCCESS != (ret = get_task(saved_task))) {
    LOG_WARN("get free task failed", K(ret));
  } else if (NULL == saved_task) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected error, saved_task shouldn't be null", K(ret));
  } else {
    *saved_task = *task;
    if (OB_SUCC(TG_PUSH_TASK(tg_id_, saved_task))) {
      LOG_DEBUG("callback task", K(*saved_task));
    } else {
      free_task(saved_task);
      saved_task = NULL;
    }
  }
  return ret;
}

void ObRoleChangeCbQueueThread::handle(void *task)
{
  ObRoleChangeCbTask *saved_task = static_cast<ObRoleChangeCbTask *>(task);
  ObRoleHandler *role_handler = nullptr;
  if (NULL == saved_task) {
    LOG_WARN("invalid callback task", KP(saved_task));
  } else if (OB_ISNULL(role_handler = saved_task->role_handler_)) {
    LOG_ERROR("role handler should not be null", K(saved_task), KP(role_handler));
  } else {
    int tmp_ret = OB_SUCCESS;
    SET_OB_LOG_TRACE_MODE();
    switch (saved_task->task_type_) {
    case LS_ON_LEADER_TAKEOVER_TASK: {
      tmp_ret = role_handler->on_leader_takeover(*saved_task);
      break;
    }
    case LS_LEADER_REVOKE_TASK: {
      tmp_ret = role_handler->internal_leader_revoke(*saved_task);
      break;
    }
    case LS_LEADER_TAKEOVER_TASK: {
      tmp_ret = role_handler->internal_leader_takeover(*saved_task);
      break;
    }
    case LS_LEADER_ACTIVE_TASK: {
      tmp_ret = role_handler->internal_leader_active(*saved_task);
      break;
    }
    default:
      LOG_WARN("invalid task type", K(saved_task->task_type_));
      break;
    }
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("ls callback failed", K(*saved_task), K(tmp_ret));
    } else {
      LOG_INFO("run ls callback task successfully", K(*saved_task));
    }
    const int64_t RETRY_PRINT_CNT = 4; // The task that was retried will only log 4 times.
    // 1. Log if success.
    // 2. Log at the first 4 retries.
    // 3. Log if the error code changes.
    if (OB_SUCCESS == tmp_ret
      || saved_task->retry_cnt_ <= RETRY_PRINT_CNT
      || tmp_ret != saved_task->ret_code_) {
      PRINT_OB_LOG_TRACE_BUF(COMMON, INFO);
    }
    CANCLE_OB_LOG_TRACE_MODE();
    if (OB_SUCCESS != tmp_ret
      && saved_task->retry_cnt_ > 0
      && (0 == saved_task->retry_cnt_ % 1000)) {
      LOG_WARN("ls callback task retry so many times", K(*saved_task), K(tmp_ret));
    }
    free_task(saved_task);
    saved_task = NULL;
  }
}

} // namespace storage
} // namespace oceanbase
