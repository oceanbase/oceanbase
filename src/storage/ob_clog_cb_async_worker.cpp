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

#include "ob_clog_cb_async_worker.h"
#include "storage/ob_partition_service.h"

using namespace oceanbase::common;
namespace oceanbase {
namespace storage {
int ObCLogCallbackAsyncWorker::init(ObPartitionService* ptt_svr)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_ISNULL(ptt_svr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(ptt_svr));
  } else if (OB_FAIL(M2SQueueThread::init(MAX_TASK_NUM, IDLE_INTERVAL))) {
    LOG_WARN("init M2SQueueThread failed", K(ret));
  } else if (OB_FAIL(free_queue_.init(MAX_TASK_NUM))) {
    LOG_WARN("init fixed queue of tasks failed", K(ret));
  } else {
    int64_t size = sizeof(ObCLogCallbackAsyncTask) * MAX_TASK_NUM;
    ObMemAttr attr(common::OB_SERVER_TENANT_ID, ObModIds::OB_CHECK2REMOVE);
    if (nullptr == (tasks_ = (ObCLogCallbackAsyncTask*)ob_malloc(size, attr))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("no memory", K(ret), K(size));
    } else {
      new (tasks_) ObCLogCallbackAsyncTask[MAX_TASK_NUM];
      for (int64_t i = 0; OB_SUCC(ret) && i < MAX_TASK_NUM; ++i) {
        if (OB_FAIL(free_queue_.push(&tasks_[i]))) {
          LOG_WARN("fail to push task to free queue", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        ptt_svr_ = ptt_svr;
        is_inited_ = true;
      }
    }
  }
  if (IS_NOT_INIT) {
    destroy();
  }
  return ret;
}

void ObCLogCallbackAsyncWorker::destroy()
{
  M2SQueueThread::destroy();
  free_queue_.destroy();
  if (nullptr != tasks_) {
    ob_free(tasks_);
    tasks_ = nullptr;
  }
  ptt_svr_ = nullptr;
  is_inited_ = false;
}

void ObCLogCallbackAsyncWorker::handle(void* task, void* pdata)
{
  UNUSED(pdata);
  int ret = OB_SUCCESS;
  ObIPartitionGroupGuard guard;
  ObIPartitionGroup* partition = NULL;
  ObCLogCallbackAsyncTask* this_task = static_cast<ObCLogCallbackAsyncTask*>(task);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not init", K(ret));
  } else if (OB_ISNULL(this_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(ret), KP(this_task));
  } else if (!this_task->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid task", K(ret), K(*this_task));
  } else if (!this_task->pg_key_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K_(this_task->pg_key), K(ret));
  } else if (OB_FAIL(ObPartitionService::get_instance().get_partition(this_task->pg_key_, guard))) {
    STORAGE_LOG(WARN, "get partition failed", K_(this_task->pg_key), K(ret));
  } else if (OB_UNLIKELY(NULL == (partition = guard.get_partition_group()))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "get partition failed", K(ret), K(*this_task));
  } else {
    switch (this_task->log_type_) {
      case ObStorageLogType::OB_LOG_OFFLINE_PARTITION:
      case ObStorageLogType::OB_LOG_OFFLINE_PARTITION_V2:
        ret = ptt_svr_->schema_drop_partition(*this_task);
        break;
      case ObStorageLogType::OB_LOG_REMOVE_PARTITION_FROM_PG:
        ret = ptt_svr_->remove_partition_from_pg(
            false, this_task->pg_key_, this_task->partition_key_, this_task->log_id_);
        break;
      default:
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not supported log type", K(ret), K(*this_task));
        break;
    }

    if (OB_SUCC(ret)) {
      LOG_INFO("handle task success", K(*this_task));
    } else {
      LOG_WARN("handle task failed", K(ret), K(*this_task));
    }
  }

  if (nullptr != this_task) {
    free_task(this_task);
    this_task = nullptr;
  }
}

int ObCLogCallbackAsyncWorker::push_task(const ObCLogCallbackAsyncTask& task)
{
  int ret = OB_SUCCESS;
  ObCLogCallbackAsyncTask* this_task = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (!task.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(task));
  } else if (OB_FAIL(get_task(this_task))) {
    STORAGE_LOG(WARN, "get task failed", K(ret));
  } else if (OB_ISNULL(this_task)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "this task shouldn't be null here", K(ret), KP(this_task));
  } else {
    this_task->deep_copy(task);
    while (OB_FAIL(M2SQueueThread::push(this_task))) {
      if (OB_EAGAIN == ret) {
        STORAGE_LOG(INFO, "thread queue full, sleep and try again", K(ret), K(*this_task));
        usleep(RETRY_INTERVAL);
      } else {
        STORAGE_LOG(WARN, "push task to thread queue failed", K(ret), K(*this_task));
        break;
      }
    }
  }
  return ret;
}

int ObCLogCallbackAsyncWorker::get_task(ObCLogCallbackAsyncTask*& task)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_FAIL(free_queue_.pop(task))) {
    STORAGE_LOG(WARN, "pop free task failed", K(ret));
  }
  return ret;
}

void ObCLogCallbackAsyncWorker::free_task(ObCLogCallbackAsyncTask* task)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(ERROR, "not init", K(ret));
  } else if (OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "invalid argument", K(ret), KP(task));
  } else if (OB_FAIL(free_queue_.push(task))) {
    STORAGE_LOG(ERROR, "push free task to free queue failed", K(ret), K(*task));
  }
}

}  // namespace storage
}  // namespace oceanbase
