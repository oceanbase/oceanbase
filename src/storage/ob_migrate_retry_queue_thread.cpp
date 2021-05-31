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

#include "storage/ob_migrate_retry_queue_thread.h"
#include "storage/ob_partition_service.h"
#include "lib/oblog/ob_log_module.h"
#include "share/ob_thread_mgr.h"

namespace oceanbase {
using namespace common;
namespace storage {

ObMigrateRetryQueueThread::ObMigrateRetryQueueThread()
    : inited_(false), partition_service_(NULL), free_queue_(), tasks_(NULL), tg_id_(-1)
{}

ObMigrateRetryQueueThread::~ObMigrateRetryQueueThread()
{
  destroy();
}

void ObMigrateRetryQueueThread::destroy()
{
  inited_ = false;
  partition_service_ = NULL;
  if (NULL != tasks_) {
    ob_free(tasks_);
  }
  tasks_ = NULL;
  STORAGE_LOG(INFO, "ObMigrateRetryQueueThread destroy");
}

int ObMigrateRetryQueueThread::init(ObPartitionService* partition_service, int tg_id)
{
  int ret = OB_SUCCESS;
  const int64_t max_task_num = OB_MAX_PARTITION_NUM_PER_SERVER * 2;
  tg_id_ = tg_id;
  if (inited_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObMigrateRetryQueueThread has already been inited", K(ret));
  } else if (NULL == partition_service) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(partition_service));
  } else if (OB_FAIL(TG_SET_HANDLER_AND_START(tg_id_, *this))) {
    STORAGE_LOG(WARN, "ObSimpleThreadPool inited error.", K(ret));
  } else if (OB_SUCCESS != (ret = free_queue_.init(max_task_num))) {
    STORAGE_LOG(WARN, "initialize fixed queue of tasks failed", K(ret));
  } else {
    int64_t size = sizeof(ObMigrateRetryTask) * max_task_num;
    ObMemAttr attr(common::OB_SERVER_TENANT_ID, ObModIds::OB_CALLBACK_TASK);
    if (NULL == (tasks_ = (ObMigrateRetryTask*)ob_malloc(size, attr))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(ERROR, "no memory", K(ret), K(size));
    } else {
      for (int64_t i = max_task_num - 1; OB_SUCC(ret) && i >= 0; --i) {
        if (OB_SUCCESS != (ret = free_queue_.push(&tasks_[i]))) {
          STORAGE_LOG(WARN, "push free task failed", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        partition_service_ = partition_service;
        inited_ = true;
      }
    }
  }
  if (OB_SUCCESS != ret && !inited_) {
    destroy();
  }
  return ret;
}

int ObMigrateRetryQueueThread::get_task(ObMigrateRetryTask*& task)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObMigrateRetryQueueThread not init", K(ret));
  } else if (OB_SUCCESS != (ret = free_queue_.pop(task))) {
    STORAGE_LOG(WARN, "pop free task failed", K(ret));
  }
  return ret;
}

void ObMigrateRetryQueueThread::free_task(ObMigrateRetryTask* task)
{
  int tmp_ret = OB_SUCCESS;
  if (!inited_) {
    tmp_ret = OB_NOT_INIT;
    STORAGE_LOG(ERROR, "ObMigrateRetryQueueThread not init", K(tmp_ret));
  } else if (NULL == task) {
    tmp_ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "ObMigrateRetryQueueThread invalid argument", K(tmp_ret), K(task));
  } else if (OB_SUCCESS != (tmp_ret = free_queue_.push(task))) {
    STORAGE_LOG(ERROR, "push free task failed", K(tmp_ret));
  }
}

int ObMigrateRetryQueueThread::push(const ObMigrateRetryTask* task)
{
  int ret = OB_SUCCESS;
  ObMigrateRetryTask* saved_task = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObMigrateRetryQueueThread is not initialized", K(ret));
  } else if (NULL == task) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(task));
  } else if (OB_SUCCESS != (ret = get_task(saved_task))) {
    STORAGE_LOG(WARN, "get free task failed", K(ret));
  } else if (NULL == saved_task) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected error, saved_task shouldn't be null", K(ret));
  } else {
    *saved_task = *task;
    if (OB_SUCC(TG_PUSH_TASK(tg_id_, saved_task))) {
      STORAGE_LOG(DEBUG, "callback task", K(*saved_task));
    } else {
      free_task(saved_task);
      saved_task = NULL;
    }
  }
  return ret;
}

void ObMigrateRetryQueueThread::handle(void* task)
{
  ObMigrateRetryTask* saved_task = static_cast<ObMigrateRetryTask*>(task);
  if (NULL == saved_task) {
    STORAGE_LOG(WARN, "invalid callback task", KP(saved_task));
  } else {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = partition_service_->process_migrate_retry_task(*saved_task))) {
      if (EXECUTE_COUNT_PER_SEC(10)) {
        STORAGE_LOG(WARN, "process_migrate_retry_task failed", K(*saved_task), K(tmp_ret));
      }
    } else {
      STORAGE_LOG(INFO, "process_migrate_retry_task successfully", K(*saved_task));
    }
    free_task(saved_task);
    saved_task = NULL;
  }
}

}  // namespace storage
}  // namespace oceanbase
