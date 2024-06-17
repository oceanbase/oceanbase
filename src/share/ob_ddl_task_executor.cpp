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

#include <sys/prctl.h>
#include "lib/thread/ob_async_task_queue.h"
#include "ob_ddl_task_executor.h"
#include "share/ob_thread_mgr.h"

#define USING_LOG_PREFIX STORAGE

using namespace oceanbase::share;
using namespace oceanbase::common;

namespace oceanbase
{
namespace share
{
ObDDLTaskQueue::ObDDLTaskQueue()
  : task_list_(), task_set_(), lock_(ObLatchIds::DDL_LOCK), is_inited_(false), allocator_()
{
  allocator_.set_label(common::ObModIds::OB_BUILD_INDEX_SCHEDULER);
}

ObDDLTaskQueue::~ObDDLTaskQueue()
{
}

void ObDDLTaskQueue::destroy()
{
  int ret = OB_SUCCESS;
  ObIDDLTask *task = NULL;
  common::ObSpinLockGuard guard(lock_);
  while (OB_SUCC(ret) && task_list_.get_size() > 0) {
    if (OB_ISNULL(task = task_list_.remove_first())) {
      ret = common::OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "fail to remove first task", K(ret));
    } else {
      allocator_.free(task);
      task = NULL;
    }
  }
  task_set_.destroy();
  allocator_.reset();
  is_inited_ = false;
}

int ObDDLTaskQueue::init(const int64_t bucket_num, const int64_t total_mem_limit,
    const int64_t hold_mem_limit, const int64_t page_size)
{
  int ret = OB_SUCCESS;
  common::ObSpinLockGuard guard(lock_);
  if (bucket_num <= 0 || total_mem_limit <= 0
      || hold_mem_limit <= 0 || page_size <= 0) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(bucket_num), K(total_mem_limit),
        K(hold_mem_limit), K(page_size));
  } else if (OB_FAIL(task_set_.create(bucket_num))) {
    STORAGE_LOG(WARN, "fail to create task set", K(ret), K(bucket_num));
  } else if (OB_FAIL(allocator_.init(total_mem_limit, hold_mem_limit, page_size))) {
    STORAGE_LOG(WARN, "fail to init allocator", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObDDLTaskQueue::push_task(const ObIDDLTask &task)
{
  int ret = OB_SUCCESS;
  ObIDDLTask *task_copy = NULL;
  char *buf = NULL;
  bool task_add_to_list = false;
  common::ObSpinLockGuard guard(lock_);
  const int64_t deep_copy_size = task.get_deep_copy_size();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObBuildIndexTaskQueue has not been inited", K(ret));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(deep_copy_size)))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to allocate memory for ObBuildIndexTask", K(ret));
  } else if (OB_ISNULL(task_copy = task.deep_copy(buf, deep_copy_size))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to deep copy task", K(ret));
  } else if (!task_list_.add_last(task_copy)) {
    ret = common::OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "unexpected error, add build index task failed", K(ret));
  } else {
    int is_overwrite = 0; // do not overwrite
    task_add_to_list = true;
    if (OB_FAIL(task_set_.set_refactored(task_copy, is_overwrite))) {
      if (common::OB_HASH_EXIST == ret) {
        ret = common::OB_ENTRY_EXIST;
      } else {
        STORAGE_LOG(WARN, "fail to set task to task set", K(ret));
      }
    } else {
      STORAGE_LOG(INFO, "add task", K(*task_copy), KP(task_copy), K(common::lbt()));
    }
  }
  if (OB_FAIL(ret) && NULL != buf) {
    if (task_add_to_list) {
      int tmp_ret = OB_SUCCESS;
      if (!task_list_.remove(task_copy)) {
        tmp_ret = common::OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "fail to remove task", K(tmp_ret), K(*task_copy));
      }
    }
    allocator_.free(buf);
    buf = NULL;
    task_copy = NULL;
  }
  return ret;
}

int ObDDLTaskQueue::get_next_task(ObIDDLTask *&task)
{
  int ret = OB_SUCCESS;
  common::ObSpinLockGuard guard(lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObBuildIndexTaskQueue has not been inited", K(ret));
  } else if (0 == task_list_.get_size()) {
    ret = common::OB_EAGAIN;
  } else if (OB_ISNULL(task = task_list_.remove_first())) {
    ret = common::OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "error unexpected, task must not be NULL", K(ret));
  }
  return ret;
}

int ObDDLTaskQueue::remove_task(ObIDDLTask *task)
{
  int ret = OB_SUCCESS;
  common::ObSpinLockGuard guard(lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObBuildIndexTaskQueue has not been inited", K(ret));
  } else if (OB_ISNULL(task)) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(task));
  } else if (OB_FAIL(task_set_.erase_refactored(task))) {
    STORAGE_LOG(WARN, "fail to erase from task set", K(ret));
  } else {
    STORAGE_LOG(INFO, "succ to remove task", K(*task), KP(task));
  }
  if (NULL != task) {
    allocator_.free(task);
    task = NULL;
  }
  return ret;
}

int ObDDLTaskQueue::add_task_to_last(ObIDDLTask *task)
{
  int ret = OB_SUCCESS;
  common::ObSpinLockGuard guard(lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObBuildIndexTaskQueue has not been inited", K(ret));
  } else if (OB_ISNULL(task)) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(task));
  } else if (!task_list_.add_last(task)) {
    ret = common::OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "error unexpected, fail to move task to last", K(ret));
  }
  return ret;
}

ObDDLTaskExecutor::ObDDLTaskExecutor()
  : is_inited_(false), task_queue_(), cond_(), tg_id_(-1)
{
}

ObDDLTaskExecutor::~ObDDLTaskExecutor()
{
}

void ObDDLTaskExecutor::stop()
{
  if (tg_id_ >= 0) {
    TG_STOP(tg_id_);
  }
}

void ObDDLTaskExecutor::wait()
{
  if (tg_id_ >= 0) {
    TG_WAIT(tg_id_);
  }
}

void ObDDLTaskExecutor::destroy()
{
  if (tg_id_ >= 0) {
    TG_DESTROY(tg_id_);
    tg_id_ = -1;
  }
  task_queue_.destroy();
  is_inited_ = false;
  cond_.destroy();
}

int ObDDLTaskExecutor::init(const int64_t task_num, int tg_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObDDLTaskExecutor has already been inited", K(ret));
  } else if (OB_UNLIKELY(task_num <= 0)) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(task_num));
  } else if (OB_FAIL(task_queue_.init(task_num, TOTAL_LIMIT, HOLD_LIMIT, PAGE_SIZE))) {
    STORAGE_LOG(WARN, "fail to init task queue", K(ret));
  } else if (OB_FAIL(cond_.init(common::ObWaitEventIds::BUILD_INDEX_SCHEDULER_COND_WAIT))) {
    STORAGE_LOG(WARN, "fail to init thread cond", K(ret));
  } else {
    tg_id_ = tg_id;
    if (OB_FAIL(TG_SET_RUNNABLE_AND_START(tg_id_, *this))) {
      STORAGE_LOG(WARN, "fail to set tg id", K(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

void ObDDLTaskExecutor::run1()
{
  int ret = OB_SUCCESS;
  int64_t executed_task_count = 0;
  ObIDDLTask *task = NULL;
  ObIDDLTask *first_retry_task = NULL;
  lib::set_thread_name("DDLTaskExecutor");
  while (!has_set_stop()) {
    while (!has_set_stop() && executed_task_count < BATCH_EXECUTE_COUNT) {
      if (OB_FAIL(task_queue_.get_next_task(task))) {
        if (common::OB_EAGAIN == ret) {
          break;
        } else {
          STORAGE_LOG(WARN, "fail to get next task", K(ret));
          break;
        }
      } else if (OB_ISNULL(task)) {
        ret = OB_ERR_SYS;
        STORAGE_LOG(WARN, "error unexpected, task must not be NULL", K(ret));
      } else if (task == first_retry_task) {
        // add the task back to the queue
        if (OB_FAIL(task_queue_.add_task_to_last(task))) {
          STORAGE_LOG(ERROR, "fail to add task to last, which should not happen", K(ret), K(*task));
        }
        break;
      } else {
        task->process();
        ++executed_task_count;
        if (task->need_retry()) {
          if (OB_FAIL(task_queue_.add_task_to_last(task))) {
            STORAGE_LOG(ERROR, "fail to add task to last, which should not happen", K(ret), K(*task));
          }
          first_retry_task = task;
        } else {
          if (OB_FAIL(task_queue_.remove_task(task))) {
            STORAGE_LOG(WARN, "fail to remove task, which should not happen", K(ret), K(*task), KP(task));
          }
        }
      }
    }
    cond_.lock();
    cond_.wait(CHECK_TASK_INTERVAL);
    cond_.unlock();
    executed_task_count = 0;
    first_retry_task = NULL;
  }
}

ObDDLReplicaBuilder::ObDDLReplicaBuilder()
  : is_thread_started_(false),
    is_stopped_(false)
{

}

ObDDLReplicaBuilder::~ObDDLReplicaBuilder()
{
  destroy();
}

int ObDDLReplicaBuilder::start()
{
  int ret = OB_SUCCESS;
  if (is_thread_started_) {
    // do nothing
  } else if (OB_FAIL(TG_START(lib::TGDefIDs::DdlBuild))) {
    LOG_WARN("index build thread start failed", KR(ret));
  } else {
    is_thread_started_ = true;
  }
  if (OB_SUCC(ret)) {
    is_stopped_ = false;
  }
  return ret;
}

void ObDDLReplicaBuilder::stop()
{
  is_stopped_ = true;
}

void ObDDLReplicaBuilder::destroy()
{
  is_stopped_ = true;
  if (is_thread_started_) {
    TG_STOP(lib::TGDefIDs::DdlBuild);
    TG_WAIT(lib::TGDefIDs::DdlBuild);
    TG_DESTROY(lib::TGDefIDs::DdlBuild);
    is_thread_started_ = false;
  }
}

int ObDDLReplicaBuilder::push_task(ObAsyncTask &task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_thread_started_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("ddl builder thread not started", K(ret), K(is_thread_started_));
  } else if (is_stopped_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("ddl builder has stopped", K(ret), K(is_stopped_));
  } else if (OB_FAIL(TG_PUSH_TASK(lib::TGDefIDs::DdlBuild, task))) {
    LOG_WARN("add task to queue failed", K(ret));
  }
  return ret;
}

}  // end namespace share
}  // end namespace oceanbase
