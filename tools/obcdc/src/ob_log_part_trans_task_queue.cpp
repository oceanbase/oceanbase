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

#define USING_LOG_PREFIX OBLOG

#include "ob_log_part_trans_task_queue.h"
#include "ob_log_part_trans_dispatcher.h"      // PartTransDispatchInfo

namespace oceanbase
{
namespace liboblog
{
// The task may be destroyed by other module threads during the dispatch process, so it needs to be popped out before it can be consumed
// The basic information of the task being dispatched is still recorded in the queue to satisfy the query of the dispatch progress
//
// pop task needs to record basic information about the task
PartTransTask* SafeTaskWithRecordQueue::pop()
{
  PartTransTask *ret_task = NULL;
  common::ObByteLockGuard guard(lock_);
  ret_task = queue_.pop();
  record_dispatching_task_info_(ret_task);
  return ret_task;
}

void SafeTaskWithRecordQueue::record_dispatching_task_info_(PartTransTask *task)
{
  if (OB_ISNULL(task)) {
    // nothing
  } else {
    dispatching_task_info_.reset(task->is_trans_committed(),
        task->is_trans_ready_to_commit(),
        task->get_type(),
        task->get_prepare_log_id(),
        task->get_timestamp(),
        task->get_global_trans_version());
  }
}

void SafeTaskWithRecordQueue::reset_dispatched_task_info()
{
  common::ObByteLockGuard guard(lock_);
  dispatching_task_info_.reset();
}

void SafePartTransTaskQueue::print_task_queue()
{
  common::ObByteLockGuard guard(lock_);
  LOG_INFO("task queue info", "ddl_size", queue_.size());

  PartTransTask *task = queue_.top();
  int64_t idx = 1;

  while (NULL != task) {
    LOG_INFO("task queue", K(idx), "ddl_size", queue_.size(),
        K(task), KPC(task));

    task = task->next_task();
    ++idx;
  }
}

// 1. have pop out the task that is being dispatched, dispatch progress takes dispatching_task_info_
//
// 2. dispatching_task_info_ is marked false, take top element task
//
void SafeTaskWithRecordQueue::update_dispatch_progress_by_task_queue(
    int64_t &dispatch_progress,
    PartTransDispatchInfo &dispatch_info)
{
  common::ObByteLockGuard guard(lock_);
  if (dispatching_task_info_.is_dispatching_) {
    if (OB_INVALID_TIMESTAMP != dispatching_task_info_.task_timestamp_) {
      dispatch_progress = dispatching_task_info_.task_timestamp_ - 1;

      dispatch_info.next_task_type_ = PartTransTask::print_task_type(dispatching_task_info_.task_type_);
      dispatch_info.next_trans_log_id_ = dispatching_task_info_.prepare_log_id_;
      dispatch_info.next_trans_committed_ = dispatching_task_info_.is_trans_committed_;
      dispatch_info.next_trans_ready_to_commit_ = dispatching_task_info_.is_trans_ready_to_commit_;
      dispatch_info.next_trans_global_version_ = dispatching_task_info_.global_trans_version_;
    }
  } else {
    PartTransTask *task = queue_.top();
    if (NULL == task) {
      // Queue is empty and not processed
    } else if (OB_INVALID_TIMESTAMP != task->get_timestamp()) {
      // If there is a task to be output, take the "task to be output timestamp - 1" as the output progress
      dispatch_progress = task->get_timestamp() - 1;

      // Update information for the next transaction
      // Note: Only DML and DDL are valid
      dispatch_info.next_task_type_ = PartTransTask::print_task_type(task->get_type());
      dispatch_info.next_trans_log_id_ = task->get_prepare_log_id();
      dispatch_info.next_trans_committed_ = task->is_trans_committed();
      dispatch_info.next_trans_ready_to_commit_ = task->is_trans_ready_to_commit();
      dispatch_info.next_trans_global_version_ = task->get_global_trans_version();
    }
  }
}

void SafeTaskWithRecordQueue::DispatchingTaskBasicInfo::reset()
{
  is_dispatching_ = false;
  is_trans_committed_ = false;
  is_trans_ready_to_commit_ = false;
  task_type_ = PartTransTask::TaskType::TASK_TYPE_UNKNOWN;
  prepare_log_id_ = OB_INVALID_ID;
  task_timestamp_ = OB_INVALID_TIMESTAMP;
  global_trans_version_ = OB_INVALID_VERSION;
}

void SafeTaskWithRecordQueue::DispatchingTaskBasicInfo::reset(bool is_trans_committed,
    bool is_trans_ready_to_commit,
    PartTransTask::TaskType task_type,
    uint64_t prepare_log_id,
    int64_t task_timestamp,
    int64_t global_trans_version)
{
  is_dispatching_ = true;
  is_trans_committed_ = is_trans_committed;
  is_trans_ready_to_commit_ = is_trans_ready_to_commit;
  task_type_ = task_type;
  prepare_log_id_ = prepare_log_id;
  task_timestamp_ = task_timestamp;
  global_trans_version_ = global_trans_version;
}

} /* liboblog */
} /* oceanbase */
