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
 *
 * PartTransTask Queue
 */

#ifndef OCEANBASE_LIBOBCDC_OB_LOG_PART_TRANS_TASK_QUEUE_H__
#define OCEANBASE_LIBOBCDC_OB_LOG_PART_TRANS_TASK_QUEUE_H__

#include "lib/lock/ob_small_spin_lock.h"    // ObByteLock
#include "logservice/logfetcher/ob_log_fetcher_ls_ctx_additional_info.h" // PartTransDispatchInfo
#include "ob_log_part_trans_task.h"         // PartTransTask

namespace oceanbase
{
namespace libobcdc
{

// task queue
// not thread-safe
struct PartTransTaskQueue
{
public:
  PartTransTaskQueue() : size_(0), head_(NULL), tail_(NULL) {}
  ~PartTransTaskQueue()
  {
    reset();
  }

  void reset()
  {
    size_ = 0;
    head_ = NULL;
    tail_ = NULL;
  }

  void push(PartTransTask *task)
  {
    if (NULL != task) {
      if (NULL == head_) {
        head_ = task;
        tail_ = head_;
        task->set_next_task(NULL);
      } else {
        tail_->set_next_task(task);
        task->set_next_task(NULL);
        tail_ = task;
      }

      size_++;
    }
  }

  PartTransTask* pop()
  {
    PartTransTask *task = head_;

    // Unconditional output
    if (NULL != task) {
      // point to next task
      head_ = head_->next_task();

      if (NULL == head_) {
        tail_ = NULL;
      }

      size_--;

      // clear the pointer to next task
      task->set_next_task(NULL);
    }

    return task;
  }

  PartTransTask* top()
  {
    return head_;
  }

  void remove(PartTransTask *task, bool &exist)
  {
    exist = false;
    if (NULL != task && NULL != head_) {
      PartTransTask *iter = head_;
      PartTransTask *prev = NULL;

      while (NULL != iter) {
        // Find the corresponding task
        if (task == iter) {
          exist = true;

          // If the previous one is empty, then it is the header node, modify the header node
          if (NULL == prev) {
            head_ = task->next_task();
          } else {
            // If the previous one is not empty, modify the next pointer of the previous node
            prev->set_next_task(task->next_task());
          }

          if (NULL == task->next_task()) {
            // If the target node is the last node, modify the tail pointer to point to the previous node
            tail_ = prev;
          }

          // Clear the pointer to next node
          task->set_next_task(NULL);
          ATOMIC_DEC(&size_);
          break;
        } else {
          prev = iter;
          iter = iter->next_task();
        }
      }
    }
  }

  int64_t size() const
  {
    return size_;
  }

private:
  int64_t             size_;
  PartTransTask       *head_;
  PartTransTask       *tail_;

private:
  DISALLOW_COPY_AND_ASSIGN(PartTransTaskQueue);
};

////////////////////////////////////////////////////////
//
// Locking for operation mutual exclusion
class SafePartTransTaskQueue
{
public:
  SafePartTransTaskQueue() : queue_(), lock_() {}
  virtual ~SafePartTransTaskQueue() { reset(); }

  void reset()
  {
    queue_.reset();
  }

  // Requires sequential push tasks
  void push(PartTransTask *task)
  {
    common::ObByteLockGuard guard(lock_);
    queue_.push(task);
  }

  PartTransTask *pop()
  {
    common::ObByteLockGuard guard(lock_);
    return queue_.pop();
  }

  PartTransTask *top()
  {
    common::ObByteLockGuard guard(lock_);
    return queue_.top();
  }

  // Lock-protected operation of TOP elements
  //
  // Func supports member functions:
  // void operate() (const PartTransTask *top_task);
  // - if top_task is empty, means the head element does not exist
  template <class Func>
  void top_operate(Func &func)
  {
    common::ObByteLockGuard guard(lock_);
    func(queue_.top());
  }

  int64_t size() const
  {
    common::ObByteLockGuard guard(lock_);
    return queue_.size();
  }

  void remove(PartTransTask *task, bool &exist)
  {
    common::ObByteLockGuard guard(lock_);
    queue_.remove(task, exist);
  }

  // used for debug
  // print all tasks in the queue with lock
  void print_task_queue();
protected:
  PartTransTaskQueue          queue_;
  mutable common::ObByteLock  lock_;
};

class SafeTaskWithRecordQueue : public SafePartTransTaskQueue
{
private:
  struct DispatchingTaskBasicInfo
  {
    bool is_dispatching_;
    bool is_trans_committed_;
    PartTransTask::TaskType task_type_;
    palf::LSN prepare_log_lsn_;
    int64_t task_timestamp_;
    int64_t trans_commit_version_;

    // clear record info
    void reset();

    // set record info
    void reset(bool is_trans_committed,
        PartTransTask::TaskType task_type,
        const palf::LSN &prepare_log_lsn,
        int64_t task_timestamp,
        int64_t trans_commit_version);

      TO_STRING_KV(K(is_dispatching_), K(is_trans_committed_),
          K(task_type_), K_(prepare_log_lsn), K(task_timestamp_), K(trans_commit_version_));
  };

public:
  SafeTaskWithRecordQueue() : dispatching_task_info_() {}
  ~SafeTaskWithRecordQueue() { dispatching_task_info_.reset(); }
public:
  PartTransTask *pop();

  void reset_dispatched_task_info();

  void update_dispatch_progress_by_task_queue(int64_t &dispatch_progress,
      logfetcher::PartTransDispatchInfo &dispatch_info);
private:
  void record_dispatching_task_info_(PartTransTask *task);
private:
  DispatchingTaskBasicInfo    dispatching_task_info_;    // The task being dispatched, the task dispatch is complete and needs to be reset
};

}
}

#endif
