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

#ifndef OCEABASE_SERVER_OB_SAFE_DESTROY_THREAD_
#define OCEABASE_SERVER_OB_SAFE_DESTROY_THREAD_

#include "lib/lock/ob_tc_rwlock.h"
#include "lib/ob_define.h"
#include "lib/queue/ob_lighty_queue.h"
#include "lib/task/ob_timer.h"
#include "lib/thread/thread_mgr.h"
#include "lib/thread/thread_mgr_interface.h"

namespace oceanbase
{
namespace storage
{
class ObSafeDestroyTask
{
public:
  enum class ObSafeDestroyTaskType
  {
    INVALID,
    LS,
  };
  const static int64_t PRINT_INTERVAL_TIME = 10 * 60 * 1000 * 1000; // a task only print if it
                                                                    // is not complete after 10 min
  const static int64_t PRINT_TIMES_INTERVAL = 10; // every 2 second loop once.
public:
  ObSafeDestroyTask()
    : type_(ObSafeDestroyTaskType::INVALID),
      recv_timestamp_(0),
      last_execute_timestamp_(0),
      retry_times_(0)
  {}
  virtual ~ObSafeDestroyTask() {}
  virtual bool safe_to_destroy() = 0;
  virtual void destroy() = 0;
  ObSafeDestroyTaskType get_type() const { return type_; }
  void set_receive_timestamp(const int64_t receive_timestamp);
  void update_retry_info();
  void update_execute_timestamp();
  int64_t get_last_execute_timestamp() const
  { return last_execute_timestamp_; }
  VIRTUAL_TO_STRING_KV(K_(recv_timestamp), K_(retry_times),
                       K_(last_execute_timestamp));
protected:
  ObSafeDestroyTaskType type_;
  // the time the task is received
  int64_t recv_timestamp_;
  // the last execute timestamp
  int64_t last_execute_timestamp_;
  // how many times the task has been processed.
  int64_t retry_times_;
}; // end of class ObSafeDestroyTask

class ObSafeDestroyProcessFunctor
{
public:
  bool operator()(ObSafeDestroyTask *task, bool &need_requeue) const;
};

class ObSafeDestroyTaskQueue
{
  using RWLock = common::TCRWLock;
  using RDLockGuard = common::TCRLockGuard;
  using WRLockGuard = common::TCWLockGuard;

public:
  static const int LIGHTY_QUEUE_SIZE = (1 << 10);
  ObSafeDestroyTaskQueue();
  virtual ~ObSafeDestroyTaskQueue();

  int init(int queue_capacity = LIGHTY_QUEUE_SIZE);
  void stop() { stop_ = true; }
  bool stop_finished();
  int push(ObSafeDestroyTask *task);
  void loop();
  void destroy();
  // For each.
  // Call fn on every element of this task queue.
  // fn: bool operator()(const Task &task);
  // If operator() returns false, for_each() would stop immediately.
  template <typename Function>
  int for_each(Function &fn);
private:
  void push_into_queue_(void *task);
  DISALLOW_COPY_AND_ASSIGN(ObSafeDestroyTaskQueue);
protected:
  bool looping_;
  // stop push into new task but not stop process exist task.
  bool stop_;

  // protect queue
  RWLock rwlock_;
  // the queue of task
  common::ObLightyQueue queue_;
};

template<typename Function>
int ObSafeDestroyTaskQueue::for_each(Function &fn)
{
  int ret = OB_SUCCESS;
  int64_t timeout = 0; // no wait
  void *task = nullptr;
  ObSafeDestroyTask *safe_destroy_task = nullptr;
  bool loop_finish = false;
  // make sure no one can push into the queue
  WRLockGuard guard(rwlock_);
  int64_t begin_loop_timestamp = ObTimeUtility::current_time();
  looping_ = true;
  while (!loop_finish) {
    // push a task to break if we loop all the task one time.
    if (OB_FAIL(queue_.pop(task, timeout))) {
      // there is no task now. just stop.
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        loop_finish = true;
      } else {
        SERVER_LOG(WARN, "queue pop task fail", K(ret), K(&queue_));
      }
    } else if (OB_ISNULL(task)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(ERROR, "queue pop successfully but task is NULL", K(ret), K(task));
    } else {
      bool need_requeue = true;
      safe_destroy_task = reinterpret_cast<ObSafeDestroyTask *>(task);
      if (safe_destroy_task->get_last_execute_timestamp() >= begin_loop_timestamp) {
        // loop all the task in the queue one time, break.
        loop_finish = true;
      } else if (!fn(safe_destroy_task, need_requeue)) {
        // no need process again.
        loop_finish = true;
      } else {
        // do nothing
      }
      // push the task into the queue again.
      if (need_requeue) {
        push_into_queue_(task);
        safe_destroy_task->update_execute_timestamp();
      }
    }
  }  // loop one time
  looping_ = false;
  return ret;
}

class ObSafeDestroyHandler
{
  const static int64_t TASK_SCHEDULER_INTERVAL = 2 * 1000 * 1000; // 2_s
public:
  ObSafeDestroyHandler();
  ~ObSafeDestroyHandler();
  int init();
  int start();
  int stop();
  void wait();
  void destroy();
  int handle();
  // add a task into the thread
  // task will be execute every TASK_SCHEDULER_INTERVAL
  int push(ObSafeDestroyTask &task);
  static OB_INLINE ObSafeDestroyHandler &get_instance();
  // For each.
  // Call fn on every element of this task queue.
  // fn: bool operator()(const Task &task);
  // If operator() returns false, for_each() would stop immediately.
  template <typename Function>
  int for_each(Function &fn);
private:
  bool is_inited_;
  // push the safe destroy task into the queue.
  ObSafeDestroyTaskQueue queue_;
  int64_t last_process_timestamp_;
};

template <typename Function>
int ObSafeDestroyHandler::for_each(Function &fn)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "safe destroy thread not inited", K(ret));
  } else if (OB_FAIL(queue_.for_each(fn))) {
    SERVER_LOG(WARN, "push safe destroy task failed", K(ret));
  } else {
    // do nothing
  }
  return ret;
}


} // observer
} // oceanbase
#endif
