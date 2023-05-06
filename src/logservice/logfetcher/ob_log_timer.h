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
 * A simple timer based on HashMap with fixed sleep time
 */

#ifndef OCEANBASE_LOG_FETCHER_TIMER_H__
#define OCEANBASE_LOG_FETCHER_TIMER_H__

#include "lib/queue/ob_fixed_queue.h"           // ObFixedQueue
#include "common/ob_queue_thread.h"             // ObCond
#include "lib/allocator/ob_small_allocator.h"   // ObSmallAllocator
#include "ob_log_utils.h"                       // _SEC_

namespace oceanbase
{
namespace logfetcher
{
class ObLogTimerTask
{
public:
  virtual ~ObLogTimerTask() {}

public:
  virtual void process_timer_task() = 0;
};

class IObLogErrHandler;
class ObLogFetcherConfig;

// Timer class
class ObLogFixedTimer
{
  static const int64_t STAT_INTERVAL = 30 * _SEC_;
  static const int64_t COND_WAIT_TIME = 1 * _SEC_;

public:
  // Timer task waiting time
  static int64_t g_wait_time;

public:
  ObLogFixedTimer();
  virtual ~ObLogFixedTimer();

public:
  int init(IObLogErrHandler &err_handler, const int64_t max_task_count);
  void destroy();

  int start();
  void stop();
  void mark_stop_flag();

public:
  int schedule(ObLogTimerTask *task);

public:
  static void configure(const ObLogFetcherConfig &config);

public:
  void run();

private:
  static void *thread_func_(void *args);
  struct QTask
  {
    int64_t         out_timestamp_;   // timestamp of out
    ObLogTimerTask  &task_;           // Actual timer tasks

    explicit QTask(ObLogTimerTask &task);
  };

  typedef common::ObFixedQueue<QTask> TaskQueue;

private:
  void destroy_all_tasks_();
  QTask *alloc_queue_task_(ObLogTimerTask &timer_task);
  int push_queue_task_(QTask &task);
  void free_queue_task_(QTask *task);
  int next_queue_task_(QTask *&task);

private:
  bool                      inited_;
  pthread_t                 tid_;           // Timer thread ID
  IObLogErrHandler          *err_handler_;  // err handler
  TaskQueue                 task_queue_;    // task queue
  common::ObCond            task_cond_;
  common::ObSmallAllocator  allocator_;

  volatile bool stop_flag_ CACHE_ALIGNED;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogFixedTimer);
};

}
}

#endif
