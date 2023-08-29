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

#ifndef OCEANBASE_SHARE_OB_ASYNC_TASK_QUEUE_H_
#define OCEANBASE_SHARE_OB_ASYNC_TASK_QUEUE_H_

#include "lib/queue/ob_lighty_queue.h"
#include "lib/allocator/ob_concurrent_fifo_allocator.h"
#include "lib/thread/ob_reentrant_thread.h"

namespace oceanbase
{
namespace share
{
class ObAsyncTask
{
public:
  ObAsyncTask() : retry_interval_(RETRY_INTERVAL), retry_times_(INFINITE_RETRY_TIMES),
      last_execute_time_(0) { }

  virtual ~ObAsyncTask() { }
  // if process fail, will push back to the queue and retry %retry_times_ times.
  virtual int process() = 0;
  virtual int64_t get_deep_copy_size() const = 0;
  virtual ObAsyncTask *deep_copy(char *buf, const int64_t buf_size) const = 0;
  inline int64_t get_retry_interval() const;
  virtual bool need_process(const int64_t switch_epoch) const { UNUSED(switch_epoch); return true; }
  virtual void set_is_retry(const bool is_retry) { UNUSED(is_retry); }
  inline int64_t get_retry_times() const;
  inline void set_retry_interval(const int64_t retry_interval);
  inline void set_retry_times(const int64_t retry_times);
  inline int64_t get_last_execute_time() const;
  inline void set_last_execute_time(const int64_t execute_time);
private:
  static const int64_t RETRY_INTERVAL = 1000 * 1000L;    // 1s
  static const int64_t INFINITE_RETRY_TIMES = INT64_MAX;
  int64_t retry_interval_;                               // us
  int64_t retry_times_;
  int64_t last_execute_time_;

  DISALLOW_COPY_AND_ASSIGN(ObAsyncTask);
};

inline int64_t ObAsyncTask::get_retry_interval() const
{
  return retry_interval_;
}

inline int64_t ObAsyncTask::get_retry_times() const
{
  return retry_times_;
}

inline void ObAsyncTask::set_retry_interval(const int64_t retry_interval)
{
  if (retry_interval < 0) {
    retry_interval_ = RETRY_INTERVAL;
  } else {
    retry_interval_ = retry_interval;
  }
}

inline void ObAsyncTask::set_retry_times(const int64_t retry_times)
{
  if (retry_times < 0) {
    retry_times_ = INFINITE_RETRY_TIMES;
  } else {
    retry_times_ = retry_times;
  }
}

inline int64_t ObAsyncTask::get_last_execute_time() const
{
  return last_execute_time_;
}

inline void ObAsyncTask::set_last_execute_time(const int64_t execute_time)
{
  last_execute_time_ = execute_time;
}

class ObAsyncTaskQueue : public ObReentrantThread
{
public:
  // if thread_cnt > 1, be sure the task can be processed in different order
  // with push order
  ObAsyncTaskQueue();
  virtual ~ObAsyncTaskQueue();
  //attention queue_size should be 2^n
  int init(const int64_t thread_cnt, const int64_t queue_size,
           const char *thread_name = nullptr, const int64_t page_size = PAGE_SIZE);
  int start();
  void stop();
  void wait();
  int destroy();

  int push(const ObAsyncTask &task);
protected:
  static const int64_t TOTAL_LIMIT = 1024L * 1024L * 1024L;
  static const int64_t HOLD_LIMIT = 512L * 1024L * 1024L;
  static const int64_t PAGE_SIZE = common::OB_MALLOC_MIDDLE_BLOCK_SIZE;
  static const int64_t SLEEP_INTERVAL = 10000; //10ms
  virtual void run2();
  virtual int blocking_run() { BLOCKING_RUN_IMPLEMENT(); }
  int pop(ObAsyncTask *&task);
protected:
  bool is_inited_;
  common::ObLightyQueue queue_;
  common::ObConcurrentFIFOAllocator allocator_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObAsyncTaskQueue);
};
}//end namespace share
}//end namespace oceanbase
#endif
