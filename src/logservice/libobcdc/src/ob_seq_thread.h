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
 * Thread class with ObConcurrentSeqQueue, support for concurrent push for task specified seq,
 * support for concurrent sequential consumption of tasks
 *
 * Assume two tasks, task numbers: M and N, 0 <= M < N.
 * If the Nth task has been assigned a thread to process, ensure that the Mth task must have
 * been assigned a thread to ensure no starvation
 */

#ifndef OCEANBASE_OB_SEQ_QUEUE_THREAD_H__
#define OCEANBASE_OB_SEQ_QUEUE_THREAD_H__

#include "lib/ob_define.h"              // RETRY_FUNC
#include "lib/allocator/ob_malloc.h"    // ObMemAttr
#include "share/ob_errno.h"             // KR

#include "ob_concurrent_seq_queue.h"    // ObConcurrentSeqQueue

namespace oceanbase
{
namespace common
{

// MAX_THREAD_NUM:    Specifies the maximum number of threads supported
// ModuleClass:       The module type of the thread class used, to distinguish between different modules for easy debugging
template <int MAX_THREAD_NUM = 256, typename ModuleClass = void>
class ObSeqThread
{
  enum { DATA_OP_TIMEOUT = 1 * 1000 * 1000 };
  typedef ObConcurrentSeqQueue  QueueType;

public:
  ObSeqThread();
  virtual ~ObSeqThread();

public:
  virtual int handle(void *task, const int64_t task_seq, const int64_t thread_index, volatile bool &stop_flag) = 0;

public:
  int push(void *task, const int64_t task_seq, const int64_t timeout);
  int start();
  void stop();
  void mark_stop_flag() { stop_flag_ = true; }
  bool is_stoped() const { return ATOMIC_LOAD(&stop_flag_); }
  int64_t get_thread_num() const { return thread_num_; }
  int64_t get_task_num() const { return queue_.size(); }

public:
  int init(const int64_t thread_num,
      const int64_t queue_size,
      const ObMemAttr &memattr = default_memattr);
  void destroy();

public:
  void run();

private:
  static void *thread_func_(void *arg);
  int next_task_(const int64_t task_seq, void *&task);
  int64_t next_seq_();

private:
  bool          inited_;
  int64_t       thread_num_;
  int64_t       thread_counter_;

  volatile bool stop_flag_ CACHE_ALIGNED;
  int64_t       task_seq_ CACHE_ALIGNED;

  QueueType     queue_;

  pthread_t     tids_[MAX_THREAD_NUM];

private:
  DISALLOW_COPY_AND_ASSIGN(ObSeqThread);
};

template <int MAX_THREAD_NUM, typename ModuleClass>
ObSeqThread<MAX_THREAD_NUM, ModuleClass>::ObSeqThread() :
    inited_(false),
    thread_num_(0),
    thread_counter_(0),
    stop_flag_(true),
    task_seq_(0),
    queue_()
{
  (void)memset(tids_, 0, sizeof(tids_));
}

template <int MAX_THREAD_NUM, typename ModuleClass>
ObSeqThread<MAX_THREAD_NUM, ModuleClass>::~ObSeqThread()
{
  destroy();
}

template <int MAX_THREAD_NUM, typename ModuleClass>
int ObSeqThread<MAX_THREAD_NUM, ModuleClass>::init(const int64_t thread_num,
    const int64_t queue_size,
    const ObMemAttr &memattr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    LIB_LOG(ERROR, "ObSeqThread has been initialized", K(inited_));
    ret = OB_INIT_TWICE;
  } else if (OB_UNLIKELY(thread_num <= 0)
      || OB_UNLIKELY(thread_num > MAX_THREAD_NUM)
      || OB_UNLIKELY(queue_size <= 0)) {
    LIB_LOG(ERROR, "invalid arguments", K(thread_num), K(MAX_THREAD_NUM), K(queue_size));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(queue_.init(queue_size, memattr))) {
    LIB_LOG(ERROR, "initialize queue fail", KR(ret), K(queue_size));
  } else {
    thread_num_ = thread_num;
    thread_counter_ = 0;
    stop_flag_ = true;
    task_seq_ = 0;
    (void)memset(tids_, 0, sizeof(tids_));

    inited_ = true;
  }

  return ret;
}

template <int MAX_THREAD_NUM, typename ModuleClass>
void ObSeqThread<MAX_THREAD_NUM, ModuleClass>::destroy()
{
  stop();

  inited_ = false;
  thread_num_ = 0;
  thread_counter_ = 0;
  stop_flag_ = true;
  task_seq_ = 0;
  queue_.destroy();

  (void)memset(tids_, 0, sizeof(tids_));
}

template <int MAX_THREAD_NUM, typename ModuleClass>
int ObSeqThread<MAX_THREAD_NUM, ModuleClass>::start()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LIB_LOG(ERROR, "ObSeqThread has not been initialized");
    ret = OB_NOT_INIT;
  } else if (stop_flag_) {
    stop_flag_ = false;

    for (int64_t index = 0; OB_SUCC(ret) && index < thread_num_; index++) {
      int pthread_ret = 0;

      if (0 != (pthread_ret = pthread_create(tids_ + index, NULL, thread_func_, this))) {
        ret = OB_ERR_UNEXPECTED;
        LIB_LOG(ERROR, "pthread_create fail", K(pthread_ret), KERRNOMSG(pthread_ret), K(index));
      }
    }
  }

  return ret;
}

template <int MAX_THREAD_NUM, typename ModuleClass>
void ObSeqThread<MAX_THREAD_NUM, ModuleClass>::stop()
{
  if (inited_) {
    stop_flag_ = true;

    for (int64_t index = 0; index < thread_num_; index++) {
      if (0 != tids_[index]) {
        int pthread_ret = pthread_join(tids_[index], NULL);

        if (0 != pthread_ret) {
          LIB_LOG_RET(ERROR, common::OB_ERR_SYS, "pthread_join fail", "thread_id", tids_[index], K(pthread_ret));
        } else {
          // do nothing
        }
      }
    }

    (void)memset(tids_, 0, sizeof(tids_));
  }
}

template <int MAX_THREAD_NUM, typename ModuleClass>
void *ObSeqThread<MAX_THREAD_NUM, ModuleClass>::thread_func_(void *arg)
{
  if (NULL != arg) {
    ObSeqThread<MAX_THREAD_NUM, ModuleClass> *td = static_cast<ObSeqThread<MAX_THREAD_NUM, ModuleClass> *>(arg);
    td->run();
  }

  return NULL;
}

template <int MAX_THREAD_NUM, typename ModuleClass>
void ObSeqThread<MAX_THREAD_NUM, ModuleClass>::run()
{
  int ret = OB_SUCCESS;
  int64_t thread_index = ATOMIC_FAA(&thread_counter_, 1);

  if (OB_UNLIKELY(! inited_)) {
    LIB_LOG(ERROR, "ObSeqThread not initialized", K(thread_index));
    ret = OB_NOT_INIT;
  } else {
    while (! stop_flag_ && OB_SUCCESS == ret) {
      void *task = NULL;
      // Get the next sequence number to be consumed
      int64_t task_seq = next_seq_();
      if (OB_FAIL(next_task_(task_seq, task))) {
        if (OB_IN_STOP_STATE != ret) {
          LIB_LOG(ERROR, "next_task_ fail", KR(ret), K(task_seq));
        }
      } else if (OB_FAIL(handle(task, task_seq, thread_index, stop_flag_))) {
        if (OB_IN_STOP_STATE != ret) {
          LIB_LOG(ERROR, "handle task fail", KR(ret), "task", (int64_t)task, K(task_seq),
              K(thread_index));
        }
      } else {
        // do nothing
      }
    }
  }

  // NOTE: One thread exits, others exit at the same time
  stop_flag_ = true;
}

template <int MAX_THREAD_NUM, typename ModuleClass>
int64_t ObSeqThread<MAX_THREAD_NUM, ModuleClass>::next_seq_()
{
  return ATOMIC_FAA(&task_seq_, 1);
}

template <int MAX_THREAD_NUM, typename ModuleClass>
int ObSeqThread<MAX_THREAD_NUM, ModuleClass>::next_task_(const int64_t task_seq, void *&task)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LIB_LOG(ERROR, "ObSeqThread not initialized");
    ret = OB_NOT_INIT;
  } else {
    RETRY_FUNC(stop_flag_, queue_, pop, task, task_seq, DATA_OP_TIMEOUT);
  }

  return ret;
}

template <int MAX_THREAD_NUM, typename ModuleClass>
int ObSeqThread<MAX_THREAD_NUM, ModuleClass>::push(void *task, const int64_t task_seq, const int64_t timeout)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LIB_LOG(ERROR, "ObSeqThread not initialized");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(task) || OB_UNLIKELY(task_seq < 0)) {
    LIB_LOG(ERROR, "invalid argument", K(task), K(task_seq));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(stop_flag_)) {
    ret = OB_IN_STOP_STATE;
  } else {
    ret = queue_.push(task, task_seq, timeout);
  }

  return ret;
}

} // namespace common
} // namespace oceanbase
#endif /* OCEANBASE_MULTI_FIXED_QUEUE_THREAD_H__ */
