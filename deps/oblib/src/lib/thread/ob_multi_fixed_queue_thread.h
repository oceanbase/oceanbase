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

#ifndef OCEANBASE_MULTI_FIXED_QUEUE_THREAD_H__
#define OCEANBASE_MULTI_FIXED_QUEUE_THREAD_H__

#include "lib/ob_define.h"                     // RETRY_FUNC
#include "lib/queue/ob_multi_fixed_queue.h"   // ObMultiFixedQueue

extern "C" {
int ob_pthread_create(void **ptr, void *(*start_routine) (void *), void *arg);
void ob_pthread_join(void *ptr);
}

namespace oceanbase
{
namespace common
{

// ModuleClass: 标识使用该线程池的目标模块
template <int MAX_THREAD_NUM = 32, typename ModuleClass = void>
class ObMQThread
{
  enum { DATA_OP_TIMEOUT = 1 * 1000 * 1000 };

  typedef ObMultiFixedQueue<MAX_THREAD_NUM> MQueue;

public:
  ObMQThread();
  virtual ~ObMQThread();

public:
  virtual int handle(void *data, const int64_t thread_index, volatile bool &stop_flag) = 0;

public:
  virtual int thread_begin();
  virtual void thread_end();

public:
  int init(const int64_t thread_num, const int64_t queue_size);
  void destroy();
  int start();
  void stop();
  void run();
  void mark_stop_flag() { stop_flag_ = true; }

  bool is_stoped() const { return ATOMIC_LOAD(&stop_flag_); }

  int push(void *data, const uint64_t hash_value, const int64_t timeout);

  int64_t get_thread_num() const { return thread_num_; }

  // 获取所有队列总任务个数
  int get_total_task_num(int64_t &task_count);

  // 获取第thread_idx个线程对应queue待处理任务个数
  int get_task_num(const int64_t thread_idx, int64_t &task_count);

private:
  static void *thread_func_(void *arg);
  int next_task_(int64_t queue_index, void *&task);

private:
  bool          inited_;
  int64_t       thread_num_;
  int64_t       thread_counter_;

  volatile bool stop_flag_ CACHE_ALIGNED;

  void*     tids_[MAX_THREAD_NUM];
  MQueue        queue_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObMQThread);
};

template <int MAX_THREAD_NUM, typename ModuleClass>
ObMQThread<MAX_THREAD_NUM, ModuleClass>::ObMQThread() :
    inited_(false),
    thread_num_(0),
    thread_counter_(0),
    stop_flag_(true),
    queue_()
{
  (void)memset(tids_, 0, sizeof(tids_));
}

template <int MAX_THREAD_NUM, typename ModuleClass>
ObMQThread<MAX_THREAD_NUM, ModuleClass>::~ObMQThread()
{
  destroy();
}

template <int MAX_THREAD_NUM, typename ModuleClass>
int ObMQThread<MAX_THREAD_NUM, ModuleClass>::init(const int64_t thread_num, const int64_t queue_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    LIB_LOG(ERROR, "MQThread has been initialized");
    ret = OB_INIT_TWICE;
  } else if (OB_UNLIKELY(thread_num <= 0)
      || OB_UNLIKELY(thread_num > MAX_THREAD_NUM)
      || OB_UNLIKELY(queue_size <= 0)) {
    LIB_LOG(ERROR, "invalid arguments", K(thread_num), K(MAX_THREAD_NUM), K(queue_size));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(queue_.init(queue_size, thread_num))) {
    LIB_LOG(ERROR, "initialize queue fail", K(ret), K(thread_num), K(queue_size));
  } else {
    thread_num_ = thread_num;
    thread_counter_ = 0;
    stop_flag_ = true;
    (void)memset(tids_, 0, sizeof(tids_));

    inited_ = true;
  }

  return ret;
}

template <int MAX_THREAD_NUM, typename ModuleClass>
void ObMQThread<MAX_THREAD_NUM, ModuleClass>::destroy()
{
  stop();

  inited_ = false;
  thread_num_ = 0;
  thread_counter_ = 0;
  stop_flag_ = true;

  (void)memset(tids_, 0, sizeof(tids_));

  (void)queue_.destroy();
}

template <int MAX_THREAD_NUM, typename ModuleClass>
int ObMQThread<MAX_THREAD_NUM, ModuleClass>::start()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LIB_LOG(ERROR, "MQThread has not been initialized");
    ret = OB_NOT_INIT;
  } else if (stop_flag_) {
    stop_flag_ = false;

    for (int64_t index = 0; OB_SUCC(ret) && index < thread_num_; index++) {

      if (OB_FAIL(ob_pthread_create(tids_ + index, thread_func_, this))) {
        LIB_LOG(ERROR, "pthread_create fail", K(ret), K(index));
      }
    }
  }

  return ret;
}

template <int MAX_THREAD_NUM, typename ModuleClass>
void ObMQThread<MAX_THREAD_NUM, ModuleClass>::stop()
{
  if (inited_) {
    stop_flag_ = true;

    for (int64_t index = 0; index < thread_num_; index++) {
      if (nullptr != tids_[index]) {
        ob_pthread_join(tids_[index]);
      }
    }

    (void)memset(tids_, 0, sizeof(tids_));
  }
}

template <int MAX_THREAD_NUM, typename ModuleClass>
void *ObMQThread<MAX_THREAD_NUM, ModuleClass>::thread_func_(void *arg)
{
  if (NULL != arg) {
    ObMQThread<MAX_THREAD_NUM, ModuleClass> *td = static_cast<ObMQThread<MAX_THREAD_NUM, ModuleClass> *>(arg);
    td->run();
  }

  return NULL;
}

template <int MAX_THREAD_NUM, typename ModuleClass>
int ObMQThread<MAX_THREAD_NUM, ModuleClass>::thread_begin()
{
  return OB_SUCCESS;
}

template <int MAX_THREAD_NUM, typename ModuleClass>
void ObMQThread<MAX_THREAD_NUM, ModuleClass>::thread_end()
{
}

template <int MAX_THREAD_NUM, typename ModuleClass>
void ObMQThread<MAX_THREAD_NUM, ModuleClass>::run()
{
  int ret = OB_SUCCESS;
  int64_t thread_index = ATOMIC_FAA(&thread_counter_, 1);

  if (OB_UNLIKELY(! inited_)) {
    LIB_LOG(ERROR, "ObMQThread not initialized", K(thread_index));
  } else if (OB_FAIL(thread_begin())) {
    LIB_LOG(ERROR, "thread_begin fail", K(ret));
  } else {
    while (! stop_flag_ && OB_SUCCESS == ret) {
      void *task = NULL;

      if (OB_FAIL(next_task_(thread_index, task))) {
        if (OB_IN_STOP_STATE != ret) {
          LIB_LOG(ERROR, "next_task_ fail", K(ret));
        }
      } else if (OB_FAIL(handle(task, thread_index, stop_flag_))) {
        if (OB_IN_STOP_STATE != ret) {
          LIB_LOG(ERROR, "handle task fail", K(ret), "task", (int64_t)task, K(thread_index));
        }
      } else {
        // do nothing
      }
    }
  }

  // 退出都调用thread_end()
  thread_end();

  // NOTE: 一个线程退出，其他线程同时退出
  stop_flag_ = true;
}

template <int MAX_THREAD_NUM, typename ModuleClass>
int ObMQThread<MAX_THREAD_NUM, ModuleClass>::next_task_(int64_t queue_index, void *&task)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LIB_LOG(ERROR, "ObMQThread not initialized");
    ret = OB_NOT_INIT;
  } else {
    RETRY_FUNC(stop_flag_, queue_, pop, task, queue_index, DATA_OP_TIMEOUT);
  }

  return ret;
}

template <int MAX_THREAD_NUM, typename ModuleClass>
int ObMQThread<MAX_THREAD_NUM, ModuleClass>::push(void *data, const uint64_t hash_value, const int64_t timeout)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LIB_LOG(ERROR, "ObMQThread not initialized");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(data)) {
    LIB_LOG(ERROR, "invalid argument", K(data));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(stop_flag_)) {
    ret = OB_IN_STOP_STATE;
  } else if (OB_FAIL(queue_.push(data, hash_value, timeout))) {
    if (OB_TIMEOUT != ret) {
      LIB_LOG(ERROR, "push queue fail", K(ret), KP(data), K(hash_value));
    }
  } else {
    // succ
  }

  return ret;
}

template <int MAX_THREAD_NUM, typename ModuleClass>
int ObMQThread<MAX_THREAD_NUM, ModuleClass>::get_total_task_num(int64_t &task_count)
{
  int ret = OB_SUCCESS;
  task_count = 0;

  for (int64_t idx= 0; idx < thread_num_ && OB_SUCC(ret); ++idx) {
    int64_t thread_task_count = 0;
    if (OB_FAIL(get_task_num(idx, thread_task_count))) {
      LIB_LOG(ERROR, "get_task_num fail", K(idx), K(thread_task_count));
    } else {
      task_count += thread_task_count;
    }
  }

  return ret;
}

template <int MAX_THREAD_NUM, typename ModuleClass>
int ObMQThread<MAX_THREAD_NUM, ModuleClass>::get_task_num(const int64_t thread_idx, int64_t &task_count)
{
  int ret = OB_SUCCESS;
  task_count = 0;

  if (OB_UNLIKELY(thread_idx < 0) || OB_UNLIKELY(thread_idx >= thread_num_)) {
    LIB_LOG(ERROR, "invalid argument", K(thread_idx), K(thread_num_));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(queue_.get_task_count(thread_idx, task_count))) {
    LIB_LOG(ERROR, "ObMultiFixedQueue get task count fail", K(ret), K(thread_idx), K(task_count));
  } else {
    // do nothing
  }

  return ret;
}
} // namespace common
} // namespace oceanbase
#endif /* OCEANBASE_MULTI_FIXED_QUEUE_THREAD_H__ */
