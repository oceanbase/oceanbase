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
 * Thread pool with ObMapQueue
 */

#ifndef OCEANBASE_LIB_MAP_QUEUE_THREAD_H__
#define OCEANBASE_LIB_MAP_QUEUE_THREAD_H__

#include "lib/thread/ob_map_queue.h"      // ObMapQueue

#include "share/ob_errno.h"                 // OB_SUCCESS
#include "lib/utility/ob_macro_utils.h"     // UNUSED
#include "lib/oblog/ob_log_module.h"        // LIB_LOG
#include "lib/atomic/ob_atomic.h"           // ATOMIC_*
#include "common/ob_queue_thread.h"         // ObCond

namespace oceanbase
{
namespace common
{

// Thread pool
//
// One ObMapQueue per thread
// Since ObMapQueue is scalable, push operations do not block
template <int MAX_THREAD_NUM = 32>
class ObMapQueueThread
{
  typedef ObMapQueue<void *> QueueType;
  static const int64_t DATA_OP_TIMEOUT = 1L * 1000L * 1000L;

public:
  ObMapQueueThread();
  virtual ~ObMapQueueThread();

public:
  // Inserting data
  // Non-blocking
  //
  // @retval OB_SUCCESS           Success
  // @retval Other_return_values  Fail
  int push(void *data, const uint64_t hash_val);

  // Thread execution function
  // Users can override this function to customize the thread execution
  virtual void run(const int64_t thread_index);

  // Data handling function
  // Users can also override this function to process data directly while keeping the run() function
  virtual int handle(void *data, const int64_t thread_index, volatile bool &stop_flag)
  {
    UNUSED(data);
    UNUSED(thread_index);
		stop_flag_ = stop_flag;
    return 0;
  }

protected:
  /// pop data from a thread-specific queue
  ///
  /// @param thread_index Thread number
  /// @param data The data returned
  ///
  /// @retval OB_SUCCESS        success
  /// @retval OB_EAGAIN         empty queue
  /// @retval other_error_code  Fail
  int pop(const int64_t thread_index, void *&data);

  /// Execute cond timedwait on a specific thread's queue
  void cond_timedwait(const int64_t thread_index, const int64_t wait_time);

public:
  int init(const int64_t thread_num, const char *label);
  void destroy();
  int start();
  void stop();
  void mark_stop_flag() { ATOMIC_STORE(&stop_flag_, true); }
  bool is_stoped() const { return ATOMIC_LOAD(&stop_flag_); }
  int64_t get_thread_num() const { return thread_num_; }

public:
  typedef ObMapQueueThread<MAX_THREAD_NUM> HostType;
  struct ThreadConf
  {
    pthread_t   tid_;
    HostType    *host_;
    int64_t     thread_index_;
    QueueType   queue_;
    ObCond      cond_;

    ThreadConf();
    virtual ~ThreadConf();

    int init(const char *label, const int64_t thread_index, HostType *host);
    void destroy();
  };

private:
  static void *thread_func_(void *arg);
  int next_task_(const int64_t thread_index, void *&task);

private:
  bool          inited_;
  int64_t       thread_num_;
  ThreadConf    tc_[MAX_THREAD_NUM];

// Valid for inherited classes
protected:
  volatile bool stop_flag_ CACHE_ALIGNED;

private:
  DISALLOW_COPY_AND_ASSIGN(ObMapQueueThread);
};

/////////////////////////////////////////////////////////////////////////////////////////////////////////

template <int MAX_THREAD_NUM>
ObMapQueueThread<MAX_THREAD_NUM>::ObMapQueueThread() :
    inited_(false),
    thread_num_(0),
    stop_flag_(true)
{
}

template <int MAX_THREAD_NUM>
ObMapQueueThread<MAX_THREAD_NUM>::~ObMapQueueThread()
{
  destroy();
}

template <int MAX_THREAD_NUM>
int ObMapQueueThread<MAX_THREAD_NUM>::init(const int64_t thread_num, const char *label)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(inited_)) {
    LIB_LOG(ERROR, "init twice");
    ret = OB_INIT_TWICE;
  } else if (OB_UNLIKELY(0 >= thread_num) || OB_UNLIKELY(thread_num > MAX_THREAD_NUM)) {
    LIB_LOG(ERROR, "invalid argument", K(thread_num));
    ret = OB_INVALID_ARGUMENT;
  } else {
    for (int64_t index = 0; OB_SUCCESS == ret && index < thread_num; index++) {
      if (OB_FAIL(tc_[index].init(label, index, this))) {
        LIB_LOG(ERROR, "init queue fail", KR(ret), K(index), K(label));
      }
    }

    thread_num_ = thread_num;
    stop_flag_ = false;
    inited_ = true;
  }

  return ret;
}

template <int MAX_THREAD_NUM>
void ObMapQueueThread<MAX_THREAD_NUM>::destroy()
{
  stop();

  inited_ = false;

  for (int64_t index = 0; index < thread_num_; index++) {
    tc_[index].destroy();
  }

  thread_num_ = 0;
  stop_flag_ = true;
}

template <int MAX_THREAD_NUM>
int ObMapQueueThread<MAX_THREAD_NUM>::start()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(! inited_)) {
    LIB_LOG(ERROR, "not inited");
    ret = OB_NOT_INIT;
  } else {
    stop_flag_ = false;

    for (int64_t index = 0; OB_SUCCESS == ret && index < thread_num_; index++) {
      int pthread_ret = 0;
      ThreadConf &tc = tc_[index];

      if (0 != (pthread_ret = pthread_create(&(tc.tid_), NULL, thread_func_, &tc))) {
        LIB_LOG(ERROR, "pthread_create fail", K(pthread_ret), KERRNOMSG(pthread_ret), K(index));
        ret = OB_ERR_UNEXPECTED;
      }
    }
  }
  return ret;
}

template <int MAX_THREAD_NUM>
void ObMapQueueThread<MAX_THREAD_NUM>::stop()
{
  mark_stop_flag();
  if (inited_) {

    for (int64_t index = 0; index < thread_num_; index++) {
      ThreadConf &tc = tc_[index];

      if (0 != tc.tid_) {
        int pthread_ret = pthread_join(tc.tid_, NULL);

        if (0 != pthread_ret) {
          LIB_LOG_RET(ERROR, OB_ERR_SYS, "pthread_join fail", "thread_id", tc.tid_, K(pthread_ret));
        }

        tc.tid_ = 0;
      }
    }
  }
}

template <int MAX_THREAD_NUM>
void *ObMapQueueThread<MAX_THREAD_NUM>::thread_func_(void *arg)
{
  ThreadConf *tc = static_cast<ThreadConf *>(arg);
  if (NULL != tc && NULL != tc->host_) {
    tc->host_->run(tc->thread_index_);
  }
  return NULL;
}

template <int MAX_THREAD_NUM>
void ObMapQueueThread<MAX_THREAD_NUM>::run(const int64_t thread_index)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LIB_LOG(ERROR, "not init");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(thread_index < 0) || OB_UNLIKELY(thread_index >= thread_num_)) {
    LIB_LOG(ERROR, "invalid thread index", K(thread_index), K(thread_num_));
    ret = OB_ERR_UNEXPECTED;
  } else {
    while (! is_stoped() && OB_SUCCESS == ret) {
      void *task = NULL;

      if (OB_FAIL(next_task_(thread_index, task))) {
        if (OB_IN_STOP_STATE != ret) {
          LIB_LOG(ERROR, "next_task_ fail", KR(ret), K(thread_index));
        }
      } else if (OB_FAIL(handle(task, thread_index, stop_flag_))) {
        if (OB_IN_STOP_STATE != ret) {
          LIB_LOG(ERROR, "handle task fail", KR(ret), "task", (int64_t)task, K(thread_index));
        }
      } else {
        // do nothing
      }
    }
  }

  // NOTE: One thread exits, others exit at the same time
  stop_flag_ = true;
}

template <int MAX_THREAD_NUM>
int ObMapQueueThread<MAX_THREAD_NUM>::pop(const int64_t thread_index, void *&data)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(! inited_)) {
    LIB_LOG(ERROR, "not init");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(thread_index < 0) || OB_UNLIKELY(thread_index >= thread_num_)) {
    LIB_LOG(ERROR, "invalid thread index", K(thread_index), K(thread_num_));
    ret = OB_ERR_UNEXPECTED;
  } else {
    ret = tc_[thread_index].queue_.pop(data);
  }
  return ret;
}

template <int MAX_THREAD_NUM>
void ObMapQueueThread<MAX_THREAD_NUM>::cond_timedwait(const int64_t thread_index,
    const int64_t wait_time)
{
  if (OB_UNLIKELY(! inited_)) {
    LIB_LOG_RET(ERROR, OB_NOT_INIT, "not init");
  } else if (OB_UNLIKELY(thread_index < 0) || OB_UNLIKELY(thread_index >= thread_num_)) {
    LIB_LOG_RET(ERROR, OB_INVALID_ARGUMENT, "invalid thread index", K(thread_index), K(thread_num_));
  } else {
    tc_[thread_index].cond_.timedwait(wait_time);
  }
}

template <int MAX_THREAD_NUM>
int ObMapQueueThread<MAX_THREAD_NUM>::next_task_(const int64_t index, void *&task)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(index < 0) || OB_UNLIKELY(index >= thread_num_)) {
    LIB_LOG(ERROR, "invalid thread index", K(index), K(thread_num_));
    ret = OB_ERR_UNEXPECTED;
  } else {
    ThreadConf &tc = tc_[index];
    while (! is_stoped() && OB_SUCCESS == ret) {
      task = NULL;

      if (OB_FAIL(tc.queue_.pop(task))) {
        if (OB_EAGAIN == ret) {
          // empty queue
          ret = OB_SUCCESS;
          tc.cond_.timedwait(DATA_OP_TIMEOUT);
        } else {
          LIB_LOG(ERROR, "pop task from queue fail", KR(ret));
        }
      } else if (OB_ISNULL(task)) {
        LIB_LOG(ERROR, "pop invalid task", K(task));
        ret = OB_ERR_UNEXPECTED;
      } else {
        break;
      }
    }

    if (is_stoped()) {
      ret = OB_IN_STOP_STATE;
    }
  }

  return ret;
}

template <int MAX_THREAD_NUM>
int ObMapQueueThread<MAX_THREAD_NUM>::push(void *data, const uint64_t hash_val)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LIB_LOG(ERROR, "not init");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(data)) {
    LIB_LOG(ERROR, "invalid argument", K(data));
    ret = OB_INVALID_ARGUMENT;
  } else {
    int64_t target_index = static_cast<int64_t>(hash_val % thread_num_);
    ThreadConf &tc = tc_[target_index];

    if (OB_FAIL(tc.queue_.push(data))) {
      LIB_LOG(ERROR, "push data fail", KR(ret), K(data), K(target_index));
    } else {
      tc.cond_.signal();
    }
  }

  return ret;
}

///////////////////////////////////////////// ThreadConf /////////////////////////////////////////////

template <int MAX_THREAD_NUM>
ObMapQueueThread<MAX_THREAD_NUM>::ThreadConf::ThreadConf() :
    tid_(0),
    host_(NULL),
    thread_index_(0),
    queue_(),
    cond_()
{}

template <int MAX_THREAD_NUM>
ObMapQueueThread<MAX_THREAD_NUM>::ThreadConf::~ThreadConf()
{
  destroy();
}

template <int MAX_THREAD_NUM>
int ObMapQueueThread<MAX_THREAD_NUM>::ThreadConf::init(const char *label,
    const int64_t thread_index,
    HostType *host)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(thread_index < 0) || OB_ISNULL(host)) {
    LIB_LOG(ERROR, "invalid argument", K(thread_index), K(host));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(queue_.init(label))) {
    LIB_LOG(ERROR, "init queue fail", KR(ret), K(label));
  } else {
    tid_ = 0;
    host_ = host;
    thread_index_ = thread_index;
  }
  return ret;
}

template <int MAX_THREAD_NUM>
void ObMapQueueThread<MAX_THREAD_NUM>::ThreadConf::destroy()
{
  queue_.destroy();
  tid_ = 0;
  host_ = NULL;
  thread_index_ = 0;
}

} // namespace common
} // namespace oceanbase
#endif /* OCEANBASE_LIB_QUEUE_M_FIXED_QUEUE_H_ */
