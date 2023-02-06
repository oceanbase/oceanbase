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
 * Thread pool with ObMsQueue
 */

#ifndef OCEANBASE_LIB_MS_QUEUE_THREAD_H__
#define OCEANBASE_LIB_MS_QUEUE_THREAD_H__

#include "lib/queue/ob_ms_queue.h"          // ObMsQueue
#include "lib/allocator/page_arena.h"       // ObArenaAllocator

#include "share/ob_errno.h"                 // OB_SUCCESS
#include "lib/oblog/ob_log_module.h"        // LIB_LOG
#include "lib/container/ob_bit_set.h"       // ObFixedBitSet
#include "common/ob_queue_thread.h"         // ObCond

namespace oceanbase
{
namespace common
{

// use a ObMsQueue global, each thread for a seq queue in ObMsQueue
// ModuleClass: Mark the module which is using the thread pool
template <int MAX_THREAD_NUM = 32, typename ModuleClass = void>
class ObMsQueueThread
{
  static const int64_t PRODUCER_TIMEWAIT = 1 * 1000;    // 1ms
  static const int64_t CONSUMER_TIMEWAIT = 100 * 1000;  // 100ms
  static const int64_t PAGE_SIZE = OB_MALLOC_NORMAL_BLOCK_SIZE;

public:
  ObMsQueueThread();
  virtual ~ObMsQueueThread();

public:
  typedef ObFixedBitSet<MAX_THREAD_NUM> BitSet;
  typedef ObLink Task;
  virtual int handle(Task *task, const int64_t thread_index, volatile bool &stop_flag) = 0;

public:
  int init(const int64_t thread_num, const int64_t queue_size);
  void destroy();
  int start();
  void stop();
  void run(const int64_t thread_index);
  void mark_stop_flag() { stop_flag_ = true; }

  bool is_stoped() const { return ATOMIC_LOAD(&stop_flag_); }

  // bit_set records the number of subqueues operated by the producer ObMsQueue, used for signal
  int push(Task *task,
      const int64_t seq,
      const uint64_t hash_value,
      BitSet &bit_set,
      const int64_t timeout);
  int end_batch(const int64_t seq,
      const int64_t count,
      const BitSet &bit_set);

  int64_t get_thread_num() const { return thread_num_; }

private:
  typedef ObMsQueueThread<MAX_THREAD_NUM, ModuleClass> HostType;
  struct ThreadConf
  {
    pthread_t   tid_;
    HostType    *host_;
    int64_t     thread_index_;
    // 1. For each sub-queue of MsQueue, it is a multi-producer-single-consumer model (multiple threads concurrently pushing tasks to the sub-queue, single-threaded consumption)
    // 2. FIXME does not wake up the exact thread that needs to be produced when the consumer gets the task and sends it via producer_cond_.
    // 3. The consumer wakes up exactly by recording the subqueue it has operated on

    // Producer cond
    ObCond      producer_cond_;
    // Consumer cond
    ObCond      consumer_cond_;

    ThreadConf();
    virtual ~ThreadConf();

    int init(const int64_t thread_index, HostType *host);
    void destroy();
  };

  static void *thread_func_(void *arg);
  int next_task_(int64_t queue_index, Task *&task);
  int push_(Task *task,
      const int64_t seq,
      const uint64_t hash_value,
      BitSet &bit_set,
      const int64_t timeout);

private:
  bool             inited_;
  int64_t          thread_num_;

  volatile bool    stop_flag_ CACHE_ALIGNED;

  ThreadConf       tc_[MAX_THREAD_NUM];
  ObMsQueue        queue_;
  ObArenaAllocator allocator_;            // 分配器

private:
  DISALLOW_COPY_AND_ASSIGN(ObMsQueueThread);
};

template <int MAX_THREAD_NUM, typename ModuleClass>
ObMsQueueThread<MAX_THREAD_NUM, ModuleClass>::ObMsQueueThread() :
    inited_(false),
    thread_num_(0),
    stop_flag_(true),
    queue_(),
    allocator_(ObModIds::OB_EXT_MS_QUEUE_QITEM, PAGE_SIZE)
{
}

template <int MAX_THREAD_NUM, typename ModuleClass>
ObMsQueueThread<MAX_THREAD_NUM, ModuleClass>::~ObMsQueueThread()
{
  destroy();
}

template <int MAX_THREAD_NUM, typename ModuleClass>
int ObMsQueueThread<MAX_THREAD_NUM, ModuleClass>::init(const int64_t thread_num, const int64_t queue_size)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(inited_)) {
    LIB_LOG(ERROR, "ObMsQueueThread has been initialized");
    ret = OB_INIT_TWICE;
  } else if (OB_UNLIKELY(thread_num <= 0)
      || OB_UNLIKELY(thread_num > MAX_THREAD_NUM)
      || OB_UNLIKELY(queue_size <= 0)) {
    LIB_LOG(ERROR, "invalid arguments", K(thread_num), K(MAX_THREAD_NUM), K(queue_size));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(queue_.init(thread_num, queue_size, &allocator_))) {
    LIB_LOG(ERROR, "initialize queue fail", KR(ret), K(thread_num), K(queue_size));
  } else {
    for (int64_t index = 0; OB_SUCCESS == ret && index < thread_num; index++) {
      if (OB_FAIL(tc_[index].init(index, this))) {
        LIB_LOG(ERROR, "init queue fail", KR(ret), K(index));
      }
    }

    thread_num_ = thread_num;
    stop_flag_ = true;

    inited_ = true;
  }

  return ret;
}

template <int MAX_THREAD_NUM, typename ModuleClass>
void ObMsQueueThread<MAX_THREAD_NUM, ModuleClass>::destroy()
{
  stop();

  inited_ = false;

  for (int64_t index = 0; index < thread_num_; index++) {
    tc_[index].destroy();
  }

  thread_num_ = 0;
  stop_flag_ = true;

  (void)queue_.destroy();
}

template <int MAX_THREAD_NUM, typename ModuleClass>
int ObMsQueueThread<MAX_THREAD_NUM, ModuleClass>::start()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LIB_LOG(ERROR, "ObMsQueueThread has not been initialized");
    ret = OB_NOT_INIT;
  } else if (stop_flag_) {
    stop_flag_ = false;

    for (int64_t index = 0; OB_SUCC(ret) && index < thread_num_; index++) {
      int pthread_ret = 0;
      ThreadConf &tc = tc_[index];

      if (0 != (pthread_ret = pthread_create(&(tc.tid_), NULL, thread_func_, &tc))) {
        LIB_LOG(ERROR, "pthread_create fail", K(pthread_ret),
                KERRNOMSG(pthread_ret), K(index));
        ret = OB_ERR_UNEXPECTED;
      }
    }
  }

  return ret;
}

template <int MAX_THREAD_NUM, typename ModuleClass>
void ObMsQueueThread<MAX_THREAD_NUM, ModuleClass>::stop()
{
  if (inited_) {
    stop_flag_ = true;

    for (int64_t index = 0; index < thread_num_; index++) {
      ThreadConf &tc = tc_[index];

      if (0 != tc.tid_) {
        int pthread_ret = pthread_join(tc.tid_, NULL);

        if (0 != pthread_ret) {
          LIB_LOG_RET(ERROR, OB_ERR_SYS, "pthread_join fail", "thread_id", tc.tid_, K(pthread_ret));
        } else {
          // do nothing
        }

        // finally reset to 0, to ensure that stop is called multiple times without problems
        // Because ObLogInstance may call stop and destroy externally, and destroy includes stop
        tc.tid_ = 0;
      }
    }
  }
}

template <int MAX_THREAD_NUM, typename ModuleClass>
void *ObMsQueueThread<MAX_THREAD_NUM, ModuleClass>::thread_func_(void *arg)
{
  if (NULL != arg) {
    ThreadConf *tc = static_cast<ThreadConf *>(arg);

    if (NULL != tc && NULL != tc->host_) {
      tc->host_->run(tc->thread_index_);
    }
  }

  return NULL;
}

template <int MAX_THREAD_NUM, typename ModuleClass>
void ObMsQueueThread<MAX_THREAD_NUM, ModuleClass>::run(const int64_t thread_index)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LIB_LOG(ERROR, "ObMQThread not initialized", K(thread_index));
  } else {
    while (! stop_flag_ && OB_SUCCESS == ret) {
      Task *task = NULL;

      if (OB_FAIL(next_task_(thread_index, task))) {
        if (OB_IN_STOP_STATE != ret) {
          LIB_LOG(ERROR, "next_task_ fail", KR(ret), K(thread_index));
        }
      } else if (OB_FAIL(handle(task, thread_index, stop_flag_))) {
        if (OB_IN_STOP_STATE != ret) {
          LIB_LOG(ERROR, "handle task fail", KR(ret), K(task), K(thread_index));
        }
      } else {
        // do nothing
      }
    }
  }

  // NOTE: One thread exits, others exit at the same time
  stop_flag_ = true;
}

// int get(Task*& task, const int64_t idx);
template <int MAX_THREAD_NUM, typename ModuleClass>
int ObMsQueueThread<MAX_THREAD_NUM, ModuleClass>::next_task_(int64_t queue_index, Task *&task)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(queue_index < 0) || OB_UNLIKELY(queue_index >= thread_num_)) {
    LIB_LOG(ERROR, "invalid thread index", K(queue_index), K(thread_num_));
    ret = OB_ERR_UNEXPECTED;
  } else {
    ThreadConf &tc = tc_[queue_index];

    while (! stop_flag_ && OB_SUCCESS == ret) {
      task = NULL;

      if (OB_FAIL(queue_.get(task, queue_index))) {
        if (OB_EAGAIN == ret) {
          // Data not ready at this time
          ret = OB_SUCCESS;
          tc.consumer_cond_.timedwait(CONSUMER_TIMEWAIT);
        } else {
          LIB_LOG(ERROR, "pop task from queue fail", KR(ret));
        }
      } else if (OB_ISNULL(task)) {
        LIB_LOG(ERROR, "get invalid task", K(task));
        ret = OB_ERR_UNEXPECTED;
      } else {
        break;
      }
    }

    if (OB_SUCC(ret)) {
      tc.producer_cond_.signal();
    }

    if (stop_flag_) {
      ret = OB_IN_STOP_STATE;
    }
  }

  return ret;
}

template <int MAX_THREAD_NUM, typename ModuleClass>
int ObMsQueueThread<MAX_THREAD_NUM, ModuleClass>::push(Task *task,
    const int64_t seq,
    const uint64_t hash_value,
    BitSet &bit_set,
    const int64_t timeout)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LIB_LOG(ERROR, "ObMsQueueThread not initialized");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(task)) {
    LIB_LOG(ERROR, "invalid argument", K(task));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(stop_flag_)) {
    ret = OB_IN_STOP_STATE;
  } else if (OB_FAIL(push_(task, seq, hash_value, bit_set, timeout))) {
    if (OB_TIMEOUT != ret) {
      LIB_LOG(ERROR, "push queue fail", KR(ret), KP(task), K(hash_value), K(bit_set));
    }
  } else {
    // succ
  }

  return ret;
}

template <int MAX_THREAD_NUM, typename ModuleClass>
int ObMsQueueThread<MAX_THREAD_NUM, ModuleClass>::push_(Task *task,
    const int64_t seq,
    const uint64_t hash,
    BitSet &bit_set,
    const int64_t timeout)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LIB_LOG(ERROR, "ObMsQueueThread not initialized");
    ret = OB_NOT_INIT;
  } else {
    int64_t target_index = static_cast<int64_t>(hash % thread_num_);
    ThreadConf &tc = tc_[target_index];
    int64_t end_time = ObTimeUtility::current_time() + timeout;

    while (true) {
      ret = queue_.push(task, seq, hash);

      if (OB_EAGAIN != ret) {
        break;
      }

      int64_t left_time = end_time - ObTimeUtility::current_time();

      if (left_time <= 0) {
        ret = OB_TIMEOUT;
        break;
      } else {
        const int64_t producer_timewait = PRODUCER_TIMEWAIT;
        tc.producer_cond_.timedwait(std::min(left_time, producer_timewait));
      }
    } // while

    if (OB_SUCC(ret)) {
      if (OB_FAIL(bit_set.add_member(target_index))) {
        LIB_LOG(ERROR, "bit_set add_member fail", KR(ret), K(task), K(seq), K(hash),
            K(bit_set), K(target_index));
      }
    }
  }

  return ret;
}

template <int MAX_THREAD_NUM, typename ModuleClass>
int ObMsQueueThread<MAX_THREAD_NUM, ModuleClass>::end_batch(const int64_t seq,
    const int64_t count,
    const BitSet &bit_set)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LIB_LOG(ERROR, "ObMsQueueThread not initialized");
    ret = OB_NOT_INIT;
  } else {
    ret = queue_.end_batch(seq, count);

    // Number of sub-queues operated on
    const int64_t handle_queue_total_cnt = bit_set.num_members();
    int64_t handle_queue_cnt = 0;

    // Iterate through all threads, trying to signal if it has operated before
    for (int64_t index = 0; OB_SUCC(ret) && index < thread_num_; index++) {
      if (bit_set.has_member(index)) {
        // Try to wake up
        ThreadConf &tc = tc_[index];
        if (queue_.next_is_ready(index)) {
          tc.consumer_cond_.signal();
        }

        // If all operated sub-queues are found, stop traversal
        ++handle_queue_cnt;
        if (handle_queue_cnt == handle_queue_total_cnt) {
          break;
        }
      }
    }
  }

  return ret;
}

///////////////////////////////////////////// ThreadConf /////////////////////////////////////////////

template <int MAX_THREAD_NUM, typename ModuleClass>
ObMsQueueThread<MAX_THREAD_NUM, ModuleClass>::ThreadConf::ThreadConf() :
    tid_(0),
    host_(NULL),
    thread_index_(0),
    producer_cond_(),
    consumer_cond_()
{}

template <int MAX_THREAD_NUM, typename ModuleClass>
ObMsQueueThread<MAX_THREAD_NUM, ModuleClass>::ThreadConf::~ThreadConf()
{
  destroy();
}

template <int MAX_THREAD_NUM, typename ModuleClass>
int ObMsQueueThread<MAX_THREAD_NUM, ModuleClass>::ThreadConf::init(const int64_t thread_index,
    HostType *host)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(thread_index < 0) || OB_ISNULL(host)) {
    LIB_LOG(ERROR, "invalid argument", K(thread_index), K(host));
    ret = OB_INVALID_ARGUMENT;
  } else {
    tid_ = 0;
    host_ = host;
    thread_index_ = thread_index;
  }

  return ret;
}

template <int MAX_THREAD_NUM, typename ModuleClass>
void ObMsQueueThread<MAX_THREAD_NUM, ModuleClass>::ThreadConf::destroy()
{
  tid_ = 0;
  host_ = NULL;
  thread_index_ = 0;
}

} // namespace common
} // namespace oceanbase
#endif /* OCEANBASE_LIB_MS_QUEUE_THREAD_H__ */
