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
 * ObCdcSharedQueueThread: a shared-queue multi-thread worker for CDC modules.
 *
 * All worker threads compete to pop from a single shared ObFixedQueue, providing
 * natural load balancing. Suitable for medium-throughput tasks with variable
 * processing times (e.g., PartTransTask recycling, post-commit processing).
 *
 * Built on lib::Threads with:
 *   - Typed ObFixedQueue<T> (no void* casting)
 *   - ObCond-based blocking pop (low CPU when idle)
 *   - Blocking push with configurable timeout
 *   - Dynamic thread count adjustment at runtime (via lib::Threads::set_thread_count)
 *   - Automatic thread naming via set_cdc_thread_name()
 *
 * Contrast with ObMQThread (per-thread queues, hash-based routing):
 *   ObMQThread is better for high-throughput, uniform-latency tasks (e.g., BR/LogEntry recycling).
 *   ObCdcSharedQueueThread is better when task latency varies and load balancing matters.
 *
 * Stop flag semantics:
 *   - Internal stop: set by mark_stop_flag() / stop(), or automatically when handle() returns error.
 *   - External stop: optional volatile bool* passed at init(), typically shared by the owning
 *     module (e.g., RC facade) so that a global error stops all sub-pools AND their push() callers.
 *   - is_stopped() checks BOTH flags: (internal || *external).
 *   - push() and thread loop both check is_stopped(), so an external stop signal
 *     immediately unblocks a push() retrying on a full queue.
 *
 * Usage:
 *   volatile bool global_stop = false;
 *
 *   class MyWorker : public ObCdcSharedQueueThread<MyTask> {
 *   protected:
 *     int handle(MyTask *task, const int64_t thread_index) override {
 *       // process task ...
 *       return OB_SUCCESS;
 *     }
 *   };
 *
 *   MyWorker worker;
 *   worker.init(4, 10000, "CDC-MyWorker", &global_stop);
 *   worker.start();
 *   worker.push(some_task);
 *   // ...
 *   global_stop = true;   // stops push() + threads across all pools sharing this flag
 *   worker.stop();
 *   worker.destroy();
 */

#ifndef OCEANBASE_LIBOBCDC_OB_CDC_SHARED_QUEUE_THREAD_H_
#define OCEANBASE_LIBOBCDC_OB_CDC_SHARED_QUEUE_THREAD_H_

#include "lib/thread/threads.h"           // lib::Threads
#include "lib/queue/ob_fixed_queue.h"     // ObFixedQueue
#include "common/ob_queue_thread.h"       // ObCond
#include "lib/oblog/ob_log_module.h"      // OBLOG_LOG
#include "lib/utility/ob_macro_utils.h"   // DISALLOW_COPY_AND_ASSIGN, OB_UNLIKELY, etc.
#include "ob_log_utils.h"                 // set_cdc_thread_name

namespace oceanbase
{
namespace libobcdc
{

template <typename T>
class ObCdcSharedQueueThread : public lib::Threads
{
  static const int64_t QUEUE_POP_TIMEOUT_US = 1L * 1000L * 1000L;    // 1s
  static const int64_t PUSH_RETRY_SLEEP_US = 1L * 1000L;             // 1ms
  static const int64_t PUSH_TIMEOUT_PRINT_INTERVAL = 10L * 1000L * 1000L; // 10s

public:
  ObCdcSharedQueueThread();
  virtual ~ObCdcSharedQueueThread();

  /// Initialize the thread pool and shared queue.
  ///
  /// @param thread_num         Number of worker threads.
  /// @param queue_size         Capacity of the shared ObFixedQueue.
  /// @param thread_label       Thread name prefix (e.g., "CDC-RC-PartTrans").
  ///                           Threads are named "{label}_{idx}" via prctl.
  /// @param external_stop_flag Optional pointer to an external stop flag.
  ///                           When non-null, is_stopped() returns true if *external_stop_flag
  ///                           is set, even if mark_stop_flag() was not called on this pool.
  ///                           Typical usage: the owning module (RC facade, Committer) passes
  ///                           its own stop_flag so all sub-pools respond to a global stop.
  int init(const int64_t thread_num,
           const int64_t queue_size,
           const char *thread_label,
           volatile bool *external_stop_flag = nullptr);

  int start();

  /// Blocking stop: set stop flag, wake all threads, wait for all to exit.
  void stop() override;

  /// Non-blocking: set stop flag and wake waiting threads, but do not join.
  void mark_stop_flag();

  void destroy();

  bool is_stopped() const;

  /// Push a task into the shared queue.
  ///
  /// @param task     Non-null task pointer. Ownership transfers to the queue.
  /// @param timeout  Max wait time in microseconds when queue is full.
  ///                   INT64_MAX (default) = block until success or stop.
  ///                   0 = non-blocking, returns OB_SIZE_OVERFLOW if full.
  /// @retval OB_SUCCESS          Task pushed successfully.
  /// @retval OB_SIZE_OVERFLOW    Queue full (only when timeout == 0).
  /// @retval OB_TIMEOUT          Queue still full after timeout.
  /// @retval OB_IN_STOP_STATE    Thread pool is stopping.
  int push(T *task, const int64_t timeout = INT64_MAX);

  /// Dynamically adjust the number of worker threads at runtime.
  /// lib::Threads handles thread creation/destruction internally.
  /// Hides base class version to add validation and logging.
  int set_thread_count(const int64_t n_threads);

  int64_t get_queue_count() const;

protected:
  /// Subclass must implement: process one task.
  /// Called from a worker thread. Returning error causes the thread to exit and the pool to stop.
  ///
  /// @param task          The task to process (guaranteed non-null).
  /// @param thread_index  Index of the calling worker thread [0, thread_num).
  virtual int handle(T *task, const int64_t thread_index) = 0;

  /// Optional hooks for per-thread setup/teardown.
  /// thread_begin is called after thread name is set, before the handle() loop.
  /// thread_end is called after the handle() loop exits.
  virtual void thread_begin(const int64_t thread_index) { UNUSED(thread_index); }
  virtual void thread_end(const int64_t thread_index) { UNUSED(thread_index); }

private:
  // lib::Threads entry point — do not override in subclasses, implement handle() instead.
  void run(int64_t idx) override;

  // Wake all threads waiting on the condition variable.
  void wake_all_();

private:
  bool                        inited_;
  common::ObFixedQueue<T>     queue_;
  common::ObCond              cond_;
  const char                  *thread_label_;
  volatile bool               *external_stop_flag_;  // Optional external stop signal (read-only)

  DISALLOW_COPY_AND_ASSIGN(ObCdcSharedQueueThread);
};

// ======================== Template Implementation ========================

template <typename T>
ObCdcSharedQueueThread<T>::ObCdcSharedQueueThread()
  : lib::Threads(0),
    inited_(false),
    queue_(),
    cond_(ObCond::SPIN_WAIT_NUM, common::ObWaitEventIds::CDC_COMMON_COND_WAIT),
    thread_label_(nullptr),
    external_stop_flag_(nullptr)
{
}

template <typename T>
ObCdcSharedQueueThread<T>::~ObCdcSharedQueueThread()
{
  destroy();
}

template <typename T>
int ObCdcSharedQueueThread<T>::init(
    const int64_t thread_num,
    const int64_t queue_size,
    const char *thread_label,
    volatile bool *external_stop_flag)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(inited_)) {
    OBLOG_LOG(ERROR, "ObCdcSharedQueueThread has been initialized");
    ret = OB_INIT_TWICE;
  } else if (OB_UNLIKELY(thread_num <= 0)
      || OB_UNLIKELY(queue_size <= 0)
      || OB_ISNULL(thread_label)) {
    OBLOG_LOG(ERROR, "invalid arguments", K(thread_num), K(queue_size), KP(thread_label));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(lib::Threads::set_thread_count(thread_num))) {
    OBLOG_LOG(ERROR, "set_thread_count fail", KR(ret), K(thread_num));
  } else if (OB_FAIL(lib::Threads::init())) {
    OBLOG_LOG(ERROR, "Threads init fail", KR(ret));
  } else if (OB_FAIL(queue_.init(queue_size))) {
    OBLOG_LOG(ERROR, "queue init fail", KR(ret), K(queue_size));
  } else {
    thread_label_ = thread_label;
    external_stop_flag_ = external_stop_flag;
    inited_ = true;

    OBLOG_LOG(INFO, "ObCdcSharedQueueThread init succ",
        KCSTRING_(thread_label), K(thread_num), K(queue_size),
        "has_external_stop_flag", (nullptr != external_stop_flag));
  }

  return ret;
}

template <typename T>
int ObCdcSharedQueueThread<T>::start()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    OBLOG_LOG(ERROR, "ObCdcSharedQueueThread has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(lib::Threads::start())) {
    OBLOG_LOG(ERROR, "Threads start fail", KR(ret), KCSTRING_(thread_label));
  } else {
    OBLOG_LOG(INFO, "ObCdcSharedQueueThread start succ",
        KCSTRING_(thread_label), "thread_num", get_thread_count());
  }

  return ret;
}

template <typename T>
void ObCdcSharedQueueThread<T>::stop()
{
  if (inited_) {
    mark_stop_flag();
    lib::Threads::wait();
    OBLOG_LOG(INFO, "ObCdcSharedQueueThread stop succ", "label", thread_label_);
  }
}

template <typename T>
void ObCdcSharedQueueThread<T>::mark_stop_flag()
{
  if (inited_) {
    lib::Threads::stop();   // sets internal stop_ = true
    wake_all_();            // unblock threads sleeping on cond_
  }
}

template <typename T>
void ObCdcSharedQueueThread<T>::destroy()
{
  if (inited_) {
    stop();
    lib::Threads::destroy();
    queue_.destroy();
    thread_label_ = nullptr;
    external_stop_flag_ = nullptr;
    inited_ = false;
    OBLOG_LOG(INFO, "ObCdcSharedQueueThread destroy succ");
  }
}

template <typename T>
bool ObCdcSharedQueueThread<T>::is_stopped() const
{
  return has_set_stop()
      || (nullptr != external_stop_flag_ && ATOMIC_LOAD(external_stop_flag_));
}

template <typename T>
int ObCdcSharedQueueThread<T>::push(T *task, const int64_t timeout)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(is_stopped())) {
    ret = OB_IN_STOP_STATE;
  } else {
    const int64_t start_time = common::ObTimeUtility::current_time();
    const int64_t end_time = (INT64_MAX == timeout)
        ? INT64_MAX
        : start_time + timeout;

    while (OB_SUCC(ret) && ! is_stopped()) {
      ret = queue_.push(task);

      if (OB_SUCCESS == ret) {
        // Successfully enqueued, wake one consumer thread
        cond_.signal();
        break;
      } else if (OB_SIZE_OVERFLOW == ret) {
        // Queue full
        if (0 == timeout) {
          // Non-blocking mode: return immediately
          break;
        }

        const int64_t now = common::ObTimeUtility::current_time();
        if (now >= end_time) {
          ret = OB_TIMEOUT;
          break;
        }

        // Log periodically while retrying
        if (now - start_time > PUSH_TIMEOUT_PRINT_INTERVAL) {
          if (TC_REACH_TIME_INTERVAL(PUSH_TIMEOUT_PRINT_INTERVAL)) {
            OBLOG_LOG(INFO, "ObCdcSharedQueueThread push retrying, queue full",
                KCSTRING_(thread_label),
                "queue_count", queue_.get_curr_total(),
                "retry_time_us", now - start_time);
          }
        }

        ret = OB_SUCCESS;
        const int64_t left_time = end_time - now;
        const int64_t sleep_time = (left_time < PUSH_RETRY_SLEEP_US)
            ? left_time
            : PUSH_RETRY_SLEEP_US;
        ob_usleep(static_cast<uint32_t>(sleep_time));
      } else {
        OBLOG_LOG(ERROR, "push to queue fail", KR(ret), KCSTRING_(thread_label));
        break;
      }
    }

    if (is_stopped() && OB_SUCC(ret)) {
      ret = OB_IN_STOP_STATE;
    }
  }

  return ret;
}

template <typename T>
int ObCdcSharedQueueThread<T>::set_thread_count(const int64_t n_threads)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(n_threads <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    OBLOG_LOG(ERROR, "invalid thread count", K(n_threads), KCSTRING_(thread_label));
  } else {
    const int64_t old_count = get_thread_count();

    if (old_count == n_threads) {
      // No change, skip
    } else if (OB_FAIL(lib::Threads::set_thread_count(n_threads))) {
      OBLOG_LOG(WARN, "set_thread_count fail", KR(ret),
        KCSTRING_(thread_label), K(old_count), K(n_threads));
    } else {
      OBLOG_LOG(INFO, "ObCdcSharedQueueThread set_thread_count succ",
        KCSTRING_(thread_label), K(old_count), K(n_threads));
    }
  }

  return ret;
}

template <typename T>
int64_t ObCdcSharedQueueThread<T>::get_queue_count() const
{
  return queue_.get_curr_total();
}

template <typename T>
void ObCdcSharedQueueThread<T>::run(int64_t idx)
{
  int ret = OB_SUCCESS;

  if (nullptr != thread_label_) {
    set_cdc_thread_name(thread_label_, idx);
  }

  thread_begin(idx);

  while (! is_stopped() && OB_SUCCESS == ret) {
    T *task = nullptr;
    ret = queue_.pop(task);

    if (OB_SUCCESS == ret && OB_NOT_NULL(task)) {
      ret = handle(task, idx);
      if (OB_SUCCESS != ret) {
        if (OB_IN_STOP_STATE != ret) {
          OBLOG_LOG(ERROR, "handle task fail", KR(ret), KCSTRING_(thread_label), K(idx));
        }
      }
    } else if (OB_ENTRY_NOT_EXIST == ret) {
      // Queue empty, wait for producer signal or timeout
      ret = OB_SUCCESS;
      cond_.timedwait(QUEUE_POP_TIMEOUT_US);
    } else if (OB_SUCCESS != ret) {
      OBLOG_LOG(ERROR, "pop from queue fail", KR(ret), KCSTRING_(thread_label), K(idx));
    }
  }

  thread_end(idx);

  // On fatal error: set internal stop flag to stop all sibling threads and unblock push() callers.
  // This matches ObMQThread behavior where one thread error causes all threads to stop.
  if (OB_SUCCESS != ret && OB_IN_STOP_STATE != ret) {
    OBLOG_LOG(ERROR, "ObCdcSharedQueueThread worker exits on error, stopping pool",
        KR(ret), KCSTRING_(thread_label), K(idx));
    mark_stop_flag();
  }
}

template <typename T>
void ObCdcSharedQueueThread<T>::wake_all_()
{
  const int64_t n = get_thread_count();
  for (int64_t i = 0; i < n; i++) {
    cond_.signal();
  }
}

} // namespace libobcdc
} // namespace oceanbase

#endif // OCEANBASE_LIBOBCDC_OB_CDC_SHARED_QUEUE_THREAD_H_
