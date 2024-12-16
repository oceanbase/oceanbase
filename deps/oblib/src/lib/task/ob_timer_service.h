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

#ifndef OCEANBASE_COMMON_OB_TIMER_SERVICE_
#define OCEANBASE_COMMON_OB_TIMER_SERVICE_

#include <pthread.h>
#include <typeinfo>
#include "lib/lock/mutex.h"
#include "lib/lock/ob_monitor.h"
#include "lib/ob_define.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/thread/thread_pool.h"
#include "lib/thread/ob_simple_thread_pool.h"
#include "lib/container/ob_heap.h"
#include "lib/hash/ob_hashset.h"
#include "lib/container/ob_se_array.h"
#include "lib/container/ob_se_array_iterator.h"
#include "lib/container/ob_vector.h"

namespace oceanbase
{

namespace common
{

extern uint64_t mtl_get_id();
class ObTimerTask
{
public:
  ObTimerTask() : timeout_check_(false) {}
  virtual ~ObTimerTask() {}
  virtual void runTimerTask() = 0;
  virtual int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    if (NULL != buf && buf_len > 0) {
      databuff_printf(buf, buf_len, pos, "task_type:%s, timeout_check:%s",
          typeid(*this).name(), timeout_check_ ? "True" : "False");
    }
    return pos;
  }
  inline void enable_timeout_check() { timeout_check_ = true; }
  inline void disable_timeout_check() { timeout_check_ = false; }
  inline bool timeout_check() { return timeout_check_; }
private:
  bool timeout_check_;
};

class ObTimer;
class TaskToken final
{
public:
  TaskToken(
      const ObTimer *timer,
      ObTimerTask *task,
      const int64_t st,
      const int64_t dt);
  TaskToken(const ObTimer *timer, ObTimerTask *task);
  TaskToken(const TaskToken &other) = delete;
  TaskToken &operator=(const TaskToken &other) = delete;
  ~TaskToken();
  TO_STRING_KV(KP(this), KP_(timer), KP_(task), K_(task_type), K_(scheduled_time), K_(delay));
public:
  char task_type_[128];
  const ObTimer *timer_;
  ObTimerTask *task_;
  int64_t scheduled_time_;
  int64_t delay_;
};

class ObTimerService;
class ObTimerTaskThreadPool final : public ObSimpleThreadPool
{
public:
  ObTimerTaskThreadPool(ObTimerService &service) : service_(service) {}
  virtual ~ObTimerTaskThreadPool() {}
  virtual void handle(void *task_token) override;
  ObTimerTaskThreadPool(const ObTimerTaskThreadPool &) = delete;
  ObTimerTaskThreadPool &operator=(const ObTimerTaskThreadPool &) = delete;
private:
  ObTimerService &service_;
private:
  static constexpr int64_t ELAPSED_TIME_LOG_THREASHOLD = 10 * 60 * 1000 * 1000; // 10 mins
};

class ObTimerService : public lib::ThreadPool
{
private:
  static constexpr int64_t INITIAL_ELEMENT_NUM = 1024L;
  using TokenAlloc = hash::SimpleAllocer<TaskToken, INITIAL_ELEMENT_NUM>;
public:
  explicit ObTimerService(uint64_t tenant_id = OB_SERVER_TENANT_ID);
  ~ObTimerService();
  static ObTimerService& get_instance()
  {
    static ObTimerService ts(OB_SERVER_TENANT_ID);
    return ts;
  }
  ObTimerService(const ObTimerService &) = delete;
  ObTimerService &operator=(const ObTimerService &) = delete;
  TO_STRING_KV(KP(this), K(tenant_id_),
      K(priority_task_queue_.size()),
      K(running_task_set_.size()),
      K(uncanceled_task_set_.size()),
      K(worker_thread_pool_.get_queue_num()));
  int start();
  void stop();
  void wait();
  void destroy();
  int schedule_task(
      const ObTimer *timer,
      ObTimerTask &task,
      const int64_t delay,
      const bool repeate = false,
      const bool immediately = false);
  int schedule_task(TaskToken *token);
  int cancel_task(const ObTimer *timer, const ObTimerTask *task);
  int wait_task(const ObTimer *timer, const ObTimerTask *task);
  bool task_exist(const ObTimer *timer, const ObTimerTask &task);
  inline uint64_t get_tenant_id() const { return tenant_id_; }
  bool is_never_started() const { return is_never_started_; }
  bool is_stopped() const { return is_stopped_; }
  static int mtl_new(ObTimerService *&timer_service);
  static int mtl_start(ObTimerService *&timer_service);
  static void mtl_stop(ObTimerService *&timer_service);
  static void mtl_wait(ObTimerService *&timer_service);
  static void mtl_destroy(ObTimerService *&timer_service);
private:
  bool has_running_task(const ObTimer *timer) const;
  bool find_task_in_set(
      const ObSortedVector<TaskToken *> &token_set,
      const ObTimer *timer,
      const ObTimerTask *task) const;
  int pop_task(int64_t now, TaskToken *&token, int64_t &st);
  void run1() final;
  static bool has_same_task_and_timer(
      const TaskToken *token,
      const ObTimer *timer,
      const ObTimerTask *task);
  static void check_clock();
  int new_token(
      TaskToken *&token,
      const ObTimer *timer,
      ObTimerTask *task,
      const int64_t st,
      const int64_t dt);
  void delete_token(TaskToken *&token);
  void dump_info();
private:
  bool is_never_started_;
  bool is_stopped_;
  uint64_t tenant_id_;
  obutil::ObMonitor<obutil::Mutex> monitor_;
  TokenAlloc token_alloc_;
  ObSortedVector<TaskToken *> priority_task_queue_;
  ObSortedVector<TaskToken *> running_task_set_;
  ObSortedVector<TaskToken *> uncanceled_task_set_;
  ObTimerTaskThreadPool worker_thread_pool_;
  lib::ObMutex mutex_;
private:
  static constexpr int64_t MIN_WAIT_INTERVAL = 10L * 1000L;          // 10ms
  static constexpr int64_t MAX_WAIT_INTERVAL = 100L * 1000L;         // 100ms
  static constexpr int64_t MIN_WORKER_THREAD_NUM = 4L;
  static constexpr int64_t MAX_WORKER_THREAD_NUM = 128L;
  static constexpr int64_t TASK_NUM_LIMIT = 10000L;
  static constexpr int64_t CLOCK_SKEW_DELTA = 20L * 1000L;          // 20ms
  static constexpr int64_t CLOCK_ERROR_DELTA = 500L * 1000L;        // 500ms
  static constexpr int64_t DUMP_INTERVAL = 60L * 1000L * 1000L;     // 60s
  using VecIter = ObSortedVector<TaskToken *>::iterator;
};

#define NEW_AND_SET_TIMER_SERVICE(id)                                                             \
do {                                                                                              \
  uint64_t tid = (uint64_t) id;                                                                   \
  int tmp_ret = OB_SUCCESS;                                                                       \
  ObTimerService *timer_service = MTL(ObTimerService *);                                          \
  if (nullptr == timer_service) {                                                                 \
    ObTenantBase *tenant_base = MTL_CTX();                                                        \
    if (nullptr == tenant_base) {                                                                 \
      tmp_ret = OB_ERR_NULL_VALUE;                                                                \
      LOG_WARN("tenant_base is NULL", K(tid), K(tmp_ret));                                        \
    } else if (nullptr == (timer_service = OB_NEW(ObTimerService, "timer_service", tid))) {       \
      tmp_ret = OB_ALLOCATE_MEMORY_FAILED;                                                        \
      LOG_WARN("failed to new timer service", K(tid), K(tmp_ret));                                \
    } else if (OB_SUCCESS != (tmp_ret = timer_service->start())) {                                \
      LOG_WARN("failed to start timer service", K(tid), K(tmp_ret));                              \
    } else {                                                                                      \
      tenant_base->set(timer_service);                                                            \
    }                                                                                             \
  }                                                                                               \
} while (0)

} /* common */
} /* oceanbase */

#endif // OCEANBASE_COMMON_OB_TIMER_SERVICE_
