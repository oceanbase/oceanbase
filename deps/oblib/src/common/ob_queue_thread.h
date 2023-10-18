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

#ifndef  OCEANBASE_COMMON_QUEUE_THREAD_H_
#define  OCEANBASE_COMMON_QUEUE_THREAD_H_
#include <sys/epoll.h>
#include "lib/ob_define.h"
#include "lib/queue/ob_fixed_queue.h"
#include "lib/allocator/page_arena.h"
#include "lib/lock/ob_drw_lock.h"
#include "lib/net/ob_addr.h"
#include "lib/metrics/ob_counter.h"
#include "common/ob_balance_filter.h"
#include "common/server_framework/ob_priority_scheduler.h"

namespace oceanbase
{
namespace common
{
enum PacketPriority
{
  HIGH_PRIV = -1,
  NORMAL_PRIV = 0,
  LOW_PRIV = 1,
};

class ObCond
{
  static const int64_t SPIN_WAIT_NUM = 0;
  static const int64_t BUSY_INTERVAL = 1000;
public:
  explicit ObCond(const int64_t spin_wait_num = SPIN_WAIT_NUM);
  ~ObCond();
public:
  void signal();
  int timedwait(const int64_t time_us);
  int wait();
private:
  const int64_t spin_wait_num_;
  volatile bool bcond_;
  int64_t last_waked_time_;
  pthread_cond_t cond_;
  pthread_mutex_t mutex_;
} __attribute__((aligned(64)));

typedef ObCond S2MCond;
class S2MQueueThread
{
  struct ThreadConf
  {
    void *pd;
    uint64_t index;
    volatile bool run_flag;
    volatile bool stop_flag;
    S2MCond queue_cond;
    volatile bool using_flag;
    volatile int64_t last_active_time;
    ObFixedQueue<void> high_prio_task_queue;
    ObFixedQueue<void> spec_task_queue;
    ObFixedQueue<void> comm_task_queue;
    ObFixedQueue<void> low_prio_task_queue;
    ObPriorityScheduler scheduler_;
    S2MQueueThread *host;
    ThreadConf() : pd(nullptr),
               index(0),
               run_flag(true),
               stop_flag(false),
               queue_cond(),
               using_flag(false),
               last_active_time(0),
               spec_task_queue(),
               comm_task_queue(),
               host(NULL)
    {
    };
  } CACHE_ALIGNED;
  static const int64_t THREAD_BUSY_TIME_LIMIT = 10 * 1000;
  static const int64_t QUEUE_WAIT_TIME = 100 * 1000;
  static const int64_t MAX_THREAD_NUM = 256;
  static const int64_t QUEUE_SIZE_TO_SWITCH = 4;
  typedef DRWLock RWLock;
  typedef DRWLock::RDLockGuard RDLockGuard;
  typedef DRWLock::WRLockGuard WRLockGuard;
  typedef ObTCCounter Counter;
public:
  enum {HIGH_PRIO_QUEUE = 0, HOTSPOT_QUEUE = 1, NORMAL_PRIO_QUEUE = 2, LOW_PRIO_QUEUE = 3, QUEUE_COUNT = 4};
public:
  S2MQueueThread();
  virtual ~S2MQueueThread();
public:
  int init(const int64_t thread_num, const int64_t task_num_limit, const bool queue_rebalance,
           const bool dynamic_rebalance);
  int set_prio_quota(v4si &quota);
  void destroy();
  int64_t get_queued_num() const;
  int64_t get_each_queued_num(int queue_id) const { return (queue_id >= 0 && queue_id < QUEUE_COUNT) ? each_queue_len_[queue_id].value() : 0; }
  int add_thread(const int64_t thread_num, const int64_t task_num_limit);
  int sub_thread(const int64_t thread_num);
  int64_t get_thread_num() const {return thread_num_;};
  int wakeup();
public:
  int push(void *task, const int64_t prio = NORMAL_PRIV);
  int push(void *task, const uint64_t task_sign, const int64_t prio);
  int push_low_prio(void *task);
  OB_INLINE int64_t &thread_index()
  {
    // Note: debug only right now
    RLOCAL_INLINE(int64_t, index);
    return index;
  }
  int64_t get_thread_index() const;
  virtual void on_iter() {}
  virtual void handle(void *task, void *pdata) = 0;
  virtual void handle_with_stopflag(void *task, void *pdata, volatile bool &stop_flag)
  {handle(task, pdata); if (stop_flag) {}};
  virtual void *on_begin() {return NULL;};
  virtual void on_end(void *ptr) {UNUSED(ptr);};
private:
  void *rebalance_(int64_t &idx, const ThreadConf &cur_thread);
  int launch_thread_(const int64_t thread_num, const int64_t task_num_limit);
  static void *thread_func_(void *data);
private:
  int64_t thread_num_;
  volatile uint64_t thread_conf_iter_;
  RWLock thread_conf_lock_;
  ThreadConf thread_conf_array_[MAX_THREAD_NUM];
  Counter queued_num_;
  Counter each_queue_len_[QUEUE_COUNT];
  bool queue_rebalance_;
  ObBalanceFilter balance_filter_;
};

typedef S2MCond M2SCond;
class M2SQueueThread
{
  static const int64_t QUEUE_WAIT_TIME;
public:
  M2SQueueThread();
  virtual ~M2SQueueThread();
public:
  int init(const int64_t task_num_limit,
           const int64_t idle_interval);
  void destroy();
public:
  int push(void *task);
  int64_t get_queued_num() const;
  virtual void handle(void *task, void *pdata) = 0;
  virtual void *on_begin() {return NULL;};
  virtual void on_end(void *ptr) {UNUSED(ptr);};
  virtual void on_idle() {};
private:
  static void *thread_func_(void *data);
private:
  bool inited_;
  void *pd_;
  volatile bool run_flag_;
  M2SCond queue_cond_;
  ObFixedQueue<void> task_queue_;
  int64_t idle_interval_;
  int64_t last_idle_time_;
} CACHE_ALIGNED;


}
}

#endif //OCEANBASE_COMMON_QUEUE_THREAD_H_
