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

#ifndef OCEANBASE_COMMON_OB_TIMER_
#define OCEANBASE_COMMON_OB_TIMER_

#include <pthread.h>
#include <typeinfo>
#include "lib/lock/mutex.h"
#include "lib/lock/ob_monitor.h"
#include "lib/ob_define.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/thread/thread_pool.h"

namespace oceanbase
{
namespace tests
{
namespace blocksstable
{
class FakeTabletManager;
}
}

namespace common
{
class ObTimer;

class ObTimerTask
{
  friend class ObTimer;
public:
  ObTimerTask() : timeout_check_(true), timer_(nullptr) {}
  virtual ~ObTimerTask() { abort_unless(OB_ISNULL(ATOMIC_LOAD(&timer_))); }
  virtual void runTimerTask() = 0;
  virtual int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    if (NULL != buf && buf_len > 0) {
      databuff_printf(buf, buf_len, pos, "tasktype:%s", typeid(*this).name());
    }
    return pos;
  }

  inline void enable_timeout_check() { timeout_check_ = true; }
  inline void disable_timeout_check() { timeout_check_ = false; }
  inline bool timeout_check() { return timeout_check_; }
private:
  bool timeout_check_;
  ObTimer* timer_;
};

class ObTimer
    : public lib::ThreadPool
{
public:
  friend class oceanbase::tests::blocksstable::FakeTabletManager;
  ObTimer(int64_t max_task_num = 32): tasks_num_(0), max_task_num_(max_task_num), wakeup_time_(0), is_inited_(false), is_stopped_(false),
      is_destroyed_(false), tokens_(nullptr), running_task_(nullptr), uncanceled_task_(nullptr), has_running_repeat_task_(false),
      thread_id_(-1), thread_name_(nullptr) {}
  ~ObTimer();
  int init(const char* thread_name = nullptr,
           const ObMemAttr &attr = ObMemAttr(OB_SERVER_TENANT_ID, "timer"));
  bool inited() const;
  int create(); // create new timer thread and start
  int start();  // only start
  void stop();   // only stop
  void wait();   // wait all running task finish
  void destroy();
public:
  int schedule(ObTimerTask &task, const int64_t delay, bool repeate = false, bool immediate = false);
  int schedule_repeate_task_immediately(ObTimerTask &task, const int64_t delay);
  bool task_exist(const common::ObTimerTask &task);
  int task_exist(const common::ObTimerTask &task, bool &exist)
  {
    exist = task_exist(task);
    return OB_SUCCESS;
  }
  int cancel(const ObTimerTask &task);
  int cancel_task(const ObTimerTask &task);
  int wait_task(const ObTimerTask &task);
  void cancel_all();
  int64_t get_tasks_num() const { return tasks_num_; }
  void dump() const;
private:
  struct Token
  {
    Token(): scheduled_time(0), delay(0), task(NULL) {}
    Token(const int64_t st, const int64_t dt, ObTimerTask *task)
        : scheduled_time(st), delay(dt), task(task) {}
    void reset()
    {
      scheduled_time = 0;
      delay = 0;
      task = NULL;
    }
    TO_STRING_KV(K(scheduled_time), K(delay), KP(task), KPC(task));
    int64_t scheduled_time;
    int64_t delay;
    ObTimerTask *task;
  };
  int insert_token(const Token &token);
  void run1() final;
  int schedule_task(ObTimerTask &task, const int64_t delay, const bool repeate, const bool is_scheduled_immediately);
  DISALLOW_COPY_AND_ASSIGN(ObTimer);
private:
  const static int64_t ELAPSED_TIME_LOG_THREASHOLD = 10 * 60 * 1000 * 1000; // 10 mins
  int64_t tasks_num_;
  int64_t max_task_num_;
  int64_t wakeup_time_;
  bool is_inited_;
  bool is_stopped_;
  bool is_destroyed_;
  obutil::ObMonitor<obutil::Mutex> monitor_;
  Token* tokens_;
  ObTimerTask *running_task_;
  ObTimerTask *uncanceled_task_;
  bool has_running_repeat_task_;
  int64_t thread_id_;
  const char* thread_name_;
};

} /* common */
} /* oceanbase */

#endif // OCEANBASE_COMMON_OB_TIMER_
