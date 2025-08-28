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

#define USING_LOG_PREFIX COMMON
#include "lib/task/ob_timer_service.h"
#include "lib/task/ob_timer_monitor.h"       // ObTimerMonitor
#include "lib/thread/thread_mgr.h"           // get_tenant_tg_helper

namespace oceanbase
{
namespace common
{
using namespace obutil;
using namespace lib;

uint64_t __attribute__((weak)) mtl_get_id()
{
  int ret = OB_SUCCESS;
  OB_LOG(WARN, "call weak mtl_get_id");
  return OB_SERVER_TENANT_ID;
}

bool compare_for_queue(const TaskToken *a, const TaskToken *b)
{
  abort_unless(nullptr != a && nullptr != b);
  return a->scheduled_time_ < b->scheduled_time_;
}

bool compare_for_set(const TaskToken *a, const TaskToken *b)
{
  abort_unless(nullptr != a);
  abort_unless(nullptr != b);
  abort_unless(nullptr != a->timer_);
  abort_unless(nullptr != b->timer_);
  return (a->timer_ < b->timer_) || (a->timer_ == b->timer_ && a->task_ < b->task_);
}

bool token_unique(const TaskToken *a, const TaskToken *b)
{
  abort_unless(nullptr != a && nullptr != b);
  return a == b;
}

TaskToken::TaskToken(
    const ObTimer *timer,
    ObTimerTask *task,
    const int64_t st,
    const int64_t dt)
  : timer_(timer), task_(task), scheduled_time_(st),
    last_try_pop_time_(0), pushed_time_(0L), delay_(dt)
{
  task_type_[0] = '\0';
  timer_name_[0] = '\0';
  if (nullptr != task) {
    ObTimerUtil::copy_buff(task_type_, sizeof(task_type_), typeid(*task).name());
  }
  if (nullptr != timer) {
    ObTimerUtil::copy_buff(timer_name_, sizeof(timer_name_), timer->timer_name_);
  }
}

TaskToken::TaskToken(const ObTimer *timer, ObTimerTask *task)
  : TaskToken(timer, task, 0, 0)
{}

TaskToken::~TaskToken()
{
  scheduled_time_ = 0L;
  last_try_pop_time_ = 0L;
  pushed_time_ = 0L;
  delay_ = 0L;
  task_ = nullptr;
  timer_ = nullptr;
}

void ObTimerTaskThreadPool::handle(void *task_token)
{
  TaskToken *token = reinterpret_cast<TaskToken *>(task_token);
  if (nullptr == token) {
    OB_LOG_RET(WARN, OB_ERR_NULL_VALUE, "TaskToken is NULL", K(ret));
  } else if (nullptr == token->task_) {
    OB_LOG_RET(WARN, OB_ERR_NULL_VALUE, "task is NULL", K(ret));
  } else {
    int64_t thread_id = 0L;
    bool do_timeout_check = token->task_->timeout_check();
    const int64_t start_time = ::oceanbase::common::ObTimeUtility::current_time();
    const int64_t start_time_sys = ObSysTime::now().toMicroSeconds();
    if (do_timeout_check) {
      thread_id = GETTID();
      ObTimerMonitor::get_instance().start_task(thread_id, start_time, token->delay_, token->task_);
    }
    THIS_WORKER.set_timeout_ts(INT64_MAX); // reset timeout to INT64_MAX
    ObCurTraceId::reset(); // reset trace_id
    set_ext_tname(token);
    ObDIActionGuard(typeid(*token->task_));
    token->task_->runTimerTask();
    clear_ext_tname(); // reset ext_tname
    const int64_t end_time = ::oceanbase::common::ObTimeUtility::current_time();
    const int64_t end_time_sys = ObSysTime::now().toMicroSeconds();
    if (do_timeout_check) {
      ObTimerMonitor::get_instance().end_task(thread_id, end_time);
    }
    alarm_if_necessary(token, start_time_sys, end_time_sys);
    IGNORE_RETURN service_.schedule_task(token);
  }
}

void ObTimerTaskThreadPool::alarm_if_necessary(
    const TaskToken *token,
    int64_t start_time,
    int64_t end_time)
{
  if (nullptr != token) {
    const int64_t delay_in_priority_queue = token->pushed_time_ - token->scheduled_time_;
    const int64_t delay_in_thread_pool = start_time - token->pushed_time_;
    const int64_t elapsed_time = end_time - start_time;
    if (delay_in_priority_queue > ObTimerService::DELAY_IN_PRI_QUEUE_THREASHOLD) {
      const int64_t threashold = ObTimerService::DELAY_IN_PRI_QUEUE_THREASHOLD;
      OB_LOG_RET(WARN, OB_ERR_TOO_MUCH_TIME, "timer task too much delay in priority queue",
          KPC(token), K(delay_in_priority_queue), K(threashold));
    }
    if (delay_in_thread_pool > DELAY_IN_THREAD_POOL_THREASHOLD) {
      const int64_t threashold = DELAY_IN_THREAD_POOL_THREASHOLD;
      OB_LOG_RET(WARN, OB_ERR_TOO_MUCH_TIME, "timer task too much delay in thread pool",
          KPC(token), K(delay_in_thread_pool), K(threashold));
    }
    if (elapsed_time > ELAPSED_TIME_LOG_THREASHOLD) {
      const int64_t threashold = ELAPSED_TIME_LOG_THREASHOLD;
      OB_LOG_RET(WARN,  OB_ERR_TOO_MUCH_TIME, "timer task cost too much time",
          KPC(token), K(elapsed_time), K(threashold));
    }
    if (token->delay_ > 0 && elapsed_time > token->delay_) {
      OB_LOG_RET(WARN,  OB_ERR_TOO_MUCH_TIME, "timer task's elapsed_time is greater than period",
          KPC(token), K(elapsed_time), "period", token->delay_);
    }
  }
}

void ObTimerTaskThreadPool::set_ext_tname(const TaskToken *token)
{
  if (nullptr != token) {
    char *ext_tname = ob_get_extended_thread_name();
    const char *tname = ob_get_tname();
    if (nullptr != ext_tname && nullptr != tname) {
      IGNORE_RETURN databuff_printf(ext_tname, OB_EXTENED_THREAD_NAME_BUF_LEN,
          "%s_%s", tname, token->timer_name_);
    }
  }
}

void ObTimerTaskThreadPool::clear_ext_tname()
{
  char *ext_tname = ob_get_extended_thread_name();
  if (nullptr != ext_tname) {
    ext_tname[0] = '\0';
  }
}

ObTimerService::ObTimerService(uint64_t tenant_id /* = OB_SERVER_TENANT_ID */)
  : is_never_started_(true),
    is_stopped_(true),
    tenant_id_(tenant_id),
    token_alloc_(),
    priority_task_queue_(64L, nullptr, ObMemAttr(tenant_id_, "ts_queue")),
    running_task_set_(64L, nullptr, ObMemAttr(tenant_id_, "ts_run_set")),
    uncanceled_task_set_(64L, nullptr, ObMemAttr(tenant_id_, "ts_cancel_set")),
    worker_thread_pool_(*this)
{
  token_alloc_.set_attr(ObMemAttr(tenant_id, "ts_token"));
}

ObTimerService::~ObTimerService()
{
  priority_task_queue_.reset();
  running_task_set_.reset();
  uncanceled_task_set_.reset();
}

int ObTimerService::start()
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard mutex_guard(mutex_);
  ObMonitor<Mutex>::Lock guard(monitor_);
  if (is_stopped_) {
    worker_thread_pool_.set_run_wrapper(get_tenant_tg_helper());
    set_run_wrapper(get_tenant_tg_helper());
    is_stopped_ = false;
    const int64_t reserve_size = INITIAL_ELEMENT_NUM * sizeof(TaskToken *);
    if (OB_FAIL(priority_task_queue_.reserve(reserve_size))) {
      OB_LOG(WARN, "reserve priority_task_queue failed", K(reserve_size), K_(tenant_id), K(ret));
    } else if (OB_FAIL(running_task_set_.reserve(reserve_size))) {
      OB_LOG(WARN, "reserve running_task_set failed", K(reserve_size), K_(tenant_id), K(ret));
    } else if (OB_FAIL(uncanceled_task_set_.reserve(reserve_size))) {
      OB_LOG(WARN, "reserve uncanceled_task_set failed", K(reserve_size), K_(tenant_id), K(ret));
    } else if (OB_FAIL(token_alloc_.reserve(INITIAL_ELEMENT_NUM))) {
      OB_LOG(WARN, "reserve token_alloc failed", K_(tenant_id), K(ret));
    } else if (OB_FAIL(worker_thread_pool_.init(
        MIN_WORKER_THREAD_NUM, TASK_NUM_LIMIT, "TimerWK", tenant_id_))) {
      OB_LOG(WARN, "init ObTimerTaskThreadPool failed", K_(tenant_id), K(ret));
    } else if (OB_FAIL(worker_thread_pool_.set_adaptive_thread(
        MIN_WORKER_THREAD_NUM, MAX_WORKER_THREAD_NUM))) {
      OB_LOG(WARN, "set adaptive thread failed", K_(tenant_id), K(ret));
    } else if (OB_FAIL(ThreadPool::start())) {
      OB_LOG(WARN, "failed to start ObTimerService thread", K(ret));
    } else {
      is_never_started_ = false;
      monitor_.notify_all();
      OB_LOG(INFO, "ObTimerService start success", K_(tenant_id), KP(this), KCSTRING(lbt()));
    }
    if (OB_FAIL(ret)) {
      is_stopped_ = true;
    }
  }
  return ret;
}

void ObTimerService::stop()
{
  {
    ObMonitor<Mutex>::Lock guard(monitor_);
    // STEP1: set stop flag
    is_stopped_ = true;
    // STEP2: cancel tasks that haven't run yet
    for (int32_t i = 0; i < priority_task_queue_.size(); ++i) {
      TaskToken *token = priority_task_queue_.at(i);
      delete_token(token);
    }
    priority_task_queue_.clear();
    // STEP3: cancel the running tasks
    int ret_tmp = OB_SUCCESS;
    VecIter it = nullptr;
    for (int32_t i = 0; i < running_task_set_.size(); ++i) {
      TaskToken *token = running_task_set_.at(i);
      // if someone fails, still continue
      if (OB_SUCCESS != (ret_tmp = uncanceled_task_set_.insert_unique(
          token, it, compare_for_set, token_unique))) {
        OB_LOG_RET(WARN, ret_tmp, "insert TaskToken into uncanceled_task_set failed",
            KP(token), KPC(token), K_(tenant_id));
        // the token cannot be deleted because the corresponding task is running
      }
    }
    running_task_set_.clear();
    // STEP4: notify_all
    monitor_.notify_all();
    // STEP5: wait running tasks done
    while (priority_task_queue_.size() > 0
        || running_task_set_.size() > 0
        || uncanceled_task_set_.size() > 0) {
      (void)monitor_.timed_wait(ObSysTime(MIN_WAIT_INTERVAL));
    }
  }
  // STEP6: stop worker threads and the scheduling thread
  worker_thread_pool_.stop();
  ThreadPool::stop();
  OB_LOG(INFO, "ObTimerService stop success", K_(tenant_id), KP(this));
}

void ObTimerService::wait()
{
  worker_thread_pool_.wait();
  ThreadPool::wait();
  OB_LOG(INFO, "ObTimerService wait success", K_(tenant_id), KP(this));
}

void ObTimerService::destroy()
{
  worker_thread_pool_.destroy();
  ThreadPool::destroy();
  OB_LOG(INFO, "ObTimerService destroyed", K_(tenant_id), KP(this));
}

int ObTimerService::schedule_task(
      const ObTimer *timer,
      ObTimerTask &task,
      const int64_t delay,
      const bool repeate /* = false */,
      const bool immediately /* = false */)
{
  int ret = OB_SUCCESS;
  ObMonitor<Mutex>::Lock guard(monitor_);
  if (nullptr == timer) {
    ret = OB_ERR_NULL_VALUE;
    OB_LOG(WARN, "timer is NULL", K_(tenant_id), K(ret));
  } else if (delay < 0) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(delay), K_(tenant_id), K(ret));
  } else {
    int64_t time = ObSysTime::now().toMicroSeconds();
    TaskToken *token = nullptr;
    if (OB_FAIL(new_token(
        token,
        timer,
        &task,
        immediately ? time : time + delay,
        repeate ? delay : 0))) {
      OB_LOG(WARN, "new token failed", K_(tenant_id), K(ret));
    } else {
      VecIter it;
      if (OB_SUCC(priority_task_queue_.insert_unique(token, it, compare_for_queue, token_unique))) {
        monitor_.notify();
      } else {
        delete_token(token);
        OB_LOG(WARN, "insert TaskToken failed", K(task), K_(tenant_id), K(ret));
      }
    }
  }
  return ret;
}

int ObTimerService::cancel_task(const ObTimer *timer, const ObTimerTask *task)
{
  int ret = OB_SUCCESS;
  ObMonitor<Mutex>::Lock guard(monitor_);

  if (nullptr == timer) {
    ret = OB_ERR_NULL_VALUE;
    OB_LOG(WARN, "timer is NULL", K_(tenant_id), K(ret));
  }

  // remove TaskToken from priority_task_queue_
  if (OB_SUCC(ret)) {
    VecIter it = priority_task_queue_.begin();
    while(it != priority_task_queue_.end() && OB_SUCC(ret)) {
      TaskToken *token = *it;
      if (has_same_task_and_timer(token, timer, task)) {
        if (OB_FAIL(priority_task_queue_.remove(it))) {
          OB_LOG(WARN, "remove TaskToken from priority_task_queue failed",
              K(token), K_(tenant_id), K(ret));
        } else {
          delete_token(token);
        }
      } else {
        ++it;
      }
    }
  }

  // move TaskToken from running_task_set_ to uncanceled_task_set_
  if (OB_SUCC(ret)) {
    TaskToken target(timer, const_cast<ObTimerTask *>(task));
    VecIter it = std::lower_bound(
        running_task_set_.begin(),
        running_task_set_.end(),
        &target,
        compare_for_set);
    bool found = true;
    VecIter pos = uncanceled_task_set_.end();
    while(it != running_task_set_.end() && found && OB_SUCC(ret)) {
      if (has_same_task_and_timer(*it, timer, task)) {
        if (OB_FAIL(uncanceled_task_set_.insert_unique(*it, pos, compare_for_set, token_unique))) {
          OB_LOG(WARN, "insert TaskToken failed", KPC(task), K_(tenant_id), K(ret));
        } else if (OB_FAIL(running_task_set_.remove(it))) {
          OB_LOG(WARN, "remove TaskToken from running_task_set failed",
              K(task), K_(tenant_id), K(ret));
        } else {}
      } else {
        found = false;
      }
    }
  }

  return ret;
}

int ObTimerService::wait_task(const ObTimer *timer, const ObTimerTask *task)
{
  int ret = OB_SUCCESS;
  ObMonitor<Mutex>::Lock guard(monitor_);
  if (nullptr == timer) {
    ret = OB_ERR_NULL_VALUE;
    OB_LOG(WARN, "timer is NULL", K_(tenant_id), K(ret));
  } else {
    do {
      bool exist = false;
      for (int32_t idx = 0; idx < priority_task_queue_.size() && !exist; ++idx) {
        TaskToken *token = priority_task_queue_.at(idx);
        exist = has_same_task_and_timer(token, timer, task);
      }
      if (!exist) {
        exist = find_task_in_set(running_task_set_, timer, task);
      }
      if (!exist) {
        exist = find_task_in_set(uncanceled_task_set_, timer, task);
      }
      if (!exist) {
        break;
      } else {
        IGNORE_RETURN monitor_.timed_wait(ObSysTime(MIN_WAIT_INTERVAL));
      }
    } while (true);
  }

  return ret;
}

int ObTimerService::schedule_task(TaskToken *token)
{
  int ret = OB_SUCCESS;
  ObMonitor<Mutex>::Lock guard(monitor_);
  if (nullptr == token) {
    ret = OB_ERR_NULL_VALUE;
    OB_LOG(WARN, "TaskToken is NULL", K(ret));
  } else {
    VecIter it = uncanceled_task_set_.end();
    int cancel_ret = uncanceled_task_set_.find(token, it, compare_for_set);
    if (OB_SUCCESS != cancel_ret && OB_ENTRY_NOT_EXIST != cancel_ret) { // unexpected
      ret = cancel_ret;
      OB_LOG(WARN, "check if TaskToken exist in uncanceled_task_set failed",
          KP(token), KPC(token), K_(tenant_id), K(ret));
    } else if (OB_SUCCESS == cancel_ret) { // has canceled
      if (OB_FAIL(uncanceled_task_set_.remove(it))) {
        OB_LOG(WARN, "remove TaskToken from uncanceled_task_set failed",
            KP(token), KPC(token), K_(tenant_id), K(ret));
      } else {
        delete_token(token);
      }
    } else if (OB_ENTRY_NOT_EXIST == cancel_ret) { // find token in running_task_set_
      it = running_task_set_.end();
      int run_ret = running_task_set_.find(token, it, compare_for_set);
      if (OB_SUCCESS != run_ret && OB_ENTRY_NOT_EXIST != run_ret) {
        ret = run_ret;
        OB_LOG(WARN, "check if Taskoken exist in running_task_set failed",
            KP(token), KPC(token), K_(tenant_id), K(ret));
      } else if (OB_ENTRY_NOT_EXIST == run_ret) {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "not find TaskToken both in uncanceled_task_set and running_task_set",
            KP(token), KPC(token), K_(tenant_id), K(ret));
      } else if (OB_SUCCESS == run_ret) {
        if (OB_FAIL(running_task_set_.remove(it))) {
          OB_LOG(WARN, "erase TaskToken from running_task_set failed",
              KP(token), KPC(token), K_(tenant_id), K(ret));
        } else {
          if (0 == token->delay_ || is_stopped_) { // no need re-schedule
            delete_token(token);
          } else { // re-schedule
            int64_t time = ObSysTime::now().toMicroSeconds();
            token->scheduled_time_ = time + token->delay_;
            token->last_try_pop_time_ = 0L;
            token->pushed_time_ = 0L;
            VecIter pos;
            if (OB_FAIL(
                priority_task_queue_.insert_unique(token, pos, compare_for_queue, token_unique))) {
              OB_LOG(WARN, "push TaskToken into priority_task_queue failed",
                  KP(token), KPC(token), K_(tenant_id), K(ret));
              delete_token(token);
            } else {}
          }
        }
      } else {}
    } else {}
  }
  monitor_.notify_all();
  return ret;
}

bool ObTimerService::task_exist(const ObTimer *timer, const ObTimerTask &task)
{
  bool exist = false;
  ObMonitor<Mutex>::Lock guard(monitor_);
  for (int32_t idx = 0; idx < priority_task_queue_.size() && !exist; ++idx) {
    TaskToken *token = priority_task_queue_.at(idx);
    exist = has_same_task_and_timer(token, timer, &task);
  }
  return exist;
}

int ObTimerService::mtl_new(ObTimerService *&timer_service)
{
  int ret = OB_SUCCESS;
  timer_service = OB_NEW(ObTimerService, ObMemAttr(mtl_get_id(), "timer_service"), mtl_get_id());
  if (nullptr == timer_service) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(WARN, "failed to alloc tenant session manager", K(ret));
  }
  return ret;
}

int ObTimerService::mtl_start(ObTimerService *&timer_service)
{
  int ret = OB_SUCCESS;
  if (nullptr == timer_service) {
    ret = OB_ERR_NULL_VALUE;
    OB_LOG(WARN, "timer service is NULL", K(ret));
  } else if (OB_FAIL(timer_service->start())) {
    OB_LOG(WARN, "failed to start timer service", K(ret));
  } else {}
  return ret;
}

void ObTimerService::mtl_stop(ObTimerService *&timer_service)
{
  if (nullptr != timer_service) {
    timer_service->stop();
    OB_LOG(INFO, "success to stop timer service");
  }
}

void ObTimerService::mtl_wait(ObTimerService *&timer_service)
{
  if (nullptr != timer_service) {
    timer_service->wait();
    OB_LOG(INFO, "success to wait timer service");
  }
}

void ObTimerService::mtl_destroy(ObTimerService *&timer_service)
{
  if (nullptr != timer_service) {
    timer_service->destroy();
    OB_LOG(INFO, "success to wait timer service");
    OB_DELETE(ObTimerService, "timer_service", timer_service);
    timer_service = nullptr;
  }
}

bool ObTimerService::has_running_task(const ObTimer *timer, const TaskToken *&running_task_token) const
{
  bool found = false;
  if (nullptr != timer) {
    found = find_task_in_set(running_task_set_, timer, nullptr, &running_task_token);
    if (!found) {
      found = find_task_in_set(uncanceled_task_set_, timer, nullptr, &running_task_token);
    }
  }
  return found;
}

int ObTimerService::pop_task(int64_t now, TaskToken *&task_token, int64_t &st)
{
  int ret = OB_SUCCESS;
  VecIter it = priority_task_queue_.begin();
  bool need_continue = true;
  while(it != priority_task_queue_.end() && OB_SUCC(ret) && need_continue) {
    TaskToken *token = *it;
    if (nullptr == token) {
      ret = OB_ERR_NULL_VALUE;
      OB_LOG(WARN, "TaskToken is NULL", K_(tenant_id), K(ret));
    } else if (nullptr == token->timer_) {
      ret = OB_ERR_NULL_VALUE;
      OB_LOG(WARN, "ObTimer is NULL", KP(token), KPC(token), K_(tenant_id), K(ret));
    } else if (token->scheduled_time_ > now) {
      st = token->scheduled_time_;
      need_continue = false;
    } else {
      const TaskToken *running_task_token = nullptr;
      if (has_running_task(token->timer_, running_task_token)) {
        ++it;
        const int64_t now = ObSysTime::now().toMicroSeconds();
        const int64_t total_delay = now - token->scheduled_time_;
        bool should_alarm = (total_delay > DELAY_IN_PRI_QUEUE_THREASHOLD) 
            && (now - token->last_try_pop_time_ > ALARM_INTERVAL);
        if (should_alarm) {
          const int64_t threashold = DELAY_IN_PRI_QUEUE_THREASHOLD;
          OB_LOG_RET(WARN, OB_ERR_TOO_MUCH_TIME, 
              "timer task too much delay because the same timer has another running task",
              KPC(token), KPC(running_task_token), K(total_delay), K(threashold), K_(tenant_id));
          token->last_try_pop_time_ = now;
        }
      } else {
        if (OB_FAIL(priority_task_queue_.remove(it))) {
          OB_LOG(WARN, "remove TaskToken from priority_task_queue failed", K_(tenant_id), K(ret));
        } else {
          task_token = token;
          need_continue = false;
        }
      }
    }
  }
  return ret;
}

void ObTimerService::run1()
{
  int64_t thread_id = GETTID();
  set_thread_name("TimerSvr");
  const char *module_name = ob_get_tname() != nullptr ? ob_get_tname() : "DefaultTimer";
  ObDIActionGuard("TimerThreadPool", module_name, nullptr);
  OB_LOG(INFO, "TimerService thread started",
      KP(this), K(thread_id), K_(tenant_id), KCSTRING(lbt()));

  while(true) {
    IGNORE_RETURN lib::Thread::update_loop_ts();
    {  // lock
      ObMonitor<Mutex>::Lock guard(monitor_);

      while(!is_stopped_ && 0 == priority_task_queue_.size()) {
        ObBKGDSessInActiveGuard inactive_guard;
        monitor_.wait();
      }
      if (is_stopped_) {
        break;
      }
      while(priority_task_queue_.size() > 0 && !is_stopped_) {
        if (REACH_TIME_INTERVAL(DUMP_INTERVAL)) {
          dump_info();
        }
        const int64_t now = ObSysTime::now().toMicroSeconds();
        TaskToken *first_token = priority_task_queue_.at(0);
        abort_unless(nullptr != first_token);
        if (first_token->scheduled_time_ <= now) {
          int ret = OB_SUCCESS;
          TaskToken *token = nullptr;
          int64_t st = 0L;
          if (OB_FAIL(pop_task(now, token, st))) {
            OB_LOG(WARN, "pop TaskToken from priority_task_queue failed", K_(tenant_id), K(ret));
          } else if (nullptr == token) {
            // wait for a schedulable task
            int64_t wait_time = st - now;
            wait_time = MIN(wait_time, MAX_WAIT_INTERVAL);
            wait_time = MAX(wait_time, MIN_WAIT_INTERVAL);
            ObBKGDSessInActiveGuard inactive_guard;
            monitor_.timed_wait(ObSysTime(wait_time));
          } else {
            VecIter it = nullptr;
            token->pushed_time_ = ObSysTime::now().toMicroSeconds();
            if (OB_FAIL(running_task_set_.insert_unique(token, it, compare_for_set, token_unique))) {
              OB_LOG(WARN, "push TaskToken into running_task_set failed",
                  KP(token), KPC(token), K_(tenant_id), K(ret));
              delete_token(token);
            } else if (OB_FAIL(worker_thread_pool_.push(reinterpret_cast<void *>(token)))) {
              OB_LOG(WARN, "push TaskToken into thread pool failed",
                  KP(token), KPC(token), K_(tenant_id), K(ret));
              if (OB_FAIL(running_task_set_.remove(it))) {
                OB_LOG(WARN, "erase TaskToken from running_task_set failed",
                    KP(token), KPC(token), K_(tenant_id), K(ret));
              } else {
                delete_token(token);
              }
            } else {}
          }
        } else {
          check_clock();
          ObBKGDSessInActiveGuard inactive_guard;
          monitor_.timed_wait(ObSysTime(first_token->scheduled_time_ - now));
        }
      }
    }  // unlock
  }
  OB_LOG(INFO, "TimerService thread exit", KP(this), K(thread_id), K_(tenant_id));
}

bool ObTimerService::has_same_task_and_timer(
    const TaskToken *token,
    const ObTimer *timer,
    const ObTimerTask *task)
{
  abort_unless(nullptr != token);
  abort_unless(nullptr != timer);
  abort_unless(nullptr != token->timer_);
  bool same_task = true;
  if (nullptr != task) {
    same_task = token->task_ == task;
  }
  bool same_timer = token->timer_ == timer;
  return same_task && same_timer;
}

bool ObTimerService::find_task_in_set(
      const ObSortedVector<TaskToken *> &token_set,
      const ObTimer *timer,
      const ObTimerTask *task_in,
      const TaskToken **token_out) const
{
  abort_unless(nullptr != timer);
  bool exist = false;
  TaskToken target(timer, const_cast<ObTimerTask *>(task_in));
  VecIter it = std::lower_bound(
      token_set.begin(),
      token_set.end(),
      &target,
      compare_for_set);
  if (it != token_set.end()) {
    exist = has_same_task_and_timer(*it, timer, task_in);
    if (exist && nullptr != token_out) {
      *token_out = *it;
    }
  }
  return exist;
}

void ObTimerService::check_clock()
{
  // clock safty check. @see 
  const int64_t rt1 = ObTimeUtility::current_time();
  const int64_t rt2 = ObTimeUtility::current_time_coarse();
  const int64_t delta = rt1 > rt2 ? (rt1 - rt2) : (rt2 - rt1);
  if (delta > CLOCK_SKEW_DELTA) {
    OB_LOG_RET(WARN, OB_ERR_SYS, "Hardware clock skew", K(rt1), K(rt2));
  } else if (delta > CLOCK_ERROR_DELTA) {
    OB_LOG_RET(ERROR, OB_ERR_SYS, "Hardware clock error", K(rt1), K(rt2));
  }
}

int ObTimerService::new_token(
    TaskToken *&token,
    const ObTimer *timer,
    ObTimerTask *task,
    const int64_t st,
    const int64_t dt)
{
  int ret = OB_SUCCESS;
  token = token_alloc_.alloc(timer, task, st, dt);
  if (nullptr == token)
  {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(WARN, "alloc TaskToken failed", K_(tenant_id), K(ret));
  }
  return ret;
}

void ObTimerService::delete_token(TaskToken *&token)
{
  token_alloc_.free(token);
}

void ObTimerService::dump_info()
{
  OB_LOG(INFO, "dump info [summary]",
      KP(this), KPC(this));
  for (int idx = 0; idx < priority_task_queue_.size(); ++idx) {
    TaskToken *token = priority_task_queue_.at(idx);
    OB_LOG(INFO, "dump queue token", KP(this), KPC(token));
  }
}

}
}
