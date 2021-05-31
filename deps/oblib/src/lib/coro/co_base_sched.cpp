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

#include "co_base_sched.h"
#include "lib/coro/co.h"
#include "lib/coro/co_timer.h"
#include "lib/allocator/ob_malloc.h"

using namespace oceanbase::lib;
using namespace oceanbase::common;

namespace oceanbase {
namespace lib {

int CoBaseSched::exit_cb_num_ = 0;
CoBaseSched::ExitCBFuncT CoBaseSched::exit_cbs_[MAX_EXIT_CB_NUM];

int CoBaseSched::add_exit_cb(const ExitCBFuncT& func)
{
  int ret = OB_SUCCESS;
  do {
    int num = ATOMIC_LOAD(&exit_cb_num_);
    if (MAX_EXIT_CB_NUM == num) {
      ret = OB_SIZE_OVERFLOW;
    } else if (!ATOMIC_BCAS(&exit_cb_num_, num, num + 1)) {
      PAUSE();
    } else {
      exit_cbs_[num] = func;
      break;
    }
  } while (OB_SUCC(ret));
  return ret;
}

CoBaseSched::CoBaseSched() : keep_alive_(false), loop_time_()
{}

CoBaseSched::~CoBaseSched()
{}

void CoBaseSched::run()
{
  // prepare workers
  instance_ = nullptr;
  prepare();
  instance_ = this;

  // check runnable workers.
  while (true) {
    store_loop_time();
    int64_t waittime = 0;
    rq_lock_.lock();
    const auto w = get_next_runnable(waittime);
    // active_routine_ must be set within rq_lock_ to prevent other
    // thread try to manipulate this routine, e.g. add running worker
    // into runnable queue when being woken up, whereas it's running
    // normally.
    if (w != nullptr) {
      active_routine_ = w;
    }

    TaskList the_tasks;
    tasks_.move(the_tasks);
    rq_lock_.unlock();

    // Create routines which holds responding tasks, and add them to
    // runnable queue.
    int ret = OB_SUCCESS;
    bool has_new_routine = false;
    DLIST_FOREACH_REMOVESAFE(curr, the_tasks)
    {
      if (OB_SUCC(create_routine(curr->func_))) {
        has_new_routine = true;
        the_tasks.remove(curr);
        common::ob_free(curr);
      } else {
        break;
      }
    }

    if (OB_UNLIKELY(!the_tasks.is_empty())) {
      rq_lock_.lock();
      tasks_.push_range(the_tasks);
      rq_lock_.unlock();
    }

    if (w != nullptr) {
      resume_routine(*w);
    } else if (!has_new_routine) {
      wait4event(waittime);
    }
    // Exit schedule if all routines have finished and one of followings
    //
    // 1. Not in KeepAlive mode.
    // 2. In KeepAlive mode and has been stopped.
    if (routines_.get_size() == 0 && (!keep_alive_ || (keep_alive_ && has_set_stop()))) {
      break;
    }
  }
  // recycle resources
  postrun();
}

CoRoutine* CoBaseSched::get_next_runnable(int64_t& waittime)
{
  Worker* w = nullptr;
  waittime = 0;

  int64_t next_timer_expires = 0L;
  // Moving overtime routine into runnable queue if exist.
  if (auto node = timerq_.get_next()) {
    // Acquire runnable queue to prevent others modify it, e.g., wake
    // routine up from other thread.
    while (node != nullptr) {
      auto timer = static_cast<CoTimerSleeper*>(node);
      if (timer->expires_ <= loop_time_) {
        auto worker = static_cast<Worker*>(timer->routine_);
        timer->routine_ = nullptr;  // set nullptr indicates timeout
        // Put routine into runnable queue only if it hasn't in the queue
        // yet.
        if (worker->node_.get_prev() == nullptr) {
          add_runnable(*worker);
        }
        timerq_.del(node);
      } else {
        next_timer_expires = timer->expires_;
        break;
      }
      node = timerq_.get_next();
    }
  }
  // Check whether there's runnable routine we can schedule. If
  // there's no runnable routine, we should calculate maximum time
  // need wait for next schedule. Here the maximum means thread would
  // sleep, wait IO events, that time except other thread notify it.
  if (runnables_.get_size() > 0) {
    w = runnables_.remove_first()->get_data();
  } else if (runnables_.get_size() == 0) {
    waittime = next_timer_expires - loop_time_;
    if (waittime <= 0) {
      waittime = 1000 * 1000L;
    }
  }
  return w;
}

int CoBaseSched::start()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(EventBase::init())) {
    OB_LOG(ERROR, "event init failed", K(ret), K(errno));
  } else if (OB_FAIL(CoThread::start())) {
  } else {
  }
  return ret;
}

void CoBaseSched::on_born(Worker& w)
{
  rq_lock_.lock();
  routines_.add_last(&w);
  add_runnable(w);
  rq_lock_.unlock();
}

void CoBaseSched::on_pause(Worker& w)
{
  rq_lock_.lock();
  add_runnable(w);
  rq_lock_.unlock();
}

void CoBaseSched::on_wakeup(Worker& w)
{
  // There's Other worker waking up a waiting worker, or in other case
  // a newborn worker has been wake up but we just ignore this
  // case. If the waking up worker and the woken up are in the same
  // thread, we can simply put it into runnable queue. But if they in
  // different thread, it's necessary to deal it gingerly.
  if (CO_IS_ENABLED() && CO_TIDX() == w.get_tidx()) {
    rq_lock_.lock();
    // Same thread since their Thread Indexes are equal
    //
    // If the worker already in ready list or runnable list, we do
    // nothing.
    if (w.node_.get_prev() == nullptr && co_current() != w  // In case of self waking up.
    ) {
      add_runnable(w);
    }
    rq_lock_.unlock();
  } else {
    // FIXME: Move signal inside.
    // Different threads
    rq_lock_.lock();
    // Put routine into runnable queue only if it hasn't in the queue
    // yet.
    if (w.node_.get_prev() == nullptr && co_current() != w  // In case of being woken up while running.
    ) {
      add_runnable(w);
    }
    rq_lock_.unlock();
  }
  signal();
}

void CoBaseSched::on_finished(Worker& w)
{
  routines_.remove(&w);
  finished_routines_.add_last(&w);
  active_routine_ = instance_;
}

void CoBaseSched::add_runnable(Worker& w)
{
  w.add_to_runnable(runnables_);
}

void CoBaseSched::Worker::on_status_change(RunStatus prev, RunStatus curr)
{
  auto& sched = sched_;
  if (prev == RunStatus::BORN && curr == RunStatus::RUNNABLE) {
    sched.on_born(*this);
  } else if (prev == RunStatus::RUNNING && curr == RunStatus::RUNNABLE) {
    sched.on_pause(*this);
  } else if (prev == RunStatus::WAITING && curr == RunStatus::RUNNABLE) {
    sched.on_wakeup(*this);
  } else if (curr == RunStatus::FINISHED) {
    sched.on_finished(*this);
  }
}

void CoBaseSched::Worker::usleep(uint32_t usec)
{
  sleep_until(co_current_time() + usec);
}

void CoBaseSched::Worker::sleep_until(int64_t abs_time)
{
  CoTimerSleeper t{*this, abs_time};
  sched_.start_timer(t);
  sched_.reschedule();
}

void CoBaseSched::start_timer(CoTimer& timer)
{
  timerq_.add(&timer);
}

void CoBaseSched::cancel_timer(CoTimer& timer)
{
  timerq_.del(&timer);
}

int CoBaseSched::submit(RunFuncT&& func)
{
  int ret = OB_SUCCESS;
  Task* t = (Task*)common::ob_malloc(sizeof(Task), ObModIds::OB_CORO);
  if (t != nullptr) {
    new (t) Task(func);
  }
  if (OB_ISNULL(t)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    rq_lock_.lock();
    tasks_.add_last(t);
    rq_lock_.unlock();
    signal();
  }
  return ret;
}

int CoBaseSched::submit(const RunFuncT& func)
{
  int ret = OB_SUCCESS;
  Task* t = (Task*)common::ob_malloc(sizeof(Task), ObModIds::OB_CORO);
  if (t != nullptr) {
    new (t) Task(func);
  }
  if (OB_ISNULL(t)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    rq_lock_.lock();
    tasks_.add_last(t);
    rq_lock_.unlock();
    signal();
  }
  return ret;
}

}  // namespace lib
}  // namespace oceanbase
