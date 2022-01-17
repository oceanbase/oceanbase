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

#ifndef CO_BASE_SCHED_H
#define CO_BASE_SCHED_H

#include "lib/coro/co_thread.h"
#include "lib/coro/co_timer_queue.h"
#include "lib/list/ob_dlist.h"
#include "lib/async/event_base.h"

namespace oceanbase {
namespace lib {

// CoBaseSched implements a fundamental co-routine scheduler. It use
// linked lists to store all routines and runnable routines. It always
// get routine to run which has the longest sojourn time, e.g. act as
// a FIFO queue.
//
// Other scheduler can inherit this class to implement complicated
// logic.
//
// @see on_born, on_pause, on_wakeup, on_finished, add_runnable,
//      get_next_runnable
//
class CoBaseSched : public CoThread, public EventBase {
  using RunFuncT = std::function<void()>;
  using ExitCBFuncT = std::function<void()>;

public:
  class Worker;
  class Task;

public:
  CoBaseSched();
  virtual ~CoBaseSched();

  // overwrite
  int start();

  void destroy();

  /// \brief Keep scheduler alive even thought no routine exists.
  void keep_alive();

  /// submit task to this scheduler
  int submit(RunFuncT&& func);
  int submit(const RunFuncT& func);
  static int add_exit_cb(const ExitCBFuncT& func);

protected:
  using TaskList = common::ObDList<Task>;
  using List = common::ObDList<Worker>;
  using LinkedNode = common::ObDLinkNode<Worker*>;
  using LinkedList = common::ObDList<LinkedNode>;
  using TimerQueue = CoTimerQueue;

  // before run schedule
  virtual int prepare() = 0;
  // after run schedule
  virtual void postrun() = 0;
  // schedule main body
  virtual void run() final;

  // Called when a new worker has been created and is ready to run.
  virtual void on_born(Worker& w);
  // Called when a worker should be paused i.e. sleep, yield.
  virtual void on_pause(Worker& w);
  // Called when woken up by other worker. The worker who wakes up
  // this worker may or may not be in the same thread as which this
  // worker in.
  virtual void on_wakeup(Worker& w);
  // Called when a worker has finished its task and needs exit.
  virtual void on_finished(Worker& w);
  // Called when a new runnable worker should be added.
  virtual void add_runnable(Worker& w);
  // Called when a scheduler wants a worker to run.
  virtual CoRoutine* get_next_runnable(int64_t& waittime);

  void start_timer(CoTimer& timer) override;
  void cancel_timer(CoTimer& timer) override;
  void store_loop_time();

protected:
  // set if scheduler won't exit unless other stop it.
  bool keep_alive_;
  // tasks others submit to this scheduler.
  TaskList tasks_;

protected:
  List routines_;
  List finished_routines_;
  LinkedList runnables_;
  CoSpinLock rq_lock_;
  TimerQueue timerq_;
  int64_t loop_time_;
  static constexpr int MAX_EXIT_CB_NUM = 16;
  static ExitCBFuncT exit_cbs_[MAX_EXIT_CB_NUM];
  static int exit_cb_num_;
};

class CoBaseSched::Worker : public CoRoutine, public common::ObDLinkBase<Worker> {
  friend class CoBaseSched;
  using LinkedNode = CoBaseSched::LinkedNode;
  using LinkedList = CoBaseSched::LinkedList;

public:
  Worker(CoBaseSched& sched, Task* task = nullptr) : CoRoutine(sched), sched_(sched), node_(), task_(task)
  {
    node_.get_data() = this;
  }
  virtual ~Worker();

  void usleep(uint32_t usec) override;
  void sleep_until(int64_t abs_time) override;
  void add_to_runnable(LinkedList& list);

protected:
  CoBaseSched& get_sched()
  {
    return sched_;
  }

private:
  virtual void on_status_change(RunStatus prev, RunStatus curr) final;

private:
  CoBaseSched& sched_;
  LinkedNode node_;  // runnable node.
  Task* task_;
};

class CoBaseSched::Task : public common::ObDLinkBase<Task> {
public:
  Task(const RunFuncT& func) : func_(func)
  {}

public:
  RunFuncT func_;
};

OB_INLINE void CoBaseSched::keep_alive()
{
  keep_alive_ = true;
}

OB_INLINE void CoBaseSched::store_loop_time()
{
  loop_time_ = co_current_time();
}

OB_INLINE void CoBaseSched::Worker::add_to_runnable(LinkedList& list)
{
  list.add_last(&node_);
}

OB_INLINE void CoBaseSched::destroy()
{
  CoThread::destroy();
  EventBase::destroy();
}

OB_INLINE CoBaseSched::Worker::~Worker()
{
  if (task_ != nullptr) {
    task_->~Task();
    task_ = nullptr;
  }
}

}  // namespace lib
}  // namespace oceanbase

#endif /* CO_BASE_SCHED_H */
