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

#ifndef CO_SET_SCHED_H
#define CO_SET_SCHED_H

// This file defines a common and powerful scheduler which supports
// priority and CPU usage isolation. This scheduler separates schedule
// function into two parts, one for routines in one group which named
// SET and another for scheduling between all SETs. Schedule between
// SETs is called 1-level schedule, and schedule in one SET called
// 2-level schedule.
//
// For 1-level scheduling, between SETs, it would call each SET to get
// next runnable routine base on their rank of each SET. SET with
// higher rank would gain higher possibility to be scheduled than
// lower ones. Currently, the range of rank is [1~10], 1 is the lowest
// rank and 10 is the highest.
//
// For 2-level scheduling, in given SET, it's the responsibility for
// the SET to choose suitable routine to run. The SET can also return
// no routine because of its CPU limitation or even has no routines at
// all. Each SET may or may not have different logic.

#include <functional>
#include "lib/coro/co_base_sched.h"
#include "lib/allocator/ob_allocator.h"

EXTERN_C_BEGIN
extern void* __libc_malloc(size_t size);
#ifndef __OB_C_LIBC_FREE__
#define __OB_C_LIBC_FREE__
extern void __libc_free(void* ptr);
#endif
EXTERN_C_END

namespace oceanbase {
namespace lib {

using ObIAllocator = common::ObIAllocator;

class CoSetSched : public CoBaseSched {
  static constexpr int MAX_SET_CNT = 32;

public:
  using PrepFuncT = std::function<int()>;
  using PostFuncT = std::function<void()>;
  using RunFuncT = std::function<void()>;

  // Nested classes.
  class Worker;
  class Set;

  CoSetSched(PrepFuncT prep = nullptr, PostFuncT post = nullptr);

  // Create a set with ID of setid.
  template <class SET = Set>
  int create_set(int setid);
  // Create worker in given set.
  template <class WorkerT = Worker>
  int create_worker(int setid, RunFuncT runnable);
  template <class WorkerT = Worker>
  int create_worker(ObIAllocator& allocator, int setid, RunFuncT runnable);
  template <class WorkerT = Worker>
  int create_worker(ObIAllocator& allocator, int setid, RunFuncT runnable, Worker*& worker);

  // statistics
  /// \brief Get number of context switch occurred on this scheduler.
  ///
  /// \return Number of context switch
  uint64_t get_cs() const
  {
    return cs_;
  }

  int create_routine(RoutineFunc start) override;

protected:
  int prepare() override;
  void postrun() override;

private:
  void add_runnable(CoBaseSched::Worker& w) override;
  CoRoutine* get_next_runnable(int64_t& waittime) override;
  void remove_finished_workers();

  void* allocator_alloc(ObIAllocator& allocator, size_t size);

private:
  using SetArr = Set* [MAX_SET_CNT];

  SetArr sets_;
  int max_setid_;
  PrepFuncT prep_;
  PostFuncT post_;

  uint64_t cs_;  // context switch
};

class CoSetSched::Worker : public CoBaseSched::Worker {
  friend class CoSetSched;
  using FuncT = std::function<void()>;

public:
  Worker(CoSetSched& sched, FuncT func = nullptr, ObIAllocator* allocator = nullptr);

  int& get_setid();
  ObIAllocator* get_allocator() const;

  void check();

private:
  void run() override;

private:
  int setid_;
  bool owned_;  // True if we're responsible delete it.
  FuncT func_;
  ObIAllocator* const allocator_;
  CoSetSched& sched_;
};

class CoSetSched::Set {
public:
  /// \note \c Set is allocated in local storage and the dctor won't
  /// be called.
  Set();

  int& get_setid();

  // Add worker into this set.
  virtual void add_worker(Worker& worker);
  virtual Worker* get_next_runnable(int64_t& waittime);

protected:
  using LinkedList = CoSetSched::LinkedList;

private:
  int setid_;
  LinkedList runnables_;
};

template <class SET>
int CoSetSched::create_set(int setid)
{
  int ret = common::OB_SUCCESS;
  if (setid >= MAX_SET_CNT || setid < 0) {
    ret = common::OB_INVALID_ARGUMENT;
  } else if (sets_[setid] != nullptr) {
    ret = common::OB_ENTRY_EXIST;
  } else {
    SET* set = nullptr;
    void* buf = __libc_malloc(sizeof(SET));
    if (buf != nullptr) {
      set = new (buf) SET();
      set->get_setid() = setid;
      sets_[setid] = set;
      max_setid_ = std::max(max_setid_, setid + 1);
      buf = nullptr;
    } else {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
    }
  }
  return ret;
}

template <class WorkerT>
int CoSetSched::create_worker(int setid, RunFuncT runnable)
{
  int ret = common::OB_SUCCESS;
  if (setid >= max_setid_ || setid < 0 || sets_[setid] == nullptr) {
    ret = common::OB_INVALID_ARGUMENT;
  } else {
    Worker* w = nullptr;
    void* buf = __libc_malloc(sizeof(WorkerT));
    if (buf != nullptr) {
      w = new (buf) WorkerT(*this, runnable, nullptr);
      w->get_setid() = setid;
      w->owned_ = true;
      w->init();
      buf = nullptr;
    } else {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
    }
  }
  return ret;
}

template <class WorkerT>
int CoSetSched::create_worker(ObIAllocator& allocator, int setid, RunFuncT runnable)
{
  int ret = common::OB_SUCCESS;
  if (setid >= max_setid_ || setid < 0 || sets_[setid] == nullptr) {
    ret = common::OB_INVALID_ARGUMENT;
  } else {
    Worker* w = nullptr;
    void* buf = allocator_alloc(allocator, sizeof(WorkerT));
    if (buf != nullptr) {
      w = new (buf) WorkerT(*this, runnable, &allocator);
      w->get_setid() = setid;
      w->owned_ = true;
      w->init();
      buf = nullptr;
    } else {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
    }
  }
  return ret;
}

template <class WorkerT>
int CoSetSched::create_worker(ObIAllocator& allocator, int setid, RunFuncT runnable, Worker*& worker)
{
  int ret = common::OB_SUCCESS;
  if (setid >= max_setid_ || setid < 0 || sets_[setid] == nullptr) {
    ret = common::OB_INVALID_ARGUMENT;
  } else {
    Worker*& w = worker;
    void* buf = allocator_alloc(allocator, sizeof(WorkerT));
    if (buf != nullptr) {
      w = new (buf) WorkerT(*this, runnable, &allocator);
      w->get_setid() = setid;
      w->owned_ = false;
      w->init();
      buf = nullptr;
    } else {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
    }
  }
  return ret;
}

inline void CoSetSched::remove_finished_workers()
{
  auto w = static_cast<Worker*>(finished_routines_.remove_first());
  while (w != nullptr) {
    if (w->owned_) {
      auto allocator = w->get_allocator();
      w->~Worker();
      if (allocator != nullptr) {
        allocator->free(w);
      } else {
        __libc_free(w);
      }
    } else {
      w->set_run_status(CoRoutine::RunStatus::EXIT);
    }
    w = static_cast<Worker*>(finished_routines_.remove_first());
  }
}

/// If <tt>get_running_tsc() + coro::config().slice_ticks_</tt> is
/// overflow, it would call \c co_yield() directly for time of \c
/// coro::config().slice_ticks_ one year or so. And it's acceptable to
/// make this function using less instructs.
///
inline void CoSetSched::Worker::check()
{
  if (OB_LIKELY(get_instance() != get_active_routine()) &&
      OB_UNLIKELY(co_rdtscp() > get_running_tsc() + coro::config().slice_ticks_)) {
    get_sched().co_yield();
  }
}

inline int& CoSetSched::Worker::get_setid()
{
  return setid_;
}

inline ObIAllocator* CoSetSched::Worker::get_allocator() const
{
  return allocator_;
}

inline int& CoSetSched::Set::get_setid()
{
  return setid_;
}

inline int CoSetSched::create_routine(RoutineFunc start)
{
  return create_worker(0, start);
}

}  // namespace lib
}  // namespace oceanbase

#endif /* CO_SET_SCHED_H */
