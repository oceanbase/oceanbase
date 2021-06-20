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

#ifndef CO_SCHED_H
#define CO_SCHED_H

#include <functional>
#include "lib/coro/co_routine.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/ob_errno.h"

namespace oceanbase {
namespace lib {
class CoTimer;

/// \c CoSched is an abstract class for co-routine scheduler. A
/// co-routine scheduler is a routine which deals with all user
/// defined routines. Each user defined routine can yield itself so
/// that the scheduler would choose a suitable routine to run, if
/// exists.
///
/// CoSched is singleton for each thread.
class CoSched : public CoMainRoutine {
public:
  static CoSched* get_instance();
  static CoRoutine* get_active_routine();
  static void set_active_routine(CoRoutine& routine);

protected:
  using RoutineFunc = std::function<void()>;

public:
  CoSched();
  virtual ~CoSched();

  // overwrite main routine start function.
  int start();

  // Inherit and make accessible
  using CoMainRoutine::get_tidx;

  /// \brief create a routine with entry function of \c start.
  ///
  /// Return OB_ERROR if derived class doesn't support create new
  /// routine.
  ///
  /// \param start The entry function of the creating routine.
  /// \return Error code
  virtual int create_routine(RoutineFunc start)
  {
    UNUSED(start);
    return common::OB_ERROR;
  }

public:
  // Called by normal routine to yield execution resources to others.
  void co_yield(void);
  int co_wait(int64_t duration);
  void co_wakeup(CoRoutine& routine);
  CoRoutine& co_current(void);

  // Reschedule among routines. This function would switch to schedule
  // routine and select appropriate routine to run.
  void reschedule(void);

  /// \brief start expiry timer, for internal usage.
  virtual void start_timer(CoTimer& timer)
  {
    UNUSED(timer);
  }
  virtual void cancel_timer(CoTimer& timer)
  {
    UNUSED(timer);
  }

protected:
  // Called by schedule routine to resume specific routine.
  void resume_routine(CoRoutine& routine);

public:
  static __thread CoSched* instance_;
  static __thread CoRoutine* active_routine_;

  bool inited_;  // Whether initialization in constructor successfully.
};

OB_INLINE CoSched::CoSched() : CoMainRoutine(*this), inited_(false)
{}

OB_INLINE CoSched* CoSched::get_instance()
{
  return instance_;
}

OB_INLINE CoRoutine* CoSched::get_active_routine()
{
  return active_routine_;
}

OB_INLINE void CoSched::set_active_routine(CoRoutine& routine)
{
  active_routine_ = &routine;
}

OB_INLINE CoSched::~CoSched()
{
  if (active_routine_ == this) {
    active_routine_ = nullptr;
  }
  if (instance_ == this) {
    active_routine_ = nullptr;
  }
}

OB_INLINE int CoSched::start()
{
  int ret = common::OB_SUCCESS;
  if (instance_ != nullptr) {
    ret = common::OB_INIT_TWICE;
  } else if (!inited_) {
    if (OB_SUCC(init())) {
      inited_ = true;
      active_routine_ = instance_ = this;
      CoMainRoutine::start();
      CoMainRoutine::destroy();
      inited_ = false;
    }
  } else {
    // Can only start once.
    //
    // If scheduler has ever start successfully, inited_ must be true.
    ret = common::OB_OP_NOT_ALLOW;
  }
  return ret;
}

OB_INLINE void CoSched::reschedule()
{
  CoRoutine* active_routine_prev = active_routine_;
  active_routine_ = get_instance();
  get_instance()->resume(*active_routine_prev);
}

OB_INLINE void CoSched::resume_routine(CoRoutine& routine)
{
  active_routine_ = &routine;
  active_routine_->set_run_status(CoRoutine::RunStatus::RUNNING);
  active_routine_->resume(*instance_);
}

OB_INLINE void CoSched::co_yield(void)
{
  auto& routine = *CoSched::get_active_routine();
  routine.set_run_status(CoRoutine::RunStatus::RUNNABLE);
  reschedule();
}

OB_INLINE void CoSched::co_wakeup(CoRoutine& routine)
{
  routine.wakeup();
}

OB_INLINE CoRoutine& CoSched::co_current(void)
{
  return *active_routine_;
}

}  // namespace lib
}  // namespace oceanbase

#endif /* CO_SCHED_H */
