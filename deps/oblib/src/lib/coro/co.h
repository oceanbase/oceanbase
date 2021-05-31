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

#ifndef OBLIB_CO_H
#define OBLIB_CO_H

#include "lib/coro/co_var.h"
#include "lib/coro/co_sched.h"
#include "lib/coro/co_thread.h"
#include "lib/coro/co_set_sched.h"
// syscalls
#include "lib/coro/syscall/co_futex.h"
// user interfaces
#include "lib/coro/routine.h"

namespace oceanbase {
namespace lib {

/// \cond
#define CO_SCHED (*CoSched::get_instance())
/// \endcond

OB_INLINE bool CO_IS_ENABLED(void)
{
  return &CO_SCHED != nullptr;
}
OB_INLINE CoRoutine& CO_CURRENT(void)
{
  return CO_SCHED.co_current();
}
OB_INLINE void CO_YIELD(void)
{
  CO_SCHED.co_yield();
}
OB_INLINE int CO_WAIT(int64_t duration = 0)
{
  return CO_SCHED.co_wait(duration);
}
OB_INLINE void CO_START_TIMER(CoTimer& t)
{
  CO_SCHED.start_timer(t);
}
OB_INLINE void CO_CANCEL_TIMER(CoTimer& t)
{
  CO_SCHED.cancel_timer(t);
}
OB_INLINE void CO_WAIT0()
{
  CO_SCHED.set_active_routine(CO_SCHED);
}
OB_INLINE void CO_SCHEDULE()
{
  CO_SCHED.reschedule();
}
OB_INLINE void CO_WAKEUP(CoRoutine& routine)
{
  CO_SCHED.co_wakeup(routine);
}
OB_INLINE uint64_t CO_TIDX(void)
{
  return CO_SCHED.get_tidx();
}
OB_INLINE uint64_t CO_CIDX(void)
{
  return CO_CURRENT().get_cidx();
}
OB_INLINE uint64_t CO_ID(void)
{
  return CO_CURRENT().get_id();
}
OB_INLINE void CO_FORCE_SYSCALL_FUTEX()
{
  CoFutex::force_syscall_futex();
}

// CoSetSched relating functions.
OB_INLINE CoSetSched& CO_SETSCHED()
{
  return static_cast<CoSetSched&>(CO_SCHED);
}

OB_INLINE CoSetSched::Worker& CO_SETSCHED_CURRENT()
{
  return static_cast<CoSetSched::Worker&>(CO_CURRENT());
}

OB_INLINE void CO_SETSCHED_SWITCH(int setid)
{
  CO_SETSCHED_CURRENT().get_setid() = (setid);
  CO_YIELD();
}

/// \brief Check if current routine should give CPU to another
/// routine.
OB_INLINE void CO_SETSCHED_CHECK()
{
  if (CO_IS_ENABLED()) {
    CO_SETSCHED_CURRENT().check();
  }
}

/// \brief Create routine in current core when using \c CoSetSched.
///
/// \param allocator Allocator used to allocate memory when create
///                  routine.
/// \param setid     ID of set when creating the routine.
/// \param start     Entry function when starting new routine.
///
/// \return OB_SUCCESS or other error code.
///
OB_INLINE int CO_SETSCHED_CREATE(ObIAllocator& allocator, int setid, CoSetSched::RunFuncT start)
{
  return CO_SETSCHED().create_worker(allocator, setid, start);
}

/// \brief Create routine in current core when using \c CoSetSched.
///
/// \param allocator      Allocator used to allocate memory when create
///                       routine.
/// \param setid          ID of set when creating the routine.
/// \param start          Entry function when starting new routine.
/// \param [out] routine  Routine to be created.
///
/// \return OB_SUCCESS or other error code.
///
OB_INLINE int CO_SETSCHED_CREATE(
    ObIAllocator& allocator, int setid, CoSetSched::RunFuncT start, CoSetSched::Worker*& routine)
{
  return CO_SETSCHED().create_worker(allocator, setid, start, routine);
}

#define CO_THREAD_ATEXIT(func) oceanbase::lib::CoBaseSched::add_exit_cb(func)

/// \brief Destroy routine
///
/// \param routine Address of routine should be destroy.
///
OB_INLINE void CO_DESTROY(CoRoutine* routine)
{
  auto w = static_cast<CoSetSched::Worker*>(routine);
  auto allocator = w->get_allocator();
  w->~Worker();
  allocator->free(w);
}

/// \brief Create a new routine by specified allocator.
///
/// \param allocator Allocator used to allocate memory when create
///                  routine.
/// \param start     Entry function when starting new routine.
///
/// \return OB_SUCCESS or other error code.
///
OB_INLINE int CO_CREATE(ObIAllocator& allocator, CoSetSched::RunFuncT start)
{
  return CO_SETSCHED_CREATE(allocator, 0, start);
}

/// \brief Create a new routine by specified allocator.
///
/// \param allocator      Allocator used to allocate memory when create
///                       routine.
/// \param start          Entry function when starting new routine.
/// \param [out] routine  Routine to be created.
///
/// \return OB_SUCCESS or other error code.
///
OB_INLINE int CO_CREATE(ObIAllocator& allocator, CoSetSched::RunFuncT start, CoRoutine*& routine)
{
  return CO_SETSCHED_CREATE(allocator, 0, start, reinterpret_cast<CoSetSched::Worker*&>(routine));
}

/// \brief Make checkpoint and give CPU to another routine.
///
OB_INLINE void CO_CHECK()
{
  CO_SETSCHED_CHECK();
}

#undef CO_SCHED

}  // namespace lib
}  // namespace oceanbase

#endif /* OBLIB_CO_H */
