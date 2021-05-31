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

#ifndef CORO_ROUTINE_H
#define CORO_ROUTINE_H

#include <cstdint>
#include "lib/coro/co_sched.h"
#include "lib/coro/co_set_sched.h"
#include "lib/coro/thread.h"

/// \cond
// Define alias to scheduler for convenience.
#define CO_SCHED (*::oceanbase::lib::CoSched::get_instance())
/// \endcond

namespace oceanbase {
namespace lib {

namespace coro {
OB_INLINE ::oceanbase::lib::CoRoutine& current()
{
  return CO_SCHED.co_current();
}

OB_INLINE bool is_enabled(void)
{
  return &CO_SCHED != nullptr;
}
}  // namespace coro

namespace this_thread {
bool is_monopoly();
}

/// This namespace groups a set of functions that access the current
/// routine.
namespace this_routine {

/// \brief Sleep for \c usec micro seconds.
///
/// Block execution of current routine during span of time specified
/// by \c usec in micro seconds.
///
/// This execution of current routine is stopped until at least \c
/// usec micro seconds has passed from now. Other routines continue
/// their execution.
OB_INLINE void usleep(uint32_t usec)
{
  if (!this_thread::is_monopoly() && coro::is_enabled()) {
    coro::current().usleep(usec);
  } else {
    ::usleep(usec);
  }
}

/// \brief Yield to other routines.
///
/// The calling routine yields, offering other routines the
/// opportunity to continue their execution.
///
/// This function shall be called when a routine waits for other
/// routines to advance without blocking itself.
OB_INLINE void yield()
{
  coro::current().yield();
}

/// \brief Check necessary to yield according to time slice.
///
/// This function checks current routine has used up its time
/// slice. If so, it will automatically call \c yield() giving chance
/// to other routines.
///
/// The function shall be called when routine seems to run for some
/// time. Otherwise it may be too long for some other routines
/// awaiting to execute.
///
/// It's a co-routine specific function that for most thread-based
/// scheduling routine would be preempted periodically if one has
/// consumed CPU for some time. But for co-routine scheduling it's
/// responsible for routine itself to give execution right to other
/// routines.
///
/// It has been optimized for performance and the time of checking
/// need to yield shall be no more than tens of nanoseconds.
///
/// \see yield()
/// \see oceanbase::lib::coro::CoConfig::slice_ticks_
OB_INLINE void check()
{
  if (coro::is_enabled() && coro::current() != CO_SCHED) {
    coro::current().check();
  }
}

/// \brief Sleep until time point.
///
/// Blocks the calling routine untile -c abs_time.
///
/// The execution of the current routine is stopped until at least \c
/// abs_time, while other routines may continue to advance.
///
/// \param abs_time Time point in micro second format.
///
/// \see usleep()
OB_INLINE void sleep_until(int64_t abs_time)
{
  coro::current().sleep_until(abs_time);
}

/// \brief Get routine ID
///
/// Returns the routine ID of the calling routine.
///
/// This value uniquely identified the routine.
///
/// \return A uint64_t value which uniquely identified the routine.
OB_INLINE uint64_t get_id()
{
  return coro::current().get_id();
}

}  // namespace this_routine

namespace this_thread {
#define CO_SETSCHED static_cast<::oceanbase::lib::CoSetSched&>(CO_SCHED)

OB_INLINE uint64_t get_cs()
{
  return CO_SETSCHED.get_cs();
}

OB_INLINE bool has_set_stop()
{
  return Thread::current().has_set_stop();
}

OB_INLINE bool is_monopoly()
{
  return Thread::monopoly_;
}

OB_INLINE void set_monopoly(bool v = true)
{
  Thread::monopoly_ = v;
}

}  // namespace this_thread

}  // namespace lib
}  // namespace oceanbase

#undef CO_SCHED
#undef CO_SETSCHED

#endif /* CORO_ROUTINE_H */
