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

#ifndef CO_ROUTINE_H
#define CO_ROUTINE_H

#include "lib/coro/co_config.h"
#include "lib/coro/co_context.h"
#include "lib/coro/co_utils.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/ob_abort.h"
#include <functional>

namespace oceanbase {
namespace lib {

class Runnable {
public:
  virtual void run() = 0;
};

class CoSched;

// Essential wrapper of a co-routine.
class CoRoutine : public Runnable {
  friend class CoMainRoutine;

public:
  static constexpr uint64_t DEFAULT_STACK_MEM_TENANT_ID = 500;
  typedef int (*CoRoutineCBFunc)(CoRoutine&);
  static CoRoutineCBFunc co_cb_;

public:
  enum class RunStatus {
    BORN,
    RUNNABLE,
    RUNNING,
    WAITING,
    FINISHED,
    EXIT,
  };

  explicit CoRoutine(CoSched& sched);
  virtual ~CoRoutine();

  // Initialize CoRoutine, must called before other functions.
  //
  // @parem ss_size Stack size of routine, default is 2MB.
  int init(int64_t ss_size = 0);

  // Resume routine and save context into current routine.
  //
  // @param current Current routine instance
  //
  // @return OB_SUCCESS if this routine resume successfully and states
  // have saved into current routine. Otherwise, return other error
  // code.
  //
  int resume(CoRoutine& current);

  // Set run status for the routine.
  void set_run_status(RunStatus status);

  // Get run status of routine.
  RunStatus get_run_status() const;

  // Return routine is running or not
  //
  // @return true if routine is running
  //         false if routine isn't running
  bool is_running() const;

  // Return whether routine has done.
  //
  // @return true if routine has finished, false if not.
  bool is_finished() const;

  // Return routine context buffer which stores routine local data.
  //
  // @return routine local storage buffer
  char* get_crls_buffer();
  // Return routine context
  CoCtx& get_context();

  void* get_rtctx()
  {
    return rtctx_;
  }

  // Return routine index:
  //
  // union CoIdx {
  //   struct {
  //     uint64_t tidx_ : 12;
  //     uint64_t cidx_ : 48;
  //   };
  //   uint64_t idx_;
  // };
  //
  // Where tidx_ is unique for each thread and cidx_ is unique for
  // each routine on the same thread, hence idx_ is global unique for
  // each existing routine.
  //
  // tidx_/cidx_ can be acquired by get_tidx()/get_cidx() function.
  CoIdx get_idx() const;

  // Get T-IDX(Thread InDeX) of this routine. T-IDX is unique among
  // all living thread.
  uint64_t get_tidx() const;

  // Get C-IDX(Coroutine InDeX) of this routine. C-IDX is unique
  // unique among all living routines in own thread.
  uint64_t get_cidx() const;

  // Return routine ID. ID is unique and increases whenever new
  // routine has been created.
  uint64_t get_id() const;

  // set_waiting and wakeup are a pair of functions dealing with
  // wait&wakeup logic. set_waiting is used to set routine status from
  // running to waiting and wakeup is used to wakeup a waiting routine
  // by another routine. The two functions must appear evenly and
  // alternately i.e. not set a waiting routine waiting or wakeup a
  // already wakuped routine. Each wakeup call will only wakeup a
  // waiting routine once, hence if one thread set_waiting twice but
  // there's one wakeup the second set_waiting must return true. It
  // also can handle calling wakeup function ahead of calling
  // set_waiting, the wakeup call won't dismiss.
  //
  // Return:
  // If set routine status to waiting before being wakeuped by
  // another routine, return true. Others return false.
  bool set_waiting();
  void wakeup();

  virtual void usleep(uint32_t usec) = 0;
  virtual void sleep_until(int64_t abs_time) = 0;

  virtual int at_create();
  virtual void at_exit();

  uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  CoSched& get_sched() const
  {
    return sched_;
  }

  /// \brief Get the value of Time Stamp Counter when routine becomes
  /// RUNNING status.
  ///
  /// If routine is in RUNNING status, the value is equal to TSC value
  /// of point it becomes RUNNING status. Otherwise its value is the
  /// start point of last RUNNING status period. In the case of
  /// routine hasn't been scheduled the value is 0.
  ///
  /// \return value described above.
  uint64_t get_running_tsc() const
  {
    return running_tsc_;
  }

  void check();
  void yield();

  // \brief Wait for routine finish.
  void join();

public:
  bool operator==(const CoRoutine& rhs) const
  {
    return this == &rhs;
  }
  bool operator!=(const CoRoutine& rhs) const
  {
    return !operator==(rhs);
  }

private:
  // These functions are called whenever a routine becomes/leaves
  // corresponding status.
  virtual void on_status_change(RunStatus /*prev_status*/, RunStatus /*curr_status*/)
  {}

private:
  // This constructor is used to create main routine which doesn't
  // have successor routine so that when it exits from its runnable
  // function, the thread exits.
  // A thread may only have one main routine and it's the first routine thread
  // creates.
  CoRoutine();

  static void __start(boost::context::detail::transfer_t from);

private:
  CoIdx idx_;
  int64_t id_;
  CoCtx cc_;
  RunStatus rs_;
  CoSched& sched_;
  const uint64_t tenant_id_;

  /// Value of Time Stamp Counter when routine becomes RUNNING status.
  uint64_t running_tsc_;

  char rtctx_[coro::CoConfig::MAX_RTCTX_SIZE];
};

// Why there's a main routine?
//
// Main routine is a special routine that it attaches to its thread
// deeply. It's usually the first routine of that thread and it exits
// the thread exists. All other routines are successors of main
// routine, Main routine use the thread's context to run itself, such
// as stack space, signals and registers.
class CoMainRoutine : public CoRoutine {
public:
  explicit CoMainRoutine(CoSched& sched) : CoRoutine(sched)
  {}
  // Caution!!!
  // Overwrite CoRoutine init function.
  int init();
  // Start the main routine.
  void start();
  // Destroy resources.
  void destroy();
  void at_exit() override
  {}

  void usleep(uint32_t usec) final;
  void sleep_until(int64_t abs_time) final;
};

// Inline Functions
OB_INLINE char* CoRoutine::get_crls_buffer()
{
  return cc_.get_crls_buffer();
}

OB_INLINE CoCtx& CoRoutine::get_context()
{
  return cc_;
}

OB_INLINE CoIdx CoRoutine::get_idx() const
{
  return idx_;
}

OB_INLINE uint64_t CoRoutine::get_tidx() const
{
  return idx_.tidx_;
}

OB_INLINE uint64_t CoRoutine::get_cidx() const
{
  return idx_.cidx_;
}

OB_INLINE uint64_t CoRoutine::get_id() const
{
  return id_;
}

OB_INLINE void CoRoutine::check()
{
  if (OB_UNLIKELY(co_rdtscp() > get_running_tsc() + coro::config().slice_ticks_)) {
    yield();
  }
}

OB_INLINE void CoMainRoutine::usleep(uint32_t usec)
{
  UNUSED(usec);
  ob_abort();
}

OB_INLINE void CoMainRoutine::sleep_until(int64_t abs_time)
{
  UNUSED(abs_time);
  ob_abort();
}

}  // namespace lib
}  // namespace oceanbase

#endif /* CO_ROUTINE_H */
