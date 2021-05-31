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

#define USING_LOG_PREFIX LIB
#include "co_routine.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/coro/co.h"
#include "lib/coro/co_config.h"
#include "lib/coro/co_sched.h"
#include "lib/coro/routine.h"
#include "lib/oblog/ob_log.h"
#include "lib/rc/context.h"

using namespace oceanbase::lib;
using namespace oceanbase::common;
using namespace boost::context::detail;

namespace oceanbase {
namespace lib {
CoRoutine::CoRoutineCBFunc CoRoutine::co_cb_;
}
}  // namespace oceanbase

CoRoutine::CoRoutine(CoSched& sched)
    : idx_(), id_(), rs_(RunStatus::BORN), sched_(sched), tenant_id_(DEFAULT_STACK_MEM_TENANT_ID), running_tsc_(0)
{
  id_ = alloc_coid();
}

CoRoutine::~CoRoutine()
{
  CoCtx::destroy(&cc_);
}

int CoRoutine::init(int64_t ss_size)
{
  int ret = OB_SUCCESS;
  if (ss_size == 0) {
    ss_size = coro::config().stack_size_;
  }
  if (OB_FAIL(CoCtx::create(get_tenant_id(), ss_size, &cc_))) {
    LOG_ERROR("create new context fail", K(ss_size), K(ret));
  }
  if (OB_SUCC(ret)) {
    void* stackaddr = nullptr;
    size_t stacksize = 0;
    cc_.get_stack(stackaddr, stacksize);
    cc_.get_ctx() = make_fcontext((char*)stackaddr + stacksize, stacksize, __start);
    set_run_status(RunStatus::BORN);
    set_run_status(RunStatus::RUNNABLE);
  }
  if (OB_SUCC(ret) && co_cb_) {
    ret = co_cb_(*this);
  }
  return ret;
}

int CoRoutine::resume(CoRoutine& current)
{
  UNUSED(current);
  int ret = OB_SUCCESS;

  running_tsc_ = co_rdtscp();
  assert(cc_.get_ctx());
  transfer_t transfer = jump_fcontext(cc_.get_ctx(), this);
  cc_.get_ctx() = transfer.fctx;

  return ret;
}

CoRoutine::RunStatus CoRoutine::get_run_status() const
{
  return rs_;
}

void CoRoutine::set_run_status(RunStatus status)
{
  on_status_change(rs_, status);
  ATOMIC_STORE((int32_t*)&rs_, (int32_t)status);
}

bool CoRoutine::is_running() const
{
  return rs_ == RunStatus::RUNNING;
}

bool CoRoutine::is_finished() const
{
  return rs_ == RunStatus::FINISHED;
}

bool CoRoutine::set_waiting()
{
  ATOMIC_STORE((int32_t*)&rs_, (int32_t)RunStatus::WAITING);
  on_status_change(RunStatus::RUNNING, RunStatus::WAITING);
  return true;
}

void CoRoutine::wakeup()
{
  ATOMIC_STORE((int32_t*)&rs_, (int32_t)RunStatus::RUNNABLE);
  on_status_change(RunStatus::WAITING, RunStatus::RUNNABLE);
}

// Start point of routine called when first resume. Reference and
// pointer is same under compiler and we use reference of routine here
// for convenience.
void CoRoutine::__start(transfer_t from)
{
  // mark context of sched for jumping after
  CoRoutine& routine = *reinterpret_cast<CoRoutine*>(from.data);
  CoRoutine& sched = routine.get_sched();
  sched.get_context().get_ctx() = from.fctx;

  routine.idx_ = alloc_coidx();

  routine.set_run_status(RunStatus::RUNNING);
  routine.at_create();
  int ret = OB_SUCCESS;
  MemoryContext** mem_context = GET_TSI0(MemoryContext*);
  // Thread has ensured the thread-local mem_context is created successfully
  assert(mem_context != nullptr && *mem_context != nullptr);
  WITH_CONTEXT(*mem_context)
  {
    routine.run();
  }
  routine.at_exit();
  free_coidx(routine.idx_);

  routine.set_run_status(RunStatus::FINISHED);
  jump_fcontext(sched.get_context().get_ctx(), &routine);
  // routine exit, jump to sched, will not reach here again
  OB_ASSERT(0);
}

int CoRoutine::at_create()
{
  CVC.at_routine_create();
  return OB_SUCCESS;
}

void CoRoutine::at_exit()
{
  CVC.at_routine_exit();
  return;
}

void CoRoutine::yield()
{
  sched_.co_yield();
}

void CoRoutine::join()
{
  // FIXME: use condition wait instead.
  while (rs_ != RunStatus::EXIT) {
    this_routine::usleep(1000);
  }
}

//// class CoMainRoutine
int CoMainRoutine::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(CoCtx::create(get_tenant_id(), 0, &cc_))) {
    LOG_ERROR("create new context fail", K(ret));
  } else if (co_cb_) {
    ret = co_cb_(*this);
  }
  return ret;
}

void CoMainRoutine::start()
{
  idx_ = alloc_coidx();
  at_create();
  run();
  CoSched::instance_ = nullptr;  // return to thread mode, enable lock waiting
  CoSched::active_routine_ = nullptr;
  at_exit();
  free_coidx(idx_);
}

void CoMainRoutine::destroy()
{
  CoCtx::destroy(&cc_);
}
