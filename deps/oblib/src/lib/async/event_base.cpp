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

#include "event_base.h"
#include "lib/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"

using namespace oceanbase;
using namespace oceanbase::lib;
using namespace oceanbase::common;

static void sched_cb(EV_P_ struct ev_async* w, int revents);
static void timer_cb(EV_P_ struct ev_timer* w, int revents);

namespace oceanbase {
namespace lib {

EventBase::EventBase() : th_(), loop_(nullptr), sched_watcher_(), timer_watcher_()
{}

EventBase::~EventBase()
{}

int EventBase::init()
{
  int ret = OB_SUCCESS;
  loop_ = ev_loop_new(EVBACKEND_EPOLL | EVFLAG_NOENV);
  if (!loop_) {
    ret = OB_INIT_FAIL;
  } else {
    ev_async_init(&sched_watcher_, sched_cb);
    ev_async_start(loop_, &sched_watcher_);
    ev_init(&timer_watcher_, timer_cb);
  }
  return ret;
}

void EventBase::destroy()
{
  ev_async_stop(loop_, &sched_watcher_);
  ev_loop_destroy(loop_);
  loop_ = nullptr;
}

void EventBase::wait4event(int64_t maxwait)
{
  // Since low-level epoll interface takes timeout parameter in
  // milliseconds, we consume CPU cycles rather than let it get into
  // epoll wait status for low latency of scheduler.
  if (maxwait >= 1000) {
    const double evwait = static_cast<double>(maxwait) / 1000000;
    timer_watcher_.repeat = evwait;
    ev_timer_again(loop_, &timer_watcher_);
    ev_run(loop_, EVRUN_ONCE);
  } else {
    ev_run(loop_, EVRUN_NOWAIT);
  }
}

void EventBase::signal()
{
  ev_async_send(loop_, &sched_watcher_);
}

}  // namespace lib
}  // namespace oceanbase

static void sched_cb(EV_P_ struct ev_async* w, int revents)
{
  UNUSEDx(loop, w, revents);
}

static void timer_cb(EV_P_ struct ev_timer* w, int revents)
{
  UNUSEDx(loop, w, revents);
}
