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

#ifndef EVENT_BASE_H
#define EVENT_BASE_H

#include <thread>
#include "io/ev.h"

namespace oceanbase {
namespace lib {

class EventBase {
public:
  EventBase();
  virtual ~EventBase();

  int init();
  void destroy();

protected:
  void signal();
  void wait4event(int64_t maxwait = 0);

private:
  std::thread th_;
  struct ev_loop* loop_;
  ev_async sched_watcher_;
  ev_timer timer_watcher_;
};

}  // namespace lib
}  // namespace oceanbase

#endif /* EVENT_BASE_H */
