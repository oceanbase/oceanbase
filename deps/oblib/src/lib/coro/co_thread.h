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

#ifndef CO_THREAD_H
#define CO_THREAD_H

#include "lib/coro/co_sched.h"
#include "lib/list/ob_dlink_node.h"
#include "lib/coro/thread.h"

namespace oceanbase {
namespace lib {

// CoThread is a thread with co-routine schedule support.
class CoThread : public Thread, public CoSched, public common::ObDLinkBase<CoThread> {
public:
  CoThread();
  // overwrite
  int start();
  using Thread::destroy;
};
}  // namespace lib
}  // namespace oceanbase

#endif /* CO_THREAD_H */
