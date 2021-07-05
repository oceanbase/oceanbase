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

#ifndef CO_THREAD_MGR_H
#define CO_THREAD_MGR_H

#include "lib/utility/ob_macro_utils.h"
#include "lib/list/ob_dlist.h"
#include "lib/coro/co_thread.h"

namespace oceanbase {
namespace lib {

// CTM(Co-Thread-Mgr) is responsible for managing co-routine threads and
// support mechanism for them to communicate with each other.
class CoThreadMgr {
  constexpr static auto MAX_COTH_COUNT = 1024;

public:
  CoThreadMgr();
  virtual ~CoThreadMgr();

  // Start all Co-Thread already added. If failed to start, Co-Thread
  // would been stopped before return.
  int start();
  // Non-block stop all Co-Thread.
  void stop();
  // Wait all Co-Thread exist after calling stop function.
  void wait();

  int add_thread(CoThread& th);

private:
  DISALLOW_COPY_AND_ASSIGN(CoThreadMgr);

  CoThread* th_vec_[MAX_COTH_COUNT];
  common::ObDList<CoThread> th_list_;
};

}  // namespace lib
}  // namespace oceanbase

#endif /* CO_THREAD_MGR_H */
