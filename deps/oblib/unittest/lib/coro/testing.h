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

#ifndef CORO_TESTING_H
#define CORO_TESTING_H

#include <functional>
#include <thread>
#include <vector>

#include "lib/thread/thread_pool.h"

namespace cotesting {
//// Flexible worker pool
//
class FlexPool
{
public:
  FlexPool() {}

  FlexPool(std::function<void ()> func, int thes) : func_(func), thes_(thes)
  {
  }

  void start(bool wait = true)
  {
    for (auto i = 0; i < thes_ ; ++i) {
      th_pools_.push_back(std::thread(func_));
    }
    if (wait) {
      this->wait();
    }
  }

  void wait()
  {
    for (auto& th : th_pools_) {
      th.join();
    }
  }

private:
  std::function<void ()> func_;
  int thes_;
  std::vector<std::thread> th_pools_;
};

// Mock lib::ThreadPool
using DefaultRunnable = oceanbase::lib::ThreadPool;

// Give an assurance that executing time of the given function must
// less than specific time.
//
// Use macro instead of function because error message in function can
// only contain the most inner function location and outsider location
// would been ignored. It's not acceptable when many TIME_LESS
// functions exist outside.
#define TIME_LESS(time, func)                                                 \
  ({                                                                          \
    const auto start_ts = ::oceanbase::common::ObTimeUtility::current_time(); \
    {func();}                                                                 \
    const auto end_ts = ::oceanbase::common::ObTimeUtility::current_time();   \
    EXPECT_LT(end_ts - start_ts, (time));                                     \
  })

}  // cotesting

#endif /* CORO_TESTING_H */
